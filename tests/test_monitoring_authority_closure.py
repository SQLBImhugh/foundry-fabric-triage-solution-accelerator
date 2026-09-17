from __future__ import annotations

import copy
import socket
from uuid import UUID

import httpx
import pytest
from test_monitoring_deployment_registration import (
    CLIENT2,
    ENDPOINT,
    OBJECT2,
    TIMER,
    InventorySources,
    RegisteredSqlFake,
    arrangement,
)
from test_monitoring_reset import SITE, TARGET, TENANT, settings_document

from scripts import reset_monitoring_state as reset
from triage.monitoring.deployment_authority import CALLABLE_TYPES, read_authority
from triage.monitoring.deployment_contracts import DeploymentError
from triage.monitoring.deployment_registry import RegisteredDeploymentInventoryReader
from triage.monitoring.deployment_schema import unqualified
from triage.monitoring.sql_permissions import build_permission_kernel

SHADOW_SITE = SITE + "-shadow-writer"
SHADOW_MODULE = 1_100
SHADOW_TRIGGER = 1_101
AUX_SCHEMA = 2


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *_: pytest.fail("Authority closure tests are offline"))


class AuthoritySqlFake(RegisteredSqlFake):
    def __init__(self):
        super().__init__()
        self.schemas = [(1, "dbo", 1), (AUX_SCHEMA, "aux", 1)]
        self.object_owners = {}
        self.principal_owners = {}
        self.extra_cascades = []

    def query(self, sql, *params):
        rows = super().query(sql, *params)
        if "deployment-authority:objects" in sql:
            rows = [(*row[:4], self.object_owners.get(row[0], row[4]), *row[5:]) for row in rows]
        if "deployment-authority:principals" in sql:
            rows = [(*row[:4], self.principal_owners.get(row[0], row[4]), *row[5:]) for row in rows]
        if "deployment-authority:cascades" in sql:
            rows += self.extra_cascades
        return self.schemas if "deployment-authority:schemas" in sql else rows


def sql_path(kind):
    db = AuthoritySqlFake()
    db.sql_writer_principals.append((12, "E", UUID(CLIENT2).bytes_le, 0))
    db.supply_default_grants = False
    known = unqualified(build_permission_kernel().rpcs["controller.reserve_action"].object_name)
    db.extra_permissions = [(11, 1, 1, db.module_ids[known], 0, "EXECUTE", "G")]
    if kind in {"schema_execute", "direct_execute"}:
        db.extra_native_objects.append((SHADOW_MODULE, "aux", "shadow_write", "P", 1, "a" * 64, None, 0))
        scope = (3, AUX_SCHEMA) if kind == "schema_execute" else (1, SHADOW_MODULE)
        db.extra_permissions.append((12, 1, *scope, 0, "EXECUTE", "G"))
    else:
        table = db.object_ids["business_orders"]
        db.extra_permissions.append((12, 1, 1, table, 1, "UPDATE", "G"))
        db.extra_native_objects.append((SHADOW_TRIGGER, "dbo", "business_write", "TR", 1, "b" * 64, -2, 0))
        db.sql_triggers.append((SHADOW_TRIGGER, 1, table, False))
    return db


def running_shadow_source():
    sources = InventorySources()
    sources.resources.append({"id": SHADOW_SITE, "type": "Microsoft.Web/sites"})
    sources.sites[SHADOW_SITE] = {
        "state": "Running", "settings": settings_document(client=CLIENT2),
        "identity": {"type": "SystemAssigned", "principalId": OBJECT2, "tenantId": TENANT},
    }
    return sources


@pytest.mark.parametrize("kind", ["schema_execute", "table_trigger", "direct_execute"])
def test_original_sql_metadata_paths_include_indirect_writer_and_refuse_authority(kind):
    db = sql_path(kind)
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert {writer.principal_id for writer in authority.writers} == {11, 12}
    assert authority.gaps
    with pytest.raises(DeploymentError):
        authority.require_protected()
    assert not db.statements


@pytest.mark.parametrize("kind", ["P", "PC"])
def test_select_only_schema_does_not_make_its_procedures_reachable(kind):
    db = sql_path("schema_execute")
    row = list(db.extra_native_objects[0])
    row[3] = kind
    db.extra_native_objects[0] = tuple(row)
    db.extra_permissions[-1] = (12, 1, 3, AUX_SCHEMA, 0, "SELECT", "G")
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps
    assert {writer.principal_id for writer in authority.writers} == {11}
    assert {"P", "PC"} <= CALLABLE_TYPES
    assert not db.statements


def test_select_only_schema_keeps_actual_discovery_and_preparation_unblocked():
    db = sql_path("schema_execute")
    db.extra_permissions[-1] = (12, 1, 3, AUX_SCHEMA, 0, "SELECT", "G")
    db, _, _, registry, _ = arrangement(db=db, sources=running_shadow_source())
    plan = registry.prepare(binding_id="select-only-control")
    assert not plan.blockers and not plan.capture.gaps
    assert {writer.writer.resource_id for writer in plan.capture.writers} == {SITE}
    assert not db.statements and db.delete_calls == 0


@pytest.mark.parametrize("kind", ["P", "PC"])
@pytest.mark.parametrize("authority_path", ["execute", "control", "ownership"])
def test_procedures_remain_reachable_through_their_actual_authority(kind, authority_path):
    db = sql_path("schema_execute")
    row = list(db.extra_native_objects[0])
    row[3] = kind
    db.extra_native_objects[0] = tuple(row)
    if authority_path == "control":
        db.extra_permissions[-1] = (12, 1, 1, SHADOW_MODULE, 0, "CONTROL", "G")
    elif authority_path == "ownership":
        db.extra_permissions.pop()
        db.object_owners[SHADOW_MODULE] = 12
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert authority.gaps
    assert {writer.principal_id for writer in authority.writers} == {11, 12}
    if authority_path == "ownership":
        assert "unreviewed_owned_module_authority" in authority.gaps


@pytest.mark.parametrize("kind", ["V", "FN", "IF", "TF", "FS", "FT", "AF", "SN"])
@pytest.mark.parametrize("permission_scope", ["object", "schema"])
def test_select_still_follows_unreviewed_views_functions_and_synonyms(kind, permission_scope):
    db = sql_path("schema_execute")
    row = list(db.extra_native_objects[0])
    row[3] = kind
    db.extra_native_objects[0] = tuple(row)
    scope = (1, SHADOW_MODULE) if permission_scope == "object" else (3, AUX_SCHEMA)
    db.extra_permissions[-1] = (12, 1, *scope, 0, "SELECT", "G")
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert "unreviewed_module_authority" in authority.gaps
    assert {writer.principal_id for writer in authority.writers} == {11, 12}


def test_select_on_a_table_does_not_fire_its_dml_trigger():
    db = sql_path("table_trigger")
    db.extra_permissions[-1] = (12, 1, 1, db.object_ids["business_orders"], 0, "SELECT", "G")
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps
    assert {writer.principal_id for writer in authority.writers} == {11}


def test_select_on_an_intact_reviewed_write_view_is_an_ordinary_read():
    db = sql_path("schema_execute")
    view = unqualified(build_permission_kernel().names.object("worker_catalogue"))
    db.extra_permissions[-1] = (12, 1, 1, db.module_ids[view], 0, "SELECT", "G")
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps
    assert {writer.principal_id for writer in authority.writers} == {11}


@pytest.mark.parametrize("kind", ["schema_execute", "table_trigger"])
def test_original_sql_paths_cannot_hide_running_writer_from_actual_collector_or_reset(kind):
    db, sources, collector, registry, _ = arrangement(db=sql_path(kind), sources=running_shadow_source())
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    capture = collector.collect(authority)
    by_client = {writer.identity_client_id: writer for writer in capture.writers}
    assert by_client[CLIENT2].writer.resource_id == SHADOW_SITE
    assert by_client[CLIENT2].state == "running"
    assert capture.gaps
    assert any(request.url.path == SHADOW_SITE for request in sources.requests)
    plan = registry.prepare(binding_id="indirect-authority-counterexample")
    assert plan.blockers
    before = copy.deepcopy(db.tables)
    with pytest.raises(DeploymentError):
        registry.accept(plan, confirmed_manifest_hash=plan.manifest_hash)
    operator = reset.SqlResetOperator(
        db, TARGET, deployment_inventory_reader=RegisteredDeploymentInventoryReader(collector),
    )
    manifest = operator.plan()
    assert manifest.snapshot.blockers and manifest.deployment_inventory is None
    assert db.tables == before and not db.statements and db.delete_calls == 0


@pytest.mark.parametrize("path", [
    "transitive_schema_execute", "empty_schema_execute", "unknown_schema", "schema_function",
    "uninspectable_module", "changed_read_rpc", "synonym", "nonowned_control",
])
def test_unknown_indirect_sql_authority_never_disappears_from_writer_universe(path):
    db = sql_path("schema_execute")
    if path == "transitive_schema_execute":
        db.role_ids.update({"outer_fixture": 800, "inner_fixture": 801})
        db.extra_role_edges = [(12, 800), (800, 801)]
        db.extra_permissions[-1] = (801, 1, 3, AUX_SCHEMA, 0, "EXECUTE", "G")
        db.extra_permissions.append((12, 1, 3, AUX_SCHEMA, 0, "EXECUTE", "D"))
    elif path == "empty_schema_execute":
        db.extra_native_objects.clear()
    elif path == "unknown_schema":
        db.extra_permissions[-1] = (12, 1, 3, 999, 0, "EXECUTE", "G")
    elif path == "schema_function":
        db.extra_native_objects = [(SHADOW_MODULE, "aux", "shadow_function", "FN", 1, "a" * 64, None, 0)]
        db.extra_permissions[-1] = (12, 1, 3, AUX_SCHEMA, 0, "SELECT", "G")
    elif path == "uninspectable_module":
        db.extra_native_objects = [(SHADOW_MODULE, "aux", "shadow_write", "P", 1, None, -2, 1)]
    elif path == "changed_read_rpc":
        name = unqualified(build_permission_kernel().rpcs["inspect"].object_name)
        db.native_module_hashes[name] = "0" * 64
        db.extra_permissions[-1] = (12, 1, 1, db.module_ids[name], 0, "EXECUTE", "G")
    elif path == "synonym":
        db.extra_native_objects = [(SHADOW_MODULE, "aux", "shadow_alias", "SN", 1, None, None, 0)]
    else:
        db.extra_permissions[-1] = (12, 1, 1, db.object_ids["business_orders"], 0, "ALTER", "G")
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert {item.principal_id for item in authority.writers} == {11, 12}
    assert authority.gaps and not db.statements


@pytest.mark.parametrize("owned", ["schema", "table", "role"])
def test_implicit_control_cannot_hide_behind_the_absence_of_direct_grants(owned):
    db = sql_path("schema_execute")
    db.extra_permissions.pop()
    if owned == "schema":
        db.schemas[-1] = (AUX_SCHEMA, "aux", 12)
    elif owned == "table":
        db.object_owners[db.object_ids["business_orders"]] = 12
    else:
        db.role_ids["indirect_owner_fixture"] = 800
        db.principal_owners[800] = 12
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert "runtime_ownership_path" in authority.gaps
    assert {item.principal_id for item in authority.writers} == {11, 12}


@pytest.mark.parametrize("missing", ["trigger_parent", "cascade_parent"])
def test_incomplete_indirect_metadata_refuses_instead_of_returning_empty_paths(missing):
    db = sql_path("table_trigger")
    if missing == "trigger_parent":
        db.sql_triggers[0] = (SHADOW_TRIGGER, 1, 99_999, False)
    else:
        db.extra_cascades.append((8_000, db.object_ids["business_orders"], 99_999, 1, 0, False))
    with db.transaction(), pytest.raises(DeploymentError, match="unresolved"):
        read_authority(db, TARGET, db.catalogue)
    assert not db.statements


@pytest.mark.parametrize("operation,delete_action,update_action", [
    ("DELETE", 1, 0), ("DELETE", 2, 0), ("DELETE", 3, 0), ("UPDATE", 0, 1),
])
def test_dml_cascades_reach_triggers_beyond_the_first_nonowned_table(operation, delete_action, update_action):
    db = sql_path("table_trigger")
    db.sql_triggers.clear()
    db.extra_permissions[-1] = (12, 1, 1, db.object_ids["business_orders"], 0, operation, "G")
    for index, name in enumerate(("business_lines", "business_audit"), start=1_200):
        db.tables[name] = [{"id": 1}]
        db.object_ids[name] = index
    db.fks.extend([
        ("dbo", "business_lines", "order_id", "dbo", "business_orders", "order_id", delete_action, update_action, False),
        ("dbo", "business_audit", "order_id", "dbo", "business_lines", "order_id", 1, 1, False),
    ])
    db.sql_triggers.append((SHADOW_TRIGGER, 1, db.object_ids["business_audit"], False))
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert "unreviewed_trigger_authority" in authority.gaps
    assert {item.principal_id for item in authority.writers} == {11, 12}
    assert not db.statements


def test_nonowned_parent_cascade_into_owned_state_is_application_write_authority():
    db = sql_path("table_trigger")
    db.sql_triggers.clear()
    db.extra_permissions[-1] = (12, 1, 1, db.object_ids["business_orders"], 0, "DELETE", "G")
    db.fks.append((
        "dbo", db.table_name("incidents"), "incident_id", "dbo", "business_orders", "order_id", 1, 0, False,
    ))
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert "raw_operational_table_write" in authority.gaps
    assert {item.principal_id for item in authority.writers} == {11, 12}


@pytest.mark.parametrize("operation", ["INSERT", "UPDATE", "DELETE"])
def test_all_direct_and_column_dml_grants_check_enabled_trigger_paths(operation):
    db = sql_path("table_trigger")
    db.extra_permissions[-1] = (
        12, 1, 1, db.object_ids["business_orders"], 1 if operation == "UPDATE" else 0, operation, "G",
    )
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert "unreviewed_trigger_authority" in authority.gaps
    assert 12 in {item.principal_id for item in authority.writers}


def test_schema_dml_expands_to_triggered_tables_outside_dbo():
    db = sql_path("schema_execute")
    table = 1_202
    db.extra_native_objects = [
        (table, "aux", "orders", "U", 1, None, None, 0),
        (SHADOW_TRIGGER, "aux", "orders_trigger", "TR", 1, None, -2, 0),
    ]
    db.extra_permissions[-1] = (12, 1, 3, AUX_SCHEMA, 0, "UPDATE", "G")
    db.sql_triggers = [(SHADOW_TRIGGER, 1, table, False)]
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert "unreviewed_trigger_authority" in authority.gaps
    assert {item.principal_id for item in authority.writers} == {11, 12}


@pytest.mark.parametrize("operation", ["inspect", "controller.publish_connector"])
def test_explicit_reviewed_rpc_through_roles_keeps_read_and_write_authority_distinct(operation):
    db = sql_path("schema_execute")
    db.extra_native_objects.clear()
    db.role_ids.update({"outer_fixture": 800, "inner_fixture": 801})
    db.extra_role_edges = [(12, 800), (800, 801)]
    name = unqualified(build_permission_kernel().rpcs[operation].object_name)
    db.extra_permissions[-1] = (801, 1, 1, db.module_ids[name], 0, "EXECUTE", "G")
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps
    expected = {11} if operation == "inspect" else {11, 12}
    assert {item.principal_id for item in authority.writers} == expected


@pytest.mark.parametrize("control", ["plain_table", "disabled_trigger", "no_action_fk", "disabled_cascade"])
def test_unrelated_table_dml_without_a_reachable_indirect_effect_is_not_an_app_writer(control):
    db = sql_path("table_trigger")
    if control == "disabled_trigger":
        db.sql_triggers = [(SHADOW_TRIGGER, 1, db.object_ids["business_orders"], True)]
    else:
        db.sql_triggers.clear()
    if control in {"no_action_fk", "disabled_cascade"}:
        db.fks.append((
            "dbo", db.table_name("incidents"), "incident_id", "dbo", "business_orders", "order_id",
            1 if control == "disabled_cascade" else 0, 1 if control == "disabled_cascade" else 0,
            control == "disabled_cascade",
        ))
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps
    assert {item.principal_id for item in authority.writers} == {11}


def test_cascade_metadata_changes_the_authority_snapshot_even_when_disabled():
    db = sql_path("table_trigger")
    db.sql_triggers.clear()
    with db.transaction():
        before = read_authority(db, TARGET, db.catalogue)
    db.fks.append((
        "dbo", db.table_name("incidents"), "incident_id", "dbo", "business_orders", "order_id", 1, 0, True,
    ))
    with db.transaction():
        after = read_authority(db, TARGET, db.catalogue)
    assert not before.gaps and not after.gaps
    assert before.snapshot_hash != after.snapshot_hash


class WorkflowSources(InventorySources):
    def __init__(self):
        super().__init__()
        self.add_timer()
        self.timer_enabled = True
        self.timer_uri = "https://relay.example.invalid/forward-to-controller"
        self.audience = "https://ai.azure.com"
        self.method = "POST"
        self.auth_type = "ManagedServiceIdentity"
        self.secure_audience = False
        self.headers = {}

    def handle(self, request):
        response = super().handle(request)
        if request.url.path != TIMER:
            return response
        body = response.json()
        definition = body["properties"]["definition"]
        definition["parameters"]["foundryAudience"]["defaultValue"] = self.audience
        if self.secure_audience:
            definition["parameters"]["foundryAudience"]["type"] = "SecureString"
        inputs = definition["actions"]["invoke"]["inputs"]
        inputs["method"] = self.method
        inputs["authentication"]["type"] = self.auth_type
        inputs["headers"] = self.headers
        return httpx.Response(200, json=body)


@pytest.mark.parametrize("enabled", [True, False])
def test_original_ai_audience_relay_never_becomes_an_unrelated_stopped_deployment(enabled):
    sources = WorkflowSources()
    sources.timer_enabled = enabled
    db, sources, collector, registry, _ = arrangement(sources=sources)
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    with pytest.raises(DeploymentError, match="(?i)(audience|relay|invocation|invoker)"):
        collector.collect(authority)
    with pytest.raises(DeploymentError):
        registry.prepare(binding_id="unresolved-relay-counterexample")
    assert any(request.url.path == TIMER for request in sources.requests)
    assert not any(request.url.host == "relay.example.invalid" for request in sources.requests)
    assert not db.statements and db.delete_calls == 0


@pytest.mark.parametrize("service", ["graph", "arm"])
@pytest.mark.parametrize("enabled", [True, False])
def test_explicit_read_only_service_audience_and_origin_can_be_unrelated(service, enabled):
    sources = WorkflowSources()
    sources.timer_enabled = enabled
    sources.method = "GET"
    if service == "graph":
        sources.audience = "https://graph.microsoft.com"
        sources.timer_uri = "https://graph.microsoft.com/v1.0/organization"
    else:
        sources.audience = "https://management.azure.com/"
        sources.timer_uri = "https://management.azure.com/subscriptions?api-version=2022-12-01"
    db, sources, collector, _, _ = arrangement(sources=sources)
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    capture = collector.collect(authority)
    assert not capture.gaps
    assert all(item.writer.resource_id != TIMER for item in capture.writers)
    assert not any(request.url.path == TIMER + "/runs" for request in sources.requests)
    assert not db.statements


@pytest.mark.parametrize("problem", [
    "opaque_audience", "opaque_url", "missing_audience", "empty_url", "unknown_audience",
    "secure_audience", "non_mi_auth", "relay_with_graph_audience", "graph_write",
    "graph_with_ai_audience", "host_header", "authorization_header", "method_override",
])
def test_unresolved_http_authentication_or_routing_cannot_establish_unrelatedness(problem):
    sources = WorkflowSources()
    sources.timer_uri = "https://graph.microsoft.com/v1.0/organization"
    sources.audience = "https://graph.microsoft.com"
    sources.method = "GET"
    if problem == "opaque_audience":
        sources.audience = "@concat('https://', triggerBody()['audience'])"
    elif problem == "opaque_url":
        sources.timer_uri = "@triggerBody()['destination']"
    elif problem == "missing_audience":
        sources.audience = None
    elif problem == "empty_url":
        sources.timer_uri = ""
    elif problem == "unknown_audience":
        sources.audience = "https://unknown.example.invalid"
    elif problem == "secure_audience":
        sources.secure_audience = True
    elif problem == "non_mi_auth":
        sources.auth_type = "Basic"
    elif problem == "relay_with_graph_audience":
        sources.timer_uri = "https://relay.example.invalid/forward-to-controller"
    elif problem == "graph_write":
        sources.method = "POST"
    elif problem == "graph_with_ai_audience":
        sources.audience = "https://ai.azure.com"
    else:
        name = {
            "host_header": "Host", "authorization_header": "Authorization",
            "method_override": "X-HTTP-Method-Override",
        }[problem]
        sources.headers = {name: "DO-NOT-READ-OR-LOG-THIS"}
    db, sources, collector, _, _ = arrangement(sources=sources)
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    with pytest.raises(DeploymentError) as failure:
        collector.collect(authority)
    assert "DO-NOT-READ-OR-LOG-THIS" not in str(failure.value)
    assert not db.statements


@pytest.mark.parametrize("enabled", [True, False])
def test_direct_foundry_invoker_requires_actual_run_state_not_just_a_known_audience(enabled):
    sources = WorkflowSources()
    sources.add_foundry()
    sources.timer_enabled = enabled
    sources.timer_uri = ENDPOINT + "/agents/controller/endpoint/protocols/openai/responses?api-version=v1"
    db = RegisteredSqlFake()
    db.sql_writer_principals.append((12, "E", UUID(CLIENT2).bytes_le, 0))
    db, sources, collector, registry, _ = arrangement(db=db, sources=sources)
    plan = registry.prepare(binding_id="actual-direct-invoker")
    timer = next(item for item in plan.capture.writers if item.writer.resource_id == TIMER)
    assert timer.state == ("running" if enabled else "disabled")
    assert any(request.url.path == TIMER + "/runs" for request in sources.requests)
    assert bool(plan.blockers) == enabled
    assert not db.statements
