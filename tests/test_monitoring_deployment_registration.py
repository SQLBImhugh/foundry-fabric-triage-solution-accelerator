from __future__ import annotations

import copy
import json
import re
import socket
from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from test_monitoring_reset import (
    IDENTITY_RESOURCE,
    ITEM,
    NOW,
    OLD_EPOCH,
    RUN,
    RUNTIME_CLIENT,
    RUNTIME_OBJECT,
    SITE,
    SUBSCRIPTION,
    TARGET,
    TENANT,
    WORKSPACE,
    LiveSources,
    TransactionalSqlFake,
    cli_args,
    execute,
    identity_document,
    settings_document,
)

from scripts import register_monitoring_writers as register
from scripts import reset_monitoring_state as reset
from triage.monitoring.deployment_authority import expected_modules, read_authority
from triage.monitoring.deployment_contracts import DeploymentError, DeploymentUncertain, fingerprint
from triage.monitoring.deployment_discovery import (
    ARM,
    AzureDeploymentDiscovery,
    DiscoveryThrottled,
)
from triage.monitoring.deployment_registry import (
    HEADER_COLUMNS,
    WRITER_COLUMNS,
    DeploymentRegistrationOperator,
    RegisteredDeploymentInventoryReader,
    RegistrationPlan,
    install_registration,
)
from triage.monitoring.deployment_schema import (
    DEFAULT_REGISTRATION_NAMES,
    RegistrationNames,
    declared_write_procedures,
    kernel_abi,
    kernel_contract_hash,
    native_module_hash,
    object_catalogue,
    schema_statements,
    unqualified,
)
from triage.monitoring.sql_permissions import build_permission_kernel, integration_contract
from triage.store.azure_sql import SqlUnavailable

ROOT = f"/providers/Microsoft.Management/managementGroups/{TENANT}"
CLIENT2, OBJECT2 = str(UUID(int=120)), str(UUID(int=121))
TIMER_CLIENT, TIMER_OBJECT = str(UUID(int=122)), str(UUID(int=123))
ACCOUNT = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/fixture/providers/Microsoft.CognitiveServices/accounts/foundry"
PROJECT = ACCOUNT + "/projects/fixture"
ENDPOINT = "https://fixture.services.ai.azure.com/api/projects/fixture"
TIMER = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/fixture/providers/Microsoft.Logic/workflows/timer"
CALLBACK = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/fixture/providers/Microsoft.Logic/workflows/callback"
CONNECTION = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/fixture/providers/Microsoft.Web/connections/callback-sql"
SQL_API = f"/subscriptions/{SUBSCRIPTION}/providers/Microsoft.Web/locations/westus/managedApis/sql"


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *_: pytest.fail("Operator tests must not use live networking"))


class RegisteredSqlFake(TransactionalSqlFake):
    """Actual row mutations, outer joins, uniqueness/FKs, rollback and lost commits."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fail_writer_at = None
        self.writer_inserts = 0
        self.fail_capture = False
        self.extra_native_objects = []
        self.module_execution_context = {}
        self.module_signatures = {}
        self.supply_default_grants = True

    def headers(self):
        return self.tables[self.table_name("deployment_registration")]

    def writer_rows(self):
        return self.tables[self.table_name("deployment_writers")]

    def query(self, sql, *params):
        assert self.active
        if self.reconciliation_unavailable:
            raise SqlUnavailable("Fixture unavailable after uncertain commit")
        if "deployment-registry:exists" in sql:
            name = params[0].split(".")[1]
            return [(self.object_ids.get(name) if name in self.tables else None,)]
        if "deployment-registry:clock" in sql:
            return [(self.clock.now,)]
        if "deployment-registry:identity" in sql:
            return [(self.server_identity, self.actual_database, self.database_id)]
        if "deployment-registry:install-control" in sql:
            return [(row["tenant_id"], row["schema_version"], row["maintenance"])
                    for row in self.tables[self.table_name("monitoring_control")]]
        if "deployment-registry:control" in sql:
            return [
                (row["tenant_id"], row["epoch"], row["maintenance"])
                for row in self.tables[self.table_name("monitoring_control")]
            ]
        if "deployment-registry:latest" in sql or "deployment-registry:request" in sql:
            rows = sorted(self.headers(), key=lambda item: item["revision"], reverse=True)
            if "deployment-registry:request" in sql:
                rows = [row for row in rows if row["registration_request_id"] == params[0]]
            return [tuple(row[key] for key in HEADER_COLUMNS) for row in rows]
        if "deployment-registry:capture" in sql:
            return [
                (row["request_id"], row["fingerprint"], row["payload"], row["recorded_at"])
                for row in self.tables[self.table_name("monitoring_receipts")]
                if (row["tenant_id"], row["operation"], row["request_hash"]) == params
            ]
        if "deployment-registry:projection" in sql:
            result = []
            rows = sorted((row for row in self.writer_rows() if row["revision"] == params[0]), key=lambda row: row["writer_id"])
            for row in rows:
                matches = [
                    principal for principal in self.sql_writer_principals
                    if row["expected_sql_sid"] is not None and principal[2] == row["expected_sql_sid"]
                ]
                for principal in matches or [(None, None, None, None)]:
                    result.append((*[row[key] for key in WRITER_COLUMNS], *principal[:3]))
            if not rows and any(row["revision"] == params[0] for row in self.headers()):
                result.append((None,) * (len(WRITER_COLUMNS) + 3))
            return result
        if "deployment-registry:install-object" in sql:
            name = unqualified(params[0])
            if name in self.tables:
                return [("U", None)]
            if name in self.module_ids:
                return [("V", self.native_module_hashes[name])]
            return []
        if "deployment-registry:install-columns" in sql:
            name = unqualified(params[0])
            return [tuple(row[:5]) for row in self.columns[name]]
        rows = super().query(sql, *params)
        if "deployment-authority:objects" in sql:
            rows = [
                (*row[:6], self.module_execution_context.get(row[0], row[6]), self.module_signatures.get(row[0], row[7]))
                for row in rows
            ] + self.extra_native_objects
        if "deployment-authority:permissions" in sql and not self.supply_default_grants:
            rows = self.extra_permissions
        return rows

    def execute(self, sql, *params):
        assert self.active
        if "deployment-registry:insert-header" in sql:
            self.statements.append((sql, params))
            row = dict(zip(HEADER_COLUMNS, params, strict=True))
            if any(
                existing["revision"] == row["revision"]
                or existing["registration_request_id"] == row["registration_request_id"]
                for existing in self.headers()
            ):
                raise SqlUnavailable("Fixture unique registration identity violation")
            self.headers().append(row)
            return 1
        if "deployment-registry:insert-writer" in sql:
            self.statements.append((sql, params))
            self.writer_inserts += 1
            if self.writer_inserts == self.fail_writer_at:
                raise SqlUnavailable("Fixture child mutation failure")
            row = dict(zip(("revision", *WRITER_COLUMNS), params, strict=True))
            assert any(header["revision"] == row["revision"] for header in self.headers())
            assert not any(
                existing["revision"] == row["revision"] and existing["writer_id"] == row["writer_id"]
                for existing in self.writer_rows()
            )
            if row["invokes_writer_id"] is not None:
                assert any(
                    existing["revision"] == row["revision"] and existing["writer_id"] == row["invokes_writer_id"]
                    for existing in self.writer_rows()
                )
            self.writer_rows().append(row)
            return 1
        if "deployment-registry:insert-capture" in sql:
            self.statements.append((sql, params))
            if self.fail_capture:
                raise SqlUnavailable("Fixture capture mutation failure")
            self.add("monitoring_receipts", **dict(zip(
                ("tenant_id", "epoch", "operation", "request_hash", "request_id", "fingerprint", "recorded_at", "payload"),
                params, strict=True,
            )))
            return 1
        if sql.startswith("CREATE TABLE"):
            name = reset._table_body(sql)[0]
            assert name not in self.tables and name in DEFAULT_REGISTRATION_NAMES.tables.values()
            self.statements.append((sql, params))
            self.tables[name] = []
            self.object_ids[name] = max(self.object_ids.values()) + 1
            return 0
        if sql.startswith("CREATE VIEW"):
            name = re.search(r"CREATE VIEW \[dbo\]\.\[(\w+)\]", sql)[1]
            assert name not in self.module_ids
            self.statements.append((sql, params))
            self.module_ids[name] = max(self.module_ids.values()) + 1
            self.native_module_hashes[name] = native_module_hash(sql)
            return 0
        return super().execute(sql, *params)


class InventorySources(LiveSources):
    def __init__(self):
        super().__init__()
        self.resources = [
            {"id": SITE, "type": "Microsoft.Web/sites"},
            {"id": IDENTITY_RESOURCE, "type": "Microsoft.ManagedIdentity/userAssignedIdentities"},
        ]
        self.sites = {SITE: {"state": "Stopped", "settings": settings_document(), "identity": identity_document()}}
        self.slots = {}
        self.identities = {RUNTIME_CLIENT: RUNTIME_OBJECT, CLIENT2: OBJECT2, TIMER_CLIENT: TIMER_OBJECT}
        self.agent_versions = [{"version": "1", "definition": {
            "kind": "hosted", "environment_variables": settings_document(client=CLIENT2),
            "container": {"image": "fixture/controller:1"},
        }}]
        self.agent_enabled = False
        self.timer_enabled = False
        self.timer_uri = ENDPOINT + "/agents/controller/endpoint/protocols/openai/responses?api-version=v1"
        self.timer_runs = []
        self.reverse_extra = []
        self.federation = []
        self.response_hook = None
        self.permissions = [{"actions": ["*/read"], "notActions": []}]
        self.denies = []
        self.callback = None
        self.connection_mode = "oauthMI"

    def add_site(self, resource=SITE + "-other", *, state="Stopped"):
        self.resources.append({"id": resource, "type": "Microsoft.Web/sites"})
        self.sites[resource] = {"state": state, "settings": settings_document(), "identity": identity_document()}
        return resource

    def add_foundry(self):
        self.resources.append({"id": ACCOUNT, "type": "Microsoft.CognitiveServices/accounts"})

    def add_timer(self):
        self.resources.append({"id": TIMER, "type": "Microsoft.Logic/workflows"})

    def add_sql_callback(self):
        self.resources.extend([
            {"id": CALLBACK, "type": "Microsoft.Logic/workflows"},
            {"id": CONNECTION, "type": "Microsoft.Web/connections"},
        ])
        self.callback = {
            "id": CALLBACK,
            "identity": {"type": "SystemAssigned", "principalId": OBJECT2, "tenantId": TENANT},
            "properties": {
                "state": "Disabled",
                "parameters": {
                    "sqlServer": {"value": TARGET.server}, "sqlDatabase": {"value": TARGET.database},
                    "$connections": {"value": {"sql": {
                        "connectionId": CONNECTION, "id": SQL_API,
                        "connectionProperties": {"authentication": {"type": "ManagedServiceIdentity"}},
                    }}},
                },
                "definition": {
                    "parameters": {"sqlServer": {"type": "String"}, "sqlDatabase": {"type": "String"}, "$connections": {"type": "Object"}},
                    "actions": {"record": {"type": "ApiConnection", "inputs": {
                        "method": "post",
                        "host": {"connection": {"name": "@parameters('$connections')['sql']['connectionId']"}},
                        "path": "/v2/datasets/@{encodeURIComponent(encodeURIComponent(parameters('sqlServer')))},"
                                "@{encodeURIComponent(encodeURIComponent(parameters('sqlDatabase')))}/procedures/"
                                "@{encodeURIComponent(encodeURIComponent('[dbo].[triage_record_approval_decision]'))}",
                        "body": {"reason": "PRIVATE-REASON-NOT-IN-CAPTURE"},
                    }}},
                },
            },
        }

    def handle(self, request):
        assert request.method == "GET" or request.method == "POST" and request.url.path.endswith(
            ("/config/appsettings/list", "/listAssociatedResources"),
        ), "No operator test may issue a mutating cloud request"
        self.requests.append(request)
        if self.on_read:
            self.on_read(request)
        if self.response_hook:
            response = self.response_hook(request)
            if response is not None:
                return response
        path = request.url.path
        if path == ROOT:
            body = {"id": ROOT, "properties": {"tenantId": TENANT}}
        elif path == ROOT + "/descendants":
            body = {"value": [{
                "id": f"/subscriptions/{SUBSCRIPTION}", "type": "Microsoft.Management/managementGroups/subscriptions",
                "name": SUBSCRIPTION, "properties": {"parent": {"id": ROOT}},
            }]}
        elif path == f"/subscriptions/{SUBSCRIPTION}":
            body = {"subscriptionId": SUBSCRIPTION, "tenantId": TENANT, "state": "Enabled"}
        elif path.endswith("/providers/Microsoft.Authorization/permissions"):
            body = {"value": self.permissions}
        elif path.endswith("/providers/Microsoft.Authorization/denyAssignments"):
            body = {"value": self.denies}
        elif path == f"/subscriptions/{SUBSCRIPTION}/resources":
            body = {"value": self.resources}
        elif path.startswith("/v1.0/servicePrincipals"):
            client = next((client for client in self.identities if client in path), None)
            if client is None:
                client = next((client for client, oid in self.identities.items() if oid in path), None)
            assert client is not None
            body = {
                "id": self.identities[client], "appId": client, "servicePrincipalType": "ManagedIdentity",
                "appOwnerOrganizationId": TENANT, "passwordCredentials": [], "keyCredentials": [],
            }
        elif path == IDENTITY_RESOURCE:
            body = {"id": path, "properties": {"clientId": RUNTIME_CLIENT, "principalId": RUNTIME_OBJECT, "tenantId": TENANT}}
        elif path == IDENTITY_RESOURCE + "/federatedIdentityCredentials":
            body = {"value": self.federation}
        elif path == IDENTITY_RESOURCE + "/listAssociatedResources":
            associated = [
                {"id": resource} for resource, site in self.sites.items()
                if IDENTITY_RESOURCE in site["identity"].get("userAssignedIdentities", {})
            ] + [{"id": resource} for resource in self.reverse_extra]
            body = {"value": associated, "totalCount": len(associated)}
        elif path in self.sites:
            site = self.sites[path]
            body = {"id": path, "identity": site["identity"], "properties": {"state": site["state"]}}
        elif path.endswith("/config/appsettings/list"):
            site = path.removesuffix("/config/appsettings/list")
            body = {"properties": self.sites[site]["settings"] | {"UNRELATED_SECRET": "DO-NOT-PERSIST-THIS"}}
        elif path.endswith("/slots"):
            body = {"value": [{"id": resource} for resource in self.slots.get(path.removesuffix("/slots"), [])]}
        elif path == ACCOUNT:
            body = {"id": path, "kind": "AIServices", "properties": {}}
        elif path == ACCOUNT + "/projects":
            body = {"value": [{"id": PROJECT, "properties": {"endpoints": {"AI Foundry API": ENDPOINT}}}]}
        elif path == "/api/projects/fixture/agents":
            body = {"data": [{"name": "controller"}], "has_more": False}
        elif path == "/api/projects/fixture/agents/controller":
            body = {
                "name": "controller", "status": "Enabled" if self.agent_enabled else "Disabled",
                "instance_identity": {"client_id": CLIENT2, "principal_id": OBJECT2},
            }
        elif path == "/api/projects/fixture/agents/controller/versions":
            body = {"data": self.agent_versions, "has_more": False}
        elif path == TIMER:
            body = {
                "id": path,
                "identity": {"type": "SystemAssigned", "principalId": TIMER_OBJECT, "tenantId": TENANT},
                "properties": {
                    "state": "Enabled" if self.timer_enabled else "Disabled",
                    "parameters": {"agentUri": {"value": self.timer_uri}},
                    "definition": {
                        "parameters": {"foundryAudience": {"type": "String", "defaultValue": "https://ai.azure.com"}},
                        "triggers": {"tick": {"type": "Recurrence"}},
                        "actions": {"invoke": {"type": "Http", "inputs": {
                            "uri": "@parameters('agentUri')", "method": "POST",
                            "authentication": {"type": "ManagedServiceIdentity", "audience": "@parameters('foundryAudience')"},
                        }}},
                    },
                },
            }
        elif path == TIMER + "/runs":
            body = {"value": self.timer_runs}
        elif path == CALLBACK and self.callback is not None:
            body = self.callback
        elif path == CALLBACK + "/runs":
            body = {"value": []}
        elif path == CONNECTION:
            body = {"id": CONNECTION, "properties": {
                "api": {"id": SQL_API}, "parameterValueSet": {"name": self.connection_mode, "values": {}},
            }}
        elif path.endswith("/jobs/instances"):
            body = {"value": self.history}
        elif path.endswith(f"/jobs/instances/{RUN}"):
            body = {"id": RUN, "itemId": ITEM, "status": self.run_status, "endTimeUtc": NOW.isoformat()}
        elif path.endswith("/refreshes"):
            body = {"value": self.powerbi_history}
        else:
            pytest.fail(f"Unexpected offline operator route: {request.method} {path}")
        return httpx.Response(200, json=body)


def arrangement(*, db=None, sources=None):
    db, sources = db or RegisteredSqlFake(), sources or InventorySources()
    transport = httpx.MockTransport(sources.handle)
    collector = AzureDeploymentDiscovery(
        db._credential, TARGET, transport=transport, allow_identity_association_preview=True,
        clock=lambda: db.clock.now, monotonic=lambda: 0.0, sleep=lambda _: None,
    )

    def observer_factory(profile, credential):
        return reset.LiveOperatorObserver(profile, credential, transport=transport, clock=lambda: db.clock.now)

    reset_operator = reset.SqlResetOperator(db, TARGET)
    registry = DeploymentRegistrationOperator(
        db, TARGET, db.catalogue, collector,
        verify_quiescence=register.live_quiescence_guard(reset_operator, observer_factory=observer_factory),
    )
    return db, sources, collector, registry, observer_factory


def accepted(**kwargs):
    db, sources, collector, registry, observers = arrangement(**kwargs)
    plan = registry.prepare(binding_id="fixture-deployment")
    result = registry.accept(plan, confirmed_manifest_hash=plan.manifest_hash)
    return db, sources, collector, registry, observers, plan, result


def reset_with_reader(db, collector, observers, *, profile=None):
    reader = RegisteredDeploymentInventoryReader(collector)
    operator = reset.SqlResetOperator(
        db, TARGET, deployment_inventory_reader=reader,
        observer=observers(profile, db._credential) if profile else None,
    )
    plan = operator.plan()
    if profile is None:
        operator.observer = observers(plan.preflight_profile, db._credential)
    return operator, plan


def test_preparation_is_read_only_and_does_not_store_general_settings():
    db, sources, _, registry, _ = arrangement()
    before = copy.deepcopy(db.tables)
    plan = registry.prepare(binding_id="fixture-deployment")
    assert not plan.blockers and not db.statements and db.tables == before
    assert "DO-NOT-PERSIST-THIS" not in plan.model_dump_json()
    assert {writer.identity_client_id for writer in plan.capture.writers} == {RUNTIME_CLIENT}
    assert any(request.url.path == ROOT + "/descendants" for request in sources.requests)
    assert any(request.url.path.endswith("/listAssociatedResources") for request in sources.requests)


def test_actual_reader_single_operator_quiet_reset_preserves_registry_capture_and_new_epoch_rows():
    db, sources, collector, registry, observers, registration_plan, result = accepted()
    headers, writers = copy.deepcopy(db.headers()), copy.deepcopy(db.writer_rows())
    capture = copy.deepcopy(db.tables[db.table_name("monitoring_receipts")])
    operator, plan = reset_with_reader(db, collector, observers)
    receipt = execute(operator, plan).receipt
    assert db.headers() == headers and db.writer_rows() == writers
    assert all(row in db.tables[db.table_name("monitoring_receipts")] for row in capture)
    assert receipt.new_control.epoch != OLD_EPOCH and db.delete_calls == len(db.catalogue.deletion_order())
    assert db.tables["business_orders"] and db.tables[db.table_name("rate_budget")][0]["used"] == 12
    db.add("incidents", incident_id="new-release", payload="new state must survive")
    before, calls, writes = copy.deepcopy(db.tables), len(sources.requests), len(db.statements)
    assert execute(operator, plan).replayed
    assert registry.accept(registration_plan, confirmed_manifest_hash=registration_plan.manifest_hash).receipt == result.receipt
    assert db.tables == before and len(sources.requests) == calls and len(db.statements) == writes


def test_real_reader_rejects_original_omission_and_misbinding_counterexample_with_zero_deletes():
    db = RegisteredSqlFake()
    model = str(UUID(int=900))
    db.add("monitoring_records", tenant_id=TENANT, epoch=OLD_EPOCH, record_kind="target",
           full_key="registered-model", workload="powerbi", workspace_id=WORKSPACE, item_id=model,
           payload=json.dumps({"identity": {
               "tenant_id": TENANT, "epoch": OLD_EPOCH, "workload": "powerbi", "workspace_id": WORKSPACE, "item_id": model,
           }}))
    db, sources, collector, _, observers, _, _ = accepted(db=db)
    sources.powerbi_history = [{"requestId": RUN, "status": "Unknown", "endTime": None}]
    unrelated = SITE + "-unrelated"
    sources.resources.append({"id": unrelated, "type": "Microsoft.Web/sites"})
    sources.sites[unrelated] = {
        "state": "Stopped", "settings": settings_document(database="another_database"), "identity": {"type": "None"},
    }
    wrong = reset.ObservationProfile(
        target=TARGET, writers=(reset.WriterSpec(writer_id="caller-list", kind="app_service", resource_id=unrelated),),
        action_targets=(reset.ActionTarget(workload="fabric_pipeline", workspace_id=WORKSPACE, item_id=ITEM),),
    )
    # The unrelated resource has no SQL identity; it cannot stand in for SITE.
    original = sources.handle

    def handle(request):
        if request.url.path == IDENTITY_RESOURCE + "/listAssociatedResources":
            return httpx.Response(200, json={"value": [{"id": SITE}], "totalCount": 1})
        return original(request)

    collector.http.close()
    collector.http = httpx.Client(transport=httpx.MockTransport(handle))
    operator, manifest = reset_with_reader(db, collector, observers, profile=wrong)
    before = copy.deepcopy(db.tables)
    with pytest.raises(reset.ResetRefused, match="SQL-registered"):
        execute(operator, manifest)
    assert db.delete_calls == 0 and db.tables == before
    # Covering the actual SQL target now reads and refuses its active refresh.
    operator, manifest = reset_with_reader(db, collector, observers)
    with pytest.raises(reset.ResetRefused, match="active or unverified refresh"):
        execute(operator, manifest)
    assert any(f"/datasets/{model}/refreshes" in request.url.path for request in sources.requests)
    assert db.delete_calls == 0


@pytest.mark.parametrize("change", ["database", "identity", "extra_writer", "active_slot", "missing_slot"])
def test_fresh_reader_detects_binding_and_identity_reuse_drift(change):
    sources = InventorySources()
    slot = SITE + "/slots/staging"
    sources.slots[SITE] = [slot]
    sources.sites[slot] = {"state": "Stopped", "settings": settings_document(), "identity": identity_document()}
    db, sources, collector, _, _, _, _ = accepted(sources=sources)
    if change == "database":
        sources.sites[SITE]["settings"]["AZURE_SQL_DATABASE"] = "different"
    elif change == "identity":
        sources.identities[RUNTIME_CLIENT] = str(UUID(int=999))
    elif change == "extra_writer":
        sources.add_site()
    elif change == "active_slot":
        sources.sites[slot]["state"] = "Running"
    else:
        sources.slots[SITE] = []
    reader = RegisteredDeploymentInventoryReader(collector)
    with db.transaction(), pytest.raises(DeploymentError):
        reader.read(db, TARGET, db.catalogue)
    assert not db.delete_calls


@pytest.mark.parametrize("tamper", ["child", "capture", "latest_target", "second_series", "missing_child", "duplicate_sid", "stale"])
def test_protected_registration_integrity_and_latest_global_revision(tamper):
    db, _, collector, _, _, _, _ = accepted()
    if tamper == "child":
        db.writer_rows()[0]["configured_sql_database"] = "forged"
    elif tamper == "capture":
        db.tables[db.table_name("monitoring_receipts")][0]["payload"] = "{}"
    elif tamper in {"latest_target", "second_series"}:
        row = copy.deepcopy(db.headers()[0])
        row.update(revision=2, registration_request_id=str(UUID(int=901)))
        row["sql_database" if tamper == "latest_target" else "binding_id"] = "different"
        db.headers().append(row)
    elif tamper == "missing_child":
        db.writer_rows().clear()
    elif tamper == "duplicate_sid":
        db.sql_writer_principals.append((12, "E", UUID(RUNTIME_CLIENT).bytes_le, 0))
    else:
        db.clock.advance(3_601)
    with db.transaction(), pytest.raises(DeploymentError):
        RegisteredDeploymentInventoryReader(collector).read(db, TARGET, db.catalogue)
    assert not db.delete_calls


@pytest.mark.parametrize("path", [
    "database", "schema", "column", "grant_option", "module", "execute_as", "signed", "changed_definition",
    "authority_view", "control_view", "implicit_module_owner", "broker", "view_trigger", "ddl_trigger", "activation",
])
def test_actual_sql_authority_refuses_registry_and_module_bypasses(path):
    db = RegisteredSqlFake()
    registry_id = db.object_ids[db.table_name("deployment_writers")]
    if path in {"database", "schema", "column", "grant_option"}:
        scope = {"database": (0, 0, 0), "schema": (3, 1, 0), "column": (1, registry_id, 4), "grant_option": (1, registry_id, 0)}[path]
        db.extra_permissions.append((11, 1, *scope, "UPDATE", "W" if path == "grant_option" else "G"))
    elif path == "module":
        db.extra_native_objects.append((999, "dbo", "unrelated_proc_with_hidden_write", "P", 1, "a" * 64, None, 0))
        db.extra_permissions.append((11, 1, 1, 999, 0, "EXECUTE", "G"))
    elif path in {"authority_view", "control_view"}:
        name = DEFAULT_REGISTRATION_NAMES.read_projection if path == "authority_view" else unqualified(
            build_permission_kernel().names.object("control_read"),
        )
        db.extra_permissions.append((11, 1, 1, db.module_ids[name], 1, "UPDATE", "G"))
    elif path == "implicit_module_owner":
        db.extra_native_objects.append((999, "dbo", "owned_unsafe_proc", "P", 11, "a" * 64, -2, 0))
    elif path == "broker":
        db.extra_permissions.append((11, 1, 17, 999, 0, "SEND", "G"))
    elif path == "view_trigger":
        name = unqualified(build_permission_kernel().names.object("worker_catalogue"))
        db.sql_triggers = [(999, 1, db.module_ids[name], False)]
    elif path == "ddl_trigger":
        db.sql_triggers = [(999, 0, 0, False)]
    elif path == "activation":
        db.sql_queues = [(999, True, True, "[dbo].[unknown_writer]", -2)]
    else:
        name = db.catalogue.procedures[0]
        if path == "execute_as":
            db.module_execution_context[db.module_ids[name]] = -2
        elif path == "signed":
            db.module_signatures[db.module_ids[name]] = 1
        else:
            db.native_module_hashes[name] = "0" * 64
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert authority.gaps
    with pytest.raises(DeploymentError):
        authority.require_protected()
    assert not db.statements


def test_legacy_role_preparation_is_not_protected_acceptance_or_a_circular_bootstrap():
    db = RegisteredSqlFake()
    db.role_ids["db_datawriter"] = 700
    db.extra_role_edges = [(11, 700)]
    db, _, _, registry, _ = arrangement(db=db)
    blocked = registry.prepare(binding_id="fixture-deployment")
    assert "legacy_or_privileged_role" in blocked.blockers
    with pytest.raises(DeploymentError, match="blocked"):
        registry.accept(blocked, confirmed_manifest_hash=blocked.manifest_hash)
    assert not db.headers()
    db.extra_role_edges.clear()
    ready = registry.prepare(binding_id="fixture-deployment")
    assert not ready.blockers
    assert registry.accept(ready, confirmed_manifest_hash=ready.manifest_hash).receipt.revision == 1


@pytest.mark.parametrize("operation", ["inspect", "lock_context", "controller.inspect_frontiers"])
def test_transaction_required_read_rpcs_do_not_create_persistent_writers(operation):
    db = RegisteredSqlFake()
    rpc = build_permission_kernel().rpcs[operation]
    db.supply_default_grants = False
    db.extra_permissions = [(11, 1, 1, db.module_ids[unqualified(rpc.object_name)], 0, "EXECUTE", "G")]
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps and not authority.writers


def test_transitive_role_permission_and_checked_view_are_in_authority_universe():
    db = RegisteredSqlFake()
    db.supply_default_grants = False
    db.role_ids.update({"outer_role": 800, "inner_role": 801})
    db.extra_role_edges = [(11, 800), (800, 801)]
    view = next(obj for obj in build_permission_kernel().objects if obj.logical_name == "worker_catalogue")
    db.extra_permissions = [(801, 1, 1, db.module_ids[unqualified(view.name)], 4, "UPDATE", "G")]
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps and [writer.principal_id for writer in authority.writers] == [11]


@pytest.mark.parametrize("fault", ["child", "capture", "committed", "rolled_back", "committed_unreadable"])
def test_registration_atomic_rollback_and_original_request_reconciliation(fault):
    db, _, _, registry, _ = arrangement()
    plan = registry.prepare(binding_id="fixture-deployment")
    before = copy.deepcopy(db.tables)
    if fault == "child":
        db.fail_writer_at = 1
    elif fault == "capture":
        db.fail_capture = True
    else:
        db.commit_fault = fault
    if fault == "committed":
        assert registry.accept(plan, confirmed_manifest_hash=plan.manifest_hash).reconciled_uncertain_commit
    else:
        with pytest.raises((SqlUnavailable, DeploymentUncertain)):
            registry.accept(plan, confirmed_manifest_hash=plan.manifest_hash)
    writes = len(db.statements)
    if fault in {"child", "capture", "rolled_back"}:
        assert db.tables == before
        with pytest.raises(DeploymentUncertain):
            registry.reconcile(plan)
    else:
        db.reconciliation_unavailable = False
        assert registry.reconcile(plan).replayed
    assert len(db.statements) == writes and not db.delete_calls


def test_wrong_operator_database_and_changed_manifest_never_append():
    db, _, _, registry, _ = arrangement()
    plan = registry.prepare(binding_id="fixture-deployment")
    with pytest.raises(DeploymentError):
        registry.accept(plan, confirmed_manifest_hash="0" * 64)
    db.actual_database = "different"
    with pytest.raises(DeploymentError):
        registry.accept(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert not db.headers() and not db.statements


def test_exact_prior_revision_cas_and_request_reuse():
    db, _, _, registry, _, original, _ = accepted()
    planned = registry.prepare(binding_id="fixture-deployment")
    result = registry.accept(planned, confirmed_manifest_hash=planned.manifest_hash)
    assert result.receipt.revision == 2
    assert registry.accept(original, confirmed_manifest_hash=original.manifest_hash).replayed
    reused = original.model_copy(update={"binding_id": "different"})
    with pytest.raises(DeploymentError):
        registry.accept(reused, confirmed_manifest_hash=reused.manifest_hash)
    assert len(db.headers()) == 2


def test_foundry_versions_and_actual_disabled_system_identity_invoker_close_the_graph():
    db = RegisteredSqlFake()
    db.sql_writer_principals.append((12, "E", UUID(CLIENT2).bytes_le, 0))
    sources = InventorySources()
    sources.add_foundry()
    sources.add_timer()
    db, sources, collector, _, observers, _, _ = accepted(db=db, sources=sources)
    operator, plan = reset_with_reader(db, collector, observers)
    assert {item.writer.kind for item in plan.deployment_inventory.writers} == {"app_service", "foundry_agent", "logic_app"}
    indirect = next(row for row in db.writer_rows() if row["writer_kind"] == "logic_app")
    assert indirect["expected_sql_sid"] is None and indirect["invokes_writer_id"]
    assert execute(operator, plan).receipt.state == "completed"


def test_actual_oauthmi_sql_connection_action_is_bound_without_reading_or_saving_a_secret():
    db = RegisteredSqlFake()
    db.sql_writer_principals.append((12, "E", UUID(CLIENT2).bytes_le, 0))
    sources = InventorySources()
    sources.add_sql_callback()
    db, sources, collector, _, observers, registration, _ = accepted(db=db, sources=sources)
    assert "PRIVATE-REASON-NOT-IN-CAPTURE" not in registration.model_dump_json()
    assert any(request.url.path == CONNECTION for request in sources.requests)
    rows = [row for row in db.writer_rows() if row["writer_kind"] == "logic_app"]
    assert len(rows) == 1 and rows[0]["expected_sql_sid"] == UUID(CLIENT2).bytes_le
    operator, manifest = reset_with_reader(db, collector, observers)
    assert execute(operator, manifest).receipt.state == "completed"


@pytest.mark.parametrize("change", ["database", "credential", "procedure", "expression", "identity"])
def test_sql_connector_drift_remains_an_explicit_binding_failure(change):
    db = RegisteredSqlFake()
    db.sql_writer_principals.append((12, "E", UUID(CLIENT2).bytes_le, 0))
    sources = InventorySources()
    sources.add_sql_callback()
    db, sources, collector, _, _, _, _ = accepted(db=db, sources=sources)
    properties = sources.callback["properties"]
    if change == "database":
        properties["parameters"]["sqlDatabase"]["value"] = "different_database"
    elif change == "credential":
        sources.connection_mode = "sqlAuthentication"
    elif change == "procedure":
        action = properties["definition"]["actions"]["record"]["inputs"]
        action["path"] = action["path"].replace("triage_record_approval_decision", "unrelated_business_procedure")
    elif change == "expression":
        properties["definition"]["actions"]["record"]["inputs"]["path"] = "@triggerBody()['sqlPath']"
    else:
        properties["parameters"]["$connections"]["value"]["sql"]["connectionProperties"]["authentication"]["identity"] = IDENTITY_RESOURCE
    with db.transaction(), pytest.raises(DeploymentError):
        RegisteredDeploymentInventoryReader(collector).read(db, TARGET, db.catalogue)
    assert not db.delete_calls


@pytest.mark.parametrize("change", ["enabled_agent", "new_version", "dynamic_invocation", "active_timer", "active_timer_run"])
def test_foundry_and_invoker_drift_is_not_hidden_by_a_stopped_parent(change):
    db = RegisteredSqlFake()
    db.sql_writer_principals.append((12, "E", UUID(CLIENT2).bytes_le, 0))
    sources = InventorySources()
    sources.add_foundry()
    sources.add_timer()
    db, sources, collector, _, _, _, _ = accepted(db=db, sources=sources)
    if change == "enabled_agent":
        sources.agent_enabled = True
    elif change == "new_version":
        sources.agent_versions.append({"version": "2", "definition": {"kind": "hosted", "environment_variables": {}}})
    elif change == "dynamic_invocation":
        sources.timer_uri = "@triggerBody()['endpoint']"
    elif change == "active_timer":
        sources.timer_enabled = True
    else:
        sources.timer_runs = [{"id": TIMER + "/runs/one", "properties": {"status": "Running"}}]
    with db.transaction(), pytest.raises(DeploymentError):
        RegisteredDeploymentInventoryReader(collector).read(db, TARGET, db.catalogue)
    assert not db.delete_calls


@pytest.mark.parametrize("failure", ["401", "403", "429", "partial", "wrong_tenant", "foreign_host", "foreign_path", "missing_total", "federation", "denied", "caller_visible"])
def test_bounded_http_inventory_never_treats_unavailable_metadata_as_empty(failure):
    db, sources, _, registry, _ = arrangement()

    def broken(request):
        if request.url.path != ROOT + "/descendants":
            if failure == "missing_total" and request.url.path.endswith("/listAssociatedResources"):
                return httpx.Response(200, json={"value": [{"id": SITE}]})
            return None
        if failure in {"401", "403", "429"}:
            return httpx.Response(int(failure), headers={"Retry-After": "17"})
        if failure == "partial":
            return httpx.Response(200, json={})
        if failure == "wrong_tenant":
            return httpx.Response(200, json={"tenantId": str(UUID(int=888)), "value": []})
        if failure in {"foreign_host", "foreign_path"}:
            url = "https://untrusted.example/next" if failure == "foreign_host" else ARM + ROOT + "/other?api-version=2020-05-01"
            return httpx.Response(200, json={"value": [], "nextLink": url})
        return None

    sources.response_hook = broken
    if failure == "federation":
        sources.federation = [{"id": IDENTITY_RESOURCE + "/federatedIdentityCredentials/external"}]
    elif failure == "denied":
        sources.denies = [{"id": "one"}]
    elif failure == "caller_visible":
        sources.permissions = [{"actions": ["Microsoft.Resources/subscriptions/resources/read"], "notActions": []}]
    with pytest.raises(DeploymentError) as error:
        registry.prepare(binding_id="fixture-deployment")
    if failure == "429":
        assert isinstance(error.value, DiscoveryThrottled) and error.value.retry_after_seconds == 17
    assert not db.headers() and not db.statements


def test_multi_page_scope_resource_and_reverse_identity_reads_are_exhausted():
    db, sources, _, registry, _ = arrangement()
    other = sources.add_site()
    counts = {}

    def pages(request):
        path = request.url.path
        if path not in {f"/subscriptions/{SUBSCRIPTION}/resources", IDENTITY_RESOURCE + "/listAssociatedResources"}:
            return None
        counts[path] = counts.get(path, 0) + 1
        second = request.url.params.get("$skiptoken") == "two"
        if path.endswith("/resources"):
            values = sources.resources[1:] if second else sources.resources[:1]
            body = {"value": values}
        else:
            body = {"value": [{"id": other if second else SITE}], "totalCount": 2}
        if not second:
            body["nextLink"] = str(request.url.copy_add_param("$skiptoken", "two"))
        return httpx.Response(200, json=body)

    sources.response_hook = pages
    plan = registry.prepare(binding_id="fixture-deployment")
    assert len(plan.capture.writers) == 2 and set(counts.values()) == {2}
    assert not db.statements


def test_explicit_install_is_separate_from_acceptance_and_retains_existing_rows():
    db = RegisteredSqlFake()
    names = DEFAULT_REGISTRATION_NAMES
    for name in names.tables.values():
        del db.tables[name]
    del db.module_ids[names.read_projection]
    catalogue = object_catalogue(names)
    ddl_hash = fingerprint(catalogue, domain="deployment.registration.ddl.v1")
    with pytest.raises(DeploymentError):
        install_registration(db, db.catalogue, confirmed_ddl_hash="0" * 64)
    assert not db.statements
    original = copy.deepcopy(db.tables)
    install_registration(db, db.catalogue, confirmed_ddl_hash=ddl_hash)
    assert all(db.tables[name] == rows for name, rows in original.items())
    db.headers().append({"operator_marker": "new row"})
    before, writes = copy.deepcopy(db.tables), len(db.statements)
    install_registration(db, db.catalogue, confirmed_ddl_hash=ddl_hash)
    assert db.tables == before and len(db.statements) == writes
    assert not db.delete_calls


def test_registration_install_refuses_another_baseline_tenant_before_any_ddl():
    db = RegisteredSqlFake()
    for name in DEFAULT_REGISTRATION_NAMES.tables.values():
        del db.tables[name]
    del db.module_ids[DEFAULT_REGISTRATION_NAMES.read_projection]
    db.tables[db.table_name("monitoring_control")][0]["tenant_id"] = str(UUID(int=998))
    ddl_hash = fingerprint(object_catalogue(), domain="deployment.registration.ddl.v1")
    with pytest.raises(DeploymentError, match="exact tenant"):
        install_registration(db, db.catalogue, confirmed_ddl_hash=ddl_hash)
    assert not db.statements


@pytest.mark.parametrize("fault", ["committed", "rolled_back"])
def test_registration_ddl_uncertainty_is_reconciled_by_exact_objects_without_wiping_rows(fault):
    db = RegisteredSqlFake()
    names = DEFAULT_REGISTRATION_NAMES
    for name in names.tables.values():
        del db.tables[name]
    del db.module_ids[names.read_projection]
    before, modules = copy.deepcopy(db.tables), copy.deepcopy(db.module_ids)
    ddl_hash = fingerprint(object_catalogue(names), domain="deployment.registration.ddl.v1")
    db.commit_fault = fault
    with pytest.raises(SqlUnavailable):
        install_registration(db, db.catalogue, confirmed_ddl_hash=ddl_hash)
    assert all(db.tables[name] == rows for name, rows in before.items())
    if fault == "rolled_back":
        assert db.tables == before and db.module_ids == modules
    else:
        db.headers().append({"post_install": "preserve on operator readback"})
        snapshot, writes = copy.deepcopy(db.tables), len(db.statements)
        install_registration(db, db.catalogue, confirmed_ddl_hash=ddl_hash)
        assert db.tables == snapshot and len(db.statements) == writes
    assert not db.delete_calls


@pytest.mark.parametrize("hazard", ["lease", "uncertain_submission", "running_submission", "active_refresh"])
def test_real_acceptance_guard_requires_actual_sql_and_external_effect_quiescence(hazard):
    db, sources, _, registry, _ = arrangement()
    if hazard == "lease":
        db.add("claims", claim_key="active-owner", expires_at=NOW + timedelta(seconds=30))
    elif hazard in {"uncertain_submission", "running_submission"}:
        db.add("pipeline_reruns", run_key="known-intent", state="unknown", workspace_id=WORKSPACE, pipeline_id=ITEM,
               payload=json.dumps({"rerun_id": RUN if hazard == "running_submission" else ""}))
        sources.run_status = "InProgress"
    else:
        db.add("monitoring_records", record_kind="target", tenant_id=TENANT, epoch=OLD_EPOCH,
               full_key="model", workload="powerbi", workspace_id=WORKSPACE, item_id=ITEM,
               payload=json.dumps({"identity": {
                   "tenant_id": TENANT, "epoch": OLD_EPOCH, "workload": "powerbi",
                   "workspace_id": WORKSPACE, "item_id": ITEM,
               }}))
        sources.powerbi_history = [{"requestId": RUN, "status": "Unknown", "endTime": None}]
    plan = registry.prepare(binding_id="fixture-deployment")
    with pytest.raises((DeploymentError, reset.ResetRefused)):
        registry.accept(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert not db.headers() and not db.writer_rows() and not db.delete_calls


def test_actual_reader_reset_lost_ack_preserves_rows_written_by_new_release():
    db, sources, collector, _, observers, _, _ = accepted()
    operator, manifest = reset_with_reader(db, collector, observers)
    db.commit_fault = "committed"

    def write_after_commit(fixture):
        fixture.after_commit = None
        fixture.add("incidents", incident_id="new-epoch-data", payload="must survive reconciliation")

    db.after_commit = write_after_commit
    result = execute(operator, manifest)
    assert result.reconciled_uncertain_commit
    before, writes = copy.deepcopy(db.tables), len(db.statements)
    sources.sites[SITE]["state"] = "Running"
    assert execute(operator, manifest).replayed
    assert db.tables == before and len(db.statements) == writes


def test_registration_names_do_not_change_kernel_namespace_and_native_hash_domains_are_distinct():
    names = RegistrationNames(registration="app_registration", writers="app_writers", read_projection="app_authority")
    catalogue = reset.build_catalogue(names)
    assert catalogue.kernel_hash == kernel_contract_hash() == fingerprint(kernel_abi())
    assert {table.name for table in catalogue.tables if table.operation == "preserve_registration"} == set(names.tables.values())
    declaration = object_catalogue(names)[2]
    assert declaration["native_sha256"] != declaration["sha256"]
    assert "GRANT" not in "\n".join(schema_statements(names))
    with pytest.raises(ValueError):
        replace(names, writers="dbo.anything; DROP DATABASE x")


def test_registration_abi_includes_the_backend_read_and_write_route_contracts():
    abi, backend = kernel_abi(), integration_contract()
    assert abi["read_routes"] == backend["read_routes"]
    assert abi["write_routes"] == backend["write_routes"]
    prior_incomplete = {key: value for key, value in abi.items() if key not in {"read_routes", "write_routes"}}
    assert fingerprint(prior_incomplete) != kernel_contract_hash()
    for routes in ("read_routes", "write_routes"):
        changed = copy.deepcopy(abi)
        key = next(iter(changed[routes]))
        changed[routes][key] += "_changed"
        assert fingerprint(changed) != kernel_contract_hash()
    assert not set(DEFAULT_REGISTRATION_NAMES.tables) & set(abi["table_map"])


def test_connector_publication_is_declared_as_a_controller_sql_mutator():
    db = RegisteredSqlFake()
    kernel = build_permission_kernel()
    rpc = kernel.rpcs["controller.publish_connector"]
    name = unqualified(rpc.object_name)
    assert rpc.components == ("controller",)
    assert name in db.catalogue.procedures
    assert name in declared_write_procedures()
    assert expected_modules()[name][1] == "procedure"
    db.supply_default_grants = False
    db.extra_permissions = [(11, 1, 1, db.module_ids[name], 0, "EXECUTE", "G")]
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps
    assert [principal.principal_id for principal in authority.writers] == [11]


def test_original_registration_replay_precedes_current_catalogue_or_revision_checks():
    db, sources, _, registry, _, plan, receipt = accepted()
    newer = registry.prepare(binding_id=plan.binding_id)
    registry.accept(newer, confirmed_manifest_hash=newer.manifest_hash)
    registry.catalogue = registry.catalogue.model_copy(update={
        "kernel_hash": "0" * 64, "declaration_hash": "1" * 64,
    })
    sources.sites[SITE]["state"] = "Running"
    before, calls, statements = copy.deepcopy(db.tables), len(sources.requests), len(db.statements)
    original = registry.accept(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert original.replayed and original.receipt == receipt.receipt
    assert db.tables == before and len(sources.requests) == calls and len(db.statements) == statements


@pytest.mark.parametrize("field", ["kernel_contract_hash", "reset_catalogue_hash", "authority_snapshot_hash", "writer_rows_hash"])
def test_original_registration_replay_does_not_accept_tampered_hash_bindings(field):
    db, _, _, registry, _, plan, _ = accepted()
    db.headers()[0][field] = "0" * 64
    statements = len(db.statements)
    with pytest.raises(DeploymentError):
        registry.reconcile(plan)
    assert len(db.statements) == statements and db.delete_calls == 0


def test_standalone_registration_and_reset_cli_use_real_adapters_without_caller_profile(tmp_path, monkeypatch):
    db, sources, collector, _, observer_factory = arrangement()
    plan_path = tmp_path / "registration.json"
    reset_path = tmp_path / "reset.json"

    def collector_factory(*args, **kwargs):
        kwargs.setdefault("transport", httpx.MockTransport(sources.handle))
        kwargs.setdefault("clock", lambda: db.clock.now)
        kwargs.setdefault("monotonic", lambda: 0.0)
        kwargs.setdefault("sleep", lambda _: None)
        return AzureDeploymentDiscovery(*args, **kwargs)

    flags = [*cli_args(), "--allow-identity-association-preview"]
    factories = {
        "database_factory": lambda *_: db, "collector_factory": collector_factory,
        "observer_factory": observer_factory,
    }
    assert register.main([*flags, "--binding-id", "fixture-deployment", "--output", str(plan_path)], **factories) == 0
    document = json.loads(plan_path.read_text())
    plan = RegistrationPlan.model_validate(document["plan"])
    assert not db.statements
    assert register.main([
        *flags, "--accept", "--plan", str(plan_path), "--confirm-manifest-hash", plan.manifest_hash,
    ], **factories) == 0
    assert register.main([*flags, "--reconcile", "--plan", str(plan_path)], **factories) == 0
    monkeypatch.setattr(reset, "AzureDeploymentDiscovery", collector_factory)
    assert reset.main(
        [*flags, "--output", str(reset_path)], database_factory=lambda *_: db,
        observer_factory=observer_factory,
    ) == 0
    manifest = reset.ManifestDocument.model_validate_json(reset_path.read_text())
    assert manifest.manifest.preflight_profile and not manifest.reset_execution_blockers
    assert reset.main([
        *flags, "--execute", "--manifest", str(reset_path),
        "--confirm-manifest-hash", manifest.manifest_hash, "--expected-epoch", OLD_EPOCH,
    ], database_factory=lambda *_: db,
        observer_factory=observer_factory) == 0


def test_ddl_mode_never_opens_sql_or_uses_environment_credentials(monkeypatch, capsys):
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "exported-fixture-not-an-input")
    monkeypatch.setenv("MONITORING_RESET_EXECUTE", "true")

    def forbidden(*args, **kwargs):
        pytest.fail("DDL plan must be pure and offline")

    assert register.main(["--ddl"], database_factory=forbidden, collector_factory=forbidden) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["ddl_hash"] and result["runtime_grants"] == []
