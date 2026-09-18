from __future__ import annotations

import copy
import socket
from uuid import UUID

import pytest
from test_azure_sql_prepare import JournalSqlFake
from test_monitoring_authority_closure import AuthoritySqlFake
from test_monitoring_deployment_registration import (
    RegisteredSqlFake,
    arrangement,
    reset_with_reader,
)
from test_monitoring_reset import SITE, TARGET, execute, initialize
from test_monitoring_reset import prepare as reset_plan

from scripts import prepare_azure_sql as prepare
from scripts import reset_monitoring_state as reset
from triage.monitoring.deployment_authority import read_authority
from triage.monitoring.deployment_contracts import DeploymentError
from triage.monitoring.deployment_schema import (
    DEPLOYMENT_JOURNAL_NAMES,
    ancillary_table_permissions,
    unqualified,
)
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.store.azure_sql import DEFAULT_TABLES


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *_: pytest.fail("Operator contract tests are offline"))


def install_component_profile(db, component, *, transitive=False):
    kernel = build_permission_kernel()
    role_id = db.role_ids[kernel.names.role(component)]
    db.supply_default_grants = False
    db.extra_role_edges = [(11, role_id)]
    grantee = 11
    if transitive:
        db.role_ids.update({"application_outer": 800, "application_inner": 801})
        db.extra_role_edges.extend(((11, 800), (800, 801)))
        grantee = 801
    _, checks, _ = prepare.schema_payload(TARGET.tenant_id)
    layouts = {unqualified(check.argument): check.expected for check in checks if check.kind == "columns"}
    for _, name, minor, permission, _ in prepare._grant_rows(kernel.grants[component], kernel.names.role(component), layouts):
        object_id = db.module_ids[unqualified(name)]
        db.extra_permissions.append((role_id, 1, 1, object_id, minor, permission, "G"))
    for name, grant in ancillary_table_permissions(component).items():
        object_id = db.object_ids[name]
        db.extra_permissions.extend((grantee, 1, 1, object_id, 0, permission, "G") for permission in grant.permissions)
        columns = {row[0]: index for index, row in enumerate(db.columns[name], 1)}
        db.extra_permissions.extend(
            (grantee, 1, 1, object_id, columns[column], "UPDATE", "G") for column in grant.update_columns
        )
    return kernel


@pytest.mark.parametrize("component,expected_count", [("controller", 13), ("web", 7), ("worker", 0)])
@pytest.mark.parametrize("transitive", [False, True])
def test_complete_reviewed_component_profiles_are_writers_not_automatic_gaps(component, expected_count, transitive):
    db = RegisteredSqlFake()
    install_component_profile(db, component, transitive=transitive)
    profile = ancillary_table_permissions(component)
    assert len(profile) == expected_count
    if component in {"controller", "web"}:
        approvals = profile[DEFAULT_TABLES["approvals"]]
        assert approvals.permissions == ("SELECT",) and approvals.update_columns == ()
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert not authority.gaps
    assert [writer.principal_id for writer in authority.writers] == [11]
    assert not db.statements


def test_full_profile_is_quiesced_for_reset_not_silently_excluded_as_ancillary():
    db = RegisteredSqlFake()
    install_component_profile(db, "web", transitive=True)
    db, sources, collector, registry, observers = arrangement(db=db)
    sources.sites[SITE]["state"] = "Running"
    running = registry.prepare(binding_id="complete-runtime-profile")
    assert "writers_not_quiescent" in running.blockers
    assert {writer.writer.resource_id for writer in running.capture.writers} == {SITE}
    assert "raw_operational_table_write" not in running.blockers
    with pytest.raises(DeploymentError, match="blocked"):
        registry.accept(running, confirmed_manifest_hash=running.manifest_hash)
    assert not db.statements
    sources.sites[SITE]["state"] = "Stopped"
    stopped = registry.prepare(binding_id="complete-runtime-profile")
    assert not stopped.blockers
    registry.accept(stopped, confirmed_manifest_hash=stopped.manifest_hash)
    operator, manifest = reset_with_reader(db, collector, observers)
    assert not manifest.snapshot.blockers
    execute(operator, manifest)
    assert db.delete_calls > 0


@pytest.mark.parametrize("fault", ["no_role", "unbound_role_name", "owned_role", "altered_anchor", "ambiguous_component"])
def test_runtime_name_or_role_label_is_not_ancillary_authority(fault):
    db = AuthoritySqlFake()
    kernel = install_component_profile(db, "controller")
    role = db.role_ids[kernel.names.role("controller")]
    if fault == "no_role":
        db.extra_role_edges.clear()
    elif fault == "unbound_role_name":
        db.extra_permissions = [row for row in db.extra_permissions if row[0] != role]
    elif fault == "owned_role":
        db.principal_owners[role] = 11
    elif fault == "altered_anchor":
        db.native_module_hashes[unqualified(kernel.rpcs["controller.reserve_action"].object_name)] = "0" * 64
    else:
        web_role = db.role_ids[kernel.names.role("web")]
        web_rpc = db.module_ids[unqualified(kernel.rpcs["web.commit_intent"].object_name)]
        db.extra_permissions.append((web_role, 1, 1, web_rpc, 0, "EXECUTE", "G"))
        db.extra_role_edges.append((11, web_role))
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert authority.gaps and {writer.principal_id for writer in authority.writers} == {11}
    with pytest.raises(DeploymentError):
        authority.require_protected()


@pytest.mark.parametrize("fault", [
    "whole_update", "immutable_column", "unknown_column", "raw_approval_insert", "raw_approval_update",
    "control", "monitoring_record", "monitoring_receipt", "delete_incident", "grant_option",
    "schema", "database", "trigger", "cascade", "worker_application", "web_controller_column",
])
def test_legitimate_profile_does_not_mask_unsafe_mutation_paths(fault):
    db = AuthoritySqlFake()
    component = "worker" if fault == "worker_application" else "web" if fault == "web_controller_column" else "controller"
    install_component_profile(db, component)
    incidents = db.object_ids[DEFAULT_TABLES["incidents"]]
    changed = (11, 1, 1, incidents, 0, "UPDATE", "G")
    if fault in {"immutable_column", "unknown_column"}:
        changed = (11, 1, 1, incidents, 1 if fault == "immutable_column" else 999, "UPDATE", "G")
    elif fault.startswith("raw_approval"):
        changed = (11, 1, 1, db.object_ids[DEFAULT_TABLES["approvals"]], 0, "INSERT" if fault.endswith("insert") else "UPDATE", "G")
    elif fault in {"control", "monitoring_record", "monitoring_receipt"}:
        logical = {"control": "monitoring_control", "monitoring_record": "monitoring_records", "monitoring_receipt": "monitoring_receipts"}[fault]
        changed = (11, 1, 1, db.object_ids[db.table_name(logical)], 0, "INSERT", "G")
    elif fault == "delete_incident":
        changed = (11, 1, 1, incidents, 0, "DELETE", "G")
    elif fault == "grant_option":
        changed = (11, 1, 1, incidents, 2, "UPDATE", "W")
    elif fault == "schema":
        changed = (11, 1, 3, 1, 0, "UPDATE", "G")
    elif fault == "database":
        changed = (11, 1, 0, 0, 0, "CONTROL", "G")
    elif fault == "trigger":
        db.sql_triggers.append((900, 1, incidents, False))
    elif fault == "cascade":
        db.extra_cascades.append((900, db.object_ids[db.table_name("monitoring_records")], incidents, 0, 1, False))
    elif fault == "worker_application":
        changed = (11, 1, 1, incidents, 0, "INSERT", "G")
    elif fault == "web_controller_column":
        commands = db.object_ids[db.table_name("agent_commands")]
        minor = next(index for index, row in enumerate(db.columns[db.table_name("agent_commands")], 1) if row[0] == "worker_id")
        changed = (11, 1, 1, commands, minor, "UPDATE", "G")
    if fault not in {"trigger", "cascade"}:
        db.extra_permissions.append(changed)
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert authority.gaps
    assert {writer.principal_id for writer in authority.writers} == {11}


def seed_journals(db):
    db.install_journal("sql_bootstrap_receipts")
    db.add(
        "sql_bootstrap_receipts", operation_id=str(UUID(int=810)), fingerprint="a" * 64,
        source_sha256="b" * 64, status="committed",
    )
    db.install_journal("sql_bootstrap_recoveries")
    db.add(
        "sql_bootstrap_recoveries", original_operation_id=str(UUID(int=811)),
        replacement_operation_id=str(UUID(int=810)), original_fingerprint="c" * 64,
        original_source_sha256="d" * 64, replacement_fingerprint="a" * 64,
        replacement_source_sha256="b" * 64, recovery_sha256="e" * 64,
        original_receipt_object_id=db.object_ids[DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]],
    )


def test_both_journals_are_optional_but_all_existing_rows_survive_reset_and_replay():
    empty = JournalSqlFake()
    manifest = reset.SqlResetOperator(empty, TARGET).plan()
    assert not manifest.snapshot.blockers and manifest.snapshot.hazards_complete
    with empty.transaction():
        assert not read_authority(empty, TARGET, empty.catalogue).gaps
    db = JournalSqlFake()
    seed_journals(db)
    originals = {name: copy.deepcopy(db.tables[name]) for name in DEPLOYMENT_JOURNAL_NAMES.values()}
    _, operator, manifest, observer, _ = reset_plan(db=db)
    assert not manifest.snapshot.blockers and manifest.snapshot.hazards_complete
    result = execute(operator, manifest)
    assert result.receipt.new_control.epoch == manifest.new_epoch
    for name, rows in originals.items():
        assert db.tables[name] == rows
        assert result.receipt.retained_counts[f"dbo.{name}"] == len(rows)
        invalid = result.receipt.model_dump()
        invalid["deleted_counts"][f"dbo.{name}"] = len(rows)
        with pytest.raises(ValueError, match="declared state objects"):
            reset.ResetReceipt.model_validate(invalid)
    writes = len(db.statements)
    assert execute(operator, manifest).replayed and len(db.statements) == writes
    assert not any(name in sql for sql, _ in db.statements for name in DEPLOYMENT_JOURNAL_NAMES.values())
    observer.close()


@pytest.mark.parametrize("fault", [
    "column", "collation", "nullable", "owner", "status_constraint", "default",
    "literal_space", "literal_case", "untrusted_check", "foreign_key",
    "unique_key", "index_disabled", "trigger", "journal_lookalike", "journal_case_variant",
])
def test_optional_journal_shape_or_ownership_is_not_a_blanket_namespace_exception(fault):
    db = JournalSqlFake(initialized=False)
    seed_journals(db)
    name = DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]
    if fault in {"column", "collation", "nullable"}:
        row = list(db.columns[name][1])
        row[{"column": 0, "collation": 5, "nullable": 4}[fault]] = {
            "column": "changed", "collation": "SQL_Latin1_General_CP1_CI_AS", "nullable": True,
        }[fault]
        db.columns[name][1] = tuple(row)
    elif fault == "owner":
        db.owners[name] = 11
    elif fault in {"status_constraint", "default"}:
        index = 1 if fault == "status_constraint" else 0
        row = list(db.journal_constraints[name][index])
        row[2] = "([status]='started' OR [status]='forged')" if index else "(getutcdate())"
        db.journal_constraints[name][index] = tuple(row)
    elif fault in {"literal_space", "literal_case", "untrusted_check"}:
        row = list(db.journal_constraints[name][1])
        if fault == "untrusted_check":
            row[4] = True
        else:
            row[2] = row[2].replace("started", "star ted" if fault == "literal_space" else "STARTED")
        db.journal_constraints[name][1] = tuple(row)
    elif fault == "foreign_key":
        db.fks.append(("dbo", name, "operation_id", "dbo", db.table_name("agent_runs"), "run_id", 0, 0, False))
    elif fault == "unique_key":
        db.journal_keys[DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_recoveries"]].pop()
    elif fault == "index_disabled":
        row = list(db.journal_keys[name][0])
        row[5] = True
        db.journal_keys[name][0] = tuple(row)
    elif fault == "trigger":
        db.safety[name] = (0, False, False, 1)
    else:
        extra = name.upper() if fault == "journal_case_variant" else name + "_archive"
        db.tables[extra] = []
        db.object_ids[extra] = 899
    operator = reset.SqlResetOperator(db, TARGET)
    manifest = operator.plan_initialization()
    assert manifest.snapshot.blockers
    with db.transaction():
        assert read_authority(db, TARGET, db.catalogue).gaps
    before = copy.deepcopy(db.tables)
    with pytest.raises(reset.ResetRefused):
        initialize(operator, manifest)
    assert db.tables == before and not db.statements


@pytest.mark.parametrize("permission_scope", ["table", "column", "schema", "public", "ddl", "legacy", "unreviewed_module"])
def test_journal_mutation_authority_refuses_initialization_not_just_reset(permission_scope):
    db = JournalSqlFake(initialized=False)
    seed_journals(db)
    object_id = db.object_ids[DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]]
    changed = (11, 1, 1, object_id, 0, "UPDATE", "G")
    if permission_scope == "column":
        changed = (11, 1, 1, object_id, 2, "UPDATE", "G")
    elif permission_scope == "schema":
        changed = (11, 1, 3, 1, 0, "UPDATE", "G")
    elif permission_scope == "public":
        changed = (0, 1, 1, object_id, 0, "INSERT", "G")
        db.sql_writer_principals.clear()
    elif permission_scope == "ddl":
        changed = (11, 1, 0, 0, 0, "CONTROL", "G")
    elif permission_scope == "legacy":
        db.role_ids["db_datawriter"] = 800
        db.extra_role_edges.append((11, 800))
    elif permission_scope == "unreviewed_module":
        db.native_module_hashes[db.catalogue.procedures[0]] = "0" * 64
    if permission_scope not in {"legacy", "unreviewed_module"}:
        db.extra_permissions.append(changed)
    operator = reset.SqlResetOperator(db, TARGET)
    manifest = operator.plan_initialization()
    before = copy.deepcopy(db.tables)
    with pytest.raises(reset.ResetRefused, match="journal authority"):
        initialize(operator, manifest)
    assert db.tables == before and not db.statements


def test_legitimate_running_profile_does_not_create_a_reset_registration_startup_gate():
    db = JournalSqlFake(initialized=False)
    seed_journals(db)
    install_component_profile(db, "web")
    operator = reset.SqlResetOperator(db, TARGET)
    plan = operator.plan_initialization()
    assert plan.deployment_inventory is None
    original = {name: copy.deepcopy(db.tables[name]) for name in DEPLOYMENT_JOURNAL_NAMES.values()}
    result = initialize(operator, plan)
    assert result.receipt.control.maintenance
    assert all(db.tables[name] == rows for name, rows in original.items())
    assert not db.tables[db.table_name("deployment_registration")]
