from __future__ import annotations

import ast
import json
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import get_args

import pytest

from triage.monitoring import models
from triage.monitoring.sql_kernel_contracts import (
    FACT_KINDS,
    IDENTITY_COLUMNS,
    KERNEL_VERSION,
    RECORD_COLUMNS,
    RPC_FACT_KINDS,
    SOURCE_DISPOSITIONS,
    WORK_FACT_KINDS,
)
from triage.monitoring.sql_permissions import (
    COMPONENTS,
    budget_policy_statements,
    build_permission_kernel,
    decode_rpc_result,
    integration_contract,
    object_catalogue,
    rpc_contracts,
    runtime_grants,
    schema_statements,
)


@pytest.fixture(scope="module")
def kernel():
    return build_permission_kernel()


def sql(kernel, operation: str) -> str:
    return next(obj.ddl for obj in kernel.objects if obj.logical_name == operation)


def test_kernel_is_pure_deterministic_and_has_fixed_roles_and_objects(kernel) -> None:
    assert kernel.statements == schema_statements()
    assert kernel.catalogue() == object_catalogue()
    assert len([obj for obj in kernel.objects if obj.kind == "role"]) == 3
    assert len(kernel.rpcs) == 27
    assert len({obj.name for obj in kernel.objects}) == len(kernel.objects)
    assert all(rpc.implemented for rpc in kernel.rpcs.values())
    assert all(rpc.result_columns == ("result_json",) for rpc in kernel.rpcs.values())
    assert all(len(item["sha256"]) == 64 for item in kernel.catalogue())


@pytest.mark.parametrize("bad", ["dbo.other", "bad;DROP TABLE x", "", "has space"])
def test_table_identifier_injection_is_refused(bad: str) -> None:
    with pytest.raises(ValueError):
        build_permission_kernel({"monitoring_records": bad})


def test_aliases_change_the_whole_fixed_namespace_and_cannot_collide() -> None:
    original = build_permission_kernel()
    changed = build_permission_kernel({"monitoring_records": "other_monitoring_records"})
    assert original.names.suffix != changed.names.suffix
    assert all(original.names.suffix not in obj.name for obj in changed.objects)
    with pytest.raises(ValueError, match="distinct"):
        build_permission_kernel({"monitoring_records": "triage_monitoring_control"})


def test_runtime_grants_have_no_base_dml_ddl_or_broad_roles(kernel) -> None:
    base_names = tuple(kernel.names.table(name) for name in (
        "monitoring_control", "monitoring_records", "monitoring_leases", "monitoring_receipts",
        "monitoring_rate_budget", "incidents", "approvals", "processed",
    ))
    for component in COMPONENTS:
        grants = runtime_grants(component)
        assert grants == kernel.grants[component]
        for statement in grants:
            assert statement.startswith("GRANT ")
            assert all(f"ON OBJECT::{base}" not in statement for base in base_names)
            assert all(value not in statement.upper() for value in (
                "GRANT ALTER", "GRANT CONTROL", "IMPERSONATE", "WITH GRANT OPTION",
                "DB_OWNER", "DB_DATAWRITER", "DB_DDLADMIN",
            ))
    with pytest.raises(ValueError):
        runtime_grants("unknown")


def test_checked_views_and_update_grants_preserve_identity_columns(kernel) -> None:
    for logical in ("worker_catalogue", "worker_evidence", "worker_telemetry", "web_drafts",
                    "controller_projections", "controller_immutable"):
        view = sql(kernel, logical)
        assert "WITH SCHEMABINDING" in view
        assert "WITH CHECK OPTION" in view
        assert "c.tenant_id=r.tenant_id AND c.epoch=r.epoch" in view
    for component in COMPONENTS:
        for grant in kernel.grants[component]:
            if grant.startswith("GRANT UPDATE"):
                columns = re.search(r"GRANT UPDATE \((.*?)\)", grant)[1]
                assert all(f"[{name}]" not in columns for name in IDENTITY_COLUMNS)
    assert not any(
        statement.startswith("GRANT UPDATE") and "controller_immutable" in statement
        for statement in kernel.grants["controller"]
    )


def test_worker_web_cannot_write_controller_families_or_work(kernel) -> None:
    for component in ("worker", "web"):
        writes = "\n".join(
            value for value in kernel.grants[component]
            if value.startswith(("GRANT INSERT", "GRANT SELECT, INSERT", "GRANT UPDATE"))
        )
        assert "controller_projections" not in writes
        assert "controller_immutable" not in writes
        assert "action_owner" not in writes
    for operation, rpc in kernel.rpcs.items():
        if operation.startswith("controller."):
            assert rpc.components == ("controller",)
    assert not any("component" == parameter.name for rpc in kernel.rpcs.values() for parameter in rpc.parameters)


def test_every_rpc_has_real_component_guard_and_explicit_one_cell_result(kernel) -> None:
    for operation, rpc in kernel.rpcs.items():
        body = sql(kernel, operation)
        assert "SET NOCOUNT ON;" in body
        assert "SET XACT_ABORT ON;" in body
        assert "SQL kernel component authority is absent or ambiguous" in body
        assert "IS_ROLEMEMBER(" in body
        assert "AS result_json;" in body
        assert "EXECUTE AS OWNER" not in body.upper()
        assert "SESSION_CONTEXT" not in body.upper()
        assert "sp_executesql" not in body
        assert "BEGIN TRAN" not in body.upper()
        assert "COMMIT TRAN" not in body.upper()
        assert "ROLLBACK" not in body.upper()
        if rpc.mutating:
            assert "@@TRANCOUNT = 0 OR XACT_STATE() <> 1" in body


def test_control_lock_never_grants_or_performs_revision_update(kernel) -> None:
    lock = sql(kernel, "lock_context")
    assert "WITH (UPDLOCK, HOLDLOCK)" in lock
    assert "UPDATE " not in lock
    assert not re.search(r"\bSET\s+revision\s*=\s*revision\b", lock, re.I)
    for component in COMPONENTS:
        assert not any("GRANT UPDATE" in g and "control_read" in g for g in kernel.grants[component])


def test_web_intent_revocation_fences_new_reservations_before_reconciliation(kernel) -> None:
    web = sql(kernel, "web.commit_intent")
    reserve = sql(kernel, "controller.reserve_action")
    assert "IF @intent_kind IN ('scope','review')" in web
    assert "SET revision=revision+1" in web
    assert web.index("SET revision=revision+1") < web.index("'reconcile_state' AS kind")
    assert "'pending-validation'" in web and "'configuring'" in web
    assert "Current policy revision differs from this new operation" in reserve
    assert "Current target projection does not authorize this action" in reserve
    assert "Controller validation is stale" in reserve
    assert "Latest protected review intent is revoked, pending or different" in reserve
    assert "IS_ROLEMEMBER" in reserve


def test_original_receipt_replay_precedes_new_revision_and_maintenance_checks(kernel) -> None:
    for operation, rpc in kernel.rpcs.items():
        if not any(parameter.name == "fingerprint" for parameter in rpc.parameters):
            continue
        body = sql(kernel, operation)
        replay = body.index("IF @prior_payload IS NOT NULL")
        assert "@prior_request_id<>@request_id" in body
        assert "binding_hash" in body
        for marker in (
            "IF @current_revision<>@expected_revision",
            "IF @maintenance=1 THROW",
        ):
            if marker in body:
                assert replay < body.index(marker)


def test_existing_reserved_effect_transitions_survive_revocation(kernel) -> None:
    for operation in ("controller.transition_action", "controller.finalize"):
        body = sql(kernel, operation)
        assert "IF @current_revision<>@expected_revision" not in body
        assert "IF @maintenance=1 THROW" not in body
        assert "Current work owner or fence was lost" in body
    reserve = sql(kernel, "controller.reserve_action")
    assert "IF @current_revision<>@expected_revision" in reserve
    assert "IF @maintenance=1 THROW" in reserve


def test_handoff_is_clean_initial_insert_not_producer_update_of_controller_work(kernel) -> None:
    for operation in ("web.commit_intent", "worker.accept_facts", "worker.commit_positions"):
        body = sql(kernel, operation)
        assert "'reconcile_state' AS kind" in body
        assert "1 AS revision,0 AS attempts,0 AS retry_attempt,'queued' AS state" in body
        assert "AS action_reservation_id" in body and "AS finalization_id" in body
        assert "DECLARE @reconcile_id" in body
    assert "Enqueue never updates or adopts an existing work row" in sql(kernel, "controller.enqueue_work")
    assert "Work enqueue cannot supply ownership, retry, finalization or state fields" in sql(kernel, "controller.enqueue_work")


def test_stored_work_family_and_owner_fence_guard_all_transitions(kernel) -> None:
    worker = sql(kernel, "worker.transition_work")
    controller = sql(kernel, "controller.transition_work")
    assert "Stored work is not in this component family" in worker
    assert "'inventory', N'capability_probe', N'poll', N'connector_reconcile'" in worker
    assert "owner_id=@owner_id AND fence=@fence" in worker
    assert "Worker cannot bind controller finalization or actions" in worker
    assert "Controller completion requires its original finalization receipt" in controller
    assert "JSON_VALUE(payload,'$.result.state')='completed'" in controller
    assert "Controller execution must use incident finalization" in controller


def test_partition_lease_and_journal_update_together(kernel) -> None:
    body = sql(kernel, "worker.partition")
    assert "@expected_ownership_revision" in body
    assert "Partition owner/fence compare-and-set failed" in body
    assert "'partition_ownership'" in body
    assert "Partition journal update lost its predicate" in body
    assert "Pinned broker start cannot be reset" in body
    assert "WHERE tenant_id=@tenant_id AND epoch=@epoch" in body
    assert "BEGIN TRAN" not in body


def test_orphan_worker_rows_are_not_visible_as_accepted_controller_evidence(kernel) -> None:
    view = sql(kernel, "accepted_worker_facts")
    assert "accepted.record_kind='accepted_fact'" in view
    assert "receipt.operation IN ('worker.accept_facts','worker.commit_positions','worker.record_heartbeat')" in view
    assert "fact_revision" in view and "payload_hash" in view
    assert "$.row_hash" in view
    assert all(f"r.[{column}]" in view for column in RECORD_COLUMNS)
    body = sql(kernel, "worker.accept_facts")
    assert "Batch cannot accept missing, changed or unrelated raw facts" in body
    assert "Current work owner or fence was lost" in body
    assert "worker.accept_facts" in body
    controller = sql(kernel, "controller_read")
    assert "accepted_worker_facts" in controller
    assert "r.record_kind IN" in controller


def test_checkpoint_requires_receipt_backed_contiguity_and_exact_offset(kernel) -> None:
    body = sql(kernel, "worker.advance_checkpoint")
    assert "r.operation='worker.commit_positions'" in body
    assert "JSON_VALUE(r.payload,'$.result.partition_key')=@partition_key" in body
    assert "accepted.record_kind='accepted_fact'" in body
    assert "JSON_VALUE(accepted.payload,'$.row_hash')" in body
    assert "@count<>@through_sequence_number-@first+1" in body
    assert "DATALENGTH(@last_offset)=DATALENGTH(@through_offset)" in body
    assert "Current policy revision differs" in body
    assert "Partition lease owner or fence was lost" in body
    positions = sql(kernel, "worker.commit_positions")
    assert "Original event source/id cannot be rebound" in positions
    assert "An original stream position or offset cannot be rewritten" in positions


def test_connector_patch_is_closed_and_cannot_rewrite_desired_authority(kernel) -> None:
    body = sql(kernel, "worker.observe_connector")
    assert "Connector observation includes an unauthorized field" in body
    assert "Established connector bindings cannot be replaced or cleared" in body
    assert "Established endpoint metadata cannot be replaced or cleared" in body
    assert "Endpoint accepts nonsecret metadata only" in body
    assert "Ready requires matched topology" in body
    allowed = re.search(r"WHERE \[key\] NOT IN\s*\((.*?)\)\)", body, re.S)[1]
    assert "desired_definition" not in allowed
    assert "'sources'" not in allowed
    assert "'ownership_id'" not in allowed


def test_approval_scopes_are_separate_and_never_refund_or_reopen(kernel) -> None:
    assert kernel.rpcs["web.decide_approval"].components == ("web",)
    assert kernel.rpcs["controller.consume_approval"].components == ("controller",)
    for operation in ("web.decide_approval", "controller.consume_approval"):
        body = sql(kernel, operation)
        assert "$.delivery_channel" in body
        assert "$.fingerprint" in body
        assert "$.expires_at" in body
        assert "$.consumed_at" in body
    assert "Approval identity already exists" in sql(kernel, "controller.open_approval")
    assert "Only an explicit approval may be consumed" in sql(kernel, "controller.consume_approval")
    assert "Incident budget is malformed, not unused" in sql(kernel, "controller.reserve_action")
    assert "No reset/refund" in sql(kernel, "controller.finalize") or "no reset/refund" in sql(kernel, "controller.finalize")


def test_required_retry_and_history_operations_have_no_temporary_blockers(kernel) -> None:
    assert kernel.unresolved_cases == ()
    assert "THROW 51077" not in "\n".join(obj.ddl for obj in kernel.objects)
    assert "generic mutation fallback" not in "\n".join(obj.ddl for obj in kernel.objects)


def test_rpc_bind_and_decode_use_explicit_single_cell_contract() -> None:
    contract = rpc_contracts()["lock_context"]
    statement, args = contract.bind({"tenant_id": "tenant", "epoch": "epoch"})
    assert statement.startswith("EXEC [dbo].[triage_mon_lock_context_")
    assert statement.endswith("@tenant_id = ?, @epoch = ?;")
    assert args == ("tenant", "epoch")
    with pytest.raises(ValueError):
        contract.bind({"tenant_id": "tenant", "epoch": "epoch", "component": "controller"})
    envelope = {
        "kernel_version": KERNEL_VERSION, "operation": "lock_context", "status": "read",
        "affected_rows": 0, "result": {
            "tenant_id": "tenant", "epoch": "epoch", "revision": 1,
            "maintenance": False, "observed_at": "2026-09-16T00:00:00Z",
        },
    }
    assert decode_rpc_result(contract, [(json.dumps(envelope),)]) == envelope
    for rows in ([], [("{}",)], [("[]",)], [(json.dumps(envelope), "extra")], [(1,)]):
        with pytest.raises(ValueError):
            decode_rpc_result(contract, rows)
    envelope["affected_rows"] = True
    with pytest.raises(ValueError):
        decode_rpc_result(contract, [(json.dumps(envelope),)])


def test_deployer_budget_seed_does_not_reset_or_update_existing_limits() -> None:
    statements = budget_policy_statements(
        "11111111-1111-4111-8111-111111111111", {"fabric:items": (200, 3600)},
    )
    assert len(statements) == 1
    assert "IF EXISTS" in statements[0] and "IF NOT EXISTS" in statements[0]
    assert "Existing service budget policy differs; no reset performed" in statements[0]
    assert "UPDATE " not in statements[0] and "DELETE " not in statements[0]
    with pytest.raises(ValueError):
        budget_policy_statements("11111111-1111-4111-8111-111111111111", {"bad';": (1, 60)})
    with pytest.raises(ValueError):
        budget_policy_statements("11111111-1111-4111-8111-111111111111", {"bucket": (True, 60)})


def test_machine_readable_integration_handoff_is_explicit() -> None:
    contract = integration_contract()
    assert len(contract["rpcs"]) == 27
    assert contract["native_sql_proven"] is False
    assert "db.query" in contract["rpc_call"]
    assert "never await" in contract["rpc_call"].lower()
    assert "finalization_plan" in contract["controller_publication_contracts"]
    assert contract["unresolved_cases"] == []
    assert set(contract["write_routes"]) == {
        "worker_catalogue", "worker_evidence", "worker_telemetry", "web_drafts",
        "controller_projections", "controller_immutable",
    }
    assert "own batch" in contract["ddl_call"]


def test_new_generators_parse_on_the_declared_python_floor() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "triage" / "monitoring"
    names = (
        "sql_permissions.py", "sql_kernel_contracts.py", "sql_kernel_common.py",
        "sql_kernel_schema.py", "sql_kernel_intents.py", "sql_kernel_work.py",
        "sql_kernel_intake.py", "sql_kernel_actions.py", "sql_kernel_json.py", "sql_kernel_frontiers.py",
        "sql_kernel_retries.py", "sql_kernel_history.py",
        "sql_kernel_connectors.py",
        "sql_kernel_arguments.py", "sql_kernel_retention.py", "sql_kernel_sources.py", "sql_kernel_proposals.py",
        "sql_kernel_correlation.py", "sql_kernel_removals.py", "sql_kernel_supersessions.py",
    )
    for name in names:
        ast.parse((root / name).read_text("utf-8"), filename=name, feature_version=(3, 11))


def _mask_sql(sql_text: str) -> str:
    return re.sub(r"N?'(?:''|[^'])*'|--[^\n]*", lambda match: " " * len(match[0]), sql_text)


def test_generated_sql_has_balanced_parentheses_and_no_use_before_declare(kernel) -> None:
    for obj in kernel.objects:
        masked = _mask_sql(obj.ddl)
        depth = 0
        for char in masked:
            depth += int(char == "(") - int(char == ")")
            assert depth >= 0, obj.logical_name
        assert depth == 0, obj.logical_name
        if obj.kind != "procedure":
            continue
        contract = kernel.rpcs[obj.logical_name]
        parameters = {"@" + parameter.name for parameter in contract.parameters}
        declared = {}
        for match in re.finditer(
            r"(?:\bDECLARE|,)\s*(@\w+)\s+(?:n?varchar|char|bigint|int|bit|datetime2|binary|varbinary|uniqueidentifier|TABLE)\b",
            masked, re.I,
        ):
            name = match[1].lower()
            assert name not in declared, (obj.logical_name, name)
            declared[name] = match.start(1)
        body = masked[masked.index("\nAS\n"):]
        base = len(masked) - len(body)
        for match in re.finditer(r"(?<!@)@\w+", body):
            name = match[0].lower()
            if name in parameters:
                continue
            assert name in declared, (obj.logical_name, name)
            assert declared[name] <= base + match.start(), (obj.logical_name, name)


def test_rpc_fact_families_have_no_direct_worker_insert_or_acceptance_bypass(kernel) -> None:
    raw_views = "\n".join(sql(kernel, name) for name in (
        "worker_catalogue", "worker_evidence", "worker_telemetry",
    ))
    accepts = sql(kernel, "worker.accept_facts")
    for kind in RPC_FACT_KINDS:
        assert f"N'{kind}'" not in raw_views
        assert f"N'{kind}'" not in accepts
    assert set(WORK_FACT_KINDS) == models.WORKER_WORK_KINDS
    assert set(FACT_KINDS) >= set(RPC_FACT_KINDS)
    assert set(SOURCE_DISPOSITIONS) == set(get_args(models.SourceDisposition))


def test_every_fact_acceptance_binds_all_promoted_fields_and_original_payload(kernel) -> None:
    for operation in ("worker.accept_facts", "worker.commit_positions", "worker.record_heartbeat"):
        body = sql(kernel, operation)
        assert "AS row_hash" in body
        assert "AS payload_hash" in body
        assert all(f".[{column}]" in body for column in RECORD_COLUMNS)
    view = sql(kernel, "accepted_worker_facts")
    assert "COALESCE(" in view and "REPLICATE('-',64)" in view
    assert "CONVERT(nvarchar(max),r.[due_at],126)" in view


def test_opaque_event_identity_is_not_restricted_to_a_guid_or_rewritten(kernel) -> None:
    function = sql(kernel, "json_identity_string")
    assert "WITH SCHEMABINDING" in function
    assert "Latin1_General_100_BIN2,@position,1" in function
    assert "@position<=DATALENGTH(@text)/2" in function
    assert "WHEN 34 THEN N'\\\"'" in function and "WHEN 92 THEN N'\\\\'" in function
    assert "WHEN 47" not in function
    assert "WHEN 8 THEN N'\\b'" in function
    assert "WHEN @code BETWEEN 32 AND 126 THEN @character" in function
    assert "N'\\u'+LOWER(" in function
    assert not any("json_identity_string" in grant for grants in kernel.grants.values() for grant in grants)
    positions = sql(kernel, "worker.commit_positions")
    assert "original opaque source/id JSON digest" in positions
    assert "original GUID source/id" not in positions
    assert "json_identity_string" in positions
    assert "json_identity_string" in sql(kernel, "controller.reserve_action")


def test_redelivery_keeps_first_evidence_and_each_original_broker_position(kernel) -> None:
    body = sql(kernel, "worker.commit_positions")
    assert "SET payload=COALESCE(prior.payload,first_delivery.payload)" in body
    assert "One delivery identity has conflicting event content" in body
    assert "'$.received_at',NULL" in body and "'$.observation.observed_at',NULL" in body
    assert "p.enqueued_at AS enqueued_at" in body
    assert "JSON_VALUE(prior.payload,'$.enqueued_at')<>p.enqueued_at" in body
    assert "SELECT MIN(first_sequence.sequence_number)" in body


def test_server_clock_is_refreshed_after_waiting_for_control_or_approval_locks(kernel) -> None:
    for operation in kernel.rpcs:
        body = sql(kernel, operation)
        assert body.index("SET @now=SYSUTCDATETIME();") > body.index("@current_revision IS NULL")
    reserve = sql(kernel, "controller.reserve_action")
    assert "Approval wait outlived the current work or technical validation" in reserve
    assert "Current controller target ownership was lost" in reserve
    assert "Full tool arguments contain invalid or extra technical fields" in reserve
    assert "$.binding_hash" in reserve


def test_handoff_times_are_explicit_utc_and_work_ids_fit_physical_lease_owners(kernel) -> None:
    for operation in ("worker.accept_facts", "worker.commit_positions", "web.commit_intent"):
        body = sql(kernel, operation)
        assert "CONVERT(nvarchar(40),@now,127)+N'Z' AS created_at" in body
        assert "CONVERT(nvarchar(40),@now,127)+N'Z' AS due_at" in body
        assert "Operation request must be a canonical nonempty GUID" in body
    for operation in ("controller.claim_work", "worker.claim_work", "worker.transition_work"):
        assert "Work and lease owner must be canonical nonempty GUIDs" in sql(kernel, operation)


def test_action_transition_preserves_submission_and_active_owner_fence(kernel) -> None:
    body = sql(kernel, "controller.transition_action")
    assert "Submitted execution identity cannot change" in body
    assert "Original submission time cannot change or be cleared" in body
    assert "Configuration evidence differs from the original reserved target/action/hash" in body
    assert "JSON_QUERY(@transition_json,'$.submitted_execution') IS NULL" in body
    assert "AND COALESCE(JSON_VALUE(@transition_json,'$.submitted_execution.run_id_kind'),'')<>'powerbi_request'" in body
    assert "Terminal action lost its original active owner fence" in body
    assert "original source execution" in sql(kernel, "controller.enqueue_work")


@pytest.mark.parametrize("field,value", [
    ("lease_seconds", True), ("lease_seconds", 2**31), ("owner_id", 3),
    ("owner_id", "x" * 129), ("work_id", "\ud800"),
])
def test_rpc_binding_rejects_truncation_coercion_and_invalid_text(field, value) -> None:
    contract = rpc_contracts()["worker.claim_work"]
    arguments = {
        "tenant_id": "11111111-1111-4111-8111-111111111111",
        "epoch": "22222222-2222-4222-8222-222222222222",
        "work_id": "33333333-3333-4333-8333-333333333333",
        "owner_id": "44444444-4444-4444-8444-444444444444",
        "lease_seconds": 120,
    }
    arguments[field] = value
    with pytest.raises(ValueError):
        contract.bind(arguments)


def test_datetime_binding_normalizes_aware_input_without_mutating_original_arguments() -> None:
    contract = rpc_contracts()["controller.open_approval"]
    expires = datetime(2026, 9, 16, 9, 0, tzinfo=timezone(timedelta(hours=2)))
    arguments = {
        "tenant_id": "11111111-1111-4111-8111-111111111111",
        "epoch": "22222222-2222-4222-8222-222222222222",
        "request_id": "33333333-3333-4333-8333-333333333333",
        "fingerprint": "a" * 64, "expected_revision": 0, "approval_id": "approval",
        "channel": "web", "approval_fingerprint": "b" * 64,
        "expires_at": expires, "proposal_json": "{}",
    }
    _, values = contract.bind(arguments)
    index = next(i for i, parameter in enumerate(contract.parameters) if parameter.name == "expires_at")
    assert values[index] == expires.astimezone(UTC).replace(tzinfo=None)
    assert arguments["expires_at"] is expires
    arguments["expires_at"] = expires.replace(tzinfo=None)
    with pytest.raises(ValueError, match="timezone-aware"):
        contract.bind(arguments)


def test_deployer_seed_is_balanced_and_reserved_aliases_are_quoted(kernel) -> None:
    for statement in budget_policy_statements(
        "11111111-1111-4111-8111-111111111111", {"fabric:items": (200, 3600)},
    ):
        masked = _mask_sql(statement)
        assert masked.count("(") == masked.count(")")
    assert "AS [identity]" in sql(kernel, "controller.reserve_action")
    assert "AS [identity]" in sql(kernel, "controller.finalize")
