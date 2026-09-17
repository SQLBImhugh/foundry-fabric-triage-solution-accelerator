"""Offline SQL decision regressions, not native role/DDL/transaction proof."""

from __future__ import annotations

import json
import re
import sqlite3

import pytest

from triage.monitoring.sql_kernel_contracts import ACTION_WORK_KINDS, FRONTIER_KINDS
from triage.monitoring.sql_kernel_frontiers import frontier_can_close_sql, frontier_snapshot_sql
from triage.monitoring.sql_permissions import (
    build_permission_kernel,
    integration_contract,
    work_policy,
)


@pytest.fixture(scope="module")
def kernel():
    return build_permission_kernel()


def _sql(kernel, name):
    return next(obj.ddl for obj in kernel.objects if obj.logical_name == name)


def _json_value(payload, path):
    if payload is None:
        return None
    value = json.loads(payload)
    for part in path.removeprefix("$.").split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    if value is None or isinstance(value, (dict, list)):
        return None
    if type(value) is bool:
        return "true" if value else "false"
    return str(value)


def _try_convert(kind, value):
    assert kind == "bigint"
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if -(2**63) <= parsed < 2**63 else None


@pytest.fixture
def decision_db(kernel):
    # Execute the generator's exact decision expressions. Only these two
    # scalar functions are adapted; SQL Server role/locking proof is separate.
    connection = sqlite3.connect(":memory:")
    connection.create_function("JSON_VALUE", 2, _json_value)
    connection.create_function("TRY_CONVERT", 2, _try_convert)
    connection.execute("ATTACH DATABASE ':memory:' AS dbo")
    connection.execute(f"""CREATE TABLE {kernel.names.table('monitoring_records')} (
        tenant_id TEXT,epoch TEXT,record_kind TEXT,full_key TEXT,sequence_number INTEGER,
        revision INTEGER,status TEXT,parent_key TEXT,target_key TEXT,payload TEXT)""")
    connection.execute(f"""CREATE TABLE {kernel.names.table('monitoring_receipts')} (
        tenant_id TEXT,epoch TEXT,operation TEXT,request_id TEXT,payload TEXT)""")
    yield connection
    connection.close()


def _rows(db, kernel, *, accepted=2, validated=2, window_state="validated",
          status="published", receipt=True, receipt_operation="controller.resolve_frontier",
          target="target"):
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    db.execute(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?,?,?)", (
        "tenant", "epoch", "validation_frontier", "frontier", accepted, 1, status,
        "frontier" if window_state else None, target,
        json.dumps({"accepted_revision": accepted, "validated_revision": validated}),
    ))
    if window_state:
        db.execute(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?,?,?)", (
            "tenant", "epoch", "validation_window", "frontier", accepted, 1,
            window_state, None, target, "{}",
        ))
    db.execute(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?,?,?)", (
        "tenant", "epoch", "frontier_commit", "frontier", validated, 1,
        status, None, target, json.dumps({"request_id": "resolution"}),
    ))
    if receipt:
        db.execute(f"INSERT INTO {receipts} VALUES (?,?,?,?,?)", (
            "tenant", "epoch", receipt_operation, "resolution",
            json.dumps({"result": {
                "frontier_key": "frontier", "validated_revision": validated, "state": status,
            }}),
        ))


def _pending(db, kernel, target="target"):
    source = frontier_snapshot_sql(kernel.names, "@target_key")
    predicate = re.search(r"CAST\(\((CASE WHEN.*?)\) AS bit\) AS pending", source, re.S)[1]
    start = source.index(f"    FROM {kernel.names.table('monitoring_records')} AS f")
    end = source.index("    ORDER BY f.full_key", start)
    scope = source[start:end].replace(
        "    WHERE f.tenant_id", "    CROSS JOIN (SELECT 'bigint' AS bigint) AS types WHERE f.tenant_id",
    )
    rows = db.execute(f"SELECT {predicate} AS pending {scope}", {
        "tenant_id": "tenant", "epoch": "epoch", "target_key": target,
    }).fetchall()
    return any(row[0] for row in rows)


def _can_close(db, *, complete=False, proof_complete=False, all_resolved=True,
               accepted=2, closing_revision=2, closing_id="last-page", proof_closing_id="last-page",
               reject=False, has_window=True, prefix_committed=True):
    window = None if not has_window else json.dumps({
        "collection_complete": complete, "closing_revision": closing_revision,
        "closing_request_id": closing_id,
    })
    return db.execute(
        f"SELECT CASE WHEN {frontier_can_close_sql()} THEN 1 ELSE 0 END "
        "FROM (SELECT 'bigint' AS bigint) AS types",
        {
            "all_resolved": int(all_resolved), "window": window, "accepted": accepted,
            "decision": "rejected" if reject else "published",
            "whole_window_rejection": int(reject), "prefix_committed": int(prefix_committed),
            "proof": json.dumps({
                "window_complete": proof_complete, "closing_request_id": proof_closing_id,
                "reject_whole_window": reject,
            }),
        },
    ).fetchone()[0]


def test_first_raw_page_fences_without_any_window_projection_or_nonterminal_producer(decision_db, kernel):
    _rows(decision_db, kernel, accepted=1, validated=0, window_state="collecting", status="pending_validation")
    assert _pending(decision_db, kernel)
    records = kernel.names.table("monitoring_records")
    decision_db.execute(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?,?,?)", (
        "tenant", "epoch", "work", "producer", None, 1, "leased", None, "target", "{}",
    ))
    decision_db.execute(f"UPDATE {records} SET status='completed' WHERE record_kind='work'")
    assert _pending(decision_db, kernel)
    assert "powerbi_window" not in frontier_snapshot_sql(kernel.names, "@target_key")


@pytest.mark.parametrize("window_state", ["collecting", "awaiting_validation"])
def test_latest_sequence_validation_cannot_clear_an_incomplete_window(decision_db, kernel, window_state):
    _rows(decision_db, kernel, window_state=window_state)
    assert _pending(decision_db, kernel)


@pytest.mark.parametrize("kwargs", [
    {"validated": 1}, {"receipt": False}, {"receipt_operation": "worker.commit_positions"},
    {"status": "pending_validation"},
])
def test_revision_gap_or_missing_controller_commit_remains_a_denial(decision_db, kernel, kwargs):
    _rows(decision_db, kernel, **kwargs)
    assert _pending(decision_db, kernel)


@pytest.mark.parametrize("status,window_state", [("published", "validated"), ("rejected", "rejected")])
def test_exact_controller_commit_can_close_publication_or_rejection(decision_db, kernel, status, window_state):
    _rows(decision_db, kernel, status=status, window_state=window_state)
    assert not _pending(decision_db, kernel)


def test_tenant_frontier_applies_to_all_targets_but_target_frontier_is_scoped(decision_db, kernel):
    _rows(decision_db, kernel, validated=0)
    assert _pending(decision_db, kernel, "target")
    assert not _pending(decision_db, kernel, "other")
    decision_db.execute(
        f"UPDATE {kernel.names.table('monitoring_records')} SET target_key=NULL "
        "WHERE record_kind='validation_frontier'",
    )
    assert _pending(decision_db, kernel, "other")


@pytest.mark.parametrize("kwargs", [
    {"proof_complete": True}, {"complete": True},
    {"complete": True, "proof_complete": True, "all_resolved": False},
    {"complete": True, "proof_complete": True, "closing_revision": 1},
    {"complete": True, "proof_complete": True, "proof_closing_id": "wrong"},
])
def test_page_or_producer_completion_is_not_full_window_validation(decision_db, kwargs):
    assert not _can_close(decision_db, **kwargs)


def test_correlated_full_window_or_explicit_durable_rejection_can_close(decision_db):
    assert _can_close(decision_db, complete=True, proof_complete=True)
    assert _can_close(decision_db, proof_complete=True, reject=True)
    assert _can_close(decision_db, proof_complete=True, reject=True, all_resolved=False)
    assert not _can_close(decision_db, proof_complete=True, reject=True, prefix_committed=False)


def test_window_free_handoffs_still_require_the_entire_contiguous_prefix(decision_db):
    assert _can_close(decision_db, has_window=False)
    assert not _can_close(decision_db, has_window=False, all_resolved=False)


def test_only_kernel_procedures_can_mutate_frontier_acknowledgements(kernel):
    for view in ("worker_catalogue", "worker_evidence", "worker_telemetry", "web_drafts",
                 "controller_projections", "controller_immutable"):
        for kind in FRONTIER_KINDS:
            assert f"N'{kind}'" not in _sql(kernel, view)
    assert "N'frontier_validation'" in _sql(kernel, "controller_immutable")
    assert kernel.rpcs["controller.resolve_frontier"].components == ("controller",)
    assert "frontier_validation" not in _sql(kernel, "worker_evidence")


def test_original_intake_receipt_replay_never_raises_a_new_frontier(kernel):
    for operation in ("worker.accept_facts", "worker.commit_positions", "worker.observe_connector",
                      "web.commit_intent"):
        body = _sql(kernel, operation)
        assert body.index("IF @prior_payload IS NOT NULL") < body.index(
            "SET @frontier_revision=COALESCE(@frontier_prior_revision,0)+1",
        )
        assert body.index("'validation_frontier'") < body.rindex(f"N'{operation}'")
        assert "SET @frontier_validated=TRY_CONVERT" in body
    assert "BEGIN TRAN" not in _sql(kernel, "controller.resolve_frontier")


def test_every_new_reservation_uses_current_frontier_but_existing_effects_do_not(kernel):
    reserve = _sql(kernel, "controller.reserve_action")
    assert "Controller action validation does not cover the committed frontier snapshot" in reserve
    assert reserve.index("IF @prior_payload IS NOT NULL") < reserve.index("IF @frontier_pending=1")
    assert "@origin" not in reserve
    for operation in ("controller.transition_action", "controller.finalize"):
        assert "IF @frontier_pending=1" not in _sql(kernel, operation)


@pytest.mark.parametrize("target", [None, {"reference_only": True}])
def test_reconciliation_routes_before_exact_source_validation_without_action_ownership(target):
    policy = work_policy({"kind": "reconcile_state", "target": target})
    assert policy.dispatch_route == "reconcile_state"
    assert not policy.requires_target and not policy.requires_execution
    assert not policy.action_ownership and not policy.permits_action_promotion


@pytest.mark.parametrize("field,value", [
    ("execution", {"run_id": "source"}), ("action_reservation_id", "action"),
    ("retry_of", "old-work"), ("retry_attempt", 1), ("retry_attempt", False),
    ("finalization_id", "incident"),
])
def test_target_reference_cannot_promote_reconciliation_into_action_work(field, value):
    with pytest.raises(ValueError, match="Reconciliation"):
        work_policy({"kind": "reconcile_state", "target": {"reference_only": True}, field: value})


def test_sql_reconciliation_uses_only_its_work_lease_and_protected_cas(kernel):
    resolve = _sql(kernel, "controller.resolve_frontier")
    assert "N'controller:'" not in resolve
    assert "Current work owner or fence was lost" in resolve
    assert "Reconciliation lost its immutable intent/evidence/receipt binding" in resolve
    assert "Accepted frontier changed" in resolve
    claim = _sql(kernel, "controller.claim_work")
    assert claim.index("Reconciliation is not an action-capable work family") < claim.index("IF @linked_action")
    assert "IF @stored_work_kind<>'reconcile_state'" in claim
    assert "Reconciliation cannot be promoted into action work" in claim
    for operation in ("controller.transition_action", "controller.finalize"):
        families = re.search(r"@stored_work_kind NOT IN \((.*?)\)", _sql(kernel, operation))[1]
        assert "reconcile_state" not in families
        assert all(kind in families for kind in ACTION_WORK_KINDS)
    transition = _sql(kernel, "controller.transition_work")
    assert "Reconciliation cannot attach incident finalization lineage" in transition
    assert "operation='controller.resolve_frontier'" in transition


def test_frontier_rpc_and_publication_contract_is_explicit():
    contract = integration_contract()
    assert "controller.inspect_frontiers" in contract["rpcs"]
    assert "controller.resolve_frontier" in contract["rpcs"]
    assert "frontier_validation" in contract["controller_publication_contracts"]
    assert "frontier_digest" in contract["controller_publication_contracts"]["reservation_validation"]
    assert contract["work_policy"]["reconcile_state"]["dispatch_route"] == "reconcile_state"
    assert not contract["work_policy"]["reconcile_state"]["action_ownership"]


def test_accepted_handoff_digest_hashes_an_empty_evidence_set(kernel):
    """A web intent with no accepted_fact rows must still produce a digest.

    FOR JSON PATH returns NULL rather than an empty array when nothing matches,
    and the handoff payload is built without INCLUDE_NULL_VALUES, so a NULL
    digest dropped the key and ValidationHandoff refused to decode. Every
    controller heartbeat then failed with "A persisted monitoring record is
    unreadable" -- triggered by the first discovery a user queues.
    """
    builders = [obj for obj in kernel.objects if "@frontier_evidence" in obj.ddl]
    assert builders, "No kernel object builds the frontier evidence digest"
    for obj in builders:
        assert "COALESCE(@frontier_evidence,N'[]')" in obj.ddl, obj.logical_name
        assert "+@frontier_evidence)" not in obj.ddl, (
            f"{obj.logical_name} still concatenates the evidence set without COALESCE"
        )


def test_no_correlation_guard_compares_against_a_bare_handoff_value(kernel):
    """A missing handoff field must fail the guard, not silently pass it.

    SQL comparisons against NULL are UNKNOWN, so `<>JSON_VALUE(@handoff, ...)`
    contributes nothing to an OR chain when the key is absent: the proof
    correlation check failed open exactly when the handoff was malformed. Every
    sibling condition already COALESCEs both sides.
    """
    for obj in kernel.objects:
        assert "<>JSON_VALUE(@handoff," not in obj.ddl, (
            f"{obj.logical_name} compares a proof value against a bare handoff value"
        )


def test_producer_handoff_emits_an_empty_evidence_array_not_json_null(kernel):
    producers = [obj for obj in kernel.objects if "AS evidence," in obj.ddl]
    assert producers
    for obj in producers:
        assert "FOR JSON PATH),N'[]')) AS evidence," in obj.ddl, obj.logical_name


def _proof_rejected(db, kernel, proof, handoff):
    sql = _sql(kernel, "controller.resolve_frontier")
    predicate = re.search(
        r"IF (@proof IS NULL OR COALESCE\(@decision,.*?)(?=\n    THROW 51072, 'Controller proof)",
        sql, re.S,
    )[1]
    return bool(db.execute(
        f"SELECT CASE WHEN {predicate} THEN 1 ELSE 0 END FROM (SELECT 'bigint' AS bigint)",
        {
            "proof": json.dumps(proof), "handoff": json.dumps(handoff),
            "decision": "published", "work_id": "work", "owner_id": "owner",
            "fence": 7, "work_revision": 3, "current_revision": 0,
            "frontier_key": "frontier", "accepted": 1, "producer_request_id": "request",
        },
    ).fetchone()[0])


@pytest.mark.parametrize("field", ["producer_fingerprint", "evidence_digest"])
@pytest.mark.parametrize("side", ["proof", "handoff", "both"])
@pytest.mark.parametrize("value", ["missing", None, ""])
def test_missing_correlation_fields_cannot_match_through_empty_sentinels(
    decision_db, kernel, field, side, value,
):
    handoff = {"producer_fingerprint": "a" * 64, "evidence_digest": "b" * 64}
    proof = {
        **handoff, "decision": "published", "work_id": "work", "lease_owner_id": "owner", "lease_fence": 7,
        "expected_work_revision": 3, "policy_revision": 0, "frontier_key": "frontier",
        "through_revision": 1, "producer_request_id": "request", "detail": "Validated original intent.",
    }
    assert not _proof_rejected(decision_db, kernel, proof, handoff)
    for document in ([proof, handoff] if side == "both" else [proof if side == "proof" else handoff]):
        if value == "missing":
            document.pop(field)
        else:
            document[field] = value
    assert _proof_rejected(decision_db, kernel, proof, handoff)
