"""Original-receipt lifecycle guards; SQLite execution is not native SQL proof."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import timedelta

import pytest
from pydantic import ValidationError
from test_monitoring_connector_retirement_store import scoped_connector
from test_monitoring_controller_publication_integration import ProvisioningSqlDatabase
from test_monitoring_sql_receiver_bindings import record_delivery_evidence
from test_monitoring_sql_removals import (
    _adapt,
    _evaluate,
    _id,
    _insert_record,
    _json,
    _mutation_params,
    _save_receipt,
    _sql,
)
from test_monitoring_sql_removals import case as case
from test_monitoring_sql_removals import db as db
from test_monitoring_sql_review9_bindings import connector_commit
from test_monitoring_sql_review9_v2 import _condition
from test_monitoring_sql_window_siblings import _transition
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
)
from triage.monitoring.memory import stable_id
from triage.monitoring.sql_kernel_common import record_insert
from triage.monitoring.sql_kernel_frontiers import (
    closed_window_authority_sql,
    handoff_decision_change_sql,
    nonwindow_handoff_authority_sql,
    page_publication_required_sql,
    stale_page_policy_sql,
)
from triage.monitoring.sql_kernel_intake import (
    connector_collection_eligible_sql,
    connector_collection_work_invalid_sql,
)
from triage.monitoring.sql_kernel_work import (
    connector_collection_completion_sql,
    reconciliation_completion_sql,
)
from triage.monitoring.sql_permissions import build_permission_kernel


def _nonwindow_case(db, case):
    kernel, connector = case
    records = kernel.names.table("monitoring_records")
    work_id, owner_id, producer_id = _id(200), _id(201), _id(202)
    frontier_key, handoff_key = "nonwindow-connector-frontier", "nonwindow-connector-frontier:handoff:1"
    work_key = f"work:v1:{_id(2)}:{_id(1)}:{work_id}"
    handoff = {
        "frontier_key": frontier_key, "frontier_revision": 1,
        "producer": "worker", "producer_operation": "worker.observe_connector",
        "producer_request_id": producer_id, "producer_fingerprint": "f" * 64,
        "producer_binding_hash": "A" * 64, "work_id": work_id, "policy_revision": 1,
        "evidence_digest": "B" * 64, "requires_window": False,
    }
    _insert_record(db, kernel, "validation_handoff", handoff_key, handoff)
    db.execute(f"UPDATE {records} SET status='published',parent_key=?,sequence_number=1 "
               "WHERE record_kind='validation_handoff' AND full_key=?", (frontier_key, handoff_key))
    for sequence in (2, 3, 4):
        sibling = {**handoff, "frontier_revision": sequence,
                   "work_id": _id(205) if sequence == 4 else _id(210 + sequence),
                   "producer_request_id": _id(206) if sequence == 4 else _id(215 + sequence)}
        key = f"{frontier_key}:handoff:{sequence}"
        _insert_record(db, kernel, "validation_handoff", key, sibling)
        db.execute(f"UPDATE {records} SET status='published',parent_key=?,sequence_number=? "
                   "WHERE record_kind='validation_handoff' AND full_key=?", (frontier_key, sequence, key))
    for row in db.execute(f"SELECT payload FROM {records} WHERE record_kind='validation_handoff'").fetchall():
        producer = json.loads(row[0])
        receipt_params = _mutation_params(connector, producer["producer_request_id"])
        receipt_params["binding_hash"] = "A" * 64
        _save_receipt(db, kernel, "worker.observe_connector", receipt_params, {
            "connector_id": connector["connector_id"], "reconcile_work_id": producer["work_id"],
            "frontier_key": frontier_key, "frontier_revision": producer["frontier_revision"],
        })
    _insert_record(db, kernel, "validation_frontier", frontier_key, {
        "frontier_key": frontier_key, "accepted_revision": 4, "validated_revision": 4,
    })
    db.execute(f"UPDATE {records} SET status='published',sequence_number=4 "
               "WHERE record_kind='validation_frontier' AND full_key=?", (frontier_key,))
    _insert_record(db, kernel, "frontier_commit", frontier_key, {
        "request_id": _id(204), "frontier_key": frontier_key, "frontier_revision": 4, "decision": "published",
    })
    db.execute(f"UPDATE {records} SET status='published',sequence_number=4 WHERE record_kind='frontier_commit'")
    work = {
        "tenant_id": _id(1), "epoch": _id(2), "work_id": work_id, "kind": "reconcile_state",
        "state": "leased", "revision": 7, "policy_revision": 1, "retry_attempt": 0,
        "reconcile_producer": "worker", "reconcile_request_id": producer_id,
        "lease": {"owner_id": owner_id, "fence": 2},
    }
    _insert_record(db, kernel, "work", work_id, work, revision=7)
    db.execute(f"UPDATE {records} SET work_kind='reconcile_state' WHERE record_kind='work' AND full_key=?", (work_id,))
    leases = kernel.names.table("monitoring_leases")
    db.execute(f"""INSERT INTO {leases}
        (tenant_id,epoch,full_key,key_hash,owner_id,fence,expires_at)
        VALUES (?,?,?,KEY_DIGEST(?),?,2,'2030')""", (_id(1), _id(2), work_key, work_key, owner_id))
    params = {
        "tenant_id": _id(1), "epoch": _id(2), "work_id": work_id, "owner_id": owner_id,
        "work_key": work_key, "fence": 2, "work_revision": 7, "stored_work": _json(work),
        "stored_work_kind": "reconcile_state", "producer_request_id": producer_id,
        "frontier_key": frontier_key, "accepted": 4, "validated": 4,
        "handoff_key": handoff_key, "handoff_revision": 1, "handoff": _json(handoff),
        "handoff_state": "published", "current_revision": 2, "maintenance": 0,
        "whole_window_rejection": 0, "window_ack": 0, "handoff_ack": 0, "decision": "published",
        "now": "2026-09-16T12:00:00Z", "completed_at": "2026-09-16T12:00:00Z",
        "new_expiry": "2026-09-16T12:00:00Z", "retry_at": None, "detail": "Original handoff is already resolved.",
    }
    old_result = {
        "work_id": work_id, "work_fence": 1, "producer_request_id": producer_id,
        "frontier_key": frontier_key, "frontier_revision": 4, "validated_revision": 0,
        "handoff_revision": 1,
        "state": "pending_validation", "handoff_decision": "published", "resolution_scope": "handoff",
    }
    root_result = {
        "work_id": _id(205), "work_fence": 1, "producer_request_id": _id(206),
        "frontier_key": frontier_key, "frontier_revision": 4, "validated_revision": 4,
        "handoff_revision": 4,
        "state": "published", "handoff_decision": "published", "resolution_scope": "handoff",
    }
    for request_id, result in ((_id(203), old_result), (_id(204), root_result)):
        _save_receipt(db, kernel, "controller.resolve_frontier", _mutation_params(connector, request_id), result)
    db.execute("UPDATE dbo.control SET revision=2")
    db.commit()
    return kernel, params


def test_c2_nonwindow_counterexample_requires_a_separate_terminal_ack(db, case):
    kernel, params = _nonwindow_case(db, case)
    assert _evaluate(db, kernel, page_publication_required_sql(), params)
    assert _evaluate(db, kernel, stale_page_policy_sql(), params)
    assert _evaluate(db, kernel, handoff_decision_change_sql(), {**params, "decision": "rejected"})
    assert db.execute(_adapt(kernel, closed_window_authority_sql(kernel.names)), params).fetchall() == []
    assert db.execute(_adapt(kernel, reconciliation_completion_sql(kernel.names)), params).fetchall() == []
    assert db.execute(f"SELECT COUNT(*) FROM {kernel.names.table('monitoring_records')} "
                      "WHERE record_kind='validation_window'").fetchone()[0] == 0


def test_c2_unlinked_connector_observation_is_not_collection_completion_authority(db, case):
    kernel, connector = case
    work_id, owner_id = _id(220), _id(221)
    params = {
        "tenant_id": _id(1), "epoch": _id(2), "work_id": work_id, "owner_id": owner_id,
        "fence": 4, "work_revision": 7, "transition": "complete", "current_revision": 3,
        "stored_work_kind": "connector_reconcile", "stored_work": _json({
            "kind": "connector_reconcile", "work_id": work_id, "connector_id": connector["connector_id"],
        }),
    }
    _save_receipt(db, kernel, "worker.observe_connector", _mutation_params(connector, _id(222)), {
        "connector_id": connector["connector_id"], "connector": connector, "observation": connector,
        "observed_definition_hash": "A" * 64, "authority": "observed_not_action_authority",
        "reconcile_work_id": _id(223), "frontier_key": "independent-connector-frontier", "frontier_revision": 1,
    })
    body = _sql(kernel, "worker.transition_work")
    refusal = _condition(body, "Collection completion requires a committed accepted batch or eligible original connector observation")
    assert _evaluate(db, kernel, refusal, params)


def _acknowledge_handoff(db, kernel, params, request_id):
    """Run the emitted receipt authority and append-only mutations in this fixture."""
    records, receipts, leases = (kernel.names.table(name) for name in (
        "monitoring_records", "monitoring_receipts", "monitoring_leases",
    ))
    with db:
        if not db.execute(
            "SELECT 1 FROM dbo.control WHERE tenant_id=@tenant_id AND epoch=@epoch AND revision=@current_revision",
            params,
        ).fetchone():
            raise ValueError("current context/policy mismatch")
        if not db.execute(f"SELECT 1 FROM {leases} WHERE tenant_id=@tenant_id AND epoch=@epoch "
                          "AND full_key=@work_key AND owner_id=@owner_id AND fence=@fence AND expires_at>@now",
                          params).fetchone():
            raise ValueError("current lease mismatch")
        if not db.execute(f"SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch "
                          "AND record_kind='work' AND full_key=@work_id AND revision=@work_revision AND status='leased'",
                          params).fetchone():
            raise ValueError("current work revision mismatch")
        assert not db.execute(f"SELECT 1 FROM {receipts} WHERE operation='controller.resolve_frontier' AND request_id=?",
                              (request_id,)).fetchone()
        rows = db.execute(_adapt(kernel, nonwindow_handoff_authority_sql(kernel.names)), params).fetchall()
        if not rows:
            raise ValueError("original handoff/prefix authority mismatch")
        original_id, original_fence, prefix_id, prefix_revision = min(rows, key=lambda row: (row[1], row[0]))
        result = m.FrontierResolution(
            work_id=params["work_id"], work_fence=params["fence"], producer_request_id=params["producer_request_id"],
            frontier_key=params["frontier_key"], frontier_revision=params["accepted"],
            handoff_revision=params["handoff_revision"], validated_revision=params["validated"],
            state=params["handoff_state"], handoff_decision=params["handoff_state"],
            resolution_scope="handoff_acknowledgement",
            handoff_resolution_request_id=original_id, handoff_resolution_work_fence=original_fence,
            frontier_resolution_request_id=prefix_id, frontier_resolution_revision=prefix_revision,
            window_rejection_request_id=None, window_resolution_request_id=None, window_resolution_state=None,
        ).model_dump(mode="json")
        values = {**params, "request_id": request_id, "acceptance_key": request_id,
                  "result": _json(result), "result_state": result["state"], "binding_hash": "C" * 64}
        db.execute(_adapt(kernel, record_insert(
            kernel.names, "reconcile_acceptance", "@acceptance_key", "@result", status="@result_state",
            parent_key="@frontier_key", sequence="@accepted",
        )), values)
        _save_receipt(db, kernel, "controller.resolve_frontier", values, result)
        return result


def _later_pending(db, kernel, params):
    records = kernel.names.table("monitoring_records")
    db.execute(f"UPDATE {records} SET status='pending_validation',sequence_number=5,"
               "payload=json_set(payload,'$.accepted_revision',5) "
               "WHERE record_kind='validation_frontier' AND full_key=?", (params["frontier_key"],))
    _insert_record(db, kernel, "validation_handoff", "later-pending", {"state": "pending_validation"})
    db.execute(f"UPDATE {records} SET parent_key=?,sequence_number=5 WHERE full_key='later-pending'",
               (params["frontier_key"],))
    db.commit()
    params["accepted"] = 5


@pytest.mark.parametrize("later_pending", [False, True])
@pytest.mark.parametrize("transition", ["complete", "disposition"])
def test_nonwindow_acknowledgement_finishes_current_work_without_mutating_original_prefix(
    db, case, later_pending, transition,
):
    kernel, params = _nonwindow_case(db, case)
    if later_pending:
        _later_pending(db, kernel, params)
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    protected = db.execute(f"SELECT * FROM {records} WHERE record_kind<>'work' ORDER BY record_kind,full_key").fetchall()
    originals = db.execute(f"SELECT * FROM {receipts} ORDER BY operation,request_id").fetchall()
    control = db.execute("SELECT * FROM dbo.control").fetchall()
    result = _acknowledge_handoff(db, kernel, params, _id(240))
    assert result["handoff_resolution_request_id"] == _id(203)
    assert result["handoff_resolution_work_fence"] == 1
    assert result["frontier_resolution_request_id"] == _id(204)
    assert result["state"] == "published" and result["work_fence"] == 2
    assert result["validated_revision"] == result["frontier_resolution_revision"] == 4
    assert result["frontier_revision"] == (5 if later_pending else 4)
    assert db.execute(_adapt(kernel, reconciliation_completion_sql(kernel.names)), params).fetchone()
    finished = _transition(db, kernel, params, transition, _id(241))
    assert finished["state"] == ("completed" if transition == "complete" else "dispositioned")
    assert finished["policy_revision"] == 1 and "lease" not in finished
    assert db.execute(f"SELECT * FROM {records} WHERE record_kind NOT IN ('work','reconcile_acceptance') "
                      "ORDER BY record_kind,full_key").fetchall() == protected
    assert db.execute(f"SELECT * FROM {receipts} WHERE request_id NOT IN (?,?) ORDER BY operation,request_id",
                      (_id(240), _id(241))).fetchall() == originals
    assert db.execute("SELECT * FROM dbo.control").fetchall() == control
    assert not db.execute(f"SELECT 1 FROM {records} WHERE record_kind='validation_window'").fetchone()
    if later_pending:
        assert db.execute(f"SELECT status FROM {records} WHERE record_kind='validation_frontier' AND full_key=?",
                          (params["frontier_key"],)).fetchone()[0] == "pending_validation"


@pytest.mark.parametrize("change", [
    {"tenant_id": _id(300)}, {"epoch": _id(301)}, {"current_revision": 1},
    {"owner_id": _id(302)}, {"fence": 1}, {"work_revision": 6}, {"accepted": 5},
    {"decision": "rejected"}, {"now": "2031"},
])
def test_nonwindow_acknowledgement_denies_wrong_current_context_or_ownership(db, case, change):
    kernel, params = _nonwindow_case(db, case)
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    before = (
        db.execute(f"SELECT * FROM {records}").fetchall(), db.execute(f"SELECT * FROM {receipts}").fetchall(),
    )
    with pytest.raises(ValueError, match="mismatch"):
        _acknowledge_handoff(db, kernel, {**params, **change}, _id(240))
    assert before == (
        db.execute(f"SELECT * FROM {records}").fetchall(), db.execute(f"SELECT * FROM {receipts}").fetchall(),
    )


@pytest.mark.parametrize("record_kind,request_id,change", [
    ("receipt", _id(203), {"work_id": _id(900)}),
    ("receipt", _id(203), {"work_fence": 3}),
    ("receipt", _id(203), {"producer_request_id": _id(900)}),
    ("receipt", _id(203), {"handoff_revision": 2}),
    ("receipt", _id(203), {"frontier_revision": 5}),
    ("receipt", _id(203), {"resolution_scope": "window_acknowledgement"}),
    ("receipt", _id(204), {"work_id": _id(900)}),
    ("receipt", _id(204), {"producer_request_id": _id(900)}),
    ("receipt", _id(204), {"frontier_key": "wrong-root"}),
    ("receipt", _id(204), {"frontier_revision": 3, "validated_revision": 3}),
    ("receipt", _id(204), {"state": "pending_validation"}),
    ("prefix", None, {"status": "pending_validation"}),
    ("prefix", None, {"delete": True}),
    ("producer", _id(202), {"delete": True}),
    ("commit", None, {"request_id": _id(900)}),
    ("window", None, {}),
])
def test_nonwindow_acknowledgement_requires_exact_original_receipts_and_resolved_prefix(
    db, case, record_kind, request_id, change,
):
    kernel, params = _nonwindow_case(db, case)
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    if record_kind == "receipt":
        original = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE request_id=?", (request_id,)).fetchone()[0])
        original["result"].update(change)
        db.execute(f"UPDATE {receipts} SET payload=? WHERE request_id=?", (_json(original), request_id))
    elif record_kind == "prefix":
        if change.get("delete"):
            db.execute(f"DELETE FROM {records} WHERE record_kind='validation_handoff' AND sequence_number=2")
        else:
            db.execute(f"UPDATE {records} SET status=? WHERE record_kind='validation_handoff' AND sequence_number=2",
                       (change["status"],))
    elif record_kind == "producer":
        db.execute(f"DELETE FROM {receipts} WHERE operation='worker.observe_connector' AND request_id=?", (request_id,))
    elif record_kind == "commit":
        db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.request_id',?) WHERE record_kind='frontier_commit'",
                   (change["request_id"],))
    else:
        _insert_record(db, kernel, "validation_window", params["frontier_key"], {"state": "validated"})
    db.commit()
    with pytest.raises(ValueError, match="authority mismatch"):
        _acknowledge_handoff(db, kernel, params, _id(240))


@pytest.mark.parametrize("field,value", [
    ("handoff_resolution_request_id", _id(901)), ("handoff_resolution_work_fence", 9),
    ("frontier_resolution_request_id", _id(901)), ("frontier_resolution_revision", 3),
    ("handoff_revision", 2), ("producer_request_id", _id(901)), ("work_fence", 1),
])
def test_nonwindow_completion_rechecks_both_original_receipt_references(db, case, field, value):
    kernel, params = _nonwindow_case(db, case)
    _acknowledge_handoff(db, kernel, params, _id(240))
    receipts = kernel.names.table("monitoring_receipts")
    db.execute(f"UPDATE {receipts} SET payload=json_set(payload,?,?) WHERE request_id=?",
               ("$.result." + field, value, _id(240)))
    db.commit()
    with pytest.raises(ValueError, match="own terminal receipt"):
        _transition(db, kernel, params, "complete", _id(241))


@pytest.mark.parametrize("value", [None, 0, 1, "true", "false", [], {}])
def test_handoff_acknowledgement_flag_is_strict_in_model_and_emitted_sql(db, case, value):
    kernel, _ = case
    guard = _condition(_sql(kernel, "controller.resolve_frontier"), "Handoff acknowledgement mode must be one strict JSON boolean")
    assert _evaluate(db, kernel, guard, {"proof": _json({"acknowledge_handoff": value})})
    with pytest.raises(ValidationError):
        m.FrontierValidation.model_validate({
            "validation_id": _id(241), "work_id": _id(200), "lease_owner_id": _id(201), "lease_fence": 2,
            "expected_work_revision": 7, "policy_revision": 2, "frontier_key": "frontier", "through_revision": 4,
            "producer_request_id": _id(202), "producer_fingerprint": "f" * 64, "evidence_digest": "b" * 64,
            "decision": "published", "detail": "Acknowledge the original committed handoff.", "acknowledge_handoff": value,
        })


def test_connector_observation_abi_has_only_explicit_collection_inputs():
    contract = build_permission_kernel().rpcs["worker.observe_connector"]
    assert [(parameter.name, parameter.sql_type, parameter.nullable) for parameter in contract.parameters] == [
        ("tenant_id", "nvarchar(36)", False), ("epoch", "nvarchar(36)", False),
        ("request_id", "nvarchar(256)", False), ("fingerprint", "char(64)", False),
        ("expected_revision", "bigint", False), ("work_id", "nvarchar(128)", False),
        ("owner_id", "nvarchar(128)", False), ("fence", "bigint", False), ("work_revision", "bigint", False),
        ("connector_id", "nvarchar(128)", False), ("expected_connector_revision", "bigint", False),
        ("ownership_id", "nvarchar(128)", False), ("observation_json", "nvarchar(max)", False),
    ]
    assert {"work_id", "work_owner_id", "work_fence", "work_revision", "collection_completion_eligible"}.issubset(
        contract.result_fields,
    )


@pytest.mark.parametrize("patch,eligible", [
    ({"state": "provisioning", "observed_definition": {}}, False),
    ({"state": "ready", "observed_definition": {}}, True),
    ({"state": "ready"}, False),
    ({"state": "ready", "observed_definition": None}, False),
    ({"state": "blocked", "gaps": [{"code": "review_required", "detail": "Durable review disposition."}]}, True),
    ({"state": "degraded", "gaps": [{"code": "awaiting_delivery_proof", "detail": "No proof observed."}]}, True),
    ({"state": "degraded", "gaps": []}, False),
    ({"state": "blocked"}, False),
    ({"gaps": [{"code": "review_required", "detail": "Inherited state is not a disposition."}]}, False),
    ({"state": "degraded", "gaps": [{"code": "definition_update_outcome_unknown", "detail": "Uncertain effect."}]}, False),
    ({"state": "blocked", "gaps": [{"code": "definition_update_submitted_or_unknown", "detail": "Pre-submit."}]}, False),
])
def test_sql_derives_collection_eligibility_from_explicit_observation_input(db, case, patch, eligible):
    kernel, _ = case
    assert _evaluate(db, kernel, connector_collection_eligible_sql(), {"observation_json": _json(patch)}) == eligible


@pytest.mark.parametrize("change", [
    {"kind": "poll"}, {"state": "completed"}, {"connector_id": _id(900)},
    {"target": {}}, {"execution": {}}, {"action_reservation_id": _id(900)},
    {"retry_of": _id(900)}, {"finalization_id": _id(900)}, {"retry_attempt": 1},
    {"lease": {"owner_id": _id(900), "fence": 4}},
    {"lease": {"owner_id": _id(231), "fence": 3}},
])
def test_emitted_connector_observation_guard_rejects_foreign_work_or_controller_lineage(db, case, change):
    kernel, connector = case
    work = {"kind": "connector_reconcile", "state": "leased", "connector_id": connector["connector_id"],
            "retry_attempt": 0, "lease": {"owner_id": _id(231), "fence": 4}}
    work.update(change)
    assert _evaluate(db, kernel, connector_collection_work_invalid_sql(), {
        "stored_work": _json(work), "stored_work_kind": work["kind"], "stored_work_status": work["state"],
        "stored_work_revision": 7, "work_revision": 7, "connector_id": connector["connector_id"],
        "owner_id": _id(231), "fence": 4, "stored_target_key": None,
    })


@pytest.mark.parametrize("field,value", [
    ("work_id", _id(900)), ("work_owner_id", _id(900)), ("work_fence", 3), ("work_revision", 8),
    ("connector_id", _id(900)), ("collection_completion_eligible", False), ("collection_completion_eligible", "true"),
    ("observation.tenant_id", _id(900)), ("observation.epoch", _id(900)),
    ("observation.policy_revision", 2), ("observation.connector_id", _id(900)),
])
def test_emitted_completion_uses_only_original_current_work_fenced_observation(db, case, field, value):
    kernel, connector = case
    result = {
        "work_id": _id(230), "work_owner_id": _id(231), "work_fence": 4, "work_revision": 7,
        "connector_id": connector["connector_id"], "collection_completion_eligible": True, "observation": connector,
    }
    params = {
        **_mutation_params(connector, _id(232)), "work_id": _id(230), "owner_id": _id(231), "fence": 4,
        "work_revision": 7, "stored_work_kind": "connector_reconcile",
        "stored_work": _json({"connector_id": connector["connector_id"]}),
    }
    _save_receipt(db, kernel, "worker.observe_connector", params, result)
    query = _adapt(kernel, connector_collection_completion_sql(kernel.names))
    assert db.execute(query, params).fetchone()
    db.execute(f"UPDATE {kernel.names.table('monitoring_receipts')} SET payload=json_set(payload,?,json(?)) "
               "WHERE request_id=?", ("$.result." + field, _json(value), _id(232)))
    assert not db.execute(query, params).fetchone()


def _record_disposition(h, db, worker, connector, commit, *, state="blocked", delivery_proof=None):
    observation = m.OwnedConnectorManifest.model_validate({
        **connector.model_dump(), "revision": connector.revision + 1, "state": state,
        "observed_definition": connector.desired_definition,
        "identity_verified_at": h.clock() if state == "ready" else None,
        "delivery_verified_at": h.clock() if state == "ready" else None,
        "delivery_proof": delivery_proof,
        "gaps": () if state == "ready" else (m.CoverageGap(code="review_required", detail="Durable original disposition."),),
        "updated_at": h.clock(),
    })
    if db:
        db.principal = "worker"
    effective = worker.record_connector(
        h.version, observation, expected_connector_revision=connector.revision, commit=commit,
    )
    receipt_id = stable_id(h.version, f"connector:{connector.connector_id}:{connector.revision}")
    receipt = worker.get_operation_receipt(h.version, "worker.observe_connector", receipt_id)
    return effective, observation, receipt


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("state", ["ready", "blocked", "degraded"])
def test_collection_completion_survives_same_fence_renewal_and_never_uses_latest_connector(backend, state):
    h, db, _, _, worker, connector = scoped_connector(backend, database_type=ProvisioningSqlDatabase)
    proof = None
    if state == "ready":
        connector, proof = record_delivery_evidence(h, worker, connector, db=db)
    commit = connector_commit(h, worker, connector.connector_id, db=db)
    effective, _, receipt = _record_disposition(
        h, db, worker, connector, commit, state=state, delivery_proof=proof,
    )
    original = m.ConnectorObservationResult.model_validate(receipt.result)
    assert original.collection_completion_eligible
    assert (original.work_id, original.work_owner_id, original.work_fence, original.work_revision) == (
        commit.work_id, commit.lease.owner_id, commit.lease.fence, commit.expected_work_revision,
    )
    other = connector_commit(h, worker, connector.connector_id, db=db)
    worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **effective.model_dump(), "revision": effective.revision + 1,
        "state": "provisioning", "observed_definition": None,
    }), expected_connector_revision=effective.revision, commit=other)
    frontier = worker.get_validation_frontier(h.version, original.frontier_key)
    assert frontier.pending
    h.clock.advance(1)
    worker.renew_lease(m.LeaseRenewal(lease=commit.lease, lease_seconds=120))
    current = worker.get_work(h.version, commit.work_id)
    assert current.revision >= original.work_revision and current.lease.fence == original.work_fence
    assert current.lease.expires_at > commit.lease.expires_at
    if db:
        assert current.revision > original.work_revision
    finished = worker.complete_collection_work(
        h.version, work_id=current.work_id, lease=current.lease, expected_work_revision=current.revision,
    )
    assert finished.state == "completed"
    assert worker.get_validation_frontier(h.version, original.frontier_key) == frontier
    assert worker.get_operation_receipt(h.version, "worker.observe_connector", receipt.request_id) == receipt
    with pytest.raises(MonitoringConflict):
        worker.complete_collection_work(
            h.version, work_id=other.work_id, lease=other.lease, expected_work_revision=other.expected_work_revision,
        )


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_reclaimed_collection_requires_new_observation_and_keeps_original_receipt_fence(backend):
    h, db, _, _, worker, connector = scoped_connector(backend, database_type=ProvisioningSqlDatabase)
    commit = connector_commit(h, worker, connector.connector_id, db=db)
    effective, observation, receipt = _record_disposition(h, db, worker, connector, commit)
    worker.disposition_work(m.WorkDispositionRequest(
        **h.context(), request_id=h.next_id(), work_id=commit.work_id, lease=commit.lease,
        expected_work_revision=commit.expected_work_revision, disposition="retry",
        retry_at=h.clock() + timedelta(seconds=1), detail="Reclaim this actual collection work in the fixture.",
    ))
    h.clock.advance(2)
    work, = [entry for entry in worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(990), kinds=("connector_reconcile",), limit=200, per_workspace_limit=200,
    )) if entry.work_id == commit.work_id]
    assert work.lease.fence > commit.lease.fence
    assert worker.record_connector(
        h.version, observation, expected_connector_revision=connector.revision, commit=commit,
    ) == effective
    assert worker.get_operation_receipt(h.version, "worker.observe_connector", receipt.request_id) == receipt
    with pytest.raises(MonitoringConflict):
        worker.complete_collection_work(
            h.version, work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
        )
    fresh_commit = m.CollectionCommit(work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision)
    _, _, fresh = _record_disposition(h, db, worker, effective, fresh_commit)
    assert fresh.request_id != receipt.request_id and fresh.result["work_fence"] == work.lease.fence
    assert worker.complete_collection_work(
        h.version, work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
    ).state == "completed"


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("change", ["missing", "owner", "fence", "revision", "context", "resource", "work"])
def test_connector_observation_requires_actual_collection_commit_without_mutating_state(backend, change):
    h, db, _, _, worker, connector = scoped_connector(backend, database_type=ProvisioningSqlDatabase)
    commit = connector_commit(h, worker, connector.connector_id, db=db)
    changes = {
        "missing": None,
        "owner": commit.model_copy(update={"lease": commit.lease.model_copy(update={"owner_id": uid(999)})}),
        "fence": commit.model_copy(update={"lease": commit.lease.model_copy(update={"fence": commit.lease.fence + 1})}),
        "revision": commit.model_copy(update={"expected_work_revision": commit.expected_work_revision - 1}),
        "context": commit.model_copy(update={"lease": commit.lease.model_copy(update={"tenant_id": uid(999)})}),
        "resource": commit.model_copy(update={"lease": commit.lease.model_copy(update={"resource_key": "different-work"})}),
        "work": commit.model_copy(update={"work_id": uid(999)}),
    }
    before = deepcopy((db.records, db.receipts) if db else (h.state.records, h.state.receipts))
    with pytest.raises((MonitoringComponentDenied, MonitoringConflict, MonitoringLeaseLost)):
        _record_disposition(h, db, worker, connector, changes[change])
    assert before == ((db.records, db.receipts) if db else (h.state.records, h.state.receipts))


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_current_policy_observation_can_finish_old_work_without_relabeling_old_receipt(backend):
    h, db, web, controller, worker, connector = scoped_connector(backend, database_type=ProvisioningSqlDatabase)
    commit = connector_commit(h, worker, connector.connector_id, db=db)
    effective, observation, receipt = _record_disposition(h, db, worker, connector, commit)
    original_version = h.version
    if db:
        db.principal = "web"
    scope = web.list_scopes(m.PageQuery(**h.context())).items[0]
    preview = web.preview_scope(m.ScopePreviewRequest(
        expected=original_version, idempotency_id=h.next_id(),
        scope=m.ScopeDefinition.model_validate({
            **scope.model_dump(exclude={"revision", "updated_at"}), "name": "Reviewed collection-policy revision",
        }),
    ))
    web.activate_scope(m.ActivateScopeRequest(
        expected=preview.expected, idempotency_id=preview.idempotency_id, plan_id=preview.plan_id,
    ))
    if db:
        db.principal = "worker"
    current_version = m.RegistryVersion(**h.context(), revision=worker.snapshot(original_version).control.revision)
    assert current_version.revision > original_version.revision
    assert worker.record_connector(
        original_version, observation, expected_connector_revision=connector.revision, commit=commit,
    ) == effective
    with pytest.raises(MonitoringConflict):
        worker.complete_collection_work(
            current_version, work_id=commit.work_id, lease=commit.lease, expected_work_revision=commit.expected_work_revision,
        )
    if db:
        db.principal = "controller"
    for _ in range(10):
        claimed = controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(993), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
        ))
        if not claimed:
            break
        for reconciliation in claimed:
            controller.reconcile_work(reconciliation)
    else:
        pytest.fail("Current-policy controller reconciliation did not reach bounded idle state")
    published = next(value for value in controller.list_connectors(m.PageQuery(**h.context())).items
                     if value.connector_id == connector.connector_id)
    assert published.policy_revision == current_version.revision
    if db:
        db.principal = "worker"
    current_work = worker.get_work(current_version, commit.work_id)
    fresh = m.OwnedConnectorManifest.model_validate({
        **published.model_dump(), "revision": published.revision + 1, "state": "blocked",
        "gaps": (m.CoverageGap(code="review_required", detail="Current-policy durable disposition."),),
    })
    worker.record_connector(current_version, fresh, expected_connector_revision=published.revision, commit=m.CollectionCommit(
        work_id=current_work.work_id, lease=current_work.lease, expected_work_revision=current_work.revision,
    ))
    finished = worker.complete_collection_work(
        current_version, work_id=current_work.work_id, lease=current_work.lease, expected_work_revision=current_work.revision,
    )
    assert finished.state == "completed" and finished.policy_revision == original_version.revision
    assert worker.get_operation_receipt(original_version, "worker.observe_connector", receipt.request_id) == receipt
