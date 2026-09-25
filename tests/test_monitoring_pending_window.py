from __future__ import annotations

import json
from copy import deepcopy

import pytest
from pydantic import ValidationError
from test_monitoring_sql_review9_bindings import Review9Database
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringCommitUncertain, MonitoringConflict
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.sql_store import AzureSqlMonitoringStore


def partial_inventory(backend="sql"):
    h = Harness()
    db = Review9Database(h) if backend == "sql" else None
    if db:
        db.principal = "controller"
    controller = (
        AzureSqlMonitoringStore(db=db, component="controller") if db else
        InMemoryMonitoringStore(state=h.state, clock=h.clock, component="controller")
    )
    draft = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=0,
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        created_at=h.clock(), due_at=h.clock(), reason="Paginated inventory fixture.",
    ))
    if db:
        db.principal = "worker"
    worker = (
        AzureSqlMonitoringStore(db=db, component="worker") if db else
        InMemoryMonitoringStore(state=h.state, clock=h.clock, component="worker")
    )
    collection, = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(902), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))
    generation = worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=draft.work_id, selector=draft.discovery_selector,
            adapter="fixture", authority="tenant_admin", completeness="partial",
            started_at=h.clock(), continuation="second-page",
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="More pages remain."),),
        ),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision, expected_generation_revision=0,
        ),
    ))
    if db:
        db.principal = "controller"
    first, = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(903), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))
    pending = controller.reconcile_work(first)
    assert pending.state == "pending_validation"
    if db:
        db.principal = "worker"
    updated = worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration.model_validate({
            **generation.model_dump(), "completed_pages": 1, "continuation": "third-page",
        }),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision,
            expected_generation_revision=generation.revision,
            expected_continuation=generation.continuation,
        ),
    ))
    h.clock.advance(16)
    if db:
        db.principal = "controller"
    claimed = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(904), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
    ))
    retry = next(work for work in claimed if work.work_id == first.work_id)
    return h, db, controller, worker, collection, updated, first, pending, retry


@pytest.mark.parametrize("backend", ["sql", "memory"])
def test_retry_of_a_published_page_preserves_its_decision_and_unfinished_window(backend):
    h, db, controller, _, _, _, first, pending, retry = partial_inventory(backend)
    before = controller.get_validation_frontier(h.version, pending.frontier_key)
    assert before.accepted_revision == 2 and before.validated_revision == 0
    protected = {
        key: deepcopy(row) for key, row in db.records.items()
        if row.kind in {"validation_frontier", "validation_handoff", "validation_window"}
    } if db else {}

    result = controller.reconcile_work(retry)

    assert result.state == "pending_validation"
    assert result.resolution_scope == "pending_window_acknowledgement"
    assert result.handoff_resolution_request_id == pending.request_id
    assert result.handoff_resolution_work_fence == first.lease.fence
    assert controller.get_work(h.version, retry.work_id).state == "waiting"
    assert controller.get_validation_frontier(h.version, pending.frontier_key) == before
    assert controller.reconcile_work(first) == pending
    assert controller.reconcile_work(retry) == result
    if db:
        assert all(db.records[key] == row for key, row in protected.items())


@pytest.mark.parametrize("change", [
    None,
    {"work_id": uid(999)},
    {"producer_request_id": uid(998)},
    {"frontier_key": "another-window"},
    {"handoff_revision": 2},
    {"handoff_decision": "rejected"},
    {"resolution_scope": "window"},
    {"state": "published"},
    {"work_fence": 100},
    {"validated_revision": 1},
])
def test_pending_acknowledgement_requires_the_exact_original_page_receipt(change):
    h, db, controller, _, _, _, _, pending, retry = partial_inventory()
    key = ("controller.resolve_frontier", pending.request_id)
    if change is None:
        del db.receipts[key]
    else:
        db.receipts[key]["payload"]["result"].update(change)
    before = deepcopy((db.records, db.receipts))

    with pytest.raises(MonitoringConflict, match="51072") as refused:
        controller.reconcile_work(retry)

    assert "original page decision receipt" in str(refused.value.__cause__)
    assert (db.records, db.receipts) == before
    assert controller.get_work(h.version, retry.work_id).state == "leased"


def test_pending_acknowledgement_lost_ack_replays_without_changing_original_decisions():
    h, db, controller, _, _, _, first, pending, retry = partial_inventory()
    db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        controller.reconcile_work(retry)
    records, receipts = deepcopy(db.records), deepcopy(db.receipts)

    restarted = AzureSqlMonitoringStore(db=db, component="controller")
    result = restarted.reconcile_work(retry)

    assert result.state == "pending_validation"
    assert result.handoff_resolution_request_id == pending.request_id
    assert restarted.reconcile_work(first) == pending
    assert (db.records, db.receipts) == (records, receipts)
    assert restarted.get_work(h.version, retry.work_id).state == "waiting"


def test_a_later_complete_page_can_close_the_window_after_pending_acknowledgement():
    h, db, controller, worker, collection, generation, first, pending, retry = partial_inventory()
    assert controller.reconcile_work(retry).state == "pending_validation"
    db.principal = "worker"
    worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration.model_validate({
            **generation.model_dump(), "completed_pages": 2, "continuation": None,
            "completeness": "complete", "completed_at": h.clock(), "gaps": (),
        }),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
            expected_generation_revision=generation.revision, expected_continuation=generation.continuation,
        ),
    ))
    db.principal = "controller"
    h.clock.advance(16)
    claimed = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(905), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
    ))
    results = [controller.reconcile_work(work) for work in claimed]
    for _ in range(3):
        h.clock.advance(121)
        claimed = controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(906), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
        ))
        results.extend(controller.reconcile_work(work) for work in claimed)
        if not controller.get_validation_frontier(h.version, pending.frontier_key).pending:
            break
    frontier = controller.get_validation_frontier(h.version, pending.frontier_key)
    assert frontier.accepted_revision == frontier.validated_revision == 3
    assert any(result.state == "published" for result in results)
    assert controller.reconcile_work(first) == pending


@pytest.mark.parametrize("change", ["absent", "fingerprint", "binding", "work", "frontier", "revision"])
def test_pending_acknowledgement_does_not_replace_original_intake_authority(change):
    h, db, controller, _, _, _, _, pending, retry = partial_inventory()
    producer = controller.get_reconciliation_request(
        h.version, retry.reconcile_request_id, producer="worker",
    )
    handoff = next(
        row for row in db.records.values()
        if row.kind == "validation_handoff" and row.parent_key == producer.frontier_key
        and row.sequence_number == producer.frontier_revision
    )
    binding = json.loads(handoff.payload)
    key = (binding["producer_operation"], binding["producer_request_id"])
    receipt = db.receipts[key]
    if change == "absent":
        del db.receipts[key]
    elif change == "fingerprint":
        receipt["fingerprint"] = "c" * 64
    elif change == "binding":
        receipt["payload"]["binding_hash"] = "c" * 64
    else:
        name, value = {
            "work": ("reconcile_work_id", uid(990)),
            "frontier": ("frontier_key", "another-window"),
            "revision": ("frontier_revision", 99),
        }[change]
        receipt["payload"]["result"][name] = value
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringConflict, match="51072"):
        controller.reconcile_work(retry)
    assert (db.records, db.receipts) == before
    assert controller.get_validation_frontier(h.version, pending.frontier_key).pending


def test_pending_acknowledgement_does_not_prevent_explicit_whole_window_rejection():
    h, _, controller, _, _, _, first, pending, retry = partial_inventory()
    assert controller.reconcile_work(retry).state == "pending_validation"
    h.clock.advance(16)
    work = next(
        row for row in controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(907), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
        )) if row.work_id == first.work_id
    )
    result = controller.reconcile_state(m.ReconcileStateRequest(
        **h.context(), request_id=h.next_id(), work_id=work.work_id, lease=work.lease,
        expected_work_revision=work.revision, expected_policy_revision=0,
        expected_frontier_revision=2, reject_whole_window=True,
        detail="Explicitly abandon this incomplete collection without changing prior page decisions.",
    ))
    assert result.state == "rejected" and result.resolution_scope == "window"
    assert not controller.get_validation_frontier(h.version, pending.frontier_key).pending
    assert controller.reconcile_work(first) == pending


@pytest.mark.parametrize("change", [
    {"state": "published"},
    {"validated_revision": 2},
    {"handoff_decision": "pending_validation"},
    {"handoff_resolution_request_id": None},
    {"handoff_resolution_work_fence": 4},
    {"frontier_resolution_request_id": uid(90)},
    {"frontier_resolution_revision": 1},
    {"window_resolution_request_id": uid(91)},
])
def test_pending_acknowledgement_result_cannot_claim_terminal_authority(change):
    value = {
        "work_id": uid(1), "work_fence": 3, "producer_request_id": uid(2),
        "frontier_key": "validation:pending-window", "frontier_revision": 2, "validated_revision": 0,
        "handoff_revision": 1, "handoff_decision": "published", "state": "pending_validation",
        "resolution_scope": "pending_window_acknowledgement",
        "handoff_resolution_request_id": uid(3), "handoff_resolution_work_fence": 1,
        "frontier_resolution_request_id": None, "frontier_resolution_revision": None,
        "window_rejection_request_id": None, "window_resolution_request_id": None, "window_resolution_state": None,
    }
    m.FrontierResolution.model_validate(value)
    with pytest.raises(ValidationError):
        m.FrontierResolution.model_validate({**value, **change})


@pytest.mark.parametrize("value", ["true", 1, None, "false", {}])
def test_pending_acknowledgement_flag_is_strict(value):
    h, _, _, _, _, _, _, _, retry = partial_inventory()
    with pytest.raises(ValidationError):
        m.FrontierValidation(
            validation_id=uid(980), work_id=retry.work_id, lease_owner_id=retry.lease.owner_id,
            lease_fence=retry.lease.fence, expected_work_revision=retry.revision,
            policy_revision=h.version.revision, frontier_key="validation:pending-window", through_revision=2,
            producer_request_id=retry.reconcile_request_id, producer_fingerprint="1" * 64,
            evidence_digest="2" * 64, decision="published", detail="Retain the original page.",
            acknowledge_pending_window=value,
        )
