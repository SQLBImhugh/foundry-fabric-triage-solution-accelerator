from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta

import pytest
from pydantic import ValidationError
from test_monitoring_sql_review9_bindings import connector_commit
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
)
from triage.monitoring.events import (
    StreamStartRequest,
    UnidentifiedReceiptBatch,
    UnidentifiedSignal,
)
from triage.monitoring.memory import InMemoryMonitoringStore, key_digest


def components(h: Harness):
    return {
        name: InMemoryMonitoringStore(clock=h.clock, state=h.state, component=name)
        for name in ("worker", "web", "controller")
    }


def ready(workload: m.Workload = "fabric_pipeline"):
    h = Harness()
    h.seed(workload=workload)
    h.activate()
    return h, components(h)


def current(store, h):
    control = store.snapshot(m.MonitoringContext(**h.context())).control
    return m.RegistryVersion(**h.context(), revision=control.revision)


def drain(stores, h):
    return stores["controller"].drain_reconciliation(
        m.MonitoringContext(**h.context()), owner_id=uid(900),
    )


def poll_page(h, stores, *, workload="fabric_pipeline", complete=False, row=None, cursor=None, poll=None):
    worker = stores["worker"]
    poll = poll or worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(901), kinds=("poll",), limit=1, per_workspace_limit=1,
    ))[0]
    progress = worker.get_poll_progress(h.targets[0])
    window = m.ObservationWindow(start_at=h.clock() - timedelta(minutes=30), end_at=h.clock())
    observation = row or h.observation(execution={
        "target": h.targets[0], "run_id": uid(40_001),
        "run_id_kind": "fabric_job" if workload == "fabric_pipeline" else "powerbi_request",
    })
    request = m.RestPageRequest(
        page_id=h.next_id(), target=h.targets[0], policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease,
        expected_checkpoint_revision=progress.checkpoint.revision if progress else 0,
        expected_cursor=progress.checkpoint.cursor if progress else None,
        next_cursor=None if complete else cursor or "next-page", window=window,
        received_count=1, observed_at=h.clock(), window_complete=complete,
        observations=(observation,) if workload == "fabric_pipeline" else (),
        powerbi_rows=(m.PowerBIWindowRow(observation=observation, refresh_id="23"),) if workload == "powerbi" else (),
        powerbi_window_complete=complete if workload == "powerbi" else False,
    )
    return poll, request, worker.record_rest_page(request)


@pytest.mark.parametrize("component,operation", [
    ("worker", "request_discovery"), ("worker", "record_safety_review"),
    ("worker", "enqueue_work"), ("worker", "reserve_action"), ("worker", "reconcile_work"),
    ("web", "claim_work"), ("web", "record_inventory"), ("web", "record_rest_page"),
    ("web", "record_action_outcome"), ("web", "finalize_work"),
    ("controller", "activate_scope"), ("controller", "record_capability"),
    ("controller", "record_stream_receipts"), ("controller", "change_partition_ownership"),
])
def test_component_rejects_an_operation_before_parsing_or_mutating(component, operation):
    h = Harness()
    store = components(h)[component]
    before = dict(h.state.records)
    with pytest.raises(MonitoringComponentDenied):
        getattr(store, operation)(None)
    assert h.state.records == before


@pytest.mark.parametrize("component,kinds", [
    ("worker", ("triage",)), ("worker", ("reconcile_state",)),
    ("controller", ("poll",)), ("controller", ("inventory",)),
    ("worker", ("poll", "reconcile_state")),
])
def test_claim_checks_actual_component_work_family(component, kinds):
    h = Harness()
    with pytest.raises(MonitoringComponentDenied):
        components(h)[component].claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(50), kinds=kinds,
        ))
    assert not h.state.leases


def test_targetless_discovery_is_immutable_controller_handoff_not_inventory_or_action():
    h = Harness()
    stores = components(h)
    selector = m.ScopeSelector(tenant_id=uid(1), kind="tenant")
    request_id = h.next_id()
    work = stores["web"].request_discovery(h.version, selector, request_id=request_id)
    assert work.kind == "reconcile_state" and work.target is None and work.execution is None
    assert work.reconcile_request_id == request_id and work.reconcile_producer == "web"
    assert not stores["worker"].claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(902), kinds=("inventory",),
    ))
    results = drain(stores, h)
    assert len(results) == 1 and results[0].state == "published"
    persisted = stores["controller"].get_work(h.version, work.work_id)
    assert persisted.state == "completed" and persisted.finalization_id is None
    assert all(not lease.resource_key.startswith("controller:") for lease in h.state.leases.values())
    assert not h.state.incidents and not h.state.approvals
    inventory = stores["worker"].claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(902), kinds=("inventory",),
    ))
    assert len(inventory) == 1 and inventory[0].discovery_selector == selector
    replay = stores["web"].request_discovery(h.version, selector, request_id=request_id)
    assert replay == work
    assert stores["controller"].get_work(h.version, work.work_id) == persisted
    producer = stores["controller"].get_reconciliation_request(h.version, request_id, producer="web")
    assert not stores["controller"].get_validation_frontier(h.version, producer.frontier_key).pending
    with pytest.raises(MonitoringConflict):
        stores["web"].request_discovery(
            h.version, m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=uid(88)),
            request_id=request_id,
        )


@pytest.mark.parametrize("field,value", [
    ("execution", {"target": {"tenant_id": uid(1), "epoch": uid(2), "workload": "powerbi",
                             "workspace_id": uid(3), "item_id": uid(4)},
                   "run_id": uid(5), "run_id_kind": "powerbi_request"}),
    ("action_reservation_id", uid(6)), ("retry_of", uid(7)),
    ("retry_attempt", 1), ("finalization_id", uid(8)),
])
def test_reconciliation_work_cannot_carry_executable_or_retry_lineage(field, value):
    h = Harness()
    work = components(h)["web"].request_discovery(
        h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id(),
    )
    with pytest.raises(ValidationError):
        m.MonitoringWork.model_validate({**work.model_dump(mode="json"), field: value})


def test_scope_intent_fences_before_target_publication_and_receipt_replay_never_reopens_work():
    h, stores = ready()
    h.source_work()
    review = h.review()
    reservation = h.reserve_request(review)
    disabled = m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False})
    preview = stores["web"].preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=disabled,
    ))
    request = m.ActivateScopeRequest(
        expected=h.version, plan_id=preview.plan_id, idempotency_id=preview.idempotency_id,
    )
    receipt = stores["web"].activate_scope(request)
    assert receipt.state == "configuring" and receipt.version.revision == h.version.revision + 1
    assert stores["controller"].reserve_action(reservation).denial == "stale_policy"
    new_request = m.ActionReservationRequest.model_validate({
        **reservation.model_dump(), "idempotency_id": h.next_id(), "expected": receipt.version,
    })
    assert stores["controller"].reserve_action(new_request).denial == "pending_validation"
    assert not h.state.incidents
    before = stores["controller"].resolve_target(h.targets[0], include_inactive=True)
    assert before.state == "current" and before.policy_revision == h.version.revision
    assert drain(stores, h)[0].state == "published"
    assert stores["controller"].resolve_target(h.targets[0], include_inactive=True).state == "paused"
    assert stores["web"].activate_scope(request) == receipt
    assert stores["controller"].get_work(receipt.version, receipt.queued_work_ids[0]).state == "completed"


def test_async_disable_preserves_readable_admission_from_another_unchanged_scope():
    h, stores = ready()
    second = m.ScopeDefinition.model_validate({
        **h.scope.model_dump(), "scope_id": uid(23),
        "rules": ({**h.scope.rules[0].model_dump(), "rule_id": uid(24)},),
    })
    h.activate(second)
    plan = stores["web"].preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(),
        scope=m.ScopeDefinition.model_validate({**second.model_dump(), "enabled": False}),
    ))
    stores["web"].activate_scope(m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    assert drain(stores, h)[0].state == "published"
    target = stores["controller"].resolve_target(h.targets[0])
    assert target is not None and target.observation.enabled
    assert target.scope_ids == (uid(20),)


def test_review_intent_is_pending_not_caller_technical_proof_and_original_receipt_survives_publication():
    h, stores = ready()
    review = h.review()
    request = m.SafetyReviewRequest(
        request_id=h.next_id(), expected=h.version, expected_review_revision=review.revision,
        review=m.SafetyReview.model_validate({
            **review.model_dump(), "revision": review.revision + 1,
            "state": "revoked", "revoked_at": h.clock(),
        }),
    )
    pending = stores["web"].record_safety_review(request)
    assert pending.state == "pending" and pending.requested_state == "revoked"
    assert pending.publication_status == "pending_validation" and not pending.exact_correlation_verified
    original = stores["web"].get_safety_review_operation(h.version, request.request_id)
    assert original.review == pending and original.expected == h.version
    assert original.new_review_revision == 2
    assert stores["web"].get_safety_review(h.version, review.review_id) == pending
    assert drain(stores, h)[0].state == "published"
    published = stores["controller"].get_safety_review(h.version, review.review_id)
    assert published.state == "revoked" and published.publication_status == "published"
    assert stores["web"].get_safety_review_operation(h.version, request.request_id) == original
    assert stores["web"].record_safety_review(request) == pending
    assert not stores["controller"].resolve_target(h.targets[0]).action.enabled


def test_pending_review_does_not_prevent_recording_an_already_reserved_uncertain_effect():
    h, stores = ready()
    h.source_work()
    review = h.review()
    action = h.store.reserve_action(h.reserve_request(review)).reservation
    request = m.SafetyReviewRequest(
        request_id=h.next_id(), expected=h.version, expected_review_revision=review.revision,
        review=m.SafetyReview.model_validate({
            **review.model_dump(), "revision": 2, "state": "revoked", "revoked_at": h.clock(),
        }),
    )
    stores["web"].record_safety_review(request)
    work = stores["controller"].get_work(h.version, action.request.work_id)
    saved = stores["controller"].record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state="uncertain", submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(minutes=1),
        detail="Lost submission acknowledgement; preserve the existing external-effect fence.",
    ), commit=m.CollectionCommit(
        work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
    ))
    assert saved.state == "uncertain" and saved.request == action.request
    assert h.store.get_incident_state(action.request.incident).action_count == 1
    assert h.state.approvals[action.request.approval.approval_id]["consumed_at"]


def test_missing_first_projection_and_producer_completion_cannot_release_partial_window_fence():
    h, stores = ready()
    h.source_work()
    review = h.review()
    request = h.reserve_request(review)
    poll, _, receipt = poll_page(h, stores)
    assert stores["worker"].get_poll_progress(h.targets[0]).checkpoint == receipt.checkpoint
    assert stores["controller"].get_rest_checkpoint(h.targets[0]) is None
    assert not stores["controller"].resolve_target(h.targets[0]).action.enabled
    assert stores["controller"].coverage(h.version).action_enabled_count == 0
    assert "pending_validation" in {gap.code for gap in stores["controller"].coverage(h.version).gaps}
    assert stores["controller"].reserve_action(request).denial == "pending_validation"
    stores["worker"].disposition_work(m.WorkDispositionRequest(
        **h.context(), request_id=h.next_id(), work_id=poll.work_id, lease=poll.lease,
        expected_work_revision=poll.revision, disposition="cancelled", detail="Collector stopped before next page.",
    ))
    result = drain(stores, h)[0]
    assert result.state == "pending_validation"
    assert stores["controller"].get_work(h.version, result.work_id).state == "waiting"
    assert stores["controller"].get_validation_frontier(h.version, result.frontier_key).pending
    again = m.ActionReservationRequest.model_validate({
        **request.model_dump(), "idempotency_id": h.next_id(),
    })
    assert stores["controller"].reserve_action(again).denial == "pending_validation"
    assert not stores["controller"].get_source(h.observation(execution={
        "target": h.targets[0], "run_id": uid(40_001), "run_id_kind": "fabric_job",
    }).execution)


def test_cross_page_alias_conflict_never_admits_early_sources_even_after_poll_completion():
    h, stores = ready("powerbi")
    first = h.observation(execution={
        "target": h.targets[0], "run_id": uid(41_001), "run_id_kind": "powerbi_request",
    })
    second = h.observation(execution={
        "target": h.targets[0], "run_id": uid(41_002), "run_id_kind": "powerbi_request",
    })
    poll, _, first_receipt = poll_page(h, stores, workload="powerbi", row=first)
    _, final_request, final_receipt = poll_page(
        h, stores, workload="powerbi", row=second, poll=poll, complete=True,
    )
    assert stores["controller"].get_powerbi_window(h.version, final_receipt.checkpoint.powerbi_window_id) is None
    assert not stores["controller"].get_source(first.execution)
    assert not stores["controller"].get_source(second.execution)
    assert stores["controller"].get_work(h.version, poll.work_id).state == "completed"
    results = drain(stores, h)
    assert len(results) == 2 and all(result.state == "rejected" for result in results)
    staged = stores["controller"].get_powerbi_window(h.version, final_receipt.checkpoint.powerbi_window_id)
    assert staged.state == "quarantined" and staged.quarantined_count == 2
    assert not stores["controller"].get_source(first.execution)
    assert not stores["controller"].get_source(second.execution)
    assert stores["controller"].get_rest_checkpoint(h.targets[0]).coverage_through is None
    frontier = stores["controller"].get_validation_frontier(h.version, results[-1].frontier_key)
    assert not frontier.pending
    assert stores["worker"].record_rest_page(final_request) == final_receipt
    assert stores["controller"].get_validation_frontier(h.version, frontier.frontier_key) == frontier
    assert first_receipt.intake.publication_status == "pending_validation"


def test_complete_window_publication_uses_its_own_fence_not_the_targets_action_lease():
    h, stores = ready()
    h.source_work()
    original = stores["controller"].resolve_target(h.targets[0], include_inactive=True)
    _, _, accepted = poll_page(h, stores, complete=True)
    assert stores["controller"].resolve_target(h.targets[0], include_inactive=True) == original
    expected_due = h.clock() + timedelta(seconds=original.observation.cadence.poll_seconds)
    assert stores["controller"].resolve_target(h.targets[0]).next_poll_at == expected_due
    assert stores["controller"].coverage(h.version).next_due_at == expected_due
    action_lease = next(lease for lease in h.state.leases.values() if lease.resource_key.startswith("controller:"))
    result = drain(stores, h)[0]
    assert result.state == "published"
    assert stores["controller"].get_rest_checkpoint(h.targets[0]).coverage_through == h.clock()
    source = h.observation(execution={
        "target": h.targets[0], "run_id": uid(40_001), "run_id_kind": "fabric_job",
    })
    assert stores["controller"].get_source(source.execution) == source
    assert h.state.leases[(uid(1), uid(2), key_digest(action_lease.resource_key))] == action_lease
    assert stores["worker"].get_rest_page(h.version, accepted.intake.request_id) == accepted


def test_publication_rechecks_raw_binding_and_lease_in_same_transaction():
    h, stores = ready()
    _, _, accepted = poll_page(h, stores, complete=True)
    work = stores["controller"].claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(910), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    original = next(row for row in h.state.records.values() if row.kind == "rest_observation")
    changed = replace(original, version=original.version + 1, payload=original.payload.replace("fixture-failure", "changed-failure"))
    h.state.records[(uid(1), uid(2), original.kind, key_digest(original.key))] = changed
    result = stores["controller"].reconcile_work(work)
    assert result.state == "rejected"
    assert stores["controller"].get_rest_checkpoint(h.targets[0]) is None
    assert stores["worker"].get_rest_page(h.version, accepted.intake.request_id) == accepted
    assert stores["controller"].reconcile_work(work) == result


def test_expired_reconciliation_cannot_publish_under_a_replacement_fence():
    h, stores = ready()
    poll_page(h, stores, complete=True)
    claim = m.WorkClaimRequest(
        **h.context(), owner_id=uid(911), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    )
    old = stores["controller"].claim_work(claim)[0]
    h.clock.advance(121)
    replacement = stores["controller"].claim_work(m.WorkClaimRequest.model_validate({
        **claim.model_dump(), "owner_id": uid(912),
    }))[0]
    with pytest.raises(MonitoringLeaseLost):
        stores["controller"].reconcile_work(old)
    assert stores["controller"].get_rest_checkpoint(h.targets[0]) is None
    assert stores["controller"].reconcile_work(replacement).state == "published"


def test_lost_acceptance_ack_replays_original_receipt_without_raising_validated_frontier_again(monkeypatch):
    h = Harness()
    stores = components(h)
    backend = stores["web"]._backend
    transaction = backend.transaction

    @contextmanager
    def lost_ack(**kwargs):
        with transaction(**kwargs):
            yield
        raise MonitoringCommitUncertain("discovery", request_id)

    request_id = h.next_id()
    selector = m.ScopeSelector(tenant_id=uid(1), kind="tenant")
    with monkeypatch.context() as patch:
        patch.setattr(backend, "transaction", lost_ack)
        with pytest.raises(MonitoringCommitUncertain):
            stores["web"].request_discovery(h.version, selector, request_id=request_id)
    result = drain(stores, h)[0]
    frontier = stores["controller"].get_validation_frontier(h.version, result.frontier_key)
    assert not frontier.pending
    replay = stores["web"].request_discovery(h.version, selector, request_id=request_id)
    assert replay.work_id == result.work_id and replay.state == "queued"
    assert stores["controller"].get_validation_frontier(h.version, result.frontier_key) == frontier
    assert stores["controller"].get_work(h.version, result.work_id).state == "completed"


def test_worker_inventory_requires_current_owner_and_never_publishes_targets_or_probes():
    h = Harness()
    stores = components(h)
    draft = m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=0,
        created_at=h.clock(), due_at=h.clock(), reason="Explicit owned generation.",
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
    )
    stores["controller"].enqueue_work(draft)
    claim = m.WorkClaimRequest(
        **h.context(), owner_id=uid(601), kinds=("inventory",), limit=1, per_workspace_limit=1,
    )
    work = stores["worker"].claim_work(claim)[0]
    generation = m.InventoryGeneration(
        **h.context(), generation_id=work.work_id, selector=work.discovery_selector,
        adapter="fixture owned collector", authority="tenant_admin", completeness="complete",
        started_at=h.clock(), completed_at=h.clock(), completed_pages=1, discovered_count=1,
    )
    item = m.InventoryItem(
        **h.context(), generation_id=work.work_id, workspace_id=uid(602), item_id=uid(603),
        name="Fixture item", item_type="DataPipeline", workload="fabric_pipeline", observed_at=h.clock(),
    )
    batch = m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=generation, items=(item,),
    )
    with pytest.raises(MonitoringConflict, match="require current work ownership"):
        stores["worker"].record_inventory(batch)
    owned = m.InventoryBatch.model_validate({
        **batch.model_dump(), "commit": m.InventoryCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_generation_revision=0,
        ),
    })
    committed = stores["worker"].record_inventory(owned)
    assert committed.recorded_item_count == 1
    assert stores["controller"].resolve_target(item.target, include_inactive=True) is None
    assert not any(row.work_kind == "capability_probe" for row in h.state.records.values())
    assert drain(stores, h)[0].state == "published"
    assert any(row.work_kind == "capability_probe" for row in h.state.records.values())
    assert stores["worker"].record_inventory(owned) == committed
    assert not drain(stores, h)
    h.clock.advance(121)
    replacement = stores["worker"].claim_work(m.WorkClaimRequest.model_validate({
        **claim.model_dump(), "owner_id": uid(604),
    }))[0]
    assert replacement.lease.fence > work.lease.fence
    with pytest.raises(MonitoringLeaseLost):
        stores["worker"].record_inventory(m.InventoryBatch.model_validate({
            **owned.model_dump(), "request_id": h.next_id(),
        }))
    assert stores["worker"].get_inventory_generation(h.version, work.work_id) == committed


def test_capability_completion_is_raw_evidence_not_action_authority():
    h, stores = ready()
    target = h.targets[0]
    original = h.store.resolve_target(target)
    probe = stores["worker"].claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(611), kinds=("capability_probe",), limit=1, per_workspace_limit=1,
    ))[0]
    denied = m.CapabilityObservation(
        capability_id=h.next_id(), target=target, inventory_generation=h.generation_id,
        collector_identity_id=uid(612), read_status="denied", action_status="denied",
        checked_at=h.clock(), expires_at=h.clock() + timedelta(minutes=30),
        gaps=(m.CoverageGap(code="http_403", detail="Current service identity cannot read this item."),),
    )
    with pytest.raises(MonitoringConflict, match="exact work fence"):
        stores["worker"].complete_collection_work(
            h.version, work_id=probe.work_id, lease=probe.lease, expected_work_revision=probe.revision,
        )
    with pytest.raises(MonitoringConflict, match="collection work fence"):
        stores["worker"].record_capability(h.version, denied)
    stores["worker"].record_capability(h.version, denied, commit=m.CollectionCommit(
        work_id=probe.work_id, lease=probe.lease, expected_work_revision=probe.revision,
    ))
    stores["worker"].complete_collection_work(
        h.version, work_id=probe.work_id, lease=probe.lease, expected_work_revision=probe.revision,
    )
    assert stores["controller"].resolve_target(target, include_inactive=True) == original
    producer = stores["controller"].get_reconciliation_request(h.version, denied.capability_id, producer="worker")
    assert stores["controller"].get_validation_frontier(h.version, producer.frontier_key).pending
    assert drain(stores, h)[0].state == "published"
    assert stores["controller"].resolve_target(target) is None
    assert stores["controller"].resolve_target(target, include_inactive=True).state == "paused"


def test_stream_positions_and_unidentified_receipts_fence_until_controller_disposition():
    h, stores = ready()
    h.connector()
    signal = h.signal(101)
    partition = signal.partition
    lease = stores["worker"].claim_partition(m.PartitionClaimRequest(
        partition=partition, owner_id=uid(620),
    ))
    stores["worker"].ensure_stream_start(StreamStartRequest(
        partition=partition, lease=lease, first_available_sequence_number=101, observed_at=h.clock(),
    ))
    first = stores["worker"].record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=partition, lease=lease, receipts=(signal,),
    ))
    again = m.SignalReceipt.model_validate({
        **signal.model_dump(), "position": m.StreamPosition(offset="1020", sequence_number=102, enqueued_at=h.clock()),
    })
    second = stores["worker"].record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=partition, lease=lease, receipts=(again,),
    ))
    unidentified = stores["worker"].record_unidentified_receipts(UnidentifiedReceiptBatch(
        request_id=h.next_id(), lease=lease, receipt=UnidentifiedSignal(
            partition=partition, position=m.StreamPosition(offset="1030", sequence_number=103, enqueued_at=h.clock()),
            received_at=h.clock(), quarantine=m.QuarantineDisposition(
                observation_id="malformed", reason="malformed", detail="No trustworthy CloudEvents identity.",
            ),
        ),
    ))
    checkpoint = stores["worker"].advance_stream_checkpoint(m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=partition, lease=lease, expected_revision=0,
        through=m.StreamPosition(offset="1030", sequence_number=103, enqueued_at=h.clock()),
    ))
    assert checkpoint.position.sequence_number == 103
    assert first.receipt_keys == second.receipt_keys
    assert all(receipt.publication_status == "pending_validation" for receipt in (first, second, unidentified))
    assert not stores["controller"].get_source(signal.observation.execution)
    assert len([row for row in h.state.records.values() if row.kind == "stream_position"]) == 3
    results = drain(stores, h)
    assert sorted(result.state for result in results) == ["published", "published", "rejected"]
    assert not stores["controller"].get_validation_frontier(h.version, results[-1].frontier_key).pending
    assert stores["controller"].get_source(signal.observation.execution).authority == "transport"
    assert stores["worker"].get_stream_acceptance(h.version, unidentified.request_id) == unidentified


def test_worker_cannot_rewrite_desired_connector_scope_or_definition():
    h, stores = ready()
    original = h.connector()
    candidate = m.OwnedConnectorManifest.model_validate({
        **original.model_dump(), "revision": original.revision + 1,
        "desired_definition": {"sources": ["different-owned-source"]},
        "observed_definition": {"sources": ["different-owned-source"]},
    })
    with pytest.raises(MonitoringComponentDenied, match="controller-owned desired"):
        stores["worker"].record_connector(
            h.version, candidate, expected_connector_revision=original.revision,
            commit=connector_commit(h, stores["worker"], original.connector_id),
        )
    assert stores["worker"].list_connectors(m.PageQuery(**h.context())).items == (original,)
