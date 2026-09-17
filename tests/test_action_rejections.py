from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from test_monitoring_controller import configured as configured
from test_monitoring_sql_store import SqlHarness, SqliteAzureDatabase, SqlProtocolFixtureStore
from test_monitoring_store import Harness

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.store.retries import MAX_ATTEMPTS, backoff_seconds


@pytest.fixture(params=("memory", "sql"))
def h(request, tmp_path):
    harness = Harness() if request.param == "memory" else SqlHarness(tmp_path / "rejections.sqlite")
    harness.seed(workload="powerbi")
    harness.activate()
    harness.source_work()
    harness.reviewed = harness.review("powerbi_refresh")
    return harness


def reserve(h):
    request = h.reserve_request(h.reviewed, approval=False)
    decision = h.store.reserve_action(request)
    assert decision.status == "reserved", decision.detail
    return decision.reservation


def rejection(h, action, *, reason="throttled", retry_after=42):
    work = h.store.get_work(m.MonitoringContext(**h.context()), action.request.work_id)
    return m.ActionRejectionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        work_id=work.work_id, lease=work.lease,
        evidence=m.ActionRejectionEvidence(
            reason=reason, attempted_at=h.clock(), rejected_at=h.clock(),
            retry_after_seconds=retry_after,
        ),
        detail="Synthetic definitive rejection, not an accepted or uncertain submission.",
    )


def finalize(h, action):
    return h.store.finalize_work(h.finalization_input(
        action=action, outcome="deferred_retry" if action.retry_work_id else "needs_human",
    ))


def next_work(h, rejected):
    work = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
    h.clock.advance(max(0, int((work.due_at - h.clock()).total_seconds())) + 1)
    claimed = h.claim(("deferred_retry",))
    assert len(claimed) == 1
    h.work = claimed[0]
    assert h.work.work_id == rejected.retry_work_id
    h.source = h.source.model_copy(update={"observed_at": h.clock()})
    h.store.observe_source(h.source, work_id=h.work.work_id, lease=h.work.lease)
    return h.work


def test_rejection_is_no_effect_and_successor_reuses_only_the_reserved_slot(h) -> None:
    original = reserve(h)
    request = rejection(h, original)
    rejected = h.store.record_action_rejection(request)
    assert rejected.state == "rejected"
    assert rejected.submitted_execution is None
    assert rejected.submitted_at is None
    assert rejected.next_verification_at is None
    assert rejected.rejection == request.evidence
    assert h.store.get_incident_state(original.request.incident).action_count == 1
    successor = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
    assert successor.retry_of == original.reservation_id
    assert successor.retry_attempt == 1
    assert successor.execution == original.request.source_execution
    assert successor.due_at == h.clock() + timedelta(seconds=42)

    # A receipt for rejection alone is not terminal incident/work finalization.
    h.clock.advance(43)
    assert h.claim(("deferred_retry",)) == ()
    finalize(h, rejected)
    h.work = h.claim(("deferred_retry",))[0]
    h.source = h.source.model_copy(update={"observed_at": h.clock()})
    h.store.observe_source(h.source, work_id=h.work.work_id, lease=h.work.lease)
    child = reserve(h)
    assert child.retry_of == rejected.reservation_id
    assert child.retry_attempt == 1
    assert child.fence > rejected.fence
    assert h.store.get_incident_state(child.request.incident).action_count == 1
    current_parent = h.store.get_action_reservation(m.MonitoringContext(**h.context()), rejected.reservation_id)
    assert current_parent.retry_reservation_id == child.reservation_id

    # Replaying the old rejection receipt cannot release the child's ownership.
    assert h.store.record_action_rejection(request).retry_work_id == successor.work_id
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), child.reservation_id).state == "reserved"


@pytest.mark.parametrize("state", ("submitted", "uncertain"))
def test_neither_accepted_nor_uncertain_submissions_can_be_released_as_rejected(h, state) -> None:
    action = reserve(h)
    submitted = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state=state,
        submitted_execution=m.SourceExecutionIdentity(
            target=h.source.execution.target, run_id_kind="powerbi_request", run_id=h.next_id(),
        ) if state == "submitted" else None,
        correlation="response_run_id" if state == "submitted" else None,
        submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(seconds=30),
        detail="Existing submitted or uncertain effect.",
    ))
    with pytest.raises(MonitoringConflict, match="original unsubmitted reservation"):
        h.store.record_action_rejection(rejection(h, submitted))
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id) == submitted
    assert h.store.get_incident_state(action.request.incident).action_count == 1


def test_definitive_non_throttling_rejection_does_not_schedule_a_retry(h) -> None:
    action = reserve(h)
    rejected = h.store.record_action_rejection(rejection(h, action, reason="definitive_client_error", retry_after=0))
    assert rejected.state == "rejected"
    assert rejected.retry_work_id is None
    finalize(h, rejected)
    assert h.claim(("deferred_retry",)) == ()
    # No global budget refund that another source could spend.
    assert h.store.get_incident_state(action.request.incident).action_count == 1


def test_no_effect_evidence_cannot_carry_a_fabricated_submitted_run(h) -> None:
    action = reserve(h)
    request = rejection(h, action)
    with pytest.raises(ValidationError):
        m.ActionRejectionRequest.model_validate({
            **request.model_dump(), "submitted_execution": h.source.execution.model_dump(),
        })
    with pytest.raises(ValidationError):
        m.ActionRejectionEvidence(
            reason="uncertain", attempted_at=h.clock(), rejected_at=h.clock(),
        )
    rejected = h.store.record_action_rejection(request)
    with pytest.raises(ValidationError):
        m.ActionReservation.model_validate({
            **rejected.model_dump(), "submitted_at": h.clock(),
        })


def test_rejection_and_successor_creation_roll_back_together(h, monkeypatch) -> None:
    action = reserve(h)
    original = h.store._save_action

    def fail(record):
        if record.state == "rejected":
            raise MonitoringUnavailable("Synthetic rejection journal write failure")
        return original(record)

    monkeypatch.setattr(h.store, "_save_action", fail)
    with pytest.raises(MonitoringUnavailable):
        h.store.record_action_rejection(rejection(h, action))
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id) == action
    h.clock.advance(43)
    assert h.claim(("deferred_retry",)) == ()
    assert h.store.get_incident_state(action.request.incident).action_count == 1


@pytest.mark.parametrize("initial_deferred", (False, True))
def test_throttled_successors_are_bounded_and_cannot_spawn_unlinked_work(h, initial_deferred) -> None:
    first_attempt = 0
    if initial_deferred:
        h.store.finalize_work(h.finalization_input(outcome="deferred_retry"))
        h.clock.advance(1)
        h.store.enqueue_work(m.MonitoringWorkDraft(
            **h.context(), work_id=h.next_id(), kind="deferred_retry",
            policy_revision=h.version.revision, due_at=h.clock(), created_at=h.clock(),
            target=h.source.execution.target, execution=h.source.execution,
            reason="Original controller deferral before any POST.",
        ))
        h.work = h.claim(("deferred_retry",))[0]
        first_attempt = 1
    action = reserve(h)
    for attempt in range(first_attempt, MAX_ATTEMPTS + 1):
        assert action.retry_attempt == attempt
        rejected = h.store.record_action_rejection(rejection(h, action, retry_after=1))
        finalize(h, rejected)
        if attempt == MAX_ATTEMPTS:
            assert rejected.retry_work_id is None
            break
        child = next_work(h, rejected)
        duplicate = h.store.enqueue_work(m.MonitoringWorkDraft(
            **h.context(), work_id=h.next_id(), kind="deferred_retry",
            policy_revision=h.version.revision, due_at=h.clock(), created_at=h.clock(),
            target=h.source.execution.target, execution=h.source.execution,
            reason="Cannot create an unrelated source opportunity.",
        ))
        assert duplicate.work_id == child.work_id
        action = reserve(h)
    assert h.claim(("deferred_retry",)) == ()
    assert h.store.get_incident_state(action.request.incident).action_count == 1


def test_default_rejection_backoff_uses_the_existing_retry_policy(h) -> None:
    rejected = h.store.record_action_rejection(rejection(h, reserve(h), retry_after=0))
    child = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
    assert child.due_at == h.clock() + timedelta(seconds=backoff_seconds(1))


def test_scope_revocation_dispositions_the_successor_without_a_new_reservation(h) -> None:
    rejected = h.store.record_action_rejection(rejection(h, reserve(h), retry_after=1))
    finalize(h, rejected)
    h.activate(h.scope.model_copy(update={"enabled": False}))
    h.clock.advance(2)
    assert h.claim(("deferred_retry",)) == ()
    child = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
    assert child.state == "dispositioned"
    assert child.action_reservation_id is None
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), rejected.reservation_id).retry_reservation_id is None


def test_rejection_requires_the_original_work_owner_and_fence(h) -> None:
    action = reserve(h)
    request = rejection(h, action)
    wrong = request.model_copy(update={"lease": request.lease.model_copy(update={"owner_id": h.next_id()})})
    with pytest.raises(MonitoringConflict):
        h.store.record_action_rejection(wrong)
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id).state == "reserved"


def test_expired_owner_cannot_release_a_reservation_from_a_late_response(h) -> None:
    action = reserve(h)
    request = rejection(h, action)
    h.clock.advance(121)
    with pytest.raises(MonitoringConflict):
        h.store.record_action_rejection(request)
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id).state == "reserved"


def test_rejection_cannot_be_reported_as_verified_resolution(h) -> None:
    rejected = h.store.record_action_rejection(rejection(h, reserve(h)))
    with pytest.raises(MonitoringConflict, match="not a resolved incident"):
        h.store.finalize_work(h.finalization_input(action=rejected, outcome="resolved"))
    assert h.store.get_work(m.MonitoringContext(**h.context()), h.work.work_id).state == "leased"


def test_gated_configuration_rejection_does_not_reuse_approval_or_retry(h) -> None:
    h.reviewed = h.review("rebind_dataset_gateway", parameters={
        "gateway_id": h.next_id(), "datasource_ids": [h.next_id()],
    })
    request = h.reserve_request(h.reviewed, approval=True)
    action = h.store.reserve_action(request).reservation
    rejected = h.store.record_action_rejection(rejection(h, action))
    assert rejected.state == "rejected"
    assert rejected.retry_work_id is None
    row = h.approval_row(request.approval.approval_id) if isinstance(h, SqlHarness) else h.state.approvals[request.approval.approval_id]
    assert row["consumed_at"]
    assert h.store.get_incident_state(action.request.incident).action_count == 1


def test_lost_sql_rejection_acknowledgement_reconciles_without_another_successor(tmp_path) -> None:
    h = SqlHarness(tmp_path / "lost-rejection.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    h.reviewed = h.review("powerbi_refresh")
    original = reserve(h)
    request = rejection(h, original)
    h.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        h.store.record_action_rejection(request)
    current = h.store.get_action_reservation(m.MonitoringContext(**h.context()), original.reservation_id)
    assert current.state == "rejected"
    replayed = h.store.record_action_rejection(request)
    assert replayed.retry_work_id == current.retry_work_id
    assert h.store.get_incident_state(original.request.incident).action_count == 1


def test_two_sql_instances_cannot_spend_one_linked_successor_twice(tmp_path) -> None:
    h = SqlHarness(tmp_path / "race.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    h.reviewed = h.review("powerbi_refresh")
    rejected = h.store.record_action_rejection(rejection(h, reserve(h), retry_after=1))
    finalize(h, rejected)
    next_work(h, rejected)
    first = h.reserve_request(h.reviewed, approval=False)
    second = first.model_copy(update={"idempotency_id": h.next_id()})
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda pair: pair[0].reserve_action(pair[1]), ((h.store, first), (other, second))))
    assert sorted(result.status for result in results) == ["denied", "reserved"]
    assert h.store.get_incident_state(first.incident).action_count == 1
    parent = h.store.get_action_reservation(m.MonitoringContext(**h.context()), rejected.reservation_id)
    assert parent.retry_reservation_id == next(result.reservation.reservation_id for result in results if result.status == "reserved")


async def test_rejected_post_still_spends_this_invocations_policy_attempt(configured) -> None:
    from triage.tools.registry import ToolDispatcher

    store, _, _, client, ctx = configured()
    client.refresh_result = "Throttled"
    client.retry_after_seconds = 42
    dispatcher = ToolDispatcher(ctx)
    first = await dispatcher.dispatch("refresh_powerbi_dataset", {"justification": "Transient failure"})
    assert first["status"] == "Throttled"
    assert first["request_id"] == ""
    assert first["retry_work_id"]
    assert ctx.ledger.write_actions == 1
    second = await dispatcher.dispatch("refresh_powerbi_dataset", {"justification": "Try immediately again"})
    assert second["status"] == "blocked_by_policy"
    assert len([entry for entry in client.calls if entry[0] == "refresh_dataset"]) == 1
    assert store.get_incident_state(ctx.monitoring.incident).action_count == 1


async def test_controller_throttle_then_linked_retry_succeeds_without_same_invocation_replay(runner, repo_root, monkeypatch) -> None:
    from triage.runner import Scenario
    from triage.tools.powerbi import MockPowerBIClient

    first = await runner.run_scenario(Scenario.load(repo_root / "scenarios" / "scenario8-capacity-backoff.yaml"))
    assert first[0].result.outcome == "deferred_retry"
    throttled = MockPowerBIClient(latency_ms=0, refresh_result="Throttled", retry_after_seconds=42)
    monkeypatch.setattr(runner, "build_powerbi", lambda *_args: throttled)
    future = datetime.now(UTC) + timedelta(hours=2)
    lines = await runner.drain_due_retries(now=future)
    assert "linked retry 2" in lines[0]
    assert len([call for call in throttled.calls if call[0] == "refresh_dataset"]) == 1
    row = runner.retries.pending()[0]
    child = runner.monitoring.get_work(runner.monitoring_context, row["monitoring_work_id"])
    assert child.retry_attempt == 2
    parent = runner.monitoring.get_action_reservation(runner.monitoring_context, child.retry_of)
    assert parent.state == "rejected"
    assert parent.submitted_execution is None and parent.submitted_at is None

    succeeded = MockPowerBIClient(latency_ms=0)
    monkeypatch.setattr(runner, "build_powerbi", lambda *_args: succeeded)
    assert await runner.drain_due_retries(now=future + timedelta(seconds=10)) == []
    assert succeeded.calls == []
    lines = await runner.drain_due_retries(now=future + timedelta(seconds=60))
    assert "completed" in lines[0]
    assert len([call for call in succeeded.calls if call[0] == "refresh_dataset"]) == 1
    assert runner.retries.pending() == []
    assert runner.monitoring.get_incident_state(parent.request.incident).action_count == 1
