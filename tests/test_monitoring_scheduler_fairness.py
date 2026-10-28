"""Each automatic pool gets served, and no claim outlives its admission window.

Deterministic reconciliation was given strict priority so that connector
publication could reach SQL while its presence evidence was still valid. Strict
priority is not the same as bounded service: with reconciliation continuously
eligible, an independent review drove the real heartbeat and drain functions and
recorded 30 reconciliation claims and zero action claims across three
invocations. Verification and finalization of already-submitted effects sit in
that starved pool.

The admission window has the same shape of gap. The budget was checked once per
loop iteration, before the reconciliation claim; a slow empty reconciliation
lookup could cross the cutoff and the fallback action claim still leased work.

These are refillable concurrency slots, not a three-job invocation cap.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace

import pytest
from test_monitoring_sql_review9_bindings import Review9Database
from test_monitoring_store import Harness, uid

from triage.models import Incident
from triage.monitoring import models as m
from triage.monitoring.controller import HeartbeatBudget, controller_heartbeat
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.runtime import build_monitoring_store
from triage.monitoring.sql_store import AzureSqlMonitoringStore
from triage.runner import TriageRunner
from triage.settings import Settings
from triage.store.incidents import InMemoryIncidentStore

RECONCILE = ("reconcile_state",)
ACTIONS = ("triage", "deferred_retry", "verify_action", "finalize")


@pytest.fixture
def runner(tmp_path):
    settings = Settings(
        _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
        triage_provider_mode="mock", azure_sql_server="", azure_sql_database="",
        applicationinsights_connection_string="",
    )
    return TriageRunner(
        settings, base_dir=tmp_path, store=InMemoryIncidentStore(),
        monitoring_store=build_monitoring_store(settings, fixture=True),
    )


def _always_eligible(claims, clock=None, step=0.0):
    """Both pools always have work, so preference alone decides who is served."""
    def claim(request):
        claims.append(request)
        if clock is not None:
            clock[0] += step
        return (SimpleNamespace(work_id=f"{request.kinds[0]}-{len(claims)}"),)
    return claim


async def _noop_command_drain(_runner, *, limit, budget):
    return []


async def test_a_continuous_reconciliation_backlog_cannot_starve_actions(runner, monkeypatch) -> None:
    claims = []
    monkeypatch.setattr(runner.monitoring, "claim_work", _always_eligible(claims))

    async def execute(work):
        return f"- {work.work_id}: done"

    monkeypatch.setattr(runner, "execute_monitoring_work", execute)

    await controller_heartbeat(runner, rounds=10, command_drain=_noop_command_drain)

    served = [request.kinds for request in claims]
    assert any(kinds == RECONCILE for kinds in served), "reconciliation must still be served"
    assert any(tuple(kinds) == ACTIONS for kinds in served), (
        f"actions were starved by reconciliation: {served}"
    )


async def test_each_pool_is_served_within_a_bounded_number_of_admissions(runner, monkeypatch) -> None:
    claims = []
    monkeypatch.setattr(runner.monitoring, "claim_work", _always_eligible(claims))

    async def execute(work):
        return f"- {work.work_id}: done"

    monkeypatch.setattr(runner, "execute_monitoring_work", execute)

    await controller_heartbeat(runner, rounds=10, command_drain=_noop_command_drain)

    served = [request.kinds for request in claims]
    first_action = next(i for i, kinds in enumerate(served) if tuple(kinds) == ACTIONS)
    # One pool may lead, but the other must not wait behind an unbounded run of
    # its competitor.
    assert first_action <= 2, f"actions waited {first_action} admissions: {served}"


async def test_the_fallback_claim_respects_the_admission_deadline(runner, monkeypatch) -> None:
    elapsed = [0.0]
    claims = []

    def claim(request):
        claims.append(request)
        # An empty reconciliation lookup that crosses the cutoff.
        elapsed[0] += 200.0
        return ()

    monkeypatch.setattr(runner.monitoring, "claim_work", claim)

    async def forbidden(work):
        pytest.fail("No work may be executed after admission closed")

    monkeypatch.setattr(runner, "execute_monitoring_work", forbidden)

    budget = HeartbeatBudget(840, 690, lambda: elapsed[0])
    assert await runner.drain_monitoring_work(limit=10, budget=budget) == []

    assert len(claims) == 1, f"a second pool was claimed after the cutoff: {claims}"


async def test_an_empty_pool_lets_the_other_pool_continue(runner, monkeypatch) -> None:
    claims = []

    def claim(request):
        claims.append(request)
        if request.kinds == RECONCILE:
            return ()
        return (SimpleNamespace(work_id=f"action-{len(claims)}"),)

    monkeypatch.setattr(runner.monitoring, "claim_work", claim)

    async def execute(work):
        return f"- {work.work_id}: done"

    monkeypatch.setattr(runner, "execute_monitoring_work", execute)

    lines = await runner.drain_monitoring_work(limit=3)

    assert lines, "an empty reconciliation pool must not stop action service"
    assert any(tuple(request.kinds) == ACTIONS for request in claims)


async def test_both_pools_empty_stops_draining(runner, monkeypatch) -> None:
    claims = []

    def claim(request):
        claims.append(request)
        return ()

    monkeypatch.setattr(runner.monitoring, "claim_work", claim)

    assert await runner.drain_monitoring_work(limit=10) == []

    # One probe of each pool, then stop; not ten rounds of both.
    assert [tuple(request.kinds) for request in claims] == [RECONCILE, ACTIONS]


class _LockedReview9Database(Review9Database):
    """Serialize concurrent thread-pool transactions as SQL control-row locks do."""

    def __init__(self, h):
        super().__init__(h)
        self._tx_lock = threading.Lock()

    @contextmanager
    def transaction(self):
        with self._tx_lock, super().transaction():
            yield self


def _build_fairness_estate(tmp_path, monkeypatch, backend: str):
    h = Harness()
    h.seed(count=3, workspaces=3)
    h.activate()
    observations = {}
    for index, target in enumerate(h.targets):
        obs = h.observation(
            target=target,
            execution={"target": target, "run_id": uid(60_001 + index), "run_id_kind": "fabric_job"},
            started_at=h.clock() - timedelta(minutes=10 - index),
            ended_at=h.clock() - timedelta(minutes=5 - index),
        )
        h.store._save_source(obs)
        observations[target.key] = obs
    db = _LockedReview9Database(h) if backend == "sql" else None
    stores = {
        component: AzureSqlMonitoringStore(db=db, component=component) if db else
        InMemoryMonitoringStore(state=h.state, clock=h.clock, component=component)
        for component in ("controller", "worker")
    }

    def use(component: str):
        if db is not None:
            db.principal = component
        return stores[component]

    settings = Settings(
        _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
        triage_provider_mode="mock", azure_sql_server="", azure_sql_database="",
        applicationinsights_connection_string="",
    )
    controller = use("controller")
    runner = TriageRunner(
        settings, base_dir=tmp_path, store=InMemoryIncidentStore(),
        monitoring_store=controller,
    )
    monkeypatch.setattr(
        TriageRunner, "monitoring_context",
        property(lambda self: m.MonitoringContext(**h.context())),
    )
    reconciled: list[tuple[m.MonitoringWork, m.ReconciliationResult]] = []
    finalized_actions: list[str] = []

    async def execute(work: m.MonitoringWork) -> str:
        use("controller")
        if work.kind == "reconcile_state":
            result = controller.reconcile_work(work)
            reconciled.append((work, result))
            return f"- {work.work_id}: deterministic reconciliation {result.state}."
        obs = observations[work.target.key]
        now = h.clock()
        identity = m.IncidentIdentity(target=work.target, signature=f"sig-{work.work_id}")
        incident = Incident(
            id="candidate-id", signature=identity.signature, status="investigating",
            first_seen_at=now.isoformat(), last_seen_at=now.isoformat(),
            outcome="needs_human", summary="Action pool service verification.",
        )
        controller.finalize_work(m.WorkFinalizationRequest(
            **h.context(), finalization_id=h.next_id(), work_id=work.work_id,
            expected_work_revision=work.revision, lease=work.lease,
            source_execution=obs.execution, incident_identity=identity,
            incident=incident, source_disposition="triaged",
        ))
        finalized_actions.append(work.work_id)
        return f"- {work.work_id}: action finalized."

    monkeypatch.setattr(runner, "execute_monitoring_work", execute)
    return h, db, use, runner, observations, reconciled, finalized_actions


def _enqueue_action(h, use, observations, target: m.TargetIdentity) -> m.MonitoringWork:
    obs = observations[target.key]
    return use("controller").enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="triage",
        policy_revision=h.version.revision, created_at=h.clock(), due_at=h.clock(),
        target=target, execution=obs.execution,
        reason="Eligible action work competing with partial-window reconciliation.",
    ))


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_partial_window_pages_across_heartbeats_do_not_starve_actions_or_replay_once_completed(
    tmp_path, monkeypatch, backend: str,
) -> None:
    h, _, use, runner, observations, reconciled, finalized_actions = _build_fairness_estate(
        tmp_path, monkeypatch, backend,
    )
    controller = use("controller")
    inv_selector = m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=uid(200))
    inv_draft = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=h.version.revision,
        discovery_selector=inv_selector,
        created_at=h.clock(), due_at=h.clock(), reason="Paginated inventory across heartbeats.",
    ))
    worker = use("worker")
    collection = next(work for work in worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(970), kinds=("inventory",), limit=5, per_workspace_limit=5,
    )) if work.work_id == inv_draft.work_id)

    page1_req_id = h.next_id()
    gen_p1 = worker.record_inventory(m.InventoryBatch(
        request_id=page1_req_id, expected=h.version,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=inv_draft.work_id, selector=inv_selector,
            adapter="fixture", authority="tenant_admin", completeness="partial",
            started_at=h.clock(), continuation="page-2",
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="Page 1."),),
        ),
        items=(m.InventoryItem(
            **h.context(), generation_id=inv_draft.work_id, workspace_id=uid(200), item_id=uid(1101),
            name="Fairness item 1", item_type="DataPipeline", workload="fabric_pipeline",
            observed_at=h.clock(),
        ),),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision, expected_generation_revision=0,
        ),
    ))
    action1 = _enqueue_action(h, use, observations, h.targets[0])

    # Heartbeat 1 serves both page 1 reconciliation and action1, then puts page 1 into waiting.
    await controller_heartbeat(runner, rounds=2, command_drain=_noop_command_drain)
    assert action1.work_id in finalized_actions
    assert len(reconciled) == 1
    page1_work_claim1, page1_initial = reconciled[0]
    assert page1_work_claim1.reconcile_request_id == page1_req_id
    assert (page1_initial.state, page1_initial.resolution_scope) == ("pending_validation", "handoff")
    assert use("controller").get_work(h.version, page1_work_claim1.work_id).state == "waiting"

    # An immediate subsequent heartbeat before the 15s wait timer expires does not re-claim page 1.
    h.clock.advance(5)
    assert await controller_heartbeat(runner, rounds=4, command_drain=_noop_command_drain) == []
    assert len(reconciled) == 1

    # Page 2 arrives (still partial) and action2 is queued; advance past 15s so page 1 is also due.
    worker = use("worker")
    page2_req_id = h.next_id()
    gen_p2 = worker.record_inventory(m.InventoryBatch(
        request_id=page2_req_id, expected=h.version,
        generation=m.InventoryGeneration.model_validate({
            **gen_p1.model_dump(), "completed_pages": 1, "continuation": "page-3",
        }),
        items=(m.InventoryItem(
            **h.context(), generation_id=inv_draft.work_id, workspace_id=uid(200), item_id=uid(1102),
            name="Fairness item 2", item_type="DataPipeline", workload="fabric_pipeline",
            observed_at=h.clock(),
        ),),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision,
            expected_generation_revision=gen_p1.revision,
            expected_continuation=gen_p1.continuation,
        ),
    ))
    action2 = _enqueue_action(h, use, observations, h.targets[1])
    h.clock.advance(11)

    # With 2 reconciliation items and 1 action item all due under rounds=2, action2 is still served.
    use("controller")
    await controller_heartbeat(runner, rounds=2, command_drain=_noop_command_drain)
    assert action2.work_id in finalized_actions
    await runner.drain_monitoring_work(limit=5)
    by_req_round2 = {work.reconcile_request_id: (work, res) for work, res in reconciled[1:]}
    assert set(by_req_round2) == {page1_req_id, page2_req_id}
    page1_work_claim2, page1_ack = by_req_round2[page1_req_id]
    assert (page1_ack.state, page1_ack.resolution_scope) == ("pending_validation", "pending_window_acknowledgement")
    assert page1_ack.handoff_resolution_request_id == page1_initial.request_id
    assert page1_ack.handoff_resolution_work_fence == page1_work_claim1.lease.fence
    page2_work_claim1, page2_initial = by_req_round2[page2_req_id]
    assert (page2_initial.state, page2_initial.resolution_scope) == ("pending_validation", "handoff")

    # Closing page 3 arrives alongside action3; advance 16s so pages 1, 2, and 3 are all due.
    worker = use("worker")
    page3_req_id = h.next_id()
    worker.record_inventory(m.InventoryBatch(
        request_id=page3_req_id, expected=h.version,
        generation=m.InventoryGeneration.model_validate({
            **gen_p2.model_dump(), "completed_pages": 2, "continuation": None,
            "completeness": "complete", "completed_at": h.clock(), "discovered_count": 3, "gaps": (),
        }),
        items=(m.InventoryItem(
            **h.context(), generation_id=inv_draft.work_id, workspace_id=uid(200), item_id=uid(1103),
            name="Fairness item 3", item_type="DataPipeline", workload="fabric_pipeline",
            observed_at=h.clock(),
        ),),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision,
            expected_generation_revision=gen_p2.revision,
            expected_continuation=gen_p2.continuation,
        ),
    ))
    action3 = _enqueue_action(h, use, observations, h.targets[2])
    h.clock.advance(16)

    use("controller")
    await controller_heartbeat(runner, rounds=6, command_drain=_noop_command_drain)
    assert action3.work_id in finalized_actions
    page3_work_claim1, page3_published = next(
        (work, res) for work, res in reconciled if work.reconcile_request_id == page3_req_id
    )
    assert (page3_published.state, page3_published.resolution_scope) == ("published", "handoff")
    frontier = use("controller").get_validation_frontier(h.version, page3_published.frontier_key)
    assert not frontier.pending and frontier.validated_revision == frontier.accepted_revision == 3

    # Wake pages 1 and 2 one final time so they complete via window_acknowledgement.
    h.clock.advance(16)
    await controller_heartbeat(runner, rounds=6, command_drain=_noop_command_drain)
    controller = use("controller")
    for work_id in (
        page1_work_claim1.work_id, page2_work_claim1.work_id, page3_work_claim1.work_id,
        action1.work_id, action2.work_id, action3.work_id,
    ):
        assert controller.get_work(h.version, work_id).state == "completed"

    # Original receipts are preserved, and later heartbeats observe zero replay churn.
    assert controller.reconcile_work(page1_work_claim1) == page1_initial
    assert controller.reconcile_work(page1_work_claim2) == page1_ack
    assert controller.reconcile_work(page2_work_claim1) == page2_initial
    assert controller.reconcile_work(page3_work_claim1) == page3_published
    recorded_count = len(reconciled)
    for step in (16, 60, 120):
        h.clock.advance(step)
        assert await controller_heartbeat(runner, rounds=6, command_drain=_noop_command_drain) == []
        assert len(reconciled) == recorded_count


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_rejected_partial_window_siblings_complete_across_heartbeats_alongside_actions(
    tmp_path, monkeypatch, backend: str,
) -> None:
    h, _, use, runner, observations, reconciled, finalized_actions = _build_fairness_estate(
        tmp_path, monkeypatch, backend,
    )
    target = h.targets[0]
    worker = use("worker")
    poll = next(work for work in worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(980), kinds=("poll",), limit=5, per_workspace_limit=5,
    )) if work.target == target)
    window = m.ObservationWindow(start_at=h.clock() - timedelta(minutes=30), end_at=h.clock())
    obs1 = h.observation(target=target, execution={"target": target, "run_id": uid(61_001), "run_id_kind": "fabric_job"})
    req1 = m.RestPageRequest(
        page_id=h.next_id(), target=target, policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=0,
        expected_cursor=None, next_cursor="cursor-2", window=window,
        received_count=1, observed_at=h.clock(), window_complete=False, observations=(obs1,),
    )
    worker.record_rest_page(req1)
    action1 = _enqueue_action(h, use, observations, h.targets[1])

    use("controller")
    await controller_heartbeat(runner, rounds=2, command_drain=_noop_command_drain)
    assert action1.work_id in finalized_actions
    page1_work, page1_initial = reconciled[0]
    assert (page1_initial.state, page1_initial.resolution_scope) == ("pending_validation", "handoff")

    worker = use("worker")
    obs2 = h.observation(target=target, execution={"target": target, "run_id": uid(61_002), "run_id_kind": "fabric_job"})
    req2 = m.RestPageRequest(
        page_id=h.next_id(), target=target, policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=1,
        expected_cursor="cursor-2", next_cursor="cursor-3", window=window,
        received_count=1, observed_at=h.clock(), window_complete=False, observations=(obs2,),
    )
    worker.record_rest_page(req2)

    # Reject the unfinished window on page 2 while page 1 is due and action2 is queued.
    h.clock.advance(16)
    controller = use("controller")
    page2_work = next(
        work for work in controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(981), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
        )) if work.reconcile_request_id == req2.page_id
    )
    rejected_window = controller.reconcile_state(m.ReconcileStateRequest(
        **h.context(), request_id=h.next_id(), work_id=page2_work.work_id, lease=page2_work.lease,
        expected_work_revision=page2_work.revision, expected_policy_revision=h.version.revision,
        expected_frontier_revision=2, reject_whole_window=True,
        detail="Reject incomplete poll window and let sibling page complete on next heartbeat.",
    ))
    assert (rejected_window.state, rejected_window.resolution_scope) == ("rejected", "window")

    # Advance past the lease taken above on page 1 so the heartbeat claims page 1 and action2.
    h.clock.advance(121)
    action2 = _enqueue_action(h, use, observations, h.targets[2])
    await controller_heartbeat(runner, rounds=4, command_drain=_noop_command_drain)
    assert action2.work_id in finalized_actions
    page1_reclaimed, page1_terminal = reconciled[-1]
    assert (page1_terminal.state, page1_terminal.resolution_scope) == ("rejected", "window_acknowledgement")
    assert page1_terminal.window_rejection_request_id == rejected_window.request_id
    assert controller.get_work(h.version, page1_reclaimed.work_id).state == "completed"
    assert controller.get_work(h.version, page2_work.work_id).state == "completed"
    assert controller.reconcile_work(page1_work) == page1_initial

    h.clock.advance(60)
    assert await controller_heartbeat(runner, rounds=4, command_drain=_noop_command_drain) == []

