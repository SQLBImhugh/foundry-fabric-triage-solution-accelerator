"""Recovery of REST detection after validated coverage falls behind.

Three defects stopped live detection together:

1. The SQL worker could not read the controller-validated ``rest_checkpoint``,
   so every poll window started 24 hours before its due time instead of at
   validated coverage. After any pause longer than that, each new window
   skipped coverage.
2. The controller raised ``Validated REST coverage cannot skip an unobserved
   window`` for such a window inside its transaction. The handoff stayed
   leased and was claimed again, failing controller heartbeats.
3. A window whose closing page went stale before publication was rejected
   page by page but never closed. The SQL kernel closes a window only through
   its published closing page or an explicit whole-window rejection, so each
   such window was acknowledged as pending every 15 seconds indefinitely. 102
   of them accumulated in one workspace.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from test_monitoring_publication import drain, poll_page, ready
from test_monitoring_sql_review9_bindings import Review9Database
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringCommitUncertain
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.monitoring.sql_store import AzureSqlMonitoringStore, SqlMonitoringAdapter


def _views() -> dict[str, str]:
    return {value.logical_name: value.ddl for value in build_permission_kernel().objects}


def test_worker_can_read_the_validated_watermark_its_polls_start_from():
    views = _views()
    # The poller starts at validated coverage; its coverage snapshot then reads
    # each Power BI checkpoint's staged window and fails closed if it is absent.
    assert "'rest_checkpoint'" in views["worker_read"]
    assert "'powerbi_window'" in views["worker_read"]
    # Reading controller-validated state is not authority to write it.
    for route in ("worker_catalogue", "worker_evidence", "worker_telemetry"):
        assert "'rest_checkpoint'" not in views[route]
        assert "'powerbi_window'" not in views[route]

def test_a_window_that_skips_validated_coverage_is_rejected_without_admitting_its_rows():
    h, stores = ready()
    poll_page(h, stores, complete=True)
    first, = drain(stores, h)
    assert first.state == "published"
    covered = stores["controller"].get_rest_checkpoint(h.targets[0]).coverage_through
    # poll_page windows span 30 minutes, so 31 minutes later the window starts after coverage.
    h.clock.advance(31 * 60)
    late = h.observation(execution={"target": h.targets[0], "run_id": uid(40_002), "run_id_kind": "fabric_job"})
    poll_page(h, stores, complete=True, row=late)

    result, = drain(stores, h)

    assert result.state == "rejected"
    assert not stores["controller"].get_validation_frontier(h.version, result.frontier_key).pending
    assert stores["controller"].get_rest_checkpoint(h.targets[0]).coverage_through == covered
    assert stores["controller"].get_source(late.execution) is None
    # Polling continues at the target's cadence, so the next window can start at validated coverage.
    target = stores["controller"].resolve_target(h.targets[0], include_inactive=True)
    h.clock.advance(target.observation.cadence.poll_seconds)
    polls = stores["worker"].claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(930), kinds=("poll",), limit=5, per_workspace_limit=5,
    ))
    assert [work.target for work in polls] == [h.targets[0]]


class Estate:
    """Controller and worker over one memory state or one SQL protocol double."""

    def __init__(self, backend: str, *, seed: bool = False):
        self.h = Harness()
        if seed:
            self.h.seed(count=2, workspaces=2)
            self.h.activate()
        self.db = Review9Database(self.h) if backend == "sql" else None
        self.stores = {
            component: AzureSqlMonitoringStore(db=self.db, component=component) if self.db else
            InMemoryMonitoringStore(state=self.h.state, clock=self.h.clock, component=component)
            for component in ("controller", "worker")
        }

    def use(self, component: str):
        if self.db:
            self.db.principal = component
        return self.stores[component]

    def record_generation(self) -> str:
        """Collect one complete single-page generation containing the same item."""
        draft = self.use("controller").enqueue_work(m.MonitoringWorkDraft(
            **self.h.context(), work_id=self.h.next_id(), kind="inventory", policy_revision=0,
            discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            created_at=self.h.clock(), due_at=self.h.clock(), reason="Periodic inventory fixture.",
        ))
        worker = self.use("worker")
        collection = next(work for work in worker.claim_work(m.WorkClaimRequest(
            **self.h.context(), owner_id=uid(940), kinds=("inventory",), limit=5, per_workspace_limit=5,
        )) if work.work_id == draft.work_id)
        worker.record_inventory(m.InventoryBatch(
            request_id=self.h.next_id(), expected=self.h.version,
            generation=m.InventoryGeneration(
                **self.h.context(), generation_id=draft.work_id, selector=draft.discovery_selector,
                adapter="fixture", authority="tenant_admin", completeness="complete",
                started_at=self.h.clock(), completed_at=self.h.clock(), discovered_count=1, completed_pages=1,
            ),
            items=(m.InventoryItem(
                **self.h.context(), generation_id=draft.work_id, workspace_id=uid(100), item_id=uid(1000),
                name="Periodically re-stamped pipeline", item_type="DataPipeline", workload="fabric_pipeline",
                observed_at=self.h.clock(),
            ),),
            commit=m.InventoryCommit(
                work_id=collection.work_id, lease=collection.lease,
                expected_work_revision=collection.revision, expected_generation_revision=0,
            ),
        ))
        return draft.work_id

    def claim_handoffs(self, owner: int) -> dict[str, m.MonitoringWork]:
        controller = self.use("controller")
        claimed = controller.claim_work(m.WorkClaimRequest(
            **self.h.context(), owner_id=uid(owner), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
        ))
        return {
            controller.get_reconciliation_request(
                self.h.version, work.reconcile_request_id, producer=work.reconcile_producer,
            ).reference_id: work
            for work in claimed
        }


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_a_window_whose_closing_page_went_stale_still_reaches_a_terminal_decision(backend):
    estate = Estate(backend)
    first = estate.record_generation()
    second = estate.record_generation()  # re-stamps the item before the first window is published
    handoffs = estate.claim_handoffs(941)
    controller = estate.use("controller")

    result = controller.reconcile_work(handoffs[first])

    # The memory adapter binds only the generation record, so the first window
    # is still current there. SQL also binds the re-stamped item row, so its
    # closing page can never publish and the whole window must be rejected.
    assert result.state == ("rejected" if backend == "sql" else "published")
    assert not controller.get_validation_frontier(estate.h.version, result.frontier_key).pending
    assert controller.get_work(estate.h.version, handoffs[first].work_id).state == "completed"
    assert controller.reconcile_work(handoffs[second]).state == "published"


def test_a_window_left_open_by_the_earlier_release_closes_on_its_next_attempt(monkeypatch):
    estate = Estate("sql")
    first = estate.record_generation()
    estate.record_generation()
    controller = estate.use("controller")
    with monkeypatch.context() as earlier_release:
        # The earlier release rejected a stale closing page without closing its window.
        earlier_release.setattr(
            SqlMonitoringAdapter, "_closes_collected_window", staticmethod(lambda window, revision: False),
        )
        stuck = controller.reconcile_work(estate.claim_handoffs(942)[first])
    assert stuck.state == "pending_validation"
    assert controller.get_work(estate.h.version, stuck.work_id).state == "waiting"
    estate.h.clock.advance(16)

    result = controller.reconcile_work(estate.claim_handoffs(943)[first])

    assert (result.state, result.resolution_scope) == ("rejected", "window")
    assert not controller.get_validation_frontier(estate.h.version, result.frontier_key).pending
    assert controller.get_work(estate.h.version, stuck.work_id).state == "completed"


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_partial_inventory_pages_acknowledge_without_replay_churn_and_close_cleanly(backend):
    estate = Estate(backend)
    h = estate.h
    controller = estate.use("controller")
    draft = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=0,
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        created_at=h.clock(), due_at=h.clock(), reason="Multi-page inventory window.",
    ))
    worker = estate.use("worker")
    collection, = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(950), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))
    page1_req_id = h.next_id()
    gen_p1 = worker.record_inventory(m.InventoryBatch(
        request_id=page1_req_id, expected=h.version,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=draft.work_id, selector=draft.discovery_selector,
            adapter="fixture", authority="tenant_admin", completeness="partial",
            started_at=h.clock(), continuation="page-2",
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="Page 1 of 3."),),
        ),
        items=(m.InventoryItem(
            **h.context(), generation_id=draft.work_id, workspace_id=uid(100), item_id=uid(1001),
            name="Pipeline page 1", item_type="DataPipeline", workload="fabric_pipeline",
            observed_at=h.clock(),
        ),),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision, expected_generation_revision=0,
        ),
    ))

    # Reconcile page 1 while the window is still collecting.
    controller = estate.use("controller")
    page1_first, = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(951), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    ))
    page1_initial = controller.reconcile_work(page1_first)
    assert (page1_initial.state, page1_initial.resolution_scope) == ("pending_validation", "handoff")
    assert controller.get_work(h.version, page1_first.work_id).state == "waiting"

    # Subsequent claims before the 15s wait timer expires do not re-claim page 1.
    h.clock.advance(5)
    assert controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(952), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    )) == ()

    # Page 2 arrives (still partial), advancing generation evidence.
    worker = estate.use("worker")
    page2_req_id = h.next_id()
    gen_p2 = worker.record_inventory(m.InventoryBatch(
        request_id=page2_req_id, expected=h.version,
        generation=m.InventoryGeneration.model_validate({
            **gen_p1.model_dump(), "completed_pages": 1, "continuation": "page-3",
        }),
        items=(m.InventoryItem(
            **h.context(), generation_id=draft.work_id, workspace_id=uid(100), item_id=uid(1002),
            name="Pipeline page 2", item_type="DataPipeline", workload="fabric_pipeline",
            observed_at=h.clock(),
        ),),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision,
            expected_generation_revision=gen_p1.revision,
            expected_continuation=gen_p1.continuation,
        ),
    ))

    # Advance past the 15s wait so page 1 is reclaimed alongside page 2.
    h.clock.advance(11)
    controller = estate.use("controller")
    due_batch = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(953), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    ))
    by_req = {work.reconcile_request_id: work for work in due_batch}
    assert set(by_req) == {page1_req_id, page2_req_id}

    if estate.db is not None:
        estate.db.fail_commit = "after"
        with pytest.raises(MonitoringCommitUncertain):
            controller.reconcile_work(by_req[page1_req_id])

    page1_ack = controller.reconcile_work(by_req[page1_req_id])
    assert page1_ack.state == "pending_validation"
    assert page1_ack.resolution_scope == "pending_window_acknowledgement"
    assert page1_ack.handoff_resolution_request_id == page1_initial.request_id
    assert page1_ack.handoff_resolution_work_fence == page1_first.lease.fence

    page2_initial = controller.reconcile_work(by_req[page2_req_id])
    assert (page2_initial.state, page2_initial.resolution_scope) == ("pending_validation", "handoff")
    assert controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(954), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    )) == ()

    # Closing page 3 arrives and completes the inventory window.
    worker = estate.use("worker")
    page3_req_id = h.next_id()
    worker.record_inventory(m.InventoryBatch(
        request_id=page3_req_id, expected=h.version,
        generation=m.InventoryGeneration.model_validate({
            **gen_p2.model_dump(), "completed_pages": 2, "continuation": None,
            "completeness": "complete", "completed_at": h.clock(), "discovered_count": 3, "gaps": (),
        }),
        items=(m.InventoryItem(
            **h.context(), generation_id=draft.work_id, workspace_id=uid(100), item_id=uid(1003),
            name="Pipeline page 3", item_type="DataPipeline", workload="fabric_pipeline",
            observed_at=h.clock(),
        ),),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision,
            expected_generation_revision=gen_p2.revision,
            expected_continuation=gen_p2.continuation,
        ),
    ))

    # Closing page 3 is immediately due while pages 1 and 2 remain in their 15s wait.
    controller = estate.use("controller")
    page3_first, = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(955), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    ))
    assert page3_first.reconcile_request_id == page3_req_id
    page3_published = controller.reconcile_work(page3_first)
    assert (page3_published.state, page3_published.resolution_scope) == ("published", "handoff")
    frontier = controller.get_validation_frontier(h.version, page3_published.frontier_key)
    assert not frontier.pending and frontier.validated_revision == frontier.accepted_revision == 3
    assert controller.get_work(h.version, page3_first.work_id).state == "completed"

    # Once pages 1 and 2 wake up, they acknowledge the closed window and complete.
    h.clock.advance(16)
    siblings = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(956), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    ))
    assert {work.reconcile_request_id for work in siblings} == {page1_req_id, page2_req_id}
    for sibling in siblings:
        ack = controller.reconcile_work(sibling)
        assert (ack.state, ack.resolution_scope) == ("published", "window_acknowledgement")
        assert ack.window_resolution_request_id == page3_published.request_id
        assert ack.window_resolution_state == "published"
        assert controller.get_work(h.version, sibling.work_id).state == "completed"

    # Original decisions and receipts remain intact; no completed page work ever replays.
    assert controller.reconcile_work(page1_first) == page1_initial
    assert controller.reconcile_work(by_req[page1_req_id]) == page1_ack
    assert controller.reconcile_work(by_req[page2_req_id]) == page2_initial
    assert controller.reconcile_work(page3_first) == page3_published
    for step in (16, 60, 120):
        h.clock.advance(step)
        assert controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(957), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
        )) == ()


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_partial_rest_poll_pages_preserve_decisions_across_reclaims_and_window_rejection(backend):
    estate = Estate(backend, seed=True)
    h = estate.h
    target = h.targets[0]
    worker = estate.use("worker")
    poll = next(work for work in worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(960), kinds=("poll",), limit=5, per_workspace_limit=5,
    )) if work.target == target)
    window = m.ObservationWindow(start_at=h.clock() - timedelta(minutes=30), end_at=h.clock())

    obs1 = h.observation(target=target, execution={"target": target, "run_id": uid(45_001), "run_id_kind": "fabric_job"})
    req1 = m.RestPageRequest(
        page_id=h.next_id(), target=target, policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=0,
        expected_cursor=None, next_cursor="cursor-2", window=window,
        received_count=1, observed_at=h.clock(), window_complete=False, observations=(obs1,),
    )
    worker.record_rest_page(req1)

    controller = estate.use("controller")
    page1_first, = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(961), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    ))
    page1_initial = controller.reconcile_work(page1_first)
    assert (page1_initial.state, page1_initial.resolution_scope) == ("pending_validation", "handoff")
    assert controller.get_work(h.version, page1_first.work_id).state == "waiting"
    assert controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(962), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    )) == ()

    worker = estate.use("worker")
    obs2 = h.observation(target=target, execution={"target": target, "run_id": uid(45_002), "run_id_kind": "fabric_job"})
    req2 = m.RestPageRequest(
        page_id=h.next_id(), target=target, policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=1,
        expected_cursor="cursor-2", next_cursor="cursor-3", window=window,
        received_count=1, observed_at=h.clock(), window_complete=False, observations=(obs2,),
    )
    worker.record_rest_page(req2)

    h.clock.advance(16)
    controller = estate.use("controller")
    due = {
        work.reconcile_request_id: work
        for work in controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(963), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
        ))
    }
    assert set(due) == {req1.page_id, req2.page_id}
    page1_ack = controller.reconcile_work(due[req1.page_id])
    assert (page1_ack.state, page1_ack.resolution_scope) == ("pending_validation", "pending_window_acknowledgement")
    assert page1_ack.handoff_resolution_request_id == page1_initial.request_id
    assert page1_ack.handoff_resolution_work_fence == page1_first.lease.fence

    page2_initial = controller.reconcile_work(due[req2.page_id])
    assert (page2_initial.state, page2_initial.resolution_scope) == ("pending_validation", "handoff")

    # Reject the unfinished window on the next cycle and verify sibling completion.
    h.clock.advance(16)
    reclaimed = {
        work.reconcile_request_id: work
        for work in controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(964), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
        ))
    }
    page2_reject = controller.reconcile_state(m.ReconcileStateRequest(
        **h.context(), request_id=h.next_id(), work_id=reclaimed[req2.page_id].work_id,
        lease=reclaimed[req2.page_id].lease, expected_work_revision=reclaimed[req2.page_id].revision,
        expected_policy_revision=h.version.revision, expected_frontier_revision=2,
        reject_whole_window=True, detail="Abandon unfinished poll window without changing earlier page decisions.",
    ))
    assert (page2_reject.state, page2_reject.resolution_scope) == ("rejected", "window")
    assert not controller.get_validation_frontier(h.version, page2_reject.frontier_key).pending
    assert controller.get_work(h.version, reclaimed[req2.page_id].work_id).state == "completed"

    page1_terminal = controller.reconcile_work(reclaimed[req1.page_id])
    assert (page1_terminal.state, page1_terminal.resolution_scope) == ("rejected", "window_acknowledgement")
    assert page1_terminal.window_rejection_request_id == page2_reject.request_id
    assert page1_terminal.window_resolution_request_id == page2_reject.request_id
    assert page1_terminal.window_resolution_state == "rejected"
    assert controller.get_work(h.version, reclaimed[req1.page_id].work_id).state == "completed"

    assert controller.reconcile_work(page1_first) == page1_initial
    assert controller.reconcile_work(due[req1.page_id]) == page1_ack
    assert controller.reconcile_work(due[req2.page_id]) == page2_initial
    assert controller.reconcile_work(reclaimed[req1.page_id]) == page1_terminal
    assert controller.get_source(obs1.execution) is None
    assert controller.get_source(obs2.execution) is None
    h.clock.advance(60)
    assert controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(965), kinds=("reconcile_state",), limit=10, per_workspace_limit=10,
    )) == ()

