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

import pytest
from test_monitoring_publication import drain, poll_page, ready
from test_monitoring_sql_review9_bindings import Review9Database
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
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

    def __init__(self, backend: str):
        self.h = Harness()
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
