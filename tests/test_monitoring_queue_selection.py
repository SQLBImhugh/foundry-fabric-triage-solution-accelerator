"""Queue selection over typed fixture state and the emitted SQL SELECT.

The SQL protocol fixture executes ranking in SQLite; native claims and roles
remain a separate acceptance check.
"""

from __future__ import annotations

import json
from collections import Counter
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from test_monitoring_sql_abi import AbiDatabase
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringUnavailable
from triage.monitoring.memory import InMemoryMonitoringStore, StoredRecord, key_digest
from triage.monitoring.sql_store import AzureSqlMonitoringStore


class QueueDatabase(AbiDatabase):
    def save_work(self, work):
        super().save_work(work)
        key = ("work", work.work_id)
        self.records[key] = replace(
            self.records[key], due_at=work.lease.expires_at if work.lease else work.due_at,
        )


class QueueFixture:
    def __init__(self, sql: bool, *, component: str = "controller") -> None:
        self.h = Harness()
        self.db = QueueDatabase(self.h, principal=component) if sql else None
        self.store = (
            AzureSqlMonitoringStore(db=self.db, component=component) if sql else
            InMemoryMonitoringStore(clock=self.h.clock, state=self.h.state, component=component)
        )

    @property
    def records(self):
        return self.db.records if self.db else self.h.state.records

    @property
    def receipts(self):
        return self.db.receipts if self.db else self.h.state.receipts

    def put(self, row):
        if self.db:
            self.db.records[(row.kind, row.key)] = row
        else:
            self.h.state.records[(*self.h.context().values(), row.kind, key_digest(row.key))] = row

    def record(self, kind, key, value, **indices):
        row = StoredRecord(
            kind=kind, key=key, context=m.MonitoringContext(**self.h.context()),
            version=1, payload=value.model_dump_json() if isinstance(value, m.MonitoringModel) else json.dumps(value),
            **indices,
        )
        self.put(row)
        return row

    def work(
        self, *, producer="worker", state="waiting", workspace=None, kind="reconcile_state",
        due_offset=-300, lease_offset=120,
    ):
        h = self.h
        target = m.TargetIdentity(
            **h.context(), workspace_id=workspace, item_id=h.next_id(), workload="fabric_pipeline",
        ) if workspace else None
        work_id = h.next_id()
        lineage = (
            {"reconcile_producer": producer, "reconcile_request_id": h.next_id()} if kind == "reconcile_state" else
            {"discovery_selector": m.ScopeSelector(tenant_id=uid(1), kind="tenant")} if kind == "inventory" else
            {"execution": m.SourceExecutionIdentity(target=target, run_id=h.next_id(), run_id_kind="fabric_job")}
        )
        if kind == "deferred_retry":
            lineage["retry_attempt"] = 1
        work = m.MonitoringWork(
            **h.context(), work_id=work_id, kind=kind, policy_revision=0,
            revision=1, attempts=1 if state == "waiting" else 0, state=state,
            created_at=h.clock() - timedelta(minutes=10), due_at=h.clock() + timedelta(seconds=due_offset),
            reason="Fixture queue selection.", target=target, **lineage,
            lease=m.LeaseToken(
                **h.context(), resource_key=m.work_key(h.version, work_id), owner_id=uid(99), fence=1,
                acquired_at=h.clock() - timedelta(minutes=5), expires_at=h.clock() + timedelta(seconds=lease_offset),
            ) if state in {"leased", "finalizing"} else None,
        )
        self.record(
            "work", work_id, work, status=state, work_kind=kind, workspace_id=workspace,
            target_key=target.key if target else None,
            due_at=work.lease.expires_at if work.lease else work.due_at,
        )
        return work

    def partial_page(self, *, workspace=None, decision="published"):
        work = self.work(workspace=workspace)
        frontier_key = f"validation:v1:{work.epoch}:{work.tenant_id}:inventory:{work.work_id}"
        handoff = m.ValidationHandoff(
            frontier_key=frontier_key, frontier_revision=1, producer="worker",
            producer_request_id=work.reconcile_request_id, producer_operation="worker.accept_facts",
            producer_fingerprint="1" * 64, producer_binding_hash="2" * 64,
            work_id=work.work_id, policy_revision=0, evidence_digest="3" * 64, requires_window=True,
        )
        self.record(
            "validation_handoff", f"{frontier_key}:handoff:1", handoff,
            status=decision, parent_key=frontier_key, sequence_number=1,
        )
        self.record("validation_window", frontier_key, {
            "frontier_key": frontier_key, "collection_id": self.h.next_id(), "collection_complete": False,
            "closing_request_id": None, "closing_revision": None,
        }, status="collecting")
        self.record("validation_frontier", frontier_key, m.ValidationFrontier(
            **self.h.context(), frontier_key=frontier_key, accepted_revision=1,
            validated_revision=0, latest_request_id=work.reconcile_request_id, updated_at=self.h.clock(),
        ), status="pending_validation", parent_key=frontier_key, sequence_number=1)
        return work

    def discovery(self):
        h = self.h
        if self.db:
            self.db.principal = "web"
            web = AzureSqlMonitoringStore(db=self.db, component="web")
        else:
            web = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="web")
        try:
            return web.request_discovery(
                h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id(),
            )
        finally:
            if self.db:
                self.db.principal = self.store.component

    def claim(self, *, kinds=("reconcile_state",), limit=1, share=1):
        return self.store.claim_work(m.WorkClaimRequest(
            **self.h.context(), owner_id=uid(90), kinds=kinds, limit=limit, per_workspace_limit=share,
        ))

    def due(self, *, kinds=("reconcile_state",), limit=1, share=1):
        request = m.WorkClaimRequest(
            **self.h.context(), owner_id=uid(90), kinds=kinds, limit=limit, per_workspace_limit=share,
        )
        backend = self.store._sql if self.db else self.store._backend
        with backend.transaction(write=False, operation="queue_selection", request_id=uid(91)):
            return backend.due(request, after_workspace="")


@pytest.fixture(params=[False, True], ids=["memory", "emitted_sql"])
def queue(request):
    return QueueFixture(request.param)


def test_fresh_web_intent_is_claimed_before_hundreds_of_waiting_worker_pages(queue):
    backlog = [
        queue.partial_page(workspace=uid(100 + index % 10) if index % 2 else None)
        for index in range(684)
    ]
    web = queue.discovery()
    before, receipts = deepcopy(queue.records), deepcopy(queue.receipts)
    claimed = queue.claim()
    assert [work.work_id for work in claimed] == [web.work_id]
    assert claimed[0].attempts == 1 and claimed[0].lease.owner_id == uid(90)
    assert queue.receipts == receipts
    assert all(queue.records[key] == row for key, row in before.items() if row.key != web.work_id)
    waiting = {
        row.key: m.MonitoringWork.model_validate_json(row.payload)
        for row in queue.records.values() if row.kind == "work" and row.status == "waiting"
    }
    assert set(waiting) == {work.work_id for work in backlog}
    assert all(work.attempts == 1 and work.lease is None for work in waiting.values())
    if queue.db:
        selects = [sql for method, sql, _ in queue.db.calls if method == "query" and "ROW_NUMBER()" in sql]
        assert len(selects) == 1
        assert "SELECT TOP (1)" in selects[0]
        assert "JSON_VALUE(r.payload, '$.reconcile_producer')" in selects[0]
        assert queue.db.names.object("controller_read") in selects[0]


def test_priority_preserves_workspace_shares_and_publication_pool_isolation(queue):
    for workspace in (uid(100), uid(101), uid(102)):
        queue.work(kind="triage", workspace=workspace, state="leased")
        for _ in range(4):
            queue.partial_page(workspace=workspace)
            queue.work(producer="web", workspace=workspace, state="queued", due_offset=0)
    claimed = queue.claim(limit=6, share=2)
    assert len(claimed) == 6 and all(work.reconcile_producer == "web" for work in claimed)
    assert Counter(work.target.workspace_id for work in claimed) == {uid(100): 2, uid(101): 2, uid(102): 2}
    assert not queue.claim(limit=6, share=2)


@pytest.mark.parametrize("state", ["leased", "finalizing"])
def test_live_ordinary_leases_do_not_occupy_the_publication_pool(queue, state):
    queue.work(kind="triage", workspace=uid(100), state=state)
    web = queue.work(producer="web", workspace=uid(100), state="queued", due_offset=0)
    assert [row.key for row in queue.due()] == [web.work_id]


def test_priority_never_exceeds_an_active_publication_share(queue):
    active = queue.work(workspace=uid(100), state="leased")
    queue.work(producer="web", workspace=uid(100), state="queued", due_offset=0)
    other = queue.work(producer="web", workspace=uid(101), state="queued", due_offset=0)
    assert [work.work_id for work in queue.claim()] == [other.work_id]
    assert queue.store.get_work(queue.h.version, active.work_id) == active


@pytest.mark.parametrize("state,due_offset,lease_offset", [("queued", 30, 120), ("leased", -300, 120)])
def test_web_priority_cannot_expedite_future_work_or_steal_a_current_lease(queue, state, due_offset, lease_offset):
    queue.work(producer="web", workspace=uid(100), state=state, due_offset=due_offset, lease_offset=lease_offset)
    old = queue.work(workspace=uid(101))
    assert [row.key for row in queue.due()] == [old.work_id]


@pytest.mark.parametrize("kind,state", [
    ("reconcile_state", "waiting"), ("reconcile_state", "queued"), ("reconcile_state", "leased"),
    ("triage", "finalizing"), ("deferred_retry", "waiting"),
])
def test_ordinary_due_work_and_expired_fences_remain_eligible(queue, kind, state):
    work = queue.work(kind=kind, state=state, workspace=uid(100), lease_offset=-1)
    assert [row.key for row in queue.due(kinds=(kind,))] == [work.work_id]


@pytest.mark.parametrize("window_state", ["collecting", "awaiting_validation", "validated", "rejected"])
def test_window_state_never_silently_filters_waiting_siblings(queue, window_state):
    work = queue.partial_page()
    window = next(row for row in queue.records.values() if row.kind == "validation_window")
    payload = json.loads(window.payload)
    payload["collection_complete"] = window_state != "collecting"
    queue.put(replace(window, status=window_state, payload=json.dumps(payload)))
    assert [row.key for row in queue.due()] == [work.work_id]


@pytest.mark.parametrize("binding", ["missing", "wrong_work", "unresolved"])
def test_absent_wrong_or_unresolved_handoff_remains_due_not_healthy(queue, binding):
    work = queue.work() if binding == "missing" else queue.partial_page()
    if binding != "missing":
        handoff = next(row for row in queue.records.values() if row.kind == "validation_handoff")
        model = m.ValidationHandoff.model_validate_json(handoff.payload)
        queue.put(replace(
            handoff, status="pending_validation" if binding == "unresolved" else handoff.status,
            payload=m.ValidationHandoff.model_validate({
                **model.model_dump(), "work_id": uid(999) if binding == "wrong_work" else model.work_id,
            }).model_dump_json(),
        ))
    assert [row.key for row in queue.due()] == [work.work_id]
    before = deepcopy(queue.records)
    with pytest.raises(MonitoringUnavailable):
        queue.store.reconcile_work(queue.claim()[0])
    assert all(queue.records[key] == row for key, row in before.items() if row.kind != "work")


def test_unreadable_reconciliation_still_fails_instead_of_becoming_a_lower_priority(queue, caplog):
    work = queue.work(producer="web", state="queued")
    row = next(row for row in queue.records.values() if row.kind == "work")
    payload = work.model_dump(mode="json")
    payload.pop("reconcile_producer")
    queue.put(replace(row, payload=json.dumps(payload)))
    with pytest.raises(MonitoringUnavailable, match="unreadable"):
        queue.claim()
    assert "Invalid persisted monitoring record" in caplog.text
    assert all(row.status != "leased" for row in queue.records.values() if row.kind == "work")


@pytest.mark.parametrize("sql", [False, True], ids=["memory", "emitted_sql"])
def test_worker_queue_order_and_workspace_quota_are_unchanged(sql):
    queue = QueueFixture(sql, component="worker")
    first = queue.work(kind="inventory", state="queued", due_offset=-60)
    queue.work(kind="inventory", state="queued", due_offset=-30)
    queue.work(producer="web", state="queued", due_offset=-120)
    assert [work.work_id for work in queue.claim(kinds=("inventory",))] == [first.work_id]
    assert not queue.claim(kinds=("inventory",))
