"""Scheduling parity for native handoffs without promoted workspace columns.

The emitted SELECTs execute in SQLite; claims use the SQL ABI double. Native
permission, query-cost and live processing acceptance remain separate checks.
"""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from test_monitoring_queue_selection import QueueFixture
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringUnavailable
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.sql_store import AzureSqlMonitoringStore


def make_queue(sql, *, component="controller"):
    queue = QueueFixture(sql, component=component)
    control = queue.h.control.model_copy(update={"revision": 1})
    queue.h.control = control
    queue.h.version = m.RegistryVersion(**queue.h.context(), revision=1)
    queue.h.state.control_row = control.model_dump(mode="json")
    if queue.db:
        queue.db.control = control
    return queue


@pytest.fixture(params=[False, True], ids=["memory-native-shape", "sql-native-shape"])
def queue(request):
    return make_queue(request.param)


def row_for(queue, work):
    return next(row for row in queue.records.values() if row.kind == "work" and row.key == work.work_id)


def native_work(queue, *, workspace=None, producer="worker", state="queued", due_offset=0, lease_offset=120):
    work = queue.work(
        workspace=workspace, producer=producer, state=state,
        due_offset=due_offset, lease_offset=lease_offset,
    ).model_copy(update={"policy_revision": 1})
    queue.put(replace(row_for(queue, work), workspace_id=None, payload=work.model_dump_json()))
    return work


def due(queue, *, after="", limit=1, share=1, kinds=("reconcile_state",)):
    request = m.WorkClaimRequest(
        **queue.h.context(), owner_id=uid(90), kinds=kinds, limit=limit, per_workspace_limit=share,
    )
    backend = queue.store._sql if queue.db else queue.store._backend
    with backend.transaction(write=False, operation="queue_selection", request_id=uid(91)):
        return backend.due(request, after_workspace=after)


def protected_completion(queue, work, *, waiting=False):
    # Advance only the protected progress timestamp. No future work or lease
    # becomes due during these microseconds.
    queue.h.clock.value += timedelta(microseconds=1)
    saved = m.MonitoringWork.model_validate({
        **work.model_dump(), "state": "waiting" if waiting else "completed", "lease": None,
        "revision": work.revision + 1,
        "completed_at": None if waiting else queue.h.clock(),
    })
    row = row_for(queue, work)
    queue.put(replace(row, status=saved.state, payload=saved.model_dump_json(), version=saved.revision))
    if queue.db:
        queue.db.receipts[("controller.transition_work", queue.h.next_id())] = {
            "fingerprint": "1" * 64, "recorded_at": queue.h.clock(),
            "payload": {
                "binding_hash": "2" * 64,
                "result": {"work_id": saved.work_id, "work": saved.model_dump(mode="json")},
            },
        }
    return saved


def test_native_null_promotion_cannot_hide_capability_under_1000_targetless_handoffs(queue):
    for _ in range(1_000):
        queue.partial_page(decision="rejected")
    capability = native_work(queue, workspace=uid(500))
    assert row_for(queue, capability).workspace_id is None
    before, receipts = deepcopy(queue.records), deepcopy(queue.receipts)
    claimed = queue.claim()
    assert [work.work_id for work in claimed] == [capability.work_id]
    assert claimed[0].attempts == 1 and claimed[0].target.workspace_id == uid(500)
    assert queue.receipts == receipts
    assert all(queue.records[key] == row for key, row in before.items() if row.key != capability.work_id)
    if queue.db:
        assert row_for(queue, capability).workspace_id is None
        assert not any(method == "execute" for method, _, _ in queue.db.calls)


@pytest.mark.parametrize("after,expected", [
    ("", 100), (uid(100), 200), (uid(200), 300), (uid(300), None),
])
def test_round_robin_cursor_precedes_due_time_between_workspace_buckets(queue, after, expected):
    workspaces = {}
    for index, workspace in enumerate((None, uid(100), uid(200), uid(300))):
        work = native_work(queue, workspace=workspace, due_offset=-400 + index * 100)
        workspaces[workspace] = work.work_id
    result = due(queue, after=after)
    assert [row.key for row in result] == [workspaces[uid(expected) if expected else None]]
    assert all(row.workspace_id is None for row in result)


def test_rotation_is_not_replaced_by_global_due_order_when_indexes_are_present(queue):
    queue.work(workspace=uid(100), state="queued", due_offset=-400)
    following = queue.work(workspace=uid(200), state="queued", due_offset=0)
    assert [row.key for row in due(queue, after=uid(100))] == [following.work_id]


def test_null_promoted_active_lease_counts_against_its_payload_workspace_only(queue):
    native_work(queue, workspace=uid(100), state="leased")
    waiting = native_work(queue, workspace=uid(100))
    global_work = native_work(queue)
    result = due(queue)
    assert [row.key for row in result] == [global_work.work_id]
    assert waiting.work_id not in {row.key for row in result}


def test_payload_workspace_keeps_publication_and_ordinary_active_pools_separate(queue):
    queue.work(kind="triage", workspace=uid(100), state="finalizing")
    capability = native_work(queue, workspace=uid(100))
    assert [row.key for row in due(queue)] == [capability.work_id]


def test_new_workspace_does_not_weaken_web_intent_priority(queue):
    for _ in range(1_000):
        queue.partial_page()
    native_work(queue, workspace=uid(500))
    web = queue.discovery()
    assert [work.work_id for work in queue.claim()] == [web.work_id]


def test_workspace_share_applies_to_multiple_null_promoted_targets(queue):
    expected = set()
    for workspace in (uid(100), uid(200), uid(300)):
        for _ in range(4):
            expected.add(native_work(queue, workspace=workspace).work_id)
    result = due(queue, limit=6, share=2)
    assert len(result) == 6 and {row.key for row in result} <= expected
    selected = [m.MonitoringWork.model_validate_json(row.payload) for row in result]
    assert all(sum(work.target.workspace_id == workspace for work in selected) == 2
               for workspace in (uid(100), uid(200), uid(300)))


def test_cursor_survives_new_store_instances_without_scheduler_dml(queue):
    for workspace in (uid(100), uid(200), uid(300)):
        for _ in range(5):
            native_work(queue, workspace=workspace, due_offset=-200)
    for _ in range(30):
        native_work(queue, due_offset=-400)
    seen = []
    for _ in range(4):
        queue.store = (
            AzureSqlMonitoringStore(db=queue.db, component="controller") if queue.db else
            InMemoryMonitoringStore(clock=queue.h.clock, state=queue.h.state, component="controller")
        )
        work = queue.claim()[0]
        seen.append(work.target.workspace_id if work.target else None)
        protected_completion(queue, work)
    assert seen == [uid(100), uid(200), uid(300), None]
    if queue.db:
        assert not any(row.kind == "scheduler" for row in queue.records.values())
        assert not any(method == "execute" for method, _, _ in queue.db.calls)


def test_both_capabilities_are_served_in_three_single_slot_claims_with_1000_old_handoffs(queue):
    for _ in range(1_000):
        queue.partial_page(decision="rejected")
    capabilities = {native_work(queue, workspace=uid(500)).work_id for _ in range(2)}
    claimed_ids = []
    for _ in range(3):
        work = queue.claim()[0]
        claimed_ids.append(work.work_id)
        protected_completion(queue, work)
    assert {claimed_ids[0], claimed_ids[2]} == capabilities
    assert claimed_ids[1] not in capabilities


def test_both_controller_slots_keep_one_workspace_share_while_first_capability_is_leased(queue):
    for _ in range(1_000):
        queue.partial_page(decision="rejected")
    capabilities = {native_work(queue, workspace=uid(500)).work_id for _ in range(2)}
    first = queue.claim()[0]
    second = queue.claim()[0]
    assert first.work_id in capabilities
    assert second.work_id not in capabilities and second.target is None
    assert not queue.claim()
    protected_completion(queue, first)
    following = queue.claim()[0]
    assert following.work_id in capabilities - {first.work_id}
    assert following.target.workspace_id == uid(500)
    assert row_for(queue, second).status == "leased"


def test_sql_cursor_uses_original_retry_receipt_when_work_lease_was_cleared():
    queue = make_queue(True)
    first = native_work(queue, workspace=uid(100))
    second = native_work(queue, workspace=uid(200))
    third = native_work(queue, workspace=uid(300))
    claimed = queue.claim()[0]
    assert claimed.work_id == first.work_id
    protected_completion(queue, claimed, waiting=True)
    queue.store = AzureSqlMonitoringStore(db=queue.db, component="controller")
    assert queue.claim()[0].work_id == second.work_id
    assert row_for(queue, third).status == "queued"


@pytest.mark.parametrize("status,delay", [("queued", 60), ("leased", -300)])
def test_workspace_projection_never_expediates_future_work_or_takes_active_ownership(queue, status, delay):
    fresh = native_work(queue, workspace=uid(500), state=status, due_offset=delay)
    other = native_work(queue, workspace=uid(600), due_offset=-300)
    assert [work.key for work in due(queue)] == [other.work_id]
    assert row_for(queue, fresh).status == status


@pytest.mark.parametrize("field,value", [
    ("workspace_id", "not-a-canonical-workspace"), ("workspace_id", ""),
    ("tenant_id", uid(999)), ("epoch", uid(999)),
])
def test_malformed_canonical_target_fails_before_a_claim_rpc(queue, field, value):
    work = native_work(queue, workspace=uid(500))
    row = row_for(queue, work)
    payload = json.loads(row.payload)
    payload["target"][field] = value
    queue.put(replace(row, payload=json.dumps(payload)))
    with pytest.raises(MonitoringUnavailable):
        queue.claim()
    assert row_for(queue, work).status == "queued"
    if queue.db:
        assert not any(sql.startswith("EXEC ") and "controller_claim_work" in sql for _, sql, _ in queue.db.calls)


def test_worker_ordering_does_not_gain_controller_rotation_or_progress_reads():
    queue = make_queue(True, component="worker")
    old = queue.work(kind="inventory", workspace=uid(100), state="queued", due_offset=-400)
    queue.work(kind="inventory", workspace=uid(200), state="queued", due_offset=0)
    assert [row.key for row in due(queue, kinds=("inventory",), after=uid(100))] == [old.work_id]
    assert queue.claim(kinds=("inventory",))[0].work_id == old.work_id
    assert not any(sql.startswith("WITH recent AS") for _, sql, _ in queue.db.calls)


@pytest.mark.parametrize("state", ["leased", "finalizing"])
def test_contradictory_active_workspace_fails_before_count_rank_or_limit(queue, state, caplog):
    active = native_work(queue, workspace=uid(100), state=state)
    queue.put(replace(row_for(queue, active), workspace_id=uid(200)))
    native_work(queue, workspace=uid(100))
    before = deepcopy((queue.records, queue.receipts))
    with pytest.raises(MonitoringUnavailable, match="workspace promotion"):
        due(queue)
    assert (queue.records, queue.receipts) == before
    assert "workspace promotion" in caplog.text
    if queue.db:
        assert not any("ROW_NUMBER()" in sql for _, sql, _ in queue.db.calls)
        assert not any("controller_claim_work" in sql for _, sql, _ in queue.db.calls)


def test_valid_recent_cursor_cannot_hide_an_older_contradictory_active_workspace(queue):
    active = native_work(queue, workspace=uid(100), state="leased")
    queue.put(replace(row_for(queue, active), workspace_id=uid(200)))
    recent = native_work(queue, workspace=uid(300), state="leased")
    recent = recent.model_copy(update={"lease": recent.lease.model_copy(update={
        "acquired_at": queue.h.clock() - timedelta(seconds=1),
    })})
    queue.put(replace(row_for(queue, recent), payload=recent.model_dump_json()))
    native_work(queue, workspace=uid(100))
    before = deepcopy((queue.records, queue.receipts))
    with pytest.raises(MonitoringUnavailable, match="workspace promotion"):
        queue.claim()
    assert (queue.records, queue.receipts) == before
    if queue.db:
        assert any(sql.startswith("WITH recent AS") for _, sql, _ in queue.db.calls)
        assert not any("ROW_NUMBER()" in sql for _, sql, _ in queue.db.calls)
        assert not any("controller_claim_work" in sql for _, sql, _ in queue.db.calls)


@pytest.mark.parametrize("workspace", [None, uid(100)])
def test_nonnull_workspace_promotion_must_match_a_queued_canonical_target(queue, workspace):
    work = native_work(queue, workspace=workspace)
    queue.put(replace(row_for(queue, work), workspace_id=uid(200)))
    before = deepcopy((queue.records, queue.receipts))
    with pytest.raises(MonitoringUnavailable, match="workspace promotion"):
        queue.claim()
    assert (queue.records, queue.receipts) == before
    if queue.db:
        assert not any("controller_claim_work" in sql for _, sql, _ in queue.db.calls)


@pytest.mark.parametrize("state", ["leased", "waiting", "completed"])
def test_sql_cursor_refuses_conflicting_promotion_instead_of_returning_fake_workspace(state, caplog):
    queue = make_queue(True)
    work = native_work(queue, workspace=uid(100), state="leased")
    if state != "leased":
        work = protected_completion(queue, work, waiting=state == "waiting")
    queue.put(replace(row_for(queue, work), workspace_id=uid(200)))
    before = deepcopy((queue.records, queue.receipts))
    request = m.WorkClaimRequest(
        **queue.h.context(), owner_id=uid(90), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    )
    with queue.store._sql.transaction(write=False, operation="queue_cursor", request_id=uid(91)):
        with pytest.raises(MonitoringUnavailable, match="workspace promotion"):
            queue.store._sql._fair_workspace(request)
    assert (queue.records, queue.receipts) == before
    assert "workspace promotion" in caplog.text
    assert not any("controller_claim_work" in sql for _, sql, _ in queue.db.calls)


@pytest.mark.parametrize("promoted", [None, uid(100)])
def test_null_or_matching_promotion_keeps_the_canonical_active_quota(queue, promoted):
    active = native_work(queue, workspace=uid(100), state="leased")
    queue.put(replace(row_for(queue, active), workspace_id=promoted))
    native_work(queue, workspace=uid(100))
    eligible = native_work(queue, workspace=uid(200))
    assert [row.key for row in due(queue)] == [eligible.work_id]
