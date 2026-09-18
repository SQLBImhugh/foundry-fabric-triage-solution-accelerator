"""Current-policy publication gets its existing slot before obsolete cleanup."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from test_monitoring_store import uid
from test_monitoring_workspace_fairness import (
    due,
    make_queue,
    native_work,
    protected_completion,
    row_for,
)

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringUnavailable


@pytest.fixture(params=[False, True], ids=["memory", "sql-emitted-selects"])
def queue(request):
    return make_queue(request.param)


def set_control(queue, revision):
    control = queue.h.control.model_copy(update={"revision": revision})
    queue.h.control = control
    queue.h.state.control_row = control.model_dump(mode="json")
    if queue.db:
        queue.db.control = control


def set_policy(queue, work, revision):
    saved = work.model_copy(update={"policy_revision": revision})
    queue.put(replace(row_for(queue, work), payload=saved.model_dump_json()))
    return saved


def inventory_handoff(queue, *, revision, due_offset):
    work = set_policy(queue, native_work(queue, due_offset=due_offset), revision)
    generation_id = queue.h.next_id()
    frontier_key = f"validation:v1:{work.epoch}:{work.tenant_id}:inventory:{generation_id}"
    request = m.ReconciliationRequest(
        **queue.h.context(), request_id=work.reconcile_request_id, producer="worker", topic="inventory",
        reference_id=generation_id, fingerprint="1" * 64, policy_revision=revision,
        work_id=work.work_id, frontier_key=frontier_key, frontier_revision=1,
        created_at=queue.h.clock(), request_payload={"generation_id": generation_id},
    )
    queue.record("worker_reconcile_request", request.request_id, request)
    queue.record("validation_frontier", frontier_key, m.ValidationFrontier(
        **queue.h.context(), frontier_key=frontier_key, accepted_revision=1,
        latest_request_id=request.request_id, updated_at=queue.h.clock(),
    ), status="pending_validation", sequence_number=1)
    return work


def test_current_targetless_inventory_is_selected_before_1000_obsolete_targetless_handoffs(queue):
    for _ in range(1_000):
        inventory_handoff(queue, revision=0, due_offset=-400)
    current = inventory_handoff(queue, revision=1, due_offset=0)
    before, receipts = deepcopy(queue.records), deepcopy(queue.receipts)
    assert queue.claim()[0].work_id == current.work_id
    assert queue.receipts == receipts
    assert all(queue.records[key] == row for key, row in before.items() if row.key != current.work_id)


def test_original_12_current_pages_are_served_in_12_single_slot_claims_without_history_repair(queue):
    started = queue.h.clock()
    old = [inventory_handoff(queue, revision=0, due_offset=-400) for _ in range(1_000)]
    current = {inventory_handoff(queue, revision=1, due_offset=0).work_id for _ in range(12)}
    old_rows = {work.work_id: row_for(queue, work) for work in old}
    protected = {key: row for key, row in queue.records.items() if row.kind != "work"}
    claimed = []
    for _ in range(12):
        work = queue.claim()[0]
        claimed.append(work.work_id)
        assert work.policy_revision == 1 and work.target is None and work.attempts == 1
        protected_completion(queue, work)
    assert set(claimed) == current
    assert all(row_for(queue, work) == old_rows[work.work_id] for work in old)
    assert all(queue.records[key] == row for key, row in protected.items())
    assert queue.h.clock() - started == timedelta(microseconds=12)
    assert queue.h.control.revision == 1
    assert queue.claim()[0].work_id in old_rows


@pytest.mark.parametrize("revision", [0, 7])
@pytest.mark.parametrize("after", ["", uid(100)])
def test_all_current_policy_keeps_original_rotation_and_batch_shape(queue, revision, after):
    set_control(queue, revision)
    works = {}
    for workspace in (None, uid(100), uid(200)):
        for index in range(3):
            work = native_work(queue, workspace=workspace, due_offset=-300 + 100 * index)
            works[(workspace, index)] = set_policy(queue, work, revision).work_id
    rotation = [uid(100), uid(200), None] if not after else [uid(200), None, uid(100)]
    expected = [works[(workspace, 0)] for workspace in rotation] + [works[(rotation[0], 1)]]
    assert [row.key for row in due(queue, after=after, limit=4, share=2)] == expected


def test_old_web_intent_keeps_priority_over_new_current_worker_inventory(queue):
    inventory_handoff(queue, revision=1, due_offset=0)
    web = queue.work(producer="web", state="queued", due_offset=-100)
    queue.record("validation_frontier", "validation:pending-revocation", m.ValidationFrontier(
        **queue.h.context(), frontier_key="validation:pending-revocation",
        accepted_revision=1, latest_request_id=web.reconcile_request_id, updated_at=queue.h.clock(),
    ), status="pending_validation")
    queue.record("action", uid(850), {"state": "uncertain", "fence": 4}, status="uncertain")
    queue.record("incident_state", "incident:reserved-budget", {"action_count": 1})
    before = deepcopy(queue.receipts)
    protected = {key: row for key, row in queue.records.items() if row.kind != "work"}
    assert queue.claim()[0].work_id == web.work_id
    assert queue.receipts == before
    assert all(queue.records[key] == row for key, row in protected.items())


def test_ordinary_finalization_keeps_its_existing_slot_before_current_metadata(queue):
    finalization = queue.work(kind="finalize", workspace=uid(100), state="queued", due_offset=-200)
    set_policy(queue, native_work(queue, workspace=uid(100), due_offset=0), 1)
    set_policy(queue, native_work(queue, workspace=uid(100), due_offset=-400), 0)
    assert [row.key for row in due(queue, kinds=("finalize", "reconcile_state"))] == [finalization.work_id]


def test_submitted_action_verification_deadline_and_original_effect_remain_unchanged():
    queue = make_queue(True)
    draft = queue.work(kind="finalize", workspace=uid(100), state="queued", due_offset=-400)
    verification = m.MonitoringWork.model_validate({
        **draft.model_dump(), "kind": "verify_action", "action_reservation_id": uid(851),
    })
    queue.put(replace(row_for(queue, draft), work_kind="verify_action", payload=verification.model_dump_json()))
    effect = queue.record("action", uid(851), {
        "state": "submitted", "fence": 4,
        "next_verification_at": (queue.h.clock() + timedelta(minutes=1)).isoformat(),
    }, status="submitted")
    current = native_work(queue, workspace=uid(100), due_offset=0)
    old = set_policy(queue, native_work(queue, workspace=uid(100), due_offset=-400), 0)
    before = deepcopy((queue.records, queue.receipts))
    assert [row.key for row in due(queue, kinds=("verify_action", "reconcile_state"))] == [current.work_id]
    assert (queue.records, queue.receipts) == before
    assert any(row == effect for row in queue.records.values())
    assert row_for(queue, old).status == "queued"


def test_both_slots_share_unchanged_workspace_cap_for_targetless_current_publication(queue):
    for _ in range(20):
        inventory_handoff(queue, revision=0, due_offset=-400)
    current = inventory_handoff(queue, revision=1, due_offset=0)
    other_workspace = native_work(queue, workspace=uid(500), due_offset=0)
    first = queue.claim()[0]
    second = queue.claim()[0]
    assert {first.work_id, second.work_id} == {current.work_id, other_workspace.work_id}
    assert not queue.claim()


@pytest.mark.parametrize("bad_policy", [None, "1", True, -1, 1.5, 2])
def test_future_or_malformed_reconciliation_policy_is_not_current_or_healthy_cleanup(queue, bad_policy):
    work = native_work(queue)
    row = row_for(queue, work)
    payload = json.loads(row.payload)
    payload["policy_revision"] = bad_policy
    queue.put(replace(row, payload=json.dumps(payload)))
    before = deepcopy((queue.records, queue.receipts))
    with pytest.raises(MonitoringUnavailable):
        queue.claim()
    assert (queue.records, queue.receipts) == before
    if queue.db:
        assert not any("controller_claim_work" in sql for _, sql, _ in queue.db.calls)


def test_authoritative_control_not_caller_cached_version_selects_current_revision(queue):
    queue.h.version = m.RegistryVersion(**queue.h.context(), revision=999)
    old = inventory_handoff(queue, revision=0, due_offset=-400)
    current = inventory_handoff(queue, revision=1, due_offset=0)
    assert queue.claim()[0].work_id == current.work_id
    assert row_for(queue, old).status == "queued"


@pytest.mark.parametrize("bad_control", ["missing", "wrong_epoch"])
def test_missing_or_wrong_context_control_cannot_default_to_policy_zero(queue, monkeypatch, bad_control):
    inventory_handoff(queue, revision=0, due_offset=-100)
    backend = queue.store._sql if queue.db else queue.store._backend
    with backend.transaction(write=False, operation="policy_selection", request_id=uid(91)):
        value = None if bad_control == "missing" else {
            **queue.h.control.model_dump(mode="json"), "epoch": uid(999),
        }
        monkeypatch.setattr(backend, "control", lambda: value)
        with pytest.raises(MonitoringUnavailable):
            backend.due(m.WorkClaimRequest(
                **queue.h.context(), owner_id=uid(90), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
            ), after_workspace="")


def test_worker_order_remains_unchanged():
    queue = make_queue(True, component="worker")
    old = queue.work(kind="inventory", state="queued", due_offset=-100)
    set_policy(queue, queue.work(kind="inventory", state="queued", due_offset=0), 1)
    assert queue.claim(kinds=("inventory",))[0].work_id == old.work_id
    assert not any("OPENJSON(payload)" in sql for _, sql, _ in queue.db.calls)


def test_memory_cannot_order_by_cached_policy_outside_an_operation_transaction():
    queue = make_queue(False)
    inventory_handoff(queue, revision=0, due_offset=-100)
    with pytest.raises(MonitoringUnavailable, match="operation transaction"):
        queue.store._backend.due(m.WorkClaimRequest(
            **queue.h.context(), owner_id=uid(90), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
        ), after_workspace="")
