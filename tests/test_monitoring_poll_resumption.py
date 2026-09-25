"""A restored admission needs runnable polling, not a terminal slot receipt."""

from __future__ import annotations

from datetime import timedelta

import pytest
from test_monitoring_sql_store import SqlHarness
from test_monitoring_store import Harness

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringUnavailable


@pytest.fixture(params=("memory", "sql"))
def harness(request, tmp_path):
    value = Harness() if request.param == "memory" else SqlHarness(tmp_path / "poll-resumption.sqlite")
    value.seed()
    value.activate()
    return value


def dispose(h, work, disposition="out_of_scope"):
    return h.store.disposition_work(m.WorkDispositionRequest(
        **h.context(), request_id=h.next_id(), work_id=work.work_id,
        expected_work_revision=work.revision, lease=work.lease, disposition=disposition,
        detail="Poll target no longer has current observation admission.",
        retry_at=h.clock() + timedelta(minutes=2) if disposition == "retry" else None,
    ))


def test_current_admission_resumes_a_dispositioned_poll_without_rewriting_it(harness):
    h = harness
    original, = h.claim(("poll",))
    terminal = dispose(h, original)
    h.clock.advance(30)
    h.capability(h.targets[0])
    resumed, = h.claim(("poll",))
    assert resumed.work_id != original.work_id
    assert resumed.policy_revision == h.version.revision
    assert resumed.due_at == h.clock()
    assert h.store.get_work(m.MonitoringContext(**h.context()), original.work_id) == terminal
    h.capability(h.targets[0])
    assert h.claim(("poll",)) == ()
    assert h.store.get_work(m.MonitoringContext(**h.context()), resumed.work_id) == resumed


@pytest.mark.parametrize("terminal", [False, True])
def test_scope_revision_does_not_reuse_old_policy_poll_work(harness, terminal):
    h = harness
    original, = h.claim(("poll",))
    retained = dispose(h, original) if terminal else original
    h.clock.advance(30)
    receipt = h.activate(h.scope.model_copy(update={"name": "Revised polling scope"}))
    context = m.MonitoringContext(**h.context())
    scheduled = [h.store.get_work(context, key) for key in receipt.queued_work_ids]
    queued, = [work for work in scheduled if work.kind == "poll"]
    assert queued.work_id != original.work_id and queued.policy_revision == h.version.revision
    assert queued.state == "queued"
    assert h.store.get_work(context, original.work_id) == retained
    if not terminal:
        # The existing workspace lease still limits concurrency until its owner
        # records the obsolete policy's non-effect disposition.
        assert h.claim(("poll",)) == ()
        retained = dispose(h, original, "superseded")
    current, = h.claim(("poll",))
    assert current.work_id != original.work_id
    assert current.policy_revision == h.version.revision
    assert current.due_at == h.clock()
    assert h.store.get_work(m.MonitoringContext(**h.context()), original.work_id) == retained


@pytest.mark.parametrize("state", ["queued", "leased", "waiting"])
def test_current_nonterminal_poll_keeps_its_original_identity_and_fence(harness, state):
    h = harness
    original = None
    if state != "queued":
        original, = h.claim(("poll",))
        if state == "waiting":
            original = dispose(h, original, "retry")
    h.clock.advance(30)
    h.capability(h.targets[0])
    if original is None:
        queued, = h.claim(("poll",))
        assert queued.due_at == h.control.updated_at
    else:
        assert h.store.get_work(m.MonitoringContext(**h.context()), original.work_id) == original
        assert h.claim(("poll",)) == ()


def test_multiple_dispositions_recover_through_deterministic_successors(harness):
    h = harness
    seen = set()
    for _ in range(3):
        work, = h.claim(("poll",))
        assert work.work_id not in seen
        seen.add(work.work_id)
        terminal = dispose(h, work)
        h.clock.advance(30)
        h.capability(h.targets[0])
        h.capability(h.targets[0])
        assert h.store.get_work(m.MonitoringContext(**h.context()), work.work_id) == terminal
    successor, = h.claim(("poll",))
    assert successor.work_id not in seen
    assert h.claim(("poll",)) == ()


def test_resumption_history_budget_fails_closed_without_rewriting_terminal_work(harness, monkeypatch):
    from triage.monitoring import engine

    h = harness
    first, = h.claim(("poll",))
    first_terminal = dispose(h, first)
    h.clock.advance(30)
    h.capability(h.targets[0])
    second, = h.claim(("poll",))
    second_terminal = dispose(h, second)
    h.clock.advance(30)
    monkeypatch.setattr(engine, "SCAN_BUDGET", 2)
    with pytest.raises(MonitoringUnavailable, match="bounded work history"):
        h.capability(h.targets[0])
    context = m.MonitoringContext(**h.context())
    assert h.store.get_work(context, first.work_id) == first_terminal
    assert h.store.get_work(context, second.work_id) == second_terminal
    assert h.claim(("poll",)) == ()
