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

from types import SimpleNamespace

import pytest

from triage.monitoring.controller import HeartbeatBudget, controller_heartbeat
from triage.monitoring.runtime import build_monitoring_store
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
