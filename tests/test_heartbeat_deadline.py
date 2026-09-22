"""The heartbeat's deadline is configured and checked, never assumed.

``HEARTBEAT_BUDGET_SECONDS`` was 840, justified by a comment stating that "the
scheduled invocation has a 900-second HTTP limit". Measured against the
deployed caller, that is false. The ``bi-triage-command-sweep`` Logic App
configures ``timeout: PT15M`` on its agent call, but Logic Apps Consumption
enforces a 120-second ceiling on a synchronous outbound request: its
``Invoke_the_agent`` action failed three times with ``code=BadRequest`` at
exactly 120 seconds, while all twenty-five successful runs finished within 115.

So the controller reasoned with a deadline seven times longer than it had. It
admitted work it could never report and was cut off mid-flight, and because
coroutine cancellation does not stop a running ``asyncio.to_thread`` body, the
SQL work continued with nobody left to observe its outcome. Cancelled
heartbeats recorded ``automatic_results=1`` and ``2``: work that finished and
whose report was discarded.

A deadline the runtime cannot verify is a guess. These tests require it to be
stated, and require the contradiction between a deadline and the work it must
accommodate to be raised rather than silently starving admission.
"""

from __future__ import annotations

import pytest

from triage.monitoring.controller import (
    HEARTBEAT_BUDGET_SECONDS,
    HeartbeatBudget,
    controller_heartbeat,
    heartbeat_budget_seconds,
    heartbeat_response_seconds,
)
from triage.settings import Settings


def _settings(**overrides) -> Settings:
    values = dict(
        _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
        triage_provider_mode="mock", azure_sql_server="", azure_sql_database="",
        applicationinsights_connection_string="",
    )
    values.update(overrides)
    return Settings(**values)


def test_the_default_budget_is_the_documented_one() -> None:
    assert heartbeat_budget_seconds(_settings()) == HEARTBEAT_BUDGET_SECONDS


def test_a_configured_deadline_replaces_the_default() -> None:
    """An operator whose caller waits longer must be able to say so."""
    assert heartbeat_budget_seconds(_settings(heartbeat_budget_seconds=1200)) == 1200


def test_a_deadline_equal_to_one_work_allowance_is_the_minimum() -> None:
    """Exactly one unit fits, admitted on the first check and no more."""
    assert heartbeat_budget_seconds(_settings(heartbeat_budget_seconds=690)) == 690
    with pytest.raises(ValueError, match="cannot admit"):
        heartbeat_budget_seconds(_settings(heartbeat_budget_seconds=689))


@pytest.mark.parametrize("deadline", [110, 400])
def test_a_deadline_shorter_than_its_work_is_refused(deadline: int) -> None:
    """Silent starvation is the failure this replaces.

    With ``work_seconds`` at 690, any shorter deadline makes ``can_claim()``
    false on the first check, so every heartbeat returns having done nothing,
    for ever, with no error anywhere. 110 is the deployed caller's real
    ceiling; 400 is a plausible operator guess. Both are contradictions and
    both are now reported.
    """
    with pytest.raises(ValueError, match="cannot admit"):
        heartbeat_budget_seconds(_settings(heartbeat_budget_seconds=deadline))


@pytest.mark.parametrize("value", [0, -1])
def test_a_nonpositive_deadline_is_refused(value: int) -> None:
    with pytest.raises(ValueError):
        heartbeat_budget_seconds(_settings(heartbeat_budget_seconds=value))


async def test_the_heartbeat_uses_the_configured_deadline() -> None:
    """The budget the workers check comes from settings, not the module constant."""
    seen: list[float] = []

    class Runner:
        settings = _settings(heartbeat_budget_seconds=1200)

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            seen.append(budget.deadline)
            return []

    async def human(_runner, *, limit, budget):
        return []

    await controller_heartbeat(Runner(), command_drain=human, started_at=0.0, clock=lambda: 0.0)

    assert seen and all(deadline == 1200.0 for deadline in seen)


def test_admission_closes_before_the_deadline_by_a_full_work_allowance() -> None:
    """The property the deadline exists for, stated directly."""
    budget = HeartbeatBudget(deadline=1200.0, work_seconds=690.0, clock=lambda: 509.0)
    assert budget.can_claim()
    assert not HeartbeatBudget(deadline=1200.0, work_seconds=690.0, clock=lambda: 511.0).can_claim()


# --- returning before the caller gives up ------------------------------------


async def test_the_heartbeat_stops_starting_work_before_the_response_deadline() -> None:
    """Return a partial report rather than being killed holding a full one.

    Cancelled heartbeats recorded automatic_calls=3 and 4: the loop kept
    starting units past the caller's ceiling. Stopping before it converts a
    cancellation into an ordinary partial result, and the units already
    admitted keep their leases either way.
    """
    elapsed = [0.0]
    calls = []

    class Runner:
        settings = _settings(heartbeat_response_seconds=100)

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            calls.append(elapsed[0])
            elapsed[0] += 40
            return [f"- unit at {int(calls[-1])}s"]

    async def human(_runner, *, limit, budget):
        return []

    lines = await controller_heartbeat(
        Runner(), command_drain=human, started_at=0.0, clock=lambda: elapsed[0],
    )

    assert calls and max(calls) < 100, f"work started after the deadline: {calls}"
    assert lines, "a bounded heartbeat still reports what it did"


async def test_an_unbounded_response_deadline_keeps_draining() -> None:
    """Zero means no response bound, for a caller that genuinely waits."""
    elapsed = [0.0]
    calls = []

    class Runner:
        settings = _settings(heartbeat_response_seconds=0)

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            calls.append(elapsed[0])
            elapsed[0] += 40
            return ["- unit"]

    async def human(_runner, *, limit, budget):
        return []

    await controller_heartbeat(
        Runner(), command_drain=human, started_at=0.0, clock=lambda: elapsed[0],
    )

    assert max(calls) >= 100, "an unbounded heartbeat stopped early"


def test_a_response_deadline_above_the_admission_budget_is_refused() -> None:
    """Promising a longer response than the admission window is incoherent."""
    with pytest.raises(ValueError, match="response"):
        heartbeat_response_seconds(_settings(heartbeat_response_seconds=900))


def test_a_negative_response_deadline_is_refused() -> None:
    with pytest.raises(ValueError, match="response"):
        heartbeat_response_seconds(_settings(heartbeat_response_seconds=-1))
