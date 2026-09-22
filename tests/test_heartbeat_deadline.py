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
