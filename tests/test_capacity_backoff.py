"""Tests for postponing a retry instead of making an outage worse.

Capacity throttling is the one failure in the set where the obvious fix is
actively harmful. The capacity has already exceeded its resource limits;
retrying adds load to the thing that is overloaded. Across several datasets at
once that turns contention into an outage caused by the system that was
supposed to be helping.

So the agent schedules the work rather than doing it, and the controller
refuses an immediate refresh while that window is open. The refusal is the
property worth testing: an agent that *usually* declines to retry is not a
control.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from test_agents import CannedProvider, _tool_response

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage_agent import TriageAgent, TriageDeps
from triage.policy import (
    REMEDIATION_ACTIONS,
    REPORTING_ACTIONS,
    PolicyLedger,
    TriagePolicy,
)
from triage.providers.mock import ScriptedDataQualityProvider
from triage.store.retries import (
    MAX_ATTEMPTS,
    InMemoryRetryStore,
    JsonFileRetryStore,
    backoff_seconds,
)
from triage.tools.flags import DataQualityFlagTable
from triage.tools.powerbi import MockPowerBIClient
from triage.tools.registry import ToolContext, ToolDispatcher
from triage.tools.teams import MockTeamsNotifier

SIGNATURE = "sigthrottle000001"


def _ctx(sample_request, tmp_path, *, retries, refresh_result="Throttled") -> ToolContext:
    return ToolContext(
        request=sample_request,
        ledger=PolicyLedger(TriagePolicy()),
        powerbi=MockPowerBIClient(latency_ms=0, refresh_result=refresh_result),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        signature=SIGNATURE,
        workspace_id="b8e42d17-3f6a-4c9b-8d21-77e4a1c9f003",
        dataset_id="9c05f731-6ad2-48be-b4e7-0f21ca8d3366",
        retries=retries,
    )


# ---------------------------------------------------------------------------
# Deferring is not remediating
# ---------------------------------------------------------------------------


def test_deferring_is_a_reporting_action() -> None:
    """It must not charge the remediation budget.

    If postponing the work spent the run's one remediation, the retry could
    never be performed when its window arrived -- the deferral would guarantee
    the thing it was scheduling could not happen.
    """
    assert "defer_refresh_retry" in REPORTING_ACTIONS
    assert "defer_refresh_retry" not in REMEDIATION_ACTIONS


async def test_deferring_costs_no_remediation_budget(sample_request, tmp_path) -> None:
    ctx = _ctx(sample_request, tmp_path, retries=InMemoryRetryStore())

    result = await ToolDispatcher(ctx).dispatch(
        "defer_refresh_retry", {"reason": "capacity saturated"}
    )

    assert result["status"] == "pending"
    assert ctx.ledger.write_actions == 0
    assert ctx.powerbi.calls == [], "deferring must not touch Power BI at all"


# ---------------------------------------------------------------------------
# The refusal, which is the point
# ---------------------------------------------------------------------------


async def test_a_refresh_is_refused_while_the_backoff_window_is_open(
    sample_request, tmp_path
) -> None:
    """The controller enforces the wait, not the prompt.

    A model that is merely *told* not to retry can be argued out of it, and a
    stale conversation reintroduces the stampede.
    """
    retries = InMemoryRetryStore()
    ctx = _ctx(sample_request, tmp_path, retries=retries)
    dispatcher = ToolDispatcher(ctx)

    await dispatcher.dispatch("defer_refresh_retry", {"reason": "throttled"})
    result = await dispatcher.dispatch("refresh_powerbi_dataset", {"justification": "try now"})

    assert result["status"] == "blocked_by_policy"
    assert "already scheduled" in result["reason"]
    assert ("refresh_dataset", {
        "workspace_id": ctx.workspace_id,
        "dataset_id": ctx.dataset_id,
    }) not in ctx.powerbi.calls


async def test_a_refused_refresh_does_not_spend_the_remediation_budget(
    sample_request, tmp_path
) -> None:
    """A refusal that costs the budget silently disarms the agent.

    The same rule as an approval denial: being told no must not consume the
    one remediation, or the next legitimate fix is refused for a reason nobody
    can see.
    """
    retries = InMemoryRetryStore()
    ctx = _ctx(sample_request, tmp_path, retries=retries)
    dispatcher = ToolDispatcher(ctx)

    await dispatcher.dispatch("defer_refresh_retry", {"reason": "throttled"})
    await dispatcher.dispatch("refresh_powerbi_dataset", {"justification": "try now"})

    assert ctx.ledger.write_actions == 0
    assert ctx.ledger.policy.max_write_actions == 1


async def test_a_refresh_is_allowed_once_the_window_has_passed(
    sample_request, tmp_path
) -> None:
    """The wait is a wait, not a ban."""
    retries = InMemoryRetryStore()
    retries.defer(signature=SIGNATURE, reason="throttled", retry_after_seconds=1)
    # One second of backoff, already elapsed by the time this line runs on any
    # machine slow enough to matter -- so assert on the store's own view.
    row = retries.get(SIGNATURE)
    row["due_at"] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat(timespec="seconds")
    retries._items[SIGNATURE] = row

    ctx = _ctx(sample_request, tmp_path, retries=retries, refresh_result="Completed")
    result = await ToolDispatcher(ctx).dispatch(
        "refresh_powerbi_dataset", {"justification": "window passed"}
    )

    assert result["status"] == "Completed"


async def test_with_no_retry_store_deferring_says_so_instead_of_pretending(
    sample_request, tmp_path
) -> None:
    """Reporting scheduled work into a store that does not exist drops it."""
    ctx = _ctx(sample_request, tmp_path, retries=None)

    result = await ToolDispatcher(ctx).dispatch(
        "defer_refresh_retry", {"reason": "throttled"}
    )

    assert result["status"] == "unavailable"


# ---------------------------------------------------------------------------
# Bounded: it must not defer forever
# ---------------------------------------------------------------------------


def test_backoff_grows() -> None:
    assert backoff_seconds(1) < backoff_seconds(2) < backoff_seconds(3)


def test_the_service_retry_after_wins_when_it_is_given() -> None:
    """Honour what the service asked for rather than guessing."""
    row = InMemoryRetryStore().defer(
        signature=SIGNATURE, reason="throttled", retry_after_seconds=42
    )
    assert row["wait_seconds"] == 42


def test_deferral_is_exhausted_rather_than_repeated_forever() -> None:
    """An agent that defers indefinitely has invented a patient way to do nothing."""
    retries = InMemoryRetryStore()
    for _ in range(MAX_ATTEMPTS):
        assert retries.defer(signature=SIGNATURE, reason="throttled")["status"] == "pending"

    final = retries.defer(signature=SIGNATURE, reason="throttled")

    assert final["status"] == "exhausted"
    assert "scheduling problem" in final["reason"]
    assert retries.pending() == [], "an exhausted row must not look like scheduled work"


def test_one_row_per_signature_not_one_per_alert() -> None:
    """A throttle storm produces many alerts for the same model.

    Scheduling a retry per alert would recreate the stampede the deferral
    exists to prevent.
    """
    retries = InMemoryRetryStore()
    retries.defer(signature=SIGNATURE, reason="first")
    retries.defer(signature=SIGNATURE, reason="second")

    assert len(retries.pending()) == 1
    assert retries.get(SIGNATURE)["attempts"] == 2


# ---------------------------------------------------------------------------
# It has to survive the process that scheduled it
# ---------------------------------------------------------------------------


def test_a_deferral_outlives_the_run_that_made_it(tmp_path) -> None:
    """The run that defers and the sweep that performs it are different processes."""
    path = tmp_path / "retries.json"
    JsonFileRetryStore(path).defer(signature=SIGNATURE, reason="throttled")

    assert JsonFileRetryStore(path).pending()


def test_an_unreadable_due_time_does_not_strand_the_retry(tmp_path) -> None:
    """A row nobody can parse must not become permanently undrainable."""
    retries = InMemoryRetryStore()
    retries.defer(signature=SIGNATURE, reason="throttled")
    retries._items[SIGNATURE]["due_at"] = "not a timestamp"

    assert retries.due(), "the retry was stranded by an unreadable due time"


# ---------------------------------------------------------------------------
# Draining actually performs the work
# ---------------------------------------------------------------------------


async def test_draining_performs_the_retry_and_closes_the_incident(
    runner, sample_request, result_factory
) -> None:
    """Nothing drains itself.

    Without a drain the tool writes a row, the run reports scheduled work, and
    the retry never happens -- which is worse than refusing, because the
    incident reads as handled.
    """
    incident = runner.store.record(result_factory(outcome="needs_human", signature=SIGNATURE))
    assert incident.status == "open"

    runner.retries.defer(
        signature=SIGNATURE,
        workspace_id="ws",
        dataset_id="ds",
        report_name="Product Performance Trends",
        reason="throttled",
    )

    # Look at the world from after the backoff window rather than sleeping.
    lines = await runner.drain_due_retries(now=datetime.now(UTC) + timedelta(hours=2))

    assert lines and "completed" in lines[0]
    assert runner.retries.pending() == []
    assert runner.store.get(incident.id).status == "resolved", (
        "an incident left open after the fix keeps suppressing real recurrences"
    )


async def test_nothing_is_drained_before_it_is_due(runner) -> None:
    runner.retries.defer(signature=SIGNATURE, reason="throttled")
    assert await runner.drain_due_retries() == []


# ---------------------------------------------------------------------------
# The claim has to be backed by a row
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("defer_first,expected", [(True, "deferred_retry"), (False, "needs_human")])
async def test_reporting_a_deferred_retry_requires_an_actual_deferral(
    sample_request, tmp_path, defer_first: bool, expected: str
) -> None:
    """"Scheduled for later" is a promise, and promises get validated here.

    Accepting it without a row means the work is neither done nor queued, and
    the incident reads as handled.
    """
    responses = []
    if defer_first:
        responses.append(_tool_response("defer_refresh_retry", {"reason": "throttled"}))
    responses.append(
        _tool_response(
            "report_resolution",
            {"outcome": "deferred_retry", "summary": "Scheduled.", "root_cause": "throttled"},
        )
    )

    deps = TriageDeps(
        powerbi=MockPowerBIClient(latency_ms=0, refresh_result="Throttled"),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        signature=SIGNATURE,
        retries=InMemoryRetryStore(),
    )
    agent = TriageAgent(
        CannedProvider(responses=responses),
        dq_agent=DataQualityAgent(ScriptedDataQualityProvider()),
    )

    result = await agent.run(sample_request, deps)

    assert result.outcome == expected
