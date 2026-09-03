"""Tests for re-arming a refresh schedule Power BI switched off.

Power BI disables a semantic model's refresh schedule after four consecutive
failures and never re-enables it. The failure mode that creates is unusually
nasty: once the schedule is off, no scheduled run happens, so no further
failure alert is raised. The model stops updating and the signal that would
tell anyone stops at the same moment.

Re-arming it is a small action with a large consequence -- it hands an
unattended job back to a platform that already switched it off once -- so it
needs both a human decision and deterministic evidence that the cause is gone.
"""

from __future__ import annotations

import pytest
from test_agents import CannedProvider, _tool_response

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage_agent import TriageAgent, TriageDeps
from triage.approvals import AutoApproveGate, AutoDenyGate
from triage.policy import (
    APPROVAL_REQUIRED_ACTIONS,
    DIAGNOSTIC_ACTIONS,
    REMEDIATION_ACTIONS,
    PolicyLedger,
    TriagePolicy,
)
from triage.providers.mock import ScriptedDataQualityProvider
from triage.tools.flags import DataQualityFlagTable
from triage.tools.powerbi import MockPowerBIClient
from triage.tools.registry import ToolContext, ToolDispatcher
from triage.tools.teams import MockTeamsNotifier

REENABLE = "reenable_refresh_schedule"

HEALTHY = [{"requestId": "r1", "status": "Completed", "refreshType": "ViaApi"}]
STILL_BROKEN = [{"requestId": "r1", "status": "Failed", "refreshType": "Scheduled"}]


def _ctx(sample_request, tmp_path, *, history, gate=None, schedule_enabled=False) -> ToolContext:
    return ToolContext(
        request=sample_request,
        ledger=PolicyLedger(TriagePolicy()),
        powerbi=MockPowerBIClient(
            latency_ms=0, history=list(history), schedule_enabled=schedule_enabled
        ),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        signature="sigschedule000001",
        workspace_id="b8e42d17-3f6a-4c9b-8d21-77e4a1c9f003",
        dataset_id="4e6b1d90-3c72-4a58-8f19-b27ec5a40d83",
        approval_gate=gate or AutoApproveGate(approver="priya"),
    )


# ---------------------------------------------------------------------------
# The allowlists
# ---------------------------------------------------------------------------


def test_the_action_is_on_the_right_lists() -> None:
    """Anything not on an allowlist is refused before dispatch."""
    assert REENABLE in REMEDIATION_ACTIONS
    assert REENABLE in APPROVAL_REQUIRED_ACTIONS
    assert "get_refresh_schedule" in DIAGNOSTIC_ACTIONS
    assert "get_refresh_schedule" not in REMEDIATION_ACTIONS


# ---------------------------------------------------------------------------
# The precondition: evidence, not optimism
# ---------------------------------------------------------------------------


async def test_a_still_failing_dataset_is_refused_before_anyone_is_asked(
    sample_request, tmp_path
) -> None:
    """Re-arming a broken schedule fails four more times and disables it again.

    Refused by the controller rather than by prompt wording, and refused
    *before* the approval request goes out: asking a human to authorise
    something the controller will reject anyway teaches people to click
    through approvals without reading them.
    """
    gate = AutoApproveGate(approver="priya")
    ctx = _ctx(sample_request, tmp_path, history=STILL_BROKEN, gate=gate)

    result = await ToolDispatcher(ctx).dispatch(REENABLE, {"justification": "please"})

    assert result["status"] == "blocked_by_policy"
    assert "not 'Completed'" in result["reason"]
    assert ctx.approvals == [], "a human was asked to approve a refused action"
    assert ("set_refresh_schedule_enabled", {"enabled": True}) not in ctx.powerbi.calls


async def test_no_history_at_all_is_refused(sample_request, tmp_path) -> None:
    """Absence of evidence is not evidence the schedule is safe to re-arm."""
    ctx = _ctx(sample_request, tmp_path, history=[])
    result = await ToolDispatcher(ctx).dispatch(REENABLE, {"justification": "please"})

    assert result["status"] == "blocked_by_policy"
    assert "no refresh history" in result["reason"].lower()


async def test_a_successful_last_refresh_permits_it(sample_request, tmp_path) -> None:
    """The real case: somebody fixed it by hand and nobody re-armed the schedule."""
    ctx = _ctx(sample_request, tmp_path, history=HEALTHY)
    result = await ToolDispatcher(ctx).dispatch(REENABLE, {"justification": "fixed"})

    assert result["status"] == "Completed"
    assert ("set_refresh_schedule_enabled", {"enabled": True}) in ctx.powerbi.calls
    assert ctx.powerbi.schedule_enabled is True


async def test_refreshing_first_leaves_no_budget_to_re_arm(
    sample_request, tmp_path
) -> None:
    """Fix-then-re-arm is two writes, and a run only gets one.

    This is not a bug to route around -- it is the remediation budget doing its
    job, and weakening it to make a nicer story would give away the property
    the whole design rests on. It is why the scenario uses the realistic shape
    instead: somebody fixed the cause and ran a manual refresh days ago, so the
    agent needs one write, not two.

    A per-playbook budget is the honest way to support both, and is listed as
    production work this design is ported from.
    """
    ctx = _ctx(sample_request, tmp_path, history=STILL_BROKEN)
    dispatcher = ToolDispatcher(ctx)

    refresh = await dispatcher.dispatch("refresh_powerbi_dataset", {"justification": "retry"})
    assert refresh["status"] == "Completed"

    result = await dispatcher.dispatch(REENABLE, {"justification": "the retry worked"})

    assert result["status"] == "blocked_by_policy"
    assert "max_write_actions=1" in result["reason"]
    assert ("set_refresh_schedule_enabled", {"enabled": True}) not in ctx.powerbi.calls


async def test_a_run_wide_budget_of_two_allows_fix_then_re_arm(
    sample_request, tmp_path
) -> None:
    """With the budget it needs, the in-run evidence path works.

    Proves the precondition accepts a refresh performed during the run, not
    only one found in history -- the block above is the budget, not the
    evidence check.
    """
    ctx = _ctx(sample_request, tmp_path, history=STILL_BROKEN)
    ctx.ledger = PolicyLedger(TriagePolicy(max_write_actions=2))
    dispatcher = ToolDispatcher(ctx)

    await dispatcher.dispatch("refresh_powerbi_dataset", {"justification": "retry"})
    result = await dispatcher.dispatch(REENABLE, {"justification": "the retry worked"})

    assert result["status"] == "Completed"


# ---------------------------------------------------------------------------
# It is still a gated action
# ---------------------------------------------------------------------------


async def test_a_declined_reenable_never_touches_power_bi(
    sample_request, tmp_path
) -> None:
    ctx = _ctx(
        sample_request,
        tmp_path,
        history=HEALTHY,
        gate=AutoDenyGate(approver="sam", reason="month end change freeze"),
    )
    result = await ToolDispatcher(ctx).dispatch(REENABLE, {"justification": "fixed"})

    assert result["status"] == "not_approved"
    assert ("set_refresh_schedule_enabled", {"enabled": True}) not in ctx.powerbi.calls
    assert ctx.powerbi.schedule_enabled is False


async def test_the_approval_card_states_what_re_arming_means(
    sample_request, tmp_path
) -> None:
    """An approval that does not state the consequence is a rubber stamp."""
    ctx = _ctx(sample_request, tmp_path, history=HEALTHY)
    await ToolDispatcher(ctx).dispatch(REENABLE, {"justification": "fixed"})

    assert ctx.approvals
    impact = ctx.approvals[0].impact
    assert "unattended" in impact
    assert "disable it again" in impact


# ---------------------------------------------------------------------------
# Reading the switch is not changing it
# ---------------------------------------------------------------------------


async def test_reading_the_schedule_costs_no_remediation_budget(
    sample_request, tmp_path
) -> None:
    """A diagnostic that consumed the write budget would make the agent choose
    between looking and acting."""
    ctx = _ctx(sample_request, tmp_path, history=HEALTHY)
    dispatcher = ToolDispatcher(ctx)

    result = await dispatcher.dispatch("get_refresh_schedule", {})

    assert result["enabled"] is False
    assert ctx.ledger.write_actions == 0
    assert ctx.approvals == [], "reading a setting must not require approval"


async def test_an_unreadable_schedule_is_not_treated_as_enabled(
    sample_request, tmp_path
) -> None:
    """Unknown is not the same as fine.

    If a permissions error made the schedule unreadable and that read as
    'enabled', the controller would conclude there is nothing to re-arm and the
    model would stay silently stale -- the exact failure being hunted.
    """

    class _Unreadable(MockPowerBIClient):
        async def get_refresh_schedule(self, workspace_id: str, dataset_id: str):
            return {"enabled": None, "error": "HTTP 403"}

    ctx = _ctx(sample_request, tmp_path, history=HEALTHY)
    ctx.powerbi = _Unreadable(latency_ms=0)

    result = await ToolDispatcher(ctx).dispatch("get_refresh_schedule", {})

    assert result["enabled"] is None
    assert ctx.schedule_enabled is None


# ---------------------------------------------------------------------------
# End to end through the agent
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("history,expected", [(HEALTHY, "resolved"), (STILL_BROKEN, "needs_human")])
async def test_the_run_reports_what_actually_happened(
    sample_request, tmp_path, history, expected
) -> None:
    """A successful re-arm is a completed remediation; a refused one is not.

    ``_validate_outcome`` refuses to record success without a remediation that
    completed, so the tool has to record itself as one -- otherwise the run does
    the work, succeeds, and is downgraded for lack of evidence it did anything.
    """
    deps = TriageDeps(
        powerbi=MockPowerBIClient(latency_ms=0, history=list(history), schedule_enabled=False),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        signature="sigschedule000002",
        approval_gate=AutoApproveGate(approver="priya"),
    )
    provider = CannedProvider(
        responses=[
            _tool_response("get_refresh_schedule", {}),
            _tool_response(REENABLE, {"justification": "the last refresh completed"}),
            _tool_response(
                "report_resolution",
                {"outcome": "resolved", "summary": "Schedule re-armed.", "root_cause": "disabled"},
            ),
        ]
    )
    agent = TriageAgent(provider, dq_agent=DataQualityAgent(ScriptedDataQualityProvider()))

    result = await agent.run(sample_request, deps)

    assert result.outcome == expected
