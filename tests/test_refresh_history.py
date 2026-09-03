"""Tests for refresh-history analysis.

The rule: Power BI deactivates a model's refresh SCHEDULE after four consecutive
SCHEDULED failures. On-demand and API-triggered refreshes are a different
trigger path — they neither advance the counter nor reset it.

Getting that wrong in either direction is bad. Counting API retries makes the
agent escalate needlessly; treating a successful API retry as having cleared the
counter walks it straight past a model one scheduled run from going silent.
"""

from __future__ import annotations

from triage.knowledge.refresh_history import (
    DEACTIVATION_THRESHOLD,
    assess_deactivation_risk,
)


def sched(status: str) -> dict:
    return {"status": status, "refreshType": "Scheduled"}


def api(status: str) -> dict:
    return {"status": status, "refreshType": "ViaApi"}


# ---------------------------------------------------------------------------
# Counting
# ---------------------------------------------------------------------------


def test_counts_consecutive_scheduled_failures_newest_first() -> None:
    risk = assess_deactivation_risk([sched("Failed"), sched("Failed"), sched("Completed")])
    assert risk.consecutive_scheduled_failures == 2


def test_a_successful_scheduled_run_resets_the_counter() -> None:
    risk = assess_deactivation_risk(
        [sched("Completed"), sched("Failed"), sched("Failed"), sched("Failed")]
    )
    assert risk.consecutive_scheduled_failures == 0
    assert risk.at_risk is False


def test_empty_history_is_safe() -> None:
    risk = assess_deactivation_risk([])
    assert risk.consecutive_scheduled_failures == 0
    assert risk.scheduled_runs_examined == 0
    assert "No scheduled refresh runs" in risk.headline()


def test_none_history_is_safe() -> None:
    assert assess_deactivation_risk(None).consecutive_scheduled_failures == 0


# ---------------------------------------------------------------------------
# The correction: API-triggered refreshes are a different trigger path
# ---------------------------------------------------------------------------


def test_api_failures_do_not_advance_the_counter() -> None:
    """An agent retrying via REST must not push the model toward deactivation."""
    risk = assess_deactivation_risk(
        [api("Failed"), api("Failed"), api("Failed"), sched("Failed"), sched("Completed")]
    )
    assert risk.consecutive_scheduled_failures == 1
    assert risk.ignored_non_scheduled == 3


def test_a_successful_api_retry_does_not_reset_the_counter() -> None:
    """The dangerous direction: assuming a retry cleared the risk.

    Three scheduled failures, then the agent's own successful retry. The
    schedule is still one scheduled failure away from being switched off.
    """
    risk = assess_deactivation_risk(
        [api("Completed"), sched("Failed"), sched("Failed"), sched("Failed")]
    )
    assert risk.consecutive_scheduled_failures == 3
    assert risk.at_risk is True


def test_interleaved_api_runs_are_skipped_not_counted() -> None:
    risk = assess_deactivation_risk(
        [sched("Failed"), api("Completed"), sched("Failed"), api("Failed"), sched("Failed")]
    )
    assert risk.consecutive_scheduled_failures == 3
    assert risk.ignored_non_scheduled == 2


def test_refresh_type_matching_is_case_insensitive() -> None:
    risk = assess_deactivation_risk(
        [{"status": "failed", "refreshType": "scheduled"}]
    )
    assert risk.consecutive_scheduled_failures == 1


def test_unknown_refresh_type_is_not_counted() -> None:
    """Fail safe: only something explicitly scheduled advances the counter."""
    risk = assess_deactivation_risk([{"status": "Failed"}, {"status": "Failed"}])
    assert risk.consecutive_scheduled_failures == 0
    assert risk.ignored_non_scheduled == 2


# ---------------------------------------------------------------------------
# Risk thresholds
# ---------------------------------------------------------------------------


def test_at_risk_fires_one_before_the_threshold() -> None:
    """Three consecutive scheduled failures means the next one switches it off."""
    risk = assess_deactivation_risk([sched("Failed")] * 3)
    assert risk.at_risk is True
    assert risk.remaining_before_deactivation == 1
    assert "NEXT scheduled failure deactivates" in risk.headline()


def test_two_failures_is_not_yet_at_risk() -> None:
    risk = assess_deactivation_risk([sched("Failed")] * 2)
    assert risk.at_risk is False
    assert risk.remaining_before_deactivation == 2


def test_remaining_never_goes_negative() -> None:
    risk = assess_deactivation_risk([sched("Failed")] * 9)
    assert risk.remaining_before_deactivation == 0
    assert risk.at_risk is True


def test_threshold_matches_documented_behaviour() -> None:
    assert DEACTIVATION_THRESHOLD == 4


def test_evidence_explains_what_does_not_count() -> None:
    """The agent should not have to infer the trigger-path rule."""
    evidence = assess_deactivation_risk([sched("Failed")]).as_evidence()
    assert "API-triggered" in evidence["note"]
    assert "neither advance nor reset" in evidence["note"]


# ---------------------------------------------------------------------------
# Wired through the tool
# ---------------------------------------------------------------------------


async def test_the_repeating_scenario_reports_the_schedule_as_at_risk(
    repo_root, runner
) -> None:
    """Scenario 5 sits at exactly three scheduled failures, by design."""
    from triage.runner import Scenario

    scenario = Scenario.load(repo_root / "scenarios" / "scenario5-approval-granted.yaml")
    risk = assess_deactivation_risk(scenario.refresh_history)

    assert risk.consecutive_scheduled_failures == 3
    assert risk.at_risk is True


async def test_the_transient_scenario_is_not_at_risk(repo_root) -> None:
    from triage.runner import Scenario

    scenario = Scenario.load(repo_root / "scenarios" / "scenario1-transient.yaml")
    risk = assess_deactivation_risk(scenario.refresh_history)

    assert risk.consecutive_scheduled_failures == 1
    assert risk.at_risk is False
