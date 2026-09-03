"""End-to-end scenario runs.

These are the demo. If they fail, the demo fails — so they run offline, in
CI, with no tenant, on every commit.
"""

from __future__ import annotations

import pytest

from triage.runner import Scenario, TriageRunner, check_expectations, discover_scenarios


def _scenarios(repo_root):
    return discover_scenarios(repo_root / "scenarios")


def test_every_scenario_file_parses(repo_root) -> None:
    scenarios = _scenarios(repo_root)
    assert len(scenarios) >= 5
    for scenario in scenarios:
        assert scenario.name
        assert scenario.title
        assert scenario.email
        assert scenario.expect.outcome


@pytest.mark.parametrize(
    "scenario_name",
    [
        "scenario1-transient",
        "scenario2-data-quality",
        "scenario2b-known-issue",
        "scenario3-policy-block",
        "scenario4-unknown-action",

        "scenario5-approval-granted",

        "scenario6-approval-denied",
        "scenario7-schedule-reenable",
        "scenario8-capacity-backoff",
    ],
)
async def test_scenario_meets_its_expectations(
    scenario_name: str, repo_root, runner: TriageRunner
) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / f"{scenario_name}.yaml")
    artifacts = await runner.run_scenario(scenario)

    failures = check_expectations(scenario, artifacts[-1])
    assert not failures, f"{scenario_name}: " + "; ".join(failures)


async def test_transient_scenario_notifies_and_records(repo_root, runner) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario1-transient.yaml")
    artifacts = (await runner.run_scenario(scenario))[-1]

    assert artifacts.teams_messages, "a resolution summary must always be posted"
    assert artifacts.result.action_taken == "refresh_powerbi_dataset"
    assert artifacts.incident is not None
    assert artifacts.incident.status == "resolved"


async def test_data_quality_scenario_states_the_specific_finding(repo_root, runner) -> None:
    """Their spec: notify 'Table X contains N duplicate records on key Y'."""
    scenario = Scenario.load(repo_root / "scenarios" / "scenario2-data-quality.yaml")
    artifacts = (await runner.run_scenario(scenario))[-1]

    message = artifacts.teams_messages[-1].to_markdown()
    assert "daily_sales" in message
    assert "4 duplicate rows" in message
    assert "store_id, sales_date" in message


async def test_data_quality_scenario_never_remediates(repo_root, runner) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario2-data-quality.yaml")
    artifacts = (await runner.run_scenario(scenario))[-1]

    assert not any(call[0] == "refresh_dataset" for call in artifacts.powerbi_calls)
    assert artifacts.result.write_actions == 0


async def test_data_quality_scenario_writes_exactly_one_flag(repo_root, runner) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario2-data-quality.yaml")
    artifacts = (await runner.run_scenario(scenario))[-1]

    assert artifacts.flag_rows_before == 0
    assert artifacts.flag_rows_after == 1

    row = runner.flag_table.read_all()[0]
    assert row["table_name"] == "daily_sales"
    assert row["issue_type"] == "duplicates"
    assert row["duplicate_row_count"] == "4"
    assert row["status"] == "open"


async def test_repeat_alert_is_suppressed_and_counted_once(repo_root, runner) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario2b-known-issue.yaml")
    runs = await runner.run_scenario(scenario)

    assert runs[0].result.outcome == "flagged_data_quality"
    assert runs[1].result.outcome == "duplicate_suppressed"

    incidents = runner.store.list_all()
    assert len(incidents) == 1, "a suppressed duplicate must not create a second incident"
    assert incidents[0].occurrence_count == 2

    assert runs[1].flag_rows_after == runs[1].flag_rows_before
    assert runs[1].result.tool_calls < runs[0].result.tool_calls


async def test_second_remediation_is_refused_but_the_run_still_reports(repo_root, runner) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario3-policy-block.yaml")
    artifacts = (await runner.run_scenario(scenario))[-1]

    assert artifacts.result.write_actions == 1
    assert artifacts.result.blocked_attempts == ["refresh_powerbi_dataset"]
    assert artifacts.result.outcome == "needs_human"
    assert artifacts.teams_messages, "a refused agent must still tell a human"
    assert artifacts.incident is not None
    assert artifacts.incident.requires_investigation


async def test_refused_remediation_never_reaches_the_api(repo_root, runner) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario3-policy-block.yaml")
    artifacts = (await runner.run_scenario(scenario))[-1]

    refreshes = [c for c in artifacts.powerbi_calls if c[0] == "refresh_dataset"]
    assert len(refreshes) == 1, "the blocked call must not have been dispatched"


async def test_unlisted_action_is_never_dispatched_and_the_agent_recovers(
    repo_root, runner
) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario4-unknown-action.yaml")
    artifacts = (await runner.run_scenario(scenario))[-1]

    assert artifacts.result.blocked_attempts == ["delete_dataset"]
    assert artifacts.result.outcome == "needs_human", (
        "a run containing a refused action must not read as all-clear"
    )
    # The legitimate remediation still happened; only the outcome label changed.
    assert artifacts.result.write_actions == 1
    assert not any("delete" in str(call) for call in artifacts.powerbi_calls)


async def test_runs_are_deterministic(repo_root, runner) -> None:
    """A demo you cannot rehearse is a demo you should not give."""
    scenario = Scenario.load(repo_root / "scenarios" / "scenario2-data-quality.yaml")

    first = (await runner.run_scenario(scenario))[-1].result
    second = (await runner.run_scenario(scenario))[-1].result

    assert first.outcome == second.outcome
    assert first.signature == second.signature
    assert [a.tool_name for a in first.actions] == [a.tool_name for a in second.actions]
    assert first.dq_finding.evidence.duplicate_row_count == second.dq_finding.evidence.duplicate_row_count  # type: ignore[union-attr]


async def test_provenance_is_recorded_on_every_incident(repo_root, runner) -> None:
    scenario = Scenario.load(repo_root / "scenarios" / "scenario1-transient.yaml")
    incident = (await runner.run_scenario(scenario))[-1].incident

    assert incident is not None
    assert incident.agent_name == "TriageAgent"
    assert incident.prompt_version_hash
    assert incident.model_provider == "mock"
    assert incident.app_version
