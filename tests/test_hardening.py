"""Regression tests for issues found in the post-build rubber-duck review.

Each test here corresponds to a bug that was real, was reproduced, and was
fixed. They exist so it cannot come back quietly.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError
from test_agents import CannedProvider, _tool_response

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage_agent import TriageAgent, TriageDeps
from triage.models import Incident
from triage.policy import PolicyLedger, PolicyViolation, TriagePolicy
from triage.providers.base import LLMResponse
from triage.providers.mock import ScriptedDataQualityProvider
from triage.runner import Scenario, discover_scenarios
from triage.tools.dataset import DatasetSource
from triage.tools.flags import DataQualityFlagTable
from triage.tools.powerbi import MockPowerBIClient
from triage.tools.teams import MockTeamsNotifier, ResolutionSummary


def triage_with_dq(provider, **kwargs) -> TriageAgent:
    """A Triage agent with a real Data Quality agent attached.

    Without the second agent the handoff tool returns ``unavailable``, which
    silently changes what these tests are exercising.
    """
    return TriageAgent(
        provider, dq_agent=DataQualityAgent(ScriptedDataQualityProvider()), **kwargs
    )

# ---------------------------------------------------------------------------
# 1. Data Quality agent must not return self-contradicting text
# ---------------------------------------------------------------------------


@pytest.fixture
def dupe_datasets(repo_root) -> dict[str, DatasetSource]:
    return {
        "daily_sales": DatasetSource(
            name="daily_sales",
            path=repo_root / "mock" / "data" / "daily_sales.csv",
            key_columns=["store_id", "sales_date"],
        )
    }


@pytest.fixture
def clean_datasets(repo_root) -> dict[str, DatasetSource]:
    return {
        "daily_sales": DatasetSource(
            name="daily_sales",
            path=repo_root / "mock" / "data" / "daily_sales_clean.csv",
            key_columns=["store_id", "sales_date"],
        )
    }


async def test_contradicted_denial_discards_the_models_prose(
    sample_request, dupe_datasets
) -> None:
    """has_issue=True next to 'looks fine to me' would ship to a Teams message."""
    provider = CannedProvider(
        responses=[
            _tool_response("check_duplicates", {"table": "daily_sales"}),
            LLMResponse(
                content=json.dumps(
                    {
                        "has_issue": False,
                        "detail": "Looks fine to me.",
                        "recommended_action": "no_action",
                    }
                ),
                finish_reason="stop",
            ),
        ]
    )
    finding = await DataQualityAgent(provider).investigate(
        request=sample_request, datasets=dupe_datasets
    )

    assert finding.has_issue is True
    assert "Looks fine" not in finding.detail
    assert "4 duplicate rows" in finding.detail
    assert finding.recommended_action == "flag_and_notify"


async def test_contradicted_invention_discards_the_models_prose(
    sample_request, clean_datasets
) -> None:
    provider = CannedProvider(
        responses=[
            _tool_response("check_duplicates", {"table": "daily_sales"}),
            LLMResponse(
                content=json.dumps(
                    {
                        "has_issue": True,
                        "detail": "There are definitely duplicates here.",
                        "recommended_action": "flag_and_notify",
                    }
                ),
                finish_reason="stop",
            ),
        ]
    )
    finding = await DataQualityAgent(provider).investigate(
        request=sample_request, datasets=clean_datasets
    )

    assert finding.has_issue is False
    assert "definitely duplicates" not in finding.detail
    assert finding.recommended_action == "no_action"


# ---------------------------------------------------------------------------
# 2. Unearned terminal outcomes
# ---------------------------------------------------------------------------


@pytest.fixture
def clean_deps(tmp_path, clean_datasets) -> TriageDeps:
    return TriageDeps(
        powerbi=MockPowerBIClient(latency_ms=0),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        datasets=clean_datasets,
        signature="testsig000000001",
    )


@pytest.fixture
def dupe_deps(tmp_path, dupe_datasets) -> TriageDeps:
    return TriageDeps(
        powerbi=MockPowerBIClient(latency_ms=0),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        datasets=dupe_datasets,
        signature="testsig000000002",
    )


async def test_flagged_without_a_written_row_is_downgraded(
    sample_request, dupe_deps
) -> None:
    """The scan found duplicates, but no flag row was ever written."""
    provider = CannedProvider(
        responses=[
            _tool_response("consult_data_quality_agent", {}),
            _tool_response(
                "report_resolution",
                {"outcome": "flagged_data_quality", "summary": "Flagged.", "root_cause": "Dupes."},
            ),
        ]
    )
    result = await triage_with_dq(provider).run(sample_request, dupe_deps)

    assert result.outcome == "needs_human"
    assert "no flag row was written" in result.summary
    assert dupe_deps.flag_table.row_count == 0


async def test_suppression_without_a_matching_incident_is_downgraded(
    sample_request, clean_deps
) -> None:
    """Claiming 'this is a known duplicate' with nothing to point at."""
    provider = CannedProvider(
        responses=[
            _tool_response(
                "report_resolution",
                {
                    "outcome": "duplicate_suppressed",
                    "summary": "Already tracked.",
                    "root_cause": "Known.",
                },
            )
        ]
    )
    result = await triage_with_dq(provider).run(sample_request, clean_deps)

    assert result.outcome == "needs_human"
    assert "no open incident matched" in result.summary


async def test_zero_duplicate_flag_is_refused(sample_request, clean_deps) -> None:
    """A flag row asserting zero duplicates poisons the table someone triages."""
    provider = CannedProvider(
        responses=[
            _tool_response("consult_data_quality_agent", {}),
            _tool_response("write_data_quality_flag", {"detail": "write it anyway"}),
            _tool_response(
                "report_resolution",
                {"outcome": "needs_human", "summary": "s", "root_cause": "r"},
            ),
        ]
    )
    await triage_with_dq(provider).run(sample_request, clean_deps)
    assert clean_deps.flag_table.row_count == 0


# ---------------------------------------------------------------------------
# 3. The ledger must cover every agent in the run
# ---------------------------------------------------------------------------


async def test_data_quality_turns_are_charged_to_the_shared_ledger(
    sample_request, dupe_deps
) -> None:
    """A budget that only covers the orchestrator is not a budget for the run."""
    provider = CannedProvider(
        responses=[
            _tool_response("consult_data_quality_agent", {}),
            _tool_response(
                "report_resolution",
                {"outcome": "needs_human", "summary": "s", "root_cause": "r"},
            ),
        ]
    )
    result = await triage_with_dq(provider).run(sample_request, dupe_deps)

    # 2 orchestrator turns + 1 data-quality turn. The DQ agent no longer runs a
    # tool loop: the orchestrator scans deterministically and passes the
    # evidence in, so interpreting it costs exactly one turn.
    assert result.llm_turns == 3, "DQ agent turns are not reaching the ledger"
    assert result.tool_calls >= 3, "DQ agent tool calls are not reaching the ledger"


async def test_a_runaway_second_agent_is_stopped_by_the_shared_budget(
    sample_request, dupe_deps
) -> None:
    provider = CannedProvider(
        responses=[_tool_response("consult_data_quality_agent", {})]
    )
    result = await triage_with_dq(provider, policy=TriagePolicy(max_llm_turns=2)).run(sample_request, dupe_deps)
    assert result.outcome == "max_turns_exceeded"


def test_attempted_actions_are_counted_separately_from_dispatched() -> None:
    """Counting only dispatched calls hides the refusals, which are the point."""
    ledger = PolicyLedger(TriagePolicy())

    ledger.charge_tool_call("get_request_context")
    with pytest.raises(PolicyViolation):
        ledger.charge_tool_call("delete_dataset")

    assert ledger.attempted_actions == 2
    assert ledger.tool_calls == 1


# ---------------------------------------------------------------------------
# 4. Incident lifecycle
# ---------------------------------------------------------------------------


def test_a_resolved_incident_stops_suppressing(store, result_factory) -> None:
    """Status must follow outcome, or a fixed issue suppresses new alerts forever."""
    store.record(result_factory(outcome="flagged_data_quality", action_taken=""))
    assert store.find_open("abc123def456") is not None

    store.record(result_factory(outcome="resolved"))

    incidents = store.list_all()
    assert len(incidents) == 1
    assert incidents[0].status == "resolved"
    assert store.find_open("abc123def456") is None, "a resolved incident still suppresses"


def test_orphan_suppression_never_creates_a_fake_open_parent(store, result_factory) -> None:
    """An orphan open row would suppress every future alert for that signature."""
    store.record(result_factory(outcome="duplicate_suppressed", action_taken=""))

    incidents = store.list_all()
    assert len(incidents) == 1
    assert incidents[0].outcome == "needs_human"
    assert incidents[0].requires_investigation


def test_a_failed_notification_is_flagged_for_investigation(store, result_factory) -> None:
    """A fix nobody was told about is still an operational failure."""
    incident = store.record(result_factory(outcome="resolved", notification_failed=True))
    assert incident.requires_investigation


def test_investigation_flag_is_sticky_across_occurrences(store, result_factory) -> None:
    store.record(result_factory(outcome="needs_human", signature="e" * 16))
    updated = store.record(result_factory(outcome="needs_human", signature="e" * 16))
    assert updated.requires_investigation
    assert updated.occurrence_count == 2


# ---------------------------------------------------------------------------
# 5. Notification delivery
# ---------------------------------------------------------------------------


class FailingNotifier:
    def __init__(self) -> None:
        self.messages: list[ResolutionSummary] = []

    async def post(self, summary: ResolutionSummary) -> dict:
        self.messages.append(summary)
        return {"status": "error", "delivered": False, "http_status": 410}


async def test_undelivered_notification_surfaces_on_the_result(
    sample_request, tmp_path, clean_datasets
) -> None:
    """Teams connector webhooks were retired; a 410 must not read as success."""
    deps = TriageDeps(
        powerbi=MockPowerBIClient(latency_ms=0),
        teams=FailingNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        datasets=clean_datasets,
        signature="testsig000000003",
    )
    provider = CannedProvider(
        responses=[
            _tool_response("refresh_powerbi_dataset", {"justification": "transient"}),
            _tool_response("notify_teams", {"title": "t", "outcome": "resolved"}),
            _tool_response(
                "report_resolution",
                {"outcome": "resolved", "summary": "Refreshed.", "root_cause": "Transient."},
            ),
        ]
    )
    result = await TriageAgent(provider).run(sample_request, deps)

    # The remediation genuinely worked, so the outcome stands - but the failure
    # to inform anyone must be visible.
    assert result.outcome == "resolved"
    assert result.notification_failed is True
    assert "not delivered" in result.summary


def test_teams_card_redacts_every_field() -> None:
    summary = ResolutionSummary(
        title="Bearer abcdefghijklmnopqrstuvwxyz0123456789",
        report_name="ghp_1234567890abcdefghijklmnopqrstuvwx",
        error="AKIAIOSFODNN7EXAMPLE",
        action_taken='client_secret="abc123def456ghi789jkl"',
        outcome="resolved",
        timestamp="2026-08-26T00:00:00Z",
    )
    blob = json.dumps(summary.to_adaptive_card()) + summary.to_markdown()

    for leaked in ("abcdefghijklmnopqrstuvwxyz0123456789", "ghp_1234567890", "AKIAIOSFODNN7EXAMPLE", "abc123def456ghi789jkl"):
        assert leaked not in blob, f"'{leaked}' reached the Teams payload"


# ---------------------------------------------------------------------------
# 6. Budget headroom
# ---------------------------------------------------------------------------

MIN_TURN_HEADROOM = 3


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
        "scenario9-pipeline-authentication",
        "scenario10-pipeline-rerun-approved",
        "scenario11-pipeline-rerun-denied",
        "scenario12-pipeline-schema-mismatch",
        "scenario13-pipeline-rerun-pending",
        "scenario14-pipeline-write-timeout",
    ],
)
async def test_scenarios_keep_turn_headroom(scenario_name, repo_root, runner) -> None:
    """A scenario sitting at the cap fails for a reason unrelated to its point.

    Before this was fixed, scenarios 3 and 4 used exactly 8 of 8 turns: one
    extra recovery step would have produced max_turns_exceeded instead of the
    behaviour being demonstrated.
    """
    scenario = Scenario.load(repo_root / "scenarios" / f"{scenario_name}.yaml")
    artifacts = await runner.run_scenario(scenario)
    cap = TriagePolicy.from_settings(runner.settings).max_llm_turns

    for run in artifacts:
        headroom = cap - run.result.llm_turns
        assert headroom >= MIN_TURN_HEADROOM, (
            f"{scenario_name} used {run.result.llm_turns}/{cap} turns "
            f"(headroom {headroom}, minimum {MIN_TURN_HEADROOM})"
        )


def test_default_budgets_exceed_the_worst_observed_run() -> None:
    policy = TriagePolicy()
    assert policy.max_llm_turns >= 10 + MIN_TURN_HEADROOM
    assert policy.max_tool_calls >= policy.max_llm_turns


def test_every_scenario_is_covered_by_the_headroom_test(repo_root) -> None:
    """A new scenario must not silently skip the budget check."""
    on_disk = {s.name for s in discover_scenarios(repo_root / "scenarios")}
    covered = set(
        test_scenarios_keep_turn_headroom.pytestmark[0].args[1]  # type: ignore[attr-defined]
    )
    assert on_disk == covered, f"scenarios missing headroom coverage: {on_disk - covered}"


# ---------------------------------------------------------------------------
# 7. Scenario independence
# ---------------------------------------------------------------------------


def test_scenarios_that_should_differ_have_distinct_signatures(repo_root) -> None:
    """Scenarios must not accidentally suppress each other.

    Scenarios 3 and 4 originally reused scenario 1's email. Run back to back
    with the incident queue preserved, scenario 4 matched the still-open
    incident from scenario 3 and demonstrated *suppression* instead of the
    allowlist refusal it exists to show — a silent demo-day failure that the
    per-scenario assertions could not see, because each one resets the store.
    """
    from triage.signature import compute_signature
    from triage.tools.inbox import MockInbox

    signatures: dict[str, str] = {}
    for scenario in discover_scenarios(repo_root / "scenarios"):
        if scenario.pipeline is not None:
            from triage.pipeline_models import (
                PipelineActivity,
                PipelineFailure,
                PipelineRun,
                PipelineTarget,
            )
            from triage.runner import _pipeline_signature

            failure = PipelineFailure(
                target=PipelineTarget.model_validate(scenario.pipeline["target"]),
                run=PipelineRun.model_validate(scenario.pipeline["runs"][0]),
                activities=[
                    PipelineActivity.model_validate(row)
                    for row in scenario.pipeline.get("activities", [])
                ],
            )
            signatures.setdefault(_pipeline_signature(failure), scenario.name)
            continue
        request = MockInbox.load(repo_root / scenario.email)
        sig, _ = compute_signature(
            source="powerbi_refresh_failure",
            error=request.error_text(),
            artifact_kind="dataset",
            artifact_name=request.report_name or request.dataset_id or "",
        )
        signatures.setdefault(sig, scenario.name)

    # scenario2 and scenario2b share an email on purpose - that IS the point of
    # 2b. Every other scenario must stand alone.
    independent = [
        s.name
        for s in discover_scenarios(repo_root / "scenarios")
        if s.name not in ("scenario2b-known-issue",)
    ]
    assert len(signatures) >= len(independent) - 1, (
        f"scenarios share failure signatures and will suppress each other: {signatures}"
    )


async def test_scenarios_run_back_to_back_without_cross_contamination(
    repo_root, runner
) -> None:
    """Scenarios run back to back, with the incident queue preserved throughout."""
    order = [
        "scenario1-transient",
        "scenario2-data-quality",
        "scenario3-policy-block",
        "scenario4-unknown-action",
        "scenario5-approval-granted",
        "scenario6-approval-denied",
    ]
    outcomes: dict[str, str] = {}
    for name in order:
        scenario = Scenario.load(repo_root / "scenarios" / f"{name}.yaml")
        artifacts = await runner.run_scenario(scenario, keep_incidents=True)
        outcomes[name] = artifacts[-1].result.outcome

    assert outcomes == {
        "scenario1-transient": "resolved",
        "scenario2-data-quality": "flagged_data_quality",
        "scenario3-policy-block": "needs_human",
        "scenario4-unknown-action": "needs_human",
        # 5 and 6 share an email deliberately: the same failure, two different
        # human decisions. 6 must NOT be suppressed as a duplicate of 5, because
        # 5 resolved it - a resolved incident stops suppressing.
        "scenario5-approval-granted": "resolved",
        "scenario6-approval-denied": "approval_denied",
    }, f"cross-contamination between scenarios: {outcomes}"

    # And the queue the presenter shows in section 6 still holds the refusal.
    assert any(i.requires_investigation for i in runner.store.list_all())


# ---------------------------------------------------------------------------
# 7. Incident model sanity
# ---------------------------------------------------------------------------


def test_incident_status_values_are_constrained() -> None:
    with pytest.raises(ValidationError):
        Incident(id="x", signature="s", outcome="resolved", status="banana")  # type: ignore[arg-type]
