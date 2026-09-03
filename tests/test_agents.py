"""Agent behaviour: evidence beats assertion, and success must be earned."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage_agent import TriageAgent, TriageDeps
from triage.models import BIRequest
from triage.policy import TriagePolicy
from triage.providers.base import LLMResponse, ToolCall
from triage.tools.dataset import DatasetSource
from triage.tools.flags import DataQualityFlagTable
from triage.tools.powerbi import MockPowerBIClient
from triage.tools.teams import MockTeamsNotifier


@dataclass
class CannedProvider:
    """Replays a fixed list of responses, one per call."""

    responses: list[LLMResponse]
    provider_name: str = "mock"
    model_name: str = "canned"
    calls: int = 0

    async def complete(self, *, messages, tools=None, temperature=0.1) -> LLMResponse:
        response = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        return response

    async def close(self) -> None:
        return None


def _tool_response(name: str, arguments: dict[str, Any]) -> LLMResponse:
    return LLMResponse(
        tool_calls=[ToolCall(id=f"c{name}", name=name, arguments=arguments)],
        finish_reason="tool_calls",
        prompt_tokens=100,
        completion_tokens=20,
    )


# ---------------------------------------------------------------------------
# Data Quality agent
# ---------------------------------------------------------------------------


@pytest.fixture
def dupe_dataset(repo_root) -> dict[str, DatasetSource]:
    return {
        "daily_sales": DatasetSource(
            name="daily_sales",
            path=repo_root / "mock" / "data" / "daily_sales.csv",
            key_columns=["store_id", "sales_date"],
        )
    }


@pytest.fixture
def clean_dataset(repo_root) -> dict[str, DatasetSource]:
    return {
        "daily_sales": DatasetSource(
            name="daily_sales",
            path=repo_root / "mock" / "data" / "daily_sales_clean.csv",
            key_columns=["store_id", "sales_date"],
        )
    }


async def test_scan_overrides_a_model_that_denies_the_duplicates(
    sample_request, dupe_dataset
) -> None:
    """An agent that can talk itself out of its own evidence is not deployable."""
    provider = CannedProvider(
        responses=[
            _tool_response("check_duplicates", {"table": "daily_sales"}),
            LLMResponse(
                content=json.dumps(
                    {"has_issue": False, "issue_type": "none", "confidence": 0.9,
                     "detail": "Looks fine to me."}
                ),
                finish_reason="stop",
            ),
        ]
    )
    finding = await DataQualityAgent(provider).investigate(
        request=sample_request, datasets=dupe_dataset
    )

    assert finding.has_issue is True
    assert finding.issue_type == "duplicates"
    assert finding.confidence == 1.0
    assert finding.evidence is not None
    assert finding.evidence.duplicate_row_count == 4


async def test_scan_overrides_a_model_that_invents_duplicates(
    sample_request, clean_dataset
) -> None:
    provider = CannedProvider(
        responses=[
            _tool_response("check_duplicates", {"table": "daily_sales"}),
            LLMResponse(
                content=json.dumps(
                    {"has_issue": True, "issue_type": "duplicates", "confidence": 0.95,
                     "detail": "I am confident there are duplicates."}
                ),
                finish_reason="stop",
            ),
        ]
    )
    finding = await DataQualityAgent(provider).investigate(
        request=sample_request, datasets=clean_dataset
    )

    assert finding.has_issue is False
    assert finding.recommended_action == "no_action"


async def test_unparseable_model_output_still_yields_a_usable_finding(
    sample_request, dupe_dataset
) -> None:
    provider = CannedProvider(
        responses=[
            _tool_response("check_duplicates", {"table": "daily_sales"}),
            LLMResponse(content="I think something is wrong but here is prose.", finish_reason="stop"),
        ]
    )
    finding = await DataQualityAgent(provider).investigate(
        request=sample_request, datasets=dupe_dataset
    )

    assert finding.has_issue is True
    assert "4 duplicate rows" in finding.detail


async def test_fenced_json_is_parsed(sample_request, dupe_dataset) -> None:
    """Models routinely wrap JSON in a code fence; that must not lose the finding."""
    provider = CannedProvider(
        responses=[
            LLMResponse(
                content='```json\n{"has_issue": true, "detail": "Four duplicates."}\n```',
                finish_reason="stop",
            ),
        ]
    )
    finding = await DataQualityAgent(provider).investigate(
        request=sample_request, datasets=dupe_dataset
    )
    assert finding.detail == "Four duplicates."


async def test_no_registered_tables_is_reported_not_guessed(sample_request) -> None:
    provider = CannedProvider(responses=[LLMResponse(content="{}", finish_reason="stop")])
    finding = await DataQualityAgent(provider).investigate(request=sample_request, datasets={})

    assert finding.issue_type == "unknown"
    assert finding.has_issue is False
    assert provider.calls == 0, "no model call should be made with nothing to inspect"


# ---------------------------------------------------------------------------
# Triage agent
# ---------------------------------------------------------------------------


@pytest.fixture
def deps(tmp_path, clean_dataset) -> TriageDeps:
    return TriageDeps(
        powerbi=MockPowerBIClient(latency_ms=0),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        datasets=clean_dataset,
        workspace_id="ws",
        dataset_id="ds",
        signature="testsig000000001",
    )


async def test_unearned_success_is_downgraded(sample_request, deps) -> None:
    """A production agent reported 'Fixed' three times while the job kept failing."""
    provider = CannedProvider(
        responses=[
            _tool_response(
                "report_resolution",
                {"outcome": "resolved", "summary": "All good.", "root_cause": "Transient."},
            )
        ]
    )
    result = await TriageAgent(provider).run(sample_request, deps)

    assert result.outcome == "needs_human"
    assert "no remediation completed" in result.summary


async def test_unsupported_data_quality_claim_is_downgraded(sample_request, deps) -> None:
    provider = CannedProvider(
        responses=[
            _tool_response(
                "report_resolution",
                {"outcome": "flagged_data_quality", "summary": "Dupes.", "root_cause": "Dupes."},
            )
        ]
    )
    result = await TriageAgent(provider).run(sample_request, deps)

    assert result.outcome == "needs_human"
    assert "deterministic scan did not find" in result.summary


async def test_earned_success_is_preserved(sample_request, deps) -> None:
    provider = CannedProvider(
        responses=[
            _tool_response("refresh_powerbi_dataset", {"justification": "transient"}),
            _tool_response(
                "report_resolution",
                {"outcome": "resolved", "summary": "Refreshed.", "root_cause": "Transient."},
            ),
        ]
    )
    result = await TriageAgent(provider).run(sample_request, deps)

    assert result.outcome == "resolved"
    assert result.write_actions == 1


async def test_a_model_that_stops_talking_is_an_escalation_not_a_success(
    sample_request, deps
) -> None:
    provider = CannedProvider(responses=[LLMResponse(content="I give up.", finish_reason="stop")])
    result = await TriageAgent(provider).run(sample_request, deps)
    assert result.outcome == "needs_human"


async def test_turn_limit_produces_a_recordable_outcome(sample_request, deps) -> None:
    provider = CannedProvider(
        responses=[_tool_response("get_request_context", {})]
    )
    result = await TriageAgent(provider, policy=TriagePolicy(max_llm_turns=2)).run(
        sample_request, deps
    )

    assert result.outcome == "max_turns_exceeded"
    assert result.llm_turns == 2


async def test_a_crashing_provider_is_recorded_with_its_exception(sample_request, deps) -> None:
    class ExplodingProvider:
        provider_name = "mock"
        model_name = "exploding"

        async def complete(self, **_: Any):
            raise RuntimeError("token endpoint returned 503")

        async def close(self) -> None:
            return None

    result = await TriageAgent(ExplodingProvider()).run(sample_request, deps)

    assert result.outcome == "agent_crashed"
    assert result.exception_class == "RuntimeError"
    assert "503" in result.exception_message


async def test_unknown_outcome_from_the_model_is_not_trusted(sample_request, deps) -> None:
    provider = CannedProvider(
        responses=[
            _tool_response(
                "report_resolution",
                {"outcome": "totally_fine", "summary": "s", "root_cause": "r"},
            )
        ]
    )
    result = await TriageAgent(provider).run(sample_request, deps)
    assert result.outcome == "needs_human"


async def test_flag_write_is_refused_without_evidence(sample_request, deps) -> None:
    """No path exists from an unsupported claim to a row in the flag table."""
    provider = CannedProvider(
        responses=[
            _tool_response("write_data_quality_flag", {"detail": "trust me"}),
            _tool_response(
                "report_resolution",
                {"outcome": "needs_human", "summary": "s", "root_cause": "r"},
            ),
        ]
    )
    await TriageAgent(provider).run(sample_request, deps)
    assert deps.flag_table.row_count == 0


async def test_events_are_emitted_for_the_ui(sample_request, deps) -> None:
    seen: list[str] = []
    provider = CannedProvider(
        responses=[
            _tool_response("get_request_context", {}),
            _tool_response(
                "report_resolution",
                {"outcome": "needs_human", "summary": "s", "root_cause": "r"},
            ),
        ]
    )
    await TriageAgent(provider, on_event=lambda e, p: seen.append(e)).run(sample_request, deps)

    assert "triage_started" in seen
    assert "tool_started" in seen
    assert "triage_finished" in seen


async def test_a_broken_event_hook_does_not_kill_the_run(sample_request, deps) -> None:
    def hook(event: str, payload: dict) -> None:
        raise ValueError("UI blew up")

    provider = CannedProvider(
        responses=[
            _tool_response(
                "report_resolution",
                {"outcome": "needs_human", "summary": "s", "root_cause": "r"},
            )
        ]
    )
    result = await TriageAgent(provider, on_event=hook).run(sample_request, deps)
    assert result.outcome == "needs_human"


def test_request_error_text_combines_subject_and_body() -> None:
    request = BIRequest(request_id="x", subject="S", body="B")
    assert request.error_text() == "S\nB"
