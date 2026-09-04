"""The data quality agent must inspect every table it says it inspected.

It used to scan `next(iter(datasets.values()))` -- one table -- while returning
`checked_tables=list(datasets)`. A two-table consultation therefore opened one
file and reported both as clean. A finding that names a table it never read is
worse than no finding, because the reader trusts it.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path

from triage.agents.data_quality_agent import DataQualityAgent
from triage.models import BIRequest
from triage.providers.base import LLMResponse
from triage.tools.dataset import DatasetSource


@dataclass
class CannedProvider:
    """Replays one fixed response. The model's opinion is not what is under test."""

    responses: list[LLMResponse] = field(default_factory=list)
    provider_name: str = "mock"
    model_name: str = "canned"
    calls: int = 0

    async def complete(self, *, messages, tools=None, temperature=0.1) -> LLMResponse:
        self.calls += 1
        return self.responses[min(self.calls - 1, len(self.responses) - 1)]

    async def close(self) -> None:
        return None

CLEAN = [
    ("store_id", "sales_date", "units_sold"),
    ("STORE-001", "2026-08-01", "412"),
    ("STORE-002", "2026-08-01", "388"),
]

DIRTY = [
    ("store_id", "sales_date", "units_sold"),
    ("STORE-001", "2026-08-01", "412"),
    ("STORE-001", "2026-08-01", "418"),
    ("STORE-002", "2026-08-01", "388"),
]


def _csv(path: Path, rows: list[tuple[str, ...]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(rows)
    return path


def _source(path: Path, name: str) -> DatasetSource:
    return DatasetSource(
        name=name,
        path=path,
        key_columns=["store_id", "sales_date"],
    )


def _agent() -> DataQualityAgent:
    # The model's opinion is irrelevant here: the scan is ground truth, and this
    # test is about which files were opened.
    return DataQualityAgent(
        provider=CannedProvider(
            responses=[
                LLMResponse(
                    content=(
                        '{"has_issue": false, "issue_type": "none", '
                        '"confidence": 0.5, "detail": "nothing to see", '
                        '"recommended_action": "no_action"}'
                    )
                )
            ]
        )
    )


def _request() -> BIRequest:
    return BIRequest(
        request_id="r1",
        subject="Power BI: Refresh failed for 'Sales Daily Summary'",
        body="DuplicateKeyInRelationship",
        source="mock",
    )


async def test_a_defect_in_the_second_table_is_not_hidden_by_a_clean_first(
    tmp_path: Path,
) -> None:
    """The regression. Ordered so the clean table is scanned first.

    Under the old code this returned has_issue=False while claiming both tables
    were checked, which is the worst available outcome: a confident all-clear
    covering a table nobody opened.
    """
    datasets = {
        "clean_table": _source(_csv(tmp_path / "clean.csv", CLEAN), "clean_table"),
        "dirty_table": _source(_csv(tmp_path / "dirty.csv", DIRTY), "dirty_table"),
    }

    finding = await _agent().investigate(request=_request(), datasets=datasets)

    assert finding.has_issue is True, "a duplicate in the second table was missed"
    assert finding.issue_type == "duplicates"
    assert finding.evidence is not None
    assert finding.evidence.table == "dirty_table"
    assert set(finding.checked_tables) == {"clean_table", "dirty_table"}


async def test_checked_tables_names_only_tables_that_were_really_opened(
    tmp_path: Path,
) -> None:
    """A table whose file is missing must not be reported as checked."""
    datasets = {
        "present": _source(_csv(tmp_path / "present.csv", CLEAN), "present"),
        "absent": _source(tmp_path / "nope.csv", "absent"),
    }

    finding = await _agent().investigate(request=_request(), datasets=datasets)

    assert finding.checked_tables == ["present"]
    assert "absent" not in finding.checked_tables


async def test_every_table_is_scanned_even_when_the_first_is_dirty(
    tmp_path: Path,
) -> None:
    """Collect all the evidence; let reporting priority pick what is announced.

    Stopping at the first defect would make the finding depend on dict order,
    and `checked_tables` would be lying again in the other direction.
    """
    datasets = {
        "dirty_table": _source(_csv(tmp_path / "dirty.csv", DIRTY), "dirty_table"),
        "clean_table": _source(_csv(tmp_path / "clean.csv", CLEAN), "clean_table"),
    }

    finding = await _agent().investigate(request=_request(), datasets=datasets)

    assert finding.has_issue is True
    assert set(finding.checked_tables) == {"dirty_table", "clean_table"}


async def test_no_readable_table_reports_a_scan_failure_not_a_clean_bill(
    tmp_path: Path,
) -> None:
    """Unable to check is not the same as checked and fine."""
    datasets = {"absent": _source(tmp_path / "nope.csv", "absent")}

    finding = await _agent().investigate(request=_request(), datasets=datasets)

    assert finding.has_issue is False
    assert finding.issue_type == "unknown"
    assert finding.recommended_action == "escalate"
    assert finding.checked_tables == []


def test_the_prompt_does_not_order_a_tool_call_the_agent_cannot_make() -> None:
    """The registered Foundry agent has no tools, by design.

    The prompt's first instruction was "Call `check_duplicates` on the
    registered table", so a live model would try to invoke a tool that was never
    registered on it. The controller runs the scan and hands the result over.

    Asserted on the imperative form rather than the bare tool name, because the
    corrected prompt legitimately mentions the name while telling the agent not
    to call it.
    """
    prompt = (
        Path(__file__).resolve().parents[1]
        / "src" / "triage" / "agents" / "prompts" / "data_quality_system.md"
    ).read_text(encoding="utf-8")

    for imperative in ("Call `check_duplicates`", "call `check_duplicates` on"):
        assert imperative not in prompt, (
            f"the prompt still instructs the agent to {imperative!r}, but the "
            "registered agent has no tools"
        )
    assert "You have no tools" in prompt
