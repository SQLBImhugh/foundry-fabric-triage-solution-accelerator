"""Regression tests for alert parsing.

Both cases here were live defects found by invoking the deployed agent with a
realistically-worded alert rather than a fixture. The mock scenarios set
``report_name`` explicitly in their JSON, so the parser was never exercised by
the suite and both bugs sat unnoticed.
"""

from __future__ import annotations

import pytest

from triage.tools.inbox import parse_hints

# The exact shape Power BI uses in its own failure notifications.
POWERBI_SUBJECT = "Power BI: Refresh failed for 'Sales Daily Summary'"


@pytest.mark.parametrize(
    "text,expected",
    [
        (POWERBI_SUBJECT, "Sales Daily Summary"),
        ("Refresh failed for \"Store Operations Overview\"", "Store Operations Overview"),
        # Curly quotes survive a copy-paste out of Outlook.
        ("Refresh failed for \u2018Product Performance Trends\u2019", "Product Performance Trends"),
        ("The dataset 'Orders Daily Rollup' could not refresh", "Orders Daily Rollup"),
        # The older keyword form must keep working.
        ("report: Finance Summary", "Finance Summary"),
        ("semantic model 'Sales Model'", "Sales Model"),
    ],
)
def test_report_name_is_extracted_from_real_alert_wording(text: str, expected: str) -> None:
    """The parser required the literal word report/dataset, which real alerts omit.

    When it returned None the model was handed an alert with no report name and
    invented one, which is worse than failing: the run looks successful and
    names the wrong report.
    """
    assert parse_hints(text, text)["report_name"] == expected


@pytest.mark.parametrize(
    "code",
    [
        "DM_GWPipeline_Gateway_SpoolFileSizeLimitExceeded",  # 47 chars
        "DM_GWPipeline_Gateway_DataSourceAccessError",
        "ScheduledRefreshTimeout",
    ],
)
def test_error_code_is_not_truncated(code: str) -> None:
    """A code capped at 40 characters was silently cut mid-word.

    Error codes feed both the incident signature and playbook matching, so a
    truncated code quietly changes the dedup identity of an incident and stops
    matching the playbook that would have explained it.
    """
    text = f"Refresh failed. Error code: {code}"
    assert parse_hints(text, text)["error_code"] == code


def test_ids_still_parse() -> None:
    text = (
        "datasetId: 6f1c9b52-8a4d-4e7f-9c31-2b5a7d0e4411 "
        "workspaceId b8e42d17-3f6a-4c9b-8d21-77e4a1c9f003"
    )
    hints = parse_hints("", text)
    assert hints["dataset_id"] == "6f1c9b52-8a4d-4e7f-9c31-2b5a7d0e4411"
    assert hints["workspace_id"] == "b8e42d17-3f6a-4c9b-8d21-77e4a1c9f003"


def test_absent_hints_are_none_not_guesses() -> None:
    hints = parse_hints("Something went wrong", "no details")
    assert hints == {
        "report_name": None,
        "dataset_id": None,
        "workspace_id": None,
        "error_code": None,
    }
