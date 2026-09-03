from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from triage.models import BIRequest, TriageResult  # noqa: E402
from triage.runner import TriageRunner  # noqa: E402
from triage.settings import Settings  # noqa: E402
from triage.store.incidents import InMemoryIncidentStore  # noqa: E402


@pytest.fixture
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture
def test_settings() -> Settings:
    """Always offline. A test that reaches the network is a flaky test."""
    return Settings(
        triage_provider_mode="mock",
        triage_tool_mode="mock",
        foundry_project_endpoint="",
        azure_openai_endpoint="",
        graph_tenant_id="",
        powerbi_tenant_id="",
        teams_webhook_url="",
        applicationinsights_connection_string="",
        # Without this an operator's populated .env would silently point the
        # test suite at a live Azure table. Tests that reach the network are
        # not tests.
        incident_table_endpoint="",
    )


@pytest.fixture
def store() -> InMemoryIncidentStore:
    return InMemoryIncidentStore()


@pytest.fixture
def runner(test_settings, store, tmp_path) -> TriageRunner:
    return TriageRunner(
        test_settings,
        base_dir=REPO_ROOT,
        store=store,
        flag_table_path=tmp_path / "dq_flags.csv",
        # Isolated per test. A shared retry store leaks deferral windows
        # between scenarios, and repeated runs exhaust the attempt limit.
        retry_store_path=tmp_path / "retries.json",
        semantic_health_path=tmp_path / "semantic_health.json",
    )


@pytest.fixture
def sample_request() -> BIRequest:
    return BIRequest(
        request_id="test-0001",
        sender="no-reply-powerbi@microsoft.com",
        subject="Power BI: Refresh failed for 'Test Report'",
        body="Error code: ScheduledRefreshTimeout\nError: The operation was cancelled.",
        report_name="Test Report",
        dataset_id="6f1c9b52-8a4d-4e7f-9c31-2b5a7d0e4411",
        workspace_id="b8e42d17-3f6a-4c9b-8d21-77e4a1c9f003",
    )


def make_result(**overrides) -> TriageResult:
    base = {
        "outcome": "resolved",
        "summary": "ok",
        "request_id": "test-0001",
        "signature": "abc123def456",
        "root_cause": "Transient failure.",
        "action_taken": "refresh_powerbi_dataset",
    }
    base.update(overrides)
    return TriageResult(**base)


@pytest.fixture
def result_factory():
    """Factory fixture so tests don't depend on conftest import paths."""
    return make_result
