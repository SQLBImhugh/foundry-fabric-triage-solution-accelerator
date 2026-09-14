from __future__ import annotations

import pytest

from triage.knowledge.playbooks import Playbook, pipeline_retry_is_allowed, select_playbooks


@pytest.mark.parametrize("error,name,retry", [
    ("ADLSGen2OperationFailed: InternalServerError", "Pipeline ADLS transient service error", True),
    ("SqlOpenConnectionTimeout", "Pipeline SQL transient connection failure", True),
    ("SqlConnectionIsClosed", "Pipeline SQL transient connection failure", True),
    ("LSROBOTokenFailure", "Pipeline identity or authorization failure", False),
    ("SqlUnauthorizedAccess", "Pipeline identity or authorization failure", False),
    ("SqlDeniedPublicAccess", "Pipeline private path or gateway failure", False),
    ("BlobNotFound", "Pipeline storage object missing", False),
    ("DelimitedTextMoreColumnsThanDefined", "Pipeline delimited-text contract failure", False),
    ("SqlInvalidColumnName", "Pipeline SQL schema mismatch", False),
    ("AnalysisException", "Pipeline notebook code or resource failure", False),
    ("SqlBatchWriteTimeout", "Pipeline write effects need reconciliation", False),
    ("AzureAppendBlobConcurrentWriteConflict", "Pipeline write effects need reconciliation", False),
    ("CapacityLimitExceeded", "Pipeline throttling or capacity pressure", False),
])
def test_scoped_pipeline_playbook_and_retry_verdict(error, name, retry) -> None:
    assert name in [book.name for book in select_playbooks(error, workload="fabric_pipeline")]
    assert pipeline_retry_is_allowed(error) is retry


@pytest.mark.parametrize("error", [
    "timeout", "Failed", "SqlFailedToConnect", "SqlOperationFailed",
    "ADLSGen2OperationFailed", "ADLSGen2ForbiddenError", "ServiceUnavailable", "RestSourceCallFailed",
])
def test_ambiguous_wrapper_or_generic_word_is_not_a_retry_candidate(error) -> None:
    assert not pipeline_retry_is_allowed(error)


def test_power_bi_rules_do_not_leak_into_pipeline_triage() -> None:
    text = "scheduled refresh disabled after four consecutive failures"
    assert select_playbooks(text)
    assert select_playbooks(text, workload="fabric_pipeline") == []
    assert not pipeline_retry_is_allowed(text)


def test_blocker_wins_over_a_transient_match() -> None:
    assert not pipeline_retry_is_allowed(
        "SqlOpenConnectionTimeout followed by SqlBatchWriteTimeout"
    )


def test_custom_fail_text_does_not_gain_platform_error_authority() -> None:
    assert pipeline_retry_is_allowed("SqlConnectionIsClosed", activity_type="Copy")
    assert not pipeline_retry_is_allowed("SqlConnectionIsClosed", activity_type="Fail")


def test_policy_blocker_cannot_fall_off_the_prompt_retrieval_cap(monkeypatch) -> None:
    books = [
        Playbook(
            name=f"candidate-{index}", triggers=("hit", "shared"),
            summary="fixture", retry_useful=True, suggested_tier="tier_2",
            guidance="fixture", source="https://learn.microsoft.com/",
            workload="fabric_pipeline",
        ) for index in range(3)
    ] + [
        Playbook(
            name="blocker", triggers=("deny",), summary="fixture", retry_useful=False,
            suggested_tier="needs_human", guidance="fixture",
            source="https://learn.microsoft.com/", workload="fabric_pipeline",
        )
    ]
    monkeypatch.setattr("triage.knowledge.playbooks.PLAYBOOKS", books)
    selected = select_playbooks("hit shared deny", workload="fabric_pipeline")
    assert len(selected) == 3 and all(book.retry_useful for book in selected)
    assert not pipeline_retry_is_allowed("hit shared deny")
