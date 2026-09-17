from __future__ import annotations

import pytest
from pydantic import ValidationError
from test_monitoring_models import (
    EPOCH,
    LATER,
    OTHER,
    REQUEST,
    REVIEW,
    RUN,
    TENANT,
    execution,
    observation,
    target,
)

from triage.monitoring import models as m
from triage.pipeline_models import PipelineActivity


def cancelled_outcome() -> m.ActionOutcomeRequest:
    submitted = execution(run_id=OTHER)
    return m.ActionOutcomeRequest(
        tenant_id=TENANT, epoch=EPOCH, request_id=REQUEST, reservation_id=REVIEW,
        expected_reservation_revision=2, action_fence=1, disposition="verified_failed",
        submitted_execution=submitted,
        observation=observation(
            execution=submitted, status="cancelled", origin="poll", authority="rest",
            invocation="manual",
        ),
        activities_complete=True, observed_at=LATER,
        detail="The exact submitted pipeline execution was cancelled.",
    )


@pytest.mark.parametrize("activity_status", [None, "Cancelled", "Succeeded"])
def test_complete_cancelled_pipeline_is_verified_non_success(activity_status):
    fields = cancelled_outcome().model_dump()
    if activity_status is not None:
        fields["activities"] = [
            PipelineActivity(name="Owned wait", activity_type="Wait", status=activity_status),
        ]
    result = m.ActionOutcomeRequest.model_validate(fields)
    assert result.disposition == "verified_failed"
    assert result.observation.status == "cancelled"
    assert result.observation.execution == result.submitted_execution


@pytest.mark.parametrize("change", [
    "success", "incomplete_activities", "missing_end", "truncated",
    "transport_only", "different_execution", "different_target", "running",
])
def test_cancelled_pipeline_does_not_relax_exact_terminal_evidence(change):
    fields = cancelled_outcome().model_dump()
    if change == "success":
        fields["disposition"] = "verified_succeeded"
    elif change == "incomplete_activities":
        fields["activities_complete"] = False
    elif change == "missing_end":
        fields["observation"]["ended_at"] = None
    elif change == "truncated":
        fields["observation"]["evidence_truncated"] = True
    elif change == "transport_only":
        fields["observation"].update(authority="transport", origin="event")
    elif change == "different_execution":
        fields["observation"]["execution"]["run_id"] = RUN
    elif change == "different_target":
        fields["observation"]["execution"]["target"]["item_id"] = OTHER
    elif change == "running":
        fields["observation"].update(status="running", ended_at=None)
    with pytest.raises(ValidationError):
        m.ActionOutcomeRequest.model_validate(fields)


def test_pipeline_cancellation_rule_does_not_infer_powerbi_cancellation_support():
    fields = cancelled_outcome().model_dump()
    submitted = execution(target=target(workload="powerbi"), run_id_kind="powerbi_request", run_id=OTHER)
    fields["submitted_execution"] = submitted.model_dump()
    fields["observation"].update(
        execution=submitted.model_dump(), job_type="Refresh",
    )
    with pytest.raises(ValidationError, match="authoritative run evidence"):
        m.ActionOutcomeRequest.model_validate(fields)
