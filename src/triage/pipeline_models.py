"""Typed evidence and operator configuration for Fabric pipeline triage."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

PIPELINE_JOB_TYPES = frozenset({"Pipeline", "Execute"})
PIPELINE_TERMINAL_STATUSES = frozenset({"Completed", "Failed", "Cancelled", "Deduped"})
PIPELINE_JOB_STATUSES = PIPELINE_TERMINAL_STATUSES | {"NotStarted", "InProgress"}


def canonical_id(value: str) -> str:
    parsed = UUID(value)
    if parsed.int == 0:
        raise ValueError("A Fabric identifier must not be the empty GUID")
    return str(parsed)


class PipelineTarget(BaseModel):
    """An explicit monitoring allowlist entry, never supplied by the model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=200)
    workspace_id: str
    pipeline_id: str
    rerun_safe: bool = Field(default=False, strict=True)
    # None means replay parameters have not been reviewed. {} explicitly means
    # no overrides; it is not interchangeable with an unknown parameter set.
    rerun_parameters: dict[str, JsonValue] | None = None

    @field_validator("workspace_id", "pipeline_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return canonical_id(value)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Pipeline target name must not be blank")
        return value.strip()

    @field_validator("rerun_parameters")
    @classmethod
    def validate_parameters(cls, values):
        if values is None:
            return None
        names = [name.casefold() for name in values]
        if len(set(names)) != len(names):
            raise ValueError("Replay parameter names must be unique without regard to case")
        for name in values:
            if not name.strip() or len(name) > 256:
                raise ValueError("Replay parameter names must contain 1-256 characters")
        json.dumps(values, allow_nan=False)
        return values

    @property
    def key(self) -> str:
        return f"{self.workspace_id}:{self.pipeline_id}"

    @property
    def permits_rerun(self) -> bool:
        return self.rerun_safe and self.rerun_parameters is not None

    @property
    def parameter_hash(self) -> str:
        raw = json.dumps(self.rerun_parameters, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()


class PipelineRun(BaseModel):
    """Run facts returned by Fabric, not an agent's interpretation of an error."""

    model_config = ConfigDict(frozen=True)

    id: str
    item_id: str
    status: str
    job_type: str
    invoke_type: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    error_code: str = ""
    failure_reason: str = ""
    retry_after_seconds: int = Field(default=0, ge=0)

    @field_validator("id", "item_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return canonical_id(value)

    @field_validator("start_time", "end_time")
    @classmethod
    def utc_time(cls, value: datetime | None) -> datetime | None:
        # Fabric names these fields startTimeUtc/endTimeUtc, including in
        # responses that omit the explicit offset.
        return value.replace(tzinfo=UTC) if value and value.tzinfo is None else value

    @property
    def failed_scheduled(self) -> bool:
        return (
            self.status == "Failed"
            and self.invoke_type == "Scheduled"
            and self.job_type in PIPELINE_JOB_TYPES
        )

    def error_text(self) -> str:
        return f"{self.error_code}: {self.failure_reason}".strip(": ")


class PipelineActivity(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(max_length=256)
    activity_type: str = ""
    status: str
    error_code: str = ""
    message: str = ""


class PipelineFailure(BaseModel):
    model_config = ConfigDict(frozen=True)

    target: PipelineTarget
    run: PipelineRun
    recent_runs: list[PipelineRun] = Field(default_factory=list)
    activities: list[PipelineActivity] = Field(default_factory=list)
    diagnostics_error: str = ""

    @model_validator(mode="after")
    def validate_failure(self) -> PipelineFailure:
        if self.run.item_id != self.target.pipeline_id:
            raise ValueError("Pipeline run belongs to a different configured item")
        if not self.run.failed_scheduled or self.run.end_time is None:
            raise ValueError("Only completed failed scheduled Pipeline jobs may be triaged")
        if self.run.start_time is not None and self.run.end_time < self.run.start_time:
            raise ValueError("Pipeline run ends before it starts")
        if any(run.item_id != self.target.pipeline_id for run in self.recent_runs):
            raise ValueError("Pipeline history contains a different item")
        return self

    @property
    def key(self) -> str:
        return f"{self.target.key}:{self.run.id}"

    def evidence(self) -> dict[str, Any]:
        # Parameters may carry business-sensitive values. The agent needs the
        # reviewed replay verdict and fingerprint, not those values.
        recent = sorted(
            self.recent_runs,
            key=lambda run: run.start_time or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        activities = sorted(
            self.activities, key=lambda activity: (activity.status != "Failed", activity.name),
        )
        return {
            "workspace_id": self.target.workspace_id,
            "pipeline_id": self.target.pipeline_id,
            "pipeline_name": self.target.name,
            "run": self.run.model_dump(mode="json"),
            "recent_runs": [run.model_dump(mode="json") for run in recent[:10]],
            "recent_run_count": len(recent),
            "activities": [activity.model_dump() for activity in activities[:30]],
            "activity_count": len(activities),
            "evidence_truncated": len(recent) > 10 or len(activities) > 30,
            "diagnostics_error": self.diagnostics_error,
            "rerun_safe": self.target.permits_rerun,
            "parameter_hash": self.target.parameter_hash,
            "portal_url": (
                "https://app.fabric.microsoft.com/groups/"
                f"{self.target.workspace_id}/pipelines/{self.target.pipeline_id}"
            ),
        }

    def error_text(self) -> str:
        activity_errors = [
            f"{activity.name} {activity.error_code}: {activity.message}"
            for activity in sorted(self.activities, key=lambda value: value.name)
            if activity.status == "Failed"
        ]
        return "\n".join([*activity_errors, self.run.error_text()]).strip()


RerunState = Literal["reserved", "submitted", "completed", "failed", "unknown"]


class PipelineRerunRecord(BaseModel):
    """A submission fence plus enough metadata to follow the new run later."""

    model_config = ConfigDict(frozen=True)

    workspace_id: str
    pipeline_id: str
    failed_run_id: str
    signature: str
    parameter_hash: str
    state: RerunState = "reserved"
    rerun_id: str = ""
    detail: str = ""
    next_poll_at: datetime | None = None
    updated_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())

    @field_validator("workspace_id", "pipeline_id", "failed_run_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return canonical_id(value)

    @field_validator("rerun_id")
    @classmethod
    def validate_optional_id(cls, value: str) -> str:
        return canonical_id(value) if value else ""

    @property
    def key(self) -> str:
        return f"{self.workspace_id}:{self.pipeline_id}:{self.failed_run_id}"


class PipelineRerunOutcome(BaseModel):
    status: str
    run_id: str = ""
    detail: str = ""

    @property
    def succeeded(self) -> bool:
        # HTTP acceptance is not execution success.
        return self.status == "Completed"


class PipelineSubmission(BaseModel):
    run_id: str
    retry_after_seconds: int = Field(default=0, ge=0)

    @field_validator("run_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return canonical_id(value)


def load_pipeline_targets(raw: str) -> list[PipelineTarget]:
    if not raw.strip():
        return []
    values = json.loads(raw)
    if not isinstance(values, list):
        raise ValueError("FABRIC_PIPELINE_TARGETS must be a JSON list")
    if len(values) > 50:
        raise ValueError("Configure at most 50 pipeline targets per controller")
    targets = [PipelineTarget.model_validate(value) for value in values]
    if len({target.key for target in targets}) != len(targets):
        raise ValueError("Duplicate workspace/pipeline target")
    return targets
