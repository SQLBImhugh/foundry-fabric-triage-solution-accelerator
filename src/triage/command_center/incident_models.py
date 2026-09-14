"""Incident collaboration HTTP contracts; callers cannot supply an actor."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from triage.store.incident_workflow import (
    Hash,
    IncidentActivity,
    IncidentTracking,
    ObserverMode,
)


class _Input(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str

    @field_validator("idempotency_key")
    @classmethod
    def uuid_key(cls, value: str) -> str:
        return str(UUID(value))

    @field_validator("body", "reason", "question", check_fields=False)
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Enter nonblank text")
        return value.strip()


class IncidentNoteInput(_Input):
    body: str = Field(min_length=1, max_length=4000)


class IncidentResolutionInput(_Input):
    reason: str = Field(min_length=1, max_length=4000)
    expected_version: int = Field(ge=0, lt=9_007_199_254_740_991, strict=True)
    source_revision: Hash


class IncidentDiscussionInput(_Input):
    question: str = Field(min_length=1, max_length=2000)


class IncidentCapabilities(BaseModel):
    note: bool
    resolve: bool
    ask: bool


class IncidentCase(BaseModel):
    detail: dict[str, Any]
    tracking: IncidentTracking
    activity: list[IncidentActivity]
    capabilities: IncidentCapabilities


class IncidentList(BaseModel):
    items: list[dict[str, Any]]
    total: int
    offset: int
    limit: int


class ObserverReply(BaseModel):
    """Required subset of the existing tool-free CommandCenterService.ask reply."""

    model_config = ConfigDict(extra="ignore")

    answer: str = Field(min_length=1)
    mode: ObserverMode
    question_id: str

    @field_validator("answer")
    @classmethod
    def nonblank_answer(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("The observer returned no answer")
        return value

    @field_validator("question_id")
    @classmethod
    def run_id(cls, value: str) -> str:
        return str(UUID(value))
