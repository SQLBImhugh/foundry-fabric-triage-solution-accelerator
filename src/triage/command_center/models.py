"""HTTP contracts. Authentication and executable targets are server-owned."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

AppRole = Literal["reader", "operator", "approver", "admin"]
APP_ROLES: tuple[AppRole, ...] = ("reader", "operator", "approver", "admin")


class WebSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="COMMAND_CENTER_", env_file=".env", extra="ignore",
    )

    mode: Literal["demo", "live"] = "demo"
    tenant_id: str = ""
    client_id: str = ""
    scope: str = ""
    static_dir: str = ""
    # Live commands are consumed by the hosted controller's command sweep,
    # not by detached work tied to a browser request.
    demo_worker: bool = True
    question_provider: Literal["records", "model"] = "records"
    validation_enabled: bool = False
    access_management_enabled: bool = Field(
        default=False, description="Deprecated. Authorization uses Entra app roles, not SQL grants.",
    )

    @field_validator("access_management_enabled")
    @classmethod
    def reject_retired_access_management(cls, value: bool) -> bool:
        if value:
            raise ValueError(
                "COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true is retired. "
                "Authorization uses Entra app roles; remove the setting or set it to false."
            )
        return value


class Actor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    display_name: str
    roles: list[AppRole]

    def permits(self, role: str) -> bool:
        return role in APP_ROLES and (
            "admin" in self.roles or role in self.roles or (role == "reader" and bool(self.roles))
        )


class DecisionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(min_length=1, max_length=200)
    decision: Literal["approve", "deny"]
    fingerprint: str = Field(min_length=1, max_length=200)
    reason: str = Field(default="", max_length=1000)


class AskInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(min_length=1, max_length=200)
    question: str = Field(min_length=1, max_length=2000)

    @field_validator("question")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Enter a question")
        return value.strip()


class ReconcileInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str = Field(min_length=1, max_length=1000)

    @field_validator("reason")
    @classmethod
    def require_reason(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Document how execution and target state were reconciled")
        return value.strip()


class CommandInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["powerbi_triage", "pipeline_sweep"]
    target_id: str = Field(min_length=1, max_length=250)
    subject: str = Field(default="", max_length=300)
    body: str = Field(default="", max_length=4000)
    idempotency_key: str

    @field_validator("idempotency_key")
    @classmethod
    def valid_key(cls, value: str) -> str:
        return str(UUID(value))


class ApiFailure(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
