"""Read-only effective-token access contracts, not current directory membership."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from triage.command_center.models import APP_ROLES, Actor, AppRole


def directory_id(value: str) -> str:
    identity = UUID(value)
    if identity.int == 0:
        raise ValueError("A nonzero UUID is required")
    return str(identity)


def effective_roles(value: list[AppRole]) -> list[AppRole]:
    return sorted(role for role in APP_ROLES if role in value or (role == "reader" and value))


class AccessModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class VerifiedEntraIdentity(Actor):
    """Validated JWT context; never constructed from request fields or SQL grants."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    tenant_id: str
    application_id: str
    token_issued_at: AwareDatetime
    token_expires_at: AwareDatetime
    display_name: str = Field(min_length=1, max_length=200)
    roles: list[AppRole] = Field(min_length=1, max_length=4)

    _ids = field_validator("id", "tenant_id", "application_id")(directory_id)
    _roles = field_validator("roles")(effective_roles)


class AccessRoleDefinition(AccessModel):
    id: AppRole
    label: str
    description: str


ROLE_CATALOG = (
    AccessRoleDefinition(id="reader", label="Reader", description="View records and ask read-only questions."),
    AccessRoleDefinition(id="operator", label="Operator", description="Reader access, investigations, incident notes and user resolution of incident tracking."),
    AccessRoleDefinition(id="approver", label="Approver", description="Reader access and individual approval or denial decisions."),
    AccessRoleDefinition(id="admin", label="Admin", description="All app permissions, scenario validation and reconciliation; no directory administration."),
)


class AccessCurrentUser(AccessModel):
    id: str
    display_name: str
    roles: list[AppRole]

    _id = field_validator("id")(directory_id)


class AccessResponse(AccessModel):
    source: Literal["entra_app_roles", "synthetic_demo"]
    current_user: AccessCurrentUser
    role_catalog: list[AccessRoleDefinition] = Field(default_factory=lambda: list(ROLE_CATALOG))
    tenant_id: str | None
    application_id: str | None
    token_issued_at: AwareDatetime | None
    token_expires_at: AwareDatetime | None
    management_url: Literal["https://entra.microsoft.com/"] = "https://entra.microsoft.com/"
