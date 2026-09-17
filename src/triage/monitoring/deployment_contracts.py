"""Operator-only deployment registration contracts; never runtime authority claims."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from triage.monitoring import models as m

CAPTURE_OPERATION = "operator.deployment_capture"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def fingerprint(value: Any, *, domain: str | None = None) -> str:
    data = {"domain": domain, "value": value} if domain else value
    return hashlib.sha256(canonical_json(data).encode("utf-8")).hexdigest()


class DeploymentError(RuntimeError):
    """Bounded operator error; never include tokens, settings payloads or SQL data."""


class DeploymentConflict(DeploymentError):
    pass


class DeploymentUncertain(DeploymentError):
    pass


def canonical_id(value: Any) -> str:
    if not isinstance(value, str):
        raise DeploymentError("Source identity is missing or is not canonical text")
    try:
        return m.canonical_id(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise DeploymentError("Source identity is malformed") from exc


class OperatorModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class ResetTarget(OperatorModel):
    server: str = Field(min_length=3, max_length=253)
    database: str = Field(min_length=1, max_length=128)
    tenant_id: m.CanonicalId
    deployer_object_id: m.CanonicalId

    @field_validator("server")
    @classmethod
    def server_name(cls, value: str) -> str:
        if len(value.split(".")) < 2 or any(
            not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", part)
            for part in value.split(".")
        ):
            raise ValueError("Use a DNS server name, not a URI or connection string")
        return value.lower()

    @field_validator("database")
    @classmethod
    def database_name(cls, value: str) -> str:
        if value != value.strip() or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_. -]{0,127}", value):
            raise ValueError("Database names cannot contain connection-string/control characters")
        return value

    @property
    def database_binding(self) -> tuple[str, str, str]:
        return self.tenant_id, self.server, self.database


class WriterSpec(OperatorModel):
    writer_id: m.OpaqueId
    kind: Literal["app_service", "logic_app", "container_app", "container_app_job", "foundry_agent"]
    resource_id: str | None = Field(default=None, max_length=1_024)
    project_endpoint: str | None = Field(default=None, max_length=512)
    agent_name: str | None = Field(default=None, max_length=63)

    @model_validator(mode="after")
    def validate_source(self) -> WriterSpec:
        if self.kind == "foundry_agent":
            endpoint = urlsplit(self.project_endpoint or "")
            if (
                self.resource_id is not None or endpoint.scheme != "https"
                or not (endpoint.hostname or "").endswith(".services.ai.azure.com")
                or endpoint.port is not None or endpoint.username is not None
                or endpoint.query or endpoint.fragment
                or not re.fullmatch(r"/api/projects/[A-Za-z0-9_.-]+", endpoint.path)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,63}", self.agent_name or "")
            ):
                raise ValueError("Foundry observation requires an exact project endpoint and agent name")
        else:
            types = {
                "app_service": r"Microsoft\.Web/sites/[A-Za-z0-9_.()-]+(?:/slots/[A-Za-z0-9_.()-]+)?",
                "logic_app": r"Microsoft\.Logic/workflows/[A-Za-z0-9_.()-]+",
                "container_app": r"Microsoft\.App/containerApps/[A-Za-z0-9_.()-]+",
                "container_app_job": r"Microsoft\.App/jobs/[A-Za-z0-9_.()-]+",
            }
            prefix = r"/subscriptions/[0-9a-fA-F-]{36}/resourceGroups/[A-Za-z0-9_.()-]+/providers/"
            if (
                self.project_endpoint is not None or self.agent_name is not None
                or re.fullmatch(prefix + types[self.kind], self.resource_id or "", re.I) is None
            ):
                raise ValueError("Writer must identify one supported ARM resource")
        return self

    @property
    def locator(self) -> str:
        return self.resource_id or f"{self.project_endpoint}/agents/{self.agent_name}"


class ActionTarget(OperatorModel):
    workload: Literal["fabric_pipeline", "powerbi"]
    workspace_id: m.CanonicalId
    item_id: m.CanonicalId

    @property
    def key(self) -> str:
        return f"{self.workload}:{self.workspace_id}:{self.item_id}"


class WriterBinding(OperatorModel):
    writer: WriterSpec
    identity_client_id: m.CanonicalId
    identity_object_id: m.CanonicalId
    sql_principal_id: int | None = Field(default=None, ge=1, strict=True)
    invokes_writer_id: m.OpaqueId | None = None

    @model_validator(mode="after")
    def validate_binding(self) -> WriterBinding:
        if (self.sql_principal_id is None) == (self.invokes_writer_id is None):
            raise ValueError("A writer binds to one SQL principal or one registered downstream writer")
        return self


class CapturedWriter(OperatorModel):
    writer: WriterSpec
    identity_client_id: m.CanonicalId
    identity_object_id: m.CanonicalId
    expected_sql_sid: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    invokes_writer_id: m.OpaqueId | None = None
    configured_sql_server: str | None = None
    configured_sql_database: str | None = None
    resource_binding_hash: m.Fingerprint
    invocation_binding_hash: m.Fingerprint | None = None
    observed_at: m.UtcDateTime
    state: Literal["stopped", "disabled", "inactive", "running", "unknown"]
    surfaces: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_direct(self) -> CapturedWriter:
        if (self.expected_sql_sid is None) == (self.invokes_writer_id is None):
            raise ValueError("Captured writer needs exact direct or indirect authority")
        if self.expected_sql_sid is not None:
            if self.expected_sql_sid != UUID(self.identity_client_id).bytes_le.hex():
                raise ValueError("SQL SID must be the client ID in GUID little-endian bytes")
            if not self.configured_sql_server or not self.configured_sql_database:
                raise ValueError("Direct SQL writer needs actual safe SQL settings")
        elif self.invocation_binding_hash is None:
            raise ValueError("Indirect writer needs an observed invocation binding")
        return self

    @property
    def registration_row(self) -> dict[str, Any]:
        return {
            "writer_id": self.writer.writer_id, "writer_kind": self.writer.kind,
            "resource_id": self.writer.resource_id, "project_endpoint": self.writer.project_endpoint,
            "agent_name": self.writer.agent_name, "identity_client_id": self.identity_client_id,
            "identity_object_id": self.identity_object_id, "expected_sql_sid": self.expected_sql_sid,
            "invokes_writer_id": self.invokes_writer_id,
            "configured_sql_server": self.configured_sql_server,
            "configured_sql_database": self.configured_sql_database,
            "resource_binding_hash": self.resource_binding_hash,
            "invocation_binding_hash": self.invocation_binding_hash,
        }


class EnumerationPage(OperatorModel):
    method: Literal["GET", "POST"]
    scope: str = Field(min_length=1, max_length=1_024)
    request_hash: m.Fingerprint
    response_hash: m.Fingerprint
    row_count: m.Count
    continuation_hash: m.Fingerprint | None = None
    observed_at: m.UtcDateTime


class DiscoveryCapture(OperatorModel):
    capture_id: m.OpaqueId
    tenant_id: m.CanonicalId
    sql_server: str
    sql_database: str
    operator_object_id: m.CanonicalId
    started_at: m.UtcDateTime
    finished_at: m.UtcDateTime
    tenant_root: str
    subscriptions: tuple[m.CanonicalId, ...]
    pages: tuple[EnumerationPage, ...]
    writers: tuple[CapturedWriter, ...] = Field(max_length=100)
    sql_identity_sids: tuple[str, ...]
    gaps: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_capture(self) -> DiscoveryCapture:
        if self.finished_at < self.started_at:
            raise ValueError("Capture finish precedes start")
        if self.tenant_root != f"/providers/Microsoft.Management/managementGroups/{self.tenant_id}":
            raise ValueError("Discovery closure must start at the actual tenant root")
        if len(set(self.subscriptions)) != len(self.subscriptions):
            raise ValueError("Subscription discovery contains duplicates")
        if len({writer.writer.writer_id for writer in self.writers}) != len(self.writers):
            raise ValueError("Writer discovery contains duplicate identities")
        by_id = {writer.writer.writer_id: writer for writer in self.writers}
        for writer in self.writers:
            seen = set()
            current = writer
            while current.invokes_writer_id is not None:
                if current.writer.writer_id in seen or current.invokes_writer_id not in by_id:
                    raise ValueError("Invocation closure is cyclic or dangling")
                seen.add(current.writer.writer_id)
                current = by_id[current.invokes_writer_id]
        return self

    @property
    def capture_hash(self) -> str:
        return fingerprint(self.model_dump(mode="json"), domain="deployment.discovery.capture.v1")

    @property
    def rows_hash(self) -> str:
        return fingerprint(
            [writer.registration_row for writer in sorted(self.writers, key=lambda item: item.writer.writer_id)],
            domain="deployment.writer.rows.v1",
        )

    @property
    def binding_hash(self) -> str:
        return fingerprint({
            "tenant_root": self.tenant_root, "subscriptions": self.subscriptions,
            "writer_rows_hash": self.rows_hash, "sql_identity_sids": self.sql_identity_sids,
        }, domain="deployment.discovery.binding.v1")


class DeploymentWriterInventory(OperatorModel):
    binding_id: m.OpaqueId
    revision: int = Field(ge=1, strict=True)
    target: ResetTarget
    server_identity: str
    database_id: int = Field(ge=1, strict=True)
    catalogue_hash: m.Fingerprint
    observed_at: m.UtcDateTime
    writers: tuple[WriterBinding, ...] = Field(min_length=1, max_length=100)
    kernel_contract_hash: m.Fingerprint
    authority_snapshot_hash: m.Fingerprint
    registration_request_id: m.CanonicalId
    discovery_capture_hash: m.Fingerprint
    current_binding_hash: m.Fingerprint
    resource_observed_at: m.UtcDateTime

    @model_validator(mode="after")
    def validate_writers(self) -> DeploymentWriterInventory:
        by_id = {binding.writer.writer_id: binding for binding in self.writers}
        if len(by_id) != len(self.writers):
            raise ValueError("Deployment writer identities must be distinct")
        for binding in self.writers:
            seen = set()
            current = binding
            while current.invokes_writer_id is not None:
                if current.writer.writer_id in seen or current.invokes_writer_id not in by_id:
                    raise ValueError("Deployment invocation bindings are cyclic or dangling")
                seen.add(current.writer.writer_id)
                current = by_id[current.invokes_writer_id]
        return self

    @property
    def state_hash(self) -> str:
        return fingerprint(self.model_dump(mode="json", exclude={"observed_at", "resource_observed_at"}))
