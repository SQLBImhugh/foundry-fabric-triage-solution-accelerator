"""Explicit prototype reset; the default CLI operation is a read-only manifest.

This tool does not stop services, revoke/grant permissions, cancel external jobs,
or delete infrastructure. The same explicitly selected operator can prepare and
execute. Reset preflight calls a trusted live observer; saved observation files,
caller booleans, signing keys and a second operator are not authorization inputs.

Only the current declared baseline is supported. Missing/incompatible tables,
custom table mappings, unexpected accelerator objects, triggers and undeclared
foreign keys block reset execution. Use --plan-initialization, then explicitly
--initialize, to add the missing empty maintenance baseline first. Initialization
does not clear, copy or migrate existing application rows.

Operational rows, the epoch replacement and a terminal metadata-only reset
receipt commit together. Reset receipts, service budgets, deployment registration
and protected operator discovery captures survive resets.
initialize_monitoring_schema owns its transaction, so it is called AFTER that
commit to verify the identical bootstrap ID/control, never nested inside it.
An ambiguous commit is reconciled by receipt/control reads, never an automatic
second DELETE. Replaying a completed reset performs no writes, including DDL.

CLI (all target/credential arguments are explicit; environment values are ignored):
  python scripts\\reset_monitoring_state.py --server HOST --database DB \
    --tenant-id GUID --deployer-object-id GUID --credential azure-cli --subscription-id GUID \
    --allow-identity-association-preview \
    --output manifest.json
  Add --execute --manifest manifest.json --confirm-manifest-hash HASH \
    --expected-epoch GUID (and omit the original --output).

First deployment uses the same explicit target/credential flags:
  --plan-initialization --output initialize.json
  --initialize --manifest initialize.json --confirm-manifest-hash HASH --expected-uninitialized
Initialization may run while old application rows exist; it never deletes or
imports them. Install the permission kernel with its explicit deployment helper
and use register_monitoring_writers.py --ddl / --install / --prepare / --accept
before preparing a NEW reset manifest. Empty registration objects are not trusted
while old broad SQL roles remain. Stop writers, retire those roles, reconcile
effects, then prepare and accept a fresh protected registration.

RegisteredDeploymentInventoryReader is the concrete CLI default. It joins the
protected registration to actual SQL principals/permissions, then independently
re-enumerates tenant-root resource scopes, identity reuse and current child
surfaces. Unknown/expired bindings are explicit dry-run blockers. The optional
--preflight-config supplies observation selectors only; SQL target records and
the protected reader establish expected coverage. Without that file, selectors
are derived from those authoritative records, not operator recollection.
--registration-names selects a separate RegistrationNames JSON object; it never
changes the kernel table map. dbo is the supported schema.
The built-in observer reads ARM, Foundry, Fabric and Power BI with the pinned
operator credential. Unknown/unreadable state or uncorrelated effects block reset.
Active SQL work/leases must first be drained or reconciled. A known submitted
execution can instead be verified terminal by the observer's exact REST read.
Use --credential broker --operator-domain DOMAIN for an explicitly selected
broker account, or --credential managed-identity --managed-identity-client-id GUID.
No mode starts automatically from runtime imports or environment variables.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import logging
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import parse_qsl, urlsplit
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from triage.monitoring import models as m
from triage.monitoring import schema as monitoring_schema
from triage.monitoring.deployment_authority import read_authority
from triage.monitoring.deployment_contracts import (
    CAPTURE_OPERATION,
    ActionTarget,
    DeploymentError,
    DeploymentWriterInventory,
    DiscoveryCapture,
    ResetTarget,
    WriterBinding,
    WriterSpec,
)
from triage.monitoring.deployment_discovery import GRAPH_SCOPE, AzureDeploymentDiscovery
from triage.monitoring.deployment_schema import (
    DEFAULT_REGISTRATION_NAMES,
    RegistrationNames,
    kernel_contract_hash,
    native_module_hash,
    unqualified,
)
from triage.monitoring.deployment_schema import schema_statements as registration_statements
from triage.monitoring.rate_limit import DEFAULT_RATE_TABLE
from triage.monitoring.rate_limit import schema_statements as rate_schema_statements
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.store.azure_sql import (
    DEFAULT_TABLES,
    SQL_SCOPE,
    AzureSqlDatabase,
    SqlCommitUncertain,
    SqlUnavailable,
    quote_identifier,
)
from triage.store.azure_sql import schema_statements as application_schema_statements

logger = logging.getLogger("triage.monitoring.reset")
RESET_OPERATION = "prototype_reset"
BOOTSTRAP_OPERATION = "prototype_bootstrap"
ARM_SCOPE = "https://management.azure.com/.default"
FOUNDRY_SCOPE = "https://ai.azure.com/.default"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
MAX_HAZARDS = 5_000
MAX_FILE_BYTES = 2_097_152
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


class ResetError(RuntimeError):
    """A sanitized, non-successful operator disposition."""


class ResetRefused(ResetError):
    """Approval, identity, schema, ownership or live state did not match."""


class ResetCommitUncertain(ResetError):
    """Receipt/schema reconciliation did not establish a committed reset."""


class ResetVerificationFailed(ResetError):
    """Reset already committed; bootstrap verification failed. Never reset again."""

    def __init__(self, receipt: ResetReceipt) -> None:
        self.receipt = receipt
        super().__init__("Reset committed, but bootstrap verification failed; reconcile this operation, do not delete again")


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ResetRefused("SQL did not return a database timestamp")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _name(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ResetRefused("A declared SQL identifier is invalid")
    return value


class ColumnSpec(FrozenModel):
    name: str
    data_type: str
    max_length: int
    scale: int = 0
    nullable: bool
    collation: str | None = None


class ForeignKeySpec(FrozenModel):
    child: str
    child_column: str
    parent: str
    parent_column: str


class TableSpec(FrozenModel):
    logical_name: str
    name: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    operation: Literal["clear", "replace_control", "preserve_reset_receipts", "preserve_service_budget", "preserve_registration"]


class ModuleSpec(FrozenModel):
    name: str
    kind: Literal["procedure", "view", "function"]
    native_hash: m.Fingerprint


class ResetCatalogue(FrozenModel):
    tables: tuple[TableSpec, ...]
    procedures: tuple[str, ...]
    modules: tuple[ModuleSpec, ...]
    roles: tuple[str, ...]
    foreign_keys: tuple[ForeignKeySpec, ...]
    declaration_hash: m.Fingerprint
    kernel_hash: m.Fingerprint
    registration_names: RegistrationNames = DEFAULT_REGISTRATION_NAMES

    def table(self, logical: str) -> TableSpec:
        found = [table for table in self.tables if table.logical_name == logical]
        if len(found) != 1:
            raise ResetRefused("Required state is not declared by the current schema")
        return found[0]

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(table.name for table in self.tables)

    def deletion_order(self) -> tuple[TableSpec, ...]:
        pending = {table.name: table for table in self.tables if table.operation == "clear"}
        ordered = []
        while pending:
            ready = sorted(
                name for name in pending
                if not any(edge.parent == name and edge.child in pending for edge in self.foreign_keys)
            )
            if not ready:
                raise ResetRefused("Declared state has a foreign-key cycle; schema-owner integration is required")
            ordered.extend(pending.pop(name) for name in ready)
        return tuple(ordered)


def _parts(text: str) -> list[str]:
    parts, start, depth, quoted = [], 0, 0, False
    index = 0
    while index < len(text):
        char = text[index]
        if char == "'":
            if quoted and index + 1 < len(text) and text[index + 1] == "'":
                index += 2
                continue
            quoted = not quoted
        elif not quoted:
            depth += int(char == "(") - int(char == ")")
            if char == "," and depth == 0:
                parts.append(text[start:index].strip())
                start = index + 1
        index += 1
    if quoted or depth:
        raise ResetRefused("The current schema declaration needs an updated operator parser")
    return [*parts, text[start:].strip()]


def _table_body(statement: str) -> tuple[str, str] | None:
    match = re.search(r"CREATE\s+TABLE\s+\[dbo\]\.\[([A-Za-z0-9_]+)\]\s*\(", statement, re.I)
    if match is None:
        return None
    depth, quoted, index = 1, False, match.end()
    start = index
    while index < len(statement):
        char = statement[index]
        if char == "'":
            if quoted and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            quoted = not quoted
        elif not quoted:
            depth += int(char == "(") - int(char == ")")
            if depth == 0:
                return _name(match[1]), statement[start:index]
        index += 1
    raise ResetRefused("The current CREATE TABLE declaration is incomplete")


def build_catalogue(registration_names: RegistrationNames = DEFAULT_REGISTRATION_NAMES) -> ResetCatalogue:
    """Derive the exact object allowlist and layout from current schema declarations."""
    if set(DEFAULT_TABLES) != {
        "incidents", "processed", "approvals", "retries", "semantic_health", "leases", "claims",
        "inbox_audit", "pipeline_reruns", "agent_runs", "agent_events", "agent_commands", "incident_activity",
        "data_quality_flags",
    } or set(monitoring_schema.DEFAULT_MONITORING_TABLES) != {
        "monitoring_control", "monitoring_records", "monitoring_leases", "monitoring_receipts",
    }:
        raise ResetRefused("Declared state categories changed; review reset/hazard coverage before execution")
    names = DEFAULT_TABLES | monitoring_schema.DEFAULT_MONITORING_TABLES | {"rate_budget": DEFAULT_RATE_TABLE} | registration_names.tables
    if len(names.values()) != len({value.casefold() for value in names.values()}):
        raise ResetRefused("Declared state table names collide")
    for name in names.values():
        _name(name)
    statements = (
        *application_schema_statements(dict(DEFAULT_TABLES)),
        *monitoring_schema.schema_statements(),
        *rate_schema_statements(),
        *registration_statements(registration_names),
    )
    tables, modules, foreign_keys = [], [], []
    for statement in statements:
        procedure = re.search(r"CREATE(?:\s+OR\s+ALTER)?\s+(PROCEDURE|VIEW)\s+(\[dbo\]\.\[\w+\]|dbo\.\w+)", statement, re.I)
        if procedure is not None:
            modules.append(ModuleSpec(
                name=unqualified(procedure[2]), kind=procedure[1].lower(),
                native_hash=native_module_hash(statement),
            ))
        parsed = _table_body(statement)
        if parsed is None:
            continue
        name, body = parsed
        logical = next((key for key, value in names.items() if value == name), None)
        if logical is None:
            raise ResetRefused("Schema DDL contains an undeclared state table")
        columns, primary_key = [], []
        for part in _parts(body):
            edge = re.fullmatch(r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+\[dbo\]\.\[(\w+)\]\s*\(([^)]+)\)", part, re.I)
            if edge is not None:
                children, parents = edge[1].split(","), edge[3].split(",")
                if len(children) != len(parents):
                    raise ResetRefused("Declared composite foreign-key arity differs")
                foreign_keys.extend(ForeignKeySpec(
                    child=name, child_column=_name(child.strip()), parent=_name(edge[2]),
                    parent_column=_name(parent.strip()),
                ) for child, parent in zip(children, parents, strict=True))
                continue
            if re.match(r"PRIMARY\s+KEY\s*\(", part, re.I):
                primary_key.extend(_name(value.strip().strip("[]")) for value in part[part.index("(") + 1:part.rindex(")")].split(","))
                continue
            if re.match(r"(CHECK|INDEX|CONSTRAINT)\b", part, re.I):
                if re.search(r"FOREIGN\s+KEY", part, re.I):
                    raise ResetRefused("New table-level foreign keys need schema-owner operator support")
                continue
            column = re.match(
                r"(\w+)\s+(NVARCHAR|VARCHAR|CHAR|BINARY|DATETIME2|UNIQUEIDENTIFIER|BIGINT|INT|BIT)"
                r"(?:\((MAX|\d+)\))?\s+(.*)", part, re.I | re.S,
            )
            if column is None:
                raise ResetRefused("A declared SQL column type needs explicit operator support")
            column_name, sql_type, width, tail = column.groups()
            sql_type = sql_type.lower()
            scale = int(width or "7") if sql_type == "datetime2" else 0
            fixed = {"uniqueidentifier": 16, "bigint": 8, "int": 4, "bit": 1}
            if sql_type == "datetime2":
                length = 6 if scale <= 2 else 7 if scale <= 4 else 8
            elif sql_type in fixed:
                length = fixed[sql_type]
            else:
                length = -1 if (width or "").upper() == "MAX" else int(width or "1") * (2 if sql_type == "nvarchar" else 1)
            collation = re.search(r"\bCOLLATE\s+(\w+)", tail, re.I)
            columns.append(ColumnSpec(
                name=_name(column_name), data_type=sql_type, max_length=length, scale=scale,
                nullable=not bool(re.search(r"\bNOT\s+NULL\b|\bPRIMARY\s+KEY\b", tail, re.I)),
                collation=collation[1] if collation else None,
            ))
            if re.search(r"\bPRIMARY\s+KEY\b", tail, re.I):
                primary_key.append(column_name)
            reference = re.search(r"REFERENCES\s+\[dbo\]\.\[(\w+)\]\s*\((\w+)\)", tail, re.I)
            if reference is not None:
                foreign_keys.append(ForeignKeySpec(
                    child=name, child_column=column_name, parent=_name(reference[1]),
                    parent_column=_name(reference[2]),
                ))
        if not columns or not primary_key:
            raise ResetRefused("Every declared state table requires an explicit column/key layout")
        operation = {
            "monitoring_control": "replace_control",
            "monitoring_receipts": "preserve_reset_receipts",
            "rate_budget": "preserve_service_budget",
            "deployment_registration": "preserve_registration",
            "deployment_writers": "preserve_registration",
        }.get(logical, "clear")
        tables.append(TableSpec(
            logical_name=logical, name=name, columns=tuple(columns), primary_key=tuple(primary_key),
            operation=operation,
        ))
    if sorted(table.name for table in tables) != sorted(names.values()):
        raise ResetRefused("Declared tables and deployment DDL differ; no state may be skipped")
    kernel = build_permission_kernel()
    for obj in kernel.objects:
        if obj.kind != "role":
            modules.append(ModuleSpec(
                name=unqualified(obj.name), kind=obj.kind, native_hash=native_module_hash(obj.ddl),
            ))
    if len({module.name for module in modules} | set(names.values())) != len(modules) + len(names):
        raise ResetRefused("Declared operator/kernel object names collide")
    return ResetCatalogue(
        tables=tuple(sorted(tables, key=lambda value: value.name)),
        procedures=tuple(sorted(module.name for module in modules if module.kind == "procedure")),
        modules=tuple(sorted(modules, key=lambda value: value.name)),
        roles=tuple(sorted(unqualified(obj.name) for obj in kernel.objects if obj.kind == "role")),
        foreign_keys=tuple(foreign_keys),
        declaration_hash=_hash([*statements, *kernel.statements]),
        kernel_hash=kernel_contract_hash(), registration_names=registration_names,
    )


class ObjectSnapshot(FrozenModel):
    name: str
    kind: Literal["table", "procedure", "view", "function", "role"]
    object_id: int | None
    schema_hash: m.Fingerprint | None
    row_count: int | None = Field(default=None, ge=0)
    content_hash: m.Fingerprint | None = None
    retained_rows: int = Field(default=0, ge=0)
    operation: str


class Hazard(FrozenModel):
    table: str
    kind: Literal["lease", "command", "run", "action", "retry", "work"]
    key_hash: m.Fingerprint
    state: str = Field(min_length=1, max_length=40)
    correlation_hash: m.Fingerprint | None = None
    expires_at: m.UtcDateTime | None = None
    workload: Literal["fabric_pipeline", "powerbi"] | None = None
    workspace_id: m.CanonicalId | None = None
    item_id: m.CanonicalId | None = None
    run_id: m.CanonicalId | None = None

    @property
    def key(self) -> str:
        return f"{self.table}:{self.kind}:{self.key_hash}"


class ResetSnapshot(FrozenModel):
    target: ResetTarget
    server_identity: str = Field(min_length=1, max_length=256)
    database_id: int = Field(ge=1)
    inspected_at: m.UtcDateTime
    control: m.DeploymentControl | None
    bootstrap_id: m.CanonicalId | None
    bootstrap_hash: m.Fingerprint | None
    objects: tuple[ObjectSnapshot, ...]
    hazards: tuple[Hazard, ...]
    hazards_complete: bool
    required_action_targets: tuple[ActionTarget, ...] = ()
    blockers: tuple[str, ...]

    @property
    def state_hash(self) -> str:
        return _hash(self.model_dump(mode="json", exclude={"inspected_at"}))


class ResetManifest(FrozenModel):
    version: Literal[1] = 1
    operation_id: m.CanonicalId
    new_epoch: m.CanonicalId
    catalogue_hash: m.Fingerprint
    snapshot: ResetSnapshot
    purpose: Literal["reset", "initialize"] = "reset"
    preflight_profile: ObservationProfile | None = None
    deployment_inventory: DeploymentWriterInventory | None = None

    @property
    def manifest_hash(self) -> str:
        return _hash(self.model_dump(mode="json"))


class ManifestDocument(FrozenModel):
    mode: Literal["dry_run"] = "dry_run"
    manifest_hash: m.Fingerprint
    manifest: ResetManifest
    reset_execution_blockers: tuple[str, ...] = ()

    @model_validator(mode="after")
    def check_hash(self) -> ManifestDocument:
        if self.manifest_hash != self.manifest.manifest_hash:
            raise ValueError("Manifest document hash does not match its content")
        blockers = []
        if self.manifest.purpose == "reset":
            blockers.extend(self.manifest.snapshot.blockers)
            if self.manifest.deployment_inventory is None:
                blockers.append("authoritative_deployment_sql_writer_inventory_unavailable")
            if self.manifest.preflight_profile is None:
                blockers.append("live_preflight_configuration_missing")
        object.__setattr__(self, "reset_execution_blockers", tuple(sorted(set(blockers))))
        return self


class ObservationProfile(FrozenModel):
    """Requested observation selectors, never authoritative inventory or observed state."""

    version: Literal[1] = 1
    target: ResetTarget
    writers: tuple[WriterSpec, ...] = Field(min_length=1, max_length=100)
    action_targets: tuple[ActionTarget, ...] = Field(default=(), max_length=1_000)
    max_age_seconds: int = Field(default=120, ge=15, le=300, strict=True)

    @model_validator(mode="after")
    def unique_sources(self) -> ObservationProfile:
        if len({writer.writer_id for writer in self.writers}) != len(self.writers):
            raise ValueError("Live writer identities must be distinct")
        if len({target.key for target in self.action_targets}) != len(self.action_targets):
            raise ValueError("Live action target identities must be distinct")
        return self


class DeploymentInventoryReader(Protocol):
    """Schema-owner integration; called inside the current SQL transaction.

    Read the protected deployment-registration/SQL-permission projection.
    Do not construct it from ObservationProfile, a caller file or a completeness
    assertion. The standalone CLI uses RegisteredDeploymentInventoryReader.
    """

    def read(
        self, database: AzureSqlDatabase, target: ResetTarget, catalogue: ResetCatalogue,
    ) -> DeploymentWriterInventory: ...


class PreflightRequest(FrozenModel):
    challenge: m.CanonicalId
    manifest_hash: m.Fingerprint
    target: ResetTarget
    expected_epoch: m.CanonicalId
    ownership_hash: m.Fingerprint
    requested_at: m.UtcDateTime
    profile: ObservationProfile
    hazards: tuple[Hazard, ...]
    required_action_targets: tuple[ActionTarget, ...]
    deployment_inventory: DeploymentWriterInventory


class WriterObservation(FrozenModel):
    writer_id: m.OpaqueId
    source: str
    state: Literal["stopped", "disabled", "inactive"]
    observed_at: m.UtcDateTime
    response_hash: m.Fingerprint
    sql_server: str
    sql_database: str
    identity_client_id: m.CanonicalId
    identity_object_id: m.CanonicalId
    binding_hash: m.Fingerprint


class ActionObservation(FrozenModel):
    hazard_key: str
    correlation_hash: m.Fingerprint
    state: Literal["Completed", "Failed", "Cancelled", "Deduped"]
    observed_at: m.UtcDateTime
    response_hash: m.Fingerprint


class PreflightObservation(FrozenModel):
    challenge: m.CanonicalId
    manifest_hash: m.Fingerprint
    target: ResetTarget
    expected_epoch: m.CanonicalId
    ownership_hash: m.Fingerprint
    profile_hash: m.Fingerprint
    deployment_inventory_hash: m.Fingerprint
    started_at: m.UtcDateTime
    completed_at: m.UtcDateTime
    writers: tuple[WriterObservation, ...]
    actions: tuple[ActionObservation, ...]
    idle_targets: tuple[str, ...]


class PreflightObserver(Protocol):
    """Injected trusted code that performs reads now, never an observation-document loader."""

    profile: ObservationProfile

    def observe(self, request: PreflightRequest) -> PreflightObservation: ...


class LiveOperatorObserver:
    """Read-only operator implementation; never consumes saved claims of quiescence.

    App Service includes deployment slots. Logic Apps include current runs.
    Container Apps require all revisions inactive and their replicas absent.
    Container App jobs must be manual with no running execution. Foundry uses
    the agent's documented Disabled endpoint status, not idle compute or a
    version's deployment readiness:
    https://learn.microsoft.com/azure/foundry/agents/how-to/manage-hosted-agent
    https://learn.microsoft.com/azure/foundry/agents/how-to/configure-agent

    App Service settings require the ARM POST config/appsettings/list read
    operation. Other POST/PATCH/DELETE operations are not permitted.
    """

    def __init__(
        self, profile: ObservationProfile, credential: PinnedDeployerCredential, *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        max_pages: int = 100,
    ) -> None:
        if credential.target != profile.target:
            raise ResetRefused("Live reads must use the explicitly selected operator identity")
        if type(max_pages) is not int or not 1 <= max_pages <= 1_000:
            raise ValueError("Live preflight pagination must have an explicit bounded budget")
        self.profile = profile
        self.credential = credential
        self.clock = clock
        self.max_pages = max_pages
        self.http = httpx.Client(
            transport=transport or httpx.HTTPTransport(retries=0), timeout=15, follow_redirects=False,
        )
        self.resource_discovery = AzureDeploymentDiscovery(
            credential, profile.target, transport=transport, clock=clock, max_pages=max_pages,
        )

    def close(self) -> None:
        self.http.close()
        self.resource_discovery.close()

    def _get(self, url: str, scope: str, *, method: str = "GET") -> dict[str, Any]:
        parsed = urlsplit(url)
        hosts = {
            ARM_SCOPE: {"management.azure.com"}, FABRIC_SCOPE: {"api.fabric.microsoft.com"},
            POWERBI_SCOPE: {"api.powerbi.com"},
            FOUNDRY_SCOPE: {urlsplit(writer.project_endpoint).hostname for writer in self.profile.writers if writer.project_endpoint},
        }
        if (
            parsed.scheme != "https" or parsed.hostname not in hosts.get(scope, set())
            or parsed.port is not None or parsed.username is not None or parsed.fragment
        ):
            raise ResetRefused("Live preflight refused an unexpected service endpoint")
        if method != "GET":
            allowed_settings = {
                writer.resource_id for writer in self.profile.writers
                if writer.kind == "app_service" and writer.resource_id is not None
            }
            if (
                method != "POST" or scope != ARM_SCOPE
                or not any(
                    parsed.path == resource + "/config/appsettings/list"
                    or re.fullmatch(re.escape(resource) + r"/slots/[A-Za-z0-9_.()-]+/config/appsettings/list", parsed.path)
                    for resource in allowed_settings
                )
            ):
                raise ResetRefused("Only the exact ARM appsettings/list read action may use POST")
        token = self.credential.get_token(scope).token
        try:
            with self.http.stream(method, url, headers={"Authorization": f"Bearer {token}"}) as response:
                if response.status_code != 200:
                    raise ResetRefused(f"Live preflight read returned HTTP {response.status_code}; quiescence is unverified")
                body = bytearray()
                for chunk in response.iter_bytes():
                    if len(body) + len(chunk) > MAX_FILE_BYTES:
                        raise ResetRefused("Live preflight response exceeded its bounded evidence size")
                    body.extend(chunk)
            payload = json.loads(body)
        except (httpx.HTTPError, ValueError) as exc:
            raise ResetRefused("Live preflight could not read valid source evidence") from exc
        if not isinstance(payload, dict):
            raise ResetRefused("Live preflight source returned an unexpected response shape")
        for name in ("tenantId", "tenant_id"):
            if name in payload and m.canonical_id(payload[name]) != self.profile.target.tenant_id:
                raise ResetRefused("Live preflight source belongs to another tenant")
        return payload

    def _list(self, url: str, scope: str) -> list[dict[str, Any]]:
        original = urlsplit(url)
        fixed = dict(parse_qsl(original.query))
        seen, rows = set(), []
        current: str | None = url
        for _ in range(self.max_pages):
            if current in seen:
                raise ResetRefused("Live preflight pagination repeated a position")
            seen.add(current)
            payload = self._get(current, scope)
            values = payload.get("value")
            if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
                raise ResetRefused("Live preflight list is incomplete or malformed")
            rows.extend(values)
            links = [payload.get(key) for key in ("nextLink", "@odata.nextLink", "continuationUri") if payload.get(key)]
            if len(set(links)) > 1:
                raise ResetRefused("Live preflight continuation links disagree")
            token = payload.get("continuationToken")
            if token is not None and (not isinstance(token, str) or not token):
                raise ResetRefused("Live preflight continuation token is malformed")
            current = links[0] if links else (
                str(httpx.URL(url).copy_add_param("continuationToken", token)) if token else None
            )
            if current is None:
                if any(payload.get(key) for key in ("hasMore", "has_more", "nextPageToken")):
                    raise ResetRefused("Live preflight did not receive a usable continuation")
                return rows
            parsed = urlsplit(current)
            values = dict(parse_qsl(parsed.query))
            if (
                parsed.scheme != "https" or parsed.netloc != original.netloc or parsed.path != original.path
                or parsed.fragment or parsed.username is not None
                or any(values.get(key, value) != value for key, value in fixed.items())
                or not set(values) <= set(fixed) | {"continuationToken", "$skiptoken", "$skip", "skipToken"}
            ):
                raise ResetRefused("Live preflight continuation left its original resource scope")
            if token is not None and values.get("continuationToken", token) != token:
                raise ResetRefused("Live preflight continuation token and URI disagree")
        raise ResetRefused("Live preflight pagination budget was exhausted; no complete proof is available")

    def _writer(self, binding: WriterBinding) -> WriterObservation:
        try:
            result = self.resource_discovery.observe_registered(binding, self._current_bindings)
        except (DeploymentError, ValueError) as exc:
            raise ResetRefused(str(exc)) from exc
        return WriterObservation(
            writer_id=binding.writer.writer_id, source=binding.writer.locator, state=result.state,
            observed_at=result.observed_at, response_hash=result.resource_binding_hash,
            sql_server=self.profile.target.server, sql_database=self.profile.target.database,
            identity_client_id=binding.identity_client_id, identity_object_id=binding.identity_object_id,
            binding_hash=_hash(binding.model_dump(mode="json")),
        )

    def _runs(self, target: ActionTarget) -> list[dict[str, Any]]:
        if target.workload == "fabric_pipeline":
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{target.workspace_id}/items/{target.item_id}/jobs/instances"
            rows = self._list(url, FABRIC_SCOPE)
            for row in rows:
                if m.canonical_id(row.get("itemId", "")) != target.item_id or row.get("status") not in {
                    "Completed", "Failed", "Cancelled", "Deduped",
                } or not row.get("endTimeUtc"):
                    raise ResetRefused("Fabric target has an active or unverified external job")
                m.canonical_id(row.get("id", ""))
        else:
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{target.workspace_id}/datasets/{target.item_id}/refreshes?$top=60"
            rows = self._list(url, POWERBI_SCOPE)
            for row in rows:
                if row.get("status") not in {"Completed", "Failed", "Cancelled"} or not row.get("endTime"):
                    raise ResetRefused("Power BI target has an active or unverified refresh")
                m.canonical_id(row.get("requestId", ""))
        return rows

    def observe(self, request: PreflightRequest) -> PreflightObservation:
        if request.profile != self.profile or request.target != self.profile.target:
            raise ResetRefused("The live observer is not configured for this deployment/manifest")
        expected = {binding.writer.writer_id: binding for binding in request.deployment_inventory.writers}
        if {writer.writer_id: writer for writer in self.profile.writers} != {
            key: binding.writer for key, binding in expected.items()
        } or not {target.key for target in request.required_action_targets} <= {
            target.key for target in self.profile.action_targets
        }:
            raise ResetRefused("Caller profile does not cover authoritative SQL/deployment inventory")
        if any(
            hazard.kind != "action" or hazard.correlation_hash is None
            or None in (hazard.workload, hazard.workspace_id, hazard.item_id, hazard.run_id)
            for hazard in request.hazards
        ):
            raise ResetRefused("Drain active SQL work/leases and reconcile uncorrelated effects before reset")
        started = self.clock()
        self._current_bindings = request.deployment_inventory.writers
        writers = tuple(self._writer(binding) for binding in request.deployment_inventory.writers)
        targets = {target.key: target for target in (*request.required_action_targets, *self.profile.action_targets)}
        histories = {target.key: self._runs(target) for target in targets.values()}
        actions = []
        for hazard in request.hazards:
            target = ActionTarget(workload=hazard.workload, workspace_id=hazard.workspace_id, item_id=hazard.item_id)
            if target.key not in histories:
                raise ResetRefused("An outstanding action is outside the live observation target inventory")
            if target.workload == "fabric_pipeline":
                row = self._get(
                    f"https://api.fabric.microsoft.com/v1/workspaces/{target.workspace_id}/items/{target.item_id}/jobs/instances/{hazard.run_id}",
                    FABRIC_SCOPE,
                )
                if m.canonical_id(row.get("id", "")) != hazard.run_id or m.canonical_id(row.get("itemId", "")) != target.item_id:
                    raise ResetRefused("Fabric terminal evidence identifies another execution")
                end = row.get("endTimeUtc")
            else:
                matches = [row for row in histories[target.key] if m.canonical_id(row.get("requestId", "")) == hazard.run_id]
                if len(matches) != 1:
                    raise ResetRefused("The exact submitted Power BI refresh cannot be reconciled")
                row, end = matches[0], matches[0].get("endTime")
            if row.get("status") not in {"Completed", "Failed", "Cancelled", "Deduped"} or not end:
                raise ResetRefused("The submitted external effect is not terminal")
            actions.append(ActionObservation(
                hazard_key=hazard.key, correlation_hash=hashlib.sha256(hazard.run_id.encode("utf-16-le")).hexdigest(),
                state=row["status"], observed_at=self.clock(), response_hash=_hash(row),
            ))
        return PreflightObservation(
            challenge=request.challenge, manifest_hash=request.manifest_hash, target=request.target,
            expected_epoch=request.expected_epoch, ownership_hash=request.ownership_hash,
            profile_hash=_hash(self.profile.model_dump(mode="json")),
            deployment_inventory_hash=request.deployment_inventory.state_hash,
            started_at=started, completed_at=self.clock(),
            writers=writers, actions=tuple(actions), idle_targets=tuple(histories),
        )


def validate_observation(
    observation: PreflightObservation, request: PreflightRequest, now: datetime,
) -> None:
    if not isinstance(observation, PreflightObservation):
        raise ResetRefused("The live observer did not return typed evidence")
    try:
        observation = PreflightObservation.model_validate_json(observation.model_dump_json())
    except ValueError as exc:
        raise ResetRefused("Live observer evidence failed its typed state/identity contract") from exc
    if (
        observation.challenge != request.challenge
        or observation.manifest_hash != request.manifest_hash or observation.target != request.target
        or observation.expected_epoch != request.expected_epoch
        or observation.ownership_hash != request.ownership_hash
        or observation.profile_hash != _hash(request.profile.model_dump(mode="json"))
        or observation.deployment_inventory_hash != request.deployment_inventory.state_hash
    ):
        raise ResetRefused("Live observation is forged, replayed or bound to another manifest/target")
    if not request.requested_at <= observation.started_at <= observation.completed_at <= now or (
        now - observation.started_at
    ).total_seconds() > request.profile.max_age_seconds:
        raise ResetRefused("Live preflight observations are stale, premature or expired")
    writers = {writer.writer_id: writer for writer in observation.writers}
    if len(writers) != len(observation.writers) or set(writers) != {
        binding.writer.writer_id for binding in request.deployment_inventory.writers
    }:
        raise ResetRefused("Live observation omits an authoritative deployed writer")
    for binding in request.deployment_inventory.writers:
        spec = binding.writer
        source = spec.resource_id or f"{spec.project_endpoint}/agents/{spec.agent_name}"
        result = writers[spec.writer_id]
        if (
            result.source != source or result.sql_server != request.target.server
            or result.sql_database != request.target.database
            or result.identity_client_id != binding.identity_client_id
            or result.identity_object_id != binding.identity_object_id
            or result.binding_hash != _hash(binding.model_dump(mode="json"))
        ):
            raise ResetRefused("Writer observation is not bound to the actual SQL deployment/identity")
    if set(observation.idle_targets) != {
        target.key for target in (*request.required_action_targets, *request.profile.action_targets)
    }:
        raise ResetRefused("Live external-action coverage is incomplete")
    actions = {action.hazard_key: action for action in observation.actions}
    if len(actions) != len(observation.actions) or set(actions) != {hazard.key for hazard in request.hazards}:
        raise ResetRefused("Outstanding SQL work or effects have not been reconciled")
    for hazard in request.hazards:
        if hazard.kind != "action" or hazard.correlation_hash is None:
            raise ResetRefused("Drain outstanding work/leases and reconcile uncorrelated effects before reset")
        if actions[hazard.key].correlation_hash != hazard.correlation_hash:
            raise ResetRefused("External terminal evidence identifies another submitted execution")
    if any(not observation.started_at <= sample.observed_at <= now for sample in (
        *observation.writers, *observation.actions,
    )):
        raise ResetRefused("Live source evidence has a stale or future observation time")


class ResetReceipt(FrozenModel):
    version: Literal[1] = 1
    operation_id: m.CanonicalId
    manifest_hash: m.Fingerprint
    expected_old_epoch: m.CanonicalId
    target: ResetTarget
    server_identity: str = Field(min_length=1, max_length=256)
    database_id: int = Field(ge=1)
    new_control: m.DeploymentControl
    bootstrap_hash: m.Fingerprint
    preflight_challenge: m.CanonicalId
    preflight_hash: m.Fingerprint
    preflight_observed_at: m.UtcDateTime
    operator_object_id: m.CanonicalId
    deployment_inventory_hash: m.Fingerprint
    required_targets_hash: m.Fingerprint
    registration_names: RegistrationNames
    deleted_counts: dict[str, m.Count]
    retained_counts: dict[str, m.Count]
    recorded_at: m.UtcDateTime
    state: Literal["completed"] = "completed"

    @model_validator(mode="after")
    def validate_metadata(self) -> ResetReceipt:
        allowed = {
            f"dbo.{value}" for value in (
                *DEFAULT_TABLES.values(), *monitoring_schema.DEFAULT_MONITORING_TABLES.values(), DEFAULT_RATE_TABLE,
                *self.registration_names.tables.values(),
            )
        }
        if not self.deleted_counts.keys() <= allowed or not self.retained_counts.keys() <= allowed:
            raise ValueError("Reset receipt counts must name declared state objects only")
        if (
            self.new_control.tenant_id != self.target.tenant_id
            or self.new_control.epoch == self.expected_old_epoch
            or self.new_control.revision != 0 or not self.new_control.maintenance
            or self.operator_object_id != self.target.deployer_object_id
            or self.bootstrap_hash != hashlib.sha256(self.new_control.model_dump_json().encode()).hexdigest()
        ):
            raise ValueError("Reset receipt baseline/authorization metadata is inconsistent")
        return self


class ResetResult(FrozenModel):
    receipt: ResetReceipt
    replayed: bool = False
    reconciled_uncertain_commit: bool = False


class BootstrapReceipt(FrozenModel):
    version: Literal[1] = 1
    operation_id: m.CanonicalId
    manifest_hash: m.Fingerprint
    target: ResetTarget
    control: m.DeploymentControl
    bootstrap_hash: m.Fingerprint
    recorded_at: m.UtcDateTime

    @model_validator(mode="after")
    def validate_control(self) -> BootstrapReceipt:
        if (
            self.control.tenant_id != self.target.tenant_id or self.control.revision != 0
            or not self.control.maintenance
            or self.bootstrap_hash != hashlib.sha256(self.control.model_dump_json().encode()).hexdigest()
        ):
            raise ValueError("Initial maintenance baseline receipt is inconsistent")
        return self


class InitializationResult(FrozenModel):
    receipt: BootstrapReceipt
    replayed: bool = False


class SqlResetOperator:
    """SQL adapter and synchronous, transaction-bound reset orchestration."""

    def __init__(
        self, database: AzureSqlDatabase, target: ResetTarget, *,
        observer: PreflightObserver | None = None,
        deployment_inventory_reader: DeploymentInventoryReader | None = None,
        registration_names: RegistrationNames = DEFAULT_REGISTRATION_NAMES,
    ) -> None:
        self.db = database
        self.target = ResetTarget.model_validate(target)
        self.catalogue = build_catalogue(registration_names)
        self.observer = observer
        self.deployment_inventory_reader = deployment_inventory_reader
        if deployment_inventory_reader is not None and not callable(getattr(deployment_inventory_reader, "read", None)):
            raise ResetRefused("Authoritative deployment inventory must come from a schema-owned live reader")
        if observer is not None and (
            not callable(getattr(observer, "observe", None))
            or not isinstance(getattr(observer, "profile", None), ObservationProfile)
            or observer.profile.target != self.target
        ):
            raise ResetRefused("Inject a trusted live observer bound to this operator and database target")
        configured = getattr(database, "_tables", None) or {}
        expected = DEFAULT_TABLES | monitoring_schema.DEFAULT_MONITORING_TABLES | {"monitoring_rate_budget": DEFAULT_RATE_TABLE}
        if any(key not in expected or value != expected[key] for key, value in configured.items()):
            raise ResetRefused("Custom table mappings need an explicit schema-owner operator contract")
        if (getattr(database, "_server", None), getattr(database, "_database", None)) != (
            target.server, target.database,
        ):
            raise ResetRefused("Injected database configuration does not match the explicit target")
        credential = getattr(database, "_credential", None)
        if not isinstance(credential, PinnedDeployerCredential) or credential.target != self.target:
            raise ResetRefused("Use a fresh database handle with an explicitly pinned deployer Entra credential")
        if getattr(getattr(database, "_local", None), "conn", None) is not None:
            raise ResetRefused("An already-open SQL connection cannot establish fresh deployer identity")

    def _identity(self) -> tuple[str, int, datetime]:
        rows = self.db.query(
            "/* monitoring-reset:identity */ SELECT "
            "CONVERT(NVARCHAR(256), SERVERPROPERTY('ServerName')), DB_NAME(), DB_ID(), SYSUTCDATETIME()",
        )
        if len(rows) != 1 or len(rows[0]) != 4 or rows[0][1] != self.target.database:
            raise ResetRefused("Connected SQL database does not match the explicit target")
        server, _, database_id, now = rows[0]
        if not isinstance(server, str) or not server or type(database_id) is not int or database_id < 1:
            raise ResetRefused("SQL server/database identity could not be established")
        return server, database_id, _utc(now)

    def _layout(self, table: TableSpec, object_id: int) -> tuple[str, bool]:
        rows = self.db.query(
            "/* monitoring-reset:columns */ SELECT c.name, t.name, c.max_length, c.scale, "
            "c.is_nullable, c.collation_name, c.is_identity, c.is_computed, "
            "c.generated_always_type, c.encryption_type "
            "FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id "
            "WHERE c.object_id=? ORDER BY c.column_id", object_id,
        )
        actual = []
        compatible = len(rows) == len(table.columns)
        for row, expected in zip(rows, table.columns, strict=False):
            if len(row) != 10:
                raise ResetRefused("SQL column metadata has an unexpected shape")
            name, kind, width, scale, nullable, collation, identity, computed, generated, encryption = row
            actual.append(list(row))
            width_matches = (
                width in {6, 7, 8} if expected.data_type == "datetime2" else width == expected.max_length
            )
            compatible &= (
                (name, kind.lower(), scale, bool(nullable))
                == (expected.name, expected.data_type, expected.scale, expected.nullable)
                and width_matches
                and (expected.collation is None or collation == expected.collation)
                and not identity and not computed and generated == 0 and encryption is None
            )
        primary = self.db.query(
            "/* monitoring-reset:primary-key */ SELECT c.name "
            "FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id "
            "JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id "
            "WHERE i.object_id=? AND i.is_primary_key=1 ORDER BY ic.key_ordinal", object_id,
        )
        compatible &= tuple(row[0] for row in primary) == table.primary_key
        modifiers = self.db.query(
            "/* monitoring-reset:table-safety */ SELECT temporal_type, is_tracked_by_cdc, is_memory_optimized, "
            "(SELECT COUNT(*) FROM sys.triggers WHERE parent_id=? AND is_disabled=0) "
            "FROM sys.tables WHERE object_id=?", object_id, object_id,
        )
        compatible &= len(modifiers) == 1 and tuple(modifiers[0]) == (0, False, False, 0)
        constraints = self.db.query(
            "/* monitoring-reset:constraints */ SELECT o.type, o.name, "
            "CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', COALESCE(OBJECT_DEFINITION(o.object_id), N'')), 2) "
            "FROM sys.objects o WHERE parent_object_id=? ORDER BY o.type, o.name", object_id,
        )
        indexes = self.db.query(
            "/* monitoring-reset:indexes */ SELECT i.name,i.is_unique,i.is_disabled,ic.key_ordinal,"
            "ic.is_descending_key,ic.is_included_column,c.name,"
            "CONVERT(VARCHAR(64),HASHBYTES('SHA2_256',COALESCE(i.filter_definition,N'')),2) "
            "FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id "
            "JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id "
            "WHERE i.object_id=? ORDER BY i.index_id,ic.index_column_id", object_id,
        )
        return _hash({
            "columns": actual, "primary_key": primary, "safety": modifiers,
            "constraints": constraints, "indexes": indexes,
        }), bool(compatible)

    def _contents(self, table: TableSpec) -> tuple[int, str, int]:
        quoted = quote_identifier(table.name)
        columns = ", ".join(f"[{_name(column.name)}]" for column in table.columns)
        ordering = ", ".join(f"[{_name(name)}]" for name in table.primary_key)
        rows = self.db.query(
            f"/* monitoring-reset:content:{table.name} */ SELECT COUNT_BIG(*), "
            "CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', "
            f"(SELECT {columns} FROM {quoted} WITH (HOLDLOCK) ORDER BY {ordering} "
            f"FOR JSON PATH, INCLUDE_NULL_VALUES)), 2) FROM {quoted} WITH (HOLDLOCK)",
        )
        if len(rows) != 1 or type(rows[0][0]) is not int or rows[0][0] < 0:
            raise ResetRefused("An exact owned-table count could not be read")
        retained = rows[0][0] if table.operation in {"preserve_service_budget", "preserve_registration"} else 0
        if table.operation == "preserve_reset_receipts":
            keep = self.db.query(
                f"/* monitoring-reset:retained:{table.name} */ SELECT COUNT_BIG(*) FROM {quoted} "
                "WITH (HOLDLOCK) WHERE operation IN (?,?,?)", RESET_OPERATION, BOOTSTRAP_OPERATION, CAPTURE_OPERATION,
            )
            if len(keep) != 1 or type(keep[0][0]) is not int:
                raise ResetRefused("Reset receipt retention count is unknown")
            retained = keep[0][0]
            if retained > MAX_HAZARDS:
                raise ResetRefused("Reset receipt history exceeds the bounded metadata validation budget")
            metadata = self.db.query(
                f"/* monitoring-reset:retained-metadata */ SELECT operation,payload FROM {quoted} "
                "WITH (HOLDLOCK) WHERE operation IN (?,?,?)", RESET_OPERATION, BOOTSTRAP_OPERATION, CAPTURE_OPERATION,
            )
            if len(metadata) != retained:
                raise ResetRefused("Reset receipt metadata changed during validation")
            try:
                for record in metadata:
                    model = {
                        RESET_OPERATION: ResetReceipt, BOOTSTRAP_OPERATION: BootstrapReceipt,
                        CAPTURE_OPERATION: DiscoveryCapture,
                    }[record[0]]
                    model.model_validate_json(record[1])
            except (ValueError, TypeError, IndexError) as exc:
                raise ResetRefused("Reserved reset receipts contain unsupported metadata; operational payloads cannot be preserved") from exc
        return rows[0][0], str(rows[0][1]).lower(), retained

    def _control(self) -> tuple[m.DeploymentControl | None, str | None, str | None]:
        table = quote_identifier(self.catalogue.table("monitoring_control").name)
        rows = self.db.query(
            f"/* monitoring-reset:control */ SELECT singleton, schema_version, tenant_id, epoch, revision, "
            f"activation_cutoff, maintenance, updated_at, bootstrap_id, bootstrap_hash, payload FROM {table} WITH (HOLDLOCK)",
        )
        if not rows:
            return None, None, None
        if len(rows) != 1 or len(rows[0]) != 11 or rows[0][0] != 1:
            raise ResetRefused("Monitoring control does not have its singleton baseline")
        row = rows[0]
        if row[2] != self.target.tenant_id:
            raise ResetRefused("The database belongs to a different monitoring tenant")
        if row[1] != m.MONITORING_SCHEMA_VERSION:
            raise ResetRefused("Monitoring schema version is incompatible; no reset is permitted")
        try:
            control = m.DeploymentControl.model_validate_json(row[10])
            promoted = (
                row[1], row[2], row[3], row[4], _utc(row[5]), bool(row[6]), _utc(row[7]),
            )
            if promoted != (
                control.schema_version, control.tenant_id, control.epoch, control.revision,
                control.activation_cutoff, control.maintenance, control.updated_at,
            ):
                raise ValueError("Promoted control disagrees with its payload")
            bootstrap_id = m.canonical_id(row[8])
            bootstrap_hash = str(row[9]).lower()
            if not re.fullmatch(r"[0-9a-f]{64}", bootstrap_hash):
                raise ValueError("Bootstrap fingerprint is invalid")
        except (ValueError, TypeError, AttributeError) as exc:
            raise ResetRefused("Monitoring control metadata is malformed") from exc
        return control, bootstrap_id, bootstrap_hash

    def _hazards(self) -> tuple[Hazard, ...]:
        requests: list[tuple[str, str, str, str, str, str, str]] = [
            ("leases", "lease", "lease_name", "'held'", "expires_at", "NULL", "expires_at > SYSUTCDATETIME()"),
            ("claims", "lease", "claim_key", "'held'", "expires_at", "NULL", "expires_at > SYSUTCDATETIME()"),
            ("monitoring_leases", "lease", "full_key", "'held'", "expires_at", "NULL", "expires_at > SYSUTCDATETIME()"),
            ("agent_commands", "command", "command_id", "state", "lease_expires_at", "NULL", "state NOT IN ('completed','failed','interrupted')"),
            ("agent_runs", "run", "run_id", "state", "NULL", "NULL", "state NOT IN ('completed','failed')"),
            ("pipeline_reruns", "action", "run_key", "state", "NULL", "JSON_VALUE(payload, '$.rerun_id')", "state NOT IN ('completed','failed')"),
            ("retries", "retry", "signature", "COALESCE(status,'unknown')", "NULL", "NULL", "status IS NULL OR status NOT IN ('completed','cancelled','failed')"),
            ("monitoring_records", "action", "full_key", "COALESCE(status,'unknown')", "NULL",
             "JSON_VALUE(payload, '$.submitted_execution.run_id')",
             "record_kind='action' AND (status IS NULL OR status NOT IN ('verified_succeeded','verified_failed'))"),
            ("monitoring_records", "work", "full_key", "COALESCE(status,'unknown')", "NULL", "NULL",
             "record_kind='work' AND (status IS NULL OR status NOT IN ('completed','dispositioned'))"),
            ("monitoring_records", "work", "full_key", "COALESCE(status,'unknown')", "NULL", "NULL",
             "record_kind NOT IN ('action','work') AND status IN "
             "('pending','running','reserved','submitted','uncertain','queued','leased','waiting',"
             "'finalizing','planned','provisioning','deleting')"),
        ]
        result = []
        for logical, kind, key, status, expires, correlation, predicate in requests:
            table = self.catalogue.table(logical)
            correlation = f"CONVERT(NVARCHAR(256), {correlation})"
            if logical == "pipeline_reruns":
                external = f"N'fabric_pipeline', workspace_id, pipeline_id, {correlation}"
            elif logical == "monitoring_records" and kind == "action":
                external = (
                    "JSON_VALUE(payload,'$.request.source_execution.target.workload'),"
                    "JSON_VALUE(payload,'$.request.source_execution.target.workspace_id'),"
                    "JSON_VALUE(payload,'$.request.source_execution.target.item_id')," + correlation
                )
            else:
                external = "NULL,NULL,NULL,NULL"
            key_expression = (
                f"CONCAT(tenant_id,N':',epoch,N':',record_kind,N':',[{key}])"
                if logical == "monitoring_records" else
                f"CONCAT(tenant_id,N':',epoch,N':',[{key}])"
                if logical == "monitoring_leases" else f"[{key}]"
            )
            rows = self.db.query(
                f"/* monitoring-reset:hazards:{logical}:{kind} */ SELECT TOP ({MAX_HAZARDS + 1}) "
                f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(MAX), {key_expression})), 2), {status}, {expires}, "
                f"CASE WHEN NULLIF({correlation},'') IS NULL THEN NULL ELSE "
                f"CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', {correlation}), 2) END, {external} "
                f"FROM {quote_identifier(table.name)} WITH (HOLDLOCK) WHERE {predicate} ORDER BY [{key}]",
            )
            if len(result) + len(rows) > MAX_HAZARDS:
                raise ResetRefused("Outstanding state exceeds the bounded proof inventory; no state was skipped")
            for row in rows:
                if len(row) != 8:
                    raise ResetRefused("Outstanding state metadata has an unexpected shape")
                if row[1] not in {
                    "held", "queued", "running", "pending", "reserved", "submitted", "unknown", "uncertain",
                    "leased", "waiting", "finalizing", "planned", "provisioning", "deleting",
                }:
                    raise ResetRefused("Outstanding state has an unrecognized status; no state was silently skipped")
                result.append(Hazard(
                    table=table.name, kind=kind, key_hash=str(row[0]).lower(), state=row[1],
                    expires_at=_utc(row[2]) if row[2] is not None else None,
                    correlation_hash=str(row[3]).lower() if row[3] is not None else None,
                    workload=row[4], workspace_id=row[5], item_id=row[6], run_id=row[7] or None,
                ))
        return tuple(sorted(result, key=lambda value: value.key))

    def _registered_targets(self, compatible: set[str], hazards: tuple[Hazard, ...]) -> tuple[ActionTarget, ...]:
        """Read target identities from authoritative records, never the caller profile."""
        result: dict[str, ActionTarget] = {}
        records = self.catalogue.table("monitoring_records")
        if records.name in compatible:
            roots = {
                "target": "$.identity", "inventory": "$",
                "action": "$.request.source_execution.target",
                "source": "$.execution.target", "rest_observation": "$.execution.target", "work": "$.target",
            }
            for kind, root in roots.items():
                fields = ",".join(
                    f"JSON_VALUE(payload,'{root}.{field}')" for field in
                    ("tenant_id", "epoch", "workload", "workspace_id", "item_id")
                )
                rows = self.db.query(
                    f"/* monitoring-reset:registered-targets:{kind} */ SELECT TOP ({MAX_HAZARDS + 1}) "
                    f"tenant_id,epoch,workload,workspace_id,item_id,{fields},JSON_VALUE(payload,'$.kind') "
                    f"FROM {quote_identifier(records.name)} WITH (HOLDLOCK) WHERE record_kind=? "
                    "ORDER BY key_hash", kind,
                )
                if len(rows) > MAX_HAZARDS:
                    raise ResetRefused("Registered target evidence exceeds the bounded read; no target may be omitted")
                for row in rows:
                    if len(row) != 11:
                        raise ResetRefused("Registered target metadata has an unsupported schema shape")
                    tenant, epoch, workload, workspace, item = row[5:10]
                    if kind == "inventory" and workload is None:
                        continue
                    if kind == "work" and tenant is None and row[10] in {"inventory", "connector_reconcile"}:
                        continue
                    try:
                        identity = m.TargetIdentity(
                            tenant_id=tenant, epoch=epoch, workload=workload, workspace_id=workspace, item_id=item,
                        )
                    except ValueError as exc:
                        raise ResetRefused("Registered target identity is unreadable; no profile fallback is permitted") from exc
                    if identity.tenant_id != self.target.tenant_id or any(
                        promoted is not None and promoted != actual
                        for promoted, actual in zip(row[:5], (
                            identity.tenant_id, identity.epoch, identity.workload, identity.workspace_id, identity.item_id,
                        ), strict=True)
                    ):
                        raise ResetRefused("Promoted SQL target identity disagrees with its stored binding")
                    target = ActionTarget(
                        workload=identity.workload, workspace_id=identity.workspace_id, item_id=identity.item_id,
                    )
                    result[target.key] = target
        reruns = self.catalogue.table("pipeline_reruns")
        if reruns.name in compatible:
            rows = self.db.query(
                f"/* monitoring-reset:registered-pipelines */ SELECT DISTINCT TOP ({MAX_HAZARDS + 1}) "
                f"workspace_id,pipeline_id FROM {quote_identifier(reruns.name)} WITH (HOLDLOCK)",
            )
            if len(rows) > MAX_HAZARDS:
                raise ResetRefused("Pipeline action inventory exceeds its bounded read")
            for row in rows:
                target = ActionTarget(workload="fabric_pipeline", workspace_id=row[0], item_id=row[1])
                result[target.key] = target
        for hazard in hazards:
            if hazard.kind == "action" and None not in (hazard.workload, hazard.workspace_id, hazard.item_id):
                target = ActionTarget(workload=hazard.workload, workspace_id=hazard.workspace_id, item_id=hazard.item_id)
                result[target.key] = target
        return tuple(result[key] for key in sorted(result))

    def _deployment_inventory(self, snapshot: ResetSnapshot) -> DeploymentWriterInventory:
        reader = self.deployment_inventory_reader
        if reader is None:
            raise ResetRefused(
                "Authoritative deployment/SQL writer inventory is unavailable: supply "
                "deployment_inventory_reader=RegisteredDeploymentInventoryReader covering resources, SQL identities and invokers. "
                "Caller preflight profiles cannot establish completeness."
            )
        try:
            supplied = reader.read(self.db, self.target, self.catalogue)
            authority = read_authority(
                self.db, self.target, self.catalogue, names=self.catalogue.registration_names,
            )
            authority.require_protected()
        except DeploymentError as exc:
            raise ResetRefused(str(exc)) from exc
        if not isinstance(supplied, DeploymentWriterInventory):
            raise ResetRefused("Deployment inventory reader returned unsupported evidence, not authoritative bindings")
        inventory = DeploymentWriterInventory.model_validate_json(supplied.model_dump_json())
        _, _, now = self._identity()
        if (
            inventory.target != self.target or inventory.database_id != snapshot.database_id
            or inventory.server_identity != snapshot.server_identity
            or inventory.catalogue_hash != self.catalogue.declaration_hash
            or not snapshot.inspected_at <= inventory.observed_at <= now
        ):
            raise ResetRefused("Authoritative deployment inventory is stale or bound to another SQL deployment/schema")
        if (
            inventory.authority_snapshot_hash != authority.snapshot_hash
            or inventory.kernel_contract_hash != authority.kernel_hash
            or inventory.registration_request_id is None or inventory.discovery_capture_hash is None
            or inventory.current_binding_hash is None or inventory.resource_observed_at is None
        ):
            raise ResetRefused("Deployment inventory lacks current protected registration/SQL authority bindings")
        bindings: dict[int, set[bytes]] = {}
        for binding in inventory.writers:
            if binding.sql_principal_id is not None:
                bindings.setdefault(binding.sql_principal_id, set()).add(UUID(binding.identity_client_id).bytes_le)
        actual: set[int] = set()
        for principal in authority.writers:
            principal_id, sid = principal.principal_id, principal.sid
            if bindings.get(principal_id) != {sid}:
                raise ResetRefused("Authoritative deployment inventory omits or misidentifies a SQL write-capable principal")
            actual.add(principal_id)
        if actual != set(bindings):
            raise ResetRefused("Deployment writer bindings do not match actual SQL write-capable principals")
        return inventory

    def _snapshot(self, *, exclusive: bool = False) -> ResetSnapshot:
        server, database_id, now = self._identity()
        objects = self.db.query(
            "/* monitoring-reset:objects */ SELECT s.name, o.name, o.type, o.object_id, "
            "COALESCE(o.principal_id,s.principal_id), o.create_date, o.modify_date, "
            "CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', "
            "LTRIM(RTRIM(REPLACE(COALESCE(OBJECT_DEFINITION(o.object_id),N''),CHAR(13)+CHAR(10),CHAR(10))))),2) "
            "FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id "
            "WHERE o.is_ms_shipped=0 AND o.parent_object_id=0 AND o.type IN ('U','P','V','FN','IF','TF','SN','SO') "
            "ORDER BY s.name,o.name",
        )
        expected_names = set(self.catalogue.table_names) | {module.name for module in self.catalogue.modules}
        blockers = []
        by_name = {}
        for row in objects:
            if len(row) != 8:
                raise ResetRefused("SQL object inventory has an unexpected shape")
            schema, name, kind, *_ = row
            if name.startswith("triage_") and (schema != "dbo" or name not in expected_names):
                blockers.append(f"unexpected_accelerator_object:{schema}.{name}")
            if schema == "dbo":
                if name in by_name:
                    raise ResetRefused("SQL object inventory repeats an identity")
                by_name[name] = row
        layouts: dict[str, str] = {}
        compatible = set()
        for table in self.catalogue.tables:
            row = by_name.get(table.name)
            if row is None:
                blockers.append(f"missing_table:{table.name}")
            elif row[2] != "U":
                blockers.append(f"wrong_object_type:{table.name}")
            else:
                if row[4] != 1:
                    blockers.append(f"unsupported_owner:{table.name}")
                layout_hash, valid = self._layout(table, row[3])
                layouts[table.name] = _hash({
                    "layout": layout_hash, "owner": row[4],
                    "created_at": _utc(row[5]).isoformat(), "modified_at": _utc(row[6]).isoformat(),
                })
                if not valid:
                    blockers.append(f"incompatible_table:{table.name}")
                else:
                    compatible.add(table.name)
        references = self.db.query(
            "/* monitoring-reset:foreign-keys */ SELECT ps.name,pt.name,pc.name,rs.name,rt.name,rc.name,"
            "fk.delete_referential_action,fk.update_referential_action,fk.is_disabled "
            "FROM sys.foreign_keys fk JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id=fk.object_id "
            "JOIN sys.tables pt ON pt.object_id=fkc.parent_object_id JOIN sys.schemas ps ON ps.schema_id=pt.schema_id "
            "JOIN sys.columns pc ON pc.object_id=pt.object_id AND pc.column_id=fkc.parent_column_id "
            "JOIN sys.tables rt ON rt.object_id=fkc.referenced_object_id JOIN sys.schemas rs ON rs.schema_id=rt.schema_id "
            "JOIN sys.columns rc ON rc.object_id=rt.object_id AND rc.column_id=fkc.referenced_column_id",
        )
        actual_edges = set()
        expected_edges = {
            (edge.child, edge.child_column, edge.parent, edge.parent_column) for edge in self.catalogue.foreign_keys
        }
        for row in references:
            if len(row) != 9:
                raise ResetRefused("SQL foreign-key inventory has an unexpected shape")
            if row[1] in self.catalogue.table_names or row[4] in self.catalogue.table_names:
                edge = (row[1], row[2], row[4], row[5])
                if row[0] != "dbo" or row[3] != "dbo" or edge not in expected_edges or any(row[6:]):
                    blockers.append("undeclared_or_cascading_foreign_key")
                actual_edges.add(edge)
        required_edges = {
            edge for edge in expected_edges if edge[0] in by_name and edge[2] in by_name
        }
        if actual_edges != required_edges:
            blockers.append("foreign_key_layout_changed")
        if exclusive and not blockers:
            # Lock all owned tables in one consistent order before the final
            # inventory/count read. Foreign/unrelated objects are never locked.
            for table in self.catalogue.tables:
                self.db.query(
                    f"/* monitoring-reset:lock:{table.name} */ SELECT COUNT_BIG(*) "
                    f"FROM {quote_identifier(table.name)} WITH (TABLOCKX,HOLDLOCK)",
                )
            # Metadata may have changed between discovery and acquiring the
            # table locks. Re-read it while those transaction locks are held.
            return self._snapshot()
        control, bootstrap_id, bootstrap_hash = (None, None, None)
        if self.catalogue.table("monitoring_control").name in compatible:
            control, bootstrap_id, bootstrap_hash = self._control()
        if control is None:
            blockers.append("monitoring_baseline_missing")
        elif not control.maintenance:
            blockers.append("monitoring_not_in_maintenance")
        snapshots = []
        for table in self.catalogue.tables:
            row = by_name.get(table.name)
            count, content_hash, retained = (None, None, 0)
            if table.name in compatible:
                count, content_hash, retained = self._contents(table)
                if table.logical_name in {"monitoring_records", "monitoring_leases", "monitoring_receipts", "deployment_registration"}:
                    tenants = self.db.query(
                        f"/* monitoring-reset:tenants:{table.name} */ SELECT COUNT_BIG(*) "
                        f"FROM {quote_identifier(table.name)} WITH (HOLDLOCK) WHERE tenant_id<>?",
                        self.target.tenant_id,
                    )
                    if len(tenants) != 1 or tenants[0][0] != 0:
                        blockers.append(f"other_tenant_state:{table.name}")
            snapshots.append(ObjectSnapshot(
                name=f"dbo.{table.name}", kind="table", object_id=row[3] if row else None,
                schema_hash=layouts.get(table.name), row_count=count, content_hash=content_hash,
                retained_rows=retained, operation=table.operation,
            ))
        for module in self.catalogue.modules:
            name = module.name
            row = by_name.get(name)
            expected_types = {"procedure": {"P"}, "view": {"V"}, "function": {"FN", "IF", "TF"}}
            if row is None:
                blockers.append(f"missing_or_wrong_module:{name}")
            elif row[2] not in expected_types[module.kind]:
                blockers.append(f"wrong_module_type:{name}")
            elif row[4] != 1 or str(row[7]).lower() != module.native_hash:
                blockers.append(f"module_definition_or_owner_changed:{name}")
            snapshots.append(ObjectSnapshot(
                name=f"dbo.{name}", kind=module.kind, object_id=row[3] if row else None,
                schema_hash=_hash([str(value) for value in row[4:]]) if row else None,
                operation="retain_definition",
            ))
        roles = self.db.query(
            "/* monitoring-reset:roles */ SELECT name,principal_id,owning_principal_id,is_fixed_role "
            "FROM sys.database_principals WHERE type='R' ORDER BY name",
        )
        role_map = {row[0]: row for row in roles}
        for name in role_map:
            if name.startswith("triage_") and name not in self.catalogue.roles:
                blockers.append(f"unexpected_accelerator_role:{name}")
        for name in self.catalogue.roles:
            row = role_map.get(name)
            if row is None:
                blockers.append(f"missing_role:{name}")
            elif tuple(row[2:]) != (1, False):
                blockers.append(f"unsupported_role_owner:{name}")
            snapshots.append(ObjectSnapshot(
                name=name, kind="role", object_id=row[1] if row else None,
                schema_hash=_hash(list(row)) if row else None, operation="retain_definition",
            ))
        hazards = self._hazards() if len(compatible) == len(self.catalogue.tables) else ()
        required_targets = self._registered_targets(compatible, hazards)
        return ResetSnapshot(
            target=self.target, server_identity=server, database_id=database_id, inspected_at=now,
            control=control, bootstrap_id=bootstrap_id, bootstrap_hash=bootstrap_hash,
            objects=tuple(snapshots), hazards=hazards, hazards_complete=len(compatible) == len(self.catalogue.tables),
            required_action_targets=required_targets,
            blockers=tuple(sorted(set(blockers))),
        )

    def plan(self) -> ResetManifest:
        with self.db.transaction():
            snapshot = self._snapshot()
            inventory = None
            if self.deployment_inventory_reader is not None and not snapshot.blockers:
                try:
                    inventory = self._deployment_inventory(snapshot)
                except ResetRefused as exc:
                    snapshot = snapshot.model_copy(update={
                        "blockers": (*snapshot.blockers, "deployment_inventory_unproved:" + str(exc)),
                    })
        profile = self.observer.profile if self.observer is not None else (
            ObservationProfile(
                target=self.target, writers=tuple(item.writer for item in inventory.writers),
                action_targets=snapshot.required_action_targets,
            ) if inventory is not None else None
        )
        return ResetManifest(
            operation_id=str(uuid4()), new_epoch=str(uuid4()),
            catalogue_hash=self.catalogue.declaration_hash, snapshot=snapshot,
            preflight_profile=profile,
            deployment_inventory=inventory,
        )

    def plan_initialization(self) -> ResetManifest:
        """Read-only first-deployment plan; existing application rows are never imported."""
        with self.db.transaction():
            snapshot = self._snapshot()
        return ResetManifest(
            operation_id=str(uuid4()), new_epoch=str(uuid4()), purpose="initialize",
            catalogue_hash=self.catalogue.declaration_hash, snapshot=snapshot,
        )

    def _initialization_ready(self, manifest: ResetManifest, current: ResetSnapshot) -> None:
        authority = read_authority(
            self.db, self.target, self.catalogue, names=self.catalogue.registration_names,
        )
        if set(authority.gaps) & {"unreviewed_trigger_authority", "autonomous_sql_writer"}:
            raise ResetRefused("Unreviewed SQL triggers/activation prevent a schema-only initial bootstrap")
        new_names = (
            set(monitoring_schema.DEFAULT_MONITORING_TABLES.values()) | {DEFAULT_RATE_TABLE}
            | set(self.catalogue.registration_names.tables.values())
        )
        new_modules = {unqualified(obj.name) for obj in build_permission_kernel().objects if obj.kind != "role"}
        new_modules.add(self.catalogue.registration_names.read_projection)
        allowed = (
            {f"missing_table:{name}" for name in new_names} | {"monitoring_baseline_missing"}
            | {f"missing_or_wrong_module:{name}" for name in new_modules}
            | {f"missing_role:{name}" for name in self.catalogue.roles}
        )
        if set(current.blockers) - allowed:
            raise ResetRefused("Initial bootstrap found incompatible, unexpected or missing application objects")
        original = {item.name: item for item in manifest.snapshot.objects}
        for item in current.objects:
            if item.name.removeprefix("dbo.") not in new_names | new_modules | set(self.catalogue.roles):
                approved = original[item.name]
                if (item.object_id, item.schema_hash) != (approved.object_id, approved.schema_hash):
                    raise ResetRefused("Application object ownership/schema changed before initial bootstrap")
            elif item.name != f"dbo.{DEFAULT_RATE_TABLE}" and item.row_count not in (None, 0):
                raise ResetRefused("An uninitialized monitoring baseline contains state; no import or upgrade is permitted")

    def _bootstrap_receipt(self, manifest: ResetManifest) -> BootstrapReceipt | None:
        table = self.catalogue.table("monitoring_receipts")
        exists = self.db.query(
            "/* monitoring-reset:receipt-table */ SELECT OBJECT_ID(?, 'U')", f"dbo.{table.name}",
        )
        if len(exists) != 1 or exists[0][0] is None:
            return None
        rows = self.db.query(
            f"/* monitoring-reset:receipt */ SELECT fingerprint,payload FROM {quote_identifier(table.name)} "
            "WITH (HOLDLOCK) WHERE tenant_id=? AND operation=? AND request_hash=?",
            self.target.tenant_id, BOOTSTRAP_OPERATION, hashlib.sha256(manifest.operation_id.encode()).digest(),
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise ResetRefused("Initial bootstrap operation is not unique")
        receipt = BootstrapReceipt.model_validate_json(rows[0][1])
        if (
            rows[0][0] != manifest.manifest_hash or receipt.manifest_hash != manifest.manifest_hash
            or receipt.target != self.target or receipt.operation_id != manifest.operation_id
            or receipt.control.epoch != manifest.new_epoch
        ):
            raise ResetRefused("Initial bootstrap operation ID was reused for another manifest")
        return receipt

    def initialize(
        self, manifest: ResetManifest, *, confirmed_manifest_hash: str, expected_uninitialized: bool,
    ) -> InitializationResult:
        """Explicit deployment-only initialization; it cannot delete or adopt old state."""
        manifest = ResetManifest.model_validate_json(manifest.model_dump_json())
        if (
            expected_uninitialized is not True or manifest.purpose != "initialize"
            or manifest.snapshot.control is not None or manifest.snapshot.target != self.target
            or manifest.manifest_hash != confirmed_manifest_hash
            or manifest.catalogue_hash != self.catalogue.declaration_hash
        ):
            raise ResetRefused("Initial bootstrap requires its exact manifest and explicit uninitialized expectation")
        control = m.DeploymentControl(
            tenant_id=self.target.tenant_id, epoch=manifest.new_epoch, revision=0, maintenance=True,
            activation_cutoff=manifest.snapshot.inspected_at, updated_at=manifest.snapshot.inspected_at,
        )
        bootstrap_hash = hashlib.sha256(control.model_dump_json().encode()).hexdigest()
        with self.db.transaction():
            current = self._snapshot()
            if (current.server_identity, current.database_id) != (
                manifest.snapshot.server_identity, manifest.snapshot.database_id,
            ):
                raise ResetRefused("Initial bootstrap reached another SQL server/database")
            prior = self._bootstrap_receipt(manifest)
            if current.control is not None:
                if (
                    current.bootstrap_id != manifest.operation_id or current.bootstrap_hash != bootstrap_hash
                    or current.control.epoch != manifest.new_epoch
                ):
                    raise ResetRefused("A different baseline exists; initial bootstrap is not a reset or upgrade")
                if prior is not None:
                    return InitializationResult(receipt=prior, replayed=True)
            else:
                self._initialization_ready(manifest, current)
        # The schema owner owns this transaction. Old application rows remain
        # untouched, including when helper acknowledgement or a later step fails.
        try:
            monitoring_schema.initialize_monitoring_schema(self.db, control=control, bootstrap_id=manifest.operation_id)
        except SqlCommitUncertain as exc:
            with self.db.transaction():
                existing = self._snapshot()
            if (
                existing.control is None or existing.bootstrap_id != manifest.operation_id
                or existing.bootstrap_hash != bootstrap_hash
            ):
                raise ResetCommitUncertain("Initial bootstrap acknowledgement is uncertain; no initialization was retried") from exc
        with self.db.transaction():
            for statement in rate_schema_statements():
                self.db.execute(statement)
            current = self._snapshot()
            pending_operator_objects = {
                *(f"missing_table:{name}" for name in self.catalogue.registration_names.tables.values()),
                *(f"missing_role:{name}" for name in self.catalogue.roles),
                *(f"missing_or_wrong_module:{unqualified(obj.name)}" for obj in build_permission_kernel().objects if obj.kind != "role"),
                f"missing_or_wrong_module:{self.catalogue.registration_names.read_projection}",
            }
            if set(current.blockers) - pending_operator_objects or current.bootstrap_id != manifest.operation_id or current.bootstrap_hash != bootstrap_hash:
                raise ResetRefused("Initial baseline verification failed; no application data was reset")
            prior = self._bootstrap_receipt(manifest)
            if prior is not None:
                return InitializationResult(receipt=prior, replayed=True)
            receipt = BootstrapReceipt(
                operation_id=manifest.operation_id, manifest_hash=manifest.manifest_hash,
                target=self.target, control=control, bootstrap_hash=bootstrap_hash, recorded_at=current.inspected_at,
            )
            table = quote_identifier(self.catalogue.table("monitoring_receipts").name)
            inserted = self.db.execute(
                f"/* monitoring-reset:insert-receipt */ INSERT INTO {table} "
                "(tenant_id,epoch,operation,request_hash,request_id,fingerprint,recorded_at,payload) VALUES (?,?,?,?,?,?,?,?)",
                self.target.tenant_id, manifest.new_epoch, BOOTSTRAP_OPERATION,
                hashlib.sha256(manifest.operation_id.encode()).digest(), manifest.operation_id,
                manifest.manifest_hash, current.inspected_at.replace(tzinfo=None), receipt.model_dump_json(),
            )
            if inserted != 1:
                raise ResetRefused("Initial bootstrap receipt was not confirmed")
        return InitializationResult(receipt=receipt)

    def _live_preflight(
        self, manifest: ResetManifest, current: ResetSnapshot,
    ) -> PreflightObservation:
        if self.observer is None or manifest.preflight_profile is None or (
            self.observer.profile != manifest.preflight_profile
        ):
            raise ResetRefused("Reset requires the trusted live observer and the exact approved deployment bindings")
        required_targets = {target.key for target in current.required_action_targets}
        if not required_targets <= {target.key for target in manifest.preflight_profile.action_targets}:
            raise ResetRefused("Caller profile omits SQL-registered targets/actions; external-effect coverage is incomplete")
        inventory = self._deployment_inventory(current)
        if manifest.deployment_inventory is None or inventory.state_hash != manifest.deployment_inventory.state_hash:
            raise ResetRefused("Authoritative deployment/SQL writer inventory changed or was not in the approved manifest")
        expected_writers = {binding.writer.writer_id: binding.writer for binding in inventory.writers}
        if {writer.writer_id: writer for writer in manifest.preflight_profile.writers} != expected_writers:
            raise ResetRefused("Caller profile differs from the authoritative deployed writer inventory")
        _, _, requested_at = self._identity()
        request = PreflightRequest(
            challenge=str(uuid4()), manifest_hash=manifest.manifest_hash, target=self.target,
            expected_epoch=current.control.epoch, ownership_hash=current.state_hash,
            requested_at=requested_at, profile=manifest.preflight_profile, hazards=current.hazards,
            required_action_targets=current.required_action_targets, deployment_inventory=inventory,
        )
        observation = self.observer.observe(request)
        _, _, now = self._identity()
        validate_observation(observation, request, now)
        return observation

    def _receipt(self, manifest: ResetManifest) -> ResetReceipt | None:
        table = self.catalogue.table("monitoring_receipts")
        exists = self.db.query(
            "/* monitoring-reset:receipt-table */ SELECT OBJECT_ID(?, 'U')", f"dbo.{table.name}",
        )
        if len(exists) != 1 or exists[0][0] is None:
            return None
        rows = self.db.query(
            f"/* monitoring-reset:receipt */ SELECT fingerprint,payload FROM {quote_identifier(table.name)} "
            "WITH (HOLDLOCK) WHERE tenant_id=? AND operation=? AND request_hash=?",
            self.target.tenant_id, RESET_OPERATION, hashlib.sha256(manifest.operation_id.encode()).digest(),
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise ResetRefused("Reset operation identity is not unique across epochs")
        try:
            receipt = ResetReceipt.model_validate_json(rows[0][1])
        except ValueError as exc:
            raise ResetRefused("Reset receipt metadata is malformed") from exc
        if (
            rows[0][0] != manifest.manifest_hash or receipt.manifest_hash != manifest.manifest_hash
            or receipt.operation_id != manifest.operation_id or receipt.target != self.target
            or manifest.snapshot.control is None
            or receipt.expected_old_epoch != manifest.snapshot.control.epoch
            or receipt.new_control.epoch != manifest.new_epoch
        ):
            raise ResetRefused("Reset operation ID was reused with different approval content")
        return receipt

    def _check_receipt_baseline(self, receipt: ResetReceipt) -> None:
        control, bootstrap_id, bootstrap_hash = self._control()
        if (
            control is None or control.epoch != receipt.new_control.epoch
            or bootstrap_id != receipt.operation_id or bootstrap_hash != receipt.bootstrap_hash
        ):
            raise ResetRefused("Reset receipt and current baseline do not match; no state was deleted")

    def execute(
        self, manifest: ResetManifest, *, confirmed_manifest_hash: str, expected_epoch: str,
    ) -> ResetResult:
        manifest = ResetManifest.model_validate_json(manifest.model_dump_json())
        expected_epoch = m.canonical_id(expected_epoch)
        if (
            confirmed_manifest_hash != manifest.manifest_hash or manifest.snapshot.target != self.target
            or manifest.snapshot.control is None
            or expected_epoch != manifest.snapshot.control.epoch
            or manifest.new_epoch == manifest.snapshot.control.epoch or manifest.purpose != "reset"
        ):
            raise ResetRefused("Exact manifest hash, target and expected old epoch must be explicitly confirmed")
        with self.db.transaction():
            server, database_id, now = self._identity()
            if (server, database_id) != (manifest.snapshot.server_identity, manifest.snapshot.database_id):
                raise ResetRefused("Connected server/database identity changed")
            prior = self._receipt(manifest)
            if prior is not None:
                self._check_receipt_baseline(prior)
                return ResetResult(receipt=prior, replayed=True)
        if manifest.catalogue_hash != self.catalogue.declaration_hash or manifest.snapshot.blockers:
            raise ResetRefused("The manifest is blocked or its declared schema changed")
        receipt = None
        try:
            with self.db.transaction():
                current = self._snapshot(exclusive=True)
                prior = self._receipt(manifest)
                if prior is not None:
                    self._check_receipt_baseline(prior)
                    return ResetResult(receipt=prior, replayed=True)
                if current.blockers or current.state_hash != manifest.snapshot.state_hash:
                    raise ResetRefused("Fresh target/object/count/state inventory differs from the approved manifest")
                server, database_id, before_delete = self._identity()
                if (server, database_id) != (current.server_identity, current.database_id):
                    raise ResetRefused("Connected server/database changed before deletion")
                observation = self._live_preflight(manifest, current)
                receipt_table = self.catalogue.table("monitoring_receipts")
                used_epochs = self.db.query(
                    f"/* monitoring-reset:used-epoch */ SELECT COUNT_BIG(*) FROM {quote_identifier(receipt_table.name)} "
                    "WITH (HOLDLOCK) WHERE epoch=? OR (operation=? AND "
                    "(JSON_VALUE(payload,'$.expected_old_epoch')=? OR JSON_VALUE(payload,'$.new_control.epoch')=?))",
                    manifest.new_epoch, RESET_OPERATION, manifest.new_epoch, manifest.new_epoch,
                )
                if len(used_epochs) != 1 or used_epochs[0][0] != 0:
                    raise ResetRefused("The proposed new epoch already appears in durable history")
                counts = {item.name: item for item in current.objects}
                deleted: dict[str, int] = {}
                retained: dict[str, int] = {}
                for table in self.catalogue.deletion_order():
                    expected_count = counts[f"dbo.{table.name}"].row_count
                    affected = self.db.execute(
                        f"/* monitoring-reset:delete:{table.name} */ DELETE FROM {quote_identifier(table.name)}",
                    )
                    if affected != expected_count:
                        raise ResetRefused("An owned-table delete did not confirm the approved row count")
                    deleted[f"dbo.{table.name}"] = affected
                receipt_counts = counts[f"dbo.{receipt_table.name}"]
                affected = self.db.execute(
                    f"/* monitoring-reset:delete-receipts */ DELETE FROM {quote_identifier(receipt_table.name)} "
                    "WHERE operation NOT IN (?,?,?)", RESET_OPERATION, BOOTSTRAP_OPERATION, CAPTURE_OPERATION,
                )
                if affected != receipt_counts.row_count - receipt_counts.retained_rows:
                    raise ResetRefused("Receipt deletion did not confirm the approved non-reset count")
                deleted[f"dbo.{receipt_table.name}"] = affected
                retained[f"dbo.{receipt_table.name}"] = receipt_counts.retained_rows
                rate_table = self.catalogue.table("rate_budget")
                retained[f"dbo.{rate_table.name}"] = counts[f"dbo.{rate_table.name}"].row_count
                for table in self.catalogue.tables:
                    if table.operation == "preserve_registration":
                        retained[f"dbo.{table.name}"] = counts[f"dbo.{table.name}"].row_count
                server, database_id, cutoff = self._identity()
                if (server, database_id) != (current.server_identity, current.database_id):
                    raise ResetRefused("Connected server/database changed during the reset transaction")
                observation = self._live_preflight(manifest, current)
                _, _, cutoff = self._identity()
                control = m.DeploymentControl(
                    tenant_id=self.target.tenant_id, epoch=manifest.new_epoch, revision=0,
                    activation_cutoff=cutoff, maintenance=True, updated_at=cutoff,
                )
                payload = control.model_dump_json()
                bootstrap_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                table = quote_identifier(self.catalogue.table("monitoring_control").name)
                affected = self.db.execute(
                    f"/* monitoring-reset:replace-control */ UPDATE {table} SET "
                    "schema_version=?,tenant_id=?,epoch=?,revision=?,activation_cutoff=?,maintenance=?,"
                    "updated_at=?,bootstrap_id=?,bootstrap_hash=?,payload=? "
                    "WHERE singleton=1 AND tenant_id=? AND epoch=? AND revision=? "
                    "AND schema_version=? AND maintenance=1 AND bootstrap_id=? AND bootstrap_hash=?",
                    control.schema_version, control.tenant_id, control.epoch, 0,
                    cutoff.replace(tzinfo=None), True, cutoff.replace(tzinfo=None),
                    manifest.operation_id, bootstrap_hash, payload,
                    self.target.tenant_id, expected_epoch, current.control.revision,
                    current.control.schema_version, current.bootstrap_id, current.bootstrap_hash,
                )
                if affected != 1:
                    raise ResetRefused("The expected old epoch/control fence was not replaced")
                receipt = ResetReceipt(
                    operation_id=manifest.operation_id, manifest_hash=manifest.manifest_hash,
                    expected_old_epoch=current.control.epoch, target=self.target,
                    server_identity=current.server_identity, database_id=current.database_id,
                    new_control=control, bootstrap_hash=bootstrap_hash,
                    preflight_challenge=observation.challenge,
                    preflight_hash=_hash(observation.model_dump(mode="json")),
                    preflight_observed_at=observation.completed_at, operator_object_id=self.target.deployer_object_id,
                    deployment_inventory_hash=observation.deployment_inventory_hash,
                    required_targets_hash=_hash([target.model_dump(mode="json") for target in current.required_action_targets]),
                    registration_names=self.catalogue.registration_names,
                    deleted_counts=deleted, retained_counts=retained, recorded_at=cutoff,
                )
                inserted = self.db.execute(
                    f"/* monitoring-reset:insert-receipt */ INSERT INTO {quote_identifier(receipt_table.name)} "
                    "(tenant_id,epoch,operation,request_hash,request_id,fingerprint,recorded_at,payload) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    self.target.tenant_id, manifest.new_epoch, RESET_OPERATION,
                    hashlib.sha256(manifest.operation_id.encode()).digest(), manifest.operation_id,
                    manifest.manifest_hash, cutoff.replace(tzinfo=None), receipt.model_dump_json(),
                )
                if inserted != 1:
                    raise ResetRefused("Terminal reset receipt insert was not confirmed")
        except SqlCommitUncertain as exc:
            try:
                with self.db.transaction():
                    server, database_id, _ = self._identity()
                    persisted = self._receipt(manifest)
                    control, _, _ = self._control()
            except (SqlUnavailable, ResetError, ValueError) as read_error:
                raise ResetCommitUncertain(
                    "Reset acknowledgement and reconciliation reads are unavailable; retain the original manifest/operation ID",
                ) from read_error
            if persisted is None or control is None or control.epoch != persisted.new_control.epoch:
                raise ResetCommitUncertain(
                    "Reset commit is not established by receipt/schema reads; no delete was retried",
                ) from exc
            if (server, database_id) != (manifest.snapshot.server_identity, manifest.snapshot.database_id):
                raise ResetCommitUncertain("Reset reconciliation reached a different server/database") from exc
            return ResetResult(receipt=persisted, replayed=True, reconciled_uncertain_commit=True)
        if receipt is None:
            raise ResetRefused("Reset did not produce a committed receipt")
        try:
            monitoring_schema.initialize_monitoring_schema(
                self.db, control=receipt.new_control, bootstrap_id=receipt.operation_id,
            )
        except Exception as exc:
            # Every post-commit failure must retain the committed disposition,
            # including optional-driver exception types not imported offline.
            logger.error("Committed reset bootstrap verification failed (%s)", type(exc).__name__)
            raise ResetVerificationFailed(receipt) from exc
        return ResetResult(receipt=receipt)


class EntraToken(Protocol):
    token: str
    expires_on: int


class DeployerCredential(Protocol):
    def get_token(self, *scopes: str, **kwargs: Any) -> EntraToken: ...


class PinnedDeployerCredential:
    """Context checks on explicitly selected operator tokens, with no credential chain."""

    def __init__(self, credential: DeployerCredential, target: ResetTarget) -> None:
        if credential is None:
            raise ResetRefused("An explicit deployer Entra credential is required")
        self.credential = credential
        self.target = target

    def get_token(self, *scopes: str, **kwargs: Any) -> EntraToken:
        if len(scopes) != 1 or scopes[0] not in {SQL_SCOPE, ARM_SCOPE, FABRIC_SCOPE, POWERBI_SCOPE, FOUNDRY_SCOPE, GRAPH_SCOPE}:
            raise ResetRefused("The operator credential was requested for an unsupported observation service")
        token = self.credential.get_token(*scopes, **kwargs)
        try:
            parts = token.token.split(".")
            if len(parts) != 3 or len(token.token) > 32_768:
                raise ValueError("SQL token context is not a bounded JWT")
            claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
            if (
                m.canonical_id(claims["tid"]) != self.target.tenant_id
                or m.canonical_id(claims["oid"]) != self.target.deployer_object_id
            ):
                raise ValueError("SQL token has another tenant/principal")
        except (ValueError, KeyError, TypeError, AttributeError, binascii.Error) as exc:
            raise ResetRefused("Operator token tenant/principal is not the explicitly selected identity") from exc
        return token


class CredentialSelection(FrozenModel):
    mode: Literal["managed-identity", "azure-cli", "broker"]
    managed_identity_client_id: m.CanonicalId | None = None
    subscription_id: m.CanonicalId | None = None
    operator_domain: str | None = Field(default=None, min_length=1, max_length=253)

    @model_validator(mode="after")
    def validate_selection(self) -> CredentialSelection:
        values = (
            self.managed_identity_client_id is not None, self.subscription_id is not None,
            self.operator_domain is not None,
        )
        if values != {
            "managed-identity": (True, False, False), "azure-cli": (False, True, False),
            "broker": (False, False, True),
        }[self.mode]:
            raise ValueError("Select one credential explicitly with only its required identity context")
        if self.operator_domain is not None and (
            self.operator_domain != self.operator_domain.strip()
            or not re.fullmatch(r"[A-Za-z0-9.-]+", self.operator_domain)
        ):
            raise ValueError("Broker mode requires an explicit account domain")
        return self


class BrokerOperatorCredential:
    """Explicit azureauth broker account selection; never changes shared az context.

    Uses the same verified token launcher as triage.cli._BrokerSqlCredential,
    extended to the read-only operator observation audiences.
    """

    def __init__(self, target: ResetTarget, domain: str) -> None:
        self.target, self.domain = target, domain

    def get_token(self, *scopes: str, **kwargs: Any) -> EntraToken:
        from azure.core.credentials import AccessToken

        resources = {
            SQL_SCOPE: "https://database.windows.net/", ARM_SCOPE: "https://management.azure.com/",
            FABRIC_SCOPE: "https://api.fabric.microsoft.com", POWERBI_SCOPE: "https://analysis.windows.net/powerbi/api",
            FOUNDRY_SCOPE: "https://ai.azure.com",
            GRAPH_SCOPE: "https://graph.microsoft.com",
        }
        if len(scopes) != 1 or scopes[0] not in resources:
            raise ResetRefused("Unsupported broker token audience")
        result = subprocess.run(
            ["azureauth", "aad", "--resource", resources[scopes[0]],
             "--client", "04b07795-8ddb-461a-bbee-02f9e1bf7b46",
             "--tenant", self.target.tenant_id, "--domain", self.domain,
             "--mode", "broker", "--output", "token", "--verbosity", "error", "--timeout", "2"],
            capture_output=True, text=True, timeout=150, check=False,
        )
        if result.returncode != 0:
            raise ResetRefused("Explicit operator broker authentication failed; no fallback was attempted")
        token = result.stdout.strip()
        try:
            parts = token.split(".")
            if len(parts) != 3:
                raise ValueError("Broker did not return one token")
            claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
            expiry = int(claims["exp"])
            if expiry <= datetime.now(UTC).timestamp():
                raise ValueError("Broker returned an expired token")
        except (ValueError, TypeError, KeyError, binascii.Error) as exc:
            raise ResetRefused("Operator broker returned invalid token metadata") from exc
        return AccessToken(token, expiry)


def create_database(target: ResetTarget, selection: CredentialSelection) -> AzureSqlDatabase:
    if selection.mode == "managed-identity":
        from azure.identity import ManagedIdentityCredential

        credential = ManagedIdentityCredential(client_id=selection.managed_identity_client_id)
    elif selection.mode == "azure-cli":
        from azure.identity import AzureCliCredential

        credential = AzureCliCredential(subscription=selection.subscription_id, process_timeout=60)
    else:
        credential = BrokerOperatorCredential(target, selection.operator_domain)
    return AzureSqlDatabase(
        server=target.server, database=target.database,
        credential=PinnedDeployerCredential(credential, target),
    )


def _read_file(path: str) -> str:
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_FILE_BYTES + 1)
    if len(raw) > MAX_FILE_BYTES:
        raise ResetRefused("An operator input file exceeds its size bound")
    return raw.decode("utf-8")


def main(
    argv: Sequence[str] | None = None, *,
    database_factory: Callable[[ResetTarget, CredentialSelection], AzureSqlDatabase] = create_database,
    observer_factory: Callable[[ObservationProfile, PinnedDeployerCredential], LiveOperatorObserver] = LiveOperatorObserver,
    deployment_inventory_reader_factory: Callable[
        [AzureSqlDatabase, ResetTarget, ResetCatalogue], DeploymentInventoryReader
    ] | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--deployer-object-id", required=True)
    parser.add_argument("--credential", choices=("managed-identity", "azure-cli", "broker"), required=True)
    parser.add_argument("--managed-identity-client-id")
    parser.add_argument("--subscription-id")
    parser.add_argument("--operator-domain")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--plan-initialization", action="store_true")
    mode.add_argument("--initialize", action="store_true")
    parser.add_argument("--manifest")
    parser.add_argument("--confirm-manifest-hash")
    parser.add_argument("--expected-epoch")
    parser.add_argument("--expected-uninitialized", action="store_true")
    parser.add_argument("--preflight-config")
    parser.add_argument("--registration-names", help="Optional JSON object matching RegistrationNames, not a kernel table map")
    parser.add_argument("--allow-identity-association-preview", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    if args.execute and not all((
        args.manifest, args.confirm_manifest_hash, args.expected_epoch,
    )):
        parser.error("Reset execution requires an exact manifest/hash/epoch")
    if args.initialize and not all((args.manifest, args.confirm_manifest_hash, args.expected_uninitialized)):
        parser.error("Initial bootstrap requires its exact manifest/hash and --expected-uninitialized")
    if not args.execute and not args.initialize and any((
        args.manifest, args.confirm_manifest_hash, args.expected_epoch, args.expected_uninitialized,
    )):
        parser.error("Execution inputs do not select execution; supply --execute explicitly")
    if (args.initialize or args.plan_initialization) and (args.expected_epoch or args.preflight_config):
        parser.error("Initial bootstrap is schema-only and does not use reset/live-observer arguments")
    committed_result: ResetResult | InitializationResult | None = None
    observer: LiveOperatorObserver | None = None
    collector: AzureDeploymentDiscovery | None = None
    try:
        if args.output and Path(args.output).exists():
            raise ResetRefused("Operator output must use a new file; no existing approval/manifest is overwritten")
        target = ResetTarget(
            server=args.server, database=args.database, tenant_id=args.tenant_id,
            deployer_object_id=args.deployer_object_id,
        )
        selection = CredentialSelection(
            mode=args.credential, managed_identity_client_id=args.managed_identity_client_id,
            subscription_id=args.subscription_id, operator_domain=args.operator_domain,
        )
        document = ManifestDocument.model_validate_json(_read_file(args.manifest)) if args.manifest else None
        manifest = document.manifest if document is not None else None
        profile = ObservationProfile.model_validate_json(_read_file(args.preflight_config)) if args.preflight_config else None
        if profile is None and manifest is not None:
            profile = manifest.preflight_profile
        names = RegistrationNames(**json.loads(_read_file(args.registration_names))) if args.registration_names else DEFAULT_REGISTRATION_NAMES
        db = database_factory(target, selection)
        if profile is not None:
            observer = observer_factory(profile, db._credential)
        reader = None
        if not (args.initialize or args.plan_initialization):
            if deployment_inventory_reader_factory is not None:
                reader = deployment_inventory_reader_factory(db, target, build_catalogue(names))
            else:
                from triage.monitoring.deployment_registry import (
                    RegisteredDeploymentInventoryReader,
                )

                collector = AzureDeploymentDiscovery(
                    db._credential, target,
                    allow_identity_association_preview=args.allow_identity_association_preview,
                )
                reader = RegisteredDeploymentInventoryReader(collector, names=names)
        operator = SqlResetOperator(
            db, target, observer=observer, deployment_inventory_reader=reader, registration_names=names,
        )
        if manifest is None:
            result = operator.plan_initialization() if args.plan_initialization else operator.plan()
            output = ManifestDocument(manifest_hash=result.manifest_hash, manifest=result).model_dump(mode="json")
        elif args.initialize:
            result = operator.initialize(
                manifest, confirmed_manifest_hash=args.confirm_manifest_hash,
                expected_uninitialized=args.expected_uninitialized,
            )
            committed_result = result
            output = {"mode": "initialize", **result.model_dump(mode="json")}
        else:
            result = operator.execute(
                manifest, confirmed_manifest_hash=args.confirm_manifest_hash,
                expected_epoch=args.expected_epoch,
            )
            committed_result = result
            output = {"mode": "execute", **result.model_dump(mode="json")}
        encoded = json.dumps(output, indent=2, ensure_ascii=True)
        if args.output:
            with Path(args.output).open("x", encoding="utf-8") as stream:
                stream.write(encoded + "\n")
        else:
            print(encoded)
        return 0
    except ResetVerificationFailed as exc:
        print(json.dumps({"error": str(exc), "committed_receipt": exc.receipt.model_dump(mode="json")}), file=sys.stderr)
    except (ResetError, DeploymentError, SqlUnavailable, ValidationError, OSError, UnicodeError, ValueError, TypeError) as exc:
        # Validation/driver exceptions can contain input values. Never print
        # their payloads, connection diagnostics or credentials.
        if committed_result is not None:
            print(json.dumps({
                "error": type(exc).__name__,
                "detail": "Reset already committed; result output failed. Reconcile the original operation, do not reset again.",
                "committed_receipt": committed_result.receipt.model_dump(mode="json"),
            }), file=sys.stderr)
        else:
            detail = str(exc) if isinstance(exc, (ResetError, DeploymentError)) else "Operator input or SQL operation failed"
            print(json.dumps({"error": type(exc).__name__, "detail": detail}), file=sys.stderr)
    finally:
        if observer is not None:
            observer.close()
        if collector is not None:
            collector.close()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
