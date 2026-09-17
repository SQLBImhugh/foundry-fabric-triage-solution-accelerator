"""Protected deployment registration and the concrete transaction-bound reader.

Preparation is read-only and may report legacy authority blockers. Acceptance
recollects live bindings after role retirement; a caller file is only an exact
confirmation document. Header, child rows and protected capture commit together.
An uncertain acknowledgement is reconciled by the original request, never by a
second unconditioned append. Registration and captures survive prototype resets.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import Field

from triage.monitoring import models as m
from triage.monitoring.deployment_authority import AuthoritySnapshot, read_authority
from triage.monitoring.deployment_contracts import (
    CAPTURE_OPERATION,
    DeploymentConflict,
    DeploymentError,
    DeploymentUncertain,
    DeploymentWriterInventory,
    DiscoveryCapture,
    OperatorModel,
    ResetTarget,
    WriterBinding,
    WriterSpec,
    canonical_id,
    fingerprint,
)
from triage.monitoring.deployment_discovery import AzureDeploymentDiscovery
from triage.monitoring.deployment_schema import (
    DEFAULT_REGISTRATION_NAMES,
    RegistrationNames,
    object_catalogue,
    schema_statements,
)
from triage.store.azure_sql import (
    AzureSqlDatabase,
    SqlCommitUncertain,
    SqlUnavailable,
    quote_identifier,
)

HEADER_COLUMNS = (
    "revision", "binding_id", "registration_request_id", "request_fingerprint", "tenant_id",
    "sql_server", "sql_database", "server_identity", "database_id", "reset_catalogue_hash",
    "kernel_contract_hash", "authority_snapshot_hash", "discovery_capture_id", "discovery_capture_hash",
    "writer_rows_hash", "writer_count", "recorded_at", "registrar_object_id", "registrar_sql_principal_id",
)
WRITER_COLUMNS = (
    "writer_id", "writer_kind", "resource_id", "project_endpoint", "agent_name",
    "identity_client_id", "identity_object_id", "expected_sql_sid", "invokes_writer_id",
    "configured_sql_server", "configured_sql_database", "resource_binding_hash", "invocation_binding_hash",
)
MAX_REVISIONS = 10_000


def _utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise DeploymentError("SQL did not supply a timestamp")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _clock(database: AzureSqlDatabase) -> datetime:
    rows = database.query("/* deployment-registry:clock */ SELECT SYSUTCDATETIME()")
    if len(rows) != 1 or len(rows[0]) != 1:
        raise DeploymentError("SQL capture clock is unreadable")
    return _utc(rows[0][0])


class RegistrationPlan(OperatorModel):
    operation_id: m.CanonicalId
    binding_id: m.OpaqueId
    expected_revision: int = Field(ge=0, strict=True)
    target: ResetTarget
    server_identity: str
    database_id: int = Field(ge=1, strict=True)
    catalogue_hash: m.Fingerprint
    kernel_hash: m.Fingerprint
    authority_hash: m.Fingerprint
    capture: DiscoveryCapture
    blockers: tuple[str, ...]

    @property
    def manifest_hash(self) -> str:
        return fingerprint(self.model_dump(mode="json"), domain="deployment.registration.request.v1")


class RegistrationReceipt(OperatorModel):
    operation_id: m.CanonicalId
    request_fingerprint: m.Fingerprint
    binding_id: m.OpaqueId
    revision: int = Field(ge=1, strict=True)
    capture_hash: m.Fingerprint
    writer_rows_hash: m.Fingerprint
    authority_hash: m.Fingerprint
    recorded_at: m.UtcDateTime


class RegistrationResult(OperatorModel):
    receipt: RegistrationReceipt
    replayed: bool = False
    reconciled_uncertain_commit: bool = False


def _header(row: Any) -> dict:
    if len(row) != len(HEADER_COLUMNS):
        raise DeploymentError("Registration header has an incompatible shape")
    result = dict(zip(HEADER_COLUMNS, row, strict=True))
    for field in ("registration_request_id", "tenant_id", "registrar_object_id"):
        result[field] = canonical_id(str(result[field]))
    for field in (
        "request_fingerprint", "reset_catalogue_hash", "kernel_contract_hash",
        "authority_snapshot_hash", "discovery_capture_hash", "writer_rows_hash",
    ):
        if not isinstance(result[field], str) or len(result[field]) != 64 or any(char not in "0123456789abcdef" for char in result[field]):
            raise DeploymentError("Registration header fingerprint is malformed")
    if (
        type(result["revision"]) is not int or result["revision"] < 1
        or type(result["writer_count"]) is not int or not 1 <= result["writer_count"] <= 100
    ):
        raise DeploymentError("Registration revision or structural writer count is unsupported")
    result["recorded_at"] = _utc(result["recorded_at"])
    return result


def _receipt(header: dict) -> RegistrationReceipt:
    return RegistrationReceipt(
        operation_id=header["registration_request_id"], request_fingerprint=header["request_fingerprint"],
        binding_id=header["binding_id"], revision=header["revision"],
        capture_hash=header["discovery_capture_hash"], writer_rows_hash=header["writer_rows_hash"],
        authority_hash=header["authority_snapshot_hash"], recorded_at=header["recorded_at"],
    )


class RegistrationRepository:
    """SQL primitives only; never begin/commit a transaction or call cloud APIs."""

    def __init__(self, names: RegistrationNames = DEFAULT_REGISTRATION_NAMES) -> None:
        self.names = names

    def exists(self, database: AzureSqlDatabase) -> bool:
        rows = database.query(
            "/* deployment-registry:exists */ SELECT OBJECT_ID(?, 'U')", f"dbo.{self.names.registration}",
        )
        if len(rows) != 1:
            raise DeploymentError("Registration schema existence is unverified")
        return rows[0][0] is not None

    def by_request(self, database: AzureSqlDatabase, request_id: str) -> dict | None:
        if not self.exists(database):
            return None
        rows = database.query(
            f"/* deployment-registry:request */ SELECT {','.join(HEADER_COLUMNS)} "
            f"FROM {quote_identifier(self.names.registration)} WITH (UPDLOCK,HOLDLOCK) "
            "WHERE registration_request_id=?", request_id,
        )
        if len(rows) > 1:
            raise DeploymentError("Registration request is duplicated")
        return _header(rows[0]) if rows else None

    def latest(self, database: AzureSqlDatabase) -> dict | None:
        if not self.exists(database):
            return None
        rows = database.query(
            f"/* deployment-registry:latest */ SELECT TOP ({MAX_REVISIONS + 1}) {','.join(HEADER_COLUMNS)} "
            f"FROM {quote_identifier(self.names.registration)} WITH (UPDLOCK,HOLDLOCK) ORDER BY revision DESC",
        )
        if len(rows) > MAX_REVISIONS:
            raise DeploymentError("Registration history exceeds the reviewed bounded reader")
        if not rows:
            return None
        headers = [_header(row) for row in rows]
        if (
            [row["revision"] for row in headers] != list(range(len(headers), 0, -1))
            or len({row["binding_id"] for row in headers}) != 1
            or len({row["registration_request_id"] for row in headers}) != len(headers)
        ):
            raise DeploymentError("Registration has multiple series, missing revisions or reused requests")
        return headers[0]

    def capture(self, database: AzureSqlDatabase, target: ResetTarget, catalogue: Any, header: dict) -> DiscoveryCapture:
        table = quote_identifier(catalogue.table("monitoring_receipts").name)
        rows = database.query(
            f"/* deployment-registry:capture */ SELECT request_id,fingerprint,payload,recorded_at FROM {table} "
            "WITH (HOLDLOCK) WHERE tenant_id=? AND operation=? AND request_hash=?",
            target.tenant_id, CAPTURE_OPERATION, hashlib.sha256(header["discovery_capture_id"].encode()).digest(),
        )
        if len(rows) != 1 or len(rows[0]) != 4:
            raise DeploymentError("Protected discovery capture is missing or duplicated across epochs")
        row = rows[0]
        try:
            capture = DiscoveryCapture.model_validate_json(row[2])
        except (TypeError, ValueError) as exc:
            raise DeploymentError("Protected discovery capture is malformed") from exc
        if (
            row[0] != header["discovery_capture_id"] or row[0] != capture.capture_id
            or row[1] != header["discovery_capture_hash"] or row[1] != capture.capture_hash
            or capture.rows_hash != header["writer_rows_hash"] or len(capture.writers) != header["writer_count"]
            or capture.operator_object_id != header["registrar_object_id"]
            or (capture.tenant_id, capture.sql_server, capture.sql_database) != target.database_binding
            or _utc(row[3]) != header["recorded_at"] or capture.gaps
        ):
            raise DeploymentError("Protected capture content, request or database binding differs")
        return capture

    def bindings(
        self, database: AzureSqlDatabase, header: dict, capture: DiscoveryCapture, authority: AuthoritySnapshot,
    ) -> tuple[WriterBinding, ...]:
        rows = database.query(
            f"/* deployment-registry:projection */ SELECT {','.join(WRITER_COLUMNS)},"
            "sql_principal_id,sql_principal_type,sql_principal_sid "
            f"FROM {quote_identifier(self.names.read_projection)} WHERE revision=? ORDER BY writer_id,sql_principal_id",
            header["revision"],
        )
        if len(rows) != header["writer_count"] or any(len(row) != len(WRITER_COLUMNS) + 3 for row in rows):
            raise DeploymentError("Protected writer projection contains missing, duplicate or unmapped rows")
        expected = {item.writer.writer_id: item for item in capture.writers}
        principal_by_sid = {item.sid: item.principal_id for item in authority.writers}
        actual_sids, result = set(), []
        for row in rows:
            values = dict(zip(WRITER_COLUMNS, row[:len(WRITER_COLUMNS)], strict=True))
            for field in ("identity_client_id", "identity_object_id"):
                values[field] = canonical_id(str(values[field]))
            sid = values["expected_sql_sid"]
            if sid is not None:
                if not isinstance(sid, (bytes, bytearray, memoryview)) or len(sid) != 16:
                    raise DeploymentError("Registered SQL identity SID is malformed")
                values["expected_sql_sid"] = bytes(sid).hex()
            captured = expected.pop(values["writer_id"], None)
            if captured is None or values != captured.registration_row:
                raise DeploymentError("Registered child rows differ from the protected discovery capture")
            principal_id, principal_type, principal_sid = row[-3:]
            if sid is not None:
                sid = bytes(sid)
                if (
                    sid != UUID(values["identity_client_id"]).bytes_le or principal_type != "E"
                    or not isinstance(principal_sid, (bytes, bytearray, memoryview))
                    or bytes(principal_sid) != sid or principal_by_sid.get(sid) != principal_id
                ):
                    raise DeploymentError("Registered direct identity does not match current SQL SID/authority")
                actual_sids.add(sid)
            elif any(value is not None for value in row[-3:]):
                raise DeploymentError("Indirect writer unexpectedly joined SQL authority")
            result.append(WriterBinding(
                writer=WriterSpec(
                    writer_id=values["writer_id"], kind=values["writer_kind"], resource_id=values["resource_id"],
                    project_endpoint=values["project_endpoint"], agent_name=values["agent_name"],
                ),
                identity_client_id=values["identity_client_id"], identity_object_id=values["identity_object_id"],
                sql_principal_id=principal_id, invokes_writer_id=values["invokes_writer_id"],
            ))
        if expected or actual_sids != set(principal_by_sid) or {
            bytes.fromhex(value) for value in capture.sql_identity_sids
        } != actual_sids:
            raise DeploymentError("Registered resources and independently enumerated SQL mutators differ")
        return tuple(result)


def inventory_from_capture(
    capture: DiscoveryCapture, authority: AuthoritySnapshot, target: ResetTarget,
    catalogue: Any, *, binding_id: str, revision: int, request_id: str,
) -> DeploymentWriterInventory:
    authority.require_protected()
    principals = {item.sid.hex(): item.principal_id for item in authority.writers}
    if {item.expected_sql_sid for item in capture.writers if item.expected_sql_sid} != set(principals):
        raise DeploymentError("Captured direct bindings omit or add a SQL mutator")
    return DeploymentWriterInventory(
        binding_id=binding_id, revision=revision, target=target,
        server_identity=authority.server_identity, database_id=authority.database_id,
        catalogue_hash=catalogue.declaration_hash, observed_at=authority.observed_at,
        writers=tuple(WriterBinding(
            writer=item.writer, identity_client_id=item.identity_client_id,
            identity_object_id=item.identity_object_id,
            sql_principal_id=principals[item.expected_sql_sid] if item.expected_sql_sid else None,
            invokes_writer_id=item.invokes_writer_id,
        ) for item in capture.writers),
        kernel_contract_hash=authority.kernel_hash, authority_snapshot_hash=authority.snapshot_hash,
        registration_request_id=request_id, discovery_capture_hash=capture.capture_hash,
        current_binding_hash=capture.binding_hash, resource_observed_at=capture.finished_at,
    )


class RegisteredDeploymentInventoryReader:
    """The production reset reader; no profile or caller completeness input."""

    def __init__(
        self, collector: AzureDeploymentDiscovery, *, names: RegistrationNames = DEFAULT_REGISTRATION_NAMES,
        max_registration_age_seconds: int = 3_600,
    ) -> None:
        if type(max_registration_age_seconds) is not int or not 120 <= max_registration_age_seconds <= 3_600:
            raise ValueError("Protected registration age must be bounded to 120..3600 seconds")
        self.collector, self.repository = collector, RegistrationRepository(names)
        self.max_age = max_registration_age_seconds

    def read(self, database: AzureSqlDatabase, target: ResetTarget, catalogue: Any) -> DeploymentWriterInventory:
        if self.collector.target != target:
            raise DeploymentError("Deployment reader collector belongs to a different pinned operator/database")
        authority = read_authority(database, target, catalogue, names=self.repository.names)
        authority.require_protected()
        header = self.repository.latest(database)
        if header is None:
            raise DeploymentError("Protected deployment registration is absent; run explicit operator preparation/acceptance")
        if (
            (header["tenant_id"], header["sql_server"], header["sql_database"]) != target.database_binding
            or header["server_identity"] != authority.server_identity or header["database_id"] != authority.database_id
            or header["reset_catalogue_hash"] != catalogue.declaration_hash
            or header["kernel_contract_hash"] != authority.kernel_hash
            or header["authority_snapshot_hash"] != authority.snapshot_hash
        ):
            raise DeploymentError("Latest database-wide registration differs from current target/schema/SQL authority")
        capture = self.repository.capture(database, target, catalogue, header)
        bindings = self.repository.bindings(database, header, capture, authority)
        if not 0 <= (authority.observed_at - header["recorded_at"]).total_seconds() <= self.max_age:
            raise DeploymentError("Protected registration expired; collect and accept a fresh revision")
        fresh = self.collector.collect(authority)
        now = _clock(database)
        if (
            fresh.binding_hash != capture.binding_hash or fresh.rows_hash != capture.rows_hash
            or fresh.gaps or any(item.state not in {"stopped", "disabled", "inactive"} for item in fresh.writers)
            or not -30 <= (now - fresh.finished_at).total_seconds() <= 120
        ):
            raise DeploymentError("Fresh deployment/identity reuse bindings changed, are stale or are not quiescent")
        return DeploymentWriterInventory(
            binding_id=header["binding_id"], revision=header["revision"], target=target,
            server_identity=authority.server_identity, database_id=authority.database_id,
            catalogue_hash=catalogue.declaration_hash, observed_at=now, writers=bindings,
            kernel_contract_hash=authority.kernel_hash, authority_snapshot_hash=authority.snapshot_hash,
            registration_request_id=header["registration_request_id"], discovery_capture_hash=capture.capture_hash,
            current_binding_hash=fresh.binding_hash, resource_observed_at=fresh.finished_at,
        )


class DeploymentRegistrationOperator:
    """Explicit deployer workflow. The acceptance callback must perform real effect reads."""

    def __init__(
        self, database: AzureSqlDatabase, target: ResetTarget, catalogue: Any,
        collector: AzureDeploymentDiscovery, *, names: RegistrationNames = DEFAULT_REGISTRATION_NAMES,
        verify_quiescence: Callable[[RegistrationPlan, DiscoveryCapture, AuthoritySnapshot], None] | None = None,
    ) -> None:
        self.db, self.target, self.catalogue, self.collector = database, target, catalogue, collector
        if (getattr(database, "_server", None), getattr(database, "_database", None)) != (target.server, target.database) or (
            getattr(getattr(database, "_credential", None), "target", None) != target or collector.target != target
            or catalogue.registration_names != names
        ):
            raise DeploymentError("Use one explicitly pinned database/collector operator target")
        self.repository = RegistrationRepository(names)
        self.verify_quiescence = verify_quiescence

    def prepare(self, *, binding_id: str, request_id: str | None = None) -> RegistrationPlan:
        with self.db.transaction():
            authority = read_authority(self.db, self.target, self.catalogue, names=self.repository.names)
            latest = self.repository.latest(self.db)
            if latest is not None and latest["binding_id"] != binding_id:
                raise DeploymentConflict("Registration binding series cannot be replaced")
            operation = m.canonical_id(request_id) if request_id else str(uuid4())
            capture = self.collector.collect(authority, capture_id=operation)
            blockers = set(authority.gaps)
            if any(item.state not in {"stopped", "disabled", "inactive"} for item in capture.writers):
                blockers.add("writers_not_quiescent")
            return RegistrationPlan(
                operation_id=operation, binding_id=binding_id,
                expected_revision=latest["revision"] if latest else 0, target=self.target,
                server_identity=authority.server_identity, database_id=authority.database_id,
                catalogue_hash=self.catalogue.declaration_hash, kernel_hash=authority.kernel_hash,
                authority_hash=authority.snapshot_hash, capture=capture, blockers=tuple(sorted(blockers)),
            )

    def _original(self, plan: RegistrationPlan) -> RegistrationReceipt | None:
        identity = self.db.query(
            "/* deployment-registry:identity */ SELECT CONVERT(NVARCHAR(512),SERVERPROPERTY('ServerName')),DB_NAME(),DB_ID()",
        )
        if len(identity) != 1 or tuple(identity[0]) != (plan.server_identity, self.target.database, plan.database_id):
            raise DeploymentError("Registration reconciliation reached another SQL database")
        header = self.repository.by_request(self.db, plan.operation_id)
        if header is None:
            return None
        if (
            header["request_fingerprint"] != plan.manifest_hash
            or header["binding_id"] != plan.binding_id or header["revision"] != plan.expected_revision + 1
            or (header["tenant_id"], header["sql_server"], header["sql_database"]) != self.target.database_binding
            or header["reset_catalogue_hash"] != plan.catalogue_hash
            or header["kernel_contract_hash"] != plan.kernel_hash
            or header["authority_snapshot_hash"] != plan.authority_hash
            or header["writer_rows_hash"] != plan.capture.rows_hash
        ):
            raise DeploymentConflict("Registration request identity was reused with different content")
        capture = self.repository.capture(self.db, self.target, self.catalogue, header)
        if capture.binding_hash != plan.capture.binding_hash:
            raise DeploymentConflict("Original registration receipt lost its approved deployment binding")
        return _receipt(header)

    def reconcile(self, plan: RegistrationPlan) -> RegistrationResult:
        if plan.target != self.target:
            raise DeploymentError("Registration plan belongs to another operator/database")
        with self.db.transaction():
            prior = self._original(plan)
        if prior is None:
            raise DeploymentUncertain("Original registration is not established; no append was retried")
        return RegistrationResult(receipt=prior, replayed=True)

    def accept(self, plan: RegistrationPlan, *, confirmed_manifest_hash: str) -> RegistrationResult:
        plan = RegistrationPlan.model_validate_json(plan.model_dump_json())
        if plan.target != self.target or confirmed_manifest_hash != plan.manifest_hash:
            raise DeploymentError("Accept requires the exact prepared operator/database manifest hash")
        try:
            with self.db.transaction():
                prior = self._original(plan)
                if prior is not None:
                    return RegistrationResult(receipt=prior, replayed=True)
                if plan.blockers or self.verify_quiescence is None:
                    raise DeploymentError("Preparation is blocked or the trusted live effect observer is absent")
                authority = read_authority(self.db, self.target, self.catalogue, names=self.repository.names)
                authority.require_protected()
                if (
                    authority.snapshot_hash != plan.authority_hash or authority.kernel_hash != plan.kernel_hash
                    or self.catalogue.declaration_hash != plan.catalogue_hash
                ):
                    raise DeploymentConflict("SQL authority/schema changed; prepare again after reviewed cutover")
                latest = self.repository.latest(self.db)
                if (latest["revision"] if latest else 0) != plan.expected_revision or latest and latest["binding_id"] != plan.binding_id:
                    raise DeploymentConflict("Registration expected prior revision or series changed")
                capture = self.collector.collect(authority, capture_id=plan.operation_id)
                if capture.binding_hash != plan.capture.binding_hash or any(
                    item.state not in {"stopped", "disabled", "inactive"} for item in capture.writers
                ):
                    raise DeploymentConflict("Actual deployment bindings changed or writers are not stopped")
                # This is an injected code boundary, not a caller boolean or a
                # saved receipt. The operator CLI wires the real reset observer.
                self.verify_quiescence(plan, capture, authority)
                final_authority = read_authority(self.db, self.target, self.catalogue, names=self.repository.names)
                final_authority.require_protected()
                if final_authority.snapshot_hash != authority.snapshot_hash:
                    raise DeploymentConflict("SQL authority changed during protected acceptance")
                now = _clock(self.db)
                if not -30 <= (now - capture.finished_at).total_seconds() <= 120:
                    raise DeploymentError("Live deployment capture expired during effect reconciliation")
                control = self.db.query(
                    f"/* deployment-registry:control */ SELECT tenant_id,epoch,maintenance FROM "
                    f"{quote_identifier(self.catalogue.table('monitoring_control').name)} WITH (UPDLOCK,HOLDLOCK) WHERE singleton=1",
                )
                if len(control) != 1 or control[0][0] != self.target.tenant_id or control[0][2] != 1:
                    raise DeploymentError("Protected acceptance requires the exact maintenance baseline")
                epoch = m.canonical_id(control[0][1])
                header = dict(zip(HEADER_COLUMNS, (
                    plan.expected_revision + 1, plan.binding_id, plan.operation_id, plan.manifest_hash,
                    self.target.tenant_id, self.target.server, self.target.database, authority.server_identity,
                    authority.database_id, self.catalogue.declaration_hash, authority.kernel_hash, authority.snapshot_hash,
                    capture.capture_id, capture.capture_hash, capture.rows_hash, len(capture.writers),
                    now.replace(tzinfo=None), self.target.deployer_object_id, authority.operator_principal_id,
                ), strict=True))
                changed = self.db.execute(
                    f"/* deployment-registry:insert-header */ INSERT INTO {quote_identifier(self.repository.names.registration)} "
                    f"({','.join(HEADER_COLUMNS)}) VALUES ({','.join('?' for _ in HEADER_COLUMNS)})",
                    *(header[key] for key in HEADER_COLUMNS),
                )
                if changed != 1:
                    raise DeploymentConflict("Registration header insert was not confirmed")
                pending = {item.writer.writer_id: item for item in capture.writers}
                while pending:
                    ready = [key for key, item in pending.items() if item.invokes_writer_id not in pending]
                    if not ready:
                        raise DeploymentError("Registration invocation graph is cyclic")
                    for key in sorted(ready):
                        item = pending.pop(key)
                        row = item.registration_row
                        row["expected_sql_sid"] = bytes.fromhex(row["expected_sql_sid"]) if row["expected_sql_sid"] else None
                        changed = self.db.execute(
                            f"/* deployment-registry:insert-writer */ INSERT INTO {quote_identifier(self.repository.names.writers)} "
                            f"(revision,{','.join(WRITER_COLUMNS)}) VALUES ({','.join('?' for _ in range(len(WRITER_COLUMNS) + 1))})",
                            header["revision"], *(row[name] for name in WRITER_COLUMNS),
                        )
                        if changed != 1:
                            raise DeploymentConflict("Registration child insert was not confirmed")
                changed = self.db.execute(
                    f"/* deployment-registry:insert-capture */ INSERT INTO "
                    f"{quote_identifier(self.catalogue.table('monitoring_receipts').name)} "
                    "(tenant_id,epoch,operation,request_hash,request_id,fingerprint,recorded_at,payload) VALUES (?,?,?,?,?,?,?,?)",
                    self.target.tenant_id, epoch, CAPTURE_OPERATION, hashlib.sha256(capture.capture_id.encode()).digest(),
                    capture.capture_id, capture.capture_hash, now.replace(tzinfo=None), capture.model_dump_json(),
                )
                if changed != 1:
                    raise DeploymentConflict("Protected capture insert was not confirmed")
                saved = self.repository.by_request(self.db, plan.operation_id)
                if saved is None:
                    raise DeploymentConflict("Registration readback did not find its exact request")
                self.repository.bindings(self.db, saved, capture, authority)
                receipt = _receipt(saved)
        except SqlCommitUncertain:
            try:
                reconciled = self.reconcile(plan)
            except (SqlUnavailable, DeploymentError) as read_error:
                raise DeploymentUncertain("Registration commit is uncertain; retain and reconcile the original request") from read_error
            return reconciled.model_copy(update={"reconciled_uncertain_commit": True})
        return RegistrationResult(receipt=receipt)


def install_registration(
    database: AzureSqlDatabase, catalogue: Any, *, confirmed_ddl_hash: str,
    names: RegistrationNames = DEFAULT_REGISTRATION_NAMES,
) -> None:
    """Explicit, schema-only install. Existing objects are verified, never altered.

    Empty tables may be created while legacy roles still exist. They are NOT
    authority until the acceptance gate proves those roles have been retired.
    This function grants nothing and never installs or updates the kernel.
    """
    declarations = object_catalogue(names)
    if confirmed_ddl_hash != fingerprint(declarations, domain="deployment.registration.ddl.v1"):
        raise DeploymentError("Registration DDL requires its exact object-catalogue hash")
    with database.transaction():
        target = getattr(getattr(database, "_credential", None), "target", None)
        if not isinstance(target, ResetTarget) or (
            getattr(database, "_server", None), getattr(database, "_database", None)
        ) != (target.server, target.database):
            raise DeploymentError("Registration installation requires the explicitly pinned deployer database")
        authority = read_authority(database, target, catalogue, names=names)
        if set(authority.gaps) & {"unreviewed_trigger_authority", "autonomous_sql_writer"}:
            raise DeploymentError("Unreviewed SQL triggers/activation prevent a bounded schema-only installation")
        control = database.query(
            f"/* deployment-registry:install-control */ SELECT tenant_id,schema_version,maintenance FROM "
            f"{quote_identifier(catalogue.table('monitoring_control').name)} WITH (HOLDLOCK) WHERE singleton=1",
        )
        if len(control) != 1 or tuple(control[0]) != (target.tenant_id, m.MONITORING_SCHEMA_VERSION, True):
            raise DeploymentError("Initialize the exact tenant's current empty maintenance baseline explicitly before registration DDL")
        for declaration, ddl in zip(declarations, schema_statements(names), strict=True):
            name = declaration["name"]
            rows = database.query(
                "/* deployment-registry:install-object */ SELECT o.type,"
                "CONVERT(VARCHAR(64),HASHBYTES('SHA2_256',LTRIM(RTRIM(REPLACE(OBJECT_DEFINITION(o.object_id),CHAR(13)+CHAR(10),CHAR(10))))),2) "
                "FROM sys.objects o WHERE o.object_id=OBJECT_ID(?)", name,
            )
            if rows:
                if declaration["kind"] == "view":
                    if len(rows) != 1 or rows[0][0] != "V" or (rows[0][1] or "").lower() != declaration["native_sha256"]:
                        raise DeploymentConflict("Existing operator projection is incompatible; no ALTER was performed")
                else:
                    table = catalogue.table(declaration["logical_name"])
                    actual = database.query(
                        "/* deployment-registry:install-columns */ SELECT c.name,t.name,c.max_length,c.scale,c.is_nullable "
                        "FROM sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id "
                        "WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id", name,
                    )
                    expected = [(c.name, c.data_type, c.max_length, c.scale, c.nullable) for c in table.columns]
                    if rows[0][0] != "U" or [tuple(row) for row in actual] != expected:
                        raise DeploymentConflict("Existing registration table is incompatible; no upgrade was performed")
                continue
            database.execute(ddl)
