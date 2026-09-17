"""Deployment-only Azure SQL bootstrap from a reviewed, immutable image.

Default: read-only SQL preflight. This does not generate SQL, discover identities,
change the server administrator, initialize application data or prove runtime
permissions. The parent exports the FINAL current-source schema/permissions,
reviews each batch and its metadata expectations, then builds the image.

The UTF-8 bundle has version=1, operation_id (UUID), ddl_owner="dbo", target,
identity, source_files (relative POSIX path -> SHA-256), batches (path/sha256)
and checks (kind/name/argument/expected rows). Target contains server, database,
application_database and kind ("application" or "proof"). An optional private_ip
pins an explicitly private deployment; public networking is the default. Proof
uses application_database + "-proof". Identity contains tenant_id, client_id,
object_id.
Checks use only the fixed SELECTs below, never caller-supplied preflight SQL.
Every batch is one driver batch, without GO; do not concatenate module DDL.

Bundle paths resolve inside this script's parent repository/image directory.
Include every src/**/*.py file and this runner in source_files. The isolated
image command is python3 -I -B /opt/state-sql-bootstrap/scripts/bootstrap_azure_sql.py.
It imports ONLY the hash-checked source tree, not an SDK base image's application.
The bundle hash binds target, identity, source hashes, ordered SQL and readbacks;
--operation-id separately pins the original operation across every invocation.

--mode apply additionally requires --approve-fingerprint equal to --bundle-sha256.
It first commits an operator-only started receipt, then executes all batches,
checks metadata and completes that receipt in one AzureSqlDatabase.transaction().
A started receipt blocks ALL further applies, even with a different operation ID.
After any lost acknowledgement or timeout, quiesce the job and use --mode reconcile
with the ORIGINAL inputs. Reconcile never retries SQL or releases a pending fence.
Committed receipt plus current readback, not job exit/HTTP status, is the evidence.
An interrupted clean bootstrap can be adjudicated with --mode recover and a
separately hash-approved --recovery file. It requires the exact original receipt,
fresh evidence that the original job is read-only and quiescent, no other SQL
sessions, and no installed user schema or principals. It appends a resolution
bound to ONE replacement bundle without rewriting the original receipt. This is
operator recovery of an empty rollback, never automatic retry or schema repair.

The job deadline bounds native calls; SQL locks also have a short timeout. A native
driver can outlive a Python timeout, so do not substitute a local timer for the
finite job or claim that administrator restoration terminates existing sessions.

Public platform contracts:
https://learn.microsoft.com/azure/azure-sql/database/authentication-aad-service-principal
https://learn.microsoft.com/sql/t-sql/statements/create-user-transact-sql#k-create-a-contained-database-user-from-a-microsoft-entra-principal-without-validation
https://learn.microsoft.com/azure/container-apps/jobs
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib
import ipaddress
import json
import logging
import os
import re
import socket
import sys
import time
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

logger = logging.getLogger("triage.bootstrap")
SHA256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
DatabaseName = Annotated[str, Field(pattern=r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,121}$")]
SQL_SCOPE = "https://database.windows.net/.default"
RECEIPT_TABLE = "dbo.triage_sql_bootstrap_receipts"
RECOVERY_TABLE = "dbo.triage_sql_bootstrap_recoveries"
ROOT = Path(__file__).resolve().parents[1]

# Parameterized metadata only. This is not a parser for untrusted deployment SQL.
CHECK_SQL = {
    "object": """SELECT RTRIM(o.type),
        COALESCE(USER_NAME(o.principal_id), USER_NAME(s.principal_id)),
        LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONVERT(VARBINARY(MAX),
            TRIM(NCHAR(9)+NCHAR(10)+NCHAR(13)+N' ' FROM
                REPLACE(m.definition, NCHAR(13)+NCHAR(10), NCHAR(10))))), 2))
        FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id
        LEFT JOIN sys.sql_modules m ON m.object_id=o.object_id
        WHERE o.object_id=OBJECT_ID(?)""",
    "columns": """SELECT c.name, TYPE_NAME(c.user_type_id), c.max_length,
        c.precision, c.scale, CONVERT(INT,c.is_nullable),
        CONVERT(INT,c.is_identity), CONVERT(INT,c.is_computed)
        FROM sys.columns c WHERE c.object_id=OBJECT_ID(?) ORDER BY c.column_id""",
    "principal": """SELECT type, authentication_type_desc, default_schema_name,
        CASE WHEN type IN ('E','X') THEN LOWER(CONVERT(VARCHAR(256),sid,2)) END
        FROM sys.database_principals WHERE name=?""",
    "permissions": """SELECT p.class_desc,
        CASE p.class WHEN 0 THEN DB_NAME()
            WHEN 1 THEN OBJECT_SCHEMA_NAME(p.major_id)+'.'+OBJECT_NAME(p.major_id)
            WHEN 3 THEN SCHEMA_NAME(p.major_id)
            WHEN 4 THEN USER_NAME(p.major_id) END,
        p.minor_id, p.permission_name, p.state_desc
        FROM sys.database_permissions p
        JOIN sys.database_principals u ON u.principal_id=p.grantee_principal_id
        WHERE u.name=?
        ORDER BY p.class_desc COLLATE Latin1_General_100_BIN2,
            2, p.minor_id, p.permission_name COLLATE Latin1_General_100_BIN2,
            p.state_desc COLLATE Latin1_General_100_BIN2""",
    "members": """SELECT u.name FROM sys.database_role_members m
        JOIN sys.database_principals r ON r.principal_id=m.role_principal_id
        JOIN sys.database_principals u ON u.principal_id=m.member_principal_id
        WHERE r.name=? ORDER BY u.name COLLATE Latin1_General_100_BIN2""",
}
TARGET_SQL = """SELECT CONVERT(NVARCHAR(128),SERVERPROPERTY('ServerName')), DB_NAME(),
    CONVERT(INT,SERVERPROPERTY('EngineEdition')), USER_NAME(),
    HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','CONTROL'),
    (SELECT USER_NAME(principal_id) FROM sys.schemas WHERE name=N'dbo'), @@SPID"""
LOCK_SQL = """DECLARE @result INT;
    EXEC @result=sys.sp_getapplock @Resource=N'triage:azure-sql-bootstrap',
        @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout=0;
    SELECT @result"""
CREATE_RECEIPTS = f"""IF OBJECT_ID(N'{RECEIPT_TABLE}',N'U') IS NULL
    CREATE TABLE {RECEIPT_TABLE} (
        operation_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        fingerprint CHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
        source_sha256 CHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
        status VARCHAR(12) NOT NULL CHECK (status IN ('started','committed')),
        started_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
        committed_at DATETIME2(6) NULL
    )"""
CREATE_RECOVERIES = f"""CREATE TABLE {RECOVERY_TABLE} (
    original_operation_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    original_fingerprint CHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    original_source_sha256 CHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    replacement_operation_id UNIQUEIDENTIFIER NOT NULL UNIQUE,
    replacement_fingerprint CHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    replacement_source_sha256 CHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    recovery_sha256 CHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
    original_receipt_object_id INT NOT NULL,
    resolved_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME()
)"""
RECOVERY_COLUMNS = (
    "original_fingerprint,original_source_sha256,"
    "CONVERT(VARCHAR(36),replacement_operation_id),replacement_fingerprint,"
    "replacement_source_sha256,recovery_sha256,original_receipt_object_id"
)
EMPTY_SCHEMA_SQL = f"""SELECT SCHEMA_NAME(o.schema_id),o.name,RTRIM(o.type)
    FROM sys.objects o WHERE o.is_ms_shipped=0 AND NOT (
        o.object_id=OBJECT_ID(N'{RECEIPT_TABLE}',N'U')
        OR (o.parent_object_id=OBJECT_ID(N'{RECEIPT_TABLE}',N'U') AND o.type IN ('C','D','PK'))
    ) ORDER BY o.object_id"""
EMPTY_PRINCIPALS_SQL = """SELECT name,type FROM sys.database_principals
    WHERE principal_id>4 AND is_fixed_role=0 AND name<>N'public'
    ORDER BY principal_id"""
OTHER_SESSIONS_SQL = """SELECT session_id FROM sys.dm_exec_sessions
    WHERE is_user_process=1 AND database_id=DB_ID() AND session_id<>@@SPID
    ORDER BY session_id"""


class BootstrapError(RuntimeError):
    """A fail-closed condition; the code is safe to log without SQL or tokens."""


class Sql(Protocol):
    def query(self, sql: str, *params: object) -> list[tuple]: ...
    def execute(self, sql: str, *params: object) -> int: ...
    def transaction(self) -> AbstractContextManager: ...


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Target(StrictModel):
    server: Annotated[str, Field(
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.database\.windows\.net$",
    )]
    application_database: DatabaseName
    database: Annotated[str, Field(min_length=1, max_length=128)]
    kind: Literal["application", "proof"]
    private_ip: str | None = None

    @model_validator(mode="after")
    def check_target(self) -> Target:
        expected = self.application_database + ("-proof" if self.kind == "proof" else "")
        if self.database != expected or self.application_database.lower() in {
            "master", "model", "msdb", "tempdb",
        }:
            raise ValueError("Explicit application/proof database binding required")
        if self.private_ip is not None:
            private = ipaddress.IPv4Address(self.private_ip)
            networks = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
            if not any(private in ipaddress.ip_network(network) for network in networks):
                raise ValueError("An explicit private endpoint must have a reviewed RFC1918 address")
        return self


class Identity(StrictModel):
    tenant_id: UUID
    client_id: UUID
    object_id: UUID


class Batch(StrictModel):
    path: str
    sha256: SHA256


class Check(StrictModel):
    kind: Literal["object", "columns", "principal", "permissions", "members"]
    name: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,128}$")]
    argument: Annotated[str, Field(min_length=1, max_length=256)]
    expected: Annotated[list[list[str | int | None]], Field(max_length=2048)]


class Bundle(StrictModel):
    version: Annotated[int, Field(ge=1, le=1)]
    operation_id: UUID
    ddl_owner: Literal["dbo"]
    target: Target
    identity: Identity
    source_files: Annotated[dict[str, SHA256], Field(min_length=4, max_length=1024)]
    batches: Annotated[list[Batch], Field(min_length=1, max_length=512)]
    checks: Annotated[list[Check], Field(min_length=2, max_length=2048)]

    @model_validator(mode="after")
    def check_contract(self) -> Bundle:
        if any(value.int == 0 for value in (
            self.operation_id, self.identity.tenant_id,
            self.identity.client_id, self.identity.object_id,
        )):
            raise ValueError("Nonempty operation and identity UUIDs are required")
        if len({batch.path for batch in self.batches}) != len(self.batches):
            raise ValueError("Duplicate SQL batch")
        if len({check.name for check in self.checks}) != len(self.checks):
            raise ValueError("Duplicate metadata check name")
        if not {"object", "columns"} <= {check.kind for check in self.checks}:
            raise ValueError("Known object and column readbacks are required")
        for check in self.checks:
            if check.kind == "object" and (
                len(check.expected) != 1 or len(check.expected[0]) != 3
                or check.expected[0][1] != self.ddl_owner
            ):
                raise ValueError("Each object must have explicit dbo ownership")
            if check.kind == "columns" and not check.expected:
                raise ValueError("Column readback must describe the expected schema")
        return self


@dataclass(frozen=True)
class Artifact:
    bundle: Bundle
    fingerprint: str
    source_sha256: str
    sql: tuple[str, ...]


class QuiescentExecution(StrictModel):
    id: Annotated[str, Field(min_length=1, max_length=1024)]
    status: Literal["Succeeded", "Failed", "Stopped"]


class QuiescentJob(StrictModel):
    id: Annotated[str, Field(pattern=r"^/subscriptions/[0-9a-f-]{36}/resourceGroups/[^/]+/providers/Microsoft\.App/jobs/[^/]+$")]
    definition_sha256: SHA256
    mode: Literal["preflight", "reconcile", "diagnostic"]
    executions: Annotated[list[QuiescentExecution], Field(max_length=100)]

    @model_validator(mode="after")
    def check_executions(self) -> QuiescentJob:
        ids = [entry.id for entry in self.executions]
        if len(set(ids)) != len(ids) or any(
            not value.startswith(self.id + "/executions/") for value in ids
        ):
            raise ValueError("Quiescence evidence must identify distinct executions of this exact job")
        return self


class RecoveryRequest(StrictModel):
    version: Literal[1]
    original_operation_id: UUID
    original_fingerprint: SHA256
    original_source_sha256: SHA256
    original_started_at: Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}$")]
    original_receipt_object_id: Annotated[int, Field(gt=0)]
    original_job_id: str
    rollback_evidence_sha256: SHA256
    replacement_operation_id: UUID
    replacement_fingerprint: SHA256
    replacement_source_sha256: SHA256
    observed_at: datetime
    expires_at: datetime
    jobs: Annotated[list[QuiescentJob], Field(min_length=1, max_length=20)]

    @model_validator(mode="after")
    def check_evidence(self) -> RecoveryRequest:
        jobs = {job.id: job for job in self.jobs}
        original = jobs.get(self.original_job_id)
        if (
            len(jobs) != len(self.jobs) or original is None or original.mode != "reconcile"
            or self.original_operation_id.int == 0 or self.replacement_operation_id.int == 0
            or self.original_operation_id == self.replacement_operation_id
            or self.observed_at.tzinfo is None or self.expires_at.tzinfo is None
            or not timedelta(0) < self.expires_at - self.observed_at <= timedelta(minutes=15)
        ):
            raise ValueError("Recovery requires distinct operations and fresh, fenced original-job evidence")
        return self


@dataclass(frozen=True)
class Recovery:
    request: RecoveryRequest
    fingerprint: str


def load_recovery(path: Path, fingerprint: str, artifact: Artifact) -> Recovery:
    if not re.fullmatch(r"[0-9a-f]{64}", fingerprint) or path.stat().st_size > 64 * 1024:
        raise BootstrapError("invalid_recovery_artifact")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != fingerprint:
        raise BootstrapError("recovery_hash_mismatch")
    json.loads(raw, object_pairs_hook=_unique_keys)
    request = RecoveryRequest.model_validate_json(raw)
    if (
        request.replacement_operation_id != artifact.bundle.operation_id
        or request.replacement_fingerprint != artifact.fingerprint
        or request.replacement_source_sha256 != artifact.source_sha256
    ):
        raise BootstrapError("recovery_replacement_mismatch")
    return Recovery(request, fingerprint)


def _read(root: Path, relative: str, maximum: int) -> bytes:
    path = PurePosixPath(relative)
    if (
        path.is_absolute() or path.as_posix() != relative
        or "\\" in relative or ":" in relative or ".." in path.parts
    ):
        raise BootstrapError("artifact_path_outside_bundle")
    resolved = root.joinpath(*path.parts).resolve()
    if not resolved.is_relative_to(root.resolve()) or resolved.stat().st_size > maximum:
        raise BootstrapError("artifact_path_or_size_refused")
    return resolved.read_bytes()


def _unique_keys(pairs: list[tuple[str, object]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise BootstrapError("duplicate_bundle_key")
        result[key] = value
    return result


def load_artifact(root: Path, bundle_path: Path, fingerprint: str, operation_id: UUID) -> Artifact:
    if not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise BootstrapError("invalid_approved_fingerprint")
    raw = _read(root, bundle_path.resolve().relative_to(root.resolve()).as_posix(), 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != fingerprint:
        raise BootstrapError("bundle_hash_mismatch")
    json.loads(raw, object_pairs_hook=_unique_keys)
    bundle = Bundle.model_validate_json(raw)
    if bundle.operation_id != operation_id:
        raise BootstrapError("operation_id_mismatch")
    sources = {path.relative_to(root).as_posix() for path in (root / "src").rglob("*.py")}
    sources.add("scripts/bootstrap_azure_sql.py")
    required = {"src/triage/store/azure_sql.py", "src/triage/redaction.py", "src/triage/__init__.py"}
    if set(bundle.source_files) != sources or not required <= sources:
        raise BootstrapError("incomplete_current_source")
    total = 0
    for path, expected in bundle.source_files.items():
        content = _read(root, path, 4 * 1024 * 1024)
        total += len(content)
        if total > 32 * 1024 * 1024 or hashlib.sha256(content).hexdigest() != expected:
            raise BootstrapError("current_source_hash_mismatch")
    statements = []
    total = 0
    for batch in bundle.batches:
        content = _read(root, batch.path, 4 * 1024 * 1024)
        total += len(content)
        if total > 8 * 1024 * 1024 or hashlib.sha256(content).hexdigest() != batch.sha256:
            raise BootstrapError("sql_batch_hash_mismatch")
        sql = content.decode("utf-8")
        if not sql.strip() or re.search(r"^\s*GO(?:\s+\d+)?\s*(?:--[^\n]*)?$", sql, re.I | re.M):
            raise BootstrapError("sql_batch_must_be_one_driver_batch")
        statements.append(sql)
    source_hash = hashlib.sha256(json.dumps(
        bundle.source_files, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    return Artifact(bundle, fingerprint, source_hash, tuple(statements))


def check_environment(bundle: Bundle, environment: Mapping[str, str]) -> None:
    expected = {
        "AZURE_SQL_SERVER": bundle.target.server,
        "AZURE_SQL_DATABASE": bundle.target.database,
        "BOOTSTRAP_APPLICATION_DATABASE": bundle.target.application_database,
        "BOOTSTRAP_TARGET_KIND": bundle.target.kind,
        "AZURE_TENANT_ID": str(bundle.identity.tenant_id),
        "AZURE_CLIENT_ID": str(bundle.identity.client_id),
        "BOOTSTRAP_IDENTITY_OBJECT_ID": str(bundle.identity.object_id),
    }
    if any(environment.get(key) != value for key, value in expected.items()):
        raise BootstrapError("deployed_target_or_identity_mismatch")


def check_token(token: str, identity: Identity) -> None:
    # Diagnostic binding, not signature verification. SQL validates the token;
    # it comes only from the pinned ManagedIdentityCredential, never operator input.
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("JWT claims unavailable")
        claims = json.loads(base64.b64decode(
            parts[1] + "=" * (-len(parts[1]) % 4), altchars=b"-_", validate=True,
        ))
        apps = [claims[key] for key in ("appid", "azp") if key in claims]
        valid = (
            claims["tid"] == str(identity.tenant_id)
            and claims["oid"] == str(identity.object_id)
            and apps and all(app == str(identity.client_id) for app in apps)
            and claims["aud"] in ("https://database.windows.net", "https://database.windows.net/")
            and type(claims["exp"]) is int and claims["exp"] > time.time() + 60
            and type(claims.get("nbf", 0)) is int and claims.get("nbf", 0) <= time.time()
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise BootstrapError("managed_identity_claims_unavailable") from exc
    if not valid:
        raise BootstrapError("managed_identity_claims_mismatch")


def open_database(root: Path, bundle: Bundle) -> Sql:
    addresses = {
        item[4][0] for item in socket.getaddrinfo(
            bundle.target.server, 1433, type=socket.SOCK_STREAM,
        )
    }
    if not addresses:
        raise BootstrapError("sql_dns_unavailable")
    if bundle.target.private_ip is not None and addresses != {bundle.target.private_ip}:
        raise BootstrapError("private_dns_mismatch")
    source = (root / "src").resolve()
    sys.path.insert(0, str(source))
    module = importlib.import_module("triage.store.azure_sql")
    for name in ("triage", "triage.redaction", "triage.store.azure_sql"):
        loaded = sys.modules[name]
        if not Path(loaded.__file__).resolve().is_relative_to(source):
            raise BootstrapError("stale_application_module_refused")
    from azure.identity import ManagedIdentityCredential

    credential = ManagedIdentityCredential(
        client_id=str(bundle.identity.client_id),
        connection_timeout=10, read_timeout=20, retry_total=0,
    )

    class PinnedCredential:
        def get_token(self, scope: str):
            if scope != SQL_SCOPE:
                raise BootstrapError("unexpected_token_scope")
            token = credential.get_token(scope)
            check_token(token.token, bundle.identity)
            return token

    pinned = PinnedCredential()
    pinned.get_token(SQL_SCOPE)
    return module.AzureSqlDatabase(
        server=bundle.target.server, database=bundle.target.database, credential=pinned,
    )


def check_sql_target(db: Sql, bundle: Bundle) -> dict:
    rows = db.query(TARGET_SQL)
    expected = (bundle.target.database, 5, "dbo", 1, bundle.ddl_owner)
    if (
        len(rows) != 1 or len(rows[0]) != 7 or tuple(rows[0][1:6]) != expected
        or type(rows[0][6]) is not int or rows[0][6] <= 0
    ):
        raise BootstrapError("sql_target_or_ddl_authority_mismatch")
    actual = str(rows[0][0]).lower()
    if actual not in {bundle.target.server, bundle.target.server.removesuffix(".database.windows.net")}:
        raise BootstrapError("sql_server_identity_mismatch")
    return dict(zip(
        ("server_identity", "database", "engine_edition", "principal", "control_database",
         "ddl_owner", "session_id"),
        rows[0], strict=True,
    ))


def check_metadata(db: Sql, artifact: Artifact) -> None:
    for check in artifact.bundle.checks:
        if [list(row) for row in db.query(CHECK_SQL[check.kind], check.argument)] != check.expected:
            raise BootstrapError(f"metadata_mismatch:{check.name}")


def receipt(db: Sql, artifact: Artifact) -> str | None:
    present = db.query("SELECT OBJECT_ID(?,N'U')", RECEIPT_TABLE)
    if present == [(None,)]:
        return None
    if len(present) != 1 or len(present[0]) != 1 or type(present[0][0]) is not int:
        raise BootstrapError("receipt_catalogue_unreadable")
    rows = db.query(
        f"SELECT fingerprint,source_sha256,status FROM {RECEIPT_TABLE} WHERE operation_id=?",
        str(artifact.bundle.operation_id),
    )
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0]) != 3 or rows[0][:2] != (
        artifact.fingerprint, artifact.source_sha256,
    ) or rows[0][2] not in {"started", "committed"}:
        raise BootstrapError("durable_receipt_conflict")
    return rows[0][2]


def _lock(db: Sql) -> None:
    db.execute("SET XACT_ABORT ON; SET LOCK_TIMEOUT 10000;")
    rows = db.query(LOCK_SQL)
    if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int or rows[0][0] not in {0, 1}:
        raise BootstrapError("bootstrap_operation_busy")


def _recovery_table_exists(db: Sql) -> bool:
    rows = db.query("SELECT OBJECT_ID(?,N'U')", RECOVERY_TABLE)
    if rows == [(None,)]:
        return False
    if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int:
        raise BootstrapError("recovery_catalogue_unreadable")
    return True


def _pending_count(db: Sql, artifact: Artifact) -> list[tuple]:
    if not _recovery_table_exists(db):
        return db.query(f"SELECT COUNT_BIG(*) FROM {RECEIPT_TABLE} WHERE status='started'")
    # A resolution licenses only its reviewed replacement until that replacement
    # commits. The original STARTED receipt remains immutable and cannot replay.
    return db.query(
        f"""SELECT COUNT_BIG(*) FROM {RECEIPT_TABLE} r WHERE r.status='started'
        AND NOT EXISTS (SELECT 1 FROM {RECOVERY_TABLE} a
            WHERE a.original_operation_id=r.operation_id
            AND a.original_fingerprint=r.fingerprint AND a.original_source_sha256=r.source_sha256
            AND ((a.replacement_operation_id=? AND a.replacement_fingerprint=?
                AND a.replacement_source_sha256=?)
                OR EXISTS (SELECT 1 FROM {RECEIPT_TABLE} c
                    WHERE c.operation_id=a.replacement_operation_id
                    AND c.fingerprint=a.replacement_fingerprint
                    AND c.source_sha256=a.replacement_source_sha256 AND c.status='committed')))""",
        str(artifact.bundle.operation_id), artifact.fingerprint, artifact.source_sha256,
    )


def _original_receipt(db: Sql, request: RecoveryRequest) -> None:
    if db.query("SELECT OBJECT_ID(?,N'U')", RECEIPT_TABLE) != [(request.original_receipt_object_id,)]:
        raise BootstrapError("recovery_original_receipt_object_changed")
    rows = db.query(
        f"SELECT fingerprint,source_sha256,status,CONVERT(VARCHAR(27),started_at,126),committed_at "
        f"FROM {RECEIPT_TABLE} WHERE operation_id=?",
        str(request.original_operation_id),
    )
    if rows != [(
        request.original_fingerprint, request.original_source_sha256, "started",
        request.original_started_at, None,
    )]:
        raise BootstrapError("recovery_original_receipt_changed")


def _recovery_values(recovery: Recovery) -> tuple:
    request = recovery.request
    return (
        request.original_fingerprint, request.original_source_sha256,
        str(request.replacement_operation_id), request.replacement_fingerprint,
        request.replacement_source_sha256, recovery.fingerprint, request.original_receipt_object_id,
    )


def _recovery_readback(db: Sql, recovery: Recovery) -> bool:
    if not _recovery_table_exists(db):
        return False
    rows = db.query(
        f"SELECT {RECOVERY_COLUMNS} FROM {RECOVERY_TABLE} WHERE original_operation_id=?",
        str(recovery.request.original_operation_id),
    )
    expected = _recovery_values(recovery)
    if len(rows) == 1 and len(rows[0]) == 7:
        actual = (*rows[0][:2], str(rows[0][2]).lower(), *rows[0][3:])
        if actual == expected:
            return True
    raise BootstrapError("recovery_receipt_conflict")


def recover(db: Sql, artifact: Artifact, recovery: Recovery) -> dict:
    request = recovery.request
    if (
        request.replacement_operation_id != artifact.bundle.operation_id
        or request.replacement_fingerprint != artifact.fingerprint
        or request.replacement_source_sha256 != artifact.source_sha256
    ):
        raise BootstrapError("recovery_replacement_mismatch")
    with db.transaction():
        check_sql_target(db, artifact.bundle)
        _lock(db)
        _original_receipt(db, request)
        if not _recovery_readback(db, recovery):
            now = datetime.now(UTC)
            if not request.observed_at <= now < request.expires_at:
                raise BootstrapError("recovery_quiescence_evidence_expired")
            if db.query(OTHER_SESSIONS_SQL):
                raise BootstrapError("recovery_other_sql_sessions")
            if db.query(EMPTY_SCHEMA_SQL) or db.query(EMPTY_PRINCIPALS_SQL):
                raise BootstrapError("recovery_requires_empty_rollback")
            if db.query(f"SELECT COUNT_BIG(*) FROM {RECEIPT_TABLE}") != [(1,)]:
                raise BootstrapError("recovery_requires_single_original_receipt")
            db.execute(CREATE_RECOVERIES)
            if db.execute(
                f"INSERT INTO {RECOVERY_TABLE} (original_operation_id,"
                "original_fingerprint,original_source_sha256,replacement_operation_id,"
                "replacement_fingerprint,replacement_source_sha256,recovery_sha256,"
                "original_receipt_object_id) VALUES (?,?,?,?,?,?,?,?)",
                str(request.original_operation_id), *_recovery_values(recovery),
            ) != 1:
                raise BootstrapError("recovery_receipt_not_recorded")
    check_sql_target(db, artifact.bundle)
    _original_receipt(db, request)
    if not _recovery_readback(db, recovery):
        raise BootstrapError("recovery_commit_not_observed")
    return {
        "status": "empty_rollback_adjudicated", "original_operation_id": str(request.original_operation_id),
        "recovery_sha256": recovery.fingerprint, "batches_executed": 0,
        "replacement_operation_id": str(request.replacement_operation_id),
    }


def reconcile(db: Sql, artifact: Artifact) -> dict:
    check_sql_target(db, artifact.bundle)
    if receipt(db, artifact) != "committed":
        raise BootstrapError("receipt_uncommitted_reconcile_original_operation")
    check_metadata(db, artifact)
    return {"status": "committed_and_read_back", "metadata_checks": len(artifact.bundle.checks)}


def run(
    db: Sql, artifact: Artifact, mode: str, approval: str = "",
    recovery: Recovery | None = None, recovery_approval: str = "",
) -> dict:
    if mode not in {"preflight", "apply", "reconcile", "recover"}:
        raise BootstrapError("unknown_mode")
    if mode in {"apply", "recover"} and approval != artifact.fingerprint:
        raise BootstrapError("explicit_apply_approval_required")
    if mode == "recover" and (recovery is None or recovery_approval != recovery.fingerprint):
        raise BootstrapError("explicit_recovery_approval_required")
    if mode != "recover" and (recovery is not None or recovery_approval):
        raise BootstrapError("recovery_inputs_require_recover_mode")
    check_sql_target(db, artifact.bundle)
    if mode == "preflight":
        return {"status": "preflight_only", "schema_checked": False, "batches_executed": 0}
    if mode == "reconcile":
        return reconcile(db, artifact)
    if mode == "recover":
        assert recovery is not None
        return recover(db, artifact, recovery)
    with db.transaction():
        check_sql_target(db, artifact.bundle)
        _lock(db)
        db.execute(CREATE_RECEIPTS)
        state = receipt(db, artifact)
        if state == "committed":
            check_metadata(db, artifact)
            return {"status": "committed_and_read_back", "batches_executed": 0}
        if state == "started" or _pending_count(db, artifact) != [(0,)]:
            raise BootstrapError("pending_operation_blocks_apply")
        if db.execute(
            f"INSERT INTO {RECEIPT_TABLE} (operation_id,fingerprint,source_sha256,status) "
            "VALUES (?,?,?,'started')",
            str(artifact.bundle.operation_id), artifact.fingerprint, artifact.source_sha256,
        ) != 1:
            raise BootstrapError("started_receipt_not_recorded")
    # The committed start survives interruption and refuses both same-ID and new-ID replay.
    with db.transaction():
        check_sql_target(db, artifact.bundle)
        _lock(db)
        if receipt(db, artifact) != "started":
            raise BootstrapError("started_receipt_changed")
        if _recovery_table_exists(db) and db.query(
            f"SELECT original_operation_id FROM {RECOVERY_TABLE} WHERE original_operation_id=?",
            str(artifact.bundle.operation_id),
        ):
            raise BootstrapError("adjudicated_operation_cannot_replay")
        for sql in artifact.sql:
            db.execute(sql)
            if db.query("SELECT @@TRANCOUNT, XACT_STATE()") != [(1, 1)]:
                raise BootstrapError("batch_changed_transaction_boundary")
        check_sql_target(db, artifact.bundle)
        check_metadata(db, artifact)
        if db.execute(
            f"UPDATE {RECEIPT_TABLE} SET status='committed',committed_at=SYSUTCDATETIME() "
            "WHERE operation_id=? AND fingerprint=? AND source_sha256=? AND status='started'",
            str(artifact.bundle.operation_id), artifact.fingerprint, artifact.source_sha256,
        ) != 1:
            raise BootstrapError("completion_receipt_not_recorded")
    result = reconcile(db, artifact)
    return {**result, "batches_executed": len(artifact.sql)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--bundle-sha256", required=True)
    parser.add_argument("--operation-id", type=UUID, required=True)
    parser.add_argument("--mode", choices=("preflight", "apply", "reconcile", "recover"), default="preflight")
    parser.add_argument("--approve-fingerprint", default="")
    parser.add_argument("--recovery", type=Path)
    parser.add_argument("--recovery-sha256", default="")
    parser.add_argument("--approve-recovery-sha256", default="")
    args = parser.parse_args(argv)
    context: dict = {}
    try:
        artifact = load_artifact(ROOT, args.bundle, args.bundle_sha256, args.operation_id)
        check_environment(artifact.bundle, os.environ)
        recovery = load_recovery(args.recovery, args.recovery_sha256, artifact) if args.recovery else None
        if args.mode in {"apply", "recover"} and args.approve_fingerprint != artifact.fingerprint:
            raise BootstrapError("explicit_apply_approval_required")
        if args.mode == "recover" and (
            recovery is None or args.approve_recovery_sha256 != recovery.fingerprint
        ):
            raise BootstrapError("explicit_recovery_approval_required")
        if args.mode != "recover" and (args.recovery or args.recovery_sha256 or args.approve_recovery_sha256):
            raise BootstrapError("recovery_inputs_require_recover_mode")
        context = {
            "operation_id": str(artifact.bundle.operation_id),
            "fingerprint": artifact.fingerprint, "source_sha256": artifact.source_sha256,
            "target": artifact.bundle.target.model_dump(),
            "identity": artifact.bundle.identity.model_dump(mode="json"), "mode": args.mode,
        }
        print(json.dumps({**context, "status": "inputs_verified"}), flush=True)
        db = open_database(ROOT, artifact.bundle)
        sql_identity = check_sql_target(db, artifact.bundle)
        print(json.dumps({
            **context, "status": "sql_preflight_verified", "sql_identity": sql_identity,
        }), flush=True)
        result = run(
            db, artifact, args.mode, args.approve_fingerprint,
            recovery, args.approve_recovery_sha256,
        )
        print(json.dumps({**context, **result, "runtime_identity_acceptance": "not_performed"}), flush=True)
        return 0
    except Exception as exc:
        # A CLI boundary reports typed failure without logging tokens, SQL or arbitrary driver text.
        uncertain = type(exc).__name__ in {"SqlCommitUncertain", "SqlRollbackUncertain"}
        code = str(exc) if isinstance(exc, BootstrapError) else (
            "reconcile_original_operation" if uncertain else "bootstrap_failed"
        )
        logger.error("Azure SQL bootstrap refused or failed (%s)", code)
        print(json.dumps({
            **context, "status": "failed", "code": code, "error_type": type(exc).__name__,
        }), flush=True)
        return 3 if uncertain else 2


if __name__ == "__main__":
    raise SystemExit(main())
