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
sessions, and no installed user schema or principals. Fresh adjudication also
requires empty_baseline_sha256 in the approved recovery request. Capture it with
recovery_baseline_fingerprint() on the independently reviewed empty target, using
the same original receipt catalogue. Never bless the failed target's current
metadata as its own baseline. The complete fixed security/catalogue query set is
compared inside the recovery transaction before any recovery CREATE or INSERT.
Recovery appends a resolution bound to ONE replacement bundle without rewriting
the original receipt. This is operator recovery, never automatic schema repair.

--mode reconcile-recovery takes the ORIGINAL --recovery and --recovery-sha256
without either approval argument. It performs SELECTs only and reports MATCHING,
MISSING or CONFLICT; only MATCHING exits zero. Expired requests and older requests
without an empty-baseline fingerprint may be read, but do not authorize mutation.
MISSING is not permission to restart. Schema reconciliation remains --mode
reconcile. Preserve the original immutable payload/source tree for either read.
For historical recovery evidence, --artifact-root selects that unchanged tree
ONLY in reconcile-recovery mode. Its bundle, Python files and SQL batches are
hash-checked as data; none of its code is imported or executed. The current
trusted runner and its own SQL adapter perform the fixed SELECTs. Other modes
reject --artifact-root and retain this runner's source-root/hash requirements.

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
MAX_BASELINE_ROWS = 20_000

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
    "computed_columns": """SELECT name,definition,CONVERT(INT,is_persisted)
        FROM sys.computed_columns WHERE object_id=OBJECT_ID(?) ORDER BY column_id""",
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
    "budget_policies": """SELECT TOP (2049) bucket_hash,request_limit,window_seconds
        FROM dbo.triage_monitoring_rate_budget WHERE tenant_id=CONVERT(UNIQUEIDENTIFIER,?)
        ORDER BY bucket_hash COLLATE Latin1_General_100_BIN2""",
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
RECOVERY_OBJECT_SQL = "SELECT OBJECT_ID(?,N'U'),OBJECT_ID(?)"

# These use the authority reader's whole-universe approach, not a filtered list
# of expected grants. SQL errors or unknown row shapes must stop adjudication.
EMPTY_BASELINE_SQL = {
    "database": (5, """SELECT database_id,is_trustworthy_on,is_db_chaining_on,containment,
        LOWER(CONVERT(VARCHAR(256),owner_sid,2)) FROM sys.databases WHERE name=DB_NAME()"""),
    "principals": (8, """SELECT principal_id,type,authentication_type_desc,
        LOWER(CONVERT(VARCHAR(256),sid,2)),is_fixed_role,owning_principal_id,name,default_schema_name
        FROM sys.database_principals ORDER BY principal_id"""),
    "permissions": (7, """SELECT grantee_principal_id,grantor_principal_id,class,major_id,
        minor_id,permission_name,state FROM sys.database_permissions
        ORDER BY grantee_principal_id,grantor_principal_id,class,major_id,minor_id,permission_name,state"""),
    "memberships": (2, """SELECT member_principal_id,role_principal_id
        FROM sys.database_role_members ORDER BY member_principal_id,role_principal_id"""),
    "schemas": (3, """SELECT schema_id,name,principal_id FROM sys.schemas ORDER BY schema_id"""),
    "objects": (10, """SELECT o.object_id,o.schema_id,o.name,RTRIM(o.type),o.parent_object_id,
        o.principal_id,o.is_ms_shipped,m.execute_as_principal_id,m.is_schema_bound,
        LOWER(CONVERT(VARCHAR(64),HASHBYTES('SHA2_256',CONVERT(VARBINARY(MAX),m.definition)),2))
        FROM sys.objects o LEFT JOIN sys.sql_modules m ON m.object_id=o.object_id
        ORDER BY o.object_id"""),
    "columns": (11, """SELECT object_id,column_id,name,user_type_id,max_length,precision,scale,
        is_nullable,is_identity,is_computed,collation_name FROM sys.columns ORDER BY object_id,column_id"""),
    "computed_columns": (5, """SELECT object_id,column_id,definition,is_persisted,uses_database_collation
        FROM sys.computed_columns ORDER BY object_id,column_id"""),
    "indexes": (8, """SELECT object_id,index_id,name,type,is_unique,is_primary_key,
        is_disabled,filter_definition FROM sys.indexes ORDER BY object_id,index_id"""),
    "index_columns": (7, """SELECT object_id,index_id,index_column_id,column_id,key_ordinal,
        is_descending_key,is_included_column FROM sys.index_columns
        ORDER BY object_id,index_id,index_column_id"""),
    "constraints": (5, """SELECT object_id,parent_object_id,name,definition,is_disabled
        FROM sys.check_constraints UNION ALL
        SELECT object_id,parent_object_id,name,definition,0 FROM sys.default_constraints
        ORDER BY object_id"""),
    "triggers": (9, """SELECT t.object_id,t.parent_class,t.parent_id,t.name,t.is_disabled,
        t.is_instead_of_trigger,t.is_ms_shipped,m.execute_as_principal_id,
        LOWER(CONVERT(VARCHAR(64),HASHBYTES('SHA2_256',CONVERT(VARBINARY(MAX),m.definition)),2))
        FROM sys.triggers t LEFT JOIN sys.sql_modules m ON m.object_id=t.object_id
        ORDER BY t.parent_class,t.object_id"""),
    "credentials": (5, """SELECT credential_id,name,credential_identity,
        CONVERT(VARCHAR(33),create_date,126),CONVERT(VARCHAR(33),modify_date,126)
        FROM sys.database_scoped_credentials ORDER BY credential_id"""),
    "external_data_sources": (5, """SELECT data_source_id,name,type_desc,location,credential_id
        FROM sys.external_data_sources ORDER BY data_source_id"""),
    "external_file_formats": (3, """SELECT file_format_id,name,format_type
        FROM sys.external_file_formats ORDER BY file_format_id"""),
    "assemblies": (7, """SELECT assembly_id,name,principal_id,permission_set,is_user_defined,
        CONVERT(VARCHAR(33),modify_date,126),clr_name FROM sys.assemblies ORDER BY assembly_id"""),
    "types": (7, """SELECT user_type_id,schema_id,name,system_type_id,is_user_defined,
        is_assembly_type,is_table_type FROM sys.types ORDER BY user_type_id"""),
    "xml_schema_collections": (3, """SELECT xml_collection_id,schema_id,name
        FROM sys.xml_schema_collections ORDER BY xml_collection_id"""),
    "certificates": (3, """SELECT certificate_id,name,principal_id
        FROM sys.certificates ORDER BY certificate_id"""),
    "asymmetric_keys": (3, """SELECT asymmetric_key_id,name,principal_id
        FROM sys.asymmetric_keys ORDER BY asymmetric_key_id"""),
    "symmetric_keys": (3, """SELECT symmetric_key_id,name,principal_id
        FROM sys.symmetric_keys ORDER BY symmetric_key_id"""),
    "column_master_keys": (2, """SELECT column_master_key_id,name
        FROM sys.column_master_keys ORDER BY column_master_key_id"""),
    "column_encryption_keys": (2, """SELECT column_encryption_key_id,name
        FROM sys.column_encryption_keys ORDER BY column_encryption_key_id"""),
    "module_signatures": (4, """SELECT class,major_id,
        LOWER(CONVERT(VARCHAR(256),thumbprint,2)),crypt_type FROM sys.crypt_properties
        ORDER BY class,major_id,thumbprint,crypt_type"""),
    "queues": (6, """SELECT object_id,is_activation_enabled,is_receive_enabled,
        activation_procedure,execute_as_principal_id,is_ms_shipped
        FROM sys.service_queues ORDER BY object_id"""),
    "plan_guides": (4, """SELECT plan_guide_id,name,is_disabled,scope_type
        FROM sys.plan_guides ORDER BY plan_guide_id"""),
}
EMPTY_BASELINE_ABSENT = frozenset({
    "credentials", "external_data_sources", "external_file_formats", "certificates",
    "asymmetric_keys", "symmetric_keys", "column_master_keys", "column_encryption_keys",
    "module_signatures", "plan_guides",
})
EMPTY_PUBLIC_PERMISSIONS = frozenset({
    "CONNECT", "VIEW ANY COLUMN ENCRYPTION KEY DEFINITION", "VIEW ANY COLUMN MASTER KEY DEFINITION",
})


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
    kind: Literal["object", "columns", "computed_columns", "principal", "permissions", "members", "budget_policies"]
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
            if check.kind == "computed_columns" and (
                not check.expected or any(
                    len(row) != 3 or not isinstance(row[0], str) or not row[0]
                    or not isinstance(row[1], str) or not row[1] or row[2] != 1
                    for row in check.expected
                ) or len({row[0] for row in check.expected}) != len(check.expected)
            ):
                raise ValueError("Computed-column readback requires unique names, native definitions and persistence")
            if check.kind == "budget_policies":
                if check.argument != str(self.identity.tenant_id) or not check.expected:
                    raise ValueError("Budget policy readback requires the bundle tenant and nonempty policies")
                if any(
                    len(row) != 3 or not isinstance(row[0], str) or not re.fullmatch(r"[0-9a-f]{64}", row[0])
                    or type(row[1]) is not int or not 1 <= row[1] <= 1_000_000
                    or type(row[2]) is not int or not 1 <= row[2] <= 86_400
                    for row in check.expected
                ):
                    raise ValueError("Budget readback accepts only bounded hash, limit and window metadata")
                if (
                    len({row[0] for row in check.expected}) != len(check.expected)
                    or check.expected != sorted(check.expected)
                ):
                    raise ValueError("Budget policy readback must be unique and hash-ordered")
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
    mode: Literal["preflight", "reconcile", "reconcile-recovery", "diagnostic"]
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
    empty_baseline_sha256: SHA256 | None = None
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
            len(jobs) != len(self.jobs) or original is None
            or original.mode not in {"reconcile", "reconcile-recovery"}
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
    rows = db.query(RECOVERY_OBJECT_SQL, RECOVERY_TABLE, RECOVERY_TABLE)
    if rows == [(None, None)]:
        return False
    if len(rows) != 1 or len(rows[0]) != 2 or any(
        value is not None and type(value) is not int for value in rows[0]
    ):
        raise BootstrapError("recovery_catalogue_unreadable")
    if rows[0][0] is None or rows[0][0] != rows[0][1]:
        raise BootstrapError("recovery_object_type_conflict")
    return True


def recovery_baseline_fingerprint(db: Sql, bundle: Bundle) -> str:
    """Read/hash a review candidate, never approve the observed state implicitly."""
    check_sql_target(db, bundle)
    if db.query(EMPTY_SCHEMA_SQL) or db.query(EMPTY_PRINCIPALS_SQL):
        raise BootstrapError("recovery_requires_empty_rollback")
    catalogue = {}
    remaining = MAX_BASELINE_ROWS
    for name, (width, sql) in EMPTY_BASELINE_SQL.items():
        rows = db.query(sql)
        remaining -= len(rows)
        if remaining < 0 or any(
            len(row) != width or any(type(value) not in {str, int, bool, type(None)} for value in row)
            for row in rows
        ) or name == "database" and len(rows) != 1:
            raise BootstrapError(f"recovery_baseline_unreadable:{name}")
        catalogue[name] = [list(row) for row in rows]
    _standard_empty_security(catalogue)
    document = {
        "contract": "triage.bootstrap.empty-baseline.v1",
        "server": bundle.target.server, "database": bundle.target.database,
        "queries": EMPTY_BASELINE_SQL, "catalogue": catalogue,
    }
    return hashlib.sha256(json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")).hexdigest()


def _standard_empty_security(catalogue: dict[str, list[list]]) -> None:
    if catalogue["database"][0][1] or catalogue["database"][0][2]:
        raise BootstrapError("recovery_empty_security_not_standard:database_flags")
    principals = {row[6]: row for row in catalogue["principals"]}
    required = {"dbo", "guest", "sys", "INFORMATION_SCHEMA", "public", "db_owner"}
    if (
        not required <= principals.keys() or len(principals) != len(catalogue["principals"])
        or any(name not in required and not row[4] for name, row in principals.items())
    ):
        raise BootstrapError("recovery_empty_security_not_standard:principals")
    schemas = {
        name: row[0] for name, row in principals.items()
        if name in {"dbo", "guest", "sys", "INFORMATION_SCHEMA"} or row[4]
    }
    public_id, owner_id = principals["public"][0], principals["dbo"][0]
    if any(row[1] not in schemas or row[2] != schemas[row[1]] for row in catalogue["schemas"]):
        raise BootstrapError("recovery_empty_security_not_standard:schemas")
    if any(
        row[0] != public_id or row[1] != owner_id or row[2:5] != [0, 0, 0]
        or row[5] not in EMPTY_PUBLIC_PERMISSIONS or row[6] != "G"
        for row in catalogue["permissions"]
    ):
        raise BootstrapError("recovery_empty_security_not_standard:permissions")
    if any(row != [owner_id, principals["db_owner"][0]] for row in catalogue["memberships"]):
        raise BootstrapError("recovery_empty_security_not_standard:memberships")
    if any(not row[6] for row in catalogue["triggers"]):
        raise BootstrapError("recovery_empty_security_not_standard:triggers")
    for name in sorted(EMPTY_BASELINE_ABSENT):
        if catalogue[name]:
            raise BootstrapError(f"recovery_empty_security_not_standard:{name}")
    if any(row[4] for row in catalogue["assemblies"]) or any(row[4] for row in catalogue["types"]):
        raise BootstrapError("recovery_empty_security_not_standard:user_types_or_assemblies")
    if any(row[1] != principals["sys"][0] for row in catalogue["xml_schema_collections"]):
        raise BootstrapError("recovery_empty_security_not_standard:xml_schema_collections")
    if any(not row[5] and (row[1] or row[2] and row[3]) for row in catalogue["queues"]):
        raise BootstrapError("recovery_empty_security_not_standard:queue_activation")


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
    if not rows:
        return False
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
            if request.empty_baseline_sha256 is None:
                raise BootstrapError("approved_empty_baseline_required")
            if recovery_baseline_fingerprint(db, artifact.bundle) != request.empty_baseline_sha256:
                raise BootstrapError("recovery_empty_baseline_changed")
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


def reconcile_recovery(db: Sql, artifact: Artifact, recovery: Recovery) -> dict:
    """Observe the original adjudication only; missing is not a mutation license."""
    request = recovery.request
    if (
        request.replacement_operation_id != artifact.bundle.operation_id
        or request.replacement_fingerprint != artifact.fingerprint
        or request.replacement_source_sha256 != artifact.source_sha256
    ):
        raise BootstrapError("recovery_replacement_mismatch")
    check_sql_target(db, artifact.bundle)
    code = None
    try:
        _original_receipt(db, request)
        status = "MATCHING" if _recovery_readback(db, recovery) else "MISSING"
    except BootstrapError as exc:
        if str(exc) not in {
            "recovery_original_receipt_object_changed", "recovery_original_receipt_changed",
            "recovery_receipt_conflict", "recovery_object_type_conflict",
        }:
            raise
        status, code = "CONFLICT", str(exc)
    if status != "MATCHING":
        logger.warning("Read-only recovery lookup %s (%s)", status, code or "record_not_found")
    return {
        "status": status, "read_only": True, "code": code,
        "original_operation_id": str(request.original_operation_id),
        "replacement_operation_id": str(request.replacement_operation_id),
        "recovery_sha256": recovery.fingerprint, "batches_executed": 0,
    }


def reconcile(db: Sql, artifact: Artifact) -> dict:
    check_sql_target(db, artifact.bundle)
    if receipt(db, artifact) != "committed":
        raise BootstrapError("receipt_uncommitted_reconcile_original_operation")
    check_metadata(db, artifact)
    return {"status": "committed_and_read_back", "metadata_checks": len(artifact.bundle.checks)}


def _validate_mode(
    artifact: Artifact, mode: str, approval: str, recovery: Recovery | None, recovery_approval: str,
) -> None:
    if mode not in {"preflight", "apply", "reconcile", "recover", "reconcile-recovery"}:
        raise BootstrapError("unknown_mode")
    if mode in {"apply", "recover"} and approval != artifact.fingerprint:
        raise BootstrapError("explicit_apply_approval_required")
    if mode == "recover" and (recovery is None or recovery_approval != recovery.fingerprint):
        raise BootstrapError("explicit_recovery_approval_required")
    if mode == "reconcile-recovery" and (recovery is None or approval or recovery_approval):
        raise BootstrapError("recovery_lookup_requires_original_request_without_approval")
    if mode not in {"recover", "reconcile-recovery"} and (recovery is not None or recovery_approval):
        raise BootstrapError("recovery_inputs_require_recover_mode")


def run(
    db: Sql, artifact: Artifact, mode: str, approval: str = "",
    recovery: Recovery | None = None, recovery_approval: str = "",
) -> dict:
    _validate_mode(artifact, mode, approval, recovery, recovery_approval)
    check_sql_target(db, artifact.bundle)
    if mode == "preflight":
        return {"status": "preflight_only", "schema_checked": False, "batches_executed": 0}
    if mode == "reconcile":
        return reconcile(db, artifact)
    if mode == "recover":
        assert recovery is not None
        return recover(db, artifact, recovery)
    if mode == "reconcile-recovery":
        assert recovery is not None
        return reconcile_recovery(db, artifact, recovery)
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
    parser.add_argument("--mode", choices=("preflight", "apply", "reconcile", "recover", "reconcile-recovery"), default="preflight")
    parser.add_argument("--approve-fingerprint", default="")
    parser.add_argument("--recovery", type=Path)
    parser.add_argument("--recovery-sha256", default="")
    parser.add_argument("--approve-recovery-sha256", default="")
    parser.add_argument("--artifact-root", type=Path)
    args = parser.parse_args(argv)
    context: dict = {}
    try:
        if args.artifact_root is not None and args.mode != "reconcile-recovery":
            raise BootstrapError("artifact_root_requires_reconcile_recovery")
        artifact_root = args.artifact_root.resolve() if args.artifact_root is not None else ROOT
        artifact = load_artifact(artifact_root, args.bundle, args.bundle_sha256, args.operation_id)
        check_environment(artifact.bundle, os.environ)
        recovery = load_recovery(args.recovery, args.recovery_sha256, artifact) if args.recovery else None
        _validate_mode(
            artifact, args.mode, args.approve_fingerprint, recovery, args.approve_recovery_sha256,
        )
        if args.mode not in {"recover", "reconcile-recovery"} and (args.recovery or args.recovery_sha256 or args.approve_recovery_sha256):
            raise BootstrapError("recovery_inputs_require_recover_mode")
        context = {
            "operation_id": str(artifact.bundle.operation_id),
            "fingerprint": artifact.fingerprint, "source_sha256": artifact.source_sha256,
            "target": artifact.bundle.target.model_dump(),
            "identity": artifact.bundle.identity.model_dump(mode="json"), "mode": args.mode,
        }
        print(json.dumps({**context, "status": "inputs_verified"}), flush=True)
        # A historical artifact root supplies evidence bytes, never executable imports.
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
        return 2 if args.mode == "reconcile-recovery" and result["status"] != "MATCHING" else 0
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
