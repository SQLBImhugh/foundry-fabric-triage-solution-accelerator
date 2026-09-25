"""Deployment-only monitoring baseline DDL and idempotent initialization.

Runtime stores never call these helpers. Existing incident, approval and processed
tables are prerequisites owned by the application's deployment schema. There is
no migration, reset, import or schema-upgrade path here.

Registration DDL and preserved objects belong to deployment_schema. Its separate
RegistrationNames map must never enter the kernel table map. Registration is
installed only through the operator's confirmed install_registration operation.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import TYPE_CHECKING

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringNotBootstrapped,
)
from triage.store.azure_sql import DEFAULT_TABLES, quote_identifier

if TYPE_CHECKING:
    from triage.store.azure_sql import AzureSqlDatabase

DEFAULT_MONITORING_TABLES = {
    "monitoring_control": "triage_monitoring_control",
    "monitoring_records": "triage_monitoring_records",
    "monitoring_leases": "triage_monitoring_leases",
    "monitoring_receipts": "triage_monitoring_receipts",
}

# This derived lookup is not a promoted record field or part of its receipt hash.
# Indexing it avoids reparsing every retained binding for each catalogue page.
ACCEPTED_FACT_KEY_HASH_EXPRESSION = (
    "CONVERT(binary(32),CASE WHEN record_kind='accepted_fact' AND ISJSON(payload)=1 THEN "
    "HASHBYTES('SHA2_256', CONVERT(varchar(max), "
    "(JSON_VALUE(payload,'$.fact_key')) COLLATE Latin1_General_100_BIN2_UTF8)) END)"
)
# SQL Server rewrites computed expressions in sys.computed_columns. Deployer
# readback checks this native form, verified against the same generated DDL.
ACCEPTED_FACT_KEY_HASH_NATIVE_DEFINITION = (
    "(CONVERT([binary](32),case when [record_kind]='accepted_fact' AND isjson([payload])=(1) "
    "then hashbytes('SHA2_256',CONVERT([varchar](max),(json_value([payload],'$.fact_key')) "
    "collate Latin1_General_100_BIN2_UTF8))  end))"
)


def resolve_tables(
    db: AzureSqlDatabase | None = None, tables: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Honor the same table overrides as the shared AzureSqlDatabase handle."""
    shared = getattr(db, "_tables", None) or {}
    names = DEFAULT_TABLES | DEFAULT_MONITORING_TABLES | dict(shared) | dict(tables or {})
    for name in names.values():
        quote_identifier(name)
    return names


def resolve_kernel_tables(
    db: AzureSqlDatabase | None = None, tables: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve only the kernel's declared physical tables, never operator objects."""
    names = resolve_tables(db, tables)
    allowed = set(DEFAULT_TABLES) | set(DEFAULT_MONITORING_TABLES) | {"monitoring_rate_budget"}
    if set(names) - allowed:
        raise ValueError(
            "Unknown kernel table-map keys; registration names and reset aliases require a separate object map",
        )
    return names


def _index_name(table: str, suffix: str) -> str:
    digest = hashlib.sha256(table.encode("utf-8")).hexdigest()[:12]
    return f"ix_monitoring_{suffix}_{digest}"


def schema_statements(tables: Mapping[str, str] | None = None) -> tuple[str, ...]:
    names = resolve_tables(tables=tables)
    control = quote_identifier(names["monitoring_control"])
    records = quote_identifier(names["monitoring_records"])
    leases = quote_identifier(names["monitoring_leases"])
    receipts = quote_identifier(names["monitoring_receipts"])
    statements = [
        f"""IF OBJECT_ID(N'dbo.{names["monitoring_control"]}', N'U') IS NULL
CREATE TABLE {control} (
    singleton INT NOT NULL PRIMARY KEY CHECK (singleton = 1),
    schema_version INT NOT NULL,
    tenant_id NVARCHAR(36) NOT NULL,
    epoch NVARCHAR(36) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    activation_cutoff DATETIME2(6) NOT NULL,
    maintenance BIT NOT NULL,
    updated_at DATETIME2(6) NOT NULL,
    bootstrap_id NVARCHAR(36) NOT NULL,
    bootstrap_hash CHAR(64) NOT NULL,
    payload NVARCHAR(MAX) NOT NULL
)""",
        f"""IF OBJECT_ID(N'dbo.{names["monitoring_records"]}', N'U') IS NULL
CREATE TABLE {records} (
    tenant_id NVARCHAR(36) NOT NULL,
    epoch NVARCHAR(36) NOT NULL,
    record_kind VARCHAR(40) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    full_key NVARCHAR(1024) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    status VARCHAR(40) NULL,
    workload VARCHAR(32) NULL,
    workspace_id NVARCHAR(36) NULL,
    item_id NVARCHAR(36) NULL,
    target_hash BINARY(32) NULL,
    target_key NVARCHAR(1024) NULL,
    parent_hash BINARY(32) NULL,
    parent_key NVARCHAR(1024) NULL,
    work_kind VARCHAR(32) NULL,
    generation_id NVARCHAR(36) NULL,
    due_at DATETIME2(6) NULL,
    sequence_number BIGINT NULL,
    payload NVARCHAR(MAX) NOT NULL,
    accepted_fact_key_hash AS {ACCEPTED_FACT_KEY_HASH_EXPRESSION} PERSISTED,
    PRIMARY KEY (tenant_id, epoch, record_kind, key_hash)
)""",
        f"""IF OBJECT_ID(N'dbo.{names["monitoring_leases"]}', N'U') IS NULL
CREATE TABLE {leases} (
    tenant_id NVARCHAR(36) NOT NULL,
    epoch NVARCHAR(36) NOT NULL,
    key_hash BINARY(32) NOT NULL,
    full_key NVARCHAR(1024) NOT NULL,
    owner_id NVARCHAR(36) NOT NULL,
    fence BIGINT NOT NULL CHECK (fence > 0),
    acquired_at DATETIME2(6) NOT NULL,
    expires_at DATETIME2(6) NOT NULL,
    PRIMARY KEY (tenant_id, epoch, key_hash)
)""",
        f"""IF OBJECT_ID(N'dbo.{names["monitoring_receipts"]}', N'U') IS NULL
CREATE TABLE {receipts} (
    tenant_id NVARCHAR(36) NOT NULL,
    epoch NVARCHAR(36) NOT NULL,
    operation VARCHAR(40) NOT NULL,
    request_hash BINARY(32) NOT NULL,
    request_id NVARCHAR(256) NOT NULL,
    fingerprint CHAR(64) NOT NULL,
    recorded_at DATETIME2(6) NOT NULL,
    payload NVARCHAR(MAX) NOT NULL,
    PRIMARY KEY (tenant_id, epoch, operation, request_hash)
)""",
    ]
    index_definitions = [
        ("due", "tenant_id, epoch, record_kind, work_kind, status, due_at, workspace_id, key_hash", ""),
        ("scope", "tenant_id, epoch, record_kind, workspace_id, workload, key_hash", ""),
        ("target", "tenant_id, epoch, record_kind, target_hash, status, key_hash", ""),
        ("parent", "tenant_id, epoch, record_kind, parent_hash, sequence_number, key_hash", ""),
        ("generation", "tenant_id, epoch, record_kind, generation_id, key_hash", ""),
        ("accepted_fact", "tenant_id, epoch, record_kind, accepted_fact_key_hash",
         " WHERE record_kind = 'accepted_fact'"),
    ]
    for suffix, columns, where in index_definitions:
        name = _index_name(names["monitoring_records"], suffix)
        statements.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{name}' "
            f"AND object_id = OBJECT_ID(N'dbo.{names['monitoring_records']}')) "
            f"CREATE INDEX [{name}] ON {records} ({columns}){where}",
        )
    position_index = _index_name(names["monitoring_records"], "position")
    statements.append(
        f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{position_index}' "
        f"AND object_id = OBJECT_ID(N'dbo.{names['monitoring_records']}')) "
        f"CREATE UNIQUE INDEX [{position_index}] ON {records} "
        "(tenant_id, epoch, parent_hash, sequence_number) WHERE record_kind = 'stream_position'",
    )
    return tuple(statements)


def initialize_monitoring_schema(
    db: AzureSqlDatabase, *, control: m.DeploymentControl, bootstrap_id: str,
    tables: Mapping[str, str] | None = None,
) -> m.DeploymentControl:
    """Explicit deployment bootstrap; replaying its identity never clears new data.

    Call with the deployment identity after creating the shared application
    tables. Keep the original control/id when reconciling a lost acknowledgement.
    A different bootstrap identity or baseline is refused, not migrated/reset.
    """
    control = m.DeploymentControl.model_validate_json(control.model_dump_json())
    bootstrap_id = m.canonical_id(bootstrap_id)
    if control.revision != 0:
        raise MonitoringConflict("A new baseline starts at registry revision zero")
    names = resolve_tables(db, tables)
    control_table = quote_identifier(names["monitoring_control"])
    payload = control.model_dump_json()
    fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    with db.transaction():
        prerequisites = db.query(
            "SELECT OBJECT_ID(?, 'U'), OBJECT_ID(?, 'U'), OBJECT_ID(?, 'U')",
            f"dbo.{names['incidents']}", f"dbo.{names['approvals']}", f"dbo.{names['processed']}",
        )
        if not prerequisites or any(prerequisites[0][index] is None for index in range(3)):
            raise MonitoringNotBootstrapped("Create the shared incident, approval and processed tables first")
        for statement in schema_statements(names):
            db.execute(statement)
        existing = db.query(
            f"SELECT bootstrap_id, bootstrap_hash, payload FROM {control_table} "
            "WITH (UPDLOCK, HOLDLOCK) WHERE singleton = 1",
        )
        if existing:
            if len(existing) != 1 or existing[0][0] != bootstrap_id or existing[0][1] != fingerprint:
                raise MonitoringConflict("A different monitoring baseline already exists; no reset was performed")
            return m.DeploymentControl.model_validate_json(existing[0][2])
        for logical in ("monitoring_records", "monitoring_leases", "monitoring_receipts"):
            rows = db.query(f"SELECT COUNT(*) FROM {quote_identifier(names[logical])}")
            if not rows or rows[0][0] != 0:
                raise MonitoringConflict("Uninitialized monitoring tables are not empty")
        changed = db.execute(
            f"INSERT INTO {control_table} "
            "(singleton, schema_version, tenant_id, epoch, revision, activation_cutoff, maintenance, "
            "updated_at, bootstrap_id, bootstrap_hash, payload) "
            "VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            control.schema_version, control.tenant_id, control.epoch, control.revision,
            control.activation_cutoff.replace(tzinfo=None), control.maintenance,
            control.updated_at.replace(tzinfo=None), bootstrap_id, fingerprint, payload,
        )
        if changed != 1:
            raise MonitoringConflict("Monitoring bootstrap insert was not confirmed")
    return control


def runtime_table_permissions(tables: Mapping[str, str] | None = None) -> dict[str, tuple[str, ...]]:
    """Reject the retired broad-DML grant surface instead of emitting unsafe grants."""
    raise MonitoringKernelUnsupported(
        "Base-table runtime DML is not a component boundary. "
        "Use sql_permissions.runtime_grants with an explicit worker, web or controller component.",
    )


def permission_kernel_objects(tables: Mapping[str, str] | None = None) -> tuple[dict[str, str], ...]:
    """Kernel objects only, not a complete writer inventory or registration installer.

    Reset/deployer composition obtains the separate preserved registration
    objects from deployment_schema.object_catalogue(registration_names).
    """
    from triage.monitoring.sql_permissions import object_catalogue

    return object_catalogue(resolve_kernel_tables(tables=tables))


def initialize_monitoring_permission_kernel(
    db: AzureSqlDatabase, *, tables: Mapping[str, str] | None = None,
) -> None:
    """Explicit deployer operation after all physical tables and rate policies exist.

    This installs the platform-owned static catalogue; it creates no SQL users,
    supplies no runtime credentials and does not assert native role acceptance.
    Runtime stores never invoke it.
    """
    from triage.monitoring.sql_permissions import schema_statements as kernel_statements

    statements = kernel_statements(resolve_kernel_tables(db, tables))
    with db.transaction():
        for statement in statements:
            db.execute(statement)
