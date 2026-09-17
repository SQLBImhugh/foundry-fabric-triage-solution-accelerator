"""Deployer-only registration DDL and catalogue. No runtime grants or SQL calls.

The two append-only tables are separate from the kernel's physical-table map.
Discovery captures use the operator-only operation in the existing receipts
table; the authority checker must prove that runtime code cannot forge it.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass

from triage.monitoring.deployment_contracts import CAPTURE_OPERATION, fingerprint
from triage.monitoring.sql_permissions import (
    COMPONENTS,
    KERNEL_VERSION,
    build_permission_kernel,
    rpc_contracts,
)
from triage.monitoring.sql_permissions import integration_contract as kernel_integration_contract
from triage.store.azure_sql import DEFAULT_TABLES, quote_identifier
from triage.store.azure_sql import schema_statements as application_statements

READ_ONLY_RPCS = frozenset({"inspect", "lock_context", "controller.inspect_frontiers"})


@dataclass(frozen=True)
class RegistrationNames:
    registration: str = "triage_deployment_registration"
    writers: str = "triage_deployment_writers"
    read_projection: str = "triage_deployment_writer_authority"

    def __post_init__(self) -> None:
        values = (self.registration, self.writers, self.read_projection)
        for value in values:
            quote_identifier(value)
        if len({value.casefold() for value in values}) != len(values):
            raise ValueError("Registration object names must be distinct")

    @property
    def tables(self) -> dict[str, str]:
        return {"deployment_registration": self.registration, "deployment_writers": self.writers}


DEFAULT_REGISTRATION_NAMES = RegistrationNames()


def module_definition(ddl: str) -> str:
    match = re.search(r"\bCREATE(?:\s+OR\s+ALTER)?\s+(?:PROCEDURE|VIEW|FUNCTION)\b", ddl, re.I)
    if match is None:
        raise ValueError("Not a declared SQL module")
    return ddl[match.start():].strip().replace("\r\n", "\n")


def native_module_hash(ddl: str) -> str:
    """SQL hashes NVARCHAR bytes; this is not the UTF-8 catalogue/ABI hash."""
    definition = module_definition(ddl)
    # A native Azure SQL VIEW probe stored CREATE OR ALTER as CREATE   VIEW:
    # only the OR/ALTER tokens disappeared; separators and body text survived.
    definition = re.sub(
        r"\A(CREATE)(\s+)OR(\s+)ALTER(\s+)(?=(?:PROCEDURE|VIEW|FUNCTION)\b)",
        lambda match: "".join(match.groups()),
        definition, count=1, flags=re.I,
    )
    return hashlib.sha256(definition.encode("utf-16-le")).hexdigest()


def unqualified(name: str) -> str:
    match = re.fullmatch(r"(?:\[dbo\]\.\[([A-Za-z_][A-Za-z0-9_]*)\]|dbo\.([A-Za-z_][A-Za-z0-9_]*)|([A-Za-z_][A-Za-z0-9_]*))", name)
    if not match:
        raise ValueError("Operator catalogue requires a declared dbo object or role")
    return next(part for part in match.groups() if part is not None)


def kernel_abi(tables: Mapping[str, str] | None = None) -> dict:
    kernel = build_permission_kernel(tables)
    if tables is not None and set(kernel.names.tables) != set(build_permission_kernel().names.tables):
        raise ValueError("Only kernel physical-table overrides belong in its ABI; registration names are separate")
    integration = kernel_integration_contract(tables)
    return {
        "kernel_version": KERNEL_VERSION,
        "table_map": dict(kernel.names.tables),
        "namespace_suffix": kernel.names.suffix,
        "roles": {component: kernel.names.role(component) for component in COMPONENTS},
        "objects": list(kernel.catalogue()),
        "grants": {component: list(kernel.grants[component]) for component in COMPONENTS},
        "rpcs": {
            name: {
                "object_name": rpc.object_name,
                "parameters": [
                    {"name": p.name, "sql_type": p.sql_type, "nullable": p.nullable}
                    for p in rpc.parameters
                ],
                "components": list(rpc.components), "mutating": rpc.mutating,
                "implemented": rpc.implemented, "blocked_cases": list(rpc.blocked_cases),
                "result_fields": list(rpc.result_fields),
                "not_acquired_fields": list(rpc.not_acquired_fields), "statuses": list(rpc.statuses),
            } for name, rpc in kernel.rpcs.items()
        },
        "read_routes": integration["read_routes"],
        "write_routes": integration["write_routes"],
    }


def kernel_contract_hash(tables: Mapping[str, str] | None = None) -> str:
    return fingerprint(kernel_abi(tables))


def declared_write_procedures() -> frozenset[str]:
    """Names for resource-path validation, not native module authority.

    The SQL authority reader separately verifies the full generated definitions.
    An HTTP connection read needs only the fixed RPC and application catalogue;
    it must not regenerate all module bodies for each workflow action.
    """
    names = {unqualified(rpc.object_name) for name, rpc in rpc_contracts().items() if name not in READ_ONLY_RPCS}
    for ddl in application_statements(dict(DEFAULT_TABLES)):
        match = re.search(r"CREATE\s+OR\s+ALTER\s+PROCEDURE\s+(\[dbo\]\.\[\w+\]|dbo\.\w+)", ddl, re.I)
        if match:
            names.add(unqualified(match[1]))
    return frozenset(names)


def schema_statements(names: RegistrationNames = DEFAULT_REGISTRATION_NAMES) -> tuple[str, ...]:
    header, writers = quote_identifier(names.registration), quote_identifier(names.writers)
    return (
        f"""CREATE TABLE {header} (
    revision BIGINT NOT NULL PRIMARY KEY CHECK (revision > 0),
    binding_id NVARCHAR(256) NOT NULL CHECK (LEN(binding_id) > 0),
    registration_request_id UNIQUEIDENTIFIER NOT NULL UNIQUE,
    request_fingerprint CHAR(64) NOT NULL,
    tenant_id UNIQUEIDENTIFIER NOT NULL,
    sql_server NVARCHAR(253) NOT NULL,
    sql_database NVARCHAR(128) NOT NULL,
    server_identity NVARCHAR(512) NOT NULL,
    database_id INT NOT NULL CHECK (database_id > 0),
    reset_catalogue_hash CHAR(64) NOT NULL,
    kernel_contract_hash CHAR(64) NOT NULL,
    authority_snapshot_hash CHAR(64) NOT NULL,
    discovery_capture_id NVARCHAR(256) NOT NULL,
    discovery_capture_hash CHAR(64) NOT NULL,
    writer_rows_hash CHAR(64) NOT NULL,
    writer_count INT NOT NULL CHECK (writer_count BETWEEN 1 AND 100),
    recorded_at DATETIME2(6) NOT NULL,
    registrar_object_id UNIQUEIDENTIFIER NOT NULL,
    registrar_sql_principal_id INT NOT NULL
)""",
        f"""CREATE TABLE {writers} (
    revision BIGINT NOT NULL REFERENCES {header}(revision),
    writer_id NVARCHAR(256) NOT NULL,
    writer_kind VARCHAR(32) NOT NULL,
    resource_id NVARCHAR(1024) NULL,
    project_endpoint NVARCHAR(512) NULL,
    agent_name NVARCHAR(63) NULL,
    identity_client_id UNIQUEIDENTIFIER NOT NULL,
    identity_object_id UNIQUEIDENTIFIER NOT NULL,
    expected_sql_sid BINARY(16) NULL,
    invokes_writer_id NVARCHAR(256) NULL,
    configured_sql_server NVARCHAR(253) NULL,
    configured_sql_database NVARCHAR(128) NULL,
    resource_binding_hash CHAR(64) NOT NULL,
    invocation_binding_hash CHAR(64) NULL,
    PRIMARY KEY (revision, writer_id),
    CHECK ((expected_sql_sid IS NOT NULL AND invokes_writer_id IS NULL
            AND configured_sql_server IS NOT NULL AND configured_sql_database IS NOT NULL)
        OR (expected_sql_sid IS NULL AND invokes_writer_id IS NOT NULL
            AND invocation_binding_hash IS NOT NULL)),
    FOREIGN KEY (revision, invokes_writer_id) REFERENCES {writers}(revision, writer_id)
)""",
        f"""CREATE VIEW {quote_identifier(names.read_projection)} AS
SELECT h.revision, w.writer_id, w.writer_kind, w.resource_id, w.project_endpoint,
       w.agent_name, w.identity_client_id, w.identity_object_id, w.expected_sql_sid,
       w.invokes_writer_id, w.configured_sql_server, w.configured_sql_database,
       w.resource_binding_hash, w.invocation_binding_hash,
       p.principal_id AS sql_principal_id, p.type AS sql_principal_type,
       p.sid AS sql_principal_sid
FROM {header} h
LEFT JOIN {writers} w ON w.revision=h.revision
LEFT JOIN sys.database_principals p ON p.sid=w.expected_sql_sid
""",
    )


def object_catalogue(names: RegistrationNames = DEFAULT_REGISTRATION_NAMES) -> tuple[dict[str, str], ...]:
    return tuple({
        "logical_name": logical, "name": quote_identifier(name), "kind": kind,
        "sha256": hashlib.sha256(ddl.encode("utf-8")).hexdigest(),
        **({"native_sha256": native_module_hash(ddl)} if kind == "view" else {}),
        "reset_operation": "preserve_registration" if kind == "table" else "retain_definition",
    } for logical, name, kind, ddl in zip(
        ("deployment_registration", "deployment_writers", "deployment_writer_authority"),
        (names.registration, names.writers, names.read_projection),
        ("table", "table", "view"), schema_statements(names), strict=True,
    ))


def integration_contract(names: RegistrationNames = DEFAULT_REGISTRATION_NAMES) -> dict:
    return {
        "objects": object_catalogue(names),
        "statements": schema_statements(names),
        "kernel_contract_hash": kernel_contract_hash(),
        "preserved_receipt_operations": (CAPTURE_OPERATION,),
        "runtime_grants": (),
        "install": "Explicit deployer only; never alter an existing incompatible object.",
        "accept": (
            "Retire legacy broad roles first. Fresh SQL authority and independently enumerated "
            "deployment bindings, stopped writers and exact external-effect reconciliation "
            "are required. Preparation while unprotected is not acceptance."
        ),
        "scope": "dbo only; names are independently selectable, never kernel table-map entries.",
    }
