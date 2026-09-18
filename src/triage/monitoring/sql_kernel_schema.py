"""Roles and views for the deployer-only SQL kernel."""

from __future__ import annotations

from dataclasses import replace

from triage.monitoring.sql_kernel_common import (
    key_hash,
    literals,
    payload_hash,
    procedure,
    record_hash,
)
from triage.monitoring.sql_kernel_contracts import (
    CATALOGUE_KINDS,
    COMPONENTS,
    CONTROLLER_ACTION_KINDS,
    CONTROLLER_IMMUTABLE_KINDS,
    CONTROLLER_PROJECTION_KINDS,
    EVIDENCE_KINDS,
    FACT_KINDS,
    FRONTIER_KINDS,
    IDENTITY_COLUMNS,
    MUTABLE_FACT_COLUMNS,
    RECORD_COLUMNS,
    SOURCE_KINDS,
    TELEMETRY_KINDS,
    WORKER_WORK_KINDS,
    KernelObject,
    PermissionKernel,
    RpcContract,
    SqlNames,
)


def _columns(alias: str = "r") -> str:
    return ", ".join(f"{alias}.[{column}]" for column in RECORD_COLUMNS)


def _role(names: SqlNames, component: str) -> KernelObject:
    name = names.role(component)
    return KernelObject(f"role_{component}", name, "role", f"""
IF DATABASE_PRINCIPAL_ID(N'{name}') IS NULL
    CREATE ROLE [{name}] AUTHORIZATION [dbo];
ELSE IF NOT EXISTS (
    SELECT 1 FROM sys.database_principals
    WHERE name=N'{name}' AND type='R' AND is_fixed_role=0
      AND owning_principal_id=DATABASE_PRINCIPAL_ID(N'dbo')
) THROW 51076, 'Existing kernel role is not the expected owned database role', 1;
""".strip())


def _view(
    names: SqlNames, logical: str, predicate: str, *, writable: bool = False,
    deny_maintenance: bool = True,
) -> KernelObject:
    check = "\nWITH CHECK OPTION" if writable else ""
    maintenance = " AND c.maintenance=0" if writable and deny_maintenance else ""
    identity = f"""
  AND ISJSON(r.payload)=1 AND LEFT(LTRIM(r.payload),1)=N'{{'
  AND r.key_hash={key_hash('r.full_key')}
  AND ((r.target_key IS NULL AND r.target_hash IS NULL)
       OR (r.target_key IS NOT NULL AND r.target_hash={key_hash('r.target_key')}))
  AND ((r.parent_key IS NULL AND r.parent_hash IS NULL)
       OR (r.parent_key IS NOT NULL AND r.parent_hash={key_hash('r.parent_key')}))""" if writable else ""
    return KernelObject(logical, names.object(logical), "view", f"""CREATE OR ALTER VIEW {names.object(logical)}
WITH SCHEMABINDING
AS
SELECT {_columns()} FROM {names.table('monitoring_records')} AS r
JOIN {names.table('monitoring_control')} AS c
  ON c.singleton=1 AND c.tenant_id=r.tenant_id AND c.epoch=r.epoch
WHERE ({predicate}){maintenance}{identity}{check};""")


def _views(names: SqlNames) -> list[KernelObject]:
    facts = FACT_KINDS
    objects = [
        _view(names, "worker_catalogue", f"r.record_kind IN ({literals(CATALOGUE_KINDS)})", writable=True),
        _view(names, "worker_evidence", f"r.record_kind IN ({literals(EVIDENCE_KINDS)})", writable=True),
        _view(names, "worker_telemetry", f"r.record_kind IN ({literals(TELEMETRY_KINDS)})", writable=True),
        _view(names, "web_drafts", "r.record_kind='plan'", writable=True),
        _view(names, "controller_projections", f"r.record_kind IN ({literals(CONTROLLER_PROJECTION_KINDS)})", writable=True, deny_maintenance=False),
        _view(names, "controller_immutable", f"r.record_kind IN ({literals(CONTROLLER_IMMUTABLE_KINDS)})", writable=True, deny_maintenance=False),
        _view(
            names, "worker_read",
            f"r.record_kind IN ({literals((*facts, 'scope', 'target', 'target_capability', 'connector', 'connector_desired', 'partition_ownership', 'stream_start', 'stream_position', 'stream_checkpoint', 'stream_gap', 'validation_frontier', 'validation_window'))}) "
            f"OR (r.record_kind='work' AND r.work_kind IN ({literals(WORKER_WORK_KINDS)}))",
        ),
        _view(
            names, "web_read",
            f"r.record_kind IN ({literals(('plan', 'scope', 'review_request', 'review', 'target', 'target_capability', 'connector', 'receiver_heartbeat', 'web_reconcile_request', 'discovery_request', 'validation_frontier', 'validation_window', 'reconcile_acceptance'))})",
        ),
    ]
    # A raw worker insert is not controller evidence until the corresponding
    # protected binding AND immutable operation receipt exist and still match.
    # Catalogue reads need both canonical hash keys to avoid rescanning all
    # accepted JSON bindings per workspace. Full identities and hashes still match.
    objects.append(KernelObject("accepted_worker_facts", names.object("accepted_worker_facts"), "view", f"""CREATE OR ALTER VIEW {names.object('accepted_worker_facts')}
WITH SCHEMABINDING
AS
SELECT {_columns()} FROM {names.table('monitoring_records')} AS r
JOIN {names.table('monitoring_control')} AS c
  ON c.singleton=1 AND c.tenant_id=r.tenant_id AND c.epoch=r.epoch
WHERE r.record_kind IN ({literals(facts)})
AND EXISTS (
    SELECT 1 FROM {names.table('monitoring_records')} AS accepted
    JOIN {names.table('monitoring_receipts')} AS receipt
      ON receipt.tenant_id=accepted.tenant_id AND receipt.epoch=accepted.epoch
     AND receipt.operation IN ('worker.accept_facts','worker.commit_positions','worker.record_heartbeat')
     AND receipt.request_hash={key_hash("JSON_VALUE(accepted.payload,'$.batch_id')")}
     AND receipt.request_id=JSON_VALUE(accepted.payload,'$.batch_id')
     AND receipt.fingerprint=JSON_VALUE(accepted.payload,'$.batch_fingerprint')
    WHERE accepted.tenant_id=r.tenant_id AND accepted.epoch=r.epoch
      AND accepted.record_kind='accepted_fact'
      AND (r.record_kind NOT IN ({literals(CATALOGUE_KINDS)})
           OR accepted.key_hash={key_hash("N'accepted:'+receipt.request_id+N':'+r.record_kind+N':'+LOWER(CONVERT(char(64),r.key_hash,2))")})
      AND JSON_VALUE(accepted.payload,'$.fact_kind')=r.record_kind
      AND JSON_VALUE(accepted.payload,'$.fact_key')=r.full_key
      AND TRY_CONVERT(bigint,JSON_VALUE(accepted.payload,'$.fact_revision'))=r.revision
      AND JSON_VALUE(accepted.payload,'$.payload_hash')={payload_hash('r.payload')}
      AND JSON_VALUE(accepted.payload,'$.row_hash')={record_hash('r')}
);"""))
    owned = (
        *CONTROLLER_PROJECTION_KINDS, *CONTROLLER_ACTION_KINDS, *CONTROLLER_IMMUTABLE_KINDS,
        *FRONTIER_KINDS, *SOURCE_KINDS, "accepted_fact", "connector_desired", "stream_gap", "connector_source_retirement",
        "scope", "review_request", "plan", "connector", "work", "scheduler", "discovery_request",
        "web_reconcile_request", "worker_reconcile_request", "partition_ownership",
        "stream_start", "stream_position", "stream_checkpoint",
    )
    objects.append(KernelObject("controller_read", names.object("controller_read"), "view", f"""CREATE OR ALTER VIEW {names.object('controller_read')}
AS
SELECT {_columns()} FROM {names.table('monitoring_records')} AS r
JOIN {names.table('monitoring_control')} AS c
  ON c.singleton=1 AND c.tenant_id=r.tenant_id AND c.epoch=r.epoch
WHERE r.record_kind IN ({literals(owned)})
UNION ALL
SELECT {_columns('a')} FROM {names.object('accepted_worker_facts')} AS a;"""))
    objects.append(KernelObject("control_read", names.object("control_read"), "view", f"""CREATE OR ALTER VIEW {names.object('control_read')}
WITH SCHEMABINDING
AS SELECT singleton,schema_version,tenant_id,epoch,revision,activation_cutoff,
          maintenance,updated_at,bootstrap_id,bootstrap_hash,payload
FROM {names.table('monitoring_control')} WHERE singleton=1;"""))
    for component in COMPONENTS:
        objects.append(KernelObject(f"receipts_{component}", names.object(f"receipts_{component}"), "view", f"""CREATE OR ALTER VIEW {names.object(f'receipts_{component}')}
WITH SCHEMABINDING
AS SELECT r.tenant_id,r.epoch,r.operation,r.request_hash,r.request_id,
          r.fingerprint,r.recorded_at,r.payload
FROM {names.table('monitoring_receipts')} AS r
JOIN {names.table('monitoring_control')} AS c
  ON c.singleton=1 AND c.tenant_id=r.tenant_id AND c.epoch=r.epoch
WHERE r.operation LIKE N'{component}.%';"""))
    for logical, table, columns in (
        ("approval_read", "approvals", "request_id,decision,responder,decided_at,payload"),
        ("incident_read", "incidents", "incident_id,signature,status,updated_at,payload"),
        ("processed_read", "processed", "fingerprint,message_id,received_at"),
    ):
        objects.append(KernelObject(logical, names.object(logical), "view", f"""CREATE OR ALTER VIEW {names.object(logical)}
WITH SCHEMABINDING
AS SELECT {columns} FROM {names.table(table)};"""))
    return objects


def build_kernel(names: SqlNames, contracts: dict[str, RpcContract]) -> PermissionKernel:
    from triage.monitoring.sql_kernel_actions import action_procedures
    from triage.monitoring.sql_kernel_arguments import argument_function
    from triage.monitoring.sql_kernel_connectors import connector_procedures
    from triage.monitoring.sql_kernel_frontiers import frontier_procedures
    from triage.monitoring.sql_kernel_intake import intake_procedures
    from triage.monitoring.sql_kernel_intents import intent_procedures
    from triage.monitoring.sql_kernel_json import json_equal_function, json_string_function
    from triage.monitoring.sql_kernel_retention import retention_procedures
    from triage.monitoring.sql_kernel_sources import source_procedures
    from triage.monitoring.sql_kernel_work import work_procedures

    objects = [_role(names, component) for component in COMPONENTS]
    objects.append(json_string_function(names))
    objects.append(json_equal_function(names))
    objects.append(argument_function(names))
    objects.extend(_views(names))
    implementations = {}
    for factory in (
        intent_procedures, work_procedures, intake_procedures, action_procedures,
        frontier_procedures, connector_procedures,
        retention_procedures, source_procedures,
    ):
        for logical, definition in factory(names, contracts).items():
            if logical in implementations:
                raise ValueError(f"Duplicate SQL kernel procedure: {logical}")
            implementations[logical] = definition
    final_contracts = dict(contracts)
    for logical, contract in contracts.items():
        if logical in implementations:
            objects.append(implementations[logical])
        else:
            reason = "The reviewed operation-specific kernel has not been implemented"
            final_contracts[logical] = replace(contract, implemented=False, blocked_reason=reason)
            objects.append(procedure(
                names, contract,
                "THROW 51077, 'This SQL kernel operation is blocked, not a generic mutation fallback', 1;",
                permit_maintenance=True, check_revision=False,
            ))
    grants = {component: [] for component in COMPONENTS}

    def allow(component, permission, logical):
        grants[component].append(
            f"GRANT {permission} ON OBJECT::{names.object(logical)} TO [{names.role(component)}];"
        )

    for component in COMPONENTS:
        allow(component, "SELECT", "control_read")
        allow(component, "SELECT", f"{component}_read")
        allow(component, "SELECT", f"receipts_{component}")
    allow("web", "SELECT", "accepted_worker_facts")
    for component in ("web", "controller"):
        allow(component, "SELECT", "approval_read")
        allow(component, "SELECT", "incident_read")
    allow("controller", "SELECT", "processed_read")
    for logical in ("worker_catalogue", "worker_telemetry"):
        allow("worker", "SELECT, INSERT", logical)
        columns = ", ".join(f"[{column}]" for column in MUTABLE_FACT_COLUMNS)
        allow("worker", f"UPDATE ({columns})", logical)
    allow("worker", "SELECT, INSERT", "worker_evidence")
    allow("web", "SELECT, INSERT", "web_drafts")
    allow("web", "UPDATE ([revision], [status], [payload])", "web_drafts")
    allow("controller", "SELECT, INSERT", "controller_projections")
    allow("controller", "SELECT, INSERT", "controller_immutable")
    columns = ", ".join(f"[{column}]" for column in RECORD_COLUMNS if column not in IDENTITY_COLUMNS)
    allow("controller", f"UPDATE ({columns})", "controller_projections")
    for logical, contract in final_contracts.items():
        for component in contract.components:
            allow(component, "EXECUTE", logical.replace(".", "_"))
    required = (
        "monitoring_control", "monitoring_records", "monitoring_leases",
        "monitoring_receipts", "monitoring_rate_budget", "incidents", "approvals", "processed",
    )
    preconditions = (
        "SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;",
        "IF (SELECT compatibility_level FROM sys.databases WHERE database_id=DB_ID())<150 "
        "THROW 51076, 'Permission kernel requires database compatibility 150 or later', 1;",
    ) + tuple(
        f"IF OBJECT_ID(N'dbo.{names.tables[key]}',N'U') IS NULL "
        "THROW 51076, 'Create the reviewed physical baseline before the permission kernel', 1;"
        for key in required
    )
    return PermissionKernel(
        names, tuple(objects), final_contracts,
        {component: tuple(values) for component, values in grants.items()}, preconditions,
    )
