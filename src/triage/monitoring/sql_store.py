"""Component-scoped Azure SQL routes over the reviewed permission kernel.

Runtime routes never use base-table DML. Unsupported semantic transitions fail
before mutation; installing a kernel or choosing a component is not permission
to substitute an unguarded table update. SQL roles, not the component argument,
authorize every checked view and static procedure.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal, TypeVar

from pydantic import BaseModel, Field, TypeAdapter, ValidationError, model_validator

from triage.models import Incident
from triage.monitoring import models as m
from triage.monitoring.contracts import (
    ConnectorPublisher,
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringLeaseLost,
    MonitoringNotBootstrapped,
    MonitoringStoreError,
    MonitoringUnavailable,
)
from triage.monitoring.events import (
    ConnectorScope,
    OwnershipChange,
    PartitionOwnership,
    ReceiverHeartbeat,
    StreamStart,
    StreamStartRequest,
    UnidentifiedReceiptBatch,
    UnidentifiedSignal,
)
from triage.monitoring.memory import (
    SCAN_BUDGET,
    ActionOwner,
    MonitoringEngine,
    StoredReceipt,
    StoredRecord,
    _json,
    _reconciliation_policy_revision,
    _reconciliation_workspace,
    _stamp,
    _update,
    canonical_incident_id,
    key_digest,
    stable_id,
)
from triage.monitoring.rate_limit import RateDecision, RatePolicy, _delay, _ordered_policies
from triage.monitoring.schema import resolve_kernel_tables
from triage.monitoring.sql_kernel_contracts import (
    CATALOGUE_KINDS,
    CONTROLLER_IMMUTABLE_KINDS,
    CONTROLLER_PROJECTION_KINDS,
    EVIDENCE_KINDS,
    TELEMETRY_KINDS,
    Component,
)
from triage.monitoring.sql_permissions import build_permission_kernel, decode_rpc_result
from triage.policy import TriagePolicy
from triage.redaction import redact_text
from triage.store.azure_sql import AzureSqlDatabase, SqlCommitUncertain

logger = logging.getLogger("triage.monitoring.sql")
ModelT = TypeVar("ModelT", bound=BaseModel)

RECORD_COLUMNS = (
    "record_kind, full_key, revision, status, workload, workspace_id, item_id, "
    "target_key, parent_key, work_kind, generation_id, due_at, sequence_number, payload, key_hash"
)
FILTER_COLUMNS = {
    "status": "status", "workload": "workload", "workspace_id": "workspace_id",
    "item_id": "item_id", "work_kind": "work_kind", "generation_id": "generation_id",
    "target_key": "target_hash", "parent_key": "parent_hash",
}


def _digest(value: str | None) -> bytes | None:
    return bytes.fromhex(key_digest(value)) if value is not None else None


def _db_time(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def _read_time(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
    raise MonitoringUnavailable("SQL returned an invalid timestamp")


def _limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= 5_001:
        raise ValueError("A SQL monitoring batch must contain 1-5001 rows")
    return value


def _kernel_model(model: type[ModelT], value: object) -> ModelT:
    try:
        return model.model_validate(value)
    except ValidationError as exc:
        logger.error("Invalid guarded SQL result model=%s", model.__name__)
        raise MonitoringUnavailable(f"Guarded SQL returned invalid {model.__name__} state") from exc


@dataclass
class CollectionFrame:
    operation: str
    request_id: str
    fingerprint: str
    context: m.MonitoringContext
    request: BaseModel | dict[str, object]
    records: dict[tuple[str, str], StoredRecord] = field(default_factory=dict)
    pending_inserts: dict[tuple[str, str], StoredRecord] = field(default_factory=dict)
    part_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceFrame:
    work: m.MonitoringWork
    producer: m.ReconciliationRequest | None = None
    evidence: tuple[m.EvidenceBinding, ...] = ()
    alias_window_id: str | None = None


class _UnidentifiedReceipt(UnidentifiedSignal):
    status: Literal["quarantined"] = "quarantined"


class _StreamPositionEnvelope(m.MonitoringModel):
    receipt_kind: Literal["identified", "unidentified"]
    receipt_key: m.StateKey
    receipt: m.SignalReceipt | _UnidentifiedReceipt

    @model_validator(mode="after")
    def validate_identity(self) -> _StreamPositionEnvelope:
        identified = isinstance(self.receipt, m.SignalReceipt)
        if identified != (self.receipt_kind == "identified"):
            raise ValueError("Native position kind disagrees with its receipt")
        expected_key = (
            self.receipt.delivery.key if identified else
            f"{self.receipt.partition.key}:unidentified:{self.receipt.position.sequence_number}"
        )
        if self.receipt_key != expected_key:
            raise ValueError("Native receipt key must preserve the original transport identity")
        if identified and self.receipt.status == "accepted" and self.receipt.observation.authority != "transport":
            raise ValueError("Broker intake cannot assert authoritative REST or fixture evidence")
        return self


_STREAM_POSITIONS = TypeAdapter(Annotated[
    tuple[_StreamPositionEnvelope, ...], Field(min_length=1, max_length=m.MAX_INTAKE_BATCH),
])


class SqlBackend:
    fixture = False

    def __init__(
        self, db: AzureSqlDatabase, tables: Mapping[str, str] | None = None, *, component: Component,
    ) -> None:
        self.db = db
        self.component = TypeAdapter(Component).validate_python(component)
        self.names = resolve_kernel_tables(db, tables)
        self.kernel = build_permission_kernel(self.names)
        self.kernel_names = self.kernel.names
        self.contracts = self.kernel.rpcs
        self.tables = {
            "monitoring_control": self.kernel_names.object("control_read"),
            "monitoring_records": self.kernel_names.object(f"{component}_read"),
            "monitoring_receipts": self.kernel_names.object(f"receipts_{component}"),
            "incidents": self.kernel_names.object("incident_read"),
            "approvals": self.kernel_names.object("approval_read"),
            "processed": self.kernel_names.object("processed_read"),
        }
        self._local = threading.local()

    @property
    def transaction_active(self) -> bool:
        return bool(getattr(self._local, "active", False))

    @contextmanager
    def transaction(self, *, write: bool, operation: str, request_id: str) -> Iterator[None]:
        if getattr(self._local, "active", False):
            raise MonitoringConflict("Nested monitoring transactions are not supported")
        self.operation_identity(operation, request_id)
        try:
            with self.db.transaction():
                self._local.active = True
                self._local.records = {}
                self._local.control_cached = False
                self._local.control_row = None
                self._local.collection = None
                self._local.source_frame = None
                required = ("monitoring_control", "monitoring_records", "monitoring_receipts")
                rows = self.db.query(
                    "SELECT " + ", ".join("OBJECT_ID(?, 'V')" for _ in required),
                    *(self.tables[name] for name in required),
                )
                if len(rows) != 1 or len(rows[0]) != len(required):
                    raise MonitoringUnavailable("SQL schema inspection returned an invalid shape")
                present = {name: rows[0][index] is not None for index, name in enumerate(required)}
                self._local.control_exists = present["monitoring_control"]
                if present["monitoring_control"] and not all(present.values()):
                    missing = ", ".join(name for name, exists in present.items() if not exists)
                    raise MonitoringNotBootstrapped(f"Monitoring baseline is incomplete: {missing}")
                if present["monitoring_control"] and not write:
                    self.db.query(
                        f"SELECT singleton FROM {self.tables['monitoring_control']} "
                        "WITH (HOLDLOCK) WHERE singleton = 1",
                    )
                yield
        except SqlCommitUncertain as exc:
            operation = self._local.operation
            request_id = self._local.request_id
            logger.error("Monitoring commit unconfirmed operation=%s request_id=%s", operation, request_id)
            raise MonitoringCommitUncertain(operation, request_id) from exc
        except (MonitoringStoreError, ValidationError, ValueError):
            raise
        except Exception as exc:
            # The class alone is not actionable. A guarded refusal carries a
            # 5107x code and is decoded below, but everything else -- a
            # ProgrammingError from a renamed column, a DataError from an
            # oversized value -- looks identical in a log that prints only the
            # type, and an operator has nothing to act on. The message is
            # redacted and truncated for the same reason every other store
            # boundary redacts: a SQL error can quote the row that failed.
            detail = redact_text(str(exc))[:400]
            logger.error(
                "Monitoring SQL operation failed operation=%s error_type=%s detail=%s",
                operation, type(exc).__name__, detail,
            )
            code = re.search(r"\b(5107[0-7])\b", str(exc))
            if code is not None:
                error = {
                    "51070": MonitoringComponentDenied, "51072": MonitoringConflict,
                    "51073": MonitoringConflict, "51074": MonitoringLeaseLost,
                    "51076": MonitoringNotBootstrapped, "51077": MonitoringKernelUnsupported,
                    "51071": MonitoringConflict,
                }.get(code[1], MonitoringUnavailable)
                raise error(f"Guarded SQL operation {operation} was refused ({code[1]})") from exc
            raise MonitoringUnavailable(f"Monitoring SQL operation {operation} failed; shared state was not replaced") from exc
        finally:
            self._local.active = False
            self._local.control_exists = False
            self._local.records = {}
            self._local.control_cached = False
            self._local.control_row = None
            self._local.collection = None
            self._local.source_frame = None

    def operation_identity(self, operation: str, request_id: str) -> None:
        self._local.operation = operation
        self._local.request_id = request_id

    @property
    def collection(self) -> CollectionFrame | None:
        return getattr(self._local, "collection", None)

    @property
    def source_frame(self) -> SourceFrame | None:
        return getattr(self._local, "source_frame", None)

    def rpc(self, operation: str, arguments: Mapping[str, object]) -> dict:
        contract = self.contracts.get(operation)
        if contract is None or not contract.implemented:
            raise MonitoringKernelUnsupported(f"Required static SQL operation is unavailable: {operation}")
        if self.component not in contract.components:
            raise MonitoringComponentDenied(f"{self.component} cannot route {operation}")
        if not getattr(self._local, "active", False):
            raise MonitoringConflict("A guarded SQL operation must join the monitoring transaction")
        self._flush_inventory_inserts()
        self.operation_identity(operation, str(arguments.get("request_id") or arguments.get("work_id") or "read"))
        sql, values = contract.bind(arguments)
        rows = self.db.query(sql, *values)
        try:
            result = decode_rpc_result(contract, rows)
        except (ValueError, TypeError) as exc:
            raise MonitoringUnavailable(f"Guarded SQL operation {operation} returned an invalid result") from exc
        self._local.records = {}
        self._local.control_cached = False
        return result

    def lock_context(self, context: m.MonitoringContext) -> None:
        self.rpc("lock_context", _stamp(context))

    def read_route(self, kind: str) -> str:
        if self.component == "web" and kind in (*CATALOGUE_KINDS, *EVIDENCE_KINDS, *TELEMETRY_KINDS):
            return self.kernel_names.object("accepted_worker_facts")
        return self.tables["monitoring_records"]

    def write_route(self, kind: str, *, insert: bool) -> str:
        if self.component == "worker":
            if kind in CATALOGUE_KINDS:
                return self.kernel_names.object("worker_catalogue")
            if kind in TELEMETRY_KINDS:
                return self.kernel_names.object("worker_telemetry")
            if kind in EVIDENCE_KINDS and insert:
                return self.kernel_names.object("worker_evidence")
        elif self.component == "web" and kind == "plan":
            return self.kernel_names.object("web_drafts")
        elif self.component == "controller":
            if kind in CONTROLLER_PROJECTION_KINDS:
                return self.kernel_names.object("controller_projections")
            if kind in CONTROLLER_IMMUTABLE_KINDS and insert:
                return self.kernel_names.object("controller_immutable")
        raise MonitoringComponentDenied(f"{self.component} cannot directly mutate record family {kind}")

    def now(self) -> datetime:
        rows = self.db.query("SELECT SYSUTCDATETIME()")
        if len(rows) != 1 or len(rows[0]) != 1:
            raise MonitoringUnavailable("SQL did not return one authoritative clock value")
        return _read_time(rows[0][0])

    def control(self) -> dict[str, object] | None:
        if not getattr(self._local, "control_exists", False):
            return None
        if self._local.control_cached:
            return deepcopy(self._local.control_row)
        rows = self.db.query(
            f"SELECT schema_version, tenant_id, epoch, revision, activation_cutoff, maintenance, updated_at, payload "
            f"FROM {self.tables['monitoring_control']} WHERE singleton = 1",
        )
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0]) != 8:
            raise MonitoringUnavailable("SQL returned ambiguous deployment control")
        row = rows[0]
        if row[0] != m.MONITORING_SCHEMA_VERSION:
            return {"schema_version": row[0]}
        control = m.DeploymentControl.model_validate_json(row[7])
        promoted = (
            row[0], row[1], row[2], row[3], _read_time(row[4]), bool(row[5]), _read_time(row[6]),
        )
        expected = (
            control.schema_version, control.tenant_id, control.epoch, control.revision,
            control.activation_cutoff, control.maintenance, control.updated_at,
        )
        if promoted != expected:
            raise MonitoringUnavailable("Deployment control columns disagree with their validated payload")
        self._local.control_row = control.model_dump(mode="json")
        self._local.control_cached = True
        return deepcopy(self._local.control_row)

    def write_control(self, value: m.DeploymentControl, expected_revision: int) -> None:
        raise MonitoringKernelUnsupported("Control revisions change only inside the typed web intent procedure")

    def _record(self, row: object, context: m.MonitoringContext) -> StoredRecord:
        if len(row) != 15:
            raise MonitoringUnavailable("SQL monitoring record has an invalid shape")
        if bytes(row[14]) != _digest(row[1]):
            raise MonitoringUnavailable("Stored record digest does not match its full key")
        return StoredRecord(
            kind=row[0], key=row[1], context=m.MonitoringContext(tenant_id=context.tenant_id, epoch=context.epoch),
            version=row[2], status=row[3], workload=row[4], workspace_id=row[5], item_id=row[6],
            target_key=row[7], parent_key=row[8], work_kind=row[9], generation_id=row[10],
            due_at=_read_time(row[11]) if row[11] is not None else None,
            sequence_number=row[12], payload=row[13],
        )

    def get(self, kind: str, key: str, context: m.MonitoringContext) -> StoredRecord | None:
        cache_key = (context.tenant_id, context.epoch, kind, key)
        if cache_key in self._local.records:
            return self._local.records[cache_key]
        rows = self.db.query(
            f"SELECT {RECORD_COLUMNS} FROM {self.read_route(kind)} "
            "WHERE tenant_id = ? AND epoch = ? AND record_kind = ? AND key_hash = ?",
            context.tenant_id, context.epoch, kind, _digest(key),
        )
        if not rows:
            self._local.records[cache_key] = None
            return None
        if len(rows) != 1:
            raise MonitoringUnavailable("SQL record lookup was not unique")
        result = self._record(rows[0], context)
        if result.key != key:
            raise MonitoringConflict("Record digest collision; full keys differ")
        self._local.records[cache_key] = result
        return result

    def put(self, record: StoredRecord) -> None:
        table = self.write_route(record.kind, insert=record.version == 1)
        if not getattr(self._local, "active", False):
            raise MonitoringConflict("Monitoring records must be written inside their acceptance transaction")
        if record.version == 1 and self.component == "worker" and self.collection is not None and self.collection.operation == "inventory":
            # A workspace page writes both generation and current catalogue rows.
            # Per-row network round trips can exhaust the work lease before
            # acceptance; bounded inserts retain the same checked view and transaction.
            self.collection.pending_inserts[(record.kind, record.key)] = record
            self.collection.records[(record.kind, record.key)] = record
            self._local.records[(record.context.tenant_id, record.context.epoch, record.kind, record.key)] = record
            return
        self._flush_inventory_inserts()
        values = (
            record.version, record.status, record.workload, record.workspace_id, record.item_id,
            _digest(record.target_key), record.target_key, _digest(record.parent_key), record.parent_key,
            record.work_kind, record.generation_id, _db_time(record.due_at) if record.due_at else None,
            record.sequence_number, record.payload,
        )
        key = (record.context.tenant_id, record.context.epoch, record.kind, _digest(record.key))
        if record.version == 1:
            changed = self.db.execute(
                f"INSERT INTO {table} "
                "(full_key, revision, status, workload, workspace_id, item_id, target_hash, target_key, "
                "parent_hash, parent_key, work_kind, generation_id, due_at, sequence_number, payload, "
                "tenant_id, epoch, record_kind, key_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                record.key, *values, *key,
            )
        elif self.component == "web" and record.kind == "plan":
            changed = self.db.execute(
                f"UPDATE {table} SET revision = ?, status = ?, payload = ? "
                "WHERE tenant_id = ? AND epoch = ? AND record_kind = ? AND key_hash = ? "
                "AND full_key = ? AND revision = ?",
                record.version, record.status, record.payload, *key, record.key, record.version - 1,
            )
        else:
            changed = self.db.execute(
                f"UPDATE {table} SET revision = ?, status = ?, "
                "workload = ?, workspace_id = ?, item_id = ?, target_hash = ?, target_key = ?, "
                "parent_hash = ?, parent_key = ?, work_kind = ?, generation_id = ?, due_at = ?, "
                "sequence_number = ?, payload = ? WHERE tenant_id = ? AND epoch = ? "
                "AND record_kind = ? AND key_hash = ? AND full_key = ? AND revision = ?",
                *values, *key, record.key, record.version - 1,
            )
        if changed != 1:
            raise MonitoringConflict("A monitoring record update lost its conditional revision")
        self._local.records[(record.context.tenant_id, record.context.epoch, record.kind, record.key)] = record
        if self.collection is not None:
            self.collection.records[(record.kind, record.key)] = record

    def _flush_inventory_inserts(self) -> None:
        frame = self.collection
        if frame is None or not frame.pending_inserts:
            return
        grouped: dict[str, list[StoredRecord]] = {}
        for record in frame.pending_inserts.values():
            grouped.setdefault(self.write_route(record.kind, insert=True), []).append(record)
        for table, records in grouped.items():
            for offset in range(0, len(records), 50):
                batch = records[offset:offset + 50]
                values = []
                for row in batch:
                    values.extend((
                        row.key, row.version, row.status, row.workload, row.workspace_id, row.item_id,
                        _digest(row.target_key), row.target_key, _digest(row.parent_key), row.parent_key,
                        row.work_kind, row.generation_id, _db_time(row.due_at) if row.due_at else None,
                        row.sequence_number, row.payload, row.context.tenant_id, row.context.epoch,
                        row.kind, _digest(row.key),
                    ))
                placeholders = ", ".join("(" + ", ".join("?" for _ in range(19)) + ")" for _ in batch)
                changed = self.db.execute(
                    f"INSERT INTO {table} "
                    "(full_key, revision, status, workload, workspace_id, item_id, target_hash, target_key, "
                    "parent_hash, parent_key, work_kind, generation_id, due_at, sequence_number, payload, "
                    f"tenant_id, epoch, record_kind, key_hash) VALUES {placeholders}",
                    *values,
                )
                if changed != len(batch):
                    raise MonitoringConflict("An inventory insert batch did not persist every checked row")
        frame.pending_inserts.clear()

    def _filters(self, filters: dict[str, object] | None) -> tuple[str, list[object]]:
        clauses = []
        params = []
        for name, value in (filters or {}).items():
            if name in {"sequence_min", "sequence_max", "due_before"}:
                column, operator = {
                    "sequence_min": ("sequence_number", ">="),
                    "sequence_max": ("sequence_number", "<="), "due_before": ("due_at", "<="),
                }[name]
                clauses.append(f"{column} {operator} ?")
                params.append(_db_time(value) if isinstance(value, datetime) else value)
            elif name == "status_in":
                if not value:
                    clauses.append("1 = 0")
                else:
                    clauses.append("status IN (" + ", ".join("?" for _ in value) + ")")
                    params.extend(value)
            elif name in FILTER_COLUMNS:
                column = FILTER_COLUMNS[name]
                if value is None:
                    clauses.append(f"{column} IS NULL")
                else:
                    clauses.append(f"{column} = ?")
                    params.append(_digest(value) if name in {"parent_key", "target_key"} else value)
            else:
                raise ValueError(f"Unsupported monitoring index filter: {name}")
        return (" AND " + " AND ".join(clauses) if clauses else ""), params

    def scan(
        self, kind: str, context: m.MonitoringContext, *, limit: int,
        after: str | None = None, filters: dict[str, object] | None = None,
    ) -> list[StoredRecord]:
        self._flush_inventory_inserts()
        where, params = self._filters(filters)
        if after is not None:
            where += " AND key_hash > ?"
            params.append(bytes.fromhex(after))
        rows = self.db.query(
            f"SELECT TOP ({_limit(limit)}) {RECORD_COLUMNS} FROM {self.read_route(kind)} "
            "WHERE tenant_id = ? AND epoch = ? AND record_kind = ?" + where + " ORDER BY key_hash",
            context.tenant_id, context.epoch, kind, *params,
        )
        return [self._record(row, context) for row in rows]

    def count(
        self, kind: str, context: m.MonitoringContext, *, filters: dict[str, object] | None = None,
    ) -> int:
        self._flush_inventory_inserts()
        where, params = self._filters(filters)
        rows = self.db.query(
            f"SELECT COUNT(*) FROM {self.read_route(kind)} "
            "WHERE tenant_id = ? AND epoch = ? AND record_kind = ?" + where,
            context.tenant_id, context.epoch, kind, *params,
        )
        if len(rows) != 1 or len(rows[0]) != 1:
            raise MonitoringUnavailable("SQL count did not return one value")
        return rows[0][0]

    def change_counter(self, kind: str, context: m.MonitoringContext) -> int:
        self._flush_inventory_inserts()
        rows = self.db.query(
            f"SELECT COALESCE(SUM(revision), 0) FROM {self.read_route(kind)} "
            "WHERE tenant_id = ? AND epoch = ? AND record_kind = ?",
            context.tenant_id, context.epoch, kind,
        )
        if len(rows) != 1 or len(rows[0]) != 1:
            raise MonitoringUnavailable("SQL cursor revision did not return one value")
        return rows[0][0]

    def _fair_workspace(self, request: m.WorkClaimRequest) -> str:
        """Read a scheduling hint from guarded work progress, not a new authority."""
        table = self.tables["monitoring_records"]
        receipts = self.tables["monitoring_receipts"]
        family = sorted(m.CONTROLLER_WORK_KINDS)
        kinds = ", ".join("?" for _ in family)
        # SQL has no writable scheduler route. Active leases and original
        # transition receipts retain progress across controller reconstruction.
        rows = self.db.query(
            f"""WITH recent AS (
    SELECT w.*, COALESCE(
        TRY_CONVERT(datetime2(6), JSON_VALUE(w.payload, '$.lease.acquired_at')),
        TRY_CONVERT(datetime2(6), JSON_VALUE(w.payload, '$.completed_at'))
    ) AS served_at
    FROM {table} AS w
    WHERE w.tenant_id = ? AND w.epoch = ? AND w.record_kind = 'work'
      AND w.work_kind IN ({kinds})
    UNION ALL
    SELECT w.*, receipt.recorded_at AS served_at
    FROM {table} AS w
    JOIN {receipts} AS receipt ON receipt.tenant_id = w.tenant_id AND receipt.epoch = w.epoch
        AND receipt.operation = 'controller.transition_work'
        AND JSON_VALUE(receipt.payload, '$.result.work_id') = w.full_key
        AND JSON_VALUE(receipt.payload, '$.result.work.work_id') = w.full_key
    WHERE w.tenant_id = ? AND w.epoch = ? AND w.record_kind = 'work'
      AND w.work_kind IN ({kinds})
)
SELECT TOP (1) {RECORD_COLUMNS}
FROM recent WHERE served_at IS NOT NULL
ORDER BY served_at DESC, key_hash DESC""",
            request.tenant_id, request.epoch, *family, request.tenant_id, request.epoch, *family,
        )
        if not rows:
            return ""
        if len(rows) != 1:
            raise MonitoringUnavailable("SQL work progress returned an ambiguous scheduling cursor")
        record = self._record(rows[0], request)
        work = _kernel_model(m.MonitoringWork, json.loads(record.payload))
        if work.work_id != record.key or _stamp(work) != _stamp(request):
            raise MonitoringUnavailable("SQL work progress lost its exact work context")
        if work.kind == "reconcile_state":
            return _reconciliation_workspace(record, work)
        return record.workspace_id or ""

    def due(self, request: m.WorkClaimRequest, *, after_workspace: str) -> list[StoredRecord]:
        kinds = ", ".join("?" for _ in request.kinds)
        family = sorted(m.WORKER_WORK_KINDS if self.component == "worker" else m.CONTROLLER_WORK_KINDS)
        family_params = ", ".join("?" for _ in family)
        table = self.tables["monitoring_records"]
        if self.component == "controller":
            # Validate before active counts and TOP: a conflicting active row
            # could otherwise charge another workspace and exceed the real cap.
            conflicts = self.db.query(
                f"SELECT TOP (1) {RECORD_COLUMNS} FROM {table} "
                "WHERE tenant_id = ? AND epoch = ? AND record_kind = 'work' "
                "AND work_kind = 'reconcile_state' AND workspace_id IS NOT NULL "
                "AND status IN ('queued', 'waiting', 'leased', 'finalizing') "
                "AND (JSON_VALUE(payload, '$.target.workspace_id') IS NULL "
                "OR workspace_id <> JSON_VALUE(payload, '$.target.workspace_id')) ORDER BY key_hash",
                request.tenant_id, request.epoch,
            )
            if conflicts:
                record = self._record(conflicts[0], request)
                logger.error(
                    "Reconciliation workspace promotion contradicts its canonical target key_hash=%s",
                    key_digest(record.key),
                )
                raise MonitoringUnavailable("Reconciliation workspace promotion contradicts its canonical target")
        current_policy = None
        if self.component == "controller" and "reconcile_state" in request.kinds:
            current_policy = _reconciliation_policy_revision(self, request)
            invalid = self.db.query(
                f"""SELECT TOP (1) {RECORD_COLUMNS} FROM {table}
WHERE tenant_id = ? AND epoch = ? AND record_kind = 'work'
  AND work_kind = 'reconcile_state' AND status IN ('queued', 'waiting', 'leased', 'finalizing')
  AND (
      (SELECT COUNT(*) FROM OPENJSON(payload) AS field WHERE field.[key] = 'policy_revision') <> 1
      OR NOT EXISTS (
          SELECT 1 FROM OPENJSON(payload) AS field
          WHERE field.[key] = 'policy_revision' AND field.[type] = 2
            AND field.[value] NOT LIKE '%[^0-9]%'
            AND TRY_CONVERT(bigint, field.[value]) BETWEEN 0 AND ?
      )
  )
ORDER BY key_hash""",
                request.tenant_id, request.epoch, current_policy,
            )
            if invalid:
                record = self._record(invalid[0], request)
                logger.error("Reconciliation policy is malformed or unpublished key_hash=%s", key_digest(record.key))
                raise MonitoringUnavailable("Reconciliation policy is malformed or unpublished")
        # This key changes only order within a publication partition; global
        # web priority, workspace rotation and ordinary work slots are unchanged.
        policy_order = (
            "CASE WHEN r.work_kind = 'reconcile_state' AND "
            "TRY_CONVERT(bigint, JSON_VALUE(r.payload, '$.policy_revision')) < ? THEN 1 ELSE 0 END, "
            if current_policy is not None else ""
        )
        priority = (
            "CASE WHEN r.work_kind = 'reconcile_state' "
            "AND JSON_VALUE(r.payload, '$.reconcile_producer') = 'web' THEN 0 ELSE 1 END"
        ) if self.component == "controller" else ""
        verification_due = f"""
      AND NOT EXISTS (
          SELECT 1 FROM {table} AS effect
          WHERE effect.tenant_id=r.tenant_id AND effect.epoch=r.epoch AND effect.record_kind='action'
            AND effect.full_key=JSON_VALUE(r.payload, '$.action_reservation_id')
            AND r.work_kind<>'finalize' AND r.status<>'finalizing'
            AND effect.status IN ('reserved','submitted','uncertain')
            AND TRY_CONVERT(datetime2(6), JSON_VALUE(effect.payload, '$.next_verification_at'))>SYSUTCDATETIME()
      )""" if self.component == "controller" else ""
        workspace_order = (
            "CASE WHEN workspace <= ? THEN 1 ELSE 0 END, workspace, publication_pool, due_at, key_hash"
            if self.component == "controller" else "due_at, workspace"
        )
        # Project one scheduling bucket for active counts, rank and controller
        # rotation. Return original records; never rewrite promoted history.
        rows = self.db.query(
            f"""WITH projected AS (
    SELECT r.*, COALESCE(r.workspace_id,
        CASE WHEN r.work_kind = 'reconcile_state'
            THEN JSON_VALUE(r.payload, '$.target.workspace_id') END, '') AS workspace,
        CASE WHEN r.work_kind = 'reconcile_state' THEN 1 ELSE 0 END AS publication_pool
    FROM {table} AS r
    WHERE r.tenant_id = ? AND r.epoch = ? AND r.record_kind = 'work'
), active AS (
    SELECT workspace, publication_pool, COUNT(*) AS active_count
    FROM projected
    WHERE work_kind IN ({family_params})
      AND status IN ('leased', 'finalizing') AND due_at > SYSUTCDATETIME()
    GROUP BY workspace, publication_pool
), candidates AS (
    SELECT r.*, {priority + " AS intent_priority, " if priority else ""}COALESCE(a.active_count, 0) AS active_count,
        ROW_NUMBER() OVER (
            PARTITION BY r.workspace, r.publication_pool
            ORDER BY {priority + ", " if priority else ""}{policy_order}r.due_at, r.key_hash
        ) AS workspace_rank
    FROM projected AS r
    LEFT JOIN active AS a ON a.workspace = r.workspace AND a.publication_pool = r.publication_pool
    WHERE r.status IN ('queued', 'waiting', 'leased', 'finalizing')
      AND r.due_at <= SYSUTCDATETIME() AND r.work_kind IN ({kinds}){verification_due}
)
SELECT TOP ({request.limit}) {RECORD_COLUMNS}
FROM candidates WHERE workspace_rank <= ? - active_count
ORDER BY {"intent_priority, " if priority else ""}workspace_rank,
    {workspace_order}""",
            request.tenant_id, request.epoch, *family,
            *((current_policy,) if current_policy is not None else ()), *request.kinds,
            request.per_workspace_limit, *((after_workspace,) if self.component == "controller" else ()),
        )
        return [self._record(row, request) for row in rows]

    def get_receipt(
        self, operation: str, request_id: str, context: m.MonitoringContext,
    ) -> StoredReceipt | None:
        if not operation.startswith(f"{self.component}."):
            raise MonitoringKernelUnsupported("SQL receipts require their exact component-qualified kernel operation")
        rows = self.db.query(
            f"SELECT request_id, fingerprint, recorded_at, payload FROM {self.tables['monitoring_receipts']} "
            "WHERE tenant_id = ? AND epoch = ? AND operation = ? AND request_hash = ?",
            context.tenant_id, context.epoch, operation, _digest(request_id),
        )
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0]) != 4 or rows[0][0] != request_id:
            raise MonitoringConflict("Idempotency receipt did not match its full request ID")
        return StoredReceipt(
            operation=operation, request_id=request_id, fingerprint=rows[0][1],
            context=m.MonitoringContext(tenant_id=context.tenant_id, epoch=context.epoch),
            recorded_at=_read_time(rows[0][2]), payload=rows[0][3],
        )

    def put_receipt(self, receipt: StoredReceipt) -> None:
        raise MonitoringKernelUnsupported("Only a guarded transition may insert its operation receipt")

    def connector_receipts(self, context, connector_id):
        raise MonitoringKernelUnsupported("Connector receipt history is validated only inside guarded SQL publication")

    def get_lease(self, context: m.MonitoringContext, key: str) -> m.LeaseToken | None:
        if key.startswith("work:v1:"):
            row = self.get("work", m.canonical_id(key.rsplit(":", 1)[1]), context)
            return m.MonitoringWork.model_validate_json(row.payload).lease if row else None
        if key.startswith("partition:v1:"):
            row = self.get("partition_ownership", key, context)
            payload = json.loads(row.payload) if row else {}
            return m.LeaseToken.model_validate(payload["lease"]) if payload.get("lease") else None
        raise MonitoringKernelUnsupported("Target-action lease checks belong inside the guarded controller operation")

    def acquire_lease(
        self, context: m.MonitoringContext, key: str, owner: str, seconds: int,
    ) -> m.LeaseToken | None:
        raise MonitoringKernelUnsupported("Lease acquisition requires a stored work-family or partition CAS procedure")

    def renew_lease(self, request: m.LeaseRenewal) -> m.LeaseToken:
        raise MonitoringKernelUnsupported("Renewal requires the original family-bound guarded transition")

    def release_lease(self, lease: m.LeaseToken) -> None:
        raise MonitoringKernelUnsupported("Release must be coupled to its guarded work or partition transition")

    def compare_exchange_partition_lease(
        self, partition: m.PartitionIdentity, expected: m.LeaseToken | None,
        *, owner_id: str | None, lease_seconds: int,
    ) -> m.LeaseToken | None:
        raise MonitoringKernelUnsupported("Partition ownership requires the typed worker.partition procedure")

    def approval(self, request_id: str) -> dict[str, object] | None:
        rows = self.db.query(
            f"SELECT decision,responder,decided_at,payload FROM {self.tables['approvals']} WHERE request_id = ?",
            request_id,
        )
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0]) != 4:
            raise MonitoringUnavailable("Approval read returned an invalid shape")
        payload = json.loads(rows[0][3])
        if payload.get("request_id") != request_id or any(
            rows[0][index] != payload.get(name) and not (
                rows[0][index] is None and payload.get(name) == ""
            ) for index, name in enumerate(("decision", "responder", "decided_at"))
        ):
            raise MonitoringUnavailable("Approval columns disagree with their persisted payload")
        return payload

    def consume_approval(self, request_id: str, fingerprint: str) -> bool:
        raise MonitoringKernelUnsupported("Only the guarded reservation may consume an approval")

    def incident(self, incident_id: str) -> str | None:
        rows = self.db.query(
            f"SELECT payload FROM {self.tables['incidents']} WHERE incident_id = ?",
            incident_id,
        )
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0]) != 1:
            raise MonitoringUnavailable("Incident lookup returned ambiguous payloads")
        return rows[0][0]

    def finalized_incident_payload(self, context: m.MonitoringContext, finalization_id: str) -> str:
        """Read the original NVARCHAR JSON fragment, never serialize a parsed result."""
        if self.component != "controller":
            raise MonitoringComponentDenied("Only the controller reads its original finalization payload")
        rows = self.db.query(
            f"SELECT JSON_QUERY(payload, '$.result.incident') FROM {self.tables['monitoring_receipts']} "
            "WHERE tenant_id = ? AND epoch = ? AND operation = 'controller.finalize' "
            "AND request_hash = ? AND request_id = ?",
            context.tenant_id, context.epoch, _digest(finalization_id), finalization_id,
        )
        if len(rows) != 1 or len(rows[0]) != 1 or not isinstance(rows[0][0], str):
            raise MonitoringUnavailable("The original finalization receipt has no exact incident JSON fragment")
        return rows[0][0]

    def explicit_definition_hash(self, observation_json: str) -> str | None:
        rows = self.db.query("SELECT JSON_QUERY(?, '$.observed_definition')", observation_json)
        if len(rows) != 1 or len(rows[0]) != 1 or (rows[0][0] is not None and not isinstance(rows[0][0], str)):
            raise MonitoringUnavailable("Original explicit observation definition could not be read as NVARCHAR")
        return hashlib.sha256(rows[0][0].encode("utf-16-le")).hexdigest().upper() if rows[0][0] is not None else None

    def write_incident(self, incident: Incident, payload: str, prior_payload: str | None) -> None:
        raise MonitoringKernelUnsupported("Incident persistence requires the guarded atomic finalization plan")

    def mark_processed(self, execution_key: str, at: datetime) -> None:
        raise MonitoringKernelUnsupported("Source disposition and processed state require guarded finalization")


class AzureSqlMonitoringStore(MonitoringEngine):
    """Explicit SQL component, checked views and static RPCs; no permissive default."""

    def __init__(
        self, *, db: AzureSqlDatabase, component: Component, tables: Mapping[str, str] | None = None,
        policy: TriagePolicy | None = None, redactor: Callable[[str], str] = redact_text,
    ) -> None:
        self._sql = SqlBackend(db, tables, component=component)
        super().__init__(self._sql, component=component, policy=policy, redactor=redactor)

    def _run_operation(self, method, write: bool, args: tuple, kwargs: dict):
        if method.__name__ in {
            "list_partition_ownership", "change_partition_ownership", "claim_partition",
            "ensure_stream_start", "get_stream_start", "record_stream_receipts",
            "record_unidentified_receipts", "get_stream_acceptance",
            "advance_stream_checkpoint", "get_stream_checkpoint", "record_receiver_heartbeat",
        } and self.component != "worker":
            raise MonitoringComponentDenied("Receiver persistence requires the worker SQL component")
        if method.__name__ in {
            "record_inventory", "record_capability", "record_rest_page", "preview_scope",
            "activate_scope", "record_safety_review", "request_discovery",
        }:
            context = (
                args[0].expected if method.__name__ in {"record_inventory", "preview_scope", "activate_scope", "record_safety_review"}
                else args[0].target if method.__name__ == "record_rest_page" else args[0]
            )
            self._sql.lock_context(context)
            return method(self, *args, **kwargs)
        handlers = {
            "enqueue_work": self._sql_enqueue_work,
            "claim_work": self._sql_claim_work,
            "renew_lease": self._sql_renew_lease,
            "disposition_work": self._sql_disposition_work,
            "complete_collection_work": self._sql_complete_collection_work,
            "record_receiver_heartbeat": self._sql_record_receiver_heartbeat,
            "list_partition_ownership": self._sql_list_partition_ownership,
            "change_partition_ownership": self._sql_change_partition_ownership,
            "claim_partition": self._sql_claim_partition,
            "ensure_stream_start": self._sql_ensure_stream_start,
            "get_stream_start": self._sql_get_stream_start,
            "record_stream_receipts": self._sql_record_stream_receipts,
            "record_unidentified_receipts": self._sql_record_unidentified_receipts,
            "get_stream_acceptance": self._sql_get_stream_acceptance,
            "advance_stream_checkpoint": self._sql_advance_stream_checkpoint,
            "get_stream_checkpoint": self._sql_get_stream_checkpoint,
            "get_safety_review_operation": self._sql_safety_review_operation,
            "get_operation_receipt": self._sql_operation_receipt,
            "get_reconciliation_request": self._sql_reconciliation_request,
            "reconcile_work": self._sql_reconcile_work,
            "reconcile_state": self._sql_reconcile_state,
            "record_action_rejection": self._sql_action_rejection,
            "record_action_submission": self._sql_action_submission,
            "record_action_outcome": self._sql_action_outcome,
            "reserve_action": self._sql_reserve_action,
            "finalize_work": self._sql_finalize_work,
            "get_source_disposition": self._sql_source_disposition,
            "observe_source": self._sql_observe_source,
            "bind_approval": self._sql_bind_approval,
            "publish_connector": self._connector_publication,
            "record_connector": self._sql_record_connector,
        }
        handler = handlers.get(method.__name__)
        if handler is not None:
            return handler(*args, **kwargs)
        if write or method.__name__ in {
            "list_partition_ownership", "get_stream_start", "get_stream_checkpoint",
        }:
            raise MonitoringKernelUnsupported(
                f"{method.__name__} requires a complete guarded semantic kernel binding; "
                "no raw-table or synchronous producer-publication fallback was attempted",
            )
        result = method(self, *args, **kwargs)
        if method.__name__ == "inspect_bootstrap" and result.status in {"ready", "maintenance"}:
            operations = {
                name: rpc for name, rpc in self._sql.contracts.items()
                if self.component in rpc.components
            }
            rows = self._sql.db.query(
                "SELECT " + ", ".join("OBJECT_ID(?, 'P')" for _ in operations),
                *(rpc.object_name for rpc in operations.values()),
            )
            if len(rows) != 1 or len(rows[0]) != len(operations):
                raise MonitoringUnavailable("Kernel procedure inspection returned an invalid shape")
            missing = tuple(
                name for index, (name, rpc) in enumerate(operations.items())
                if rows[0][index] is None or not rpc.implemented or rpc.blocked_cases
            )
            if missing:
                return m.BootstrapInspection(
                    status="kernel_incomplete", expected_tenant_id=result.expected_tenant_id,
                    found_schema_version=result.found_schema_version, control=result.control,
                    missing_operations=missing,
                    detail="Required component-scoped v2 procedures are missing or unavailable; no fallback was selected.",
                )
            inspected = self._sql.rpc("inspect", _stamp(result.control))["result"]
            if (
                inspected["tenant_id"] != result.control.tenant_id or inspected["epoch"] != result.control.epoch
                or inspected["revision"] != result.control.revision or inspected["maintenance"] != result.control.maintenance
            ):
                raise MonitoringUnavailable("The deployed v2 kernel inspection disagrees with current control")
        return result

    def _pending_validation(self, identity):
        for frontier in self._all("validation_frontier", identity, m.ValidationFrontier):
            if frontier.target is not None and frontier.target != identity:
                continue
            row = self._sql.get("validation_frontier", frontier.frontier_key, identity)
            if frontier.pending or row.status not in {"published", "rejected"}:
                return True
            if row.parent_key is not None:
                window = self._sql.get("validation_window", frontier.frontier_key, identity)
                if window is None or window.status not in {"validated", "rejected"}:
                    return True
        return False

    def _pending_frontier_count(self, context):
        return self._sql.count("validation_frontier", context, filters={"status": "pending_validation"})

    def _save_work(self, work):
        if work.state == "queued" and work.revision == 1 and work.lease is None:
            draft = m.MonitoringWorkDraft.model_validate(
                work.model_dump(include=set(m.MonitoringWorkDraft.model_fields)),
            )
            return self._sql_enqueue_work(draft)
        raise MonitoringKernelUnsupported("Existing work changes require their exact guarded transition")

    def _schedule_connector(self, target, control):
        if len(self._registered_connectors(control)) == 1:
            return None
        capability = self._get("target_capability", target.key, control, m.CapabilityObservation)
        if capability is None or capability.event_status != "verified":
            return None
        existing = [
            connector for connector in self._all("connector", control, m.OwnedConnectorManifest)
            if connector.state != "deleted" and any(
                source.target == target.identity for source in (*connector.sources, *connector.source_proposals)
            )
        ]
        if not existing:
            frame = self._sql.source_frame
            if frame is None or frame.producer is None:
                raise MonitoringConflict("Initial desired source needs its current controller reconciliation context")
            frontier = self._get("validation_frontier", frame.producer.frontier_key, control, m.ValidationFrontier)
            proposal_id = stable_id(control, f"connector-source-proposal:{target.key}")
            node_name = f"source_{proposal_id.replace('-', '')}"
            events = ("Microsoft.Fabric.JobEvents.ItemJobCreated", "Microsoft.Fabric.JobEvents.ItemJobFailed")
            proposal = m.ConnectorSourceProposal(
                proposal_id=proposal_id, node_name=node_name, source_id=None, target=target.identity, event_types=events,
            )
            definition = {"parts": {"eventstream.json": {
                "sources": [{
                    "name": node_name, "type": "FabricJobEvents", "properties": {
                        "eventScope": "Item", "workspaceId": target.identity.workspace_id,
                        "itemId": target.identity.item_id, "includedEventTypes": list(events),
                    },
                }],
                "operators": [],
                "streams": [{"name": "monitoring_stream", "type": "DefaultStream", "inputNodes": [{"name": node_name}]}],
                "destinations": [{
                    "name": "monitoring_endpoint", "type": "CustomEndpoint", "inputNodes": [{"name": "monitoring_stream"}],
                }],
            }}, "component_ids": {}}
            result = self._connector_publication(m.ConnectorPublicationRequest(
                request_id=stable_id(control, f"connector-proposal:{frame.work.work_id}:{target.key}"),
                expected=m.RegistryVersion(**_stamp(control), revision=control.revision),
                work_id=frame.work.work_id, lease=frame.work.lease, expected_work_revision=frame.work.revision,
                expected_frontier_revision=frontier.accepted_revision,
                connector_id=stable_id(control, f"connector-target:{target.key}"),
                ownership_id=stable_id(control, f"connector-owner:{target.key}"),
                expected_connector_revision=0, name="Pending monitoring connector",
                sources=(), source_proposals=(proposal,), desired_definition=definition,
                detail="Publish an explicit logical per-item source proposal without any physical component identity.",
            ))
            return self._connector_followup(result.connector, control)
        if len(existing) != 1:
            raise MonitoringConflict("A target has more than one owned desired event subscription")
        return None if existing[0].state == "ready" else self._connector_followup(existing[0], control)

    def _idempotent(self, operation, request_id, context, request, model, apply):
        web = operation in {"preview", "activation", "safety_review", "discovery"}
        if operation not in {"inventory", "capability", "rest_page", "preview", "activation", "safety_review", "discovery"}:
            raise MonitoringKernelUnsupported(f"{operation} has no guarded operation-receipt adapter")
        raw = request.model_dump(mode="json") if isinstance(request, BaseModel) else request
        fingerprint = key_digest(_json(raw))
        native = "web.commit_intent" if web else "worker.accept_facts"
        native_id = self._native_request_id(context, operation, request_id)
        self._control(context)
        replay = self._rpc_replay(native, native_id, context, fingerprint)
        if replay is not None:
            if web:
                return self._web_response(context, request_id, operation, replay)
            return self._collection_response(context, request_id, operation, replay)
        if self._sql.collection is not None:
            raise MonitoringConflict("A collection operation cannot nest another acceptance")
        self._sql._local.collection = CollectionFrame(
            operation=operation, request_id=request_id, fingerprint=fingerprint,
            context=m.MonitoringContext(**_stamp(context)), request=request,
        )
        try:
            apply()
            receipt = self._sql.get_receipt(native, native_id, context)
            if receipt is None:
                raise MonitoringUnavailable("The guarded operation did not commit its original receipt")
            self._sql.operation_identity(native, native_id)
            if web:
                return self._web_response(context, request_id, operation, self._receipt_result(receipt))
            return self._collection_response(context, request_id, operation, self._receipt_result(receipt))
        finally:
            self._sql._local.collection = None

    @staticmethod
    def _native_request_id(context, operation, request_id):
        return stable_id(context, f"preview:{request_id}") if operation == "preview" else request_id

    def _put(self, kind, key, context, value, **indices):
        frame = self._sql.collection
        if kind == "plan" and frame is not None and frame.operation == "preview":
            saved = self._persisted(value)
            reply = self._sql.rpc("web.commit_intent", {
                **_stamp(context), "request_id": self._native_request_id(context, "preview", frame.request_id),
                "fingerprint": frame.fingerprint,
                "expected_revision": saved.expected.revision, "intent_kind": "preview",
                "intent_id": saved.plan_id, "expected_intent_revision": 0,
                "intent_json": _json(saved.model_dump(mode="json")),
            })
            return _kernel_model(m.ActivationPlan, reply["result"]["original_intent"])
        return super()._put(kind, key, context, value, **indices)

    def _commit_scope_intent(self, control, definition):
        frame = self._sql.collection
        if frame is None or frame.operation != "activation":
            raise MonitoringKernelUnsupported("A scope edit requires its original typed activation operation")
        prior = self._sql.get("scope", definition.scope_id, control)
        result = self._sql.rpc("web.commit_intent", {
            **_stamp(control), "request_id": frame.request_id, "fingerprint": frame.fingerprint,
            "expected_revision": control.revision, "intent_kind": "scope", "intent_id": definition.scope_id,
            "expected_intent_revision": prior.version if prior else 0,
            "intent_json": _json(self._persisted(definition).model_dump(mode="json")),
        })["result"]
        receipt = self._sql.get_receipt("web.commit_intent", frame.request_id, control)
        updated = self._control(control)
        scope = m.ScopePolicy(
            **result["original_intent"], revision=result["policy_revision"], updated_at=receipt.recorded_at,
        )
        return updated, scope

    def _commit_review_intent(self, control, pending, expected_review_revision):
        frame = self._sql.collection
        if frame is None or frame.operation != "safety_review":
            raise MonitoringKernelUnsupported("A review edit requires its original typed intent operation")
        saved = self._persisted(pending)
        intent = saved.model_dump(mode="json", include={
            "review_id", "target", "action", "requested_state", "reviewer_id", "reviewed_at", "expires_at",
            "parameters", "parameter_hash", "definition_hash", "configuration_hash", "replay_safe", "detail",
        })
        result = self._sql.rpc("web.commit_intent", {
            **_stamp(control), "request_id": frame.request_id, "fingerprint": frame.fingerprint,
            "expected_revision": control.revision, "intent_kind": "review", "intent_id": saved.review_id,
            "expected_intent_revision": expected_review_revision, "intent_json": _json(intent),
        })["result"]
        receipt = self._sql.get_receipt("web.commit_intent", frame.request_id, control)
        return self._control(control), self._review_from_intent(
            result["original_intent"], revision=result["new_intent_revision"],
            expected_policy=control.revision, accepted_at=receipt.recorded_at,
        )

    def _decode(self, record, model):
        if record.kind == "stream_start" and model is StreamStart:
            value = _kernel_model(m.StreamStartRecord, json.loads(record.payload))
            if value.partition_key != record.key or value.first_sequence_number != record.sequence_number:
                raise MonitoringUnavailable("Native stream-start metadata disagrees with its record columns")
            return self._stream_start_result(value)
        if record.kind == "stream_checkpoint" and model is m.StreamCheckpoint:
            value = _kernel_model(m.StreamCheckpointRecord, json.loads(record.payload))
            if value.partition_key != record.key or value.revision != record.version:
                raise MonitoringUnavailable("Native checkpoint metadata disagrees with its record columns")
            return self._checkpoint_result(value.partition, value.model_dump())
        if record.kind == "partition_ownership" and model is PartitionOwnership:
            value = _kernel_model(m.PartitionOwnershipResult, json.loads(record.payload))
            return self._ownership_result(value.partition, value.model_dump())
        if record.kind == "source_disposition":
            value = json.loads(record.payload)
            execution = _kernel_model(m.SourceExecutionIdentity, value["execution"])
            result = self._sql_source_disposition(execution)
            if result is None:
                raise MonitoringUnavailable("A recorded source disposition disappeared during its read")
            return result
        if record.kind not in {"scope", "plan", "review_request"}:
            return super()._decode(record, model)
        try:
            document = json.loads(record.payload)
            request_id = m.canonical_id(document.pop("request_id"))
            policy_revision = TypeAdapter(m.Revision).validate_python(document.pop("policy_revision"))
            revision = TypeAdapter(m.PositiveRevision).validate_python(document.pop("revision"))
            if revision != record.version:
                raise ValueError("Native intent revision disagrees with its row")
            if record.kind == "plan":
                if (
                    self._native_request_id(record.context, "preview", document["idempotency_id"]) != request_id
                    or document["expected"]["revision"] != policy_revision
                ):
                    raise ValueError("Native plan lost its original request or policy binding")
                return _kernel_model(m.ActivationPlan, document)
            if record.kind == "scope":
                return _kernel_model(m.ScopePolicy, {**document, "revision": policy_revision, "updated_at": None})
            handoff = self._sql.get("web_reconcile_request", request_id, record.context)
            if handoff is None:
                raise MonitoringUnavailable("A review intent has no immutable accepted handoff")
            accepted = json.loads(handoff.payload)
            return self._review_from_intent(
                document, revision=revision, expected_policy=policy_revision - 1,
                accepted_at=TypeAdapter(m.UtcDateTime).validate_python(accepted["created_at"]),
            )
        except (ValueError, TypeError, KeyError) as exc:
            raise MonitoringUnavailable(f"Native {record.kind} intent is unreadable") from exc

    @staticmethod
    def _review_from_intent(intent, *, revision, expected_policy, accepted_at):
        parameters = intent.get("parameters")
        parameter_hash = intent.get("parameter_hash")
        redacted = parameters is None and parameter_hash is not None and parameter_hash != m._digest(None)
        return _kernel_model(m.SafetyReview, dict(
            review_id=intent["review_id"], target=intent["target"], action=intent["action"],
            revision=revision, policy_revision=expected_policy, state="pending",
            requested_state=intent["requested_state"], publication_status="pending_validation",
            reviewer_id=intent["reviewer_id"], reviewed_at=intent["reviewed_at"], expires_at=intent["expires_at"],
            parameters=parameters, parameter_hash=parameter_hash, parameters_redacted=redacted,
            definition_hash=intent.get("definition_hash"), configuration_hash=intent.get("configuration_hash"),
            replay_safe=intent.get("replay_safe", False), exact_correlation_verified=False, detail=intent["detail"],
        ))

    def _request_reconciliation(
        self, control, *, request_id, topic, reference_id, fingerprint, payload,
        target=None, window=None, evidence=(), producer_commit=None,
    ):
        frame = self._sql.collection
        if frame is not None and frame.operation in {"activation", "safety_review", "discovery"}:
            if frame.operation == "discovery":
                self._sql.rpc("web.commit_intent", {
                    **_stamp(control), "request_id": request_id, "fingerprint": frame.fingerprint,
                    "expected_revision": control.revision, "intent_kind": "discovery",
                    "intent_id": request_id, "expected_intent_revision": 0,
                    "intent_json": _json(payload["selector"]),
                })
            receipt = self._sql.get_receipt("web.commit_intent", request_id, control)
            if receipt is None:
                raise MonitoringUnavailable("An accepted web intent has no original guarded receipt")
            reply = self._receipt_result(receipt)
            return self._initial_reconciliation(
                control, reply["reconcile_work_id"], request_id, "web",
                policy_revision=reply["policy_revision"], created_at=receipt.recorded_at, target=target,
            )
        if frame is None or frame.operation not in {"inventory", "capability", "rest_page"} or producer_commit is None:
            raise MonitoringKernelUnsupported("Producer reconciliation requires a bound native collection operation")
        if frame.request_id != request_id:
            raise MonitoringConflict("Native intake cannot change the original API request identity")
        if frame.operation == "inventory":
            result = self._get("generation", reference_id, control, m.InventoryGeneration)
        elif frame.operation == "capability":
            result = self._get("capability", request_id, control, m.CapabilityObservation)
        else:
            progress = self._get("poll_progress", target.key, control, m.PollProgress)
            receipt_keys = []
            for kind, key in frame.records:
                if kind in {"rest_observation", "rest_powerbi_row", "quarantine"}:
                    receipt_keys.append(key)
            result = m.RestPageReceipt(
                intake=m.IntakeReceipt(
                    **_stamp(control), request_id=request_id, recorded_at=self._now(),
                    receipt_keys=tuple(receipt_keys), work_ids=(), publication_status="pending_validation",
                ),
                checkpoint=progress.checkpoint,
            )
        if result is None:
            raise MonitoringUnavailable("Collection acceptance has no canonical result material")
        raw_records = tuple(frame.records.values())
        groups = [raw_records[offset:offset + 199] for offset in range(0, len(raw_records), 199)] or [()]
        part_ids = tuple(
            stable_id(control, f"intake:{frame.operation}:{request_id}:part:{index}")
            for index in range(len(groups) - 1)
        ) + (request_id,)
        core = m.CollectionAcceptance(
            **_stamp(control), operation=frame.operation, request_id=request_id, fingerprint=frame.fingerprint,
            producer_commit=producer_commit, part_ids=part_ids, reference_id=reference_id, target=target,
            window=window, reconciliation_payload=payload, result=result,
        )
        core_key = f"intake:{frame.operation}:{request_id}"
        if self._sql.get("intake_disposition", core_key, control) is not None:
            raise MonitoringConflict("Unaccepted intake result material already exists; no orphan adoption was attempted")
        self._put(
            "intake_disposition", core_key, control, core, target_key=target.key if target else None,
            parent_key=producer_commit.work_id, generation_id=producer_commit.work_id if topic == "inventory" else None,
        )
        core_record = frame.records[("intake_disposition", core_key)]
        complete = (
            result.completed_at is not None if isinstance(result, m.InventoryGeneration)
            else result.checkpoint.cursor is None if isinstance(result, m.RestPageReceipt) else True
        )
        last = None
        for index, group in enumerate(groups):
            descriptors = [{
                "kind": row.kind, "key": row.key, "revision": row.version,
                "payload_hash": hashlib.sha256(row.payload.encode("utf-16-le")).hexdigest().upper(),
            } for row in (*group, core_record)]
            last = self._sql.rpc("worker.accept_facts", {
                **_stamp(control), "request_id": part_ids[index], "fingerprint": frame.fingerprint,
                "expected_revision": control.revision, "work_id": producer_commit.work_id,
                "owner_id": producer_commit.lease.owner_id, "fence": producer_commit.lease.fence,
                "work_revision": producer_commit.expected_work_revision, "facts_json": _json(descriptors),
                "window_start_at": window.start_at if window else None, "window_end_at": window.end_at if window else None,
                "collection_complete": complete and index == len(groups) - 1,
            })["result"]
        frame.part_ids = part_ids
        receipt = self._sql.get_receipt("worker.accept_facts", request_id, control)
        return self._initial_reconciliation(
            control, last["reconcile_work_id"], request_id, "worker",
            policy_revision=control.revision, created_at=receipt.recorded_at, target=target,
        )

    @staticmethod
    def _initial_reconciliation(context, work_id, request_id, producer, *, policy_revision, created_at, target=None):
        return m.MonitoringWork(
            **_stamp(context), work_id=work_id, kind="reconcile_state", policy_revision=policy_revision,
            due_at=created_at, created_at=created_at, reason="Accepted intent requires deterministic reconciliation",
            target=target, reconcile_request_id=request_id, reconcile_producer=producer,
            revision=1, attempts=0, retry_attempt=0, state="queued",
        )

    def _collection_core(self, context, request_id, operation, native_result):
        key = f"intake:{operation}:{request_id}"
        matches = [
            value for value in native_result["facts"]
            if value["kind"] == "intake_disposition" and value["key"] == key
        ]
        if len(matches) != 1:
            raise MonitoringUnavailable("Native receipt has no unique canonical result binding")
        row = self._sql.get("intake_disposition", key, context)
        binding = matches[0]
        if row is None or row.version != binding["revision"] or (
            hashlib.sha256(row.payload.encode("utf-16-le")).hexdigest().lower() != binding["payload_hash"].lower()
        ):
            raise MonitoringUnavailable("The originally accepted API result is missing or has changed")
        core = _kernel_model(m.CollectionAcceptance, json.loads(row.payload))
        if core.request_id != request_id or core.operation != operation or _stamp(core) != _stamp(context):
            raise MonitoringUnavailable("Collection result material identifies another original operation")
        receipt = self._sql.get_receipt("worker.accept_facts", request_id, context)
        if receipt is None or receipt.fingerprint != core.fingerprint:
            raise MonitoringUnavailable("Collection result fingerprint differs from the original guarded receipt")
        return core

    def _collection_response(self, context, request_id, operation, native_result):
        core = self._collection_core(context, request_id, operation, native_result)
        if not isinstance(core.result, m.RestPageReceipt):
            return core.result
        work_ids = []
        recorded_at = None
        for part_id in core.part_ids:
            receipt = self._sql.get_receipt("worker.accept_facts", part_id, context)
            if receipt is None or receipt.fingerprint != core.fingerprint:
                raise MonitoringUnavailable("The original atomic intake part receipt is missing or changed")
            part = self._receipt_result(receipt)
            work_ids.append(m.canonical_id(part["reconcile_work_id"]))
            recorded_at = receipt.recorded_at
        return _update(core.result, intake=_update(
            core.result.intake, work_ids=tuple(work_ids), recorded_at=recorded_at,
        ))

    def _finish_poll_work(self, work):
        frame = self._sql.collection
        if frame is None or not frame.part_ids:
            raise MonitoringUnavailable("A poll cannot finish before its guarded intake receipts")
        return self._sql_transition(
            work, work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            request_id=stable_id(work, f"poll-complete:{frame.request_id}"),
            fingerprint=frame.fingerprint, transition="complete",
        )

    def _web_response(self, context, request_id, operation, native_result):
        receipt = self._sql.get_receipt(
            "web.commit_intent", self._native_request_id(context, operation, request_id), context,
        )
        if receipt is None:
            raise MonitoringUnavailable("The original web intent receipt is absent")
        expected_kind = {
            "preview": None, "activation": "scope", "safety_review": "review", "discovery": "discovery",
        }[operation]
        if native_result.get("intent_kind") != expected_kind:
            raise MonitoringConflict("The original request ID belongs to another web intent operation")
        if operation == "preview":
            return _kernel_model(m.ActivationPlan, native_result["original_intent"])
        if operation == "discovery":
            return self._initial_reconciliation(
                context, native_result["reconcile_work_id"], request_id, "web",
                policy_revision=native_result["policy_revision"], created_at=receipt.recorded_at,
            )
        if operation == "safety_review":
            return self._review_from_intent(
                native_result["original_intent"], revision=native_result["new_intent_revision"],
                expected_policy=native_result["policy_revision"] - 1, accepted_at=receipt.recorded_at,
            )
        plan_id = stable_id(context, f"plan:{request_id}")
        plan = self._get("plan", plan_id, context, m.ActivationPlan)
        if plan is None:
            raise MonitoringUnavailable("The accepted activation lost its original dry-run plan")
        scope = m.ScopePolicy(
            **native_result["original_intent"], revision=native_result["policy_revision"],
            updated_at=receipt.recorded_at,
        )
        return m.ActivationReceipt(
            plan_id=plan.plan_id, idempotency_id=request_id, scope=scope, state="configuring",
            version=m.RegistryVersion(**_stamp(context), revision=native_result["policy_revision"]),
            activated_at=receipt.recorded_at, requested_by=plan.requested_by,
            queued_work_ids=(native_result["reconcile_work_id"],),
        )

    def _receipt(self, operation, request_id, context, model):
        self._control(context)
        if operation in {"activation", "safety_review", "discovery", "preview"}:
            receipt = self._sql.get_receipt(
                "web.commit_intent", self._native_request_id(context, operation, request_id), context,
            )
            return self._web_response(context, request_id, operation, self._receipt_result(receipt)) if receipt else None
        if operation in {"rest_page", "inventory", "capability"}:
            receipt = self._sql.get_receipt("worker.accept_facts", request_id, context)
            return self._collection_response(context, request_id, operation, self._receipt_result(receipt)) if receipt else None
        if operation == "action_rejection":
            receipt = self._sql.get_receipt("controller.transition_action", request_id, context)
            if receipt is None:
                return None
            return _kernel_model(m.ActionTransitionResult, self._receipt_result(receipt)).reservation
        if operation == "action_reservation":
            receipt = self._sql.get_receipt("controller.reserve_action", request_id, context)
            if receipt is None:
                return None
            result = self._receipt_result(receipt)
            action = _kernel_model(m.ActionReservation, result["reservation"])
            if (
                result["reservation_id"] != action.reservation_id or action.request.idempotency_id != request_id
                or _stamp(action.request.expected) != _stamp(context)
            ):
                raise MonitoringUnavailable("Original reservation receipt identity is inconsistent")
            return m.ActionReservationDecision(status="reserved", reservation=action, detail=action.detail)
        if operation == "finalization":
            receipt = self._sql.get_receipt("controller.finalize", request_id, context)
            return self._finalization_response(context, request_id, receipt) if receipt else None
        if operation == "connector_publication":
            receipt = self._sql.get_receipt("controller.publish_connector", request_id, context)
            return _kernel_model(m.ConnectorPublicationResult, self._receipt_result(receipt)) if receipt else None
        if operation == "connector":
            receipt = self._sql.get_receipt("worker.observe_connector", request_id, context)
            return _kernel_model(m.ConnectorObservationResult, self._receipt_result(receipt)).connector if receipt else None
        return super()._receipt(operation, request_id, context, model)

    def _sql_operation_receipt(self, context, operation, request_id):
        self._control(context)
        if operation == "approval_binding":
            binding, receipt = self._approval_binding_receipt(context, request_id)
            if binding is None:
                return None
            return m.OperationReceipt(
                **_stamp(context), operation=operation, request_id=request_id,
                fingerprint=receipt.fingerprint, recorded_at=receipt.recorded_at,
                result=binding.model_dump(mode="json"),
            )
        models = {
            "inventory": m.InventoryGeneration, "capability": m.CapabilityObservation,
            "rest_page": m.RestPageReceipt, "activation": m.ActivationReceipt,
            "safety_review": m.SafetyReview, "discovery": m.MonitoringWork, "preview": m.ActivationPlan,
            "action_rejection": m.ActionReservation, "action_reservation": m.ActionReservationDecision,
            "finalization": m.FinalizationReceipt,
            "connector": m.OwnedConnectorManifest, "connector_publication": m.ConnectorPublicationResult,
        }
        if operation not in models:
            receipt = self._sql.get_receipt(operation, request_id, context)
            if receipt is None:
                return None
            result = self._receipt_result(receipt)
        else:
            result_model = self._receipt(operation, request_id, context, models[operation])
            if result_model is None:
                return None
            native = {
                "action_rejection": "controller.transition_action", "action_reservation": "controller.reserve_action",
                "finalization": "controller.finalize",
                "connector": "worker.observe_connector", "connector_publication": "controller.publish_connector",
            }.get(operation, "worker.accept_facts" if operation in {"inventory", "capability", "rest_page"} else "web.commit_intent")
            receipt = self._sql.get_receipt(native, self._native_request_id(context, operation, request_id), context)
            result = result_model.model_dump(mode="json")
        return m.OperationReceipt(
            **_stamp(context), operation=operation, request_id=request_id, fingerprint=receipt.fingerprint,
            recorded_at=receipt.recorded_at, result=result,
        )

    def _sql_record_connector(self, expected, manifest, *, expected_connector_revision, commit=None, inspection=None):
        if commit is None:
            raise MonitoringComponentDenied("Connector observations require their actual collection work fence")
        if inspection is not None:
            inspection = _kernel_model(m.ConnectorPresenceInspection, inspection)
        self._sql.lock_context(expected)
        request_id = stable_id(expected, f"connector:{manifest.connector_id}:{expected_connector_revision}")
        original_request = {
            "expected": expected.model_dump(mode="json"), "manifest": manifest.model_dump(mode="json"),
            "expected_connector_revision": expected_connector_revision,
            "commit": commit.model_dump(mode="json"),
        }
        if inspection is not None:
            original_request["inspection"] = inspection.model_dump(mode="json")
        fingerprint = key_digest(_json(original_request))
        replay = self._rpc_replay("worker.observe_connector", request_id, expected, fingerprint)
        if replay is not None:
            return self._connector_collection_response(expected, manifest, commit, replay, inspection=inspection).connector
        control = self._current(expected, intake=True)
        if _stamp(commit.lease) != _stamp(expected) or commit.lease.resource_key != m.work_key(expected, commit.work_id):
            raise MonitoringConflict("Connector collection lease belongs to another context or work")
        work = self._owned_work(expected, commit.work_id, commit.lease, commit.expected_work_revision)
        if (
            work.kind != "connector_reconcile" or work.connector_id != manifest.connector_id
            or work.target is not None or work.execution is not None
            or work.action_reservation_id is not None or work.retry_of is not None
            or work.finalization_id is not None or work.retry_attempt != 0
        ):
            raise MonitoringConflict("Connector observation belongs to another collection work identity")
        prior = self._get("connector", manifest.connector_id, control, m.OwnedConnectorManifest)
        if prior is None or any(getattr(prior, key) != getattr(manifest, key) for key in (
            "ownership_id", "name", "sources", "source_proposals", "source_removals", "desired_definition",
        )):
            raise MonitoringComponentDenied("Worker observations cannot create or replace desired connector topology")
        if (
            prior.revision != expected_connector_revision or manifest.revision != prior.revision + 1
            or manifest.policy_revision != control.revision or _stamp(manifest) != _stamp(control)
        ):
            raise MonitoringConflict("Connector observation lost its current revision or context")
        if manifest.state == "ready":
            self._require_connector_delivery(manifest, control)
        if inspection is not None:
            self._validate_connector_presence_inspection(manifest, inspection)
        observed = self._persisted(manifest)
        values = observed.model_dump(mode="json", include={
            "workspace_id", "eventstream_id", "destination_id", "observed_definition", "endpoint", "operation_id",
            "state", "identity_verified_at", "delivery_verified_at", "delivery_proof", "gaps",
        })
        if inspection is not None:
            values["inspection"] = inspection.model_dump(mode="json")
        observation_json = _json(values)
        expected_definition_hash = self._sql.explicit_definition_hash(observation_json)
        result = self._sql.rpc("worker.observe_connector", {
            **_stamp(control), "request_id": request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "connector_id": manifest.connector_id,
            "work_id": commit.work_id, "owner_id": commit.lease.owner_id,
            "fence": commit.lease.fence, "work_revision": commit.expected_work_revision,
            "expected_connector_revision": expected_connector_revision, "ownership_id": prior.ownership_id,
            "observation_json": observation_json,
        })["result"]
        saved = self._connector_collection_response(expected, observed, commit, result, inspection=inspection)
        if (
            saved.connector_id != prior.connector_id or saved.connector.revision != prior.revision + 1
            or saved.connector.identity_verified_at != prior.identity_verified_at
            or saved.connector.delivery_verified_at != prior.delivery_verified_at
            or saved.connector.delivery_proof != prior.delivery_proof
            or saved.connector.state == "ready" and prior.state != "ready"
            or saved.observed_definition_hash != expected_definition_hash
        ):
            raise MonitoringUnavailable("Worker observation attempted to publish new readiness or another connector")
        return saved.connector

    @staticmethod
    def _connector_collection_response(expected, observation, commit, result, *, inspection=None) -> m.ConnectorObservationResult:
        saved = _kernel_model(m.ConnectorObservationResult, result)
        if (
            saved.work_id != commit.work_id or saved.work_owner_id != commit.lease.owner_id
            or saved.work_fence != commit.lease.fence or saved.work_revision != commit.expected_work_revision
            or _stamp(saved.observation) != _stamp(expected) or saved.observation.policy_revision != expected.revision
            or saved.connector_id != observation.connector_id or saved.connector.revision != observation.revision
            or saved.collection_completion_eligible != m.connector_collection_eligible(observation)
            or saved.inspection != inspection
        ):
            raise MonitoringUnavailable("Connector observation receipt lost its exact collection work or original eligibility")
        return saved

    def _connector_observation_projection(self, context, request_id):
        document, handoff = self._native_reconciliation(context, request_id, "worker")
        if document is None:
            return None
        if document["topic"] != "connector":
            raise MonitoringConflict("The requested producer handoff is not a connector observation")
        arguments = document["request_payload"]
        connector = self._get("connector", document["reference_id"], context, m.OwnedConnectorManifest)
        revision = arguments.get("expected_connector_revision")
        if (
            connector is None or type(revision) is not int or revision < 0
            or connector.revision != revision + 1
            or connector.connector_id != arguments.get("connector_id")
            or connector.ownership_id != arguments.get("ownership_id")
            or connector.policy_revision != document["policy_revision"]
            or connector.policy_revision != arguments.get("expected_revision")
        ):
            logger.error("Original connector observation baseline changed request_hash=%s", key_digest(request_id))
            raise MonitoringUnavailable("Original connector observation cannot be reconstructed from a changed baseline")
        observed = self._connector_observation_from_handoff(connector, document)
        patch = json.loads(arguments["observation_json"])
        return _kernel_model(m.ConnectorObservationResult, {
            "connector_id": connector.connector_id, "connector": connector,
            "observation": observed,
            "observed_definition_hash": self._sql.explicit_definition_hash(arguments["observation_json"]),
            "authority": "observed_not_action_authority",
            "reconcile_work_id": handoff.work_id, "frontier_key": handoff.frontier_key,
            "frontier_revision": handoff.frontier_revision,
            "work_id": arguments.get("work_id"), "work_owner_id": arguments.get("owner_id"),
            "work_fence": arguments.get("fence"), "work_revision": arguments.get("work_revision"),
            "collection_completion_eligible": m.connector_collection_eligible(observed),
            "inspection": patch.get("inspection"),
        })

    def _connector_publication(self, request: m.ConnectorPublicationRequest) -> m.ConnectorPublicationResult:
        self._sql.lock_context(request.expected)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        replay = self._rpc_replay("controller.publish_connector", request.request_id, request.expected, fingerprint)
        if replay is not None:
            result = _kernel_model(m.ConnectorPublicationResult, replay)
            if result.connector_id != request.connector_id or result.connector.ownership_id != request.ownership_id:
                raise MonitoringUnavailable("Original connector publication identifies another owner or connector")
            return result
        control = self._current(request.expected, intake=True)
        work = self._owned_work(control, request.work_id, request.lease, request.expected_work_revision)
        if work.kind != "reconcile_state":
            raise MonitoringConflict("Desired connector publication requires reconciliation work, not an action lease")
        document, handoff = self._native_reconciliation(control, work.reconcile_request_id, work.reconcile_producer)
        frontier = self._get("validation_frontier", handoff.frontier_key, control, m.ValidationFrontier) if handoff else None
        if (
            document is None or handoff.work_id != work.work_id or handoff.policy_revision != control.revision
            or frontier is None or frontier.accepted_revision != request.expected_frontier_revision
        ):
            raise MonitoringConflict("Desired connector publication lost its current producer/frontier binding")
        prior = self._get("connector", request.connector_id, control, m.OwnedConnectorManifest)
        desired = self._get("connector_desired", request.connector_id, control, m.ConnectorDesiredState)
        if (prior.revision if prior else 0) != request.expected_connector_revision:
            raise MonitoringConflict("Desired connector revision changed")
        self._validate_initial_connector_publication(prior, desired, request)
        persisted = self._persisted(request)
        if persisted.sources != request.sources or persisted.source_proposals != request.source_proposals or (
            persisted.source_removals != request.source_removals
        ) or (
            persisted.source_removal_supersessions != request.source_removal_supersessions
        ) or (
            persisted.desired_definition != request.desired_definition
        ):
            raise MonitoringConflict("Redacted desired topology cannot become an executable configuration")
        observed = None
        observed_definition_hash = None
        if request.readiness_receipt_id is not None:
            if (
                prior is None or work.reconcile_producer != "worker" or document["topic"] != "connector"
                or work.reconcile_request_id != request.readiness_receipt_id
                or document["reference_id"] != request.connector_id
            ):
                raise MonitoringConflict("Readiness requires the exact original owned worker observation")
            readiness = self._connector_observation_from_handoff(prior, document)
            self._require_connector_delivery(readiness, control)
        if request.observation_receipt_id is not None:
            if (
                prior is None or work.reconcile_producer != "worker" or document["topic"] != "connector"
                or work.reconcile_request_id != request.observation_receipt_id
                or document["reference_id"] != request.connector_id
            ):
                raise MonitoringConflict("Physical binding requires the exact owned worker observation handoff")
            observed = self._connector_observation_from_handoff(prior, document)
            observed_definition_hash = self._sql.explicit_definition_hash(document["request_payload"]["observation_json"])
            if observed_definition_hash is None:
                raise MonitoringConflict("Physical reconciliation requires an explicit original definition observation")
        expected_sources, expected_proposals, expected_definition = self._connector_desired_values(request, prior, observed)
        plan = m.ConnectorPublicationPlan(
            connector_id=request.connector_id, ownership_id=request.ownership_id, work_id=work.work_id,
            lease_owner_id=work.lease.owner_id, lease_fence=work.lease.fence,
            expected_work_revision=work.revision, expected_connector_revision=request.expected_connector_revision,
            policy_revision=control.revision, producer_request_id=handoff.producer_request_id,
            producer_fingerprint=handoff.producer_fingerprint, frontier_key=handoff.frontier_key,
            frontier_revision=frontier.accepted_revision, name=persisted.name, sources=persisted.sources,
            source_proposals=persisted.source_proposals, desired_definition=persisted.desired_definition,
            source_removals=persisted.source_removals,
            source_removal_supersessions=persisted.source_removal_supersessions,
            observation_receipt_id=request.observation_receipt_id, readiness_receipt_id=request.readiness_receipt_id,
            detail=persisted.detail,
        )
        publication_id = stable_id(control, f"connector-publication:{request.request_id}")
        if self._sql.get("connector_publication", publication_id, control) is not None:
            raise MonitoringConflict("An uncommitted connector publication plan cannot be adopted")
        payload = _json(plan.model_dump(mode="json"))
        self._sql.put(StoredRecord(
            kind="connector_publication", key=publication_id, context=m.MonitoringContext(**_stamp(control)),
            payload=payload, version=1, parent_key=work.work_id,
        ))
        native = self._sql.rpc("controller.publish_connector", {
            **_stamp(control), "request_id": request.request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "work_id": work.work_id, "owner_id": work.lease.owner_id,
            "fence": work.lease.fence, "work_revision": work.revision,
            "publication_id": publication_id, "publication_hash": hashlib.sha256(payload.encode("utf-16-le")).hexdigest().upper(),
        })["result"]
        result = _kernel_model(m.ConnectorPublicationResult, native)
        if (
            result.connector_id != request.connector_id or result.connector.ownership_id != request.ownership_id
            or result.connector.revision != request.expected_connector_revision + 1
            or result.connector.policy_revision != control.revision or result.connector.sources != expected_sources
            or result.connector.source_proposals != expected_proposals
            or result.connector.desired_definition != expected_definition
            or result.observation_receipt_id != request.observation_receipt_id
            or desired is None and not result.desired_changed
            or result.state == "ready" and request.readiness_receipt_id is None and (prior is None or prior.state != "ready")
        ):
            raise MonitoringUnavailable("Guarded connector publication returned a different identity, definition or readiness")
        if prior is not None and any(
            getattr(result.connector, name) != getattr(prior, name)
            for name in ("workspace_id", "eventstream_id", "destination_id", "endpoint")
        ):
            raise MonitoringUnavailable("Desired publication changed an established physical binding")
        original = self._sql.get_receipt("controller.publish_connector", request.request_id, control)
        if original is None or _kernel_model(m.ConnectorPublicationResult, self._receipt_result(original)) != result:
            raise MonitoringUnavailable("Connector publication result differs from its original committed receipt")
        self._validate_removal_result(request, prior, result, work, publication_id, observed_definition_hash, desired)
        self._publish_connector(result.connector, control)
        self._schedule_supersession_capabilities(request, result.superseded_source_removals, control)
        if result.desired_changed and result.state in {"planned", "provisioning"}:
            self._connector_followup(result.connector, control)
        self._sql.operation_identity("controller.publish_connector", request.request_id)
        return result

    @staticmethod
    def _connector_observation_from_handoff(connector, document):
        patch = json.loads(document["request_payload"]["observation_json"])
        # Inspection is immutable observation evidence, not current manifest state.
        patch.pop("inspection", None)
        return _kernel_model(m.OwnedConnectorManifest, {
            **connector.model_dump(mode="json"), **patch, "updated_at": document["created_at"],
        })

    def _validate_removal_result(self, request, prior, result, work, publication_id, observed_definition_hash, prior_desired):
        previous = {removal.removal_id: removal for removal in prior.source_removals} if prior else {}
        requested = {removal.removal_id: removal for removal in request.source_removals}
        if request.source_removal_supersessions:
            selected = {item.removal_id: item.source_id for item in request.source_removal_supersessions}
            expected = {key: value for key, value in previous.items() if key in selected}
            desired = self._get("connector_desired", request.connector_id, request.expected, m.ConnectorDesiredState)
            expected_anchor = (
                prior_desired.supersession_request_id
                if prior_desired and prior_desired.supersession_request_id else request.request_id
            )
            if (
                len(expected) != len(selected)
                or {entry.removal_id: entry for entry in result.superseded_source_removals} != expected
                or any(value.source_id != selected[key] for key, value in expected.items())
                or {entry.removal_id: entry for entry in result.pending_removals}
                != {key: value for key, value in previous.items() if key not in selected}
                or result.retired_sources or not result.desired_changed or result.state != "provisioning"
                or desired is None or desired.supersession_request_id != expected_anchor
                or any(value is not None for value in (
                    result.connector.identity_verified_at, result.connector.delivery_verified_at,
                    result.connector.delivery_proof,
                ))
            ):
                raise MonitoringUnavailable("Supersession result lost original removals, retained ownership or unready publication")
            for original in expected.values():
                receipt = self._sql.get_receipt("controller.publish_connector", original.request_id, request.expected)
                if receipt is None:
                    raise MonitoringUnavailable("Supersession result lost its original removal publication receipt")
                publication = _kernel_model(m.ConnectorPublicationResult, self._receipt_result(receipt))
                if (
                    publication.connector_id != request.connector_id
                    or publication.connector.ownership_id != request.ownership_id
                    or original not in publication.pending_removals
                    or not any(
                        source.source_id == original.source_id and source.target == original.target
                        for source in publication.connector.sources
                    )
                ):
                    raise MonitoringUnavailable("Supersession rewrote its original removal history")
            return
        if result.superseded_source_removals:
            raise MonitoringUnavailable("An ordinary publication cannot report unrequested source supersession")
        if request.observation_receipt_id is None:
            if result.retired_sources or {removal.removal_id for removal in result.pending_removals} != set(requested):
                raise MonitoringUnavailable("Unobserved desired removal cannot retire or discard ownership")
            for removal in result.pending_removals:
                if removal.intent() != requested[removal.removal_id]:
                    raise MonitoringUnavailable("Pending removal differs from its original requested selector/detail")
                if removal.removal_id in previous:
                    if removal != previous[removal.removal_id]:
                        raise MonitoringUnavailable("An existing pending removal was rewritten")
                    continue
                binding, node = self._removal_binding(prior, requested[removal.removal_id])
                if (
                    removal.node_name != node or removal.target != binding.target
                    or removal.request_id != request.request_id or removal.publication_id != publication_id
                    or removal.policy_revision != request.expected.revision
                ):
                    raise MonitoringUnavailable("New pending removal lost its original ownership or publication binding")
        else:
            if result.pending_removals or {entry.removal_id for entry in result.retired_sources} != set(previous):
                raise MonitoringUnavailable("Physical absence confirmation must explain every retirement atomically")
            for entry in result.retired_sources:
                binding, node = self._removal_binding(prior, requested[entry.removal_id])
                if (
                    entry.original_removal != previous[entry.removal_id] or entry.original_binding != binding
                    or entry.node_name != node or entry.confirmation_request_id != request.request_id
                    or entry.work_id != work.work_id or entry.work_fence != work.lease.fence
                    or entry.policy_revision != request.expected.revision
                    or entry.observed_definition_hash != observed_definition_hash
                ):
                    raise MonitoringUnavailable("Retirement does not match its original binding, observation and current work")

    def _reconcile_connector_observation(self, producer, connector, control):
        if (
            (connector.source_proposals or connector.source_removals)
            and producer.request_payload.get("definition_observed")
            and connector.state in {"provisioning", "ready", "degraded"}
        ):
            work = self._get("work", producer.work_id, control, m.MonitoringWork)
            frontier = self._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
            result = self._publish_connector_context(m.ConnectorPublicationContext(
                phase="binding",
                request_id=stable_id(control, f"connector-bind:{producer.request_id}:{work.lease.fence}"),
                expected=m.RegistryVersion(**_stamp(control), revision=control.revision),
                work=work, frontier=frontier, connector=connector,
            ))
            if result is None:
                raise MonitoringUnavailable("Connector binding did not return its original publication result")
            self._publish_connector(result.connector, control)
            return
        if producer.request_payload.get("readiness_requested"):
            work = self._get("work", producer.work_id, control, m.MonitoringWork)
            frontier = self._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
            result = self._connector_publication(m.ConnectorPublicationRequest(
                request_id=stable_id(control, f"connector-ready:{producer.request_id}:{work.lease.fence}"),
                expected=m.RegistryVersion(**_stamp(control), revision=control.revision),
                work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
                expected_frontier_revision=frontier.accepted_revision, connector_id=connector.connector_id,
                ownership_id=connector.ownership_id, expected_connector_revision=connector.revision,
                name=connector.name, sources=connector.sources, desired_definition=connector.desired_definition,
                readiness_receipt_id=producer.request_id,
                detail="Publish the original receipt-bound readiness observation without changing desired scope.",
            ))
            connector = result.connector
        self._publish_connector(connector, control)

    def _action_work(self, context, work_id, lease, *, revision=None):
        work = self._get("work", m.canonical_id(work_id), context, m.MonitoringWork)
        if (
            work is None or work.kind not in {"triage", "deferred_retry", "verify_action", "finalize"}
            or work.state not in {"leased", "finalizing"} or work.lease is None
            or _stamp(work) != _stamp(context) or work.key != lease.resource_key
            or (work.lease.owner_id, work.lease.fence) != (lease.owner_id, lease.fence)
            or work.lease.expires_at <= self._now()
        ):
            raise MonitoringLeaseLost("The exact action work lease is absent, superseded or expired")
        if revision is not None and work.revision != revision:
            raise MonitoringConflict("The action work revision changed")
        return work

    def _action_commit_work(self, context, commit):
        return self._action_work(context, commit.work_id, commit.lease, revision=commit.expected_work_revision)

    @contextmanager
    def _source_scope(self, work: m.MonitoringWork, producer: m.ReconciliationRequest | None = None):
        previous = self._sql.source_frame
        evidence = tuple(producer.evidence) if producer is not None else ()
        alias_window_id = None
        if producer is not None and producer.topic == "rest_page":
            by_key = {}
            for handoff in self._all(
                "validation_handoff", work, m.ValidationHandoff,
                filters={"parent_key": producer.frontier_key}, budget=m.MAX_RECONCILIATION_BINDINGS,
            ):
                row = self._sql.get(f"{handoff.producer}_reconcile_request", handoff.producer_request_id, work)
                if row is None:
                    raise MonitoringUnavailable("Accepted source window lost an original producer request")
                for raw in json.loads(row.payload)["evidence"]:
                    binding = _kernel_model(m.EvidenceBinding, raw)
                    if binding.kind in {"rest_observation", "rest_powerbi_row", "signal"}:
                        key = (binding.kind, binding.key)
                        if key in by_key and by_key[key] != binding:
                            raise MonitoringConflict("Accepted source window contains changed evidence bindings")
                        by_key[key] = binding
            evidence = tuple(by_key[key] for key in sorted(by_key))
            page = m.RestPageRequest.model_validate(producer.request_payload)
            if page.target.workload == "powerbi":
                alias_window_id = self._powerbi_window_id(page)
        self._sql._local.source_frame = SourceFrame(work, producer, evidence, alias_window_id)
        try:
            yield
        finally:
            self._sql._local.source_frame = previous

    def _source_evidence(self, observation, *, disposing=False):
        frame = self._sql.source_frame
        if frame is None:
            raise MonitoringConflict("Source publication requires its explicit current work context")
        if frame.work.kind != "reconcile_state":
            return None, None, None
        if disposing and frame.producer is not None and frame.producer.topic in {"scope", "review", "inventory", "capability"}:
            return None, None, None
        for binding in frame.evidence:
            if binding.kind not in {"rest_observation", "rest_powerbi_row", "signal"}:
                continue
            row = self._sql.get(binding.kind, binding.key, frame.work)
            if row is None or row.version != binding.revision or (
                hashlib.sha256(row.payload.encode("utf-16-le")).hexdigest() != binding.payload_hash
            ):
                continue
            alias = None
            if binding.kind == "rest_observation":
                candidate = self._decode(row, m.SourceRunObservation)
            elif binding.kind == "signal":
                signal = self._decode(row, m.SignalReceipt)
                candidate = signal.observation if signal.status == "accepted" else None
            else:
                raw = self._decode(row, m.PowerBIWindowRow)
                alias = frame.alias_window_id
                candidate = self._resolve_powerbi_row(frame.work, alias, raw) if alias is not None else raw.observation
            if candidate == observation:
                return binding.kind, binding.key, alias
        raise MonitoringConflict("Source observation has no exact accepted evidence in this owned reconciliation window")

    def _source_arguments(self, observation, operation, *, disposing=False, extra=None):
        frame = self._sql.source_frame
        if frame is None or frame.work.lease is None:
            raise MonitoringConflict("Source operation requires its original work lease")
        persisted = self._persisted(observation)
        kind, key, alias = self._source_evidence(persisted, disposing=disposing)
        control = self._control(frame.work)
        arguments = {
            **_stamp(frame.work),
            "expected_revision": control.revision if disposing or frame.work.kind == "reconcile_state" else frame.work.policy_revision,
            "work_id": frame.work.work_id, "owner_id": frame.work.lease.owner_id,
            "fence": frame.work.lease.fence, "work_revision": frame.work.revision,
            "observation_json": _json(persisted.model_dump(mode="json")),
            "evidence_kind": kind, "evidence_key": key, "alias_window_id": alias,
            **(extra or {}),
        }
        fingerprint = key_digest(_json(arguments))
        return {
            **arguments, "request_id": stable_id(frame.work, f"{operation}:{fingerprint}"), "fingerprint": fingerprint,
        }

    def _save_source(self, observation):
        arguments = self._source_arguments(observation, "controller.publish_source")
        native = self._sql.rpc("controller.publish_source", arguments)["result"]
        result = _kernel_model(m.SourcePublicationResult, native)
        if result.source_key != observation.key or result.observation.execution != observation.execution:
            raise MonitoringUnavailable("Native source publication returned a different exact execution")
        return result.observation

    def _sql_observe_source(self, observation, *, work_id, lease):
        self._sql.lock_context(observation.execution.target)
        work = self._action_work(observation.execution.target, work_id, lease)
        if work.target != observation.execution.target or observation.authority != "rest":
            raise MonitoringConflict("Fresh source evidence requires the exact owned target and REST authority")
        with self._source_scope(work):
            return self._save_source(observation)

    def _source_disposition(
        self, execution, disposition, detail, *, work_id=None, finalization_id=None,
        incident_identity=None, observation=None,
    ):
        if finalization_id is not None or incident_identity is not None:
            raise MonitoringConflict("Incident-backed source disposition belongs to the atomic finalization RPC")
        frame = self._sql.source_frame
        if frame is None:
            raise MonitoringConflict("Non-effect source disposition requires its explicit current work context")
        source = observation or self._get("source", execution.key, execution.target, m.SourceRunObservation)
        if source is None or source.execution != execution:
            raise MonitoringConflict("Source disposition requires exact recorded or accepted source evidence")
        if self._submitted_action_owner(execution) is not None:
            raise MonitoringConflict("A submitted action execution retains action verification and finalization")
        subject = None
        if work_id is not None and work_id != frame.work.work_id:
            subject = self._get("work", work_id, frame.work, m.MonitoringWork)
            if subject is None or subject.execution != execution:
                raise MonitoringConflict("Source cleanup subject has another execution")
        arguments = self._source_arguments(source, "controller.disposition_source", disposing=True, extra={
            "disposition": disposition, "detail": self._redactor(detail)[:2000],
            "subject_work_id": subject.work_id if subject else None,
            "expected_subject_revision": subject.revision if subject else None,
        })
        result = _kernel_model(
            m.SourceDispositionResult, self._sql.rpc("controller.disposition_source", arguments)["result"],
        )
        if result.source_key != execution.key or result.disposition.execution != execution:
            raise MonitoringUnavailable("Native source disposition returned another exact execution")
        return result.disposition

    def _approval_binding_receipt(self, context, approval_id):
        binding = self._get("approval_binding", approval_id, context, m.ApprovalBinding)
        if binding is None:
            return None, None
        request_id = stable_id(context, f"approval-binding:{approval_id}")
        receipt = self._sql.get_receipt("controller.publish_source", request_id, context)
        origin = binding.model_dump(mode="json", exclude={"created_at", "expires_at"})
        if receipt is None or receipt.fingerprint != key_digest(_json(origin)):
            raise MonitoringUnavailable("Approval binding lost its original lease-checked source receipt")
        source = _kernel_model(m.SourcePublicationResult, self._receipt_result(receipt))
        if source.source_key != binding.source_execution.key:
            raise MonitoringUnavailable("Approval binding source differs from its original guarded publication")
        return binding, receipt

    def _sql_bind_approval(self, request):
        self._sql.lock_context(request.expected)
        origin = self._approval_origin(request)
        original, _ = self._approval_binding_receipt(request.expected, request.approval.approval_id)
        if original is not None:
            if original.model_dump(mode="json", exclude={"created_at", "expires_at"}) != origin:
                raise MonitoringConflict("Approval identity was already bound to different scope/source/review intent")
            return original
        work = self._action_work(request.expected, request.work_id, request.lease)
        binding = self._approval_binding_record(request, work)
        source = self._get("source", request.source_execution.key, request.expected, m.SourceRunObservation)
        if source is None or source.authority != "rest":
            raise MonitoringConflict("Approval binding requires the recorded exact REST source")
        request_id = stable_id(request.expected, f"approval-binding:{request.approval.approval_id}")
        if self._sql.get_receipt("controller.publish_source", request_id, request.expected) is not None:
            raise MonitoringUnavailable("A source-checked approval transaction is missing its immutable binding")
        # The source RPC locks the actual work and target leases. The immutable
        # approval binding joins that transaction; replay cannot repair a half-write.
        with self._source_scope(work):
            arguments = self._source_arguments(source, "controller.publish_source")
            arguments.update(request_id=request_id, fingerprint=key_digest(_json(origin)))
            result = _kernel_model(m.SourcePublicationResult, self._sql.rpc("controller.publish_source", arguments)["result"])
        if result.source_key != request.source_execution.key:
            raise MonitoringUnavailable("Approval source validation returned another execution")
        saved = self._put(
            "approval_binding", request.approval.approval_id, request.expected, binding,
            target_key=request.source_execution.target.key,
        )
        self._sql.operation_identity("approval_binding", request.approval.approval_id)
        return saved

    def _schedule_verification(self, action):
        control = self._control(action.request.expected)
        # The action's durable deadline gates due claims. Reuse queued work
        # without borrowing its lease to rewrite its older due_at or lineage.
        return self._enqueue(m.MonitoringWorkDraft(
            **_stamp(control), work_id=stable_id(control, f"verify:{action.reservation_id}:{action.revision}"),
            kind="verify_action", policy_revision=control.revision, created_at=self._now(),
            due_at=action.next_verification_at or self._now(), target=action.request.source_execution.target,
            execution=action.request.source_execution, action_reservation_id=action.reservation_id,
            reason="Reconcile the existing external effect; never submit another POST.",
        ))

    def _submitted_action_owner(self, execution):
        rows = self._sql.db.query(
            f"SELECT TOP (2) {RECORD_COLUMNS} FROM {self._sql.tables['monitoring_records']} "
            "WHERE tenant_id = ? AND epoch = ? AND record_kind = 'action' AND target_hash = ? "
            "AND JSON_VALUE(payload, '$.submitted_execution.run_id') = ? "
            "AND JSON_VALUE(payload, '$.submitted_execution.run_id_kind') = ?",
            execution.target.tenant_id, execution.target.epoch, _digest(execution.target.key),
            execution.run_id, execution.run_id_kind,
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise MonitoringUnavailable("Submitted execution correlates to more than one native reservation")
        row = self._sql._record(rows[0], execution.target)
        action = self._decode(row, m.ActionReservation)
        if action.submitted_execution != execution:
            raise MonitoringUnavailable("Native submission lookup returned another exact execution")
        return ActionOwner(
            reservation_id=action.reservation_id, fence=action.fence,
            active=action.state not in {"rejected", "verified_succeeded", "verified_failed"},
        )

    def _sql_reserve_action(self, request: m.ActionReservationRequest) -> m.ActionReservationDecision:
        self._sql.lock_context(request.expected)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        replay = self._rpc_replay("controller.reserve_action", request.idempotency_id, request.expected, fingerprint)
        if replay is not None:
            action = _kernel_model(m.ActionReservation, replay["reservation"])
            if action.request != request:
                raise MonitoringUnavailable("Original reservation receipt differs from the requested source/intent")
            return m.ActionReservationDecision(status="reserved", reservation=action, detail=action.detail)
        control = self._current(request.expected, intake=True)
        if self._policy.max_write_actions < 1 or m.ACTION_TO_TOOL[request.action] not in self._policy.allowed_actions:
            raise MonitoringConflict("The controller policy does not permit this new action")
        work = self._action_work(request.expected, request.work_id, request.lease)
        if work.kind not in {"triage", "deferred_retry"} or work.execution != request.source_execution:
            raise MonitoringConflict("A reservation requires the exact current source work")
        target = self._target(request.source_execution.target)
        review = self._get("review", request.review_id, control, m.SafetyReview)
        capability = self._get("target_capability", request.source_execution.target.key, control, m.CapabilityObservation)
        source = self._get("source", request.source_execution.key, control, m.SourceRunObservation)
        if (
            target is None or not target.action.enabled or target.action.action != request.action
            or review is None or not self._review_current(review, capability, control)
            or target.action.review_id != request.review_id or review.revision != request.expected_review_revision
            or review.parameter_hash != request.parameter_hash or review.definition_hash != request.definition_hash
            or review.configuration_hash != request.configuration_hash
            or source is None or source.authority != "rest" or source.status != "failed"
        ):
            raise MonitoringConflict("Current protected source, review and capability do not validate this action")
        snapshot = self._sql.rpc("controller.inspect_frontiers", {
            **_stamp(control), "target_key": target.key,
        })["result"]
        if snapshot["target_key"] != target.key or TypeAdapter(m.StrictBool).validate_python(snapshot["pending"]):
            raise MonitoringConflict("Accepted intake still fences this target's new action")
        expiry = min(review.expires_at, capability.expires_at, work.lease.expires_at,
                     source.observed_at + timedelta(seconds=300))
        if expiry <= self._now():
            raise MonitoringConflict("Source, ownership or review validation expired")
        validation = m.ReservationValidation(
            policy_revision=control.revision, work_fence=work.lease.fence, source_key=source.key,
            parameter_hash=review.parameter_hash, review_id=review.review_id, review_revision=review.revision,
            expires_at=expiry, frontier_digest=snapshot["frontier_digest"],
            exact_action_correlation=capability.exact_action_correlation,
            definition_hash=review.definition_hash, configuration_hash=review.configuration_hash,
        )
        proof = validation.model_dump(mode="json")
        proof["frontier_digest"] = validation.frontier_digest.upper()
        prior = self._sql.get("controller_validation", work.work_id, control)
        self._sql.put(StoredRecord(
            kind="controller_validation", key=work.work_id, context=m.MonitoringContext(**_stamp(control)),
            version=prior.version + 1 if prior else 1, payload=_json(proof), target_key=target.key,
        ))
        persisted = self._persisted(request)
        if persisted != request:
            raise MonitoringConflict("Redacted action arguments cannot authorize the requested external effect")
        state = self._incident_state(request.incident)
        reservation_id = stable_id(control, f"action:{request.idempotency_id}")
        result = self._sql.rpc("controller.reserve_action", {
            **_stamp(control), "request_id": request.idempotency_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "work_id": work.work_id, "owner_id": work.lease.owner_id,
            "fence": work.lease.fence, "work_revision": work.revision, "reservation_id": reservation_id,
            "reservation_json": _json({
                "request": request.model_dump(mode="json"),
                "incident_id": state.incident_id if state else canonical_incident_id(request.incident),
            }),
        })["result"]
        action = _kernel_model(m.ActionReservation, result["reservation"])
        if action.reservation_id != reservation_id or action.request != request or action.state != "reserved":
            raise MonitoringUnavailable("Guarded reservation returned another request or state")
        return m.ActionReservationDecision(status="reserved", reservation=action, detail=action.detail)

    def _sql_action_submission(self, request, *, commit=None):
        return self._sql_action_update(request, commit)

    def _sql_action_outcome(self, request, *, commit=None):
        return self._sql_action_update(request, commit)

    def _sql_action_update(
        self, request: m.ActionSubmissionRequest | m.ActionOutcomeRequest, commit: m.CollectionCommit | None,
    ) -> m.ActionReservation:
        if commit is None:
            raise MonitoringConflict("Runtime action transitions require an explicit current work commit")
        self._sql.lock_context(request)
        fingerprint = key_digest(_json({
            "request": request.model_dump(mode="json"), "commit": commit.model_dump(mode="json"),
        }))
        replay = self._rpc_replay("controller.transition_action", request.request_id, request, fingerprint)
        if replay is not None:
            result = _kernel_model(m.ActionTransitionResult, replay)
            if result.reservation_id != request.reservation_id or result.retry_work is not None:
                raise MonitoringUnavailable("Original action-transition receipt has different lineage")
            return result.reservation
        control = self._control(request)
        action = self._fenced_action(
            request, request.reservation_id, request.expected_reservation_revision, request.action_fence,
        )
        work = self._action_transition_commit(request, action, commit)
        saved = self._persisted(request)
        changes: dict[str, object]
        if isinstance(saved, m.ActionSubmissionRequest):
            submitted = self._validate_action_submission(action, saved)
            changes = {
                "submitted_at": saved.submitted_at.isoformat(),
                "next_verification_at": saved.next_verification_at.isoformat(), "detail": saved.detail,
            }
            if submitted is not None:
                changes["submitted_execution"] = submitted.model_dump(mode="json")
            transition = saved.state
        else:
            self._validate_action_outcome(action, saved)
            submitted = action.submitted_execution
            transition = saved.disposition
            changes = {
                "detail": saved.detail,
                "next_verification_at": (self._now() + timedelta(seconds=120)).isoformat()
                if transition == "uncertain" else None,
            }
            if saved.configuration is not None:
                changes["configuration"] = saved.configuration.model_dump(mode="json")
            if transition != "uncertain":
                expires_at = min(work.lease.expires_at, saved.observed_at + timedelta(seconds=300))
                if expires_at <= self._now():
                    raise MonitoringConflict("Current work or exact action verification has expired")
                validation = m.ActionOutcomeValidation(
                    reservation_id=action.reservation_id, outcome=transition, work_fence=work.lease.fence,
                    expires_at=expires_at, submitted_execution=saved.submitted_execution,
                    configuration=saved.configuration, request=saved, commit=commit,
                )
                self._put(
                    "controller_validation", action.reservation_id, request, validation,
                    target_key=action.request.source_execution.target.key,
                )
        native = self._sql.rpc("controller.transition_action", {
            **_stamp(request), "request_id": request.request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "work_id": work.work_id,
            "owner_id": commit.lease.owner_id, "fence": commit.lease.fence, "work_revision": work.revision,
            "reservation_id": action.reservation_id, "expected_action_revision": action.revision,
            "transition": transition, "transition_json": _json(changes),
        })["result"]
        result = _kernel_model(m.ActionTransitionResult, native)
        updated = result.reservation
        if (
            result.reservation_id != action.reservation_id or result.retry_work is not None
            or updated.request != action.request or updated.fence != action.fence
            or updated.revision != action.revision + 1 or updated.state != transition
            or updated.submitted_execution != submitted
            or updated.submitted_at != (
                saved.submitted_at if isinstance(saved, m.ActionSubmissionRequest) else action.submitted_at
            )
            or updated.configuration != (
                saved.configuration if isinstance(saved, m.ActionOutcomeRequest) and saved.configuration is not None
                else action.configuration
            )
        ):
            raise MonitoringUnavailable("Guarded action transition changed original request, execution or fence")
        if isinstance(saved, m.ActionSubmissionRequest) or transition == "uncertain":
            self._schedule_verification(updated)
        if isinstance(saved, m.ActionOutcomeRequest) and saved.observation is not None:
            with self._source_scope(work):
                self._save_source(saved.observation)
        self._sql.operation_identity("controller.transition_action", request.request_id)
        return updated

    def _sql_action_rejection(self, request: m.ActionRejectionRequest) -> m.ActionReservation:
        self._sql.lock_context(request)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        replay = self._rpc_replay("controller.transition_action", request.request_id, request, fingerprint)
        if replay is not None:
            result = _kernel_model(m.ActionTransitionResult, replay)
            if result.reservation_id != request.reservation_id or result.reservation.rejection != self._persisted(request.evidence):
                raise MonitoringUnavailable("Original rejection receipt disagrees with its request")
            return result.reservation
        control = self._control(request)
        action = self._fenced_action(
            request, request.reservation_id, request.expected_reservation_revision, request.action_fence,
        )
        work = self._action_work(request, request.work_id, request.lease)
        if (
            action.state != "reserved" or action.submitted_execution is not None or action.submitted_at is not None
            or action.configuration is not None or work.kind not in {"triage", "deferred_retry"}
            or work.action_reservation_id != action.reservation_id or work.execution != action.request.source_execution
            or action.request.work_id != work.work_id
            or (action.request.lease.owner_id, action.request.lease.fence) != (request.lease.owner_id, request.lease.fence)
        ):
            raise MonitoringConflict("Only the original unsubmitted action owner can record definitive rejection")
        saved = self._persisted(request)
        native = self._sql.rpc("controller.transition_action", {
            **_stamp(request), "request_id": request.request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "work_id": work.work_id, "owner_id": work.lease.owner_id,
            "fence": work.lease.fence, "work_revision": work.revision,
            "reservation_id": action.reservation_id, "expected_action_revision": action.revision,
            "transition": "rejected",
            "transition_json": _json({"rejection": saved.evidence.model_dump(mode="json"), "detail": saved.detail}),
        })["result"]
        result = _kernel_model(m.ActionTransitionResult, native)
        if (
            result.reservation_id != action.reservation_id or result.reservation.request != action.request
            or result.reservation.fence != action.fence or result.reservation.revision != action.revision + 1
            or result.reservation.state != "rejected" or result.reservation.rejection != saved.evidence
        ):
            raise MonitoringUnavailable("Guarded rejection returned different action identity or evidence")
        return result.reservation

    def _sql_finalize_work(self, request: m.WorkFinalizationRequest) -> m.FinalizationReceipt:
        self._sql.lock_context(request)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        replay = self._rpc_replay("controller.finalize", request.finalization_id, request, fingerprint)
        if replay is not None:
            receipt = self._sql.get_receipt("controller.finalize", request.finalization_id, request)
            return self._finalization_response(request, request.finalization_id, receipt)
        control = self._control(request)
        work = self._action_work(request, request.work_id, request.lease, revision=request.expected_work_revision)
        if work.execution != request.source_execution:
            raise MonitoringConflict("Finalization must retain the exact owned source execution")
        if request.action_reservation_id is not None and request.action_reservation_id != work.action_reservation_id:
            raise MonitoringConflict("Finalization cannot change the work's existing action lineage")
        source = self._get("source", request.source_execution.key, request, m.SourceRunObservation)
        if source is None or source.started_at is None:
            raise MonitoringConflict("Finalization requires exact durable source chronology")
        state = self._incident_state(request.incident_identity)
        incident_id = state.incident_id if state else canonical_incident_id(request.incident_identity)
        prior = self._sql.incident(incident_id)
        if prior is not None and Incident.model_validate_json(prior).signature != request.incident_identity.signature:
            raise MonitoringConflict("The existing incident belongs to another canonical signature")
        candidate = request.incident.model_copy(deep=True)
        candidate.id = incident_id
        candidate = self._persisted(candidate)
        plan = m.FinalizationPlan(
            work_id=work.work_id, expected_work_revision=work.revision,
            lease_owner_id=work.lease.owner_id, lease_fence=work.lease.fence,
            incident_id=incident_id, incident_key=request.incident_identity.key, incident_identity=request.incident_identity,
            signature=request.incident_identity.signature, source_key=source.key, source_execution=source.execution,
            source_started_at=source.started_at,
            prior_incident_hash=hashlib.sha256(prior.encode("utf-16-le")).hexdigest() if prior is not None else None,
            merged_incident=candidate, source_disposition=request.source_disposition,
        )
        body = plan.model_dump(mode="json")
        if plan.prior_incident_hash is not None:
            body["prior_incident_hash"] = plan.prior_incident_hash.upper()
        payload = _json(body)
        if self._sql.get("finalization_plan", request.finalization_id, request) is not None:
            raise MonitoringConflict("A finalization plan without its original receipt cannot be replaced")
        self._sql.put(StoredRecord(
            kind="finalization_plan", key=request.finalization_id, context=m.MonitoringContext(**_stamp(request)),
            payload=payload, version=1, parent_key=work.work_id, target_key=request.source_execution.target.key,
        ))
        native = self._sql.rpc("controller.finalize", {
            **_stamp(request), "request_id": request.finalization_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "work_id": work.work_id, "owner_id": work.lease.owner_id,
            "fence": work.lease.fence, "work_revision": work.revision,
            "finalization_id": request.finalization_id,
            "finalization_json": _json({
                "plan_key": request.finalization_id,
                "plan_hash": hashlib.sha256(payload.encode("utf-16-le")).hexdigest().upper(),
            }),
        })["result"]
        result = _kernel_model(m.FinalizationResult, native)
        original = self._sql.finalized_incident_payload(request, request.finalization_id)
        persisted = self._sql.incident(incident_id)
        if persisted is None or original != persisted:
            raise MonitoringUnavailable("Kernel receipt did not preserve the exact persisted NVARCHAR incident payload")
        if result.incident_id != incident_id or result.work_id != work.work_id:
            raise MonitoringUnavailable("Guarded finalization returned another incident or work identity")
        receipt = self._sql.get_receipt("controller.finalize", request.finalization_id, request)
        return self._finalization_response(request, request.finalization_id, receipt)

    def _finalization_response(self, context, finalization_id, receipt):
        if receipt is None:
            raise MonitoringUnavailable("The original finalization receipt is absent")
        result = _kernel_model(m.FinalizationResult, self._receipt_result(receipt))
        plan = self._get("finalization_plan", finalization_id, context, m.FinalizationPlan)
        if (
            plan is None or result.finalization_id != finalization_id or result.work_id != plan.work_id
            or result.incident_id != plan.incident_id or result.incident.signature != plan.signature
            or _stamp(plan.source_execution.target) != _stamp(context)
        ):
            raise MonitoringUnavailable("Finalization receipt lost its immutable incident/source binding")
        payload = self._sql.finalized_incident_payload(context, finalization_id)
        if _kernel_model(Incident, json.loads(payload)) != result.incident:
            raise MonitoringUnavailable("Finalization receipt incident fragment disagrees with its result")
        return m.FinalizationReceipt(
            **_stamp(context), finalization_id=finalization_id, work_id=result.work_id,
            source_execution=plan.source_execution, incident_identity=plan.incident_identity,
            source_disposition=result.source_disposition,
            incident_payload_hash=hashlib.sha256(payload.encode("utf-16-le")).hexdigest(),
            persisted_at=receipt.recorded_at, incident_id=result.incident_id, state=result.state,
        )

    def _sql_source_disposition(self, execution):
        self._control(execution.target)
        row = self._sql.get("source_disposition", execution.key, execution.target)
        if row is None:
            return None
        value = json.loads(row.payload)
        finalization_id = value.get("finalization_id")
        if finalization_id is None:
            record = _kernel_model(m.ProcessedSourceRecord, value)
            if record.disposition_request_id is None:
                raise MonitoringUnavailable("Non-effect source disposition lost its original operation identity")
            receipt = self._sql.get_receipt("controller.disposition_source", record.disposition_request_id, execution.target)
            if receipt is None:
                raise MonitoringUnavailable("Non-effect source disposition lost its original operation receipt")
            result = _kernel_model(m.SourceDispositionResult, self._receipt_result(receipt))
            if result.disposition != record or result.source_key != execution.key:
                raise MonitoringUnavailable("Source disposition differs from its original non-effect receipt")
            return record
        receipt = self._receipt("finalization", m.canonical_id(finalization_id), execution.target, m.FinalizationReceipt)
        if (
            receipt is None or receipt.state != "completed" or receipt.source_execution != execution
            or value.get("work_id") != receipt.work_id or value.get("disposition") != receipt.source_disposition
        ):
            raise MonitoringUnavailable("Source disposition does not match its original completed finalization")
        return m.ProcessedSourceRecord(
            execution=execution, disposition=receipt.source_disposition, recorded_at=value["recorded_at"],
            work_id=receipt.work_id, finalization_id=finalization_id, incident_identity=receipt.incident_identity,
            detail="The guarded finalization committed this terminal source disposition.",
        )

    def _native_reconciliation(self, context, request_id, producer):
        producer = TypeAdapter(m.ProducerComponent).validate_python(producer)
        row = self._sql.get(f"{producer}_reconcile_request", m.canonical_id(request_id), context)
        if row is None:
            return None, None
        document = json.loads(row.payload)
        if (
            document["request_id"] != request_id or document["producer"] != producer
            or (document["tenant_id"], document["epoch"]) != (context.tenant_id, context.epoch)
        ):
            raise MonitoringUnavailable("Native reconciliation request identity is inconsistent")
        key = f"{document['frontier_key']}:handoff:{document['frontier_revision']}"
        binding = self._sql.get("validation_handoff", key, context)
        if binding is None:
            raise MonitoringUnavailable("Native producer request has no protected validation handoff")
        handoff = self._decode(binding, m.ValidationHandoff)
        if (
            handoff.work_id != document["work_id"] or handoff.producer_request_id != request_id
            or handoff.producer_fingerprint != document["fingerprint"]
        ):
            raise MonitoringUnavailable("Native producer and protected handoff bindings disagree")
        return document, handoff

    def _canonical_reconciliation(self, context, document):
        payload = document["request_payload"]
        canonical = dict(document)
        core = None
        if document["producer"] == "web":
            topic = document["topic"]
            if topic == "scope":
                canonical["request_payload"] = {"plan_id": stable_id(context, f"plan:{document['request_id']}")}
            elif topic == "review":
                canonical["request_payload"] = {"review_id": document["reference_id"]}
            elif topic == "discovery":
                canonical["request_payload"] = {"selector": json.loads(payload["intent_json"])}
            else:
                raise MonitoringUnavailable("Unsupported native human-intent topic")
        else:
            if document["topic"] == "stream_intake":
                partition = _kernel_model(m.PartitionIdentity, {
                    **_stamp(context), **{key: payload[key] for key in ("connector_id", "consumer_group", "partition_id")},
                })
                positions = self._stream_positions(payload["positions_json"])
                if any(entry.receipt.partition != partition for entry in positions):
                    raise MonitoringUnavailable("Original stream handoff mixes partition identities")
                evidence = {}
                for entry in document["evidence"]:
                    key = (entry["kind"], entry["key"])
                    if key in evidence and evidence[key] != entry:
                        raise MonitoringUnavailable("One original stream receipt has conflicting evidence bindings")
                    evidence[key] = entry
                canonical["evidence"] = list(evidence.values())
                canonical["request_payload"] = {
                    "partition": partition.model_dump(mode="json"),
                    "receipt_keys": list(dict.fromkeys(
                        entry.receipt_key for entry in positions if entry.receipt_kind == "identified"
                    )),
                    "unidentified_keys": list(dict.fromkeys(
                        entry.receipt_key for entry in positions if entry.receipt_kind == "unidentified"
                    )),
                }
                return _kernel_model(m.ReconciliationRequest, canonical), None
            if document["topic"] == "connector":
                observation = json.loads(payload["observation_json"])
                canonical["request_payload"] = {
                    "connector_id": document["reference_id"], "readiness_requested": observation.get("state") == "ready",
                    "definition_observed": isinstance(observation.get("observed_definition"), dict),
                }
                return _kernel_model(m.ReconciliationRequest, canonical), None
            cores = [
                value for value in document["evidence"]
                if value["kind"] == "intake_disposition" and value["key"].startswith("intake:")
            ]
            if len(cores) != 1:
                raise MonitoringKernelUnsupported("The native worker handoff has no unique typed collection result")
            row = self._sql.get("intake_disposition", cores[0]["key"], context)
            if row is None:
                raise MonitoringUnavailable("Accepted collection result material is unavailable")
            core = self._decode(row, m.CollectionAcceptance)
            if document["request_id"] not in core.part_ids or core.fingerprint != document["fingerprint"]:
                raise MonitoringUnavailable("Collection result material is not bound to this native part")
            canonical.update(
                topic={"inventory": "inventory", "capability": "capability", "rest_page": "rest_page"}[core.operation],
                reference_id=core.reference_id, producer_commit=core.producer_commit.model_dump(mode="json"),
                target=core.target.model_dump(mode="json") if core.target else None,
                window=core.window.model_dump(mode="json") if core.window else None,
                request_payload=core.reconciliation_payload,
            )
        return _kernel_model(m.ReconciliationRequest, canonical), core

    def _sql_reconciliation_request(self, context, request_id, *, producer):
        self._control(context)
        document, _ = self._native_reconciliation(context, m.canonical_id(request_id), producer)
        return self._canonical_reconciliation(context, document)[0] if document is not None else None

    def _frontier_proof(self, context: m.MonitoringContext, request_id: str) -> m.FrontierValidation:
        key = stable_id(context, f"frontier-proof:{request_id}")
        row = self._sql.get("frontier_validation", key, context)
        if row is None:
            raise MonitoringUnavailable("The original resolution lost its immutable proof")
        try:
            return m.FrontierValidation.model_validate({"validation_id": key, **json.loads(row.payload)})
        except (TypeError, ValueError) as exc:
            logger.error("Invalid immutable frontier proof request_hash=%s", key_digest(request_id))
            raise MonitoringUnavailable("The original resolution proof is unreadable") from exc

    def _sql_reconcile_work(self, work, *, connector_publisher: ConnectorPublisher | None = None):
        with self._using_connector_publisher(connector_publisher):
            return self._sql_reconcile_claimed_work(work)

    def _sql_reconcile_claimed_work(self, work):
        if work.kind != "reconcile_state" or work.lease is None:
            raise MonitoringConflict("Deterministic reconciliation requires its own leased work")
        self._sql.lock_context(work)
        operation_id = stable_id(work, f"publication:{work.work_id}:{work.lease.fence}")
        prior = self._sql.get_receipt("controller.resolve_frontier", operation_id, work)
        if prior is not None:
            proof = self._frontier_proof(work, operation_id)
            if (
                proof.work_id != work.work_id or proof.lease_owner_id != work.lease.owner_id
                or proof.lease_fence != work.lease.fence
            ):
                raise MonitoringConflict("The original reconciliation belongs to another work owner")
            return self._reconciliation_response(m.ReconcileStateRequest(
                **_stamp(work), request_id=operation_id, work_id=work.work_id, lease=work.lease,
                expected_work_revision=proof.expected_work_revision, expected_policy_revision=proof.policy_revision,
                expected_frontier_revision=proof.through_revision,
            ), self._receipt_result(prior))
        document, handoff = self._native_reconciliation(work, work.reconcile_request_id, work.reconcile_producer)
        if document is None or handoff.work_id != work.work_id:
            raise MonitoringUnavailable("Reconciliation has no exact immutable producer handoff")
        control = self._control(work)
        frontier = self._get("validation_frontier", handoff.frontier_key, work, m.ValidationFrontier)
        if frontier is None:
            raise MonitoringUnavailable("Accepted intake has no protected frontier")
        request = m.ReconcileStateRequest(
            **_stamp(work), request_id=operation_id,
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_policy_revision=control.revision, expected_frontier_revision=frontier.accepted_revision,
        )
        return self._sql_reconcile_state(request)

    def _sql_reconcile_state(self, request):
        self._sql.lock_context(request)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        replay = self._rpc_replay("controller.resolve_frontier", request.request_id, request, fingerprint)
        if replay is not None:
            return self._reconciliation_response(request, replay)
        control = self._current(m.RegistryVersion(**_stamp(request), revision=request.expected_policy_revision))
        work = self._owned_work(request, request.work_id, request.lease, request.expected_work_revision)
        if work.kind != "reconcile_state":
            raise MonitoringConflict("Publication cannot promote ordinary work into reconciliation")
        document, handoff = self._native_reconciliation(request, work.reconcile_request_id, work.reconcile_producer)
        if document is None or handoff.work_id != work.work_id:
            raise MonitoringUnavailable("Current work has no exact accepted handoff")
        current = handoff.policy_revision == control.revision and not control.maintenance
        for value in document["evidence"]:
            binding = m.EvidenceBinding.model_validate(value)
            row = self._sql.get(binding.kind, binding.key, request)
            if row is None or row.version != binding.revision or (
                hashlib.sha256(row.payload.encode("utf-16-le")).hexdigest() != binding.payload_hash
            ):
                current = False
        window_row = self._sql.get("validation_window", handoff.frontier_key, request)
        if handoff.requires_window and window_row is None:
            raise MonitoringUnavailable("A first-page validation window is missing, not complete")
        window = json.loads(window_row.payload) if window_row else None
        complete = False
        reject_window = False
        handoff_row = self._sql.get(
            "validation_handoff", f"{handoff.frontier_key}:handoff:{handoff.frontier_revision}", request,
        )
        acknowledge_handoff = (
            window_row is None and handoff_row is not None and handoff_row.status in {"published", "rejected"}
        )
        if request.reject_whole_window:
            if window_row is None or window_row.status not in {"collecting", "awaiting_validation"}:
                raise MonitoringConflict("Whole-window rejection requires a current unfinished window")
            decision, detail, complete, reject_window = "rejected", request.detail, True, True
        elif window_row is not None and window_row.status in {"rejected", "validated"}:
            decision, detail = "rejected", "Acknowledge the exact protected terminal window without republishing it."
        elif acknowledge_handoff:
            decision = handoff_row.status
            detail = "Acknowledge the original non-window handoff under its committed prefix without republishing it."
        elif not current:
            decision, detail = "rejected", "Accepted intent or raw evidence was superseded or changed policy."
            if handoff.policy_revision != control.revision and window_row is not None and window_row.status in {
                "collecting", "awaiting_validation",
            }:
                complete, reject_window = True, True
                detail = "Reject the unfinished window under the current policy without changing earlier page decisions."
        else:
            canonical, core = self._canonical_reconciliation(request, document)
            if core is not None and work.reconcile_request_id != core.request_id:
                decision, detail = "published", "Accepted bounded intake part was validated; publication remains fenced."
            else:
                with self._source_scope(work, canonical):
                    state, detail = self._publish_reconciliation(canonical, control)
                decision = "rejected" if state == "rejected" else "published"
                complete = bool(window is not None and window.get("collection_complete"))
                if core is not None and core.operation == "inventory" and core.result.completeness != "complete":
                    decision = "rejected" if core.result.completed_at is not None else "published"
                reject_window = complete and decision == "rejected"
        validation = m.FrontierValidation(
            validation_id=stable_id(request, f"frontier-proof:{request.request_id}"), work_id=work.work_id,
            lease_owner_id=work.lease.owner_id, lease_fence=work.lease.fence,
            expected_work_revision=work.revision, policy_revision=control.revision,
            frontier_key=handoff.frontier_key, through_revision=request.expected_frontier_revision,
            producer_request_id=handoff.producer_request_id, producer_fingerprint=handoff.producer_fingerprint,
            evidence_digest=handoff.evidence_digest, decision=decision, detail=detail,
            window_complete=complete, reject_whole_window=reject_window,
            acknowledge_handoff=acknowledge_handoff,
            closing_request_id=window.get("closing_request_id") if window and complete and not reject_window else None,
        )
        proof = self._persisted(validation).model_dump(mode="json", exclude={"validation_id"})
        proof["evidence_digest"] = validation.evidence_digest.upper()
        key = validation.validation_id
        if self._sql.get("frontier_validation", key, request) is not None:
            raise MonitoringConflict("Uncommitted frontier proof identity already exists")
        payload = _json(proof)
        self._sql.put(StoredRecord(
            context=m.MonitoringContext(**_stamp(request)), kind="frontier_validation", key=key,
            version=1, payload=payload, parent_key=work.work_id,
        ))
        native = self._sql.rpc("controller.resolve_frontier", {
            **_stamp(request), "request_id": request.request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "work_id": work.work_id, "owner_id": work.lease.owner_id,
            "fence": work.lease.fence, "work_revision": work.revision,
            "expected_frontier_revision": request.expected_frontier_revision,
            "validation_id": key, "validation_hash": hashlib.sha256(payload.encode("utf-16-le")).hexdigest().upper(),
        })["result"]
        resolved = self._reconciliation_response(request, native)
        transition = "retry" if resolved.state == "pending_validation" else "complete"
        self._sql_transition(
            request, work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            request_id=stable_id(request, f"publication-work:{request.request_id}"), fingerprint=fingerprint,
            transition=transition, retry_at=self._now() + timedelta(seconds=15) if transition == "retry" else None,
            detail=detail[:2000], reconciliation=True,
        )
        self._sql.operation_identity("controller.resolve_frontier", request.request_id)
        return resolved

    def _reconciliation_response(self, request, native):
        value = _kernel_model(m.FrontierResolution, native)
        receipt = self._sql.get_receipt("controller.resolve_frontier", request.request_id, request)
        if receipt is None:
            raise MonitoringUnavailable("The original frontier resolution receipt is unavailable")
        proof = self._frontier_proof(request, request.request_id)
        if (
            value != _kernel_model(m.FrontierResolution, self._receipt_result(receipt))
            or value.work_id != request.work_id or value.work_fence != request.lease.fence
            or value.frontier_revision != request.expected_frontier_revision
            or proof.work_id != value.work_id or proof.lease_owner_id != request.lease.owner_id
            or proof.lease_fence != value.work_fence or proof.expected_work_revision != request.expected_work_revision
            or proof.policy_revision != request.expected_policy_revision
            or proof.frontier_key != value.frontier_key or proof.through_revision != value.frontier_revision
            or proof.producer_request_id != value.producer_request_id
            or (value.resolution_scope == "window") != proof.reject_whole_window
            or (value.resolution_scope == "handoff_acknowledgement") != proof.acknowledge_handoff
        ):
            raise MonitoringUnavailable("Original frontier resolution does not match its immutable work/frontier proof")
        if value.resolution_scope == "window_acknowledgement":
            original = self._sql.get_receipt("controller.resolve_frontier", value.window_resolution_request_id, request)
            if original is None:
                raise MonitoringUnavailable("The exact original terminal window receipt is unavailable")
            resolution = _kernel_model(m.FrontierResolution, self._receipt_result(original))
            if (
                proof.decision != "rejected" or resolution.state != value.window_resolution_state
                or resolution.resolution_scope == "window_acknowledgement"
                or resolution.state == "rejected" and resolution.resolution_scope != "window"
                or resolution.frontier_key != value.frontier_key or resolution.frontier_revision != value.frontier_revision
                or resolution.validated_revision != value.validated_revision
            ):
                raise MonitoringUnavailable("Window acknowledgement does not match the original terminal window/prefix")
        elif value.resolution_scope == "handoff_acknowledgement":
            original_receipt = self._sql.get_receipt(
                "controller.resolve_frontier", value.handoff_resolution_request_id, request,
            )
            prefix_receipt = self._sql.get_receipt(
                "controller.resolve_frontier", value.frontier_resolution_request_id, request,
            )
            if original_receipt is None or prefix_receipt is None:
                raise MonitoringUnavailable("Handoff acknowledgement lost its original decision or committed-prefix receipt")
            original = _kernel_model(m.FrontierResolution, self._receipt_result(original_receipt))
            prefix = _kernel_model(m.FrontierResolution, self._receipt_result(prefix_receipt))
            if (
                proof.decision != value.state or original.resolution_scope != "handoff"
                or original.work_id != value.work_id or original.work_fence != value.handoff_resolution_work_fence
                or original.producer_request_id != value.producer_request_id
                or original.handoff_revision != value.handoff_revision or original.handoff_decision != value.state
                or original.state not in {"pending_validation", value.state}
                or original.frontier_key != value.frontier_key or prefix.frontier_key != value.frontier_key
                or prefix.resolution_scope != "handoff" or prefix.state not in {"published", "rejected"}
                or prefix.validated_revision != value.frontier_resolution_revision
                or not original.frontier_revision <= prefix.frontier_revision <= value.frontier_revision
            ):
                raise MonitoringUnavailable("Handoff acknowledgement does not match its immutable decision and prefix")
        return m.ReconciliationResult(
            **_stamp(request), request_id=request.request_id, work_id=value.work_id,
            producer_request_id=value.producer_request_id, policy_revision=request.expected_policy_revision,
            frontier_key=value.frontier_key, frontier_revision=value.frontier_revision, state=value.state,
            detail="Controller acknowledgement is durable; pending_validation is not completed technical proof.",
            published_at=receipt.recorded_at,
            resolution_scope=value.resolution_scope, window_rejection_request_id=value.window_rejection_request_id,
            handoff_revision=value.handoff_revision,
            handoff_resolution_request_id=value.handoff_resolution_request_id,
            handoff_resolution_work_fence=value.handoff_resolution_work_fence,
            frontier_resolution_request_id=value.frontier_resolution_request_id,
            frontier_resolution_revision=value.frontier_resolution_revision,
            window_resolution_request_id=value.window_resolution_request_id,
            window_resolution_state=value.window_resolution_state,
        )

    def _rpc_replay(
        self, operation: str, request_id: str, context: m.MonitoringContext, fingerprint: str,
    ) -> dict | None:
        receipt = self._sql.get_receipt(operation, request_id, context)
        if receipt is None:
            return None
        if receipt.fingerprint != fingerprint:
            raise MonitoringConflict("Original SQL operation request was reused with different validated content")
        return self._receipt_result(receipt)

    @staticmethod
    def _receipt_result(receipt: StoredReceipt) -> dict:
        try:
            payload = json.loads(receipt.payload)
            if not isinstance(payload, dict) or set(payload) != {"binding_hash", "result"} or not isinstance(payload["result"], dict):
                raise ValueError("Invalid guarded receipt shape")
            TypeAdapter(m.Fingerprint).validate_python(payload["binding_hash"])
        except (ValueError, TypeError) as exc:
            logger.error("Invalid guarded SQL receipt operation=%s request_hash=%s", receipt.operation, key_digest(receipt.request_id))
            raise MonitoringUnavailable("The original guarded SQL receipt is unreadable") from exc
        return payload["result"]

    def _sql_enqueue_work(self, work: m.MonitoringWorkDraft) -> m.MonitoringWork:
        if work.kind == "reconcile_state":
            raise MonitoringKernelUnsupported("Only protected producer handoffs may create reconciliation work")
        fingerprint = key_digest(_json(work.model_dump(mode="json")))
        replay = self._rpc_replay("controller.enqueue_work", work.work_id, work, fingerprint)
        if replay is not None:
            return _kernel_model(m.MonitoringWork, replay["work"])
        self._sql.lock_context(work)
        result = None
        for index, draft in enumerate(self._inventory_drafts(work)):
            reply = self._sql.rpc("controller.enqueue_work", {
                **_stamp(work), "request_id": draft.work_id,
                "fingerprint": fingerprint if index == 0 else key_digest(_json(draft.model_dump(mode="json"))),
                "expected_revision": work.policy_revision, "work_id": draft.work_id,
                "draft_json": _json(self._persisted(draft).model_dump(mode="json", exclude_none=True)),
            })["result"]
            if index == 0:
                result = reply
        self._sql.operation_identity("controller.enqueue_work", work.work_id)
        saved = _kernel_model(m.MonitoringWork, result["work"])
        if saved.work_id != work.work_id or saved.target != work.target or saved.execution != work.execution:
            raise MonitoringUnavailable("Guarded enqueue returned a different immutable work identity")
        return saved

    def _sql_claim_work(self, request: m.WorkClaimRequest) -> tuple[m.MonitoringWork, ...]:
        for kind in request.kinds:
            self._authorize_work(kind)
        self._sql.lock_context(request)
        after_workspace = self._sql._fair_workspace(request) if self.component == "controller" else ""
        claimed = []
        for record in self._sql.due(request, after_workspace=after_workspace):
            candidate = self._decode(record, m.MonitoringWork)
            if candidate.action_reservation_id is not None:
                action = self._get("action", candidate.action_reservation_id, request, m.ActionReservation)
                if action is None or action.request.source_execution != candidate.execution or (
                    action.state in {"submitted", "uncertain"} and action.next_verification_at is None
                ):
                    raise MonitoringUnavailable("Queued action work lost its exact action lineage or verification deadline")
            reply = self._sql.rpc(f"{self.component}.claim_work", {
                **_stamp(request), "work_id": record.key, "owner_id": request.owner_id,
                "lease_seconds": request.lease_seconds,
            })
            if reply["status"] == "not_acquired":
                continue
            saved = _kernel_model(m.MonitoringWork, reply["result"]["work"])
            lease = _kernel_model(m.LeaseToken, reply["result"]["lease"])
            self._authorize_work(saved.kind)
            if (
                saved.work_id != record.key or saved.lease != lease or saved.state != "leased"
                or lease.owner_id != request.owner_id
                or _stamp(saved) != _stamp(request)
            ):
                raise MonitoringUnavailable("Guarded claim returned inconsistent work/lease ownership")
            claimed.append(saved)
        return tuple(claimed)

    def _sql_transition(
        self, context: m.MonitoringContext, *, work_id: str, lease: m.LeaseToken,
        expected_work_revision: int | None, request_id: str, fingerprint: str,
        transition: str, lease_seconds: int | None = None, retry_at: datetime | None = None,
        detail: str | None = None, reconciliation: bool = False,
    ) -> m.MonitoringWork:
        operation = f"{self.component}.transition_work"
        control = self._control(context)
        replay = self._rpc_replay(operation, request_id, context, fingerprint)
        if replay is not None:
            saved = _kernel_model(m.MonitoringWork, replay["work"])
            if saved.work_id != work_id or _stamp(saved) != _stamp(context):
                raise MonitoringUnavailable("Original work-transition receipt identifies another work/context")
            return saved
        work = self._get("work", m.canonical_id(work_id), context, m.MonitoringWork)
        if work is None:
            raise MonitoringLeaseLost("Guarded transition requires its existing work")
        self._authorize_work(work.kind)
        if work.kind == "reconcile_state" and transition in {"complete", "disposition"} and not reconciliation:
            raise MonitoringKernelUnsupported("Reconciliation completion must atomically publish or reject its protected frontier")
        result = self._sql.rpc(operation, {
            **_stamp(context), "request_id": request_id, "fingerprint": fingerprint,
            # A fresh observation can finish an older collection work item;
            # its original receipt, not the work's creation policy, is the proof.
            "expected_revision": (
                control.revision if self.component == "worker" and work.kind == "connector_reconcile"
                and transition == "complete" else work.policy_revision
            ),
            "work_id": work_id,
            "owner_id": lease.owner_id, "fence": lease.fence,
            "work_revision": work.revision if expected_work_revision is None else expected_work_revision,
            "transition": transition, "lease_seconds": lease_seconds,
            "retry_at": retry_at,
            "detail": self._redactor(detail) if detail else None, "finalization_id": None,
        })["result"]
        saved = _kernel_model(m.MonitoringWork, result["work"])
        if saved.work_id != work_id:
            raise MonitoringUnavailable("Guarded transition returned another work identity")
        return saved

    def _sql_renew_lease(self, request: m.LeaseRenewal) -> m.LeaseToken:
        if request.lease.resource_key.startswith("partition:v1:"):
            row = self._sql.get("partition_ownership", request.lease.resource_key, request.lease)
            if row is None:
                raise MonitoringLeaseLost("Partition ownership is absent")
            owner = _kernel_model(m.PartitionOwnershipResult, json.loads(row.payload))
            self._owned_native_partition(owner.partition, request.lease)
            result = self._sql_change_partition_ownership(OwnershipChange(
                partition=owner.partition, expected_etag=owner.etag,
                claim=m.PartitionClaimRequest(
                    partition=owner.partition, owner_id=request.lease.owner_id, lease_seconds=request.lease_seconds,
                ),
            ))
            if result is None or result.lease is None or result.lease.fence != request.lease.fence:
                raise MonitoringLeaseLost("Partition renewal lost its exact ownership fence")
            return result.lease
        if not request.lease.resource_key.startswith("work:v1:"):
            raise MonitoringComponentDenied("Renewal requires a typed work or partition lease")
        identity = key_digest(_json(request.model_dump(mode="json")))
        saved = self._sql_transition(
            request.lease, work_id=request.lease.resource_key.rsplit(":", 1)[1],
            lease=request.lease, expected_work_revision=None,
            request_id=stable_id(request.lease, f"renew:{identity}"), fingerprint=identity,
            transition="renew", lease_seconds=request.lease_seconds,
        )
        if saved.lease is None:
            raise MonitoringUnavailable("Guarded renewal omitted its durable current lease")
        return saved.lease

    def _sql_disposition_work(self, request: m.WorkDispositionRequest) -> m.MonitoringWork:
        if self.component == "controller" and request.disposition != "retry":
            self._sql.lock_context(request)
            fingerprint = key_digest(_json(request.model_dump(mode="json")))
            replay = self._rpc_replay("controller.disposition_source", request.request_id, request, fingerprint)
            if replay is not None:
                result = _kernel_model(m.SourceDispositionResult, replay)
                if result.work.work_id != request.work_id:
                    raise MonitoringUnavailable("Original disposition receipt belongs to another work")
                return result.work
            work = self._action_work(request, request.work_id, request.lease, revision=request.expected_work_revision)
            if work.action_reservation_id is not None or self._work_reservation(work) is not None:
                raise MonitoringConflict("Existing effects require incident verification/finalization")
            source = self._noneffect_source(work, request.disposition)
            with self._source_scope(work):
                arguments = self._source_arguments(source, "controller.disposition_source", disposing=True, extra={
                    "disposition": request.disposition, "detail": self._redactor(request.detail)[:2000],
                    "subject_work_id": None, "expected_subject_revision": None,
                })
            arguments.update(request_id=request.request_id, fingerprint=fingerprint)
            result = _kernel_model(m.SourceDispositionResult, self._sql.rpc("controller.disposition_source", arguments)["result"])
            if result.work.work_id != work.work_id or result.work.state != "dispositioned" or (
                result.work.lease is not None or result.disposition.execution != work.execution
            ):
                raise MonitoringUnavailable("Non-effect disposition did not atomically finish its exact source work")
            return result.work
        return self._sql_transition(
            request, work_id=request.work_id, lease=request.lease,
            expected_work_revision=request.expected_work_revision,
            request_id=request.request_id, fingerprint=key_digest(_json(request.model_dump(mode="json"))),
            transition="retry" if request.disposition == "retry" else "disposition",
            retry_at=request.retry_at, detail=request.detail,
        )

    def _sql_complete_collection_work(
        self, context: m.MonitoringContext, *, work_id: str, lease: m.LeaseToken, expected_work_revision: int,
    ) -> m.MonitoringWork:
        payload = {
            "context": context.model_dump(mode="json"), "work_id": work_id,
            "lease": lease.model_dump(mode="json"), "expected_work_revision": expected_work_revision,
        }
        identity = key_digest(_json(payload))
        return self._sql_transition(
            context, work_id=work_id, lease=lease, expected_work_revision=expected_work_revision,
            request_id=stable_id(context, f"collection-complete:{identity}"),
            fingerprint=identity, transition="complete",
        )

    @staticmethod
    def _partition_arguments(partition: m.PartitionIdentity) -> dict[str, object]:
        return partition.model_dump(mode="json")

    def _partition_journal(self, partition: m.PartitionIdentity) -> m.PartitionOwnershipResult | None:
        row = self._sql.get("partition_ownership", partition.key, partition)
        if row is None:
            return None
        value = _kernel_model(m.PartitionOwnershipResult, json.loads(row.payload))
        if (
            value.partition != partition or value.partition_key != partition.key or value.ownership_revision != row.version
            or row.status != ("owned" if value.lease else "released")
            or value.lease is not None and _stamp(value.lease) != _stamp(partition)
        ):
            raise MonitoringUnavailable("Partition journal columns disagree with its native ownership result")
        return value

    @staticmethod
    def _ownership_result(partition, value):
        result = _kernel_model(m.PartitionOwnershipResult, value)
        if result.partition != partition or result.partition_key != partition.key or (
            result.lease is not None and _stamp(result.lease) != _stamp(partition)
        ):
            raise MonitoringUnavailable("Native partition ownership identifies another partition/context")
        return PartitionOwnership(
            partition=partition, lease=result.lease,
            etag=result.etag, modified_at=result.modified_at,
        )

    def _sql_list_partition_ownership(self, scope: ConnectorScope) -> tuple[PartitionOwnership, ...]:
        self._partition_connector(scope, receiving=False)
        rows = self._all(
            "partition_ownership", scope, m.PartitionOwnershipResult, filters={"parent_key": scope.connector_id},
        )
        result = []
        for row in rows:
            if row.partition.connector_id != scope.connector_id or row.partition.consumer_group != scope.consumer_group:
                raise MonitoringUnavailable("Partition catalogue belongs to another connector or consumer group")
            current = self._partition_journal(row.partition)
            if current != row:
                raise MonitoringUnavailable("Partition catalogue changed during its shared-state read")
            result.append(self._ownership_result(row.partition, row.model_dump()))
        return tuple(sorted(result, key=lambda item: item.partition.partition_id))

    def _sql_change_partition_ownership(self, request: OwnershipChange) -> PartitionOwnership | None:
        partition = request.partition
        self._sql.lock_context(partition)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        request_id = stable_id(partition, f"partition-ownership:{fingerprint}")
        replay = self._rpc_replay("worker.partition", request_id, partition, fingerprint)
        if replay is not None:
            return self._ownership_result(partition, replay)
        control = self._control(partition)
        prior = self._partition_journal(partition)
        prior_etag = prior.etag if prior else None
        if request.expected_etag != prior_etag:
            return None
        lease = prior.lease if prior else None
        if request.release is not None:
            self._partition_connector(partition, receiving=False)
            if lease is None or (
                lease.owner_id, lease.fence,
            ) != (request.release.owner_id, request.release.fence):
                return None
            transition, new_owner, seconds = "release", None, None
        else:
            self._partition_connector(partition, receiving=True)
            claim = request.claim
            if lease is not None and lease.expires_at > self._now() and lease.owner_id != claim.owner_id:
                return None
            transition = "renew" if lease and lease.owner_id == claim.owner_id and lease.expires_at > self._now() else "claim"
            new_owner, seconds = claim.owner_id, claim.lease_seconds
        native = self._sql.rpc("worker.partition", {
            **self._partition_arguments(partition), "request_id": request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "transition": transition,
            "expected_owner_id": prior.last_owner_id if prior else None,
            "expected_fence": prior.last_fence if prior else None,
            "expected_ownership_revision": prior.ownership_revision if prior else 0,
            "new_owner_id": new_owner, "lease_seconds": seconds, "first_sequence_number": None,
            "broker_observed_at": None,
        })["result"]
        result = self._ownership_result(partition, native)
        stored = self._partition_journal(partition)
        if stored is None or self._ownership_result(partition, stored.model_dump()) != result:
            raise MonitoringUnavailable("The native ownership result disagrees with its atomic journal")
        if (transition == "release") != (result.lease is None) or (
            result.lease is not None and result.lease.owner_id != new_owner
        ):
            raise MonitoringUnavailable("Native partition transition returned a different owner or release state")
        return result

    def _sql_claim_partition(self, request: m.PartitionClaimRequest) -> m.LeaseToken | None:
        self._sql.lock_context(request.partition)
        prior = self._partition_journal(request.partition)
        result = self._sql_change_partition_ownership(OwnershipChange(
            partition=request.partition,
            expected_etag=prior.etag if prior else None,
            claim=request,
        ))
        return result.lease if result else None

    def _owned_native_partition(self, partition, lease):
        self._partition_connector(partition, receiving=True)
        prior = self._partition_journal(partition)
        if prior is None or prior.lease is None or prior.lease.expires_at <= self._now() or (
            prior.lease.owner_id, prior.lease.fence,
        ) != (lease.owner_id, lease.fence):
            raise MonitoringLeaseLost("Partition ownership is absent, expired or superseded")
        return prior

    @staticmethod
    def _stream_start_result(value: m.StreamStartRecord) -> StreamStart:
        return StreamStart(
            partition=value.partition, first_sequence_number=value.first_sequence_number,
            recorded_at=value.recorded_at, history_before_start=value.history_before_start, gaps=value.gaps,
        )

    def _sql_get_stream_start(self, partition: m.PartitionIdentity) -> StreamStart | None:
        self._control(partition)
        row = self._sql.get("stream_start", partition.key, partition)
        if row is None:
            return None
        value = _kernel_model(m.StreamStartRecord, json.loads(row.payload))
        if value.partition != partition or value.partition_key != partition.key or row.sequence_number != value.first_sequence_number:
            raise MonitoringUnavailable("Pinned stream start disagrees with its exact native partition/sequence")
        return self._stream_start_result(value)

    def _sql_ensure_stream_start(self, request: StreamStartRequest) -> StreamStart:
        partition = request.partition
        self._sql.lock_context(partition)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        request_id = stable_id(partition, f"stream-start:{fingerprint}")
        replay = self._rpc_replay("worker.partition", request_id, partition, fingerprint)
        if replay is not None:
            result = _kernel_model(m.PartitionStartResult, replay)
            if result.partition != partition:
                raise MonitoringUnavailable("Original broker-start receipt lost its immutable pinned boundary")
            return self._stream_start_result(result.start)
        replay = self._rpc_replay("worker.observe_retention", request_id, partition, fingerprint)
        if replay is not None:
            result = _kernel_model(m.StreamRetentionResult, replay)
            if result.partition != partition:
                raise MonitoringUnavailable("Original retention receipt belongs to another partition")
            return self._stream_start_result(result.start)
        control = self._control(partition)
        owner = self._owned_native_partition(partition, request.lease)
        if request.observed_at > self._now():
            raise MonitoringConflict("Broker boundary observation time is in the database clock's future")
        current = self._sql_get_stream_start(partition)
        checkpoint = self._sql_get_stream_checkpoint(partition)
        if current is None and checkpoint is not None:
            raise MonitoringUnavailable("A stream checkpoint exists without its original broker start")
        if current is not None:
            result = _kernel_model(m.StreamRetentionResult, self._sql.rpc("worker.observe_retention", {
                **self._partition_arguments(partition), "request_id": request_id, "fingerprint": fingerprint,
                "expected_revision": control.revision, "owner_id": request.lease.owner_id, "fence": request.lease.fence,
                "expected_checkpoint_revision": checkpoint.revision if checkpoint else 0,
                "first_available_sequence_number": request.first_available_sequence_number, "observed_at": request.observed_at,
            })["result"])
            if result.partition != partition or result.start.first_sequence_number != current.first_sequence_number or (
                (result.checkpoint is None) != (checkpoint is None)
            ) or (result.checkpoint is not None and result.checkpoint.position != checkpoint.position):
                raise MonitoringUnavailable("Retention observation changed its original boundary or checkpoint")
            if result.observation is not None and (
                result.observation.first_available_sequence_number != request.first_available_sequence_number
            ):
                raise MonitoringUnavailable("Retention receipt reports another observed broker boundary")
            return self._stream_start_result(result.start)
        first = current.first_sequence_number if current else request.first_available_sequence_number
        result = _kernel_model(m.PartitionStartResult, self._sql.rpc("worker.partition", {
            **self._partition_arguments(partition), "request_id": request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "transition": "pin_start",
            "expected_owner_id": request.lease.owner_id, "expected_fence": request.lease.fence,
            "expected_ownership_revision": owner.ownership_revision,
            "new_owner_id": None, "lease_seconds": None, "first_sequence_number": first,
            "broker_observed_at": request.observed_at,
        })["result"])
        saved = self._sql_get_stream_start(partition)
        if (
            saved is None or result.partition != partition or result.first_sequence_number != first
            or result.ownership_revision != owner.ownership_revision or saved.first_sequence_number != first
        ):
            raise MonitoringUnavailable("Native broker-start result disagrees with the persisted boundary")
        return saved

    @staticmethod
    def _stream_positions(value: str) -> tuple[_StreamPositionEnvelope, ...]:
        try:
            positions = _STREAM_POSITIONS.validate_json(value)
        except ValidationError as exc:
            raise MonitoringUnavailable("Native stream handoff has invalid bounded original positions") from exc
        sequences = [entry.receipt.position.sequence_number for entry in positions]
        if sequences != sorted(set(sequences)):
            raise MonitoringUnavailable("Native original broker positions are not distinct and ordered")
        return positions

    def _stream_position(self, partition, sequence):
        key = f"{partition.key}:position:{sequence}"
        row = self._sql.get("stream_position", key, partition)
        if row is None:
            return None
        value = _kernel_model(m.StreamPositionRecord, json.loads(row.payload))
        if row.parent_key != partition.key or row.sequence_number != sequence:
            raise MonitoringUnavailable("Native position journal belongs to another partition/sequence")
        kind = "signal" if value.receipt_kind == "identified" else "unidentified_signal"
        receipt = self._sql.get(kind, value.receipt_key, partition)
        if receipt is None or hashlib.sha256(receipt.payload.encode("utf-16-le")).hexdigest() != value.payload_hash:
            raise MonitoringUnavailable("Native position lost its exact durable receipt payload")
        return value

    def _delivery_candidates(self, context, connector, collector_identity_id):
        desired = self._get("connector_desired", connector.connector_id, context, m.ConnectorDesiredState)
        if desired is None:
            return []
        parameters = (
            context.tenant_id, context.epoch, _digest(connector.connector_id), connector.connector_id,
            collector_identity_id, connector.ownership_id, connector.policy_revision,
            m.connector_definition_hash(connector.desired_definition),
            desired.published_at.isoformat(), desired.published_at.isoformat(),
        )
        candidates = []
        after = None
        while True:
            limit = min(200, SCAN_BUDGET + 1 - len(candidates))
            rows = self._sql.db.query(
                f"SELECT TOP ({limit}) payload, full_key FROM {self._sql.tables['monitoring_records']} "
                "WHERE tenant_id=? AND epoch=? AND record_kind='signal' AND status='accepted' "
                "AND parent_hash=? AND parent_key=? "
                "AND JSON_VALUE(payload,'$.transport.collector_identity_id')=? "
                "AND JSON_VALUE(payload,'$.transport.ownership_id')=? "
                "AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.transport.policy_revision'))=? "
                "AND JSON_VALUE(payload,'$.transport.definition_hash')=? "
                "AND TRY_CONVERT(datetimeoffset,JSON_VALUE(payload,'$.position.enqueued_at'))>=TRY_CONVERT(datetimeoffset,?) "
                "AND TRY_CONVERT(datetimeoffset,JSON_VALUE(payload,'$.transport.identity_verified_at'))>=TRY_CONVERT(datetimeoffset,?) "
                + ("AND key_hash>? " if after is not None else "")
                + "ORDER BY key_hash",
                *parameters, *((after,) if after is not None else ()),
            )
            if len(rows) > limit or any(
                len(row) != 2 or not all(isinstance(value, str) for value in row) for row in rows
            ):
                raise MonitoringUnavailable("Original connector delivery query returned an invalid shape")
            for payload, key in rows:
                signal = _kernel_model(m.SignalReceipt, json.loads(payload))
                cursor = _digest(key)
                if signal.delivery.key != key or after is not None and cursor <= after:
                    raise MonitoringUnavailable("Original connector delivery pagination changed identity or did not advance")
                after = cursor
                candidates.append(signal)
            if len(candidates) > SCAN_BUDGET:
                raise MonitoringConflict("Connector delivery verification exceeds its bounded candidate budget")
            if len(rows) < limit:
                return candidates

    def _delivery_original(self, signal, control) -> datetime:
        transport = signal.transport
        journal = self._stream_position(signal.partition, signal.position.sequence_number)
        if journal is None or journal.batch_id != transport.request_id or journal.receipt_key != signal.delivery.key:
            raise MonitoringUnavailable("Delivery proof lost its original native acceptance or broker position")
        if self.component == "controller":
            # The checked fact view requires an immutable accepted receipt.
            # Planning uses its original protected handoff; the publication RPC
            # rechecks the exact private receipt without granting cross-role reads.
            document, handoff = self._native_reconciliation(control, transport.request_id, "worker")
            if (
                document is None or handoff is None or document["topic"] != "stream_intake"
                or handoff.producer_operation != "worker.commit_positions"
                or handoff.policy_revision != transport.policy_revision
            ):
                raise MonitoringUnavailable("Delivery proof lost its protected original stream handoff")
            positions = self._stream_positions(document["request_payload"]["positions_json"])
            originals = [entry for entry in positions if (
                entry.receipt_key == signal.delivery.key and entry.receipt.position == signal.position
                and entry.receipt.partition == signal.partition
                and isinstance(entry.receipt, m.SignalReceipt) and entry.receipt.status == "accepted"
                and entry.receipt.transport == transport
            )]
            bindings = [binding for binding in document["evidence"] if (
                binding["kind"] == "signal" and binding["key"] == signal.delivery.key
                and binding["payload_hash"].lower() == journal.payload_hash
            )]
            if len(originals) != 1 or len(bindings) != 1:
                raise MonitoringUnavailable("Delivery proof differs from its original protected stream evidence")
            # The protected handoff and batch receipt share the RPC's SQL @now.
            return _read_time(document["created_at"])
        original = self._sql.get_receipt("worker.commit_positions", transport.request_id, control)
        if original is None:
            raise MonitoringUnavailable("Delivery proof lost its original native acceptance receipt")
        accepted = _kernel_model(m.StreamAcceptanceResult, self._receipt_result(original))
        positions = [value for value in accepted.positions if value.sequence_number == signal.position.sequence_number]
        if (
            accepted.batch_id != transport.request_id or accepted.partition != signal.partition
            or signal.delivery.key not in accepted.receipt_keys or len(positions) != 1
            or positions[0].receipt_kind != "identified" or positions[0].receipt_key != signal.delivery.key
            or positions[0].offset != signal.position.offset
            or positions[0].enqueued_at != signal.position.enqueued_at
            or positions[0].first_committed_batch_id != transport.request_id
            or positions[0].original_payload_hash != journal.payload_hash
            or journal.batch_id != transport.request_id
        ):
            raise MonitoringUnavailable("Delivery proof differs from its immutable native stream acceptance")
        return original.recorded_at

    def _stream_acceptance(self, context, request_id, native, *, positions=None):
        value = _kernel_model(m.StreamAcceptanceResult, native)
        receipt = self._sql.get_receipt("worker.commit_positions", request_id, context)
        if receipt is None or value.batch_id != request_id or _stamp(value.partition) != _stamp(context) or value != _kernel_model(
            m.StreamAcceptanceResult, self._receipt_result(receipt),
        ):
            raise MonitoringUnavailable("Original stream acceptance result is inconsistent")
        if positions is not None:
            if len(positions) != value.position_count:
                raise MonitoringUnavailable("Native stream acceptance omitted original broker positions")
            for position, recorded in zip(positions, value.positions, strict=True):
                partition = position.receipt.partition
                original = position.receipt.position
                if value.partition != partition or (
                    recorded.sequence_number != original.sequence_number
                    or recorded.offset != original.offset or recorded.enqueued_at != original.enqueued_at
                    or recorded.receipt_key != position.receipt_key or recorded.receipt_kind != position.receipt_kind
                ):
                    raise MonitoringUnavailable("Native stream acceptance differs from its exact original positions")
        return m.IntakeReceipt(
            **_stamp(context), request_id=request_id, recorded_at=receipt.recorded_at,
            receipt_keys=value.receipt_keys, work_ids=(value.reconcile_work_id,), publication_status="pending_validation",
        )

    def _commit_stream_positions(self, request, partition, positions):
        self._sql.lock_context(partition)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        replay = self._rpc_replay("worker.commit_positions", request.request_id, partition, fingerprint)
        if replay is not None:
            return self._stream_acceptance(partition, request.request_id, replay, positions=positions)
        control = self._control(partition)
        self._owned_native_partition(partition, request.lease)
        connector = self._partition_connector(partition, receiving=True)
        positions = tuple(
            _update(entry, receipt=self._quarantine_pending_removal(entry.receipt, connector))
            if isinstance(entry.receipt, m.SignalReceipt) else entry
            for entry in positions
        )
        native_json = _json([entry.model_dump(mode="json") for entry in positions])
        if len(native_json.encode("utf-16-le")) > 1_048_576:
            raise MonitoringConflict("Native stream position batch exceeds the bounded 1 MiB NVARCHAR payload")
        native = self._sql.rpc("worker.commit_positions", {
            **self._partition_arguments(partition), "request_id": request.request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "owner_id": request.lease.owner_id, "fence": request.lease.fence,
            "positions_json": native_json,
        })["result"]
        return self._stream_acceptance(partition, request.request_id, native, positions=positions)

    def _sql_record_stream_receipts(self, request: m.StreamReceiptBatch) -> m.IntakeReceipt:
        positions = tuple(_StreamPositionEnvelope(
            receipt_kind="identified", receipt_key=receipt.delivery.key, receipt=self._persisted(receipt),
        ) for receipt in request.receipts)
        return self._commit_stream_positions(request, request.partition, positions)

    def _sql_record_unidentified_receipts(self, request: UnidentifiedReceiptBatch) -> m.IntakeReceipt:
        saved = _UnidentifiedReceipt.model_validate(self._persisted(request.receipt).model_dump())
        position = _StreamPositionEnvelope(
            receipt_kind="unidentified",
            receipt_key=f"{saved.partition.key}:unidentified:{saved.position.sequence_number}", receipt=saved,
        )
        return self._commit_stream_positions(request, saved.partition, (position,))

    def _sql_get_stream_acceptance(self, context, request_id):
        self._control(context)
        request_id = m.canonical_id(request_id)
        receipt = self._sql.get_receipt("worker.commit_positions", request_id, context)
        if receipt is None:
            return None
        return self._stream_acceptance(context, request_id, self._receipt_result(receipt))

    def _checkpoint_result(self, partition, native):
        value = _kernel_model(m.StreamCheckpointRecord, native)
        if value.partition != partition or value.partition_key != partition.key:
            raise MonitoringUnavailable("Native checkpoint identifies another partition")
        return m.StreamCheckpoint(
            partition=partition, revision=value.revision, updated_at=value.updated_at,
            position=value.position,
        )

    def _sql_get_stream_checkpoint(self, partition: m.PartitionIdentity) -> m.StreamCheckpoint | None:
        self._control(partition)
        row = self._sql.get("stream_checkpoint", partition.key, partition)
        if row is None:
            return None
        value = _kernel_model(m.StreamCheckpointRecord, json.loads(row.payload))
        if value.revision != row.version or value.sequence_number != row.sequence_number:
            raise MonitoringUnavailable("Native checkpoint columns disagree with its payload")
        return self._checkpoint_result(partition, value.model_dump())

    def _sql_advance_stream_checkpoint(self, request: m.StreamCheckpointAdvance) -> m.StreamCheckpoint:
        partition = request.partition
        self._sql.lock_context(partition)
        fingerprint = key_digest(_json(request.model_dump(mode="json")))
        replay = self._rpc_replay("worker.advance_checkpoint", request.request_id, partition, fingerprint)
        if replay is not None:
            return self._checkpoint_result(partition, replay)
        control = self._control(partition)
        self._owned_native_partition(partition, request.lease)
        position = self._stream_position(partition, request.through.sequence_number)
        if position is None or (
            position.offset != request.through.offset or position.enqueued_at != request.through.enqueued_at
        ):
            raise MonitoringConflict("Checkpoint must name the exact durable terminal broker position")
        native = self._sql.rpc("worker.advance_checkpoint", {
            **self._partition_arguments(partition), "request_id": request.request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "owner_id": request.lease.owner_id, "fence": request.lease.fence,
            "expected_checkpoint_revision": request.expected_revision,
            "through_sequence_number": request.through.sequence_number, "through_offset": request.through.offset,
        })["result"]
        result = self._checkpoint_result(partition, native)
        if result.revision != request.expected_revision + 1 or result.position != request.through:
            raise MonitoringUnavailable("Guarded checkpoint returned a different revision or original position")
        return result

    def _sql_record_receiver_heartbeat(self, heartbeat: ReceiverHeartbeat) -> ReceiverHeartbeat:
        control = self._control(heartbeat)
        request_id = stable_id(heartbeat, f"heartbeat:{key_digest(heartbeat.model_dump_json())}")
        fingerprint = key_digest(_json(heartbeat.model_dump(mode="json")))
        replay = self._rpc_replay("worker.record_heartbeat", request_id, heartbeat, fingerprint)
        if replay is not None:
            return _kernel_model(ReceiverHeartbeat, replay)
        if heartbeat.observed_at > self._now() or any(
            value is not None and value > heartbeat.observed_at
            for value in (heartbeat.last_delivery_at, heartbeat.last_maintenance_at)
        ):
            raise MonitoringConflict("Receiver heartbeat timestamps are inconsistent with its observation")
        saved = self._persisted(heartbeat)
        result = self._sql.rpc("worker.record_heartbeat", {
            **_stamp(heartbeat), "request_id": request_id, "fingerprint": fingerprint,
            "expected_revision": control.revision, "worker_id": saved.worker_id,
            "connector_id": saved.connector_id, "state": saved.state,
            "transport_connected": saved.transport_connected, "accepted_positions": saved.accepted_positions,
            "last_delivery_at": saved.last_delivery_at,
            "last_maintenance_at": saved.last_maintenance_at,
            "error_code": saved.error_code,
        })["result"]
        recorded = _kernel_model(ReceiverHeartbeat, result)
        if (
            _stamp(recorded) != _stamp(heartbeat) or recorded.worker_id != heartbeat.worker_id
            or recorded.connector_id != heartbeat.connector_id
            or recorded.accepted_positions != heartbeat.accepted_positions
            or recorded.transport_connected != heartbeat.transport_connected
            or recorded.state != heartbeat.state and not (
                control.maintenance and heartbeat.state == "running" and recorded.state == "blocked"
            )
        ):
            raise MonitoringUnavailable("Guarded heartbeat returned a different worker, connector or health state")
        return recorded

    def _sql_safety_review_operation(
        self, context: m.MonitoringContext, request_id: str,
    ) -> m.SafetyReviewOperationReceipt | None:
        if self.component != "web":
            raise MonitoringComponentDenied("Original human-intent receipts are read through the web component SQL route")
        self._control(context)
        request_id = m.canonical_id(request_id)
        receipt = self._sql.get_receipt("web.commit_intent", request_id, context)
        if receipt is None:
            return None
        result = self._receipt_result(receipt)
        if result.get("intent_kind") != "review":
            return None
        intent = result["original_intent"]
        policy_revision = TypeAdapter(m.PositiveRevision).validate_python(result["policy_revision"])
        if result.get("request_id") != request_id or result["intent_id"] != intent["review_id"]:
            raise MonitoringUnavailable("Original safety receipt has inconsistent request/review identities")
        expected_revision = policy_revision - 1
        review = self._review_from_intent(
            intent, revision=result["new_intent_revision"], expected_policy=expected_revision,
            accepted_at=receipt.recorded_at,
        )
        return _kernel_model(m.SafetyReviewOperationReceipt, dict(
            request_id=receipt.request_id, target=review.target, action=review.action,
            expected=m.RegistryVersion(**_stamp(context), revision=expected_revision),
            expected_review_revision=result["expected_intent_revision"], new_review_revision=review.revision,
            fingerprint=receipt.fingerprint, recorded_at=receipt.recorded_at, review=review,
            requested_state=review.requested_state, publication_status=review.publication_status,
        ))


class _KernelBudgetDenied(MonitoringConflict):
    def __init__(self, decision: RateDecision) -> None:
        self.decision = decision
        super().__init__("A required physical-request budget is not currently available")


class KernelRateBudget:
    """Atomic paired budgets through the worker RPC; policy installation is deployer-only."""

    def __init__(self, database: AzureSqlDatabase, *, tables: Mapping[str, str] | None = None) -> None:
        self._sql = SqlBackend(database, tables, component="worker")

    def acquire(self, context: m.MonitoringContext, bucket: str, policy: RatePolicy) -> RateDecision:
        return self.acquire_many(context, ((bucket, policy),))

    def acquire_many(
        self, context: m.MonitoringContext, policies: tuple[tuple[str, RatePolicy], ...],
    ) -> RateDecision:
        policies = _ordered_policies(context, policies)
        try:
            with self._sql.transaction(write=True, operation="worker.rate_budget", request_id="rate-debit"):
                self._sql.lock_context(context)
                decisions = [self._change(context, bucket, policy, None) for bucket, policy in policies]
                denied = [value for value in decisions if not value.allowed]
                if denied:
                    raise _KernelBudgetDenied(RateDecision(
                        False, max(value.checked_at for value in decisions),
                        max(value.retry_at for value in denied),
                    ))
                now = max(value.checked_at for value in decisions)
                return RateDecision(True, now, now)
        except _KernelBudgetDenied as exc:
            return exc.decision

    def defer(
        self, context: m.MonitoringContext, bucket: str, policy: RatePolicy, *, seconds: int,
    ) -> RateDecision:
        _ordered_policies(context, ((bucket, policy),))
        _delay(seconds)
        with self._sql.transaction(write=True, operation="worker.rate_budget", request_id="rate-cooldown"):
            return self._change(context, bucket, policy, seconds)

    def _change(self, context, bucket, policy, delay):
        reply = self._sql.rpc("worker.rate_budget", {
            **_stamp(context), "bucket": bucket, "delay_seconds": delay,
        })["result"]
        if (
            type(reply["request_limit"]) is not int or type(reply["window_seconds"]) is not int
            or (reply["request_limit"], reply["window_seconds"]) != (policy.requests, policy.window_seconds)
        ):
            raise MonitoringConflict("The caller's REST budget differs from the provisioned SQL policy")
        allowed = TypeAdapter(m.StrictBool).validate_python(reply["allowed"])
        now = self._sql.now()
        if allowed:
            return RateDecision(True, now, now)
        blocked = _read_time(reply["blocked_until"]) if reply["blocked_until"] else now
        end = _read_time(reply["window_ends_at"])
        retry_at = max(now, blocked, end if reply["used"] >= policy.requests else now)
        return RateDecision(False, now, retry_at)
