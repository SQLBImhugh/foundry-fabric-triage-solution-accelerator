"""Shared monitoring rules and an explicit, deterministic offline store.

The engine reads/writes individual records through a backend. The SQL backend
does not load this fixture state or inherit an in-memory fallback.
EventPersistence uses that same transaction for ownership metadata and fenced
leases, actual stream starts, every broker position, quarantine and heartbeat
health. A heartbeat is never a source-delivery verification.
Controller reconciliation also plans the one registered transport from current
published admission; only the separate worker performs Fabric topology effects.
"""

from __future__ import annotations

import base64
import hashlib
import inspect
import json
import logging
import threading
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import wraps
from typing import Concatenate, Literal, ParamSpec, Protocol, TypeVar
from uuid import UUID, uuid5

from pydantic import BaseModel, TypeAdapter, ValidationError

from triage.models import Incident
from triage.monitoring import models as m
from triage.monitoring.contracts import (
    ConnectorPublisher,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringLeaseLost,
    MonitoringNotBootstrapped,
    MonitoringSchemaMismatch,
    MonitoringUnavailable,
)
from triage.monitoring.events import (
    WIRE_TO_SUBSCRIPTION_TYPE,
    ConnectorScope,
    OwnershipChange,
    PartitionOwnership,
    ReceiverHeartbeat,
    StreamStart,
    StreamStartRequest,
    UnidentifiedReceiptBatch,
    UnidentifiedSignal,
)
from triage.policy import APPROVAL_REQUIRED_ACTIONS, TriagePolicy
from triage.redaction import redact_text
from triage.store.retries import MAX_ATTEMPTS, backoff_seconds

logger = logging.getLogger("triage.monitoring.memory")
SCAN_BUDGET = 5_000
PLAN_TTL_SECONDS = 900
SOURCE_FRESHNESS_SECONDS = 300
ModelT = TypeVar("ModelT", bound=BaseModel)
ResultT = TypeVar("ResultT")
P = ParamSpec("P")
WEB_OPERATIONS = frozenset({"preview_scope", "activate_scope", "record_safety_review", "request_discovery"})
WORKER_OPERATIONS = frozenset({
    "record_inventory", "record_capability", "record_connector", "record_rest_page",
    "claim_partition", "change_partition_ownership", "ensure_stream_start",
    "record_stream_receipts", "record_unidentified_receipts", "advance_stream_checkpoint",
    "record_receiver_heartbeat", "complete_collection_work",
})
CONTROLLER_OPERATIONS = frozenset({
    "enqueue_work", "observe_source", "bind_approval", "reserve_action",
    "record_action_submission", "record_action_rejection", "record_action_outcome",
    "finalize_work", "reconcile_state", "reconcile_work", "publish_connector",
})
SHARED_WORK_OPERATIONS = frozenset({"claim_work", "renew_lease", "disposition_work"})
CONNECTOR_PUBLICATION_OPERATIONS = frozenset({
    "snapshot", "get_work", "get_reconciliation_request", "get_validation_frontier",
    "list_connectors", "get_connector_desired", "list_targets", "resolve_target", "get_connector_publication",
    "get_operation_receipt", "publish_connector", "enqueue_work",
})


def key_digest(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def stable_id(context: m.MonitoringContext, purpose: str) -> str:
    return str(uuid5(UUID(context.epoch), f"{context.tenant_id}:{purpose}"))


def canonical_incident_id(identity: m.IncidentIdentity) -> str:
    """Use this ID in controller notifications before finalization persists it."""
    return stable_id(identity.target, f"incident:{identity.key}")


def _stamp(context: m.MonitoringContext) -> dict[str, str]:
    return {"tenant_id": context.tenant_id, "epoch": context.epoch}


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _update(record: ModelT, **changes: object) -> ModelT:
    return type(record).model_validate({**record.model_dump(mode="json"), **changes})


def _utc(value: datetime) -> datetime:
    return TypeAdapter(m.UtcDateTime).validate_python(value)


@dataclass(frozen=True)
class StoredRecord:
    kind: str
    key: str
    context: m.MonitoringContext
    payload: str
    version: int = 0
    status: str | None = None
    workload: str | None = None
    workspace_id: str | None = None
    item_id: str | None = None
    target_key: str | None = None
    parent_key: str | None = None
    work_kind: str | None = None
    generation_id: str | None = None
    due_at: datetime | None = None
    sequence_number: int | None = None


@dataclass(frozen=True)
class StoredReceipt:
    operation: str
    request_id: str
    fingerprint: str
    context: m.MonitoringContext
    payload: str
    recorded_at: datetime


class WorkLink(m.MonitoringModel):
    work_id: m.CanonicalId
    execution: m.SourceExecutionIdentity


class PositionJournal(m.MonitoringModel):
    """One broker sequence, independent of CloudEvents delivery deduplication."""

    partition: m.PartitionIdentity
    position: m.StreamPosition
    receipt_key: m.StateKey
    receipt_kind: Literal["identified", "unidentified"]


class FairCursor(m.MonitoringModel):
    after_workspace: str = ""


class ActionOwner(m.MonitoringModel):
    reservation_id: m.CanonicalId
    fence: m.PositiveRevision
    active: m.StrictBool


class RecordBackend(Protocol):
    fixture: bool

    @property
    def transaction_active(self) -> bool: ...

    def transaction(self, *, write: bool, operation: str, request_id: str) -> AbstractContextManager[None]: ...
    def operation_identity(self, operation: str, request_id: str) -> None: ...
    def now(self) -> datetime: ...
    def control(self) -> dict[str, object] | None: ...
    def write_control(self, value: m.DeploymentControl, expected_revision: int) -> None: ...
    def get(self, kind: str, key: str, context: m.MonitoringContext) -> StoredRecord | None: ...
    def put(self, record: StoredRecord) -> None: ...
    def scan(
        self, kind: str, context: m.MonitoringContext, *, limit: int,
        after: str | None = None, filters: dict[str, object] | None = None,
    ) -> list[StoredRecord]: ...
    def count(
        self, kind: str, context: m.MonitoringContext, *, filters: dict[str, object] | None = None,
    ) -> int: ...
    def change_counter(self, kind: str, context: m.MonitoringContext) -> int: ...
    def due(
        self, request: m.WorkClaimRequest, *, after_workspace: str,
    ) -> list[StoredRecord]: ...
    def get_receipt(
        self, operation: str, request_id: str, context: m.MonitoringContext,
    ) -> StoredReceipt | None: ...
    def put_receipt(self, receipt: StoredReceipt) -> None: ...
    def get_lease(self, context: m.MonitoringContext, key: str) -> m.LeaseToken | None: ...
    def acquire_lease(
        self, context: m.MonitoringContext, key: str, owner: str, seconds: int,
    ) -> m.LeaseToken | None: ...
    def renew_lease(self, request: m.LeaseRenewal) -> m.LeaseToken: ...
    def release_lease(self, lease: m.LeaseToken) -> None: ...
    def compare_exchange_partition_lease(
        self, partition: m.PartitionIdentity, expected: m.LeaseToken | None,
        *, owner_id: str | None, lease_seconds: int,
    ) -> m.LeaseToken | None: ...
    def approval(self, request_id: str) -> dict[str, object] | None: ...
    def consume_approval(self, request_id: str, fingerprint: str) -> bool: ...
    def incident(self, incident_id: str) -> str | None: ...
    def write_incident(self, incident: Incident, payload: str, prior_payload: str | None) -> None: ...
    def mark_processed(self, execution_key: str, at: datetime) -> None: ...


@dataclass
class InMemoryMonitoringState:
    """Explicit fixture state, shareable by independent offline store instances."""

    control_row: dict[str, object] | None = None
    records: dict[tuple[str, str, str, str], StoredRecord] = field(default_factory=dict)
    receipts: dict[tuple[str, str, str, str], StoredReceipt] = field(default_factory=dict)
    leases: dict[tuple[str, str, str], m.LeaseToken] = field(default_factory=dict)
    approvals: dict[str, dict[str, object]] = field(default_factory=dict)
    incidents: dict[str, str] = field(default_factory=dict)
    processed: dict[str, str] = field(default_factory=dict)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    @classmethod
    def empty(cls, control: m.DeploymentControl) -> InMemoryMonitoringState:
        return cls(control_row=control.model_dump(mode="json"))


def _record_id(kind: str, key: str, context: m.MonitoringContext) -> tuple[str, str, str, str]:
    return context.tenant_id, context.epoch, kind, key_digest(key)


def _matches(record: StoredRecord, filters: dict[str, object]) -> bool:
    for name, value in filters.items():
        if name == "sequence_min":
            if record.sequence_number is None or record.sequence_number < value:
                return False
        elif name == "sequence_max":
            if record.sequence_number is None or record.sequence_number > value:
                return False
        elif name == "due_before":
            if record.due_at is None or record.due_at > value:
                return False
        elif name == "status_in":
            if record.status not in value:
                return False
        elif getattr(record, name) != value:
            return False
    return True


def _reconciliation_workspace(record: StoredRecord, work: m.MonitoringWork) -> str:
    workspace = work.target.workspace_id if work.target is not None else None
    if record.workspace_id is not None and record.workspace_id != workspace:
        logger.error("Reconciliation workspace promotion contradicts its canonical target key_hash=%s", key_digest(record.key))
        raise MonitoringUnavailable("Reconciliation workspace promotion contradicts its canonical target")
    return workspace or ""


def _reconciliation_policy_revision(backend: RecordBackend, context: m.MonitoringContext) -> int:
    if not backend.transaction_active:
        logger.error("Reconciliation scheduling attempted outside its operation transaction")
        raise MonitoringUnavailable("Reconciliation scheduling requires its operation transaction")
    try:
        control = m.DeploymentControl.model_validate(backend.control())
    except ValidationError as exc:
        logger.error("Reconciliation scheduling has no valid authoritative deployment control")
        raise MonitoringUnavailable("Reconciliation scheduling requires authoritative deployment control") from exc
    if _stamp(control) != _stamp(context):
        logger.error("Reconciliation scheduling control belongs to another tenant or epoch")
        raise MonitoringUnavailable("Reconciliation scheduling control context changed")
    return control.revision


class MemoryBackend:
    fixture = True

    def __init__(
        self, state: InMemoryMonitoringState, clock: Callable[[], datetime],
        *, component: m.RuntimeComponent = "fixture",
    ) -> None:
        self.state = state
        self.clock = clock
        self.component = component
        self._local = threading.local()

    @property
    def transaction_active(self) -> bool:
        return bool(getattr(self._local, "active", False))

    @contextmanager
    def transaction(self, *, write: bool, operation: str, request_id: str) -> Iterator[None]:
        with self.state.lock:
            if getattr(self._local, "active", False):
                raise MonitoringConflict("Nested monitoring transactions are not supported")
            self._local.active = True
            self.operation_identity(operation, request_id)
            names = ("control_row", "records", "receipts", "leases", "approvals", "incidents", "processed")
            before = {
                name: deepcopy(getattr(self.state, name)) if name in {"control_row", "approvals"}
                else dict(getattr(self.state, name)) for name in names
            } if write else {}
            try:
                yield
            except BaseException:
                for name, value in before.items():
                    setattr(self.state, name, value)
                raise
            finally:
                self._local.active = False

    def operation_identity(self, operation: str, request_id: str) -> None:
        self._local.operation = operation
        self._local.request_id = request_id

    def now(self) -> datetime:
        return _utc(self.clock())

    def control(self) -> dict[str, object] | None:
        return deepcopy(self.state.control_row)

    def write_control(self, value: m.DeploymentControl, expected_revision: int) -> None:
        current = self.control()
        if current is None or current["revision"] != expected_revision:
            raise MonitoringConflict("Deployment revision changed")
        self.state.control_row = value.model_dump(mode="json")

    def get(self, kind: str, key: str, context: m.MonitoringContext) -> StoredRecord | None:
        found = self.state.records.get(_record_id(kind, key, context))
        if found is not None and found.key != key:
            raise MonitoringConflict("A record digest does not identify the expected full key")
        return found

    def put(self, record: StoredRecord) -> None:
        prior = self.get(record.kind, record.key, record.context)
        expected = prior.version + 1 if prior else 1
        if record.version != expected:
            raise MonitoringConflict("Record revision changed")
        self.state.records[_record_id(record.kind, record.key, record.context)] = record

    def scan(
        self, kind: str, context: m.MonitoringContext, *, limit: int,
        after: str | None = None, filters: dict[str, object] | None = None,
    ) -> list[StoredRecord]:
        return sorted(
            (
                record for (tenant, epoch, record_kind, digest), record in self.state.records.items()
                if (tenant, epoch, record_kind) == (context.tenant_id, context.epoch, kind)
                and (after is None or digest > after) and _matches(record, filters or {})
            ),
            key=lambda record: key_digest(record.key),
        )[:limit]

    def count(
        self, kind: str, context: m.MonitoringContext, *, filters: dict[str, object] | None = None,
    ) -> int:
        return sum(
            1 for (tenant, epoch, record_kind, _), record in self.state.records.items()
            if (tenant, epoch, record_kind) == (context.tenant_id, context.epoch, kind)
            and _matches(record, filters or {})
        )

    def change_counter(self, kind: str, context: m.MonitoringContext) -> int:
        return sum(
            record.version for (tenant, epoch, record_kind, _), record in self.state.records.items()
            if (tenant, epoch, record_kind) == (context.tenant_id, context.epoch, kind)
        )

    def due(self, request: m.WorkClaimRequest, *, after_workspace: str) -> list[StoredRecord]:
        now = self.now()
        rows = self.scan("work", request, limit=len(self.state.records))
        if self.component != "fixture":
            kinds = m.WORKER_WORK_KINDS if self.component == "worker" else m.CONTROLLER_WORK_KINDS
            rows = [row for row in rows if row.work_kind in kinds]
        current_policy = (
            _reconciliation_policy_revision(self, request)
            if self.component != "worker" and "reconcile_state" in request.kinds else None
        )
        decoded: dict[str, m.MonitoringWork] = {}

        def work(record: StoredRecord) -> m.MonitoringWork:
            if record.key not in decoded:
                try:
                    decoded[record.key] = m.MonitoringWork.model_validate_json(record.payload)
                except ValidationError as exc:
                    logger.error("Invalid persisted monitoring record kind=%s key_hash=%s", record.kind, key_digest(record.key))
                    raise MonitoringUnavailable("A persisted monitoring record is unreadable") from exc
            result = decoded[record.key]
            if current_policy is not None and result.policy_revision > current_policy:
                logger.error("Reconciliation work refers to an unpublished policy key_hash=%s", key_digest(record.key))
                raise MonitoringUnavailable("Reconciliation work refers to an unpublished policy")
            return result

        def priority(record: StoredRecord) -> int:
            return 0 if record.work_kind == "reconcile_state" and work(record).reconcile_producer == "web" else 1

        def obsolete(record: StoredRecord) -> bool:
            return (
                current_policy is not None and record.work_kind == "reconcile_state"
                and work(record).policy_revision < current_policy
            )

        def group(record: StoredRecord) -> tuple[str, str]:
            if record.work_kind == "reconcile_state":
                return _reconciliation_workspace(record, work(record)), "publication"
            return record.workspace_id or "", "ordinary"

        active = Counter(
            group(record) for record in rows
            if record.status in {"leased", "finalizing"} and record.due_at and record.due_at > now
        )
        grouped: dict[tuple[str, str], list[StoredRecord]] = {}
        for record in rows:
            if (
                record.work_kind in request.kinds
                and record.status in {"queued", "waiting", "leased", "finalizing"}
                and record.due_at is not None and record.due_at <= now
            ):
                workspace = group(record)
                if active[workspace] < request.per_workspace_limit:
                    grouped.setdefault(workspace, []).append(record)
        ranked = []
        for workspace_group, records in grouped.items():
            workspace, pool = workspace_group
            # Native reconciliation preserves its target in the protected payload
            # even when the optional promoted workspace column is absent.
            # Prefer current publication inside its existing share, without
            # promoting metadata ahead of ordinary verification/finalization.
            for rank, record in enumerate(sorted(records, key=lambda row: (priority(row), obsolete(row), row.due_at, key_digest(row.key)))):
                if rank < request.per_workspace_limit - active[workspace_group]:
                    ranked.append((priority(record), rank, workspace <= after_workspace, workspace, pool, record))
        return [row[5] for row in sorted(ranked, key=lambda row: row[:5])[:request.limit]]

    def get_receipt(
        self, operation: str, request_id: str, context: m.MonitoringContext,
    ) -> StoredReceipt | None:
        result = self.state.receipts.get(_record_id(operation, request_id, context))
        if result is not None and result.request_id != request_id:
            raise MonitoringConflict("Receipt digest does not match its full idempotency ID")
        return result

    def put_receipt(self, receipt: StoredReceipt) -> None:
        key = _record_id(receipt.operation, receipt.request_id, receipt.context)
        if key in self.state.receipts:
            raise MonitoringConflict("An idempotency receipt cannot be overwritten")
        self.state.receipts[key] = receipt

    def get_lease(self, context: m.MonitoringContext, key: str) -> m.LeaseToken | None:
        found = self.state.leases.get((context.tenant_id, context.epoch, key_digest(key)))
        if found is not None and found.resource_key != key:
            raise MonitoringConflict("Lease digest does not match the expected full key")
        return deepcopy(found)

    def acquire_lease(
        self, context: m.MonitoringContext, key: str, owner: str, seconds: int,
    ) -> m.LeaseToken | None:
        prior = self.get_lease(context, key)
        now = self.now()
        if prior is not None and prior.expires_at > now:
            return prior if prior.owner_id == owner else None
        result = m.LeaseToken(
            **_stamp(context), resource_key=key, owner_id=owner,
            fence=prior.fence + 1 if prior else 1, acquired_at=now,
            expires_at=now + timedelta(seconds=seconds),
        )
        self.state.leases[(context.tenant_id, context.epoch, key_digest(key))] = result
        return result

    def renew_lease(self, request: m.LeaseRenewal) -> m.LeaseToken:
        prior = self.get_lease(request.lease, request.lease.resource_key)
        if (
            prior is None or prior.owner_id != request.lease.owner_id
            or prior.fence != request.lease.fence or prior.expires_at <= self.now()
        ):
            raise MonitoringLeaseLost("The lease expired or changed owner/fence")
        result = _update(prior, expires_at=self.now() + timedelta(seconds=request.lease_seconds))
        self.state.leases[(prior.tenant_id, prior.epoch, key_digest(prior.resource_key))] = result
        return result

    def release_lease(self, lease: m.LeaseToken) -> None:
        prior = self.get_lease(lease, lease.resource_key)
        if prior is None or (prior.owner_id, prior.fence) != (lease.owner_id, lease.fence):
            raise MonitoringLeaseLost("Cannot release another owner's lease")
        # Keep its monotonic fence after release; a new owner never starts at one.
        expired = max(self.now(), prior.acquired_at + timedelta(microseconds=1))
        self.state.leases[(prior.tenant_id, prior.epoch, key_digest(prior.resource_key))] = _update(
            prior, expires_at=expired,
        )

    def compare_exchange_partition_lease(
        self, partition: m.PartitionIdentity, expected: m.LeaseToken | None,
        *, owner_id: str | None, lease_seconds: int,
    ) -> m.LeaseToken | None:
        prior = self.get_lease(partition, partition.key)
        if prior != expected:
            raise MonitoringLeaseLost("Partition lease changed before its conditional mutation")
        key = (partition.tenant_id, partition.epoch, key_digest(partition.key))
        now = self.now()
        if owner_id is None:
            if prior is None:
                raise MonitoringLeaseLost("An absent partition lease cannot be released")
            self.state.leases[key] = _update(
                prior, fence=prior.fence + 1,
                expires_at=max(now, prior.acquired_at + timedelta(microseconds=1)),
            )
            return None
        TypeAdapter(m.LeaseSeconds).validate_python(lease_seconds)
        result = m.LeaseToken(
            **_stamp(partition), resource_key=partition.key,
            owner_id=TypeAdapter(m.CanonicalId).validate_python(owner_id),
            fence=prior.fence + 1 if prior else 1,
            acquired_at=now, expires_at=now + timedelta(seconds=lease_seconds),
        )
        self.state.leases[key] = result
        return result

    def approval(self, request_id: str) -> dict[str, object] | None:
        return deepcopy(self.state.approvals.get(request_id))

    def consume_approval(self, request_id: str, fingerprint: str) -> bool:
        row = self.approval(request_id)
        if (
            row is None or row.get("fingerprint") != fingerprint or row.get("decision") != "approve"
            or row.get("consumed_at") or not row.get("responder") or not row.get("decided_at")
        ):
            return False
        try:
            expiry = TypeAdapter(m.UtcDateTime).validate_python(row["expires_at"])
        except (KeyError, ValidationError):
            return False
        if expiry <= self.now():
            return False
        row["consumed_at"] = self.now().isoformat()
        self.state.approvals[request_id] = row
        return True

    def incident(self, incident_id: str) -> str | None:
        return self.state.incidents.get(incident_id)

    def write_incident(self, incident: Incident, payload: str, prior_payload: str | None) -> None:
        if self.incident(incident.id) != prior_payload:
            raise MonitoringConflict("The incident changed before persistence")
        self.state.incidents[incident.id] = payload

    def mark_processed(self, execution_key: str, at: datetime) -> None:
        self.state.processed[key_digest(execution_key)] = at.isoformat()


def atomic(*, write: bool = False):
    def decorate(method: Callable[Concatenate[MonitoringEngine, P], ResultT]):
        @wraps(method)
        def invoke(self: MonitoringEngine, *args: P.args, **kwargs: P.kwargs) -> ResultT:
            self._authorize_operation(method.__name__, write=write)
            copied = tuple(
                type(value).model_validate_json(value.model_dump_json())
                if isinstance(value, BaseModel) else deepcopy(value) for value in args
            )
            copied_kwargs = {
                name: value if method.__name__ == "reconcile_work" and name == "connector_publisher" else (
                    type(value).model_validate_json(value.model_dump_json())
                    if isinstance(value, BaseModel) else deepcopy(value)
                ) for name, value in kwargs.items()
            }
            if self._connector_calls.get() is not None:
                self._check_connector_call(method.__name__, copied, copied_kwargs)
                return deepcopy(self._run_operation(method, write, copied, copied_kwargs))
            operation_id = "read"
            for value in (*copied, *copied_kwargs.values()):
                if isinstance(value, BaseModel):
                    for field_name in (
                        "idempotency_id", "request_id", "page_id", "finalization_id",
                        "work_id", "capability_id", "connector_id",
                    ):
                        candidate = getattr(value, field_name, None)
                        if candidate:
                            operation_id = str(candidate)
                            break
            with self._backend.transaction(
                write=write, operation=method.__name__, request_id=operation_id,
            ):
                result = self._run_operation(method, write, copied, copied_kwargs)
                return deepcopy(result)
        return invoke
    return decorate


class MonitoringEngine:
    def __init__(
        self, backend: RecordBackend, *, component: m.RuntimeComponent,
        policy: TriagePolicy | None = None,
        redactor: Callable[[str], str] = redact_text,
    ) -> None:
        self._backend = backend
        self._component = TypeAdapter(m.RuntimeComponent).validate_python(component)
        if component == "fixture" and not backend.fixture:
            raise MonitoringComponentDenied("Fixture authority requires an explicitly offline backend")
        self._policy = policy or TriagePolicy()
        self._redactor = redactor
        self._connector_publisher: ContextVar[ConnectorPublisher | None] = ContextVar(
            "monitoring_connector_publisher", default=None,
        )
        self._connector_calls: ContextVar[tuple[int, m.ConnectorPublicationContext] | None] = ContextVar(
            "monitoring_connector_calls", default=None,
        )

    @property
    def component(self) -> m.RuntimeComponent:
        return self._component

    def _authorize_operation(self, operation: str, *, write: bool) -> None:
        if self.component == "fixture" or not write:
            return
        allowed = {
            "worker": WORKER_OPERATIONS | SHARED_WORK_OPERATIONS,
            "web": WEB_OPERATIONS,
            "controller": CONTROLLER_OPERATIONS | SHARED_WORK_OPERATIONS,
        }[self.component]
        if operation not in allowed:
            raise MonitoringComponentDenied(f"{self.component} cannot perform {operation}")

    def _run_operation(self, method, write: bool, args: tuple, kwargs: dict):
        return method(self, *args, **kwargs)

    @contextmanager
    def _using_connector_publisher(self, publisher: ConnectorPublisher | None) -> Iterator[None]:
        if publisher is not None and (not callable(publisher) or inspect.iscoroutinefunction(publisher)):
            raise MonitoringConflict("Connector publication requires synchronous controller composition")
        token = self._connector_publisher.set(publisher)
        try:
            yield
        finally:
            self._connector_publisher.reset(token)

    def _check_connector_call(self, operation: str, args: tuple, kwargs: dict) -> None:
        scope = self._connector_calls.get()
        if (
            scope is None or scope[0] != threading.get_ident() or not self._backend.transaction_active
            or self.component != "controller" or operation not in CONNECTOR_PUBLICATION_OPERATIONS
        ):
            raise MonitoringComponentDenied("Connector composition cannot escape its synchronous reconciliation transaction")
        context = scope[1]
        if operation == "publish_connector":
            request = args[0] if args else kwargs.get("request")
            if not isinstance(request, m.ConnectorPublicationRequest) or (
                request.work_id != context.work.work_id or request.lease != context.work.lease
                or request.expected_work_revision != context.work.revision
                or request.expected != context.expected or request.connector_id != context.connector.connector_id
                or request.expected_frontier_revision != context.frontier.accepted_revision
            ):
                raise MonitoringLeaseLost("Connector publication must retain its current reconciliation scope")
        elif operation == "enqueue_work":
            work = args[0] if args else kwargs.get("work")
            if not isinstance(work, m.MonitoringWorkDraft) or (
                work.kind != "connector_reconcile" or work.connector_id != context.connector.connector_id
                or work.target is not None or work.execution is not None
                or _stamp(work) != _stamp(context.expected) or work.policy_revision != context.expected.revision
            ):
                raise MonitoringComponentDenied("Connector composition can enqueue only its own worker follow-up")
        elif operation == "get_operation_receipt":
            name = args[1] if len(args) > 1 else kwargs.get("operation")
            if name != "connector_publication":
                raise MonitoringComponentDenied("Connector composition reads only original controller publication receipts")

    def _publish_connector_context(
        self, context: m.ConnectorPublicationContext,
    ) -> m.ConnectorPublicationResult | None:
        if self.component != "controller" or not self._backend.transaction_active or self._connector_calls.get() is not None:
            raise MonitoringComponentDenied("Connector publication requires an active controller reconciliation transaction")
        publisher = self._connector_publisher.get()
        if publisher is None:
            from triage.monitoring.controller import publish_reconciliation_connector

            publisher = publish_reconciliation_connector
        token = self._connector_calls.set((threading.get_ident(), context))
        try:
            result = publisher(self, context)
            if inspect.iscoroutine(result):
                result.close()
                raise MonitoringConflict("Connector publication cannot await inside the reconciliation transaction")
            if result is not None and not isinstance(result, m.ConnectorPublicationResult):
                raise MonitoringUnavailable("Connector orchestration returned an invalid publication result")
            if context.phase == "binding" and result is None:
                raise MonitoringUnavailable("Physical binding requires its original controller publication result")
            return result
        finally:
            self._connector_calls.reset(token)

    def _authorize_work(self, kind: m.WorkKind) -> None:
        allowed = (
            m.WORKER_WORK_KINDS if self.component == "worker"
            else m.CONTROLLER_WORK_KINDS if self.component == "controller"
            else m.WORKER_WORK_KINDS | m.CONTROLLER_WORK_KINDS if self.component == "fixture"
            else frozenset()
        )
        if kind not in allowed:
            raise MonitoringComponentDenied(f"{self.component} cannot own {kind} work")

    def _now(self) -> datetime:
        return self._backend.now()

    def _control(self, context: m.MonitoringContext | None = None) -> m.DeploymentControl:
        raw = self._backend.control()
        if raw is None:
            raise MonitoringNotBootstrapped("Monitoring requires an explicit deployment bootstrap")
        if raw.get("schema_version") != m.MONITORING_SCHEMA_VERSION:
            raise MonitoringSchemaMismatch("Monitoring schema and runtime do not match")
        try:
            control = m.DeploymentControl.model_validate(raw)
        except ValidationError as exc:
            logger.error("Invalid monitoring deployment control; refusing shared-state operations")
            raise MonitoringUnavailable("Monitoring deployment control is unreadable") from exc
        if context is not None and _stamp(control) != _stamp(context):
            raise MonitoringConflict("Monitoring tenant or epoch changed")
        return control

    def _current(self, expected: m.RegistryVersion, *, intake: bool = False) -> m.DeploymentControl:
        control = self._control(expected)
        if control.revision != expected.revision:
            raise MonitoringConflict("Monitoring registry revision changed")
        if intake and control.maintenance:
            raise MonitoringConflict("Monitoring maintenance stops new intake")
        return control

    def _decode(self, record: StoredRecord, model: type[ModelT]) -> ModelT:
        try:
            return model.model_validate_json(record.payload)
        except ValidationError as exc:
            logger.error("Invalid persisted monitoring record kind=%s key_hash=%s", record.kind, key_digest(record.key))
            raise MonitoringUnavailable("A persisted monitoring record is unreadable") from exc

    def _get(
        self, kind: str, key: str, context: m.MonitoringContext, model: type[ModelT],
    ) -> ModelT | None:
        record = self._backend.get(kind, key, context)
        return self._decode(record, model) if record else None

    def _redacted(self, value: object) -> object:
        if isinstance(value, str):
            return self._redactor(value)
        if isinstance(value, list):
            return [self._redacted(child) for child in value]
        if isinstance(value, dict):
            result = {}
            for key, child in value.items():
                if isinstance(child, str):
                    contextual = f"{key}={child}"
                    result[key] = (
                        "[REDACTED]" if self._redactor(contextual) != contextual
                        and self._redactor(child) == child else self._redacted(child)
                    )
                else:
                    result[key] = self._redacted(child)
            return result
        return value

    def _persisted(self, value: ModelT) -> ModelT:
        raw = value.model_dump(mode="json")
        redacted = self._redacted(raw)
        if isinstance(value, m.SafetyReview) and redacted["parameters"] != raw["parameters"]:
            redacted.update(
                parameters=None, parameters_redacted=True,
                state="pending" if value.publication_status == "pending_validation" else "unverifiable",
                detail="Replay parameters were redacted at persistence; a new usable review is required.",
            )
        if isinstance(value, Incident) and redacted != raw:
            redacted["redaction_applied"] = True
            redacted["redaction_kinds"] = sorted(set(value.redaction_kinds) | {"monitoring_boundary"})
        try:
            return type(value).model_validate(redacted)
        except ValidationError as exc:
            logger.error("Redaction made record type=%s unusable; refusing persistence", type(value).__name__)
            raise MonitoringUnavailable("Redacted monitoring state cannot be safely represented") from exc

    def _put(
        self, kind: str, key: str, context: m.MonitoringContext, value: ModelT,
        **indices: object,
    ) -> ModelT:
        persisted = self._persisted(value)
        prior = self._backend.get(kind, key, context)
        self._backend.put(StoredRecord(
            kind=kind, key=key, context=m.MonitoringContext(**_stamp(context)),
            # Kernel JSON_QUERY identity comparisons use the same canonical
            # property order as named RPC JSON arguments.
            payload=_json(persisted.model_dump(mode="json")), version=prior.version + 1 if prior else 1,
            **indices,
        ))
        return persisted

    def _all(
        self, kind: str, context: m.MonitoringContext, model: type[ModelT],
        *, filters: dict[str, object] | None = None, budget: int = SCAN_BUDGET,
    ) -> list[ModelT]:
        result = []
        after = None
        while True:
            rows = self._backend.scan(kind, context, limit=min(1_000, budget + 1 - len(result)), after=after, filters=filters)
            result.extend(self._decode(row, model) for row in rows)
            if len(result) > budget:
                raise MonitoringConflict(f"{kind} exceeds this operation's {budget}-record budget; narrow the scope")
            if len(rows) < min(1_000, budget + 1 - (len(result) - len(rows))):
                return result
            after = key_digest(rows[-1].key)

    def _idempotent(
        self, operation: str, request_id: str, context: m.MonitoringContext,
        request: BaseModel | dict[str, object], model: type[ModelT], apply: Callable[[], ModelT],
    ) -> ModelT:
        self._backend.operation_identity(operation, request_id)
        self._control(context)
        raw = request.model_dump(mode="json") if isinstance(request, BaseModel) else request
        fingerprint = key_digest(_json(raw))
        prior = self._backend.get_receipt(operation, request_id, context)
        if prior is not None:
            if prior.fingerprint != fingerprint:
                raise MonitoringConflict("Idempotency ID was reused for different validated content")
            return model.model_validate_json(prior.payload)
        result = self._persisted(apply())
        self._backend.operation_identity(operation, request_id)
        self._backend.put_receipt(StoredReceipt(
            operation=operation, request_id=request_id, fingerprint=fingerprint,
            context=m.MonitoringContext(**_stamp(context)), payload=result.model_dump_json(),
            recorded_at=self._now(),
        ))
        return result

    def _receipt(
        self, operation: str, request_id: str, context: m.MonitoringContext, model: type[ModelT],
    ) -> ModelT | None:
        self._control(context)
        receipt = self._backend.get_receipt(operation, request_id, context)
        return model.model_validate_json(receipt.payload) if receipt else None

    def _evidence_binding(self, kind: str, key: str, context: m.MonitoringContext) -> m.EvidenceBinding:
        record = self._backend.get(kind, key, context)
        if record is None:
            raise MonitoringUnavailable("Accepted evidence must exist before its immutable handoff")
        return m.EvidenceBinding(
            kind=kind, key=key, revision=record.version,
            payload_hash=hashlib.sha256(record.payload.encode("utf-16-le")).hexdigest(),
        )

    def _request_reconciliation(
        self, control: m.DeploymentControl, *, request_id: str, topic: str,
        reference_id: str, fingerprint: str, payload: dict[str, object],
        target: m.TargetIdentity | None = None, window: m.ObservationWindow | None = None,
        evidence: tuple[m.EvidenceBinding, ...] = (),
        producer_commit: m.CollectionCommit | None = None,
    ) -> m.MonitoringWork:
        producer = "web" if topic in {"scope", "review", "discovery"} else "worker"
        kind = f"{producer}_reconcile_request"
        existing = self._get(kind, request_id, control, m.ReconciliationRequest)
        if existing is not None:
            if existing.fingerprint != fingerprint:
                raise MonitoringConflict("A producer handoff cannot change its original request")
            work = self._get("work", existing.work_id, control, m.MonitoringWork)
            if work is None:
                raise MonitoringUnavailable("An accepted producer request has no initial reconciliation work")
            return work
        work_id = stable_id(control, f"reconcile:{producer}:{request_id}")
        if self._backend.get("work", work_id, control) is not None:
            raise MonitoringConflict("Producer handoffs cannot mutate existing controller work")
        frontier_key = f"validation:v1:{control.epoch}:{control.tenant_id}:{topic}:{reference_id}"
        prior = self._get("validation_frontier", frontier_key, control, m.ValidationFrontier)
        if prior is not None and topic in {"inventory", "rest_page"} and self._get(
            "window_rejection", frontier_key, control, m.ReconciliationResult,
        ) is not None:
            raise MonitoringConflict("A rejected collection window cannot accept another page")
        frontier = m.ValidationFrontier(
            **_stamp(control), frontier_key=frontier_key, target=target, window=window,
            accepted_revision=prior.accepted_revision + 1 if prior else 1,
            validated_revision=prior.validated_revision if prior else 0,
            latest_request_id=request_id, updated_at=self._now(),
        )
        request = m.ReconciliationRequest(
            **_stamp(control), request_id=request_id, producer=producer, topic=topic,
            reference_id=reference_id, fingerprint=fingerprint, policy_revision=control.revision,
            work_id=work_id, target=target, window=window, frontier_key=frontier_key,
            producer_commit=producer_commit,
            frontier_revision=frontier.accepted_revision, created_at=self._now(),
            evidence=evidence, request_payload=payload,
        )
        self._put(
            "validation_frontier", frontier_key, control, frontier,
            target_key=target.key if target else None, status="pending",
        )
        self._put(kind, request_id, control, request, target_key=target.key if target else None)
        return self._save_work(m.MonitoringWork(
            **_stamp(control), work_id=work_id, kind="reconcile_state",
            policy_revision=control.revision, created_at=self._now(), due_at=self._now(),
            reason="Publish or reject immutable producer evidence without an agent or workload action.",
            target=target, reconcile_request_id=request_id, reconcile_producer=producer,
            revision=1, state="queued",
        ))

    def _pending_validation(self, identity: m.TargetIdentity) -> bool:
        return any(self._backend.count(
            "validation_frontier", identity, filters={"status": "pending", "target_key": key},
        ) for key in (identity.key, None))

    def _pending_frontier_count(self, context: m.MonitoringContext) -> int:
        return self._backend.count("validation_frontier", context, filters={"status": "pending"})

    @atomic()
    def get_reconciliation_request(
        self, context: m.MonitoringContext, request_id: str, *, producer: str,
    ) -> m.ReconciliationRequest | None:
        self._control(context)
        producer = TypeAdapter(m.ProducerComponent).validate_python(producer)
        return self._get(
            f"{producer}_reconcile_request", m.canonical_id(request_id), context, m.ReconciliationRequest,
        )

    @atomic()
    def get_validation_frontier(
        self, context: m.MonitoringContext, frontier_key: str,
    ) -> m.ValidationFrontier | None:
        self._control(context)
        return self._get(
            "validation_frontier", TypeAdapter(m.StateKey).validate_python(frontier_key),
            context, m.ValidationFrontier,
        )

    def _reconcile_request(self, work: m.MonitoringWork) -> m.ReconciliationRequest:
        if work.kind != "reconcile_state":
            raise MonitoringConflict("Deterministic publication accepts only reconcile_state work")
        request = self._get(
            f"{work.reconcile_producer}_reconcile_request", work.reconcile_request_id,
            work, m.ReconciliationRequest,
        )
        if request is None or request.work_id != work.work_id or request.target != work.target:
            raise MonitoringUnavailable("Reconciliation lost its immutable producer binding")
        return request

    @atomic(write=True)
    def reconcile_work(
        self, work: m.MonitoringWork, *, connector_publisher: ConnectorPublisher | None = None,
    ) -> m.ReconciliationResult:
        with self._using_connector_publisher(connector_publisher):
            return self._reconcile_claimed_work(work)

    def _reconcile_claimed_work(self, work: m.MonitoringWork) -> m.ReconciliationResult:
        producer = self._reconcile_request(work)
        control = self._control(work)
        frontier = self._get("validation_frontier", producer.frontier_key, work, m.ValidationFrontier)
        if frontier is None:
            raise MonitoringUnavailable("Accepted producer evidence has no protected validation frontier")
        operation_id = stable_id(work, f"publication:{work.work_id}:{work.lease.fence if work.lease else 0}")
        prior = self._receipt("reconciliation", operation_id, work, m.ReconciliationResult)
        if prior is not None:
            return prior
        if work.lease is None:
            raise MonitoringLeaseLost("Reconciliation requires its claimed work lease")
        return self._reconcile_state(m.ReconcileStateRequest(
            **_stamp(work), request_id=operation_id, work_id=work.work_id, lease=work.lease,
            expected_work_revision=work.revision, expected_policy_revision=control.revision,
            expected_frontier_revision=frontier.accepted_revision,
        ))

    @atomic(write=True)
    def reconcile_state(self, request: m.ReconcileStateRequest) -> m.ReconciliationResult:
        return self._reconcile_state(request)

    def _reconcile_state(self, request: m.ReconcileStateRequest) -> m.ReconciliationResult:
        def apply() -> m.ReconciliationResult:
            control = self._control(request)
            work = self._owned_work(request, request.work_id, request.lease, request.expected_work_revision)
            producer = self._reconcile_request(work)
            frontier = self._get("validation_frontier", producer.frontier_key, request, m.ValidationFrontier)
            if frontier is None:
                raise MonitoringUnavailable("Reconciliation has no protected accepted frontier")
            if (
                control.revision != request.expected_policy_revision
                or frontier.accepted_revision != request.expected_frontier_revision
                or producer.frontier_revision > frontier.accepted_revision
            ):
                raise MonitoringConflict("Publication configuration or accepted-evidence frontier changed")
            resolution = self._get("window_resolution", frontier.frontier_key, request, m.ReconciliationResult)
            if resolution is not None:
                if request.reject_whole_window:
                    raise MonitoringConflict("A completed window resolution cannot be replaced by another rejection")
                original = self._receipt("reconciliation", resolution.request_id, request, m.ReconciliationResult)
                if (
                    original != resolution or _stamp(resolution) != _stamp(control)
                    or resolution.state not in {"published", "rejected"}
                    or resolution.resolution_scope == "window_acknowledgement"
                    or resolution.state == "rejected" and resolution.resolution_scope != "window"
                    or resolution.frontier_key != frontier.frontier_key
                    or resolution.frontier_revision != frontier.accepted_revision or frontier.pending
                ):
                    raise MonitoringUnavailable("Window acknowledgement lost its exact original terminal receipt")
                result = m.ReconciliationResult(
                    **_stamp(control), request_id=request.request_id, work_id=work.work_id,
                    producer_request_id=producer.request_id, policy_revision=control.revision,
                    frontier_key=frontier.frontier_key, frontier_revision=frontier.accepted_revision,
                    state=resolution.state, detail="Acknowledged the original protected terminal window.",
                    published_at=self._now(), resolution_scope="window_acknowledgement",
                    window_rejection_request_id=resolution.request_id if resolution.state == "rejected" else None,
                    window_resolution_request_id=resolution.request_id, window_resolution_state=resolution.state,
                )
                self._finish_reconciliation(work, result)
                return result
            if request.reject_whole_window or (
                producer.policy_revision != control.revision and producer.topic in {"inventory", "rest_page"}
                and frontier.pending
            ):
                return self._reject_reconciliation_window(request, work, producer, frontier, control)
            current = True
            for binding in producer.evidence:
                stored = self._backend.get(binding.kind, binding.key, request)
                if (
                    stored is None or stored.version != binding.revision
                    or hashlib.sha256(stored.payload.encode("utf-16-le")).hexdigest() != binding.payload_hash
                ):
                    current = False
                    break
            if control.maintenance or producer.policy_revision != control.revision or not current:
                state, detail = "rejected", "Accepted evidence was superseded, changed policy, or met deployment maintenance."
            else:
                state, detail = self._publish_reconciliation(producer, control)
            result = m.ReconciliationResult(
                **_stamp(control), request_id=request.request_id, work_id=work.work_id,
                producer_request_id=producer.request_id, policy_revision=control.revision,
                frontier_key=frontier.frontier_key, frontier_revision=producer.frontier_revision,
                state=state, detail=detail, published_at=self._now(),
            )
            self._put(
                "reconcile_acceptance", producer.request_id, control, result,
                parent_key=frontier.frontier_key, status=state, sequence_number=producer.frontier_revision,
            )
            validated = frontier.validated_revision
            if state != "pending_validation":
                if producer.topic != "stream_intake" and current and producer.frontier_revision == frontier.accepted_revision:
                    validated = frontier.accepted_revision
                else:
                    terminal = self._all(
                        "reconcile_acceptance", control, m.ReconciliationResult,
                        filters={"parent_key": frontier.frontier_key, "status_in": ("published", "rejected")},
                    )
                    completed = {item.frontier_revision for item in terminal}
                    while validated + 1 in completed:
                        validated += 1
                self._put(
                    "validation_frontier", frontier.frontier_key, control,
                    _update(frontier, validated_revision=validated, updated_at=self._now()),
                    target_key=frontier.target.key if frontier.target else None,
                    status="pending" if validated < frontier.accepted_revision else "validated",
                )
                if validated == frontier.accepted_revision and producer.topic in {"inventory", "rest_page"}:
                    self._put("window_resolution", frontier.frontier_key, control, result, parent_key=frontier.frontier_key)
            self._finish_reconciliation(work, result)
            return result
        return self._idempotent(
            "reconciliation", request.request_id, request, request, m.ReconciliationResult, apply,
        )

    def _finish_reconciliation(self, work: m.MonitoringWork, result: m.ReconciliationResult) -> None:
        self._backend.release_lease(work.lease)
        pending = result.state == "pending_validation"
        self._save_work(_update(
            work, revision=work.revision + 1, state="waiting" if pending else "completed", lease=None,
            completed_at=None if pending else self._now(), disposition=result.detail,
            due_at=self._now() + timedelta(seconds=15) if pending else work.due_at,
        ))

    def _reject_reconciliation_window(self, request, work, producer, frontier, control):
        if producer.topic not in {"inventory", "rest_page"} or not frontier.pending:
            raise MonitoringConflict("Whole-window rejection requires an unfinished collection window")
        entries = [
            entry for entry in self._all("worker_reconcile_request", control, m.ReconciliationRequest)
            if entry.frontier_key == frontier.frontier_key
        ]
        if {entry.frontier_revision for entry in entries} != set(range(1, frontier.accepted_revision + 1)):
            raise MonitoringUnavailable("The accepted window prefix has a missing producer handoff")
        for entry in entries:
            receipt = self._backend.get_receipt(entry.topic, entry.request_id, control)
            if receipt is None or receipt.fingerprint != entry.fingerprint:
                raise MonitoringUnavailable("The original accepted window receipt prefix is incomplete")
        result = m.ReconciliationResult(
            **_stamp(control), request_id=request.request_id, work_id=work.work_id,
            producer_request_id=producer.request_id, policy_revision=control.revision,
            frontier_key=frontier.frontier_key, frontier_revision=frontier.accepted_revision,
            state="rejected", resolution_scope="window",
            detail=request.detail or "The unfinished window was rejected under the current policy.",
            published_at=self._now(),
        )
        self._put("window_resolution", frontier.frontier_key, control, result, parent_key=frontier.frontier_key)
        self._put("validation_frontier", frontier.frontier_key, control, _update(
            frontier, validated_revision=frontier.accepted_revision, updated_at=self._now(),
        ), status="rejected", target_key=frontier.target.key if frontier.target else None)
        self._finish_reconciliation(work, result)
        return result

    def _publish_reconciliation(
        self, request: m.ReconciliationRequest, control: m.DeploymentControl,
    ) -> tuple[str, str]:
        payload = request.request_payload
        if request.topic == "inventory":
            generation = self._get("generation", payload["generation_id"], control, m.InventoryGeneration)
            if generation is None:
                raise MonitoringUnavailable("Accepted inventory generation is absent")
            self._publish_inventory(generation, control)
            if generation.completed_at is None:
                return "pending_validation", "The inventory window remains unfinished and fenced."
        elif request.topic == "capability":
            observation = self._get("capability", payload["capability_id"], control, m.CapabilityObservation)
            if observation is None:
                raise MonitoringUnavailable("Accepted capability evidence is absent")
            self._publish_capability(observation, control)
        elif request.topic == "scope":
            plan = self._get("plan", payload["plan_id"], control, m.ActivationPlan)
            if plan is None:
                raise MonitoringUnavailable("Accepted scope intent lost its original dry-run plan")
            self._publish_scope(plan, control)
        elif request.topic == "review":
            review = self._get("review_request", payload["review_id"], control, m.SafetyReview)
            if review is None:
                raise MonitoringUnavailable("Accepted safety intent is absent")
            self._publish_review(review, control)
        elif request.topic == "discovery":
            selector = m.ScopeSelector.model_validate(payload["selector"])
            self._enqueue(m.MonitoringWorkDraft(
                **_stamp(control), work_id=stable_id(control, f"discovery:{request.request_id}"),
                kind="inventory", discovery_selector=selector, policy_revision=control.revision,
                created_at=self._now(), due_at=self._now(), reason="Controller-published explicit discovery intent.",
            ))
        elif request.topic == "connector":
            connector = self._get("connector", payload["connector_id"], control, m.OwnedConnectorManifest)
            if connector is None:
                raise MonitoringUnavailable("Accepted connector observation is absent")
            self._reconcile_connector_observation(request, connector, control)
        elif request.topic == "rest_page":
            return self._publish_rest_window(request, control)
        elif request.topic == "stream_intake":
            if payload.get("unidentified_keys") and not payload.get("receipt_keys"):
                return "rejected", "Malformed transport evidence remains durably quarantined without a source identity."
            for key in payload.get("receipt_keys", ()):
                signal = self._get("signal", key, control, m.SignalReceipt)
                if signal is None:
                    raise MonitoringUnavailable("Accepted broker position lost its original signal")
                if signal.status == "accepted":
                    self._ingest(signal.observation, control)
                    if signal.transport is not None:
                        connector = self._get(
                            "connector", signal.delivery.connector_id, control, m.OwnedConnectorManifest,
                        )
                        if connector is not None and connector.state != "ready" and self._connector_delivery(
                            control, connector.connector_id, signal.transport.collector_identity_id,
                        ) is not None:
                            self._connector_followup(connector, control)
        else:
            raise MonitoringConflict("Unknown deterministic reconciliation topic")
        if request.topic in {"inventory", "capability", "scope", "discovery"}:
            self._reconcile_registered_connector_intent(request, control)
        return "published", "Accepted producer state was validated and published by the controller."

    def _registered_connectors(self, control):
        return tuple(
            connector for connector in self._all("connector", control, m.OwnedConnectorManifest)
            if connector.state not in {"deleting", "deleted"}
            and connector.workspace_id is not None and connector.eventstream_id is not None
            and connector.destination_id is not None and connector.endpoint is not None
            and set(connector.desired_definition) == {"parts", "component_ids"}
            and isinstance(connector.desired_definition["parts"], dict)
            and "eventstream.json" in connector.desired_definition["parts"]
        )

    def _reconcile_registered_connector_intent(self, producer, control):
        if self.component != "controller":
            return
        configured = self._registered_connectors(control)
        if len(configured) != 1:
            # Assignment of another physical shard is an operator decision, not
            # selection by list order or a fabricated destination.
            if configured:
                logger.warning("connector_assignment_requires_review count=%d", len(configured))
            return
        connector = configured[0]
        other_targets = {
            source.target.key
            for other in self._all("connector", control, m.OwnedConnectorManifest)
            if other.connector_id != connector.connector_id and other.state != "deleted"
            for source in (*other.sources, *other.source_proposals)
        }
        targets = []
        narrowed = False
        for candidate in self._all("target", control, m.MonitoringTarget):
            if candidate.identity.workload != "fabric_pipeline" or candidate.key in other_targets:
                continue
            current = self._target(candidate.identity)
            if current is None or current.state != "current" or not current.observation.enabled:
                continue
            capability = self._get("target_capability", current.key, control, m.CapabilityObservation)
            if (
                capability is None or capability.read_status != "verified"
                or capability.event_status != "verified" or capability.expires_at <= self._now()
            ):
                logger.warning("connector_capability_not_published target_key=%s", current.key)
                if any(
                    source.target == current.identity
                    for source in (*connector.sources, *connector.source_proposals)
                ):
                    return
                narrowed = True
                continue
            targets.append(current)
        if len(targets) > 100:
            raise MonitoringKernelUnsupported("Additional connector shards require explicit approved metadata")
        desired = self._get("connector_desired", connector.connector_id, control, m.ConnectorDesiredState)
        if desired is None and (
            not targets or not {
                source.target.key for source in (*connector.sources, *connector.source_proposals)
            }.issubset({target.key for target in targets})
        ):
            # Registration retains physical ownership, not an empty desired
            # intake policy. Later revocation still publishes removals normally.
            logger.info("registered_connector_dormant connector_id=%s", connector.connector_id)
            return
        work = self._get("work", producer.work_id, control, m.MonitoringWork)
        frontier = self._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
        if work is None or work.lease is None or frontier is None:
            raise MonitoringLeaseLost("Connector planning lost its current reconciliation ownership")
        self._publish_connector_context(m.ConnectorPublicationContext(
            phase="desired", expected=m.RegistryVersion(**_stamp(control), revision=control.revision),
            work=work, frontier=frontier, connector=connector,
            eligible_targets=tuple(targets) if narrowed else None,
            request_id=stable_id(
                control, f"connector-plan:{work.work_id}:{work.lease.fence}:{connector.connector_id}",
            ),
        ))

    def _publish_rest_window(
        self, producer: m.ReconciliationRequest, control: m.DeploymentControl,
    ) -> tuple[str, str]:
        request = m.RestPageRequest.model_validate(producer.request_payload)
        if request.next_cursor is not None:
            if request.target.workload == "powerbi":
                self._stage_powerbi_page(request, control, stage_rows=False)
            return "pending_validation", "The partial source window remains fenced until terminal controller validation."
        target = self._target(request.target)
        if target is None:
            return "rejected", "The raw source window no longer belongs to an admitted target."
        quarantines = self._backend.count("quarantine", control, filters={"parent_key": producer.reference_id})
        if not request.window_complete or request.retention_exhausted:
            self._save_target(_update(
                target, action=_update(target.action, enabled=False),
                reason="Complete source history must be controller-validated before another action.",
            ))
            self._schedule_poll(target)
            return "rejected", "Incomplete terminal source history was rejected; no source or action authority was published."
        staged = None
        if request.target.workload == "powerbi":
            staged, _, _ = self._stage_powerbi_page(request, control, stage_rows=False)
        elif not quarantines:
            for observation in self._all(
                "rest_observation", control, m.SourceRunObservation, filters={"parent_key": producer.reference_id},
            ):
                self._ingest(observation, control)
        prior = self._get("rest_checkpoint", request.target.key, control, m.RestCheckpoint)
        coverage = prior.coverage_through if prior else None
        valid = not quarantines and (staged is None or staged.state == "validated")
        if valid:
            if coverage is not None and request.window.start_at > coverage:
                raise MonitoringConflict("Validated REST coverage cannot skip an unobserved window")
            coverage = max(coverage, request.window.end_at) if coverage else request.window.end_at
            if target.action.review_id is not None and target.admission_basis == "reviewed":
                review = self._get("review", target.action.review_id, control, m.SafetyReview)
                capability = self._get("target_capability", target.key, control, m.CapabilityObservation)
                if self._review_current(review, capability, control):
                    target = self._save_target(_update(
                        target, action=_update(target.action, enabled=True, review_revision=review.revision),
                    ))
        checkpoint = m.RestCheckpoint(
            target=request.target, revision=(prior.revision if prior else 0) + 1, window=request.window,
            coverage_through=coverage, last_page_id=request.page_id, updated_at=self._now(),
            powerbi_window_id=staged.window_id if staged else None,
        )
        self._put("rest_checkpoint", request.target.key, control, checkpoint, target_key=request.target.key)
        self._schedule_poll(target)
        return (
            ("published", "The complete source window and its exact identities were controller-validated.")
            if valid else ("rejected", "The source window contains quarantined identities; no source work was admitted.")
        )

    @atomic()
    def get_operation_receipt(
        self, context: m.MonitoringContext, operation: str, request_id: str,
    ) -> m.OperationReceipt | None:
        self._control(context)
        operation = TypeAdapter(m.OpaqueId).validate_python(operation)
        request_id = TypeAdapter(m.OpaqueId).validate_python(request_id)
        receipt = self._backend.get_receipt(
            "connector" if operation == "worker.observe_connector" else operation, request_id, context,
        )
        if receipt is None:
            return None
        return m.OperationReceipt(
            **_stamp(context), operation=operation, request_id=receipt.request_id,
            fingerprint=receipt.fingerprint, recorded_at=receipt.recorded_at,
            result=(
                self._connector_observation_result(context, request_id).model_dump(mode="json")
                if operation == "worker.observe_connector" else json.loads(receipt.payload)
            ),
        )

    def _page(
        self, kind: str, query: m.PageQuery, model: type[ModelT],
        filters: dict[str, object] | None = None, *, snapshot_revision: int | None = None,
    ) -> m.RecordPage[ModelT]:
        control = self._control(query)
        token_base = {
            **_stamp(query), "kind": kind, "revision": control.revision,
            "filters": json.loads(_json(filters or {})),
            "data_revision": (
                self._backend.change_counter(kind, query) if snapshot_revision is None else snapshot_revision
            ),
        }
        after = None
        if query.cursor is not None:
            try:
                token = json.loads(base64.urlsafe_b64decode(query.cursor.encode("ascii")))
                if not isinstance(token, dict):
                    raise ValueError("A monitoring cursor must be an object")
                after = token.pop("after")
                if token != token_base or not isinstance(after, str) or len(bytes.fromhex(after)) != 32:
                    raise ValueError("Cursor does not match the current query/revision")
            except (ValueError, KeyError, UnicodeError) as exc:
                raise MonitoringConflict("Invalid or stale monitoring continuation") from exc
        rows = self._backend.scan(kind, query, limit=query.limit + 1, after=after, filters=filters)
        cursor = None
        if len(rows) > query.limit:
            cursor = base64.urlsafe_b64encode(_json({
                **token_base, "after": key_digest(rows[query.limit - 1].key),
            }).encode("utf-8")).decode("ascii")
        return m.RecordPage[model](
            version=m.RegistryVersion(**_stamp(control), revision=control.revision),
            as_of=self._now(), items=tuple(self._decode(row, model) for row in rows[:query.limit]),
            next_cursor=cursor,
        )

    @atomic()
    def inspect_bootstrap(self, *, expected_tenant_id: str) -> m.BootstrapInspection:
        tenant = TypeAdapter(m.CanonicalId).validate_python(expected_tenant_id)
        raw = self._backend.control()
        if raw is None:
            return m.BootstrapInspection(status="missing", expected_tenant_id=tenant, detail="Deployment bootstrap is required.")
        if raw.get("schema_version") != m.MONITORING_SCHEMA_VERSION:
            return m.BootstrapInspection(
                status="incompatible", expected_tenant_id=tenant,
                found_schema_version=raw.get("schema_version"), detail="The deployed schema is incompatible.",
            )
        control = self._control()
        status = "wrong_tenant" if control.tenant_id != tenant else "maintenance" if control.maintenance else "ready"
        return m.BootstrapInspection(
            status=status, expected_tenant_id=tenant, found_schema_version=control.schema_version,
            control=control, detail=f"Monitoring bootstrap state: {status}.",
        )

    @atomic()
    def list_scopes(self, query: m.PageQuery) -> m.RecordPage[m.ScopePolicy]:
        return self._page("scope", query, m.ScopePolicy)

    @atomic()
    def list_inventory(
        self, query: m.TargetQuery, *, generation_id: str | None = None,
    ) -> m.RecordPage[m.InventoryItem]:
        filters = {}
        if query.workspace_id is not None:
            filters["workspace_id"] = query.workspace_id
        if query.workload is not None:
            filters["workload"] = query.workload
        if generation_id is not None:
            return self._generation_page("inventory_seen", query, m.InventoryItem, generation_id, filters)
        return self._page("inventory", query, m.InventoryItem, filters)

    @atomic()
    def list_workspaces(
        self, query: m.PageQuery, *, generation_id: str | None = None,
    ) -> m.RecordPage[m.InventoryWorkspace]:
        if generation_id is not None:
            return self._generation_page("workspace_seen", query, m.InventoryWorkspace, generation_id)
        return self._page("workspace", query, m.InventoryWorkspace)

    @atomic()
    def list_domains(
        self, query: m.PageQuery, *, generation_id: str | None = None,
    ) -> m.RecordPage[m.InventoryDomain]:
        if generation_id is not None:
            return self._generation_page("domain_seen", query, m.InventoryDomain, generation_id)
        return self._page("domain", query, m.InventoryDomain)

    def _generation_page(
        self, kind: str, query: m.PageQuery, model: type[ModelT], generation_id: str,
        filters: dict[str, object] | None = None,
    ) -> m.RecordPage[ModelT]:
        self._control(query)
        generation_id = m.canonical_id(generation_id)
        generation = self._get("generation", generation_id, query, m.InventoryGeneration)
        if generation is None:
            raise MonitoringConflict("The requested inventory generation does not exist")
        return self._page(
            kind, query, model, {**(filters or {}), "parent_key": generation_id},
            snapshot_revision=generation.revision,
        )

    @atomic()
    def get_inventory_generation(self, context: m.MonitoringContext, generation_id: str) -> m.InventoryGeneration | None:
        self._control(context)
        return self._get("generation", m.canonical_id(generation_id), context, m.InventoryGeneration)

    @atomic()
    def list_connectors(self, query: m.PageQuery) -> m.RecordPage[m.OwnedConnectorManifest]:
        return self._page("connector", query, m.OwnedConnectorManifest)

    def _target(self, identity: m.TargetIdentity, *, include_inactive: bool = False) -> m.MonitoringTarget | None:
        control = self._control(identity)
        target = self._get("target", identity.key, identity, m.MonitoringTarget)
        if target is None or (not include_inactive and target.state != "current"):
            return None
        if target.identity != identity:
            raise MonitoringConflict("Stored target identity disagrees with its key")
        if not include_inactive and (control.maintenance or target.policy_revision != control.revision):
            return None
        if not include_inactive:
            item = self._get("inventory", f"{identity.workspace_id}:{identity.item_id}", identity, m.InventoryItem)
            if item is None or item.state != "present":
                return None
            workspace = self._get("workspace", identity.workspace_id, identity, m.InventoryWorkspace)
            if workspace is not None and workspace.state != "present":
                return None
            policies = self._all("scope", identity, m.ScopePolicy, budget=1_000)
            matches, excluded = self._scope_matches(policies, item)
            if not matches or excluded:
                return None
            capability = self._get("target_capability", target.key, identity, m.CapabilityObservation)
            if capability is None or capability.read_status != "verified" or capability.expires_at <= self._now():
                return None
            if target.action.enabled:
                review = self._get("review", target.action.review_id, identity, m.SafetyReview)
                if not self._review_current(review, capability, control) or self._pending_validation(identity):
                    target = _update(target, action=_update(target.action, enabled=False))
            schedule = self._get("poll_schedule", target.key, identity, m.PollSchedule)
            if schedule is not None:
                if schedule.target != identity:
                    raise MonitoringUnavailable("Poll scheduling metadata belongs to another target")
                if schedule.policy_revision == control.revision:
                    target = _update(target, next_poll_at=schedule.next_poll_at)
        return target

    @atomic()
    def resolve_target(self, identity: m.TargetIdentity, *, include_inactive: bool = False) -> m.MonitoringTarget | None:
        if type(include_inactive) is not bool:
            raise ValueError("include_inactive must be a boolean")
        return self._target(identity, include_inactive=include_inactive)

    @atomic()
    def list_targets(self, query: m.TargetQuery) -> m.RecordPage[m.MonitoringTarget]:
        filters = {}
        if query.workspace_id is not None:
            filters["workspace_id"] = query.workspace_id
        if query.workload is not None:
            filters["workload"] = query.workload
        if not query.include_inactive:
            filters["status"] = "current"
        page = self._page("target", query, m.MonitoringTarget, filters)
        current = tuple(self._effective_target(target) for target in page.items)
        return _update(page, items=current)

    def _effective_target(self, target: m.MonitoringTarget) -> m.MonitoringTarget:
        if target.state != "current":
            return target
        effective = self._target(target.identity)
        if effective is None:
            return _update(
                target, state="paused", observation=m.ObservationPolicy(),
                action=m.ActionPolicy(), reason="Current admission or service-access evidence is unavailable.",
            )
        return effective

    def _selector_matches(self, selector: m.ScopeSelector, item: m.InventoryItem) -> bool:
        if selector.tenant_id != item.tenant_id:
            return False
        if selector.kind == "tenant":
            return True
        if selector.kind == "workspace":
            return selector.workspace_id == item.workspace_id
        if selector.kind == "item":
            return (selector.workspace_id, selector.item_id) == (item.workspace_id, item.item_id)
        selected_domain = self._get("domain", selector.domain_id, item, m.InventoryDomain)
        if selected_domain is not None and selected_domain.state != "present":
            return False
        domains = set(item.domain_ids)
        workspace = self._get("workspace", item.workspace_id, item, m.InventoryWorkspace)
        if workspace is not None and workspace.state == "present" and workspace.domain_id:
            domains.add(workspace.domain_id)
        if selector.domain_id in domains:
            return True
        if not selector.include_descendants:
            return False
        ancestors = set(item.domain_ancestor_ids)
        for domain_id in domains:
            visited = set()
            current = domain_id
            while current is not None:
                if current in visited or len(visited) >= 100:
                    raise MonitoringConflict("Domain hierarchy is cyclic or exceeds the traversal budget")
                visited.add(current)
                domain = self._get("domain", current, item, m.InventoryDomain)
                if domain is None or domain.state != "present":
                    break
                current = domain.parent_domain_id
                if current is not None:
                    ancestors.add(current)
        return selector.domain_id in ancestors

    def _scope_matches(
        self, policies: list[m.ScopeDefinition], item: m.InventoryItem,
    ) -> tuple[list[tuple[m.ScopeDefinition, m.ScopeRule]], bool]:
        includes = []
        excluded = False
        for policy in policies:
            if not policy.enabled:
                continue
            for rule in policy.rules:
                if item.workload in rule.workloads and self._selector_matches(rule.selector, item):
                    if rule.effect == "exclude":
                        excluded = True
                    else:
                        includes.append((policy, rule))
        return includes, excluded

    def _inventory_complete(
        self, policies: list[m.ScopeDefinition], generations: list[m.InventoryGeneration],
    ) -> bool:
        selectors = [
            rule.selector for policy in policies if policy.enabled
            for rule in policy.rules if rule.effect == "include"
        ]
        for selector in selectors:
            covering = [
                generation for generation in generations
                if generation.enumeration == "items" and (generation.selector == selector or (
                    generation.selector.kind == "tenant"
                    and generation.authority in {"tenant_admin", "fixture"}
                ))
            ]
            if not covering:
                return False
            latest = max(covering, key=lambda generation: (generation.started_at, generation.generation_id))
            if latest.completeness != "complete":
                return False
        return True

    def _inventory_revision(self, context: m.MonitoringContext) -> int:
        return sum(self._backend.change_counter(kind, context) for kind in (
            "generation", "inventory", "workspace", "domain", "target_capability",
        ))

    def _scope_disable_targets(
        self, control: m.DeploymentControl, scopes: list[m.ScopeDefinition],
        *, publishing: bool = False,
    ) -> list[tuple[m.MonitoringTarget, m.MonitoringTarget]]:
        enabled = {scope.scope_id: scope for scope in scopes if scope.enabled}
        changes = []
        for prior in self._all("target", control, m.MonitoringTarget):
            if prior.state == "removed":
                continue
            effective = prior if publishing else self._effective_target(prior)
            if publishing:
                capability = self._get("target_capability", prior.key, control, m.CapabilityObservation)
                if capability is None or capability.read_status != "verified" or capability.expires_at <= self._now():
                    effective = _update(prior, state="paused", observation=m.ObservationPolicy(), action=m.ActionPolicy())
            remaining = [
                scope for scope_id in prior.scope_ids if (scope := enabled.get(scope_id)) is not None
                and any(
                    rule.effect == "include" and rule.rule_id in prior.admitted_rule_ids
                    for rule in scope.rules
                )
            ]
            if not remaining:
                target = _update(
                    effective, state="paused", observation=m.ObservationPolicy(),
                    action=_update(prior.action, enabled=False),
                    reason="Explicit scope disable removed the recorded inclusion; monitoring is paused.",
                )
            else:
                rule_ids = {
                    rule.rule_id for scope in remaining for rule in scope.rules
                    if rule.effect == "include" and rule.rule_id in prior.admitted_rule_ids
                }
                cadence = m.PollCadence(
                    poll_seconds=min(scope.cadence.poll_seconds for scope in remaining),
                    reconciliation_seconds=min(scope.cadence.reconciliation_seconds for scope in remaining),
                )
                target = _update(
                    effective, scope_ids=tuple(sorted(scope.scope_id for scope in remaining)),
                    admitted_rule_ids=tuple(sorted(rule_ids)),
                    observation=_update(effective.observation, cadence=cadence),
                )
            # Contract existing admission only. Releasing an exclusion must not
            # admit a previously excluded item as a side effect of disabling a scope.
            changes.append((prior, _update(
                target, policy_revision=control.revision + 1,
                action=_update(prior.action, enabled=False),
            )))
        return changes

    def _reconcile_item(
        self, item: m.InventoryItem, control: m.DeploymentControl,
        policies: list[m.ScopeDefinition], *, reviewed: bool = False, removal_complete: bool = True,
    ) -> m.MonitoringTarget | None:
        identity = item.target
        if identity is None:
            return None
        prior = self._get("target", identity.key, identity, m.MonitoringTarget)
        matches, excluded = self._scope_matches(policies, item)
        if not matches or excluded or item.state == "deleted":
            if prior is None:
                return None
            result = _update(
                prior, state="removed" if removal_complete or excluded else "paused",
                policy_revision=control.revision,
                observation=m.ObservationPolicy(), action=m.ActionPolicy(),
                reason="Explicit exclusion, completed removal or no current inclusion.",
            )
            return self._save_target(result)
        capability = self._get("target_capability", identity.key, identity, m.CapabilityObservation)
        allow_read = capability is not None and capability.read_status == "verified" and capability.expires_at > self._now()
        automatic = any(rule.auto_enrol_detection_only for _, rule in matches)
        was_admitted = prior is not None and prior.admission_basis != "pending_review" and prior.state != "removed"
        basis = "reviewed" if reviewed or (was_admitted and prior.admission_basis == "reviewed") else (
            "auto_detection_only" if automatic or was_admitted else "pending_review"
        )
        state = "current" if allow_read and basis != "pending_review" and item.state == "present" else (
            "review_required" if basis == "pending_review" else "paused"
        )
        cadence = m.PollCadence(
            poll_seconds=min(policy.cadence.poll_seconds for policy, _ in matches),
            reconciliation_seconds=min(policy.cadence.reconciliation_seconds for policy, _ in matches),
        )
        action = m.ActionPolicy()
        if prior and state == "current" and basis == "reviewed" and prior.action.review_id:
            review = self._get("review", prior.action.review_id, identity, m.SafetyReview)
            if self._review_current(review, capability, control):
                action = prior.action
        result = m.MonitoringTarget(
            identity=identity, name=item.name,
            scope_ids=tuple(sorted({policy.scope_id for policy, _ in matches})),
            admitted_rule_ids=tuple(sorted({rule.rule_id for _, rule in matches})),
            inventory_generation=item.generation_id,
            capability_id=capability.capability_id if capability else stable_id(identity, "missing-capability"),
            policy_revision=control.revision, admitted_at=prior.admitted_at if prior else self._now(),
            state=state, admission_basis=basis,
            reason="Reviewed scope admission." if reviewed else "Effective scope and service-access reconciliation.",
            observation=m.ObservationPolicy(enabled=state == "current", cadence=cadence),
            action=action, next_poll_at=prior.next_poll_at if prior and prior.next_poll_at else self._now(),
        )
        saved = self._save_target(result)
        if not allow_read:
            probe = m.MonitoringWorkDraft(
                **_stamp(identity), work_id=stable_id(identity, f"probe:{item.generation_id}:{identity.key}"),
                kind="capability_probe", target=identity, policy_revision=control.revision,
                created_at=self._now(), due_at=self._now(), reason="Verify service access for discovered inventory.",
            )
            self._enqueue(probe)
        elif saved.observation.enabled:
            self._schedule_poll(saved)
        return saved

    def _save_target(self, target: m.MonitoringTarget) -> m.MonitoringTarget:
        return self._put(
            "target", target.key, target.identity, target, status=target.state,
            workload=target.identity.workload, workspace_id=target.identity.workspace_id,
            item_id=target.identity.item_id, target_key=target.key,
            due_at=target.next_poll_at, generation_id=target.inventory_generation,
        )

    def _review_current(
        self, review: m.SafetyReview | None, capability: m.CapabilityObservation | None,
        control: m.DeploymentControl,
    ) -> bool:
        if review is None or capability is None:
            return False
        if (
            review.state != "verified" or review.publication_status != "published" or review.parameters_redacted
            or review.policy_revision != control.revision or review.expires_at <= self._now()
            or capability.expires_at <= self._now() or capability.read_status != "verified"
            or capability.action_status != "verified"
        ):
            return False
        if review.action in {"pipeline_rerun", "powerbi_refresh"}:
            if not capability.exact_action_correlation:
                return False
        elif not capability.configuration_verification:
            return False
        return review.definition_hash is None or review.definition_hash == capability.definition_hash

    @atomic(write=True)
    def record_inventory(self, batch: m.InventoryBatch) -> m.InventoryGeneration:
        def apply() -> m.InventoryGeneration:
            control = self._current(batch.expected, intake=True)
            generation = batch.generation
            if batch.commit is None:
                if self.component != "fixture":
                    raise MonitoringConflict("Live inventory commits require current work ownership and generation position")
            else:
                commit = batch.commit
                work = self._owned_work(
                    control, commit.work_id, commit.lease, commit.expected_work_revision,
                )
                if (
                    work.kind != "inventory" or generation.generation_id != work.work_id
                    or work.discovery_selector != generation.selector
                ):
                    raise MonitoringConflict("Inventory generation does not match its owned selector work")
            if generation.started_at > self._now() or (
                generation.completed_at is not None and generation.completed_at > self._now()
            ):
                raise MonitoringConflict("Inventory cannot be observed in the database clock's future")
            if not self._backend.fixture and generation.authority == "fixture":
                raise MonitoringConflict("Fixture inventory cannot establish live admission")
            if (
                generation.enumeration == "items" and generation.completeness == "complete"
                and not batch.items and (batch.workspaces or batch.domains)
            ):
                raise MonitoringConflict("A metadata-only scan must declare workspace/domain enumeration, not complete item coverage")
            old = self._get("generation", generation.generation_id, control, m.InventoryGeneration)
            if batch.commit is not None and (
                batch.commit.expected_generation_revision != (old.revision if old else 0)
                or batch.commit.expected_continuation != (old.continuation if old else None)
            ):
                raise MonitoringConflict("Inventory generation revision or continuation changed before commit")
            if old is not None and (
                old.selector != generation.selector or old.started_at != generation.started_at
                or old.completed_pages > generation.completed_pages
                or (old.completeness == "complete" and generation != old)
            ):
                raise MonitoringConflict("Inventory generation changed or pagination moved backwards")
            for workspace in batch.workspaces:
                self._put(
                    "workspace_seen", f"{generation.generation_id}:{workspace.workspace_id}", control, workspace,
                    parent_key=generation.generation_id, status=workspace.state,
                    generation_id=generation.generation_id, workspace_id=workspace.workspace_id,
                )
                prior_workspace = self._get("workspace", workspace.workspace_id, control, m.InventoryWorkspace)
                if prior_workspace is None or prior_workspace.observed_at <= workspace.observed_at:
                    self._put(
                        "workspace", workspace.workspace_id, control, workspace,
                        workspace_id=workspace.workspace_id, status=workspace.state,
                        generation_id=workspace.generation_id,
                    )
            for domain in batch.domains:
                self._put(
                    "domain_seen", f"{generation.generation_id}:{domain.domain_id}", control, domain,
                    parent_key=generation.generation_id, status=domain.state, generation_id=generation.generation_id,
                )
                prior_domain = self._get("domain", domain.domain_id, control, m.InventoryDomain)
                if prior_domain is None or prior_domain.observed_at <= domain.observed_at:
                    self._put(
                        "domain", domain.domain_id, control, domain,
                        status=domain.state, generation_id=domain.generation_id,
                    )
            for item in batch.items:
                if item.observed_at > self._now():
                    raise MonitoringConflict("Inventory item observation time is in the future")
                if not self._selector_matches(generation.selector, item):
                    raise MonitoringConflict("Inventory item is outside the enumerated selector")
                key = f"{item.workspace_id}:{item.item_id}"
                prior = self._get("inventory", key, control, m.InventoryItem)
                self._put(
                    "inventory_seen", f"{generation.generation_id}:{key}", control, item,
                    parent_key=generation.generation_id, status=item.state, generation_id=generation.generation_id,
                    workspace_id=item.workspace_id, item_id=item.item_id, workload=item.workload,
                )
                if prior is not None and prior.observed_at > item.observed_at:
                    continue
                self._put(
                    "inventory", key, control, item, workspace_id=item.workspace_id,
                    item_id=item.item_id, workload=item.workload, status=item.state,
                    generation_id=item.generation_id,
                )
            if generation.completeness == "complete":
                seen_count = self._backend.count(
                    {"items": "inventory_seen", "workspaces": "workspace_seen", "domains": "domain_seen"}[generation.enumeration],
                    control, filters={"parent_key": generation.generation_id, "status": "present"},
                )
                if seen_count != generation.discovered_count:
                    raise MonitoringConflict("Complete inventory count does not match durable observed items")
                for prior in self._all("inventory", control, m.InventoryItem):
                    if (
                        generation.enumeration == "items" and prior.generation_id != generation.generation_id
                        and prior.observed_at <= generation.started_at
                        and self._selector_matches(generation.selector, prior)
                    ):
                        removed = _update(
                            prior, state="deleted", observed_at=generation.completed_at,
                            generation_id=generation.generation_id,
                        )
                        self._put(
                            "inventory", f"{prior.workspace_id}:{prior.item_id}", control, removed,
                            workspace_id=prior.workspace_id, item_id=prior.item_id,
                            workload=prior.workload, status="deleted", generation_id=generation.generation_id,
                        )
            committed = _update(
                generation, revision=(old.revision if old else 0) + 1,
                recorded_item_count=max(
                    old.recorded_item_count if old else 0,
                    self._backend.count("inventory_seen", control, filters={"parent_key": generation.generation_id}),
                ),
                recorded_workspace_count=max(
                    old.recorded_workspace_count if old else 0,
                    self._backend.count("workspace_seen", control, filters={"parent_key": generation.generation_id}),
                ),
                recorded_domain_count=max(
                    old.recorded_domain_count if old else 0,
                    self._backend.count("domain_seen", control, filters={"parent_key": generation.generation_id}),
                ),
            )
            committed = self._put(
                "generation", generation.generation_id, control, committed,
                status=generation.completeness, due_at=generation.completed_at or generation.started_at,
            )
            if self.component == "fixture":
                self._publish_inventory(committed, control)
            else:
                self._request_reconciliation(
                    control, request_id=batch.request_id, topic="inventory",
                    reference_id=generation.generation_id, fingerprint=key_digest(_json(batch.model_dump(mode="json"))),
                    payload={"generation_id": generation.generation_id},
                    evidence=(self._evidence_binding("generation", generation.generation_id, control),),
                    producer_commit=m.CollectionCommit(
                        work_id=batch.commit.work_id, lease=batch.commit.lease,
                        expected_work_revision=batch.commit.expected_work_revision,
                    ),
                )
            return committed
        return self._idempotent("inventory", batch.request_id, batch.expected, batch, m.InventoryGeneration, apply)

    def _publish_inventory(self, generation: m.InventoryGeneration, control: m.DeploymentControl) -> None:
        policies = self._all("scope", control, m.ScopePolicy, budget=1_000)
        for item in self._all("inventory", control, m.InventoryItem):
            if generation.enumeration == "items" and not self._selector_matches(generation.selector, item):
                continue
            self._reconcile_item(item, control, policies, removal_complete=generation.completeness == "complete")
            if item.target is not None and item.state == "present" and item.generation_id == generation.generation_id:
                self._enqueue(m.MonitoringWorkDraft(
                    **_stamp(control), work_id=stable_id(control, f"probe:{generation.generation_id}:{item.target.key}"),
                    kind="capability_probe", target=item.target, policy_revision=control.revision,
                    created_at=self._now(), due_at=self._now(),
                    reason="Discovery requires a service-identity capability probe.",
                ))
        if generation.completeness != "complete" and generation.selector.kind == "domain":
            affected = {
                policy.scope_id for policy in policies if policy.enabled and any(
                    rule.effect == "include" and rule.selector.kind == "domain" for rule in policy.rules
                )
            }
            for target in self._all("target", control, m.MonitoringTarget):
                if target.state == "current" and affected.intersection(target.scope_ids):
                    self._save_target(_update(
                        target, state="paused", observation=m.ObservationPolicy(),
                        action=m.ActionPolicy(), reason="Domain membership is incomplete; admission is paused.",
                    ))
        if generation.next_scan_at is not None:
            self._enqueue(m.MonitoringWorkDraft(
                **_stamp(control), work_id=stable_id(control, f"next-inventory:{generation.generation_id}"),
                kind="inventory", discovery_selector=generation.selector, policy_revision=control.revision,
                created_at=self._now(), due_at=generation.next_scan_at,
                reason="Controller-published periodic inventory for the accepted explicit selector.",
            ))

    @atomic(write=True)
    def record_capability(
        self, expected: m.RegistryVersion, observation: m.CapabilityObservation,
        *, commit: m.CollectionCommit | None = None,
    ) -> m.CapabilityObservation:
        def apply() -> m.CapabilityObservation:
            control = self._current(expected, intake=True)
            if commit is None:
                if self.component != "fixture":
                    raise MonitoringConflict("Capability acceptance requires its current collection work fence")
            else:
                work = self._owned_work(control, commit.work_id, commit.lease, commit.expected_work_revision)
                if work.kind != "capability_probe" or work.target != observation.target:
                    raise MonitoringConflict("Capability evidence belongs to another collection work item")
            if _stamp(control) != _stamp(observation.target):
                raise MonitoringConflict("Capability evidence belongs to another tenant or epoch")
            item = self._get(
                "inventory", f"{observation.target.workspace_id}:{observation.target.item_id}",
                control, m.InventoryItem,
            )
            if item is None or item.target != observation.target or item.state != "present":
                raise MonitoringConflict("Capability evidence requires current discovered inventory")
            if item.generation_id != observation.inventory_generation or observation.checked_at > self._now():
                raise MonitoringConflict("Capability generation or observation time is not current")
            prior = self._get("target_capability", observation.target.key, control, m.CapabilityObservation)
            if prior is not None and prior.checked_at > observation.checked_at:
                raise MonitoringConflict("Older capability evidence cannot overwrite a newer probe")
            saved = self._put(
                "capability", observation.capability_id, control, observation,
                target_key=observation.target.key, generation_id=observation.inventory_generation,
            )
            if self.component == "fixture":
                self._publish_capability(saved, control)
            else:
                self._request_reconciliation(
                    control, request_id=observation.capability_id, topic="capability",
                    reference_id=observation.target.key, target=observation.target,
                    fingerprint=key_digest(_json(observation.model_dump(mode="json"))),
                    payload={"capability_id": observation.capability_id},
                    evidence=(self._evidence_binding("capability", observation.capability_id, control),),
                    producer_commit=commit,
                )
            return saved
        return self._idempotent(
            "capability", observation.capability_id, expected,
            {"expected": expected.model_dump(mode="json"), "observation": observation.model_dump(mode="json"),
             "commit": commit.model_dump(mode="json") if commit else None},
            m.CapabilityObservation, apply,
        )

    def _publish_capability(self, observation: m.CapabilityObservation, control: m.DeploymentControl) -> None:
        prior = self._get("target_capability", observation.target.key, control, m.CapabilityObservation)
        if prior is not None and prior.checked_at > observation.checked_at:
            return
        item = self._get(
            "inventory", f"{observation.target.workspace_id}:{observation.target.item_id}",
            control, m.InventoryItem,
        )
        if item is None or item.target != observation.target or item.generation_id != observation.inventory_generation:
            raise MonitoringConflict("Capability publication requires the accepted current inventory identity")
        self._put("target_capability", observation.target.key, control, observation, target_key=observation.target.key)
        self._reconcile_item(item, control, self._all("scope", control, m.ScopePolicy, budget=1_000))

    def _evaluate_scope_plan(
        self, request: m.ScopePreviewRequest, control: m.DeploymentControl,
    ) -> m.ActivationPlan:
        """Read current reviewed effects without persisting a replacement plan."""
        scopes: list[m.ScopeDefinition] = [
            scope for scope in self._all("scope", control, m.ScopePolicy, budget=1_000)
            if scope.scope_id != request.scope.scope_id
        ] + [request.scope]
        generations = self._all("generation", control, m.InventoryGeneration)
        latest: dict[str, m.InventoryGeneration] = {}
        for generation in generations:
            key = generation.enumeration + ":" + _json(generation.selector.model_dump(mode="json"))
            if key not in latest or latest[key].started_at < generation.started_at:
                latest[key] = generation
        disabling = not request.scope.enabled
        complete = self._inventory_complete(scopes, list(latest.values())) and all(
            generation.completeness == "complete"
            for generation in latest.values() if generation.enumeration != "items"
        )
        changes = []
        gaps = []
        permissions = set()
        poll_delta = 0
        desired_subscriptions = set()
        current_subscriptions = {
            source.target.key for connector in self._all("connector", control, m.OwnedConnectorManifest)
            if connector.state != "deleted" for source in connector.sources
        }
        if disabling:
            contracted = self._scope_disable_targets(control, scopes)
            retained = {target.key for _, target in contracted if target.observation.enabled}
            desired_subscriptions = current_subscriptions & retained
            for prior, target in contracted:
                changes.append(m.TargetChange(
                    identity=target.identity,
                    change="retain" if target.state == prior.state else "pause",
                    reason=target.reason, basis="explicit_policy",
                ))
                poll_delta += int(target.observation.enabled) - int(prior.observation.enabled)
        for item in (() if disabling else self._all("inventory", control, m.InventoryItem)):
            matches, excluded = self._scope_matches(scopes, item)
            if item.target is None:
                if any(
                    policy.enabled and any(
                        rule.effect == "include" and self._selector_matches(rule.selector, item)
                        for rule in policy.rules
                    ) for policy in scopes
                ):
                    gaps.append(m.CoverageGap(
                        code="unsupported", detail=item.unsupported_reason,
                        workspace_id=item.workspace_id, item_id=item.item_id,
                    ))
                continue
            prior = self._get("target", item.target.key, control, m.MonitoringTarget)
            wanted = bool(matches) and not excluded and item.state == "present"
            if wanted:
                change = "retain" if prior and prior.state == "current" else "admit"
                poll_delta += int(change == "admit")
                probe = self._get("target_capability", item.target.key, control, m.CapabilityObservation)
                if probe is not None:
                    permissions.update(probe.required_permissions)
                    if probe.event_status == "verified":
                        desired_subscriptions.add(item.target.key)
                    elif item.workload == "fabric_pipeline":
                        gaps.append(m.CoverageGap(
                            code="event_capability_unverified", detail="Event provisioning remains gated by its platform/service proof.",
                            workspace_id=item.workspace_id, item_id=item.item_id,
                        ))
                if probe is None or probe.read_status != "verified" or probe.expires_at <= self._now():
                    gaps.append(m.CoverageGap(
                        code="access_unverified", detail="A current service-identity read probe is required.",
                        workspace_id=item.workspace_id, item_id=item.item_id,
                    ))
                changes.append(m.TargetChange(
                    identity=item.target, change=change, reason="Effective proposed inclusion.",
                    basis="explicit_policy",
                ))
            elif prior is not None and prior.state != "removed":
                changes.append(m.TargetChange(
                    identity=item.target, change="remove", reason="The proposed policy excludes this target.",
                    basis="explicit_policy",
                ))
                poll_delta -= int(prior.observation.enabled)
        if not complete:
            gaps.append(m.CoverageGap(
                code="inventory_incomplete",
                detail=(
                    "Inventory remains incomplete; this explicit disable only contracts stored admissions."
                    if disabling else "Finish the required inventory before activating this scope."
                ),
            ))
        return m.ActivationPlan(
            **request.model_dump(), plan_id=stable_id(control, f"plan:{request.idempotency_id}"),
            created_at=self._now(), expires_at=self._now() + timedelta(seconds=PLAN_TTL_SECONDS),
            inventory_generations=tuple(sorted(generation.generation_id for generation in latest.values())),
            inventory_completeness="complete" if complete else "partial",
            inventory_revision=self._inventory_revision(control),
            status="ready" if complete or disabling else "blocked", changes=tuple(changes),
            required_permissions=tuple(sorted(permissions)), poll_count_delta=poll_delta,
            subscription_count_delta=len(desired_subscriptions - current_subscriptions) - len(current_subscriptions - desired_subscriptions),
            gaps=self._bounded_gaps(gaps),
        )

    @atomic(write=True)
    def preview_scope(self, request: m.ScopePreviewRequest) -> m.ActivationPlan:
        def apply() -> m.ActivationPlan:
            control = self._current(request.expected)
            plan = self._evaluate_scope_plan(request, control)
            return self._put("plan", plan.plan_id, control, plan)
        return self._idempotent("preview", request.idempotency_id, request.expected, request, m.ActivationPlan, apply)

    @atomic()
    def get_plan(self, context: m.MonitoringContext, plan_id: str) -> m.ActivationPlan | None:
        self._control(context)
        return self._get("plan", m.canonical_id(plan_id), context, m.ActivationPlan)

    @atomic(write=True)
    def activate_scope(self, request: m.ActivateScopeRequest) -> m.ActivationReceipt:
        def apply() -> m.ActivationReceipt:
            control = self._current(request.expected, intake=True)
            plan = self._get("plan", request.plan_id, control, m.ActivationPlan)
            if (
                plan is None or plan.idempotency_id != request.idempotency_id
                or plan.expected != request.expected or plan.expires_at <= self._now()
                or plan.status != "ready"
            ):
                raise MonitoringConflict("The activation plan is absent, blocked, expired or changed")
            disabling = not plan.scope.enabled
            contracted = []
            if disabling:
                proposed: list[m.ScopeDefinition] = [
                    scope for scope in self._all("scope", control, m.ScopePolicy, budget=1_000)
                    if scope.scope_id != plan.scope.scope_id
                ] + [plan.scope]
                # Re-read current admissions under the same transaction as the
                # disable, including targets enrolled since its preview.
                contracted = self._scope_disable_targets(control, proposed)
            else:
                current = self._persisted(self._evaluate_scope_plan(m.ScopePreviewRequest(
                    expected=plan.expected, idempotency_id=plan.idempotency_id,
                    scope=plan.scope, requested_by=plan.requested_by,
                ), control))
                # Global collection metadata can advance outside the reviewed
                # scope. Compare scoped effects once under the existing operation
                # lock, including expiry and subscriptions absent from that counter.
                metadata = {"created_at", "expires_at", "inventory_generations", "inventory_revision"}
                if (
                    current.model_dump(exclude=metadata) != plan.model_dump(exclude=metadata)
                    # Truncated gaps cannot prove unchanged effects after an update.
                    or current.inventory_revision != plan.inventory_revision
                    and any(gap.code == "additional_gaps" for gap in plan.gaps)
                ):
                    raise MonitoringConflict("Inventory or service capabilities changed after the dry run")
            updated, scope = self._commit_scope_intent(control, plan.scope)
            if self.component == "fixture":
                queued, configuring = self._publish_scope(plan, updated, contracted)
            else:
                work = self._request_reconciliation(
                    updated, request_id=request.idempotency_id, topic="scope",
                    reference_id=scope.scope_id, fingerprint=key_digest(_json(request.model_dump(mode="json"))),
                    payload={"plan_id": plan.plan_id},
                    evidence=(self._evidence_binding("scope", scope.scope_id, updated),),
                )
                queued, configuring = [work.work_id], True
            return m.ActivationReceipt(
                plan_id=plan.plan_id, idempotency_id=request.idempotency_id,
                version=m.RegistryVersion(**_stamp(updated), revision=updated.revision), scope=scope,
                activated_at=self._now(), state="configuring" if configuring else "active",
                requested_by=plan.requested_by,
                queued_work_ids=tuple(sorted(set(queued))),
            )
        return self._idempotent("activation", request.idempotency_id, request.expected, request, m.ActivationReceipt, apply)

    def _commit_scope_intent(
        self, control: m.DeploymentControl, definition: m.ScopeDefinition,
    ) -> tuple[m.DeploymentControl, m.ScopePolicy]:
        updated = _update(control, revision=control.revision + 1, updated_at=self._now())
        self._backend.write_control(updated, control.revision)
        scope = m.ScopePolicy(
            **definition.model_dump(), revision=updated.revision, updated_at=self._now(),
        )
        self._put("scope", scope.scope_id, control, scope, status="enabled" if scope.enabled else "disabled")
        return updated, scope

    def _publish_scope(
        self, plan: m.ActivationPlan, control: m.DeploymentControl,
        contracted: list[tuple[m.MonitoringTarget, m.MonitoringTarget]] | None = None,
    ) -> tuple[list[str], bool]:
        scopes = self._all("scope", control, m.ScopePolicy, budget=1_000)
        disabling = not plan.scope.enabled
        if disabling and contracted is None:
            contracted = self._scope_disable_targets(
                _update(control, revision=control.revision - 1), scopes, publishing=True,
            )
        reviewed = {change.identity.key for change in plan.changes if change.change in {"admit", "retain"}}
        queued = []
        configuring = False
        for _, target in contracted or ():
            saved = self._save_target(target)
            if saved.observation.enabled and self._target(saved.identity) is not None:
                queued.append(self._schedule_poll(saved).work_id)
        for item in (() if disabling else self._all("inventory", control, m.InventoryItem)):
            target = self._reconcile_item(
                item, control, scopes, reviewed=item.target is not None and item.target.key in reviewed,
            )
            if target is not None:
                configuring |= target.state in {"paused", "review_required"}
                if target.observation.enabled:
                    queued.append(self._schedule_poll(target).work_id)
                    connector_work = self._schedule_connector(target, control)
                    if connector_work is not None:
                        queued.append(connector_work.work_id)
                        configuring = True
        for connector in self._all("connector", control, m.OwnedConnectorManifest):
            if connector.state == "deleted":
                continue
            if (
                any(self._target(source.target) is None for source in connector.sources)
                or disabling and connector.policy_revision != control.revision
            ):
                queued.append(self._enqueue(m.MonitoringWorkDraft(
                    **_stamp(control), work_id=stable_id(control, f"connector:{connector.connector_id}:{control.revision}"),
                    kind="connector_reconcile", policy_revision=control.revision,
                    created_at=self._now(), due_at=self._now(), connector_id=connector.connector_id,
                    reason="Reconcile removed subscriptions against current admission.",
                )).work_id)
                configuring = True
        return queued, configuring

    @atomic()
    def get_activation(self, context: m.MonitoringContext, idempotency_id: str) -> m.ActivationReceipt | None:
        return self._receipt("activation", m.canonical_id(idempotency_id), context, m.ActivationReceipt)

    def _delivery_candidates(self, context, connector, collector_identity_id):
        return [signal for signal in self._all(
            "signal", context, m.SignalReceipt,
            filters={"parent_key": connector.connector_id, "status": "accepted"},
        ) if signal.transport is not None]

    def _delivery_original(self, signal: m.SignalReceipt, control: m.DeploymentControl) -> None:
        transport = signal.transport
        original = self._receipt("stream_intake", transport.request_id, control, m.IntakeReceipt)
        journal = self._get(
            "stream_position", f"{signal.partition.key}:position:{signal.position.sequence_number}",
            control, PositionJournal,
        )
        if (
            original is None or signal.delivery.key not in original.receipt_keys
            or journal is None or journal.receipt_kind != "identified"
            or journal.receipt_key != signal.delivery.key or journal.partition != signal.partition
            or journal.position != signal.position
        ):
            raise MonitoringUnavailable("Delivery proof lost its original accepted receipt or broker position")

    def _verified_delivery(self, signal, connector, control, desired, collector_identity_id):
        transport = signal.transport
        if (
            signal.status != "accepted" or signal.observation is None or transport is None
            or _stamp(signal.partition) != _stamp(control)
            or signal.delivery.connector_id != connector.connector_id
            or desired is None or desired.policy_revision != control.revision
            or desired.ownership_id != connector.ownership_id
            or connector.policy_revision != control.revision
            or connector.source_proposals or connector.source_removals
            or connector.state in {"blocked", "deleting", "deleted"}
            or connector.observed_definition != connector.desired_definition
            or not transport.matches_connector(connector, control.revision)
            or transport.collector_identity_id != collector_identity_id
            or not desired.published_at <= transport.identity_verified_at <= signal.received_at <= self._now()
            or not desired.published_at <= signal.position.enqueued_at <= signal.received_at
        ):
            return None
        source = next((source for source in connector.sources if (
            source.source_id == transport.source_id and source.target == signal.observation.execution.target
        )), None)
        capability = self._get("target_capability", signal.observation.execution.target.key, control, m.CapabilityObservation)
        wire = signal.event_type or signal.observation.evidence.get("native_event_type")
        subscription = WIRE_TO_SUBSCRIPTION_TYPE.get(wire) if isinstance(wire, str) else None
        if (
            source is None or source.event_source != signal.delivery.event_source
            or subscription not in source.event_types
            or self._target(signal.observation.execution.target) is None
            or capability is None or capability.collector_identity_id != collector_identity_id
            or capability.read_status != "verified" or capability.event_status != "verified"
            or capability.expires_at <= self._now()
        ):
            return None
        self._delivery_original(signal, control)
        return m.ConnectorDeliveryProof(
            request_id=transport.request_id, receipt_key=signal.delivery.key,
            collector_identity_id=collector_identity_id, received_at=signal.received_at,
            identity_verified_at=transport.identity_verified_at,
        )

    def _connector_delivery(self, context, connector_id, collector_identity_id):
        control = self._control(context)
        connector = self._get("connector", m.canonical_id(connector_id), control, m.OwnedConnectorManifest)
        if (
            connector is None or control.maintenance or connector.state not in {"ready", "degraded"}
            or connector.source_proposals or connector.source_removals
            or connector.observed_definition != connector.desired_definition
        ):
            return None
        candidates = self._delivery_candidates(control, connector, m.canonical_id(collector_identity_id))
        if not candidates:
            return None
        desired = self._get("connector_desired", connector.connector_id, control, m.ConnectorDesiredState)
        proofs = [
            proof for signal in candidates
            if (proof := self._verified_delivery(signal, connector, control, desired, collector_identity_id)) is not None
        ]
        return max(proofs, key=lambda proof: (proof.received_at, proof.receipt_key)) if proofs else None

    @atomic()
    def get_connector_delivery(self, context, connector_id, collector_identity_id):
        return self._connector_delivery(context, connector_id, collector_identity_id)

    def _require_connector_delivery(self, observed, control):
        proof = observed.delivery_proof
        signal = self._get("signal", proof.receipt_key, control, m.SignalReceipt) if proof else None
        connector = self._get("connector", observed.connector_id, control, m.OwnedConnectorManifest)
        desired = self._get("connector_desired", observed.connector_id, control, m.ConnectorDesiredState)
        if (
            proof is None or signal is None or connector is None
            or self._verified_delivery(signal, connector, control, desired, proof.collector_identity_id) != proof
            or observed.delivery_verified_at != proof.received_at
            or observed.identity_verified_at != signal.transport.identity_verified_at
        ):
            raise MonitoringConflict("Readiness requires the original current accepted transport receipt")

    @atomic()
    def get_connector_desired(self, context, connector_id):
        self._control(context)
        return self._get("connector_desired", m.canonical_id(connector_id), context, m.ConnectorDesiredState)

    @staticmethod
    def _validate_initial_connector_publication(prior, desired, request):
        if prior is not None and desired is None and (
            prior.state != "planned"
            or any(getattr(prior, key) is None for key in (
                "workspace_id", "eventstream_id", "destination_id", "endpoint", "observed_definition",
            ))
            or prior.observed_definition != prior.desired_definition
            or prior.source_proposals or prior.source_removals
            or any(getattr(prior, key) is not None for key in (
                "operation_id", "identity_verified_at", "delivery_verified_at",
                "delivery_proof", "last_receiver_activity_at",
            ))
            or request.source_removals or request.observation_receipt_id is not None
            or request.readiness_receipt_id is not None
            or not (request.sources or request.source_proposals)
        ):
            raise MonitoringConflict("Initial connector publication requires a proof-free registered baseline and admitted sources")

    @atomic(write=True)
    def record_connector(
        self, expected: m.RegistryVersion, manifest: m.OwnedConnectorManifest,
        *, expected_connector_revision: int, commit: m.CollectionCommit | None = None,
    ) -> m.OwnedConnectorManifest:
        TypeAdapter(m.Revision).validate_python(expected_connector_revision)
        payload = {
            "expected": expected.model_dump(mode="json"), "manifest": manifest.model_dump(mode="json"),
            "expected_connector_revision": expected_connector_revision,
            "commit": commit.model_dump(mode="json") if commit is not None else None,
        }
        def apply() -> m.OwnedConnectorManifest:
            control = self._current(expected, intake=True)
            if _stamp(manifest) != _stamp(control) or manifest.policy_revision != control.revision:
                raise MonitoringConflict("Connector manifest is bound to another deployment policy")
            if commit is None and self.component != "fixture":
                raise MonitoringComponentDenied("Connector observations require their actual collection work fence")
            if commit is not None:
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
            if manifest.state == "ready" and any(
                value is not None and value > self._now()
                for value in (manifest.identity_verified_at, manifest.delivery_verified_at)
            ):
                raise MonitoringConflict("Ready observations require nonfuture identity and delivery evidence")
            prior = self._get("connector", manifest.connector_id, control, m.OwnedConnectorManifest)
            if self.component != "fixture" and (
                prior is None or any(
                    getattr(prior, name) != getattr(manifest, name)
                    for name in ("ownership_id", "name", "sources", "source_proposals", "source_removals", "desired_definition")
                )
            ):
                raise MonitoringComponentDenied("The worker may observe, not replace, controller-owned desired connector topology")
            if (prior.revision if prior else 0) != expected_connector_revision:
                raise MonitoringConflict("Connector revision changed")
            if manifest.revision != expected_connector_revision + 1:
                raise MonitoringConflict("Connector updates must advance their revision exactly once")
            if prior is not None and (
                prior.ownership_id != manifest.ownership_id
                or (prior.workspace_id is not None and prior.workspace_id != manifest.workspace_id)
                or (prior.eventstream_id is not None and prior.eventstream_id != manifest.eventstream_id)
                or (prior.destination_id is not None and prior.destination_id != manifest.destination_id)
                or (prior.endpoint is not None and prior.endpoint != manifest.endpoint)
            ):
                raise MonitoringConflict("A connector cannot adopt a different owner or Eventstream")
            if manifest.state == "ready" and self.component != "fixture":
                self._require_connector_delivery(manifest, control)
            for source in manifest.sources:
                inventory = self._get(
                    "inventory", f"{source.target.workspace_id}:{source.target.item_id}", control, m.InventoryItem,
                )
                if inventory is None or inventory.target != source.target:
                    raise MonitoringConflict("Connector sources require exact discovered targets")
            for other in self._all("connector", control, m.OwnedConnectorManifest):
                if other.connector_id != manifest.connector_id and other.state != "deleted":
                    if {source.target.key for source in other.sources} & {source.target.key for source in manifest.sources}:
                        raise MonitoringConflict("Overlapping scopes cannot create duplicate event subscriptions")
            request_id = stable_id(control, f"connector:{manifest.connector_id}:{expected_connector_revision}")
            observation = self._put("connector_observation", request_id, control, manifest)
            effective = observation
            if self.component != "fixture":
                effective = _update(
                    observation, identity_verified_at=prior.identity_verified_at,
                    delivery_verified_at=prior.delivery_verified_at,
                    delivery_proof=prior.delivery_proof,
                    state="provisioning" if observation.state == "ready" and prior.state != "ready" else observation.state,
                )
            saved = self._put("connector", manifest.connector_id, control, effective, status=effective.state)
            if self.component == "fixture":
                self._publish_connector(saved, control)
            else:
                self._request_reconciliation(
                    control, request_id=request_id,
                    topic="connector", reference_id=manifest.connector_id,
                    fingerprint=key_digest(_json(payload)),
                    payload={"connector_id": manifest.connector_id},
                    evidence=(self._evidence_binding("connector", manifest.connector_id, control),),
                    producer_commit=commit,
                )
            return saved
        return self._idempotent(
            "connector", stable_id(expected, f"connector:{manifest.connector_id}:{expected_connector_revision}"), expected,
            payload,
            m.OwnedConnectorManifest, apply,
        )

    def _connector_observation_result(self, context, request_id) -> m.ConnectorObservationResult:
        receipt = self._backend.get_receipt("connector", request_id, context)
        producer = self._get("worker_reconcile_request", request_id, context, m.ReconciliationRequest)
        observation = self._get("connector_observation", request_id, context, m.OwnedConnectorManifest)
        if (
            receipt is None or producer is None or producer.producer_commit is None or observation is None
            or producer.topic != "connector" or producer.reference_id != observation.connector_id
            or producer.fingerprint != receipt.fingerprint or _stamp(observation) != _stamp(context)
        ):
            raise MonitoringUnavailable("Connector observation lost its original receipt or collection binding")
        commit = producer.producer_commit
        return m.ConnectorObservationResult(
            connector_id=observation.connector_id,
            connector=m.OwnedConnectorManifest.model_validate_json(receipt.payload), observation=observation,
            observed_definition_hash=(
                hashlib.sha256(_json(observation.observed_definition).encode("utf-16-le")).hexdigest().upper()
                if observation.observed_definition is not None else None
            ),
            authority="observed_not_action_authority", reconcile_work_id=producer.work_id,
            frontier_key=producer.frontier_key, frontier_revision=producer.frontier_revision,
            work_id=commit.work_id, work_owner_id=commit.lease.owner_id, work_fence=commit.lease.fence,
            work_revision=commit.expected_work_revision,
            collection_completion_eligible=m.connector_collection_eligible(observation),
        )

    @atomic(write=True)
    def publish_connector(self, request: m.ConnectorPublicationRequest) -> m.ConnectorPublicationResult:
        return self._connector_publication(request)

    def _connector_publication(self, request: m.ConnectorPublicationRequest) -> m.ConnectorPublicationResult:
        def apply():
            control = self._current(request.expected, intake=True)
            work = self._owned_work(control, request.work_id, request.lease, request.expected_work_revision)
            producer = self._reconcile_request(work)
            frontier = self._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
            if (
                producer.policy_revision != control.revision or frontier is None
                or frontier.accepted_revision != request.expected_frontier_revision
            ):
                raise MonitoringConflict("Connector publication lost its current intent/frontier binding")
            prior = self._get("connector", request.connector_id, control, m.OwnedConnectorManifest)
            desired = self._get("connector_desired", request.connector_id, control, m.ConnectorDesiredState)
            if (prior.revision if prior else 0) != request.expected_connector_revision:
                raise MonitoringConflict("Connector publication revision changed")
            if prior is not None and (prior.ownership_id != request.ownership_id or prior.state in {"deleting", "deleted"}):
                raise MonitoringConflict("An established connector cannot change owner or be revived")
            self._validate_initial_connector_publication(prior, desired, request)
            removed_sources = {removal.source_id for removal in request.source_removals if removal.source_id is not None}
            removed_proposals = {removal.proposal_id for removal in request.source_removals if removal.proposal_id is not None}
            desired_sources = (
                *(source for source in request.sources if source.source_id not in removed_sources),
                *(proposal for proposal in request.source_proposals if proposal.proposal_id not in removed_proposals),
            )
            for source in desired_sources:
                target = self._target(source.target)
                capability = self._get("target_capability", source.target.key, control, m.CapabilityObservation)
                if (
                    target is None or capability is None or capability.read_status != "verified"
                    or capability.event_status != "verified" or capability.expires_at <= self._now()
                ):
                    raise MonitoringConflict("Desired event sources require current approved observation/event capability")
            if prior is not None:
                self._check_connector_bindings(prior, request)
            saved_request = self._persisted(request)
            if saved_request.sources != request.sources or saved_request.source_proposals != request.source_proposals or (
                saved_request.source_removals != request.source_removals
            ) or (
                saved_request.desired_definition != request.desired_definition
            ):
                raise MonitoringConflict("Redacted desired topology cannot be published as executable configuration")
            observed = None
            if request.observation_receipt_id is not None:
                if request.observation_receipt_id != producer.request_id:
                    raise MonitoringConflict("Physical binding requires the current original worker observation")
                observed = self._get("connector_observation", producer.request_id, control, m.OwnedConnectorManifest)
            sources, proposals, definition = self._connector_desired_values(request, prior, observed)
            removals, retirements = self._connector_removal_records(request, prior, observed, work, control)
            changed = prior is None or desired is None or any((
                prior.policy_revision != control.revision, prior.name != request.name,
                prior.sources != sources, prior.source_proposals != proposals, prior.desired_definition != definition,
                prior.source_removals != removals,
            ))
            if prior is None:
                candidate = m.OwnedConnectorManifest(
                    **_stamp(control), connector_id=request.connector_id, ownership_id=request.ownership_id,
                    revision=1, policy_revision=control.revision, name=request.name, sources=sources,
                    source_proposals=proposals, source_removals=removals,
                    desired_definition=definition, state="planned", updated_at=self._now(),
                )
            else:
                candidate = _update(
                    prior, name=request.name, sources=sources, source_proposals=proposals, source_removals=removals,
                    desired_definition=definition,
                    revision=prior.revision + 1, policy_revision=control.revision, updated_at=self._now(),
                    state="provisioning" if changed else prior.state,
                    identity_verified_at=None if changed else prior.identity_verified_at,
                    delivery_verified_at=None if changed else prior.delivery_verified_at,
                    delivery_proof=None if changed else prior.delivery_proof,
                )
            if request.readiness_receipt_id is not None:
                if changed or prior is None or request.readiness_receipt_id != producer.request_id:
                    raise MonitoringConflict("Readiness must use the exact existing desired-connector observation")
                observed = self._get("connector_observation", producer.request_id, control, m.OwnedConnectorManifest)
                desired = self._get("connector_desired", request.connector_id, control, m.ConnectorDesiredState)
                if (
                    observed is None or desired is None or observed.state != "ready"
                    or observed.desired_definition != request.desired_definition
                    or observed.observed_definition != request.desired_definition
                    or observed.ownership_id != request.ownership_id or observed.policy_revision != control.revision
                    or any(getattr(observed, key) != getattr(prior, key) for key in (
                        "workspace_id", "eventstream_id", "destination_id", "endpoint",
                    ))
                    or observed.identity_verified_at is None or observed.delivery_verified_at is None
                    or not desired.published_at <= observed.identity_verified_at <= self._now()
                    or not desired.published_at <= observed.delivery_verified_at <= self._now()
                ):
                    raise MonitoringConflict("The observation does not prove current owned topology readiness")
                self._require_connector_delivery(observed, control)
                candidate = _update(
                    candidate, state="ready", identity_verified_at=observed.identity_verified_at,
                    delivery_verified_at=observed.delivery_verified_at,
                    delivery_proof=observed.delivery_proof,
                )
            saved = self._put("connector", candidate.connector_id, control, candidate, status=candidate.state)
            if changed:
                self._put("connector_desired", saved.connector_id, control, m.ConnectorDesiredState(
                    connector_id=saved.connector_id, ownership_id=saved.ownership_id, publication_id=request.request_id,
                    policy_revision=control.revision, sources_hash=m._digest([source.model_dump(mode="json") for source in saved.sources]),
                    definition_hash=m._digest(saved.desired_definition), published_at=self._now(),
                ))
            self._publish_connector(saved, control)
            if saved.state in {"planned", "provisioning"} and changed:
                self._connector_followup(saved, control)
            return m.ConnectorPublicationResult(
                connector_id=saved.connector_id, connector=saved, state=saved.state, desired_changed=changed,
                pending_removals=removals, retired_sources=retirements, observation_receipt_id=request.observation_receipt_id,
            )
        return self._idempotent(
            "connector_publication", request.request_id, request.expected, request, m.ConnectorPublicationResult, apply,
        )

    @staticmethod
    def _connector_desired_values(request, prior, observed):
        old_sources = {source.source_id: source for source in prior.sources} if prior else {}
        old_proposals = {proposal.proposal_id: proposal for proposal in prior.source_proposals} if prior else {}
        if prior is not None and request.sources != prior.sources:
            raise MonitoringConflict("Physical source ownership cannot be removed or changed before verified retirement")
        proposed = {proposal.proposal_id: proposal for proposal in request.source_proposals}
        if any(proposed.get(identifier) != proposal for identifier, proposal in old_proposals.items()):
            raise MonitoringConflict("Uncertain logical proposal ownership cannot be silently discarded")
        prior_removals = {removal.removal_id: removal for removal in prior.source_removals} if prior else {}
        intents = {removal.removal_id: removal for removal in request.source_removals}
        if any(intents.get(identifier) != removal.intent() for identifier, removal in prior_removals.items()):
            raise MonitoringConflict("Pending removal cannot be omitted, cancelled or rewritten")
        for source in request.sources:
            if source.source_id not in old_sources or old_sources[source.source_id].target != source.target:
                raise MonitoringConflict("New physical source IDs require receipt-bound logical proposal resolution")
        for proposal in request.source_proposals:
            previous = old_proposals.get(proposal.proposal_id)
            if previous is not None and previous != proposal:
                raise MonitoringConflict("A logical source proposal cannot change identity or target")
            key = f"sources/{proposal.node_name}"
            if request.observation_receipt_id is None:
                if key in request.desired_definition.get("component_ids", {}) or any(
                    node.get("name") == proposal.node_name and node.get("id") is not None
                    for node in request.desired_definition["parts"]["eventstream.json"]["sources"]
                ):
                    raise MonitoringConflict("An unresolved proposal cannot introduce unreceipted physical identity")
                if previous is None and prior is not None and key in (prior.observed_definition or {}).get("component_ids", {}):
                    raise MonitoringConflict("A new logical proposal cannot adopt an already observed unowned source ID")
        if request.observation_receipt_id is None:
            retained = tuple(prior.source_proposals) if prior else ()
            additions = tuple(proposal for proposal in request.source_proposals if proposal.proposal_id not in old_proposals)
            return request.sources, retained + additions, request.desired_definition
        if (
            prior is None or observed is None or request.sources != prior.sources
            or request.source_proposals != prior.source_proposals
            or request.desired_definition != prior.desired_definition
            or observed.ownership_id != prior.ownership_id or observed.policy_revision != request.expected.revision
            or observed.state not in {"provisioning", "ready", "degraded"}
            or observed.sources != prior.sources or observed.source_proposals != prior.source_proposals
            or observed.source_removals != prior.source_removals
            or observed.desired_definition != prior.desired_definition or observed.observed_definition is None
            or observed.observed_definition.get("parts") != prior.desired_definition.get("parts")
            or set(intents) != set(prior_removals)
        ):
            raise MonitoringConflict("Physical source binding lost its exact approved proposal and original observation")
        MonitoringEngine._complete_observed_components(observed.observed_definition)
        removed_sources = {removal.source_id for removal in prior_removals.values() if removal.source_id is not None}
        removed_proposals = {removal.proposal_id for removal in prior_removals.values() if removal.proposal_id is not None}
        observed_graph = observed.observed_definition["parts"]["eventstream.json"]
        returned = observed.observed_definition["component_ids"]
        for removal in prior_removals.values():
            if f"sources/{removal.node_name}" in returned or any(
                identifier is not None and identifier in returned.values()
                for identifier in (removal.source_id, removal.last_observed_source_id)
            ) or any(
                node.get("name") == removal.node_name or node.get("id") in {
                    value for value in (removal.source_id, removal.last_observed_source_id) if value is not None
                }
                for node in observed_graph["sources"]
            ) or any(
                node.get("name") == removal.node_name
                for stream in observed_graph["streams"] for node in stream.get("inputNodes", ())
            ):
                raise MonitoringConflict("The original complete observation does not prove remote source absence")
        sources = [source for source in request.sources if source.source_id not in removed_sources]
        proposals = []
        used = {source.source_id for source in sources}
        for proposal in request.source_proposals:
            if proposal.proposal_id in removed_proposals:
                continue
            identifier = returned.get(f"sources/{proposal.node_name}")
            try:
                valid = isinstance(identifier, str) and m.canonical_id(identifier) == identifier
            except ValueError:
                valid = False
            if not valid:
                proposals.append(proposal)
                continue
            if identifier in used:
                raise MonitoringConflict("One observed physical source cannot bind multiple logical proposals")
            used.add(identifier)
            sources.append(m.ConnectorSource(
                source_id=identifier, target=proposal.target, event_types=proposal.event_types,
                event_source=proposal.event_source,
            ))
        return tuple(sources), tuple(proposals), observed.observed_definition

    @staticmethod
    def _complete_observed_components(definition):
        graph = definition.get("parts", {}).get("eventstream.json", {})
        components = definition.get("component_ids")
        if not isinstance(components, dict) or not all(
            isinstance(graph.get(kind), list) for kind in ("sources", "streams", "destinations")
        ):
            raise MonitoringConflict("Physical reconciliation requires an explicit complete component map")
        expected = {}
        for kind in ("sources", "streams", "destinations"):
            for node in graph[kind]:
                if not isinstance(node, dict) or not isinstance(node.get("name"), str):
                    raise MonitoringConflict("Observed topology has an invalid node")
                key = f"{kind}/{node['name']}"
                identifier = components.get(key)
                try:
                    valid = isinstance(identifier, str) and m.canonical_id(identifier) == identifier
                except ValueError:
                    valid = False
                if not valid or key in expected or node.get("id", identifier) != identifier:
                    raise MonitoringConflict("Observed topology lacks its exact canonical node-to-ID mapping")
                expected[key] = identifier
        if set(expected) != set(components) or len(set(expected.values())) != len(expected):
            raise MonitoringConflict("Observed component map is incomplete, duplicated or contains unrelated identities")

    @staticmethod
    def _removal_binding(prior, intent):
        if prior is None:
            raise MonitoringConflict("Source removal requires existing connector ownership")
        values = prior.sources if intent.source_id is not None else prior.source_proposals
        matches = [
            value for value in values
            if (value.source_id == intent.source_id if intent.source_id is not None else value.proposal_id == intent.proposal_id)
        ]
        if len(matches) != 1:
            raise MonitoringConflict("Source removal does not identify one owned binding")
        binding = matches[0]
        pending = next((removal for removal in prior.source_removals if removal.removal_id == intent.removal_id), None)
        if pending is not None:
            if pending.intent() != intent or pending.target != binding.target:
                raise MonitoringConflict("An original pending removal cannot change its selector or target")
            return binding, pending.node_name
        if intent.proposal_id is not None:
            return binding, binding.node_name
        names = {
            key.removeprefix("sources/")
            for definition in (prior.desired_definition, prior.observed_definition or {})
            for key, value in definition.get("component_ids", {}).items()
            if key.startswith("sources/") and value == intent.source_id
        }
        if len(names) != 1:
            raise MonitoringConflict("Physical source removal requires one original ID-to-node ownership mapping")
        return binding, names.pop()

    def _connector_removal_records(self, request, prior, observed, work, control):
        existing = {removal.removal_id: removal for removal in prior.source_removals} if prior else {}
        pending = []
        retired = []
        for intent in sorted(request.source_removals, key=lambda value: value.removal_id):
            binding, node_name = self._removal_binding(prior, intent)
            tombstone_key = f"{request.connector_id}:removal:{intent.removal_id}"
            original = existing.get(intent.removal_id)
            if original is None:
                if self._backend.get("connector_source_retirement", tombstone_key, control) is not None:
                    raise MonitoringConflict("A retired source-removal identity cannot be reused")
                remembered = (prior.observed_definition or {}).get("component_ids", {}).get(f"sources/{node_name}")
                try:
                    remembered = remembered if isinstance(remembered, str) and m.canonical_id(remembered) == remembered else None
                except ValueError:
                    remembered = None
                original = m.PendingSourceRemoval(
                    **intent.model_dump(), node_name=node_name, last_observed_source_id=remembered,
                    target=binding.target,
                    binding_hash=hashlib.sha256(_json(binding.model_dump(mode="json")).encode("utf-16-le")).hexdigest().upper(),
                    policy_revision=control.revision, request_id=request.request_id,
                    publication_id=stable_id(control, f"connector-publication:{request.request_id}"),
                    requested_at=self._now(), state="pending_remote_absence",
                )
            if request.observation_receipt_id is None:
                pending.append(original)
                continue
            receipt = self._backend.get_receipt("connector", request.observation_receipt_id, control)
            if receipt is None or observed is None or observed.observed_definition is None:
                raise MonitoringUnavailable("Source retirement requires the original recorded observation")
            payload = observed.model_dump(mode="json")
            observation_arguments = {
                **_stamp(control), "expected_revision": observed.policy_revision,
                "connector_id": observed.connector_id, "expected_connector_revision": observed.revision - 1,
                "ownership_id": observed.ownership_id, "observation_json": _json({
                    key: payload[key] for key in (
                        "workspace_id", "eventstream_id", "destination_id", "observed_definition", "endpoint",
                        "operation_id", "state", "identity_verified_at", "delivery_verified_at", "gaps",
                    )
                }),
            }
            binding_hash = hashlib.sha256(_json(observation_arguments).encode("utf-16-le")).hexdigest().upper()
            envelope = _json({"binding_hash": binding_hash, "result": {
                "connector": json.loads(receipt.payload), "observation": payload,
            }})
            retirement = m.ConnectorSourceRetirement(
                connector_id=request.connector_id, ownership_id=request.ownership_id,
                removal_id=intent.removal_id, source_id=intent.source_id, proposal_id=intent.proposal_id,
                node_name=node_name, original_binding=binding, original_removal=original,
                observation_receipt_id=request.observation_receipt_id, observation_fingerprint=receipt.fingerprint,
                observation_binding_hash=binding_hash,
                observation_receipt_hash=hashlib.sha256(envelope.encode("utf-16-le")).hexdigest().upper(),
                observed_definition_hash=hashlib.sha256(_json(observed.observed_definition).encode("utf-16-le")).hexdigest().upper(),
                confirmation_request_id=request.request_id, work_id=work.work_id, work_fence=work.lease.fence,
                policy_revision=control.revision, retired_at=self._now(), state="retired_verified",
            )
            retired.append(self._put(
                "connector_source_retirement", tombstone_key, control, retirement, parent_key=request.connector_id,
            ))
        return tuple(pending), tuple(retired)

    def _connector_followup(self, connector, control):
        return self._enqueue(m.MonitoringWorkDraft(
            **_stamp(control), work_id=stable_id(control, f"connector:{connector.connector_id}:publication:{connector.revision}"),
            kind="connector_reconcile", policy_revision=control.revision, created_at=self._now(), due_at=self._now(),
            connector_id=connector.connector_id,
            reason="Controller-published desired connector state requires its next bounded observation.",
        ))

    @staticmethod
    def _check_connector_bindings(prior, request):
        existing = {source.target.key: source.source_id for source in prior.sources}
        owners = {source.source_id: source.target for source in prior.sources}
        for source in request.sources:
            if (
                source.target.key in existing and existing[source.target.key] != source.source_id
                or source.source_id in owners and owners[source.source_id] != source.target
            ):
                raise MonitoringConflict("Established source component identities cannot be rebound")
        old_graph = prior.desired_definition.get("parts", {}).get("eventstream.json", {})
        graph = request.desired_definition["parts"]["eventstream.json"]
        new_nodes = {
            (node["properties"]["workspaceId"], node["properties"]["itemId"]): node["name"]
            for node in graph["sources"]
        }
        for node in old_graph.get("sources", []):
            identity = (node["properties"]["workspaceId"], node["properties"]["itemId"])
            if identity in new_nodes and new_nodes[identity] != node["name"]:
                raise MonitoringConflict("Established per-item source node identities cannot be replaced")
        for kind in ("streams", "destinations"):
            if old_graph.get(kind) and old_graph[kind][0].get("name") != graph[kind][0]["name"]:
                raise MonitoringConflict("Established stream/destination identities cannot be replaced")
        names = {node["name"] for node in graph["sources"]}
        for key, value in prior.desired_definition.get("component_ids", {}).items():
            if key.startswith(("streams/", "destinations/")) or key.removeprefix("sources/") in names:
                if request.desired_definition.get("component_ids", {}).get(key) != value:
                    raise MonitoringConflict("Established physical component IDs must be retained")

    @atomic()
    def get_connector_publication(self, context, request_id):
        return self._receipt("connector_publication", m.canonical_id(request_id), context, m.ConnectorPublicationResult)

    def _reconcile_connector_observation(self, producer, connector, control):
        observed = self._get("connector_observation", producer.request_id, control, m.OwnedConnectorManifest)
        if (
            observed is not None and observed.observed_definition is not None
            and observed.state in {"provisioning", "ready", "degraded"}
            and (connector.source_proposals or connector.source_removals) and self.component != "fixture"
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
        if observed is not None and observed.state == "ready" and self.component != "fixture":
            work = self._get("work", producer.work_id, control, m.MonitoringWork)
            frontier = self._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
            result = self._connector_publication(m.ConnectorPublicationRequest(
                request_id=stable_id(control, f"connector-ready:{producer.request_id}:{work.lease.fence}"),
                expected=m.RegistryVersion(**_stamp(control), revision=control.revision),
                work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
                expected_frontier_revision=frontier.accepted_revision,
                connector_id=connector.connector_id, ownership_id=connector.ownership_id,
                expected_connector_revision=connector.revision, name=connector.name, sources=connector.sources,
                desired_definition=connector.desired_definition, readiness_receipt_id=producer.request_id,
                detail="Publish only the original matched worker readiness observation.",
            ))
            connector = result.connector
        self._publish_connector(connector, control)

    def _publish_connector(self, manifest: m.OwnedConnectorManifest, control: m.DeploymentControl) -> None:
        connectors = self._all("connector", control, m.OwnedConnectorManifest)
        for target in self._all("target", control, m.MonitoringTarget):
            capability = self._get("target_capability", target.key, control, m.CapabilityObservation)
            active_source = any(
                connector.state == "ready" and any(source.target == target.identity for source in connector.sources)
                for connector in connectors
            )
            if not active_source and not target.observation.events_enabled:
                continue
            events = bool(
                active_source and target.observation.enabled
                and capability is not None and capability.event_status == "verified"
                and capability.expires_at > self._now()
            )
            if events != target.observation.events_enabled:
                self._save_target(_update(target, observation=_update(target.observation, events_enabled=events)))

    @atomic(write=True)
    def record_safety_review(self, request: m.SafetyReviewRequest) -> m.SafetyReview:
        def apply() -> m.SafetyReview:
            control = self._current(request.expected, intake=True)
            review = request.review
            target = self._target(review.target, include_inactive=True)
            if target is None or target.state != "current":
                raise MonitoringConflict("A safety review requires a currently admitted target")
            kind = "review" if self.component == "fixture" else "review_request"
            prior = self._get(kind, review.review_id, control, m.SafetyReview)
            if prior is None:
                prior = self._get("review", review.review_id, control, m.SafetyReview)
            if (prior.revision if prior else 0) != request.expected_review_revision:
                raise MonitoringConflict("Safety review revision changed")
            if prior is not None and (prior.target != review.target or prior.action != review.action):
                raise MonitoringConflict("A review identity cannot move to another target or action")
            if review.reviewed_at > self._now():
                raise MonitoringConflict("A review cannot have a future original review time")
            if prior is not None and review.state == "revoked" and (
                review.reviewed_at != prior.reviewed_at or review.expires_at != prior.expires_at
            ):
                raise MonitoringConflict("Revocation must retain the original review time and expiry")
            if self.component == "fixture":
                return self._publish_review(review, control)
            pending = _update(
                review, state="pending", requested_state=(
                    review.requested_state if review.publication_status == "pending_validation" else review.state
                ),
                publication_status="pending_validation", revoked_at=None,
                exact_correlation_verified=False,
            )
            updated, saved = self._commit_review_intent(control, pending, request.expected_review_revision)
            self._request_reconciliation(
                updated, request_id=request.request_id, topic="review",
                reference_id=review.review_id, target=review.target,
                fingerprint=key_digest(_json(request.model_dump(mode="json"))),
                payload={"review_id": review.review_id},
                evidence=(self._evidence_binding(kind, review.review_id, updated),),
            )
            return saved
        return self._idempotent("safety_review", request.request_id, request.expected, request, m.SafetyReview, apply)

    def _commit_review_intent(
        self, control: m.DeploymentControl, pending: m.SafetyReview, expected_review_revision: int,
    ) -> tuple[m.DeploymentControl, m.SafetyReview]:
        updated = _update(control, revision=control.revision + 1, updated_at=self._now())
        self._backend.write_control(updated, control.revision)
        saved = self._put(
            "review_request", pending.review_id, updated, pending, target_key=pending.target.key,
        )
        return updated, saved

    def _publish_review(self, review: m.SafetyReview, control: m.DeploymentControl) -> m.SafetyReview:
        capability = self._get("target_capability", review.target.key, control, m.CapabilityObservation)
        if review.publication_status == "pending_validation":
            desired = review.requested_state
            usable = (
                capability is not None and capability.read_status == "verified"
                and capability.action_status == "verified" and capability.expires_at > self._now()
                and review.expires_at > self._now() and not review.parameters_redacted
                and (review.definition_hash is None or capability.definition_hash == review.definition_hash)
            )
            if review.action in {"pipeline_rerun", "powerbi_refresh"}:
                usable = usable and capability is not None and capability.exact_action_correlation
            else:
                usable = (
                    usable and capability is not None and capability.configuration_verification
                    and review.parameters is not None and review.configuration_hash == review.parameter_hash
                )
            if review.action == "pipeline_rerun":
                usable = usable and review.replay_safe and review.parameters is not None and review.definition_hash is not None
            state = "unverifiable" if desired == "verified" and not usable else desired
            review = _update(
                review, state=state, policy_revision=control.revision, publication_status="published",
                exact_correlation_verified=bool(
                    capability is not None and capability.exact_action_correlation
                    and review.action in {"pipeline_rerun", "powerbi_refresh"}
                ),
                revoked_at=self._now() if state == "revoked" else None,
            )
            for item in self._all("inventory", control, m.InventoryItem):
                self._reconcile_item(item, control, self._all("scope", control, m.ScopePolicy, budget=1_000))
        saved = self._put("review", review.review_id, control, review, target_key=review.target.key)
        target = self._get("target", review.target.key, control, m.MonitoringTarget)
        if target is None:
            raise MonitoringConflict("Safety publication has no admitted target")
        enabled = target.state == "current" and self._review_current(saved, capability, control)
        action = m.ActionPolicy(
            enabled=enabled, action=saved.action, review_id=saved.review_id, review_revision=saved.revision,
        )
        self._save_target(_update(
            target, policy_revision=control.revision, admission_basis="reviewed", action=action,
        ))
        return saved

    @atomic()
    def get_safety_review(self, context: m.MonitoringContext, review_id: str) -> m.SafetyReview | None:
        self._control(context)
        review_id = m.canonical_id(review_id)
        published = self._get("review", review_id, context, m.SafetyReview)
        intent = self._get("review_request", review_id, context, m.SafetyReview)
        if intent is not None and (published is None or intent.revision > published.revision):
            return intent
        return published

    @atomic()
    def get_safety_review_operation(
        self, context: m.MonitoringContext, request_id: str,
    ) -> m.SafetyReviewOperationReceipt | None:
        self._control(context)
        request_id = m.canonical_id(request_id)
        receipt = self._backend.get_receipt("safety_review", request_id, context)
        if receipt is None:
            return None
        if (
            receipt.operation != "safety_review" or receipt.request_id != request_id
            or _stamp(receipt.context) != _stamp(context)
        ):
            raise MonitoringUnavailable("Safety-review operation receipt identity is inconsistent")
        try:
            review = m.SafetyReview.model_validate_json(receipt.payload)
            # The original validated request binds policy_revision to expected.revision
            # and advances the review exactly once. Use that immutable result, not the
            # mutable review row; receipt storage and redaction remain unchanged.
            return m.SafetyReviewOperationReceipt(
                request_id=receipt.request_id, target=review.target, action=review.action,
                expected=m.RegistryVersion(**_stamp(context), revision=review.policy_revision),
                expected_review_revision=review.revision - 1, new_review_revision=review.revision,
                fingerprint=receipt.fingerprint, recorded_at=receipt.recorded_at, review=review,
                requested_state=review.requested_state, publication_status=review.publication_status,
            )
        except ValidationError as exc:
            logger.error("Unreadable safety-review operation receipt key_hash=%s", key_digest(request_id))
            raise MonitoringUnavailable("The persisted safety-review operation receipt is unreadable") from exc

    def _bounded_gaps(self, gaps: list[m.CoverageGap]) -> tuple[m.CoverageGap, ...]:
        if len(gaps) <= 200:
            return tuple(gaps)
        return (*gaps[:199], m.CoverageGap(
            code="additional_gaps", detail=f"{len(gaps) - 199} additional gaps are available in paginated target/inventory records.",
        ))

    def _coverage(self, context: m.MonitoringContext) -> m.CoverageView:
        control = self._control(context)
        now = self._now()
        discovered = self._backend.count("inventory", context, filters={"status_in": ("present", "unknown")})
        inventory_rows = self._backend.scan("inventory", context, limit=SCAN_BUDGET, filters={"status_in": ("present", "unknown")})
        items = [self._decode(row, m.InventoryItem) for row in inventory_rows]
        target_rows = self._backend.scan("target", context, limit=SCAN_BUDGET, filters={"status_in": ("current", "paused", "review_required")})
        targets = [self._effective_target(self._decode(row, m.MonitoringTarget)) for row in target_rows]
        generations = self._all("generation", context, m.InventoryGeneration)
        scopes = self._all("scope", context, m.ScopePolicy, budget=1_000)
        latest_generations = {}
        for generation in generations:
            selector_key = generation.enumeration + ":" + _json(generation.selector.model_dump(mode="json"))
            prior_generation = latest_generations.get(selector_key)
            if prior_generation is None or generation.started_at >= prior_generation.started_at:
                latest_generations[selector_key] = generation
        gaps = [
            gap for generation in latest_generations.values()
            if generation.completeness != "complete" for gap in generation.gaps
        ]
        # These counters cover the deployment's inventory, not just enabled
        # scopes. A complete scope preview cannot certify an unfinished estate.
        complete = self._inventory_complete(scopes, generations) and all(
            generation.completeness == "complete" for generation in latest_generations.values()
        )
        if discovered > SCAN_BUDGET:
            complete = False
            gaps.append(m.CoverageGap(code="coverage_budget", detail="Coverage counters below are a bounded verified subset; page through remaining inventory."))
        access = 0
        supported = 0
        for item in items:
            if item.target is None:
                continue
            supported += 1
            capability = self._get("target_capability", item.target.key, context, m.CapabilityObservation)
            if capability and capability.read_status == "verified" and capability.expires_at > now:
                access += 1
        if not complete:
            gaps.append(m.CoverageGap(code="inventory_incomplete", detail="The active scope has incomplete or unknown inventory."))
        if self._pending_frontier_count(context):
            gaps.append(m.CoverageGap(
                code="pending_validation", detail="Accepted intent or raw intake awaits controller validation; affected new actions are fenced.",
            ))
        if access < supported:
            gaps.append(m.CoverageGap(code="access_unverified", detail=f"{supported - access} discovered supported items lack a current read probe."))
        connectors = self._all("connector", context, m.OwnedConnectorManifest)
        for connector in connectors:
            gaps.extend(connector.gaps)
            heartbeats = self._all(
                "receiver_heartbeat", context, ReceiverHeartbeat,
                filters={"parent_key": connector.connector_id},
            )
            if heartbeats and not any(
                heartbeat.state == "running" and heartbeat.transport_connected
                and now - heartbeat.observed_at <= timedelta(seconds=120)
                for heartbeat in heartbeats
            ):
                gaps.append(m.CoverageGap(
                    code="receiver_not_running",
                    detail=f"Connector {connector.connector_id} has no current running, connected receiver heartbeat.",
                ))
            if connector.state in {"planned", "provisioning"}:
                gaps.append(m.CoverageGap(
                    code="connector_configuring", detail=f"Connector {connector.connector_id} has not completed provisioning/delivery verification.",
                ))
            if connector.state == "ready" and (
                connector.last_receiver_activity_at is None
                or now - connector.last_receiver_activity_at > timedelta(seconds=120)
            ):
                gaps.append(m.CoverageGap(code="stale_receiver", detail=f"Connector {connector.connector_id} has no recent receiver activity."))
        for stream_start in self._all("stream_start", context, StreamStart):
            gaps.extend(stream_start.gaps)
        checkpoints = self._all("rest_checkpoint", context, m.RestCheckpoint)
        for checkpoint in checkpoints:
            if checkpoint.powerbi_window_id is not None:
                staged = self._get("powerbi_window", checkpoint.powerbi_window_id, context, m.PowerBIWindowState)
                if staged is None:
                    raise MonitoringUnavailable("REST checkpoint references a missing Power BI validation window")
                gaps.extend(staged.gaps)
        completed_generations = [generation.completed_at for generation in generations if generation.completeness == "complete" and generation.completed_at]
        completed_windows = [checkpoint.coverage_through for checkpoint in checkpoints if checkpoint.coverage_through]
        receiver_times = [connector.last_receiver_activity_at for connector in connectors if connector.last_receiver_activity_at]
        due_times = [target.next_poll_at for target in targets if target.observation.enabled and target.next_poll_at]
        current = [target for target in targets if target.state == "current" and target.observation.enabled]
        return m.CoverageView(
            **_stamp(control), revision=control.revision, as_of=now,
            inventory_completeness="complete" if complete else "partial",
            capability_completeness="complete" if access == supported and discovered <= SCAN_BUDGET else "partial",
            scope_item_count=discovered if complete else None, discovered_count=discovered,
            access_verified_count=access, admitted_count=sum(target.admission_basis != "pending_review" for target in targets),
            current_count=len(current), action_enabled_count=sum(target.action.enabled for target in current),
            unsupported_count=sum(item.target is None for item in items),
            backlog_count=self._backend.count("work", context, filters={"status_in": ("queued", "waiting", "leased", "finalizing")}),
            last_inventory_completed_at=max(completed_generations, default=None),
            last_poll_window_end=min(completed_windows, default=None),
            last_receiver_activity_at=max(receiver_times, default=None), next_due_at=min(due_times, default=None),
            gaps=self._bounded_gaps(gaps),
        )

    @atomic()
    def coverage(self, context: m.MonitoringContext) -> m.CoverageView:
        return self._coverage(context)

    @atomic()
    def snapshot(self, context: m.MonitoringContext) -> m.MonitoringSnapshot:
        return m.MonitoringSnapshot(control=self._control(context), coverage=self._coverage(context))

    def _save_work(self, work: m.MonitoringWork) -> m.MonitoringWork:
        return self._put(
            "work", work.work_id, work, work, status=work.state, work_kind=work.kind,
            workspace_id=work.target.workspace_id if work.target else None,
            workload=work.target.workload if work.target else None,
            target_key=work.target.key if work.target else None,
            parent_key=work.execution.key if work.execution else None,
            due_at=work.lease.expires_at if work.lease else work.due_at,
        )

    def _schedule_poll(self, target: m.MonitoringTarget) -> m.MonitoringWork:
        progress = self._get("poll_schedule", target.key, target.identity, m.PollSchedule)
        due = (
            progress.next_poll_at if progress is not None and progress.policy_revision == target.policy_revision
            else target.next_poll_at or self._now()
        )
        return self._enqueue(m.MonitoringWorkDraft(
            **_stamp(target.identity), work_id=stable_id(target.identity, f"poll:{target.key}:{due.isoformat()}"),
            kind="poll", policy_revision=target.policy_revision, due_at=due, created_at=self._now(),
            reason="Durable due monitoring poll.", target=target.identity,
        ))

    def _schedule_connector(
        self, target: m.MonitoringTarget, control: m.DeploymentControl,
    ) -> m.MonitoringWork | None:
        if self.component == "controller" and len(self._registered_connectors(control)) == 1:
            return None
        capability = self._get("target_capability", target.key, control, m.CapabilityObservation)
        if capability is None or capability.event_status != "verified":
            return None
        connector_id = stable_id(control, f"connector-target:{target.key}")
        matching = [
            connector for connector in self._all("connector", control, m.OwnedConnectorManifest)
            if connector.state != "deleted" and any(source.target == target.identity for source in connector.sources)
        ]
        if matching:
            connector = matching[0]
            if connector.state == "ready":
                return None
            connector_id = connector.connector_id
        else:
            connector = self._get("connector", connector_id, control, m.OwnedConnectorManifest)
            if connector is None:
                self._put("connector", connector_id, control, m.OwnedConnectorManifest(
                    **_stamp(control), connector_id=connector_id, ownership_id=control.epoch,
                    revision=1, policy_revision=control.revision,
                    name="Pending monitoring connector", sources=(), desired_definition={},
                    state="planned", updated_at=self._now(),
                ), status="planned", target_key=target.key)
        return self._enqueue(m.MonitoringWorkDraft(
            **_stamp(control), work_id=stable_id(control, f"connector:{connector_id}:{control.revision}"),
            kind="connector_reconcile", policy_revision=control.revision,
            created_at=self._now(), due_at=self._now(), target=target.identity, connector_id=connector_id,
            reason="Provision only an owned verified event topology; pending resource IDs are unknown.",
        ))

    def _enqueue(self, draft: m.MonitoringWorkDraft) -> m.MonitoringWork:
        expanded = self._inventory_drafts(draft)
        if len(expanded) != 1 or expanded[0] != draft:
            work = self._enqueue(expanded[0])
            for child in expanded[1:]:
                self._enqueue(child)
            return work
        control = self._control(draft)
        selector = self._inventory_scope(draft)
        prior = self._get("work", draft.work_id, draft, m.MonitoringWork)
        if prior is not None:
            recovering = (
                prior.kind == "verify_action" and draft.kind in {"triage", "deferred_retry"}
                and prior.action_reservation_id is not None
            )
            if (
                prior.target != draft.target or prior.execution != draft.execution
                or prior.scope_id != draft.scope_id
                or prior.reconcile_request_id != draft.reconcile_request_id
                or prior.reconcile_producer != draft.reconcile_producer
                or self._inventory_scope(prior) != selector
                or (not recovering and (
                    prior.kind != draft.kind or prior.action_reservation_id != draft.action_reservation_id
                ))
            ):
                raise MonitoringConflict("Work identity was reused for another operation")
            return prior
        followup = draft.kind in {"verify_action", "finalize"}
        reconciliation = draft.kind == "reconcile_state"
        if control.maintenance and not (followup or reconciliation):
            raise MonitoringConflict("Maintenance stops new monitoring work")
        if not followup and draft.policy_revision != control.revision:
            raise MonitoringConflict("Queued work must use the current policy revision")
        if reconciliation:
            request = self._get(
                f"{draft.reconcile_producer}_reconcile_request", draft.reconcile_request_id,
                draft, m.ReconciliationRequest,
            )
            if request is None or request.work_id != draft.work_id or request.target != draft.target:
                raise MonitoringConflict("Reconciliation work requires its immutable producer handoff")
        elif draft.kind == "inventory":
            if selector is None and draft.scope_id is None:
                raise MonitoringConflict("Discovery work needs an explicit selector or saved scope")
            if draft.scope_id is not None and self._get("scope", draft.scope_id, draft, m.ScopePolicy) is None:
                raise MonitoringConflict("Inventory work references an absent saved scope")
        elif draft.kind == "capability_probe":
            item = self._get(
                "inventory", f"{draft.target.workspace_id}:{draft.target.item_id}", draft, m.InventoryItem,
            )
            if item is None or item.target != draft.target or item.state != "present":
                raise MonitoringConflict("Capability work requires discovered inventory")
        elif draft.kind == "connector_reconcile":
            if self._get("connector", draft.connector_id, draft, m.OwnedConnectorManifest) is None:
                raise MonitoringConflict("Connector work requires an owned manifest")
        elif followup:
            if draft.kind == "verify_action":
                reservation = self._get("action", draft.action_reservation_id, draft, m.ActionReservation)
                if reservation is None or reservation.request.source_execution != draft.execution:
                    raise MonitoringConflict("Verification must retain the original reserved source")
        elif self._target(draft.target) is None:
            raise MonitoringConflict("Target is not currently admitted for monitoring work")
        if draft.execution is not None:
            if draft.execution.target.workload == "powerbi" and draft.execution.run_id_kind != "powerbi_request":
                raise MonitoringConflict("Resolve Power BI history-ID aliases to the authoritative request ID before admission")
            link_key = self._work_link_key(draft)
            existing = self._get("source_work", link_key, draft, WorkLink)
            if existing is not None:
                work = self._get("work", existing.work_id, draft, m.MonitoringWork)
                if work is None:
                    raise MonitoringUnavailable("Source/work link has no durable work record")
                if draft.kind != "verify_action" or work.state not in {"completed", "dispositioned"}:
                    return work
            processed = self._get("source_disposition", draft.execution.key, draft, m.ProcessedSourceRecord)
            if processed is not None and draft.kind == "triage":
                raise MonitoringConflict("This source execution already has a durable terminal disposition")
        work = m.MonitoringWork(
            **draft.model_dump(), revision=1, state="queued",
            retry_attempt=1 if draft.kind == "deferred_retry" else 0,
        )
        saved = self._save_work(work)
        if draft.execution is not None:
            self._put(
                "source_work", self._work_link_key(draft), draft,
                WorkLink(work_id=work.work_id, execution=draft.execution), parent_key=draft.execution.key,
            )
        return saved

    def _inventory_drafts(self, draft: m.MonitoringWorkDraft) -> tuple[m.MonitoringWorkDraft, ...]:
        if draft.kind != "inventory" or draft.discovery_selector is not None or draft.scope_id is None:
            return (draft,)
        scope = self._get("scope", draft.scope_id, draft, m.ScopePolicy)
        if scope is None:
            raise MonitoringConflict("Inventory work references an absent saved scope")
        if not scope.enabled:
            raise MonitoringConflict("Scope inventory requires an enabled current saved scope")
        selectors = {
            _json(rule.selector.model_dump(mode="json")): rule.selector
            for rule in scope.rules if rule.effect == "include"
        }
        if not selectors:
            raise MonitoringConflict("Scope inventory has no explicit inclusion selector")
        return tuple(_update(
            draft, discovery_selector=selectors[key],
            work_id=draft.work_id if index == 0 else stable_id(draft, f"selector:{draft.work_id}:{key}"),
        ) for index, key in enumerate(sorted(selectors)))

    def _inventory_scope(self, work: m.MonitoringWorkDraft) -> m.ScopeSelector | None:
        selector = work.discovery_selector
        if selector is not None and (
            not isinstance(selector, m.ScopeSelector)
            or work.kind != "inventory" or selector.tenant_id != work.tenant_id
        ):
            raise MonitoringConflict("Discovery selection must be a typed selector for this tenant")
        return selector

    def _work_link_key(self, work: m.MonitoringWorkDraft) -> str:
        if work.kind == "verify_action":
            return f"verify_action:{work.action_reservation_id}"
        return f"{work.kind}:{work.execution.key}"

    def _schedule_verification(self, action: m.ActionReservation) -> m.MonitoringWork:
        control = self._control(action.request.expected)
        due = action.next_verification_at or self._now()
        work = self._enqueue(m.MonitoringWorkDraft(
            **_stamp(control),
            work_id=stable_id(control, f"verify:{action.reservation_id}:{action.revision}"),
            kind="verify_action", policy_revision=control.revision, created_at=self._now(), due_at=due,
            target=action.request.source_execution.target, execution=action.request.source_execution,
            action_reservation_id=action.reservation_id,
            reason="Reconcile the existing external effect; never submit another POST.",
        ))
        if work.state in {"queued", "waiting"} and work.due_at != due:
            work = self._save_work(_update(work, due_at=due, revision=work.revision + 1))
        return work

    @atomic(write=True)
    def enqueue_work(self, work: m.MonitoringWorkDraft) -> m.MonitoringWork:
        return self._idempotent("enqueue", work.work_id, work, work, m.MonitoringWork, lambda: self._enqueue(work))

    @atomic(write=True)
    def request_discovery(
        self, expected: m.RegistryVersion, selector: m.ScopeSelector, *, request_id: str,
    ) -> m.MonitoringWork:
        draft = m.MonitoringWorkDraft(
            **_stamp(expected), work_id=m.canonical_id(request_id), kind="inventory",
            policy_revision=expected.revision, created_at=self._now(), due_at=self._now(),
            discovery_selector=selector, reason="Explicit discovery request; no active scope is required.",
        )
        payload = {"expected": expected.model_dump(mode="json"), "selector": selector.model_dump(mode="json")}
        def apply() -> m.MonitoringWork:
            control = self._current(expected, intake=True)
            if self.component == "fixture":
                return self._enqueue(draft)
            return self._request_reconciliation(
                control, request_id=draft.work_id, topic="discovery", reference_id=draft.work_id,
                fingerprint=key_digest(_json(payload)), payload={"selector": selector.model_dump(mode="json")},
            )
        return self._idempotent(
            "discovery", draft.work_id, expected,
            payload, m.MonitoringWork, apply,
        )

    @atomic()
    def get_work(self, context: m.MonitoringContext, work_id: str) -> m.MonitoringWork | None:
        self._control(context)
        return self._get("work", m.canonical_id(work_id), context, m.MonitoringWork)

    def _owned_work(
        self, context: m.MonitoringContext, work_id: str, lease: m.LeaseToken,
        expected_revision: int | None = None,
    ) -> m.MonitoringWork:
        self._control(context)
        work = self._get("work", work_id, context, m.MonitoringWork)
        current = self._backend.get_lease(context, m.work_key(context, work_id))
        if (
            work is None or work.state not in {"leased", "finalizing"} or current is None
            or current.expires_at <= self._now()
            or (current.owner_id, current.fence) != (lease.owner_id, lease.fence)
            or work.lease is None or work.lease.fence != current.fence
        ):
            raise MonitoringLeaseLost("Work ownership expired or another owner won its fence")
        self._authorize_work(work.kind)
        if expected_revision is not None and work.revision != expected_revision:
            raise MonitoringConflict("Work revision changed")
        if work.kind in {"triage", "deferred_retry", "verify_action", "finalize"}:
            controller = self._backend.get_lease(context, f"controller:{work.target.key}")
            if controller is None or controller.owner_id != work.work_id or controller.expires_at <= self._now():
                raise MonitoringLeaseLost("Target controller ownership expired or changed")
        return work

    def _work_reservation(self, work: m.MonitoringWork) -> m.ActionReservation | None:
        if work.kind not in {"triage", "deferred_retry", "verify_action", "finalize"}:
            return None
        rows = self._backend.scan("action", work, limit=2, filters={"parent_key": work.key, "status_in": ("reserved", "submitted", "uncertain")})
        if len(rows) > 1:
            raise MonitoringUnavailable("Work has multiple active action reservations")
        return self._decode(rows[0], m.ActionReservation) if rows else None

    @atomic(write=True)
    def claim_work(self, request: m.WorkClaimRequest) -> tuple[m.MonitoringWork, ...]:
        for kind in request.kinds:
            self._authorize_work(kind)
        control = self._control(request)
        cursor_key = "fair-work" if self.component == "fixture" else f"fair-work:{self.component}"
        cursor = self._get("scheduler", cursor_key, request, FairCursor) or FairCursor()
        candidates = self._backend.due(request, after_workspace=cursor.after_workspace)
        claimed = []
        last_workspace = cursor.after_workspace
        for record in candidates:
            work = self._decode(record, m.MonitoringWork)
            last_workspace = work.target.workspace_id if work.target else ""
            if work.retry_of is not None:
                parent = self._get("action", work.retry_of, work, m.ActionReservation)
                if (
                    parent is None or parent.state != "rejected" or parent.retry_work_id != work.work_id
                    or parent.retry_attempt + 1 != work.retry_attempt
                ):
                    raise MonitoringUnavailable("Successor retry lineage is missing or inconsistent")
                parent_work = self._get("work", parent.request.work_id, work, m.MonitoringWork)
                if parent_work is None:
                    raise MonitoringUnavailable("Rejected action has no originating work")
                if parent_work.state != "completed":
                    continue
            followup = work.kind in {"verify_action", "finalize", "reconcile_state"}
            reservation = self._work_reservation(work)
            if reservation is not None:
                work = _update(work, kind="verify_action", action_reservation_id=reservation.reservation_id)
                followup = True
            elif work.action_reservation_id is not None:
                recorded = self._get("action", work.action_reservation_id, work, m.ActionReservation)
                if recorded is not None and recorded.state == "rejected":
                    work = _update(work, kind="finalize")
                    followup = True
            elif work.state == "finalizing":
                work = _update(work, kind="finalize")
                followup = True
            if control.maintenance and not followup:
                continue
            stale = work.policy_revision != control.revision
            admitted = work.target is None or self._target(work.target) is not None
            if not followup and work.kind not in {"inventory", "capability_probe", "connector_reconcile"} and (stale or not admitted):
                self._dispose_unclaimed(work, "out_of_scope", "Queued work lost current admission; no effect was submitted.")
                continue
            lease = self._backend.acquire_lease(request, work.key, request.owner_id, request.lease_seconds)
            if lease is None:
                continue
            if work.kind in {"triage", "deferred_retry", "verify_action", "finalize"}:
                controller_lease = self._backend.acquire_lease(
                    request, f"controller:{work.target.key}", work.work_id, request.lease_seconds,
                )
                if controller_lease is None:
                    self._backend.release_lease(lease)
                    continue
            claimed.append(self._save_work(_update(
                work, state="leased", lease=lease, attempts=work.attempts + 1, revision=work.revision + 1,
            )))
        if candidates:
            self._put("scheduler", cursor_key, request, FairCursor(after_workspace=last_workspace))
        return tuple(claimed)

    @atomic(write=True)
    def renew_lease(self, request: m.LeaseRenewal) -> m.LeaseToken:
        self._control(request.lease)
        if request.lease.resource_key.startswith("work:v1:"):
            work_id = request.lease.resource_key.rsplit(":", 1)[1]
            work = self._owned_work(request.lease, work_id, request.lease)
            renewed = self._backend.renew_lease(request)
            if work.kind in {"triage", "deferred_retry", "verify_action", "finalize"}:
                controller = self._backend.get_lease(work, f"controller:{work.target.key}")
                if controller is None or controller.owner_id != work.work_id:
                    raise MonitoringLeaseLost("Target controller ownership changed")
                self._backend.renew_lease(m.LeaseRenewal(lease=controller, lease_seconds=request.lease_seconds))
            self._save_work(_update(work, lease=renewed))
            return renewed
        if request.lease.resource_key.startswith("partition:v1:"):
            if self.component not in {"worker", "fixture"}:
                raise MonitoringComponentDenied("Only the worker owns receiver partition leases")
            ownership = self._get(
                "partition_ownership", request.lease.resource_key, request.lease, PartitionOwnership,
            )
            if ownership is None or ownership.lease is None or (
                ownership.lease.owner_id, ownership.lease.fence
            ) != (request.lease.owner_id, request.lease.fence):
                raise MonitoringLeaseLost("Partition ownership is absent, released or superseded")
            self._backend.operation_identity("partition_renew", request.lease.resource_key)
            renewed = self._backend.renew_lease(request)
            self._save_partition_ownership(ownership.partition, renewed)
            return renewed
        raise MonitoringComponentDenied("A runtime caller cannot renew an untyped resource lease")

    def _source_disposition(
        self, execution: m.SourceExecutionIdentity, disposition: str, detail: str,
        *, work_id: str | None = None, finalization_id: str | None = None,
        incident_identity: m.IncidentIdentity | None = None, observation: m.SourceRunObservation | None = None,
    ) -> m.ProcessedSourceRecord:
        prior = self._get("source_disposition", execution.key, execution.target, m.ProcessedSourceRecord)
        if prior is not None:
            return prior
        result = m.ProcessedSourceRecord(
            execution=execution, disposition=disposition, detail=detail, recorded_at=self._now(),
            work_id=work_id, finalization_id=finalization_id, incident_identity=incident_identity,
        )
        saved = self._put(
            "source_disposition", execution.key, execution.target, result,
            target_key=execution.target.key, status=disposition,
        )
        self._backend.mark_processed(execution.key, self._now())
        return saved

    def _dispose_unclaimed(self, work: m.MonitoringWork, disposition: str, detail: str) -> m.MonitoringWork:
        if work.execution is not None:
            self._source_disposition(work.execution, disposition, detail, work_id=work.work_id)
        if work.lease is not None:
            self._backend.release_lease(work.lease)
        self._release_controller(work)
        return self._save_work(_update(
            work, state="dispositioned", lease=None, completed_at=self._now(),
            disposition=detail, revision=work.revision + 1,
        ))

    def _noneffect_source(self, work, disposition):
        source = self._get("source", work.execution.key, work, m.SourceRunObservation)
        if source is None or self._submitted_action_owner(work.execution) is not None:
            raise MonitoringConflict("Non-effect disposition requires an exact source that is not a submitted action")
        control = self._control(work)
        if control.maintenance:
            raise MonitoringConflict("New non-effect disposition requires active current policy")
        if disposition not in {"historical", "out_of_scope"} and source.authority not in {"rest", "fixture"}:
            raise MonitoringConflict("Operational non-effect disposition requires authoritative source evidence")
        head = self._get("source_head", work.target.key, work, m.SourceRunObservation)
        allowed = (
            disposition == "historical" and source.started_at is not None and source.started_at < control.activation_cutoff
            or disposition == "out_of_scope" and self._target(work.target) is None
            or disposition == "cancelled" and source.status == "cancelled"
            or disposition == "superseded" and head is not None and head.started_at is not None
            and source.started_at is not None and head.started_at > source.started_at
            or disposition == "unsupported" and work.target.workload == "fabric_pipeline" and (
                source.invocation != "scheduled" or source.job_type != "Pipeline" or source.status not in {"failed", "unknown"}
            )
        )
        if not allowed:
            raise MonitoringConflict("The requested non-effect disposition is not established by deterministic evidence")
        return source

    @atomic(write=True)
    def disposition_work(self, request: m.WorkDispositionRequest) -> m.MonitoringWork:
        def apply() -> m.MonitoringWork:
            work = self._owned_work(request, request.work_id, request.lease, request.expected_work_revision)
            if self._work_reservation(work) is not None:
                raise MonitoringConflict("Effectful work retains its fence and must be verified/finalized")
            if request.disposition == "retry":
                self._backend.release_lease(work.lease)
                self._release_controller(work)
                return self._save_work(_update(
                    work, state="waiting", lease=None, due_at=request.retry_at,
                    disposition=request.detail, revision=work.revision + 1,
                ))
            if work.kind in {"triage", "deferred_retry", "verify_action", "finalize"}:
                self._noneffect_source(work, request.disposition)
            return self._dispose_unclaimed(work, request.disposition, request.detail)
        return self._idempotent("work_disposition", request.request_id, request, request, m.MonitoringWork, apply)

    @atomic(write=True)
    def complete_collection_work(
        self, context: m.MonitoringContext, *, work_id: str, lease: m.LeaseToken, expected_work_revision: int,
    ) -> m.MonitoringWork:
        work = self._owned_work(context, m.canonical_id(work_id), lease, expected_work_revision)
        if work.kind not in {"inventory", "capability_probe", "connector_reconcile"}:
            raise MonitoringConflict("Only evidence collection uses this completion boundary")
        if self.component != "fixture" and work.kind in {"inventory", "capability_probe"}:
            accepted = self._all("worker_reconcile_request", context, m.ReconciliationRequest)
            if not any(
                entry.producer_commit is not None
                and entry.producer_commit.work_id == work.work_id
                and entry.producer_commit.lease.owner_id == lease.owner_id
                and entry.producer_commit.lease.fence == lease.fence
                for entry in accepted
            ):
                raise MonitoringConflict("Collection completion requires acceptance under this exact work fence")
        if work.kind == "inventory":
            explicit = self._inventory_scope(work)
            selectors = [explicit] if explicit else []
            if work.scope_id is not None:
                scope = self._get("scope", work.scope_id, context, m.ScopePolicy)
                if scope is None:
                    raise MonitoringConflict("The discovery scope is absent")
                selectors.extend(rule.selector for rule in scope.rules if rule.effect == "include")
            generations = self._all("generation", context, m.InventoryGeneration)
            if not selectors or not all(any(
                generation.selector == selector and generation.completeness == "complete"
                and generation.completed_at is not None and generation.started_at >= work.created_at
                for generation in generations
            ) for selector in selectors):
                raise MonitoringConflict("Discovery cannot complete before its declared inventory is durable")
        elif work.kind == "capability_probe":
            capabilities = self._all(
                "capability", context, m.CapabilityObservation, filters={"target_key": work.target.key},
            )
            if not any(capability.checked_at >= work.created_at for capability in capabilities):
                raise MonitoringConflict("Capability work has no durable current probe result")
        elif self.component == "fixture":
            connector = self._get("connector", work.connector_id, context, m.OwnedConnectorManifest)
            if connector is None or connector.state in {"planned", "provisioning"} or connector.updated_at < work.created_at:
                raise MonitoringConflict("Connector work has no durable provisioning disposition")
        else:
            control = self._control(context)
            accepted = self._all("worker_reconcile_request", context, m.ReconciliationRequest)
            eligible = False
            for entry in accepted:
                if (
                    entry.topic != "connector" or entry.producer_commit is None
                    or entry.producer_commit.work_id != work.work_id
                    or entry.producer_commit.lease.owner_id != lease.owner_id
                    or entry.producer_commit.lease.fence != lease.fence
                ):
                    continue
                original = self._connector_observation_result(context, entry.request_id)
                if (
                    original.collection_completion_eligible and original.work_revision <= work.revision
                    and original.connector_id == work.connector_id
                    and original.observation.policy_revision == control.revision and not control.maintenance
                ):
                    eligible = True
            if not eligible:
                raise MonitoringConflict("Connector completion requires an eligible original observation under this work fence")
        self._backend.release_lease(work.lease)
        return self._save_work(_update(
            work, state="completed", lease=None, completed_at=self._now(), revision=work.revision + 1,
        ))

    def _save_source(self, observation: m.SourceRunObservation) -> m.SourceRunObservation:
        if not self._backend.fixture and observation.authority == "fixture":
            raise MonitoringConflict("Fixture source evidence cannot authorize live monitoring")
        context = observation.execution.target
        if observation.observed_at > self._now():
            raise MonitoringConflict("Source observation time is in the database clock's future")
        prior = self._get("source", observation.key, context, m.SourceRunObservation)
        if prior is not None and (
            (
                (prior.authority == observation.authority or (
                    prior.authority in {"rest", "fixture"} and observation.authority in {"rest", "fixture"}
                )) and prior.observed_at > observation.observed_at
            )
            or (prior.authority in {"rest", "fixture"} and observation.authority == "transport")
        ):
            return prior
        saved = self._put(
            "source", observation.key, context, observation, target_key=context.key,
            status=observation.status, due_at=observation.started_at, parent_key=context.key,
        )
        head = self._get("source_head", context.key, context, m.SourceRunObservation)
        if saved.authority in {"rest", "fixture"} and observation.started_at is not None and (
            head is None or head.started_at is None or observation.started_at >= head.started_at
            or head.execution == observation.execution
        ):
            self._put("source_head", context.key, context, saved, target_key=context.key)
        return saved

    @atomic(write=True)
    def observe_source(
        self, observation: m.SourceRunObservation, *, work_id: str, lease: m.LeaseToken,
    ) -> m.SourceRunObservation:
        work = self._owned_work(observation.execution.target, m.canonical_id(work_id), lease)
        if work.execution != observation.execution and work.target != observation.execution.target:
            raise MonitoringConflict("Work cannot record another target's source evidence")
        if observation.authority == "transport":
            raise MonitoringConflict("Controller source refresh requires authoritative REST evidence")
        return self._save_source(observation)

    @atomic()
    def get_source(self, execution: m.SourceExecutionIdentity) -> m.SourceRunObservation | None:
        self._control(execution.target)
        return self._get("source", execution.key, execution.target, m.SourceRunObservation)

    @atomic()
    def get_source_disposition(self, execution: m.SourceExecutionIdentity) -> m.ProcessedSourceRecord | None:
        self._control(execution.target)
        return self._get("source_disposition", execution.key, execution.target, m.ProcessedSourceRecord)

    def _submitted_action_owner(self, execution: m.SourceExecutionIdentity) -> ActionOwner | None:
        return self._get("submitted_action", execution.key, execution.target, ActionOwner)

    def _ingest(self, observation: m.SourceRunObservation, control: m.DeploymentControl) -> str | None:
        if _stamp(observation.execution.target) != _stamp(control):
            raise MonitoringConflict("Source evidence belongs to another tenant or epoch")
        if observation.execution.target.workload == "powerbi" and observation.execution.run_id_kind != "powerbi_request":
            raise MonitoringConflict("Power BI aliases must be resolved before common admission")
        submitted = self._submitted_action_owner(observation.execution)
        target = self._target(observation.execution.target)
        if target is None and submitted is None:
            self._source_disposition(
                observation.execution, "out_of_scope", "No current admitted target.", observation=observation,
            )
            return None
        saved = self._save_source(observation)
        if submitted is not None:
            action = self._get("action", submitted.reservation_id, control, m.ActionReservation)
            if action is None:
                raise MonitoringUnavailable("Submitted-execution correlation has no action reservation")
            if action.state.startswith("verified_"):
                current = self._get("source_work", f"verify_action:{action.reservation_id}", control, WorkLink)
                if current is not None:
                    existing = self._get("work", current.work_id, control, m.MonitoringWork)
                    if existing is not None and existing.state in {"completed", "dispositioned"}:
                        return None
            work = self._schedule_verification(_update(action, next_verification_at=self._now()))
            return work.work_id
        if saved.started_at is not None and saved.started_at < control.activation_cutoff:
            self._source_disposition(
                saved.execution, "historical", "Source execution predates the activation cutoff.", observation=saved,
            )
            return None
        if self._get("source_disposition", saved.key, control, m.ProcessedSourceRecord) is not None:
            return None
        if saved.authority in {"rest", "fixture"}:
            if saved.status not in {"failed", "unknown"}:
                return None
            if saved.execution.target.workload == "fabric_pipeline" and not saved.failed_scheduled_pipeline:
                self._source_disposition(
                    saved.execution, "unsupported", "Not a terminal failed scheduled supported pipeline execution.",
                    observation=saved,
                )
                return None
        draft = m.MonitoringWorkDraft(
            **_stamp(control), work_id=stable_id(control, f"triage:{saved.key}"), kind="triage",
            policy_revision=control.revision, created_at=self._now(), due_at=self._now(),
            target=saved.execution.target, execution=saved.execution, reason="Accepted exact source execution needs controller triage.",
        )
        return self._enqueue(draft).work_id

    def _powerbi_alias(
        self, context: m.MonitoringContext, window_id: str, namespace: str,
        identifier: str, mapped: str | None,
    ) -> m.PowerBIAliasState:
        key = f"{window_id}:{namespace}:{identifier}"
        prior = self._get("powerbi_alias", key, context, m.PowerBIAliasState)
        values = set(prior.mapped_ids if prior else ())
        if mapped is not None:
            values.add(mapped)
        updated = m.PowerBIAliasState(
            window_id=window_id, namespace=namespace, identifier=identifier,
            mapped_ids=tuple(sorted(values)),
        )
        if prior != updated:
            self._put("powerbi_alias", key, context, updated, parent_key=window_id)
        return updated

    def _resolve_powerbi_row(
        self, context: m.MonitoringContext, window_id: str, row: m.PowerBIWindowRow,
    ) -> m.SourceRunObservation | None:
        execution = row.observation.execution
        candidates = set()
        if row.refresh_id is not None:
            mapping = self._get(
                "powerbi_alias", f"{window_id}:refresh:{row.refresh_id}", context, m.PowerBIAliasState,
            )
            if mapping is not None:
                candidates.update(mapping.mapped_ids)
        if execution.run_id_kind == "powerbi_request":
            candidates.add(execution.run_id)
        if len(candidates) != 1:
            return None
        request_id = next(iter(candidates))
        reverse = self._get(
            "powerbi_alias", f"{window_id}:request:{request_id}", context, m.PowerBIAliasState,
        )
        if reverse is None or len(reverse.mapped_ids) > 1:
            return None
        for refresh_id in reverse.mapped_ids:
            mapping = self._get(
                "powerbi_alias", f"{window_id}:refresh:{refresh_id}", context, m.PowerBIAliasState,
            )
            if mapping is None or mapping.mapped_ids != (request_id,):
                return None
        return _update(
            row.observation,
            execution=m.SourceExecutionIdentity(
                target=execution.target, run_id_kind="powerbi_request", run_id=request_id,
            ),
            evidence={**row.observation.evidence, "request_id": request_id},
        )

    def _powerbi_window_id(self, request: m.RestPageRequest) -> str:
        return stable_id(
            request.target, f"powerbi-window:{request.poll_work_id}:{request.target.key}:{request.window.model_dump_json()}",
        )

    def _stage_powerbi_page(
        self, request: m.RestPageRequest, control: m.DeploymentControl, *, stage_rows: bool = True,
    ) -> tuple[m.PowerBIWindowState, list[str], list[str]]:
        window_id = self._powerbi_window_id(request)
        prior = self._get("powerbi_window", window_id, control, m.PowerBIWindowState)
        if prior is not None and prior.state != "collecting":
            if not stage_rows:
                return prior, [], []
            raise MonitoringConflict("A closed Power BI source window cannot accept another page")
        keys = []
        rows = request.powerbi_rows if stage_rows else self._all(
            "powerbi_window_row", control, m.PowerBIWindowRow,
            filters={"parent_key": window_id}, budget=m.MAX_POWERBI_WINDOW_ROWS,
        )
        for index, row in enumerate(rows):
            content = {
                "refresh_id": row.refresh_id,
                "observation": row.observation.model_dump(mode="json", exclude={"observed_at"}),
            }
            row_key = f"{window_id}:{key_digest(_json(content))}"
            if stage_rows:
                self._put(
                    "powerbi_window_row", row_key, control, row,
                    parent_key=window_id, target_key=request.target.key,
                )
                receipt_key = f"rest:{request.page_id}:powerbi:{index}"
                self._put("rest_powerbi_row", receipt_key, control, row, parent_key=request.page_id)
                keys.append(receipt_key)
            request_id = (
                row.observation.execution.run_id
                if row.observation.execution.run_id_kind == "powerbi_request" else None
            )
            if row.refresh_id is not None:
                self._powerbi_alias(control, window_id, "refresh", row.refresh_id, request_id)
            if request_id is not None:
                self._powerbi_alias(control, window_id, "request", request_id, row.refresh_id)
        for quarantine in request.quarantines if stage_rows else ():
            self._put(
                "powerbi_window_quarantine",
                f"{window_id}:{request.page_id}:{key_digest(quarantine.observation_id)}",
                control, quarantine, parent_key=window_id,
            )
        row_count = self._backend.count("powerbi_window_row", control, filters={"parent_key": window_id})
        if row_count > m.MAX_POWERBI_WINDOW_ROWS:
            raise MonitoringConflict("Power BI source window exceeded its bounded staging limit")
        quarantined = self._backend.count("powerbi_window_quarantine", control, filters={"parent_key": window_id})
        state = "collecting"
        gaps = []
        work_ids = []
        resolved: dict[str, m.SourceRunObservation] = {}
        if request.powerbi_window_complete:
            rows = self._all(
                "powerbi_window_row", control, m.PowerBIWindowRow,
                filters={"parent_key": window_id}, budget=m.MAX_POWERBI_WINDOW_ROWS,
            )
            invalid = []
            for row in rows:
                observation = self._resolve_powerbi_row(control, window_id, row)
                if observation is None:
                    invalid.append(row)
                else:
                    previous = resolved.get(observation.key)
                    if previous is None or observation.observed_at >= previous.observed_at:
                        resolved[observation.key] = observation
            if invalid or quarantined:
                state = "quarantined"
                for row in invalid if stage_rows else ():
                    identity = key_digest(row.model_dump_json())
                    self._put("powerbi_window_quarantine", f"{window_id}:alias:{identity}", control, m.QuarantineDisposition(
                        observation_id=identity, reason="ambiguous_execution",
                        detail="The complete source window does not establish one refresh/request alias.",
                        metadata={"refresh_id": row.refresh_id, "source_key": row.observation.key},
                    ), parent_key=window_id)
                quarantined += len(invalid)
                gaps.append(m.CoverageGap(
                    code="powerbi_alias_validation_incomplete",
                    detail="The source window contains conflicting, unresolved or malformed identities; no source work was admitted.",
                    workspace_id=request.target.workspace_id, item_id=request.target.item_id,
                ))
            else:
                state = "validated"
        staged = self._put("powerbi_window", window_id, control, m.PowerBIWindowState(
            window_id=window_id, target=request.target, poll_work_id=request.poll_work_id,
            window=request.window, revision=prior.revision + 1 if prior else 1,
            state=state, row_count=row_count, quarantined_count=quarantined,
            gaps=tuple(gaps), updated_at=self._now(),
        ), target_key=request.target.key, parent_key=request.poll_work_id, status=state)
        if state == "validated":
            for key in sorted(resolved):
                queued = self._ingest(resolved[key], control)
                if queued is not None:
                    work_ids.append(queued)
        return staged, keys, work_ids

    @atomic()
    def get_powerbi_window(self, context: m.MonitoringContext, window_id: str) -> m.PowerBIWindowState | None:
        self._control(context)
        return self._get("powerbi_window", m.canonical_id(window_id), context, m.PowerBIWindowState)

    @atomic()
    def list_powerbi_aliases(
        self, query: m.PageQuery, *, window_id: str,
    ) -> m.RecordPage[m.PowerBIAliasState]:
        self._control(query)
        window_id = m.canonical_id(window_id)
        staged = self._get("powerbi_window", window_id, query, m.PowerBIWindowState)
        if staged is None:
            raise MonitoringConflict("The requested Power BI source window does not exist")
        return self._page(
            "powerbi_alias", query, m.PowerBIAliasState, {"parent_key": window_id},
            snapshot_revision=staged.revision,
        )

    @atomic(write=True)
    def record_rest_page(self, request: m.RestPageRequest) -> m.RestPageReceipt:
        def apply() -> m.RestPageReceipt:
            control = self._current(m.RegistryVersion(**_stamp(request.target), revision=request.policy_revision), intake=True)
            work = self._owned_work(request.target, request.poll_work_id, request.lease)
            if work.kind != "poll" or work.target != request.target or self._target(request.target) is None:
                raise MonitoringConflict("REST page requires an admitted target's owned poll work")
            progress = self._get("poll_progress", request.target.key, request.target, m.PollProgress)
            prior = (
                progress.checkpoint if progress else None
            ) if self.component != "fixture" else self._get(
                "rest_checkpoint", request.target.key, request.target, m.RestCheckpoint,
            )
            if (prior.revision if prior else 0) != request.expected_checkpoint_revision:
                raise MonitoringConflict("REST checkpoint revision changed")
            if (prior.cursor if prior else None) != request.expected_cursor:
                raise MonitoringConflict("REST continuation changed before page acceptance")
            if prior and prior.cursor is not None and prior.window != request.window:
                raise MonitoringConflict("A continued REST window cannot change its bounds")
            if self.component != "fixture":
                return self._accept_rest_page(request, control, work, prior)
            keys = []
            work_ids = []
            staged = None
            if request.target.workload == "powerbi":
                staged, staged_keys, staged_work = self._stage_powerbi_page(request, control)
                keys.extend(staged_keys)
                work_ids.extend(staged_work)
            for index, observation in enumerate(request.observations):
                receipt_key = f"rest:{request.page_id}:{index}"
                self._put("rest_observation", receipt_key, control, observation, parent_key=request.page_id)
                keys.append(receipt_key)
                queued = self._ingest(observation, control)
                if queued:
                    work_ids.append(queued)
            for quarantine in request.quarantines:
                key = f"rest:{request.page_id}:quarantine:{key_digest(quarantine.observation_id)}"
                self._put("quarantine", key, control, quarantine, parent_key=request.page_id)
                keys.append(key)
            coverage = prior.coverage_through if prior else None
            if request.window_complete and not request.quarantines and (
                staged is None or staged.state == "validated"
            ):
                if coverage is not None and request.window.start_at > coverage:
                    raise MonitoringConflict("A REST coverage watermark cannot skip an unobserved window")
                coverage = max(coverage, request.window.end_at) if coverage else request.window.end_at
            checkpoint = m.RestCheckpoint(
                target=request.target, revision=(prior.revision if prior else 0) + 1,
                window=request.window, cursor=request.next_cursor, coverage_through=coverage,
                last_page_id=request.page_id, updated_at=self._now(),
                powerbi_window_id=staged.window_id if staged is not None else None,
            )
            checkpoint = self._put("rest_checkpoint", request.target.key, control, checkpoint, target_key=request.target.key)
            self._put("poll_progress", request.target.key, control, m.PollProgress(
                checkpoint=checkpoint, producer_request_id=request.page_id,
                powerbi_window_complete=request.powerbi_window_complete,
                window_complete=request.window_complete, retention_exhausted=request.retention_exhausted,
            ), target_key=request.target.key)
            if request.next_cursor is None:
                self._backend.release_lease(work.lease)
                self._save_work(_update(work, state="completed", lease=None, completed_at=self._now(), revision=work.revision + 1))
                target = self._target(request.target)
                seconds = (
                    target.observation.cadence.reconciliation_seconds if target.observation.events_enabled
                    else target.observation.cadence.poll_seconds
                )
                target = self._save_target(_update(target, next_poll_at=self._now() + timedelta(seconds=seconds)))
                self._schedule_poll(target)
            intake = m.IntakeReceipt(
                **_stamp(control), request_id=request.page_id, recorded_at=self._now(),
                receipt_keys=tuple(keys), work_ids=tuple(sorted(set(work_ids))),
            )
            return m.RestPageReceipt(intake=intake, checkpoint=checkpoint, powerbi_window=staged)
        return self._idempotent("rest_page", request.page_id, request.target, request, m.RestPageReceipt, apply)

    def _accept_rest_page(
        self, request: m.RestPageRequest, control: m.DeploymentControl,
        work: m.MonitoringWork, prior: m.RestCheckpoint | None,
    ) -> m.RestPageReceipt:
        prefix = "powerbi-window" if request.target.workload == "powerbi" else "rest-window"
        window_id = stable_id(
            control, f"{prefix}:{request.poll_work_id}:{request.target.key}:{request.window.model_dump_json()}",
        )
        keys = []
        for index, row in enumerate(request.powerbi_rows):
            content = {
                "refresh_id": row.refresh_id,
                "observation": row.observation.model_dump(mode="json", exclude={"observed_at"}),
            }
            key = f"{window_id}:{key_digest(_json(content))}"
            if self._backend.get("powerbi_window_row", key, control) is None:
                self._put(
                    "powerbi_window_row", key, control, row,
                    parent_key=window_id, target_key=request.target.key,
                )
            receipt_key = f"rest:{request.page_id}:powerbi:{index}"
            self._put(
                "rest_powerbi_row", receipt_key, control, row,
                parent_key=window_id, target_key=request.target.key,
            )
            keys.append(receipt_key)
        for index, observation in enumerate(request.observations):
            key = f"rest:{request.page_id}:{index}"
            self._put(
                "rest_observation", key, control, observation,
                parent_key=window_id, target_key=request.target.key,
            )
            keys.append(key)
        for quarantine in request.quarantines:
            key = f"rest:{request.page_id}:quarantine:{key_digest(quarantine.observation_id)}"
            self._put("quarantine", key, control, quarantine, parent_key=window_id, target_key=request.target.key)
            if request.target.workload == "powerbi":
                self._put(
                    "powerbi_window_quarantine", key, control, quarantine,
                    parent_key=window_id, target_key=request.target.key,
                )
            keys.append(key)
        if request.target.workload == "powerbi" and self._backend.count(
            "powerbi_window_row", control, filters={"parent_key": window_id},
        ) > m.MAX_POWERBI_WINDOW_ROWS:
            raise MonitoringConflict("Power BI source window exceeded its bounded staging limit")
        checkpoint = m.RestCheckpoint(
            target=request.target, revision=(prior.revision if prior else 0) + 1,
            window=request.window, cursor=request.next_cursor,
            coverage_through=prior.coverage_through if prior else None,
            last_page_id=request.page_id, updated_at=self._now(),
            powerbi_window_id=window_id if request.target.workload == "powerbi" else None,
        )
        progress = m.PollProgress(
            checkpoint=checkpoint, producer_request_id=request.page_id,
            powerbi_window_complete=request.powerbi_window_complete,
            window_complete=request.window_complete, retention_exhausted=request.retention_exhausted,
        )
        self._put("poll_progress", request.target.key, control, progress, target_key=request.target.key)
        if request.next_cursor is None:
            target = self._target(request.target)
            seconds = (
                target.observation.cadence.reconciliation_seconds if target.observation.events_enabled
                else target.observation.cadence.poll_seconds
            )
            self._put("poll_schedule", target.key, control, m.PollSchedule(
                target=target.identity, policy_revision=control.revision,
                next_poll_at=self._now() + timedelta(seconds=seconds), updated_at=self._now(),
            ), target_key=target.key)
        header = request.model_dump(mode="json", exclude={"observations", "powerbi_rows", "quarantines"})
        header["received_count"] = 0
        bindings = [self._evidence_binding("poll_progress", request.target.key, control)]
        for kind in ("powerbi_window_row", "rest_observation", "quarantine"):
            records = self._backend.scan(kind, control, limit=SCAN_BUDGET + 1, filters={"parent_key": window_id})
            if len(bindings) + len(records) > m.MAX_RECONCILIATION_BINDINGS:
                raise MonitoringConflict("Accepted window evidence exceeds the bounded publication manifest")
            bindings.extend(self._evidence_binding(kind, record.key, control) for record in records)
        handoff = self._request_reconciliation(
            control, request_id=request.page_id, topic="rest_page", reference_id=window_id,
            target=request.target, window=request.window, fingerprint=key_digest(_json(request.model_dump(mode="json"))),
            payload=header, evidence=tuple(bindings),
            producer_commit=m.CollectionCommit(
                work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            ),
        )
        if request.next_cursor is None:
            self._finish_poll_work(work)
        return m.RestPageReceipt(
            intake=m.IntakeReceipt(
                **_stamp(control), request_id=request.page_id, recorded_at=self._now(),
                receipt_keys=tuple(keys), work_ids=(handoff.work_id,), publication_status="pending_validation",
            ),
            checkpoint=checkpoint,
        )

    def _finish_poll_work(self, work: m.MonitoringWork) -> m.MonitoringWork:
        self._backend.release_lease(work.lease)
        return self._save_work(_update(
            work, state="completed", lease=None, completed_at=self._now(), revision=work.revision + 1,
        ))

    @atomic()
    def get_rest_page(self, context: m.MonitoringContext, page_id: str) -> m.RestPageReceipt | None:
        return self._receipt("rest_page", m.canonical_id(page_id), context, m.RestPageReceipt)

    @atomic()
    def get_rest_checkpoint(self, target: m.TargetIdentity) -> m.RestCheckpoint | None:
        self._control(target)
        return self._get("rest_checkpoint", target.key, target, m.RestCheckpoint)

    @atomic()
    def get_poll_progress(self, target: m.TargetIdentity) -> m.PollProgress | None:
        self._control(target)
        return self._get("poll_progress", target.key, target, m.PollProgress)

    def _partition_connector(
        self, scope: ConnectorScope | m.PartitionIdentity, *, receiving: bool,
    ) -> m.OwnedConnectorManifest:
        control = self._control(scope)
        if receiving and control.maintenance:
            raise MonitoringConflict("Maintenance stops stream intake")
        connector = self._get("connector", scope.connector_id, control, m.OwnedConnectorManifest)
        if connector is None or connector.endpoint is None:
            raise MonitoringConflict("Partition ownership requires a verified owned connector endpoint")
        if receiving and connector.state not in {"ready", "degraded"}:
            raise MonitoringConflict("The owned connector is not enabled for reception")
        if connector.endpoint.consumer_group != scope.consumer_group:
            raise MonitoringConflict("Partition consumer group differs from the owned endpoint")
        return connector

    def _ownership_scope_key(self, scope: ConnectorScope | m.PartitionIdentity) -> str:
        return f"{scope.connector_id}:{key_digest(scope.consumer_group)}"

    def _save_partition_ownership(
        self, partition: m.PartitionIdentity, lease: m.LeaseToken | None,
    ) -> PartitionOwnership:
        prior = self._backend.get("partition_ownership", partition.key, partition)
        version = prior.version + 1 if prior else 1
        return self._put("partition_ownership", partition.key, partition, PartitionOwnership(
            partition=partition, lease=lease, etag=f"{key_digest(partition.key)}:{version}",
            modified_at=self._now(),
        ), parent_key=self._ownership_scope_key(partition), status="owned" if lease else "released")

    def _partition_ownership(self, partition: m.PartitionIdentity) -> PartitionOwnership | None:
        ownership = self._get("partition_ownership", partition.key, partition, PartitionOwnership)
        if ownership is not None:
            if ownership.partition != partition:
                raise MonitoringUnavailable("Partition ownership identity disagrees with its key")
            if ownership.lease is not None:
                current = self._backend.get_lease(partition, partition.key)
                if current != ownership.lease:
                    raise MonitoringUnavailable("Partition ownership and its authoritative lease disagree")
        return ownership

    @atomic()
    def list_partition_ownership(self, scope: ConnectorScope) -> tuple[PartitionOwnership, ...]:
        self._partition_connector(scope, receiving=False)
        rows = self._all(
            "partition_ownership", scope, PartitionOwnership,
            filters={"parent_key": self._ownership_scope_key(scope)},
        )
        result = []
        for row in rows:
            current = self._partition_ownership(row.partition)
            if current is None:
                raise MonitoringUnavailable("An enumerated partition ownership record disappeared")
            result.append(current)
        return tuple(sorted(result, key=lambda row: row.partition.partition_id))

    def _change_partition_ownership(self, request: OwnershipChange) -> PartitionOwnership | None:
        self._control(request.partition)
        self._backend.operation_identity("partition_ownership", request.partition.key)
        existing = self._partition_ownership(request.partition)
        if request.expected_etag != (existing.etag if existing else None):
            return None
        raw = self._backend.get_lease(request.partition, request.partition.key)
        if request.release is not None:
            if (
                existing is None or existing.lease is None
                or _stamp(request.release) != _stamp(request.partition)
                or (existing.lease.owner_id, existing.lease.fence)
                != (request.release.owner_id, request.release.fence)
            ):
                return None
            self._backend.compare_exchange_partition_lease(
                request.partition, raw, owner_id=None, lease_seconds=0,
            )
            return self._save_partition_ownership(request.partition, None)
        self._partition_connector(request.partition, receiving=True)
        claim = request.claim
        if claim is None:
            raise MonitoringConflict("Partition ownership mutation has no claim or release")
        if existing is None and raw is not None:
            raise MonitoringUnavailable("An existing partition lease has no ownership journal")
        if (
            existing is not None and existing.lease is not None
            and existing.lease.owner_id == claim.owner_id and existing.lease.expires_at > self._now()
        ):
            lease = self._backend.renew_lease(m.LeaseRenewal(
                lease=existing.lease, lease_seconds=claim.lease_seconds,
            ))
        else:
            lease = self._backend.compare_exchange_partition_lease(
                request.partition, raw, owner_id=claim.owner_id, lease_seconds=claim.lease_seconds,
            )
        if lease is None:
            raise MonitoringUnavailable("A successful ownership claim did not return its lease")
        return self._save_partition_ownership(request.partition, lease)

    @atomic(write=True)
    def change_partition_ownership(self, request: OwnershipChange) -> PartitionOwnership | None:
        return self._change_partition_ownership(request)

    @atomic(write=True)
    def claim_partition(self, request: m.PartitionClaimRequest) -> m.LeaseToken | None:
        self._partition_connector(request.partition, receiving=True)
        existing = self._partition_ownership(request.partition)
        if existing is not None and existing.lease is not None and (
            existing.lease.owner_id != request.owner_id and existing.lease.expires_at > self._now()
        ):
            return None
        changed = self._change_partition_ownership(OwnershipChange(
            partition=request.partition, expected_etag=existing.etag if existing else None, claim=request,
        ))
        # Broker metadata must be persisted separately by ensure_stream_start;
        # the claim's default initial_sequence_number is never a start boundary.
        return changed.lease if changed is not None else None

    def _owned_partition(self, partition: m.PartitionIdentity, lease: m.LeaseToken) -> None:
        self._control(partition)
        ownership = self._partition_ownership(partition)
        current = ownership.lease if ownership else None
        if current is None or current.expires_at <= self._now() or (
            current.owner_id, current.fence
        ) != (lease.owner_id, lease.fence):
            raise MonitoringLeaseLost("Partition lease expired or was rebalanced")

    @atomic(write=True)
    def ensure_stream_start(self, request: StreamStartRequest) -> StreamStart:
        self._partition_connector(request.partition, receiving=True)
        self._owned_partition(request.partition, request.lease)
        self._backend.operation_identity("stream_start", request.partition.key)
        if request.observed_at > self._now():
            raise MonitoringConflict("Broker boundary observation time is in the database clock's future")
        current = self._get("stream_start", request.partition.key, request.partition, StreamStart)
        checkpoint = self._get("stream_checkpoint", request.partition.key, request.partition, m.StreamCheckpoint)
        if current is None:
            if checkpoint is not None:
                raise MonitoringUnavailable("A checkpoint exists without its actual stream-start boundary")
            return self._put("stream_start", request.partition.key, request.partition, StreamStart(
                partition=request.partition, first_sequence_number=request.first_available_sequence_number,
                recorded_at=self._now(), history_before_start="unobserved",
                gaps=(m.CoverageGap(
                    code="unobserved_stream_history",
                    detail=f"History before the actual initial broker sequence {request.first_available_sequence_number} was not observed.",
                ),),
            ), parent_key=request.partition.connector_id)
        expected = checkpoint.position.sequence_number + 1 if checkpoint else current.first_sequence_number
        gaps = list(current.gaps)
        if request.first_available_sequence_number > expected:
            gap = m.CoverageGap(
                code="stream_retention_gap",
                detail=f"Broker retention begins at {request.first_available_sequence_number}, beyond uncheckpointed sequence {expected}; the checkpoint was not advanced.",
            )
            if gap not in gaps:
                gaps.append(gap)
        elif request.first_available_sequence_number < current.first_sequence_number:
            gap = m.CoverageGap(
                code="stream_boundary_regressed",
                detail="Broker sequence history precedes the pinned initial boundary; endpoint/history reconciliation is required.",
            )
            if gap not in gaps:
                gaps.append(gap)
        if tuple(gaps) != current.gaps:
            current = self._put(
                "stream_start", request.partition.key, request.partition,
                _update(current, gaps=self._bounded_gaps(gaps)), parent_key=request.partition.connector_id,
            )
        return current

    @atomic()
    def get_stream_start(self, partition: m.PartitionIdentity) -> StreamStart | None:
        self._control(partition)
        return self._get("stream_start", partition.key, partition, StreamStart)

    def _require_stream_start(self, partition: m.PartitionIdentity, position: m.StreamPosition) -> StreamStart:
        start = self._get("stream_start", partition.key, partition, StreamStart)
        if start is None:
            raise MonitoringConflict("Persist the actual broker start boundary before stream acceptance or checkpointing")
        if position.sequence_number < start.first_sequence_number:
            raise MonitoringConflict("Stream position precedes the pinned actual broker boundary")
        return start

    def _record_position(
        self, partition: m.PartitionIdentity, position: m.StreamPosition,
        receipt_key: str, receipt_kind: Literal["identified", "unidentified"],
    ) -> None:
        self._require_stream_start(partition, position)
        key = f"{partition.key}:position:{position.sequence_number}"
        journal = PositionJournal(
            partition=partition, position=position, receipt_key=receipt_key, receipt_kind=receipt_kind,
        )
        prior = self._get("stream_position", key, partition, PositionJournal)
        if prior is not None:
            if prior != journal:
                raise MonitoringConflict("A broker sequence cannot be rebound to another receipt or identity")
            return
        self._put(
            "stream_position", key, partition, journal,
            parent_key=partition.key, sequence_number=position.sequence_number,
        )

    @staticmethod
    def _quarantine_pending_removal(
        receipt: m.SignalReceipt, connector: m.OwnedConnectorManifest,
    ) -> m.SignalReceipt:
        if receipt.status != "accepted":
            return receipt
        if receipt.observation is None:
            raise MonitoringConflict("Accepted transport intake requires its exact source observation")
        if not any(removal.target == receipt.observation.execution.target for removal in connector.source_removals):
            return receipt
        return _update(receipt, status="quarantined", quarantine=m.QuarantineDisposition(
            observation_id=receipt.delivery.event_id, reason="out_of_scope",
            detail="Desired source removal is pending; retained physical ownership does not authorize intake.",
            replayable=True,
        ))

    @atomic(write=True)
    def record_stream_receipts(self, request: m.StreamReceiptBatch) -> m.IntakeReceipt:
        def apply() -> m.IntakeReceipt:
            control = self._control(request.partition)
            if control.maintenance:
                raise MonitoringConflict("Maintenance stops stream intake")
            self._owned_partition(request.partition, request.lease)
            connector = self._get("connector", request.partition.connector_id, control, m.OwnedConnectorManifest)
            if connector is None or connector.endpoint is None or connector.state not in {"ready", "degraded"}:
                raise MonitoringConflict("Connector provenance is not currently verified")
            keys = []
            work_ids = []
            for receipt in request.receipts:
                self._require_stream_start(request.partition, receipt.position)
                accepted = self._quarantine_pending_removal(receipt, connector)
                if accepted.status == "accepted":
                    source = next((
                        source for source in connector.sources if source.target == receipt.observation.execution.target
                    ), None)
                    target = self._target(receipt.observation.execution.target)
                    if receipt.transport is not None and (
                        not receipt.transport.matches_connector(connector, control.revision)
                        or source is None or source.source_id != receipt.transport.source_id
                    ):
                        raise MonitoringConflict("Transport evidence differs from its current pinned connector")
                    reason = None
                    if source is None or source.event_source != receipt.delivery.event_source:
                        reason = "unverified_provenance"
                    elif receipt.event_type not in source.event_types and WIRE_TO_SUBSCRIPTION_TYPE.get(
                        receipt.event_type or "",
                    ) not in source.event_types:
                        reason = "unsupported"
                    elif target is None and self._get(
                        "submitted_action", receipt.observation.key, control, ActionOwner,
                    ) is None:
                        reason = "out_of_scope"
                    elif (
                        receipt.observation.execution.target.workload == "powerbi"
                        and receipt.observation.execution.run_id_kind != "powerbi_request"
                    ):
                        reason = "ambiguous_execution"
                    if reason:
                        accepted = _update(receipt, status="quarantined", quarantine=m.QuarantineDisposition(
                            observation_id=receipt.delivery.event_id, reason=reason,
                            detail="Current owned-source admission could not be established.", replayable=True,
                        ))
                prior = self._get("signal", receipt.delivery.key, control, m.SignalReceipt)
                if prior is None:
                    self._put("signal", receipt.delivery.key, control, accepted, parent_key=connector.connector_id, status=accepted.status)
                    if accepted.status == "accepted" and self.component == "fixture":
                        queued = self._ingest(accepted.observation, control)
                        if queued:
                            work_ids.append(queued)
                elif prior.observation is not None and accepted.observation is not None and prior.observation.execution != accepted.observation.execution:
                    raise MonitoringConflict("Original event source/ID cannot be rebound to another execution")
                self._record_position(
                    request.partition, receipt.position, receipt.delivery.key, "identified",
                )
                keys.append(receipt.delivery.key)
            self._put("connector", connector.connector_id, control, _update(
                connector, last_receiver_activity_at=self._now(), updated_at=self._now(),
                revision=connector.revision + 1,
            ), status=connector.state)
            if self.component != "fixture":
                handoff = self._request_reconciliation(
                    control, request_id=request.request_id, topic="stream_intake",
                    reference_id=request.partition.key, fingerprint=key_digest(_json(request.model_dump(mode="json"))),
                    payload={"partition": request.partition.model_dump(mode="json"), "receipt_keys": keys},
                    evidence=tuple(self._evidence_binding("signal", key, control) for key in sorted(set(keys))),
                )
                work_ids = [handoff.work_id]
            return m.IntakeReceipt(
                **_stamp(control), request_id=request.request_id, recorded_at=self._now(),
                receipt_keys=tuple(keys), work_ids=tuple(sorted(set(work_ids))),
                publication_status="published" if self.component == "fixture" else "pending_validation",
            )
        return self._idempotent("stream_intake", request.request_id, request.partition, request, m.IntakeReceipt, apply)

    @atomic(write=True)
    def record_unidentified_receipts(self, request: UnidentifiedReceiptBatch) -> m.IntakeReceipt:
        partition = request.receipt.partition
        def apply() -> m.IntakeReceipt:
            connector = self._partition_connector(partition, receiving=True)
            self._owned_partition(partition, request.lease)
            self._require_stream_start(partition, request.receipt.position)
            key = f"{partition.key}:unidentified:{request.receipt.position.sequence_number}"
            saved = self._persisted(request.receipt)
            prior = self._get("unidentified_signal", key, partition, UnidentifiedSignal)
            if prior is None:
                self._put(
                    "unidentified_signal", key, partition, saved,
                    parent_key=connector.connector_id, status="quarantined",
                )
            elif prior.model_dump(exclude={"received_at"}) != saved.model_dump(exclude={"received_at"}):
                raise MonitoringConflict("An unidentified broker position cannot be rebound to different quarantine evidence")
            self._record_position(partition, request.receipt.position, key, "unidentified")
            self._put("connector", connector.connector_id, partition, _update(
                connector, last_receiver_activity_at=self._now(), updated_at=self._now(),
                revision=connector.revision + 1,
            ), status=connector.state)
            handoff = None
            if self.component != "fixture":
                handoff = self._request_reconciliation(
                    self._control(partition), request_id=request.request_id, topic="stream_intake",
                    reference_id=partition.key, fingerprint=key_digest(_json(request.model_dump(mode="json"))),
                    payload={"partition": partition.model_dump(mode="json"), "unidentified_keys": [key]},
                    evidence=(self._evidence_binding("unidentified_signal", key, partition),),
                )
            return m.IntakeReceipt(
                **_stamp(partition), request_id=request.request_id, recorded_at=self._now(),
                receipt_keys=(key,), work_ids=(handoff.work_id,) if handoff else (),
                publication_status="published" if self.component == "fixture" else "pending_validation",
            )
        return self._idempotent(
            "stream_intake", request.request_id, partition, request, m.IntakeReceipt, apply,
        )

    @atomic()
    def get_stream_acceptance(self, context: m.MonitoringContext, request_id: str) -> m.IntakeReceipt | None:
        return self._receipt("stream_intake", m.canonical_id(request_id), context, m.IntakeReceipt)

    @atomic(write=True)
    def advance_stream_checkpoint(self, request: m.StreamCheckpointAdvance) -> m.StreamCheckpoint:
        def apply() -> m.StreamCheckpoint:
            self._owned_partition(request.partition, request.lease)
            prior = self._get("stream_checkpoint", request.partition.key, request.partition, m.StreamCheckpoint)
            if (prior.revision if prior else 0) != request.expected_revision:
                raise MonitoringConflict("Stream checkpoint revision changed")
            state = self._require_stream_start(request.partition, request.through)
            start = prior.position.sequence_number + 1 if prior else state.first_sequence_number
            end = request.through.sequence_number
            if end < start:
                raise MonitoringConflict("Checkpoint cannot move backwards")
            count = self._backend.count(
                "stream_position", request.partition,
                filters={"parent_key": request.partition.key, "sequence_min": start, "sequence_max": end},
            )
            if count != end - start + 1:
                raise MonitoringConflict("Checkpoint would skip an uncommitted partition position")
            terminal = self._get(
                "stream_position", f"{request.partition.key}:position:{end}", request.partition, PositionJournal,
            )
            if terminal is None or terminal.position != request.through:
                raise MonitoringConflict("Checkpoint offset does not match its durable terminal position")
            return self._put("stream_checkpoint", request.partition.key, request.partition, m.StreamCheckpoint(
                partition=request.partition, position=request.through, revision=request.expected_revision + 1,
                updated_at=self._now(),
            ), parent_key=request.partition.connector_id)
        return self._idempotent(
            "stream_checkpoint", request.request_id, request.partition, request, m.StreamCheckpoint, apply,
        )

    @atomic()
    def get_stream_checkpoint(self, partition: m.PartitionIdentity) -> m.StreamCheckpoint | None:
        self._control(partition)
        return self._get("stream_checkpoint", partition.key, partition, m.StreamCheckpoint)

    @atomic(write=True)
    def record_receiver_heartbeat(self, heartbeat: ReceiverHeartbeat) -> ReceiverHeartbeat:
        request_id = stable_id(heartbeat, f"heartbeat:{key_digest(heartbeat.model_dump_json())}")
        def apply() -> ReceiverHeartbeat:
            self._control(heartbeat)
            connector = (
                self._get("connector", heartbeat.connector_id, heartbeat, m.OwnedConnectorManifest)
                if heartbeat.connector_id is not None else None
            )
            if heartbeat.connector_id is not None and connector is None:
                raise MonitoringConflict("Receiver heartbeat requires an owned connector record")
            if heartbeat.observed_at > self._now() or any(
                value is not None and value > heartbeat.observed_at
                for value in (heartbeat.last_delivery_at, heartbeat.last_maintenance_at)
            ):
                raise MonitoringConflict("Receiver heartbeat timestamps are inconsistent")
            key = f"{heartbeat.connector_id or 'collector'}:{heartbeat.worker_id}"
            prior = self._get("receiver_heartbeat", key, heartbeat, ReceiverHeartbeat)
            if prior is not None and prior.observed_at > heartbeat.observed_at:
                raise MonitoringConflict("An older heartbeat cannot replace newer receiver health")
            saved = self._put(
                "receiver_heartbeat", key, heartbeat, heartbeat,
                parent_key=heartbeat.connector_id, status=heartbeat.state, due_at=self._now(),
            )
            # Process/transport health is not a source-delivery or provisioning proof.
            if connector is not None:
                self._put("connector", connector.connector_id, heartbeat, _update(
                    connector, last_receiver_activity_at=self._now(), updated_at=self._now(),
                    revision=connector.revision + 1,
                ), status=connector.state)
            return saved
        return self._idempotent(
            "receiver_heartbeat", request_id, heartbeat, heartbeat, ReceiverHeartbeat, apply,
        )

    @atomic()
    def list_receiver_heartbeats(
        self, context: m.MonitoringContext, *, connector_id: str,
    ) -> tuple[ReceiverHeartbeat, ...]:
        self._control(context)
        connector_id = m.canonical_id(connector_id)
        return tuple(self._all(
            "receiver_heartbeat", context, ReceiverHeartbeat, filters={"parent_key": connector_id},
        ))

    def _release_controller(self, work: m.MonitoringWork) -> None:
        if work.target is None:
            return
        controller = self._backend.get_lease(work, f"controller:{work.target.key}")
        if controller is not None and controller.owner_id == work.work_id:
            self._backend.release_lease(controller)

    def _incident_state(self, identity: m.IncidentIdentity) -> m.IncidentState | None:
        return self._get("incident_state", identity.key, identity.target, m.IncidentState)

    @atomic()
    def get_incident_state(self, identity: m.IncidentIdentity) -> m.IncidentState | None:
        self._control(identity.target)
        return self._incident_state(identity)

    @atomic()
    def get_incident(self, identity: m.IncidentIdentity) -> Incident | None:
        self._control(identity.target)
        state = self._incident_state(identity)
        if state is None:
            return None
        raw = self._backend.incident(state.incident_id)
        return Incident.model_validate_json(raw) if raw is not None else None

    def _deny(self, code: str, detail: str) -> m.ActionReservationDecision:
        return m.ActionReservationDecision(status="denied", denial=code, detail=detail)

    def _approval_denial(
        self, request: m.ActionReservationRequest,
    ) -> m.ActionReservationDecision | None:
        tool = m.ACTION_TO_TOOL[request.action]
        if tool not in APPROVAL_REQUIRED_ACTIONS and request.approval is None:
            return None
        if request.approval is None:
            return self._deny("approval_required", "The existing controller policy requires explicit human approval.")
        row = self._backend.approval(request.approval.approval_id)
        if row is None:
            return self._deny("approval_required", "The referenced approval does not exist.")
        binding = self._get("approval_binding", request.approval.approval_id, request.expected, m.ApprovalBinding)
        if binding is None:
            return self._deny("approval_required", "The approval was not bound to this monitoring intent before publication.")
        origin = binding.model_dump(mode="json", exclude={"created_at", "expires_at"})
        if origin != self._approval_origin(request):
            return self._deny("fingerprint_mismatch", "Scope, safety review or source changed after the approval was opened.")
        if (
            row.get("request_id") != request.approval.approval_id
            or row.get("fingerprint") != request.approval.fingerprint
            or row.get("action") != tool or row.get("arguments") != request.arguments
            or row.get("signature") != request.incident.signature
        ):
            return self._deny("fingerprint_mismatch", "Approval identity, immutable request content or incident signature differs.")
        if row.get("consumed_at"):
            return self._deny("approval_used", "The approval has already been consumed.")
        try:
            expiry = TypeAdapter(m.UtcDateTime).validate_python(row.get("expires_at"))
            decided = TypeAdapter(m.UtcDateTime).validate_python(row.get("decided_at"))
        except ValidationError:
            return self._deny("approval_denied", "The approval does not have valid decision/expiry evidence.")
        if expiry <= self._now() or expiry != binding.expires_at:
            return self._deny("approval_expired", "The approval window has closed.")
        if (
            row.get("decision") != "approve" or not row.get("responder")
            or decided > self._now() or decided < binding.created_at
        ):
            return self._deny("approval_denied", "A current explicit affirmative human decision is required.")
        return None

    def _approval_origin(self, request: m.ActionReservationRequest) -> dict[str, object]:
        if request.approval is None:
            raise MonitoringConflict("A real approval reference is required")
        return {
            "reference": request.approval.model_dump(mode="json"),
            "expected": request.expected.model_dump(mode="json"),
            "work_id": request.work_id, "source_execution": request.source_execution.model_dump(mode="json"),
            "incident": request.incident.model_dump(mode="json"), "action": request.action,
            "review_id": request.review_id, "review_revision": request.expected_review_revision,
            "definition_hash": request.definition_hash, "configuration_hash": request.configuration_hash,
            "parameter_hash": request.parameter_hash, "arguments_hash": m._digest(request.arguments),
        }

    def _approval_binding_record(self, request, work):
        control = self._current(request.expected, intake=True)
        origin = self._approval_origin(request)
        if work.execution != request.source_execution or work.kind not in {"triage", "deferred_retry"}:
            raise MonitoringConflict("Approval intent does not match the owned source work")
        target = self._target(request.source_execution.target)
        review = self._get("review", request.review_id, control, m.SafetyReview)
        capability = self._get("target_capability", request.source_execution.target.key, control, m.CapabilityObservation)
        if (
            target is None or not target.action.enabled or target.action.action != request.action
            or target.action.review_id != request.review_id
            or target.action.review_revision != request.expected_review_revision
            or not self._review_current(review, capability, control)
            or review.parameter_hash != request.parameter_hash
            or review.definition_hash != request.definition_hash
            or review.configuration_hash != request.configuration_hash
        ):
            raise MonitoringConflict("Approval intent requires the current admitted safety review")
        row = self._backend.approval(request.approval.approval_id)
        if (
            row is None or row.get("decision") or row.get("consumed_at")
            or row.get("fingerprint") != request.approval.fingerprint
            or row.get("action") != m.ACTION_TO_TOOL[request.action]
            or row.get("arguments") != request.arguments
            or row.get("signature") != request.incident.signature
        ):
            raise MonitoringConflict("Bind the actual matching unanswered approval before publishing it")
        expiry = TypeAdapter(m.UtcDateTime).validate_python(row.get("expires_at"))
        if expiry <= self._now():
            raise MonitoringConflict("The approval expired before its intent was bound")
        return m.ApprovalBinding(**origin, created_at=self._now(), expires_at=expiry)

    @atomic(write=True)
    def bind_approval(self, request: m.ActionReservationRequest) -> m.ApprovalBinding:
        origin = self._approval_origin(request)
        def apply() -> m.ApprovalBinding:
            control = self._current(request.expected, intake=True)
            work = self._owned_work(control, request.work_id, request.lease)
            return self._put(
                "approval_binding", request.approval.approval_id, control,
                self._approval_binding_record(request, work),
                target_key=request.source_execution.target.key,
            )
        return self._idempotent(
            "approval_binding", request.approval.approval_id, request.expected, origin, m.ApprovalBinding, apply,
        )

    def _save_action(self, action: m.ActionReservation) -> m.ActionReservation:
        return self._put(
            "action", action.reservation_id, action.request.expected, action,
            target_key=action.request.source_execution.target.key,
            parent_key=m.work_key(action.request.expected, action.request.work_id), status=action.state,
        )

    @atomic(write=True)
    def reserve_action(self, request: m.ActionReservationRequest) -> m.ActionReservationDecision:
        def apply() -> m.ActionReservationDecision:
            control = self._control(request.expected)
            if control.maintenance:
                return self._deny("maintenance", "Deployment maintenance forbids a new external mutation.")
            if control.revision != request.expected.revision:
                return self._deny("stale_policy", "The effective monitoring policy changed.")
            if self._pending_validation(request.source_execution.target):
                return self._deny(
                    "pending_validation", "Committed intent or raw intake awaits correlated controller validation.",
                )
            tool = m.ACTION_TO_TOOL[request.action]
            if tool not in self._policy.allowed_actions:
                return self._deny("policy_blocked", "This action is not in the controller allowlist.")
            try:
                work = self._owned_work(control, request.work_id, request.lease)
            except MonitoringLeaseLost:
                return self._deny("lease_lost", "Current fenced work ownership could not be established.")
            if work.execution != request.source_execution or work.kind not in {"triage", "deferred_retry"}:
                return self._deny("source_ineligible", "Only the current controller source work may reserve an effect.")
            if work.policy_revision != control.revision:
                return self._deny("stale_policy", "The owned work predates the current monitoring policy.")
            if work.action_reservation_id is not None:
                return self._deny("source_ineligible", "This invocation already reserved its one submission attempt.")
            target = self._target(request.source_execution.target)
            if target is None:
                return self._deny("out_of_scope", "The target is not currently admitted with verified access.")
            if not target.action.enabled or target.action.action != request.action:
                configured = self._target(request.source_execution.target, include_inactive=True)
                if configured and configured.action.review_id:
                    current_review = self._get("review", configured.action.review_id, control, m.SafetyReview)
                    if current_review and current_review.expires_at <= self._now():
                        return self._deny("expired_review", "The safety review expired.")
                return self._deny("observation_only", "Observation does not grant this action capability.")
            if (
                target.action.review_id != request.review_id
                or target.action.review_revision != request.expected_review_revision
            ):
                return self._deny("stale_review", "The admitted action review changed.")
            review = self._get("review", request.review_id, control, m.SafetyReview)
            capability = self._get("target_capability", target.key, control, m.CapabilityObservation)
            if review is None or review.revision != request.expected_review_revision or review.state != "verified":
                return self._deny("stale_review", "The referenced safety review is not current and verified.")
            if review.expires_at <= self._now():
                return self._deny("expired_review", "The safety review expired.")
            if (
                not self._review_current(review, capability, control) or review.target != target.identity
                or review.action != request.action
                or review.parameter_hash != request.parameter_hash
                or review.definition_hash != request.definition_hash
                or review.configuration_hash != request.configuration_hash
            ):
                return self._deny("fingerprint_mismatch", "Current capability and reviewed action fingerprints differ.")
            source = self._get("source", request.source_execution.key, control, m.SourceRunObservation)
            if source is None or source.authority == "transport" or source.status != "failed":
                return self._deny("source_ineligible", "An authoritative exact failed source execution is required.")
            now = self._now()
            if source.started_at is None or source.ended_at is None or source.observed_at > now:
                return self._deny("source_ineligible", "Source execution times are missing or invalid.")
            if source.started_at < control.activation_cutoff:
                return self._deny("before_cutoff", "The source execution predates this monitoring epoch.")
            if now - source.observed_at > timedelta(seconds=SOURCE_FRESHNESS_SECONDS):
                return self._deny("source_ineligible", "Re-read the exact source after the approval wait.")
            if target.identity.workload == "powerbi" and source.execution.run_id_kind != "powerbi_request":
                return self._deny("source_ineligible", "Power BI aliases have not been resolved to the canonical request ID.")
            if target.identity.workload == "powerbi":
                windows = self._all(
                    "powerbi_window", control, m.PowerBIWindowState,
                    filters={"target_key": target.key, "status_in": ("collecting", "quarantined")},
                )
                for pending in windows:
                    if pending.state == "collecting":
                        return self._deny("source_ineligible", "Power BI window-wide alias validation is still in progress.")
                    elif self._get(
                        "powerbi_alias", f"{pending.window_id}:request:{source.execution.run_id}",
                        control, m.PowerBIAliasState,
                    ) is not None:
                        return self._deny("source_ineligible", "This source belongs to a quarantined Power BI identity window.")
            if request.action == "pipeline_rerun" and (
                not source.failed_scheduled_pipeline or source.job_type != "Pipeline"
            ):
                return self._deny("source_ineligible", "The current controller only reruns failed scheduled Core Pipeline jobs.")
            head = self._get("source_head", target.key, control, m.SourceRunObservation)
            if head and head.started_at and head.started_at > source.started_at:
                if not (request.action == "reenable_refresh_schedule" and head.status == "succeeded"):
                    return self._deny("source_ineligible", "A newer source execution must be reconciled first.")
            if request.action == "reenable_refresh_schedule" and (head is None or head.status != "succeeded"):
                return self._deny("source_ineligible", "Schedule restoration requires a newer successful refresh.")
            owner = self._get("action_owner", target.key, control, ActionOwner)
            if owner is not None and owner.active:
                return self._deny("target_owned", "Another reserved/submitted/uncertain mutation owns this target.")
            state = self._incident_state(request.incident)
            if (state.revision if state else 0) != request.expected_incident_revision:
                return self._deny("stale_policy", "The durable incident budget revision changed.")
            predecessor = None
            if work.retry_of is not None:
                predecessor = self._get("action", work.retry_of, control, m.ActionReservation)
                parent_work = self._get(
                    "work", predecessor.request.work_id, control, m.MonitoringWork,
                ) if predecessor is not None else None
                if (
                    predecessor is None or predecessor.state != "rejected"
                    or predecessor.rejection.reason != "throttled"
                    or predecessor.retry_work_id != work.work_id
                    or predecessor.retry_reservation_id is not None
                    or predecessor.retry_attempt + 1 != work.retry_attempt
                    or not 1 <= work.retry_attempt <= MAX_ATTEMPTS
                    or parent_work is None or parent_work.state != "completed"
                    or request.action != "powerbi_refresh"
                    or predecessor.request.action != request.action
                    or predecessor.request.source_execution != request.source_execution
                    or predecessor.request.incident != request.incident
                    or predecessor.request.parameter_hash != request.parameter_hash
                    or predecessor.request.definition_hash != request.definition_hash
                    or predecessor.request.configuration_hash != request.configuration_hash
                ):
                    return self._deny("source_ineligible", "This is not the unused bounded successor of the rejected action.")
                if state is None or state.action_count < 1:
                    raise MonitoringUnavailable("The rejected action's incident budget slot is missing")
            used = state.action_count if state else 0
            if self._policy.max_write_actions < 1 or (
                used > self._policy.max_write_actions if predecessor is not None
                else used >= self._policy.max_write_actions
            ):
                return self._deny("budget_exhausted", "The durable incident remediation allowance is exhausted.")
            denied = self._approval_denial(request)
            if denied is not None:
                return denied
            if request.approval is not None and not self._backend.consume_approval(
                request.approval.approval_id, request.approval.fingerprint,
            ):
                return self._deny("approval_used", "The approval was consumed, expired or changed before reservation.")
            reservation_id = stable_id(control, f"action:{request.idempotency_id}")
            action = m.ActionReservation(
                reservation_id=reservation_id, request=request, revision=1,
                fence=owner.fence + 1 if owner else 1, state="reserved",
                retry_attempt=work.retry_attempt, retry_of=work.retry_of,
                reserved_at=now, updated_at=now, next_verification_at=now + timedelta(seconds=120),
                detail="Durable pre-POST action fence; an ambiguous effect must not be retried.",
            )
            self._put("action_owner", target.key, control, ActionOwner(
                reservation_id=action.reservation_id, fence=action.fence, active=True,
            ), target_key=target.key)
            state = m.IncidentState(
                identity=request.incident, incident_id=state.incident_id if state else canonical_incident_id(request.incident),
                revision=(state.revision if state else 0) + 1,
                action_count=used + int(predecessor is None),
                latest_execution=state.latest_execution if state else None,
                latest_started_at=state.latest_started_at if state else None, updated_at=now,
            )
            self._put("incident_state", state.identity.key, control, state, target_key=target.key)
            saved = self._save_action(action)
            if predecessor is not None:
                self._save_action(_update(
                    predecessor, retry_reservation_id=saved.reservation_id,
                    revision=predecessor.revision + 1, updated_at=now,
                ))
            self._save_work(_update(work, action_reservation_id=action.reservation_id, revision=work.revision + 1))
            self._schedule_verification(saved)
            return m.ActionReservationDecision(status="reserved", reservation=saved, detail="Action reservation committed.")
        return self._idempotent(
            "action_reservation", request.idempotency_id, request.expected, request, m.ActionReservationDecision, apply,
        )

    @atomic()
    def get_action_reservation(self, context: m.MonitoringContext, reservation_id: str) -> m.ActionReservation | None:
        self._control(context)
        return self._get("action", m.canonical_id(reservation_id), context, m.ActionReservation)

    @atomic()
    def get_action_by_request(self, context: m.MonitoringContext, idempotency_id: str) -> m.ActionReservationDecision | None:
        return self._receipt("action_reservation", m.canonical_id(idempotency_id), context, m.ActionReservationDecision)

    def _fenced_action(
        self, context: m.MonitoringContext, reservation_id: str, revision: int, fence: int,
    ) -> m.ActionReservation:
        self._control(context)
        action = self._get("action", reservation_id, context, m.ActionReservation)
        if action is None or action.revision != revision or action.fence != fence:
            raise MonitoringConflict("The action reservation revision or fence changed")
        return action

    def _action_commit_work(self, context, commit):
        return self._owned_work(context, commit.work_id, commit.lease, commit.expected_work_revision)

    def _action_transition_commit(self, request, action, commit):
        if commit is None:
            if self.component == "fixture":
                return None
            raise MonitoringConflict("Runtime action transitions require an explicit current work commit")
        work = self._action_commit_work(request, commit)
        if (
            work.kind not in {"triage", "deferred_retry", "verify_action", "finalize"}
            or work.action_reservation_id != action.reservation_id
            or work.execution != action.request.source_execution
        ):
            raise MonitoringConflict("Current work does not own this action's original source lineage")
        return work

    def _validate_action_submission(self, action, request):
        if action.state == "rejected" or action.state.startswith("verified_"):
            raise MonitoringConflict("A terminal action cannot return to submission")
        if request.submitted_at < action.reserved_at or request.submitted_at > self._now():
            raise MonitoringConflict("Submission time is outside the committed action interval")
        configuration_action = action.request.action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}
        if configuration_action:
            if request.submitted_execution is not None or (
                request.state == "submitted" and request.configuration_action != action.request.action
            ):
                raise MonitoringConflict("Non-job submission must identify the exact configuration action")
        elif request.configuration_action is not None:
            raise MonitoringConflict("A job submission cannot use configuration correlation")
        if action.submitted_execution is not None and request.submitted_execution not in {None, action.submitted_execution}:
            raise MonitoringConflict("A recorded submission cannot be rebound to another execution")
        submitted = request.submitted_execution or action.submitted_execution
        if submitted is not None and (
            submitted.target != action.request.source_execution.target or submitted == action.request.source_execution
        ):
            raise MonitoringConflict("The submitted action is not the reserved target's new execution")
        return submitted

    @atomic(write=True)
    def record_action_submission(
        self, request: m.ActionSubmissionRequest, *, commit: m.CollectionCommit | None = None,
    ) -> m.ActionReservation:
        def apply() -> m.ActionReservation:
            action = self._fenced_action(
                request, request.reservation_id, request.expected_reservation_revision, request.action_fence,
            )
            self._action_transition_commit(request, action, commit)
            submitted = self._validate_action_submission(action, request)
            if submitted is not None:
                prior = self._get("submitted_action", submitted.key, request, ActionOwner)
                if prior is not None and prior.reservation_id != action.reservation_id:
                    raise MonitoringConflict("This submitted execution belongs to another reservation")
                self._put("submitted_action", submitted.key, request, ActionOwner(
                    reservation_id=action.reservation_id, fence=action.fence, active=True,
                ), target_key=submitted.target.key)
            saved = self._save_action(_update(
                action, state=request.state, submitted_execution=submitted, submitted_at=request.submitted_at,
                next_verification_at=request.next_verification_at, updated_at=self._now(),
                revision=action.revision + 1, detail=request.detail,
            ))
            self._schedule_verification(saved)
            return saved
        fingerprint = {"request": request.model_dump(mode="json"),
                       "commit": commit.model_dump(mode="json") if commit else None}
        return self._idempotent("action_submission", request.request_id, request, fingerprint, m.ActionReservation, apply)

    def _rejection_successor(
        self, action: m.ActionReservation, control: m.DeploymentControl,
    ) -> m.MonitoringWork | None:
        if (
            action.request.action != "powerbi_refresh" or action.rejection.reason != "throttled"
            or action.retry_attempt >= MAX_ATTEMPTS or control.maintenance
            or control.revision != action.request.expected.revision
        ):
            return None
        target = self._target(action.request.source_execution.target)
        review = self._get("review", action.request.review_id, control, m.SafetyReview)
        capability = self._get("target_capability", target.key, control, m.CapabilityObservation) if target else None
        if (
            target is None or not target.action.enabled or target.action.action != action.request.action
            or target.action.review_id != action.request.review_id
            or target.action.review_revision != action.request.expected_review_revision
            or not self._review_current(review, capability, control)
        ):
            return None
        attempt = action.retry_attempt + 1
        wait = action.rejection.retry_after_seconds or backoff_seconds(attempt)
        work = m.MonitoringWork(
            **_stamp(control), work_id=stable_id(control, f"rejected-retry:{action.reservation_id}"),
            kind="deferred_retry", policy_revision=control.revision,
            created_at=self._now(), due_at=self._now() + timedelta(seconds=wait),
            target=action.request.source_execution.target, execution=action.request.source_execution,
            retry_of=action.reservation_id, retry_attempt=attempt, revision=1, state="queued",
            reason=f"Confirmed throttling rejection; linked deferred attempt {attempt} of {MAX_ATTEMPTS}.",
        )
        saved = self._save_work(work)
        self._put(
            "source_work", self._work_link_key(work), control,
            WorkLink(work_id=work.work_id, execution=work.execution), parent_key=work.execution.key,
        )
        return saved

    @atomic(write=True)
    def record_action_rejection(self, request: m.ActionRejectionRequest) -> m.ActionReservation:
        def apply() -> m.ActionReservation:
            control = self._control(request)
            action = self._fenced_action(
                request, request.reservation_id, request.expected_reservation_revision, request.action_fence,
            )
            work = self._owned_work(request, request.work_id, request.lease)
            if (
                action.state != "reserved" or action.submitted_execution is not None
                or action.submitted_at is not None or action.configuration is not None
                or action.request.work_id != work.work_id or work.action_reservation_id != action.reservation_id
                or work.kind not in {"triage", "deferred_retry"}
                or (action.request.lease.owner_id, action.request.lease.fence)
                != (request.lease.owner_id, request.lease.fence)
            ):
                raise MonitoringConflict("Only the original unsubmitted reservation owner may record confirmed rejection")
            if request.evidence.attempted_at < action.reserved_at or request.evidence.rejected_at > self._now():
                raise MonitoringConflict("Rejection evidence is outside the owned submission interval")
            owner = self._get("action_owner", action.request.source_execution.target.key, request, ActionOwner)
            if owner is None or owner.reservation_id != action.reservation_id or owner.fence != action.fence or not owner.active:
                raise MonitoringConflict("Rejected action no longer owns the target mutation fence")
            rejected = _update(
                action, state="rejected", rejection=request.evidence,
                next_verification_at=None, updated_at=self._now(), revision=action.revision + 1,
                detail=request.detail,
            )
            successor = self._rejection_successor(rejected, control)
            if successor is not None:
                rejected = _update(rejected, retry_work_id=successor.work_id)
            saved = self._save_action(rejected)
            self._put("action_rejection", action.reservation_id, request, request, target_key=action.request.source_execution.target.key)
            self._put(
                "action_owner", action.request.source_execution.target.key, request, _update(owner, active=False),
            )
            # Verification work was reserved before POST. A definitive rejection
            # invalidates that poll, not the source or its unfinished finalization.
            link = self._get("source_work", f"verify_action:{action.reservation_id}", request, WorkLink)
            verification = self._get("work", link.work_id, request, m.MonitoringWork) if link else None
            if verification is not None and verification.state in {"queued", "waiting"}:
                self._save_work(_update(
                    verification, state="dispositioned", completed_at=self._now(),
                    disposition="Confirmed no effect; no submitted execution to verify.",
                    revision=verification.revision + 1,
                ))
            return saved
        return self._idempotent("action_rejection", request.request_id, request, request, m.ActionReservation, apply)

    def _validate_action_outcome(self, action, request):
        if action.state == "rejected" or action.state.startswith("verified_"):
            raise MonitoringConflict("A terminal action outcome is immutable")
        if request.observed_at > self._now() or (
            action.submitted_at is not None and request.observed_at < action.submitted_at
        ):
            raise MonitoringConflict("Verification time is outside the submitted action interval")
        if request.configuration is not None:
            if (
                request.configuration.target != action.request.source_execution.target
                or request.configuration.action != action.request.action
                or request.configuration.expected_hash != action.request.parameter_hash
                or action.submitted_at is None or request.configuration.observed_at < action.submitted_at
                or request.configuration.observed_at > self._now()
            ):
                raise MonitoringConflict("Configuration verification does not match the reserved mutation")
            if not self._backend.fixture and request.configuration.authority == "fixture":
                raise MonitoringConflict("Fixture configuration cannot verify a live action")
        elif request.disposition != "uncertain":
            if (
                action.submitted_execution is None or request.submitted_execution != action.submitted_execution
                or request.observation is None or action.submitted_at is None
                or request.observation.ended_at < action.submitted_at
            ):
                raise MonitoringConflict("Outcome does not identify the exact recorded submission")
            if not self._backend.fixture and request.observation.authority == "fixture":
                raise MonitoringConflict("Fixture run evidence cannot verify a live action")
        elif request.submitted_execution is not None and request.submitted_execution != action.submitted_execution:
            raise MonitoringConflict("Uncertainty cannot introduce a different submitted execution")

    @atomic(write=True)
    def record_action_outcome(
        self, request: m.ActionOutcomeRequest, *, commit: m.CollectionCommit | None = None,
    ) -> m.ActionReservation:
        def apply() -> m.ActionReservation:
            action = self._fenced_action(
                request, request.reservation_id, request.expected_reservation_revision, request.action_fence,
            )
            self._action_transition_commit(request, action, commit)
            self._validate_action_outcome(action, request)
            saved = self._save_action(_update(
                action, state=request.disposition, configuration=request.configuration,
                revision=action.revision + 1, updated_at=self._now(), detail=request.detail,
                next_verification_at=self._now() + timedelta(seconds=120) if request.disposition == "uncertain" else None,
            ))
            self._put("action_outcome", action.reservation_id, request, request, target_key=action.request.source_execution.target.key)
            if request.observation is not None:
                self._save_source(request.observation)
            if saved.state.startswith("verified_"):
                owner = self._get("action_owner", action.request.source_execution.target.key, request, ActionOwner)
                if owner is None or owner.reservation_id != action.reservation_id or owner.fence != action.fence:
                    raise MonitoringConflict("Target action ownership no longer matches the outcome")
                self._put("action_owner", action.request.source_execution.target.key, request, _update(owner, active=False))
            else:
                self._schedule_verification(saved)
            return saved
        fingerprint = {"request": request.model_dump(mode="json"),
                       "commit": commit.model_dump(mode="json") if commit else None}
        return self._idempotent("action_outcome", request.request_id, request, fingerprint, m.ActionReservation, apply)

    @atomic(write=True)
    def finalize_work(self, request: m.WorkFinalizationRequest) -> m.FinalizationReceipt:
        def apply() -> m.FinalizationReceipt:
            work = self._owned_work(request, request.work_id, request.lease, request.expected_work_revision)
            if work.execution != request.source_execution or work.kind not in {"triage", "deferred_retry", "verify_action", "finalize"}:
                raise MonitoringConflict("Finalization must match the current controller source work")
            source = self._get("source", request.source_execution.key, request, m.SourceRunObservation)
            if source is None:
                raise MonitoringConflict("A final incident requires durable source evidence")
            state = self._incident_state(request.incident_identity)
            incident_id = state.incident_id if state else canonical_incident_id(request.incident_identity)
            if not incident_id or len(incident_id) > 200:
                raise MonitoringConflict("Incident identity must fit the deployed incident key")
            prior_payload = self._backend.incident(incident_id)
            prior = Incident.model_validate_json(prior_payload) if prior_payload is not None else None
            if prior is not None and prior.signature != request.incident_identity.signature:
                raise MonitoringConflict("This incident ID is already bound to another signature")
            action_id = request.action_reservation_id or work.action_reservation_id
            action = None
            if action_id is not None:
                action = self._get("action", action_id, request, m.ActionReservation)
                if action is None or action.request.source_execution != request.source_execution:
                    raise MonitoringConflict("Finalization action does not match the source failure")
                if request.incident.outcome == "resolved" and action.state != "verified_succeeded":
                    raise MonitoringConflict("An uncertain or failed external mutation is not a resolved incident")
            elif self._work_reservation(work) is not None:
                raise MonitoringConflict("An existing action fence cannot be omitted from finalization")
            processed = self._get("source_disposition", source.key, request, m.ProcessedSourceRecord)
            historical = bool(
                state and state.latest_started_at and source.started_at
                and source.started_at < state.latest_started_at
            )
            if request.source_disposition == "historical" and prior is None:
                historical = False
            if historical and prior is not None:
                incident = prior.model_copy(deep=True)
            else:
                incident = request.incident.model_copy(deep=True)
                incident.id = incident_id
            if prior is not None:
                incident.occurrence_count = prior.occurrence_count + int(processed is None and work.kind != "verify_action")
                incident.notified_count = max(prior.notified_count, incident.notified_count)
                incident.first_seen_at = prior.first_seen_at
                if historical:
                    incident.last_seen_at = prior.last_seen_at
                else:
                    incident.last_seen_at = max(prior.last_seen_at, incident.last_seen_at)
            else:
                incident.occurrence_count = max(1, incident.occurrence_count)
            persisted = self._persisted(incident)
            payload = persisted.model_dump_json()
            self._backend.write_incident(persisted, payload, prior_payload)
            disposition = "historical" if historical else request.source_disposition
            self._source_disposition(
                source.execution, disposition, "Controller terminal outcome and incident are durable.",
                work_id=work.work_id, finalization_id=request.finalization_id,
                incident_identity=request.incident_identity,
            )
            state = m.IncidentState(
                identity=request.incident_identity, incident_id=incident_id,
                revision=(state.revision if state else 0) + 1, action_count=state.action_count if state else 0,
                latest_execution=state.latest_execution if historical else source.execution,
                latest_started_at=state.latest_started_at if historical else source.started_at,
                updated_at=self._now(),
            )
            self._put("incident_state", state.identity.key, request, state, target_key=state.identity.target.key)
            self._backend.release_lease(work.lease)
            self._release_controller(work)
            self._save_work(_update(
                work, state="completed", lease=None, completed_at=self._now(), revision=work.revision + 1,
                finalization_id=request.finalization_id, disposition=disposition,
            ))
            if action is not None and action.state.startswith("verified_"):
                link = self._get("source_work", f"verify_action:{action.reservation_id}", request, WorkLink)
                followup = self._get("work", link.work_id, request, m.MonitoringWork) if link else None
                if followup is not None and followup.work_id != work.work_id and followup.state in {"queued", "waiting"}:
                    self._save_work(_update(
                        followup, state="dispositioned", lease=None, completed_at=self._now(),
                        revision=followup.revision + 1,
                        disposition="The correlated terminal action and incident were already durably finalized.",
                    ))
            if action is not None and not action.state.startswith("verified_"):
                self._schedule_verification(action)
            return m.FinalizationReceipt(
                **_stamp(request), finalization_id=request.finalization_id, work_id=work.work_id,
                source_execution=source.execution, incident_identity=request.incident_identity,
                source_disposition=disposition, incident_payload_hash=hashlib.sha256(payload.encode("utf-16-le")).hexdigest(),
                persisted_at=self._now(), incident_id=incident_id,
            )
        return self._idempotent("finalization", request.finalization_id, request, request, m.FinalizationReceipt, apply)

    @atomic()
    def get_finalization(self, context: m.MonitoringContext, finalization_id: str) -> m.FinalizationReceipt | None:
        return self._receipt("finalization", m.canonical_id(finalization_id), context, m.FinalizationReceipt)


class InMemoryMonitoringStore(MonitoringEngine):
    """Explicit offline setup or component simulation; never a SQL-failure fallback.

    The fixture component performs deterministic setup directly. Worker/web
    simulations stage the same immutable handoffs that a controller must drain.
    """

    def __init__(
        self, *, clock: Callable[[], datetime], state: InMemoryMonitoringState | None = None,
        component: m.RuntimeComponent = "fixture",
        policy: TriagePolicy | None = None, redactor: Callable[[str], str] = redact_text,
    ) -> None:
        super().__init__(
            MemoryBackend(state or InMemoryMonitoringState(), clock, component=component),
            component=component, policy=policy, redactor=redactor,
        )

    def drain_reconciliation(
        self, context: m.MonitoringContext, *, owner_id: str, limit: int = 200,
    ) -> tuple[m.ReconciliationResult, ...]:
        """Explicit bounded offline draining, not an automatically selected live route."""
        if self.component not in {"controller", "fixture"}:
            raise MonitoringComponentDenied("Only the controller or explicit fixture setup can drain publication")
        TypeAdapter(m.BatchSize).validate_python(limit)
        results = []
        while len(results) < limit:
            claimed = self.claim_work(m.WorkClaimRequest(
                **_stamp(context), owner_id=owner_id, kinds=("reconcile_state",),
                limit=min(20, limit - len(results)), per_workspace_limit=1,
            ))
            if not claimed:
                break
            results.extend(self.reconcile_work(work) for work in claimed)
        return tuple(results)
