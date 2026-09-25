"""Explicit offline records and semantics; never selected after a SQL failure."""

from __future__ import annotations

import json
import logging
import threading
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TypeVar

from pydantic import BaseModel, TypeAdapter, ValidationError

from triage.models import Incident
from triage.monitoring import models as m
from triage.monitoring.adapters import MonitoringAdapter, Rejection
from triage.monitoring.contracts import (
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringUnavailable,
)
from triage.monitoring.engine import (
    CONNECTOR_PUBLICATION_OPERATIONS as CONNECTOR_PUBLICATION_OPERATIONS,
)
from triage.monitoring.engine import (
    CONTROLLER_OPERATIONS as CONTROLLER_OPERATIONS,
)
from triage.monitoring.engine import (
    EVIDENCE_REJECTION_REASONS as EVIDENCE_REJECTION_REASONS,
)
from triage.monitoring.engine import (
    PLAN_TTL_SECONDS as PLAN_TTL_SECONDS,
)
from triage.monitoring.engine import (
    SCAN_BUDGET as SCAN_BUDGET,
)
from triage.monitoring.engine import (
    SHARED_WORK_OPERATIONS as SHARED_WORK_OPERATIONS,
)
from triage.monitoring.engine import (
    SOURCE_FRESHNESS_SECONDS as SOURCE_FRESHNESS_SECONDS,
)
from triage.monitoring.engine import (
    WEB_OPERATIONS as WEB_OPERATIONS,
)
from triage.monitoring.engine import (
    WORKER_OPERATIONS as WORKER_OPERATIONS,
)
from triage.monitoring.engine import (
    MonitoringEngine as MonitoringEngine,
)
from triage.monitoring.engine import (
    _inventory_absence_matches as _inventory_absence_matches,
)
from triage.monitoring.engine import (
    atomic as atomic,
)
from triage.monitoring.engine import (
    inventory_confirms_deletion as inventory_confirms_deletion,
)
from triage.monitoring.engine import (
    policy_removes_target as policy_removes_target,
)
from triage.monitoring.engine import (
    telemetry_logger as telemetry_logger,
)
from triage.monitoring.records import (
    ActionOwner as ActionOwner,
)
from triage.monitoring.records import (
    FairCursor as FairCursor,
)
from triage.monitoring.records import (
    PositionJournal as PositionJournal,
)
from triage.monitoring.records import (
    RecordBackend as RecordBackend,
)
from triage.monitoring.records import (
    StoredReceipt as StoredReceipt,
)
from triage.monitoring.records import (
    StoredRecord as StoredRecord,
)
from triage.monitoring.records import (
    WorkLink as WorkLink,
)
from triage.monitoring.records import (
    _json as _json,
)
from triage.monitoring.records import (
    _reconciliation_policy_revision as _reconciliation_policy_revision,
)
from triage.monitoring.records import (
    _reconciliation_workspace as _reconciliation_workspace,
)
from triage.monitoring.records import (
    _stamp as _stamp,
)
from triage.monitoring.records import (
    _update as _update,
)
from triage.monitoring.records import (
    _utc as _utc,
)
from triage.monitoring.records import (
    canonical_incident_id as canonical_incident_id,
)
from triage.monitoring.records import (
    key_digest as key_digest,
)
from triage.monitoring.records import (
    stable_id as stable_id,
)
from triage.policy import TriagePolicy
from triage.redaction import redact_text

logger = logging.getLogger("triage.monitoring.memory")
ModelT = TypeVar("ModelT", bound=BaseModel)
ResultT = TypeVar("ResultT")


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
        elif name in {"status_in", "workload_in"}:
            if getattr(record, name.removesuffix("_in")) not in value:
                return False
        elif getattr(record, name) != value:
            return False
    return True


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

    def connector_receipts(self, context, connector_id):
        result = []
        for receipt in self.state.receipts.values():
            if _stamp(receipt.context) != _stamp(context) or receipt.operation not in {
                "connector", "connector_publication", "collection_completion",
            }:
                continue
            try:
                payload = json.loads(receipt.payload)
                if not isinstance(payload, dict):
                    raise ValueError("Receipt payload is not an object")
            except (ValueError, TypeError) as exc:
                logger.error("Unreadable connector history request_hash=%s", key_digest(receipt.request_id))
                raise MonitoringUnavailable("Connector history is unreadable") from exc
            if payload.get("connector_id") == connector_id:
                result.append(receipt)
                if len(result) > SCAN_BUDGET:
                    raise MonitoringConflict("Connector history exceeds the bounded recovery scan")
        return tuple(result)

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


class MemoryMonitoringAdapter(MonitoringAdapter):
    """Offline transitions over explicit fixture records and original receipts."""

    def run_operation(self, method: Callable[..., ResultT], write: bool, args: tuple[object, ...], kwargs: dict[str, object]) -> ResultT:
        return method(self.engine, *args, **kwargs)

    def decode(self, record: StoredRecord, model: type[ModelT]) -> ModelT:
        return self.engine._decode_record(record, model)

    def put(self, kind: str, key: str, context: m.MonitoringContext, value: ModelT, **indices: object) -> ModelT:
        return self.engine._put_record(kind, key, context, value, **indices)

    def idempotent(self, operation: str, request_id: str, context: m.MonitoringContext, request: BaseModel | dict[str, object], model: type[ModelT], apply: Callable[[], ModelT]) -> ModelT:
        self.engine._backend.operation_identity(operation, request_id)
        self.engine._control(context)
        raw = request.model_dump(mode="json") if isinstance(request, BaseModel) else request
        fingerprint = key_digest(_json(raw))
        prior = self.engine._backend.get_receipt(operation, request_id, context)
        if prior is not None:
            if prior.fingerprint != fingerprint:
                raise MonitoringConflict("Idempotency ID was reused for different validated content")
            return model.model_validate_json(prior.payload)
        result = self.engine._persisted(apply())
        self.engine._backend.operation_identity(operation, request_id)
        self.engine._backend.put_receipt(StoredReceipt(
            operation=operation, request_id=request_id, fingerprint=fingerprint,
            context=m.MonitoringContext(**_stamp(context)), payload=result.model_dump_json(),
            recorded_at=self.engine._now(),
        ))
        return result

    def receipt(self, operation: str, request_id: str, context: m.MonitoringContext, model: type[ModelT]) -> ModelT | None:
        return self.engine._read_receipt(operation, request_id, context, model)

    def request_reconciliation(self, control: m.DeploymentControl, *, request_id: str, topic: str, reference_id: str, fingerprint: str, payload: dict[str, object], target: m.TargetIdentity | None=None, window: m.ObservationWindow | None=None, evidence: tuple[m.EvidenceBinding, ...]=(), producer_commit: m.CollectionCommit | None=None) -> m.MonitoringWork:
        producer = "web" if topic in {"scope", "review", "discovery"} else "worker"
        kind = f"{producer}_reconcile_request"
        existing = self.engine._get(kind, request_id, control, m.ReconciliationRequest)
        if existing is not None:
            if existing.fingerprint != fingerprint:
                raise MonitoringConflict("A producer handoff cannot change its original request")
            work = self.engine._get("work", existing.work_id, control, m.MonitoringWork)
            if work is None:
                raise MonitoringUnavailable("An accepted producer request has no initial reconciliation work")
            return work
        work_id = stable_id(control, f"reconcile:{producer}:{request_id}")
        if self.engine._backend.get("work", work_id, control) is not None:
            raise MonitoringConflict("Producer handoffs cannot mutate existing controller work")
        frontier_key = f"validation:v1:{control.epoch}:{control.tenant_id}:{topic}:{reference_id}"
        prior = self.engine._get("validation_frontier", frontier_key, control, m.ValidationFrontier)
        if prior is not None and topic in {"inventory", "rest_page"} and self.engine._get(
            "window_rejection", frontier_key, control, m.ReconciliationResult,
        ) is not None:
            raise MonitoringConflict("A rejected collection window cannot accept another page")
        frontier = m.ValidationFrontier(
            **_stamp(control), frontier_key=frontier_key, target=target, window=window,
            accepted_revision=prior.accepted_revision + 1 if prior else 1,
            validated_revision=prior.validated_revision if prior else 0,
            latest_request_id=request_id, updated_at=self.engine._now(),
        )
        request = m.ReconciliationRequest(
            **_stamp(control), request_id=request_id, producer=producer, topic=topic,
            reference_id=reference_id, fingerprint=fingerprint, policy_revision=control.revision,
            work_id=work_id, target=target, window=window, frontier_key=frontier_key,
            producer_commit=producer_commit,
            frontier_revision=frontier.accepted_revision, created_at=self.engine._now(),
            evidence=evidence, request_payload=payload,
        )
        self.engine._put(
            "validation_frontier", frontier_key, control, frontier,
            target_key=target.key if target else None, status="pending",
        )
        self.engine._put(kind, request_id, control, request, target_key=target.key if target else None)
        return self.engine._save_work(m.MonitoringWork(
            **_stamp(control), work_id=work_id, kind="reconcile_state",
            policy_revision=control.revision, created_at=self.engine._now(), due_at=self.engine._now(),
            reason="Publish or reject immutable producer evidence without an agent or workload action.",
            target=target, reconcile_request_id=request_id, reconcile_producer=producer,
            revision=1, state="queued",
        ))

    def pending_validation(self, identity: m.TargetIdentity) -> bool:
        return any(self.engine._backend.count(
            "validation_frontier", identity, filters={"status": "pending", "target_key": key},
        ) for key in (identity.key, None))

    def pending_frontier_count(self, context: m.MonitoringContext) -> int:
        return self.engine._backend.count("validation_frontier", context, filters={"status": "pending"})

    def commit_scope_intent(self, control: m.DeploymentControl, definition: m.ScopeDefinition) -> tuple[m.DeploymentControl, m.ScopePolicy]:
        updated = _update(control, revision=control.revision + 1, updated_at=self.engine._now())
        self.engine._backend.write_control(updated, control.revision)
        scope = m.ScopePolicy(
            **definition.model_dump(), revision=updated.revision, updated_at=self.engine._now(),
        )
        self.engine._put("scope", scope.scope_id, control, scope, status="enabled" if scope.enabled else "disabled")
        return updated, scope

    def delivery_candidates(self, context: m.MonitoringContext, connector: m.OwnedConnectorManifest, collector_identity_id: str) -> list[m.SignalReceipt]:
        return [signal for signal in self.engine._all(
            "signal", context, m.SignalReceipt,
            filters={"parent_key": connector.connector_id, "status": "accepted"},
        ) if signal.transport is not None]

    def delivery_original(self, signal: m.SignalReceipt, control: m.DeploymentControl) -> datetime:
        transport = signal.transport
        self.engine._control(control)
        receipt = self.engine._backend.get_receipt("stream_intake", transport.request_id, control)
        original = m.IntakeReceipt.model_validate_json(receipt.payload) if receipt is not None else None
        journal = self.engine._get(
            "stream_position", f"{signal.partition.key}:position:{signal.position.sequence_number}",
            control, PositionJournal,
        )
        if (
            receipt is None or original is None or signal.delivery.key not in original.receipt_keys
            or journal is None or journal.receipt_kind != "identified"
            or journal.receipt_key != signal.delivery.key or journal.partition != signal.partition
            or journal.position != signal.position
        ):
            raise MonitoringUnavailable("Delivery proof lost its original accepted receipt or broker position")
        return receipt.recorded_at

    def connector_observation_projection(self, context: m.MonitoringContext, request_id: str) -> m.ConnectorObservationResult | None:
        producer = self.engine._get("worker_reconcile_request", request_id, context, m.ReconciliationRequest)
        if producer is None:
            return None
        if producer.topic != "connector":
            raise MonitoringConflict("The requested producer handoff is not a connector observation")
        return self.engine._connector_observation_result(context, request_id)

    def connector_publication(self, request: m.ConnectorPublicationRequest) -> m.ConnectorPublicationResult:
        def apply():
            control = self.engine._current(request.expected, intake=True)
            work = self.engine._owned_work(control, request.work_id, request.lease, request.expected_work_revision)
            producer = self.engine._reconcile_request(work)
            frontier = self.engine._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
            if (
                producer.policy_revision != control.revision or frontier is None
                or frontier.accepted_revision != request.expected_frontier_revision
            ):
                raise MonitoringConflict("Connector publication lost its current intent/frontier binding")
            prior = self.engine._get("connector", request.connector_id, control, m.OwnedConnectorManifest)
            desired = self.engine._get("connector_desired", request.connector_id, control, m.ConnectorDesiredState)
            if (prior.revision if prior else 0) != request.expected_connector_revision:
                raise MonitoringConflict("Connector publication revision changed")
            if prior is not None and (prior.ownership_id != request.ownership_id or prior.state in {"deleting", "deleted"}):
                raise MonitoringConflict("An established connector cannot change owner or be revived")
            self.engine._validate_initial_connector_publication(prior, desired, request)
            observed = None
            if request.observation_receipt_id is not None:
                if request.observation_receipt_id != producer.request_id:
                    raise MonitoringConflict("Physical binding requires the current original worker observation")
                observed = self.engine._get("connector_observation", producer.request_id, control, m.OwnedConnectorManifest)
            superseded, queued = (), ()
            if request.source_removal_supersessions:
                superseded, queued = self.engine._connector_supersession_guard(
                    request, prior, desired, observed, producer, control,
                )
            removed_sources = {removal.source_id for removal in request.source_removals if removal.source_id is not None}
            removed_proposals = {removal.proposal_id for removal in request.source_removals if removal.proposal_id is not None}
            desired_sources = (
                *(source for source in request.sources if source.source_id not in removed_sources),
                *(proposal for proposal in request.source_proposals if proposal.proposal_id not in removed_proposals),
            )
            for source in desired_sources:
                target = self.engine._target(source.target)
                capability = self.engine._get("target_capability", source.target.key, control, m.CapabilityObservation)
                if (
                    target is None or capability is None or capability.read_status != "verified"
                    or capability.expires_at <= self.engine._now()
                    or capability.event_status != "verified" and not any(
                        entry.source_id == getattr(source, "source_id", None) for entry in superseded
                    )
                ):
                    raise MonitoringConflict("Desired event sources require current approved observation/event capability")
            if prior is not None:
                self.engine._check_connector_bindings(prior, request)
            saved_request = self.engine._persisted(request)
            if saved_request.sources != request.sources or saved_request.source_proposals != request.source_proposals or (
                saved_request.source_removals != request.source_removals
            ) or (
                saved_request.source_removal_supersessions != request.source_removal_supersessions
            ) or (
                saved_request.desired_definition != request.desired_definition
            ):
                raise MonitoringConflict("Redacted desired topology cannot be published as executable configuration")
            sources, proposals, definition = self.engine._connector_desired_values(request, prior, observed)
            removals, retirements = self.engine._connector_removal_records(request, prior, observed, work, control)
            publication_id = stable_id(control, f"connector-publication:{request.request_id}")
            if self.engine._backend.get("connector_publication", publication_id, control) is not None:
                raise MonitoringConflict("An uncommitted connector publication plan cannot be adopted")
            self.engine._put("connector_publication", publication_id, control, m.ConnectorPublicationPlan(
                connector_id=request.connector_id, ownership_id=request.ownership_id, work_id=work.work_id,
                lease_owner_id=work.lease.owner_id, lease_fence=work.lease.fence,
                expected_work_revision=work.revision, expected_connector_revision=request.expected_connector_revision,
                policy_revision=control.revision, producer_request_id=producer.request_id,
                producer_fingerprint=producer.fingerprint, frontier_key=frontier.frontier_key,
                frontier_revision=frontier.accepted_revision, name=saved_request.name,
                sources=saved_request.sources, source_proposals=saved_request.source_proposals,
                source_removals=saved_request.source_removals,
                source_removal_supersessions=saved_request.source_removal_supersessions,
                desired_definition=saved_request.desired_definition,
                observation_receipt_id=request.observation_receipt_id, readiness_receipt_id=request.readiness_receipt_id,
                detail=saved_request.detail,
            ), parent_key=work.work_id)
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
                    desired_definition=definition, state="planned", updated_at=self.engine._now(),
                )
            else:
                candidate = _update(
                    prior, name=request.name, sources=sources, source_proposals=proposals, source_removals=removals,
                    desired_definition=definition,
                    revision=prior.revision + 1, policy_revision=control.revision, updated_at=self.engine._now(),
                    state="provisioning" if changed else prior.state,
                    identity_verified_at=None if changed else prior.identity_verified_at,
                    delivery_verified_at=None if changed else prior.delivery_verified_at,
                    delivery_proof=None if changed else prior.delivery_proof,
                )
            if request.readiness_receipt_id is not None:
                if changed or prior is None or request.readiness_receipt_id != producer.request_id:
                    raise MonitoringConflict("Readiness must use the exact existing desired-connector observation")
                observed = self.engine._get("connector_observation", producer.request_id, control, m.OwnedConnectorManifest)
                desired = self.engine._get("connector_desired", request.connector_id, control, m.ConnectorDesiredState)
                if (
                    observed is None or desired is None or observed.state != "ready"
                    or observed.desired_definition != request.desired_definition
                    or observed.observed_definition != request.desired_definition
                    or observed.ownership_id != request.ownership_id or observed.policy_revision != control.revision
                    or any(getattr(observed, key) != getattr(prior, key) for key in (
                        "workspace_id", "eventstream_id", "destination_id", "endpoint",
                    ))
                    or observed.identity_verified_at is None or observed.delivery_verified_at is None
                    or not desired.published_at <= observed.identity_verified_at <= self.engine._now()
                    or not desired.published_at <= observed.delivery_verified_at <= self.engine._now()
                ):
                    raise MonitoringConflict("The observation does not prove current owned topology readiness")
                self.engine._require_connector_delivery(observed, control)
                candidate = _update(
                    candidate, state="ready", identity_verified_at=observed.identity_verified_at,
                    delivery_verified_at=observed.delivery_verified_at,
                    delivery_proof=observed.delivery_proof,
                )
            saved = self.engine._put("connector", candidate.connector_id, control, candidate, status=candidate.state)
            if changed:
                self.engine._put("connector_desired", saved.connector_id, control, m.ConnectorDesiredState(
                    connector_id=saved.connector_id, ownership_id=saved.ownership_id, publication_id=request.request_id,
                    policy_revision=control.revision, sources_hash=m._digest([source.model_dump(mode="json") for source in saved.sources]),
                    definition_hash=m._digest(saved.desired_definition), published_at=self.engine._now(),
                    supersession_request_id=(
                        desired.supersession_request_id if desired and desired.supersession_request_id
                        else request.request_id if superseded else None
                    ),
                ))
            for pending_work in queued:
                self.engine._save_work(_update(
                    pending_work, state="dispositioned", revision=pending_work.revision + 1,
                    completed_at=self.engine._now(),
                    disposition=f"Superseded by connector publication {request.request_id}",
                ))
            self.engine._publish_connector(saved, control)
            self.engine._schedule_supersession_capabilities(request, superseded, control)
            if saved.state in {"planned", "provisioning"} and changed:
                self.engine._connector_followup(saved, control)
            return m.ConnectorPublicationResult(
                connector_id=saved.connector_id, connector=saved, state=saved.state, desired_changed=changed,
                pending_removals=removals, retired_sources=retirements, observation_receipt_id=request.observation_receipt_id,
                superseded_source_removals=superseded,
            )
        return self.engine._idempotent(
            "connector_publication", request.request_id, request.expected, request, m.ConnectorPublicationResult, apply,
        )

    def reconcile_connector_observation(self, producer: m.ReconciliationRequest, connector: m.OwnedConnectorManifest, control: m.DeploymentControl) -> Rejection | None:
        observed = self.engine._get("connector_observation", producer.request_id, control, m.OwnedConnectorManifest)
        if (
            observed is not None and observed.observed_definition is not None
            and observed.state in {"provisioning", "ready", "degraded"}
            and (connector.source_proposals or connector.source_removals) and self.engine.component != "fixture"
        ):
            stale = self.engine._stale_presence_evidence(producer, connector, control)
            if stale is not None:
                return stale
            work = self.engine._get("work", producer.work_id, control, m.MonitoringWork)
            frontier = self.engine._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
            rejected, result = self.engine._bind_connector_evidence(
                producer, connector, control, m.ConnectorPublicationContext(
                    phase="binding",
                    request_id=stable_id(control, f"connector-bind:{producer.request_id}:{work.lease.fence}"),
                    expected=m.RegistryVersion(**_stamp(control), revision=control.revision),
                    work=work, frontier=frontier, connector=connector,
                ),
            )
            if rejected is not None:
                return rejected
            if result is None:
                raise MonitoringUnavailable("Connector binding did not return its original publication result")
            self.engine._publish_connector(result.connector, control)
            return None
        if observed is not None and observed.state == "ready" and self.engine.component != "fixture":
            work = self.engine._get("work", producer.work_id, control, m.MonitoringWork)
            frontier = self.engine._get("validation_frontier", producer.frontier_key, control, m.ValidationFrontier)
            result = self.engine._connector_publication(m.ConnectorPublicationRequest(
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
        self.engine._publish_connector(connector, control)
        return None

    def connector_observation_overtaken(self, producer: m.ReconciliationRequest, connector: m.OwnedConnectorManifest, control: m.DeploymentControl) -> bool:
        """Has a later observation already advanced this connector?

        The offline store keeps the observation record, so the stale evidence
        stays readable and would be published as if current. The SQL store
        instead refuses to reconstruct a changed baseline, which reads as an
        outage and is retried forever. Either way the handoff is finished: a
        connector revision never goes backwards.

        This is not cosmetic. The live connector frontier reached
        accepted=8/validated=0 with no committed prefix at all, because handoff
        revision 1 stayed pending_validation from the start, and every later
        acknowledgement then failed guard 51072 for lacking that prefix.
        """
        observed = self.engine._get("connector_observation", producer.request_id, control, m.OwnedConnectorManifest)
        return observed is not None and observed.revision != connector.revision

    def commit_review_intent(self, control: m.DeploymentControl, pending: m.SafetyReview, expected_review_revision: int) -> tuple[m.DeploymentControl, m.SafetyReview]:
        updated = _update(control, revision=control.revision + 1, updated_at=self.engine._now())
        self.engine._backend.write_control(updated, control.revision)
        saved = self.engine._put(
            "review_request", pending.review_id, updated, pending, target_key=pending.target.key,
        )
        return updated, saved

    def save_work(self, work: m.MonitoringWork) -> m.MonitoringWork:
        return self.engine._put(
            "work", work.work_id, work, work, status=work.state, work_kind=work.kind,
            workspace_id=work.target.workspace_id if work.target else None,
            workload=work.target.workload if work.target else None,
            target_key=work.target.key if work.target else None,
            parent_key=work.execution.key if work.execution else None,
            due_at=work.lease.expires_at if work.lease else work.due_at,
        )

    def schedule_connector(self, target: m.MonitoringTarget, control: m.DeploymentControl) -> m.MonitoringWork | None:
        if self.engine.component == "controller" and len(self.engine._registered_connectors(control)) == 1:
            return None
        capability = self.engine._get("target_capability", target.key, control, m.CapabilityObservation)
        if capability is None or capability.event_status != "verified":
            return None
        connector_id = stable_id(control, f"connector-target:{target.key}")
        matching = [
            connector for connector in self.engine._all("connector", control, m.OwnedConnectorManifest)
            if connector.state != "deleted" and any(source.target == target.identity for source in connector.sources)
        ]
        if matching:
            connector = matching[0]
            if connector.state == "ready":
                return None
            connector_id = connector.connector_id
        else:
            connector = self.engine._get("connector", connector_id, control, m.OwnedConnectorManifest)
            if connector is None:
                self.engine._put("connector", connector_id, control, m.OwnedConnectorManifest(
                    **_stamp(control), connector_id=connector_id, ownership_id=control.epoch,
                    revision=1, policy_revision=control.revision,
                    name="Pending monitoring connector", sources=(), desired_definition={},
                    state="planned", updated_at=self.engine._now(),
                ), status="planned", target_key=target.key)
        return self.engine._enqueue(m.MonitoringWorkDraft(
            **_stamp(control), work_id=stable_id(control, f"connector:{connector_id}:{control.revision}"),
            kind="connector_reconcile", policy_revision=control.revision,
            created_at=self.engine._now(), due_at=self.engine._now(), target=target.identity, connector_id=connector_id,
            reason="Provision only an owned verified event topology; pending resource IDs are unknown.",
        ))

    def schedule_verification(self, action: m.ActionReservation) -> m.MonitoringWork:
        control = self.engine._control(action.request.expected)
        due = action.next_verification_at or self.engine._now()
        work = self.engine._enqueue(m.MonitoringWorkDraft(
            **_stamp(control),
            work_id=stable_id(control, f"verify:{action.reservation_id}:{action.revision}"),
            kind="verify_action", policy_revision=control.revision, created_at=self.engine._now(), due_at=due,
            target=action.request.source_execution.target, execution=action.request.source_execution,
            action_reservation_id=action.reservation_id,
            reason="Reconcile the existing external effect; never submit another POST.",
        ))
        if work.state in {"queued", "waiting"} and work.due_at != due:
            work = self.engine._save_work(_update(work, due_at=due, revision=work.revision + 1))
        return work

    def source_disposition(self, execution: m.SourceExecutionIdentity, disposition: str, detail: str, *, work_id: str | None=None, finalization_id: str | None=None, incident_identity: m.IncidentIdentity | None=None, observation: m.SourceRunObservation | None=None) -> m.ProcessedSourceRecord:
        prior = self.engine._get("source_disposition", execution.key, execution.target, m.ProcessedSourceRecord)
        if prior is not None:
            return prior
        result = m.ProcessedSourceRecord(
            execution=execution, disposition=disposition, detail=detail, recorded_at=self.engine._now(),
            work_id=work_id, finalization_id=finalization_id, incident_identity=incident_identity,
        )
        saved = self.engine._put(
            "source_disposition", execution.key, execution.target, result,
            target_key=execution.target.key, status=disposition,
        )
        self.engine._backend.mark_processed(execution.key, self.engine._now())
        return saved

    def save_source(self, observation: m.SourceRunObservation) -> m.SourceRunObservation:
        if not self.engine._backend.fixture and observation.authority == "fixture":
            raise MonitoringConflict("Fixture source evidence cannot authorize live monitoring")
        context = observation.execution.target
        if observation.observed_at > self.engine._now():
            raise MonitoringConflict("Source observation time is in the database clock's future")
        prior = self.engine._get("source", observation.key, context, m.SourceRunObservation)
        if prior is not None and (
            (
                (prior.authority == observation.authority or (
                    prior.authority in {"rest", "fixture"} and observation.authority in {"rest", "fixture"}
                )) and prior.observed_at > observation.observed_at
            )
            or (prior.authority in {"rest", "fixture"} and observation.authority == "transport")
        ):
            return prior
        saved = self.engine._put(
            "source", observation.key, context, observation, target_key=context.key,
            status=observation.status, due_at=observation.started_at, parent_key=context.key,
        )
        head = self.engine._get("source_head", context.key, context, m.SourceRunObservation)
        if saved.authority in {"rest", "fixture"} and observation.started_at is not None and (
            head is None or head.started_at is None or observation.started_at >= head.started_at
            or head.execution == observation.execution
        ):
            self.engine._put("source_head", context.key, context, saved, target_key=context.key)
        return saved

    def submitted_action_owner(self, execution: m.SourceExecutionIdentity) -> ActionOwner | None:
        return self.engine._get("submitted_action", execution.key, execution.target, ActionOwner)

    def finish_poll_work(self, work: m.MonitoringWork) -> m.MonitoringWork:
        self.engine._backend.release_lease(work.lease)
        return self.engine._save_work(_update(
            work, state="completed", lease=None, completed_at=self.engine._now(), revision=work.revision + 1,
        ))

    def action_commit_work(self, context: m.MonitoringContext, commit: m.CollectionCommit) -> m.MonitoringWork:
        return self.engine._owned_work(context, commit.work_id, commit.lease, commit.expected_work_revision)


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
            component=component, adapter_factory=MemoryMonitoringAdapter,
            policy=policy, redactor=redactor,
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
