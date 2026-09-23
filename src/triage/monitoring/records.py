"""Record representations and persistence operations shared by monitoring adapters."""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, TypeVar
from uuid import UUID, uuid5

from pydantic import BaseModel, TypeAdapter, ValidationError

from triage.models import Incident
from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringUnavailable

logger = logging.getLogger("triage.monitoring.records")
ModelT = TypeVar("ModelT", bound=BaseModel)


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
    def connector_receipts(self, context: m.MonitoringContext, connector_id: str) -> tuple[StoredReceipt, ...]: ...
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
