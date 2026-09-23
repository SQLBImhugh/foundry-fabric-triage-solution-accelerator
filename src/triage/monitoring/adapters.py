"""Required semantic operations for each monitoring persistence adapter.

The shared engine cannot infer native SQL behavior from an offline record layout.
Every adapter implements these operations explicitly; a missing implementation
must fail construction rather than inherit a plausible but ineffective check.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Literal, TypeVar

from pydantic import BaseModel

from triage.monitoring import models as m
from triage.monitoring.records import ActionOwner, StoredRecord

if TYPE_CHECKING:
    from triage.monitoring.engine import MonitoringEngine

ModelT = TypeVar("ModelT", bound=BaseModel)
ResultT = TypeVar("ResultT")
Rejection = tuple[Literal["rejected"], str]


class MonitoringAdapter(ABC):
    def __init__(self, engine: MonitoringEngine) -> None:
        self.engine = engine

    @abstractmethod
    def run_operation(
        self, method: Callable[..., ResultT], write: bool,
        args: tuple[object, ...], kwargs: dict[str, object],
    ) -> ResultT: ...

    @abstractmethod
    def decode(self, record: StoredRecord, model: type[ModelT]) -> ModelT: ...

    @abstractmethod
    def put(
        self, kind: str, key: str, context: m.MonitoringContext,
        value: ModelT, **indices: object,
    ) -> ModelT: ...

    @abstractmethod
    def receipt(
        self, operation: str, request_id: str, context: m.MonitoringContext,
        model: type[ModelT],
    ) -> ModelT | None: ...

    @abstractmethod
    def idempotent(
        self, operation: str, request_id: str, context: m.MonitoringContext,
        request: BaseModel | dict[str, object], model: type[ModelT],
        apply: Callable[[], ModelT],
    ) -> ModelT: ...

    @abstractmethod
    def request_reconciliation(
        self, control: m.DeploymentControl, *, request_id: str, topic: str,
        reference_id: str, fingerprint: str, payload: dict[str, object],
        target: m.TargetIdentity | None = None, window: m.ObservationWindow | None = None,
        evidence: tuple[m.EvidenceBinding, ...] = (),
        producer_commit: m.CollectionCommit | None = None,
    ) -> m.MonitoringWork: ...

    @abstractmethod
    def pending_validation(self, identity: m.TargetIdentity) -> bool: ...

    @abstractmethod
    def pending_frontier_count(self, context: m.MonitoringContext) -> int: ...

    @abstractmethod
    def commit_scope_intent(
        self, control: m.DeploymentControl, definition: m.ScopeDefinition,
    ) -> tuple[m.DeploymentControl, m.ScopePolicy]: ...

    @abstractmethod
    def commit_review_intent(
        self, control: m.DeploymentControl, pending: m.SafetyReview,
        expected_review_revision: int,
    ) -> tuple[m.DeploymentControl, m.SafetyReview]: ...

    @abstractmethod
    def connector_observation_projection(
        self, context: m.MonitoringContext, request_id: str,
    ) -> m.ConnectorObservationResult | None: ...

    @abstractmethod
    def connector_observation_overtaken(
        self, producer: m.ReconciliationRequest, connector: m.OwnedConnectorManifest,
        control: m.DeploymentControl,
    ) -> bool: ...

    @abstractmethod
    def reconcile_connector_observation(
        self, producer: m.ReconciliationRequest, connector: m.OwnedConnectorManifest,
        control: m.DeploymentControl,
    ) -> Rejection | None: ...

    @abstractmethod
    def connector_publication(
        self, request: m.ConnectorPublicationRequest,
    ) -> m.ConnectorPublicationResult: ...

    @abstractmethod
    def delivery_candidates(
        self, context: m.MonitoringContext, connector: m.OwnedConnectorManifest,
        collector_identity_id: str,
    ) -> list[m.SignalReceipt]: ...

    @abstractmethod
    def delivery_original(
        self, signal: m.SignalReceipt, control: m.DeploymentControl,
    ) -> datetime: ...

    @abstractmethod
    def save_work(self, work: m.MonitoringWork) -> m.MonitoringWork: ...

    @abstractmethod
    def schedule_connector(
        self, target: m.MonitoringTarget, control: m.DeploymentControl,
    ) -> m.MonitoringWork | None: ...

    @abstractmethod
    def schedule_verification(self, action: m.ActionReservation) -> m.MonitoringWork: ...

    @abstractmethod
    def source_disposition(
        self, execution: m.SourceExecutionIdentity, disposition: str, detail: str, *,
        work_id: str | None = None, finalization_id: str | None = None,
        incident_identity: m.IncidentIdentity | None = None,
        observation: m.SourceRunObservation | None = None,
    ) -> m.ProcessedSourceRecord: ...

    @abstractmethod
    def save_source(self, observation: m.SourceRunObservation) -> m.SourceRunObservation: ...

    @abstractmethod
    def submitted_action_owner(self, execution: m.SourceExecutionIdentity) -> ActionOwner | None: ...

    @abstractmethod
    def finish_poll_work(self, work: m.MonitoringWork) -> m.MonitoringWork: ...

    @abstractmethod
    def action_commit_work(
        self, context: m.MonitoringContext, commit: m.CollectionCommit,
    ) -> m.MonitoringWork: ...
