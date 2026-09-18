"""Native Fabric job candidates and a durable Event Hubs checkpoint adapter.

The public probe supplies the existing endpoint, identity and wire-projection
checks. The worker image must package scripts/hybrid_platform_probe.py.
No destination /connection (accessKeys) endpoint is called here.

EventPersistence below names missing shared-store operations explicitly. It must
use the same durable backend as MonitoringStore, not a second local state store.
The common claim now accepts initial_sequence_number. The additional boundary
still needs ownership enumeration/CAS, starting-boundary reads/gap reporting and
quarantine without an invented CloudEvents source/id. Normal reception refuses
to run without those operations.

https://learn.microsoft.com/fabric/real-time-hub/explore-fabric-job-events
https://learn.microsoft.com/python/api/azure-eventhub/azure.eventhub.aio.checkpointstore
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import logging
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, ParamSpec, Protocol, TypeVar, runtime_checkable
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field, TypeAdapter, ValidationError, model_validator

from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringStore,
    MonitoringStoreError,
)
from triage.monitoring.models import (
    MAX_JSON_BYTES,
    CanonicalId,
    Count,
    CoverageGap,
    DeploymentControl,
    EndpointMetadata,
    IntakeReceipt,
    JsonObject,
    LeaseRenewal,
    LeaseToken,
    MonitoringContext,
    MonitoringModel,
    OpaqueId,
    OwnedConnectorManifest,
    PageQuery,
    PartitionClaimRequest,
    PartitionIdentity,
    QuarantineDisposition,
    SignalReceipt,
    SourceExecutionIdentity,
    SourceRunObservation,
    StreamCheckpointAdvance,
    StreamPosition,
    StreamReceiptBatch,
    TargetIdentity,
    TransportDeliveryIdentity,
    UtcDateTime,
)

LOG = logging.getLogger("triage.monitoring.events")
JSON_OBJECT = TypeAdapter(JsonObject)
P = ParamSpec("P")
T = TypeVar("T")

# These are separate contracts, not suffix-based aliases for incoming events.
WIRE_TO_SUBSCRIPTION_TYPE = {
    "Microsoft.Fabric.ItemJobCreated": "Microsoft.Fabric.JobEvents.ItemJobCreated",
    "Microsoft.Fabric.ItemJobStatusChanged": "Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
    "Microsoft.Fabric.ItemJobSucceeded": "Microsoft.Fabric.JobEvents.ItemJobSucceeded",
    "Microsoft.Fabric.ItemJobFailed": "Microsoft.Fabric.JobEvents.ItemJobFailed",
    # Exact literals observed in the owned MI replay; no namespace-wide alias.
    "Microsoft.Fabric.JobEvents.ItemJobCreated": "Microsoft.Fabric.JobEvents.ItemJobCreated",
    "Microsoft.Fabric.JobEvents.ItemJobFailed": "Microsoft.Fabric.JobEvents.ItemJobFailed",
    "Microsoft.Fabric.JobEvents.ItemJobSucceeded": "Microsoft.Fabric.JobEvents.ItemJobSucceeded",
    "Microsoft.Fabric.JobEvents.ItemJobStatusChanged": "Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
}


class EventContractError(MonitoringStoreError):
    """An explicit event/runtime prerequisite is missing."""


class EventProtocolError(MonitoringStoreError):
    """Bounded transport metadata could not be verified."""


class StreamHistoryGap(MonitoringConflict):
    """An earlier position has not been durably accounted for."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def probe_helpers():
    """Load only the existing, packaged public probe, never a supplied code path."""
    try:
        return importlib.import_module("scripts.hybrid_platform_probe")
    except ModuleNotFoundError as exc:
        if exc.name not in {"scripts", "scripts.hybrid_platform_probe"}:
            raise
        raise EventContractError(
            "The public hybrid_platform_probe module must be packaged"
        ) from None


class IdentityBinding(MonitoringModel):
    tenant_id: CanonicalId
    client_id: CanonicalId
    object_id: CanonicalId
    subscription_id: CanonicalId
    resource_id: str = Field(min_length=1, max_length=1024)

    @model_validator(mode="after")
    def validate_resource(self) -> IdentityBinding:
        for value in (self.tenant_id, self.client_id, self.object_id, self.subscription_id):
            if UUID(value).int == 0:
                raise ValueError("Managed identity bindings require nonempty UUIDs")
        pattern = (
            rf"/subscriptions/{re.escape(self.subscription_id)}/resourceGroups/[^/]+/"
            r"providers/Microsoft\.ManagedIdentity/userAssignedIdentities/[^/]+"
        )
        if not re.fullmatch(pattern, self.resource_id, re.IGNORECASE):
            raise ValueError("The UAMI resource must belong to the pinned subscription")
        return self

    def check_token(self, token: object) -> None:
        if not isinstance(token, str) or len(token) > 32_768:
            raise EventProtocolError("The managed-identity token cannot be inspected safely")
        helpers = probe_helpers()
        try:
            helpers.assert_identity(
                token,
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                object_id=self.object_id,
                subscription_id=self.subscription_id,
                identity_resource_id=self.resource_id,
            )
        except helpers.ProbeError:
            # Do not attach the token, claims, SDK error body, or exception chain.
            raise EventProtocolError("The managed-identity token binding did not match") from None


class Token(Protocol):
    token: str
    expires_on: int


class AsyncCredential(Protocol):
    async def get_token(self, *scopes: str, **kwargs: object) -> Token: ...
    async def close(self) -> None: ...


class PinnedAsyncCredential:
    def __init__(self, credential: AsyncCredential, binding: IdentityBinding) -> None:
        self._credential = credential
        self.binding = binding

    async def get_token(self, *scopes: str, **kwargs: object) -> Token:
        token = await self._credential.get_token(*scopes, **kwargs)
        self.binding.check_token(token.token)
        return token

    async def close(self) -> None:
        await self._credential.close()


def managed_identity_credential(binding: IdentityBinding) -> PinnedAsyncCredential:
    from azure.identity.aio import ManagedIdentityCredential

    return PinnedAsyncCredential(ManagedIdentityCredential(client_id=binding.client_id), binding)


class ConnectorBinding(MonitoringModel):
    tenant_id: CanonicalId
    connector_id: CanonicalId
    workspace_id: CanonicalId
    eventstream_id: CanonicalId
    destination_id: OpaqueId
    endpoint: EndpointMetadata

    def check(self, manifest: OwnedConnectorManifest, control: DeploymentControl) -> None:
        if control.maintenance:
            raise EventContractError("Monitoring is in maintenance")
        if (
            manifest.tenant_id != self.tenant_id
            or control.tenant_id != self.tenant_id
            or manifest.epoch != control.epoch
            or manifest.connector_id != self.connector_id
            or manifest.workspace_id != self.workspace_id
            or manifest.eventstream_id != self.eventstream_id
            or manifest.destination_id != self.destination_id
            or manifest.endpoint != self.endpoint
        ):
            raise EventContractError(
                "The current owned connector does not match its bootstrap binding"
            )
        if manifest.state not in {"ready", "degraded"}:
            raise EventContractError("The owned connector is not enabled for reception")
        if (
            manifest.observed_definition is None
            or manifest.observed_definition != manifest.desired_definition
        ):
            raise EventContractError(
                "The owned connector topology has not been round-trip verified"
            )
        if not manifest.sources:
            raise EventContractError("The owned connector has no configured source subscriptions")
        # Reuse the public probe's strict nonsecret public-endpoint validation.
        probe_helpers().Endpoint.from_document(
            {
                "type": "CustomEndpoint",
                "fullyQualifiedNamespace": self.endpoint.namespace,
                "eventHubName": self.endpoint.entity,
                "consumerGroupName": self.endpoint.consumer_group,
                "workspaceId": self.workspace_id,
                "eventstreamId": self.eventstream_id,
                "destinationId": self.destination_id,
            }
        )


class ConnectorScope(MonitoringContext):
    connector_id: CanonicalId
    consumer_group: OpaqueId


class PartitionOwnership(MonitoringModel):
    partition: PartitionIdentity
    lease: LeaseToken | None
    etag: OpaqueId
    modified_at: UtcDateTime

    @model_validator(mode="after")
    def validate_lease(self) -> PartitionOwnership:
        if self.lease is not None and (
            self.lease.resource_key != self.partition.key
            or self.lease.tenant_id != self.partition.tenant_id
            or self.lease.epoch != self.partition.epoch
        ):
            raise ValueError("Ownership must carry this partition's lease, never an action fence")
        return self


class OwnershipChange(MonitoringModel):
    """One backend CAS, including SDK rebalance/release; never read-then-write."""

    partition: PartitionIdentity
    expected_etag: OpaqueId | None
    claim: PartitionClaimRequest | None = None
    release: LeaseToken | None = None

    @model_validator(mode="after")
    def validate_operation(self) -> OwnershipChange:
        if (self.claim is None) == (self.release is None):
            raise ValueError("Choose exactly one partition claim or fenced release")
        if self.claim is not None and self.claim.partition != self.partition:
            raise ValueError("Claim must name the same partition")
        if self.release is not None and (
            self.release.resource_key != self.partition.key or self.expected_etag is None
        ):
            raise ValueError("Release requires this partition's fence and ETag")
        return self


class StreamStart(MonitoringModel):
    partition: PartitionIdentity
    first_sequence_number: Count
    recorded_at: UtcDateTime
    history_before_start: Literal["unobserved"] = "unobserved"
    gaps: tuple[CoverageGap, ...]


class StreamStartRequest(MonitoringModel):
    partition: PartitionIdentity
    lease: LeaseToken
    first_available_sequence_number: Count
    observed_at: UtcDateTime

    @model_validator(mode="after")
    def validate_lease(self) -> StreamStartRequest:
        if (
            self.lease.resource_key != self.partition.key
            or self.lease.tenant_id != self.partition.tenant_id
            or self.lease.epoch != self.partition.epoch
        ):
            raise ValueError("Stream start requires this partition's lease")
        return self


class UnidentifiedSignal(MonitoringModel):
    """A broker position with no trustworthy original CloudEvents source/id."""

    partition: PartitionIdentity
    position: StreamPosition
    received_at: UtcDateTime
    quarantine: QuarantineDisposition


class UnidentifiedReceiptBatch(MonitoringModel):
    request_id: CanonicalId
    lease: LeaseToken
    receipt: UnidentifiedSignal

    @model_validator(mode="after")
    def validate_lease(self) -> UnidentifiedReceiptBatch:
        partition = self.receipt.partition
        if (
            self.lease.resource_key != partition.key
            or self.lease.tenant_id != partition.tenant_id
            or self.lease.epoch != partition.epoch
        ):
            raise ValueError("Unidentified receipt requires this partition's lease")
        return self


class ReceiverHeartbeat(MonitoringContext):
    worker_id: CanonicalId
    connector_id: CanonicalId | None
    observed_at: UtcDateTime
    state: Literal["starting", "running", "degraded", "blocked", "stopping", "stopped"]
    transport_connected: bool = False
    accepted_positions: Count = 0
    last_delivery_at: UtcDateTime | None = None
    last_maintenance_at: UtcDateTime | None = None
    error_code: OpaqueId | None = None

    @model_validator(mode="after")
    def collector_cannot_assert_event_delivery(self) -> ReceiverHeartbeat:
        if self.connector_id is None and (
            self.transport_connected or self.accepted_positions or self.last_delivery_at is not None
        ):
            raise ValueError("Collector-only health cannot assert event transport or delivery")
        return self


@runtime_checkable
class EventPersistence(Protocol):
    """Required additions/facade for the shared store owner, not existing helpers.

    All methods must use the SAME backend/transaction/epoch checks as
    MonitoringStore. Ownership modification times and expiry use database time.
    CAS changes fence on transfer, and release touches only the partition lease.
    Initial starts persist an explicit unobserved-history gap, not a sequence-zero
    assumption. Both receipt paths map EVERY broker position independently of
    source+id deduplication; advance_stream_checkpoint must consult that mapping
    and this durable initial boundary, including unidentified quarantines.
    Unidentified intake publishes its receipt through get_stream_acceptance,
    using the same stream-intake idempotency namespace. Implement these hooks
    inside the existing atomic backend, not by nesting engine transactions.
    Heartbeats never assert delivery proof.
    """

    def list_partition_ownership(self, scope: ConnectorScope) -> tuple[PartitionOwnership, ...]: ...
    def change_partition_ownership(self, request: OwnershipChange) -> PartitionOwnership | None: ...
    def ensure_stream_start(self, request: StreamStartRequest) -> StreamStart: ...
    def get_stream_start(self, partition: PartitionIdentity) -> StreamStart | None: ...
    def record_unidentified_receipts(self, request: UnidentifiedReceiptBatch) -> IntakeReceipt: ...
    def record_receiver_heartbeat(self, heartbeat: ReceiverHeartbeat) -> ReceiverHeartbeat: ...


@dataclass(frozen=True)
class BodySummary:
    data: bytes | None = field(repr=False)
    size: int
    sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.size) is not int
            or self.size < 0
            or not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
        ):
            raise EventProtocolError("Envelope summary metadata is invalid")
        if (self.data is None and self.size <= MAX_JSON_BYTES) or (
            self.data is not None and len(self.data) != self.size
        ):
            raise EventProtocolError("Envelope summary does not account for the original bytes")


def summarize_body(sections: bytes | Iterable[bytes]) -> BodySummary:
    """Hash all bytes while retaining at most the accepted 64 KiB envelope."""
    helpers = probe_helpers()
    try:
        raw, size, digest = helpers.bounded_event_body(sections)
    except helpers.ProbeError:
        raise EventProtocolError("Expected an AMQP data body with byte sections") from None
    return BodySummary(raw, size, digest)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _no_constant(_: str) -> None:
    raise ValueError("Non-finite JSON value")


def decode_envelope(body: BodySummary) -> dict:
    if body.data is None or body.size > MAX_JSON_BYTES:
        raise EventProtocolError("Native envelope exceeds the bounded JSON limit")
    try:
        return JSON_OBJECT.validate_python(
            json.loads(
                body.data,
                object_pairs_hook=_unique_object,
                parse_constant=_no_constant,
            )
        )
    except (ValueError, UnicodeError, RecursionError):
        raise EventProtocolError("Native envelope is not bounded structured UTF-8 JSON") from None


def _instant(value: object) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("Invalid timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp requires an offset")
    return parsed.astimezone(UTC)


def _optional_instant(data: Mapping[str, object], key: str) -> datetime | None:
    value = data.get(key)
    return None if value is None else _instant(value)


def parse_native_job_event(
    body: BodySummary,
    *,
    partition: PartitionIdentity,
    position: StreamPosition,
    received_at: datetime,
    control: DeploymentControl,
    manifest: OwnedConnectorManifest,
) -> SignalReceipt | UnidentifiedSignal:
    """Project candidate evidence only; do not infer failed/scheduled eligibility."""
    metadata = {"payload_sha256": body.sha256, "payload_bytes": body.size}
    observation_id = hashlib.sha256(
        f"{partition.key}:{position.sequence_number}:{position.offset}".encode()
    ).hexdigest()
    delivery: TransportDeliveryIdentity | None = None

    def refuse(reason, detail: str) -> SignalReceipt | UnidentifiedSignal:
        quarantine = QuarantineDisposition(
            observation_id=observation_id,
            reason=reason,
            detail=detail,
            metadata=metadata,
        )
        if delivery is None:
            return UnidentifiedSignal(
                partition=partition,
                position=position,
                received_at=received_at,
                quarantine=quarantine,
            )
        return SignalReceipt(
            delivery=delivery,
            partition=partition,
            position=position,
            received_at=received_at,
            status="quarantined",
            quarantine=quarantine,
        )

    if body.data is None or body.size > MAX_JSON_BYTES:
        return refuse(
            "oversized", "Native envelope exceeds 65536 bytes; raw content was not retained"
        )
    try:
        event = decode_envelope(body)
    except EventProtocolError:
        return refuse("malformed", "Native envelope is not bounded structured UTF-8 JSON")
    try:
        delivery = TransportDeliveryIdentity(
            tenant_id=partition.tenant_id,
            epoch=partition.epoch,
            connector_id=partition.connector_id,
            event_source=event.get("source"),
            event_id=event.get("id"),
        )
    except ValidationError:
        return refuse(
            "malformed", "Native envelope lacks a trustworthy original source and event ID"
        )
    helpers = probe_helpers()
    try:
        source_tenant = helpers.canonical_id(event["source"])
    except helpers.ProbeError:
        return refuse(
            "unverified_provenance", "Native source is not a documented tenant identifier"
        )
    if source_tenant != control.tenant_id or source_tenant != partition.tenant_id:
        return refuse("wrong_tenant", "Native event belongs to another tenant")
    if (
        manifest.tenant_id != partition.tenant_id
        or manifest.epoch != partition.epoch
        or manifest.connector_id != partition.connector_id
        or control.epoch != partition.epoch
    ):
        return refuse(
            "unknown_connector", "Receipt context does not match the current owned connector"
        )
    event_type = event.get("type")
    if isinstance(event_type, str):
        if len(event_type) <= 256 and re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]*", event_type):
            # Persist bounded diagnostic metadata through the store's redactor;
            # never print an unrecognized event value or retain its raw body.
            metadata["native_event_type"] = event_type
        else:
            metadata["native_event_type_sha256"] = hashlib.sha256(
                event_type.encode("utf-8", errors="replace")
            ).hexdigest()
    if not isinstance(event_type, str) or event_type not in WIRE_TO_SUBSCRIPTION_TYPE:
        return refuse(
            "unsupported", "Native wire type has not been documented or explicitly verified"
        )
    if event.get("specversion") != "1.0" or event.get("dataschemaversion") != "1.0":
        return refuse("unsupported", "Native event schema version is not the documented version")
    data = event.get("data")
    if not isinstance(data, dict):
        return refuse("malformed", "Native event has no structured data object")
    try:
        workspace = helpers.canonical_id(data.get("workspaceId"))
        item = helpers.canonical_id(data.get("itemId"))
        projected = helpers.event_receipt(
            body.data,
            tenant_id=control.tenant_id,
            workspace_id=workspace,
            item_id=item,
        )
        event_time = _instant(event.get("time"))
        started = _optional_instant(data, "jobStartTime")
        ended = _optional_instant(data, "jobEndTime")
        scheduled = _optional_instant(data, "jobScheduleTime")
    except (helpers.ProbeError, ValueError):
        return refuse("malformed", "Native subject, source execution or timestamp is inconsistent")
    if event_time > received_at + timedelta(minutes=5):
        return refuse(
            "unverified_provenance", "Native generation time is outside the clock-skew bound"
        )
    if any(
        value is not None and value < control.activation_cutoff
        for value in (event_time, started, ended)
    ):
        return refuse(
            "before_cutoff", "Native event or source execution predates the active epoch cutoff"
        )
    matching_sources = [
        source
        for source in manifest.sources
        if source.target.workspace_id == workspace and source.target.item_id == item
    ]
    if len(matching_sources) != 1:
        return refuse("out_of_scope", "Native target is not a current owned source subscription")
    source = matching_sources[0]
    if source.target.workload != "fabric_pipeline":
        return refuse("unsupported", "This adapter supports registered pipeline targets only")
    item_kind = data.get("itemKind")
    if not isinstance(item_kind, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", item_kind):
        return refuse("malformed", "Native item-kind metadata is missing or unbounded")
    subscription_type = WIRE_TO_SUBSCRIPTION_TYPE[event_type]
    if subscription_type not in source.event_types:
        return refuse("out_of_scope", "Native event is outside the configured source event filters")
    status = {
        "NotStarted": "not_started",
        "InProgress": "running",
        "Completed": "succeeded",
        "Failed": "failed",
        "Cancelled": "cancelled",
    }.get(data.get("jobStatus"), "unknown")
    invocation = {"Scheduled": "scheduled", "Manual": "manual"}.get(
        data.get("jobInovkeType"),
        "unknown",
    )
    evidence = {
        **metadata,
        "native_event_type": event_type,
        "subscription_event_type": subscription_type,
        "native_event_time": projected["event_time"],
        "native_subject": event["subject"],
        # Workload comes from the owned target, not a guess that display labels
        # in the guide's supported-item table are wire enum values.
        "native_item_kind": item_kind,
        "observed_job_metadata": projected["observed_job_metadata"],
        "source_rest_verified": False,
    }
    if scheduled is not None:
        evidence["native_schedule_time"] = scheduled.isoformat()
    try:
        observation = SourceRunObservation(
            execution=SourceExecutionIdentity(
                target=TargetIdentity(**source.target.model_dump()),
                run_id_kind="fabric_job",
                run_id=projected["job_instance_id"],
            ),
            origin="event",
            authority="transport",
            observed_at=received_at,
            started_at=started,
            ended_at=ended,
            status=status,
            invocation=invocation,
            job_type=projected["observed_job_metadata"].get("jobType"),
            evidence=evidence,
        )
    except ValidationError:
        return refuse(
            "malformed", "Native source-run metadata violates the typed observation contract"
        )
    return SignalReceipt(
        delivery=delivery,
        partition=partition,
        position=position,
        received_at=received_at,
        status="accepted",
        observation=observation,
    )


def _request_id(partition: PartitionIdentity, position: StreamPosition, digest: str) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            json.dumps(
                [partition.key, position.sequence_number, position.offset, digest],
                separators=(",", ":"),
            ),
        )
    )


async def current_connector(
    store: MonitoringStore,
    binding: ConnectorBinding,
    context: MonitoringContext,
) -> tuple[DeploymentControl, OwnedConnectorManifest]:
    snapshot = await asyncio.to_thread(store.snapshot, context)
    cursor: str | None = None
    seen: set[str] = set()
    found: OwnedConnectorManifest | None = None
    while True:
        page = await asyncio.to_thread(
            store.list_connectors,
            PageQuery(**context.model_dump(), limit=100, cursor=cursor),
        )
        if (
            page.version.tenant_id != context.tenant_id
            or page.version.epoch != context.epoch
            or page.version.revision != snapshot.control.revision
        ):
            raise MonitoringConflict("Connector enumeration changed registry revision")
        for item in page.items:
            if item.connector_id == binding.connector_id:
                if found is not None:
                    raise EventContractError("Connector enumeration returned duplicate identities")
                found = item
        if page.next_cursor is None:
            break
        if page.next_cursor in seen:
            raise EventContractError("Connector enumeration repeated a continuation token")
        seen.add(page.next_cursor)
        cursor = page.next_cursor
    if found is None:
        raise EventContractError(
            "The configured owned connector is absent from the active registry"
        )
    binding.check(found, snapshot.control)
    return snapshot.control, found


@dataclass(frozen=True)
class AcceptedPosition:
    request_id: str
    partition: PartitionIdentity
    position: StreamPosition
    receipt_keys: tuple[str, ...]


class SqlCheckpointStore:
    """Duck-typed azure-eventhub 5.15 aio CheckpointStore, with exact SDK methods.

    SDK 5.15.1 performs arithmetic on last_modified_time, so it requires a UTC
    UNIX timestamp despite the interface docstring mentioning datetime.
    Local maps contain only in-flight SDK handles. Every authorization, receipt
    recovery and checkpoint decision re-enters the durable store.
    """

    def __init__(
        self,
        store: MonitoringStore,
        *,
        persistence: EventPersistence,
        binding: ConnectorBinding,
        context: MonitoringContext,
        lease_seconds: int = 120,
        clock: Callable[[], datetime] = utc_now,
        on_failure: Callable[[Exception], None] | None = None,
    ) -> None:
        if not isinstance(persistence, EventPersistence):
            raise EventContractError(
                "EventPersistence is required: durable ownership CAS/listing, stream starts, "
                "unidentified receipt quarantine and receiver heartbeat are not in MonitoringStore"
            )
        if not 15 <= lease_seconds <= 900:
            raise EventContractError("Partition lease duration must be between 15 and 900 seconds")
        if context.tenant_id != binding.tenant_id:
            raise EventContractError("Checkpoint tenant does not match the connector binding")
        self.store = store
        self.persistence = persistence
        self.binding = binding
        self.context = context
        self.lease_seconds = lease_seconds
        self.clock = clock
        self.on_failure = on_failure
        self.partition_ids: tuple[str, ...] = ()
        self._available_starts: dict[str, int] = {}
        self._leases: dict[str, LeaseToken] = {}
        self._pending: dict[str, AcceptedPosition] = {}
        self.accepted_positions = 0
        self.last_delivery_at: datetime | None = None

    async def _call(self, operation: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await asyncio.to_thread(operation, *args, **kwargs)
        except Exception as exc:
            if self.on_failure is not None:
                self.on_failure(exc)
            raise

    def bind_partitions(self, partitions: Iterable[str]) -> None:
        values = tuple(partitions)
        if not values or len(values) != len(set(values)):
            raise EventProtocolError(
                "The owned broker did not return a nonempty unique partition list"
            )
        for value in values:
            if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,10}", value):
                raise EventProtocolError("The broker returned an invalid partition identifier")
        self.partition_ids = values

    def partition(self, value: object) -> PartitionIdentity:
        if not isinstance(value, str) or value not in self.partition_ids:
            raise EventProtocolError("Checkpoint operation names an unverified broker partition")
        return PartitionIdentity(
            **self.context.model_dump(),
            connector_id=self.binding.connector_id,
            consumer_group=self.binding.endpoint.consumer_group,
            partition_id=value,
        )

    def set_partition_properties(self, partition_id: str, properties: Mapping[str, object]) -> int:
        self.partition(partition_id)
        if (
            properties.get("id") != partition_id
            or properties.get("eventhub_name") != self.binding.endpoint.entity
        ):
            raise EventProtocolError("Broker properties belong to another partition or entity")
        beginning = properties.get("beginning_sequence_number")
        last = properties.get("last_enqueued_sequence_number")
        empty = properties.get("is_empty")
        if type(empty) is not bool or type(last) is not int or last < -1:
            raise EventProtocolError("Broker start metadata is missing or malformed")
        first = last + 1 if empty else beginning
        if type(first) is not int or first < 0 or (not empty and first > last):
            raise EventProtocolError("The actual available broker start position is unknown")
        self._available_starts[partition_id] = first
        return first

    def verify_endpoint(self, namespace: object, entity: object, group: object) -> None:
        endpoint = self.binding.endpoint
        if (namespace, entity, group) != (
            endpoint.namespace,
            endpoint.entity,
            endpoint.consumer_group,
        ):
            raise EventProtocolError("SDK operation does not match the configured Entra endpoint")

    def _sdk_identity(self, partition: PartitionIdentity) -> dict[str, object]:
        return {
            "fully_qualified_namespace": self.binding.endpoint.namespace,
            "eventhub_name": self.binding.endpoint.entity,
            "consumer_group": partition.consumer_group,
            "partition_id": partition.partition_id,
        }

    def _sdk_ownership(self, row: PartitionOwnership) -> dict[str, object]:
        if self.partition(row.partition.partition_id) != row.partition:
            raise EventProtocolError("Durable ownership returned another connector's partition")
        return {
            **self._sdk_identity(row.partition),
            "owner_id": "" if row.lease is None else row.lease.owner_id,
            "last_modified_time": row.modified_at.timestamp(),
            "etag": row.etag,
        }

    async def _current(self) -> tuple[DeploymentControl, OwnedConnectorManifest]:
        try:
            return await current_connector(self.store, self.binding, self.context)
        except Exception as exc:
            if self.on_failure is not None:
                self.on_failure(exc)
            raise

    async def list_ownership(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        self.verify_endpoint(fully_qualified_namespace, eventhub_name, consumer_group)
        await self._current()
        rows = await self._call(
            self.persistence.list_partition_ownership,
            ConnectorScope(
                **self.context.model_dump(),
                connector_id=self.binding.connector_id,
                consumer_group=consumer_group,
            ),
        )
        if len({row.partition.partition_id for row in rows}) != len(rows):
            raise EventProtocolError("Durable ownership contains duplicate partitions")
        return [self._sdk_ownership(row) for row in rows]

    async def claim_ownership(
        self,
        ownership_list: Iterable[Mapping[str, object]],
        **kwargs: object,
    ) -> list[dict[str, object]]:
        ownership_list = tuple(ownership_list)
        if any(desired.get("owner_id") != "" for desired in ownership_list):
            await self._current()
        else:
            # Stopping a disabled connector can release only its current partition
            # handles; it must not acquire authority or touch controller fences.
            await self._call(self.store.snapshot, self.context)
        result = []
        for desired in ownership_list:
            self.verify_endpoint(
                desired.get("fully_qualified_namespace"),
                desired.get("eventhub_name"),
                desired.get("consumer_group"),
            )
            partition = self.partition(desired.get("partition_id"))
            owner = desired.get("owner_id")
            etag = desired.get("etag")
            if etag is not None and not isinstance(etag, str):
                raise EventProtocolError("SDK ownership ETag must be an opaque string")
            if owner == "":
                lease = self._leases.get(partition.partition_id)
                if lease is None:
                    raise MonitoringLeaseLost(
                        "No current SDK handle exists for this partition release"
                    )
                request = OwnershipChange(partition=partition, expected_etag=etag, release=lease)
            else:
                if partition.partition_id not in self._available_starts:
                    raise EventContractError(
                        "Actual broker start metadata must precede partition claims"
                    )
                request = OwnershipChange(
                    partition=partition,
                    expected_etag=etag,
                    claim=PartitionClaimRequest(
                        partition=partition,
                        owner_id=owner,
                        lease_seconds=self.lease_seconds,
                        initial_sequence_number=self._available_starts[partition.partition_id],
                    ),
                )
            row = await self._call(self.persistence.change_partition_ownership, request)
            if row is None:
                self._leases.pop(partition.partition_id, None)
                continue
            if (
                row.partition != partition
                or (
                    request.claim is not None
                    and (row.lease is None or row.lease.owner_id != request.claim.owner_id)
                )
                or (request.release is not None and row.lease is not None)
            ):
                raise EventProtocolError("Durable ownership CAS returned an inconsistent result")
            if row.lease is None:
                self._leases.pop(partition.partition_id, None)
            else:
                self._leases[partition.partition_id] = row.lease
            result.append(self._sdk_ownership(row))
        return result

    async def _renew(self, partition: PartitionIdentity) -> LeaseToken:
        lease = self._leases.get(partition.partition_id)
        if lease is None:
            raise MonitoringLeaseLost("The receiver has not durably claimed this partition")
        current = await self._call(
            self.store.renew_lease,
            LeaseRenewal(lease=lease, lease_seconds=self.lease_seconds),
        )
        if (
            current.resource_key != partition.key
            or current.owner_id != lease.owner_id
            or current.fence != lease.fence
            or current.epoch != self.context.epoch
            or current.tenant_id != self.context.tenant_id
        ):
            raise MonitoringLeaseLost("Partition renewal returned another owner or fencing token")
        self._leases[partition.partition_id] = current
        return current

    async def initialize_partition(
        self, partition_id: str, properties: Mapping[str, object]
    ) -> None:
        partition = self.partition(partition_id)
        await self._current()
        first = self.set_partition_properties(partition_id, properties)
        lease = await self._renew(partition)
        start = await self._call(
            self.persistence.ensure_stream_start,
            StreamStartRequest(
                partition=partition,
                lease=lease,
                first_available_sequence_number=first,
                observed_at=self.clock(),
            ),
        )
        if start.partition != partition or start.first_sequence_number > first:
            raise EventProtocolError("The durable stream start does not match the broker evidence")
        checkpoint = await self._call(self.store.get_stream_checkpoint, partition)
        expected = (
            start.first_sequence_number
            if checkpoint is None
            else checkpoint.position.sequence_number + 1
        )
        if first > expected:
            raise StreamHistoryGap(
                "Retention removed positions before the durable checkpoint; reconciliation is required"
            )

    async def list_checkpoints(
        self,
        fully_qualified_namespace: str,
        eventhub_name: str,
        consumer_group: str,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        self.verify_endpoint(fully_qualified_namespace, eventhub_name, consumer_group)
        await self._current()
        if not self.partition_ids:
            raise EventProtocolError(
                "Broker partition discovery must precede checkpoint enumeration"
            )
        result = []
        for partition_id in self.partition_ids:
            partition = self.partition(partition_id)
            checkpoint = await self._call(self.store.get_stream_checkpoint, partition)
            if checkpoint is not None:
                if checkpoint.partition != partition:
                    raise EventProtocolError("Durable checkpoint belongs to another partition")
                result.append(
                    {
                        **self._sdk_identity(partition),
                        "offset": checkpoint.position.offset,
                        "sequence_number": checkpoint.position.sequence_number,
                    }
                )
        return result

    async def accept(
        self,
        partition_id: str,
        position: StreamPosition,
        body: BodySummary,
    ) -> SignalReceipt | UnidentifiedSignal:
        partition = self.partition(partition_id)
        control, manifest = await self._current()
        lease = await self._renew(partition)
        start = await self._call(self.persistence.get_stream_start, partition)
        if start is None or start.partition != partition:
            raise EventContractError("The actual stream starting boundary has not been persisted")
        if position.sequence_number < start.first_sequence_number:
            raise StreamHistoryGap("Broker delivery precedes the verified stream start")
        receipt = parse_native_job_event(
            body,
            partition=partition,
            position=position,
            received_at=self.clock(),
            control=control,
            manifest=manifest,
        )
        request_id = _request_id(partition, position, body.sha256)
        # The deterministic request ID survives restart. Read it BEFORE rebuilding
        # a request with a new receive time or lease, so replay cannot change content.
        intake = await self._call(self.store.get_stream_acceptance, self.context, request_id)
        new_intake = intake is None
        if intake is None:
            try:
                if isinstance(receipt, SignalReceipt):
                    intake = await asyncio.to_thread(
                        self.store.record_stream_receipts,
                        StreamReceiptBatch(
                            request_id=request_id,
                            partition=partition,
                            lease=lease,
                            receipts=(receipt,),
                        ),
                    )
                else:
                    intake = await asyncio.to_thread(
                        self.persistence.record_unidentified_receipts,
                        UnidentifiedReceiptBatch(
                            request_id=request_id, lease=lease, receipt=receipt
                        ),
                    )
            except MonitoringCommitUncertain as exc:
                if exc.idempotency_id != request_id:
                    raise EventProtocolError(
                        "Uncertain intake returned a different receipt identity"
                    ) from None
                intake = await self._call(
                    self.store.get_stream_acceptance, self.context, request_id
                )
                if intake is None:
                    raise MonitoringCommitUncertain("stream_intake", request_id) from None
        if (
            intake.request_id != request_id
            or intake.tenant_id != self.context.tenant_id
            or intake.epoch != self.context.epoch
            or len(intake.receipt_keys) != 1
        ):
            raise EventProtocolError(
                "Durable stream acceptance did not account for this broker position"
            )
        if isinstance(receipt, SignalReceipt) and intake.receipt_keys != (receipt.delivery.key,):
            raise EventProtocolError(
                "Durable stream acceptance changed the original delivery identity"
            )
        # Pending intake IDs belong only to deterministic controller validation.
        # Published fixture intake must not give quarantine executable work.
        if (
            new_intake
            and (not isinstance(receipt, SignalReceipt) or receipt.status == "quarantined")
            and intake.work_ids
            and intake.publication_status != "pending_validation"
        ):
            raise EventProtocolError("A quarantined envelope must not enqueue controller work")
        self._pending[partition_id] = AcceptedPosition(
            request_id,
            partition,
            position,
            intake.receipt_keys,
        )
        self.last_delivery_at = receipt.received_at
        return receipt

    async def update_checkpoint(self, checkpoint: Mapping[str, object], **kwargs: object) -> None:
        self.verify_endpoint(
            checkpoint.get("fully_qualified_namespace"),
            checkpoint.get("eventhub_name"),
            checkpoint.get("consumer_group"),
        )
        partition = self.partition(checkpoint.get("partition_id"))
        pending = self._pending.get(partition.partition_id)
        if pending is None or (
            type(checkpoint.get("sequence_number")) is not int
            or checkpoint["sequence_number"] != pending.position.sequence_number
            or str(checkpoint.get("offset")) != pending.position.offset
        ):
            raise EventProtocolError(
                "Checkpoint is not bound to a durably accepted in-flight position"
            )
        await self._current()
        lease = await self._renew(partition)
        intake = await self._call(
            self.store.get_stream_acceptance,
            self.context,
            pending.request_id,
        )
        if intake is None or (
            intake.receipt_keys != pending.receipt_keys
            or intake.request_id != pending.request_id
            or intake.tenant_id != self.context.tenant_id
            or intake.epoch != self.context.epoch
        ):
            raise EventProtocolError(
                "Checkpoint acceptance cannot be re-established from shared state"
            )
        current = await self._call(self.store.get_stream_checkpoint, partition)
        if (
            current is not None
            and current.position.sequence_number >= pending.position.sequence_number
        ):
            if (
                current.position.sequence_number == pending.position.sequence_number
                and current.position != pending.position
            ):
                raise EventProtocolError(
                    "Replayed checkpoint metadata conflicts with the durable position"
                )
            self._pending.pop(partition.partition_id, None)
            return
        start = await self._call(self.persistence.get_stream_start, partition)
        if start is None:
            raise EventContractError("The stream starting boundary is missing")
        expected_sequence = (
            start.first_sequence_number if current is None else current.position.sequence_number + 1
        )
        if pending.position.sequence_number != expected_sequence:
            raise StreamHistoryGap("An earlier broker position is not durably checkpointed")
        request = StreamCheckpointAdvance(
            request_id=pending.request_id,
            partition=partition,
            lease=lease,
            expected_revision=0 if current is None else current.revision,
            through=pending.position,
        )
        try:
            advanced = await asyncio.to_thread(self.store.advance_stream_checkpoint, request)
        except MonitoringCommitUncertain:
            advanced = await self._call(self.store.get_stream_checkpoint, partition)
            if advanced is None or advanced.position != pending.position:
                raise MonitoringCommitUncertain("stream_checkpoint", pending.request_id) from None
        if advanced.partition != partition or advanced.position != pending.position:
            raise EventProtocolError("The durable checkpoint did not confirm the accepted position")
        self._pending.pop(partition.partition_id, None)
        self.accepted_positions += 1
