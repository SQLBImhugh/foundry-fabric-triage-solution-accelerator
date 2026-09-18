"""Outbound-only monitoring worker and explicit finite transport canary.

Normal startup requires deployed SQL bootstrap, MonitoringStore plus the
EventPersistence operations, and the inventory/polling and connector-observation
maintenance handlers. Only the controller publishes desired topology. Intake
waits, visibly degraded, while that publication or an observed binding is pending;
delivery proof is not a provisioning gate.
--collector-only runs durable inventory/polling and non-transport health before
an Eventstream exists. It never creates a receiver or fabricates connector ownership.
--reconcile-once drains durable connector work without requiring intake hooks.
Missing contracts never select fixture state. The transport probe deliberately
bypasses SQL and reports that distinction.

MONITORING_INVENTORY_MODE defaults to caller_visible. tenant_admin_preview opts
into admin workspace/domain inventory and preview Admin Items; it grants no
permissions and does not establish tenant-wide operational telemetry.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import traceback
from collections.abc import Awaitable, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID, uuid4

from pydantic import ValidationError

from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringStore,
    MonitoringStoreError,
    MonitoringUnavailable,
)
from triage.monitoring.events import (
    AsyncCredential,
    ConnectorBinding,
    EventContractError,
    EventPersistence,
    EventProtocolError,
    IdentityBinding,
    PinnedAsyncCredential,
    ReceiverHeartbeat,
    SqlCheckpointStore,
    StreamHistoryGap,
    Token,
    current_connector,
    decode_envelope,
    managed_identity_credential,
    probe_helpers,
    summarize_body,
    utc_now,
)
from triage.monitoring.models import (
    EndpointMetadata,
    MonitoringContext,
    SignalReceipt,
    StreamPosition,
    connector_definition_hash,
)

LOG = logging.getLogger("triage.monitoring.worker")
SHUTDOWN_SECONDS = 60

if TYPE_CHECKING:
    from triage.monitoring.inventory import InventoryApiOptions
    from triage.monitoring.polling import CollectorRunResult
    from triage.monitoring.provisioning import ReconcileRun


class SyncCredential(Protocol):
    def get_token(self, *scopes: str, **kwargs: object) -> Token: ...
    def close(self) -> None: ...


class PinnedSyncCredential:
    """The same explicit identity binding for SQL and collector REST tokens."""

    def __init__(self, credential: SyncCredential, binding: IdentityBinding) -> None:
        self._credential = credential
        self.binding = binding

    def get_token(self, *scopes: str, **kwargs: object) -> Token:
        token = self._credential.get_token(*scopes, **kwargs)
        self.binding.check_token(token.token)
        return token

    def close(self) -> None:
        self._credential.close()


@dataclass(frozen=True)
class WorkerConfig:
    identity: IdentityBinding
    connector: ConnectorBinding | None
    azure_sql_server: str = ""
    azure_sql_database: str = ""
    heartbeat_seconds: float = 30
    maintenance_seconds: float = 5
    retry_attempts: int = 5
    retry_base_seconds: float = 1
    retry_max_seconds: float = 30
    inventory_mode: Literal["caller_visible", "tenant_admin_preview"] = "caller_visible"

    def __post_init__(self) -> None:
        if not isinstance(self.inventory_mode, str) or self.inventory_mode not in {
            "caller_visible",
            "tenant_admin_preview",
        }:
            raise EventContractError(
                "MONITORING_INVENTORY_MODE must be caller_visible or tenant_admin_preview"
            )
        if self.connector is not None and self.identity.tenant_id != self.connector.tenant_id:
            raise EventContractError("Managed identity and connector tenant bindings differ")
        if (bool(self.azure_sql_server) != bool(self.azure_sql_database)) or (
            self.azure_sql_server
            and (
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]{1,252}", self.azure_sql_server)
                or "." not in self.azure_sql_server
                or not re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9 ._()-]{0,127}", self.azure_sql_database
                )
            )
        ):
            raise EventContractError(
                "Supply SQL hostname/catalog metadata, never connection-string fields"
            )
        self.validate_intervals()

    @property
    def monitoring_mode(self) -> str:
        return "live"

    @property
    def monitoring_tenant_id(self) -> str:
        return self.identity.tenant_id

    @property
    def azure_client_id(self) -> str:
        return self.identity.client_id

    def inventory_options(self) -> InventoryApiOptions:
        from triage.monitoring.inventory import InventoryApiOptions

        admin = self.inventory_mode == "tenant_admin_preview"
        return InventoryApiOptions(
            admin_workspaces=admin,
            admin_domains=admin,
            admin_items_preview=admin,
            powerbi_datasets=True,
        )

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
        *,
        transport_probe: bool = False,
        collector_only: bool = False,
    ) -> WorkerConfig:
        values = os.environ if environment is None else environment
        if values.get("MONITORING_MODE", "live") != "live":
            raise EventContractError(
                "This worker refuses fixture/demo mode; inject offline test boundaries instead"
            )
        for name in ("AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_PASSWORD"):
            if values.get(name):
                raise EventContractError(
                    "Credential-bearing environment configuration is not accepted"
                )

        def required(name: str) -> str:
            value = values.get(name)
            if not isinstance(value, str) or not value or value != value.strip():
                raise EventContractError(f"Required nonsecret configuration is missing: {name}")
            return value

        try:
            tenant = required("AZURE_TENANT_ID")
            if UUID(tenant) != UUID(required("MONITORING_TENANT_ID")):
                raise EventContractError("Azure and monitoring tenant bindings must match")
            identity = IdentityBinding(
                tenant_id=tenant,
                client_id=required("AZURE_CLIENT_ID"),
                object_id=required("MONITORING_IDENTITY_OBJECT_ID"),
                subscription_id=required("AZURE_SUBSCRIPTION_ID"),
                resource_id=required("MONITORING_IDENTITY_RESOURCE_ID"),
            )
            connector = None
            if collector_only:
                if transport_probe or any(
                    value for key, value in values.items()
                    if key == "MONITORING_CONNECTOR_ID" or key.startswith("MONITORING_EVENTSTREAM_")
                ):
                    raise EventContractError("Collector-only mode cannot carry event transport configuration")
            else:
                endpoint = EndpointMetadata(
                    namespace=required("MONITORING_EVENTSTREAM_NAMESPACE"),
                    entity=required("MONITORING_EVENTSTREAM_ENTITY"),
                    consumer_group=required("MONITORING_EVENTSTREAM_CONSUMER_GROUP"),
                )
                connector = ConnectorBinding(
                    tenant_id=tenant,
                    connector_id=required("MONITORING_CONNECTOR_ID"),
                    workspace_id=required("MONITORING_EVENTSTREAM_WORKSPACE_ID"),
                    eventstream_id=required("MONITORING_EVENTSTREAM_ID"),
                    destination_id=required("MONITORING_EVENTSTREAM_DESTINATION_ID"),
                    endpoint=endpoint,
                )
                probe_helpers().Endpoint.from_document(
                    {
                        "type": "CustomEndpoint",
                        "fullyQualifiedNamespace": endpoint.namespace,
                        "eventHubName": endpoint.entity,
                        "consumerGroupName": endpoint.consumer_group,
                        "workspaceId": connector.workspace_id,
                        "eventstreamId": connector.eventstream_id,
                        "destinationId": connector.destination_id,
                    }
                )
        except (ValueError, ValidationError):
            raise EventContractError("Worker identity or connector metadata is invalid") from None
        server = "" if transport_probe else required("AZURE_SQL_SERVER")
        database = "" if transport_probe else required("AZURE_SQL_DATABASE")
        return cls(
            identity=identity,
            connector=connector,
            azure_sql_server=server,
            azure_sql_database=database,
            inventory_mode=values.get("MONITORING_INVENTORY_MODE", "caller_visible"),
        )

    def validate_intervals(self) -> None:
        if not 0 < self.heartbeat_seconds <= 60 or not 0 < self.maintenance_seconds <= 300:
            raise EventContractError("Heartbeat and maintenance intervals are outside their bounds")
        if not 1 <= self.retry_attempts <= 10 or not (
            0 < self.retry_base_seconds <= self.retry_max_seconds <= 60
        ):
            raise EventContractError("Worker retry policy is outside its bounded range")


class EventData(Protocol):
    sequence_number: int
    offset: str
    enqueued_time: datetime
    body: bytes | Iterable[bytes]


class PartitionContext(Protocol):
    partition_id: str
    fully_qualified_namespace: str
    eventhub_name: str
    consumer_group: str

    async def update_checkpoint(self, event: EventData) -> None: ...


class Consumer(Protocol):
    async def get_partition_ids(self) -> list[str]: ...
    async def get_partition_properties(self, partition_id: str) -> Mapping[str, object]: ...
    async def receive(self, on_event: Callable[..., Awaitable[None]], **kwargs: object) -> None: ...
    async def close(self) -> None: ...


class Collector(Protocol):
    async def run_once(self) -> CollectorRunResult: ...


class ConnectorMaintenance(Protocol):
    async def run_once(self) -> ReconcileRun: ...


ClientFactory = Callable[..., Consumer]


def create_event_hub_client(
    binding: ConnectorBinding,
    credential: AsyncCredential,
    *,
    checkpoint_store: SqlCheckpointStore | None,
    transport_probe: bool = False,
) -> Consumer:
    if (checkpoint_store is None) != transport_probe:
        raise EventContractError("Only an explicit transport probe may omit durable checkpoints")
    from azure.eventhub import TransportType
    from azure.eventhub.aio import EventHubConsumerClient

    # SDK callback failures can include arbitrary exception text. This process
    # reports bounded error classes itself instead of enabling AMQP/body tracing.
    logging.getLogger("azure").setLevel(logging.CRITICAL)
    logging.getLogger("uamqp").setLevel(logging.CRITICAL)
    return EventHubConsumerClient(
        fully_qualified_namespace=binding.endpoint.namespace,
        eventhub_name=binding.endpoint.entity,
        consumer_group=binding.endpoint.consumer_group,
        credential=credential,
        checkpoint_store=checkpoint_store,
        transport_type=TransportType.AmqpOverWebsocket,
        logging_enable=False,
        retry_total=3,
        retry_backoff_factor=1,
        retry_backoff_max=30,
        load_balancing_interval=20,
        partition_ownership_expiration_interval=120,
    )


def broker_position(event: EventData) -> StreamPosition:
    if type(event.sequence_number) is not int or event.sequence_number < 0:
        raise EventProtocolError("Broker sequence metadata is missing or malformed")
    offset = event.offset
    if not (isinstance(offset, str) or type(offset) is int) or not re.fullmatch(
        r"[0-9]{1,256}", str(offset)
    ):
        raise EventProtocolError("Broker offset metadata is missing or malformed")
    try:
        return StreamPosition(
            sequence_number=event.sequence_number,
            offset=str(offset),
            enqueued_at=event.enqueued_time,
        )
    except ValidationError:
        raise EventProtocolError("Broker enqueue time is missing or malformed") from None


async def _cancel_and_join(*tasks: asyncio.Task) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


class EventReceiver:
    def __init__(
        self,
        checkpoints: SqlCheckpointStore,
        identity: IdentityBinding,
        *,
        client_factory: ClientFactory = create_event_hub_client,
        credential_factory: Callable[
            [IdentityBinding], AsyncCredential
        ] = managed_identity_credential,
    ) -> None:
        if identity.tenant_id != checkpoints.binding.tenant_id:
            raise EventContractError("Receiver identity belongs to another connector tenant")
        self.checkpoints = checkpoints
        self.identity = identity
        self.client_factory = client_factory
        self.credential_factory = credential_factory
        self.connected = False
        self.candidate_envelopes = 0

    async def run(self, stop: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        failure: asyncio.Future[Exception] = loop.create_future()

        def failed(exc: Exception) -> None:
            if not failure.done():
                failure.set_result(exc)

        self.checkpoints.on_failure = failed
        credential = PinnedAsyncCredential(self.credential_factory(self.identity), self.identity)
        try:
            client = self.client_factory(
                self.checkpoints.binding,
                credential,
                checkpoint_store=self.checkpoints,
                transport_probe=False,
            )
        except Exception:
            await credential.close()
            raise
        receive_task: asyncio.Task | None = None
        stop_task: asyncio.Task | None = None
        verified_context: tuple[int, int, str, str, str] | None = None

        async def on_error(partition: PartitionContext | None, exc: Exception) -> None:
            failed(exc)

        async def initialize(partition: PartitionContext) -> None:
            try:
                properties = await client.get_partition_properties(partition.partition_id)
                await self.checkpoints.initialize_partition(partition.partition_id, properties)
            except Exception as exc:
                failed(exc)
                raise

        async def on_event(partition: PartitionContext, event: EventData | None) -> None:
            nonlocal verified_context
            if event is None:
                return
            if stop.is_set():
                # Prefetched deliveries remain unacknowledged for the next owner.
                return
            if failure.done():
                raise EventProtocolError(
                    "Reception is stopping; no later position may be acknowledged"
                )
            try:
                self.checkpoints.verify_endpoint(
                    partition.fully_qualified_namespace,
                    partition.eventhub_name,
                    partition.consumer_group,
                )
                control, manifest = await self.checkpoints._current()
                publication = await self.checkpoints._publication(manifest)
                current_context = (
                    control.revision, manifest.policy_revision, manifest.ownership_id,
                    connector_definition_hash(manifest.desired_definition), publication.publication_id,
                )
                if current_context != verified_context:
                    await credential.get_token("https://eventhubs.azure.net/.default")
                    self.checkpoints.set_partition_properties(
                        partition.partition_id, await client.get_partition_properties(partition.partition_id),
                    )
                    self.checkpoints.receiver_identity_verified_at = self.checkpoints.clock()
                    self.checkpoints.receiver_publication_id = publication.publication_id
                    verified_context = current_context
                summary = summarize_body(event.body)
                receipt = await self.checkpoints.accept(
                    partition.partition_id,
                    broker_position(event),
                    summary,
                )
                await partition.update_checkpoint(event)
                if isinstance(receipt, SignalReceipt) and receipt.status == "accepted":
                    self.candidate_envelopes += 1
                LOG.info(
                    "event_position_stored partition=%s sequence=%d disposition=%s",
                    partition.partition_id,
                    event.sequence_number,
                    "candidate"
                    if isinstance(receipt, SignalReceipt) and receipt.status == "accepted"
                    else "quarantine",
                )
            except Exception as exc:
                failed(exc)
                raise

        try:
            await credential.get_token("https://eventhubs.azure.net/.default")
            control, manifest = await self.checkpoints._current()
            publication = await self.checkpoints._publication(manifest)
            partitions = await client.get_partition_ids()
            self.checkpoints.bind_partitions(partitions)
            for partition_id in partitions:
                self.checkpoints.set_partition_properties(
                    partition_id,
                    await client.get_partition_properties(partition_id),
                )
            self.connected = True
            self.checkpoints.receiver_identity = self.identity
            self.checkpoints.receiver_identity_verified_at = self.checkpoints.clock()
            self.checkpoints.receiver_publication_id = publication.publication_id
            verified_context = (
                control.revision, manifest.policy_revision, manifest.ownership_id,
                connector_definition_hash(manifest.desired_definition), publication.publication_id,
            )
            receive_task = asyncio.create_task(
                client.receive(
                    on_event=on_event,
                    starting_position="-1",
                    max_wait_time=5,
                    prefetch=20,
                    on_error=on_error,
                    on_partition_initialize=initialize,
                )
            )
            stop_task = asyncio.create_task(stop.wait())
            done, _ = await asyncio.wait(
                (receive_task, stop_task, failure),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if failure in done:
                raise failure.result()
            if receive_task in done:
                await receive_task
                if not stop.is_set():
                    raise EventProtocolError("The event consumer returned without a stop request")
        finally:
            self.connected = False
            self.checkpoints.receiver_identity = None
            self.checkpoints.receiver_identity_verified_at = None
            self.checkpoints.receiver_publication_id = None
            try:
                async with asyncio.timeout(SHUTDOWN_SECONDS):
                    await client.close()
            finally:
                tasks = [task for task in (receive_task, stop_task) if task is not None]
                await _cancel_and_join(*tasks)
                await credential.close()


async def _wait_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        async with asyncio.timeout(seconds):
            await stop.wait()
    except TimeoutError:
        pass


def _retryable(exc: Exception) -> bool:
    if isinstance(exc, (EventContractError, EventProtocolError, StreamHistoryGap)):
        return False
    if isinstance(
        exc,
        (
            MonitoringUnavailable,
            MonitoringLeaseLost,
            MonitoringCommitUncertain,
            MonitoringConflict,
            OSError,
            TimeoutError,
        ),
    ):
        return True
    try:
        from azure.core.exceptions import AzureError
        from azure.eventhub.exceptions import EventHubError
    except ModuleNotFoundError:
        return False
    return isinstance(exc, (AzureError, EventHubError))


class MonitoringWorker:
    """Runs independent event, due-work and heartbeat loops; never invokes an agent."""

    def __init__(
        self,
        store: MonitoringStore,
        persistence: EventPersistence,
        collector: Collector,
        config: WorkerConfig,
        context: MonitoringContext,
        *,
        provisioner: ConnectorMaintenance | None = None,
        worker_id: str | None = None,
        client_factory: ClientFactory = create_event_hub_client,
        credential_factory: Callable[
            [IdentityBinding], AsyncCredential
        ] = managed_identity_credential,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        config.validate_intervals()
        self.store = store
        self.persistence = persistence
        self.collector = collector
        self.provisioner = provisioner
        self.config = config
        self.context = context
        self.worker_id = str(UUID(worker_id)) if worker_id else str(uuid4())
        self.clock = clock
        self.checkpoints = (
            SqlCheckpointStore(
                store, persistence=persistence, binding=config.connector, context=context, clock=clock,
            ) if config.connector is not None else None
        )
        self.receiver = (
            EventReceiver(
                self.checkpoints, config.identity, client_factory=client_factory,
                credential_factory=credential_factory,
            ) if self.checkpoints is not None else None
        )
        self.last_maintenance_at: datetime | None = None
        self.errors: dict[str, str] = {}

    async def _heartbeat(self, state: str) -> None:
        report = ReceiverHeartbeat(
            **self.context.model_dump(),
            worker_id=self.worker_id,
            connector_id=self.config.connector.connector_id if self.config.connector else None,
            observed_at=self.clock(),
            state="degraded" if self.errors and state == "running" else state,
            transport_connected=self.receiver.connected if self.receiver else False,
            accepted_positions=self.checkpoints.accepted_positions if self.checkpoints else 0,
            last_delivery_at=self.checkpoints.last_delivery_at if self.checkpoints else None,
            last_maintenance_at=self.last_maintenance_at,
            error_code=next(iter(self.errors.values()), None),
        )
        recorded = await asyncio.to_thread(self.persistence.record_receiver_heartbeat, report)
        if not isinstance(recorded, ReceiverHeartbeat) or (
            recorded.worker_id != report.worker_id
            or recorded.tenant_id != report.tenant_id
            or recorded.epoch != report.epoch
            or recorded.state != report.state
            or recorded.accepted_positions != report.accepted_positions
        ):
            raise EventProtocolError("Shared state did not confirm the worker heartbeat")
        LOG.info(
            "worker_heartbeat state=%s transport_connected=%s checkpointed_positions=%d",
            report.state,
            report.transport_connected,
            report.accepted_positions,
        )

    async def _maintain(self) -> None:
        collected = await self.collector.run_once()
        if any(result.state == "lease_lost" for result in collected.results):
            self.errors["collection"] = "CollectorWorkFenced"
        elif collected.claimed:
            self.errors.pop("collection", None)
        if self.provisioner is not None:
            report = await self.provisioner.run_once()
            pending = [result for result in report.results if result.state != "completed"]
            if pending:
                self.errors["connector_publication"] = pending[0].code
            elif report.claimed:
                self.errors.pop("connector_publication", None)
            elif "connector_publication" in self.errors:
                try:
                    _, effective = await current_connector(
                        self.store, self.config.connector, self.context,
                    )
                except EventContractError:
                    effective = None
                if effective is not None and effective.state == "ready":
                    self.errors.pop("connector_publication", None)
        self.last_maintenance_at = self.clock()

    async def _receive_when_configured(self, stop: asyncio.Event) -> None:
        if self.receiver is None or self.config.connector is None:
            raise EventContractError("Collector-only mode has no event receiver")
        if self.provisioner is None:
            await self.receiver.run(stop)
            return
        while not stop.is_set():
            try:
                _, manifest = await current_connector(
                    self.store, self.config.connector, self.context,
                )
                if manifest.source_proposals or manifest.source_removals:
                    raise EventContractError("Published source changes still need controller confirmation")
                self.errors.pop("event_admission", None)
                await self.receiver.run(stop)
                return
            except EventContractError:
                # Initial subscriptions and LROs must not be gated by delivery
                # proof. Keep maintenance running, but never report intake ready.
                self.errors["event_admission"] = "ConnectorNotReady"
                LOG.warning("event_intake_waiting_for_owned_connector")
                await _wait_or_stop(stop, self.config.maintenance_seconds)

    async def _supervise(
        self,
        name: str,
        operation: Callable[[], Awaitable[None]],
        stop: asyncio.Event,
        *,
        interval: float | None,
    ) -> None:
        attempts = 0
        while not stop.is_set():
            try:
                await operation()
                self.errors.pop(name, None)
                attempts = 0
                if interval is None:
                    if not stop.is_set():
                        raise EventProtocolError(
                            "A continuous worker operation stopped unexpectedly"
                        )
                    return
                await _wait_or_stop(stop, interval)
            except Exception as exc:
                code = type(exc).__name__
                self.errors[name] = code
                LOG.error("worker_operation_failed operation=%s error_class=%s", name, code)
                attempts += 1
                if name != "heartbeat":
                    try:
                        await self._heartbeat("degraded")
                    except MonitoringStoreError as health_error:
                        LOG.error(
                            "worker_health_unavailable error_class=%s", type(health_error).__name__
                        )
                if not _retryable(exc) or attempts >= self.config.retry_attempts:
                    raise
                await _wait_or_stop(
                    stop,
                    min(
                        self.config.retry_max_seconds,
                        self.config.retry_base_seconds * 2 ** (attempts - 1),
                    ),
                )

    async def run(self, stop: asyncio.Event) -> None:
        inspection = await asyncio.to_thread(
            self.store.inspect_bootstrap,
            expected_tenant_id=self.config.identity.tenant_id,
        )
        if inspection.status != "ready" or inspection.control is None:
            raise EventContractError(
                "Normal reception requires ready SQL bootstrap, not a transport-only fallback"
            )
        if inspection.control.epoch != self.context.epoch:
            raise MonitoringConflict("The worker context no longer matches the deployed epoch")
        if self.provisioner is None and self.config.connector is not None:
            await current_connector(self.store, self.config.connector, self.context)
        await self._heartbeat("starting")
        tasks = [
            asyncio.create_task(
                self._supervise(
                    "maintenance",
                    self._maintain,
                    stop,
                    interval=self.config.maintenance_seconds,
                )
            ),
            asyncio.create_task(
                self._supervise(
                    "heartbeat",
                    lambda: self._heartbeat("running"),
                    stop,
                    interval=self.config.heartbeat_seconds,
                )
            ),
        ]
        if self.receiver is not None:
            tasks.append(asyncio.create_task(
                self._supervise(
                    "events", lambda: self._receive_when_configured(stop), stop, interval=None,
                )
            ))
        stopped = asyncio.create_task(stop.wait())
        successful_stop = False
        try:
            done, _ = await asyncio.wait((*tasks, stopped), return_when=asyncio.FIRST_COMPLETED)
            for task in tasks:
                if task in done:
                    await task
                    if not stop.is_set():
                        raise EventProtocolError(
                            "A worker loop returned while monitoring was active"
                        )
            successful_stop = True
        finally:
            stop.set()
            try:
                async with asyncio.timeout(SHUTDOWN_SECONDS):
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    if any(isinstance(result, Exception) for result in results):
                        successful_stop = False
            except TimeoutError:
                successful_stop = False
                LOG.error("worker_shutdown_timeout")
            finally:
                await _cancel_and_join(*tasks, stopped)
            await self._heartbeat("stopped" if successful_stop else "blocked")
        if not successful_stop:
            raise EventProtocolError("Worker shutdown did not complete inside the grace period")


@dataclass(frozen=True)
class MaintenanceServices:
    store: MonitoringStore
    context: MonitoringContext
    collector: Collector
    provisioner: ConnectorMaintenance | None
    owner_id: str


@asynccontextmanager
async def live_maintenance(config: WorkerConfig):
    """Worker-only observations and published-intent execution, with no dotenv load."""
    if not config.azure_sql_server or not config.azure_sql_database:
        raise EventContractError(
            "Normal live startup requires the deployed SQL hostname and catalog"
        )
    from azure.identity import ManagedIdentityCredential

    from triage.monitoring.inventory import FabricInventoryClient
    from triage.monitoring.polling import (
        FabricPipelinePollingClient,
        MonitoringCollector,
        PowerBIPollingClient,
    )
    from triage.monitoring.provisioning import (
        ConnectorReconciler,
        OwnedEventCapabilityProbe,
        ProvisioningRestClient,
    )
    from triage.monitoring.runtime import build_monitoring_store
    from triage.monitoring.sql_store import KernelRateBudget
    from triage.policy import TriagePolicy
    from triage.store.azure_sql import AzureSqlDatabase

    credential = PinnedSyncCredential(
        ManagedIdentityCredential(client_id=config.identity.client_id),
        config.identity,
    )
    rest = None
    try:
        database = AzureSqlDatabase(
            server=config.azure_sql_server,
            database=config.azure_sql_database,
            credential=credential,
        )
        store = await asyncio.to_thread(
            build_monitoring_store, config, db=database, fixture=False, component="worker",
            # This component has no model/settings or remediation authority.
            policy=TriagePolicy(max_write_actions=0, allowed_actions=frozenset()),
        )
        inspection = await asyncio.to_thread(
            store.inspect_bootstrap,
            expected_tenant_id=config.identity.tenant_id,
        )
        if inspection.status != "ready" or inspection.control is None:
            raise EventContractError("The deployed monitoring registry is not ready")
        context = MonitoringContext(
            tenant_id=config.identity.tenant_id,
            epoch=inspection.control.epoch,
        )
        owner = str(uuid4())
        rest = ProvisioningRestClient(
            context,
            config.identity.object_id,
            credential,
            KernelRateBudget(database),
        )
        pipeline = FabricPipelinePollingClient(rest)
        inventory = FabricInventoryClient(rest, options=config.inventory_options())
        LOG.info(
            "inventory_api_selection mode=%s authority=%s adapter=%s",
            config.inventory_mode,
            inventory.authority,
            inventory.adapter,
        )
        collector = MonitoringCollector(
            store,
            context,
            inventory,
            pipeline,
            PowerBIPollingClient(rest),
            config.identity.object_id,
            owner,
            event_probe=(
                OwnedEventCapabilityProbe(store, context, rest, config.connector)
                if config.connector is not None else None
            ),
        )
        provisioner = (
            ConnectorReconciler(
                store, context, rest, pipeline, owner, config.connector.connector_id,
            ) if config.connector is not None else None
        )
        yield MaintenanceServices(store, context, collector, provisioner, owner)
    finally:
        if rest is not None:
            await rest.close()
        await asyncio.to_thread(credential.close)


@asynccontextmanager
async def live_worker(config: WorkerConfig):
    async with live_maintenance(config) as services:
        if not isinstance(services.store, EventPersistence):
            raise EventContractError(
                "The shared store must implement EventPersistence before live reception; "
                "--reconcile-once runs only the durable provisioning boundary"
            )
        yield MonitoringWorker(
            services.store,
            services.store,
            services.collector,
            config,
            services.context,
            worker_id=services.owner_id,
            provisioner=services.provisioner,
        )


@dataclass(frozen=True)
class ProbeResult:
    status: str
    received: int
    rejected: int

    @property
    def exit_code(self) -> int:
        return {"received": 0, "blocked": 2, "no_events": 3, "stopped": 130}[self.status]

    def document(self) -> dict[str, object]:
        return {
            "mode": "transport_probe",
            "status": self.status,
            "owned_envelopes_received": self.received,
            "rejected_envelopes": self.rejected,
            "sql_acceptance_proven": False,
            "durable_checkpoint_written": False,
            "normal_worker_health_verified": False,
            "source_rest_verified": False,
        }


async def transport_probe(
    config: WorkerConfig,
    *,
    source_workspace_id: str,
    item_id: str,
    seconds: float,
    stop: asyncio.Event | None = None,
    client_factory: ClientFactory = create_event_hub_client,
    credential_factory: Callable[[IdentityBinding], AsyncCredential] = managed_identity_credential,
) -> ProbeResult:
    """Read one explicitly selected owned canary; never construct a SQL store."""
    if not 0 < seconds <= 900:
        raise EventContractError("Transport probe duration must be between 0 and 900 seconds")
    helpers = probe_helpers()
    workspace = helpers.canonical_id(source_workspace_id)
    item = helpers.canonical_id(item_id)
    stopping = asyncio.Event() if stop is None else stop
    credential = credential_factory(config.identity)
    try:
        client = client_factory(
            config.connector, credential, checkpoint_store=None, transport_probe=True
        )
    except Exception:
        await credential.close()
        raise
    received = 0
    rejected = 0
    failure: asyncio.Future[Exception] = asyncio.get_running_loop().create_future()
    receive_task: asyncio.Task | None = None
    stop_task: asyncio.Task | None = None

    async def on_error(partition, exc: Exception) -> None:
        if not failure.done():
            failure.set_result(exc)

    async def on_event(partition: PartitionContext, event: EventData | None) -> None:
        nonlocal received, rejected
        if event is None:
            return
        summary = summarize_body(event.body)
        try:
            endpoint = config.connector.endpoint
            if (
                partition.fully_qualified_namespace,
                partition.eventhub_name,
                partition.consumer_group,
            ) != (endpoint.namespace, endpoint.entity, endpoint.consumer_group):
                raise EventProtocolError("Probe delivery came from another endpoint binding")
            position = broker_position(event)
            if summary.data is None:
                raise EventProtocolError("Oversized probe envelope")
            value = decode_envelope(summary)
            if value.get("dataschemaversion") != "1.0":
                raise EventProtocolError("Unverified native data schema")
            helpers.event_receipt(
                summary.data,
                tenant_id=config.identity.tenant_id,
                workspace_id=workspace,
                item_id=item,
            )
        except (ValueError, EventProtocolError, RecursionError):
            rejected += 1
            LOG.warning(
                "transport_probe_refused payload_sha256=%s payload_bytes=%d",
                summary.sha256,
                summary.size,
            )
            return
        received += 1
        LOG.info(
            "transport_probe_receipt partition=%s sequence=%d payload_sha256=%s",
            partition.partition_id,
            position.sequence_number,
            summary.sha256,
        )

    try:
        partitions = await client.get_partition_ids()
        if not partitions:
            raise EventProtocolError("The owned probe endpoint returned no partitions")
        receive_task = asyncio.create_task(
            client.receive(
                on_event=on_event,
                starting_position="-1",
                max_wait_time=5,
                prefetch=20,
                on_error=on_error,
            )
        )
        stop_task = asyncio.create_task(stopping.wait())
        done, _ = await asyncio.wait(
            (receive_task, stop_task, failure),
            timeout=seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if failure in done:
            raise failure.result()
        if receive_task in done:
            await receive_task
            if not stopping.is_set():
                raise EventProtocolError(
                    "The transport probe returned before its bound or stop request"
                )
        status = (
            "stopped"
            if stopping.is_set()
            else ("received" if received else ("blocked" if rejected else "no_events"))
        )
        return ProbeResult(status, received, rejected)
    finally:
        try:
            async with asyncio.timeout(SHUTDOWN_SECONDS):
                await client.close()
        finally:
            await _cancel_and_join(
                *[task for task in (receive_task, stop_task) if task is not None]
            )
            await credential.close()


async def _run_command(args, config: WorkerConfig) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.getsignal(sig)
        signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        if args.transport_probe:
            result = await transport_probe(
                config,
                source_workspace_id=args.probe_workspace_id,
                item_id=args.probe_item_id,
                seconds=args.probe_seconds,
                stop=stop,
            )
            print(json.dumps(result.document()), flush=True)
            return result.exit_code
        if args.reconcile_once:
            async with live_maintenance(config) as services:
                if services.provisioner is None:
                    raise EventContractError("Connector reconciliation requires explicit event configuration")
                result = await services.provisioner.run_once()
            records = [
                {"work_id": item.work_id, "state": item.state, "code": item.code}
                for item in result.results
            ]
            print(
                json.dumps(
                    {
                        "mode": "connector_reconcile",
                        "claimed": result.claimed,
                        "results": records,
                        "worker_ready": False,
                        "event_delivery_verified": False,
                    }
                ),
                flush=True,
            )
            if any(item.state == "waiting" for item in result.results):
                return 4
            return 2 if any(item.code != "definition_verified" for item in result.results) else 0
        async with live_worker(config) as worker:
            await worker.run(stop)
        print(json.dumps({"mode": "live", "status": "stopped"}), flush=True)
        return 0
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--transport-probe",
        action="store_true",
        help="Explicit read-only canary; no SQL acceptance or live health claim",
    )
    modes.add_argument(
        "--reconcile-once",
        action="store_true",
        help="Drain a bounded connector_reconcile batch using shared SQL work; not worker readiness",
    )
    modes.add_argument(
        "--collector-only",
        action="store_true",
        help="Run durable inventory and REST polling without requiring an Eventstream connector",
    )
    parser.add_argument(
        "--probe-workspace-id",
        help="Owned source canary workspace, distinct from the transport workspace",
    )
    parser.add_argument("--probe-item-id", help="Owned source canary pipeline")
    parser.add_argument("--probe-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    if args.transport_probe:
        if (
            not args.probe_workspace_id
            or not args.probe_item_id
            or not 1 <= args.probe_seconds <= 900
        ):
            parser.error(
                "Transport probe requires source workspace/item IDs and a 1-900 second bound"
            )
    elif args.probe_workspace_id or args.probe_item_id or args.probe_seconds != 120:
        parser.error("Probe arguments require --transport-probe")
    logging.basicConfig(level=logging.INFO, format="%(name)s %(levelname)s %(message)s")
    try:
        config = WorkerConfig.from_environment(
            transport_probe=args.transport_probe, collector_only=args.collector_only,
        )
        return asyncio.run(_run_command(args, config))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        # Pydantic/SDK exceptions may contain input or HTTP bodies. Only report
        # their type; the bounded contract error text is safe and actionable.
        error = {
            "mode": "transport_probe"
            if args.transport_probe
            else ("connector_reconcile" if args.reconcile_once else "live"),
            "status": "blocked",
            "error_class": type(exc).__name__,
        }
        if isinstance(exc, EventContractError):
            error["detail"] = str(exc)
        frames = traceback.extract_tb(exc.__traceback__)
        if frames:
            frame = frames[-1]
            error["location"] = {
                "file": Path(frame.filename).name, "function": frame.name, "line": frame.lineno,
            }
        LOG.error("worker_blocked error_class=%s", type(exc).__name__)
        print(json.dumps(error), flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
