from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
from test_monitoring_events import (
    DESTINATION,
    DESTINATION_WORKSPACE,
    EPOCH,
    EVENTSTREAM,
    ITEM,
    NOW,
    OWNER,
    WORKSPACE,
    MemoryBackend,
    adapter,
    identity_binding,
    identity_token,
    native_event,
    ownership_request,
    position,
)

from triage.monitoring.contracts import MonitoringUnavailable
from triage.monitoring.events import (
    EventContractError,
    EventProtocolError,
    ReceiverHeartbeat,
    SqlCheckpointStore,
)
from triage.monitoring.models import ConnectorSourceProposal, PendingSourceRemoval
from triage.monitoring.provisioning import ReconcileResult, ReconcileRun
from triage.monitoring.worker import (
    EventReceiver,
    MonitoringWorker,
    PinnedSyncCredential,
    WorkerConfig,
    create_event_hub_client,
    live_maintenance,
    main,
    transport_probe,
)


def worker_config(backend=None):
    backend = MemoryBackend() if backend is None else backend
    return WorkerConfig(
        identity=identity_binding(),
        connector=backend.binding,
        azure_sql_server="sample.database.windows.net",
        azure_sql_database="monitoring",
        heartbeat_seconds=0.02,
        maintenance_seconds=0.01,
        retry_base_seconds=0.001,
        retry_max_seconds=0.01,
    )


def environment():
    config = worker_config()
    return {
        "AZURE_TENANT_ID": config.identity.tenant_id,
        "MONITORING_TENANT_ID": config.identity.tenant_id,
        "AZURE_CLIENT_ID": config.identity.client_id,
        "AZURE_SUBSCRIPTION_ID": config.identity.subscription_id,
        "MONITORING_IDENTITY_OBJECT_ID": config.identity.object_id,
        "MONITORING_IDENTITY_RESOURCE_ID": config.identity.resource_id,
        "MONITORING_CONNECTOR_ID": config.connector.connector_id,
        "MONITORING_EVENTSTREAM_WORKSPACE_ID": DESTINATION_WORKSPACE,
        "MONITORING_EVENTSTREAM_ID": EVENTSTREAM,
        "MONITORING_EVENTSTREAM_DESTINATION_ID": DESTINATION,
        "MONITORING_EVENTSTREAM_NAMESPACE": "sample.servicebus.windows.net",
        "MONITORING_EVENTSTREAM_ENTITY": "owned-events",
        "MONITORING_EVENTSTREAM_CONSUMER_GROUP": "$Default",
        "AZURE_SQL_SERVER": config.azure_sql_server,
        "AZURE_SQL_DATABASE": config.azure_sql_database,
    }


class FakeCredential:
    def __init__(self, binding):
        self.binding = binding
        self.closed = False

    async def get_token(self, *scopes, **kwargs):
        return SimpleNamespace(
            token=identity_token(self.binding),
            expires_on=2_000_000_000,
        )

    async def close(self):
        self.closed = True


class FakeEvent:
    def __init__(self, value=None, sequence=100, *, raw=None):
        self.body = [
            raw
            if raw is not None
            else json.dumps(native_event() if value is None else value).encode()
        ]
        self.sequence_number = sequence
        self.offset = position(sequence).offset
        self.enqueued_time = position(sequence).enqueued_at

    def body_as_str(self):
        raise AssertionError("Do not materialize an unbounded decoded body")


class FakePartition:
    def __init__(self, checkpoint_store):
        self.partition_id = "0"
        self.fully_qualified_namespace = "sample.servicebus.windows.net"
        self.eventhub_name = "owned-events"
        self.consumer_group = "$Default"
        self.checkpoint_store = checkpoint_store
        self.checkpoint_calls = 0

    async def update_checkpoint(self, event):
        self.checkpoint_calls += 1
        if self.checkpoint_store is None:
            raise AssertionError("Transport probes must never checkpoint")
        await self.checkpoint_store.update_checkpoint(
            {
                "fully_qualified_namespace": self.fully_qualified_namespace,
                "eventhub_name": self.eventhub_name,
                "consumer_group": self.consumer_group,
                "partition_id": self.partition_id,
                "sequence_number": event.sequence_number,
                "offset": event.offset,
            }
        )


class FakeConsumer:
    def __init__(
        self,
        binding,
        credential,
        *,
        checkpoint_store,
        transport_probe=False,
        events=(),
        return_early=False,
        discovery_error=None,
    ):
        self.binding = binding
        self.credential = credential
        self.checkpoint_store = checkpoint_store
        self.transport_probe = transport_probe
        self.events = events
        self.return_early = return_early
        self.discovery_error = discovery_error
        self.closed = asyncio.Event()
        self.delivered = asyncio.Event()
        self.partition = FakePartition(checkpoint_store)
        self.receive_options = {}

    async def get_partition_ids(self):
        if self.discovery_error is not None:
            raise self.discovery_error
        return ["0"]

    async def get_partition_properties(self, partition_id):
        return {
            "id": partition_id,
            "eventhub_name": "owned-events",
            "is_empty": False,
            "beginning_sequence_number": 100,
            "last_enqueued_sequence_number": 120,
        }

    async def receive(self, on_event, **kwargs):
        self.receive_options = kwargs
        if self.checkpoint_store is not None:
            await self.checkpoint_store.claim_ownership([ownership_request(self.checkpoint_store)])
            await kwargs["on_partition_initialize"](self.partition)
        for event in self.events:
            try:
                await on_event(self.partition, event)
            except Exception as exc:
                # The real SDK catches callback failures and keeps receiving.
                # Its on_error callback must wake our supervisor and stop that.
                await kwargs["on_error"](self.partition, exc)
            await asyncio.sleep(0)
        self.delivered.set()
        if self.return_early:
            return
        await self.closed.wait()

    async def close(self):
        self.closed.set()


class FakeCollector:
    def __init__(self, *, fail_once=False):
        self.calls = 0
        self.fail_once = fail_once

    async def run_once(self):
        self.calls += 1
        if self.fail_once and self.calls == 1:
            raise MonitoringUnavailable("bounded fixture failure")
        return SimpleNamespace(claimed=0, results=(), gaps=())


def receiver_fixture(backend, *, events=(), **consumer_options):
    credentials = []
    consumers = []

    def credential_factory(binding):
        credential = FakeCredential(binding)
        credentials.append(credential)
        return credential

    def client_factory(binding, credential, **kwargs):
        client = FakeConsumer(
            binding,
            credential,
            events=events,
            **consumer_options,
            **kwargs,
        )
        consumers.append(client)
        return client

    return client_factory, credential_factory, consumers, credentials


async def wait_until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.001)


def test_production_mode_defaults_live_and_never_loads_fixture_from_azure():
    assert WorkerConfig.from_environment(environment()).monitoring_mode == "live"
    for mode in ("fixture", "demo", ""):
        values = {**environment(), "MONITORING_MODE": mode, "CONTAINER_APP_NAME": "worker"}
        with pytest.raises(EventContractError, match="refuses fixture/demo"):
            WorkerConfig.from_environment(values)


def test_inventory_api_mode_is_explicit_and_defaults_to_caller_visible():
    config = WorkerConfig.from_environment(environment())
    assert config.inventory_mode == "caller_visible"
    options = config.inventory_options()
    assert not options.admin_workspaces and not options.admin_domains
    assert not options.admin_items_preview and options.powerbi_datasets
    config = WorkerConfig.from_environment(
        {
            **environment(),
            "MONITORING_INVENTORY_MODE": "tenant_admin_preview",
        }
    )
    options = config.inventory_options()
    assert options.admin_workspaces and options.admin_domains and options.admin_items_preview
    assert options.powerbi_datasets


def test_collector_only_configuration_needs_no_eventstream_but_still_requires_sql():
    values = {
        key: value for key, value in environment().items()
        if key != "MONITORING_CONNECTOR_ID" and not key.startswith("MONITORING_EVENTSTREAM_")
    }
    config = WorkerConfig.from_environment(values, collector_only=True)
    assert config.connector is None and config.monitoring_mode == "live"
    with pytest.raises(EventContractError):
        WorkerConfig.from_environment(values)
    with pytest.raises(EventContractError):
        WorkerConfig.from_environment(environment(), collector_only=True)
    with pytest.raises(EventContractError):
        WorkerConfig.from_environment({**values, "AZURE_SQL_SERVER": ""}, collector_only=True)


def test_worker_store_factory_does_not_require_reasoning_settings(monkeypatch):
    from triage.monitoring import runtime, sql_store
    from triage.policy import TriagePolicy

    config = replace(worker_config(), connector=None)
    created = {}
    marker = object()

    def store(**kwargs):
        created.update(kwargs)
        return marker

    monkeypatch.setattr(sql_store, "AzureSqlMonitoringStore", store)
    monkeypatch.setattr(runtime, "inspect_context", lambda *args: None)
    policy = TriagePolicy(max_write_actions=0, allowed_actions=frozenset())
    assert runtime.build_monitoring_store(
        config, db=object(), component="worker", policy=policy,
    ) is marker
    assert created["policy"] is policy and created["component"] == "worker"


@pytest.mark.parametrize("fields", [
    {"transport_connected": True}, {"accepted_positions": 1}, {"last_delivery_at": NOW},
])
def test_collector_health_never_claims_event_delivery(fields):
    with pytest.raises(ValueError, match="cannot assert event"):
        ReceiverHeartbeat(
            **MemoryBackend().context.model_dump(), worker_id=OWNER, connector_id=None,
            observed_at=NOW, state="running", **fields,
        )


@pytest.mark.parametrize("mode", ["", "all", "tenant", "tenant_admin", "true", True, None, []])
def test_invalid_inventory_mode_does_not_silently_select_admin_or_partial_mode(mode):
    with pytest.raises(EventContractError, match="MONITORING_INVENTORY_MODE"):
        replace(worker_config(), inventory_mode=mode)


@pytest.mark.parametrize(
    ("mode", "authority"),
    [
        ("caller_visible", "caller_visible"),
        ("tenant_admin_preview", "tenant_admin"),
    ],
)
async def test_live_worker_factory_wires_the_selected_inventory_adapter(
    monkeypatch, mode, authority
):
    import sys
    from types import ModuleType

    backend = MemoryBackend()

    class Credential:
        def __init__(self, *, client_id):
            assert client_id == worker_config().identity.client_id

        def get_token(self, *args, **kwargs):
            raise AssertionError("Factory wiring must not make live calls in this test")

        def close(self):
            pass

    def build_store(settings, *, db, fixture, component, policy):
        assert settings.inventory_mode == mode
        assert settings.monitoring_mode == "live"
        assert fixture is False and db is not None
        assert component == "worker"
        assert policy.max_write_actions == 0 and not policy.allowed_actions
        return backend

    azure = ModuleType("azure")
    identity = ModuleType("azure.identity")
    azure.identity = identity
    identity.ManagedIdentityCredential = Credential
    monkeypatch.setitem(sys.modules, "azure", azure)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    monkeypatch.setattr("triage.monitoring.runtime.build_monitoring_store", build_store)
    config = replace(worker_config(backend), inventory_mode=mode)
    async with live_maintenance(config) as services:
        inventory = services.collector.inventory_client
        assert inventory.authority == authority
        assert inventory.options == config.inventory_options()
        assert services.collector.context == backend.context
        assert services.provisioner.store is services.store
        assert services.provisioner.claim.kinds == ("connector_reconcile",)


@pytest.mark.parametrize(
    "name",
    [
        "AZURE_CLIENT_SECRET",
        "AZURE_CLIENT_CERTIFICATE_PATH",
        "AZURE_PASSWORD",
    ],
)
def test_credential_bearing_environment_is_rejected(name):
    with pytest.raises(EventContractError, match="Credential-bearing"):
        WorkerConfig.from_environment({**environment(), name: "not-a-credential"})


def test_env_identity_tenant_and_sql_are_validated_without_echoing_values():
    values = environment()
    values["MONITORING_TENANT_ID"] = OWNER
    with pytest.raises(EventContractError):
        WorkerConfig.from_environment(values)
    values = environment()
    values["AZURE_SQL_DATABASE"] = "db;Password=DO_NOT_ECHO"
    with pytest.raises(EventContractError) as failure:
        WorkerConfig.from_environment(values)
    assert "DO_NOT_ECHO" not in str(failure.value)


def test_probe_config_omits_sql_only_when_explicit():
    values = environment()
    del values["AZURE_SQL_SERVER"]
    del values["AZURE_SQL_DATABASE"]
    with pytest.raises(EventContractError, match="AZURE_SQL_SERVER"):
        WorkerConfig.from_environment(values)
    config = WorkerConfig.from_environment(values, transport_probe=True)
    assert config.azure_sql_server == ""
    assert config.azure_sql_database == ""


async def test_receiver_stores_then_checkpoints_and_closes_on_stop():
    backend = MemoryBackend()
    client_factory, credential_factory, consumers, credentials = receiver_fixture(
        backend,
        events=(FakeEvent(),),
    )
    checkpoints = adapter(backend)
    receiver = EventReceiver(
        checkpoints,
        identity_binding(),
        client_factory=client_factory,
        credential_factory=credential_factory,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(receiver.run(stop))
    await wait_until(lambda: consumers and consumers[0].delivered.is_set())
    assert checkpoints.accepted_positions == 1
    assert receiver.candidate_envelopes == 1
    assert backend.intakes and backend.checkpoints
    assert consumers[0].receive_options["starting_position"] == "-1"
    stop.set()
    await asyncio.wait_for(task, 2)
    assert consumers[0].closed.is_set()
    assert credentials[0].closed
    assert checkpoints.receiver_identity is None and checkpoints.receiver_identity_verified_at is None
    assert checkpoints.receiver_publication_id is None
    assert backend.action_fences == {"existing-action-fence"}


@pytest.mark.parametrize("change", ["policy", "definition", "publication"])
async def test_changed_publication_reverifies_pinned_transport_before_recording_identity_time(change):
    backend = MemoryBackend()
    clients = []

    class ChangedContextConsumer(FakeConsumer):
        property_reads = 0

        async def get_partition_properties(self, partition_id):
            self.property_reads += 1
            return await super().get_partition_properties(partition_id)

        async def receive(self, on_event, **kwargs):
            backend.now += timedelta(seconds=5)
            if change == "policy":
                backend.control = backend.control.model_copy(update={"revision": 2})
                backend.manifest = backend.manifest.model_copy(update={"policy_revision": 2})
            elif change == "definition":
                definition = {**backend.manifest.desired_definition, "publication": "changed"}
                backend.manifest = backend.manifest.model_copy(update={
                    "desired_definition": definition, "observed_definition": definition,
                })
            else:
                backend.manifest = backend.manifest.model_copy(update={"name": "Renamed connector"})
            backend.publication = backend.publication.model_copy(update={
                "publication_id": ITEM, "policy_revision": backend.manifest.policy_revision,
                "published_at": backend.now,
            })
            await super().receive(on_event, **kwargs)

    def client_factory(binding, credential, **kwargs):
        client = ChangedContextConsumer(binding, credential, events=(FakeEvent(),), **kwargs)
        clients.append(client)
        return client

    checkpoints = adapter(backend)
    receiver = EventReceiver(
        checkpoints, identity_binding(), client_factory=client_factory, credential_factory=FakeCredential,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(receiver.run(stop))
    try:
        await wait_until(lambda: task.done() or clients and clients[0].delivered.is_set())
    finally:
        stop.set()
        await task
    receipt, = backend.positions.values()
    assert clients[0].property_reads == 3
    assert receipt.transport.identity_verified_at == NOW + timedelta(seconds=5)
    assert receipt.transport.policy_revision == backend.control.revision
    assert checkpoints.accepted_positions == 1


async def test_swallowed_sdk_callback_failure_stops_before_later_checkpoint(caplog):
    backend = MemoryBackend()
    backend.fail = "before_record"
    client_factory, credential_factory, consumers, credentials = receiver_fixture(
        backend,
        events=(FakeEvent(), FakeEvent(sequence=101)),
    )
    receiver = EventReceiver(
        adapter(backend),
        identity_binding(),
        client_factory=client_factory,
        credential_factory=credential_factory,
    )
    with pytest.raises(MonitoringUnavailable):
        await asyncio.wait_for(receiver.run(asyncio.Event()), 2)
    assert not backend.checkpoints
    assert consumers[0].partition.checkpoint_calls == 0
    assert consumers[0].closed.is_set()
    assert credentials[0].closed
    assert "DO_NOT_PERSIST_THIS_BODY" not in caplog.text


async def test_premature_consumer_return_is_not_a_success_shaped_empty_worker():
    backend = MemoryBackend()
    client_factory, credential_factory, _, _ = receiver_fixture(backend, return_early=True)
    receiver = EventReceiver(
        adapter(backend),
        identity_binding(),
        client_factory=client_factory,
        credential_factory=credential_factory,
    )
    with pytest.raises(EventProtocolError, match="returned without a stop"):
        await receiver.run(asyncio.Event())


async def test_worker_runs_independent_maintenance_and_durable_heartbeat_without_delivery_claim():
    backend = MemoryBackend()
    collector = FakeCollector(fail_once=True)
    client_factory, credential_factory, consumers, credentials = receiver_fixture(
        backend,
        events=(None,),
    )
    worker = MonitoringWorker(
        backend,
        backend,
        collector,
        worker_config(backend),
        backend.context,
        worker_id=OWNER,
        client_factory=client_factory,
        credential_factory=credential_factory,
        clock=lambda: backend.now,
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker.run(stop))
    await wait_until(lambda: collector.calls >= 2 and len(backend.heartbeats) >= 3)
    assert worker.last_maintenance_at == NOW
    assert backend.manifest.delivery_verified_at is None
    assert all(heartbeat.last_delivery_at is None for heartbeat in backend.heartbeats)
    assert any(heartbeat.state == "degraded" for heartbeat in backend.heartbeats)
    stop.set()
    await asyncio.wait_for(running, 2)
    assert backend.heartbeats[-1].state == "stopped"
    assert credentials[0].closed
    assert consumers[0].partition.checkpoint_calls == 0


async def test_collector_runs_on_an_empty_registry_without_creating_a_receiver():
    backend = MemoryBackend()
    collector = FakeCollector()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Initial inventory must not require event transport")

    worker = MonitoringWorker(
        backend, backend, collector, replace(worker_config(backend), connector=None),
        backend.context, worker_id=OWNER, client_factory=forbidden,
        credential_factory=forbidden, clock=lambda: backend.now,
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker.run(stop))
    await wait_until(lambda: collector.calls >= 2 and len(backend.heartbeats) >= 2)
    stop.set()
    await asyncio.wait_for(running, 2)
    assert worker.receiver is None and worker.checkpoints is None
    assert backend.heartbeats[-1].state == "stopped"
    assert all(
        row.connector_id is None and not row.transport_connected and row.accepted_positions == 0
        for row in backend.heartbeats
    )


def test_collector_health_persists_without_a_connector_in_the_shared_memory_store():
    from test_monitoring_store import Harness

    from triage.monitoring.memory import InMemoryMonitoringStore

    h = Harness()
    store = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="worker")
    heartbeat = ReceiverHeartbeat(
        **h.context(), worker_id=h.owner, connector_id=None,
        observed_at=h.clock(), state="running", last_maintenance_at=h.clock(),
    )
    assert store.record_receiver_heartbeat(heartbeat) == heartbeat
    assert all(record.kind != "connector" for record in h.state.records.values())


async def test_worker_maintenance_provisions_initial_sources_without_delivery_gate():
    backend = MemoryBackend()
    original = backend.manifest
    backend.manifest = original.model_copy(update={"sources": (), "state": "planned"})

    class Provisioner:
        calls = 0

        async def run_once(self):
            self.calls += 1
            backend.manifest = original
            return ReconcileRun(1, (ReconcileResult(OWNER, "completed", "definition_verified"),))

    provisioner = Provisioner()
    collector = FakeCollector()
    client_factory, credential_factory, consumers, _ = receiver_fixture(backend, events=(None,))
    worker = MonitoringWorker(
        backend,
        backend,
        collector,
        worker_config(backend),
        backend.context,
        provisioner=provisioner,
        client_factory=client_factory,
        credential_factory=credential_factory,
        clock=lambda: backend.now,
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker.run(stop))
    await wait_until(
        lambda: provisioner.calls > 0 and consumers and consumers[0].delivered.is_set()
    )
    assert collector.calls > 0
    assert backend.manifest.delivery_verified_at is None
    stop.set()
    await asyncio.wait_for(running, 2)
    assert backend.heartbeats[-1].state == "stopped"


async def test_worker_waits_for_effective_controller_publication_and_keeps_health_degraded():
    backend = MemoryBackend()
    original = backend.manifest
    backend.manifest = original.model_copy(update={"state": "provisioning"})

    class Provisioner:
        calls = 0

        async def run_once(self):
            self.calls += 1
            return (
                ReconcileRun(1, (
                    ReconcileResult(OWNER, "waiting", "awaiting_controller_publication"),
                ))
                if self.calls == 1 else ReconcileRun(0, ())
            )

    provisioner = Provisioner()
    client_factory, credential_factory, consumers, _ = receiver_fixture(backend, events=(None,))
    worker = MonitoringWorker(
        backend, backend, FakeCollector(), worker_config(backend), backend.context,
        provisioner=provisioner, client_factory=client_factory,
        credential_factory=credential_factory, clock=lambda: backend.now,
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker.run(stop))
    await wait_until(lambda: provisioner.calls > 1 and any(
        heartbeat.state == "degraded" for heartbeat in backend.heartbeats
    ))
    assert not consumers
    assert worker.errors["connector_publication"] == "awaiting_controller_publication"
    backend.manifest = type(original).model_validate({
        **original.model_dump(), "state": "ready",
        "identity_verified_at": NOW, "delivery_verified_at": NOW,
    })
    await wait_until(lambda: consumers and consumers[0].delivered.is_set()
                     and "connector_publication" not in worker.errors)
    stop.set()
    await asyncio.wait_for(running, 2)
    assert backend.heartbeats[-1].state == "stopped"


@pytest.mark.parametrize("pending_kind", ["proposal", "removal"])
async def test_existing_bound_sources_do_not_start_intake_with_pending_changes(pending_kind):
    backend = MemoryBackend()
    original = backend.manifest
    proposal = ConnectorSourceProposal(
        proposal_id=EPOCH, node_name="pending-approved-source", source_id=None,
        target=original.sources[0].target.model_copy(update={"item_id": OWNER}),
        event_types=original.sources[0].event_types,
    )
    pending = {"source_proposals": (proposal,)}
    if pending_kind == "removal":
        pending = {"source_removals": (PendingSourceRemoval(
            removal_id=EPOCH, source_id=original.sources[0].source_id, proposal_id=None,
            detail="Explicit pending fixture removal", node_name="owned-source",
            last_observed_source_id=None, target=original.sources[0].target,
            binding_hash="A" * 64, policy_revision=original.policy_revision,
            request_id=OWNER, publication_id=OWNER, requested_at=NOW,
            state="pending_remote_absence",
        ),)}
    backend.manifest = type(original).model_validate({**original.model_dump(), **pending})
    client_factory, credential_factory, consumers, _ = receiver_fixture(backend)
    worker = MonitoringWorker(
        backend, backend, FakeCollector(), worker_config(backend), backend.context,
        provisioner=FakeCollector(), client_factory=client_factory,
        credential_factory=credential_factory,
    )
    stop = asyncio.Event()
    running = asyncio.create_task(worker._receive_when_configured(stop))
    try:
        await wait_until(lambda: "event_admission" in worker.errors)
        assert not consumers
    finally:
        stop.set()
        await asyncio.wait_for(running, 2)


async def test_missing_sql_bootstrap_never_starts_transport_or_maintenance():
    backend = MemoryBackend()
    backend.inspect_bootstrap = lambda **_: SimpleNamespace(status="missing", control=None)
    collector = FakeCollector()
    client_factory, credential_factory, consumers, _ = receiver_fixture(backend)
    worker = MonitoringWorker(
        backend,
        backend,
        collector,
        worker_config(backend),
        backend.context,
        client_factory=client_factory,
        credential_factory=credential_factory,
    )
    with pytest.raises(EventContractError, match="ready SQL bootstrap"):
        await worker.run(asyncio.Event())
    assert not consumers
    assert collector.calls == 0
    assert not backend.heartbeats


async def test_worker_retry_limit_is_bounded_and_terminal_health_is_visible():
    backend = MemoryBackend()
    client_factory, credential_factory, consumers, _ = receiver_fixture(
        backend,
        discovery_error=MonitoringUnavailable("DO_NOT_LOG_RAW_DETAIL"),
    )
    worker = MonitoringWorker(
        backend,
        backend,
        FakeCollector(),
        replace(worker_config(backend), retry_attempts=2),
        backend.context,
        client_factory=client_factory,
        credential_factory=credential_factory,
        clock=lambda: backend.now,
    )
    with pytest.raises(MonitoringUnavailable):
        await asyncio.wait_for(worker.run(asyncio.Event()), 2)
    assert len(consumers) == 2
    assert backend.heartbeats[-1].state == "blocked"
    assert backend.heartbeats[-1].error_code == "MonitoringUnavailable"


async def test_cancellation_after_intake_leaves_recoverable_receipt_not_checkpoint():
    backend = MemoryBackend()
    recorded = asyncio.Event()
    waiting = asyncio.Event()

    class PauseAfterIntake(SqlCheckpointStore):
        async def accept(self, *args, **kwargs):
            result = await super().accept(*args, **kwargs)
            recorded.set()
            await waiting.wait()
            return result

    checkpoints = PauseAfterIntake(
        backend,
        persistence=backend,
        binding=backend.binding,
        context=backend.context,
        clock=lambda: backend.now,
    )
    client_factory, credential_factory, consumers, _ = receiver_fixture(
        backend,
        events=(FakeEvent(),),
    )
    receiver = EventReceiver(
        checkpoints,
        identity_binding(),
        client_factory=client_factory,
        credential_factory=credential_factory,
    )
    running = asyncio.create_task(receiver.run(asyncio.Event()))
    await asyncio.wait_for(recorded.wait(), 2)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert backend.intakes
    assert not backend.checkpoints
    assert consumers[0].partition.checkpoint_calls == 0
    assert backend.action_fences == {"existing-action-fence"}


@pytest.mark.parametrize(
    ("events", "status", "exit_code"),
    [
        ((), "no_events", 3),
        ((FakeEvent(),), "received", 0),
        ((FakeEvent(raw=b"not-json"),), "blocked", 2),
    ],
    ids=["quiet", "owned-receipt", "malformed"],
)
async def test_finite_probe_distinguishes_outcomes_and_never_claims_sql(events, status, exit_code):
    backend = MemoryBackend()
    client_factory, credential_factory, consumers, credentials = receiver_fixture(
        backend,
        events=events,
    )
    result = await transport_probe(
        worker_config(backend),
        source_workspace_id=WORKSPACE,
        item_id=ITEM,
        seconds=0.03,
        client_factory=client_factory,
        credential_factory=credential_factory,
    )
    assert result.status == status
    assert result.exit_code == exit_code
    assert result.document()["sql_acceptance_proven"] is False
    assert result.document()["durable_checkpoint_written"] is False
    assert result.document()["normal_worker_health_verified"] is False
    assert consumers[0].checkpoint_store is None
    assert consumers[0].partition.checkpoint_calls == 0
    assert not backend.intakes and not backend.heartbeats
    assert credentials[0].closed


async def test_probe_preserves_source_workspace_distinction_and_rejects_other_targets():
    backend = MemoryBackend()
    client_factory, credential_factory, _, _ = receiver_fixture(backend, events=(FakeEvent(),))
    result = await transport_probe(
        worker_config(backend),
        source_workspace_id=DESTINATION_WORKSPACE,
        item_id=ITEM,
        seconds=0.02,
        client_factory=client_factory,
        credential_factory=credential_factory,
    )
    assert result.status == "blocked"
    assert result.received == 0


async def test_probe_closes_credential_when_client_construction_fails():
    credential = FakeCredential(identity_binding())

    def broken_client(*args, **kwargs):
        raise EventProtocolError("fixture constructor failed")

    with pytest.raises(EventProtocolError):
        await transport_probe(
            worker_config(),
            source_workspace_id=WORKSPACE,
            item_id=ITEM,
            seconds=1,
            client_factory=broken_client,
            credential_factory=lambda _: credential,
        )
    assert credential.closed


async def test_real_sdk_construction_is_websocket_443_and_durable_by_default():
    pytest.importorskip("azure.eventhub")
    from azure.eventhub import TransportType

    backend = MemoryBackend()
    checkpoints = adapter(backend)
    client = create_event_hub_client(
        backend.binding, FakeCredential(identity_binding()), checkpoint_store=checkpoints
    )
    try:
        assert client._checkpoint_store is checkpoints
        assert client._config.transport_type == TransportType.AmqpOverWebsocket
        assert client._config.connection_port == 443
        assert client._config.network_tracing is False
    finally:
        await client.close()
    with pytest.raises(EventContractError, match="explicit transport probe"):
        create_event_hub_client(
            backend.binding, FakeCredential(identity_binding()), checkpoint_store=None
        )


def test_sql_and_rest_credential_wrapper_also_checks_managed_identity_binding():
    binding = identity_binding()

    class Credential:
        def get_token(self, *scopes, **kwargs):
            return SimpleNamespace(
                token=identity_token(binding, oid=EPOCH), expires_on=2_000_000_000
            )

        def close(self):
            pass

    credential = PinnedSyncCredential(Credential(), binding)
    with pytest.raises(EventProtocolError):
        credential.get_token("https://database.windows.net/.default")


def test_cli_help_is_offline_and_probe_arguments_are_explicit(capsys):
    with pytest.raises(SystemExit) as result:
        main(["--help"])
    assert result.value.code == 0
    assert "--transport-probe" in capsys.readouterr().out
    with pytest.raises(SystemExit) as result:
        main(["--probe-item-id", ITEM])
    assert result.value.code == 2


def test_cli_missing_env_reports_blocked_without_printing_credentials(monkeypatch, capsys):
    monkeypatch.setenv("MONITORING_MODE", "live")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "DO_NOT_PRINT_THIS_SECRET")
    assert main([]) == 2
    output = capsys.readouterr().out
    assert json.loads(output)["status"] == "blocked"
    assert "DO_NOT_PRINT_THIS_SECRET" not in output
