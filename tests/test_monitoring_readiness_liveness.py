from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest
from test_monitoring_connector_retirement_store import scoped_connector
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_event_readiness import native_delivery
from test_monitoring_events import (
    ITEM,
    OWNER,
    MemoryBackend,
    adapter,
    identity_binding,
    ownership_request,
)
from test_monitoring_initial_connector_publication import (
    binding,
    fresh_event_capability,
    register_physical_fixture,
)
from test_monitoring_inventory import CONTEXT, IDENTITY
from test_monitoring_sql_receiver_bindings import ReceiverAbiDatabase
from test_monitoring_sql_review9_bindings import connector_commit
from test_monitoring_store import uid
from test_monitoring_worker import (
    FakeConsumer,
    FakeCredential,
    FakeEvent,
    receiver_fixture,
    wait_until,
)

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringConflict, MonitoringUnavailable
from triage.monitoring.events import (
    IdentityBinding,
    OwnershipChange,
    SqlCheckpointStore,
    StreamStartRequest,
)
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.provisioning import prepare_connector_publication
from triage.monitoring.sql_store import AzureSqlMonitoringStore
from triage.monitoring.worker import EventReceiver


def delivery_fixture(backend):
    h, db, _, _, worker, owned = scoped_connector(backend, database_type=ReceiverAbiDatabase)
    commit = connector_commit(h, worker, owned.connector_id, db=db)
    owned = worker.record_connector(
        h.version, m.OwnedConnectorManifest.model_validate({
            **owned.model_dump(), "revision": owned.revision + 1, "state": "degraded",
            "observed_definition": owned.desired_definition,
            "gaps": (m.CoverageGap(code="awaiting_delivery", detail="Awaiting current publication delivery"),),
        }),
        expected_connector_revision=owned.revision, commit=commit,
    )
    desired = worker.get_connector_desired(h.version, owned.connector_id)
    assert desired is not None
    h.clock.advance(1)
    return h, db, worker, owned, desired


def accept_delivery(h, worker, owned, partition_id, *, enqueued_at):
    partition = m.PartitionIdentity(
        **h.context(), connector_id=owned.connector_id,
        consumer_group=owned.endpoint.consumer_group, partition_id=partition_id,
    )
    ownership = worker.change_partition_ownership(OwnershipChange(
        partition=partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=partition, owner_id=h.owner, initial_sequence_number=100),
    ))
    worker.ensure_stream_start(StreamStartRequest(
        partition=partition, lease=ownership.lease, first_available_sequence_number=100, observed_at=h.clock(),
    ))
    request_id = h.next_id()
    source = owned.sources[0]
    signal = m.SignalReceipt(
        delivery=m.TransportDeliveryIdentity(
            **h.context(), connector_id=owned.connector_id, event_source=source.event_source, event_id=request_id,
        ),
        partition=partition, position=m.StreamPosition(offset="1000", sequence_number=100, enqueued_at=enqueued_at),
        received_at=h.clock(), event_type=source.event_types[0], status="accepted",
        observation=h.observation(source.target, origin="event", authority="transport", status="running"),
        transport=m.EventTransportEvidence(
            request_id=request_id, ownership_id=owned.ownership_id, policy_revision=owned.policy_revision,
            workspace_id=owned.workspace_id, eventstream_id=owned.eventstream_id,
            destination_id=owned.destination_id, endpoint=owned.endpoint,
            definition_hash=m.connector_definition_hash(owned.desired_definition), source_id=source.source_id,
            collector_identity_id=uid(4), identity_verified_at=h.clock(),
        ),
    )
    intake = worker.record_stream_receipts(m.StreamReceiptBatch(
        request_id=request_id, partition=partition, lease=ownership.lease, receipts=(signal,),
    ))
    assert intake.receipt_keys == (signal.delivery.key,)
    return signal


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("late_only", [False, True])
def test_later_backlog_in_another_partition_cannot_shadow_original_current_delivery(backend, late_only):
    h, db, worker, owned, desired = delivery_fixture(backend)
    first = None
    if not late_only:
        first = accept_delivery(h, worker, owned, "0", enqueued_at=h.clock())
    h.clock.advance(5)
    late = accept_delivery(
        h, worker, owned, "1", enqueued_at=desired.published_at - timedelta(seconds=1),
    )
    assert h.control.activation_cutoff < late.position.enqueued_at < desired.published_at
    assert first is None or late.received_at > first.received_at
    restarted = (
        AzureSqlMonitoringStore(db=db, component="worker") if db
        else InMemoryMonitoringStore(state=h.state, clock=h.clock, component="worker")
    )
    proof = restarted.get_connector_delivery(h.version, owned.connector_id, uid(4))
    if late_only:
        assert proof is None
    else:
        assert proof == m.ConnectorDeliveryProof(
            request_id=first.transport.request_id, receipt_key=first.delivery.key,
            collector_identity_id=uid(4), received_at=first.received_at,
            identity_verified_at=first.transport.identity_verified_at,
        )


@pytest.mark.parametrize("damage", ["receipt", "hash"])
def test_current_delivery_receipt_or_hash_damage_remains_loud_with_later_backlog(damage):
    h, db, worker, owned, desired = delivery_fixture("sql")
    first = accept_delivery(h, worker, owned, "0", enqueued_at=h.clock())
    h.clock.advance(5)
    accept_delivery(h, worker, owned, "1", enqueued_at=desired.published_at - timedelta(seconds=1))
    if damage == "receipt":
        del db.receipts[("worker.commit_positions", first.transport.request_id)]
    else:
        key = f"{first.partition.key}:position:{first.position.sequence_number}"
        row = db.records[("stream_position", key)]
        payload = {**json.loads(row.payload), "payload_hash": "0" * 64}
        db.native_put("stream_position", key, payload, status="accepted", parent_key=first.partition.key,
                      sequence_number=first.position.sequence_number)
    restarted = AzureSqlMonitoringStore(db=db, component="worker")
    with pytest.raises(MonitoringUnavailable, match="original native acceptance|exact durable receipt"):
        restarted.get_connector_delivery(h.version, owned.connector_id, uid(4))


async def test_publication_race_before_attaching_proof_does_not_accept_or_checkpoint(monkeypatch):
    backend = MemoryBackend()
    original = backend.get_connector_desired
    reads = 0

    def publication(context, connector_id):
        nonlocal reads
        reads += 1
        if reads == 3:
            backend.publication = backend.publication.model_copy(update={"publication_id": ITEM})
        return original(context, connector_id)

    monkeypatch.setattr(backend, "get_connector_desired", publication)
    client_factory, credential_factory, _, _ = receiver_fixture(backend, events=(FakeEvent(),))
    receiver = EventReceiver(
        adapter(backend), identity_binding(), client_factory=client_factory, credential_factory=credential_factory,
    )
    with pytest.raises(MonitoringConflict, match="publication changed after"):
        await asyncio.wait_for(receiver.run(asyncio.Event()), 3)
    assert reads == 3 and backend.positions == {} and backend.intakes == {} and backend.checkpoints == {}


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_alive_receiver_reverifies_same_policy_name_only_publication(backend, tmp_path):
    fixture = PublicationHarness(backend, tmp_path, register_transport=False)
    await fixture.collect(event_status="unknown")
    await fixture.execute(fixture.activate())
    register_physical_fixture(fixture)
    await fresh_event_capability(fixture)
    await fixture.drain()
    await fixture.apply_worker()
    await fixture.drain()
    fixture.h.clock.advance(1)
    worker = fixture.use("worker")
    bound = binding(fixture.connector("worker"))
    checkpoints = SqlCheckpointStore(worker, persistence=worker, binding=bound, context=CONTEXT, clock=fixture.h.clock)
    identity = IdentityBinding(
        tenant_id=CONTEXT.tenant_id, object_id=IDENTITY, client_id=uid(96_000), subscription_id=uid(96_001),
        resource_id=(
            f"/subscriptions/{uid(96_001)}/resourceGroups/fixture/providers/"
            "Microsoft.ManagedIdentity/userAssignedIdentities/worker"
        ),
    )
    clients = []

    class LiveConsumer(FakeConsumer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.queue = asyncio.Queue()
            self.checkpoints_at_broker_read = []

        async def get_partition_properties(self, partition_id):
            position = await asyncio.to_thread(worker.get_stream_checkpoint, checkpoints.partition(partition_id))
            self.checkpoints_at_broker_read.append(position.position.sequence_number if position else None)
            return await super().get_partition_properties(partition_id)

        async def receive(self, on_event, **kwargs):
            await checkpoints.claim_ownership([ownership_request(checkpoints, owner=OWNER)])
            await kwargs["on_partition_initialize"](self.partition)
            while True:
                event = await self.queue.get()
                try:
                    if event is None:
                        return
                    try:
                        await on_event(self.partition, event)
                    except Exception as exc:
                        await kwargs["on_error"](self.partition, exc)
                finally:
                    self.queue.task_done()

        async def close(self):
            self.queue.put_nowait(None)
            await super().close()

    def client_factory(binding, credential, **kwargs):
        client = LiveConsumer(binding, credential, **kwargs)
        clients.append(client)
        return client

    receiver = EventReceiver(checkpoints, identity, client_factory=client_factory, credential_factory=FakeCredential)
    stop = asyncio.Event()
    task = asyncio.create_task(receiver.run(stop))
    try:
        await wait_until(lambda: task.done() or clients and len(clients[0].checkpoints_at_broker_read) == 2)
        client = clients[0]
        await client.queue.put(native_delivery(fixture.h.clock, fixture.identity))
        await asyncio.wait_for(client.queue.join(), 3)
        await fixture.drain()
        await fixture.apply_worker()
        await fixture.drain()
        first = fixture.connector()
        assert first.state == "ready" and first.delivery_proof is not None and not task.done()
        first_desired = fixture.use("controller").get_connector_desired(CONTEXT, bound.connector_id)

        fixture.h.clock.advance(5)
        fixture.use("web").request_discovery(
            fixture.version("web"), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
            request_id=fixture.h.next_id(),
        )
        work, = fixture.claim("controller", "reconcile_state")
        controller = fixture.use("controller")
        request = prepare_connector_publication(controller, work, bound.connector_id, request_id=fixture.h.next_id())
        request = m.ConnectorPublicationRequest.model_validate({
            **request.model_dump(), "name": "Renamed owned transport",
        })
        changed = controller.publish_connector(request)
        assert changed.desired_changed and changed.connector.policy_revision == first.policy_revision
        assert changed.connector.desired_definition == first.desired_definition
        current_desired = controller.get_connector_desired(CONTEXT, bound.connector_id)
        assert current_desired.publication_id != first_desired.publication_id
        assert current_desired.published_at > first.identity_verified_at
        controller.reconcile_work(work)
        await fixture.apply_worker()
        await fixture.drain()
        assert fixture.connector().state == "degraded" and not task.done() and len(clients) == 1

        fixture.h.clock.advance(1)
        fixture.use("worker")
        second = native_delivery(fixture.h.clock, fixture.identity)
        second.sequence_number, second.offset = 101, "2020"
        payload = json.loads(second.body[0])
        payload["id"] = "delivery-after-name-only-publication"
        payload["data"]["jobInstanceId"] = uid(96_002)
        payload["subject"] = (
            f"/workspaces/{fixture.identity.workspace_id}/items/{fixture.identity.item_id}/jobs/instances/{uid(96_002)}"
        )
        second.body = [json.dumps(payload).encode()]
        await client.queue.put(second)
        await asyncio.wait_for(client.queue.join(), 3)
        assert client.checkpoints_at_broker_read == [None, None, 100]
        assert checkpoints.accepted_positions == 2 and not task.done()
        proof = worker.get_connector_delivery(CONTEXT, bound.connector_id, IDENTITY)
        assert proof.identity_verified_at == fixture.h.clock() > current_desired.published_at
        assert proof.request_id != first.delivery_proof.request_id
        await fixture.drain()
        await fixture.apply_worker()
        await fixture.drain()
        assert fixture.connector().state == "ready" and fixture.connector().delivery_proof == proof
        assert len(clients) == 1 and not task.done()
    finally:
        fixture.use("worker")
        stop.set()
        await asyncio.wait_for(task, 3)
