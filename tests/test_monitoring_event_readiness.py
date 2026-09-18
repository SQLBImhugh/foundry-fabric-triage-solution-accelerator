from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import httpx
import pytest
from test_monitoring_inventory import CONTEXT, IDENTITY, Credential
from test_monitoring_polling import collector, target
from test_monitoring_provisioning import (
    CONNECTOR,
    SOURCE_EVENTS,
    connector,
    decode_snapshot,
    setup_store,
    version,
    wire_definition,
)
from test_monitoring_store import uid
from test_monitoring_worker import FakeConsumer, FakeCredential, FakeEvent, wait_until

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringConflict, MonitoringUnavailable
from triage.monitoring.events import (
    ConnectorBinding,
    IdentityBinding,
    ReceiverHeartbeat,
    SqlCheckpointStore,
)
from triage.monitoring.memory import InMemoryMonitoringStore, StoredRecord, key_digest
from triage.monitoring.polling import FabricPipelinePollingClient
from triage.monitoring.provisioning import (
    ConnectorReconciler,
    OwnedEventCapabilityProbe,
    ProvisioningRestClient,
)
from triage.monitoring.rate_limit import InMemoryRateBudget
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.monitoring.worker import EventReceiver


def registered_fixture():
    fixture, state, clock, remote = setup_store()
    prior = connector(fixture)
    baseline = decode_snapshot(wire_definition(remote.graph), remote.topology())
    current = fixture.record_connector(
        version(fixture),
        m.OwnedConnectorManifest.model_validate({
            **prior.model_dump(), "revision": prior.revision + 1,
            "sources": tuple(source.model_copy(update={"event_types": SOURCE_EVENTS}) for source in prior.sources),
            "desired_definition": baseline, "observed_definition": baseline,
            "state": "degraded",
            "gaps": (m.CoverageGap(code="awaiting_source_delivery_proof", detail="No delivery has been observed"),),
        }),
        expected_connector_revision=prior.revision,
    )
    # Deployer-fixture provenance for the already registered physical baseline;
    # there are no identity, delivery, readiness or action assertions.
    desired = m.ConnectorDesiredState(
        connector_id=current.connector_id, ownership_id=current.ownership_id,
        publication_id=uid(91_000), policy_revision=current.policy_revision,
        sources_hash=m._digest([source.model_dump(mode="json") for source in current.sources]),
        definition_hash=m.connector_definition_hash(baseline), published_at=clock(),
    )
    state.records[(
        CONTEXT.tenant_id, CONTEXT.epoch, "connector_desired", key_digest(CONNECTOR),
    )] = StoredRecord(
        kind="connector_desired", key=CONNECTOR, context=CONTEXT,
        payload=desired.model_dump_json(), version=1,
    )
    binding = ConnectorBinding(
        tenant_id=CONTEXT.tenant_id, connector_id=CONNECTOR,
        workspace_id=current.workspace_id, eventstream_id=current.eventstream_id,
        destination_id=current.destination_id, endpoint=current.endpoint,
    )
    worker = InMemoryMonitoringStore(state=state, clock=clock, component="worker")
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    rest = ProvisioningRestClient(
        CONTEXT, IDENTITY, Credential(clock), InMemoryRateBudget(clock=clock),
        transport=httpx.MockTransport(remote), clock=clock,
    )
    return fixture, worker, controller, state, clock, remote, binding, rest


def drain_controller(controller):
    results = []
    for _ in range(20):
        claimed = controller.claim_work(m.WorkClaimRequest(
            **CONTEXT.model_dump(), owner_id=uid(91_001), kinds=("reconcile_state",),
            limit=1, per_workspace_limit=1,
        ))
        if not claimed:
            return results
        results.append(controller.reconcile_work(claimed[0]))
    raise AssertionError("Controller publication did not reach bounded idle")


async def collect_capability(worker, controller, clock, binding, rest):
    current = controller.snapshot(CONTEXT).control
    work_id = uid(91_002)
    controller.enqueue_work(m.MonitoringWorkDraft(
        **CONTEXT.model_dump(), work_id=work_id, target=target(), kind="capability_probe",
        policy_revision=current.revision, created_at=clock(), due_at=clock(),
        reason="Verify the configured owned event path through the normal collector",
    ))
    work, = worker.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=uid(91_003), kinds=("capability_probe",),
        limit=1, per_workspace_limit=1,
    ))
    assert work.work_id == work_id
    service = collector(
        worker, rest, clock,
        event_probe=OwnedEventCapabilityProbe(worker, CONTEXT, rest, binding, clock=clock),
    )
    result = await service._capability(work)
    return result


def native_delivery(clock, identity):
    run_id = uid(91_004)
    now = clock().isoformat()
    event = FakeEvent({
        "source": identity.tenant_id, "id": "readiness-delivery",
        "subject": f"/workspaces/{identity.workspace_id}/items/{identity.item_id}/jobs/instances/{run_id}",
        "type": "Microsoft.Fabric.JobEvents.ItemJobFailed",
        "specversion": "1.0", "dataschemaversion": "1.0", "time": now,
        "data": {
            "workspaceId": identity.workspace_id, "itemId": identity.item_id, "itemKind": "Pipeline",
            "jobInstanceId": run_id, "jobStatus": "Failed", "jobType": "Pipeline",
            "jobInovkeType": "Scheduled", "jobStartTime": now, "jobEndTime": now,
        },
    })
    event.enqueued_time = clock()
    return event


async def receive(worker, clock, binding, events, *, delivery_delay_seconds=0):
    identity = IdentityBinding(
        tenant_id=CONTEXT.tenant_id, object_id=IDENTITY, client_id=uid(91_005), subscription_id=uid(91_006),
        resource_id=(
            f"/subscriptions/{uid(91_006)}/resourceGroups/fixture/providers/"
            "Microsoft.ManagedIdentity/userAssignedIdentities/worker"
        ),
    )
    clients = []

    class DelayedConsumer(FakeConsumer):
        async def receive(self, on_event, **kwargs):
            clock.advance(delivery_delay_seconds)
            await super().receive(on_event, **kwargs)

    def client_factory(bound, credential, **kwargs):
        client = DelayedConsumer(bound, credential, events=events, **kwargs)
        clients.append(client)
        return client

    checkpoints = SqlCheckpointStore(worker, persistence=worker, binding=binding, context=CONTEXT, clock=clock)
    receiver = EventReceiver(
        checkpoints, identity, client_factory=client_factory, credential_factory=FakeCredential,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(receiver.run(stop))
    try:
        await wait_until(lambda: task.done() or clients and clients[0].delivered.is_set())
    finally:
        stop.set()
        await task
    return checkpoints


async def test_owned_event_capability_uses_fresh_collector_reads_and_fenced_publication():
    fixture, worker, controller, state, clock, remote, binding, rest = registered_fixture()
    try:
        result = await collect_capability(worker, controller, clock, binding, rest)
        assert result.state == "recorded"
        raw = [
            m.CapabilityObservation.model_validate_json(row.payload)
            for row in state.records.values() if row.kind == "capability"
        ]
        proof, = [value for value in raw if value.event_evidence is not None]
        assert proof.read_status == proof.event_status == "verified"
        assert proof.action_status == "unknown" and not proof.exact_action_correlation
        assert proof.event_evidence.source_id == connector(fixture).sources[0].source_id
        assert proof.collector_identity_id == IDENTITY
        assert not worker.resolve_target(target()).observation.events_enabled
        assert connector(worker).delivery_proof is None
        assert remote.definition_reads == 1 and remote.update_bodies == []
        assert not any("/connection" in str(request.url) for request in remote.requests)
        drain_controller(controller)
        assert not controller.resolve_target(target()).observation.events_enabled
        assert not controller.resolve_target(target()).action.enabled
    finally:
        await rest.close()


@pytest.mark.parametrize("fault", ["stopped", "source_id", "item_id", "events", "destination", "endpoint"])
async def test_event_capability_never_infers_proof_from_registered_labels(fault):
    _, worker, controller, _, clock, remote, binding, rest = registered_fixture()
    if fault == "stopped":
        remote.status = "Stopped"
    elif fault == "source_id":
        remote.graph["sources"][0]["id"] = uid(92_001)
    elif fault == "item_id":
        remote.graph["sources"][0]["properties"]["itemId"] = uid(92_002)
    elif fault == "events":
        remote.graph["sources"][0]["properties"]["includedEventTypes"] = [SOURCE_EVENTS[0]]
    elif fault == "destination":
        remote.graph["destinations"][0]["id"] = uid(92_003)
    else:
        binding = binding.model_copy(update={
            "endpoint": binding.endpoint.model_copy(update={"entity": "another-owned-name"}),
        })
    try:
        result = await collect_capability(worker, controller, clock, binding, rest)
        assert result.gaps and not any(
            value.observation.events_enabled for value in worker.list_targets(m.TargetQuery(**CONTEXT.model_dump())).items
        )
        if fault != "endpoint":
            assert result.state == "deferred"
        assert remote.update_bodies == []
    finally:
        await rest.close()


async def test_normal_receiver_receipt_survives_restart_and_only_controller_publishes_ready():
    _, worker, controller, state, clock, _, binding, rest = registered_fixture()
    try:
        await collect_capability(worker, controller, clock, binding, rest)
        drain_controller(controller)
        worker.record_receiver_heartbeat(ReceiverHeartbeat(
            **CONTEXT.model_dump(), worker_id=uid(91_010), connector_id=CONNECTOR,
            observed_at=clock(), state="running", transport_connected=True,
            accepted_positions=100, last_delivery_at=clock(),
        ))
        assert worker.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY) is None
        clock.advance(1)
        identity_verified_at = clock()
        checkpoints = await receive(
            worker, clock, binding, (native_delivery(clock, target()),), delivery_delay_seconds=5,
        )
        assert checkpoints.accepted_positions == 1
        proof = worker.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY)
        assert proof is not None
        assert proof.identity_verified_at == identity_verified_at < proof.received_at
        assert connector(worker).state == "degraded" and connector(worker).delivery_proof is None
        restarted = InMemoryMonitoringStore(state=state, clock=clock, component="worker")
        assert restarted.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY) == proof
        drain_controller(controller)
        run = await ConnectorReconciler(
            restarted, CONTEXT, rest, FabricPipelinePollingClient(rest),
            uid(91_011), CONNECTOR, clock=clock, batch_size=1,
        ).run_once()
        assert run.claimed == 1 and run.results[0].code == "awaiting_controller_publication"
        assert connector(restarted).state == "provisioning"
        assert connector(restarted).delivery_proof is None
        drain_controller(controller)
        ready = connector(controller)
        assert ready.state == "ready" and ready.delivery_proof == proof
        assert ready.identity_verified_at == identity_verified_at
        assert ready.delivery_verified_at == proof.received_at
        assert controller.resolve_target(target()).observation.events_enabled
        assert not controller.resolve_target(target()).action.enabled
        assert not any(row.kind in {"action", "action_owner"} for row in state.records.values())
    finally:
        await rest.close()


@pytest.mark.parametrize("fault", ["empty", "malformed", "foreign", "policy", "receipt", "endpoint"])
async def test_empty_quarantined_stale_or_unlinked_delivery_cannot_prove_readiness(fault):
    _, worker, controller, state, clock, _, binding, rest = registered_fixture()
    try:
        await collect_capability(worker, controller, clock, binding, rest)
        drain_controller(controller)
        clock.advance(1)
        event = native_delivery(clock, target())
        if fault == "malformed":
            event.body = [b"not-json"]
        elif fault == "foreign":
            value = json.loads(event.body[0])
            value["source"] = uid(99_001)
            event.body = [json.dumps(value).encode()]
        await receive(worker, clock, binding, () if fault == "empty" else (event,))
        if fault == "policy":
            state.control_row = {**state.control_row, "revision": state.control_row["revision"] + 1}
        elif fault == "endpoint":
            for key, row in tuple(state.records.items()):
                if row.kind == "connector":
                    changed = json.loads(row.payload)
                    changed["endpoint"]["entity"] = "different-transport"
                    state.records[key] = replace(row, payload=json.dumps(changed))
        elif fault == "receipt":
            proof = worker.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY)
            assert proof is not None
            key = (CONTEXT.tenant_id, CONTEXT.epoch, "stream_intake", key_digest(proof.request_id))
            del state.receipts[key]
            with pytest.raises(MonitoringUnavailable, match="original accepted receipt"):
                worker.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY)
            return
        assert worker.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY) is None
        assert connector(worker).state != "ready"
    finally:
        await rest.close()


async def test_worker_cannot_replace_delivery_receipt_with_heartbeats_or_timestamps():
    fixture, worker, _, _, clock, _, _, rest = registered_fixture()
    # No HTTP client operation is needed by this store-boundary refusal.
    await rest.close()
    prior = connector(fixture)
    work, = worker.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=uid(91_012), kinds=("connector_reconcile",),
        limit=1, per_workspace_limit=1,
    ))
    with pytest.raises(MonitoringConflict, match="original current accepted transport"):
        worker.record_connector(
            version(worker), m.OwnedConnectorManifest.model_validate({
                **prior.model_dump(), "revision": prior.revision + 1, "state": "ready", "gaps": (),
                "identity_verified_at": clock(), "delivery_verified_at": clock(),
            }),
            expected_connector_revision=prior.revision,
            commit=m.CollectionCommit(work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision),
        )


def test_native_readiness_guards_bind_original_stream_receipt_and_keep_roles_separate():
    kernel = build_permission_kernel()
    objects = {value.logical_name: value.ddl for value in kernel.objects}
    for operation in ("worker.observe_connector", "controller.publish_connector"):
        sql = objects[operation]
        assert "'Readiness requires the original current accepted transport receipt'" in sql
        assert "operation='worker.commit_positions'" in sql
        assert "$.first_committed_batch_id" in sql and "$.original_payload_hash" in sql
        assert "$.transport.policy_revision" in sql and "$.transport.collector_identity_id" in sql
        assert "record_kind='accepted_fact'" in sql and "$.row_hash" in sql
    assert "'connector_desired'" in objects["worker_read"]
    assert "r.operation LIKE N'controller.%'" in objects["receipts_controller"]
    assert all(
        event in objects["worker.commit_positions"] for event in (
            "Microsoft.Fabric.JobEvents.ItemJobStatusChanged", "Microsoft.Fabric.JobEvents.ItemJobSucceeded",
        )
    )


async def test_event_capability_refuses_changed_collector_identity():
    _, worker, _, _, clock, _, binding, rest = registered_fixture()
    try:
        observation = await FabricPipelinePollingClient(rest).probe(
            target(), inventory_generation=uid(91_030), checked_at=clock(),
        )
        observation = observation.model_copy(update={"collector_identity_id": uid(91_031)})

        async def renew():
            raise AssertionError("An identity mismatch must fail before reading")

        with pytest.raises(MonitoringConflict, match="another collector"):
            await OwnedEventCapabilityProbe(worker, CONTEXT, rest, binding, clock=clock).verify(observation, renew)
    finally:
        await rest.close()
