from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest
from pydantic import ValidationError
from test_monitoring_sql_receiver_bindings import ReceiverAbiDatabase
from test_monitoring_sql_review9_bindings import (
    Review9Database,
    claim_sibling,
    connector_commit,
    definition,
    publication,
    setup_sql,
)
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.monitoring.events import StreamStartRequest
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.sql_store import AzureSqlMonitoringStore


def scoped_connector(backend, *, database_type=Review9Database):
    h = Harness()
    h.seed()
    h.activate()
    db = database_type(h) if backend == "sql" else None
    web = AzureSqlMonitoringStore(db=db, component="web") if db else InMemoryMonitoringStore(
        state=h.state, clock=h.clock, component="web",
    )
    controller = AzureSqlMonitoringStore(db=db, component="controller") if db else InMemoryMonitoringStore(
        state=h.state, clock=h.clock, component="controller",
    )
    worker = AzureSqlMonitoringStore(db=db, component="worker") if db else InMemoryMonitoringStore(
        state=h.state, clock=h.clock, component="worker",
    )
    if db:
        db.principal = "web"
    web.request_discovery(h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id())
    if db:
        db.principal = "controller"
    work = claim_sibling(h, controller)
    producer = controller.get_reconciliation_request(h.version, work.reconcile_request_id, producer="web")
    frontier = controller.get_validation_frontier(h.version, producer.frontier_key)
    planned = controller.publish_connector(publication(h, work, frontier)).connector
    controller.reconcile_work(work)
    h.clock.advance(1)
    if db:
        db.principal = "worker"
    observed = definition(h.targets[0])
    observed["component_ids"]["sources/owned-source"] = uid(1901)
    worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **planned.model_dump(), "revision": planned.revision + 1,
        "workspace_id": uid(970), "eventstream_id": uid(971), "destination_id": uid(903),
        "endpoint": m.EndpointMetadata(namespace="fixture.example.invalid", entity="fixture", consumer_group="$Default"),
        "state": "provisioning", "observed_definition": observed, "updated_at": h.clock(),
    }), expected_connector_revision=planned.revision,
        commit=connector_commit(h, worker, planned.connector_id, db=db))
    if db:
        db.principal = "controller"
    controller.reconcile_work(claim_sibling(h, controller))
    owned = next(value for value in controller.list_connectors(m.PageQuery(**h.context())).items
                 if value.connector_id == planned.connector_id)
    assert owned.sources[0].source_id == uid(1901) and owned.source_proposals == ()
    return h, db, web, controller, worker, owned


def test_sql_plan_forwards_proposals_and_original_receipt_then_accepts_materialized_result():
    h, db, _, controller, _, owned = scoped_connector("sql")
    plans = [
        m.ConnectorPublicationPlan.model_validate_json(row.payload)
        for row in db.records.values() if row.kind == "connector_publication"
    ]
    binding, = [
        plan for plan in plans
        if plan.connector_id == owned.connector_id and plan.observation_receipt_id is not None
    ]
    assert binding.sources == ()
    proposal, = binding.source_proposals
    assert proposal.source_id is None and proposal.node_name == "owned-source"
    assert proposal.target == owned.sources[0].target
    assert proposal.proposal_id != owned.sources[0].source_id
    assert binding.producer_request_id == binding.observation_receipt_id
    assert binding.desired_definition["component_ids"] == {}
    original = db.receipts[("worker.observe_connector", binding.observation_receipt_id)]["payload"]["result"]
    request_id, = [
        request_id for (operation, request_id), receipt in db.receipts.items()
        if operation == "controller.publish_connector"
        and receipt["payload"]["result"]["observation_receipt_id"] == binding.observation_receipt_id
    ]
    result = controller.get_connector_publication(h.version, request_id)
    assert result.connector.sources == owned.sources != binding.sources
    assert result.connector.source_proposals == ()
    assert result.connector.desired_definition == original["observation"]["observed_definition"]
    assert result.connector.desired_definition != binding.desired_definition
    assert result.connector.policy_revision == binding.policy_revision
    assert result.connector.revision == binding.expected_connector_revision + 1
    assert result.observation_receipt_id == binding.observation_receipt_id


@pytest.mark.parametrize("change", ["source_id", "definition"])
def test_materialized_result_must_match_original_worker_observation_even_if_publication_receipt_matches(change):
    class ChangedBindingResultDatabase(Review9Database):
        def publish_connector(self, args):
            reply = super().publish_connector(args)
            result = reply["result"]
            if result["observation_receipt_id"] is not None:
                if change == "source_id":
                    result["connector"]["sources"][0]["source_id"] = uid(1902)
                else:
                    result["connector"]["desired_definition"]["component_ids"]["sources/owned-source"] = uid(1902)
                self.receipts[("controller.publish_connector", args["request_id"])]["payload"]["result"] = deepcopy(result)
            return reply

    with pytest.raises(MonitoringUnavailable, match="different identity, definition or readiness"):
        scoped_connector("sql", database_type=ChangedBindingResultDatabase)


def removal_request(h, web, controller, owned, db):
    if db:
        db.principal = "web"
    web.request_discovery(h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id())
    if db:
        db.principal = "controller"
    work = claim_sibling(h, controller)
    producer = controller.get_reconciliation_request(h.version, work.reconcile_request_id, producer="web")
    frontier = controller.get_validation_frontier(h.version, producer.frontier_key)
    desired = deepcopy(owned.desired_definition)
    desired["parts"]["eventstream.json"]["sources"] = []
    desired["parts"]["eventstream.json"]["streams"][0]["inputNodes"] = []
    desired["component_ids"].pop("sources/owned-source")
    request = m.ConnectorPublicationRequest(
        request_id=h.next_id(), expected=h.version, work_id=work.work_id, lease=work.lease,
        expected_work_revision=work.revision, expected_frontier_revision=frontier.accepted_revision,
        connector_id=owned.connector_id, ownership_id=owned.ownership_id,
        expected_connector_revision=owned.revision, name=owned.name,
        sources=owned.sources, source_proposals=owned.source_proposals,
        source_removals=(m.SourceRemovalIntent(
            removal_id=h.next_id(), source_id=owned.sources[0].source_id, proposal_id=None,
            detail="Remove this owned source after its exact remote absence is verified.",
        ),), desired_definition=desired, detail="Publish a removal intent without releasing physical ownership.",
    )
    return work, request


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_removal_retains_ownership_until_exact_receipt_verified_absence(backend):
    h, db, web, controller, worker, owned = scoped_connector(backend)
    work, request = removal_request(h, web, controller, owned, db)
    pending = controller.publish_connector(request)
    assert pending.connector.sources == owned.sources
    assert pending.connector.source_removals == pending.pending_removals
    assert len(pending.pending_removals) == 1 and pending.retired_sources == ()
    assert pending.pending_removals[0].source_id == owned.sources[0].source_id
    assert pending.state == "provisioning"
    assert controller.publish_connector(request) == pending
    controller.reconcile_work(work)
    h.clock.advance(1)
    if db:
        db.principal = "worker"
    worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **pending.connector.model_dump(), "revision": pending.connector.revision + 1,
        "observed_definition": pending.connector.desired_definition,
        "state": "provisioning", "updated_at": h.clock(),
    }), expected_connector_revision=pending.connector.revision,
        commit=connector_commit(h, worker, pending.connector.connector_id, db=db))
    if db:
        db.principal = "controller"
    confirmation = claim_sibling(h, controller)
    result = controller.reconcile_work(confirmation)
    assert result.state == "published"
    current = next(value for value in controller.list_connectors(m.PageQuery(**h.context())).items
                   if value.connector_id == owned.connector_id)
    assert current.sources == () and current.source_removals == ()
    assert current.workspace_id == owned.workspace_id and current.endpoint == owned.endpoint
    rows = db.records.values() if db else h.state.records.values()
    retired = [row for row in rows if row.kind == "connector_source_retirement"]
    assert len(retired) == 1
    tombstone = m.ConnectorSourceRetirement.model_validate_json(retired[0].payload)
    assert tombstone.original_binding == owned.sources[0]
    assert tombstone.original_removal == pending.pending_removals[0]
    assert tombstone.observation_receipt_id == confirmation.reconcile_request_id
    assert controller.publish_connector(request) == pending


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_removal_cannot_drop_ownership_or_cancel_pending_intent(backend):
    h, db, web, controller, _, owned = scoped_connector(backend)
    _, request = removal_request(h, web, controller, owned, db)
    with pytest.raises(ValidationError):
        m.ConnectorPublicationRequest.model_validate({**request.model_dump(), "sources": ()})
    pending = controller.publish_connector(request)
    omitted = m.ConnectorPublicationRequest.model_validate({
        **request.model_dump(), "request_id": h.next_id(),
        "expected_connector_revision": pending.connector.revision,
        "source_removals": (), "desired_definition": owned.desired_definition,
    })
    with pytest.raises(MonitoringConflict, match="omitted|cancelled|rewritten"):
        controller.publish_connector(omitted)


@pytest.mark.parametrize("fault", ["receipt", "commit_after"])
def test_sql_removal_intent_receipt_failure_or_lost_ack_does_not_release_ownership(fault):
    h, db, web, controller, _, owned = scoped_connector("sql")
    _, request = removal_request(h, web, controller, owned, db)
    before = deepcopy((db.records, db.receipts))
    if fault == "receipt":
        db.fail_connector_receipt = True
        with pytest.raises(MonitoringConflict):
            controller.publish_connector(request)
        assert (db.records, db.receipts) == before
    else:
        db.fail_commit = "after"
        with pytest.raises(MonitoringCommitUncertain):
            controller.publish_connector(request)
        original = controller.get_connector_publication(h.version, request.request_id)
        assert original.connector.sources == owned.sources
        assert original.pending_removals and not original.retired_sources
        assert controller.publish_connector(request) == original


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_incomplete_remote_absence_snapshot_keeps_all_pending_ownership(backend):
    h, db, web, controller, worker, owned = scoped_connector(backend)
    work, request = removal_request(h, web, controller, owned, db)
    pending = controller.publish_connector(request)
    controller.reconcile_work(work)
    h.clock.advance(1)
    bad = deepcopy(pending.connector.desired_definition)
    bad["component_ids"] = {}
    if db:
        db.principal = "worker"
    worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **pending.connector.model_dump(), "revision": pending.connector.revision + 1,
        "observed_definition": bad, "state": "provisioning", "updated_at": h.clock(),
    }), expected_connector_revision=pending.connector.revision,
        commit=connector_commit(h, worker, pending.connector.connector_id, db=db))
    if db:
        db.principal = "controller"
    confirmation = claim_sibling(h, controller)
    with pytest.raises(MonitoringConflict, match="component|canonical|map"):
        controller.reconcile_work(confirmation)
    current = next(value for value in controller.list_connectors(m.PageQuery(**h.context())).items
                   if value.connector_id == owned.connector_id)
    assert current.sources == owned.sources and current.source_removals == pending.pending_removals
    rows = db.records.values() if db else h.state.records.values()
    assert not any(row.kind == "connector_source_retirement" for row in rows)


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_pending_removal_denies_intake_even_after_a_degraded_worker_observation(backend):
    h, db, web, controller, worker, owned = scoped_connector(backend, database_type=ReceiverAbiDatabase)
    gap = m.CoverageGap(code="remote_update_failed", detail="The owned remote source is still present.")
    if db:
        db.principal = "worker"
    owned = worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **owned.model_dump(), "revision": owned.revision + 1,
        "state": "degraded", "gaps": (gap,), "updated_at": h.clock(),
    }), expected_connector_revision=owned.revision,
        commit=connector_commit(h, worker, owned.connector_id, db=db))
    if db:
        db.principal = "controller"
    controller.reconcile_work(claim_sibling(h, controller))
    partition = m.PartitionIdentity(
        **h.context(), connector_id=owned.connector_id, consumer_group="$Default", partition_id="0",
    )
    if db:
        db.principal = "worker"
    lease = worker.claim_partition(m.PartitionClaimRequest(partition=partition, owner_id=h.owner))
    worker.ensure_stream_start(StreamStartRequest(
        partition=partition, lease=lease, first_available_sequence_number=100, observed_at=h.clock(),
    ))
    work, removal = removal_request(h, web, controller, owned, db)
    pending = controller.publish_connector(removal)
    controller.reconcile_work(work)
    signal = m.SignalReceipt(
        delivery=m.TransportDeliveryIdentity(
            **h.context(), connector_id=owned.connector_id,
            event_source=owned.sources[0].event_source, event_id="fixture-pending-removal",
        ), partition=partition,
        position=m.StreamPosition(offset="1000", sequence_number=100, enqueued_at=h.clock()),
        event_type=owned.sources[0].event_types[1], received_at=h.clock(), status="accepted",
        observation=m.SourceRunObservation.model_validate({
            **h.observation().model_dump(), "origin": "event", "authority": "transport",
        }),
    )
    batch = m.StreamReceiptBatch(request_id=h.next_id(), partition=partition, lease=lease, receipts=(signal,))
    if db:
        db.principal = "worker"
    with pytest.raises(MonitoringConflict):
        worker.record_stream_receipts(batch)
    assert worker.get_stream_acceptance(h.version, batch.request_id) is None
    assert worker.get_stream_checkpoint(partition) is None
    degraded = worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **pending.connector.model_dump(), "revision": pending.connector.revision + 1,
        "state": "degraded", "gaps": (gap,), "updated_at": h.clock(),
    }), expected_connector_revision=pending.connector.revision,
        commit=connector_commit(h, worker, pending.connector.connector_id, db=db))
    assert degraded.sources == owned.sources and degraded.source_removals == pending.pending_removals
    if db:
        raw_positions = json.dumps([{
            "receipt_kind": "identified", "receipt_key": signal.delivery.key, "receipt": signal.model_dump(mode="json"),
        }])
        request_id = h.next_id()
        with pytest.raises(MonitoringConflict, match="51072"):
            with worker._sql.transaction(write=True, operation="worker.commit_positions", request_id=request_id):
                worker._sql.rpc("worker.commit_positions", {
                    **partition.model_dump(), "request_id": request_id,
                    "fingerprint": hashlib.sha256(raw_positions.encode()).hexdigest(),
                    "expected_revision": h.version.revision, "owner_id": lease.owner_id, "fence": lease.fence,
                    "positions_json": raw_positions,
                })
    receipt = worker.record_stream_receipts(batch)
    rows = db.records if db else h.state.records
    saved = m.SignalReceipt.model_validate_json(next(
        row.payload for row in rows.values() if row.kind == "signal" and row.key == signal.delivery.key
    ))
    assert saved.status == "quarantined" and saved.quarantine.reason == "out_of_scope"
    assert saved.observation == signal.observation
    assert signal.status == "accepted" and signal.quarantine is None
    assert receipt.publication_status == "pending_validation" and len(receipt.work_ids) == 1
    queued = m.MonitoringWork.model_validate_json(next(
        row.payload for row in rows.values() if row.kind == "work" and row.key == receipt.work_ids[0]
    ))
    assert queued.kind == "reconcile_state" and queued.target is None
    assert not any(row.kind == "connector_source_retirement" for row in rows.values())
    if db:
        db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "maintenance": True})
    else:
        h.state.control_row = {**h.state.control_row, "maintenance": True}
    original = deepcopy(rows)
    assert worker.record_stream_receipts(batch) == receipt
    assert rows == original


@pytest.mark.parametrize("change", [
    {"source_id": None, "proposal_id": None},
    {"source_id": "owned", "proposal_id": uid(991)},
    {"source_id": "x" * 257},
    {"detail": "x" * 2001},
])
def test_removal_intent_has_one_bounded_owned_selector(change):
    with pytest.raises(ValidationError):
        m.SourceRemovalIntent.model_validate({
            "removal_id": uid(992), "source_id": "owned", "proposal_id": None,
            "detail": "Explicit removal.", **change,
        })


@pytest.mark.parametrize("location", ["component_map", "node_id"])
def test_unresolved_proposal_cannot_introduce_physical_identity_without_receipt(location):
    h, _, _, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    desired = deepcopy(request.desired_definition)
    if location == "component_map":
        desired["component_ids"]["sources/owned-source"] = uid(901)
    else:
        desired["parts"]["eventstream.json"]["sources"][0]["id"] = uid(901)
    with pytest.raises(ValidationError, match="unresolved proposal"):
        m.ConnectorPublicationRequest.model_validate({
            **request.model_dump(), "expected_connector_revision": 1, "desired_definition": desired,
        })
