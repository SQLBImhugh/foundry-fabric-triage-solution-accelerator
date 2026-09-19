from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from monitoring_supersession_protocol import SupersessionProtocolDatabase
from test_monitoring_readiness_liveness import accept_delivery
from test_monitoring_sql_review9_bindings import EVENTS, connector_commit, definition
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringConflict, MonitoringUnavailable
from triage.monitoring.controller import reconcile_monitoring_work
from triage.monitoring.events import OwnershipChange, ReceiverHeartbeat, StreamStartRequest
from triage.monitoring.memory import InMemoryMonitoringStore, key_digest, stable_id
from triage.monitoring.sql_store import AzureSqlMonitoringStore


class AnchorCase:
    def __init__(self, backend):
        self.h = Harness()
        h = self.h
        h.seed(2)
        h.activate()
        self.sources = tuple(m.ConnectorSource(
            source_id=uid(1901 + index), target=target, event_types=EVENTS, event_source=f"fixture:source:{index}",
        ) for index, target in enumerate(h.targets))
        first = definition(h.targets[0])
        second = definition(h.targets[1], source_name="second-source")
        first["parts"]["eventstream.json"]["sources"].extend(second["parts"]["eventstream.json"]["sources"])
        first["parts"]["eventstream.json"]["streams"][0]["inputNodes"].append({"name": "second-source"})
        first["component_ids"].update({"sources/owned-source": uid(1901), "sources/second-source": uid(1902)})
        self.physical = first
        self.connector_id = uid(910)
        h.store.record_connector(h.version, m.OwnedConnectorManifest(
            **h.context(), connector_id=self.connector_id, ownership_id=uid(911), revision=1,
            policy_revision=h.version.revision, workspace_id=uid(970), eventstream_id=uid(971),
            destination_id=uid(903), name="Owned two-source fixture", sources=self.sources,
            desired_definition=first, observed_definition=first, state="planned", updated_at=h.clock(),
            endpoint=m.EndpointMetadata(namespace="fixture.example.invalid", entity="fixture", consumer_group="$Default"),
        ), expected_connector_revision=0)
        self.db = SupersessionProtocolDatabase(h) if backend == "sql" else None
        stores = {
            component: AzureSqlMonitoringStore(db=self.db, component=component) if self.db else
            InMemoryMonitoringStore(state=h.state, clock=h.clock, component=component)
            for component in ("web", "controller", "worker")
        }
        self.web, self.controller, self.worker = (stores[name] for name in ("web", "controller", "worker"))
        work, frontier = self.web_work()
        current = self.connector()
        self.controller.publish_connector(self.publication(work, frontier, current, definition=first))
        reconcile_monitoring_work(self.controller, work)

    def principal(self, component):
        if self.db:
            self.db.principal = component

    def connector(self):
        return next(value for value in self.controller.list_connectors(m.PageQuery(**self.h.context())).items
                    if value.connector_id == self.connector_id)

    def web_work(self):
        h = self.h
        self.principal("web")
        queued = self.web.request_discovery(
            h.version, m.ScopeSelector(tenant_id=h.version.tenant_id, kind="tenant"), request_id=h.next_id(),
        )
        self.principal("controller")
        work = next(value for value in self.controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(920), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
        )) if value.work_id == queued.work_id)
        producer = self.controller.get_reconciliation_request(h.version, work.reconcile_request_id, producer="web")
        return work, self.controller.get_validation_frontier(h.version, producer.frontier_key)

    def publication(self, work, frontier, current, *, definition, removals=(), supersessions=(), observation=None):
        return m.ConnectorPublicationRequest(
            request_id=self.h.next_id(), expected=self.h.version, work_id=work.work_id, lease=work.lease,
            expected_work_revision=work.revision, expected_frontier_revision=frontier.accepted_revision,
            connector_id=current.connector_id, ownership_id=current.ownership_id,
            expected_connector_revision=current.revision, name=current.name, sources=current.sources,
            source_proposals=current.source_proposals, source_removals=removals,
            source_removal_supersessions=supersessions, desired_definition=definition,
            observation_receipt_id=observation, detail="Bounded original-receipt anchor fixture.",
        )

    def restore(self, source_index, *, heartbeat_timing=None):
        h = self.h
        h.clock.advance(1)
        work, frontier = self.web_work()
        current = self.connector()
        source = self.sources[source_index]
        name = "owned-source" if source_index == 0 else "second-source"
        without = deepcopy(current.desired_definition)
        graph = without["parts"]["eventstream.json"]
        graph["sources"] = [node for node in graph["sources"] if node["name"] != name]
        graph["streams"][0]["inputNodes"] = [node for node in graph["streams"][0]["inputNodes"] if node["name"] != name]
        without["component_ids"].pop(f"sources/{name}")
        intent = m.SourceRemovalIntent(
            removal_id=h.next_id(), source_id=source.source_id, proposal_id=None, detail="Original fixture removal.",
        )
        self.controller.publish_connector(self.publication(
            work, frontier, current, definition=without, removals=(intent,),
        ))
        reconcile_monitoring_work(self.controller, work)
        h.clock.advance(1)
        self.principal("worker")
        collection = None
        for _ in range(20):
            candidates = self.worker.claim_work(m.WorkClaimRequest(
                **h.context(), owner_id=h.owner, kinds=("connector_reconcile",), limit=1, per_workspace_limit=1,
            ))
            assert candidates
            if candidates[0].connector_id == self.connector_id:
                collection = candidates[0]
                break
        assert collection is not None
        current = self.connector()
        if heartbeat_timing in {"before_inspection", "both"}:
            self.worker.record_receiver_heartbeat(ReceiverHeartbeat(
                **h.context(), worker_id=h.owner, connector_id=self.connector_id,
                observed_at=h.clock(), state="running", transport_connected=True,
            ))
            assert self.connector() == current
        observation = m.OwnedConnectorManifest.model_validate({
            **current.model_dump(), "revision": current.revision + 1, "state": "degraded",
            "observed_definition": self.physical, "updated_at": h.clock(),
            "identity_verified_at": None, "delivery_verified_at": None, "delivery_proof": None,
            "gaps": (m.CoverageGap(code="pending_source_removal_presence_observed", detail="Original fixture presence."),),
        })
        self.worker.record_connector(
            h.version, observation, expected_connector_revision=current.revision,
            commit=m.CollectionCommit(
                work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
            ),
            inspection=m.ConnectorPresenceInspection(
                read_only=True, observed_at=h.clock(), definition_hash=m.connector_definition_hash(self.physical),
                component_states={value: "Running" for value in self.physical["component_ids"].values()},
            ),
        )
        if heartbeat_timing in {"after_inspection", "both"}:
            h.clock.advance(1)
            before_heartbeat = self.connector()
            self.worker.record_receiver_heartbeat(ReceiverHeartbeat(
                **h.context(), worker_id=h.owner, connector_id=self.connector_id,
                observed_at=h.clock(), state="running", transport_connected=True,
            ))
            assert self.connector() == before_heartbeat
        self.worker.complete_collection_work(
            h.version, work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
        )
        h.clock.advance(1)
        self.principal("controller")
        original_id = stable_id(h.version, f"connector:{current.connector_id}:{current.revision}")
        original = self.controller.get_connector_observation(h.version, original_id)
        reconciler = next(value for value in self.controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(920), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
        )) if value.work_id == original.reconcile_work_id)
        frontier = self.controller.get_validation_frontier(h.version, original.frontier_key)
        request = self.publication(
            reconciler, frontier, self.connector(), definition=self.physical,
            supersessions=(m.SourceRemovalSupersession(removal_id=intent.removal_id, source_id=source.source_id),),
            observation=original_id,
        )
        result = self.controller.publish_connector(request)
        reconcile_monitoring_work(self.controller, reconciler)
        return request, result

    def observe_degraded(self):
        h = self.h
        h.clock.advance(1)
        self.principal("worker")
        work = None
        for _ in range(20):
            candidates = self.worker.claim_work(m.WorkClaimRequest(
                **h.context(), owner_id=h.owner, kinds=("connector_reconcile",), limit=1, per_workspace_limit=1,
            ))
            assert candidates
            if candidates[0].connector_id == self.connector_id:
                work = candidates[0]
                break
        assert work is not None
        current = self.connector()
        observed = m.OwnedConnectorManifest.model_validate({
            **current.model_dump(), "revision": current.revision + 1, "state": "degraded",
            "observed_definition": current.desired_definition, "updated_at": h.clock(),
            "gaps": (m.CoverageGap(code="awaiting_delivery", detail="Awaiting original current delivery."),),
        })
        self.worker.record_connector(
            h.version, observed, expected_connector_revision=current.revision,
            commit=m.CollectionCommit(work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision),
        )
        self.worker.complete_collection_work(
            h.version, work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
        )
        h.clock.advance(1)
        self.principal("controller")
        return self.connector()

    def deliver(self, source_index, partition_id):
        self.principal("worker")
        current = self.connector()
        sources = current.sources[source_index:] + current.sources[:source_index]
        selected = m.OwnedConnectorManifest.model_validate({**current.model_dump(), "sources": sources})
        return accept_delivery(self.h, self.worker, selected, partition_id, enqueued_at=self.h.clock())


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_later_distinct_restoration_keeps_first_anchor(backend):
    case = AnchorCase(backend)
    first, first_result = case.restore(0)
    second, second_result = case.restore(1)
    desired = case.controller.get_connector_desired(case.h.version, case.connector_id)
    assert desired.supersession_request_id == first.request_id
    assert first.request_id != second.request_id
    assert first_result.superseded_source_removals[0].source_id == case.sources[0].source_id
    assert second_result.superseded_source_removals[0].source_id == case.sources[1].source_id
    assert case.controller.get_connector_publication(case.h.version, first.request_id) == first_result


def test_sql_caller_refuses_a_later_result_that_replaces_the_original_anchor(monkeypatch):
    case = AnchorCase("sql")
    first, first_result = case.restore(0)
    publish = case.db.publish_connector

    def reset_anchor(args):
        result = publish(args)
        if result["result"].get("superseded_source_removals"):
            key = ("connector_desired", case.connector_id)
            row = case.db.records[key]
            payload = json.loads(row.payload)
            payload["supersession_request_id"] = args["request_id"]
            case.db.records[key] = replace(row, payload=json.dumps(payload))
        return result

    monkeypatch.setattr(case.db, "publish_connector", reset_anchor)
    with pytest.raises(MonitoringUnavailable, match="Supersession result"):
        case.restore(1)
    assert case.controller.get_connector_publication(case.h.version, first.request_id) == first_result
    assert case.controller.get_connector_desired(case.h.version, case.connector_id).supersession_request_id == first.request_id
    assert case.connector().source_removals


def test_safe_queued_disposition_names_the_original_publication():
    case = AnchorCase("memory")
    request, _ = case.restore(0)
    dispositions = [
        m.MonitoringWork.model_validate_json(row.payload)
        for row in case.h.state.records.values()
        if row.kind == "work" and row.work_kind == "connector_reconcile" and row.status == "dispositioned"
    ]
    assert dispositions
    assert all(request.request_id in work.disposition for work in dispositions)


@pytest.mark.parametrize("timing", ["before_inspection", "after_inspection", "both"])
def test_heartbeat_during_removal_does_not_change_authority_or_original_observation(timing):
    case = AnchorCase("memory")
    request, result = case.restore(0, heartbeat_timing=timing)
    assert result.state == "provisioning" and result.superseded_source_removals
    assert case.controller.get_connector_desired(case.h.version, case.connector_id).supersession_request_id == request.request_id
    assert case.worker.list_receiver_heartbeats(case.h.version, connector_id=case.connector_id)


@pytest.mark.parametrize("source_index", [0, 1])
def test_every_still_owned_restoration_needs_fresh_capability_after_later_publication(source_index):
    case = AnchorCase("memory")
    first, _ = case.restore(0)
    case.restore(1)
    case.observe_degraded()
    assert case.controller.get_connector_desired(case.h.version, case.connector_id).supersession_request_id == first.request_id
    with pytest.raises(MonitoringConflict, match="new capability"):
        case.deliver(source_index, f"stale-{source_index}")
    case.h.capability(case.h.targets[source_index], action_status="unknown")
    signal = case.deliver(source_index, f"fresh-{source_index}")
    assert case.h.state.records[(
        case.h.version.tenant_id, case.h.version.epoch, "signal", key_digest(signal.delivery.key),
    )].status == "accepted"
    assert not case.controller.resolve_target(case.h.targets[source_index]).action.enabled


@pytest.mark.parametrize("damage", [
    "missing_anchor", "malformed_anchor", "missing_middle", "duplicate_revision", "wrong_owner", "wrong_epoch", "malformed",
])
def test_anchored_intake_refuses_missing_ambiguous_or_rebound_original_history(damage):
    case = AnchorCase("memory")
    first, _ = case.restore(0)
    second, _ = case.restore(1)
    case.observe_degraded()
    case.h.capability(case.h.targets[0], action_status="unknown")
    receipts = case.h.state.receipts
    if damage in {"missing_anchor", "malformed_anchor"}:
        key = next(key for key, row in receipts.items()
                   if row.operation == "connector_publication" and row.request_id == first.request_id)
        if damage == "missing_anchor":
            receipts.pop(key)
        else:
            receipts[key] = replace(receipts[key], payload="[]")
    elif damage == "missing_middle":
        key = next(key for key, row in receipts.items()
                   if row.operation == "connector_publication" and row.request_id == second.request_id)
        receipts.pop(key)
    else:
        key = next(key for key, row in receipts.items()
                   if row.operation == "connector_publication" and row.request_id == second.request_id)
        row = receipts[key]
        if damage == "duplicate_revision":
            identity = case.h.next_id()
            receipts[(case.h.version.tenant_id, case.h.version.epoch, row.operation, key_digest(identity))] = replace(
                row, request_id=identity,
            )
        elif damage == "malformed":
            receipts[key] = replace(row, payload="[]")
        else:
            payload = json.loads(row.payload)
            payload["connector"]["ownership_id" if damage == "wrong_owner" else "epoch"] = uid(9999)
            receipts[key] = replace(row, payload=json.dumps(payload))
    before = deepcopy(receipts)
    signals = {key: row for key, row in case.h.state.records.items() if row.kind == "signal"}
    with pytest.raises(MonitoringUnavailable):
        case.deliver(0, "damaged-history")
    assert {key: row for key, row in case.h.state.records.items() if row.kind == "signal"} == signals
    assert all(receipts[key] == value for key, value in before.items())


def test_receiver_health_and_deliveries_do_not_create_unreceipted_connector_revisions():
    case = AnchorCase("memory")
    case.restore(0)
    case.restore(1)
    observed = case.observe_degraded()
    for target in case.h.targets:
        case.h.capability(target, action_status="unknown")
    case.deliver(0, "first")
    case.worker.record_receiver_heartbeat(ReceiverHeartbeat(
        **case.h.context(), worker_id=case.h.owner, connector_id=case.connector_id,
        observed_at=case.h.clock(), state="running", transport_connected=True,
    ))
    case.deliver(1, "second")
    assert case.connector().revision == observed.revision
    assert case.worker.list_receiver_heartbeats(case.h.version, connector_id=case.connector_id)


def test_intake_audit_does_not_follow_a_different_current_physical_source_id():
    case = AnchorCase("memory")
    case.restore(0)
    case.observe_degraded()
    case.h.capability(case.h.targets[0], action_status="unknown")
    signal = case.deliver(0, "original-binding")
    current = case.connector()
    case.h.capability(case.h.targets[0], event_status="unknown", action_status="unknown")
    with pytest.raises(MonitoringConflict, match="new capability"):
        case.controller._require_restored_source_intake(signal, current, case.h.control)
    replacement = current.sources[0].model_copy(update={"source_id": uid(2901)})
    physical = deepcopy(current.desired_definition)
    physical["component_ids"]["sources/owned-source"] = replacement.source_id
    different = m.OwnedConnectorManifest.model_validate({
        **current.model_dump(), "sources": (replacement, *current.sources[1:]),
        "desired_definition": physical, "observed_definition": physical,
    })
    replacement_signal = signal.model_copy(update={"transport": signal.transport.model_copy(update={
        "source_id": replacement.source_id, "definition_hash": m.connector_definition_hash(physical),
    })})
    # Ownership adoption is checked before this private predicate; an old audit
    # must not impose its restored-source fence on another admitted physical ID.
    case.controller._require_restored_source_intake(replacement_signal, different, case.h.control)


def test_first_anchor_intake_refuses_a_future_capability_even_after_publication():
    case = AnchorCase("memory")
    case.restore(0)
    case.observe_degraded()
    case.h.capability(case.h.targets[0], action_status="unknown")
    key, row = next((key, row) for key, row in case.h.state.records.items()
                    if row.kind == "target_capability" and row.key == case.h.targets[0].key)
    capability = m.CapabilityObservation.model_validate_json(row.payload)
    case.h.state.records[key] = replace(row, payload=capability.model_copy(update={
        "checked_at": case.h.clock() + timedelta(seconds=1),
    }).model_dump_json())
    with pytest.raises(MonitoringConflict, match="new capability"):
        case.deliver(0, "future-capability")


def _accept_reported_delivery(case, reported_at):
    h, worker, current = case.h, case.worker, case.connector()
    partition = m.PartitionIdentity(
        **h.context(), connector_id=current.connector_id, consumer_group=current.endpoint.consumer_group,
        partition_id="reported-time",
    )
    ownership = worker.change_partition_ownership(OwnershipChange(
        partition=partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=partition, owner_id=h.owner, initial_sequence_number=100),
    ))
    worker.ensure_stream_start(StreamStartRequest(
        partition=partition, lease=ownership.lease, first_available_sequence_number=100, observed_at=h.clock(),
    ))
    request_id = h.next_id()
    source = current.sources[0]
    signal = m.SignalReceipt(
        delivery=m.TransportDeliveryIdentity(
            **h.context(), connector_id=current.connector_id, event_source=source.event_source, event_id=request_id,
        ),
        partition=partition, position=m.StreamPosition(offset="1000", sequence_number=100, enqueued_at=reported_at),
        received_at=reported_at, event_type=source.event_types[0], status="accepted",
        observation=h.observation(source.target, origin="event", authority="transport", status="running"),
        transport=m.EventTransportEvidence(
            request_id=request_id, ownership_id=current.ownership_id, policy_revision=current.policy_revision,
            workspace_id=current.workspace_id, eventstream_id=current.eventstream_id,
            destination_id=current.destination_id, endpoint=current.endpoint,
            definition_hash=m.connector_definition_hash(current.desired_definition), source_id=source.source_id,
            collector_identity_id=uid(4), identity_verified_at=reported_at,
        ),
    )
    worker.record_stream_receipts(m.StreamReceiptBatch(
        request_id=request_id, partition=partition, lease=ownership.lease, receipts=(signal,),
    ))
    return signal


def test_original_prepublication_batch_cannot_become_ready_from_reported_future_times():
    case = AnchorCase("memory")
    case.observe_degraded()
    reported_at = case.h.clock() + timedelta(seconds=30)
    signal = _accept_reported_delivery(case, reported_at)
    original = case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id)
    case.restore(0)
    case.observe_degraded()
    desired = case.controller.get_connector_desired(case.h.version, case.connector_id)
    assert original.recorded_at < desired.published_at < reported_at
    case.h.clock.advance(int((reported_at - case.h.clock()).total_seconds()) + 1)
    case.h.capability(case.h.targets[0], action_status="unknown")
    assert case.worker.get_connector_delivery(case.h.version, case.connector_id, uid(4)) is None
    assert case.controller.get_connector_delivery(case.h.version, case.connector_id, uid(4)) is None
    current = case.connector()
    forged = m.ConnectorDeliveryProof(
        request_id=signal.transport.request_id, receipt_key=signal.delivery.key,
        collector_identity_id=uid(4), received_at=signal.received_at,
        identity_verified_at=signal.transport.identity_verified_at,
    )
    commit = connector_commit(case.h, case.worker, case.connector_id)
    before = deepcopy((case.h.state.records, case.h.state.receipts))
    with pytest.raises(MonitoringConflict, match="original current accepted transport"):
        case.worker.record_connector(
            case.h.version,
            m.OwnedConnectorManifest.model_validate({
                **current.model_dump(), "revision": current.revision + 1, "state": "ready", "gaps": (),
                "identity_verified_at": forged.identity_verified_at,
                "delivery_verified_at": forged.received_at, "delivery_proof": forged,
            }),
            expected_connector_revision=current.revision, commit=commit,
        )
    assert (case.h.state.records, case.h.state.receipts) == before
    assert case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id) == original


@pytest.mark.parametrize("boundary", ["before_publication", "future"])
def test_recovered_readiness_uses_original_receipt_recorded_at_not_result_or_signal_times(boundary):
    case = AnchorCase("memory")
    case.restore(0)
    case.observe_degraded()
    case.h.capability(case.h.targets[0], action_status="unknown")
    signal = case.deliver(0, "native-receipt-time")
    original = case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id)
    assert case.worker.get_connector_delivery(case.h.version, case.connector_id, uid(4)) is not None
    key, receipt = next((key, receipt) for key, receipt in case.h.state.receipts.items()
                        if receipt.operation == "stream_intake" and receipt.request_id == signal.transport.request_id)
    desired = case.controller.get_connector_desired(case.h.version, case.connector_id)
    recorded_at = (
        desired.published_at - timedelta(microseconds=1) if boundary == "before_publication"
        else case.h.clock() + timedelta(microseconds=1)
    )
    case.h.state.receipts[key] = replace(receipt, recorded_at=recorded_at)
    before = deepcopy(case.h.state.receipts)
    assert case.worker.get_connector_delivery(case.h.version, case.connector_id, uid(4)) is None
    assert case.h.state.receipts == before
    assert case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id) == original


def test_recovered_readiness_refuses_future_capability_without_changing_original_acceptance():
    case = AnchorCase("memory")
    case.restore(0)
    case.observe_degraded()
    case.h.capability(case.h.targets[0], action_status="unknown")
    signal = case.deliver(0, "future-readiness-capability")
    original = case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id)
    key, row = next((key, row) for key, row in case.h.state.records.items()
                    if row.kind == "target_capability" and row.key == case.h.targets[0].key)
    cap = m.CapabilityObservation.model_validate_json(row.payload)
    case.h.state.records[key] = replace(row, payload=cap.model_copy(update={
        "checked_at": case.h.clock() + timedelta(microseconds=1),
    }).model_dump_json())
    assert case.worker.get_connector_delivery(case.h.version, case.connector_id, uid(4)) is None
    assert case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id) == original


def test_original_ready_publication_replay_remains_historical_after_new_desired_publication(monkeypatch):
    case = AnchorCase("memory")
    case.restore(0)
    case.observe_degraded()
    case.h.capability(case.h.targets[0], action_status="unknown")
    signal = case.deliver(0, "original-ready")
    proof = case.worker.get_connector_delivery(case.h.version, case.connector_id, uid(4))
    current = case.connector()
    commit = connector_commit(case.h, case.worker, case.connector_id)
    case.worker.record_connector(
        case.h.version, m.OwnedConnectorManifest.model_validate({
            **current.model_dump(), "revision": current.revision + 1, "state": "ready", "gaps": (),
            "identity_verified_at": proof.identity_verified_at, "delivery_verified_at": proof.received_at,
            "delivery_proof": proof,
        }),
        expected_connector_revision=current.revision, commit=commit,
    )
    observation_id = stable_id(case.h.version, f"connector:{case.connector_id}:{current.revision}")
    original = case.controller.get_connector_observation(case.h.version, observation_id)
    work = next(item for item in case.controller.claim_work(m.WorkClaimRequest(
        **case.h.context(), owner_id=uid(920), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
    )) if item.work_id == original.reconcile_work_id)
    captured = []
    publish = case.controller._connector_publication

    def retain_request(request):
        if request.readiness_receipt_id == observation_id:
            captured.append(request)
        return publish(request)

    monkeypatch.setattr(case.controller, "_connector_publication", retain_request)
    reconcile_monitoring_work(case.controller, work)
    ready_request, = captured
    ready_result = case.controller.get_connector_publication(case.h.version, ready_request.request_id)
    assert ready_result.state == "ready" and ready_result.connector.delivery_proof == proof
    accepted = case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id)
    case.h.clock.advance(1)
    changed_work, frontier = case.web_work()
    current = case.connector()
    changed = case.publication(changed_work, frontier, current, definition=current.desired_definition)
    case.controller.publish_connector(changed.model_copy(update={"name": "Later desired publication"}))
    assert case.connector().state == "provisioning"
    assert case.worker.get_connector_delivery(case.h.version, case.connector_id, uid(4)) is None
    assert case.controller.publish_connector(ready_request) == ready_result
    assert case.controller.get_connector_publication(case.h.version, ready_request.request_id) == ready_result
    assert case.worker.get_stream_acceptance(case.h.version, signal.transport.request_id) == accepted
    assert case.connector().state == "provisioning"


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_ordinary_later_publication_retains_original_recovery_anchor(backend):
    case = AnchorCase(backend)
    first, _ = case.restore(0)
    work, frontier = case.web_work()
    current = case.connector()
    request = case.publication(work, frontier, current, definition=current.desired_definition)
    request = request.model_copy(update={"name": "Renamed owned fixture"})
    case.controller.publish_connector(request)
    assert case.controller.get_connector_desired(case.h.version, case.connector_id).supersession_request_id == first.request_id
