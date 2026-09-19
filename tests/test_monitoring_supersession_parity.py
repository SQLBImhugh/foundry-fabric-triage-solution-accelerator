from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from monitoring_supersession_protocol import SupersessionProtocolDatabase
from test_monitoring_connector_retirement_store import scoped_connector
from test_monitoring_connector_supersession_store import recovery_case
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringConflict, MonitoringUnavailable
from triage.monitoring.events import ReceiverHeartbeat
from triage.monitoring.memory import key_digest


def fresh_capability(h, db, target):
    if db is None:
        return h.capability(target, event_status="verified", action_status="unknown")
    original = db.model("target_capability", target.key, m.CapabilityObservation)
    capability = original.model_copy(update={
        "capability_id": h.next_id(), "checked_at": h.clock(),
        "event_status": "verified", "action_status": "unknown",
    })
    db.native_put("target_capability", target.key, capability.model_dump(mode="json"), target_key=target.key)
    current = db.model("target", target.key, m.MonitoringTarget)
    db.native_put("target", target.key, current.model_copy(update={
        "capability_id": capability.capability_id, "action": m.ActionPolicy(),
    }).model_dump(mode="json"))
    return capability


def test_heartbeat_does_not_change_an_original_presence_revision_or_payload():
    h, controller, worker, owned, _, _, _, request = recovery_case()
    before = controller.get_connector_observation(h.version, request.observation_receipt_id)
    current = next(item for item in controller.list_connectors(m.PageQuery(**h.context())).items
                   if item.connector_id == owned.connector_id)
    h.clock.advance(1)
    heartbeat = ReceiverHeartbeat(
        **h.context(), worker_id=uid(2220), connector_id=owned.connector_id,
        observed_at=h.clock(), state="running", transport_connected=False,
    )
    worker.record_receiver_heartbeat(heartbeat)
    after = next(item for item in controller.list_connectors(m.PageQuery(**h.context())).items
                 if item.connector_id == owned.connector_id)
    assert after == current
    assert controller.get_connector_observation(h.version, request.observation_receipt_id) == before
    assert worker.list_receiver_heartbeats(h.version, connector_id=owned.connector_id) == (heartbeat,)
    assert controller.publish_connector(request).superseded_source_removals


def two_restorations(backend):
    setup = scoped_connector(backend, database_type=SupersessionProtocolDatabase, source_count=2)
    h, db, web, controller, worker, _ = setup
    *_, first_request = recovery_case(backend, existing=setup)
    first = controller.publish_connector(first_request)
    controller.reconcile_work(controller.get_work(h.version, first_request.work_id))
    before = deepcopy(db.receipts if db else h.state.receipts)
    h.clock.advance(1)
    fresh_capability(h, db, first.connector.sources[0].target)
    h.clock.advance(1)
    *_, second_request = recovery_case(backend, existing=(
        h, db, web, controller, worker, first.connector,
    ), source_index=1)
    second = controller.publish_connector(second_request)
    return h, db, controller, worker, first_request, first, second_request, second, before


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_a_later_supersession_keeps_the_first_original_anchor_and_receipt(backend):
    h, db, controller, _, first_request, first, second_request, second, before = two_restorations(backend)
    desired = controller.get_connector_desired(h.version, first.connector_id)
    assert desired.supersession_request_id == first_request.request_id
    assert second.superseded_source_removals != first.superseded_source_removals
    assert controller.get_connector_publication(h.version, first_request.request_id) == first
    assert controller.publish_connector(second_request) == second
    receipts = db.receipts if db else h.state.receipts
    assert all(receipts[key] == value for key, value in before.items())


def receiving_connector(h, worker, restored):
    h.clock.advance(1)
    work = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=h.owner, kinds=("connector_reconcile",), limit=1, per_workspace_limit=1,
    ))[0]
    return worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **restored.model_dump(), "revision": restored.revision + 1, "state": "degraded",
        "updated_at": h.clock(), "observed_definition": restored.desired_definition,
        "gaps": (m.CoverageGap(code="awaiting_delivery", detail="Awaiting fresh delivery."),),
    }), expected_connector_revision=restored.revision, commit=m.CollectionCommit(
        work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
    ))


@pytest.mark.parametrize("source_index", [0, 1])
def test_every_still_owned_restored_source_keeps_its_fresh_capability_barrier(source_index):
    from test_monitoring_readiness_liveness import accept_delivery

    h, _, _, worker, _, _, _, second, _ = two_restorations("memory")
    connector = receiving_connector(h, worker, second.connector)
    with pytest.raises(MonitoringConflict, match="new capability"):
        accept_delivery(
            h, worker, connector, "stale-restored-source", enqueued_at=h.clock(), source_index=source_index,
        )
    fresh_capability(h, None, connector.sources[source_index].target)
    assert accept_delivery(
        h, worker, connector, "fresh-restored-source", enqueued_at=h.clock(), source_index=source_index,
    ).status == "accepted"


@pytest.mark.parametrize("damage", ["missing", "duplicate"])
def test_restored_intake_refuses_incomplete_or_duplicate_revision_history(damage):
    from test_monitoring_readiness_liveness import accept_delivery

    h, _, _, worker, first_request, first, _, second, _ = two_restorations("memory")
    connector = receiving_connector(h, worker, second.connector)
    fresh_capability(h, None, connector.sources[0].target)
    key = next(
        key for key, row in h.state.receipts.items()
        if row.operation == "connector_publication" and row.request_id != first_request.request_id
        and m.ConnectorPublicationResult.model_validate_json(row.payload).connector.revision > first.connector.revision
    )
    if damage == "missing":
        h.state.receipts.pop(key)
    else:
        row = h.state.receipts[key]
        extra_id = h.next_id()
        h.state.receipts[(*key[:-1], key_digest(extra_id))] = replace(row, request_id=extra_id)
    with pytest.raises(MonitoringUnavailable, match="history"):
        accept_delivery(h, worker, connector, "damaged-history", enqueued_at=h.clock())


def test_recovered_readiness_rejects_an_original_batch_recorded_before_publication():
    from test_monitoring_readiness_liveness import accept_delivery

    h, controller, worker, owned, _, _, _, request = recovery_case()
    restored = controller.publish_connector(request).connector
    h.clock.advance(1)
    h.capability(h.targets[0], event_status="verified", action_status="unknown")
    degraded = receiving_connector(h, worker, restored)
    signal = accept_delivery(h, worker, degraded, "batch-recorded-before-publication", enqueued_at=h.clock())
    assert worker.get_connector_delivery(h.version, owned.connector_id, uid(4)) is not None
    desired = controller.get_connector_desired(h.version, owned.connector_id)
    key = next(
        key for key, row in h.state.receipts.items()
        if row.operation == "stream_intake" and row.request_id == signal.transport.request_id
    )
    row = h.state.receipts[key]
    receipt = m.IntakeReceipt.model_validate_json(row.payload)
    recorded_at = desired.published_at - timedelta(seconds=1)
    h.state.receipts[key] = replace(
        row, recorded_at=recorded_at,
        payload=receipt.model_copy(update={"recorded_at": recorded_at}).model_dump_json(),
    )
    assert worker.get_connector_delivery(h.version, owned.connector_id, uid(4)) is None
