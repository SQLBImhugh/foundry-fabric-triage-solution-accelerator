from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from monitoring_supersession_protocol import SupersessionProtocolDatabase
from pydantic import ValidationError
from test_monitoring_connector_retirement_store import removal_request, scoped_connector
from test_monitoring_readiness_liveness import accept_delivery
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.monitoring.controller import reconcile_monitoring_work
from triage.monitoring.memory import StoredRecord, key_digest, stable_id


def _collection_work_rows(h, db=None):
    return [
        m.MonitoringWork.model_validate_json(row.payload)
        for row in (db.records if db else h.state.records).values()
        if row.kind == "work" and row.work_kind == "connector_reconcile"
    ]


def _end_bootstrap_collections(h, worker, connector_id, db=None):
    if db:
        db.principal = "worker"
    for work in _collection_work_rows(h, db):
        if work.connector_id == connector_id and work.lease is not None:
            worker.disposition_work(m.WorkDispositionRequest(
                **h.context(), request_id=h.next_id(), work_id=work.work_id, lease=work.lease,
                expected_work_revision=work.revision, disposition="superseded",
                detail="Fixture bootstrap collection ends before the recovery scenario.",
            ))


def recovery_case(backend="memory", *, possible_write=False, finish_inspection=True, existing=None, source_index=0):
    h, db, web, controller, worker, owned = existing or scoped_connector(
        backend, database_type=SupersessionProtocolDatabase,
    )
    _end_bootstrap_collections(h, worker, owned.connector_id, db)
    removal_work, removal = removal_request(h, web, controller, owned, db, source_index=source_index)
    pending = controller.publish_connector(removal)
    controller.reconcile_work(removal_work)
    h.clock.advance(1)
    target_identity = owned.sources[source_index].target
    if db:
        prior_capability = db.model("target_capability", target_identity.key, m.CapabilityObservation)
        capability = prior_capability.model_copy(update={
            "capability_id": h.next_id(), "event_status": "unknown", "event_evidence": None,
            "action_status": "unknown", "checked_at": h.clock(),
        })
        db.native_put("target_capability", target_identity.key, capability.model_dump(mode="json"), target_key=target_identity.key)
        target = db.model("target", target_identity.key, m.MonitoringTarget)
        db.native_put("target", target_identity.key, target.model_copy(update={
            "capability_id": capability.capability_id, "observation": target.observation.model_copy(update={"events_enabled": False}),
            "action": m.ActionPolicy(),
        }).model_dump(mode="json"))
        db.principal = "worker"
    else:
        h.capability(target_identity, event_status="unknown", action_status="unknown")
    collection = None
    for _ in range(20):
        candidates = worker.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=h.owner, kinds=("connector_reconcile",), limit=1, per_workspace_limit=1,
        ))
        assert candidates, "Fixture has no remaining connector collection work"
        actual = candidates[0]
        if actual.connector_id == owned.connector_id:
            collection = actual
            break
    assert collection is not None
    current = next(item for item in worker.list_connectors(m.PageQuery(**h.context())).items
                   if item.connector_id == owned.connector_id)
    commit = m.CollectionCommit(
        work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
    )
    if possible_write:
        current = worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
            **current.model_dump(), "revision": current.revision + 1,
            "state": "provisioning", "updated_at": h.clock(),
            "gaps": (m.CoverageGap(
                code="definition_update_submitted_or_unknown",
                detail="An original pre-dispatch write intent must not be forgotten by later clean presence.",
            ),),
        }), expected_connector_revision=current.revision, commit=commit)
        h.clock.advance(1)
    observation = m.OwnedConnectorManifest.model_validate({
        **current.model_dump(), "revision": current.revision + 1, "state": "degraded",
        "observed_definition": owned.desired_definition, "updated_at": h.clock(),
        "operation_id": None, "identity_verified_at": None, "delivery_verified_at": None, "delivery_proof": None,
        "gaps": (m.CoverageGap(
            code="pending_source_removal_presence_observed",
            detail="Original complete GET-only running source presence.",
        ),),
    })
    inspection = m.ConnectorPresenceInspection(
        read_only=True, observed_at=h.clock(), definition_hash=m.connector_definition_hash(observation.observed_definition),
        component_states={value: "Running" for value in observation.observed_definition["component_ids"].values()},
    )
    worker.record_connector(
        h.version, observation, expected_connector_revision=current.revision, commit=commit, inspection=inspection,
    )
    receipt_id = stable_id(h.version, f"connector:{current.connector_id}:{current.revision}")
    if finish_inspection:
        worker.complete_collection_work(
            h.version, work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
        )
    h.clock.advance(1)
    if db:
        db.principal = "controller"
    original = controller.get_connector_observation(h.version, receipt_id)
    assert original.observation == observation and original.inspection == inspection
    claimed = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(920), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
    ))
    work = next(item for item in claimed if item.work_id == original.reconcile_work_id)
    frontier = controller.get_validation_frontier(h.version, original.frontier_key)
    request = m.ConnectorPublicationRequest(
        request_id=h.next_id(), expected=h.version, work_id=work.work_id, lease=work.lease,
        expected_work_revision=work.revision, expected_frontier_revision=frontier.accepted_revision,
        connector_id=current.connector_id, ownership_id=current.ownership_id,
        expected_connector_revision=observation.revision, name=current.name, sources=current.sources,
        source_proposals=current.source_proposals, source_removals=(),
        source_removal_supersessions=tuple(
            m.SourceRemovalSupersession(removal_id=item.removal_id, source_id=item.source_id)
            for item in pending.pending_removals
        ),
        desired_definition=owned.desired_definition, observation_receipt_id=receipt_id,
        detail="Restore exact retained source presence without readiness or action authority.",
    )
    return h, controller, worker, owned, pending, collection, observation, request


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_expired_presence_evidence_is_rejected_once_and_requeues_a_fresh_observation(backend):
    """Evidence that can never become younger must not be retried forever.

    A deployed controller retried the identical refusal 48 times across 23
    hours: the presence inspection had aged past its TTL, the preparation path
    raised, the SQL boundary reported it as MonitoringUnavailable -- a
    transient outage -- and the worker's collection work was already
    ``completed``, so nothing ever produced fresher evidence.
    """
    existing = scoped_connector(backend, database_type=SupersessionProtocolDatabase)
    h, db, _, controller, worker, owned = existing
    _, controller, worker, owned, pending, collection, _, request = recovery_case(backend, existing=existing)

    # The 120s lease expires inside the 300s evidence window, which is the
    # production shape: the controller re-claims work whose evidence has
    # already aged out.
    h.clock.advance(m.SUPERSESSION_EVIDENCE_TTL_SECONDS + 1)
    if db:
        db.principal = "controller"
    claimed = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(931), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
    ))
    work = next(item for item in claimed if item.work_id == request.work_id)

    outcome = reconcile_monitoring_work(controller, work)

    assert outcome.state == "rejected"
    # The detail is the kernel's own wording on the SQL backend, so the
    # backend-independent guarantee is the queued re-observation below.
    # Resolved, not left leased for another doomed attempt.
    assert controller.get_work(h.version, work.work_id).state == "completed"
    # The removal fence is untouched; nothing was superseded on stale evidence.
    current = next(item for item in controller.list_connectors(m.PageQuery(**h.context())).items
                   if item.connector_id == owned.connector_id)
    assert len(current.source_removals) == len(pending.pending_removals)
    assert current.sources == owned.sources
    # A fresh observation is queued, keyed so it cannot collide with the
    # completed collection work.
    if db:
        db.principal = "worker"
    refreshed = [
        item for item in _collection_work_rows(h, db)
        if item.connector_id == owned.connector_id and item.state == "queued"
        and "fresh bounded observation" in item.reason
    ]
    assert len(refreshed) == 1
    assert refreshed[0].work_id != collection.work_id


def test_an_overtaken_connector_handoff_is_rejected_rather_than_reconstructed():
    """A handoff the connector has moved past can never be reconstructed.

    The observation projection refuses a changed baseline by raising
    MonitoringUnavailable directly, with no cause, so it reads as a store
    outage and is retried. Two such handoffs sat leased for 23 hours in
    production behind exactly this.

    The guard lives on the shared engine and the SQL store inherits it; this
    exercises it directly, which the SQL protocol double cannot do outside a
    transaction. End-to-end SQL behaviour is covered by the expired-evidence
    test above.
    """
    existing = scoped_connector("memory", database_type=SupersessionProtocolDatabase)
    h, _, _, controller, _, owned = existing
    _, controller, _, owned, _, _, _, request = recovery_case("memory", existing=existing)
    producer = controller._get(
        "worker_reconcile_request", request.observation_receipt_id, h.version, m.ReconciliationRequest,
    )
    control = controller._control(h.version)
    connector = controller._get("connector", owned.connector_id, h.version, m.OwnedConnectorManifest)
    overtaken = connector.model_copy(update={"revision": connector.revision + 1})

    outcome = controller._stale_presence_evidence(producer, overtaken, control)

    assert outcome is not None and outcome[0] == "rejected"


def test_memory_supersession_preserves_original_removal_and_physical_identity():
    h, controller, _, owned, pending, _, _, request = recovery_case()
    before_receipts = deepcopy(h.state.receipts)
    result = controller.publish_connector(request)
    assert result.desired_changed and result.state == "provisioning"
    assert result.connector.sources == owned.sources
    assert result.connector.desired_definition == owned.desired_definition
    assert result.pending_removals == result.retired_sources == ()
    assert result.superseded_source_removals == pending.pending_removals
    assert result.connector.identity_verified_at is result.connector.delivery_verified_at is result.connector.delivery_proof is None
    assert all(h.state.receipts[key] == value for key, value in before_receipts.items())
    assert controller.get_connector_publication(h.version, pending.pending_removals[0].request_id) == pending
    assert controller.publish_connector(request) == result
    assert not any(row.kind == "connector_source_retirement" for row in h.state.records.values())
    target = controller.resolve_target(owned.sources[0].target)
    assert target is not None and target.observation.enabled and not target.observation.events_enabled and not target.action.enabled
    desired = controller.get_connector_desired(h.version, owned.connector_id)
    assert desired.supersession_request_id == request.request_id
    assert any(
        row.kind == "work" and row.work_kind == "capability_probe" and row.status == "queued"
        and m.MonitoringWork.model_validate_json(row.payload).target == owned.sources[0].target
        for row in h.state.records.values()
    )


def test_sql_adapter_restoration_uses_original_guarded_receipt_without_cross_role_reads():
    h, controller, _, owned, pending, _, _, request = recovery_case("sql")
    db = controller._sql.db
    before_receipts = deepcopy(db.receipts)
    before_calls = len(db.calls)
    result = controller.publish_connector(request)
    assert result.connector.sources == owned.sources
    assert result.superseded_source_removals == pending.pending_removals
    assert result.pending_removals == result.retired_sources == ()
    assert result.state == "provisioning" and result.desired_changed
    assert all(db.receipts[key] == receipt for key, receipt in before_receipts.items())
    assert controller.get_connector_publication(h.version, pending.pending_removals[0].request_id) == pending
    assert controller.publish_connector(request) == result
    assert not any(
        sql.startswith("SELECT request_id, fingerprint") and params[2].startswith("worker.")
        for _, sql, params in db.calls[before_calls:]
    )
    plan = m.ConnectorPublicationPlan.model_validate_json(
        db.records[("connector_publication", stable_id(h.version, f"connector-publication:{request.request_id}"))].payload,
    )
    assert plan.source_removal_supersessions == request.source_removal_supersessions


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_normal_controller_binding_dispatch_restores_from_original_presence_after_collection_completes(backend):
    h, controller, _, owned, pending, _, _, request = recovery_case(backend)
    work = controller.get_work(h.version, request.work_id)
    outcome = reconcile_monitoring_work(controller, work)
    assert outcome.state == "published"
    current = next(item for item in controller.list_connectors(m.PageQuery(**h.context())).items
                   if item.connector_id == owned.connector_id)
    assert current.sources == owned.sources and current.source_removals == ()
    assert current.state == "provisioning" and current.identity_verified_at is None and current.delivery_proof is None
    original_id = stable_id(h.version, f"connector-bind:{work.reconcile_request_id}:{work.lease.fence}")
    original = controller.get_connector_publication(h.version, original_id)
    assert original.superseded_source_removals == pending.pending_removals
    assert controller.get_work(h.version, work.work_id).state == "completed"
    assert controller.get_connector_publication(h.version, pending.pending_removals[0].request_id) == pending


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_explicit_scope_disable_supplies_policy_removal_authority_not_paused_state(backend):
    h, db, web, controller, _, owned = scoped_connector(backend, database_type=SupersessionProtocolDatabase)
    if db:
        db.principal = "web"
    plan = web.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(),
        scope=m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}),
    ))
    accepted = web.activate_scope(m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    if db:
        db.principal = "controller"
    work = next(item for item in controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(923), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
    )) if item.work_id in accepted.queued_work_ids)
    assert reconcile_monitoring_work(controller, work).state == "published"
    current = next(item for item in controller.list_connectors(m.PageQuery(**h.context())).items
                   if item.connector_id == owned.connector_id)
    assert current.sources == owned.sources
    assert len(current.source_removals) == 1
    assert current.source_removals[0].target == owned.sources[0].target
    assert current.desired_definition["parts"]["eventstream.json"]["sources"] == []


def test_memory_restoration_needs_new_capability_and_post_publication_delivery_before_readiness():
    h, controller, worker, owned, _, _, _, request = recovery_case()
    restored = controller.publish_connector(request).connector
    h.clock.advance(1)
    work = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=h.owner, kinds=("connector_reconcile",), limit=1, per_workspace_limit=1,
    ))[0]
    assert work.connector_id == owned.connector_id
    degraded = worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **restored.model_dump(), "revision": restored.revision + 1, "state": "degraded",
        "observed_definition": restored.desired_definition, "updated_at": h.clock(),
        "gaps": (m.CoverageGap(code="awaiting_delivery", detail="Awaiting new post-restoration delivery."),),
    }), expected_connector_revision=restored.revision, commit=m.CollectionCommit(
        work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
    ))
    with pytest.raises(MonitoringConflict, match="new capability"):
        accept_delivery(h, worker, degraded, "stale-capability", enqueued_at=h.clock())
    assert controller.get_connector_delivery(h.version, owned.connector_id, uid(4)) is None
    h.capability(h.targets[0], event_status="verified", action_status="unknown")
    current = next(item for item in worker.list_connectors(m.PageQuery(**h.context())).items
                   if item.connector_id == owned.connector_id)
    signal = accept_delivery(h, worker, current, "fresh-capability", enqueued_at=h.clock())
    proof = worker.get_connector_delivery(h.version, current.connector_id, uid(4))
    assert proof is not None and proof.receipt_key == signal.delivery.key
    current = next(item for item in worker.list_connectors(m.PageQuery(**h.context())).items
                   if item.connector_id == owned.connector_id)
    assert current.state == "degraded"
    reported_ready = m.OwnedConnectorManifest.model_validate({
        **current.model_dump(), "revision": current.revision + 1, "state": "ready", "gaps": (),
        "updated_at": h.clock(), "identity_verified_at": proof.identity_verified_at,
        "delivery_verified_at": proof.received_at, "delivery_proof": proof,
    })
    worker.record_connector(
        h.version, reported_ready, expected_connector_revision=current.revision,
        commit=m.CollectionCommit(work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision),
    )
    receipt_id = stable_id(h.version, f"connector:{current.connector_id}:{current.revision}")
    projected = controller.get_connector_observation(h.version, receipt_id)
    pending = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(922), kinds=("reconcile_state",), limit=200, per_workspace_limit=200,
    ))
    readiness = next(item for item in pending if item.work_id == projected.reconcile_work_id)
    assert reconcile_monitoring_work(controller, readiness).state == "published"
    ready = next(item for item in controller.list_connectors(m.PageQuery(**h.context())).items
                 if item.connector_id == owned.connector_id)
    assert ready.state == "ready" and ready.delivery_proof == proof
    target = controller.resolve_target(h.targets[0])
    assert target is not None and not target.action.enabled


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("fault", ["possible_write", "unfinished_inspection"])
def test_clean_latest_presence_cannot_erase_uncertain_write_or_unfinished_collection(backend, fault):
    h, controller, _, _, _, _, _, request = recovery_case(
        backend, possible_write=fault == "possible_write", finish_inspection=fault != "unfinished_inspection",
    )
    state = controller._sql.db if backend == "sql" else h.state
    before = deepcopy((state.records, state.receipts))
    with pytest.raises((MonitoringConflict, MonitoringUnavailable)):
        controller.publish_connector(request)
    assert (state.records, state.receipts) == before


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("damage", ["clock_regression", "pruned_intent"])
def test_retained_write_intent_or_missing_revision_history_always_blocks_supersession(backend, damage):
    h, controller, _, _, pending, _, _, request = recovery_case(backend, possible_write=True)
    state = controller._sql.db if backend == "sql" else h.state
    if backend == "sql":
        key = next(
            key for key, receipt in state.receipts.items() if key[0] == "worker.observe_connector"
            and any(gap["code"] == "definition_update_submitted_or_unknown"
                    for gap in receipt["payload"]["result"]["observation"]["gaps"])
        )
        if damage == "pruned_intent":
            state.receipts.pop(key)
        else:
            state.receipts[key]["recorded_at"] = pending.pending_removals[0].requested_at - timedelta(seconds=1)
    else:
        key = next(
            key for key, receipt in state.receipts.items() if receipt.operation == "connector"
            and any(gap.code == "definition_update_submitted_or_unknown"
                    for gap in m.OwnedConnectorManifest.model_validate_json(receipt.payload).gaps)
        )
        if damage == "pruned_intent":
            state.receipts.pop(key)
        else:
            state.receipts[key] = replace(
                state.receipts[key], recorded_at=pending.pending_removals[0].requested_at - timedelta(seconds=1),
            )
    before = deepcopy((state.records, state.receipts))
    with pytest.raises((MonitoringConflict, MonitoringUnavailable)):
        controller.publish_connector(request)
    assert (state.records, state.receipts) == before


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("event_status", ["denied", "blocked", "unsupported"])
def test_restoration_does_not_waive_explicit_event_capability_denial(backend, event_status):
    h, controller, _, owned, _, _, _, request = recovery_case(backend)
    state = controller._sql.db if backend == "sql" else h.state
    key, row = next(
        (key, row) for key, row in state.records.items()
        if row.kind == "target_capability" and row.key == owned.sources[0].target.key
    )
    capability = m.CapabilityObservation.model_validate_json(row.payload).model_copy(update={"event_status": event_status})
    state.records[key] = replace(row, payload=capability.model_dump_json())
    before = deepcopy((state.records, state.receipts))
    with pytest.raises((MonitoringConflict, MonitoringUnavailable)):
        controller.publish_connector(request)
    assert (state.records, state.receipts) == before


@pytest.mark.parametrize("event_status", ["unknown", "verified", "denied", "blocked", "unsupported"])
def test_memory_auto_detection_restoration_never_grants_action_or_releases_existing_budgets(event_status):
    h, controller, _, owned, pending, _, _, request = recovery_case()
    target_key = owned.sources[0].target.key
    target_row_key, target_row = next(
        (key, row) for key, row in h.state.records.items() if row.kind == "target" and row.key == target_key
    )
    target = m.MonitoringTarget.model_validate_json(target_row.payload)
    assert not target.action.enabled
    target = target.model_copy(update={"admission_basis": "auto_detection_only"})
    h.state.records[target_row_key] = replace(target_row, payload=target.model_dump_json())
    cap_key, cap_row = next(
        (key, row) for key, row in h.state.records.items() if row.kind == "target_capability" and row.key == target_key
    )
    capability = m.CapabilityObservation.model_validate_json(cap_row.payload).model_copy(update={"event_status": event_status})
    h.state.records[cap_key] = replace(cap_row, payload=capability.model_dump_json())
    protected = {}
    for kind, payload in (
        ("action", {"state": "uncertain", "fence": 7, "reserved_budget": 1}),
        ("incident_budget", {"actions_used": 1, "maximum_actions": 1}),
        ("approval_binding", {"state": "consumed", "fingerprint": "a" * 64}),
    ):
        key = f"protected:{kind}"
        identity = (h.version.tenant_id, h.version.epoch, kind, key_digest(key))
        row = StoredRecord(
            kind=kind, key=key, context=m.MonitoringContext(**h.context()),
            payload=json.dumps(payload, sort_keys=True), version=1, status=payload.get("state"),
        )
        h.state.records[identity] = row
        protected[identity] = row
    before_records, before_receipts = deepcopy((h.state.records, h.state.receipts))
    if event_status in {"unknown", "verified"}:
        result = controller.publish_connector(request)
        assert result.connector.sources == owned.sources
        assert result.superseded_source_removals == pending.pending_removals
        assert result.state == "provisioning" and result.connector.delivery_proof is None
    else:
        with pytest.raises(MonitoringConflict):
            controller.publish_connector(request)
        assert h.state.records == before_records
    current = controller.resolve_target(owned.sources[0].target)
    assert current is not None and current.admission_basis == "auto_detection_only"
    assert not current.action.enabled and not current.observation.events_enabled
    assert all(h.state.records[key] == row for key, row in protected.items())
    assert all(h.state.receipts[key] == receipt for key, receipt in before_receipts.items())


@pytest.mark.parametrize("fault", ["receipt_failure", "lost_ack"])
def test_sql_supersession_receipt_failure_and_lost_ack_keep_original_operation(fault):
    h, controller, _, owned, pending, _, _, request = recovery_case("sql")
    db = controller._sql.db
    before = deepcopy((db.records, db.receipts))
    if fault == "receipt_failure":
        db.fail_connector_receipt = True
        with pytest.raises(MonitoringConflict):
            controller.publish_connector(request)
        assert (db.records, db.receipts) == before
    else:
        db.fail_commit = "after"
        with pytest.raises(MonitoringCommitUncertain):
            controller.publish_connector(request)
        result = controller.get_connector_publication(h.version, request.request_id)
        assert result.connector.sources == owned.sources
        assert result.superseded_source_removals == pending.pending_removals
        assert controller.publish_connector(request) == result
        assert controller.get_connector_publication(h.version, pending.pending_removals[0].request_id) == pending


@pytest.mark.parametrize("fault", ["missing_superseded", "altered_original"])
def test_sql_caller_cannot_accept_default_empty_or_rewritten_supersession_result(monkeypatch, fault):
    _, controller, _, _, _, _, _, request = recovery_case("sql")
    db = controller._sql.db
    publish = db.publish_connector
    before = deepcopy((db.records, db.receipts))

    def changed(args):
        reply = publish(args)
        if fault == "missing_superseded":
            reply["result"].pop("superseded_source_removals")
        else:
            reply["result"]["superseded_source_removals"][0]["detail"] = "Rewritten original"
        db.receipts[("controller.publish_connector", args["request_id"])]["payload"]["result"] = deepcopy(reply["result"])
        return reply

    monkeypatch.setattr(db, "publish_connector", changed)
    with pytest.raises(MonitoringUnavailable):
        controller.publish_connector(request)
    assert (db.records, db.receipts) == before


def test_pending_removal_cannot_be_omitted_without_explicit_supersession():
    h, controller, _, _, _, _, _, request = recovery_case()
    h.capability(h.targets[0], event_status="verified", action_status="unknown")
    before = deepcopy((h.state.records, h.state.receipts))
    unrequested = request.model_copy(update={"source_removal_supersessions": ()})
    with pytest.raises(MonitoringConflict, match="omitted|cancelled|rewritten"):
        controller.publish_connector(unrequested)
    assert (h.state.records, h.state.receipts) == before


@pytest.mark.parametrize("fault", ["inspection", "fingerprint", "completion_receipt", "original_publication", "old_plan"])
def test_memory_supersession_refuses_missing_or_changed_original_evidence(fault):
    h, controller, _, _, pending, _, _, request = recovery_case()
    if fault == "inspection":
        key = next(key for key, row in h.state.records.items()
                   if row.kind == "worker_reconcile_request" and row.key == request.observation_receipt_id)
        row = h.state.records[key]
        payload = json.loads(row.payload)
        payload["request_payload"].pop("inspection")
        h.state.records[key] = replace(row, payload=json.dumps(payload))
    elif fault == "fingerprint":
        key = next(key for key, receipt in h.state.receipts.items()
                   if receipt.operation == "connector" and receipt.request_id == request.observation_receipt_id)
        h.state.receipts[key] = replace(h.state.receipts[key], fingerprint="f" * 64)
    elif fault == "completion_receipt":
        for key in [key for key, receipt in h.state.receipts.items() if receipt.operation == "collection_completion"]:
            h.state.receipts.pop(key)
    elif fault == "original_publication":
        key = next(key for key, receipt in h.state.receipts.items()
                   if receipt.operation == "connector_publication" and receipt.request_id == pending.pending_removals[0].request_id)
        h.state.receipts.pop(key)
    else:
        key = next(key for key, row in h.state.records.items()
                   if row.kind == "connector_publication" and row.key == pending.pending_removals[0].publication_id)
        h.state.records.pop(key)
    before = deepcopy((h.state.records, h.state.receipts))
    with pytest.raises((MonitoringConflict, MonitoringUnavailable)):
        controller.publish_connector(request)
    assert (h.state.records, h.state.receipts) == before


@pytest.mark.parametrize("fault", ["expired", "future", "scope_disabled", "read_expired", "worker_active", "worker_attempted"])
def test_memory_supersession_denials_preserve_all_state(fault):
    h, controller, worker, _, _, _, _, request = recovery_case()
    if fault == "expired":
        h.clock.advance(301)
    elif fault == "future":
        h.clock.advance(-2)
    elif fault == "scope_disabled":
        key = next(key for key, row in h.state.records.items() if row.kind == "scope")
        row = h.state.records[key]
        scope = m.ScopePolicy.model_validate_json(row.payload).model_copy(update={"enabled": False})
        h.state.records[key] = replace(row, payload=scope.model_dump_json())
    elif fault == "read_expired":
        key = next(key for key, row in h.state.records.items() if row.kind == "target_capability")
        row = h.state.records[key]
        capability = m.CapabilityObservation.model_validate_json(row.payload)
        capability = capability.model_copy(update={"expires_at": h.clock() - timedelta(seconds=1)})
        h.state.records[key] = replace(row, payload=capability.model_dump_json())
    else:
        if fault == "worker_active":
            candidate = worker.claim_work(m.WorkClaimRequest(
                **h.context(), owner_id=uid(921), kinds=("connector_reconcile",), limit=200, per_workspace_limit=200,
            ))
            assert any(item.connector_id == request.connector_id for item in candidate)
        else:
            key = next(key for key, row in h.state.records.items() if row.kind == "work"
                       and row.work_kind == "connector_reconcile" and row.status == "queued"
                       and m.MonitoringWork.model_validate_json(row.payload).connector_id == request.connector_id)
            row = h.state.records[key]
            work = m.MonitoringWork.model_validate_json(row.payload).model_copy(update={"attempts": 1})
            h.state.records[key] = replace(row, payload=work.model_dump_json())
    before = deepcopy((h.state.records, h.state.receipts))
    with pytest.raises((MonitoringConflict, MonitoringUnavailable)):
        controller.publish_connector(request)
    assert (h.state.records, h.state.receipts) == before


def test_original_observation_projection_is_controller_only():
    h, _, worker, _, _, _, _, request = recovery_case()
    with pytest.raises(MonitoringComponentDenied):
        worker.get_connector_observation(h.version, request.observation_receipt_id)


def test_empty_supersession_keeps_original_request_fingerprint_shape():
    _, _, _, _, _, _, _, request = recovery_case()
    normal = request.model_copy(update={"source_removal_supersessions": ()})
    payload = normal.model_dump(mode="json")
    assert "source_removal_supersessions" not in payload
    assert m.ConnectorPublicationRequest.model_validate(payload).model_dump(mode="json") == payload
    assert request.model_dump(mode="json")["source_removal_supersessions"]
    assert set(payload) == set(m.ConnectorPublicationRequest.model_fields) - {"source_removal_supersessions"}


@pytest.mark.parametrize("change", [
    {"read_only": False}, {"read_only": 1}, {"read_only": "true"},
    {"component_states": {}}, {"definition_hash": "not-a-hash"},
])
def test_presence_inspection_is_explicit_and_closed(change):
    inspection = {
        "read_only": True, "observed_at": "2035-01-01T12:00:00Z",
        "definition_hash": "A" * 64,
        "component_states": {uid(1): "Running", uid(2): "Running", uid(3): "Running"},
    }
    with pytest.raises(ValidationError):
        m.ConnectorPresenceInspection.model_validate(inspection | change)
