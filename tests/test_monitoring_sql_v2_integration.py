from __future__ import annotations

import json
from copy import deepcopy
from datetime import timedelta

import pytest
from test_monitoring_sql_abi import AbiDatabase
from test_monitoring_sql_review9_bindings import Review9Database, claim_sibling
from test_monitoring_sql_store import DriverRow
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
)
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.sql_store import AzureSqlMonitoringStore


def source_store(*, approval=False):
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    review = h.review("powerbi_refresh")
    request = h.reserve_request(review, approval=approval)
    db = AbiDatabase(h, principal="controller")
    db.seed_published_fixture(h)
    store = AzureSqlMonitoringStore(db=db, component="controller")
    source = m.SourceRunObservation.model_validate({**h.source.model_dump(), "authority": "rest", "origin": "poll"})
    return h, db, store, request, source


def test_fresh_source_uses_named_lease_checked_rpc_not_projection_dml():
    h, db, store, _, source = source_store()
    saved = store.observe_source(source, work_id=h.work.work_id, lease=h.work.lease)
    assert saved == source
    assert store.get_source(source.execution) == source
    assert any("controller_publish_source" in sql for _, sql, _ in db.calls)
    assert not any(method == "execute" for method, _, _ in db.calls)
    db.target_leases[source.execution.target.key] = (uid(99_990), h.clock() + timedelta(minutes=5))
    changed = source.model_copy(update={"observed_at": h.clock() + timedelta(seconds=1)})
    h.clock.advance(1)
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringLeaseLost):
        store.observe_source(changed, work_id=h.work.work_id, lease=h.work.lease)
    assert (db.records, db.receipts) == before


def test_nonreserved_old_policy_cannot_publish_fresh_source():
    h, db, store, _, source = source_store()
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2})
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringConflict):
        store.observe_source(source, work_id=h.work.work_id, lease=h.work.lease)
    assert (db.records, db.receipts) == before


def test_scope_publication_creates_only_logical_source_proposals_from_an_empty_connector_baseline():
    h = Harness()
    h.seed()
    db = Review9Database(h)
    db.principal = "web"
    web = AzureSqlMonitoringStore(db=db, component="web")
    plan = web.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=m.ScopeDefinition(
            **h.context(), scope_id=h.next_id(), name="Explicit proposal scope",
            rules=(m.ScopeRule(
                rule_id=h.next_id(), selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"), effect="include",
            ),),
        ),
    ))
    receipt = web.activate_scope(m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    assert receipt.state == "configuring"
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = claim_sibling(h, controller)
    result = controller.reconcile_work(work)
    assert result.state == "published"
    connectors = controller.list_connectors(m.PageQuery(**h.context())).items
    assert len(connectors) == 1
    planned = connectors[0]
    assert planned.state == "planned" and planned.sources == ()
    assert len(planned.source_proposals) == 1 and planned.source_proposals[0].source_id is None
    assert planned.source_proposals[0].target == h.targets[0]
    assert planned.desired_definition["component_ids"] == {}
    assert planned.workspace_id is None and planned.eventstream_id is None and planned.destination_id is None
    assert any(row.kind == "work" and row.work_kind == "connector_reconcile" for row in db.records.values())


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_owned_historical_source_disposition_is_atomic_and_needs_no_fake_incident(backend):
    h, db, sql, _, source = source_store()
    source = m.SourceRunObservation.model_validate({
        **source.model_dump(), "started_at": h.control.activation_cutoff - timedelta(minutes=2),
        "ended_at": h.control.activation_cutoff - timedelta(minutes=1),
    })
    store = sql if backend == "sql" else InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    store.observe_source(source, work_id=h.work.work_id, lease=h.work.lease)
    request = m.WorkDispositionRequest(
        **h.context(), request_id=h.next_id(), work_id=h.work.work_id, lease=h.work.lease,
        expected_work_revision=h.work.revision, disposition="historical",
        detail="Exact source execution predates this deployment's activation cutoff.",
    )
    saved = store.disposition_work(request)
    assert saved.state == "dispositioned" and saved.lease is None
    assert store.get_source_disposition(source.execution).disposition == "historical"
    assert store.disposition_work(request) == saved
    assert not h.state.incidents
    if backend == "sql":
        assert source.key in db.processed


@pytest.mark.parametrize("failure", ["before", "after"])
def test_pending_approval_binding_keeps_original_arguments_and_current_source_ownership(failure):
    h, db, store, request, source = source_store(approval=True)
    approval_id = request.approval.approval_id
    db.records.pop(("approval_binding", approval_id))
    pending = deepcopy(h.state.approvals[approval_id])
    pending.update(decision="", responder="", decided_at="", consumed_at="")
    # The approval store is a separate existing boundary; keep its pending row
    # explicit while exercising only the monitoring binding transaction.
    query = db.query

    def approval_query(sql, *args):
        if sql.startswith("SELECT decision,responder,decided_at,payload"):
            db.calls.append(("query", sql, args))
            return [DriverRow((None, None, None, json.dumps(pending)))]
        return query(sql, *args)

    db.query = approval_query
    store.observe_source(source, work_id=h.work.work_id, lease=h.work.lease)
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain) as uncertain:
        store.bind_approval(request)
    assert uncertain.value.operation == "approval_binding"
    assert uncertain.value.idempotency_id == approval_id
    bound = store.bind_approval(request)
    assert bound.reference == request.approval
    assert bound.arguments_hash == m._digest(request.arguments)
    assert pending["decision"] == "" and pending["consumed_at"] == ""
    assert store.get_operation_receipt(h.version, "approval_binding", approval_id).result == bound.model_dump(mode="json")
    before = deepcopy((db.records, db.receipts))
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2, "maintenance": True})
    assert store.bind_approval(request) == bound
    assert (db.records, db.receipts) == before


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_published_window_sibling_preserves_published_root_under_new_policy(backend):
    h = Harness()
    h.seed()
    h.activate()
    db = Review9Database(h) if backend == "sql" else None
    controller = (
        AzureSqlMonitoringStore(db=db, component="controller") if db else
        InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    )
    if db:
        db.principal = "worker"
    worker = (
        AzureSqlMonitoringStore(db=db, component="worker") if db else
        InMemoryMonitoringStore(clock=h.clock, state=h.state, component="worker")
    )
    if db:
        db.principal = "controller"
    draft = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=h.version.revision,
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=h.targets[0].workspace_id),
        created_at=h.clock(), due_at=h.clock(), reason="Bounded two-page publication.",
    ))
    if db:
        db.principal = "worker"
    collection = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(995), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))[0]
    generation = worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=draft.work_id, selector=draft.discovery_selector, adapter="fixture",
            authority="tenant_admin", completeness="partial", started_at=h.clock(), continuation="next",
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="One further original page remains."),),
        ),
        commit=m.InventoryCommit(work_id=collection.work_id, lease=collection.lease,
                                 expected_work_revision=collection.revision, expected_generation_revision=0),
    ))
    if db:
        db.principal = "controller"
    first = claim_sibling(h, controller)
    pending = controller.reconcile_work(first)
    assert pending.state == "pending_validation"
    if db:
        db.principal = "worker"
    worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration.model_validate({
            **generation.model_dump(), "completeness": "complete", "continuation": None,
            "completed_at": h.clock(), "completed_pages": 2, "gaps": (),
        }),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
            expected_generation_revision=generation.revision, expected_continuation=generation.continuation,
        ),
    ))
    if db:
        db.principal = "controller"
    last = claim_sibling(h, controller)
    published = controller.reconcile_work(last)
    assert published.state == "published" and last.work_id != first.work_id
    if db:
        db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2})
    else:
        h.state.control_row["revision"] = 2
    h.clock.advance(16)
    sibling = claim_sibling(h, controller)
    assert sibling.work_id == first.work_id
    acknowledged = controller.reconcile_work(sibling)
    assert acknowledged.state == "published" and acknowledged.resolution_scope == "window_acknowledgement"
    assert acknowledged.window_resolution_request_id == published.request_id
    assert acknowledged.window_resolution_state == "published" and acknowledged.window_rejection_request_id is None
    assert controller.reconcile_work(first) == pending
    assert controller.reconcile_work(sibling) == acknowledged
    assert controller.get_work(h.version, sibling.work_id).state == "completed"
