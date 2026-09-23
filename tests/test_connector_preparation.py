from __future__ import annotations

from datetime import timedelta

import pytest
from monitoring_supersession_protocol import SupersessionProtocolDatabase
from test_monitoring_connector_retirement_store import scoped_connector
from test_monitoring_connector_supersession_store import recovery_case

from triage.monitoring import models as m
from triage.monitoring import provisioning
from triage.monitoring.contracts import MonitoringComponentDenied, MonitoringConflict
from triage.monitoring.controller import reconcile_monitoring_work
from triage.monitoring.records import stable_id


def preparation_case(backend):
    existing = scoped_connector(backend, database_type=SupersessionProtocolDatabase)
    h, db, _, controller, _, _ = existing
    _, _, worker, owned, pending, _, _, request = recovery_case(backend, existing=existing)
    work = controller.get_work(h.version, request.work_id)
    original = controller.get_connector_observation(h.version, request.observation_receipt_id)
    frontier = controller.get_validation_frontier(h.version, original.frontier_key)
    connector = next(
        item for item in controller.list_connectors(m.PageQuery(**h.context())).items
        if item.connector_id == owned.connector_id
    )
    context = m.ConnectorPublicationContext(
        phase="binding", request_id=h.next_id(), expected=h.version,
        work=work, frontier=frontier, connector=connector,
    )
    return h, db, controller, worker, pending, original, context


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_one_preparation_read_binds_the_original_observation(backend, monkeypatch):
    h, _, controller, _, _, original, context = preparation_case(backend)
    calls = {"snapshot": 0, "observation": 0}
    snapshot = controller.snapshot
    observation = controller.get_connector_observation

    def read_snapshot(*args, **kwargs):
        calls["snapshot"] += 1
        return snapshot(*args, **kwargs)

    def read_observation(*args, **kwargs):
        calls["observation"] += 1
        return observation(*args, **kwargs)

    monkeypatch.setattr(controller, "snapshot", read_snapshot)
    monkeypatch.setattr(controller, "get_connector_observation", read_observation)

    request = provisioning.prepare_connector_reconciliation(controller, context)

    assert calls == {"snapshot": 1, "observation": 1}
    assert request.expected == h.version
    assert request.work_id == context.work.work_id
    assert request.lease == context.work.lease
    assert request.observation_receipt_id == context.work.reconcile_request_id
    assert request.desired_definition == original.observation.observed_definition
    assert request.source_removal_supersessions
    assert request.readiness_receipt_id is None


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_expiry_during_target_reads_is_a_durable_rejection(backend, monkeypatch):
    h, _, controller, _, pending, original, context = preparation_case(backend)
    removal_request = pending.pending_removals[0].request_id
    prior_receipt = controller.get_connector_publication(h.version, removal_request)
    remaining = original.inspection.observed_at + timedelta(seconds=m.SUPERSESSION_EVIDENCE_TTL_SECONDS) - h.clock()
    h.clock.advance(int(remaining.total_seconds()) - 1)
    work = next(
        item for item in controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=h.owner, kinds=("reconcile_state",),
            limit=200, per_workspace_limit=200, lease_seconds=120,
        )) if item.work_id == context.work.work_id
    )
    resolve = controller.resolve_target
    calls = []

    def slow_target(*args, **kwargs):
        result = resolve(*args, **kwargs)
        h.clock.advance(2)
        calls.append(result)
        return result

    monkeypatch.setattr(controller, "resolve_target", slow_target)

    result = reconcile_monitoring_work(controller, work)

    assert calls, "The inspection must expire after target evidence is read"
    assert result.state == "rejected"
    assert controller.get_work(h.version, work.work_id).state == "completed"
    assert controller.get_connector_publication(h.version, removal_request) == prior_receipt
    current = next(
        item for item in controller.list_connectors(m.PageQuery(**h.context())).items
        if item.connector_id == context.connector.connector_id
    )
    assert current.source_removals == context.connector.source_removals
    assert current.sources == context.connector.sources
    assert current.revision == context.connector.revision
    followup_id = stable_id(h.version, f"connector-reobserve:{work.reconcile_request_id}")
    followup = controller.get_work(h.version, followup_id)
    assert followup is not None and followup.kind == "connector_reconcile"
    assert controller.reconcile_work(work) == result
    assert controller.get_work(h.version, followup_id) == followup


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_preparation_refuses_an_obsolete_context_without_publishing(backend):
    h, _, controller, _, _, _, context = preparation_case(backend)
    context = context.model_copy(update={
        "expected": m.RegistryVersion(**h.context(), revision=h.version.revision + 1),
    })
    with pytest.raises(MonitoringConflict):
        provisioning.prepare_connector_reconciliation(controller, context)


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_preparation_is_not_a_worker_capability(backend):
    _, db, _, worker, _, _, context = preparation_case(backend)
    if db is not None:
        db.principal = "worker"
    with pytest.raises(MonitoringComponentDenied):
        provisioning.prepare_connector_reconciliation(worker, context)
