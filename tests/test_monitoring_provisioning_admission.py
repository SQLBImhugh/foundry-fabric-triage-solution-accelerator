from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import pytest
from monitoring_supersession_protocol import SupersessionProtocolDatabase
from test_monitoring_connector_retirement_store import scoped_connector
from test_monitoring_connector_supersession_store import recovery_case
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_event_readiness import native_delivery, receive
from test_monitoring_inventory import IDENTITY
from test_monitoring_provisioning import (
    CONNECTOR,
    CONTEXT,
    OWNER,
    connector,
    exclude,
    factory,
    setup_store,
    target,
    version,
)
from test_monitoring_provisioning_orchestration import (
    drain_controller,
    runner_for,
    seed_existing_publication,
)
from test_monitoring_sql_review9_bindings import connector_commit
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringUnavailable,
)
from triage.monitoring.controller import reconcile_monitoring_work
from triage.monitoring.events import ConnectorBinding
from triage.monitoring.memory import InMemoryMonitoringStore, key_digest, stable_id
from triage.monitoring.provisioning import (
    PRESENCE_GAP,
    OwnedEventCapabilityProbe,
    ProvisioningReview,
    _restored_source_definition,
    plan_definition,
    prepare_connector_binding,
    prepare_connector_publication,
    prepare_connector_supersession,
    publication_from_plan,
    publish_connector_intent,
)
from triage.monitoring.sql_store import AzureSqlMonitoringStore

__all__ = ["factory"]


def capability(seed, clock, *, event_status="verified", seconds=60):
    inventory, = seed.list_inventory(m.TargetQuery(**CONTEXT.model_dump())).items
    return seed.record_capability(version(seed), m.CapabilityObservation(
        capability_id=str(UUID(int=150_000 + int(clock().timestamp()))),
        target=target(), inventory_generation=inventory.generation_id, collector_identity_id=IDENTITY,
        read_status="verified", event_status=event_status, checked_at=clock(),
        expires_at=clock() + timedelta(seconds=seconds),
    ))


async def test_sql_capability_expiry_preserves_published_physical_source(tmp_path):
    fixture = PublicationHarness("sql", tmp_path)
    await fixture.collect()
    await fixture.execute(fixture.activate())
    await fixture.apply_worker()
    await fixture.drain()
    await fixture.apply_worker()
    await fixture.drain()
    original = fixture.connector()
    assert len(original.sources) == 1 and not original.source_removals
    expected = fixture.version("controller")
    desired = fixture.use("controller").get_connector_desired(CONTEXT, original.connector_id)
    fixture.h.clock.advance(3601)
    assert fixture.use("controller").resolve_target(fixture.identity) is None
    fixture.use("web").request_discovery(
        fixture.version("web"), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=fixture.h.next_id(),
    )
    await fixture.drain()
    current = fixture.connector()
    assert fixture.version("controller") == expected
    assert current.sources == original.sources and current.source_removals == ()
    assert current.desired_definition == original.desired_definition
    assert fixture.use("controller").get_connector_desired(CONTEXT, original.connector_id) == desired
    assert fixture.use("controller").resolve_target(fixture.identity) is None
    assert len(fixture.remote.update_bodies) == 1


@pytest.mark.parametrize("unavailable", ["expired", "inventory_unknown"])
async def test_same_policy_unknown_does_not_publish_source_removal(factory, tmp_path, unavailable):
    seed, state, clock, remote = setup_store()
    seed_existing_publication(seed, clock)
    capability(seed, clock)
    await factory(seed, clock, remote).run_once()
    original = connector(seed)
    original_version = version(seed)
    if unavailable == "expired":
        clock.advance(61)
    else:
        clock.advance(1)
        item, = seed.list_inventory(m.TargetQuery(**CONTEXT.model_dump())).items
        generation_id = str(UUID(int=151_001))
        seed.record_inventory(m.InventoryBatch(
            request_id=str(UUID(int=151_002)), expected=version(seed),
            generation=m.InventoryGeneration(
                **CONTEXT.model_dump(), generation_id=generation_id,
                selector=m.ScopeSelector(
                    tenant_id=CONTEXT.tenant_id, kind="workspace", workspace_id=target().workspace_id,
                ),
                adapter="bounded_unknown_fixture", authority="fixture", completeness="partial",
                started_at=clock(), completed_at=clock(), discovered_count=0, completed_pages=1,
                gaps=(m.CoverageGap(code="inventory_incomplete", detail="Source metadata is unreadable"),),
            ),
            items=(item.model_copy(update={
                "generation_id": generation_id, "state": "unknown", "observed_at": clock(),
            }),),
        ))
    assert seed.resolve_target(target()) is None
    web = InMemoryMonitoringStore(state=state, clock=clock, component="web")
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=str(UUID(int=151_003)),
    )
    await drain_controller(runner_for(controller, tmp_path), controller)
    assert version(seed) == original_version
    assert controller.resolve_target(target()) is None
    assert connector(seed).source_removals == ()
    assert connector(seed).sources == original.sources
    assert connector(seed).desired_definition == original.desired_definition
    assert len(remote.update_bodies) == 1
    clock.advance(1)
    if unavailable == "inventory_unknown":
        item, = seed.list_inventory(m.TargetQuery(**CONTEXT.model_dump())).items
        generation_id = str(UUID(int=152_001))
        seed.record_inventory(m.InventoryBatch(
            request_id=str(UUID(int=152_002)), expected=version(seed),
            generation=m.InventoryGeneration(
                **CONTEXT.model_dump(), generation_id=generation_id,
                selector=m.ScopeSelector(
                    tenant_id=CONTEXT.tenant_id, kind="workspace", workspace_id=target().workspace_id,
                ),
                adapter="bounded_recovered_fixture", authority="fixture", completeness="complete",
                started_at=clock(), completed_at=clock(), discovered_count=1, completed_pages=1,
            ),
            items=(item.model_copy(update={
                "generation_id": generation_id, "state": "present", "observed_at": clock(),
            }),),
        ))
    service = factory(seed, clock, remote)
    item, = seed.list_inventory(m.TargetQuery(**CONTEXT.model_dump())).items
    read = await service.pipeline_probe.probe(
        target(), inventory_generation=item.generation_id, checked_at=clock(),
    )

    async def renew():
        return None

    current = connector(seed)
    event = await OwnedEventCapabilityProbe(
        service.store, CONTEXT, service.rest, ConnectorBinding(
            tenant_id=current.tenant_id, connector_id=current.connector_id,
            workspace_id=current.workspace_id, eventstream_id=current.eventstream_id,
            destination_id=current.destination_id, endpoint=current.endpoint,
        ), clock=clock,
    ).verify(read, renew)
    assert event.read_status == event.event_status == "verified"
    seed.record_capability(version(seed), event)
    web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=str(UUID(int=152_003)),
    )
    await drain_controller(runner_for(controller, tmp_path), controller)
    assert controller.resolve_target(target()).observation.enabled
    assert not controller.resolve_target(target()).action.enabled
    assert connector(seed).desired_definition == original.desired_definition
    assert connector(seed).source_removals == () and connector(seed).state != "ready"
    assert version(seed) == original_version and len(remote.update_bodies) == 1


async def historical_pending_removal(factory):
    seed, state, clock, remote = setup_store()
    seed_existing_publication(seed, clock)
    capability(seed, clock, seconds=3600)
    await factory(seed, clock, remote).run_once()
    clock.advance(1)
    web = InMemoryMonitoringStore(state=state, clock=clock, component="web")
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    queued = web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=str(UUID(int=153_001)),
    )
    work, = controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))
    assert queued.work_id == work.work_id
    producer = controller.get_reconciliation_request(CONTEXT, work.reconcile_request_id, producer="web")
    frontier = controller.get_validation_frontier(CONTEXT, producer.frontier_key)
    prior = connector(seed)
    # Reproduce a previously published removal, not new planner authority.
    plan = plan_definition(prior, prior.desired_definition, (), removal_targets=(target(),))
    request = publication_from_plan(
        version(seed), work, frontier, prior, plan, request_id=str(UUID(int=153_002)),
    )
    original = controller.publish_connector(request)
    clock.advance(1)
    return seed, controller, clock, remote, request, original


async def test_readmitted_pending_removal_records_presence_without_post_or_history_rewrite(factory):
    seed, controller, clock, remote, request, original = await historical_pending_removal(factory)
    pending = connector(seed)
    assert controller.resolve_target(target()).observation.enabled
    service = factory(seed, clock, remote)
    result = await service.run_once()
    assert result.results[0].code == PRESENCE_GAP
    collected = service.store.get_work(CONTEXT, result.results[0].work_id)
    assert collected.state == "completed" and collected.lease is None
    observed = connector(seed)
    assert observed.source_removals == pending.source_removals and observed.sources == pending.sources
    assert observed.desired_definition == pending.desired_definition
    assert observed.state == "degraded" and observed.delivery_proof is None
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{pending.revision}")
    receipt = service.store.get_operation_receipt(CONTEXT, "worker.observe_connector", receipt_id)
    assert receipt.result["observed_definition_hash"] is not None
    assert receipt.result["collection_completion_eligible"]
    assert receipt.result["observation"]["observed_definition"] == observed.observed_definition
    inspection = m.ConnectorPresenceInspection.model_validate(receipt.result["inspection"])
    assert inspection.read_only is True
    assert inspection.definition_hash == receipt.result["observed_definition_hash"]
    assert set(inspection.component_states) == set(observed.observed_definition["component_ids"].values())
    assert set(inspection.component_states.values()) == {"Running"}
    assert "inspection" not in observed.model_dump()
    assert len(remote.update_bodies) == 1
    assert controller.get_connector_publication(CONTEXT, request.request_id) == original
    with pytest.raises(MonitoringConflict, match="Pending removal"):
        controller.publish_connector(m.ConnectorPublicationRequest.model_validate({
            **request.model_dump(), "request_id": str(UUID(int=153_003)),
            "expected_connector_revision": observed.revision, "source_removals": (),
            "desired_definition": pending.observed_definition,
        }))


async def prepared_supersession(factory):
    seed, controller, clock, remote, old_request, original = await historical_pending_removal(factory)
    service = factory(seed, clock, remote)
    prior = connector(seed)
    result = await service.run_once()
    assert result.results[0].code == PRESENCE_GAP
    clock.advance(1)
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{prior.revision}")
    work = next(value for value in controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",), limit=20, per_workspace_limit=20,
        lease_seconds=900,
    )) if value.reconcile_request_id == receipt_id)
    request = prepare_connector_supersession(
        controller, work, CONNECTOR, request_id=str(UUID(int=157_001)),
    )
    return seed, controller, clock, remote, old_request, original, work, request


async def test_supersession_preparation_uses_original_inspection_and_disjoint_intents(factory):
    seed, controller, _, remote, _, _, work, request = await prepared_supersession(factory)
    prior = connector(seed)
    original = controller.get_connector_observation(CONTEXT, work.reconcile_request_id)
    assert original.inspection is not None
    assert request.observation_receipt_id == work.reconcile_request_id
    assert request.readiness_receipt_id is None
    assert request.source_removals == ()
    assert request.sources == prior.sources
    assert request.source_removal_supersessions == (
        m.SourceRemovalSupersession(
            removal_id=prior.source_removals[0].removal_id, source_id=prior.sources[0].source_id,
        ),
    )
    assert request.desired_definition == original.observation.observed_definition
    assert prepare_connector_binding(
        controller, work, CONNECTOR, request_id=request.request_id,
    ) == request
    assert connector(seed) == prior and len(remote.update_bodies) == 1


async def test_guarded_supersession_restores_only_retained_source_unready_and_replays_original_result(factory):
    seed, controller, clock, remote, old_request, old_result, _, request = await prepared_supersession(factory)
    before = connector(seed)
    clock.advance(1)
    capability(seed, clock, event_status="unknown", seconds=3600)
    restored = publish_connector_intent(controller, request)
    assert restored.desired_changed and restored.state != "ready"
    assert restored.connector.sources == before.sources
    assert restored.superseded_source_removals == before.source_removals
    assert restored.pending_removals == () and restored.retired_sources == ()
    assert restored.connector.desired_definition == request.desired_definition
    assert all(getattr(restored.connector, name) is None for name in (
        "identity_verified_at", "delivery_verified_at", "delivery_proof",
    ))
    assert controller.get_connector_publication(CONTEXT, old_request.request_id) == old_result
    restarted = InMemoryMonitoringStore(state=seed._backend.state, clock=clock, component="controller")
    assert publish_connector_intent(restarted, request) == restored
    assert controller.get_connector_publication(CONTEXT, request.request_id) == restored
    assert not restarted.resolve_target(target()).action.enabled
    assert len(remote.update_bodies) == 1
    clock.advance(1)
    service = factory(seed, clock, remote)
    next_run = await service.run_once()
    assert next_run.results[0].code == "definition_verified"
    ordinary_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{restored.connector.revision}")
    ordinary = controller.get_connector_observation(CONTEXT, ordinary_id)
    assert ordinary.inspection is None
    assert ordinary.work_id == next_run.results[0].work_id
    current = connector(seed)
    read = await service.pipeline_probe.probe(
        target(), inventory_generation=controller.resolve_target(target()).inventory_generation,
        checked_at=clock(),
    )

    async def renew():
        return None

    probe = await OwnedEventCapabilityProbe(
        service.store, CONTEXT, service.rest, ConnectorBinding(
            tenant_id=current.tenant_id, connector_id=CONNECTOR, workspace_id=current.workspace_id,
            eventstream_id=current.eventstream_id, destination_id=current.destination_id, endpoint=current.endpoint,
        ), clock=clock,
    ).verify(read, renew)
    assert probe.event_status == "verified"
    assert current.delivery_proof is None and current.state != "ready"
    assert len(remote.update_bodies) == 1
    seed.record_capability(version(seed), probe)
    assert not controller.resolve_target(target()).observation.events_enabled
    clock.advance(1)
    checkpoints = await receive(
        service.store, clock, ConnectorBinding(
            tenant_id=current.tenant_id, connector_id=CONNECTOR, workspace_id=current.workspace_id,
            eventstream_id=current.eventstream_id, destination_id=current.destination_id, endpoint=current.endpoint,
        ),
        (native_delivery(clock, target()),),
    )
    assert checkpoints.accepted_positions == 1
    assert service.store.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY) is not None
    assert connector(seed).state != "ready"


def test_sql_caller_uses_original_inspection_projection_and_guarded_supersession():
    h, controller, _, owned, pending, collection, _, expected = recovery_case("sql")
    work = controller.get_work(h.version, expected.work_id)
    before = deepcopy(controller._sql.db.receipts)
    request = prepare_connector_supersession(
        controller, work, owned.connector_id, request_id=expected.request_id,
    )
    assert request.source_removal_supersessions == expected.source_removal_supersessions
    assert request.source_removals == () and request.desired_definition == owned.desired_definition
    original = controller.get_connector_observation(h.version, request.observation_receipt_id)
    assert original.work_id == collection.work_id and original.inspection is not None
    before_calls = len(controller._sql.db.calls)
    result = publish_connector_intent(controller, request)
    assert result.superseded_source_removals == pending.pending_removals
    assert result.connector.sources == owned.sources and result.state == "provisioning"
    assert result.connector.delivery_proof is None
    assert all(controller._sql.db.receipts[key] == value for key, value in before.items())
    assert publish_connector_intent(controller, request) == result
    assert not any(
        sql.startswith("SELECT request_id, fingerprint") and args[2].startswith("worker.")
        for _, sql, args in controller._sql.db.calls[before_calls:]
    )


async def test_normal_controller_binding_dispatch_selects_receipt_bound_supersession(factory):
    seed, controller, clock, remote, old_request, old_result, work, _ = await prepared_supersession(factory)
    clock.advance(1)
    capability(seed, clock, event_status="unknown", seconds=3600)
    result = controller.reconcile_work(work)
    current = connector(seed)
    assert result.state == "published"
    assert current.source_removals == () and current.sources == old_result.connector.sources
    assert current.state == "provisioning" and current.delivery_proof is None
    assert controller.get_work(CONTEXT, work.work_id).state == "completed"
    assert controller.get_connector_publication(CONTEXT, old_request.request_id) == old_result
    assert len(remote.update_bodies) == 1


async def test_supersession_preparation_cannot_refresh_an_old_inspection_clock(factory):
    _, controller, clock, _, _, _, work, _ = await prepared_supersession(factory)
    clock.advance(301)
    with pytest.raises(ProvisioningReview, match="inspection_expired"):
        prepare_connector_supersession(controller, work, CONNECTOR, request_id=str(UUID(int=157_002)))


async def test_supersession_lost_ack_recovers_original_result_without_second_mutation(factory, monkeypatch):
    seed, controller, _, remote, old_request, old_result, _, request = await prepared_supersession(factory)
    original_publish = controller.publish_connector
    calls = 0

    def lose_ack(value):
        nonlocal calls
        calls += 1
        original_publish(value)
        raise MonitoringCommitUncertain("controller.publish_connector", value.request_id)

    monkeypatch.setattr(controller, "publish_connector", lose_ack)
    result = publish_connector_intent(controller, request)
    current = connector(seed)
    assert calls == 1 and result.superseded_source_removals == old_result.pending_removals
    assert publish_connector_intent(controller, request) == result and calls == 1
    assert connector(seed) == current
    assert controller.get_connector_publication(CONTEXT, old_request.request_id) == old_result
    assert len(remote.update_bodies) == 1


@pytest.mark.parametrize("event_status", ["denied", "blocked"])
async def test_supersession_never_waives_explicit_event_denial(factory, event_status):
    seed, controller, clock, remote, _, _, _, request = await prepared_supersession(factory)
    prior = connector(seed)
    clock.advance(1)
    capability(seed, clock, event_status=event_status, seconds=3600)
    with pytest.raises(MonitoringConflict):
        publish_connector_intent(controller, request)
    assert connector(seed) == prior and len(remote.update_bodies) == 1


async def test_later_presence_cannot_erase_historical_before_post_intent(factory):
    seed, controller, clock, remote, old_request, old_result = await historical_pending_removal(factory)
    service = factory(seed, clock, remote)
    work, = service.store.claim_work(service.claim)
    prior = connector(seed)
    submitted = await service._save(
        work, prior, observed_definition=None, state="provisioning",
        gaps=(service._submission_gap(prior, work),),
    )
    intent_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{prior.revision}")
    intent = service.store.get_operation_receipt(CONTEXT, "worker.observe_connector", intent_id)
    clock.advance(1)
    await service._save(
        work, submitted, observed_definition=prior.observed_definition, state="degraded",
        gaps=(m.CoverageGap(code="later_metadata", detail="A later report cannot settle the original possible effect"),),
    )
    clock.advance(1)
    before_inspection = connector(seed)
    result = await service._process(work)
    assert result.code == PRESENCE_GAP
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{before_inspection.revision}")
    followup = next(value for value in controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",), limit=20, per_workspace_limit=20,
    )) if value.reconcile_request_id == receipt_id)
    request = prepare_connector_supersession(
        controller, followup, CONNECTOR, request_id=str(UUID(int=157_020)),
    )
    current = connector(seed)
    with pytest.raises(MonitoringConflict, match="prior possible connector write"):
        publish_connector_intent(controller, request)
    assert connector(seed) == current
    assert service.store.get_operation_receipt(CONTEXT, "worker.observe_connector", intent_id) == intent
    assert controller.get_connector_publication(CONTEXT, old_request.request_id) == old_result
    assert len(remote.update_bodies) == 1


async def test_supersession_preparation_rejects_a_reconstructed_latest_observation(factory):
    seed, controller, _, _, _, _, work, _ = await prepared_supersession(factory)
    original = controller.get_connector_observation(CONTEXT, work.reconcile_request_id)
    changed = original.observation.model_copy(update={"name": "A later observation is not the original"})
    with pytest.raises(MonitoringUnavailable, match="differs from the original"):
        prepare_connector_supersession(
            controller, work, CONNECTOR, request_id=str(UUID(int=157_003)), original_observation=changed,
        )
    assert connector(seed).source_removals


async def test_presence_inspection_time_is_original_get_time_not_later_save_time(factory):
    seed, controller, clock, remote, _, _ = await historical_pending_removal(factory)
    service = factory(seed, clock, remote)
    before = clock()
    original_probe = service.pipeline_probe.probe

    async def slow_read(*args, **kwargs):
        clock.advance(11)
        return await original_probe(*args, **kwargs)

    service.pipeline_probe.probe = slow_read
    prior = connector(seed)
    await service.run_once()
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{prior.revision}")
    original = controller.get_connector_observation(CONTEXT, receipt_id)
    assert original.inspection.observed_at == before
    assert original.observation.updated_at == clock() > before
    assert len(remote.update_bodies) == 1


async def test_lost_ack_recovers_same_original_inspection_and_collection_fence(factory):
    seed, controller, clock, remote, _, _ = await historical_pending_removal(factory)
    service = factory(seed, clock, remote)
    original_record = service.store.record_connector
    captured = []

    def lose_ack(expected, manifest, *, expected_connector_revision, commit, inspection=None):
        result = original_record(
            expected, manifest, expected_connector_revision=expected_connector_revision,
            commit=commit, inspection=inspection,
        )
        if inspection is not None:
            captured.append(inspection)
            clock.advance(1)
            raise MonitoringCommitUncertain(
                "worker.observe_connector",
                stable_id(CONTEXT, f"connector:{CONNECTOR}:{expected_connector_revision}"),
            )
        return result

    service.store.record_connector = lose_ack
    prior = connector(seed)
    result = await service.run_once()
    assert len(captured) == 1 and result.results[0].code == PRESENCE_GAP
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{prior.revision}")
    original = controller.get_connector_observation(CONTEXT, receipt_id)
    assert original.inspection == captured[0]
    assert original.work_id == result.results[0].work_id
    assert service.store.get_work(CONTEXT, original.work_id).state == "completed"
    assert len(remote.update_bodies) == 1


@pytest.mark.parametrize("fault", ["missing_original", "changed_original", "changed_endpoint"])
async def test_supersession_result_cannot_acknowledge_missing_or_changed_originals(factory, monkeypatch, fault):
    seed, controller, clock, _, _, _, _, request = await prepared_supersession(factory)
    prior = connector(seed)
    restored = m.OwnedConnectorManifest.model_validate({
        **prior.model_dump(), "revision": prior.revision + 1, "state": "provisioning",
        "desired_definition": request.desired_definition, "source_removals": (), "updated_at": clock(),
    })
    originals = prior.source_removals
    if fault == "missing_original":
        originals = ()
    elif fault == "changed_original":
        originals = (originals[0].model_copy(update={"request_id": str(UUID(int=157_009))}),)
    else:
        restored = restored.model_copy(update={
            "endpoint": restored.endpoint.model_copy(update={"entity": "different-transport"}),
        })
    reply = m.ConnectorPublicationResult(
        connector_id=CONNECTOR, connector=restored, state="provisioning", desired_changed=True,
        pending_removals=(), retired_sources=(), observation_receipt_id=request.observation_receipt_id,
        superseded_source_removals=originals,
    )
    monkeypatch.setattr(controller, "publish_connector", lambda value: reply)
    with pytest.raises(MonitoringUnavailable, match="Supersession"):
        publish_connector_intent(controller, request)
    assert connector(seed) == prior


@pytest.mark.parametrize("unverified", ["denied_read", "stopped"])
async def test_presence_inspection_never_reports_success_from_unverified_service_state(factory, unverified):
    seed, _, clock, remote, _, _ = await historical_pending_removal(factory)
    if unverified == "denied_read":
        remote.denied_items.add(target().item_id)
    else:
        remote.status = "Stopped"
    service = factory(seed, clock, remote)
    prior = connector(seed)
    result = await service.run_once()
    assert result.results[0].state == "waiting"
    assert result.results[0].code in {
        "pending_removal_source_read_unverified", "retained_source_topology_not_running",
    }
    assert connector(seed) == prior and connector(seed).delivery_proof is None
    assert len(remote.update_bodies) == 1


async def test_restoration_uses_only_exact_observed_retained_physical_source(factory):
    seed, _, _, remote, _, _ = await historical_pending_removal(factory)
    prior = connector(seed)
    restored = _restored_source_definition(prior, prior.observed_definition, prior.source_removals)
    assert restored == prior.observed_definition
    node = restored["parts"]["eventstream.json"]["sources"][0]
    assert restored["component_ids"][f"sources/{node['name']}"] == node["id"] == prior.sources[0].source_id
    assert connector(seed) == prior and len(remote.update_bodies) == 1


@pytest.mark.parametrize("drift", ["source_id", "source_target", "stream", "destination"])
async def test_restoration_rejects_changed_physical_identity_or_routing(factory, drift):
    seed, _, _, _, _, _ = await historical_pending_removal(factory)
    prior = connector(seed)
    observed = deepcopy(prior.observed_definition)
    graph = observed["parts"]["eventstream.json"]
    if drift == "source_id":
        node = graph["sources"][0]
        node["id"] = str(UUID(int=156_001))
        observed["component_ids"][f"sources/{node['name']}"] = node["id"]
    elif drift == "source_target":
        graph["sources"][0]["properties"]["itemId"] = str(UUID(int=156_002))
    elif drift == "stream":
        graph["streams"][0]["name"] = "different-stream"
    else:
        node = graph["destinations"][0]
        node["id"] = str(UUID(int=156_003))
        observed["component_ids"][f"destinations/{node['name']}"] = node["id"]
    with pytest.raises(ProvisioningReview):
        _restored_source_definition(prior, observed, prior.source_removals)


async def test_readmitted_uncertain_removal_reads_original_intent_without_another_post(factory):
    seed, _, clock, remote, _, _ = await historical_pending_removal(factory)
    service = factory(seed, clock, remote)
    work, = service.store.claim_work(service.claim)
    prior = connector(seed)
    await service._save(
        work, prior, observed_definition=None, operation_id=None, state="provisioning",
        gaps=(service._submission_gap(prior, work),),
    )
    submitted = connector(seed)
    result = await service._process(work)
    assert result.code == "definition_update_outcome_unknown"
    assert connector(seed) == submitted and connector(seed).source_removals == prior.source_removals
    assert len(remote.update_bodies) == 1


async def test_readmission_cannot_undo_already_observed_remote_removal(factory):
    seed, controller, clock, remote, request, original = await historical_pending_removal(factory)
    pending = connector(seed)
    remote.graph = deepcopy(pending.desired_definition["parts"]["eventstream.json"])
    result = await factory(seed, clock, remote).run_once()
    assert result.results[0].code == "controller_source_retirement_required"
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{pending.revision}")
    work = next(value for value in controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",), limit=20, per_workspace_limit=20,
    )) if value.reconcile_request_id == receipt_id)
    confirmation = prepare_connector_binding(
        controller, work, CONNECTOR, request_id=str(UUID(int=153_004)),
    )
    retired = publish_connector_intent(controller, confirmation)
    assert retired.connector.sources == () and retired.connector.source_removals == ()
    assert retired.retired_sources[0].original_removal == pending.source_removals[0]
    assert controller.get_connector_publication(CONTEXT, request.request_id) == original
    assert len(remote.update_bodies) == 1


async def test_complete_present_observation_cannot_cancel_without_explicit_supersession(factory):
    seed, controller, clock, remote, request, original = await historical_pending_removal(factory)
    service = factory(seed, clock, remote)
    work, = service.store.claim_work(service.claim)
    prior = connector(seed)
    observed, _ = await service.rest.inspect(prior, lambda: service._renew(work))
    assert observed["parts"]["eventstream.json"]["sources"]
    await service._save(
        work, prior, observed_definition=observed, state="degraded",
        gaps=(m.CoverageGap(code="source_present_readmitted", detail="Exact owned source remains present"),),
    )
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{prior.revision}")
    followup = next(value for value in controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",), limit=20, per_workspace_limit=20,
    )) if value.reconcile_request_id == receipt_id)
    confirmation = prepare_connector_binding(
        controller, followup, CONNECTOR, request_id=str(UUID(int=155_001)),
    )
    with pytest.raises(MonitoringConflict, match="observation|definition|absence"):
        publish_connector_intent(controller, confirmation)
    assert connector(seed).source_removals == prior.source_removals
    assert connector(seed).sources == prior.sources
    assert controller.get_connector_publication(CONTEXT, request.request_id) == original
    assert len(remote.update_bodies) == 1


async def test_paused_scope_disable_needs_explicit_removal_evidence_from_controller(factory):
    seed, state, clock, remote = setup_store()
    seed_existing_publication(seed, clock)
    capability(seed, clock, seconds=3600)
    await factory(seed, clock, remote).run_once()
    policy, = seed.list_scopes(m.PageQuery(**CONTEXT.model_dump())).items
    scope = m.ScopeDefinition.model_validate(policy.model_dump(exclude={"revision", "updated_at"}))
    plan = seed.preview_scope(m.ScopePreviewRequest(
        expected=version(seed), idempotency_id=str(UUID(int=154_001)),
        scope=scope.model_copy(update={"enabled": False}),
    ))
    seed.activate_scope(m.ActivateScopeRequest(
        expected=plan.expected, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    assert seed.resolve_target(target(), include_inactive=True).state == "paused"
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    web = InMemoryMonitoringStore(state=state, clock=clock, component="web")
    queued = web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=str(UUID(int=154_002)),
    )
    work = next(value for value in controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",), limit=20, per_workspace_limit=20,
    )) if value.work_id == queued.work_id)
    with pytest.raises(ProvisioningReview, match="source_removal_authority_unverified"):
        prepare_connector_publication(
            controller, work, CONNECTOR, request_id=str(UUID(int=154_003)),
        )
    request = prepare_connector_publication(
        controller, work, CONNECTOR, request_id=str(UUID(int=154_004)), removal_targets=(target(),),
    )
    pending = publish_connector_intent(controller, request)
    assert len(pending.pending_removals) == 1
    result = await factory(seed, clock, remote).run_once()
    assert result.results[0].code == "controller_source_retirement_required"
    assert remote.graph["sources"] == []


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_add_only_overtaken_observation_is_rejected_and_requeues_followup_on_both_adapters(
    backend, tmp_path,
):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    await fixture.execute(fixture.activate())
    planned = fixture.connector()
    proposal, = planned.source_proposals
    assert planned.sources == () and planned.source_removals == ()
    run = await fixture.apply_worker()
    assert run.results[0].code == "controller_source_binding_required"
    first_observed = fixture.connector()
    intent_receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{planned.revision}")
    first_receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{first_observed.revision - 1}")
    fixture.h.clock.advance(1)
    worker = fixture.use("worker")
    second_observed = worker.record_connector(
        fixture.version("worker"),
        m.OwnedConnectorManifest.model_validate({
            **first_observed.model_dump(),
            "revision": first_observed.revision + 1,
            "updated_at": fixture.h.clock(),
        }),
        expected_connector_revision=first_observed.revision,
        commit=connector_commit(fixture.h, worker, CONNECTOR, db=fixture.db),
    )
    second_receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{first_observed.revision}")
    controller = fixture.use("controller")
    claimed = controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=uid(94_101), kinds=("reconcile_state",),
        limit=20, per_workspace_limit=20, lease_seconds=120,
    ))
    intent_work = next(item for item in claimed if item.reconcile_request_id == intent_receipt_id)
    first_work = next(item for item in claimed if item.reconcile_request_id == first_receipt_id)
    second_work = next(item for item in claimed if item.reconcile_request_id == second_receipt_id)
    before_receipts = deepcopy(fixture.db.receipts if fixture.db else fixture.h.state.receipts)

    intent_outcome = reconcile_monitoring_work(controller, intent_work)
    first_outcome = reconcile_monitoring_work(controller, first_work)

    assert intent_outcome.state == ("pending_validation" if backend == "sql" else "rejected")
    assert first_outcome.state == ("pending_validation" if backend == "sql" else "rejected")
    reobserve_id = stable_id(CONTEXT, f"connector-reobserve:{first_receipt_id}")
    followup = controller.get_work(CONTEXT, reobserve_id)
    assert followup is not None and followup.kind == "connector_reconcile" and followup.state == "queued"
    assert followup.connector_id == CONNECTOR
    assert fixture.connector().sources == () and fixture.connector().source_proposals == (proposal,)
    assert fixture.connector().revision == second_observed.revision
    receipts = fixture.db.receipts if fixture.db else fixture.h.state.receipts
    assert all(receipts[key] == value for key, value in before_receipts.items())

    second_outcome = reconcile_monitoring_work(controller, second_work)
    assert second_outcome.state == "published"
    bound = fixture.connector()
    assert bound.source_proposals == () and bound.source_removals == ()
    assert len(bound.sources) == 1
    assert bound.sources[0].source_id == fixture.remote.graph["sources"][0]["id"]
    assert bound.sources[0].source_id not in {proposal.proposal_id, proposal.node_name}
    assert bound.state == "provisioning" and bound.delivery_proof is None

    if backend == "sql":
        fixture.h.clock.advance(16)
        reclaimed = {
            item.work_id: item for item in controller.claim_work(m.WorkClaimRequest(
                **CONTEXT.model_dump(), owner_id=uid(94_102), kinds=("reconcile_state",),
                limit=20, per_workspace_limit=20, lease_seconds=120,
            ))
        }
        for prior_work in (intent_work, first_work):
            ack = reconcile_monitoring_work(controller, reclaimed[prior_work.work_id])
            assert ack.state == "rejected" and ack.resolution_scope == "handoff_acknowledgement"
    assert controller.get_work(CONTEXT, intent_work.work_id).state == "completed"
    assert controller.get_work(CONTEXT, first_work.work_id).state == "completed"
    assert controller.get_work(CONTEXT, second_work.work_id).state == "completed"
    assert controller.get_work(CONTEXT, reobserve_id) == followup
    assert len(fixture.remote.update_bodies) == 1


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("phase", ["add", "remove"])
@pytest.mark.parametrize("fault", ["missing_baseline", "future_baseline"])
async def test_missing_or_future_baseline_fails_closed_without_false_overtaken_rejection(
    backend, phase, fault, tmp_path,
):
    if phase == "add":
        fixture = PublicationHarness(backend, tmp_path)
        await fixture.collect()
        await fixture.execute(fixture.activate())
        await fixture.apply_worker()
        controller = fixture.use("controller")
        current_version = fixture.version("controller")
        current_connector = fixture.connector()
        binding_receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{current_connector.revision - 1}")
        claimed = controller.claim_work(m.WorkClaimRequest(
            **CONTEXT.model_dump(), owner_id=uid(94_200), kinds=("reconcile_state",),
            limit=20, per_workspace_limit=20, lease_seconds=120,
        ))
        work = next(item for item in claimed if item.reconcile_request_id == binding_receipt_id)
        db = fixture.db
        state = fixture.h.state
    else:
        h, controller, _, _, _, _, _, request = recovery_case(backend)
        work = controller.get_work(h.version, request.work_id)
        current_version = h.version
        current_connector = next(
            item for item in controller.list_connectors(m.PageQuery(**h.context())).items
            if item.connector_id == request.connector_id
        )
        db = controller._sql.db if backend == "sql" else None
        state = h.state

    request_id = work.reconcile_request_id
    if backend == "memory":
        obs_key = (
            current_version.tenant_id, current_version.epoch,
            "connector_observation", key_digest(request_id),
        )
        if fault == "missing_baseline":
            state.records.pop(obs_key)
        else:
            row = state.records[obs_key]
            future_obs = m.OwnedConnectorManifest.model_validate_json(row.payload).model_copy(
                update={"revision": current_connector.revision + 2},
            )
            state.records[obs_key] = replace(row, payload=future_obs.model_dump_json())
    else:
        req_key = ("worker_reconcile_request", request_id)
        row = db.records[req_key]
        document = json.loads(row.payload)
        if fault == "missing_baseline":
            document["request_payload"]["expected_connector_revision"] = None
        else:
            document["request_payload"]["expected_connector_revision"] = current_connector.revision + 1
        db.records[req_key] = replace(row, payload=json.dumps(document, sort_keys=True))

    def check_not_overtaken():
        producer = controller._get(
            "worker_reconcile_request", request_id, current_version, m.ReconciliationRequest,
        )
        return controller._connector_observation_overtaken(producer, current_connector, current_version)

    if db is None:
        assert check_not_overtaken() is False
    else:
        with controller._sql.transaction(write=False, operation="inspect", request_id=uid(94_201)):
            assert check_not_overtaken() is False

    before_records = deepcopy(db.records if db else state.records)
    before_receipts = deepcopy(db.receipts if db else state.receipts)
    with pytest.raises(MonitoringUnavailable):
        controller.get_connector_observation(current_version, request_id)
    with pytest.raises(MonitoringUnavailable):
        reconcile_monitoring_work(controller, work)

    assert (db.records if db else state.records) == before_records
    assert (db.receipts if db else state.receipts) == before_receipts
    assert controller.get_work(current_version, work.work_id).state == "leased"
    reobserve_id = stable_id(current_version, f"connector-reobserve:{request_id}")
    assert controller.get_work(current_version, reobserve_id) is None


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_late_expiry_during_supersession_preparation_rejects_and_requeues_on_both_adapters(
    backend, monkeypatch,
):
    h, controller, _, owned, pending, collection, _, request = recovery_case(backend)
    original_observation = controller.get_connector_observation(h.version, request.observation_receipt_id)
    removal_request_id = pending.pending_removals[0].request_id
    original_removal_receipt = controller.get_connector_publication(h.version, removal_request_id)
    remaining = (
        original_observation.inspection.observed_at
        + timedelta(seconds=m.SUPERSESSION_EVIDENCE_TTL_SECONDS)
        - h.clock()
    )
    h.clock.advance(int(remaining.total_seconds()) - 1)
    assert h.clock() - original_observation.inspection.observed_at < timedelta(
        seconds=m.SUPERSESSION_EVIDENCE_TTL_SECONDS,
    )
    work = next(
        item for item in controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(94_250), kinds=("reconcile_state",),
            limit=200, per_workspace_limit=200, lease_seconds=120,
        )) if item.work_id == request.work_id
    )
    original_snapshot = controller.snapshot
    snapshot_calls = []

    def expiring_snapshot(*args, **kwargs):
        result = original_snapshot(*args, **kwargs)
        h.clock.advance(3)
        snapshot_calls.append(h.clock())
        return result

    monkeypatch.setattr(controller, "snapshot", expiring_snapshot)

    outcome = reconcile_monitoring_work(controller, work)

    assert snapshot_calls, "Preparation must execute after the early freshness guard"
    assert outcome.state == "rejected"
    assert outcome.producer_request_id == work.reconcile_request_id
    finished = controller.get_work(h.version, work.work_id)
    assert finished.state == "completed" and finished.lease is None
    reobserve_id = stable_id(h.version, f"connector-reobserve:{work.reconcile_request_id}")
    followup = controller.get_work(h.version, reobserve_id)
    assert followup is not None and followup.kind == "connector_reconcile" and followup.state == "queued"
    assert followup.work_id != collection.work_id
    current = next(
        item for item in controller.list_connectors(m.PageQuery(**h.context())).items
        if item.connector_id == owned.connector_id
    )
    assert current.sources == owned.sources
    assert current.source_removals == pending.pending_removals
    assert current.revision == original_observation.observation.revision
    assert controller.get_connector_publication(h.version, removal_request_id) == original_removal_receipt
    assert controller.get_connector_observation(h.version, request.observation_receipt_id) == original_observation


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("reason", ["overtaken", "late_expiry"])
def test_rejection_rollback_and_followup_reobservation_deduplication(
    backend, reason, monkeypatch,
):
    existing = scoped_connector(backend, database_type=SupersessionProtocolDatabase)
    h, db, _, controller, worker, owned = existing
    _, controller, worker, owned, pending, collection, observed, request = recovery_case(
        backend, existing=existing, finish_inspection=reason != "overtaken",
    )
    work = controller.get_work(h.version, request.work_id)
    reobserve_id = stable_id(h.version, f"connector-reobserve:{work.reconcile_request_id}")
    if reason == "overtaken":
        h.clock.advance(1)
        if db:
            db.principal = "worker"
        newer = m.OwnedConnectorManifest.model_validate({
            **observed.model_dump(), "revision": observed.revision + 1, "updated_at": h.clock(),
        })
        worker.record_connector(
            h.version, newer, expected_connector_revision=observed.revision,
            commit=m.CollectionCommit(
                work_id=collection.work_id, lease=collection.lease,
                expected_work_revision=collection.revision,
            ),
            inspection=m.ConnectorPresenceInspection(
                read_only=True, observed_at=h.clock(),
                definition_hash=m.connector_definition_hash(newer.observed_definition),
                component_states={
                    value: "Running" for value in newer.observed_definition["component_ids"].values()
                },
            ),
        )
        worker.complete_collection_work(
            h.version, work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision,
        )
        if db:
            db.principal = "controller"
            newer_id = stable_id(h.version, f"connector:{owned.connector_id}:{observed.revision}")
            newer_obs = controller.get_connector_observation(h.version, newer_id)
            newer_work, = [
                item for item in controller.claim_work(m.WorkClaimRequest(
                    **h.context(), owner_id=uid(94_301), kinds=("reconcile_state",),
                    limit=200, per_workspace_limit=200, lease_seconds=120,
                )) if item.work_id == newer_obs.reconcile_work_id
            ]
            assert reconcile_monitoring_work(controller, newer_work).state == "pending_validation"
            work = controller.get_work(h.version, request.work_id)
    else:
        original_observation = controller.get_connector_observation(h.version, request.observation_receipt_id)
        remaining = (
            original_observation.inspection.observed_at
            + timedelta(seconds=m.SUPERSESSION_EVIDENCE_TTL_SECONDS)
            - h.clock()
        )
        h.clock.advance(int(remaining.total_seconds()) - 1)
        work = next(
            item for item in controller.claim_work(m.WorkClaimRequest(
                **h.context(), owner_id=uid(94_302), kinds=("reconcile_state",),
                limit=200, per_workspace_limit=200, lease_seconds=120,
            )) if item.work_id == request.work_id
        )
        original_snapshot = controller.snapshot

        def expiring_snapshot(*args, **kwargs):
            result = original_snapshot(*args, **kwargs)
            h.clock.advance(3)
            return result

        monkeypatch.setattr(controller, "snapshot", expiring_snapshot)

    before_records = deepcopy(db.records if db else h.state.records)
    before_receipts = deepcopy(db.receipts if db else h.state.receipts)
    if db:
        db.fail_commit = "before"
        with pytest.raises(MonitoringCommitUncertain):
            reconcile_monitoring_work(controller, work)
    else:
        original_finish = controller._finish_reconciliation

        def fail_before_finish(*args, **kwargs):
            assert controller._get("work", reobserve_id, h.version, m.MonitoringWork) is not None
            raise MonitoringConflict("Injected rollback after follow-up enqueue")

        monkeypatch.setattr(controller, "_finish_reconciliation", fail_before_finish)
        with pytest.raises(MonitoringConflict, match="Injected rollback"):
            reconcile_monitoring_work(controller, work)
        monkeypatch.setattr(controller, "_finish_reconciliation", original_finish)

    assert (db.records if db else h.state.records) == before_records
    assert (db.receipts if db else h.state.receipts) == before_receipts
    assert controller.get_work(h.version, reobserve_id) is None
    assert controller.get_work(h.version, work.work_id).state == "leased"

    if reason == "late_expiry":
        # Restore the 1-second pre-expiry margin consumed by the rolled-back attempt.
        h.clock.advance(-3)
    first = reconcile_monitoring_work(controller, work)
    assert first.state == "rejected"
    followup = controller.get_work(h.version, reobserve_id)
    assert followup is not None and followup.kind == "connector_reconcile" and followup.state == "queued"

    assert reconcile_monitoring_work(controller, work) == first
    deduped = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=reobserve_id, kind="connector_reconcile",
        connector_id=owned.connector_id, policy_revision=h.version.revision,
        created_at=followup.created_at, due_at=followup.due_at,
        reason="Expired source-presence evidence requires a fresh bounded observation.",
    ))
    assert deduped == followup
    reobserve_rows = [
        row for row in (db.records if db else h.state.records).values()
        if row.kind == "work" and row.key == reobserve_id
    ]
    assert len(reobserve_rows) == 1


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("phase", ["add", "remove", "expired_supersession"])
@pytest.mark.parametrize("recovery_mode", ["reclaim_after_rollback", "lost_ack"])
async def test_reclaim_and_lost_ack_recovery_preserve_ownership_authority_and_original_receipts(
    backend, phase, recovery_mode, tmp_path, monkeypatch,
):
    if phase in {"add", "remove"}:
        fixture = PublicationHarness(backend, tmp_path)
        await fixture.collect()
        await fixture.execute(fixture.activate())
        await fixture.apply_worker()
        if phase == "remove":
            await fixture.drain()
            exclude(fixture.use("web"), fixture.identity, nonce=94_401)
            await fixture.drain()
            await fixture.apply_worker()
        controller = fixture.use("controller")
        current_version = fixture.version("controller")
        current_connector = fixture.connector()
        binding_receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{current_connector.revision - 1}")
        claimed = controller.claim_work(m.WorkClaimRequest(
            **CONTEXT.model_dump(), owner_id=uid(94_400), kinds=("reconcile_state",),
            limit=20, per_workspace_limit=20, lease_seconds=120,
        ))
        for item in claimed:
            if item.reconcile_request_id != binding_receipt_id:
                reconcile_monitoring_work(controller, item)
        work = next(item for item in claimed if item.reconcile_request_id == binding_receipt_id)
        db = fixture.db
        state = fixture.h.state
        clock = fixture.h.clock
    else:
        h, controller, _, owned, pending, _, _, request = recovery_case(backend)
        clock = h.clock
        clock.advance(m.SUPERSESSION_EVIDENCE_TTL_SECONDS + 1)
        current_version = h.version
        db = controller._sql.db if backend == "sql" else None
        state = h.state
        if db:
            db.principal = "controller"
        work = next(
            item for item in controller.claim_work(m.WorkClaimRequest(
                **h.context(), owner_id=uid(94_402), kinds=("reconcile_state",),
                limit=200, per_workspace_limit=200, lease_seconds=120,
            )) if item.work_id == request.work_id
        )

    before_receipts = deepcopy(db.receipts if db else state.receipts)
    if recovery_mode == "reclaim_after_rollback":
        before_records = deepcopy(db.records if db else state.records)
        if db:
            db.fail_commit = "before"
            with pytest.raises(MonitoringCommitUncertain):
                reconcile_monitoring_work(controller, work)
        else:
            original_finish = controller._finish_reconciliation

            def abort_before_commit(*args, **kwargs):
                raise MonitoringConflict("Injected abort before reconciliation finishes")

            monkeypatch.setattr(controller, "_finish_reconciliation", abort_before_commit)
            with pytest.raises(MonitoringConflict, match="Injected abort"):
                reconcile_monitoring_work(controller, work)
            monkeypatch.setattr(controller, "_finish_reconciliation", original_finish)
        assert (db.records if db else state.records) == before_records
        clock.advance(121)
        reclaimed = next(
            item for item in controller.claim_work(m.WorkClaimRequest(
                **current_version.model_dump(exclude={"revision"}),
                owner_id=uid(94_403), kinds=("reconcile_state",),
                limit=200, per_workspace_limit=200, lease_seconds=120,
            )) if item.work_id == work.work_id
        )
        assert reclaimed.lease.fence == work.lease.fence + 1
        with pytest.raises((MonitoringLeaseLost, MonitoringConflict)):
            reconcile_monitoring_work(controller, work)
        outcome = reconcile_monitoring_work(controller, reclaimed)
        target_work = reclaimed
    else:
        if db:
            db.fail_commit = "after"
            with pytest.raises(MonitoringCommitUncertain):
                reconcile_monitoring_work(controller, work)
            restarted = AzureSqlMonitoringStore(db=db, component="controller")
        else:
            original_reconcile = controller.reconcile_work

            def lose_ack_after_commit(claimed_work, **kwargs):
                res = original_reconcile(claimed_work, **kwargs)
                raise MonitoringCommitUncertain("reconciliation", res.request_id)

            monkeypatch.setattr(controller, "reconcile_work", lose_ack_after_commit)
            with pytest.raises(MonitoringCommitUncertain):
                reconcile_monitoring_work(controller, work)
            restarted = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
        snapshot_records = deepcopy(db.records if db else state.records)
        snapshot_receipts = deepcopy(db.receipts if db else state.receipts)
        outcome = reconcile_monitoring_work(restarted, work)
        assert (db.records if db else state.records) == snapshot_records
        assert (db.receipts if db else state.receipts) == snapshot_receipts
        target_work = work

    assert controller.get_work(current_version, target_work.work_id).state == "completed"
    receipts = db.receipts if db else state.receipts
    assert all(receipts[key] == value for key, value in before_receipts.items())
    if phase == "add":
        bound = fixture.connector()
        assert len(bound.sources) == 1 and bound.source_proposals == () and bound.source_removals == ()
        assert bound.state == "provisioning" and bound.delivery_proof is None
    elif phase == "remove":
        retired = fixture.connector()
        assert retired.sources == () and retired.source_proposals == () and retired.source_removals == ()
        tombstones = [row for row in fixture.rows() if row.kind == "connector_source_retirement"]
        assert len(tombstones) == 1
    else:
        assert outcome.state == "rejected"
        current = next(
            item for item in controller.list_connectors(
                m.PageQuery(**current_version.model_dump(exclude={"revision"})),
            ).items if item.connector_id == owned.connector_id
        )
        assert current.sources == owned.sources and current.source_removals == pending.pending_removals
        reobserve_id = stable_id(current_version, f"connector-reobserve:{target_work.reconcile_request_id}")
        assert controller.get_work(current_version, reobserve_id) is not None


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_remove_retains_ownership_across_overtaken_and_present_observations_until_verified_absence(
    backend, tmp_path,
):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    await fixture.execute(fixture.activate())
    planned = fixture.connector()
    proposal, = planned.source_proposals
    await fixture.apply_worker()
    await fixture.drain()
    active = fixture.connector()
    owned_source, = active.sources
    assert owned_source.source_id not in {proposal.proposal_id, proposal.node_name}

    exclude(fixture.use("web"), fixture.identity, nonce=94_501)
    await fixture.drain()
    pending = fixture.connector()
    removal, = pending.source_removals
    assert pending.sources == (owned_source,)
    assert fixture.use("controller").resolve_target(fixture.identity) is None
    assert not any(row.kind == "connector_source_retirement" for row in fixture.rows())
    removal_publication = fixture.use("controller").get_connector_publication(
        CONTEXT, removal.request_id,
    )

    worker = fixture.use("worker")
    first_present = worker.record_connector(
        fixture.version("worker"),
        m.OwnedConnectorManifest.model_validate({
            **pending.model_dump(),
            "revision": pending.revision + 1,
            "observed_definition": active.desired_definition,
            "state": "degraded",
            "gaps": (m.CoverageGap(
                code="source_still_present", detail="Remote source is still present before update.",
            ),),
            "updated_at": fixture.h.clock(),
        }),
        expected_connector_revision=pending.revision,
        commit=connector_commit(fixture.h, worker, CONNECTOR, db=fixture.db),
    )
    first_receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{pending.revision}")
    fixture.h.clock.advance(1)
    second_present = worker.record_connector(
        fixture.version("worker"),
        m.OwnedConnectorManifest.model_validate({
            **first_present.model_dump(),
            "revision": first_present.revision + 1,
            "updated_at": fixture.h.clock(),
        }),
        expected_connector_revision=first_present.revision,
        commit=connector_commit(fixture.h, worker, CONNECTOR, db=fixture.db),
    )
    second_receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{first_present.revision}")

    controller = fixture.use("controller")
    claimed = controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=uid(94_502), kinds=("reconcile_state",),
        limit=20, per_workspace_limit=20, lease_seconds=120,
    ))
    first_work = next(item for item in claimed if item.reconcile_request_id == first_receipt_id)
    second_work = next(item for item in claimed if item.reconcile_request_id == second_receipt_id)

    overtaken_outcome = reconcile_monitoring_work(controller, first_work)
    assert overtaken_outcome.state == ("pending_validation" if backend == "sql" else "rejected")
    reobserve_id = stable_id(CONTEXT, f"connector-reobserve:{first_receipt_id}")
    assert controller.get_work(CONTEXT, reobserve_id) is not None
    assert fixture.connector().sources == (owned_source,)
    assert fixture.connector().source_removals == (removal,)
    assert not any(row.kind == "connector_source_retirement" for row in fixture.rows())

    with pytest.raises(MonitoringConflict, match="observation|absence|present"):
        reconcile_monitoring_work(controller, second_work)
    assert fixture.connector().sources == (owned_source,)
    assert fixture.connector().source_removals == (removal,)
    assert fixture.connector().revision == second_present.revision
    assert not any(row.kind == "connector_source_retirement" for row in fixture.rows())
    assert controller.get_connector_publication(CONTEXT, removal.request_id) == removal_publication

    fixture.h.clock.advance(121)
    run = await fixture.apply_worker()
    assert run.results[0].code == "controller_source_retirement_required"
    assert fixture.remote.graph["sources"] == []
    controller = fixture.use("controller")
    due_work = controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=uid(94_503), kinds=("reconcile_state",),
        limit=20, per_workspace_limit=20, lease_seconds=120,
    ))
    for item in sorted(due_work, key=lambda candidate: candidate.work_id == first_work.work_id):
        reconcile_monitoring_work(controller, item)
    if backend == "sql":
        fixture.h.clock.advance(16)
        await fixture.drain()

    retired = fixture.connector()
    assert retired.sources == () and retired.source_proposals == () and retired.source_removals == ()
    tombstones = [
        m.ConnectorSourceRetirement.model_validate_json(row.payload)
        for row in fixture.rows() if row.kind == "connector_source_retirement"
    ]
    assert len(tombstones) == 1
    assert tombstones[0].original_binding == owned_source
    assert tombstones[0].original_removal == removal
    assert controller.get_connector_publication(CONTEXT, removal.request_id) == removal_publication


