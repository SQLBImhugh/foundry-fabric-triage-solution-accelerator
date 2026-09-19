from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
from uuid import UUID

import pytest
from test_monitoring_connector_supersession_store import recovery_case
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_event_readiness import native_delivery, receive
from test_monitoring_inventory import IDENTITY
from test_monitoring_provisioning import (
    CONNECTOR,
    CONTEXT,
    OWNER,
    connector,
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

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.monitoring.events import ConnectorBinding
from triage.monitoring.memory import InMemoryMonitoringStore, stable_id
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
