from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import pytest
from test_monitoring_inventory import IDENTITY
from test_monitoring_provisioning import (
    CONNECTOR,
    CONTEXT,
    OWNER,
    SOURCE_EVENTS,
    connector,
    factory,
    setup_store,
    target,
    version,
)
from test_monitoring_provisioning_publication import current_result_fields
from test_monitoring_sql_review9_bindings import setup_sql
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.runner import TriageRunner
from triage.settings import Settings
from triage.store.incidents import InMemoryIncidentStore

__all__ = ["factory"]


def prior_publication(manifest, now):
    return m.ConnectorDesiredState(
        connector_id=manifest.connector_id, ownership_id=manifest.ownership_id,
        publication_id=uid(90_999), policy_revision=manifest.policy_revision,
        sources_hash=m._digest([source.model_dump(mode="json") for source in manifest.sources]),
        definition_hash=m._digest(manifest.desired_definition), published_at=now,
    )


def seed_existing_publication(seed, clock):
    # These orchestration cases start with a previously published transport,
    # not the separate proof-free registration/first-publication lifecycle.
    assert seed.component == "fixture"
    manifest = connector(seed)
    with seed._backend.transaction(write=True, operation="fixture_publication", request_id=uid(90_999)):
        seed._put("connector_desired", manifest.connector_id, CONTEXT, prior_publication(manifest, clock()))


async def drain_controller(runner, controller):
    results = []
    for _ in range(10):
        claimed = controller.claim_work(m.WorkClaimRequest(
            **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",),
            limit=1, per_workspace_limit=1,
        ))
        if not claimed:
            return results
        results.append(await runner.execute_monitoring_work(claimed[0]))
    raise AssertionError("Controller reconciliation did not reach its bounded idle state")


def runner_for(controller, tmp_path):
    settings = Settings(
        _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
        triage_provider_mode="mock", azure_sql_server="", azure_sql_database="",
        applicationinsights_connection_string="",
    )
    return TriageRunner(
        settings, base_dir=tmp_path, monitoring_store=controller,
        store=InMemoryIncidentStore(),
    )


@pytest.mark.parametrize("event_capability", ["verified", "unknown"])
async def test_runner_dispatch_plans_only_the_registered_transport_with_current_capability(
    factory, tmp_path, event_capability,
):
    seed, state, clock, remote = setup_store(existing=())
    seed_existing_publication(seed, clock)
    current = seed.resolve_target(target())
    seed.record_capability(
        version(seed), m.CapabilityObservation(
            capability_id=str(UUID(int=91_001)), target=target(),
            inventory_generation=current.inventory_generation, collector_identity_id=IDENTITY,
            read_status="verified", event_status=event_capability, action_status="unknown",
            checked_at=clock(), expires_at=clock() + timedelta(hours=1),
        ),
    )
    web = InMemoryMonitoringStore(state=state, clock=clock, component="web")
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    runner = runner_for(controller, tmp_path)
    web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=str(UUID(int=91_002)),
    )
    output = await drain_controller(runner, controller)
    assert output and all("deterministic reconciliation" in line for line in output)
    planned = connector(seed)
    assert planned.connector_id == CONNECTOR
    assert not remote.requests
    if event_capability == "unknown":
        assert planned.source_proposals == ()
        return
    assert len(planned.source_proposals) == 1
    assert planned.source_proposals[0].source_id is None
    assert planned.source_proposals[0].event_types == SOURCE_EVENTS
    await factory(seed, clock, remote).run_once()
    assert len(remote.update_bodies) == 1
    assert connector(seed).sources == ()
    await drain_controller(runner, controller)
    bound = connector(seed)
    assert bound.source_proposals == ()
    assert bound.sources[0].source_id == remote.graph["sources"][0]["id"]
    assert bound.delivery_verified_at is None and bound.identity_verified_at is None


async def test_runner_scope_change_worker_delete_and_controller_retirement_are_wired(factory, tmp_path):
    seed, state, clock, remote = setup_store()
    seed_existing_publication(seed, clock)
    await factory(seed, clock, remote).run_once()
    original = connector(seed).sources[0]
    web = InMemoryMonitoringStore(state=state, clock=clock, component="web")
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    runner = runner_for(controller, tmp_path)
    policy = web.list_scopes(m.PageQuery(**CONTEXT.model_dump())).items[0]
    scope = m.ScopeDefinition.model_validate(policy.model_dump(exclude={"revision", "updated_at"}))
    disabled = scope.model_copy(update={"rules": (*scope.rules, m.ScopeRule(
        rule_id=str(UUID(int=92_001)), effect="exclude", workloads=("fabric_pipeline",),
        selector=m.ScopeSelector(
            tenant_id=CONTEXT.tenant_id, kind="item",
            workspace_id=target().workspace_id, item_id=target().item_id,
        ),
    ))})
    preview = web.preview_scope(m.ScopePreviewRequest(
        expected=version(seed), idempotency_id=str(UUID(int=92_002)), scope=disabled,
    ))
    web.activate_scope(m.ActivateScopeRequest(
        expected=version(seed), idempotency_id=preview.idempotency_id, plan_id=preview.plan_id,
    ))
    await drain_controller(runner, controller)
    pending = connector(seed)
    assert controller.resolve_target(target()) is None
    assert pending.sources == (original,)
    assert len(pending.source_removals) == 1
    assert pending.desired_definition["parts"]["eventstream.json"]["sources"] == []
    await factory(seed, clock, remote).run_once()
    assert connector(seed).sources == (original,)
    await drain_controller(runner, controller)
    retired = connector(seed)
    assert retired.sources == () and retired.source_removals == ()
    key = f"{CONNECTOR}:removal:{pending.source_removals[0].removal_id}"
    row = seed._backend.get("connector_source_retirement", key, CONTEXT)
    tombstone = m.ConnectorSourceRetirement.model_validate_json(row.payload)
    assert tombstone.original_binding == original
    assert len(remote.update_bodies) == 2


def test_sql_reconciliation_routes_registered_transport_to_guarded_publication():
    h, db, controller, work, _ = setup_sql()
    current_result_fields(db)
    registered = m.OwnedConnectorManifest(
        **h.context(), connector_id=uid(99_001), ownership_id=uid(99_002),
        revision=1, policy_revision=h.version.revision, name="Registered transport fixture",
        workspace_id=uid(99_003), eventstream_id=uid(99_004), destination_id=uid(99_005),
        endpoint=m.EndpointMetadata(
            namespace="fixture.servicebus.windows.net", entity="fixture", consumer_group="$Default",
        ),
        sources=(), desired_definition={
            "parts": {"eventstream.json": {
                "compatibilityLevel": "1.1", "sources": [], "operators": [],
                "streams": [{"name": "owned-stream", "type": "DefaultStream", "inputNodes": []}],
                "destinations": [{
                    "name": "owned-endpoint", "type": "CustomEndpoint",
                    "inputNodes": [{"name": "owned-stream"}],
                }],
            }},
            "component_ids": {
                "streams/owned-stream": uid(99_006), "destinations/owned-endpoint": uid(99_005),
            },
        },
        state="planned", updated_at=h.clock(),
    )
    db.native_put(
        "connector", registered.connector_id, registered.model_dump(mode="json"), status="planned",
    )
    db.native_put(
        "connector_desired", registered.connector_id,
        prior_publication(registered, h.clock()).model_dump(mode="json"),
    )
    result = controller.reconcile_work(work)
    assert result.state == "published"
    current = next(
        item for item in controller.list_connectors(m.PageQuery(**h.context())).items
        if item.connector_id == registered.connector_id
    )
    assert current.sources == ()
    assert len(current.source_proposals) == 1
    assert current.source_proposals[0].target == h.targets[0]
    assert current.source_proposals[0].event_types == SOURCE_EVENTS
    assert current.source_proposals[0].source_id is None
    assert current.eventstream_id == registered.eventstream_id
    assert current.endpoint == registered.endpoint
