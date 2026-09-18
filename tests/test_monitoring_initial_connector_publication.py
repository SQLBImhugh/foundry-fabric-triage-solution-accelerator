from __future__ import annotations

import json

import httpx
import pytest
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_event_readiness import native_delivery, receive
from test_monitoring_inventory import CONTEXT, IDENTITY, Credential
from test_monitoring_polling import collector
from test_monitoring_provisioning import (
    CONNECTOR,
    DESTINATION,
    EVENTSTREAM,
    SOURCE_EVENTS,
    TRANSPORT_WORKSPACE,
    Remote,
    exclude,
    graph_for,
    wire_definition,
)
from test_monitoring_sql_removals import _adapt

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringConflict
from triage.monitoring.events import ConnectorBinding
from triage.monitoring.provisioning import (
    OwnedEventCapabilityProbe,
    ProvisioningRestClient,
    decode_snapshot,
    plan_definition,
    publication_from_plan,
)
from triage.monitoring.rate_limit import InMemoryRateBudget
from triage.monitoring.sql_kernel_connectors import initial_publication_invalid_sql


def register_physical_fixture(fixture):
    """Deployer-only metadata fixture; no desired row, admission or proof is seeded."""
    fixture.remote = Remote(graph_for((fixture.identity,)), (fixture.identity,))
    baseline = decode_snapshot(wire_definition(fixture.remote.graph), fixture.remote.topology())
    expected = fixture.version("controller")
    manifest = m.OwnedConnectorManifest(
        **CONTEXT.model_dump(), connector_id=CONNECTOR, ownership_id=CONTEXT.epoch,
        revision=1, policy_revision=expected.revision, name="Registered owned physical source",
        workspace_id=TRANSPORT_WORKSPACE, eventstream_id=EVENTSTREAM, destination_id=DESTINATION,
        endpoint=m.EndpointMetadata(
            namespace="sample.servicebus.windows.net", entity="owned-events", consumer_group="$Default",
        ),
        sources=(m.ConnectorSource(
            source_id=fixture.remote.graph["sources"][0]["id"], target=fixture.identity,
            event_types=SOURCE_EVENTS, event_source=CONTEXT.tenant_id,
        ),),
        desired_definition=baseline, observed_definition=baseline, state="planned", updated_at=fixture.h.clock(),
        gaps=(m.CoverageGap(
            code="gated_until_controller_publication", detail="Registered physical ownership only",
        ),),
    )
    if fixture.db:
        fixture.db.native_put("connector", CONNECTOR, manifest.model_dump(mode="json"), status="planned")
    else:
        fixture.h.store.record_connector(expected, manifest, expected_connector_revision=0)
    assert fixture.use("controller").get_connector_desired(CONTEXT, CONNECTOR) is None
    return manifest


def binding(manifest):
    return ConnectorBinding(
        tenant_id=manifest.tenant_id, connector_id=manifest.connector_id, workspace_id=manifest.workspace_id,
        eventstream_id=manifest.eventstream_id, destination_id=manifest.destination_id, endpoint=manifest.endpoint,
    )


async def fresh_event_capability(fixture):
    fixture.h.clock.advance(1)
    version = fixture.version("controller")
    controller = fixture.use("controller")
    draft = controller.enqueue_work(m.MonitoringWorkDraft(
        **CONTEXT.model_dump(), work_id=fixture.h.next_id(), kind="capability_probe",
        target=fixture.identity, policy_revision=version.revision,
        due_at=fixture.h.clock(), created_at=fixture.h.clock(),
        reason="Fresh same-identity source and registered event-path capability",
    ))
    claimed, = fixture.claim("worker", "capability_probe")
    assert claimed.work_id == draft.work_id
    worker = fixture.use("worker")
    rest = ProvisioningRestClient(
        CONTEXT, IDENTITY, Credential(fixture.h.clock), InMemoryRateBudget(clock=fixture.h.clock),
        transport=httpx.MockTransport(fixture.remote), clock=fixture.h.clock,
    )
    try:
        service = collector(
            worker, rest, fixture.h.clock,
            event_probe=OwnedEventCapabilityProbe(
                worker, CONTEXT, rest, binding(fixture.connector("worker")), clock=fixture.h.clock,
            ),
        )
        result = await service._capability(claimed)
        assert result.state == "recorded"
    finally:
        await rest.close()
    return claimed


def original_publication(fixture):
    if fixture.db:
        candidates = [
            (request_id, receipt["payload"]["result"])
            for (operation, request_id), receipt in fixture.db.receipts.items()
            if operation == "controller.publish_connector"
            and receipt["payload"]["result"]["connector_id"] == CONNECTOR
        ]
    else:
        candidates = [
            (receipt.request_id, json.loads(receipt.payload))
            for receipt in fixture.h.state.receipts.values()
            if receipt.operation == "connector_publication"
            and json.loads(receipt.payload)["connector_id"] == CONNECTOR
        ]
    request_id, result = min(candidates, key=lambda entry: entry[1]["connector"]["revision"])
    return request_id, m.ConnectorPublicationResult.model_validate(result)


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("event_verified", [False, True])
async def test_registered_source_stays_dormant_without_scope_then_first_admission_publishes(
    backend, event_verified, tmp_path,
):
    fixture = PublicationHarness(backend, tmp_path, register_transport=False)
    await fixture.collect(event_status="unknown")
    registered = register_physical_fixture(fixture)
    if event_verified:
        await fresh_event_capability(fixture)
    else:
        fixture.use("web").request_discovery(
            fixture.version("web"), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
            request_id=fixture.h.next_id(),
        )
    await fixture.drain()
    assert fixture.connector() == registered
    assert fixture.use("controller").get_connector_desired(CONTEXT, CONNECTOR) is None
    assert fixture.use("controller").list_scopes(m.PageQuery(**CONTEXT.model_dump())).items == ()
    assert not any(row.kind == "connector_publication" for row in fixture.rows())
    assert not any(row.kind == "work" and row.work_kind == "connector_reconcile" for row in fixture.rows())
    assert fixture.remote.update_bodies == []

    await fixture.execute(fixture.activate())
    if not event_verified:
        assert fixture.use("controller").get_connector_desired(CONTEXT, CONNECTOR) is None
        assert fixture.connector() == registered
        await fresh_event_capability(fixture)
        await fixture.drain()
    desired = fixture.use("controller").get_connector_desired(CONTEXT, CONNECTOR)
    assert desired is not None and desired.policy_revision == fixture.version("controller").revision
    first_id, first = original_publication(fixture)
    assert first.desired_changed and first.state == "provisioning"
    assert first.connector.sources == registered.sources
    assert first.connector.desired_definition == registered.desired_definition
    assert first.pending_removals == () and first.retired_sources == ()
    assert first.connector.identity_verified_at is None and first.connector.delivery_proof is None
    assert fixture.use("controller").get_connector_publication(CONTEXT, first_id) == first
    assert fixture.remote.update_bodies == []


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_same_policy_registration_publishes_once_then_receives_and_retires_normally(backend, tmp_path):
    fixture = PublicationHarness(backend, tmp_path, register_transport=False)
    await fixture.collect(event_status="unknown")
    await fixture.execute(fixture.activate())
    admitted = fixture.use("controller").resolve_target(fixture.identity)
    assert admitted is not None and admitted.observation.enabled and not admitted.action.enabled
    registered = register_physical_fixture(fixture)
    before_revision = fixture.version("controller").revision
    await fresh_event_capability(fixture)
    assert fixture.use("controller").get_connector_desired(CONTEXT, CONNECTOR) is None
    await fixture.drain()
    first_id, first = original_publication(fixture)
    assert fixture.version("controller").revision == before_revision == registered.policy_revision
    assert first.desired_changed and first.state == "provisioning"
    assert first.connector.sources == registered.sources and first.connector.desired_definition == registered.desired_definition
    assert first.connector.identity_verified_at is None and first.connector.delivery_verified_at is None
    assert first.connector.delivery_proof is None
    desired = fixture.use("controller").get_connector_desired(CONTEXT, CONNECTOR)
    assert desired is not None

    await fresh_event_capability(fixture)
    await fixture.drain()
    assert fixture.use("controller").get_connector_desired(CONTEXT, CONNECTOR) == desired
    assert original_publication(fixture) == (first_id, first)
    await fixture.apply_worker()
    await fixture.drain()
    assert fixture.connector().state == "degraded"
    assert fixture.remote.update_bodies == []
    fixture.h.clock.advance(1)
    worker = fixture.use("worker")
    checkpoints = await receive(
        worker, fixture.h.clock, binding(fixture.connector("worker")),
        (native_delivery(fixture.h.clock, fixture.identity),), delivery_delay_seconds=3,
    )
    assert checkpoints.accepted_positions == 1
    assert worker.get_connector_delivery(CONTEXT, CONNECTOR, IDENTITY) is not None
    assert fixture.connector("worker").delivery_proof is None
    await fixture.drain()
    await fixture.apply_worker()
    assert fixture.connector("worker").state == "provisioning"
    await fixture.drain()
    assert fixture.connector().state == "ready"
    assert fixture.connector().delivery_proof is not None
    assert fixture.connector().identity_verified_at < fixture.connector().delivery_verified_at
    assert not fixture.use("controller").resolve_target(fixture.identity).action.enabled

    exclude(fixture.use("web"), fixture.identity, nonce=91_800)
    await fixture.drain()
    revoked = fixture.connector()
    assert len(revoked.source_removals) == 1 and revoked.sources == registered.sources
    assert revoked.delivery_proof is None and revoked.state != "ready"
    assert fixture.use("controller").resolve_target(fixture.identity) is None
    await fixture.apply_worker()
    await fixture.drain()
    assert fixture.remote.graph["sources"] == []
    assert fixture.connector().sources == () and fixture.connector().source_removals == ()
    assert fixture.use("controller").get_connector_publication(CONTEXT, first_id) == first
    assert not any(row.kind in {"action", "action_owner"} for row in fixture.rows())


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_direct_initial_removal_cannot_bypass_dormant_orchestration(backend, tmp_path):
    fixture = PublicationHarness(backend, tmp_path, register_transport=False)
    await fixture.collect(event_status="unknown")
    registered = register_physical_fixture(fixture)
    fixture.use("web").request_discovery(
        fixture.version("web"), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=fixture.h.next_id(),
    )
    work, = fixture.claim("controller", "reconcile_state")
    controller = fixture.use("controller")
    producer = controller.get_reconciliation_request(CONTEXT, work.reconcile_request_id, producer="web")
    frontier = controller.get_validation_frontier(CONTEXT, producer.frontier_key)
    request = publication_from_plan(
        fixture.version("controller"), work, frontier, registered,
        plan_definition(registered, registered.desired_definition, ()), request_id=fixture.h.next_id(),
    )
    with pytest.raises(MonitoringConflict, match="Initial connector publication"):
        controller.publish_connector(request)
    assert fixture.connector() == registered
    assert controller.get_connector_desired(CONTEXT, CONNECTOR) is None
    assert not any(row.kind == "connector_publication" for row in fixture.rows())
    if fixture.db:
        sql = "SELECT CASE WHEN " + _adapt(fixture.db, initial_publication_invalid_sql(fixture.db.names)) + " THEN 1 ELSE 0 END"
        assert fixture.db.guards.execute(sql, {
            "prior": registered.model_dump_json(), "desired": None, "plan": request.model_dump_json(),
        }).fetchone()[0] == 1
        assert fixture.db.guards.execute(sql, {
            "prior": registered.model_dump_json(), "desired": '{"published_at":"current"}',
            "plan": request.model_dump_json(),
        }).fetchone()[0] == 0
