from __future__ import annotations

import asyncio
import base64
import copy
import json
import threading
from uuid import UUID

import httpx
import pytest
from test_monitoring_inventory import CONTEXT, IDENTITY, TENANT, WORKSPACE, Credential
from test_monitoring_polling import OWNER, make_store, target

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringCommitUncertain, MonitoringConflict
from triage.monitoring.inventory import RestRoute
from triage.monitoring.memory import InMemoryMonitoringStore, stable_id
from triage.monitoring.polling import FabricPipelinePollingClient
from triage.monitoring.provisioning import (
    SOURCE_EVENTS,
    ConnectorReconciler,
    ProvisioningRestClient,
    ProvisioningReview,
    binding_observation,
    decode_snapshot,
    encode_definition,
    matches_update,
    plan_definition,
    publication_sources,
)
from triage.monitoring.rate_limit import InMemoryRateBudget

CONNECTOR = str(UUID(int=30_001))
TRANSPORT_WORKSPACE = str(UUID(int=30_002))
EVENTSTREAM = str(UUID(int=30_003))
DESTINATION = str(UUID(int=30_004))
STREAM = str(UUID(int=30_005))
OPERATION = str(UUID(int=30_006))
WORK = str(UUID(int=30_007))
SECOND_ITEM = str(UUID(int=30_008))
PUBLISHED_EVENTS = (SOURCE_EVENTS[0], SOURCE_EVENTS[-1])


def test_verified_wire_aliases_do_not_duplicate_subscription_filters():
    assert SOURCE_EVENTS == (
        "Microsoft.Fabric.JobEvents.ItemJobCreated",
        "Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
        "Microsoft.Fabric.JobEvents.ItemJobSucceeded",
        "Microsoft.Fabric.JobEvents.ItemJobFailed",
    )


def wire_definition(graph):
    parts = {
        "eventstream.json": graph,
        "eventstreamProperties.json": {"retentionTimeInDays": 1, "eventThroughputLevel": "Low"},
        ".platform": {"metadata": {"displayName": "Owned monitoring transport"}},
    }
    return {
        "definition": {
            "format": "eventstream",
            "parts": [
                {
                    "path": name,
                    "payloadType": "InlineBase64",
                    "payload": base64.b64encode(json.dumps(value).encode()).decode(),
                }
                for name, value in parts.items()
            ],
        }
    }


def graph_for(targets=()):
    sources = []
    for index, identity in enumerate(targets):
        sources.append(
            {
                "id": str(UUID(int=31_000 + index)),
                "name": f"existing-owned-{index}",
                "type": "FabricJobEvents",
                "properties": {
                    "eventScope": "Item",
                    "workspaceId": identity.workspace_id,
                    "itemId": identity.item_id,
                    "includedEventTypes": list(SOURCE_EVENTS),
                },
            }
        )
    return {
        "compatibilityLevel": "1.1",
        "sources": sources,
        "operators": [],
        "streams": [
            {
                "id": STREAM,
                "name": "owned-stream",
                "type": "DefaultStream",
                "properties": {},
                "inputNodes": [{"name": source["name"]} for source in sources],
            }
        ],
        "destinations": [
            {
                "id": DESTINATION,
                "name": "owned-endpoint",
                "type": "CustomEndpoint",
                "properties": {"nonsecretMarker": "retain-exactly"},
                "inputNodes": [{"name": "owned-stream"}],
            }
        ],
    }


class Remote:
    def __init__(self, graph, targets):
        self.graph = copy.deepcopy(graph)
        self.targets = {item.item_id: item for item in targets}
        self.requests = []
        self.update_bodies = []
        self.pending = None
        self.async_update = False
        self.operation_state = "Running"
        self.timeout_after_apply = False
        self.timeout_before_apply = False
        self.on_update = None
        self.status = "Running"
        self.denied_items = set()
        self.throttled_items = set()
        self.change_destination_id = False
        self.definition_reads = 0
        self.next_source_id = 32_000

    def topology(self):
        value = copy.deepcopy(self.graph)
        for kind in ("sources", "streams", "destinations"):
            for node in value[kind]:
                node["status"] = self.status
        return value

    def apply(self, definition):
        parts = definition["definition"]["parts"]
        source = next(part for part in parts if part["path"] == "eventstream.json")
        graph = json.loads(base64.b64decode(source["payload"]))
        for node in graph["sources"]:
            if "id" not in node:
                node["id"] = str(UUID(int=self.next_source_id))
                self.next_source_id += 1
        if self.change_destination_id:
            graph["destinations"][0]["id"] = str(UUID(int=99_999))
        self.graph = graph

    def __call__(self, request):
        self.requests.append(request)
        path = request.url.path
        root = f"/v1/workspaces/{TRANSPORT_WORKSPACE}/eventstreams/{EVENTSTREAM}"
        if request.method == "GET" and path == root:
            return httpx.Response(
                200,
                json={
                    "id": EVENTSTREAM,
                    "workspaceId": TRANSPORT_WORKSPACE,
                    "type": "Eventstream",
                    "displayName": "Owned monitoring transport",
                },
            )
        if request.method == "POST" and path == root + "/getDefinition":
            self.definition_reads += 1
            return httpx.Response(200, json=wire_definition(self.graph))
        if request.method == "GET" and path == root + "/topology":
            return httpx.Response(200, json=self.topology())
        if request.method == "POST" and path == root + "/updateDefinition":
            assert request.url.params["updateMetadata"] == "false"
            payload = json.loads(request.content)
            self.update_bodies.append(payload)
            if self.on_update:
                self.on_update()
            if self.timeout_before_apply:
                raise httpx.ReadTimeout("DO_NOT_LOG_RESPONSE", request=request)
            if self.async_update:
                self.pending = payload
                return httpx.Response(
                    202,
                    headers={
                        "Location": f"https://api.fabric.microsoft.com/v1/operations/{OPERATION}",
                        "x-ms-operation-id": OPERATION,
                        "Retry-After": "1",
                    },
                )
            self.apply(payload)
            if self.timeout_after_apply:
                raise httpx.ReadTimeout("DO_NOT_LOG_RESPONSE", request=request)
            return httpx.Response(200)
        if request.method == "GET" and path == f"/v1/operations/{OPERATION}":
            if self.operation_state == "Succeeded" and self.pending is not None:
                self.apply(self.pending)
                self.pending = None
            return httpx.Response(
                200, headers={"Retry-After": "1"}, json={"status": self.operation_state}
            )
        if request.method == "GET":
            for identifier, identity in self.targets.items():
                item_path = f"/v1/workspaces/{identity.workspace_id}/items/{identifier}"
                if path.startswith(item_path):
                    if identifier in self.denied_items:
                        return httpx.Response(403, json={"message": "DO_NOT_LOG_RESPONSE"})
                    if identifier in self.throttled_items:
                        return httpx.Response(429, headers={"Retry-After": "60"})
                    if path == item_path:
                        return httpx.Response(
                            200,
                            json={
                                "id": identifier,
                                "workspaceId": identity.workspace_id,
                                "type": "DataPipeline",
                                "displayName": "Admitted pipeline",
                            },
                        )
                    if path == item_path + "/jobs/instances":
                        return httpx.Response(200, json={"value": []})
        raise AssertionError(f"Unexpected offline request: {request.method} {path}")


def version(store):
    control = store.snapshot(CONTEXT).control
    return m.RegistryVersion(**CONTEXT.model_dump(), revision=control.revision)


def setup_store(*, admitted=None, existing=None, metadata=True):
    admitted = tuple(admitted or (target(),))
    existing = (admitted[0],) if existing is None else existing
    store, state, clock = make_store(targets=admitted)
    remote = Remote(graph_for(existing), admitted)
    snapshot = decode_snapshot(wire_definition(remote.graph), remote.topology())
    desired_graph = copy.deepcopy(remote.graph)
    for node in desired_graph["sources"]:
        node["properties"]["includedEventTypes"] = list(PUBLISHED_EVENTS)
    desired = decode_snapshot(wire_definition(desired_graph), desired_graph)
    sources = tuple(
        m.ConnectorSource(
            source_id=node["id"],
            target=identity,
            event_types=PUBLISHED_EVENTS,
            event_source=TENANT,
        )
        for node, identity in zip(remote.graph["sources"], existing, strict=True)
    )
    manifest = m.OwnedConnectorManifest(
        **CONTEXT.model_dump(),
        connector_id=CONNECTOR,
        ownership_id=CONTEXT.epoch,
        revision=1,
        policy_revision=version(store).revision,
        name="Owned monitoring transport",
        workspace_id=TRANSPORT_WORKSPACE if metadata else None,
        eventstream_id=EVENTSTREAM if metadata else None,
        destination_id=DESTINATION if metadata else None,
        endpoint=m.EndpointMetadata(
            namespace="sample.servicebus.windows.net",
            entity="owned-events",
            consumer_group="$Default",
        )
        if metadata
        else None,
        sources=sources,
        desired_definition=desired,
        observed_definition=snapshot,
        state="provisioning",
        updated_at=clock(),
    )
    store.record_connector(version(store), manifest, expected_connector_revision=0)
    enqueue(store, clock)
    return store, state, clock, remote


def enqueue(store, clock, work_id=WORK, connector_id=CONNECTOR):
    return store.enqueue_work(
        m.MonitoringWorkDraft(
            **CONTEXT.model_dump(),
            work_id=work_id,
            connector_id=connector_id,
            kind="connector_reconcile",
            policy_revision=version(store).revision,
            created_at=clock(),
            due_at=clock(),
            reason="Reconcile current admitted sources",
        )
    )


def connector(store):
    return next(
        item
        for item in store.list_connectors(m.PageQuery(**CONTEXT.model_dump())).items
        if item.connector_id == CONNECTOR
    )


def exclude(store, identity, nonce=40_001):
    current = version(store)
    policy = store.list_scopes(m.PageQuery(**CONTEXT.model_dump())).items[0]
    scope = m.ScopeDefinition.model_validate(policy.model_dump(exclude={"revision", "updated_at"}))
    exclusion = m.ScopeRule(
        rule_id=str(UUID(int=nonce)),
        effect="exclude",
        workloads=("fabric_pipeline",),
        selector=m.ScopeSelector(
            tenant_id=TENANT,
            kind="item",
            workspace_id=identity.workspace_id,
            item_id=identity.item_id,
        ),
    )
    scope = scope.model_copy(update={"rules": (*scope.rules, exclusion)})
    plan = store.preview_scope(
        m.ScopePreviewRequest(
            expected=current,
            idempotency_id=str(UUID(int=nonce + 1)),
            scope=scope,
        )
    )
    store.activate_scope(
        m.ActivateScopeRequest(
            expected=current,
            idempotency_id=plan.idempotency_id,
            plan_id=plan.plan_id,
        )
    )


@pytest.fixture
async def factory():
    clients = []

    def build(store, clock, remote, **kwargs):
        rest = ProvisioningRestClient(
            CONTEXT,
            IDENTITY,
            Credential(clock),
            kwargs.pop("budget", InMemoryRateBudget(clock=clock)),
            transport=httpx.MockTransport(remote),
            clock=clock,
        )
        clients.append(rest)
        worker_store = InMemoryMonitoringStore(
            clock=clock, state=store._backend.state, component="worker",
        )
        return ConnectorReconciler(
            worker_store,
            CONTEXT,
            rest,
            FabricPipelinePollingClient(rest),
            OWNER,
            CONNECTOR,
            clock=clock,
            **kwargs,
        )

    yield build
    for client in clients:
        await client.close()


async def test_worker_applies_published_intent_without_event_or_delivery_proof(factory):
    store, _, clock, remote = setup_store()
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "definition_verified"
    assert len(remote.update_bodies) == 1
    saved = connector(store)
    assert len(saved.sources) == 1
    assert saved.sources[0].target == target()
    assert saved.state == "degraded"
    assert saved.delivery_verified_at is None
    assert saved.identity_verified_at is None
    assert saved.desired_definition == saved.observed_definition
    assert store.get_work(CONTEXT, WORK).state == "completed"
    assert remote.definition_reads == 2
    assert remote.graph["destinations"][0]["properties"] == {"nonsecretMarker": "retain-exactly"}


async def test_initial_empty_transport_requires_controller_published_baseline(factory):
    store, _, clock, remote = setup_store(existing=())
    prior = connector(store)
    store.record_connector(
        version(store),
        prior.model_copy(
            update={
                "revision": prior.revision + 1,
                "desired_definition": {},
                "observed_definition": None,
            }
        ),
        expected_connector_revision=prior.revision,
    )
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "controller_definition_baseline_required"
    assert not remote.update_bodies
    assert connector(store).delivery_verified_at is None


async def test_scope_revocation_before_post_cancels_only_the_unsent_intent(factory):
    store, _, clock, remote = setup_store()
    reconciler = factory(store, clock, remote)
    original = reconciler.store.record_connector
    desired = connector(store).desired_definition
    changed = False

    def revoke_after_intent(*args, **kwargs):
        nonlocal changed
        saved = original(*args, **kwargs)
        if not changed and saved.state == "provisioning":
            changed = True
            exclude(store, target())
        return saved

    reconciler.store.record_connector = revoke_after_intent
    result = await reconciler.run_once()
    assert result.results[0].code == "policy_changed_before_definition_update"
    assert not remote.update_bodies
    assert connector(store).state == "degraded"
    assert connector(store).desired_definition == desired
    assert connector(store).desired_definition != connector(store).observed_definition


async def test_unpublished_add_remove_never_changes_owned_ids_or_other_parts(factory):
    first, second = target(), target(item_id=SECOND_ITEM)
    store, _, clock, remote = setup_store(admitted=(first, second), existing=(first,))
    first_id = remote.graph["sources"][0]["id"]
    reconciler = factory(store, clock, remote)
    await reconciler.run_once()
    assert len(remote.graph["sources"]) == 1
    assert remote.graph["sources"][0]["id"] == first_id
    assert remote.graph["streams"][0]["id"] == STREAM
    assert remote.graph["destinations"][0]["id"] == DESTINATION
    assert all(
        any(part["path"] == ".platform" for part in request["definition"]["parts"])
        for request in remote.update_bodies
    )
    exclude(store, first)
    enqueue(store, clock, str(UUID(int=50_000)))
    await reconciler.run_once()
    assert [node["properties"]["itemId"] for node in remote.graph["sources"]] == [first.item_id]
    assert [source.source_id for source in connector(store).sources] == [first_id]
    assert remote.graph["streams"][0]["id"] == STREAM
    assert remote.graph["destinations"][0]["id"] == DESTINATION
    assert store.resolve_target(first) is None


async def test_scope_revocation_during_lro_retains_ownership_for_controller_cleanup(factory):
    first, second = target(), target(item_id=SECOND_ITEM)
    store, _, clock, remote = setup_store(admitted=(first, second), existing=(first, second))
    bindings = connector(store).sources
    remote.async_update = True
    remote.on_update = lambda: exclude(store, first)
    reconciler = factory(store, clock, remote)
    result = await reconciler.run_once()
    assert result.results[0].state == "waiting"
    assert connector(store).operation_id == OPERATION
    assert store.resolve_target(first) is None
    remote.operation_state = "Succeeded"
    clock.advance(31)
    result = await reconciler.run_once()
    assert result.results[0].code == "controller_scope_publication_required"
    assert connector(store).state == "degraded"
    assert connector(store).delivery_verified_at is None
    assert connector(store).gaps[0].code == "controller_scope_publication_required"
    remote.async_update = False
    remote.on_update = None
    enqueue(store, clock, str(UUID(int=50_001)))
    await reconciler.run_once()
    assert connector(store).sources == bindings
    assert len(remote.update_bodies) == 1


async def test_uncertain_update_restart_reconciles_definition_without_reposting(factory):
    store, _, clock, remote = setup_store()
    remote.timeout_after_apply = True
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "definition_update_outcome_unknown"
    assert connector(store).state == "provisioning"
    assert connector(store).operation_id is None
    clock.advance(31)
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "definition_verified"
    assert len(remote.update_bodies) == 1
    assert connector(store).sources


async def test_uncertain_unapplied_update_is_not_blindly_retried(factory):
    store, _, clock, remote = setup_store()
    bindings = connector(store).sources
    remote.timeout_before_apply = True
    await factory(store, clock, remote).run_once()
    clock.advance(31)
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "definition_update_outcome_unknown"
    assert len(remote.update_bodies) == 1
    assert connector(store).sources == bindings
    assert connector(store).desired_definition != connector(store).observed_definition


async def test_duplicate_work_does_not_repeat_an_already_verified_update(factory):
    store, _, clock, remote = setup_store()
    reconciler = factory(store, clock, remote)
    await reconciler.run_once()
    enqueue(store, clock, str(UUID(int=51_000)))
    await reconciler.run_once()
    assert len(remote.update_bodies) == 1


async def test_concurrent_distinct_work_cannot_replay_an_identical_intent_cas(factory):
    store, _, clock, remote = setup_store()
    enqueue(store, clock, str(UUID(int=51_001)))
    work = store.claim_work(
        m.WorkClaimRequest(
            **CONTEXT.model_dump(),
            owner_id=OWNER,
            kinds=("connector_reconcile",),
            limit=2,
            per_workspace_limit=2,
        )
    )
    assert len(work) == 2
    barrier = threading.Barrier(2)
    first = factory(store, clock, remote)
    second = factory(store, clock, remote)
    original = first.store.record_connector

    def synchronized(expected, manifest, **kwargs):
        if manifest.state == "provisioning":
            barrier.wait(timeout=5)
        return original(expected, manifest, **kwargs)

    first.store.record_connector = synchronized
    second.store.record_connector = synchronized
    results = await asyncio.gather(
        first._process(work[0]),
        second._process(work[1]),
        return_exceptions=True,
    )
    assert sum(isinstance(result, MonitoringConflict) for result in results) == 1
    assert len(remote.update_bodies) == 1


async def test_drift_is_preserved_and_blocked_instead_of_overwritten(factory):
    store, _, clock, remote = setup_store(existing=(target(),))
    remote.graph["destinations"][0]["properties"]["unowned-change"] = "keep"
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "owned_definition_drift_review"
    assert connector(store).state == "blocked"
    assert remote.graph["destinations"][0]["properties"]["unowned-change"] == "keep"
    assert not remote.update_bodies


async def test_unowned_source_or_destination_cannot_be_adopted(factory):
    store, _, clock, remote = setup_store()
    unknown = {
        "id": str(UUID(int=60_001)),
        "name": "external",
        "type": "CustomEndpoint",
        "properties": {},
    }
    remote.graph["sources"].append(unknown)
    remote.graph["streams"][0]["inputNodes"].append({"name": "external"})
    baseline = decode_snapshot(wire_definition(remote.graph), remote.topology())
    prior = connector(store)
    store.record_connector(
        version(store),
        prior.model_copy(
            update={
                "revision": prior.revision + 1,
                "observed_definition": baseline,
                "desired_definition": baseline,
                "sources": tuple(
                    source.model_copy(update={"event_types": SOURCE_EVENTS})
                    for source in prior.sources
                ),
            }
        ),
        expected_connector_revision=prior.revision,
    )
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "unowned_source_requires_review"
    assert not remote.update_bodies


async def test_server_component_replacement_is_not_success(factory):
    store, _, clock, remote = setup_store()
    remote.change_destination_id = True
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "updated_definition_or_component_id_mismatch"
    assert connector(store).state == "blocked"
    assert connector(store).delivery_verified_at is None


async def test_metadata_missing_shard_is_explicitly_blocked_without_any_http(factory):
    store, _, clock, remote = setup_store(metadata=False)
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "approved_nonsecret_connector_metadata_required"
    assert connector(store).state == "blocked"
    assert connector(store).eventstream_id is None
    assert connector(store).endpoint is None
    assert not remote.requests


async def test_current_mi_read_denial_blocks_connector_without_unfenced_capability_writes(factory):
    store, _, clock, remote = setup_store()
    remote.denied_items.add(target().item_id)
    await factory(store, clock, remote).run_once()
    assert connector(store).state == "blocked"
    assert connector(store).gaps[0].code == "source_read_denied_requires_controller_publication"
    assert not remote.update_bodies
    assert all(request.method in {"GET", "POST"} for request in remote.requests)
    assert not any("role" in request.url.path.lower() for request in remote.requests)


async def test_transient_probe_throttling_does_not_remove_existing_sources(factory):
    store, _, clock, remote = setup_store(existing=(target(),))
    before = connector(store).sources
    remote.throttled_items.add(target().item_id)
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].state == "waiting"
    assert connector(store).sources == before
    assert not remote.update_bodies


async def test_service_and_api_budget_admission_is_atomic_for_every_http(factory):
    store, _, clock, remote = setup_store()

    class AtomicOnlyBudget(InMemoryRateBudget):
        def __init__(self):
            super().__init__(clock=clock)
            self.calls = []

        def acquire(self, *args, **kwargs):
            raise AssertionError(
                "Do not consume a service allowance separately from its API allowance"
            )

        def acquire_many(self, context, policies):
            self.calls.append(policies)
            return super().acquire_many(context, policies)

    budget = AtomicOnlyBudget()
    await factory(store, clock, remote, budget=budget).run_once()
    assert len(budget.calls) == len(remote.requests)
    assert all(
        len(policies) == 2 and policies[0][0] == "service:fabric" for policies in budget.calls
    )


@pytest.mark.parametrize(
    "path",
    [
        "/workspaces/not-a-guid/eventstreams/x/updateDefinition",
        f"/workspaces/{TRANSPORT_WORKSPACE}/eventstreams/{EVENTSTREAM}/destinations/x/connection",
        f"/workspaces/{WORKSPACE}/items/{target().item_id}/jobs/instances",
        "https://example.invalid/webhook",
    ],
)
async def test_no_arbitrary_url_key_or_remediation_route(factory, path):
    store, _, clock, remote = setup_store()
    reconciler = factory(store, clock, remote)
    with pytest.raises((ProvisioningReview, ValueError)):
        await reconciler.rest._request(
            RestRoute("fabric", "fabric.eventstream.update", path, None),
            method="POST",
            update=True,
        )
    assert not remote.requests


async def test_manifest_commit_uncertainty_is_recovered_before_post(factory):
    store, _, clock, remote = setup_store()
    reconciler = factory(store, clock, remote)
    original = reconciler.store.record_connector
    failed = False

    def uncertain(*args, **kwargs):
        nonlocal failed
        result = original(*args, **kwargs)
        if not failed and result.state == "provisioning":
            failed = True
            raise MonitoringCommitUncertain(
                "connector", stable_id(CONTEXT, f"connector:{result.connector_id}:{result.revision - 1}")
            )
        return result

    reconciler.store.record_connector = uncertain
    result = await reconciler.run_once()
    assert result.results[0].code == "definition_verified"
    assert len(remote.update_bodies) == 1


def test_initial_source_plan_has_no_fabricated_physical_identity():
    store, _, _, remote = setup_store(existing=())
    baseline = decode_snapshot(wire_definition(remote.graph), remote.topology())
    plan = plan_definition(connector(store), baseline, (store.resolve_target(target()),))
    assert len(plan.new_names) == 1
    name = next(iter(plan.new_names))
    assert "id" not in plan.desired["parts"]["eventstream.json"]["sources"][0]
    assert f"sources/{name}" not in plan.desired["component_ids"]
    assert publication_sources(connector(store), plan) == ()
    assert len(plan.source_proposals) == 1
    proposal = plan.source_proposals[0]
    assert proposal.node_name == name and proposal.source_id is None
    assert proposal.target == target() and proposal.event_types == SOURCE_EVENTS
    assert proposal == plan_definition(
        connector(store), baseline, (store.resolve_target(target()),),
    ).source_proposals[0]
    assert not remote.update_bodies


def test_new_physical_source_binding_comes_only_from_exact_remote_roundtrip():
    store, _, _, remote = setup_store(existing=())
    prior = connector(store)
    plan = plan_definition(prior, prior.observed_definition, (store.resolve_target(target()),))
    name = next(iter(plan.new_names))
    remote.apply(encode_definition(plan.desired))
    observed = decode_snapshot(wire_definition(remote.graph), remote.topology())
    assert matches_update(observed, plan.desired)
    assert observed["component_ids"][f"sources/{name}"] == remote.graph["sources"][0]["id"]
    assert f"sources/{name}" not in plan.desired["component_ids"]
    changed = copy.deepcopy(observed)
    changed["component_ids"]["destinations/owned-endpoint"] = SECOND_ITEM
    assert not matches_update(changed, plan.desired)
    changed = copy.deepcopy(observed)
    changed["parts"]["eventstream.json"]["sources"][0]["properties"]["itemId"] = SECOND_ITEM
    assert not matches_update(changed, plan.desired)
    canonical = binding_observation(observed, plan.desired)
    assert canonical["parts"] == plan.desired["parts"]
    assert canonical["component_ids"] == observed["component_ids"]
    assert matches_update(observed, canonical)
    assert publication_sources(prior, plan) == ()


async def test_unbound_target_without_published_proposal_stays_blocked(factory):
    store, _, clock, remote = setup_store(existing=())
    prior = connector(store)
    plan = plan_definition(prior, prior.observed_definition, (store.resolve_target(target()),))
    store.record_connector(
        version(store),
        prior.model_copy(update={
            "revision": prior.revision + 1, "desired_definition": plan.desired,
        }),
        expected_connector_revision=prior.revision,
    )
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "published_source_intent_missing"
    assert connector(store).sources == ()
    assert not remote.update_bodies


@pytest.mark.parametrize("lose_observation_ack", [False, True])
async def test_worker_adds_published_proposal_without_assigning_ownership_or_readiness(
    factory, lose_observation_ack,
):
    store, _, clock, remote = setup_store(existing=())
    prior = connector(store)
    plan = plan_definition(prior, prior.desired_definition, (store.resolve_target(target()),))
    published = store.record_connector(
        version(store),
        prior.model_copy(update={
            "revision": prior.revision + 1, "desired_definition": plan.desired,
            "source_proposals": plan.source_proposals,
        }),
        expected_connector_revision=prior.revision,
    )
    reconciler = factory(store, clock, remote)
    if lose_observation_ack:
        original = reconciler.store.record_connector

        def uncertain(expected, manifest, **kwargs):
            saved = original(expected, manifest, **kwargs)
            if any(gap.code == "controller_source_binding_required" for gap in manifest.gaps):
                raise MonitoringCommitUncertain(
                    "connector", stable_id(CONTEXT, f"connector:{CONNECTOR}:{manifest.revision - 1}"),
                )
            return saved

        reconciler.store.record_connector = uncertain
    result = await reconciler.run_once()
    assert result.results[0].state == "waiting"
    assert result.results[0].code == "controller_source_binding_required"
    saved = connector(store)
    assert saved.sources == ()
    assert saved.source_proposals == published.source_proposals
    assert saved.desired_definition == plan.desired
    assert saved.observed_definition["parts"] == plan.desired["parts"]
    name = plan.source_proposals[0].node_name
    assert saved.observed_definition["component_ids"][f"sources/{name}"] == remote.graph["sources"][0]["id"]
    assert saved.delivery_verified_at is None and saved.identity_verified_at is None
    assert len(remote.update_bodies) == 1
    source_part = next(
        part for part in remote.update_bodies[0]["definition"]["parts"]
        if part["path"] == "eventstream.json"
    )
    assert "id" not in json.loads(base64.b64decode(source_part["payload"]))["sources"][0]
    enqueue(store, clock, str(UUID(int=65_000)))
    await factory(store, clock, remote).run_once()
    assert len(remote.update_bodies) == 1


async def test_new_proposal_scope_revocation_during_lro_keeps_original_ownership_evidence(factory):
    store, _, clock, remote = setup_store(existing=())
    prior = connector(store)
    plan = plan_definition(prior, prior.desired_definition, (store.resolve_target(target()),))
    store.record_connector(
        version(store), prior.model_copy(update={
            "revision": prior.revision + 1, "desired_definition": plan.desired,
            "source_proposals": plan.source_proposals,
        }),
        expected_connector_revision=prior.revision,
    )
    remote.async_update = True
    remote.on_update = lambda: exclude(store, target())
    await factory(store, clock, remote).run_once()
    assert connector(store).operation_id == OPERATION
    assert store.resolve_target(target()) is None
    remote.operation_state = "Succeeded"
    clock.advance(31)
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].state == "waiting"
    saved = connector(store)
    assert saved.sources == () and saved.source_proposals == plan.source_proposals
    assert saved.delivery_verified_at is None
    assert saved.observed_definition["component_ids"][
        f"sources/{plan.source_proposals[0].node_name}"
    ] == remote.graph["sources"][0]["id"]
    assert len(remote.update_bodies) == 1


async def test_completed_addition_lro_waiting_for_binding_is_not_reposted_or_lost_on_restart(factory):
    store, _, clock, remote = setup_store(existing=())
    prior = connector(store)
    plan = plan_definition(prior, prior.desired_definition, (store.resolve_target(target()),))
    store.record_connector(
        version(store), prior.model_copy(update={
            "revision": prior.revision + 1, "desired_definition": plan.desired,
            "source_proposals": plan.source_proposals,
        }),
        expected_connector_revision=prior.revision,
    )
    remote.async_update = True
    await factory(store, clock, remote).run_once()
    remote.operation_state = "Succeeded"
    clock.advance(31)
    await factory(store, clock, remote).run_once()
    enqueue(store, clock, str(UUID(int=65_001)))
    result = await factory(store, clock, remote).run_once()
    assert result.results[0].code == "controller_source_binding_required"
    assert connector(store).operation_id == OPERATION
    assert connector(store).source_proposals == plan.source_proposals
    assert len(remote.update_bodies) == 1


def test_existing_logical_proposal_is_preserved_by_typed_withdrawal_intent():
    store, _, _, _ = setup_store(existing=())
    prior = connector(store)
    first = plan_definition(prior, prior.desired_definition, (store.resolve_target(target()),))
    published = prior.model_copy(update={
        "desired_definition": first.desired, "source_proposals": first.source_proposals,
    })
    again = plan_definition(published, published.desired_definition, (store.resolve_target(target()),))
    assert again.source_proposals == first.source_proposals
    assert again.new_names == frozenset()
    removal = plan_definition(published, published.desired_definition, (), removal_targets=(target(),))
    assert publication_sources(published, removal) == ()
    assert removal.source_proposals == first.source_proposals
    assert len(removal.source_removals) == 1
    intent = removal.source_removals[0]
    assert intent.source_id is None
    assert intent.proposal_id == first.source_proposals[0].proposal_id
    assert set(intent.model_dump()) == {"removal_id", "source_id", "proposal_id", "detail"}


def test_removal_plan_cannot_release_owned_component_before_receipt_bound_publication():
    store, _, _, _ = setup_store()
    prior = connector(store)
    plan = plan_definition(prior, prior.desired_definition, (), removal_targets=(target(),))
    assert publication_sources(prior, plan) == prior.sources
    assert len(plan.source_removals) == 1
    assert plan.source_removals[0].source_id == prior.sources[0].source_id
    assert plan.source_removals[0].proposal_id is None
    assert plan.desired["parts"]["eventstream.json"]["sources"] == []


async def test_async_operation_does_not_claim_delivery_or_success_before_status(factory):
    store, _, clock, remote = setup_store()
    remote.async_update = True
    reconciler = factory(store, clock, remote)
    await reconciler.run_once()
    clock.advance(31)
    result = await reconciler.run_once()
    assert result.results[0].code == "definition_update_pending"
    assert connector(store).state == "provisioning"
    assert connector(store).delivery_verified_at is None
    assert store.get_work(CONTEXT, WORK).state == "waiting"


async def test_failed_update_is_reviewed_not_replayed_by_new_work(factory):
    store, _, clock, remote = setup_store()
    remote.async_update = True
    reconciler = factory(store, clock, remote)
    await reconciler.run_once()
    remote.operation_state = "Failed"
    clock.advance(31)
    await reconciler.run_once()
    enqueue(store, clock, str(UUID(int=63_000)))
    await reconciler.run_once()
    assert len(remote.update_bodies) == 1
    assert connector(store).state == "blocked"
    assert connector(store).operation_id == OPERATION


async def test_matching_definition_is_not_success_until_node_status_is_running(factory):
    store, _, clock, remote = setup_store()
    remote.status = "Creating"
    reconciler = factory(store, clock, remote)
    result = await reconciler.run_once()
    assert result.results[0].code == "published_topology_not_running"
    assert connector(store).state == "provisioning"
    remote.status = "Running"
    clock.advance(31)
    result = await reconciler.run_once()
    assert result.results[0].code == "definition_verified"
    assert len(remote.update_bodies) == 1
