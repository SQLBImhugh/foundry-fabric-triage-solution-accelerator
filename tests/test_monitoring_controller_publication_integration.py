from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from copy import deepcopy
from datetime import UTC, timedelta

import httpx
import pytest
from test_monitoring_inventory import CONTEXT, IDENTITY, Credential
from test_monitoring_provisioning import (
    CONNECTOR,
    DESTINATION,
    EVENTSTREAM,
    TRANSPORT_WORKSPACE,
    Remote,
    exclude,
    graph_for,
    target,
    wire_definition,
)
from test_monitoring_sql_receiver_bindings import ReceiverAbiDatabase, record_delivery_evidence
from test_monitoring_sql_review9_bindings import connector_commit
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring import provisioning
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
)
from triage.monitoring.controller import publish_reconciliation_connector, reconcile_monitoring_work
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.polling import FabricPipelinePollingClient
from triage.monitoring.provisioning import (
    ConnectorReconciler,
    ProvisioningRestClient,
    decode_snapshot,
)
from triage.monitoring.rate_limit import InMemoryRateBudget
from triage.monitoring.sql_kernel_contracts import KERNEL_VERSION
from triage.monitoring.sql_store import AzureSqlMonitoringStore
from triage.runner import TriageRunner
from triage.settings import Settings
from triage.store.incidents import InMemoryIncidentStore


@pytest.fixture
def publication_calls(monkeypatch):
    calls = []
    for name in ("prepare_connector_publication", "prepare_connector_binding", "publish_connector_intent"):
        original = getattr(provisioning, name)

        def invoke(store, value, *args, operation=name, implementation=original, **kwargs):
            work_id = value.work_id
            current = store.get_work(CONTEXT, work_id)
            assert store.component == "controller" and store._backend.transaction_active
            assert current.kind == "reconcile_state" and current.state == "leased" and current.lease is not None
            assert current.lease == value.lease
            calls.append((operation, work_id, current.lease.fence))
            return implementation(store, value, *args, **kwargs)

        monkeypatch.setattr(provisioning, name, invoke)
    return calls


class PublicationHarness:
    def __init__(self, backend, tmp_path, *, register_transport=True):
        self.h = Harness()
        assert self.h.context() == CONTEXT.model_dump()
        assert self.h.state.records == {}
        self.identity = target()
        self.remote = Remote(graph_for(()), (self.identity,))
        definition = decode_snapshot(wire_definition(self.remote.graph), self.remote.topology())
        # Explicit deployer-fixture bootstrap records the observed empty baseline
        # and endpoint, never a runtime-created binding or pre-admitted target.
        if register_transport:
            self.h.store.record_connector(self.h.version, m.OwnedConnectorManifest(
                **CONTEXT.model_dump(), connector_id=CONNECTOR, ownership_id=CONTEXT.epoch,
                revision=1, policy_revision=0, name="Registered source-empty transport",
                workspace_id=TRANSPORT_WORKSPACE, eventstream_id=EVENTSTREAM, destination_id=DESTINATION,
                endpoint=m.EndpointMetadata(
                    namespace="fixture.servicebus.windows.net", entity="owned-events", consumer_group="$Default",
                ),
                sources=(), desired_definition=definition, observed_definition=definition,
                state="planned", updated_at=self.h.clock(),
            ), expected_connector_revision=0)
        self.db = ProvisioningSqlDatabase(self.h) if backend == "sql" else None
        self.stores = {
            component: AzureSqlMonitoringStore(db=self.db, component=component) if self.db else InMemoryMonitoringStore(
                state=self.h.state, clock=self.h.clock, component=component,
            )
            for component in ("web", "worker", "controller")
        }
        settings = Settings(
            _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock", triage_provider_mode="mock",
            azure_sql_server="", azure_sql_database="", applicationinsights_connection_string="",
        )
        self.runner = TriageRunner(
            settings, base_dir=tmp_path, monitoring_store=self.stores["controller"], store=InMemoryIncidentStore(),
        )

    def use(self, component):
        if self.db:
            self.db.principal = component
        return self.stores[component]

    def version(self, component):
        control = self.use(component).snapshot(CONTEXT).control
        return m.RegistryVersion(**CONTEXT.model_dump(), revision=control.revision)

    def connector(self, component="controller"):
        return next(
            value for value in self.use(component).list_connectors(m.PageQuery(**CONTEXT.model_dump())).items
            if value.connector_id == CONNECTOR
        )

    def claim(self, component, kind):
        return self.use(component).claim_work(m.WorkClaimRequest(
            **CONTEXT.model_dump(), owner_id=uid(90_001), kinds=(kind,), limit=1, per_workspace_limit=1,
            lease_seconds=120,
        ))

    async def execute(self, work):
        self.use("controller")
        return await self.runner.execute_monitoring_work(work)

    async def drain(self):
        results = []
        for _ in range(20):
            claimed = self.claim("controller", "reconcile_state")
            if not claimed:
                return results
            results.append(await self.execute(claimed[0]))
        raise AssertionError("Deterministic controller work did not reach bounded idle state")

    def complete_collection(self, work):
        store = self.use("worker")
        current = store.get_work(CONTEXT, work.work_id)
        return store.complete_collection_work(
            CONTEXT, work_id=current.work_id, lease=current.lease, expected_work_revision=current.revision,
        )

    async def collect(self, *, event_status="verified"):
        web = self.use("web")
        web.request_discovery(
            self.version("web"), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
            request_id=self.h.next_id(),
        )
        await self.drain()
        work, = self.claim("worker", "inventory")
        generation = m.InventoryGeneration(
            **CONTEXT.model_dump(), generation_id=work.work_id, selector=work.discovery_selector,
            adapter="explicit_offline_inventory", authority="tenant_admin", completeness="complete",
            started_at=self.h.clock(), completed_at=self.h.clock(), discovered_count=1, completed_pages=1,
        )
        self.use("worker").record_inventory(m.InventoryBatch(
            request_id=self.h.next_id(), expected=self.version("worker"), generation=generation,
            items=(m.InventoryItem(
                **CONTEXT.model_dump(), generation_id=generation.generation_id,
                workspace_id=self.identity.workspace_id, item_id=self.identity.item_id,
                name="New synthetic pipeline", item_type="DataPipeline", workload="fabric_pipeline",
                observed_at=self.h.clock(),
            ),),
            commit=m.InventoryCommit(
                work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
                expected_generation_revision=0,
            ),
        ))
        self.complete_collection(work)
        assert self.use("controller").list_targets(m.TargetQuery(**CONTEXT.model_dump())).items == ()
        await self.drain()
        probe, = self.claim("worker", "capability_probe")
        self.use("worker").record_capability(
            self.version("worker"),
            m.CapabilityObservation(
                capability_id=self.h.next_id(), target=self.identity, inventory_generation=generation.generation_id,
                collector_identity_id=IDENTITY, read_status="verified", event_status=event_status,
                checked_at=self.h.clock(), expires_at=self.h.clock() + timedelta(hours=1),
            ),
            commit=m.CollectionCommit(
                work_id=probe.work_id, lease=probe.lease, expected_work_revision=probe.revision,
            ),
        )
        self.complete_collection(probe)
        await self.drain()
        assert self.use("controller").list_targets(m.TargetQuery(**CONTEXT.model_dump())).items == ()
        assert all(
            value.source_proposals == ()
            for value in self.use("controller").list_connectors(m.PageQuery(**CONTEXT.model_dump())).items
        )

    def activate(self):
        web = self.use("web")
        preview = web.preview_scope(m.ScopePreviewRequest(
            expected=self.version("web"), idempotency_id=self.h.next_id(),
            scope=m.ScopeDefinition(
                **CONTEXT.model_dump(), scope_id=self.h.next_id(), name="Explicit pipeline observation scope",
                rules=(m.ScopeRule(
                    rule_id=self.h.next_id(), selector=m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
                    effect="include", workloads=("fabric_pipeline",),
                ),),
            ),
        ))
        receipt = web.activate_scope(m.ActivateScopeRequest(
            expected=preview.expected, plan_id=preview.plan_id, idempotency_id=preview.idempotency_id,
        ))
        assert receipt.state == "configuring"
        work, = self.claim("controller", "reconcile_state")
        return work

    async def apply_worker(self):
        worker = self.use("worker")
        rest = ProvisioningRestClient(
            CONTEXT, IDENTITY, Credential(self.h.clock), InMemoryRateBudget(clock=self.h.clock),
            transport=httpx.MockTransport(self.remote), clock=self.h.clock,
        )
        try:
            return await ConnectorReconciler(
                worker, CONTEXT, rest, FabricPipelinePollingClient(rest), uid(90_002), CONNECTOR,
                batch_size=1, clock=self.h.clock,
            ).run_once()
        finally:
            await rest.close()

    def rows(self):
        return tuple((self.db.records if self.db else self.h.state.records).values())


class ProvisioningSqlDatabase(ReceiverAbiDatabase):
    """Model native worker renewal/retry without relaxing its completion receipt gate."""

    def query(self, sql, *params):
        assert not (self.principal == "controller" and "receipts_worker" in sql), (
            "Controller callers must use guarded original-receipt publication, not worker-private receipt views"
        )
        return super().query(sql, *params)

    def apply_rpc(self, operation, args):
        if operation != "worker.transition_work" or args["transition"] not in {"renew", "retry", "disposition"}:
            return super().apply_rpc(operation, args)
        prior = self.receipts.get((operation, args["request_id"]))
        if prior is not None:
            if prior["fingerprint"] != args["fingerprint"]:
                raise RuntimeError("Original work-transition fingerprint differs (51072)")
            return {
                "kernel_version": KERNEL_VERSION, "operation": operation, "status": "replayed",
                "affected_rows": 0, "result": prior["payload"]["result"],
            }
        work = self.model("work", args["work_id"], m.MonitoringWork)
        if (
            work is None or work.kind not in m.WORKER_WORK_KINDS or work.lease is None
            or work.lease.owner_id != args["owner_id"] or work.lease.fence != args["fence"]
            or work.lease.expires_at <= self.clock() or work.revision != args["work_revision"]
        ):
            raise RuntimeError("Worker transition lost its original work ownership (51074)")
        if args["transition"] == "renew":
            if not 1 <= args["lease_seconds"] <= 86400:
                raise RuntimeError("Worker renewal lifetime is outside the native bound (51073)")
            changes = {
                "lease": {**work.lease.model_dump(), "expires_at": self.clock() + timedelta(seconds=args["lease_seconds"])},
            }
        elif args["transition"] == "retry":
            if args["retry_at"] is None or args["retry_at"].replace(tzinfo=UTC) <= self.clock():
                raise RuntimeError("Worker retry requires its bounded future due time (51073)")
            changes = {"lease": None, "state": "waiting", "due_at": args["retry_at"].replace(tzinfo=UTC)}
        else:
            if not args["detail"] or work.action_reservation_id is not None:
                raise RuntimeError("Worker disposition needs a reason and cannot abandon an action (51073)")
            changes = {
                "lease": None, "state": "dispositioned", "disposition": args["detail"],
                "completed_at": self.clock(),
            }
        updated = m.MonitoringWork.model_validate({**work.model_dump(), "revision": work.revision + 1, **changes})
        self.save_work(updated)
        return self.reply(operation, args, {"work_id": updated.work_id, "work": updated.model_dump(mode="json")})


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_empty_registry_runs_real_controller_store_and_worker_to_receipt_bound_source(
    backend, tmp_path, publication_calls,
):
    fixture = PublicationHarness(backend, tmp_path)
    assert fixture.use("controller").list_scopes(m.PageQuery(**CONTEXT.model_dump())).items == ()
    assert fixture.use("controller").list_targets(m.TargetQuery(**CONTEXT.model_dump())).items == ()
    await fixture.collect()
    scope_work = fixture.activate()
    assert scope_work.target is None and scope_work.execution is None and scope_work.action_reservation_id is None
    assert fixture.use("controller").list_targets(m.TargetQuery(**CONTEXT.model_dump())).items == ()
    result = await fixture.execute(scope_work)
    assert "deterministic reconciliation published" in result
    planned = fixture.connector()
    assert planned.sources == () and len(planned.source_proposals) == 1
    proposal = planned.source_proposals[0]
    assert proposal.source_id is None and proposal.target == fixture.identity
    assert fixture.remote.update_bodies == []
    assert ("prepare_connector_publication", scope_work.work_id, scope_work.lease.fence) in publication_calls
    assert ("publish_connector_intent", scope_work.work_id, scope_work.lease.fence) in publication_calls
    before_worker = tuple(publication_calls)
    run = await fixture.apply_worker()
    assert tuple(publication_calls) == before_worker
    assert len(fixture.remote.update_bodies) == 1, run
    assert run.results[0].code == "controller_source_binding_required"
    collection = fixture.use("worker").get_work(CONTEXT, run.results[0].work_id)
    assert collection.state == "completed" and collection.lease is None
    if fixture.db:
        assert not any(
            operation == "worker.accept_facts" and receipt["payload"]["result"]["work_id"] == collection.work_id
            for (operation, _), receipt in fixture.db.receipts.items()
        )
        observations = [
            receipt["payload"]["result"] for (operation, _), receipt in fixture.db.receipts.items()
            if operation == "worker.observe_connector" and receipt["payload"]["result"]["work_id"] == collection.work_id
        ]
        assert any(value["collection_completion_eligible"] is True for value in observations)
        assert any(value["collection_completion_eligible"] is False for value in observations)
    assert fixture.connector().sources == () and fixture.connector().source_proposals == (proposal,)
    await fixture.drain()
    bound = fixture.connector()
    assert bound.source_proposals == () and len(bound.sources) == 1
    assert bound.sources[0].target == fixture.identity
    assert bound.sources[0].source_id == fixture.remote.graph["sources"][0]["id"]
    assert bound.sources[0].source_id not in {proposal.proposal_id, proposal.node_name}
    assert bound.state != "ready" and bound.identity_verified_at is None and bound.delivery_verified_at is None
    assert any(operation == "prepare_connector_binding" for operation, _, _ in publication_calls)
    assert not any(row.kind in {"action", "action_owner", "incident_state"} for row in fixture.rows())
    assert fixture.use("controller").get_work(CONTEXT, scope_work.work_id).state == "completed"
    fixture.h.clock.advance(1)
    worker = fixture.use("worker")
    bound, proof = record_delivery_evidence(
        fixture.h, worker, bound, db=fixture.db, collector_identity_id=IDENTITY,
    )
    effective = worker.record_connector(
        fixture.version("worker"),
        m.OwnedConnectorManifest.model_validate({
            **bound.model_dump(), "revision": bound.revision + 1, "state": "ready", "gaps": (),
            "observed_definition": bound.desired_definition,
            "identity_verified_at": fixture.h.clock(), "delivery_verified_at": fixture.h.clock(),
            "delivery_proof": proof,
            "updated_at": fixture.h.clock(),
        }),
        expected_connector_revision=bound.revision,
        commit=connector_commit(fixture.h, worker, bound.connector_id, db=fixture.db),
    )
    assert effective.state != "ready" and effective.identity_verified_at is None and effective.delivery_verified_at is None
    await fixture.drain()
    ready = fixture.connector()
    assert ready.state == "ready" and ready.identity_verified_at == ready.delivery_verified_at == fixture.h.clock()
    assert len(fixture.remote.update_bodies) == 1


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("interrupt_confirmation", [False, True])
async def test_real_add_then_remove_retains_ownership_until_original_remote_absence_is_published(
    backend, interrupt_confirmation, tmp_path, publication_calls, monkeypatch,
):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    await fixture.execute(fixture.activate())
    await fixture.apply_worker()
    await fixture.drain()
    original_connector = fixture.connector()
    original_source, = original_connector.sources
    assert original_source.source_id == fixture.remote.graph["sources"][0]["id"]
    assert len(fixture.remote.update_bodies) == 1
    exclude(fixture.use("web"), fixture.identity, nonce=90_101)
    removal_work, = fixture.claim("controller", "reconcile_state")
    await fixture.execute(removal_work)
    pending = fixture.connector()
    removal, = pending.source_removals
    assert pending.sources == (original_source,)
    assert pending.desired_definition["parts"]["eventstream.json"]["sources"] == []
    assert fixture.use("controller").resolve_target(fixture.identity) is None
    selector = removal.intent().model_dump(mode="json")
    assert set(selector) == {"removal_id", "source_id", "proposal_id", "detail"}
    assert selector["source_id"] == original_source.source_id and selector["proposal_id"] is None
    assert ("prepare_connector_publication", removal_work.work_id, removal_work.lease.fence) in publication_calls
    assert not any(row.kind == "connector_source_retirement" for row in fixture.rows())
    in_flight = []

    def inspect_before_remote_update():
        current = fixture.connector("worker")
        assert current.sources == (original_source,) and current.source_removals == (removal,)
        assert current.observed_definition is None
        if fixture.db:
            intent = json.loads(next(gap.detail for gap in current.gaps if gap.code == provisioning.INTENT_GAP))
            receipt = fixture.db.receipts[("worker.observe_connector", intent["observation_receipt_id"])]
            assert receipt["payload"]["result"]["observed_definition_hash"] is None
        assert not any(row.kind == "connector_source_retirement" for row in fixture.rows())
        in_flight.append(current.revision)

    fixture.remote.on_update = inspect_before_remote_update
    calls_before_worker = tuple(publication_calls)
    await fixture.apply_worker()
    assert in_flight and tuple(publication_calls) == calls_before_worker
    assert len(fixture.remote.update_bodies) == 2
    assert fixture.remote.graph["sources"] == []
    assert fixture.remote.graph["streams"][0]["inputNodes"] == []
    assert fixture.connector().sources == (original_source,)
    assert not any(row.kind == "connector_source_retirement" for row in fixture.rows())
    failed_work = []
    original_publish = provisioning.publish_connector_intent

    def interrupt_after_retirement(store, request):
        result = original_publish(store, request)
        if result.retired_sources and not failed_work:
            failed_work.append(store.get_work(request.expected, request.work_id))
            raise MonitoringConflict("Injected after retirement before reconciliation commit")
        return result

    if interrupt_confirmation:
        monkeypatch.setattr(provisioning, "publish_connector_intent", interrupt_after_retirement)
        with pytest.raises(MonitoringConflict, match="after retirement"):
            await fixture.drain()
        assert fixture.connector().sources == (original_source,)
        assert not any(row.kind == "connector_source_retirement" for row in fixture.rows())
        monkeypatch.setattr(provisioning, "publish_connector_intent", original_publish)
        await fixture.execute(failed_work[0])
    await fixture.drain()
    retired = fixture.connector()
    assert retired.sources == () and retired.source_proposals == () and retired.source_removals == ()
    assert retired.eventstream_id == original_connector.eventstream_id
    assert retired.destination_id == original_connector.destination_id
    assert retired.endpoint == original_connector.endpoint
    tombstone_row, = [row for row in fixture.rows() if row.kind == "connector_source_retirement"]
    tombstone = m.ConnectorSourceRetirement.model_validate_json(tombstone_row.payload)
    assert tombstone.original_binding == original_source
    assert tombstone.original_removal == removal
    assert tombstone.policy_revision == fixture.version("controller").revision
    assert ("prepare_connector_binding", tombstone.work_id, tombstone.work_fence) in publication_calls
    controller = fixture.use("controller")
    confirmation = controller.get_connector_publication(CONTEXT, tombstone.confirmation_request_id)
    assert confirmation.retired_sources == (tombstone,)
    assert confirmation.observation_receipt_id == tombstone.observation_receipt_id
    assert fixture.remote.update_bodies and len(fixture.remote.update_bodies) == 2
    before_acknowledgement = tuple(publication_calls)
    if fixture.db:
        original_receipts = deepcopy(fixture.db.receipts)
        original_records = {
            key: row for key, row in fixture.db.records.items() if row.kind not in {"work", "scheduler"}
        }
        pending_work = [
            m.MonitoringWork.model_validate_json(row.payload) for row in fixture.db.records.values()
            if row.kind == "work" and row.work_kind == "reconcile_state" and row.status == "waiting"
        ]
        assert pending_work
    fixture.h.clock.advance(16)
    await fixture.drain()
    assert tuple(publication_calls) == before_acknowledgement
    if fixture.db:
        assert all(fixture.db.records[key] == row for key, row in original_records.items())
        assert all(fixture.db.receipts[key] == receipt for key, receipt in original_receipts.items())
        assert all(
            fixture.use("controller").get_work(CONTEXT, work.work_id).state == "completed" for work in pending_work
        )
        resolutions = [
            receipt["payload"]["result"] for key, receipt in fixture.db.receipts.items()
            if key not in original_receipts and key[0] == "controller.resolve_frontier"
        ]
        assert resolutions and all(value["resolution_scope"] == "handoff_acknowledgement" for value in resolutions)
        for resolution in resolutions:
            original = original_receipts[
                ("controller.resolve_frontier", resolution["handoff_resolution_request_id"])
            ]["payload"]["result"]
            assert original["handoff_decision"] == resolution["state"]
            assert original["work_fence"] == resolution["handoff_resolution_work_fence"] < resolution["work_fence"]
    assert next(row for row in fixture.rows() if row.kind == "connector_source_retirement") == tombstone_row
    assert fixture.remote.graph["sources"] == []


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_controller_publication_failure_rolls_back_admission_and_followup_without_losing_intent(
    backend, tmp_path, publication_calls, monkeypatch,
):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    work = fixture.activate()
    before = deepcopy(fixture.rows())
    original = provisioning.publish_connector_intent

    def fail_after_publication(store, request):
        result = original(store, request)
        assert result.connector.source_proposals
        raise MonitoringConflict("Injected after connector publication, before frontier completion")

    monkeypatch.setattr(provisioning, "publish_connector_intent", fail_after_publication)
    with pytest.raises(MonitoringConflict, match="before frontier completion"):
        await fixture.execute(work)
    assert fixture.rows() == before
    controller = fixture.use("controller")
    assert controller.get_work(CONTEXT, work.work_id).state == "leased"
    assert controller.list_targets(m.TargetQuery(**CONTEXT.model_dump())).items == ()
    producer = controller.get_reconciliation_request(CONTEXT, work.reconcile_request_id, producer="web")
    assert controller.get_validation_frontier(CONTEXT, producer.frontier_key).pending
    monkeypatch.setattr(provisioning, "publish_connector_intent", original)
    await fixture.execute(work)
    assert len(fixture.connector().source_proposals) == 1
    assert controller.get_work(CONTEXT, work.work_id).state == "completed"


async def test_sql_outer_lost_ack_recovers_original_reconciliation_without_replanning_latest_state(
    tmp_path, publication_calls,
):
    fixture = PublicationHarness("sql", tmp_path)
    await fixture.collect()
    work = fixture.activate()
    fixture.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain) as uncertain:
        await fixture.execute(work)
    assert uncertain.value.operation == "controller.resolve_frontier"
    controller = fixture.use("controller")
    assert controller.get_work(CONTEXT, work.work_id).state == "completed"
    original = controller.get_operation_receipt(CONTEXT, uncertain.value.operation, uncertain.value.idempotency_id)
    assert original.result["state"] == "published"
    published = fixture.connector()
    worker = fixture.use("worker")
    latest = worker.record_connector(
        fixture.version("worker"),
        m.OwnedConnectorManifest.model_validate({
            **published.model_dump(), "revision": published.revision + 1,
            "state": "provisioning", "observed_definition": None, "updated_at": fixture.h.clock(),
        }),
        expected_connector_revision=published.revision,
        commit=connector_commit(fixture.h, worker, published.connector_id, db=fixture.db),
    )
    calls = tuple(publication_calls)
    assert "deterministic reconciliation published" in await fixture.execute(work)
    assert tuple(publication_calls) == calls
    assert fixture.connector() == latest


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_connector_composition_cannot_enqueue_action_work(backend, tmp_path, monkeypatch):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    work = fixture.activate()
    before = deepcopy(fixture.rows())

    def attempt_action(store, context):
        store.enqueue_work(m.MonitoringWorkDraft(
            **CONTEXT.model_dump(), work_id=fixture.h.next_id(), kind="triage",
            policy_revision=context.expected.revision, created_at=fixture.h.clock(), due_at=fixture.h.clock(),
            target=fixture.identity,
            execution=m.SourceExecutionIdentity(target=fixture.identity, run_id_kind="fabric_job", run_id=uid(90_009)),
            reason="An action queue must not be reachable through connector composition.",
        ))

    monkeypatch.setattr("triage.monitoring.controller.publish_reconciliation_connector", attempt_action)
    with pytest.raises(MonitoringComponentDenied, match="only its own worker follow-up"):
        await fixture.execute(work)
    assert fixture.rows() == before
    assert not any(row.kind in {"action", "action_owner", "incident_state"} for row in fixture.rows())


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_connector_transaction_scope_cannot_be_borrowed_by_another_thread(backend, tmp_path, monkeypatch):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    work = fixture.activate()

    def check_thread(store, context):
        copied = copy_context()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(copied.run, store.get_work, CONTEXT, context.work.work_id)
            with pytest.raises(MonitoringComponentDenied, match="synchronous reconciliation transaction"):
                future.result(timeout=5)
        return publish_reconciliation_connector(store, context)

    monkeypatch.setattr("triage.monitoring.controller.publish_reconciliation_connector", check_thread)
    await fixture.execute(work)
    assert len(fixture.connector().source_proposals) == 1


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_connector_composition_rejects_async_callbacks_without_publishing(backend, tmp_path):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    work = fixture.activate()
    before = deepcopy(fixture.rows())

    async def invalid_publisher(store, context):
        raise AssertionError("An async publisher must never execute inside the transaction")

    controller = fixture.use("controller")
    with pytest.raises(MonitoringConflict, match="synchronous controller composition"):
        controller.reconcile_work(work, connector_publisher=invalid_publisher)
    assert fixture.rows() == before


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_worker_maintenance_cannot_enter_controller_reconciliation(backend, tmp_path):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    work = fixture.activate()
    before = deepcopy(fixture.rows())
    with pytest.raises(MonitoringComponentDenied, match="requires the controller component"):
        reconcile_monitoring_work(fixture.use("worker"), work)
    assert fixture.rows() == before


async def test_controller_component_argument_does_not_authorize_the_worker_sql_principal(tmp_path):
    fixture = PublicationHarness("sql", tmp_path)
    await fixture.collect()
    work = fixture.activate()
    before = deepcopy(fixture.rows())
    fixture.db.principal = "worker"
    with pytest.raises(MonitoringComponentDenied):
        await fixture.runner.execute_monitoring_work(work)
    assert fixture.rows() == before


@pytest.mark.parametrize("backend", ["memory", "sql"])
async def test_trusted_publisher_is_code_not_deep_copied_request_data(backend, tmp_path):
    fixture = PublicationHarness(backend, tmp_path)
    await fixture.collect()
    work = fixture.activate()

    class Publisher:
        def __init__(self):
            self.calls = 0

        def __deepcopy__(self, memo):
            raise AssertionError("Trusted synchronous code must not be copied as request data")

        def __call__(self, store, context):
            self.calls += 1
            return publish_reconciliation_connector(store, context)

    publisher = Publisher()
    result = fixture.use("controller").reconcile_work(work, connector_publisher=publisher)
    assert result.state == "published" and publisher.calls == 1
    assert len(fixture.connector().source_proposals) == 1
