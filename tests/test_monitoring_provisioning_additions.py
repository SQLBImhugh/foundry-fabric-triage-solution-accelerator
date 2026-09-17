from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from uuid import UUID, uuid5

import pytest
from test_monitoring_provisioning import (
    CONNECTOR,
    CONTEXT,
    OWNER,
    SECOND_ITEM,
    connector,
    exclude,
    factory,
    setup_store,
    target,
    version,
)

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.monitoring.memory import InMemoryMonitoringStore, stable_id
from triage.monitoring.provisioning import (
    ProvisioningReview,
    plan_definition,
    prepare_connector_binding,
    prepare_connector_publication,
    publish_connector_intent,
)
from triage.monitoring.sql_kernel_connectors import PUBLICATION_FIELDS
from triage.monitoring.sql_kernel_contracts import KERNEL_VERSION, decode_rpc_result
from triage.monitoring.sql_permissions import build_permission_kernel

__all__ = ["factory"]


class V2CallerStore:
    """DTO/RPC-shape fixture, not a claim that native SQL or its adapter passed."""

    component = "controller"

    def __init__(self, seed, clock):
        self.seed = seed
        self.clock = clock
        self.controller = InMemoryMonitoringStore(
            state=seed._backend.state, clock=clock, component="controller",
        )
        self.worker = InMemoryMonitoringStore(
            state=seed._backend.state, clock=clock, component="worker",
        )
        self.kernel = build_permission_kernel()
        self.receipts = {}
        self.retirements = {}
        self.publish_calls = 0
        self.lose_ack = False

    def snapshot(self, context):
        return self.controller.snapshot(context)

    def list_connectors(self, query):
        return self.controller.list_connectors(query)

    def list_targets(self, query):
        return self.controller.list_targets(query)

    def resolve_target(self, identity):
        return self.controller.resolve_target(identity)

    def get_work(self, context, work_id):
        return self.controller.get_work(context, work_id)

    def get_reconciliation_request(self, context, request_id, *, producer):
        return self.controller.get_reconciliation_request(context, request_id, producer=producer)

    def get_validation_frontier(self, context, key):
        return self.controller.get_validation_frontier(context, key)

    def enqueue_work(self, request):
        return self.controller.enqueue_work(request)

    def claim(self):
        return self.controller.claim_work(m.WorkClaimRequest(
            **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",),
            limit=20, per_workspace_limit=20,
        ))

    def get_connector_publication(self, context, request_id):
        receipt = self.receipts.get(("connector_publication", request_id))
        return m.ConnectorPublicationResult.model_validate(receipt.result) if receipt else None

    def _envelope(self, operation, arguments, result):
        contract = self.kernel.rpcs[operation]
        sql, parameters = contract.bind(arguments)
        assert sql and len(parameters) == len(contract.parameters)
        return decode_rpc_result(contract, [(json.dumps({
            "kernel_version": KERNEL_VERSION, "operation": operation,
            "status": "applied", "affected_rows": 1, "result": result,
        }),)])["result"]

    def get_operation_receipt(self, context, operation, request_id):
        if operation == "connector_publication":
            return self.receipts.get((operation, request_id))
        assert operation == "worker.observe_connector"
        key = (operation, request_id)
        if key in self.receipts:
            return self.receipts[key]
        original = self.worker.get_operation_receipt(context, operation, request_id)
        if original is None:
            return None
        result = m.ConnectorObservationResult.model_validate(original.result)
        observed = result.observation
        checked = self._envelope(operation, {
            **CONTEXT.model_dump(), "request_id": request_id, "fingerprint": original.fingerprint,
            "expected_revision": observed.policy_revision,
            "work_id": result.work_id, "owner_id": result.work_owner_id,
            "fence": result.work_fence, "work_revision": result.work_revision,
            "connector_id": observed.connector_id,
            "expected_connector_revision": observed.revision - 1,
            "ownership_id": observed.ownership_id,
            "observation_json": json.dumps(observed.model_dump(mode="json", include={
                "workspace_id", "eventstream_id", "destination_id", "observed_definition",
                "endpoint", "operation_id", "state", "identity_verified_at",
                "delivery_verified_at", "gaps",
            }), sort_keys=True),
        }, result.model_dump(mode="json"))
        receipt = m.OperationReceipt(
            **CONTEXT.model_dump(), operation=operation, request_id=request_id,
            fingerprint=original.fingerprint, recorded_at=original.recorded_at, result=checked,
        )
        self.receipts[key] = receipt
        return receipt

    def publish_connector(self, request):
        self.publish_calls += 1
        current = connector(self.seed)
        work = self.get_work(request.expected, request.work_id)
        producer = self.get_reconciliation_request(
            request.expected, work.reconcile_request_id, producer=work.reconcile_producer,
        )
        frontier = self.get_validation_frontier(request.expected, producer.frontier_key)
        assert current.revision == request.expected_connector_revision
        assert work.lease == request.lease and work.revision == request.expected_work_revision
        assert frontier.accepted_revision == request.expected_frontier_revision
        plan = m.ConnectorPublicationPlan(
            connector_id=request.connector_id, ownership_id=request.ownership_id,
            work_id=work.work_id, lease_owner_id=work.lease.owner_id,
            lease_fence=work.lease.fence, expected_work_revision=work.revision,
            expected_connector_revision=current.revision, policy_revision=request.expected.revision,
            producer_request_id=producer.request_id, producer_fingerprint=producer.fingerprint,
            frontier_key=frontier.frontier_key, frontier_revision=frontier.accepted_revision,
            name=request.name, sources=request.sources, source_proposals=request.source_proposals,
            source_removals=request.source_removals,
            desired_definition=request.desired_definition,
            observation_receipt_id=request.observation_receipt_id,
            readiness_receipt_id=request.readiness_receipt_id, detail=request.detail,
        )
        assert set(m.ConnectorPublicationPlan.model_fields) == set(PUBLICATION_FIELDS)
        sources, proposals, definition = request.sources, request.source_proposals, request.desired_definition
        pending = {removal.removal_id: removal for removal in current.source_removals}
        retired = []
        for intent in request.source_removals:
            if intent.removal_id in pending:
                assert pending[intent.removal_id].intent() == intent
                continue
            assert request.observation_receipt_id is None
            assert intent.removal_id not in self.retirements
            if intent.source_id is not None:
                binding = next(source for source in current.sources if source.source_id == intent.source_id)
                names = {
                    key.removeprefix("sources/")
                    for snapshot in (current.desired_definition, current.observed_definition)
                    if snapshot is not None
                    for key, value in snapshot["component_ids"].items()
                    if key.startswith("sources/") and value == intent.source_id
                }
                assert len(names) == 1
                name = names.pop()
            else:
                binding = next(p for p in current.source_proposals if p.proposal_id == intent.proposal_id)
                name = binding.node_name
            last_id = (current.observed_definition or {}).get("component_ids", {}).get(f"sources/{name}")
            pending[intent.removal_id] = m.PendingSourceRemoval(
                **intent.model_dump(), node_name=name, last_observed_source_id=last_id,
                target=binding.target,
                binding_hash=hashlib.sha256(binding.model_dump_json().encode("utf-16-le")).hexdigest().upper(),
                policy_revision=request.expected.revision, request_id=request.request_id,
                publication_id=str(uuid5(UUID(request.request_id), "fixture-publication")),
                requested_at=self.clock(), state="pending_remote_absence",
            )
        assert set(pending) == {intent.removal_id for intent in request.source_removals}
        if request.observation_receipt_id is not None:
            assert request.observation_receipt_id == work.reconcile_request_id
            assert request.readiness_receipt_id is None
            assert (sources, proposals, definition) == (
                current.sources, current.source_proposals, current.desired_definition,
            )
            raw = self.get_operation_receipt(
                request.expected, "worker.observe_connector", request.observation_receipt_id,
            )
            evidence = m.ConnectorObservationResult.model_validate(raw.result)
            if (
                evidence.connector.revision != current.revision
                or evidence.observed_definition_hash is None
                or evidence.observation.observed_definition["parts"] != definition["parts"]
            ):
                raise MonitoringConflict("Original fixture observation does not match the published intent")
            definition = evidence.observation.observed_definition
            new_sources = []
            remaining = []
            for proposal in proposals:
                if any(item.proposal_id == proposal.proposal_id for item in pending.values()):
                    remaining.append(proposal)
                    continue
                physical = definition["component_ids"].get(f"sources/{proposal.node_name}")
                if physical is None:
                    remaining.append(proposal)
                    continue
                assert str(UUID(physical)) == physical
                values = proposal.model_dump(exclude={"proposal_id", "node_name"})
                new_sources.append(m.ConnectorSource.model_validate({**values, "source_id": physical}))
            sources, proposals = (*sources, *new_sources), tuple(remaining)
            for removal in pending.values():
                graph = definition["parts"]["eventstream.json"]
                ids = {identifier for identifier in (
                    removal.source_id, removal.last_observed_source_id,
                ) if identifier is not None}
                if (
                    f"sources/{removal.node_name}" in definition["component_ids"]
                    or ids.intersection(definition["component_ids"].values())
                    or any(node["name"] == removal.node_name or node.get("id") in ids for node in graph["sources"])
                    or any(
                        node["name"] == removal.node_name
                        for stream in graph["streams"] for node in stream["inputNodes"]
                    )
                ):
                    raise MonitoringConflict("Original fixture observation does not prove source absence")
                binding = next(s for s in sources if s.source_id == removal.source_id) if removal.source_id else next(
                    p for p in proposals if p.proposal_id == removal.proposal_id
                )
                retirement = m.ConnectorSourceRetirement(
                    connector_id=current.connector_id, ownership_id=current.ownership_id,
                    removal_id=removal.removal_id, source_id=removal.source_id,
                    proposal_id=removal.proposal_id, node_name=removal.node_name,
                    original_binding=binding, original_removal=removal,
                    observation_receipt_id=request.observation_receipt_id,
                    observation_fingerprint=raw.fingerprint,
                    observation_binding_hash=hashlib.sha256(raw.model_dump_json().encode("utf-16-le")).hexdigest().upper(),
                    observation_receipt_hash=hashlib.sha256(raw.model_dump_json().encode("utf-16-le")).hexdigest().upper(),
                    observed_definition_hash=evidence.observed_definition_hash,
                    confirmation_request_id=request.request_id, work_id=work.work_id,
                    work_fence=work.lease.fence, policy_revision=request.expected.revision,
                    retired_at=self.clock(), state="retired_verified",
                )
                assert removal.removal_id not in self.retirements
                self.retirements[removal.removal_id] = retirement
                retired.append(retirement)
            removed_ids = {item.source_id for item in pending.values()}
            removed_proposals = {item.proposal_id for item in pending.values()}
            sources = tuple(source for source in sources if source.source_id not in removed_ids)
            proposals = tuple(p for p in proposals if p.proposal_id not in removed_proposals)
            pending = {}
        else:
            assert {source.source_id for source in sources}.issubset(
                source.source_id for source in current.sources
            )
        saved = self.seed.record_connector(
            request.expected,
            m.OwnedConnectorManifest.model_validate({
                **current.model_dump(), "revision": current.revision + 1,
                "sources": sources, "source_proposals": proposals, "desired_definition": definition,
                "source_removals": tuple(pending.values()),
                "policy_revision": request.expected.revision,
                "state": "provisioning", "identity_verified_at": None, "delivery_verified_at": None,
                "updated_at": self.clock(),
            }),
            expected_connector_revision=current.revision,
        )
        result = m.ConnectorPublicationResult(
            connector_id=saved.connector_id, connector=saved,
            state=saved.state, desired_changed=True,
            pending_removals=saved.source_removals, retired_sources=tuple(retired),
            observation_receipt_id=request.observation_receipt_id,
        )
        canonical = json.dumps(plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        fingerprint = m._digest(request.model_dump(mode="json"))
        checked = self._envelope("controller.publish_connector", {
            **CONTEXT.model_dump(), "request_id": request.request_id, "fingerprint": fingerprint,
            "expected_revision": request.expected.revision, "work_id": work.work_id,
            "owner_id": work.lease.owner_id, "fence": work.lease.fence,
            "work_revision": work.revision,
            "publication_id": str(uuid5(UUID(request.request_id), "fixture-publication")),
            "publication_hash": hashlib.sha256(canonical.encode("utf-16-le")).hexdigest().upper(),
        }, result.model_dump(mode="json"))
        self.receipts[("connector_publication", request.request_id)] = m.OperationReceipt(
            **CONTEXT.model_dump(), operation="connector_publication", request_id=request.request_id,
            fingerprint=fingerprint, recorded_at=self.clock(), result=checked,
        )
        if self.lose_ack:
            self.lose_ack = False
            raise MonitoringCommitUncertain("controller.publish_connector", request.request_id)
        return m.ConnectorPublicationResult.model_validate(checked)


async def staged_addition(factory):
    seed, _, clock, remote = setup_store(existing=())
    store = V2CallerStore(seed, clock)
    prior = connector(seed)
    collection, = store.worker.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("connector_reconcile",), limit=1, per_workspace_limit=1,
    ))
    store.worker.record_connector(
        version(seed), prior.model_copy(update={
            "revision": prior.revision + 1, "state": "degraded",
            "gaps": (m.CoverageGap(code="controller_scope_publication_required", detail="No desired source has been published."),),
        }),
        expected_connector_revision=prior.revision,
        commit=m.CollectionCommit(
            work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
        ),
    )
    store.worker.complete_collection_work(
        CONTEXT, work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
    )
    work = store.claim()[0]
    request = prepare_connector_publication(
        store, work, CONNECTOR, request_id=str(UUID(int=80_001)),
    )
    assert request.sources == () and len(request.source_proposals) == 1
    assert request.source_proposals[0].source_id is None
    publish_connector_intent(store, request)
    await factory(seed, clock, remote).run_once()
    observed = connector(seed)
    observation_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{observed.revision - 1}")
    binding_work = next(
        work for work in store.claim() if work.reconcile_request_id == observation_id
    )
    binding = prepare_connector_binding(
        store, binding_work, CONNECTOR, request_id=str(UUID(int=80_002)),
    )
    return seed, store, clock, remote, binding


async def test_addition_materializes_only_actual_observation_and_never_readiness(factory):
    seed, store, clock, remote, request = await staged_addition(factory)
    result = publish_connector_intent(store, request)
    assert len(result.connector.sources) == 1 and result.connector.source_proposals == ()
    assert result.connector.sources[0].source_id == remote.graph["sources"][0]["id"]
    assert result.connector.sources != request.sources
    assert result.connector.desired_definition != request.desired_definition
    assert result.state == "provisioning"
    assert result.connector.identity_verified_at is None and result.connector.delivery_verified_at is None
    assert request.readiness_receipt_id is None
    await factory(seed, clock, remote).run_once()
    assert len(remote.update_bodies) == 1


async def test_binding_lost_ack_recovers_original_materialized_publication(factory):
    seed, store, _, remote, request = await staged_addition(factory)
    store.lose_ack = True
    before = store.publish_calls
    original = publish_connector_intent(store, request)
    assert store.publish_calls == before + 1
    current = connector(seed)
    seed.record_connector(
        version(seed), current.model_copy(update={
            "revision": current.revision + 1, "state": "blocked",
            "gaps": (m.CoverageGap(code="later_observation", detail="Different observation"),),
        }),
        expected_connector_revision=current.revision,
    )
    assert publish_connector_intent(store, request) == original
    assert store.publish_calls == before + 1
    assert len(remote.update_bodies) == 1


@pytest.mark.parametrize("change", ["missing_id", "stale_revision", "wrong_parts", "foreign_component"])
async def test_binding_refuses_incomplete_or_unmatched_original_observation(factory, change):
    _, store, _, remote, request = await staged_addition(factory)
    key = ("worker.observe_connector", request.observation_receipt_id)
    receipt = store.get_operation_receipt(CONTEXT, *key)
    value = deepcopy(receipt.result)
    if change == "missing_id":
        value["observation"]["observed_definition"]["component_ids"].pop(
            "sources/" + request.source_proposals[0].node_name,
        )
    elif change == "stale_revision":
        value["connector"]["revision"] += 1
        value["observation"]["revision"] += 1
    elif change == "wrong_parts":
        value["observation"]["observed_definition"]["parts"]["eventstream.json"]["sources"][0]["properties"]["itemId"] = str(UUID(int=999_999))
    else:
        value["observation"]["observed_definition"]["component_ids"]["sources/unowned"] = str(UUID(int=999_999))
    store.receipts[key] = receipt.model_copy(update={"result": value})
    before = store.publish_calls
    with pytest.raises((ProvisioningReview, MonitoringConflict, MonitoringUnavailable)):
        publish_connector_intent(store, request)
    assert store.publish_calls == before + 1
    assert len(remote.update_bodies) == 1


def fresh_controller_work(seed, store, clock, identifier):
    web = InMemoryMonitoringStore(state=seed._backend.state, clock=clock, component="web")
    queued = web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=identifier,
    )
    return next(work for work in store.claim() if work.work_id == queued.work_id)


@pytest.mark.parametrize("lost_ack", [False, True])
async def test_remove_retains_identity_until_exact_original_absence_then_retires(factory, lost_ack):
    seed, store, clock, remote, addition = await staged_addition(factory)
    added = publish_connector_intent(store, addition).connector
    source_id = added.sources[0].source_id
    exclude(seed, target())
    work = fresh_controller_work(seed, store, clock, str(UUID(int=81_001)))
    request = prepare_connector_publication(
        store, work, CONNECTOR, request_id=str(UUID(int=81_002)),
    )
    assert request.sources == added.sources
    assert len(request.source_removals) == 1
    pending = publish_connector_intent(store, request)
    assert pending.connector.sources == added.sources
    assert pending.pending_removals[0].source_id == source_id
    assert pending.retired_sources == ()
    result = await factory(seed, clock, remote).run_once()
    assert any(item.code == "controller_source_retirement_required" for item in result.results), result
    observed = connector(seed)
    assert observed.sources == added.sources
    assert observed.observed_definition["parts"] == request.desired_definition["parts"]
    assert source_id not in observed.observed_definition["component_ids"].values()
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{observed.revision - 1}")
    receipt = store.get_operation_receipt(CONTEXT, "worker.observe_connector", receipt_id)
    assert receipt.result["observed_definition_hash"] is not None
    confirmation_work = next(
        value for value in store.claim() if value.reconcile_request_id == receipt_id
    )
    confirmation = prepare_connector_binding(
        store, confirmation_work, CONNECTOR, request_id=str(UUID(int=81_003)),
    )
    store.lose_ack = lost_ack
    retired = publish_connector_intent(store, confirmation)
    assert retired.connector.sources == () and retired.pending_removals == ()
    tombstone = retired.retired_sources[0]
    assert tombstone.original_binding == added.sources[0]
    assert tombstone.observation_receipt_id == receipt_id
    assert tombstone.original_removal == pending.pending_removals[0]
    assert store.retirements[tombstone.removal_id] == tombstone
    assert publish_connector_intent(store, confirmation) == retired
    assert len(remote.update_bodies) == 2


async def test_pending_logical_withdrawal_uses_fresh_absence_without_fabricating_source_id(factory):
    seed, _, clock, remote = setup_store(existing=())
    store = V2CallerStore(seed, clock)
    work = fresh_controller_work(seed, store, clock, str(UUID(int=82_001)))
    addition = prepare_connector_publication(
        store, work, CONNECTOR, request_id=str(UUID(int=82_002)),
    )
    planned = publish_connector_intent(store, addition).connector
    assert planned.source_proposals[0].source_id is None
    exclude(seed, target())
    removal_work = fresh_controller_work(seed, store, clock, str(UUID(int=82_003)))
    removal = prepare_connector_publication(
        store, removal_work, CONNECTOR, request_id=str(UUID(int=82_004)),
    )
    pending = publish_connector_intent(store, removal)
    assert pending.pending_removals[0].source_id is None
    assert pending.pending_removals[0].proposal_id == planned.source_proposals[0].proposal_id
    await factory(seed, clock, remote).run_once()
    observed = connector(seed)
    original_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{observed.revision - 1}")
    confirmation_work = next(
        value for value in store.claim() if value.reconcile_request_id == original_id
    )
    confirmation = prepare_connector_binding(
        store, confirmation_work, CONNECTOR, request_id=str(UUID(int=82_005)),
    )
    retired = publish_connector_intent(store, confirmation)
    assert retired.connector.source_proposals == ()
    assert retired.retired_sources[0].source_id is None
    assert isinstance(retired.retired_sources[0].original_binding, m.ConnectorSourceProposal)
    assert not remote.update_bodies


async def test_inherited_snapshot_is_not_reissued_as_fresh_definition_evidence(factory):
    seed, _, clock, remote = setup_store(existing=())
    store = V2CallerStore(seed, clock)
    prior = connector(seed)
    assert prior.observed_definition is not None
    collection, = store.worker.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("connector_reconcile",), limit=1, per_workspace_limit=1,
    ))
    saved = await factory(seed, clock, remote)._save(
        collection, prior, state="degraded",
        gaps=(m.CoverageGap(code="outcome_unknown", detail="No fresh topology read"),),
    )
    original_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{prior.revision}")
    original = store.get_operation_receipt(CONTEXT, "worker.observe_connector", original_id)
    assert saved.observed_definition is None
    assert original.result["observed_definition_hash"] is None
    assert original.result["observation"]["observed_definition"] is None


async def test_uncertain_removal_cannot_retire_from_intent_or_repeat_post(factory):
    seed, store, clock, remote, addition = await staged_addition(factory)
    source = publish_connector_intent(store, addition).connector.sources[0]
    exclude(seed, target())
    work = fresh_controller_work(seed, store, clock, str(UUID(int=83_001)))
    removal = prepare_connector_publication(
        store, work, CONNECTOR, request_id=str(UUID(int=83_002)),
    )
    publish_connector_intent(store, removal)
    remote.timeout_after_apply = True
    result = await factory(seed, clock, remote).run_once()
    assert result.results[0].code == "definition_update_outcome_unknown"
    pending = connector(seed)
    assert pending.sources == (source,) and pending.observed_definition is None
    intent_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{pending.revision - 1}")
    receipt = store.get_operation_receipt(CONTEXT, "worker.observe_connector", intent_id)
    assert receipt.result["observed_definition_hash"] is None
    intent_work = next(value for value in store.claim() if value.reconcile_request_id == intent_id)
    premature = prepare_connector_binding(
        store, intent_work, CONNECTOR, request_id=str(UUID(int=83_003)),
    )
    with pytest.raises(MonitoringConflict):
        publish_connector_intent(store, premature)
    assert connector(seed).sources == (source,) and not store.retirements
    clock.advance(31)
    remote.timeout_after_apply = False
    verified = await factory(seed, clock, remote).run_once()
    assert verified.results[0].code == "controller_source_retirement_required"
    assert len(remote.update_bodies) == 2
    fresh = connector(seed)
    fresh_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{fresh.revision - 1}")
    work = next(value for value in store.claim() if value.reconcile_request_id == fresh_id)
    confirmation = prepare_connector_binding(
        store, work, CONNECTOR, request_id=str(UUID(int=83_004)),
    )
    retired = publish_connector_intent(store, confirmation)
    assert retired.connector.sources == () and len(retired.retired_sources) == 1
    assert len(remote.update_bodies) == 2


@pytest.mark.parametrize("logical", [False, True], ids=["physical", "logical"])
async def test_component_stores_execute_removal_and_retirement_through_real_store_methods(factory, logical):
    seed, state, clock, remote = setup_store(existing=() if logical else None)
    if logical:
        prior = connector(seed)
        plan = plan_definition(prior, prior.desired_definition, (seed.resolve_target(target()),))
        seed.record_connector(
            version(seed), prior.model_copy(update={
                "revision": prior.revision + 1, "desired_definition": plan.desired,
                "source_proposals": plan.source_proposals,
            }), expected_connector_revision=prior.revision,
        )
    else:
        await factory(seed, clock, remote).run_once()
    owned = connector(seed)
    original = owned.source_proposals[0] if logical else owned.sources[0]
    exclude(seed, target())
    web = InMemoryMonitoringStore(state=state, clock=clock, component="web")
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    queued = web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=str(UUID(int=84_001)),
    )

    def claim():
        return controller.claim_work(m.WorkClaimRequest(
            **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",),
            limit=20, per_workspace_limit=20,
        ))

    work = next(value for value in claim() if value.work_id == queued.work_id)
    request = prepare_connector_publication(
        controller, work, CONNECTOR, request_id=str(UUID(int=84_002)),
    )
    pending = publish_connector_intent(controller, request)
    assert (*pending.connector.sources, *pending.connector.source_proposals) == (original,)
    assert len(pending.pending_removals) == 1
    await factory(seed, clock, remote).run_once()
    fresh = connector(seed)
    assert (*fresh.sources, *fresh.source_proposals) == (original,)
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{fresh.revision - 1}")
    work = next(value for value in claim() if value.reconcile_request_id == receipt_id)
    confirmation = prepare_connector_binding(
        controller, work, CONNECTOR, request_id=str(UUID(int=84_003)),
    )
    result = publish_connector_intent(controller, confirmation)
    assert result.connector.sources == ()
    assert result.connector.source_proposals == ()
    assert result.retired_sources[0].original_binding == original
    assert result.retired_sources[0].observation_receipt_id == receipt_id
    assert publish_connector_intent(controller, confirmation) == result
    assert len(remote.update_bodies) == (0 if logical else 2)


async def test_one_published_change_adds_and_removes_with_one_remote_update(factory):
    first, second = target(), target(item_id=SECOND_ITEM)
    seed, _, clock, remote = setup_store(admitted=(first, second), existing=(first,))
    await factory(seed, clock, remote).run_once()
    before = connector(seed)
    store = V2CallerStore(seed, clock)
    exclude(seed, first)
    work = fresh_controller_work(seed, store, clock, str(UUID(int=85_001)))
    change = prepare_connector_publication(
        store, work, CONNECTOR, request_id=str(UUID(int=85_002)),
    )
    assert change.sources == before.sources
    assert change.source_proposals[0].target == second
    assert change.source_removals[0].source_id == before.sources[0].source_id
    pending = publish_connector_intent(store, change)
    updates = len(remote.update_bodies)
    await factory(seed, clock, remote).run_once()
    observed = connector(seed)
    assert observed.sources == before.sources
    assert len(remote.update_bodies) == updates + 1
    assert [node["properties"]["itemId"] for node in remote.graph["sources"]] == [SECOND_ITEM]
    receipt_id = stable_id(CONTEXT, f"connector:{CONNECTOR}:{observed.revision - 1}")
    work = next(value for value in store.claim() if value.reconcile_request_id == receipt_id)
    confirmation = prepare_connector_binding(
        store, work, CONNECTOR, request_id=str(UUID(int=85_003)),
    )
    complete = publish_connector_intent(store, confirmation)
    assert [source.target for source in complete.connector.sources] == [second]
    assert complete.pending_removals == () and complete.connector.source_proposals == ()
    assert complete.retired_sources[0].original_removal == pending.pending_removals[0]
    assert complete.retired_sources[0].original_binding == before.sources[0]
