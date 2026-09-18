from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from test_monitoring_sql_review9_bindings import connector_commit
from test_monitoring_sql_review9_bindings import publication as proposal_publication
from test_monitoring_sql_review9_bindings import setup_sql as original_setup_sql
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringUnavailable,
)
from triage.monitoring.memory import stable_id
from triage.monitoring.provisioning import (
    ConnectorReconciler,
    ProvisioningReview,
    prepare_connector_binding,
    prepare_connector_publication,
    publish_connector_intent,
)
from triage.monitoring.sql_store import AzureSqlMonitoringStore


class NoSourceCalls:
    async def probe(self, *args, **kwargs):
        raise AssertionError("Receipt adapter tests must not invoke Fabric")


def setup_sql():
    from test_monitoring_sql_receiver_bindings import ReceiverAbiDatabase

    h, db, store, work, frontier = original_setup_sql(database_type=ReceiverAbiDatabase)
    current_result_fields(db)
    request = publication(h, work, frontier)
    # Explicit pre-observed fixture ownership, not a new caller-assigned ID.
    prior = m.OwnedConnectorManifest(
        **h.context(), connector_id=request.connector_id, ownership_id=request.ownership_id,
        revision=1, policy_revision=h.version.revision, name="Prior owned fixture",
        sources=request.sources, desired_definition=request.desired_definition,
        state="planned", updated_at=h.clock(),
    )
    db.native_put("connector", prior.connector_id, prior.model_dump(mode="json"), status=prior.state)
    db.native_put("connector_desired", prior.connector_id, m.ConnectorDesiredState(
        connector_id=prior.connector_id, ownership_id=prior.ownership_id,
        publication_id=h.next_id(), policy_revision=prior.policy_revision,
        sources_hash=m._digest([value.model_dump(mode="json") for value in prior.sources]),
        definition_hash=m.connector_definition_hash(prior.desired_definition), published_at=h.clock(),
    ).model_dump(mode="json"))
    return h, db, store, work, frontier


def current_result_fields(db):
    """Extend this test's no-removal fixture with the current explicit result fields."""
    original = db.reply
    original_query = db.query

    def query(sql, *parameters):
        if sql == "SELECT JSON_QUERY(?, '$.observed_definition')":
            value = json.loads(parameters[0]).get("observed_definition")
            return [(None if value is None else json.dumps(
                value, sort_keys=True, separators=(",", ":"),
            ),)]
        return original_query(sql, *parameters)

    def reply(operation, args, result):
        if operation == "controller.publish_connector":
            row = db.records[("connector_publication", args["publication_id"])]
            plan = m.ConnectorPublicationPlan.model_validate_json(row.payload)
            assert not plan.source_removals, "Removal SQL integration needs its own full fixture"
            result = {
                **result, "pending_removals": [], "retired_sources": [],
                "observation_receipt_id": plan.observation_receipt_id,
            }
        elif operation == "worker.observe_connector":
            definition = json.loads(args["observation_json"]).get("observed_definition")
            result = {**result, "observed_definition_hash": (
                hashlib.sha256(json.dumps(
                    definition, sort_keys=True, separators=(",", ":"),
                ).encode("utf-16-le")).hexdigest().upper() if definition is not None else None
            )}
        return original(operation, args, result)

    db.reply = reply
    db.query = query


def publication(h, work, frontier):
    target = h.targets[0]
    events = ("Microsoft.Fabric.JobEvents.ItemJobCreated", "Microsoft.Fabric.JobEvents.ItemJobFailed")
    definition = {
        "parts": {"eventstream.json": {
            "sources": [{
                "name": "owned-source", "type": "FabricJobEvents",
                "properties": {
                    "eventScope": "Item", "workspaceId": target.workspace_id,
                    "itemId": target.item_id, "includedEventTypes": list(events),
                },
            }],
            "operators": [],
            "streams": [{
                "name": "owned-stream", "type": "DefaultStream",
                "inputNodes": [{"name": "owned-source"}],
            }],
            "destinations": [{
                "name": "owned-endpoint", "type": "CustomEndpoint",
                "inputNodes": [{"name": "owned-stream"}],
            }],
        }},
        "component_ids": {
            "sources/owned-source": uid(901), "streams/owned-stream": uid(902),
            "destinations/owned-endpoint": uid(903),
        },
    }
    return m.ConnectorPublicationRequest(
        request_id=h.next_id(), expected=h.version, work_id=work.work_id,
        lease=work.lease, expected_work_revision=work.revision,
        expected_frontier_revision=frontier.accepted_revision,
        connector_id=uid(910), ownership_id=uid(911), expected_connector_revision=1,
        name="Explicit owned connector",
        sources=(m.ConnectorSource(
            source_id=uid(901), target=target, event_types=events, event_source="fixture:Source",
        ),),
        desired_definition=definition, detail="Update an explicitly pre-observed fixture binding.",
    )


def bound_worker():
    from test_monitoring_sql_receiver_bindings import record_delivery_evidence

    h, db, controller, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    created = controller.publish_connector(request).connector
    controller.reconcile_work(work)
    h.clock.advance(1)
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    commit = connector_commit(h, worker, created.connector_id, db=db)
    bound = worker.record_connector(
        h.version,
        m.OwnedConnectorManifest.model_validate({
            **created.model_dump(), "revision": created.revision + 1,
            "workspace_id": uid(930), "eventstream_id": uid(931), "destination_id": uid(903),
            "endpoint": m.EndpointMetadata(
                namespace="fixture.example.invalid", entity="fixture", consumer_group="$Default",
            ),
            "observed_definition": created.desired_definition, "state": "degraded",
            "gaps": (m.CoverageGap(code="delivery_unverified", detail="Explicit offline fixture"),),
            "updated_at": h.clock(),
        }),
        expected_connector_revision=created.revision,
        commit=commit,
    )
    bound, _ = record_delivery_evidence(h, worker, bound, db=db, reconcile_intake=False)
    context = m.MonitoringContext(**h.context())
    reconciler = ConnectorReconciler(
        worker, context, SimpleNamespace(context=context), NoSourceCalls(),
        uid(950), bound.connector_id, clock=h.clock,
    )
    return h, db, worker, bound, reconciler, worker.get_work(h.version, commit.work_id)


async def report_ready(reconciler, prior, clock, work):
    proof = reconciler.store.get_connector_delivery(reconciler.context, prior.connector_id, uid(4))
    return await reconciler._save(
        work, prior, state="ready", observed_definition=prior.desired_definition,
        identity_verified_at=clock(), delivery_verified_at=clock(), delivery_proof=proof, gaps=(),
    )


async def test_worker_ready_response_is_effective_provisioning_not_reported_authority():
    h, _, worker, prior, reconciler, work = bound_worker()
    saved = await report_ready(reconciler, prior, h.clock, work)
    assert saved.state == "provisioning"
    assert saved.identity_verified_at is None and saved.delivery_verified_at is None
    request_id = stable_id(h.version, f"connector:{prior.connector_id}:{prior.revision}")
    receipt = worker.get_operation_receipt(h.version, "worker.observe_connector", request_id)
    original = m.ConnectorObservationResult.model_validate(receipt.result)
    assert original.observation.state == "ready"
    assert original.connector == saved
    assert original.authority == "observed_not_action_authority"


async def test_reported_ready_cannot_be_returned_as_effective_worker_authority():
    h, _, worker, prior, reconciler, work = bound_worker()
    worker.record_connector = lambda expected, manifest, **kwargs: manifest
    with pytest.raises(MonitoringUnavailable):
        await report_ready(reconciler, prior, h.clock, work)


async def test_lost_ack_recovers_original_observation_not_a_newer_effective_connector():
    h, db, worker, prior, reconciler, work = bound_worker()
    original = worker.record_connector
    calls = 0

    def lose_ack(expected, manifest, **kwargs):
        nonlocal calls
        calls += 1
        db.fail_commit = "after"
        try:
            return original(expected, manifest, **kwargs)
        except MonitoringCommitUncertain:
            current = next(
                item for item in worker.list_connectors(m.PageQuery(**h.context())).items
                if item.connector_id == prior.connector_id
            )
            original(
                expected,
                m.OwnedConnectorManifest.model_validate({
                    **current.model_dump(), "revision": current.revision + 1,
                    "state": "blocked",
                    "gaps": (m.CoverageGap(code="later_observation", detail="A different receipt"),),
                }),
                expected_connector_revision=current.revision,
                commit=kwargs["commit"],
            )
            raise

    async def no_latest_equality(*args):
        raise AssertionError("Reconcile the original operation receipt, never latest-state equality")

    worker.record_connector = lose_ack
    reconciler._connector = no_latest_equality
    saved = await report_ready(reconciler, prior, h.clock, work)
    assert calls == 1
    assert saved.state == "provisioning" and saved.revision == prior.revision + 1
    current = next(
        item for item in worker.list_connectors(m.PageQuery(**h.context())).items
        if item.connector_id == prior.connector_id
    )
    assert current.state == "blocked" and current.revision == saved.revision + 1
    assert current.sources == saved.sources == prior.sources


@pytest.mark.parametrize("failure", [
    "before", "fingerprint", "work_id", "work_owner_id", "work_fence", "work_revision", "collection_completion_eligible",
])
async def test_missing_or_mismatched_original_receipt_never_becomes_confirmed_observation(failure):
    h, db, worker, prior, reconciler, work = bound_worker()
    original = worker.record_connector
    read = worker.get_operation_receipt
    calls = 0

    def uncertain(*args, **kwargs):
        nonlocal calls
        calls += 1
        db.fail_commit = "before" if failure == "before" else "after"
        return original(*args, **kwargs)

    def mismatch(*args, **kwargs):
        receipt = read(*args, **kwargs)
        if receipt is None:
            return None
        if failure == "fingerprint":
            return receipt.model_copy(update={"fingerprint": "0" * 64})
        value = dict(receipt.result)
        value[failure] = (
            uid(999) if failure in {"work_id", "work_owner_id"} else
            False if failure == "collection_completion_eligible" else value[failure] + 1
        )
        return receipt.model_copy(update={"result": value})

    worker.record_connector = uncertain
    if failure != "before":
        worker.get_operation_receipt = mismatch
    expected_error = MonitoringCommitUncertain if failure == "before" else MonitoringUnavailable
    with pytest.raises(expected_error):
        await report_ready(reconciler, prior, h.clock, work)
    assert calls == 1


@pytest.mark.parametrize("changes", [
    {"sources": ()},
    {"desired_definition": {}},
    {"name": "Different controller intent"},
    {"ownership_id": uid(990)},
    {"endpoint": None},
])
async def test_worker_save_refuses_controller_intent_or_physical_rebinding_before_sql(changes):
    _, db, _, prior, reconciler, work = bound_worker()
    before = len(db.calls)
    with pytest.raises(ProvisioningReview, match="worker_cannot_publish_connector_intent"):
        await reconciler._save(work, prior, **changes)
    assert len(db.calls) == before


def test_controller_publication_queues_worker_reconciliation_once():
    h, _, controller, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    result = publish_connector_intent(controller, request)
    repeated = publish_connector_intent(controller, request)
    assert repeated == result
    identifier = stable_id(
        h.version, f"connector:{request.connector_id}:publication:{result.connector.revision}",
    )
    queued = controller.get_work(h.version, identifier)
    assert queued.kind == "connector_reconcile"
    assert queued.connector_id == result.connector_id
    assert result.state == "provisioning"
    assert result.connector.delivery_verified_at is None


def test_controller_publication_lost_ack_uses_original_typed_receipt_without_republishing():
    h, db, controller, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    original = controller.publish_connector
    calls = 0

    def uncertain(value):
        nonlocal calls
        calls += 1
        db.fail_commit = "after"
        return original(value)

    controller.publish_connector = uncertain
    result = publish_connector_intent(controller, request)
    assert calls == 1
    assert result == controller.get_connector_publication(h.version, request.request_id)
    assert result.connector.revision == request.expected_connector_revision + 1


def test_worker_facade_cannot_publish_desired_topology():
    h, db, _, work, frontier = setup_sql()
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    with pytest.raises(MonitoringComponentDenied):
        publish_connector_intent(worker, publication(h, work, frontier))


def test_controller_prepares_existing_source_intent_from_current_admission_and_frontier():
    h, db, _, prior, _, _ = bound_worker()
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(970), kinds=("reconcile_state",),
        limit=1, per_workspace_limit=1,
    ))[0]
    request = prepare_connector_publication(
        controller, work, prior.connector_id, request_id=h.next_id(),
    )
    assert request.sources == prior.sources
    assert request.desired_definition == prior.desired_definition
    assert request.work_id == work.work_id and request.lease == work.lease
    assert request.expected_work_revision == work.revision
    assert request.readiness_receipt_id is None


def test_current_shared_source_contract_rejects_null_physical_ids():
    h, _, _, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    value = request.sources[0].model_dump()
    with pytest.raises(ValidationError):
        m.ConnectorSource.model_validate({**value, "source_id": None})


def test_direct_publication_cannot_drop_ownership_without_the_binding_receipt_abi():
    h, db, _, prior, _, _ = bound_worker()
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(971), kinds=("reconcile_state",),
        limit=1, per_workspace_limit=1,
    ))[0]
    request = prepare_connector_publication(
        controller, work, prior.connector_id, request_id=h.next_id(),
    )
    definition = deepcopy(request.desired_definition)
    definition["parts"]["eventstream.json"]["sources"] = []
    definition["parts"]["eventstream.json"]["streams"][0]["inputNodes"] = []
    removal = m.ConnectorPublicationRequest.model_validate({
        **request.model_dump(), "sources": (), "desired_definition": definition,
    })
    before = deepcopy(db.records)
    with pytest.raises(ProvisioningReview, match="source_ownership_must_be_retained_until_confirmation"):
        publish_connector_intent(controller, removal)
    assert db.records == before


@pytest.mark.parametrize("lost_ack", [False, True])
def test_sql_adapter_addition_and_original_observation_materialization(lost_ack):
    h, db, controller, work, frontier = original_setup_sql()
    current_result_fields(db)
    proposal = proposal_publication(h, work, frontier)
    planned = publish_connector_intent(controller, proposal).connector
    assert planned.sources == () and planned.source_proposals == proposal.source_proposals
    h.clock.advance(1)
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    observed = deepcopy(planned.desired_definition)
    source_id = uid(980)
    observed["component_ids"] = {
        f"sources/{planned.source_proposals[0].node_name}": source_id,
        "streams/owned-stream": uid(902),
        "destinations/owned-endpoint": uid(903),
    }
    reported = m.OwnedConnectorManifest.model_validate({
        **planned.model_dump(), "revision": planned.revision + 1,
        "workspace_id": uid(930), "eventstream_id": uid(931), "destination_id": uid(903),
        "endpoint": m.EndpointMetadata(
            namespace="fixture.example.invalid", entity="fixture", consumer_group="$Default",
        ),
        "observed_definition": observed, "state": "degraded",
        "gaps": (m.CoverageGap(
            code="controller_source_binding_required", detail="Actual fixture topology observed",
        ),),
        "updated_at": h.clock(),
    })
    saved = worker.record_connector(
        h.version, reported, expected_connector_revision=planned.revision,
        commit=connector_commit(h, worker, planned.connector_id, db=db),
    )
    original_id = stable_id(h.version, f"connector:{saved.connector_id}:{planned.revision}")
    db.principal = "controller"
    claimed = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(981), kinds=("reconcile_state",),
        limit=20, per_workspace_limit=20,
    ))
    binding_work = next(item for item in claimed if item.reconcile_request_id == original_id)
    request = prepare_connector_binding(
        controller, binding_work, saved.connector_id, request_id=h.next_id(),
    )
    if lost_ack:
        original = controller.publish_connector

        def uncertain(value):
            db.fail_commit = "after"
            return original(value)

        controller.publish_connector = uncertain
    result = publish_connector_intent(controller, request)
    assert result.connector.sources[0].source_id == source_id
    assert result.connector.source_proposals == ()
    assert result.connector.desired_definition == observed
    assert result.state == "provisioning"
    assert result.connector.delivery_verified_at is None
    assert result.connector.identity_verified_at is None
    assert publish_connector_intent(controller, request) == result
