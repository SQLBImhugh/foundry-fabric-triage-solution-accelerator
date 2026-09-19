from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace

import pytest
from pydantic import ValidationError
from test_monitoring_sql_action_bindings import ActionAbiDatabase
from test_monitoring_sql_retry_finalization import _eval, _json
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringUnavailable,
)
from triage.monitoring.deployment_schema import RegistrationNames, kernel_abi
from triage.monitoring.memory import InMemoryMonitoringStore, key_digest
from triage.monitoring.schema import permission_kernel_objects, resolve_kernel_tables
from triage.monitoring.sql_kernel_connectors import (
    PUBLICATION_FIELDS,
    initial_publication_invalid_sql,
    source_authorized_predicate,
)
from triage.monitoring.sql_kernel_contracts import KERNEL_VERSION
from triage.monitoring.sql_kernel_intake import (
    connector_collection_eligible_sql,
    connector_collection_work_invalid_sql,
)
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.monitoring.sql_store import AzureSqlMonitoringStore

EVENTS = ("Microsoft.Fabric.JobEvents.ItemJobCreated", "Microsoft.Fabric.JobEvents.ItemJobFailed")


def definition(target, *, source_name="owned-source"):
    return {
        "parts": {"eventstream.json": {
            "sources": [{
                "name": source_name, "type": "FabricJobEvents",
                "properties": {
                    "eventScope": "Item", "workspaceId": target.workspace_id, "itemId": target.item_id,
                    "includedEventTypes": list(EVENTS),
                },
            }],
            "operators": [],
            "streams": [{"name": "owned-stream", "type": "DefaultStream", "inputNodes": [{"name": source_name}]}],
            "destinations": [{"name": "owned-endpoint", "type": "CustomEndpoint", "inputNodes": [{"name": "owned-stream"}]}],
        }},
        "component_ids": {f"sources/{source_name}": uid(901), "streams/owned-stream": uid(902),
                          "destinations/owned-endpoint": uid(903)},
    }


def publication(h, work, frontier, *, connector_id=None, revision=0, readiness=None):
    target = h.targets[0]
    desired = definition(target)
    desired["component_ids"] = {}
    proposal = m.ConnectorSourceProposal(
        proposal_id=uid(901), node_name="owned-source", source_id=None, target=target,
        event_types=EVENTS, event_source="fixture:Source",
    )
    return m.ConnectorPublicationRequest(
        request_id=h.next_id(), expected=m.RegistryVersion(**h.context(), revision=work.policy_revision),
        work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
        expected_frontier_revision=frontier.accepted_revision,
        connector_id=connector_id or uid(910), ownership_id=uid(911), expected_connector_revision=revision,
        name="Explicit owned connector", sources=(), source_proposals=(proposal,),
        desired_definition=desired, readiness_receipt_id=readiness,
        detail="Publish the explicitly configured, already observed fixture source bindings.",
    )


class Review9Database(ActionAbiDatabase):
    """Exercise the actual boundary DTOs and generated review9 predicates offline."""

    def __init__(self, h):
        super().__init__(h)
        self.work_fences = {}
        self.fail_connector_receipt = False
        self.guards.create_function(
            "JSON_EQUAL", 2,
            lambda left, right: int(left is not None and right is not None and json.loads(left) == json.loads(right)),
        )

    def handoff(self, *args, **kwargs):
        return super().handoff(*args, **kwargs)

    @contextmanager
    def transaction(self):
        fences = dict(self.work_fences)
        try:
            with super().transaction():
                yield self
        except BaseException:
            self.work_fences = fences
            raise

    def apply_rpc(self, operation, args):
        if operation.endswith(".claim_work"):
            existing = self.model("work", args["work_id"], m.MonitoringWork)
            if existing is not None and (
                existing.state not in {"queued", "waiting", "leased", "finalizing"}
                or existing.due_at > self.clock()
                or existing.lease is not None and existing.lease.expires_at > self.clock()
            ):
                return {
                    "kernel_version": KERNEL_VERSION, "operation": operation, "status": "not_acquired",
                    "affected_rows": 0, "result": {"reason": "not_due_or_owned"},
                }
            reply = super().apply_rpc(operation, args)
            if reply["status"] != "not_acquired":
                work = m.MonitoringWork.model_validate(reply["result"]["work"])
                fence = self.work_fences.get(work.work_id, 0) + 1
                self.work_fences[work.work_id] = fence
                lease = work.lease.model_copy(update={"fence": fence})
                work = m.MonitoringWork.model_validate({**work.model_dump(), "lease": lease})
                self.save_work(work)
                reply["result"].update(work=work.model_dump(mode="json"), lease=lease.model_dump(mode="json"))
            return reply
        if operation == "controller.publish_connector":
            return self.publish_connector(args)
        if operation == "worker.observe_connector":
            return self.observe_connector(args)
        if operation == "controller.resolve_frontier":
            return self.resolve_frontier(args)
        return super().apply_rpc(operation, args)

    def owned(self, args):
        work = self.model("work", args["work_id"], m.MonitoringWork)
        if (
            work is None or work.kind != "reconcile_state" or work.lease is None
            or work.revision != args["work_revision"] or work.lease.owner_id != args["owner_id"]
            or work.lease.fence != args["fence"] or work.lease.expires_at <= self.clock()
            or args["expected_revision"] != self.control.revision
        ):
            raise RuntimeError("Current reconciliation/control fence changed (51074)")
        return work

    def publish_connector(self, args):
        work = self.owned(args)
        row = self.records[("connector_publication", args["publication_id"])]
        assert hashlib.sha256(row.payload.encode("utf-16-le")).hexdigest().upper() == args["publication_hash"]
        plan = m.ConnectorPublicationPlan.model_validate_json(row.payload)
        assert plan.work_id == work.work_id and plan.producer_request_id == work.reconcile_request_id
        frontier = self.model("validation_frontier", plan.frontier_key, m.ValidationFrontier)
        if frontier.accepted_revision != plan.frontier_revision:
            raise RuntimeError("Accepted frontier changed (51072)")
        prior = self.model("connector", plan.connector_id, m.OwnedConnectorManifest)
        desired_row = self.records.get(("connector_desired", plan.connector_id))
        if (prior.revision if prior else 0) != plan.expected_connector_revision:
            raise RuntimeError("Desired connector CAS changed (51072)")
        from test_monitoring_sql_removals import _adapt

        if self.guards.execute(
            "SELECT CASE WHEN " + _adapt(self, initial_publication_invalid_sql(self.names)) + " THEN 1 ELSE 0 END",
            {"prior": prior.model_dump_json() if prior else None,
             "desired": desired_row.payload if desired_row else None, "plan": plan.model_dump_json()},
        ).fetchone()[0]:
            raise RuntimeError("Initial publication is not an admitted proof-free registered baseline (51072)")
        sources, proposals, desired_definition = plan.sources, plan.source_proposals, plan.desired_definition
        old_pending = {item.removal_id: item for item in prior.source_removals} if prior else {}
        pending = []
        retirements = []
        for intent in plan.source_removals:
            existing = old_pending.get(intent.removal_id)
            if existing is None:
                binding, node_name = InMemoryMonitoringStore._removal_binding(prior, intent)
                existing = m.PendingSourceRemoval(
                    **intent.model_dump(), node_name=node_name, last_observed_source_id=None, target=binding.target,
                    binding_hash=hashlib.sha256(_json(binding.model_dump(mode="json")).encode("utf-16-le")).hexdigest().upper(),
                    policy_revision=self.control.revision, request_id=args["request_id"],
                    publication_id=args["publication_id"], requested_at=self.clock(), state="pending_remote_absence",
                )
            pending.append(existing)
        pending = tuple(sorted(pending, key=lambda item: item.removal_id))
        removing_sources = {item.source_id for item in pending if item.source_id is not None}
        removing_proposals = {item.proposal_id for item in pending if item.proposal_id is not None}
        if plan.observation_receipt_id is not None:
            if prior is None or plan.observation_receipt_id != work.reconcile_request_id:
                raise RuntimeError("Physical binding lost its current observation (51072)")
            report = self.receipts[("worker.observe_connector", plan.observation_receipt_id)]["payload"]["result"]
            observation = m.OwnedConnectorManifest.model_validate(report["observation"])
            assert observation.observed_definition["parts"] == plan.desired_definition["parts"]
            bound = [item for item in sources if item.source_id not in removing_sources]
            remaining = []
            for proposal in proposals:
                if proposal.proposal_id in removing_proposals:
                    continue
                identifier = observation.observed_definition.get("component_ids", {}).get(f"sources/{proposal.node_name}")
                if identifier is None:
                    remaining.append(proposal)
                else:
                    bound.append(m.ConnectorSource(
                        source_id=identifier, target=proposal.target, event_types=proposal.event_types,
                        event_source=proposal.event_source,
                    ))
            raw_receipt = self.receipts[("worker.observe_connector", plan.observation_receipt_id)]
            for removal in pending:
                binding, node_name = InMemoryMonitoringStore._removal_binding(prior, removal.intent())
                if f"sources/{node_name}" in observation.observed_definition["component_ids"] or (
                    removal.source_id is not None and removal.source_id in observation.observed_definition["component_ids"].values()
                ):
                    raise RuntimeError("Source remains remotely present (51072)")
                retirement = m.ConnectorSourceRetirement(
                    connector_id=plan.connector_id, ownership_id=plan.ownership_id,
                    removal_id=removal.removal_id, source_id=removal.source_id, proposal_id=removal.proposal_id,
                    node_name=node_name, original_binding=binding, original_removal=removal,
                    observation_receipt_id=plan.observation_receipt_id, observation_fingerprint=raw_receipt["fingerprint"],
                    observation_binding_hash=raw_receipt["payload"]["binding_hash"].upper(),
                    observation_receipt_hash=hashlib.sha256(_json(raw_receipt["payload"]).encode("utf-16-le")).hexdigest().upper(),
                    observed_definition_hash=report["observed_definition_hash"],
                    confirmation_request_id=args["request_id"], work_id=work.work_id, work_fence=work.lease.fence,
                    policy_revision=self.control.revision, retired_at=self.clock(), state="retired_verified",
                )
                self.native_put("connector_source_retirement", f"{plan.connector_id}:removal:{removal.removal_id}",
                                retirement.model_dump(mode="json"), parent_key=plan.connector_id)
                retirements.append(retirement)
            pending = ()
            sources, proposals, desired_definition = tuple(bound), tuple(remaining), observation.observed_definition
        elif sources and (prior is None or any(
            source.source_id not in {item.source_id for item in prior.sources} for source in sources
        )):
            raise RuntimeError("New physical sources require an observed proposal receipt (51072)")
        for source in (
            *(item for item in sources if item.source_id not in removing_sources),
            *(item for item in proposals if item.proposal_id not in removing_proposals),
        ):
            target = self.model("target", source.target.key, m.MonitoringTarget)
            capability = self.model("target_capability", source.target.key, m.CapabilityObservation)
            if not _eval(self.guards, source_authorized_predicate(), {
                "approved_target": _json(target.model_dump(mode="json")) if target else None,
                "source_capability": _json(capability.model_dump(mode="json")) if capability else None,
                "source_target": _json(source.target.model_dump(mode="json")),
                "current_revision": self.control.revision, "now": self.clock().isoformat(),
            }):
                raise RuntimeError("Desired source is not currently approved (51072)")
        changed = prior is None or desired_row is None or (
            prior.sources != sources or prior.source_proposals != proposals or prior.desired_definition != desired_definition
            or prior.source_removals != pending
            or prior.policy_revision != self.control.revision or prior.name != plan.name
        )
        if prior is None:
            current = m.OwnedConnectorManifest(
                **self.context.model_dump(), connector_id=plan.connector_id, ownership_id=plan.ownership_id,
                revision=1, policy_revision=self.control.revision, name=plan.name, sources=sources,
                source_proposals=proposals, source_removals=pending,
                desired_definition=desired_definition, state="planned", updated_at=self.clock(),
            )
        else:
            if prior.ownership_id != plan.ownership_id:
                raise RuntimeError("Desired connector owner cannot change (51072)")
            current = m.OwnedConnectorManifest.model_validate({
                **prior.model_dump(), "sources": sources, "source_proposals": proposals, "source_removals": pending, "name": plan.name,
                "desired_definition": desired_definition, "policy_revision": self.control.revision,
                "revision": prior.revision + 1, "updated_at": self.clock(),
                "state": "provisioning" if changed else prior.state,
                "identity_verified_at": None if changed else prior.identity_verified_at,
                "delivery_verified_at": None if changed else prior.delivery_verified_at,
                "delivery_proof": None if changed else prior.delivery_proof,
            })
        if plan.readiness_receipt_id is not None:
            if changed or prior is None or plan.readiness_receipt_id != work.reconcile_request_id:
                raise RuntimeError("Readiness must use exact current worker receipt (51072)")
            reported = self.receipts[("worker.observe_connector", plan.readiness_receipt_id)]["payload"]["result"]
            observation = m.OwnedConnectorManifest.model_validate(reported["observation"])
            desired = json.loads(self.records[("connector_desired", plan.connector_id)].payload)
            if (
                observation.state != "ready" or observation.observed_definition != current.desired_definition
                or observation.identity_verified_at.isoformat() < desired["published_at"]
                or observation.delivery_verified_at.isoformat() < desired["published_at"]
            ):
                raise RuntimeError("Readiness proof is old or different (51072)")
            current = m.OwnedConnectorManifest.model_validate({
                **current.model_dump(), "state": "ready", "identity_verified_at": observation.identity_verified_at,
                "delivery_verified_at": observation.delivery_verified_at,
                "delivery_proof": observation.delivery_proof,
            })
        self.native_put("connector", current.connector_id, current.model_dump(mode="json"), status=current.state)
        if changed:
            self.native_put("connector_desired", current.connector_id, m.ConnectorDesiredState(
                connector_id=current.connector_id, ownership_id=current.ownership_id,
                publication_id=args["publication_id"], policy_revision=self.control.revision,
                sources_hash=m._digest([value.model_dump(mode="json") for value in current.sources]),
                definition_hash=m.connector_definition_hash(current.desired_definition),
                published_at=self.clock(),
                supersession_request_id=(
                    json.loads(desired_row.payload).get("supersession_request_id") if desired_row else None
                ),
            ).model_dump(mode="json"))
        reply = self.reply("controller.publish_connector", args, {
            "connector_id": current.connector_id, "connector": current.model_dump(mode="json"),
            "state": current.state, "desired_changed": changed,
            "pending_removals": [value.model_dump(mode="json") for value in current.source_removals],
            "retired_sources": [value.model_dump(mode="json") for value in retirements],
            "observation_receipt_id": plan.observation_receipt_id,
        })
        if self.fail_connector_receipt:
            self.fail_connector_receipt = False
            raise RuntimeError("Injected publication receipt failure (51072)")
        return reply

    def observe_connector(self, args):
        work = self.model("work", args["work_id"], m.MonitoringWork)
        if (
            work is None or work.lease is None or work.lease.expires_at <= self.clock()
            or work.lease.owner_id != args["owner_id"] or work.lease.fence != args["fence"]
            or (work.tenant_id, work.epoch) != (args["tenant_id"], args["epoch"])
        ):
            raise RuntimeError("Connector observation work ownership was lost (51074)")
        if self.native_rows(f"SELECT CASE WHEN {connector_collection_work_invalid_sql()} THEN 1 ELSE 0 END", {
            **args, "stored_work_revision": work.revision, "stored_work_status": work.state,
            "stored_work_kind": work.kind, "stored_work": work.model_dump_json(),
            "stored_target_key": work.target.key if work.target else None,
        })[0][0]:
            raise RuntimeError("Connector observation requires its actual collection work (51074)")
        prior = self.model("connector", args["connector_id"], m.OwnedConnectorManifest)
        if (
            prior is None or prior.revision != args["expected_connector_revision"]
            or prior.ownership_id != args["ownership_id"] or prior.policy_revision != args["expected_revision"]
            or args["expected_revision"] != self.control.revision or self.control.maintenance
        ):
            raise RuntimeError("Connector observation CAS changed (51072)")
        patch = json.loads(args["observation_json"])
        inspection = patch.pop("inspection", None)
        if inspection is not None:
            inspection = m.ConnectorPresenceInspection.model_validate(inspection).model_dump(mode="json")
        for field in ("workspace_id", "eventstream_id", "destination_id", "endpoint"):
            value = prior.model_dump(mode="json")[field]
            if value is not None and patch[field] != value:
                raise RuntimeError("Established physical binding cannot be replaced (51072)")
        observed = m.OwnedConnectorManifest.model_validate({
            **prior.model_dump(mode="json"), **patch,
            "revision": prior.revision + 1, "updated_at": self.clock(),
        })
        effective = m.OwnedConnectorManifest.model_validate({
            **observed.model_dump(), "identity_verified_at": prior.identity_verified_at,
            "delivery_verified_at": prior.delivery_verified_at,
            "delivery_proof": prior.delivery_proof,
            "state": "provisioning" if observed.state == "ready" and prior.state != "ready" else observed.state,
        })
        self.native_put("connector", prior.connector_id, effective.model_dump(mode="json"), status=effective.state)
        return self.reply("worker.observe_connector", args, {
            "connector_id": prior.connector_id, "connector": effective.model_dump(mode="json"),
            "observation": observed.model_dump(mode="json"), "authority": "observed_not_action_authority",
            "observed_definition_hash": hashlib.sha256(_json(patch["observed_definition"]).encode("utf-16-le")).hexdigest().upper()
            if patch.get("observed_definition") is not None else None,
            "work_id": work.work_id, "work_owner_id": work.lease.owner_id,
            "work_fence": work.lease.fence, "work_revision": work.revision,
            "collection_completion_eligible": bool(self.native_rows(
                f"SELECT CASE WHEN {connector_collection_eligible_sql()} THEN 1 ELSE 0 END", args,
            )[0][0]),
            "inspection": inspection,
            **self.handoff("worker.observe_connector", args, topic="connector", reference=prior.connector_id),
        })

    def resolve_frontier(self, args):
        return self.reply("controller.resolve_frontier", args, self._resolve_frontier_result(args))


def setup_sql(*, database_type=Review9Database):
    h = Harness()
    h.seed()
    h.activate()
    db = database_type(h)
    db.principal = "web"
    web = AzureSqlMonitoringStore(db=db, component="web")
    queued = web.request_discovery(h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id())
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(920), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    assert work.work_id == queued.work_id
    producer = controller.get_reconciliation_request(h.version, work.reconcile_request_id, producer="web")
    frontier = controller.get_validation_frontier(h.version, producer.frontier_key)
    return h, db, controller, work, frontier


def connector_commit(h, worker, connector_id, *, db=None):
    """Lease a real, explicitly enqueued connector collection in this offline fixture."""
    if db:
        db.principal = "controller"
        controller = AzureSqlMonitoringStore(db=db, component="controller")
    else:
        controller = InMemoryMonitoringStore(state=h.state, clock=h.clock, component="controller")
    context = m.MonitoringContext(**h.context())
    control = controller.snapshot(context).control
    work = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), connector_id=connector_id, kind="connector_reconcile",
        policy_revision=control.revision, created_at=h.clock(), due_at=h.clock(),
        reason="Collect an explicit original fixture observation under this work lease.",
    ))
    if db:
        db.principal = "worker"
    claimed = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=h.owner, kinds=("connector_reconcile",), limit=200, per_workspace_limit=200,
    ))
    current, = [entry for entry in claimed if entry.work_id == work.work_id]
    return m.CollectionCommit(
        work_id=current.work_id, lease=current.lease, expected_work_revision=current.revision,
    )


def test_controller_connector_creation_is_planned_and_receipt_replays_before_revision_cas():
    h, db, store, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    result = store.publish_connector(request)
    assert result.state == "planned" and result.desired_changed
    assert result.connector.workspace_id is None and result.connector.endpoint is None
    assert result.connector.identity_verified_at is None and result.connector.delivery_verified_at is None
    assert store.get_connector_publication(h.version, request.request_id) == result
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2})
    assert store.publish_connector(request) == result
    assert not any("controller:" in str(params) for _, _, params in db.calls)


def test_worker_ready_observation_requires_receipt_bound_controller_publication():
    from test_monitoring_sql_receiver_bindings import ReceiverAbiDatabase, record_delivery_evidence

    h, db, store, work, frontier = setup_sql(database_type=ReceiverAbiDatabase)
    request = publication(h, work, frontier)
    created = store.publish_connector(request).connector
    store.reconcile_work(work)
    h.clock.advance(1)
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    reported = m.OwnedConnectorManifest.model_validate({
        **created.model_dump(), "revision": 2, "workspace_id": uid(930), "eventstream_id": uid(931),
        "destination_id": uid(903), "endpoint": m.EndpointMetadata(namespace="fixture.example.invalid", entity="fixture", consumer_group="$Default"),
        "observed_definition": definition(h.targets[0]), "state": "provisioning", "updated_at": h.clock(),
    })
    effective = worker.record_connector(
        h.version, reported, expected_connector_revision=created.revision,
        commit=connector_commit(h, worker, created.connector_id, db=db),
    )
    assert effective.state == "provisioning"
    assert effective.identity_verified_at is None and effective.delivery_verified_at is None
    db.principal = "controller"
    reconciliation = store.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(932), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    result = store.reconcile_work(reconciliation)
    assert result.state == "published"
    bound = next(item for item in store.list_connectors(m.PageQuery(**h.context())).items if item.connector_id == created.connector_id)
    assert bound.source_proposals == () and bound.sources[0].source_id == uid(901)
    assert bound.state == "provisioning"
    h.clock.advance(1)
    db.principal = "worker"
    bound, proof = record_delivery_evidence(h, worker, bound, db=db)
    effective = worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **bound.model_dump(), "revision": bound.revision + 1, "state": "ready",
        "identity_verified_at": h.clock(), "delivery_verified_at": h.clock(), "updated_at": h.clock(),
        "delivery_proof": proof,
    }), expected_connector_revision=bound.revision,
        commit=connector_commit(h, worker, bound.connector_id, db=db))
    assert effective.state == "provisioning" and effective.delivery_verified_at is None
    db.principal = "controller"
    reconciliation = store.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(932), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    assert store.reconcile_work(reconciliation).state == "published"
    ready = next(item for item in store.list_connectors(m.PageQuery(**h.context())).items if item.connector_id == created.connector_id)
    assert ready.state == "ready" and ready.delivery_verified_at == h.clock()
    assert ready.endpoint == effective.endpoint and ready.destination_id == effective.destination_id
    assert store.resolve_target(h.targets[0]).observation.events_enabled
    assert store.get_work(h.version, reconciliation.work_id).state == "completed"
    db.principal = "web"
    web = AzureSqlMonitoringStore(db=db, component="web")
    web.request_discovery(h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id())
    db.principal = "controller"
    next_work = store.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(933), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    producer = store.get_reconciliation_request(h.version, next_work.reconcile_request_id, producer="web")
    current_frontier = store.get_validation_frontier(h.version, producer.frontier_key)
    update = publication(h, next_work, current_frontier, connector_id=ready.connector_id, revision=ready.revision)
    update = m.ConnectorPublicationRequest.model_validate({
        **update.model_dump(), "name": "Revised owned label",
        "sources": ready.sources, "source_proposals": (), "desired_definition": ready.desired_definition,
    })
    changed = store.publish_connector(update)
    assert changed.state == "provisioning" and changed.desired_changed
    assert changed.connector.identity_verified_at is None and changed.connector.delivery_verified_at is None
    assert changed.connector.endpoint == ready.endpoint
    assert changed.connector.eventstream_id == ready.eventstream_id and changed.connector.destination_id == ready.destination_id
    assert not store.resolve_target(h.targets[0]).observation.events_enabled
    with pytest.raises(MonitoringConflict):
        store.publish_connector(m.ConnectorPublicationRequest.model_validate({
            **update.model_dump(), "request_id": h.next_id(),
            "expected_connector_revision": changed.connector.revision,
            "readiness_receipt_id": reconciliation.reconcile_request_id,
        }))


@pytest.mark.parametrize("change", ["owner", "revision", "scope", "lease"])
def test_connector_publication_denies_changed_owner_scope_or_fence(change):
    h, db, store, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    created = store.publish_connector(request).connector
    update = m.ConnectorPublicationRequest.model_validate({
        **request.model_dump(), "request_id": h.next_id(), "expected_connector_revision": created.revision,
    })
    if change == "owner":
        update = update.model_copy(update={"ownership_id": uid(999)})
    elif change == "revision":
        update = update.model_copy(update={"expected_connector_revision": 0})
    elif change == "lease":
        update = update.model_copy(update={"lease": update.lease.model_copy(update={"fence": 99})})
    else:
        row = db.records[("target", h.targets[0].key)]
        target = m.MonitoringTarget.model_validate_json(row.payload)
        paused = m.MonitoringTarget.model_validate({
            **target.model_dump(), "state": "paused", "observation": m.ObservationPolicy(), "action": m.ActionPolicy(),
        })
        db.records[("target", target.key)] = replace(row, payload=paused.model_dump_json())
    before = deepcopy(db.records)
    with pytest.raises(MonitoringConflict):
        store.publish_connector(update)
    assert db.records == before


@pytest.mark.parametrize("failure", ["before", "after"])
def test_connector_publication_lost_ack_keeps_original_work_binding(failure):
    h, db, store, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain):
        store.publish_connector(request)
    result = store.publish_connector(request)
    assert result.connector.revision == 1
    assert store.get_connector_publication(h.version, request.request_id) == result


def test_connector_receipt_failure_rolls_back_manifest_and_publication_plan():
    h, db, store, work, frontier = setup_sql()
    before = deepcopy(db.records)
    request = publication(h, work, frontier)
    db.fail_connector_receipt = True
    with pytest.raises(MonitoringConflict):
        store.publish_connector(request)
    assert db.records == before
    assert store.get_connector_publication(h.version, request.request_id) is None


def test_publication_model_forbids_caller_readiness_or_physical_binding_assertions():
    h, _, _, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    for field, value in (("state", "ready"), ("endpoint", {}), ("identity_verified_at", h.clock()), ("workspace_id", uid(999))):
        with pytest.raises(ValidationError):
            m.ConnectorPublicationRequest.model_validate({**request.model_dump(), field: value})


def test_worker_cannot_invoke_controller_publication():
    h, db, _, work, frontier = setup_sql()
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    with pytest.raises(MonitoringComponentDenied):
        worker.publish_connector(publication(h, work, frontier))


def test_component_argument_cannot_grant_sql_connector_publication_authority():
    h, db, store, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    before = deepcopy(db.records)
    db.principal = "worker"
    with pytest.raises(MonitoringComponentDenied):
        store.publish_connector(request)
    assert db.records == before


def test_review9_catalogue_and_hash_keep_registration_names_separate():
    tables = {"monitoring_records": "fixture_review9_records", "monitoring_rate_budget": "fixture_review9_budget"}
    kernel = build_permission_kernel(resolve_kernel_tables(tables=tables))
    objects = permission_kernel_objects(tables)
    assert any(item["logical_name"] == "controller.publish_connector" for item in objects)
    abi = kernel_abi(resolve_kernel_tables(tables=tables))
    digest = hashlib.sha256(json.dumps(abi, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    assert len(digest) == 64 and abi["namespace_suffix"] == kernel.names.suffix
    registration = RegistrationNames(registration="fixture_r9_reg", writers="fixture_r9_writers", read_projection="fixture_r9_authority")
    assert not set(registration.tables) & set(abi["table_map"])
    assert tuple(abi["objects"]) == objects
    assert set(m.ConnectorPublicationPlan.model_fields) == set(PUBLICATION_FIELDS)


def partial_inventory(h, controller, worker):
    draft = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=h.version.revision,
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=h.targets[0].workspace_id),
        created_at=h.clock(), due_at=h.clock(), reason="Explicit partial collection.",
    ))
    work = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(940), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))[0]
    generation = worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=draft.work_id, selector=draft.discovery_selector, adapter="fixture",
            authority="tenant_admin", completeness="partial", started_at=h.clock(), continuation="more",
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="Further pages are not yet observed."),),
        ),
        commit=m.InventoryCommit(work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
                                 expected_generation_revision=0),
    ))
    return work, generation


@pytest.fixture
def rejected_sql_window():
    h = Harness()
    h.seed()
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review())).reservation
    db = Review9Database(h)
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    draft = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=h.version.revision,
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=h.targets[0].workspace_id),
        created_at=h.clock(), due_at=h.clock(), reason="Partial inventory for window rejection.",
    ))
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    collection = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(941), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))[0]
    worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=draft.work_id, selector=draft.discovery_selector,
            adapter="fixture", authority="tenant_admin", completeness="partial",
            started_at=h.clock(), continuation="more",
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="The window remains unfinished."),),
        ),
        commit=m.InventoryCommit(work_id=collection.work_id, lease=collection.lease,
                                 expected_work_revision=collection.revision, expected_generation_revision=0),
    ))
    db.principal = "controller"
    original_work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(942), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    pending = controller.reconcile_work(original_work)
    assert pending.state == "pending_validation"
    old_receipt = deepcopy(db.receipts[("controller.resolve_frontier", pending.request_id)])
    handoff = next(row for row in db.records.values() if row.kind == "validation_handoff"
                   and json.loads(row.payload)["work_id"] == original_work.work_id)
    assert handoff.status == "published"
    original_action = db.records[("action", action.reservation_id)]
    original_budget = db.records[("incident_state", action.request.incident.key)]
    db.principal = "worker"
    prior_generation = worker.get_inventory_generation(h.version, draft.work_id)
    worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration.model_validate({
            **prior_generation.model_dump(), "completed_pages": 1, "continuation": "still-more",
        }),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
            expected_generation_revision=prior_generation.revision, expected_continuation=prior_generation.continuation,
        ),
    ))
    db.principal = "controller"
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2})
    h.clock.advance(16)
    current_work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(943), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    rejected = controller.reconcile_state(m.ReconcileStateRequest(
        **h.context(), request_id=h.next_id(), work_id=current_work.work_id, lease=current_work.lease,
        expected_work_revision=current_work.revision, expected_policy_revision=2,
        expected_frontier_revision=2, reject_whole_window=True,
        detail="The obsolete collection window is rejected under the current policy.",
    ))
    assert rejected.state == "rejected" and rejected.resolution_scope == "window"
    assert db.records[(handoff.kind, handoff.key)] == handoff
    assert db.receipts[("controller.resolve_frontier", pending.request_id)] == old_receipt
    assert db.records[("action", action.reservation_id)] == original_action
    assert db.records[("incident_state", action.request.incident.key)] == original_budget
    assert db.control.revision == 2
    assert controller.reconcile_work(original_work) == pending
    assert not controller.get_validation_frontier(h.version, rejected.frontier_key).pending
    assert current_work.work_id != original_work.work_id
    return h, db, controller, original_work, pending, rejected


def claim_sibling(h, controller):
    return controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(944), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]


def test_sql_policy_advance_rejects_window_without_rewriting_published_page_or_old_receipt(rejected_sql_window):
    h, db, controller, original_work, pending, rejected = rejected_sql_window
    sibling = claim_sibling(h, controller)
    assert sibling.work_id == original_work.work_id
    before_records = deepcopy(db.records)
    before_receipts = deepcopy(db.receipts)
    before_approvals = deepcopy(db.approvals)
    before_calls = len(db.calls)
    acknowledged = controller.reconcile_work(sibling)
    assert acknowledged.resolution_scope == "window_acknowledgement"
    assert acknowledged.window_rejection_request_id == rejected.request_id
    assert acknowledged.state == "rejected"
    assert controller.get_work(h.version, sibling.work_id).state == "completed"
    assert all(db.records[key] == row for key, row in before_records.items() if key != ("work", sibling.work_id))
    assert all(db.receipts[key] == receipt for key, receipt in before_receipts.items())
    assert db.approvals == before_approvals
    assert not any("reserve_action" in sql or "controller:" in sql or any(
        isinstance(value, str) and value.startswith("controller:") for value in params
    ) for _, sql, params in db.calls[before_calls:])
    assert sibling.target is None and sibling.execution is None and sibling.action_reservation_id is None
    assert controller.reconcile_work(original_work) == pending
    assert controller.reconcile_work(sibling) == acknowledged


@pytest.mark.parametrize("change", [
    {"frontier_key": "validation:other"},
    {"producer_request_id": uid(990)},
    {"window_rejection_request_id": uid(991)},
    {"resolution_scope": "handoff"},
    {"work_fence": 99},
])
def test_sql_sibling_rejects_a_native_reply_different_from_its_original_receipt(rejected_sql_window, monkeypatch, change):
    h, db, controller, _, _, _ = rejected_sql_window
    sibling = claim_sibling(h, controller)
    before = deepcopy((db.records, db.receipts, db.approvals))
    resolve = db.resolve_frontier

    def altered_reply(args):
        reply = deepcopy(resolve(args))
        reply["result"].update(change)
        return reply

    monkeypatch.setattr(db, "resolve_frontier", altered_reply)
    calls = len(db.calls)
    with pytest.raises(MonitoringUnavailable):
        controller.reconcile_work(sibling)
    assert (db.records, db.receipts, db.approvals) == before
    assert not any("controller_transition_work" in sql for _, sql, _ in db.calls[calls:])


@pytest.mark.parametrize("change", [
    {"frontier_key": "validation:other"},
    {"producer_request_id": uid(990)},
    {"window_rejection_request_id": uid(991)},
])
def test_sql_sibling_replay_requires_its_immutable_proof_and_exact_original_window_receipt(rejected_sql_window, change):
    h, db, controller, _, _, _ = rejected_sql_window
    sibling = claim_sibling(h, controller)
    acknowledged = controller.reconcile_work(sibling)
    db.receipts[("controller.resolve_frontier", acknowledged.request_id)]["payload"]["result"].update(change)
    before = deepcopy((db.records, db.receipts, db.approvals))
    fresh = AzureSqlMonitoringStore(db=db, component="controller")
    with pytest.raises(MonitoringUnavailable):
        fresh.reconcile_work(sibling)
    assert (db.records, db.receipts, db.approvals) == before


@pytest.mark.parametrize("change", [
    {"frontier_key": "validation:other"},
    {"frontier_revision": 3, "validated_revision": 3},
])
def test_sql_sibling_cannot_complete_against_a_different_root_window_or_prefix(rejected_sql_window, change):
    h, db, controller, _, _, rejected = rejected_sql_window
    sibling = claim_sibling(h, controller)
    db.receipts[("controller.resolve_frontier", rejected.request_id)]["payload"]["result"].update(change)
    before = deepcopy((db.records, db.receipts, db.approvals))
    with pytest.raises((MonitoringConflict, MonitoringUnavailable)):
        controller.reconcile_work(sibling)
    assert (db.records, db.receipts, db.approvals) == before


@pytest.mark.parametrize("guard", ["owner", "fence", "work_revision", "policy_revision", "frontier_revision"])
def test_sql_sibling_new_operation_requires_current_work_policy_and_frontier(rejected_sql_window, guard):
    h, db, controller, _, _, _ = rejected_sql_window
    sibling = claim_sibling(h, controller)
    request = m.ReconcileStateRequest(
        **h.context(), request_id=h.next_id(), work_id=sibling.work_id, lease=sibling.lease,
        expected_work_revision=sibling.revision, expected_policy_revision=2, expected_frontier_revision=2,
    )
    changes = {
        "owner": {"lease": sibling.lease.model_copy(update={"owner_id": uid(990)})},
        "fence": {"lease": sibling.lease.model_copy(update={"fence": sibling.lease.fence + 1})},
        "work_revision": {"expected_work_revision": sibling.revision - 1},
        "policy_revision": {"expected_policy_revision": 1},
        "frontier_revision": {"expected_frontier_revision": 1},
    }
    before = deepcopy((db.records, db.receipts, db.approvals))
    with pytest.raises((MonitoringConflict, MonitoringLeaseLost, MonitoringUnavailable)):
        controller.reconcile_state(m.ReconcileStateRequest.model_validate({**request.model_dump(), **changes[guard]}))
    assert (db.records, db.receipts, db.approvals) == before


@pytest.mark.parametrize("failure", ["before", "after"])
def test_sql_sibling_lost_ack_recovery_keeps_both_original_receipts(rejected_sql_window, failure):
    h, db, controller, original_work, pending, rejected = rejected_sql_window
    sibling = claim_sibling(h, controller)
    before = deepcopy((db.records, db.receipts, db.approvals))
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain) as uncertain:
        controller.reconcile_work(sibling)
    assert uncertain.value.operation == "controller.resolve_frontier"
    if failure == "before":
        assert (db.records, db.receipts, db.approvals) == before
    else:
        assert controller.get_work(h.version, sibling.work_id).state == "completed"
        db.control = m.DeploymentControl.model_validate({
            **db.control.model_dump(), "revision": 3, "maintenance": True,
        })
        h.clock.advance(600)
    fresh = AzureSqlMonitoringStore(db=db, component="controller")
    recovered = fresh.reconcile_work(sibling)
    assert recovered.request_id == uncertain.value.idempotency_id
    assert recovered.window_rejection_request_id == rejected.request_id
    assert recovered.state == "rejected" and recovered.resolution_scope == "window_acknowledgement"
    assert fresh.reconcile_work(original_work) == pending
    assert all(db.receipts[key] == receipt for key, receipt in before[1].items())
    assert db.approvals == before[2]


@pytest.fixture
def rejected_memory_window():
    h = Harness()
    h.seed()
    h.activate()
    controller = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    worker = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="worker")
    collection, generation = partial_inventory(h, controller, worker)
    first = claim_sibling(h, controller)
    pending = controller.reconcile_work(first)
    worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration.model_validate({
            **generation.model_dump(), "completed_pages": 1, "continuation": "still-more",
        }),
        commit=m.InventoryCommit(
            work_id=collection.work_id, lease=collection.lease, expected_work_revision=collection.revision,
            expected_generation_revision=generation.revision, expected_continuation=generation.continuation,
        ),
    ))
    h.state.control_row["revision"] = 2
    h.clock.advance(16)
    current = claim_sibling(h, controller)
    rejected = controller.reconcile_state(m.ReconcileStateRequest(
        **h.context(), request_id=h.next_id(), work_id=current.work_id, lease=current.lease,
        expected_work_revision=current.revision, expected_policy_revision=2, expected_frontier_revision=2,
        reject_whole_window=True, detail="Reject the obsolete unfinished collection window.",
    ))
    assert current.work_id != first.work_id and rejected.resolution_scope == "window"
    return h, controller, first, pending, rejected


def test_memory_sibling_acknowledgement_preserves_original_window_page_and_receipts(rejected_memory_window):
    h, controller, first, pending, rejected = rejected_memory_window
    sibling = claim_sibling(h, controller)
    before_records, before_receipts = deepcopy((h.state.records, h.state.receipts))
    result = controller.reconcile_work(sibling)
    assert result.state == "rejected" and result.resolution_scope == "window_acknowledgement"
    assert result.window_rejection_request_id == rejected.request_id
    assert controller.get_work(h.version, sibling.work_id).state == "completed"
    assert all(h.state.records[key] == row for key, row in before_records.items()
               if not (row.kind == "work" and row.key == sibling.work_id))
    assert all(h.state.receipts[key] == receipt for key, receipt in before_receipts.items())
    h.state.control_row.update(revision=3, maintenance=True)
    h.clock.advance(600)
    fresh = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    assert fresh.reconcile_work(first) == pending
    assert fresh.reconcile_work(sibling) == result


@pytest.mark.parametrize("corruption", ["missing_receipt", "wrong_frontier", "wrong_prefix", "different_receipt"])
def test_memory_sibling_cannot_trust_a_window_marker_without_exact_original_receipt(rejected_memory_window, corruption):
    h, controller, _, _, rejected = rejected_memory_window
    sibling = claim_sibling(h, controller)
    marker_key = (uid(1), uid(2), "window_resolution", key_digest(rejected.frontier_key))
    receipt_key = (uid(1), uid(2), "reconciliation", key_digest(rejected.request_id))
    if corruption == "missing_receipt":
        h.state.receipts.pop(receipt_key)
    elif corruption == "different_receipt":
        receipt = h.state.receipts[receipt_key]
        value = json.loads(receipt.payload)
        value["work_id"] = uid(991)
        h.state.receipts[receipt_key] = replace(receipt, payload=json.dumps(value))
    else:
        marker = h.state.records[marker_key]
        value = json.loads(marker.payload)
        value.update({"frontier_key": "validation:other"} if corruption == "wrong_frontier" else {"frontier_revision": 3})
        h.state.records[marker_key] = replace(marker, payload=json.dumps(value))
    before = deepcopy((h.state.records, h.state.receipts))
    with pytest.raises(MonitoringUnavailable):
        controller.reconcile_work(sibling)
    assert (h.state.records, h.state.receipts) == before


def test_sql_sibling_rejects_explicit_repeat_of_whole_window_rejection(rejected_sql_window):
    h, db, controller, _, _, _ = rejected_sql_window
    sibling = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(944), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringConflict, match="unfinished"):
        controller.reconcile_state(m.ReconcileStateRequest(
            **h.context(), request_id=h.next_id(), work_id=sibling.work_id, lease=sibling.lease,
            expected_work_revision=sibling.revision, expected_policy_revision=2, expected_frontier_revision=2,
            reject_whole_window=True, detail="An existing whole-window decision must not be rewritten.",
        ))
    assert (db.records, db.receipts) == before


def test_memory_whole_window_rejection_preserves_the_original_page_record():
    h = Harness()
    h.seed()
    h.activate()
    controller = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    worker = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="worker")
    partial_inventory(h, controller, worker)
    first = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(950), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    pending = controller.reconcile_work(first)
    prior_page = h.state.records[(uid(1), uid(2), "reconcile_acceptance", key_digest(first.reconcile_request_id))]
    prior_receipt = h.state.receipts[(uid(1), uid(2), "reconciliation", key_digest(pending.request_id))]
    h.state.control_row["revision"] = 2
    h.clock.advance(16)
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(951), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    request = m.ReconcileStateRequest(
        **h.context(), request_id=h.next_id(), work_id=work.work_id, lease=work.lease,
        expected_work_revision=work.revision, expected_policy_revision=2, expected_frontier_revision=1,
        reject_whole_window=True, detail="Reject the unfinished obsolete window, not its recorded page decision.",
    )
    result = controller.reconcile_state(request)
    assert result.resolution_scope == "window" and result.state == "rejected"
    assert h.state.records[(uid(1), uid(2), "reconcile_acceptance", key_digest(first.reconcile_request_id))] == prior_page
    assert h.state.receipts[(uid(1), uid(2), "reconciliation", key_digest(pending.request_id))] == prior_receipt
    assert controller.reconcile_state(request) == result


def test_memory_worker_observation_cannot_publish_new_readiness():
    from test_monitoring_sql_receiver_bindings import record_delivery_evidence

    h = Harness()
    h.seed()
    h.activate()
    web = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="web")
    controller = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    worker = InMemoryMonitoringStore(clock=h.clock, state=h.state, component="worker")
    web.request_discovery(h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id())
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(952), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    producer = controller.get_reconciliation_request(h.version, work.reconcile_request_id, producer="web")
    frontier = controller.get_validation_frontier(h.version, producer.frontier_key)
    created = controller.publish_connector(publication(h, work, frontier)).connector
    controller.reconcile_work(work)
    h.clock.advance(1)
    reported = m.OwnedConnectorManifest.model_validate({
        **created.model_dump(), "revision": 2, "workspace_id": uid(960), "eventstream_id": uid(961),
        "destination_id": uid(903), "endpoint": m.EndpointMetadata(namespace="fixture.example.invalid", entity="fixture", consumer_group="$Default"),
        "observed_definition": definition(h.targets[0]), "state": "provisioning", "updated_at": h.clock(),
    })
    effective = worker.record_connector(
        h.version, reported, expected_connector_revision=1,
        commit=connector_commit(h, worker, created.connector_id),
    )
    assert effective.state == "provisioning" and effective.delivery_verified_at is None
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(953), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    assert controller.reconcile_work(work).state == "published"
    bound = next(value for value in controller.list_connectors(m.PageQuery(**h.context())).items if value.connector_id == created.connector_id)
    assert bound.source_proposals == () and bound.sources[0].source_id == uid(901)
    h.clock.advance(1)
    bound, proof = record_delivery_evidence(h, worker, bound)
    effective = worker.record_connector(h.version, m.OwnedConnectorManifest.model_validate({
        **bound.model_dump(), "revision": bound.revision + 1, "state": "ready",
        "identity_verified_at": h.clock(), "delivery_verified_at": h.clock(), "updated_at": h.clock(),
        "delivery_proof": proof,
    }), expected_connector_revision=bound.revision,
        commit=connector_commit(h, worker, bound.connector_id))
    assert effective.state == "provisioning" and effective.delivery_verified_at is None
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(953), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    assert controller.reconcile_work(work).state == "published"
    ready = next(value for value in controller.list_connectors(m.PageQuery(**h.context())).items if value.connector_id == created.connector_id)
    assert ready.state == "ready" and ready.delivery_verified_at == h.clock()


@pytest.mark.parametrize("extra", [{"state": "ready"}, {"workspace_id": uid(999)}, {"identity_verified_at": "2035-01-01T12:00:00Z"}])
def test_publication_request_rejects_authority_fields_before_store_dispatch(extra):
    h, _, _, work, frontier = setup_sql()
    request = publication(h, work, frontier)
    with pytest.raises(ValidationError):
        m.ConnectorPublicationRequest.model_validate({**request.model_dump(), **extra})


def test_identity_values_are_not_case_folded_or_sql_padding_normalized_by_models():
    base = {"tenant_id": uid(1), "epoch": uid(2), "connector_id": uid(3)}
    first = m.TransportDeliveryIdentity(**base, event_source="Source-A", event_id="Event-A")
    changed_case = m.TransportDeliveryIdentity(**base, event_source="Source-A", event_id="event-a")
    changed_source = m.TransportDeliveryIdentity(**base, event_source="source-a", event_id="Event-A")
    assert len({first.key, changed_case.key, changed_source.key}) == 3
    assert first.model_dump(mode="json")["event_source"] == "Source-A"
    for value in ("Event-A ", " Event-A", "Event-A\t"):
        with pytest.raises(ValidationError):
            m.TransportDeliveryIdentity(**base, event_source="Source-A", event_id=value)


def test_controller_comparable_record_json_uses_the_same_canonical_order_as_rpc_arguments():
    h = Harness()
    h.seed()
    h.activate()
    for row in h.state.records.values():
        assert row.payload == _json(json.loads(row.payload))
