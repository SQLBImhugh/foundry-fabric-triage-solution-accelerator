from __future__ import annotations

import hashlib
import json
from collections import Counter
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from triage.approvals import ApprovalRequest
from triage.models import Incident
from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringNotBootstrapped,
    MonitoringStore,
)
from triage.monitoring.controller import MonitoringExecution
from triage.monitoring.events import (
    ConnectorScope,
    EventPersistence,
    OwnershipChange,
    ReceiverHeartbeat,
    StreamStartRequest,
    UnidentifiedReceiptBatch,
    UnidentifiedSignal,
)
from triage.monitoring.memory import (
    InMemoryMonitoringState,
    InMemoryMonitoringStore,
    MonitoringEngine,
    canonical_incident_id,
    key_digest,
    stable_id,
)
from triage.pipeline_models import PipelineActivity
from triage.policy import REMEDIATION_ACTIONS, PolicyLedger, PolicyViolation, TriagePolicy
from triage.store.approvals import _request_row
from triage.store.retries import MAX_ATTEMPTS, backoff_seconds
from triage.tools.powerbi import MockPowerBIClient, RefreshOutcome


def uid(value: int) -> str:
    return str(UUID(int=value))


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2035, 1, 1, 12, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class Harness:
    def __init__(self, store: MonitoringEngine | None = None, clock: Clock | None = None) -> None:
        self.clock = clock or Clock()
        self.control = m.DeploymentControl(
            tenant_id=uid(1), epoch=uid(2), revision=0,
            activation_cutoff=self.clock() - timedelta(hours=1),
            maintenance=False, updated_at=self.clock(),
        )
        self.state = InMemoryMonitoringState.empty(self.control)
        self.store = store or InMemoryMonitoringStore(clock=self.clock, state=self.state)
        self.version = m.RegistryVersion(**self.context(), revision=0)
        self.owner = uid(3)
        self.sequence = 10_000
        self.targets: list[m.TargetIdentity] = []
        self.definition_hash = hashlib.sha256(b"fixture definition").hexdigest()

    def context(self) -> dict[str, str]:
        return {"tenant_id": self.control.tenant_id, "epoch": self.control.epoch}

    def next_id(self) -> str:
        self.sequence += 1
        return uid(self.sequence)

    def seed(self, count: int = 1, workspaces: int = 1, workload: m.Workload = "fabric_pipeline") -> None:
        self.generation_id = self.next_id()
        self.targets = [
            m.TargetIdentity(
                **self.context(), workload=workload, workspace_id=uid(100 + index % workspaces),
                item_id=uid(1_000 + index),
            ) for index in range(count)
        ]
        items = tuple(m.InventoryItem(
            **self.context(), generation_id=self.generation_id, workspace_id=target.workspace_id,
            item_id=target.item_id, name="Same fixture label",
            item_type="DataPipeline" if workload == "fabric_pipeline" else "SemanticModel",
            workload=workload, observed_at=self.clock() - timedelta(seconds=1),
            definition_hash=self.definition_hash,
        ) for target in self.targets)
        self.record_inventory(m.InventoryBatch(
            request_id=self.next_id(), expected=self.version,
            generation=m.InventoryGeneration(
                **self.context(), generation_id=self.generation_id,
                selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
                adapter="fixture_inventory_adapter", authority="tenant_admin",
                completeness="complete", started_at=self.clock() - timedelta(seconds=2),
                completed_at=self.clock(), discovered_count=count, completed_pages=1,
            ),
            items=items,
        ))
        for target in self.targets:
            self.capability(target)

    def record_inventory(self, batch: m.InventoryBatch) -> m.InventoryGeneration:
        return self.store.record_inventory(batch)

    def capability(self, target: m.TargetIdentity, **changes: object) -> m.CapabilityObservation:
        observation = m.CapabilityObservation.model_validate({
            "capability_id": self.next_id(), "target": target,
            "inventory_generation": self.generation_id, "collector_identity_id": uid(4),
            "read_status": "verified", "event_status": "verified", "action_status": "verified",
            "exact_action_correlation": True, "configuration_verification": target.workload == "powerbi",
            "definition_hash": self.definition_hash, "checked_at": self.clock(),
            "expires_at": self.clock() + timedelta(hours=1), **changes,
        })
        return self.store.record_capability(self.version, observation)

    def activate(self, definition: m.ScopeDefinition | None = None) -> m.ActivationReceipt:
        self.scope = definition or m.ScopeDefinition(
            **self.context(), scope_id=uid(20), name="Fixture scope",
            rules=(m.ScopeRule(
                rule_id=uid(21), selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
                effect="include",
            ),),
        )
        self.plan = self.store.preview_scope(m.ScopePreviewRequest(
            expected=self.version, idempotency_id=self.next_id(), scope=self.scope,
        ))
        self.activation_request = m.ActivateScopeRequest(
            expected=self.version, plan_id=self.plan.plan_id, idempotency_id=self.plan.idempotency_id,
        )
        receipt = self.store.activate_scope(self.activation_request)
        self.version = receipt.version
        return receipt

    def observation(self, target: m.TargetIdentity | None = None, **changes: object) -> m.SourceRunObservation:
        target = target or self.targets[0]
        return m.SourceRunObservation.model_validate({
            "execution": {
                "target": target, "run_id": uid(30_000),
                "run_id_kind": "fabric_job" if target.workload == "fabric_pipeline" else "powerbi_request",
            },
            "origin": "poll", "authority": "rest", "status": "failed", "invocation": "scheduled",
            "job_type": "Pipeline" if target.workload == "fabric_pipeline" else None,
            "started_at": self.clock() - timedelta(minutes=10),
            "ended_at": self.clock() - timedelta(minutes=5), "observed_at": self.clock(),
            "failure_signature": "fixture-failure", **changes,
        })

    def claim(self, kinds: tuple[m.WorkKind, ...], limit: int = 1) -> tuple[m.MonitoringWork, ...]:
        return self.store.claim_work(m.WorkClaimRequest(
            **self.context(), owner_id=self.owner, kinds=kinds, limit=limit,
            per_workspace_limit=min(2, limit), lease_seconds=120,
        ))

    def source_work(self, observation: m.SourceRunObservation | None = None) -> m.MonitoringWork:
        observation = observation or self.observation()
        draft = m.MonitoringWorkDraft(
            **self.context(), work_id=self.next_id(), kind="triage", policy_revision=self.version.revision,
            due_at=self.clock(), created_at=self.clock(), reason="Fixture exact failure.",
            target=observation.execution.target, execution=observation.execution,
        )
        self.store.enqueue_work(draft)
        work = self.claim(("triage",))[0]
        self.store.observe_source(observation, work_id=work.work_id, lease=work.lease)
        self.source = observation
        self.work = work
        return work

    def review(self, action: m.ActionKind = "pipeline_rerun", parameters: dict | None = None) -> m.SafetyReview:
        desired = parameters if parameters is not None else {}
        configuration = action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}
        review = m.SafetyReview(
            review_id=self.next_id(), target=self.targets[0], revision=1, policy_revision=self.version.revision,
            action=action, state="verified", reviewer_id=uid(5), reviewed_at=self.clock(),
            expires_at=self.clock() + timedelta(hours=1), definition_hash=self.definition_hash,
            parameters=desired, replay_safe=True, exact_correlation_verified=not configuration,
            configuration_hash=m._digest(desired) if configuration else None,
            detail="Explicit synthetic safety review.",
        )
        return self.store.record_safety_review(m.SafetyReviewRequest(
            request_id=self.next_id(), expected=self.version, expected_review_revision=0, review=review,
        ))

    def reserve_request(
        self, review: m.SafetyReview, *, approval: bool = True, **changes: object,
    ) -> m.ActionReservationRequest:
        identity = m.IncidentIdentity.model_validate(changes.get(
            "incident", {"target": self.source.execution.target, "signature": "fixture-failure"},
        ))
        state = self.store.get_incident_state(identity)
        request = m.ActionReservationRequest.model_validate({
            "idempotency_id": self.next_id(), "expected": self.version,
            "work_id": self.work.work_id, "lease": self.work.lease,
            "source_execution": self.source.execution, "incident": identity,
            "expected_incident_revision": state.revision if state else 0, "action": review.action,
            "review_id": review.review_id, "expected_review_revision": review.revision,
            "definition_hash": review.definition_hash, "configuration_hash": review.configuration_hash,
            "parameter_hash": review.parameter_hash, "arguments": {}, **changes,
        })
        if approval:
            proposal = ApprovalRequest(
                action=m.ACTION_TO_TOOL[review.action], arguments=request.arguments,
                justification="Approve this fixture action.", request_id=self.next_id(),
                signature=identity.signature, requested_at=self.clock(), timeout_seconds=3_600,
            )
            row = _request_row(proposal)
            self.seed_approval(row)
            request = m.ActionReservationRequest.model_validate({
                **request.model_dump(), "approval": {
                    "approval_id": proposal.request_id, "fingerprint": proposal.fingerprint,
                },
            })
            self.store.bind_approval(request)
            row.update(decision="approve", responder="fixture-reviewer", decided_at=self.clock().isoformat())
            self.decide_approval(row)
        return request

    def seed_approval(self, row: dict) -> None:
        self.state.approvals[row["request_id"]] = deepcopy(row)

    def decide_approval(self, row: dict) -> None:
        self.state.approvals[row["request_id"]] = deepcopy(row)

    def connector(
        self, *, endpoint: m.EndpointMetadata | None = None, destination_id: str = "fixture-destination",
    ) -> m.OwnedConnectorManifest:
        self.connector_id = stable_id(self.version, f"connector-target:{self.targets[0].key}")
        prior = next(connector for connector in self.store.list_connectors(m.PageQuery(**self.context())).items if connector.connector_id == self.connector_id)
        manifest = m.OwnedConnectorManifest(
            **self.context(), connector_id=self.connector_id, ownership_id=prior.ownership_id, revision=prior.revision + 1,
            policy_revision=self.version.revision, workspace_id=uid(42), eventstream_id=uid(43),
            destination_id=destination_id, name="Fixture connector",
            sources=(m.ConnectorSource(
                source_id="fixture-source", target=self.targets[0],
                event_types=("ItemJobFailed", "ItemJobCompleted"), event_source="fixture:jobs",
            ),),
            desired_definition={"sources": ["fixture-source"]},
            observed_definition={"sources": ["fixture-source"]},
            endpoint=endpoint or m.EndpointMetadata(
                namespace="fixture.example.invalid", entity="fixture", consumer_group="$Default",
            ),
            state="ready", updated_at=self.clock(), identity_verified_at=self.clock(),
            delivery_verified_at=self.clock(), last_receiver_activity_at=self.clock(),
        )
        return self.store.record_connector(self.version, manifest, expected_connector_revision=prior.revision)

    def signal(self, sequence: int, observation: m.SourceRunObservation | None = None) -> m.SignalReceipt:
        observation = observation or self.observation()
        self.partition = m.PartitionIdentity(
            **self.context(), connector_id=self.connector_id, consumer_group="$Default", partition_id="0",
        )
        return m.SignalReceipt(
            delivery=m.TransportDeliveryIdentity(
                **self.context(), connector_id=self.connector_id, event_source="fixture:jobs", event_id=f"fixture-event-{sequence}",
            ),
            partition=self.partition,
            position=m.StreamPosition(offset=str(sequence * 10), sequence_number=sequence, enqueued_at=self.clock()),
            event_type="ItemJobFailed", received_at=self.clock(), status="accepted",
            observation=m.SourceRunObservation.model_validate({
                **observation.model_dump(), "origin": "event", "authority": "transport",
            }),
        )

    def start_partition(self, *, first_sequence_number: int, lease_seconds: int = 120) -> m.LeaseToken:
        lease = self.store.claim_partition(m.PartitionClaimRequest(
            partition=self.partition, owner_id=self.owner, lease_seconds=lease_seconds,
        ))
        self.store.ensure_stream_start(StreamStartRequest(
            partition=self.partition, lease=lease, first_available_sequence_number=first_sequence_number,
            observed_at=self.clock(),
        ))
        return lease

    def finalization_input(
        self, *, action: m.ActionReservation | None = None, outcome: str = "needs_human",
        source_disposition: str = "triaged",
    ) -> m.WorkFinalizationRequest:
        work = self.store.get_work(m.MonitoringContext(**self.context()), self.work.work_id)
        identity = m.IncidentIdentity(target=self.source.execution.target, signature="fixture-failure")
        request = m.WorkFinalizationRequest(
            **self.context(), finalization_id=self.next_id(), work_id=work.work_id,
            expected_work_revision=work.revision, lease=work.lease,
            source_execution=self.source.execution, incident_identity=identity,
            incident=Incident(
                id="caller-label-is-not-identity", signature=identity.signature,
                outcome=outcome, status="resolved" if outcome == "resolved" else "open",
                original_error="Synthetic evidence to persist.", first_seen_at=self.clock().isoformat(),
                last_seen_at=self.clock().isoformat(),
            ),
            source_disposition=source_disposition, action_reservation_id=action.reservation_id if action else None,
        )
        return request

    def finalize(
        self, *, action: m.ActionReservation | None = None, outcome: str = "needs_human",
        source_disposition: str = "triaged",
    ) -> m.FinalizationReceipt:
        self.finalization_request = self.finalization_input(
            action=action, outcome=outcome, source_disposition=source_disposition,
        )
        return self.store.finalize_work(self.finalization_request)


def owned_inventory_start(h: Harness) -> tuple[m.MonitoringWork, m.InventoryGeneration]:
    selector = m.ScopeSelector(tenant_id=h.control.tenant_id, kind="tenant")
    queued = h.store.request_discovery(h.version, selector, request_id=h.next_id())
    work = h.claim(("inventory",))[0]
    assert work.work_id == queued.work_id
    generation = m.InventoryGeneration(
        **h.context(), generation_id=work.work_id, selector=selector, adapter="fenced_fixture_collector",
        authority="tenant_admin", completeness="partial", started_at=h.clock(), continuation="source-page-1",
        gaps=(m.CoverageGap(code="inventory_in_progress", detail="Collection is in progress."),),
    )
    committed = h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=generation, items=(),
        commit=m.InventoryCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_generation_revision=0,
        ),
    ))
    return work, committed


def inventory_update_batch(
    h: Harness, work: m.MonitoringWork, generation: m.InventoryGeneration, *, complete: bool = False,
) -> m.InventoryBatch:
    proposed = m.InventoryGeneration.model_validate({
        **generation.model_dump(), "completeness": "complete" if complete else "partial",
        "completed_at": h.clock() if complete else None,
        "continuation": None if complete else generation.continuation,
        "gaps": [] if complete else [m.CoverageGap(code="http_403", detail="Current collector access was denied.")],
    })
    return m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=proposed, items=(),
        commit=m.InventoryCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_generation_revision=generation.revision,
            expected_continuation=generation.continuation,
        ),
    )


def powerbi_staged_page(
    h: Harness, work: m.MonitoringWork, observation: m.SourceRunObservation,
    *, checkpoint: m.RestCheckpoint | None = None, final: bool = False, refresh_id: str = "23",
) -> m.RestPageRequest:
    return m.RestPageRequest(
        page_id=h.next_id(), target=work.target, policy_revision=work.policy_revision,
        poll_work_id=work.work_id, lease=work.lease,
        expected_checkpoint_revision=checkpoint.revision if checkpoint else 0,
        expected_cursor=checkpoint.cursor if checkpoint else None,
        next_cursor=None if final else "second-page",
        window=checkpoint.window if checkpoint else m.ObservationWindow(
            start_at=h.clock() - timedelta(minutes=15), end_at=h.clock(),
        ),
        received_count=1, powerbi_rows=(m.PowerBIWindowRow(observation=observation, refresh_id=refresh_id),),
        powerbi_window_complete=final, window_complete=final, observed_at=h.clock(),
    )


@pytest.fixture
def harness() -> Harness:
    result = Harness()
    result.seed()
    result.activate()
    return result


def test_all_protocol_operations_are_implemented_and_missing_bootstrap_is_visible() -> None:
    clock = Clock()
    empty = InMemoryMonitoringStore(clock=clock)
    assert isinstance(empty, MonitoringStore)
    assert empty.inspect_bootstrap(expected_tenant_id=uid(1)).status == "missing"
    with pytest.raises(MonitoringNotBootstrapped):
        empty.snapshot(m.MonitoringContext(tenant_id=uid(1), epoch=uid(2)))
    h = Harness()
    snapshot = h.store.snapshot(m.MonitoringContext(**h.context()))
    assert snapshot.coverage.discovered_count == 0
    assert snapshot.coverage.scope_item_count == 0
    work = h.store.request_discovery(
        h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id(),
    )
    assert work.kind == "inventory"
    assert work.discovery_selector == m.ScopeSelector(tenant_id=uid(1), kind="tenant")
    assert h.store.list_scopes(m.PageQuery(**h.context())).items == ()


def test_inventory_work_requires_a_real_scope_when_no_selector_is_supplied() -> None:
    h = Harness()
    fields = {
        **h.context(), "kind": "inventory", "policy_revision": 0, "created_at": h.clock(),
        "due_at": h.clock(), "reason": "Refresh a saved monitoring scope.",
    }
    with pytest.raises(MonitoringConflict, match="absent saved scope"):
        h.store.enqueue_work(m.MonitoringWorkDraft(**fields, work_id=h.next_id(), scope_id=uid(999)))
    h.seed()
    h.activate()
    request = m.MonitoringWorkDraft.model_validate({
        **fields, "work_id": h.next_id(), "policy_revision": h.version.revision, "scope_id": h.scope.scope_id,
    })
    assert h.store.enqueue_work(request).scope_id == h.scope.scope_id


def test_request_discovery_replays_original_revision_after_scope_activation() -> None:
    h = Harness()
    original_version = h.version
    selected = m.ScopeSelector(tenant_id=uid(1), kind="tenant")
    request_id = h.next_id()
    queued = h.store.request_discovery(original_version, selected, request_id=request_id)
    h.seed()
    h.activate()
    h.clock.advance(1)
    replayed = h.store.request_discovery(original_version, selected, request_id=request_id)
    assert replayed == queued
    assert replayed.discovery_selector == selected
    with pytest.raises(MonitoringConflict):
        h.store.request_discovery(
            original_version, m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=uid(100)),
            request_id=request_id,
        )


def test_verified_requester_is_retained_in_plan_and_activation_audit() -> None:
    h = Harness()
    h.seed()
    scope = m.ScopeDefinition(
        **h.context(), scope_id=uid(20), name="Audited scope",
        rules=(m.ScopeRule(
            rule_id=uid(21), effect="include", selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        ),),
    )
    request = m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=scope, requested_by=uid(77),
    )
    plan = h.store.preview_scope(request)
    assert plan.requested_by == uid(77)
    assert h.store.get_plan(m.MonitoringContext(**h.context()), plan.plan_id).requested_by == uid(77)
    activation = m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=request.idempotency_id,
    )
    receipt = h.store.activate_scope(activation)
    assert receipt.requested_by == uid(77)
    assert h.store.activate_scope(activation) == receipt
    audit = h.store.get_operation_receipt(m.MonitoringContext(**h.context()), "activation", request.idempotency_id)
    assert audit.result["requested_by"] == uid(77)
    with pytest.raises(MonitoringConflict):
        h.store.preview_scope(m.ScopePreviewRequest.model_validate({
            **request.model_dump(), "requested_by": uid(78),
        }))


def test_independent_instances_share_scopes_targets_and_revision_conflicts(harness: Harness) -> None:
    h = harness
    other = InMemoryMonitoringStore(clock=h.clock, state=h.state)
    assert other.resolve_target(h.targets[0]).identity == h.targets[0]
    assert other.list_scopes(m.PageQuery(**h.context())).items[0].name == "Fixture scope"
    assert other.activate_scope(h.activation_request) == h.store.get_activation(
        m.MonitoringContext(**h.context()), h.activation_request.idempotency_id,
    )
    with pytest.raises(MonitoringConflict):
        other.preview_scope(m.ScopePreviewRequest(
            expected=m.RegistryVersion(**h.context(), revision=0),
            idempotency_id=h.next_id(), scope=h.scope,
        ))


def test_pagination_is_stable_and_binds_query_and_revision() -> None:
    h = Harness()
    h.seed(count=7, workspaces=2)
    first = h.store.list_inventory(m.TargetQuery(**h.context(), limit=3))
    second = h.store.list_inventory(m.TargetQuery(**h.context(), limit=3, cursor=first.next_cursor))
    assert len(first.items) == len(second.items) == 3
    assert {item.item_id for item in first.items}.isdisjoint(item.item_id for item in second.items)
    with pytest.raises(MonitoringConflict):
        h.store.list_inventory(m.TargetQuery(**h.context(), limit=3, cursor=first.next_cursor, workspace_id=uid(100)))
    h.activate()
    with pytest.raises(MonitoringConflict):
        h.store.list_inventory(m.TargetQuery(**h.context(), limit=3, cursor=first.next_cursor))


def test_unfinished_inventory_never_becomes_complete_empty_or_deletes_known_items(harness: Harness) -> None:
    h = harness
    h.clock.advance(1)
    generation = m.InventoryGeneration(
        **h.context(), generation_id=h.next_id(), selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        adapter="fixture", authority="tenant_admin", completeness="partial", started_at=h.clock(),
        continuation="next-page", gaps=(m.CoverageGap(code="throttled", detail="Continue later."),),
    )
    h.store.record_inventory(m.InventoryBatch(request_id=h.next_id(), expected=h.version, generation=generation, items=()))
    assert h.store.list_inventory(m.TargetQuery(**h.context())).items[0].state == "present"
    coverage = h.store.coverage(m.MonitoringContext(**h.context()))
    assert coverage.inventory_completeness == "partial"
    assert coverage.scope_item_count is None
    assert coverage.discovered_count == 1
    broken = m.InventoryGeneration.model_validate({
        **generation.model_dump(), "generation_id": h.next_id(), "completeness": "complete",
        "completed_at": h.clock(), "continuation": None, "gaps": [], "discovered_count": 1,
    })
    with pytest.raises(MonitoringConflict):
        h.store.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version, generation=broken, items=(),
        ))


def test_initial_discovery_is_partial_before_any_scope_is_configured() -> None:
    h = Harness()
    generation = m.InventoryGeneration(
        **h.context(), generation_id=h.next_id(),
        selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        adapter="fixture", authority="tenant_admin", completeness="partial",
        started_at=h.clock(), continuation="next-page",
        gaps=(m.CoverageGap(code="inventory_in_progress", detail="First tenant discovery is running."),),
    )
    h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=generation, items=(),
    ))
    coverage = h.store.coverage(m.MonitoringContext(**h.context()))
    assert coverage.inventory_completeness == "partial"
    assert coverage.scope_item_count is None
    assert any(gap.code == "inventory_incomplete" for gap in coverage.gaps)


def test_complete_workspace_preview_does_not_certify_partial_estate_coverage() -> None:
    h = Harness()
    h.seed()
    selector = m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=h.targets[0].workspace_id)
    h.activate(m.ScopeDefinition(
        **h.context(), scope_id=h.next_id(), name="One reviewed workspace",
        rules=(m.ScopeRule(rule_id=h.next_id(), selector=selector, effect="include"),),
    ))
    h.clock.advance(1)
    h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=h.next_id(),
            selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            adapter="fixture", authority="tenant_admin", completeness="partial",
            started_at=h.clock(), continuation="more-workspaces",
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="Other workspaces remain."),),
        ),
    ))
    h.clock.advance(1)
    generation_id = h.next_id()
    item = h.store.list_inventory(m.TargetQuery(**h.context())).items[0]
    h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version,
        items=(item.model_copy(update={"generation_id": generation_id, "observed_at": h.clock()}),),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=generation_id, selector=selector,
            adapter="fixture", authority="tenant_admin", completeness="complete",
            started_at=h.clock(), completed_at=h.clock(), completed_pages=1, discovered_count=1,
        ),
    ))
    coverage = h.store.coverage(m.MonitoringContext(**h.context()))
    assert coverage.inventory_completeness == "partial"
    assert coverage.scope_item_count is None
    assert coverage.discovered_count == 1
    preview = h.store.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=h.scope,
    ))
    assert preview.status == "ready"
    assert preview.inventory_completeness == "complete"


def test_scope_exclusions_win_and_names_do_not_alias() -> None:
    h = Harness()
    h.seed(count=2, workspaces=2)
    definition = m.ScopeDefinition(
        **h.context(), scope_id=uid(20), name="Fixture scope",
        rules=(
            m.ScopeRule(rule_id=uid(21), selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"), effect="include"),
            m.ScopeRule(rule_id=uid(22), selector=m.ScopeSelector(
                tenant_id=uid(1), kind="workspace", workspace_id=h.targets[0].workspace_id,
            ), effect="exclude"),
        ),
    )
    h.activate(definition)
    assert h.store.resolve_target(h.targets[0]) is None
    assert h.store.resolve_target(h.targets[1]) is not None
    assert h.targets[0].key != h.targets[1].key


def test_domain_descendants_use_explicit_membership() -> None:
    h = Harness()
    parent, child = uid(50), uid(51)
    item = m.InventoryItem(
        **h.context(), generation_id=uid(52), workspace_id=uid(100), item_id=uid(1_000),
        name="Fixture", item_type="DataPipeline", workload="fabric_pipeline",
        domain_ids=(child,), domain_ancestor_ids=(parent,), observed_at=h.clock(),
    )
    assert not h.store._selector_matches(m.ScopeSelector(tenant_id=uid(1), kind="domain", domain_id=parent), item)
    assert h.store._selector_matches(m.ScopeSelector(
        tenant_id=uid(1), kind="domain", domain_id=parent, include_descendants=True,
    ), item)


def test_named_workspaces_and_domains_are_persisted_without_item_counts() -> None:
    h = Harness()
    generation_id = h.next_id()
    h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=generation_id,
            selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"), adapter="fixture",
            authority="tenant_admin", completeness="complete", started_at=h.clock(), completed_at=h.clock(),
            enumeration="workspaces", discovered_count=1, completed_pages=1,
        ), items=(),
        workspaces=(m.InventoryWorkspace(
            **h.context(), generation_id=generation_id, workspace_id=uid(100),
            name="Named fixture workspace", domain_id=uid(50), observed_at=h.clock(),
        ),),
        domains=(m.InventoryDomain(
            **h.context(), generation_id=generation_id, domain_id=uid(50),
            name="Named fixture domain", observed_at=h.clock(),
        ),),
    ))
    assert h.store.list_workspaces(m.PageQuery(**h.context())).items[0].name == "Named fixture workspace"
    assert h.store.list_domains(m.PageQuery(**h.context())).items[0].name == "Named fixture domain"
    assert h.store.coverage(m.MonitoringContext(**h.context())).discovered_count == 0


def test_generation_catalogue_survives_overlapping_latest_projection_updates() -> None:
    h = Harness()
    generation_a, generation_b = h.next_id(), h.next_id()
    for generation_id, suffix in ((generation_a, "A"), (generation_b, "B")):
        h.store.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version, items=(),
            generation=m.InventoryGeneration(
                **h.context(), generation_id=generation_id,
                selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
                adapter="catalogue_fixture", authority="tenant_admin", enumeration="domains",
                completeness="complete", started_at=h.clock(), completed_at=h.clock(),
                discovered_count=2, completed_pages=1,
            ),
            domains=tuple(m.InventoryDomain(
                **h.context(), generation_id=generation_id, domain_id=uid(50 + index),
                name=f"Domain {index} {suffix}", observed_at=h.clock(),
            ) for index in range(2)),
        ))
        if suffix == "A":
            first = h.store.list_domains(m.PageQuery(**h.context(), limit=1), generation_id=generation_a)
        h.clock.advance(1)
    second = h.store.list_domains(
        m.PageQuery(**h.context(), limit=1, cursor=first.next_cursor), generation_id=generation_a,
    )
    assert {first.items[0].name, second.items[0].name} == {"Domain 0 A", "Domain 1 A"}
    assert {item.name for item in h.store.list_domains(m.PageQuery(**h.context())).items} == {"Domain 0 B", "Domain 1 B"}
    with pytest.raises(MonitoringConflict):
        h.store.list_domains(m.PageQuery(**h.context()), generation_id=uid(999))


@pytest.mark.parametrize("terminal", [False, True])
def test_stale_inventory_worker_cannot_erase_replacement_gap_or_terminal_state(terminal: bool) -> None:
    h = Harness()
    old_work, initial = owned_inventory_start(h)
    stale_success = inventory_update_batch(h, old_work, initial, complete=True)
    h.clock.advance(121)
    h.owner = uid(99)
    replacement = h.claim(("inventory",))[0]
    denied = h.store.record_inventory(inventory_update_batch(h, replacement, initial))
    assert denied.revision == initial.revision + 1
    if terminal:
        late_same_owner = inventory_update_batch(h, replacement, denied, complete=True)
        h.store.disposition_work(m.WorkDispositionRequest(
            **h.context(), request_id=h.next_id(), work_id=replacement.work_id,
            expected_work_revision=replacement.revision, lease=replacement.lease,
            disposition="superseded", detail="The denied collector was explicitly stopped.",
        ))
        with pytest.raises(MonitoringLeaseLost):
            h.store.record_inventory(late_same_owner)
    with pytest.raises(MonitoringLeaseLost):
        h.store.record_inventory(stale_success)
    retained = h.store.get_inventory_generation(m.MonitoringContext(**h.context()), initial.generation_id)
    assert retained.revision == denied.revision
    assert retained.continuation == initial.continuation
    assert retained.completeness == "partial"
    assert [gap.code for gap in retained.gaps] == ["http_403"]


def test_inventory_position_compare_and_set_rejects_same_owner_stale_page() -> None:
    h = Harness()
    work, initial = owned_inventory_start(h)
    stale_success = inventory_update_batch(h, work, initial, complete=True)
    denied = h.store.record_inventory(inventory_update_batch(h, work, initial))
    with pytest.raises(MonitoringConflict, match="revision or continuation"):
        h.store.record_inventory(stale_success)
    assert h.store.get_inventory_generation(m.MonitoringContext(**h.context()), initial.generation_id) == denied


def test_powerbi_window_blocks_action_even_if_another_entry_point_supplies_source_evidence() -> None:
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    poll = h.claim(("poll",))[0]
    observation = h.observation()
    first = h.store.record_rest_page(powerbi_staged_page(h, poll, observation))
    assert first.intake.work_ids == ()
    queued = h.store.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="triage", policy_revision=h.version.revision,
        target=h.targets[0], execution=observation.execution, created_at=h.clock(), due_at=h.clock(),
        reason="An independent operator supplied an exact source request.",
    ))
    claimed = h.store.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=h.owner, kinds=("triage",), limit=2, per_workspace_limit=2,
    ))
    h.work = next(work for work in claimed if work.work_id == queued.work_id)
    h.source = observation
    h.store.observe_source(observation, work_id=h.work.work_id, lease=h.work.lease)
    request = h.reserve_request(h.review("powerbi_refresh"), approval=False)
    denied = h.store.reserve_action(request)
    assert denied.status == "denied"
    assert "alias validation" in denied.detail
    assert h.store.get_incident_state(request.incident) is None


def test_future_targets_default_to_review_and_auto_enrol_never_copies_action_review(harness: Harness) -> None:
    h = harness
    h.review()
    h.clock.advance(1)
    generation_id = h.next_id()
    new_target = m.TargetIdentity(**h.context(), workload="fabric_pipeline", workspace_id=uid(100), item_id=uid(2_000))
    generation = m.InventoryGeneration(
        **h.context(), generation_id=generation_id,
        selector=m.ScopeSelector(tenant_id=uid(1), kind="item", workspace_id=uid(100), item_id=uid(2_000)),
        adapter="fixture", authority="tenant_admin", completeness="complete",
        started_at=h.clock(), completed_at=h.clock(), discovered_count=1, completed_pages=1,
    )
    h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=generation,
        items=(m.InventoryItem(
            **h.context(), generation_id=generation_id, workspace_id=uid(100), item_id=uid(2_000),
            name="New fixture item", item_type="DataPipeline", workload="fabric_pipeline", observed_at=h.clock(),
        ),),
    ))
    target = h.store.resolve_target(new_target, include_inactive=True)
    assert target.state == "review_required"
    assert target.action.review_id is None
    assert not target.action.enabled


def test_fair_claims_cover_200_targets_across_ten_workspaces_without_duplicates() -> None:
    h = Harness()
    h.seed(count=200, workspaces=10)
    h.activate()
    counts = Counter()
    claimed_ids = set()
    for _ in range(10):
        batch = h.claim(("poll",), limit=20)
        assert len(batch) == 20
        assert max(Counter(work.target.workspace_id for work in batch).values()) == 2
        for work in batch:
            assert work.work_id not in claimed_ids
            claimed_ids.add(work.work_id)
            counts[work.target.workspace_id] += 1
            request = m.RestPageRequest(
                page_id=h.next_id(), target=work.target, policy_revision=h.version.revision,
                poll_work_id=work.work_id, lease=work.lease, expected_checkpoint_revision=0,
                window=m.ObservationWindow(start_at=h.clock() - timedelta(minutes=15), end_at=h.clock()),
                received_count=0, observations=(), window_complete=True, observed_at=h.clock(),
            )
            h.store.record_rest_page(request)
    assert len(claimed_ids) == 200
    assert set(counts.values()) == {20}


def test_expired_owner_cannot_renew_or_disposition_new_owner_work(harness: Harness) -> None:
    h = harness
    first = h.source_work()
    h.clock.advance(121)
    h.owner = uid(99)
    second = h.claim(("triage",))[0]
    assert second.work_id == first.work_id
    assert second.lease.fence > first.lease.fence
    with pytest.raises(MonitoringLeaseLost):
        h.store.renew_lease(m.LeaseRenewal(lease=first.lease))
    with pytest.raises(MonitoringLeaseLost):
        h.store.observe_source(h.source, work_id=first.work_id, lease=first.lease)


def test_rest_page_atomic_intake_and_duplicate_reconciliation(harness: Harness) -> None:
    h = harness
    poll = h.claim(("poll",))[0]
    request = m.RestPageRequest(
        page_id=h.next_id(), target=h.targets[0], policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=0,
        window=m.ObservationWindow(start_at=h.clock() - timedelta(minutes=15), end_at=h.clock()),
        received_count=1, observations=(h.observation(),), window_complete=True, observed_at=h.clock(),
    )
    receipt = h.store.record_rest_page(request)
    assert len(receipt.intake.work_ids) == 1
    assert receipt.checkpoint.coverage_through == request.window.end_at
    assert h.store.record_rest_page(request) == receipt
    assert h.store.get_work(m.MonitoringContext(**h.context()), poll.work_id).state == "completed"
    with pytest.raises(MonitoringConflict):
        h.store.record_rest_page(m.RestPageRequest.model_validate({
            **request.model_dump(), "window_complete": False,
        }))


def test_partial_rest_page_has_continuation_but_no_complete_watermark(harness: Harness) -> None:
    h = harness
    poll = h.claim(("poll",))[0]
    request = m.RestPageRequest(
        page_id=h.next_id(), target=h.targets[0], policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=0,
        window=m.ObservationWindow(start_at=h.clock() - timedelta(minutes=15), end_at=h.clock()),
        received_count=0, next_cursor="page-2", observed_at=h.clock(),
    )
    partial = h.store.record_rest_page(request)
    assert partial.checkpoint.coverage_through is None
    assert h.store.get_work(m.MonitoringContext(**h.context()), poll.work_id).state == "leased"
    with pytest.raises(MonitoringConflict):
        h.store.record_rest_page(m.RestPageRequest.model_validate({
            **request.model_dump(), "page_id": h.next_id(), "expected_cursor": "wrong", "next_cursor": None,
            "expected_checkpoint_revision": 1,
        }))


def test_stream_gap_duplicate_and_poll_event_convergence(harness: Harness) -> None:
    h = harness
    h.connector()
    zero, two = h.signal(0), h.signal(2)
    lease = h.start_partition(first_sequence_number=0)
    batch = m.StreamReceiptBatch(request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(zero, two))
    receipt = h.store.record_stream_receipts(batch)
    assert len(receipt.work_ids) == 1
    assert h.store.record_stream_receipts(batch) == receipt
    advance = m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=lease, expected_revision=0, through=two.position,
    )
    with pytest.raises(MonitoringConflict):
        h.store.advance_stream_checkpoint(advance)
    h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(h.signal(1),),
    ))
    checkpoint = h.store.advance_stream_checkpoint(advance)
    assert checkpoint.position.sequence_number == 2
    assert h.store.get_work(m.MonitoringContext(**h.context()), receipt.work_ids[0]).state == "queued"
    poll = h.claim(("poll",))[0]
    page = m.RestPageRequest(
        page_id=h.next_id(), target=h.targets[0], policy_revision=h.version.revision,
        poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=0,
        window=m.ObservationWindow(start_at=h.clock() - timedelta(minutes=15), end_at=h.clock()),
        received_count=1, observations=(h.observation(),), window_complete=True, observed_at=h.clock(),
    )
    assert h.store.record_rest_page(page).intake.work_ids == receipt.work_ids


def test_wrong_stream_provenance_is_durably_quarantined_not_executed(harness: Harness) -> None:
    h = harness
    h.connector()
    signal = h.signal(0)
    signal = m.SignalReceipt.model_validate({
        **signal.model_dump(), "delivery": {
            **signal.delivery.model_dump(), "event_source": "fixture:unowned",
        },
    })
    lease = h.start_partition(first_sequence_number=0)
    result = h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(signal,),
    ))
    assert result.work_ids == ()
    assert any(
        record.kind == "signal" and json.loads(record.payload)["status"] == "quarantined"
        for record in h.state.records.values()
    )
    assert h.store.advance_stream_checkpoint(m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=lease, expected_revision=0, through=signal.position,
    )).position.sequence_number == 0


@pytest.mark.parametrize("reason", ["missing", "declined", "expired", "consumed", "fingerprint", "arguments"])
def test_approval_denial_consumes_neither_budget_nor_reservation(harness: Harness, reason: str) -> None:
    h = harness
    h.source_work()
    review = h.review()
    request = h.reserve_request(review)
    row = h.state.approvals[request.approval.approval_id]
    if reason == "missing":
        h.state.approvals.pop(request.approval.approval_id)
    elif reason == "declined":
        row["decision"] = "decline"
    elif reason == "expired":
        row["expires_at"] = h.clock().isoformat()
    elif reason == "consumed":
        row["consumed_at"] = h.clock().isoformat()
    elif reason == "fingerprint":
        row["fingerprint"] = "different"
    else:
        row["arguments"] = {"unapproved": True}
    result = h.store.reserve_action(request)
    assert result.status == "denied"
    assert h.store.get_incident_state(request.incident) is None
    assert not any(record.kind == "action" for record in h.state.records.values())


def test_reserved_action_and_approval_consumption_are_one_idempotent_unit(harness: Harness) -> None:
    h = harness
    h.source_work()
    request = h.reserve_request(h.review())
    decision = h.store.reserve_action(request)
    assert decision.status == "reserved"
    assert h.state.approvals[request.approval.approval_id]["consumed_at"]
    assert h.store.get_incident_state(request.incident).action_count == 1
    assert h.store.reserve_action(request) == decision
    assert h.store.get_incident_state(request.incident).action_count == 1
    with pytest.raises(MonitoringConflict):
        h.store.reserve_action(m.ActionReservationRequest.model_validate({
            **request.model_dump(), "arguments": {"different": True},
        }))


def test_scope_revocation_wins_without_consuming_approval(harness: Harness) -> None:
    h = harness
    h.source_work()
    request = h.reserve_request(h.review())
    h.activate(m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}))
    result = h.store.reserve_action(request)
    assert result.denial == "stale_policy"
    assert not h.state.approvals[request.approval.approval_id]["consumed_at"]
    assert h.store.get_incident_state(request.incident) is None


def test_reservation_wins_then_disabled_scope_cannot_erase_its_fence(harness: Harness) -> None:
    h = harness
    h.source_work()
    request = h.reserve_request(h.review())
    action = h.store.reserve_action(request).reservation
    h.activate(m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}))
    submission = m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence, state="uncertain",
        submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(seconds=120),
        detail="Submission acknowledgement was lost.",
    )
    uncertain = h.store.record_action_submission(submission)
    assert uncertain.state == "uncertain"
    h.finalize(action=uncertain)
    h.clock.advance(121)
    followup = h.claim(("verify_action",))[0]
    assert followup.action_reservation_id == action.reservation_id
    assert h.store.get_incident_state(request.incident).action_count == 1


def test_powerbi_non_job_actions_are_not_refresh_aliases_and_require_approval() -> None:
    assert set(m.ACTION_TO_TOOL.values()) == REMEDIATION_ACTIONS
    for action in ("rebind_dataset_gateway", "reenable_refresh_schedule"):
        h = Harness()
        h.seed(workload="powerbi")
        h.activate()
        h.source_work()
        desired = {"gateway_id": uid(60), "datasource_ids": [uid(61)]} if action == "rebind_dataset_gateway" else {"enabled": True}
        review = h.review(action, desired)
        if action == "reenable_refresh_schedule":
            head = h.observation(
                execution=m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="powerbi_request", run_id=uid(31_000)),
                status="succeeded", started_at=h.clock() - timedelta(minutes=2), ended_at=h.clock() - timedelta(minutes=1),
            )
            h.store.observe_source(head, work_id=h.work.work_id, lease=h.work.lease)
        denied = h.store.reserve_action(h.reserve_request(review, approval=False))
        assert denied.denial == "approval_required"
        request = h.reserve_request(review)
        decision = h.store.reserve_action(request)
        assert decision.status == "reserved"
        action_record = decision.reservation
        submitted = h.store.record_action_submission(m.ActionSubmissionRequest(
            **h.context(), request_id=h.next_id(), reservation_id=action_record.reservation_id,
            expected_reservation_revision=1, action_fence=action_record.fence,
            state="submitted", configuration_action=action, submitted_at=h.clock(),
            next_verification_at=h.clock() + timedelta(seconds=30), detail="Exact configuration mutation accepted.",
        ))
        outcome = h.store.record_action_outcome(m.ActionOutcomeRequest(
            **h.context(), request_id=h.next_id(), reservation_id=submitted.reservation_id,
            expected_reservation_revision=submitted.revision, action_fence=submitted.fence,
            disposition="verified_succeeded", observed_at=h.clock(), detail="Exact configuration verified.",
            configuration=m.ConfigurationVerification(
                target=h.targets[0], action=action, expected_hash=review.configuration_hash,
                configuration=desired, observed_at=h.clock(), authority="rest",
            ),
        ))
        assert outcome.submitted_execution is None
        assert outcome.configuration.matches
        receipt = h.finalize(action=outcome, outcome="resolved")
        assert h.store.get_work(m.MonitoringContext(**h.context()), h.work.work_id).finalization_id == receipt.finalization_id


def test_exact_pipeline_verification_rejects_an_unrelated_concurrent_job(harness: Harness) -> None:
    h = harness
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review())).reservation
    submitted_id = m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="fabric_job", run_id=uid(32_000))
    action = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state="submitted", submitted_execution=submitted_id, correlation="response_run_id",
        submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(seconds=30), detail="Known rerun.",
    ))
    h.clock.advance(2)
    wrong_id = m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="fabric_job", run_id=uid(33_000))
    observed = h.observation(
        execution=wrong_id, status="succeeded", started_at=h.clock() - timedelta(seconds=1), ended_at=h.clock(),
    )
    request = m.ActionOutcomeRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        disposition="verified_succeeded", submitted_execution=wrong_id, observation=observed,
        activities=(PipelineActivity(name="Conditional", status="Skipped"),),
        activities_complete=True, observed_at=h.clock(), detail="Unrelated job.",
    )
    with pytest.raises(MonitoringConflict):
        h.store.record_action_outcome(request)
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id).state == "submitted"


def test_incident_finalization_is_atomic_and_returns_utf16_payload_hash(harness: Harness) -> None:
    h = harness
    h.source_work()
    receipt = h.finalize()
    payload = h.state.incidents[receipt.incident_id]
    assert hashlib.sha256(payload.encode("utf-16-le")).hexdigest() == receipt.incident_payload_hash
    assert h.store.get_work(m.MonitoringContext(**h.context()), h.work.work_id).state == "completed"
    assert key_digest(h.source.key) in h.state.processed
    assert h.store.finalize_work(h.finalization_request) == receipt
    assert h.store.get_incident(h.finalization_request.incident_identity).occurrence_count == 1


def test_finalization_write_failure_rolls_back_incident_processed_and_work(harness: Harness, monkeypatch) -> None:
    h = harness
    h.source_work()
    original = h.store._backend.put
    def fail_after_source(record):
        original(record)
        if record.kind == "source_disposition":
            raise RuntimeError("Fixture persistence failure")
    monkeypatch.setattr(h.store._backend, "put", fail_after_source)
    with pytest.raises(RuntimeError, match="Fixture persistence failure"):
        h.finalize()
    assert h.state.incidents == {}
    assert h.state.processed == {}
    assert h.store.get_work(m.MonitoringContext(**h.context()), h.work.work_id).state == "leased"


def test_reservation_write_failure_rolls_back_real_approval_consumption(harness: Harness, monkeypatch) -> None:
    h = harness
    h.source_work()
    request = h.reserve_request(h.review())
    original = h.store._backend.put
    def fail_action(record):
        original(record)
        if record.kind == "action":
            raise RuntimeError("Fixture action write failed")
    monkeypatch.setattr(h.store._backend, "put", fail_action)
    with pytest.raises(RuntimeError):
        h.store.reserve_action(request)
    assert not h.state.approvals[request.approval.approval_id]["consumed_at"]
    assert h.store.get_incident_state(request.incident) is None


def test_store_redacts_only_at_persistence_and_returned_json_is_detached(harness: Harness) -> None:
    h = harness
    h.store._redactor = lambda value: value.replace("SYNTHETIC_PRIVATE", "[REDACTED]")
    observation = h.observation(evidence={"note": "SYNTHETIC_PRIVATE"})
    h.source_work(observation)
    loaded = h.store.get_source(observation.execution)
    assert observation.evidence["note"] == "SYNTHETIC_PRIVATE"
    assert "SYNTHETIC_PRIVATE" not in loaded.model_dump_json()
    loaded.evidence["note"] = "Mutated by reader"
    assert h.store.get_source(observation.execution).evidence["note"] != "Mutated by reader"
    reviewed = h.review(parameters={"region": "SYNTHETIC_PRIVATE"})
    assert reviewed.state == "unverifiable"
    assert reviewed.parameters_redacted
    assert not h.store.resolve_target(h.targets[0]).action.enabled


def test_approval_cannot_be_rebound_to_a_new_review_after_human_decision(harness: Harness) -> None:
    h = harness
    h.source_work()
    review = h.review()
    request = h.reserve_request(review)
    updated_review = m.SafetyReview.model_validate({**review.model_dump(), "revision": 2})
    h.store.record_safety_review(m.SafetyReviewRequest(
        request_id=h.next_id(), expected=h.version, expected_review_revision=1, review=updated_review,
    ))
    changed_request = m.ActionReservationRequest.model_validate({
        **request.model_dump(), "idempotency_id": h.next_id(), "expected_review_revision": 2,
    })
    result = h.store.reserve_action(changed_request)
    assert result.denial == "fingerprint_mismatch"
    assert not h.state.approvals[request.approval.approval_id]["consumed_at"]
    with pytest.raises(MonitoringConflict):
        h.store.bind_approval(changed_request)


@pytest.mark.parametrize("action", tuple(m.ACTION_TO_TOOL))
def test_every_remediation_respects_observation_only_admission(action: m.ActionKind) -> None:
    h = Harness()
    h.seed(workload="fabric_pipeline" if action == "pipeline_rerun" else "powerbi")
    h.activate()
    h.source_work()
    identity = m.IncidentIdentity(target=h.targets[0], signature="fixture-failure")
    parameters = {"enabled": True} if action in {
        "rebind_dataset_gateway", "reenable_refresh_schedule",
    } else {}
    request = m.ActionReservationRequest(
        idempotency_id=h.next_id(), expected=h.version, work_id=h.work.work_id, lease=h.work.lease,
        source_execution=h.source.execution, incident=identity, expected_incident_revision=0,
        action=action, review_id=h.next_id(), expected_review_revision=1,
        definition_hash=h.definition_hash, parameter_hash=m._digest(parameters),
        configuration_hash=m._digest(parameters) if action in {
            "rebind_dataset_gateway", "reenable_refresh_schedule",
        } else None,
    )
    assert h.store.reserve_action(request).denial == "observation_only"
    assert h.store.get_incident_state(identity) is None


def test_expired_review_disables_projection_and_does_not_spend_approval(harness: Harness) -> None:
    h = harness
    h.source_work()
    review = h.review()
    short = m.SafetyReview.model_validate({
        **review.model_dump(), "revision": 2, "expires_at": h.clock() + timedelta(seconds=60),
    })
    h.store.record_safety_review(m.SafetyReviewRequest(
        request_id=h.next_id(), expected=h.version, expected_review_revision=1, review=short,
    ))
    request = h.reserve_request(short)
    h.clock.advance(61)
    assert not h.store.resolve_target(h.targets[0]).action.enabled
    assert h.store.reserve_action(request).denial == "expired_review"
    assert not h.state.approvals[request.approval.approval_id]["consumed_at"]


def test_catalogue_scan_cannot_delete_items_or_claim_item_coverage(harness: Harness) -> None:
    h = harness
    h.clock.advance(1)
    generation_id = h.next_id()
    workspace = m.InventoryWorkspace(
        **h.context(), generation_id=generation_id, workspace_id=h.targets[0].workspace_id,
        name="Renamed container", observed_at=h.clock(),
    )
    generation = m.InventoryGeneration(
        **h.context(), generation_id=generation_id, selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        adapter="workspace_catalogue", authority="tenant_admin", enumeration="workspaces",
        completeness="complete", started_at=h.clock(), completed_at=h.clock(),
        discovered_count=1, completed_pages=1,
    )
    h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=generation, items=(), workspaces=(workspace,),
    ))
    assert h.store.list_inventory(m.TargetQuery(**h.context())).items[0].state == "present"
    assert h.store.resolve_target(h.targets[0]) is not None
    with pytest.raises(MonitoringConflict):
        h.store.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version, items=(), workspaces=(workspace,),
            generation=m.InventoryGeneration.model_validate({
                **generation.model_dump(), "enumeration": "items", "discovered_count": 0,
            }),
        ))


def test_uncollected_discovery_cannot_be_completed() -> None:
    h = Harness()
    queued = h.store.request_discovery(
        h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id(),
    )
    work = h.claim(("inventory",))[0]
    with pytest.raises(MonitoringConflict):
        h.store.complete_collection_work(
            m.MonitoringContext(**h.context()), work_id=queued.work_id,
            lease=work.lease, expected_work_revision=work.revision,
        )
    generation = m.InventoryGeneration(
        **h.context(), generation_id=h.next_id(), selector=work.discovery_selector,
        adapter="empty_fixture_inventory", authority="tenant_admin",
        completeness="complete", started_at=h.clock(), completed_at=h.clock(),
        discovered_count=0, completed_pages=1,
    )
    h.store.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=generation, items=(),
    ))
    assert h.store.complete_collection_work(
        m.MonitoringContext(**h.context()), work_id=queued.work_id,
        lease=work.lease, expected_work_revision=work.revision,
    ).state == "completed"


def test_queued_connectors_have_no_fabricated_resource_ids(harness: Harness) -> None:
    h = harness
    assert h.plan.subscription_count_delta == 1
    connector = h.store.list_connectors(m.PageQuery(**h.context())).items[0]
    assert connector.state == "planned"
    assert connector.workspace_id is None
    assert connector.eventstream_id is None
    assert connector.destination_id is None
    assert connector.endpoint is None
    assert h.store.get_activation(
        m.MonitoringContext(**h.context()), h.activation_request.idempotency_id,
    ).state == "configuring"
    work = h.claim(("connector_reconcile",))[0]
    with pytest.raises(MonitoringConflict):
        h.store.complete_collection_work(
            m.MonitoringContext(**h.context()), work_id=work.work_id,
            lease=work.lease, expected_work_revision=work.revision,
        )
    h.connector()
    assert h.store.complete_collection_work(
        m.MonitoringContext(**h.context()), work_id=work.work_id,
        lease=work.lease, expected_work_revision=work.revision,
    ).state == "completed"


def test_uncertain_verification_finalization_schedules_another_read_only_round(harness: Harness) -> None:
    h = harness
    h.source_work()
    request = h.reserve_request(h.review())
    action = h.store.reserve_action(request).reservation
    action = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state="uncertain", submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(seconds=120),
        detail="No exact submission acknowledgement.",
    ))
    first = h.finalize(action=action)
    h.clock.advance(121)
    h.work = h.claim(("verify_action",))[0]
    old_work_id = h.work.work_id
    action = h.store.record_action_outcome(m.ActionOutcomeRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        disposition="uncertain", observed_at=h.clock(), detail="Read-only correlation remains unavailable.",
    ))
    h.finalize(action=action)
    h.clock.advance(121)
    next_round = h.claim(("verify_action",))[0]
    assert next_round.work_id != old_work_id
    assert next_round.action_reservation_id == action.reservation_id
    assert h.store.get_incident_state(request.incident).action_count == 1
    assert h.store.get_incident(request.incident).occurrence_count == 1
    assert first.incident_id == canonical_incident_id(request.incident)


def test_duplicate_intake_reuses_recovery_work_without_another_action(harness: Harness) -> None:
    h = harness
    h.connector()
    original = h.signal(0)
    partition_lease = h.start_partition(first_sequence_number=0, lease_seconds=900)
    receipt = h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=partition_lease, receipts=(original,),
    ))
    h.work = h.claim(("triage",))[0]
    h.source = h.observation()
    h.store.observe_source(h.source, work_id=h.work.work_id, lease=h.work.lease)
    action = h.store.reserve_action(h.reserve_request(h.review())).reservation
    h.clock.advance(121)
    recovered = h.claim(("triage",))[0]
    assert recovered.kind == "verify_action"
    assert recovered.action_reservation_id == action.reservation_id
    again = h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=partition_lease, receipts=(h.signal(1, h.source),),
    ))
    assert again.work_ids == receipt.work_ids


def test_late_historical_occurrence_preserves_verified_incident_and_budget(harness: Harness) -> None:
    h = harness
    h.source_work()
    request = h.reserve_request(h.review())
    action = h.store.reserve_action(request).reservation
    submitted = m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="fabric_job", run_id=uid(35_000))
    action = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence, state="submitted",
        submitted_execution=submitted, correlation="response_run_id", submitted_at=h.clock(),
        next_verification_at=h.clock() + timedelta(seconds=120), detail="Exact fixture rerun.",
    ))
    h.clock.advance(2)
    action = h.store.record_action_outcome(m.ActionOutcomeRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        disposition="verified_succeeded", submitted_execution=submitted,
        observation=h.observation(
            execution=submitted, status="succeeded",
            started_at=h.clock() - timedelta(seconds=1), ended_at=h.clock(),
        ),
        activities=(PipelineActivity(name="Fixture activity", status="Succeeded"),),
        activities_complete=True, observed_at=h.clock(), detail="Verified rerun.",
    ))
    h.finalize(action=action, outcome="resolved")
    latest = h.store.get_incident(request.incident)
    h.clock.advance(1)
    old = h.observation(
        execution=m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="fabric_job", run_id=uid(29_000)),
        started_at=h.clock() - timedelta(minutes=20), ended_at=h.clock() - timedelta(minutes=15),
    )
    h.source_work(old)
    receipt = h.finalize(outcome="needs_human")
    retained = h.store.get_incident(request.incident)
    assert receipt.source_disposition == "historical"
    assert retained.outcome == "resolved"
    assert retained.status == "resolved"
    assert retained.last_seen_at == latest.last_seen_at
    assert retained.occurrence_count == latest.occurrence_count + 1
    assert h.store.get_incident_state(request.incident).action_count == 1


def rejection_request(
    h: Harness, action: m.ActionReservation, *, reason: str = "throttled", retry_after: int = 0,
) -> m.ActionRejectionRequest:
    work = h.store.get_work(m.MonitoringContext(**h.context()), action.request.work_id)
    return m.ActionRejectionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        work_id=work.work_id, lease=work.lease,
        evidence=m.ActionRejectionEvidence(
            reason=reason, attempted_at=action.reserved_at, rejected_at=h.clock(),
            retry_after_seconds=retry_after,
        ),
        detail="The fixture service definitively rejected the submission without an effect.",
    )


def test_rejection_transfers_one_budget_slot_only_to_the_recorded_successor() -> None:
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    review = h.review("powerbi_refresh")
    intent = h.reserve_request(review, approval=False)
    action = h.store.reserve_action(intent).reservation
    request = rejection_request(h, action, retry_after=25)
    rejected = h.store.record_action_rejection(request)
    assert rejected.state == "rejected"
    assert rejected.submitted_execution is None
    assert rejected.submitted_at is None
    assert rejected.next_verification_at is None
    assert h.store.record_action_rejection(request) == rejected
    assert h.store.get_incident_state(intent.incident).action_count == 1
    successor = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
    assert successor.retry_of == rejected.reservation_id
    assert successor.retry_attempt == 1
    assert successor.execution == h.source.execution
    assert successor.due_at == h.clock() + timedelta(seconds=25)
    second_same_invocation = m.ActionReservationRequest.model_validate({
        **intent.model_dump(), "idempotency_id": h.next_id(),
        "expected_incident_revision": h.store.get_incident_state(intent.incident).revision,
    })
    assert h.store.reserve_action(second_same_invocation).status == "denied"
    assert h.claim(("verify_action",)) == ()
    h.clock.advance(26)
    assert h.claim(("deferred_retry",)) == ()
    h.finalize(action=rejected, outcome="deferred_retry")
    h.work = h.claim(("deferred_retry",))[0]
    h.source = m.SourceRunObservation.model_validate({**h.source.model_dump(), "observed_at": h.clock()})
    h.store.observe_source(h.source, work_id=h.work.work_id, lease=h.work.lease)
    accepted = h.store.reserve_action(h.reserve_request(review, approval=False))
    assert accepted.status == "reserved"
    assert accepted.reservation.retry_of == rejected.reservation_id
    assert accepted.reservation.retry_attempt == 1
    assert h.store.get_incident_state(intent.incident).action_count == 1
    parent = h.store.get_action_reservation(m.MonitoringContext(**h.context()), rejected.reservation_id)
    assert parent.retry_reservation_id == accepted.reservation.reservation_id


@pytest.mark.parametrize("definition_changed", [False, True])
def test_retry_keeps_fresh_full_approval_hash_but_cannot_change_technical_identity(definition_changed):
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    review = h.review("powerbi_refresh")
    original = h.reserve_request(review, arguments={"justification": "Original transient refresh failure."})
    action = h.store.reserve_action(original).reservation
    consumed = h.state.approvals[original.approval.approval_id]["consumed_at"]
    rejected = h.store.record_action_rejection(rejection_request(h, action, retry_after=1))
    h.finalize(action=rejected, outcome="deferred_retry")
    h.clock.advance(2)
    h.work = h.claim(("deferred_retry",))[0]
    h.source = m.SourceRunObservation.model_validate({**h.source.model_dump(), "observed_at": h.clock()})
    h.store.observe_source(h.source, work_id=h.work.work_id, lease=h.work.lease)
    if definition_changed:
        h.definition_hash = "b" * 64
        h.capability(h.targets[0])
        review = h.review("powerbi_refresh")
    retry = h.reserve_request(review, arguments={"justification": "The persisted throttling interval has elapsed."})
    original_binding = h.store.get_operation_receipt(h.version, "approval_binding", original.approval.approval_id)
    retry_binding = h.store.get_operation_receipt(h.version, "approval_binding", retry.approval.approval_id)
    assert retry_binding.result["arguments_hash"] == m._digest(retry.arguments)
    assert retry_binding.result["arguments_hash"] != original_binding.result["arguments_hash"]
    assert retry.approval != original.approval
    decision = h.store.reserve_action(retry)
    if definition_changed:
        assert decision.status == "denied" and decision.denial == "source_ineligible"
        assert not h.state.approvals[retry.approval.approval_id]["consumed_at"]
    else:
        assert decision.status == "reserved"
        assert decision.reservation.retry_of == action.reservation_id
        assert decision.reservation.request.arguments == retry.arguments
        assert h.state.approvals[retry.approval.approval_id]["consumed_at"]
    assert h.state.approvals[original.approval.approval_id]["consumed_at"] == consumed
    assert h.store.get_incident_state(original.incident).action_count == 1


@pytest.mark.parametrize("state", ["submitted", "uncertain"])
def test_rejection_never_releases_a_previously_accepted_or_uncertain_effect(state: str) -> None:
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review("powerbi_refresh"), approval=False)).reservation
    submitted = m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="powerbi_request", run_id=uid(45_000))
    action = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state=state, submitted_execution=submitted if state == "submitted" else None,
        correlation="response_run_id" if state == "submitted" else None,
        submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(seconds=120),
        detail="The request was accepted or its acknowledgement was lost.",
    ))
    with pytest.raises(MonitoringConflict):
        h.store.record_action_rejection(rejection_request(h, action))
    retained = h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id)
    assert retained.state == state
    assert retained.retry_work_id is None
    owner = next(record for record in h.state.records.values() if record.kind == "action_owner")
    assert json.loads(owner.payload)["active"] is True


def test_definitive_client_error_is_no_effect_without_an_automatic_retry() -> None:
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review("powerbi_refresh"), approval=False)).reservation
    rejected = h.store.record_action_rejection(rejection_request(h, action, reason="definitive_client_error"))
    assert rejected.state == "rejected"
    assert rejected.retry_work_id is None
    assert rejected.submitted_execution is None
    assert h.store.get_incident_state(action.request.incident).action_count == 1


def test_rejected_retry_is_refused_after_scope_revocation() -> None:
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review("powerbi_refresh"), approval=False)).reservation
    rejected = h.store.record_action_rejection(rejection_request(h, action, retry_after=10))
    h.finalize(action=rejected, outcome="deferred_retry")
    h.activate(m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}))
    h.clock.advance(11)
    assert h.claim(("deferred_retry",)) == ()
    successor = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
    assert successor.state == "dispositioned"
    assert h.store.get_incident_state(action.request.incident).action_count == 1


@pytest.mark.parametrize("workload", ["fabric_pipeline", "powerbi"])
def test_cancellation_releases_only_verified_pipeline_target_not_budget_or_approval(workload: m.Workload) -> None:
    h = Harness()
    h.seed(workload=workload)
    h.activate()
    h.source_work()
    review = h.review("pipeline_rerun" if workload == "fabric_pipeline" else "powerbi_refresh")
    request = h.reserve_request(review)
    action = h.store.reserve_action(request).reservation
    submitted = m.SourceExecutionIdentity(
        target=h.targets[0], run_id=uid(49_000),
        run_id_kind="fabric_job" if workload == "fabric_pipeline" else "powerbi_request",
    )
    action = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence, state="submitted",
        submitted_execution=submitted, correlation="response_run_id", submitted_at=h.clock(),
        next_verification_at=h.clock() + timedelta(seconds=30), detail="Exact accepted submission.",
    ))
    began = h.clock()
    h.clock.advance(1)
    fields = {
        **h.context(), "request_id": h.next_id(), "reservation_id": action.reservation_id,
        "expected_reservation_revision": action.revision, "action_fence": action.fence,
        "disposition": "verified_failed", "submitted_execution": submitted,
        "observation": h.observation(
            execution=submitted, status="cancelled", started_at=began, ended_at=h.clock(),
        ), "observed_at": h.clock(), "activities_complete": True,
        "detail": "The exact submitted execution was cancelled.",
    }
    if workload == "powerbi":
        with pytest.raises(ValidationError):
            m.ActionOutcomeRequest.model_validate(fields)
        unresolved = h.store.record_action_outcome(m.ActionOutcomeRequest.model_validate({
            **fields, "disposition": "uncertain",
        }))
        assert unresolved.state == "uncertain"
        owner = next(row for row in h.state.records.values() if row.kind == "action_owner")
        assert json.loads(owner.payload)["active"] is True
        assert h.store.get_incident_state(request.incident).action_count == 1
        assert h.state.approvals[request.approval.approval_id]["consumed_at"]
        return
    cancelled = h.store.record_action_outcome(m.ActionOutcomeRequest.model_validate(fields))
    assert cancelled.state == "verified_failed"
    assert cancelled.request.source_execution == h.source.execution
    assert cancelled.submitted_execution == submitted
    assert cancelled.next_verification_at is None and cancelled.retry_work_id is None
    assert h.store.get_source(submitted).status == "cancelled"
    assert h.store.get_incident_state(request.incident).action_count == 1
    assert h.state.approvals[request.approval.approval_id]["consumed_at"]
    owner = next(row for row in h.state.records.values() if row.kind == "action_owner")
    assert json.loads(owner.payload)["active"] is False
    with pytest.raises(MonitoringConflict):
        h.finalize(action=cancelled, outcome="resolved")
    h.finalize(action=cancelled, outcome="needs_human")
    assert h.store.get_incident(request.incident).outcome == "needs_human"
    h.clock.advance(121)
    assert h.claim(("verify_action",)) == ()
    h.clock.advance(1)
    independent = h.observation(
        execution=m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="fabric_job", run_id=uid(49_001)),
        started_at=h.clock(), ended_at=h.clock(), failure_signature="independent-failure",
    )
    h.source_work(independent)
    next_request = h.reserve_request(
        review, incident=m.IncidentIdentity(target=h.targets[0], signature="independent-failure"),
    )
    next_action = h.store.reserve_action(next_request)
    assert next_action.status == "reserved"
    assert next_action.reservation.fence > cancelled.fence
    assert h.store.get_incident_state(request.incident).action_count == 1


def test_rejection_chain_keeps_existing_retry_count_and_exponential_backoff() -> None:
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.capability(h.targets[0], expires_at=h.clock() + timedelta(days=1))
    h.source_work()
    review = h.review("powerbi_refresh")
    review = h.store.record_safety_review(m.SafetyReviewRequest(
        request_id=h.next_id(), expected=h.version, expected_review_revision=review.revision,
        review=m.SafetyReview.model_validate({
            **review.model_dump(), "revision": review.revision + 1,
            "expires_at": h.clock() + timedelta(days=1),
        }),
    ))
    for attempt in range(MAX_ATTEMPTS + 1):
        intent = h.reserve_request(review, approval=False)
        action = h.store.reserve_action(intent).reservation
        assert action.retry_attempt == attempt
        rejected = h.store.record_action_rejection(rejection_request(h, action))
        assert h.store.get_incident_state(intent.incident).action_count == 1
        h.finalize(action=rejected, outcome="deferred_retry" if rejected.retry_work_id else "needs_human")
        if attempt == MAX_ATTEMPTS:
            assert rejected.retry_work_id is None
            break
        successor = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
        wait = backoff_seconds(attempt + 1)
        assert successor.due_at == h.clock() + timedelta(seconds=wait)
        h.clock.advance(wait + 1)
        h.work = h.claim(("deferred_retry",))[0]
        h.source = m.SourceRunObservation.model_validate({**h.source.model_dump(), "observed_at": h.clock()})
        h.store.observe_source(h.source, work_id=h.work.work_id, lease=h.work.lease)
    assert len([record for record in h.state.records.values() if record.kind == "action"]) == MAX_ATTEMPTS + 1


def test_partition_ownership_cas_renews_rebalances_and_releases_only_partitions(harness: Harness) -> None:
    h = harness
    h.connector()
    h.signal(100)
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review())).reservation
    before_actions = {
        key: value.payload for key, value in h.state.records.items()
        if value.kind in {"action", "action_owner", "incident_state"}
    }
    assert isinstance(h.store, EventPersistence)
    first = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner, initial_sequence_number=100),
    ))
    assert first.modified_at == h.clock()
    assert first.lease.fence == 1
    assert h.store.get_stream_start(h.partition) is None
    scope = ConnectorScope(**h.context(), connector_id=h.connector_id, consumer_group="$Default")
    assert h.store.list_partition_ownership(scope) == (first,)
    assert h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(99)),
    )) is None
    h.clock.advance(1)
    renewed = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=first.etag,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner),
    ))
    assert renewed.lease.fence == first.lease.fence
    assert renewed.etag != first.etag
    assert renewed.modified_at == h.clock()
    moved = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=renewed.etag,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(99)),
    ))
    assert moved.lease.fence > first.lease.fence
    with pytest.raises(MonitoringLeaseLost):
        h.store.renew_lease(m.LeaseRenewal(lease=renewed.lease))
    released = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=moved.etag, release=moved.lease,
    ))
    assert released.lease is None
    with pytest.raises(MonitoringLeaseLost):
        h.store.renew_lease(m.LeaseRenewal(lease=moved.lease))
    reclaimed = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=released.etag,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(99)),
    ))
    assert reclaimed.lease.fence > moved.lease.fence
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id).state == "reserved"
    assert {
        key: value.payload for key, value in h.state.records.items()
        if value.kind in {"action", "action_owner", "incident_state"}
    } == before_actions


def test_partition_release_remains_possible_during_maintenance(harness: Harness) -> None:
    h = harness
    h.connector()
    h.signal(100)
    owner = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner),
    ))
    h.state.control_row["maintenance"] = True
    with pytest.raises(MonitoringConflict):
        h.store.change_partition_ownership(OwnershipChange(
            partition=h.partition, expected_etag=owner.etag,
            claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(99)),
        ))
    assert h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=owner.etag, release=owner.lease,
    )).lease is None


def test_actual_stream_start_is_not_inferred_from_claim_or_zero(harness: Harness) -> None:
    h = harness
    h.connector()
    signal = h.signal(100)
    lease = h.store.claim_partition(m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner))
    assert h.store.get_stream_start(h.partition) is None
    with pytest.raises(MonitoringConflict, match="actual broker start"):
        h.store.record_stream_receipts(m.StreamReceiptBatch(
            request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(signal,),
        ))
    start = h.store.ensure_stream_start(StreamStartRequest(
        partition=h.partition, lease=lease, first_available_sequence_number=100, observed_at=h.clock(),
    ))
    assert start.first_sequence_number == 100
    assert start.recorded_at == h.clock()
    assert start.history_before_start == "unobserved"
    assert start.gaps[0].code == "unobserved_stream_history"
    with pytest.raises(MonitoringConflict, match="precedes"):
        h.store.record_stream_receipts(m.StreamReceiptBatch(
            request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(h.signal(99),),
        ))
    h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(signal,),
    ))
    h.store.advance_stream_checkpoint(m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=lease, expected_revision=0, through=signal.position,
    ))
    retention = h.store.ensure_stream_start(StreamStartRequest(
        partition=h.partition, lease=lease, first_available_sequence_number=105, observed_at=h.clock(),
    ))
    assert retention.first_sequence_number == 100
    assert "stream_retention_gap" in {gap.code for gap in retention.gaps}
    assert h.store.get_stream_checkpoint(h.partition).position.sequence_number == 100


def test_identified_duplicates_and_unidentified_quarantine_keep_every_broker_position(harness: Harness) -> None:
    h = harness
    h.connector()
    first = h.signal(100)
    lease = h.start_partition(first_sequence_number=100)
    repeated = m.SignalReceipt.model_validate({**h.signal(101).model_dump(), "delivery": first.delivery})
    intake = h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(first, repeated),
    ))
    assert intake.receipt_keys == (first.delivery.key, first.delivery.key)
    assert len(intake.work_ids) == 1
    malformed = UnidentifiedSignal(
        partition=h.partition,
        position=m.StreamPosition(offset="1020", sequence_number=102, enqueued_at=h.clock()),
        received_at=h.clock(),
        quarantine=m.QuarantineDisposition(
            observation_id="fixture-malformed-body", reason="malformed", detail="The fixture body is not JSON.",
            metadata={"body_sha256": hashlib.sha256(b"not-json").hexdigest(), "size_bytes": 8},
        ),
    )
    request = UnidentifiedReceiptBatch(request_id=h.next_id(), lease=lease, receipt=malformed)
    quarantined = h.store.record_unidentified_receipts(request)
    assert quarantined.work_ids == ()
    assert len(quarantined.receipt_keys) == 1
    assert quarantined.receipt_keys[0] == f"{h.partition.key}:unidentified:102"
    assert h.store.get_stream_acceptance(m.MonitoringContext(**h.context()), request.request_id) == quarantined
    assert h.store.record_unidentified_receipts(request) == quarantined
    checkpoint = h.store.advance_stream_checkpoint(m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=lease,
        expected_revision=0, through=malformed.position,
    ))
    assert checkpoint.position.sequence_number == 102
    assert len([row for row in h.state.records.values() if row.kind == "stream_position"]) == 3
    raw = next(row.payload for row in h.state.records.values() if row.kind == "unidentified_signal")
    assert "delivery" not in json.loads(raw)
    with pytest.raises(MonitoringConflict, match="broker sequence"):
        h.store.record_unidentified_receipts(UnidentifiedReceiptBatch(
            request_id=h.next_id(), lease=lease,
            receipt=malformed.model_copy(update={"position": first.position}),
        ))


def test_receiver_heartbeat_is_health_not_delivery_verification(harness: Harness) -> None:
    h = harness
    connector = h.store.list_connectors(m.PageQuery(**h.context())).items[0]
    assert connector.state == "planned" and connector.delivery_verified_at is None
    heartbeat = ReceiverHeartbeat(
        **h.context(), worker_id=uid(70), connector_id=connector.connector_id,
        observed_at=h.clock(), state="running", transport_connected=True,
        accepted_positions=50, last_delivery_at=h.clock(),
    )
    recorded = h.store.record_receiver_heartbeat(heartbeat)
    assert recorded == heartbeat
    after = h.store.list_connectors(m.PageQuery(**h.context())).items[0]
    assert after.state == "planned"
    assert after.delivery_verified_at is None and after.identity_verified_at is None
    assert after == connector
    assert h.store.record_receiver_heartbeat(heartbeat) == recorded
    assert h.store.list_connectors(m.PageQuery(**h.context())).items[0].revision == after.revision
    h.clock.advance(1)
    stopped = heartbeat.model_copy(update={"observed_at": h.clock(), "state": "stopped", "transport_connected": False})
    h.store.record_receiver_heartbeat(stopped)
    assert "receiver_not_running" in {gap.code for gap in h.store.coverage(m.MonitoringContext(**h.context())).gaps}
    with pytest.raises(MonitoringConflict):
        h.store.record_receiver_heartbeat(heartbeat.model_copy(update={"error_code": "late_report"}))


def test_partition_ownership_listing_is_complete_beyond_one_record_page(harness: Harness) -> None:
    h = harness
    h.connector()
    scope = ConnectorScope(**h.context(), connector_id=h.connector_id, consumer_group="$Default")
    for index in range(1_001):
        partition = m.PartitionIdentity(**scope.model_dump(), partition_id=str(index))
        h.store.change_partition_ownership(OwnershipChange(
            partition=partition, expected_etag=None,
            claim=m.PartitionClaimRequest(partition=partition, owner_id=h.owner),
        ))
    rows = h.store.list_partition_ownership(scope)
    assert len(rows) == 1_001
    assert len({row.partition.partition_id for row in rows}) == 1_001
    assert all(row.lease.owner_id == h.owner and row.modified_at == h.clock() for row in rows)


async def test_controller_records_rejection_without_resetting_invocation_ledger() -> None:
    class RejectedClient(MockPowerBIClient):
        submissions = 0

        async def submit_refresh(self, workspace_id: str, dataset_id: str) -> RefreshOutcome:
            self.submissions += 1
            return RefreshOutcome(
                status="Throttled", submission_state="rejected", retry_after_seconds=30,
                detail="HTTP 429 rejected the fixture request without an effect.",
            )

        async def verify_refresh(self, workspace_id: str, dataset_id: str, request_id: str) -> RefreshOutcome:
            raise AssertionError("Rejected requests have no submitted refresh to verify")

    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    h.review("powerbi_refresh")
    execution = MonitoringExecution(
        store=h.store, work=h.work,
        incident=m.IncidentIdentity(target=h.targets[0], signature="fixture-failure"),
        observation=h.source, clock=h.clock,
    )
    allowed, _ = execution.reserve("refresh_powerbi_dataset")
    assert allowed
    ledger = PolicyLedger(TriagePolicy())
    ledger.charge_write("refresh_powerbi_dataset")
    client = RejectedClient()
    outcome = await execution.refresh(client)
    assert outcome.submission_state == "rejected"
    assert execution.reservation.state == "rejected"
    assert execution.reservation.submitted_execution is None
    assert execution.reservation.retry_work_id is not None
    assert ledger.write_actions == 1
    with pytest.raises(PolicyViolation):
        ledger.charge_write("refresh_powerbi_dataset")
    assert execution.reserve("refresh_powerbi_dataset")[0] is False
    assert client.submissions == 1
