from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from triage.models import Incident
from triage.monitoring import MONITORING_SCHEMA_VERSION, MonitoringStore
from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringNotBootstrapped,
    MonitoringSchemaMismatch,
    MonitoringStoreError,
    MonitoringUnavailable,
)
from triage.pipeline_models import PipelineActivity, PipelineTarget

TENANT = "10000000-0000-4000-8000-000000000001"
EPOCH = "20000000-0000-4000-8000-000000000002"
WORKSPACE = "30000000-0000-4000-8000-000000000003"
ITEM = "40000000-0000-4000-8000-000000000004"
RUN = "50000000-0000-4000-8000-000000000005"
OTHER = "60000000-0000-4000-8000-000000000006"
OWNER = "70000000-0000-4000-8000-000000000007"
WORK = "80000000-0000-4000-8000-000000000008"
REQUEST = "90000000-0000-4000-8000-000000000009"
GENERATION = "a0000000-0000-4000-8000-00000000000a"
REVIEW = "b0000000-0000-4000-8000-00000000000b"
SCOPE = "c0000000-0000-4000-8000-00000000000c"
RULE = "d0000000-0000-4000-8000-00000000000d"
CONNECTOR = "e0000000-0000-4000-8000-00000000000e"
NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
LATER = NOW + timedelta(minutes=5)
HASH = "a" * 64


def target(**changes: object) -> m.TargetIdentity:
    return m.TargetIdentity.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "workload": "fabric_pipeline",
        "workspace_id": WORKSPACE, "item_id": ITEM, **changes,
    })


def test_component_publication_models_preserve_exact_json_and_pending_frontier():
    frontier = m.ValidationFrontier(
        tenant_id=TENANT, epoch=EPOCH, frontier_key="validation:fixture", target=target(),
        accepted_revision=2, validated_revision=1, latest_request_id=REQUEST, updated_at=NOW,
    )
    assert frontier.pending
    assert m.ValidationFrontier.model_validate_json(frontier.model_dump_json()) == frontier
    request = m.ReconciliationRequest(
        tenant_id=TENANT, epoch=EPOCH, request_id=REQUEST, producer="worker", topic="capability",
        reference_id=ITEM, fingerprint=HASH, policy_revision=3, work_id=WORK,
        target=target(), frontier_key=frontier.frontier_key, frontier_revision=2,
        created_at=NOW, request_payload={"capability_id": OTHER},
        evidence=(m.EvidenceBinding(kind="capability", key=OTHER, revision=1, payload_hash=HASH),),
    )
    assert m.ReconciliationRequest.model_validate_json(request.model_dump_json()) == request
    assert request.model_dump(mode="json")["target"]["workload"] == "fabric_pipeline"
    with pytest.raises(ValidationError):
        m.ValidationFrontier.model_validate({**frontier.model_dump(), "validated_revision": 3})


@pytest.mark.parametrize("change", [
    {"accepted_revision": True}, {"validated_revision": -1},
    {"updated_at": NOW.replace(tzinfo=None)}, {"tenant_id": OTHER},
])
def test_publication_frontier_rejects_coercion_naive_dates_and_context_mismatch(change):
    with pytest.raises(ValidationError):
        m.ValidationFrontier.model_validate({
            "tenant_id": TENANT, "epoch": EPOCH, "frontier_key": "validation:fixture",
            "target": target(), "accepted_revision": 1, "latest_request_id": REQUEST,
            "updated_at": NOW, **change,
        })


def test_targetless_reconciliation_uses_work_lease_not_target_or_action_identity():
    request = m.ReconcileStateRequest(
        tenant_id=TENANT, epoch=EPOCH, request_id=REQUEST, work_id=WORK,
        lease=lease(), expected_work_revision=4, expected_policy_revision=3, expected_frontier_revision=1,
    )
    assert "target" not in request.model_dump()
    with pytest.raises(ValidationError):
        m.ReconcileStateRequest.model_validate({
            **request.model_dump(), "lease": lease(resource_key=f"controller:{target().key}"),
        })


@pytest.mark.parametrize("model", [m.ReconciliationResult, m.FrontierResolution])
@pytest.mark.parametrize("scope,state,rejection_id", [
    ("handoff", "pending_validation", None),
    ("handoff", "published", None),
    ("handoff", "rejected", None),
    ("window", "rejected", None),
    ("window_acknowledgement", "rejected", OTHER),
    ("window_acknowledgement", "published", None),
])
def test_window_resolution_json_distinguishes_original_rejection_from_sibling_receipt(model, scope, state, rejection_id):
    document = {
        "work_id": WORK, "producer_request_id": REQUEST,
        "frontier_key": "validation:fixture", "frontier_revision": 2,
        "handoff_revision": 1, "handoff_resolution_request_id": None, "handoff_resolution_work_fence": None,
        "frontier_resolution_request_id": None, "frontier_resolution_revision": None,
        "state": state, "resolution_scope": scope, "window_rejection_request_id": rejection_id,
        "window_resolution_request_id": OTHER if scope == "window_acknowledgement" else None,
        "window_resolution_state": state if scope == "window_acknowledgement" else None,
    }
    if model is m.FrontierResolution:
        document.update(work_fence=3, validated_revision=1 if state == "pending_validation" else 2,
                        handoff_decision="published")
    else:
        document.update(tenant_id=TENANT, epoch=EPOCH, request_id=GENERATION,
                        policy_revision=4, detail="The original window decision is immutable.", published_at=NOW)
    value = model.model_validate(document)
    assert model.model_validate_json(value.model_dump_json()) == value
    assert value.model_dump(mode="json")["window_rejection_request_id"] == rejection_id


@pytest.mark.parametrize("model", [m.ReconciliationResult, m.FrontierResolution])
@pytest.mark.parametrize("scope,state,rejection_id", [
    ("window_acknowledgement", "rejected", None),
    ("window_acknowledgement", "published", OTHER),
    ("window_acknowledgement", "pending_validation", OTHER),
    ("handoff", "rejected", OTHER),
    ("window", "rejected", OTHER),
    ("window", "published", None),
    ("window", "pending_validation", None),
])
def test_window_resolution_rejects_missing_or_misleading_original_rejection(model, scope, state, rejection_id):
    document = {
        "work_id": WORK, "producer_request_id": REQUEST,
        "frontier_key": "validation:fixture", "frontier_revision": 2,
        "handoff_revision": 1, "handoff_resolution_request_id": None, "handoff_resolution_work_fence": None,
        "frontier_resolution_request_id": None, "frontier_resolution_revision": None,
        "state": state, "resolution_scope": scope, "window_rejection_request_id": rejection_id,
        "window_resolution_request_id": OTHER if scope == "window_acknowledgement" else None,
        "window_resolution_state": state if scope == "window_acknowledgement" else None,
    }
    if model is m.FrontierResolution:
        document.update(work_fence=3, validated_revision=1 if state == "pending_validation" else 2,
                        handoff_decision="published")
    else:
        document.update(tenant_id=TENANT, epoch=EPOCH, request_id=GENERATION,
                        policy_revision=4, detail="Invalid window resolution.", published_at=NOW)
    with pytest.raises(ValidationError):
        model.model_validate(document)


@pytest.mark.parametrize("missing", [
    "resolution_scope", "window_rejection_request_id", "window_resolution_request_id", "window_resolution_state",
    "handoff_revision", "handoff_resolution_request_id", "handoff_resolution_work_fence",
    "frontier_resolution_request_id", "frontier_resolution_revision",
])
def test_native_frontier_result_requires_explicit_window_and_handoff_fields(missing):
    document = {
        "work_id": WORK, "work_fence": 3, "producer_request_id": REQUEST,
        "frontier_key": "validation:fixture", "frontier_revision": 2, "validated_revision": 2,
        "handoff_revision": 1, "handoff_resolution_request_id": None, "handoff_resolution_work_fence": None,
        "frontier_resolution_request_id": None, "frontier_resolution_revision": None,
        "state": "rejected", "handoff_decision": "published", "resolution_scope": "window",
        "window_rejection_request_id": None,
        "window_resolution_request_id": None, "window_resolution_state": None,
    }
    m.FrontierResolution.model_validate(document)
    document.pop(missing)
    with pytest.raises(ValidationError):
        m.FrontierResolution.model_validate(document)


@pytest.mark.parametrize("change", [
    {"state": "pending_validation"}, {"handoff_decision": "rejected"},
    {"handoff_revision": 5}, {"handoff_resolution_request_id": None},
    {"handoff_resolution_work_fence": 4}, {"frontier_resolution_request_id": None},
    {"frontier_resolution_revision": 3}, {"window_resolution_request_id": OTHER},
])
def test_native_handoff_acknowledgement_requires_its_exact_terminal_prefix_without_claiming_later_intake(change):
    document = {
        "work_id": WORK, "work_fence": 3, "producer_request_id": REQUEST,
        "frontier_key": "validation:fixture", "frontier_revision": 5, "validated_revision": 4,
        "handoff_revision": 1, "handoff_resolution_request_id": OTHER, "handoff_resolution_work_fence": 2,
        "frontier_resolution_request_id": GENERATION, "frontier_resolution_revision": 4,
        "state": "published", "handoff_decision": "published", "resolution_scope": "handoff_acknowledgement",
        "window_rejection_request_id": None, "window_resolution_request_id": None, "window_resolution_state": None,
    }
    result = m.FrontierResolution.model_validate(document)
    assert result.validated_revision < result.frontier_revision
    with pytest.raises(ValidationError):
        m.FrontierResolution.model_validate({**document, **change})


def test_kernel_incomplete_bootstrap_cannot_report_ready_or_omit_its_missing_operations():
    control = m.DeploymentControl(
        tenant_id=TENANT, epoch=EPOCH, revision=0, activation_cutoff=NOW,
        maintenance=False, updated_at=NOW,
    )
    document = {
        "status": "kernel_incomplete", "expected_tenant_id": TENANT, "found_schema_version": 1,
        "control": control, "missing_operations": ("controller_publication",), "detail": "Guarded publication is unbound.",
    }
    assert m.BootstrapInspection.model_validate(document).status == "kernel_incomplete"
    for change in ({"status": "ready"}, {"missing_operations": ()}):
        with pytest.raises(ValidationError):
            m.BootstrapInspection.model_validate({**document, **change})


def test_pending_review_cannot_assert_platform_correlation_and_keeps_intent_validation():
    pending = review(
        state="pending", requested_state="verified", publication_status="pending_validation",
        exact_correlation_verified=False,
    )
    assert pending.parameter_hash == review().parameter_hash
    with pytest.raises(ValidationError):
        review(state="pending", requested_state="verified", publication_status="pending_validation")
    with pytest.raises(ValidationError, match="contradictory prior requested-state"):
        m.SafetyReviewRequest(
            request_id=REQUEST, expected=version(), expected_review_revision=0,
            review=review(state="revoked", revoked_at=LATER, requested_state="verified"),
        )


def version(**changes: object) -> m.RegistryVersion:
    return m.RegistryVersion.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "revision": 3, **changes,
    })


def execution(**changes: object) -> m.SourceExecutionIdentity:
    return m.SourceExecutionIdentity.model_validate({
        "target": target(), "run_id": RUN, "run_id_kind": "fabric_job", **changes,
    })


def incident_identity(**changes: object) -> m.IncidentIdentity:
    return m.IncidentIdentity.model_validate({
        "target": target(), "signature": "v1:fixture-failure", **changes,
    })


def lease(**changes: object) -> m.LeaseToken:
    return m.LeaseToken.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "owner_id": OWNER, "fence": 2,
        "resource_key": m.work_key(version(), WORK), "acquired_at": NOW, "expires_at": LATER,
        **changes,
    })


def observation(**changes: object) -> m.SourceRunObservation:
    return m.SourceRunObservation.model_validate({
        "execution": execution(), "origin": "fixture", "authority": "fixture",
        "observed_at": LATER, "started_at": NOW, "ended_at": NOW + timedelta(minutes=1),
        "status": "failed", "invocation": "scheduled", "job_type": "Pipeline", **changes,
    })


def selector(**changes: object) -> m.ScopeSelector:
    return m.ScopeSelector.model_validate({"tenant_id": TENANT, "kind": "tenant", **changes})


def rule(**changes: object) -> m.ScopeRule:
    return m.ScopeRule.model_validate({
        "rule_id": RULE, "selector": selector(), "effect": "include", **changes,
    })


def scope(**changes: object) -> m.ScopeDefinition:
    return m.ScopeDefinition.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "scope_id": SCOPE, "name": "Fixture scope",
        "rules": [rule()], **changes,
    })


def generation(**changes: object) -> m.InventoryGeneration:
    return m.InventoryGeneration.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "generation_id": GENERATION,
        "selector": selector(), "adapter": "fixture_inventory", "authority": "fixture",
        "completeness": "complete", "started_at": NOW, "completed_at": LATER,
        "discovered_count": 1, "completed_pages": 1, **changes,
    })


def inventory_item(**changes: object) -> m.InventoryItem:
    return m.InventoryItem.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "generation_id": GENERATION,
        "workspace_id": WORKSPACE, "item_id": ITEM, "name": "Fixture pipeline",
        "item_type": "DataPipeline", "workload": "fabric_pipeline", "observed_at": NOW,
        **changes,
    })


def admitted(**changes: object) -> m.MonitoringTarget:
    return m.MonitoringTarget.model_validate({
        "identity": target(), "name": "Fixture pipeline", "scope_ids": [SCOPE],
        "admitted_rule_ids": [RULE], "inventory_generation": GENERATION,
        "capability_id": REQUEST, "policy_revision": 3, "admitted_at": NOW,
        "state": "current", "admission_basis": "auto_detection_only",
        "reason": "Explicit automatic detection-only scope.",
        "observation": {"enabled": True}, **changes,
    })


def review(**changes: object) -> m.SafetyReview:
    return m.SafetyReview.model_validate({
        "review_id": REVIEW, "target": target(), "revision": 1, "policy_revision": 3,
        "action": "pipeline_rerun", "state": "verified", "reviewer_id": OWNER,
        "reviewed_at": NOW, "expires_at": LATER, "definition_hash": HASH,
        "parameters": {}, "replay_safe": True, "exact_correlation_verified": True,
        "detail": "Explicit fixture review, not deployment authority.", **changes,
    })


def reservation_request(**changes: object) -> m.ActionReservationRequest:
    return m.ActionReservationRequest.model_validate({
        "idempotency_id": REQUEST, "expected": version(), "work_id": WORK,
        "lease": lease(), "source_execution": execution(), "incident": incident_identity(),
        "expected_incident_revision": 0, "action": "pipeline_rerun", "review_id": REVIEW,
        "expected_review_revision": 1, "definition_hash": HASH,
        "parameter_hash": review().parameter_hash, **changes,
    })


def partition(**changes: object) -> m.PartitionIdentity:
    return m.PartitionIdentity.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "connector_id": CONNECTOR,
        "consumer_group": "$Default", "partition_id": "0", **changes,
    })


def signal(**changes: object) -> m.SignalReceipt:
    return m.SignalReceipt.model_validate({
        "delivery": {
            "tenant_id": TENANT, "epoch": EPOCH, "connector_id": CONNECTOR,
            "event_source": "/fixture/fabric/jobs", "event_id": "fixture-event-1",
        },
        "partition": partition(),
        "position": {"offset": "42", "sequence_number": 7, "enqueued_at": NOW},
        "received_at": LATER, "status": "accepted",
        "observation": observation(origin="event", authority="transport"), **changes,
    })


def page(**changes: object) -> m.RestPageRequest:
    return m.RestPageRequest.model_validate({
        "page_id": REQUEST, "target": target(), "policy_revision": 3, "poll_work_id": WORK,
        "lease": lease(), "expected_checkpoint_revision": 0,
        "window": {"start_at": NOW, "end_at": LATER},
        "received_count": 1, "observations": [observation()], "observed_at": LATER, **changes,
    })


def work(**changes: object) -> m.MonitoringWork:
    return m.MonitoringWork.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "work_id": WORK, "kind": "triage",
        "policy_revision": 3, "due_at": NOW, "created_at": NOW,
        "reason": "Fixture source failure.", "target": target(), "execution": execution(),
        "revision": 1, "state": "queued", **changes,
    })


def finalization(**changes: object) -> m.WorkFinalizationRequest:
    return m.WorkFinalizationRequest.model_validate({
        "tenant_id": TENANT, "epoch": EPOCH, "finalization_id": REQUEST, "work_id": WORK,
        "expected_work_revision": 1, "lease": lease(), "source_execution": execution(),
        "incident_identity": incident_identity(),
        "incident": Incident(
            id="fixture-incident", signature=incident_identity().signature,
            outcome="needs_human", original_error="Synthetic business evidence.",
        ),
        "source_disposition": "triaged", **changes,
    })


def test_canonical_target_execution_and_incident_wire_identity() -> None:
    canonical = target(item_id=ITEM.upper(), workspace_id="{" + WORKSPACE + "}")
    expected = f"monitor:v1:{EPOCH}:{TENANT}:fabric_pipeline:{WORKSPACE}:{ITEM}"
    assert canonical.key == expected
    assert canonical.model_dump(mode="json") == {
        "tenant_id": TENANT, "epoch": EPOCH, "workload": "fabric_pipeline",
        "workspace_id": WORKSPACE, "item_id": ITEM,
    }
    assert canonical.execution_key(RUN, "fabric_job") == f"{expected}:run:fabric_job:{RUN}"
    assert canonical.incident_key("v1:fixture-failure") == incident_identity().key
    assert execution(run_id=RUN.replace("-", "").upper()).key == execution().key


@pytest.mark.parametrize("field", ["tenant_id", "epoch", "workspace_id", "item_id"])
@pytest.mark.parametrize("invalid", ["", "not-a-uuid", "00000000-0000-0000-0000-000000000000", 7])
def test_target_ids_are_explicit_nonempty_uuids(field: str, invalid: object) -> None:
    with pytest.raises(ValidationError):
        target(**{field: invalid})


@pytest.mark.parametrize("field", ["tenant_id", "epoch", "workspace_id", "item_id", "workload"])
def test_identity_keys_cover_every_admission_dimension(field: str) -> None:
    other = target(**{field: "powerbi" if field == "workload" else OTHER})
    assert other.key != target().key
    assert other.incident_key("same-signature") != target().incident_key("same-signature")


def test_display_rename_and_recurrence_do_not_create_another_incident_budget() -> None:
    assert admitted(name="First label").key == admitted(name="Renamed label").key
    assert execution().key != execution(run_id=OTHER).key
    assert execution().target.incident_key("same") == execution(run_id=OTHER).target.incident_key("same")
    assert admitted(name="Same name", identity=target(workspace_id=OTHER)).key != admitted(name="Same name").key


@pytest.mark.parametrize("workload", ["pipeline", "semantic_model", "powerbi_triage", "pipeline_sweep"])
def test_wire_workloads_are_not_command_kinds(workload: str) -> None:
    with pytest.raises(ValidationError):
        target(workload=workload)


def test_powerbi_execution_id_namespaces_are_distinct() -> None:
    model = target(workload="powerbi")
    history = execution(target=model, run_id_kind="powerbi_refresh", run_id="00027")
    request = execution(target=model, run_id_kind="powerbi_request", run_id=RUN)
    assert history.run_id == "27"
    assert history.key != request.key
    with pytest.raises(ValidationError):
        execution(target=model)
    with pytest.raises(ValidationError):
        execution(run_id_kind="powerbi_request")
    with pytest.raises(ValidationError):
        execution(target=model, run_id_kind="powerbi_refresh", run_id=RUN)


@pytest.mark.parametrize("invalid", ["0", "-2", "1.5", " 27", "27 ", "٢٧", "27:other"])
def test_powerbi_numeric_execution_ids_are_not_coerced_or_ambiguous(invalid: str) -> None:
    with pytest.raises(ValidationError):
        execution(target=target(workload="powerbi"), run_id_kind="powerbi_refresh", run_id=invalid)


def test_frozen_extra_forbid_and_nonblank_labels() -> None:
    with pytest.raises(ValidationError):
        target(name="Names are not identity.")
    identity = target()
    with pytest.raises(ValidationError):
        identity.item_id = OTHER
    assert admitted(name="  Label  ").name == "Label"
    for name in ("", " \t", "x" * 201, "one\ntwo"):
        with pytest.raises(ValidationError):
            admitted(name=name)


@pytest.mark.parametrize("invalid", ["true", "false", 0, 1, None])
def test_boolean_boundary_does_not_coerce(invalid: object) -> None:
    with pytest.raises(ValidationError):
        m.ObservationPolicy(enabled=invalid)
    with pytest.raises(ValidationError):
        rule(auto_enrol_detection_only=invalid)
    with pytest.raises(ValidationError):
        review(replay_safe=invalid)


@pytest.mark.parametrize("invalid", [True, "3", 1.5, -1])
def test_revision_is_a_strict_nonnegative_counter(invalid: object) -> None:
    with pytest.raises(ValidationError):
        version(revision=invalid)


@pytest.mark.parametrize("invalid", [datetime(2026, 1, 1), "2026-01-01T12:00:00", 1_767_225_600, True])
def test_source_and_lease_times_require_explicit_timezones(invalid: object) -> None:
    with pytest.raises(ValidationError):
        observation(observed_at=invalid)
    with pytest.raises(ValidationError):
        lease(expires_at=invalid)


def test_offset_times_normalize_to_utc_without_replacing_execution_time() -> None:
    observed = observation(observed_at="2026-01-01T08:05:00-04:00")
    assert observed.observed_at == LATER
    assert observed.observed_at.tzinfo is UTC
    assert observed.started_at == NOW
    assert observed.model_dump(mode="json")["observed_at"] == "2026-01-01T12:05:00Z"
    with pytest.raises(ValidationError):
        observation(ended_at=NOW - timedelta(seconds=1))
    with pytest.raises(ValidationError):
        lease(expires_at=NOW)


def test_bootstrap_inspection_cannot_silently_initialize() -> None:
    missing = m.BootstrapInspection(status="missing", expected_tenant_id=TENANT, detail="Run deployment bootstrap.")
    assert missing.control is None
    control = m.DeploymentControl(**version().model_dump(), activation_cutoff=NOW, updated_at=NOW)
    assert control.maintenance is True
    with pytest.raises(ValidationError):
        m.BootstrapInspection(
            status="ready", expected_tenant_id=TENANT, found_schema_version=1,
            control=control, detail="Not ready during maintenance.",
        )
    ready_control = m.DeploymentControl.model_validate({
        **control.model_dump(), "maintenance": False,
    })
    ready = m.BootstrapInspection(
        status="ready", expected_tenant_id=TENANT, found_schema_version=1,
        control=ready_control, detail="Current schema.",
    )
    assert ready.control.schema_version == MONITORING_SCHEMA_VERSION
    with pytest.raises(ValidationError):
        m.BootstrapInspection.model_validate({**ready.model_dump(), "expected_tenant_id": OTHER})
    with pytest.raises(ValidationError):
        m.DeploymentControl.model_validate({**control.model_dump(), "schema_version": True})
    incompatible = m.BootstrapInspection(
        status="incompatible", expected_tenant_id=TENANT, found_schema_version=2,
        detail="Deployment schema does not match the runtime.",
    )
    assert incompatible.control is None


@pytest.mark.parametrize(
    "fields",
    [
        {"kind": "domain"}, {"kind": "item", "item_id": ITEM},
        {"kind": "tenant", "workspace_id": WORKSPACE},
        {"kind": "workspace", "workspace_id": WORKSPACE, "include_descendants": True},
        {"kind": "item", "workspace_id": WORKSPACE, "item_id": ITEM, "domain_id": OTHER},
    ],
)
def test_scope_selector_rejects_ambiguous_hierarchies(fields: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        selector(**fields)


def test_scope_rules_keep_exclusions_and_future_resource_behavior_explicit() -> None:
    domain = selector(kind="domain", domain_id=OTHER, include_descendants=True)
    included = rule(selector=domain, auto_enrol_detection_only=True)
    excluded = rule(rule_id=OTHER, effect="exclude", selector=selector(kind="workspace", workspace_id=WORKSPACE))
    policy = scope(rules=[included, excluded])
    assert policy.rules[0].auto_enrol_detection_only
    assert policy.rules[1].effect == "exclude"
    assert rule().auto_enrol_detection_only is False
    with pytest.raises(ValidationError):
        rule(effect="exclude", auto_enrol_detection_only=True)
    with pytest.raises(ValidationError):
        scope(rules=[included, included])
    with pytest.raises(ValidationError):
        scope(rules=[rule(selector=selector(tenant_id=OTHER))])
    with pytest.raises(ValidationError):
        rule(workloads=["powerbi", "powerbi"])
    assert scope(rules=[]).rules == ()


def test_complete_empty_inventory_differs_from_partial_or_caller_visible() -> None:
    empty = generation(discovered_count=0)
    assert empty.completeness == "complete"
    with pytest.raises(ValidationError):
        generation(completeness="partial", completed_at=None)
    gap = m.CoverageGap(code="inventory_budget", detail="Continue the next page.")
    partial = generation(completeness="partial", completed_at=None, continuation="page-2", gaps=[gap])
    assert partial.continuation == "page-2"
    with pytest.raises(ValidationError):
        generation(continuation="page-2")
    with pytest.raises(ValidationError):
        generation(authority="caller_visible")
    with pytest.raises(ValidationError):
        m.InventoryBatch(
            request_id=REQUEST, expected=version(), generation=partial,
            items=[inventory_item(state="deleted")],
        )


def test_inventory_keeps_unsupported_items_without_granting_a_workload() -> None:
    unsupported = inventory_item(item_type="Notebook", workload=None, unsupported_reason="No detector contract.")
    assert unsupported.target is None
    assert inventory_item().target == target()
    with pytest.raises(ValidationError):
        inventory_item(item_type="Notebook")
    with pytest.raises(ValidationError):
        inventory_item(workload=None)
    with pytest.raises(ValidationError):
        m.InventoryBatch(
            request_id=REQUEST, expected=version(), generation=generation(),
            items=[inventory_item(generation_id=OTHER)],
        )


def test_capability_proof_and_observation_are_separate_from_action_admission() -> None:
    fields = {
        "capability_id": REQUEST, "target": target(), "inventory_generation": GENERATION,
        "collector_identity_id": OWNER, "read_status": "verified",
        "checked_at": NOW, "expires_at": LATER,
    }
    capability = m.CapabilityObservation.model_validate(fields)
    assert capability.action_status == "unknown"
    with pytest.raises(ValidationError):
        m.CapabilityObservation.model_validate({**fields, "action_status": "verified"})
    assert admitted().observation.enabled
    assert not admitted().action.enabled
    action = m.ActionPolicy(
        enabled=True, action="pipeline_rerun", review_id=REVIEW, review_revision=1,
    )
    with pytest.raises(ValidationError):
        admitted(action=action)
    assert admitted(action=action, admission_basis="reviewed").action.enabled
    with pytest.raises(ValidationError):
        admitted(state="paused")
    with pytest.raises(ValidationError):
        m.ActionPolicy(enabled=True, action="pipeline_rerun")
    with pytest.raises(ValidationError):
        m.ObservationPolicy(events_enabled=True)


def test_preview_is_revision_and_idempotency_bound_without_action_switches() -> None:
    plan = m.ActivationPlan(
        expected=version(), idempotency_id=REQUEST, scope=scope(), plan_id=OTHER,
        created_at=NOW, expires_at=LATER, inventory_generations=[GENERATION],
        inventory_completeness="complete", status="ready",
        changes=[m.TargetChange(
            identity=target(), change="admit", reason="Explicit policy.", basis="explicit_policy",
        )],
        poll_count_delta=1, subscription_count_delta=0,
    )
    assert len(plan.scope_hash) == 64
    assert m.ActivationPlan.model_validate_json(plan.model_dump_json()) == plan
    request = m.ActivateScopeRequest(expected=version(), plan_id=plan.plan_id, idempotency_id=REQUEST)
    assert set(request.model_dump()) == {"expected", "plan_id", "idempotency_id"}
    with pytest.raises(ValidationError):
        m.ActivationPlan.model_validate({**plan.model_dump(), "expires_at": NOW})
    with pytest.raises(ValidationError):
        m.ActivationPlan.model_validate({**plan.model_dump(), "expected": version(epoch=OTHER)})
    with pytest.raises(ValidationError):
        m.TargetChange(identity=target(), change="remove", basis="uncertain_inventory", reason="Incomplete.")


def test_coverage_has_an_unknown_denominator_and_honest_counts() -> None:
    fields = {
        **version().model_dump(), "as_of": NOW, "inventory_completeness": "partial",
        "capability_completeness": "unknown", "scope_item_count": None,
        "discovered_count": 4, "access_verified_count": 2, "admitted_count": 2,
        "current_count": 1, "action_enabled_count": 0, "unsupported_count": 1, "backlog_count": 2,
        "gaps": [{"code": "domain_unavailable", "detail": "Domain membership is incomplete."}],
    }
    coverage = m.CoverageView.model_validate(fields)
    assert coverage.scope_item_count is None
    for changes in (
        {"gaps": []}, {"action_enabled_count": 2}, {"admitted_count": 4},
        {"inventory_completeness": "complete"},
    ):
        with pytest.raises(ValidationError):
            m.CoverageView.model_validate({**fields, **changes})


def test_parameters_preserve_values_and_reuse_pipeline_fingerprint() -> None:
    parameters = {"Region": "fixture-east", "Filters": {"dates": ["2026-01-01"], "note": "Sensitive synthetic evidence"}}
    reviewed = review(parameters=parameters)
    controller_boundary = PipelineTarget(
        name="Fixture pipeline", workspace_id=WORKSPACE, pipeline_id=ITEM,
        rerun_safe=True, rerun_parameters=parameters,
    )
    assert reviewed.parameters == parameters
    assert reviewed.parameter_hash == controller_boundary.parameter_hash
    assert reviewed.model_dump(mode="json")["parameter_hash"] == reviewed.parameter_hash
    assert observation(evidence=parameters).evidence == parameters
    assert review(parameters={}).parameter_hash != review(state="pending", parameters=None).parameter_hash
    parameters["Region"] = "Changed outside the validated record"
    assert reviewed.parameters["Region"] == "fixture-east"


def test_persisted_redaction_keeps_original_fingerprint_but_cannot_authorize_replay() -> None:
    reviewed = review(parameters={"Region": "Synthetic restricted value"})
    redacted = m.SafetyReview.model_validate({
        **reviewed.model_dump(), "parameters": None, "parameters_redacted": True,
        "state": "unverifiable", "detail": "Replay values were removed by persistence redaction.",
    })
    assert redacted.parameter_hash == reviewed.parameter_hash
    assert redacted.parameters is None
    with pytest.raises(ValidationError):
        m.SafetyReview.model_validate({**redacted.model_dump(), "state": "verified"})
    with pytest.raises(ValidationError):
        m.SafetyReview.model_validate({**redacted.model_dump(), "parameter_hash": None})
    with pytest.raises(ValidationError):
        review(parameter_hash=HASH)
    reviewed.parameters["Region"] = "Mutated after validation"
    with pytest.raises(ValidationError):
        m.SafetyReviewRequest(
            request_id=REQUEST, expected=version(), expected_review_revision=0, review=reviewed,
        )


@pytest.mark.parametrize(
    "parameters",
    [
        {"Region": 1, "region": 2}, {" ": 1}, {"x" * 257: 1}, {1: "not-a-string"},
        {"x": float("nan")}, {"x": float("inf")}, {"x": datetime(2026, 1, 1, tzinfo=UTC)},
        {"x": b"bytes"}, {"x": (1, 2)}, {"x": {1, 2}},
    ],
)
def test_parameters_are_bounded_json_not_implicitly_serialized_objects(parameters: object) -> None:
    with pytest.raises(ValidationError):
        review(parameters=parameters)


def test_evidence_size_depth_and_node_limits_are_enforced() -> None:
    for evidence in (
        {"x": "a" * m.MAX_JSON_BYTES},
        {"x": "z" * (m.MAX_JSON_BYTES + 1)},
        {"x": [0] * m.MAX_JSON_NODES},
    ):
        with pytest.raises(ValidationError):
            observation(evidence=evidence)
    nested: dict[str, object] = {"leaf": "value"}
    for _ in range(m.MAX_JSON_DEPTH + 1):
        nested = {"next": nested}
    with pytest.raises(ValidationError):
        observation(evidence=nested)
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValidationError):
        observation(evidence=cyclic)


@pytest.mark.parametrize(
    "changes",
    [
        {"parameters": None}, {"definition_hash": None}, {"replay_safe": False},
        {"exact_correlation_verified": False}, {"expires_at": NOW},
        {"state": "revoked"}, {"revoked_at": NOW},
    ],
)
def test_verified_review_requires_current_explicit_safety(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        review(**changes)


def test_review_mutation_advances_one_revision_and_does_not_supply_human_roles() -> None:
    request = m.SafetyReviewRequest(
        request_id=REQUEST, expected=version(), expected_review_revision=0, review=review(),
    )
    assert request.review.parameter_hash == review().parameter_hash
    with pytest.raises(ValidationError):
        m.SafetyReviewRequest.model_validate({**request.model_dump(), "expected_review_revision": 1})
    with pytest.raises(ValidationError):
        m.ApprovalReference(approval_id=OTHER, revision=1, fingerprint=HASH, granted=True)
    assert m.ApprovalReference(approval_id="fixture-approval", fingerprint="1234567890abcdef")
    with pytest.raises(ValidationError):
        m.ApprovalReference(approval_id=OTHER, revision=1, fingerprint=HASH)


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "cancelled"}, {"invocation": "manual"}, {"job_type": "Notebook"},
        {"authority": "transport", "origin": "event"}, {"ended_at": None}, {"started_at": None},
    ],
)
def test_pipeline_eligibility_is_not_inferred_from_a_failure_event(changes: dict[str, object]) -> None:
    assert observation().failed_scheduled_pipeline
    assert not observation(**changes).failed_scheduled_pipeline


def test_transport_dedup_identity_retains_original_source_and_event_id() -> None:
    first = signal()
    second_delivery = m.TransportDeliveryIdentity.model_validate({
        **first.delivery.model_dump(), "event_id": "fixture-event-2",
    })
    second = signal(delivery=second_delivery)
    assert first.delivery.key != second.delivery.key
    assert first.observation.key == second.observation.key
    moved = m.TransportDeliveryIdentity.model_validate({
        **first.delivery.model_dump(), "event_source": "/fixture/another-source",
    })
    assert moved.key != first.delivery.key
    assert "fixture-event-1" == first.delivery.event_id
    with pytest.raises(ValidationError):
        signal(observation=None)
    with pytest.raises(ValidationError):
        signal(partition=partition(connector_id=OTHER))


def test_quarantine_preserves_bounded_evidence_without_granting_admission() -> None:
    quarantine = m.QuarantineDisposition(
        observation_id="fixture-event-1", reason="wrong_tenant",
        detail="Claimed target tenant differs from deployment.", metadata={"source_note": "Synthetic evidence"},
    )
    receipt = signal(status="quarantined", quarantine=quarantine, observation=None)
    assert not receipt.quarantine.replayable
    assert receipt.observation is None
    assert receipt.quarantine.metadata["source_note"] == "Synthetic evidence"
    with pytest.raises(ValidationError):
        signal(status="quarantined")


def test_owned_connector_readiness_requires_roundtrip_and_delivery_proof() -> None:
    fields = {
        "tenant_id": TENANT, "epoch": EPOCH, "connector_id": CONNECTOR, "ownership_id": OWNER,
        "revision": 0, "policy_revision": 3, "workspace_id": OTHER, "eventstream_id": ITEM,
        "destination_id": "fixture-destination", "name": "Fixture connector",
        "sources": [{"source_id": "fixture-source", "target": target(), "event_types": ["ItemJobFailed"]}],
        "desired_definition": {"sources": ["fixture-source"]}, "state": "provisioning", "updated_at": NOW,
    }
    manifest = m.OwnedConnectorManifest.model_validate(fields)
    assert manifest.endpoint is None
    with pytest.raises(ValidationError):
        m.OwnedConnectorManifest.model_validate({**fields, "state": "ready"})
    ready = m.OwnedConnectorManifest.model_validate({
        **fields, "state": "ready", "observed_definition": fields["desired_definition"],
        "endpoint": {"namespace": "fixture.example.invalid", "entity": "fixture", "consumer_group": "$Default"},
        "identity_verified_at": NOW, "delivery_verified_at": NOW,
    })
    assert ready.state == "ready"
    with pytest.raises(ValidationError):
        m.OwnedConnectorManifest.model_validate({**ready.model_dump(), "observed_definition": {}})
    with pytest.raises(ValidationError):
        m.EndpointMetadata(namespace="https://fixture.example.invalid/", entity="fixture", consumer_group="$Default")


def test_rest_pages_cannot_skip_dispositions_or_claim_partial_complete() -> None:
    partial = page(next_cursor="page-2")
    assert not partial.window_complete
    complete = page(window_complete=True)
    assert complete.next_cursor is None
    assert page(received_count=0, observations=[], window_complete=True).window_complete
    for changes in (
        {"received_count": 2},
        {"window_complete": True, "next_cursor": "page-2"},
        {"window_complete": True, "retention_exhausted": True},
        {"expected_cursor": "page-2", "next_cursor": "page-2"},
        {"lease": lease(fence=3, resource_key="different-work")},
        {"observations": [observation(execution=execution(target=target(item_id=OTHER)))]},
        {"observations": [observation(origin="event", authority="transport")]},
        {"observations": [observation(origin="poll", authority="transport")]},
    ):
        with pytest.raises(ValidationError):
            page(**changes)
    with pytest.raises(ValidationError):
        page(received_count=m.MAX_INTAKE_BATCH + 1, observations=[observation()] * (m.MAX_INTAKE_BATCH + 1))


def test_partition_batches_are_fenced_bounded_and_ordered_but_do_not_imply_checkpointing() -> None:
    first = signal()
    second = signal(position={"offset": "44", "sequence_number": 9, "enqueued_at": NOW})
    request = m.StreamReceiptBatch(
        request_id=REQUEST, partition=partition(), lease=lease(resource_key=partition().key),
        receipts=[first, second],
    )
    assert [receipt.position.sequence_number for receipt in request.receipts] == [7, 9]
    # The store, not batch validation, proves the intervening position is durable.
    assert "checkpoint" not in request.model_dump()
    for changes in (
        {"receipts": [second, first]}, {"receipts": [first, first]}, {"receipts": []},
        {"lease": lease()}, {"partition": partition(epoch=OTHER)},
    ):
        with pytest.raises(ValidationError):
            m.StreamReceiptBatch.model_validate({**request.model_dump(), **changes})


def test_work_claims_bound_fair_shares_and_lease_fences() -> None:
    request = m.WorkClaimRequest(
        tenant_id=TENANT, epoch=EPOCH, owner_id=OWNER, kinds=["triage", "verify_action"],
        limit=10, per_workspace_limit=2,
    )
    assert request.per_workspace_limit == 2
    for changes in (
        {"limit": m.MAX_INTAKE_BATCH + 1}, {"limit": 1},
        {"kinds": ["triage", "triage"]}, {"lease_seconds": 0}, {"lease_seconds": True},
    ):
        with pytest.raises(ValidationError):
            m.WorkClaimRequest.model_validate({**request.model_dump(), **changes})
    with pytest.raises(ValidationError):
        lease(fence=0)
    assert work(state="leased", lease=lease()).lease.fence == 2


def test_discovery_selector_is_explicit_typed_and_context_bound() -> None:
    fields = {
        "tenant_id": TENANT, "epoch": EPOCH, "work_id": WORK, "kind": "inventory",
        "policy_revision": 0, "created_at": NOW, "due_at": NOW, "reason": "Explicit inventory request.",
    }
    selected = m.MonitoringWorkDraft(**fields, discovery_selector=selector())
    assert selected.discovery_selector == selector()
    assert selected.model_dump(mode="json")["discovery_selector"]["kind"] == "tenant"
    assert "inventory_selector" not in m.MonitoringWorkDraft.model_fields
    assert m.MonitoringWorkDraft(**fields, scope_id=SCOPE).discovery_selector is None
    for changes in (
        {},
        {"discovery_selector": selector(tenant_id=OTHER)},
        {"discovery_selector": selector(), "kind": "poll", "target": target()},
        {"inventory_selector": selector()},
    ):
        with pytest.raises(ValidationError):
            m.MonitoringWorkDraft.model_validate({**fields, **changes})


@pytest.mark.parametrize("requested_by", ["", " ", "x" * 257, 17])
def test_preview_requester_is_a_bounded_nonblank_audit_id(requested_by: object) -> None:
    with pytest.raises(ValidationError):
        m.ScopePreviewRequest(
            expected=version(), idempotency_id=REQUEST, scope=scope(), requested_by=requested_by,
        )


def test_work_completion_requires_a_durable_incident_receipt_and_preserves_followup_fences() -> None:
    with pytest.raises(ValidationError):
        work(state="completed", completed_at=LATER)
    completed = work(state="completed", completed_at=LATER, finalization_id=REQUEST)
    assert completed.finalization_id == REQUEST
    for changes in (
        {"state": "leased"}, {"state": "queued", "lease": lease()},
        {"kind": "verify_action"}, {"execution": None},
        {"execution": execution(target=target(item_id=OTHER))},
    ):
        with pytest.raises(ValidationError):
            work(**changes)
    followup = work(kind="verify_action", action_reservation_id=REQUEST)
    assert followup.action_reservation_id == REQUEST
    disposition = m.WorkDispositionRequest(
        tenant_id=TENANT, epoch=EPOCH, request_id=REQUEST, work_id=WORK,
        expected_work_revision=1, lease=lease(), disposition="retry",
        detail="Service throttled.", retry_at=LATER,
    )
    with pytest.raises(ValidationError):
        m.WorkDispositionRequest.model_validate({**disposition.model_dump(), "retry_at": None})


def test_reservation_request_has_current_state_references_not_caller_claims_of_authority() -> None:
    request = reservation_request()
    assert request.expected.revision == 3
    assert request.expected_review_revision == 1
    assert request.lease.fence == 2
    assert request.approval is None
    for field in ("remaining_budget", "approved", "action_enabled", "maintenance"):
        with pytest.raises(ValidationError):
            reservation_request(**{field: True})
    with pytest.raises(ValidationError):
        reservation_request(expected=version(epoch=OTHER))
    with pytest.raises(ValidationError):
        reservation_request(incident=incident_identity(target=target(item_id=OTHER)))
    with pytest.raises(ValidationError):
        reservation_request(action="powerbi_refresh")
    denied = m.ActionReservationDecision(status="denied", denial="approval_denied", detail="No explicit approval.")
    assert denied.reservation is None
    with pytest.raises(ValidationError):
        m.ActionReservationDecision(status="denied", detail="Missing reason.")


def test_source_failure_and_submitted_action_are_never_the_same_identity() -> None:
    fields = {
        "reservation_id": REQUEST, "request": reservation_request(), "revision": 1, "fence": 1,
        "reserved_at": NOW, "updated_at": LATER, "state": "submitted",
        "submitted_at": NOW, "submitted_execution": execution(run_id=OTHER),
        "next_verification_at": LATER, "detail": "Exact submitted fixture run.",
    }
    action = m.ActionReservation.model_validate(fields)
    assert action.request.source_execution.run_id == RUN
    assert action.submitted_execution.run_id == OTHER
    with pytest.raises(ValidationError):
        m.ActionReservation.model_validate({**fields, "submitted_execution": execution()})
    with pytest.raises(ValidationError):
        m.ActionReservation.model_validate({**fields, "submitted_execution": None})
    with pytest.raises(ValidationError):
        m.ActionReservation.model_validate({**fields, "next_verification_at": None})


def test_powerbi_uncorrelated_post_remains_uncertain() -> None:
    fields = {
        "tenant_id": TENANT, "epoch": EPOCH, "request_id": REQUEST, "reservation_id": REVIEW,
        "expected_reservation_revision": 1, "action_fence": 1, "state": "uncertain",
        "submitted_at": NOW, "next_verification_at": LATER, "detail": "Response identity unavailable.",
    }
    uncertain = m.ActionSubmissionRequest.model_validate(fields)
    assert uncertain.submitted_execution is None
    with pytest.raises(ValidationError):
        m.ActionSubmissionRequest.model_validate({**fields, "state": "submitted"})
    correlated = m.ActionSubmissionRequest.model_validate({
        **fields, "state": "submitted", "correlation": "response_run_id",
        "submitted_execution": execution(target=target(workload="powerbi"), run_id_kind="powerbi_request"),
    })
    assert correlated.submitted_execution.target.workload == "powerbi"
    with pytest.raises(ValidationError):
        m.ActionSubmissionRequest.model_validate({**correlated.model_dump(), "correlation": "first_unseen_refresh"})


def test_verified_outcomes_require_exact_rest_and_pipeline_activity_evidence() -> None:
    submitted = execution(run_id=OTHER)
    observed = observation(execution=submitted, status="succeeded")
    fields = {
        "tenant_id": TENANT, "epoch": EPOCH, "request_id": REQUEST, "reservation_id": REVIEW,
        "expected_reservation_revision": 2, "action_fence": 1,
        "disposition": "verified_succeeded", "submitted_execution": submitted,
        "observation": observed, "activities_complete": True,
        "activities": [PipelineActivity(name="Fixture activity", status="Succeeded")],
        "observed_at": LATER, "detail": "Correlated fixture evidence.",
    }
    verified = m.ActionOutcomeRequest.model_validate(fields)
    assert verified.disposition == "verified_succeeded"
    for changes in (
        {"observation": observation(status="succeeded")},
        {"observation": observation(execution=submitted, status="succeeded", authority="transport", origin="event")},
        {"observation": observation(execution=submitted)},
        {"activities_complete": False},
        {"activities": [PipelineActivity(name="Fixture activity", status="Failed")]},
        {"activities": []},
        {"activities": [PipelineActivity(name="Fixture activity", status="Completed")]},
        {"activities": [PipelineActivity(name="Fixture activity", status="Unknown")]},
        {"observation": observation(execution=submitted, status="succeeded", evidence_truncated=True)},
    ):
        with pytest.raises(ValidationError):
            m.ActionOutcomeRequest.model_validate({**fields, **changes})
    skipped = m.ActionOutcomeRequest.model_validate({
        **fields, "activities": [
            PipelineActivity(name="Fixture activity", status="Succeeded"),
            PipelineActivity(name="Conditional activity", status="Skipped"),
        ],
    })
    assert skipped.disposition == "verified_succeeded"


def test_non_job_actions_use_configuration_not_fabricated_execution_ids() -> None:
    expected = {"enabled": True}
    reviewed = review(
        target=target(workload="powerbi"), action="reenable_refresh_schedule",
        parameters=expected, configuration_hash=m._digest(expected),
        exact_correlation_verified=False,
    )
    evidence = m.ConfigurationVerification(
        target=reviewed.target, action="reenable_refresh_schedule",
        expected_hash=reviewed.configuration_hash, configuration=expected,
        observed_at=LATER, authority="fixture",
    )
    result = m.ActionOutcomeRequest(
        tenant_id=TENANT, epoch=EPOCH, request_id=REQUEST, reservation_id=REVIEW,
        expected_reservation_revision=2, action_fence=1, disposition="verified_succeeded",
        configuration=evidence, observed_at=LATER, detail="The exact schedule is enabled.",
    )
    assert result.submitted_execution is None
    with pytest.raises(ValidationError):
        m.ActionOutcomeRequest.model_validate({**result.model_dump(), "submitted_execution": execution()})


@pytest.mark.parametrize("state", ["pending", "verified"])
@pytest.mark.parametrize("parameters", [
    {"enabled": False}, {"enabled": 1}, {"enabled": "true"}, {},
    {"enabled": True, "days": ["Monday"]}, {"enabled": True, "times": ["12:00"]},
])
def test_schedule_review_authorizes_only_exact_reenable_intent(state: str, parameters: dict) -> None:
    with pytest.raises(ValidationError):
        review(
            target=target(workload="powerbi"), action="reenable_refresh_schedule", state=state,
            parameters=parameters, configuration_hash=m._digest(parameters),
            exact_correlation_verified=False,
        )


@pytest.mark.parametrize("parameters", [
    {"gateway_id": REVIEW, "datasource_ids": []},
    {"gateway_id": "", "datasource_ids": [RUN]},
    {"gateway_id": "00000000-0000-0000-0000-000000000000", "datasource_ids": [RUN]},
    {"gateway_id": REVIEW.upper(), "datasource_ids": [RUN]},
    {"gateway_id": REVIEW, "datasource_ids": [OTHER, RUN]},
    {"gateway_id": REVIEW, "datasource_ids": [RUN, RUN]},
    {"gateway_id": REVIEW, "datasource_ids": [""]},
    {"gateway_id": REVIEW, "datasource_ids": ["00000000-0000-0000-0000-000000000000"]},
    {"gateway_id": REVIEW, "datasource_ids": [REVIEW.upper()]},
    {"gateway_id": REVIEW, "datasource_ids": [RUN], "unrelated_setting": True},
])
def test_gateway_review_requires_exact_canonical_binding_intent(parameters: dict) -> None:
    with pytest.raises(ValidationError):
        review(
            target=target(workload="powerbi"), action="rebind_dataset_gateway",
            parameters=parameters, configuration_hash=m._digest(parameters),
            exact_correlation_verified=False,
        )


def test_gateway_review_accepts_sorted_canonical_nonempty_binding() -> None:
    parameters = {"gateway_id": REVIEW, "datasource_ids": [RUN, OTHER]}
    reviewed = review(
        target=target(workload="powerbi"), action="rebind_dataset_gateway",
        parameters=parameters, configuration_hash=m._digest(parameters),
        exact_correlation_verified=False,
    )
    assert reviewed.parameters == parameters
    assert reviewed.parameter_hash == reviewed.configuration_hash


def test_disabled_schedule_is_valid_observation_but_never_a_disable_review() -> None:
    observed = m.ConfigurationVerification(
        target=target(workload="powerbi"), action="reenable_refresh_schedule",
        expected_hash=m._digest({"enabled": True}), configuration={"enabled": False},
        observed_at=NOW, authority="rest",
    )
    assert observed.matches is False
    result = m.ActionOutcomeRequest(
        tenant_id=TENANT, epoch=EPOCH, request_id=REQUEST, reservation_id=REVIEW,
        expected_reservation_revision=2, action_fence=1, disposition="uncertain",
        configuration=observed, observed_at=NOW, detail="The schedule is not yet enabled.",
    )
    assert result.configuration.configuration == {"enabled": False}
    with pytest.raises(ValidationError):
        m.ActionOutcomeRequest.model_validate({**result.model_dump(), "disposition": "verified_succeeded"})
    with pytest.raises(ValidationError):
        m.ActionOutcomeRequest.model_validate({**result.model_dump(), "disposition": "verified_failed"})


def test_reservation_configuration_hash_cannot_override_reviewed_parameter_hash() -> None:
    model = target(workload="powerbi")
    with pytest.raises(ValidationError):
        reservation_request(
            source_execution=execution(target=model, run_id_kind="powerbi_request"),
            incident=incident_identity(target=model), action="reenable_refresh_schedule",
            parameter_hash=m._digest({"enabled": True}), configuration_hash=m._digest({"enabled": False}),
        )


def test_completed_pipeline_with_failed_activity_is_verified_failure_not_success() -> None:
    submitted = execution(run_id=OTHER)
    outcome = m.ActionOutcomeRequest(
        tenant_id=TENANT, epoch=EPOCH, request_id=REQUEST, reservation_id=REVIEW,
        expected_reservation_revision=2, action_fence=1, disposition="verified_failed",
        submitted_execution=submitted, observation=observation(execution=submitted, status="succeeded"),
        activities=(PipelineActivity(name="Handled failure", status="Failed"),),
        activities_complete=True, observed_at=LATER, detail="Completed job contains a failed activity.",
    )
    assert outcome.disposition == "verified_failed"
    with pytest.raises(ValidationError):
        m.ActionOutcomeRequest.model_validate({**outcome.model_dump(), "disposition": "verified_succeeded"})


@pytest.mark.parametrize("workload", ["fabric_pipeline", "powerbi"])
def test_cancelled_execution_requires_pipeline_and_complete_evidence(workload: str) -> None:
    identity = target(workload=workload)
    submitted = execution(
        target=identity, run_id=OTHER,
        run_id_kind="fabric_job" if workload == "fabric_pipeline" else "powerbi_request",
    )
    observed = observation(execution=submitted, status="cancelled", origin="poll", authority="rest")
    fields = {
        "tenant_id": TENANT, "epoch": EPOCH, "request_id": REQUEST, "reservation_id": REVIEW,
        "expected_reservation_revision": 2, "action_fence": 1, "disposition": "verified_failed",
        "submitted_execution": submitted, "observation": observed, "observed_at": LATER,
        "activities_complete": True,
        "detail": "The exact submitted execution was cancelled with complete REST evidence.",
    }
    if workload == "powerbi":
        with pytest.raises(ValidationError, match="authoritative run evidence"):
            m.ActionOutcomeRequest.model_validate(fields)
        assert m.ActionOutcomeRequest.model_validate({**fields, "disposition": "uncertain"}).observation.status == "cancelled"
        return
    result = m.ActionOutcomeRequest.model_validate(fields)
    assert result.observation.status == "cancelled"
    assert result.activities == () and result.activities_complete
    for changes in (
        {"disposition": "verified_succeeded"},
        {"activities_complete": False},
        {"observation": observed.model_copy(update={"started_at": None})},
        {"observation": observed.model_copy(update={"ended_at": None})},
        {"observation": observed.model_copy(update={"authority": "transport", "origin": "event"})},
    ):
        with pytest.raises(ValidationError):
            m.ActionOutcomeRequest.model_validate({**result.model_dump(), **changes})


def test_finalization_keeps_existing_incident_and_never_redacts_at_validation() -> None:
    request = finalization()
    assert isinstance(request.incident, Incident)
    assert request.incident.original_error == "Synthetic business evidence."
    assert "persisted" not in request.model_dump()
    assert m.WorkFinalizationRequest.model_validate_json(request.model_dump_json()) == request
    with pytest.raises(ValidationError):
        finalization(incident=Incident(id="fixture", signature="other-signature", outcome="needs_human"))
    with pytest.raises(ValidationError):
        finalization(lease=lease(resource_key="wrong-work"))
    with pytest.raises(ValidationError):
        finalization(incident=None)


def test_nested_models_write_and_reload_json_with_utc_datetimes(tmp_path: Path) -> None:
    records = [review(), page(), signal(), work(state="leased", lease=lease()), finalization()]
    for index, record in enumerate(records):
        path = tmp_path / f"monitoring-record-{index}.json"
        payload = record.model_dump(mode="json")
        path.write_text(json.dumps(payload, allow_nan=False), encoding="utf-8")
        restored = type(record).model_validate_json(path.read_text(encoding="utf-8"))
        assert restored == record
    assert json.loads((tmp_path / "monitoring-record-1.json").read_text(encoding="utf-8"))["lease"]["expires_at"].endswith("Z")


def test_public_record_schemas_are_json_safe() -> None:
    record_types = [
        value for value in vars(m).values()
        if inspect.isclass(value) and issubclass(value, m.MonitoringModel)
        and value.__module__ == m.__name__
    ]
    assert len(record_types) > 30
    for record_type in record_types:
        json.dumps(record_type.model_json_schema(), allow_nan=False)
    page_type = m.RecordPage[m.MonitoringTarget]
    response = page_type(version=version(), as_of=NOW, items=[admitted()])
    assert page_type.model_validate_json(response.model_dump_json()) == response


def test_store_contract_is_runtime_checkable_with_explicit_atomic_boundaries() -> None:
    assert MonitoringStore._is_protocol
    assert not isinstance(object(), MonitoringStore)
    required = {
        "inspect_bootstrap", "snapshot", "preview_scope", "activate_scope",
        "record_inventory", "record_capability", "record_connector", "resolve_target",
        "claim_work", "renew_lease", "record_rest_page", "get_rest_page",
        "record_stream_receipts", "advance_stream_checkpoint", "get_stream_acceptance",
        "record_safety_review", "reserve_action", "get_action_by_request",
        "record_action_submission", "record_action_outcome", "finalize_work", "get_finalization",
    }
    assert required.issubset(dir(MonitoringStore))
    assert not {"bootstrap", "migrate", "reset", "grant_role"}.intersection(vars(MonitoringStore))
    assert all(inspect.signature(getattr(MonitoringStore, method)).return_annotation is not inspect.Signature.empty for method in required)


def test_store_failure_contract_distinguishes_conflict_and_ambiguous_commit() -> None:
    for error_type in (MonitoringUnavailable, MonitoringNotBootstrapped, MonitoringSchemaMismatch, MonitoringConflict):
        assert issubclass(error_type, MonitoringStoreError)
    assert issubclass(MonitoringLeaseLost, MonitoringConflict)
    uncertain = MonitoringCommitUncertain("rest_page", REQUEST)
    assert uncertain.operation == "rest_page"
    assert uncertain.idempotency_id == REQUEST
    assert "reconcile receipt" in str(uncertain)
