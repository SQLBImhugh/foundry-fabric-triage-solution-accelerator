from __future__ import annotations

import json
from copy import deepcopy
from datetime import timedelta

import pytest
from test_monitoring_sql_action_bindings import ActionAbiDatabase
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
)
from triage.monitoring.controller import MonitoringExecution
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.sql_store import AzureSqlMonitoringStore
from triage.pipeline_models import PipelineFailure, PipelineRun, PipelineTarget
from triage.store.pipeline_reruns import InMemoryPipelineRerunStore
from triage.tools.fabric_pipeline import MockFabricPipelineClient
from triage.tools.pipeline_actions import PipelineToolContext


class LifecycleAbiDatabase(ActionAbiDatabase):
    """Exercise the submission/outcome adapter ABI, not native full-procedure proof."""

    def __init__(self, h):
        super().__init__(h)
        self.fail_transition_receipt = False

    def transition_action(self, args, work):
        if args["transition"] == "rejected":
            return super().transition_action(args, work)
        action = self.model("action", args["reservation_id"], m.ActionReservation)
        if action.revision != args["expected_action_revision"] or work.action_reservation_id != action.reservation_id:
            raise RuntimeError("Native action revision or lineage differs (51072)")
        if action.state in {"verified_succeeded", "verified_failed", "rejected"}:
            raise RuntimeError("Native terminal action cannot reopen (51072)")
        patch = json.loads(args["transition_json"])
        assert set(patch) <= {"submitted_execution", "submitted_at", "next_verification_at", "configuration", "detail"}
        updated = m.ActionReservation.model_validate({
            **action.model_dump(), **patch, "state": args["transition"],
            "revision": action.revision + 1, "updated_at": self.clock(),
        })
        owner = None
        if updated.state.startswith("verified_"):
            proof = self.model("controller_validation", action.reservation_id, m.ActionOutcomeValidation)
            if (
                proof is None or not proof.verified or proof.outcome != updated.state
                or proof.work_fence != work.lease.fence or proof.expires_at <= self.clock()
                or proof.submitted_execution != updated.submitted_execution or proof.configuration != updated.configuration
            ):
                raise RuntimeError("Native outcome lacks exact current controller proof (51072)")
            owner = json.loads(self.records[("action_owner", work.target.key)].payload)
            if owner["reservation_id"] != action.reservation_id or owner["fence"] != action.fence or not owner["active"]:
                raise RuntimeError("Native terminal action lost its target fence (51074)")
        if updated.submitted_execution is not None:
            if any(
                row.kind == "action" and row.key != action.reservation_id
                and m.ActionReservation.model_validate_json(row.payload).submitted_execution == updated.submitted_execution
                for row in self.records.values()
            ):
                raise RuntimeError("Submitted execution belongs to another action, even without an index (51072)")
            key = updated.submitted_execution.key
            correlation = {"reservation_id": action.reservation_id, "fence": action.fence, "active": True}
            prior = self.records.get(("submitted_action", key))
            if prior is not None and json.loads(prior.payload) != correlation:
                raise RuntimeError("Submitted correlation is immutable (51072)")
            if prior is None:
                self.native_put("submitted_action", key, correlation,
                                parent_key=action.reservation_id, target_key=work.target.key)
        self.native_put("action", action.reservation_id, updated.model_dump(mode="json"),
                        target_key=work.target.key, status=updated.state)
        if owner is not None:
            self.native_put("action_owner", work.target.key, {**owner, "active": False})
        reply = self.reply("controller.transition_action", args, {
            "reservation_id": action.reservation_id, "reservation": updated.model_dump(mode="json"), "retry_work": None,
        })
        if self.fail_transition_receipt:
            self.fail_transition_receipt = False
            raise RuntimeError("Injected action transition receipt failure (51072)")
        return reply


def pipeline_arguments(h, review):
    target = PipelineTarget(
        name="Synthetic reviewed pipeline", workspace_id=h.source.execution.target.workspace_id,
        pipeline_id=h.source.execution.target.item_id, rerun_safe=True, rerun_parameters=review.parameters,
    )
    run = PipelineRun(
        id=h.source.execution.run_id, item_id=target.pipeline_id,
        status="Failed", job_type="Pipeline", invoke_type="Scheduled",
        start_time=h.source.started_at, end_time=h.source.ended_at,
    )
    client = MockFabricPipelineClient([run])
    context = PipelineToolContext(
        failure=PipelineFailure(target=target, run=run), client=client,
        reruns=InMemoryPipelineRerunStore(), signature="fixture-failure",
    )
    arguments = context.approval_arguments("Replay only this reviewed synthetic failure.")
    assert client.calls == []
    return arguments


def setup(workload="fabric_pipeline", *, parameters=None):
    h = Harness()
    h.seed(workload=workload)
    h.activate()
    h.source_work()
    review = h.review("pipeline_rerun" if workload == "fabric_pipeline" else "powerbi_refresh", parameters)
    arguments = pipeline_arguments(h, review) if workload == "fabric_pipeline" else {
        "justification": "Refresh only this reviewed synthetic model.",
    }
    action = h.store.reserve_action(h.reserve_request(review, arguments=arguments)).reservation
    db = LifecycleAbiDatabase(h)
    store = AzureSqlMonitoringStore(db=db, component="controller")
    work = db.model("work", action.request.work_id, m.MonitoringWork)
    commit = m.CollectionCommit(work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision)
    submitted = m.SourceExecutionIdentity(
        target=action.request.source_execution.target, run_id=uid(79_000),
        run_id_kind="fabric_job" if workload == "fabric_pipeline" else "powerbi_request",
    )
    request = m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state="submitted", submitted_execution=submitted, correlation="response_run_id",
        submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(seconds=60),
        detail="The controller received the exact submitted execution ID.",
    )
    return h, db, store, action, commit, request


def outcome(h, action, *, status="failed"):
    return m.ActionOutcomeRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence, disposition="verified_failed",
        submitted_execution=action.submitted_execution, observed_at=h.clock(),
        observation=m.SourceRunObservation(
            execution=action.submitted_execution, origin="poll", authority="rest", observed_at=h.clock(),
            started_at=action.submitted_at, ended_at=h.clock(), status=status, invocation="manual", job_type="Pipeline",
        ),
        activities_complete=True, detail="Exact submitted execution is a complete terminal non-success.",
    )


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_nonfixture_lifecycle_requires_explicit_current_work_commit(backend):
    h, db, sql, action, commit, request = setup()
    store = sql if backend == "sql" else InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    with pytest.raises(MonitoringConflict, match="explicit current work commit"):
        store.record_action_submission(request)
    saved = store.record_action_submission(request, commit=commit)
    assert saved.submitted_execution == request.submitted_execution
    assert saved.request.source_execution == action.request.source_execution
    h.clock.advance(2)
    verification = outcome(h, saved)
    with pytest.raises(MonitoringConflict, match="explicit current work commit"):
        store.record_action_outcome(verification)
    verified = store.record_action_outcome(verification, commit=commit)
    assert verified.state == "verified_failed"


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_core_lifecycle_supplies_the_current_work_commit_without_fixture_authority(backend):
    h, db, sql, action, expected_commit, request = setup()
    store = sql if backend == "sql" else InMemoryMonitoringStore(clock=h.clock, state=h.state, component="controller")
    execution = MonitoringExecution(
        store=store, work=h.work, incident=action.request.incident, observation=h.source,
        reservation=action, clock=h.clock,
    )
    assert execution.work.revision < expected_commit.expected_work_revision
    submitted = execution._submission(
        execution=request.submitted_execution, detail=request.detail,
        submitted_at=request.submitted_at, retry_after_seconds=60,
    )
    assert execution.work.revision == expected_commit.expected_work_revision
    assert execution.work.lease == expected_commit.lease
    assert submitted.submitted_execution == request.submitted_execution
    h.clock.advance(2)
    evidence = outcome(h, submitted)
    execution._outcome(
        evidence.observation, succeeded=False, activities_complete=True, detail=evidence.detail,
    )
    assert execution.reservation.state == "verified_failed"
    assert execution.persistence_error is None
    assert store.get_incident_state(action.request.incident).action_count == 1


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_native_terminal_outcome_preserves_original_source_budget_approval_and_fence(status):
    h, db, store, action, commit, request = setup()
    budget = db.records[("incident_state", action.request.incident.key)]
    approvals = deepcopy(db.approvals)
    submitted = store.record_action_submission(request, commit=commit)
    assert submitted.submitted_execution != submitted.request.source_execution
    correlation = db.records[("submitted_action", submitted.submitted_execution.key)]
    assert json.loads(correlation.payload) == {
        "reservation_id": submitted.reservation_id, "fence": submitted.fence, "active": True,
    }
    assert any(row.kind == "work" and row.work_kind == "verify_action" for row in db.records.values())
    h.clock.advance(2)
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2, "maintenance": True})
    verification = outcome(h, submitted, status=status)
    terminal = store.record_action_outcome(verification, commit=commit)
    assert terminal.state == "verified_failed" and terminal.fence == action.fence
    assert terminal.submitted_execution == submitted.submitted_execution
    assert db.records[("submitted_action", submitted.submitted_execution.key)] == correlation
    assert db.records[("incident_state", action.request.incident.key)] == budget
    assert db.approvals == approvals
    assert json.loads(db.records[("action_owner", action.request.source_execution.target.key)].payload)["active"] is False
    assert store.get_work(h.version, action.request.work_id).state == "leased"
    assert store.get_source(submitted.submitted_execution).status == status
    h.clock.advance(300)
    assert AzureSqlMonitoringStore(db=db, component="controller").record_action_outcome(verification, commit=commit) == terminal


@pytest.mark.parametrize("failure", ["before", "after"])
def test_submission_lost_ack_replays_original_commit_before_new_work_or_policy_cas(failure):
    h, db, store, action, commit, request = setup()
    before = deepcopy((db.records, db.receipts, db.approvals))
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain) as uncertain:
        store.record_action_submission(request, commit=commit)
    assert uncertain.value.operation == "controller.transition_action"
    assert uncertain.value.idempotency_id == request.request_id
    if failure == "before":
        assert (db.records, db.receipts, db.approvals) == before
    else:
        db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2, "maintenance": True})
        h.clock.advance(300)
    saved = AzureSqlMonitoringStore(db=db, component="controller").record_action_submission(request, commit=commit)
    assert saved.state == "submitted" and saved.request == action.request
    assert saved.submitted_execution == request.submitted_execution
    assert len([row for row in db.records.values() if row.kind == "submitted_action"]) == 1


@pytest.mark.parametrize("parameters", [{"BatchId": 42, "Filters": {"enabled": True}}, {"Notes": "synthetic " * 250}])
def test_pipeline_lifecycle_keeps_the_actual_complete_approval_arguments(parameters):
    h, db, store, action, commit, request = setup(parameters=parameters)
    arguments = deepcopy(action.request.arguments)
    assert set(arguments) == {
        "workspace_id", "pipeline_id", "failed_run_id", "justification", "parameter_hash", "parameter_preview",
    }
    assert arguments["workspace_id"] == action.request.source_execution.target.workspace_id
    assert arguments["pipeline_id"] == action.request.source_execution.target.item_id
    assert arguments["failed_run_id"] == action.request.source_execution.run_id
    assert arguments["parameter_preview"] == json.dumps(parameters, sort_keys=True)[:1800]
    binding = db.model("approval_binding", action.request.approval.approval_id, m.ApprovalBinding)
    assert binding.arguments_hash == m._digest(arguments) != action.request.parameter_hash
    assert db.approvals[action.request.approval.approval_id]["arguments"] == arguments
    submitted = store.record_action_submission(request, commit=commit)
    h.clock.advance(2)
    terminal = store.record_action_outcome(outcome(h, submitted), commit=commit)
    assert terminal.request == action.request
    assert db.model("action", action.reservation_id, m.ActionReservation).request.arguments == arguments


def test_submission_correlation_and_action_roll_back_when_the_original_receipt_fails():
    h, db, store, _, commit, request = setup()
    before = deepcopy((db.records, db.receipts, db.approvals))
    db.fail_transition_receipt = True
    with pytest.raises(MonitoringConflict, match="51072"):
        store.record_action_submission(request, commit=commit)
    assert (db.records, db.receipts, db.approvals) == before
    saved = store.record_action_submission(request, commit=commit)
    assert json.loads(db.records[("submitted_action", saved.submitted_execution.key)].payload) == {
        "reservation_id": saved.reservation_id, "fence": saved.fence, "active": True,
    }


@pytest.mark.parametrize("change", ["owner", "fence", "revision", "other_work"])
def test_explicit_commit_cannot_borrow_other_work_ownership(change):
    h, db, store, _, commit, request = setup()
    changes = {
        "owner": {"lease": commit.lease.model_copy(update={"owner_id": uid(999)})},
        "fence": {"lease": commit.lease.model_copy(update={"fence": commit.lease.fence + 1})},
        "revision": {"expected_work_revision": commit.expected_work_revision + 1},
        "other_work": {"work_id": uid(999)},
    }
    before = deepcopy((db.records, db.receipts))
    with pytest.raises((MonitoringConflict, MonitoringLeaseLost)):
        store.record_action_submission(request, commit=m.CollectionCommit.model_validate({
            **commit.model_dump(), **changes[change],
        }))
    assert (db.records, db.receipts) == before


def test_missing_index_does_not_hide_exact_authoritative_action_correlation():
    h, db, store, _, commit, request = setup()
    saved = store.record_action_submission(request, commit=commit)
    # Explicit corruption case; a normal native transition must insert this row.
    db.records.pop(("submitted_action", saved.submitted_execution.key))
    with store._sql.transaction(write=False, operation="test_exact_submission", request_id=h.next_id()):
        owner = store._submitted_action_owner(saved.submitted_execution)
        assert owner.reservation_id == saved.reservation_id and owner.fence == saved.fence
        assert store._submitted_action_owner(saved.request.source_execution) is None
    assert not any(row.kind == "submitted_action" for row in db.records.values())
