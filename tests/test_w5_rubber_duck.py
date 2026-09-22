from __future__ import annotations

import asyncio
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from test_monitoring_controller import configured as configured
from test_pipeline_triage import (
    NOW,
    _reservation,
    _run,
    _target,
)
from test_pipeline_triage import pipeline_runner as pipeline_runner
from test_pipeline_triage import retry_playbook as retry_playbook

from triage.approvals import AutoApproveGate
from triage.monitoring.contracts import MonitoringUnavailable
from triage.monitoring.controller import MonitoringExecution, controller_heartbeat
from triage.monitoring.models import (
    IncidentIdentity,
    MonitoringWorkDraft,
    SourceExecutionIdentity,
    SourceRunObservation,
    WorkClaimRequest,
)
from triage.monitoring.runtime import (
    ensure_fixture_target,
    fixture_setup,
    fixture_target,
    target_signature,
)
from triage.pipeline_models import PipelineFailure, PipelineRun, PipelineTarget
from triage.runner import TriageRunner
from triage.store.incidents import InMemoryIncidentStore
from triage.tools.fabric_pipeline import MockFabricPipelineClient
from triage.tools.registry import ToolDispatcher


def history_row(execution, *, status="Failed", start=None, end=None):
    now = datetime.now(UTC)
    return {
        "requestId": execution.run_id, "status": status, "refreshType": "Scheduled",
        "startTime": (start or now - timedelta(minutes=2)).isoformat(),
        "endTime": end.isoformat() if end else None,
        "serviceExceptionJson": "TimeoutError: synthetic transport failure" if status == "Failed" else "",
    }


def bind_reader(store, ctx, client, test_settings, tmp_path):
    reader = TriageRunner(
        test_settings, base_dir=tmp_path, store=InMemoryIncidentStore(), monitoring_store=store,
    )
    # Exercise the REST-shaped core path with explicit in-process collaborators.
    reader.fixture = False
    reader._bind_powerbi_history(ctx.monitoring, client)
    source = ctx.monitoring.observation
    client.history = [history_row(source.execution, start=source.started_at, end=source.ended_at)]
    return reader


@pytest.mark.parametrize("status,newer", (("Completed", True), ("Unknown", True), ("Unknown", False)))
async def test_fresh_history_prevents_post_and_persists_other_execution(
    configured, test_settings, tmp_path, status, newer,
) -> None:
    store, _, _, client, ctx = configured()
    bind_reader(store, ctx, client, test_settings, tmp_path)
    source = ctx.monitoring.observation
    other = SourceExecutionIdentity(
        target=source.execution.target, run_id_kind="powerbi_request", run_id=str(uuid4()),
    )
    start = source.started_at + timedelta(seconds=10) if newer else source.started_at - timedelta(minutes=5)
    client.history.append(history_row(
        other, status=status, start=start,
        end=start + timedelta(seconds=1) if status == "Completed" else None,
    ))
    response = await ToolDispatcher(ctx).dispatch("refresh_powerbi_dataset", {"justification": "Transient"})
    assert response["status"] == "blocked_by_policy"
    assert ctx.ledger.write_actions == 0
    assert not any(name == "refresh_dataset" for name, _ in client.calls)
    recorded = store.get_source(other)
    assert recorded is not None
    assert recorded.status == ("succeeded" if status == "Completed" else "running")
    assert recorded.authority == "rest"
    assert store.get_source(source.execution).status == "failed"


@pytest.mark.parametrize("status", ("Completed", "Unknown"))
async def test_post_approval_history_is_reconciled_before_reservation(
    configured, test_settings, tmp_path, status,
) -> None:
    store, _, channel, client, ctx = configured("rebind_dataset_gateway")
    bind_reader(store, ctx, client, test_settings, tmp_path)
    source = ctx.monitoring.observation
    other = SourceExecutionIdentity(
        target=source.execution.target, run_id_kind="powerbi_request", run_id=str(uuid4()),
    )

    class NewHistory(AutoApproveGate):
        async def request_approval(self, request):
            start = source.started_at + timedelta(seconds=10)
            client.history.append(history_row(
                other, status=status, start=start,
                end=start + timedelta(seconds=1) if status == "Completed" else None,
            ))
            return await super().request_approval(request)

    ctx.approval_gate = NewHistory()
    response = await ToolDispatcher(ctx).dispatch("rebind_dataset_gateway", {
        "target_gateway": "gw-onprem-02", "justification": "Reviewed replacement.",
    })
    assert response["status"] == "blocked_by_policy"
    assert ctx.ledger.write_actions == 0
    assert not any(name == "rebind_gateway" for name, _ in client.calls)
    assert not channel.get(ctx.pending_approval[0].request_id)["consumed_at"]
    assert store.get_source(other).status == ("succeeded" if status == "Completed" else "running")
    assert store.get_incident_state(ctx.monitoring.incident) is None


def pipeline_work(runner, *, status="Failed", invocation="Scheduled", completed=True):
    identity = fixture_target("fabric_pipeline", str(uuid4()), str(uuid4()))
    with fixture_setup(runner.monitoring) as setup:
        ensure_fixture_target(setup, identity, "Pipeline candidate")
    context = runner.monitoring_context
    now = datetime.now(UTC)
    execution = SourceExecutionIdentity(target=identity, run_id_kind="fabric_job", run_id=str(uuid4()))
    runner.monitoring.enqueue_work(MonitoringWorkDraft(
        **context.model_dump(), work_id=str(uuid4()), kind="triage",
        policy_revision=runner.monitoring.snapshot(context).control.revision,
        target=identity, execution=execution, created_at=now, due_at=now,
        reason="Native failure event is a candidate, not eligibility.",
    ))
    work = runner.monitoring.claim_work(WorkClaimRequest(
        **context.model_dump(), owner_id=str(uuid4()), kinds=("triage",),
        limit=1, per_workspace_limit=1, lease_seconds=900,
    ))[0]
    run = PipelineRun(
        id=execution.run_id, item_id=identity.item_id, status=status, invoke_type=invocation,
        job_type="Pipeline", start_time=now - timedelta(minutes=2),
        end_time=now - timedelta(minutes=1) if completed else None,
        error_code="FixtureTransientFailure", failure_reason="Synthetic source status.",
    )
    return work, run


@pytest.mark.parametrize("status,invocation", (
    ("Completed", "Scheduled"), ("Cancelled", "Scheduled"), ("Failed", "Manual"), ("Deduped", "Scheduled"),
))
async def test_ineligible_pipeline_candidate_is_recorded_and_finalized_without_failure_model(
    runner, monkeypatch, status, invocation,
) -> None:
    work, run = pipeline_work(runner, status=status, invocation=invocation)

    class CandidateClient(MockFabricPipelineClient):
        async def list_runs(self, _target):
            raise AssertionError("History is unnecessary for an ineligible candidate")

        async def activity_runs(self, _target, _run):
            raise AssertionError("Activities are unnecessary for an ineligible candidate")

    def forbidden_failure(**_kwargs):
        raise AssertionError("An ineligible job cannot construct PipelineFailure")

    monkeypatch.setattr("triage.runner.PipelineFailure", forbidden_failure)
    summary = await runner.execute_monitoring_work(work, pipeline_client=CandidateClient([run]))
    stored = runner.monitoring.get_source(work.execution)
    assert stored.status == {"Completed": "succeeded", "Cancelled": "cancelled", "Failed": "failed", "Deduped": "unknown"}[status]
    assert stored.evidence["job_status"] == status
    assert stored.invocation == ("scheduled" if invocation == "Scheduled" else "manual")
    assert runner.monitoring.get_work(runner.monitoring_context, work.work_id).state == "completed"
    assert runner.monitoring.get_source_disposition(work.execution).disposition == "refused"
    assert "durably refused" in summary
    native = IncidentIdentity(target=work.target, signature=target_signature(work.target, run.error_text(), exception_class=run.error_code))
    assert runner.monitoring.get_incident_state(native) is None


@pytest.mark.parametrize("status,observed_status", (("InProgress", "running"), ("Failed", "failed")))
async def test_incomplete_pipeline_candidate_waits_durably_without_claiming_failure(
    runner, status, observed_status,
) -> None:
    work, run = pipeline_work(runner, status=status, completed=False)
    client = MockFabricPipelineClient([run])
    summary = await runner.execute_monitoring_work(work, pipeline_client=client)
    assert "read-only follow-up" in summary
    observed = runner.monitoring.get_source(work.execution)
    assert observed.status == observed_status and observed.ended_at is None
    assert runner.monitoring.get_work(runner.monitoring_context, work.work_id).state == "waiting"
    assert runner.monitoring.get_source_disposition(work.execution) is None
    assert client.calls == [("get_run", run.id)]


@pytest.mark.parametrize("leading_incomplete", [False, True])
async def test_ineligible_pipeline_candidates_do_not_abort_the_rest_of_the_drain(
    runner, monkeypatch, leading_incomplete,
) -> None:
    identity = fixture_target("fabric_pipeline", str(uuid4()), str(uuid4()))
    with fixture_setup(runner.monitoring) as setup:
        ensure_fixture_target(setup, identity, "Pipeline candidates")
    context = runner.monitoring_context
    now = datetime.now(UTC)
    runs = []
    work_ids = []
    statuses = ("Failed", "Completed", "Cancelled") if leading_incomplete else ("Completed", "Cancelled")
    for index, status in enumerate(statuses):
        execution = SourceExecutionIdentity(target=identity, run_id_kind="fabric_job", run_id=str(uuid4()))
        due = now - timedelta(seconds=len(statuses) - index)
        work = runner.monitoring.enqueue_work(MonitoringWorkDraft(
            **context.model_dump(), work_id=str(uuid4()), kind="triage",
            policy_revision=runner.monitoring.snapshot(context).control.revision,
            target=identity, execution=execution, created_at=due, due_at=due,
            reason="Candidate notification requires exact REST classification.",
        ))
        work_ids.append(work.work_id)
        runs.append(PipelineRun(
            id=execution.run_id, item_id=identity.item_id, status=status, invoke_type="Scheduled",
            job_type="Pipeline", start_time=now - timedelta(minutes=2),
            end_time=None if status == "Failed" else now - timedelta(minutes=1),
        ))
    monkeypatch.setattr(runner, "build_pipeline_client", lambda: MockFabricPipelineClient(runs))
    lines = await runner.drain_monitoring_work(limit=len(statuses))
    assert len(lines) == len(statuses)
    completed_offset = 1 if leading_incomplete else 0
    if leading_incomplete:
        assert "read-only follow-up" in lines[0]
        assert runner.monitoring.get_work(context, work_ids[0]).state == "waiting"
        source = SourceExecutionIdentity(target=identity, run_id_kind="fabric_job", run_id=runs[0].id)
        assert runner.monitoring.get_source(source).ended_at is None
        assert runner.monitoring.get_source_disposition(source) is None
    assert all("durably refused" in line for line in lines[completed_offset:])
    assert all(
        runner.monitoring.get_work(context, work_id).state == "completed"
        for work_id in work_ids[completed_offset:]
    )


@pytest.mark.parametrize("fail_finalization", (False, True))
async def test_pipeline_construction_crash_uses_canonical_finalization(runner, monkeypatch, fail_finalization) -> None:
    work, run = pipeline_work(runner)
    target = PipelineTarget(name="Pipeline candidate", workspace_id=work.target.workspace_id, pipeline_id=work.target.item_id)
    failure = PipelineFailure(target=target, run=run)
    signature = target_signature(work.target, failure.error_text(), exception_class=run.error_code)
    observed = SourceRunObservation(
        execution=work.execution, origin="fixture", authority="fixture",
        observed_at=datetime.now(UTC), started_at=run.start_time, ended_at=run.end_time,
        status="failed", invocation="scheduled", job_type=run.job_type, failure_reason=failure.error_text(),
    )
    runner.monitoring.observe_source(observed, work_id=work.work_id, lease=work.lease)
    monitoring = MonitoringExecution(
        store=runner.monitoring, work=work,
        incident=IncidentIdentity(target=work.target, signature=signature),
        observation=observed, fixture=True,
    )

    def crash(*_args, **_kwargs):
        raise RuntimeError("Synthetic provider construction failure")

    def wrong_store(*_args, **_kwargs):
        raise AssertionError("Canonical monitoring crashes must not call the standalone incident store")

    monkeypatch.setattr("triage.runner.get_provider", crash)
    monkeypatch.setattr(runner.store, "record", wrong_store)
    if fail_finalization:
        def unavailable(_request):
            raise MonitoringUnavailable("Synthetic finalization failure")
        monkeypatch.setattr(runner.monitoring, "finalize_work", unavailable)
        with pytest.raises(MonitoringUnavailable, match="finalization failure"):
            await runner._triage_pipeline(
                failure, client=MockFabricPipelineClient([run]),
                reruns=runner.build_pipeline_rerun_store(), monitoring=monitoring,
            )
        assert runner.monitoring.get_work(runner.monitoring_context, work.work_id).state == "leased"
        assert runner.monitoring.get_incident(monitoring.incident) is None
        assert runner.monitoring.get_source_disposition(work.execution) is None
    else:
        artifacts = await runner._triage_pipeline(
            failure, client=MockFabricPipelineClient([run]),
            reruns=runner.build_pipeline_rerun_store(), monitoring=monitoring,
        )
        assert artifacts.result.outcome == "agent_crashed"
        assert artifacts.monitoring_work_id == work.work_id
        assert artifacts.incident == runner.monitoring.get_incident(monitoring.incident)
        assert not artifacts.incident.id.startswith("usig:")
        assert runner.monitoring.get_source_disposition(work.execution).disposition == "failed"
        assert runner.monitoring.get_work(runner.monitoring_context, work.work_id).state == "completed"


async def test_pipeline_crash_finalization_retains_an_existing_action_fence(test_settings, tmp_path, monkeypatch) -> None:
    from test_monitoring_store import Harness

    h = Harness()
    h.seed()
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review())).reservation
    target = PipelineTarget(
        name="Reserved pipeline", workspace_id=h.targets[0].workspace_id, pipeline_id=h.targets[0].item_id,
        rerun_safe=True, rerun_parameters={},
    )
    run = PipelineRun(
        id=h.source.execution.run_id, item_id=target.pipeline_id, status="Failed",
        invoke_type="Scheduled", job_type="Pipeline",
        start_time=h.source.started_at, end_time=h.source.ended_at,
    )
    monitoring = MonitoringExecution(
        store=h.store, work=h.store.get_work(h.version, h.work.work_id),
        incident=action.request.incident, observation=h.source, reservation=action,
        fixture=True, clock=h.clock,
    )
    runner = TriageRunner(
        test_settings, base_dir=tmp_path, store=InMemoryIncidentStore(), monitoring_store=h.store,
    )

    def crash(*_args, **_kwargs):
        raise RuntimeError("Synthetic construction failure with a pre-existing fence")

    monkeypatch.setattr("triage.runner.get_provider", crash)
    result = await runner._triage_pipeline(
        PipelineFailure(target=target, run=run), client=MockFabricPipelineClient([run]),
        reruns=runner.build_pipeline_rerun_store(), monitoring=monitoring,
    )
    assert result.result.outcome == "agent_crashed"
    assert h.store.get_work(h.version, h.work.work_id).state == "completed"
    assert h.store.get_action_reservation(h.version, action.reservation_id).state == "reserved"
    assert h.store.get_incident_state(action.request.incident).action_count == 1
    assert h.state.approvals[action.request.approval.approval_id]["consumed_at"]


async def test_cancelled_rerun_is_terminal_non_success_without_budget_refund(pipeline_runner, retry_playbook) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([_run()], rerun_status="Cancelled")
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert report.artifacts
    artifact = report.artifacts[0]
    action = _reservation(pipeline_runner, artifact)
    assert action.state == "verified_failed"
    observed = pipeline_runner.monitoring.get_source(action.submitted_execution)
    assert observed.execution == action.submitted_execution
    assert observed.status == "cancelled"
    assert artifact.result.outcome == "needs_human"
    assert pipeline_runner.monitoring.get_incident_state(action.request.incident).action_count == 1
    assert pipeline_runner.build_approval_channel().get(action.request.approval.approval_id)["consumed_at"]
    assert pipeline_runner.monitoring.get_work(pipeline_runner.monitoring_context, artifact.monitoring_work_id).state == "completed"
    assert sum(name == "rerun" for name, _ in client.calls) == 1


async def test_approval_wait_does_not_block_other_bounded_heartbeat_slots(test_settings) -> None:
    held = asyncio.Event()
    entered = asyncio.Event()
    automatic_progress = asyncio.Event()
    human_progress = asyncio.Event()
    starts = Counter()
    active = Counter()
    maxima = Counter()

    async def drain(kind):
        starts[kind] += 1
        ordinal = starts[kind]
        active[kind] += 1
        maxima[kind] = max(maxima[kind], active[kind])
        maxima["total"] = max(maxima["total"], sum(active.values()))
        try:
            if kind == "automatic" and ordinal == 1:
                entered.set()
                await held.wait()
                return ["approval completed"]
            if ordinal > 4:
                return []
            await asyncio.sleep(0)
            if kind == "automatic" and ordinal == 4:
                automatic_progress.set()
            if kind == "human" and ordinal == 3:
                human_progress.set()
            return [f"{kind}-{ordinal}"]
        finally:
            active[kind] -= 1

    class Runner:
        settings = test_settings

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            assert limit == 1
            assert budget.can_claim()
            return await drain("automatic")

    async def human(_runner, *, limit, budget):
        assert limit == 1
        assert budget.can_claim()
        return await drain("human")

    heartbeat = asyncio.create_task(controller_heartbeat(Runner(), rounds=8, command_drain=human))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        await asyncio.wait_for(asyncio.gather(automatic_progress.wait(), human_progress.wait()), timeout=2)
        assert not held.is_set()
        assert not heartbeat.done()
        assert maxima["automatic"] == 2
        assert maxima["human"] == 1
        assert maxima["total"] <= 3
        assert starts["automatic"] <= 8 and starts["human"] <= 8
    finally:
        held.set()
        await asyncio.wait_for(heartbeat, timeout=2)


def test_controller_import_is_fresh_process_and_azure_free(repo_root) -> None:
    source = str(Path(repo_root) / "src")
    script = f"""
import importlib.abc
import sys
class NoLiveSDK(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {{'azure', 'mssql_python', 'agent_framework', 'agent_framework_foundry_hosting'}}:
            raise AssertionError('Live SDK imported: ' + fullname)
sys.meta_path.insert(0, NoLiveSDK())
sys.path.insert(0, {source!r})
from triage.monitoring.controller import controller_heartbeat
assert callable(controller_heartbeat)
assert 'triage.tools.registry' not in sys.modules
print('fresh Azure-free import passed')
"""
    result = subprocess.run([sys.executable, "-I", "-c", script], capture_output=True, text=True, timeout=30, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "fresh Azure-free import passed" in result.stdout
