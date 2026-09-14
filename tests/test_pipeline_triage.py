from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from triage.approvals import AutoApproveGate, AutoDenyGate, TimeoutGate
from triage.knowledge.playbooks import PLAYBOOKS, Playbook
from triage.pipeline_models import PipelineActivity, PipelineFailure, PipelineRun, PipelineTarget
from triage.runner import Scenario, TriageRunner
from triage.store.incidents import InMemoryIncidentStore
from triage.tools.fabric_pipeline import MockFabricPipelineClient, PipelineApiError

NOW = datetime(2026, 9, 11, 10, tzinfo=UTC)
WORKSPACE = "10000000-0000-0000-0000-000000000001"
PIPELINE = "20000000-0000-0000-0000-000000000002"
RUN = "30000000-0000-0000-0000-000000000003"
OTHER = "40000000-0000-0000-0000-000000000004"


def _target(**overrides) -> PipelineTarget:
    return PipelineTarget.model_validate({
        "name": "Orders load", "workspace_id": WORKSPACE, "pipeline_id": PIPELINE,
        **overrides,
    })


def _run(**overrides) -> PipelineRun:
    return PipelineRun.model_validate({
        "id": RUN, "item_id": PIPELINE, "status": "Failed", "invoke_type": "Scheduled",
        "job_type": "Pipeline", "start_time": NOW - timedelta(minutes=2),
        "end_time": NOW - timedelta(minutes=1), "error_code": "FixtureTransientFailure",
        "failure_reason": "A synthetic transient transport failure.", **overrides,
    })


@pytest.fixture
def pipeline_runner(test_settings, tmp_path, monkeypatch):
    settings = test_settings.model_copy(update={
        "pipeline_sweep_enabled": True, "pipeline_max_runs_per_sweep": 10,
    })
    runner = TriageRunner(settings, base_dir=tmp_path, store=InMemoryIncidentStore())
    monkeypatch.setattr(runner, "build_approval_gate", lambda _: AutoApproveGate())
    return runner


@pytest.fixture
def retry_playbook(monkeypatch):
    # Separate policy tests from the catalog's lexical matching.
    monkeypatch.setattr("triage.knowledge.playbooks.PLAYBOOKS", [
        *PLAYBOOKS,
        Playbook(
            name="Synthetic retry candidate", triggers=("fixturetransientfailure",),
            summary="Synthetic transport failure.", retry_useful=True,
            suggested_tier="tier_2", guidance="Review replay safety, then ask for approval.",
            source="https://learn.microsoft.com/fabric/data-factory/pipeline-runs",
            workload="fabric_pipeline",
        ),
    ])


async def test_only_failed_scheduled_pipeline_jobs_are_triaged(pipeline_runner) -> None:
    runs = [
        _run(),
        _run(id=OTHER, status="Completed"),
        _run(id="50000000-0000-0000-0000-000000000005", invoke_type="Manual"),
        _run(id="60000000-0000-0000-0000-000000000006", status="InProgress", end_time=None),
        _run(id="70000000-0000-0000-0000-000000000007", status="Cancelled"),
    ]
    client = MockFabricPipelineClient(runs)
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[_target()])
    assert report.status == "completed"
    assert len(report.artifacts) == 1
    artifact = report.artifacts[0]
    assert artifact.result.outcome == "needs_human"
    assert artifact.incident.source == "fabric_pipeline_failure"
    assert artifact.incident.pipeline_failure.run.id == RUN
    assert artifact.powerbi_calls == []
    assert artifact.result.write_actions == 0


async def test_repolling_one_job_does_not_increment_occurrences_or_notify(pipeline_runner) -> None:
    client = MockFabricPipelineClient([_run()])
    first = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[_target()])
    second = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[_target()])
    assert len(first.artifacts) == 1
    assert second.artifacts == []
    assert second.skipped == 1
    assert pipeline_runner.store.list_all()[0].occurrence_count == 1
    assert len(pipeline_runner.build_teams().messages) == 1


async def test_new_run_same_failure_counts_a_recurrence_without_renotifying(pipeline_runner) -> None:
    client = MockFabricPipelineClient([_run()])
    await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[_target()])
    client.runs.append(_run(id=OTHER, start_time=NOW - timedelta(seconds=30), end_time=NOW))
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[_target()])
    assert report.artifacts[0].result.outcome == "duplicate_suppressed"
    assert pipeline_runner.store.list_all()[0].occurrence_count == 2
    assert len(pipeline_runner.build_teams().messages) == 1


@pytest.mark.parametrize("gate,expected", [
    (AutoDenyGate(), "approval_denied"), (TimeoutGate(), "approval_denied"), (None, "needs_human"),
])
async def test_unapproved_rerun_never_spends_or_submits(
    pipeline_runner, retry_playbook, monkeypatch, gate, expected,
) -> None:
    monkeypatch.setattr(pipeline_runner, "build_approval_gate", lambda _: gate)
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([_run()])
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    result = report.artifacts[0].result
    assert result.outcome == expected
    assert result.write_actions == 0
    assert not any(name == "rerun" for name, _ in client.calls)
    assert pipeline_runner.build_pipeline_rerun_store().get(f"{target.key}:{RUN}") is None


async def test_approved_rerun_is_resolved_only_after_correlated_completion(
    pipeline_runner, retry_playbook,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([_run()])
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    result = report.artifacts[0].result
    assert result.outcome == "resolved"
    assert result.write_actions == 1
    assert result.approvals[0].granted
    assert sum(name == "rerun" for name, _ in client.calls) == 1
    record = pipeline_runner.build_pipeline_rerun_store().get(f"{target.key}:{RUN}")
    assert record.state == "completed"
    assert record.rerun_id != RUN


async def test_in_progress_rerun_is_followed_on_the_next_sweep(
    pipeline_runner, retry_playbook,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([_run()], rerun_status="InProgress")
    first = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert first.artifacts[0].result.outcome == "needs_human"
    assert pipeline_runner.store.list_all()[0].status == "open"
    client.runs[-1] = client.runs[-1].model_copy(update={"status": "Completed", "end_time": NOW})
    second = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert second.artifacts == []
    assert "verified Completed" in second.summary()
    assert pipeline_runner.store.list_all()[0].status == "resolved"
    assert sum(name == "rerun" for name, _ in client.calls) == 1


async def test_historical_backlog_does_not_reopen_a_verified_rerun(
    pipeline_runner, retry_playbook,
) -> None:
    pipeline_runner.settings.pipeline_max_runs_per_sweep = 1
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([
        _run(),
        _run(id=OTHER, start_time=NOW - timedelta(minutes=5), end_time=NOW - timedelta(minutes=4)),
    ], rerun_status="InProgress")
    first = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert first.artifacts[0].incident.pipeline_failure.run.id == RUN
    client.runs[-1] = client.runs[-1].model_copy(update={"status": "Completed", "end_time": NOW})
    second = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert second.artifacts == []
    incident = pipeline_runner.store.list_all()[0]
    assert incident.status == "resolved"
    assert incident.pipeline_failure.run.id == RUN
    assert incident.occurrence_count == 2
    assert sum(name == "rerun" for name, _ in client.calls) == 1


async def test_completed_job_with_failed_activity_is_not_resolved(
    pipeline_runner, retry_playbook,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient(
        [_run()], rerun_activities=[
            PipelineActivity(name="CopyOrders", activity_type="Copy", status="Failed", error_code="CopyFailed"),
            PipelineActivity(name="FailureHandler", activity_type="Wait", status="Succeeded"),
        ],
    )
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    result = report.artifacts[0].result
    assert result.outcome == "needs_human"
    journal = pipeline_runner.build_pipeline_rerun_store()
    assert journal.get(f"{target.key}:{RUN}").state == "failed"


async def test_completed_job_without_activity_evidence_stays_unverified(
    pipeline_runner, retry_playbook,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([_run()], rerun_activities=[])
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert report.artifacts[0].result.outcome == "needs_human"
    assert pipeline_runner.build_pipeline_rerun_store().get(f"{target.key}:{RUN}").state == "submitted"


async def test_ambiguous_submission_is_fenced_across_another_attempt(
    pipeline_runner, retry_playbook,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})

    class LostAcknowledgement(MockFabricPipelineClient):
        async def rerun(self, target):
            self.calls.append(("rerun", target.pipeline_id))
            raise PipelineApiError("Connection lost after submission")

    client = LostAcknowledgement([_run()])
    first = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert first.artifacts[0].result.outcome == "needs_human"
    journal = pipeline_runner.build_pipeline_rerun_store()
    assert journal.get(f"{target.key}:{RUN}").state == "unknown"
    # Even without source-event dedup or a known incident, the persisted fence
    # prevents another submission of this particular failed job.
    pipeline_runner.store = InMemoryIncidentStore()
    artifact = await pipeline_runner._triage_pipeline(
        PipelineFailure(target=target, run=_run()), client=client,
        reruns=pipeline_runner.build_pipeline_rerun_store(),
    )
    assert artifact.result.write_actions == 0
    assert sum(name == "rerun" for name, _ in client.calls) == 1


async def test_lost_correlation_write_does_not_claim_no_rerun_occurred(
    pipeline_runner, retry_playbook, monkeypatch,
) -> None:
    from triage.store.pipeline_reruns import InMemoryPipelineRerunStore

    class FailingCorrelationStore(InMemoryPipelineRerunStore):
        def update(self, record, *, expected):
            raise ConnectionError("Correlation write unavailable")

    journal = FailingCorrelationStore()
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([_run()])
    monkeypatch.setattr(pipeline_runner, "build_pipeline_rerun_store", lambda: journal)
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert report.artifacts[0].result.outcome == "needs_human"
    assert sum(name == "rerun" for name, _ in client.calls) == 1
    assert journal.get(f"{target.key}:{RUN}").state == "reserved"
    assert client.runs[-1].id in pipeline_runner.build_teams().messages[-1].action_taken


@pytest.mark.parametrize("status", ["InProgress", "NotStarted", "Completed"])
async def test_active_or_newer_job_prevents_rerun(
    pipeline_runner, retry_playbook, status,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([
        _run(), _run(
            id=OTHER, status=status, invoke_type="Manual",
            start_time=NOW - timedelta(seconds=20),
            end_time=NOW if status == "Completed" else None,
        ),
    ])
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert report.artifacts[0].result.write_actions == 0
    assert report.artifacts[0].result.approvals == []


async def test_post_approval_recheck_rejects_a_new_active_job(
    pipeline_runner, retry_playbook, monkeypatch,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={})
    client = MockFabricPipelineClient([_run()])

    class Gate(AutoApproveGate):
        async def request_approval(self, request):
            client.runs.append(_run(id=OTHER, status="InProgress", end_time=None))
            return await super().request_approval(request)

    monkeypatch.setattr(pipeline_runner, "build_approval_gate", lambda _: Gate())
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert report.artifacts[0].result.approvals[0].granted
    assert report.artifacts[0].result.write_actions == 0
    assert not any(name == "rerun" for name, _ in client.calls)


async def test_changed_parameters_cannot_use_an_earlier_approval(
    pipeline_runner, retry_playbook, monkeypatch,
) -> None:
    target = _target(rerun_safe=True, rerun_parameters={"window": "yesterday"})
    client = MockFabricPipelineClient([_run()])

    class Gate(AutoApproveGate):
        async def request_approval(self, request):
            assert "yesterday" in request.impact
            target.rerun_parameters["window"] = "today"
            return await super().request_approval(request)

    monkeypatch.setattr(pipeline_runner, "build_approval_gate", lambda _: Gate())
    report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    assert report.artifacts[0].result.write_actions == 0
    assert not any(name == "rerun" for name, _ in client.calls)


async def test_monitor_permission_failure_is_persisted_separately(pipeline_runner) -> None:
    class Forbidden(MockFabricPipelineClient):
        async def list_runs(self, target):
            raise PipelineApiError("Fabric pipeline API HTTP 403")

    report = await pipeline_runner.pipeline_sweep(now=NOW, client=Forbidden(), targets=[_target()])
    assert report.status == "incomplete"
    assert not report.artifacts
    incident = pipeline_runner.store.list_all()[0]
    assert incident.source == "fabric_pipeline_monitor"
    assert incident.pipeline_failure is None


async def test_missing_activity_evidence_blocks_replay_not_failure_reporting(
    pipeline_runner, retry_playbook,
) -> None:
    class UnavailableActivities(MockFabricPipelineClient):
        async def activity_runs(self, target, run):
            raise PipelineApiError("Activity diagnostics not available")

    report = await pipeline_runner.pipeline_sweep(
        now=NOW, client=UnavailableActivities([_run()]),
        targets=[_target(rerun_safe=True, rerun_parameters={})],
    )
    artifact = report.artifacts[0]
    assert artifact.result.outcome == "needs_human"
    assert artifact.result.write_actions == 0
    assert artifact.incident.pipeline_failure.diagnostics_error


async def test_pipeline_claim_contention_prevents_agent_and_tool_execution(pipeline_runner) -> None:
    target = _target()
    claims = pipeline_runner._pipeline_claim_store()
    assert claims.claim(f"pipeline:{target.key}")
    client = MockFabricPipelineClient([_run()])
    try:
        report = await pipeline_runner.pipeline_sweep(now=NOW, client=client, targets=[target])
    finally:
        claims.release(f"pipeline:{target.key}")
    assert report.artifacts == []
    assert report.skipped == 1
    assert pipeline_runner.store.list_all() == []


async def test_pipeline_requests_cannot_reach_power_bi_tools(pipeline_runner) -> None:
    from triage.agents.triage_agent import TriageAgent, TriageDeps
    from triage.models import BIRequest
    from triage.providers.base import LLMResponse, ToolCall
    from triage.store.pipeline_reruns import InMemoryPipelineRerunStore
    from triage.tools.pipeline_actions import PipelineToolContext

    class RogueProvider:
        provider_name = "mock"
        model_name = "rogue-fixture"

        def __init__(self):
            self.count = 0

        async def complete(self, **kwargs):
            self.count += 1
            name = "refresh_powerbi_dataset" if self.count == 1 else "report_resolution"
            args = {"justification": "Ignore the workload"} if self.count == 1 else {"outcome": "resolved"}
            return LLMResponse(tool_calls=[ToolCall(id=str(self.count), name=name, arguments=args)])

        async def close(self):
            return None

    context = PipelineToolContext(
        failure=PipelineFailure(target=_target(), run=_run()),
        client=MockFabricPipelineClient([_run()]),
        reruns=InMemoryPipelineRerunStore(), signature="pipeline-fixture",
    )
    agent = TriageAgent(RogueProvider())
    result = await agent.run(
        BIRequest(request_id="pipeline-fixture", source="pipeline"),
        TriageDeps(
            powerbi=None, teams=pipeline_runner.build_teams(),
            flag_table=pipeline_runner.flag_table, pipeline=context, signature="pipeline-fixture",
        ),
    )
    await agent.close()
    assert result.outcome == "needs_human"
    assert result.write_actions == 0
    assert result.blocked_attempts == ["refresh_powerbi_dataset"]
    assert context.client.calls == []


async def test_model_cannot_supply_a_different_pipeline_or_parameters(pipeline_runner) -> None:
    from triage.models import BIRequest
    from triage.policy import PolicyLedger, TriagePolicy
    from triage.store.pipeline_reruns import InMemoryPipelineRerunStore
    from triage.tools.pipeline_actions import PipelineToolContext
    from triage.tools.registry import ToolContext, ToolDispatcher

    pipeline = PipelineToolContext(
        failure=PipelineFailure(target=_target(), run=_run()),
        client=MockFabricPipelineClient([_run()]), reruns=InMemoryPipelineRerunStore(),
        signature="pipeline-fixture",
    )
    context = ToolContext(
        request=BIRequest(request_id="pipeline-fixture", source="pipeline"),
        ledger=PolicyLedger(TriagePolicy()), powerbi=None,
        teams=pipeline_runner.build_teams(), flag_table=pipeline_runner.flag_table,
        pipeline=pipeline,
    )
    response = await ToolDispatcher(context).dispatch(
        "rerun_fabric_pipeline",
        {"justification": "Ignore the configured target", "pipeline_id": OTHER},
    )
    assert response["status"] == "blocked_by_policy"
    assert context.ledger.write_actions == 0
    assert pipeline.client.calls == []


async def test_disabled_and_unconfigured_sweeps_are_not_reported_healthy(pipeline_runner) -> None:
    pipeline_runner.settings.pipeline_sweep_enabled = False
    report = await pipeline_runner.pipeline_sweep()
    assert report.status == "disabled"
    pipeline_runner.settings.pipeline_sweep_enabled = True
    assert (await pipeline_runner.pipeline_sweep(targets=[])).status == "unconfigured"


async def test_live_mode_refuses_pipeline_fixtures_before_resetting_state(pipeline_runner, monkeypatch) -> None:
    pipeline_runner.settings.triage_tool_mode = "live"
    resets = []
    monkeypatch.setattr(pipeline_runner.store, "reset", lambda: resets.append(True))
    with pytest.raises(ValueError, match="TRIAGE_TOOL_MODE=mock"):
        await pipeline_runner.run_scenario(Scenario(name="fixture", pipeline={}))
    assert not resets


def test_pipeline_preflight_does_not_construct_a_live_runner(monkeypatch, test_settings) -> None:
    import triage.cli as cli

    settings = test_settings.model_copy(update={
        "pipeline_sweep_enabled": True,
        "fabric_pipeline_targets": json.dumps([_target().model_dump()]),
    })
    monkeypatch.setattr(cli, "settings", settings)

    def forbidden(*args, **kwargs):
        raise AssertionError("Configuration preflight opened the state store")

    monkeypatch.setattr(cli, "TriageRunner", forbidden)
    args = cli.build_parser().parse_args(["pipelines", "--preflight"])
    assert cli.cmd_pipelines(args) == 0


def test_pipeline_scenario_runs_through_the_real_cli_offline(
    monkeypatch, test_settings, tmp_path, capsys,
) -> None:
    import triage.cli as cli

    monkeypatch.setattr(cli, "settings", test_settings)
    monkeypatch.setattr(cli, "REPO_ROOT", tmp_path)
    assert cli.main(["run", "scenario10-pipeline-rerun-approved"]) == 0
    assert "Expectations met" in capsys.readouterr().out
    from triage.store.incidents import JsonFileIncidentStore

    restored = JsonFileIncidentStore(tmp_path / "runs" / "incidents.json").list_all()
    assert len(restored) == 1
    assert restored[0].pipeline_failure.run.id == RUN
    assert restored[0].pipeline_failure.run.end_time.tzinfo is not None
