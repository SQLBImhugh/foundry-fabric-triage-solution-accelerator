from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from triage.models import BIRequest
from triage.monitoring.runtime import (
    ensure_fixture_target,
    fixture_component,
    fixture_setup,
    fixture_target,
)
from triage.runner import Scenario


@pytest.fixture
def command_target(runner):
    with fixture_setup(runner.monitoring) as setup:
        return ensure_fixture_target(
            setup, fixture_target("powerbi", "workspace", "dataset"), "Synthetic dataset",
        )


async def test_full_result_and_safe_progress_events_are_recorded(runner, repo_root) -> None:
    from triage.store.command_center import InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.run_history_enabled = True
    scenario = Scenario.load(repo_root / "scenarios" / "scenario1-transient.yaml")
    artifact = (await runner.run_scenario(scenario))[-1]
    saved = store.get_run(artifact.run_id)
    assert saved.incident_id == artifact.incident.id
    assert saved.result.outcome == "resolved"
    assert saved.result.actions
    assert saved.result.tool_calls == artifact.result.tool_calls
    events = store.events(artifact.run_id)
    assert any(event.kind == "tool_completed" for event in events)
    assert not any(event.kind == "thinking" for event in events)


async def test_web_notification_is_durable_before_reporting_delivery(runner, repo_root) -> None:
    from triage.store.command_center import InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.run_history_enabled = True
    runner.settings.notification_channel = "web"
    scenario = Scenario.load(repo_root / "scenarios" / "scenario1-transient.yaml")
    artifact = (await runner.run_scenario(scenario))[-1]
    assert artifact.result.notification_delivered
    assert not artifact.result.notification_failed
    events = store.events(artifact.run_id)
    assert any(event.kind == "notification" and event.status == "recorded" for event in events)
    assert artifact.teams_messages == []


async def test_failed_history_start_prevents_untracked_execution(runner, monkeypatch) -> None:
    class Unavailable:
        def start_run(self, _record):
            raise RuntimeError("History unavailable")

    calls = []
    runner.settings.run_history_enabled = True
    runner._command_center_store = Unavailable()
    monkeypatch.setattr(runner, "build_powerbi", lambda *_: calls.append("built"))
    with pytest.raises(RuntimeError, match="History unavailable"):
        await runner.run_request(BIRequest(request_id="test", subject="Synthetic alert"))
    assert not calls


async def test_operator_command_is_claimed_once_and_not_replayed(runner, command_target, monkeypatch) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    command = CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id=command_target.key,
        actor_id="operator", subject="Synthetic refresh failure",
    )
    store.enqueue(command)
    seen = []

    async def execute(request):
        seen.append(request)
        return SimpleNamespace(
            result=SimpleNamespace(outcome="needs_human", summary="Investigation recorded", write_actions=0),
            run_id=str(uuid4()),
        )

    monkeypatch.setattr(runner, "run_request", execute)
    assert len(await drain_commands(runner)) == 1
    assert await drain_commands(runner) == []
    assert len(seen) == 1
    assert seen[0].source == "web"
    assert store.get_command(command.id).state == "completed"


async def test_selected_pipeline_command_uses_the_delivered_core_binding(runner, monkeypatch) -> None:
    from triage.command_center.models import Actor, CommandInput, WebSettings
    from triage.command_center.service import CommandCenterService
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import InMemoryCommandCenterStore

    runner.settings = runner.settings.model_copy(update={"pipeline_sweep_enabled": True})
    with fixture_setup(runner.monitoring) as setup:
        selected = ensure_fixture_target(
            setup, fixture_target("fabric_pipeline", "workspace-a", "pipeline-a"),
            "Same display name",
        )
        ensure_fixture_target(
            setup, fixture_target("fabric_pipeline", "workspace-b", "pipeline-b"),
            "Same display name",
        )
    history = InMemoryCommandCenterStore()
    runner._command_center_store = history
    runtime = CommandCenterService(
        runner.settings, WebSettings(_env_file=None, mode="demo", demo_worker=False),
        monitoring_store=fixture_component(runner.monitoring, "web"), history=history, incidents=runner.store,
        approvals=runner.build_approval_channel(),
    )

    class ReadOnlyPipelineClient:
        def __init__(self):
            self.targets = []
            self.closed = False

        async def list_runs(self, target):
            self.targets.append((target.workspace_id, target.pipeline_id))
            return []

        async def close(self):
            self.closed = True

    client = ReadOnlyPipelineClient()
    monkeypatch.setattr(runner, "build_pipeline_client", lambda: client)
    value = CommandInput(
        kind="pipeline_sweep", target_id=selected.key, idempotency_key=str(uuid4()),
    )
    runtime.enqueue(value, Actor(id="operator", display_name="Operator", roles=["operator"]))
    before = runner.settings.model_dump()
    lines = await drain_commands(runner)
    assert len(lines) == 1
    assert client.targets == [(selected.identity.workspace_id, selected.identity.item_id)]
    assert client.closed
    assert runtime.monitoring.store.component == "web" and runner.monitoring.component == "controller"
    assert runtime.monitoring.store._backend.state is runner.monitoring._backend.state
    assert history.get_command(value.idempotency_key).state == "completed"
    assert history.get_command(value.idempotency_key).run_id == ""
    assert runner.settings.model_dump() == before
    assert runner.store.list_all() == []
    assert await drain_commands(runner) == []


async def test_changed_target_does_not_execute_an_operator_command(runner, monkeypatch) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    command = CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id="removed-target", actor_id="operator",
    )
    store.enqueue(command)
    calls = []

    async def forbidden(*args, **kwargs):
        calls.append(True)

    monkeypatch.setattr(runner, "run_request", forbidden)
    await drain_commands(runner)
    assert not calls
    assert store.get_command(command.id).state == "failed"


async def test_distinct_commands_for_one_target_do_not_overlap(runner, command_target, monkeypatch) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    for _ in range(2):
        store.enqueue(CommandRecord(
            id=str(uuid4()), kind="powerbi_triage", target_id=command_target.key,
            actor_id="operator", subject="Synthetic failure",
        ))
    active, peak, calls = 0, 0, 0

    async def execute(request):
        nonlocal active, peak, calls
        calls += 1
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.03)
        active -= 1
        return SimpleNamespace(
            result=SimpleNamespace(outcome="needs_human", summary="Read only", write_actions=0),
            run_id=str(uuid4()),
        )

    monkeypatch.setattr(runner, "run_request", execute)
    await asyncio.gather(drain_commands(runner), drain_commands(runner))
    assert peak == 1
    assert calls == 1
    assert len(store.queued_commands()) == 1
    await drain_commands(runner)
    assert calls == 2


async def test_timeout_retains_target_uncertainty_without_reexecution(runner, command_target, monkeypatch) -> None:
    import triage.command_center.worker as worker
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    for _ in range(2):
        store.enqueue(CommandRecord(
            id=str(uuid4()), kind="powerbi_triage", target_id=command_target.key,
            actor_id="operator", subject="Synthetic failure",
        ))
    writes = []
    timeout = asyncio.timeout
    monkeypatch.setattr(worker.asyncio, "timeout", lambda _: timeout(0.01))

    async def execute(request):
        writes.append("accepted")
        await asyncio.sleep(1)

    monkeypatch.setattr(runner, "run_request", execute)
    await worker.drain_commands(runner)
    await worker.drain_commands(runner)
    assert writes == ["accepted"]
    assert store.target_blocked(command_target.key)
    assert sorted(row.state for row in store.commands()) == ["interrupted", "queued"]
    assert runner.store.list_all() == []


async def test_expired_finalization_is_not_retried_as_execution_failure(
    runner, command_target, monkeypatch,
) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    store.enqueue(CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id=command_target.key, actor_id="operator",
    ))
    finishes = []
    finish = store.finish_command

    def record_finish(*args, **kwargs):
        finishes.append(True)
        return finish(*args, **kwargs)

    async def execute(request):
        store.expire_commands(now=datetime.now(UTC) + timedelta(days=1))
        return SimpleNamespace(
            result=SimpleNamespace(outcome="resolved", summary="Verified", write_actions=1),
            run_id=str(uuid4()),
        )

    monkeypatch.setattr(store, "finish_command", record_finish)
    monkeypatch.setattr(runner, "run_request", execute)
    await drain_commands(runner)
    assert finishes == [True]
    assert store.commands()[0].state == "interrupted"


@pytest.mark.parametrize("overrun", [0, 61])
async def test_command_acquisition_cannot_extend_the_execution_deadline(
    runner, command_target, monkeypatch, overrun,
) -> None:
    import triage.command_center.worker as worker
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    command = CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id=command_target.key,
        actor_id="operator",
    )
    store.enqueue(command)
    elapsed = [0]
    monkeypatch.setattr(worker, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))
    claim = store.claim_command

    def slow_claim(*args, **kwargs):
        result = claim(*args, **kwargs)
        elapsed[0] = (
            runner.settings.triage_timeout_seconds
            + runner.settings.approval_timeout_seconds + 30 + overrun
        )
        return result

    calls = []

    async def execute(request):
        calls.append(request)
        return SimpleNamespace(
            result=SimpleNamespace(outcome="needs_human", summary="Must not run", write_actions=0),
            run_id=str(uuid4()),
        )

    monkeypatch.setattr(store, "claim_command", slow_claim)
    monkeypatch.setattr(runner, "run_request", execute)
    await worker.drain_commands(runner)
    assert calls == []
    assert store.get_command(command.id).state == "failed"
    assert "not executed" in store.get_command(command.id).summary
    assert runner.store.list_all() == []


async def test_unrelated_command_progresses_behind_100_blocked_target_commands(
    runner, command_target, monkeypatch,
) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    blocked = CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id="previously-configured-target",
        actor_id="operator",
    )
    store.enqueue(blocked)
    store.claim_command(blocked.id, "previous-worker")
    store.interrupt_command(blocked.id, "previous-worker", "Requires reconciliation")
    for _ in range(100):
        store.enqueue(blocked.model_copy(update={"id": str(uuid4())}))
    eligible = blocked.model_copy(update={
        "id": str(uuid4()), "target_id": command_target.key,
        "created_at": (datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
    })
    store.enqueue(eligible)
    calls = []

    async def execute(request):
        calls.append(request)
        return SimpleNamespace(
            result=SimpleNamespace(outcome="needs_human", summary="Read-only investigation", write_actions=0),
            run_id=str(uuid4()),
        )

    monkeypatch.setattr(runner, "run_request", execute)
    await drain_commands(runner)
    assert len(calls) == 1
    assert store.get_command(eligible.id).state == "completed"
    assert store.target_blocked(blocked.target_id)
    assert len(store.queued_commands()) == 100


async def test_heartbeat_deadline_leaves_second_long_command_queued(runner, command_target, monkeypatch):
    import triage.command_center.worker as worker
    from triage.monitoring.controller import controller_heartbeat
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    for _ in range(2):
        store.enqueue(CommandRecord(
            id=str(uuid4()), kind="powerbi_triage", target_id=command_target.key, actor_id="operator",
        ))
    elapsed, executed = [0.0], []
    monkeypatch.setattr(worker, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))

    async def automatic(*, limit, budget, prefer="reconcile_state"):
        return []

    async def execute(request):
        executed.append(request.request_id)
        elapsed[0] += 500
        return SimpleNamespace(
            result=SimpleNamespace(outcome="needs_human", summary="Recorded", write_actions=0),
            run_id=str(uuid4()),
        )

    monkeypatch.setattr(runner, "drain_monitoring_work", automatic)
    monkeypatch.setattr(runner, "run_request", execute)
    assert len(await controller_heartbeat(runner, clock=lambda: elapsed[0])) == 1
    assert len(executed) == 1
    assert sorted(command.state for command in store.commands()) == ["completed", "queued"]
    assert not store.target_blocked(command_target.key)


async def test_heartbeat_budget_rechecked_before_command_claim(runner, command_target, monkeypatch):
    from triage.command_center.worker import drain_commands
    from triage.monitoring.controller import HeartbeatBudget
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    command = CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id=command_target.key, actor_id="operator",
    )
    store.enqueue(command)
    elapsed = [0.0]
    claims = runner._pipeline_claim_store()
    claim = claims.claim
    released = []
    release = claims.release

    def slow_target_claim(*args, **kwargs):
        result = claim(*args, **kwargs)
        elapsed[0] = 200
        return result

    def target_release(key):
        released.append(key)
        release(key)

    monkeypatch.setattr(claims, "claim", slow_target_claim)
    monkeypatch.setattr(claims, "release", target_release)
    assert await drain_commands(runner, budget=HeartbeatBudget(840, 690, lambda: elapsed[0])) == []
    assert store.get_command(command.id).state == "queued"
    assert len(released) == 1
