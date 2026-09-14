from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from triage.models import BIRequest
from triage.runner import Scenario


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


async def test_operator_command_is_claimed_once_and_not_replayed(runner, monkeypatch) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.powerbi_workspace_id = "workspace"
    runner.settings.powerbi_dataset_id = "dataset"
    command = CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id="powerbi:workspace:dataset",
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


async def test_distinct_commands_for_one_target_do_not_overlap(runner, monkeypatch) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.powerbi_workspace_id = "workspace"
    runner.settings.powerbi_dataset_id = "dataset"
    for _ in range(2):
        store.enqueue(CommandRecord(
            id=str(uuid4()), kind="powerbi_triage", target_id="powerbi:workspace:dataset",
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


async def test_timeout_retains_target_uncertainty_without_reexecution(runner, monkeypatch) -> None:
    import triage.command_center.worker as worker
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.powerbi_workspace_id = "workspace"
    runner.settings.powerbi_dataset_id = "dataset"
    for _ in range(2):
        store.enqueue(CommandRecord(
            id=str(uuid4()), kind="powerbi_triage", target_id="powerbi:workspace:dataset",
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
    assert store.target_blocked("powerbi:workspace:dataset")
    assert sorted(row.state for row in store.commands()) == ["interrupted", "queued"]
    assert runner.store.list_all() == []


async def test_expired_finalization_is_not_retried_as_execution_failure(runner, monkeypatch) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.powerbi_workspace_id = "workspace"
    runner.settings.powerbi_dataset_id = "dataset"
    store.enqueue(CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id="powerbi:workspace:dataset", actor_id="operator",
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
    runner, monkeypatch, overrun,
) -> None:
    import triage.command_center.worker as worker
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.powerbi_workspace_id = "workspace"
    runner.settings.powerbi_dataset_id = "dataset"
    command = CommandRecord(
        id=str(uuid4()), kind="powerbi_triage", target_id="powerbi:workspace:dataset",
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


async def test_unrelated_command_progresses_behind_100_blocked_target_commands(runner, monkeypatch) -> None:
    from triage.command_center.worker import drain_commands
    from triage.store.command_center import CommandRecord, InMemoryCommandCenterStore

    store = InMemoryCommandCenterStore()
    runner._command_center_store = store
    runner.settings.powerbi_workspace_id = "workspace"
    runner.settings.powerbi_dataset_id = "dataset"
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
        "id": str(uuid4()), "target_id": "powerbi:workspace:dataset",
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
