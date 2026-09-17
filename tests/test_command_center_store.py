from __future__ import annotations

import json
import re
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from triage.models import TriageAction, TriageClassification, TriageResult
from triage.store.azure_sql import SqlUnavailable
from triage.store.command_center import (
    AzureSqlCommandCenterStore,
    CommandRecord,
    InMemoryCommandCenterStore,
    JsonFileCommandCenterStore,
    RunEvent,
    RunRecord,
    schema_statements,
)

NOW = datetime(2026, 9, 11, 18, 0, 0, tzinfo=UTC)
RUN_ID = "10000000-0000-0000-0000-000000000001"
OTHER_RUN_ID = "10000000-0000-0000-0000-000000000002"


def _run(**updates) -> RunRecord:
    return RunRecord.model_validate({
        "id": RUN_ID, "request_id": "request-1", "signature": "sig-1",
        "target": "Sales refresh", "started_at": NOW.isoformat(),
    } | updates)


def _result(**updates) -> TriageResult:
    return TriageResult.model_validate({
        "outcome": "resolved", "summary": "Refresh verified",
        "request_id": "request-1", "signature": "sig-1",
        "started_at": NOW.isoformat(), "finished_at": (NOW + timedelta(seconds=2)).isoformat(),
        "wall_clock_ms": 2000, "tool_calls": 3, "tokens_used": 121, "write_actions": 1,
    } | updates)


def _command(**updates) -> CommandRecord:
    return CommandRecord.model_validate({
        "id": "command-1", "kind": "powerbi_triage", "target_id": "dataset-1",
        "actor_id": "operator-1", "actor_name": "Operator", "subject": "Refresh failed",
        "body": "ScheduledRefreshTimeout", "created_at": NOW.isoformat(),
    } | updates)


class _Sql:
    """Run the actual parameterized DML and translated DDL against local SQLite.

    Only SQL Server syntax and its UTC clock functions are adapted. Constraints,
    filtering, row counts, and concurrency are enforced by SQLite, not a mock.
    """

    def __init__(self, path: Path, tables: tuple[str, str, str] | None = None) -> None:
        self.path = path
        self.now = NOW
        self.down = False
        self.fail_writes = False
        self.lose_write_ack = False
        self.operations: list[tuple[str, str, tuple]] = []
        self._lock = threading.Lock()
        with self._connection() as conn:
            for statement in schema_statements(*(tables or ())):
                sql = statement[statement.index("CREATE TABLE"):].replace(
                    "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1,
                )
                sql = re.sub(r",\s*INDEX\s+\w+\s+\([^\n]*\)", "", sql)
                sql = re.sub(r"NVARCHAR\((?:MAX|\d+)\)|CHAR\(\d+\)|DATETIME2\(\d+\)", "TEXT", sql)
                sql = sql.replace("Latin1_General_100_BIN2", "BINARY")
                conn.execute(sql.replace("[dbo].", ""))

    def _connection(self):
        conn = sqlite3.connect(self.path, timeout=15)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.create_function(
            "SYSUTCDATETIME", 0,
            lambda: self.now.replace(tzinfo=None).isoformat(timespec="microseconds"),
        )
        conn.create_function(
            "DATEADD_SECONDS", 2,
            lambda seconds, value: (
                datetime.fromisoformat(value) + timedelta(seconds=seconds)
            ).isoformat(timespec="microseconds"),
        )
        return conn

    @property
    def is_available(self) -> bool:
        return not self.down

    def ensure_schema_once(self) -> bool:
        raise AssertionError("Runtime stores must not invoke schema migration")

    @staticmethod
    def _translate(sql: str) -> str:
        sql = sql.replace("[dbo].", "").replace("COUNT_BIG(", "COUNT(")
        sql = re.sub(
            r"OFFSET (\d+) ROWS FETCH NEXT (\d+) ROWS ONLY", r"LIMIT \2 OFFSET \1", sql,
        )
        return sql.replace("DATEADD(second, ?, SYSUTCDATETIME())", "DATEADD_SECONDS(?, SYSUTCDATETIME())")

    @staticmethod
    def _parameters(params: tuple) -> tuple:
        return tuple(
            p.isoformat(timespec="microseconds") if isinstance(p, datetime) else p for p in params
        )

    def execute(self, sql: str, *params) -> int:
        if self.down or self.fail_writes:
            raise ConnectionError("Offline database write failure")
        with self._lock:
            self.operations.append(("execute", sql, params))
        with self._connection() as conn:
            affected = conn.execute(self._translate(sql), self._parameters(params)).rowcount
        if self.lose_write_ack:
            self.lose_write_ack = False
            raise ConnectionError("Offline database lost the write acknowledgement")
        return affected

    def query(self, sql: str, *params) -> list[tuple]:
        if self.down:
            raise ConnectionError("Offline database read failure")
        with self._lock:
            self.operations.append(("query", sql, params))
        with self._connection() as conn:
            return conn.execute(self._translate(sql), self._parameters(params)).fetchall()

    def integrity_error(self):
        return sqlite3.IntegrityError


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    monkeypatch.setattr("triage.store.command_center._utcnow", lambda: NOW.isoformat())


@pytest.fixture(params=["memory", "json", "sql"])
def store(request, tmp_path):
    if request.param == "memory":
        return InMemoryCommandCenterStore()
    if request.param == "json":
        return JsonFileCommandCenterStore(tmp_path / "command-center.json")
    return AzureSqlCommandCenterStore(_Sql(tmp_path / "command-center.db"))


def test_run_history_retains_full_result_and_derived_counters(store) -> None:
    store.start_run(_run())
    running = store.get_run(RUN_ID)
    assert running.state == "running"
    assert (running.duration_ms, running.tool_calls, running.tokens_used, running.write_actions) == (
        0, 0, 0, 0,
    )
    result = _result(actions=[TriageAction(tool_name="refresh_powerbi_dataset", is_remediation=True)])
    finished = store.finish_run(RUN_ID, result, "incident-1")
    assert finished.state == "completed"
    assert finished.result == result
    assert finished.incident_id == "incident-1"
    assert finished.summary == result.summary
    assert (finished.duration_ms, finished.tool_calls, finished.tokens_used, finished.write_actions) == (
        2000, 3, 121, 1,
    )
    assert store.get_run(RUN_ID) == finished


def test_terminal_run_cannot_be_restarted_or_rewritten(store) -> None:
    original = _run()
    store.start_run(original)
    finished = store.finish_run(RUN_ID, _result(), "incident-1")
    store.start_run(original)
    assert store.finish_run(RUN_ID, _result(), "incident-1") == finished
    with pytest.raises(ValueError, match="conflict"):
        store.start_run(_run(target="Different target"))
    with pytest.raises(ValueError, match="terminal"):
        store.finish_run(RUN_ID, _result(outcome="needs_human"))
    assert store.get_run(RUN_ID) == finished


def test_crashes_are_persisted_as_failed_runs(store) -> None:
    store.start_run(_run())
    result = _result(outcome="agent_crashed", exception_class="RuntimeError", exception_message="failed")
    assert store.finish_run(RUN_ID, result).state == "failed"
    assert store.get_run(RUN_ID).result.exception_class == "RuntimeError"


def test_unknown_run_cannot_finish_or_accept_events(store) -> None:
    with pytest.raises(ValueError, match="unknown run"):
        store.finish_run(RUN_ID, _result())
    with pytest.raises(ValueError, match="unknown run"):
        store.append_event(RunEvent(run_id=RUN_ID, sequence=0, kind="status", label="Start"))


def test_wrapper_run_retains_its_identity_and_nested_result_identity(store) -> None:
    record = _run(
        request_id="validation:request-1", signature="validation:scenario-1",
        workload="validation", target="scenario1-transient",
    )
    store.start_run(record)
    store.append_event(RunEvent(
        run_id=record.id, sequence=1, kind="validation_result", label="Scenario validation",
        detail=json.dumps({"scenario": record.target, "provider": "mock", "passed": True}),
    ))
    finished = store.finish_run(RUN_ID, _result())
    assert finished.workload == "validation"
    assert finished.target == "scenario1-transient"
    assert finished.request_id == record.request_id
    assert finished.signature == record.signature
    assert finished.result.request_id == "request-1"
    assert finished.result.signature == "sig-1"
    assert json.loads(store.events(record.id)[0].detail)["passed"] is True


def test_repeated_incident_has_distinct_runs_and_filtered_paging(store) -> None:
    for index, signature in enumerate(("sig-1", "sig-1", "sig-2")):
        id = _run_id(index)
        store.start_run(_run(
            id=id, signature=signature, started_at=(NOW + timedelta(seconds=index)).isoformat(),
        ))
        store.finish_run(id, _result(signature=signature), "same-incident")
    assert store.count_runs() == 3
    assert store.count_runs(signature="sig-1") == 2
    assert [row.id for row in store.list_runs(limit=1, offset=1)] == [_run_id(1)]
    assert len(store.list_runs(signature="sig-1")) == 2
    assert store.list_runs(offset=10) == []
    assert store.get_run(str(uuid4())) is None


def _run_id(index: int) -> str:
    return f"10000000-0000-0000-0000-{index + 1:012d}"


def test_finish_fills_a_signature_not_known_at_start(store) -> None:
    original = _run(signature="")
    store.start_run(original)
    store.finish_run(RUN_ID, _result())
    store.start_run(original)
    assert store.count_runs("sig-1") == 1


def test_events_are_deduplicated_and_ordered_per_run(store) -> None:
    store.start_run(_run())
    store.start_run(_run(id=OTHER_RUN_ID))
    first = RunEvent(
        run_id=RUN_ID, sequence=1, timestamp=(NOW + timedelta(seconds=2)).isoformat(),
        kind="tool_call", label="Read history",
    )
    later_sequence = first.model_copy(update={"id": str(uuid4()), "sequence": 2})
    earlier_time = first.model_copy(update={
        "id": str(uuid4()), "timestamp": NOW.isoformat(),
    })
    other = first.model_copy(update={"id": str(uuid4()), "run_id": OTHER_RUN_ID})
    for event in (later_sequence, first, earlier_time, other, first):
        store.append_event(event)
    assert [e.id for e in store.events(RUN_ID)] == [earlier_time.id, first.id, later_sequence.id]
    assert len(store.events(OTHER_RUN_ID)) == 1
    with pytest.raises(ValueError, match="conflict"):
        store.append_event(first.model_copy(update={"run_id": OTHER_RUN_ID}))


@pytest.mark.parametrize("kind", [
    "thinking", "THINKING", "model.thinking", "prompt", "raw_prompt",
    "model_completion", "reasoning", "chain_of_thought", "thinkingDelta", "rawPrompt",
])
def test_raw_model_event_kinds_are_refused(kind) -> None:
    with pytest.raises(ValidationError):
        RunEvent(run_id=RUN_ID, sequence=0, kind=kind, label="Unsafe")
    with pytest.raises(ValidationError):
        RunEvent(run_id=RUN_ID, sequence=0, kind="status", label="Unsafe", payload={"prompt": "raw"})


def test_store_redacts_all_nested_strings_and_bare_credential_arguments(store) -> None:
    secret = "AKIA" + "IOSFODNN7EXAMPLE"
    raw = _result(
        summary=f"Error {secret}", exception_message="Password=fixture1",
        classification=TriageClassification(
            root_cause=f"Cause {secret}", reasoning=[f"Evidence {secret}"],
            action_params={"password": "short", "safe": {"access_token": "bare-token"}},
        ),
        actions=[TriageAction(
            tool_name="inspect", result_summary=f"Failure {secret}",
            arguments={
                "nested": [f"Failure {secret}", {"clientSecret": "bare-secret"}],
                "prompt": "raw system prompt must not be recorded",
                "messages": [{"role": "assistant", "content": "raw completion"}],
                "api_key": "tiny",
                "headers": {"X-Api-Key": "tiny-header-secret"},
            },
        )],
    )
    store.start_run(_run(target=f"Report {secret}"))
    store.finish_run(RUN_ID, raw)
    store.append_event(RunEvent(
        run_id=RUN_ID, sequence=0, kind="status", label=secret,
        detail=f"Error {secret}", status=secret, tool_name=secret,
    ))
    queued = store.enqueue(_command(subject=secret, body="Password=fixture1"))
    serialized = (
        store.get_run(RUN_ID).model_dump_json() + store.events(RUN_ID)[0].model_dump_json()
        + queued.model_dump_json()
    )
    for forbidden in (
        secret, "fixture1", "bare-token", "bare-secret", '"short"', '"tiny"',
        "raw system prompt must not be recorded", "raw completion", "tiny-header-secret",
    ):
        assert forbidden not in serialized
    assert "REDACTED" in serialized
    assert "OMITTED:model_content" in serialized
    assert secret in raw.summary, "the caller's result must not be mutated"


def test_store_bounds_text_after_redaction(store) -> None:
    text = "x" * 5000
    store.start_run(_run())
    store.finish_run(RUN_ID, _result(summary=text))
    store.append_event(RunEvent(
        run_id=RUN_ID, sequence=1, kind="status", label=text, detail=text,
        status=text, tool_name=text,
    ))
    command = store.enqueue(_command(subject=text, body=text, actor_name=text))
    event = store.events(RUN_ID)[0]
    assert len(store.get_run(RUN_ID).summary) <= 4000
    assert (len(event.label), len(event.detail), len(event.status), len(event.tool_name)) == (
        200, 4000, 100, 200,
    )
    assert len(command.body) == len(command.subject) == 4000
    assert len(command.actor_name) == 200


def test_returned_records_and_input_records_do_not_alias_store_state(store) -> None:
    run = _run()
    store.start_run(run)
    run.target = "Changed by caller"
    finished = store.finish_run(RUN_ID, _result(actions=[TriageAction(
        tool_name="inspect", arguments={"nested": ["original"]},
    )]))
    finished.result.actions[0].arguments["nested"].append("mutated")
    listed = store.list_runs()[0]
    listed.result.actions[0].arguments["nested"].append("also mutated")
    assert store.get_run(RUN_ID).result.actions[0].arguments["nested"] == ["original"]
    assert store.get_run(RUN_ID).target == "Sales refresh"
    event = RunEvent(run_id=RUN_ID, sequence=1, kind="status", label="Original")
    store.append_event(event)
    event.label = "Caller changed"
    store.events(RUN_ID)[0].label = "Read changed"
    assert store.events(RUN_ID)[0].label == "Original"
    queued = store.enqueue(_command())
    queued.body = "Caller changed"
    store.commands()[0].body = "Read changed"
    assert store.get_command(queued.id).body == "ScheduledRefreshTimeout"


def test_duplicate_command_is_one_request_even_with_a_new_timestamp(store) -> None:
    command = _command()
    first = store.enqueue(command)
    replay = store.enqueue(_command(created_at=(NOW + timedelta(hours=1)).isoformat()))
    assert replay == first
    assert len(first.request_hash) == 64
    assert len(store.commands()) == 1
    store.claim_command(command.id, "worker-1")
    assert store.enqueue(command).state == "running"
    assert store.claim_command(command.id, "worker-1") is None


@pytest.mark.parametrize("changes", [
    {"body": "Different input"}, {"subject": "Another error"}, {"target_id": "dataset-2"},
    {"actor_id": "operator-2"}, {"actor_name": "Someone else"}, {"kind": "pipeline_sweep"},
])
def test_changed_immutable_command_input_conflicts(store, changes) -> None:
    original = store.enqueue(_command())
    with pytest.raises(ValueError, match="hash"):
        store.enqueue(_command(**changes, request_hash=original.request_hash))
    assert store.get_command(original.id) == original


def test_hash_is_computed_from_original_text_not_redacted_or_truncated_content(store) -> None:
    store.enqueue(_command(body="password=fixture2", subject="x" * 4100 + "a"))
    with pytest.raises(ValueError, match="hash"):
        store.enqueue(_command(body="password=fixture3", subject="x" * 4100 + "a"))
    with pytest.raises(ValueError, match="hash"):
        store.enqueue(_command(body="password=fixture2", subject="x" * 4100 + "b"))


def test_commands_only_start_queued_and_wrong_owner_cannot_finish(store) -> None:
    with pytest.raises(ValueError, match="queued"):
        store.enqueue(_command(state="running"))
    with pytest.raises(ValueError, match="queued"):
        store.enqueue(_command(worker_id="preclaimed"))
    queued = store.enqueue(_command())
    claimed = store.claim_command(queued.id, "worker-1", lease_seconds=90)
    assert claimed.state == "running"
    assert datetime.fromisoformat(claimed.lease_expires_at) == NOW + timedelta(seconds=90)
    with pytest.raises(ValueError, match="owner"):
        store.finish_command(queued.id, "worker-2", "completed", "Wrong worker")
    completed = store.finish_command(queued.id, "worker-1", "completed", "Done", RUN_ID)
    assert (completed.state, completed.run_id, completed.summary) == ("completed", RUN_ID, "Done")
    assert completed.finished_at is not None
    assert store.claim_command(queued.id, "worker-1") is None
    with pytest.raises(ValueError):
        store.finish_command(queued.id, "worker-1", "failed", "Cannot overwrite")
    assert store.get_command(queued.id) == completed


def test_interrupted_work_is_never_requeued_or_finished_by_stale_worker(store) -> None:
    command = _command()
    store.enqueue(command)
    store.enqueue(_command(id="still-queued"))
    store.claim_command(command.id, "worker-1", lease_seconds=90)
    assert store.expire_commands(now="2026-09-11T13:01:29-05:00") == 0
    assert store.expire_commands(now="2026-09-11T13:01:30-05:00") == 1
    interrupted = store.get_command(command.id)
    assert interrupted.state == "interrupted"
    assert interrupted.worker_id == "worker-1"
    assert datetime.fromisoformat(interrupted.finished_at) == NOW + timedelta(seconds=90)
    assert store.expire_commands(now=NOW + timedelta(days=1)) == 0
    assert store.claim_command(command.id, "worker-2") is None
    assert store.enqueue(command).state == "interrupted"
    assert store.get_command("still-queued").state == "queued"
    assert [row.id for row in store.queued_commands()] == ["still-queued"]
    with pytest.raises(ValueError, match="owner"):
        store.finish_command(command.id, "worker-1", "completed", "Late")


def test_expired_worker_cannot_finish_before_expiration_sweep(store, monkeypatch) -> None:
    store.enqueue(_command())
    store.claim_command("command-1", "worker-1", lease_seconds=1)
    future = NOW + timedelta(seconds=1)
    monkeypatch.setattr("triage.store.command_center._utcnow", lambda: future.isoformat())
    if isinstance(store, AzureSqlCommandCenterStore):
        store._db.now = future
    with pytest.raises(ValueError, match="lease"):
        store.finish_command("command-1", "worker-1", "completed", "Too late")
    assert store.expire_commands() == 1


@pytest.mark.parametrize("uncertain_state", ["running", "interrupted"])
def test_target_blocking_filters_before_newer_history_and_is_target_scoped(
    store, uncertain_state,
) -> None:
    store.enqueue(_command(id="old-work", created_at=(NOW - timedelta(days=1)).isoformat()))
    store.claim_command("old-work", "worker-1")
    if uncertain_state == "interrupted":
        store.interrupt_command("old-work", "worker-1", "External result is uncertain")
    store.enqueue(_command(id="different-queued-command"))
    for index in range(105):
        store.enqueue(_command(id=f"unrelated-{index:03d}", target_id="dataset-2"))
    assert all(row.id != "old-work" for row in store.commands(limit=100))
    assert store.target_blocked("dataset-1") is True
    assert store.target_blocked("dataset-2") is False
    assert store.target_blocked("unknown-target") is False
    assert store.get_command("different-queued-command").state == "queued"


@pytest.mark.parametrize("terminal_state", ["completed", "failed"])
def test_terminal_commands_do_not_block_a_target(store, terminal_state) -> None:
    store.enqueue(_command())
    assert store.target_blocked("dataset-1") is False
    store.claim_command("command-1", "worker-1")
    assert store.target_blocked("dataset-1") is True
    store.finish_command("command-1", "worker-1", terminal_state, "Finished")
    assert store.target_blocked("dataset-1") is False


def test_owner_can_interrupt_after_lease_expiry_without_rewriting_existing_audit(
    store, monkeypatch,
) -> None:
    queued = store.enqueue(_command())
    store.claim_command(queued.id, "worker-1", lease_seconds=1)
    future = NOW + timedelta(seconds=2)
    monkeypatch.setattr("triage.store.command_center._utcnow", lambda: future.isoformat())
    if isinstance(store, AzureSqlCommandCenterStore):
        store._db.now = future
    assert store.target_blocked(queued.target_id) is True
    interrupted = store.interrupt_command(
        queued.id, "worker-1", "Submission acknowledged; final result unknown", RUN_ID,
    )
    assert interrupted.state == "interrupted"
    assert interrupted.run_id == RUN_ID
    assert interrupted.worker_id == "worker-1"
    assert interrupted.request_hash == queued.request_hash
    assert datetime.fromisoformat(interrupted.finished_at) == future
    assert store.target_blocked(queued.target_id) is True
    replay = store.interrupt_command(queued.id, "worker-1", "Do not overwrite", OTHER_RUN_ID)
    assert replay == interrupted
    replay.summary = "Changed by caller"
    assert store.get_command(queued.id) == interrupted
    assert store.claim_command(queued.id, "worker-2") is None
    with pytest.raises(ValueError, match="owner"):
        store.finish_command(queued.id, "worker-1", "completed", "Late")


def test_interruption_refuses_lost_ownership_and_nonrunning_commands(store) -> None:
    with pytest.raises(ValueError, match="owner or state"):
        store.interrupt_command("unknown-command", "worker-1", "Cannot interrupt")
    queued = store.enqueue(_command())
    with pytest.raises(ValueError, match="owner or state"):
        store.interrupt_command(queued.id, "worker-1", "Not yet running")
    running = store.claim_command(queued.id, "worker-1")
    with pytest.raises(ValueError, match="owner or state"):
        store.interrupt_command(queued.id, "worker-2", "Wrong owner")
    assert store.get_command(queued.id) == running
    interrupted = store.interrupt_command(queued.id, "worker-1", "Needs review")
    with pytest.raises(ValueError, match="owner or state"):
        store.interrupt_command(queued.id, "worker-2", "Wrong owner again")
    assert store.get_command(queued.id) == interrupted
    reconciled = store.reconcile_command(queued.id, "admin-1", "Verified no external job is running")
    with pytest.raises(ValueError, match="owner or state"):
        store.interrupt_command(queued.id, "worker-1", "Cannot reopen reconciliation")
    assert store.get_command(queued.id) == reconciled


def test_operator_reconciliation_preserves_audit_and_clears_uncertainty_without_execution(
    store, monkeypatch,
) -> None:
    original = _command()
    queued = store.enqueue(original)
    store.enqueue(_command(id="still-queued"))
    store.claim_command(queued.id, "worker-1")
    interrupted = store.interrupt_command(queued.id, "worker-1", "Final result was uncertain", RUN_ID)
    later = NOW + timedelta(minutes=5)
    monkeypatch.setattr("triage.store.command_center._utcnow", lambda: later.isoformat())
    if isinstance(store, AzureSqlCommandCenterStore):
        store._db.now = later
    reason = "Verified the correlated platform job is stopped"
    reconciled = store.reconcile_command(queued.id, "admin-1", reason)
    assert reconciled.state == "failed"
    assert reconciled.summary == f"{interrupted.summary}\n\nReconciled: {reason}"
    assert reconciled.reconciled_by == "admin-1"
    assert datetime.fromisoformat(reconciled.reconciled_at) == later
    assert reconciled.reconciliation_reason == reason
    assert reconciled.request_hash == queued.request_hash
    assert reconciled.worker_id == interrupted.worker_id
    assert reconciled.run_id == interrupted.run_id
    assert reconciled.started_at == interrupted.started_at
    assert reconciled.finished_at == interrupted.finished_at
    assert reconciled.lease_expires_at == interrupted.lease_expires_at
    assert store.target_blocked(queued.target_id) is False
    assert [row.id for row in store.queued_commands()] == ["still-queued"]
    assert len(store.commands()) == 2
    assert store.count_runs() == 0
    assert store.enqueue(original) == reconciled
    assert store.claim_command(queued.id, "worker-2") is None
    with pytest.raises(ValueError, match="interrupted"):
        store.reconcile_command(queued.id, "admin-2", "Cannot replace the first review")
    if isinstance(store, JsonFileCommandCenterStore):
        restored = JsonFileCommandCenterStore(store.path)
    elif isinstance(store, AzureSqlCommandCenterStore):
        restored = AzureSqlCommandCenterStore(store._db)
    else:
        restored = store
    assert restored.get_command(queued.id) == reconciled
    reconciled.reconciliation_reason = "Changed by caller"
    assert restored.get_command(queued.id).reconciliation_reason == reason


@pytest.mark.parametrize(("actor", "reason"), [
    ("", "Checked"), ("  ", "Checked"), (None, "Checked"),
    ("admin-1", ""), ("admin-1", " \t\n"), ("admin-1", None), ("admin-1", "x" * 4001),
])
def test_reconciliation_requires_an_explicit_actor_and_bounded_nonempty_reason(
    store, actor, reason,
) -> None:
    store.enqueue(_command())
    store.claim_command("command-1", "worker-1")
    interrupted = store.interrupt_command("command-1", "worker-1", "Uncertain")
    with pytest.raises(ValueError):
        store.reconcile_command("command-1", actor, reason)
    assert store.get_command("command-1") == interrupted
    assert store.target_blocked("dataset-1") is True


@pytest.mark.parametrize("state", ["queued", "running", "completed", "failed"])
def test_reconciliation_only_accepts_interrupted_commands(store, state) -> None:
    with pytest.raises(ValueError, match="interrupted"):
        store.reconcile_command("unknown-command", "admin-1", "Checked")
    queued = store.enqueue(_command())
    if state != "queued":
        store.claim_command(queued.id, "worker-1")
    if state in ("completed", "failed"):
        store.finish_command(queued.id, "worker-1", state, "Finished")
    prior = store.get_command(queued.id)
    with pytest.raises(ValueError, match="interrupted"):
        store.reconcile_command(queued.id, "admin-1", "Checked")
    assert store.get_command(queued.id) == prior


def test_reconciliation_redacts_reason_without_losing_a_full_original_summary(store) -> None:
    store.enqueue(_command())
    store.claim_command("command-1", "worker-1")
    original_summary = "x" * 4000
    store.interrupt_command("command-1", "worker-1", original_summary)
    reconciled = store.reconcile_command(
        "command-1", "admin-1", "Checked external status; Password=fixture4",
    )
    assert reconciled.summary == original_summary
    assert "Checked external status" in reconciled.reconciliation_reason
    assert "REDACTED" in reconciled.reconciliation_reason
    assert "fixture4" not in reconciled.model_dump_json()


@pytest.mark.parametrize("audit", [
    {"reconciled_by": "admin-1"}, {"reconciled_at": NOW.isoformat()},
    {"reconciliation_reason": "Pre-approved"},
])
def test_new_commands_cannot_supply_fabricated_reconciliation_audit(store, audit) -> None:
    with pytest.raises(ValueError, match="queued"):
        store.enqueue(_command(**audit))
    assert store.commands() == []


def test_concurrent_reconciliations_have_one_audit_winner(store) -> None:
    queued = store.enqueue(_command())
    store.claim_command(queued.id, "worker-1")
    store.interrupt_command(queued.id, "worker-1", "Uncertain")
    barrier = threading.Barrier(4)

    def reconcile(index):
        caller = AzureSqlCommandCenterStore(store._db) if isinstance(
            store, AzureSqlCommandCenterStore,
        ) else store
        barrier.wait()
        try:
            return caller.reconcile_command(queued.id, f"admin-{index}", f"Reviewed by {index}")
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        winners = [row for row in pool.map(reconcile, range(4)) if row is not None]
    assert len(winners) == 1
    assert store.get_command(queued.id) == winners[0]
    assert winners[0].request_hash == queued.request_hash
    assert store.target_blocked(queued.target_id) is False


def test_queue_order_and_bounded_listing(store) -> None:
    for index in range(4):
        store.enqueue(_command(
            id=f"command-{index}", created_at=(NOW + timedelta(seconds=index)).isoformat(),
        ))
    assert [c.id for c in store.commands(limit=2)] == ["command-3", "command-2"]
    assert store.get_command("does-not-exist") is None
    assert store.claim_command("does-not-exist", "worker-1") is None


def test_queued_reader_finds_old_work_behind_over_100_newer_completed_commands(store) -> None:
    store.enqueue(_command(id="old-queued", created_at=(NOW - timedelta(days=2)).isoformat()))
    for index in range(105):
        id = f"completed-{index:03d}"
        store.enqueue(_command(
            id=id, created_at=(NOW - timedelta(minutes=105 - index)).isoformat(),
        ))
        assert store.claim_command(id, "worker-1") is not None
        store.finish_command(id, "worker-1", "completed", "Done")
    history = store.commands(limit=100)
    assert len(history) == 100
    assert all(row.state == "completed" for row in history)
    assert [row.id for row in store.queued_commands()] == ["old-queued"]
    if isinstance(store, JsonFileCommandCenterStore):
        restored = JsonFileCommandCenterStore(store.path)
        assert [row.id for row in restored.queued_commands()] == ["old-queued"]


def test_queued_reader_is_oldest_first_bounded_and_returns_copies(store) -> None:
    store.enqueue(_command(id="later", created_at=(NOW + timedelta(seconds=1)).isoformat()))
    store.enqueue(_command(id="tie-b"))
    store.enqueue(_command(id="tie-a"))
    pending = store.queued_commands(limit=2)
    assert [row.id for row in pending] == ["tie-a", "tie-b"]
    pending[0].body = "Changed by caller"
    assert store.get_command("tie-a").body == "ScheduledRefreshTimeout"
    assert store.claim_command("tie-a", "worker-1") is not None
    assert [row.id for row in store.queued_commands()] == ["tie-b", "later"]

@pytest.mark.parametrize("interrupted", [False, True])
def test_eligible_reader_filters_target_barriers_before_limiting(store, interrupted) -> None:
    store.enqueue(_command(id="barrier", target_id="blocked"))
    store.claim_command("barrier", "worker-1")
    if interrupted:
        store.interrupt_command("barrier", "worker-1", "Unconfirmed execution")
    for index in range(105):
        store.enqueue(_command(
            id=f"blocked-{index:03d}", target_id="blocked",
            created_at=(NOW + timedelta(seconds=index + 1)).isoformat(),
        ))
    store.enqueue(_command(
        id="eligible", target_id="unrelated",
        created_at=(NOW + timedelta(minutes=10)).isoformat(),
    ))
    assert len(store.queued_commands()) == 100
    assert all(row.target_id == "blocked" for row in store.queued_commands())
    eligible = store.eligible_commands(limit=1)
    assert [row.id for row in eligible] == ["eligible"]
    eligible[0].body = "Changed by caller"
    assert store.get_command("eligible").body == "ScheduledRefreshTimeout"
    if isinstance(store, JsonFileCommandCenterStore):
        assert [row.id for row in JsonFileCommandCenterStore(store.path).eligible_commands()] == ["eligible"]
    if interrupted:
        store.reconcile_command("barrier", "admin-1", "Verified stopped")
    else:
        store.finish_command("barrier", "worker-1", "completed", "Verified complete")
    assert [row.id for row in store.eligible_commands(limit=1)] == ["blocked-000"]


def test_active_reader_keeps_blocking_work_visible_behind_over_100_terminal_commands(store) -> None:
    expected_ids = ["old-interrupted", "old-queued", "old-running"]
    for index, id in enumerate(expected_ids):
        store.enqueue(_command(
            id=id, created_at=(NOW - timedelta(days=2) + timedelta(seconds=index)).isoformat(),
        ))
    store.claim_command("old-running", "worker-1")
    store.claim_command("old-interrupted", "worker-2")
    store.interrupt_command("old-interrupted", "worker-2", "Needs reconciliation")
    for index in range(105):
        id = f"terminal-{index:03d}"
        store.enqueue(_command(
            id=id, created_at=(NOW - timedelta(minutes=105 - index)).isoformat(),
        ))
        store.claim_command(id, "worker-3")
        store.finish_command(id, "worker-3", "completed" if index % 2 else "failed", "Finished")
    history = store.commands(limit=100)
    assert len(history) == 100
    assert all(row.state in ("completed", "failed") for row in history)
    active = store.active_commands()
    assert [row.id for row in active] == expected_ids
    assert [row.state for row in active] == ["interrupted", "queued", "running"]
    assert [row.id for row in store.active_commands(limit=2)] == expected_ids[:2]
    active[0].summary = "Changed by caller"
    assert store.get_command("old-interrupted").summary == "Needs reconciliation"
    if isinstance(store, JsonFileCommandCenterStore):
        assert [row.id for row in JsonFileCommandCenterStore(store.path).active_commands()] == expected_ids
    if isinstance(store, AzureSqlCommandCenterStore):
        assert any(
            "WHERE state IN ('queued', 'running', 'interrupted') "
            "ORDER BY created_at ASC, command_id ASC "
            "OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY" in sql
            for kind, sql, _ in store._db.operations if kind == "query"
        )


def test_active_reader_has_a_stable_id_tiebreak_and_drops_reconciled_work(store) -> None:
    for id in ("c-running", "b-interrupted", "a-queued"):
        store.enqueue(_command(id=id))
    store.claim_command("c-running", "worker-1")
    store.claim_command("b-interrupted", "worker-2")
    store.interrupt_command("b-interrupted", "worker-2", "Uncertain")
    assert [row.id for row in store.active_commands(limit=2)] == ["a-queued", "b-interrupted"]
    store.reconcile_command("b-interrupted", "admin-1", "Verified platform job is stopped")
    assert [row.id for row in store.active_commands()] == ["a-queued", "c-running"]


def test_identifiers_are_never_truncated_or_case_collapsed(store) -> None:
    for id in ("A" + "x" * 99, "a" + "x" * 99):
        assert store.enqueue(_command(id=id)).id == id
    assert len(store.commands()) == 2
    with pytest.raises(ValidationError):
        _command(id="a" * 101)
    with pytest.raises(ValidationError):
        _command(id="trailing-space ")
    with pytest.raises(ValidationError):
        _command(target_id="x" * 201)
    with pytest.raises(ValidationError):
        _run(id="not-a-uuid")


@pytest.mark.parametrize("kwargs", [
    {"limit": 0}, {"limit": -1}, {"limit": 1001}, {"limit": True},
    {"limit": "1; DROP TABLE triage_agent_runs"}, {"offset": -1}, {"offset": 1.5},
])
def test_paging_inputs_are_validated(store, kwargs) -> None:
    with pytest.raises(ValueError):
        store.list_runs(**kwargs)
    if "limit" in kwargs:
        with pytest.raises(ValueError):
            store.queued_commands(limit=kwargs["limit"])
        with pytest.raises(ValueError):
            store.eligible_commands(limit=kwargs["limit"])
        with pytest.raises(ValueError):
            store.active_commands(limit=kwargs["limit"])


@pytest.mark.parametrize("lease", [0, -1, True, 1.2, 2**31])
def test_invalid_leases_cannot_authorize_a_command(store, lease) -> None:
    store.enqueue(_command())
    with pytest.raises(ValueError):
        store.claim_command("command-1", "worker-1", lease_seconds=lease)
    assert store.get_command("command-1").state == "queued"


def test_naive_dates_are_refused_instead_of_comparing_different_timezones(store) -> None:
    with pytest.raises(ValidationError, match="offset"):
        _command(created_at="2026-09-11T12:00:00")
    with pytest.raises(ValueError, match="offset"):
        store.expire_commands(now=datetime(2026, 9, 11))


def test_json_roundtrip_serializes_nested_datetime_and_preserves_history(tmp_path) -> None:
    path = tmp_path / "nested" / "history.json"
    original = JsonFileCommandCenterStore(path)
    original.start_run(_run())
    raw_result = _result(actions=[TriageAction(
        tool_name="inspect", arguments={"observed_at": NOW, "nested": [{"at": NOW}]},
    )])
    finished = original.finish_run(RUN_ID, raw_result, "incident-1")
    original.append_event(RunEvent(run_id=RUN_ID, sequence=0, kind="run_completed", label="Finished"))
    original.enqueue(_command())
    original.claim_command("command-1", "worker-1")
    restored = JsonFileCommandCenterStore(path)
    assert restored.get_run(RUN_ID) == finished
    assert len(restored.events(RUN_ID)) == 1
    assert restored.get_command("command-1").state == "running"
    assert restored.claim_command("command-1", "worker-2") is None
    assert datetime.fromisoformat(
        restored.get_run(RUN_ID).result.actions[0].arguments["observed_at"],
    ) == NOW
    assert isinstance(json.loads(path.read_text())["runs"][RUN_ID]["result"]["actions"][0][
        "arguments"
    ]["nested"][0]["at"], str)
    restored.start_run(_run())
    assert restored.count_runs() == 1
    assert restored.is_durable is False


@pytest.mark.parametrize("content", [
    "not-json", "{}", "[]",
    '{"version":1,"runs":{},"run_hashes":{},"events":{},"commands":{"lost":{}}}',
])
def test_invalid_json_history_is_not_an_empty_queue(tmp_path, content) -> None:
    path = tmp_path / "history.json"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError):
        JsonFileCommandCenterStore(path)


def test_json_write_failure_does_not_publish_a_claim_in_memory(tmp_path, monkeypatch) -> None:
    path = tmp_path / "history.json"
    store = JsonFileCommandCenterStore(path)
    store.enqueue(_command())

    def refuse_replace(*args):
        raise OSError("Offline replacement failed")

    monkeypatch.setattr("os.replace", refuse_replace)
    with pytest.raises(OSError, match="replacement"):
        store.claim_command("command-1", "worker-1")
    assert store.get_command("command-1").state == "queued"
    assert JsonFileCommandCenterStore(path).get_command("command-1").state == "queued"
    assert not list(tmp_path.glob("*.tmp"))
    with pytest.raises(OSError, match="replacement"):
        store.start_run(_run())
    assert store.get_run(RUN_ID) is None


def test_json_failed_interruption_and_reconciliation_do_not_publish_audit(
    tmp_path, monkeypatch,
) -> None:
    path = tmp_path / "review.json"
    store = JsonFileCommandCenterStore(path)
    store.enqueue(_command())
    running = store.claim_command("command-1", "worker-1")

    def refuse_replace(*args):
        raise OSError("Offline replacement failed")

    with monkeypatch.context() as patch:
        patch.setattr("os.replace", refuse_replace)
        with pytest.raises(OSError):
            store.interrupt_command("command-1", "worker-1", "Uncertain")
    assert store.get_command("command-1") == running
    assert JsonFileCommandCenterStore(path).get_command("command-1") == running
    interrupted = store.interrupt_command("command-1", "worker-1", "Uncertain")
    with monkeypatch.context() as patch:
        patch.setattr("os.replace", refuse_replace)
        with pytest.raises(OSError):
            store.reconcile_command("command-1", "admin-1", "Checked")
    assert store.get_command("command-1") == interrupted
    assert JsonFileCommandCenterStore(path).get_command("command-1") == interrupted
    assert store.target_blocked("dataset-1") is True


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_exactly_one_concurrent_claim_winner(tmp_path, backend) -> None:
    db = _Sql(tmp_path / "concurrent.db") if backend == "sql" else None
    memory = InMemoryCommandCenterStore()
    store = AzureSqlCommandCenterStore(db) if db is not None else memory
    store.enqueue(_command())
    barrier = threading.Barrier(8)

    def claim(index):
        caller = AzureSqlCommandCenterStore(db) if db is not None else memory
        barrier.wait()
        return caller.claim_command("command-1", f"worker-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(claim, range(8)))
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert store.get_command("command-1").worker_id == winners[0].worker_id


def test_sql_cas_promoted_columns_override_stale_json(tmp_path) -> None:
    db = _Sql(tmp_path / "promoted.db")
    first = AzureSqlCommandCenterStore(db)
    second = AzureSqlCommandCenterStore(db)
    first.enqueue(_command())
    db.operations.clear()
    first.claim_command("command-1", "worker-1")
    assert db.operations[0][0] == "execute", "claim must not read before its conditional UPDATE"
    claim_sql = db.operations[0][1]
    assert "state = 'queued'" in claim_sql
    assert "SYSUTCDATETIME()" in claim_sql
    assert second.commands()[0].state == "running"
    second.finish_command("command-1", "worker-1", "failed", "Password=fixture5", RUN_ID)
    finished = first.get_command("command-1")
    assert (finished.state, finished.run_id) == ("failed", RUN_ID)
    assert "fixture5" not in finished.summary
    assert any("worker_id = ?" in sql and "lease_expires_at > SYSUTCDATETIME()" in sql
               for kind, sql, _ in db.operations if kind == "execute")
    payload = db.query("SELECT payload FROM [dbo].[triage_agent_commands]")[0][0]
    assert json.loads(payload)["state"] == "queued", "the read must not rely on this stale field"


def test_sql_lost_claim_acknowledgement_cannot_execute_work_twice(tmp_path) -> None:
    db = _Sql(tmp_path / "ambiguous.db")
    store = AzureSqlCommandCenterStore(db)
    store.enqueue(_command())
    db.lose_write_ack = True
    with pytest.raises(ConnectionError, match="acknowledgement"):
        store.claim_command("command-1", "worker-1")
    assert AzureSqlCommandCenterStore(db).get_command("command-1").state == "running"
    assert store.claim_command("command-1", "worker-1") is None
    assert store.expire_commands(NOW + timedelta(hours=1)) == 1
    assert store.claim_command("command-1", "worker-2") is None


def test_sql_runtime_reads_need_no_schema_privileges(tmp_path) -> None:
    db = _Sql(tmp_path / "schema.db")
    store = AzureSqlCommandCenterStore(db)
    assert db.operations == [], "construction must not access the database"
    assert store.is_durable is True
    assert store.get_run(RUN_ID) is None
    assert store.list_runs() == []
    assert store.count_runs() == 0
    assert store.events(RUN_ID) == []
    assert store.get_command("command-1") is None
    assert store.commands() == []
    assert store.queued_commands() == []
    assert store.active_commands() == []
    assert store.target_blocked("dataset-1") is False
    assert all(kind == "query" for kind, _, _ in db.operations)
    assert store.enqueue(_command()).state == "queued"


def test_sql_missing_deployment_schema_raises_without_runtime_migration(tmp_path) -> None:
    db = _Sql(tmp_path / "unmigrated.db")
    with db._connection() as conn:
        conn.execute("DROP TABLE triage_agent_commands")
    store = AzureSqlCommandCenterStore(db)
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        store.get_command("command-1")
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        store.enqueue(_command())


def test_sql_outage_raises_for_history_and_queue_reads_and_writes(tmp_path) -> None:
    db = _Sql(tmp_path / "outage.db")
    store = AzureSqlCommandCenterStore(db)
    store.start_run(_run())
    store.enqueue(_command())
    db.down = True
    assert store.is_durable is False
    calls = (
        lambda: store.start_run(_run()),
        lambda: store.append_event(RunEvent(run_id=RUN_ID, sequence=1, kind="status", label="test")),
        lambda: store.finish_run(RUN_ID, _result()),
        lambda: store.get_run(RUN_ID), lambda: store.list_runs(), lambda: store.count_runs(),
        lambda: store.events(RUN_ID), lambda: store.enqueue(_command()),
        lambda: store.get_command("command-1"), lambda: store.commands(),
        lambda: store.queued_commands(),
        lambda: store.active_commands(),
        lambda: store.target_blocked("dataset-1"),
        lambda: store.claim_command("command-1", "worker-1"),
        lambda: store.finish_command("command-1", "worker-1", "completed", "Done"),
        lambda: store.interrupt_command("command-1", "worker-1", "Uncertain"),
        lambda: store.reconcile_command("command-1", "admin-1", "Checked"),
        lambda: store.expire_commands(),
    )
    for call in calls:
        with pytest.raises(SqlUnavailable):
            call()
    db.down = False
    assert store.is_durable is True
    assert store.get_run(RUN_ID).state == "running"
    assert store.get_command("command-1").state == "queued"


def test_sql_write_failure_does_not_report_a_successful_run_finish_or_claim(tmp_path) -> None:
    db = _Sql(tmp_path / "write-failure.db")
    store = AzureSqlCommandCenterStore(db)
    store.start_run(_run())
    store.enqueue(_command())
    db.fail_writes = True
    with pytest.raises(ConnectionError):
        store.finish_run(RUN_ID, _result())
    with pytest.raises(ConnectionError):
        store.claim_command("command-1", "worker-1")
    db.fail_writes = False
    assert store.get_run(RUN_ID).state == "running"
    assert store.get_command("command-1").state == "queued"


def test_sql_only_one_conflicting_terminal_result_is_accepted(tmp_path) -> None:
    db = _Sql(tmp_path / "finish-race.db")
    store = AzureSqlCommandCenterStore(db)
    store.start_run(_run())
    barrier = threading.Barrier(4)

    def finish(index):
        caller = AzureSqlCommandCenterStore(db)
        barrier.wait()
        try:
            return caller.finish_run(RUN_ID, _result(summary=f"Outcome {index}"))
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        completed = [result for result in pool.map(finish, range(4)) if result is not None]
    assert len(completed) == 1
    assert store.get_run(RUN_ID).summary == completed[0].summary


def test_sql_interruption_and_reconciliation_are_fenced_and_keep_audit_in_payload(tmp_path) -> None:
    db = _Sql(tmp_path / "reconcile.db")
    store = AzureSqlCommandCenterStore(db)
    queued = store.enqueue(_command())
    store.claim_command(queued.id, "worker-1")
    db.operations.clear()
    store.interrupt_command(queued.id, "worker-1", "Uncertain", RUN_ID)
    kind, sql, _ = db.operations[0]
    assert kind == "execute", "interruption must arbitrate in SQL before reading"
    assert "state = 'running' AND worker_id = ?" in sql
    assert "lease_expires_at" not in sql
    assert store.target_blocked(queued.target_id) is True
    assert any(
        "WHERE target_id = ? AND state IN ('running', 'interrupted')" in query
        and "OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY" in query
        for kind, query, _ in db.operations if kind == "query"
    )
    reconciled = store.reconcile_command(queued.id, "admin-1", "Correlated job verified stopped")
    updates = [sql for kind, sql, _ in db.operations if kind == "execute"]
    assert any("WHERE command_id = ? AND state = 'interrupted'" in sql for sql in updates)
    payload, fingerprint = db.query(
        "SELECT payload, request_hash FROM [dbo].[triage_agent_commands] WHERE command_id = ?",
        queued.id,
    )[0]
    audit = json.loads(payload)
    assert audit["reconciled_by"] == reconciled.reconciled_by == "admin-1"
    assert audit["reconciled_at"] == reconciled.reconciled_at
    assert audit["reconciliation_reason"] == reconciled.reconciliation_reason
    assert audit["request_hash"] == fingerprint == queued.request_hash


def test_sql_lost_interruption_acknowledgement_is_idempotent_without_replay(tmp_path) -> None:
    db = _Sql(tmp_path / "interruption-ack.db")
    store = AzureSqlCommandCenterStore(db)
    store.enqueue(_command())
    store.claim_command("command-1", "worker-1")
    db.lose_write_ack = True
    with pytest.raises(ConnectionError, match="acknowledgement"):
        store.interrupt_command("command-1", "worker-1", "Original uncertainty", RUN_ID)
    interrupted = store.get_command("command-1")
    assert interrupted.state == "interrupted"
    assert store.interrupt_command("command-1", "worker-1", "Later text") == interrupted
    assert store.target_blocked("dataset-1") is True
    assert store.claim_command("command-1", "worker-2") is None


def test_sql_uses_validated_custom_tables_and_server_bounded_pages(tmp_path) -> None:
    names = ("test_runs", "test_events", "test_commands")
    db = _Sql(tmp_path / "custom.db", names)
    store = AzureSqlCommandCenterStore(db, *names)
    store.start_run(_run())
    store.enqueue(_command())
    store.append_event(RunEvent(run_id=RUN_ID, sequence=0, kind="status", label="test"))
    assert len(store.list_runs(limit=7, offset=0, signature="sig-1")) == 1
    assert len(store.commands(limit=6)) == 1
    assert len(store.queued_commands(limit=5)) == 1
    queries = [sql for kind, sql, _ in db.operations if kind == "query"]
    assert any("OFFSET 0 ROWS FETCH NEXT 7 ROWS ONLY" in sql for sql in queries)
    assert any("OFFSET 0 ROWS FETCH NEXT 6 ROWS ONLY" in sql for sql in queries)
    assert any(
        "WHERE state = 'queued' ORDER BY created_at ASC, command_id ASC "
        "OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY" in sql for sql in queries
    )
    assert any("WHERE signature = ?" in sql for sql in queries)
    ddl = schema_statements(*names)
    assert len(ddl) == 3
    assert all("IF OBJECT_ID" in sql and "CREATE TABLE" in sql for sql in ddl)
    assert all("MERGE" not in sql and "PROCEDURE" not in sql for sql in ddl)
    with pytest.raises(ValueError):
        schema_statements(command_table="unsafe; DROP TABLE anything")
    with pytest.raises(ValueError):
        AzureSqlCommandCenterStore(db, run_table="bad]")
    with pytest.raises(ValueError):
        schema_statements("same", "SAME", "different")
