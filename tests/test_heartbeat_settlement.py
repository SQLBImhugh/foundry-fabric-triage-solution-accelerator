from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager

import pytest
from test_command_center_store import _Sql
from test_monitoring_queue_selection import QueueFixture

from triage.command_center.worker import drain_commands
from triage.monitoring.controller import controller_heartbeat
from triage.runner import TriageRunner
from triage.settings import Settings
from triage.store.command_center import AzureSqlCommandCenterStore
from triage.store.incidents import InMemoryIncidentStore


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    queue = QueueFixture(True)
    monkeypatch.setattr("triage.runner.FIXTURE_TENANT_ID", queue.h.control.tenant_id)
    lock = threading.RLock()
    transaction = queue.db.transaction

    @contextmanager
    def serialized_transaction():
        with lock, transaction() as db:
            yield db

    monkeypatch.setattr(queue.db, "transaction", serialized_transaction)
    settings = Settings(
        _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
        triage_provider_mode="mock", azure_sql_server="", azure_sql_database="",
        applicationinsights_connection_string="",
    )
    runner = TriageRunner(
        settings, base_dir=tmp_path, monitoring_store=queue.store, store=InMemoryIncidentStore(),
    )
    return runner, queue


async def test_human_slot_can_start_while_a_sql_claim_is_blocked(runtime, monkeypatch):
    runner, queue = runtime
    entered, release, human_started = threading.Event(), threading.Event(), threading.Event()
    observed = []
    query = queue.db.query

    def delayed_query(sql, *parameters):
        if "WITH projected AS" in sql and not entered.is_set():
            entered.set()
            assert release.wait(2), "The test must release the original SQL call"
        return query(sql, *parameters)

    def observe_and_release():
        if not entered.wait(2):
            observed.append(False)
            release.set()
            return
        time.sleep(.1)
        observed.append(human_started.is_set())
        release.set()

    async def human(_runner, *, limit, budget):
        human_started.set()
        return []

    monkeypatch.setattr(queue.db, "query", delayed_query)
    observer = threading.Thread(target=observe_and_release)
    observer.start()
    try:
        await controller_heartbeat(runner, command_drain=human)
    finally:
        release.set()
        observer.join(2)
    assert observed == [True], "The native SQL claim blocked all heartbeat coroutine progress"


@pytest.mark.parametrize("cancellations", [1, 2])
async def test_cancellation_waits_for_the_original_reconciliation_to_settle(runtime, monkeypatch, cancellations):
    runner, queue = runtime
    original = queue.discovery()
    entered, release = threading.Event(), threading.Event()
    query = queue.db.query
    publications = []

    def delayed_publication(sql, *parameters):
        if sql.startswith(f"EXEC {queue.db.names.object('controller_resolve_frontier')} "):
            publications.append(sql)
            entered.set()
            assert release.wait(3), "The test must release the original reconciliation"
        return query(sql, *parameters)

    async def human(_runner, *, limit, budget):
        return []

    monkeypatch.setattr(queue.db, "query", delayed_publication)
    task = asyncio.create_task(controller_heartbeat(runner, command_drain=human, rounds=2))
    returned_before_settlement = False
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        for _ in range(cancellations):
            task.cancel()
            await asyncio.sleep(.025)
        returned_before_settlement = task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    completed = await asyncio.to_thread(queue.store.get_work, queue.h.version, original.work_id)
    assert completed.state == "completed"
    assert len(publications) == 1
    assert not returned_before_settlement, "Cancellation returned while the original SQL operation still owned work"


async def test_human_queue_cancellation_waits_for_its_original_sql_write(runtime, tmp_path, monkeypatch):
    runner, _ = runtime
    db = _Sql(tmp_path / "command-state.db")
    runner._command_center_store = AzureSqlCommandCenterStore(db)
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()
    execute = db.execute

    def delayed_write(sql, *parameters):
        entered.set()
        assert release.wait(3)
        result = execute(sql, *parameters)
        completed.set()
        return result

    monkeypatch.setattr(db, "execute", delayed_write)
    task = asyncio.create_task(drain_commands(runner))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(.025)
        returned_before_settlement = task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert await asyncio.to_thread(completed.wait, 2)
    assert not returned_before_settlement


async def test_failure_after_cancellation_is_observed_without_replaying_work(runtime, monkeypatch, caplog):
    runner, queue = runtime
    original = queue.discovery()
    entered, release = threading.Event(), threading.Event()
    query = queue.db.query
    attempts = []
    private = "synthetic-private-row-that-must-not-enter-telemetry"

    def refused_publication(sql, *parameters):
        if sql.startswith(f"EXEC {queue.db.names.object('controller_resolve_frontier')} "):
            attempts.append(sql)
            entered.set()
            assert release.wait(3)
            raise RuntimeError(f"Guarded refusal {private} (51072)")
        return query(sql, *parameters)

    async def human(_runner, *, limit, budget):
        return []

    monkeypatch.setattr(queue.db, "query", refused_publication)
    task = asyncio.create_task(controller_heartbeat(runner, command_drain=human, rounds=2))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(.025)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    remaining = await asyncio.to_thread(queue.store.get_work, queue.h.version, original.work_id)
    assert remaining.state == "leased" and remaining.attempts == 1
    assert len(attempts) == 1
    exported = [record.getMessage() for record in caplog.records if record.name.startswith("triage.telemetry")]
    assert any("heartbeat_sync_settled status=failed error_type=MonitoringConflict" in row for row in exported)
    assert all(private not in row for row in exported)
