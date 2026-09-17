from __future__ import annotations

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from types import ModuleType, SimpleNamespace

import pytest

from triage.store.azure_sql import (
    AzureSqlDatabase,
    SqlCommitUncertain,
    SqlRollbackUncertain,
    SqlTransactionAborted,
    schema_statements,
)


class FakeConnection:
    def __init__(self, *, fail_commit=False):
        self.autocommit = True
        self.statements = []
        self.pending = []
        self.committed = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.fail_commit = fail_commit
        self.rowcount = 1

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1
        self.committed.extend(self.pending)
        self.pending.clear()
        if self.fail_commit:
            raise RuntimeError("Commit acknowledgement lost")

    def rollback(self):
        self.rollbacks += 1
        self.pending.clear()

    def close(self):
        self.closed = True


class FakeCursor:
    rowcount = 1

    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, *params):
        if sql == "fail":
            raise ValueError("Statement rejected")
        self.connection.statements.append((sql, params, self.connection.autocommit))
        self.connection.pending.append((sql, params))

    def fetchall(self):
        return [("value",)]

    def close(self):
        pass


def database(monkeypatch, connection):
    db = AzureSqlDatabase(server="offline", database="offline")
    monkeypatch.setattr(db, "_connect", lambda: connection)
    return db


def test_statements_commit_together_and_restore_autocommit(monkeypatch):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    with db.transaction() as tx:
        assert tx is db
        assert tx.execute("first", "a") == 1
        assert tx.query("second", "b") == [("value",)]
        assert connection.committed == []
    assert connection.committed == [("first", ("a",)), ("second", ("b",))]
    assert all(not autocommit for _, _, autocommit in connection.statements)
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.autocommit


def test_body_failure_rolls_back_without_committing(monkeypatch):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    with pytest.raises(ValueError, match="Statement rejected"), db.transaction():
        db.execute("first")
        db.execute("fail")
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert not connection.committed
    assert connection.autocommit


def test_caught_statement_failure_still_prevents_partial_commit(monkeypatch):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    with pytest.raises(SqlTransactionAborted), db.transaction():
        db.execute("first")
        with pytest.raises(ValueError):
            db.execute("fail")
        with pytest.raises(SqlTransactionAborted):
            db.execute("must not execute")
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert [sql for sql, _, _ in connection.statements] == ["first"]


@pytest.mark.parametrize("values", [
    (0,), (None,), (1, 1), ("fingerprint", "source", "started"),
])
def test_query_normalizes_native_sequence_rows_to_its_tuple_contract(monkeypatch, values):
    class NativeRow:
        def __getitem__(self, index):
            return values[index]

        def __len__(self):
            return len(values)

    raw = NativeRow()
    assert raw != values
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    monkeypatch.setattr(FakeCursor, "fetchall", lambda _: [raw])
    result = db.query("SELECT native_fixture")
    assert result == [values]
    assert type(result[0]) is tuple
    assert result[0][:2] == values[:2]


def test_lost_commit_is_not_rolled_back_or_retried_as_if_nothing_happened(monkeypatch):
    connection = FakeConnection(fail_commit=True)
    db = database(monkeypatch, connection)
    with pytest.raises(SqlCommitUncertain, match="reconcile"), db.transaction():
        db.execute("receipt")
        db.execute("work")
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.committed == [("receipt", ()), ("work", ())]
    assert connection.closed
    assert db._local.conn is None


def test_nested_transaction_is_rejected_and_aborts_outer_work(monkeypatch):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    with pytest.raises(SqlTransactionAborted), db.transaction():
        db.execute("first")
        with pytest.raises(SqlTransactionAborted, match="Nested"), db.transaction():
            pytest.fail("Nested transaction must not start")
    assert connection.rollbacks == 1
    assert connection.commits == 0


def test_lost_connection_cannot_reconnect_inside_transaction(monkeypatch):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    with pytest.raises(SqlTransactionAborted), db.transaction():
        db.execute("first")
        db._drop()
        db.execute("must not reconnect")
    assert [sql for sql, _, _ in connection.statements] == ["first"]
    assert db._local.conn is None


def test_thread_transactions_do_not_share_connections(monkeypatch):
    db = AzureSqlDatabase(server="offline", database="offline")
    connections = []
    lock = Lock()

    def connect():
        connection = FakeConnection()
        with lock:
            connections.append(connection)
        return connection

    monkeypatch.setattr(db, "_connect", connect)

    def write(index):
        with db.transaction():
            db.execute("write", index)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(write, range(20)))
    assert sum(connection.commits for connection in connections) == 20
    assert all(connection.autocommit for connection in connections)
    assert sum(len(connection.committed) for connection in connections) == 20


def test_default_sql_identity_excludes_human_and_secret_fallbacks(monkeypatch):
    arguments = {}

    def credential(**kwargs):
        arguments.update(kwargs)
        return SimpleNamespace(get_token=lambda scope: SimpleNamespace(token="offline-token"))

    identity = ModuleType("azure.identity")
    identity.DefaultAzureCredential = credential
    driver = ModuleType("mssql_python")
    driver.connect = lambda *args, **kwargs: FakeConnection()
    monkeypatch.setitem(sys.modules, "azure", ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    monkeypatch.setitem(sys.modules, "mssql_python", driver)
    db = AzureSqlDatabase(server="offline", database="offline")
    db._connect()
    assert arguments == {
        "exclude_environment_credential": True,
        "exclude_cli_credential": True,
        "exclude_developer_cli_credential": True,
        "exclude_interactive_browser_credential": True,
        "exclude_shared_token_cache_credential": True,
        "exclude_visual_studio_code_credential": True,
        "exclude_powershell_credential": True,
        "exclude_broker_credential": True,
    }


def test_callback_decision_cannot_omit_fingerprint_expiry_or_consumption_guards():
    procedure = next(
        statement for statement in schema_statements({})
        if "CREATE OR ALTER PROCEDURE dbo.triage_record_approval_decision" in statement
    )
    assert "@fingerprint NVARCHAR(200) = NULL" not in procedure
    assert "@fingerprint IS NULL" not in procedure
    assert "JSON_VALUE(payload, '$.expires_at') IS NULL" not in procedure
    assert "DATALENGTH(@fingerprint) = 128" in procedure
    assert "NULLIF(JSON_VALUE(payload, '$.consumed_at'), '') IS NULL" in procedure
    assert "JSON_VALUE(payload, '$.delivery_channel') = 'teams'" in procedure
    assert "@decision IN ('approve', 'decline')" in procedure
    assert "COLLATE Latin1_General_100_BIN2" in procedure
    assert "SELECT @@ROWCOUNT AS recorded" in procedure


def test_unacknowledged_rollback_is_not_reported_as_an_ordinary_refusal(monkeypatch):
    connection = FakeConnection()
    db = database(monkeypatch, connection)

    def failed_rollback():
        raise RuntimeError("Rollback acknowledgement lost")

    connection.rollback = failed_rollback
    with pytest.raises(SqlRollbackUncertain, match="reconcile"), db.transaction():
        db.execute("provisional charge")
        raise ValueError("A second budget denied the request")
    assert connection.closed
    assert db._local.conn is None


@pytest.mark.parametrize("method", ["execute", "query"])
@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_caught_statement_interrupt_cannot_commit_partial_work(monkeypatch, method, interrupt_type):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    original_execute = FakeCursor.execute
    interruption = interrupt_type("interrupted statement")

    def execute(cursor, sql, *params):
        if sql == "interrupt":
            raise interruption
        original_execute(cursor, sql, *params)

    monkeypatch.setattr(FakeCursor, "execute", execute)
    with pytest.raises(SqlTransactionAborted), db.transaction():
        db.execute("receipt")
        with pytest.raises(interrupt_type) as caught:
            getattr(db, method)("interrupt")
        assert caught.value is interruption
        with pytest.raises(SqlTransactionAborted):
            db.execute("must not run")
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.committed == []
    assert connection.pending == []


@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_commit_interrupt_discards_connection_without_resetting_autocommit(
    monkeypatch, applied, interrupt_type,
):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    interruption = interrupt_type("interrupted commit")

    def commit():
        connection.commits += 1
        if applied:
            connection.committed.extend(connection.pending)
            connection.pending.clear()
        raise interruption

    monkeypatch.setattr(connection, "commit", commit)
    with pytest.raises(interrupt_type) as caught, db.transaction():
        db.execute("receipt")
        db.execute("work")
    assert caught.value is interruption
    assert any("reconcile" in note for note in caught.value.__notes__)
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed
    assert not connection.autocommit
    assert db._local.conn is None
    assert not db._local.transaction_active
    assert not db._local.transaction_failed
    assert len(connection.committed) == (2 if applied else 0)


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_rollback_interrupt_preserves_interruption_and_discards_connection(monkeypatch, interrupt_type):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    interruption = interrupt_type("interrupted rollback")

    def rollback():
        connection.rollbacks += 1
        raise interruption

    monkeypatch.setattr(connection, "rollback", rollback)
    with pytest.raises(interrupt_type) as caught, db.transaction():
        db.execute("provisional charge")
        raise ValueError("second budget refused")
    assert caught.value is interruption
    assert any("rollback" in note and "reconcile" in note for note in caught.value.__notes__)
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.pending == [("provisional charge", ())]
    assert connection.closed
    assert not connection.autocommit
    assert db._local.conn is None


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_body_interruption_is_not_downgraded_when_rollback_fails(monkeypatch, interrupt_type):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    interruption = interrupt_type("interrupted transaction")

    def rollback():
        raise RuntimeError("rollback acknowledgement lost")

    monkeypatch.setattr(connection, "rollback", rollback)
    with pytest.raises(interrupt_type) as caught, db.transaction():
        db.execute("provisional charge")
        raise interruption
    assert caught.value is interruption
    assert isinstance(caught.value.__cause__, RuntimeError)
    assert any("rollback" in note and "reconcile" in note for note in caught.value.__notes__)
    assert connection.closed
    assert not connection.autocommit
    assert db._local.conn is None


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_cursor_close_interruption_is_not_swallowed(monkeypatch, interrupt_type):
    connection = FakeConnection()
    db = database(monkeypatch, connection)
    interruption = interrupt_type("interrupted cursor close")

    def close(cursor):
        raise interruption

    monkeypatch.setattr(FakeCursor, "close", close)
    with pytest.raises(SqlTransactionAborted), db.transaction():
        with pytest.raises(interrupt_type) as caught:
            db.execute("receipt")
        assert caught.value is interruption
    assert connection.commits == 0
    assert connection.closed
    assert db._local.conn is None


@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_autocommit_reset_interruption_discards_connection_and_propagates(monkeypatch, interrupt_type):
    interruption = interrupt_type("interrupted mode reset")

    class InterruptedResetConnection(FakeConnection):
        interrupt_reset = False

        @property
        def autocommit(self):
            return self._autocommit

        @autocommit.setter
        def autocommit(self, value):
            if value and self.interrupt_reset:
                raise interruption
            self._autocommit = value

    connection = InterruptedResetConnection()
    db = database(monkeypatch, connection)
    with pytest.raises(interrupt_type) as caught, db.transaction():
        db.execute("receipt")
        connection.interrupt_reset = True
    assert caught.value is interruption
    assert connection.commits == 1
    assert connection.committed == [("receipt", ())]
    assert connection.closed
    assert db._local.conn is None
