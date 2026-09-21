"""A pooled connection that idled out must not surface as a failed read.

The deployed command center sat idle for two days. Every first request after
that answered 503 with ``Communication link failure``, then recovered on the
next call, so the UI showed empty lists with no reason. These pin the retry and,
just as importantly, the places it must not happen.
"""

from __future__ import annotations

from threading import Lock

import pytest

from triage.store.azure_sql import AzureSqlDatabase, SqlTransactionAborted


class OperationalError(Exception):
    """Named to match what the driver raises for a dead link."""


class StaleCursor:
    rowcount = 1

    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, *params):
        self.connection.attempts.append(sql)
        if self.connection.stale:
            raise OperationalError("Communication link failure")
        self.connection.statements.append(sql)

    def fetchall(self):
        return [("value",)]

    def close(self):
        pass


class StaleConnection:
    def __init__(self, *, stale: bool) -> None:
        self.stale = stale
        self.attempts: list[str] = []
        self.statements: list[str] = []
        self.closed = False
        self.autocommit = True

    def cursor(self):
        return StaleCursor(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _database(monkeypatch, connections):
    db = AzureSqlDatabase(server="offline", database="offline")
    lock = Lock()
    order = list(connections)

    def connect():
        with lock:
            return order.pop(0)

    monkeypatch.setattr(db, "_connect", connect)
    return db


def test_idle_read_retries_once_on_a_fresh_connection(monkeypatch):
    stale = StaleConnection(stale=True)
    fresh = StaleConnection(stale=False)
    db = _database(monkeypatch, [stale, fresh])

    assert db.query("SELECT 1") == [("value",)]

    assert stale.attempts == ["SELECT 1"]
    assert stale.closed is True
    assert fresh.statements == ["SELECT 1"]


def test_a_second_dead_connection_is_reported_rather_than_retried_forever(monkeypatch):
    first = StaleConnection(stale=True)
    second = StaleConnection(stale=True)
    db = _database(monkeypatch, [first, second])

    with pytest.raises(OperationalError):
        db.query("SELECT 1")

    assert first.attempts == ["SELECT 1"]
    assert second.attempts == ["SELECT 1"]


def test_a_rejected_statement_is_not_retried(monkeypatch):
    connection = StaleConnection(stale=False)
    db = _database(monkeypatch, [connection])

    def reject(self, sql, *params):
        connection.attempts.append(sql)
        raise ValueError("Invalid column name")

    monkeypatch.setattr(StaleCursor, "execute", reject)

    with pytest.raises(ValueError):
        db.query("SELECT missing")

    assert connection.attempts == ["SELECT missing"]
    assert connection.closed is False


def test_writes_are_never_replayed_after_a_lost_link(monkeypatch):
    stale = StaleConnection(stale=True)
    fresh = StaleConnection(stale=False)
    db = _database(monkeypatch, [stale, fresh])

    with pytest.raises(OperationalError):
        db.execute("UPDATE claims SET owner = ?", "a")

    assert stale.attempts == ["UPDATE claims SET owner = ?"]
    assert fresh.attempts == []
    assert fresh.statements == []


def test_transaction_refreshes_dead_pooled_connection_before_begin(monkeypatch):
    stale = StaleConnection(stale=False)
    fresh = StaleConnection(stale=False)
    db = _database(monkeypatch, [stale, fresh])
    assert db.query("SELECT warmup") == [("value",)]
    stale.stale = True

    with db.transaction():
        assert db.query("SELECT inside") == [("value",)]

    assert stale.attempts == ["SELECT warmup", "SELECT 1"]
    assert stale.closed is True
    assert fresh.attempts == ["SELECT 1", "SELECT inside"]
    assert fresh.statements == ["SELECT 1", "SELECT inside"]


def test_a_read_inside_a_transaction_does_not_reconnect(monkeypatch):
    stale = StaleConnection(stale=False)
    fresh = StaleConnection(stale=False)
    db = _database(monkeypatch, [stale, fresh])

    # The driver error propagates unchanged; what matters is that no second
    # connection was opened to finish the transaction's remaining work.
    with pytest.raises(OperationalError), db.transaction():
        stale.stale = True
        db.query("SELECT 1")

    assert stale.attempts == ["SELECT 1"]
    assert fresh.attempts == []
    assert db._local.conn is None


def test_a_transaction_aborted_by_a_lost_link_refuses_further_statements(monkeypatch):
    stale = StaleConnection(stale=False)
    fresh = StaleConnection(stale=False)
    db = _database(monkeypatch, [stale, fresh])

    with pytest.raises(OperationalError), db.transaction():
        stale.stale = True
        try:
            db.query("SELECT 1")
        except OperationalError:
            pass
        with pytest.raises(SqlTransactionAborted):
            db.query("SELECT 2")
        raise OperationalError("Communication link failure")

    assert fresh.attempts == []
