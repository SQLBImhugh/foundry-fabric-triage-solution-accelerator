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

    assert db.read("SELECT 1") == [("value",)]

    assert stale.attempts == ["SELECT 1"]
    assert stale.closed is True
    assert fresh.statements == ["SELECT 1"]


def test_query_opt_in_read_only_retries_once_on_a_fresh_connection(monkeypatch):
    stale = StaleConnection(stale=True)
    fresh = StaleConnection(stale=False)
    db = _database(monkeypatch, [stale, fresh])

    assert db.query("SELECT 1", read_only=True) == [("value",)]

    assert stale.attempts == ["SELECT 1"]
    assert stale.closed is True
    assert fresh.statements == ["SELECT 1"]


def test_a_second_dead_connection_is_reported_rather_than_retried_forever(monkeypatch):
    first = StaleConnection(stale=True)
    second = StaleConnection(stale=True)
    db = _database(monkeypatch, [first, second])

    with pytest.raises(OperationalError):
        db.read("SELECT 1")

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
        db.read("SELECT missing")

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


@pytest.mark.parametrize(
    "statement",
    [
        "EXEC dbo.mutate_once ?, ?",
        "SELECT 1; EXEC dbo.mutate_once ?, ?",
    ],
)
@pytest.mark.parametrize("failure_phase", ["execute", "fetchall"])
def test_generic_result_calls_are_not_replayed_and_reconcile_from_original_receipt(
    monkeypatch, statement: str, failure_phase: str,
):
    effects: list[str] = []
    receipts: dict[str, tuple[str, str]] = {}

    class ReceiptCursor:
        rowcount = 1

        def __init__(self, connection: StaleConnection) -> None:
            self.connection = connection
            self.rows: list[tuple[str, ...]] = []

        def execute(self, sql: str, *params):
            self.connection.attempts.append(sql)
            if sql.startswith("SELECT receipt_json"):
                request_id, fingerprint = params
                stored = receipts.get(request_id)
                self.rows = [(stored[1],)] if stored and stored[0] == fingerprint else []
                self.connection.statements.append(sql)
                return
            request_id, fingerprint = params
            effects.append(request_id)
            receipts[request_id] = (fingerprint, f'{{"request_id":"{request_id}","status":"applied"}}')
            self.rows = [(receipts[request_id][1],)]
            if self.connection.stale and failure_phase == "execute":
                raise OperationalError("Communication link failure after autocommit mutation")
            self.connection.statements.append(sql)

        def fetchall(self):
            if self.connection.stale and failure_phase == "fetchall":
                raise OperationalError("Communication link failure while fetching RPC result")
            return list(self.rows)

        def close(self):
            pass

    stale = StaleConnection(stale=True)
    fresh = StaleConnection(stale=False)
    monkeypatch.setattr(StaleConnection, "cursor", lambda self: ReceiptCursor(self))
    db = _database(monkeypatch, [stale, fresh])

    with pytest.raises(OperationalError):
        db.query(statement, "req-1", "fp-1")

    assert effects == ["req-1"]
    assert stale.closed is True
    assert fresh.attempts == []
    assert db.read(
        "SELECT receipt_json FROM receipts WHERE request_id = ? AND fingerprint = ?",
        "req-1",
        "fp-1",
    ) == [('{"request_id":"req-1","status":"applied"}',)]
    assert effects == ["req-1"]


def test_transaction_refreshes_dead_pooled_connection_before_begin(monkeypatch):
    stale = StaleConnection(stale=False)
    fresh = StaleConnection(stale=False)
    db = _database(monkeypatch, [stale, fresh])
    assert db.read("SELECT warmup") == [("value",)]
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
        db.read("SELECT 1")

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
            db.read("SELECT 1")
        except OperationalError:
            pass
        with pytest.raises(SqlTransactionAborted):
            db.read("SELECT 2")
        raise OperationalError("Communication link failure")

    assert fresh.attempts == []


@pytest.mark.parametrize("failure_mode", ["fetch_abort", "commit_uncertain"])
def test_transactional_rpc_fetch_and_commit_uncertainty_reconcile_with_original_receipts(
    monkeypatch, failure_mode: str,
):
    from triage.store.azure_sql import SqlCommitUncertain

    committed_effects: list[str] = []
    committed_receipts: dict[str, tuple[str, str]] = {}
    rpc_invocations = 0

    class TransactionalReceiptConnection:
        def __init__(self, *, fault: str | None = None) -> None:
            self.fault = fault
            self.autocommit = True
            self.closed = False
            self.pending_effects: list[str] = []
            self.pending_receipts: dict[str, tuple[str, str]] = {}

        def cursor(self):
            connection = self

            class Cursor:
                rowcount = 1

                def __init__(self) -> None:
                    self.rows: list[tuple[str, ...]] = []
                    self.is_rpc = False

                def execute(self, sql: str, *params):
                    self.is_rpc = False
                    if sql == "SELECT 1":
                        self.rows = [(1,)]
                        return
                    if sql.startswith("SELECT fingerprint, payload FROM receipts"):
                        (request_id,) = params
                        receipt = (
                            connection.pending_receipts.get(request_id)
                            or committed_receipts.get(request_id)
                        )
                        self.rows = [receipt] if receipt is not None else []
                        return
                    if sql.startswith("EXEC dbo.guarded_rpc"):
                        nonlocal rpc_invocations
                        self.is_rpc = True
                        rpc_invocations += 1
                        request_id, fingerprint = params
                        payload = f'{{"request_id":"{request_id}","status":"applied"}}'
                        connection.pending_effects.append(request_id)
                        connection.pending_receipts[request_id] = (fingerprint, payload)
                        self.rows = [(payload,)]
                        return
                    raise AssertionError(f"Unexpected SQL: {sql}")

                def fetchall(self):
                    if self.is_rpc and connection.fault == "fetch_abort":
                        connection.fault = None
                        raise OperationalError("Lost connection while fetching RPC result")
                    return list(self.rows)

                def close(self):
                    pass

            return Cursor()

        def commit(self):
            committed_effects.extend(self.pending_effects)
            committed_receipts.update(self.pending_receipts)
            self.pending_effects.clear()
            self.pending_receipts.clear()
            if self.fault == "commit_uncertain":
                self.fault = None
                raise OperationalError("Lost commit acknowledgement")

        def rollback(self):
            self.pending_effects.clear()
            self.pending_receipts.clear()

        def close(self):
            self.closed = True

    first = TransactionalReceiptConnection(fault=failure_mode)
    second = TransactionalReceiptConnection()
    db = _database(monkeypatch, [first, second])

    def run_idempotent(request_id: str, fingerprint: str) -> str:
        with db.transaction():
            existing = db.query(
                "SELECT fingerprint, payload FROM receipts WHERE request_id = ?",
                request_id,
            )
            if existing:
                saved_fingerprint, saved_payload = existing[0]
                assert saved_fingerprint == fingerprint
                return saved_payload
            rows = db.query("EXEC dbo.guarded_rpc ?, ?", request_id, fingerprint)
            return rows[0][0]

    expected_error = OperationalError if failure_mode == "fetch_abort" else SqlCommitUncertain
    with pytest.raises(expected_error):
        run_idempotent("req-tx-1", "fp-tx-1")

    assert first.closed is True
    assert db._local.conn is None
    if failure_mode == "fetch_abort":
        assert committed_effects == []
        assert committed_receipts == {}
        assert rpc_invocations == 1
    else:
        assert committed_effects == ["req-tx-1"]
        assert "req-tx-1" in committed_receipts
        assert rpc_invocations == 1

    reconciled = run_idempotent("req-tx-1", "fp-tx-1")
    assert reconciled == '{"request_id":"req-tx-1","status":"applied"}'
    assert committed_effects == ["req-tx-1"]
    assert committed_receipts["req-tx-1"] == (
        "fp-tx-1",
        '{"request_id":"req-tx-1","status":"applied"}',
    )
    assert rpc_invocations == (2 if failure_mode == "fetch_abort" else 1)


def test_established_store_reads_opt_in_to_dead_connection_recovery(monkeypatch):
    from triage.store.sql_incidents import AzureSqlIncidentStore

    incident_json = (
        '{"id": "inc-1", "signature": "sig-1", "status": "open", '
        '"outcome": "needs_human", "summary": "s", "request_id": "r-0", '
        '"occurrence_count": 1}'
    )
    warmup = StaleConnection(stale=False)
    fresh = StaleConnection(stale=False)
    monkeypatch.setattr(StaleCursor, "fetchall", lambda _: [(incident_json,)])
    db = _database(monkeypatch, [warmup, fresh])

    store = AzureSqlIncidentStore(db=db)
    warmup.stale = True

    found = store.find_open("sig-1")
    assert found is not None
    assert found.id == "inc-1"
    assert warmup.closed is True
    assert len(fresh.statements) == 1

