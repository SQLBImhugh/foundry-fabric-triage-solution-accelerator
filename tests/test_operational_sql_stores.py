"""Operational-store failures and recovery against offline, shared SQL state."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from triage.approvals import ApprovalRequest, WebApprovalGate
from triage.models import TriageResult
from triage.store.approvals import AzureSqlApprovalChannel
from triage.store.azure_sql import SqlCommitUncertain, SqlUnavailable
from triage.store.inbox_audit import AzureSqlInboxAudit
from triage.store.processed import AzureSqlProcessedLog
from triage.store.retries import AzureSqlRetryStore
from triage.store.semantic_health import AzureSqlSemanticHealthStore, ProbeState
from triage.store.sql_incidents import AzureSqlIncidentStore

NOW = datetime(2026, 9, 15, 12, tzinfo=UTC)
KINDS = ("incidents", "processed", "retries", "health", "audit", "approvals")
TABLES = {
    "incidents": "triage_incidents",
    "processed": "triage_processed_messages",
    "retries": "triage_deferred_retries",
    "health": "triage_semantic_health",
    "audit": "triage_inbox_audit",
    "approvals": "triage_approvals",
}
READS = {
    "incidents": ("find_open", "get", "list_all"),
    "processed": ("seen", "count"),
    "retries": ("get", "is_deferred", "due", "pending"),
    "health": ("get", "all_states"),
    "audit": ("recent", "count"),
    "approvals": ("get", "get_exact", "poll", "pending", "list_requests"),
}
WRITES = {
    "incidents": ("record", "mark", "reset"),
    "processed": ("mark", "reset"),
    "retries": ("defer", "complete", "reset"),
    "health": ("put", "try_acquire_lease", "release_lease", "reset"),
    "audit": ("record", "reset"),
    "approvals": ("open", "open_exact", "decide", "decide_exact", "consume_exact", "reset"),
}


class OperationalSql:
    """Execute real store DML with local constraints and independent connections.

    Adapt SQL Server syntax/functions only. Failure injection distinguishes
    rejection before execution from loss of acknowledgement after commit.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.down = False
        self.fail_writes = False
        self.lose_write_ack = False
        self.failure_prefix = ""
        self.rowcount_override: int | None = None
        self.before_execute: Callable[[], None] | None = None
        self.operations: list[tuple[str, str, tuple]] = []
        self.last_failure: Exception | None = None
        with self.connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS triage_incidents (
                    incident_id TEXT PRIMARY KEY, signature TEXT NOT NULL, status TEXT NOT NULL,
                    updated_at TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS triage_processed_messages (
                    fingerprint TEXT PRIMARY KEY, message_id TEXT, received_at TEXT);
                CREATE TABLE IF NOT EXISTS triage_deferred_retries (
                    signature TEXT PRIMARY KEY, status TEXT, due_at TEXT, attempts INTEGER NOT NULL,
                    payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS triage_semantic_health (
                    probe_key TEXT PRIMARY KEY, probe_name TEXT, report_name TEXT, last_max_date TEXT,
                    last_row_count INTEGER, suspect_count INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS triage_sweep_leases (
                    lease_name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS triage_inbox_audit (
                    fingerprint TEXT PRIMARY KEY, sender TEXT, subject TEXT, reason TEXT,
                    ignored_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS triage_approvals (
                    request_id TEXT PRIMARY KEY, decision TEXT, responder TEXT, decided_at TEXT,
                    payload TEXT NOT NULL);
            """)

    def connection(self):
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        conn.create_function(
            "HASHBYTES", 2,
            lambda algorithm, raw: hashlib.sha256(raw.encode("utf-16-le")).digest()
            if algorithm == "SHA2_256" else None,
        )
        conn.create_function("UTC_NOW", 0, lambda: NOW.isoformat())
        conn.create_function("ADD_SECONDS", 1, lambda seconds: (NOW + timedelta(seconds=seconds)).isoformat())
        return conn

    @staticmethod
    def translate(statement: str) -> str:
        sql = statement.replace("[dbo].", "")
        sql = re.sub(r" WITH \((?:UPDLOCK, )?HOLDLOCK\)", "", sql)
        sql = sql.replace("JSON_VALUE", "json_extract").replace("JSON_MODIFY", "json_set")
        sql = re.sub(
            r"TRY_CAST\((json_extract\(payload, '[^']+'\)) AS DATETIMEOFFSET\)",
            r"julianday(\1)", sql,
        )
        sql = sql.replace("DATEADD(second, ?, SYSUTCDATETIME())", "ADD_SECONDS(?)")
        sql = sql.replace("SYSDATETIMEOFFSET()", "julianday(UTC_NOW())")
        sql = sql.replace("SYSUTCDATETIME()", "UTC_NOW()")
        if match := re.search(r"TOP \((\d+)\) ", sql):
            sql = sql.replace(match[0], "") + f" LIMIT {match[1]}"
        return sql

    @property
    def is_available(self) -> bool:
        return not self.down

    def ensure_schema_once(self):
        raise AssertionError("Runtime identities cannot create or repair schema")

    @staticmethod
    def integrity_error():
        return sqlite3.IntegrityError

    def query(self, sql: str, *params: Any) -> list[tuple]:
        self.operations.append(("query", sql, params))
        if self.down:
            raise SqlUnavailable("Offline database unavailable")
        with self.connection() as conn:
            return conn.execute(self.translate(sql), params).fetchall()

    def execute(self, sql: str, *params: Any) -> int:
        self.operations.append(("execute", sql, params))
        if self.before_execute is not None:
            callback, self.before_execute = self.before_execute, None
            callback()
        inject = sql.lstrip().startswith(self.failure_prefix)
        if self.down or (self.fail_writes and inject):
            self.last_failure = SqlUnavailable("Offline write rejected")
            raise self.last_failure
        with self.connection() as conn:
            changed = conn.execute(self.translate(sql), params).rowcount
        if self.lose_write_ack and inject:
            self.lose_write_ack = False
            self.last_failure = SqlCommitUncertain("Offline commit acknowledgement lost")
            raise self.last_failure
        return changed if self.rowcount_override is None else self.rowcount_override

    def writes(self) -> list[tuple[str, str, tuple]]:
        return [operation for operation in self.operations if operation[0] == "execute"]


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch) -> None:
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr("triage.store.approvals.datetime", Clock)
    monkeypatch.setattr("triage.store.retries._utcnow", lambda: NOW)
    for module in ("incidents", "sql_incidents", "semantic_health", "inbox_audit"):
        monkeypatch.setattr(f"triage.store.{module}._utcnow", lambda: NOW.isoformat())


@pytest.fixture
def database(tmp_path) -> OperationalSql:
    return OperationalSql(tmp_path / "operational.db")


def build(kind: str, db: OperationalSql):
    factory = {
        "incidents": AzureSqlIncidentStore,
        "processed": AzureSqlProcessedLog,
        "retries": AzureSqlRetryStore,
        "health": AzureSqlSemanticHealthStore,
        "audit": AzureSqlInboxAudit,
        "approvals": AzureSqlApprovalChannel,
    }[kind]
    return factory(db=db)


def result(signature: str = "one", **updates) -> TriageResult:
    return TriageResult(**{
        "signature": signature, "request_id": "source-one", "outcome": "needs_human",
        "summary": "Synthetic failure",
    } | updates)


def proposal(key: str = "one", **updates) -> ApprovalRequest:
    return ApprovalRequest(**{
        "request_id": key, "action": "rerun_fabric_pipeline", "arguments": {"pipeline_id": "synthetic"},
        "justification": "Offline test", "requested_at": NOW, "timeout_seconds": 600,
    } | updates)


def seed(kind: str, store, key: str = "one") -> str:
    if kind == "incidents":
        return store.record(result(key), report_name="Synthetic model").id
    if kind == "processed":
        store.mark(key, received_at=NOW.isoformat())
    elif kind == "retries":
        store.defer(signature=key, workspace_id="w", dataset_id="d", retry_after_seconds=60)
    elif kind == "health":
        store.put(ProbeState("w", "d", key, last_row_count=10, observations=1))
    elif kind == "audit":
        store.record(message_id=key, sender="monitor", subject="Synthetic failure", reason="Not allowed")
    elif kind == "approvals":
        store.open(proposal(key))
    return key


def snapshot(kind: str, store):
    method = {
        "incidents": "list_all", "processed": "count", "retries": "pending",
        "health": "all_states", "audit": "recent", "approvals": "list_requests",
    }[kind]
    return getattr(store, method)()


def read(kind: str, method: str, store, identifier: str):
    if kind == "audit" and method == "count":
        return store.count
    if method == "poll":
        return asyncio.run(store.poll(identifier))
    if kind == "health" and method == "get":
        return store.get("w", "d", identifier)
    if method in {"find_open", "is_deferred", "seen"}:
        return getattr(store, method)("one")
    if method in {"get", "get_exact"}:
        return getattr(store, method)(identifier)
    return getattr(store, method)()


def mutate(kind: str, method: str, store, identifier: str) -> None:
    if method == "reset":
        store.reset()
    elif kind == "incidents":
        if method == "mark":
            store.mark(identifier, "investigating", "Offline review")
        else:
            store.record(result(), notified=True)
    elif kind == "processed":
        store.mark(identifier, received_at=NOW.isoformat())
    elif kind == "retries":
        if method == "complete":
            store.complete(identifier, outcome="resolved")
        else:
            store.defer(signature=identifier)
    elif kind == "health":
        if method == "put":
            store.put(ProbeState("w", "d", identifier, last_row_count=20, observations=2))
        elif method == "try_acquire_lease":
            store.try_acquire_lease("sweep", "owner", 120)
        else:
            store.release_lease("sweep", "owner")
    elif kind == "audit":
        seed(kind, store, "two")
    elif method in {"open", "open_exact"}:
        getattr(store, method)(proposal("two"))
    elif method == "decide":
        store.decide(identifier, decision="approve", responder="operator")
    elif method == "decide_exact":
        store.decide_exact(
            identifier, decision="approve", responder="operator", fingerprint=proposal().fingerprint,
        )
    elif method == "consume_exact":
        store.consume_exact(identifier, proposal().fingerprint)


@pytest.mark.parametrize("kind", KINDS)
def test_missing_deployment_schema_is_an_error_without_runtime_ddl(database, kind) -> None:
    with database.connection() as conn:
        conn.execute(f"DROP TABLE {TABLES[kind]}")
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        snapshot(kind, build(kind, database))
    assert not database.writes()


@pytest.mark.parametrize("kind", KINDS)
def test_incompatible_deployment_schema_is_not_a_healthy_empty_store(database, kind) -> None:
    column = {"processed": "received_at", "audit": "ignored_at"}.get(kind, "payload")
    with database.connection() as conn:
        conn.execute(f"ALTER TABLE {TABLES[kind]} DROP COLUMN {column}")
    with pytest.raises(sqlite3.OperationalError, match="no such column"):
        snapshot(kind, build(kind, database))
    assert not database.writes()


@pytest.mark.parametrize("kind", KINDS)
def test_startup_outage_never_becomes_an_explicit_offline_store(database, kind) -> None:
    database.down = True
    with pytest.raises(SqlUnavailable):
        snapshot(kind, build(kind, database))
    assert not database.writes()


@pytest.mark.parametrize("kind,method", [(kind, method) for kind in KINDS for method in READS[kind]])
def test_every_public_read_rechecks_shared_backend(database, kind, method) -> None:
    store = build(kind, database)
    identifier = seed(kind, store)
    before = read(kind, method, store, identifier)
    database.operations.clear()
    database.down = True
    with pytest.raises(SqlUnavailable):
        read(kind, method, store, identifier)
    database.down = False
    assert read(kind, method, store, identifier) == before
    assert not database.writes()


@pytest.mark.parametrize("kind", KINDS)
def test_two_instances_observe_new_rows_and_deletions_without_an_outage(database, kind) -> None:
    first = build(kind, database)
    other_db = OperationalSql(database.path)
    second = build(kind, other_db)
    empty = snapshot(kind, second)
    seed(kind, first)
    current = snapshot(kind, second)
    assert current != empty
    assert (current if kind == "processed" else len(current)) == 1
    first.reset()
    assert snapshot(kind, second) == empty
    assert not other_db.writes()


@pytest.mark.parametrize("kind", KINDS)
def test_recovery_reads_authoritative_rows_written_by_another_instance(database, kind) -> None:
    reader = build(kind, database)
    seed(kind, reader)
    database.down = True
    with pytest.raises(SqlUnavailable):
        snapshot(kind, reader)
    writer = build(kind, OperationalSql(database.path))
    writer.reset()
    seed(kind, writer, "two")
    expected = snapshot(kind, writer)
    database.down = False
    database.operations.clear()
    assert snapshot(kind, reader) == expected
    assert not database.writes()


@pytest.mark.parametrize("kind,method", [(kind, method) for kind in KINDS for method in READS[kind]])
def test_point_and_queue_reads_observe_another_instances_changes(database, kind, method) -> None:
    writer = build(kind, database)
    reader = build(kind, OperationalSql(database.path))
    identifier = seed(kind, writer)
    if kind == "approvals":
        writer.decide("one", decision="approve", responder="operator")
    assert read(kind, method, reader, identifier) == read(kind, method, writer, identifier)
    writer.reset()
    assert read(kind, method, reader, identifier) == read(kind, method, writer, identifier)


@pytest.mark.parametrize("after_commit", (False, True), ids=("rejected", "commit-uncertain"))
@pytest.mark.parametrize("kind", KINDS)
def test_new_records_cannot_report_success_from_an_unconfirmed_insert(database, kind, after_commit) -> None:
    store = build(kind, database)
    database.failure_prefix = "INSERT"
    database.fail_writes = not after_commit
    database.lose_write_ack = after_commit
    with pytest.raises(SqlUnavailable) as raised:
        seed(kind, store)
    assert raised.value is database.last_failure
    assert len(database.writes()) == (2 if kind == "processed" else 1)
    database.fail_writes = False
    database.operations.clear()
    current = snapshot(kind, store)
    assert (current if kind == "processed" else len(current)) == int(after_commit)
    assert not database.writes()


@pytest.mark.parametrize("after_commit", (False, True), ids=("rejected", "commit-uncertain"))
@pytest.mark.parametrize("kind,method", [(kind, method) for kind in KINDS for method in WRITES[kind]])
def test_writes_raise_without_retry_or_cached_success(database, kind, method, after_commit) -> None:
    store = build(kind, database)
    identifier = seed(kind, store)
    if kind == "health" and method in {"try_acquire_lease", "release_lease"}:
        assert store.try_acquire_lease("sweep", "owner", 60)
    if method == "consume_exact":
        store.decide_exact(
            identifier, decision="approve", responder="operator", fingerprint=proposal().fingerprint,
        )
    database.operations.clear()
    database.fail_writes = not after_commit
    database.lose_write_ack = after_commit
    with pytest.raises(SqlUnavailable) as raised:
        mutate(kind, method, store, identifier)
    assert raised.value is database.last_failure
    assert len(database.writes()) == 1
    database.fail_writes = False
    database.operations.clear()
    expected = snapshot(kind, build(kind, OperationalSql(database.path)))
    assert snapshot(kind, store) == expected
    assert not database.writes()


@pytest.mark.parametrize("kind,method", [(kind, method) for kind in KINDS for method in WRITES[kind]])
def test_unknown_rowcounts_do_not_report_write_success(database, kind, method) -> None:
    store = build(kind, database)
    identifier = seed(kind, store)
    if method == "consume_exact":
        store.decide_exact(
            identifier, decision="approve", responder="operator", fingerprint=proposal().fingerprint,
        )
    database.rowcount_override = -1
    database.operations.clear()
    with pytest.raises(SqlUnavailable, match="row count|row-count|affected-row|not confirmed"):
        mutate(kind, method, store, identifier)
    assert len(database.writes()) == 1


@pytest.mark.parametrize("kind", ("incidents", "retries", "health", "approvals"))
@pytest.mark.parametrize("corrupt", ("{", "null", "[]", "{}"))
def test_corrupt_payload_never_disappears_from_a_partially_read_queue(database, kind, corrupt) -> None:
    store = build(kind, database)
    seed(kind, store)
    seed(kind, store, "two")
    key = {"incidents": "incident_id", "retries": "signature", "health": "probe_key", "approvals": "request_id"}[kind]
    with database.connection() as conn:
        conn.execute(
            f"UPDATE {TABLES[kind]} SET payload = ? WHERE {key} = "
            f"(SELECT {key} FROM {TABLES[kind]} ORDER BY {key} DESC LIMIT 1)", (corrupt,),
        )
    # Approval priority is computed in SQL; malformed JSON can fail there
    # before Python decodes a row. That driver error must propagate unchanged.
    error = sqlite3.OperationalError if kind == "approvals" and corrupt == "{" else SqlUnavailable
    message = "malformed JSON" if error is sqlite3.OperationalError else "Unreadable"
    with pytest.raises(error, match=message):
        snapshot(kind, store)


@pytest.mark.parametrize("kind", ("processed", "audit"))
def test_corrupt_relational_row_never_looks_like_an_empty_store(database, kind) -> None:
    store = build(kind, database)
    seed(kind, store)
    with database.connection() as conn:
        conn.execute(f"UPDATE {TABLES[kind]} SET fingerprint = ''")
    with pytest.raises(SqlUnavailable, match="Unreadable"):
        snapshot(kind, store)


def test_incident_occurrences_notifications_and_source_survive_two_instances(database) -> None:
    first = AzureSqlIncidentStore(db=database)
    second = AzureSqlIncidentStore(db=OperationalSql(database.path))
    created = first.record(result(), source="fabric_pipeline_failure", notified=True)
    repeated = second.record(result(outcome="duplicate_suppressed"))
    assert repeated.id == created.id
    assert repeated.occurrence_count == 2
    assert repeated.notified_count == 1
    assert repeated.source == "fabric_pipeline_failure"
    first.mark(created.id, "investigating", "Still under review")
    latest = second.get(created.id)
    assert latest.occurrence_count == 2
    assert latest.notified_count == 1
    assert latest.status == "investigating"


def test_original_nvarchar_payload_hash_is_used_without_reserializing_defaults(database) -> None:
    store = AzureSqlIncidentStore(db=database)
    iid = seed("incidents", store)
    with database.connection() as conn:
        current = json.loads(conn.execute("SELECT payload FROM triage_incidents").fetchone()[0])
        current.pop("triage_notes")
        current["report_name"] = "Synthetic \u00e9vidence"
        raw = json.dumps(current, indent=3, ensure_ascii=False)
        conn.execute("UPDATE triage_incidents SET payload = ?", (raw,))
    database.operations.clear()
    store.mark(iid, "investigating")
    write = database.writes()[0]
    assert write[2][-1] == hashlib.sha256(raw.encode("utf-16-le")).digest()
    assert store.get(iid).report_name == "Synthetic \u00e9vidence"


@pytest.mark.parametrize("kind", ("incidents", "retries", "health"))
def test_concurrent_snapshot_update_is_refused_without_overwriting_shared_state(database, kind) -> None:
    first = build(kind, database)
    second = build(kind, OperationalSql(database.path))
    seed(kind, first)
    database.before_execute = (
        lambda: second.put(ProbeState("w", "d", "one", last_row_count=30, observations=3))
    ) if kind == "health" else lambda: seed(kind, second)
    with pytest.raises(SqlUnavailable, match="revision changed"):
        seed(kind, first)
    assert snapshot(kind, first) == snapshot(kind, second)


@pytest.mark.parametrize("kind", ("incidents", "retries", "health"))
def test_insert_race_cannot_overwrite_the_other_workers_record(database, kind) -> None:
    first = build(kind, database)
    second = build(kind, OperationalSql(database.path))
    database.before_execute = lambda: seed(kind, second)
    with pytest.raises(sqlite3.IntegrityError):
        seed(kind, first)
    assert len(database.writes()) == 1
    assert snapshot(kind, first) == snapshot(kind, second)


def test_retry_reads_and_mutations_preserve_shared_attempts_and_completion(database) -> None:
    first = AzureSqlRetryStore(db=database)
    second = AzureSqlRetryStore(db=OperationalSql(database.path))
    seed("retries", first)
    assert second.is_deferred("one")
    assert second.defer(signature="one")["attempts"] == 2
    assert first.defer(signature="one")["attempts"] == 3
    assert second.defer(signature="one")["status"] == "exhausted"
    assert not first.is_deferred("one")
    second.complete("one", outcome="needs_human")
    assert first.get("one")["status"] == "done"
    assert first.get("one")["attempts"] == 3
    assert first.due(now=NOW + timedelta(days=1)) == []


@pytest.mark.parametrize("field,value", (
    ("due_at", "not a timestamp"), ("due_at", "2026-09-15T12:00:00"),
    ("status", "unknown"), ("attempts", -1), ("signature", "other"),
))
def test_invalid_live_retry_cannot_be_treated_as_due_or_missing(database, field, value) -> None:
    store = AzureSqlRetryStore(db=database)
    seed("retries", store)
    with database.connection() as conn:
        payload = json.loads(conn.execute("SELECT payload FROM triage_deferred_retries").fetchone()[0])
        payload[field] = value
        conn.execute("UPDATE triage_deferred_retries SET payload = ?", (json.dumps(payload),))
    for lookup in (store.pending, store.due, lambda: store.is_deferred("one")):
        with pytest.raises(SqlUnavailable, match="Unreadable retry state"):
            lookup()


def test_probe_reads_pick_up_shared_baselines_and_circuit_changes(database) -> None:
    first = AzureSqlSemanticHealthStore(db=database)
    second = AzureSqlSemanticHealthStore(db=OperationalSql(database.path))
    seed("health", first)
    updated = second.get("w", "d", "one")
    updated.last_row_count = 20
    updated.consecutive_errors = 3
    updated.circuit_opened_at = NOW.isoformat()
    second.put(updated)
    current = first.get("w", "d", "one")
    assert current.last_row_count == 20
    assert current.consecutive_errors == 3
    assert current.circuit_opened_at == NOW.isoformat()


@pytest.mark.parametrize("field,value", (
    ("suspect_count", "two"), ("consecutive_errors", -1), ("workspace_id", "other"),
    ("last_row_count", -1), ("last_control_totals", {"sales": "not a number"}),
    ("updated_at", ""), ("future_field", "mixed version"),
))
def test_invalid_live_baseline_is_not_a_healthy_or_missing_probe(database, field, value) -> None:
    store = AzureSqlSemanticHealthStore(db=database)
    seed("health", store)
    with database.connection() as conn:
        payload = json.loads(conn.execute("SELECT payload FROM triage_semantic_health").fetchone()[0])
        payload[field] = value
        conn.execute("UPDATE triage_semantic_health SET payload = ?", (json.dumps(payload),))
    with pytest.raises(SqlUnavailable, match="Unreadable probe state"):
        store.get("w", "d", "one")
    with pytest.raises(SqlUnavailable, match="Unreadable probe state"):
        store.all_states()


def test_sweep_leases_arbitrate_in_sql_and_old_owners_cannot_release(database) -> None:
    first = AzureSqlSemanticHealthStore(db=database)
    other_db = OperationalSql(database.path)
    second = AzureSqlSemanticHealthStore(db=other_db)
    assert first.try_acquire_lease("sweep", "first", 60)
    assert not second.try_acquire_lease("sweep", "second", 60)
    second.release_lease("sweep", "second")
    assert not second.try_acquire_lease("sweep", "second", 60)
    with database.connection() as conn:
        conn.execute("UPDATE triage_sweep_leases SET expires_at = ?", ((NOW - timedelta(seconds=1)).isoformat(),))
    assert second.try_acquire_lease("sweep", "second", 60)
    first.release_lease("sweep", "first")
    assert not first.try_acquire_lease("sweep", "first", 60)
    assert all("SYSUTCDATETIME()" in sql for _, sql, _ in other_db.writes() if sql.startswith("UPDATE"))


@pytest.mark.parametrize("after_commit", (False, True))
def test_audit_pruning_failure_is_visible_and_recovery_does_not_replay_it(database, after_commit) -> None:
    store = AzureSqlInboxAudit(db=database, max_rows=1)
    seed("audit", store)
    database.failure_prefix = "DELETE"
    database.fail_writes = not after_commit
    database.lose_write_ack = after_commit
    with pytest.raises(SqlUnavailable):
        seed("audit", store, "two")
    database.fail_writes = False
    database.operations.clear()
    expected = AzureSqlInboxAudit(db=OperationalSql(database.path)).recent()
    assert store.recent() == expected
    assert not database.writes()


def test_redaction_remains_inside_incident_audit_and_approval_stores(database) -> None:
    secret = "AKIAIOSFODNN7EXAMPLE"
    incidents = AzureSqlIncidentStore(db=database)
    incidents.record(result(root_cause=secret), original_error=secret)
    incidents.mark(incidents.list_all()[0].id, "investigating", secret)
    audit = AzureSqlInboxAudit(db=database)
    audit.record(message_id="one", sender=secret, subject=secret, reason=secret)
    approvals = AzureSqlApprovalChannel(db=database)
    request = proposal(arguments={"value": secret}, justification=secret, report_name=secret, impact=secret)
    approvals.open(request)
    approvals.decide("one", decision="decline", responder="operator", reason=secret)
    with database.connection() as conn:
        for table in ("triage_incidents", "triage_inbox_audit", "triage_approvals"):
            assert secret not in str(conn.execute(f"SELECT * FROM {table}").fetchall())


@pytest.mark.parametrize("method", ("open", "open_exact"))
def test_reopening_an_approval_cannot_renew_erase_or_unconsume_it(database, method) -> None:
    first = AzureSqlApprovalChannel(db=database)
    second = AzureSqlApprovalChannel(db=OperationalSql(database.path))
    request = proposal()
    getattr(first, method)(request)
    before = first.get("one")
    getattr(second, method)(proposal(timeout_seconds=3600))
    assert first.get("one") == before
    first.decide("one", decision="approve", responder="operator")
    answered = second.get("one")
    with pytest.raises(ValueError, match="answered"):
        getattr(second, method)(request)
    assert first.get("one") == answered
    assert second.consume_exact("one", request.fingerprint)
    assert not first.consume_exact("one", request.fingerprint)
    with pytest.raises(ValueError, match="consumed"):
        getattr(first, method)(request)
    assert asyncio.run(second.poll("one")) is None


@pytest.mark.parametrize("field,value", (
    ("fingerprint", "other"), ("expires_at", (NOW - timedelta(seconds=1)).isoformat()),
    ("consumed_at", NOW.isoformat()), ("decision", "decline"),
))
def test_sql_approval_guard_rechecks_changes_after_the_python_read(database, field, value) -> None:
    store = AzureSqlApprovalChannel(db=database)
    store.open(proposal())

    def change():
        with database.connection() as conn:
            row = json.loads(conn.execute("SELECT payload FROM triage_approvals").fetchone()[0])
            row[field] = value
            conn.execute(
                "UPDATE triage_approvals SET decision = ?, payload = ?",
                (row["decision"] or None, json.dumps(row)),
            )

    database.before_execute = change
    with pytest.raises(ValueError, match="expired, changed or was answered"):
        store.decide_exact("one", decision="approve", responder="operator", fingerprint=proposal().fingerprint)


@pytest.mark.parametrize("method", READS["approvals"])
def test_unreadable_approval_is_not_a_missing_record_or_healthy_empty_queue(database, method) -> None:
    channel = AzureSqlApprovalChannel(db=database)
    channel.open(proposal())
    with database.connection() as conn:
        conn.execute("UPDATE triage_approvals SET payload = '{'")
    error = sqlite3.OperationalError if method == "list_requests" else SqlUnavailable
    message = "malformed JSON" if method == "list_requests" else "Unreadable approval state"
    with pytest.raises(error, match=message):
        read("approvals", method, channel, "one")


def test_denial_cannot_be_consumed_and_does_not_reopen_by_polling(database) -> None:
    channel = AzureSqlApprovalChannel(db=database)
    channel.open(proposal())
    channel.decide("one", decision="decline", responder="operator")
    assert not channel.consume_exact("one", proposal().fingerprint)
    assert asyncio.run(channel.poll("one"))["decision"] == "decline"
    assert not channel.get("one")["consumed_at"]


def test_unavailable_approval_registration_never_grants_an_action(database) -> None:
    channel = AzureSqlApprovalChannel(db=database)
    database.down = True
    decision = asyncio.run(WebApprovalGate(channel).request_approval(proposal()))
    assert not decision.granted
    assert decision.outcome == "error"
