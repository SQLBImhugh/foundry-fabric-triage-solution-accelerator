from __future__ import annotations

import hashlib
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

from triage.models import Incident
from triage.store.fabric_sql import SqlUnavailable
from triage.store.incident_workflow import (
    FabricSqlIncidentWorkflowStore,
    IncidentQuery,
    InMemoryIncidentWorkflowStore,
    WorkflowConflict,
    schema_statements,
    source_revision,
)
from triage.store.incidents import InMemoryIncidentStore

NOW = datetime(2026, 9, 14, 12, tzinfo=UTC)
ACTOR = {"user_id": "operator-1", "user_name": "Test operator"}


def incident(**updates) -> Incident:
    return Incident.model_validate({
        "id": "incident-1", "signature": "signature-1", "outcome": "needs_human",
        "status": "open", "requires_investigation": True, "report_name": "Synthetic report",
        "first_seen_at": NOW.isoformat(), "last_seen_at": NOW.isoformat(),
        "original_error": "Source access failed", "diagnosed_root_cause": "Credentials need review",
        "action_applied": "refresh_powerbi_dataset", "occurrence_count": 2,
    } | updates)


class WorkflowSql:
    """Execute the real DML with local constraints, concurrency and row counts.

    Adapt only SQL Server syntax/functions. The fake does not decide whether
    a resolution wins: the conditional INSERT and unique indexes do that.
    """

    def __init__(
        self, path: Path, *, activity_table: str = "triage_incident_activity",
        incident_table: str = "triage_incidents",
    ) -> None:
        self.path = path
        self.activity_table = activity_table
        self.incident_table = incident_table
        self.down = False
        self.fail_writes = False
        self.lose_write_ack = False
        self.before_insert = None
        self.operations: list[tuple[str, str, tuple]] = []
        self._lock = threading.Lock()
        with self._connection() as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS [{incident_table}] "
                "(incident_id TEXT PRIMARY KEY, signature TEXT NOT NULL, status TEXT NOT NULL, "
                "updated_at TEXT NOT NULL, payload TEXT NOT NULL)",
            )
            for statement in schema_statements(activity_table):
                start = statement.index("CREATE ")
                sql = statement[start:].replace(
                    "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1,
                ).replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
                sql = re.sub(r",\s*INDEX\s+\w+\s+\([^\n]*\)", "", sql)
                sql = re.sub(r"NVARCHAR\((?:MAX|\d+)\)|CHAR\(\d+\)|DATETIME2\(\d+\)", "TEXT", sql)
                sql = sql.replace("Latin1_General_100_BIN2", "BINARY")
                conn.execute(sql.replace("[dbo].", ""))

    @staticmethod
    def _json_value(payload: str, path: str):
        value = json.loads(payload).get(path[2:])
        if isinstance(value, bool):
            return str(value).lower()
        return None if isinstance(value, str) and len(value) > 4000 else value

    def _connection(self):
        conn = sqlite3.connect(self.path, timeout=20)
        conn.create_function(
            "SOURCE_REVISION", 1,
            lambda payload: hashlib.sha256(payload.encode("utf-16-le")).hexdigest(),
        )
        conn.create_function("JSON_VALUE", 2, self._json_value)
        return conn

    @staticmethod
    def _translate(sql: str) -> str:
        sql = sql.replace("[dbo].", "").replace("COUNT_BIG(", "COUNT(")
        sql = sql.replace("Latin1_General_100_BIN2", "BINARY")
        sql = re.sub(r" WITH \((?:UPDLOCK, )?HOLDLOCK\)", "", sql)
        sql = sql.replace(
            "LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', i.payload), 2))",
            "SOURCE_REVISION(i.payload)",
        ).replace("OPENJSON(i.payload)", "json_each(i.payload)")
        return re.sub(r"OFFSET (\d+) ROWS FETCH NEXT (\d+) ROWS ONLY", r"LIMIT \2 OFFSET \1", sql)

    @staticmethod
    def _params(params: tuple) -> tuple:
        return tuple(
            value.isoformat(timespec="microseconds") if isinstance(value, datetime) else value
            for value in params
        )

    def execute(self, sql: str, *params) -> int:
        with self._lock:
            self.operations.append(("execute", sql, params))
        if self.down or self.fail_writes:
            raise ConnectionError("Offline write failure")
        if self.before_insert is not None:
            callback, self.before_insert = self.before_insert, None
            callback()
        with self._connection() as conn:
            changed = conn.execute(self._translate(sql), self._params(params)).rowcount
        if self.lose_write_ack:
            self.lose_write_ack = False
            raise ConnectionError("Offline acknowledgement lost")
        return changed

    def query(self, sql: str, *params) -> list[tuple]:
        with self._lock:
            self.operations.append(("query", sql, params))
        if self.down:
            raise ConnectionError("Offline read failure")
        with self._connection() as conn:
            return conn.execute(self._translate(sql), self._params(params)).fetchall()

    def seed(self, value: Incident, payload: str | None = None) -> None:
        with self._connection() as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO [{self.incident_table}] VALUES (?, ?, ?, ?, ?)",
                (value.id, value.signature, value.status, value.last_seen_at,
                 payload if payload is not None else value.model_dump_json()),
            )

    def ensure_schema_once(self):
        raise AssertionError("Runtime identities cannot install schema")

    @staticmethod
    def integrity_error():
        return sqlite3.IntegrityError


class Harness:
    def __init__(self, mode: str, path: Path) -> None:
        self.core = InMemoryIncidentStore()
        self.db = WorkflowSql(path) if mode == "sql" else None
        self.store = (
            FabricSqlIncidentWorkflowStore(self.db) if self.db else InMemoryIncidentWorkflowStore(self.core)
        )
        self.put(incident())

    def put(self, value: Incident) -> None:
        if self.db:
            self.db.seed(value)
        else:
            with self.core._lock:
                self.core._items[value.id] = value.model_copy(deep=True)

    def resolve(self, *, key: str | None = None, **updates):
        state = self.store.state("incident-1")
        arguments = {
            "expected_version": state.tracking.version,
            "source_revision": state.tracking.source_revision,
            "idempotency_key": key or str(uuid4()), **ACTOR,
        } | updates
        return self.store.resolve("incident-1", "Source repaired by the operator", **arguments)


@pytest.fixture(params=["memory", "sql"])
def harness(request, tmp_path) -> Harness:
    return Harness(request.param, tmp_path / "workflow.db")


def test_notes_append_without_changing_source_or_tracking_version(harness) -> None:
    before = harness.store.source("incident-1")
    key = str(uuid4())
    row = harness.store.add_note("incident-1", "Investigating source access", idempotency_key=key, **ACTOR)
    state = harness.store.state("incident-1")
    assert state.source == before
    assert state.tracking.version == 0
    assert state.tracking.status == "open"
    assert state.activity == [row]
    assert row.user_id == ACTOR["user_id"]
    assert row.kind == "note" and row.status == "recorded"
    state.activity[0].body = "Changed local copy"
    state.source.incident.status = "resolved"
    assert harness.store.state("incident-1").activity == [row]
    assert harness.store.source("incident-1") == before


def test_mutation_activity_ids_are_receipts_even_after_redaction(harness) -> None:
    store = harness.store
    secret = "AKIA" + "IOSFODNN7EXAMPLE"
    note_key, question_key, resolution_key = (str(uuid4()) for _ in range(3))
    store.add_note("incident-1", f"Note {secret}", idempotency_key=note_key, **ACTOR)
    store.reserve_question("incident-1", f"Question {secret}", idempotency_key=question_key, **ACTOR)
    harness.resolve(key=resolution_key)
    reloaded = FabricSqlIncidentWorkflowStore(harness.db) if harness.db else store
    entries = {entry.id: entry for entry in reloaded.state("incident-1").activity}
    assert set(entries) == {note_key, question_key, resolution_key}
    for key, kind in ((note_key, "note"), (question_key, "question"), (resolution_key, "resolution")):
        assert entries[key].kind == kind and entries[key].incident_id == "incident-1"
        assert entries[key].correlation_id == (question_key if kind == "question" else None)
    assert secret not in entries[note_key].body
    assert secret not in entries[question_key].body


def test_answer_content_is_not_lost_to_leading_whitespace_before_truncation(harness) -> None:
    reserved = harness.store.reserve_question(
        "incident-1", "What happened?", idempotency_key=str(uuid4()), **ACTOR,
    )
    answer = harness.store.finish_question(
        reserved.question.id, " \n" * 2500 + "Recorded source failure.\n ", mode="records",
    )
    assert answer.body == "Recorded source failure."
    state = harness.store.state("incident-1")
    assert next(row for row in state.activity if row.kind == "question").status == "completed"
    assert next(row for row in state.activity if row.kind == "answer").body.strip()


def test_idempotency_is_bound_to_body_actor_incident_and_kind(harness) -> None:
    store = harness.store
    key = str(uuid4())
    saved = store.add_note("incident-1", "Investigating", idempotency_key=key, **ACTOR)
    assert store.add_note(
        "incident-1", "Investigating", idempotency_key=key,
        **(ACTOR | {"user_name": "Renamed operator"}),
    ) == saved
    for body, actor in (("Different", ACTOR), ("Investigating", ACTOR | {"user_id": "other-user"})):
        with pytest.raises(WorkflowConflict, match="different request"):
            store.add_note("incident-1", body, idempotency_key=key, **actor)
    harness.put(incident(id="incident-2"))
    with pytest.raises(WorkflowConflict):
        store.add_note("incident-2", "Investigating", idempotency_key=key, **ACTOR)
    with pytest.raises(WorkflowConflict):
        store.reserve_question("incident-1", "Investigating", idempotency_key=key, **ACTOR)
    assert len(store.state("incident-1").activity) == 1
    assert store.state("incident-2").activity == []


def test_store_redacts_notes_resolution_questions_answers_and_display_names(harness) -> None:
    secret = "AKIA" + "IOSFODNN7EXAMPLE"
    author = ACTOR | {"user_name": f"Operator {secret}"}
    store = harness.store
    key = str(uuid4())
    store.add_note("incident-1", f"Note {secret}", idempotency_key=key, **author)
    question = store.reserve_question(
        "incident-1", f"Question {secret}", idempotency_key=str(uuid4()), **author,
    )
    store.finish_question(question.question.id, f"Answer {secret}", mode="records")
    state = store.state("incident-1")
    store.resolve(
        "incident-1", f"Resolution {secret}", expected_version=0,
        source_revision=state.source.revision, idempotency_key=str(uuid4()), **author,
    )
    state = store.state("incident-1")
    assert secret not in state.model_dump_json()
    assert all("[REDACTED:aws_access_key]" in row.body for row in state.activity)
    assert "[REDACTED:aws_access_key]" in state.tracking.resolution_note
    if harness.db:
        raw = harness.db.query("SELECT payload FROM [dbo].[triage_incident_activity]")
        assert secret not in json.dumps(raw)
    other_secret = "ASIA" + "IOSFODNN7EXAMPLE"
    with pytest.raises(WorkflowConflict):
        store.add_note("incident-1", f"Note {other_secret}", idempotency_key=key, **author)


def test_manual_resolution_is_source_bound_and_does_not_modify_core(harness) -> None:
    before = harness.store.source("incident-1")
    key = str(uuid4())
    row = harness.resolve(key=key)
    state = harness.store.state("incident-1")
    assert state.source == before
    assert state.tracking.status == "resolved_by_user"
    assert state.tracking.version == 1
    assert state.tracking.resolved_by == ACTOR["user_name"]
    assert state.tracking.resolved_at == row.created_at
    assert harness.store.resolve(
        "incident-1", "Source repaired by the operator", expected_version=0,
        source_revision=before.revision, idempotency_key=key, **ACTOR,
    ) == row
    with pytest.raises(WorkflowConflict, match="already closed"):
        harness.resolve()
    harness.store.add_note("incident-1", "Follow-up", idempotency_key=str(uuid4()), **ACTOR)
    assert harness.store.state("incident-1").tracking.version == 1
    assert len(harness.store.state("incident-1").activity) == 2


@pytest.mark.parametrize("updates", [
    {"occurrence_count": 3},
    {"last_seen_at": (NOW + timedelta(minutes=1)).isoformat()},
    {"original_error": "New diagnostic evidence"},
    {"triage_notes": "Updated automated evidence"},
])
def test_changed_source_invalidates_a_closure_and_retains_the_audit(harness, updates) -> None:
    harness.resolve()
    harness.put(incident(**updates))
    state = harness.store.state("incident-1")
    assert state.tracking.status == "open"
    assert state.tracking.version == 1
    assert state.tracking.resolved_at is None
    assert state.tracking.resolution_note is None
    assert [row.kind for row in state.activity] == ["resolution"]
    harness.resolve()
    state = harness.store.state("incident-1")
    assert state.tracking.status == "resolved_by_user" and state.tracking.version == 2


def test_verified_resolution_is_not_reopened_by_old_human_closure(harness) -> None:
    harness.resolve()
    harness.put(incident(status="resolved", outcome="resolved", occurrence_count=3))
    state = harness.store.state("incident-1")
    assert state.tracking.status == "resolved"
    assert state.tracking.resolution_note is None
    assert state.source.incident.outcome == "resolved"
    with pytest.raises(WorkflowConflict, match="already closed"):
        harness.resolve()


@pytest.mark.parametrize("changes", [
    {"expected_version": 1},
    {"source_revision": "0" * 64},
])
def test_stale_version_or_source_is_refused_without_a_resolution(harness, changes) -> None:
    with pytest.raises(WorkflowConflict, match="changed"):
        harness.resolve(**changes)
    assert harness.store.state("incident-1").activity == []


@pytest.mark.parametrize("bad_version", [-1, True, 0.0, 9_007_199_254_740_991])
def test_invalid_tracking_versions_are_rejected(harness, bad_version) -> None:
    with pytest.raises(ValueError):
        harness.resolve(expected_version=bad_version)
    assert harness.store.state("incident-1").activity == []


def test_concurrent_resolutions_have_one_winner(harness) -> None:
    state = harness.store.state("incident-1")
    barrier = threading.Barrier(2)

    def resolve():
        store = FabricSqlIncidentWorkflowStore(harness.db) if harness.db else harness.store
        barrier.wait()
        try:
            store.resolve(
                "incident-1", "Operator verified externally", expected_version=0,
                source_revision=state.source.revision, idempotency_key=str(uuid4()), **ACTOR,
            )
            return "won"
        except WorkflowConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: resolve(), range(2)))
    assert sorted(outcomes) == ["conflict", "won"]
    assert harness.store.state("incident-1").tracking.version == 1
    assert len(harness.store.state("incident-1").activity) == 1


def test_concurrent_question_reservations_acquire_once(harness) -> None:
    key = str(uuid4())
    barrier = threading.Barrier(2)

    def reserve():
        store = FabricSqlIncidentWorkflowStore(harness.db) if harness.db else harness.store
        barrier.wait()
        return store.reserve_question("incident-1", "What happened?", idempotency_key=key, **ACTOR).acquired

    with ThreadPoolExecutor(max_workers=2) as pool:
        acquired = list(pool.map(lambda _: reserve(), range(2)))
    assert sum(acquired) == 1
    assert len(harness.store.state("incident-1").activity) == 1


@pytest.mark.parametrize("failed", [False, True])
def test_question_completion_is_append_only_and_cannot_be_overwritten(harness, failed) -> None:
    store = harness.store
    reservation = store.reserve_question(
        "incident-1", "Why did this fail?", idempotency_key=str(uuid4()), **ACTOR,
    )
    assert reservation.acquired
    before = store.state("incident-1")
    assert before.activity[0].status == "pending"
    answer = store.finish_question(
        reservation.question.id, "Failure is recorded" if failed else "Recorded explanation",
        failed=failed, mode=None if failed else "records",
    )
    state = store.state("incident-1")
    question = next(row for row in state.activity if row.kind == "question")
    assert question.status == answer.status == ("failed" if failed else "completed")
    assert question.correlation_id == answer.correlation_id == reservation.question.id
    assert answer.user_id == "incident-observer"
    assert store._entry(question.id).status == "pending"
    with pytest.raises(WorkflowConflict):
        store.finish_question(question.id, "A contradictory answer", mode="model")
    assert len(store.state("incident-1").activity) == 2
    with pytest.raises(ValueError, match="unknown question"):
        store.finish_question(str(uuid4()), "Unknown", mode="records")


def test_paging_search_and_counts_include_incidents_beyond_the_old_200_row_limit(harness) -> None:
    for index in range(1, 216):
        harness.put(incident(
            id=f"batch-{index:03d}", signature=f"signature-{index}",
            last_seen_at=(NOW + timedelta(minutes=index)).isoformat(),
            report_name=f"Report {index:03d}", source="fabric_pipeline_schedule" if index % 2 else "powerbi",
        ))
    harness.resolve()
    page = harness.store.page(IncidentQuery(limit=5, offset=210, status="open"))
    assert page.total == 215 and len(page.items) == 5
    assert page.offset == 210 and page.limit == 5
    assert [row.source.incident.id for row in page.items] == [f"batch-{i:03d}" for i in range(5, 0, -1)]
    found = harness.store.page(IncidentQuery(query="SYNTHETIC", status="resolved"))
    assert found.total == 1 and found.items[0].tracking.status == "resolved_by_user"
    assert harness.store.page(IncidentQuery(workload="fabric_pipeline")).total == 108
    assert harness.store.page(IncidentQuery(status="needs_review")).total == 215
    assert harness.store.page(IncidentQuery(status="needs_investigation")).total == 215
    assert harness.store.page(IncidentQuery(offset=1000)).items == []
    assert harness.store.counts() == {"needs_investigation": 215, "resolved": 1, "resolved_by_user": 1}


@pytest.mark.parametrize("query", ["100%", "a_b", "[x]", "\\path", "O'Brien"])
def test_search_treats_metacharacters_as_literal_text(harness, query) -> None:
    harness.put(incident(report_name=f"Literal {query} report"))
    harness.put(incident(id="other", report_name="An unrelated report"))
    page = harness.store.page(IncidentQuery(query=query))
    assert [row.source.incident.id for row in page.items] == ["incident-1"]


def test_search_finds_long_scalar_evidence(harness) -> None:
    harness.put(incident(original_error="x" * 4100 + " Long-evidence-marker"))
    assert harness.store.page(IncidentQuery(query="Long-evidence-marker")).total == 1


@pytest.mark.parametrize("bad_id", ["", " ", "../incident", "incident/1", "x" * 201, "test\n"])
def test_ids_are_validated_without_querying_or_writing(harness, bad_id) -> None:
    with pytest.raises(ValueError):
        harness.store.source(bad_id)
    with pytest.raises(ValueError):
        harness.store.add_note(bad_id, "Text", idempotency_key=str(uuid4()), **ACTOR)


def test_missing_incidents_and_invalid_idempotency_keys_are_not_saved(harness) -> None:
    with pytest.raises(KeyError):
        harness.store.add_note("missing", "Note", idempotency_key=str(uuid4()), **ACTOR)
    with pytest.raises(ValueError):
        harness.store.add_note("incident-1", "Note", idempotency_key="not-a-uuid", **ACTOR)
    with pytest.raises(ValueError):
        harness.store.add_note("incident-1", "  ", idempotency_key=str(uuid4()), **ACTOR)
    assert harness.store.state("incident-1").activity == []


def test_sql_restart_readback_preserves_notes_closure_and_pending_questions(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "restart.db")
    harness.store.add_note("incident-1", "Durable note", idempotency_key=str(uuid4()), **ACTOR)
    key = str(uuid4())
    harness.store.reserve_question("incident-1", "Pending question", idempotency_key=key, **ACTOR)
    harness.resolve()
    before = harness.store.state("incident-1")
    restarted = FabricSqlIncidentWorkflowStore(WorkflowSql(harness.db.path))
    assert restarted.state("incident-1") == before
    assert not restarted.reserve_question(
        "incident-1", "Pending question", idempotency_key=key, **ACTOR,
    ).acquired
    assert restarted.is_durable
    assert not InMemoryIncidentWorkflowStore(InMemoryIncidentStore()).is_durable


def test_sql_preserves_actual_payload_revision_not_reserialized_defaults(tmp_path) -> None:
    db = WorkflowSql(tmp_path / "raw.db")
    value = incident()
    raw = json.dumps(value.model_dump(mode="json"), indent=2)
    db.seed(value, raw)
    store = FabricSqlIncidentWorkflowStore(db)
    state = store.state(value.id)
    assert state.source.revision == hashlib.sha256(raw.encode("utf-16-le")).hexdigest()
    assert state.source.revision != source_revision(value)
    store.resolve(
        value.id, "Manual closure", expected_version=0, source_revision=state.source.revision,
        idempotency_key=str(uuid4()), **ACTOR,
    )
    assert store.page(IncidentQuery(status="resolved_by_user")).total == 1
    db.seed(value)
    assert store.state(value.id).tracking.status == "open"
    assert store.page(IncidentQuery(status="resolved")).total == 0


def test_sql_filters_follow_authoritative_payload_not_promoted_status(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "promoted-status.db")
    with harness.db._connection() as conn:
        conn.execute("UPDATE triage_incidents SET status = 'resolved'")
    assert harness.store.page(IncidentQuery(status="open")).total == 1
    assert harness.store.page(IncidentQuery(status="resolved")).total == 0
    assert harness.store.counts()["resolved"] == 0
    harness.resolve()
    assert harness.store.page(IncidentQuery(status="resolved_by_user")).total == 1


def test_sql_readback_rejects_corrupt_activity_instead_of_hiding_it(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "corrupt.db")
    saved = harness.resolve()
    with harness.db._connection() as conn:
        payload = json.loads(conn.execute(
            "SELECT payload FROM triage_incident_activity WHERE activity_id = ?", (saved.id,),
        ).fetchone()[0])
        payload["tracking_version"] = None
        conn.execute(
            "UPDATE triage_incident_activity SET payload = ? WHERE activity_id = ?",
            (json.dumps(payload), saved.id),
        )
    with pytest.raises(ValidationError):
        harness.store.state("incident-1")
    with pytest.raises(ValidationError):
        harness.store.page(IncidentQuery())


def test_sql_cross_table_joins_explicitly_match_binary_identifier_collations(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "collation.db")
    assert harness.store.page(IncidentQuery()).total == 1
    joins = [sql for _, sql, _ in harness.db.operations if "LEFT JOIN" in sql]
    assert joins
    assert all(
        "r.incident_id = i.incident_id COLLATE Latin1_General_100_BIN2" in sql
        and "v.incident_id = i.incident_id COLLATE Latin1_General_100_BIN2" in sql
        for sql in joins
    )


def test_sql_resolution_guard_detects_source_change_after_the_python_check(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "source-race.db")
    harness.db.before_insert = lambda: harness.put(incident(occurrence_count=3))
    with pytest.raises(WorkflowConflict):
        harness.resolve()
    assert harness.store.state("incident-1").activity == []


def test_sql_resolution_guard_detects_version_change_after_the_python_check(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "version-race.db")
    harness.db.before_insert = lambda: harness.resolve()
    with pytest.raises(WorkflowConflict):
        harness.resolve()
    assert len(harness.store.state("incident-1").activity) == 1


def test_sql_outage_and_lost_acknowledgement_never_report_local_success(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "outage.db")
    store, db = harness.store, harness.db
    db.down = True
    with pytest.raises(SqlUnavailable):
        store.state("incident-1")
    with pytest.raises(SqlUnavailable):
        store.page(IncidentQuery())
    db.down = False
    db.fail_writes = True
    with pytest.raises(SqlUnavailable):
        store.add_note("incident-1", "Not saved", idempotency_key=str(uuid4()), **ACTOR)
    db.fail_writes = False
    assert store.state("incident-1").activity == []
    key = str(uuid4())
    db.lose_write_ack = True
    before = len([op for op in db.operations if op[0] == "execute"])
    with pytest.raises(SqlUnavailable, match="not confirmed"):
        store.reserve_question("incident-1", "Uncertain", idempotency_key=key, **ACTOR)
    assert len([op for op in db.operations if op[0] == "execute"]) == before + 1
    restarted = FabricSqlIncidentWorkflowStore(db)
    reservation = restarted.reserve_question("incident-1", "Uncertain", idempotency_key=key, **ACTOR)
    assert not reservation.acquired
    assert restarted.state("incident-1").activity[0].status == "pending"


def test_sql_writes_are_append_only_and_never_target_automated_state(tmp_path) -> None:
    harness = Harness("sql", tmp_path / "permissions.db")
    harness.resolve()
    question = harness.store.reserve_question(
        "incident-1", "Question", idempotency_key=str(uuid4()), **ACTOR,
    )
    harness.store.finish_question(question.question.id, "Answer", mode="records")
    writes = [sql for kind, sql, _ in harness.db.operations if kind == "execute"]
    assert len(writes) == 3
    assert all(sql.startswith("INSERT INTO [dbo].[triage_incident_activity]") for sql in writes)
    assert all("UPDATE " not in sql and "DELETE " not in sql for sql in writes)
    assert "MAX(tracking_version)" in writes[0] and "HASHBYTES('SHA2_256'" in writes[0]


def test_schema_and_stores_support_configured_names_and_reject_unsafe_tables(tmp_path) -> None:
    db = WorkflowSql(tmp_path / "custom.db", activity_table="case_activity", incident_table="core_incidents")
    db.seed(incident())
    store = FabricSqlIncidentWorkflowStore(db, activity_table="case_activity", incident_table="core_incidents")
    store.add_note("incident-1", "Configured table", idempotency_key=str(uuid4()), **ACTOR)
    assert len(store.state("incident-1").activity) == 1
    with pytest.raises(ValueError):
        schema_statements("unsafe]; DROP TABLE incidents")
    with pytest.raises(ValueError, match="distinct"):
        FabricSqlIncidentWorkflowStore(db, activity_table="triage_incidents")


@pytest.mark.parametrize("fields", [
    {"limit": 0}, {"limit": 101}, {"limit": True}, {"offset": -1},
    {"query": "x" * 201}, {"status": "invented"}, {"workload": "invented"},
])
def test_query_contract_is_bounded_and_explicit(fields) -> None:
    with pytest.raises(ValidationError):
        IncidentQuery.model_validate(fields)
