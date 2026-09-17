"""The Azure SQL store layer, tested without a database.

Rule 1 of this repository is that tests never touch the network, so nothing
here connects to anything. What is worth testing offline is the part that was
actually wrong in production:

* a store that lost its database must recover when the database comes back,
* a claim must be granted to exactly one caller,
* a table name from configuration must not be able to carry SQL with it.

The connection itself is covered by the live deployment check, not here. A test
that needs a tenant is not a test.
"""

from __future__ import annotations

import json

import pytest

from triage.approvals import ApprovalRequest
from triage.models import TriageResult
from triage.store.approvals import AzureSqlApprovalChannel, InMemoryApprovalChannel
from triage.store.azure_sql import (
    DEFAULT_TABLES,
    AzureSqlDatabase,
    SqlUnavailable,
    ensure_schema,
    quote_identifier,
    schema_statements,
)
from triage.store.claims import AzureSqlClaimStore, build_claim_store
from triage.store.sql_incidents import AzureSqlIncidentStore


class FakeIntegrityError(Exception):
    """Stands in for the driver's duplicate-key error."""


class FakeSql:
    """A database that can be told to be down, and what to return when up."""

    def __init__(self, *, rows: list[tuple] | None = None, down: bool = False) -> None:
        self.rows = rows or []
        self.down = down
        self.executed: list[tuple[str, tuple]] = []
        self.queried: list[str] = []
        #: rowcount the next execute() should report
        self.affected = 1
        #: raise a duplicate-key error on the next execute()
        self.raise_duplicate = False
        #: fail writes while still serving reads, which is how a write that
        #: nobody recorded gets mistaken for a decision
        self.fail_writes = False
        self.schema_calls = 0
        self.target = "fake"

    @property
    def is_available(self) -> bool:
        return not self.down

    def ensure_schema_once(self) -> bool:
        self.schema_calls += 1
        if self.down:
            raise SqlUnavailable("fake database is down")
        return True

    def _guard(self) -> None:
        if self.down:
            raise SqlUnavailable("fake database is down")

    def query(self, sql: str, *params):
        self._guard()
        self.queried.append(sql)
        return list(self.rows)

    def execute(self, sql: str, *params) -> int:
        self._guard()
        if self.fail_writes:
            raise SqlUnavailable("fake write failure")
        if self.raise_duplicate:
            self.raise_duplicate = False
            raise FakeIntegrityError("duplicate key")
        self.executed.append((sql, params))
        return self.affected

    def integrity_error(self):
        return FakeIntegrityError


def test_sql_cursors_are_closed_before_reusing_a_connection(monkeypatch) -> None:
    """A procedure's remaining result handles must not block the next query."""
    class Connection:
        busy = False
        closed = 0

        def cursor(self):
            connection = self

            class Cursor:
                rowcount = 1

                def execute(self, *_args):
                    if connection.busy:
                        raise RuntimeError("Connection is busy with results for another command")
                    connection.busy = True

                def fetchall(self):
                    return [(1,)]

                def close(self):
                    connection.busy = False
                    connection.closed += 1

            return Cursor()

    connection = Connection()
    database = AzureSqlDatabase(server="unused", database="unused")
    monkeypatch.setattr(database, "_ensure", lambda: connection)
    assert database.query("EXEC fixture") == [(1,)]
    assert database.query("SELECT 1") == [(1,)]
    assert database.execute("UPDATE fixture SET value=1") == 1
    assert connection.closed == 3


def test_sql_failure_preserves_a_redacted_cause_and_clears_it_on_recovery(monkeypatch) -> None:
    from triage.store.command_center import AzureSqlCommandCenterStore

    secret = "AKIAIOSFODNN7EXAMPLE"
    database = AzureSqlDatabase(server="unused", database="unused", cooldown_seconds=0)

    def unavailable():
        raise RuntimeError(f"Connection rejected with {secret}")

    monkeypatch.setattr(database, "_connect", unavailable)
    with pytest.raises(SqlUnavailable) as error:
        AzureSqlCommandCenterStore(database).commands()
    assert "RuntimeError" in str(error.value)
    assert "Connection rejected" in str(error.value)
    assert secret not in str(error.value)
    assert secret not in database.connection_error
    monkeypatch.setattr(database, "_connect", object)
    assert database.is_available
    assert database.connection_error == ""


def test_sql_attempts_its_first_connection_in_a_fresh_sandbox(monkeypatch) -> None:
    monkeypatch.setattr("triage.store.azure_sql.time.monotonic", lambda: 1.0)
    database = AzureSqlDatabase(server="unused", database="unused")
    calls = []

    def connect():
        calls.append(True)
        return object()

    monkeypatch.setattr(database, "_connect", connect)
    assert database.is_available
    assert calls == [True]


def test_sql_failure_at_monotonic_zero_still_observes_recovery_cooldown(monkeypatch) -> None:
    now = [0.0]
    monkeypatch.setattr("triage.store.azure_sql.time.monotonic", lambda: now[0])
    database = AzureSqlDatabase(server="unused", database="unused")
    calls = []

    def connect():
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError("Synthetic connection failure")
        return object()

    monkeypatch.setattr(database, "_connect", connect)
    assert not database.is_available
    now[0] = 1.0
    assert not database.is_available
    assert len(calls) == 1
    now[0] = 31.0
    assert database.is_available
    assert len(calls) == 2
    assert database.connection_error == ""


def _result(**overrides) -> TriageResult:
    base = {
        "outcome": "needs_human",
        "summary": "ok",
        "request_id": "r-1",
        "signature": "sig-1",
        "root_cause": "Transient failure.",
    }
    base.update(overrides)
    return TriageResult(**base)


# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["triage_incidents", "Incidents", "_x", "a1_b2"],
)
def test_valid_table_names_are_accepted(name: str) -> None:
    assert quote_identifier(name) == f"[dbo].[{name}]"


@pytest.mark.parametrize(
    "name",
    [
        "incidents; DROP TABLE users",
        "incidents]",
        "dbo.incidents",
        "",
        "1incidents",
        "x" * 200,
    ],
)
def test_table_names_that_could_carry_sql_are_refused(name: str) -> None:
    """Table names are interpolated, because SQL has no placeholder for one.

    They come from configuration rather than from a request, so this is a
    defence in depth rather than the front line -- but it is the reason the
    interpolation is safe to do at all.
    """
    with pytest.raises(ValueError):
        quote_identifier(name)


def test_schema_covers_every_store() -> None:
    sql = " ".join(schema_statements(DEFAULT_TABLES))
    for table in DEFAULT_TABLES.values():
        assert table in sql, f"{table} has no CREATE statement"


def test_retiring_permission_storage_preserves_every_operational_table(runner, test_settings) -> None:
    operational = {
        "incidents", "processed", "approvals", "retries", "semantic_health", "leases",
        "claims", "inbox_audit", "pipeline_reruns", "agent_runs", "agent_events",
        "agent_commands", "incident_activity", "data_quality_flags",
    }
    assert set(DEFAULT_TABLES) == set(runner._sql_tables()) == operational
    assert "command_center_access_table_name" not in type(test_settings).model_fields
    sql = " ".join(schema_statements(runner._sql_tables()))
    assert "triage_command_center_access" not in sql
    for table in runner._sql_tables().values():
        assert table in sql
    assert all(statement not in sql.upper() for statement in ("DROP TABLE", "TRUNCATE TABLE", "DELETE FROM"))


def test_schema_creation_reports_failure_rather_than_raising() -> None:
    """Deployment reports a failure; runtime stores do not call this helper."""
    assert ensure_schema(FakeSql(down=True)) is False


# ---------------------------------------------------------------------------
# Recovery -- the regression test for the bug this migration fixed
# ---------------------------------------------------------------------------


def test_a_store_that_starts_without_its_database_refuses_to_start() -> None:
    db = FakeSql(down=True)
    with pytest.raises(SqlUnavailable):
        AzureSqlIncidentStore(db=db)
    assert db.schema_calls == 0
    assert not db.executed


def test_a_store_recovers_when_the_database_comes_back() -> None:
    """Recovery requires an authoritative reload, never a local fallback.

    Tenant policy disabled network access on the state store minutes after it
    was created. The container started while it was unreachable, fell back to
    in-memory, and then reported healthy triage outcomes while persisting none
    of them -- including after connectivity was restored, because nothing ever
    tried again. Only a redeploy fixed it.

    Recovery has to include a *reload*, not just a reconnect: ``find_open`` is
    what stops the agent remediating the same failure twice, and an empty cache
    answers "no open incident" to everything.
    """
    incident_json = (
        '{"id": "inc-1", "signature": "sig-1", "status": "open", '
        '"outcome": "needs_human", "summary": "s", "request_id": "r-0", '
        '"occurrence_count": 1}'
    )
    db = FakeSql(rows=[(incident_json,)])
    store = AzureSqlIncidentStore(db=db)

    db.down = True
    with pytest.raises(SqlUnavailable):
        store.find_open("sig-1")
    assert store.is_durable is False

    db.down = False

    found = store.find_open("sig-1")
    assert found is not None, "must reload once the database is reachable again"
    assert found.id == "inc-1"
    assert store.is_durable is True


def test_a_failed_write_marks_the_store_undurable_so_it_reloads() -> None:
    """A write that fails must not leave the store believing it is in sync."""
    db = FakeSql()
    store = AzureSqlIncidentStore(db=db)
    assert store.is_durable is True

    db.fail_writes = True
    with pytest.raises(SqlUnavailable):
        store.record(_result(), report_name="R")

    assert store.is_durable is False


def test_recording_cannot_report_an_in_memory_outcome_while_sql_is_down() -> None:
    db = FakeSql()
    store = AzureSqlIncidentStore(db=db)
    db.down = True
    with pytest.raises(SqlUnavailable):
        store.record(_result(), report_name="R")
    assert not store._items
    db.down = False
    assert store.list_all() == []
    assert not db.executed


# ---------------------------------------------------------------------------
# Claims
# ---------------------------------------------------------------------------


def test_an_unclaimed_key_is_granted() -> None:
    db = FakeSql()
    claims = AzureSqlClaimStore(db=db)

    assert claims.claim("message:abc") is True


def test_a_key_someone_else_holds_is_refused() -> None:
    """Duplicate key, then a conditional steal that matches no expired row."""
    db = FakeSql()
    db.raise_duplicate = True
    db.affected = 0
    claims = AzureSqlClaimStore(db=db)

    assert claims.claim("message:abc") is False


def test_an_expired_claim_is_stolen() -> None:
    """The steal is one conditional UPDATE, so rowcount decides the winner."""
    db = FakeSql()
    db.raise_duplicate = True
    db.affected = 1
    claims = AzureSqlClaimStore(db=db)

    assert claims.claim("message:abc") is True


def test_the_steal_is_conditional_on_expiry_in_sql() -> None:
    """The expiry test must run on the server, not on the caller's clock.

    Two containers with skewed clocks would otherwise disagree about whether a
    lease had expired, and both could take it.
    """
    db = FakeSql()
    db.raise_duplicate = True
    db.affected = 1
    AzureSqlClaimStore(db=db).claim("message:abc")

    update = next(sql for sql, _ in db.executed if sql.strip().startswith("UPDATE"))
    assert "expires_at < SYSUTCDATETIME()" in update


def test_an_unreachable_database_never_grants_a_claim() -> None:
    """Losing this store must stop work, not let it happen twice.

    Operational stores must fail closed too: a claim alone cannot compensate
    for a terminal incident or processed disposition that was never persisted.
    """
    claims = AzureSqlClaimStore(db=FakeSql(down=True))

    assert claims.claim("message:abc") is False


def test_release_is_survivable_when_the_database_is_down() -> None:
    """Releasing early is an optimisation; the lease expires regardless."""
    AzureSqlClaimStore(db=FakeSql(down=True)).release("message:abc")


def test_release_does_not_delete_a_claim_someone_else_now_holds() -> None:
    """A slow caller must not hand back the claim that replaced its own.

    If A's lease expires mid-run and B steals it, A finishing afterwards used
    to `DELETE ... WHERE claim_key = ?` and let C straight in while B was still
    working. The delete has to prove ownership.
    """
    db = FakeSql()
    claims = AzureSqlClaimStore(db=db)
    assert claims.claim("message:abc") is True

    db.executed.clear()
    claims.release("message:abc")

    delete = next(sql for sql, _ in db.executed if sql.strip().startswith("DELETE"))
    assert "owner = ?" in delete, "release deletes by key alone"


def test_release_without_holding_the_claim_writes_nothing() -> None:
    """Releasing something this process never took must not touch the row."""
    db = FakeSql()
    AzureSqlClaimStore(db=db).release("message:never-held")

    assert not [sql for sql, _ in db.executed if sql.strip().startswith("DELETE")]


def test_two_claims_in_one_process_do_not_share_an_owner() -> None:
    """Host and pid are not unique: a hosted agent is rebuilt per request."""
    db = FakeSql()
    first = AzureSqlClaimStore(db=db)
    second = AzureSqlClaimStore(db=db)
    first.claim("a")
    second.claim("b")

    owners = {owner for owner in list(first._held.values()) + list(second._held.values())}
    assert len(owners) == 2, "two concurrent holders were given the same owner"


# ---------------------------------------------------------------------------
# Approvals
# ---------------------------------------------------------------------------


def _open_request(db: FakeSql, request_id: str = "req-1") -> AzureSqlApprovalChannel:
    fixture = InMemoryApprovalChannel()
    fixture.open(ApprovalRequest(
        request_id=request_id, action="refresh", arguments={}, justification="Offline test",
    ))
    db.rows = [(json.dumps(fixture.get(request_id)),)]
    return AzureSqlApprovalChannel(db=db)


def test_a_decision_is_written_conditionally() -> None:
    """Check-then-write is a race; the WHERE clause has to do the checking.

    Two responders can both read an unanswered request and both write, and the
    later one wins silently. The Azure Table version closed that with an ETag.
    """
    db = FakeSql()
    channel = _open_request(db)
    channel.decide("req-1", decision="approve", responder="a")

    update = next(sql for sql, _ in db.executed if sql.strip().startswith("UPDATE"))
    assert "decision IS NULL OR decision = ''" in update, (
        "the write is unconditional, so simultaneous decisions overwrite"
    )


def test_losing_the_decision_race_is_refused_not_reported_as_success() -> None:
    db = FakeSql()
    channel = _open_request(db)
    db.affected = 0  # somebody answered between the read and the write

    with pytest.raises(ValueError):
        channel.decide("req-1", decision="approve", responder="b")


def test_a_failed_decision_write_is_raised_not_swallowed() -> None:
    """An approval nobody recorded must never read as an approval.

    The read succeeds here and only the write fails, which is the dangerous
    shape: the caller sees a well-formed request, answers it, and the answer
    goes nowhere. Swallowing that would have the CLI print "Approved" for a
    decision the gate will never see.
    """
    db = FakeSql()
    channel = _open_request(db)
    db.fail_writes = True

    with pytest.raises(RuntimeError):
        channel.decide("req-1", decision="approve", responder="a")


# ---------------------------------------------------------------------------
# Recovery must reconcile SQL without replaying uncertain local changes
# ---------------------------------------------------------------------------


def test_recovery_does_not_flush_an_unconfirmed_local_outcome() -> None:
    """The finalization owner reconciles failures; a read cannot replay writes."""
    db = FakeSql()
    store = AzureSqlIncidentStore(db=db)
    db.fail_writes = True
    with pytest.raises(SqlUnavailable):
        store.record(_result(), report_name="R")
    db.fail_writes = False
    assert store.list_all() == []
    assert not db.executed


def test_startup_and_recovery_never_attempt_runtime_schema_creation() -> None:
    db = FakeSql(down=True)
    with pytest.raises(SqlUnavailable):
        AzureSqlIncidentStore(db=db)
    assert db.schema_calls == 0

    db.down = False
    store = AzureSqlIncidentStore(db=db)
    store.list_all()

    assert db.schema_calls == 0
    assert not db.executed


def test_build_claim_store_without_a_database_is_in_process() -> None:
    from triage.store.claims import InMemoryClaimStore as InMem

    assert isinstance(build_claim_store(db=None), InMem)


def _last_payload(db: FakeSql) -> str:
    """The payload of the most recent write, whichever statement carried it."""
    for sql, params in reversed(db.executed):
        head = sql.strip().upper()
        if head.startswith("UPDATE"):
            return params[3]
        if head.startswith("INSERT"):
            return params[4]
    raise AssertionError("nothing was written")


def test_find_open_sees_an_incident_opened_by_another_instance() -> None:
    """find_open must read through, not answer from a cache loaded once.

    The cache is only refreshed when a write fails, so an incident opened by a
    second container was invisible here -- and this call is what decides whether
    the agent may remediate. A stale "no open incident" is exactly how the same
    failure gets remediated twice.
    """
    db = FakeSql(rows=[])
    store = AzureSqlIncidentStore(db=db)
    assert store.find_open("sig-1") is None

    # Another instance opens an incident for the same signature.
    db.rows = [(
        '{"id": "inc-other", "signature": "sig-1", "status": "open", '
        '"outcome": "needs_human", "summary": "s", "request_id": "r-9", '
        '"occurrence_count": 1}',
    )]

    found = store.find_open("sig-1")
    assert found is not None, "answered from a stale cache; a second remediation follows"
    assert found.id == "inc-other"


def test_find_open_drops_a_cached_incident_the_database_no_longer_has() -> None:
    """Otherwise a row deleted or resolved elsewhere lingers as open forever."""
    db = FakeSql(rows=[(
        '{"id": "inc-1", "signature": "sig-1", "status": "open", '
        '"outcome": "needs_human", "summary": "s", "request_id": "r-0", '
        '"occurrence_count": 1}',
    )])
    store = AzureSqlIncidentStore(db=db)
    assert store.find_open("sig-1") is not None

    db.rows = []
    assert store.find_open("sig-1") is None


def test_recovery_keeps_authoritative_counts_not_the_unconfirmed_local_change() -> None:
    """Replaying a dirty cached row can overwrite another worker's newer counts."""
    db = FakeSql(rows=[])
    store = AzureSqlIncidentStore(db=db)
    first = store.record(_result(), report_name="R")
    stale = _last_payload(db)
    assert first.occurrence_count == 1

    # The database goes read-only: reads still work, writes fail.
    db.rows = [(stale,)]
    db.fail_writes = True
    with pytest.raises(SqlUnavailable):
        store.record(_result(), report_name="R")
    assert store.is_durable is False

    # Another worker has finalized newer evidence while this worker was failing.
    db.fail_writes = False
    shared = json.loads(stale)
    shared.update(occurrence_count=5, notified_count=2)
    db.rows = [(json.dumps(shared),)]
    db.executed.clear()

    found = store.find_open("sig-1")
    assert found is not None
    assert found.occurrence_count == 5
    assert found.notified_count == 2
    assert not db.executed
