"""Rejected-window sibling liveness through the emitted work-transition path.

SQLite adapts scalar functions, key hashing and timestamp formatting only.
Native full-procedure, driver and SQL-role acceptance remain separate gates.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3

import pytest
from test_monitoring_sql_retry_finalization import _json
from test_monitoring_sql_retry_finalization import db as db

from triage.monitoring.sql_kernel_common import key_hash
from triage.monitoring.sql_kernel_frontiers import close_frontier_sql, rejected_window_authority_sql
from triage.monitoring.sql_kernel_work import reconciliation_completion_sql
from triage.monitoring.sql_permissions import build_permission_kernel


def _digest(value):
    return hashlib.sha256(value.encode("utf-8")).digest()


def _scalar_sql(statement):
    for variable in ("@work_key", "@work_id"):
        statement = statement.replace(key_hash(variable), f"KEY_DIGEST({variable})")
    statement = statement.replace(" WITH (UPDLOCK,HOLDLOCK)", "")
    statement = statement.replace("CONVERT(nvarchar(40),@now,127)+N'Z'", "@completed_at")
    return re.sub(r"\bN'", "'", statement)


def _typed_query(statement):
    return statement.replace(
        "\nWHERE ", "\nCROSS JOIN (SELECT 'bigint' AS bigint) AS scalar_types WHERE ", 1,
    )


@pytest.fixture
def sibling_case(db):
    kernel = build_permission_kernel()
    records = kernel.names.table("monitoring_records")
    receipts = kernel.names.table("monitoring_receipts")
    leases = kernel.names.table("monitoring_leases")
    db.create_function("KEY_DIGEST", 1, _digest)
    db.execute(f"""CREATE TABLE {records} (
        tenant_id TEXT,epoch TEXT,record_kind TEXT,full_key TEXT,key_hash BLOB,revision INTEGER,
        sequence_number INTEGER,status TEXT,parent_key TEXT,target_key TEXT,work_kind TEXT,due_at TEXT,payload TEXT)""")
    db.execute(f"""CREATE TABLE {receipts} (
        tenant_id TEXT,epoch TEXT,operation TEXT,request_id TEXT,fingerprint TEXT,payload TEXT,
        PRIMARY KEY (tenant_id,epoch,operation,request_id))""")
    db.execute(f"""CREATE TABLE {leases} (
        tenant_id TEXT,epoch TEXT,key_hash BLOB,full_key TEXT,owner_id TEXT,fence INTEGER,
        acquired_at TEXT,expires_at TEXT)""")
    db.execute("CREATE TABLE dbo.control (tenant_id TEXT,epoch TEXT,revision INTEGER)")
    db.execute("INSERT INTO dbo.control VALUES ('tenant','epoch',2)")
    db.execute("CREATE TABLE dbo.actions (state TEXT,budget INTEGER,approval_consumed TEXT)")
    db.execute("INSERT INTO dbo.actions VALUES ('uncertain',1,'original-consumption')")

    def record(kind, key, payload, *, status=None, sequence=None, parent=None, work_kind=None):
        db.execute(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            "tenant", "epoch", kind, key, _digest(key), 1, sequence, status, parent, None,
            work_kind, None, _json(payload),
        ))

    record("validation_frontier", "window", {"accepted_revision": 2, "validated_revision": 0},
           status="pending_validation", sequence=2)
    record("validation_window", "window", {"collection_complete": False}, status="collecting")
    record("validation_handoff", "A", {"policy_revision": 1, "work_id": "work-A", "producer_request_id": "input-A"},
           status="published", sequence=1, parent="window")
    record("validation_handoff", "B", {"policy_revision": 1, "work_id": "work-B", "producer_request_id": "input-B"},
           status="pending_validation", sequence=2, parent="window")
    record("validation_frontier", "unrelated", {"accepted_revision": 1, "validated_revision": 0},
           status="pending_validation", sequence=1)
    work_key = "work:v1:epoch:tenant:work-B"
    work = {
        "tenant_id": "tenant", "epoch": "epoch", "work_id": "work-B", "kind": "reconcile_state",
        "revision": 1, "state": "leased", "policy_revision": 1, "retry_attempt": 0,
        "reconcile_producer": "worker", "reconcile_request_id": "input-B",
        "lease": {"owner_id": "owner-B", "fence": 9},
    }
    record("work", "work-B", work, status="leased", work_kind="reconcile_state")
    db.execute(f"INSERT INTO {leases} VALUES (?,?,?,?,?,?,?,?)", (
        "tenant", "epoch", _digest(work_key), work_key, "owner-B", 9, "before", "2030",
    ))
    db.execute(f"INSERT INTO {leases} VALUES (?,?,?,?,?,?,?,?)", (
        "tenant", "epoch", _digest("controller:inflight"), "controller:inflight", "other", 19, "before", "2030",
    ))

    def receipt(operation, request_id, result, fingerprint="original"):
        db.execute(f"INSERT INTO {receipts} VALUES (?,?,?,?,?,?)", (
            "tenant", "epoch", operation, request_id, fingerprint, _json({"result": result}),
        ))

    receipt("controller.resolve_frontier", "B-pending", {
        "work_id": "work-B", "work_fence": 9, "frontier_key": "window",
        "frontier_revision": 2, "validated_revision": 0, "state": "pending_validation",
        "resolution_scope": "handoff",
    })
    # A's current-fenced policy-2 whole-window rejection preserves both page rows.
    assert db.execute(close_frontier_sql(kernel.names), {
        "tenant_id": "tenant", "epoch": "epoch", "frontier_key": "window",
        "expected_frontier_revision": 2, "validated": 2, "window_decision": "rejected",
        "resolved_at": "2026-09-16T12:00:00Z",
    }).rowcount == 1
    db.execute(f"UPDATE {records} SET status='rejected' WHERE record_kind='validation_window' AND full_key='window'")
    record("frontier_commit", "window", {"request_id": "A-window-rejection"}, status="rejected", sequence=2)
    receipt("controller.resolve_frontier", "A-window-rejection", {
        "work_id": "work-A", "work_fence": 8, "frontier_key": "window", "frontier_revision": 2,
        "validated_revision": 2, "state": "rejected", "resolution_scope": "window",
    })
    db.commit()
    params = {
        "tenant_id": "tenant", "epoch": "epoch", "work_id": "work-B", "work_key": work_key,
        "owner_id": "owner-B", "fence": 9, "work_revision": 1, "frontier_key": "window",
        "accepted": 2, "handoff_key": "B", "handoff_revision": 2, "producer_request_id": "input-B",
        "now": "2026-09-16T12:01:00Z", "new_expiry": "2026-09-16T12:01:00Z",
        "completed_at": "2026-09-16T12:01:00Z", "retry_at": None, "detail": "Window was durably rejected.",
    }
    return kernel, params, receipt


def _acknowledge(db, kernel, params, request_id):
    receipts = kernel.names.table("monitoring_receipts")
    records = kernel.names.table("monitoring_records")
    leases = kernel.names.table("monitoring_leases")
    with db:
        if not db.execute("SELECT 1 FROM dbo.control WHERE tenant_id=@tenant_id AND epoch=@epoch", params).fetchone():
            raise ValueError("context mismatch")
        prior = db.execute(f"SELECT payload FROM {receipts} WHERE tenant_id=@tenant_id AND epoch=@epoch "
                           "AND operation='controller.resolve_frontier' AND request_id=@request_id",
                           {**params, "request_id": request_id}).fetchone()
        if prior:
            return json.loads(prior[0])["result"]
        if not db.execute(f"SELECT 1 FROM {leases} WHERE tenant_id=@tenant_id AND epoch=@epoch "
                          "AND full_key=@work_key AND owner_id=@owner_id AND fence=@fence AND expires_at>@now",
                          params).fetchone():
            raise ValueError("lease lost")
        if not db.execute(f"SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch "
                          "AND record_kind='work' AND full_key=@work_id AND revision=@work_revision AND status='leased'",
                          params).fetchone():
            raise ValueError("work revision changed")
        authority = db.execute(_typed_query(rejected_window_authority_sql(kernel.names)), params).fetchall()
        if len(authority) != 1:
            raise ValueError("window rejection authority mismatch")
        result = {
            "work_id": params["work_id"], "work_fence": params["fence"],
            "frontier_key": params["frontier_key"], "frontier_revision": params["accepted"],
            "validated_revision": params["accepted"], "state": "rejected",
            "handoff_decision": "pending_validation", "resolution_scope": "window_acknowledgement",
            "window_rejection_request_id": authority[0][0],
        }
        db.execute(f"INSERT INTO {receipts} VALUES (?,?,?,?,?,?)", (
            params["tenant_id"], params["epoch"], "controller.resolve_frontier", request_id, "original",
            _json({"result": result}),
        ))
    return result


def _transition(db, kernel, original_params, transition, request_id):
    """Execute the exact completion query, lease CAS and work update emitted by transition_work."""
    params = {**original_params, "transition": transition}
    body = next(obj.ddl for obj in kernel.objects if obj.logical_name == "controller.transition_work")
    records, leases, receipts = (kernel.names.table(name) for name in (
        "monitoring_records", "monitoring_leases", "monitoring_receipts",
    ))
    with db:
        prior = db.execute(f"SELECT payload FROM {receipts} WHERE tenant_id=@tenant_id AND epoch=@epoch "
                           "AND operation='controller.transition_work' AND request_id=@request_id",
                           {**params, "request_id": request_id}).fetchone()
        if prior:
            return json.loads(prior[0])["result"]["work"]
        if not db.execute(_typed_query(reconciliation_completion_sql(kernel.names)), params).fetchone():
            raise ValueError("own terminal receipt missing")
        work = db.execute(f"SELECT payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch "
                          "AND record_kind='work' AND full_key=@work_id AND revision=@work_revision AND status='leased'",
                          params).fetchone()
        if work is None:
            raise ValueError("work revision/state mismatch")
        params["stored_work"] = work[0]
        lease_sql = re.search(
            re.escape(f"UPDATE {leases} SET expires_at=@new_expiry") + r".*?;",
            body, re.S,
        )[0]
        if db.execute(_scalar_sql(lease_sql), params).rowcount != 1:
            raise ValueError("lease transition lost ownership")
        expressions = [
            re.search(r"SET @stored_work=(JSON_MODIFY\(@stored_work,'\$\.lease',NULL\));", body)[1],
            re.search(r"SET @stored_work=(JSON_MODIFY\(@stored_work,'\$\.state',\s*CASE @transition.*?END\));", body, re.S)[1],
            re.search(r"SET @stored_work=(JSON_MODIFY\(@stored_work,'\$\.completed_at',.*?\));", body)[1],
            re.search(r"SET @stored_work=(JSON_MODIFY\(@stored_work,'\$\.disposition',@detail\));", body)[1],
            re.search(r"SET @stored_work=(JSON_MODIFY\(@stored_work,'\$\.revision',@work_revision\+1\));", body)[1],
        ]
        for expression in expressions:
            params["stored_work"] = db.execute("SELECT " + _scalar_sql(expression), params).fetchone()[0]
        update = re.search(
            re.escape(f"UPDATE {records} SET revision=revision+1,status=JSON_VALUE(@stored_work,'$.state'),")
            + r".*?;", body, re.S,
        )[0]
        if db.execute(_scalar_sql(update), params).rowcount != 1:
            raise ValueError("work transition lost revision")
        result = {"work_id": params["work_id"], "work": json.loads(params["stored_work"])}
        db.execute(f"INSERT INTO {receipts} (tenant_id,epoch,operation,request_id,fingerprint,payload) VALUES (?,?,?,?,?,?)", (
            params["tenant_id"], params["epoch"], "controller.transition_work", request_id, "original", _json({"result": result}),
        ))
    return result["work"]


@pytest.mark.parametrize("transition,terminal", [("complete", "completed"), ("disposition", "dispositioned")])
def test_two_handoffs_policy_advance_and_sibling_completion_use_actual_transition_path(db, sibling_case, transition, terminal):
    kernel, params, _ = sibling_case
    records = kernel.names.table("monitoring_records")
    preserved = db.execute(f"SELECT * FROM {records} WHERE record_kind<>'work' ORDER BY record_kind,full_key").fetchall()
    control, actions = db.execute("SELECT * FROM dbo.control").fetchall(), db.execute("SELECT * FROM dbo.actions").fetchall()
    with pytest.raises(ValueError, match="terminal receipt"):
        _transition(db, kernel, params, transition, "before-ack")
    assert _acknowledge(db, kernel, params, "B-pending")["state"] == "pending_validation"
    ack = _acknowledge(db, kernel, params, "B-terminal-ack")
    assert ack["window_rejection_request_id"] == "A-window-rejection"
    assert _acknowledge(db, kernel, params, "B-terminal-ack") == ack
    completed = _transition(db, kernel, params, transition, "B-completion")
    assert completed["state"] == terminal and completed.get("lease") is None
    assert completed["revision"] == 2
    assert _transition(db, kernel, params, transition, "B-completion") == completed
    assert _acknowledge(db, kernel, params, "B-pending")["state"] == "pending_validation"
    assert db.execute(f"SELECT * FROM {records} WHERE record_kind<>'work' ORDER BY record_kind,full_key").fetchall() == preserved
    assert db.execute("SELECT * FROM dbo.control").fetchall() == control
    assert db.execute("SELECT * FROM dbo.actions").fetchall() == actions
    assert db.execute(f"SELECT expires_at FROM {kernel.names.table('monitoring_leases')} "
                      "WHERE full_key='controller:inflight'").fetchone()[0] == "2030"


@pytest.mark.parametrize("changed", [
    {"epoch": "old-epoch"}, {"owner_id": "wrong-owner"}, {"fence": 8}, {"work_revision": 0},
    {"frontier_key": "unrelated"}, {"handoff_key": "A"}, {"producer_request_id": "input-A"}, {"accepted": 3},
])
def test_sibling_ack_requires_own_context_lease_and_exact_window_binding(db, sibling_case, changed):
    kernel, params, _ = sibling_case
    with pytest.raises(ValueError):
        _acknowledge(db, kernel, {**params, **changed}, "bad-ack")
    assert not db.execute(_typed_query(reconciliation_completion_sql(kernel.names)), params).fetchone()


@pytest.mark.parametrize("change", ["wrong_window", "wrong_scope", "missing_receipt", "wrong_revision"])
def test_rejected_flag_alone_is_not_sibling_completion_authority(db, sibling_case, change):
    kernel, params, _ = sibling_case
    table = kernel.names.table("monitoring_receipts")
    if change == "missing_receipt":
        db.execute(f"DELETE FROM {table} WHERE request_id='A-window-rejection'")
    else:
        payload = json.loads(db.execute(f"SELECT payload FROM {table} WHERE request_id='A-window-rejection'").fetchone()[0])
        if change == "wrong_window":
            payload["result"]["frontier_key"] = "unrelated"
        elif change == "wrong_scope":
            payload["result"]["resolution_scope"] = "handoff"
        else:
            payload["result"]["validated_revision"] = 1
        db.execute(f"UPDATE {table} SET payload=? WHERE request_id='A-window-rejection'", (_json(payload),))
    db.commit()
    with pytest.raises(ValueError, match="authority"):
        _acknowledge(db, kernel, params, "bad-authority")


def test_another_valid_rejected_window_cannot_authorize_this_sibling(db, sibling_case):
    kernel, params, receipt = sibling_case
    records = kernel.names.table("monitoring_records")
    for kind, payload in (
        ("validation_frontier", {"accepted_revision": 2, "validated_revision": 2}),
        ("validation_window", {"collection_complete": False}),
        ("frontier_commit", {"request_id": "other-window-rejection"}),
    ):
        db.execute(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            "tenant", "epoch", kind, "other-window", _digest("other-window"), 1, 2,
            "rejected", None, None, None, None, _json(payload),
        ))
    receipt("controller.resolve_frontier", "other-window-rejection", {
        "frontier_key": "other-window", "frontier_revision": 2, "validated_revision": 2,
        "state": "rejected", "resolution_scope": "window",
    })
    db.commit()
    with pytest.raises(ValueError, match="authority"):
        _acknowledge(db, kernel, {**params, "frontier_key": "other-window"}, "cross-window")


def test_terminal_work_completion_rechecks_root_receipt_and_owner(db, sibling_case):
    kernel, params, _ = sibling_case
    _acknowledge(db, kernel, params, "B-terminal-ack")
    with pytest.raises(ValueError, match="lease transition"):
        _transition(db, kernel, {**params, "owner_id": "other-owner"}, "complete", "wrong-owner")
    table = kernel.names.table("monitoring_receipts")
    payload = json.loads(db.execute(f"SELECT payload FROM {table} WHERE request_id='A-window-rejection'").fetchone()[0])
    payload["result"]["frontier_key"] = "unrelated"
    db.execute(f"UPDATE {table} SET payload=? WHERE request_id='A-window-rejection'", (_json(payload),))
    db.commit()
    with pytest.raises(ValueError, match="terminal receipt"):
        _transition(db, kernel, params, "complete", "wrong-root")


def test_completion_rechecks_ack_root_and_rolls_back_lease_on_receipt_failure(db, sibling_case):
    kernel, params, _ = sibling_case
    _acknowledge(db, kernel, params, "B-terminal-ack")
    with pytest.raises(ValueError, match="terminal receipt"):
        _transition(db, kernel, {**params, "fence": 8}, "complete", "bad-fence")
    receipts = kernel.names.tables["monitoring_receipts"]
    db.execute(f"""CREATE TRIGGER dbo.fail_completion BEFORE INSERT ON [{receipts}]
        WHEN NEW.operation='controller.transition_work'
        BEGIN SELECT RAISE(ABORT,'injected completion receipt failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="completion receipt failure"):
        _transition(db, kernel, params, "complete", "failure")
    assert db.execute(f"SELECT status,revision FROM {kernel.names.table('monitoring_records')} "
                      "WHERE record_kind='work' AND full_key='work-B'").fetchone() == ("leased", 1)
    assert db.execute(f"SELECT expires_at FROM {kernel.names.table('monitoring_leases')} "
                      "WHERE full_key=@work_key", params).fetchone()[0] == "2030"
