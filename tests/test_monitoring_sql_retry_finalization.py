"""Execute emitted SQL guards/mutation fragments offline, not native SQL proof."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
from datetime import UTC, datetime

import pytest

from triage.monitoring.sql_kernel_history import (
    historical_payload_expression,
    historical_predicate,
    occurrence_increment_expression,
    retain_budget_head_expression,
    save_incident_sql,
)
from triage.monitoring.sql_kernel_retries import (
    budget_debit_expression,
    link_reservation_sql,
    retry_admission_predicate,
    save_budget_sql,
    successor_predicate,
)
from triage.monitoring.sql_permissions import build_permission_kernel, integration_contract
from triage.store.retries import MAX_ATTEMPTS, backoff_seconds


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _path(payload, path):
    if payload is None:
        return None
    value = json.loads(payload)
    for part in path.removeprefix("$.").split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def _value(payload, path):
    value = _path(payload, path)
    if value is None or isinstance(value, (dict, list)):
        return None
    return ("true" if value else "false") if type(value) is bool else str(value)


def _query(payload, path):
    value = _path(payload, path)
    return _json(value) if isinstance(value, (dict, list)) else None


def _modify(payload, path, value):
    body = json.loads(payload)
    parts = path.removeprefix("$.").split(".")
    parent = body
    for part in parts[:-1]:
        parent = parent.setdefault(part, {})
    if value is None:
        parent.pop(parts[-1], None)
    else:
        parent[parts[-1]] = value
    return _json(body)


def _convert(kind, value):
    if value is None:
        return None
    try:
        if kind in {"int", "bigint"}:
            return int(value)
        assert kind == "datetimeoffset"
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC).isoformat()
    except (TypeError, ValueError):
        return None


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.create_function("JSON_VALUE", 2, _value)
    conn.create_function("JSON_QUERY", 2, _query)
    conn.create_function("JSON_MODIFY", 3, _modify)
    conn.create_function("TRY_CONVERT", 2, _convert)
    conn.create_function("TODATETIMEOFFSET", 2, lambda value, zone: _convert("datetimeoffset", value))
    conn.create_function("HASHBYTES", 2, lambda algorithm, value: hashlib.sha256(value.encode("utf-16-le")).digest())
    conn.create_collation("Latin1_General_100_BIN2", lambda a, b: (a > b) - (a < b))
    conn.execute("ATTACH DATABASE ':memory:' AS dbo")
    yield conn
    conn.close()


def _eval(db, expression, params):
    # N-prefixed literal syntax is the only lexical adaptation. The predicates
    # and update statements themselves are the production generator's output.
    expression = re.sub(r"\bN'", "'", expression)
    return db.execute(
        f"SELECT CASE WHEN ({expression}) THEN 1 ELSE 0 END "
        "FROM (SELECT 'bigint' AS bigint,'int' AS int,'datetimeoffset' AS datetimeoffset) AS types",
        params,
    ).fetchone()[0]


@pytest.fixture
def retry():
    target = {"tenant_id": "tenant", "epoch": "epoch", "workload": "powerbi",
              "workspace_id": "workspace", "item_id": "item"}
    request = {
        "expected": {"revision": 7}, "action": "powerbi_refresh", "work_id": "parent-work",
        "review_id": "review", "expected_review_revision": 3, "parameter_hash": "a" * 64,
        "arguments": {}, "source_execution": {"target": target, "run_id": "exact-source"},
        "incident": {"target": target, "signature": "signature"},
    }
    parent = {
        "state": "rejected", "rejection": {"reason": "throttled", "retry_after_seconds": 0},
        "retry_attempt": 0, "retry_work_id": "retry-work", "retry_reservation_id": None,
        "request": request,
    }
    return {
        "request": copy.deepcopy(request), "retry_parent": parent,
        "retry_parent_work": {"state": "completed", "finalization_id": "finalization",
                              "action_reservation_id": "parent-action"},
        "retry_attempt": 1, "work_id": "retry-work", "retry_of": "parent-action",
    }


def _successor_params(retry):
    return {key: _json(value) if isinstance(value, dict) else value for key, value in retry.items()}


def test_only_the_original_unused_exact_successor_can_reserve(db, retry):
    assert _eval(db, successor_predicate(), _successor_params(retry))
    for field, value in (
        ("state", "submitted"), ("state", "uncertain"), ("retry_work_id", "fork"),
        ("retry_reservation_id", "already-used"), ("submitted_at", "2026-09-16T00:00:00Z"),
        ("submitted_execution", {"run_id": "accepted"}), ("configuration", {"changed": True}),
    ):
        changed = copy.deepcopy(retry)
        changed["retry_parent"][field] = value
        assert not _eval(db, successor_predicate(), _successor_params(changed)), field


@pytest.mark.parametrize("change", ["unfinalized", "no_receipt", "wrong_incident", "wrong_source",
                                  "wrong_hash", "wrong_configuration_hash", "not_throttled", "approval_reuse"])
def test_successor_guard_rejects_changed_lineage_and_approval_reuse(db, retry, change):
    if change == "unfinalized":
        retry["retry_parent_work"]["state"] = "finalizing"
    elif change == "no_receipt":
        retry["retry_parent_work"]["finalization_id"] = None
    elif change == "wrong_incident":
        retry["request"]["incident"]["signature"] = "different"
    elif change == "wrong_source":
        retry["request"]["source_execution"]["run_id"] = "different"
    elif change == "wrong_hash":
        retry["request"]["parameter_hash"] = "b" * 64
    elif change == "wrong_configuration_hash":
        retry["request"]["configuration_hash"] = "changed-technical-identity"
    elif change == "not_throttled":
        retry["retry_parent"]["rejection"]["reason"] = "definitive_client_error"
    else:
        retry["request"]["approval"] = {"approval_id": "consumed"}
        retry["retry_parent"]["request"]["approval"] = {"approval_id": "consumed"}
    assert not _eval(db, successor_predicate(), _successor_params(retry))


def test_existing_three_deferred_attempt_cap_is_exact(db, retry):
    assert MAX_ATTEMPTS == 3
    for attempt in range(1, MAX_ATTEMPTS + 1):
        retry["retry_attempt"] = attempt
        retry["retry_parent"]["retry_attempt"] = attempt - 1
        assert _eval(db, successor_predicate(), _successor_params(retry))
    for attempt in (0, 4):
        retry["retry_attempt"] = attempt
        retry["retry_parent"]["retry_attempt"] = attempt - 1
        assert not _eval(db, successor_predicate(), _successor_params(retry))


@pytest.fixture
def admission(retry):
    request = retry["request"]
    target = request["source_execution"]["target"]
    return {
        "retry_request": request, "maintenance": 0, "current_revision": 7, "now": "2026-09-16T12:00:00Z",
        "retry_target": {
            "state": "current", "admission_basis": "reviewed", "observation": {"enabled": True},
            "action": {"enabled": True, "action": "powerbi_refresh", "review_id": "review", "review_revision": 3},
            "policy_revision": 7,
        },
        "retry_review": {
            "state": "verified", "publication_status": "published", "parameters_redacted": False,
            "policy_revision": 7, "revision": 3, "action": "powerbi_refresh", "parameter_hash": "a" * 64,
            "target": target, "expires_at": "2026-09-17T00:00:00Z",
        },
        "retry_capability": {
            "read_status": "verified", "action_status": "verified", "exact_action_correlation": True,
            "target": target, "expires_at": "2026-09-17T00:00:00Z",
        },
    }


@pytest.mark.parametrize("change", ["maintenance", "epoch_policy", "scope", "review", "expiry",
                                  "missing_expiry", "capability", "correlation"])
def test_retry_creation_and_claim_recheck_current_admission(db, admission, change):
    assert _eval(db, retry_admission_predicate(), _successor_params(admission))
    if change == "maintenance":
        admission["maintenance"] = 1
    elif change == "epoch_policy":
        admission["current_revision"] = 8
    elif change == "scope":
        admission["retry_target"]["action"]["enabled"] = False
    elif change == "review":
        admission["retry_review"]["state"] = "revoked"
    elif change in {"expiry", "missing_expiry"}:
        admission["retry_review"]["expires_at"] = "2026-09-15T00:00:00Z" if change == "expiry" else None
    elif change == "capability":
        admission["retry_capability"]["action_status"] = "denied"
    else:
        admission["retry_capability"]["exact_action_correlation"] = False
    assert not _eval(db, retry_admission_predicate(), _successor_params(admission))


def test_link_budget_and_receipt_are_one_rollback_unit_without_approval_refund(db, retry):
    kernel = build_permission_kernel()
    table = kernel.names.table("monitoring_records")
    db.execute(f"""CREATE TABLE {table} (
        tenant_id TEXT,epoch TEXT,record_kind TEXT,full_key TEXT,revision INTEGER,status TEXT,payload TEXT)""")
    db.execute("CREATE TABLE dbo.receipts (request_id TEXT PRIMARY KEY,fingerprint TEXT,payload TEXT)")
    db.execute("CREATE TABLE dbo.approvals (consumed_at TEXT)")
    db.execute("INSERT INTO dbo.approvals VALUES ('original-consumption')")
    budget = {"revision": 5, "action_count": 1}
    parent = {**retry["retry_parent"], "revision": 2}
    db.executemany(f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?)", [
        ("tenant", "epoch", "action", "parent-action", 2, "rejected", _json(parent)),
        ("tenant", "epoch", "incident_state", "incident", 5, None, _json(budget)),
    ])
    db.commit()
    params = {
        "tenant_id": "tenant", "epoch": "epoch", "retry_of": "parent-action", "retry_parent_revision": 2,
        "work_id": "retry-work", "reservation_id": "new-action", "updated_at_text": "2026-09-16T12:00:00Z",
        "budget": _json(budget), "used": 1, "budget_debit": 0, "budget_revision": 5, "incident_key": "incident",
    }
    params["budget_json"] = db.execute("SELECT " + budget_debit_expression(), params).fetchone()[0]
    assert json.loads(params["budget_json"])["action_count"] == 1
    db.execute("""CREATE TRIGGER dbo.reject_receipt BEFORE INSERT ON receipts
        BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END""")

    def commit():
        with db:
            prior = db.execute("SELECT fingerprint,payload FROM dbo.receipts WHERE request_id='request'").fetchone()
            if prior:
                if prior[0] != "fingerprint":
                    raise ValueError("original fingerprint changed")
                return prior[1]
            if db.execute(link_reservation_sql(kernel.names), params).rowcount != 1:
                raise ValueError("successor already used")
            if db.execute(save_budget_sql(kernel.names), params).rowcount != 1:
                raise ValueError("budget revision changed")
            db.execute("INSERT INTO dbo.receipts VALUES ('request','fingerprint','original-result')")
        return "original-result"

    with pytest.raises(sqlite3.IntegrityError, match="receipt failure"):
        commit()
    rows = db.execute(f"SELECT full_key,revision,payload FROM {table} ORDER BY full_key").fetchall()
    assert rows == [("incident", 5, _json(budget)), ("parent-action", 2, _json(parent))]
    assert db.execute("SELECT consumed_at FROM dbo.approvals").fetchone()[0] == "original-consumption"
    db.execute("DROP TRIGGER dbo.reject_receipt")
    assert commit() == "original-result"
    assert commit() == "original-result"
    assert db.execute("SELECT COUNT(*) FROM dbo.receipts").fetchone()[0] == 1
    assert db.execute(link_reservation_sql(kernel.names), {**params, "retry_parent_revision": 3}).rowcount == 0
    saved = json.loads(db.execute(f"SELECT payload FROM {table} WHERE record_kind='incident_state'").fetchone()[0])
    assert saved["action_count"] == 1 and saved["revision"] == 6
    assert db.execute("SELECT consumed_at FROM dbo.approvals").fetchone()[0] == "original-consumption"


def test_historical_sql_retains_every_newer_field_and_occupied_slot(db):
    prior = {
        "id": "incident", "signature": "sig", "status": "resolved", "outcome": "resolved",
        "occurrence_count": 4, "notified_count": 2, "first_seen_at": "2026-09-14T00:00:00Z",
        "last_seen_at": "2026-09-16T12:00:00Z", "pipeline_failure": {"run": {"id": "newer-run"}},
        "actions": [{"name": "verified-newer-action"}], "trace": ["keep"], "unknown_metadata": {"keep": 1},
    }
    budget = {
        "revision": 9, "action_count": 1, "latest_started_at": "2026-09-16T11:00:00Z",
        "latest_execution": {"run_id": "newer-run"},
    }
    params = {"budget": _json(budget), "plan": _json({"source_started_at": "2026-09-15T00:00:00Z"})}
    assert _eval(db, historical_predicate(), params)
    merged = json.loads(db.execute("SELECT " + historical_payload_expression(), {
        "prior": _json(prior), "next_occurrences": 5,
    }).fetchone()[0])
    assert merged == {**prior, "occurrence_count": 5}
    updated = json.loads(db.execute("SELECT " + retain_budget_head_expression(), {
        "budget": _json(budget), "budget_revision": 9, "updated_at_text": "2026-09-16T13:00:00Z",
    }).fetchone()[0])
    assert updated == {**budget, "revision": 10, "updated_at": "2026-09-16T13:00:00Z"}
    for recorded, kind, expected in ((0, "triage", 1), (1, "triage", 0), (0, "verify_action", 0)):
        increment = db.execute("SELECT " + occurrence_increment_expression(), {
            "source_recorded": recorded, "stored_work_kind": kind,
        }).fetchone()[0]
        assert increment == expected


def test_historical_payload_cas_marker_and_receipt_rollback_together(db):
    kernel = build_permission_kernel()
    table = kernel.names.table("incidents")
    db.execute(f"""CREATE TABLE {table} (
        incident_id TEXT PRIMARY KEY,signature TEXT,status TEXT,updated_at TEXT,payload TEXT)""")
    db.execute("CREATE TABLE dbo.occurrences (source_key TEXT PRIMARY KEY)")
    db.execute("CREATE TABLE dbo.receipts (request_id TEXT PRIMARY KEY,payload TEXT)")
    prior = _json({"id": "incident", "signature": "sig", "status": "resolved",
                   "outcome": "resolved", "occurrence_count": 4, "latest_evidence": {"run": "new"}})
    db.execute(f"INSERT INTO {table} VALUES ('incident','sig','resolved','newer-time',?)", (prior,))
    db.commit()
    db.execute("""CREATE TRIGGER dbo.fail_history_receipt BEFORE INSERT ON receipts
        BEGIN SELECT RAISE(ABORT,'injected finalization receipt failure'); END""")

    def finalize(request_id, expected_digest=None):
        with db:
            receipt = db.execute("SELECT payload FROM dbo.receipts WHERE request_id=?", (request_id,)).fetchone()
            if receipt:
                return receipt[0]
            original = db.execute(f"SELECT payload FROM {table} WHERE incident_id='incident'").fetchone()[0]
            recorded = db.execute("SELECT 1 FROM dbo.occurrences WHERE source_key='older-source'").fetchone()
            increment = db.execute("SELECT " + occurrence_increment_expression(), {
                "source_recorded": int(recorded is not None), "stored_work_kind": "triage",
            }).fetchone()[0]
            merged = db.execute("SELECT " + historical_payload_expression(), {
                "prior": original, "next_occurrences": json.loads(original)["occurrence_count"] + increment,
            }).fetchone()[0]
            changed = db.execute(save_incident_sql(kernel.names), {
                "historical": 1, "signature": "sig", "updated_at_text": "late-observation",
                "merged": merged, "incident_id": "incident",
                "prior_payload_digest": expected_digest or hashlib.sha256(original.encode("utf-16-le")).digest(),
            }).rowcount
            if changed != 1:
                raise ValueError("incident CAS lost")
            db.execute("INSERT OR IGNORE INTO dbo.occurrences VALUES ('older-source')")
            db.execute("INSERT INTO dbo.receipts VALUES (?,?)", (request_id, merged))
        return merged

    with pytest.raises(sqlite3.IntegrityError, match="finalization receipt failure"):
        finalize("first")
    assert db.execute(f"SELECT payload FROM {table}").fetchone()[0] == prior
    assert db.execute("SELECT COUNT(*) FROM dbo.occurrences").fetchone()[0] == 0
    db.execute("DROP TRIGGER dbo.fail_history_receipt")
    with pytest.raises(ValueError, match="CAS lost"):
        finalize("stale", b"\x01" * 32)
    assert db.execute("SELECT COUNT(*) FROM dbo.occurrences").fetchone()[0] == 0
    accepted = finalize("first")
    assert finalize("first") == accepted
    assert json.loads(finalize("different-receipt-same-source"))["occurrence_count"] == 5
    assert json.loads(accepted) == {**json.loads(prior), "occurrence_count": 5}
    assert db.execute(f"SELECT status,updated_at FROM {table}").fetchone() == ("resolved", "newer-time")


def test_generated_operations_wire_the_guards_before_writes_and_receipts():
    kernel = build_permission_kernel()
    bodies = {obj.logical_name: obj.ddl for obj in kernel.objects}
    reject = bodies["controller.transition_action"]
    assert reject.index("Only a confirmed no-effect rejection") < reject.index("DECLARE @retry_work")
    assert reject.index("DECLARE @retry_work") < reject.index("SET revision=revision+1,status=@transition")
    assert reject.index("Terminal action lost its original active owner fence") < reject.index("AS retry_work")
    assert "'$.request.lease.fence'" in reject
    assert "WHEN 1 THEN 900 WHEN 2 THEN 1800 WHEN 3 THEN 3600" in reject
    assert [backoff_seconds(i) for i in range(1, MAX_ATTEMPTS + 1)] == [900, 1800, 3600]
    reserve = bodies["controller.reserve_action"]
    assert "SET @budget_debit=0" in reserve
    assert "Older incident metadata cannot authorize another source action" in reserve
    assert "Retry lost current scope, review or action capability" in reserve
    assert "IF @frontier_pending=1" in reserve
    assert "The rejected predecessor has no committed original finalization" in reserve
    assert reserve.index("IF @prior_payload IS NOT NULL") < reserve.index("IF @retry_of IS NOT NULL")
    assert "owner_id=@owner_id AND fence=@fence" in reserve
    claim = bodies["controller.claim_work"]
    assert claim.index("predecessor_not_finalized") < claim.index("SET @target_lease_key=N'controller:'")
    assert "retry_no_longer_admitted" in claim
    finalize = bodies["controller.finalize"]
    assert "$.occurrences" not in finalize
    assert finalize.index("Original SQL NVARCHAR incident payload changed") < finalize.index("SET @merged=JSON_MODIFY(@prior")
    assert "incident_occurrence" in finalize and "WHEN @historical=1" in finalize
    assert "THROW 51077" not in "\n".join(bodies.values())
    assert kernel.unresolved_cases == ()
    assert "linked_retry_contract" in integration_contract()
