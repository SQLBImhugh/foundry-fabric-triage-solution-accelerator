"""Real-caller argument and submitted-execution regressions; no live SQL."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest
from test_monitoring_sql_retry_finalization import _eval, _json
from test_monitoring_sql_retry_finalization import db as db
from test_monitoring_sql_retry_finalization import retry as retry

from triage.monitoring import models as m
from triage.monitoring.sql_kernel_arguments import (
    PIPELINE_ARGUMENT_FIELDS,
    pipeline_canonical_expression,
    pipeline_identity_predicate,
    pipeline_shape_predicate,
)
from triage.monitoring.sql_kernel_common import key_hash
from triage.monitoring.sql_kernel_contracts import RECORD_COLUMNS
from triage.monitoring.sql_kernel_correlation import (
    correlation_insert_sql,
    execution_reservations_sql,
    submitted_collision_sql,
)
from triage.monitoring.sql_kernel_retries import successor_predicate
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.pipeline_models import PipelineFailure, PipelineRun, PipelineTarget
from triage.store.pipeline_reruns import InMemoryPipelineRerunStore
from triage.tools.fabric_pipeline import MockFabricPipelineClient
from triage.tools.pipeline_actions import PipelineToolContext


def _sql(kernel, name):
    return next(o.ddl for o in kernel.objects if o.logical_name == name)


@pytest.fixture
def actual_pipeline():
    target = PipelineTarget(
        name="Synthetic reviewed pipeline", workspace_id="11111111-1111-4111-8111-111111111111",
        pipeline_id="22222222-2222-4222-8222-222222222222", rerun_safe=True,
        rerun_parameters={"BatchId": 42, "Label": "synthetic \u00e9", "Nested": {"enabled": True}},
    )
    now = datetime(2026, 9, 16, 12, tzinfo=UTC)
    run = PipelineRun(
        id="33333333-3333-4333-8333-333333333333", item_id=target.pipeline_id,
        job_type="Pipeline", invoke_type="Scheduled", status="Failed",
        start_time=now-timedelta(minutes=2), end_time=now-timedelta(minutes=1),
    )
    client = MockFabricPipelineClient([run])
    context = PipelineToolContext(
        failure=PipelineFailure(target=target, run=run), client=client,
        reruns=InMemoryPipelineRerunStore(), signature="synthetic-signature",
    )
    arguments = context.approval_arguments("Retry only the reviewed synthetic batch.")
    assert client.calls == []
    return context, arguments


def _pipeline_shape(db, arguments):
    predicate = pipeline_shape_predicate().replace("OPENJSON(", "json_each(").replace("type<>1", "type<>'text'")
    return _eval(db, predicate, {"arguments": _json(arguments)})


def _pipeline_canonical(db, kernel, arguments):
    db.create_function("JSON_IDENTITY_STRING", 1, lambda text: json.dumps(text, ensure_ascii=True))
    expression = pipeline_canonical_expression(kernel.names)
    expression = expression.replace(kernel.names.object("json_identity_string"), "JSON_IDENTITY_STRING")
    expression = re.sub(r"\bN'", "'", expression).replace("+", "||")
    return db.execute("SELECT " + expression, {
        "arguments": _json(arguments), "justification": arguments["justification"],
        "parameter_preview": arguments["parameter_preview"],
    }).fetchone()[0]


def test_real_pipeline_approval_arguments_are_preserved_and_canonicalized(db, actual_pipeline):
    context, arguments = actual_pipeline
    kernel = build_permission_kernel()
    assert set(arguments) == set(PIPELINE_ARGUMENT_FIELDS)
    assert _pipeline_shape(db, arguments)
    rendered = _pipeline_canonical(db, kernel, arguments)
    assert rendered == json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    assert json.loads(rendered) == arguments
    assert hashlib.sha256(rendered.encode("utf-8")).hexdigest() != context.failure.target.parameter_hash
    assert context.approval_parameter_hash == arguments["parameter_hash"]
    request = {
        "source_execution": {"target": {"workload": "fabric_pipeline", "workspace_id": context.failure.target.workspace_id,
                                       "item_id": context.failure.target.pipeline_id},
                             "run_id": context.failure.run.id, "run_id_kind": "fabric_job"},
    }
    params = {"full_arguments": _json(arguments), "request": _json(request), "parameter_hash": arguments["parameter_hash"]}
    assert _eval(db, pipeline_identity_predicate(), params)
    for key in ("workspace_id", "pipeline_id", "failed_run_id"):
        altered = {**arguments, key: "44444444-4444-4444-8444-444444444444"}
        assert not _eval(db, pipeline_identity_predicate(), {**params, "full_arguments": _json(altered)})
    changed_preview = {**arguments, "parameter_preview": "Different non-authoritative preview text"}
    assert _eval(db, pipeline_identity_predicate(), {**params, "full_arguments": _json(changed_preview)})
    assert _pipeline_canonical(db, kernel, changed_preview) != rendered


@pytest.mark.parametrize("change", ["missing_id", "extra", "numeric_preview", "case_key"])
def test_real_pipeline_closed_shape_refuses_missing_extra_or_wrong_typed_fields(db, actual_pipeline, change):
    _, arguments = actual_pipeline
    values = dict(arguments)
    if change == "missing_id":
        values.pop("workspace_id")
    elif change == "extra":
        values["parameters"] = {"unreviewed": True}
    elif change == "numeric_preview":
        values["parameter_preview"] = 42
    else:
        values["WORKSPACE_ID"] = values.pop("workspace_id")
    assert not _pipeline_shape(db, values)


def test_deferred_reason_changes_without_reusing_parent_full_argument_hash(db, retry):
    retry["retry_parent"]["request"]["arguments"] = {"justification": "Original transient failure."}
    retry["request"]["arguments"] = {"justification": "The persisted throttling window has elapsed."}
    params = {key: _json(value) if isinstance(value, dict) else value for key, value in retry.items()}
    assert _eval(db, successor_predicate(), params)
    assert _json(retry["request"]["arguments"]) != _json(retry["retry_parent"]["request"]["arguments"])
    assert hashlib.sha256(_json(retry["request"]["arguments"]).encode()).digest() != hashlib.sha256(
        _json(retry["retry_parent"]["request"]["arguments"]).encode()).digest()
    for key in ("parameter_hash", "definition_hash", "configuration_hash"):
        changed = copy.deepcopy(retry)
        changed["request"][key] = "different-technical-identity"
        values = {name: _json(value) if isinstance(value, dict) else value for name, value in changed.items()}
        assert not _eval(db, successor_predicate(), values)
    body = _sql(build_permission_kernel(), "controller.reserve_action")
    assert "@full_arguments_hash" in body and "Full tool arguments contain invalid or extra technical fields" in body
    assert "SET @budget_debit=0" in body
    assert "$.request.arguments" not in successor_predicate()


def _correlation_db(db, kernel):
    table = kernel.names.table("monitoring_records")
    columns = ",".join(f"[{c}] {'BLOB' if c.endswith('_hash') else 'INTEGER' if c in ('revision','sequence_number') else 'TEXT'}"
                       for c in RECORD_COLUMNS)
    db.execute(f"CREATE TABLE {table} ({columns}, PRIMARY KEY(tenant_id,epoch,record_kind,key_hash))")
    db.create_function("JSON_EQUAL", 2, lambda a, b: int(a is not None and b is not None and json.loads(a) == json.loads(b)))
    db.create_function("KEY_DIGEST", 1, lambda value: hashlib.sha256(value.encode("utf-8")).digest())
    return table


def _adapt(kernel, statement):
    statement = statement.replace(kernel.names.object("json_equal"), "JSON_EQUAL")
    for variable in ("@correlated_key", "@reservation_id", "@stored_target_key"):
        statement = statement.replace(key_hash(variable), f"KEY_DIGEST({variable})")
    return re.sub(r"\bN'", "'", statement)


def _insert_action(db, table, key, action):
    target = action["request"]["source_execution"]["target"]
    db.execute(f"""INSERT INTO {table}
        (tenant_id,epoch,record_kind,key_hash,full_key,revision,status,payload)
        VALUES (?,?,'action',?,?,1,?,?)""", (
            target.get("tenant_id", "tenant"), target.get("epoch", "epoch"),
            hashlib.sha256(key.encode()).digest(), key, action["state"], _json(action),
        ))


def test_r1_to_manual_running_r2_is_not_non_effect_with_empty_index(db, actual_pipeline):
    kernel = build_permission_kernel()
    table = _correlation_db(db, kernel)
    pipeline, arguments = actual_pipeline
    now = datetime(2026, 9, 16, 12, tzinfo=UTC)
    target = m.TargetIdentity(
        tenant_id="44444444-4444-4444-8444-444444444444", epoch="55555555-5555-4555-8555-555555555555",
        workload="fabric_pipeline", workspace_id=pipeline.failure.target.workspace_id,
        item_id=pipeline.failure.target.pipeline_id,
    )
    r1 = m.SourceExecutionIdentity(target=target, run_id_kind="fabric_job", run_id=pipeline.failure.run.id)
    r2 = m.SourceExecutionIdentity(target=target, run_id_kind="fabric_job", run_id="66666666-6666-4666-8666-666666666666")
    work_id = "77777777-7777-4777-8777-777777777777"
    lease = m.LeaseToken(
        tenant_id=target.tenant_id, epoch=target.epoch, resource_key=m.work_key(target, work_id),
        owner_id="88888888-8888-4888-8888-888888888888", fence=1, acquired_at=now, expires_at=now+timedelta(minutes=5),
    )
    request = m.ActionReservationRequest(
        idempotency_id="99999999-9999-4999-8999-999999999999",
        expected=m.RegistryVersion(tenant_id=target.tenant_id, epoch=target.epoch, revision=1),
        work_id=work_id, lease=lease, source_execution=r1,
        incident=m.IncidentIdentity(target=target, signature="synthetic-signature"), expected_incident_revision=0,
        action="pipeline_rerun", review_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", expected_review_revision=1,
        definition_hash="a"*64, parameter_hash=pipeline.failure.target.parameter_hash, arguments=arguments,
        approval=m.ApprovalReference(approval_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd", fingerprint="b"*64),
    )
    action = m.ActionReservation(
        reservation_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", request=request, revision=2, fence=5,
        state="submitted", reserved_at=now, updated_at=now, submitted_execution=r2, submitted_at=now,
        next_verification_at=now+timedelta(minutes=1), detail="Exact owned R1-to-R2 submission.",
    )
    observation = m.SourceRunObservation(
        execution=r2, origin="poll", authority="rest", observed_at=now,
        started_at=now, status="running", invocation="manual", job_type="Pipeline",
    )
    _insert_action(db, table, action.reservation_id, action.model_dump(mode="json"))
    predicate = _adapt(kernel, execution_reservations_sql(kernel.names))
    params = {"tenant_id": target.tenant_id, "epoch": target.epoch, "execution": r2.model_dump_json()}
    assert db.execute(f"SELECT COUNT(*) FROM {table} WHERE record_kind='submitted_action'").fetchone()[0] == 0
    assert observation.status == "running" and observation.invocation == "manual"
    assert db.execute(predicate, params).fetchone()
    unrelated = m.SourceExecutionIdentity(
        target=target, run_id_kind="fabric_job", run_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    )
    assert not db.execute(predicate, {**params, "execution": unrelated.model_dump_json()}).fetchone()
    db.execute(f"UPDATE {table} SET status='verified_succeeded',payload=JSON_MODIFY(payload,'$.state','verified_succeeded') WHERE record_kind='action'")
    assert db.execute(predicate, params).fetchone()


def test_uncertain_to_exact_correlation_is_immutable_atomic_and_replayable(db):
    kernel = build_permission_kernel()
    table = _correlation_db(db, kernel)
    db.execute("CREATE TABLE dbo.receipts (request_id TEXT PRIMARY KEY,payload TEXT)")
    db.execute("CREATE TABLE dbo.budget_approval (slots INTEGER,consumed TEXT)")
    db.execute("INSERT INTO dbo.budget_approval VALUES (1,'original-consumption')")
    target = {"workload": "fabric_pipeline", "workspace_id": "workspace", "item_id": "pipeline"}
    r1 = {"target": target, "run_id_kind": "fabric_job", "run_id": "R1"}
    r2 = {**r1, "run_id": "R2"}
    action = {"state": "uncertain", "fence": 5, "request": {"source_execution": r1}, "submitted_execution": None}
    _insert_action(db, table, "action", action)
    db.execute("INSERT INTO dbo.receipts VALUES ('original-uncertain',?)", (_json(action),))
    db.commit()
    db.execute("""CREATE TRIGGER dbo.fail_correlation_receipt BEFORE INSERT ON receipts
        BEGIN SELECT RAISE(ABORT,'injected correlation receipt failure'); END""")

    def correlate(request_id):
        with db:
            receipt = db.execute("SELECT payload FROM dbo.receipts WHERE request_id=?", (request_id,)).fetchone()
            if receipt:
                return json.loads(receipt[0])
            params = {
                "tenant_id": "tenant", "epoch": "epoch", "reservation_id": "action",
                "correlated_execution": _json(r2), "correlated_key": "target:run:fabric_job:R2",
                "stored_target_key": "target", "correlation_payload": _json({"reservation_id": "action", "fence": 5, "active": True}),
            }
            if db.execute(_adapt(kernel, submitted_collision_sql(kernel.names)), params).fetchone():
                raise ValueError("another action already owns this execution")
            existing = db.execute(f"SELECT payload FROM {table} WHERE record_kind='submitted_action' AND full_key=@correlated_key", params).fetchone()
            if existing and json.loads(existing[0]) != json.loads(params["correlation_payload"]):
                raise ValueError("immutable correlation differs")
            if not existing:
                db.execute(_adapt(kernel, correlation_insert_sql(kernel.names)), params)
            result = {**action, "submitted_execution": r2}
            db.execute(f"UPDATE {table} SET revision=revision+1,payload=? WHERE record_kind='action' AND full_key='action'", (_json(result),))
            db.execute("INSERT INTO dbo.receipts VALUES (?,?)", (request_id, _json(result)))
        return result

    with pytest.raises(sqlite3.IntegrityError, match="correlation receipt failure"):
        correlate("exact-correlation")
    assert db.execute(f"SELECT COUNT(*) FROM {table} WHERE record_kind='submitted_action'").fetchone()[0] == 0
    assert json.loads(db.execute(f"SELECT payload FROM {table} WHERE record_kind='action'").fetchone()[0]) == action
    db.execute("DROP TRIGGER dbo.fail_correlation_receipt")
    result = correlate("exact-correlation")
    assert correlate("exact-correlation") == result
    assert correlate("original-uncertain") == action
    assert db.execute(f"SELECT COUNT(*) FROM {table} WHERE record_kind='submitted_action'").fetchone()[0] == 1
    assert db.execute("SELECT * FROM dbo.budget_approval").fetchone() == (1, "original-consumption")
    original_index = db.execute(f"SELECT payload FROM {table} WHERE record_kind='submitted_action'").fetchone()[0]
    db.execute(f"UPDATE {table} SET payload=? WHERE record_kind='submitted_action'",
               (_json({"reservation_id": "another", "fence": 5, "active": True}),))
    db.commit()
    with pytest.raises(ValueError, match="immutable correlation"):
        correlate("rebind-attempt")
    db.execute(f"UPDATE {table} SET payload=? WHERE record_kind='submitted_action'", (original_index,))
    _insert_action(db, table, "collision", {"state": "verified_failed", "request": {"source_execution": r1}, "submitted_execution": r2})
    db.commit()
    with pytest.raises(ValueError, match="another action"):
        correlate("new-request")


def test_generated_submission_path_records_correlation_before_action_receipt():
    kernel = build_permission_kernel()
    transition = _sql(kernel, "controller.transition_action")
    assert transition.index("Submitted execution identity cannot change") < transition.index("DECLARE @correlated_execution")
    assert transition.index("DECLARE @correlated_execution") < transition.index("SET revision=revision+1,status=@transition")
    assert "Submitted correlation is immutable and cannot be rebound" in transition
    assert "even without an index" in transition
    assert transition.index("IF @prior_payload IS NOT NULL") < transition.index("DECLARE @correlated_execution")
    assert "SET XACT_ABORT ON" in transition
    assert "A terminal action cannot be reopened" in transition
    disposition = _sql(kernel, "controller.disposition_source")
    assert "JSON_QUERY(reservation.payload,'$.submitted_execution')" in disposition
    assert "JSON_QUERY(reservation.payload,'$.request.source_execution')" in disposition
