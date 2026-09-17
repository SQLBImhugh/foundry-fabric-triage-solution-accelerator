"""Post-binding ABI invariants; native MI/role/procedure proof remains separate."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from uuid import UUID

import pytest
from test_monitoring_sql_retry_finalization import _eval, _json
from test_monitoring_sql_retry_finalization import db as db
from test_monitoring_sql_window_siblings import _transition, _typed_query
from test_monitoring_sql_window_siblings import sibling_case as sibling_case

from triage.monitoring.sql_kernel_common import canonical_guid, key_hash
from triage.monitoring.sql_kernel_contracts import RECORD_COLUMNS
from triage.monitoring.sql_kernel_frontiers import closed_window_authority_sql
from triage.monitoring.sql_kernel_retention import retention_classification_sql
from triage.monitoring.sql_kernel_sources import disposition_records_sql, stale_source_policy_sql
from triage.monitoring.sql_permissions import (
    KERNEL_VERSION,
    build_permission_kernel,
    decode_rpc_result,
)


def _sql(kernel, name):
    return next(o.ddl for o in kernel.objects if o.logical_name == name)


def _hash(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


@pytest.mark.parametrize("action,parameters,args", [
    ("powerbi_refresh", None, {"justification": "Transient failure"}),
    ("powerbi_refresh", {}, {"justification": "Transient failure"}),
    ("rebind_dataset_gateway", {"gateway_id": "11111111-1111-4111-8111-111111111111",
                               "datasource_ids": ["22222222-2222-4222-8222-222222222222"]},
     {"justification": "Approved binding", "configuration": {
         "gateway_id": "11111111-1111-4111-8111-111111111111",
         "datasource_ids": ["22222222-2222-4222-8222-222222222222"]}}),
    ("reenable_refresh_schedule", {"enabled": True},
     {"justification": "Latest refresh succeeded", "configuration": {"enabled": True}}),
])
def test_action_binding_preserves_full_arguments_and_separate_hash_domains(action, parameters, args):
    kernel = build_permission_kernel()
    request = {"action": action, "arguments": args, "parameter_hash": _hash(parameters)}
    original = _json({"request": request, "incident_id": "incident"})
    rpc = kernel.rpcs["controller.reserve_action"]
    values = {p.name: 1 if p.sql_type == "bigint" else "11111111-1111-4111-8111-111111111111" for p in rpc.parameters}
    values.update(fingerprint="a" * 64, reservation_json=original)
    _, bound = rpc.bind(values)
    assert bound[-1] == original
    assert request["arguments"] == args
    assert request["parameter_hash"] != _hash(args)
    body = _sql(kernel, "controller.reserve_action")
    assert "canonical_action_arguments" in body
    assert "$.arguments_hash" in body and "@full_arguments_hash" in body
    assert "Approval must retain every original tool argument" in body
    assert "@technical_arguments" in body and "@canonical_arguments" in body
    assert "JSON_MODIFY(@request,'$.arguments'" not in body


def test_argument_sql_has_closed_action_shapes_and_null_empty_are_not_coerced():
    kernel = build_permission_kernel()
    fn = _sql(kernel, "canonical_action_arguments")
    assert "'justification','parameter_hash','configuration'" in fn
    assert "@action='powerbi_refresh'" in fn and "[key]<>'justification'" in fn
    assert "@action='pipeline_rerun'" in fn and '"parameter_preview":' in fn
    assert "'gateway_id','datasource_ids'" in fn and "datasource_ids" in fn
    assert "type=3 AND value='true'" in fn
    assert "DATALENGTH(@arguments)>131072" in fn
    reserve = _sql(kernel, "controller.reserve_action")
    assert "CASE WHEN @review_parameters IS NULL THEN N'null' ELSE N'{}' END" in reserve
    assert hashlib.sha256(b"null").digest() != hashlib.sha256(b"{}").digest()
    assert "AS @full_arguments_hash" not in reserve
    assert "TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.decided_at'))" in reserve
    assert "SET @budget_debit=0" in reserve


def test_partition_tombstone_and_start_checkpoint_results_are_complete():
    kernel = build_permission_kernel()
    partition = _sql(kernel, "worker.partition")
    for required in ("AS partition", "AS last_owner_id", "AS last_fence", "AS etag", "AS start"):
        assert required in partition
    assert "@next_fence AS last_fence" in partition
    assert "CASE WHEN @transition='release' THEN NULL" in partition
    assert "unobserved_stream_history" in partition
    checkpoint = _sql(kernel, "worker.advance_checkpoint")
    assert "@last_enqueued_at=JSON_VALUE(payload,'$.enqueued_at')" in checkpoint
    assert "AS position" in checkpoint and "AS partition" in checkpoint
    for operation in ("worker.partition", "worker.advance_checkpoint"):
        assert _sql(kernel, operation).index("IF @prior_payload IS NOT NULL") < _sql(kernel, operation).index("DECLARE @partition_digest")


@pytest.mark.parametrize("first,expected,code", [
    (110, 104, "stream_retention_gap"), (104, 104, None), (99, 104, "stream_boundary_regressed"),
])
def test_retention_classifier_uses_uncheckpointed_position_without_moving_state(db, first, expected, code):
    assert db.execute("SELECT " + retention_classification_sql(), {
        "first_available_sequence_number": first, "expected_sequence": expected, "pinned": 100,
    }).fetchone()[0] == code
    body = _sql(build_permission_kernel(), "worker.observe_retention")
    assert "COALESCE(@checkpoint_sequence+1,@pinned)" in body
    assert "first_available_sequence_number-1" in body
    assert "UPDATE [dbo].[triage_monitoring_records] SET revision=revision+1,payload=JSON_MODIFY(payload,'append $.gaps'" in body
    assert "SET sequence_number=" not in body
    assert "UPDATE [dbo].[triage_monitoring_leases]" not in body
    assert "record_kind='stream_checkpoint'" in body
    assert "Current policy revision differs" in body


def test_original_position_mapping_is_in_receipt_result_not_recovered_from_new_batch_filter():
    kernel = build_permission_kernel()
    body = _sql(kernel, "worker.commit_positions")
    assert "AS receipt_keys" in body and "AS positions" in body
    assert "AS first_committed_batch_id" in body and "AS original_payload_hash" in body
    assert "ORDER BY p.sequence_number FOR JSON PATH" in body
    assert "WITHIN GROUP (ORDER BY sequence_number)" in body
    assert body.index("AS first_committed_batch_id") > body.index("SET @result=")
    assert {"partition", "positions", "receipt_keys"} <= set(kernel.rpcs["worker.commit_positions"].result_fields)


@pytest.mark.parametrize("maintenance,current,expected,effect,blocked", [
    (0, 2, 2, None, False), (1, 2, 2, None, True), (0, 3, 2, None, True),
    (1, 3, 2, "reserved-effect", False),
])
def test_existing_effect_can_fresh_read_under_policy_change_but_new_work_cannot(db, maintenance, current, expected, effect, blocked):
    assert bool(_eval(db, stale_source_policy_sql(), {
        "maintenance": maintenance, "current_revision": current,
        "expected_revision": expected, "existing_effect": effect,
    })) is blocked
    body = _sql(build_permission_kernel(), "controller.publish_source")
    assert "Fresh source publication lost its actual controller target lease" in body
    assert "Source observation is not in this work window accepted receipt set" in body
    assert "Source observation revision changed during publication" in body
    assert "Source head compare-and-set failed" in body
    assert "IF @prior_payload IS NOT NULL" in body


def test_no_alternate_source_head_or_processed_disposition_write_view_exists():
    kernel = build_permission_kernel()
    for view in ("controller_projections", "controller_immutable", "worker_evidence", "web_drafts"):
        text = _sql(kernel, view)
        assert "N'source'" not in text and "N'source_head'" not in text and "N'source_disposition'" not in text
    body = _sql(kernel, "controller.disposition_source")
    assert "Source disposition and processed marker disagree" in body
    assert "An existing" not in body or "reserved" in body
    assert "A reserved source retains incident verification/finalization" in body
    assert "INSERT INTO [dbo].[triage_processed_messages]" in body
    assert body.index("'source_disposition',HASHBYTES") < body.index("INSERT INTO [dbo].[triage_processed_messages]")
    assert "Unclaimed source-work disposition lost its compare-and-set" in body
    assert "UPDATE [dbo].[triage_approvals]" not in body
    assert "record_kind='incident_state'" not in body


def test_non_effect_pair_and_receipt_fail_atomically_without_incident_or_action_mutation(db):
    kernel = build_permission_kernel()
    records, processed = kernel.names.table("monitoring_records"), kernel.names.table("processed")
    columns = ",".join(f"[{name}] {'BLOB' if name.endswith('_hash') else 'TEXT'}" for name in RECORD_COLUMNS)
    db.execute(f"CREATE TABLE {records} ({columns})")
    db.execute(f"CREATE TABLE {processed} (fingerprint TEXT PRIMARY KEY,message_id TEXT,received_at TEXT)")
    db.execute("CREATE TABLE dbo.receipts (request_id TEXT PRIMARY KEY,payload TEXT)")
    db.execute("CREATE TABLE dbo.actions (state TEXT,budget INTEGER,approval TEXT)")
    db.execute("INSERT INTO dbo.actions VALUES ('uncertain',1,'consumed')")
    db.create_function("KEY_DIGEST", 1, lambda value: hashlib.sha256(value.encode("utf-8")).digest())
    db.create_function("KEY_HEX", 1, lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest())
    statements = list(disposition_records_sql(kernel.names))
    marker = f"LOWER(CONVERT(char(64),{key_hash('@source_key')},2))"
    statements[1] = statements[1].replace(marker, "KEY_HEX(@source_key)")
    for index, statement in enumerate(statements):
        for parameter in ("@source_key", "@source_target_key"):
            statement = statement.replace(key_hash(parameter), f"KEY_DIGEST({parameter})")
        statements[index] = re.sub(r"\bN'", "'", statement)
    params = {
        "tenant_id": "tenant", "epoch": "epoch", "source_key": "source", "source_target_key": "target",
        "disposition": "out_of_scope", "prior_disposition": _json({"execution": {"run_id": "source"}, "disposition": "out_of_scope"}),
        "recorded_at_text": "2026-09-16T12:00:00Z",
    }
    db.commit()
    db.execute("""CREATE TRIGGER dbo.fail_disposition BEFORE INSERT ON receipts
        BEGIN SELECT RAISE(ABORT,'injected disposition receipt failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="disposition receipt failure"):
        with db:
            for statement in statements:
                db.execute(statement, params)
            db.execute("INSERT INTO dbo.receipts VALUES ('request','original')")
    assert db.execute(f"SELECT COUNT(*) FROM {records}").fetchone()[0] == 0
    assert db.execute(f"SELECT COUNT(*) FROM {processed}").fetchone()[0] == 0
    assert db.execute("SELECT * FROM dbo.actions").fetchone() == ("uncertain", 1, "consumed")
    db.execute("DROP TRIGGER dbo.fail_disposition")
    with db:
        for statement in statements:
            db.execute(statement, params)
        db.execute("INSERT INTO dbo.receipts VALUES ('request','original')")
    assert db.execute(f"SELECT COUNT(*) FROM {records}").fetchone()[0] == 1
    assert db.execute(f"SELECT COUNT(*) FROM {processed}").fetchone()[0] == 1


def test_original_review_time_is_preserved_and_expired_revocation_remains_deny_only():
    body = _sql(build_permission_kernel(), "web.commit_intent")
    assert "'reviewer_id','reviewed_at'" in body
    assert "Revocation must retain the original review and expiry timestamps" in body
    assert "JSON_QUERY(@intent_json) AS original_intent" in body
    assert "'pending-validation'" in body
    assert "'$.requested_state')<>'revoked'" in body
    assert "SET @intent_json=JSON_MODIFY" not in body
    assert "'unverifiable'" in body


def test_logical_source_binding_select_uses_returned_identity_not_node_name(db):
    kernel = build_permission_kernel()
    body = _sql(kernel, "controller.publish_connector")
    select = re.search(r"INSERT INTO @bound (SELECT .*?);", body, re.S)[1]
    select = select.replace("OPENJSON(", "json_each(").replace("ids.type=1", "ids.type='text'")
    select = select.replace(canonical_guid("ids.value"), "IS_GUID(ids.value)=1")
    select = select.replace("@pending_removals", "pending_removals")
    select = re.sub(r"\bN'", "'", select)
    select = select.replace("'sources/'+", "'sources/' || ")
    db.create_function("DATALENGTH", 1, lambda value: None if value is None else len(value.encode("utf-16-le")))
    def is_guid(value):
        try:
            return value == str(UUID(value)) and UUID(value).int != 0
        except (ValueError, TypeError):
            return False
    db.create_function("IS_GUID", 1, is_guid)
    db.execute("CREATE TABLE pending_removals (proposal_id TEXT)")
    proposal = {"proposal_id": "11111111-1111-4111-8111-111111111111", "node_name": "owned-node",
                "source_id": None, "target": {"item_id": "item"}, "event_types": ["event"]}
    params = {"proposals": _json([proposal]), "binding_observation": _json({"observed_definition": {"component_ids": {}}})}
    assert db.execute(select, params).fetchall() == []
    observed = {"observed_definition": {"component_ids": {"sources/owned-node": "owned-node"}}}
    assert db.execute(select, {**params, "binding_observation": _json(observed)}).fetchall() == []
    returned = "22222222-2222-4222-8222-222222222222"
    observed["observed_definition"]["component_ids"]["sources/owned-node"] = returned
    rows = db.execute(select, {**params, "binding_observation": _json(observed)}).fetchall()
    assert len(rows) == 1 and rows[0][2] == returned
    bound = json.loads(rows[0][3])
    assert bound["source_id"] == returned and "proposal_id" not in bound and "node_name" not in bound
    assert proposal["source_id"] is None
    db.execute("INSERT INTO pending_removals VALUES (?)", (proposal["proposal_id"],))
    assert db.execute(select, {**params, "binding_observation": _json(observed)}).fetchall() == []
    assert "New physical sources require receipt-bound proposal resolution, not caller IDs" in body
    assert "Existing" not in body or "proposals" in body


def test_published_window_sibling_ack_is_published_not_rejection(db, sibling_case):
    kernel, params, receipt = sibling_case
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    db.execute(f"UPDATE {records} SET status='published' WHERE record_kind IN ('validation_frontier','frontier_commit') AND full_key='window'")
    db.execute(f"UPDATE {records} SET status='validated' WHERE record_kind='validation_window' AND full_key='window'")
    old = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE request_id='A-window-rejection'").fetchone()[0])
    old["result"].update(state="published", resolution_scope="handoff")
    db.execute(f"UPDATE {receipts} SET payload=? WHERE request_id='A-window-rejection'", (_json(old),))
    db.commit()
    authority = db.execute(_typed_query(closed_window_authority_sql(kernel.names)), params).fetchone()
    assert authority == ("A-window-rejection", "published")
    preserved = db.execute(f"SELECT * FROM {records} WHERE record_kind<>'work' ORDER BY record_kind,full_key").fetchall()
    result = {
        "work_id": params["work_id"], "work_fence": params["fence"], "frontier_key": params["frontier_key"],
        "validated_revision": params["accepted"], "state": "published", "resolution_scope": "window_acknowledgement",
        "window_resolution_request_id": authority[0], "window_resolution_state": authority[1],
        "window_rejection_request_id": None,
    }
    receipt("controller.resolve_frontier", "B-published-ack", result)
    db.commit()
    assert _transition(db, kernel, params, "complete", "B-completed")["state"] == "completed"
    assert db.execute(f"SELECT * FROM {records} WHERE record_kind<>'work' ORDER BY record_kind,full_key").fetchall() == preserved
    assert db.execute(f"SELECT payload FROM {receipts} WHERE request_id='B-pending'").fetchone()[0].find("pending_validation")>=0


def test_new_rpc_matrix_and_heartbeat_stopping_do_not_widen_roles():
    kernel = build_permission_kernel()
    assert KERNEL_VERSION == 2
    assert kernel.rpcs["controller.publish_source"].components == ("controller",)
    assert kernel.rpcs["controller.disposition_source"].components == ("controller",)
    assert kernel.rpcs["worker.observe_retention"].components == ("worker",)
    assert "'stopping'" in _sql(kernel, "worker.record_heartbeat")
    assert kernel.unresolved_cases == ()
    for rpc in kernel.rpcs.values():
        body = _sql(kernel, rpc.operation)
        assert f"SELECT {KERNEL_VERSION} AS kernel_version" in body
    with pytest.raises(ValueError, match="version"):
        decode_rpc_result(kernel.rpcs["lock_context"], [(_json({
            "kernel_version": 1, "operation": "lock_context", "status": "read", "affected_rows": 0, "result": {},
        }),)])
