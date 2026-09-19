"""Execute the emitted readiness predicate offline, not a native SQL proof."""

from __future__ import annotations

import json
import re

import pytest
from test_monitoring_sql_removals import (
    _adapt,
    _guard,
    _hash,
    _id,
    _insert_record,
    _json,
    _mutation_params,
    _save_receipt,
)
from test_monitoring_sql_removals import (
    case as case,
)
from test_monitoring_sql_removals import (
    db as db,
)

from triage.monitoring.sql_kernel_common import payload_hash, record_hash
from triage.monitoring.sql_kernel_connectors import (
    recovered_readiness_time_current_sql,
    require_delivery_proof_sql,
)
from triage.monitoring.sql_kernel_contracts import RECORD_COLUMNS
from triage.monitoring.sql_permissions import build_permission_kernel


def _row_hash(*values):
    encoded = []
    for value in values:
        if value is None:
            encoded.append("-" * 64)
        else:
            assert isinstance(value, (str, int, bytes))
            encoded.append(_hash(value.hex().upper() if isinstance(value, bytes) else str(value)))
    return _hash("".join(encoded))


def _sql(kernel, expression):
    columns = ",".join(f"fact.[{column}]" for column in RECORD_COLUMNS)
    expression = expression.replace(record_hash("fact"), f"SQL_ROW_HASH({columns})")
    for value in ("@delivery_signal", "fact.payload", "JSON_QUERY(@prior,'$.desired_definition')"):
        expression = expression.replace(payload_hash(value), f"PAYLOAD_HASH({value})")
    expression = expression.replace("TRY_CONVERT(datetimeoffset,", "TRY_CONVERT('datetimeoffset',")
    return _adapt(kernel, expression, concatenate=True)


@pytest.fixture
def readiness(db, case):
    kernel, prior = case
    db.create_function("SQL_ROW_HASH", len(RECORD_COLUMNS), _row_hash)
    batch_id, receipt_key, partition_key = _id(800), "synthetic-readiness-signal", "synthetic-readiness-partition"
    identity_at, enqueued_at, received_at = (
        "2026-09-16T12:04:00Z", "2026-09-16T12:05:00Z", "2026-09-16T12:06:00Z",
    )
    source = prior["sources"][1]
    partition = {
        "tenant_id": _id(1), "epoch": _id(2), "connector_id": _id(3),
        "consumer_group": "$Default", "partition_id": "0",
    }
    proof = {
        "request_id": batch_id, "receipt_key": receipt_key, "collector_identity_id": _id(801),
        "received_at": received_at, "identity_verified_at": identity_at,
    }
    signal = {
        "status": "accepted", "received_at": received_at, "partition": partition,
        "delivery": {"tenant_id": _id(1), "epoch": _id(2), "connector_id": _id(3),
                     "event_source": source["event_source"], "event_id": "readiness-event"},
        "event_type": "Microsoft.Fabric.JobEvents.ItemJobFailed",
        "observation": {"execution": {"target": source["target"]}},
        "position": {"sequence_number": 4, "offset": "4", "enqueued_at": enqueued_at},
        "transport": {
            "request_id": batch_id, "ownership_id": _id(4), "policy_revision": 3,
            "definition_hash": _hash(_json(prior["desired_definition"])), "collector_identity_id": _id(801),
            "workspace_id": prior["workspace_id"], "eventstream_id": prior["eventstream_id"],
            "destination_id": prior["destination_id"], "endpoint": prior["endpoint"],
            "identity_verified_at": identity_at, "source_id": source["source_id"],
        },
    }
    signal_hash = _hash(_json(signal))
    _insert_record(db, kernel, "signal", receipt_key, signal)
    records = kernel.names.table("monitoring_records")
    db.execute(f"UPDATE {records} SET status='accepted' WHERE record_kind='signal'")
    columns = ",".join(f"[{column}]" for column in RECORD_COLUMNS)
    row = db.execute(f"SELECT {columns} FROM {records} WHERE record_kind='signal'").fetchone()
    _insert_record(db, kernel, "accepted_fact", "synthetic-readiness-acceptance", {
        "batch_id": batch_id, "fact_key": receipt_key, "fact_kind": "signal", "fact_revision": 1,
        "payload_hash": signal_hash, "row_hash": _row_hash(*row),
    })
    _insert_record(db, kernel, "stream_position", partition_key + ":position:4", {
        "batch_id": batch_id, "payload_hash": signal_hash, "receipt_key": receipt_key,
        "offset": "4", "enqueued_at": enqueued_at,
    })
    db.execute(f"UPDATE {records} SET status='accepted',parent_key=?,sequence_number=4 "
               "WHERE record_kind='stream_position'", (partition_key,))
    batch = {
        "batch_id": batch_id, "partition": partition, "partition_key": partition_key,
        "positions": [{
            "sequence_number": 4, "receipt_kind": "identified", "receipt_key": receipt_key,
            "offset": "4", "enqueued_at": enqueued_at, "first_committed_batch_id": batch_id,
            "original_payload_hash": signal_hash,
        }],
    }
    _save_receipt(db, kernel, "worker.commit_positions", {
        **_mutation_params(prior, batch_id), "now": "2026-09-16T12:06:01Z",
    }, batch)
    desired = {"ownership_id": _id(4), "policy_revision": 3, "published_at": "2026-09-16T12:02:00Z",
               "supersession_request_id": _id(802)}
    _insert_record(db, kernel, "connector_desired", _id(3), desired)
    capability = {
        "target": source["target"], "collector_identity_id": _id(801),
        "read_status": "verified", "event_status": "verified",
        "checked_at": "2026-09-16T12:03:00Z", "expires_at": "2026-09-16T13:00:00Z",
    }
    _insert_record(db, kernel, "target_capability", "synthetic-readiness-target", capability)
    params = {
        **_mutation_params(prior, _id(803)), "now": "2026-09-16T12:10:00Z", "prior": _json(prior),
        "ready_observation": _json({"delivery_proof": proof, "identity_verified_at": identity_at,
                                    "delivery_verified_at": received_at}),
        "delivery_proof": _json(proof), "delivery_signal": _json(signal),
    }
    db.commit()
    return kernel, params


def _readiness_denied(db, kernel, params, *, predicate=None):
    body = require_delivery_proof_sql(kernel.names, "@ready_observation")
    batch_select = re.search(
        r"SELECT @delivery_batch=payload,@delivery_batch_recorded_at=recorded_at (FROM .*?);", body, re.S,
    )[1]
    row = db.execute("SELECT payload,recorded_at " + _sql(kernel, batch_select), params).fetchone()
    desired = db.execute(f"SELECT payload FROM {kernel.names.table('monitoring_records')} "
                         "WHERE record_kind='connector_desired'").fetchone()[0]
    values = {**params, "delivery_batch": row[0] if row else None,
              "delivery_batch_recorded_at": row[1] if row else None, "delivery_desired": desired}
    if predicate is None:
        predicate = _guard(body, "Readiness requires the original current accepted transport receipt")
    return db.execute("SELECT CASE WHEN (" + _sql(kernel, predicate) + ") THEN 1 ELSE 0 END", values).fetchone()[0] == 1


@pytest.mark.parametrize("checked_at", [
    "2026-09-16T12:01:59.999999Z", "2026-09-16T12:10:00.000001Z", None,
])
def test_recovered_readiness_requires_current_capability_checked_after_publication(db, readiness, checked_at):
    kernel, params = readiness
    assert not _readiness_denied(db, kernel, params)
    records = kernel.names.table("monitoring_records")
    db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.checked_at',?) "
               "WHERE record_kind='target_capability'", (checked_at,))
    assert _readiness_denied(db, kernel, params)


@pytest.mark.parametrize("recorded_at", [
    "2026-09-16T12:01:59.999999Z", "2026-09-16T12:10:00.000001Z", None,
])
def test_recovered_readiness_uses_original_native_batch_time_not_reported_event_times(db, readiness, recorded_at):
    kernel, params = readiness
    receipts = kernel.names.table("monitoring_receipts")
    before = db.execute(f"SELECT payload FROM {receipts} WHERE operation='worker.commit_positions'").fetchone()[0]
    assert not _readiness_denied(db, kernel, params)
    db.execute(f"UPDATE {receipts} SET recorded_at=? WHERE operation='worker.commit_positions'", (recorded_at,))
    assert _readiness_denied(db, kernel, params)
    assert db.execute(f"SELECT payload FROM {receipts} WHERE operation='worker.commit_positions'").fetchone()[0] == before


def test_future_reported_times_cannot_relicense_a_pre_recovery_batch_when_clock_catches_up(db, readiness):
    kernel, params = readiness
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    db.execute(f"UPDATE {receipts} SET recorded_at='2026-09-16T12:00:00Z' WHERE operation='worker.commit_positions'")
    db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.checked_at','2026-09-16T11:59:00Z') "
               "WHERE record_kind='target_capability'")
    old_predicate = _guard(require_delivery_proof_sql(kernel.names, "@ready_observation"),
                           "Readiness requires the original current accepted transport receipt")
    old_predicate = old_predicate.replace(
        "OR NOT " + recovered_readiness_time_current_sql("TODATETIMEOFFSET(@delivery_batch_recorded_at,'+00:00')"), "",
    ).replace(
        "AND " + recovered_readiness_time_current_sql("TRY_CONVERT(datetimeoffset,JSON_VALUE(capability.payload,'$.checked_at'))"), "",
    )
    assert not _readiness_denied(db, kernel, params, predicate=old_predicate)
    assert _readiness_denied(db, kernel, params)
    db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.checked_at','2026-09-16T12:03:00Z') "
               "WHERE record_kind='target_capability'")
    assert _readiness_denied(db, kernel, params)
    assert _readiness_denied(db, kernel, {**params, "now": "2026-09-16T12:30:00Z"})


@pytest.mark.parametrize("timestamp", ["2026-09-16T12:02:00Z", "2026-09-16T12:10:00Z"])
def test_recovered_readiness_accepts_exact_publication_and_current_time_boundaries(db, readiness, timestamp):
    kernel, params = readiness
    db.execute(f"UPDATE {kernel.names.table('monitoring_receipts')} SET recorded_at=? "
               "WHERE operation='worker.commit_positions'", (timestamp,))
    db.execute(f"UPDATE {kernel.names.table('monitoring_records')} SET payload=json_set(payload,'$.checked_at',?) "
               "WHERE record_kind='target_capability'", (timestamp,))
    assert not _readiness_denied(db, kernel, params)


def test_new_time_checks_do_not_change_ordinary_unmarked_readiness(db, readiness):
    kernel, params = readiness
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    db.execute(f"UPDATE {records} SET payload=json_remove(payload,'$.supersession_request_id') "
               "WHERE record_kind='connector_desired'")
    db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.checked_at','2026-09-16T11:59:00Z') "
               "WHERE record_kind='target_capability'")
    db.execute(f"UPDATE {receipts} SET recorded_at='2026-09-16T12:00:00Z' WHERE operation='worker.commit_positions'")
    assert not _readiness_denied(db, kernel, params)


@pytest.mark.parametrize("fault", ["collector", "endpoint", "definition", "policy", "source", "position", "event", "row_hash"])
def test_recovered_readiness_keeps_existing_exact_transport_and_acceptance_bindings(db, readiness, fault):
    kernel, params = readiness
    records = kernel.names.table("monitoring_records")
    if fault == "row_hash":
        db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.row_hash',?) "
                   "WHERE record_kind='accepted_fact'", ("F" * 64,))
    elif fault in {"collector", "event"}:
        if fault == "collector":
            db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.collector_identity_id',?) "
                       "WHERE record_kind='target_capability'", (_id(999),))
        else:
            signal = json.loads(params["delivery_signal"])
            signal["event_type"] = "Microsoft.Fabric.JobEvents.ItemJobSucceeded"
            params = {**params, "delivery_signal": _json(signal)}
    else:
        signal = json.loads(params["delivery_signal"])
        path, value = {
            "endpoint": ("endpoint", {"namespace": "different.invalid", "entity": "owned", "consumer_group": "$Default"}),
            "definition": ("definition_hash", "F" * 64), "policy": ("policy_revision", 2),
            "source": ("source_id", _id(999)), "position": ("source_id", _id(12)),
        }[fault]
        if fault == "position":
            signal["position"]["sequence_number"] = 5
        else:
            signal["transport"][path] = value
        params = {**params, "delivery_signal": _json(signal)}
    assert _readiness_denied(db, kernel, params)


def test_both_readiness_rpcs_keep_original_receipt_replay_before_new_freshness_guards():
    kernel = build_permission_kernel()
    batch_check = recovered_readiness_time_current_sql("TODATETIMEOFFSET(@delivery_batch_recorded_at,'+00:00')")
    cap_check = recovered_readiness_time_current_sql("TRY_CONVERT(datetimeoffset,JSON_VALUE(capability.payload,'$.checked_at'))")
    for operation in ("worker.observe_connector", "controller.publish_connector"):
        ddl = next(obj.ddl for obj in kernel.objects if obj.logical_name == operation)
        replay_end = ddl.index("RETURN;", ddl.index("IF @prior_payload IS NOT NULL"))
        assert replay_end < ddl.index("SELECT @delivery_batch=payload,@delivery_batch_recorded_at=recorded_at")
        assert replay_end < ddl.index(batch_check)
        assert cap_check in ddl
        assert "events_enabled" not in require_delivery_proof_sql(kernel.names, "@ready_observation")
