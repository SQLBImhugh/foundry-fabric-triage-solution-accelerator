"""Review9 counterexamples using the generated SQL predicates and CAS writes."""

from __future__ import annotations

import copy
import json
import re

import pytest
from test_monitoring_sql_retry_finalization import _eval, _json, _modify
from test_monitoring_sql_retry_finalization import db as db

from triage.monitoring.sql_kernel_common import exact_text_equal, receipt_content_expression
from triage.monitoring.sql_kernel_connectors import (
    PUBLICATION_FIELDS,
    desired_update_expression,
    invalidate_readiness_expression,
    publish_update_sql,
    restore_worker_proof_expression,
    source_authorized_predicate,
    subscription_type_sql,
    worker_ready_upgrade_sql,
)
from triage.monitoring.sql_kernel_frontiers import (
    close_frontier_sql,
    frontier_can_close_sql,
    handoff_decision_change_sql,
    page_publication_required_sql,
    stale_page_policy_sql,
)
from triage.monitoring.sql_permissions import build_permission_kernel, integration_contract


def _sql(kernel, name):
    return next(obj.ddl for obj in kernel.objects if obj.logical_name == name)


def test_published_partial_page_can_be_rejected_as_a_window_after_policy_advances(db):
    kernel = build_permission_kernel()
    records = kernel.names.table("monitoring_records")
    db.execute(f"""CREATE TABLE {records} (
        tenant_id TEXT,epoch TEXT,record_kind TEXT,full_key TEXT,revision INTEGER,
        sequence_number INTEGER,status TEXT,payload TEXT)""")
    db.execute("CREATE TABLE dbo.control (epoch TEXT,revision INTEGER)")
    db.execute("INSERT INTO dbo.control VALUES ('epoch',2)")
    db.execute("CREATE TABLE dbo.leases (work_id TEXT,owner TEXT,fence INTEGER)")
    db.execute("INSERT INTO dbo.leases VALUES ('work','owner',8)")
    db.execute("CREATE TABLE dbo.actions (state TEXT,fence INTEGER)")
    db.execute("INSERT INTO dbo.actions VALUES ('uncertain',19)")
    db.execute("CREATE TABLE dbo.receipts (request_id TEXT PRIMARY KEY,payload TEXT)")
    db.execute("INSERT INTO dbo.receipts VALUES ('old-publication','pending_validation')")
    db.executemany(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?)", [
        ("tenant", "epoch", "validation_frontier", "frontier", 1, 1, "pending_validation",
         _json({"accepted_revision": 1, "validated_revision": 0})),
        ("tenant", "epoch", "validation_handoff", "page", 2, 1, "published", _json({"policy_revision": 1})),
        ("tenant", "epoch", "validation_window", "frontier", 1, 1, "collecting", _json({"collection_complete": False})),
    ])
    db.commit()
    params = {
        "whole_window_rejection": 1, "window_ack": 0, "handoff_ack": 0, "prefix_committed": 1, "all_resolved": 1,
        "decision": "rejected", "handoff_state": "published", "maintenance": 0,
        "current_revision": 2, "handoff": _json({"policy_revision": 1}),
        "window": _json({"collection_complete": False}), "accepted": 1,
        "proof": _json({"decision": "rejected", "reject_whole_window": True}),
    }
    assert _eval(db, stale_page_policy_sql(), params)
    assert not _eval(db, page_publication_required_sql(), params)
    assert not _eval(db, handoff_decision_change_sql(), params)
    assert _eval(db, frontier_can_close_sql(), params)
    assert not _eval(db, frontier_can_close_sql(), {**params, "prefix_committed": 0})
    assert _eval(db, page_publication_required_sql(), {**params, "whole_window_rejection": 0, "decision": "published"})
    before_control = db.execute("SELECT * FROM dbo.control").fetchall()
    before_action = db.execute("SELECT * FROM dbo.actions").fetchall()
    before_handoff = db.execute(f"SELECT payload,status FROM {records} WHERE record_kind='validation_handoff'").fetchall()

    def reject(request_id, epoch="epoch", fence=8):
        with db:
            receipt = db.execute("SELECT payload FROM dbo.receipts WHERE request_id=?", (request_id,)).fetchone()
            if receipt:
                return receipt[0]
            if db.execute("SELECT epoch FROM dbo.control").fetchone()[0] != epoch:
                raise ValueError("epoch changed")
            if db.execute("SELECT fence FROM dbo.leases WHERE work_id='work' AND owner='owner'").fetchone()[0] != fence:
                raise ValueError("lease lost")
            count = db.execute(close_frontier_sql(kernel.names), {
                "tenant_id": "tenant", "epoch": epoch, "frontier_key": "frontier",
                "expected_frontier_revision": 1, "validated": 1, "window_decision": "rejected",
                "resolved_at": "2026-09-16T12:00:00Z",
            }).rowcount
            if count != 1:
                raise ValueError("frontier CAS")
            db.execute(f"UPDATE {records} SET status='rejected' WHERE record_kind='validation_window'")
            db.execute("INSERT INTO dbo.receipts VALUES (?, 'rejected')", (request_id,))
        return "rejected"

    with pytest.raises(ValueError, match="lease"):
        reject("bad-lease", fence=7)
    with pytest.raises(ValueError, match="epoch"):
        reject("bad-epoch", epoch="old-epoch")
    assert reject("old-publication") == "pending_validation"
    assert reject("whole-window-rejection") == "rejected"
    assert reject("whole-window-rejection") == "rejected"
    assert db.execute("SELECT * FROM dbo.control").fetchall() == before_control
    assert db.execute("SELECT * FROM dbo.actions").fetchall() == before_action
    assert db.execute(f"SELECT payload,status FROM {records} WHERE record_kind='validation_handoff'").fetchall() == before_handoff


def _opaque_setup(db):
    db.create_function("DATALENGTH", 1, lambda value: None if value is None else len(value.encode("utf-16-le")))
    db.create_collation("SQL_CI", lambda a, b: (a.casefold().rstrip(" ") > b.casefold().rstrip(" ")) -
                        (a.casefold().rstrip(" ") < b.casefold().rstrip(" ")))
    # SQL pads text comparisons even under BIN2. Length is part of the new predicate.
    db.create_collation("Latin1_General_100_BIN2", lambda a, b:
                        (a.rstrip(" ") > b.rstrip(" ")) - (a.rstrip(" ") < b.rstrip(" ")))


@pytest.mark.parametrize("left,right", [
    ("Event-A", "event-a"), ("event-a", "event-a "), ("Source-A", "source-a"),
    ("a\t", "a"), ("a", "a\u00a0"),
])
def test_opaque_comparison_never_uses_ci_or_sql_padding_equivalence(db, left, right):
    _opaque_setup(db)
    predicate = exact_text_equal("@left", "@right")
    assert not _eval(db, predicate, {"left": left, "right": right})
    assert _eval(db, predicate, {"left": left, "right": left})
    if left.casefold().rstrip(" ") == right.casefold().rstrip(" "):
        assert db.execute("SELECT @left COLLATE SQL_CI=@right COLLATE SQL_CI", {"left": left, "right": right}).fetchone()[0]


def test_actual_prior_event_predicate_keeps_case_distinct_deliveries_separate(db):
    _opaque_setup(db)
    predicate = (
        exact_text_equal("JSON_VALUE(prior.payload,'$.delivery.event_source')",
                         "JSON_VALUE(p.payload,'$.delivery.event_source')")
        + " AND " +
        exact_text_equal("JSON_VALUE(prior.payload,'$.delivery.event_id')",
                         "JSON_VALUE(p.payload,'$.delivery.event_id')")
    )
    db.execute("CREATE TABLE dbo.events (payload TEXT COLLATE SQL_CI)")
    prior = {"delivery": {"event_source": "Source", "event_id": "Event-A"}, "observation": {"status": "failed"}}
    incoming = copy.deepcopy(prior)
    incoming["delivery"]["event_id"] = "event-a"
    db.execute("INSERT INTO dbo.events VALUES (?)", (_json(prior),))
    query = f"SELECT COUNT(*) FROM dbo.events AS prior JOIN (SELECT @payload AS payload) AS p ON {predicate}"
    assert db.execute(query, {"payload": _json(incoming)}).fetchone()[0] == 0
    assert db.execute(query, {"payload": _json(prior)}).fetchone()[0] == 1
    body = _sql(build_permission_kernel(), "worker.commit_positions")
    assert predicate.split(" AND ", 1)[0] in body
    assert "DATALENGTH(JSON_VALUE(prior.payload,'$.delivery.event_id'))" in body


def test_duplicate_event_content_and_original_positions_stay_distinct_checks(db):
    _opaque_setup(db)
    first = {"delivery": {"event_source": "Source", "event_id": "Event-A"},
             "partition": {"partition_id": "0"}, "position": {"offset": "1"},
             "received_at": "old", "observation": {"observed_at": "old", "status": "failed"}}
    duplicate = copy.deepcopy(first)
    duplicate.update(received_at="new", position={"offset": "2"})
    duplicate["observation"]["observed_at"] = "new"
    expression = receipt_content_expression("@payload")
    def stable(value):
        return db.execute("SELECT " + expression, {"payload": _json(value)}).fetchone()[0]
    assert stable(first) == stable(duplicate)
    duplicate["observation"]["status"] = "succeeded"
    assert stable(first) != stable(duplicate)
    assert not _eval(db, exact_text_equal("@old", "@new"), {"old": "1", "new": "1 "})


def test_native_receiver_evidence_maps_only_exact_subscription_types(db):
    _opaque_setup(db)
    expression = re.sub(r"\bN'", "'", subscription_type_sql("@payload"))
    for wire in ("Microsoft.Fabric.ItemJobFailed", "Microsoft.Fabric.JobEvents.ItemJobFailed"):
        payload = {"observation": {"evidence": {
            "native_event_type": wire, "subscription_event_type": "Microsoft.Fabric.JobEvents.ItemJobFailed",
        }}}
        assert db.execute("SELECT " + expression, {"payload": _json(payload)}).fetchone()[0] == "Microsoft.Fabric.JobEvents.ItemJobFailed"
        payload["observation"]["evidence"]["native_event_type"] = wire.lower()
        assert db.execute("SELECT " + expression, {"payload": _json(payload)}).fetchone()[0] is None


def test_desired_connector_cas_preserves_bindings_and_invalidates_old_readiness(db):
    kernel = build_permission_kernel()
    records = kernel.names.table("monitoring_records")
    db.execute(f"""CREATE TABLE {records} (
        tenant_id TEXT,epoch TEXT,record_kind TEXT,full_key TEXT,revision INTEGER,status TEXT,payload TEXT)""")
    # The fixed JSON paths below model SQL JSON_QUERY's object marker, not a general JSON setter.
    def modify(payload, path, value):
        if path in ("$.sources", "$.desired_definition"):
            document = json.loads(payload)
            document[path.removeprefix("$.")] = json.loads(value)
            return _json(document)
        return _modify(payload, path, value)
    db.create_function("JSON_MODIFY", 3, modify)
    db.create_function("JSON_QUERY", 1, lambda value: _json(json.loads(value)))
    initial = {
        "connector_id": "connector", "ownership_id": "owner", "revision": 1,
        "policy_revision": 1, "name": "transport", "sources": [], "desired_definition": {"version": 1},
        "state": "planned",
    }
    assert db.execute(f"SELECT COUNT(*) FROM {records}").fetchone()[0] == 0
    db.execute(f"INSERT INTO {records} VALUES ('tenant','epoch','connector','connector',1,'planned',?)", (_json(initial),))
    reported = {**initial, "revision": 2, "workspace_id": "workspace", "eventstream_id": "stream",
                "destination_id": "destination", "endpoint": {"namespace": "example.invalid", "entity": "hub"},
                "state": "ready", "identity_verified_at": "prior-identity-proof",
                "delivery_verified_at": "prior-delivery-proof", "observed_definition": {"version": 1}}
    effective = db.execute("SELECT " + restore_worker_proof_expression(), {
        "next": _json(reported), "prior": _json(initial),
    }).fetchone()[0]
    assert _eval(db, worker_ready_upgrade_sql(), {"next": effective, "prior": _json(initial)})
    assert "identity_verified_at" not in json.loads(effective)
    assert "delivery_verified_at" not in json.loads(effective)
    # A later controller publication may promote the saved observation; worker output cannot.
    observed = reported
    db.execute(f"UPDATE {records} SET revision=2,status='ready',payload=?", (_json(observed),))
    candidate = db.execute("SELECT " + desired_update_expression(), {
        "prior": _json(observed), "name": "transport", "sources": _json([{"approved": "new-source"}]),
        "definition": _json({"version": 2}), "current_revision": 2,
    }).fetchone()[0]
    candidate = db.execute("SELECT " + invalidate_readiness_expression(), {"next": candidate}).fetchone()[0]
    merged = json.loads(candidate)
    for key in ("ownership_id", "workspace_id", "eventstream_id", "destination_id", "endpoint"):
        assert merged[key] == observed[key]
    assert merged["state"] == "provisioning"
    assert "identity_verified_at" not in merged and "delivery_verified_at" not in merged
    values = {"tenant_id": "tenant", "epoch": "epoch", "connector_id": "connector", "ownership_id": "owner",
              "expected_connector_revision": 2, "next": candidate}
    assert db.execute(publish_update_sql(kernel.names), {**values, "expected_connector_revision": 1}).rowcount == 0
    assert db.execute(publish_update_sql(kernel.names), {**values, "ownership_id": "other"}).rowcount == 0
    assert db.execute(publish_update_sql(kernel.names), values).rowcount == 1
    assert db.execute(publish_update_sql(kernel.names), values).rowcount == 0


def test_current_source_approval_is_not_a_caller_complete_flag(db):
    target = {"workspace_id": "workspace", "item_id": "item"}
    params = {
        "source_target": _json(target), "current_revision": 2, "now": "2026-09-16T12:00:00Z",
        "approved_target": _json({"state": "current", "policy_revision": 2, "observation": {"enabled": True}}),
        "source_capability": _json({"read_status": "verified", "event_status": "verified", "target": target,
                                   "expires_at": "2026-09-17T00:00:00Z"}),
    }
    assert _eval(db, source_authorized_predicate(), params)
    assert not _eval(db, source_authorized_predicate(), {**params, "current_revision": 3})
    assert not _eval(db, source_authorized_predicate(), {**params, "source_capability": None})
    assert not _eval(db, source_authorized_predicate(), {**params, "approved_target": _json({
        "state": "current", "policy_revision": 2, "observation": {"enabled": False},
    })})


def test_sql_procedures_enforce_review9_boundaries_and_receipt_replay():
    kernel = build_permission_kernel()
    reject = _sql(kernel, "controller.resolve_frontier")
    assert "IF @whole_window_rejection=0" in reject
    assert "Whole-window rejection must cover every committed original intake receipt" in reject
    assert reject.index("IF @prior_payload IS NOT NULL") < reject.index("SET @whole_window_rejection=1")
    assert "Current work owner or fence was lost" in reject
    assert "SQL kernel tenant or epoch does not match current control" in reject
    assert "UPDATE [dbo].[triage_monitoring_control]" not in reject
    for operation in ("controller.transition_action", "controller.finalize"):
        assert "whole_window_rejection" not in _sql(kernel, operation)
    publish = _sql(kernel, "controller.publish_connector")
    assert kernel.rpcs["controller.publish_connector"].components == ("controller",)
    assert "N'controller:'" not in publish
    assert "Current work owner or fence was lost" in publish
    assert "IF @prior IS NULL" in publish and "'planned' AS state" in publish
    assert "Connector revision/ownership changed" in publish
    assert "Current policy revision differs" in publish
    assert publish.index("IF @prior_payload IS NOT NULL") < publish.index("Connector revision/ownership changed")
    assert "Existing connector has no protected desired-publication provenance" in publish
    assert "Readiness lacks current matched ownership/topology/identity/delivery evidence" in publish
    assert not {"state", "workspace_id", "eventstream_id", "destination_id", "endpoint",
                "identity_verified_at", "delivery_verified_at"} & set(PUBLICATION_FIELDS)
    worker = _sql(kernel, "worker.observe_connector")
    assert "'$.identity_verified_at',JSON_VALUE(@prior,'$.identity_verified_at')" in worker
    assert "SET @next=JSON_MODIFY(@next,'$.state','provisioning')" in worker
    assert "JSON_QUERY(@observation) AS observation" in worker
    for view in ("worker_catalogue", "worker_evidence", "worker_telemetry", "web_drafts", "controller_projections"):
        assert "N'connector'" not in _sql(kernel, view)
        assert "N'connector_desired'" not in _sql(kernel, view)
    assert "connector_publication" in integration_contract()["controller_publication_contracts"]
