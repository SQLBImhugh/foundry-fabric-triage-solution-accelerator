"""Execute emitted removal predicates and mutations with SQLite scalar adapters.

The JSON serializers and cursor orchestration below are test scaffolding, not a
substitute for native SQL procedure, role, locking or deployed-identity proof.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
from uuid import UUID

import pytest
from test_monitoring_sql_retry_finalization import _json
from test_monitoring_sql_retry_finalization import db as db
from test_monitoring_sql_review9 import _opaque_setup

from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    current_work,
    key_hash,
    payload_hash,
    save_receipt,
)
from triage.monitoring.sql_kernel_connectors import (
    desired_update_expression,
    invalidate_readiness_expression,
    publish_update_sql,
)
from triage.monitoring.sql_kernel_contracts import RECORD_COLUMNS
from triage.monitoring.sql_kernel_proposals import (
    binding_observation_matches_sql,
    binding_receipt_sql,
    complete_component_map_sql,
    proposal_binding_sql,
)
from triage.monitoring.sql_kernel_removals import (
    confirm_removals_sql,
    desired_source_projection_sql,
    original_removal_matches_sql,
    prepare_removals_sql,
    remote_absence_predicate,
    removal_binding_sql,
    retained_ownership_sql,
    retirement_records_sql,
    source_is_pending_removal_sql,
)
from triage.monitoring.sql_permissions import build_permission_kernel, integration_contract


def _id(number):
    return f"{number:08x}-1111-4111-8111-111111111111"


def _hash(value):
    return hashlib.sha256(value.encode("utf-16-le")).hexdigest().upper()


def _path(payload, path):
    value = json.loads(payload) if payload is not None else None
    tokens = re.findall(r'\."([^"]+)"|\.([^.\[]+)|\[(\d+)\]', path.removeprefix("$"))
    for quoted, plain, index in tokens:
        key = quoted or plain
        if index:
            value = value[int(index)] if isinstance(value, list) and int(index) < len(value) else None
        else:
            value = value.get(key) if isinstance(value, dict) else None
    return value


def _value(payload, path):
    value = _path(payload, path)
    if isinstance(value, (dict, list)) or value is None:
        return None
    return ("true" if value else "false") if isinstance(value, bool) else str(value)


def _query(payload, path="$"):
    value = _path(payload, path)
    return _json(value) if isinstance(value, (dict, list)) else None


def _modify(payload, path, value):
    result = json.loads(payload)
    key = path.removeprefix("$.")
    assert "." not in key, "Only emitted top-level connector JSON mutations are adapted here"
    if value is None:
        result.pop(key, None)
    else:
        if key in {"sources", "source_proposals", "source_removals", "desired_definition"}:
            value = json.loads(value)
        result[key] = value
    return _json(result)


def _guid(value):
    try:
        return isinstance(value, str) and value == str(UUID(value)) and UUID(value).int != 0
    except ValueError:
        return False


def _sql(kernel, logical):
    return next(obj.ddl for obj in kernel.objects if obj.logical_name == logical)


def _adapt(kernel, sql, *, concatenate=False):
    for expression in ("@remove_binding_hash", payload_hash("JSON_QUERY(@binding_observation,'$.observed_definition')")):
        sql = sql.replace(f"CONVERT(nvarchar(64),{expression})", expression)
    for expression in (
        "component.value", "ids.value", "value",
        "JSON_VALUE(requested.value,'$.removal_id')",
        "JSON_VALUE(requested.value,'$.proposal_id')",
    ):
        sql = sql.replace(canonical_guid(expression), f"IS_GUID({expression})=1")
    for expression in (
        "@connector_id", "@partition_key", "@work_key", "@request_id", "@acceptance_key", "@frontier_key",
        "@connector_id+N':removal:'+retired.removal_id",
    ):
        sql = sql.replace(key_hash(expression), f"KEY_DIGEST({expression})")
    for expression in (
        "@remove_binding", "@binding_receipt_payload",
        "JSON_QUERY(@binding_observation,'$.observed_definition')",
        "JSON_QUERY(@observation_json,'$.observed_definition')",
    ):
        sql = sql.replace(payload_hash(expression), f"PAYLOAD_HASH({expression})")
    sql = sql.replace(kernel.names.object("json_equal"), "JSON_EQUAL")
    sql = re.sub(r"\bN'", "'", sql)
    sql = re.sub(r" WITH \((?:UPDLOCK, ?HOLDLOCK)\)", "", sql)
    sql = sql.replace("OPENJSON(", "json_each(").replace("CROSS APPLY", "JOIN")
    sql = sql.replace("COUNT_BIG(", "COUNT(")
    sql = re.sub(
        r"\b([a-z_]+\.type|(?<!\.)type)\b",
        lambda m: f"(CASE {m[1]} WHEN 'null' THEN 0 WHEN 'text' THEN 1 WHEN 'integer' THEN 2 "
        "WHEN 'real' THEN 2 WHEN 'true' THEN 3 WHEN 'false' THEN 3 "
        "WHEN 'array' THEN 4 WHEN 'object' THEN 5 END)", sql,
    )
    sql = re.sub(r"TRY_CONVERT\((bigint|int),", r"TRY_CONVERT('\1',", sql)
    sql = re.sub(
        r"STRING_AGG\(CONVERT\(nvarchar\(max\),([\w.]+)\),','\)\s*"
        r"WITHIN GROUP \(ORDER BY (ownership_order,position|removal_id|CONVERT\(int,([\w.\[\]]+)\))\)",
        lambda m: f"GROUP_CONCAT({m[1]},',' ORDER BY "
        + (f"CAST({m[3]} AS INTEGER)" if m[3] else m[2]) + ")", sql,
    )
    sql = re.sub(r"CONVERT\(int,([\w.\[\]]+)\)", r"CAST(\1 AS INTEGER)", sql)
    if concatenate:
        sql = sql.replace("+", "||")
    return sql


def _evaluate(db, kernel, predicate, params):
    return db.execute("SELECT CASE WHEN (" + _adapt(kernel, predicate, concatenate=True)
                      + ") THEN 1 ELSE 0 END", params).fetchone()[0] == 1


def _guard(body, message):
    prefix = body.split(f"THROW 51072, '{message}'", 1)[0]
    if prefix == body:
        prefix = body.split(f"THROW 51073, '{message}'", 1)[0]
    assert prefix != body, message
    match = list(re.finditer(r"(?m)^\s*IF ", prefix))[-1]
    return prefix[match.end():].strip()


def _assignment(body, variable):
    return re.search(rf"SET @{variable}=(.*?);", body, re.S)[1]


def _set_value(db, kernel, body, variable, params):
    return db.execute("SELECT " + _adapt(kernel, _assignment(body, variable), concatenate=True), params).fetchone()[0]


def _insert_record(db, kernel, kind, key, payload, revision=1):
    db.execute(f"""INSERT INTO {kernel.names.table('monitoring_records')}
        (tenant_id,epoch,record_kind,key_hash,full_key,revision,status,payload)
        VALUES (?,?,?,?,?,?,?,?)""", (
            _id(1), _id(2), kind, hashlib.sha256(key.encode()).digest(), key, revision,
            payload.get("state"), _json(payload),
        ))


def _manifest(db, kernel):
    row = db.execute(f"SELECT payload FROM {kernel.names.table('monitoring_records')} "
                     "WHERE record_kind='connector'").fetchone()
    return json.loads(row[0])


def _save_receipt(db, kernel, operation, params, result):
    sql = save_receipt(kernel.names, operation)
    sql = sql.replace(
        "(SELECT @binding_hash AS binding_hash, JSON_QUERY(@result) AS result\n"
        "     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)",
        "json_object('binding_hash',@binding_hash,'result',json(@result))",
    )
    db.execute(_adapt(kernel, sql), {**params, "fingerprint": "f" * 64, "result": _json(result)})


def _replay(db, kernel, params):
    row = db.execute(f"SELECT payload FROM {kernel.names.table('monitoring_receipts')} "
                     "WHERE operation='controller.publish_connector' AND request_id=?", (params["request_id"],)).fetchone()
    if row:
        receipt = json.loads(row[0])
        if receipt["binding_hash"] != params["binding_hash"]:
            raise ValueError("original request binding")
        return receipt["result"]
    return None


def _mutation_params(manifest, request_id):
    return {
        "tenant_id": _id(1), "epoch": _id(2), "connector_id": _id(3),
        "ownership_id": _id(4), "expected_connector_revision": manifest["revision"],
        "current_revision": 3, "expected_revision": 3, "request_id": request_id,
        "work_id": _id(5), "owner_id": _id(6), "fence": 7, "work_key": "synthetic-work-key",
        "now": "2026-09-16T12:00:00Z", "publication_id": _id(7),
        "binding_hash": _hash(request_id), "name": manifest["name"],
    }


def _apply_connector(db, kernel, manifest, params):
    if db.execute(publish_update_sql(kernel.names), {**params, "next": _json(manifest)}).rowcount != 1:
        raise ValueError("connector revision/ownership CAS")


def _topology(sources, proposals=()):
    nodes = [
        {"name": "source_" + source["target"]["item_id"][:8], "type": "FabricJobEvents",
         "properties": {"eventScope": "Item", "workspaceId": source["target"]["workspace_id"],
                        "itemId": source["target"]["item_id"], "includedEventTypes": source["event_types"]}}
        for source in sources
    ]
    nodes.extend(
        {"name": p["node_name"], "type": "FabricJobEvents",
         "properties": {"eventScope": "Item", "workspaceId": p["target"]["workspace_id"],
                        "itemId": p["target"]["item_id"], "includedEventTypes": p["event_types"]}}
        for p in proposals
    )
    return {
        "parts": {"eventstream.json": {
            "compatibilityLevel": "1.0", "sources": nodes, "operators": [],
            "streams": [{"name": "owned_stream", "type": "DefaultStream",
                         "inputNodes": [{"name": n["name"]} for n in nodes]}],
            "destinations": [{"name": "owned_destination", "type": "CustomEndpoint",
                              "inputNodes": [{"name": "owned_stream"}]}],
        }},
        "component_ids": {
            "streams/owned_stream": _id(30), "destinations/owned_destination": _id(31),
            **{"sources/source_" + s["target"]["item_id"][:8]: s["source_id"] for s in sources},
        },
    }


@pytest.fixture
def case(db):
    kernel = build_permission_kernel()
    _opaque_setup(db)
    db.create_function("JSON_VALUE", 2, _value)
    db.create_function("JSON_QUERY", 1, _query)
    db.create_function("JSON_QUERY", 2, _query)
    db.create_function("JSON_MODIFY", 3, _modify)
    db.create_function("JSON_EQUAL", 2, lambda a, b: int(a is not None and b is not None and json.loads(a) == json.loads(b)))
    db.create_function("PAYLOAD_HASH", 1, lambda value: _hash(value) if value is not None else None)
    db.create_function("KEY_DIGEST", 1, lambda value: hashlib.sha256(value.encode()).digest())
    db.create_function("IS_GUID", 1, _guid)
    columns = ",".join(f"[{c}] {'INTEGER' if c in ('revision','sequence_number') else 'BLOB' if c.endswith('_hash') else 'TEXT'}"
                       for c in RECORD_COLUMNS)
    db.execute(f"CREATE TABLE {kernel.names.table('monitoring_records')} "
               f"({columns}, PRIMARY KEY(tenant_id,epoch,record_kind,key_hash))")
    db.execute(f"""CREATE TABLE {kernel.names.table('monitoring_receipts')} (
        tenant_id TEXT,epoch TEXT,operation TEXT,request_hash BLOB,request_id TEXT,
        fingerprint TEXT,recorded_at TEXT,payload TEXT,
        PRIMARY KEY(tenant_id,epoch,operation,request_hash))""")
    db.execute(f"""CREATE TABLE {kernel.names.table('monitoring_leases')} (
        tenant_id TEXT,epoch TEXT,full_key TEXT,key_hash BLOB,owner_id TEXT,fence INTEGER,expires_at TEXT)""")
    db.execute(f"INSERT INTO {kernel.names.table('monitoring_leases')} VALUES (?,?,?,?,?,?,?)", (
        _id(1), _id(2), "synthetic-work-key", hashlib.sha256(b"synthetic-work-key").digest(),
        _id(6), 7, "2026-09-16T13:00:00Z",
    ))
    db.execute("CREATE TABLE dbo.control (tenant_id TEXT,epoch TEXT,revision INTEGER)")
    db.execute("INSERT INTO dbo.control VALUES (?,?,3)", (_id(1), _id(2)))
    sources = [{
        "source_id": _id(number), "target": {
            "tenant_id": _id(1), "epoch": _id(2), "workload": "fabric_pipeline",
            "workspace_id": _id(10), "item_id": _id(number + 100),
        }, "event_types": ["Microsoft.Fabric.JobEvents.ItemJobFailed"],
        "event_source": "synthetic-event-source-" + str(number),
    } for number in (11, 12)]
    manifest = {
        "tenant_id": _id(1), "epoch": _id(2), "connector_id": _id(3), "ownership_id": _id(4),
        "revision": 8, "policy_revision": 3, "name": "Owned transport",
        "sources": sources, "source_proposals": [], "source_removals": [],
        "workspace_id": _id(20), "eventstream_id": _id(21), "destination_id": _id(31),
        "endpoint": {"namespace": "example.invalid", "entity": "owned", "consumer_group": "$Default"},
        "desired_definition": _topology(sources), "observed_definition": _topology(sources),
        "state": "ready", "identity_verified_at": "original-identity", "delivery_verified_at": "original-delivery",
    }
    _insert_record(db, kernel, "connector", _id(3), manifest, revision=8)
    for kind in ("action", "incident_budget", "approval_binding", "validation_frontier",
                 "partition_ownership", "stream_checkpoint"):
        _insert_record(db, kernel, kind, kind, {"state": "untouched", "value": 17})
    db.commit()
    return kernel, manifest


def _protected(db, kernel):
    return db.execute(f"SELECT * FROM {kernel.names.table('monitoring_records')} "
                      "WHERE record_kind NOT IN ('connector','connector_source_retirement') "
                      "ORDER BY record_kind").fetchall()


def _intent(source, removal_id=None, *, proposal=False):
    return {"removal_id": removal_id or _id(40), "source_id": None if proposal else source["source_id"],
            "proposal_id": source["proposal_id"] if proposal else None, "detail": "Remove reviewed subscription"}


def _new_pending(manifest, intent):
    original = next(
        s for s in (*manifest["sources"], *manifest["source_proposals"])
        if (intent["source_id"] is not None and s["source_id"] == intent["source_id"])
        or (intent["proposal_id"] is not None and s.get("proposal_id") == intent["proposal_id"])
    )
    node = original.get("node_name")
    if node is None:
        node = next(k.removeprefix("sources/") for k, value in manifest["observed_definition"]["component_ids"].items()
                    if k.startswith("sources/") and value == original["source_id"])
    return {
        **intent, "node_name": node, "target": original["target"], "binding_hash": _hash(_json(original)),
        "last_observed_source_id": manifest["observed_definition"]["component_ids"].get("sources/" + node),
        "policy_revision": 3, "request_id": _id(41), "publication_id": _id(7),
        "requested_at": "2026-09-16T12:00:00Z", "state": "pending_remote_absence",
    }


def _stage(db, kernel, *, proposals=(), source_removals=None):
    manifest = _manifest(db, kernel)
    intents = source_removals if source_removals is not None else [_intent(manifest["sources"][1])]
    params = _mutation_params(manifest, _id(41))
    params.update(prior=_json(manifest), sources=_json(manifest["sources"]),
                  proposals=_json([*manifest["source_proposals"], *proposals]),
                  removal_intents=_json(intents), superseded_json="[]")
    body = prepare_removals_sql(kernel.names)
    for message in (
        "Desired changes must retain all owned source bindings until receipt-verified retirement",
        "Uncertain logical proposal ownership cannot be discarded without verified absence",
        "A removal selects exactly one already-owned source or proposal with a stable request ID",
        "Pending removal cannot be cancelled by omitting its ownership record",
    ):
        if _evaluate(db, kernel, _guard(body, message), params):
            raise ValueError(message)
    pending = []
    for intent in intents:
        rows = db.execute(_adapt(kernel, removal_binding_sql()), {
            **params, "remove_source_id": intent["source_id"], "remove_proposal_id": intent["proposal_id"],
        }).fetchall()
        if len(rows) != 1:
            raise ValueError("unowned source/proposal")
        pending.append(_new_pending(manifest, intent))
    params["removals"] = _json(pending)
    effective = json.loads(_set_value(db, kernel, desired_source_projection_sql(), "desired_sources", params))
    definition = _topology([s for s in effective if s["source_id"] is not None],
                           [s for s in effective if s["source_id"] is None])
    candidate = json.loads(db.execute("SELECT " + desired_update_expression(), {
        **params, "definition": _json(definition),
    }).fetchone()[0])
    candidate = json.loads(db.execute("SELECT " + invalidate_readiness_expression(), {
        "next": _json(candidate),
    }).fetchone()[0])
    candidate.update(source_proposals=json.loads(params["proposals"]), source_removals=pending, revision=manifest["revision"] + 1)
    result = {"connector_id": _id(3), "connector": candidate, "state": candidate["state"], "desired_changed": True,
              "pending_removals": pending, "retired_sources": [], "observation_receipt_id": None}
    with db:
        _apply_connector(db, kernel, candidate, params)
        _save_receipt(db, kernel, "controller.publish_connector", params, result)
    return candidate


def _observe(db, kernel, definition, request_id=None, *, include_definition=True, state="provisioning"):
    manifest = _manifest(db, kernel)
    params = _mutation_params(manifest, request_id or _id(42))
    observed = {**manifest, "revision": manifest["revision"] + 1, "state": state, "operation_id": "original-remote-operation"}
    if include_definition:
        observed["observed_definition"] = definition
    result = {"connector_id": _id(3), "connector": observed, "observation": observed,
              "observed_definition_hash": _hash(_json(definition)) if include_definition else None,
              "authority": "observed_not_action_authority"}
    with db:
        _apply_connector(db, kernel, observed, params)
        _save_receipt(db, kernel, "worker.observe_connector", params, result)
    return observed


def _confirm(db, kernel, params=None):
    prior = _manifest(db, kernel)
    params = {**_mutation_params(prior, _id(43)), "binding_receipt_id": _id(42), **(params or {})}
    with db:
        db.execute("BEGIN")
        context = db.execute("SELECT tenant_id,epoch,revision FROM dbo.control").fetchone()
        if context[:2] != (params["tenant_id"], params["epoch"]):
            raise ValueError("context")
        replay = _replay(db, kernel, params)
        if replay is not None:
            return replay
        if context[2] != params["expected_revision"]:
            raise ValueError("policy")
        lease_guard = current_work(kernel.names, ("reconcile_state",)).split("IF NOT EXISTS (", 1)[1]
        lease_query = lease_guard.split(") THROW 51074", 1)[0]
        if not db.execute(_adapt(kernel, lease_query), params).fetchone():
            raise ValueError("lease")
        params.update(prior=_json(prior), sources=_json(prior["sources"]),
                      proposals=_json(prior["source_proposals"]), definition=_json(prior["desired_definition"]),
                      readiness_id=None, stored_work=_json({"reconcile_request_id": _id(42)}))
        body = proposal_binding_sql(kernel.names)
        if _evaluate(db, kernel, _guard(body, "Observed binding may resolve only the exact current approved proposals"), params):
            raise ValueError("original observation work binding")
        row = db.execute(_adapt(kernel, binding_receipt_sql(kernel.names)), params).fetchone()
        if row is None:
            raise ValueError("original observation receipt")
        params.update(binding_receipt_payload=row[0], binding_receipt_fingerprint=row[1],
                      binding_observation=_json(json.loads(row[0])["result"]["observation"]))
        if not _evaluate(db, kernel, binding_observation_matches_sql(kernel.names), params):
            raise ValueError("original observation binding")
        if not _evaluate(db, kernel, complete_component_map_sql(), params):
            raise ValueError("incomplete component map")
        db.execute("CREATE TEMP TABLE pending_removals (removal_id TEXT PRIMARY KEY,source_id TEXT,proposal_id TEXT,"
                   "node_name TEXT,binding_json TEXT,payload TEXT)")
        db.execute("CREATE TEMP TABLE retired (removal_id TEXT PRIMARY KEY,payload TEXT)")
        for removal in prior["source_removals"]:
            bindings = db.execute(_adapt(kernel, removal_binding_sql()), {
                **params, "remove_source_id": removal["source_id"], "remove_proposal_id": removal["proposal_id"],
            }).fetchall()
            assert len(bindings) == 1
            original = json.loads(bindings[0][0])
            params.update(remove_node_name=removal["node_name"], remove_source_id=removal["source_id"],
                          remove_physical_id=removal["last_observed_source_id"])
            if not _evaluate(db, kernel, remote_absence_predicate(), params):
                raise ValueError("remote absence")
            tombstone = {
                "connector_id": _id(3), "ownership_id": _id(4), "removal_id": removal["removal_id"],
                "source_id": removal["source_id"], "proposal_id": removal["proposal_id"],
                "node_name": removal["node_name"], "original_binding": original, "original_removal": removal,
                "observation_receipt_id": params["binding_receipt_id"], "observation_fingerprint": row[1],
                "observation_binding_hash": json.loads(row[0])["binding_hash"],
                "observation_receipt_hash": _hash(row[0]),
                "observed_definition_hash": _hash(_json(json.loads(params["binding_observation"])["observed_definition"])),
                "confirmation_request_id": params["request_id"], "work_id": params["work_id"],
                "work_fence": params["fence"], "policy_revision": params["current_revision"],
                "retired_at": params["now"], "state": "retired_verified",
            }
            db.execute("INSERT INTO pending_removals VALUES (?,?,?,?,?,?)", (
                removal["removal_id"], removal["source_id"], removal["proposal_id"],
                removal["node_name"], _json(original), _json(removal),
            ))
            db.execute("INSERT INTO retired VALUES (?,?)", (removal["removal_id"], _json(tombstone)))
        db.execute("CREATE TEMP TABLE bound (proposal_id TEXT,node_name TEXT,source_id TEXT,source_json TEXT)")
        select = re.search(r"INSERT INTO @bound (SELECT .*?);", body, re.S)[1]
        db.execute("INSERT INTO bound " + _adapt(kernel, select, concatenate=True).replace("@pending_removals", "pending_removals"), params)
        binding_body = body[body.index("INSERT INTO @bound"):]
        for variable in ("sources", "proposals"):
            params[variable] = _set_value(db, kernel, binding_body.replace("@bound", "bound"), variable, params)
        confirmation = confirm_removals_sql(kernel.names).replace("@pending_removals", "pending_removals").replace("@retired AS", "retired AS")
        for variable in ("sources", "proposals"):
            params[variable] = _set_value(db, kernel, confirmation, variable, params)
        retirement_insert = retirement_records_sql(kernel.names).replace("@retired AS", "retired AS")
        db.execute(_adapt(kernel, retirement_insert, concatenate=True), params)
        definition = json.loads(params["binding_observation"])["observed_definition"]
        candidate = json.loads(db.execute("SELECT " + desired_update_expression(), {
            **params, "definition": _json(definition),
        }).fetchone()[0])
        candidate.update(revision=prior["revision"] + 1, source_proposals=json.loads(params["proposals"]), source_removals=[])
        retired = [json.loads(row[0]) for row in db.execute("SELECT payload FROM retired ORDER BY removal_id")]
        result = {"connector_id": _id(3), "connector": candidate, "state": candidate["state"], "desired_changed": True,
                  "pending_removals": [], "retired_sources": retired, "observation_receipt_id": params["binding_receipt_id"]}
        _apply_connector(db, kernel, candidate, params)
        _save_receipt(db, kernel, "controller.publish_connector", params, result)
        for table in ("pending_removals", "retired", "bound"):
            db.execute(f"DROP TABLE {table}")
        return result


def test_old_subset_mutation_was_unsafe_and_current_emitted_guard_refuses_it(db, case):
    kernel, original = case
    params = {"prior": _json(original), "name": original["name"], "sources": _json(original["sources"][:1]),
              "definition": _json(_topology(original["sources"][:1])), "current_revision": 3}
    old_result = json.loads(db.execute("SELECT " + desired_update_expression(), params).fetchone()[0])
    assert old_result["sources"] == original["sources"][:1]  # The original unsafe mutation, without a receipt.
    predicate = _guard(retained_ownership_sql(kernel.names),
                       "Desired changes must retain all owned source bindings until receipt-verified retirement")
    assert _evaluate(db, kernel, predicate, params)
    assert not _evaluate(db, kernel, predicate, {**params, "sources": _json(original["sources"])})
    body = _sql(kernel, "controller.publish_connector")
    assert body.index(predicate) < body.index("SET @next=" + desired_update_expression())


def test_pending_removal_retains_ownership_denies_intake_and_does_not_claim_readiness(db, case):
    kernel, original = case
    before = _protected(db, kernel)
    staged = _stage(db, kernel)
    assert staged["sources"] == original["sources"] and staged["state"] == "provisioning"
    assert "identity_verified_at" not in staged and "delivery_verified_at" not in staged
    assert staged["source_removals"][0]["source_id"] == original["sources"][1]["source_id"]
    for source, denied in zip(original["sources"], (False, True), strict=True):
        assert _evaluate(db, kernel, source_is_pending_removal_sql("@source", "@removals"), {
            "source": _json(source), "removals": _json(staged["source_removals"]),
        }) == denied
    intake = _sql(kernel, "worker.commit_positions")
    assert source_is_pending_removal_sql("s.value", "JSON_QUERY(@connector,'$.source_removals')") in intake
    assert "'$.observation.enabled')='true'" in intake
    assert "IF @prior_payload IS NOT NULL" in intake
    assert intake.index("IF @prior_payload IS NOT NULL") < intake.index("Accepted event is outside")
    assert _protected(db, kernel) == before


def test_actual_intake_guard_excludes_retained_but_revoked_membership_and_stale_admission(db, case):
    kernel, original = case
    staged = _stage(db, kernel)
    body = _sql(kernel, "worker.commit_positions")
    predicate = _guard(body, "Accepted event is outside the owned desired source; quarantine explicitly")
    predicate = predicate.replace("@positions", "incoming_positions")
    db.execute("CREATE TEMP TABLE incoming_positions (disposition TEXT,payload TEXT)")
    for source in original["sources"]:
        target = source["target"]
        target_key = ":".join(("monitor:v1", target["epoch"], target["tenant_id"],
                               target["workload"], target["workspace_id"], target["item_id"]))
        _insert_record(db, kernel, "target", target_key, {
            "state": "current", "policy_revision": 3, "observation": {"enabled": True},
        })
    for index, source in enumerate(original["sources"]):
        payload = {"delivery": {"event_source": source["event_source"]},
                   "observation": {"execution": {"target": source["target"]}},
                   "event_type": "Microsoft.Fabric.ItemJobFailed"}
        db.execute("DELETE FROM incoming_positions")
        db.execute("INSERT INTO incoming_positions VALUES ('accepted',?)", (_json(payload),))
        params = {"tenant_id": _id(1), "epoch": _id(2), "current_revision": 3, "connector": _json(staged)}
        assert _evaluate(db, kernel, predicate, params) == (index == 1)
        assert _evaluate(db, kernel, predicate, {**params, "current_revision": 4})
        db.execute("UPDATE incoming_positions SET disposition='quarantined'")
        assert not _evaluate(db, kernel, predicate, params)
    # These are membership decisions only; provisioning still fails the separate partition-owner entry gate.


def test_uncertain_ack_recovery_original_absence_receipt_then_retirement_and_replay(db, case):
    kernel, original = case
    before = _protected(db, kernel)
    staged = _stage(db, kernel)
    _observe(db, kernel, None, _id(44), include_definition=False)
    assert _manifest(db, kernel)["sources"] == original["sources"]
    with pytest.raises(ValueError, match="original observation work binding"):
        _confirm(db, kernel, {"binding_receipt_id": _id(44)})
    observed = _observe(db, kernel, staged["desired_definition"])
    params = _mutation_params(observed, _id(43))
    result = _confirm(db, kernel, params)
    assert result["connector"]["sources"] == original["sources"][:1]
    assert result["connector"]["source_removals"] == [] and result["state"] == "provisioning"
    retired = result["retired_sources"][0]
    assert retired["original_binding"] == original["sources"][1]
    assert retired["observation_receipt_id"] == _id(42) and retired["state"] == "retired_verified"
    assert retired["observation_receipt_hash"] and retired["observation_binding_hash"]
    assert _protected(db, kernel) == before
    db.execute("UPDATE dbo.control SET revision=4")
    db.execute(f"UPDATE {kernel.names.table('monitoring_leases')} SET fence=99")
    db.commit()
    assert _confirm(db, kernel, params) == result
    with pytest.raises(ValueError, match="original request binding"):
        _confirm(db, kernel, {**params, "binding_hash": "changed"})
    original_stage = _replay(db, kernel, _mutation_params(original, _id(41)))
    assert len(original_stage["connector"]["sources"]) == 2
    assert _manifest(db, kernel)["source_removals"] == [] and _protected(db, kernel) == before


@pytest.mark.parametrize("fault", ["still_present", "missing_map", "partial_map", "blocked",
                                  "omitted_definition", "wrong_policy", "wrong_ownership"])
def test_failed_partial_stale_or_nonoriginal_observation_never_retires(db, case, fault):
    kernel, original = case
    staged = _stage(db, kernel)
    definition = copy.deepcopy(staged["desired_definition"])
    if fault == "still_present":
        definition = original["desired_definition"]
    elif fault == "missing_map":
        definition.pop("component_ids")
    elif fault == "partial_map":
        definition["component_ids"].pop("streams/owned_stream")
    _observe(db, kernel, definition, include_definition=fault != "omitted_definition",
             state="blocked" if fault == "blocked" else "provisioning")
    if fault in {"wrong_policy", "wrong_ownership"}:
        receipts = kernel.names.table("monitoring_receipts")
        payload = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE request_id=?", (_id(42),)).fetchone()[0])
        payload["result"]["observation"]["policy_revision" if fault == "wrong_policy" else "ownership_id"] = 2 if fault == "wrong_policy" else _id(90)
        db.execute(f"UPDATE {receipts} SET payload=? WHERE request_id=?", (_json(payload), _id(42)))
        db.commit()
    before = _manifest(db, kernel)
    with pytest.raises(ValueError, match="observation|component map"):
        _confirm(db, kernel)
    assert _manifest(db, kernel) == before
    assert db.execute(f"SELECT COUNT(*) FROM {kernel.names.table('monitoring_records')} "
                      "WHERE record_kind='connector_source_retirement'").fetchone()[0] == 0


@pytest.mark.parametrize("delta", [
    {"tenant_id": _id(80)}, {"epoch": _id(80)}, {"expected_revision": 2},
    {"owner_id": _id(80)}, {"fence": 6}, {"work_key": "another-work"},
    {"expected_connector_revision": 8}, {"binding_receipt_id": _id(80)},
])
def test_confirmation_requires_current_context_work_fence_revision_and_original_receipt(db, case, delta):
    kernel, _ = case
    staged = _stage(db, kernel)
    _observe(db, kernel, staged["desired_definition"])
    before = _manifest(db, kernel)
    with pytest.raises(ValueError, match="context|policy|lease|receipt|work binding"):
        _confirm(db, kernel, delta)
    assert _manifest(db, kernel) == before


def test_receipt_failure_rolls_back_tombstone_and_current_ownership_together(db, case):
    kernel, original = case
    staged = _stage(db, kernel)
    _observe(db, kernel, staged["desired_definition"])
    before = _manifest(db, kernel)
    records = kernel.names.table("monitoring_records")
    receipts = kernel.names.table("monitoring_receipts")
    db.execute(f"""CREATE TRIGGER dbo.fail_retirement_receipt BEFORE INSERT ON {receipts.split('.')[-1]}
        WHEN NEW.request_id='{_id(43)}' BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="injected receipt"):
        _confirm(db, kernel)
    assert _manifest(db, kernel) == before and before["sources"] == original["sources"]
    assert db.execute(f"SELECT COUNT(*) FROM {records} WHERE record_kind='connector_source_retirement'").fetchone()[0] == 0
    db.execute("DROP TRIGGER dbo.fail_retirement_receipt")
    result = _confirm(db, kernel)
    assert result["connector"]["sources"] == original["sources"][:1]
    assert db.execute(f"SELECT COUNT(*) FROM {records} WHERE record_kind='connector_source_retirement'").fetchone()[0] == 1


def test_an_omitted_definition_cannot_borrow_a_previous_matching_snapshot(db, case):
    kernel, _ = case
    staged = _stage(db, kernel)
    _observe(db, kernel, staged["desired_definition"], _id(44))
    _observe(db, kernel, None, include_definition=False)
    assert _manifest(db, kernel)["observed_definition"] == staged["desired_definition"]
    before = _manifest(db, kernel)
    with pytest.raises(ValueError, match="original observation binding"):
        _confirm(db, kernel)
    assert _manifest(db, kernel) == before


@pytest.mark.parametrize("fault", ["extra", "duplicate_id", "wrong_node_id", "case_key", "padded_key",
                                  "non_guid", "sources_not_array"])
def test_complete_mapping_requires_every_exact_source_stream_and_destination_once(db, case, fault):
    kernel, original = case
    definition = copy.deepcopy(original["desired_definition"])
    params = {"binding_observation": _json({"observed_definition": definition})}
    assert _evaluate(db, kernel, complete_component_map_sql(), params)
    ids = definition["component_ids"]
    source_key = next(key for key in ids if key.startswith("sources/"))
    if fault == "extra":
        ids["sources/unowned"] = _id(90)
    elif fault == "duplicate_id":
        ids["streams/owned_stream"] = ids[source_key]
    elif fault == "wrong_node_id":
        definition["parts"]["eventstream.json"]["sources"][0]["id"] = _id(90)
    elif fault in {"case_key", "padded_key"}:
        ids[source_key.upper() if fault == "case_key" else source_key + " "] = ids.pop(source_key)
    elif fault == "non_guid":
        ids[source_key] = "display-name-not-identity"
    else:
        definition["parts"]["eventstream.json"]["sources"] = {}
    assert not _evaluate(db, kernel, complete_component_map_sql(), {
        "binding_observation": _json({"observed_definition": definition}),
    })


@pytest.mark.parametrize("fault", ["old_node", "old_id_elsewhere", "old_reference", "last_observed_id_elsewhere"])
def test_renaming_or_rebinding_is_not_exact_absence(db, case, fault):
    kernel, original = case
    source = original["sources"][1]
    node = "source_" + source["target"]["item_id"][:8]
    definition = _topology(original["sources"][:1])
    if fault == "old_node":
        definition["component_ids"]["sources/" + node] = _id(90)
    elif fault == "old_id_elsewhere":
        definition["component_ids"]["sources/renamed"] = source["source_id"]
    elif fault == "last_observed_id_elsewhere":
        definition["component_ids"]["sources/renamed"] = _id(91)
    else:
        definition["parts"]["eventstream.json"]["streams"][0]["inputNodes"].append({"name": node})
    params = {
        "binding_observation": _json({"observed_definition": definition}),
        "remove_node_name": node, "remove_source_id": source["source_id"], "remove_physical_id": _id(91),
    }
    assert not _evaluate(db, kernel, remote_absence_predicate(), params)
    params["binding_observation"] = _json({"observed_definition": _topology(original["sources"][:1])})
    assert _evaluate(db, kernel, remote_absence_predicate(), params)


def test_mixed_add_remove_binds_only_new_proposal_and_retires_only_proved_old_source(db, case):
    kernel, original = case
    target = {**original["sources"][0]["target"], "item_id": _id(113)}
    proposal = {"proposal_id": _id(50), "node_name": "proposed_c", "source_id": None,
                "target": target, "event_types": original["sources"][0]["event_types"], "event_source": "source-c"}
    staged = _stage(db, kernel, proposals=[proposal])
    observed = copy.deepcopy(staged["desired_definition"])
    observed["component_ids"]["sources/proposed_c"] = _id(13)
    _observe(db, kernel, observed)
    result = _confirm(db, kernel)
    sources = result["connector"]["sources"]
    assert sources == [original["sources"][0], {
        "source_id": _id(13), "target": target, "event_types": proposal["event_types"], "event_source": "source-c",
    }]
    assert result["connector"]["source_proposals"] == []
    assert [r["source_id"] for r in result["retired_sources"]] == [original["sources"][1]["source_id"]]
    assert result["state"] != "ready"


def test_uncertain_unbound_proposal_withdrawal_retains_null_identity_until_observed_absence(db, case):
    kernel, original = case
    proposal = {"proposal_id": _id(50), "node_name": "uncertain_c", "source_id": None,
                "target": {**original["sources"][0]["target"], "item_id": _id(113)},
                "event_types": original["sources"][0]["event_types"], "event_source": "source-c"}
    initial = {**original, "source_proposals": [proposal], "desired_definition": _topology(original["sources"], [proposal]),
               "observed_definition": _topology(original["sources"]), "state": "provisioning"}
    db.execute(f"UPDATE {kernel.names.table('monitoring_records')} SET payload=? WHERE record_kind='connector'", (_json(initial),))
    db.commit()
    staged = _stage(db, kernel, source_removals=[_intent(proposal, proposal=True)])
    assert staged["source_proposals"] == [proposal]
    assert staged["source_removals"][0]["source_id"] is None
    _observe(db, kernel, staged["desired_definition"])
    result = _confirm(db, kernel)
    assert result["connector"]["sources"] == original["sources"]
    assert result["connector"]["source_proposals"] == []
    assert result["retired_sources"][0]["source_id"] is None
    assert result["retired_sources"][0]["original_binding"] == proposal


def test_unowned_and_case_or_padding_distinct_selectors_cannot_remove_another_source(db, case):
    kernel, original = case
    for selector in (_id(90), original["sources"][1]["source_id"].upper(), original["sources"][1]["source_id"] + " "):
        if selector == original["sources"][1]["source_id"]:
            continue
        assert db.execute(_adapt(kernel, removal_binding_sql()), {
            "prior": _json(original), "remove_source_id": selector, "remove_proposal_id": None,
        }).fetchall() == []
    intent = _intent(original["sources"][1])
    old = _new_pending(original, intent)
    params = {"old_removal": _json(old), "remove_source_id": intent["source_id"], "remove_proposal_id": None,
              "remove_binding_hash": old["binding_hash"], "removal_intent": _json(intent)}
    assert _evaluate(db, kernel, original_removal_matches_sql(), params)
    for field, value in (("remove_source_id", intent["source_id"] + " "), ("remove_binding_hash", "different"),
                         ("remove_proposal_id", _id(90))):
        assert not _evaluate(db, kernel, original_removal_matches_sql(), {**params, field: value})


def test_pending_removals_cannot_be_cancelled_or_reidentified_without_absence(db, case):
    kernel, _ = case
    staged = _stage(db, kernel)
    body = prepare_removals_sql(kernel.names)
    predicate = _guard(body, "Pending removal cannot be cancelled by omitting its ownership record")
    for requested in ([], [{**_intent(staged["sources"][1]), "removal_id": _id(99)}]):
        assert _evaluate(db, kernel, predicate, {
            "prior": _json(staged), "removal_intents": _json(requested), "superseded_json": "[]",
        })
    assert "An old observation cannot authorize a new removal intent" in body
    assert "A retired removal identity cannot be reused" in body


@pytest.mark.parametrize("fault", ["both", "neither", "extra", "numeric", "wrong_case", "duplicate_field",
                                  "missing_null", "padded_null_key"])
def test_removal_selector_is_a_closed_typed_shape_with_explicit_nulls(db, case, fault):
    kernel, original = case
    intent = _intent(original["sources"][1])
    body = prepare_removals_sql(kernel.names)
    predicate = _guard(body, "A removal selects exactly one already-owned source or proposal with a stable request ID")
    assert not _evaluate(db, kernel, predicate, {"removal_intents": _json([intent])})
    if fault == "both":
        intent["proposal_id"] = _id(50)
    elif fault == "neither":
        intent["source_id"] = None
    elif fault == "extra":
        intent["state"] = "retired_verified"
    elif fault == "numeric":
        intent["source_id"] = 42
    elif fault == "wrong_case":
        intent["DETAIL"] = intent.pop("detail")
    elif fault == "missing_null":
        intent.pop("proposal_id")
    elif fault == "padded_null_key":
        intent["proposal_id "] = intent.pop("proposal_id")
    encoded = _json([intent])
    if fault == "duplicate_field":
        encoded = encoded.replace('"detail":', '"detail":"first","detail":')
    assert _evaluate(db, kernel, predicate, {"removal_intents": encoded})


def test_equivalent_source_json_keeps_original_binding_bytes_and_ownership_order(db, case):
    kernel, original = case
    prior = copy.deepcopy(original)
    prior["sources"] = [dict(reversed(tuple(s.items()))) for s in prior["sources"]]
    original_json = json.dumps(prior, separators=(",", ":"))
    requested = _json(prior["sources"])
    predicate = _guard(retained_ownership_sql(kernel.names),
                       "Desired changes must retain all owned source bindings until receipt-verified retirement")
    assert not _evaluate(db, kernel, predicate, {"prior": original_json, "sources": requested})
    body = retained_ownership_sql(kernel.names)
    assert "SET @sources=JSON_QUERY(@prior,'$.sources')" in body
    assert "ORDER BY ownership_order,position" in body
    assert "CONVERT(nvarchar(64),@remove_binding_hash)" in original_removal_matches_sql()
    assert "CONVERT(nvarchar(64),CONVERT(char(64)" in binding_observation_matches_sql(kernel.names)


def test_removal_contract_is_fixed_receipt_bound_and_has_no_raw_write_route():
    kernel = build_permission_kernel()
    publication = _sql(kernel, "controller.publish_connector")
    worker = _sql(kernel, "worker.observe_connector")
    assert publication.index("IF @prior_payload IS NOT NULL") < publication.index("DECLARE @removal_intents")
    assert publication.index(retirement_records_sql(kernel.names)) < publication.index(publish_update_sql(kernel.names))
    assert publication.index(publish_update_sql(kernel.names)) < publication.index(save_receipt(kernel.names, "controller.publish_connector"))
    assert "SET XACT_ABORT ON" in publication and "SET NOCOUNT ON" in publication
    assert "COMMIT TRAN" not in publication and "EXECUTE AS" not in publication
    allowed = worker.split("WHERE [key] NOT IN", 1)[1].split("THROW 51073", 1)[0]
    for forbidden in ("source_removals", "source_proposals", "desired_definition"):
        assert f"'{forbidden}'" not in allowed
    assert payload_hash("JSON_QUERY(@observation_json,'$.observed_definition')") + " AS observed_definition_hash" in worker
    assert kernel.rpcs["controller.publish_connector"].components == ("controller",)
    assert kernel.rpcs["controller.publish_connector"].result_fields[-3:] == (
        "pending_removals", "retired_sources", "observation_receipt_id",
    )
    assert "observed_definition_hash" in kernel.rpcs["worker.observe_connector"].result_fields
    for name in ("worker_catalogue", "worker_evidence", "worker_telemetry", "web_drafts",
                 "controller_projections", "controller_immutable"):
        assert "connector_source_retirement" not in _sql(kernel, name)
    assert "connector_source_retirement" in _sql(kernel, "controller_read")
    assert {role: len(grants) for role, grants in kernel.grants.items()} == {"worker": 20, "web": 12, "controller": 25}
    assert integration_contract()["unresolved_cases"] == []
