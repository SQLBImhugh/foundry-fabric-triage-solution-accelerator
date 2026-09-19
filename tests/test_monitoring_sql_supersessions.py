"""Execute emitted supersession guards offline; this is not native SQL proof."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta

import pytest
from test_monitoring_sql_removals import (
    _adapt,
    _apply_connector,
    _guard,
    _hash,
    _id,
    _insert_record,
    _intent,
    _json,
    _mutation_params,
    _replay,
    _save_receipt,
    _stage,
)
from test_monitoring_sql_removals import (
    case as case,
)
from test_monitoring_sql_removals import (
    db as db,
)

from triage.monitoring.sql_kernel_common import canonical_guid, current_work, key_hash, payload_hash
from triage.monitoring.sql_kernel_connectors import (
    desired_supersession_request_expression,
    desired_update_expression,
    invalidate_readiness_expression,
    restored_source_authorized_predicate,
    source_authorized_predicate,
)
from triage.monitoring.sql_kernel_proposals import complete_component_map_sql
from triage.monitoring.sql_kernel_removals import prepare_removals_sql
from triage.monitoring.sql_kernel_supersessions import (
    inspection_invalid_sql,
    prepare_supersession_selection_sql,
    restored_intake_invalid_sql,
    supersession_collection_work_sql,
    supersession_current_scope_sql,
    supersession_disposition_work_sql,
    supersession_history_invalid_sql,
    supersession_observation_matches_sql,
    supersession_observation_receipt_sql,
    supersession_original_removal_invalid_sql,
    supersession_presence_topology_invalid_sql,
    supersession_selector_invalid_sql,
    supersession_topology_delta_invalid_sql,
    supersession_uncertain_write_sql,
    validate_supersession_sql,
)


def _nested_modify(payload, path, value):
    result = json.loads(payload)
    tokens = [quoted or plain or int(index) for quoted, plain, index in
              re.findall(r'\."([^"]+)"|\.([^.\[]+)|\[(\d+)\]', path.removeprefix("$"))]
    current = result
    for token in tokens[:-1]:
        current = current[token]
    if value is None:
        if isinstance(current, dict):
            current.pop(tokens[-1], None)
        else:
            current[tokens[-1]] = None
    else:
        if tokens[-1] in {"sources", "source_proposals", "source_removals", "desired_definition", "gaps"}:
            value = json.loads(value)
        current[tokens[-1]] = value
    return _json(result)


def _sql(kernel, expression):
    for value in (
        "@supersession_request_binding", "JSON_QUERY(@supersession_patch,'$.observed_definition')",
        "JSON_QUERY(@prior,'$.desired_definition')", "owned.value",
    ):
        expression = expression.replace(f"CONVERT(nvarchar(64),{payload_hash(value)})", f"PAYLOAD_HASH({value})")
    for value in ("JSON_VALUE(selected.payload,'$.publication_id')", "JSON_VALUE(selected.payload,'$.request_id')",
                  "@binding_receipt_id", "N'work:v1:'+@epoch+N':'+@tenant_id+N':'+@supersession_work_id"):
        expression = expression.replace(key_hash(value), f"KEY_DIGEST({value})")
    expression = expression.replace("CONVERT(nvarchar(64),@binding_receipt_fingerprint)", "@binding_receipt_fingerprint")
    for value in ("component.[key]", "JSON_VALUE(@source_capability,'$.collector_identity_id')"):
        expression = expression.replace(canonical_guid(value), f"IS_GUID({value})=1")
    expression = expression.replace("TRY_CONVERT(datetimeoffset,", "TRY_CONVERT('datetimeoffset',")
    sql = _adapt(kernel, expression, concatenate=True).replace("@supersessions", "supersessions")
    sql = sql.replace("||(SELECT COUNT(*) FROM supersessions)", "+(SELECT COUNT(*) FROM supersessions)")
    sql = sql.replace(")||1", ")+1").replace("DATEADD(second,", "DATEADD('second',")
    return re.sub(r"LEFT\(LTRIM\((@\w+)\),1\)", r"substr(ltrim(\1),1,1)", sql)


def _invalid(db, kernel, expression, params):
    return db.execute(
        "SELECT CASE WHEN (" + _sql(kernel, expression) + ") THEN 1 ELSE 0 END", params,
    ).fetchone()[0] == 1


@pytest.fixture
def restoration(db, case):
    kernel, original = case
    staged = _stage(db, kernel)
    removal = staged["source_removals"][0]
    binding = original["sources"][1]
    db.create_function("JSON_MODIFY", 3, _nested_modify)
    db.create_function("DATEADD", 3, lambda unit, count, when:
                       (datetime.fromisoformat(when.replace("Z", "+00:00")) + timedelta(seconds=count)).isoformat().replace("+00:00", "Z"))
    db.execute("CREATE TEMP TABLE supersessions (removal_id TEXT,source_id TEXT,node_name TEXT,binding_json TEXT,payload TEXT)")
    db.execute("INSERT INTO supersessions VALUES (?,?,?,?,?)", (
        removal["removal_id"], removal["source_id"], removal["node_name"], _json(binding), _json(removal),
    ))
    observation = {**staged, "observed_definition": original["desired_definition"], "operation_id": None}
    params = {
        **_mutation_params(staged, _id(101)),
        "prior": _json(staged), "binding_observation": _json(observation),
        "definition": _json(original["desired_definition"]),
    }
    return kernel, original, staged, params


@pytest.mark.parametrize("change", [
    {"removal_id": None}, {"removal_id": "not-an-id"}, {"removal_id": _id(1).upper()},
    {"source_id": None}, {"source_id": ""}, {"source_id": "a" * 257}, {"source_id": 42},
    {"proposal_id": None}, {"detail": "Do not accept extra fields"},
])
def test_supersession_selector_is_closed_and_physically_bound(db, case, change):
    kernel, original = case
    selector = {"removal_id": _id(140), "source_id": original["sources"][1]["source_id"]}
    assert not _invalid(db, kernel, supersession_selector_invalid_sql(), {"supersession_intents": _json([selector])})
    selector.update(change)
    # IDs containing only digits do not have a distinct upper-case spelling.
    if change == {"removal_id": _id(1).upper()}:
        selector["removal_id"] = "abcdefab-1111-4111-8111-111111111111".upper()
    assert _invalid(db, kernel, supersession_selector_invalid_sql(), {"supersession_intents": _json([selector])})


@pytest.mark.parametrize("encoded", [
    '[{"removal_id":"0000008c-1111-4111-8111-111111111111","source_id":"a","source_id":"b"}]',
    '[{"removal_id":"0000008c-1111-4111-8111-111111111111","source_id ":"a"}]',
    '[{"removal_id":"0000008c-1111-4111-8111-111111111111","SOURCE_ID":"a"}]',
    '[{"removal_id":"0000008c-1111-4111-8111-111111111111"}]',
])
def test_supersession_selector_rejects_duplicate_missing_or_padded_fields(db, case, encoded):
    assert _invalid(db, case[0], supersession_selector_invalid_sql(), {"supersession_intents": encoded})


@pytest.mark.parametrize("fault", [
    "source_id", "missing_source_id", "source_node", "workspace", "item", "events", "wrong_node_id", "object_node_id",
    "stream_id", "destination_id", "stream_reference", "destination_reference",
])
def test_presence_requires_exact_retained_source_and_transport_bindings(db, restoration, fault):
    kernel, original, staged, params = restoration
    predicate = supersession_presence_topology_invalid_sql(kernel.names)
    assert not _invalid(db, kernel, predicate, params)
    observation = json.loads(params["binding_observation"])
    definition = observation["observed_definition"]
    node_name = staged["source_removals"][0]["node_name"]
    graph = definition["parts"]["eventstream.json"]
    if fault == "source_id":
        definition["component_ids"]["sources/" + node_name] = _id(999)
    elif fault == "missing_source_id":
        definition["component_ids"].pop("sources/" + node_name)
    elif fault == "source_node":
        graph["sources"][1]["name"] = "different_node"
    elif fault in {"workspace", "item"}:
        graph["sources"][1]["properties"][fault + "Id"] = _id(999)
    elif fault == "events":
        graph["sources"][1]["properties"]["includedEventTypes"] = []
    elif fault in {"wrong_node_id", "object_node_id"}:
        graph["sources"][1]["id"] = _id(999) if fault == "wrong_node_id" else {}
    elif fault in {"stream_id", "destination_id"}:
        key = "streams/owned_stream" if fault == "stream_id" else "destinations/owned_destination"
        definition["component_ids"][key] = _id(999)
    elif fault == "stream_reference":
        graph["streams"][0]["inputNodes"] = graph["streams"][0]["inputNodes"][:1]
    else:
        graph["destinations"][0]["inputNodes"] = [{"name": "another_stream"}]
    assert _invalid(db, kernel, predicate, {**params, "binding_observation": _json(observation)})
    assert original["sources"][1]["source_id"] == staged["source_removals"][0]["source_id"]


def _observed_topology_fault(definition, fault):
    graph = definition["parts"]["eventstream.json"]
    name = graph["sources"][0]["name"]
    if fault == "unowned_source":
        node = copy.deepcopy(graph["sources"][0])
        node["name"] = "unowned_source"
        node["properties"]["itemId"] = _id(990)
        graph["sources"].append(node)
        graph["streams"][0]["inputNodes"].append({"name": node["name"]})
        definition["component_ids"]["sources/" + node["name"]] = _id(991)
    elif fault == "changed_other_target":
        graph["sources"][0]["properties"]["itemId"] = _id(990)
    elif fault == "changed_other_events":
        graph["sources"][0]["properties"]["includedEventTypes"] = ["Microsoft.Fabric.JobEvents.ItemJobSucceeded"]
    elif fault == "changed_other_id":
        definition["component_ids"]["sources/" + name] = _id(990)
    elif fault == "missing_old_node":
        graph["sources"].pop(0)
        definition["component_ids"].pop("sources/" + name)
        graph["streams"][0]["inputNodes"] = [item for item in graph["streams"][0]["inputNodes"] if item["name"] != name]
    elif fault == "renamed_old_node":
        graph["sources"][0]["name"] = "renamed_source"
        definition["component_ids"]["sources/renamed_source"] = definition["component_ids"].pop("sources/" + name)
        graph["streams"][0]["inputNodes"][0]["name"] = "renamed_source"
    elif fault == "changed_old_property":
        graph["sources"][0]["properties"]["unexpected"] = "not in the published source"
    elif fault == "unowned_operator":
        graph["operators"] = [{"name": "unowned_operator", "type": "Filter"}]
    elif fault == "missing_other_route":
        graph["streams"][0]["inputNodes"] = graph["streams"][0]["inputNodes"][1:]
    elif fault == "duplicate_route":
        graph["streams"][0]["inputNodes"][0] = graph["streams"][0]["inputNodes"][1]
    else:
        raise AssertionError(f"Unrecognized observation fault: {fault}")


@pytest.mark.parametrize("fault", [
    "unowned_source", "changed_other_target", "changed_other_events", "changed_other_id",
    "missing_old_node", "renamed_old_node", "changed_old_property", "unowned_operator",
    "missing_other_route", "duplicate_route",
])
def test_selected_presence_cannot_hide_drift_in_the_rest_of_a_complete_observation(db, restoration, fault):
    kernel, _, _, params = restoration
    observed = json.loads(params["binding_observation"])
    _observed_topology_fault(observed["observed_definition"], fault)
    params["binding_observation"] = _json(observed)
    assert _invalid(db, kernel, complete_component_map_sql(), params)
    assert _invalid(db, kernel, supersession_presence_topology_invalid_sql(kernel.names), params)


def test_old_desired_node_may_differ_only_by_its_already_verified_explicit_id(db, restoration):
    kernel, original, _, params = restoration
    observed = json.loads(params["binding_observation"])
    observed["observed_definition"]["parts"]["eventstream.json"]["sources"][0]["id"] = original["sources"][0]["source_id"]
    params["binding_observation"] = _json(observed)
    assert _invalid(db, kernel, complete_component_map_sql(), params)
    assert not _invalid(db, kernel, supersession_presence_topology_invalid_sql(kernel.names), params)


@pytest.mark.parametrize("fault", [
    "omit_restoration", "change_retained", "extra_operator", "extra_property", "replace_stream",
    "replace_component", "missing_component", "padded_component", "change_restored", "wrong_node_id", "null_node_id",
])
def test_supersession_cannot_hide_other_desired_topology_changes(db, restoration, fault):
    kernel, _, staged, params = restoration
    predicate = supersession_topology_delta_invalid_sql(kernel.names)
    assert not _invalid(db, kernel, predicate, params)
    definition = json.loads(params["definition"])
    graph = definition["parts"]["eventstream.json"]
    node = staged["source_removals"][0]["node_name"]
    if fault == "omit_restoration":
        definition = staged["desired_definition"]
    elif fault == "change_retained":
        graph["sources"][0]["properties"]["includedEventTypes"] = []
    elif fault == "extra_operator":
        graph["operators"].append({"name": "extra"})
    elif fault == "extra_property":
        graph["destinations"][0]["properties"] = {"changed": True}
    elif fault == "replace_stream":
        graph["streams"][0]["name"] = "different_stream"
    elif fault == "replace_component":
        definition["component_ids"]["streams/owned_stream"] = _id(999)
    elif fault == "missing_component":
        definition["component_ids"].pop("sources/" + node)
    elif fault == "padded_component":
        definition["component_ids"]["sources/" + node + " "] = definition["component_ids"].pop("sources/" + node)
    elif fault in {"wrong_node_id", "null_node_id"}:
        graph["sources"][1]["id"] = _id(999) if fault == "wrong_node_id" else None
    else:
        graph["sources"][1]["properties"]["itemId"] = _id(999)
    assert _invalid(db, kernel, predicate, {**params, "definition": _json(definition)})


@pytest.mark.parametrize("fault", ["current_operation", "observed_operation", "current_pending", "observed_pending",
                                  "historical_operation", "historical_submission", "historical_uncertain"])
def test_clearing_latest_metadata_cannot_supersede_dispatched_or_uncertain_removal(db, restoration, fault):
    kernel, _, _, params = restoration
    predicate = supersession_uncertain_write_sql(kernel.names)
    assert not _invalid(db, kernel, predicate, params)
    if fault.startswith("historical"):
        observed = json.loads(params["binding_observation"])
        observed["operation_id"] = _id(999) if fault == "historical_operation" else None
        code = "definition_update_submitted_or_unknown" if fault == "historical_submission" else "definition_update_outcome_unknown"
        observed["gaps"] = [] if fault == "historical_operation" else [{"code": code, "detail": "Original effect evidence"}]
        _save_receipt(db, kernel, "worker.observe_connector", params, {
            "connector_id": _id(3), "observation": observed,
        })
    else:
        field = "prior" if fault.startswith("current") else "binding_observation"
        value = json.loads(params[field])
        if fault.endswith("operation"):
            value["operation_id"] = _id(999)
        else:
            value["gaps"] = [{"code": "definition_update_pending", "detail": "Original pending operation"}]
        params[field] = _json(value)
    assert _invalid(db, kernel, predicate, params)


def _scope(db, kernel, target):
    value = {
        "scope_id": _id(200), "enabled": True, "rules": [{
            "rule_id": _id(201), "effect": "include", "workloads": ["fabric_pipeline"],
            "selector": {"tenant_id": _id(1), "kind": "workspace", "workspace_id": target["workspace_id"]},
        }],
    }
    _insert_record(db, kernel, "scope", _id(200), value)
    return value


@pytest.mark.parametrize("admission_basis", ["reviewed", "auto_detection_only"])
@pytest.mark.parametrize("fault", [
    "disabled_scope", "removed_scope_reference", "removed_rule_reference", "pending_review", "missing_basis",
    "different_workspace", "different_workload", "actual_exclusion", "domain_exclusion",
])
def test_restoration_requires_affirmative_scope_not_merely_missing_exclusion(db, restoration, admission_basis, fault):
    kernel, original, _, params = restoration
    target = original["sources"][1]["target"]
    scope = _scope(db, kernel, target)
    approved = {"admission_basis": admission_basis, "scope_ids": [_id(200)], "admitted_rule_ids": [_id(201)]}
    predicate = supersession_current_scope_sql(kernel.names)
    params.update(source_target=_json(target), approved_target=_json(approved))
    assert _invalid(db, kernel, predicate, params)
    if fault == "disabled_scope":
        scope["enabled"] = False
    elif fault == "removed_scope_reference":
        approved["scope_ids"] = []
    elif fault == "removed_rule_reference":
        approved["admitted_rule_ids"] = []
    elif fault == "pending_review":
        approved["admission_basis"] = "pending_review"
    elif fault == "missing_basis":
        approved.pop("admission_basis")
    elif fault == "different_workspace":
        scope["rules"][0]["selector"]["workspace_id"] = _id(999)
    elif fault == "different_workload":
        scope["rules"][0]["workloads"] = ["powerbi"]
    else:
        excluded = copy.deepcopy(scope["rules"][0])
        excluded.update(rule_id=_id(202), effect="exclude")
        if fault == "domain_exclusion":
            excluded["selector"] = {"tenant_id": _id(1), "kind": "domain", "domain_id": _id(203)}
        scope["rules"].append(excluded)
    db.execute(f"UPDATE {kernel.names.table('monitoring_records')} SET payload=? WHERE record_kind='scope'", (_json(scope),))
    assert not _invalid(db, kernel, predicate, {**params, "approved_target": _json(approved)})


def _terminal_collection(db, kernel, params):
    work = {
        "tenant_id": _id(1), "epoch": _id(2), "work_id": _id(210), "connector_id": _id(3),
        "kind": "connector_reconcile", "state": "completed", "revision": 4, "retry_attempt": 0,
        "lease": None, "attempts": 1, "completed_at": "2026-09-16T12:01:00Z",
    }
    _insert_record(db, kernel, "work", work["work_id"], work, revision=4)
    db.execute(f"UPDATE {kernel.names.table('monitoring_records')} SET work_kind='connector_reconcile' "
               "WHERE record_kind='work'")
    lease_key = f"work:v1:{_id(2)}:{_id(1)}:{work['work_id']}"
    db.execute(f"INSERT INTO {kernel.names.table('monitoring_leases')} VALUES (?,?,?,?,?,?,?)", (
        _id(1), _id(2), lease_key, hashlib.sha256(lease_key.encode()).digest(), _id(211), 9,
        "2026-09-16T12:01:00Z",
    ))
    _save_receipt(db, kernel, "worker.transition_work", {**params, "now": work["completed_at"]}, {
        "work_id": work["work_id"], "work": work,
    })
    params.update({
        "now": "2026-09-16T12:02:00Z",
        "supersession_work_id": work["work_id"], "supersession_work": _json(work), "supersession_work_revision": 4,
        "supersession_observed_at": "2026-09-16T12:00:00Z",
        "binding_receipt_payload": _json({"result": {
            "work_id": work["work_id"], "work_owner_id": _id(211), "work_fence": 9, "work_revision": 3,
            "collection_completion_eligible": True,
        }}),
    })
    return work


@pytest.mark.parametrize("fault", ["active_lease", "wrong_fence", "wrong_owner", "foreign_work", "missing_original_completion",
                                  "not_completed", "later_revision", "missing_observation_revision", "action_lineage"])
def test_original_presence_producer_must_finish_under_its_exact_collection_fence(db, restoration, fault):
    kernel, _, _, params = restoration
    work = _terminal_collection(db, kernel, params)
    predicate = _guard(supersession_collection_work_sql(kernel.names),
                       "Supersession requires the original inspection collection to be terminal under its exact fence")
    assert not _invalid(db, kernel, predicate, params)
    if fault in {"active_lease", "wrong_fence", "wrong_owner"}:
        column, value = {
            "active_lease": ("expires_at", "2026-09-16T13:00:00Z"),
            "wrong_fence": ("fence", 10), "wrong_owner": ("owner_id", _id(999)),
        }[fault]
        db.execute(f"UPDATE {kernel.names.table('monitoring_leases')} SET {column}=? WHERE owner_id=?", (value, _id(211)))
    elif fault == "missing_original_completion":
        db.execute(f"DELETE FROM {kernel.names.table('monitoring_receipts')} WHERE operation='worker.transition_work'")
    elif fault == "missing_observation_revision":
        receipt = json.loads(params["binding_receipt_payload"])
        receipt["result"].pop("work_revision")
        params["binding_receipt_payload"] = _json(receipt)
    else:
        work.update({
            "foreign_work": {"work_id": _id(999)}, "not_completed": {"state": "leased"},
            "later_revision": {"revision": 5}, "action_lineage": {"action_reservation_id": _id(999)},
        }[fault])
        params["supersession_work"] = _json(work)
    assert _invalid(db, kernel, predicate, params)


def _observation_context(params):
    observed = json.loads(params["binding_observation"])
    observed.update(revision=observed["revision"] + 1, state="degraded", gaps=[{
        "code": "pending_source_removal_readmitted_requires_supersession",
        "detail": "Original complete read-only inspection; no definition update was sent",
    }])
    patch = {"observed_definition": observed["observed_definition"], "state": "degraded", "gaps": observed["gaps"]}
    binding = {
        "tenant_id": _id(1), "epoch": _id(2), "expected_revision": 3, "work_id": _id(210),
        "owner_id": _id(211), "fence": 9, "work_revision": 3, "connector_id": _id(3), "ownership_id": _id(4),
        "expected_connector_revision": observed["revision"] - 1, "observation_json": _json(patch),
    }
    receipt = {"binding_hash": _hash(_json(binding)), "result": {
        "connector_id": _id(3), "connector": observed, "observation": observed,
        "work_id": binding["work_id"], "work_owner_id": binding["owner_id"],
        "work_fence": binding["fence"], "work_revision": binding["work_revision"],
        "observed_definition_hash": _hash(_json(patch["observed_definition"])),
        "authority": "observed_not_action_authority", "reconcile_work_id": params["work_id"],
        "frontier_key": "fixture-global-frontier", "frontier_revision": 1,
    }}
    request = {
        "producer": "worker", "topic": "connector", "reference_id": _id(3), "work_id": params["work_id"],
        "request_id": _id(212), "policy_revision": 3, "request_payload": binding, "fingerprint": "f" * 64,
    }
    desired = {
        "ownership_id": _id(4), "policy_revision": 3,
        "definition_hash": _hash(_json(observed["desired_definition"])), "published_at": params["now"],
    }
    params.update(
        prior=_json(observed), binding_observation=_json(observed), expected_connector_revision=observed["revision"],
        supersession_request=_json(request), supersession_request_binding=_json(binding), supersession_patch=_json(patch),
        supersession_observed_at="2026-09-16T12:01:00Z", now="2026-09-16T12:02:00Z",
        binding_receipt_id=_id(212), binding_receipt_payload=_json(receipt), binding_receipt_fingerprint="f" * 64,
        plan=_json({"producer_fingerprint": "f" * 64, "frontier_key": "fixture-global-frontier", "frontier_revision": 1}),
        desired=_json(desired), proposals="[]",
    )


@pytest.mark.parametrize("field,value", [
    ("supersession_observed_at", None), ("supersession_observed_at", "2026-09-16T11:50:00Z"),
    ("supersession_observed_at", "2026-09-16T12:05:00Z"), ("binding_observation", None),
    ("supersession_request", None), ("supersession_request_binding", None), ("supersession_patch", None),
    ("expected_connector_revision", 11), ("current_revision", 4), ("binding_receipt_fingerprint", "e" * 64),
    ("ownership_id", _id(999)), ("connector_id", _id(999)), ("work_id", _id(999)),
])
def test_supersession_original_observation_cannot_use_stale_or_missing_context(db, restoration, field, value):
    kernel, _, _, params = restoration
    _observation_context(params)
    predicate = supersession_observation_matches_sql(kernel.names)
    assert _invalid(db, kernel, predicate, params)
    assert not _invalid(db, kernel, predicate, {**params, field: value})


@pytest.mark.parametrize("kind", [
    "inherited_definition", "changed_raw_input", "different_original_hash", "different_desired",
    "different_frontier", "wrong_collection_fence", "different_transport", "missing_producer",
])
def test_supersession_decodes_original_input_and_never_borrows_current_observed_definition(db, restoration, kind):
    kernel, _, _, params = restoration
    _observation_context(params)
    predicate = supersession_observation_matches_sql(kernel.names)
    assert _invalid(db, kernel, predicate, params)
    if kind in {"inherited_definition", "changed_raw_input"}:
        patch = json.loads(params["supersession_patch"])
        if kind == "inherited_definition":
            patch.pop("observed_definition")
        else:
            patch["observed_definition"]["component_ids"]["streams/owned_stream"] = _id(999)
        params["supersession_patch"] = _json(patch)
    elif kind == "different_original_hash":
        receipt = json.loads(params["binding_receipt_payload"])
        receipt["binding_hash"] = "F" * 64
        params["binding_receipt_payload"] = _json(receipt)
    elif kind == "different_frontier":
        plan = json.loads(params["plan"])
        plan["frontier_revision"] = 2
        params["plan"] = _json(plan)
    elif kind == "wrong_collection_fence":
        binding = json.loads(params["supersession_request_binding"])
        binding["fence"] += 1
        params["supersession_request_binding"] = _json(binding)
    elif kind == "missing_producer":
        request = json.loads(params["supersession_request"])
        request.pop("producer")
        params["supersession_request"] = _json(request)
    else:
        observation = json.loads(params["binding_observation"])
        if kind == "different_desired":
            observation["desired_definition"] = observation["observed_definition"]
        else:
            observation["eventstream_id"] = _id(999)
        params["binding_observation"] = _json(observation)
    assert not _invalid(db, kernel, predicate, params)


@pytest.mark.parametrize("fault", [
    "missing_receipt", "missing_publication", "wrong_publication", "different_original_removal",
    "different_original_binding", "wrong_policy", "observation_before_removal",
])
def test_supersession_proves_the_exact_original_removal_publication_not_just_latest_state(db, restoration, fault):
    kernel, original, _, params = restoration
    params["supersession_observed_at"] = "2026-09-16T12:01:00Z"
    publication = {
        "connector_id": _id(3), "ownership_id": _id(4), "policy_revision": 3,
        "expected_connector_revision": 8, "source_removals": [_intent(original["sources"][1])],
    }
    _insert_record(db, kernel, "connector_publication", _id(7), publication)
    predicate = supersession_original_removal_invalid_sql(kernel.names)
    assert not _invalid(db, kernel, predicate, params)
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    if fault == "missing_receipt":
        db.execute(f"DELETE FROM {receipts} WHERE operation='controller.publish_connector'")
    elif fault == "missing_publication":
        db.execute(f"DELETE FROM {records} WHERE record_kind='connector_publication'")
    elif fault == "wrong_publication":
        publication["ownership_id"] = _id(999)
        db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='connector_publication'", (_json(publication),))
    elif fault in {"different_original_removal", "different_original_binding"}:
        receipt = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE operation='controller.publish_connector'").fetchone()[0])
        if fault == "different_original_removal":
            receipt["result"]["pending_removals"][0]["detail"] = "Changed original removal"
        else:
            receipt["result"]["connector"]["sources"][1]["event_source"] = "changed-event-source"
        db.execute(f"UPDATE {receipts} SET payload=? WHERE operation='controller.publish_connector'", (_json(receipt),))
    elif fault == "wrong_policy":
        params["current_revision"] = 4
    else:
        params["supersession_observed_at"] = "2026-09-16T11:59:00Z"
    assert _invalid(db, kernel, predicate, params)


@pytest.mark.parametrize("fault", [
    "missing_history", "duplicate_revision", "different_owner", "changed_source_binding",
    "changed_removal", "missing_original", "unreceipted_revision",
])
def test_supersession_requires_contiguous_unambiguous_retained_connector_receipt_history(db, restoration, fault):
    kernel, _, staged, params = restoration
    history = []
    for revision in (10, 11):
        observed = {**staged, "revision": revision, "state": "degraded", "operation_id": None}
        history.append(observed)
        _save_receipt(db, kernel, "worker.observe_connector", {
            **params, "request_id": _id(300 + revision), "now": f"2026-09-16T12:0{revision - 9}:00Z",
        }, {"connector_id": _id(3), "connector": observed, "observation": observed})
    params["expected_connector_revision"] = 11
    predicate = supersession_history_invalid_sql(kernel.names)
    assert not _invalid(db, kernel, predicate, params)
    receipts = kernel.names.table("monitoring_receipts")
    if fault == "missing_history":
        db.execute(f"DELETE FROM {receipts} WHERE request_id=?", (_id(310),))
    elif fault == "duplicate_revision":
        _save_receipt(db, kernel, "worker.observe_connector", {**params, "request_id": _id(399)}, {
            "connector_id": _id(3), "connector": history[0], "observation": history[0],
        })
    elif fault in {"different_owner", "changed_source_binding", "changed_removal"}:
        result = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE request_id=?", (_id(310),)).fetchone()[0])
        connector = result["result"]["connector"]
        if fault == "different_owner":
            connector["ownership_id"] = _id(999)
        elif fault == "changed_source_binding":
            connector["sources"][1]["source_id"] = _id(999)
        else:
            connector["source_removals"][0]["detail"] = "Changed after the original removal"
        db.execute(f"UPDATE {receipts} SET payload=? WHERE request_id=?", (_json(result), _id(310)))
    elif fault == "missing_original":
        db.execute(f"DELETE FROM {receipts} WHERE operation='controller.publish_connector'")
    else:
        params["expected_connector_revision"] = 12
    assert _invalid(db, kernel, predicate, params)


def test_pending_effect_in_original_publication_is_not_lost_by_clean_later_inspection(db, restoration):
    kernel, _, _, params = restoration
    receipts = kernel.names.table("monitoring_receipts")
    original = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE operation='controller.publish_connector'").fetchone()[0])
    original["result"]["connector"]["gaps"] = [{
        "code": "definition_update_submitted_or_unknown", "detail": "Original durable pre-POST intent",
    }]
    db.execute(f"UPDATE {receipts} SET payload=? WHERE operation='controller.publish_connector'", (_json(original),))
    assert _invalid(db, kernel, supersession_uncertain_write_sql(kernel.names), params)


def _inspection(definition, *, observed_at="2026-09-16T12:00:00Z"):
    return {
        "observed_at": observed_at, "read_only": True, "definition_hash": _hash(_json(definition)),
        "component_states": dict.fromkeys(definition["component_ids"].values(), "Running"),
    }


@pytest.mark.parametrize("fault", [
    "absent", "too_old", "future", "not_read_only", "partial_states", "missing_flag", "numeric_flag",
    "string_flag", "different_hash", "not_running", "padded_running", "different_key", "invalid_id",
    "missing_component", "duplicate_component", "extra_field", "invalid_component_state",
])
def test_inspection_is_explicit_complete_fresh_and_running_not_a_gap_label(db, restoration, fault):
    kernel, original, _, params = restoration
    inspection = _inspection(original["desired_definition"])
    params["inspection"] = _json(inspection)
    predicate = inspection_invalid_sql("@inspection", "@binding_observation")
    assert not _invalid(db, kernel, predicate, params)
    component_id = next(iter(inspection["component_states"]))
    if fault == "absent":
        inspection = None
    elif fault in {"too_old", "future"}:
        inspection["observed_at"] = "2026-09-16T11:54:59Z" if fault == "too_old" else "2026-09-16T12:00:01Z"
    elif fault == "not_read_only":
        inspection["read_only"] = False
    elif fault == "partial_states":
        inspection["component_states"].pop(component_id)
    elif fault == "missing_flag":
        inspection.pop("read_only")
    elif fault == "numeric_flag":
        inspection["read_only"] = 1
    elif fault == "string_flag":
        inspection["read_only"] = "true"
    elif fault == "different_hash":
        inspection["definition_hash"] = "F" * 64
    elif fault in {"not_running", "padded_running"}:
        inspection["component_states"][component_id] = "Stopped" if fault == "not_running" else "Running "
    elif fault == "different_key":
        inspection["component_states"][_id(999)] = inspection["component_states"].pop(component_id)
    elif fault == "invalid_id":
        inspection["component_states"]["not-a-guid"] = inspection["component_states"].pop(component_id)
    elif fault == "missing_component":
        inspection.pop("component_states")
    elif fault == "duplicate_component":
        serialized = _json(inspection).replace(f'"{component_id}":', f'"{component_id}":"Running","{component_id}":')
        assert _invalid(db, kernel, predicate, {**params, "inspection": serialized})
        return
    elif fault == "extra_field":
        inspection["ready"] = True
    else:
        inspection["component_states"][component_id] = {"trusted": True, "status": "Running"}
    params["inspection"] = _json(inspection) if inspection is not None else None
    assert _invalid(db, kernel, predicate, params)


def test_running_inspection_freshness_boundary_is_exactly_300_seconds(db, restoration):
    kernel, original, _, params = restoration
    predicate = inspection_invalid_sql("@inspection", "@binding_observation")
    inspection = _inspection(original["desired_definition"], observed_at="2026-09-16T11:55:00Z")
    assert not _invalid(db, kernel, predicate, {**params, "inspection": _json(inspection)})
    inspection["observed_at"] = "2026-09-16T11:54:59.999999Z"
    assert _invalid(db, kernel, predicate, {**params, "inspection": _json(inspection)})


def test_sql_inspection_fixture_matches_the_shared_closed_dto(restoration):
    from triage.monitoring.models import ConnectorPresenceInspection

    inspection = _inspection(restoration[1]["desired_definition"])
    assert ConnectorPresenceInspection.model_validate(inspection).model_dump(mode="json") == inspection


def _read_admission(db, kernel, source, *, event_status="unknown"):
    target = source["target"]
    key = ":".join(("monitor:v1", target["epoch"], target["tenant_id"], target["workload"],
                    target["workspace_id"], target["item_id"]))
    approved = {
        "identity": target, "state": "current", "policy_revision": 3, "admission_basis": "reviewed",
        "observation": {"enabled": True, "events_enabled": event_status == "verified"},
        "scope_ids": [_id(200)], "admitted_rule_ids": [_id(201)], "capability_id": _id(401),
        "inventory_generation": _id(402),
    }
    capability = {
        "target": target, "capability_id": _id(401), "inventory_generation": _id(402), "collector_identity_id": _id(400),
        "read_status": "verified", "event_status": event_status,
        "checked_at": "2026-09-16T11:59:00Z", "expires_at": "2026-09-16T13:00:00Z",
    }
    _insert_record(db, kernel, "target", key, approved)
    _insert_record(db, kernel, "target_capability", key, capability)
    return approved, capability


@pytest.mark.parametrize("admission_basis", ["reviewed", "auto_detection_only"])
@pytest.mark.parametrize("fault", [
    "event_denied", "event_blocked", "read_denied", "expired", "future_read", "malformed_read_identity", "wrong_generation",
    "wrong_capability", "pending_review", "different_policy", "disabled_observation",
])
def test_only_selected_owned_source_can_restore_with_unknown_event_capability(db, restoration, admission_basis, fault):
    kernel, original, _, params = restoration
    source = original["sources"][1]
    _scope(db, kernel, source["target"])
    approved, capability = _read_admission(db, kernel, source)
    approved["admission_basis"] = admission_basis
    params.update(source_target=_json(source["target"]), approved_target=_json(approved),
                  source_capability=_json(capability),
                  supersession_inspection=_json(_inspection(original["desired_definition"])))
    predicate = restored_source_authorized_predicate(kernel.names)
    assert _invalid(db, kernel, predicate, params)
    assert not _invalid(db, kernel, source_authorized_predicate(), params)
    if fault in {"event_denied", "event_blocked"}:
        capability["event_status"] = fault.removeprefix("event_")
    elif fault == "read_denied":
        capability["read_status"] = "denied"
    elif fault == "expired":
        capability["expires_at"] = params["now"]
    elif fault == "future_read":
        capability["checked_at"] = "2026-09-16T12:00:01Z"
    elif fault == "malformed_read_identity":
        capability["collector_identity_id"] = "not-a-guid"
    elif fault == "wrong_generation":
        capability["inventory_generation"] = _id(999)
    elif fault == "wrong_capability":
        capability["capability_id"] = _id(999)
    elif fault == "pending_review":
        approved["admission_basis"] = "pending_review"
    elif fault == "different_policy":
        approved["policy_revision"] = 2
    else:
        approved["observation"]["enabled"] = False
    assert not _invalid(db, kernel, predicate, {
        **params, "approved_target": _json(approved), "source_capability": _json(capability),
    })


def _restored_intake(db, kernel, original, staged, params):
    source = original["sources"][1]
    _read_admission(db, kernel, source, event_status="verified")
    records = kernel.names.table("monitoring_records")
    capability = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='target_capability'").fetchone()[0])
    capability["checked_at"] = "2026-09-16T12:02:01Z"
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='target_capability'", (_json(capability),))
    db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.observation.events_enabled',json('false')) "
               "WHERE record_kind='target'")
    restored = {**original, "revision": 12, "state": "degraded", "identity_verified_at": None, "delivery_verified_at": None}
    _insert_record(db, kernel, "connector_desired", _id(3), {
        "supersession_request_id": _id(410), "published_at": "2026-09-16T12:02:00Z",
    })
    _save_receipt(db, kernel, "controller.publish_connector", {**params, "request_id": _id(410)}, {
        "connector_id": _id(3), "connector": {**restored, "revision": 11, "state": "provisioning"},
        "superseded_source_removals": staged["source_removals"],
    })
    _save_receipt(db, kernel, "worker.observe_connector", {**params, "request_id": _id(411)}, {
        "connector_id": _id(3), "connector": restored, "observation": restored,
    })
    position = {
        "observation": {"execution": {"target": source["target"]}},
        "transport": {"source_id": source["source_id"], "collector_identity_id": _id(400),
                      "identity_verified_at": "2026-09-16T12:02:02Z"},
        "position": {"enqueued_at": "2026-09-16T12:02:03Z"}, "received_at": "2026-09-16T12:02:04Z",
    }
    db.execute("CREATE TEMP TABLE incoming_positions (disposition TEXT,payload TEXT)")
    db.execute("INSERT INTO incoming_positions VALUES ('accepted',?)", (_json(position),))
    params.update(connector=_json(restored), now="2026-09-16T12:03:00Z")
    return capability, position


@pytest.mark.parametrize("fault", [
    "unknown_event", "denied_event", "stale_capability", "expired_capability", "missing_transport",
    "old_identity", "other_identity", "old_delivery", "missing_publication_receipt", "observation_disabled",
    "target_paused", "wrong_policy", "future_receive", "enqueue_after_receive",
])
def test_degraded_after_restoration_does_not_bypass_new_capability_and_identity_proof(db, restoration, fault):
    kernel, original, staged, params = restoration
    capability, position = _restored_intake(db, kernel, original, staged, params)
    predicate = restored_intake_invalid_sql(kernel.names).replace("@positions", "incoming_positions")
    assert not _invalid(db, kernel, predicate, params)
    records = kernel.names.table("monitoring_records")
    if fault in {"unknown_event", "denied_event", "stale_capability", "expired_capability"}:
        if fault.endswith("event"):
            capability["event_status"] = "unknown" if fault == "unknown_event" else "denied"
        elif fault == "stale_capability":
            capability["checked_at"] = "2026-09-16T12:01:59Z"
        else:
            capability["expires_at"] = params["now"]
        db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='target_capability'", (_json(capability),))
    elif fault == "missing_publication_receipt":
        db.execute(f"DELETE FROM {kernel.names.table('monitoring_receipts')} WHERE request_id=?", (_id(410),))
    elif fault == "observation_disabled":
        db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.observation.enabled',json('false')) "
                   "WHERE record_kind='target'")
    elif fault == "target_paused":
        db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.state','paused') WHERE record_kind='target'")
    elif fault == "wrong_policy":
        db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.policy_revision',2) WHERE record_kind='target'")
    else:
        if fault == "missing_transport":
            position.pop("transport")
        elif fault == "old_identity":
            position["transport"]["identity_verified_at"] = "2026-09-16T12:01:59Z"
        elif fault == "other_identity":
            position["transport"]["collector_identity_id"] = _id(999)
        elif fault == "future_receive":
            position["received_at"] = "2026-09-16T12:03:01Z"
        elif fault == "enqueue_after_receive":
            position["position"]["enqueued_at"] = "2026-09-16T12:02:05Z"
        else:
            position["position"]["enqueued_at"] = "2026-09-16T12:01:59Z"
        db.execute("UPDATE incoming_positions SET payload=?", (_json(position),))
    assert _invalid(db, kernel, predicate, params)
    db.execute("UPDATE incoming_positions SET disposition='quarantined'")
    assert not _invalid(db, kernel, predicate, params)


def test_first_qualifying_restored_event_does_not_require_already_published_readiness(db, restoration):
    kernel, original, staged, params = restoration
    _restored_intake(db, kernel, original, staged, params)
    target = json.loads(db.execute(f"SELECT payload FROM {kernel.names.table('monitoring_records')} "
                                   "WHERE record_kind='target'").fetchone()[0])
    connector = json.loads(params["connector"])
    assert target["observation"]["enabled"] is True
    assert target["observation"]["events_enabled"] is False
    assert connector["state"] == "degraded"
    assert connector["identity_verified_at"] is None and connector["delivery_verified_at"] is None
    assert not _invalid(db, kernel, restored_intake_invalid_sql(kernel.names).replace("@positions", "incoming_positions"), params)


@pytest.mark.parametrize("prior_marker", [None, _id(410)])
def test_ordinary_desired_publication_keeps_the_original_recovery_marker(db, restoration, prior_marker):
    kernel, _, _, _ = restoration
    prior = None if prior_marker is None else _json({"supersession_request_id": prior_marker})
    expression = desired_supersession_request_expression()
    params = {"request_id": _id(421), "desired": prior}
    assert db.execute("SELECT " + _sql(kernel, expression), params).fetchone()[0] == (prior_marker or _id(421))
    db.execute("DELETE FROM supersessions")
    assert db.execute("SELECT " + _sql(kernel, expression), params).fetchone()[0] == prior_marker
    body = next(obj.ddl for obj in kernel.objects if obj.logical_name == "controller.publish_connector")
    assert expression + " AS supersession_request_id" in body


def test_ordinary_name_change_cannot_erase_recovery_intake_requirements(db, restoration):
    kernel, original, staged, params = restoration
    capability, _ = _restored_intake(db, kernel, original, staged, params)
    records = kernel.names.table("monitoring_records")
    desired = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='connector_desired'").fetchone()[0])
    db.execute("DELETE FROM supersessions")
    marker = db.execute("SELECT " + _sql(kernel, desired_supersession_request_expression()), {
        "request_id": _id(421), "desired": _json(desired),
    }).fetchone()[0]
    assert marker == _id(410)
    desired.update(supersession_request_id=marker, published_at="2026-09-16T12:02:30Z")
    capability["event_status"] = "unknown"
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='connector_desired'", (_json(desired),))
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='target_capability'", (_json(capability),))
    assert _invalid(db, kernel, restored_intake_invalid_sql(kernel.names).replace("@positions", "incoming_positions"), params)


@pytest.mark.parametrize("fault", ["missing_revision", "duplicate_revision", "different_owner"])
def test_recovery_intake_requires_contiguous_original_history_after_its_anchor(db, restoration, fault):
    kernel, original, staged, params = restoration
    _restored_intake(db, kernel, original, staged, params)
    predicate = restored_intake_invalid_sql(kernel.names).replace("@positions", "incoming_positions")
    assert not _invalid(db, kernel, predicate, params)
    receipts = kernel.names.table("monitoring_receipts")
    if fault == "missing_revision":
        db.execute(f"DELETE FROM {receipts} WHERE request_id=?", (_id(411),))
    else:
        payload = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE request_id=?", (_id(411),)).fetchone()[0])
        if fault == "duplicate_revision":
            _save_receipt(db, kernel, "worker.observe_connector", {**params, "request_id": _id(412)}, payload["result"])
        else:
            payload["result"]["connector"]["ownership_id"] = _id(999)
            db.execute(f"UPDATE {receipts} SET payload=? WHERE request_id=?", (_json(payload), _id(411)))
    assert _invalid(db, kernel, predicate, params)


@pytest.mark.parametrize("source_index", [0, 1])
def test_later_supersession_preserves_each_still_owned_sources_original_intake_fence(db, restoration, source_index):
    kernel, original, staged, params = restoration
    _, position = _restored_intake(db, kernel, original, staged, params)
    _read_admission(db, kernel, original["sources"][0])
    records = kernel.names.table("monitoring_records")
    first_marker = db.execute("SELECT " + _sql(kernel, desired_supersession_request_expression()), {
        "request_id": _id(432), "desired": _json({"supersession_request_id": _id(410)}),
    }).fetchone()[0]
    assert first_marker == _id(410)
    second_removal = {
        **staged["source_removals"][0], "removal_id": _id(440), "source_id": original["sources"][0]["source_id"],
        "target": original["sources"][0]["target"], "node_name": "source_" + original["sources"][0]["target"]["item_id"][:8],
        "binding_hash": _hash(_json(original["sources"][0])), "request_id": _id(430), "publication_id": _id(530),
        "requested_at": "2026-09-16T12:02:10Z",
    }
    connector = json.loads(params["connector"])
    for request_number, revision, operation, pending, superseded in (
        (430, 13, "controller.publish_connector", [second_removal], []),
        (431, 14, "worker.observe_connector", [second_removal], []),
        (432, 15, "controller.publish_connector", [], [second_removal]),
        (433, 16, "worker.observe_connector", [], []),
    ):
        value = {**connector, "revision": revision, "source_removals": pending}
        _save_receipt(db, kernel, operation, {**params, "request_id": _id(request_number)}, {
            "connector_id": _id(3), "connector": value, "observation": value,
            "superseded_source_removals": superseded,
        })
    connector["revision"] = 16
    params["connector"] = _json(connector)
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='connector_desired'", (_json({
        "supersession_request_id": first_marker, "published_at": "2026-09-16T12:02:30Z",
    }),))
    selected = original["sources"][source_index]
    position["observation"]["execution"]["target"] = selected["target"]
    position["transport"].update(source_id=selected["source_id"], identity_verified_at="2026-09-16T12:02:32Z")
    position["position"]["enqueued_at"] = "2026-09-16T12:02:33Z"
    position["received_at"] = "2026-09-16T12:02:34Z"
    db.execute("UPDATE incoming_positions SET payload=?", (_json(position),))
    db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.event_status','unknown') WHERE record_kind='target_capability'")
    predicate = restored_intake_invalid_sql(kernel.names).replace("@positions", "incoming_positions")
    assert _invalid(db, kernel, predicate, params)
    db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.event_status','verified','$.checked_at','2026-09-16T12:02:31Z') "
               "WHERE record_kind='target_capability' AND JSON_VALUE(payload,'$.target.item_id')=?", (selected["target"]["item_id"],))
    assert not _invalid(db, kernel, predicate, params)


def test_recovery_audit_does_not_bind_a_later_different_physical_source_to_the_old_id(db, restoration):
    kernel, original, staged, params = restoration
    _, position = _restored_intake(db, kernel, original, staged, params)
    connector = json.loads(params["connector"])
    old_id = connector["sources"][1]["source_id"]
    connector["sources"][1]["source_id"] = _id(999)
    connector["revision"] = 13
    _save_receipt(db, kernel, "controller.publish_connector", {**params, "request_id": _id(420)}, {
        "connector_id": _id(3), "connector": connector, "retired_sources": [{"source_id": old_id}],
        "superseded_source_removals": [],
    })
    params["connector"] = _json(connector)
    position["transport"]["source_id"] = _id(999)
    db.execute("UPDATE incoming_positions SET payload=?", (_json(position),))
    assert not _invalid(db, kernel, restored_intake_invalid_sql(kernel.names).replace("@positions", "incoming_positions"), params)


@pytest.fixture
def publication(db, restoration):
    kernel, original, staged, params = restoration
    _observation_context(params)
    original_result = json.loads(params["binding_receipt_payload"])["result"]
    inspection = _inspection(original["desired_definition"], observed_at=params["supersession_observed_at"])
    patch = json.loads(params["supersession_patch"])
    patch["inspection"] = inspection
    binding = json.loads(params["supersession_request_binding"])
    binding["observation_json"] = _json(patch)
    request = json.loads(params["supersession_request"])
    request["request_payload"] = binding
    original_result.update(inspection=inspection, collection_completion_eligible=True)
    _save_receipt(db, kernel, "worker.observe_connector", {
        **params, "request_id": params["binding_receipt_id"], "binding_hash": _hash(_json(binding)),
        "now": params["supersession_observed_at"],
    }, original_result)
    _insert_record(db, kernel, "worker_reconcile_request", params["binding_receipt_id"], request)
    _insert_record(db, kernel, "connector_publication", _id(7), {
        "connector_id": _id(3), "ownership_id": _id(4), "policy_revision": 3,
        "expected_connector_revision": 8, "source_removals": [_intent(original["sources"][1])],
    })
    _terminal_collection(db, kernel, params)
    _scope(db, kernel, original["sources"][1]["target"])
    for index, source in enumerate(original["sources"]):
        _read_admission(db, kernel, source, event_status="verified" if index == 0 else "unknown")
    records = kernel.names.table("monitoring_records")
    db.execute(f"UPDATE {records} SET revision=10,status='degraded',payload=? WHERE record_kind='connector'", (params["prior"],))
    queued = {
        "tenant_id": _id(1), "epoch": _id(2), "work_id": _id(450), "kind": "connector_reconcile",
        "connector_id": _id(3), "revision": 1, "attempts": 0, "retry_attempt": 0, "state": "queued", "lease": None,
    }
    _insert_record(db, kernel, "work", queued["work_id"], queued)
    db.execute(f"UPDATE {records} SET work_kind='connector_reconcile' WHERE record_kind='work'")
    desired = json.loads(params["desired"])
    desired["publication_id"] = _id(7)
    _insert_record(db, kernel, "connector_desired", _id(3), desired)
    plan = {
        **json.loads(params["plan"]), "source_removal_supersessions": [{
            "removal_id": staged["source_removals"][0]["removal_id"], "source_id": original["sources"][1]["source_id"],
        }], "source_removals": [],
    }
    params.update(
        publication_id=_id(451), plan=_json(plan), sources=_json(original["sources"]), proposals="[]",
        stored_work=_json({"reconcile_producer": "worker", "reconcile_request_id": _id(212)}),
        readiness_id=None, desired=_json(desired), binding_receipt_id=_id(212),
        removal_intents="[]", supersession_intents=_json(plan["source_removal_supersessions"]), superseded_json="[]",
    )
    params["work_key"] = f"work:v1:{_id(2)}:{_id(1)}:{params['work_id']}"
    db.execute(f"UPDATE {kernel.names.table('monitoring_leases')} SET full_key=?,key_hash=? WHERE owner_id=?", (
        params["work_key"], hashlib.sha256(params["work_key"].encode()).digest(), params["owner_id"],
    ))
    db.execute("CREATE TEMP TABLE supersession_queued_work (work_id TEXT,revision INTEGER)")
    db.commit()
    return kernel, original, staged, params


def _all_state(db, kernel):
    return tuple(db.execute(f"SELECT * FROM {kernel.names.table(kind)} ORDER BY 1,2,3,4").fetchall()
                 for kind in ("monitoring_records", "monitoring_receipts", "monitoring_leases"))


def _require_guard(db, kernel, body, message, params):
    if _invalid(db, kernel, _guard(body, message), params):
        raise ValueError(message)


def _publish(db, kernel, supplied):
    """Execute production guards/mutations; JSON serialization and orchestration are adapters."""
    params = dict(supplied)
    records = kernel.names.table("monitoring_records")
    with db:
        db.execute("BEGIN")
        replay = _replay(db, kernel, params)
        if replay is not None:
            return replay
        control = db.execute("SELECT tenant_id,epoch,revision FROM dbo.control").fetchone()
        if control != (params["tenant_id"], params["epoch"], params["expected_revision"]):
            raise ValueError("current control context")
        lease_query = current_work(kernel.names, ("reconcile_state",)).split("IF NOT EXISTS (", 1)[1].split(") THROW 51074", 1)[0]
        if not db.execute(_sql(kernel, lease_query), params).fetchone():
            raise ValueError("current reconciliation work fence")
        selection = prepare_supersession_selection_sql(kernel.names)
        for message in (
            "Source removal supersessions require bounded exact physical removal selectors",
            "Superseded removal and physical source identities must be unique",
            "Supersession requires an original current observation and unchanged retained ownership",
            "A physical removal cannot remain requested and be superseded together",
        ):
            _require_guard(db, kernel, selection, message, params)
        db.execute("DELETE FROM supersessions")
        select = re.search(r"INSERT INTO @supersessions\s+(SELECT .*?);", selection, re.S)[1]
        db.execute("INSERT INTO supersessions " + _sql(kernel, select), params)
        _require_guard(db, kernel, selection, "Supersession must select exact current owned pending physical removals", params)
        original = db.execute(_sql(kernel, supersession_observation_receipt_sql(kernel.names)), params).fetchone()
        if original is None:
            raise ValueError("original observation receipt missing")
        params.update(binding_receipt_payload=original[0], binding_receipt_fingerprint=original[1],
                      supersession_observed_at=original[2])
        request = db.execute(f"SELECT payload FROM {records} WHERE record_kind='worker_reconcile_request' "
                             "AND full_key=?", (params["binding_receipt_id"],)).fetchone()
        if request is None:
            raise ValueError("original observation handoff missing")
        request = json.loads(request[0])
        params.update(
            supersession_request=_json(request), supersession_request_binding=_json(request["request_payload"]),
            supersession_patch=request["request_payload"]["observation_json"],
            binding_observation=_json(json.loads(original[0])["result"]["observation"]),
            supersession_inspection=_json(json.loads(original[0])["result"].get("inspection")),
        )
        body = validate_supersession_sql(kernel.names, complete_component_map_sql())
        for message in (
            "Supersession requires the exact fresh original observation input and protected global handoff",
            "Supersession needs fresh complete original read-only running inspection evidence",
            "Supersession requires the complete original physical component map",
            "Supersession lost the immutable original removal publication and binding",
            "Supersession history is missing or ambiguous; no unexecuted removal is proved",
            "A retained possible definition-write intent forbids source removal supersession",
            "Supersession observation does not prove the exact retained physical source and transport",
            "Supersession may restore only the selected original physical nodes",
        ):
            _require_guard(db, kernel, body, message, params)
        result = json.loads(original[0])["result"]
        work_row = db.execute(f"SELECT payload,revision FROM {records} WHERE record_kind='work' AND full_key=?",
                              (result["work_id"],)).fetchone()
        params.update(supersession_work_id=result["work_id"], supersession_work=work_row[0] if work_row else None,
                      supersession_work_revision=work_row[1] if work_row else None)
        for message in (
            "Supersession requires the original inspection collection to be terminal under its exact fence",
            "Another connector worker is active or has an unreconciled attempted effect",
        ):
            _require_guard(db, kernel, body, message, params)
        queued = re.search(r"INSERT INTO @supersession_queued_work (SELECT .*?);", body, re.S)[1]
        db.execute("DELETE FROM supersession_queued_work")
        db.execute("INSERT INTO supersession_queued_work " + _sql(kernel, queued), params)
        originals = [json.loads(row[0]) for row in db.execute("SELECT payload FROM supersessions ORDER BY removal_id")]
        params["superseded_json"] = _json(originals)
        _require_guard(db, kernel, prepare_removals_sql(kernel.names),
                       "Pending removal cannot be cancelled by omitting its ownership record", params)
        for source in json.loads(params["sources"]):
            target = source["target"]
            target_key = ":".join(("monitor:v1", target["epoch"], target["tenant_id"], target["workload"],
                                   target["workspace_id"], target["item_id"]))
            target_row = db.execute(f"SELECT payload FROM {records} WHERE record_kind='target' AND full_key=?", (target_key,)).fetchone()
            capability = db.execute(f"SELECT payload FROM {records} WHERE record_kind='target_capability' AND full_key=?", (target_key,)).fetchone()
            params.update(source_target=_json(target), approved_target=target_row[0] if target_row else None,
                          source_capability=capability[0] if capability else None)
            restored = any(removal["source_id"] == source["source_id"] for removal in originals)
            predicate = restored_source_authorized_predicate(kernel.names) if restored else source_authorized_predicate()
            if not _invalid(db, kernel, predicate, params):
                raise ValueError("current reviewed scope/read or ordinary event capability")
        candidate = json.loads(db.execute("SELECT " + desired_update_expression(), params).fetchone()[0])
        candidate.update(source_removals=[], revision=params["expected_connector_revision"] + 1, updated_at=params["now"])
        candidate = json.loads(db.execute("SELECT " + invalidate_readiness_expression(), {"next": _json(candidate)}).fetchone()[0])
        mutation = supersession_disposition_work_sql(kernel.names)
        update = re.search(r"UPDATE work_record SET (.*?)\n    FROM .*?\n    JOIN .*?\n    WHERE (.*?);", mutation, re.S)
        assignment = update[1].replace("CONVERT(nvarchar(40),@now,127)+N'Z'", "@now")
        assignment = _sql(kernel, assignment).replace("work_record.revision||1", "work_record.revision+1")
        affected = db.execute(f"UPDATE {records} AS work_record SET {assignment} "
                              "FROM supersession_queued_work AS queued WHERE queued.work_id=work_record.full_key "
                              "AND queued.revision=work_record.revision AND " + _sql(kernel, update[2]), params).rowcount
        if affected != db.execute("SELECT COUNT(*) FROM supersession_queued_work").fetchone()[0]:
            raise ValueError("queued work compare-and-set")
        _apply_connector(db, kernel, candidate, params)
        anchor = db.execute(
            "SELECT " + _sql(kernel, desired_supersession_request_expression()), params,
        ).fetchone()[0]
        desired = {**json.loads(params["desired"]), "publication_id": params["publication_id"],
                   "published_at": params["now"], "supersession_request_id": anchor,
                   "definition_hash": _hash(params["definition"])}
        db.execute(f"UPDATE {records} SET revision=revision+1,payload=? WHERE record_kind='connector_desired'", (_json(desired),))
        result = {
            "connector_id": params["connector_id"], "connector": candidate, "state": candidate["state"],
            "desired_changed": True, "pending_removals": [], "retired_sources": [],
            "observation_receipt_id": params["binding_receipt_id"], "superseded_source_removals": originals,
        }
        _save_receipt(db, kernel, "controller.publish_connector", params, result)
        return result


@pytest.mark.parametrize("admission_basis", ["reviewed", "auto_detection_only"])
def test_valid_sql_restoration_keeps_physical_identity_and_all_original_receipts(db, publication, admission_basis):
    kernel, original, staged, params = publication
    receipts = kernel.names.table("monitoring_receipts")
    records = kernel.names.table("monitoring_records")
    target_key, target_json = db.execute(f"SELECT full_key,payload FROM {records} WHERE record_kind='target' "
                                        "AND JSON_VALUE(payload,'$.identity.item_id')=?",
                                        (original["sources"][1]["target"]["item_id"],)).fetchone()
    target = json.loads(target_json)
    target.update(admission_basis=admission_basis, action={"enabled": False})
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='target' AND full_key=?", (_json(target), target_key))
    scope = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='scope'").fetchone()[0])
    scope["rules"][0]["auto_enrol_detection_only"] = True
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='scope'", (_json(scope),))
    db.commit()
    protected = db.execute(f"SELECT * FROM {records} WHERE record_kind NOT IN ('connector','connector_desired','work') "
                           "ORDER BY record_kind,full_key").fetchall()
    leases = db.execute(f"SELECT * FROM {kernel.names.table('monitoring_leases')} ORDER BY full_key").fetchall()
    originals = db.execute(f"SELECT operation,request_id,payload FROM {receipts} ORDER BY operation,request_id").fetchall()
    result = _publish(db, kernel, params)
    assert result["connector"]["sources"] == original["sources"]
    assert result["connector"]["source_removals"] == [] and result["retired_sources"] == []
    assert result["superseded_source_removals"] == staged["source_removals"]
    assert result["state"] == "provisioning" and result["desired_changed"] is True
    for field in ("identity_verified_at", "delivery_verified_at", "delivery_proof"):
        assert result["connector"].get(field) is None
    for field in ("connector_id", "ownership_id", "workspace_id", "eventstream_id", "destination_id", "endpoint"):
        assert result["connector"][field] == original[field]
    assert db.execute(f"SELECT operation,request_id,payload FROM {receipts} "
                      "WHERE NOT(operation='controller.publish_connector' AND request_id=?) "
                      "ORDER BY operation,request_id", (params["request_id"],)).fetchall() == originals
    assert db.execute(f"SELECT * FROM {records} WHERE record_kind NOT IN ('connector','connector_desired','work') "
                      "ORDER BY record_kind,full_key").fetchall() == protected
    assert db.execute(f"SELECT * FROM {kernel.names.table('monitoring_leases')} ORDER BY full_key").fetchall() == leases
    queued = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='work' AND full_key=?", (_id(450),)).fetchone()[0])
    assert queued["state"] == "dispositioned" and queued["revision"] == 2 and queued["attempts"] == 0
    assert queued["disposition"] == "Unclaimed connector work superseded by controller.publish_connector request " + params["request_id"]
    claim = next(obj.ddl for obj in kernel.objects if obj.logical_name == "worker.claim_work")
    terminal_guard = re.search(r"IF (@stored_work_status NOT IN \([^)]+\))\nBEGIN\n    SET @status='not_acquired'", claim)[1]
    assert _invalid(db, kernel, terminal_guard, {"stored_work_status": queued["state"]})
    old = _replay(db, kernel, _mutation_params(original, _id(41)))
    assert old["pending_removals"] == staged["source_removals"]
    db.execute("UPDATE dbo.control SET revision=4")
    db.execute(f"UPDATE {kernel.names.table('monitoring_leases')} SET fence=99")
    db.commit()
    assert _publish(db, kernel, params) == result
    with pytest.raises(ValueError, match="original request binding"):
        _publish(db, kernel, {**params, "binding_hash": "changed"})


def test_supersession_receipt_failure_rolls_back_work_fencing_and_manifest_together(db, publication):
    kernel, _, _, params = publication
    receipts = kernel.names.table("monitoring_receipts")
    before = _all_state(db, kernel)
    db.execute(f"""CREATE TRIGGER dbo.fail_supersession BEFORE INSERT ON {receipts.split('.')[-1]}
        WHEN NEW.request_id='{params["request_id"]}' AND NEW.operation='controller.publish_connector'
        BEGIN SELECT RAISE(ABORT,'injected supersession receipt failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="injected supersession"):
        _publish(db, kernel, params)
    assert _all_state(db, kernel) == before
    db.execute("DROP TRIGGER dbo.fail_supersession")
    assert _publish(db, kernel, params)["superseded_source_removals"]


@pytest.mark.parametrize("fault", [
    "missing_observation", "stale_observation", "wrong_source", "wrong_binding", "wrong_frontier",
    "wrong_policy", "wrong_work_fence", "wrong_collection_fence", "active_worker", "expired_attempt",
    "uncertain_operation", "already_removed", "actual_exclusion", "event_denied", "event_blocked",
])
def test_denied_supersession_changes_no_durable_state(db, publication, fault):
    kernel, _, _, params = publication
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    if fault == "missing_observation":
        db.execute(f"DELETE FROM {receipts} WHERE operation='worker.observe_connector'")
    elif fault == "stale_observation":
        db.execute(f"UPDATE {receipts} SET recorded_at='2026-09-16T11:00:00Z' WHERE operation='worker.observe_connector'")
    elif fault in {"wrong_source", "wrong_frontier"}:
        plan = json.loads(params["plan"])
        if fault == "wrong_source":
            plan["source_removal_supersessions"][0]["source_id"] = _id(999)
            params["supersession_intents"] = _json(plan["source_removal_supersessions"])
        else:
            plan["frontier_revision"] += 1
        params["plan"] = _json(plan)
    elif fault == "wrong_policy":
        db.execute("UPDATE dbo.control SET revision=4")
    elif fault == "wrong_work_fence":
        params["fence"] += 1
    elif fault == "wrong_collection_fence":
        db.execute(f"UPDATE {kernel.names.table('monitoring_leases')} SET fence=fence+1 WHERE owner_id=?", (_id(211),))
    elif fault in {"event_denied", "event_blocked"}:
        target_id = json.loads(params["sources"])[1]["target"]["item_id"]
        db.execute(f"UPDATE {records} SET payload=json_set(payload,'$.event_status',?) "
                   "WHERE record_kind='target_capability' AND JSON_VALUE(payload,'$.target.item_id')=?",
                   (fault.removeprefix("event_"), target_id))
    elif fault in {"active_worker", "expired_attempt"}:
        queued = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='work' AND full_key=?", (_id(450),)).fetchone()[0])
        queued.update(state="leased" if fault == "active_worker" else "waiting", attempts=1)
        db.execute(f"UPDATE {records} SET payload=?,status=? WHERE record_kind='work' AND full_key=?",
                   (_json(queued), queued["state"], _id(450)))
        if fault == "active_worker":
            key = f"work:v1:{_id(2)}:{_id(1)}:{_id(450)}"
            db.execute(f"INSERT INTO {kernel.names.table('monitoring_leases')} VALUES (?,?,?,?,?,?,?)", (
                _id(1), _id(2), key, hashlib.sha256(key.encode()).digest(), _id(452), 1, "2026-09-16T13:00:00Z",
            ))
    elif fault == "actual_exclusion":
        scope = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='scope'").fetchone()[0])
        scope["rules"][0]["effect"] = "exclude"
        db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='scope'", (_json(scope),))
    else:
        prior = json.loads(params["prior"])
        if fault == "wrong_binding":
            prior["source_removals"][0]["binding_hash"] = "F" * 64
        elif fault == "uncertain_operation":
            prior["operation_id"] = _id(999)
        else:
            prior["sources"] = prior["sources"][:1]
            prior["source_removals"] = []
            params["sources"] = _json(prior["sources"])
        params["prior"] = _json(prior)
        db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='connector'", (params["prior"],))
    db.commit()
    before = _all_state(db, kernel)
    with pytest.raises(ValueError):
        _publish(db, kernel, params)
    assert _all_state(db, kernel) == before


@pytest.mark.parametrize("fault", [
    "unowned_source", "changed_other_target", "changed_other_events", "changed_other_id",
    "missing_old_node", "changed_old_property",
])
def test_original_hash_bound_but_drifted_snapshot_cannot_publish_supersession(db, publication, fault):
    kernel, _, _, params = publication
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    original = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE operation='worker.observe_connector' "
                                     "AND request_id=?", (params["binding_receipt_id"],)).fetchone()[0])
    handoff = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='worker_reconcile_request' "
                                    "AND full_key=?", (params["binding_receipt_id"],)).fetchone()[0])
    binding = handoff["request_payload"]
    patch = json.loads(binding["observation_json"])
    _observed_topology_fault(patch["observed_definition"], fault)
    patch["inspection"] = _inspection(patch["observed_definition"], observed_at="2026-09-16T12:01:00Z")
    binding["observation_json"] = _json(patch)
    original["binding_hash"] = _hash(_json(binding))
    original["result"]["observed_definition_hash"] = patch["inspection"]["definition_hash"]
    original["result"]["inspection"] = patch["inspection"]
    for field in ("connector", "observation"):
        original["result"][field]["observed_definition"] = patch["observed_definition"]
    prior = json.loads(params["prior"])
    prior["observed_definition"] = patch["observed_definition"]
    params["prior"] = _json(prior)
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='connector'", (params["prior"],))
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='worker_reconcile_request'", (_json(handoff),))
    db.execute(f"UPDATE {receipts} SET payload=? WHERE operation='worker.observe_connector' AND request_id=?",
               (_json(original), params["binding_receipt_id"]))
    db.commit()
    before = _all_state(db, kernel)
    with pytest.raises(ValueError, match="exact retained physical source and transport"):
        _publish(db, kernel, params)
    assert _all_state(db, kernel) == before


@pytest.mark.parametrize("expires_at", ["2026-09-16T11:59:00Z", "2026-09-16T12:02:00Z"])
def test_attempts_zero_does_not_override_an_existing_connector_lease_tombstone(db, publication, expires_at):
    kernel, _, _, params = publication
    key = f"work:v1:{_id(2)}:{_id(1)}:{_id(450)}"
    db.execute(f"INSERT INTO {kernel.names.table('monitoring_leases')} VALUES (?,?,?,?,?,?,?)", (
        _id(1), _id(2), key, hashlib.sha256(key.encode()).digest(), _id(452), 1, expires_at,
    ))
    db.commit()
    before = _all_state(db, kernel)
    with pytest.raises(ValueError, match="Another connector worker"):
        _publish(db, kernel, params)
    assert _all_state(db, kernel) == before


@pytest.mark.parametrize("field,value", [
    ("retry_attempt", 1), ("retry_attempt", None), ("lease", "malformed lease descriptor"),
    ("execution", {}), ("action_reservation_id", _id(460)), ("retry_of", _id(461)), ("finalization_id", _id(462)),
])
def test_never_claimed_queue_fencing_requires_absent_lease_and_effect_lineage(db, publication, field, value):
    kernel, _, _, params = publication
    records = kernel.names.table("monitoring_records")
    queued = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='work' AND full_key=?", (_id(450),)).fetchone()[0])
    queued[field] = value
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='work' AND full_key=?", (_json(queued), _id(450)))
    db.commit()
    before = _all_state(db, kernel)
    with pytest.raises(ValueError, match="Another connector worker"):
        _publish(db, kernel, params)
    assert _all_state(db, kernel) == before


@pytest.mark.parametrize("prune_submission,clock_regression", [(False, False), (True, False), (False, True)])
def test_lost_ack_then_clean_running_inspection_cannot_cancel_an_async_removal(db, publication, prune_submission, clock_regression):
    kernel, _, staged, params = publication
    records, receipts = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_receipts")
    for revision, gaps in (
        (10, [{"code": "definition_update_submitted_or_unknown", "detail": "Original write-ahead intent; ACK lost"}]),
        (11, [{"code": "awaiting_source_delivery_proof", "detail": "Later metadata cannot retract the earlier effect"}]),
    ):
        observed = {**staged, "revision": revision, "gaps": gaps, "state": "degraded", "operation_id": None}
        _save_receipt(db, kernel, "worker.observe_connector", {
            **params, "request_id": _id(500 + revision),
            "now": "2026-09-16T11:59:00Z" if clock_regression and revision == 10 else f"2026-09-16T12:00:{revision}Z",
        }, {"connector_id": _id(3), "connector": observed, "observation": observed})
    original = json.loads(db.execute(f"SELECT payload FROM {receipts} WHERE request_id=?", (_id(212),)).fetchone()[0])
    request = json.loads(db.execute(f"SELECT payload FROM {records} WHERE record_kind='worker_reconcile_request'").fetchone()[0])
    original["result"]["connector"]["revision"] = original["result"]["observation"]["revision"] = 12
    request["request_payload"]["expected_connector_revision"] = 11
    original["binding_hash"] = _hash(_json(request["request_payload"]))
    db.execute(f"UPDATE {receipts} SET payload=? WHERE request_id=?", (_json(original), _id(212)))
    db.execute(f"UPDATE {records} SET payload=? WHERE record_kind='worker_reconcile_request'", (_json(request),))
    prior = json.loads(params["prior"])
    prior["revision"] = 12
    params.update(prior=_json(prior), expected_connector_revision=12)
    db.execute(f"UPDATE {records} SET payload=?,revision=12 WHERE record_kind='connector'", (params["prior"],))
    if prune_submission:
        db.execute(f"DELETE FROM {receipts} WHERE request_id=?", (_id(510),))
    db.commit()
    before = _all_state(db, kernel)
    message = "history is missing" if prune_submission else "possible definition-write intent"
    with pytest.raises(ValueError, match=message):
        _publish(db, kernel, params)
    assert _all_state(db, kernel) == before


def test_sql_supersession_reuses_existing_transaction_routes_and_keeps_inspection_out_of_manifest():
    from triage.monitoring.sql_permissions import build_permission_kernel, integration_contract

    kernel = build_permission_kernel()
    publication = next(obj.ddl for obj in kernel.objects if obj.logical_name == "controller.publish_connector")
    observation = next(obj.ddl for obj in kernel.objects if obj.logical_name == "worker.observe_connector")
    claim = next(obj.ddl for obj in kernel.objects if obj.logical_name == "worker.claim_work")
    assert (len(kernel.rpcs), len(kernel.objects), sum(map(len, kernel.grants.values()))) == (27, 50, 56)
    assert publication.index("IF @prior_payload IS NOT NULL") < publication.index("DECLARE @supersession_intents")
    assert publication.index("Supersession history is missing") < publication.index("SET @superseded_json=N'['")
    assert publication.index("SET @superseded_json=N'['") < publication.index("Pending removal cannot be cancelled")
    assert publication.index(supersession_disposition_work_sql(kernel.names)) < publication.index("SET @affected=1")
    for ddl in (publication, claim):
        assert ddl.index(kernel.names.table("monitoring_control") + " WITH (UPDLOCK, HOLDLOCK)") < ddl.index("DECLARE @work_key")
    assert "WHERE [key]<>'inspection'" in observation
    assert "JSON_QUERY(@inspection) AS inspection" in observation
    assert "JSON_QUERY(@superseded_json) AS superseded_source_removals" in publication
    assert "A superseded removal identity cannot be reused" in publication
    # Optional evidence cannot make an earlier immutable receipt unreadable on replay.
    assert "inspection" not in kernel.rpcs["worker.observe_connector"].result_fields
    assert "superseded_source_removals" not in kernel.rpcs["controller.publish_connector"].result_fields
    assert integration_contract()["connector_supersession_contract"]["never_submitted"]
