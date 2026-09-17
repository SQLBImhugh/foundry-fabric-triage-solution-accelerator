"""Unbound proposal and consumer-group regressions against emitted SQL.

SQLite adapts scalar syntax and JSON serialization only; native procedure,
permission and concurrent lease execution remain separate acceptance work.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3

import pytest
from test_monitoring_sql_removals import (
    _adapt,
    _apply_connector,
    _confirm,
    _evaluate,
    _id,
    _json,
    _manifest,
    _mutation_params,
    _observe,
    _replay,
    _save_receipt,
    _sql,
    _stage,
)
from test_monitoring_sql_removals import case as case
from test_monitoring_sql_removals import db as db

from triage.monitoring.sql_kernel_common import key_hash, partition_identity
from triage.monitoring.sql_kernel_connectors import desired_update_expression


def _condition(body, message):
    prefix = body[:body.index(f"'{message}', 1;")]
    prefix = prefix[:prefix.rindex("THROW ")]
    match = list(re.finditer(r"(?m)^\s*IF ", prefix))[-1]
    return prefix[match.end():].strip()


def _proposal_guard(kernel):
    return re.search(
        r"(?ms)^IF (@binding_receipt_id IS NULL AND EXISTS .*?)\n    THROW 51072,",
        _sql(kernel, "controller.publish_connector"),
    )[1]


def _group_guard(kernel):
    guard = _condition(partition_identity(kernel.names), "Partition does not belong to the owned connector")
    # Before the fix this operand inherits the database CI/padded collation.
    inherited = "COALESCE(JSON_VALUE(@connector,'$.endpoint.consumer_group'),'')"
    return guard.replace(inherited, f"({inherited} COLLATE SQL_CI)")


def _proposal(original):
    return {
        "proposal_id": _id(50), "source_id": None, "node_name": "owned_proposal_c",
        "target": {**original["sources"][0]["target"], "item_id": _id(113)},
        "event_types": original["sources"][0]["event_types"], "event_source": "synthetic-source-c",
    }


def _proposal_params(manifest, definition=None, *, receipt=None):
    return {
        "prior": _json(manifest), "proposals": _json(manifest["source_proposals"]),
        "definition": _json(definition or manifest["desired_definition"]),
        "binding_receipt_id": receipt,
    }


def test_existing_unresolved_proposal_cannot_acquire_a_fabricated_id(db, case):
    kernel, original = case
    staged = _stage(db, kernel, proposals=[_proposal(original)], source_removals=[])
    forged = copy.deepcopy(staged["desired_definition"])
    forged["component_ids"]["sources/owned_proposal_c"] = _id(99)
    assert staged["source_proposals"][0]["source_id"] is None
    assert _evaluate(db, kernel, _proposal_guard(kernel), _proposal_params(staged, forged))
    assert _manifest(db, kernel)["desired_definition"] == staged["desired_definition"]


def test_configured_group_case_is_not_a_partition_identity_alias(db, case):
    kernel, original = case
    params = {"connector": _json(original), "consumer_group": "$default"}
    assert _evaluate(db, kernel, _group_guard(kernel), params)
    pairs = (_json([group, "0"]) for group in ("$Default", "$default"))
    assert len({hashlib.sha256(pair.encode("utf-8")).digest() for pair in pairs}) == 2


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("location", ["component_ids", "node_id"])
def test_every_unresolved_proposal_rejects_caller_physical_identity(db, case, existing, location):
    kernel, original = case
    proposal = _proposal(original)
    staged = _stage(db, kernel, proposals=[proposal], source_removals=[])
    prior = staged if existing else original
    definition = copy.deepcopy(staged["desired_definition"])
    if location == "component_ids":
        definition["component_ids"]["sources/" + proposal["node_name"]] = _id(99)
    else:
        definition["parts"]["eventstream.json"]["sources"][-1]["id"] = _id(99)
    params = {**_proposal_params(prior, definition), "proposals": _json([proposal])}
    assert _evaluate(db, kernel, _proposal_guard(kernel), params)
    # This bypasses only the no-receipt gate; the complete original receipt/CAS guards still follow it.
    assert not _evaluate(db, kernel, _proposal_guard(kernel), {**params, "binding_receipt_id": _id(42)})


def test_observed_existing_proposal_is_not_ownership_but_can_remain_pending_or_be_withdrawn(db, case):
    kernel, original = case
    proposal = _proposal(original)
    staged = _stage(db, kernel, proposals=[proposal], source_removals=[])
    observed = copy.deepcopy(staged["desired_definition"])
    observed["component_ids"]["sources/" + proposal["node_name"]] = _id(13)
    current = _observe(db, kernel, observed)
    assert current["source_proposals"][0]["source_id"] is None
    assert not _evaluate(db, kernel, _proposal_guard(kernel), _proposal_params(current))
    assert _evaluate(db, kernel, _proposal_guard(kernel), _proposal_params(current, observed))
    # A newly named proposal cannot adopt the same already-observed node.
    replaced = {**proposal, "proposal_id": _id(51)}
    assert _evaluate(db, kernel, _proposal_guard(kernel), {
        **_proposal_params(current), "proposals": _json([replaced]),
    })
    withdrawn = copy.deepcopy(current["desired_definition"])
    withdrawn["parts"]["eventstream.json"]["sources"].pop()
    withdrawn["parts"]["eventstream.json"]["streams"][0]["inputNodes"].pop()
    assert not _evaluate(db, kernel, _proposal_guard(kernel), _proposal_params(current, withdrawn))


def _republish(db, kernel, definition, request_id, *, expected_revision=None):
    prior = _manifest(db, kernel)
    params = _mutation_params(prior, request_id)
    if expected_revision is not None:
        params["expected_connector_revision"] = expected_revision
    params["binding_hash"] = hashlib.sha256(_json(definition).encode()).hexdigest()
    with db:
        db.execute("BEGIN")
        replay = _replay(db, kernel, params)
        if replay is not None:
            return replay
        if _evaluate(db, kernel, _proposal_guard(kernel), _proposal_params(prior, definition)):
            raise ValueError("unresolved proposal physical ID")
        candidate = json.loads(db.execute("SELECT " + desired_update_expression(), {
            **params, "prior": _json(prior), "sources": _json(prior["sources"]),
            "definition": _json(definition),
        }).fetchone()[0])
        candidate["revision"] = prior["revision"] + 1
        _apply_connector(db, kernel, candidate, params)
        result = {"connector": candidate}
        _save_receipt(db, kernel, "controller.publish_connector", params, result)
        return result


def test_rejected_forgery_cannot_poison_later_original_receipt_binding(db, case):
    kernel, original = case
    staged = _stage(db, kernel, proposals=[_proposal(original)], source_removals=[])
    forged = copy.deepcopy(staged["desired_definition"])
    forged["component_ids"]["sources/owned_proposal_c"] = _id(99)
    with pytest.raises(ValueError, match="unresolved proposal"):
        _republish(db, kernel, forged, _id(61))
    assert _manifest(db, kernel) == staged
    with pytest.raises(ValueError, match="connector revision/ownership CAS"):
        _republish(db, kernel, staged["desired_definition"], _id(62), expected_revision=staged["revision"] - 1)
    assert _manifest(db, kernel) == staged
    republished = _republish(db, kernel, staged["desired_definition"], _id(63))
    observed = copy.deepcopy(republished["connector"]["desired_definition"])
    observed["component_ids"]["sources/owned_proposal_c"] = _id(13)
    _observe(db, kernel, observed)
    result = _confirm(db, kernel)
    assert result["connector"]["source_proposals"] == []
    assert result["connector"]["sources"][-1]["source_id"] == _id(13)
    assert _id(99) not in _json(result)
    # The old pending publication replays verbatim, without overwriting the now-bound manifest.
    assert _republish(db, kernel, staged["desired_definition"], _id(63)) == republished
    assert _manifest(db, kernel) == result["connector"]
    receipts = kernel.names.table("monitoring_receipts")
    assert db.execute(f"SELECT COUNT(*) FROM {receipts} WHERE request_id IN (?,?)", (_id(61), _id(62))).fetchone()[0] == 0


def test_unbound_publication_receipt_failure_rolls_back_and_replay_never_introduces_ids(db, case):
    kernel, original = case
    staged = _stage(db, kernel, proposals=[_proposal(original)], source_removals=[])
    receipts = kernel.names.table("monitoring_receipts")
    db.execute(f"""CREATE TRIGGER dbo.fail_unbound_receipt BEFORE INSERT ON {receipts.split('.')[-1]}
        WHEN NEW.request_id='{_id(64)}' BEGIN SELECT RAISE(ABORT,'injected unbound receipt failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="injected unbound"):
        _republish(db, kernel, staged["desired_definition"], _id(64))
    assert _manifest(db, kernel) == staged
    db.execute("DROP TRIGGER dbo.fail_unbound_receipt")
    result = _republish(db, kernel, staged["desired_definition"], _id(64))
    assert _republish(db, kernel, staged["desired_definition"], _id(64)) == result
    assert result["connector"]["source_proposals"][0]["source_id"] is None
    forged = copy.deepcopy(staged["desired_definition"])
    forged["component_ids"]["sources/owned_proposal_c"] = _id(99)
    with pytest.raises(ValueError, match="original request binding"):
        _republish(db, kernel, forged, _id(64))
    assert _manifest(db, kernel) == result["connector"]


@pytest.mark.parametrize("configured,requested", [
    ("$Default", "$default"), ("$Default", "$DEFAULT"),
    ("$Default", "$Default "), ("$Default ", "$Default"),
    ("$Default", " $Default"), ("$Default", "$Default\t"),
])
def test_group_case_and_padding_mismatches_fail_before_hash_derivation(db, case, configured, requested):
    kernel, original = case
    manifest = {**original, "endpoint": {**original["endpoint"], "consumer_group": configured}}
    assert _evaluate(db, kernel, _group_guard(kernel), {
        "connector": _json(manifest), "consumer_group": requested,
    })
    body = partition_identity(kernel.names)
    assert body.index("Partition does not belong") < body.index("DECLARE @partition_digest")
    assert body.index("Partition does not belong") < body.index("DECLARE @partition_json")


def _partition_step(db, kernel, transition, *, group="$Default", expected_owner=None, expected_fence=None,
                    expected_revision=0, new_owner=None, minute=0):
    manifest = _manifest(db, kernel)
    body = _sql(kernel, "worker.partition")
    if _evaluate(db, kernel, _group_guard(kernel), {"connector": _json(manifest), "consumer_group": group}):
        raise ValueError("configured group identity")
    params = {
        "connector": _json(manifest), "tenant_id": _id(1), "epoch": _id(2), "connector_id": _id(3),
        "consumer_group": group, "partition_id": "0", "transition": transition,
        "expected_owner_id": expected_owner, "expected_fence": expected_fence,
        "expected_ownership_revision": expected_revision, "new_owner_id": new_owner,
        "now": f"2026-09-16T12:{minute:02}:00Z",
    }
    pair_sql = """N'["'+@consumer_group+N'","'+@partition_id+N'"]'"""
    assert key_hash(pair_sql) in body
    pair = db.execute("SELECT " + _adapt(kernel, pair_sql, concatenate=True), params).fetchone()[0]
    params["partition_digest"] = hashlib.sha256(pair.encode("utf-8")).hexdigest()
    key_sql = "N'partition:v1:'+@epoch+N':'+@tenant_id+N':'+@connector_id+N':'+@partition_digest"
    assert key_sql in body
    params["partition_key"] = db.execute("SELECT " + _adapt(kernel, key_sql, concatenate=True), params).fetchone()[0]
    records, leases = kernel.names.table("monitoring_records"), kernel.names.table("monitoring_leases")
    with db:
        db.execute("BEGIN")
        journal = db.execute(f"SELECT revision FROM {records} WHERE record_kind='partition_ownership' AND full_key=?",
                             (params["partition_key"],)).fetchone()
        lease = db.execute(f"SELECT owner_id,fence,expires_at,acquired_at FROM {leases} WHERE full_key=?",
                           (params["partition_key"],)).fetchone()
        params.update(journal_revision=journal[0] if journal else None,
                      prior_owner=lease[0] if lease else None, prior_fence=lease[1] if lease else None,
                      prior_expiry=lease[2] if lease else None, acquired=lease[3] if lease else None)
        for message in ("Partition ownership journal revision changed", "Partition owner/fence compare-and-set failed"):
            if _evaluate(db, kernel, _condition(body, message), params):
                raise ValueError(message)
        fence_expression = re.search(r"DECLARE @next_fence bigint=(.*?),\s*@expires", body, re.S)[1]
        params["next_fence"] = db.execute("SELECT " + _adapt(kernel, fence_expression), params).fetchone()[0]
        params["expires"] = params["now"] if transition == "release" else f"2026-09-16T12:{minute + 5:02}:00Z"
        if lease is None:
            mutation = re.search(rf"(INSERT INTO {re.escape(leases)} \(tenant_id,epoch,key_hash,full_key,owner_id,fence,acquired_at,expires_at\).*?;)", body, re.S)[1]
        else:
            mutation = re.search(rf"(UPDATE {re.escape(leases)} SET owner_id=COALESCE.*?;)", body, re.S)[1]
        assert db.execute(_adapt(kernel, mutation), params).rowcount == 1
        partition = {key: params[key] for key in ("tenant_id", "epoch", "connector_id", "consumer_group", "partition_id")}
        result = {
            "partition_key": params["partition_key"], "partition": partition,
            "lease": None if transition == "release" else {
                "resource_key": params["partition_key"], "owner_id": new_owner, "fence": params["next_fence"],
            },
            "last_owner_id": new_owner or params["prior_owner"], "last_fence": params["next_fence"],
            "ownership_revision": expected_revision + 1,
        }
        params["ownership_json"] = _json(result)
        if journal:
            mutation = re.search(rf"(UPDATE {re.escape(records)} SET revision=revision\+1,payload=@ownership_json.*?;)", body, re.S)[1]
        else:
            creation = body.split("IF @journal_revision IS NULL\n    BEGIN", 1)[1]
            mutation = re.search(rf"(INSERT INTO {re.escape(records)}.*?;)", creation, re.S)[1]
        assert db.execute(_adapt(kernel, mutation), params).rowcount == 1
        return result


def test_exact_group_claim_release_and_reclaim_keep_one_identity_and_tombstone_fence(db, case):
    kernel, _ = case
    leases = kernel.names.table("monitoring_leases")
    db.execute(f"ALTER TABLE {leases} ADD COLUMN acquired_at TEXT")
    db.commit()
    claimed = _partition_step(db, kernel, "claim", new_owner=_id(70))
    with pytest.raises(ValueError, match="configured group"):
        _partition_step(db, kernel, "claim", group="$default", new_owner=_id(71))
    released = _partition_step(
        db, kernel, "release", expected_owner=_id(70), expected_fence=1, expected_revision=1, minute=1,
    )
    assert released["lease"] is None and released["last_owner_id"] == _id(70) and released["last_fence"] == 2
    with pytest.raises(ValueError, match="compare-and-set"):
        _partition_step(
            db, kernel, "claim", expected_owner=_id(70), expected_fence=1, expected_revision=2,
            new_owner=_id(71), minute=2,
        )
    reclaimed = _partition_step(
        db, kernel, "claim", expected_owner=_id(70), expected_fence=2, expected_revision=2,
        new_owner=_id(71), minute=2,
    )
    assert reclaimed["partition_key"] == released["partition_key"] == claimed["partition_key"]
    assert reclaimed["partition"] == claimed["partition"] and reclaimed["partition"]["consumer_group"] == "$Default"
    assert reclaimed["lease"]["fence"] == 3 and reclaimed["ownership_revision"] == 3
    assert db.execute(f"SELECT COUNT(*) FROM {leases} WHERE full_key=?", (claimed["partition_key"],)).fetchone()[0] == 1
    for operation in ("worker.partition", "worker.commit_positions", "worker.advance_checkpoint", "worker.observe_retention"):
        ddl = _sql(kernel, operation)
        assert ddl.index("IF @prior_payload IS NOT NULL") < ddl.index("Partition does not belong")
        assert ddl.index("Partition does not belong") < ddl.index("DECLARE @partition_digest")
