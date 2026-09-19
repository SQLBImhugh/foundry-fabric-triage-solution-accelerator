from __future__ import annotations

import base64
import copy
import json
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import TypeAdapter

from scripts import register_monitoring_connector as cli
from scripts.monitoring_eventstream_canary import CanarySpec
from triage.monitoring import connector_bootstrap as bootstrap
from triage.monitoring import models as m
from triage.monitoring.deployment_contracts import (
    DeploymentConflict,
    DeploymentError,
    DeploymentUncertain,
    ResetTarget,
    canonical_json,
    fingerprint,
)
from triage.monitoring.provisioning import (
    SOURCE_EVENTS,
    decode_snapshot,
    encode_definition,
    plan_definition,
)
from triage.store.azure_sql import AzureSqlDatabase, SqlCommitUncertain, SqlUnavailable

TENANT, EPOCH, DEPLOYER, WORKSPACE, PIPELINE, EVENTSTREAM, CONNECTOR, OWNER, REQUEST = (
    str(UUID(int=value)) for value in range(1, 10)
)
NOW = datetime(2026, 9, 18, 8, 0, tzinfo=UTC)
TARGET = ResetTarget(
    server="bootstrap-fixture.database.windows.net", database="fixture",
    tenant_id=TENANT, deployer_object_id=DEPLOYER,
)


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Connector bootstrap tests must not access a real transport or database")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(AzureSqlDatabase, "_connect", forbidden)


def capture():
    spec = CanarySpec("create", OWNER, WORKSPACE, PIPELINE, 120)
    creation = spec.request_body()
    # The actual Fabric readback assigns physical IDs; these are not inferred
    # from names or from the requested logical definition.
    graph = json.loads(base64.b64decode(next(
        part["payload"] for part in creation["definition"]["parts"] if part["path"] == "eventstream.json"
    )))
    for index, kind in enumerate(("sources", "streams", "destinations"), 20):
        graph[kind][0]["id"] = str(UUID(int=index))
    topology = copy.deepcopy(graph)
    for kind in ("sources", "streams", "destinations"):
        topology[kind][0]["status"] = "Running"
    parts = {
        part["path"]: json.loads(base64.b64decode(part["payload"]))
        for part in creation["definition"]["parts"]
    }
    # The current getDefinition response can retain the original logical graph.
    definition = encode_definition({"parts": parts, "component_ids": {}})
    item = bootstrap.EventstreamItem(
        id=EVENTSTREAM, workspaceId=WORKSPACE, type="Eventstream",
        displayName=creation["displayName"], description=creation["description"],
    )
    submitted_hash = fingerprint(creation)
    receipt = bootstrap.CreationReceipt(
        intent_id=OWNER, workspace_id=WORKSPACE, eventstream_id=EVENTSTREAM,
        request_sha256=submitted_hash, completed_at=NOW - timedelta(days=1),
    )
    evidence = {
        "item": item.model_dump(mode="json"), "definition": definition,
        "topology": topology, "observed_at": NOW.isoformat(),
    }
    return bootstrap.ConnectorCapture(
        provenance="operator_reviewed_capture",
        review="original_creation_and_complete_current_readbacks_reviewed",
        request_id=REQUEST, expected=m.RegistryVersion(tenant_id=TENANT, epoch=EPOCH, revision=0),
        expected_maintenance=False, connector_id=CONNECTOR, expected_connector_revision=0,
        ownership_id=OWNER, name="Reviewed owned transport",
        endpoint=m.EndpointMetadata(
            namespace="fixture.servicebus.windows.net", entity="owned-events", consumer_group="$Default",
        ),
        sources=(m.ConnectorSource(
            source_id=str(UUID(int=20)),
            target=m.TargetIdentity(
                tenant_id=TENANT, epoch=EPOCH, workload="fabric_pipeline", workspace_id=WORKSPACE, item_id=PIPELINE,
            ),
            event_types=SOURCE_EVENTS, event_source=TENANT,
        ),),
        creation_request=creation, creation_request_sha256=submitted_hash,
        creation_receipt=receipt, creation_receipt_sha256=fingerprint(receipt.model_dump(mode="json")),
        item=item, definition_response=definition, topology_response=topology,
        readback_sha256=fingerprint(evidence), observed_at=NOW,
    )


class Engine:
    def __init__(self):
        self.control = m.DeploymentControl(
            tenant_id=TENANT, epoch=EPOCH, revision=0, maintenance=False,
            activation_cutoff=NOW - timedelta(days=2), updated_at=NOW - timedelta(days=1),
        )
        self.control_payload = canonical_json(self.control.model_dump(mode="json"))
        self.records, self.receipts = [], []
        self.effects = []
        self.effect_receipts = []
        self.reads, self.writes = [], []
        self.lock = threading.RLock()
        self.now = NOW
        self.control_authority = True
        self.bad_table = False
        self.table_type = "U "
        self.table_owner = 1
        self.table_triggers = 0
        self.foreign_key = False
        self.write_count = 1
        self.receipt_count = 1
        self.commit_fault = None
        self.unavailable = False
        self.database_id = 7
        self.delay_snapshot = False

    def set_control(self, **changes):
        self.control = m.DeploymentControl.model_validate(self.control.model_dump() | changes)
        self.control_payload = canonical_json(self.control.model_dump(mode="json"))

    def add(self, connector, *, promoted_change=None):
        row = (
            connector.tenant_id, connector.epoch, connector.connector_id, bootstrap._key(connector.connector_id),
            connector.revision, connector.state, canonical_json(connector.model_dump(mode="json")),
        )
        if promoted_change is not None:
            values = list(row)
            values[promoted_change] = "changed"
            row = tuple(values)
        self.records.append(row)


class SqlFake(AzureSqlDatabase):
    def __init__(self, engine):
        self.engine = engine
        self._server, self._database, self._tables = TARGET.server, TARGET.database, {}
        self._credential = SimpleNamespace(target=TARGET)
        self.active = False

    @contextmanager
    def transaction(self):
        with self.engine.lock:
            assert not self.active
            self.active = True
            before = copy.deepcopy((self.engine.records, self.engine.receipts))
            try:
                yield self
            except BaseException:
                self.engine.records, self.engine.receipts = before
                raise
            else:
                if before != (self.engine.records, self.engine.receipts) and self.engine.commit_fault:
                    fault, self.engine.commit_fault = self.engine.commit_fault, None
                    if fault == "rollback":
                        self.engine.records, self.engine.receipts = before
                    if fault == "unreadable":
                        self.engine.unavailable = True
                    raise SqlCommitUncertain("Fixture commit acknowledgement lost")
            finally:
                self.active = False

    def query(self, sql, *params):
        assert self.active
        engine = self.engine
        if engine.unavailable:
            raise SqlUnavailable("Fixture database unavailable")
        engine.reads.append((sql, params))
        if "connector-bootstrap:identity" in sql:
            return [(1, int(engine.control_authority), 1, TARGET.server, TARGET.database, engine.database_id, engine.now)]
        if "connector-bootstrap:lock-control" in sql:
            return [(1,)]
        if "connector-bootstrap:control" in sql:
            control = engine.control
            return [(
                control.tenant_id, control.epoch, control.revision, control.maintenance,
                control.schema_version, engine.control_payload,
            )]
        if "connector-bootstrap:table-authority" in sql:
            kind = engine.table_type.rstrip(" ") if "RTRIM(o.type)" in sql else engine.table_type
            return [
                ("dbo", name.removeprefix("dbo."), kind,
                 11 if engine.bad_table else engine.table_owner, engine.table_triggers)
                for name in sorted(params)
            ]
        if "connector-bootstrap:table-relationships" in sql:
            return [(1,)] if engine.foreign_key else []
        if "connector-bootstrap:connectors" in sql:
            return engine.records[:101]
        if "connector-bootstrap:effects" in sql:
            return engine.effects[:1]
        if "connector-bootstrap:effect-receipts" in sql:
            if engine.delay_snapshot:
                engine.now += timedelta(minutes=16)
            return engine.effect_receipts[:1]
        if "connector-bootstrap:receipt" in sql:
            return [
                (row["request_id"], row["fingerprint"], row["recorded_at"], row["payload"])
                for row in engine.receipts
                if (row["tenant_id"], row["epoch"], row["operation"], row["request_hash"]) == params
            ]
        raise AssertionError(f"Unsupported SQL fixture read: {sql[:70]}")

    def execute(self, sql, *params):
        assert self.active
        engine = self.engine
        engine.writes.append((sql, params))
        if "connector-bootstrap:insert-receipt" in sql:
            if engine.receipt_count == 1:
                row = dict(zip(
                    ("tenant_id", "epoch", "operation", "request_hash", "request_id", "fingerprint", "recorded_at", "payload"),
                    params, strict=True,
                ))
                assert not any(
                    (prior["tenant_id"], prior["epoch"], prior["operation"], prior["request_hash"])
                    == (row["tenant_id"], row["epoch"], row["operation"], row["request_hash"])
                    for prior in engine.receipts
                )
                engine.receipts.append(row)
            return engine.receipt_count
        if "connector-bootstrap:insert" in sql:
            tenant, epoch, key, digest, revision, payload, *_ = params
            assert digest == bootstrap._key(key)
            assert engine.now.replace(tzinfo=None) < params[-1]
            if engine.write_count == 1:
                assert not any(row[:2] == (tenant, epoch) and row[3] == digest for row in engine.records)
                engine.records.append((tenant, epoch, key, digest, revision, "planned", payload))
            return engine.write_count
        if "connector-bootstrap:assign" in sql:
            revision, payload, tenant, epoch, key, digest, old_revision, old_hash, expiry = params
            assert engine.now.replace(tzinfo=None) < expiry
            matches = [
                index for index, row in enumerate(engine.records)
                if row[:5] == (tenant, epoch, key, digest, old_revision)
                and bytes.fromhex(bootstrap._payload_hash(row[6])) == old_hash
            ]
            if engine.write_count == 1:
                assert len(matches) == 1
                engine.records[matches[0]] = (tenant, epoch, key, digest, revision, "planned", payload)
            return engine.write_count
        raise AssertionError(f"Unsupported SQL fixture write: {sql[:70]}")


def arranged():
    engine = Engine()
    operator = bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET)
    return engine, operator, operator.prepare(capture())


def test_native_char2_table_type_is_normalized_only_by_the_sql_projection():
    engine = Engine()
    database = SqlFake(engine)
    with database.transaction():
        raw = database.query("/* connector-bootstrap:table-authority */ SELECT o.type", "dbo.triage_monitoring_control")
    assert raw == [("dbo", "triage_monitoring_control", "U ", 1, 0)]
    plan = bootstrap.ConnectorBootstrapOperator(database, TARGET).prepare(capture())
    assert plan.capture.connector_id == CONNECTOR and not engine.writes
    query = next(sql for sql, _ in reversed(engine.reads) if "connector-bootstrap:table-authority" in sql)
    assert "SELECT s.name,o.name,RTRIM(o.type)," in query


@pytest.mark.parametrize("kind,owner,triggers", [("V ", 1, 0), ("U ", 2, 0), ("U ", 1, 1)])
def test_native_type_normalization_retains_view_owner_and_trigger_refusals(kind, owner, triggers):
    engine = Engine()
    engine.table_type, engine.table_owner, engine.table_triggers = kind, owner, triggers
    with pytest.raises(DeploymentError, match="dbo ownership and no enabled triggers"):
        bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET).prepare(capture())
    assert not engine.writes


def test_actual_canary_contract_prepares_complete_nonsecret_physical_metadata_only():
    engine, _, plan = arranged()
    connector = plan.connector
    assert not engine.writes
    assert connector.workspace_id == WORKSPACE and connector.eventstream_id == EVENTSTREAM
    assert connector.destination_id == str(UUID(int=22))
    assert connector.sources == plan.capture.sources
    assert connector.desired_definition == decode_snapshot(
        plan.capture.definition_response, plan.capture.topology_response,
    )
    assert connector.state == "planned" and connector.gaps[0].code == bootstrap.GATE
    assert all(getattr(connector, field) is None for field in (
        "identity_verified_at", "delivery_verified_at", "delivery_proof", "last_receiver_activity_at", "operation_id",
    ))
    # Missing admission is neither permission to receive nor authority to remove
    # retained physical ownership.
    proposal = plan_definition(connector, connector.desired_definition, ())
    assert proposal.source_removals == () and not proposal.new_names
    assert proposal.deferred_targets == tuple(source.target for source in connector.sources)
    assert proposal.desired == connector.desired_definition
    assert plan.expires_at == NOW + timedelta(minutes=15)


def test_apply_atomic_metadata_receipt_and_select_only_replay_after_policy_changes():
    engine, operator, plan = arranged()
    original_control = engine.control_payload
    result = operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert result.receipt.state == "registered_metadata_only"
    assert result.receipt.connector == plan.connector
    assert not result.receipt.identity_verified and not result.receipt.delivery_verified
    assert engine.control_payload == original_control
    assert len(engine.records) == len(engine.receipts) == 1
    assert engine.receipts[0]["operation"] == bootstrap.OPERATION
    assert all(
        all(word not in sql.upper() for word in ("CREATE ", "ALTER ", "DELETE ", "GRANT ", "EXEC "))
        for sql, _ in engine.writes
    )
    assert not any("connector_desired" in sql for sql, _ in engine.writes)
    saved = copy.deepcopy((engine.records, engine.receipts))
    writes = len(engine.writes)
    engine.now += timedelta(days=1)
    engine.set_control(revision=3, maintenance=True)
    other = bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET)
    assert other.reconcile(plan).receipt == result.receipt
    assert other.apply(plan, confirmed_manifest_hash=plan.manifest_hash).replayed
    assert len(engine.writes) == writes and (engine.records, engine.receipts) == saved


def test_independent_instances_do_not_duplicate_registration_or_original_receipt():
    engine, operator, plan = arranged()
    second = bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(current.apply, plan, confirmed_manifest_hash=plan.manifest_hash)
            for current in (operator, second)
        ]
        results = [future.result() for future in futures]
    assert sum(result.replayed for result in results) == 1
    assert len(engine.records) == len(engine.receipts) == 1
    assert len(engine.writes) == 2


@pytest.mark.parametrize("fault", ["committed", "rollback", "unreadable"])
def test_lost_ack_reconciles_only_the_original_immutable_receipt(fault):
    engine, operator, plan = arranged()
    engine.commit_fault = fault
    if fault == "committed":
        result = operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
        assert result.reconciled_uncertain_commit
    else:
        with pytest.raises(DeploymentUncertain):
            operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert len(engine.writes) == 2
    engine.unavailable = False
    if fault == "rollback":
        assert not engine.records and not engine.receipts
        with pytest.raises(DeploymentUncertain):
            operator.reconcile(plan)
    else:
        assert operator.reconcile(plan).replayed
    assert len(engine.writes) == 2


@pytest.mark.parametrize("changed", [
    "epoch", "tenant", "revision", "maintenance", "payload", "database", "expired", "future",
    "authority", "table_owner", "foreign_key", "pending_effect", "effect_receipt",
    "record_hash", "receipt_write", "record_write",
])
def test_current_fences_and_atomic_failures_never_leave_partial_registration(changed):
    engine, operator, plan = arranged()
    if changed in {"epoch", "tenant"}:
        engine.set_control(**{changed if changed == "epoch" else "tenant_id": str(UUID(int=990))})
    elif changed == "revision":
        engine.set_control(revision=1)
    elif changed == "maintenance":
        engine.set_control(maintenance=True)
    elif changed == "payload":
        engine.control_payload += " "
    elif changed == "database":
        engine.database_id = 8
    elif changed == "expired":
        engine.now = plan.expires_at
    elif changed == "future":
        engine.now -= timedelta(seconds=1)
    elif changed == "authority":
        engine.control_authority = False
    elif changed == "table_owner":
        engine.bad_table = True
    elif changed == "foreign_key":
        engine.foreign_key = True
    elif changed == "pending_effect":
        engine.effects = [("connector_desired",)]
    elif changed == "effect_receipt":
        engine.effect_receipts = [("worker.observe_connector",)]
    elif changed == "record_hash":
        engine.add(plan.connector, promoted_change=3)
    elif changed == "receipt_write":
        engine.receipt_count = 0
    else:
        engine.write_count = 0
    before = copy.deepcopy((engine.records, engine.receipts))
    with pytest.raises(DeploymentError):
        operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert (engine.records, engine.receipts) == before


@pytest.mark.parametrize("count", [-1, 0, 2])
@pytest.mark.parametrize("phase", ["record", "receipt"])
def test_unconfirmed_direct_rowcounts_roll_back_the_whole_registration(phase, count):
    engine, operator, plan = arranged()
    if phase == "record":
        engine.write_count = count
    else:
        engine.receipt_count = count
    with pytest.raises(DeploymentConflict):
        operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert not engine.records and not engine.receipts


@pytest.mark.parametrize("age", [-1, 900, 901])
def test_prepare_refuses_future_or_stale_capture_without_writes(age):
    engine = Engine()
    engine.now = NOW + timedelta(seconds=age)
    with pytest.raises(DeploymentConflict, match="fifteen minutes"):
        bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET).prepare(capture())
    assert not engine.writes


def test_maintenance_expectation_is_explicit_and_never_changed():
    engine = Engine()
    engine.set_control(maintenance=True)
    value = bootstrap.ConnectorCapture.model_validate(capture().model_dump() | {"expected_maintenance": True})
    operator = bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET)
    plan = operator.prepare(value)
    before = engine.control_payload
    operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert engine.control_payload == before and engine.control.maintenance


def test_capture_must_remain_fresh_after_waiting_for_sql_fences():
    engine, operator, plan = arranged()
    engine.delay_snapshot = True
    with pytest.raises(DeploymentConflict, match="freshness changed"):
        operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert not engine.writes


def test_specific_unbound_planned_connector_can_be_assigned_once_without_replacing_intent():
    engine = Engine()
    value = capture()
    planned = m.OwnedConnectorManifest(
        tenant_id=TENANT, epoch=EPOCH, connector_id=CONNECTOR, ownership_id=OWNER,
        revision=3, policy_revision=0, name=value.name, sources=(), desired_definition={},
        state="planned", updated_at=NOW - timedelta(minutes=1),
    )
    engine.add(planned)
    value = bootstrap.ConnectorCapture.model_validate(value.model_dump() | {"expected_connector_revision": 3})
    operator = bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET)
    plan = operator.prepare(value)
    assert plan.prior_connector_sha256 == bootstrap._payload_hash(engine.records[0][6])
    operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert len(engine.records) == 1 and engine.records[0][4] == 4
    assert plan.connector.name == planned.name and plan.connector.ownership_id == planned.ownership_id
    assert any("connector-bootstrap:assign" in sql for sql, _ in engine.writes)
    with pytest.raises(DeploymentConflict, match="unbound planned"):
        operator.prepare(value)


@pytest.mark.parametrize("changed", [
    "owner", "endpoint", "physical", "source", "desired", "operation",
    "state", "readiness", "delivery_proof", "revision",
])
def test_established_connector_is_never_replaced_or_adopted(changed):
    engine = Engine()
    value = capture()
    prior = m.OwnedConnectorManifest(
        tenant_id=TENANT, epoch=EPOCH, connector_id=CONNECTOR, ownership_id=OWNER,
        revision=1, policy_revision=0, name=value.name, sources=(), desired_definition={},
        state="planned", updated_at=NOW,
    )
    updates = {
        "owner": {"ownership_id": str(UUID(int=900))},
        "endpoint": {"endpoint": value.endpoint},
        "physical": {"workspace_id": WORKSPACE, "eventstream_id": EVENTSTREAM},
        "source": {"sources": value.sources},
        "desired": {"desired_definition": value.connector(NOW).desired_definition},
        "operation": {"operation_id": str(UUID(int=901))},
        "state": {"state": "provisioning"},
        "readiness": {"identity_verified_at": NOW},
        "delivery_proof": {
            "identity_verified_at": NOW, "delivery_verified_at": NOW,
            "delivery_proof": m.ConnectorDeliveryProof(
                request_id=str(UUID(int=902)), receipt_key="original-signal",
                collector_identity_id=str(UUID(int=903)), received_at=NOW, identity_verified_at=NOW,
            ),
        },
        "revision": {"policy_revision": 1},
    }[changed]
    prior = m.OwnedConnectorManifest.model_validate(prior.model_dump() | updates)
    engine.add(prior)
    value = bootstrap.ConnectorCapture.model_validate(value.model_dump() | {"expected_connector_revision": 1})
    operator = bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET)
    with pytest.raises(DeploymentConflict, match="unbound planned"):
        operator.prepare(value)
    assert not engine.writes


@pytest.mark.parametrize("changed", ["physical", "endpoint", "ownership", "source", "old_epoch"])
def test_other_connector_and_historical_physical_ownership_cannot_be_adopted(changed):
    engine = Engine()
    value = capture()
    prior = value.connector(NOW).model_dump()
    prior.update(connector_id=str(UUID(int=970)), ownership_id=str(UUID(int=971)))
    if changed != "physical":
        prior.update(workspace_id=str(UUID(int=972)), eventstream_id=str(UUID(int=973)))
    if changed != "endpoint":
        prior["endpoint"] = {"namespace": "other.servicebus.windows.net", "entity": "other", "consumer_group": "$Default"}
    if changed == "ownership":
        prior["ownership_id"] = OWNER
    if changed not in {"source", "old_epoch"}:
        prior["sources"] = ()
    if changed == "old_epoch":
        prior["epoch"] = str(UUID(int=974))
        prior["sources"] = tuple(
            source.model_copy(update={"target": source.target.model_copy(update={"epoch": prior["epoch"]})})
            for source in value.sources
        )
    engine.add(m.OwnedConnectorManifest.model_validate(prior))
    with pytest.raises(DeploymentConflict, match="already registered"):
        bootstrap.ConnectorBootstrapOperator(SqlFake(engine), TARGET).prepare(value)
    assert not engine.writes


def rehash_readback(value):
    value["readback_sha256"] = fingerprint({
        "item": value["item"], "definition": value["definition_response"],
        "topology": value["topology_response"],
        "observed_at": TypeAdapter(m.UtcDateTime).validate_python(value["observed_at"]).isoformat(),
    })


@pytest.mark.parametrize("alias", ["primaryKey", "secondaryKey", "apiKey", "credentials"])
@pytest.mark.parametrize("sentinel", [123, "sentinel-value"])
@pytest.mark.parametrize("location", ["topology_only", "definition_and_topology", "definition_only"])
def test_rehashed_credential_aliases_refuse_before_sql_and_output(tmp_path, capsys, alias, sentinel, location):
    value = capture().model_dump(mode="json")
    nested = {"metadata": [{alias: sentinel}]}
    if location != "definition_only":
        value["topology_response"]["destinations"][0].update(copy.deepcopy(nested))
    if location != "topology_only":
        path = "eventstreamProperties.json" if location == "definition_only" else "eventstream.json"
        part = next(part for part in value["definition_response"]["definition"]["parts"] if part["path"] == path)
        decoded = json.loads(base64.b64decode(part["payload"]))
        if location == "definition_only":
            decoded.update(copy.deepcopy(nested))
        else:
            decoded["destinations"][0].update(copy.deepcopy(nested))
        part["payload"] = base64.b64encode(canonical_json(decoded).encode()).decode()
    rehash_readback(value)
    raw = json.dumps(value)
    with pytest.raises(DeploymentError, match="credential-bearing"):
        bootstrap.ConnectorCapture.model_validate_json(raw)
    capture_path, output = tmp_path / "capture.json", tmp_path / "plan.json"
    capture_path.write_text(raw)

    def no_sql(*_):
        pytest.fail("Credential-bearing evidence must be refused before opening SQL")

    assert cli.main([
        *cli_args(), "--prepare", "--capture", str(capture_path), "--output", str(output),
    ], database_factory=no_sql) == 1
    assert not output.exists()
    result = capsys.readouterr()
    assert result.out == ""
    assert "sentinel-value" not in result.err


@pytest.mark.parametrize("alias", ["PRIMARY_KEY", "Secondary-Key", "api key", "Credentials"])
@pytest.mark.parametrize("value", [None, True, ["sentinel"], {"nested": 123}])
def test_normalized_credential_alias_refusal_does_not_depend_on_value_type(alias, value):
    with pytest.raises(DeploymentError, match="credential-bearing"):
        bootstrap._nonsecret({"nested": [{alias: value}]})


def test_nonsecret_identity_and_hash_keys_are_not_blanket_refused():
    bootstrap._nonsecret({
        "connector_id": CONNECTOR, "ownership_id": OWNER, "source_id": str(UUID(int=20)),
        "component_ids": {"sources/owned": str(UUID(int=20))},
        "record": {"full_key": "owned-source", "key_hash": "a" * 64},
        "request_hash": "b" * 64, "definition_hash": "c" * 64, "source_sha256": "d" * 64,
    })


@pytest.mark.parametrize("changed", [
    "marker", "same_name_foreign_item", "creation_hash", "receipt_hash", "readback_hash",
    "source_target", "source_id", "source_origin", "event_types", "duplicate_source",
    "topology_node", "topology_id", "routing", "partial_parts", "credential_field",
    "endpoint_secret", "future_creation", "wrong_tenant", "extra_input", "invalid_definition_envelope",
])
def test_closed_capture_requires_original_ownership_and_complete_exact_bindings(changed):
    value = capture().model_dump(mode="json")
    if changed == "marker":
        value["item"]["description"] = "Same display name is not ownership"
    elif changed == "same_name_foreign_item":
        value["item"]["id"] = str(UUID(int=888))
    elif changed in {"creation_hash", "receipt_hash", "readback_hash"}:
        value[{"creation_hash": "creation_request_sha256", "receipt_hash": "creation_receipt_sha256",
               "readback_hash": "readback_sha256"}[changed]] = "0" * 64
    elif changed == "source_target":
        value["sources"][0]["target"]["item_id"] = str(UUID(int=887))
    elif changed == "source_id":
        value["sources"][0]["source_id"] = str(UUID(int=887))
    elif changed == "source_origin":
        value["sources"][0]["event_source"] = str(UUID(int=887))
    elif changed == "event_types":
        value["sources"][0]["event_types"] = ["unsupported"]
    elif changed == "duplicate_source":
        value["sources"].append(copy.deepcopy(value["sources"][0]))
    elif changed == "topology_node":
        value["topology_response"]["sources"].append(copy.deepcopy(value["topology_response"]["sources"][0]))
    elif changed == "topology_id":
        value["topology_response"]["destinations"][0]["id"] = value["topology_response"]["sources"][0]["id"]
    elif changed == "routing":
        value["topology_response"]["streams"][0]["inputNodes"] = []
    elif changed == "partial_parts":
        value["definition_response"]["definition"]["parts"] = []
    elif changed == "credential_field":
        value["topology_response"]["destinations"][0]["accessKeys"] = {"primaryKey": "do-not-store"}
    elif changed == "endpoint_secret":
        value["endpoint"]["entity"] = "events?sig=do-not-store"
    elif changed == "future_creation":
        value["creation_receipt"]["completed_at"] = (NOW + timedelta(seconds=1)).isoformat()
        value["creation_receipt_sha256"] = fingerprint(value["creation_receipt"])
    elif changed == "wrong_tenant":
        value["sources"][0]["target"]["tenant_id"] = str(UUID(int=885))
    elif changed == "invalid_definition_envelope":
        value["definition_response"]["definition"] = []
    else:
        value["identity_verified_at"] = NOW.isoformat()
    if changed in {"topology_node", "topology_id", "routing", "partial_parts", "credential_field"}:
        rehash_readback(value)
    with pytest.raises((DeploymentError, ValueError)):
        bootstrap.ConnectorCapture.model_validate_json(json.dumps(value))


def cli_args():
    return [
        "--server", TARGET.server, "--database", TARGET.database,
        "--tenant-id", TENANT, "--deployer-object-id", DEPLOYER,
        "--credential", "azure-cli", "--subscription-id", str(UUID(int=801)),
    ]


def test_cli_prepare_apply_reconcile_with_explicit_identity_and_original_plan(tmp_path, capsys, monkeypatch):
    engine = Engine()
    calls = []

    def factory(target, selection):
        calls.append((target, selection))
        assert target == TARGET and selection.mode == "azure-cli"
        return SqlFake(engine)

    capture_path, plan_path = tmp_path / "capture.json", tmp_path / "plan.json"
    capture_path.write_text(capture().model_dump_json())
    monkeypatch.setenv("AZURE_SQL_DATABASE", "ignored-wrong-database")
    assert cli.main([
        *cli_args(), "--prepare", "--capture", str(capture_path), "--output", str(plan_path),
    ], database_factory=factory) == 0
    assert not engine.writes
    document = cli.PlanDocument.model_validate_json(plan_path.read_bytes())
    assert cli.main([
        *cli_args(), "--apply", "--plan", str(plan_path), "--confirm-manifest-hash", document.manifest_hash,
    ], database_factory=factory) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["receipt"]["state"] == "registered_metadata_only"
    writes = len(engine.writes)
    assert cli.main([*cli_args(), "--reconcile", "--plan", str(plan_path)], database_factory=factory) == 0
    assert len(engine.writes) == writes and len(calls) == 3
    assert json.loads(capsys.readouterr().out)["replayed"]


def test_output_becomes_visible_only_after_complete_flush_fsync_and_exclusive_link(tmp_path, monkeypatch):
    output = tmp_path / "ack.json"
    content = json.dumps({"state": "complete", "items": list(range(100))}) + "\n"
    events = []
    real_sync, real_link = cli.os.fsync, cli.os.link

    def fsync(descriptor):
        assert not output.exists()
        events.append("fsync")
        return real_sync(descriptor)

    def link(source, destination):
        assert events == ["fsync"]
        assert Path(source).parent == output.parent and Path(source) != output
        assert Path(destination) == output and not output.exists()
        assert Path(source).read_text(encoding="utf-8") == content
        events.append("link")
        real_link(source, destination)
        assert output.read_text(encoding="utf-8") == content

    monkeypatch.setattr(cli.os, "fsync", fsync)
    monkeypatch.setattr(cli.os, "link", link)
    cli._publish_output(output, content)
    assert events == ["fsync", "link"]
    assert list(tmp_path.iterdir()) == [output]


@pytest.mark.parametrize("mode", ["prepare", "apply", "reconcile"])
@pytest.mark.parametrize("fault", ["create", "before_write", "during_write", "short_write", "flush", "fsync", "link"])
def test_output_failure_never_leaves_a_final_ack_or_replays_mutations(tmp_path, monkeypatch, capsys, mode, fault):
    engine, operator, plan = arranged()
    capture_path, plan_path = tmp_path / "capture.json", tmp_path / "original-plan.json"
    output, unrelated = tmp_path / "ack.json", tmp_path / "unrelated.tmp"
    capture_path.write_text(plan.capture.model_dump_json())
    plan_path.write_text(cli.PlanDocument(manifest_hash=plan.manifest_hash, plan=plan).model_dump_json())
    unrelated.write_bytes(b"retain unrelated file")
    if mode == "reconcile":
        operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    real_temporary = cli.tempfile.NamedTemporaryFile

    def fail(*_args, **_kwargs):
        raise OSError("Fixture local output failure")

    def temporary(**kwargs):
        assert Path(kwargs["dir"]) == output.parent
        if fault == "create":
            fail()
        stream = real_temporary(**kwargs)
        write = stream.write

        def incomplete(content):
            if fault == "before_write":
                fail()
            write(content[:1])
            if fault == "during_write":
                fail()
            return 1

        if fault in {"before_write", "during_write", "short_write"}:
            stream.write = incomplete
        if fault == "flush":
            stream.flush = fail
        return stream

    monkeypatch.setattr(cli.tempfile, "NamedTemporaryFile", temporary)
    if fault == "fsync":
        monkeypatch.setattr(cli.os, "fsync", fail)
    if fault == "link":
        monkeypatch.setattr(cli.os, "link", fail)
    arguments = [*cli_args(), "--" + mode, "--output", str(output)]
    if mode == "prepare":
        arguments.extend(("--capture", str(capture_path)))
    else:
        arguments.extend(("--plan", str(plan_path)))
    if mode == "apply":
        arguments.extend(("--confirm-manifest-hash", plan.manifest_hash))
    assert cli.main(arguments, database_factory=lambda *_: SqlFake(engine)) == 1
    assert not output.exists()
    assert sorted(path.name for path in tmp_path.iterdir()) == ["capture.json", "original-plan.json", "unrelated.tmp"]
    assert unrelated.read_bytes() == b"retain unrelated file"
    assert len(engine.writes) == (0 if mode == "prepare" else 2)
    captured = capsys.readouterr()
    assert captured.out == ""
    error = json.loads(captured.err)
    if mode == "prepare":
        assert "committed_receipt" not in error
        assert not engine.records and not engine.receipts
    else:
        assert error["committed_receipt"]["request_id"] == plan.capture.request_id
        assert error["committed_receipt"]["manifest_hash"] == plan.manifest_hash
        assert len(engine.records) == len(engine.receipts) == 1


@pytest.mark.parametrize("existing", [b"", b'{"original":"complete"}\n'])
def test_existing_final_output_is_never_overwritten_even_when_empty(tmp_path, existing):
    output = tmp_path / "ack.json"
    output.write_bytes(existing)
    with pytest.raises(FileExistsError):
        cli._publish_output(output, '{"replacement":true}\n')
    assert output.read_bytes() == existing
    assert list(tmp_path.iterdir()) == [output]

    def no_sql(*_):
        pytest.fail("Existing final output must be refused before opening SQL")

    assert cli.main([
        *cli_args(), "--apply", "--plan", "unused.json", "--confirm-manifest-hash", "0" * 64,
        "--output", str(output),
    ], database_factory=no_sql) == 1
    assert output.read_bytes() == existing


def test_competing_final_ack_cannot_be_overwritten_after_sql_commit(tmp_path, monkeypatch, capsys):
    engine, _, plan = arranged()
    plan_path, output = tmp_path / "original-plan.json", tmp_path / "ack.json"
    plan_path.write_text(cli.PlanDocument(manifest_hash=plan.manifest_hash, plan=plan).model_dump_json())
    real_link = cli.os.link
    original = b'{"other_original":"retain"}\n'

    def competing_link(source, destination):
        Path(destination).write_bytes(original)
        real_link(source, destination)

    monkeypatch.setattr(cli.os, "link", competing_link)
    assert cli.main([
        *cli_args(), "--apply", "--plan", str(plan_path), "--confirm-manifest-hash", plan.manifest_hash,
        "--output", str(output),
    ], database_factory=lambda *_: SqlFake(engine)) == 1
    assert output.read_bytes() == original
    assert sorted(path.name for path in tmp_path.iterdir()) == ["ack.json", "original-plan.json"]
    assert len(engine.writes) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["committed_receipt"]["request_id"] == plan.capture.request_id


def test_lost_file_ack_reconciles_original_plan_select_only_to_complete_final(tmp_path, monkeypatch, capsys):
    engine, _, plan = arranged()
    plan_path, output = tmp_path / "original-plan.json", tmp_path / "ack.json"
    cli._publish_output(plan_path, cli.PlanDocument(manifest_hash=plan.manifest_hash, plan=plan).model_dump_json() + "\n")
    original_plan = plan_path.read_bytes()

    def fail(_descriptor):
        raise OSError("Fixture acknowledgement fsync failure")

    with monkeypatch.context() as patch:
        patch.setattr(cli.os, "fsync", fail)
        assert cli.main([
            *cli_args(), "--apply", "--plan", str(plan_path), "--confirm-manifest-hash", plan.manifest_hash,
            "--output", str(output),
        ], database_factory=lambda *_: SqlFake(engine)) == 1
    assert not output.exists()
    original_receipt = json.loads(capsys.readouterr().err)["committed_receipt"]
    rows = copy.deepcopy((engine.records, engine.receipts))
    writes = len(engine.writes)
    engine.now += timedelta(days=1)
    engine.set_control(revision=1, maintenance=True)
    engine.reads.clear()
    assert cli.main([
        *cli_args(), "--reconcile", "--plan", str(plan_path), "--output", str(output),
    ], database_factory=lambda *_: SqlFake(engine)) == 0
    assert json.loads(output.read_bytes())["receipt"] == original_receipt
    assert json.loads(output.read_bytes())["replayed"]
    assert plan_path.read_bytes() == original_plan
    assert len(engine.writes) == writes and (engine.records, engine.receipts) == rows
    assert all("connector-bootstrap:identity" in sql or "connector-bootstrap:receipt" in sql for sql, _ in engine.reads)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["ack.json", "original-plan.json"]
    assert capsys.readouterr().out == ""


def test_cli_rejects_ambiguous_input_overwrite_or_missing_approval_before_sql(tmp_path):
    def forbidden(*_):
        pytest.fail("Invalid operator input must not open SQL")

    with pytest.raises(SystemExit):
        cli.main([*cli_args(), "--apply", "--plan", "unknown"], database_factory=forbidden)
    path = tmp_path / "capture.json"
    path.write_text('{"request_id":"duplicate","request_id":"duplicate"}')
    assert cli.main([*cli_args(), "--prepare", "--capture", str(path)], database_factory=forbidden) == 1
    assert cli.main([
        *cli_args(), "--prepare", "--capture", str(path), "--output", str(path),
    ], database_factory=forbidden) == 1
    with pytest.raises(DeploymentError, match="pinned"):
        bootstrap.ConnectorBootstrapOperator(SimpleNamespace(_server=TARGET.server, _database=TARGET.database), TARGET)


def test_wrong_approval_and_conflicting_original_request_never_mutate():
    engine, operator, plan = arranged()
    with pytest.raises(DeploymentConflict, match="reviewed plan hash"):
        operator.apply(plan, confirmed_manifest_hash="0" * 64)
    assert not engine.writes
    with pytest.raises(DeploymentUncertain):
        operator.reconcile(plan)
    operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    writes = len(engine.writes)
    engine.receipts[0]["fingerprint"] = "0" * 64
    with pytest.raises(DeploymentConflict, match="immutable receipt"):
        operator.reconcile(plan)
    with pytest.raises(DeploymentConflict, match="immutable receipt"):
        operator.apply(plan, confirmed_manifest_hash=plan.manifest_hash)
    assert len(engine.writes) == writes
