from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from test_monitoring_sql_frontiers import _try_convert
from test_monitoring_sql_store import DriverRow, SqliteConnection
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringUnavailable,
)
from triage.monitoring.events import ReceiverHeartbeat
from triage.monitoring.memory import StoredRecord, key_digest
from triage.monitoring.schema import permission_kernel_objects, runtime_table_permissions
from triage.monitoring.sql_kernel_contracts import KERNEL_VERSION, SqlNames, rpc_contracts
from triage.monitoring.sql_permissions import runtime_grants
from triage.monitoring.sql_store import AzureSqlMonitoringStore
from triage.store.azure_sql import SqlCommitUncertain


class KernelProtocolDatabase:
    """RPC/role/result-shape test double, not native SQL permission acceptance."""

    def __init__(self, h, *, principal, tables=None):
        self._tables = tables or {}
        self.names = SqlNames.from_tables(self._tables)
        self.contracts = rpc_contracts(self._tables)
        self.principal = principal
        self.control = h.control
        self.context = m.MonitoringContext(tenant_id=h.control.tenant_id, epoch=h.control.epoch)
        self.clock = h.clock
        self.calls = []
        self.records = {}
        self.receipts = {}
        self.active = False
        self.fail_commit = None
        self.bad_result = None
        self.rpc_error = None
        self.missing_procedures = set()

    @contextmanager
    def transaction(self):
        assert not self.active, "RPCs must join the caller's single synchronous transaction"
        self.active = True
        before = deepcopy((self.records, self.receipts))
        try:
            yield self
            if self.fail_commit == "before":
                self.fail_commit = None
                raise SqlCommitUncertain("Injected lost acknowledgement before commit")
        except BaseException:
            self.records, self.receipts = before
            raise
        finally:
            self.active = False
        if self.fail_commit == "after":
            self.fail_commit = None
            raise SqlCommitUncertain("Injected lost acknowledgement after commit")

    def execute(self, sql, *params):
        assert self.active
        self.calls.append(("execute", sql, params))
        allowed = {
            "worker": ("worker_catalogue", "worker_evidence", "worker_telemetry"),
            "web": ("web_drafts",),
            "controller": ("controller_projections", "controller_immutable"),
        }
        if not any(self.names.object(name) in sql for name in allowed[self.principal]):
            raise RuntimeError("SQL role refused view DML (51070)")
        assert not sql.startswith("EXEC "), "Static procedures return result_json, not execute rowcount"
        return 1

    def query(self, sql, *params):
        assert self.active
        self.calls.append(("query", sql, params))
        if sql.startswith("SELECT OBJECT_ID("):
            assert ("'V'" in sql or "'P'" in sql) and "'U'" not in sql
            return [DriverRow(tuple(None if name in self.missing_procedures else 1 for name in params))]
        if sql.startswith("SELECT singleton"):
            return [DriverRow((1,))]
        if sql.startswith("SELECT schema_version"):
            control = self.control
            return [DriverRow((
                control.schema_version, control.tenant_id, control.epoch, control.revision,
                control.activation_cutoff, control.maintenance, control.updated_at, control.model_dump_json(),
            ))]
        if sql == "SELECT SYSUTCDATETIME()":
            return [DriverRow((self.clock(),))]
        if sql.startswith("SELECT request_id, fingerprint"):
            operation, digest = params[2:]
            matches = [
                (request_id, receipt) for (name, request_id), receipt in self.receipts.items()
                if name == operation and bytes.fromhex(key_digest(request_id)) == digest
            ]
            return [DriverRow((
                request_id, receipt["fingerprint"], receipt["recorded_at"], json.dumps(receipt["payload"]),
            )) for request_id, receipt in matches]
        if sql.startswith("WITH recent AS") or (
            sql.startswith("SELECT TOP (1) record_kind")
            and ("workspace_id IS NOT NULL" in sql or "OPENJSON(payload)" in sql)
        ):
            return self._query_work_progress(sql, params)
        if "ROW_NUMBER()" in sql:
            result = []
            for (kind, _), record in self.records.items():
                if kind == "work" and record.status == "queued":
                    result.append(DriverRow(self.record_row(record)))
            return result
        if sql.startswith("SELECT record_kind, full_key"):
            kind, digest = params[2:]
            return [
                DriverRow(self.record_row(row)) for (name, key), row in self.records.items()
                if name == kind and bytes.fromhex(key_digest(key)) == digest
            ]
        if sql.startswith("EXEC "):
            contract = next(value for value in self.contracts.values() if sql.startswith(f"EXEC {value.object_name} "))
            arguments = dict(zip((value.name for value in contract.parameters), params, strict=True))
            assert contract.bind({
                key: value.replace(tzinfo=UTC) if isinstance(value, datetime) and value.tzinfo is None else value
                for key, value in arguments.items()
            }) == (sql, params)
            if self.principal not in contract.components:
                raise RuntimeError("Actual SQL principal refused the selected route (51070)")
            if self.rpc_error is not None:
                raise self.rpc_error
            reply = self.apply_rpc(contract.operation, arguments)
            return self.bad_result if self.bad_result is not None else [DriverRow((json.dumps(reply),))]
        raise AssertionError(f"Unexpected SQL route: {sql}")

    def _query_work_progress(self, sql, params):
        def time_value(value):
            return value.replace(tzinfo=None).isoformat(timespec="microseconds") if isinstance(value, datetime) else value

        with sqlite3.connect(":memory:") as connection:
            connection.execute(
                "CREATE TABLE records (record_kind,full_key,revision,status,workload,workspace_id,item_id,"
                "target_key,parent_key,work_kind,generation_id,due_at,sequence_number,payload,key_hash,tenant_id,epoch)"
            )
            connection.execute("CREATE TABLE receipts (tenant_id,epoch,operation,request_id,recorded_at,payload)")
            connection.create_function("READ_UTC", 1, SqliteConnection.utc_time)
            connection.executemany("INSERT INTO records VALUES (" + ",".join("?" for _ in range(17)) + ")", [
                (*map(time_value, self.record_row(row)), row.context.tenant_id, row.context.epoch)
                for row in self.records.values()
            ])
            connection.executemany("INSERT INTO receipts VALUES (?,?,?,?,?,?)", [
                (
                    self.context.tenant_id, self.context.epoch, operation, request_id,
                    time_value(receipt["recorded_at"]), json.dumps(receipt["payload"]),
                ) for (operation, request_id), receipt in self.receipts.items()
            ])
            translated = sql.replace(self.names.object("controller_read"), "records")
            translated = translated.replace(self.names.object("receipts_controller"), "receipts")
            translated = SqliteConnection.translate(translated)
            translated = translated.replace("JSON_VALUE(", "json_extract(")
            translated = translated.replace("TRY_CONVERT(datetime2(6), ", "READ_UTC(")
            translated = self._policy_sql(connection, translated)
            return [DriverRow(tuple(row)) for row in connection.execute(translated, params).fetchall()]

    @staticmethod
    def _policy_sql(connection, sql):
        connection.create_function("TRY_BIGINT", 1, lambda value: _try_convert("bigint", value))
        connection.create_function(
            "DIGITS_ONLY", 1,
            lambda value: bool(re.fullmatch(r"[0-9]+", str(value))) if value is not None else False,
        )
        return (
            sql.replace("OPENJSON(payload) AS field", "json_each(payload) AS field")
            .replace("field.[type] = 2", "field.type IN ('integer', 'real')")
            .replace("field.[value] NOT LIKE '%[^0-9]%'", "DIGITS_ONLY(CAST(field.value AS TEXT))")
            .replace("field.[value]", "CAST(field.value AS TEXT)")
            .replace("field.[key]", "field.key")
            .replace("TRY_CONVERT(bigint, ", "TRY_BIGINT(")
        )

    @staticmethod
    def record_row(row):
        return (
            row.kind, row.key, row.version, row.status, row.workload, row.workspace_id,
            row.item_id, row.target_key, row.parent_key, row.work_kind, row.generation_id,
            row.due_at, row.sequence_number, row.payload, bytes.fromhex(key_digest(row.key)),
        )

    def save_work(self, work):
        self.records[("work", work.work_id)] = StoredRecord(
            kind="work", key=work.work_id, context=m.MonitoringContext(
                tenant_id=work.tenant_id, epoch=work.epoch,
            ), payload=work.model_dump_json(), version=work.revision, status=work.state,
            work_kind=work.kind, due_at=work.lease.expires_at if work.lease else work.due_at,
            target_key=work.target.key if work.target else None,
            # Native producer handoffs retain the target only in their payload.
            workspace_id=work.target.workspace_id if work.target and work.kind != "reconcile_state" else None,
        )

    def apply_rpc(self, operation, arguments):
        def envelope(result, *, status="applied"):
            return {
                "kernel_version": KERNEL_VERSION, "operation": operation, "status": status,
                "affected_rows": 0 if status in {"read", "replayed"} else 1, "result": result,
            }
        if operation in {"lock_context", "inspect"}:
            return envelope({
                "tenant_id": self.control.tenant_id, "epoch": self.control.epoch,
                "revision": self.control.revision, "maintenance": self.control.maintenance,
                "observed_at": self.clock().isoformat(),
            }, status="read")
        request_id = arguments.get("request_id")
        binding = hashlib.sha256(json.dumps(
            {key: value for key, value in arguments.items() if key not in {"request_id", "fingerprint"}},
            sort_keys=True, default=str,
        ).encode("utf-16-le")).hexdigest()
        prior = self.receipts.get((operation, request_id))
        if prior is not None:
            if prior["fingerprint"] != arguments["fingerprint"] or prior["payload"]["binding_hash"] != binding:
                raise RuntimeError("Original operation fingerprint changed (51072)")
            return envelope(prior["payload"]["result"], status="replayed")
        if operation == "controller.enqueue_work":
            if arguments["expected_revision"] != self.control.revision:
                raise RuntimeError("New operation lost its configuration revision (51072)")
            draft = m.MonitoringWorkDraft.model_validate_json(arguments["draft_json"])
            work = m.MonitoringWork(**draft.model_dump(), state="queued", revision=1)
            if ("work", work.work_id) in self.records:
                raise RuntimeError("Immutable work identity already exists (51072)")
            self.save_work(work)
            result = {"work_id": work.work_id, "work": work.model_dump(mode="json")}
        elif operation.endswith(".claim_work"):
            row = self.records[("work", arguments["work_id"])]
            work = m.MonitoringWork.model_validate_json(row.payload)
            lease = m.LeaseToken(
                tenant_id=work.tenant_id, epoch=work.epoch, resource_key=work.key,
                owner_id=arguments["owner_id"], fence=1, acquired_at=self.clock(),
                expires_at=self.clock() + timedelta(seconds=arguments["lease_seconds"]),
            )
            work = m.MonitoringWork.model_validate({
                **work.model_dump(), "state": "leased", "lease": lease, "revision": work.revision + 1,
                "attempts": work.attempts + 1,
            })
            self.save_work(work)
            return envelope({"work": work.model_dump(mode="json"), "lease": lease.model_dump(mode="json")})
        elif operation == "worker.record_heartbeat":
            heartbeat = ReceiverHeartbeat(
                tenant_id=arguments["tenant_id"], epoch=arguments["epoch"],
                worker_id=arguments["worker_id"], connector_id=arguments["connector_id"],
                observed_at=self.clock(), state=arguments["state"],
                transport_connected=arguments["transport_connected"],
                accepted_positions=arguments["accepted_positions"],
                error_code=arguments["error_code"],
                last_delivery_at=(
                    arguments["last_delivery_at"].replace(tzinfo=UTC) if arguments["last_delivery_at"] else None
                ),
                last_maintenance_at=(
                    arguments["last_maintenance_at"].replace(tzinfo=UTC) if arguments["last_maintenance_at"] else None
                ),
            )
            result = heartbeat.model_dump(mode="json")
        else:
            raise AssertionError(f"Unexpected RPC in this bounded protocol test: {operation}")
        self.receipts[(operation, request_id)] = {
            "fingerprint": arguments["fingerprint"], "recorded_at": self.clock(),
            "payload": {"binding_hash": binding, "result": result},
        }
        return envelope(result)


def sql_store(component="controller", *, principal=None, tables=None):
    h = Harness()
    db = KernelProtocolDatabase(h, principal=principal or component, tables=tables)
    store = AzureSqlMonitoringStore(db=db, component=component)
    return h, db, store


def inventory_draft(h):
    return m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory",
        policy_revision=h.version.revision, due_at=h.clock(), created_at=h.clock(),
        reason="Explicit controller-published collector work.",
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
    )


def test_runtime_requires_explicit_component_and_has_no_fixture_credential_route():
    h = Harness()
    db = KernelProtocolDatabase(h, principal="worker")
    with pytest.raises(TypeError):
        AzureSqlMonitoringStore(db=db)
    with pytest.raises(ValidationError):
        AzureSqlMonitoringStore(db=db, component="fixture")
    assert db.calls == []


def test_bootstrap_inspection_names_missing_kernel_semantics_instead_of_healthy_success():
    _, db, store = sql_store()
    db.missing_procedures.add(db.contracts["controller.publish_source"].object_name)
    inspection = store.inspect_bootstrap(expected_tenant_id=uid(1))
    assert inspection.status == "kernel_incomplete"
    assert inspection.missing_operations == ("controller.publish_source",)
    assert not any("CREATE " in sql or "ALTER " in sql for _, sql, _ in db.calls)
    assert not any("UPDATE " in sql for _, sql, _ in db.calls)


@pytest.mark.parametrize("component", ["worker", "web", "controller"])
def test_complete_component_routes_and_matching_v2_inspection_can_report_ready(component):
    _, db, store = sql_store(component)
    result = store.inspect_bootstrap(expected_tenant_id=uid(1))
    assert result.status == "ready" and result.missing_operations == ()
    assert any("EXEC " in sql and "triage_mon_inspect_" in sql for _, sql, _ in db.calls)


@pytest.mark.parametrize("component,kind", [
    ("worker", "target"), ("worker", "source"), ("worker", "review"),
    ("worker", "action"), ("worker", "incident_state"), ("worker", "work"),
    ("web", "target_capability"), ("web", "review"), ("web", "source_head"),
    ("controller", "inventory"), ("controller", "scope"), ("controller", "action"),
])
def test_component_cannot_mutate_another_record_family_directly(component, kind):
    h, db, store = sql_store(component)
    with pytest.raises(MonitoringComponentDenied):
        store._sql.put(StoredRecord(
            kind=kind, key=uid(90), context=h.version, payload="{}", version=1,
        ))
    assert db.calls == []


def test_actual_sql_principal_not_the_component_argument_authorizes_a_route():
    h, db, store = sql_store("controller", principal="worker")
    with pytest.raises(MonitoringComponentDenied, match="51070"):
        store.enqueue_work(inventory_draft(h))
    assert not db.records and not db.receipts
    assert not any(method == "execute" for method, _, _ in db.calls)


@pytest.mark.parametrize("kind,component", [("inventory", "worker"), ("target", "controller"), ("plan", "web")])
def test_checked_view_updates_never_write_identity_columns_or_base_tables(kind, component):
    h, db, store = sql_store(component)
    row = StoredRecord(kind=kind, key=uid(90), context=h.version, payload="{}", version=2)
    with store._sql.transaction(write=True, operation="fixture_route_assertion", request_id=uid(90)):
        store._sql.put(row)
    update = next(sql for method, sql, _ in db.calls if method == "execute")
    assignments = update.split(" SET ", 1)[1].split(" WHERE ", 1)[0]
    assert not any(name in assignments for name in ("full_key", "tenant_id", "epoch", "record_kind", "key_hash"))
    assert "[dbo].[triage_mon_" in update
    assert "[dbo].[triage_monitoring_records]" not in update


def test_immutable_worker_evidence_and_controller_bindings_cannot_be_overwritten():
    h, db, store = sql_store("worker")
    with pytest.raises(MonitoringComponentDenied):
        store._sql.put(StoredRecord(kind="capability", key=uid(99), context=h.version, payload="{}", version=2))
    _, other_db, controller = sql_store()
    with pytest.raises(MonitoringComponentDenied):
        controller._sql.put(StoredRecord(kind="approval_binding", key=uid(99), context=h.version, payload="{}", version=2))
    assert not db.calls and not other_db.calls


def test_even_permitted_view_writes_require_the_shared_transaction():
    h, db, worker = sql_store("worker")
    with pytest.raises(MonitoringConflict, match="acceptance transaction"):
        worker._sql.put(StoredRecord(
            kind="capability", key=uid(99), context=h.version, payload="{}", version=1,
        ))
    with pytest.raises(MonitoringConflict, match="monitoring transaction"):
        worker._sql.rpc("worker.claim_work", {
            **h.context(), "work_id": uid(99), "owner_id": uid(98), "lease_seconds": 120,
        })
    assert db.calls == []


@pytest.mark.parametrize("failure,observed", [("before", False), ("after", True)])
def test_rpc_lost_ack_preserves_original_receipt_and_replays_before_new_policy_cas(failure, observed):
    h, db, store = sql_store()
    draft = inventory_draft(h)
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain) as error:
        store.enqueue_work(draft)
    assert error.value.operation == "controller.enqueue_work"
    assert error.value.idempotency_id == draft.work_id
    assert bool(db.records) is observed
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 1})
    if observed:
        restored = store.enqueue_work(draft)
        assert restored.work_id == draft.work_id and len(db.records) == 1
    else:
        with pytest.raises(MonitoringConflict):
            store.enqueue_work(draft)
        assert not db.records
    assert not any(method == "execute" for method, _, _ in db.calls)


@pytest.mark.parametrize("bad_result", [
    [], [DriverRow(("{}",))], [DriverRow(("not-json",))],
    [DriverRow((json.dumps({"kernel_version": KERNEL_VERSION, "operation": "different", "status": "applied",
                          "affected_rows": 1, "result": {}}),))],
])
def test_invalid_rpc_output_rolls_back_instead_of_claiming_a_durable_write(bad_result):
    h, db, store = sql_store()
    db.bad_result = bad_result
    with pytest.raises(MonitoringUnavailable):
        store.enqueue_work(inventory_draft(h))
    assert not db.records and not db.receipts


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(7)])
def test_sql_routing_preserves_interruption_type_and_rolls_back(error):
    h, db, store = sql_store()
    db.rpc_error = error
    with pytest.raises(type(error)):
        store.enqueue_work(inventory_draft(h))
    assert not db.records and not db.receipts


def test_claim_routes_one_work_family_rpc_under_the_control_lock_not_generic_lease_dml():
    h, db, controller = sql_store()
    controller.enqueue_work(inventory_draft(h))
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    claimed = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(99), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))
    assert len(claimed) == 1 and claimed[0].lease.owner_id == uid(99)
    sql = "\n".join(sql for _, sql, _ in db.calls)
    assert "worker_claim_work" in sql and "lock_context" in sql
    assert "publication_pool" in sql and "ROW_NUMBER()" in sql
    assert "SET revision = revision" not in sql
    assert "[dbo].[triage_monitoring_leases]" not in sql
    assert not any(method == "execute" for method, _, _ in db.calls)


def test_stale_reservation_and_invalid_reconciliation_fail_before_effectful_sql():
    h, db, controller = sql_store()
    h.seed()
    h.activate()
    h.source_work()
    request = h.reserve_request(h.review())
    with pytest.raises(MonitoringConflict, match="registry revision"):
        controller.reserve_action(request)
    with pytest.raises(MonitoringConflict, match="own leased work"):
        controller.reconcile_work(h.work)
    assert not db.records and not db.receipts
    assert not any("controller_reserve_action" in sql or method == "execute" for method, sql, _ in db.calls)


def test_receiver_heartbeat_has_a_guarded_route_and_never_updates_delivery_proof():
    h, db, store = sql_store("worker")
    heartbeat = ReceiverHeartbeat(
        **h.context(), worker_id=uid(71), connector_id=uid(72), observed_at=h.clock(),
        state="running", transport_connected=True, last_delivery_at=h.clock() - timedelta(seconds=1),
        last_maintenance_at=h.clock() - timedelta(seconds=2),
    )
    result = store.record_receiver_heartbeat(heartbeat)
    assert result == heartbeat
    assert not db.records
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2})
    assert store.record_receiver_heartbeat(heartbeat) == result
    assert len(db.receipts) == 1
    assert not any(method == "execute" for method, _, _ in db.calls)


def test_stopping_heartbeat_is_recorded_as_stopping_not_silently_renamed():
    h, db, store = sql_store("worker")
    result = store.record_receiver_heartbeat(ReceiverHeartbeat(
        **h.context(), worker_id=uid(71), connector_id=uid(72), observed_at=h.clock(), state="stopping",
    ))
    assert result.state == "stopping" and len(db.receipts) == 1


def test_original_review_receipt_is_not_forged_from_a_newer_projection():
    h, db, store = sql_store("web")
    h.seed()
    h.activate()
    review = h.review()
    request_id = h.next_id()
    original = {
        "request_id": request_id, "intent_kind": "review", "intent_id": review.review_id,
        "expected_intent_revision": 1, "new_intent_revision": 2, "policy_revision": 2,
        "state": "pending-validation", "reconcile_work_id": h.next_id(),
        "original_intent": {
            **review.model_dump(mode="json", include={
                "review_id", "target", "action", "reviewer_id", "reviewed_at", "expires_at", "parameters",
                "parameter_hash", "definition_hash", "configuration_hash", "replay_safe", "detail",
            }), "requested_state": "revoked",
        },
    }
    db.receipts[("web.commit_intent", request_id)] = {
        "fingerprint": "a" * 64, "recorded_at": h.clock(),
        "payload": {"binding_hash": "b" * 64, "result": original},
    }
    receipt = store.get_safety_review_operation(h.version, request_id)
    assert receipt.request_id == request_id and receipt.new_review_revision == 2
    assert receipt.requested_state == "revoked" and receipt.review.state == "pending"
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 5})
    assert store.get_safety_review_operation(h.version, request_id) == receipt
    assert store.get_safety_review_operation(h.version, h.next_id()) is None
    assert not any("record_kind" in sql for _, sql, _ in db.calls)


def test_deployer_catalogue_extends_objects_without_broad_runtime_grants():
    tables = {"monitoring_records": "fixture_monitoring_records"}
    with pytest.raises(MonitoringKernelUnsupported):
        runtime_table_permissions(tables)
    objects = permission_kernel_objects(tables)
    assert any(item["kind"] == "procedure" for item in objects)
    assert any(item["kind"] == "view" for item in objects)
    for component in ("worker", "web", "controller"):
        assert not any("OBJECT::[dbo].[fixture_monitoring_records]" in statement for statement in runtime_grants(component, tables))
