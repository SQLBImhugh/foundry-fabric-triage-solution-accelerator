from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
import socket
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError

from scripts import reset_monitoring_state as reset
from triage.monitoring import models as m
from triage.monitoring.deployment_authority import read_authority
from triage.monitoring.deployment_schema import unqualified
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.store.azure_sql import AzureSqlDatabase, SqlCommitUncertain, SqlUnavailable

TENANT = str(UUID(int=1))
OLD_EPOCH = str(UUID(int=2))
DEPLOYER = str(UUID(int=3))
OLD_BOOTSTRAP = str(UUID(int=5))
CLIENT = str(UUID(int=6))
SUBSCRIPTION = str(UUID(int=7))
RUNTIME_CLIENT = str(UUID(int=8))
RUNTIME_OBJECT = str(UUID(int=9))
WORKSPACE = str(UUID(int=31))
ITEM = str(UUID(int=32))
RUN = str(UUID(int=33))
NOW = datetime(2026, 9, 15, 12, tzinfo=UTC)
TARGET = reset.ResetTarget(
    server="fixture.database.windows.net", database="triage_fixture",
    tenant_id=TENANT, deployer_object_id=DEPLOYER,
)
SITE = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/fixture/providers/Microsoft.Web/sites/fixture-api"
IDENTITY_RESOURCE = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/fixture/"
    "providers/Microsoft.ManagedIdentity/userAssignedIdentities/runtime"
)
PRIVATE_TEXT = "Private incident contents and identity must never appear in a manifest"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *_: pytest.fail("Reset tests never use real networking"))


def jsonable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(type(value).__name__)


def digest_text(value):
    return hashlib.sha256(str(value).encode("utf-16-le")).hexdigest()


def token(tenant=TENANT, principal=DEPLOYER, expiry=None):
    expiry = expiry or int((datetime.now(UTC) + timedelta(hours=1)).timestamp())
    payload = base64.urlsafe_b64encode(json.dumps({"tid": tenant, "oid": principal, "exp": expiry}).encode()).decode().rstrip("=")
    return SimpleNamespace(token=f"fixture.{payload}.fixture", expires_on=expiry)


class OfflineCredential:
    def get_token(self, *scopes, **kwargs):
        return token()


@dataclass
class Clock:
    now: datetime = NOW

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


class TransactionalSqlFake(AzureSqlDatabase):
    """Mutable SQL rows/DDL with rollback and committed/uncommitted acknowledgement loss."""

    def __init__(self, *, initialized=True):
        self._server, self._database, self._tables = TARGET.server, TARGET.database, None
        self._credential = reset.PinnedDeployerCredential(OfflineCredential(), TARGET)
        self.actual_database, self.server_identity, self.database_id = TARGET.database, "fixture-sql-server", 7
        self.clock = Clock()
        self.catalogue = reset.build_catalogue()
        self.tables = {table.name: [] for table in self.catalogue.tables}
        self.tables["business_orders"] = [{"order_id": 1, "description": "retain unrelated data"}]
        self.columns = {
            table.name: [
                (column.name, column.data_type, column.max_length, column.scale, column.nullable,
                 column.collation or ("fixture_collation" if column.data_type in {"nvarchar", "varchar", "char"} else None),
                 False, False, 0, None)
                for column in table.columns
            ] for table in self.catalogue.tables
        }
        self.object_ids = {name: 100 + index for index, name in enumerate(sorted(self.tables))}
        self.module_ids = {item.name: 300 + index for index, item in enumerate(self.catalogue.modules)}
        self.procedures = {name: self.module_ids[name] for name in self.catalogue.procedures}
        self.native_module_hashes = {item.name: item.native_hash for item in self.catalogue.modules}
        self.role_ids = {name: 600 + index for index, name in enumerate(self.catalogue.roles)}
        self.extra_permissions, self.extra_role_edges = [], []
        self.sql_triggers, self.sql_queues = [], []
        self.safety = {name: (0, False, False, 0) for name in self.tables}
        self.fks = [
            ("dbo", edge.child, edge.child_column, "dbo", edge.parent, edge.parent_column, 0, 0, False)
            for edge in self.catalogue.foreign_keys
        ]
        self.active, self.lock = False, threading.RLock()
        self.queries, self.statements = [], []
        self.delete_calls, self.fail_delete_number = 0, None
        self.fail_receipt_insert = False
        self.commit_fault = None
        self.reconciliation_unavailable = False
        self.fail_bootstrap = False
        self.before_exclusive = self.after_commit = None
        self.sql_writer_principals = [(11, "E", UUID(RUNTIME_CLIENT).bytes_le, 0)]
        self.permission_metadata_visible = True
        self.operator_principal_id = 5
        self.public_write_permissions = 0
        self.deployment_bindings = (reset.WriterBinding(
            writer=reset.WriterSpec(writer_id="api", kind="app_service", resource_id=SITE),
            identity_client_id=RUNTIME_CLIENT, identity_object_id=RUNTIME_OBJECT, sql_principal_id=11,
        ),)
        self.control_model = m.DeploymentControl(
            tenant_id=TENANT, epoch=OLD_EPOCH, revision=9, maintenance=True,
            activation_cutoff=NOW - timedelta(days=10), updated_at=NOW - timedelta(days=1),
        )
        self.put_control(self.control_model, OLD_BOOTSTRAP)
        self.add("incidents", incident_id="private-incident", signature="signature", status="open", payload=PRIVATE_TEXT)
        self.add("agent_runs", run_id="run-one", state="completed")
        self.add("agent_events", event_id="event-one", run_id="run-one")
        self.add("incident_activity", activity_id="activity-one", kind="note", payload=PRIVATE_TEXT)
        self.add("processed", fingerprint="processed-one")
        self.add("monitoring_records", tenant_id=TENANT, epoch=OLD_EPOCH, record_kind="scope", full_key="scope-one", status="active")
        self.add("monitoring_records", tenant_id=TENANT, epoch=OLD_EPOCH, record_kind="target",
                 workload="fabric_pipeline", workspace_id=WORKSPACE, item_id=ITEM, full_key="pipeline-target",
                 payload=json.dumps({"identity": {
                     "tenant_id": TENANT, "epoch": OLD_EPOCH, "workload": "fabric_pipeline",
                     "workspace_id": WORKSPACE, "item_id": ITEM,
                 }}))
        self.add("rate_budget", tenant_id=TENANT, bucket_hash="b" * 64, request_limit=60, window_seconds=60, used=12)
        if not initialized:
            for logical in (*reset.monitoring_schema.DEFAULT_MONITORING_TABLES, "rate_budget"):
                self.tables.pop(self.table_name(logical))

    def table_name(self, logical):
        return self.catalogue.table(logical).name

    def add(self, logical, **changes):
        table = self.catalogue.table(logical)
        row = {}
        for column in table.columns:
            if column.nullable:
                row[column.name] = None
            elif column.data_type == "datetime2":
                row[column.name] = NOW
            elif column.data_type in {"int", "bigint"}:
                row[column.name] = 1
            elif column.data_type == "bit":
                row[column.name] = False
            elif column.data_type == "binary":
                row[column.name] = b"\0" * column.max_length
            else:
                row[column.name] = "{}" if column.name == "payload" else "fixture"
        row.update(changes)
        self.tables[table.name].append(row)
        return row

    def put_control(self, control, bootstrap_id, *, bootstrap_hash=None):
        payload = control.model_dump_json()
        self.tables[self.table_name("monitoring_control")] = [{
            "singleton": 1, "schema_version": control.schema_version, "tenant_id": control.tenant_id,
            "epoch": control.epoch, "revision": control.revision, "activation_cutoff": control.activation_cutoff,
            "maintenance": control.maintenance, "updated_at": control.updated_at,
            "bootstrap_id": bootstrap_id,
            "bootstrap_hash": bootstrap_hash or hashlib.sha256(payload.encode()).hexdigest(), "payload": payload,
        }]

    @contextmanager
    def transaction(self):
        with self.lock:
            assert not self.active, "Nested SQL transactions are forbidden"
            self.active = True
            before = copy.deepcopy(self.tables)
            ids_before = copy.deepcopy(self.object_ids)
            modules_before = copy.deepcopy(self.module_ids)
            hashes_before = copy.deepcopy(self.native_module_hashes)
            try:
                yield self
            except BaseException:
                self.tables, self.object_ids = before, ids_before
                self.module_ids, self.native_module_hashes = modules_before, hashes_before
                raise
            else:
                changed = (
                    self.tables != before or self.object_ids != ids_before
                    or self.module_ids != modules_before or self.native_module_hashes != hashes_before
                )
                if changed and self.commit_fault is not None:
                    fault, self.commit_fault = self.commit_fault, None
                    if fault == "rolled_back":
                        self.tables, self.object_ids = before, ids_before
                        self.module_ids, self.native_module_hashes = modules_before, hashes_before
                    if fault == "committed_unreadable":
                        self.reconciliation_unavailable = True
                    if self.after_commit is not None:
                        self.after_commit(self)
                    raise SqlCommitUncertain("Fixture commit acknowledgement was lost")
                if changed and self.after_commit is not None:
                    self.after_commit(self)
            finally:
                self.active = False

    def _object(self, object_id):
        return next(name for name, value in self.object_ids.items() if value == object_id)

    def _content(self, table_name):
        table = next(table for table in self.catalogue.tables if table.name == table_name)
        projected = [
            {column.name: row.get(column.name) for column in table.columns}
            for row in sorted(self.tables[table_name], key=lambda row: tuple(str(row.get(key)) for key in table.primary_key))
        ]
        return len(projected), digest_text(json.dumps(projected, sort_keys=True, separators=(",", ":"), default=jsonable))

    def _hazards(self, logical, kind, sql):
        key = re.search(r"ORDER BY \[(\w+)\]", sql)[1]
        selected = []
        for row in self.tables[self.table_name(logical)]:
            correlation, expiry, workload, workspace, item = None, None, None, None, None
            if kind == "lease":
                expiry, status = row["expires_at"], "held"
                include = expiry > self.clock.now
            elif logical == "agent_runs":
                status = row["state"]
                include = status not in {"completed", "failed"}
            elif logical == "agent_commands":
                status, expiry = row["state"], row["lease_expires_at"]
                include = status not in {"completed", "failed", "interrupted"}
            elif logical == "pipeline_reruns":
                status = row["state"]
                include = status not in {"completed", "failed"}
                correlation = json.loads(row["payload"]).get("rerun_id")
                workload, workspace, item = "fabric_pipeline", row["workspace_id"], row["pipeline_id"]
            elif logical == "retries":
                status = row["status"] or "unknown"
                include = status not in {"completed", "cancelled", "failed"}
            else:
                status = row["status"] or "unknown"
                if kind == "action":
                    include = row["record_kind"] == "action" and status not in {"verified_succeeded", "verified_failed"}
                    payload = json.loads(row["payload"])
                    correlation = (payload.get("submitted_execution") or {}).get("run_id")
                    target = payload.get("request", {}).get("source_execution", {}).get("target", {})
                    workload, workspace, item = target.get("workload"), target.get("workspace_id"), target.get("item_id")
                elif "record_kind='work'" in sql:
                    include = row["record_kind"] == "work" and status not in {"completed", "dispositioned"}
                else:
                    include = row["record_kind"] not in {"action", "work"} and status in {
                        "pending", "running", "reserved", "submitted", "uncertain", "queued", "leased",
                        "waiting", "finalizing", "planned", "provisioning", "deleting",
                    }
            if include:
                text = str(row[key])
                if logical == "monitoring_records":
                    text = f"{row['tenant_id']}:{row['epoch']}:{row['record_kind']}:{text}"
                elif logical == "monitoring_leases":
                    text = f"{row['tenant_id']}:{row['epoch']}:{text}"
                selected.append((digest_text(text), status, expiry, digest_text(correlation) if correlation else None,
                                 workload, workspace, item, correlation))
        return selected[:reset.MAX_HAZARDS + 1]

    def query(self, sql, *params):
        assert self.active, "Every operator query must have a transaction owner"
        self.queries.append((sql, params))
        if self.reconciliation_unavailable:
            raise SqlUnavailable("Fixture SQL remains unavailable")
        if "deployment-authority:identity" in sql:
            return [(1, int(self.permission_metadata_visible), 1, self.operator_principal_id,
                     self.server_identity, self.actual_database, self.database_id, self.clock.now)]
        if "deployment-authority:principals" in sql:
            principals = {
                0: (0, "R", None, False, 1, "public"),
                1: (1, "S", b"\1", False, None, "dbo"),
                self.operator_principal_id: (
                    self.operator_principal_id, "E", UUID(DEPLOYER).bytes_le, False, None, "operator",
                ),
            }
            principals.update({
                pid: (pid, kind, sid, False, None, f"principal_{pid}")
                for pid, kind, sid, _ in self.sql_writer_principals
            })
            principals.update({
                pid: (pid, "R", None, False, 1, name) for name, pid in self.role_ids.items()
            })
            return [principals[key] for key in sorted(principals)]
        if "deployment-authority:roles" in sql:
            return self.extra_role_edges
        if "deployment-authority:permissions" in sql:
            kernel = build_permission_kernel()
            procedure = self.module_ids[unqualified(kernel.rpcs["controller.reserve_action"].object_name)]
            result = [(pid, 1, 1, procedure, 0, "EXECUTE", "G") for pid, *_ in self.sql_writer_principals]
            result.extend((pid, 1, 0, 0, 0, "CONTROL", "G") for pid, _, _, privileged in self.sql_writer_principals if privileged)
            if self.public_write_permissions:
                result.append((0, 1, 3, 1, 0, "INSERT", "G"))
            return result + self.extra_permissions
        if "deployment-authority:schemas" in sql:
            return [(1, "dbo", 1)]
        if "deployment-authority:objects" in sql:
            return [
                (self.object_ids[name], "dbo", name, "U", 1, None, None, 0)
                for name in sorted(self.tables)
            ] + [
                (self.module_ids[item.name], "dbo", item.name,
                 {"procedure": "P", "view": "V", "function": "FN"}[item.kind],
                 1, self.native_module_hashes[item.name], None, 0)
                for item in self.catalogue.modules if item.name in self.module_ids
            ]
        if "deployment-authority:columns" in sql:
            return [
                ("dbo", name, *row[:5], *row[6:], row[5]) for name in sorted(self.tables)
                if name in self.columns for row in self.columns[name]
            ]
        if "deployment-authority:triggers" in sql:
            return self.sql_triggers
        if "deployment-authority:queues" in sql:
            return self.sql_queues
        if "deployment-authority:cascades" in sql:
            return [
                (index + 1, self.object_ids[row[1]], self.object_ids[row[4]], row[6], row[7], row[8])
                for index, row in enumerate(self.fks)
                if row[1] in self.tables and row[4] in self.tables
            ]
        if "monitoring-reset:identity" in sql:
            return [(self.server_identity, self.actual_database, self.database_id, self.clock.now)]
        if "deployment-registry:exists" in sql:
            name = params[0].split(".")[1]
            return [(self.object_ids.get(name) if name in self.tables else None,)]
        if "deployment-registry:latest" in sql:
            return []
        if "monitoring-reset:writer-visibility" in sql:
            return [(int(self.permission_metadata_visible), self.operator_principal_id)]
        if "monitoring-reset:public-writer-permissions" in sql:
            return [(self.public_write_permissions,)]
        if "monitoring-reset:writer-principals" in sql:
            return self.sql_writer_principals
        if "monitoring-reset:registered-targets:" in sql:
            kind = params[0]
            fields = ("tenant_id", "epoch", "workload", "workspace_id", "item_id")
            output = []
            for row in self.tables[self.table_name("monitoring_records")]:
                if row["record_kind"] != kind:
                    continue
                payload = json.loads(row["payload"])
                if kind == "target":
                    identity = payload.get("identity", {})
                elif kind == "inventory":
                    identity = payload
                elif kind == "work":
                    identity = payload.get("target") or {}
                elif kind == "action":
                    identity = payload.get("request", {}).get("source_execution", {}).get("target", {})
                else:
                    identity = payload.get("execution", {}).get("target", {})
                output.append((*[row.get(field) for field in fields],
                               *[identity.get(field) for field in fields], payload.get("kind")))
            return output
        if "monitoring-reset:registered-pipelines" in sql:
            return list({(row["workspace_id"], row["pipeline_id"]) for row in self.tables[self.table_name("pipeline_reruns")]})
        if "monitoring-reset:objects" in sql:
            return [
                ("dbo", name, "U", self.object_ids[name], 1, NOW - timedelta(days=30), NOW - timedelta(days=1), "d" * 64)
                for name in sorted(self.tables)
            ] + [
                ("dbo", item.name, {"procedure": "P", "view": "V", "function": "FN"}[item.kind],
                 self.module_ids[item.name], 1, NOW - timedelta(days=30), NOW - timedelta(days=1),
                 self.native_module_hashes[item.name])
                for item in self.catalogue.modules if item.name in self.module_ids
            ]
        if "monitoring-reset:roles" in sql:
            return [(name, principal, 1, False) for name, principal in self.role_ids.items()]
        if "monitoring-reset:columns" in sql:
            return self.columns[self._object(params[0])]
        if "monitoring-reset:primary-key" in sql:
            table = next(table for table in self.catalogue.tables if table.name == self._object(params[0]))
            return [(column,) for column in table.primary_key]
        if "monitoring-reset:table-safety" in sql:
            return [self.safety[self._object(params[0])]]
        if any(tag in sql for tag in ("monitoring-reset:constraints", "monitoring-reset:indexes")):
            return []
        if "monitoring-reset:foreign-keys" in sql:
            return self.fks
        if "monitoring-reset:control" in sql:
            return [
                tuple(row[name] for name in (
                    "singleton", "schema_version", "tenant_id", "epoch", "revision", "activation_cutoff",
                    "maintenance", "updated_at", "bootstrap_id", "bootstrap_hash", "payload",
                )) for row in self.tables[self.table_name("monitoring_control")]
            ]
        if "monitoring-reset:retained-metadata" in sql:
            return [(row["operation"], row["payload"]) for row in self.tables[self.table_name("monitoring_receipts")]
                    if row["operation"] in params]
        if "monitoring-reset:used-epoch" in sql:
            return [(sum(
                row["epoch"] == params[0] or row["operation"] == params[1] and (
                    json.loads(row["payload"])["expected_old_epoch"] == params[2]
                    or json.loads(row["payload"])["new_control"]["epoch"] == params[3]
                ) for row in self.tables[self.table_name("monitoring_receipts")]
            ),)]
        match = re.search(r"monitoring-reset:(content|retained|tenants|lock):(\w+)", sql)
        if match:
            action, table = match.groups()
            if action == "content":
                return [self._content(table)]
            if action == "retained":
                return [(sum(row["operation"] in params for row in self.tables[table]),)]
            if action == "tenants":
                return [(sum(row["tenant_id"] != params[0] for row in self.tables[table]),)]
            if self.before_exclusive is not None:
                hook, self.before_exclusive = self.before_exclusive, None
                hook(self)
            return [(len(self.tables[table]),)]
        match = re.search(r"monitoring-reset:hazards:(\w+):(\w+)", sql)
        if match:
            return self._hazards(*match.groups(), sql)
        if "monitoring-reset:receipt-table" in sql:
            name = params[0].split(".")[1]
            return [(self.object_ids[name] if name in self.tables else None,)]
        if "monitoring-reset:receipt" in sql:
            return [
                (row["fingerprint"], row["payload"]) for row in self.tables[self.table_name("monitoring_receipts")]
                if (row["tenant_id"], row["operation"], row["request_hash"]) == params
            ]
        if sql.startswith("SELECT OBJECT_ID("):
            if self.fail_bootstrap:
                raise SqlUnavailable("Fixture bootstrap verification failed")
            return [tuple(self.object_ids.get(value.split(".")[1]) if value.split(".")[1] in self.tables else None for value in params)]
        if sql.startswith("SELECT bootstrap_id, bootstrap_hash, payload"):
            return [(row["bootstrap_id"], row["bootstrap_hash"], row["payload"])
                    for row in self.tables[self.table_name("monitoring_control")]]
        if sql.startswith("SELECT COUNT(*)"):
            name = re.search(r"\[dbo\]\.\[(\w+)\]", sql)[1]
            return [(len(self.tables[name]),)]
        raise AssertionError(f"Unimplemented fake SQL read: {sql[:100]}")

    def execute(self, sql, *params):
        assert self.active, "Every operator write must have a transaction owner"
        self.statements.append((sql, params))
        delete = re.search(r"monitoring-reset:delete:(\w+)", sql)
        if delete:
            table = delete[1]
            assert table in self.catalogue.table_names
            self.delete_calls += 1
            if self.fail_delete_number == self.delete_calls:
                raise SqlUnavailable("Fixture failure during multi-table deletion")
            for edge in self.catalogue.foreign_keys:
                if edge.parent == table and self.tables[edge.child]:
                    raise SqlUnavailable("Fixture foreign-key restriction")
            count = len(self.tables[table])
            self.tables[table] = []
            return count
        if "monitoring-reset:delete-receipts" in sql:
            rows = self.tables[self.table_name("monitoring_receipts")]
            kept = [row for row in rows if row["operation"] in params]
            self.tables[self.table_name("monitoring_receipts")] = kept
            return len(rows) - len(kept)
        if "monitoring-reset:replace-control" in sql:
            old = self.tables[self.table_name("monitoring_control")][0]
            if (
                old["tenant_id"], old["epoch"], old["revision"], old["schema_version"],
                old["bootstrap_id"], old["bootstrap_hash"],
            ) != params[10:] or not old["maintenance"]:
                return 0
            self.put_control(m.DeploymentControl.model_validate_json(params[9]), params[7], bootstrap_hash=params[8])
            return 1
        if "monitoring-reset:insert-receipt" in sql:
            if self.fail_receipt_insert:
                raise SqlUnavailable("Fixture failure before receipt persistence")
            self.add("monitoring_receipts", **dict(zip(
                ("tenant_id", "epoch", "operation", "request_hash", "request_id", "fingerprint", "recorded_at", "payload"),
                params, strict=True,
            )))
            return 1
        if sql.lstrip().startswith(("IF OBJECT_ID", "IF NOT EXISTS")):
            parsed = reset._table_body(sql)
            if parsed is not None and parsed[0] not in self.tables:
                name = parsed[0]
                assert name in set(reset.monitoring_schema.DEFAULT_MONITORING_TABLES.values()) | {reset.DEFAULT_RATE_TABLE}
                self.tables[name] = []
                self.object_ids[name] = max(self.object_ids.values()) + 1
            return 0
        if sql.startswith(f"INSERT INTO [dbo].[{self.table_name('monitoring_control')}]"):
            assert not self.tables[self.table_name("monitoring_control")]
            self.put_control(m.DeploymentControl.model_validate_json(params[-1]), params[-3], bootstrap_hash=params[-2])
            return 1
        raise AssertionError(f"Unimplemented fake SQL write: {sql[:100]}")


def profile(**changes):
    return reset.ObservationProfile.model_validate({
        "target": TARGET,
        "writers": [{"writer_id": "api", "kind": "app_service", "resource_id": SITE}],
        "action_targets": [{"workload": "fabric_pipeline", "workspace_id": WORKSPACE, "item_id": ITEM}],
        **changes,
    })


class SqlFixtureDeploymentReader:
    """Reads fixture deployment/SQL registration, never the requested profile."""

    def read(self, database, target, catalogue):
        assert database.active
        authority = read_authority(database, target, catalogue)
        return reset.DeploymentWriterInventory(
            binding_id="fixture-deployment-sql-registration", revision=1, target=target,
            server_identity=database.server_identity, database_id=database.database_id,
            catalogue_hash=catalogue.declaration_hash, observed_at=database.clock.now,
            writers=database.deployment_bindings,
            kernel_contract_hash=authority.kernel_hash, authority_snapshot_hash=authority.snapshot_hash,
            registration_request_id=str(UUID(int=501)), discovery_capture_hash="d" * 64,
            current_binding_hash="b" * 64, resource_observed_at=database.clock.now,
        )


def identity_document():
    return {"type": "UserAssigned", "userAssignedIdentities": {IDENTITY_RESOURCE: {}}}


def settings_document(server=TARGET.server, database=TARGET.database, client=RUNTIME_CLIENT):
    return {
        "AZURE_SQL_SERVER": server, "AZURE_SQL_DATABASE": database,
        "MONITORING_TENANT_ID": TENANT, "AZURE_CLIENT_ID": client,
    }


class LiveSources:
    def __init__(self):
        self.site_state = "Stopped"
        self.agent_state = "Disabled"
        self.history = []
        self.powerbi_history = []
        self.run_status = "Completed"
        self.requests = []
        self.on_read = None
        self.sql_server = TARGET.server
        self.sql_database = TARGET.database
        self.identity_client = RUNTIME_CLIENT
        self.identity_object = RUNTIME_OBJECT

    def handle(self, request):
        assert request.method == "GET" or (
            request.method == "POST" and request.url.path.endswith("/config/appsettings/list")
        ), "The observer can POST only the read-only ARM settings-list action"
        self.requests.append(request)
        if self.on_read:
            self.on_read(request)
        path = request.url.path
        if path == f"/subscriptions/{SUBSCRIPTION}":
            return httpx.Response(200, json={"subscriptionId": SUBSCRIPTION, "tenantId": TENANT, "state": "Enabled"})
        if path.startswith("/v1.0/servicePrincipals"):
            return httpx.Response(200, json={
                "appId": self.identity_client, "id": self.identity_object,
                "servicePrincipalType": "ManagedIdentity", "appOwnerOrganizationId": TENANT,
                "passwordCredentials": [], "keyCredentials": [],
            })
        if path == IDENTITY_RESOURCE:
            return httpx.Response(200, json={"id": IDENTITY_RESOURCE, "properties": {
                "tenantId": TENANT, "clientId": self.identity_client, "principalId": self.identity_object,
            }})
        if path == SITE:
            return httpx.Response(200, json={"id": SITE, "identity": identity_document(), "properties": {"state": self.site_state}})
        if path == SITE + "/config/appsettings/list":
            return httpx.Response(200, json={"properties": settings_document(self.sql_server, self.sql_database, self.identity_client)})
        if path == SITE + "/slots":
            return httpx.Response(200, json={"value": []})
        if path.endswith("/versions") and path.startswith("/api/projects/fixture/agents/"):
            return httpx.Response(200, json={"data": [{"version": "1", "definition": {
                "kind": "hosted", "environment_variables": settings_document(self.sql_server, self.sql_database),
            }}], "has_more": False})
        if path.startswith("/api/projects/fixture/agents/"):
            return httpx.Response(200, json={
                "name": path.rsplit("/", 1)[1], "status": self.agent_state,
                "instance_identity": {"client_id": self.identity_client, "principal_id": self.identity_object},
            })
        if path.endswith("/jobs/instances"):
            return httpx.Response(200, json={"value": self.history})
        if path.endswith(f"/jobs/instances/{RUN}"):
            return httpx.Response(200, json={
                "id": RUN, "itemId": ITEM, "status": self.run_status, "endTimeUtc": NOW.isoformat(),
            })
        if path.endswith("/refreshes"):
            return httpx.Response(200, json={"value": self.powerbi_history})
        pytest.fail(f"Unexpected mock observer route: {path}")


def prepare(db=None, sources=None):
    db = db or TransactionalSqlFake()
    sources = sources or LiveSources()
    observer = reset.LiveOperatorObserver(
        profile(), db._credential, transport=httpx.MockTransport(sources.handle), clock=lambda: db.clock.now,
    )
    operator = reset.SqlResetOperator(
        db, TARGET, observer=observer, deployment_inventory_reader=SqlFixtureDeploymentReader(),
    )
    return db, operator, operator.plan(), observer, sources


def execute(operator, manifest, **changes):
    return operator.execute(manifest, **{
        "confirmed_manifest_hash": manifest.manifest_hash,
        "expected_epoch": manifest.snapshot.control.epoch if manifest.snapshot.control else OLD_EPOCH,
        **changes,
    })


def initialize(operator, manifest, **changes):
    return operator.initialize(manifest, **{
        "confirmed_manifest_hash": manifest.manifest_hash, "expected_uninitialized": True, **changes,
    })


def test_default_plan_is_read_only_exact_and_contains_no_private_payload():
    db = TransactionalSqlFake()
    before = copy.deepcopy(db.tables)
    manifest = reset.SqlResetOperator(db, TARGET).plan()
    assert db.tables == before and not db.statements
    assert len(manifest.snapshot.objects) == (
        len(db.catalogue.tables) + len(db.catalogue.modules) + len(db.catalogue.roles)
    )
    assert manifest.snapshot.control.epoch == OLD_EPOCH
    assert PRIVATE_TEXT not in manifest.model_dump_json() and "payload" not in manifest.model_dump_json()
    assert len(manifest.manifest_hash) == 64
    order = [table.logical_name for table in reset.build_catalogue().deletion_order()]
    assert order.index("agent_events") < order.index("agent_runs")


def test_single_operator_with_actual_observed_state_can_reset():
    db, operator, manifest, observer, sources = prepare()
    unrelated, budget = copy.deepcopy(db.tables["business_orders"]), copy.deepcopy(db.tables[db.table_name("rate_budget")])
    result = execute(operator, manifest)
    assert result.receipt.operator_object_id == DEPLOYER
    assert result.receipt.new_control.epoch == manifest.new_epoch and result.receipt.new_control.maintenance
    assert len(sources.requests) == 14  # Two fresh subscription/site/identity/settings/slots/history observations.
    assert db.tables["business_orders"] == unrelated and db.tables[db.table_name("rate_budget")] == budget
    assert all(not db.tables[table.name] for table in operator.catalogue.deletion_order())
    assert PRIVATE_TEXT not in result.model_dump_json()
    observer.close()


def test_profile_cannot_omit_sql_registered_active_target_and_substitute_unrelated_stopped_writer():
    db = TransactionalSqlFake()
    active_model = str(UUID(int=900))
    db.add(
        "monitoring_records", tenant_id=TENANT, epoch=OLD_EPOCH, record_kind="target",
        full_key="registered-powerbi-target", workload="powerbi", workspace_id=WORKSPACE,
        item_id=active_model, status="current",
        payload=json.dumps({
            "identity": {"tenant_id": TENANT, "epoch": OLD_EPOCH, "workload": "powerbi",
                         "workspace_id": WORKSPACE, "item_id": active_model},
        }),
    )
    sources = LiveSources()
    original = sources.handle

    def handler(request):
        if request.url.path == SITE:
            sources.requests.append(request)
            return httpx.Response(200, json={
                "id": SITE, "properties": {"state": "Stopped", "siteConfig": {"appSettings": [
                    {"name": "AZURE_SQL_SERVER", "value": "different.database.windows.net"},
                    {"name": "AZURE_SQL_DATABASE", "value": "different-database"},
                ]}},
            })
        if request.url.path.endswith(f"/datasets/{active_model}/refreshes"):
            sources.requests.append(request)
            return httpx.Response(200, json={"value": [{"requestId": RUN, "status": "Unknown", "endTime": None}]})
        return original(request)

    observer = reset.LiveOperatorObserver(
        profile(), db._credential, transport=httpx.MockTransport(handler), clock=lambda: db.clock.now,
    )
    operator = reset.SqlResetOperator(
        db, TARGET, observer=observer, deployment_inventory_reader=SqlFixtureDeploymentReader(),
    )
    manifest = operator.plan()
    before = copy.deepcopy(db.tables)
    with pytest.raises(reset.ResetRefused, match="SQL-registered"):
        execute(operator, manifest)
    assert db.delete_calls == 0 and db.tables == before


@pytest.mark.parametrize("field", ["sql_server", "sql_database"])
def test_stopped_writer_must_actually_bind_to_the_database_being_reset(field):
    db, operator, manifest, _, sources = prepare()
    setattr(sources, field, "different.database.windows.net" if field == "sql_server" else "different-database")
    with pytest.raises(reset.ResetRefused, match="SQL settings"):
        execute(operator, manifest)
    assert any(request.method == "POST" and request.url.path.endswith("/appsettings/list") for request in sources.requests)
    assert db.delete_calls == 0


@pytest.mark.parametrize("field", ["identity_client", "identity_object"])
def test_stopped_writer_identity_must_match_authoritative_sql_binding(field):
    db, operator, manifest, _, sources = prepare()
    setattr(sources, field, str(UUID(int=999)))
    with pytest.raises(reset.ResetRefused, match="identity"):
        execute(operator, manifest)
    assert db.delete_calls == 0


def test_including_the_actual_registered_target_reads_and_refuses_its_active_refresh():
    db = TransactionalSqlFake()
    active_model = str(UUID(int=900))
    db.add(
        "monitoring_records", tenant_id=TENANT, epoch=OLD_EPOCH, record_kind="target",
        full_key="powerbi-target", workload="powerbi", workspace_id=WORKSPACE, item_id=active_model, status="current",
        payload=json.dumps({"identity": {
            "tenant_id": TENANT, "epoch": OLD_EPOCH, "workload": "powerbi",
            "workspace_id": WORKSPACE, "item_id": active_model,
        }}),
    )
    configuration = profile(action_targets=[
        {"workload": "fabric_pipeline", "workspace_id": WORKSPACE, "item_id": ITEM},
        {"workload": "powerbi", "workspace_id": WORKSPACE, "item_id": active_model},
    ])
    sources = LiveSources()
    sources.powerbi_history = [{"requestId": RUN, "status": "Unknown", "endTime": None}]
    observer = reset.LiveOperatorObserver(
        configuration, db._credential, transport=httpx.MockTransport(sources.handle), clock=lambda: db.clock.now,
    )
    operator = reset.SqlResetOperator(
        db, TARGET, observer=observer, deployment_inventory_reader=SqlFixtureDeploymentReader(),
    )
    with pytest.raises(reset.ResetRefused, match="active or unverified refresh"):
        execute(operator, operator.plan())
    assert any(f"/datasets/{active_model}/refreshes" in request.url.path for request in sources.requests)
    assert db.delete_calls == 0


def test_profile_cannot_omit_an_authoritatively_registered_writer_using_the_same_identity():
    db = TransactionalSqlFake()
    db.deployment_bindings += (reset.WriterBinding(
        writer=reset.WriterSpec(writer_id="another-writer", kind="app_service", resource_id=SITE + "-other"),
        identity_client_id=RUNTIME_CLIENT, identity_object_id=RUNTIME_OBJECT, sql_principal_id=11,
    ),)
    db, operator, manifest, _, sources = prepare(db)
    with pytest.raises(reset.ResetRefused, match="authoritative deployed writer"):
        execute(operator, manifest)
    assert db.delete_calls == 0 and sources.requests == []


def test_missing_authoritative_deployment_reader_fails_closed_instead_of_trusting_profile():
    db, _, _, observer, sources = prepare()
    operator = reset.SqlResetOperator(db, TARGET, observer=observer)
    with pytest.raises(reset.ResetRefused, match="deployment_inventory_reader"):
        execute(operator, operator.plan())
    assert db.delete_calls == 0 and sources.requests == []
    manifest = operator.plan()
    document = reset.ManifestDocument(manifest_hash=manifest.manifest_hash, manifest=manifest)
    assert "authoritative_deployment_sql_writer_inventory_unavailable" in document.reset_execution_blockers


@pytest.mark.parametrize("problem", ["missing_principal", "wrong_sid", "group", "privileged", "metadata_hidden", "public_write"])
def test_sql_permission_inventory_must_agree_with_registered_deployment(problem):
    db = TransactionalSqlFake()
    if problem == "missing_principal":
        db.sql_writer_principals.append((12, "E", UUID(int=99).bytes_le, 0))
    elif problem == "wrong_sid":
        db.sql_writer_principals = [(11, "E", UUID(int=99).bytes_le, 0)]
    elif problem == "group":
        db.sql_writer_principals = [(11, "X", UUID(RUNTIME_CLIENT).bytes_le, 0)]
    elif problem == "privileged":
        db.sql_writer_principals = [(11, "E", UUID(RUNTIME_CLIENT).bytes_le, 1)]
    elif problem == "metadata_hidden":
        db.permission_metadata_visible = False
    else:
        db.public_write_permissions = 1
    _, operator, manifest, _, _ = prepare(db)
    assert manifest.snapshot.blockers
    with pytest.raises(reset.ResetRefused, match="blocked"):
        execute(operator, manifest)
    assert db.delete_calls == 0


def test_explicit_operator_principal_is_not_mistaken_for_an_application_writer():
    db = TransactionalSqlFake()
    db.sql_writer_principals.append((db.operator_principal_id, "E", UUID(CLIENT).bytes_le, 1))
    _, operator, manifest, _, _ = prepare(db)
    assert execute(operator, manifest).receipt.operator_object_id == DEPLOYER


@pytest.mark.parametrize("state", ["Running", "", None])
def test_actual_running_or_unknown_writer_state_refuses_reset(state):
    db, operator, manifest, _, sources = prepare()
    sources.site_state = state
    with pytest.raises(reset.ResetRefused, match="not stopped"):
        execute(operator, manifest)
    assert db.delete_calls == 0


def test_writer_restarting_during_reset_rolls_back_all_rows():
    db, operator, manifest, _, sources = prepare()
    before = copy.deepcopy(db.tables)
    sources.on_read = lambda _: setattr(sources, "site_state", "Running") if db.delete_calls else None
    with pytest.raises(reset.ResetRefused, match="not stopped"):
        execute(operator, manifest)
    assert db.tables == before


@pytest.mark.parametrize("tamper", ["challenge", "manifest_hash", "ownership_hash", "time", "writers", "state", "boolean"])
def test_forged_or_stale_live_observations_fail(tamper):
    db, operator, manifest, observer, _ = prepare()
    read = observer.observe

    def forged(request):
        result = read(request)
        if tamper == "boolean":
            return {"quiesced": True}
        if tamper == "state":
            return result.model_copy(update={"writers": (result.writers[0].model_copy(update={"state": "running"}),)})
        changes = {
            "challenge": {"challenge": str(UUID(int=999))},
            "manifest_hash": {"manifest_hash": "f" * 64},
            "ownership_hash": {"ownership_hash": "f" * 64},
            "time": {"started_at": NOW - timedelta(minutes=10)},
            "writers": {"writers": ()},
        }[tamper]
        return result.model_copy(update=changes)

    observer.observe = forged
    with pytest.raises(reset.ResetRefused):
        execute(operator, manifest)
    assert db.delete_calls == 0


def test_foundry_observer_requires_disabled_endpoint_not_idle_version_status():
    db = TransactionalSqlFake()
    configuration = profile(writers=[{
        "writer_id": "controller", "kind": "foundry_agent",
        "project_endpoint": "https://fixture.services.ai.azure.com/api/projects/fixture", "agent_name": "controller",
    }])
    db.deployment_bindings = (reset.WriterBinding(
        writer=configuration.writers[0], identity_client_id=RUNTIME_CLIENT,
        identity_object_id=RUNTIME_OBJECT, sql_principal_id=11,
    ),)
    sources = LiveSources()
    observer = reset.LiveOperatorObserver(
        configuration, db._credential, transport=httpx.MockTransport(sources.handle), clock=lambda: db.clock.now,
    )
    operator = reset.SqlResetOperator(
        db, TARGET, observer=observer, deployment_inventory_reader=SqlFixtureDeploymentReader(),
    )
    manifest = operator.plan()
    sources.agent_state = "Enabled"
    with pytest.raises(reset.ResetRefused, match="not observably Disabled"):
        execute(operator, manifest)
    assert not db.delete_calls
    sources.agent_state = "Disabled"
    assert execute(operator, manifest).receipt.operator_object_id == DEPLOYER


@pytest.mark.parametrize("kind", ["logic_app", "container_app", "container_app_job"])
def test_live_arm_observers_check_executions_revisions_and_triggers(kind):
    db = TransactionalSqlFake()
    suffix = {
        "logic_app": "Microsoft.Logic/workflows/timer",
        "container_app": "Microsoft.App/containerApps/worker",
        "container_app_job": "Microsoft.App/jobs/canary",
    }[kind]
    resource = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/fixture/providers/{suffix}"
    configuration = profile(writers=[{"writer_id": kind, "kind": kind, "resource_id": resource}])
    db.deployment_bindings = (reset.WriterBinding(
        writer=configuration.writers[0], identity_client_id=RUNTIME_CLIENT,
        identity_object_id=RUNTIME_OBJECT, sql_principal_id=11,
    ),)
    calls = []
    template = {"containers": [{"name": "worker", "env": [
        {"name": key, "value": value} for key, value in settings_document().items()
    ]}]}

    def handler(request):
        assert request.method == "GET"
        calls.append(request)
        path = request.url.path
        if path == f"/subscriptions/{SUBSCRIPTION}":
            return httpx.Response(200, json={"subscriptionId": SUBSCRIPTION, "tenantId": TENANT, "state": "Enabled"})
        if path.startswith("/v1.0/servicePrincipals"):
            return httpx.Response(200, json={
                "appId": RUNTIME_CLIENT, "id": RUNTIME_OBJECT, "servicePrincipalType": "ManagedIdentity",
                "appOwnerOrganizationId": TENANT, "passwordCredentials": [], "keyCredentials": [],
            })
        if path == IDENTITY_RESOURCE:
            return httpx.Response(200, json={"id": IDENTITY_RESOURCE, "properties": {
                "tenantId": TENANT, "clientId": RUNTIME_CLIENT, "principalId": RUNTIME_OBJECT,
            }})
        if path == resource:
            properties = {"state": "Disabled"} if kind == "logic_app" else {
                "configuration": {"triggerType": "Manual"}, "template": template,
            }
            return httpx.Response(200, json={"id": resource, "properties": properties, "identity": identity_document()})
        if path == resource + "/revisions":
            return httpx.Response(200, json={"value": [{
                "id": resource + "/revisions/version-one", "properties": {"active": False},
            }]})
        if path == resource + "/revisions/version-one":
            return httpx.Response(200, json={"id": path, "properties": {"active": False, "template": template}})
        return httpx.Response(200, json={"value": []})

    observer = reset.LiveOperatorObserver(
        configuration, db._credential, transport=httpx.MockTransport(handler), clock=lambda: db.clock.now,
    )
    operator = reset.SqlResetOperator(
        db, TARGET, observer=observer, deployment_inventory_reader=SqlFixtureDeploymentReader(),
    )
    if kind == "logic_app":
        with pytest.raises(reset.ResetRefused, match="Workflow actions"):
            execute(operator, operator.plan())
        assert db.delete_calls == 0
    else:
        assert execute(operator, operator.plan()).receipt.state == "completed"
        assert len(calls) >= 6


def test_saved_observations_and_caller_boolean_are_not_an_observer():
    db = TransactionalSqlFake()
    with pytest.raises(reset.ResetRefused, match="trusted live observer"):
        reset.SqlResetOperator(db, TARGET, observer={"quiesced": True})
    with pytest.raises(ValidationError):
        reset.ObservationProfile.model_validate({**profile().model_dump(), "writers_stopped": True})
    operator = reset.SqlResetOperator(db, TARGET)
    with pytest.raises(reset.ResetRefused, match="trusted live observer"):
        execute(operator, operator.plan())
    assert db.delete_calls == 0


@pytest.mark.parametrize("mismatch", ["tenant", "database", "configured_database"])
def test_wrong_target_cannot_delete_state(mismatch):
    db = TransactionalSqlFake()
    if mismatch == "tenant":
        db.put_control(db.control_model.model_copy(update={"tenant_id": str(UUID(int=90))}), OLD_BOOTSTRAP)
    elif mismatch == "database":
        db.actual_database = "other_database"
    else:
        db._database = "other_database"
    with pytest.raises(reset.ResetRefused):
        reset.SqlResetOperator(db, TARGET).plan()
    assert not db.statements


@pytest.mark.parametrize("mode", ["count", "payload", "database_identity", "epoch"])
def test_changed_manifest_is_rejected(mode):
    db, operator, manifest, _, _ = prepare()
    if mode == "count":
        db.add("incidents", incident_id="new-row")
    elif mode == "payload":
        db.tables[db.table_name("incidents")][0]["payload"] = "changed same-count row"
    elif mode == "database_identity":
        db.database_id += 1
    else:
        db.put_control(db.control_model.model_copy(update={"epoch": str(UUID(int=91))}), OLD_BOOTSTRAP)
    before = copy.deepcopy(db.tables)
    with pytest.raises(reset.ResetRefused):
        execute(operator, manifest)
    assert db.tables == before and db.delete_calls == 0


@pytest.mark.parametrize("change", ["object_id", "new_table", "database"])
def test_metadata_is_rechecked_after_exclusive_locks(change):
    db, operator, manifest, _, _ = prepare()

    def mutate(fixture):
        if change == "object_id":
            fixture.object_ids[fixture.table_name("incidents")] += 1000
        elif change == "new_table":
            fixture.tables["triage_unregistered_state"] = []
            fixture.object_ids["triage_unregistered_state"] = 9999
        else:
            fixture.actual_database = "other_database"

    db.before_exclusive = mutate
    with pytest.raises(reset.ResetRefused):
        execute(operator, manifest)
    assert db.delete_calls == 0


@pytest.mark.parametrize("problem", ["missing", "columns", "trigger", "cascade"])
def test_unsafe_schema_is_a_blocked_reset_plan(problem):
    db = TransactionalSqlFake()
    if problem == "missing":
        del db.tables[db.table_name("claims")]
    elif problem == "columns":
        db.columns[db.table_name("incidents")] = db.columns[db.table_name("incidents")][:-1]
    elif problem == "trigger":
        db.safety[db.table_name("incidents")] = (0, False, False, 1)
    else:
        db.fks.append(("dbo", "business_orders", "order_id", "dbo", db.table_name("incidents"), "incident_id", 1, 0, False))
    _, operator, manifest, _, _ = prepare(db)
    assert manifest.snapshot.blockers
    with pytest.raises(reset.ResetRefused, match="blocked"):
        execute(operator, manifest)
    assert not db.statements


def test_control_must_be_in_maintenance_for_reset():
    db = TransactionalSqlFake()
    db.put_control(db.control_model.model_copy(update={"maintenance": False}), OLD_BOOTSTRAP)
    _, operator, manifest, _, _ = prepare(db)
    with pytest.raises(reset.ResetRefused, match="blocked"):
        execute(operator, manifest)
    assert not db.statements


@pytest.mark.parametrize("kind", ["lease", "command", "run", "uncorrelated_action"])
def test_unresolved_work_or_effects_cannot_be_overridden(kind):
    db = TransactionalSqlFake()
    if kind == "lease":
        db.add("claims", claim_key="claim", expires_at=NOW + timedelta(minutes=1))
    elif kind == "command":
        db.add("agent_commands", command_id="command", state="running")
    elif kind == "run":
        db.add("agent_runs", run_id="active", state="running")
    else:
        db.add("pipeline_reruns", run_key="pending", state="unknown", workspace_id=WORKSPACE,
               pipeline_id=ITEM, payload=json.dumps({"rerun_id": ""}))
    _, operator, manifest, _, _ = prepare(db)
    with pytest.raises(reset.ResetRefused, match="Drain"):
        execute(operator, manifest)
    assert db.delete_calls == 0


@pytest.mark.parametrize("state,allowed", [("Completed", True), ("Failed", True), ("InProgress", False)])
def test_exact_external_action_is_re_read_instead_of_accepting_an_attestation(state, allowed):
    db = TransactionalSqlFake()
    db.add("pipeline_reruns", run_key="old-rerun", state="unknown", workspace_id=WORKSPACE,
           pipeline_id=ITEM, payload=json.dumps({"rerun_id": RUN}))
    _, operator, manifest, _, sources = prepare(db)
    sources.run_status = state
    if allowed:
        assert execute(operator, manifest).receipt.state == "completed"
        assert sum(request.url.path.endswith(RUN) for request in sources.requests) == 2
    else:
        with pytest.raises(reset.ResetRefused, match="not terminal"):
            execute(operator, manifest)
        assert db.delete_calls == 0


def test_live_external_history_must_be_idle():
    db, operator, manifest, _, sources = prepare()
    sources.history = [{"id": RUN, "itemId": ITEM, "status": "InProgress"}]
    with pytest.raises(reset.ResetRefused, match="active or unverified"):
        execute(operator, manifest)
    assert db.delete_calls == 0


@pytest.mark.parametrize("failure", ["middle_delete", "receipt_insert"])
def test_partial_reset_rolls_back_every_owned_row_and_epoch(failure):
    db, operator, manifest, _, _ = prepare()
    before = copy.deepcopy(db.tables)
    if failure == "middle_delete":
        db.fail_delete_number = 4
    else:
        db.fail_receipt_insert = True
    with pytest.raises(SqlUnavailable):
        execute(operator, manifest)
    assert db.tables == before


@pytest.mark.parametrize("fault", ["committed", "rolled_back", "committed_unreadable"])
def test_ambiguous_commit_never_blindly_repeats_deletion(fault):
    db, operator, manifest, _, _ = prepare()
    before = copy.deepcopy(db.tables)
    db.commit_fault = fault
    if fault == "committed":
        assert execute(operator, manifest).reconciled_uncertain_commit
    else:
        with pytest.raises(reset.ResetCommitUncertain):
            execute(operator, manifest)
    calls = db.delete_calls
    if fault == "rolled_back":
        assert db.tables == before
    else:
        db.reconciliation_unavailable = False
        assert execute(operator, manifest).replayed
        assert db.delete_calls == calls


def test_repeat_protects_new_release_rows_without_reobserving_or_ddl():
    db, operator, manifest, _, sources = prepare()
    first = execute(operator, manifest)
    db.add("incidents", incident_id="new-release", payload="must survive")
    db.clock.advance(900)
    sources.site_state = "Running"
    before, statements, reads = copy.deepcopy(db.tables), len(db.statements), len(sources.requests)
    result = execute(operator, manifest)
    assert result.replayed and result.receipt == first.receipt
    assert db.tables == before and len(db.statements) == statements and len(sources.requests) == reads


def test_wrong_confirmation_and_new_epoch_without_receipt_are_protected():
    db, operator, manifest, _, _ = prepare()
    for changes in ({"expected_epoch": str(UUID(int=90))}, {"confirmed_manifest_hash": "f" * 64}):
        with pytest.raises(reset.ResetRefused, match="explicitly confirmed"):
            execute(operator, manifest, **changes)
    db.put_control(db.control_model.model_copy(update={"epoch": manifest.new_epoch, "revision": 0}), manifest.operation_id)
    db.add("incidents", incident_id="new-release", payload="retain this")
    before = copy.deepcopy(db.tables)
    with pytest.raises(reset.ResetRefused):
        execute(operator, manifest)
    assert db.tables == before and db.delete_calls == 0


def test_post_commit_bootstrap_verification_failure_retains_committed_disposition():
    db, operator, manifest, _, _ = prepare()
    db.fail_bootstrap = True
    with pytest.raises(reset.ResetVerificationFailed) as error:
        execute(operator, manifest)
    assert error.value.receipt.new_control.epoch == manifest.new_epoch
    calls = db.delete_calls
    db.fail_bootstrap = False
    assert execute(operator, manifest).replayed and db.delete_calls == calls


def test_explicit_initialization_creates_empty_maintenance_baseline_without_copying_or_deleting_history():
    db = TransactionalSqlFake(initialized=False)
    original = copy.deepcopy(db.tables)
    operator = reset.SqlResetOperator(db, TARGET)
    manifest = operator.plan_initialization()
    assert manifest.purpose == "initialize" and manifest.snapshot.control is None and not db.statements
    result = initialize(operator, manifest)
    assert result.receipt.control.maintenance and result.receipt.control.revision == 0
    assert all(db.tables[name] == rows for name, rows in original.items())
    assert db.tables[db.table_name("monitoring_records")] == []
    assert db.tables[db.table_name("monitoring_leases")] == []
    assert len(db.tables[db.table_name("monitoring_receipts")]) == 1
    assert db.delete_calls == 0
    db.add("monitoring_records", tenant_id=TENANT, epoch=manifest.new_epoch, record_kind="scope", full_key="new", status="active")
    statements, before = len(db.statements), copy.deepcopy(db.tables)
    assert initialize(operator, manifest).replayed
    assert len(db.statements) == statements and db.tables == before


def test_initialization_is_not_a_reset_or_silent_missing_schema_fallback():
    db = TransactionalSqlFake(initialized=False)
    operator = reset.SqlResetOperator(db, TARGET)
    reset_plan, init_plan = operator.plan(), operator.plan_initialization()
    with pytest.raises(reset.ResetRefused):
        execute(operator, reset_plan, expected_epoch=OLD_EPOCH)
    with pytest.raises(reset.ResetRefused, match="explicit"):
        initialize(operator, init_plan, expected_uninitialized=False)
    with pytest.raises(reset.ResetRefused):
        initialize(operator, reset_plan)
    assert not db.statements


@pytest.mark.parametrize("problem", ["old_schema_missing", "new_state_present", "wrong_epoch"])
def test_initialization_refuses_adopting_or_upgrading_state(problem):
    db = TransactionalSqlFake(initialized=False)
    operator = reset.SqlResetOperator(db, TARGET)
    manifest = operator.plan_initialization()
    if problem == "old_schema_missing":
        del db.tables[db.table_name("claims")]
    elif problem == "new_state_present":
        db.tables[db.table_name("monitoring_records")] = []
        db.add("monitoring_records", tenant_id=TENANT, epoch=OLD_EPOCH, record_kind="work", full_key="old", status="queued")
    else:
        db.put_control(db.control_model, OLD_BOOTSTRAP)
    before = copy.deepcopy(db.tables)
    with pytest.raises(reset.ResetRefused):
        initialize(operator, manifest)
    assert db.tables == before and db.delete_calls == 0


@pytest.mark.parametrize("fault", ["committed", "rolled_back"])
def test_initial_bootstrap_ambiguous_commit_is_reconciled_without_data_loss(fault):
    db = TransactionalSqlFake(initialized=False)
    operator = reset.SqlResetOperator(db, TARGET)
    manifest = operator.plan_initialization()
    old = copy.deepcopy(db.tables)
    db.commit_fault = fault
    if fault == "committed":
        assert initialize(operator, manifest).receipt.control.epoch == manifest.new_epoch
    else:
        with pytest.raises(reset.ResetCommitUncertain):
            initialize(operator, manifest)
    assert all(db.tables[name] == rows for name, rows in old.items())
    assert db.delete_calls == 0


def cli_args():
    return [
        "--server", TARGET.server, "--database", TARGET.database, "--tenant-id", TENANT,
        "--deployer-object-id", DEPLOYER, "--credential", "azure-cli", "--subscription-id", SUBSCRIPTION,
    ]


def test_cli_default_no_write_even_with_exported_credentials(monkeypatch, capsys):
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "fixture-environment-must-not-be-used")
    monkeypatch.setenv("MONITORING_RESET_EXECUTE", "true")
    db = TransactionalSqlFake()
    assert reset.main(cli_args(), database_factory=lambda *_: db) == 0
    document = reset.ManifestDocument.model_validate_json(capsys.readouterr().out)
    assert any("registration is absent" in blocker for blocker in document.reset_execution_blockers)
    assert not db.statements


def test_cli_explicit_modes_and_no_signing_inputs(tmp_path):
    def forbidden(*_):
        pytest.fail("Bad/default arguments must not open SQL")

    with pytest.raises(SystemExit):
        reset.main([], database_factory=forbidden)
    with pytest.raises(SystemExit):
        reset.main([*cli_args(), "--execute"], database_factory=forbidden)
    with pytest.raises(SystemExit):
        reset.main([*cli_args(), "--trust-policy", "unused.json"], database_factory=forbidden)
    output = tmp_path / "manifest.json"
    output.write_text("keep original")
    assert reset.main([*cli_args(), "--output", str(output)], database_factory=forbidden) == 1
    assert output.read_text() == "keep original"


def test_cli_first_deployment_then_single_operator_reset(tmp_path):
    db = TransactionalSqlFake(initialized=False)
    init_path, reset_path, bindings = tmp_path / "initialize.json", tmp_path / "reset.json", tmp_path / "bindings.json"
    def factory(*_):
        return db
    assert reset.main(
        [*cli_args(), "--plan-initialization", "--output", str(init_path)], database_factory=factory,
    ) == 0
    initial = reset.ManifestDocument.model_validate_json(init_path.read_text())
    assert reset.main([
        *cli_args(), "--initialize", "--manifest", str(init_path),
        "--confirm-manifest-hash", initial.manifest_hash, "--expected-uninitialized",
    ], database_factory=factory) == 0
    sources = LiveSources()
    bindings.write_text(profile().model_dump_json())

    def observer_factory(configuration, credential):
        return reset.LiveOperatorObserver(
            configuration, credential, transport=httpx.MockTransport(sources.handle), clock=lambda: db.clock.now,
        )

    def inventory_reader_factory(*_):
        return SqlFixtureDeploymentReader()

    assert reset.main([
        *cli_args(), "--preflight-config", str(bindings), "--output", str(reset_path),
    ], database_factory=factory, observer_factory=observer_factory,
        deployment_inventory_reader_factory=inventory_reader_factory) == 0
    planned = reset.ManifestDocument.model_validate_json(reset_path.read_text())
    assert reset.main([
        *cli_args(), "--execute", "--manifest", str(reset_path), "--confirm-manifest-hash", planned.manifest_hash,
        "--expected-epoch", initial.manifest.new_epoch, "--preflight-config", str(bindings),
    ], database_factory=factory, observer_factory=observer_factory,
        deployment_inventory_reader_factory=inventory_reader_factory) == 0
    assert db.tables[db.table_name("incidents")] == []
    assert len(db.tables[db.table_name("monitoring_receipts")]) == 2


def test_explicit_delegated_azure_cli_selection_is_pinned_without_fallback(monkeypatch):
    import sys
    from types import ModuleType

    seen = []
    azure = ModuleType("azure")
    identity = ModuleType("azure.identity")
    azure.identity = identity
    identity.AzureCliCredential = lambda **kwargs: seen.append(kwargs) or OfflineCredential()
    monkeypatch.setitem(sys.modules, "azure", azure)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    db = reset.create_database(TARGET, reset.CredentialSelection(mode="azure-cli", subscription_id=SUBSCRIPTION))
    assert seen == [{"subscription": SUBSCRIPTION, "process_timeout": 60}]
    assert isinstance(db._credential, reset.PinnedDeployerCredential)
    assert db._credential.get_token(reset.SQL_SCOPE).token


def test_explicit_broker_path_uses_selected_tenant_domain_and_no_shared_az_switch(monkeypatch):
    commands = []

    def broker(command, **kwargs):
        commands.append(command)
        assert kwargs["check"] is False and kwargs["capture_output"] is True
        return SimpleNamespace(returncode=0, stdout=token().token)

    monkeypatch.setattr(reset.subprocess, "run", broker)
    selection = reset.CredentialSelection(mode="broker", operator_domain="example.test")
    db = reset.create_database(TARGET, selection)
    db._credential.get_token(reset.SQL_SCOPE)
    db._credential.get_token(reset.ARM_SCOPE)
    assert all(command[:2] == ["azureauth", "aad"] for command in commands)
    assert all(command[command.index("--tenant") + 1] == TENANT for command in commands)
    assert all(command[command.index("--domain") + 1] == "example.test" for command in commands)


@pytest.mark.parametrize("tenant,principal", [(str(UUID(int=90)), DEPLOYER), (TENANT, str(UUID(int=90)))])
def test_pinned_credentials_refuse_another_tenant_or_operator(tenant, principal):
    credential = SimpleNamespace(get_token=lambda *_args, **_kwargs: token(tenant, principal))
    with pytest.raises(reset.ResetRefused, match="explicitly selected"):
        reset.PinnedDeployerCredential(credential, TARGET).get_token(reset.SQL_SCOPE)


def test_no_unpinned_or_preopened_database_handle():
    db = TransactionalSqlFake()
    db._credential = None
    with pytest.raises(reset.ResetRefused, match="pinned deployer"):
        reset.SqlResetOperator(db, TARGET)
    db = TransactionalSqlFake()
    db._local = SimpleNamespace(conn=object())
    with pytest.raises(reset.ResetRefused, match="already-open"):
        reset.SqlResetOperator(db, TARGET)
