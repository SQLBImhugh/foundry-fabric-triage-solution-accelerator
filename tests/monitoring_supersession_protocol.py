"""Offline bridge from the typed SQL adapter to emitted supersession SQL guards."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta

from test_monitoring_sql_removals import _json
from test_monitoring_sql_removals import case as removal_case
from test_monitoring_sql_removals import db as removal_db
from test_monitoring_sql_review9_bindings import Review9Database
from test_monitoring_sql_supersessions import _nested_modify, _publish

from triage.monitoring import models as m
from triage.monitoring.memory import StoredRecord, key_digest
from triage.monitoring.sql_kernel_connectors import desired_supersession_request_expression
from triage.monitoring.sql_kernel_contracts import RECORD_COLUMNS
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.monitoring.sql_store import _read_time
from triage.store.azure_sql import SqlCommitUncertain


def _time(value):
    return value.isoformat().replace("+00:00", "Z") if isinstance(value, datetime) else value


class SupersessionProtocolDatabase(Review9Database):
    def __init__(self, h):
        self.collection_leases = {}
        super().__init__(h)

    @staticmethod
    def rpc_binding(args):
        value = {key: _time(value) for key, value in args.items() if key not in {"request_id", "fingerprint"}}
        return hashlib.sha256(_json(value).encode("utf-16-le")).hexdigest().upper()

    def handoff(self, operation, args, **kwargs):
        result = super().handoff(operation, args, **kwargs)
        key = (f"{operation.split('.')[0]}_reconcile_request", args["request_id"])
        row = self.records[key]
        document = json.loads(row.payload)
        document["request_payload"] = {
            name: _time(value) for name, value in args.items() if name not in {"request_id", "fingerprint"}
        }
        self.records[key] = StoredRecord(**{**row.__dict__, "payload": _json(document)})
        return result

    @contextmanager
    def transaction(self):
        before = deepcopy(self.collection_leases)
        fail_commit = self.fail_commit
        try:
            with super().transaction():
                yield self
        except BaseException as exc:
            if not (isinstance(exc, SqlCommitUncertain) and fail_commit == "after"):
                self.collection_leases = before
            raise

    def save_work(self, work):
        super().save_work(work)
        if work.lease is not None:
            self.collection_leases[work.key] = work.lease
        elif work.key in self.collection_leases:
            self.collection_leases[work.key] = self.collection_leases[work.key].model_copy(update={
                "expires_at": self.clock(),
            })

    def native_put(self, kind, key, payload, **indices):
        if kind == "connector_desired":
            connector = self.model("connector", key, m.OwnedConnectorManifest)
            assert connector is not None
            previous = self.records.get((kind, key))
            first_anchor = json.loads(previous.payload).get("supersession_request_id") if previous else None
            payload = {
                **payload,
                "definition_hash": m.connector_definition_hash(connector.desired_definition),
                "sources_hash": hashlib.sha256(_json([
                    source.model_dump(mode="json") for source in connector.sources
                ]).encode("utf-16-le")).hexdigest().upper(),
                "supersession_request_id": first_anchor or payload.get("supersession_request_id"),
            }
        return super().native_put(kind, key, payload, **indices)

    def publish_connector(self, args):
        plan = m.ConnectorPublicationPlan.model_validate_json(
            self.records[("connector_publication", args["publication_id"])].payload,
        )
        if not plan.source_removal_supersessions:
            return super().publish_connector(args)
        work = self.owned(args)
        if hashlib.sha256(
            self.records[("connector_publication", args["publication_id"])].payload.encode("utf-16-le"),
        ).hexdigest().upper() != args["publication_hash"]:
            raise RuntimeError("Publication hash changed (51072)")
        prior = self.model("connector", plan.connector_id, m.OwnedConnectorManifest)
        desired = self.records[("connector_desired", plan.connector_id)]
        kernel = build_permission_kernel()
        generator = removal_db.__wrapped__()
        connection = next(generator)
        try:
            removal_case.__wrapped__(connection)
            records, receipts, leases = (
                kernel.names.table(name) for name in ("monitoring_records", "monitoring_receipts", "monitoring_leases")
            )
            for table in (records, receipts, leases, "dbo.control"):
                connection.execute(f"DELETE FROM {table}")
            connection.execute("INSERT INTO dbo.control VALUES (?,?,?)", (
                self.control.tenant_id, self.control.epoch, self.control.revision,
            ))
            for row in self.records.values():
                values = {
                    "tenant_id": row.context.tenant_id, "epoch": row.context.epoch, "record_kind": row.kind,
                    "key_hash": bytes.fromhex(key_digest(row.key)), "full_key": row.key, "revision": row.version,
                    "status": row.status, "workload": row.workload, "workspace_id": row.workspace_id,
                    "item_id": row.item_id, "target_key": row.target_key, "parent_key": row.parent_key,
                    "target_hash": bytes.fromhex(key_digest(row.target_key)) if row.target_key else None,
                    "parent_hash": bytes.fromhex(key_digest(row.parent_key)) if row.parent_key else None,
                    "work_kind": row.work_kind, "generation_id": row.generation_id, "due_at": _time(row.due_at),
                    "sequence_number": row.sequence_number, "payload": row.payload,
                }
                connection.execute(
                    f"INSERT INTO {records} (" + ",".join(RECORD_COLUMNS) + ") VALUES ("
                    + ",".join("?" for _ in RECORD_COLUMNS) + ")",
                    tuple(values[name] for name in RECORD_COLUMNS),
                )
            for (operation, request_id), receipt in self.receipts.items():
                connection.execute(f"INSERT INTO {receipts} VALUES (?,?,?,?,?,?,?,?)", (
                    self.context.tenant_id, self.context.epoch, operation,
                    bytes.fromhex(key_digest(request_id)), request_id, receipt["fingerprint"],
                    _time(receipt["recorded_at"]), _json(receipt["payload"]),
                ))
            for lease in self.collection_leases.values():
                connection.execute(f"INSERT INTO {leases} VALUES (?,?,?,?,?,?,?)", (
                    lease.tenant_id, lease.epoch, lease.resource_key, bytes.fromhex(key_digest(lease.resource_key)),
                    lease.owner_id, lease.fence, _time(lease.expires_at),
                ))
            connection.create_function("JSON_MODIFY", 3, _nested_modify)
            connection.create_function("DATEADD", 3, lambda unit, count, when:
                                       _time(datetime.fromisoformat(when.replace("Z", "+00:00")) + timedelta(seconds=count)))
            connection.execute("CREATE TEMP TABLE supersessions "
                               "(removal_id TEXT,source_id TEXT,node_name TEXT,binding_json TEXT,payload TEXT)")
            connection.execute("CREATE TEMP TABLE supersession_queued_work (work_id TEXT,revision INTEGER)")
            params = {
                **args, "current_revision": self.control.revision, "now": _time(self.clock()),
                "connector_id": plan.connector_id, "ownership_id": plan.ownership_id,
                "expected_connector_revision": plan.expected_connector_revision, "work_key": work.key,
                "binding_hash": self.rpc_binding(args), "name": plan.name, "prior": _json(prior.model_dump(mode="json")),
                "desired": desired.payload, "plan": _json(plan.model_dump(mode="json")),
                "stored_work": _json(work.model_dump(mode="json")),
                "sources": _json([source.model_dump(mode="json") for source in plan.sources]),
                "proposals": _json([item.model_dump(mode="json") for item in plan.source_proposals]),
                "definition": _json(plan.desired_definition),
                "binding_receipt_id": plan.observation_receipt_id, "readiness_id": plan.readiness_receipt_id,
                "removal_intents": _json([item.model_dump(mode="json") for item in plan.source_removals]),
                "supersession_intents": _json([item.model_dump(mode="json") for item in plan.source_removal_supersessions]),
                "superseded_json": "[]",
            }
            connection.commit()
            try:
                result = _publish(connection, kernel, params)
            except ValueError as exc:
                raise RuntimeError(f"Guarded supersession refused: {exc} (51072)") from exc
            anchor_expression = desired_supersession_request_expression().replace("JSON_VALUE(", "json_extract(")
            anchor_expression = anchor_expression.replace("@supersessions", "supersessions")
            anchor = connection.execute("SELECT " + anchor_expression, params).fetchone()[0]
            for row in connection.execute("SELECT " + ",".join(RECORD_COLUMNS) + f" FROM {records}"):
                value = dict(zip(RECORD_COLUMNS, row, strict=True))
                key = (value["record_kind"], value["full_key"])
                if key == ("connector_desired", plan.connector_id):
                    # The guard executor adapts JSON serialization; use the
                    # production expression rather than its old latest-ID default.
                    value["payload"] = _json({**json.loads(value["payload"]), "supersession_request_id": anchor})
                self.records[key] = StoredRecord(
                    kind=key[0], key=key[1], context=m.MonitoringContext(
                        tenant_id=value["tenant_id"], epoch=value["epoch"],
                    ), version=value["revision"], payload=value["payload"], status=value["status"],
                    workload=value["workload"], workspace_id=value["workspace_id"], item_id=value["item_id"],
                    target_key=value["target_key"], parent_key=value["parent_key"], work_kind=value["work_kind"],
                    generation_id=value["generation_id"], due_at=_read_time(value["due_at"]) if value["due_at"] else None,
                    sequence_number=value["sequence_number"],
                )
            reply = self.reply("controller.publish_connector", args, result)
            if self.fail_connector_receipt:
                self.fail_connector_receipt = False
                raise RuntimeError("Injected publication receipt failure (51072)")
            return reply
        finally:
            generator.close()
