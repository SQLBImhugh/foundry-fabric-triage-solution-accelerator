from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from test_monitoring_sql_routing import KernelProtocolDatabase
from test_monitoring_sql_store import DriverRow, SqliteConnection
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
)
from triage.monitoring.memory import StoredRecord, key_digest, stable_id
from triage.monitoring.rate_limit import RatePolicy
from triage.monitoring.sql_kernel_contracts import (
    CATALOGUE_KINDS,
    EVIDENCE_KINDS,
    KERNEL_VERSION,
    RPC_FACT_KINDS,
    TELEMETRY_KINDS,
    WORK_FACT_KINDS,
)
from triage.monitoring.sql_kernel_frontiers import (
    closed_window_authority_sql,
    frontier_can_close_sql,
    handoff_decision_change_sql,
    nonwindow_handoff_authority_sql,
    page_publication_required_sql,
    stale_page_policy_sql,
)
from triage.monitoring.sql_kernel_work import (
    connector_collection_completion_sql,
    reconciliation_completion_sql,
)
from triage.monitoring.sql_store import AzureSqlMonitoringStore, KernelRateBudget
from triage.store.azure_sql import SqlCommitUncertain

FACT_KINDS = (*CATALOGUE_KINDS, *EVIDENCE_KINDS, *TELEMETRY_KINDS, *RPC_FACT_KINDS)


class AbiDatabase(KernelProtocolDatabase):
    """Execute emitted record queries/CAS and emulate the stable RPC envelopes offline.

    This composes the real SQL adapter with named ABI operations. It does not
    claim to execute T-SQL procedures or prove deployed SQL role permissions.
    """

    def __init__(self, h, *, principal):
        super().__init__(h, principal=principal)
        self.context = m.MonitoringContext(**h.context())
        self.next_identifier = 700_000
        self.fail_after_native = None
        self.target_leases = {}
        self.processed = set()

    @contextmanager
    def transaction(self):
        control = self.control
        failure = self.fail_commit
        authority = deepcopy((self.target_leases, self.processed))
        try:
            with super().transaction():
                yield self
        except BaseException as exc:
            if not (isinstance(exc, SqlCommitUncertain) and failure == "after"):
                self.control = control
                self.target_leases, self.processed = authority
            raise

    def seed_published_fixture(self, h):
        self.control = m.DeploymentControl.model_validate(h.state.control_row)
        self.records = {}
        for row in h.state.records.values():
            payload = json.loads(row.payload)
            if row.kind == "scope":
                payload.pop("updated_at")
                payload.update(policy_revision=payload["revision"], revision=row.version, request_id=uid(699_999))
            elif row.kind == "plan":
                payload.update(
                    request_id=stable_id(row.context, f"preview:{payload['idempotency_id']}"),
                    policy_revision=payload["expected"]["revision"], revision=row.version,
                )
            self.records[(row.kind, row.key)] = replace(row, payload=json.dumps(payload))
        self.fixture_facts = {
            (row.kind, row.key): self.row_hash(row) for row in self.records.values() if row.kind in FACT_KINDS
        }
        for row in self.records.values():
            if row.kind == "work":
                work = m.MonitoringWork.model_validate_json(row.payload)
                if work.lease is not None and work.target is not None and work.kind in m.CONTROLLER_WORK_KINDS:
                    self.target_leases[work.target.key] = (work.work_id, work.lease.expires_at)

    def save_work(self, work):
        super().save_work(work)
        if work.lease is not None and work.target is not None and work.kind in m.CONTROLLER_WORK_KINDS:
            self.target_leases[work.target.key] = (work.work_id, work.lease.expires_at)

    @staticmethod
    def row_hash(row):
        payload = {
            **row.__dict__, "context": row.context.model_dump(mode="json"),
            "due_at": row.due_at.isoformat() if row.due_at else None,
        }
        return key_digest(json.dumps(payload, sort_keys=True))

    def accepted(self, row):
        if getattr(self, "fixture_facts", {}).get((row.kind, row.key)) == self.row_hash(row):
            return True
        for binding in self.records.values():
            if binding.kind != "accepted_fact":
                continue
            value = json.loads(binding.payload)
            if (
                value["fact_kind"] == row.kind and value["fact_key"] == row.key
                and value["fact_revision"] == row.version and value["row_hash"] == self.row_hash(row)
                and any((operation, value["batch_id"]) in self.receipts for operation in (
                    "worker.accept_facts", "worker.commit_positions", "worker.record_heartbeat",
                ))
            ):
                return True
        return False

    def query(self, sql, *params):
        if sql == "SELECT JSON_QUERY(?, '$.observed_definition')":
            self.calls.append(("query", sql, params))
            with sqlite3.connect(":memory:") as connection:
                value = connection.execute("SELECT json_extract(?, '$.observed_definition')", params).fetchone()[0]
            return [DriverRow((value,))]
        if (
            sql.startswith(("SELECT record_kind", "SELECT TOP", "SELECT COUNT", "SELECT COALESCE"))
            or "ROW_NUMBER()" in sql
        ):
            assert self.active
            self.calls.append(("query", sql, params))
            accepted_only = "accepted_worker_facts" in sql
            rows = [
                row for row in self.records.values()
                if not (accepted_only or self.principal == "controller") or row.kind not in FACT_KINDS or self.accepted(row)
            ]
            with sqlite3.connect(":memory:") as connection:
                connection.execute(
                    "CREATE TABLE records (tenant_id,epoch,record_kind,full_key,revision,status,workload,"
                    "workspace_id,item_id,target_key,parent_key,work_kind,generation_id,due_at,sequence_number,payload,"
                    "key_hash,target_hash,parent_hash)"
                )
                connection.create_function(
                    "SYSUTCDATETIME", 0, lambda: self.clock().replace(tzinfo=None).isoformat(timespec="microseconds"),
                )
                connection.create_function("READ_UTC", 1, self._read_utc)
                connection.executemany("INSERT INTO records VALUES (" + ",".join("?" for _ in range(19)) + ")", [
                    (
                        row.context.tenant_id, row.context.epoch, row.kind, row.key, row.version, row.status,
                        row.workload, row.workspace_id, row.item_id, row.target_key, row.parent_key, row.work_kind,
                        row.generation_id, row.due_at.replace(tzinfo=None).isoformat(timespec="microseconds") if row.due_at else None,
                        row.sequence_number, row.payload, bytes.fromhex(key_digest(row.key)),
                        bytes.fromhex(key_digest(row.target_key)) if row.target_key else None,
                        bytes.fromhex(key_digest(row.parent_key)) if row.parent_key else None,
                    ) for row in rows
                ])
                translated = re.sub(r"\[dbo\]\.\[[^\]]+\]", "records", sql)
                translated = SqliteConnection.translate(translated)
                translated = translated.replace("JSON_VALUE(", "json_extract(")
                translated = translated.replace("TRY_CONVERT(datetime2(6), ", "READ_UTC(")
                translated = self._policy_sql(connection, translated)
                bound = tuple(
                    value.replace(tzinfo=None).isoformat(timespec="microseconds") if isinstance(value, datetime) else value
                    for value in params
                )
                return [DriverRow(tuple(row)) for row in connection.execute(translated, bound).fetchall()]
        return super().query(sql, *params)

    @staticmethod
    def _read_utc(value):
        if value is None:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None).isoformat(
                timespec="microseconds",
            )
        except (TypeError, ValueError, AttributeError):
            return None

    def execute(self, sql, *params):
        super().execute(sql, *params)
        if sql.startswith("INSERT"):
            columns = [column.strip() for column in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
            assert len(params) % len(columns) == 0
            written = [
                dict(zip(columns, params[offset:offset + len(columns)], strict=True))
                for offset in range(0, len(params), len(columns))
            ]
            keys = [(row["record_kind"], row["full_key"]) for row in written]
            if len(set(keys)) != len(keys) or any(key in self.records for key in keys):
                raise RuntimeError("Duplicate record identity (51072)")
        else:
            assignments, predicates = sql.split(" SET ", 1)[1].split(" WHERE ", 1)
            columns = [entry.strip().split(" = ")[0] for entry in assignments.split(",")]
            changes = dict(zip(columns, params[:len(columns)], strict=True))
            identity = params[len(columns):]
            key = (identity[2], identity[4])
            prior = self.records.get(key)
            if prior is None or prior.version != identity[5]:
                return 0
            values = {
                "record_kind": prior.kind, "full_key": prior.key, "revision": prior.version,
                "status": prior.status, "workload": prior.workload, "workspace_id": prior.workspace_id,
                "item_id": prior.item_id, "target_key": prior.target_key, "parent_key": prior.parent_key,
                "work_kind": prior.work_kind, "generation_id": prior.generation_id,
                "due_at": prior.due_at, "sequence_number": prior.sequence_number, "payload": prior.payload,
            } | changes
            written = [values]
        for values in written:
            key = (values["record_kind"], values["full_key"])
            at = values.get("due_at")
            self.records[key] = StoredRecord(
                kind=key[0], key=key[1], context=self.context, version=values["revision"], status=values.get("status"),
                workload=values.get("workload"), workspace_id=values.get("workspace_id"), item_id=values.get("item_id"),
                target_key=values.get("target_key"), parent_key=values.get("parent_key"), work_kind=values.get("work_kind"),
                generation_id=values.get("generation_id"), due_at=at.replace(tzinfo=UTC) if at else None,
                sequence_number=values.get("sequence_number"), payload=values["payload"],
            )
        return len(written)

    def native_put(self, kind, key, payload, **indices):
        prior = self.records.get((kind, key))
        row = StoredRecord(
            kind=kind, key=key, context=self.context, version=prior.version + 1 if prior else 1,
            payload=json.dumps(payload), **indices,
        )
        self.records[(kind, key)] = row
        return row

    @staticmethod
    def rpc_binding(args):
        return key_digest(json.dumps({
            key: value.isoformat() if isinstance(value, datetime) else value
            for key, value in args.items() if key not in {"request_id", "fingerprint"}
        }, sort_keys=True, separators=(",", ":")))

    def native_rows(self, sql, params):
        """Run emitted decision queries over the fixture's original records/receipts."""
        from test_monitoring_sql_removals import _adapt, _query, _value
        from test_monitoring_sql_retry_finalization import _convert

        with sqlite3.connect(":memory:") as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("ATTACH DATABASE ':memory:' AS dbo")
            connection.create_function("JSON_VALUE", 2, _value)
            connection.create_function("JSON_QUERY", 2, _query)
            connection.create_function("TRY_CONVERT", 2, _convert)
            connection.create_collation("Latin1_General_100_BIN2", lambda a, b: (a > b) - (a < b))
            records, receipts = self.names.table("monitoring_records"), self.names.table("monitoring_receipts")
            connection.execute(f"CREATE TABLE {records} "
                               "(tenant_id,epoch,record_kind,full_key,revision,status,parent_key,sequence_number,payload)")
            connection.executemany(f"INSERT INTO {records} VALUES (?,?,?,?,?,?,?,?,?)", [
                (row.context.tenant_id, row.context.epoch, row.kind, row.key, row.version, row.status,
                 row.parent_key, row.sequence_number, row.payload) for row in self.records.values()
            ])
            connection.execute(f"CREATE TABLE {receipts} "
                               "(tenant_id,epoch,operation,request_id,fingerprint,payload)")
            connection.executemany(f"INSERT INTO {receipts} VALUES (?,?,?,?,?,?)", [
                (self.context.tenant_id, self.context.epoch, operation, request_id,
                 value["fingerprint"], json.dumps(value["payload"]))
                for (operation, request_id), value in self.receipts.items()
            ])
            return connection.execute(_adapt(self, sql).replace("COUNT_BIG(", "COUNT("), params).fetchall()

    def handoff(self, operation, args, *, topic, reference, target=None, complete=True, window=None):
        self.next_identifier += 1
        work_id = uid(self.next_identifier)
        frontier_key = f"validation:test:{topic}:{reference}"
        prior = self.records.get(("validation_frontier", frontier_key))
        ordinal = json.loads(prior.payload)["accepted_revision"] + 1 if prior else 1
        frontier = m.ValidationFrontier(
            **self.context.model_dump(), frontier_key=frontier_key, target=target, window=window,
            accepted_revision=ordinal, latest_request_id=args["request_id"], updated_at=self.clock(),
            validated_revision=json.loads(prior.payload)["validated_revision"] if prior else 0,
        )
        self.native_put("validation_frontier", frontier_key, frontier.model_dump(mode="json"),
                        status="pending_validation", target_key=target.key if target else None, sequence_number=ordinal,
                        parent_key=frontier_key if topic in {"inventory", "poll"} else None)
        if topic in {"inventory", "poll"}:
            self.native_put("validation_window", frontier_key, {
                "frontier_key": frontier_key, "collection_id": reference, "collection_complete": complete,
                "closing_request_id": args["request_id"] if complete else None,
                "closing_revision": ordinal if complete else None,
                "window": window.model_dump(mode="json") if window else None,
            }, status="awaiting_validation" if complete else "collecting")
        work = m.MonitoringWork(
            **self.context.model_dump(), work_id=work_id, kind="reconcile_state", policy_revision=self.control.revision,
            created_at=self.clock(), due_at=self.clock(), target=target, revision=1, state="queued",
            reason="Accepted intent requires deterministic reconciliation",
            reconcile_request_id=args["request_id"], reconcile_producer=operation.split(".")[0],
        )
        self.save_work(work)
        self.native_put(f"{operation.split('.')[0]}_reconcile_request", args["request_id"], {
            **self.context.model_dump(), "request_id": args["request_id"], "producer": operation.split(".")[0],
            "topic": topic, "reference_id": reference, "fingerprint": args["fingerprint"],
            "policy_revision": self.control.revision, "work_id": work_id,
            "target": target.model_dump(mode="json") if target else None,
            "window": window.model_dump(mode="json") if window else None, "frontier_key": frontier_key,
            "frontier_revision": ordinal, "created_at": self.clock().isoformat(),
            "request_payload": {key: value.isoformat() if isinstance(value, datetime) else value for key, value in args.items()},
            "evidence": json.loads(args["facts_json"]) if "facts_json" in args else [],
        })
        self.native_put("validation_handoff", f"{frontier_key}:handoff:{ordinal}", {
            "frontier_key": frontier_key, "frontier_revision": ordinal, "producer": operation.split(".")[0],
            "producer_request_id": args["request_id"], "producer_operation": operation,
            "producer_fingerprint": args["fingerprint"], "producer_binding_hash": self.rpc_binding(args),
            "work_id": work_id, "policy_revision": self.control.revision,
            "evidence_digest": "b" * 64, "requires_window": topic in {"inventory", "poll"},
        }, parent_key=frontier_key, sequence_number=ordinal, status="pending_validation")
        return {"reconcile_work_id": work_id, "frontier_key": frontier_key, "frontier_revision": ordinal}

    def _resolve_frontier_result(self, args):
        work = m.MonitoringWork.model_validate_json(self.records[("work", args["work_id"])].payload)
        if (
            work.kind != "reconcile_state" or work.state != "leased" or work.lease is None
            or work.lease.owner_id != args["owner_id"] or work.lease.fence != args["fence"]
            or work.revision != args["work_revision"] or work.lease.expires_at <= self.clock()
            or args["expected_revision"] != self.control.revision
        ):
            raise RuntimeError("Current reconciliation/control fence changed (51074)")
        proof_row = self.records[("frontier_validation", args["validation_id"])]
        assert hashlib.sha256(proof_row.payload.encode("utf-16-le")).hexdigest().upper() == args["validation_hash"]
        proof = m.FrontierValidation.model_validate({
            "validation_id": args["validation_id"], **json.loads(proof_row.payload),
        })
        key = proof.frontier_key
        frontier = json.loads(self.records[("validation_frontier", key)].payload)
        handoff = next(row for row in self.records.values() if row.kind == "validation_handoff"
                       and json.loads(row.payload)["work_id"] == work.work_id)
        binding = m.ValidationHandoff.model_validate_json(handoff.payload)
        if (
            proof.work_id != work.work_id or proof.lease_owner_id != work.lease.owner_id
            or proof.lease_fence != work.lease.fence or proof.expected_work_revision != work.revision
            or proof.policy_revision != self.control.revision
            or proof.through_revision != args["expected_frontier_revision"]
            or proof.through_revision != frontier["accepted_revision"] or proof.frontier_key != binding.frontier_key
            or proof.producer_request_id != binding.producer_request_id
            or proof.producer_fingerprint != binding.producer_fingerprint or proof.evidence_digest != binding.evidence_digest
        ):
            raise RuntimeError("Original frontier proof binding differs (51072)")
        window_row = self.records.get(("validation_window", key))
        window = json.loads(window_row.payload) if window_row else None
        whole = proof.reject_whole_window
        window_ack = window_row is not None and window_row.status in {"rejected", "validated"} and not whole
        handoff_ack = proof.acknowledge_handoff
        params = {
            **args, "frontier_key": key, "accepted": frontier["accepted_revision"],
            "validated": frontier["validated_revision"], "handoff_key": handoff.key,
            "handoff_revision": handoff.sequence_number, "producer_request_id": work.reconcile_request_id,
            "whole_window_rejection": int(whole), "window_ack": int(window_ack), "handoff_ack": int(handoff_ack),
            "handoff_state": handoff.status, "handoff": handoff.payload, "decision": proof.decision,
            "current_revision": self.control.revision, "maintenance": int(self.control.maintenance),
        }
        references = dict.fromkeys((
            "handoff_resolution_request_id", "handoff_resolution_work_fence",
            "frontier_resolution_request_id", "frontier_resolution_revision",
        ))
        window_resolution = None
        resolved_state = None
        if handoff_ack:
            if window is not None or handoff.status not in {"published", "rejected"} or proof.decision != handoff.status:
                raise RuntimeError("Handoff acknowledgement requires one non-window terminal decision (51072)")
            authority = self.native_rows(nonwindow_handoff_authority_sql(self.names), params)
            if not authority:
                raise RuntimeError("Handoff acknowledgement lacks original decision and committed prefix receipts (51072)")
            references.update(dict(min(authority, key=lambda row: (
                row["handoff_resolution_work_fence"], row["handoff_resolution_request_id"],
            ))))
            resolved_state = handoff.status
        elif window_ack:
            authority = self.native_rows(closed_window_authority_sql(self.names), params)
            if len(authority) != 1:
                raise RuntimeError("Window acknowledgement lacks original terminal resolution (51072)")
            window_resolution, resolved_state = authority[0]
        elif whole and (window_row is None or window_row.status not in {"collecting", "awaiting_validation"}):
            raise RuntimeError("Whole-window rejection requires an unfinished window (51072)")
        refuse = (
            f"({handoff_decision_change_sql()}) OR "
            f"(({page_publication_required_sql()}) AND ({stale_page_policy_sql()}))"
        )
        if self.native_rows(f"SELECT CASE WHEN {refuse} THEN 1 ELSE 0 END", params)[0][0]:
            raise RuntimeError("Earlier handoff decision or publication policy is immutable (51072)")
        if not whole and not window_ack and not handoff_ack:
            self.records[(handoff.kind, handoff.key)] = replace(
                handoff, status=proof.decision, version=handoff.version + 1,
            )
        pages = [row for row in self.records.values() if row.kind == "validation_handoff"
                 and row.parent_key == key and 1 <= row.sequence_number <= frontier["accepted_revision"]]
        prefix_committed = len(pages) == frontier["accepted_revision"] and all(
            (json.loads(page.payload)["producer_operation"], json.loads(page.payload)["producer_request_id"])
            in self.receipts for page in pages
        )
        if whole and not prefix_committed:
            raise RuntimeError("Whole-window rejection lost original committed intake (51072)")
        close = not (window_ack or handoff_ack) and bool(self.native_rows(
            f"SELECT CASE WHEN {frontier_can_close_sql()} THEN 1 ELSE 0 END", {
                **params, "prefix_committed": int(prefix_committed),
                "all_resolved": int(len(pages) == frontier["accepted_revision"] and all(
                    page.status in {"published", "rejected"} for page in pages
                )),
                "window": json.dumps(window) if window is not None else None, "proof": proof.model_dump_json(),
            },
        )[0][0])
        state = resolved_state if window_ack or handoff_ack else proof.decision if close else "pending_validation"
        scope = (
            "handoff_acknowledgement" if handoff_ack else "window_acknowledgement" if window_ack
            else "window" if whole else "handoff"
        )
        if close:
            frontier["validated_revision"] = frontier["accepted_revision"]
            self.native_put("validation_frontier", key, frontier, status=state, sequence_number=frontier["accepted_revision"])
            if window is not None:
                self.native_put("validation_window", key, window, status="rejected" if state == "rejected" else "validated")
            self.native_put("frontier_commit", key, {
                "request_id": args["request_id"], "validation_id": args["validation_id"],
                "frontier_key": key, "frontier_revision": frontier["accepted_revision"], "decision": state,
            }, status=state, sequence_number=frontier["accepted_revision"])
        result = {
            "work_id": work.work_id, "work_fence": work.lease.fence, "producer_request_id": work.reconcile_request_id,
            "frontier_key": key, "frontier_revision": frontier["accepted_revision"],
            "handoff_revision": handoff.sequence_number, "validated_revision": frontier["validated_revision"],
            "state": state, "handoff_decision": handoff.status if whole or window_ack or handoff_ack else proof.decision,
            "resolution_scope": scope, **references,
            "window_rejection_request_id": window_resolution if resolved_state == "rejected" and window_ack else None,
            "window_resolution_request_id": window_resolution,
            "window_resolution_state": resolved_state if window_ack else None,
        }
        self.native_put("reconcile_acceptance", args["request_id"], result,
                        parent_key=key, status=state, sequence_number=frontier["accepted_revision"])
        return result

    def apply_rpc(self, operation, args):
        if operation not in {
            "worker.accept_facts", "web.commit_intent", "worker.transition_work",
            "controller.resolve_frontier", "controller.transition_work",
            "controller.publish_source", "controller.disposition_source",
        }:
            return super().apply_rpc(operation, args)
        identity = (operation, args["request_id"])
        binding = self.rpc_binding(args)
        prior = self.receipts.get(identity)
        if prior is not None:
            if prior["fingerprint"] != args["fingerprint"] or prior["payload"]["binding_hash"] != binding:
                raise RuntimeError("Idempotent input changed (51072)")
            return {"kernel_version": KERNEL_VERSION, "operation": operation, "status": "replayed", "affected_rows": 0,
                    "result": prior["payload"]["result"]}
        if args["expected_revision"] != self.control.revision and operation not in {
            "controller.transition_work", "controller.publish_source", "controller.disposition_source",
        }:
            raise RuntimeError("Current policy changed (51072)")
        if operation == "worker.accept_facts":
            work = m.MonitoringWork.model_validate_json(self.records[("work", args["work_id"])].payload)
            if work.state != "leased" or work.revision != args["work_revision"] or (
                work.lease.owner_id != args["owner_id"] or work.lease.fence != args["fence"]
                or work.lease.expires_at <= self.clock()
            ):
                raise RuntimeError("Collection lease/revision changed (51074)")
            facts = json.loads(args["facts_json"])
            assert 1 <= len(facts) <= 200
            for fact in facts:
                row = self.records[(fact["kind"], fact["key"])]
                if row.kind not in WORK_FACT_KINDS[work.kind] or row.version != fact["revision"] or (
                    hashlib.sha256(row.payload.encode("utf-16-le")).hexdigest().upper() != fact["payload_hash"]
                ):
                    raise RuntimeError("Raw fact binding changed (51072)")
                if work.target is not None and row.target_key != work.target.key:
                    raise RuntimeError("Raw fact belongs to another target (51072)")
                if work.kind == "inventory" and (row.generation_id or row.parent_key or row.key) != work.work_id:
                    raise RuntimeError("Inventory fact belongs to another generation (51072)")
                self.native_put("accepted_fact", f"{args['request_id']}:{row.kind}:{key_digest(row.key)}", {
                    "batch_id": args["request_id"], "batch_fingerprint": args["fingerprint"], "fact_kind": row.kind,
                    "fact_key": row.key, "fact_revision": row.version, "row_hash": self.row_hash(row),
                })
            window = m.ObservationWindow(
                start_at=args["window_start_at"].replace(tzinfo=UTC), end_at=args["window_end_at"].replace(tzinfo=UTC),
            ) if args["window_start_at"] else None
            result = {
                "batch_id": args["request_id"], "work_id": work.work_id, "work_fence": work.lease.fence,
                "state": "accepted_for_reconciliation", "facts": facts,
                **self.handoff(operation, args, topic=work.kind, reference=work.work_id,
                               target=work.target, complete=args["collection_complete"], window=window),
            }
        elif operation == "web.commit_intent":
            intent = json.loads(args["intent_json"])
            kind = {"preview": "plan", "scope": "scope", "review": "review_request", "discovery": "discovery_request"}[args["intent_kind"]]
            prior_row = self.records.get((kind, args["intent_id"]))
            if (prior_row.version if prior_row else 0) != args["expected_intent_revision"]:
                raise RuntimeError("Intent revision changed (51072)")
            if args["intent_kind"] in {"scope", "review"}:
                self.control = m.DeploymentControl.model_validate({
                    **self.control.model_dump(), "revision": self.control.revision + 1,
                })
            revision = args["expected_intent_revision"] + 1
            self.native_put(kind, args["intent_id"], {
                **intent, "request_id": args["request_id"], "revision": revision, "policy_revision": self.control.revision,
            })
            result = {"request_id": args["request_id"], "intent_id": args["intent_id"], "state": "configuring",
                      "policy_revision": self.control.revision, "original_intent": intent}
            if args["intent_kind"] != "preview":
                target = m.TargetIdentity.model_validate(intent["target"]) if args["intent_kind"] == "review" else None
                result.update(
                    intent_kind=args["intent_kind"], expected_intent_revision=args["expected_intent_revision"],
                    new_intent_revision=revision,
                    **self.handoff(operation, args, topic=args["intent_kind"], reference=args["intent_id"], target=target),
                )
        elif operation in {"controller.publish_source", "controller.disposition_source"}:
            result = self.source_operation(operation, args)
        elif operation == "controller.resolve_frontier":
            result = self._resolve_frontier_result(args)
        else:
            work = m.MonitoringWork.model_validate_json(self.records[("work", args["work_id"])].payload)
            if work.lease is None or work.lease.fence != args["fence"] or work.revision != args["work_revision"]:
                raise RuntimeError("Work transition lost ownership (51074)")
            assert args["transition"] in {"complete", "retry", "disposition"}
            if args["transition"] == "disposition":
                assert operation == "worker.transition_work" and args["detail"]
            if operation == "worker.transition_work" and args["transition"] == "complete" and not any(
                name == "worker.accept_facts" and entry["payload"]["result"]["work_id"] == work.work_id
                and entry["payload"]["result"]["work_fence"] == work.lease.fence
                for (name, _), entry in self.receipts.items()
            ) and not self.native_rows(connector_collection_completion_sql(self.names), {
                **args, "stored_work_kind": work.kind, "stored_work": work.model_dump_json(),
                "current_revision": self.control.revision,
            }):
                raise RuntimeError("Collection has no original accepted batch (51072)")
            if operation == "controller.transition_work" and args["transition"] == "complete" and not self.native_rows(
                reconciliation_completion_sql(self.names), args,
            ):
                raise RuntimeError("Reconciliation has no terminal guarded result (51072)")
            work = m.MonitoringWork.model_validate({
                **work.model_dump(), "revision": work.revision + 1,
                "state": {"complete": "completed", "retry": "waiting", "disposition": "dispositioned"}[args["transition"]],
                "lease": None, "completed_at": self.clock() if args["transition"] != "retry" else None,
                "due_at": args["retry_at"].replace(tzinfo=UTC) if args["retry_at"] else work.due_at,
                "disposition": args["detail"] if args["transition"] == "disposition" else work.disposition,
            })
            self.save_work(work)
            result = {"work_id": work.work_id, "work": work.model_dump(mode="json")}
        self.receipts[identity] = {"fingerprint": args["fingerprint"], "recorded_at": self.clock(),
                                  "payload": {"binding_hash": binding, "result": result}}
        if self.fail_after_native == operation:
            self.fail_after_native = None
            raise RuntimeError("Injected after guarded operation (51072)")
        return {"kernel_version": KERNEL_VERSION, "operation": operation, "status": "applied", "affected_rows": 1, "result": result}

    def source_operation(self, operation, args):
        work = m.MonitoringWork.model_validate_json(self.records[("work", args["work_id"])].payload)
        if work.lease is None or work.lease.expires_at <= self.clock() or (
            work.revision != args["work_revision"] or work.lease.owner_id != args["owner_id"]
            or work.lease.fence != args["fence"]
        ):
            raise RuntimeError("Actual source work ownership changed (51074)")
        observation = m.SourceRunObservation.model_validate_json(args["observation_json"])
        if observation.authority not in {"rest", "transport"} or observation.observed_at > self.clock():
            raise RuntimeError("Source authority/time differs (51073)")
        disposing = operation == "controller.disposition_source"
        if work.kind == "reconcile_state":
            if self.control.maintenance or args["expected_revision"] != self.control.revision:
                raise RuntimeError("Source publication policy changed (51072)")
            if args["evidence_kind"] is None and disposing:
                source = self.records.get(("source", observation.key))
                if source is None or m.SourceRunObservation.model_validate_json(source.payload) != observation:
                    raise RuntimeError("Scope cleanup requires the exact published source (51072)")
            else:
                raw = self.records.get((args["evidence_kind"], args["evidence_key"]))
                if raw is None or not self.accepted(raw):
                    raise RuntimeError("Source lacks accepted evidence (51072)")
                payload = json.loads(raw.payload)
                source = payload if args["evidence_kind"] == "rest_observation" else payload["observation"]
                if args["alias_window_id"] is not None:
                    window = self.records.get(("powerbi_window", args["alias_window_id"]))
                    if window is None or window.status != "validated":
                        raise RuntimeError("Source alias window is incomplete (51072)")
                    source = deepcopy(source)
                    source["execution"] = observation.execution.model_dump(mode="json")
                    source["evidence"]["request_id"] = observation.execution.run_id
                if m.SourceRunObservation.model_validate(source) != observation:
                    raise RuntimeError("Source differs from accepted evidence (51072)")
        else:
            owner, expiry = self.target_leases.get(observation.execution.target.key, (None, self.clock()))
            if owner != work.work_id or expiry <= self.clock():
                raise RuntimeError("Actual controller target lease changed (51074)")
            if observation.authority != "rest" or work.target != observation.execution.target:
                raise RuntimeError("Fresh source is not the exact owned REST target (51072)")
            if (self.control.maintenance or args["expected_revision"] != self.control.revision) and work.action_reservation_id is None:
                raise RuntimeError("Only existing effects may refresh after revocation (51072)")
        if disposing:
            if work.action_reservation_id is not None or any(
                row.kind == "action" and m.ActionReservation.model_validate_json(row.payload).request.source_execution == observation.execution
                for row in self.records.values()
            ):
                raise RuntimeError("A reserved source cannot use non-effect disposition (51072)")
            previous = self.records.get(("source_disposition", observation.key))
            if (previous is None) != (observation.key not in self.processed):
                raise RuntimeError("Disposition and processed marker disagree (51072)")
            if previous is None:
                if args["disposition"] == "historical" and (
                    observation.started_at is None or observation.started_at >= self.control.activation_cutoff
                ):
                    raise RuntimeError("Historical evidence does not predate activation (51072)")
                value = {
                    "execution": observation.execution.model_dump(mode="json"), "disposition": args["disposition"],
                    "detail": args["detail"], "work_id": work.work_id, "disposition_request_id": args["request_id"],
                    "recorded_at": self.clock().isoformat(),
                }
                self.native_put("source_disposition", observation.key, value, status=args["disposition"],
                                target_key=observation.execution.target.key)
                self.processed.add(observation.key)
            else:
                value = json.loads(previous.payload)
            if work.kind != "reconcile_state":
                work = m.MonitoringWork.model_validate({
                    **work.model_dump(), "revision": work.revision + 1, "state": "dispositioned",
                    "lease": None, "completed_at": self.clock(), "disposition": args["detail"],
                })
                self.save_work(work)
            return {"source_key": observation.key, "disposition": value, "work": work.model_dump(mode="json")}
        previous = self.records.get(("source", observation.key))
        prior = m.SourceRunObservation.model_validate_json(previous.payload) if previous else None
        if prior is not None and (
            prior.authority == "rest" and observation.authority == "transport"
            or prior.authority == observation.authority and prior.observed_at > observation.observed_at
        ):
            observation = prior
        else:
            self.native_put("source", observation.key, observation.model_dump(mode="json"),
                            target_key=observation.execution.target.key, status=observation.status)
        head_row = self.records.get(("source_head", observation.execution.target.key))
        head = m.SourceRunObservation.model_validate_json(head_row.payload) if head_row else None
        if observation.authority == "rest" and observation.started_at is not None and (
            head is None or head.started_at is None or observation.started_at >= head.started_at
            or observation.execution == head.execution
        ):
            self.native_put("source_head", observation.execution.target.key, observation.model_dump(mode="json"),
                            target_key=observation.execution.target.key)
        return {"source_key": observation.key, "observation": observation.model_dump(mode="json")}


def seeded(workload="fabric_pipeline"):
    h = Harness()
    h.seed(workload=workload)
    h.activate()
    h.review("pipeline_rerun" if workload == "fabric_pipeline" else "powerbi_refresh")
    db = AbiDatabase(h, principal="worker")
    db.seed_published_fixture(h)
    return h, db


def test_native_scope_preview_and_activation_keep_distinct_receipt_namespaces():
    h, db = seeded()
    db.principal = "web"
    store = AzureSqlMonitoringStore(db=db, component="web")
    disabled = m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False})
    request = m.ScopePreviewRequest(expected=h.version, idempotency_id=h.next_id(), scope=disabled)
    plan = store.preview_scope(request)
    receipt = store.activate_scope(m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=request.idempotency_id,
    ))
    assert receipt.state == "configuring"
    assert receipt.version.revision == h.version.revision + 1
    assert len(db.receipts) == 2
    assert store.preview_scope(request) == plan
    assert store.get_activation(h.version, request.idempotency_id) == receipt
    assert not any(method == "execute" for method, _, _ in db.calls)


@pytest.mark.parametrize("expired", [False, True])
def test_native_review_revocation_is_pending_and_keeps_immutable_original_receipt(expired):
    h, db = seeded()
    db.principal = "web"
    store = AzureSqlMonitoringStore(db=db, component="web")
    original = h.store.resolve_target(h.targets[0]).action
    review = h.store.get_safety_review(h.version, original.review_id)
    reviewed_at = h.clock()
    if expired:
        h.clock.advance(int((review.expires_at - h.clock()).total_seconds()) + 1)
    request = m.SafetyReviewRequest(
        request_id=h.next_id(), expected=h.version, expected_review_revision=review.revision,
        review=m.SafetyReview.model_validate({
            **review.model_dump(), "revision": review.revision + 1, "state": "revoked", "revoked_at": h.clock(),
        }),
    )
    # The native fixture begins with controller-published review authority and
    # its corresponding prior human intent revision.
    db.native_put("review_request", review.review_id, {
        **review.model_dump(mode="json", include={
            "review_id", "target", "action", "reviewer_id", "reviewed_at", "expires_at", "parameters", "parameter_hash",
            "definition_hash", "configuration_hash", "replay_safe", "detail",
        }), "requested_state": "verified", "request_id": uid(699_999), "revision": review.revision,
        "policy_revision": h.version.revision,
    })
    db.native_put("web_reconcile_request", uid(699_999), {"created_at": reviewed_at.isoformat()})
    pending = store.record_safety_review(request)
    assert pending.state == "pending" and pending.requested_state == "revoked"
    assert pending.reviewed_at == review.reviewed_at and pending.expires_at == review.expires_at
    receipt = store.get_safety_review_operation(h.version, request.request_id)
    assert receipt.review == pending
    assert receipt.recorded_at >= pending.reviewed_at
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": db.control.revision + 1})
    assert store.record_safety_review(request) == pending
    assert store.get_safety_review_operation(h.version, request.request_id) == receipt


@pytest.mark.parametrize("lost_ack", ["before", "after"])
def test_native_discovery_lost_ack_recovers_only_the_original_handoff(lost_ack):
    h = Harness()
    db = AbiDatabase(h, principal="web")
    store = AzureSqlMonitoringStore(db=db, component="web")
    request_id = h.next_id()
    selector = m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=uid(100))
    db.fail_commit = lost_ack
    with pytest.raises(MonitoringCommitUncertain):
        store.request_discovery(h.version, selector, request_id=request_id)
    count = len(db.records)
    recovered = store.request_discovery(h.version, selector, request_id=request_id)
    assert recovered.kind == "reconcile_state" and recovered.reconcile_request_id == request_id
    assert count == (0 if lost_ack == "before" else len(db.records))
    assert len([row for row in db.records.values() if row.kind == "work"]) == 1


@pytest.mark.parametrize("short_insert", [False, True])
def test_native_inventory_accepts_all_parts_atomically_under_one_work_fence(short_insert):
    h = Harness()
    db = AbiDatabase(h, principal="controller")
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = controller.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=0,
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"), reason="Owned native collection.",
        created_at=h.clock(), due_at=h.clock(),
    ))
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    owned = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(99), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))[0]
    items = tuple(m.InventoryItem(
        **h.context(), generation_id=work.work_id, workspace_id=uid(100), item_id=uid(1_000 + index),
        workload="fabric_pipeline", item_type="DataPipeline", name=f"Fixture {index}", observed_at=h.clock(),
    ) for index in range(110))
    batch = m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=items,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=work.work_id, selector=work.discovery_selector, adapter="fixture",
            authority="tenant_admin", completeness="complete", started_at=h.clock(), completed_at=h.clock(),
            discovered_count=len(items), completed_pages=1,
        ),
        commit=m.InventoryCommit(work_id=owned.work_id, lease=owned.lease, expected_work_revision=owned.revision,
                                 expected_generation_revision=0),
    )
    if short_insert:
        before = deepcopy((db.records, db.receipts))
        execute = db.execute

        def incomplete(sql, *params):
            changed = execute(sql, *params)
            return changed - 1 if sql.startswith("INSERT") and len(params) > 19 else changed

        db.execute = incomplete
        with pytest.raises(MonitoringConflict, match="every checked row"):
            worker.record_inventory(batch)
        assert (db.records, db.receipts) == before
        return
    result = worker.record_inventory(batch)
    assert result.recorded_item_count == 110
    parts = [entry for (name, _), entry in db.receipts.items() if name == "worker.accept_facts"]
    assert len(parts) == 2
    assert all(len(entry["payload"]["result"]["facts"]) <= 200 for entry in parts)
    assert not any(row.kind in {"target", "target_capability", "source", "source_head"} for row in db.records.values())
    assert worker.record_inventory(batch) == result
    assert len(db.receipts) == 3  # enqueue + the two atomic intake parts
    inserts = [
        params for method, sql, params in db.calls
        if method == "execute" and sql.startswith("INSERT")
        and ("worker_catalogue" in sql or "worker_evidence" in sql)
    ]
    assert inserts and max(map(len, inserts)) <= 50 * 19
    assert len(inserts) < 10  # 220 catalogue rows must not cause 220 write round trips.


def test_native_targetless_discovery_reconciles_through_owned_frontier_without_action_lease():
    h = Harness()
    db = AbiDatabase(h, principal="web")
    web = AzureSqlMonitoringStore(db=db, component="web")
    queued = web.request_discovery(h.version, m.ScopeSelector(tenant_id=uid(1), kind="tenant"), request_id=h.next_id())
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(501), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    assert work.work_id == queued.work_id and work.target is None
    result = controller.reconcile_work(work)
    assert result.state == "published"
    assert controller.get_work(h.version, work.work_id).state == "completed"
    assert any(row.work_kind == "inventory" for row in db.records.values())
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 1})
    assert controller.reconcile_work(work) == result
    assert not any("controller:" in str(params) for _, _, params in db.calls)


def test_native_partial_window_resolution_waits_instead_of_completing_work():
    h = Harness()
    db = AbiDatabase(h, principal="controller")
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    draft = m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=0,
        discovery_selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        created_at=h.clock(), due_at=h.clock(), reason="Partial scope enumeration.",
    )
    controller.enqueue_work(draft)
    db.principal = "worker"
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    owned = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(502), kinds=("inventory",), limit=1, per_workspace_limit=1,
    ))[0]
    worker.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=draft.work_id, selector=draft.discovery_selector,
            adapter="fixture", authority="tenant_admin", completeness="partial", started_at=h.clock(),
            continuation="next", gaps=(m.CoverageGap(code="inventory_in_progress", detail="More pages remain."),),
        ),
        commit=m.InventoryCommit(work_id=owned.work_id, lease=owned.lease, expected_work_revision=owned.revision,
                                 expected_generation_revision=0),
    ))
    db.principal = "controller"
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(503), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    result = controller.reconcile_work(work)
    assert result.state == "pending_validation"
    assert controller.get_work(h.version, work.work_id).state == "waiting"
    assert controller.get_validation_frontier(h.version, result.frontier_key).pending


def test_native_powerbi_pages_stage_conflicts_before_any_source_publication():
    h, db = seeded("powerbi")
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    poll = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(504), kinds=("poll",), limit=1, per_workspace_limit=1,
    ))[0]
    window = m.ObservationWindow(start_at=h.clock() - timedelta(minutes=30), end_at=h.clock())
    requests = []
    for index, request_guid in enumerate((uid(600_001), uid(600_002))):
        source = h.observation(execution={
            "target": h.targets[0], "run_id_kind": "powerbi_request", "run_id": request_guid,
        })
        request = m.RestPageRequest(
            page_id=h.next_id(), target=h.targets[0], policy_revision=h.version.revision,
            poll_work_id=poll.work_id, lease=poll.lease, expected_checkpoint_revision=index,
            expected_cursor=None if index == 0 else "next", next_cursor="next" if index == 0 else None,
            window=window, received_count=1, observed_at=h.clock(),
            powerbi_rows=(m.PowerBIWindowRow(observation=source, refresh_id="23"),),
            powerbi_window_complete=index == 1, window_complete=index == 1,
        )
        receipt = worker.record_rest_page(request)
        assert receipt.intake.publication_status == "pending_validation"
        requests.append((request, receipt))
    assert worker.get_work(h.version, poll.work_id).state == "completed"
    assert not any(row.kind in {"source", "source_head", "powerbi_alias"} for row in db.records.values())
    assert worker.get_rest_page(h.version, requests[0][0].page_id) == requests[0][1]
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    results = []
    for _ in range(2):
        work = controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(505), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
        ))[0]
        results.append(controller.reconcile_work(work))
    assert "rejected" in {result.state for result in results}
    assert not any(row.kind in {"source", "source_head"} for row in db.records.values())
    staged = controller.get_powerbi_window(h.version, requests[-1][1].checkpoint.powerbi_window_id)
    assert staged.state == "quarantined" and staged.quarantined_count == 2


def test_native_raw_commit_failure_rolls_back_facts_handoff_frontier_and_progress():
    h, db = seeded()
    worker = AzureSqlMonitoringStore(db=db, component="worker")
    work = worker.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(507), kinds=("poll",), limit=1, per_workspace_limit=1,
    ))[0]
    before = dict(db.records)
    request = m.RestPageRequest(
        page_id=h.next_id(), target=h.targets[0], policy_revision=h.version.revision,
        poll_work_id=work.work_id, lease=work.lease, expected_checkpoint_revision=0,
        window=m.ObservationWindow(start_at=h.clock() - timedelta(minutes=30), end_at=h.clock()),
        received_count=1, observations=(h.observation(),), window_complete=True, observed_at=h.clock(),
    )
    db.fail_after_native = "worker.accept_facts"
    with pytest.raises(MonitoringConflict):
        worker.record_rest_page(request)
    assert db.records == before
    assert worker.get_poll_progress(h.targets[0]) is None
    assert worker.get_rest_page(h.version, request.page_id) is None
    assert worker.get_work(h.version, work.work_id).state == "leased"


class BudgetAbiDatabase(AbiDatabase):
    def __init__(self, h):
        super().__init__(h, principal="worker")
        self.budgets = {
            "tenant": {"used": 0, "request_limit": 2, "window_seconds": 60},
            "workspace": {"used": 0, "request_limit": 2, "window_seconds": 60},
        }

    @contextmanager
    def transaction(self):
        before = deepcopy(self.budgets)
        failure = self.fail_commit
        try:
            with super().transaction():
                yield self
        except BaseException as exc:
            if not (isinstance(exc, SqlCommitUncertain) and failure == "after"):
                self.budgets = before
            raise

    def apply_rpc(self, operation, args):
        if operation != "worker.rate_budget":
            return super().apply_rpc(operation, args)
        value = self.budgets[args["bucket"]]
        allowed = value["used"] < value["request_limit"] and args["delay_seconds"] is None
        if allowed:
            value["used"] += 1
        result = {
            **value, "allowed": allowed, "window_ends_at": (self.clock() + timedelta(seconds=60)).isoformat(),
            "blocked_until": (
                (self.clock() + timedelta(seconds=args["delay_seconds"])).isoformat()
                if args["delay_seconds"] is not None else None
            ),
        }
        return {
            "kernel_version": KERNEL_VERSION, "operation": operation, "status": "applied" if allowed else "not_acquired",
            "affected_rows": int(allowed or args["delay_seconds"] is not None), "result": result,
        }


def test_kernel_paired_budget_denial_rolls_back_every_debit_without_base_dml():
    h = Harness()
    db = BudgetAbiDatabase(h)
    db.budgets["workspace"]["used"] = 2
    budget = KernelRateBudget(db)
    before = deepcopy(db.budgets)
    decision = budget.acquire_many(h.version, (("tenant", RatePolicy(2, 60)), ("workspace", RatePolicy(2, 60))))
    assert not decision.allowed and decision.retry_at > h.clock()
    assert db.budgets == before
    assert not any(method == "execute" for method, _, _ in db.calls)


def test_kernel_budget_cannot_choose_or_reset_provisioned_policy():
    h = Harness()
    db = BudgetAbiDatabase(h)
    budget = KernelRateBudget(db)
    with pytest.raises(MonitoringConflict, match="provisioned"):
        budget.acquire(h.version, "tenant", RatePolicy(100, 60))
    assert db.budgets["tenant"]["used"] == 0


def test_kernel_budget_uncertain_commit_keeps_debit_spent():
    h = Harness()
    db = BudgetAbiDatabase(h)
    budget = KernelRateBudget(db)
    db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        budget.acquire(h.version, "tenant", RatePolicy(2, 60))
    assert db.budgets["tenant"]["used"] == 1
