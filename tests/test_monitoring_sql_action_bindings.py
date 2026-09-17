from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
import test_monitoring_sql_retry_finalization as fragments
from test_monitoring_sql_abi import AbiDatabase
from test_monitoring_sql_store import DriverRow
from test_monitoring_store import Harness, rejection_request, uid

from triage.models import Incident
from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.monitoring.memory import canonical_incident_id, key_digest
from triage.monitoring.sql_kernel_contracts import KERNEL_VERSION
from triage.monitoring.sql_kernel_history import historical_payload_expression, historical_predicate
from triage.monitoring.sql_kernel_retries import retry_admission_predicate, successor_predicate
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.monitoring.sql_store import AzureSqlMonitoringStore
from triage.store.azure_sql import SqlCommitUncertain
from triage.store.retries import MAX_ATTEMPTS, backoff_seconds


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def test_active_kernel_declares_retry_and_finalization_result_delta_without_case_fallbacks():
    kernel = build_permission_kernel()
    assert "retry_work" in kernel.rpcs["controller.transition_action"].result_fields
    assert "source_disposition" in kernel.rpcs["controller.finalize"].result_fields
    for operation in ("controller.enqueue_work", "controller.reserve_action", "controller.finalize"):
        assert kernel.rpcs[operation].implemented
        assert not kernel.rpcs[operation].blocked_cases


class ActionAbiDatabase(AbiDatabase):
    """Stable RPC result/caller harness with the production retry/history predicates."""

    def __init__(self, h):
        super().__init__(h, principal="controller")
        self.seed_published_fixture(h)
        self.incidents = deepcopy(h.state.incidents)
        self.approvals = deepcopy(h.state.approvals)
        self.processed = set(h.state.processed)
        self.original_payloads = {}
        self.change_receipt_bytes = False
        self.fail_finalize_receipt = False
        self.bad_transition_result = None
        self.interrupt_after_mutation = None
        self.guards = sqlite3.connect(":memory:", check_same_thread=False)
        self.guards.create_function("JSON_VALUE", 2, fragments._value)
        self.guards.create_function("JSON_QUERY", 2, fragments._query)
        self.guards.create_function("JSON_MODIFY", 3, fragments._modify)
        self.guards.create_function("TRY_CONVERT", 2, fragments._convert)
        self.guards.create_function("TODATETIMEOFFSET", 2, lambda value, _: fragments._convert("datetimeoffset", value))
        self.guards.create_collation("Latin1_General_100_BIN2", lambda left, right: (left > right) - (left < right))

    @contextmanager
    def transaction(self):
        before = deepcopy((self.incidents, self.approvals, self.processed, self.original_payloads))
        failure = self.fail_commit
        try:
            with super().transaction():
                yield self
        except BaseException as exc:
            if not (isinstance(exc, SqlCommitUncertain) and failure == "after"):
                self.incidents, self.approvals, self.processed, self.original_payloads = before
            raise

    def query(self, sql, *params):
        if sql.startswith("SELECT JSON_QUERY(payload, '$.result.incident')"):
            self.calls.append(("query", sql, params))
            payload = self.original_payloads.get(params[-1])
            return [DriverRow((payload,))] if payload is not None else []
        if sql.startswith("SELECT payload FROM") and "incident_read" in sql:
            self.calls.append(("query", sql, params))
            payload = self.incidents.get(params[0])
            return [DriverRow((payload,))] if payload is not None else []
        return super().query(sql, *params)

    def model(self, kind, key, model):
        row = self.records.get((kind, key))
        return model.model_validate_json(row.payload) if row else None

    def admission(self, action):
        request = action.request
        target = self.model("target", request.source_execution.target.key, m.MonitoringTarget)
        review = self.model("review", request.review_id, m.SafetyReview)
        capability = self.model("target_capability", request.source_execution.target.key, m.CapabilityObservation)
        values = {
            "retry_request": canonical(request.model_dump(mode="json")),
            "retry_target": canonical(target.model_dump(mode="json")) if target else None,
            "retry_review": canonical(review.model_dump(mode="json")) if review else None,
            "retry_capability": canonical(capability.model_dump(mode="json")) if capability else None,
            "current_revision": self.control.revision, "maintenance": int(self.control.maintenance),
            "now": self.clock().isoformat(),
        }
        return bool(fragments._eval(self.guards, retry_admission_predicate(), values))

    def reply(self, operation, args, result):
        binding = self.rpc_binding(args)
        self.receipts[(operation, args["request_id"])] = {
            "fingerprint": args["fingerprint"], "recorded_at": self.clock(),
            "payload": {"binding_hash": binding, "result": result},
        }
        return {"kernel_version": KERNEL_VERSION, "operation": operation, "status": "applied", "affected_rows": 1, "result": result}

    def apply_rpc(self, operation, args):
        if operation == "controller.inspect_frontiers":
            return {
                "kernel_version": KERNEL_VERSION, "operation": operation, "status": "read", "affected_rows": 0,
                "result": {"target_key": args["target_key"], "frontier_digest": "C" * 64, "pending": False, "frontiers": []},
            }
        if operation == "controller.enqueue_work":
            draft = m.MonitoringWorkDraft.model_validate_json(args["draft_json"])
            if draft.kind == "deferred_retry":
                work = self.model("work", draft.work_id, m.MonitoringWork)
                parent = self.model("action", work.retry_of, m.ActionReservation) if work and work.retry_of else None
                if (
                    work is None or parent is None or parent.state != "rejected"
                    or parent.retry_work_id != work.work_id or work.execution != draft.execution
                    or work.target != draft.target
                ):
                    raise RuntimeError("Deferred lookup requires recorded exact successor (51072)")
                return self.reply(operation, args, {"work_id": work.work_id, "work": work.model_dump(mode="json")})
        if operation not in {"controller.transition_action", "controller.finalize", "controller.reserve_action"}:
            return super().apply_rpc(operation, args)
        work = self.model("work", args["work_id"], m.MonitoringWork)
        if (
            work is None or work.lease is None or work.lease.owner_id != args["owner_id"]
            or work.lease.fence != args["fence"] or work.revision != args["work_revision"]
            or work.lease.expires_at <= self.clock()
        ):
            raise RuntimeError("Original work fence no longer owns this transition (51074)")
        if operation == "controller.transition_action":
            return self.transition_action(args, work)
        if operation == "controller.reserve_action":
            return self.reserve(args, work)
        return self.finalize(args, work)

    def reserve(self, args, work):
        value = json.loads(args["reservation_json"])
        request = m.ActionReservationRequest.model_validate(value["request"])
        if (
            work.action_reservation_id is not None or request.expected.revision != self.control.revision
            or work.policy_revision != self.control.revision or request.source_execution != work.execution
        ):
            raise RuntimeError("Current action work or source differs (51072)")
        budget = self.model("incident_state", request.incident.key, m.IncidentState)
        if budget is None or request.expected_incident_revision != budget.revision:
            raise RuntimeError("Incident budget revision changed (51072)")
        parent = self.model("action", work.retry_of, m.ActionReservation) if work.retry_of else None
        if parent is None:
            raise RuntimeError("This test path requires the exact recorded successor (51072)")
        parent_work = self.model("work", parent.request.work_id, m.MonitoringWork)
        valid = fragments._eval(self.guards, successor_predicate(), {
            "request": canonical(request.model_dump(mode="json")),
            "retry_parent": canonical(parent.model_dump(mode="json")),
            "retry_parent_work": canonical(parent_work.model_dump(mode="json")),
            "retry_attempt": work.retry_attempt, "work_id": work.work_id, "retry_of": work.retry_of,
        })
        if not valid or ("controller.finalize", parent_work.finalization_id) not in self.receipts:
            raise RuntimeError("Successor must be unused, exact and durably finalized (51072)")
        owner = json.loads(self.records[("action_owner", work.target.key)].payload)
        if owner["active"] or budget.action_count != 1:
            raise RuntimeError("Occupied target or lost incident slot (51072)")
        if request.approval is not None:
            approval = self.approvals[request.approval.approval_id]
            if approval.get("consumed_at"):
                raise RuntimeError("Consumed approval cannot be reused (51072)")
            approval["consumed_at"] = self.clock().isoformat()
        action = m.ActionReservation(
            reservation_id=args["reservation_id"], request=request, revision=1, fence=owner["fence"] + 1,
            state="reserved", reserved_at=self.clock(), updated_at=self.clock(),
            retry_attempt=work.retry_attempt, retry_of=work.retry_of,
            next_verification_at=self.clock() + timedelta(seconds=120),
            detail="Guarded linked successor retained the occupied incident slot.",
        )
        self.native_put("action", action.reservation_id, action.model_dump(mode="json"),
                        status="reserved", target_key=work.target.key)
        self.native_put("action_owner", work.target.key, {
            "reservation_id": action.reservation_id, "fence": action.fence, "active": True,
        })
        self.native_put("incident_state", request.incident.key, {
            **budget.model_dump(mode="json"), "revision": budget.revision + 1,
        })
        self.native_put("action", parent.reservation_id, {
            **parent.model_dump(mode="json"), "retry_reservation_id": action.reservation_id,
            "revision": parent.revision + 1,
        }, status="rejected", target_key=work.target.key)
        self.save_work(m.MonitoringWork.model_validate({
            **work.model_dump(), "action_reservation_id": action.reservation_id, "revision": work.revision + 1,
        }))
        return self.reply("controller.reserve_action", args, {
            "reservation_id": action.reservation_id, "reservation": action.model_dump(mode="json"),
        })

    def transition_action(self, args, work):
        action = self.model("action", args["reservation_id"], m.ActionReservation)
        if (
            action is None or action.revision != args["expected_action_revision"]
            or action.reservation_id != work.action_reservation_id
            or action.request.source_execution != work.execution
        ):
            raise RuntimeError("Action lineage mismatch (51072)")
        assert args["transition"] == "rejected"
        change = json.loads(args["transition_json"])
        if action.state != "reserved" or action.submitted_execution is not None or action.submitted_at is not None:
            raise RuntimeError("Accepted/uncertain effects cannot be rejected (51072)")
        evidence = m.ActionRejectionEvidence.model_validate(change["rejection"])
        successor = None
        if evidence.reason == "throttled" and action.retry_attempt < MAX_ATTEMPTS and self.admission(action):
            self.next_identifier += 1
            wait = evidence.retry_after_seconds or backoff_seconds(action.retry_attempt + 1)
            successor = m.MonitoringWork(
                **self.context.model_dump(), work_id=uid(self.next_identifier), kind="deferred_retry",
                policy_revision=self.control.revision, created_at=self.clock(), due_at=self.clock() + timedelta(seconds=wait),
                target=work.target, execution=work.execution, retry_of=action.reservation_id,
                retry_attempt=action.retry_attempt + 1, revision=1, attempts=0, state="queued",
                reason="Single linked retry after confirmed no-effect throttling rejection",
            )
            self.save_work(successor)
            self.native_put("source_work", f"deferred_retry:{work.execution.key}", {
                "work_id": successor.work_id, "execution": work.execution.model_dump(mode="json"),
            }, parent_key=work.execution.key, target_key=work.target.key)
        saved = m.ActionReservation.model_validate({
            **action.model_dump(), "revision": action.revision + 1, "state": "rejected",
            "rejection": evidence, "retry_work_id": successor.work_id if successor else None,
            "next_verification_at": None, "updated_at": self.clock(), "detail": change["detail"],
        })
        self.native_put("action", action.reservation_id, saved.model_dump(mode="json"),
                        status="rejected", target_key=work.target.key)
        owner = json.loads(self.records[("action_owner", work.target.key)].payload)
        self.native_put("action_owner", work.target.key, {**owner, "active": False}, target_key=work.target.key)
        for row in tuple(self.records.values()):
            if row.kind != "work" or row.work_kind != "verify_action" or row.status not in {"queued", "waiting"}:
                continue
            followup = m.MonitoringWork.model_validate_json(row.payload)
            if followup.action_reservation_id == action.reservation_id:
                self.save_work(m.MonitoringWork.model_validate({
                    **followup.model_dump(), "state": "dispositioned", "completed_at": self.clock(),
                    "revision": followup.revision + 1, "disposition": "Confirmed no effect.",
                }))
        result = {
            "reservation_id": saved.reservation_id, "reservation": saved.model_dump(mode="json"),
            "retry_work": successor.model_dump(mode="json") if successor else None,
        }
        if self.bad_transition_result == "missing":
            result.pop("retry_work")
        elif self.bad_transition_result == "wrong_parent":
            result["retry_work"]["retry_of"] = uid(899_999)
        reply = self.reply("controller.transition_action", args, result)
        if self.interrupt_after_mutation is not None:
            raise self.interrupt_after_mutation
        return reply

    def finalize(self, args, work):
        reference = json.loads(args["finalization_json"])
        row = self.records[("finalization_plan", reference["plan_key"])]
        assert hashlib.sha256(row.payload.encode("utf-16-le")).hexdigest().upper() == reference["plan_hash"]
        plan = m.FinalizationPlan.model_validate_json(row.payload)
        prior = self.incidents.get(plan.incident_id)
        current_hash = hashlib.sha256(prior.encode("utf-16-le")).hexdigest() if prior is not None else None
        if current_hash != plan.prior_incident_hash:
            raise RuntimeError("Original NVARCHAR payload CAS lost (51072)")
        state = self.model("incident_state", plan.incident_key, m.IncidentState)
        historical = bool(fragments._eval(self.guards, historical_predicate(), {
            "budget": canonical(state.model_dump(mode="json")) if state else None,
            "plan": row.payload,
        }))
        occurrence_key = f"{plan.incident_key}:occurrence:{key_digest(plan.source_key)}"
        recorded = (
            ("source_disposition", plan.source_key) in self.records
            or ("incident_occurrence", occurrence_key) in self.records or key_digest(plan.source_key) in self.processed
        )
        count = json.loads(prior)["occurrence_count"] + int(not recorded and work.kind != "verify_action") if prior else max(1, plan.merged_incident.occurrence_count)
        if historical:
            payload = self.guards.execute(
                "SELECT " + historical_payload_expression(), {"prior": prior, "next_occurrences": count},
            ).fetchone()[0]
        else:
            body = plan.merged_incident.model_dump(mode="json")
            body["occurrence_count"] = count
            if prior:
                previous = json.loads(prior)
                body.update(
                    first_seen_at=previous["first_seen_at"],
                    last_seen_at=max(previous["last_seen_at"], body["last_seen_at"]),
                    notified_count=max(previous["notified_count"], body["notified_count"]),
                )
            payload = canonical(body)
        self.incidents[plan.incident_id] = payload
        self.original_payloads[args["finalization_id"]] = (
            json.dumps(json.loads(payload), indent=2) if self.change_receipt_bytes else payload
        )
        self.native_put("incident_occurrence", occurrence_key, {
            "incident_id": plan.incident_id, "source_key": plan.source_key, "first_work_id": work.work_id,
            "historical": historical, "recorded_at": self.clock().isoformat(),
        })
        budget = m.IncidentState(
            identity=plan.incident_identity, incident_id=plan.incident_id,
            revision=state.revision + 1 if state else 1, action_count=state.action_count if state else 0,
            latest_execution=state.latest_execution if historical else plan.source_execution,
            latest_started_at=state.latest_started_at if historical else plan.source_started_at,
            updated_at=self.clock(),
        )
        self.native_put("incident_state", plan.incident_key, budget.model_dump(mode="json"), target_key=work.target.key)
        action = self.model("action", work.action_reservation_id, m.ActionReservation) if work.action_reservation_id else None
        pending = action is not None and action.state in {"reserved", "submitted", "uncertain"}
        disposition = "historical" if historical else plan.source_disposition
        if not pending:
            if ("source_disposition", plan.source_key) not in self.records:
                self.native_put("source_disposition", plan.source_key, {
                    "execution": plan.source_execution.model_dump(mode="json"), "disposition": disposition,
                    "work_id": work.work_id, "finalization_id": args["finalization_id"], "recorded_at": self.clock().isoformat(),
                }, status=disposition, target_key=work.target.key)
            self.processed.add(key_digest(plan.source_key))
        self.save_work(m.MonitoringWork.model_validate({
            **work.model_dump(), "revision": work.revision + 1,
            "state": "finalizing" if pending else "completed", "lease": work.lease if pending else None,
            "completed_at": None if pending else self.clock(),
            "finalization_id": None if pending else args["finalization_id"],
        }))
        result = {
            "work_id": work.work_id, "finalization_id": args["finalization_id"], "incident_id": plan.incident_id,
            "state": "persisted_waiting_verification" if pending else "completed",
            "incident": json.loads(payload), "source_disposition": disposition,
        }
        reply = self.reply("controller.finalize", args, result)
        if self.interrupt_after_mutation is not None:
            raise self.interrupt_after_mutation
        if self.fail_finalize_receipt:
            self.fail_finalize_receipt = False
            raise RuntimeError("Injected original receipt insertion failure (51072)")
        return reply


def reserved(*, arguments=None):
    h = Harness()
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(
        h.review("powerbi_refresh"), arguments={} if arguments is None else arguments,
    )).reservation
    db = ActionAbiDatabase(h)
    return h, db, AzureSqlMonitoringStore(db=db, component="controller"), action


def finalization(h, db, *, source=None, work=None, request_id=None, outcome="needs_human"):
    source = source or h.source
    work = work or db.model("work", h.work.work_id, m.MonitoringWork)
    identity = m.IncidentIdentity(target=source.execution.target, signature="fixture-failure")
    now = h.clock()
    incident = Incident(
        id="candidate-id", signature=identity.signature, status="investigating",
        first_seen_at=now.isoformat(), last_seen_at=now.isoformat(), outcome=outcome, summary="Candidate source evidence.",
    )
    return m.WorkFinalizationRequest(
        **h.context(), finalization_id=request_id or h.next_id(), work_id=work.work_id,
        expected_work_revision=work.revision, lease=work.lease, source_execution=source.execution,
        incident_identity=identity, incident=incident, source_disposition="triaged",
        action_reservation_id=work.action_reservation_id,
    )


def test_rejection_returns_only_the_native_linked_successor_and_keeps_budget_approval():
    h, db, store, action = reserved()
    approvals = deepcopy(db.approvals)
    request = rejection_request(h, action, retry_after=31)
    rejected = store.record_action_rejection(request)
    assert rejected.state == "rejected" and rejected.submitted_execution is None
    retry = store.get_work(h.version, rejected.retry_work_id)
    assert retry.retry_of == action.reservation_id and retry.retry_attempt == 1
    assert retry.due_at == h.clock() + timedelta(seconds=31)
    assert store.get_incident_state(action.request.incident).action_count == 1
    assert db.approvals == approvals
    assert not json.loads(db.records[("action_owner", action.request.source_execution.target.key)].payload)["active"]
    assert not any("controller_enqueue_work" in sql for _, sql, _ in db.calls)
    assert store.record_action_rejection(request) == rejected
    assert store.get_operation_receipt(h.version, "action_rejection", request.request_id).result["retry_work_id"] == retry.work_id


@pytest.mark.parametrize("change", ["revoked", "maintenance", "cap", "non_throttled"])
def test_ineligible_rejection_commits_without_a_successor_or_refund(change):
    h, db, store, action = reserved()
    request = rejection_request(h, action)
    if change == "revoked":
        review = db.model("review", action.request.review_id, m.SafetyReview)
        db.native_put("review", review.review_id, {
            **review.model_dump(mode="json"), "state": "revoked", "revoked_at": h.clock().isoformat(),
        })
    elif change == "maintenance":
        db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "maintenance": True})
    elif change == "cap":
        action = m.ActionReservation.model_validate({**action.model_dump(), "retry_attempt": MAX_ATTEMPTS})
        db.records[("action", action.reservation_id)] = replace(
            db.records[("action", action.reservation_id)], payload=action.model_dump_json(),
        )
    else:
        request = m.ActionRejectionRequest.model_validate({
            **request.model_dump(), "evidence": {**request.evidence.model_dump(), "reason": "definitive_client_error"},
        })
    rejected = store.record_action_rejection(request)
    assert rejected.retry_work_id is None
    assert store.get_incident_state(action.request.incident).action_count == 1
    assert all(value.get("consumed_at") for value in db.approvals.values())


@pytest.mark.parametrize("state", ["submitted", "uncertain"])
def test_accepted_or_uncertain_effect_cannot_enter_no_effect_rejection(state):
    h, db, store, action = reserved()
    submitted = m.SourceExecutionIdentity(target=action.request.source_execution.target,
                                         run_id=uid(810_000), run_id_kind="powerbi_request")
    value = m.ActionReservation.model_validate({
        **action.model_dump(), "state": state, "submitted_execution": submitted,
        "submitted_at": h.clock(), "next_verification_at": h.clock() + timedelta(seconds=120),
    })
    db.records[("action", action.reservation_id)] = replace(
        db.records[("action", action.reservation_id)], payload=value.model_dump_json(), status=state,
    )
    with pytest.raises(MonitoringConflict):
        store.record_action_rejection(rejection_request(h, action))
    assert not any("controller_transition_action" in sql for _, sql, _ in db.calls)


@pytest.mark.parametrize("failure", ["before", "after"])
def test_lost_rejection_ack_does_not_create_a_second_successor(failure):
    h, db, store, action = reserved()
    request = rejection_request(h, action)
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain):
        store.record_action_rejection(request)
    if failure == "after":
        db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2})
    saved = store.record_action_rejection(request)
    assert saved.retry_work_id is not None
    assert len([row for row in db.records.values() if row.work_kind == "deferred_retry"]) == 1
    assert store.get_incident_state(action.request.incident).action_count == 1


def test_deferred_enqueue_retrieves_exact_recorded_successor_without_new_work():
    h, db, store, action = reserved()
    rejected = store.record_action_rejection(rejection_request(h, action))
    retry = store.get_work(h.version, rejected.retry_work_id)
    draft = m.MonitoringWorkDraft.model_validate(retry.model_dump(include=set(m.MonitoringWorkDraft.model_fields)))
    before = deepcopy(db.records)
    assert store.enqueue_work(draft) == retry
    assert db.records == before
    with pytest.raises(MonitoringConflict):
        store.enqueue_work(m.MonitoringWorkDraft.model_validate({**draft.model_dump(), "work_id": uid(820_000)}))


def test_rejected_parent_finalizes_before_its_successor_and_preserves_slot():
    h, db, store, action = reserved()
    rejected = store.record_action_rejection(rejection_request(h, action))
    request = finalization(h, db)
    receipt = store.finalize_work(request)
    assert receipt.state == "completed"
    assert store.get_work(h.version, request.work_id).finalization_id == request.finalization_id
    assert receipt.incident_payload_hash == hashlib.sha256(db.incidents[receipt.incident_id].encode("utf-16-le")).hexdigest()
    assert store.get_source_disposition(request.source_execution).disposition == "triaged"
    assert store.get_incident_state(action.request.incident).action_count == 1
    assert store.get_work(h.version, rejected.retry_work_id).state == "queued"


def successor_request(h, db, store, action, *, finalize_parent):
    rejected = store.record_action_rejection(rejection_request(h, action, retry_after=1))
    if finalize_parent:
        store.finalize_work(finalization(h, db))
    retry = store.get_work(h.version, rejected.retry_work_id)
    h.clock.advance(2)
    lease = m.LeaseToken(
        **h.context(), resource_key=retry.key, owner_id=uid(840_000), fence=1,
        acquired_at=h.clock(), expires_at=h.clock() + timedelta(seconds=120),
    )
    claimed = m.MonitoringWork.model_validate({
        **retry.model_dump(), "lease": lease, "state": "leased", "revision": retry.revision + 1,
    })
    db.save_work(claimed)
    budget = store.get_incident_state(action.request.incident)
    return m.ActionReservationRequest.model_validate({
        **action.request.model_dump(), "idempotency_id": h.next_id(), "work_id": claimed.work_id,
        "lease": claimed.lease, "approval": None, "expected_incident_revision": budget.revision,
    })


def test_successor_reservation_retains_slot_and_links_exact_predecessor_once():
    h, db, store, action = reserved()
    request = successor_request(h, db, store, action, finalize_parent=True)
    approvals = deepcopy(db.approvals)
    result = store.reserve_action(request)
    assert result.reservation.retry_of == action.reservation_id
    assert result.reservation.retry_attempt == 1
    assert store.get_incident_state(action.request.incident).action_count == 1
    assert db.approvals == approvals
    parent = store.get_action_reservation(h.version, action.reservation_id)
    assert parent.retry_reservation_id == result.reservation.reservation_id
    assert store.reserve_action(request) == result
    with pytest.raises(MonitoringConflict):
        store.reserve_action(m.ActionReservationRequest.model_validate({
            **request.model_dump(), "idempotency_id": h.next_id(),
            "expected_incident_revision": store.get_incident_state(action.request.incident).revision,
        }))


def test_sql_successor_keeps_its_new_justification_without_changing_technical_identity():
    h, db, store, action = reserved(arguments={"justification": "Original transient refresh failure."})
    request = successor_request(h, db, store, action, finalize_parent=True)
    arguments = {"justification": "The persisted throttling interval has elapsed."}
    request = m.ActionReservationRequest.model_validate({**request.model_dump(), "arguments": arguments})
    result = store.reserve_action(request)
    assert result.reservation.request == request
    assert result.reservation.request.arguments == arguments != action.request.arguments
    assert m._digest(arguments) != m._digest(action.request.arguments)
    assert result.reservation.request.source_execution == action.request.source_execution
    assert result.reservation.request.parameter_hash == action.request.parameter_hash
    assert result.reservation.retry_of == action.reservation_id
    assert db.model("action", result.reservation.reservation_id, m.ActionReservation).request.arguments == arguments
    assert store.get_incident_state(action.request.incident).action_count == 1


@pytest.mark.parametrize("reason", ["unfinished_parent", "changed_source", "approval_reuse"])
def test_successor_cannot_bypass_parent_finalization_exact_source_or_consumed_approval(reason):
    h, db, store, action = reserved()
    request = successor_request(h, db, store, action, finalize_parent=reason != "unfinished_parent")
    if reason == "changed_source":
        other = m.SourceExecutionIdentity(
            target=request.source_execution.target, run_id=uid(840_001), run_id_kind="powerbi_request",
        )
        request = m.ActionReservationRequest.model_validate({**request.model_dump(), "source_execution": other})
    elif reason == "approval_reuse":
        request = m.ActionReservationRequest.model_validate({**request.model_dump(), "approval": action.request.approval})
    before = deepcopy((db.records, db.incidents, db.approvals))
    with pytest.raises(MonitoringConflict):
        store.reserve_action(request)
    assert (db.records, db.incidents, db.approvals) == before
    assert store.get_incident_state(action.request.incident).action_count == 1


def test_historical_finalization_preserves_newer_terminal_evidence_head_and_original_receipt():
    h, db, store, action = reserved()
    work = db.model("work", h.work.work_id, m.MonitoringWork)
    db.save_work(m.MonitoringWork.model_validate({**work.model_dump(), "action_reservation_id": None}))
    identity = action.request.incident
    incident_id = canonical_incident_id(identity)
    newer_execution = m.SourceExecutionIdentity(target=identity.target, run_id=uid(830_000), run_id_kind="powerbi_request")
    prior = Incident(
        id=incident_id, signature=identity.signature, status="resolved", outcome="resolved",
        first_seen_at=(h.clock() - timedelta(hours=1)).isoformat(), last_seen_at=h.clock().isoformat(),
        occurrence_count=4, notified_count=1, summary="Newer verified terminal evidence.",
    )
    original_document = {**prior.model_dump(mode="json"), "future_tracking_field": {"fixture": "preserve verbatim values"}}
    db.incidents[incident_id] = json.dumps(original_document, indent=2)
    old_state = store.get_incident_state(identity)
    db.native_put("incident_state", identity.key, {
        **old_state.model_dump(mode="json"), "revision": old_state.revision + 1,
        "latest_execution": newer_execution.model_dump(mode="json"), "latest_started_at": h.clock().isoformat(),
    }, target_key=identity.target.key)
    budget_before = store.get_incident_state(identity)
    request = finalization(h, db)
    receipt = store.finalize_work(request)
    saved = store.get_incident(identity)
    assert receipt.source_disposition == "historical"
    assert saved.occurrence_count == 5
    assert saved.model_dump(exclude={"occurrence_count"}) == prior.model_dump(exclude={"occurrence_count"})
    assert json.loads(db.incidents[incident_id])["future_tracking_field"] == original_document["future_tracking_field"]
    budget = store.get_incident_state(identity)
    assert budget.action_count == budget_before.action_count
    assert budget.latest_execution == budget_before.latest_execution and budget.latest_started_at == budget_before.latest_started_at
    current_work = db.model("work", request.work_id, m.MonitoringWork)
    repeated_id = h.next_id()
    repeated = m.MonitoringWork.model_validate({
        **current_work.model_dump(), "work_id": repeated_id, "state": "leased", "revision": 1,
        "finalization_id": None, "completed_at": None,
        "lease": m.LeaseToken(
            **h.context(), resource_key=m.work_key(h.version, repeated_id), owner_id=uid(830_001), fence=1,
            acquired_at=h.clock(), expires_at=h.clock() + timedelta(seconds=120),
        ),
    })
    db.save_work(repeated)
    duplicate = store.finalize_work(finalization(h, db, work=repeated))
    assert duplicate.source_disposition == "historical"
    assert store.get_incident(identity).occurrence_count == 5
    original_hash = receipt.incident_payload_hash
    db.incidents[incident_id] = prior.model_copy(update={"summary": "Later independent evidence."}).model_dump_json()
    assert store.get_finalization(h.version, request.finalization_id) == receipt
    assert store.finalize_work(request).incident_payload_hash == original_hash


def test_pending_effect_persists_incident_but_cannot_claim_completed_work():
    h, db, store, action = reserved()
    request = finalization(h, db)
    receipt = store.finalize_work(request)
    assert receipt.state == "persisted_waiting_verification"
    assert store.get_work(h.version, request.work_id).state == "finalizing"
    assert ("source_disposition", request.source_execution.key) not in db.records
    assert store.get_incident_state(action.request.incident).action_count == 1


@pytest.mark.parametrize("failure", ["receipt", "different_bytes"])
def test_finalization_failure_rolls_back_incident_occurrence_budget_and_work(failure):
    h, db, store, action = reserved()
    store.record_action_rejection(rejection_request(h, action))
    before = deepcopy((db.records, db.incidents, db.processed, db.approvals))
    request = finalization(h, db)
    db.fail_finalize_receipt = failure == "receipt"
    db.change_receipt_bytes = failure == "different_bytes"
    with pytest.raises((MonitoringConflict, MonitoringUnavailable)):
        store.finalize_work(request)
    assert (db.records, db.incidents, db.processed, db.approvals) == before
    assert store.get_finalization(h.version, request.finalization_id) is None


@pytest.mark.parametrize("failure", ["before", "after"])
def test_lost_finalization_ack_reconciles_original_bytes_without_repeating_occurrence(failure):
    h, db, store, action = reserved()
    store.record_action_rejection(rejection_request(h, action))
    request = finalization(h, db)
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain):
        store.finalize_work(request)
    original = store.get_finalization(h.version, request.finalization_id)
    assert (original is not None) == (failure == "after")
    committed = store.finalize_work(request)
    assert committed.state == "completed"
    assert store.get_incident(action.request.incident).occurrence_count == 1
    assert len([row for row in db.records.values() if row.kind == "incident_occurrence"]) == 1
    if original is not None:
        assert committed == original


@pytest.mark.parametrize("malformation", ["missing", "wrong_parent"])
def test_invalid_native_successor_result_cannot_commit_a_rejection(malformation):
    h, db, store, action = reserved()
    before = deepcopy((db.records, db.approvals, db.incidents))
    db.bad_transition_result = malformation
    with pytest.raises(MonitoringUnavailable):
        store.record_action_rejection(rejection_request(h, action))
    assert (db.records, db.approvals, db.incidents) == before
    assert not db.receipts


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(7), asyncio.CancelledError()])
@pytest.mark.parametrize("operation", ["rejection", "finalization"])
def test_interruption_propagates_and_rolls_back_the_atomic_native_unit(error, operation):
    h, db, store, action = reserved()
    request = rejection_request(h, action)
    if operation == "finalization":
        store.record_action_rejection(request)
        request = finalization(h, db)
    before = deepcopy((db.records, db.incidents, db.approvals, db.processed, db.receipts))
    db.interrupt_after_mutation = error
    with pytest.raises(type(error)):
        if operation == "rejection":
            store.record_action_rejection(request)
        else:
            store.finalize_work(request)
    assert (db.records, db.incidents, db.approvals, db.processed, db.receipts) == before
