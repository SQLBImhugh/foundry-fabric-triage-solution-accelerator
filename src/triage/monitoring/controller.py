"""Controller-owned admission and action journaling over the monitoring store.

This module adds no model or prompt policy. PolicyLedger still owns the tool
allowlist and per-run limits. SQL owns current admission, approval consumption,
shared budgets, action fences and terminal work finalization.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from triage.approvals import ApprovalDecision, ApprovalRequest
from triage.monitoring.contracts import (
    ControllerMonitoringStore,
    FixedDiagnosticError,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
)
from triage.monitoring.models import (
    ActionKind,
    ActionOutcomeRequest,
    ActionRejectionEvidence,
    ActionRejectionRequest,
    ActionReservation,
    ActionReservationRequest,
    ActionSubmissionRequest,
    ApprovalReference,
    CollectionCommit,
    ConfigurationVerification,
    ConnectorPublicationContext,
    ConnectorPublicationResult,
    IncidentIdentity,
    MonitoringContext,
    MonitoringTarget,
    MonitoringWork,
    MonitoringWorkDraft,
    ReconciliationResult,
    RegistryVersion,
    SourceExecutionIdentity,
    SourceRunObservation,
)
from triage.monitoring.runtime import fixture_id, stable_id, target_signature
from triage.observability import heartbeat_span, telemetry_status

if TYPE_CHECKING:
    from triage.tools.powerbi import PowerBIClient, RefreshOutcome

logger = logging.getLogger("triage.monitoring.controller")
heartbeat_logger = logging.getLogger("triage.telemetry.heartbeat")
# The controller's own admission deadline, in seconds.
#
# This is not the host's HTTP limit. An earlier comment here claimed "the
# scheduled invocation has a 900-second HTTP limit" and sized the budget at
# 840 on that basis. Measured against the deployed caller it was false: the
# bi-triage-command-sweep Logic App configures timeout PT15M on its agent call,
# but Logic Apps Consumption enforces a 120-second ceiling on a synchronous
# outbound request, and its Invoke_the_agent action failed three times with
# code=BadRequest at exactly 120 seconds while every successful run finished
# within 115.
#
# So the controller reasoned with a deadline seven times longer than it had:
# it admitted work it could never report and was cut off mid-flight, and since
# coroutine cancellation does not stop a running asyncio.to_thread body, the
# SQL work carried on with nobody left to observe it.
#
# Override it with HEARTBEAT_BUDGET_SECONDS when the caller's real deadline
# differs. A deadline the runtime cannot verify is a guess, so this one is
# stated, and heartbeat_budget_seconds refuses a value that cannot accommodate
# one unit of work. Reaching it never cancels a claimed effect; the lease does
# that.
HEARTBEAT_BUDGET_SECONDS = 840


def heartbeat_work_seconds(settings: Any) -> int:
    """The execution allowance one admitted unit may need."""
    # Existing per-command execution bound plus its lease/finalization allowance.
    return settings.triage_timeout_seconds + settings.approval_timeout_seconds + 90


def heartbeat_budget_seconds(settings: Any) -> int:
    """The configured admission deadline, checked against the work it must hold.

    A deadline shorter than one work allowance makes ``can_claim()`` false on
    its first check, so every heartbeat returns having done nothing, for ever,
    with no error anywhere. That silent starvation is worse than a refusal.
    """
    budget = int(getattr(settings, "heartbeat_budget_seconds", HEARTBEAT_BUDGET_SECONDS))
    if budget <= 0:
        raise ValueError("The heartbeat admission deadline must be positive")
    work_seconds = heartbeat_work_seconds(settings)
    if work_seconds <= 0:
        raise ValueError("Heartbeat work requires a positive execution budget")
    if budget < work_seconds:
        raise ValueError(
            f"A heartbeat deadline of {budget}s cannot admit {work_seconds}s of work"
        )
    return budget


#: Stop starting new units this many seconds before the caller gives up.
#: Zero disables the bound, for a caller that genuinely waits.
HEARTBEAT_RESPONSE_SECONDS = 100


def heartbeat_response_seconds(settings: Any) -> int:
    """How long the heartbeat may take before it must report what it has.

    Separate from the admission deadline, which asks whether a unit can finish.
    This asks whether the caller is still listening. The shipped caller is a
    Logic App, and Logic Apps Consumption aborts a synchronous outbound request
    at 120 seconds whatever its configured timeout says, so a heartbeat that
    keeps starting units past that point is killed holding its report. Returning
    a partial result costs nothing: every admitted unit is protected by its
    lease, and the next invocation continues from the queue.
    """
    response = int(getattr(settings, "heartbeat_response_seconds", HEARTBEAT_RESPONSE_SECONDS))
    if response < 0:
        raise ValueError("The heartbeat response deadline cannot be negative")
    if response > heartbeat_budget_seconds(settings):
        raise ValueError(
            "The heartbeat response deadline cannot outlast its admission deadline"
        )
    return response
_CURRENT: ContextVar[MonitoringExecution | None] = ContextVar("monitoring_execution", default=None)
ACTION_KINDS: dict[str, ActionKind] = {
    "refresh_powerbi_dataset": "powerbi_refresh",
    "rerun_fabric_pipeline": "pipeline_rerun",
    "rebind_dataset_gateway": "rebind_dataset_gateway",
    "reenable_refresh_schedule": "reenable_refresh_schedule",
}


def publish_reconciliation_connector(
    store: ControllerMonitoringStore, context: ConnectorPublicationContext,
) -> ConnectorPublicationResult | None:
    """Compose current desired intent or original-receipt binding before work completion."""
    from triage.monitoring.provisioning import (
        prepare_connector_binding,
        prepare_connector_publication,
        publish_connector_intent,
    )

    if store.component != "controller":
        raise MonitoringComponentDenied("Only the controller composes connector publication")
    connector = context.connector
    if context.phase == "binding":
        request = prepare_connector_binding(
            store, context.work, connector.connector_id, request_id=context.request_id,
        )
    else:
        # Technical uncertainty holds topology; only affirmative policy evidence
        # authorizes removal, including when publication uses a narrowed subset.
        request = prepare_connector_publication(
            store, context.work, connector.connector_id, request_id=context.request_id,
            eligible_targets=context.eligible_targets, removal_targets=context.removal_targets,
        )
        if (
            store.get_connector_desired(context.expected, connector.connector_id) is not None
            and connector.policy_revision == context.expected.revision and request.sources == connector.sources
            and request.source_proposals == connector.source_proposals
            and request.source_removals == tuple(removal.intent() for removal in connector.source_removals)
            and request.desired_definition == connector.desired_definition
        ):
            return None
    return publish_connector_intent(store, request)


def reconcile_monitoring_work(store: ControllerMonitoringStore, work: MonitoringWork) -> ReconciliationResult:
    """Keep deterministic connector composition inside the store's one transaction."""
    if store.component != "controller":
        raise MonitoringComponentDenied("Deterministic runtime publication requires the controller component")
    if work.kind != "reconcile_state" or work.lease is None:
        raise MonitoringLeaseLost("Deterministic publication requires its own leased reconciliation work")
    return store.reconcile_work(work, connector_publisher=publish_reconciliation_connector)


@dataclass(frozen=True)
class HeartbeatBudget:
    deadline: float
    work_seconds: float
    clock: Callable[[], float] = time.monotonic

    def can_claim(self) -> bool:
        return self.deadline - self.clock() >= self.work_seconds


async def controller_heartbeat(
    runner: Any, *, rounds: int = 10, command_drain: Any = None,
    started_at: float | None = None, clock: Callable[[], float] | None = None,
) -> list[str]:
    """Refill two automatic slots and one human slot before the shared admission deadline."""
    if command_drain is None:
        from triage.command_center.worker import drain_commands

        command_drain = drain_commands
    clock = clock or time.monotonic
    began = clock() if started_at is None else started_at
    work_seconds = heartbeat_work_seconds(runner.settings)
    budget_seconds = heartbeat_budget_seconds(runner.settings)
    response_seconds = heartbeat_response_seconds(runner.settings)
    budget = HeartbeatBudget(began + budget_seconds, work_seconds, clock)

    def still_listening() -> bool:
        return not response_seconds or clock() - began < response_seconds
    lines: list[str] = []
    remaining = {"automatic": max(1, min(rounds, 100)), "human": max(1, min(rounds, 100))}
    started = {"automatic": 0, "human": 0}
    completed = {"automatic": 0, "human": 0}
    failed = False

    async def worker(queue: str, prefer: str = "reconcile_state") -> None:
        nonlocal failed
        try:
            while remaining[queue] and not failed and budget.can_claim() and still_listening():
                # Both automatic workers share one quota. The drain rechecks
                # the deadline immediately before each durable claim.
                remaining[queue] -= 1
                started[queue] += 1
                result = (
                    await runner.drain_monitoring_work(limit=1, budget=budget, prefer=prefer)
                    if queue == "automatic"
                    else await command_drain(runner, limit=1, budget=budget)
                )
                completed[queue] += len(result)
                lines.extend(result)
                if not result:
                    return
                await asyncio.sleep(0)
        except BaseException:
            failed = True
            raise

    heartbeat_logger.info(
        "heartbeat_started budget_seconds=%d per_queue_limit=%d work_budget_seconds=%s",
        budget_seconds, remaining["human"], work_seconds,
    )
    with heartbeat_span() as span:
        status, error_type, error_cause = "completed", "", ""
        workers = [
            # One automatic worker leads each pool. They share the automatic
            # quota and borrow when their own pool is empty, so neither pool
            # can be starved by a continuous backlog in the other.
            asyncio.create_task(worker("automatic", prefer="reconcile_state")),
            asyncio.create_task(worker("human")),
            asyncio.create_task(worker("automatic", prefer="action")),
        ]
        try:
            # A sibling's SQL failure stops new claims, not an in-flight action.
            results = await asyncio.gather(*workers, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        except BaseException as exc:
            status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
            error_type = type(exc).__name__
            error_cause = _root_cause(exc)
            raise
        finally:
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            elapsed_ms = max(0, int((clock() - began) * 1000))
            exhausted = not budget.can_claim()
            for key, value in {
                "heartbeat.status": status, "heartbeat.elapsed_ms": elapsed_ms,
                "heartbeat.automatic_calls": started["automatic"], "heartbeat.human_calls": started["human"],
                "heartbeat.automatic_results": completed["automatic"], "heartbeat.human_results": completed["human"],
                "heartbeat.budget_exhausted": exhausted, "error.type": error_type,
                "error.cause": error_cause,
            }.items():
                span.set(key, value)
            health = telemetry_status()
            heartbeat_logger.log(
                logging.INFO if status == "completed" else logging.WARNING,
                "heartbeat_finished status=%s elapsed_ms=%d automatic_calls=%d human_calls=%d "
                "automatic_results=%d human_results=%d budget_exhausted=%s error_type=%s error_cause=%s "
                "telemetry_configuration=%s export_failures=%d export_warnings=%d ingestion=unverified",
                status, elapsed_ms, started["automatic"], started["human"],
                completed["automatic"], completed["human"], exhausted, error_type, error_cause,
                health["configuration"], health["export_failures"], health["export_warnings"],
            )
    if not lines and not budget.can_claim():
        return ["Heartbeat admission budget exhausted; no new work was claimed. Any queued work remains pending."]
    return lines


#: Longest cause identifier this boundary will export, matching the SQL store's
#: own field bound.
MAX_EXPORTED_CAUSE = 128
#: A fixed diagnostic code is a lower-case identifier chosen in this repository.
_FIXED_CODE = re.compile(r"\A[a-z][a-z0-9_]{0,63}\Z")


def _root_cause(exc: BaseException) -> str:
    """Name the originating failure behind a wrapped store error.

    The SQL store boundary reports anything it did not expect as
    MonitoringUnavailable, "shared state was not replaced". A deterministic
    policy refusal wrapped that way is indistinguishable in telemetry from a
    database outage: a deployed controller repeated one identical refusal for
    23 hours while the heartbeat reported only ``error_type=MonitoringUnavailable``,
    and nothing in App Insights could tell an operator that retrying was futile.

    Only the class name, and a code from an exception type that declares its
    code to be a fixed identifier, are copied out. Reading ``code`` from any
    class was not safe: a driver exception can carry whatever the server put in
    that attribute, and an independent review captured an 833-character value
    quoting a synthetic row. The pattern check is a second bound, not the
    primary one -- the type is what establishes provenance.
    """
    cause, depth = exc.__cause__, 0
    while cause is not None and depth < 5:
        code = getattr(cause, "code", None) if isinstance(cause, FixedDiagnosticError) else None
        if isinstance(code, str) and _FIXED_CODE.match(code):
            return f"{type(cause).__name__}:{code}"[:MAX_EXPORTED_CAUSE]
        if cause.__cause__ is None:
            return type(cause).__name__[:MAX_EXPORTED_CAUSE]
        cause, depth = cause.__cause__, depth + 1
    return ""


def current_execution() -> MonitoringExecution | None:
    return _CURRENT.get()


@contextmanager
def bind_execution(execution: MonitoringExecution | None) -> Iterator[None]:
    token = _CURRENT.set(execution)
    try:
        yield
    finally:
        _CURRENT.reset(token)


@dataclass
class MonitoringApprovalRequest(ApprovalRequest):
    """An immutable proposal identity scoped to its exact monitoring work."""

    monitoring_work_id: str = ""
    target_key: str = ""
    source_execution_key: str = ""

    @property
    def fingerprint(self) -> str:
        raw = json.dumps({
            "action": self.action, "arguments": self.arguments,
            "work": self.monitoring_work_id, "target": self.target_key,
            "source": self.source_execution_key,
        }, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(raw.encode()).hexdigest()


@dataclass
class MonitoringExecution:
    store: ControllerMonitoringStore
    work: MonitoringWork
    incident: IncidentIdentity
    observation: SourceRunObservation | None = None
    approval_channel: Any = None
    fixture: bool = False
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    expected_incident_revision: int = 0
    reservation: ActionReservation | None = None
    persistence_error: Exception | None = None
    refresh_source: Callable[[], Awaitable[SourceRunObservation]] | None = None
    refresh_history: Callable[[str], Awaitable[SourceRunObservation]] | None = None
    powerbi_client: PowerBIClient | None = None

    @property
    def context(self) -> MonitoringContext:
        return MonitoringContext(tenant_id=self.work.tenant_id, epoch=self.work.epoch)

    def current_work(self) -> MonitoringWork:
        current = self.store.get_work(self.context, self.work.work_id)
        expected = self.work.lease
        actual = current.lease if current is not None else None
        if (
            current is None or expected is None or actual is None
            or (actual.owner_id, actual.fence, actual.resource_key)
            != (expected.owner_id, expected.fence, expected.resource_key)
        ):
            raise MonitoringLeaseLost("This controller no longer owns the monitoring work.")
        self.work = current
        return current

    def current_target(self, *, include_inactive: bool = False) -> MonitoringTarget:
        target = self.store.resolve_target(self.incident.target, include_inactive=include_inactive)
        if target is None:
            raise MonitoringConflict("The target is no longer admitted to monitoring.")
        return target

    def approval_request(self, action: str, arguments: dict[str, Any], **fields: Any) -> MonitoringApprovalRequest:
        fields.setdefault("requested_at", self.clock())
        if action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}:
            target = self.current_target()
            review = self.store.get_safety_review(self.context, target.action.review_id) if target.action.review_id else None
            if review is None or review.parameters is None:
                raise MonitoringConflict("The requested configuration has no current reviewed parameter set.")
            if action == "rebind_dataset_gateway":
                requested = str(arguments.get("target_gateway", ""))
                if self.fixture:
                    requested = fixture_id(requested, kind="gateway")
                if requested != review.parameters.get("gateway_id"):
                    raise MonitoringConflict("The proposed gateway differs from the reviewed gateway.")
            arguments = {
                "justification": str(arguments.get("justification", "")),
                "configuration": deepcopy(review.parameters),
            }
        return MonitoringApprovalRequest(
            action=action, arguments=arguments,
            request_id=stable_id(f"{self.work.key}:approval:{action}"),
            monitoring_work_id=self.work.work_id, target_key=self.incident.target.key,
            source_execution_key=self.work.execution.key if self.work.execution else "",
            **fields,
        )

    def approval_reference(self, request: ApprovalRequest, decision: ApprovalDecision) -> ApprovalReference:
        if self.approval_channel is None:
            raise MonitoringConflict("The authoritative approval channel is unavailable.")
        row = self.approval_channel.get(request.request_id)
        if row is None:
            raise MonitoringConflict("The approval decision has not been durably recorded.")
        if (
            row.get("fingerprint") != request.fingerprint or row.get("decision") != "approve"
            or row.get("consumed_at") or not decision.is_valid_for(request, now=self.clock())[0]
            or not row.get("responder")
        ):
            raise MonitoringConflict("The recorded approval is not an explicit, matching unused decision.")
        return ApprovalReference(
            approval_id=request.request_id, fingerprint=request.fingerprint,
        )

    async def recheck_source(self, action: str = "refresh_powerbi_dataset") -> None:
        if self.refresh_source is None and self.refresh_history is None:
            if not self.fixture:
                raise MonitoringConflict("Live admission requires a fresh exact-source REST reader.")
            return
        observation = (
            await self.refresh_history(action) if self.refresh_history is not None
            else await self.refresh_source()
        )
        declared_fixture = self.fixture and observation.authority == "fixture" and observation.failure_signature == self.incident.signature
        if self.incident.target.workload == "powerbi" and not declared_fixture and target_signature(
            self.incident.target, observation.failure_reason or observation.error_code or "Unspecified failure",
            exception_class=observation.error_code,
        ) != self.incident.signature:
            raise MonitoringConflict("The exact source failure evidence changed while this action was being considered.")
        current = self.current_work()
        if current.lease is None:
            raise MonitoringLeaseLost("Source refresh lost its work lease.")
        try:
            self.observation = self.store.observe_source(
                observation, work_id=current.work_id, lease=current.lease,
            )
        except Exception as exc:
            self.persistence_error = exc
            raise

    def observe_successful_refresh(self, row: dict[str, Any]) -> None:
        """Persist the exact healthy head used to justify schedule restoration."""
        if row.get("status") != "Completed":
            raise MonitoringConflict("Schedule restoration requires an exact successful refresh.")
        try:
            observation = SourceRunObservation(
                execution=SourceExecutionIdentity(
                    target=self.incident.target, run_id_kind="powerbi_request",
                    run_id=row["requestId"],
                ),
                origin="poll", authority="rest", observed_at=self.clock(),
                started_at=datetime.fromisoformat(row["startTime"].replace("Z", "+00:00")),
                ended_at=datetime.fromisoformat(row["endTime"].replace("Z", "+00:00")),
                status="succeeded", invocation="scheduled" if row.get("refreshType") == "Scheduled" else "manual",
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MonitoringConflict("The latest successful refresh lacks exact identity or timestamps.") from exc
        work = self.current_work()
        try:
            self.store.observe_source(observation, work_id=work.work_id, lease=work.lease)
        except Exception as exc:
            self.persistence_error = exc
            raise

    def _reservation_request(
        self, name: str, approval: ApprovalReference | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> ActionReservationRequest | str:
        kind = ACTION_KINDS.get(name)
        if kind is None:
            return f"The deployed monitoring action contract does not support {name}."
        current = self.current_work()
        target = self.current_target()
        snapshot = self.store.snapshot(self.context)
        if current.execution is None or current.lease is None:
            raise MonitoringLeaseLost("An exact source execution and current work lease are required.")
        if not target.action.enabled or target.action.action != kind:
            return "This target is observation-only or is reviewed for a different action."
        if target.action.review_id is None or target.action.review_revision is None:
            return "No current safety review authorizes this action."
        review = self.store.get_safety_review(self.context, target.action.review_id)
        if review is None or review.parameter_hash is None:
            return "The current safety review cannot be verified."
        arguments = arguments or {}
        if name == "rerun_fabric_pipeline" and arguments.get("parameter_hash") != review.parameter_hash:
            return "The approved pipeline parameter set differs from its current review."
        if name in {"rebind_dataset_gateway", "reenable_refresh_schedule"} and arguments.get("configuration") != review.parameters:
            return "The approved configuration differs from its current review."
        if name == "reenable_refresh_schedule" and review.parameters.get("enabled") is not True:
            return "The reviewed schedule does not authorize enabling refresh."
        state = self.store.get_incident_state(self.incident)
        return ActionReservationRequest(
            idempotency_id=stable_id(f"{current.key}:action:{name}"),
            expected=RegistryVersion(
                **self.context.model_dump(), revision=snapshot.control.revision,
            ),
            work_id=current.work_id, lease=current.lease,
            source_execution=current.execution, incident=self.incident,
            expected_incident_revision=state.revision if state is not None else 0,
            action=kind, review_id=review.review_id,
            expected_review_revision=target.action.review_revision,
            definition_hash=review.definition_hash, parameter_hash=review.parameter_hash,
            configuration_hash=review.configuration_hash,
            approval=approval, arguments=arguments,
        )

    def bind_approval(self, proposal: ApprovalRequest) -> None:
        request = self._reservation_request(
            proposal.action,
            ApprovalReference(approval_id=proposal.request_id, fingerprint=proposal.fingerprint),
            proposal.arguments,
        )
        if isinstance(request, str):
            raise MonitoringConflict(request)
        try:
            self.store.bind_approval(request)
        except Exception as exc:
            self.persistence_error = exc
            raise

    def reserve(
        self, name: str, approval: ApprovalReference | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        """Reserve once against fresh shared state; a replay never submits another effect."""
        prior = self.store.get_action_by_request(
            self.context, stable_id(f"{self.work.key}:action:{name}"),
        )
        if prior is not None:
            if prior.reservation is not None:
                self.reservation = self.store.get_action_reservation(
                    self.context, prior.reservation.reservation_id,
                )
                return False, "This work already reserved an action; only read-only reconciliation may resume."
            return False, prior.detail
        request = self._reservation_request(name, approval, arguments)
        if isinstance(request, str):
            return False, request
        try:
            decision = self.store.reserve_action(request)
        except Exception as exc:
            self.persistence_error = exc
            raise
        if decision.status == "denied":
            return False, decision.detail
        self.reservation = decision.reservation
        if decision.status == "replayed":
            return False, "This work already reserved an action. Resume read-only verification, not submission."
        return True, decision.detail

    def schedule_retry(self, row: dict[str, Any]) -> MonitoringWork:
        current = self.current_work()
        if current.execution is None:
            raise MonitoringConflict("A deferred retry must retain its exact source execution.")
        try:
            return self.store.enqueue_work(MonitoringWorkDraft(
                **self.context.model_dump(),
                work_id=stable_id(f"{current.execution.key}:deferred-retry"),
                kind="deferred_retry", policy_revision=row["policy_revision"],
                created_at=self.clock(), due_at=datetime.fromisoformat(row["due_at"]),
                target=current.execution.target, execution=current.execution,
                reason="Controller deferred a throttled refresh; current admission is required at dequeue.",
            ))
        except Exception as exc:
            self.persistence_error = exc
            raise

    def _submission(
        self, *, execution: SourceExecutionIdentity | None, detail: str,
        configuration_action: str | None = None,
        submitted_at: datetime | None = None,
        retry_after_seconds: int = 0,
    ) -> ActionReservation:
        reservation = self.reservation
        if reservation is None:
            raise MonitoringConflict("No durable action reservation exists before submission.")
        now = max(self.clock(), reservation.reserved_at)
        request = ActionSubmissionRequest(
            **self.context.model_dump(),
            request_id=stable_id(f"{reservation.reservation_id}:submission"),
            reservation_id=reservation.reservation_id,
            expected_reservation_revision=reservation.revision,
            action_fence=reservation.fence,
            state="submitted" if execution is not None or configuration_action is not None else "uncertain",
            submitted_execution=execution,
            configuration_action=configuration_action,
            correlation="response_run_id" if execution is not None else None,
            submitted_at=submitted_at or now,
            next_verification_at=now + timedelta(seconds=max(0, retry_after_seconds)),
            detail=detail or "Submission recorded; execution is not yet verified.",
        )
        try:
            self.reservation = self.store.record_action_submission(request, commit=self._action_commit())
        except Exception as exc:
            self.persistence_error = exc
            raise
        return self.reservation

    def _action_commit(self) -> CollectionCommit:
        work = self.current_work()
        if work.lease is None:
            raise MonitoringLeaseLost("Action persistence requires this controller's current work lease.")
        return CollectionCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
        )

    def reject(self, outcome: RefreshOutcome, attempted_at: datetime) -> ActionReservation:
        """Record the SDK's definitive rejection; never infer it from verification."""
        reservation = self.reservation
        if reservation is None or outcome.submission_state != "rejected":
            raise MonitoringConflict("Confirmed rejection requires the original reserved submission attempt.")
        work = self.current_work()
        if work.lease is None:
            raise MonitoringLeaseLost("Rejection cannot be recorded without the original work lease.")
        request = ActionRejectionRequest(
            **self.context.model_dump(),
            request_id=stable_id(f"{reservation.reservation_id}:rejection"),
            reservation_id=reservation.reservation_id,
            expected_reservation_revision=reservation.revision, action_fence=reservation.fence,
            work_id=work.work_id, lease=work.lease,
            evidence=ActionRejectionEvidence(
                reason="throttled" if outcome.throttled else "definitive_client_error",
                attempted_at=attempted_at, rejected_at=self.clock(),
                retry_after_seconds=outcome.retry_after_seconds,
            ),
            detail=outcome.detail or "The service definitively rejected the request without an effect.",
        )
        try:
            self.reservation = self.store.record_action_rejection(request)
        except Exception as exc:
            self.persistence_error = exc
            raise
        return self.reservation

    def rejected_outcome(self) -> RefreshOutcome:
        from triage.tools.powerbi import RefreshOutcome

        action = self.reservation
        if action is None or action.state != "rejected" or action.rejection is None:
            raise MonitoringConflict("There is no durable confirmed rejection to report.")
        return RefreshOutcome(
            status="Throttled" if action.rejection.reason == "throttled" else "Failed",
            submission_state="rejected", retry_after_seconds=action.rejection.retry_after_seconds,
            detail=action.detail,
        )

    def _outcome(
        self, observation: SourceRunObservation | None, *, succeeded: bool | None,
        detail: str, activities: tuple = (), activities_complete: bool = False,
        configuration: ConfigurationVerification | None = None,
    ) -> None:
        reservation = self.reservation
        if reservation is None:
            raise MonitoringConflict("No durable action reservation exists for verification.")
        now = self.clock()
        request = ActionOutcomeRequest(
            **self.context.model_dump(),
            request_id=stable_id(f"{reservation.reservation_id}:verify:{reservation.revision}:{now.isoformat()}"),
            reservation_id=reservation.reservation_id,
            expected_reservation_revision=reservation.revision, action_fence=reservation.fence,
            disposition="uncertain" if succeeded is None else "verified_succeeded" if succeeded else "verified_failed",
            submitted_execution=reservation.submitted_execution, observation=observation,
            activities=activities, activities_complete=activities_complete,
            configuration=configuration,
            observed_at=now, detail=detail or "Exact action execution inspected.",
        )
        try:
            self.reservation = self.store.record_action_outcome(request, commit=self._action_commit())
        except Exception as exc:
            self.persistence_error = exc
            raise

    async def refresh(self, client: PowerBIClient) -> RefreshOutcome:
        from triage.tools.powerbi import RefreshOutcome

        reservation = self.reservation
        if reservation is None or reservation.state != "reserved":
            raise MonitoringConflict("Refresh submission requires a new durable reservation.")
        target = self.incident.target
        began = max(self.clock(), reservation.reserved_at)
        try:
            submission = await client.submit_refresh(target.workspace_id, target.item_id)
        except Exception as exc:
            submission = RefreshOutcome(
                status="Unknown", submission_state="uncertain",
                detail=f"Refresh submission was not acknowledged ({type(exc).__name__}).",
            )
        if submission.submission_state == "rejected" and (not submission.request_id or self.fixture):
            self.reject(submission, began)
            return replace(submission, request_id="")
        execution = None
        if submission.submission_state == "submitted" and submission.request_id:
            run_id = fixture_id(submission.request_id, kind=f"{target.key}:refresh") if self.fixture else submission.request_id
            try:
                execution = SourceExecutionIdentity(target=target, run_id_kind="powerbi_request", run_id=run_id)
            except ValueError:
                logger.error("Refresh acknowledgement did not identify a valid request; keeping the action uncertain")
        self._submission(
            execution=execution, detail=submission.detail, submitted_at=began,
            retry_after_seconds=submission.retry_after_seconds,
        )
        if execution is None:
            return replace(submission, status="Unknown", submission_state="uncertain")
        return await self.verify_refresh(client)

    async def configure(self, client: PowerBIClient) -> RefreshOutcome:
        from triage.tools.powerbi import RefreshOutcome

        reservation = self.reservation
        if reservation is None or reservation.state != "reserved":
            raise MonitoringConflict("Configuration changes require a new durable reservation.")
        target = reservation.request.source_execution.target
        desired = reservation.request.arguments.get("configuration")
        if not isinstance(desired, dict):
            raise MonitoringConflict("The reservation has no immutable reviewed configuration.")
        action = reservation.request.action
        began = max(self.clock(), reservation.reserved_at)
        try:
            if action == "rebind_dataset_gateway":
                outcome = await client.rebind_gateway(
                    target.workspace_id, target.item_id, desired["gateway_id"], desired["datasource_ids"],
                )
            elif action == "reenable_refresh_schedule":
                if desired.get("enabled") is not True:
                    raise MonitoringConflict("Only the reviewed enabled schedule may be restored.")
                outcome = await client.set_refresh_schedule_enabled(target.workspace_id, target.item_id, True)
            else:
                raise MonitoringConflict("This reservation is not a configuration action.")
        except Exception as exc:
            self._submission(execution=None, detail=f"Configuration acknowledgement is uncertain ({type(exc).__name__}).", submitted_at=began)
            return RefreshOutcome(status="Unknown", submission_state="uncertain")
        if outcome.submission_state == "rejected" and (not outcome.request_id or self.fixture):
            self.reject(outcome, began)
            return replace(outcome, request_id="", configuration={})
        self._submission(
            execution=None, detail=outcome.detail or "Configuration request acknowledged.",
            configuration_action=action if outcome.submission_state == "submitted" else None,
            submitted_at=began,
        )
        return await self.verify_configuration(client)

    async def verify_configuration(self, client: PowerBIClient) -> RefreshOutcome:
        reservation = self.reservation
        if reservation is None:
            raise MonitoringConflict("Configuration verification requires an existing action fence.")
        if reservation.state == "rejected":
            return self.rejected_outcome()
        target = reservation.request.source_execution.target
        desired = reservation.request.arguments.get("configuration")
        if not isinstance(desired, dict):
            raise MonitoringConflict("The reserved configuration is unavailable.")
        if reservation.request.action == "rebind_dataset_gateway":
            outcome = await client.verify_gateway_binding(
                target.workspace_id, target.item_id, desired["gateway_id"], desired["datasource_ids"],
            )
        else:
            outcome = await client.verify_refresh_schedule(target.workspace_id, target.item_id, True)
        evidence = None
        if outcome.configuration:
            evidence = ConfigurationVerification(
                target=target, action=reservation.request.action,
                expected_hash=reservation.request.parameter_hash,
                configuration=outcome.configuration,
                observed_at=self.clock(), authority="fixture" if self.fixture else "rest",
            )
        verified = outcome.succeeded and evidence is not None and evidence.matches
        self._outcome(
            None, succeeded=True if verified else None, configuration=evidence,
            detail=outcome.detail or "Configuration readback is incomplete.",
        )
        return outcome if verified else replace(outcome, status="Unknown", submission_state="uncertain")

    async def verify_refresh(self, client: PowerBIClient) -> RefreshOutcome:
        from triage.tools.powerbi import RefreshOutcome

        reservation = self.reservation
        if reservation is not None and reservation.state == "rejected":
            return self.rejected_outcome()
        if reservation is None or reservation.submitted_execution is None:
            return RefreshOutcome(status="Unknown", submission_state="uncertain", detail="Submission correlation is unavailable.")
        execution = reservation.submitted_execution
        if reservation.next_verification_at is not None and reservation.next_verification_at > self.clock():
            return RefreshOutcome(
                status="Submitted", request_id=execution.run_id, submission_state="submitted",
                detail="Waiting for the recorded service Retry-After interval.",
            )
        target = execution.target
        request_id = execution.run_id
        if self.fixture:
            history = getattr(client, "history", [])
            matched = [
                str(row["requestId"]) for row in history if row.get("requestId")
                and fixture_id(str(row["requestId"]), kind=f"{target.key}:refresh") == request_id
            ]
            if len(matched) != 1:
                self._outcome(None, succeeded=None, detail="Fixture submission correlation is absent or ambiguous.")
                return RefreshOutcome(status="Unknown", request_id=request_id, submission_state="uncertain")
            request_id = matched[0]
        result = await client.verify_refresh(target.workspace_id, target.item_id, request_id)
        returned = fixture_id(result.request_id, kind=f"{target.key}:refresh") if self.fixture else result.request_id
        if returned != execution.run_id:
            self._outcome(None, succeeded=None, detail="Verification returned a different refresh request.")
            return replace(result, status="Unknown", submission_state="uncertain")
        observation = None
        if result.status in {"Completed", "Failed"}:
            if self.fixture:
                started, ended = reservation.submitted_at, self.clock()
            else:
                rows = await client.get_refresh_history(target.workspace_id, target.item_id, top=100)
                exact = [
                    row for row in rows
                    if str(row.get("requestId", "")).lower() == execution.run_id
                    and row.get("status") == result.status
                ]
                started = ended = None
                if len(exact) == 1:
                    try:
                        started = datetime.fromisoformat(exact[0]["startTime"].replace("Z", "+00:00"))
                        ended = datetime.fromisoformat(exact[0]["endTime"].replace("Z", "+00:00"))
                    except (KeyError, TypeError, ValueError):
                        logger.warning("Terminal refresh evidence has no complete timestamps")
            if started is not None and ended is not None:
                observation = SourceRunObservation(
                    execution=execution, origin="fixture" if self.fixture else "poll",
                    authority="fixture" if self.fixture else "rest",
                    observed_at=self.clock(), started_at=started, ended_at=ended,
                    status="succeeded" if result.succeeded else "failed", invocation="manual",
                )
        self._outcome(
            observation, succeeded=result.succeeded if observation is not None else None,
            detail=result.detail or "Refresh verification is incomplete.",
        )
        return result if observation is not None else replace(result, status="Unknown", submission_state="uncertain")

    async def submit_pipeline(self, client, target):
        from triage.pipeline_models import PipelineRerunOutcome

        if self.reservation is None or self.reservation.state != "reserved":
            raise MonitoringConflict("Pipeline submission requires a new durable reservation.")
        began = max(self.clock(), self.reservation.reserved_at)
        try:
            submission = await client.rerun(target)
        except Exception as exc:
            detail = f"Pipeline submission acknowledgement is uncertain ({type(exc).__name__})."
            self._submission(execution=None, detail=detail, submitted_at=began)
            return PipelineRerunOutcome(status="Unknown", detail=detail)
        execution = SourceExecutionIdentity(
            target=self.incident.target, run_id_kind="fabric_job", run_id=submission.run_id,
        )
        self._submission(
            execution=execution, detail="Fabric accepted the exact pipeline job; execution is not yet verified.",
            submitted_at=began, retry_after_seconds=submission.retry_after_seconds,
        )
        return PipelineRerunOutcome(status="Submitted", run_id=execution.run_id, detail="Submission correlation is durable.")

    async def verify_pipeline(self, client, target):
        from triage.pipeline_models import PIPELINE_TERMINAL_STATUSES, PipelineRerunOutcome
        from triage.tools.pipeline_actions import verify_rerun

        reservation = self.reservation
        if reservation is None or reservation.submitted_execution is None:
            return PipelineRerunOutcome(status="Unknown", detail="Submission correlation is unavailable.")
        execution = reservation.submitted_execution
        if reservation.next_verification_at is not None and reservation.next_verification_at > self.clock():
            return PipelineRerunOutcome(
                status="Submitted", run_id=execution.run_id,
                detail="Waiting for the recorded service Retry-After interval.",
            )
        run = await client.get_run(target, execution.run_id)
        if run.id != execution.run_id or run.item_id != execution.target.item_id:
            raise MonitoringConflict("Pipeline verification returned a different job or item.")
        if run.status not in PIPELINE_TERMINAL_STATUSES:
            self._outcome(None, succeeded=None, detail="The correlated pipeline job remains in progress.")
            return PipelineRerunOutcome(status="Submitted", run_id=run.id, detail="Execution is not yet complete.")
        activities = await client.activity_runs(target, run)
        outcome = await verify_rerun(client, target, run, activities=activities)
        observation = None
        if run.start_time is not None and run.end_time is not None and outcome.status in {"Completed", "Failed", "ActivityFailed", "Cancelled"}:
            # The scripted client derives rerun times from its old sample run.
            # Explicit fixture evidence uses this simulation's submission clock;
            # REST evidence must retain the platform's original timestamps.
            observation = SourceRunObservation(
                execution=execution, authority="fixture" if self.fixture else "rest",
                origin="fixture" if self.fixture else "poll", observed_at=self.clock(),
                started_at=reservation.submitted_at if self.fixture else run.start_time,
                ended_at=self.clock() if self.fixture else run.end_time,
                status={"Completed": "succeeded", "Failed": "failed", "Cancelled": "cancelled"}[run.status],
                invocation="manual", job_type=run.job_type,
                evidence={"job_status": run.status},
            )
        if run.status == "Cancelled" and not outcome.detail:
            outcome = outcome.model_copy(update={"detail": f"Correlated pipeline job {run.id} was cancelled."})
        self._outcome(
            observation, succeeded=outcome.succeeded if observation is not None else None,
            detail=outcome.detail or "Correlated pipeline job and activities inspected.",
            activities=tuple(activities), activities_complete=True,
        )
        return outcome if observation is not None else PipelineRerunOutcome(
            status="Unknown", run_id=run.id, detail="Terminal pipeline evidence is incomplete.",
        )
