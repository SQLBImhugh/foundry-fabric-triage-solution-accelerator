"""Tool schemas and the dispatcher that enforces policy.

The dispatcher is where the safety story lives. Every call is charged against
the :class:`~triage.policy.PolicyLedger` *before* it executes.

One deliberate asymmetry in how violations are handled:

* ``policy_blocked`` is returned to the model **as a tool result**. The agent
  sees "refused, and here's why", and can still notify a human. Killing the
  run at that moment would leave the operator with silence.
* ``timed_out`` / ``budget_exceeded`` / ``max_turns_exceeded`` propagate and
  end the run. Those mean the agent has consumed its allowance; letting it
  keep talking is how you get a runaway.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from triage.approvals import DEFAULT_TIMEOUT_SECONDS, ApprovalGate, ApprovalRequest
from triage.knowledge.refresh_history import assess_deactivation_risk
from triage.models import ApprovalRecord, BIRequest, DataQualityFinding, TriageAction
from triage.observability import tool_span
from triage.policy import (
    REMEDIATION_ACTIONS,
    PolicyLedger,
    PolicyViolation,
    is_remediation,
    requires_approval,
)
from triage.tools.dataset import DatasetSource
from triage.tools.flags import DataQualityFlagTable, build_flag
from triage.tools.powerbi import PowerBIClient
from triage.tools.teams import ResolutionSummary, TeamsNotifier

logger = logging.getLogger("triage.tools")

# Actions whose dispatch is subject to a deterministic precondition. Their
# remediation charge is deferred until the check passes, so being refused never
# costs the run its one remediation.
_PRECONDITIONED_ACTIONS: frozenset[str] = frozenset(
    {
        "refresh_powerbi_dataset",
        "reenable_refresh_schedule",
    }
)


# ---------------------------------------------------------------------------
# Function-calling schemas
# ---------------------------------------------------------------------------

TRIAGE_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_request_context",
            "description": (
                "Return the parsed BI request: subject, body, and any report / "
                "dataset / workspace / error-code hints extracted deterministically."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_known_incidents",
            "description": (
                "Check whether this failure signature has been seen before and is "
                "still open. Call this BEFORE proposing any remediation. If an open "
                "incident exists, the correct action is to wait, not to act again."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dataset_refresh_history",
            "description": (
                "Return the last N refresh attempts for the dataset, plus a "
                "deterministic assessment of how close the refresh SCHEDULE is to being "
                "deactivated. Use this to distinguish a one-off transient failure from a "
                "repeating one. Note that only scheduled runs count toward deactivation — "
                "your own API-triggered retries do not."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "top": {
                        "type": "integer",
                        "description": "How many history entries to return (1-20).",
                        "default": 5,
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "consult_data_quality_agent",
            "description": (
                "Hand off to the Data Quality agent, a separate agent that inspects "
                "the underlying dataset for duplicate records and returns a "
                "structured finding. This is the first triage decision: a data "
                "quality issue is never resolved by refreshing a report."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Why this consultation is needed.",
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "refresh_powerbi_dataset",
            "description": (
                "REMEDIATION. Trigger a Power BI dataset refresh via the REST API. "
                "Only valid for a Tier 1 transient failure with no outstanding data "
                "quality issue. The controller permits exactly one remediation per run."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "justification": {
                        "type": "string",
                        "description": "One sentence on why a refresh resolves this.",
                    }
                },
                "required": ["justification"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rebind_dataset_gateway",
            "description": (
                "REMEDIATION REQUIRING HUMAN APPROVAL. Rebind the dataset to a "
                "different data gateway. Use only when refresh history shows the "
                "SAME failure repeating, so another refresh will not help. This "
                "affects every dataset bound to the gateway, not just this one, "
                "so a human must authorise it before it runs. Propose it, explain "
                "why, and accept the answer."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "target_gateway": {
                        "type": "string",
                        "description": "Name or id of the gateway to bind to.",
                    },
                    "justification": {
                        "type": "string",
                        "description": (
                            "Why a rebind is the right call, citing the refresh history."
                        ),
                    },
                },
                "required": ["target_gateway", "justification"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_refresh_schedule",
            "description": (
                "Read the dataset's refresh schedule, including whether it is still "
                "enabled. Power BI disables a schedule automatically after four "
                "consecutive failures and never re-enables it, so a model whose cause "
                "has been fixed can still be silently out of date. Check this whenever "
                "the refresh history shows repeated failures."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "reenable_refresh_schedule",
            "description": (
                "REMEDIATION REQUIRING HUMAN APPROVAL. Turn the dataset's refresh "
                "schedule back on after Power BI disabled it. Only valid once the "
                "most recent refresh has actually succeeded -- re-arming a schedule "
                "whose cause is unfixed just fails again and disables it again. The "
                "controller checks that and will refuse otherwise."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "justification": {
                        "type": "string",
                        "description": (
                            "Why the schedule is safe to re-arm, citing the successful "
                            "refresh that proves it."
                        ),
                    }
                },
                "required": ["justification"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "defer_refresh_retry",
            "description": (
                "Schedule this dataset's refresh to be retried later instead of now. "
                "Use when the failure is capacity throttling: the capacity is already "
                "over its limits, so retrying immediately adds load to the thing that "
                "is overloaded and makes the contention worse. This does not touch "
                "Power BI -- it records the work so a later sweep performs it once the "
                "window has passed. It does not spend the remediation budget."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": (
                            "Why the retry is being postponed, citing the throttling "
                            "evidence."
                        ),
                    },
                    "retry_after_seconds": {
                        "type": "integer",
                        "description": (
                            "How long the service asked us to wait, if it said. Omit "
                            "or use 0 to let the controller apply its own backoff."
                        ),
                    },
                },
                "required": ["reason"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_data_quality_flag",
            "description": (
                "Record a data quality issue in the flag table. Flags the affected "
                "table and the nature of the issue. Does NOT fix the data."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "detail": {
                        "type": "string",
                        "description": "Human-readable description of the issue.",
                    }
                },
                "required": ["detail"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "notify_teams",
            "description": "Post a summary to the Teams channel.",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "outcome": {"type": "string"},
                    "action_taken": {"type": "string"},
                    "detail": {"type": "string"},
                },
                "required": ["title", "outcome"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "report_resolution",
            "description": (
                "REQUIRED FINAL CALL. Declare the terminal outcome of this triage "
                "run. Call this exactly once, after any action and notification."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "outcome": {
                        "type": "string",
                        "enum": [
                            "resolved",
                            "flagged_data_quality",
                            "duplicate_suppressed",
                            "approval_denied",
                            "deferred_retry",
                            "needs_human",
                            "declared_failed",
                        ],
                    },
                    "tier": {"type": "string", "enum": ["tier_1", "tier_2", "needs_human"]},
                    "category": {
                        "type": "string",
                        "enum": ["transient", "data_quality", "config", "app", "user"],
                    },
                    "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                    "root_cause": {"type": "string"},
                    "summary": {"type": "string"},
                    "reasoning": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["outcome", "summary", "root_cause"],
            },
        },
    },
]

DQ_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "check_duplicates",
            "description": (
                "Deterministically scan a registered table for rows sharing a "
                "composite key. Returns counts and sample key values only - never "
                "row contents."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "Registered table name to inspect.",
                    }
                },
                "required": ["table"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Execution context
# ---------------------------------------------------------------------------


@dataclass
class ToolContext:
    """Everything the tools need. Constructed once per run by the runner."""

    request: BIRequest
    ledger: PolicyLedger
    powerbi: PowerBIClient
    teams: TeamsNotifier
    flag_table: DataQualityFlagTable
    datasets: dict[str, DatasetSource] = field(default_factory=dict)
    known_incident: Any = None  # Incident | None, set by the orchestrator
    signature: str = ""

    # Filled in as the run proceeds so the orchestrator can build its result.
    dq_finding: DataQualityFinding | None = None
    remediation_outcome: Any = None
    resolution: dict[str, Any] | None = None
    notifications: list[ResolutionSummary] = field(default_factory=list)
    flag_written: bool = False
    notification_attempted: bool = False
    notification_delivered: bool = False
    # Set when the controller declined to deliver because the incident had
    # already been announced. Distinct from a delivery failure.
    notification_suppressed: bool = False

    workspace_id: str = ""
    dataset_id: str = ""
    #: True / False / None where None means "not checked or unreadable".
    #: Deliberately tri-state: treating unknown as enabled would let the
    #: controller conclude there is nothing to re-arm.
    schedule_enabled: bool | None = None
    approval_gate: ApprovalGate | None = None
    #: Where postponed retries live. None means deferring is unavailable, which
    #: the tool reports rather than silently dropping the work.
    retries: Any = None
    retry_deferred: bool = False
    # How long a gated action waits for an answer. Carried on the context
    # because the dispatcher has no settings object, and hardcoding it here
    # would make APPROVAL_TIMEOUT_SECONDS a knob that silently does nothing --
    # which is the failure `TriagePolicy.from_settings` exists to prevent.
    approval_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    approvals: list[ApprovalRecord] = field(default_factory=list)
    gateway_rebound_to: str = ""
    deactivation_risk: Any = None

    def default_dataset(self) -> DatasetSource | None:
        return next(iter(self.datasets.values()), None)


class ToolDispatcher:
    """Executes one tool call, charging policy first."""

    def __init__(self, ctx: ToolContext, *, dq_agent=None):
        self.ctx = ctx
        self._dq_agent = dq_agent
        self.actions: list[TriageAction] = []

    def _refusal_guidance(self) -> str:
        """Tell the model what it may still do, not just what it may not.

        A refusal that only says "no" is a bad refusal. The two policy blocks
        need different advice and previously shared one message:

        - **Action not on the allowlist** — the remediation budget is untouched,
          so the right response is to pick a permitted action and carry on.
        - **Remediation budget exhausted** — there is no permitted action left,
          so the right response is to escalate.

        The shared message said "escalate" for both. That is correct for the
        second case and wrong for the first, and the model duly followed it:
        scenario 4 failed its expectations 2 runs in 5 because the agent was
        being told to stop when it should have adapted. The flakiness was in
        the instruction, not the model.
        """
        ledger = self.ctx.ledger
        remaining = ledger.policy.max_write_actions - ledger.write_actions

        if remaining > 0:
            permitted = sorted(REMEDIATION_ACTIONS & ledger.policy.allowed_actions)
            return (
                "That action was not dispatched, and this run is not over. You "
                f"still have {remaining} remediation action(s) available. If a "
                "permitted action addresses the failure, call it now: "
                f"{', '.join(permitted)}. If none is appropriate, notify_teams "
                "and report_resolution with outcome 'needs_human'."
            )

        return (
            "That action was not dispatched, and no remediation budget remains, "
            "so no further fix is possible in this run. Call notify_teams, then "
            "report_resolution with outcome 'needs_human'."
        )

    async def dispatch(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        started = time.monotonic()
        gated = requires_approval(name)
        # Defer the remediation charge for anything that might still be refused:
        # a gated action awaiting a human, and any action with a deterministic
        # precondition. A refused action must never spend the run's one
        # remediation -- otherwise the next legitimate fix is denied for a
        # reason nobody can see.
        defer_charge = gated or name in _PRECONDITIONED_ACTIONS

        try:
            # Gated actions defer the remediation charge until a human has
            # actually said yes. A denial must not spend the run's one
            # remediation, or the next legitimate fix gets refused for a reason
            # nobody can see.
            self.ctx.ledger.charge_tool_call(name, defer_write=defer_charge)
        except PolicyViolation as violation:
            if violation.kind == "policy_blocked":
                # Surface the refusal to the model instead of ending the run.
                self._record(
                    name, arguments, f"BLOCKED: {violation.message}", started, blocked=True
                )
                logger.warning("Policy blocked '%s': %s", name, violation.message)
                return {
                    "status": "blocked_by_policy",
                    "reason": violation.message,
                    "guidance": self._refusal_guidance(),
                }
            raise

        blocked_reason = await self._precondition_failure(name)
        if blocked_reason is not None:
            # For a gated action this also means the human is never asked:
            # sending an approval request for something the controller will
            # reject anyway trains people to click through them.
            self._record(name, arguments, f"BLOCKED: {blocked_reason}", started, blocked=True)
            logger.warning("Precondition blocked '%s': %s", name, blocked_reason)
            return {
                "status": "blocked_by_policy",
                "reason": blocked_reason,
                "guidance": self._refusal_guidance(),
            }

        if gated:
            allowed, approval_result = await self._seek_approval(name, arguments)
            if not allowed:
                self._record(
                    name,
                    arguments,
                    f"NOT APPROVED: {approval_result.get('reason', '')}",
                    started,
                    blocked=True,
                )
                return approval_result
            try:
                self.ctx.ledger.charge_write(name)
            except PolicyViolation as violation:
                self._record(name, arguments, f"BLOCKED: {violation.message}", started, blocked=True)
                return {
                    "status": "blocked_by_policy",
                    "reason": violation.message,
                    "guidance": (
                        "Approval was granted but the remediation budget is spent. "
                        "Notify a human and report 'needs_human'."
                    ),
                }
        elif defer_charge:
            # Non-gated, but its precondition has now passed, so it really is a
            # remediation and must be charged as one.
            try:
                self.ctx.ledger.charge_write(name)
            except PolicyViolation as violation:
                self._record(name, arguments, f"BLOCKED: {violation.message}", started, blocked=True)
                logger.warning("Policy blocked '%s': %s", name, violation.message)
                return {
                    "status": "blocked_by_policy",
                    "reason": violation.message,
                    "guidance": self._refusal_guidance(),
                }

        with tool_span(name):
            try:
                result = await self._execute(name, arguments)
            except PolicyViolation:
                raise
            except Exception as exc:  # noqa: BLE001 - tool errors are data
                logger.exception("Tool '%s' raised", name)
                result = {"status": "error", "error_type": type(exc).__name__, "error": str(exc)[:500]}

        self._record(name, arguments, _summarize(result), started)
        return result

    # --- human approval ----------------------------------------------------

    async def _seek_approval(
        self, name: str, arguments: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """Ask a human. Return (allowed, result_to_return_if_not_allowed).

        Every path that is not an explicit, matching, unexpired, unused "yes"
        returns False. That includes the absence of a gate entirely: an
        approval-required action with nowhere to send the request is not
        approved, it is unapprovable.
        """
        ctx = self.ctx
        gate = ctx.approval_gate
        started = time.monotonic()

        request = ApprovalRequest(
            action=name,
            arguments=dict(arguments or {}),
            justification=str(arguments.get("justification", "")),
            request_id=f"{ctx.request.request_id}:{name}",
            report_name=ctx.request.report_name or "",
            signature=ctx.signature,
            impact=_impact_of(name, arguments),
            timeout_seconds=ctx.approval_timeout_seconds,
        )

        if gate is None:
            logger.warning("No approval gate configured; refusing '%s'", name)
            record = ApprovalRecord(
                action=name,
                fingerprint=request.fingerprint,
                requested_at=request.requested_at.isoformat(),
                justification=request.justification,
                impact=request.impact,
                granted=False,
                outcome="error",
                reason="No approval channel is configured.",
            )
            ctx.approvals.append(record)
            ctx.ledger.record_approval_denied(name)
            return False, {
                "status": "approval_unavailable",
                "reason": record.reason,
                "guidance": (
                    "This action requires human approval and no approval channel "
                    "exists. Notify a human and report 'needs_human'."
                ),
            }

        # The clock stops here. Waiting on a person is not the agent consuming
        # budget, and with both limits at 300s an honest approval would other-
        # wise kill the run as `timed_out` at the moment it was granted.
        with ctx.ledger.awaiting_human():
            decision = await gate.request_approval(request)
        waited_ms = int((time.monotonic() - started) * 1000)

        valid, why = decision.is_valid_for(request)
        if valid and hasattr(gate, "consume") and not gate.consume(decision):
            valid, why = False, "approval already used"

        record = ApprovalRecord(
            action=name,
            fingerprint=request.fingerprint,
            requested_at=request.requested_at.isoformat(),
            justification=request.justification,
            impact=request.impact,
            granted=bool(valid),
            outcome=decision.outcome if valid else (decision.outcome or "denied"),
            decided_by=decision.decided_by,
            decided_at=decision.decided_at.isoformat(),
            reason=decision.reason or why,
            waited_ms=waited_ms,
        )
        ctx.approvals.append(record)
        await self._acknowledge_decision(request, record)

        if valid:
            logger.info("Approval granted for %s by %s", name, decision.decided_by or "unknown")
            return True, {}

        ctx.ledger.record_approval_denied(name)
        logger.info("Approval not granted for %s: %s", name, record.reason)
        return False, {
            "status": "not_approved",
            "outcome": record.outcome,
            "decided_by": record.decided_by,
            "reason": record.reason,
            "guidance": (
                "A human did not authorise this action, so it was not performed. "
                "Do not attempt it again and do not look for another route to the "
                "same effect. Notify the requester and call report_resolution with "
                "outcome 'approval_denied'."
            ),
        }

    async def _precondition_failure(self, name: str) -> str | None:
        """Deterministic checks an action must pass before it is dispatched.

        Enforced here rather than in the prompt, for the same reason as the
        allowlist: a model can be argued out of a precondition, a controller
        cannot. Returns a refusal reason, or None when the action may proceed.
        """
        ctx = self.ctx

        if name == "refresh_powerbi_dataset":
            # The one case where the obvious fix makes the outage worse. If a
            # retry has already been scheduled for this failure and its window
            # has not arrived, refreshing now is exactly the stampede the
            # deferral exists to prevent -- and the model proposing it anyway
            # is precisely why this is not left to prompt wording.
            if ctx.retries is not None and ctx.retries.is_deferred(ctx.signature):
                row = ctx.retries.get(ctx.signature) or {}
                return (
                    "A retry for this failure is already scheduled for "
                    f"{row.get('due_at')} because the capacity was throttling. "
                    "Refreshing now would add load to a capacity that is already "
                    "over its limits."
                )
            return None

        if name != "reenable_refresh_schedule":
            return None

        # A refresh that succeeded during this run is the strongest evidence
        # available: the thing that was failing now works.
        outcome = ctx.remediation_outcome
        if outcome is not None and getattr(outcome, "succeeded", False):
            return None

        # Otherwise the common real case: somebody fixed the cause by hand and
        # ran a manual refresh, but nobody re-armed the schedule.
        try:
            history = await ctx.powerbi.get_refresh_history(
                ctx.workspace_id, ctx.dataset_id, top=1
            )
        except Exception as exc:  # noqa: BLE001
            return (
                "Could not read the refresh history to confirm the dataset is "
                f"healthy ({type(exc).__name__}), so the schedule was not re-armed."
            )

        latest = history[0] if history else None
        if latest is None:
            return (
                "There is no refresh history for this dataset, so there is no "
                "evidence the schedule is safe to re-arm."
            )
        if str(latest.get("status")) != "Completed":
            return (
                "The most recent refresh is "
                f"'{latest.get('status')}', not 'Completed'. Re-arming the schedule "
                "now would fail again and disable it again. Fix the cause, get one "
                "successful refresh, then re-enable."
            )
        return None

    async def _acknowledge_decision(
        self, request: ApprovalRequest, record: ApprovalRecord
    ) -> None:
        """Post who answered, because the card itself cannot show it.

        The approval card's buttons are ``Action.OpenUrl`` links -- a link
        cannot alter the card it sits on, and an incoming webhook cannot edit a
        message it already posted. So the original card keeps its buttons
        forever, and a channel reading back over an outage has no record of who
        authorised what. A second click is refused, but nothing on screen says
        so.

        This is that record. It is posted straight to the notifier rather than
        through the ``notify_teams`` tool on purpose: that path is deduplicated
        against the incident's notified_count, and routing an approval
        acknowledgement through it would consume the incident's one
        announcement and silence the actual outcome. For the same reason it
        must not touch ctx.notification_* -- those drive both the "was the
        human told" check and the dedup, and an acknowledgement is neither.
        """
        ctx = self.ctx
        granted = record.granted
        summary = ResolutionSummary(
            title=("Approval granted" if granted else "Approval not granted"),
            report_name=ctx.request.report_name or "",
            error=request.justification,
            action_taken=request.action,
            outcome=record.outcome,
            timestamp=record.decided_at,
            detail=(
                record.reason
                or ("The agent may now perform this one action." if granted else "")
            ),
            facts={
                "Decided by": record.decided_by or "nobody",
                "Request": request.request_id,
                "Note": (
                    "The buttons on the original card stay visible; a second "
                    "click is refused."
                ),
            },
        )

        try:
            await ctx.teams.post(summary)
        except Exception as exc:  # noqa: BLE001
            # A missing acknowledgement is cosmetic. Failing the run over it
            # would turn a Teams outage into a refused remediation.
            logger.warning(
                "Could not acknowledge the approval decision (%s)", type(exc).__name__
            )

    # --- individual tools --------------------------------------------------

    async def _execute(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        ctx = self.ctx

        if name == "get_request_context":
            return {
                "status": "ok",
                "request_id": ctx.request.request_id,
                "received_at": ctx.request.received_at,
                "subject": ctx.request.subject,
                "body": ctx.request.body[:4000],
                "report_name": ctx.request.report_name,
                "dataset_id": ctx.request.dataset_id or ctx.dataset_id,
                "workspace_id": ctx.request.workspace_id or ctx.workspace_id,
                "error_code": ctx.request.error_code,
                "signature": ctx.signature,
            }

        if name == "get_known_incidents":
            known = ctx.known_incident
            if known is None:
                return {"known_related_issue": False, "signature": ctx.signature}
            return {
                "known_related_issue": True,
                "signature": ctx.signature,
                "incident_id": known.id,
                "status": known.status,
                "occurrence_count": known.occurrence_count,
                "first_seen_at": known.first_seen_at,
                "diagnosed_root_cause": known.diagnosed_root_cause,
                "guidance": (
                    "An open incident already exists for this exact failure. Do not "
                    "remediate again. Notify and report 'duplicate_suppressed'."
                ),
            }

        if name == "get_dataset_refresh_history":
            top = max(1, min(int(args.get("top", 5) or 5), 20))
            history = await ctx.powerbi.get_refresh_history(
                ctx.workspace_id, ctx.dataset_id, top=top
            )
            # Counting consecutive SCHEDULED failures is an exact question, so
            # it is answered here rather than left to the model. API-triggered
            # refreshes - including this agent's own retries - are a different
            # trigger path and must not be counted.
            risk = assess_deactivation_risk(history)
            ctx.deactivation_risk = risk
            return {
                "status": "ok",
                "count": len(history),
                "history": history,
                "schedule_deactivation_risk": risk.as_evidence(),
            }

        if name == "consult_data_quality_agent":
            if self._dq_agent is None:
                return {"status": "unavailable", "reason": "No Data Quality agent configured"}
            finding = await self._dq_agent.investigate(
                request=ctx.request,
                datasets=ctx.datasets,
                reason=str(args.get("reason", "")),
                ledger=ctx.ledger,
            )
            ctx.dq_finding = finding
            return finding.model_dump()

        if name == "refresh_powerbi_dataset":
            outcome = await ctx.powerbi.refresh_dataset(ctx.workspace_id, ctx.dataset_id)
            ctx.remediation_outcome = outcome
            return {
                "status": outcome.status,
                "succeeded": outcome.succeeded,
                "request_id": outcome.request_id,
                "duration_ms": outcome.duration_ms,
                "detail": outcome.detail,
            }

        if name == "rebind_dataset_gateway":
            target = str(args.get("target_gateway") or "").strip()
            if not target:
                return {"status": "refused", "reason": "No target gateway supplied."}
            outcome = await ctx.powerbi.rebind_gateway(
                ctx.workspace_id, ctx.dataset_id, target
            )
            ctx.remediation_outcome = outcome
            ctx.gateway_rebound_to = target
            return {
                "status": outcome.status,
                "succeeded": outcome.succeeded,
                "target_gateway": target,
                "detail": outcome.detail,
            }

        if name == "write_data_quality_flag":
            finding = ctx.dq_finding
            if finding is None or finding.evidence is None:
                return {
                    "status": "refused",
                    "reason": (
                        "No deterministic duplicate evidence is available. Consult the "
                        "Data Quality agent before writing a flag."
                    ),
                }
            # A flag row asserting zero duplicates is worse than no row: someone
            # triages it, finds nothing, and stops trusting the table.
            if not finding.has_issue or finding.evidence.duplicate_row_count <= 0:
                return {
                    "status": "refused",
                    "reason": (
                        f"The scan found no duplicates in {finding.evidence.table}. "
                        "There is nothing to flag."
                    ),
                }
            flag = build_flag(
                request_id=ctx.request.request_id,
                evidence=finding.evidence,
                detail=str(args.get("detail") or finding.detail)[:1000],
            )
            ctx.flag_table.append(flag)
            ctx.flag_written = True
            return {"status": "written", "flag": flag.model_dump()}

        if name == "notify_teams":
            summary = ResolutionSummary(
                title=str(args.get("title", "BI request triage")),
                report_name=ctx.request.report_name or "",
                error=ctx.request.subject,
                action_taken=str(args.get("action_taken", "")),
                outcome=str(args.get("outcome", "")),
                timestamp=ctx.request.received_at,
                detail=str(args.get("detail", "")),
                facts=_facts_for(ctx),
            )
            ctx.notifications.append(summary)
            ctx.notification_attempted = True

            # Announce an incident once, not once per occurrence. A known open
            # incident that has already been announced is, by definition, one
            # somebody has been told about; re-posting an identical card every
            # sweep is the alert fatigue this system exists to remove. Observed
            # in the demo tenant: a five-minute routine over two unread alerts
            # posted 24 identical cards an hour.
            #
            # Decided here rather than in the prompt on purpose. A limit that
            # exists only as prompt wording is not a limit -- the model can
            # always be argued out of it, and a stale conversation would
            # reintroduce the flood. The tool call is still recorded, so the
            # audit trail shows the agent chose to notify and the controller
            # declined to deliver.
            known = ctx.known_incident
            if known is not None and known.notified_count > 0:
                ctx.notification_suppressed = True
                ctx.notification_delivered = False
                logger.info(
                    "Notification suppressed: incident %s already announced %d time(s), "
                    "now at occurrence %d",
                    known.id,
                    known.notified_count,
                    known.occurrence_count,
                )
                return {
                    "delivered": False,
                    "suppressed": True,
                    "reason": (
                        f"Incident {known.id} has already been announced. This "
                        f"occurrence ({known.occurrence_count + 1}) was counted on the "
                        "existing incident instead of posting another card."
                    ),
                }

            delivery = await ctx.teams.post(summary)
            ctx.notification_delivered = bool(
                delivery.get("delivered") if isinstance(delivery, dict) else False
            )
            return delivery

        if name == "get_refresh_schedule":
            schedule = await ctx.powerbi.get_refresh_schedule(
                ctx.workspace_id, ctx.dataset_id
            )
            enabled = schedule.get("enabled")
            ctx.schedule_enabled = enabled
            return {
                "status": "ok",
                "enabled": enabled,
                "days": schedule.get("days", []),
                "times": schedule.get("times", []),
                "guidance": (
                    "Power BI disabled this schedule. Nothing re-enables it "
                    "automatically, so the dataset will stay stale until somebody "
                    "turns it back on -- but only re-enable it once a refresh has "
                    "actually succeeded."
                    if enabled is False
                    else ""
                ),
            }

        if name == "reenable_refresh_schedule":
            outcome = await ctx.powerbi.set_refresh_schedule_enabled(
                ctx.workspace_id, ctx.dataset_id, True
            )
            # Records the completed remediation, which is what
            # ``_validate_outcome`` checks before it will accept "resolved".
            # Without this the run does the work, succeeds, and is then
            # downgraded to needs_human for lack of evidence it did anything.
            ctx.remediation_outcome = outcome
            ctx.schedule_enabled = True if outcome.succeeded else ctx.schedule_enabled
            return {
                "status": outcome.status,
                "detail": outcome.detail,
                "request_id": outcome.request_id,
            }

        if name == "defer_refresh_retry":
            if ctx.retries is None:
                # Deferring into a store that does not exist would drop the work
                # while reporting that it was scheduled. Say so instead.
                return {
                    "status": "unavailable",
                    "reason": (
                        "No retry store is configured, so a deferred retry would "
                        "never run. Report needs_human instead of deferring."
                    ),
                }
            requested = int(args.get("retry_after_seconds") or 0)
            row = ctx.retries.defer(
                signature=ctx.signature,
                request_id=ctx.request.request_id,
                workspace_id=ctx.workspace_id,
                dataset_id=ctx.dataset_id,
                report_name=ctx.request.report_name or "",
                reason=str(args.get("reason", "")),
                retry_after_seconds=max(0, requested),
            )
            ctx.retry_deferred = row.get("status") == "pending"
            return {
                "status": row.get("status"),
                "attempt": row.get("attempts"),
                "due_at": row.get("due_at"),
                "wait_seconds": row.get("wait_seconds"),
                "guidance": (
                    "The retry is scheduled. Report 'deferred_retry' as the outcome; "
                    "do not also attempt a refresh in this run."
                    if row.get("status") == "pending"
                    else (
                        "This dataset has now been deferred as many times as the "
                        "policy allows. Repeated throttling is a capacity scheduling "
                        "problem for a human. Report needs_human."
                    )
                ),
            }

        if name == "report_resolution":
            ctx.resolution = dict(args)
            return {"status": "recorded"}

        return {"status": "unknown_tool", "tool": name}

    # --- bookkeeping -------------------------------------------------------

    def _record(
        self,
        name: str,
        args: dict[str, Any],
        summary: str,
        started: float,
        *,
        blocked: bool = False,
    ) -> None:
        self.actions.append(
            TriageAction(
                tool_name=name,
                arguments=args,
                result_summary=summary[:600],
                duration_ms=int((time.monotonic() - started) * 1000),
                is_remediation=is_remediation(name),
                blocked=blocked,
            )
        )


def _impact_of(action: str, arguments: dict[str, Any]) -> str:
    """Plain-language blast radius, shown to whoever is asked to approve.

    An approval request that does not state the consequence is a rubber stamp
    with extra steps.
    """
    if action == "rebind_dataset_gateway":
        target = arguments.get("target_gateway", "the target gateway")
        return (
            f"Repoints this dataset to {target}. Other datasets bound to either "
            "gateway may be affected, and in-flight refreshes will fail."
        )
    if action == "reenable_refresh_schedule":
        return (
            "Turns the scheduled refresh back on. The dataset resumes refreshing "
            "unattended on its existing schedule. If the underlying cause has "
            "returned, Power BI will disable it again after four failures."
        )
    return "Modifies the live BI environment."


def _facts_for(ctx: ToolContext) -> dict[str, str]:
    facts: dict[str, str] = {"Signature": ctx.signature or "n/a"}
    finding = ctx.dq_finding
    if finding is not None and finding.has_issue and finding.evidence is not None:
        facts["Data quality"] = finding.evidence.headline()
    if ctx.known_incident is not None:
        facts["Known incident"] = (
            f"{ctx.known_incident.id} (seen {ctx.known_incident.occurrence_count}x)"
        )
    return facts


def _summarize(result: Any) -> str:
    if isinstance(result, dict):
        keys = ("status", "succeeded", "known_related_issue", "has_issue", "count")
        parts = [f"{k}={result[k]}" for k in keys if k in result]
        risk = result.get("schedule_deactivation_risk")
        if isinstance(risk, dict) and risk.get("schedule_at_risk"):
            parts.append("SCHEDULE AT RISK")
        if parts:
            return ", ".join(parts)
    return str(result)[:300]


def load_dataset_sources(config: list[dict[str, Any]], base_dir: Path) -> dict[str, DatasetSource]:
    """Build the registry of tables the DQ agent may inspect."""
    out: dict[str, DatasetSource] = {}
    for entry in config or []:
        name = entry["name"]
        out[name] = DatasetSource(
            name=name,
            path=(base_dir / entry["path"]).resolve(),
            key_columns=list(entry.get("key_columns") or []),
        )
    return out
