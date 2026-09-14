"""Human-in-the-loop approval gates.

The branch this implements
-------------------------
Most real failures are not safe to fix automatically. The valuable pattern is
not "the agent fixes it" — it is **the agent works the problem, proposes a
bounded fix, and stops**. A human makes one decision instead of doing one
investigation.

That is the "Does It Need Human Involvement?" box in the customer's own flow,
and it is the branch that makes the whole thing generalise past a refresh
button.

Why this is a controller concern, not a prompt concern
------------------------------------------------------
An agent instructed to "ask before doing X" will usually ask. Usually is not a
control. Microsoft's own toolbox guidance says the same thing from the other
direction:

    "If MCP tools have require_approval: 'always' ... the toolbox endpoint does
     not enforce this - your agent code is responsible."

So the gate lives here, in front of dispatch, and an un-approved action is
never executed regardless of what any model asked for.

The four properties that make an approval trustworthy
-----------------------------------------------------
A naive gate is worse than no gate, because it looks like control while
providing none. These are the properties that have to hold, and each one is
pinned by a test:

1. **Bound to the exact action.** A decision carries a fingerprint of the action
   name *and* its arguments. Approving "rebind gateway G on dataset A" cannot be
   replayed to authorise dataset B, or a different gateway, or the same action
   with a widened scope.
2. **Expiring.** An approval that sits unanswered overnight and then fires at
   3am is a liability. Decisions carry a deadline and are refused past it.
3. **Single use.** One approval authorises one execution. A retry needs a new
   decision.
4. **Fail closed.** Timeout, transport error, malformed response, no responder —
   every one of these means *not approved*. The dangerous default is the one
   where silence reads as consent.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import quote

logger = logging.getLogger("triage.approvals")

DEFAULT_TIMEOUT_SECONDS = 300


def _utcnow() -> datetime:
    return datetime.now(UTC)


def action_fingerprint(action: str, arguments: dict[str, Any]) -> str:
    """Stable hash of exactly what is being asked for.

    Includes the arguments, not just the action name. That is the whole point:
    an approval to rebind *this* dataset to *this* gateway must not authorise a
    different one.
    """
    payload = json.dumps(
        {"action": action, "arguments": arguments or {}},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@dataclass
class ApprovalRequest:
    """What a human is being asked to decide."""

    action: str
    arguments: dict[str, Any]
    justification: str
    request_id: str
    report_name: str = ""
    signature: str = ""
    impact: str = ""
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    requested_at: datetime = field(default_factory=_utcnow)
    run_id: str = ""

    @property
    def fingerprint(self) -> str:
        return action_fingerprint(self.action, self.arguments)

    @property
    def expires_at(self) -> datetime:
        return self.requested_at + timedelta(seconds=self.timeout_seconds)

    def summary(self) -> str:
        return (
            f"{self.action} on {self.report_name or 'the affected report'} — "
            f"{self.justification}"
        )


@dataclass
class ApprovalDecision:
    """A human's answer, bound to the thing they answered about."""

    granted: bool
    fingerprint: str
    decided_by: str = ""
    reason: str = ""
    decision_id: str = field(default_factory=lambda: f"apr-{uuid.uuid4().hex[:8]}")
    decided_at: datetime = field(default_factory=_utcnow)
    expires_at: datetime | None = None
    #: Why it was not granted, when it was not granted. Distinguishes an active
    #: "no" from silence - they mean different things to whoever reads the queue.
    outcome: str = "granted"  # granted | denied | timed_out | error

    def is_valid_for(self, request: ApprovalRequest, *, now: datetime | None = None) -> tuple[bool, str]:
        """Check this decision actually authorises this request, right now."""
        now = now or _utcnow()
        if not self.granted:
            return False, f"not granted ({self.outcome})"
        if self.fingerprint != request.fingerprint:
            # The loud case: someone approved something else.
            return False, (
                "decision does not match the requested action "
                f"(approved {self.fingerprint}, requested {request.fingerprint})"
            )
        deadline = self.expires_at or request.expires_at
        if now > deadline:
            return False, f"approval expired at {deadline.isoformat()}"
        return True, ""


class ApprovalGate(Protocol):
    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision: ...


class _BaseGate:
    """Shared single-use bookkeeping.

    An approval authorises one execution. Without this, an agent that retries a
    gated action would ride the first human decision every time - which is
    exactly the loophole a gate is supposed to close.
    """

    def __init__(self) -> None:
        self._consumed: set[str] = set()

    def consume(self, decision: ApprovalDecision) -> bool:
        if decision.decision_id in self._consumed:
            logger.warning("Approval %s already used; refusing replay", decision.decision_id)
            return False
        self._consumed.add(decision.decision_id)
        return True


# ---------------------------------------------------------------------------
# Deterministic gates — demo, rehearsal and tests
# ---------------------------------------------------------------------------


class AutoApproveGate(_BaseGate):
    """Approves everything. For the 'human says yes' path."""

    def __init__(self, approver: str = "m.hughes@contoso.com", delay_seconds: float = 0.0):
        super().__init__()
        self.approver = approver
        self.delay_seconds = delay_seconds
        self.requests: list[ApprovalRequest] = []

    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        import asyncio

        self.requests.append(request)
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        logger.info("Approval granted for %s by %s", request.action, self.approver)
        return ApprovalDecision(
            granted=True,
            fingerprint=request.fingerprint,
            decided_by=self.approver,
            reason="Approved — gateway rebind is the right call for a repeating failure.",
            expires_at=request.expires_at,
            outcome="granted",
        )


class AutoDenyGate(_BaseGate):
    """Declines everything. For the 'human says no' path."""

    def __init__(
        self,
        approver: str = "m.hughes@contoso.com",
        reason: str = "Gateway is shared with the finance datasets; not during close week.",
    ):
        super().__init__()
        self.approver = approver
        self.reason = reason
        self.requests: list[ApprovalRequest] = []

    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        self.requests.append(request)
        logger.info("Approval denied for %s by %s", request.action, self.approver)
        return ApprovalDecision(
            granted=False,
            fingerprint=request.fingerprint,
            decided_by=self.approver,
            reason=self.reason,
            outcome="denied",
        )


class TimeoutGate(_BaseGate):
    """Nobody answers.

    The most important gate to get right, and the easiest to get wrong. Silence
    must never read as consent.
    """

    def __init__(self, requests_seen: list[ApprovalRequest] | None = None):
        super().__init__()
        self.requests: list[ApprovalRequest] = requests_seen if requests_seen is not None else []

    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        self.requests.append(request)
        logger.warning(
            "No approval response for %s within %ss — failing closed",
            request.action,
            request.timeout_seconds,
        )
        return ApprovalDecision(
            granted=False,
            fingerprint=request.fingerprint,
            reason=f"No response within {request.timeout_seconds}s.",
            outcome="timed_out",
        )


# ---------------------------------------------------------------------------
# Teams Adaptive Card gate — the production shape
# ---------------------------------------------------------------------------


class TeamsCardApprovalGate(_BaseGate):
    """Post an approval card and wait for a decision.

    Two paths exist in the wild, and they are not equivalent:

    * **Teams connector** ``PostCardAndWaitForResponse`` blocks until someone
      answers. Clean, but it authenticates per-user, which is a poor fit for an
      unattended agent (there is no signed-in caller to consent).
    * **Workflows webhook + a callback the responder hits.** Works unattended.
      The card posts, the run records that it is waiting, and the decision
      arrives out of band.

    This implementation posts the card and then polls a pluggable
    ``decision_source``. Anything it cannot verify is treated as *not approved* —
    a malformed answer, a mismatched fingerprint and no answer at all are all
    the same outcome, deliberately.
    """

    def __init__(
        self,
        notifier: Any,
        decision_source: Any | None = None,
        *,
        poll_seconds: float = 5.0,
        callback_url: str = "",
    ):
        super().__init__()
        self._notifier = notifier
        self._decision_source = decision_source
        self._poll_seconds = poll_seconds
        self._callback_url = callback_url

    def _actions(self, request: ApprovalRequest) -> list[dict[str, Any]]:
        """Buttons, but only ones that do something.

        A card posted through an incoming webhook has no bot behind it, so
        ``Action.Submit`` renders a button that silently does nothing -- worse
        than no button, because it looks like the decision was recorded. When
        no callback is configured the card carries the request id instead and
        the answer comes from ``bi-triage approve|deny``.
        """
        if not self._callback_url:
            return []

        separator = "&" if "?" in self._callback_url else "?"
        return [
            {
                "type": "Action.OpenUrl",
                "title": title,
                "url": (
                    f"{self._callback_url}{separator}"
                    f"request_id={quote(request.request_id, safe='')}"
                    f"&fingerprint={quote(request.fingerprint, safe='')}"
                    f"&decision={verb}"
                ),
            }
            for title, verb in (("Approve", "approve"), ("Decline", "decline"))
        ]

    def build_card(self, request: ApprovalRequest) -> dict[str, Any]:
        """Adaptive Card with the decision and its consequences on it.

        The fingerprint is carried in the action payload so the answer can be
        tied back to exactly what was asked.
        """
        instruction = (
            "No response means no action. The agent will escalate rather than proceed."
        )
        if not self._callback_url:
            instruction += (
                f"\n\nAnswer with: bi-triage approve {request.request_id} "
                f"— or deny, with a reason."
            )

        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Medium",
                                "weight": "Bolder",
                                "text": "Approval required — BI triage",
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {"title": "Report", "value": request.report_name or "n/a"},
                                    {"title": "Proposed action", "value": request.action},
                                    {"title": "Why", "value": request.justification},
                                    {"title": "Impact", "value": request.impact or "n/a"},
                                    {
                                        "title": "Expires",
                                        "value": request.expires_at.isoformat(timespec="seconds"),
                                    },
                                    {"title": "Request", "value": request.request_id},
                                    {"title": "Fingerprint", "value": request.fingerprint},
                                ],
                            },
                            {
                                "type": "TextBlock",
                                "wrap": True,
                                "isSubtle": True,
                                "text": instruction,
                            },
                        ],
                        "actions": self._actions(request),
                    },
                }
            ],
        }

    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        import asyncio

        # Register what is being asked *before* asking, so a decision can never
        # arrive against a request nobody recorded -- and so `bi-triage
        # approvals` can show what is waiting even if the card never lands.
        if self._decision_source is not None:
            opener = getattr(self._decision_source, "open", None)
            if opener is not None:
                try:
                    opener(request)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Could not open approval %s (%s); failing closed",
                        request.request_id,
                        type(exc).__name__,
                    )
                    return ApprovalDecision(
                        granted=False,
                        fingerprint=request.fingerprint,
                        reason="The approval request could not be recorded.",
                        outcome="error",
                    )

        try:
            await self._post(request)
        except Exception as exc:  # noqa: BLE001 - a failed post is a failed approval
            logger.warning("Could not post approval card: %s", type(exc).__name__)
            return ApprovalDecision(
                granted=False,
                fingerprint=request.fingerprint,
                reason=f"Approval request could not be delivered ({type(exc).__name__}).",
                outcome="error",
            )

        if self._decision_source is None:
            logger.warning("No decision source configured — failing closed")
            return ApprovalDecision(
                granted=False,
                fingerprint=request.fingerprint,
                reason="No approval channel is configured to receive a decision.",
                outcome="error",
            )

        deadline = request.expires_at
        while _utcnow() < deadline:
            try:
                raw = await self._decision_source.poll(request.request_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Approval poll failed: %s", type(exc).__name__)
                raw = None

            if raw:
                return _decision_from_payload(raw, request)
            await asyncio.sleep(self._poll_seconds)

        return ApprovalDecision(
            granted=False,
            fingerprint=request.fingerprint,
            reason=f"No response within {request.timeout_seconds}s.",
            outcome="timed_out",
        )

    async def _post(self, request: ApprovalRequest) -> None:
        card = self.build_card(request)
        post = getattr(self._notifier, "post_card", None)
        if post is not None:
            await post(card)
            return
        # Fall back to the plain notifier so a webhook-only setup still works.
        from triage.tools.teams import ResolutionSummary

        await self._notifier.post(
            ResolutionSummary(
                title="Approval required — BI triage",
                report_name=request.report_name,
                error=request.justification,
                action_taken=f"awaiting approval: {request.action}",
                outcome="awaiting_approval",
                timestamp=request.requested_at.isoformat(timespec="seconds"),
                detail=request.impact,
            )
        )


class WebApprovalGate(TeamsCardApprovalGate):
    """Wait on durable decisions; Teams delivery is optional, not a prerequisite."""

    def __init__(
        self, decision_source: Any, notifier: Any = None, *,
        poll_seconds: float = 2.0, command_center_url: str = "",
    ):
        super().__init__(notifier, decision_source, poll_seconds=poll_seconds, callback_url=command_center_url)
        self._requests: dict[str, ApprovalRequest] = {}

    def _actions(self, request: ApprovalRequest) -> list[dict[str, Any]]:
        if not self._callback_url:
            return []
        return [{
            "type": "Action.OpenUrl", "title": "Review in command center",
            "url": f"{self._callback_url.rstrip('/')}/?approval={quote(request.request_id, safe='')}",
        }]

    async def request_approval(self, request: ApprovalRequest) -> ApprovalDecision:
        import asyncio

        try:
            self._decision_source.open_exact(request)
        except Exception as exc:
            logger.error("Could not register web approval (%s)", type(exc).__name__)
            return ApprovalDecision(
                granted=False, fingerprint=request.fingerprint, outcome="error",
                reason="The approval request could not be persisted.",
            )
        async def notify():
            try:
                await asyncio.wait_for(self._post(request), timeout=10)
            except Exception as exc:
                logger.warning("Optional Teams approval delivery failed (%s); web inbox remains available", type(exc).__name__)

        notification = asyncio.create_task(notify()) if self._notifier is not None else None
        try:
            while _utcnow() < request.expires_at:
                try:
                    raw = await self._decision_source.poll(request.request_id)
                except Exception as exc:
                    logger.warning("Web approval poll failed (%s)", type(exc).__name__)
                    raw = None
                if raw:
                    if raw.get("fingerprint") != request.fingerprint or not raw.get("responder"):
                        return ApprovalDecision(
                            granted=False, fingerprint=request.fingerprint, outcome="error",
                            reason="The decision has no matching proposal or responder.",
                        )
                    decision = _decision_from_payload(raw, request)
                    self._requests[decision.decision_id] = request
                    return decision
                await asyncio.sleep(self._poll_seconds)
            return ApprovalDecision(
                granted=False, fingerprint=request.fingerprint, outcome="timed_out",
                reason=f"No response within {request.timeout_seconds}s.",
            )
        finally:
            if notification is not None:
                if not notification.done():
                    notification.cancel()
                await asyncio.gather(notification, return_exceptions=True)

    def consume(self, decision: ApprovalDecision) -> bool:
        request = self._requests.pop(decision.decision_id, None)
        if request is None or not decision.is_valid_for(request)[0]:
            return False
        try:
            return self._decision_source.consume_exact(request.request_id, request.fingerprint)
        except Exception as exc:
            logger.error("Could not consume web approval (%s); action refused", type(exc).__name__)
            return False


def _decision_from_payload(raw: dict[str, Any], request: ApprovalRequest) -> ApprovalDecision:
    """Turn a responder's payload into a decision, distrusting it by default."""
    decision = str(raw.get("decision", "")).lower()
    responder = str(raw.get("responder") or raw.get("decided_by") or "")
    claimed = str(raw.get("fingerprint", ""))

    if claimed and claimed != request.fingerprint:
        logger.error(
            "Approval fingerprint mismatch: payload=%s expected=%s", claimed, request.fingerprint
        )
        return ApprovalDecision(
            granted=False,
            fingerprint=request.fingerprint,
            decided_by=responder,
            reason="Approval did not match the requested action.",
            outcome="error",
        )

    if decision == "approve":
        return ApprovalDecision(
            granted=True,
            fingerprint=request.fingerprint,
            decided_by=responder,
            reason=str(raw.get("reason", "")),
            expires_at=request.expires_at,
            outcome="granted",
        )

    if decision == "decline":
        return ApprovalDecision(
            granted=False,
            fingerprint=request.fingerprint,
            decided_by=responder,
            reason=str(raw.get("reason", "")) or "Declined.",
            outcome="denied",
        )

    # Anything we do not positively recognise is a refusal.
    return ApprovalDecision(
        granted=False,
        fingerprint=request.fingerprint,
        decided_by=responder,
        reason=f"Unrecognised approval response: {decision!r}",
        outcome="error",
    )
