"""Teams notification — the "Send Resolution Summary" terminal step.

Message contents are fixed by the customer's spec: report name, error, action
taken, outcome, timestamp. Everything posted is redacted first, because error
text from a BI platform routinely carries tokens.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from triage.redaction import redact_text

logger = logging.getLogger("triage.teams")


@dataclass
class ResolutionSummary:
    """The card posted to Teams."""

    title: str
    report_name: str
    error: str
    action_taken: str
    outcome: str
    timestamp: str
    detail: str = ""
    facts: dict[str, str] = field(default_factory=dict)

    def to_markdown(self) -> str:
        lines = [
            f"**{redact_text(self.title)}**",
            "",
            f"- **Report**: {redact_text(self.report_name) or 'n/a'}",
            f"- **Error**: {redact_text(self.error) or 'n/a'}",
            f"- **Action taken**: {redact_text(self.action_taken) or 'none'}",
            f"- **Outcome**: {redact_text(self.outcome)}",
            f"- **Timestamp**: {self.timestamp}",
        ]
        for key, value in self.facts.items():
            lines.append(f"- **{key}**: {redact_text(str(value))}")
        if self.detail:
            lines += ["", redact_text(self.detail)]
        return "\n".join(lines)

    def to_adaptive_card(self) -> dict[str, Any]:
        facts = [
            {"title": "Report", "value": redact_text(self.report_name) or "n/a"},
            {"title": "Error", "value": redact_text(self.error)[:300] or "n/a"},
            {"title": "Action taken", "value": redact_text(self.action_taken) or "none"},
            {"title": "Outcome", "value": redact_text(self.outcome)},
            {"title": "Timestamp", "value": self.timestamp},
        ] + [{"title": k, "value": redact_text(str(v))[:300]} for k, v in self.facts.items()]

        body: list[dict[str, Any]] = [
            {
                "type": "TextBlock",
                "size": "Medium",
                "weight": "Bolder",
                "text": redact_text(self.title),
            },
            {"type": "FactSet", "facts": facts},
        ]
        if self.detail:
            body.append(
                {"type": "TextBlock", "wrap": True, "text": redact_text(self.detail)[:1000]}
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
                        "body": body,
                    },
                }
            ],
        }


@dataclass
class UnconfiguredTeamsNotifier:
    """Live mode with no webhook: report honestly that nothing was delivered.

    This slot used to hold ``MockTeamsNotifier``, which returns
    ``delivered: True``. That is right in a test, where the in-memory list really
    did receive the message, and wrong in a live deployment, where it means the
    run records a notification that no human will ever see.

    It also corrupts deduplication. The controller announces an incident once,
    counted against ``notified_count``, so a fabricated delivery consumes the
    single announcement and suppresses the *first real* notification after
    someone fixes the webhook.

    Not delivering is survivable; notification is not the agent's primary job.
    Claiming to have delivered is not.
    """

    attempts: list[ResolutionSummary] = field(default_factory=list)

    async def post(self, summary: ResolutionSummary) -> dict[str, Any]:
        self.attempts.append(summary)
        logger.warning(
            "Teams notification NOT delivered (%s): TEAMS_WEBHOOK_URL is not set",
            summary.title,
        )
        return {
            "status": "skipped",
            "delivered": False,
            "transport": "unconfigured",
            "reason": "TEAMS_WEBHOOK_URL is not set",
        }

    async def post_card(self, card: dict[str, Any]) -> dict[str, Any]:
        logger.warning("Teams card NOT delivered: TEAMS_WEBHOOK_URL is not set")
        return {
            "status": "skipped",
            "delivered": False,
            "transport": "unconfigured",
            "reason": "TEAMS_WEBHOOK_URL is not set",
        }


class TeamsNotifier(Protocol):
    async def post(self, summary: ResolutionSummary) -> dict[str, Any]: ...


@dataclass
class MockTeamsNotifier:
    """Collects messages in memory so tests can assert on them."""

    messages: list[ResolutionSummary] = field(default_factory=list)
    cards: list[dict[str, Any]] = field(default_factory=list)

    async def post(self, summary: ResolutionSummary) -> dict[str, Any]:
        self.messages.append(summary)
        logger.info("Teams (mock) <- %s", summary.title)
        return {"status": "ok", "delivered": True, "transport": "mock"}

    async def post_card(self, card: dict[str, Any]) -> dict[str, Any]:
        """Mirrors the real notifier so tests exercise the same code path."""
        self.cards.append(card)
        logger.info("Teams (mock) <- card")
        return {"status": "ok", "delivered": True, "transport": "mock"}

    @property
    def last(self) -> ResolutionSummary | None:
        return self.messages[-1] if self.messages else None


class WorkflowsWebhookTeamsNotifier:
    """Posts an Adaptive Card to a Power Automate **Workflows** webhook.

    Not the old Office 365 connector "Incoming Webhook" — those were retired on
    **22 May 2026** and no longer deliver. If you find a demo guide that says
    "Channel -> Connectors -> Incoming Webhook", it predates the retirement.

    The replacement is a Power Automate Workflows webhook, created from the
    channel via *Workflows -> Post to a channel when a webhook request is
    received*. The payload shape below is unchanged, which is why this class
    did not need rewriting — only the URL source did.

    Two known behavioural differences worth stating before someone notices on
    screen: posts appear as the Workflows bot rather than a custom name/icon,
    and interactive MessageCard buttons are not carried over (use Adaptive Card
    actions instead).

    Production alternative: post via Graph with an app registration so the
    message is attributable to an identity rather than to a URL that anyone
    holding it can post to.
    """

    def __init__(self, webhook_url: str, timeout: int = 20):
        self._url = webhook_url
        self._timeout = timeout

    async def post(self, summary: ResolutionSummary) -> dict[str, Any]:
        return await self._send(summary.to_adaptive_card())

    async def post_card(self, card: dict[str, Any]) -> dict[str, Any]:
        """Post an already-built card.

        ``TeamsCardApprovalGate`` looks for this method and falls back to a
        plain summary when it is missing -- which is what happened here: the
        approval card, buttons and all, was built, tested, and then quietly
        replaced with a text summary on the way out. The human saw a
        notification with nothing to click.
        """
        return await self._send(card)

    async def _send(self, payload: dict[str, Any]) -> dict[str, Any]:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(self._url, json=payload)
        except httpx.HTTPError as exc:
            # A notification failure must be reported as a failure. Swallowing
            # it produces a run that claims it told a human when it did not.
            logger.warning("Teams post failed: %s", type(exc).__name__)
            return {
                "status": "error",
                "delivered": False,
                "error_type": type(exc).__name__,
                "transport": "workflows_webhook",
            }

        ok = 200 <= resp.status_code < 300
        if not ok:
            logger.warning("Teams webhook returned %s: %s", resp.status_code, resp.text[:200])
            if resp.status_code in (404, 410):
                logger.warning(
                    "404/410 from a Teams webhook usually means a retired Office 365 "
                    "connector URL. Recreate it as a Power Automate Workflows webhook."
                )
        return {
            "status": "ok" if ok else "error",
            "delivered": ok,
            "http_status": resp.status_code,
            "transport": "workflows_webhook",
        }


# Back-compat alias. The transport changed; the interface did not.
WebhookTeamsNotifier = WorkflowsWebhookTeamsNotifier
