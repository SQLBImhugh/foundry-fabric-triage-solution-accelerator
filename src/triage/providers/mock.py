"""Scripted provider — deterministic, offline, no network.

**This is not an agent.** It is a fixed state machine that emits the tool
calls a competent agent would emit, so that:

* the repo runs end-to-end on a laptop with no tenant,
* the tests assert on orchestration and policy rather than on model output,
* a demo rehearsal produces byte-identical results every time.

Switch ``TRIAGE_PROVIDER_MODE`` to ``direct`` or ``foundry`` for the real
thing. Everything downstream of this class is identical in all three modes —
that is the point of the interface.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from triage.providers.base import LLMResponse, ToolCall


def _tool_results(messages: list[dict[str, Any]]) -> dict[str, Any]:
    """Map tool name -> parsed result, for the most recent call of each tool."""
    out: dict[str, Any] = {}
    for msg in messages:
        if msg.get("role") != "tool":
            continue
        name = msg.get("name")
        if not name:
            continue
        raw = msg.get("content", "")
        try:
            out[name] = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            out[name] = {"raw": raw}
    return out


def _call_counts(messages: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for msg in messages:
        for call in msg.get("tool_calls") or []:
            fn = (call.get("function") or {}).get("name") or call.get("name")
            if fn:
                counts[fn] = counts.get(fn, 0) + 1
    return counts


@dataclass
class ScriptedProvider:
    """Emits one tool call per turn following the triage flow."""

    provider_name: str = "mock"
    model_name: str = "scripted-triage-v1"

    #: When True, attempts a second remediation after the first succeeds.
    #: Used to demonstrate that the controller — not the prompt — refuses it.
    rogue_second_refresh: bool = False

    #: When True, proposes an action that is not on the allowlist at all.
    rogue_unknown_action: bool = False

    #: When True, checks the refresh schedule and re-arms it if Power BI
    #: disabled it. Off by default so the other scenarios keep their exact
    #: tool sequences.
    check_schedule: bool = False

    turns: list[str] = field(default_factory=list)

    async def complete(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float = 0.1,
    ) -> LLMResponse:
        results = _tool_results(messages)
        counts = _call_counts(messages)
        name, args, thought = self._next_step(results, counts)
        self.turns.append(name)

        return LLMResponse(
            content=thought,
            tool_calls=[ToolCall(id=f"call_{uuid.uuid4().hex[:8]}", name=name, arguments=args)],
            finish_reason="tool_calls",
            prompt_tokens=420 + 60 * len(messages),
            completion_tokens=90,
        )

    # --- the state machine -------------------------------------------------

    def _next_step(
        self, results: dict[str, Any], counts: dict[str, int]
    ) -> tuple[str, dict[str, Any], str]:
        called = counts.keys()

        if "get_request_context" not in called:
            return ("get_request_context", {}, "Reading the request before deciding anything.")

        # Known-issue check ALWAYS precedes remediation.
        if "get_known_incidents" not in called:
            return (
                "get_known_incidents",
                {},
                "Checking whether this failure signature is already open.",
            )

        known = results.get("get_known_incidents", {})
        if known.get("known_related_issue"):
            if "notify_teams" not in called:
                return (
                    "notify_teams",
                    {
                        "title": "Known issue - no action taken",
                        "outcome": "duplicate_suppressed",
                        "action_taken": "none",
                        "detail": (
                            f"This matches open incident {known.get('incident_id')}, "
                            f"seen {known.get('occurrence_count')} time(s). Waiting for "
                            "the existing investigation rather than remediating again."
                        ),
                    },
                    "Already tracked. Notifying without acting.",
                )
            return (
                "report_resolution",
                {
                    "outcome": "duplicate_suppressed",
                    "tier": "tier_1",
                    "category": "transient",
                    "severity": "low",
                    "root_cause": known.get("diagnosed_root_cause")
                    or "Recurrence of a known open incident.",
                    "summary": (
                        "Suppressed as a duplicate of an open incident. No remediation "
                        "performed."
                    ),
                    "reasoning": [
                        "Signature matched an open incident.",
                        "Repeat remediation would add load without adding information.",
                    ],
                },
                "Reporting suppression.",
            )

        # Data quality gate — always before remediation.
        if "consult_data_quality_agent" not in called:
            return (
                "consult_data_quality_agent",
                {"reason": "Rule out a data quality cause before treating this as transient."},
                "Handing off to the Data Quality agent.",
            )

        # Chaos injection comes before any legitimate branch, so the allowlist
        # refusal is demonstrated whatever path the alert would otherwise take.
        # Scenario 4 shares the throttled alert with scenario 8; without this
        # ordering the throttle branch short-circuits and the rogue action is
        # never proposed.
        if self.rogue_unknown_action and "delete_dataset" not in called:
            return (
                "delete_dataset",
                {"justification": "Clean slate."},
                "Attempting an action outside the allowlist.",
            )

        # Capacity throttling is the one failure where the obvious fix makes it
        # worse: the capacity is already over its limits, so a retry adds load
        # to the thing that is overloaded. Postpone the work instead. Checked
        # before the transient path, because a retry here is not merely
        # unhelpful -- it is harmful.
        context = results.get("get_request_context", {})
        error_code = str(context.get("error_code") or "").lower()
        if "throttl" in error_code or "capacity" in error_code:
            deferred = results.get("defer_refresh_retry", {})
            if "defer_refresh_retry" not in called:
                return (
                    "defer_refresh_retry",
                    {
                        "reason": (
                            "The capacity rejected the refresh because it is over its "
                            "resource limits. Retrying now would add load to a "
                            "saturated capacity."
                        ),
                    },
                    "Postponing the retry rather than adding load.",
                )

            scheduled = str(deferred.get("status")) == "pending"
            if "notify_teams" not in called:
                return (
                    "notify_teams",
                    {
                        "title": (
                            "Refresh retry postponed"
                            if scheduled
                            else "Repeated throttling needs a human"
                        ),
                        "outcome": "deferred_retry" if scheduled else "needs_human",
                        "action_taken": (
                            f"Scheduled a retry for {deferred.get('due_at')}. No refresh "
                            "was attempted."
                            if scheduled
                            else "None. This dataset has been deferred as many times "
                            "as the policy allows."
                        ),
                        "detail": str(deferred.get("guidance") or ""),
                    },
                    "Reporting the postponement.",
                )
            return (
                "report_resolution",
                {
                    "outcome": "deferred_retry" if scheduled else "needs_human",
                    "tier": "tier_1",
                    "category": "transient",
                    "severity": "low",
                    "summary": (
                        f"Retry scheduled for {deferred.get('due_at')} rather than "
                        "performed now."
                        if scheduled
                        else "Throttling has recurred past the deferral limit."
                    ),
                    "root_cause": (
                        "The capacity was saturated and rejected the refresh. The model "
                        "and its source are healthy."
                    ),
                    "reasoning": [
                        "Retrying into a saturated capacity adds load to the cause.",
                        "The work is scheduled, not abandoned.",
                    ],
                },
                "Reporting the outcome.",
            )

        finding = results.get("consult_data_quality_agent", {})
        if finding.get("has_issue"):
            if "write_data_quality_flag" not in called:
                return (
                    "write_data_quality_flag",
                    {"detail": finding.get("detail", "Duplicate records detected.")},
                    "Recording the issue in the flag table.",
                )
            if "notify_teams" not in called:
                return (
                    "notify_teams",
                    {
                        "title": "Data quality issue detected",
                        "outcome": "flagged_data_quality",
                        "action_taken": "Flagged in the data quality table. No automated fix.",
                        "detail": finding.get("detail", ""),
                    },
                    "Notifying the owner with the specific finding.",
                )
            return (
                "report_resolution",
                {
                    "outcome": "flagged_data_quality",
                    "tier": "tier_2",
                    "category": "data_quality",
                    "severity": "medium",
                    "root_cause": finding.get("detail", "Duplicate records in the source table."),
                    "summary": "Data quality issue flagged and notified. No automated remediation.",
                    "reasoning": [
                        "Data Quality agent returned duplicate evidence.",
                        "Refreshing a report does not remove duplicate source rows.",
                        "Remediation deliberately withheld; a human owns the fix.",
                    ],
                },
                "Reporting a flagged data quality issue.",
            )

        # No data quality issue -> transient path.
        if "get_dataset_refresh_history" not in called:
            return (
                "get_dataset_refresh_history",
                {"top": 5},
                "Checking history to confirm this is a one-off.",
            )

        # The schedule is a separate switch from the data. Power BI turns it off
        # after four consecutive failures and never turns it back on, so a model
        # can be healthy and still never refresh again -- and because no
        # scheduled run happens, no further alert is raised either.
        if self.check_schedule:
            if "get_refresh_schedule" not in called:
                return (
                    "get_refresh_schedule",
                    {},
                    "Checking whether the refresh schedule is still switched on.",
                )

            schedule = results.get("get_refresh_schedule", {})
            if schedule.get("enabled") is False and "reenable_refresh_schedule" not in called:
                return (
                    "reenable_refresh_schedule",
                    {
                        "justification": (
                            "Power BI disabled the schedule after consecutive failures. "
                            "The most recent refresh completed successfully, so the "
                            "cause is resolved and the schedule can be re-armed."
                        ),
                    },
                    "Proposing to re-arm the schedule. This needs a human decision.",
                )

            if "reenable_refresh_schedule" in called:
                restored = results.get("reenable_refresh_schedule", {})
                worked = str(restored.get("status")) == "Completed"
                if "notify_teams" not in called:
                    return (
                        "notify_teams",
                        {
                            "title": (
                                "Refresh schedule re-armed"
                                if worked
                                else "Refresh schedule still disabled"
                            ),
                            "outcome": "resolved" if worked else "needs_human",
                            "action_taken": (
                                "Re-enabled the scheduled refresh after approval."
                                if worked
                                else "None. The schedule remains off."
                            ),
                            "detail": str(
                                restored.get("reason") or restored.get("detail") or ""
                            ),
                        },
                        "Reporting the schedule outcome.",
                    )
                return (
                    "report_resolution",
                    {
                        "outcome": "resolved" if worked else "needs_human",
                        "summary": (
                            "The schedule was disabled by Power BI after repeated "
                            "failures. The cause is fixed and the schedule is back on."
                            if worked
                            else "The schedule is disabled and was not re-armed."
                        ),
                        "root_cause": (
                            "Power BI deactivated the refresh schedule after "
                            "consecutive failures; nothing re-enables it automatically."
                        ),
                    },
                    "Reporting the outcome.",
                )

        # A REPEATING failure is a different problem. Another refresh will not
        # help - the same thing will happen again. The right move is a fix with
        # a wider blast radius, which means asking a human first.
        history = results.get("get_dataset_refresh_history", {}).get("history", []) or []
        failures = [h for h in history if str(h.get("status")) == "Failed"]
        repeating = len(failures) >= 2

        if repeating:
            rebind = results.get("rebind_dataset_gateway", {})
            if "rebind_dataset_gateway" not in called:
                return (
                    "rebind_dataset_gateway",
                    {
                        "target_gateway": "gw-onprem-02",
                        "justification": (
                            f"{len(failures)} consecutive refreshes failed on the same "
                            "gateway, so retrying will reproduce the failure. Rebinding "
                            "to the standby gateway is the smallest fix that could work."
                        ),
                    },
                    "Proposing a gateway rebind. This needs a human decision.",
                )

            approved = bool(rebind.get("succeeded"))
            if "notify_teams" not in called:
                return (
                    "notify_teams",
                    {
                        "title": (
                            "Gateway rebind applied" if approved else "Gateway rebind not approved"
                        ),
                        "outcome": "resolved" if approved else "approval_denied",
                        "action_taken": (
                            "Rebound the dataset to the standby gateway after approval."
                            if approved
                            else "None. The proposed fix was not authorised."
                        ),
                        "detail": str(rebind.get("reason") or rebind.get("detail") or ""),
                    },
                    "Reporting the outcome of the approval.",
                )

            if approved:
                return (
                    "report_resolution",
                    {
                        "outcome": "resolved",
                        "tier": "tier_2",
                        "category": "config",
                        "severity": "medium",
                        "root_cause": (
                            "Repeated refresh failures against a single data gateway."
                        ),
                        "summary": (
                            "Diagnosed a repeating gateway failure, proposed a rebind, and "
                            "applied it once a human approved."
                        ),
                        "reasoning": [
                            "Refresh history showed the same failure repeating.",
                            "A further retry would reproduce it.",
                            "A rebind affects other datasets, so it required approval.",
                            "Approval was granted and the rebind completed.",
                        ],
                    },
                    "Reporting an approved, applied fix.",
                )

            return (
                "report_resolution",
                {
                    "outcome": "approval_denied",
                    "tier": "tier_2",
                    "category": "config",
                    "severity": "medium",
                    "root_cause": (
                        "Repeated refresh failures against a single data gateway."
                    ),
                    "summary": (
                        "Diagnosed a repeating gateway failure and proposed a rebind. "
                        "A human declined, so nothing was changed."
                    ),
                    "reasoning": [
                        "Refresh history showed the same failure repeating.",
                        "The proposed fix required human authorisation.",
                        "Authorisation was not given, so no action was taken.",
                    ],
                },
                "Reporting a declined proposal.",
            )

        refresh_count = counts.get("refresh_powerbi_dataset", 0)
        if refresh_count == 0:
            return (
                "refresh_powerbi_dataset",
                {"justification": "Transient refresh failure with a clean data quality check."},
                "Applying the single permitted remediation.",
            )

        if self.rogue_second_refresh and refresh_count == 1:
            return (
                "refresh_powerbi_dataset",
                {"justification": "Trying once more for good measure."},
                "Attempting a second remediation.",
            )

        refresh = results.get("refresh_powerbi_dataset", {})
        blocked = refresh.get("status") == "blocked_by_policy"
        succeeded = bool(refresh.get("succeeded"))

        if "notify_teams" not in called:
            return (
                "notify_teams",
                {
                    "title": "Transient failure resolved" if succeeded else "Triage needs a human",
                    "outcome": "resolved" if succeeded else "needs_human",
                    "action_taken": "Power BI dataset refresh via REST API",
                    "detail": refresh.get("detail", ""),
                },
                "Posting the resolution summary.",
            )

        if succeeded and not blocked:
            return (
                "report_resolution",
                {
                    "outcome": "resolved",
                    "tier": "tier_1",
                    "category": "transient",
                    "severity": "low",
                    "root_cause": "Transient dataset refresh failure.",
                    "summary": "Refresh re-triggered via the Power BI REST API and completed.",
                    "reasoning": [
                        "No open incident for this signature.",
                        "Data Quality agent reported no issue.",
                        "Refresh history showed an isolated failure.",
                        "Single remediation applied and verified.",
                    ],
                },
                "Reporting a resolved Tier 1 failure.",
            )

        return (
            "report_resolution",
            {
                "outcome": "needs_human",
                "tier": "needs_human",
                "category": "app",
                "severity": "medium",
                "root_cause": "Remediation did not succeed or was refused by policy.",
                "summary": "Escalating to a human; the agent's remediation allowance is spent.",
                "reasoning": ["Remediation unavailable or unsuccessful.", "Escalating rather than retrying."],
            },
            "Escalating.",
        )

    async def close(self) -> None:
        return None


@dataclass
class ScriptedDataQualityProvider:
    """Scripted reasoning for the Data Quality agent.

    The agent no longer calls a tool: the orchestrator runs the deterministic
    scan and passes the evidence in, so this provider just interprets whatever
    evidence appears in the user message.
    """

    provider_name: str = "mock"
    model_name: str = "scripted-dq-v1"

    async def complete(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float = 0.1,
    ) -> LLMResponse:
        evidence: dict[str, Any] = {}
        for msg in messages:
            if msg.get("role") != "user":
                continue
            text = str(msg.get("content", ""))
            start, end = text.find("{"), text.rfind("}")
            if 0 <= start < end:
                try:
                    evidence = json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    evidence = {}

        dupes = int(evidence.get("duplicate_row_count", 0) or 0)
        table = evidence.get("table", "the source table")
        keys = ", ".join(evidence.get("key_columns") or [])

        if dupes > 0:
            payload = {
                "has_issue": True,
                "issue_type": "duplicates",
                "confidence": 1.0,
                "detail": (
                    f"Table {table} contains {dupes} duplicate rows across "
                    f"{evidence.get('duplicate_group_count', 0)} key groups on ({keys})"
                ),
                "recommended_action": "flag_and_notify",
            }
        else:
            payload = {
                "has_issue": False,
                "issue_type": "none",
                "confidence": 1.0,
                "detail": f"No duplicate keys found in {table}.",
                "recommended_action": "no_action",
            }

        return LLMResponse(
            content=json.dumps(payload),
            finish_reason="stop",
            prompt_tokens=320,
            completion_tokens=60,
        )

    async def close(self) -> None:
        return None
