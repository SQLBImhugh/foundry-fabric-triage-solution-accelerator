"""Typed contracts for the triage pipeline.

Every agent boundary is a Pydantic model. An agent that returns prose is an
agent you cannot test, alert on, or replay.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Terminal outcomes
# ---------------------------------------------------------------------------

TerminalOutcome = Literal[
    "resolved",              # Tier 1, remediation applied, verified
    "flagged_data_quality",  # DQ issue found; flagged + notified, no auto-fix
    "duplicate_suppressed",  # known open incident; occurrence counted, no action
    "approval_denied",       # agent proposed a bounded fix; a human said no
    "deferred_retry",        # throttled; retry scheduled for later, nothing done yet
    "needs_human",           # agent stopped without resolving (no crash)
    "declared_failed",       # agent explicitly declared it unresolvable
    "agent_crashed",         # unhandled exception in the loop
    "timed_out",             # wall-clock budget exhausted
    "budget_exceeded",       # token or tool-call budget exhausted
    "max_turns_exceeded",    # loop hit max_llm_turns without resolving
    "policy_blocked",        # controller refused a disallowed/over-budget action
]

RESOLVED_OUTCOMES: frozenset[str] = frozenset(
    {"resolved", "flagged_data_quality", "duplicate_suppressed"}
)


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# Inbound
# ---------------------------------------------------------------------------


class BIRequest(BaseModel):
    """One BI request / alert email pulled from the monitored inbox."""

    request_id: str
    received_at: str = Field(default_factory=_utcnow)
    sender: str = ""
    subject: str = ""
    body: str = ""

    # Parsed hints. Deliberately optional — the agent must cope when the
    # email is unstructured, which is the normal case in the wild.
    report_name: str | None = None
    dataset_id: str | None = None
    workspace_id: str | None = None
    error_code: str | None = None

    # "interactive" covers an alert pasted straight into the Foundry Playground.
    # It is a genuinely different provenance from a polled mailbox and the
    # incident record should say so rather than claim the mail arrived.
    #
    # "detector" is the silent-failure path, where nothing arrived at all: the
    # refresh reported success and the scanner found the data wrong anyway.
    # Worth distinguishing, because "nobody was told" is the interesting part
    # of those incidents.
    source: Literal["mock", "graph", "interactive", "detector"] = "mock"

    def error_text(self) -> str:
        """The blob a signature is computed over."""
        return f"{self.subject}\n{self.body}".strip()


# ---------------------------------------------------------------------------
# Data Quality agent
# ---------------------------------------------------------------------------


class DuplicateEvidence(BaseModel):
    """Deterministic, privacy-safe evidence of duplication.

    Carries key values and counts only — never whole rows. The demo dataset is
    synthetic, but the shape has to be right, because the shape is what a
    customer copies into production.
    """

    table: str
    key_columns: list[str]
    duplicate_group_count: int = 0
    duplicate_row_count: int = 0
    total_row_count: int = 0
    sample_keys: list[str] = Field(default_factory=list)

    def headline(self) -> str:
        keys = ", ".join(self.key_columns) or "no key"
        if self.duplicate_row_count == 0:
            return (
                f"No duplicate keys in {self.table} "
                f"({self.total_row_count} rows scanned on ({keys}))"
            )
        return (
            f"Table {self.table} contains {self.duplicate_row_count} duplicate rows "
            f"across {self.duplicate_group_count} key groups on ({keys})"
        )


class DataQualityFinding(BaseModel):
    """Structured hand-back from the Data Quality agent to the Triage agent.

    Note ``confidence`` and ``recommended_action``: this agent *reports*, it
    does not decide. The orchestrator owns the decision. That separation is
    what lets you add a third agent later without rewriting the flow.
    """

    has_issue: bool = False
    issue_type: Literal["duplicates", "none", "unknown"] = "none"
    confidence: float = 0.0
    detail: str = ""
    evidence: DuplicateEvidence | None = None
    checked_tables: list[str] = Field(default_factory=list)
    recommended_action: Literal["flag_and_notify", "no_action", "escalate"] = "no_action"
    agent_name: str = "DataQualityAgent"


class DataQualityFlag(BaseModel):
    """A row written to the data quality flag table."""

    flag_id: str
    flagged_at: str = Field(default_factory=_utcnow)
    request_id: str
    table_name: str
    issue_type: str
    key_columns: str
    duplicate_group_count: int
    duplicate_row_count: int
    total_row_count: int
    detail: str
    detected_by: str = "DataQualityAgent"
    status: Literal["open", "acknowledged", "resolved"] = "open"


# ---------------------------------------------------------------------------
# Triage agent
# ---------------------------------------------------------------------------


class TriageClassification(BaseModel):
    """The Triage agent's reasoning output, before any action is taken."""

    tier: Literal["tier_1", "tier_2", "needs_human"] = "needs_human"
    category: Literal["transient", "data_quality", "config", "app", "user"] = "app"
    severity: Literal["low", "medium", "high"] = "medium"
    root_cause: str = ""
    reasoning: list[str] = Field(default_factory=list)
    proposed_action: str = ""
    action_params: dict[str, Any] = Field(default_factory=dict)
    requires_human: bool = True
    confidence: float = 0.0


class ApprovalRecord(BaseModel):
    """What was asked of a human, and what they said.

    Persisted on the result and the incident. A denial is the highest-signal
    event in the whole system: the agent proposed something a person judged
    wrong, which is precisely the input you want when deciding what to automate
    next — and what never to.
    """

    action: str
    fingerprint: str
    requested_at: str
    justification: str = ""
    impact: str = ""
    granted: bool = False
    outcome: str = "granted"  # granted | denied | timed_out | error
    decided_by: str = ""
    decided_at: str = ""
    reason: str = ""
    waited_ms: int = 0


class TriageAction(BaseModel):
    """One tool call executed during a run — the audit trail."""

    tool_name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    result_summary: str = ""
    timestamp: str = Field(default_factory=_utcnow)
    duration_ms: int = 0
    is_remediation: bool = False
    blocked: bool = False


class TriageResult(BaseModel):
    """Terminal result of one triage run. Every field is safe to log."""

    outcome: TerminalOutcome
    summary: str = ""
    request_id: str = ""
    signature: str = ""
    signature_version: str = "v1"

    root_cause: str = ""
    action_taken: str = ""
    classification: TriageClassification | None = None
    dq_finding: DataQualityFinding | None = None

    actions: list[TriageAction] = Field(default_factory=list)
    approvals: list[ApprovalRecord] = Field(default_factory=list)
    llm_turns: int = 0
    tool_calls: int = 0
    write_actions: int = 0
    attempted_actions: int = 0
    tokens_used: int = 0
    wall_clock_ms: int = 0
    blocked_attempts: list[str] = Field(default_factory=list)
    denied_actions: list[str] = Field(default_factory=list)

    # A remediation that worked but was never reported is still an operational
    # failure — the human who needed to know was not told. It does not change
    # the outcome, but it must not be invisible.
    notification_failed: bool = False

    # Whether a card actually went out, and whether the controller deliberately
    # held it back because the incident had already been announced. Kept apart
    # from ``notification_failed`` on purpose: one is a fault, the other is the
    # deduplication working.
    notification_delivered: bool = False
    notification_suppressed: bool = False

    # Populated only on ``agent_crashed`` — so "why did it crash" is answerable
    # from the incident queue without grepping App Insights.
    exception_class: str = ""
    exception_message: str = ""

    started_at: str = Field(default_factory=_utcnow)
    finished_at: str = Field(default_factory=_utcnow)

    @property
    def succeeded(self) -> bool:
        return self.outcome in RESOLVED_OUTCOMES


# ---------------------------------------------------------------------------
# Incident store
# ---------------------------------------------------------------------------


class Incident(BaseModel):
    """A deduplicated failure record.

    Persisted for EVERY terminal outcome, not just successes. An earlier
    production system persisted only successful recoveries and consequently
    missed 10 agent crashes over two weeks — they left zero trace in the queue
    that operators actually read.
    """

    id: str
    signature: str
    signature_version: str = "v1"
    outcome: TerminalOutcome
    status: Literal["open", "investigating", "resolved", "wont_fix"] = "open"

    request_id: str = ""
    report_name: str = ""
    source: str = "powerbi_refresh_failure"

    occurrence_count: int = 1
    first_seen_at: str = Field(default_factory=_utcnow)
    last_seen_at: str = Field(default_factory=_utcnow)

    # How many times this incident has been announced to a channel. A recurring
    # failure must not produce a card per occurrence: a five-minute sweep over
    # an unresolved alert posted 12 identical cards an hour, which is precisely
    # the alert fatigue this system exists to remove. The controller consults
    # this before delivering, so the limit does not depend on prompt wording.
    notified_count: int = 0
    last_notified_at: str = ""

    # Redacted at the store boundary — never write these directly.
    original_error: str = ""
    diagnosed_root_cause: str = ""
    action_applied: str = ""

    action_type: Literal[
        "nondeterministic_retry",
        "known_workaround",
        "deterministic_fix",
        "flag_only",
        "none",
        "unknown",
    ] = "unknown"
    requires_investigation: bool = False

    # Provenance — which agent, which prompt, which model produced this.
    agent_name: str = ""
    prompt_version_hash: str = ""
    model_provider: str = ""
    model_name: str = ""
    app_version: str = ""

    redaction_applied: bool = False
    redaction_kinds: list[str] = Field(default_factory=list)
    triage_notes: str = ""
