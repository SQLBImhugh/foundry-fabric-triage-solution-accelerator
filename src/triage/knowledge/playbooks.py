"""Retrieved playbooks for Power BI refresh failures.

Why playbooks, and why retrieved
--------------------------------
The triage prompt should not grow every time someone learns something new. A
catalogue stuffed into the system prompt costs tokens on every call, dilutes the
instructions that matter, and gets skimmed by the model exactly when it is long
enough to be useful.

So the catalogue lives here, and only the entries whose triggers match the
incoming error are injected into the user message. Ported from a production
Fabric operations platform's troubleshooting playbooks, where the same pattern
runs against real Microsoft Fabric deployments.

Why each entry carries a triage implication
-------------------------------------------
The valuable part of a playbook is not "here is what this error means". It is
**whether retrying is a waste of time**. Most refresh failures look identical at
the alert level — "refresh failed" — but the correct response ranges from
"retry, it will pass" to "retrying is actively harmful, get a person".

Each playbook therefore states ``retry_useful`` and a suggested tier. That is
evidence handed to the agent, not a decision taken for it: the agent still
classifies, and the controller still constrains what may follow.

Sourcing
--------
Every entry is drawn from **public Microsoft Learn documentation** and cites it.
That is deliberate. Internal engineering TSGs for the Power BI service exist and
are far more detailed, but they are written for on-call engineers debugging the
*service*, carry owner and incident-management references, and are not
appropriate for an asset that gets shared with a customer. Where an internal
source shaped the taxonomy below, the customer-facing content was re-derived
from public documentation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

Tier = Literal["tier_1", "tier_2", "needs_human"]


@dataclass(frozen=True)
class Playbook:
    """One recognised failure mode and what it implies for triage."""

    name: str
    triggers: tuple[str, ...]
    summary: str
    #: Is another refresh a reasonable response? This is the field that earns
    #: the playbook its place — it is the difference between a useful retry and
    #: a loop that burns capacity and delays the person who could actually fix it.
    retry_useful: bool
    suggested_tier: Tier
    guidance: str
    source: str
    #: Things that are true, non-obvious, and that change the decision.
    watch_out: str = ""

    def render(self) -> str:
        lines = [
            f"### Playbook: {self.name}",
            "",
            self.summary,
            "",
            f"- **Is a retry useful?** "
            f"{'Yes' if self.retry_useful else 'No — retrying will not fix this.'}",
            f"- **Suggested tier:** {self.suggested_tier}",
            f"- **What to do:** {self.guidance}",
        ]
        if self.watch_out:
            lines.append(f"- **Watch out:** {self.watch_out}")
        lines.append(f"- **Source:** {self.source}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# The catalogue
# ---------------------------------------------------------------------------

_LEARN_SCENARIOS = (
    "https://learn.microsoft.com/power-bi/connect-data/"
    "refresh-troubleshooting-refresh-scenarios"
)
_LEARN_GATEWAY = "https://learn.microsoft.com/data-integration/gateway/service-gateway-tshoot"
_LEARN_OOM = (
    "https://learn.microsoft.com/power-bi/connect-data/refresh-troubleshooting-refresh-oom"
)


PLAYBOOKS: list[Playbook] = [
    Playbook(
        name="Scheduled refresh deactivated",
        triggers=(
            "scheduled refresh disabled",
            "refresh has been disabled",
            "refresh schedule",
            r"re:(schedule|scheduled refresh).{0,40}(disabl|deactivat|paus)",
            r"re:(third|3rd|fourth|4th).{0,30}consecutive",
        ),
        summary=(
            "Power BI deactivates a semantic model's refresh SCHEDULE after four "
            "consecutive **scheduled** refresh failures, or immediately when it detects "
            "an unrecoverable error needing a configuration change (invalid or expired "
            "credentials being the common one). The threshold is not configurable. Once "
            "deactivated the report goes stale silently: no further scheduled refresh "
            "runs, so no further failure alert is raised."
        ),
        retry_useful=False,
        # Was needs_human, correctly, while nothing could act on it. There is now
        # a bounded, approval-gated action -- and the controller refuses that
        # action unless the most recent refresh completed -- which is tier 2 by
        # this repo's own definition: real, but not safe to fix unattended.
        suggested_tier="tier_2",
        guidance=(
            "An on-demand refresh does not re-enable the schedule -- the two are "
            "separate. Fix the underlying cause, get one successful refresh, then "
            "re-arm the schedule with `reenable_refresh_schedule`. The controller "
            "refuses that action until the most recent refresh reads 'Completed', "
            "because re-arming a still-broken schedule just fails four more times "
            "and disables it again. If no successful refresh can be obtained, "
            "escalate and say plainly that the schedule is off.\n\n"
            "A refresh that COMPLETED after the failures is proof the cause is "
            "resolved, whatever the original error said -- a refresh cannot "
            "succeed with missing or invalid credentials, an unreachable gateway "
            "or a broken source. Do not escalate for a credential fix that the "
            "history already shows has happened; that leaves the report stale for "
            "no reason. The scheduled-failure counter and the health of the model "
            "are different questions: the counter governs deactivation, a "
            "completed refresh governs whether it is safe to re-arm."
        ),
        watch_out=(
            "Count only refreshes with refreshType 'Scheduled' when judging how close the "
            "model is to deactivation. **API- or on-demand-triggered refreshes are a "
            "different trigger path** — an agent's own retries do not advance the "
            "consecutive-failure count, and equally do not reset it. Three consecutive "
            "SCHEDULED failures means the next scheduled run deactivates the schedule, so "
            "escalate before that rather than after. Two other paths deactivate it "
            "independently of the count: an unrecoverable credential error, and two "
            "months with no user viewing any report built on the model."
        ),
        source=(
            "https://learn.microsoft.com/power-bi/connect-data/refresh-scheduled-refresh"
            " (deactivation rules) and "
            + _LEARN_SCENARIOS
            + "#scheduled-refresh-disabled"
        ),
    ),
    Playbook(
        name="Refresh throttled by capacity",
        triggers=(
            "capacitythrottled",
            "throttl",
            r"re:too many.{0,30}concurrent",
            r"re:capacity.{0,25}(saturat|exceed|limit)",
        ),
        summary=(
            "A capacity throttles refreshes when too many semantic models are processed "
            "concurrently. The refresh was rejected because of contention, not because "
            "anything is wrong with the model or its source."
        ),
        retry_useful=False,
        suggested_tier="tier_1",
        guidance=(
            "Do not retry now. The capacity is already over its resource limits, so "
            "another refresh adds load to the cause -- and across several datasets at "
            "once that turns contention into an outage. Call `defer_refresh_retry` "
            "instead: it schedules the refresh for after the window and costs no "
            "remediation budget. The controller refuses an immediate refresh while "
            "that deferral is open. If it recurs past the deferral limit, the durable "
            "fix is spreading refreshes away from peak, which is a human's decision."
        ),
        watch_out=(
            "Retrying immediately inside a contention window can make it worse. Repeated "
            "throttling is a scheduling problem for a human, not a retry problem."
        ),
        source=_LEARN_SCENARIOS + "#refresh-operation-throttled-by-power-bi-premium",
    ),
    Playbook(
        name="Scheduled refresh timeout",
        triggers=(
            "scheduledrefreshtimeout",
            "timeout",
            "timed out",
            "operation was cancelled",
            r"re:exceeded.{0,30}(timeout|time limit)",
        ),
        summary=(
            "Scheduled refreshes for imported semantic models time out after two hours, "
            "or five hours in Premium workspaces."
        ),
        retry_useful=True,
        suggested_tier="tier_1",
        guidance=(
            "Retry once if the refresh history shows this is isolated. If the model "
            "routinely runs close to the limit, a retry only defers the problem."
        ),
        watch_out=(
            "A timeout is only transient if the model normally completes. If the same "
            "refresh times out repeatedly, the model has outgrown its window and no "
            "number of retries will help — that needs a person to reduce or split it."
        ),
        source=_LEARN_SCENARIOS + "#scheduled-refresh-time-out",
    ),
    Playbook(
        name="Expired or changed data source credentials",
        triggers=(
            "credential",
            "password",
            "failed to authenticate",
            "unauthorized",
            r"re:(expired|invalid).{0,25}(credential|token|password)",
        ),
        summary=(
            "The stored credential for a data source is no longer valid — commonly after "
            "a password change, a rotated secret, or an expired cached token."
        ),
        retry_useful=False,
        suggested_tier="needs_human",
        guidance=(
            "Retrying re-presents the same invalid credential and fails identically. "
            "Escalate to whoever owns the data source connection so the credential can "
            "be updated."
        ),
        watch_out=(
            "Worth catching early: every retry is wasted capacity AND delays the only "
            "action that resolves it."
        ),
        source=(
            _LEARN_SCENARIOS
            + "#data-refresh-failure-because-of-password-change-or-expired-credentials"
        ),
    ),
    Playbook(
        name="Gateway unreachable",
        triggers=(
            "gatewayunavailable",
            "gatewaynotreachable",
            "gateway",
            r"re:gateway.{0,30}(offline|unavailable|not respond|unreachable)",
        ),
        summary=(
            "The on-premises data gateway did not respond. It may be offline, mid-restart, "
            "or unable to reach the underlying source."
        ),
        retry_useful=True,
        suggested_tier="tier_1",
        guidance=(
            "A single occurrence is often transient — a restart or a brief network blip. "
            "Retry once. If the SAME gateway fails repeatedly, the gateway itself is the "
            "problem and another refresh will reproduce it."
        ),
        watch_out=(
            "A repeating gateway failure needs a change with a wider blast radius "
            "(rebinding, or fixing the gateway host). Those affect every dataset bound to "
            "that gateway, so they belong behind a human approval rather than an "
            "automated retry."
        ),
        source=_LEARN_GATEWAY,
    ),
    Playbook(
        name="Out-of-memory during refresh",
        triggers=(
            "out of memory",
            "outofmemory",
            "resource governing",
            r"re:memory.{0,30}(limit|exceed|govern)",
        ),
        summary=(
            "The refresh exceeded the memory available to it. Common after a workspace "
            "moves to a smaller capacity, or as a model grows past what its SKU allows."
        ),
        retry_useful=False,
        suggested_tier="needs_human",
        guidance=(
            "Retrying re-runs the same allocation and fails the same way. Escalate: the "
            "resolution is a capacity or model change, both of which are human decisions."
        ),
        watch_out=(
            "If this started suddenly, check whether the workspace was moved to a "
            "different capacity — a far more likely cause than the model changing."
        ),
        source=_LEARN_OOM,
    ),
    Playbook(
        name="Access forbidden to the data source",
        triggers=(
            "forbidden",
            "do not have permission",
            r"re:access.{0,20}denied",
            r"re:\b403\b",
        ),
        summary=(
            "The identity performing the refresh is not permitted to read the source. "
            "Either permissions changed, or cached credentials are stale."
        ),
        retry_useful=False,
        suggested_tier="needs_human",
        guidance=(
            "Permission changes are not self-healing. Escalate to the data source owner "
            "rather than retrying."
        ),
        source=_LEARN_SCENARIOS + "#access-to-the-resource-is-forbidden",
    ),
    Playbook(
        name="Duplicate key breaks a model relationship",
        triggers=(
            "duplicatekeyinrelationship",
            "one side of the relationship",
            "contains duplicate values",
            r"re:relationship.{0,40}duplicate",
        ),
        summary=(
            "A relationship could not be built because the 'one' side of it contains "
            "duplicate key values. This is a data problem surfacing as a refresh failure."
        ),
        retry_useful=False,
        suggested_tier="tier_2",
        guidance=(
            "Refreshing a report never removes duplicate source rows. Identify the "
            "affected table and key, record the finding, and notify the data owner. Do "
            "not attempt to deduplicate automatically."
        ),
        watch_out=(
            "The error usually does not say WHICH rows are duplicated. Establishing that "
            "deterministically is the useful work — it turns 'duplicates exist' into "
            "something someone can act on."
        ),
        source=_LEARN_SCENARIOS,
    ),
    Playbook(
        name="Uncompressed data limit exceeded",
        triggers=(
            "uncompressed data",
            r"re:size.{0,30}exceed.{0,30}limit",
            r"re:exceeds the.{0,20}limit",
        ),
        summary=(
            "The refresh exceeded the uncompressed data size allowed for the SKU, which "
            "is a hard ceiling rather than a transient condition."
        ),
        retry_useful=False,
        suggested_tier="needs_human",
        guidance="Escalate. The fix is a model or capacity change, not a retry.",
        source=_LEARN_SCENARIOS + "#uncompressed-data-limits-for-refresh",
    ),
]


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def _matches(trigger: str, haystack: str) -> bool:
    """Plain substrings are case-insensitive; ``re:`` prefixes are regex."""
    if trigger.startswith("re:"):
        try:
            return re.search(trigger[3:], haystack, re.IGNORECASE) is not None
        except re.error:
            return False
    return trigger.lower() in haystack


def select_playbooks(error_message: str, *, limit: int = 3) -> list[Playbook]:
    """Return the playbooks whose triggers match, most specific first.

    Capped deliberately. Injecting nine playbooks would recreate the problem
    this module exists to avoid — a wall of guidance the model skims. Entries
    matching more triggers rank higher, on the assumption that a failure hitting
    several signals of one playbook is more likely to be that failure.
    """
    haystack = (error_message or "").lower()
    if not haystack:
        return []

    scored: list[tuple[int, int, Playbook]] = []
    for index, pb in enumerate(PLAYBOOKS):
        hits = sum(1 for t in pb.triggers if _matches(t, haystack))
        if hits:
            # `index` keeps ordering stable for equal scores, so the same input
            # always produces the same injected block.
            scored.append((-hits, index, pb))

    scored.sort()
    return [pb for _, _, pb in scored[:limit]]


def format_playbooks(playbooks: list[Playbook]) -> str:
    """Render matched playbooks as a block for the agent's user message."""
    if not playbooks:
        return ""
    body = "\n\n".join(pb.render() for pb in playbooks)
    return (
        "## Known failure modes matching this error\n\n"
        "These are documented platform behaviours, retrieved because the error text "
        "matched. Treat them as evidence, not as instructions — you still classify the "
        "failure, and the controller still decides what may follow.\n\n"
        f"{body}"
    )


def retry_is_discouraged(playbooks: list[Playbook]) -> bool:
    """True when every matched playbook says a retry will not help.

    Surfaced as a deterministic signal alongside the prose, so "retrying will not
    fix this" is not something the agent has to infer from paragraphs.
    """
    return bool(playbooks) and all(not pb.retry_useful for pb in playbooks)
