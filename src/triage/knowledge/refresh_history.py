"""Deterministic analysis of Power BI refresh history.

Why this is not left to the model
---------------------------------
"How close is this model to having its refresh schedule switched off?" is a
counting question with an exact answer. Handing the raw history to a model and
hoping it counts correctly — while also correctly ignoring the entries that do
not count — is the kind of thing that works in a demo and fails at 3am.

So the count is computed here and passed to the agent as evidence, in the same
way the duplicate scan is.

The rule being implemented
--------------------------
Power BI deactivates a semantic model's refresh **schedule** after four
consecutive **scheduled** refresh failures. The threshold is not configurable.

The subtlety that makes this worth a module: **on-demand and API-triggered
refreshes are a different trigger path.** An agent that retries via the REST API
is not advancing the model toward deactivation — and equally, its successful
retry does not reset the counter. Only scheduled runs move it in either
direction.

Getting that wrong in either direction is bad. Counting API retries would make
the agent escalate needlessly; assuming a successful API retry cleared the
counter would let it walk straight past a model one scheduled run away from
going silent.

Source: https://learn.microsoft.com/power-bi/connect-data/refresh-scheduled-refresh
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: Power BI deactivates the schedule on the fourth consecutive scheduled
#: failure. Not configurable, so this is a constant rather than a setting.
DEACTIVATION_THRESHOLD = 4

#: Refresh types that count toward the threshold. Everything else — ``ViaApi``,
#: ``OnDemand``, enhanced refresh — is a different trigger path.
_SCHEDULED_TYPES = {"scheduled"}

_FAILED = {"failed", "disabled"}


@dataclass(frozen=True)
class DeactivationRisk:
    """How close the refresh schedule is to being switched off."""

    consecutive_scheduled_failures: int
    threshold: int
    remaining_before_deactivation: int
    at_risk: bool
    scheduled_runs_examined: int
    ignored_non_scheduled: int

    def headline(self) -> str:
        if self.scheduled_runs_examined == 0:
            return "No scheduled refresh runs in the available history."
        if self.consecutive_scheduled_failures == 0:
            return "The most recent scheduled refresh succeeded; the schedule is not at risk."
        plural = "s" if self.consecutive_scheduled_failures != 1 else ""
        base = (
            f"{self.consecutive_scheduled_failures} consecutive scheduled refresh "
            f"failure{plural} (threshold {self.threshold})."
        )
        if self.at_risk:
            return (
                base
                + " The NEXT scheduled failure deactivates the refresh schedule, after "
                "which the report goes stale with no further alerts."
            )
        return base + f" {self.remaining_before_deactivation} more would deactivate the schedule."

    def as_evidence(self) -> dict[str, Any]:
        return {
            "consecutive_scheduled_failures": self.consecutive_scheduled_failures,
            "threshold": self.threshold,
            "remaining_before_deactivation": self.remaining_before_deactivation,
            "schedule_at_risk": self.at_risk,
            "scheduled_runs_examined": self.scheduled_runs_examined,
            "non_scheduled_runs_ignored": self.ignored_non_scheduled,
            "note": (
                "Only refreshType 'Scheduled' counts toward deactivation. On-demand and "
                "API-triggered refreshes neither advance nor reset the counter."
            ),
            "summary": self.headline(),
        }


def _is_scheduled(entry: dict[str, Any]) -> bool:
    return str(entry.get("refreshType", "")).strip().lower() in _SCHEDULED_TYPES


def _is_failure(entry: dict[str, Any]) -> bool:
    return str(entry.get("status", "")).strip().lower() in _FAILED


def assess_deactivation_risk(
    history: list[dict[str, Any]] | None,
    *,
    threshold: int = DEACTIVATION_THRESHOLD,
) -> DeactivationRisk:
    """Count consecutive scheduled failures, newest first.

    ``history`` is expected newest-first, which is what the Power BI refresh
    history endpoint returns. Non-scheduled entries are skipped entirely rather
    than treated as either a failure or a success — they are simply not part of
    this counter.
    """
    entries = list(history or [])
    scheduled = [e for e in entries if _is_scheduled(e)]
    ignored = len(entries) - len(scheduled)

    consecutive = 0
    for entry in scheduled:
        if _is_failure(entry):
            consecutive += 1
            continue
        # A successful scheduled run resets the counter.
        break

    remaining = max(threshold - consecutive, 0)
    return DeactivationRisk(
        consecutive_scheduled_failures=consecutive,
        threshold=threshold,
        remaining_before_deactivation=remaining,
        # "At risk" means one more scheduled failure switches the schedule off.
        at_risk=consecutive >= threshold - 1,
        scheduled_runs_examined=len(scheduled),
        ignored_non_scheduled=ignored,
    )
