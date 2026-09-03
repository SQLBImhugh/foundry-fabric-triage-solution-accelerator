"""Incident store — dedup, occurrence counting, and redaction at the boundary.

Two responsibilities:

1. Answer "have we seen this before?" so the orchestrator can suppress a
   repeat instead of re-remediating (the *Known Related Issue* branch).
2. Persist **every** terminal outcome, including crashes and policy blocks.

Redaction is applied inside :meth:`record` rather than at call sites, so a
future code path cannot forget it.

The JSON-file backend is deliberate: the demo must survive a process restart
between Scenario 1 and Scenario 1b, otherwise the dedup beat doesn't land.
Swap in Cosmos/SQL for production by implementing the same three methods.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from triage.models import Incident, TriageResult
from triage.redaction import redact
from triage.signature import incident_id

logger = logging.getLogger("triage.store")

# Actions that resolved the issue vs. merely observed it. Drives
# ``requires_investigation`` — a retry that worked is far less interesting
# than a crash or a policy block.
_ACTION_TYPES: dict[str, str] = {
    "refresh_powerbi_dataset": "nondeterministic_retry",
    "rebind_dataset_gateway": "known_workaround",
    # Restoring a schedule the platform disabled. Deterministic: the state it
    # produces is known exactly, unlike a retry, which may or may not work.
    "reenable_refresh_schedule": "deterministic_fix",
    "write_data_quality_flag": "flag_only",
    "": "none",
}

_INVESTIGATE_OUTCOMES = frozenset(
    {
        "agent_crashed",
        "policy_blocked",
        "budget_exceeded",
        "max_turns_exceeded",
        "timed_out",
        # An escalation is the agent saying "I could not do this". That is
        # precisely the population you mine to decide what to automate next,
        # so it belongs in the investigate queue rather than out of it.
        "needs_human",
        "declared_failed",
        # A denial is the highest-signal event of all: the agent proposed
        # something a person judged wrong. That is the input for deciding what
        # to automate next - and what never to.
        "approval_denied",
        # Deliberately absent: `deferred_retry`. A postponed retry is work in
        # hand, not a problem for a person. If it is still throttled after the
        # deferral limit the controller reports `needs_human` instead, which is
        # in this set and does reach the queue.
    }
)


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _status_for(outcome: str) -> str:
    """Status is derived from the outcome, never set independently.

    Keeping these in sync matters: an incident left ``open`` after it was
    resolved keeps suppressing new alerts forever, so a real recurrence is
    silently swallowed.
    """
    return "resolved" if outcome == "resolved" else "open"


def _needs_investigation(result: TriageResult) -> bool:
    return (
        result.outcome in _INVESTIGATE_OUTCOMES
        or bool(result.blocked_attempts)
        or bool(getattr(result, "notification_failed", False))
    )


class IncidentStore(Protocol):
    def find_open(self, signature: str) -> Incident | None: ...
    def record(self, result: TriageResult, **provenance) -> Incident: ...
    def list_all(self) -> list[Incident]: ...
    def reset(self) -> None: ...


class InMemoryIncidentStore:
    """Reference implementation. Thread-safe; not durable."""

    def __init__(self) -> None:
        self._items: dict[str, Incident] = {}
        self._lock = threading.RLock()

    # --- reads -------------------------------------------------------------

    def find_open(self, signature: str) -> Incident | None:
        """Return an *open* incident for this signature, if one exists.

        Only open incidents suppress. A resolved incident means the underlying
        problem was fixed; a recurrence after that is genuinely new
        information and must be allowed to trigger action again.
        """
        with self._lock:
            found = self._find_open_unlocked(signature)
            return found.model_copy(deep=True) if found else None

    def _find_open_unlocked(self, signature: str) -> Incident | None:
        for inc in self._items.values():
            if inc.signature == signature and inc.status in ("open", "investigating"):
                return inc
        return None

    def _find_open_ref(self, signature: str) -> Incident | None:
        """Live reference (not a copy) to an open incident for this signature."""
        with self._lock:
            return self._find_open_unlocked(signature)

    def get(self, incident_id_: str) -> Incident | None:
        with self._lock:
            found = self._items.get(incident_id_)
            return found.model_copy(deep=True) if found else None

    def list_all(self) -> list[Incident]:
        with self._lock:
            return [i.model_copy(deep=True) for i in self._items.values()]

    # --- writes ------------------------------------------------------------

    def record(
        self,
        result: TriageResult,
        *,
        report_name: str = "",
        original_error: str = "",
        agent_name: str = "",
        prompt_version_hash: str = "",
        model_provider: str = "",
        model_name: str = "",
        app_version: str = "",
        source: str = "powerbi_refresh_failure",
        notified: bool = False,
    ) -> Incident:
        """Upsert an incident for this result. Increments on repeat."""
        now = _utcnow()

        # A suppressed duplicate must increment the incident it duplicated.
        # Writing a parallel row would defeat the entire point of dedup: you
        # would end up with one incident per alert, which is the state the
        # signature exists to prevent.
        if result.outcome == "duplicate_suppressed":
            with self._lock:
                parent = self._find_open_ref(result.signature)
                if parent is not None:
                    parent.occurrence_count += 1
                    parent.last_seen_at = now
                    if notified:
                        parent.notified_count += 1
                        parent.last_notified_at = now
                    self._persist(parent)
                    logger.info(
                        "Incident %s suppressed duplicate -> occurrence %d (notified %d)",
                        parent.id,
                        parent.occurrence_count,
                        parent.notified_count,
                    )
                    return parent.model_copy(deep=True)
            logger.warning(
                "Result reported duplicate_suppressed but no open incident matched "
                "signature %s — recording it as needs_human instead of creating an "
                "orphan open incident that would suppress future alerts",
                result.signature,
            )
            result = result.model_copy(update={"outcome": "needs_human"})

        resolved = result.outcome in ("resolved", "flagged_data_quality")
        iid = incident_id(result.signature, resolved=resolved)

        red_error, kinds_a = redact(original_error)
        red_cause, kinds_b = redact(result.root_cause)
        red_action, kinds_c = redact(result.action_taken)
        fired = sorted(set(kinds_a) | set(kinds_b) | set(kinds_c))

        with self._lock:
            existing = self._items.get(iid)
            if existing is not None:
                existing.occurrence_count += 1
                existing.last_seen_at = now
                existing.outcome = result.outcome
                if notified:
                    existing.notified_count += 1
                    existing.last_notified_at = now
                # Status must follow the outcome, or a since-resolved incident
                # keeps suppressing under a stale 'open'.
                existing.status = _status_for(result.outcome)  # type: ignore[assignment]
                if red_cause:
                    existing.diagnosed_root_cause = red_cause
                if _needs_investigation(result):
                    existing.requires_investigation = True
                self._persist(existing)
                logger.info(
                    "Incident %s occurrence -> %d", iid, existing.occurrence_count
                )
                return existing.model_copy(deep=True)

            incident = Incident(
                id=iid,
                signature=result.signature,
                signature_version=result.signature_version,
                outcome=result.outcome,
                status=_status_for(result.outcome),  # type: ignore[arg-type]
                request_id=result.request_id,
                report_name=report_name,
                source=source,
                occurrence_count=1,
                first_seen_at=now,
                last_seen_at=now,
                notified_count=1 if notified else 0,
                last_notified_at=now if notified else "",
                original_error=red_error,
                diagnosed_root_cause=red_cause,
                action_applied=red_action,
                action_type=_ACTION_TYPES.get(result.action_taken, "unknown"),  # type: ignore[arg-type]
                requires_investigation=_needs_investigation(result),
                agent_name=agent_name,
                prompt_version_hash=prompt_version_hash,
                model_provider=model_provider,
                model_name=model_name,
                app_version=app_version,
                redaction_applied=bool(fired),
                redaction_kinds=fired,
            )
            self._items[iid] = incident
            self._persist(incident)
            logger.info("Incident %s created (outcome=%s)", iid, result.outcome)
            return incident.model_copy(deep=True)

    def mark(self, incident_id_: str, status: str, notes: str = "") -> Incident | None:
        with self._lock:
            inc = self._items.get(incident_id_)
            if inc is None:
                return None
            inc.status = status  # type: ignore[assignment]
            if notes:
                inc.triage_notes = redact(notes)[0]
            self._persist(inc)
            return inc.model_copy(deep=True)

    def reset(self) -> None:
        """Clear the store — used between demo rehearsals and in tests.

        Defined on the base class rather than only on the durable subclass, so
        ``reset_incidents`` behaves identically whichever backend is injected.
        A reset that silently does nothing is worse than no reset at all.
        """
        with self._lock:
            self._items.clear()
            self._on_reset()

    def _on_reset(self) -> None:  # pragma: no cover - no-op base
        return None

    # --- hook for durable subclasses --------------------------------------

    def _persist(self, incident: Incident) -> None:  # pragma: no cover - no-op base
        return None


class JsonFileIncidentStore(InMemoryIncidentStore):
    """Durable across process restarts. One JSON document per store."""

    def __init__(self, path: str | Path):
        super().__init__()
        self.path = Path(path)
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not read incident store at %s: %s", self.path, exc)
            return
        for item in raw.get("incidents", []):
            try:
                inc = Incident.model_validate(item)
                self._items[inc.id] = inc
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Skipping malformed incident row: %s", exc)

    def _persist(self, incident: Incident) -> None:
        # Caller already holds the lock.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "incidents": [i.model_dump() for i in self._items.values()],
            "updated_at": _utcnow(),
        }
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def _on_reset(self) -> None:
        if self.path.exists():
            self.path.unlink()
