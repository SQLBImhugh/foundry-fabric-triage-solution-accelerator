"""What the inbox filter refused, and why.

The filter is a security control: an agent that acts on every message it
receives is steerable by anyone who can email it. Until now a refusal produced
one log line and a count, which is enough to notice the filter is working and
not enough to answer the two questions an operator actually has.

    "Did it ignore the alert I sent?"
    "What has it been ignoring?"

A count cannot answer either. Worse, the two failure modes look identical from
outside: a filter that is correctly rejecting noise and a filter that is
silently rejecting the real alerts both report "ignored N messages". One is the
control working; the other is an outage in which the agent is quietly deaf, and
that is exactly the class of failure this accelerator exists to surface.

So refusals are recorded as rows. Every row is evidence that a decision was
made, and the reason is stored alongside it because "not on the allowlist" and
"subject does not look like an alert" send an operator to different places.

Deliberately *not* a queue. Nothing reads these rows back to reconsider a
message: a refusal is final, and a store that could resurrect one would be a way
around the control rather than a record of it. It is append-only evidence and
its own retention problem -- see `prune`.

Redaction stays inside `record`, matching every other store here. A rejected
message is the one most likely to contain something that has no business being
persisted, precisely because nothing vouched for it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from triage.redaction import redact
from triage.store.azure_sql import SqlUnavailable, quote_identifier

logger = logging.getLogger("triage.store.inbox_audit")

#: Keep the audit bounded. These rows are evidence, not history: an operator
#: looks at the last hour when an alert did not land, and nobody mines them a
#: month later. Unbounded growth in a table nothing reads back is a slow leak.
DEFAULT_MAX_ROWS = 500


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _fingerprint(message_id: str) -> str:
    """Stable id for a message, so the same one is never audited twice."""
    return hashlib.sha256((message_id or "").encode("utf-8")).hexdigest()[:48]


class InboxAuditLog(Protocol):
    def record(self, *, message_id: str, sender: str, subject: str, reason: str) -> None: ...
    def recent(self, limit: int = 50) -> list[dict[str, Any]]: ...
    def reset(self) -> None: ...


class InMemoryInboxAudit:
    """Reference implementation. Thread-safe; not durable."""

    def __init__(self, *, max_rows: int = DEFAULT_MAX_ROWS) -> None:
        self._lock = threading.RLock()
        self._items: dict[str, dict[str, Any]] = {}
        self._max_rows = max_rows

    @property
    def is_durable(self) -> bool:
        return False

    def record(self, *, message_id: str, sender: str, subject: str, reason: str) -> None:
        # Redact before anything is stored, not at the call site. A rejected
        # message is unvetted input by definition.
        #
        # `reason` is redacted too, and that is not belt-and-braces: the
        # allowlist rejection interpolates the sender into its text
        # ("sender x@y not on the allowlist", mail_filter.py), so leaving it raw
        # would reinstate in one column exactly what the next line strips out of
        # another.
        clean_subject, subject_hits = redact(subject or "")
        clean_sender, sender_hits = redact(sender or "")
        clean_reason, reason_hits = redact(reason or "")

        row = {
            "fingerprint": _fingerprint(message_id),
            "sender": clean_sender[:200],
            "subject": clean_subject[:400],
            "reason": clean_reason[:200],
            "ignored_at": _utcnow(),
            "redaction_applied": bool(subject_hits or sender_hits or reason_hits),
        }
        with self._lock:
            # Keyed on the message, so a sweep that re-reads the same mail does
            # not audit it again -- the filter is deterministic, and a hundred
            # identical rows would bury the one an operator is looking for.
            if row["fingerprint"] in self._items:
                return
            self._items[row["fingerprint"]] = row
            self._persist(row)
            self._prune_unlocked()

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = sorted(
                self._items.values(), key=lambda r: r["ignored_at"], reverse=True
            )
            return [dict(r) for r in rows[:limit]]

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._items)

    def reset(self) -> None:
        with self._lock:
            self._items.clear()
            self._on_reset()

    def _prune_unlocked(self) -> None:
        """Drop the oldest rows past the cap. Caller holds the lock."""
        if len(self._items) <= self._max_rows:
            return
        ordered = sorted(self._items.items(), key=lambda kv: kv[1]["ignored_at"])
        for key, _row in ordered[: len(self._items) - self._max_rows]:
            self._items.pop(key, None)
            self._on_prune(key)

    # --- hooks for durable subclasses -------------------------------------

    def _persist(self, row: dict[str, Any]) -> None:  # pragma: no cover - no-op base
        return None

    def _on_prune(self, fingerprint: str) -> None:  # pragma: no cover - no-op base
        return None

    def _on_reset(self) -> None:  # pragma: no cover - no-op base
        return None


class JsonFileInboxAudit(InMemoryInboxAudit):
    """Durable across process restarts. One JSON document per store."""

    def __init__(self, path: str | Path, *, max_rows: int = DEFAULT_MAX_ROWS) -> None:
        super().__init__(max_rows=max_rows)
        self.path = Path(path)
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not read inbox audit at %s: %s", self.path, exc)
            return
        for row in raw.get("ignored", []):
            fingerprint = str(row.get("fingerprint", ""))
            if fingerprint:
                self._items[fingerprint] = row

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"ignored": list(self._items.values()), "updated_at": _utcnow()}
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def _persist(self, row: dict[str, Any]) -> None:
        self._write()

    def _on_prune(self, fingerprint: str) -> None:
        self._write()

    def _on_reset(self) -> None:
        if self.path.exists():
            self.path.unlink()


class AzureSqlInboxAudit(InMemoryInboxAudit):
    """Durable filter evidence. An unrecorded refusal cannot complete ingestion."""

    def __init__(
        self,
        *,
        db: Any,
        table: str = "triage_inbox_audit",
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> None:
        super().__init__(max_rows=max_rows)
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._loaded = False
        self._ensure_loaded()

    @property
    def is_durable(self) -> bool:
        return self._loaded and self._db.is_available

    def _ensure_loaded(self) -> None:
        with self._lock:
            self._loaded = False
            self._items.clear()
            try:
                rows = self._db.query(
                    f"SELECT fingerprint, sender, subject, reason, ignored_at "
                    f"FROM {self._table}"
                )
            except Exception as exc:
                logger.error(
                    "Cannot read deployed inbox audit in %s (%s); ingestion stopped",
                    self._table_name, type(exc).__name__,
                )
                raise
            loaded: dict[str, dict[str, Any]] = {}
            try:
                for key, sender, subject, reason, ignored_at in rows:
                    if (
                        not isinstance(key, str) or len(key) != 48 or key in loaded
                        or any(value is not None and not isinstance(value, str) for value in (sender, subject, reason))
                        or not isinstance(ignored_at, str)
                        or datetime.fromisoformat(ignored_at).tzinfo is None
                    ):
                        raise ValueError("Invalid inbox-audit row")
                    loaded[key] = {
                        "fingerprint": key, "sender": sender, "subject": subject,
                        "reason": reason, "ignored_at": ignored_at,
                    }
            except (TypeError, ValueError) as exc:
                logger.error("Unreadable inbox audit in %s; ingestion stopped", self._table_name)
                raise SqlUnavailable(
                    f"Unreadable inbox audit in {self._table_name}; repair deployed state."
                ) from exc
            self._items = loaded
            self._loaded = True

    def record(self, *, message_id: str, sender: str, subject: str, reason: str) -> None:
        with self._lock:
            self._ensure_loaded()
            super().record(message_id=message_id, sender=sender, subject=subject, reason=reason)

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            self._ensure_loaded()
            return super().recent(limit)

    @property
    def count(self) -> int:
        with self._lock:
            self._ensure_loaded()
            return super().count

    def _persist(self, row: dict[str, Any]) -> None:
        try:
            inserted = self._db.execute(
                f"INSERT INTO {self._table} "
                f"(fingerprint, sender, subject, reason, ignored_at) "
                f"SELECT ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM {self._table} "
                "WITH (UPDLOCK, HOLDLOCK) WHERE fingerprint = ?)",
                row["fingerprint"], row["sender"], row["subject"],
                row["reason"], row["ignored_at"], row["fingerprint"],
            )
            if inserted not in (0, 1):
                raise SqlUnavailable("Inbox-audit insert returned no reliable row count.")
            if inserted == 0:
                self._ensure_loaded()
        except Exception as exc:
            self._loaded = False
            self._items.clear()
            logger.error(
                "Inbox-audit write unconfirmed (%s); ingestion stopped", type(exc).__name__,
            )
            raise

    def _on_prune(self, fingerprint: str) -> None:
        try:
            removed = self._db.execute(
                f"DELETE FROM {self._table} WHERE fingerprint = ?", fingerprint
            )
            if removed not in (0, 1):
                raise SqlUnavailable("Inbox-audit pruning returned no reliable row count.")
        except Exception as exc:
            self._loaded = False
            self._items.clear()
            logger.error("Inbox-audit pruning unconfirmed (%s)", type(exc).__name__)
            raise

    def _on_reset(self) -> None:
        self._loaded = False
        try:
            if self._db.execute(f"DELETE FROM {self._table}") < 0:
                raise SqlUnavailable("Inbox-audit reset returned no reliable row count.")
        except Exception as exc:
            logger.error("Could not clear %s (%s)", self._table_name, type(exc).__name__)
            raise
        self._loaded = True
