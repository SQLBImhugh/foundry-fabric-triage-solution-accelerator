"""Durable record of which alert messages have already been triaged.

``GraphInbox`` used to keep this in a set on the instance. That is the wrong
lifetime. A hosted agent is constructed fresh for every invocation -- see
``app.py::_drain_mailbox``, which calls ``build_inbox()`` each time -- so the
set was always empty and a scheduled sweep re-triaged the whole inbox on every
run. Observed in the demo tenant: a five-minute routine with two unread alerts
sitting in the mailbox produced two Teams cards every five minutes,
indefinitely, and drove one incident to 130 occurrences.

The agent holds ``Mail.Read`` and nothing more. It deliberately cannot mark a
message read or move it to a folder, because being unable to write to the
mailbox is one of the security properties this demo exists to show. So "have I
already handled this?" has to be the agent's own state, not the mailbox's.

Messages are marked **after** a terminal outcome is recorded, never at fetch
time. A crash mid-run then re-triages the alert on the next sweep, which the
signature dedup already handles, rather than dropping it silently -- losing an
alert is the worse of the two failures.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Any, Protocol

from triage.store.azure_sql import SqlUnavailable, quote_identifier

logger = logging.getLogger("triage.store.processed")


def _fingerprint(message_id: str) -> str:
    """Stable, key-safe identifier for a message.

    Graph message ids are long and may contain characters Table Storage
    rejects in a key. Hashing sidesteps both problems at once and, unlike
    truncation, cannot collide two different messages onto one row.
    """
    return hashlib.sha256((message_id or "").encode("utf-8")).hexdigest()


class ProcessedMessageLog(Protocol):
    def seen(self, message_id: str) -> bool: ...
    def mark(self, message_id: str, *, received_at: str = "") -> None: ...
    def reset(self) -> None: ...


class InMemoryProcessedLog:
    """Per-process log. Correct for one CLI run, useless across invocations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: dict[str, str] = {}

    def seen(self, message_id: str) -> bool:
        with self._lock:
            return _fingerprint(message_id) in self._items

    def mark(self, message_id: str, *, received_at: str = "") -> None:
        with self._lock:
            self._items[_fingerprint(message_id)] = received_at
            self._persist(message_id, received_at)

    def reset(self) -> None:
        with self._lock:
            self._items.clear()
            self._on_reset()

    @property
    def is_durable(self) -> bool:
        return False

    def count(self) -> int:
        with self._lock:
            return len(self._items)

    # --- durability hooks --------------------------------------------------

    def _persist(self, message_id: str, received_at: str) -> None:  # pragma: no cover
        """No-op. Caller holds the lock."""

    def _on_reset(self) -> None:  # pragma: no cover - no-op base
        """No-op."""


class JsonFileProcessedLog(InMemoryProcessedLog):
    """Survives a restart on the offline path, with no Azure dependency."""

    def __init__(self, path: str | Path):
        super().__init__()
        self._path = Path(path)
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Unreadable processed-message log (%s)", type(exc).__name__)
            return
        if isinstance(raw, dict):
            self._items.update({str(k): str(v) for k, v in raw.items()})

    def _persist(self, message_id: str, received_at: str) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._items, indent=2), encoding="utf-8")

    def _on_reset(self) -> None:
        if self._path.exists():
            self._path.unlink()


class AzureSqlProcessedLog(InMemoryProcessedLog):
    """Read-through processed state; an unconfirmed mark cannot finish work."""

    def __init__(self, *, db: Any, table: str = "triage_processed_messages") -> None:
        super().__init__()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._loaded = False
        self._ensure_loaded()

    @property
    def is_durable(self) -> bool:
        return self._loaded and self._db.is_available

    def _ensure_loaded(self, fingerprint: str | None = None) -> None:
        self._loaded = False
        self._items.clear()
        try:
            self._load(fingerprint)
        except Exception as exc:
            logger.error(
                "Cannot read deployed processed-message state in %s (%s); ingestion stopped",
                self._table_name, type(exc).__name__,
            )
            raise
        self._loaded = True

    def _load(self, fingerprint: str | None = None) -> None:
        where = " WHERE fingerprint = ?" if fingerprint is not None else ""
        params = (fingerprint,) if fingerprint is not None else ()
        rows = self._db.query(f"SELECT fingerprint, received_at FROM {self._table}{where}", *params)
        loaded: dict[str, str] = {}
        try:
            for key, received in rows:
                if (
                    not isinstance(key, str) or len(key) != 64 or key in loaded
                    or (received is not None and not isinstance(received, str))
                ):
                    raise ValueError("Invalid processed-message row")
                loaded[key] = received or ""
        except (TypeError, ValueError) as exc:
            raise SqlUnavailable(
                f"Unreadable processed-message state in {self._table_name}; repair deployed state."
            ) from exc
        self._items = loaded

    def seen(self, message_id: str) -> bool:
        with self._lock:
            fingerprint = _fingerprint(message_id)
            self._ensure_loaded(fingerprint)
            return fingerprint in self._items

    def count(self) -> int:
        with self._lock:
            self._ensure_loaded()
            return len(self._items)

    def mark(self, message_id: str, *, received_at: str = "") -> None:
        with self._lock:
            self._persist(message_id, received_at)
            self._items[_fingerprint(message_id)] = received_at
            self._loaded = True

    def _persist(self, message_id: str, received_at: str) -> None:
        fingerprint = _fingerprint(message_id)
        try:
            updated = self._db.execute(
                f"UPDATE {self._table} SET received_at = ?, message_id = ? "
                f"WHERE fingerprint = ?",
                received_at,
                (message_id or "")[:512],
                fingerprint,
            )
            if updated not in (0, 1):
                raise SqlUnavailable("Processed-message update returned no reliable row count.")
            if updated == 0:
                inserted = self._db.execute(
                    f"INSERT INTO {self._table} (fingerprint, message_id, received_at) "
                    f"SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM {self._table} "
                    "WITH (UPDLOCK, HOLDLOCK) WHERE fingerprint = ?)",
                    fingerprint, (message_id or "")[:512], received_at, fingerprint,
                )
                if inserted not in (0, 1):
                    raise SqlUnavailable("Processed-message insert returned no reliable row count.")
        except Exception as exc:
            self._loaded = False
            self._items.clear()
            logger.error(
                "Processed-message write unconfirmed (%s); work must remain unfinished",
                type(exc).__name__,
            )
            raise

    def _on_reset(self) -> None:
        self._loaded = False
        try:
            if self._db.execute(f"DELETE FROM {self._table}") < 0:
                raise SqlUnavailable("Processed-message reset returned no reliable row count.")
        except Exception as exc:
            logger.error("Could not clear %s (%s)", self._table_name, type(exc).__name__)
            raise
        self._loaded = True
