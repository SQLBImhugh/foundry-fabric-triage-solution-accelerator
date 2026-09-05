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


class FabricSqlProcessedLog(InMemoryProcessedLog):
    """Survives a container restart, which is the case that actually matters.

    Degrades to in-memory rather than failing to start, matching the other
    stores -- but keeps retrying, because a permanent degradation here means
    every sweep re-triages every message it has already handled and notifies
    about all of them again.
    """

    def __init__(self, *, db: Any, table: str = "triage_processed_messages") -> None:
        from triage.store.fabric_sql import quote_identifier

        super().__init__()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._loaded = False
        self._ensure_loaded()

    @property
    def is_durable(self) -> bool:
        return self._loaded and self._db.is_available

    def _ensure_loaded(self) -> bool:
        if self._loaded:
            return True
        try:
            self._load()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Processed-message log degraded to in-memory: cannot read %s (%s). "
                "Repeat sweeps will re-triage the same mail.",
                self._table_name,
                type(exc).__name__,
            )
            return False
        self._loaded = True
        logger.info(
            "Loaded %d processed message(s) from %s", len(self._items), self._table_name
        )
        return True

    def _load(self) -> None:
        rows = self._db.query(f"SELECT fingerprint, received_at FROM {self._table}")
        self._items = {str(k): str(v or "") for k, v in rows}

    def seen(self, message_id: str) -> bool:
        # Guarded: answering "not seen" from an empty cache is what causes the
        # duplicate notification this log exists to prevent.
        self._ensure_loaded()
        return super().seen(message_id)

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
            if not updated:
                try:
                    self._db.execute(
                        f"INSERT INTO {self._table} "
                        f"(fingerprint, message_id, received_at) VALUES (?, ?, ?)",
                        fingerprint,
                        (message_id or "")[:512],
                        received_at,
                    )
                except self._db.integrity_error():
                    # Another invocation recorded it first. Nothing to do: the
                    # message is marked processed either way.
                    pass
        except Exception as exc:  # noqa: BLE001
            # Failing to write means this message gets triaged again on the
            # next sweep. That is noisy but safe, so the run continues.
            self._loaded = False
            logger.error(
                "Could not record processed message (%s); it will be re-triaged",
                type(exc).__name__,
            )

    def _on_reset(self) -> None:
        try:
            self._db.execute(f"DELETE FROM {self._table}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not clear %s (%s)", self._table_name, type(exc).__name__)
