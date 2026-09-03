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

from triage.store.table_helpers import build_table_client

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


class AzureTableProcessedLog(InMemoryProcessedLog):
    """Survives a container restart, which is the case that actually matters.

    Degrades to in-memory rather than failing to start, matching
    ``AzureTableIncidentStore``. A demo that cannot reach storage should run
    loudly degraded, not refuse to run in front of an audience -- though in
    this degraded state repeat sweeps will re-triage, so it is logged as an
    error rather than a warning.
    """

    _PARTITION = "message"

    def __init__(
        self,
        *,
        endpoint: str,
        table_name: str = "processedmessages",
        credential: Any = None,
    ) -> None:
        super().__init__()
        self._endpoint = endpoint
        self._table_name = table_name
        self._client = None
        self._degraded = False

        try:
            self._client = self._build_client(credential)
            self._load()
        except Exception as exc:
            self._degraded = True
            logger.error(
                "Processed-message log degraded to in-memory: could not open %s at %s "
                "(%s). Repeat sweeps will re-triage the same mail.",
                table_name,
                endpoint,
                type(exc).__name__,
            )

    @property
    def is_durable(self) -> bool:
        return self._client is not None and not self._degraded

    def _build_client(self, credential: Any):
        return build_table_client(self._endpoint, self._table_name, credential)

    def _load(self) -> None:
        if self._client is None:
            return
        loaded = 0
        for entity in self._client.list_entities():
            key = str(entity.get("RowKey", ""))
            if key:
                self._items[key] = str(entity.get("received_at", ""))
                loaded += 1
        logger.info("Loaded %d processed message(s) from table %s", loaded, self._table_name)

    def _persist(self, message_id: str, received_at: str) -> None:
        if self._client is None:
            return
        try:
            self._client.upsert_entity(
                {
                    "PartitionKey": self._PARTITION,
                    "RowKey": _fingerprint(message_id),
                    "received_at": received_at,
                    # Kept for debugging: without it a row is an opaque hash and
                    # nobody can tell which mail it corresponds to.
                    "message_id": (message_id or "")[:512],
                }
            )
        except Exception as exc:
            # Failing to write means this message gets triaged again on the
            # next sweep. That is noisy but safe, so the run continues.
            self._degraded = True
            logger.error(
                "Could not record processed message (%s); it will be re-triaged",
                type(exc).__name__,
            )

    def _on_reset(self) -> None:
        if self._client is None:
            return
        for entity in self._client.list_entities():
            try:
                self._client.delete_entity(
                    partition_key=entity["PartitionKey"], row_key=entity["RowKey"]
                )
            except Exception:  # pragma: no cover - best effort
                logger.warning("Could not delete processed-message row %s", entity.get("RowKey"))
