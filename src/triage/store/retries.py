"""Retries the agent has deliberately postponed.

Capacity throttling is the one failure where the obvious fix makes the problem
worse. A refresh is rejected because the capacity is already saturated;
retrying immediately adds load to the thing that is overloaded. Do that across
several datasets at once and contention becomes an outage -- caused by the
system that was supposed to be helping.

So the agent does not retry a throttled refresh. It records the work here with
a due time and stops. A later sweep picks it up when the window has passed.

That makes this store load-bearing rather than a note: if nothing drains it,
the retry never happens and the agent has quietly dropped the job it said it
would do. ``due()`` is what the sweep reads, and ``bi-triage retries`` is what
a human reads.

Bounded on purpose. Each deferral doubles the wait and increments an attempt
count, and after ``max_attempts`` the row is marked ``exhausted`` instead of
being postponed again. An agent that defers forever has invented a very
patient way of doing nothing.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger("triage.store.retries")

#: First backoff. Doubles per attempt: 15, 30, 60 minutes.
DEFAULT_BACKOFF_SECONDS = 900
MAX_ATTEMPTS = 3


def _utcnow() -> datetime:
    return datetime.now(UTC)


def backoff_seconds(attempt: int, *, base: int = DEFAULT_BACKOFF_SECONDS) -> int:
    """Exponential, so a persistent contention window is not hammered.

    ``attempt`` is 1-based: the first deferral waits ``base``.
    """
    return int(base * (2 ** max(0, attempt - 1)))


class RetryStore(Protocol):
    def defer(self, **fields: Any) -> dict[str, Any]: ...
    def due(self, *, now: datetime | None = None) -> list[dict[str, Any]]: ...
    def pending(self) -> list[dict[str, Any]]: ...
    def complete(self, signature: str, *, outcome: str) -> None: ...
    def reset(self) -> None: ...


class InMemoryRetryStore:
    """Correct for one process, useless across hosted-agent invocations."""

    def __init__(self, *, max_attempts: int = MAX_ATTEMPTS) -> None:
        self._lock = threading.Lock()
        self._items: dict[str, dict[str, Any]] = {}
        self._max_attempts = max_attempts

    def defer(
        self,
        *,
        signature: str,
        request_id: str = "",
        workspace_id: str = "",
        dataset_id: str = "",
        report_name: str = "",
        reason: str = "",
        retry_after_seconds: int = 0,
    ) -> dict[str, Any]:
        """Postpone this dataset's retry. One row per signature, not per alert.

        Keyed by signature so a throttle storm producing twenty alerts for the
        same model schedules one retry, not twenty -- which would recreate the
        stampede this exists to prevent.
        """
        with self._lock:
            row = self._items.get(signature)
            attempts = int(row["attempts"]) + 1 if row else 1

            if attempts > self._max_attempts:
                exhausted = dict(row or {})
                exhausted.update(
                    {
                        "status": "exhausted",
                        "reason": (
                            f"Still throttled after {self._max_attempts} deferred "
                            "retries. This is a capacity scheduling problem, not a "
                            "retry problem."
                        ),
                        "updated_at": _utcnow().isoformat(timespec="seconds"),
                    }
                )
                self._items[signature] = exhausted
                self._persist(exhausted)
                logger.warning(
                    "Retry for %s exhausted after %d attempts", signature, self._max_attempts
                )
                return dict(exhausted)

            wait = retry_after_seconds or backoff_seconds(attempts)
            now = _utcnow()
            fresh = {
                "signature": signature,
                "request_id": request_id or (row or {}).get("request_id", ""),
                "workspace_id": workspace_id or (row or {}).get("workspace_id", ""),
                "dataset_id": dataset_id or (row or {}).get("dataset_id", ""),
                "report_name": report_name or (row or {}).get("report_name", ""),
                "reason": reason,
                "attempts": attempts,
                "wait_seconds": wait,
                "due_at": (now + timedelta(seconds=wait)).isoformat(timespec="seconds"),
                "created_at": (row or {}).get(
                    "created_at", now.isoformat(timespec="seconds")
                ),
                "updated_at": now.isoformat(timespec="seconds"),
                "status": "pending",
            }
            self._items[signature] = fresh
            self._persist(fresh)
            logger.info(
                "Deferred retry for %s: attempt %d, due in %ds", signature, attempts, wait
            )
            return dict(fresh)

    def get(self, signature: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._items.get(signature)
            return dict(row) if row else None

    def is_deferred(self, signature: str, *, now: datetime | None = None) -> bool:
        """True while a retry for this signature is scheduled but not yet due.

        This is what stops the controller dispatching an immediate refresh into
        a contention window it has already agreed to wait out.
        """
        row = self.get(signature)
        if row is None or row.get("status") != "pending":
            return False
        return _parse(row["due_at"]) > (now or _utcnow())

    def due(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        moment = now or _utcnow()
        with self._lock:
            return [
                dict(r)
                for r in self._items.values()
                if r.get("status") == "pending" and _parse(r["due_at"]) <= moment
            ]

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._items.values() if r.get("status") == "pending"]

    def complete(self, signature: str, *, outcome: str) -> None:
        with self._lock:
            row = self._items.get(signature)
            if row is None:
                return
            row["status"] = "done"
            row["outcome"] = outcome
            row["updated_at"] = _utcnow().isoformat(timespec="seconds")
            self._persist(row)
            logger.info("Deferred retry for %s closed: %s", signature, outcome)

    def reset(self) -> None:
        with self._lock:
            self._items.clear()
            self._on_reset()

    @property
    def is_durable(self) -> bool:
        return False

    # --- durability hooks --------------------------------------------------

    def _persist(self, row: dict[str, Any]) -> None:  # pragma: no cover
        """No-op. Caller holds the lock."""

    def _on_reset(self) -> None:  # pragma: no cover - no-op base
        """No-op."""


def _parse(value: str) -> datetime:
    """Read a stored timestamp, treating anything unreadable as due now.

    A row whose due time cannot be parsed must not become permanently
    undrainable -- that would silently strand the retry it represents.
    """
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        logger.warning("Unreadable due_at %r; treating the retry as due", value)
        return datetime.min.replace(tzinfo=UTC)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


class JsonFileRetryStore(InMemoryRetryStore):
    """Survives a restart offline, and lets the CLI show what is waiting."""

    def __init__(self, path: str | Path, *, max_attempts: int = MAX_ATTEMPTS):
        super().__init__(max_attempts=max_attempts)
        self._path = Path(path)
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Unreadable retry store (%s)", type(exc).__name__)
            return
        if isinstance(raw, dict):
            self._items.update(raw)

    def _reload(self) -> None:
        self._items.clear()
        self._load()

    def get(self, signature: str) -> dict[str, Any] | None:
        with self._lock:
            self._reload()
        return super().get(signature)

    def due(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        with self._lock:
            self._reload()
        return super().due(now=now)

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            self._reload()
        return super().pending()

    def _persist(self, row: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._items, indent=2), encoding="utf-8")

    def _on_reset(self) -> None:
        if self._path.exists():
            self._path.unlink()


class FabricSqlRetryStore(InMemoryRetryStore):
    """The deployed path. Degrades to in-memory, loudly, and keeps retrying.

    In the degraded state a deferred retry is scheduled into a store that will
    not exist on the next invocation, so the work is silently dropped. That is
    worse than not deferring at all, hence the error rather than a warning --
    and it is why this reconnects instead of giving up for the life of the
    process.
    """

    def __init__(
        self,
        *,
        db: Any,
        table: str = "triage_deferred_retries",
        max_attempts: int = MAX_ATTEMPTS,
    ) -> None:
        from triage.store.fabric_sql import quote_identifier

        super().__init__(max_attempts=max_attempts)
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
                "Retry store degraded to in-memory: cannot read %s (%s). "
                "Deferred retries will be dropped rather than performed.",
                self._table_name,
                type(exc).__name__,
            )
            return False
        self._loaded = True
        logger.info(
            "Loaded %d deferred retry row(s) from %s", len(self._items), self._table_name
        )
        return True

    def _load(self) -> None:
        rows = self._db.query(f"SELECT signature, payload FROM {self._table}")
        loaded: dict[str, dict[str, Any]] = {}
        for signature, raw in rows:
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except Exception:  # noqa: BLE001
                logger.warning("Skipping unreadable retry row %s", signature)
                continue
            loaded[str(row.get("signature", signature))] = row
        self._items = loaded

    def due(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        # Guarded: the sweep that performs deferred work reads this, and an
        # empty cache silently drops every retry that was scheduled.
        self._ensure_loaded()
        return super().due(now=now)

    def pending(self) -> list[dict[str, Any]]:
        self._ensure_loaded()
        return super().pending()

    def _persist(self, row: dict[str, Any]) -> None:
        signature = str(row["signature"])
        payload = json.dumps(row)
        args = (
            row.get("status", ""),
            row.get("due_at", ""),
            int(row.get("attempts", 0)),
            payload,
            signature,
        )
        try:
            updated = self._db.execute(
                f"UPDATE {self._table} SET status = ?, due_at = ?, attempts = ?, "
                f"payload = ? WHERE signature = ?",
                *args,
            )
            if not updated:
                try:
                    self._db.execute(
                        f"INSERT INTO {self._table} "
                        f"(signature, status, due_at, attempts, payload) "
                        f"VALUES (?, ?, ?, ?, ?)",
                        signature,
                        row.get("status", ""),
                        row.get("due_at", ""),
                        int(row.get("attempts", 0)),
                        payload,
                    )
                except self._db.integrity_error():
                    self._db.execute(
                        f"UPDATE {self._table} SET status = ?, due_at = ?, "
                        f"attempts = ?, payload = ? WHERE signature = ?",
                        *args,
                    )
        except Exception as exc:  # noqa: BLE001
            self._loaded = False
            logger.error(
                "Could not persist deferred retry %s (%s); it will not run",
                signature,
                type(exc).__name__,
            )

    def _on_reset(self) -> None:
        try:
            self._db.execute(f"DELETE FROM {self._table}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not clear %s (%s)", self._table_name, type(exc).__name__)
