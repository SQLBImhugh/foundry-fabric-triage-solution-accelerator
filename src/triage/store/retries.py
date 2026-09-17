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

import hashlib
import json
import logging
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from triage.store.azure_sql import SqlUnavailable, quote_identifier

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
    def record_linked_retry(self, **fields: Any) -> dict[str, Any]: ...
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
        source_execution: dict[str, Any] | None = None,
        policy_revision: int | None = None,
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
            if source_execution is not None:
                fresh["source_execution"] = source_execution
                fresh["policy_revision"] = policy_revision
            elif row and "source_execution" in row:
                fresh["source_execution"] = row["source_execution"]
                fresh["policy_revision"] = row["policy_revision"]
            self._items[signature] = fresh
            self._persist(fresh)
            logger.info(
                "Deferred retry for %s: attempt %d, due in %ds", signature, attempts, wait
            )
            return dict(fresh)

    def record_linked_retry(
        self, *, signature: str, work_id: str, retry_of: str, attempt: int,
        due_at: datetime, created_at: datetime, source_execution: dict[str, Any],
        policy_revision: int, report_name: str = "",
    ) -> dict[str, Any]:
        """Project an already-durable successor; this row grants no retry authority."""
        if type(attempt) is not int or not 1 <= attempt <= self._max_attempts:
            raise ValueError("Linked retries must retain the existing bounded attempt count.")
        if due_at.tzinfo is None or created_at.tzinfo is None:
            raise ValueError("Linked retry timestamps require a timezone.")
        with self._lock:
            prior = self._items.get(signature)
            if prior and (
                prior.get("monitoring_work_id") == work_id or int(prior["attempts"]) > attempt
            ):
                logger.info("Retaining the existing or newer linked retry projection for %s", signature)
                return dict(prior)
            target = source_execution["target"]
            row = {
                "signature": signature, "request_id": source_execution["run_id"],
                "workspace_id": target["workspace_id"], "dataset_id": target["item_id"],
                "report_name": report_name or (prior or {}).get("report_name", ""),
                "reason": "The service confirmed no effect; a bounded linked successor is scheduled.",
                "attempts": attempt, "wait_seconds": max(0, int((due_at - created_at).total_seconds())),
                "due_at": due_at.isoformat(), "created_at": created_at.isoformat(),
                "updated_at": _utcnow().isoformat(), "status": "pending",
                "source_execution": source_execution, "policy_revision": policy_revision,
                "monitoring_work_id": work_id, "retry_of": retry_of,
            }
            self._items[signature] = row
            self._persist(row)
            return dict(row)

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


class AzureSqlRetryStore(InMemoryRetryStore):
    """Shared retry state. Missing or uncertain durable work stops the caller."""

    def __init__(
        self,
        *,
        db: Any,
        table: str = "triage_deferred_retries",
        max_attempts: int = MAX_ATTEMPTS,
    ) -> None:
        super().__init__(max_attempts=max_attempts)
        self._lock = threading.RLock()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._loaded = False
        self._revisions: dict[str, bytes] = {}
        self._ensure_loaded()

    @property
    def is_durable(self) -> bool:
        return self._loaded and self._db.is_available

    def _ensure_loaded(self, signature: str | None = None) -> None:
        self._loaded = False
        self._items.clear()
        self._revisions.clear()
        try:
            self._load(signature)
        except Exception as exc:
            logger.error(
                "Cannot read deployed retry state in %s (%s); deferred work stopped",
                self._table_name, type(exc).__name__,
            )
            raise
        self._loaded = True

    def _load(self, signature: str | None = None) -> None:
        where = " WHERE signature = ?" if signature is not None else ""
        params = (signature,) if signature is not None else ()
        rows = self._db.query(f"SELECT signature, payload FROM {self._table}{where}", *params)
        loaded: dict[str, dict[str, Any]] = {}
        revisions: dict[str, bytes] = {}
        try:
            for key, raw in rows:
                row = json.loads(raw)
                if not isinstance(row, dict) or row["signature"] != key or not key or key in loaded:
                    raise ValueError("Invalid retry identity")
                for name in (
                    "signature", "request_id", "workspace_id", "dataset_id",
                    "report_name", "reason", "status", "created_at", "updated_at", "due_at",
                ):
                    if not isinstance(row[name], str):
                        raise ValueError("Invalid retry field")
                if row["status"] not in {"pending", "done", "exhausted"}:
                    raise ValueError("Invalid retry status")
                if type(row["attempts"]) is not int or row["attempts"] < 1:
                    raise ValueError("Invalid retry attempt count")
                if type(row["wait_seconds"]) is not int or row["wait_seconds"] < 0:
                    raise ValueError("Invalid retry wait")
                for name in ("due_at", "created_at", "updated_at"):
                    if datetime.fromisoformat(row[name]).tzinfo is None:
                        raise ValueError("Retry timestamp has no timezone")
                loaded[key] = row
                revisions[key] = hashlib.sha256(raw.encode("utf-16-le")).digest()
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise SqlUnavailable(
                f"Unreadable retry state in {self._table_name}; repair deployed state."
            ) from exc
        self._items = loaded
        self._revisions = revisions

    def get(self, signature: str) -> dict[str, Any] | None:
        with self._lock:
            self._ensure_loaded(signature)
            return super().get(signature)

    def due(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        with self._lock:
            self._ensure_loaded()
            return super().due(now=now)

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            self._ensure_loaded()
            return super().pending()

    def defer(
        self, *, signature: str, request_id: str = "", workspace_id: str = "",
        dataset_id: str = "", report_name: str = "", reason: str = "",
        retry_after_seconds: int = 0,
        source_execution: dict[str, Any] | None = None,
        policy_revision: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._ensure_loaded(signature)
            return super().defer(
                signature=signature, request_id=request_id, workspace_id=workspace_id,
                dataset_id=dataset_id, report_name=report_name, reason=reason,
                retry_after_seconds=retry_after_seconds,
                source_execution=source_execution, policy_revision=policy_revision,
            )

    def complete(self, signature: str, *, outcome: str) -> None:
        with self._lock:
            self._ensure_loaded(signature)
            super().complete(signature, outcome=outcome)

    def record_linked_retry(self, *, signature: str, **fields: Any) -> dict[str, Any]:
        with self._lock:
            self._ensure_loaded(signature)
            return super().record_linked_retry(signature=signature, **fields)

    def _persist(self, row: dict[str, Any]) -> None:
        signature = row["signature"]
        payload = json.dumps(row)
        args = (
            row["status"],
            row["due_at"],
            row["attempts"],
            payload,
            signature,
        )
        try:
            revision = self._revisions.get(signature)
            if revision is None:
                changed = self._db.execute(
                    f"INSERT INTO {self._table} "
                    f"(signature, status, due_at, attempts, payload) VALUES (?, ?, ?, ?, ?)",
                    signature, *args[:4],
                )
            else:
                changed = self._db.execute(
                    f"UPDATE {self._table} SET status = ?, due_at = ?, attempts = ?, "
                    "payload = ? WHERE signature = ? AND HASHBYTES('SHA2_256', payload) = ?",
                    *args, revision,
                )
            if changed != 1:
                raise SqlUnavailable(
                    "Retry write was not confirmed or its shared revision changed; reload before continuing."
                )
        except Exception as exc:
            self._loaded = False
            self._items.clear()
            self._revisions.clear()
            logger.error(
                "Deferred retry %s write unconfirmed (%s); not retried",
                signature, type(exc).__name__,
            )
            raise
        self._revisions[signature] = hashlib.sha256(payload.encode("utf-16-le")).digest()

    def _on_reset(self) -> None:
        self._loaded = False
        self._revisions.clear()
        try:
            if self._db.execute(f"DELETE FROM {self._table}") < 0:
                raise SqlUnavailable("Retry reset returned no reliable affected-row count.")
        except Exception as exc:
            logger.error("Could not clear %s (%s)", self._table_name, type(exc).__name__)
            raise
        self._loaded = True
