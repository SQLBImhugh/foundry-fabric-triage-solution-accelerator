"""Where a human's approval decision actually lives.

``TeamsCardApprovalGate`` posts a card and then polls a ``decision_source`` for
an answer. Until now no source existed outside the tests, so the production
gate could never be wired up: with nothing to poll it fails closed, which is
safe but means the approval branch was demonstrated by scripted gates rather
than by a person deciding anything.

This is that source. The shape is deliberately boring -- one row per request,
updated in place when a decision arrives:

    open()    the agent records what it is asking for, before posting the card
    decide()  a human writes an answer against that request id
    poll()    the agent reads it back

Decisions are written by two things, and the store cannot tell them apart:

* ``bi-triage approve|deny`` -- needs no infrastructure at all, and is how an
  on-call engineer holding the repo answers.
* the card's Approve/Decline buttons, which lead to a pair of Logic Apps in
  ``infra/approval-callback.json``. The GET side renders a confirmation page and
  can change nothing; the POST side calls
  ``dbo.triage_record_approval_decision`` with its own managed identity. Two
  workflows because a Request trigger accepts exactly one HTTP method.

``decide()`` is a single conditional UPDATE rather than a read followed by a
write, and the stored procedure the callback uses has the same ``WHERE`` clause
for the same reason. Two responders can both read an unanswered request, and a
check-then-write would let the later one silently overwrite the earlier.
``rowcount`` reports who won.

The store never decides anything itself. Validation stays in
``approvals.py``: the fingerprint check, the expiry check and the single-use
check all run against what comes back out of here, so a forged or stale row
cannot authorise anything.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger("triage.store.approvals")


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class ApprovalChannel(Protocol):
    def open(self, request: Any) -> None: ...
    async def poll(self, request_id: str) -> dict[str, Any] | None: ...
    def decide(self, request_id: str, *, decision: str, responder: str, reason: str = "") -> dict[str, Any]: ...
    def pending(self) -> list[dict[str, Any]]: ...
    def get(self, request_id: str) -> dict[str, Any] | None: ...
    def reset(self) -> None: ...


class InMemoryApprovalChannel:
    """Correct for one process. Useless across a hosted agent's invocations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: dict[str, dict[str, Any]] = {}

    def open(self, request: Any) -> None:
        row = {
            "request_id": request.request_id,
            "action": request.action,
            "fingerprint": request.fingerprint,
            "report_name": request.report_name,
            "justification": request.justification,
            "impact": request.impact,
            "requested_at": request.requested_at.isoformat(timespec="seconds"),
            "expires_at": request.expires_at.isoformat(timespec="seconds"),
            "decision": "",
            "responder": "",
            "reason": "",
            "decided_at": "",
        }
        with self._lock:
            self._items[request.request_id] = row
            self._persist(row)
        logger.info("Approval %s opened for %s", request.request_id, request.action)

    async def poll(self, request_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._items.get(request_id)
            if row is None or not row.get("decision"):
                return None
            return dict(row)

    def decide(
        self, request_id: str, *, decision: str, responder: str, reason: str = ""
    ) -> dict[str, Any]:
        """Record an answer. Refuses to invent a request that was never asked.

        A decision against an unknown id would otherwise sit in the table
        looking authoritative, and the first request that happened to reuse the
        id would consume it.
        """
        with self._lock:
            row = self._items.get(request_id)
            if row is None:
                raise KeyError(f"No approval request with id {request_id!r}")
            if row.get("decision"):
                raise ValueError(
                    f"Approval {request_id} was already answered "
                    f"({row['decision']} by {row.get('responder') or 'unknown'})"
                )
            row["decision"] = decision
            row["responder"] = responder
            row["reason"] = reason
            row["decided_at"] = _utcnow()
            self._persist(row)
            logger.info("Approval %s -> %s by %s", request_id, decision, responder)
            return dict(row)

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._items.values() if not r.get("decision")]

    def get(self, request_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._items.get(request_id)
            return dict(row) if row else None

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


class JsonFileApprovalChannel(InMemoryApprovalChannel):
    """Survives a restart offline, and lets the CLI answer a local run."""

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
            logger.warning("Unreadable approval channel (%s)", type(exc).__name__)
            return
        if isinstance(raw, dict):
            self._items.update(raw)

    def _reload(self) -> None:
        """Pick up a decision written by another process, e.g. the CLI."""
        self._items.clear()
        self._load()

    async def poll(self, request_id: str) -> dict[str, Any] | None:
        # The whole point is that somebody *else* answers, so the in-memory
        # copy is stale by definition. Re-read before every look.
        with self._lock:
            self._reload()
        return await super().poll(request_id)

    def decide(
        self, request_id: str, *, decision: str, responder: str, reason: str = ""
    ) -> dict[str, Any]:
        with self._lock:
            self._reload()
        return super().decide(
            request_id, decision=decision, responder=responder, reason=reason
        )

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            self._reload()
        return super().pending()

    def get(self, request_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._reload()
        return super().get(request_id)

    def _persist(self, row: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._items, indent=2), encoding="utf-8")

    def _on_reset(self) -> None:
        if self._path.exists():
            self._path.unlink()


class FabricSqlApprovalChannel(InMemoryApprovalChannel):
    """The deployed path: the agent polls here, a human writes here.

    The row is the meeting point between two processes that never share
    memory. The agent writes the request in one invocation; a person answers
    from a Teams card or the CLI, in a different process and often a different
    container; the agent reads the answer back on a later poll.

    Every read goes to the database rather than to the in-memory copy, because
    the in-memory copy cannot contain a decision made somewhere else. Degrades
    loudly: in that state no human can answer and every gated action fails
    closed, which is safe but useless, so it keeps retrying.
    """

    def __init__(self, *, db: Any, table: str = "triage_approvals") -> None:
        from triage.store.fabric_sql import quote_identifier

        super().__init__()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table

    @property
    def is_durable(self) -> bool:
        return self._db.is_available

    def _fetch(self, request_id: str) -> dict[str, Any] | None:
        try:
            rows = self._db.query(
                f"SELECT payload FROM {self._table} WHERE request_id = ?", request_id
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Could not read approval %s (%s); the gate will fail closed",
                request_id,
                type(exc).__name__,
            )
            return None
        if not rows:
            return None
        try:
            return json.loads(rows[0][0])
        except Exception:  # noqa: BLE001
            logger.warning("Approval %s has an unreadable payload", request_id)
            return None

    async def poll(self, request_id: str) -> dict[str, Any] | None:
        row = self._fetch(request_id)
        if row is None or not row.get("decision"):
            return None
        return row

    def decide(
        self, request_id: str, *, decision: str, responder: str, reason: str = ""
    ) -> dict[str, Any]:
        """Record a decision, and refuse if one is already there.

        The check and the write are **one conditional statement**. Reading the
        row, seeing no decision, and then writing is a race: two responders can
        both read an unanswered request and both write, and the later one wins
        silently. The Azure Table version closed that with an ETag; here the
        ``WHERE`` clause does it, and ``rowcount`` reports who won.

        Losing this race is not an error condition to paper over -- it means
        somebody else answered first, and their answer stands.
        """
        row = self._fetch(request_id)
        if row is None:
            raise KeyError(f"No approval request with id {request_id!r}")
        if row.get("decision"):
            raise ValueError(
                f"Approval {request_id} was already answered "
                f"({row['decision']} by {row.get('responder') or 'unknown'})"
            )

        row.update(
            {
                "decision": decision,
                "responder": responder,
                "reason": reason,
                "decided_at": _utcnow(),
            }
        )
        payload = json.dumps(row, default=str)

        try:
            won = self._db.execute(
                f"UPDATE {self._table} "
                f"   SET decision = ?, responder = ?, decided_at = ?, payload = ? "
                f" WHERE request_id = ? AND (decision IS NULL OR decision = '')",
                decision,
                responder,
                row["decided_at"],
                payload,
                request_id,
            )
        except Exception as exc:  # noqa: BLE001
            # A write that did not happen must not read as a decision. The gate
            # polls the database, so returning normally here would report an
            # approval nobody recorded.
            raise RuntimeError(
                f"Could not record the decision for {request_id} "
                f"({type(exc).__name__}); it has not been answered"
            ) from exc

        if not won:
            current = self._fetch(request_id) or {}
            raise ValueError(
                f"Approval {request_id} was already answered "
                f"({current.get('decision', 'unknown')} by "
                f"{current.get('responder') or 'unknown'})"
            )

        logger.info("Approval %s -> %s by %s", request_id, decision, responder)
        return row

    def pending(self) -> list[dict[str, Any]]:
        try:
            rows = self._db.query(
                f"SELECT payload FROM {self._table} WHERE decision IS NULL OR decision = ''"
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Could not list pending approvals (%s)", type(exc).__name__)
            return []
        out: list[dict[str, Any]] = []
        for (raw,) in rows:
            try:
                out.append(json.loads(raw))
            except Exception:  # noqa: BLE001
                continue
        return out

    def get(self, request_id: str) -> dict[str, Any] | None:
        return self._fetch(request_id)

    def _persist(self, row: dict[str, Any]) -> None:
        self._write(row["request_id"], row)

    def _write(self, request_id: str, row: dict[str, Any]) -> None:
        payload = json.dumps(row, default=str)
        args = (
            row.get("decision") or None,
            row.get("responder") or None,
            row.get("decided_at") or None,
            payload,
            request_id,
        )
        try:
            updated = self._db.execute(
                f"UPDATE {self._table} SET decision = ?, responder = ?, "
                f"decided_at = ?, payload = ? WHERE request_id = ?",
                *args,
            )
            if not updated:
                try:
                    self._db.execute(
                        f"INSERT INTO {self._table} "
                        f"(request_id, decision, responder, decided_at, payload) "
                        f"VALUES (?, ?, ?, ?, ?)",
                        request_id,
                        row.get("decision") or None,
                        row.get("responder") or None,
                        row.get("decided_at") or None,
                        payload,
                    )
                except self._db.integrity_error():
                    self._db.execute(
                        f"UPDATE {self._table} SET decision = ?, responder = ?, "
                        f"decided_at = ?, payload = ? WHERE request_id = ?",
                        *args,
                    )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Could not write approval %s (%s); the gate will fail closed",
                request_id,
                type(exc).__name__,
            )

    def _on_reset(self) -> None:
        try:
            self._db.execute(f"DELETE FROM {self._table}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not clear %s (%s)", self._table_name, type(exc).__name__)
