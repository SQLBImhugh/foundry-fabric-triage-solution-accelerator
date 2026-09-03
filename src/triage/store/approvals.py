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

Two things write decisions, and they share this one table so the agent cannot
tell them apart:

* ``bi-triage approve|deny`` -- needs no infrastructure at all, and is how an
  on-call engineer holding the repo would answer.
* a Power Automate flow behind the card's buttons -- the demo path, where the
  click in Teams lands here.

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

from triage.store.table_helpers import build_table_client

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


class AzureTableApprovalChannel(InMemoryApprovalChannel):
    """The deployed path: the agent polls here, a human writes here.

    Degrades to in-memory rather than refusing to start, like the other
    stores -- but logs an error, because in that state no human can answer and
    every gated action will fail closed.
    """

    _PARTITION = "approval"

    def __init__(
        self,
        *,
        endpoint: str,
        table_name: str = "approvals",
        credential: Any = None,
    ) -> None:
        super().__init__()
        self._endpoint = endpoint
        self._table_name = table_name
        self._client = None
        self._degraded = False

        try:
            self._client = self._build_client(credential)
        except Exception as exc:
            self._degraded = True
            logger.error(
                "Approval channel degraded to in-memory: could not open %s at %s (%s). "
                "No human will be able to answer; gated actions will fail closed.",
                table_name,
                endpoint,
                type(exc).__name__,
            )

    @property
    def is_durable(self) -> bool:
        return self._client is not None and not self._degraded

    def _build_client(self, credential: Any):
        return build_table_client(self._endpoint, self._table_name, credential)

    def _fetch(self, request_id: str) -> dict[str, Any] | None:
        if self._client is None:
            return None
        try:
            entity = self._client.get_entity(self._PARTITION, request_id)
        except Exception:
            return None
        return {k: v for k, v in entity.items() if not k.startswith(("PartitionKey", "RowKey", "odata", "Timestamp"))}

    async def poll(self, request_id: str) -> dict[str, Any] | None:
        row = self._fetch(request_id)
        if row is None or not row.get("decision"):
            return None
        return row

    def decide(
        self, request_id: str, *, decision: str, responder: str, reason: str = ""
    ) -> dict[str, Any]:
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
        self._write(request_id, row)
        logger.info("Approval %s -> %s by %s", request_id, decision, responder)
        return row

    def pending(self) -> list[dict[str, Any]]:
        if self._client is None:
            return super().pending()
        out: list[dict[str, Any]] = []
        for entity in self._client.list_entities():
            if not entity.get("decision"):
                out.append({k: v for k, v in entity.items() if not k.startswith(("PartitionKey", "odata", "Timestamp"))})
        return out

    def get(self, request_id: str) -> dict[str, Any] | None:
        return self._fetch(request_id)

    def open(self, request: Any) -> None:
        super().open(request)

    def _persist(self, row: dict[str, Any]) -> None:
        self._write(row["request_id"], row)

    def _write(self, request_id: str, row: dict[str, Any]) -> None:
        if self._client is None:
            return
        entity = {"PartitionKey": self._PARTITION, "RowKey": request_id, **row}
        try:
            self._client.upsert_entity(entity)
        except Exception as exc:
            self._degraded = True
            logger.error(
                "Could not write approval %s (%s); the gate will fail closed",
                request_id,
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
                logger.warning("Could not delete approval row %s", entity.get("RowKey"))
