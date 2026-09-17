"""Fail-closed Azure SQL incidents with redaction in the shared store boundary.

The base class owns deduplication, counts and redaction. Its local snapshot is
used only to calculate a change from a fresh SQL read. A failed write never
reports a terminal outcome as durable, and recovery never replays that snapshot.
Schema creation belongs to deployment, not an agent invocation.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from triage.models import Incident, TriageResult
from triage.pipeline_models import PipelineFailure
from triage.store.azure_sql import AzureSqlDatabase, SqlUnavailable, quote_identifier
from triage.store.incidents import InMemoryIncidentStore, _utcnow

logger = logging.getLogger("triage.store.sql.incidents")


class AzureSqlIncidentStore(InMemoryIncidentStore):
    """Read-through incidents; SQL errors and concurrent changes stop finalization."""

    def __init__(self, *, db: AzureSqlDatabase, table: str = "triage_incidents") -> None:
        super().__init__()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._loaded = False
        self._revisions: dict[str, bytes] = {}
        self._ensure_loaded()

    @property
    def is_durable(self) -> bool:
        return self._loaded and self._db.is_available

    def _ensure_loaded(self, where: str = "", *params: Any) -> None:
        """Replace the working snapshot, even after a previously successful read."""
        with self._lock:
            self._loaded = False
            self._items.clear()
            self._revisions.clear()
            try:
                self._load(where, *params)
            except Exception as exc:
                logger.error(
                    "Cannot read deployed incident state in %s (%s); triage stopped",
                    self._table_name, type(exc).__name__,
                )
                raise
            self._loaded = True

    def _load(self, where: str = "", *params: Any) -> None:
        rows = self._db.query(f"SELECT payload FROM {self._table}{where}", *params)
        loaded: dict[str, Incident] = {}
        revisions: dict[str, bytes] = {}
        try:
            for (raw,) in rows:
                incident = Incident.model_validate_json(raw)
                if (
                    not incident.id or not incident.signature or incident.id in loaded
                    or incident.occurrence_count < 1 or incident.notified_count < 0
                ):
                    raise ValueError("Invalid incident identity or counts")
                loaded[incident.id] = incident
                # Match the original SQL NVARCHAR bytes, not a reserialized model
                # whose defaults or formatting can change the revision.
                revisions[incident.id] = hashlib.sha256(raw.encode("utf-16-le")).digest()
        except (TypeError, ValueError, AttributeError) as exc:
            raise SqlUnavailable(
                f"Unreadable incident state in {self._table_name}; repair deployed state."
            ) from exc
        self._items = loaded
        self._revisions = revisions

    def find_open(self, signature: str) -> Incident | None:
        with self._lock:
            self._ensure_loaded(" WHERE signature = ?", signature[:200])
            return super().find_open(signature)

    def get(self, incident_id_: str) -> Incident | None:
        with self._lock:
            self._ensure_loaded(" WHERE incident_id = ?", incident_id_)
            return super().get(incident_id_)

    def list_all(self) -> list[Incident]:
        with self._lock:
            self._ensure_loaded()
            return super().list_all()

    def record(self, result: TriageResult, **provenance: Any) -> Incident:
        with self._lock:
            self._ensure_loaded(" WHERE signature = ?", result.signature[:200])
            return super().record(result, **provenance)

    def mark(self, incident_id_: str, status: str, notes: str = "") -> Incident | None:
        with self._lock:
            self._ensure_loaded(" WHERE incident_id = ?", incident_id_)
            return super().mark(incident_id_, status, notes)

    def note_pipeline_occurrence(
        self, incident_id_: str, failure: PipelineFailure,
    ) -> Incident | None:
        with self._lock:
            self._ensure_loaded(" WHERE incident_id = ?", incident_id_)
            return super().note_pipeline_occurrence(incident_id_, failure)

    def _persist(self, incident: Incident) -> None:
        try:
            self._upsert(incident)
        except Exception as exc:
            self._loaded = False
            self._items.clear()
            self._revisions.clear()
            logger.error(
                "Incident %s write unconfirmed (%s); not retried",
                incident.id, type(exc).__name__,
            )
            raise

    def _upsert(self, incident: Incident) -> None:
        payload = incident.model_dump_json()
        revision = self._revisions.get(incident.id)
        if revision is None:
            changed = self._db.execute(
                f"INSERT INTO {self._table} "
                f"(incident_id, signature, status, updated_at, payload) "
                f"VALUES (?, ?, ?, ?, ?)",
                incident.id[:200], incident.signature[:200], incident.status, _utcnow(), payload,
            )
        else:
            # A fresh read alone cannot prevent two writers losing an occurrence
            # or notification count. Reject a changed snapshot instead of replaying it.
            changed = self._db.execute(
                f"UPDATE {self._table} SET signature = ?, status = ?, updated_at = ?, "
                f"payload = ? WHERE incident_id = ? AND HASHBYTES('SHA2_256', payload) = ?",
                incident.signature[:200], incident.status, _utcnow(), payload,
                incident.id[:200], revision,
            )
        if changed != 1:
            raise SqlUnavailable(
                "Incident write was not confirmed or its shared revision changed; "
                "reload before finalizing."
            )
        self._revisions[incident.id] = hashlib.sha256(payload.encode("utf-16-le")).digest()

    def _on_reset(self) -> None:
        self._loaded = False
        self._revisions.clear()
        try:
            if self._db.execute(f"DELETE FROM {self._table}") < 0:
                raise SqlUnavailable("Incident reset returned no reliable affected-row count.")
        except Exception as exc:
            logger.error("Could not clear %s (%s)", self._table_name, type(exc).__name__)
            raise
        self._loaded = True
