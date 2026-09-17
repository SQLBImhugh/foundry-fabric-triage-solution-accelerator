"""Durable, at-most-once submission fences for approved pipeline reruns.

A failed HTTP response can mean the pipeline started but its acknowledgement
was lost. Reserve before POST and never expire that reservation automatically.
An operator must reconcile an unknown submission; another POST is not a retry
of the transport, it is another execution of the pipeline.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Any, Protocol

from triage.pipeline_models import PipelineRerunRecord, RerunState
from triage.redaction import redact_text
from triage.store.azure_sql import quote_identifier

logger = logging.getLogger("triage.store.pipeline_reruns")


def _key(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _redacted(record: PipelineRerunRecord) -> PipelineRerunRecord:
    return record.model_copy(update={"detail": redact_text(record.detail)[:2000]})


class PipelineRerunStore(Protocol):
    @property
    def is_durable(self) -> bool: ...
    def get(self, key: str) -> PipelineRerunRecord | None: ...
    def reserve(self, record: PipelineRerunRecord) -> bool: ...
    def update(self, record: PipelineRerunRecord, *, expected: RerunState) -> bool: ...
    def pending(self, workspace_id: str, pipeline_id: str) -> list[PipelineRerunRecord]: ...


class InMemoryPipelineRerunStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._items: dict[str, PipelineRerunRecord] = {}

    @property
    def is_durable(self) -> bool:
        return False

    def get(self, key: str) -> PipelineRerunRecord | None:
        with self._lock:
            return self._items.get(_key(key))

    def reserve(self, record: PipelineRerunRecord) -> bool:
        if record.state != "reserved":
            raise ValueError("A rerun must start with a reservation")
        with self._lock:
            key = _key(record.key)
            if key in self._items:
                return False
            self._items[key] = _redacted(record)
            try:
                self._persist()
            except Exception:
                self._items.pop(key)
                raise
            return True

    def update(self, record: PipelineRerunRecord, *, expected: RerunState) -> bool:
        with self._lock:
            key = _key(record.key)
            prior = self._items.get(key)
            if prior is None or prior.state != expected:
                return False
            self._items[key] = _redacted(record)
            try:
                self._persist()
            except Exception:
                self._items[key] = prior
                raise
            return True

    def pending(self, workspace_id: str, pipeline_id: str) -> list[PipelineRerunRecord]:
        with self._lock:
            return [
                item
                for item in self._items.values()
                if item.workspace_id == workspace_id
                and item.pipeline_id == pipeline_id
                and item.state == "submitted"
            ]

    def _persist(self) -> None:
        pass


class JsonFilePipelineRerunStore(InMemoryPipelineRerunStore):
    """Offline single-process persistence; not a distributed claim mechanism."""

    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)
        if self.path.exists():
            # Corrupt safety state must refuse reruns, never become an empty log.
            self._items = {
                key: PipelineRerunRecord.model_validate(row)
                for key, row in json.loads(self.path.read_text(encoding="utf-8")).items()
            }

    def _persist(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        temp.write_text(
            json.dumps({key: value.model_dump(mode="json") for key, value in self._items.items()}),
            encoding="utf-8",
        )
        temp.replace(self.path)


class AzureSqlPipelineRerunStore:
    """Read-through SQL state. Database errors propagate and prevent dispatch."""

    def __init__(self, *, db: Any, table: str = "triage_pipeline_reruns") -> None:
        self._db = db
        self._table = quote_identifier(table)

    @property
    def is_durable(self) -> bool:
        return self._db.is_available

    def get(self, key: str) -> PipelineRerunRecord | None:
        rows = self._db.query(
            f"SELECT payload FROM {self._table} WHERE run_key = ?", _key(key)
        )
        return PipelineRerunRecord.model_validate_json(rows[0][0]) if rows else None

    def reserve(self, record: PipelineRerunRecord) -> bool:
        if record.state != "reserved":
            raise ValueError("A rerun must start with a reservation")
        clean = _redacted(record)
        try:
            inserted = self._db.execute(
                f"INSERT INTO {self._table} "
                "(run_key, workspace_id, pipeline_id, state, payload) VALUES (?, ?, ?, ?, ?)",
                _key(clean.key), clean.workspace_id, clean.pipeline_id,
                clean.state, clean.model_dump_json(),
            )
        except self._db.integrity_error():
            return False
        if inserted != 1:
            raise RuntimeError("Pipeline rerun reservation did not insert exactly one row")
        return True

    def update(self, record: PipelineRerunRecord, *, expected: RerunState) -> bool:
        clean = _redacted(record)
        return self._db.execute(
            f"UPDATE {self._table} SET state = ?, payload = ? "
            "WHERE run_key = ? AND state = ?",
            clean.state, clean.model_dump_json(), _key(clean.key), expected,
        ) == 1

    def pending(self, workspace_id: str, pipeline_id: str) -> list[PipelineRerunRecord]:
        return [
            PipelineRerunRecord.model_validate_json(row[0])
            for row in self._db.query(
                f"SELECT payload FROM {self._table} "
                "WHERE workspace_id = ? AND pipeline_id = ? AND state = 'submitted'",
                workspace_id, pipeline_id,
            )
        ]
