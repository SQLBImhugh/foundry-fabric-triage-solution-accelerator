"""The data quality flag table.

Backed by a CSV so it can be opened in Excel mid-demo and shown before/after.
Swap for a Fabric Lakehouse table or SQL table in production — the interface
is three methods.
"""

from __future__ import annotations

import csv
import threading
import uuid
from pathlib import Path

from triage.models import DataQualityFlag

_COLUMNS = [
    "flag_id",
    "flagged_at",
    "request_id",
    "table_name",
    "issue_type",
    "key_columns",
    "duplicate_group_count",
    "duplicate_row_count",
    "total_row_count",
    "detail",
    "detected_by",
    "status",
]


class DataQualityFlagTable:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._ensure_header()

    def _ensure_header(self) -> None:
        if self.path.exists():
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", newline="", encoding="utf-8") as fh:
            csv.DictWriter(fh, fieldnames=_COLUMNS).writeheader()

    def read_all(self) -> list[dict[str, str]]:
        if not self.path.exists():
            return []
        with self.path.open(newline="", encoding="utf-8-sig") as fh:
            return list(csv.DictReader(fh))

    def append(self, flag: DataQualityFlag) -> DataQualityFlag:
        with self._lock:
            self._ensure_header()
            with self.path.open("a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=_COLUMNS)
                writer.writerow({c: getattr(flag, c) for c in _COLUMNS})
        return flag

    def reset(self) -> None:
        with self._lock:
            if self.path.exists():
                self.path.unlink()
            self._ensure_header()

    @property
    def row_count(self) -> int:
        return len(self.read_all())


def build_flag(*, request_id: str, evidence, detail: str) -> DataQualityFlag:
    """Construct a flag row from deterministic evidence."""
    return DataQualityFlag(
        flag_id=f"dqf-{uuid.uuid4().hex[:8]}",
        request_id=request_id,
        table_name=evidence.table,
        issue_type="duplicates",
        key_columns=",".join(evidence.key_columns),
        duplicate_group_count=evidence.duplicate_group_count,
        duplicate_row_count=evidence.duplicate_row_count,
        total_row_count=evidence.total_row_count,
        detail=detail,
    )
