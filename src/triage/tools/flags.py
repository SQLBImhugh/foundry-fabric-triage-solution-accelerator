"""Data quality flags: explicit offline CSV or fail-closed Azure SQL persistence."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import threading
from pathlib import Path
from typing import Protocol

from triage.models import DataQualityFlag
from triage.redaction import redact_text
from triage.store.azure_sql import AzureSqlDatabase, SqlUnavailable, quote_identifier

logger = logging.getLogger("triage.tools.flags")

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


class FlagStore(Protocol):
    def read_all(self) -> list[dict[str, str]]: ...
    def append(self, flag: DataQualityFlag) -> DataQualityFlag: ...
    def reset(self) -> None: ...

    @property
    def row_count(self) -> int: ...


def _persisted_flag(flag: DataQualityFlag) -> DataQualityFlag:
    return DataQualityFlag.model_validate({
        key: redact_text(value) if isinstance(value, str) else value
        for key, value in flag.model_dump().items()
    })


class DataQualityFlagTable:
    """CSV state for explicit offline scenarios, never a live fallback."""

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
        flag = _persisted_flag(flag)
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


class AzureSqlFlagTable:
    """Append-only live flags with original-ID reconciliation and no local cache."""

    def __init__(self, db: AzureSqlDatabase, table: str = "triage_data_quality_flags"):
        self._db = db
        self._table = quote_identifier(table)

    @staticmethod
    def _decode(raw: str) -> DataQualityFlag:
        try:
            return DataQualityFlag.model_validate_json(raw)
        except (ValueError, TypeError) as exc:
            raise SqlUnavailable("Stored data quality flag is unreadable.") from exc

    def read_all(self) -> list[dict[str, str]]:
        rows = self._db.query(f"SELECT payload FROM {self._table} ORDER BY flagged_at, flag_id")
        return [
            {key: str(value) for key, value in self._decode(raw).model_dump().items()}
            for (raw,) in rows
        ]

    def append(self, flag: DataQualityFlag) -> DataQualityFlag:
        flag = _persisted_flag(flag)
        payload = flag.model_dump_json()
        try:
            changed = self._db.execute(
                f"INSERT INTO {self._table} (flag_id, request_id, flagged_at, payload) "
                "SELECT ?, ?, ?, ? WHERE NOT EXISTS "
                f"(SELECT 1 FROM {self._table} WITH (UPDLOCK, HOLDLOCK) WHERE flag_id = ?)",
                flag.flag_id, flag.request_id, flag.flagged_at, payload, flag.flag_id,
            )
            if changed == 1:
                return flag
            if changed != 0:
                raise SqlUnavailable("Data quality flag write returned an unconfirmed row count.")
            rows = self._db.query(
                f"SELECT payload FROM {self._table} WHERE flag_id = ?", flag.flag_id,
            )
            if len(rows) != 1:
                raise SqlUnavailable("The original data quality flag could not be reconciled.")
            original = self._decode(rows[0][0])
            if original.model_dump(exclude={"flagged_at"}) != flag.model_dump(exclude={"flagged_at"}):
                raise SqlUnavailable("The data quality flag identity conflicts with its stored evidence.")
            return original
        except Exception as exc:
            logger.error("Data quality flag write unconfirmed (%s); not retried.", type(exc).__name__)
            raise

    def reset(self) -> None:
        raise ValueError("Live data quality flags may be reset only by the reviewed deployment reset.")

    @property
    def row_count(self) -> int:
        rows = self._db.query(f"SELECT COUNT(*) FROM {self._table}")
        if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int or rows[0][0] < 0:
            raise SqlUnavailable("Data quality flag count could not be verified.")
        return rows[0][0]


def build_flag(*, request_id: str, evidence, detail: str) -> DataQualityFlag:
    """Construct a flag row from deterministic evidence."""
    identity = json.dumps({
        "request_id": request_id, "table": evidence.table,
        "key_columns": evidence.key_columns,
        "duplicate_group_count": evidence.duplicate_group_count,
        "duplicate_row_count": evidence.duplicate_row_count,
        "total_row_count": evidence.total_row_count,
    }, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return DataQualityFlag(
        flag_id=f"dqf-{hashlib.sha256(identity.encode('utf-8')).hexdigest()}",
        request_id=request_id,
        table_name=evidence.table,
        issue_type="duplicates",
        key_columns=",".join(evidence.key_columns),
        duplicate_group_count=evidence.duplicate_group_count,
        duplicate_row_count=evidence.duplicate_row_count,
        total_row_count=evidence.total_row_count,
        detail=detail,
    )
