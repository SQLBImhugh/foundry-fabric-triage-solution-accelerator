"""Baselines for the silent-failure detector.

Detecting "this did not move" requires knowing where it was. That is the whole
difficulty: a stale model looks exactly like a healthy one in a single reading,
and only differs from its own history.

Two rules govern what goes in here, and both exist because a detector that
cries wolf gets muted, after which it may as well not exist:

**Baselines are only ever updated from healthy observations.** Accepting a
suspect reading as the new normal teaches the detector that the failure is
fine, and it never alerts again. That is a detector which reports success
while blind, which is worse than no detector.

**A suspect reading is not a finding.** The first anomalous scan records
suspicion and says nothing. A finding needs the condition to survive a
confirmation scan, because a probe run mid-refresh sees a half-loaded table
and would otherwise page somebody about a model that was fine ninety seconds
later.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from triage.store.azure_sql import SqlUnavailable, quote_identifier

logger = logging.getLogger("triage.store.semantic_health")


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def probe_key(workspace_id: str, dataset_id: str, probe_name: str) -> str:
    """Stable, key-safe identity for one probe on one model."""
    raw = f"{workspace_id}|{dataset_id}|{probe_name}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


@dataclass
class ProbeState:
    """What healthy looked like last time, and how odd things look now."""

    workspace_id: str
    dataset_id: str
    probe_name: str
    report_name: str = ""

    #: Last accepted healthy values. Never written from a suspect scan.
    last_max_date: str = ""
    last_row_count: int | None = None
    last_control_totals: dict[str, float] = field(default_factory=dict)
    last_healthy_at: str = ""

    #: How many consecutive scans have looked wrong, and since when.
    suspect_count: int = 0
    first_suspect_at: str = ""
    suspect_kind: str = ""

    #: Detector health, kept apart from data health on purpose. A probe that
    #: cannot run is not evidence that the data is stale.
    consecutive_errors: int = 0
    last_error: str = ""
    #: When this probe was parked for repeated failure. Empty when running.
    circuit_opened_at: str = ""

    #: The model's shape as last seen: sorted ``table[column]`` and measure
    #: names. Only populated for probes that opt into schema watching, because
    #: it costs an extra query per sweep.
    last_schema: list[str] = field(default_factory=list)

    observations: int = 0
    updated_at: str = field(default_factory=_utcnow)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ProbeState:
        """Rebuild from stored JSON, ignoring fields this version does not know.

        ``ProbeState(**raw)`` raises on an unexpected key, which makes the store
        unreadable the moment two versions of the agent share it -- a newer
        instance writes a new field, an older one crashes reading its own
        table. During a rolling deploy that is a detector-wide outage caused by
        adding a field, so unknown keys are dropped instead.
        """
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known})


class SemanticHealthStore(Protocol):
    def get(self, workspace_id: str, dataset_id: str, probe_name: str) -> ProbeState | None: ...
    def put(self, state: ProbeState) -> None: ...
    def all_states(self) -> list[ProbeState]: ...
    def reset(self) -> None: ...
    def try_acquire_lease(self, name: str, owner: str, ttl_seconds: int) -> bool: ...
    def release_lease(self, name: str, owner: str) -> None: ...


class InMemorySemanticHealthStore:
    """Correct for one process, useless across hosted-agent invocations.

    A detector whose memory dies with the process compares every reading
    against nothing, so it can never conclude that something failed to move --
    the exact question it exists to answer.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: dict[str, dict[str, Any]] = {}
        self._leases: dict[str, tuple[str, float]] = {}

    def try_acquire_lease(self, name: str, owner: str, ttl_seconds: int) -> bool:
        """Claim the right to sweep. In one process this is exact.

        Correct here and insufficient in the hosted shape, which is why the
        Azure Table store overrides it: two container instances woken by the
        same schedule would each hold their own dictionary and both proceed.
        """
        now = time.time()
        with self._lock:
            held = self._leases.get(name)
            if held and held[0] != owner and held[1] > now:
                return False
            self._leases[name] = (owner, now + ttl_seconds)
            return True

    def release_lease(self, name: str, owner: str) -> None:
        with self._lock:
            held = self._leases.get(name)
            if held and held[0] == owner:
                del self._leases[name]

    def get(self, workspace_id: str, dataset_id: str, probe_name: str) -> ProbeState | None:
        with self._lock:
            raw = self._items.get(probe_key(workspace_id, dataset_id, probe_name))
            return ProbeState.from_dict(raw) if raw else None

    def put(self, state: ProbeState) -> None:
        state.updated_at = _utcnow()
        with self._lock:
            key = probe_key(state.workspace_id, state.dataset_id, state.probe_name)
            self._items[key] = state.as_dict()
            self._persist(key, state)

    def all_states(self) -> list[ProbeState]:
        with self._lock:
            return [ProbeState.from_dict(raw) for raw in self._items.values()]

    def reset(self) -> None:
        with self._lock:
            self._items.clear()
            self._on_reset()

    @property
    def is_durable(self) -> bool:
        return False

    # --- durability hooks --------------------------------------------------

    def _persist(self, key: str, state: ProbeState) -> None:  # pragma: no cover
        """No-op. Caller holds the lock."""

    def _on_reset(self) -> None:  # pragma: no cover - no-op base
        """No-op."""


class JsonFileSemanticHealthStore(InMemorySemanticHealthStore):
    """Survives a restart offline, with no Azure dependency."""

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
            logger.warning("Unreadable semantic health store (%s)", type(exc).__name__)
            return
        if isinstance(raw, dict):
            self._items.update(raw)

    def _reload(self) -> None:
        self._items.clear()
        self._load()

    def get(self, workspace_id: str, dataset_id: str, probe_name: str) -> ProbeState | None:
        with self._lock:
            self._reload()
        return super().get(workspace_id, dataset_id, probe_name)

    def all_states(self) -> list[ProbeState]:
        with self._lock:
            self._reload()
        return super().all_states()

    def _persist(self, key: str, state: ProbeState) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._items, indent=2), encoding="utf-8")

    def _on_reset(self) -> None:
        if self._path.exists():
            self._path.unlink()


class AzureSqlSemanticHealthStore(InMemorySemanticHealthStore):
    """Shared baselines and sweep leases. Unknown state is not a healthy scan."""

    def __init__(
        self,
        *,
        db: Any,
        table: str = "triage_semantic_health",
        lease_table: str = "triage_sweep_leases",
    ) -> None:
        super().__init__()
        self._lock = threading.RLock()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._lease_table = quote_identifier(lease_table)
        self._loaded = False
        self._revisions: dict[str, bytes] = {}
        self._ensure_loaded()

    @property
    def is_durable(self) -> bool:
        return self._loaded and self._db.is_available

    def _ensure_loaded(self, key: str | None = None) -> None:
        self._loaded = False
        self._items.clear()
        self._revisions.clear()
        try:
            self._load(key)
        except Exception as exc:
            logger.error(
                "Cannot read deployed probe state in %s (%s); scan stopped",
                self._table_name, type(exc).__name__,
            )
            raise
        self._loaded = True

    @staticmethod
    def _validate_state(key: str, row: Any) -> None:
        if not isinstance(row, dict) or set(row) != {f.name for f in fields(ProbeState)}:
            raise ValueError("Probe state does not match the deployed model")
        for name in (
            "workspace_id", "dataset_id", "probe_name", "report_name", "last_max_date",
            "last_healthy_at", "first_suspect_at", "suspect_kind", "last_error",
            "circuit_opened_at", "updated_at",
        ):
            if not isinstance(row[name], str):
                raise ValueError("Invalid probe text field")
        if (
            not row["workspace_id"] or not row["dataset_id"] or not row["probe_name"]
            or key != probe_key(row["workspace_id"], row["dataset_id"], row["probe_name"])
        ):
            raise ValueError("Invalid probe identity")
        for name in ("suspect_count", "consecutive_errors", "observations"):
            if type(row[name]) is not int or row[name] < 0:
                raise ValueError("Invalid probe count")
        count = row["last_row_count"]
        if count is not None and (type(count) is not int or count < 0):
            raise ValueError("Invalid baseline row count")
        totals = row["last_control_totals"]
        if not isinstance(totals, dict) or any(
            not isinstance(name, str) or type(value) not in (float, int) or not math.isfinite(value)
            for name, value in totals.items()
        ):
            raise ValueError("Invalid baseline totals")
        if not isinstance(row["last_schema"], list) or any(
            not isinstance(name, str) for name in row["last_schema"]
        ):
            raise ValueError("Invalid probe schema")
        for name in ("last_healthy_at", "first_suspect_at", "circuit_opened_at", "updated_at"):
            if row[name] and datetime.fromisoformat(row[name]).tzinfo is None:
                raise ValueError("Probe timestamp has no timezone")
        if not row["updated_at"]:
            raise ValueError("Missing probe update timestamp")

    def _load(self, key: str | None = None) -> None:
        where = " WHERE probe_key = ?" if key is not None else ""
        params = (key,) if key is not None else ()
        rows = self._db.query(f"SELECT probe_key, payload FROM {self._table}{where}", *params)
        loaded: dict[str, dict[str, Any]] = {}
        revisions: dict[str, bytes] = {}
        try:
            for stored_key, raw in rows:
                row = json.loads(raw)
                self._validate_state(stored_key, row)
                if stored_key in loaded:
                    raise ValueError("Duplicate probe state")
                loaded[stored_key] = row
                revisions[stored_key] = hashlib.sha256(raw.encode("utf-16-le")).digest()
        except (TypeError, ValueError, AttributeError, OverflowError) as exc:
            raise SqlUnavailable(
                f"Unreadable probe state in {self._table_name}; repair deployed state."
            ) from exc
        self._items = loaded
        self._revisions = revisions

    def get(self, workspace_id: str, dataset_id: str, probe_name: str) -> ProbeState | None:
        with self._lock:
            self._ensure_loaded(probe_key(workspace_id, dataset_id, probe_name))
            return super().get(workspace_id, dataset_id, probe_name)

    def all_states(self) -> list[ProbeState]:
        with self._lock:
            self._ensure_loaded()
            return super().all_states()

    def put(self, state: ProbeState) -> None:
        with self._lock:
            self._ensure_loaded(probe_key(state.workspace_id, state.dataset_id, state.probe_name))
            super().put(state)

    def _persist(self, key: str, state: ProbeState) -> None:
        try:
            raw = state.as_dict()
            self._validate_state(key, raw)
            payload = json.dumps(raw, allow_nan=False)
            promoted = (
                state.probe_name, state.report_name, state.last_max_date,
                state.last_row_count, state.suspect_count, payload,
            )
            revision = self._revisions.get(key)
            if revision is None:
                changed = self._db.execute(
                    f"INSERT INTO {self._table} (probe_key, probe_name, "
                    "report_name, last_max_date, last_row_count, "
                    "suspect_count, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    key, *promoted,
                )
            else:
                changed = self._db.execute(
                    f"UPDATE {self._table} SET probe_name = ?, report_name = ?, "
                    "last_max_date = ?, last_row_count = ?, suspect_count = ?, "
                    "payload = ? WHERE probe_key = ? AND HASHBYTES('SHA2_256', payload) = ?",
                    *promoted, key, revision,
                )
            if changed != 1:
                raise SqlUnavailable(
                    "Probe write was not confirmed or its shared revision changed; reload before continuing."
                )
        except Exception as exc:
            self._loaded = False
            self._items.clear()
            self._revisions.clear()
            logger.error(
                "Probe baseline %s write unconfirmed (%s); scan stopped",
                state.probe_name, type(exc).__name__,
            )
            raise
        self._revisions[key] = hashlib.sha256(payload.encode("utf-16-le")).digest()

    def try_acquire_lease(self, name: str, owner: str, ttl_seconds: int) -> bool:
        """Claim the sweep across instances, using the database as the arbiter.

        A hosted agent is rebuilt per request and a schedule can wake more than
        one instance, so an in-process lock decides nothing. Two sweeps running
        together would each increment ``suspect_count`` for the same probe and
        confirm a finding on its first real occurrence -- turning the
        suspect-then-confirm rule, which exists to stop false positives, into a
        generator of them.

        Conditional UPDATE and INSERT statements use database time and report
        the winner by row count. An unavailable database is an error, not an
        ordinary competing sweep. Neither statement falls back to local leases.
        """
        if not name or not owner or ttl_seconds <= 0:
            raise ValueError("A sweep lease requires a name, owner and positive lifetime.")
        try:
            won = self._db.execute(
                f"UPDATE {self._lease_table} "
                f"   SET owner = ?, expires_at = DATEADD(second, ?, SYSUTCDATETIME()) "
                f" WHERE lease_name = ? "
                f"   AND (expires_at < SYSUTCDATETIME() OR owner = ?)",
                owner,
                int(ttl_seconds),
                name,
                owner,
            )
            if won not in (0, 1):
                raise SqlUnavailable("Sweep lease update returned no reliable row count.")
            if won == 1:
                return True
            inserted = self._db.execute(
                f"INSERT INTO {self._lease_table} (lease_name, owner, expires_at) "
                "SELECT ?, ?, DATEADD(second, ?, SYSUTCDATETIME()) "
                f"WHERE NOT EXISTS (SELECT 1 FROM {self._lease_table} "
                "WITH (UPDLOCK, HOLDLOCK) WHERE lease_name = ?)",
                name, owner, int(ttl_seconds), name,
            )
            if inserted not in (0, 1):
                raise SqlUnavailable("Sweep lease insert returned no reliable row count.")
            return inserted == 1
        except Exception as exc:
            self._loaded = False
            logger.error("Sweep lease write unconfirmed (%s); scan stopped", type(exc).__name__)
            raise

    def release_lease(self, name: str, owner: str) -> None:
        try:
            released = self._db.execute(
                f"DELETE FROM {self._lease_table} WHERE lease_name = ? AND owner = ?",
                name,
                owner,
            )
            if released not in (0, 1):
                raise SqlUnavailable("Sweep lease release returned no reliable row count.")
        except Exception as exc:
            self._loaded = False
            logger.error("Sweep lease release unconfirmed (%s)", type(exc).__name__)
            raise

    def _on_reset(self) -> None:
        self._loaded = False
        self._revisions.clear()
        try:
            if self._db.execute(f"DELETE FROM {self._table}") < 0:
                raise SqlUnavailable("Probe reset returned no reliable affected-row count.")
        except Exception as exc:
            logger.error("Could not clear %s (%s)", self._table_name, type(exc).__name__)
            raise
        self._loaded = True
