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
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

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


class FabricSqlSemanticHealthStore(InMemorySemanticHealthStore):
    """The deployed path. Degrades to in-memory, loudly, and keeps retrying.

    Degraded, every sweep starts with no history and can never detect a
    watermark that failed to advance. The detector would run, find nothing,
    and report health it has not established -- so this logs an error rather
    than a warning, and reconnects rather than staying blind for the life of
    the container.
    """

    def __init__(
        self,
        *,
        db: Any,
        table: str = "triage_semantic_health",
        lease_table: str = "triage_sweep_leases",
    ) -> None:
        from triage.store.fabric_sql import quote_identifier

        super().__init__()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._lease_table = quote_identifier(lease_table)
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
                "Semantic health store degraded to in-memory: cannot read %s (%s). "
                "Every sweep will start blind and cannot detect staleness.",
                self._table_name,
                type(exc).__name__,
            )
            return False
        self._loaded = True
        logger.info(
            "Loaded %d probe baseline(s) from %s", len(self._items), self._table_name
        )
        return True

    def _load(self) -> None:
        rows = self._db.query(f"SELECT probe_key, payload FROM {self._table}")
        loaded: dict[str, dict[str, Any]] = {}
        for key, raw in rows:
            if not raw:
                continue
            try:
                loaded[str(key)] = json.loads(raw)
            except Exception:  # noqa: BLE001
                logger.warning("Skipping unreadable probe state %s", key)
        self._items = loaded

    def get(self, workspace_id: str, dataset_id: str, probe_name: str) -> ProbeState | None:
        # Guarded: a missing baseline reads as "first sighting", which silently
        # suppresses the staleness finding this store exists to produce.
        self._ensure_loaded()
        return super().get(workspace_id, dataset_id, probe_name)

    def all_states(self) -> list[ProbeState]:
        self._ensure_loaded()
        return super().all_states()

    def _persist(self, key: str, state: ProbeState) -> None:
        payload = json.dumps(state.as_dict())
        promoted = (
            state.probe_name,
            state.report_name,
            state.last_max_date,
            int(state.last_row_count or 0),
            int(state.suspect_count),
            payload,
        )
        try:
            updated = self._db.execute(
                f"UPDATE {self._table} SET probe_name = ?, report_name = ?, "
                f"last_max_date = ?, last_row_count = ?, suspect_count = ?, "
                f"payload = ? WHERE probe_key = ?",
                *promoted,
                key,
            )
            if not updated:
                try:
                    self._db.execute(
                        f"INSERT INTO {self._table} (probe_key, probe_name, "
                        f"report_name, last_max_date, last_row_count, "
                        f"suspect_count, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        key,
                        *promoted,
                    )
                except self._db.integrity_error():
                    self._db.execute(
                        f"UPDATE {self._table} SET probe_name = ?, report_name = ?, "
                        f"last_max_date = ?, last_row_count = ?, suspect_count = ?, "
                        f"payload = ? WHERE probe_key = ?",
                        *promoted,
                        key,
                    )
        except Exception as exc:  # noqa: BLE001
            self._loaded = False
            logger.error(
                "Could not persist probe baseline %s (%s); the next sweep will be blind",
                state.probe_name,
                type(exc).__name__,
            )

    def try_acquire_lease(self, name: str, owner: str, ttl_seconds: int) -> bool:
        """Claim the sweep across instances, using the database as the arbiter.

        A hosted agent is rebuilt per request and a schedule can wake more than
        one instance, so an in-process lock decides nothing. Two sweeps running
        together would each increment ``suspect_count`` for the same probe and
        confirm a finding on its first real occurrence -- turning the
        suspect-then-confirm rule, which exists to stop false positives, into a
        generator of them.

        Insert-if-absent is the atomic primitive: whoever creates the row wins.
        An expired row, or one this same owner already holds, is taken over by a
        conditional UPDATE whose WHERE clause is evaluated on the server, so two
        instances racing on an expired lease cannot both get ``rowcount`` 1.
        """
        try:
            self._db.execute(
                f"INSERT INTO {self._lease_table} (lease_name, owner, expires_at) "
                f"VALUES (?, ?, DATEADD(second, ?, SYSUTCDATETIME()))",
                name,
                owner,
                int(ttl_seconds),
            )
            return True
        except Exception as exc:  # noqa: BLE001
            try:
                duplicate = isinstance(exc, self._db.integrity_error())
            except Exception:  # pragma: no cover - driver missing
                duplicate = False
            if not duplicate:
                # Cannot arbitrate, so do not sweep. Declining is safe;
                # proceeding risks the double confirmation this prevents.
                logger.warning(
                    "Could not take sweep lease (%s); skipping", type(exc).__name__
                )
                return False

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
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not renew sweep lease (%s); skipping", type(exc).__name__)
            return False
        return bool(won)

    def release_lease(self, name: str, owner: str) -> None:
        try:
            self._db.execute(
                f"DELETE FROM {self._lease_table} WHERE lease_name = ? AND owner = ?",
                name,
                owner,
            )
        except Exception:  # noqa: BLE001 - the TTL releases it anyway
            logger.debug("Could not release sweep lease %s", name)

    def _on_reset(self) -> None:
        try:
            self._db.execute(f"DELETE FROM {self._table}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not clear %s (%s)", self._table_name, type(exc).__name__)
