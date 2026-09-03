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

from triage.store.table_helpers import build_table_client

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


class AzureTableSemanticHealthStore(InMemorySemanticHealthStore):
    """The deployed path. Degrades to in-memory, loudly.

    Degraded, every sweep starts with no history and can never detect a
    watermark that failed to advance. The detector would run, find nothing,
    and report health it has not established -- so this logs an error rather
    than a warning.
    """

    _PARTITION = "probe"
    _LEASE_PARTITION = "lease"

    def __init__(
        self,
        *,
        endpoint: str,
        table_name: str = "semantichealth",
        credential: Any = None,
    ) -> None:
        super().__init__()
        self._endpoint = endpoint
        self._table_name = table_name
        self._client = None
        self._degraded = False

        try:
            self._client = self._build_client(credential)
            self._load()
        except Exception as exc:
            self._degraded = True
            logger.error(
                "Semantic health store degraded to in-memory: could not open %s at %s "
                "(%s). Every sweep will start blind and cannot detect staleness.",
                table_name,
                endpoint,
                type(exc).__name__,
            )

    @property
    def is_durable(self) -> bool:
        return self._client is not None and not self._degraded

    def _build_client(self, credential: Any):
        return build_table_client(self._endpoint, self._table_name, credential)

    def _load(self) -> None:
        if self._client is None:
            return
        loaded = 0
        for entity in self._client.list_entities():
            raw = entity.get("payload")
            if not raw:
                continue
            try:
                self._items[str(entity["RowKey"])] = json.loads(raw)
                loaded += 1
            except Exception:
                logger.warning("Skipping unreadable probe state %s", entity.get("RowKey"))
        logger.info("Loaded %d probe baseline(s) from %s", loaded, self._table_name)

    def _persist(self, key: str, state: ProbeState) -> None:
        if self._client is None:
            return
        try:
            self._client.upsert_entity(
                {
                    "PartitionKey": self._PARTITION,
                    "RowKey": key,
                    # Promoted for operator queries; payload stays authoritative.
                    "probe_name": state.probe_name,
                    "report_name": state.report_name,
                    "last_max_date": state.last_max_date,
                    "last_row_count": state.last_row_count or 0,
                    "suspect_count": state.suspect_count,
                    "payload": json.dumps(state.as_dict()),
                }
            )
        except Exception as exc:
            self._degraded = True
            logger.error(
                "Could not persist probe baseline %s (%s); the next sweep will be blind",
                state.probe_name,
                type(exc).__name__,
            )

    def try_acquire_lease(self, name: str, owner: str, ttl_seconds: int) -> bool:
        """Claim the sweep across instances, using the table as the arbiter.

        A hosted agent is rebuilt per request and a schedule can wake more than
        one instance, so an in-process lock decides nothing. Two sweeps running
        together would each increment ``suspect_count`` for the same probe and
        confirm a finding on its first real occurrence -- turning the
        suspect-then-confirm rule, which exists to stop false positives, into a
        generator of them.

        Insert-if-absent is the atomic primitive: whoever creates the row wins.
        An expired row is taken over with an ETag match, so a late loser cannot
        overwrite the winner.
        """
        if self._client is None:
            return super().try_acquire_lease(name, owner, ttl_seconds)

        from azure.core import MatchConditions
        from azure.core.exceptions import (
            ResourceExistsError,
            ResourceModifiedError,
            ResourceNotFoundError,
        )

        row = {
            "PartitionKey": self._LEASE_PARTITION,
            "RowKey": name,
            "owner": owner,
            "expires_at": time.time() + ttl_seconds,
        }
        try:
            self._client.create_entity(row)
            return True
        except ResourceExistsError:
            pass
        except Exception as exc:  # pragma: no cover - transient table failure
            # Cannot arbitrate, so do not sweep. Declining is safe; proceeding
            # risks the double confirmation this exists to prevent.
            logger.warning("Could not take sweep lease (%s); skipping", type(exc).__name__)
            return False

        try:
            held = self._client.get_entity(self._LEASE_PARTITION, name)
        except ResourceNotFoundError:  # pragma: no cover - released in between
            return False

        if str(held.get("owner")) != owner and float(held.get("expires_at", 0)) > time.time():
            return False

        try:
            from azure.data.tables import UpdateMode

            self._client.update_entity(
                row,
                mode=UpdateMode.REPLACE,
                etag=held.metadata["etag"],
                match_condition=MatchConditions.IfNotModified,
            )
            return True
        except (ResourceModifiedError, KeyError):
            return False
        except Exception as exc:  # pragma: no cover - transient table failure
            logger.warning("Could not renew sweep lease (%s); skipping", type(exc).__name__)
            return False

    def release_lease(self, name: str, owner: str) -> None:
        if self._client is None:
            super().release_lease(name, owner)
            return
        try:
            held = self._client.get_entity(self._LEASE_PARTITION, name)
            if str(held.get("owner")) == owner:
                self._client.delete_entity(
                    partition_key=self._LEASE_PARTITION, row_key=name
                )
        except Exception:  # pragma: no cover - the TTL releases it anyway
            logger.debug("Could not release sweep lease %s", name)

    def _on_reset(self) -> None:
        if self._client is None:
            return
        for entity in self._client.list_entities():
            try:
                self._client.delete_entity(
                    partition_key=entity["PartitionKey"], row_key=entity["RowKey"]
                )
            except Exception:  # pragma: no cover - best effort
                logger.warning("Could not delete probe state %s", entity.get("RowKey"))
