"""The incident store, in Fabric SQL.

Replaces the Azure Table implementation. The domain behaviour -- dedup,
occurrence counting, redaction, status transitions -- lives in
``InMemoryIncidentStore`` and is untouched; this class only decides where the
rows go.

The one behavioural difference is deliberate and is a bug fix. The Table
version opened its client once in ``__init__`` and, on failure, degraded to
in-memory for the life of the process. Because a hosted agent is a long-lived
container, a database that was briefly unreachable at startup meant every
incident for the rest of that container's life was written nowhere, while the
agent went on reporting terminal outcomes normally. This version re-checks on
every read and write, and reloads once the database comes back -- reloading
matters more than reconnecting, because ``find_open`` is what stops the agent
remediating the same failure twice, and an empty cache answers "no open
incident" to everything.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from triage.models import Incident
from triage.store.fabric_sql import FabricSqlDatabase, quote_identifier
from triage.store.incidents import InMemoryIncidentStore, _utcnow

logger = logging.getLogger("triage.store.sql.incidents")


class FabricSqlIncidentStore(InMemoryIncidentStore):
    """Incidents that survive a container restart.

    Degrades to in-memory rather than refusing to start -- an accelerator that
    cannot reach its database should still triage, loudly degraded, rather than
    fail to start in front of an audience -- but unlike the previous
    implementation it keeps trying, so the degradation lasts as long as the
    outage rather than as long as the process.
    """

    def __init__(self, *, db: FabricSqlDatabase, table: str = "triage_incidents") -> None:
        super().__init__()
        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        self._loaded = False
        #: Incidents whose in-memory copy is ahead of the database because a
        #: write failed. They must survive a reload, or recovery would throw
        #: away precisely the changes that could not be saved.
        self._dirty: set[str] = set()
        self._ensure_loaded()

    @property
    def is_durable(self) -> bool:
        return self._loaded and self._db.is_available

    # --- recovery ----------------------------------------------------------

    def _ensure_loaded(self) -> bool:
        """Load the table into memory, retrying while the database is down.

        Returns True when the in-memory view is backed by a successful read.
        ``_lock`` is reentrant, so calling this from methods that already hold
        it is safe.
        """
        with self._lock:
            if self._loaded:
                return True
            try:
                # The schema may not exist yet: if the database was unreachable
                # when the runner started, nothing has created the tables, and
                # without this every recovery attempt would select from a table
                # that is never going to appear.
                self._db.ensure_schema_once()
                self._load()
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Incident store degraded to in-memory: cannot read %s (%s). "
                    "Incidents are not being persisted, and duplicate alerts "
                    "will not be recognised until this recovers.",
                    self._table_name,
                    type(exc).__name__,
                )
                return False
            self._loaded = True
            logger.info("Loaded %d incident(s) from %s", len(self._items), self._table_name)
            return True

    def _load(self) -> None:
        rows = self._db.query(f"SELECT payload FROM {self._table}")
        loaded: dict[str, Incident] = {}
        for (raw,) in rows:
            if not raw:
                continue
            try:
                incident = Incident.model_validate(json.loads(raw))
            except Exception as exc:  # noqa: BLE001
                # One malformed row must not cost us the rest of the table.
                logger.warning("Skipping unreadable incident row (%s)", type(exc).__name__)
                continue
            loaded[incident.id] = incident

        # Anything the database has not got, or has an out-of-date copy of,
        # exists only here. Replacing the dictionary outright would drop those
        # terminal outcomes at the exact moment recovery made it possible to
        # save them -- turning "degraded, and saying so" into permanent data
        # loss, which is the bug this store was rewritten to fix.
        #
        # Membership of `loaded` is not the test. An incident that already
        # existed before the outage *is* in `loaded`, so an earlier version of
        # this method kept the stale database row and silently discarded the
        # local update -- and since a failed write is what sets `_loaded` to
        # False, the reload threw away the very change that had just failed to
        # persist. A regressed occurrence_count or notified_count then licenses
        # a second announcement, or a second remediation.
        #
        # So the test is `_dirty`: rows with a pending local change win, and
        # the database wins for everything else.
        pending = {
            iid: inc
            for iid, inc in self._items.items()
            if iid not in loaded or iid in self._dirty
        }
        self._items = loaded
        for iid, incident in pending.items():
            logger.warning(
                "Flushing incident %s that was recorded or modified while the "
                "database was unreachable",
                iid,
            )
            self._items[iid] = incident
            self._upsert(incident)
            # Only clear the flag once the write has actually gone through;
            # _upsert raises on failure, so reaching here means it did.
            self._dirty.discard(iid)

    # --- reads, guarded so a recovered database is picked up ---------------

    def find_open(self, signature: str) -> Incident | None:
        """Read through to the database for this signature, not just the cache.

        The cache is loaded once and then only refreshed when a write fails, so
        an incident opened by *another* instance was invisible here — and this
        call is the one that decides whether the agent may remediate. A stale
        "no open incident" is how the same failure gets remediated twice, which
        is the outcome the whole store exists to prevent.

        `triage_incidents` carries an index on (signature, status) for exactly
        this query; until now nothing issued it. Scoping the refresh to one
        signature keeps it cheap enough to run on every check, which a full
        reload would not be.
        """
        with self._lock:
            if self._ensure_loaded():
                self._refresh_signature(signature)
            return super().find_open(signature)

    def _refresh_signature(self, signature: str) -> None:
        """Make the database authoritative for one signature. Caller holds the lock."""
        try:
            rows = self._db.query(
                f"SELECT payload FROM {self._table} WHERE signature = ?",
                signature[:200],
            )
        except Exception as exc:  # noqa: BLE001
            # Fall back to the cached view rather than failing the lookup. A
            # refusal here would stop triage entirely; a stale read only risks
            # the duplicate this method is trying to avoid, and the claim taken
            # before remediation is the second line of defence.
            self._loaded = False
            logger.warning(
                "Could not re-read signature from %s (%s); answering from cache",
                self._table_name,
                type(exc).__name__,
            )
            return

        fresh: dict[str, Incident] = {}
        for (raw,) in rows:
            if not raw:
                continue
            try:
                incident = Incident.model_validate(json.loads(raw))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping unreadable incident row (%s)", type(exc).__name__)
                continue
            fresh[incident.id] = incident

        # Drop cached incidents for this signature that the database no longer
        # has, so a locally-resolved-then-deleted row cannot linger as open.
        # Dirty rows are exempt: their local copy is the newer one.
        stale = [
            iid
            for iid, inc in self._items.items()
            if inc.signature == signature and iid not in fresh and iid not in self._dirty
        ]
        for iid in stale:
            self._items.pop(iid, None)

        for iid, incident in fresh.items():
            if iid in self._dirty:
                continue
            self._items[iid] = incident

    def list_all(self) -> list[Incident]:
        self._ensure_loaded()
        return super().list_all()

    def record(self, result: Any, **provenance: Any) -> Incident:
        self._ensure_loaded()
        return super().record(result, **provenance)

    # --- durability hooks --------------------------------------------------

    def _persist(self, incident: Incident) -> None:
        # Caller already holds the lock (contract of the base class).
        try:
            self._upsert(incident)
        except Exception as exc:  # noqa: BLE001
            # The in-memory copy is already correct, so the run continues -- but
            # this incident is not durable, and the next restart forgets it.
            # Marking it dirty is what stops the recovery reload overwriting it
            # with the stale row still sitting in the database.
            self._loaded = False
            self._dirty.add(incident.id)
            logger.error(
                "Could not persist incident %s (%s)", incident.id, type(exc).__name__
            )
        else:
            self._dirty.discard(incident.id)

    def _upsert(self, incident: Incident) -> None:
        """Update, and insert only if the row was not there.

        Deliberately two statements rather than MERGE. MERGE on SQL Server has
        a long history of concurrency defects, and the fallback here is honest:
        if another writer inserts the same id between the UPDATE and the
        INSERT, the primary key rejects the second one and we simply update
        instead. Losing that race is not an error, it is the constraint doing
        its job.
        """
        payload = incident.model_dump_json()
        args = (
            incident.signature[:200],
            incident.status,
            _utcnow(),
            payload,
            incident.id[:200],
        )
        updated = self._db.execute(
            f"UPDATE {self._table} SET signature = ?, status = ?, updated_at = ?, "
            f"payload = ? WHERE incident_id = ?",
            *args,
        )
        if updated:
            return
        try:
            self._db.execute(
                f"INSERT INTO {self._table} "
                f"(incident_id, signature, status, updated_at, payload) "
                f"VALUES (?, ?, ?, ?, ?)",
                incident.id[:200],
                incident.signature[:200],
                incident.status,
                _utcnow(),
                payload,
            )
        except self._db.integrity_error():
            self._db.execute(
                f"UPDATE {self._table} SET signature = ?, status = ?, updated_at = ?, "
                f"payload = ? WHERE incident_id = ?",
                *args,
            )

    def _on_reset(self) -> None:
        try:
            self._db.execute(f"DELETE FROM {self._table}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not clear %s (%s)", self._table_name, type(exc).__name__)
