"""A distributed claim, so two invocations cannot do the same work twice.

The controller deduplicates in two places, and both check *before* the work and
record *after* it:

* a message is marked processed only once its outcome is persisted
* an incident signature is looked up before acting and written afterwards

Both windows are correct for one process and wrong for two. A hosted agent can
be invoked manually while a schedule fires, or run as more than one instance,
and then both see "not processed, no open incident", and both dispatch the
remediation. The write-action budget does not help: it is per run, and these are
two runs.

The lock that existed was ``asyncio.Lock`` on the agent instance, which is
process-local -- and a hosted agent is constructed fresh per request, so it did
not even span two requests to the same container.

This is the missing primitive: a claim that exactly one caller can hold.

``INSERT`` on a primary key fails with ``IntegrityError`` when the row is
already there. That is an atomic compare-and-set against shared state, which is
all a lease needs. No extra service, and it reuses the Fabric SQL database the
incident store already requires.

Claims expire. A container that crashes mid-remediation must not hold a lock for
ever, so a claim older than its lease is stolen and the theft is logged.
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
import threading
import time
from typing import Any, Protocol

logger = logging.getLogger("triage.store.claims")

#: Long enough for a slow triage run (the policy wall clock defaults to 300s),
#: short enough that a crashed container does not block the next sweep for long.
DEFAULT_LEASE_SECONDS = 600


def _row_key(key: str) -> str:
    """Hash the key so the primary key is a fixed, bounded width.

    A message id has no length limit worth relying on, and a primary key column
    does. Hashing also keeps the key printable, which matters because the
    readable original is stored alongside it in ``claim_text``.
    """
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:48]


def _owner() -> str:
    """Who holds this particular claim.

    A short random token is appended to the host and pid because those two are
    not unique enough: a hosted agent is constructed fresh per request inside
    one container, so two overlapping invocations share both. Without the
    token, the second invocation's release would match the first invocation's
    row and hand the claim back while work was still running.
    """
    host = os.environ.get("CONTAINER_APP_REPLICA_NAME") or os.environ.get("HOSTNAME") or "local"
    return f"{host}:{os.getpid()}:{secrets.token_hex(4)}"


class ClaimStore(Protocol):
    def claim(self, key: str, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> bool: ...
    def release(self, key: str) -> None: ...


class InMemoryClaimStore:
    """Correct within one process, and useless across two.

    This is the right implementation offline, where there is only ever one
    process, and it is what the test suite uses.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._held: dict[str, float] = {}

    @property
    def is_durable(self) -> bool:
        return False

    def claim(self, key: str, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> bool:
        now = time.time()
        with self._lock:
            expires = self._held.get(key)
            if expires is not None and expires > now:
                return False
            self._held[key] = now + lease_seconds
            return True

    def release(self, key: str) -> None:
        with self._lock:
            self._held.pop(key, None)


class FabricSqlClaimStore:
    """A lease held in a row, taken with a primary-key insert.

    Unlike the incident and processed stores, this one does **not** degrade
    silently to in-memory. Those degrade because losing them makes the agent
    noisy; losing this one makes it act twice, and acting twice is the thing it
    exists to prevent. If the database cannot be reached, ``claim`` returns
    False and the work is skipped until it comes back.

    Two SQL properties do the whole job, and both are single statements:

    * ``INSERT`` on a primary key raises ``IntegrityError`` when someone
      already holds the claim. That is an atomic compare-and-set.
    * ``UPDATE ... WHERE expires_at < SYSUTCDATETIME()`` steals an expired
      claim and reports through ``rowcount`` whether this caller won. Two
      racers cannot both get 1.

    The Azure Table version needed a read, an ETag and a conditional replace to
    express the second one. This is the same guarantee in one round trip, and
    the expiry comparison happens on the server, so it does not depend on the
    caller's clock being right.
    """

    def __init__(self, *, db: Any, table: str = "triage_claims") -> None:
        from triage.store.fabric_sql import quote_identifier

        self._db = db
        self._table = quote_identifier(table)
        self._table_name = table
        #: Claims this instance actually holds, so release can prove ownership
        #: rather than deleting whatever row happens to have the key.
        self._held: dict[str, str] = {}

    @property
    def is_durable(self) -> bool:
        return self._db.is_available

    def claim(self, key: str, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> bool:
        row = _row_key(key)
        owner = _owner()

        try:
            self._db.execute(
                f"INSERT INTO {self._table} "
                f"(claim_key, owner, claimed_at, expires_at, claim_text) "
                f"VALUES (?, ?, SYSUTCDATETIME(), "
                f"DATEADD(second, ?, SYSUTCDATETIME()), ?)",
                row,
                owner,
                int(lease_seconds),
                key[:512],
            )
            self._held[row] = owner
            return True
        except Exception as exc:  # noqa: BLE001
            if not _is_duplicate_key(self._db, exc):
                logger.error(
                    "Could not take claim %r (%s); skipping rather than risk "
                    "doing the work twice",
                    key,
                    type(exc).__name__,
                )
                return False

        # Somebody holds it. Take it over only if their lease has expired. The
        # WHERE clause is the arbiter, so a loser gets rowcount 0 rather than
        # silently overwriting the winner.
        try:
            won = self._db.execute(
                f"UPDATE {self._table} "
                f"   SET owner = ?, claimed_at = SYSUTCDATETIME(), "
                f"       expires_at = DATEADD(second, ?, SYSUTCDATETIME()) "
                f" WHERE claim_key = ? AND expires_at < SYSUTCDATETIME()",
                owner,
                int(lease_seconds),
                row,
            )
        except Exception as exc:  # noqa: BLE001
            logger.info("Did not take claim %r (%s)", key, type(exc).__name__)
            return False

        if won:
            logger.warning("Stole expired claim %r for %s", key, owner)
            self._held[row] = owner
            return True
        return False

    def release(self, key: str) -> None:
        """Give the claim back early, and only if this caller still holds it.

        Not required for correctness -- leases expire -- but releasing after a
        run means a retry of the same message does not wait ten minutes.

        The owner check is load-bearing rather than tidy. A caller whose lease
        expired mid-run, and whose claim was therefore stolen by somebody else,
        would otherwise delete the *new* holder's live claim on its way out and
        let a third caller straight in. Deleting by key alone quietly converts a
        slow run into duplicate work.
        """
        row = _row_key(key)
        owner = self._held.pop(row, None)
        if owner is None:
            # Never held it in this process, so there is nothing to give back.
            return
        try:
            self._db.execute(
                f"DELETE FROM {self._table} WHERE claim_key = ? AND owner = ?",
                row,
                owner,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not release claim %r (%s)", key, type(exc).__name__)


def _is_duplicate_key(db: Any, exc: Exception) -> bool:
    """True when the driver rejected an insert for violating the primary key."""
    try:
        return isinstance(exc, db.integrity_error())
    except Exception:  # pragma: no cover - driver missing entirely
        return False


def build_claim_store(*, db: Any = None, table: str = "triage_claims") -> ClaimStore:
    """Durable when a Fabric SQL database is configured, in-process when not."""
    if db is None:
        return InMemoryClaimStore()
    return FabricSqlClaimStore(db=db, table=table)
