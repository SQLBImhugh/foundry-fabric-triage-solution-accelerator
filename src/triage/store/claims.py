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

``create_entity`` on Azure Tables fails with ``ResourceExistsError`` when the row
is already there. That is an atomic compare-and-set against shared state, which
is all a lease needs. No extra service, and it reuses the storage account the
incident store already requires.

Claims expire. A container that crashes mid-remediation must not hold a lock for
ever, so a claim older than its lease is stolen and the theft is logged.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from typing import Any, Protocol

from triage.store.table_helpers import build_table_client

logger = logging.getLogger("triage.store.claims")

#: Long enough for a slow triage run (the policy wall clock defaults to 300s),
#: short enough that a crashed container does not block the next sweep for long.
DEFAULT_LEASE_SECONDS = 600


def _row_key(key: str) -> str:
    """Hash the key: a message id can contain characters Table keys reject."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:48]


def _owner() -> str:
    """Who holds the claim. Only ever read by a human looking at the table."""
    host = os.environ.get("CONTAINER_APP_REPLICA_NAME") or os.environ.get("HOSTNAME") or "local"
    return f"{host}:{os.getpid()}"


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


class AzureTableClaimStore:
    """A lease held in a table row, taken with a conditional create.

    Unlike the incident and processed stores, this one does **not** degrade
    silently to in-memory. Those degrade because losing them makes the agent
    noisy; losing this one makes it act twice, and acting twice is the thing it
    exists to prevent. If the table cannot be reached, ``claim`` returns False
    and the work is skipped until storage comes back.
    """

    _PARTITION = "claim"

    def __init__(
        self,
        *,
        endpoint: str,
        table_name: str = "claims",
        credential: Any = None,
    ) -> None:
        self._table_name = table_name
        self._client = None
        try:
            self._client = build_table_client(endpoint, table_name, credential)
        except Exception as exc:
            logger.error(
                "Claim store unavailable (%s at %s, %s). Work that requires a "
                "claim will be skipped rather than risk being done twice.",
                table_name,
                endpoint,
                type(exc).__name__,
            )

    @property
    def is_durable(self) -> bool:
        return self._client is not None

    def claim(self, key: str, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> bool:
        if self._client is None:
            return False

        from azure.core.exceptions import ResourceExistsError

        row = _row_key(key)
        now = time.time()
        entity = {
            "PartitionKey": self._PARTITION,
            "RowKey": row,
            "owner": _owner(),
            "claimed_at": now,
            "expires_at": now + lease_seconds,
            # Kept for debugging: without it a row is an opaque hash.
            "claim_key": key[:512],
        }

        try:
            self._client.create_entity(entity)
            return True
        except ResourceExistsError:
            pass
        except Exception as exc:
            logger.error("Could not take claim %r (%s); skipping", key, type(exc).__name__)
            return False

        # Someone holds it. Take it over only if their lease has expired, and
        # use the ETag so two callers racing to steal the same dead claim cannot
        # both win.
        try:
            existing = self._client.get_entity(self._PARTITION, row)
            expires_at = float(existing.get("expires_at", 0) or 0)
            if expires_at > now:
                return False

            from azure.core import MatchConditions

            logger.warning(
                "Stealing expired claim %r from %s (expired %.0fs ago)",
                key,
                existing.get("owner", "unknown"),
                now - expires_at,
            )
            self._client.update_entity(
                entity,
                mode="replace",
                etag=existing.metadata["etag"],
                match_condition=MatchConditions.IfNotModified,
            )
            return True
        except Exception as exc:
            # Includes the lost race: the other caller updated first, its ETag
            # no longer matches, and this one correctly does not get the claim.
            logger.info("Did not take claim %r (%s)", key, type(exc).__name__)
            return False

    def release(self, key: str) -> None:
        """Give the claim back early.

        Not required for correctness -- leases expire -- but releasing after a
        run means a retry of the same message does not wait ten minutes.
        """
        if self._client is None:
            return
        try:
            self._client.delete_entity(self._PARTITION, _row_key(key))
        except Exception as exc:
            logger.debug("Could not release claim %r (%s)", key, type(exc).__name__)


def build_claim_store(
    *, endpoint: str, table_name: str = "claims", credential: Any = None
) -> ClaimStore:
    """Durable when storage is configured, in-process when it is not."""
    if not endpoint:
        return InMemoryClaimStore()
    return AzureTableClaimStore(
        endpoint=endpoint, table_name=table_name, credential=credential
    )
