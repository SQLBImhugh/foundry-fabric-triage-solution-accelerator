"""Durable incident store backed by Azure Table Storage.

Why this exists
---------------
The JSON-file store is correct and is still the right choice for a laptop
rehearsal. It stops being correct the moment the controller runs in a
container: the filesystem goes away when the container is recycled, and with it
every open incident. That matters more than it sounds. Incident state is what
makes the agent *idempotent* -- it is how a second alert about a problem
already being worked is recognised as a duplicate instead of triggering a
second remediation. Losing it does not merely lose history; it turns a restart
into a licence to act twice.

Design notes
------------
Incidents are stored as one entity per incident, with the full model kept as a
JSON payload and a few fields promoted to real columns so an operator can query
the table without deserialising anything.

Partition key is the incident *signature*, which is also the key the dedup
lookup uses. That keeps related recurrences physically together and leaves room
to replace the load-everything-at-startup approach with a point query if this
ever outgrows a demo.

Redaction is inherited, not reimplemented. ``record()`` on the base class
redacts before calling ``_persist``, so this subclass never sees raw text. That
is deliberate: redaction lives at the store boundary precisely so that a new
backend cannot forget to apply it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from triage.store.incidents import Incident, InMemoryIncidentStore, _utcnow
from triage.store.table_helpers import build_table_client

logger = logging.getLogger("triage.store.table")

# Table Storage rejects these in key fields. Signatures are hex digests today,
# so this is belt-and-braces against a future signature scheme that is not.
_ILLEGAL_KEY_CHARS = str.maketrans({c: "-" for c in "/\\#?\t\n\r"})


def _safe_key(value: str) -> str:
    return (value or "none").translate(_ILLEGAL_KEY_CHARS)[:512]


class AzureTableIncidentStore(InMemoryIncidentStore):
    """Incidents that survive a container restart.

    Falls back to in-memory behaviour if the table cannot be reached at
    startup. A demo that cannot talk to storage should still run -- degraded
    and loudly logged -- rather than fail to start in front of an audience.
    """

    def __init__(
        self,
        *,
        endpoint: str,
        table_name: str = "incidents",
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
            # Never let storage take the demo down. Record it plainly so the
            # operator knows incidents are not being persisted this run.
            self._degraded = True
            logger.error(
                "Incident store degraded to in-memory: could not open table %s at %s (%s)",
                table_name,
                endpoint,
                type(exc).__name__,
            )

    @property
    def is_durable(self) -> bool:
        return self._client is not None and not self._degraded

    def _build_client(self, credential: Any):
        # The controller authenticates as its own agent identity when it runs
        # in Foundry. DefaultAzureCredential is deliberately avoided elsewhere
        # in this codebase, but a *table* is not mail: the worst case of
        # falling back to a developer login here is writing demo incidents to a
        # demo table as yourself.
        return build_table_client(self._endpoint, self._table_name, credential)

    # --- durability hooks --------------------------------------------------

    def _load(self) -> None:
        if self._client is None:
            return
        loaded = 0
        for entity in self._client.list_entities():
            raw = entity.get("payload")
            if not raw:
                continue
            try:
                incident = Incident.model_validate(json.loads(raw))
            except Exception as exc:
                # One malformed row must not cost us the rest of the table.
                logger.warning(
                    "Skipping unreadable incident %s (%s)",
                    entity.get("RowKey", "?"),
                    type(exc).__name__,
                )
                continue
            self._items[incident.id] = incident
            loaded += 1
        logger.info("Loaded %d incident(s) from table %s", loaded, self._table_name)

    def _persist(self, incident: Incident) -> None:
        # Caller already holds the lock (contract of the base class).
        if self._client is None:
            return
        entity = {
            "PartitionKey": _safe_key(incident.signature),
            "RowKey": _safe_key(incident.id),
            # Promoted for operator queries; the payload remains authoritative.
            "status": incident.status,
            "signature": incident.signature,
            "updated_at": _utcnow(),
            "payload": incident.model_dump_json(),
        }
        try:
            self._client.upsert_entity(entity)
        except Exception as exc:
            # An incident that cannot be written is a real problem, but the
            # in-memory copy is already correct, so the run continues.
            self._degraded = True
            logger.error(
                "Could not persist incident %s (%s)", incident.id, type(exc).__name__
            )

    def _on_reset(self) -> None:
        if self._client is None:
            return
        for entity in list(self._client.list_entities()):
            try:
                self._client.delete_entity(
                    partition_key=entity["PartitionKey"], row_key=entity["RowKey"]
                )
            except Exception as exc:
                logger.warning("Could not delete incident row (%s)", type(exc).__name__)
