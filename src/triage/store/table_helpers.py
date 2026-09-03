"""Azure Table plumbing shared by every durable store.

Five stores -- incidents, approvals, processed messages, deferred retries and
semantic health baselines -- each opened a table the same way and each emptied
one the same way. The domain behaviour around them is genuinely different and
deliberately stays in each store: dedup and occurrence counting, fail-closed
approval polling, idempotency, backoff exhaustion, and baselines that only
advance from healthy readings. A base class covering all of that would hide the
part a reader most needs to see.

What was worth extracting is the part with no domain meaning at all: build a
client, create the table if it is missing, delete every row. Written five times,
it is five places to fix a credential or API change and five chances to fix only
four of them.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("triage.store")


def build_table_client(endpoint: str, table_name: str, credential: Any = None) -> Any:
    """Open a table client, creating the table when it does not exist yet.

    Imports are local so the offline path never needs the Azure SDK installed:
    the whole test suite runs against the in-memory and JSON stores.
    """
    from azure.data.tables import TableServiceClient

    if credential is None:
        from azure.identity import DefaultAzureCredential

        credential = DefaultAzureCredential()

    service = TableServiceClient(endpoint=endpoint, credential=credential)
    service.create_table_if_not_exists(table_name)
    return service.get_table_client(table_name)


def delete_all_entities(client: Any, label: str) -> None:
    """Empty a table, one row at a time, without giving up on the first failure.

    Best effort by design. ``reset`` exists so a demo or a test starts clean; a
    row that refuses to delete should not stop the other rows going, and should
    not raise into a caller whose next line assumes an empty table.
    """
    if client is None:
        return
    for entity in client.list_entities():
        try:
            client.delete_entity(
                partition_key=entity["PartitionKey"], row_key=entity["RowKey"]
            )
        except Exception:  # pragma: no cover - best effort
            logger.warning("Could not delete %s row %s", label, entity.get("RowKey"))
