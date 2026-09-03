"""Offline tests for the durable incident store.

No Azure. A fake table client stands in for the service so the persistence
contract can be tested without a storage account, which keeps the default test
path offline.
"""

from __future__ import annotations

import json
from typing import Any

from triage.runner import TriageRunner
from triage.settings import Settings
from triage.store.azure_table import AzureTableIncidentStore, _safe_key
from triage.store.incidents import JsonFileIncidentStore


class FakeTableClient:
    """In-memory stand-in for azure.data.tables.TableClient."""

    def __init__(self, seed: list[dict[str, Any]] | None = None) -> None:
        self.rows: dict[tuple[str, str], dict[str, Any]] = {}
        for row in seed or []:
            self.rows[(row["PartitionKey"], row["RowKey"])] = row
        self.upserts = 0
        self.fail_on_upsert = False

    def list_entities(self):
        return list(self.rows.values())

    def upsert_entity(self, entity: dict[str, Any]) -> None:
        if self.fail_on_upsert:
            raise RuntimeError("storage unavailable")
        self.upserts += 1
        self.rows[(entity["PartitionKey"], entity["RowKey"])] = entity

    def delete_entity(self, partition_key: str, row_key: str) -> None:
        self.rows.pop((partition_key, row_key), None)


def _store(client: FakeTableClient) -> AzureTableIncidentStore:
    """Build the store with the client injected, bypassing Azure entirely."""
    store = AzureTableIncidentStore.__new__(AzureTableIncidentStore)
    # Re-run the in-memory half of construction without touching the network.
    from triage.store.incidents import InMemoryIncidentStore

    InMemoryIncidentStore.__init__(store)
    store._endpoint = "https://fake.table.core.windows.net/"
    store._table_name = "incidents"
    store._client = client
    store._degraded = False
    store._load()
    return store


def test_incident_round_trips_through_the_table(result_factory) -> None:
    client = FakeTableClient()
    store = _store(client)

    incident = store.record(result_factory(), report_name="Sales")
    assert client.upserts == 1

    row = next(iter(client.rows.values()))
    assert row["PartitionKey"] == _safe_key(incident.signature)
    assert row["RowKey"] == _safe_key(incident.id)
    # Promoted columns let an operator query the table without deserialising.
    assert row["status"] == incident.status
    assert json.loads(row["payload"])["id"] == incident.id


def test_a_restart_reloads_open_incidents(result_factory) -> None:
    """This is the property that makes the agent safe to restart.

    Without it, a recycled container forgets which failures are already being
    worked and is free to remediate the same problem a second time.
    """
    first = _store(FakeTableClient())
    incident = first.record(
        result_factory(outcome="needs_human", action_taken=""), report_name="Sales"
    )
    rows = list(first._client.rows.values())

    # A brand new process, same table.
    second = _store(FakeTableClient(seed=rows))
    assert second.find_open(incident.signature) is not None
    assert len(second.list_all()) == 1


def test_one_corrupt_row_does_not_lose_the_rest(result_factory) -> None:
    good = _store(FakeTableClient())
    incident = good.record(result_factory(), report_name="Sales")
    rows = list(good._client.rows.values())
    rows.append(
        {"PartitionKey": "junk", "RowKey": "junk", "payload": "{not valid json"}
    )

    reloaded = _store(FakeTableClient(seed=rows))
    assert len(reloaded.list_all()) == 1
    assert reloaded.list_all()[0].id == incident.id


def test_storage_failure_degrades_instead_of_killing_the_run(result_factory) -> None:
    """A demo should survive storage going away, loudly but alive."""
    client = FakeTableClient()
    store = _store(client)
    client.fail_on_upsert = True

    incident = store.record(
        result_factory(outcome="needs_human", action_taken=""), report_name="Sales"
    )

    assert not store.is_durable
    # The in-memory copy is still correct, so triage continues.
    assert store.find_open(incident.signature) is not None


def test_reset_clears_the_table(result_factory) -> None:
    client = FakeTableClient()
    store = _store(client)
    store.record(result_factory(), report_name="Sales")
    assert client.rows

    store.reset()
    assert client.rows == {}
    assert store.list_all() == []


def test_keys_survive_characters_table_storage_rejects() -> None:
    assert "/" not in _safe_key("a/b")
    assert "\\" not in _safe_key("a\\b")
    assert "#" not in _safe_key("a#b")
    assert _safe_key("") == "none"
    assert len(_safe_key("x" * 900)) <= 512


def test_offline_settings_never_select_the_table_store(test_settings, repo_root, tmp_path) -> None:
    """Guards the invariant that the default test path is offline.

    A populated .env on a developer machine would otherwise point the whole
    suite at a live storage account without anyone noticing.
    """
    assert test_settings.incident_table_endpoint == ""
    runner = TriageRunner(
        test_settings, base_dir=repo_root, flag_table_path=tmp_path / "flags.csv"
    )
    assert isinstance(runner.store, JsonFileIncidentStore)


def test_table_store_is_chosen_when_configured(repo_root, tmp_path, monkeypatch) -> None:
    """The other half: configuring an endpoint must actually switch backends."""
    built: dict[str, Any] = {}

    class _Stub(AzureTableIncidentStore):
        def __init__(self, *, endpoint: str, table_name: str = "incidents") -> None:
            built["endpoint"] = endpoint
            built["table"] = table_name
            from triage.store.incidents import InMemoryIncidentStore

            InMemoryIncidentStore.__init__(self)
            self._client = None
            self._degraded = False

    monkeypatch.setattr("triage.store.azure_table.AzureTableIncidentStore", _Stub)

    settings = Settings(
        triage_provider_mode="mock",
        triage_tool_mode="mock",
        incident_table_endpoint="https://example.table.core.windows.net/",
        incident_table_name="incidents",
    )
    runner = TriageRunner(
        settings, base_dir=repo_root, flag_table_path=tmp_path / "flags.csv"
    )
    assert built["endpoint"] == "https://example.table.core.windows.net/"
    assert isinstance(runner.store, _Stub)
