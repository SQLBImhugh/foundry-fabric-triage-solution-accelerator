from __future__ import annotations

from types import SimpleNamespace

import pytest

from triage.store.azure_sql import AzureSqlDatabase, SqlUnavailable
from triage.store.durability import (
    StateConfigurationError,
    persistence_confirmed,
    require_shared_persistence,
    select_state_database,
)


@pytest.fixture(autouse=True)
def no_sql_connections(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("State-selection tests must not open a SQL connection")

    monkeypatch.setattr(AzureSqlDatabase, "_connect", forbidden)


def configuration(server="", database=""):
    return SimpleNamespace(azure_sql_server=server, azure_sql_database=database)


def test_fixture_selection_ignores_live_settings_without_constructing_sql():
    assert select_state_database(
        configuration("example.database.windows.net", "state"), fixture=True,
    ) is None


def test_fixture_selection_refuses_an_injected_live_handle():
    db = AzureSqlDatabase(server="example.database.windows.net", database="state")
    with pytest.raises(StateConfigurationError, match="fixture"):
        select_state_database(configuration(), fixture=True, db=db)


@pytest.mark.parametrize(("server", "database"), [("", ""), ("server", ""), ("", "state")])
def test_live_selection_requires_both_sql_settings(server, database):
    with pytest.raises(StateConfigurationError, match="AZURE_SQL_SERVER.*AZURE_SQL_DATABASE"):
        select_state_database(configuration(server, database), fixture=False)


def test_live_selection_keeps_the_one_injected_handle_without_probing_health():
    db = AzureSqlDatabase(server="example.database.windows.net", database="state")
    assert select_state_database(configuration(), fixture=False, db=db) is db


def test_live_selection_preserves_credentials_and_table_configuration():
    credential = object()
    tables = {"incidents": "custom_incidents"}
    db = select_state_database(
        configuration("example.database.windows.net", "state"),
        fixture=False, credential=credential, tables=tables,
    )
    assert db._credential is credential
    assert db._tables == tables
    assert db.target == "state on example.database.windows.net"


@pytest.mark.parametrize("store", [object(), SimpleNamespace(is_durable=False)])
def test_unknown_or_offline_persistence_is_not_confirmed(store):
    assert not persistence_confirmed(store)
    with pytest.raises(SqlUnavailable, match="shared persistence"):
        require_shared_persistence(store, operation="Live incident construction")


@pytest.mark.parametrize("value", ["true", "false", 1, None])
def test_malformed_health_does_not_become_durable_by_truthiness(value):
    with pytest.raises(SqlUnavailable, match="boolean"):
        persistence_confirmed(SimpleNamespace(is_durable=value))


def test_health_is_checked_again_after_startup_failure_and_recovery():
    class Store:
        ready = True
        checks = 0

        @property
        def is_durable(self):
            self.checks += 1
            return self.ready

    store = Store()
    require_shared_persistence(store, operation="Startup")
    store.ready = False
    with pytest.raises(SqlUnavailable, match="shared persistence"):
        require_shared_persistence(store, operation="Record verified outcome")
    store.ready = True
    require_shared_persistence(store, operation="Recovered authoritative read")
    assert store.checks == 3


def test_live_runner_rejects_injected_local_incidents_before_inspecting_sql(tmp_path):
    from triage.runner import TriageRunner
    from triage.settings import Settings
    from triage.store.incidents import InMemoryIncidentStore

    settings = Settings(
        _env_file=None, monitoring_mode="live", triage_tool_mode="mock",
        triage_provider_mode="mock",
        monitoring_tenant_id="11111111-1111-1111-1111-111111111111",
        azure_sql_server="example.database.windows.net", azure_sql_database="state",
        applicationinsights_connection_string="",
    )
    with pytest.raises(SqlUnavailable, match="Live incident store selection"):
        TriageRunner(settings, base_dir=tmp_path, store=InMemoryIncidentStore())
