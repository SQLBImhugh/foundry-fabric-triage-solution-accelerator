from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from test_monitoring_sql_store import SqliteAzureDatabase
from test_monitoring_store import Clock

from triage.cli import _render_flags
from triage.models import DuplicateEvidence
from triage.runner import TriageRunner
from triage.settings import Settings
from triage.store.azure_sql import DEFAULT_TABLES, SqlUnavailable, schema_statements
from triage.store.incidents import InMemoryIncidentStore
from triage.tools.flags import AzureSqlFlagTable, DataQualityFlagTable, build_flag


def flag(*, request="request-one", detail="Two redundant rows.", rows=2):
    return build_flag(
        request_id=request,
        evidence=DuplicateEvidence(
            table="daily_sales", key_columns=["sale_id"], duplicate_group_count=1,
            duplicate_row_count=rows, total_row_count=10,
        ),
        detail=detail,
    )


@pytest.fixture
def database(tmp_path):
    db = SqliteAzureDatabase(tmp_path / "flags.sqlite", Clock())
    statements = [
        sql for sql in schema_statements(DEFAULT_TABLES)
        if "[triage_data_quality_flags]" in sql
    ]
    assert len(statements) == 1
    db.execute(statements[0])
    return db


def test_sql_flags_survive_a_new_instance_and_render_without_a_csv(database):
    first = AzureSqlFlagTable(database)
    written = first.append(flag())
    second = AzureSqlFlagTable(SqliteAzureDatabase(database.path, database.clock))
    assert second.row_count == 1
    rows = second.read_all()
    assert rows[0]["flag_id"] == written.flag_id
    assert rows[0]["duplicate_row_count"] == "2"
    assert "daily_sales" in _render_flags(SimpleNamespace(flag_table=second))
    assert not hasattr(second, "path")


def test_flag_identity_binds_request_and_deterministic_evidence():
    first = flag()
    assert first.flag_id == flag().flag_id
    assert first.flag_id != flag(request="another").flag_id
    assert first.flag_id != flag(rows=3).flag_id


@pytest.mark.parametrize("backend", ["csv", "sql"])
def test_redaction_is_inside_each_flag_store(database, tmp_path, backend):
    store = DataQualityFlagTable(tmp_path / "flags.csv") if backend == "csv" else AzureSqlFlagTable(database)
    original = flag(detail="Failure contained AKIAIOSFODNN7EXAMPLE")
    saved = store.append(original)
    assert "AKIAIOSFODNN7EXAMPLE" in original.detail
    assert "AKIAIOSFODNN7EXAMPLE" not in saved.detail
    assert "AKIAIOSFODNN7EXAMPLE" not in str(store.read_all())


def test_lost_insert_ack_preserves_original_identity_for_reconciliation(database):
    store = AzureSqlFlagTable(database)
    original = flag()
    database.fail_statement = lambda sql, _: sql.startswith("INSERT INTO")
    with pytest.raises(Exception, match="Injected SQL statement failure"):
        store.append(original)
    another = AzureSqlFlagTable(SqliteAzureDatabase(database.path, database.clock))
    result = another.append(original.model_copy(update={"flagged_at": "2099-01-01T00:00:00Z"}))
    assert result == original
    assert another.row_count == 1


def test_two_instances_cannot_duplicate_the_same_flag(database):
    original = flag()
    stores = [
        AzureSqlFlagTable(SqliteAzureDatabase(database.path, database.clock))
        for _ in range(2)
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda store: store.append(original), stores))
    assert results == [original, original]
    assert stores[0].row_count == 1


def test_reusing_a_flag_identity_cannot_replace_recorded_evidence(database):
    store = AzureSqlFlagTable(database)
    original = store.append(flag())
    with pytest.raises(SqlUnavailable, match="conflicts"):
        store.append(original.model_copy(update={"duplicate_row_count": 99}))
    assert store.read_all()[0]["duplicate_row_count"] == "2"


def test_sql_failure_has_no_local_fallback_and_recovery_reads_shared_state(database, monkeypatch):
    store = AzureSqlFlagTable(database)
    original = store.append(flag())
    native_query = database.query

    def down(*_args):
        raise SqlUnavailable("Synthetic SQL outage")

    monkeypatch.setattr(database, "query", down)
    with pytest.raises(SqlUnavailable, match="outage"):
        store.read_all()
    with pytest.raises(SqlUnavailable, match="outage"):
        _ = store.row_count
    monkeypatch.setattr(database, "query", native_query)
    assert store.read_all()[0]["flag_id"] == original.flag_id


def test_unconfirmed_rowcount_cannot_report_a_flag_written(database, monkeypatch):
    monkeypatch.setattr(database, "execute", lambda *_args: -1)
    with pytest.raises(SqlUnavailable, match="unconfirmed row count"):
        AzureSqlFlagTable(database).append(flag())


def test_live_flag_reset_is_not_a_runtime_operation(database):
    store = AzureSqlFlagTable(database)
    store.append(flag())
    with pytest.raises(ValueError, match="deployment reset"):
        store.reset()
    assert store.row_count == 1


def test_live_runner_selects_sql_flags_without_creating_a_file(database, tmp_path, monkeypatch):
    monkeypatch.setattr(TriageRunner, "_build_sql", lambda _: database)
    monkeypatch.setattr(TriageRunner, "_build_store", lambda _: InMemoryIncidentStore())
    monkeypatch.setattr(TriageRunner, "build_retry_store", lambda *_: object())
    monkeypatch.setattr(TriageRunner, "build_semantic_health_store", lambda *_: object())
    monkeypatch.setattr("triage.runner.inspect_context", lambda *_: object())
    runner = TriageRunner(
        Settings(_env_file=None, monitoring_mode="live"),
        base_dir=tmp_path, monitoring_store=object(),
    )
    assert isinstance(runner.flag_table, AzureSqlFlagTable)
    assert runner.flag_table.row_count == 0
    assert not (tmp_path / "runs" / "dq_flags.csv").exists()
    with pytest.raises(ValueError, match="only in fixture mode"):
        TriageRunner(
            Settings(_env_file=None, monitoring_mode="live"),
            base_dir=tmp_path, monitoring_store=object(), flag_table_path=tmp_path / "local.csv",
        )


def test_default_flag_setting_survives_an_empty_azd_substitution():
    assert Settings(_env_file=None, data_quality_flag_table_name="").data_quality_flag_table_name == DEFAULT_TABLES["data_quality_flags"]
