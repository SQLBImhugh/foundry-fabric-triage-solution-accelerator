from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from triage.pipeline_models import PipelineRerunRecord, PipelineTarget, load_pipeline_targets
from triage.store.pipeline_reruns import (
    FabricSqlPipelineRerunStore,
    InMemoryPipelineRerunStore,
    JsonFilePipelineRerunStore,
)

WORKSPACE = "10000000-0000-0000-0000-000000000001"
PIPELINE = "20000000-0000-0000-0000-000000000002"
RUN = "30000000-0000-0000-0000-000000000003"


def _record(**updates) -> PipelineRerunRecord:
    return PipelineRerunRecord(
        workspace_id=WORKSPACE, pipeline_id=PIPELINE, failed_run_id=RUN,
        signature="failure-signature", parameter_hash="reviewed-parameters", **updates,
    )


class _Sql:
    """Execute the store's parameterized DML offline, including unique-key conflicts."""

    is_available = True

    def __init__(self, path) -> None:
        self.path = path
        self.down = False
        with sqlite3.connect(path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS triage_pipeline_reruns "
                "(run_key TEXT PRIMARY KEY, workspace_id TEXT, pipeline_id TEXT, "
                "state TEXT, payload TEXT)"
            )

    def execute(self, sql, *params):
        if self.down:
            raise ConnectionError("test database unavailable")
        with sqlite3.connect(self.path) as conn:
            return conn.execute(sql.replace("[dbo].", ""), params).rowcount

    def query(self, sql, *params):
        if self.down:
            raise ConnectionError("test database unavailable")
        with sqlite3.connect(self.path) as conn:
            return conn.execute(sql.replace("[dbo].", ""), params).fetchall()

    def integrity_error(self):
        return sqlite3.IntegrityError


@pytest.fixture(params=["memory", "file", "sql"])
def reruns(request, tmp_path):
    if request.param == "memory":
        return InMemoryPipelineRerunStore()
    if request.param == "file":
        return JsonFilePipelineRerunStore(tmp_path / "reruns.json")
    return FabricSqlPipelineRerunStore(db=_Sql(tmp_path / "reruns.db"))


def test_reserved_run_cannot_be_submitted_again(reruns) -> None:
    record = _record()
    assert reruns.reserve(record) is True
    assert reruns.reserve(record) is False
    assert reruns.get(record.key).state == "reserved"


def test_unknown_submission_keeps_its_fence(reruns) -> None:
    record = _record()
    assert reruns.reserve(record)
    assert reruns.update(record.model_copy(update={"state": "unknown"}), expected="reserved")
    assert not reruns.reserve(record)
    assert reruns.pending(WORKSPACE, PIPELINE) == []


def test_only_correlated_submitted_runs_are_polled(reruns) -> None:
    record = _record()
    reruns.reserve(record)
    submitted = record.model_copy(update={"state": "submitted", "rerun_id": RUN})
    assert reruns.update(submitted, expected="reserved")
    assert reruns.pending(WORKSPACE, PIPELINE) == [submitted]
    assert reruns.pending(WORKSPACE, WORKSPACE) == []
    assert reruns.update(submitted.model_copy(update={"state": "completed"}), expected="submitted")
    assert reruns.pending(WORKSPACE, PIPELINE) == []
    assert not reruns.reserve(record)


def test_stale_state_cannot_overwrite_a_later_transition(reruns) -> None:
    record = _record()
    reruns.reserve(record)
    assert reruns.update(record.model_copy(update={"state": "unknown"}), expected="reserved")
    assert not reruns.update(record.model_copy(update={"state": "submitted"}), expected="reserved")
    assert reruns.get(record.key).state == "unknown"


def test_rerun_details_are_redacted_at_the_store_boundary(reruns) -> None:
    record = _record(detail="Failure with AKIAIOSFODNN7EXAMPLE")
    reruns.reserve(record)
    assert "AKIAIOSFODNN7EXAMPLE" not in reruns.get(record.key).detail


def test_file_reservations_survive_a_restart(tmp_path) -> None:
    path = tmp_path / "reruns.json"
    first = JsonFilePipelineRerunStore(path)
    first.reserve(_record())
    assert not JsonFilePipelineRerunStore(path).reserve(_record())


def test_corrupt_file_is_not_treated_as_an_empty_journal(tmp_path) -> None:
    path = tmp_path / "reruns.json"
    path.write_text("not json", encoding="utf-8")
    with pytest.raises(ValueError):
        JsonFilePipelineRerunStore(path)


def test_sql_reservation_has_exactly_one_winner_across_store_instances(tmp_path) -> None:
    db = _Sql(tmp_path / "reruns.db")
    barrier = threading.Barrier(8)

    def reserve(_index):
        store = FabricSqlPipelineRerunStore(db=db)
        barrier.wait()
        return store.reserve(_record())

    with ThreadPoolExecutor(max_workers=8) as pool:
        winners = list(pool.map(reserve, range(8)))
    assert winners.count(True) == 1
    assert not FabricSqlPipelineRerunStore(db=db).reserve(_record())


def test_sql_outage_refuses_instead_of_creating_an_in_memory_reservation(tmp_path) -> None:
    db = _Sql(tmp_path / "reruns.db")
    store = FabricSqlPipelineRerunStore(db=db)
    db.down = True
    with pytest.raises(ConnectionError):
        store.reserve(_record())
    db.down = False
    assert store.reserve(_record())


def test_replay_requires_explicit_safety_and_parameter_review() -> None:
    target = PipelineTarget(name="Orders load", workspace_id=WORKSPACE, pipeline_id=PIPELINE)
    assert not target.permits_rerun
    assert not target.model_copy(update={"rerun_safe": True}).permits_rerun
    assert not target.model_copy(update={"rerun_parameters": {}}).permits_rerun
    assert target.model_copy(update={"rerun_safe": True, "rerun_parameters": {}}).permits_rerun


@pytest.mark.parametrize("raw", ["{}", "[null]", '[{"name":"x"}]', "[true]", "{"])
def test_invalid_target_configuration_fails_closed(raw) -> None:
    with pytest.raises(ValueError):
        load_pipeline_targets(raw)


def test_duplicate_target_configuration_is_rejected() -> None:
    target = {"name": "Orders", "workspace_id": WORKSPACE, "pipeline_id": PIPELINE}
    with pytest.raises(ValueError, match="Duplicate"):
        load_pipeline_targets(json.dumps([target, target]))


def test_no_targets_is_distinct_from_invalid_configuration() -> None:
    assert load_pipeline_targets("") == []
    assert load_pipeline_targets("[]") == []


def test_target_ids_cannot_supply_urls_or_paths() -> None:
    with pytest.raises(ValueError):
        PipelineTarget(name="Orders", workspace_id=WORKSPACE, pipeline_id="../another-item")


def test_unset_hosted_pipeline_values_use_defaults_without_enabling_monitoring() -> None:
    from triage.settings import Settings

    settings = Settings(
        pipeline_sweep_enabled="", pipeline_lookback_hours="",
        pipeline_max_pages="", pipeline_max_runs_per_sweep="",
        pipeline_rerun_table_name="",
    )
    assert settings.pipeline_sweep_enabled is False
    assert settings.pipeline_lookback_hours == 24
    assert settings.pipeline_max_pages == 10
    assert settings.pipeline_max_runs_per_sweep == 1
    assert settings.pipeline_rerun_table_name == "triage_pipeline_reruns"
