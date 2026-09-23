from __future__ import annotations

import inspect
import subprocess
import sys

import pytest
from monitoring_supersession_protocol import SupersessionProtocolDatabase
from test_monitoring_connector_retirement_store import scoped_connector
from test_monitoring_connector_supersession_store import recovery_case

from triage.monitoring import models as m
from triage.monitoring.adapters import MonitoringAdapter
from triage.monitoring.controller import reconcile_monitoring_work
from triage.monitoring.engine import MonitoringEngine
from triage.monitoring.memory import InMemoryMonitoringStore, MemoryMonitoringAdapter
from triage.monitoring.records import stable_id
from triage.monitoring.sql_store import AzureSqlMonitoringStore, SqlMonitoringAdapter


def test_sql_and_memory_select_explicit_semantics_without_overriding_shared_rules():
    for store in (AzureSqlMonitoringStore, InMemoryMonitoringStore):
        overrides = {
            name for name, value in vars(store).items()
            if name != "__init__" and callable(value) and name in vars(MonitoringEngine)
        }
        assert not overrides, f"{store.__name__} overrides shared rules: {sorted(overrides)}"
    assert not issubclass(SqlMonitoringAdapter, MemoryMonitoringAdapter)


@pytest.mark.parametrize("adapter_type", [MemoryMonitoringAdapter, SqlMonitoringAdapter])
def test_each_adapter_implements_every_semantic_operation(adapter_type):
    assert not inspect.isabstract(adapter_type)
    assert MonitoringAdapter.__abstractmethods__
    assert MonitoringAdapter.__abstractmethods__ <= vars(adapter_type).keys()


@pytest.mark.parametrize("adapter_type", [MemoryMonitoringAdapter, SqlMonitoringAdapter])
def test_missing_observation_semantics_cannot_fall_back_to_the_other_adapter(adapter_type):
    implementation = {
        name: value for name, value in vars(adapter_type).items()
        if name not in {"__dict__", "__weakref__", "__abstractmethods__", "_abc_impl",
                        "connector_observation_overtaken"}
    }
    incomplete = type("IncompleteAdapter", (MonitoringAdapter,), implementation)
    with pytest.raises(TypeError, match="connector_observation_overtaken"):
        incomplete(None)


def test_sql_import_does_not_load_the_offline_adapter():
    script = """
import importlib.abc
import sys

class RefuseFixtureImport(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "triage.monitoring.memory":
            raise AssertionError("SQL imported the offline adapter")

sys.meta_path.insert(0, RefuseFixtureImport())
from triage.monitoring.sql_store import AzureSqlMonitoringStore
assert AzureSqlMonitoringStore.__name__ == "AzureSqlMonitoringStore"
assert "triage.monitoring.memory" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_persisted_later_observation_cannot_publish_old_authority(backend):
    existing = scoped_connector(backend, database_type=SupersessionProtocolDatabase)
    h, db, _, controller, worker, _ = existing
    _, _, _, owned, pending, collection, observed, request = recovery_case(
        backend, existing=existing, finish_inspection=False,
    )
    work = controller.get_work(h.version, request.work_id)
    h.clock.advance(1)
    if db is not None:
        db.principal = "worker"
    newer = m.OwnedConnectorManifest.model_validate({
        **observed.model_dump(), "revision": observed.revision + 1, "updated_at": h.clock(),
    })
    inspection = m.ConnectorPresenceInspection(
        read_only=True, observed_at=h.clock(),
        definition_hash=m.connector_definition_hash(newer.observed_definition),
        component_states={
            physical_id: "Running"
            for physical_id in newer.observed_definition["component_ids"].values()
        },
    )
    worker.record_connector(
        h.version, newer, expected_connector_revision=observed.revision,
        commit=m.CollectionCommit(
            work_id=collection.work_id, lease=collection.lease,
            expected_work_revision=collection.revision,
        ),
        inspection=inspection,
    )
    worker.complete_collection_work(
        h.version, work_id=collection.work_id, lease=collection.lease,
        expected_work_revision=collection.revision,
    )
    if db is not None:
        db.principal = "controller"

    result = reconcile_monitoring_work(controller, work)

    # The native frontier may still need predecessor resolution. Neither that
    # pending decision nor an ordinary rejection authorizes old evidence.
    assert result.state in {"rejected", "pending_validation"}
    assert result.producer_request_id == request.observation_receipt_id
    saved_work = controller.get_work(h.version, work.work_id)
    assert saved_work.state == ("completed" if result.state == "rejected" else "waiting")
    current = next(
        entry for entry in controller.list_connectors(m.PageQuery(**h.context())).items
        if entry.connector_id == owned.connector_id
    )
    assert current.revision == newer.revision
    assert current.source_removals == pending.pending_removals
    assert current.sources == owned.sources
    assert not current.delivery_proof
    newer_id = stable_id(h.version, f"connector:{owned.connector_id}:{observed.revision}")
    newer_work = controller.get_connector_observation(h.version, newer_id)
    assert newer_work is not None
    assert controller.get_work(h.version, newer_work.reconcile_work_id) is not None
