from __future__ import annotations

from typing import get_overloads, get_type_hints
from unittest.mock import create_autospec

import pytest

from triage.monitoring.contracts import (
    ControllerMonitoringStore,
    MonitoringComponentDenied,
    MonitoringReader,
    MonitoringStore,
    WebMonitoringStore,
    WorkerMonitoringStore,
)
from triage.monitoring.engine import (
    CONTROLLER_OPERATIONS,
    SHARED_WORK_OPERATIONS,
    WEB_OPERATIONS,
    WORKER_OPERATIONS,
)
from triage.monitoring.events import EventPersistence
from triage.monitoring.runtime import build_monitoring_store, fixture_component


def methods(interface):
    return {name for name in dir(interface) if not name.startswith("_") and callable(getattr(interface, name))}


@pytest.mark.parametrize(
    ("interface", "required", "forbidden"),
    [
        (WorkerMonitoringStore, "record_inventory", ("reserve_action", "activate_scope", "publish_connector")),
        (WebMonitoringStore, "activate_scope", ("record_inventory", "claim_work", "reserve_action")),
        (ControllerMonitoringStore, "reserve_action", ("record_inventory", "activate_scope", "record_stream_receipts")),
    ],
)
def test_role_fakes_expose_only_their_callers_operations(interface, required, forbidden):
    fake = create_autospec(interface, instance=True, spec_set=True)
    assert callable(getattr(fake, required))
    for name in forbidden:
        with pytest.raises(AttributeError):
            getattr(fake, name)


def test_role_mutation_interfaces_match_runtime_authority():
    all_writes = WEB_OPERATIONS | WORKER_OPERATIONS | CONTROLLER_OPERATIONS | SHARED_WORK_OPERATIONS
    assert not methods(MonitoringReader) & all_writes
    assert methods(WebMonitoringStore) & all_writes == WEB_OPERATIONS
    assert methods(ControllerMonitoringStore) & all_writes == CONTROLLER_OPERATIONS | SHARED_WORK_OPERATIONS
    worker = methods(WorkerMonitoringStore) | methods(EventPersistence)
    assert worker & all_writes == WORKER_OPERATIONS | SHARED_WORK_OPERATIONS


def test_aggregate_fixture_contract_retains_all_role_operations():
    assert methods(MonitoringStore) == (
        methods(WorkerMonitoringStore) | methods(WebMonitoringStore) | methods(ControllerMonitoringStore)
    )


@pytest.mark.parametrize("factory", [build_monitoring_store, fixture_component])
def test_factories_annotate_all_three_role_results(factory):
    results = {get_type_hints(overload)["return"] for overload in get_overloads(factory)}
    assert {WorkerMonitoringStore, WebMonitoringStore, ControllerMonitoringStore} <= results


def test_runtime_callers_use_the_narrow_contracts():
    from triage.command_center.monitoring import MonitoringService, resolve_command_target
    from triage.monitoring.polling import MonitoringCollector
    from triage.monitoring.provisioning import ConnectorReconciler, publish_connector_intent
    from triage.runner import TriageRunner

    assert get_type_hints(MonitoringService.__init__)["store"] is WebMonitoringStore
    assert get_type_hints(resolve_command_target)["store"] is MonitoringReader
    assert get_type_hints(MonitoringCollector.__init__)["store"] is WorkerMonitoringStore
    assert get_type_hints(ConnectorReconciler.__init__)["store"] is WorkerMonitoringStore
    assert get_type_hints(publish_connector_intent)["store"] is ControllerMonitoringStore
    assert get_type_hints(TriageRunner.monitoring.fget)["return"] is ControllerMonitoringStore


@pytest.mark.parametrize(
    ("role", "forbidden"),
    [("worker", "reserve_action"), ("web", "record_inventory"), ("controller", "activate_scope")],
)
def test_narrow_interfaces_do_not_replace_runtime_authority(test_settings, role, forbidden):
    store = build_monitoring_store(test_settings, fixture=True, component=role)
    with pytest.raises(MonitoringComponentDenied):
        getattr(store, forbidden)(None)
