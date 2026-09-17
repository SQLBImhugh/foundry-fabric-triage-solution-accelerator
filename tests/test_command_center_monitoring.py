from __future__ import annotations

import asyncio
import threading
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from test_monitoring_api import (
    CAPABILITY,
    CLIENT,
    EPOCH,
    GENERATION,
    ITEM,
    NOW,
    OTHER,
    RULE,
    SCOPE,
    TENANT,
    USER,
    WORKSPACE,
    RecordingStore,
    nonready_inspection,
)
from test_monitoring_api import no_network as no_network
from test_monitoring_api import signing as signing

from triage.command_center.api import create_app
from triage.command_center.demo import DemoRunner, start_demo
from triage.command_center.models import Actor, ApiFailure, CommandInput, WebSettings
from triage.command_center.service import CommandCenterService
from triage.command_center.worker import drain_commands
from triage.models import BIRequest
from triage.monitoring.contracts import (
    MonitoringComponentDenied,
    MonitoringKernelUnsupported,
    MonitoringNotBootstrapped,
    MonitoringUnavailable,
)
from triage.monitoring.models import (
    DeploymentControl,
    MonitoringContext,
    MonitoringTarget,
    ObservationPolicy,
    RecordPage,
    RegistryVersion,
    TargetIdentity,
    TargetQuery,
)
from triage.monitoring.runtime import (
    FIXTURE_TENANT_ID,
    build_monitoring_store,
    fixture_approvals,
    fixture_target,
)
from triage.store.approvals import InMemoryApprovalChannel
from triage.store.claims import InMemoryClaimStore
from triage.store.command_center import InMemoryCommandCenterStore

READER = Actor(id=USER, display_name="Reader", roles=["reader"])
OPERATOR = Actor(id=USER, display_name="Operator", roles=["operator"])


class NoSql:
    def query(self, *_args, **_kwargs):
        raise AssertionError("The injected monitoring tests must not query SQL")

    def execute(self, *_args, **_kwargs):
        raise AssertionError("The web runtime must not issue SQL or DDL in this test")

    def ensure_schema_once(self):
        raise AssertionError("Runtime schema creation is forbidden")


def make_target(*, item_id=ITEM, workload="powerbi", state="current", observing=True):
    return MonitoringTarget(
        identity=TargetIdentity(
            tenant_id=TENANT, epoch=EPOCH, workload=workload,
            workspace_id=WORKSPACE, item_id=item_id,
        ),
        name="A display label, not an identity", scope_ids=(SCOPE,), admitted_rule_ids=(RULE,),
        inventory_generation=GENERATION, capability_id=CAPABILITY, policy_revision=7,
        admitted_at=NOW, state=state, admission_basis="reviewed",
        reason="Explicit test admission.", observation=ObservationPolicy(enabled=observing),
    )


@pytest.fixture
def registry():
    store = RecordingStore()
    store.current_targets = [make_target()]

    def resolve(identity: TargetIdentity, *, include_inactive=False):
        store._record("resolve_target", identity)
        store._context(identity)
        return next((
            target for target in store.current_targets
            if target.identity == identity and (target.state == "current" or include_inactive)
        ), None)

    def targets(query: TargetQuery):
        store._record("list_targets", query)
        store._context(query)
        return RecordPage[MonitoringTarget](
            version=store.version, as_of=NOW, items=tuple(store.current_targets),
        )

    store.port.resolve_target.side_effect = resolve
    store.port.list_targets.side_effect = targets
    return store


@pytest.fixture
def bootstrap_adapter(registry, monkeypatch):
    calls = []

    def adapter(*, db, component):
        assert component == "web"
        calls.append((db, threading.get_ident()))
        return registry.port

    monkeypatch.setattr("triage.monitoring.sql_store.AzureSqlMonitoringStore", adapter)
    return calls


def live_runtime(test_settings, *, monitoring_store=None, db=None):
    return CommandCenterService(
        test_settings.model_copy(update={
            "monitoring_mode": "live", "monitoring_tenant_id": TENANT,
            "azure_sql_server": "offline.database.invalid", "azure_sql_database": "offline",
            "approval_delivery_mode": "web", "run_history_enabled": True,
        }),
        WebSettings(_env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT),
        monitoring_store=monitoring_store, db=db if db is not None else NoSql(),
        history=InMemoryCommandCenterStore(), approvals=InMemoryApprovalChannel(),
    )


@pytest.fixture
def runtime(test_settings, registry):
    return live_runtime(test_settings, monitoring_store=registry.port)


def command_input(target: MonitoringTarget, *, kind="powerbi_triage", **changes):
    return CommandInput(
        kind=kind, target_id=target.key, subject="Investigate the recorded failure.",
        idempotency_key=str(uuid4()), **changes,
    )


def http_client(runtime, signing, *, static_dir=""):
    return TestClient(create_app(
        runtime, token_verifier=signing[0],
        web_settings=WebSettings(
            _env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT,
            static_dir=static_dir,
        ),
    ), raise_server_exceptions=False)


class CommandRunner:
    def __init__(self, runtime):
        self.settings = runtime.settings
        self.monitoring = runtime.monitoring.store
        self.monitoring_context = MonitoringContext(tenant_id=TENANT, epoch=EPOCH)
        self.history = runtime.history
        self.claims = InMemoryClaimStore()
        self.requests: list[BIRequest] = []
        self.selections: list[TargetIdentity] = []

    def build_command_center_store(self):
        return self.history

    def _pipeline_claim_store(self):
        return self.claims

    async def run_request(self, request: BIRequest):
        self.requests.append(request)
        return SimpleNamespace(
            result=SimpleNamespace(
                outcome="needs_human", summary="Diagnostic report only.", write_actions=0,
            ),
            run_id="",
        )

    async def pipeline_sweep(self, *, selection: TargetIdentity):
        self.selections.append(selection)
        return SimpleNamespace(
            status="queued", artifacts=[], summary=lambda: "One canonical pipeline observation queued.",
        )


def test_monitoring_router_is_mounted_before_static_files(runtime, registry, signing, tmp_path):
    (tmp_path / "index.html").write_text("<html>Synthetic frontend</html>", encoding="utf-8")
    with http_client(runtime, signing, static_dir=str(tmp_path)) as client:
        response = client.get("/api/monitoring/snapshot", headers=signing[1]())
        bootstrap = client.get("/api/monitoring/bootstrap", headers=signing[1]())
        frontend = client.get("/")
    assert response.status_code == bootstrap.status_code == frontend.status_code == 200
    assert response.json()["control"]["tenant_id"] == TENANT
    assert bootstrap.json()["status"] == "ready"
    assert "Synthetic frontend" in frontend.text
    assert response.headers["Cache-Control"] == "no-store"
    assert registry.mutations == []


def test_live_factory_is_lazy_shared_and_resolved_off_the_api_thread(
    test_settings, registry, signing, monkeypatch, bootstrap_adapter,
):
    calls = []
    db = NoSql()

    def factory(settings, *, db=None, fixture=False, component):
        assert component == "web"
        calls.append((settings, db, fixture, threading.get_ident()))
        return registry.port

    monkeypatch.setattr("triage.command_center.service.build_monitoring_store", factory)
    runtime = live_runtime(test_settings, db=db)
    assert calls == []
    client = http_client(runtime, signing)

    @client.app.middleware("http")
    async def record_thread(request, call_next):
        client.app.state.api_thread = threading.get_ident()
        return await call_next(request)

    with client:
        assert client.get("/api/access", headers=signing[1]()).status_code == 200
        assert calls == []
        assert client.get("/api/monitoring/bootstrap", headers=signing[1]()).status_code == 200
        assert calls == []
        assert runtime._monitoring_service is None
        assert client.get("/api/monitoring/snapshot", headers=signing[1]()).status_code == 200
        assert runtime.monitoring is runtime.monitoring
        api_thread = client.app.state.api_thread
    assert len(calls) == 1
    settings, actual_db, fixture, thread = calls[0]
    assert settings is runtime.settings and actual_db is db and fixture is False
    assert thread != api_thread
    assert len(bootstrap_adapter) == 1
    assert bootstrap_adapter[0][0] is db and bootstrap_adapter[0][1] != api_thread
    assert runtime.monitoring.store is registry.port


@pytest.mark.parametrize("error,code", [
    (MonitoringNotBootstrapped("Missing schema"), "monitoring_not_bootstrapped"),
    (MonitoringUnavailable("SQL is unavailable"), "monitoring_unavailable"),
    (MonitoringKernelUnsupported("Kernel unavailable"), "monitoring_kernel_incomplete"),
    (MonitoringComponentDenied("Wrong component"), "monitoring_component_mismatch"),
])
def test_live_factory_failure_is_explicit_and_can_recover_without_a_fixture(
    test_settings, registry, signing, monkeypatch, error, code,
):
    attempts = []

    def factory(settings, *, db=None, fixture=False, component):
        assert component == "web"
        attempts.append(fixture)
        if len(attempts) == 1:
            raise error
        return registry.port

    monkeypatch.setattr("triage.command_center.service.build_monitoring_store", factory)
    runtime = live_runtime(test_settings)
    with http_client(runtime, signing) as client:
        first = client.get("/api/monitoring/snapshot", headers=signing[1]())
        second = client.get("/api/monitoring/snapshot", headers=signing[1]())
    assert first.status_code == 503 and first.json()["code"] == code
    assert second.status_code == 200
    assert attempts == [False, False]
    assert runtime.settings.monitoring_mode == "live"


@pytest.mark.parametrize("component", ["fixture", "worker", "controller"])
def test_live_factory_cannot_return_another_component_or_fixture(
    test_settings, registry, signing, monkeypatch, component,
):
    registry.port.component = component

    def factory(settings, *, db=None, fixture=False, component):
        assert component == "web" and fixture is False
        return registry.port

    monkeypatch.setattr("triage.command_center.service.build_monitoring_store", factory)
    runtime = live_runtime(test_settings)
    with http_client(runtime, signing) as client:
        response = client.get("/api/monitoring/snapshot", headers=signing[1]())
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_component_mismatch"
    assert runtime._monitoring_service is None and registry.calls == []


def test_live_common_factory_routes_only_to_the_production_web_store(
    test_settings, registry, signing, monkeypatch,
):
    from test_monitoring_sql_store import SqlProtocolFixtureStore

    selected = []

    def production(*, db, component, policy):
        selected.append((db, component, policy))
        return registry.port

    def forbidden_fixture(*_args, **_kwargs):
        raise AssertionError("A live factory must never construct the SQL protocol fixture store")

    monkeypatch.setattr("triage.monitoring.sql_store.AzureSqlMonitoringStore", production)
    monkeypatch.setattr(SqlProtocolFixtureStore, "__init__", forbidden_fixture)
    runtime = live_runtime(test_settings)
    with http_client(runtime, signing) as client:
        response = client.get("/api/monitoring/snapshot", headers=signing[1]())
    assert response.status_code == 200
    assert len(selected) == 1 and selected[0][0] is runtime.db and selected[0][1] == "web"
    assert registry.port.inspect_bootstrap.call_count >= 1


@pytest.mark.parametrize("status", ["missing", "incompatible", "wrong_tenant"])
def test_cold_live_bootstrap_diagnosis_does_not_require_or_create_an_admission_store(
    test_settings, registry, signing, monkeypatch, bootstrap_adapter, status,
):
    registry.inspection = nonready_inspection(registry, status)
    calls = []

    def factory(settings, *, db=None, fixture=False, component):
        assert component == "web"
        calls.append(fixture)
        return registry.port

    monkeypatch.setattr("triage.command_center.service.build_monitoring_store", factory)
    runtime = live_runtime(test_settings)
    with http_client(runtime, signing) as client:
        response = client.get("/api/monitoring/bootstrap", headers=signing[1]())
        assert response.status_code == 200
        assert response.json()["status"] == status and response.json()["control"] is None
        assert response.json()["expected_tenant_id"] == TENANT
        assert OTHER not in response.text and "Private diagnostic" not in response.text
        assert calls == [] and runtime._monitoring_service is None
        assert client.get("/api/monitoring/snapshot", headers=signing[1]()).status_code == 503
    assert calls == [False]
    assert len(bootstrap_adapter) == 1
    assert bootstrap_adapter[0][0] is runtime.db
    assert registry.mutations == []


def test_cold_live_bootstrap_outage_is_not_a_missing_or_ready_diagnosis(
    test_settings, registry, signing, bootstrap_adapter,
):
    registry.failures["inspect_bootstrap"] = MonitoringUnavailable("Private connection details")
    runtime = live_runtime(test_settings)
    with http_client(runtime, signing) as client:
        response = client.get("/api/monitoring/bootstrap", headers=signing[1]())
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_unavailable"
    assert set(response.json()) == {"code", "message"}
    assert runtime._monitoring_service is None
    assert len(bootstrap_adapter) == 1 and registry.mutations == []


def test_cold_bootstrap_keeps_authentication_ahead_of_sql_inspection(
    test_settings, registry, signing, bootstrap_adapter,
):
    runtime = live_runtime(test_settings)
    with http_client(runtime, signing) as client:
        response = client.get("/api/monitoring/bootstrap")
    assert response.status_code == 401
    assert bootstrap_adapter == registry.calls == []


@pytest.mark.parametrize("status", [
    "missing", "partial_baseline", "incompatible", "wrong_tenant", "kernel_incomplete",
])
def test_cold_bootstrap_uses_the_real_sql_inspector_without_ddl_or_state_writes(
    test_settings, registry, signing, status,
):
    control = registry.control.model_copy(update={
        "tenant_id": TENANT if status == "kernel_incomplete" else OTHER,
    })

    class DiagnosticSql(NoSql):
        def __init__(self):
            self.reads = []

        @contextmanager
        def transaction(self):
            yield

        def query(self, statement, *parameters):
            self.reads.append(statement)
            if statement.startswith("SELECT OBJECT_ID"):
                if status == "kernel_incomplete" and "'P'" in statement:
                    return [tuple(None for _ in parameters)]
                if status == "missing":
                    return [tuple(None for _ in parameters)]
                if status == "partial_baseline":
                    return [tuple(
                        1 if name.endswith("monitoring_control") else None for name in parameters
                    )]
                return [tuple(1 for _ in parameters)]
            if statement == "SELECT SYSUTCDATETIME()":
                return [(NOW,)]
            if statement.startswith("SELECT singleton FROM"):
                return [(1,)]
            if statement.startswith("SELECT schema_version, tenant_id, epoch"):
                return [(
                    2 if status == "incompatible" else 1,
                    control.tenant_id, control.epoch, control.revision,
                    control.activation_cutoff, control.maintenance, control.updated_at,
                    control.model_dump_json(),
                )]
            raise AssertionError("Unexpected SQL outside bootstrap diagnosis")

    db = DiagnosticSql()
    runtime = live_runtime(test_settings, db=db)
    with http_client(runtime, signing) as client:
        response = client.get("/api/monitoring/bootstrap", headers=signing[1]())
        if status == "kernel_incomplete":
            blocked = client.get("/api/monitoring/snapshot", headers=signing[1]())
            assert blocked.status_code == 503
            assert blocked.json()["code"] == "monitoring_kernel_incomplete"
    assert response.status_code == 200, response.text
    assert response.json()["status"] == ("missing" if status == "partial_baseline" else status)
    if status == "kernel_incomplete":
        assert response.json()["control"]["tenant_id"] == TENANT
        assert response.json()["found_schema_version"] == 1
        assert response.json()["missing_operations"]
    else:
        assert response.json()["control"] is None
        assert response.json()["found_schema_version"] == (2 if status == "incompatible" else None)
    assert OTHER not in response.text and runtime._monitoring_service is None
    assert db.reads and all(statement.startswith("SELECT ") for statement in db.reads)


@pytest.mark.parametrize("mode,tenant", [("fixture", TENANT), ("live", OTHER), ("live", "")])
def test_live_constructor_rejects_fixture_or_mismatched_tenant_before_state_access(
    test_settings, registry, mode, tenant,
):
    with pytest.raises(ValueError):
        CommandCenterService(
            test_settings.model_copy(update={
                "monitoring_mode": mode, "monitoring_tenant_id": tenant,
                "azure_sql_server": "offline.invalid", "azure_sql_database": "offline",
            }),
            WebSettings(_env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT),
            db=NoSql(), monitoring_store=registry.port,
        )
    assert registry.calls == []


@pytest.mark.parametrize("mode,tenant", [("live", OTHER), ("demo", TENANT)])
def test_app_cannot_replace_live_registry_authorization_with_another_tenant_or_demo(
    runtime, registry, signing, mode, tenant,
):
    with pytest.raises(ValueError, match="Entra tenant"):
        create_app(
            runtime, token_verifier=signing[0],
            web_settings=WebSettings(
                _env_file=None, mode=mode, tenant_id=tenant, client_id=CLIENT,
            ),
        )
    assert registry.calls == []


def test_explicit_demo_store_and_approval_state_are_shared_with_its_runner(
    test_settings, tmp_path, monkeypatch,
):
    store = build_monitoring_store(test_settings, fixture=True, component="web")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("An injected demo registry must not construct a second authority")

    monkeypatch.setattr("triage.command_center.service.build_monitoring_store", forbidden)
    runtime = CommandCenterService(
        test_settings, WebSettings(_env_file=None, mode="demo", demo_worker=False),
        monitoring_store=store,
    )
    runner = DemoRunner(runtime, tmp_path)
    assert runtime.monitoring.store is store and store.component == "web"
    assert runner.monitoring.component == "controller" and runner.monitoring is not store
    assert runner.monitoring._backend.state is store._backend.state
    assert runtime.approvals is runner.build_approval_channel() is fixture_approvals(store)
    assert runtime.monitoring.tenant_id == FIXTURE_TENANT_ID
    assert runner.fixture is True and runner.sql is None


async def test_demo_startup_uses_registry_targets_without_changing_callers_settings(test_settings):
    original = test_settings.model_dump()
    runtime = CommandCenterService(
        test_settings, WebSettings(_env_file=None, mode="demo", demo_worker=False),
    )
    store = runtime.monitoring.store
    try:
        await start_demo(runtime)
        assert runtime.demo_runner.monitoring.component == "controller" and store.component == "web"
        assert runtime.demo_runner.monitoring._backend.state is store._backend.state
        assert runtime.demo_runner.build_approval_channel() is runtime.approvals
        targets = runtime.target_views(READER)
        assert all(target["id"].startswith("monitor:v1:") for target in targets)
        assert any(target["name"] == "Orders daily ingestion" for target in targets)
        assert any(target["name"] == "Customer service performance" for target in targets)
        assert test_settings.model_dump() == original
    finally:
        for task in runtime.demo_tasks:
            task.cancel()
        if runtime.demo_tasks:
            await asyncio.gather(*runtime.demo_tasks, return_exceptions=True)
        if temporary := getattr(runtime, "demo_temporary", None):
            temporary.cleanup()


def test_command_picker_reads_all_pages_at_one_registry_revision(runtime, registry):
    rows = [make_target(item_id=str(UUID(int=number + 100))) for number in range(205)]
    queries = []

    def pages(query: TargetQuery):
        queries.append(query)
        start = int(query.cursor or "0")
        end = start + query.limit
        return RecordPage[MonitoringTarget](
            version=registry.version, as_of=NOW, items=tuple(rows[start:end]),
            next_cursor=str(end) if end < len(rows) else None,
        )

    registry.port.list_targets.side_effect = pages
    views = runtime.target_views(READER)
    assert len(views) == 205 and {view["id"] for view in views} == {row.key for row in rows}
    assert [query.cursor for query in queries] == [None, "100", "200"]
    assert all(query.tenant_id == TENANT and query.epoch == EPOCH for query in queries)
    assert all(query.limit == 100 for query in queries)
    assert len({view["name"] for view in views}) == 1


@pytest.mark.parametrize("fault,status", [("changed_revision", 409), ("repeated_cursor", 503)])
def test_picker_does_not_return_partial_success_when_pagination_fails(
    runtime, registry, fault, status,
):
    calls = 0

    def pages(query: TargetQuery):
        nonlocal calls
        calls += 1
        return RecordPage[MonitoringTarget](
            version=RegistryVersion(
                tenant_id=TENANT, epoch=EPOCH,
                revision=8 if calls > 1 and fault == "changed_revision" else 7,
            ),
            as_of=NOW, items=(make_target(),), next_cursor="same",
        )

    registry.port.list_targets.side_effect = pages
    with pytest.raises(ApiFailure) as error:
        runtime.target_views(READER)
    assert error.value.status == status
    assert calls == 2


def test_picker_does_not_offer_paused_removed_or_nonobserving_targets(runtime, registry):
    admitted = make_target()
    registry.current_targets = [
        admitted, make_target(item_id=OTHER, state="paused", observing=False),
        make_target(item_id=CLIENT, state="removed", observing=False),
        make_target(item_id=USER, observing=False),
    ]
    assert [target["id"] for target in runtime.target_views(READER)] == [admitted.key]


@pytest.mark.parametrize("change,status", [
    ("legacy", 422), ("wrong_tenant", 422), ("wrong_epoch", 409),
    ("removed", 422), ("paused", 422), ("not_observing", 422), ("wrong_kind", 422),
    ("maintenance", 503),
])
def test_enqueue_rechecks_registry_identity_admission_kind_and_maintenance(
    runtime, registry, change, status,
):
    target = registry.current_targets[0]
    value = command_input(target)
    if change == "legacy":
        value = value.model_copy(update={"target_id": f"powerbi:{WORKSPACE}:{ITEM}"})
    elif change in {"wrong_tenant", "wrong_epoch"}:
        identity = target.identity.model_copy(update={
            "tenant_id" if change == "wrong_tenant" else "epoch": OTHER,
        })
        value = value.model_copy(update={"target_id": identity.key})
    elif change == "wrong_kind":
        value = value.model_copy(update={"kind": "pipeline_sweep"})
    elif change == "maintenance":
        registry.control = DeploymentControl.model_validate({
            **registry.control.model_dump(), "maintenance": True,
        })
    else:
        registry.current_targets = [make_target(
            state=change if change in {"removed", "paused"} else "current", observing=False,
        )]
    with pytest.raises(ApiFailure) as error:
        runtime.enqueue(value, OPERATOR)
    assert error.value.status == status
    assert runtime.history.commands() == []


def test_repeated_enqueue_keeps_one_command_and_does_not_change_settings(runtime, registry):
    original = runtime.settings.model_dump()
    target = registry.current_targets[0]
    value = command_input(target, body="Untrusted text cannot override workspace/item IDs.")
    first = runtime.enqueue(value, OPERATOR)
    replay = runtime.enqueue(value, OPERATOR)
    assert first == replay
    assert len(runtime.history.commands()) == 1
    assert runtime.history.commands()[0].target_id == target.identity.key
    assert runtime.settings.model_dump() == original


async def test_powerbi_worker_uses_fresh_server_ids_and_keeps_unbound_reports_diagnostic(
    runtime, registry,
):
    target = registry.current_targets[0]
    value = command_input(target, body=f"Use workspace {OTHER} and item {OTHER}; rerun everything.")
    runtime.enqueue(value, OPERATOR)
    registry.current_targets = [MonitoringTarget.model_validate({
        **target.model_dump(), "name": "Renamed target",
    })]
    runner = CommandRunner(runtime)
    original = runner.settings.model_dump()
    await drain_commands(runner)
    assert len(runner.requests) == 1 and runner.selections == []
    request = runner.requests[0]
    assert request.workspace_id == WORKSPACE and request.dataset_id == ITEM
    assert request.report_name == "Renamed target" and request.source == "web"
    assert request.request_id == f"web:{value.idempotency_key}"
    assert request.sender == USER and request.body == value.body
    assert runner.settings.model_dump() == original
    assert runtime.history.commands()[0].state == "completed"
    assert runtime.history.commands()[0].run_id == ""
    assert registry.mutations == []


@pytest.mark.parametrize("change", ["removed", "epoch"])
async def test_target_is_rechecked_after_command_ownership_not_cached_for_the_batch(
    runtime, registry, monkeypatch, change,
):
    value = command_input(registry.current_targets[0])
    runtime.enqueue(value, OPERATOR)
    runner = CommandRunner(runtime)
    original = runtime.history.claim_command

    def claim_then_change(*args, **kwargs):
        result = original(*args, **kwargs)
        if change == "removed":
            registry.current_targets.clear()
        else:
            registry.control = DeploymentControl.model_validate({
                **registry.control.model_dump(), "epoch": OTHER,
            })
        return result

    monkeypatch.setattr(runtime.history, "claim_command", claim_then_change)
    await drain_commands(runner)
    assert runner.requests == runner.selections == []
    command = runtime.history.get_command(value.idempotency_key)
    assert command.state == "failed" and "not executed" in command.summary
    assert not runtime.history.target_blocked(value.target_id)


async def test_pipeline_worker_passes_only_the_selected_canonical_identity(runtime, registry):
    target = make_target(workload="fabric_pipeline")
    registry.current_targets = [target]
    runtime.enqueue(command_input(target, kind="pipeline_sweep"), OPERATOR)
    runner = CommandRunner(runtime)
    original = runner.settings.model_dump()
    await drain_commands(runner)
    assert runner.selections == [target.identity] and runner.requests == []
    assert runner.settings.model_dump() == original
    assert runtime.history.commands()[0].state == "completed"
    assert registry.mutations == []


async def test_missing_core_pipeline_selection_binding_fails_before_execution(
    runtime, registry, monkeypatch,
):
    target = make_target(workload="fabric_pipeline")
    registry.current_targets = [target]
    value = command_input(target, kind="pipeline_sweep")
    runtime.enqueue(value, OPERATOR)
    runner = CommandRunner(runtime)
    unbound_calls = []

    async def unbound(*, targets=None):
        unbound_calls.append(targets)
        raise AssertionError("An unbound pipeline sweep must never be used for a selected command")

    monkeypatch.setattr(runner, "pipeline_sweep", unbound)
    await drain_commands(runner)
    assert unbound_calls == runner.requests == runner.selections == []
    command = runtime.history.get_command(value.idempotency_key)
    assert command.state == "failed"
    assert "pipeline_sweep(selection: TargetIdentity)" in command.summary
    assert not runtime.history.target_blocked(value.target_id)


def test_default_demo_picker_uses_registry_identity_instead_of_environment_targets(test_settings):
    runtime = CommandCenterService(
        test_settings, WebSettings(_env_file=None, mode="demo", demo_worker=False),
    )
    views = runtime.target_views(READER)
    expected = fixture_target("powerbi", "fixture-workspace", "fixture-model")
    assert any(view["id"] == expected.key and view["kind"] == "powerbi_triage" for view in views)
    assert all(view["id"].startswith("monitor:v1:") for view in views)
