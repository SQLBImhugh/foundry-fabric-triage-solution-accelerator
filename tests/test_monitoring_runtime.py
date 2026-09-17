from __future__ import annotations

from datetime import timedelta

import pytest

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringComponentDenied, MonitoringNotBootstrapped
from triage.monitoring.runtime import (
    FIXTURE_NOW,
    FIXTURE_TENANT_ID,
    build_monitoring_store,
    ensure_fixture_target,
    fixture_approvals,
    fixture_clock,
    fixture_component,
    fixture_setup,
    fixture_target,
    fixture_time,
    inspect_context,
    registered_targets,
    stable_id,
)
from triage.settings import Settings


@pytest.fixture
def settings():
    return Settings(
        _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
        triage_provider_mode="mock", azure_sql_server="", azure_sql_database="",
        applicationinsights_connection_string="",
    )


@pytest.fixture
def clock_scope():
    with fixture_time(FIXTURE_NOW + timedelta(days=1)):
        yield


@pytest.mark.parametrize("component", ["worker", "web", "controller"])
@pytest.mark.parametrize("explicit_flag", [False, True])
def test_fixture_factory_returns_requested_component_after_explicit_setup(settings, clock_scope, component, explicit_flag):
    store = build_monitoring_store(settings, fixture=explicit_flag, component=component)
    context = inspect_context(store, FIXTURE_TENANT_ID)
    assert store.component == component
    targets = registered_targets(store, context)
    assert {target.identity.workload for target in targets} == {"powerbi", "fabric_pipeline"}
    assert all(target.state == "current" for target in targets)
    assert all(not target.action.enabled for target in targets)


def test_shared_fixture_views_preserve_state_clock_and_approval_identity(settings, clock_scope):
    web = build_monitoring_store(settings, component="web")
    controller = fixture_component(web, "controller")
    worker = fixture_component(web, "worker")
    assert web is not controller and web is not worker
    assert (web.component, controller.component, worker.component) == ("web", "controller", "worker")
    assert fixture_clock(web) is fixture_clock(controller) is fixture_clock(worker)
    assert fixture_approvals(web) is fixture_approvals(controller) is fixture_approvals(worker)
    assert fixture_component(web, "web") is web


def action_request(store):
    context = inspect_context(store, FIXTURE_TENANT_ID)
    target = fixture_target("powerbi", "fixture-workspace", "fixture-model")
    now = fixture_clock(store)()
    work_id = stable_id("fixture-component-test-work")
    return m.ActionReservationRequest(
        idempotency_id=stable_id("fixture-component-test-action"),
        expected=m.RegistryVersion(**context.model_dump(), revision=store.snapshot(context).control.revision),
        work_id=work_id,
        lease=m.LeaseToken(
            **context.model_dump(), resource_key=m.work_key(context, work_id),
            owner_id=stable_id("fixture-component-test-owner"), fence=1,
            acquired_at=now, expires_at=now + timedelta(minutes=1),
        ),
        source_execution=m.SourceExecutionIdentity(
            target=target, run_id=stable_id("fixture-component-test-run"), run_id_kind="powerbi_request",
        ),
        incident=m.IncidentIdentity(target=target, signature="v1:fixture-component-test"),
        expected_incident_revision=0, action="powerbi_refresh",
        review_id=stable_id("fixture-component-test-review"), expected_review_revision=1,
        parameter_hash=m._digest({}),
    )


@pytest.mark.parametrize("component", ["web", "worker"])
def test_restricted_fixture_view_cannot_reserve_actions(settings, clock_scope, component):
    store = build_monitoring_store(settings, component=component)
    request = action_request(store)
    with pytest.raises(MonitoringComponentDenied):
        store.reserve_action(request)


def test_web_intent_stays_pending_until_shared_controller_publication(settings, clock_scope):
    web = build_monitoring_store(settings, component="web")
    controller = fixture_component(web, "controller")
    worker = fixture_component(web, "worker")
    context = inspect_context(web, FIXTURE_TENANT_ID)
    scope = web.list_scopes(m.PageQuery(**context.model_dump())).items[0]
    definition = m.ScopeDefinition.model_validate({
        **scope.model_dump(include=set(m.ScopeDefinition.model_fields)), "enabled": False,
    })
    version = m.RegistryVersion(**context.model_dump(), revision=web.snapshot(context).control.revision)
    plan = web.preview_scope(m.ScopePreviewRequest(
        expected=version, idempotency_id=stable_id("fixture-component-disable"), scope=definition,
    ))
    request = m.ActivateScopeRequest(expected=version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id)
    with pytest.raises(MonitoringComponentDenied):
        worker.activate_scope(request)
    receipt = web.activate_scope(request)
    assert receipt.state == "configuring"
    assert len(receipt.queued_work_ids) == 1
    claim = m.WorkClaimRequest(
        **context.model_dump(), owner_id=stable_id("fixture-component-controller"),
        kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    )
    with pytest.raises(MonitoringComponentDenied):
        web.claim_work(claim)
    work = controller.claim_work(claim)[0]
    assert work.work_id == receipt.queued_work_ids[0]
    assert work.kind == "reconcile_state" and work.execution is None
    assert work.lease.resource_key == work.key
    assert controller.reconcile_work(work).state == "published"
    assert web.get_activation(context, request.idempotency_id) == receipt
    assert any(target.state == "paused" for target in registered_targets(web, context, include_inactive=True))


def test_seeding_requires_explicit_setup_and_does_not_change_runtime_component(settings, clock_scope):
    web = build_monitoring_store(settings, component="web")
    controller = fixture_component(web, "controller")
    target = fixture_target("powerbi", "explicit-setup-workspace", "explicit-setup-model")
    with pytest.raises(ValueError, match="fixture_setup"):
        ensure_fixture_target(web, target, "Explicit fixture target")
    with pytest.raises(ValueError, match="fixture_setup"):
        ensure_fixture_target(controller, target, "Explicit fixture target")
    with fixture_setup(web) as setup:
        assert setup.component == "fixture"
        seeded = ensure_fixture_target(setup, target, "Explicit fixture target", action="powerbi_refresh")
        assert seeded.action.enabled
        assert fixture_clock(setup) is fixture_clock(web)
        assert fixture_approvals(setup) is fixture_approvals(web)
    assert web.component == "web" and controller.component == "controller"
    assert web.resolve_target(target) == controller.resolve_target(target)
    assert web.resolve_target(target).action.enabled
    with pytest.raises(ValueError, match="externally owned"):
        ensure_fixture_target(setup, target, "An expired setup handle")


def test_omitted_component_remains_explicit_fixture_setup_only(settings, clock_scope):
    setup = build_monitoring_store(settings, fixture=True)
    assert setup.component == "fixture"
    target = fixture_target("fabric_pipeline", "setup-workspace", "setup-pipeline")
    assert ensure_fixture_target(setup, target, "Setup pipeline").state == "current"


@pytest.mark.parametrize("component", ["admin", "fixture", "", 1])
def test_fixture_factory_rejects_invalid_runtime_component(settings, component):
    with pytest.raises(ValueError, match="Monitoring component"):
        build_monitoring_store(settings, fixture=True, component=component)


def test_fixture_contexts_cannot_adopt_live_or_external_state(settings):
    external = object()
    with pytest.raises(ValueError, match="runtime fixture"):
        fixture_component(external, "web")
    with pytest.raises(ValueError, match="externally owned"):
        with fixture_setup(external):
            pytest.fail("External state must not obtain fixture authority")
    with pytest.raises(ValueError, match="live SQL handle"):
        build_monitoring_store(settings, fixture=True, component="web", db=external)
    live = Settings(
        _env_file=None, monitoring_mode="live", monitoring_tenant_id=stable_id("live-factory-test-tenant"),
        azure_sql_server="", azure_sql_database="",
    )
    with pytest.raises(MonitoringNotBootstrapped):
        build_monitoring_store(live, component="web")
