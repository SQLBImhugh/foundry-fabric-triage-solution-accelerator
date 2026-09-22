from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from triage.approvals import AutoApproveGate
from triage.models import BIRequest, TriageResult
from triage.monitoring.contracts import MonitoringNotBootstrapped, MonitoringUnavailable
from triage.monitoring.controller import (
    HEARTBEAT_BUDGET_SECONDS,
    HeartbeatBudget,
    MonitoringExecution,
    controller_heartbeat,
)
from triage.monitoring.models import (
    SUPERSESSION_EVIDENCE_TTL_SECONDS,
    IncidentIdentity,
    MonitoringWorkDraft,
    RegistryVersion,
    SafetyReviewRequest,
    SourceExecutionIdentity,
    SourceRunObservation,
    WorkClaimRequest,
)
from triage.monitoring.runtime import (
    build_monitoring_store,
    ensure_fixture_target,
    fixture_approvals,
    fixture_id,
    fixture_target,
    inspect_context,
    target_signature,
)
from triage.policy import REMEDIATION_ACTIONS, PolicyLedger, TriagePolicy
from triage.runner import RECONCILE_LEASE_SECONDS, TriageRunner
from triage.settings import Settings
from triage.store.incidents import InMemoryIncidentStore
from triage.tools.flags import DataQualityFlagTable
from triage.tools.powerbi import MockPowerBIClient
from triage.tools.registry import ToolContext, ToolDispatcher
from triage.tools.teams import MockTeamsNotifier


@pytest.fixture
def configured(tmp_path):
    def build(action: str = "powerbi_refresh"):
        settings = Settings(
            _env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
            triage_provider_mode="mock", azure_sql_server="", azure_sql_database="",
            applicationinsights_connection_string="",
        )
        store = build_monitoring_store(settings, fixture=True)
        identity = fixture_target("powerbi", "controller-test-workspace", "controller-test-model")
        desired = None
        if action == "rebind_dataset_gateway":
            desired = {"gateway_id": fixture_id("gw-onprem-02", kind="gateway"), "datasource_ids": [str(uuid4())]}
        elif action == "reenable_refresh_schedule":
            desired = {"enabled": True}
        target = ensure_fixture_target(store, identity, "Synthetic controller target", action=action, parameters=desired)
        context = inspect_context(store, identity.tenant_id)
        now = datetime.now(UTC)
        signature = target_signature(identity, "TimeoutError: synthetic transport failure")
        source = SourceExecutionIdentity(target=identity, run_id_kind="powerbi_request", run_id=str(uuid4()))
        observation = SourceRunObservation(
            execution=source, origin="fixture", authority="fixture", observed_at=now,
            started_at=now - timedelta(minutes=2), ended_at=now - timedelta(minutes=1),
            status="failed", invocation="scheduled", failure_reason="TimeoutError: synthetic transport failure",
            failure_signature=signature,
        )
        store.enqueue_work(MonitoringWorkDraft(
            **context.model_dump(), work_id=str(uuid4()), kind="triage",
            policy_revision=store.snapshot(context).control.revision,
            created_at=now, due_at=now, reason="Controller regression fixture.",
            target=identity, execution=source,
        ))
        work = store.claim_work(WorkClaimRequest(
            **context.model_dump(), owner_id=str(uuid4()), kinds=("triage",),
            limit=1, per_workspace_limit=1, lease_seconds=900,
        ))[0]
        store.observe_source(observation, work_id=work.work_id, lease=work.lease)
        if action == "reenable_refresh_schedule":
            store.observe_source(SourceRunObservation(
                execution=SourceExecutionIdentity(target=identity, run_id_kind="powerbi_request", run_id=str(uuid4())),
                origin="fixture", authority="fixture", observed_at=now,
                started_at=now - timedelta(seconds=20), ended_at=now - timedelta(seconds=10),
                status="succeeded", invocation="manual",
            ), work_id=work.work_id, lease=work.lease)
        channel = fixture_approvals(store)
        monitoring = MonitoringExecution(
            store=store, work=work, incident=IncidentIdentity(target=identity, signature=signature),
            observation=observation, approval_channel=channel, fixture=True,
        )

        async def reread():
            return observation.model_copy(update={"observed_at": datetime.now(UTC)})

        monitoring.refresh_source = reread
        client = MockPowerBIClient(
            latency_ms=0, history=[{"status": "Completed"}] if action == "reenable_refresh_schedule" else [],
            schedule_enabled=False,
        )
        ctx = ToolContext(
            request=BIRequest(
                request_id="controller-fixture", source="interactive", sender="fixture",
                subject="Synthetic failure", body=observation.failure_reason,
                workspace_id=identity.workspace_id, dataset_id=identity.item_id,
            ),
            ledger=PolicyLedger(TriagePolicy()), powerbi=client, teams=MockTeamsNotifier(),
            flag_table=DataQualityFlagTable(tmp_path / f"{action}.csv"),
            workspace_id=identity.workspace_id, dataset_id=identity.item_id,
            signature=signature, approval_gate=AutoApproveGate(), monitoring=monitoring,
        )
        return store, target, channel, client, ctx
    return build


@pytest.mark.parametrize("action,tool,arguments", [
    ("powerbi_refresh", "refresh_powerbi_dataset", {"justification": "Transient source failure."}),
    ("rebind_dataset_gateway", "rebind_dataset_gateway", {"target_gateway": "gw-onprem-02", "justification": "Reviewed replacement."}),
    ("reenable_refresh_schedule", "reenable_refresh_schedule", {"justification": "A newer refresh succeeded."}),
])
async def test_powerbi_mutations_share_reservation_and_exact_verification(configured, action, tool, arguments) -> None:
    store, _, channel, client, ctx = configured(action)
    response = await ToolDispatcher(ctx).dispatch(tool, arguments)
    assert response["status"] == "Completed"
    assert ctx.ledger.write_actions == 1
    reservation = store.get_action_reservation(ctx.monitoring.context, ctx.monitoring.reservation.reservation_id)
    assert reservation.state == "verified_succeeded"
    assert store.get_incident_state(ctx.monitoring.incident).action_count == 1
    if action == "powerbi_refresh":
        assert reservation.submitted_execution != reservation.request.source_execution
        assert sum(name == "refresh_dataset" for name, _ in client.calls) == 1
    else:
        assert reservation.submitted_execution is None
        assert reservation.configuration is not None
        assert channel.get(ctx.pending_approval[0].request_id)["consumed_at"]


async def test_review_revocation_after_human_approval_spends_nothing(configured) -> None:
    store, target, channel, client, ctx = configured("rebind_dataset_gateway")

    class Revoke(AutoApproveGate):
        async def request_approval(self, request):
            review = store.get_safety_review(ctx.monitoring.context, target.action.review_id)
            store.record_safety_review(SafetyReviewRequest(
                request_id=str(uuid4()),
                expected=RegistryVersion(**ctx.monitoring.context.model_dump(), revision=store.snapshot(ctx.monitoring.context).control.revision),
                expected_review_revision=review.revision,
                review=review.model_copy(update={
                    "revision": review.revision + 1, "state": "revoked", "revoked_at": datetime.now(UTC),
                }),
            ))
            return await super().request_approval(request)

    ctx.approval_gate = Revoke()
    response = await ToolDispatcher(ctx).dispatch("rebind_dataset_gateway", {
        "target_gateway": "gw-onprem-02", "justification": "Reviewed replacement.",
    })
    assert response["status"] == "blocked_by_policy"
    assert ctx.ledger.write_actions == 0
    assert client.calls == []
    assert not channel.get(ctx.pending_approval[0].request_id)["consumed_at"]
    assert store.get_incident_state(ctx.monitoring.incident) is None


@pytest.mark.parametrize("tool", sorted(REMEDIATION_ACTIONS))
async def test_missing_live_monitoring_context_cannot_dispatch_any_remediation(tmp_path, tool) -> None:
    class LiveClient:
        pass

    ctx = ToolContext(
        request=BIRequest(request_id="unbound", source="interactive", sender="operator", subject="A report", body="Untrusted annotation"),
        ledger=PolicyLedger(TriagePolicy()), powerbi=LiveClient(), teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "unused.csv"), approval_gate=AutoApproveGate(),
    )
    response = await ToolDispatcher(ctx).dispatch(tool, {"justification": "A report", **(
        {"target_gateway": str(uuid4())} if tool == "rebind_dataset_gateway" else {}
    )})
    assert response["status"] == "blocked_by_policy"
    assert ctx.ledger.write_actions == 0
    assert ctx.approval_gate.requests == []


async def test_reservation_failure_cannot_reach_the_refresh_endpoint(configured, monkeypatch) -> None:
    store, _, _, client, ctx = configured()

    def fail(_request):
        raise MonitoringUnavailable("Synthetic SQL outage")

    monkeypatch.setattr(store, "reserve_action", fail)
    with pytest.raises(MonitoringUnavailable):
        await ToolDispatcher(ctx).dispatch("refresh_powerbi_dataset", {"justification": "Transient"})
    assert client.calls == []
    assert ctx.ledger.write_actions == 0


async def test_submission_persistence_failure_retains_fence_and_unfinished_work(configured, monkeypatch) -> None:
    store, _, _, client, ctx = configured()

    def fail(_request, *, commit):
        assert commit.work_id == ctx.monitoring.work.work_id
        raise MonitoringUnavailable("Synthetic correlation failure")

    monkeypatch.setattr(store, "record_action_submission", fail)
    with pytest.raises(MonitoringUnavailable):
        await ToolDispatcher(ctx).dispatch("refresh_powerbi_dataset", {"justification": "Transient"})
    assert sum(name == "refresh_dataset" for name, _ in client.calls) == 1
    reservation = store.get_action_reservation(ctx.monitoring.context, ctx.monitoring.reservation.reservation_id)
    assert reservation.state == "reserved"
    assert store.get_work(ctx.monitoring.context, ctx.monitoring.work.work_id).state == "leased"
    ctx.ledger = PolicyLedger(TriagePolicy())
    response = await ToolDispatcher(ctx).dispatch("refresh_powerbi_dataset", {"justification": "Try again"})
    assert response["status"] == "blocked_by_policy"
    assert sum(name == "refresh_dataset" for name, _ in client.calls) == 1
    assert ctx.ledger.write_actions == 0


def test_runner_sql_selection_never_bootstraps_or_borrows_a_credential(monkeypatch) -> None:
    credential = object()
    selected = []

    class Database:
        def __init__(self, **kwargs):
            selected.append(kwargs)

        def ensure_schema_once(self):
            raise AssertionError("Runtime DDL is forbidden")

    monkeypatch.setattr("triage.store.azure_sql.AzureSqlDatabase", Database)
    runner = TriageRunner.__new__(TriageRunner)
    runner.fixture = False
    runner._credential = credential
    runner.settings = Settings(
        _env_file=None, monitoring_mode="live", monitoring_tenant_id=str(uuid4()),
        azure_sql_server="synthetic.invalid", azure_sql_database="synthetic",
    )
    assert isinstance(runner._build_sql(), Database)
    assert selected[0]["credential"] is credential


def test_fixture_runner_ignores_exported_sql_configuration(tmp_path, monkeypatch) -> None:
    def forbidden(**_kwargs):
        raise AssertionError("A fixture cannot construct a live SQL handle")

    monkeypatch.setattr("triage.store.azure_sql.AzureSqlDatabase", forbidden)
    runner = TriageRunner(
        Settings(_env_file=None, monitoring_mode="fixture", triage_tool_mode="mock",
                 azure_sql_server="configured.invalid", azure_sql_database="configured"),
        base_dir=tmp_path, store=InMemoryIncidentStore(),
    )
    assert runner.sql is None


def test_live_factory_refuses_missing_sql_instead_of_seeding_fixtures() -> None:
    with pytest.raises(MonitoringNotBootstrapped):
        build_monitoring_store(Settings(
            _env_file=None, monitoring_mode="live", monitoring_tenant_id=str(uuid4()),
            azure_sql_server="", azure_sql_database="",
        ), component="controller")


def test_finalization_failure_returns_no_local_success_or_processed_source(configured, tmp_path, monkeypatch) -> None:
    store, _, _, client, ctx = configured()
    runner = TriageRunner(
        Settings(_env_file=None, monitoring_mode="fixture", triage_tool_mode="mock"),
        base_dir=tmp_path, store=InMemoryIncidentStore(), monitoring_store=store,
    )

    def fail(_request):
        raise MonitoringUnavailable("Synthetic terminal commit failure")

    monkeypatch.setattr(store, "finalize_work", fail)
    with pytest.raises(MonitoringUnavailable):
        runner._finalize_monitoring_result(
            ctx.monitoring,
            TriageResult(outcome="needs_human", signature=ctx.signature, summary="Not finalized."),
            {"source": "powerbi_refresh_failure"},
        )
    assert store.get_incident(ctx.monitoring.incident) is None
    assert store.get_source_disposition(ctx.monitoring.work.execution) is None
    assert store.get_work(ctx.monitoring.context, ctx.monitoring.work.work_id).state == "leased"
    assert runner.store.list_all() == []
    assert client.calls == []


def test_signature_uses_target_identity_not_labels_or_source_delivery() -> None:
    from triage.signature import compute_signature

    first = fixture_target("powerbi", "workspace-one", "same-model")
    second = fixture_target("powerbi", "workspace-two", "same-model")
    assert target_signature(first, "TimeoutError") != target_signature(second, "TimeoutError")
    before = compute_signature(source="powerbi_refresh_failure", error="TimeoutError", target_key=first.key, artifact_name="Old label")[0]
    after = compute_signature(source="powerbi_refresh_failure", error="TimeoutError", target_key=first.key, artifact_name="New label")[0]
    assert before == after


def test_duplicate_execution_references_return_the_original_work(configured, tmp_path) -> None:
    store, _, _, _, ctx = configured()
    runner = TriageRunner(
        Settings(_env_file=None, monitoring_mode="fixture", triage_tool_mode="mock"),
        base_dir=tmp_path, store=InMemoryIncidentStore(), monitoring_store=store,
    )
    from triage.monitoring.runtime import source_work_id

    other = SourceExecutionIdentity(
        target=ctx.monitoring.incident.target, run_id_kind="powerbi_request", run_id=str(uuid4()),
    )
    first = runner.enqueue_execution_reference(other)
    second = runner.enqueue_execution_reference(other)
    assert first.work_id == second.work_id == source_work_id(other)
    assert first.created_at == second.created_at
    assert second.state == "queued"


async def test_selected_live_pipeline_command_never_queues_other_targets(configured, monkeypatch) -> None:
    store, _, _, _, ctx = configured()
    selected = fixture_target("fabric_pipeline", "selected-workspace", "selected-pipeline")
    other = fixture_target("fabric_pipeline", "other-workspace", "other-pipeline")
    ensure_fixture_target(store, selected, "Selected pipeline")
    ensure_fixture_target(store, other, "Other pipeline")
    queued = []
    original = store.enqueue_work

    def capture(work):
        queued.append(work)
        return original(work)

    monkeypatch.setattr(store, "enqueue_work", capture)
    runner = TriageRunner.__new__(TriageRunner)
    runner.fixture = False
    runner._monitoring_store = store
    runner.settings = Settings(
        _env_file=None, monitoring_mode="live", monitoring_tenant_id=ctx.monitoring.context.tenant_id,
    )
    report = await runner.pipeline_sweep(selection=selected)
    assert report.status == "queued"
    assert len(queued) == 1
    assert queued[0].target == selected


async def test_heartbeat_interleaves_bounded_automatic_and_human_work(test_settings) -> None:
    calls = []

    class Runner:
        settings = test_settings

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            assert budget.can_claim()
            calls.append(("automatic", limit))
            return ["automatic result"] if len(calls) <= 2 else []

    async def human(_runner, *, limit, budget):
        assert budget.can_claim()
        calls.append(("human", limit))
        return ["human result"] if len(calls) <= 2 else []

    lines = await controller_heartbeat(Runner(), command_drain=human)
    assert lines == ["automatic result", "human result"]
    assert calls[:2] == [("automatic", 1), ("human", 1)]
    assert calls.count(("automatic", 1)) == 3
    assert calls.count(("human", 1)) == 2


async def test_heartbeat_does_not_turn_sql_outage_into_an_empty_success(test_settings) -> None:
    class Runner:
        settings = test_settings

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            raise MonitoringUnavailable("Synthetic SQL outage")

    async def human(_runner, *, limit, budget):
        return []

    with pytest.raises(MonitoringUnavailable):
        await controller_heartbeat(Runner(), command_drain=human)


async def test_heartbeat_stops_before_second_long_human_command(test_settings, caplog):
    elapsed = [0.0]
    calls = []

    class Runner:
        settings = test_settings

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            calls.append(("automatic", budget.deadline))
            return []

    async def human(_runner, *, limit, budget):
        calls.append(("human", budget.deadline))
        assert budget.work_seconds == 690
        await asyncio.sleep(0)
        elapsed[0] += 500
        return ["private incident text not for telemetry"]

    with caplog.at_level("INFO", logger="triage.telemetry.heartbeat"):
        result = await controller_heartbeat(Runner(), command_drain=human, clock=lambda: elapsed[0])
    assert len(result) == 1 and sum(queue == "human" for queue, _ in calls) == 1
    assert all(deadline == HEARTBEAT_BUDGET_SECONDS for _, deadline in calls)
    assert "elapsed_ms=500000" in caplog.text and "human_results=1" in caplog.text
    assert "budget_exhausted=True" in caplog.text
    assert "private incident text" not in caplog.text


async def test_heartbeat_includes_elapsed_time_before_it_acquires_the_invocation_lock(test_settings):
    async def unexpected(*args, **kwargs):
        pytest.fail("Insufficient invocation budget must leave both queues unclaimed")

    runner = SimpleNamespace(settings=test_settings, drain_monitoring_work=unexpected)
    result = await controller_heartbeat(
        runner, command_drain=unexpected, started_at=100.0, clock=lambda: 251.0,
    )
    assert len(result) == 1 and "budget exhausted" in result[0] and "remains pending" in result[0]


async def test_heartbeat_shared_quotas_still_bound_fast_work(test_settings):
    calls = {"automatic": 0, "human": 0}

    class Runner:
        settings = test_settings

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            calls["automatic"] += 1
            await asyncio.sleep(0)
            return ["automatic"]

    async def human(_runner, *, limit, budget):
        calls["human"] += 1
        await asyncio.sleep(0)
        return ["human"]

    assert len(await controller_heartbeat(Runner(), rounds=4, command_drain=human, clock=lambda: 0.0)) == 8
    assert calls == {"automatic": 4, "human": 4}


async def test_heartbeat_waits_for_claimed_sibling_after_failure_without_cancelling_it(test_settings):
    entered, release = asyncio.Event(), asyncio.Event()
    automatic_calls = 0
    completed = []

    class Runner:
        settings = test_settings

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            nonlocal automatic_calls
            automatic_calls += 1
            if automatic_calls == 1:
                await entered.wait()
                raise MonitoringUnavailable("Private SQL error must not enter heartbeat telemetry")
            return []

    async def human(_runner, *, limit, budget):
        entered.set()
        await release.wait()
        completed.append("original action completion")
        return ["completed once"]

    task = asyncio.create_task(controller_heartbeat(Runner(), command_drain=human))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(MonitoringUnavailable):
        await asyncio.wait_for(task, 2)
    assert completed == ["original action completion"]


async def test_elapsed_heartbeat_deadline_does_not_cancel_or_repeat_claimed_work(test_settings):
    elapsed = [0.0]
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    class Runner:
        settings = test_settings

        async def drain_monitoring_work(self, *, limit, budget, prefer="reconcile_state"):
            return []

    async def human(_runner, *, limit, budget):
        calls.append("claimed")
        entered.set()
        await release.wait()
        calls.append("completed")
        return ["original result"]

    task = asyncio.create_task(controller_heartbeat(Runner(), command_drain=human, clock=lambda: elapsed[0]))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        elapsed[0] = HEARTBEAT_BUDGET_SECONDS + 1
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    assert await asyncio.wait_for(task, 2) == ["original result"]
    assert calls == ["claimed", "completed"]


async def test_monitoring_drain_rechecks_budget_after_context_read(runner, monkeypatch):
    elapsed = [0.0]
    context = runner.monitoring_context

    def slow_context(_runner):
        elapsed[0] = 200.0
        return context

    def forbidden(*args, **kwargs):
        pytest.fail("No SQL work claim is allowed after context lookup exhausted admission time")

    monkeypatch.setattr(type(runner), "monitoring_context", property(slow_context))
    monkeypatch.setattr(runner.monitoring, "claim_work", forbidden)
    budget = HeartbeatBudget(840, 690, lambda: elapsed[0])
    assert await runner.drain_monitoring_work(limit=10, budget=budget) == []


async def test_monitoring_drain_leaves_later_work_queued_after_elapsed_budget(runner, monkeypatch):
    elapsed = [0.0]
    claims = []

    def claim(request):
        claims.append(request)
        return (SimpleNamespace(work_id="original"),)

    async def execute(work):
        elapsed[0] += 500
        return "completed"

    monkeypatch.setattr(runner.monitoring, "claim_work", claim)
    monkeypatch.setattr(runner, "execute_monitoring_work", execute)
    assert await runner.drain_monitoring_work(
        limit=10, budget=HeartbeatBudget(840, 690, lambda: elapsed[0]),
    ) == ["completed"]
    assert len(claims) == 1


async def test_reconciliation_claims_its_own_pool_ahead_of_actions_with_a_short_lease(runner, monkeypatch):
    """Deterministic publication must not queue behind a triage awaiting approval.

    Reconciliation used to share one claim with the action kinds and inherit
    their lease, which is sized for a model call plus an approval wait. It is a
    single SQL transaction, and the connector evidence it publishes expires
    after SUPERSESSION_EVIDENCE_TTL_SECONDS, so that lease outlived the
    evidence and the retry could never succeed.
    """
    claims = []

    def claim(request):
        claims.append(request)
        if request.kinds == ("reconcile_state",):
            return (SimpleNamespace(work_id="reconciliation"),)
        pytest.fail("Actions must not be claimed while reconciliation is available")

    async def execute(work):
        return f"- {work.work_id}: done"

    monkeypatch.setattr(runner.monitoring, "claim_work", claim)
    monkeypatch.setattr(runner, "execute_monitoring_work", execute)

    assert await runner.drain_monitoring_work(limit=1) == ["- reconciliation: done"]

    assert [request.kinds for request in claims] == [("reconcile_state",)]
    assert claims[0].lease_seconds == RECONCILE_LEASE_SECONDS
    assert claims[0].lease_seconds < SUPERSESSION_EVIDENCE_TTL_SECONDS


async def test_actions_are_claimed_when_no_reconciliation_is_due(runner, monkeypatch):
    claims = []

    def claim(request):
        claims.append(request)
        if request.kinds == ("reconcile_state",):
            return ()
        return (SimpleNamespace(work_id="triage-item"),)

    async def execute(work):
        return f"- {work.work_id}: done"

    monkeypatch.setattr(runner.monitoring, "claim_work", claim)
    monkeypatch.setattr(runner, "execute_monitoring_work", execute)

    assert await runner.drain_monitoring_work(limit=1) == ["- triage-item: done"]

    assert [request.kinds for request in claims] == [
        ("reconcile_state",),
        ("triage", "deferred_retry", "verify_action", "finalize"),
    ]
    # Actions keep the long lease: they wait on a model call and an approval.
    assert claims[1].lease_seconds > RECONCILE_LEASE_SECONDS


async def test_an_empty_queue_stops_draining_without_claiming_forever(runner, monkeypatch):
    claims = []

    def claim(request):
        claims.append(request)
        return ()

    monkeypatch.setattr(runner.monitoring, "claim_work", claim)

    assert await runner.drain_monitoring_work(limit=10) == []

    # One probe of each pool, then stop; not ten rounds of both.
    assert [request.kinds for request in claims] == [
        ("reconcile_state",),
        ("triage", "deferred_retry", "verify_action", "finalize"),
    ]


def test_a_reconciliation_lease_cannot_outlive_the_evidence_it_publishes():
    assert RECONCILE_LEASE_SECONDS < SUPERSESSION_EVIDENCE_TTL_SECONDS

