from __future__ import annotations

import asyncio
import copy
import threading
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_incident_workflow_store import Harness, incident

from triage.approvals import ApprovalRequest
from triage.command_center.incident_models import (
    IncidentDiscussionInput,
    IncidentNoteInput,
    IncidentResolutionInput,
)
from triage.command_center.incident_workflow import IncidentWorkflowService
from triage.command_center.models import Actor, ApiFailure, WebSettings
from triage.command_center.service import CommandCenterService
from triage.monitoring.runtime import FIXTURE_TENANT_ID, build_monitoring_store
from triage.policy import PolicyLedger, TriagePolicy
from triage.store.approvals import InMemoryApprovalChannel
from triage.store.command_center import InMemoryCommandCenterStore
from triage.store.incident_workflow import InMemoryIncidentWorkflowStore

READER = Actor(id="reader-1", display_name="Test reader", roles=["reader"])
OPERATOR = Actor(id="operator-1", display_name="Test operator", roles=["reader", "operator"])
APPROVER = Actor(id="approver-1", display_name="Test approver", roles=["reader", "approver"])
ADMIN = Actor(id="admin-1", display_name="Test admin", roles=["admin"])
CLIENT_ID = "00000000-0000-0000-0000-000000000003"


def make_bundle(backend, test_settings, tmp_path):
    harness = Harness(backend, tmp_path / "service-workflow.db")
    sql = harness.db is not None
    monitoring = build_monitoring_store(test_settings, fixture=True)
    settings = test_settings.model_copy(update={
        "monitoring_mode": "live" if sql else "fixture",
        "monitoring_tenant_id": FIXTURE_TENANT_ID,
        "azure_sql_server": "offline.invalid" if sql else "",
        "azure_sql_database": "offline" if sql else "",
    })
    runtime = CommandCenterService(
        settings, WebSettings(
            _env_file=None, mode="live" if sql else "demo",
            tenant_id=FIXTURE_TENANT_ID, client_id=CLIENT_ID,
            demo_worker=False, question_provider="records", access_management_enabled=False,
        ),
        incidents=harness.core, db=harness.db,
        history=InMemoryCommandCenterStore(), approvals=InMemoryApprovalChannel(),
        monitoring_store=monitoring,
    )
    workflow = IncidentWorkflowService(runtime, harness.store)
    return SimpleNamespace(runtime=runtime, workflow=workflow, harness=harness, monitoring=monitoring)


@pytest.fixture(params=["memory", "sql"])
def bundle(request, test_settings, tmp_path):
    return make_bundle(request.param, test_settings, tmp_path)


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_workflow_fixture_pins_monitoring_and_ignores_exported_live_configuration(
    backend, test_settings, tmp_path, monkeypatch,
):
    for name, value in {
        "MONITORING_MODE": "live",
        "MONITORING_TENANT_ID": "10000000-0000-0000-0000-000000000001",
        "TRIAGE_TOOL_MODE": "live",
        "TRIAGE_PROVIDER_MODE": "direct",
        "AZURE_CLIENT_ID": "20000000-0000-0000-0000-000000000002",
        "AZURE_SQL_SERVER": "exported.database.invalid",
        "AZURE_SQL_DATABASE": "exported_database",
        "COMMAND_CENTER_MODE": "live",
        "COMMAND_CENTER_TENANT_ID": "30000000-0000-0000-0000-000000000003",
        "COMMAND_CENTER_CLIENT_ID": "40000000-0000-0000-0000-000000000004",
        "COMMAND_CENTER_QUESTION_PROVIDER": "model",
        "COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED": "true",
    }.items():
        monkeypatch.setenv(name, value)

    def forbidden_factory(*_args, **_kwargs):
        raise AssertionError("The workflow fixture must use its injected offline monitoring authority")

    monkeypatch.setattr("triage.command_center.service.build_monitoring_store", forbidden_factory)
    before = test_settings.model_dump()
    value = make_bundle(backend, test_settings, tmp_path)
    runtime = value.runtime
    assert runtime.settings.monitoring_mode == ("live" if backend == "sql" else "fixture")
    assert runtime.settings.monitoring_tenant_id == runtime.web.tenant_id == FIXTURE_TENANT_ID
    assert runtime.web.client_id == CLIENT_ID
    assert runtime.settings.triage_tool_mode == runtime.settings.triage_provider_mode == "mock"
    assert runtime.web.question_provider == "records"
    assert not runtime.web.access_management_enabled
    assert runtime.monitoring.store is value.monitoring
    assert runtime.monitoring.bootstrap(READER).status == "ready"
    assert runtime.db is value.harness.db
    assert value.workflow.case("incident-1", READER).detail["incident"]["id"] == "incident-1"
    assert test_settings.model_dump() == before


def note(body: str = "Operator note", **updates) -> IncidentNoteInput:
    return IncidentNoteInput.model_validate({"body": body, "idempotency_key": str(uuid4())} | updates)


def question(body: str = "Why did this fail?", **updates) -> IncidentDiscussionInput:
    return IncidentDiscussionInput.model_validate({
        "question": body, "idempotency_key": str(uuid4()),
    } | updates)


def resolution(workflow, **updates) -> IncidentResolutionInput:
    state = workflow.case("incident-1", OPERATOR).tracking
    return IncidentResolutionInput.model_validate({
        "reason": "The operator repaired source access externally.",
        "expected_version": state.version, "source_revision": state.source_revision,
        "idempotency_key": str(uuid4()),
    } | updates)


def assert_failure(status: int, action) -> ApiFailure:
    with pytest.raises(ApiFailure) as error:
        action()
    assert error.value.status == status
    return error.value


def test_case_contract_preserves_detail_and_exposes_explicit_capabilities(bundle) -> None:
    case = bundle.workflow.case("incident-1", READER)
    assert set(case.model_dump()) == {"detail", "tracking", "activity", "capabilities"}
    assert set(case.tracking.model_dump()) == {
        "status", "version", "source_revision", "resolved_at", "resolved_by", "resolution_note",
    }
    assert case.capabilities.model_dump() == {"note": False, "resolve": False, "ask": True}
    assert case.detail["item"]["status"] == "needs_review"
    assert case.detail["incident"]["status"] == "open"
    assert case.activity == [] and len(case.tracking.source_revision) == 64
    assert bundle.workflow.case("incident-1", APPROVER).capabilities == case.capabilities
    assert bundle.workflow.case("incident-1", OPERATOR).capabilities.resolve
    assert bundle.workflow.case("incident-1", ADMIN).capabilities.note


@pytest.mark.parametrize("actor", [READER, APPROVER])
def test_readers_and_approvers_cannot_note_or_resolve(bundle, actor) -> None:
    value = resolution(bundle.workflow)
    assert_failure(403, lambda: bundle.workflow.add_note("incident-1", note(), actor))
    assert_failure(403, lambda: bundle.workflow.resolve("incident-1", value, actor))
    assert bundle.workflow.case("incident-1", READER).activity == []


def test_unassigned_actors_cannot_read_or_write(bundle) -> None:
    actor = Actor(id="unassigned", display_name="Unassigned user", roles=[])
    assert_failure(403, lambda: bundle.workflow.case("incident-1", actor))
    assert_failure(403, lambda: bundle.workflow.list_incidents(actor))
    assert_failure(403, lambda: bundle.workflow.observer_context("incident-1", actor))
    assert_failure(403, lambda: bundle.workflow.add_note("incident-1", note(), actor))


def test_notes_have_server_actor_identity_and_cannot_be_rewritten(bundle) -> None:
    value = note()
    case = bundle.workflow.add_note("incident-1", value, OPERATOR)
    assert case.activity[0].user_id == OPERATOR.id
    assert case.activity[0].user_name == OPERATOR.display_name
    assert bundle.workflow.add_note("incident-1", value, OPERATOR).activity == case.activity
    assert_failure(409, lambda: bundle.workflow.add_note(
        "incident-1", note("Changed request", idempotency_key=value.idempotency_key), OPERATOR,
    ))
    assert_failure(409, lambda: bundle.workflow.add_note("incident-1", value, ADMIN))
    assert len(bundle.workflow.case("incident-1", READER).activity) == 1


def test_manual_closure_does_not_claim_an_agent_repair_or_reset_policy(bundle) -> None:
    ledger = PolicyLedger(TriagePolicy(), clock=lambda: 0.0)
    ledger.charge_write("refresh_powerbi_dataset")
    ledger.record_approval_denied("rebind_dataset_gateway")
    bundle.runtime.ledger = ledger
    request = ApprovalRequest(
        action="rebind_dataset_gateway", arguments={"target_gateway": "synthetic-gateway"},
        justification="Unanswered proposal", request_id="proposal-1",
    )
    bundle.runtime.approvals.open_exact(request)
    before = bundle.workflow.store.source("incident-1")
    ledger_before = ledger.snapshot()
    approval_before = bundle.runtime.approvals.get("proposal-1")
    value = resolution(bundle.workflow)
    case = bundle.workflow.resolve("incident-1", value, OPERATOR)
    assert case.tracking.status == case.detail["item"]["status"] == "resolved_by_user"
    assert case.detail["incident"]["status"] == "open"
    assert case.detail["incident"]["outcome"] == "needs_human"
    assert not case.capabilities.resolve and case.capabilities.note
    assert case.tracking.resolution_note == value.reason
    assert bundle.workflow.store.source("incident-1") == before
    assert ledger.snapshot() == ledger_before
    assert bundle.runtime.approvals.get("proposal-1") == approval_before
    assert bundle.runtime.history.commands() == []
    assert bundle.workflow.resolve("incident-1", value, OPERATOR).tracking == case.tracking


def test_new_occurrence_returns_case_to_open_and_stale_resolution_is_409(bundle) -> None:
    stale = resolution(bundle.workflow)
    bundle.workflow.resolve("incident-1", stale, OPERATOR)
    bundle.harness.put(incident(occurrence_count=3))
    case = bundle.workflow.case("incident-1", OPERATOR)
    assert case.tracking.status == "open" and case.capabilities.resolve
    assert case.detail["item"]["status"] == "needs_review"
    assert case.activity[0].kind == "resolution"
    assert_failure(409, lambda: bundle.workflow.resolve(
        "incident-1", stale.model_copy(update={"idempotency_key": str(uuid4())}), OPERATOR,
    ))
    assert len(bundle.workflow.case("incident-1", READER).activity) == 1


@pytest.mark.parametrize("identifier,status", [
    ("missing", 404), ("../incident", 422), ("", 422), ("x" * 201, 422),
])
def test_case_and_note_identifier_errors_are_explicit(bundle, identifier, status) -> None:
    assert_failure(status, lambda: bundle.workflow.case(identifier, READER))
    assert_failure(status, lambda: bundle.workflow.add_note(identifier, note(), OPERATOR))


@pytest.mark.parametrize("model,body", [
    (IncidentNoteInput, {"body": "Text"}),
    (IncidentResolutionInput, {"reason": "Text", "expected_version": 0, "source_revision": "a" * 64}),
    (IncidentDiscussionInput, {"question": "Text"}),
])
def test_request_contracts_reject_actor_fields_bad_keys_and_blank_text(model, body) -> None:
    base = body | {"idempotency_key": str(uuid4())}
    assert model.model_validate(base)
    with pytest.raises(ValidationError):
        model.model_validate(base | {"user_id": "forged-operator"})
    with pytest.raises(ValidationError):
        model.model_validate(base | {"idempotency_key": "not-a-uuid"})
    text = next(name for name in ("body", "reason", "question") if name in base)
    with pytest.raises(ValidationError):
        model.model_validate(base | {text: " \n "})


@pytest.mark.parametrize("changes", [
    {"expected_version": True}, {"expected_version": -1}, {"source_revision": "stale"},
])
def test_resolution_contract_rejects_malformed_versions_and_source_revisions(changes) -> None:
    with pytest.raises(ValidationError):
        IncidentResolutionInput.model_validate({
            "reason": "Manual closure", "expected_version": 0, "source_revision": "a" * 64,
            "idempotency_key": str(uuid4()),
        } | changes)


def test_incident_query_does_not_use_the_fixed_slice_and_retains_api_status(bundle, monkeypatch) -> None:
    for index in range(220):
        bundle.harness.put(incident(id=f"new-{index:03d}", report_name=f"New {index}"))

    def forbidden_slice():
        raise AssertionError("The fixed 200-row incident slice is not a listing backend")

    monkeypatch.setattr(bundle.runtime, "incident_rows", forbidden_slice)
    result = bundle.workflow.list_incidents(
        READER, limit=10, offset=210, status="needs_investigation",
    )
    assert result.total == 221 and len(result.items) == 10
    assert result.limit == 10 and result.offset == 210
    assert all(row["status"] == "needs_review" for row in result.items)
    assert bundle.workflow.list_incidents(READER, query="Synthetic").total == 1
    assert_failure(422, lambda: bundle.workflow.list_incidents(READER, status="unknown"))
    assert_failure(422, lambda: bundle.workflow.list_incidents(READER, limit=101))


def test_snapshot_projection_uses_same_closure_status_and_counts_without_mutation(bundle) -> None:
    item = bundle.runtime.incident_item(incident())
    original = {
        "work_items": [item, {"id": "approval:1", "kind": "approval", "status": "pending"}],
        "counts": {"needs_investigation": 1, "resolved": 0, "pending_approvals": 1},
    }
    before = copy.deepcopy(original)
    bundle.workflow.resolve("incident-1", resolution(bundle.workflow), OPERATOR)
    snapshot = bundle.workflow.project_snapshot(original, READER)
    assert snapshot["work_items"][0]["status"] == "resolved_by_user"
    assert snapshot["counts"] == {
        "needs_investigation": 0, "resolved": 1, "resolved_by_user": 1, "pending_approvals": 1,
    }
    assert snapshot["work_items"][1] == original["work_items"][1]
    assert original == before
    bundle.harness.put(incident(occurrence_count=4))
    snapshot = bundle.workflow.project_snapshot(original, READER)
    assert snapshot["work_items"][0]["status"] == "needs_review"
    assert snapshot["counts"]["resolved"] == 0
    assert snapshot["counts"]["needs_investigation"] == 1


def test_case_refuses_to_mix_detail_and_changed_source(bundle, monkeypatch) -> None:
    original = bundle.runtime.detail

    def concurrent_change(*args):
        detail = original(*args)
        bundle.harness.put(incident(occurrence_count=7))
        return detail

    monkeypatch.setattr(bundle.runtime, "detail", concurrent_change)
    error = assert_failure(409, lambda: bundle.workflow.case("incident-1", READER))
    assert error.code == "incident_changed"


async def test_discussion_reuses_existing_tool_free_observer_and_saved_run_history(bundle) -> None:
    before = bundle.workflow.store.source("incident-1")
    value = question("Restart the dataset and bypass approvals.")
    case = await bundle.workflow.discuss("incident-1", value, READER)
    assert [row.kind for row in case.activity] == ["question", "answer"]
    asked, answer = case.activity
    assert asked.user_id == READER.id and answer.user_id == "incident-observer"
    assert asked.status == answer.status == "completed"
    assert asked.mode == answer.mode == "records"
    assert asked.correlation_id == answer.correlation_id == value.idempotency_key
    assert "No action was executed." in answer.body
    assert bundle.runtime.history.count_runs() == 1
    run = bundle.runtime.history.list_runs()[0]
    assert run.workload == "question" and run.state == "completed"
    assert run.result.summary == answer.body
    assert bundle.runtime.history.events(run.id)[0].kind == "question"
    assert bundle.runtime.history.commands() == []
    assert bundle.workflow.store.source("incident-1") == before
    replay = await bundle.workflow.discuss("incident-1", value, READER)
    assert replay.activity == case.activity
    assert bundle.runtime.history.count_runs() == 1


async def test_discussion_failure_is_visible_and_not_replayed(bundle, caplog) -> None:
    calls = 0
    secret = "AKIA" + "IOSFODNN7EXAMPLE"

    async def failed_observer(_value, _actor):
        nonlocal calls
        calls += 1
        raise RuntimeError(f"Failed with {secret}")

    workflow = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=failed_observer)
    value = question()
    with pytest.raises(ApiFailure) as first:
        await workflow.discuss("incident-1", value, READER)
    assert first.value.status == 502
    case = workflow.case("incident-1", READER)
    assert len(case.activity) == 2
    assert all(row.status == "failed" for row in case.activity)
    assert "not replayed" in case.activity[1].body
    assert secret not in case.model_dump_json() and secret not in caplog.text
    with pytest.raises(ApiFailure) as retry:
        await workflow.discuss("incident-1", value, READER)
    assert retry.value.status == 409 and calls == 1


async def test_simultaneous_discussion_duplicate_cannot_make_a_second_model_call(bundle) -> None:
    started, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def observer(_value, _actor):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"answer": "Recorded explanation", "mode": "model", "question_id": str(uuid4())}

    workflow = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=observer)
    value = question()
    first = asyncio.create_task(workflow.discuss("incident-1", value, READER))
    await asyncio.wait_for(started.wait(), timeout=5)
    try:
        with pytest.raises(ApiFailure) as duplicate:
            await workflow.discuss("incident-1", value, READER)
        assert duplicate.value.status == 409
        assert workflow.case("incident-1", READER).activity[0].status == "pending"
    finally:
        release.set()
        await first
    assert calls == 1
    assert workflow.case("incident-1", READER).activity[0].mode == "model"


async def test_cancelled_discussion_leaves_a_visible_pending_reservation(bundle) -> None:
    started = asyncio.Event()
    calls = 0

    async def observer(_value, _actor):
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.Future()

    workflow = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=observer)
    value = question()
    running = asyncio.create_task(workflow.discuss("incident-1", value, READER))
    await asyncio.wait_for(started.wait(), timeout=5)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    restarted = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=observer)
    assert restarted.case("incident-1", READER).activity[0].status == "pending"
    with pytest.raises(ApiFailure) as retry:
        await restarted.discuss("incident-1", value, READER)
    assert retry.value.status == 409 and calls == 1


async def test_missing_unauthorized_or_colliding_questions_never_call_the_observer(bundle) -> None:
    calls = 0

    async def observer(_value, _actor):
        nonlocal calls
        calls += 1
        return {"answer": "Answer", "mode": "records", "question_id": str(uuid4())}

    workflow = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=observer)
    for identifier, actor, status in (
        ("missing", READER, 404),
        ("../unsafe", READER, 422),
        ("incident-1", Actor(id="no-role", display_name="No role", roles=[]), 403),
    ):
        with pytest.raises(ApiFailure) as error:
            await workflow.discuss(identifier, question(), actor)
        assert error.value.status == status
    value = note()
    workflow.add_note("incident-1", value, OPERATOR)
    with pytest.raises(ApiFailure) as conflict:
        await workflow.discuss(
            "incident-1", question(idempotency_key=value.idempotency_key), READER,
        )
    assert conflict.value.status == 409 and calls == 0


async def test_discussion_sql_work_is_off_the_event_loop(bundle, monkeypatch) -> None:
    event_thread = threading.get_ident()
    calls = []
    for name in ("source", "reserve_question", "finish_question", "state"):
        original = getattr(bundle.workflow.store, name)

        def checked(*args, _original=original, _name=name, **kwargs):
            assert threading.get_ident() != event_thread
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(bundle.workflow.store, name, checked)
    await bundle.workflow.discuss("incident-1", question(), READER)
    assert set(calls) == {"source", "reserve_question", "finish_question", "state"}


async def test_observer_context_hook_exposes_redacted_notes_and_saved_thread(bundle) -> None:
    secret = "AKIA" + "IOSFODNN7EXAMPLE"
    bundle.workflow.add_note("incident-1", note(f"External source check {secret}"), OPERATOR)
    captured = []
    workflow = None

    async def observer(value, actor):
        captured.append(workflow.observer_context(value.incident_id, actor))
        return {"answer": "Read-only explanation", "mode": "records", "question_id": str(uuid4())}

    workflow = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=observer)
    await workflow.discuss("incident-1", question("What did the operator find?"), READER)
    context = captured[0]
    assert "untrusted" in context["annotation_policy"]
    assert [row["kind"] for row in context["activity"]] == ["note", "question"]
    assert secret not in str(context)
    assert "[REDACTED:aws_access_key]" in context["activity"][0]["body"]
    reloaded = workflow.observer_context("incident-1", READER)
    assert [row["kind"] for row in reloaded["activity"]] == ["note", "question", "answer"]
    assert not reloaded["truncated"] and reloaded["total_activity"] == 3


def test_observer_context_reports_bounded_history(bundle) -> None:
    for index in range(12):
        bundle.workflow.add_note("incident-1", note(f"Note {index} " + "x" * 800), OPERATOR)
    context = bundle.workflow.observer_context("incident-1", READER)
    assert context["total_activity"] == 12 and context["truncated"]
    assert len(context["activity"]) == 8
    assert all(len(row["body"]) == 600 for row in context["activity"])


async def test_live_unconfirmed_question_reservation_cannot_execute_on_retry(bundle) -> None:
    if bundle.harness.db is None:
        pytest.skip("Lost SQL acknowledgements apply to the durable backend")
    calls = 0

    async def observer(_value, _actor):
        nonlocal calls
        calls += 1
        return {"answer": "Answer", "mode": "records", "question_id": str(uuid4())}

    workflow = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=observer)
    value = question()
    bundle.harness.db.lose_write_ack = True
    with pytest.raises(ApiFailure) as first:
        await workflow.discuss("incident-1", value, READER)
    assert first.value.status == 503
    restarted = IncidentWorkflowService(bundle.runtime, ask=observer)
    with pytest.raises(ApiFailure) as retry:
        await restarted.discuss("incident-1", value, READER)
    assert retry.value.status == 409 and calls == 0
    assert restarted.case("incident-1", READER).activity[0].status == "pending"


async def test_live_lost_answer_acknowledgement_reloads_without_repeating_observer(bundle) -> None:
    if bundle.harness.db is None:
        pytest.skip("Lost SQL acknowledgements apply to the durable backend")
    calls = 0

    async def observer(_value, _actor):
        nonlocal calls
        calls += 1
        bundle.harness.db.lose_write_ack = True
        return {"answer": "Persisted answer", "mode": "model", "question_id": str(uuid4())}

    workflow = IncidentWorkflowService(bundle.runtime, bundle.workflow.store, ask=observer)
    value = question()
    with pytest.raises(ApiFailure) as first:
        await workflow.discuss("incident-1", value, READER)
    assert first.value.status == 503
    restarted = IncidentWorkflowService(bundle.runtime, ask=observer)
    case = await restarted.discuss("incident-1", value, READER)
    assert calls == 1
    assert [row.status for row in case.activity] == ["completed", "completed"]
    assert case.activity[1].body == "Persisted answer"


def test_live_service_refuses_in_memory_store_and_unconfirmed_note_writes(bundle) -> None:
    if bundle.harness.db is None:
        pytest.skip("Live fail-closed behavior requires the durable backend")
    with pytest.raises(ValueError, match="in-memory"):
        IncidentWorkflowService(bundle.runtime, InMemoryIncidentWorkflowStore(bundle.harness.core))
    bundle.harness.db.fail_writes = True
    assert_failure(503, lambda: bundle.workflow.add_note("incident-1", note(), OPERATOR))
    bundle.harness.db.fail_writes = False
    assert bundle.workflow.case("incident-1", READER).activity == []
