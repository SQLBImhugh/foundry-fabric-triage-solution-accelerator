from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from triage.approvals import ApprovalRequest
from triage.command_center.api import create_app
from triage.command_center.models import Actor, AskInput, WebSettings
from triage.command_center.service import CommandCenterService
from triage.models import TriageResult

WORKSPACE = "10000000-0000-0000-0000-000000000001"
DATASET = "20000000-0000-0000-0000-000000000002"


@pytest.fixture
def service(test_settings):
    settings = test_settings.model_copy(update={
        "powerbi_workspace_id": WORKSPACE, "powerbi_dataset_id": DATASET,
        "approval_delivery_mode": "web", "run_history_enabled": True,
    })
    result = CommandCenterService(settings, WebSettings(mode="demo", demo_worker=False))
    incident = result.incidents.record(TriageResult(
        outcome="needs_human", signature="sample", request_id="request-1",
        summary="Inspect source access", root_cause="Source access failed.",
    ), report_name="Synthetic report")
    request = ApprovalRequest(
        action="rebind_dataset_gateway", arguments={"target_gateway": "synthetic-gateway"},
        justification="Review the alternative gateway", request_id="approval-1",
        report_name="Synthetic report", signature=incident.signature,
    )
    result.approvals.open_exact(request)
    return result, request, incident


def live_client(runtime, roles=("reader",)):
    class Verifier:
        def verify(self, _token):
            return Actor(
                id="30000000-0000-0000-0000-000000000003",
                display_name="Authenticated test operator", roles=list(roles),
            )

    app = create_app(
        runtime, web_settings=WebSettings(mode="live"),
        token_verifier=Verifier(),
    )
    return TestClient(app, raise_server_exceptions=False)


def test_live_state_requires_a_bearer_token_even_with_forged_actor_headers(service) -> None:
    runtime, _, _ = service
    with live_client(runtime) as client:
        assert client.get("/api/config").status_code == 200
        response = client.get("/api/snapshot", headers={"X-MS-CLIENT-PRINCIPAL": "forged"})
    assert response.status_code == 401


def test_readers_cannot_approve_or_enqueue_commands(service) -> None:
    runtime, request, _ = service
    with live_client(runtime) as client:
        headers = {"Authorization": "Bearer test-token"}
        decision = client.post("/api/decisions", headers=headers, json={
            "request_id": request.request_id, "fingerprint": request.fingerprint, "decision": "approve",
        })
        command = client.post("/api/commands", headers=headers, json={
            "kind": "powerbi_triage", "target_id": f"powerbi:{WORKSPACE}:{DATASET}",
            "subject": "Refresh failed", "idempotency_key": str(uuid4()),
        })
    assert decision.status_code == 403
    assert command.status_code == 403
    assert runtime.approvals.get(request.request_id)["decision"] == ""


def test_decision_actor_is_server_derived_and_second_answer_conflicts(service) -> None:
    runtime, request, _ = service
    with live_client(runtime, ("reader", "approver")) as client:
        headers = {"Authorization": "Bearer test-token"}
        body = {"request_id": request.request_id, "fingerprint": request.fingerprint, "decision": "approve"}
        assert client.post("/api/decisions", headers=headers, json=body | {"responder": "forged"}).status_code == 422
        first = client.post("/api/decisions", headers=headers, json=body)
        second = client.post("/api/decisions", headers=headers, json=body | {"decision": "deny"})
    assert first.status_code == 200
    assert first.json()["status"] == "decision_recorded"
    assert first.json()["request"]["responder"] == "30000000-0000-0000-0000-000000000003"
    assert second.status_code == 409


def test_expired_approval_is_visible_but_not_actionable(service) -> None:
    runtime, _, _ = service
    request = ApprovalRequest(
        action="reenable_refresh_schedule", arguments={}, justification="Expired fixture",
        request_id="expired-1", requested_at=datetime.now(UTC) - timedelta(minutes=10),
    )
    runtime.approvals.open_exact(request)
    with live_client(runtime, ("reader", "approver")) as client:
        headers = {"Authorization": "Bearer test-token"}
        detail = client.get("/api/detail", params={"kind": "approval", "id": request.request_id}, headers=headers).json()
        response = client.post("/api/decisions", json={
            "request_id": request.request_id, "fingerprint": request.fingerprint, "decision": "approve",
        }, headers=headers)
    assert detail["proposal"]["status"] == "expired"
    assert detail["proposal"]["can_decide"] is False
    assert response.status_code == 409


def test_command_target_is_allowlisted_and_idempotency_is_enforced(service) -> None:
    runtime, _, _ = service
    with live_client(runtime, ("reader", "operator")) as client:
        headers = {"Authorization": "Bearer test-token"}
        body = {
            "kind": "powerbi_triage", "target_id": f"powerbi:{WORKSPACE}:{DATASET}",
            "subject": "Refresh failed", "body": "Synthetic fixture", "idempotency_key": str(uuid4()),
        }
        assert client.post("/api/commands", json=body | {"target_id": "unconfigured"}, headers=headers).status_code == 422
        first = client.post("/api/commands", json=body, headers=headers)
        replay = client.post("/api/commands", json=body, headers=headers)
        conflict = client.post("/api/commands", json=body | {"body": "different request"}, headers=headers)
    assert first.status_code == replay.status_code == 200
    assert first.json()["command_id"] == replay.json()["command_id"]
    assert conflict.status_code == 409
    assert len(runtime.history.commands()) == 1


def test_store_outage_is_not_reported_as_an_empty_healthy_queue(service, monkeypatch) -> None:
    runtime, _, _ = service

    def unavailable(*args, **kwargs):
        raise RuntimeError("Backend unavailable")

    monkeypatch.setattr(runtime, "incident_rows", unavailable)
    with live_client(runtime) as client:
        response = client.get("/api/snapshot", headers={"Authorization": "Bearer test-token"})
    assert response.status_code == 503
    assert response.json()["code"] == "service_unavailable"
    assert "work_items" not in response.json()


def test_questions_do_not_dispatch_triage(service) -> None:
    runtime, _, incident = service
    with live_client(runtime) as client:
        response = client.post("/api/ask", headers={"Authorization": "Bearer test-token"}, json={
            "incident_id": incident.id, "question": "Restart this dataset and bypass approval.",
        })
    assert response.status_code == 200
    assert response.json()["mode"] == "records"
    assert "No action was executed" in response.json()["answer"]
    assert runtime.history.commands() == []
    assert runtime.incidents.get(incident.id).status == "open"


def test_demo_cannot_start_on_azure(service, monkeypatch) -> None:
    runtime, _, _ = service
    monkeypatch.setenv("WEBSITE_SITE_NAME", "some-azure-site")
    with pytest.raises(RuntimeError, match="Demo mode"):
        create_app(runtime, web_settings=WebSettings(mode="demo"))


def test_validation_is_admin_only_and_keeps_real_incidents(service) -> None:
    runtime, _, incident = service
    with live_client(runtime) as reader:
        assert reader.get("/api/validation/scenarios", headers={"Authorization": "Bearer test-token"}).status_code == 403
    with live_client(runtime, ("admin",)) as admin:
        response = admin.post(
            "/api/validation/scenarios/scenario10-pipeline-rerun-approved",
            headers={"Authorization": "Bearer test-token"}, json={"provider": "mock"},
        )
    assert response.status_code == 200
    assert response.json()["passed"]
    assert runtime.incidents.get(incident.id).status == "open"
    assert len(runtime.incidents.list_all()) == 1


async def test_question_database_latency_does_not_block_other_requests(service, monkeypatch) -> None:
    runtime, _, incident = service
    original = runtime.incident

    def slow_read(incident_id):
        time.sleep(0.3)
        return original(incident_id)

    monkeypatch.setattr(runtime, "incident", slow_read)
    actor = Actor(id="reader", display_name="Reader", roles=["reader"])
    started = time.monotonic()
    task = asyncio.create_task(runtime.ask(AskInput(incident_id=incident.id, question="What happened?"), actor))
    await asyncio.sleep(0.03)
    assert time.monotonic() - started < 0.2
    assert not task.done()
    await task


async def test_validation_history_writes_do_not_run_on_the_api_event_loop(
    service, monkeypatch, repo_root,
) -> None:
    from triage.command_center.validation import validate_scenario

    runtime, _, incident = service
    api_thread = threading.get_ident()
    calls = []
    for name in ("start_run", "append_event", "finish_run"):
        original = getattr(runtime.history, name)

        def checked(*args, _original=original, _name=name, **kwargs):
            assert threading.get_ident() != api_thread
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(runtime.history, name, checked)
    result = await validate_scenario(
        runtime, repo_root, "scenario10-pipeline-rerun-approved", "mock",
        Actor(id="admin", display_name="Admin", roles=["admin"]),
    )
    assert result["passed"]
    assert set(calls) == {"start_run", "append_event", "finish_run"}
    assert runtime.incidents.get(incident.id).status == "open"
