from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from triage.command_center.api import create_app
from triage.command_center.models import Actor, AskInput, WebSettings
from triage.command_center.service import CommandCenterService, _observer_message
from triage.models import TriageResult

USER_ID = "10000000-0000-0000-0000-000000000001"


@pytest.fixture
def runtime(test_settings):
    service = CommandCenterService(test_settings, WebSettings(mode="demo", demo_worker=False))
    incident = service.incidents.record(
        TriageResult(outcome="needs_human", signature="case-integration", request_id="case-request"),
        report_name="Synthetic integration case", original_error="Source authentication failed.",
    )
    return service, incident


def client_for(service, roles):
    class Verifier:
        def verify(self, _token):
            return Actor(id=USER_ID, display_name="Validated user", roles=list(roles))

    return TestClient(create_app(
        service, web_settings=WebSettings(mode="live"), token_verifier=Verifier(),
    ), raise_server_exceptions=False)


def test_incident_notes_and_resolution_use_verified_identity_and_update_queue_projection(runtime):
    service, original = runtime
    headers = {"Authorization": "Bearer test"}
    path = f"/api/incidents/{original.id}"
    with client_for(service, ["reader", "operator"]) as client:
        listing = client.get("/api/incidents", headers=headers)
        assert listing.status_code == 200
        assert listing.json()["total"] == 1
        assert listing.json()["items"][0]["source_id"] == original.id
        detail = client.get(path, headers=headers)
        assert detail.status_code == 200
        note = {"body": "Reviewed the upstream access configuration.", "idempotency_key": str(uuid4())}
        assert client.post(f"{path}/notes", headers=headers, json=note | {"user_id": "forged"}).status_code == 422
        saved = client.post(f"{path}/notes", headers=headers, json=note)
        assert saved.status_code == 200
        entry = next(item for item in saved.json()["activity"] if item["kind"] == "note")
        assert entry["user_id"] == USER_ID and entry["user_name"] == "Validated user"
        assert client.post(f"{path}/notes", headers=headers, json=note).status_code == 200
        case = client.get(path, headers=headers).json()
        assert len([item for item in case["activity"] if item["kind"] == "note"]) == 1
        tracking = case["tracking"]
        resolved = client.post(f"{path}/resolution", headers=headers, json={
            "reason": "The upstream access was corrected and reviewed by the operator.",
            "expected_version": tracking["version"], "source_revision": tracking["source_revision"],
            "idempotency_key": str(uuid4()),
        })
        assert resolved.status_code == 200
        assert resolved.json()["tracking"]["status"] == "resolved_by_user"
        snapshot = client.get("/api/snapshot", headers=headers).json()
        assert snapshot["counts"]["needs_investigation"] == 0
        assert snapshot["counts"]["resolved"] == 1
        assert snapshot["work_items"][0]["status"] == "resolved_by_user"
    assert service.incidents.get(original.id) == original, "User tracking must not rewrite automated evidence"


def test_new_evidence_reopens_tracking_without_resetting_the_controller_incident(runtime):
    service, incident = runtime
    headers = {"Authorization": "Bearer test"}
    path = f"/api/incidents/{incident.id}"
    with client_for(service, ["admin", "reader"]) as client:
        current = client.get(path, headers=headers)
        assert current.status_code == 200
        tracking = current.json()["tracking"]
        resolved = client.post(f"{path}/resolution", headers=headers, json={
            "reason": "Operator reviewed the recorded issue.",
            "expected_version": tracking["version"], "source_revision": tracking["source_revision"],
            "idempotency_key": str(uuid4()),
        })
        assert resolved.status_code == 200
        newer = service.incidents.record(
            TriageResult(outcome="needs_human", signature=incident.signature, request_id="new-occurrence"),
            report_name=incident.report_name, original_error="The source failed again.",
        )
        assert newer.id == incident.id and newer.occurrence_count > incident.occurrence_count
        assert client.get(path, headers=headers).json()["tracking"]["status"] == "open"
        snapshot = client.get("/api/snapshot", headers=headers).json()
        assert snapshot["counts"]["needs_investigation"] == 1
        assert snapshot["counts"]["resolved"] == 0


def test_readers_cannot_write_notes_or_resolve_cases(runtime):
    service, incident = runtime
    headers = {"Authorization": "Bearer test"}
    path = f"/api/incidents/{incident.id}"
    with client_for(service, ["reader"]) as client:
        current = client.get(path, headers=headers)
        assert current.status_code == 200
        tracking = current.json()["tracking"]
        assert client.post(f"{path}/notes", headers=headers, json={
            "body": "Unauthorized note", "idempotency_key": str(uuid4()),
        }).status_code == 403
        assert client.post(f"{path}/resolution", headers=headers, json={
            "reason": "Unauthorized resolution", "expected_version": tracking["version"],
            "source_revision": tracking["source_revision"], "idempotency_key": str(uuid4()),
        }).status_code == 403


def test_profile_policy_allows_only_the_explicit_graph_origin(runtime):
    service, _ = runtime
    with client_for(service, ["reader"]) as client:
        response = client.get("/api/config")
    directives = {
        key: values.split() for part in response.headers["Content-Security-Policy"].split(";")
        if part.strip() for key, values in [part.strip().split(" ", 1)]
    }
    assert "https://graph.microsoft.com" in directives["connect-src"]
    assert "*" not in directives["connect-src"]
    assert "blob:" in directives["img-src"]
    assert directives["script-src"] == ["'self'"]


def test_discussion_saves_both_sides_includes_notes_and_does_not_repeat_on_retry(runtime):
    service, incident = runtime
    headers = {"Authorization": "Bearer test"}
    path = f"/api/incidents/{incident.id}"
    with client_for(service, ["reader", "operator"]) as client:
        note = client.post(f"{path}/notes", headers=headers, json={
            "body": "Synthetic note: the upstream configuration was reviewed.",
            "idempotency_key": str(uuid4()),
        })
        assert note.status_code == 200
        body = {"question": "What has been recorded?", "idempotency_key": str(uuid4())}
        answer = client.post(f"{path}/discussion", headers=headers, json=body)
        assert answer.status_code == 200
        activity = answer.json()["activity"]
        question = next(row for row in activity if row["id"] == body["idempotency_key"])
        reply = next(row for row in activity if row["kind"] == "answer")
        assert question["status"] == reply["status"] == "completed"
        assert reply["correlation_id"] == question["id"]
        assert reply["mode"] == "records" and "upstream configuration was reviewed" in reply["body"]
        assert client.post(f"{path}/discussion", headers=headers, json=body).status_code == 200
        assert len(service.history.list_runs()) == 1


def test_observer_budget_keeps_valid_json_and_human_notes_without_mutating_sources():
    evidence = {"large_diagnostics": "recorded " * 10000}
    collaboration = {"activity": [{"kind": "note", "body": "Reviewed the source configuration."}], "truncated": False}
    original = json.dumps(collaboration)
    content = _observer_message("Summarize the notes.", evidence, collaboration)
    value = json.loads(content)
    assert len(content) <= 20000
    assert value["collaboration"]["activity"] == collaboration["activity"]
    assert value["recorded_evidence"]["truncated"] is True
    assert json.dumps(collaboration) == original


def test_observer_budget_marks_omitted_history_instead_of_cutting_json():
    collaboration = {
        "activity": [{"kind": "note", "body": "\0" * 4000} for _ in range(8)],
        "truncated": False,
    }
    value = _observer_message("Question", {"diagnostics": "evidence"}, collaboration)
    parsed = json.loads(value)
    assert len(value) <= 20000
    assert parsed["collaboration"]["truncated"] is True
    assert len(parsed["collaboration"]["activity"]) < len(collaboration["activity"])


@pytest.mark.asyncio
async def test_observer_trims_padding_before_store_redaction(runtime, monkeypatch):
    service, incident = runtime
    service.web = WebSettings(mode="live", question_provider="model")
    captured = []

    class Provider:
        async def complete(self, *, messages, tools):
            assert tools == []
            captured.append(json.loads(messages[-1]["content"]))
            return SimpleNamespace(wants_tools=False, content=" " * 25000 + "A useful recorded answer.")

        async def close(self):
            pass

    monkeypatch.setattr("triage.command_center.service.get_provider", lambda *_args: Provider())
    response = await service.ask(
        AskInput(incident_id=incident.id, question="What is recorded?"),
        Actor(id=USER_ID, display_name="Validated reader", roles=["reader"]),
    )
    assert response["answer"] == "A useful recorded answer."
    assert "collaboration" in captured[0]
