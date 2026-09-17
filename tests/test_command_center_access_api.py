from __future__ import annotations

import copy
import time
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from test_command_center_api import TARGET
from test_command_center_api import service as service
from test_command_center_auth import CLIENT, TENANT, USER
from test_command_center_auth import signing as signing

from triage.command_center.api import create_app
from triage.command_center.models import Actor, WebSettings
from triage.command_center.service import CommandCenterService
from triage.store.approvals import InMemoryApprovalChannel
from triage.store.command_center import InMemoryCommandCenterStore

ACCESS_FIELDS = {
    "source", "current_user", "role_catalog", "tenant_id", "application_id",
    "token_issued_at", "token_expires_at", "management_url",
}
RETIRED_ROUTES = [
    ("GET", "/api/admin/users"), ("POST", "/api/admin/users"), ("GET", "/api/admin/audit"),
]


def live_client(runtime, verifier):
    return TestClient(create_app(
        runtime, token_verifier=verifier, web_settings=WebSettings(
            _env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT,
        ),
    ), raise_server_exceptions=False)


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


def test_access_reports_only_effective_token_roles_and_validated_token_dates(service, signing):
    runtime, _, _ = service
    verifier, token = signing
    issued_at = int(time.time()) - 30
    expires_at = issued_at + 300
    signed = token(iat=issued_at, exp=expires_at, roles=["CommandCenter.Operator"], groups=["unused"])
    with live_client(runtime, verifier) as client:
        response = client.get("/api/access", headers=bearer(signed))
        assert response.status_code == 200
        body = response.json()
        assert set(body) == ACCESS_FIELDS
        assert body["source"] == "entra_app_roles"
        assert body["current_user"] == {
            "id": USER, "display_name": "Test operator", "roles": ["operator", "reader"],
        }
        assert body["tenant_id"] == TENANT and body["application_id"] == CLIENT
        assert datetime.fromisoformat(body["token_issued_at"]) == datetime.fromtimestamp(issued_at, UTC)
        assert datetime.fromisoformat(body["token_expires_at"]) == datetime.fromtimestamp(expires_at, UTC)
        assert body["management_url"] == "https://entra.microsoft.com/"
        assert [role["id"] for role in body["role_catalog"]] == ["reader", "operator", "approver", "admin"]
        assert all(set(role) == {"id", "label", "description"} for role in body["role_catalog"])
        assert response.headers["Cache-Control"] == "no-store"
        snapshot = client.get("/api/snapshot", headers=bearer(signed))
        assert snapshot.status_code == 200
        assert snapshot.json()["actor"] == body["current_user"]
        assert client.post("/api/access", headers=bearer(signed), json={}).status_code == 405
        assert signed not in response.text


def test_access_describes_the_presented_token_not_current_group_membership(service, signing):
    runtime, _, _ = service
    verifier, token = signing
    old_token = token(roles=["CommandCenter.Admin"], iat=int(time.time()) - 60)
    new_token = token(roles=["CommandCenter.Reader"])
    with live_client(runtime, verifier) as client:
        for signed, roles in ((old_token, ["admin", "reader"]), (new_token, ["reader"]), (old_token, ["admin", "reader"])):
            response = client.get("/api/access", headers=bearer(signed))
            assert response.status_code == 200
            assert response.json()["current_user"]["roles"] == roles
        expired = token(roles=["CommandCenter.Admin"], iat=1, nbf=1, exp=2)
        assert client.get("/api/access", headers=bearer(expired)).status_code == 401


@pytest.mark.parametrize("method,path", [("GET", "/api/access"), *RETIRED_ROUTES])
def test_access_and_retired_routes_require_a_valid_role_bearing_token(service, signing, method, path):
    runtime, _, _ = service
    verifier, token = signing
    with live_client(runtime, verifier) as client:
        assert client.request(method, path, headers={
            "X-MS-CLIENT-PRINCIPAL": '{"id":"forged","roles":["admin"]}',
            "X-User-Id": USER, "X-User-Roles": "CommandCenter.Admin",
        }).status_code == 401
        assert client.request(method, path, headers=bearer("forged")).status_code == 401
        assert client.request(method, path, headers=bearer(token(roles=[]))).status_code == 403


@pytest.mark.parametrize("role", ["Reader", "Operator", "Approver", "Admin"])
@pytest.mark.parametrize("method,path", RETIRED_ROUTES)
def test_legacy_permission_operations_fail_explicitly_for_every_authenticated_role(
    service, signing, role, method, path,
):
    runtime, _, _ = service
    verifier, token = signing
    with live_client(runtime, verifier) as client:
        response = client.request(
            method, path, headers=bearer(token(roles=[f"CommandCenter.{role}"])),
            json={"user_id": USER, "roles": ["admin"], "enabled": True, "expected_version": 1},
        )
    assert response.status_code == 410
    assert response.json()["code"] == "managed_in_entra"
    assert "Microsoft Entra" in response.json()["message"]


def test_retired_user_writes_do_not_change_operational_incidents_notes_history_or_approvals(
    service, signing,
):
    runtime, request, incident = service
    verifier, token = signing
    headers = bearer(token(roles=["CommandCenter.Admin"]))
    with live_client(runtime, verifier) as client:
        path = f"/api/incidents/{incident.id}"
        assert client.post(f"{path}/notes", headers=headers, json={
            "body": "Preserve this incident note.", "idempotency_key": str(uuid4()),
        }).status_code == 200
        assert client.post("/api/ask", headers=headers, json={
            "incident_id": incident.id, "question": "What evidence is recorded?",
        }).status_code == 200
        case_before = client.get(path, headers=headers).json()
        history_before = copy.deepcopy(runtime.history.list_runs())
        approval_before = copy.deepcopy(runtime.approvals.get(request.request_id))
        for method, endpoint in RETIRED_ROUTES:
            assert client.request(method, endpoint, headers=headers, json={
                "user_id": USER, "roles": [], "enabled": False,
                "reason": "Retired local grant change.", "idempotency_key": str(uuid4()),
            }).status_code == 410
        assert client.get(path, headers=headers).json() == case_before
    assert runtime.incidents.get(incident.id) == incident
    assert runtime.history.list_runs() == history_before
    assert runtime.approvals.get(request.request_id) == approval_before


def test_entra_authorization_never_reads_sql_or_honors_a_stale_local_grant(test_settings, signing):
    verifier, token = signing

    class NoSql:
        def query(self, *_args, **_kwargs):
            raise AssertionError("Authorization must not query operational or permission tables")

        def execute(self, *_args, **_kwargs):
            raise AssertionError("Authorization must not change SQL state")

    class StaleAccess:
        def resolve(self, _identity):
            raise AssertionError("A stale local administrator grant must never be consulted")

    web = WebSettings(_env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT)
    runtime = CommandCenterService(
        test_settings.model_copy(update={
            "azure_sql_server": "offline.database.windows.net",
            "azure_sql_database": "offline_state",
            "monitoring_mode": "live", "monitoring_tenant_id": TENANT,
        }),
        web, db=NoSql(), history=InMemoryCommandCenterStore(), approvals=InMemoryApprovalChannel(),
    )
    assert not hasattr(runtime, "access") and not hasattr(runtime, "demo_identity")
    runtime.access = StaleAccess()
    with live_client(runtime, verifier) as client:
        response = client.get("/api/access", headers=bearer(token(roles=["CommandCenter.Reader"])))
        assert response.status_code == 200
        assert response.json()["current_user"]["roles"] == ["reader"]
        assert client.get("/api/knowledge", headers=bearer(token())).status_code == 200
        for roles in ([], ["UnrecognizedRole"]):
            denied = client.get("/api/access", headers=bearer(token(roles=roles)))
            assert denied.status_code == 403


@pytest.mark.parametrize("role,operator_status,approver_status", [
    ("Reader", 403, 403), ("Operator", 200, 403), ("Approver", 403, 200), ("Admin", 200, 200),
])
def test_real_role_tokens_keep_backend_operation_guards(
    service, signing, role, operator_status, approver_status,
):
    runtime, request, incident = service
    verifier, token = signing
    headers = bearer(token(roles=[f"CommandCenter.{role}"]))
    with live_client(runtime, verifier) as client:
        assert client.get("/api/snapshot", headers=headers).status_code == 200
        assert client.post("/api/ask", headers=headers, json={
            "incident_id": incident.id, "question": "What happened?",
        }).status_code == 200
        command = client.post("/api/commands", headers=headers, json={
            "kind": "powerbi_triage", "target_id": TARGET.key,
            "subject": "Synthetic failure", "idempotency_key": str(uuid4()),
        })
        assert command.status_code == operator_status
        path = f"/api/incidents/{incident.id}"
        note = client.post(f"{path}/notes", headers=headers, json={
            "body": "Synthetic investigation note.", "idempotency_key": str(uuid4()),
        })
        assert note.status_code == operator_status
        tracking = client.get(path, headers=headers).json()["tracking"]
        resolution = client.post(f"{path}/resolution", headers=headers, json={
            "reason": "Synthetic operator tracking decision.",
            "expected_version": tracking["version"], "source_revision": tracking["source_revision"],
            "idempotency_key": str(uuid4()),
        })
        assert resolution.status_code == operator_status
        decision = client.post("/api/decisions", headers=headers, json={
            "request_id": request.request_id, "fingerprint": request.fingerprint, "decision": "approve",
        })
        assert decision.status_code == approver_status
        scenarios = client.get("/api/validation/scenarios", headers=headers)
        assert scenarios.status_code == (200 if role == "Admin" else 403)
    assert runtime.incidents.get(incident.id) == incident


def test_demo_uses_a_fixed_synthetic_actor_without_entra_token_metadata(service):
    runtime, _, _ = service
    web = WebSettings(_env_file=None, mode="demo", tenant_id=TENANT, client_id=CLIENT)
    with TestClient(create_app(runtime, web_settings=web)) as client:
        response = client.get("/api/access")
        assert response.status_code == 200
        body = response.json()
        assert set(body) == ACCESS_FIELDS
        assert body["source"] == "synthetic_demo"
        assert body["current_user"] == {
            "id": "00000000-0000-0000-0000-000000000002",
            "display_name": "Synthetic demo operator", "roles": ["admin", "reader"],
        }
        for field in ("tenant_id", "application_id", "token_issued_at", "token_expires_at"):
            assert body[field] is None
        assert client.get("/api/snapshot").json()["actor"] == body["current_user"]
        assert client.get("/api/config").json()["auth"]["authorization_source"] == "synthetic_demo"
        assert client.get("/api/admin/users").status_code == 410


def test_public_config_names_entra_as_the_only_live_authority(service, signing):
    runtime, _, _ = service
    verifier, _ = signing
    with live_client(runtime, verifier) as client:
        response = client.get("/api/config")
    assert response.status_code == 200
    assert response.json()["auth"] == {
        "enabled": True, "tenant_id": TENANT, "client_id": CLIENT,
        "scope": f"api://{CLIENT}/access_as_user", "authorization_source": "entra_app_roles",
    }


def test_server_owned_verifier_seam_cannot_invent_token_timestamps(service):
    runtime, _, _ = service

    class Verifier:
        def verify(self, _token):
            return Actor(id=USER, display_name="Offline test actor", roles=["reader"])

    with live_client(runtime, Verifier()) as client:
        response = client.get("/api/access", headers=bearer("offline-fixture"))
    assert response.status_code == 200
    assert response.json()["token_issued_at"] is None
    assert response.json()["token_expires_at"] is None


def test_malformed_verifier_output_fails_explicitly_instead_of_becoming_an_actor(service):
    runtime, _, _ = service

    class Verifier:
        def verify(self, _token):
            return {"id": USER, "roles": ["admin"]}

    with live_client(runtime, Verifier()) as client:
        response = client.get("/api/access", headers=bearer("offline-fixture"))
    assert response.status_code == 401
    assert response.json()["code"] == "unauthenticated"
