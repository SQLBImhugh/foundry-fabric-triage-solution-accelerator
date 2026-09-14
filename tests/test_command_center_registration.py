from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from register_command_center import (  # noqa: E402
    GRAPH_CLIENT_ID,
    USER_READ_SCOPE_ID,
    application_patch,
    assign_admin,
    ensure_principal,
    grant_user_consent,
)


def test_admin_assignment_uses_the_supported_scoped_relationship_query() -> None:
    user = "10000000-0000-0000-0000-000000000001"
    principal = "20000000-0000-0000-0000-000000000002"
    role = "30000000-0000-0000-0000-000000000003"

    class Graph:
        posted = []

        def collection(self, path, **params):
            assert path == f"servicePrincipals/{principal}/appRoleAssignedTo"
            assert params == {}, "The live relationship rejects principalId filtering"
            return []

        def request(self, method, path, body=None):
            if method == "GET" and path.startswith("users/"):
                return {"id": user}
            if method == "GET":
                return {"id": principal, "appRoles": [{"id": role, "isEnabled": True}]}
            self.posted.append(body)
            return {}

    graph = Graph()
    assign_admin(graph, {"id": principal}, user, role)
    assert graph.posted == [{"principalId": user, "resourceId": principal, "appRoleId": role}]


def test_registration_requests_only_delegated_own_user_photo_permission() -> None:
    patch = application_patch({}, "Test command center", ["https://example.test"], False)
    assert patch["requiredResourceAccess"] == [{
        "resourceAppId": GRAPH_CLIENT_ID,
        "resourceAccess": [{"id": USER_READ_SCOPE_ID, "type": "Scope"}],
    }]
    assert not patch.get("passwordCredentials") and not patch.get("keyCredentials")


def test_registration_preserves_permissions_and_deduplicates_profile_scope() -> None:
    existing = application_patch({}, "Test command center", ["https://example.test"], False)
    existing["appId"] = "10000000-0000-0000-0000-000000000001"
    first = application_patch(existing, "Test command center", ["https://example.test"], False)
    second = application_patch(existing | first, "Test command center", ["https://example.test"], False)
    assert first == second
    assert len(first["requiredResourceAccess"]) == 2
    assert existing["requiredResourceAccess"] == [{
        "resourceAppId": GRAPH_CLIENT_ID,
        "resourceAccess": [{"id": USER_READ_SCOPE_ID, "type": "Scope"}],
    }], "Registration must not mutate its existing-configuration input"


def test_new_and_existing_principals_require_entra_assignment() -> None:
    class Graph:
        def __init__(self, existing):
            self.existing = existing
            self.writes = []

        def collection(self, _path, **_params):
            return self.existing

        def request(self, method, path, body=None):
            self.writes.append((method, path, body))
            if method == "GET":
                return {"id": "principal", "appId": "app", "accountEnabled": True, "appRoleAssignmentRequired": True}
            return {"id": "principal", **body}

    new = Graph([])
    assert ensure_principal(new, "app")["appRoleAssignmentRequired"] is True
    assert new.writes == [("POST", "servicePrincipals", {
        "appId": "app", "appRoleAssignmentRequired": True,
    })]
    managed = Graph([{"id": "principal", "accountEnabled": True, "appRoleAssignmentRequired": False}])
    assert ensure_principal(managed, "app")["appRoleAssignmentRequired"] is True
    assert managed.writes[0] == ("PATCH", "servicePrincipals/principal", {"appRoleAssignmentRequired": True})


def test_profile_consent_is_a_user_specific_delegated_grant() -> None:
    class Graph:
        writes = []

        def collection(self, _path, **_params):
            return []

        def request(self, method, path, body=None):
            self.writes.append((method, path, body))
            return {}

    graph = Graph()
    grant_user_consent(graph, "spa", "graph", "user", scope="User.Read")
    assert graph.writes == [("POST", "oauth2PermissionGrants", {
        "clientId": "spa", "resourceId": "graph", "principalId": "user",
        "consentType": "Principal", "scope": "User.Read",
    })]
