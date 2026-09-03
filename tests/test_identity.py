"""Offline tests for agent identity inspection.

These use a synthetic directory rather than a live tenant. The shapes are the
ones Graph really returns, including the awkward ones: the collection projection
that omits ``appId``, and role assignments that carry a role *id* rather than a
name. Every identifier below is fabricated.
"""

from __future__ import annotations

from typing import Any

import pytest

from triage.identity import (
    AGENT_IDENTITY_COLLECTION,
    GRAPH,
    MailboxScopeProof,
    load_agent_identity,
    load_app_registration,
    project_name_prefix,
)

TRIAGE_ID = "aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb"
DQ_ID = "bbbbbbbb-1111-2222-3333-cccccccccccc"
OTHER_PROJECT_DQ_ID = "cccccccc-2222-3333-4444-dddddddddddd"
BLUEPRINT_APP_ID = "dddddddd-3333-4444-5555-eeeeeeeeeeee"
GRAPH_SP_ID = "eeeeeeee-4444-5555-6666-ffffffffffff"
MAIL_READ_ROLE = "810c84a8-4a9e-49e6-bf7d-12d183f40d01"

PREFIX = "contoso-foundry-bi-request-triage"


class FakeGraph:
    """A recorded directory. Records every URL so tests can assert on reads."""

    def __init__(self, routes: dict[str, dict[str, Any]]) -> None:
        self.routes = routes
        self.seen: list[str] = []

    def get(self, url: str) -> dict[str, Any]:
        self.seen.append(url)
        if url not in self.routes:
            raise KeyError(f"no recorded response for {url}")
        return self.routes[url]


def _directory(
    *,
    key_creds: int = 0,
    password_creds: int = 0,
    fics: list[str] | None = None,
    triage_roles: bool = True,
) -> FakeGraph:
    fics = ["fmi-fic"] if fics is None else fics
    return FakeGraph(
        {
            # The real collection projection omits appId -- that omission is
            # the reason load_agent_identity re-reads the single object.
            AGENT_IDENTITY_COLLECTION: {
                "value": [
                    {
                        "id": OTHER_PROJECT_DQ_ID,
                        "displayName": "othertenant-foundry-eus-other-project-bi-data-quality-AgentIdentity",
                        "accountEnabled": True,
                    },
                    {
                        "id": TRIAGE_ID,
                        "displayName": f"{PREFIX}-bi-triage-AgentIdentity",
                        "accountEnabled": True,
                        "agentIdentityBlueprintId": BLUEPRINT_APP_ID,
                    },
                    {
                        "id": DQ_ID,
                        "displayName": f"{PREFIX}-bi-data-quality-AgentIdentity",
                        "accountEnabled": True,
                    },
                ]
            },
            f"{GRAPH}/servicePrincipals/{TRIAGE_ID}": {
                "id": TRIAGE_ID,
                "appId": TRIAGE_ID,
                "displayName": f"{PREFIX}-bi-triage-AgentIdentity",
                "accountEnabled": True,
                "agentIdentityBlueprintId": BLUEPRINT_APP_ID,
            },
            f"{GRAPH}/servicePrincipals/{DQ_ID}": {
                "id": DQ_ID,
                "appId": DQ_ID,
                "displayName": f"{PREFIX}-bi-data-quality-AgentIdentity",
                "accountEnabled": True,
            },
            f"{GRAPH}/servicePrincipals/{OTHER_PROJECT_DQ_ID}": {
                "id": OTHER_PROJECT_DQ_ID,
                "appId": OTHER_PROJECT_DQ_ID,
                "displayName": "othertenant-foundry-eus-other-project-bi-data-quality-AgentIdentity",
                "accountEnabled": True,
            },
            f"{GRAPH}/applications(appId='{BLUEPRINT_APP_ID}')": {
                "id": BLUEPRINT_APP_ID,
                "displayName": f"{PREFIX}-bi-triage-9a283-AgentIdentityBlueprint",
                "keyCredentials": [{"keyId": f"k{i}"} for i in range(key_creds)],
                "passwordCredentials": [{"keyId": f"p{i}"} for i in range(password_creds)],
            },
            f"{GRAPH}/applications/{BLUEPRINT_APP_ID}/federatedIdentityCredentials": {
                "value": [{"name": n} for n in fics]
            },
            f"{GRAPH}/servicePrincipals/{TRIAGE_ID}/sponsors": {
                "value": [{"displayName": "Robin Avery", "id": "ffffffff-5555-6666-7777-aaaaaaaaaaaa"}]
            },
            f"{GRAPH}/servicePrincipals/{DQ_ID}/sponsors": {
                "value": [{"displayName": "Robin Avery", "id": "ffffffff-5555-6666-7777-aaaaaaaaaaaa"}]
            },
            f"{GRAPH}/servicePrincipals/{TRIAGE_ID}/appRoleAssignments": {
                "value": (
                    [
                        {
                            "resourceId": GRAPH_SP_ID,
                            "appRoleId": MAIL_READ_ROLE,
                            "resourceDisplayName": "Microsoft Graph",
                        }
                    ]
                    if triage_roles
                    else []
                )
            },
            f"{GRAPH}/servicePrincipals/{DQ_ID}/appRoleAssignments": {"value": []},
            f"{GRAPH}/servicePrincipals/{GRAPH_SP_ID}": {
                "id": GRAPH_SP_ID,
                "appRoles": [{"id": MAIL_READ_ROLE, "value": "Mail.Read"}],
            },
        }
    )


@pytest.mark.parametrize(
    "endpoint,expected",
    [
        (
            "https://contoso-foundry.services.ai.azure.com/api/projects/bi-request-triage",
            "contoso-foundry-bi-request-triage",
        ),
        ("https://acct.services.ai.azure.com/api/projects/proj", "acct-proj"),
        ("", ""),
        ("not a url", ""),
    ],
)
def test_project_name_prefix(endpoint: str, expected: str) -> None:
    assert project_name_prefix(endpoint) == expected


def test_reads_identity_and_resolves_role_name() -> None:
    report = load_agent_identity(
        _directory(), display_name_contains="bi-triage", name_prefix=PREFIX
    )
    assert report is not None
    assert report.object_id == TRIAGE_ID
    # The collection omits appId; this proves the single-object re-read happened.
    assert report.app_id == TRIAGE_ID
    assert report.identity_matches_app
    assert report.sponsors == ["Robin Avery"]
    # Role ids are meaningless to a customer -- assert we resolve the name.
    assert report.graph_app_roles == ["Mail.Read on Microsoft Graph"]
    assert report.short_name == "triage"


def test_secretless_requires_both_no_secrets_and_a_federated_credential() -> None:
    assert load_agent_identity(
        _directory(), display_name_contains="bi-triage", name_prefix=PREFIX
    ).is_secretless

    with_password = load_agent_identity(
        _directory(password_creds=1), display_name_contains="bi-triage", name_prefix=PREFIX
    )
    assert not with_password.is_secretless

    with_key = load_agent_identity(
        _directory(key_creds=1), display_name_contains="bi-triage", name_prefix=PREFIX
    )
    assert not with_key.is_secretless

    # No credentials of any kind is not "secretless", it is "cannot authenticate".
    # Reporting it as secretless would turn a broken agent into a selling point.
    no_creds = load_agent_identity(
        _directory(fics=[]), display_name_contains="bi-triage", name_prefix=PREFIX
    )
    assert not no_creds.is_secretless


def test_prefix_prevents_matching_another_projects_agent() -> None:
    """Two projects both have a 'bi-data-quality' agent.

    Without the prefix the wrong one matches, and the output looks correct.
    """
    scoped = load_agent_identity(
        _directory(), display_name_contains="bi-data-quality", name_prefix=PREFIX
    )
    assert scoped is not None
    assert scoped.object_id == DQ_ID

    unscoped = load_agent_identity(_directory(), display_name_contains="bi-data-quality")
    assert unscoped is not None
    assert unscoped.object_id == OTHER_PROJECT_DQ_ID


def test_mailbox_scope_only_claimed_for_an_agent_that_can_read_mail() -> None:
    """A scope claim against an agent with no mail permission is misleading.

    It implies a control is protecting something when the agent has no mailbox
    access to protect in the first place.
    """
    proof = MailboxScopeProof(granted=["alerts@example.com"], denied=["ceo@example.com"])

    triage = load_agent_identity(
        _directory(), display_name_contains="bi-triage", name_prefix=PREFIX, scope_proof=proof
    )
    assert triage.holds_mail_permission
    assert triage.scope_proof is proof

    dq = load_agent_identity(
        _directory(),
        display_name_contains="bi-data-quality",
        name_prefix=PREFIX,
        scope_proof=proof,
    )
    assert not dq.holds_mail_permission
    assert dq.scope_proof is None


def test_scope_proof_needs_both_halves() -> None:
    assert MailboxScopeProof(granted=["a"], denied=["b"]).is_scoped
    # Granting access proves nothing on its own: an unscoped app also grants.
    assert not MailboxScopeProof(granted=["a"], denied=[]).is_scoped
    assert not MailboxScopeProof(granted=[], denied=["b"]).is_scoped


def test_missing_agent_returns_none_rather_than_raising() -> None:
    assert (
        load_agent_identity(
            _directory(), display_name_contains="does-not-exist", name_prefix=PREFIX
        )
        is None
    )


def test_directory_read_failures_degrade_instead_of_crashing() -> None:
    """A partial directory read should still produce a usable report.

    Permissions on directory objects vary; losing the sponsor list should not
    cost us the whole panel mid-demo.
    """
    directory = _directory()
    del directory.routes[f"{GRAPH}/servicePrincipals/{TRIAGE_ID}/sponsors"]

    report = load_agent_identity(
        directory, display_name_contains="bi-triage", name_prefix=PREFIX
    )
    assert report is not None
    assert report.sponsors == []
    assert report.graph_app_roles == ["Mail.Read on Microsoft Graph"]


APP_ID = "3f2b91c4-77ad-4e10-9c55-b81e6a0d2f47"
APP_OBJ = "f1e6a93b-a17b-44b6-a837-b1e6b50eb985"
APP_SP = "809f7034-8adc-4dea-bc8e-124b73cfd8c4"


def _directory_with_app() -> FakeGraph:
    directory = _directory()
    directory.routes[f"{GRAPH}/applications(appId='{APP_ID}')"] = {
        "id": APP_OBJ,
        "displayName": "bi-bi-triage-inbox",
        "keyCredentials": [],
        "passwordCredentials": [{"keyId": "p1", "endDateTime": "2027-08-28T00:16:51Z"}],
    }
    directory.routes[f"{GRAPH}/servicePrincipals(appId='{APP_ID}')"] = {"id": APP_SP}
    directory.routes[f"{GRAPH}/servicePrincipals/{APP_SP}/appRoleAssignments"] = {
        "value": [
            {
                "resourceId": GRAPH_SP_ID,
                "appRoleId": MAIL_READ_ROLE,
                "resourceDisplayName": "Microsoft Graph",
            }
        ]
    }
    return directory


def test_app_registration_is_reported_as_holding_a_secret_with_an_expiry() -> None:
    """The contrast the demo rests on.

    Agent identities have no credential to expire; this one does, and the date
    is the argument for moving off it.
    """
    report = load_app_registration(_directory_with_app(), app_id=APP_ID)
    assert report is not None
    assert not report.is_agent_identity
    assert not report.is_secretless
    assert report.password_credentials == 1
    assert report.secret_expires_at == "2027-08-28T00:16:51Z"
    # An app registration records who created it, not who is accountable.
    assert report.sponsors == []
    assert report.graph_app_roles == ["Mail.Read on Microsoft Graph"]


def test_app_registration_shows_its_mailbox_scope() -> None:
    proof = MailboxScopeProof(granted=["alerts@example.com"], denied=["ceo@example.com"])
    report = load_app_registration(_directory_with_app(), app_id=APP_ID, scope_proof=proof)
    assert report.scope_proof is proof


def test_missing_app_registration_returns_none() -> None:
    assert load_app_registration(_directory(), app_id="") is None
    assert load_app_registration(_directory(), app_id="not-a-real-app") is None
