from __future__ import annotations

import time
from datetime import UTC, datetime
from itertools import combinations
from types import SimpleNamespace
from urllib.request import OpenerDirector

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import ValidationError

from triage.command_center.access_models import VerifiedEntraIdentity
from triage.command_center.auth import EntraTokenVerifier, require
from triage.command_center.models import APP_ROLES, Actor, ApiFailure, WebSettings

TENANT = "10000000-0000-0000-0000-000000000001"
CLIENT = "20000000-0000-0000-0000-000000000002"
USER = "30000000-0000-0000-0000-000000000003"


@pytest.fixture
def signing(monkeypatch):
    def no_network(*_args, **_kwargs):
        raise AssertionError("JWT authorization tests must remain offline")

    monkeypatch.setattr(OpenerDirector, "open", no_network)
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    class Keys:
        def get_signing_key_from_jwt(self, _token):
            return SimpleNamespace(key=private.public_key())

    verifier = EntraTokenVerifier(
        WebSettings(_env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT),
        signing_keys=Keys(),
    )

    def token(*, omit=(), key=private, algorithm="RS256", **changes):
        now = int(time.time())
        claims = {
            "iss": f"https://login.microsoftonline.com/{TENANT}/v2.0",
            "aud": CLIENT, "tid": TENANT, "oid": USER,
            "exp": now + 300, "nbf": now - 5, "iat": now - 5,
            "scp": "access_as_user", "name": "Test operator",
            "roles": ["CommandCenter.Approver"],
        } | changes
        for field in omit:
            claims.pop(field, None)
        return jwt.encode(claims, key, algorithm=algorithm)

    return verifier, token


def test_actor_comes_from_validated_directory_claims(signing) -> None:
    verifier, token = signing
    issued_at = int(time.time()) - 10
    expires_at = issued_at + 300
    actor = verifier.verify(token(iat=issued_at, exp=expires_at))
    assert isinstance(actor, VerifiedEntraIdentity)
    assert actor.id == USER
    assert actor.display_name == "Test operator"
    assert actor.roles == ["approver", "reader"]
    assert not actor.permits("operator")
    assert actor.tenant_id == TENANT and actor.application_id == CLIENT
    assert actor.token_issued_at == datetime.fromtimestamp(issued_at, UTC)
    assert actor.token_expires_at == datetime.fromtimestamp(expires_at, UTC)
    assert set(actor.model_dump()) == {
        "id", "display_name", "roles", "tenant_id", "application_id",
        "token_issued_at", "token_expires_at",
    }


@pytest.mark.parametrize("granted", [
    list(roles) for count in range(1, 5) for roles in combinations(APP_ROLES, count)
])
def test_only_four_app_roles_and_their_reader_implication_authorize(signing, granted) -> None:
    verifier, token = signing
    actor = verifier.verify(token(roles=[f"CommandCenter.{role.title()}" for role in granted]))
    assert actor.roles == sorted(set(granted) | {"reader"})
    for permission in APP_ROLES:
        assert actor.permits(permission) == (
            permission == "reader" or permission in granted or "admin" in granted
        )
    assert not actor.permits("directory_admin")


def test_duplicate_and_unrecognized_claims_do_not_expand_known_roles(signing) -> None:
    verifier, token = signing
    actor = verifier.verify(token(roles=[
        "CommandCenter.Operator", "CommandCenter.Operator", "Admin", "Directory.ReadWrite.All",
    ]))
    assert actor.roles == ["operator", "reader"]
    assert not actor.permits("admin") and not actor.permits("approver")


@pytest.mark.parametrize("changes", [
    {"aud": "another-api"},
    {"aud": [CLIENT]},
    {"iss": f"https://login.microsoftonline.com/{CLIENT}/v2.0"},
    {"exp": 1},
    {"tid": CLIENT},
    {"scp": ""},
    {"scp": "prefix_access_as_user access_as_user_suffix"},
    {"idtyp": "app"},
    {"roles": []},
    {"roles": ["Admin"]},
    {"roles": ["CommandCenter.reader"]},
    {"roles": ["CommandCenter.Owner"]},
    {"oid": "not-a-guid"},
])
def test_wrong_tenant_audience_expiry_scope_or_role_is_rejected(signing, changes) -> None:
    verifier, token = signing
    with pytest.raises(ApiFailure):
        verifier.verify(token(**changes))


@pytest.mark.parametrize("value", ["not-a-token", "", None, b"not-a-token", "a" * 16385])
def test_arbitrary_bearer_value_is_not_an_identity(signing, value) -> None:
    verifier, _ = signing
    with pytest.raises(ApiFailure):
        verifier.verify(value)


@pytest.mark.parametrize("algorithm,key", [
    ("none", None), ("HS256", b"offline-test-signing-material" * 2),
])
def test_unsigned_and_symmetric_tokens_cannot_supply_an_identity(signing, algorithm, key) -> None:
    verifier, token = signing
    with pytest.raises(ApiFailure) as error:
        verifier.verify(token(algorithm=algorithm, key=key, roles=["CommandCenter.Admin"]))
    assert error.value.status == 401


def test_a_forged_rs256_signature_cannot_supply_an_admin_identity(signing, caplog) -> None:
    verifier, token = signing
    forged_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = token(key=forged_key, roles=["CommandCenter.Admin"])
    with pytest.raises(ApiFailure) as error:
        verifier.verify(forged)
    assert error.value.status == 401
    assert "InvalidSignatureError" in caplog.text
    assert forged not in caplog.text


def test_permission_roles_do_not_imply_each_other() -> None:
    reader = Actor(id=USER, display_name="Reader", roles=["reader"])
    with pytest.raises(ApiFailure) as error:
        require(reader, "approver")
    assert error.value.status == 403


@pytest.mark.parametrize("missing", [
    "exp", "iat", "nbf", "iss", "aud", "tid", "oid", "scp", "roles",
])
def test_missing_required_identity_scope_or_role_claim_fails_closed(signing, missing) -> None:
    verifier, token = signing
    with pytest.raises(ApiFailure):
        verifier.verify(token(omit=(missing,), sub=USER))


@pytest.mark.parametrize("changes", [
    {"exp": 1}, {"exp": None}, {"exp": []}, {"iat": {}}, {"nbf": []},
    {"exp": 10**30}, {"exp": float("inf")}, {"exp": "9999999999"},
    {"iat": 1.5}, {"nbf": True},
    {"iat": None}, {"nbf": None}, {"oid": None}, {"oid": {}},
    {"oid": 42}, {"oid": "00000000-0000-0000-0000-000000000000"},
    {"scp": ["access_as_user"]}, {"scp": {"access_as_user": True}},
    {"roles": "CommandCenter.Admin"}, {"roles": [None]}, {"roles": [["CommandCenter.Admin"]]},
])
def test_malformed_claims_fail_with_an_authorization_error(signing, changes) -> None:
    verifier, token = signing
    with pytest.raises(ApiFailure) as error:
        verifier.verify(token(**changes))
    assert error.value.status in {401, 403}


@pytest.mark.parametrize("field", ["iat", "nbf"])
def test_future_or_inverted_validity_interval_is_rejected(signing, field) -> None:
    verifier, token = signing
    now = int(time.time())
    for changes in ({field: now + 120}, {field: now + 20, "exp": now + 10}):
        with pytest.raises(ApiFailure) as error:
            verifier.verify(token(**changes))
        assert error.value.status == 401


def test_existing_thirty_second_clock_leeway_is_preserved(signing) -> None:
    verifier, token = signing
    now = int(time.time())
    assert verifier.verify(token(iat=now - 100, nbf=now - 100, exp=now - 5)).permits("reader")
    with pytest.raises(ApiFailure):
        verifier.verify(token(iat=now - 100, nbf=now - 100, exp=now - 60))


def test_malformed_optional_name_is_not_presented_as_a_verified_directory_name(signing) -> None:
    verifier, token = signing
    actor = verifier.verify(token(name={"admin": True}, preferred_username=[]))
    assert actor.display_name == USER


@pytest.mark.parametrize("name,preferred,expected", [
    (" \r\nTest\x00 operator\t\u202e ", None, "Test operator"),
    ("\x00\u202e", " test@example.invalid ", "test@example.invalid"),
    ("x" * 250, None, "x" * 200),
    (None, None, USER),
])
def test_token_display_names_are_bounded_and_control_free(signing, name, preferred, expected) -> None:
    verifier, token = signing
    actor = verifier.verify(token(name=name, preferred_username=preferred))
    assert actor.display_name == expected
    assert actor.display_name.isprintable()


@pytest.mark.parametrize("roles", [[], ["CommandCenter.Admin"]])
def test_groups_and_group_overage_neither_grant_nor_limit_access(signing, roles) -> None:
    verifier, token = signing
    value = token(
        roles=roles, groups=["CommandCenter.Admin"], hasgroups=True,
        _claim_names={"groups": "src1"},
        _claim_sources={"src1": {"endpoint": "https://graph.microsoft.com/untrusted"}},
    )
    if roles:
        assert verifier.verify(value).roles == ["admin", "reader"]
    else:
        with pytest.raises(ApiFailure) as error:
            verifier.verify(value)
        assert error.value.status == 403


@pytest.mark.parametrize("field", ["tenant_id", "client_id"])
@pytest.mark.parametrize("value", ["", "not-a-guid", "00000000-0000-0000-0000-000000000000"])
def test_live_verifier_requires_specific_nonzero_directory_and_application_ids(field, value) -> None:
    settings = WebSettings(
        _env_file=None, mode="live",
        **({"tenant_id": TENANT, "client_id": CLIENT} | {field: value}),
    )
    with pytest.raises(ValueError, match="valid Entra tenant and API client IDs"):
        EntraTokenVerifier(settings)


@pytest.mark.parametrize("mode", ["demo", "live"])
@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes"])
def test_retired_sql_authority_cannot_be_enabled_from_the_environment(monkeypatch, mode, value) -> None:
    monkeypatch.setenv("COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED", value)
    with pytest.raises(ValidationError, match="COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true is retired"):
        WebSettings(_env_file=None, mode=mode)


def test_retired_sql_authority_cannot_be_enabled_programmatically() -> None:
    with pytest.raises(ValidationError, match="Authorization uses Entra app roles"):
        WebSettings(_env_file=None, access_management_enabled=True)


@pytest.mark.parametrize("value", [None, "false", "0"])
def test_unset_and_false_deprecated_config_are_accepted(monkeypatch, value) -> None:
    if value is None:
        monkeypatch.delenv("COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED", raising=False)
    else:
        monkeypatch.setenv("COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED", value)
    assert WebSettings(_env_file=None).access_management_enabled is False
