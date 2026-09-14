"""Entra access-token validation; never accept a client-supplied actor header."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Protocol

from triage.command_center.access_models import (
    VerifiedEntraIdentity,
    directory_id,
)
from triage.command_center.models import Actor, ApiFailure, AppRole, WebSettings

logger = logging.getLogger("triage.command_center.auth")

_ROLES: dict[str, AppRole] = {
    "CommandCenter.Reader": "reader",
    "CommandCenter.Operator": "operator",
    "CommandCenter.Approver": "approver",
    "CommandCenter.Admin": "admin",
}


class TokenVerifier(Protocol):
    """Server-owned authentication seam for offline tests, never a request input."""

    def verify(self, token: str) -> Actor: ...


class EntraTokenVerifier:
    def __init__(self, settings: WebSettings, *, signing_keys=None) -> None:
        import jwt

        try:
            self.tenant_id = directory_id(settings.tenant_id)
            self.client_id = directory_id(settings.client_id)
        except ValueError as exc:
            raise ValueError("Live command center needs valid Entra tenant and API client IDs") from exc
        self.issuer = f"https://login.microsoftonline.com/{self.tenant_id}/v2.0"
        self.keys = signing_keys if signing_keys is not None else jwt.PyJWKClient(
            f"https://login.microsoftonline.com/{self.tenant_id}/discovery/v2.0/keys",
            cache_keys=True, lifespan=3600,
        )

    def verify(self, token: str) -> VerifiedEntraIdentity:
        import jwt

        if not isinstance(token, str) or not token or len(token) > 16384:
            raise ApiFailure(401, "unauthenticated", "A valid Entra access token is required.")
        try:
            if jwt.get_unverified_header(token).get("alg") != "RS256":
                raise jwt.InvalidAlgorithmError("Only RS256 access tokens are accepted")
            key = self.keys.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token, key, algorithms=["RS256"], audience=self.client_id,
                issuer=self.issuer,
                options={
                    "require": ["exp", "iat", "nbf", "iss", "aud", "tid", "oid"],
                    "strict_aud": True,
                },
                leeway=30,
            )
            if any(type(claims[field]) is not int for field in ("iat", "exp", "nbf")):
                raise ValueError("Entra token timestamps must be integer NumericDates")
            if claims["iat"] >= claims["exp"] or claims["nbf"] >= claims["exp"]:
                raise ValueError("Entra token validity interval is invalid")
            issued_at = datetime.fromtimestamp(claims["iat"], UTC)
            expires_at = datetime.fromtimestamp(claims["exp"], UTC)
        except (jwt.PyJWTError, ValueError, TypeError, OverflowError, OSError) as exc:
            logger.warning("Command-center access token rejected (%s)", type(exc).__name__)
            raise ApiFailure(401, "unauthenticated", "The access token is invalid or expired.") from exc
        scope = claims.get("scp")
        if (
            claims.get("tid") != self.tenant_id or claims.get("idtyp") == "app"
            or not isinstance(scope, str) or "access_as_user" not in scope.split()
        ):
            raise ApiFailure(403, "forbidden", "A delegated command-center access token is required.")
        try:
            if not isinstance(claims["oid"], str):
                raise ValueError("Directory object ID must be a string")
            actor_id = directory_id(claims["oid"])
        except (ValueError, TypeError) as exc:
            raise ApiFailure(401, "unauthenticated", "The token has no valid directory identity.") from exc
        raw_roles = claims.get("roles", [])
        if not isinstance(raw_roles, list) or not all(isinstance(role, str) for role in raw_roles):
            raise ApiFailure(403, "forbidden", "No command-center role was granted.")
        roles = {_ROLES[value] for value in raw_roles if value in _ROLES}
        if not roles:
            raise ApiFailure(403, "forbidden", "Assign a command-center application role in Entra before using this API.")
        name = actor_id
        for value in (claims.get("name"), claims.get("preferred_username")):
            if isinstance(value, str):
                label = " ".join("".join(char if char.isprintable() else " " for char in value).split())
                if label:
                    name = label[:200].strip()
                    break
        return VerifiedEntraIdentity(
            id=actor_id, tenant_id=self.tenant_id, display_name=name[:200],
            application_id=self.client_id, roles=sorted(roles),
            token_issued_at=issued_at, token_expires_at=expires_at,
        )


def require(actor: Actor, permission: str) -> None:
    if not actor.permits(permission):
        raise ApiFailure(403, "forbidden", f"This operation requires the {permission} role.")
