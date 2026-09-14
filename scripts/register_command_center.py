"""Register the command center's single-tenant, secretless SPA/API in Entra.

The browser uses authorization code + PKCE. The API validates access_as_user
and CommandCenter.* roles itself; this is not App Service EasyAuth.

Use --dry-run for an offline plan with no Azure CLI or Graph calls. Execution
requires an already authenticated Azure CLI session and directory permissions
to manage this application. Optional role assignments and user-specific consent
also require the corresponding Graph permissions and administrator directory
role. This script never grants those permissions to its own caller.

--authorize-azure-cli preauthorizes Microsoft's Azure CLI public client for ONLY
this API's scope. --grant-admin-consent additionally creates Principal grants for
the selected administrator, not AllPrincipals grants. Remove validation access
when evaluation ends. --grant-profile-consent separately grants delegated
Graph User.Read only for that selected user and the command-center client.
No password, certificate, application permission to Graph, or tenant-wide role
is created.

Public API references:
https://learn.microsoft.com/graph/api/application-post-applications
https://learn.microsoft.com/graph/api/resources/preauthorizedapplication
https://learn.microsoft.com/graph/api/serviceprincipal-post-approleassignedto
https://learn.microsoft.com/graph/api/oauth2permissiongrant-post
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import shutil
import subprocess
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID, uuid4

GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
AZURE_CLI_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
GRAPH_CLIENT_ID = "00000003-0000-0000-c000-000000000000"
USER_READ_SCOPE_ID = "e1fe6dd8-ba31-4d61-89e7-88639da4683d"
REGISTRATION_TAG = "triage-command-center"
SCOPE = "access_as_user"
ROLE_DESCRIPTIONS = {
    "CommandCenter.Reader": "Read incidents, run history and health, and ask read-only questions.",
    "CommandCenter.Operator": "Submit configured investigations, add incident notes, and record user resolutions.",
    "CommandCenter.Approver": "Approve or deny pending remediation requests.",
    "CommandCenter.Admin": "Administer application operations, scenario validation and reconciliation. Access assignments are managed in Entra.",
}
APP_SELECT = (
    "id,appId,displayName,signInAudience,identifierUris,api,appRoles,spa,web,"
    "requiredResourceAccess,isFallbackPublicClient,tags,passwordCredentials,keyCredentials"
)


class RegistrationError(RuntimeError):
    """A registration operation failed without assuming success."""


class GraphError(RegistrationError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def guid(value: str) -> str:
    try:
        result = UUID(value)
    except (ValueError, AttributeError) as exc:
        raise argparse.ArgumentTypeError("Expected a GUID, not a display name.") from exc
    if result.int == 0:
        raise argparse.ArgumentTypeError("The empty GUID is not an identity.")
    return str(result)


def subscription_name(value: str) -> str:
    # az.cmd runs through Windows command processing. CLI-bound input excludes
    # shell metacharacters; Graph-bound names never pass through a shell.
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,127}", value):
        raise argparse.ArgumentTypeError("Use a subscription GUID or a simple subscription name.")
    return value


def redirect_uri(value: str, *, local: bool = False) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise argparse.ArgumentTypeError("The redirect URI is malformed.") from exc
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise argparse.ArgumentTypeError("Redirect URIs cannot contain credentials, queries, or fragments.")
    if not parsed.hostname or any(char.isspace() for char in value):
        raise argparse.ArgumentTypeError("The redirect URI must have a hostname.")
    if local:
        if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise argparse.ArgumentTypeError("Development redirects must use an explicit loopback host.")
    elif (
        parsed.scheme != "https" or parsed.path not in {"", "/"} or port not in {None, 443}
        or parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        or not re.fullmatch(r"[A-Za-z0-9.-]+", parsed.hostname)
    ):
        raise argparse.ArgumentTypeError("--webapp-origin must be an HTTPS origin, without a path.")
    return value.rstrip("/") if parsed.path in {"", "/"} else value


def az_json(*arguments: str) -> dict[str, Any]:
    executable = shutil.which("az")
    if not executable:
        raise RegistrationError("Azure CLI is missing. Install it and authenticate before registration.")
    try:
        result = subprocess.run(
            [executable, *arguments, "--only-show-errors", "--output", "json"],
            capture_output=True, text=True, encoding="utf-8", check=False, timeout=120,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RegistrationError(f"Azure CLI could not complete {' '.join(arguments[:2])}.") from exc
    if result.returncode:
        raise RegistrationError(
            f"Azure CLI {' '.join(arguments[:2])} failed ({result.returncode}): "
            f"{result.stderr.strip()[:1500]}"
        )
    if not result.stdout.strip():
        return {}
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RegistrationError("Azure CLI returned invalid JSON.") from exc
    if not isinstance(value, dict):
        raise RegistrationError("Azure CLI returned an unexpected response shape.")
    return value


def graph_token(subscription: str, tenant_id: str) -> str:
    az_json("account", "set", "--subscription", subscription)
    account = az_json("account", "show", "--subscription", subscription)
    if account.get("tenantId", "").lower() != tenant_id.lower():
        raise RegistrationError("The selected subscription belongs to a different tenant; no Graph mutation occurred.")
    if account.get("environmentName") != "AzureCloud":
        raise RegistrationError("This script targets the Azure public cloud only.")
    az_json("account", "set", "--subscription", subscription)
    token = az_json(
        "account", "get-access-token", "--subscription", subscription,
        "--resource", "https://graph.microsoft.com",
    )
    if token.get("tenant", "").lower() != tenant_id.lower():
        raise RegistrationError("The Graph token belongs to a different tenant; no Graph mutation occurred.")
    access_token = token.get("accessToken")
    if not isinstance(access_token, str) or not access_token:
        raise RegistrationError("Azure CLI did not provide a Graph access token.")
    return access_token


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward a directory access token to a redirect destination.
        return None


class GraphClient:
    def __init__(self, token: str) -> None:
        self._token = token
        self._opener = build_opener(NoRedirect())

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = path if path.startswith("https://") else f"{GRAPH_ROOT}/{path.lstrip('/')}"
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https" or parsed.netloc != "graph.microsoft.com"
            or not parsed.path.startswith("/v1.0/")
        ):
            raise RegistrationError("Refusing a Graph URL outside the configured directory endpoint.")
        request = Request(
            url, data=json.dumps(body).encode("utf-8") if body is not None else None,
            headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"},
            method=method,
        )
        for attempt in range(4):
            try:
                with self._opener.open(request, timeout=60) as response:
                    content = response.read()
                value = json.loads(content) if content else {}
                if not isinstance(value, dict):
                    raise RegistrationError("Graph returned an unexpected response shape.")
                return value
            except HTTPError as exc:
                if method in {"GET", "PATCH"} and exc.code in {429, 503, 504} and attempt < 3:
                    delay = exc.headers.get("Retry-After", "")
                    time.sleep(min(int(delay), 30) if delay.isdigit() else 2 ** attempt)
                    continue
                try:
                    envelope = json.loads(exc.read())
                    detail = envelope.get("error", {}) if isinstance(envelope, dict) else {}
                except (json.JSONDecodeError, UnicodeDecodeError):
                    detail = {}
                if not isinstance(detail, dict):
                    detail = {}
                raise GraphError(
                    exc.code,
                    f"Graph {method} {parsed.path} failed ({exc.code}): "
                    f"{detail.get('code', 'request_failed')}. {detail.get('message', '')[:1200]}",
                ) from exc
            except (URLError, TimeoutError) as exc:
                raise RegistrationError(
                    f"Graph {method} {parsed.path} could not complete. "
                    "A write may have succeeded; rerun to reconcile rather than creating another application."
                ) from exc
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise RegistrationError("Graph returned invalid JSON.") from exc
        raise RegistrationError("Graph retry budget exhausted.")

    def collection(self, resource: str, **query: str) -> list[dict[str, Any]]:
        path = f"{resource}?{urlencode(query)}" if query else resource
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        while path:
            if path in seen or len(seen) >= 100:
                raise RegistrationError("Graph pagination did not terminate within its bounded limit.")
            seen.add(path)
            page = self.request("GET", path)
            values = page.get("value")
            if not isinstance(values, list) or not all(isinstance(item, dict) for item in values):
                raise RegistrationError("Graph returned a malformed collection.")
            rows.extend(values)
            path = page.get("@odata.nextLink", "")
            if not isinstance(path, str):
                raise RegistrationError("Graph returned a malformed continuation URL.")
        return rows


def unique(rows: list[dict[str, Any]], description: str) -> dict[str, Any] | None:
    if len(rows) > 1:
        raise RegistrationError(f"Multiple {description} matched. Supply an explicit application client ID.")
    return rows[0] if rows else None


def application_patch(
    existing: dict[str, Any], display_name: str, redirects: list[str], authorize_cli: bool,
) -> dict[str, Any]:
    if existing.get("signInAudience", "AzureADMyOrg") != "AzureADMyOrg":
        raise RegistrationError("Use a dedicated single-tenant registration; this application is multitenant.")
    if existing.get("passwordCredentials") or existing.get("keyCredentials"):
        raise RegistrationError("Use a dedicated secretless SPA/API registration; existing credentials will not be removed.")
    api = copy.deepcopy(existing.get("api") or {})
    scopes = api.setdefault("oauth2PermissionScopes", [])
    scope = unique([item for item in scopes if item.get("value") == SCOPE], "access_as_user scopes")
    if scope is None:
        scope = {
            "id": str(uuid4()), "value": SCOPE, "type": "User", "isEnabled": True,
            "adminConsentDisplayName": "Access the triage command center",
            "adminConsentDescription": "Access the command center as the signed-in user, subject to assigned roles.",
            "userConsentDisplayName": "Access the triage command center",
            "userConsentDescription": "Access the command center using your assigned roles.",
        }
        scopes.append(scope)
    else:
        scope["isEnabled"] = True
    api["requestedAccessTokenVersion"] = 2
    if authorize_cli:
        authorized = api.setdefault("preAuthorizedApplications", [])
        cli = unique([item for item in authorized if item.get("appId") == AZURE_CLI_CLIENT_ID], "Azure CLI preauthorizations")
        if cli is None:
            authorized.append({"appId": AZURE_CLI_CLIENT_ID, "delegatedPermissionIds": [scope["id"]]})
        elif scope["id"] not in cli["delegatedPermissionIds"]:
            cli["delegatedPermissionIds"].append(scope["id"])

    role_keys = {"allowedMemberTypes", "description", "displayName", "id", "isEnabled", "value"}
    roles = [
        {key: copy.deepcopy(value) for key, value in item.items() if key in role_keys}
        for item in existing.get("appRoles", [])
    ]
    for value, description in ROLE_DESCRIPTIONS.items():
        role = unique([item for item in roles if item.get("value") == value], f"{value} roles")
        if role is None:
            roles.append({
                "id": str(uuid4()), "value": value, "displayName": value.rsplit(".", 1)[1],
                "description": description, "isEnabled": True, "allowedMemberTypes": ["User"],
            })
        else:
            if role.get("allowedMemberTypes") != ["User"]:
                raise RegistrationError(f"{value} must be a user/group role, not an application permission.")
            role["isEnabled"] = True
            role["description"] = description
    spa = copy.deepcopy(existing.get("spa") or {})
    spa["redirectUris"] = list(dict.fromkeys([*spa.get("redirectUris", []), *redirects]))
    web = copy.deepcopy(existing.get("web") or {})
    web["implicitGrantSettings"] = {
        "enableAccessTokenIssuance": False, "enableIdTokenIssuance": False,
    }
    patch: dict[str, Any] = {
        "displayName": display_name, "signInAudience": "AzureADMyOrg",
        "isFallbackPublicClient": False, "api": api, "appRoles": roles, "spa": spa, "web": web,
        "tags": list(dict.fromkeys([*existing.get("tags", []), REGISTRATION_TAG])),
    }
    access = copy.deepcopy(existing.get("requiredResourceAccess") or [])
    graph_access = unique([item for item in access if item.get("resourceAppId") == GRAPH_CLIENT_ID], "Graph permissions")
    profile_permission = {"id": USER_READ_SCOPE_ID, "type": "Scope"}
    if graph_access is None:
        access.append({"resourceAppId": GRAPH_CLIENT_ID, "resourceAccess": [profile_permission]})
    elif profile_permission not in graph_access["resourceAccess"]:
        graph_access["resourceAccess"].append(profile_permission)
    if client_id := existing.get("appId"):
        patch["identifierUris"] = list(dict.fromkeys([*existing.get("identifierUris", []), f"api://{client_id}"]))
        own_api = unique([item for item in access if item.get("resourceAppId") == client_id], "own-API permissions")
        permission = {"id": scope["id"], "type": "Scope"}
        if own_api is None:
            access.append({"resourceAppId": client_id, "resourceAccess": [permission]})
        elif permission not in own_api["resourceAccess"]:
            own_api["resourceAccess"].append(permission)
    patch["requiredResourceAccess"] = access
    return patch


def ensure_principal(graph: GraphClient, client_id: str) -> dict[str, Any]:
    principal = unique(
        graph.collection("servicePrincipals", **{"$filter": f"appId eq '{client_id}'"}),
        "service principals",
    )
    if principal is None:
        return graph.request("POST", "servicePrincipals", {
            "appId": client_id, "appRoleAssignmentRequired": True,
        })
    if not principal.get("accountEnabled", True):
        raise RegistrationError("The command-center service principal is disabled. Review its state before proceeding.")
    if not principal.get("appRoleAssignmentRequired"):
        graph.request("PATCH", f"servicePrincipals/{principal['id']}", {"appRoleAssignmentRequired": True})
        principal = graph.request(
            "GET", f"servicePrincipals/{principal['id']}?$select=id,appId,accountEnabled,appRoleAssignmentRequired",
        )
        if principal.get("appRoleAssignmentRequired") is not True:
            raise RegistrationError("Entra assignment-required sign-in was not confirmed. Review the enterprise application before continuing.")
    return principal


def assign_admin(graph: GraphClient, principal: dict[str, Any], user_id: str, role_id: str) -> None:
    # /users rejects a service-principal object ID instead of assigning a machine
    # identity the human administrator role.
    user = graph.request("GET", f"users/{user_id}?$select=id")
    if user.get("id", "").lower() != user_id.lower():
        raise RegistrationError("The administrator lookup did not return the requested user.")
    path = f"servicePrincipals/{principal['id']}/appRoleAssignedTo"
    # This relationship rejects principalId filtering in some tenants
    # (Request_UnsupportedQuery). It is already scoped to this one application.
    assignments = graph.collection(path)
    if any(item.get("principalId") == user_id and item.get("appRoleId") == role_id for item in assignments):
        return
    for attempt in range(6):
        ready = graph.request("GET", f"servicePrincipals/{principal['id']}?$select=id,appRoles")
        if any(role.get("id") == role_id and role.get("isEnabled") for role in ready.get("appRoles", [])):
            break
        if attempt == 5:
            raise RegistrationError("The new role has not propagated to the service principal. Rerun registration.")
        time.sleep(2)
    graph.request("POST", path, {
        "principalId": user_id, "resourceId": principal["id"], "appRoleId": role_id,
    })


def grant_user_consent(
    graph: GraphClient, client_sp: str, resource_sp: str, user_id: str, *, scope: str = SCOPE,
) -> None:
    if scope not in {SCOPE, "User.Read"}:
        raise RegistrationError("Only the command-center API and own-user profile scopes are supported.")
    grants = graph.collection("oauth2PermissionGrants", **{
        "$filter": f"clientId eq '{client_sp}' and resourceId eq '{resource_sp}'",
    })
    matching = [
        item for item in grants
        if item.get("consentType") == "Principal" and item.get("principalId") == user_id
    ]
    grant = unique(matching, "user-specific delegated grants")
    if grant is None:
        graph.request("POST", "oauth2PermissionGrants", {
            "clientId": client_sp, "resourceId": resource_sp, "principalId": user_id,
            "consentType": "Principal", "scope": scope,
        })
    elif scope not in grant.get("scope", "").split():
        graph.request("PATCH", f"oauth2PermissionGrants/{grant['id']}", {
            "scope": " ".join([*grant.get("scope", "").split(), scope]),
        })


def register(graph: GraphClient, args: argparse.Namespace) -> dict[str, Any]:
    filters = (
        f"appId eq '{args.application_id}'" if args.application_id
        else "displayName eq '" + args.display_name.replace("'", "''") + "'"
    )
    existing = unique(
        graph.collection("applications", **{"$filter": filters, "$select": APP_SELECT}),
        "applications",
    )
    if args.application_id and existing is None:
        raise RegistrationError("The supplied application client ID was not found in the asserted tenant.")
    if existing and not args.application_id and REGISTRATION_TAG not in existing.get("tags", []):
        raise RegistrationError("An unmarked application has that name. Review it and supply --application-id explicitly.")
    redirects = [args.webapp_origin]
    if args.localhost_redirect_uri:
        redirects.append(args.localhost_redirect_uri)
    created = existing is None
    if created:
        existing = graph.request("POST", "applications", application_patch(
            {}, args.display_name, redirects, args.authorize_azure_cli,
        ))
        existing = graph.request("GET", f"applications/{existing['id']}?$select={APP_SELECT}")
    if existing is None:
        raise RegistrationError("The application could not be resolved.")
    patch = application_patch(existing, args.display_name, redirects, args.authorize_azure_cli)
    changed = any(existing.get(key) != value for key, value in patch.items())
    # The self-API delegated permission references this resource principal.
    principal = ensure_principal(graph, existing["appId"])
    if changed:
        graph.request("PATCH", f"applications/{existing['id']}", patch)
    user_id = args.admin_user_object_id
    if args.admin_current_user:
        user_id = guid(graph.request("GET", "me?$select=id").get("id", ""))
    if user_id:
        admin_role = next(role for role in patch["appRoles"] if role["value"] == "CommandCenter.Admin")
        assign_admin(graph, principal, user_id, admin_role["id"])
    if args.grant_admin_consent:
        grant_user_consent(graph, principal["id"], principal["id"], user_id)
        if args.authorize_azure_cli:
            cli_sp = unique(graph.collection("servicePrincipals", **{
                "$filter": f"appId eq '{AZURE_CLI_CLIENT_ID}'", "$select": "id,appId",
            }), "Azure CLI service principals")
            if not cli_sp:
                # A tenant can use Azure CLI against Azure without having its
                # local enterprise application yet. This explicit validation
                # opt-in creates only the Microsoft-owned client principal.
                cli_sp = graph.request("POST", "servicePrincipals", {"appId": AZURE_CLI_CLIENT_ID})
            grant_user_consent(graph, cli_sp["id"], principal["id"], user_id)
    if getattr(args, "grant_profile_consent", False):
        graph_sp = unique(graph.collection("servicePrincipals", **{
            "$filter": f"appId eq '{GRAPH_CLIENT_ID}'", "$select": "id,appId",
        }), "Microsoft Graph service principals")
        if graph_sp is None:
            raise RegistrationError("The Microsoft Graph service principal is unavailable; no profile consent was granted.")
        grant_user_consent(graph, principal["id"], graph_sp["id"], user_id, scope="User.Read")
    return {
        "created": created, "applicationUpdated": changed,
        "applicationObjectId": existing["id"], "applicationClientId": existing["appId"],
        "servicePrincipalObjectId": principal["id"], "tenantId": args.tenant_id,
        "appRoleAssignmentRequired": principal.get("appRoleAssignmentRequired", True),
        "scope": f"api://{existing['appId']}/{SCOPE}", "redirectUris": patch["spa"]["redirectUris"],
        "roles": {role["value"]: role["id"] for role in patch["appRoles"] if role["value"] in ROLE_DESCRIPTIONS},
        "administratorUserObjectId": user_id,
        "azureCliPreauthorizationRequested": args.authorize_azure_cli,
        "userSpecificConsentRequested": args.grant_admin_consent,
        "profileConsentRequested": getattr(args, "grant_profile_consent", False),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--subscription", required=True, type=subscription_name)
    parser.add_argument("--tenant-id", required=True, type=guid)
    parser.add_argument("--display-name", required=True)
    parser.add_argument("--webapp-origin", required=True, type=redirect_uri)
    parser.add_argument("--localhost-redirect-uri", type=lambda value: redirect_uri(value, local=True))
    parser.add_argument("--application-id", type=guid, help="Existing application CLIENT ID, not its object ID.")
    administrator = parser.add_mutually_exclusive_group()
    administrator.add_argument("--admin-current-user", action="store_true")
    administrator.add_argument("--admin-user-object-id", type=guid)
    parser.add_argument("--authorize-azure-cli", action="store_true", help="Opt in to resource-specific CLI validation access.")
    parser.add_argument(
        "--grant-admin-consent", action="store_true",
        help="Grant access_as_user only for the selected administrator (SPA and, if opted in, CLI).",
    )
    parser.add_argument(
        "--grant-profile-consent", action="store_true",
        help="Grant delegated Graph User.Read only for the selected user and this SPA.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print an offline plan; no CLI, token, or Graph calls.")
    args = parser.parse_args(argv)
    if not args.display_name.strip() or len(args.display_name) > 256 or any(ord(char) < 32 for char in args.display_name):
        parser.error("--display-name must be 1-256 printable characters.")
    if args.grant_admin_consent and not (args.admin_current_user or args.admin_user_object_id):
        parser.error("--grant-admin-consent requires a selected administrator user.")
    if args.grant_profile_consent and not (args.admin_current_user or args.admin_user_object_id):
        parser.error("--grant-profile-consent requires a selected administrator user.")
    if args.dry_run:
        print(json.dumps({
            "dryRun": True, "subscription": args.subscription, "tenantId": args.tenant_id,
            "applicationClientId": args.application_id, "displayName": args.display_name,
            "redirectUris": [uri for uri in (args.webapp_origin, args.localhost_redirect_uri) if uri],
            "scope": SCOPE, "roles": list(ROLE_DESCRIPTIONS), "accessTokenVersion": 2,
            "administrator": "current user" if args.admin_current_user else args.admin_user_object_id,
            "azureCliPreauthorization": args.authorize_azure_cli,
            "userSpecificConsent": args.grant_admin_consent, "createsSecrets": False,
            "delegatedGraphPermissions": ["User.Read"],
            "userSpecificProfileConsent": args.grant_profile_consent,
        }, indent=2))
        return 0
    try:
        graph = GraphClient(graph_token(args.subscription, args.tenant_id))
        print(json.dumps(register(graph, args), indent=2))
    except (RegistrationError, argparse.ArgumentTypeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
