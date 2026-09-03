"""Agent identity inspection -- the accountability story, told with live data.

Every agent in this demo has its own Microsoft Entra *agent identity*, created
automatically by Foundry. This module reads that identity back out of the
directory so the demo can show it rather than assert it.

Why this exists as a first-class part of the demo
-------------------------------------------------
The usual way an unattended service reaches Microsoft Graph is a shared app
registration holding a client secret. That has three problems a customer will
recognise immediately:

1. **The secret expires.** Under tenant governance that purges app secrets on a
   schedule, a secret-based integration starts failing roughly a month after it
   ships -- long after whoever built it has moved on.
2. **The identity is shared.** When five agents authenticate as the same service
   principal, the audit log cannot tell you which one read the mailbox, and
   revoking one agent's access revokes all of them.
3. **Nobody owns it.** An app registration records who *created* it, not who is
   accountable for what it does.

Agent identities address all three, and we verified each claim against a live
tenant rather than taking the documentation's word for it:

- The blueprint behind an agent identity holds **zero key credentials and zero
  password credentials**. It authenticates through a federated identity
  credential instead. There is no secret to expire because there is no secret.
- Each agent gets **its own** identity and its own permission grants, so
  least-privilege and per-agent audit are the default rather than an effort.
- Entra records a **sponsor**: the human accountable for the agent, used to
  reach a person when something goes wrong.

A note on which identity does what
----------------------------------
The *agent* uses its own identity to do its work -- reading the alerts mailbox,
calling Power BI. This module is an **operator inspection tool**: the human
running it authenticates as themselves to read directory metadata. That
separation is deliberate. An agent that could enumerate and modify directory
objects would be a much larger blast radius than one that can read one mailbox.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger("triage.identity")

GRAPH = "https://graph.microsoft.com/v1.0"

# Agent identities and their blueprints are OData *subtypes*, not top-level
# collections. Querying /agentIdentities returns "Resource not found for the
# segment", which reads like the feature is missing when it is simply a
# different URL shape. Recording the working paths here so the next person
# does not repeat that hour.
AGENT_IDENTITY_COLLECTION = f"{GRAPH}/servicePrincipals/microsoft.graph.agentIdentity"
BLUEPRINT_COLLECTION = f"{GRAPH}/applications/microsoft.graph.agentIdentityBlueprint"


@dataclass
class MailboxScopeProof:
    """The result of asking Exchange whether the agent may read a mailbox.

    ``granted`` is the mailbox the agent is supposed to read. ``denied`` are
    mailboxes it must not. Both halves matter: a policy that grants access
    proves nothing on its own, because an unscoped app also grants access.
    """

    granted: list[str] = field(default_factory=list)
    denied: list[str] = field(default_factory=list)

    @property
    def is_scoped(self) -> bool:
        return bool(self.granted) and bool(self.denied)


@dataclass
class AgentIdentityReport:
    """What the directory knows about one agent."""

    display_name: str
    object_id: str
    app_id: str
    enabled: bool
    blueprint_name: str = ""
    blueprint_object_id: str = ""
    key_credentials: int = 0
    password_credentials: int = 0
    federated_credentials: list[str] = field(default_factory=list)
    sponsors: list[str] = field(default_factory=list)
    graph_app_roles: list[str] = field(default_factory=list)
    azure_roles: list[str] = field(default_factory=list)
    scope_proof: MailboxScopeProof | None = None
    #: Only set for a conventional app registration; agent identities have none.
    secret_expires_at: str = ""
    is_agent_identity: bool = True

    @property
    def is_secretless(self) -> bool:
        """True when the identity cannot be authenticated with a stored secret.

        This is the claim the demo rests on, so it is computed from the
        directory rather than configured. Federated credentials are required
        as well as absent secrets: an identity with neither has no way to
        authenticate at all, which is not the same as being secretless.
        """
        return (
            self.key_credentials == 0
            and self.password_credentials == 0
            and bool(self.federated_credentials)
        )

    @property
    def identity_matches_app(self) -> bool:
        """Agent identities have equal object id and app id, unlike other SPs."""
        return bool(self.app_id) and self.object_id.lower() == self.app_id.lower()

    @property
    def holds_mail_permission(self) -> bool:
        """Whether this agent can read mail at all.

        Used to decide whether a mailbox-scope claim is meaningful for it.
        """
        return any(role.startswith("Mail.") for role in self.graph_app_roles)

    @property
    def short_name(self) -> str:
        """The agent's own name, without the account/project/suffix scaffolding.

        Foundry names these ``<account>-<project>-<agent>-AgentIdentity``.
        """
        name = self.display_name
        for suffix in ("-AgentIdentity", "AgentIdentity"):
            if name.endswith(suffix):
                name = name[: -len(suffix)].rstrip("-")
                break
        return name.rsplit("-", 1)[-1] if "-" in name else name


def project_name_prefix(project_endpoint: str) -> str:
    """Derive the agent-identity name prefix from a Foundry project endpoint.

    Foundry names agent identities ``<account>-<project>-<agent>-AgentIdentity``,
    so the prefix can be reconstructed from the endpoint rather than configured
    separately -- one less value to keep in sync.

    ``https://acct.services.ai.azure.com/api/projects/proj`` -> ``acct-proj``
    """
    if not project_endpoint:
        return ""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(project_endpoint)
        account = (parsed.hostname or "").split(".", 1)[0]
        parts = [p for p in (parsed.path or "").split("/") if p]
        project = parts[parts.index("projects") + 1] if "projects" in parts else ""
    except (ValueError, IndexError):
        return ""
    return f"{account}-{project}" if account and project else account


def load_app_registration(
    reader: GraphReader,
    *,
    app_id: str,
    scope_proof: MailboxScopeProof | None = None,
) -> AgentIdentityReport | None:
    """Report on a conventional app registration, for contrast.

    Mailbox ingestion cannot use an agent identity: Exchange rejects them for
    app-only mail access, verified as a 401 against the same token Graph's
    directory endpoint accepted. So one ordinary app registration remains, and
    it is worth showing side by side with the agent identities rather than
    hiding it. It is the only thing in the deployment holding a secret, and the
    only thing with an expiry date.
    """
    if not app_id:
        return None
    try:
        app = reader.get(f"{GRAPH}/applications(appId='{app_id}')")
    except Exception as exc:
        logger.warning("Could not read app registration (%s)", type(exc).__name__)
        return None

    passwords = app.get("passwordCredentials") or []
    report = AgentIdentityReport(
        display_name=str(app.get("displayName", "")),
        object_id=str(app.get("id", "")),
        app_id=app_id,
        enabled=True,
        key_credentials=len(app.get("keyCredentials") or []),
        password_credentials=len(passwords),
        is_agent_identity=False,
    )
    report.secret_expires_at = str((passwords[0] or {}).get("endDateTime", "")) if passwords else ""

    try:
        sp = reader.get(f"{GRAPH}/servicePrincipals(appId='{app_id}')")
        _fill_graph_roles(reader, report, str(sp.get("id", "")))
    except Exception as exc:
        logger.warning("Could not read app service principal (%s)", type(exc).__name__)

    if scope_proof is not None and report.holds_mail_permission:
        report.scope_proof = scope_proof
    return report


def load_azure_roles(principal_id: str, *, subscription_id: str = "") -> list[str]:
    """List Azure RBAC roles held by a principal, resolved to readable names.

    Separate from Graph app roles on purpose. The controller holds no Graph
    permissions at all but does hold Azure roles, and a panel that reported
    only the former would say "permissions: none" about the one component that
    actually acts -- true, and badly misleading.

    Shells out to the Azure CLI rather than calling ARM directly: this is an
    operator inspection command, and the operator already has a CLI login.
    """
    import json
    import subprocess

    if not principal_id:
        return []
    try:
        proc = subprocess.run(
            [
                "az", "role", "assignment", "list",
                "--assignee", principal_id, "--all",
                "--query", "[].{role:roleDefinitionName, scope:scope}",
                "-o", "json",
            ],
            capture_output=True, text=True, timeout=60, shell=True,
        )
        if proc.returncode != 0:
            return []
        assignments = json.loads(proc.stdout or "[]")
    except Exception as exc:
        logger.warning("Could not read Azure role assignments (%s)", type(exc).__name__)
        return []

    out: list[str] = []
    for item in assignments:
        role = str(item.get("role", ""))
        scope = str(item.get("scope", ""))
        # The resource name is the useful part; the full ARM path is noise.
        resource = scope.rstrip("/").rsplit("/", 1)[-1] if scope else ""
        out.append(f"{role} on {resource}" if resource else role)
    return out


class GraphReader(Protocol):
    """Minimal read surface, so tests can supply recorded directory data."""

    def get(self, url: str) -> dict[str, Any]: ...


class HttpGraphReader:
    """Reads the directory as the *operator*, not as the agent.

    Uses the caller's own credentials. Directory enumeration is an
    administrative action and deliberately is not something the agent itself
    is permitted to do.
    """

    def __init__(self, token: str) -> None:
        self._token = token

    def get(self, url: str) -> dict[str, Any]:
        import httpx

        resp = httpx.get(
            url, headers={"Authorization": f"Bearer {self._token}"}, timeout=30
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload if isinstance(payload, dict) else {}


def _first(items: list[dict[str, Any]], key: str) -> str:
    return str(items[0].get(key, "")) if items else ""


def load_agent_identity(
    reader: GraphReader,
    *,
    display_name_contains: str,
    name_prefix: str = "",
    scope_proof: MailboxScopeProof | None = None,
) -> AgentIdentityReport | None:
    """Build a report for an agent identity.

    ``name_prefix`` scopes the match to one Foundry project. Without it, a
    tenant running several projects will happily return another project's
    ``bi-data-quality`` agent, which looks right and is wrong -- exactly the
    kind of error that survives review because the output is plausible.

    Returns ``None`` rather than raising when nothing matches: a tenant without
    agent identities is a legitimate state (the feature is recent), and the CLI
    should explain that rather than produce a stack trace in front of a customer.
    """
    identities = reader.get(AGENT_IDENTITY_COLLECTION).get("value", [])
    needle = display_name_contains.lower()
    prefix = name_prefix.lower()
    match = next(
        (
            i
            for i in identities
            if needle in str(i.get("displayName", "")).lower()
            and str(i.get("displayName", "")).lower().startswith(prefix)
        ),
        None,
    )
    if match is None:
        logger.info(
            "No agent identity matched %r (prefix %r)", display_name_contains, name_prefix
        )
        return None

    object_id = str(match.get("id", ""))

    # The collection projection omits appId. Re-read the single object so the
    # "object id == app id" property can be shown truthfully rather than blank.
    detail = match
    if object_id and not match.get("appId"):
        try:
            detail = reader.get(f"{GRAPH}/servicePrincipals/{object_id}")
        except Exception as exc:
            logger.warning("Could not re-read agent identity (%s)", type(exc).__name__)

    report = AgentIdentityReport(
        display_name=str(detail.get("displayName", "")),
        object_id=object_id,
        app_id=str(detail.get("appId", "")),
        enabled=bool(detail.get("accountEnabled", False)),
    )

    blueprint_id = str(detail.get("agentIdentityBlueprintId", "") or "")
    if blueprint_id:
        _fill_blueprint(reader, report, blueprint_id)

    _fill_sponsors(reader, report, object_id)
    _fill_graph_roles(reader, report, object_id)

    # Only claim a mailbox boundary for an agent that can actually read mail.
    # Showing "granted / denied" against an agent with no mail permission
    # implies a control that is not doing anything, and a customer who checks
    # will find the agent has no mailbox access to be scoped in the first place.
    if scope_proof is not None and report.holds_mail_permission:
        report.scope_proof = scope_proof
    return report


def _fill_blueprint(
    reader: GraphReader, report: AgentIdentityReport, blueprint_app_id: str
) -> None:
    try:
        blueprint = reader.get(f"{GRAPH}/applications(appId='{blueprint_app_id}')")
    except Exception as exc:
        logger.warning("Could not read blueprint (%s)", type(exc).__name__)
        return

    report.blueprint_name = str(blueprint.get("displayName", ""))
    report.blueprint_object_id = str(blueprint.get("id", ""))
    report.key_credentials = len(blueprint.get("keyCredentials") or [])
    report.password_credentials = len(blueprint.get("passwordCredentials") or [])

    if not report.blueprint_object_id:
        return
    try:
        fics = reader.get(
            f"{GRAPH}/applications/{report.blueprint_object_id}/federatedIdentityCredentials"
        )
    except Exception as exc:
        logger.warning("Could not read federated credentials (%s)", type(exc).__name__)
        return
    report.federated_credentials = [
        str(f.get("name", "")) for f in fics.get("value", []) if f.get("name")
    ]


def _fill_sponsors(
    reader: GraphReader, report: AgentIdentityReport, object_id: str
) -> None:
    try:
        sponsors = reader.get(f"{GRAPH}/servicePrincipals/{object_id}/sponsors")
    except Exception as exc:
        logger.warning("Could not read sponsors (%s)", type(exc).__name__)
        return
    report.sponsors = [
        str(s.get("displayName", "")) for s in sponsors.get("value", []) if s.get("displayName")
    ]


def _fill_graph_roles(
    reader: GraphReader, report: AgentIdentityReport, object_id: str
) -> None:
    """Resolve app role assignments to human-readable permission names.

    The assignment records a role *id*, not its name, so the names have to be
    resolved from the resource service principal. Showing "Mail.Read" rather
    than "810c84a8-..." is the difference between a slide a customer follows
    and one they nod politely at.
    """
    try:
        assignments = reader.get(
            f"{GRAPH}/servicePrincipals/{object_id}/appRoleAssignments"
        ).get("value", [])
    except Exception as exc:
        logger.warning("Could not read app role assignments (%s)", type(exc).__name__)
        return

    role_names: list[str] = []
    resource_cache: dict[str, dict[str, str]] = {}
    for assignment in assignments:
        resource_id = str(assignment.get("resourceId", ""))
        role_id = str(assignment.get("appRoleId", ""))
        if not resource_id or not role_id:
            continue
        if resource_id not in resource_cache:
            try:
                resource = reader.get(f"{GRAPH}/servicePrincipals/{resource_id}")
                resource_cache[resource_id] = {
                    str(r.get("id", "")): str(r.get("value", ""))
                    for r in resource.get("appRoles", [])
                }
            except Exception as exc:
                logger.warning("Could not resolve resource SP (%s)", type(exc).__name__)
                resource_cache[resource_id] = {}
        name = resource_cache[resource_id].get(role_id)
        role_names.append(
            f"{name} on {assignment.get('resourceDisplayName', 'resource')}"
            if name
            else f"{role_id} on {assignment.get('resourceDisplayName', 'resource')}"
        )
    report.graph_app_roles = role_names
