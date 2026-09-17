"""Bounded, read-only operator discovery of SQL identities and their Azure hosts.

Expected scope comes from the tenant-root management group, not resource IDs in
a profile. All subscription resources, supported child surfaces and Logic App
invokers are inspected. UAMI reverse association is an explicit preview
cross-check, not the sole completeness proof. Ordinary service principals,
federated UAMIs, unreadable scopes and unmodelled hosts/expressions are refused.

These reads do not prove arbitrary non-Azure clients are stopped. Such clients
need a different reviewed observer; they cannot enter this supported inventory.
An unmatched HTTP host is not proof that a workflow is unrelated. Authentication,
audience and destination must identify a supported invoker or a known read-only
Graph/ARM request. Unresolved relays remain coverage failures even when disabled.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import SplitResult, parse_qsl, unquote, urlsplit
from uuid import UUID, uuid4

import httpx

from triage.monitoring.deployment_authority import AuthoritySnapshot
from triage.monitoring.deployment_contracts import (
    CapturedWriter,
    DeploymentError,
    DiscoveryCapture,
    EnumerationPage,
    ResetTarget,
    WriterBinding,
    WriterSpec,
    canonical_id,
    fingerprint,
)
from triage.monitoring.deployment_schema import declared_write_procedures

ARM = "https://management.azure.com"
GRAPH = "https://graph.microsoft.com"
AI_SCOPE = "https://ai.azure.com/.default"
GRAPH_SCOPE = GRAPH + "/.default"
ARM_SCOPE = ARM + "/.default"
SQL_SETTINGS = frozenset({"AZURE_SQL_SERVER", "AZURE_SQL_DATABASE", "AZURE_CLIENT_ID", "MONITORING_TENANT_ID"})
VERSIONS = {
    "microsoft.web/sites": "2023-12-01",
    "microsoft.web/sites/slots": "2023-12-01",
    "microsoft.app/containerapps": "2024-03-01",
    "microsoft.app/jobs": "2024-03-01",
    "microsoft.logic/workflows": "2019-05-01",
    "microsoft.managedidentity/userassignedidentities": "2023-01-31",
    "microsoft.cognitiveservices/accounts": "2025-06-01",
}
KINDS = {
    "microsoft.web/sites": "app_service", "microsoft.web/sites/slots": "app_service",
    "microsoft.app/containerapps": "container_app", "microsoft.app/jobs": "container_app_job",
    "microsoft.logic/workflows": "logic_app",
}
# SQL modules are inspected through the database authority catalogue. These
# resource types add no independent scheduler; SQL job agents remain unsupported.
# Unknown hosts are not excluded merely because ARM omits identity details.
NON_HOST_TYPES = frozenset({
    "microsoft.network/virtualnetworks", "microsoft.network/virtualnetworks/subnets",
    "microsoft.network/networksecuritygroups", "microsoft.network/routetables",
    "microsoft.network/publicipaddresses", "microsoft.network/natgateways",
    "microsoft.network/privateendpoints", "microsoft.network/privatednszones",
    "microsoft.network/privatednszones/virtualnetworklinks", "microsoft.network/networkinterfaces",
    "microsoft.storage/storageaccounts", "microsoft.keyvault/vaults",
    "microsoft.insights/components", "microsoft.insights/actiongroups",
    "microsoft.insights/metricalerts", "microsoft.insights/scheduledqueryrules",
    "microsoft.operationalinsights/workspaces", "microsoft.containerregistry/registries",
    "microsoft.app/managedenvironments", "microsoft.web/serverfarms",
    "microsoft.web/connections",
    "microsoft.eventhub/namespaces", "microsoft.servicebus/namespaces",
    "microsoft.cognitiveservices/accounts/projects",
    "microsoft.sql/servers", "microsoft.sql/servers/databases",
    "microsoft.sql/servers/elasticpools", "microsoft.sql/servers/administrators",
    "microsoft.sql/servers/azureadonlyauthentications",
    "microsoft.sql/servers/auditingsettings",
    "microsoft.sql/servers/databases/auditingsettings",
})


class DiscoveryThrottled(DeploymentError):
    def __init__(self, seconds: int) -> None:
        self.retry_after_seconds = seconds
        super().__init__(f"Operator inventory was throttled; retry read-only preparation after {seconds} seconds")


def _id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"/subscriptions/[0-9a-fA-F-]{36}/resourceGroups/[A-Za-z0-9_.()-]+/"
        r"providers/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.()~-]+)+", value, re.I,
    ):
        raise DeploymentError("Resource identity is malformed or outside supported ARM locators")
    canonical_id(value.split("/")[2])
    return value


def _writer_id(locator: str) -> str:
    return "deployment:" + fingerprint(locator.casefold())


def _properties(payload: dict, resource: str) -> dict:
    if str(payload.get("id", "")).casefold() != resource.casefold() or not isinstance(payload.get("properties"), dict):
        raise DeploymentError("Resource read returned another identity or incomplete properties")
    return payload["properties"]


def _safe(value: Any, *, settings: bool = False) -> Any:
    """Persist hashes of nonsecret projections, never a general settings/code dump."""
    if isinstance(value, list):
        return [_safe(item, settings=settings) for item in value]
    if not isinstance(value, dict):
        return value
    allowed = SQL_SETTINGS if settings else {
        "id", "name", "type", "value", "data", "properties", "state", "status", "active",
        "enabled", "provisioningState", "clientId", "principalId", "tenantId", "appId",
        "servicePrincipalType", "appOwnerOrganizationId", "parent", "subscriptionId",
        "actions", "notActions", "dataActions", "notDataActions", "identity", "userAssignedIdentities",
        "instance_identity", "client_id", "principal_id", "version", "etag", "totalCount",
    }
    result = {key: _safe(item) for key, item in value.items() if key in allowed}
    if "userAssignedIdentities" in value:
        result["userAssignedIdentities"] = sorted(value["userAssignedIdentities"])
    return result


class AzureDeploymentDiscovery:
    """Synchronous collector. Credentials/transports are explicit and never chained."""

    def __init__(
        self, credential: Any, target: ResetTarget, *, transport: httpx.BaseTransport | None = None,
        allow_identity_association_preview: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_requests: int = 2_000, max_pages: int = 100, max_resources: int = 10_000,
        max_seconds: int = 120,
    ) -> None:
        if getattr(credential, "target", None) != target or not callable(getattr(credential, "get_token", None)):
            raise DeploymentError("Discovery requires the explicitly pinned operator credential")
        for bound in (max_requests, max_pages, max_resources, max_seconds):
            if type(bound) is not int or bound < 1:
                raise ValueError("Operator discovery budgets must be positive strict integers")
        self.credential, self.target = credential, target
        self.allow_preview = allow_identity_association_preview
        self.clock, self.monotonic, self.sleep = clock, monotonic, sleep
        self.max_requests, self.max_pages = max_requests, max_pages
        self.max_resources, self.max_seconds = max_resources, max_seconds
        self.http = httpx.Client(transport=transport or httpx.HTTPTransport(retries=0), timeout=15, follow_redirects=False)
        self.pages: list[EnumerationPage] = []
        self._started = self.monotonic()
        self._requests = 0
        self._last_association: float | None = None
        self._foundry_endpoints: set[str] = set()
        self._directory: dict[str, tuple[str, str]] = {}
        self._verified_subscriptions: set[str] = set()
        self._scope_subscriptions: set[str] | None = None

    def close(self) -> None:
        self.http.close()

    def _request(self, url: str, *, method: str = "GET", settings: bool = False) -> dict:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https" or parsed.username or parsed.password or parsed.port
            or parsed.fragment or "%" in parsed.path or "\\" in parsed.path
        ):
            raise DeploymentError("Discovery endpoint is outside the reviewed URI contract")
        if parsed.netloc == "management.azure.com":
            scope = ARM_SCOPE
        elif parsed.netloc == "graph.microsoft.com" and parsed.path.startswith("/v1.0/servicePrincipals"):
            scope = GRAPH_SCOPE
        elif any(url.startswith(endpoint + "/agents") for endpoint in self._foundry_endpoints):
            scope = AI_SCOPE
        else:
            raise DeploymentError("Discovery refused an unexpected host or project endpoint")
        if method != "GET" and not (
            method == "POST" and scope == ARM_SCOPE
            and (
                re.fullmatch(r"/subscriptions/.+/providers/Microsoft\.Web/sites/[^/]+(?:/slots/[^/]+)?/config/appsettings/list", parsed.path, re.I)
                or re.fullmatch(r"/subscriptions/.+/providers/Microsoft\.ManagedIdentity/userAssignedIdentities/[^/]+/listAssociatedResources", parsed.path, re.I)
            )
        ):
            raise DeploymentError("Operator discovery permits only GET and the two reviewed read-only POST actions")
        if self._requests >= self.max_requests or self.monotonic() - self._started >= self.max_seconds:
            raise DeploymentError("Operator discovery budget expired; no partial inventory is authoritative")
        if parsed.path.endswith("/listAssociatedResources"):
            if not self.allow_preview:
                raise DeploymentError("Explicit --allow-identity-association-preview is required for the reverse-identity read")
            if self._last_association is not None:
                wait = max(0.0, 1.0 - (self.monotonic() - self._last_association))
                if self.monotonic() - self._started + wait >= self.max_seconds:
                    raise DeploymentError("Identity read pacing would exceed the capture validity budget")
                self.sleep(wait)
            self._last_association = self.monotonic()
        self._requests += 1
        token = self.credential.get_token(scope).token
        try:
            with self.http.stream(method, url, headers={"Authorization": f"Bearer {token}"}) as response:
                if response.status_code == 429:
                    value = response.headers.get("Retry-After", "60")
                    try:
                        seconds = int(value) if value.isdecimal() else int(
                            (parsedate_to_datetime(value).astimezone(UTC) - self.clock()).total_seconds()
                        )
                    except (TypeError, ValueError, OverflowError):
                        seconds = 60
                    raise DiscoveryThrottled(max(1, seconds))
                if response.status_code != 200:
                    raise DeploymentError(f"Operator inventory read returned HTTP {response.status_code}; coverage is unproved")
                body = bytearray()
                for chunk in response.iter_bytes():
                    if len(body) + len(chunk) > 2_097_152:
                        raise DeploymentError("Operator evidence response exceeded its bounded size")
                    body.extend(chunk)
                payload = json.loads(body)
        except (httpx.HTTPError, ValueError) as exc:
            raise DeploymentError("Operator inventory transport or JSON read failed") from exc
        if not isinstance(payload, dict):
            raise DeploymentError("Operator evidence must be an object")
        for key in ("tenantId", "tenant_id"):
            if key in payload and canonical_id(payload[key]) != self.target.tenant_id:
                raise DeploymentError("Operator evidence belongs to another tenant")
        values = payload.get("value", payload.get("data"))
        projection = _safe(payload.get("properties"), settings=True) if settings else _safe(payload)
        continuation = [payload.get(key) for key in ("nextLink", "@odata.nextLink", "continuationUri", "continuationToken", "last_id")]
        self.pages.append(EnumerationPage(
            method=method, scope=parsed.scheme + "://" + parsed.netloc + parsed.path,
            request_hash=fingerprint({"method": method, "url": url}),
            response_hash=fingerprint(projection), row_count=len(values) if isinstance(values, list) else 1,
            continuation_hash=fingerprint(continuation) if any(continuation) else None,
            observed_at=self.clock(),
        ))
        return payload

    def _list(self, url: str, *, method: str = "GET", foundry: bool = False, total: bool = False) -> list[dict]:
        original, fixed = urlsplit(url), dict(parse_qsl(urlsplit(url).query))
        seen, result, expected_total = set(), [], None
        current = url
        for _ in range(self.max_pages):
            if current in seen:
                raise DeploymentError("Operator pagination repeated a position")
            seen.add(current)
            payload = self._request(current, method=method)
            values = payload.get("data" if foundry else "value")
            if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
                raise DeploymentError("Operator list is malformed, not an empty inventory")
            result.extend(values)
            ids = [row.get("id") for row in result]
            if ids and all(isinstance(value, str) for value in ids) and len(set(ids)) != len(ids):
                raise DeploymentError("Operator pages repeated a resource identity")
            if len(result) > self.max_resources:
                raise DeploymentError("Operator inventory exceeded its bounded row budget")
            if total:
                count = payload.get("totalCount")
                if type(count) is not int or count < 0 or expected_total not in (None, count):
                    raise DeploymentError("Reverse identity inventory has a missing or changing total")
                expected_total = count
            links = [payload[key] for key in ("nextLink", "@odata.nextLink", "continuationUri") if payload.get(key)]
            if any(not isinstance(link, str) for link in links) or len(set(links)) > 1:
                raise DeploymentError("Operator continuation links disagree")
            token = payload.get("continuationToken")
            if token is not None and (not isinstance(token, str) or not token):
                raise DeploymentError("Operator continuation token is malformed")
            if foundry:
                if type(payload.get("has_more")) is not bool or links or token:
                    raise DeploymentError("Foundry list completion/pagination is unsupported")
                if payload["has_more"]:
                    last = payload.get("last_id")
                    if not isinstance(last, str) or not last:
                        raise DeploymentError("Foundry continuation identity is missing")
                    current = str(httpx.URL(url).copy_add_param("after", last))
                else:
                    current = ""
            else:
                current = links[0] if links else (
                    str(httpx.URL(url).copy_add_param("continuationToken", token)) if token else ""
                )
            if not current:
                if any(payload.get(key) for key in ("hasMore", "has_more", "nextPageToken")):
                    raise DeploymentError("Operator list signalled an unhandled continuation")
                if total and len(result) != expected_total:
                    raise DeploymentError("Reverse identity inventory did not exhaust its declared total")
                return result
            parsed = urlsplit(current)
            query = parse_qsl(parsed.query)
            values = dict(query)
            if (
                parsed.scheme != original.scheme or parsed.netloc != original.netloc
                or parsed.path != original.path or parsed.fragment or len(values) != len(query)
                or any(values.get(key) != value for key, value in fixed.items())
                or not set(values) <= set(fixed) | {"after", "$skiptoken", "skiptoken", "skipToken", "$skip", "continuationToken"}
                or token is not None and values.get("continuationToken") != token
            ):
                raise DeploymentError("Operator continuation left its original path, tenant scope or API version")
        raise DeploymentError("Operator list exceeded its page budget")

    def _arm(self, resource: str, version: str) -> dict:
        resource = _id(resource)
        subscription = canonical_id(resource.split("/")[2])
        if self._scope_subscriptions is not None and subscription not in self._scope_subscriptions:
            raise DeploymentError("Resource/identity/connection left the tenant-root subscription closure")
        if subscription not in self._verified_subscriptions:
            metadata = self._request(f"{ARM}/subscriptions/{subscription}?api-version=2022-12-01")
            if (
                metadata.get("subscriptionId") != subscription or metadata.get("tenantId") != self.target.tenant_id
                or metadata.get("state") != "Enabled"
            ):
                raise DeploymentError("Resource subscription tenant/state is unverified")
            self._verified_subscriptions.add(subscription)
        return self._request(f"{ARM}{resource}?api-version={version}")

    def _principal(self, client_id: str | None = None, object_id: str | None = None) -> tuple[str, str]:
        key = client_id or object_id
        if key in self._directory:
            result = self._directory[key]
            if client_id and result[0] != client_id or object_id and result[1] != object_id:
                raise DeploymentError("Directory identity bindings disagree")
            return result
        locator = f"(appId='{canonical_id(client_id)}')" if client_id else "/" + canonical_id(object_id)
        payload = self._request(
            f"{GRAPH}/v1.0/servicePrincipals{locator}"
            "?$select=id,appId,servicePrincipalType,appOwnerOrganizationId,passwordCredentials,keyCredentials"
        )
        actual = canonical_id(payload.get("appId")), canonical_id(payload.get("id"))
        if (
            client_id and actual[0] != client_id or object_id and actual[1] != object_id
            or payload.get("servicePrincipalType") != "ManagedIdentity"
            or payload.get("appOwnerOrganizationId") not in (None, self.target.tenant_id)
            or payload.get("passwordCredentials") != [] or payload.get("keyCredentials") != []
            or actual[1] == self.target.deployer_object_id
        ):
            raise DeploymentError("Uninspectable/non-MI, credential-bearing, reused-operator or mismatched directory identity")
        self._directory[actual[0]] = self._directory[actual[1]] = actual
        return actual

    def _identities(self, payload: dict) -> dict[str, tuple[str, str | None]]:
        identity = payload.get("identity")
        if identity is None:
            return {}
        if not isinstance(identity, dict):
            raise DeploymentError("Resource identity metadata is unreadable")
        if identity.get("type") == "None":
            return {}
        if identity.get("type") not in {"SystemAssigned", "UserAssigned", "SystemAssigned, UserAssigned", "SystemAssigned,UserAssigned"}:
            raise DeploymentError("Resource identity kind is unsupported")
        found = {}
        if "SystemAssigned" in identity.get("type", ""):
            if canonical_id(identity.get("tenantId")) != self.target.tenant_id:
                raise DeploymentError("Resource system identity belongs to another tenant")
            client, principal = self._principal(object_id=identity.get("principalId"))
            found[client] = principal, None
        assigned = identity.get("userAssignedIdentities", {})
        if not isinstance(assigned, dict):
            raise DeploymentError("User-assigned identity attachment metadata is malformed")
        for resource in assigned:
            if not re.search(r"/providers/Microsoft\.ManagedIdentity/userAssignedIdentities/[^/]+$", resource, re.I):
                raise DeploymentError("Resource has an unsupported identity attachment")
            properties = _properties(self._arm(resource, "2023-01-31"), resource)
            client, principal = self._principal(
                client_id=canonical_id(properties.get("clientId")),
                object_id=canonical_id(properties.get("principalId")),
            )
            if canonical_id(properties.get("tenantId")) != self.target.tenant_id or client in found:
                raise DeploymentError("Identity attachment tenant or uniqueness is unproved")
            found[client] = principal, resource
        return found

    def _settings(self, settings: Any, identities: dict, wanted: set[str]) -> tuple[str, str, dict] | None:
        if not isinstance(settings, dict):
            raise DeploymentError("Writer SQL settings are uninspectable")
        relevant = set(identities) & wanted
        if not relevant:
            return None
        selected = settings.get("AZURE_CLIENT_ID")
        if selected is None:
            system = [client for client in relevant if identities[client][1] is None]
            if len(system) != 1:
                raise DeploymentError("Writer must explicitly select its SQL user-assigned identity")
            selected = system[0]
        if selected not in relevant or relevant != {selected}:
            raise DeploymentError("An attached SQL-capable identity is unused, ambiguous or selected differently")
        safe = {key: settings[key] for key in SQL_SETTINGS if key in settings}
        if (
            not isinstance(safe.get("AZURE_SQL_SERVER"), str)
            or safe["AZURE_SQL_SERVER"].casefold() != self.target.server
            or safe.get("AZURE_SQL_DATABASE") != self.target.database
            or safe.get("MONITORING_TENANT_ID", self.target.tenant_id) != self.target.tenant_id
        ):
            raise DeploymentError("SQL settings on a SQL-capable resource name another or unreadable database")
        return selected, identities[selected][0], safe

    def _containers(self, template: Any) -> list[dict]:
        if not isinstance(template, dict) or not isinstance(template.get("containers"), list) or not template["containers"]:
            raise DeploymentError("Container definition is not inspectable")
        result = []
        for container in template["containers"]:
            if not isinstance(container, dict) or not isinstance(container.get("env"), list):
                raise DeploymentError("Container environment metadata is not inspectable")
            values = {}
            for row in container["env"]:
                if not isinstance(row, dict) or not isinstance(row.get("name"), str) or row["name"] in values:
                    raise DeploymentError("Container settings contain a duplicate or malformed name")
                if row["name"] in SQL_SETTINGS:
                    if not isinstance(row.get("value"), str) or row.get("secretRef"):
                        raise DeploymentError("SQL/identity settings must be explicit nonsecret values, not secret references")
                    values[row["name"]] = row["value"]
            result.append({"settings": values, "image_hash": fingerprint(container.get("image"))})
        return result

    def inspect_direct(self, writer: WriterSpec, wanted: set[str]) -> CapturedWriter | None:
        """Observe one known resource; full expected coverage is collected separately."""
        evidence, surfaces = [], []
        selected: tuple[str, str, dict] | None = None
        if writer.kind == "foundry_agent":
            self._foundry_endpoints.add(writer.project_endpoint)
            payload = self._request(f"{writer.locator}?api-version=v1")
            if payload.get("name") != writer.agent_name or not isinstance(payload.get("instance_identity"), dict):
                raise DeploymentError("Foundry agent identity/status metadata is incomplete")
            identity = payload["instance_identity"]
            client, principal = self._principal(
                client_id=canonical_id(identity.get("client_id")),
                object_id=canonical_id(identity.get("principal_id")),
            )
            versions = self._list(f"{writer.locator}/versions?api-version=v1&include_drafts=true", foundry=True)
            if client not in wanted:
                return None
            if not versions:
                raise DeploymentError("Foundry SQL writer has no observable version")
            for version in versions:
                number = version.get("version")
                if not isinstance(number, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", number):
                    raise DeploymentError("Foundry version identity is missing")
                definition = version.get("definition")
                if not isinstance(definition, dict) or definition.get("kind") != "hosted":
                    raise DeploymentError("A SQL-capable Foundry version has an uninspectable hosted definition")
                current = self._settings(definition.get("environment_variables"), {client: (principal, None)}, wanted)
                if current is None:
                    raise DeploymentError("Foundry version SQL binding is missing")
                selected = current
                surfaces.append(writer.locator + "/versions/" + number)
                evidence.append({"version": number, "settings": current[2], "image_hash": fingerprint(definition.get("container"))})
            if len(set(surfaces)) != len(surfaces):
                raise DeploymentError("Foundry version enumeration repeated an identity")
            state = "disabled" if payload.get("status") == "Disabled" else "running"
            evidence.append({"instance_identity": identity, "status": payload.get("status")})
        else:
            resource = writer.resource_id
            kind = next(kind for kind, mapped in KINDS.items() if mapped == writer.kind)
            payload = self._arm(resource, VERSIONS[kind])
            properties = _properties(payload, resource)
            identities = self._identities(payload)
            if not set(identities) & wanted:
                return None
            evidence.append({"identity": identities, "etag": payload.get("etag")})
            if writer.kind == "app_service":
                settings = self._request(f"{ARM}{resource}/config/appsettings/list?api-version=2023-12-01", method="POST", settings=True)
                selected = self._settings(settings.get("properties"), identities, wanted)
                state = "stopped" if properties.get("state") == "Stopped" else "running"
                if "/slots/" not in resource.lower():
                    slots = self._list(f"{ARM}{resource}/slots?api-version=2023-12-01")
                    for slot in slots:
                        slot_id = _id(slot.get("id"))
                        if not slot_id.casefold().startswith(resource.casefold() + "/slots/"):
                            raise DeploymentError("App Service slot belongs to another root")
                        child = self.inspect_direct(WriterSpec(
                            writer_id=_writer_id(slot_id), kind="app_service", resource_id=slot_id,
                        ), wanted)
                        surfaces.append(slot_id)
                        if child is not None:
                            evidence.append({"slot": slot_id, "binding": child.resource_binding_hash})
                            if child.state != "stopped":
                                state = "running"
            elif writer.kind in {"container_app", "container_app_job"}:
                configurations = self._containers(properties.get("template"))
                if writer.kind == "container_app":
                    revisions = self._list(f"{ARM}{resource}/revisions?api-version=2024-03-01")
                    state = "inactive"
                    for revision in revisions:
                        rid = _id(revision.get("id"))
                        if not rid.casefold().startswith(resource.casefold() + "/revisions/"):
                            raise DeploymentError("Container revision belongs to another app")
                        full = _properties(self._arm(rid, "2024-03-01"), rid)
                        configurations.extend(self._containers(full.get("template")))
                        replicas = self._list(f"{ARM}{rid}/replicas?api-version=2024-03-01")
                        if full.get("active") is not False or replicas:
                            state = "running"
                        surfaces.append(rid)
                else:
                    executions = self._list(f"{ARM}{resource}/executions?api-version=2024-03-01")
                    state = "stopped" if properties.get("configuration", {}).get("triggerType") == "Manual" else "running"
                    for execution in executions:
                        eid = _id(execution.get("id"))
                        if not eid.casefold().startswith(resource.casefold() + "/executions/"):
                            raise DeploymentError("Container execution belongs to another job")
                        if execution.get("properties", {}).get("status") not in {"Succeeded", "Failed", "Stopped"}:
                            state = "running"
                        surfaces.append(eid)
                for config in configurations:
                    if any(key in config["settings"] for key in ("AZURE_SQL_SERVER", "AZURE_SQL_DATABASE")):
                        current = self._settings(config["settings"], identities, wanted)
                        if selected and current[:2] != selected[:2]:
                            raise DeploymentError("Container versions select different SQL identities")
                        selected = current
                    evidence.append(config)
            else:
                parameters, actions = self._workflow(properties)
                connection_reads = [
                    self._sql_connection(action["inputs"], parameters, identities)
                    for action in actions if action.get("type") == "ApiConnection"
                ]
                if not connection_reads:
                    raise DeploymentError("Direct Logic App SQL connection binding has no inspectable SQL action")
                for connection in connection_reads:
                    current = self._settings(connection["settings"], identities, wanted)
                    if current is None or selected is not None and current[:2] != selected[:2]:
                        raise DeploymentError("Workflow SQL actions do not share one current registered identity")
                    selected = current
                    evidence.append(connection)
                runs = self._list(f"{ARM}{resource}/runs?api-version=2019-05-01")
                state = "disabled" if properties.get("state") == "Disabled" and all(
                    row.get("properties", {}).get("status") in {"Succeeded", "Failed", "Cancelled", "Aborted", "TimedOut"}
                    for row in runs
                ) else "running"
                evidence.append(_safe(runs))
            if selected is None:
                raise DeploymentError("Attached SQL authority has no readable SQL configuration")
            evidence.append(selected[2])
        client, principal, _ = selected
        return CapturedWriter(
            writer=writer, identity_client_id=client, identity_object_id=principal,
            expected_sql_sid=UUID(client).bytes_le.hex(), configured_sql_server=self.target.server,
            configured_sql_database=self.target.database,
            resource_binding_hash=fingerprint({
                "resource": writer.locator, "evidence": evidence, "surfaces": sorted(surfaces), "state": state,
            }, domain="deployment.resource.binding.v1"),
            observed_at=self.clock(), state=state, surfaces=tuple(sorted(surfaces)),
        )

    @staticmethod
    def _expression(value: Any, parameters: dict) -> str:
        if not isinstance(value, str):
            raise DeploymentError("Workflow endpoint expression is not inspectable")
        match = re.fullmatch(r"@parameters\('([^']+)'\)", value)
        if match:
            parameter = parameters.get(match[1])
            if not isinstance(parameter, dict) or not isinstance(parameter.get("value"), str):
                raise DeploymentError("Workflow endpoint parameter is missing or secret/opaque")
            resolved = parameter["value"]
            if "@" in resolved:
                raise DeploymentError("Nested/dynamic workflow endpoint values require a reviewed resolver")
            return resolved
        if "@" in value:
            raise DeploymentError("Dynamic workflow endpoint expressions require a reviewed resolver")
        return value

    @staticmethod
    def _http_uri(value: str, purpose: str) -> SplitResult:
        try:
            parsed = urlsplit(value)
            valid = (
                value == value.strip() and parsed.scheme == "https" and parsed.hostname
                and parsed.username is None and parsed.password is None and parsed.port is None
                and not parsed.fragment and "\\" not in value
            )
        except ValueError as exc:
            raise DeploymentError(f"Workflow {purpose} is not an inspectable HTTPS URI") from exc
        if not valid:
            raise DeploymentError(f"Workflow {purpose} is not an inspectable HTTPS URI")
        return parsed

    def _http_authentication(self, inputs: dict, parameters: dict) -> tuple[dict, str]:
        auth = inputs.get("authentication")
        if not isinstance(auth, dict) or auth.get("type") != "ManagedServiceIdentity":
            raise DeploymentError("Workflow HTTP invoker authentication/audience is unresolved")
        audience = self._expression(auth.get("audience"), parameters)
        parsed = self._http_uri(audience, "audience")
        if parsed.query or parsed.path not in {"", "/"}:
            raise DeploymentError("Workflow HTTP audience requires an explicit supported resource binding")
        headers = inputs.get("headers", {})
        if not isinstance(headers, dict) or any(
            not isinstance(name, str) or "@" in name or name.lower() in {
                "authorization", "host", "x-http-method", "x-http-method-override", "x-method-override",
            } for name in headers
        ):
            raise DeploymentError("Workflow HTTP routing/authentication headers cannot establish unrelatedness")
        return auth, audience

    @staticmethod
    def _unrelated_http_read(inputs: dict, destination: SplitResult, audience: str) -> bool:
        if inputs.get("method") not in {"GET", "HEAD"}:
            return False
        # A known audience on an arbitrary relay is insufficient. Only these
        # explicit read-only service destinations establish unrelatedness.
        if audience in {"https://graph.microsoft.com", "https://graph.microsoft.com/"}:
            return destination.netloc == "graph.microsoft.com" and destination.path.startswith(("/v1.0/", "/beta/"))
        if audience in {"https://management.azure.com", "https://management.azure.com/"}:
            return destination.netloc == "management.azure.com" and any(
                destination.path == root or destination.path.startswith(root + "/")
                for root in ("/subscriptions", "/providers", "/tenants")
            )
        return False

    @staticmethod
    def _workflow(properties: dict) -> tuple[dict, list[dict]]:
        definition = properties.get("definition")
        if not isinstance(definition, dict) or not isinstance(definition.get("actions"), dict):
            raise DeploymentError("Workflow actions are unreadable; SQL/invocation closure is unproved")
        defaults, parameters = definition.get("parameters", {}), properties.get("parameters", {})
        if not isinstance(defaults, dict) or not isinstance(parameters, dict):
            raise DeploymentError("Workflow parameters are unreadable")
        parameters = {
            key: {"value": value.get("defaultValue")} for key, value in defaults.items()
            if isinstance(value, dict) and value.get("type", "").lower() not in {"securestring", "secureobject"}
        } | {
            key: value for key, value in parameters.items()
            if defaults.get(key, {}).get("type", "").lower() not in {"securestring", "secureobject"}
        }
        pending, actions = [definition["actions"]], []
        while pending:
            for action in pending.pop().values():
                if not isinstance(action, dict):
                    raise DeploymentError("Workflow action metadata is malformed")
                actions.append(action)
                if len(actions) > 1_000:
                    raise DeploymentError("Workflow action graph exceeds its read budget")
                for block in (action.get("actions"), (action.get("else") or {}).get("actions")):
                    if block is not None:
                        if not isinstance(block, dict):
                            raise DeploymentError("Workflow child actions are malformed")
                        pending.append(block)
        return parameters, actions

    @staticmethod
    def _selected_identity(authentication: Any, identities: dict) -> tuple[str, str]:
        if not isinstance(authentication, dict) or authentication.get("type") != "ManagedServiceIdentity":
            raise DeploymentError("Workflow connection must use its actual managed identity")
        resource = authentication.get("identity")
        choices = [
            (client, value[0]) for client, value in identities.items()
            if value[1] == resource
        ]
        if len(choices) != 1:
            raise DeploymentError("Workflow connection identity selection is ambiguous or unattached")
        return choices[0]

    def _sql_connection(self, inputs: Any, parameters: dict, identities: dict) -> dict:
        """Resolve the deployed oauthMI SQL procedure action, not a SQL login/URI."""
        if not isinstance(inputs, dict) or str(inputs.get("method", "")).lower() != "post":
            raise DeploymentError("SQL connector requires the reviewed procedure-call action")
        name = inputs.get("host", {}).get("connection", {}).get("name")
        match = re.fullmatch(r"@parameters\('\$connections'\)\['([A-Za-z0-9_-]+)'\]\['connectionId'\]", name or "")
        connections = parameters.get("$connections", {}).get("value")
        if match is None or not isinstance(connections, dict) or not isinstance(connections.get(match[1]), dict):
            raise DeploymentError("SQL connection reference is dynamic or unreadable")
        connection = connections[match[1]]
        resource = _id(connection.get("connectionId"))
        if not re.search(r"/providers/Microsoft\.Web/connections/[^/]+$", resource, re.I):
            raise DeploymentError("SQL connector resource identity has an unsupported type")
        properties = _properties(self._arm(resource, "2016-06-01"), resource)
        api = properties.get("api", {}).get("id")
        parameter_set = properties.get("parameterValueSet")
        if (
            not isinstance(api, str) or not re.fullmatch(
                r"/subscriptions/[0-9a-fA-F-]{36}/providers/Microsoft\.Web/locations/[A-Za-z0-9-]+/managedApis/sql", api, re.I,
            )
            or connection.get("id") != api or not isinstance(parameter_set, dict)
            or parameter_set.get("name") != "oauthMI" or parameter_set.get("values") != {}
            or properties.get("parameterValues", {}) != {}
        ):
            raise DeploymentError("SQL connector is not the inspectable credential-free oauthMI connection")
        authentication = connection.get("connectionProperties", {}).get("authentication")
        client, principal = self._selected_identity(authentication, identities)
        path = inputs.get("path")
        if not isinstance(path, str) or len(path) > 4_096:
            raise DeploymentError("SQL connector action path is missing or oversized")

        def resolve(match: re.Match) -> str:
            expression = match[1]
            if expression.startswith("parameters("):
                return self._expression("@" + expression, parameters)
            return expression[1:-1]

        path = re.sub(
            r"@\{encodeURIComponent\(encodeURIComponent\((parameters\('[^']+'\)|'[^']*')\)\)\}",
            resolve, path,
        )
        path = unquote(unquote(path))
        match = re.fullmatch(r"/v2/datasets/([^/,]+),([^/]+)/procedures/(\[dbo\]\.\[([A-Za-z_][A-Za-z0-9_]*)\])", path)
        if match is None or "@" in path:
            raise DeploymentError("SQL connector target/procedure path is dynamic or unsupported")
        if match[4] not in declared_write_procedures():
            raise DeploymentError("SQL connector invokes an undeclared write module")
        return {
            "connection_id": resource, "api_id": api, "authentication": "oauthMI",
            "identity_client_id": client, "identity_object_id": principal, "procedure": match[3],
            "settings": {"AZURE_SQL_SERVER": match[1], "AZURE_SQL_DATABASE": match[2], "AZURE_CLIENT_ID": client},
        }

    def inspect_invoker(self, writer: WriterSpec, direct: Sequence[CapturedWriter]) -> CapturedWriter | None:
        resource = writer.resource_id
        payload = self._arm(resource, "2019-05-01")
        properties = _properties(payload, resource)
        parameters, actions = self._workflow(properties)
        calls = []
        for action in actions:
            if action.get("type") in {"Scope", "If", "Foreach", "Until", "Response", "Compose", "Terminate", "InitializeVariable", "SetVariable", "ParseJson"}:
                continue
            if action.get("type") == "ApiConnection":
                self._sql_connection(action.get("inputs"), parameters, self._identities(payload))
                continue
            if action.get("type") != "Http" or not isinstance(action.get("inputs"), dict):
                raise DeploymentError("Workflow uses an unsupported connector/invoker; explicit adapter required")
            inputs = action["inputs"]
            auth, audience = self._http_authentication(inputs, parameters)
            uri = self._expression(inputs.get("uri"), parameters)
            parsed = self._http_uri(uri, "invocation destination")
            matches = [
                item for item in direct if item.writer.kind == "foundry_agent"
                and parsed.scheme + "://" + parsed.netloc + parsed.path
                == item.writer.locator + "/endpoint/protocols/openai/responses"
                and parse_qsl(parsed.query, keep_blank_values=True) == [("api-version", "v1")]
            ]
            if not matches:
                if self._unrelated_http_read(inputs, parsed, audience):
                    continue
                raise DeploymentError("Workflow HTTP invoker or relay lacks a supported explicit downstream binding")
            if audience != "https://ai.azure.com" or inputs.get("method") != "POST":
                raise DeploymentError("Workflow invocation is not bound to a secretless Foundry identity")
            identities = self._identities(payload)
            identity = self._selected_identity(auth, identities)
            if len(matches) != 1:
                raise DeploymentError("Workflow's actual invocation destination is ambiguous")
            calls.append((matches[0], identity, fingerprint({"uri": uri, "authentication": auth})))
        if not calls:
            return None
        if len({(item.writer.writer_id, identity) for item, identity, _ in calls}) != 1:
            raise DeploymentError("A multi-destination workflow requires a reviewed graph contract")
        runs = self._list(f"{ARM}{resource}/runs?api-version=2019-05-01")
        quiet = properties.get("state") == "Disabled" and all(
            row.get("properties", {}).get("status") in {"Succeeded", "Failed", "Cancelled", "Aborted", "TimedOut"}
            for row in runs
        )
        downstream, (client, principal), _ = calls[0]
        invocation_hash = fingerprint([item[2] for item in calls], domain="deployment.invocation.v1")
        return CapturedWriter(
            writer=writer, identity_client_id=client, identity_object_id=principal,
            invokes_writer_id=downstream.writer.writer_id, invocation_binding_hash=invocation_hash,
            resource_binding_hash=fingerprint({
                "resource": resource, "invocation": invocation_hash, "identity": (client, principal),
                "state": properties.get("state"), "runs": _safe(runs),
            }, domain="deployment.resource.binding.v1"),
            observed_at=self.clock(), state="disabled" if quiet else "running",
        )

    def observe_registered(self, binding: WriterBinding, bindings: Sequence[WriterBinding]) -> CapturedWriter:
        self._started, self._requests, self.pages, self._directory = self.monotonic(), 0, [], {}
        self._verified_subscriptions, self._scope_subscriptions = set(), None
        if binding.invokes_writer_id is None:
            result = self.inspect_direct(binding.writer, {binding.identity_client_id})
        else:
            downstream = next((item for item in bindings if item.writer.writer_id == binding.invokes_writer_id), None)
            if downstream is None or downstream.invokes_writer_id is not None:
                raise DeploymentError("Unsupported indirect writer chain")
            direct = self.inspect_direct(downstream.writer, {downstream.identity_client_id})
            result = self.inspect_invoker(binding.writer, [direct]) if direct else None
        if result is None or (
            result.identity_client_id != binding.identity_client_id
            or result.identity_object_id != binding.identity_object_id
            or result.invokes_writer_id != binding.invokes_writer_id
        ):
            raise DeploymentError("Registered writer identity is misbound or uninspectable")
        if result.state not in {"stopped", "disabled", "inactive"}:
            raise DeploymentError(
                "Foundry endpoint is not observably Disabled" if binding.writer.kind == "foundry_agent"
                else "Registered writer is not stopped, disabled or inactive"
            )
        return result

    def collect(self, authority: AuthoritySnapshot, *, capture_id: str | None = None) -> DiscoveryCapture:
        self._started, self._requests, self.pages, self._directory = self.monotonic(), 0, [], {}
        self._foundry_endpoints = set()
        self._verified_subscriptions, self._scope_subscriptions = set(), None
        started = self.clock()
        if not self.allow_preview:
            raise DeploymentError("Explicit reverse-identity preview capability must be selected")
        root = f"/providers/Microsoft.Management/managementGroups/{self.target.tenant_id}"
        root_doc = self._request(f"{ARM}{root}?api-version=2020-05-01")
        if str(root_doc.get("id", "")).casefold() != root.casefold() or (
            root_doc.get("properties", {}).get("tenantId") != self.target.tenant_id
        ):
            raise DeploymentError("Tenant-root management group identity is unverified")
        descendants = self._list(f"{ARM}{root}/descendants?api-version=2020-05-01")
        nodes = {root: None}
        subscriptions = []
        for row in descendants:
            rid = row.get("id")
            if not isinstance(rid, str) or rid in nodes:
                raise DeploymentError("Tenant descendant identity is missing or duplicated")
            nodes[rid] = row.get("properties", {}).get("parent", {}).get("id")
            if row.get("type") == "Microsoft.Management/managementGroups/subscriptions":
                subscription = canonical_id(row.get("name"))
                if rid != f"/subscriptions/{subscription}":
                    raise DeploymentError("Tenant subscription descendant identity disagrees")
                subscriptions.append(subscription)
            elif row.get("type") != "Microsoft.Management/managementGroups":
                raise DeploymentError("Tenant descendant kind is unsupported")
            elif re.fullmatch(r"/providers/Microsoft\.Management/managementGroups/[A-Za-z0-9_.()-]+", rid) is None:
                raise DeploymentError("Management-group descendant is outside its declared scope")
        for child in nodes:
            seen = set()
            while child != root:
                if child not in nodes or child in seen:
                    raise DeploymentError("Tenant descendant scope is incomplete or cyclic")
                seen.add(child)
                child = nodes[child]
        if not subscriptions or len(subscriptions) >= 5_000:
            raise DeploymentError("Tenant subscription closure is empty or exceeds the reverse-identity API limit")
        self._scope_subscriptions = set(subscriptions)
        wanted = {principal.client_id for principal in authority.writers}
        for client in wanted:
            self._principal(client_id=client)
        resources = {}
        for subscription in sorted(subscriptions):
            base = f"{ARM}/subscriptions/{subscription}"
            metadata = self._request(f"{base}?api-version=2022-12-01")
            if metadata.get("subscriptionId") != subscription or metadata.get("tenantId") != self.target.tenant_id or metadata.get("state") != "Enabled":
                raise DeploymentError("Subscription tenant/state cannot support complete enumeration")
            self._verified_subscriptions.add(subscription)
            permissions = self._list(f"{base}/providers/Microsoft.Authorization/permissions?api-version=2022-04-01")
            if not any(
                isinstance(row.get("actions"), list) and set(row["actions"]) & {"*", "*/read"}
                and row.get("notActions") == [] for row in permissions
            ):
                raise DeploymentError("Subscription-wide read visibility is not proven by effective permissions")
            if self._list(f"{base}/providers/Microsoft.Authorization/denyAssignments?api-version=2022-04-01"):
                raise DeploymentError("Deny assignments require a reviewed visibility resolver; no partial scope is accepted")
            rows = self._list(f"{base}/resources?api-version=2021-04-01")
            for row in rows:
                rid = _id(row.get("id"))
                if rid.split("/")[2] != subscription or rid.casefold() in resources or not isinstance(row.get("type"), str):
                    raise DeploymentError("Resource enumeration contains a duplicate, foreign or untyped resource")
                resources[rid.casefold()] = row
        if len(resources) > self.max_resources:
            raise DeploymentError("Tenant resource inventory exceeded its explicit budget")
        direct, invokers, attached, inspected = [], [], {}, set()
        user_identities = {}
        for row in list(resources.values()):
            rid, kind = row["id"], row["type"].lower()
            if kind == "microsoft.managedidentity/userassignedidentities":
                properties = _properties(self._arm(rid, VERSIONS[kind]), rid)
                if properties.get("clientId") in wanted:
                    user_identities[rid] = properties["clientId"]
                continue
            if kind == "microsoft.cognitiveservices/accounts":
                account = self._arm(rid, VERSIONS[kind])
                if account.get("kind") not in {"AIServices", "CognitiveServices"}:
                    continue
                projects = self._list(f"{ARM}{rid}/projects?api-version=2025-06-01")
                for project in projects:
                    project_id = _id(project.get("id"))
                    if not project_id.casefold().startswith(rid.casefold() + "/projects/"):
                        raise DeploymentError("Foundry project belongs to another account")
                    properties = _properties(project, project_id)
                    endpoints = properties.get("endpoints")
                    if not isinstance(endpoints, dict):
                        raise DeploymentError("Foundry ARM project endpoints are unavailable")
                    candidates = {
                        value.rstrip("/") for value in endpoints.values() if isinstance(value, str)
                        and re.fullmatch(r"https://[A-Za-z0-9-]+\.services\.ai\.azure\.com/api/projects/[A-Za-z0-9_.-]+/?", value)
                    }
                    if len(candidates) != 1:
                        raise DeploymentError("ARM does not expose one exact Foundry project endpoint; do not invent it")
                    endpoint = candidates.pop()
                    self._foundry_endpoints.add(endpoint)
                    agents = self._list(f"{endpoint}/agents?api-version=v1", foundry=True)
                    for agent in agents:
                        spec = WriterSpec(
                            writer_id=_writer_id(f"{endpoint}/agents/{agent.get('name')}"),
                            kind="foundry_agent", project_endpoint=endpoint, agent_name=agent.get("name"),
                        )
                        result = self.inspect_direct(spec, wanted)
                        if result:
                            direct.append(result)
                inspected.add(rid.casefold())
                continue
            if kind not in KINDS:
                if kind not in NON_HOST_TYPES:
                    raise DeploymentError(f"Unsupported resource type needs a complete identity/host adapter: {kind}")
                identity = row.get("identity")
                if identity is not None and set(self._identities(row)) & wanted:
                    raise DeploymentError("A SQL identity is attached to an unsupported hosting resource")
                continue
            if kind == "microsoft.web/sites":
                for slot in self._list(f"{ARM}{rid}/slots?api-version=2023-12-01"):
                    slot_id = _id(slot.get("id"))
                    if not slot_id.casefold().startswith(rid.casefold() + "/slots/"):
                        raise DeploymentError("App Service slot belongs to another root")
                    resources.setdefault(slot_id.casefold(), slot | {"type": "Microsoft.Web/sites/slots"})
            if kind == "microsoft.logic/workflows":
                invokers.append(WriterSpec(writer_id=_writer_id(rid), kind="logic_app", resource_id=rid))
        for row in resources.values():
            rid, kind = row["id"], row["type"].lower()
            if kind not in KINDS:
                continue
            payload = self._arm(rid, VERSIONS[kind])
            identities = self._identities(payload)
            for client in set(identities) & wanted:
                identity_resource = identities[client][1]
                if identity_resource is not None and identity_resource not in user_identities:
                    raise DeploymentError("A SQL UAMI attachment is outside the complete declared identity inventory")
                attached.setdefault(client, set()).add(rid.casefold())
            spec = WriterSpec(writer_id=_writer_id(rid), kind=KINDS[kind], resource_id=rid)
            if kind != "microsoft.logic/workflows" or set(identities) & wanted:
                result = self.inspect_direct(spec, wanted)
                if result:
                    direct.append(result)
            inspected.add(rid.casefold())
        for resource, client in user_identities.items():
            if self._list(f"{ARM}{resource}/federatedIdentityCredentials?api-version=2023-01-31"):
                raise DeploymentError("SQL UAMI has external federated credential reuse; no host-completeness proof is available")
            associated = self._list(
                f"{ARM}{resource}/listAssociatedResources?api-version=2021-09-30-preview",
                method="POST", total=True,
            )
            reverse = {_id(row.get("id")).casefold() for row in associated}
            if len(reverse) != len(associated) or reverse != attached.get(client, set()) or not reverse <= inspected:
                raise DeploymentError("Identity reuse differs from the independently enumerated supported host set")
        found = {item.identity_client_id for item in direct}
        if found != wanted:
            raise DeploymentError("SQL write identities do not exactly match independently discovered direct deployments")
        captured = list(direct)
        for writer in invokers:
            result = self.inspect_invoker(writer, direct)
            if result:
                captured.append(result)
        if not captured:
            raise DeploymentError("No SQL deployment writer was independently established")
        if self.monotonic() - self._started > self.max_seconds:
            raise DeploymentError("Capture expired before completing deployment enumeration")
        return DiscoveryCapture(
            capture_id=capture_id or str(uuid4()), tenant_id=self.target.tenant_id,
            sql_server=self.target.server, sql_database=self.target.database,
            operator_object_id=self.target.deployer_object_id, started_at=started, finished_at=self.clock(),
            tenant_root=root, subscriptions=tuple(sorted(subscriptions)), pages=tuple(self.pages),
            writers=tuple(sorted(captured, key=lambda item: item.writer.writer_id)),
            sql_identity_sids=tuple(sorted(principal.sid.hex() for principal in authority.writers)),
            gaps=authority.gaps,
        )
