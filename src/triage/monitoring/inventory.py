"""Read-only, tenant-bound Fabric/Power BI inventory and scope resolution.

``FabricInventoryClient.read_page`` returns one bounded page, not a tenant-sized
list. The collector persists its items and container metadata before using the
continuation. Current-generation workspace/domain records are supplied by the
shared store on every step; an interrupted scan needs no process-local cache.

Admin item enumeration is an explicit preview opt-in. Core listings retain
caller-visible authority, even when the caller can list admin workspaces.
Power BI dataset inventory is not refresh telemetry or a write-capability probe.
Unreadable domain metadata makes item state unknown so exclusions cannot be
bypassed using an incomplete hierarchy.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal, Protocol
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

import httpx
from pydantic import Field, TypeAdapter, ValidationError

from triage.monitoring.models import (
    SUPPORTED_INVENTORY_TYPES,
    CanonicalId,
    CoverageGap,
    Cursor,
    InventoryDomain,
    InventoryItem,
    InventoryWorkspace,
    JsonObject,
    MonitoringContext,
    ScopeDefinition,
    ScopeSelector,
)
from triage.monitoring.rate_limit import RateBudget, RatePolicy
from triage.pipeline_models import canonical_id

logger = logging.getLogger("triage.monitoring.inventory")

Service = Literal["fabric", "powerbi"]
_HOSTS = {"fabric": "api.fabric.microsoft.com", "powerbi": "api.powerbi.com"}
_PREFIXES = {"fabric": "/v1", "powerbi": "/v1.0/myorg"}
_SCOPES = {
    "fabric": "https://api.fabric.microsoft.com/.default",
    "powerbi": "https://analysis.windows.net/powerbi/api/.default",
}
_AUDIENCES = {
    "fabric": {"https://api.fabric.microsoft.com"},
    "powerbi": {
        "https://analysis.windows.net/powerbi/api",
        "00000009-0000-0000-c000-000000000000",
    },
}
# Starting operational budgets, not promises about tenant/service quotas.
SERVICE_POLICIES = {"fabric": RatePolicy(120, 60), "powerbi": RatePolicy(60, 60)}
API_POLICIES = {
    "fabric.domains": RatePolicy(200, 3600),
    "fabric.domain_workspaces": RatePolicy(200, 3600),
    "fabric.admin_workspaces": RatePolicy(200, 3600),
    "fabric.admin_items_preview": RatePolicy(200, 3600),
    "fabric.workspaces": RatePolicy(120, 60),
    "fabric.items": RatePolicy(120, 60),
    "fabric.item": RatePolicy(120, 60),
    "fabric.jobs": RatePolicy(120, 60),
    "powerbi.groups": RatePolicy(60, 60),
    "powerbi.datasets": RatePolicy(60, 60),
    "powerbi.dataset": RatePolicy(60, 60),
    "powerbi.refreshes": RatePolicy(60, 60),
}
_CURSOR_ADAPTER = TypeAdapter(Cursor)
_JSON_ADAPTER = TypeAdapter(JsonObject)


class AccessToken(Protocol):
    token: str
    expires_on: int


class CollectorCredential(Protocol):
    def get_token(self, *scopes: str) -> AccessToken: ...


def managed_identity_credential(client_id: str) -> CollectorCredential:
    """Select one user-assigned MI; token tenant/object ID are checked by the REST client."""
    from azure.identity import ManagedIdentityCredential

    return ManagedIdentityCredential(client_id=canonical_id(client_id))


class RestReadError(RuntimeError):
    """Sanitized source failure; never include response bodies or token text."""

    def __init__(
        self, code: str, detail: str, *, retry_at: datetime | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.retry_at = retry_at
        self.status_code = status_code

    def gap(self, workspace_id: str | None = None, item_id: str | None = None) -> CoverageGap:
        return CoverageGap(
            code=self.code, detail=self.detail, workspace_id=workspace_id,
            item_id=item_id, retry_at=self.retry_at,
        )


@dataclass(frozen=True)
class RestRoute:
    service: Service
    api: str
    path: str
    collection: str | None = "value"
    query: tuple[tuple[str, str], ...] = ()

    @property
    def url(self) -> str:
        return urlunsplit((
            "https", _HOSTS[self.service], f"{_PREFIXES[self.service]}{self.path}",
            urlencode(self.query), "",
        ))


def _query(url: str) -> dict[str, str]:
    try:
        pairs = parse_qsl(urlsplit(url).query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise RestReadError("unexpected_pagination", "REST continuation query is malformed") from exc
    if len({key for key, _ in pairs}) != len(pairs):
        raise RestReadError("unexpected_pagination", "REST continuation repeats a query parameter")
    return dict(pairs)


def validate_rest_url(route: RestRoute, url: str) -> str:
    """Accept only the original resource; continuation is never a free-form URL."""
    try:
        parsed = urlsplit(url)
        valid = (
            parsed.scheme == "https" and parsed.hostname == _HOSTS[route.service]
            and parsed.port in (None, 443) and parsed.username is None
            and parsed.password is None and not parsed.fragment
            and parsed.path == f"{_PREFIXES[route.service]}{route.path}"
            and "\\" not in url and not any(ord(char) < 32 for char in url)
        )
    except ValueError as exc:
        raise RestReadError("unexpected_pagination", "REST continuation URL is malformed") from exc
    if not valid:
        raise RestReadError(
            "untrusted_continuation", "REST continuation left the pinned service or resource path",
        )
    fixed = dict(route.query)
    values = _query(url)
    allowed = set(fixed) | (
        {"continuationToken"} if route.service == "fabric" else {"$skip", "$skiptoken"}
    )
    if not values.keys() <= allowed or any(
        key in values and values[key] != value for key, value in fixed.items()
    ):
        raise RestReadError(
            "unexpected_pagination", "REST continuation changed its scope, tenant, or query",
        )
    if any(not value for value in values.values()):
        raise RestReadError("unexpected_pagination", "REST continuation has an empty query value")
    return urlunsplit((
        "https", _HOSTS[route.service], parsed.path, urlencode(fixed | values), "",
    ))


def next_page_url(payload: Mapping[str, Any], route: RestRoute) -> str | None:
    """Support Fabric tokens/URIs and OData links without ignoring extra cursors."""
    for key in ("next", "nextPage", "nextPageToken", "hasMore", "hasMoreResults", "isTruncated", "continuation"):
        if payload.get(key) not in (None, False):
            raise RestReadError("unexpected_pagination", "REST used an unsupported pagination field")
    links = [
        payload[key] for key in ("continuationUri", "@odata.nextLink", "odata.nextLink", "nextLink")
        if payload.get(key) is not None
    ]
    if any(not isinstance(link, str) or not link for link in links):
        raise RestReadError("unexpected_pagination", "REST returned an invalid continuation link")
    normalized = [validate_rest_url(route, link) for link in links]
    if len(set(normalized)) > 1:
        raise RestReadError("unexpected_pagination", "REST continuation links disagree")
    token = payload.get("continuationToken")
    if token is not None and (
        route.service != "fabric" or not isinstance(token, str) or not token
    ):
        raise RestReadError("unexpected_pagination", "REST returned an invalid continuation token")
    link = normalized[0] if normalized else None
    if route.service == "fabric" and link is not None:
        from_uri = _query(link).get("continuationToken")
        if not from_uri or token is not None and token != from_uri and unquote(token) != from_uri:
            raise RestReadError("unexpected_pagination", "Fabric continuation token and URI disagree")
    if link is not None:
        if not (_query(link).keys() - dict(route.query).keys()):
            raise RestReadError("unexpected_pagination", "REST next page has no continuation position")
        return link
    if token is not None:
        return validate_rest_url(
            route, route.url + ("&" if route.query else "?") + urlencode({"continuationToken": token}),
        )
    return None


def _assert_payload_tenant(payload: Mapping[str, Any], context: MonitoringContext) -> None:
    for name in ("tenantId", "tenant_id"):
        if name in payload:
            try:
                actual = canonical_id(payload[name])
            except (ValueError, TypeError, AttributeError) as exc:
                raise RestReadError("wrong_tenant", "REST evidence has an invalid tenant identity") from exc
            if actual != context.tenant_id:
                raise RestReadError("wrong_tenant", "REST evidence belongs to another tenant")


def _retry_delay(headers: httpx.Headers, now: datetime) -> tuple[int, bool]:
    value = headers.get("Retry-After")
    if value is None:
        return 60, False
    try:
        if value.isascii() and value.isdecimal():
            seconds = int(value)
        else:
            deadline = parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                raise ValueError("Retry date has no timezone")
            seconds = max(0, math.ceil((deadline.astimezone(UTC) - now).total_seconds()))
        if not 0 <= seconds <= 2_147_483_647:
            raise ValueError("Retry delay is outside the SQL interval range")
    except (TypeError, ValueError, OverflowError):
        return 60, False
    return seconds, True


def _invalid_json_constant(value: str) -> None:
    raise ValueError("Nonfinite numbers are not JSON evidence")


class TenantBoundRestClient:
    """One physical GET per call, with explicit MI/credential and shared budgets.

    The injected credential must be the deployed collector credential (or an
    explicit offline fixture). There is no DefaultAzureCredential, secret, CLI,
    browser, or developer fallback. JWT claims are context checks on a token
    acquired from that trusted SDK, not authentication of a caller-supplied JWT.
    ``collector_identity_id`` is the principal/object ID, not the MI client ID.
    """

    def __init__(
        self, context: MonitoringContext, collector_identity_id: str,
        credential: CollectorCredential, rate_budget: RateBudget, *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        service_policies: Mapping[str, RatePolicy] | None = None,
        api_policies: Mapping[str, RatePolicy] | None = None,
        timeout_seconds: float = 30,
        max_response_bytes: int = 2_097_152,
    ) -> None:
        self.context = MonitoringContext.model_validate(context)
        self.collector_identity_id = canonical_id(collector_identity_id)
        if credential is None or rate_budget is None:
            raise ValueError("An explicit collector credential and shared request budget are required")
        if not 0 < timeout_seconds <= 60 or not 1 <= max_response_bytes <= 8_388_608:
            raise ValueError("REST read timeout or response bound is outside the supported range")
        self._credential = credential
        self._budget = rate_budget
        self._clock = clock
        self._service_policies = SERVICE_POLICIES | dict(service_policies or {})
        self._api_policies = API_POLICIES | dict(api_policies or {})
        self._max_bytes = max_response_bytes
        self._http = httpx.AsyncClient(
            transport=transport or httpx.AsyncHTTPTransport(retries=0),
            follow_redirects=False, timeout=timeout_seconds,
        )
        self._tokens: dict[Service, tuple[str, float]] = {}

    async def _token(self, service: Service) -> str:
        now = self._clock().timestamp()
        cached = self._tokens.get(service)
        if cached is not None and cached[1] > now + 60:
            return cached[0]
        try:
            token = await asyncio.to_thread(self._credential.get_token, _SCOPES[service])
        except Exception as exc:
            logger.error("Collector token acquisition failed (%s)", type(exc).__name__)
            raise RestReadError("credential_unavailable", "Collector identity could not obtain a token") from exc
        try:
            parts = token.token.split(".")
            if len(parts) != 3 or len(token.token) > 32_768:
                raise ValueError("Token has no bounded JWT context")
            claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
            if not isinstance(claims, dict):
                raise ValueError("Token claims must be an object")
            tenant = canonical_id(claims["tid"])
            identity = canonical_id(claims["oid"])
            expiry = min(float(token.expires_on), float(claims["exp"]))
            audience = claims["aud"]
            if not math.isfinite(expiry) or expiry <= now:
                raise ValueError("Token expired")
        except (ValueError, TypeError, KeyError, AttributeError, binascii.Error) as exc:
            raise RestReadError(
                "token_context_unverifiable", "Collector token lacks verifiable tenant/identity/expiry context",
            ) from exc
        if tenant != self.context.tenant_id:
            raise RestReadError("wrong_tenant", "Collector token belongs to another deployment tenant")
        if identity != self.collector_identity_id:
            raise RestReadError("wrong_collector_identity", "Collector token belongs to another principal")
        if not isinstance(audience, str) or audience.rstrip("/") not in _AUDIENCES[service]:
            raise RestReadError("wrong_token_audience", "Collector token is not for the selected REST service")
        self._tokens[service] = token.token, expiry
        return token.token

    async def get(self, route: RestRoute, *, continuation: str | None = None) -> dict[str, Any]:
        if route.api not in self._api_policies or not route.api.startswith(route.service + "."):
            raise ValueError("Every REST API must have an explicit service request policy")
        url = validate_rest_url(route, continuation or route.url)
        token = await self._token(route.service)
        policies = (
            (f"service:{route.service}", self._service_policies[route.service]),
            (f"api:{route.api}", self._api_policies[route.api]),
        )
        decision = await asyncio.to_thread(self._budget.acquire_many, self.context, policies)
        if not decision.allowed:
            raise RestReadError(
                "request_budget_exhausted", "Shared REST request budget is exhausted",
                retry_at=decision.retry_at,
            )
        try:
            async with self._http.stream(
                "GET", url, headers={"Authorization": f"Bearer {token}"},
            ) as response:
                if response.status_code == 429 or (
                    response.status_code in {502, 503, 504} and "Retry-After" in response.headers
                ):
                    seconds, valid_header = _retry_delay(response.headers, decision.checked_at)
                    deferred = [
                        await asyncio.to_thread(
                            self._budget.defer, self.context, bucket, policy, seconds=seconds,
                        )
                        for bucket, policy in policies
                    ]
                    raise RestReadError(
                        "service_throttled" if valid_header else "service_throttled_invalid_retry_after",
                        "REST service requested backoff" if valid_header else
                        "REST backoff header was missing or invalid; a shared 60-second cooldown was applied",
                        retry_at=max(value.retry_at for value in deferred),
                        status_code=response.status_code,
                    )
                if response.status_code != 200:
                    raise RestReadError(
                        f"http_{response.status_code}",
                        f"REST evidence read returned HTTP {response.status_code}",
                        status_code=response.status_code,
                    )
                header_tenant = response.headers.get("x-ms-tenant-id")
                if header_tenant is not None:
                    _assert_payload_tenant({"tenantId": header_tenant}, self.context)
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > self._max_bytes:
                        raise RestReadError("response_too_large", "REST evidence exceeded the response bound")
                    body.extend(chunk)
        except httpx.HTTPError as exc:
            logger.warning("Collector REST transport failed (%s)", type(exc).__name__)
            raise RestReadError("transport_error", "REST evidence could not be read from the source") from exc
        try:
            payload = json.loads(body, parse_constant=_invalid_json_constant)
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise RestReadError("malformed_response", "REST evidence is not a bounded JSON object") from exc
        if not isinstance(payload, dict):
            raise RestReadError("malformed_response", "REST evidence must be a JSON object")
        _assert_payload_tenant(payload, self.context)
        return payload

    async def close(self) -> None:
        await self._http.aclose()


def collection_rows(payload: Mapping[str, Any], route: RestRoute) -> list[Any]:
    if route.collection is None:
        return [dict(payload)]
    rows = payload.get(route.collection)
    if not isinstance(rows, list):
        raise RestReadError(
            "missing_collection", f"REST response has no {route.collection} array for {route.api}",
        )
    return rows


def definition_fingerprint(definition: Mapping[str, Any]) -> str:
    """Hash explicitly authorized, bounded definition content; never log it."""
    bounded = _JSON_ADAPTER.validate_python(dict(definition))
    encoded = json.dumps(
        bounded, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def bounded_gaps(gaps: Sequence[CoverageGap], limit: int = 200) -> tuple[CoverageGap, ...]:
    distinct = list(dict.fromkeys(gaps))
    if len(distinct) <= limit:
        return tuple(distinct)
    return (*distinct[:limit - 1], CoverageGap(
        code="coverage_gap_limit",
        detail=f"{len(distinct) - limit + 1} additional source coverage gaps were summarized",
    ))


def domain_ancestors(
    domain_id: str, domains: Sequence[InventoryDomain],
) -> tuple[tuple[str, ...], tuple[CoverageGap, ...]]:
    """Return the direct domain first, followed by its verified ancestor chain."""
    by_id = {domain.domain_id: domain for domain in domains if domain.state == "present"}
    ancestors: list[str] = []
    current: str | None = canonical_id(domain_id)
    while current is not None:
        if current in ancestors:
            return tuple(ancestors), (CoverageGap(
                code="domain_cycle", detail="Domain hierarchy contains a cycle",
            ),)
        ancestors.append(current)
        domain = by_id.get(current)
        if domain is None:
            return tuple(ancestors), (CoverageGap(
                code="domain_metadata_missing", detail="Domain ancestor metadata is incomplete",
            ),)
        current = domain.parent_domain_id
    return tuple(ancestors), ()


@dataclass(frozen=True)
class ScopeResolution:
    state: Literal["included", "excluded", "out_of_scope", "unknown", "unsupported"]
    rule_ids: tuple[str, ...] = ()
    auto_enrol_detection_only: bool = False
    gaps: tuple[CoverageGap, ...] = ()


def resolve_scope(
    scope: ScopeDefinition, item: InventoryItem, *,
    domains: Sequence[InventoryDomain] = (),
    workspaces: Sequence[InventoryWorkspace] = (),
    metadata_complete: bool = True,
) -> ScopeResolution:
    """Resolve evidence only. Shared admission still owns review, cutoff and policy fences.

    ``InventoryItem.domain_ids`` are direct memberships, not flattened ancestors.
    Otherwise a non-descendant domain selection would silently include children.
    Unknown domain exclusions fail closed even if a tenant include matched.
    """
    if (scope.tenant_id, scope.epoch) != (item.tenant_id, item.epoch):
        raise ValueError("Scope and inventory belong to different tenants or epochs")
    if item.workload is None:
        return ScopeResolution("unsupported", gaps=(CoverageGap(
            code="unsupported_item_type", detail=item.unsupported_reason or "No workload contract",
            workspace_id=item.workspace_id, item_id=item.item_id,
        ),))
    if not scope.enabled or item.state == "deleted":
        return ScopeResolution("out_of_scope")
    relevant_domains = tuple(
        domain for domain in domains
        if (domain.tenant_id, domain.epoch, domain.generation_id)
        == (item.tenant_id, item.epoch, item.generation_id)
    )
    workspace = next((
        workspace for workspace in workspaces
        if workspace.workspace_id == item.workspace_id
        and (workspace.tenant_id, workspace.epoch, workspace.generation_id)
        == (item.tenant_id, item.epoch, item.generation_id)
    ), None)
    memberships = item.domain_ids
    if workspace is not None:
        memberships = (workspace.domain_id,) if workspace.domain_id is not None else ()
    included: list[str] = []
    excluded: list[str] = []
    uncertain_include = False
    uncertain_exclude = False
    automatic = False
    gaps: list[CoverageGap] = []
    for rule in scope.rules:
        if item.workload not in rule.workloads:
            continue
        selector = rule.selector
        unknown = False
        if selector.kind == "tenant":
            matches = True
        elif selector.kind in {"workspace", "item"}:
            matches = selector.workspace_id == item.workspace_id and (
                selector.kind == "workspace" or selector.item_id == item.item_id
            )
        else:
            matches = selector.domain_id in memberships
            unknown = not metadata_complete or workspace is not None and workspace.state != "present"
            if not memberships and workspace is None:
                unknown = True
            if selector.include_descendants:
                for direct in memberships:
                    ancestors, missing = domain_ancestors(direct, relevant_domains)
                    matches |= selector.domain_id in ancestors
                    gaps.extend(missing)
                    unknown |= bool(missing)
        if matches and not unknown:
            if rule.effect == "exclude":
                excluded.append(rule.rule_id)
            else:
                included.append(rule.rule_id)
                automatic |= rule.auto_enrol_detection_only
        elif unknown:
            uncertain_exclude |= rule.effect == "exclude"
            uncertain_include |= rule.effect == "include"
    if excluded:
        return ScopeResolution("excluded", tuple(excluded), gaps=bounded_gaps(gaps))
    if item.state == "unknown" or uncertain_exclude or not included and uncertain_include:
        gaps.append(CoverageGap(
            code="scope_metadata_incomplete", detail="Current inventory cannot resolve this scope safely",
            workspace_id=item.workspace_id, item_id=item.item_id,
        ))
        return ScopeResolution("unknown", gaps=bounded_gaps(gaps))
    if not included:
        return ScopeResolution("out_of_scope", gaps=bounded_gaps(gaps))
    return ScopeResolution("included", tuple(included), automatic, bounded_gaps(gaps))


@dataclass(frozen=True)
class InventoryApiOptions:
    admin_workspaces: bool = False
    admin_domains: bool = False
    admin_items_preview: bool = False
    powerbi_datasets: bool = True

    def __post_init__(self) -> None:
        if any(type(value) is not bool for value in asdict(self).values()):
            raise ValueError("Inventory API selection requires explicit boolean values")
        if self.admin_items_preview and not self.admin_workspaces:
            raise ValueError("Admin Items preview requires an explicit admin-workspace adapter")


class _InventoryCursor(MonitoringContext):
    version: Literal[2] = 2
    generation_id: CanonicalId
    selector_hash: str
    adapter: str
    options_hash: str
    phase: Literal["domains", "workspaces", "memberships", "fabric_items", "powerbi_items"]
    item_type: Literal["DataPipeline", "SemanticModel"] = "DataPipeline"
    after_id: CanonicalId | None = None
    active_id: CanonicalId | None = None
    url: str | None = None
    offset: int = Field(default=0, ge=0)
    page_hash: str | None = None
    trail: tuple[str, ...] = ()
    metadata_complete: bool = True

    def encode(self) -> str:
        try:
            return _CURSOR_ADAPTER.validate_python(self.model_dump_json(exclude_none=True))
        except ValidationError as exc:
            raise RestReadError(
                "continuation_state_too_large", "Inventory continuation exceeds the durable cursor bound",
            ) from exc


@dataclass(frozen=True)
class InventoryReadPage:
    items: tuple[InventoryItem, ...] = ()
    workspaces: tuple[InventoryWorkspace, ...] = ()
    domains: tuple[InventoryDomain, ...] = ()
    continuation: str | None = None
    gaps: tuple[CoverageGap, ...] = ()
    completed_pages: int = 0
    retry_at: datetime | None = None
    finished: bool = False


def _selector_hash(selector: ScopeSelector) -> str:
    return hashlib.sha256(selector.model_dump_json().encode("utf-8")).hexdigest()


def _raw_id(row: Mapping[str, Any], name: str = "id") -> str:
    value = row.get(name)
    if not isinstance(value, str):
        raise RestReadError("missing_identity", "Inventory record has no valid resource identity")
    try:
        return canonical_id(value)
    except ValueError as exc:
        raise RestReadError("missing_identity", "Inventory record has an invalid resource identity") from exc


def _name(row: Mapping[str, Any], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > 200:
        raise RestReadError("metadata_incomplete", "Inventory record has no bounded display name")
    return value


def _state(row: Mapping[str, Any]) -> Literal["present", "unknown"]:
    value = row.get("state", "Active")
    if value in {"Active", "Available"}:
        return "present"
    # A partial generation may not establish deletion, including containers.
    return "unknown"


class FabricInventoryClient:
    """Fabric containers/items plus Power BI dataset metadata, all GET-only.

    Routes:
      /v1/admin/domains?preview=false             (domains)
      /v1/admin/domains/{id}/workspaces           (value)
      /v1/admin/workspaces                       (workspaces)
      /v1/admin/items?workspaceId={id}&type={type} (itemEntities; explicit preview)
      /v1/workspaces/{id}/items?type={type}       (caller-visible core)
      /v1.0/myorg/groups/{id}/datasets[/{id}]     (Power BI inventory)

    Options select APIs to attempt, not grants or claims that those APIs work.
    The token's actual tenant/object ID is checked before every source is used.
    """

    def __init__(
        self, rest: TenantBoundRestClient, *, options: InventoryApiOptions | None = None,
    ) -> None:
        self.rest = rest
        self.options = options or InventoryApiOptions()
        self.adapter = (
            "fabric_admin_items_preview" if self.options.admin_items_preview
            else "fabric_caller_visible"
        ) + ("+powerbi_datasets" if self.options.powerbi_datasets else "")
        self.authority: Literal["tenant_admin", "caller_visible"] = (
            "tenant_admin" if self.options.admin_items_preview else "caller_visible"
        )
        self._options_hash = hashlib.sha256(
            json.dumps(asdict(self.options), sort_keys=True).encode(),
        ).hexdigest()

    def initial_cursor(
        self, context: MonitoringContext, selector: ScopeSelector, generation_id: str,
    ) -> str:
        return _InventoryCursor(
            **context.model_dump(), generation_id=generation_id,
            selector_hash=_selector_hash(selector), adapter=self.adapter,
            options_hash=self._options_hash, phase="domains",
        ).encode()

    def _cursor(
        self, context: MonitoringContext, selector: ScopeSelector, generation_id: str,
        continuation: str | None,
    ) -> _InventoryCursor:
        if context != self.rest.context or selector.tenant_id != context.tenant_id:
            raise RestReadError("wrong_tenant", "Inventory selection differs from the pinned REST context")
        try:
            raw = json.loads(continuation or self.initial_cursor(context, selector, generation_id))
            if isinstance(raw, dict) and type(raw.get("version")) is int and raw["version"] == 1:
                if (
                    (raw.get("tenant_id"), raw.get("epoch"), raw.get("generation_id"))
                    != (context.tenant_id, context.epoch, generation_id)
                    or raw.get("selector_hash") != _selector_hash(selector)
                    or raw.get("adapter") != self.adapter or raw.get("options_hash") != self._options_hash
                ):
                    raise RestReadError("invalid_continuation", "Inventory continuation belongs to another scan")
                raise RestReadError(
                    "inventory_adapter_changed",
                    "An older unfiltered inventory cursor cannot resume supported-type discovery; a fresh scan is required",
                )
            cursor = _InventoryCursor.model_validate(raw)
        except (ValidationError, json.JSONDecodeError) as exc:
            raise RestReadError("invalid_continuation", "Inventory continuation is malformed") from exc
        if (
            (cursor.tenant_id, cursor.epoch, cursor.generation_id)
            != (context.tenant_id, context.epoch, generation_id)
            or cursor.selector_hash != _selector_hash(selector) or cursor.adapter != self.adapter
            or cursor.options_hash != self._options_hash
        ):
            raise RestReadError("invalid_continuation", "Inventory continuation belongs to another scan")
        return cursor

    @staticmethod
    def _advance(cursor: _InventoryCursor, **changes: Any) -> _InventoryCursor:
        return cursor.model_copy(update={
            "url": None, "offset": 0, "page_hash": None, "trail": (), **changes,
        })

    def _next_source(self, cursor: _InventoryCursor, selector: ScopeSelector) -> _InventoryCursor | None:
        if cursor.phase == "domains":
            return self._advance(cursor, phase="workspaces")
        if cursor.phase == "workspaces":
            return self._advance(cursor, phase="memberships", active_id=None, after_id=None)
        if cursor.phase == "memberships":
            return self._advance(cursor, after_id=cursor.active_id, active_id=None)
        if cursor.phase == "fabric_items":
            if cursor.item_type == "DataPipeline":
                return self._advance(cursor, item_type="SemanticModel")
            if self.options.powerbi_datasets:
                return self._advance(cursor, phase="powerbi_items")
        return self._advance(
            cursor, phase="fabric_items", item_type="DataPipeline", after_id=cursor.active_id, active_id=None,
        )

    def _route(
        self, cursor: _InventoryCursor, selector: ScopeSelector,
        workspaces: Sequence[InventoryWorkspace], domains: Sequence[InventoryDomain],
        known_items: Sequence[InventoryItem],
    ) -> tuple[_InventoryCursor | None, RestRoute | None, list[CoverageGap]]:
        gaps: list[CoverageGap] = []
        while True:
            if cursor.phase == "domains":
                if self.options.admin_domains:
                    return cursor, RestRoute(
                        "fabric", "fabric.domains", "/admin/domains", "domains", (("preview", "false"),),
                    ), gaps
                gaps.append(CoverageGap(
                    code="domain_inventory_unavailable",
                    detail="Admin domain metadata was not enabled for the collector; domains are not grants",
                ))
                cursor = self._advance(cursor, phase="workspaces", metadata_complete=False)
            elif cursor.phase == "workspaces":
                if selector.kind in {"workspace", "item"}:
                    return cursor, RestRoute(
                        "fabric", "fabric.workspaces", f"/workspaces/{selector.workspace_id}", None,
                    ), gaps
                if self.options.admin_workspaces:
                    return cursor, RestRoute(
                        "fabric", "fabric.admin_workspaces", "/admin/workspaces", "workspaces",
                    ), gaps
                return cursor, RestRoute("fabric", "fabric.workspaces", "/workspaces"), gaps
            elif cursor.phase == "memberships":
                remaining = sorted(
                    domain.domain_id for domain in domains if domain.state == "present"
                    and (cursor.after_id is None or domain.domain_id > cursor.after_id)
                )
                active = cursor.active_id or (remaining[0] if remaining else None)
                if active is None or not self.options.admin_domains:
                    cursor = self._advance(
                        cursor, phase="fabric_items", active_id=None, after_id=None,
                    )
                    continue
                cursor = cursor.model_copy(update={"active_id": active})
                return cursor, RestRoute(
                    "fabric", "fabric.domain_workspaces", f"/admin/domains/{active}/workspaces",
                ), gaps
            else:
                remaining_workspaces = []
                for workspace in sorted(workspaces, key=lambda value: value.workspace_id):
                    if cursor.after_id is not None and workspace.workspace_id <= cursor.after_id:
                        continue
                    if selector.kind in {"workspace", "item"}:
                        matches = workspace.workspace_id == selector.workspace_id
                    elif selector.kind == "tenant":
                        matches = True
                    else:
                        ancestors, missing = domain_ancestors(workspace.domain_id, domains) if (
                            workspace.domain_id is not None
                        ) else ((), ())
                        gaps.extend(missing)
                        matches = workspace.domain_id == selector.domain_id or (
                            selector.include_descendants and selector.domain_id in ancestors
                        )
                    if matches:
                        remaining_workspaces.append(workspace.workspace_id)
                active = cursor.active_id or (
                    remaining_workspaces[0] if remaining_workspaces else None
                )
                if active is None:
                    if selector.kind == "domain" and not any(
                        domain.domain_id == selector.domain_id for domain in domains
                    ):
                        gaps.append(CoverageGap(
                            code="domain_metadata_missing", detail="Selected domain was not returned by inventory",
                        ))
                    if selector.kind in {"workspace", "item"} and not workspaces:
                        gaps.append(CoverageGap(
                            code="workspace_metadata_missing", detail="Selected workspace could not be read",
                            workspace_id=selector.workspace_id,
                        ))
                    return None, None, gaps
                cursor = cursor.model_copy(update={"active_id": active})
                if cursor.phase == "powerbi_items":
                    if selector.kind == "item":
                        known = next((
                            item for item in known_items
                            if item.workspace_id == active and item.item_id == selector.item_id
                        ), None)
                        if known is not None and known.workload != "powerbi":
                            cursor = self._advance(
                                cursor, phase="fabric_items", item_type="DataPipeline", after_id=active, active_id=None,
                            )
                            continue
                        return cursor, RestRoute(
                            "powerbi", "powerbi.dataset",
                            f"/groups/{active}/datasets/{selector.item_id}", None,
                        ), gaps
                    return cursor, RestRoute(
                        "powerbi", "powerbi.datasets", f"/groups/{active}/datasets",
                    ), gaps
                if self.options.admin_items_preview:
                    return cursor, RestRoute(
                        "fabric", "fabric.admin_items_preview", "/admin/items", "itemEntities",
                        (("workspaceId", active), ("type", cursor.item_type)),
                    ), gaps
                return cursor, RestRoute(
                    "fabric", "fabric.items", f"/workspaces/{active}/items", query=(("type", cursor.item_type),),
                ), gaps

    async def read_page(
        self, context: MonitoringContext, selector: ScopeSelector, *,
        generation_id: str, observed_at: datetime, continuation: str | None = None,
        workspaces: Sequence[InventoryWorkspace] = (), domains: Sequence[InventoryDomain] = (),
        known_items: Sequence[InventoryItem] = (),
    ) -> InventoryReadPage:
        cursor = self._cursor(context, selector, generation_id, continuation)
        workspaces = tuple(value for value in workspaces if (
            value.tenant_id, value.epoch, value.generation_id,
        ) == (context.tenant_id, context.epoch, generation_id))
        domains = tuple(value for value in domains if (
            value.tenant_id, value.epoch, value.generation_id,
        ) == (context.tenant_id, context.epoch, generation_id))
        known_items = tuple(value for value in known_items if (
            value.tenant_id, value.epoch, value.generation_id,
        ) == (context.tenant_id, context.epoch, generation_id))
        selected, route, gaps = self._route(cursor, selector, workspaces, domains, known_items)
        if selector.kind == "tenant" and self.authority == "caller_visible":
            gaps.append(CoverageGap(
                code="caller_visible_inventory",
                detail="Core item listings are caller-visible; tenant completeness needs explicit Admin Items preview capability",
            ))
        if selected is None or route is None:
            return InventoryReadPage(gaps=bounded_gaps(gaps), finished=True)
        cursor = selected
        try:
            payload = await self.rest.get(route, continuation=cursor.url)
            rows = collection_rows(payload, route)
            next_url = next_page_url(payload, route)
            current_url = validate_rest_url(route, cursor.url or route.url)
            current_hash = hashlib.sha256(current_url.encode()).hexdigest()[:16]
            if next_url is not None and hashlib.sha256(next_url.encode()).hexdigest()[:16] in (
                *cursor.trail, current_hash,
            ):
                raise RestReadError("pagination_cycle", "Inventory repeated a continuation position")
        except RestReadError as exc:
            gaps.append(exc.gap(
                cursor.active_id if cursor.phase in {"fabric_items", "powerbi_items"} else None,
            ))
            if exc.retry_at is not None or exc.code in {"transport_error", "credential_unavailable"}:
                return InventoryReadPage(
                    continuation=cursor.encode(), gaps=bounded_gaps(gaps), retry_at=exc.retry_at,
                )
            if cursor.phase in {"domains", "workspaces", "memberships"}:
                cursor = cursor.model_copy(update={"metadata_complete": False})
            next_cursor = self._next_source(cursor, selector)
            return InventoryReadPage(
                continuation=next_cursor.encode() if next_cursor is not None else None,
                gaps=bounded_gaps(gaps), finished=next_cursor is None,
            )

        page_hash = hashlib.sha256(json.dumps(
            rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode()).hexdigest()
        offset = cursor.offset
        if offset and page_hash != cursor.page_hash:
            gaps.append(CoverageGap(
                code="inventory_page_changed",
                detail="An interrupted source page changed; its prefix is reread rather than skipped",
            ))
            offset = 0
        if offset > len(rows):
            raise RestReadError("invalid_continuation", "Inventory continuation exceeds its source page")
        items: dict[tuple[str, str], InventoryItem] = {}
        workspace_rows: dict[str, InventoryWorkspace] = {}
        domain_rows: dict[str, InventoryDomain] = {}
        for raw in rows[offset:offset + 1_000]:
            try:
                if not isinstance(raw, dict):
                    raise RestReadError("malformed_inventory_row", "Inventory returned a non-object record")
                _assert_payload_tenant(raw, context)
                if cursor.phase == "fabric_items" and raw.get("type") != cursor.item_type:
                    raise RestReadError(
                        "inventory_type_filter_mismatch",
                        "Source inventory did not honor the requested supported type filter; coverage is incomplete",
                    )
                common = {
                    **context.model_dump(), "generation_id": generation_id, "observed_at": observed_at,
                }
                resource_id = _raw_id(raw)
                if cursor.phase == "domains":
                    domain = InventoryDomain(
                        **common, domain_id=resource_id, name=_name(raw, "displayName"),
                        parent_domain_id=_raw_id(raw, "parentDomainId") if raw.get("parentDomainId") else None,
                        state=_state(raw),
                    )
                    prior_domain = domain_rows.get(resource_id) or next((
                        value for value in domains if value.domain_id == resource_id
                    ), None)
                    if prior_domain is not None and prior_domain.parent_domain_id != domain.parent_domain_id:
                        domain = domain.model_copy(update={"state": "unknown"})
                        cursor = cursor.model_copy(update={"metadata_complete": False})
                        gaps.append(CoverageGap(
                            code="domain_hierarchy_changed",
                            detail="A domain changed parent during the inventory generation",
                        ))
                    domain_rows[domain.domain_id] = domain
                elif cursor.phase == "workspaces":
                    if selector.workspace_id is not None and resource_id != selector.workspace_id:
                        raise RestReadError("wrong_workspace", "Workspace metadata belongs to another selection")
                    workspace = InventoryWorkspace(
                        **common, workspace_id=resource_id,
                        name=_name(raw, "name" if route.collection == "workspaces" else "displayName"),
                        capacity_id=_raw_id(raw, "capacityId") if raw.get("capacityId") else None,
                        domain_id=_raw_id(raw, "domainId") if raw.get("domainId") else None,
                        state=_state(raw),
                    )
                    workspace_rows[resource_id] = workspace
                elif cursor.phase == "memberships":
                    existing = workspace_rows.get(resource_id) or next((
                        value for value in workspaces if value.workspace_id == resource_id
                    ), None)
                    if selector.workspace_id is not None and resource_id != selector.workspace_id:
                        continue
                    if existing is None:
                        workspace = InventoryWorkspace(
                            **common, workspace_id=resource_id, name=_name(raw, "displayName"),
                            domain_id=cursor.active_id, state="present",
                        )
                    else:
                        state = existing.state
                        if existing.domain_id is not None and existing.domain_id != cursor.active_id:
                            state = "unknown"
                            gaps.append(CoverageGap(
                                code="domain_membership_changed",
                                detail="Workspace domain membership changed during this generation",
                                workspace_id=resource_id,
                            ))
                        workspace = InventoryWorkspace.model_validate({
                            **existing.model_dump(), **common, "domain_id": cursor.active_id, "state": state,
                        })
                    workspace_rows[resource_id] = workspace
                else:
                    if cursor.active_id is None:
                        raise RestReadError("missing_workspace", "Item inventory has no selected workspace")
                    if raw.get("workspaceId") is not None and _raw_id(raw, "workspaceId") != cursor.active_id:
                        raise RestReadError("wrong_workspace", "Item metadata belongs to another workspace")
                    if selector.item_id is not None and resource_id != selector.item_id:
                        if cursor.phase == "fabric_items":
                            continue
                        raise RestReadError("wrong_item", "Item metadata belongs to another selection")
                    is_powerbi = cursor.phase == "powerbi_items"
                    item_type = "Dataset" if is_powerbi else _name(raw, "type")
                    existing = items.get((cursor.active_id, resource_id)) or next((
                        item for item in known_items
                        if (item.workspace_id, item.item_id) == (cursor.active_id, resource_id)
                    ), None)
                    if is_powerbi and existing is not None:
                        if existing.workload != "powerbi":
                            raise RestReadError("item_type_conflict", "Source inventories disagree about item type")
                        continue
                    if existing is not None and (
                        existing.workload != SUPPORTED_INVENTORY_TYPES.get(item_type)
                        or existing.workload is None and existing.item_type != item_type
                    ):
                        items[(cursor.active_id, resource_id)] = existing.model_copy(update={
                            "state": "unknown", "observed_at": observed_at,
                        })
                        raise RestReadError("item_type_conflict", "Source item type changed during this generation")
                    workspace = next((
                        value for value in workspaces if value.workspace_id == cursor.active_id
                    ), None)
                    ancestors, hierarchy_gaps = domain_ancestors(workspace.domain_id, domains) if (
                        workspace is not None and workspace.domain_id is not None
                    ) else ((), ())
                    gaps.extend(hierarchy_gaps)
                    workload = SUPPORTED_INVENTORY_TYPES[item_type]
                    item = InventoryItem(
                        **common, workspace_id=cursor.active_id, item_id=resource_id,
                        name=_name(raw, "name" if is_powerbi or route.collection == "itemEntities" else "displayName"),
                        item_type=item_type, workload=workload,
                        domain_ids=(workspace.domain_id,) if workspace and workspace.domain_id else (),
                        domain_ancestor_ids=ancestors[1:],
                        state="unknown" if (
                            not cursor.metadata_complete or workspace is None
                            or workspace.state != "present" or hierarchy_gaps
                        ) else _state(raw),
                    )
                    items[(item.workspace_id, item.item_id)] = item
                if _state(raw) == "unknown":
                    if cursor.phase in {"domains", "workspaces", "memberships"}:
                        cursor = cursor.model_copy(update={"metadata_complete": False})
                    gaps.append(CoverageGap(
                        code="resource_state_unknown",
                        detail="Inventory resource is not active; partial enumeration cannot establish deletion",
                    ))
            except (RestReadError, ValidationError) as exc:
                if cursor.phase in {"domains", "workspaces", "memberships"}:
                    cursor = cursor.model_copy(update={"metadata_complete": False})
                gaps.append(exc.gap() if isinstance(exc, RestReadError) else CoverageGap(
                    code="malformed_inventory_row", detail="Inventory record failed the typed metadata contract",
                ))
        end_offset = min(len(rows), offset + 1_000)
        if end_offset < len(rows):
            next_cursor = cursor.model_copy(update={"offset": end_offset, "page_hash": page_hash})
        elif next_url is not None:
            next_cursor = cursor.model_copy(update={
                "url": next_url, "offset": 0, "page_hash": None,
                "trail": (*cursor.trail, current_hash),
            })
        else:
            next_cursor = self._next_source(cursor, selector)
        return InventoryReadPage(
            items=tuple(items.values()), workspaces=tuple(workspace_rows.values()),
            domains=tuple(domain_rows.values()),
            continuation=next_cursor.encode() if next_cursor is not None else None,
            gaps=bounded_gaps(gaps), completed_pages=int(end_offset == len(rows)),
            finished=next_cursor is None,
        )
