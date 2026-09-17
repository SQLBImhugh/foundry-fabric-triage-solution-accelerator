from __future__ import annotations

import base64
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringUnavailable
from triage.monitoring.inventory import (
    FabricInventoryClient,
    InventoryApiOptions,
    RestReadError,
    RestRoute,
    TenantBoundRestClient,
    definition_fingerprint,
    domain_ancestors,
    next_page_url,
    resolve_scope,
    validate_rest_url,
)
from triage.monitoring.rate_limit import (
    AzureSqlRateBudget,
    InMemoryRateBudget,
    RatePolicy,
    schema_statements,
)
from triage.store.azure_sql import AzureSqlDatabase

TENANT = str(UUID(int=1))
EPOCH = str(UUID(int=2))
IDENTITY = str(UUID(int=3))
WORKSPACE = str(UUID(int=4))
ITEM = str(UUID(int=5))
DOMAIN = str(UUID(int=6))
CHILD = str(UUID(int=7))
OTHER_DOMAIN = str(UUID(int=8))
GENERATION = str(UUID(int=9))
SCOPE = str(UUID(int=10))
RULE = str(UUID(int=11))
NOW = datetime(2026, 9, 15, 12, tzinfo=UTC)
CONTEXT = m.MonitoringContext(tenant_id=TENANT, epoch=EPOCH)


@dataclass
class Clock:
    now: datetime = NOW

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)


@dataclass
class Token:
    token: str
    expires_on: int


class Credential:
    def __init__(self, clock: Clock, **claims) -> None:
        self.clock = clock
        self.claims = claims
        self.scopes: list[str] = []

    def get_token(self, *scopes: str) -> Token:
        self.scopes.extend(scopes)
        expiry = int((self.clock() + timedelta(hours=1)).timestamp())
        claims = {
            "tid": TENANT, "oid": IDENTITY, "exp": expiry,
            "aud": scopes[0].removesuffix("/.default"), **self.claims,
        }
        payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        return Token(f"fixture.{payload}.fixture", expiry)


@pytest.fixture
async def rest_factory():
    clients: list[TenantBoundRestClient] = []

    def create(handler, *, clock=None, budget=None, credential=None, **kwargs):
        clock = clock or Clock()
        client = TenantBoundRestClient(
            CONTEXT, IDENTITY, credential or Credential(clock),
            budget or InMemoryRateBudget(clock=clock),
            transport=httpx.MockTransport(handler), clock=clock, **kwargs,
        )
        clients.append(client)
        return client

    yield create
    for client in clients:
        await client.close()


def workspace(*, domain_id=DOMAIN, generation_id=GENERATION, **changes):
    return m.InventoryWorkspace.model_validate({
        **CONTEXT.model_dump(), "generation_id": generation_id, "workspace_id": WORKSPACE,
        "name": "Workspace label", "domain_id": domain_id, "state": "present",
        "observed_at": NOW, **changes,
    })


def domain(domain_id=DOMAIN, parent=None, **changes):
    return m.InventoryDomain.model_validate({
        **CONTEXT.model_dump(), "generation_id": GENERATION, "domain_id": domain_id,
        "name": f"Domain {domain_id[-1]}", "parent_domain_id": parent,
        "state": "present", "observed_at": NOW, **changes,
    })


def item(**changes):
    return m.InventoryItem.model_validate({
        **CONTEXT.model_dump(), "generation_id": GENERATION, "workspace_id": WORKSPACE,
        "item_id": ITEM, "name": "Pipeline", "item_type": "DataPipeline",
        "workload": "fabric_pipeline", "domain_ids": [DOMAIN], "observed_at": NOW, **changes,
    })


def scope(*rules):
    return m.ScopeDefinition(
        **CONTEXT.model_dump(), scope_id=SCOPE, name="Explicit scope", rules=rules,
    )


def rule(kind="domain", *, effect="include", descendants=False, rule_id=RULE, **changes):
    selector = {"tenant_id": TENANT, "kind": kind}
    if kind == "domain":
        selector.update(domain_id=DOMAIN, include_descendants=descendants)
    if kind in {"workspace", "item"}:
        selector["workspace_id"] = WORKSPACE
    if kind == "item":
        selector["item_id"] = ITEM
    selector.update(changes.pop("selector", {}))
    return m.ScopeRule.model_validate({
        "rule_id": rule_id, "selector": selector, "effect": effect, **changes,
    })


def test_fixture_budget_is_shared_across_replicas_and_epochs():
    clock = Clock()
    budget = InMemoryRateBudget(clock=clock)
    policy = RatePolicy(7, 60)
    with ThreadPoolExecutor(max_workers=12) as pool:
        decisions = list(pool.map(
            lambda _: budget.acquire(CONTEXT, "service:fabric", policy), range(200),
        ))
    assert sum(decision.allowed for decision in decisions) == 7
    different_epoch = m.MonitoringContext(tenant_id=TENANT, epoch=str(UUID(int=99)))
    assert not budget.acquire(different_epoch, "service:fabric", policy).allowed
    assert {decision.retry_at for decision in decisions if not decision.allowed} == {NOW + timedelta(seconds=60)}
    clock.advance(60)
    assert budget.acquire(CONTEXT, "service:fabric", policy).allowed


def test_budget_cooldown_never_shortens_and_policy_mismatch_is_visible():
    clock = Clock()
    budget = InMemoryRateBudget(clock=clock)
    policy = RatePolicy(10, 60)
    budget.defer(CONTEXT, "service:powerbi", policy, seconds=600)
    clock.advance(20)
    decision = budget.defer(CONTEXT, "service:powerbi", policy, seconds=5)
    assert decision.retry_at == NOW + timedelta(seconds=600)
    assert not budget.acquire(CONTEXT, "service:powerbi", policy).allowed
    with pytest.raises(MonitoringUnavailable, match="disagree"):
        budget.acquire(CONTEXT, "service:powerbi", RatePolicy(100, 60))
    clock.advance(580)
    assert budget.acquire(CONTEXT, "service:powerbi", policy).allowed


class BudgetDatabase(AzureSqlDatabase):
    def __init__(self, rows):
        self.rows = rows
        self.statements = []
        self.inside_transaction = False

    @contextmanager
    def transaction(self):
        assert not self.inside_transaction
        self.inside_transaction = True
        try:
            yield self
        finally:
            self.inside_transaction = False

    def query(self, sql, *params):
        assert self.inside_transaction
        self.statements.append((sql, params))
        return self.rows


def test_sql_budget_uses_database_time_conditional_update_and_explicit_ddl():
    database = BudgetDatabase([(0, NOW.replace(tzinfo=None), (NOW + timedelta(seconds=91)).replace(tzinfo=None))])
    budget = AzureSqlRateBudget(database)
    decision = budget.acquire(CONTEXT, "service:fabric", RatePolicy(20, 60))
    assert not decision.allowed and decision.retry_at == NOW + timedelta(seconds=91)
    sql, params = database.statements[0]
    assert "SYSUTCDATETIME()" in sql
    assert "WITH (UPDLOCK, HOLDLOCK)" in sql
    assert "IF @@ROWCOUNT = 1 SET @granted = 1" in sql
    assert "CREATE TABLE" not in sql
    assert params[0] == TENANT and params[2:] == (20, 60, None)
    assert "CREATE TABLE [dbo].[triage_monitoring_rate_budget]" in schema_statements()[0]
    with pytest.raises(ValueError, match="Refusing"):
        schema_statements("bad; DROP TABLE something")


@pytest.mark.parametrize("rows", [[], [(True, NOW, None)], [(2, NOW, NOW)]])
def test_sql_budget_invalid_decision_fails_closed(rows):
    with pytest.raises(MonitoringUnavailable, match="invalid decision"):
        AzureSqlRateBudget(BudgetDatabase(rows)).acquire(CONTEXT, "service:fabric", RatePolicy(2, 60))


@pytest.mark.parametrize(
    ("claims", "code"),
    [
        ({"tid": str(UUID(int=100))}, "wrong_tenant"),
        ({"oid": str(UUID(int=100))}, "wrong_collector_identity"),
        ({"aud": "https://another.service"}, "wrong_token_audience"),
        ({"tid": "common"}, "token_context_unverifiable"),
        ({"exp": 0}, "token_context_unverifiable"),
    ],
)
async def test_token_is_explicitly_tenant_identity_and_audience_bound(rest_factory, claims, code):
    requests = []
    clock = Clock()
    client = rest_factory(
        lambda request: requests.append(request) or httpx.Response(200, json={"value": []}),
        clock=clock, credential=Credential(clock, **claims),
    )
    with pytest.raises(RestReadError) as error:
        await client.get(RestRoute("fabric", "fabric.workspaces", "/workspaces"))
    assert error.value.code == code
    assert requests == []


async def test_unverifiable_opaque_token_is_not_a_fallback(rest_factory):
    class OpaqueCredential:
        def get_token(self, *scopes):
            return Token("fixture-not-a-jwt", int((NOW + timedelta(hours=1)).timestamp()))

    client = rest_factory(lambda _: pytest.fail("HTTP was not permitted"), credential=OpaqueCredential())
    with pytest.raises(RestReadError, match="verifiable tenant"):
        await client.get(RestRoute("fabric", "fabric.workspaces", "/workspaces"))


async def test_retry_after_coordinates_two_clients_without_sleep_or_hidden_retry(rest_factory):
    clock = Clock()
    budget = InMemoryRateBudget(clock=clock)
    calls = []

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        return httpx.Response(429, headers={"Retry-After": "700"}, text="not logged")

    first = rest_factory(handler, clock=clock, budget=budget)
    second = rest_factory(handler, clock=clock, budget=budget)
    route = RestRoute("fabric", "fabric.jobs", f"/workspaces/{WORKSPACE}/items/{ITEM}/jobs/instances")
    with pytest.raises(RestReadError) as throttled:
        await first.get(route)
    with pytest.raises(RestReadError) as limited:
        await second.get(route)
    assert len(calls) == 1
    assert throttled.value.code == "service_throttled"
    assert limited.value.code == "request_budget_exhausted"
    assert throttled.value.retry_at == limited.value.retry_at == NOW + timedelta(seconds=700)


@pytest.mark.parametrize(
    ("header", "seconds", "code"),
    [
        ("Tue, 15 Sep 2026 12:02:00 GMT", 120, "service_throttled"),
        ("not-a-date", 60, "service_throttled_invalid_retry_after"),
        ("-1", 60, "service_throttled_invalid_retry_after"),
        (None, 60, "service_throttled_invalid_retry_after"),
    ],
)
async def test_retry_after_date_and_malformed_header_are_explicit(rest_factory, header, seconds, code):
    client = rest_factory(lambda _: httpx.Response(
        429, headers={"Retry-After": header} if header is not None else {},
    ))
    with pytest.raises(RestReadError) as error:
        await client.get(RestRoute("powerbi", "powerbi.groups", "/groups"))
    assert error.value.code == code
    assert error.value.retry_at == NOW + timedelta(seconds=seconds)


@pytest.mark.parametrize("status", [204, 206, 301, 401, 403, 404, 500, 503])
async def test_non_200_read_is_not_a_healthy_empty_response(rest_factory, status):
    calls = []
    client = rest_factory(lambda request: calls.append(request) or httpx.Response(
        status, headers={"Location": "https://example.invalid"}, json={"value": []},
    ))
    with pytest.raises(RestReadError) as error:
        await client.get(RestRoute("fabric", "fabric.workspaces", "/workspaces"))
    assert error.value.status_code == status
    assert len(calls) == 1


@pytest.mark.parametrize(
    "content",
    [b"[]", b"null", b"not JSON", b'{"value": NaN}', b" " * 201],
)
async def test_malformed_or_oversized_responses_fail_loudly(rest_factory, content):
    client = rest_factory(lambda _: httpx.Response(200, content=content), max_response_bytes=200)
    with pytest.raises(RestReadError):
        await client.get(RestRoute("fabric", "fabric.workspaces", "/workspaces"))


@pytest.mark.parametrize("header", [False, True])
async def test_response_tenant_mismatch_is_a_coverage_failure(rest_factory, header):
    wrong = str(UUID(int=100))
    client = rest_factory(lambda _: httpx.Response(
        200, headers={"x-ms-tenant-id": wrong} if header else {},
        json={"value": []} if header else {"value": [], "tenantId": wrong},
    ))
    with pytest.raises(RestReadError) as error:
        await client.get(RestRoute("fabric", "fabric.workspaces", "/workspaces"))
    assert error.value.code == "wrong_tenant"


@pytest.mark.parametrize(
    "url",
    [
        "http://api.fabric.microsoft.com/v1/workspaces?continuationToken=x",
        "https://api.fabric.microsoft.com.evil.invalid/v1/workspaces?continuationToken=x",
        "https://person@api.fabric.microsoft.com/v1/workspaces?continuationToken=x",
        "https://api.fabric.microsoft.com:444/v1/workspaces?continuationToken=x",
        "https://api.fabric.microsoft.com/v1/admin/workspaces?continuationToken=x",
        "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=x#fragment",
        "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=x&tenantId=other",
        "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=x&continuationToken=y",
    ],
)
def test_continuation_cannot_change_host_path_scope_or_tenant(url):
    route = RestRoute("fabric", "fabric.workspaces", "/workspaces")
    with pytest.raises(RestReadError):
        validate_rest_url(route, url)


def test_fabric_continuation_preserves_released_domain_query_and_checks_aliases():
    route = RestRoute("fabric", "fabric.domains", "/admin/domains", "domains", (("preview", "false"),))
    payload = {
        "domains": [], "continuationToken": "part%2Btwo",
        "continuationUri": "https://api.fabric.microsoft.com/v1/admin/domains?continuationToken=part%2Btwo",
    }
    next_url = next_page_url(payload, route)
    assert httpx.URL(next_url).params["preview"] == "false"
    assert httpx.URL(next_url).params["continuationToken"] == "part+two"
    with pytest.raises(RestReadError, match="disagree"):
        next_page_url({**payload, "continuationToken": "different"}, route)
    with pytest.raises(RestReadError, match="scope"):
        next_page_url({**payload, "continuationUri": payload["continuationUri"] + "&preview=true"}, route)


def test_odata_next_link_preserves_resource_and_fixed_top():
    path = f"/groups/{WORKSPACE}/datasets/{ITEM}/refreshes"
    route = RestRoute("powerbi", "powerbi.refreshes", path, query=(("$top", "60"),))
    next_url = next_page_url({
        "@odata.nextLink": f"https://api.powerbi.com/v1.0/myorg{path}?$skip=60",
    }, route)
    assert dict(httpx.URL(next_url).params) == {"$top": "60", "$skip": "60"}
    for payload in (
        {"hasMore": True}, {"continuationToken": "fabric-token"},
        {"@odata.nextLink": route.url}, {"@odata.nextLink": route.url + "&$top=200"},
    ):
        with pytest.raises(RestReadError):
            next_page_url(payload, route)


def test_definition_fingerprint_is_canonical_bounded_and_not_a_permission():
    assert definition_fingerprint({"b": 2, "a": {"x": 1}}) == definition_fingerprint({"a": {"x": 1}, "b": 2})
    with pytest.raises(ValidationError):
        definition_fingerprint({"content": "x" * 70_000})


@pytest.mark.parametrize("field", ["admin_items_preview", "admin_domains"])
@pytest.mark.parametrize("value", ["false", "true", 0, 1, None])
def test_inventory_api_selection_does_not_coerce_preview_or_admin_opt_in(field, value):
    with pytest.raises(ValueError, match="explicit boolean"):
        InventoryApiOptions(**{field: value})


def test_domain_descendants_exclusions_movement_and_automatic_admission_are_distinct():
    domains = (domain(), domain(CHILD, DOMAIN), domain(OTHER_DOMAIN))
    child_workspace = workspace(domain_id=CHILD)
    source = item(domain_ids=(CHILD,))
    direct_only = resolve_scope(scope(rule()), source, domains=domains, workspaces=(child_workspace,))
    assert direct_only.state == "out_of_scope"
    selected = resolve_scope(
        scope(rule(descendants=True, auto_enrol_detection_only=True)),
        source, domains=domains, workspaces=(child_workspace,),
    )
    assert selected.state == "included" and selected.auto_enrol_detection_only
    assert domain_ancestors(CHILD, domains) == ((CHILD, DOMAIN), ())
    denied = resolve_scope(scope(
        rule(descendants=True),
        rule("workspace", effect="exclude", rule_id=str(UUID(int=100))),
    ), source, domains=domains, workspaces=(child_workspace,))
    assert denied.state == "excluded" and not denied.auto_enrol_detection_only
    moved = resolve_scope(
        scope(rule(descendants=True)), source, domains=domains,
        workspaces=(workspace(domain_id=OTHER_DOMAIN),),
    )
    assert moved.state == "out_of_scope"


def test_incomplete_domain_exclusion_is_not_bypassed_by_tenant_include():
    result = resolve_scope(
        scope(rule("tenant"), rule(effect="exclude", descendants=True, rule_id=str(UUID(int=100)))),
        item(domain_ids=(CHILD,)), domains=(domain(CHILD, str(UUID(int=999))),),
        workspaces=(workspace(domain_id=CHILD),), metadata_complete=False,
    )
    assert result.state == "unknown"
    assert {"domain_metadata_missing", "scope_metadata_incomplete"} <= {gap.code for gap in result.gaps}


def test_domain_cycles_and_stale_metadata_are_explicit():
    ancestors, gaps = domain_ancestors(DOMAIN, (domain(parent=CHILD), domain(CHILD, DOMAIN)))
    assert ancestors == (DOMAIN, CHILD)
    assert gaps[0].code == "domain_cycle"
    result = resolve_scope(
        scope(rule(descendants=True)), item(domain_ids=(CHILD,)),
        domains=(domain(CHILD, DOMAIN, generation_id=str(UUID(int=101))),),
    )
    assert result.state == "unknown"


def test_unsupported_item_and_wrong_tenant_never_resolve_to_monitoring():
    unsupported = item(item_type="Notebook", workload=None, unsupported_reason="No notebook detector")
    assert resolve_scope(scope(rule("tenant")), unsupported).state == "unsupported"
    with pytest.raises(ValueError, match="different tenants"):
        resolve_scope(scope(rule("tenant")), item(tenant_id=str(UUID(int=100))))


async def collect_inventory(client, selector):
    continuation = None
    workspaces = {}
    domains = {}
    items = {}
    pages = []
    for _ in range(100):
        page = await client.read_page(
            CONTEXT, selector, generation_id=GENERATION, observed_at=NOW, continuation=continuation,
            workspaces=tuple(workspaces.values()), domains=tuple(domains.values()), known_items=tuple(items.values()),
        )
        pages.append(page)
        workspaces.update((value.workspace_id, value) for value in page.workspaces)
        domains.update((value.domain_id, value) for value in page.domains)
        items.update(((value.workspace_id, value.item_id), value) for value in page.items)
        if page.finished:
            return pages, workspaces, domains, items
        assert page.continuation is not None
        continuation = page.continuation
    pytest.fail("Fixture inventory did not terminate")


async def test_admin_inventory_pages_domains_display_names_and_supported_mapping(rest_factory):
    calls = []
    second_item = str(UUID(int=101))
    unsupported_item = str(UUID(int=102))

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        path = request.url.path
        if path == "/v1/admin/domains":
            assert request.url.params["preview"] == "false"
            if "continuationToken" not in request.url.params:
                return httpx.Response(200, json={
                    "domains": [{"id": DOMAIN, "displayName": "Parent"}], "continuationToken": "domains-two",
                })
            return httpx.Response(200, json={
                "domains": [{"id": CHILD, "parentDomainId": DOMAIN, "displayName": "Child"}],
            })
        if path == "/v1/admin/workspaces":
            return httpx.Response(200, json={"workspaces": [{"id": WORKSPACE, "name": "Named workspace"}]})
        if path == f"/v1/admin/domains/{DOMAIN}/workspaces":
            return httpx.Response(200, json={"value": []})
        if path == f"/v1/admin/domains/{CHILD}/workspaces":
            return httpx.Response(200, json={"value": [{"id": WORKSPACE}]})
        if path == "/v1/admin/items":
            assert request.url.params["workspaceId"] == WORKSPACE
            if "continuationToken" not in request.url.params:
                return httpx.Response(200, json={
                    "itemEntities": [{"id": ITEM, "name": "Pipeline", "type": "DataPipeline", "workspaceId": WORKSPACE}],
                    "continuationToken": "items-two",
                })
            return httpx.Response(200, json={"itemEntities": [
                {"id": second_item, "name": "Model", "type": "SemanticModel", "workspaceId": WORKSPACE},
                {"id": unsupported_item, "name": "Notebook", "type": "Notebook", "workspaceId": WORKSPACE},
            ]})
        if path == f"/v1.0/myorg/groups/{WORKSPACE}/datasets":
            return httpx.Response(200, json={"value": [{"id": second_item, "name": "Model"}]})
        pytest.fail(f"Unexpected fixture route {path}")

    client = FabricInventoryClient(rest_factory(handler), options=InventoryApiOptions(True, True, True, True))
    pages, workspaces, domains, items = await collect_inventory(
        client, m.ScopeSelector(tenant_id=TENANT, kind="domain", domain_id=DOMAIN, include_descendants=True),
    )
    assert client.authority == "tenant_admin" and "preview" in client.adapter
    assert workspaces[WORKSPACE].name == "Named workspace"
    assert workspaces[WORKSPACE].domain_id == CHILD
    assert domains[CHILD].parent_domain_id == DOMAIN
    assert len(items) == 3
    assert items[(WORKSPACE, ITEM)].workload == "fabric_pipeline"
    assert items[(WORKSPACE, second_item)].workload == "powerbi"
    assert items[(WORKSPACE, unsupported_item)].workload is None
    assert items[(WORKSPACE, ITEM)].domain_ids == (CHILD,)
    assert items[(WORKSPACE, ITEM)].domain_ancestor_ids == (DOMAIN,)
    assert sum(len(page.items) for page in pages) == 3
    assert {gap.code for page in pages for gap in page.gaps} == {"unsupported_item_type"}
    assert len(calls) == 8


async def test_core_inventory_is_never_tenant_complete_or_silent_preview_fallback(rest_factory):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"value": []})

    client = FabricInventoryClient(rest_factory(handler), options=InventoryApiOptions(powerbi_datasets=False))
    pages, _, _, items = await collect_inventory(client, m.ScopeSelector(tenant_id=TENANT, kind="tenant"))
    assert client.authority == "caller_visible"
    assert not items
    assert calls == ["/v1/workspaces"]
    assert "caller_visible_inventory" in {gap.code for page in pages for gap in page.gaps}
    assert "/v1/admin/items" not in calls
    with pytest.raises(ValueError, match="explicit admin-workspace"):
        InventoryApiOptions(admin_items_preview=True)


async def test_inventory_keeps_known_rows_when_a_page_is_partial_or_denied(rest_factory):
    client = FabricInventoryClient(rest_factory(lambda request: httpx.Response(200, json={
        "value": [
            {"id": ITEM, "type": "DataPipeline", "displayName": "Known"},
            {"id": str(UUID(int=100)), "type": "DataPipeline"},
            {"displayName": "Missing identity", "type": "Notebook"},
        ],
    })), options=InventoryApiOptions(powerbi_datasets=False))
    selector = m.ScopeSelector(tenant_id=TENANT, kind="workspace", workspace_id=WORKSPACE)
    state = json.loads(client.initial_cursor(CONTEXT, selector, GENERATION))
    state.update(phase="fabric_items", active_id=WORKSPACE)
    page = await client.read_page(
        CONTEXT, selector, generation_id=GENERATION, observed_at=NOW, continuation=json.dumps(state),
        workspaces=(workspace(),), domains=(domain(),),
    )
    assert len(page.items) == 1 and page.items[0].item_id == ITEM
    assert {gap.code for gap in page.gaps} == {"metadata_incomplete", "missing_identity"}
    assert page.continuation is not None and not page.finished


async def test_inventory_resume_keeps_a_429_position_without_suppressing_other_metadata(rest_factory):
    count = 0

    def handler(request):
        nonlocal count
        count += 1
        return httpx.Response(429, headers={"Retry-After": "300"})

    client = FabricInventoryClient(rest_factory(handler), options=InventoryApiOptions(admin_domains=True))
    selector = m.ScopeSelector(tenant_id=TENANT, kind="tenant")
    cursor = client.initial_cursor(CONTEXT, selector, GENERATION)
    page = await client.read_page(
        CONTEXT, selector, generation_id=GENERATION, observed_at=NOW, continuation=cursor,
    )
    assert page.continuation == cursor and page.retry_at == NOW + timedelta(seconds=300)
    assert page.completed_pages == 0 and not page.finished and count == 1
    assert "service_throttled" in {gap.code for gap in page.gaps}


async def test_inventory_oversized_api_page_is_resumed_in_typed_batches(rest_factory):
    raw = [
        {"id": str(UUID(int=index + 100)), "displayName": f"Domain {index}"} for index in range(1_001)
    ]
    client = FabricInventoryClient(
        rest_factory(lambda _: httpx.Response(200, json={"domains": raw})),
        options=InventoryApiOptions(admin_domains=True),
    )
    selector = m.ScopeSelector(tenant_id=TENANT, kind="tenant")
    first = await client.read_page(CONTEXT, selector, generation_id=GENERATION, observed_at=NOW)
    second = await client.read_page(
        CONTEXT, selector, generation_id=GENERATION, observed_at=NOW,
        continuation=first.continuation, domains=first.domains,
    )
    assert len(first.domains) == 1_000 and len(second.domains) == 1
    assert first.completed_pages == 0 and second.completed_pages == 1
    assert len({value.domain_id for value in (*first.domains, *second.domains)}) == 1_001


async def test_inventory_cursor_cannot_be_reused_after_context_or_adapter_changes(rest_factory):
    rest = rest_factory(lambda _: pytest.fail("Cursor validation must precede HTTP"))
    client = FabricInventoryClient(rest)
    selector = m.ScopeSelector(tenant_id=TENANT, kind="tenant")
    cursor = client.initial_cursor(CONTEXT, selector, GENERATION)
    changed = FabricInventoryClient(rest, options=InventoryApiOptions(admin_domains=True))
    with pytest.raises(RestReadError, match="another scan"):
        await changed.read_page(
            CONTEXT, selector, generation_id=GENERATION, observed_at=NOW, continuation=cursor,
        )
    with pytest.raises(RestReadError, match="another scan"):
        await client.read_page(
            CONTEXT, selector, generation_id=str(UUID(int=100)), observed_at=NOW, continuation=cursor,
        )


async def test_denied_domain_metadata_marks_items_unknown_instead_of_bypassing_exclusions(rest_factory):
    def handler(request):
        if request.url.path == "/v1/admin/domains":
            return httpx.Response(403)
        if request.url.path == "/v1/admin/workspaces":
            return httpx.Response(200, json={"workspaces": [{"id": WORKSPACE, "name": "Workspace"}]})
        if request.url.path == "/v1/admin/items":
            return httpx.Response(200, json={
                "itemEntities": [{"id": ITEM, "workspaceId": WORKSPACE, "type": "DataPipeline", "name": "Pipeline"}],
            })
        pytest.fail(f"Unexpected fixture route {request.url.path}")

    client = FabricInventoryClient(rest_factory(handler), options=InventoryApiOptions(True, True, True, False))
    pages, workspaces, _, items = await collect_inventory(client, m.ScopeSelector(tenant_id=TENANT, kind="tenant"))
    assert any(gap.code == "http_403" for page in pages for gap in page.gaps)
    source = items[(WORKSPACE, ITEM)]
    assert source.state == "unknown"
    result = resolve_scope(
        scope(rule("tenant"), rule(effect="exclude", rule_id=str(UUID(int=100)))),
        source, workspaces=tuple(workspaces.values()),
    )
    assert result.state == "unknown"
