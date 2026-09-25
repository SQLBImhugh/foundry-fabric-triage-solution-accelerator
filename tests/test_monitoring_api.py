from __future__ import annotations

import ast
import socket
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import create_autospec
from urllib.request import OpenerDirector
from uuid import NAMESPACE_URL, uuid5

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from pydantic import ValidationError

from triage.command_center import monitoring
from triage.command_center.api import authenticated_actor, create_app
from triage.command_center.auth import EntraTokenVerifier
from triage.command_center.models import Actor, ApiFailure, WebSettings
from triage.command_center.monitoring import (
    BootstrapResponse,
    InventoryRefreshInput,
    MonitoringService,
    ScopeActivationInput,
    create_monitoring_router,
)
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringLeaseLost,
    MonitoringNotBootstrapped,
    MonitoringSchemaMismatch,
    MonitoringStoreError,
    MonitoringUnavailable,
    WebMonitoringStore,
)
from triage.monitoring.models import (
    ActivateScopeRequest,
    ActivationPlan,
    ActivationReceipt,
    BootstrapInspection,
    CanonicalId,
    CapabilityObservation,
    CoverageGap,
    CoverageView,
    DeploymentControl,
    InventoryDomain,
    InventoryItem,
    InventoryWorkspace,
    MonitoringContext,
    MonitoringSnapshot,
    MonitoringTarget,
    MonitoringWork,
    OwnedConnectorManifest,
    PageQuery,
    RecordPage,
    RegistryVersion,
    SafetyReview,
    SafetyReviewOperationReceipt,
    SafetyReviewRequest,
    ScopeDefinition,
    ScopePolicy,
    ScopePreviewRequest,
    ScopeRule,
    ScopeSelector,
    TargetIdentity,
    TargetQuery,
)

TENANT = "10000000-0000-0000-0000-000000000001"
EPOCH = "20000000-0000-0000-0000-000000000002"
USER = "30000000-0000-0000-0000-000000000003"
CLIENT = "40000000-0000-0000-0000-000000000004"
WORKSPACE = "50000000-0000-0000-0000-000000000005"
ITEM = "60000000-0000-0000-0000-000000000006"
SCOPE = "70000000-0000-0000-0000-000000000007"
RULE = "80000000-0000-0000-0000-000000000008"
GENERATION = "90000000-0000-0000-0000-000000000009"
PLAN = "a0000000-0000-0000-0000-00000000000a"
OPERATION = "b0000000-0000-0000-0000-00000000000b"
DOMAIN = "c0000000-0000-0000-0000-00000000000c"
CAPABILITY = "d0000000-0000-0000-0000-00000000000d"
REVIEW = "e0000000-0000-0000-0000-00000000000e"
OTHER = "f0000000-0000-0000-0000-00000000000f"
NOW = datetime(2026, 9, 15, 12, tzinfo=UTC)
HASH = "a" * 64
PREFIX = "/api/monitoring"
MUTATIONS = ("preview", "activate", "refresh", "review")
READS = (
    "bootstrap", "snapshot", "scopes", "inventory", "targets", "workspaces",
    "domains", "connectors", "plan", "activation", "safety_review", "safety_review_operation",
)
STORE_METHODS = (
    "inspect_bootstrap", "snapshot", "list_scopes", "list_inventory", "list_targets",
    "list_workspaces", "list_domains", "list_connectors", "get_plan", "get_activation",
    "preview_scope", "activate_scope", "request_discovery", "record_safety_review",
    "get_safety_review", "get_safety_review_operation",
)
MUTATION_METHODS = {
    "preview_scope", "activate_scope", "request_discovery", "record_safety_review",
}


class WebStoreSpec(WebMonitoringStore):
    component = "web"


class RecordingStore:
    """Typed HTTP-facing fake methods behind the web-only monitoring interface.

    All other protocol calls fail. The fake records admission and committed
    mutations separately so a refused request cannot be mistaken for a write.
    It is not a fixture implementation for application use.
    """

    def __init__(self) -> None:
        self.port = create_autospec(WebStoreSpec, instance=True, spec_set=True)
        self.port.component = "web"
        for name in dir(WebMonitoringStore):
            if not name.startswith("_") and callable(getattr(WebMonitoringStore, name)):
                getattr(self.port, name).side_effect = self.unexpected
        for name in STORE_METHODS:
            getattr(self.port, name).side_effect = getattr(self, name)
        self.calls: list[tuple[str, object]] = []
        self.threads: list[int] = []
        self.mutations: list[tuple[str, object]] = []
        self.failures: dict[str, Exception] = {}
        self.control = DeploymentControl(
            tenant_id=TENANT, epoch=EPOCH, revision=7, maintenance=False,
            activation_cutoff=NOW, updated_at=NOW,
        )
        self.inspection: BootstrapInspection | None = None
        self.gap = CoverageGap(
            code="inventory_partial", detail="A workspace page is still unavailable.",
            workspace_id=WORKSPACE,
        )
        self.scope = ScopePolicy(
            **self.scope_definition().model_dump(), revision=7, updated_at=NOW,
        )
        self.target = TargetIdentity(
            tenant_id=TENANT, epoch=EPOCH, workload="fabric_pipeline",
            workspace_id=WORKSPACE, item_id=ITEM,
        )
        self.preview_blocked = False
        self.plans = {PLAN: self.make_plan(self.preview_input(), plan_id=PLAN)}
        self.preview_ids: dict[str, ActivationPlan] = {}
        self.activations: dict[str, tuple[ActivateScopeRequest, ActivationReceipt]] = {}
        self.works: dict[str, MonitoringWork] = {}
        self.discoveries: dict[str, tuple[RegistryVersion, ScopeSelector, MonitoringWork]] = {}
        self.discovery_time = NOW
        self.reviews: dict[str, SafetyReview] = {}
        self.review_operations: dict[str, SafetyReviewOperationReceipt] = {}
        self.review_intents: dict[str, SafetyReviewRequest] = {}
        self.capability: CapabilityObservation | None = None

    @staticmethod
    def unexpected(*_args, **_kwargs):
        raise AssertionError("The monitoring HTTP surface called a worker/action store operation")

    @property
    def version(self) -> RegistryVersion:
        return RegistryVersion(
            tenant_id=self.control.tenant_id, epoch=self.control.epoch,
            revision=self.control.revision,
        )

    @staticmethod
    def scope_definition() -> ScopeDefinition:
        return ScopeDefinition(
            tenant_id=TENANT, epoch=EPOCH, scope_id=SCOPE, name="Synthetic monitoring scope",
            rules=(ScopeRule(
                rule_id=RULE, selector=ScopeSelector(tenant_id=TENANT, kind="tenant"),
                effect="include",
            ),),
        )

    def preview_input(self) -> ScopePreviewRequest:
        return ScopePreviewRequest(
            expected=self.version, idempotency_id=OPERATION, scope=self.scope_definition(),
            requested_by=USER,
        )

    def review_input(self, *, verified: bool = False) -> SafetyReviewRequest:
        return SafetyReviewRequest(
            request_id=OPERATION, expected=self.version, expected_review_revision=0,
            review=SafetyReview(
                review_id=REVIEW, target=self.target, revision=1,
                policy_revision=self.version.revision, action="pipeline_rerun",
                state="verified" if verified else "pending",
                reviewer_id=OTHER, reviewed_at=NOW, expires_at=NOW + timedelta(hours=1),
                definition_hash=HASH, parameters={}, replay_safe=verified,
                exact_correlation_verified=verified,
                detail="Review a synthetic pipeline for replay safety.",
            ),
        )

    def make_plan(self, request: ScopePreviewRequest, *, plan_id: str) -> ActivationPlan:
        blocked = self.preview_blocked
        return ActivationPlan(
            **request.model_dump(), plan_id=plan_id, created_at=NOW,
            expires_at=NOW + timedelta(minutes=15), inventory_generations=(GENERATION,),
            inventory_completeness="partial" if blocked else "complete",
            status="blocked" if blocked else "ready", poll_count_delta=1,
            subscription_count_delta=0, gaps=(self.gap,) if blocked else (),
        )

    def _record(self, method: str, value: object) -> None:
        self.calls.append((method, value))
        self.threads.append(threading.get_ident())
        if failure := self.failures.get(method):
            raise failure

    def _context(self, context: MonitoringContext) -> None:
        if (context.tenant_id, context.epoch) != (
            self.control.tenant_id, self.control.epoch,
        ):
            raise MonitoringConflict("The fixture epoch changed")

    def _expected(self, expected: RegistryVersion) -> None:
        self._context(expected)
        if expected != self.version:
            raise MonitoringConflict("The fixture registry revision changed")
        if self.control.maintenance:
            raise MonitoringUnavailable("The fixture is in maintenance")

    def inspect_bootstrap(self, *, expected_tenant_id: CanonicalId) -> BootstrapInspection:
        self._record("inspect_bootstrap", expected_tenant_id)
        assert expected_tenant_id == TENANT
        return self.inspection or BootstrapInspection(
            status="maintenance" if self.control.maintenance else "ready",
            expected_tenant_id=expected_tenant_id, found_schema_version=1, control=self.control,
            detail="Synthetic deployment control.",
        )

    def snapshot(self, context: MonitoringContext) -> MonitoringSnapshot:
        self._record("snapshot", context)
        self._context(context)
        return MonitoringSnapshot(
            control=self.control,
            coverage=CoverageView(
                **self.version.model_dump(), as_of=NOW, inventory_completeness="partial",
                capability_completeness="unknown", scope_item_count=None,
                discovered_count=3, access_verified_count=1, admitted_count=1,
                current_count=0, action_enabled_count=0, unsupported_count=0, backlog_count=2,
                checkpoint_lag_seconds=None, gaps=(self.gap,),
            ),
        )

    def list_scopes(self, query: PageQuery) -> RecordPage[ScopePolicy]:
        self._record("list_scopes", query)
        self._context(query)
        return RecordPage[ScopePolicy](
            version=self.version, as_of=NOW, items=(self.scope,), next_cursor="scope-next",
        )

    def list_inventory(self, query: TargetQuery) -> RecordPage[InventoryItem]:
        self._record("list_inventory", query)
        self._context(query)
        rows = (
            InventoryItem(
                tenant_id=query.tenant_id, epoch=query.epoch, generation_id=GENERATION,
                workspace_id=WORKSPACE, item_id=ITEM, name="Synthetic model",
                item_type="SemanticModel", workload="powerbi", observed_at=NOW,
            ),
            InventoryItem(
                tenant_id=query.tenant_id, epoch=query.epoch, generation_id=GENERATION,
                workspace_id=WORKSPACE, item_id=OTHER, name="Synthetic pipeline",
                item_type="DataPipeline", workload="fabric_pipeline",
                state="unknown", observed_at=NOW,
            ),
        )
        items = tuple(row for row in rows if (
            (query.workload is None or row.workload == query.workload)
            and (query.workspace_id is None or row.workspace_id == query.workspace_id)
        ))[:query.limit]
        return RecordPage[InventoryItem](
            version=self.version, as_of=NOW, items=items, next_cursor="inventory-next",
        )

    def list_targets(self, query: TargetQuery) -> RecordPage[MonitoringTarget]:
        self._record("list_targets", query)
        self._context(query)
        target = MonitoringTarget(
            identity=self.target, name="Synthetic pipeline", scope_ids=(SCOPE,),
            admitted_rule_ids=(RULE,), inventory_generation=GENERATION,
            capability_id=CAPABILITY, policy_revision=self.version.revision, admitted_at=NOW,
            state="paused", admission_basis="reviewed", reason="Source access is unverified.",
        )
        items = (target,) if query.include_inactive else ()
        return RecordPage[MonitoringTarget](version=self.version, as_of=NOW, items=items)

    def list_workspaces(self, query: PageQuery) -> RecordPage[InventoryWorkspace]:
        self._record("list_workspaces", query)
        self._context(query)
        return RecordPage[InventoryWorkspace](
            version=self.version, as_of=NOW, next_cursor="workspace-next",
            items=(InventoryWorkspace(
                tenant_id=query.tenant_id, epoch=query.epoch, workspace_id=WORKSPACE,
                name="Synthetic workspace", domain_id=DOMAIN, capacity_id=OTHER,
                state="present", observed_at=NOW, generation_id=GENERATION,
            ),),
        )

    def list_domains(self, query: PageQuery) -> RecordPage[InventoryDomain]:
        self._record("list_domains", query)
        self._context(query)
        return RecordPage[InventoryDomain](
            version=self.version, as_of=NOW, next_cursor="domain-next",
            items=(InventoryDomain(
                tenant_id=query.tenant_id, epoch=query.epoch, domain_id=DOMAIN,
                name="Synthetic domain", parent_domain_id=OTHER, state="unknown",
                observed_at=NOW, generation_id=GENERATION,
            ),),
        )

    def list_connectors(self, query: PageQuery) -> RecordPage[OwnedConnectorManifest]:
        self._record("list_connectors", query)
        self._context(query)
        return RecordPage[OwnedConnectorManifest](
            version=self.version, as_of=NOW,
            items=(OwnedConnectorManifest(
                tenant_id=query.tenant_id, epoch=query.epoch, connector_id=OTHER,
                ownership_id=SCOPE, revision=1, policy_revision=self.version.revision,
                workspace_id=WORKSPACE, eventstream_id=ITEM, destination_id="destination",
                name="Synthetic blocked connector", sources=(),
                desired_definition={f"part-{number}": "x" * 800 for number in range(10)},
                state="blocked", updated_at=NOW, gaps=(self.gap,),
            ),),
        )

    def preview_scope(self, request: ScopePreviewRequest) -> ActivationPlan:
        self._record("preview_scope", request)
        existing = self.preview_ids.get(request.idempotency_id)
        if existing is not None:
            original = ScopePreviewRequest.model_validate({
                field: getattr(existing, field) for field in ScopePreviewRequest.model_fields
            })
            if original != request:
                raise MonitoringConflict("Different dry-run intent")
            return existing
        self._expected(request.expected)
        plan_id = str(uuid5(NAMESPACE_URL, f"monitoring-preview:{request.idempotency_id}"))
        result = self.make_plan(request, plan_id=plan_id)
        self.plans[plan_id] = result
        self.preview_ids[request.idempotency_id] = result
        self.mutations.append(("preview_scope", request))
        return result

    def get_plan(self, context: MonitoringContext, plan_id: CanonicalId) -> ActivationPlan | None:
        self._record("get_plan", (context, plan_id))
        self._context(context)
        return self.plans.get(plan_id)

    def activate_scope(self, request: ActivateScopeRequest) -> ActivationReceipt:
        self._record("activate_scope", request)
        if existing := self.activations.get(request.idempotency_id):
            if existing[0] != request:
                raise MonitoringConflict("Different activation intent")
            return existing[1]
        self._expected(request.expected)
        plan = self.plans.get(request.plan_id)
        if (
            plan is None or plan.expected != request.expected
            or plan.status != "ready" or plan.expires_at <= NOW
        ):
            raise MonitoringConflict("The plan is absent, blocked, expired or stale")
        self.control = DeploymentControl.model_validate({
            **self.control.model_dump(), "revision": self.control.revision + 1,
        })
        self.scope = ScopePolicy(
            **plan.scope.model_dump(), revision=self.version.revision, updated_at=NOW,
        )
        receipt = ActivationReceipt(
            plan_id=plan.plan_id, idempotency_id=request.idempotency_id, version=self.version,
            scope=self.scope, activated_at=NOW, state="configuring", queued_work_ids=(OTHER,),
        )
        self.activations[request.idempotency_id] = (request, receipt)
        self.mutations.append(("activate_scope", request))
        return receipt

    def get_activation(
        self, context: MonitoringContext, idempotency_id: CanonicalId,
    ) -> ActivationReceipt | None:
        self._record("get_activation", (context, idempotency_id))
        self._context(context)
        result = self.activations.get(idempotency_id)
        return result[1] if result else None

    def request_discovery(
        self, expected: RegistryVersion, selector: ScopeSelector, *, request_id: CanonicalId,
    ) -> MonitoringWork:
        self._record("request_discovery", (expected, selector, request_id))
        self._context(expected)
        if existing := self.discoveries.get(request_id):
            if existing[:2] != (expected, selector):
                raise MonitoringConflict("Different discovery intent")
            return existing[2]
        self._expected(expected)
        result = MonitoringWork(
            tenant_id=expected.tenant_id, epoch=expected.epoch, work_id=request_id,
            kind="inventory", policy_revision=expected.revision,
            discovery_selector=selector, created_at=self.discovery_time, due_at=self.discovery_time,
            reason="Explicit discovery request; no active scope is required.",
            revision=0, state="queued",
        )
        self.discoveries[request_id] = (expected, selector, result)
        self.works[request_id] = result
        self.mutations.append(("request_discovery", (expected, selector, request_id)))
        return result

    def record_safety_review(self, request: SafetyReviewRequest) -> SafetyReview:
        self._record("record_safety_review", request)
        if existing := self.review_operations.get(request.request_id):
            if self.review_intents[request.request_id] != request:
                raise MonitoringConflict("Different review intent")
            return existing.review
        self._expected(request.expected)
        review = SafetyReview.model_validate({
            **request.review.model_dump(), "state": "pending", "publication_status": "pending_validation",
            "requested_state": (
                request.review.requested_state
                if request.review.publication_status == "pending_validation" else request.review.state
            ),
            "exact_correlation_verified": False, "revoked_at": None, "reviewed_at": NOW,
        })
        self.control = DeploymentControl.model_validate({
            **self.control.model_dump(), "revision": self.control.revision + 1,
        })
        self.mutations.append(("record_safety_review", request))
        self.reviews[review.review_id] = review
        self.review_intents[request.request_id] = request
        self.review_operations[request.request_id] = SafetyReviewOperationReceipt(
            request_id=request.request_id, target=review.target, action=review.action,
            expected=request.expected, expected_review_revision=request.expected_review_revision,
            new_review_revision=review.revision, fingerprint=HASH, recorded_at=NOW, review=review,
            requested_state=review.requested_state, publication_status=review.publication_status,
        )
        return review

    def get_safety_review(
        self, context: MonitoringContext, review_id: CanonicalId,
    ) -> SafetyReview | None:
        self._record("get_safety_review", (context, review_id))
        self._context(context)
        review = self.reviews.get(review_id)
        if review is None or (
            review.target.tenant_id, review.target.epoch
        ) != (context.tenant_id, context.epoch):
            return None
        return review

    def get_safety_review_operation(
        self, context: MonitoringContext, request_id: CanonicalId,
    ) -> SafetyReviewOperationReceipt | None:
        self._record("get_safety_review_operation", (context, request_id))
        self._context(context)
        receipt = self.review_operations.get(request_id)
        if receipt is not None and (
            receipt.target.tenant_id, receipt.target.epoch
        ) != (context.tenant_id, context.epoch):
            return None
        return receipt


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Monitoring HTTP tests must remain offline")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(OpenerDirector, "open", forbidden)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", forbidden)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)


@pytest.fixture(scope="module")
def signing():
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    class Keys:
        def get_signing_key_from_jwt(self, _token):
            return SimpleNamespace(key=private.public_key())

    web = WebSettings(_env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT)
    verifier = EntraTokenVerifier(web, signing_keys=Keys())

    def headers(app_roles=("reader",), **changes):
        now = int(time.time())
        claims = {
            "iss": f"https://login.microsoftonline.com/{TENANT}/v2.0",
            "aud": CLIENT, "tid": TENANT, "oid": USER, "exp": now + 300,
            "iat": now - 5, "nbf": now - 5, "scp": "access_as_user", "name": "Test operator",
            "roles": [f"CommandCenter.{role.title()}" for role in app_roles],
        } | changes
        token = jwt.encode(claims, private, algorithm="RS256")
        return {"Authorization": "Bearer " + token}

    return verifier, headers


@pytest.fixture
def store() -> RecordingStore:
    return RecordingStore()


@pytest.fixture
def service(store) -> MonitoringService:
    return MonitoringService(store.port, tenant_id=TENANT)


@pytest.fixture
def client(service, signing, tmp_path):
    verifier, _ = signing
    runtime = SimpleNamespace(monitoring=service, demo_tasks=[])
    app = create_app(
        runtime, token_verifier=verifier,
        web_settings=WebSettings(
            _env_file=None, mode="live", tenant_id=TENANT, client_id=CLIENT,
            static_dir=str(tmp_path / "not-built"),
        ),
    )
    # Isolate the exported router while retaining the application's actual
    # authentication, request boundary and exception handlers.
    app.router.routes.clear()
    app.include_router(create_monitoring_router(authenticated_actor))

    @app.middleware("http")
    async def remember_api_thread(request, call_next):
        app.state.api_thread = threading.get_ident()
        return await call_next(request)

    with TestClient(app, raise_server_exceptions=False) as result:
        yield result


def request_for(store: RecordingStore, operation: str) -> tuple[str, str, dict | None]:
    if operation == "plan":
        return "GET", f"{PREFIX}/plans/{PLAN}", None
    if operation == "activation":
        return "GET", f"{PREFIX}/activations/{OPERATION}", None
    if operation == "safety_review":
        return "GET", f"{PREFIX}/safety-reviews/{REVIEW}", None
    if operation == "safety_review_operation":
        return "GET", f"{PREFIX}/safety-review-operations/{OPERATION}", None
    if operation in READS:
        return "GET", f"{PREFIX}/{operation}", None
    if operation == "preview":
        return "POST", f"{PREFIX}/scopes/preview", store.preview_input().model_dump(mode="json")
    if operation == "activate":
        return "POST", f"{PREFIX}/plans/{PLAN}/activate", {
            "expected": store.version.model_dump(mode="json"), "idempotency_id": OPERATION,
        }
    if operation == "refresh":
        return "POST", f"{PREFIX}/inventory/refresh", {
            "expected": store.version.model_dump(mode="json"), "idempotency_id": OPERATION,
            "selector": {"tenant_id": TENANT, "kind": "tenant"},
        }
    if operation == "review":
        return "POST", f"{PREFIX}/safety-reviews", store.review_input().model_dump(mode="json")
    raise AssertionError(f"Unrecognized test operation: {operation}")


def send(client, store, signing, operation: str, *, roles=("reader",), body=None):
    method, path, original = request_for(store, operation)
    return client.request(
        method, path, headers=signing[1](roles), json=body if body is not None else original,
    )


@pytest.mark.parametrize("operation", READS)
def test_reader_can_read_without_mutating(client, store, signing, operation):
    if operation == "activation":
        store.activate_scope(ActivateScopeRequest(
            expected=store.version, plan_id=PLAN, idempotency_id=OPERATION,
        ))
    elif operation == "safety_review":
        store.reviews[REVIEW] = store.review_input().review
    elif operation == "safety_review_operation":
        store.record_safety_review(store.review_input())
    before = list(store.mutations)
    response = send(client, store, signing, operation)
    assert response.status_code == 200, response.text
    assert store.mutations == before
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("operation", READS + MUTATIONS)
def test_forged_actor_headers_without_a_bearer_token_do_not_reach_state(client, store, operation):
    method, path, body = request_for(store, operation)
    response = client.request(method, path, json=body, headers={
        "X-MS-CLIENT-PRINCIPAL": '{"roles":["admin"],"oid":"forged"}',
        "X-Actor-Id": OTHER, "X-Role": "admin",
    })
    assert response.status_code == 401
    assert store.calls == store.mutations == []


@pytest.mark.parametrize("role", ["reader", "operator", "approver"])
@pytest.mark.parametrize("operation", MUTATIONS)
def test_only_admin_can_mutate_monitoring(client, store, signing, operation, role):
    response = send(client, store, signing, operation, roles=(role,))
    assert response.status_code == 403
    assert response.json()["code"] == "forbidden"
    assert store.calls == store.mutations == []


@pytest.mark.parametrize("claims,status", [
    ({"idtyp": "app"}, 403),
    ({"scp": ""}, 403),
    ({"scp": "Fabric.Read.All"}, 403),
    ({"tid": OTHER}, 403),
    ({"aud": OTHER}, 401),
    ({"exp": 1}, 401),
    ({"roles": [], "groups": ["CommandCenter.Admin"], "hasgroups": True}, 403),
])
def test_managed_identity_or_wrong_human_tokens_do_not_authorize(
    client, store, signing, claims, status,
):
    response = client.get(f"{PREFIX}/snapshot", headers=signing[1](("admin",), **claims))
    assert response.status_code == status
    assert store.calls == []


@pytest.mark.parametrize("method,args,kwargs", [
    ("bootstrap", (), {}),
    ("snapshot", (), {}),
    ("list_scopes", (), {}),
    ("list_inventory", (), {}),
    ("list_targets", (), {}),
    ("command_targets", (), {}),
    ("list_workspaces", (), {}),
    ("list_domains", (), {}),
    ("list_connectors", (), {}),
    ("get_plan", (PLAN,), {}),
    ("get_activation", (OPERATION,), {}),
    ("get_safety_review", (REVIEW,), {}),
    ("get_safety_review_operation", (OPERATION,), {}),
])
def test_service_reads_require_a_role_even_without_http(service, store, method, args, kwargs):
    actor = Actor(id=USER, display_name="No role", roles=[])
    with pytest.raises(ApiFailure) as failure:
        getattr(service, method)(*args, actor, **kwargs)
    assert failure.value.status == 403
    assert store.calls == []


@pytest.mark.parametrize("operation", MUTATIONS)
def test_service_mutations_require_admin_even_without_http(service, store, operation):
    actor = Actor(id=USER, display_name="Operator", roles=["operator"])
    with pytest.raises(ApiFailure) as failure:
        if operation == "preview":
            service.preview_scope(store.preview_input(), actor)
        elif operation == "activate":
            service.activate_scope(PLAN, ScopeActivationInput(
                expected=store.version, idempotency_id=OPERATION,
            ), actor)
        elif operation == "refresh":
            service.refresh_inventory(InventoryRefreshInput(
                expected=store.version, idempotency_id=OPERATION,
                selector=ScopeSelector(tenant_id=TENANT, kind="tenant"),
            ), actor)
        else:
            service.record_safety_review(store.review_input(), actor)
    assert failure.value.status == 403
    assert store.calls == []


@pytest.mark.parametrize("operation", MUTATIONS)
@pytest.mark.parametrize("field,status", [("tenant_id", 422), ("epoch", 409)])
def test_wrong_request_context_is_refused_before_a_mutating_store_call(
    client, store, signing, operation, field, status,
):
    _, _, body = request_for(store, operation)
    body["expected"][field] = OTHER
    if operation == "preview":
        body["scope"][field] = OTHER
        if field == "tenant_id":
            for rule in body["scope"]["rules"]:
                rule["selector"]["tenant_id"] = OTHER
    elif operation == "refresh" and field == "tenant_id":
        body["selector"]["tenant_id"] = OTHER
    elif operation == "review":
        body["review"]["target"][field] = OTHER
    response = send(client, store, signing, operation, roles=("admin",), body=body)
    assert response.status_code == status, response.text
    assert store.mutations == []
    assert not any(method in MUTATION_METHODS for method, _ in store.calls)
    assert all(value == TENANT for method, value in store.calls if method == "inspect_bootstrap")


@pytest.mark.parametrize("operation", MUTATIONS)
@pytest.mark.parametrize("change", ["unknown_field", "boolean_revision", "malformed_id"])
def test_invalid_mutations_use_safe_request_errors(client, store, signing, operation, change):
    _, _, body = request_for(store, operation)
    marker = "not-a-valid-identifier-do-not-echo"
    if change == "unknown_field":
        body["private_input_do_not_echo"] = marker
    elif change == "boolean_revision":
        body["expected"]["revision"] = True
    else:
        body["request_id" if operation == "review" else "idempotency_id"] = marker
    response = send(client, store, signing, operation, roles=("admin",), body=body)
    assert response.status_code == 422
    assert response.json() == {
        "code": "invalid_request",
        "message": "The request does not match the expected fields or values.",
    }
    assert marker not in response.text and "private_input_do_not_echo" not in response.text
    assert store.mutations == []


@pytest.mark.parametrize("operation", MUTATIONS)
def test_current_revision_is_enforced_without_changing_the_submitted_version(
    client, store, signing, operation,
):
    _, _, body = request_for(store, operation)
    old_revision = body["expected"]["revision"]
    store.control = DeploymentControl.model_validate({
        **store.control.model_dump(), "revision": old_revision + 1,
    })
    response = send(client, store, signing, operation, roles=("admin",), body=body)
    assert response.status_code == 409
    assert store.mutations == []
    for method, value in store.calls:
        if method in {"preview_scope", "activate_scope", "record_safety_review"}:
            assert value.expected.revision == old_revision
        elif method == "request_discovery":
            assert value[0].revision == old_revision


@pytest.mark.parametrize("operation,method", [
    ("preview", "preview_scope"), ("activate", "activate_scope"),
    ("refresh", "request_discovery"), ("review", "record_safety_review"),
])
def test_store_arbitration_still_wins_when_revision_changes_after_inspection(
    client, store, signing, operation, method,
):
    original = getattr(store, method)

    def changed_before_commit(*args, **kwargs):
        store.control = DeploymentControl.model_validate({
            **store.control.model_dump(), "revision": store.control.revision + 1,
        })
        return original(*args, **kwargs)

    getattr(store.port, method).side_effect = changed_before_commit
    response = send(client, store, signing, operation, roles=("admin",))
    assert response.status_code == 409
    assert store.mutations == []
    assert getattr(store.port, method).call_count == 1


def test_each_read_inspects_current_server_context_instead_of_caching_or_using_query_tenant(
    client, store, signing,
):
    first = client.get(
        f"{PREFIX}/inventory", params={"tenant_id": OTHER, "epoch": OTHER},
        headers=signing[1](),
    )
    store.control = DeploymentControl.model_validate({
        **store.control.model_dump(), "epoch": OTHER,
    })
    second = client.get(f"{PREFIX}/inventory", headers=signing[1]())
    assert first.status_code == second.status_code == 200
    queries = [value for method, value in store.calls if method == "list_inventory"]
    assert [(query.tenant_id, query.epoch) for query in queries] == [(TENANT, EPOCH), (TENANT, OTHER)]
    assert store.port.inspect_bootstrap.call_count == 2


def test_preview_author_is_taken_from_the_validated_actor_and_replay_is_stable(
    client, store, signing,
):
    body = store.preview_input().model_dump(mode="json") | {"requested_by": OTHER}
    first = send(client, store, signing, "preview", roles=("admin",), body=body)
    replay = send(client, store, signing, "preview", roles=("admin",), body=body | {"requested_by": USER})
    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.json()["requested_by"] == USER
    assert first.json()["expected"] == body["expected"]
    assert len(store.mutations) == 1
    assert store.mutations[0][1].requested_by == USER
    assert set(method for method, _ in store.calls) == {"inspect_bootstrap", "preview_scope"}


@pytest.mark.parametrize("field,value", [
    ("roles", ["admin"]), ("actor", {"id": OTHER, "roles": ["admin"]}),
    ("tenant_id", OTHER), ("grant_permissions", True),
])
def test_preview_cannot_supply_an_actor_or_grants(client, store, signing, field, value):
    body = store.preview_input().model_dump(mode="json") | {field: value}
    response = send(client, store, signing, "preview", roles=("admin",), body=body)
    assert response.status_code == 422
    assert store.mutations == []


@pytest.mark.parametrize("role,can_admin", [
    ("reader", False), ("operator", False), ("approver", False), ("admin", True),
])
def test_snapshot_retains_partial_coverage_and_only_projects_token_roles(
    client, store, signing, role, can_admin,
):
    response = send(client, store, signing, "snapshot", roles=(role,))
    assert response.status_code == 200
    result = response.json()
    assert set(result) == {"control", "coverage", "can_admin", "user"}
    assert result["can_admin"] is can_admin
    assert result["user"] == {
        "id": USER, "display_name": "Test operator", "roles": sorted({role, "reader"}),
    }
    assert result["coverage"]["inventory_completeness"] == "partial"
    assert result["coverage"]["capability_completeness"] == "unknown"
    assert result["coverage"]["scope_item_count"] is None
    assert result["coverage"]["checkpoint_lag_seconds"] is None
    assert result["coverage"]["current_count"] == result["coverage"]["action_enabled_count"] == 0
    assert result["coverage"]["backlog_count"] == 2
    assert result["coverage"]["gaps"] == [store.gap.model_dump(mode="json")]


@pytest.mark.parametrize("path,method", [
    ("scopes", "list_scopes"), ("workspaces", "list_workspaces"),
    ("domains", "list_domains"), ("connectors", "list_connectors"),
])
def test_metadata_pages_preserve_model_json_and_pagination(client, store, signing, path, method):
    response = client.get(
        f"{PREFIX}/{path}", params={"limit": 2, "cursor": "opaque:previous"},
        headers=signing[1](),
    )
    assert response.status_code == 200
    query = next(value for name, value in store.calls if name == method)
    assert query == PageQuery(tenant_id=TENANT, epoch=EPOCH, limit=2, cursor="opaque:previous")
    result = response.json()
    assert set(result) == {"version", "as_of", "items", "next_cursor"}
    assert result["version"] == store.version.model_dump(mode="json")
    assert all("key" not in item for item in result["items"])
    if path == "workspaces":
        assert result["items"][0]["workspace_id"] == WORKSPACE
        assert result["items"][0]["domain_id"] == DOMAIN
        assert result["items"][0]["capacity_id"] == OTHER
    elif path == "domains":
        assert result["items"][0]["domain_id"] == DOMAIN
        assert result["items"][0]["parent_domain_id"] == OTHER
        assert result["items"][0]["state"] == "unknown"
    elif path == "connectors":
        assert result["items"][0]["state"] == "blocked"
        assert result["items"][0]["endpoint"] is None
        assert result["items"][0]["desired_definition"]["part-9"] == "x" * 800
        assert len(response.content) > 4_000


@pytest.mark.parametrize("path,method,workload", [
    ("inventory", "list_inventory", "powerbi"),
    ("targets", "list_targets", "fabric_pipeline"),
])
def test_target_queries_use_typed_server_context_and_wire_identity(
    client, store, signing, path, method, workload,
):
    params = {"workload": workload, "workspace_id": WORKSPACE, "limit": 3, "cursor": "next:page"}
    if path == "targets":
        params["include_inactive"] = "true"
    response = client.get(f"{PREFIX}/{path}", params=params, headers=signing[1]())
    assert response.status_code == 200
    query = next(value for name, value in store.calls if name == method)
    assert query == TargetQuery(
        tenant_id=TENANT, epoch=EPOCH, workload=workload, workspace_id=WORKSPACE,
        limit=3, cursor="next:page", include_inactive=path == "targets",
    )
    item = response.json()["items"][0]
    assert "key" not in item
    if path == "targets":
        assert item["identity"] == store.target.model_dump(mode="json")
        assert "key" not in item["identity"]
        assert item["state"] == "paused"
        assert item["action"]["enabled"] is item["observation"]["enabled"] is False
    else:
        assert item["workspace_id"] == WORKSPACE and item["item_id"] == ITEM
        assert "target" not in item


def test_inventory_exposes_supported_detector_types_without_inventing_admission(client, store, signing):
    response = send(client, store, signing, "inventory")
    rows = response.json()["items"]
    assert {(row["item_type"], row["workload"]) for row in rows} == {
        ("SemanticModel", "powerbi"), ("DataPipeline", "fabric_pipeline"),
    }
    assert all(row["unsupported_reason"] is None for row in rows)
    assert rows[1]["state"] == "unknown"
    assert all("action" not in row and "observation" not in row for row in rows)


@pytest.mark.parametrize("path,params", [
    ("inventory", {"workload": "Notebook"}),
    ("inventory", {"workspace_id": "invalid-private-value"}),
    ("scopes", {"limit": 0}), ("scopes", {"limit": 1001}),
    ("domains", {"limit": "invalid-private-value"}),
    ("workspaces", {"cursor": ""}), ("connectors", {"cursor": "x" * 4097}),
    ("inventory", {"cursor": "hidden\nvalue"}),
    ("targets", {"include_inactive": "invalid-private-value"}),
])
def test_invalid_read_filters_do_not_echo_request_data(client, store, signing, path, params):
    response = client.get(f"{PREFIX}/{path}", params=params, headers=signing[1]())
    assert response.status_code == 422
    assert response.json()["code"] == "invalid_request"
    assert "invalid-private-value" not in response.text and "hidden" not in response.text
    assert store.mutations == []


@pytest.mark.parametrize("operation", MUTATIONS)
def test_maintenance_reads_remain_visible_but_mutations_are_refused(
    client, store, signing, operation,
):
    store.control = DeploymentControl.model_validate({
        **store.control.model_dump(), "maintenance": True,
    })
    bootstrap = send(client, store, signing, "bootstrap")
    snapshot = send(client, store, signing, "snapshot")
    response = send(client, store, signing, operation, roles=("admin",))
    assert bootstrap.status_code == snapshot.status_code == 200
    assert bootstrap.json()["status"] == "maintenance"
    assert snapshot.json()["control"]["maintenance"] is True
    assert response.status_code == 503 and response.json()["code"] == "monitoring_maintenance"
    assert store.mutations == []


def nonready_inspection(store: RecordingStore, status: str) -> BootstrapInspection:
    extra = {}
    if status == "incompatible":
        extra["found_schema_version"] = 2
    elif status == "wrong_tenant":
        extra = {
            "found_schema_version": 1,
            "control": DeploymentControl.model_validate({
                **store.control.model_dump(), "tenant_id": OTHER,
            }),
        }
    return BootstrapInspection(
        status=status, expected_tenant_id=TENANT, detail="Private diagnostic not for the response.",
        **extra,
    )


@pytest.mark.parametrize("status", ["missing", "incompatible", "wrong_tenant"])
def test_bootstrap_returns_safe_nonready_diagnoses(client, store, signing, status):
    store.inspection = nonready_inspection(store, status)
    response = send(client, store, signing, "bootstrap")
    assert response.status_code == 200
    body = response.json()
    assert BootstrapResponse.model_validate(body).status == status
    assert set(body) == {
        "status", "expected_tenant_id", "found_schema_version", "control", "detail", "missing_operations",
    }
    assert body["missing_operations"] == []
    assert body["expected_tenant_id"] == TENANT
    assert body["control"] is None
    assert body["found_schema_version"] == (2 if status == "incompatible" else None)
    assert "Private diagnostic" not in response.text and OTHER not in response.text
    assert store.mutations == []
    assert [method for method, _ in store.calls] == ["inspect_bootstrap"]


@pytest.mark.parametrize("operation", [value for value in READS if value != "bootstrap"] + list(MUTATIONS))
def test_kernel_incomplete_is_readable_but_cannot_admit_api_mutations(
    client, store, signing, operation,
):
    store.inspection = BootstrapInspection(
        status="kernel_incomplete", expected_tenant_id=TENANT, found_schema_version=1,
        control=store.control, missing_operations=("partition_catalogue",),
        detail="Required guarded runtime operations are not yet available.",
    )
    response = send(client, store, signing, "bootstrap")
    assert response.status_code == 200
    assert response.json()["status"] == "kernel_incomplete"
    assert response.json()["missing_operations"] == ["partition_catalogue"]
    blocked = send(client, store, signing, operation, roles=("admin",))
    assert blocked.status_code == 503
    assert blocked.json()["code"] == "monitoring_kernel_incomplete"
    assert store.mutations == []
    assert {method for method, _ in store.calls} == {"inspect_bootstrap"}


@pytest.mark.parametrize("status,code", [
    ("missing", "monitoring_not_bootstrapped"),
    ("incompatible", "monitoring_schema_mismatch"),
    ("wrong_tenant", "monitoring_bootstrap_mismatch"),
])
@pytest.mark.parametrize("operation", [
    "snapshot", "safety_review", "safety_review_operation", *MUTATIONS,
])
def test_nonready_bootstrap_still_blocks_snapshots_and_mutations(
    client, store, signing, status, code, operation,
):
    store.inspection = nonready_inspection(store, status)
    response = send(client, store, signing, operation, roles=("admin",))
    assert response.status_code == 503
    assert response.json()["code"] == code
    assert set(response.json()) == {"code", "message"}
    assert "Private diagnostic" not in response.text
    assert store.port.snapshot.call_count == 0
    assert store.mutations == []
    assert not any(method in MUTATION_METHODS for method, _ in store.calls)


def test_bootstrap_partial_schema_is_a_missing_diagnosis_not_a_healthy_result(client, store, signing):
    store.failures["inspect_bootstrap"] = MonitoringNotBootstrapped("Private missing table names")
    response = send(client, store, signing, "bootstrap")
    assert response.status_code == 200
    assert response.json()["status"] == "missing"
    assert response.json()["control"] is None
    assert "Private" not in response.text and store.mutations == []


def test_bootstrap_unreachable_backend_remains_an_explicit_503(client, store, signing):
    store.failures["inspect_bootstrap"] = MonitoringUnavailable("Private connection details")
    response = send(client, store, signing, "bootstrap")
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_unavailable"
    assert set(response.json()) == {"code", "message"}
    assert "Private" not in response.text and store.mutations == []


def test_wrong_tenant_response_model_rejects_foreign_metadata(store):
    with pytest.raises(ValidationError):
        BootstrapResponse.model_validate(nonready_inspection(store, "wrong_tenant").model_dump())


@pytest.mark.parametrize("error,status,code", [
    (MonitoringUnavailable("private driver error"), 503, "monitoring_unavailable"),
    (MonitoringNotBootstrapped("private schema details"), 503, "monitoring_not_bootstrapped"),
    (MonitoringSchemaMismatch("private schema details"), 503, "monitoring_schema_mismatch"),
    (MonitoringKernelUnsupported("private kernel details"), 503, "monitoring_kernel_incomplete"),
    (MonitoringComponentDenied("private component details"), 503, "monitoring_component_mismatch"),
    (MonitoringStoreError("private state failure"), 503, "monitoring_unavailable"),
    (MonitoringConflict("private mismatch"), 409, "monitoring_conflict"),
    (MonitoringLeaseLost("private lease"), 409, "monitoring_lease_lost"),
    (PermissionError("private access failure"), 403, "forbidden"),
])
@pytest.mark.parametrize("operation", ["snapshot", "safety_review", "safety_review_operation"])
def test_typed_store_failures_never_become_empty_or_healthy_results(
    client, store, signing, caplog, error, status, code, operation,
):
    store.failures["snapshot" if operation == "snapshot" else f"get_{operation}"] = error
    response = send(client, store, signing, operation)
    assert response.status_code == status
    assert response.json()["code"] == code
    assert set(response.json()) == {"code", "message"}
    assert str(error) not in response.text and str(error) not in caplog.text


@pytest.mark.parametrize("path", [
    f"plans/{OTHER}", f"activations/{OTHER}", f"safety-reviews/{OTHER}",
    f"safety-review-operations/{OTHER}",
])
def test_missing_plan_receipt_or_review_is_not_found(client, store, signing, path):
    response = client.get(f"{PREFIX}/{path}", headers=signing[1]())
    assert response.status_code == 404 and response.json()["code"] == "not_found"
    assert store.mutations == []


@pytest.mark.parametrize("path", [
    "plans/not-an-id", "activations/not-an-id", "safety-reviews/not-an-id",
    "safety-review-operations/not-an-id",
])
def test_invalid_receipt_paths_use_safe_validation_errors(client, signing, path):
    response = client.get(f"{PREFIX}/{path}", headers=signing[1]())
    assert response.status_code == 422 and response.json()["code"] == "invalid_request"
    assert "not-an-id" not in response.text


def test_activation_preserves_expected_revision_idempotency_and_path_binding(client, store, signing):
    _, path, body = request_for(store, "activate")
    first = client.post(path, json=body, headers=signing[1](("admin",)))
    replay = client.post(path, json=body, headers=signing[1](("admin",)))
    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.json()["version"]["revision"] == body["expected"]["revision"] + 1
    assert first.json()["plan_id"] == PLAN and first.json()["idempotency_id"] == OPERATION
    assert first.json()["state"] == "configuring"
    assert first.json()["queued_work_ids"] == [OTHER]
    assert len(store.mutations) == 1
    assert store.mutations[0][1].expected.revision == body["expected"]["revision"]
    assert send(client, store, signing, "activation").json() == first.json()

    store.plans[OTHER] = ActivationPlan.model_validate({
        **store.plans[PLAN].model_dump(), "plan_id": OTHER,
    })
    wrong_path = client.post(
        f"{PREFIX}/plans/{OTHER}/activate", json=body, headers=signing[1](("admin",)),
    )
    spoofed_path = client.post(
        path, json=body | {"plan_id": OTHER}, headers=signing[1](("admin",)),
    )
    changed_revision = client.post(
        path, json=body | {"expected": store.version.model_dump(mode="json")},
        headers=signing[1](("admin",)),
    )
    assert wrong_path.status_code == changed_revision.status_code == 409
    assert spoofed_path.status_code == 422
    assert len(store.mutations) == 1


@pytest.mark.parametrize("condition", ["missing", "expired", "blocked"])
def test_activation_does_not_bypass_the_stored_plan(client, store, signing, condition):
    if condition == "missing":
        store.plans.clear()
    elif condition == "expired":
        store.plans[PLAN] = ActivationPlan.model_validate({
            **store.plans[PLAN].model_dump(),
            "created_at": NOW - timedelta(hours=1), "expires_at": NOW - timedelta(minutes=1),
        })
    else:
        store.plans[PLAN] = ActivationPlan.model_validate({
            **store.plans[PLAN].model_dump(), "status": "blocked", "gaps": (store.gap,),
        })
    response = send(client, store, signing, "activate", roles=("admin",))
    assert response.status_code == (404 if condition == "missing" else 409)
    assert store.mutations == []


def test_blocked_dry_run_reports_its_gaps_without_authorizing_activation(client, store, signing):
    store.preview_blocked = True
    response = send(client, store, signing, "preview", roles=("admin",))
    assert response.status_code == 200
    assert response.json()["status"] == "blocked"
    assert response.json()["inventory_completeness"] == "partial"
    assert response.json()["gaps"] == [store.gap.model_dump(mode="json")]
    assert store.port.activate_scope.call_count == store.port.request_discovery.call_count == 0


@pytest.mark.parametrize("operation,method", [
    ("preview", "preview_scope"), ("activate", "activate_scope"),
    ("refresh", "request_discovery"), ("review", "record_safety_review"),
])
def test_uncertain_mutations_preserve_receipt_id_without_reporting_success_or_retrying(
    client, store, signing, caplog, operation, method,
):
    store.failures[method] = MonitoringCommitUncertain("private-operation-details", OPERATION)
    response = send(client, store, signing, operation, roles=("admin",))
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_commit_uncertain"
    assert OPERATION in response.json()["message"]
    assert "Do not retry" in response.json()["message"]
    assert "private-operation-details" not in response.text + caplog.text
    assert set(response.json()) == {"code", "message"}
    assert getattr(store.port, method).call_count == 1
    if operation == "refresh":
        assert store.port.get_work.call_count == 0
        assert not hasattr(store.port, "enqueue_work")


def test_uncertain_activation_can_be_reconciled_by_its_original_receipt(client, store, signing):
    def committed_without_acknowledgement(request: ActivateScopeRequest) -> ActivationReceipt:
        store.activate_scope(request)
        raise MonitoringCommitUncertain("activation", request.idempotency_id)

    store.port.activate_scope.side_effect = committed_without_acknowledgement
    response = send(client, store, signing, "activate", roles=("admin",))
    assert response.status_code == 503
    reconciled = send(client, store, signing, "activation")
    assert reconciled.status_code == 200
    assert reconciled.json()["idempotency_id"] == OPERATION
    assert reconciled.json()["state"] == "configuring"
    assert store.port.activate_scope.call_count == len(store.mutations) == 1


def test_an_invalid_uncertain_receipt_identifier_is_not_echoed(client, store, signing):
    marker = "invalid-private-receipt\nvalue"
    store.failures["snapshot"] = MonitoringCommitUncertain("private", marker)
    response = send(client, store, signing, "snapshot")
    assert response.status_code == 503
    assert marker not in response.text and "invalid-private-receipt" not in response.text
    assert "receipt unavailable" in response.json()["message"]


def test_inventory_refresh_queues_one_typed_intent_without_scanning_fabric(client, store, signing):
    _, _, body = request_for(store, "refresh")
    first = send(client, store, signing, "refresh", roles=("admin",), body=body)
    replay = send(client, store, signing, "refresh", roles=("admin",), body=body)
    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json() == {"work_id": OPERATION, "status": "queued"}
    work = store.works[OPERATION]
    assert isinstance(work, MonitoringWork)
    assert (work.tenant_id, work.epoch, work.policy_revision) == (TENANT, EPOCH, 7)
    assert work.kind == "inventory" and work.discovery_selector == ScopeSelector(
        tenant_id=TENANT, kind="tenant",
    )
    assert work.created_at == work.due_at == store.discovery_time
    assert work.target is work.execution is work.action_reservation_id is None
    assert store.port.request_discovery.call_count == 2
    store.port.request_discovery.assert_called_with(
        RegistryVersion.model_validate(body["expected"]),
        ScopeSelector.model_validate(body["selector"]), request_id=OPERATION,
    )
    assert store.port.get_work.call_count == 0
    assert not hasattr(store.port, "enqueue_work")
    assert len(store.works) == len(store.mutations) == 1
    assert set(method for method, _ in store.calls) == {
        "inspect_bootstrap", "request_discovery",
    }


def test_inventory_idempotency_checks_selector_and_original_revision(client, store, signing):
    _, _, body = request_for(store, "refresh")
    assert send(client, store, signing, "refresh", roles=("admin",), body=body).status_code == 200
    changed = body | {"selector": {"tenant_id": TENANT, "kind": "workspace", "workspace_id": WORKSPACE}}
    assert send(client, store, signing, "refresh", roles=("admin",), body=changed).status_code == 409
    store.control = DeploymentControl.model_validate({
        **store.control.model_dump(), "revision": 8,
    })
    assert send(client, store, signing, "refresh", roles=("admin",), body=body).status_code == 200
    new_revision = body | {"expected": store.version.model_dump(mode="json")}
    assert send(client, store, signing, "refresh", roles=("admin",), body=new_revision).status_code == 409
    assert len(store.works) == len(store.mutations) == 1
    assert store.port.request_discovery.call_count == 4
    assert store.port.get_work.call_count == 0
    assert not hasattr(store.port, "enqueue_work")


@pytest.mark.parametrize("same_intent", [True, False])
def test_discovery_replay_is_decided_by_the_store_without_api_work_lookups(
    client, store, signing, same_intent,
):
    selector = ScopeSelector(tenant_id=TENANT, kind="tenant") if same_intent else ScopeSelector(
        tenant_id=TENANT, kind="workspace", workspace_id=WORKSPACE,
    )
    original = store.request_discovery(store.version, selector, request_id=OPERATION)
    store.discovery_time += timedelta(minutes=5)
    response = send(client, store, signing, "refresh", roles=("admin",))
    assert response.status_code == (200 if same_intent else 409)
    assert store.port.request_discovery.call_count == 1
    assert store.port.get_work.call_count == 0
    assert not hasattr(store.port, "enqueue_work")
    assert len(store.works) == len(store.mutations) == 1
    assert store.works[OPERATION] == original


def test_inventory_queue_commit_uncertainty_is_not_hidden_by_a_followup_read(client, store, signing):
    def committed(
        expected: RegistryVersion, selector: ScopeSelector, *, request_id: CanonicalId,
    ) -> MonitoringWork:
        store.request_discovery(expected, selector, request_id=request_id)
        raise MonitoringCommitUncertain("discovery", request_id)

    store.port.request_discovery.side_effect = committed
    response = send(client, store, signing, "refresh", roles=("admin",))
    assert response.status_code == 503 and OPERATION in response.json()["message"]
    assert store.port.request_discovery.call_count == 1
    assert store.port.get_work.call_count == 0
    assert not hasattr(store.port, "enqueue_work")
    assert OPERATION in store.works


@pytest.mark.parametrize("error,code", [
    (MonitoringLeaseLost("Ownership is no longer valid"), "monitoring_lease_lost"),
    (MonitoringConflict("Registry arbitration refused this request"), "monitoring_conflict"),
])
def test_discovery_conflict_is_not_reconciled_into_success(client, store, signing, error, code):
    def refused(
        expected: RegistryVersion, selector: ScopeSelector, *, request_id: CanonicalId,
    ) -> MonitoringWork:
        store.request_discovery(expected, selector, request_id=request_id)
        raise error

    store.port.request_discovery.side_effect = refused
    response = send(client, store, signing, "refresh", roles=("admin",))
    assert response.status_code == 409
    assert response.json()["code"] == code
    assert store.port.request_discovery.call_count == 1
    assert store.port.get_work.call_count == 0
    assert not hasattr(store.port, "enqueue_work")


@pytest.mark.parametrize("field,value", [
    ("kind", "triage"), ("target", {"item_id": OTHER}), ("discovery_selector", {"kind": "tenant"}),
    ("reviewer_id", OTHER), ("roles", ["admin"]),
])
def test_inventory_refresh_does_not_accept_worker_or_authorization_fields(
    client, store, signing, field, value,
):
    _, _, body = request_for(store, "refresh")
    response = send(client, store, signing, "refresh", roles=("admin",), body=body | {field: value})
    assert response.status_code == 422 and store.mutations == []


def test_discovery_selector_must_match_the_expected_deployment(client, store, signing):
    _, _, body = request_for(store, "refresh")
    body["selector"]["tenant_id"] = OTHER
    response = send(client, store, signing, "refresh", roles=("admin",), body=body)
    assert response.status_code == 422
    assert store.port.request_discovery.call_count == 0


def test_discovery_http_replay_uses_the_real_store_receipt_and_revision_gate(client, store, signing):
    from triage.monitoring.memory import InMemoryMonitoringState, InMemoryMonitoringStore

    clock = [NOW]
    state = InMemoryMonitoringState.empty(store.control)
    registry = InMemoryMonitoringStore(state=state, clock=lambda: clock[0])
    client.app.state.service.monitoring = MonitoringService(registry, tenant_id=TENANT)
    _, _, body = request_for(store, "refresh")
    first = send(client, store, signing, "refresh", roles=("admin",), body=body)
    assert first.status_code == 200, first.text
    clock[0] += timedelta(minutes=5)
    with state.lock:
        state.control_row = DeploymentControl.model_validate({
            **store.control.model_dump(), "revision": 8, "updated_at": clock[0],
        }).model_dump(mode="json")
    replay = send(client, store, signing, "refresh", roles=("admin",), body=body)
    assert replay.status_code == 200 and replay.json() == first.json()
    context = MonitoringContext(tenant_id=TENANT, epoch=EPOCH)
    queued = registry.get_work(context, OPERATION)
    assert queued is not None
    assert queued.created_at == NOW
    assert queued.discovery_selector == ScopeSelector.model_validate(body["selector"])
    changed = body | {
        "selector": {"tenant_id": TENANT, "kind": "workspace", "workspace_id": WORKSPACE},
    }
    assert send(client, store, signing, "refresh", roles=("admin",), body=changed).status_code == 409
    stale = body | {
        "idempotency_id": OTHER, "expected": body["expected"] | {"revision": 6},
    }
    assert send(client, store, signing, "refresh", roles=("admin",), body=stale).status_code == 409
    assert registry.get_work(context, OTHER) is None
    assert registry.get_work(context, OPERATION) == queued


def test_safety_review_reviewer_is_the_validated_actor_not_the_body(client, store, signing):
    response = send(client, store, signing, "review", roles=("admin",))
    assert response.status_code == 200
    assert response.json()["reviewer_id"] == USER
    assert response.json()["state"] == "pending"
    assert response.json()["requested_state"] == "pending"
    assert response.json()["publication_status"] == "pending_validation"
    assert response.json()["exact_correlation_verified"] is False
    assert store.mutations[0][1].review.reviewer_id == USER
    assert set(method for method, _ in store.calls) == {"inspect_bootstrap", "record_safety_review"}


@pytest.mark.parametrize("evidence", ["missing", "mismatched_definition", "expired", "verified"])
def test_client_attestation_remains_pending_even_when_read_proof_exists(
    client, store, signing, evidence,
):
    if evidence != "missing":
        store.capability = CapabilityObservation(
            capability_id=CAPABILITY, target=store.target, inventory_generation=GENERATION,
            collector_identity_id=CLIENT, read_status="verified", action_status="verified",
            exact_action_correlation=True, checked_at=NOW - timedelta(minutes=10),
            expires_at=NOW + timedelta(minutes=-1 if evidence == "expired" else 10),
            definition_hash="b" * 64 if evidence == "mismatched_definition" else HASH,
        )
    body = store.review_input(verified=True).model_dump(mode="json")
    response = send(client, store, signing, "review", roles=("admin",), body=body)
    assert response.status_code == 200
    assert response.json()["state"] == "pending"
    assert response.json()["requested_state"] == "verified"
    assert response.json()["publication_status"] == "pending_validation"
    assert response.json()["exact_correlation_verified"] is False
    assert len(store.mutations) == 1
    request = store.port.record_safety_review.call_args.args[0]
    assert request.review.reviewer_id == USER
    assert set(method for method, _ in store.calls) == {"inspect_bootstrap", "record_safety_review"}


def test_safety_review_returns_the_stores_redacted_pending_intent(client, store, signing):
    def redacted(request: SafetyReviewRequest) -> SafetyReview:
        return SafetyReview.model_validate({
            **request.review.model_dump(),
            "state": "pending", "parameters": None, "parameters_redacted": True,
            "requested_state": "verified", "publication_status": "pending_validation",
            "exact_correlation_verified": False,
            "detail": "Replay parameters are unavailable after store redaction.",
        })

    store.port.record_safety_review.side_effect = redacted
    body = store.review_input(verified=True).model_dump(mode="json")
    response = send(client, store, signing, "review", roles=("admin",), body=body)
    assert response.status_code == 200
    assert response.json()["state"] == "pending"
    assert response.json()["requested_state"] == "verified"
    assert response.json()["publication_status"] == "pending_validation"
    assert response.json()["parameters"] is None
    assert response.json()["parameters_redacted"] is True
    assert response.json()["parameter_hash"] == body["review"]["parameter_hash"]


def test_web_result_cannot_publish_or_default_old_review_metadata(client, store, signing):
    store.port.record_safety_review.side_effect = lambda request: request.review
    response = send(
        client, store, signing, "review", roles=("admin",),
        body=store.review_input(verified=True).model_dump(mode="json"),
    )
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_unavailable"
    assert "review" not in response.json()
    assert store.port.record_safety_review.call_count == 1


@pytest.mark.parametrize("maintenance", [False, True])
def test_reader_gets_the_redacted_persisted_review_without_rewriting_it(
    client, store, signing, maintenance,
):
    original = store.review_input(verified=True).review
    saved = SafetyReview.model_validate({
        **original.model_dump(), "state": "unverifiable",
        "parameters": None, "parameters_redacted": True,
        "detail": "Replay values are unavailable after store redaction.",
    })
    store.reviews[REVIEW] = saved
    store.control = DeploymentControl.model_validate({
        **store.control.model_dump(), "maintenance": maintenance,
    })
    response = send(client, store, signing, "safety_review")
    assert response.status_code == 200
    assert response.json() == saved.model_dump(mode="json")
    assert response.json()["reviewer_id"] == OTHER
    assert response.json()["parameter_hash"] == original.parameter_hash
    assert response.json()["parameters"] is None
    assert response.headers["Cache-Control"] == "no-store"
    assert store.reviews[REVIEW] == saved and store.mutations == []
    assert [method for method, _ in store.calls] == ["inspect_bootstrap", "get_safety_review"]


def test_safety_review_lookup_uses_current_server_context_and_does_not_cache_old_revisions(
    client, store, signing,
):
    saved = store.review_input().review
    store.reviews[REVIEW] = saved
    response = client.get(
        f"{PREFIX}/safety-reviews/{REVIEW.upper()}",
        params={"tenant_id": OTHER, "epoch": OTHER}, headers=signing[1](),
    )
    assert response.status_code == 200 and response.json()["revision"] == 1
    store.reviews[REVIEW] = SafetyReview.model_validate({
        **saved.model_dump(), "revision": 2, "detail": "A later persisted revision.",
    })
    refreshed = send(client, store, signing, "safety_review")
    assert refreshed.status_code == 200 and refreshed.json()["revision"] == 2
    context, review_id = store.port.get_safety_review.call_args.args
    assert context == MonitoringContext(tenant_id=TENANT, epoch=EPOCH)
    assert review_id == REVIEW
    store.control = DeploymentControl.model_validate({
        **store.control.model_dump(), "epoch": OTHER,
    })
    assert send(client, store, signing, "safety_review").status_code == 404
    assert store.mutations == []


def test_uncertain_review_save_can_be_inspected_without_repeating_the_write(client, store, signing):
    def committed_without_acknowledgement(request: SafetyReviewRequest) -> SafetyReview:
        store.record_safety_review(request)
        raise MonitoringCommitUncertain("safety review", request.request_id)

    store.port.record_safety_review.side_effect = committed_without_acknowledgement
    response = send(client, store, signing, "review", roles=("admin",))
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_commit_uncertain"
    assert OPERATION in response.json()["message"]
    saved = send(client, store, signing, "safety_review")
    assert saved.status_code == 200
    assert saved.json() == store.reviews[REVIEW].model_dump(mode="json")
    assert saved.json()["reviewer_id"] == USER
    assert saved.json()["state"] == "pending"
    assert store.port.record_safety_review.call_count == len(store.mutations) == 1


def test_missing_original_operation_is_not_forged_from_the_prewrite_review_after_reload(
    client, store, signing,
):
    store.reviews[REVIEW] = store.review_input().review
    current = send(client, store, signing, "safety_review")
    assert current.status_code == 200 and current.json()["revision"] == 1
    client.app.state.service.monitoring = MonitoringService(store.port, tenant_id=TENANT)
    response = send(client, store, signing, "safety_review_operation")
    assert response.status_code == 404
    assert response.json()["code"] == "not_found"
    assert "not been observed" in response.json()["message"]
    assert store.port.get_safety_review.call_count == 1
    assert store.port.record_safety_review.call_count == 0


def test_reader_can_reconcile_the_original_revocation_even_when_current_review_is_newer(
    client, store, signing,
):
    first = store.review_input()
    original = store.record_safety_review(first)
    revoked = SafetyReview.model_validate({
        **original.model_dump(), "state": "revoked", "revision": 2, "revoked_at": NOW,
        "policy_revision": store.version.revision, "requested_state": None,
        "publication_status": "published",
    })
    operation = SafetyReviewRequest(
        request_id=OTHER, expected=store.version, expected_review_revision=1, review=revoked,
    )
    store.record_safety_review(operation)
    receipt = store.review_operations[OTHER]
    store.reviews[REVIEW] = SafetyReview.model_validate({
        **original.model_dump(), "revision": 3, "publication_status": "published",
        "requested_state": None,
    })
    client.app.state.service.monitoring = MonitoringService(store.port, tenant_id=TENANT)
    response = client.get(
        f"{PREFIX}/safety-review-operations/{OTHER.upper()}",
        params={"tenant_id": OTHER, "epoch": OTHER}, headers=signing[1](),
    )
    assert response.status_code == 200
    assert response.json() == {
        "request_id": receipt.request_id, "review": receipt.review.model_dump(mode="json"),
    }
    assert set(response.json()) == {"request_id", "review"}
    assert receipt.expected_review_revision == 1 and receipt.new_review_revision == 2
    assert response.json()["review"]["revision"] == 2
    assert response.json()["review"]["state"] == "pending"
    assert response.json()["review"]["requested_state"] == "revoked"
    assert response.json()["review"]["publication_status"] == "pending_validation"
    assert send(client, store, signing, "safety_review").json()["revision"] == 3
    assert store.port.record_safety_review.call_count == 0
    context, request_id = store.port.get_safety_review_operation.call_args.args
    assert context == MonitoringContext(tenant_id=TENANT, epoch=EPOCH) and request_id == OTHER
    assert response.headers["Cache-Control"] == "no-store"


def test_operation_endpoint_returns_the_persisted_redacted_review_without_parameter_reconstruction(
    client, store, signing,
):
    request = store.review_input(verified=True)
    saved = SafetyReview.model_validate({
        **request.review.model_dump(), "parameters": None,
        "parameters_redacted": True, "state": "unverifiable",
        "detail": "Replay parameters are unavailable after persistence redaction.",
    })
    receipt = SafetyReviewOperationReceipt(
        request_id=OPERATION, target=saved.target, action=saved.action,
        expected=request.expected, expected_review_revision=0, new_review_revision=1,
        fingerprint=HASH, recorded_at=NOW, review=saved,
    )
    store.review_operations[OPERATION] = receipt
    response = send(client, store, signing, "safety_review_operation")
    assert response.status_code == 200
    assert response.json() == {
        "request_id": receipt.request_id, "review": receipt.review.model_dump(mode="json"),
    }
    assert set(response.json()) == {"request_id", "review"}
    assert response.json()["review"]["parameters"] is None
    assert response.json()["review"]["parameter_hash"] == request.review.parameter_hash
    assert store.port.get_safety_review.call_count == store.port.record_safety_review.call_count == 0


@pytest.mark.parametrize("mismatch", ["request_id", "context"])
def test_mismatched_original_receipt_fails_closed_without_a_current_review_fallback(
    client, store, signing, mismatch,
):
    store.record_safety_review(store.review_input())
    original = store.review_operations[OPERATION]
    changes = {"request_id": OTHER}
    if mismatch == "context":
        target = original.target.model_copy(update={"epoch": OTHER})
        changes = {
            "target": target,
            "expected": original.expected.model_copy(update={"epoch": OTHER}),
            "review": original.review.model_copy(update={"target": target}),
        }
    wrong = SafetyReviewOperationReceipt.model_validate(original.model_dump() | changes)
    store.port.get_safety_review_operation.side_effect = lambda *_args: wrong
    response = send(client, store, signing, "safety_review_operation")
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_unavailable"
    assert store.port.get_safety_review.call_count == store.port.record_safety_review.call_count == 0


@pytest.mark.parametrize("operation", READS + MUTATIONS)
def test_every_blocking_store_call_runs_off_the_api_event_loop(client, store, signing, operation):
    if operation == "activation":
        store.activate_scope(ActivateScopeRequest(
            expected=store.version, plan_id=PLAN, idempotency_id=OPERATION,
        ))
        store.threads.clear()
    elif operation == "safety_review":
        store.reviews[REVIEW] = store.review_input().review
    elif operation == "safety_review_operation":
        store.record_safety_review(store.review_input())
        store.threads.clear()
    response = send(client, store, signing, operation, roles=("admin",))
    assert response.status_code == 200, response.text
    assert store.threads
    assert all(thread != client.app.state.api_thread for thread in store.threads)


def test_missing_service_is_an_explicit_failure_without_a_fallback(client, store, signing):
    client.app.state.service.monitoring = None
    response = send(client, store, signing, "snapshot")
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_unavailable"
    assert store.calls == []


def test_constructor_and_router_do_not_bootstrap_or_add_permissions_or_execution_routes(store):
    service = MonitoringService(store.port, tenant_id=TENANT.upper())
    assert service.tenant_id == TENANT
    assert store.port.mock_calls == []
    router = create_monitoring_router(authenticated_actor)
    assert {(method, route.path) for route in router.routes for method in route.methods} == {
        ("GET", f"{PREFIX}/bootstrap"), ("GET", f"{PREFIX}/snapshot"),
        ("GET", f"{PREFIX}/scopes"), ("GET", f"{PREFIX}/inventory"),
        ("GET", f"{PREFIX}/targets"), ("GET", f"{PREFIX}/workspaces"),
        ("GET", f"{PREFIX}/domains"), ("GET", f"{PREFIX}/connectors"),
        ("GET", f"{PREFIX}/plans/{{plan_id}}"),
        ("GET", f"{PREFIX}/activations/{{idempotency_id}}"),
        ("GET", f"{PREFIX}/safety-reviews/{{review_id}}"),
        ("GET", f"{PREFIX}/safety-review-operations/{{request_id}}"),
        ("POST", f"{PREFIX}/scopes/preview"),
        ("POST", f"{PREFIX}/plans/{{plan_id}}/activate"),
        ("POST", f"{PREFIX}/inventory/refresh"), ("POST", f"{PREFIX}/safety-reviews"),
    }
    tree = ast.parse(Path(monitoring.__file__).read_text(encoding="utf-8"))
    imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    assert "triage.command_center.api" not in imports
    assert not any(name and name.startswith(("azure", "triage.store", "triage.tools")) for name in imports)
    assert store.port.mock_calls == []


@pytest.mark.parametrize("tenant_id", ["", "invalid", "00000000-0000-0000-0000-000000000000"])
def test_constructor_rejects_an_unpinned_deployment_without_reading_state(store, tenant_id):
    with pytest.raises(ValidationError):
        MonitoringService(store.port, tenant_id=tenant_id)
    assert store.calls == []
