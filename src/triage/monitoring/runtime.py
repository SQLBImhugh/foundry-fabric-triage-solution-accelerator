"""Common monitoring-store selection and canonical identity helpers.

Fixture selection is explicit. A live bootstrap failure never selects local
state, creates a schema, or imports environment target lists.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5
from weakref import WeakKeyDictionary

from triage.monitoring.contracts import (
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringNotBootstrapped,
    MonitoringSchemaMismatch,
    MonitoringStore,
    MonitoringUnavailable,
)
from triage.monitoring.models import (
    ActionKind,
    ActivateScopeRequest,
    CapabilityObservation,
    DeploymentControl,
    InventoryBatch,
    InventoryGeneration,
    InventoryItem,
    MonitoringContext,
    MonitoringTarget,
    RegistryVersion,
    SafetyReview,
    SafetyReviewRequest,
    ScopeDefinition,
    ScopePreviewRequest,
    ScopeRule,
    ScopeSelector,
    SourceExecutionIdentity,
    TargetIdentity,
    TargetQuery,
    Workload,
)
from triage.pipeline_models import canonical_id
from triage.policy import TriagePolicy
from triage.signature import compute_signature
from triage.store.approvals import InMemoryApprovalChannel
from triage.store.azure_sql import AzureSqlDatabase
from triage.store.processed import ProcessedMessageLog

logger = logging.getLogger("triage.monitoring.runtime")

FIXTURE_TENANT_ID = str(uuid5(NAMESPACE_URL, "bi-triage:fixture:tenant"))
FIXTURE_EPOCH = str(uuid5(NAMESPACE_URL, "bi-triage:fixture:epoch"))
FIXTURE_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_FIXTURES: WeakKeyDictionary = WeakKeyDictionary()
_FIXTURE_TIME: ContextVar[tuple[datetime, float] | None] = ContextVar("fixture_time", default=None)


def fixture_now() -> datetime:
    simulated = _FIXTURE_TIME.get()
    return (
        simulated[0] + timedelta(seconds=time.monotonic() - simulated[1])
        if simulated is not None else datetime.now(UTC)
    )


@contextmanager
def fixture_time(instant: datetime | None) -> Iterator[None]:
    if instant is not None and instant.tzinfo is None:
        raise ValueError("Fixture time must include a timezone.")
    token = _FIXTURE_TIME.set((instant, time.monotonic()) if instant is not None else None)
    try:
        yield
    finally:
        _FIXTURE_TIME.reset(token)


class FixtureClock:
    """A simulated advance is monotonic for the lifetime of its fixture store."""

    def __init__(self) -> None:
        self._base = fixture_now()
        self._started = time.monotonic()

    def __call__(self) -> datetime:
        current = self._base + timedelta(seconds=time.monotonic() - self._started)
        requested = fixture_now()
        if requested > current:
            self._base, self._started = requested, time.monotonic()
            return requested
        return current


class ScopedProcessedLog:
    """Mailbox delivery checkpoints cannot cross a tenant or monitoring epoch."""

    def __init__(self, inner: ProcessedMessageLog, context: MonitoringContext) -> None:
        self._inner = inner
        self._prefix = f"mail:{context.tenant_id}:{context.epoch}:"

    def seen(self, message_id: str) -> bool:
        return self._inner.seen(self._prefix + message_id)

    def mark(self, message_id: str, *, received_at: str = "") -> None:
        self._inner.mark(self._prefix + message_id, received_at=received_at)

    def reset(self) -> None:
        raise ValueError("Live mailbox checkpoints are reset only by deployment tooling.")

    @property
    def is_durable(self) -> bool:
        return bool(getattr(self._inner, "is_durable", False))


class FixtureApprovalChannel(InMemoryApprovalChannel):
    """The fixture approval channel and atomic registry share one state/lock."""

    def __init__(self, state: Any, clock: FixtureClock) -> None:
        self._state = state
        self._lock = state.lock
        self._clock = clock

    @property
    def _items(self):
        return self._state.approvals

    def decide_exact(self, request_id: str, **kwargs):
        with self._lock:
            row = super().decide_exact(request_id, **kwargs)
            row["decided_at"] = self._clock().isoformat()
            self._items[request_id] = row
            return dict(row)


@dataclass
class FixtureBinding:
    state: Any
    approvals: FixtureApprovalChannel
    clock: FixtureClock
    policy: TriagePolicy


def _fixture_view(
    binding: FixtureBinding, component: Literal["worker", "web", "controller", "fixture"],
) -> MonitoringStore:
    from triage.monitoring.memory import InMemoryMonitoringStore

    store = InMemoryMonitoringStore(
        state=binding.state, clock=binding.clock, policy=binding.policy, component=component,
    )
    _FIXTURES[store] = binding
    return store


def _fixture_binding(store: MonitoringStore) -> FixtureBinding:
    try:
        binding = _FIXTURES.get(store)
    except TypeError as exc:
        raise ValueError("An explicit runtime fixture is required, not live or externally owned state.") from exc
    if binding is None:
        raise ValueError("An explicit runtime fixture is required, not live or externally owned state.")
    return binding


def fixture_component(
    store: MonitoringStore, component: Literal["worker", "web", "controller"],
) -> MonitoringStore:
    """Construct an explicit restricted view over the same offline fixture state."""
    if component not in {"worker", "web", "controller"}:
        raise ValueError("A fixture component view must select worker, web or controller.")
    binding = _fixture_binding(store)
    return store if store.component == component else _fixture_view(binding, component)


@contextmanager
def fixture_setup(store: MonitoringStore) -> Iterator[MonitoringStore]:
    """Grant an explicit test/demo setup context, never authority to the runtime view."""
    binding = _fixture_binding(store)
    setup = _fixture_view(binding, "fixture")
    try:
        yield setup
    finally:
        _FIXTURES.pop(setup, None)


def fixture_approvals(store: MonitoringStore) -> FixtureApprovalChannel:
    binding = _FIXTURES.get(store)
    if binding is None:
        raise ValueError("This store was not constructed as an explicit runtime fixture.")
    return binding.approvals


def fixture_clock(store: MonitoringStore) -> FixtureClock:
    binding = _FIXTURES.get(store)
    if binding is None:
        raise ValueError("The registry has no runtime fixture clock.")
    return binding.clock


def stable_id(value: str) -> str:
    return str(uuid5(NAMESPACE_URL, value))


def source_work_id(execution: SourceExecutionIdentity, kind: str = "triage") -> str:
    from triage.monitoring.memory import stable_id as registry_id

    return registry_id(execution.target, f"{kind}:{execution.key}")


def fixture_id(value: str, *, kind: str) -> str:
    """Map synthetic fixture labels without weakening live ID validation."""
    try:
        return canonical_id(value)
    except ValueError:
        return stable_id(f"bi-triage:fixture:{kind}:{value}")


def fixture_target(workload: Workload, workspace_id: str, item_id: str) -> TargetIdentity:
    return TargetIdentity(
        tenant_id=FIXTURE_TENANT_ID, epoch=FIXTURE_EPOCH, workload=workload,
        workspace_id=fixture_id(workspace_id, kind="workspace"),
        item_id=fixture_id(item_id, kind=workload),
    )


def inspect_context(store: MonitoringStore, tenant_id: str) -> MonitoringContext:
    tenant_id = canonical_id(tenant_id)
    inspection = store.inspect_bootstrap(expected_tenant_id=tenant_id)
    if inspection.status == "missing":
        raise MonitoringNotBootstrapped(inspection.detail)
    if inspection.status == "incompatible":
        raise MonitoringSchemaMismatch(inspection.detail)
    if inspection.status == "kernel_incomplete":
        raise MonitoringKernelUnsupported(inspection.detail)
    if inspection.status == "wrong_tenant":
        raise MonitoringConflict(inspection.detail)
    if inspection.control is None:
        raise MonitoringUnavailable("Bootstrap inspection returned no deployment control.")
    return MonitoringContext(tenant_id=tenant_id, epoch=inspection.control.epoch)


def build_monitoring_store(
    settings: Any, *, db: AzureSqlDatabase | None = None, fixture: bool = False,
    component: Literal["worker", "web", "controller"] | None = None,
) -> MonitoringStore:
    """Build the common API/worker/controller store and verify deployment identity.

    Maintenance permits inspection and setup; admission/reservation still checks
    it atomically. Epoch and activation cutoff come only from deployed control.
    Explicit fixture components have the same authority separation as runtime
    components. Omitting component selects a fixture setup store only; live stores
    always require worker, web or controller.
    """
    if component is not None and component not in {"worker", "web", "controller"}:
        raise ValueError("Monitoring component must select worker, web or controller.")
    if fixture or settings.monitoring_mode == "fixture":
        if db is not None:
            raise ValueError("An explicit fixture monitoring store cannot use a live SQL handle.")
        try:
            from triage.monitoring.memory import InMemoryMonitoringState
        except ModuleNotFoundError as exc:
            if exc.name != "triage.monitoring.memory":
                raise
            raise MonitoringUnavailable("The fixture monitoring-store implementation is not installed.") from exc
        control = DeploymentControl(
            tenant_id=FIXTURE_TENANT_ID, epoch=FIXTURE_EPOCH, revision=0,
            activation_cutoff=FIXTURE_NOW, updated_at=FIXTURE_NOW, maintenance=False,
        )
        state = InMemoryMonitoringState.empty(control)
        clock = FixtureClock()
        binding = FixtureBinding(
            state, FixtureApprovalChannel(state, clock), clock, TriagePolicy.from_settings(settings),
        )
        setup = _fixture_view(binding, "fixture")
        inspect_context(setup, FIXTURE_TENANT_ID)
        ensure_fixture_target(setup, fixture_target("powerbi", "fixture-workspace", "fixture-model"), "Synthetic semantic model")
        ensure_fixture_target(setup, fixture_target("fabric_pipeline", "fixture-workspace", "fixture-pipeline"), "Synthetic pipeline")
        return fixture_component(setup, component) if component is not None else setup
    if settings.monitoring_mode != "live":
        raise ValueError("MONITORING_MODE must select fixture or live explicitly.")
    if component not in {"worker", "web", "controller"}:
        raise ValueError("Live monitoring requires an explicit worker, web or controller component.")
    tenant_id = canonical_id(settings.monitoring_tenant_id)
    if db is None:
        if not settings.azure_sql_server or not settings.azure_sql_database:
            raise MonitoringNotBootstrapped("Live monitoring requires both Azure SQL connection settings.")
        db = AzureSqlDatabase(server=settings.azure_sql_server, database=settings.azure_sql_database)
    try:
        from triage.monitoring.sql_store import AzureSqlMonitoringStore
    except ModuleNotFoundError as exc:
        if exc.name != "triage.monitoring.sql_store":
            raise
        raise MonitoringUnavailable("The live monitoring-store implementation is not installed.") from exc
    store = AzureSqlMonitoringStore(db=db, component=component, policy=TriagePolicy.from_settings(settings))
    inspect_context(store, tenant_id)
    return store


def registered_targets(
    store: MonitoringStore, context: MonitoringContext, *, workload: Workload | None = None,
    include_inactive: bool = False,
) -> list[MonitoringTarget]:
    """Read all pages at one registry revision; never return a truncated estate."""
    cursor = None
    seen: set[str] = set()
    version = None
    targets: list[MonitoringTarget] = []
    while True:
        page = store.list_targets(TargetQuery(
            **context.model_dump(), workload=workload, include_inactive=include_inactive,
            limit=100, cursor=cursor,
        ))
        if version is not None and page.version != version:
            raise MonitoringConflict("Monitoring scope changed while its targets were being read.")
        version = page.version
        targets.extend(page.items)
        cursor = page.next_cursor
        if cursor is None:
            return targets
        if cursor in seen:
            raise MonitoringUnavailable("Monitoring target pagination repeated a continuation.")
        seen.add(cursor)


def target_signature(target: TargetIdentity, error: str, *, exception_class: str | None = None) -> str:
    return compute_signature(
        source="fabric_pipeline_failure" if target.workload == "fabric_pipeline" else "powerbi_refresh_failure",
        artifact_kind="pipeline" if target.workload == "fabric_pipeline" else "dataset",
        target_key=target.key, error=error, exception_class=exception_class,
    )[0]


def ensure_fixture_target(
    store: MonitoringStore, identity: TargetIdentity, name: str, *,
    action: ActionKind | None = None, parameters: dict | None = None,
) -> MonitoringTarget:
    """Seed fixture inventory/admission only within an explicit fixture setup context."""
    if store not in _FIXTURES:
        raise ValueError("Fixture target seeding is forbidden on a live or externally owned registry.")
    if store.component != "fixture":
        raise ValueError("Fixture target seeding requires an explicit fixture_setup(store) context.")
    context = inspect_context(store, FIXTURE_TENANT_ID)
    now = fixture_clock(store)()
    definition = hashlib.sha256(f"fixture-definition:{identity.key}".encode()).hexdigest()

    def version() -> RegistryVersion:
        return RegistryVersion(**context.model_dump(), revision=store.snapshot(context).control.revision)

    target = store.resolve_target(identity, include_inactive=True)
    if target is None:
        generation_id = stable_id(f"{identity.key}:fixture-inventory:{now.isoformat()}")
        selector = ScopeSelector(
            tenant_id=context.tenant_id, kind="item",
            workspace_id=identity.workspace_id, item_id=identity.item_id,
        )
        store.record_inventory(InventoryBatch(
            request_id=generation_id, expected=version(),
            generation=InventoryGeneration(
                **context.model_dump(), generation_id=generation_id, selector=selector,
                adapter="explicit fixture", authority="fixture", completeness="complete",
                started_at=now, completed_at=now, discovered_count=1, completed_pages=1,
            ),
            items=(InventoryItem(
                **context.model_dump(), generation_id=generation_id,
                workspace_id=identity.workspace_id, item_id=identity.item_id,
                name=name or "Synthetic target",
                item_type="DataPipeline" if identity.workload == "fabric_pipeline" else "SemanticModel",
                workload=identity.workload, observed_at=now, definition_hash=definition,
            ),),
        ))
        store.record_capability(version(), CapabilityObservation(
            capability_id=stable_id(f"{generation_id}:capability"), target=identity,
            inventory_generation=generation_id, collector_identity_id=stable_id("fixture-collector"),
            read_status="verified", action_status="verified",
            exact_action_correlation=True, configuration_verification=True,
            definition_hash=definition, checked_at=now, expires_at=now + timedelta(days=1),
        ))
        plan = store.preview_scope(ScopePreviewRequest(
            expected=version(), idempotency_id=stable_id(f"{generation_id}:preview"),
            scope=ScopeDefinition(
                **context.model_dump(), scope_id=stable_id(f"{identity.key}:fixture-scope"),
                name=name or "Synthetic scope",
                rules=(ScopeRule(
                    rule_id=stable_id(f"{identity.key}:fixture-rule"),
                    selector=selector, effect="include", workloads=(identity.workload,),
                ),),
            ),
        ))
        store.activate_scope(ActivateScopeRequest(
            expected=version(), plan_id=plan.plan_id,
            idempotency_id=plan.idempotency_id,
        ))
        target = store.resolve_target(identity, include_inactive=True)
    if target is None:
        raise MonitoringUnavailable("Fixture scope activation did not create its target.")
    if action is not None and target.state == "current":
        review_id = stable_id(f"{identity.key}:fixture-review:{action}")
        prior = store.get_safety_review(context, review_id)
        if prior is None:
            configuration = action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}
            if action == "pipeline_rerun" and parameters is None:
                parameters = {}
            digest = hashlib.sha256(
                json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode(),
            ).hexdigest()
            store.record_safety_review(SafetyReviewRequest(
                request_id=stable_id(f"{review_id}:1"),
                expected=version(), expected_review_revision=0,
                review=SafetyReview(
                    review_id=review_id, target=identity, revision=1, policy_revision=version().revision,
                    action=action, state="verified", reviewer_id=stable_id("fixture-reviewer"),
                    reviewed_at=now, expires_at=now + timedelta(days=1),
                    definition_hash=definition if action == "pipeline_rerun" else None,
                    configuration_hash=digest if configuration else None,
                    parameters=parameters, replay_safe=action == "pipeline_rerun",
                    exact_correlation_verified=not configuration, detail="Explicit synthetic scenario review.",
                ),
            ))
    current = store.resolve_target(identity, include_inactive=True)
    if current is None:
        raise MonitoringUnavailable("The fixture target disappeared during setup.")
    return current
