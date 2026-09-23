"""Delegated-human monitoring API; durable state and revision arbitration stay in the store.

The application supplies its existing authenticated-actor dependency and mounts
this router before static files. It also constructs ``MonitoringService`` at
``app.state.service.monitoring``. This module does not bootstrap a database,
discover Fabric resources, grant permissions or execute workload actions.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Literal, TypeVar

from fastapi import APIRouter, Depends, Query, Request
from pydantic import TypeAdapter, ValidationError, model_validator

from triage.command_center.auth import require
from triage.command_center.models import Actor, ApiFailure
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringLeaseLost,
    MonitoringNotBootstrapped,
    MonitoringReader,
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
    DeploymentControl,
    InventoryDomain,
    InventoryItem,
    InventoryWorkspace,
    MonitoringContext,
    MonitoringModel,
    MonitoringSnapshot,
    MonitoringTarget,
    OwnedConnectorManifest,
    PageQuery,
    RecordPage,
    RegistryVersion,
    SafetyReview,
    SafetyReviewOperationReceipt,
    SafetyReviewRequest,
    ScopePolicy,
    ScopePreviewRequest,
    ScopeSelector,
    TargetIdentity,
    TargetQuery,
    Workload,
)
from triage.monitoring.runtime import registered_targets

logger = logging.getLogger("triage.command_center.monitoring")
_IDENTIFIER = TypeAdapter(CanonicalId)
_INVALID_REQUEST = "The request does not match the expected fields or values."
_Result = TypeVar("_Result")


class BootstrapResponse(BootstrapInspection):
    """The public diagnosis omits the foreign control required by store-level inspection."""

    @model_validator(mode="after")
    def validate_status(self) -> BootstrapResponse:
        if self.status == "wrong_tenant":
            if self.control is not None or self.found_schema_version is not None or self.missing_operations:
                raise ValueError("A wrong-tenant response must not expose foreign deployment metadata")
        else:
            super().validate_status()
        return self


class ScopeActivationInput(MonitoringModel):
    expected: RegistryVersion
    idempotency_id: CanonicalId


class InventoryRefreshInput(MonitoringModel):
    expected: RegistryVersion
    idempotency_id: CanonicalId
    selector: ScopeSelector


class InventoryRefreshReceipt(MonitoringModel):
    """Acknowledges durable queue acceptance, not the work's current execution state."""

    work_id: CanonicalId
    status: Literal["queued"] = "queued"


class SafetyReviewOperationResponse(MonitoringModel):
    """The UI envelope projects only the original request ID and committed redacted review."""

    request_id: CanonicalId
    review: SafetyReview


class MonitoringSnapshotResponse(MonitoringSnapshot):
    can_admin: bool
    user: Actor


@contextmanager
def _store_errors() -> Iterator[None]:
    try:
        yield
    except MonitoringCommitUncertain as exc:
        try:
            receipt_id = _IDENTIFIER.validate_python(exc.idempotency_id)
        except ValidationError:
            receipt_id = "unavailable"
        logger.warning("Monitoring commit requires receipt reconciliation")
        raise ApiFailure(
            503, "monitoring_commit_uncertain",
            f"The monitoring commit is uncertain; reconcile receipt {receipt_id}. "
            "Do not retry the mutation or use a new idempotency ID before reconciliation.",
        ) from exc
    except MonitoringLeaseLost as exc:
        raise ApiFailure(
            409, "monitoring_lease_lost", "Monitoring ownership changed. Reload the current state.",
        ) from exc
    except MonitoringConflict as exc:
        raise ApiFailure(
            409, "monitoring_conflict",
            "The monitoring revision, epoch or idempotent request no longer matches. "
            "Reload the current state and reconcile any existing receipt.",
        ) from exc
    except MonitoringNotBootstrapped as exc:
        raise ApiFailure(
            503, "monitoring_not_bootstrapped",
            "Monitoring has not been bootstrapped by deployment tooling.",
        ) from exc
    except MonitoringSchemaMismatch as exc:
        raise ApiFailure(
            503, "monitoring_schema_mismatch",
            "The monitoring schema is incompatible with this application.",
        ) from exc
    except MonitoringKernelUnsupported as exc:
        raise ApiFailure(
            503, "monitoring_kernel_incomplete",
            "The required guarded monitoring operation is unavailable. No success is being reported.",
        ) from exc
    except MonitoringComponentDenied as exc:
        raise ApiFailure(
            503, "monitoring_component_mismatch",
            "The configured service component cannot perform this monitoring operation.",
        ) from exc
    except MonitoringUnavailable as exc:
        logger.warning("Shared monitoring state is unavailable")
        raise ApiFailure(
            503, "monitoring_unavailable",
            "Shared monitoring state is unavailable. No success is being reported.",
        ) from exc
    except MonitoringStoreError as exc:
        logger.warning("Monitoring persistence failed (%s)", type(exc).__name__)
        raise ApiFailure(
            503, "monitoring_unavailable",
            "Monitoring persistence could not complete the request. No success is being reported.",
        ) from exc
    except PermissionError as exc:
        raise ApiFailure(
            403, "forbidden", "The required monitoring permission has not been granted.",
        ) from exc
    except ValidationError as exc:
        raise ApiFailure(422, "invalid_request", _INVALID_REQUEST) from exc


def _inspect_monitoring(store: MonitoringReader, tenant_id: str) -> BootstrapInspection:
    inspection = store.inspect_bootstrap(expected_tenant_id=tenant_id)
    if inspection.expected_tenant_id != tenant_id or inspection.status == "wrong_tenant":
        raise ApiFailure(
            503, "monitoring_bootstrap_mismatch",
            "Monitoring bootstrap does not belong to the server's deployment tenant.",
        )
    if inspection.status == "missing":
        raise MonitoringNotBootstrapped()
    if inspection.status == "incompatible":
        raise MonitoringSchemaMismatch()
    if inspection.status == "kernel_incomplete":
        raise MonitoringKernelUnsupported(inspection.detail)
    if inspection.control is None or inspection.control.tenant_id != tenant_id:
        raise ApiFailure(
            503, "monitoring_bootstrap_mismatch",
            "Monitoring bootstrap has no valid deployment control.",
        )
    return inspection


class MonitoringService:
    def __init__(self, store: WebMonitoringStore, *, tenant_id: str) -> None:
        self.store = store
        self.tenant_id = _IDENTIFIER.validate_python(tenant_id)

    def _inspection(self) -> BootstrapInspection:
        return _inspect_monitoring(self.store, self.tenant_id)

    def _control(self, expected: RegistryVersion | None = None) -> DeploymentControl:
        inspection = self._inspection()
        control = inspection.control
        if control is None:
            raise MonitoringNotBootstrapped()
        if expected is not None:
            if expected.tenant_id != self.tenant_id:
                raise ApiFailure(
                    422, "invalid_request", "The request must use the server's deployment tenant.",
                )
            if expected.epoch != control.epoch:
                raise MonitoringConflict()
            if control.maintenance:
                raise ApiFailure(
                    503, "monitoring_maintenance",
                    "Monitoring is in maintenance. Configuration changes are not admitted.",
                )
            # The store owns revision CAS, including replay of an already committed
            # receipt whose original expected revision is no longer current.
        return control

    @staticmethod
    def _context(control: DeploymentControl) -> MonitoringContext:
        return MonitoringContext(tenant_id=control.tenant_id, epoch=control.epoch)

    def bootstrap(self, actor: Actor) -> BootstrapResponse:
        require(actor, "reader")
        with _store_errors():
            try:
                inspection = self.store.inspect_bootstrap(expected_tenant_id=self.tenant_id)
            except MonitoringNotBootstrapped:
                return BootstrapResponse(
                    status="missing", expected_tenant_id=self.tenant_id,
                    detail="Monitoring has not been fully bootstrapped by deployment tooling.",
                )
            if (
                inspection.expected_tenant_id != self.tenant_id
                or inspection.status == "wrong_tenant"
                or inspection.control is not None and inspection.control.tenant_id != self.tenant_id
            ):
                return BootstrapResponse(
                    status="wrong_tenant", expected_tenant_id=self.tenant_id,
                    detail="Monitoring bootstrap does not belong to the server's deployment tenant.",
                )
            if inspection.status in {"missing", "incompatible"}:
                return BootstrapResponse(
                    status=inspection.status, expected_tenant_id=self.tenant_id,
                    found_schema_version=inspection.found_schema_version,
                    detail=(
                        "Monitoring has not been fully bootstrapped by deployment tooling."
                        if inspection.status == "missing"
                        else "The monitoring schema is incompatible with this application."
                    ),
                )
            if inspection.status == "kernel_incomplete":
                return BootstrapResponse(
                    status="kernel_incomplete", expected_tenant_id=self.tenant_id,
                    found_schema_version=inspection.found_schema_version, control=inspection.control,
                    missing_operations=inspection.missing_operations,
                    detail="The monitoring baseline is readable, but required guarded operations are unavailable.",
                )
            return BootstrapResponse.model_validate(inspection.model_dump())

    def snapshot(self, actor: Actor) -> MonitoringSnapshotResponse:
        require(actor, "reader")
        with _store_errors():
            snapshot = self.store.snapshot(self._context(self._control()))
            return MonitoringSnapshotResponse(
                control=snapshot.control, coverage=snapshot.coverage,
                can_admin=actor.permits("admin"),
                user=Actor(id=actor.id, display_name=actor.display_name, roles=list(actor.roles)),
            )

    def list_scopes(
        self, actor: Actor, *, limit: int = 100, cursor: str | None = None,
    ) -> RecordPage[ScopePolicy]:
        require(actor, "reader")
        with _store_errors():
            context = self._context(self._control())
            return self.store.list_scopes(PageQuery(
                **context.model_dump(), limit=limit, cursor=cursor,
            ))

    def list_inventory(
        self, actor: Actor, *, workload: Workload | None = None, workspace_id: str | None = None,
        limit: int = 100, cursor: str | None = None,
    ) -> RecordPage[InventoryItem]:
        require(actor, "reader")
        with _store_errors():
            context = self._context(self._control())
            return self.store.list_inventory(TargetQuery(
                **context.model_dump(), workload=workload, workspace_id=workspace_id,
                limit=limit, cursor=cursor,
            ))

    def list_targets(
        self, actor: Actor, *, workload: Workload | None = None, workspace_id: str | None = None,
        limit: int = 100, cursor: str | None = None, include_inactive: bool = False,
    ) -> RecordPage[MonitoringTarget]:
        require(actor, "reader")
        with _store_errors():
            context = self._context(self._control())
            return self.store.list_targets(TargetQuery(
                **context.model_dump(), workload=workload, workspace_id=workspace_id,
                limit=limit, cursor=cursor, include_inactive=include_inactive,
            ))

    def command_targets(self, actor: Actor) -> list[MonitoringTarget]:
        require(actor, "reader")
        with _store_errors():
            context = self._context(self._control())
            return [
                target for target in registered_targets(self.store, context)
                if target.state == "current" and target.observation.enabled
            ]

    def list_workspaces(
        self, actor: Actor, *, limit: int = 100, cursor: str | None = None,
    ) -> RecordPage[InventoryWorkspace]:
        require(actor, "reader")
        with _store_errors():
            context = self._context(self._control())
            return self.store.list_workspaces(PageQuery(
                **context.model_dump(), limit=limit, cursor=cursor,
            ))

    def list_domains(
        self, actor: Actor, *, limit: int = 100, cursor: str | None = None,
    ) -> RecordPage[InventoryDomain]:
        require(actor, "reader")
        with _store_errors():
            context = self._context(self._control())
            return self.store.list_domains(PageQuery(
                **context.model_dump(), limit=limit, cursor=cursor,
            ))

    def list_connectors(
        self, actor: Actor, *, limit: int = 100, cursor: str | None = None,
    ) -> RecordPage[OwnedConnectorManifest]:
        require(actor, "reader")
        with _store_errors():
            context = self._context(self._control())
            return self.store.list_connectors(PageQuery(
                **context.model_dump(), limit=limit, cursor=cursor,
            ))

    def preview_scope(self, value: ScopePreviewRequest, actor: Actor) -> ActivationPlan:
        require(actor, "admin")
        with _store_errors():
            value = ScopePreviewRequest.model_validate(value)
            request = ScopePreviewRequest.model_validate({
                **value.model_dump(), "requested_by": actor.id,
            })
            self._control(request.expected)
            return self.store.preview_scope(request)

    def get_plan(self, plan_id: str, actor: Actor) -> ActivationPlan:
        require(actor, "reader")
        with _store_errors():
            plan_id = _IDENTIFIER.validate_python(plan_id)
            context = self._context(self._control())
            plan = self.store.get_plan(context, plan_id)
            if plan is None:
                raise ApiFailure(404, "not_found", "Monitoring activation plan not found.")
            return plan

    def activate_scope(
        self, plan_id: str, value: ScopeActivationInput, actor: Actor,
    ) -> ActivationReceipt:
        require(actor, "admin")
        with _store_errors():
            value = ScopeActivationInput.model_validate(value)
            request = ActivateScopeRequest(
                expected=value.expected, idempotency_id=value.idempotency_id, plan_id=plan_id,
            )
            context = self._context(self._control(request.expected))
            plan = self.store.get_plan(context, request.plan_id)
            if plan is None:
                raise ApiFailure(404, "not_found", "Monitoring activation plan not found.")
            if plan.expected != request.expected:
                raise MonitoringConflict()
            return self.store.activate_scope(request)

    def get_activation(self, idempotency_id: str, actor: Actor) -> ActivationReceipt:
        require(actor, "reader")
        with _store_errors():
            idempotency_id = _IDENTIFIER.validate_python(idempotency_id)
            context = self._context(self._control())
            receipt = self.store.get_activation(context, idempotency_id)
            if receipt is None:
                raise ApiFailure(404, "not_found", "Monitoring activation receipt not found.")
            return receipt

    def refresh_inventory(self, value: InventoryRefreshInput, actor: Actor) -> InventoryRefreshReceipt:
        require(actor, "admin")
        with _store_errors():
            value = InventoryRefreshInput.model_validate(value)
            if value.selector.tenant_id != value.expected.tenant_id:
                raise ApiFailure(422, "invalid_request", _INVALID_REQUEST)
            self._control(value.expected)
            work = self.store.request_discovery(
                value.expected, value.selector, request_id=value.idempotency_id,
            )
            return InventoryRefreshReceipt(work_id=work.work_id)

    def record_safety_review(self, value: SafetyReviewRequest, actor: Actor) -> SafetyReview:
        require(actor, "admin")
        with _store_errors():
            value = SafetyReviewRequest.model_validate(value)
            request = SafetyReviewRequest.model_validate({
                **value.model_dump(),
                "review": {**value.review.model_dump(), "reviewer_id": actor.id},
            })
            control = self._control(request.expected)
            desired = (
                request.review.requested_state
                if request.review.publication_status == "pending_validation"
                else request.review.state
            )
            if desired == "revoked":
                prior = self.store.get_safety_review(self._context(control), request.review.review_id)
                # Both intent representations revoke the original window. A newer
                # current row must not block replay of an older operation receipt.
                if prior is not None and prior.revision == request.expected_review_revision and (
                    request.review.reviewed_at != prior.reviewed_at
                    or request.review.expires_at != prior.expires_at
                ):
                    raise MonitoringConflict("Revocation must retain the original review time and expiry.")
            # Review fields express human intent. Only stored capability and definition
            # evidence can establish verification; this API never manufactures that proof.
            saved = self.store.record_safety_review(request)
            if self.store.component == "web":
                if (
                    saved.publication_status != "pending_validation" or saved.state != "pending"
                    or saved.requested_state != desired or saved.exact_correlation_verified
                    or saved.revoked_at is not None or saved.reviewer_id != actor.id
                    or saved.review_id != request.review.review_id
                    or saved.target != request.review.target or saved.action != request.review.action
                    or saved.revision != request.review.revision
                    or saved.policy_revision != request.expected.revision
                    or saved.parameter_hash != request.review.parameter_hash
                    or saved.reviewed_at != request.review.reviewed_at
                    or saved.expires_at != request.review.expires_at
                ):
                    raise MonitoringUnavailable(
                        "The web review result does not match its accepted pending intent."
                    )
            return saved

    def get_safety_review(self, review_id: str, actor: Actor) -> SafetyReview:
        require(actor, "reader")
        with _store_errors():
            review_id = _IDENTIFIER.validate_python(review_id)
            context = self._context(self._control())
            review = self.store.get_safety_review(context, review_id)
            if review is None:
                raise ApiFailure(404, "not_found", "Monitoring safety review not found.")
            return review

    def get_safety_review_operation(self, request_id: str, actor: Actor) -> SafetyReviewOperationReceipt:
        require(actor, "reader")
        with _store_errors():
            request_id = _IDENTIFIER.validate_python(request_id)
            context = self._context(self._control())
            receipt = self.store.get_safety_review_operation(context, request_id)
            if receipt is None:
                raise ApiFailure(404, "not_found", "Monitoring safety-review operation has not been observed.")
            if receipt.request_id != request_id or (
                receipt.target.tenant_id, receipt.target.epoch
            ) != (context.tenant_id, context.epoch):
                raise MonitoringUnavailable("The safety-review receipt does not match the operation lookup.")
            return receipt


def resolve_command_target(
    store: MonitoringReader, *, tenant_id: str, target_id: str, kind: str,
) -> MonitoringTarget:
    """Resolve a human selection again at enqueue and after durable command ownership.

    This read is not an action reservation. The core controller must revalidate
    current admission and authoritative execution evidence at its atomic fence.
    """
    with _store_errors():
        parts = target_id.split(":")
        if len(parts) != 7 or parts[:2] != ["monitor", "v1"]:
            raise ApiFailure(422, "invalid_target", "Select a current registered monitoring target.")
        identity = TargetIdentity(
            epoch=parts[2], tenant_id=parts[3], workload=parts[4],
            workspace_id=parts[5], item_id=parts[6],
        )
        if identity.key != target_id:
            raise ApiFailure(422, "invalid_target", "Use the target's canonical registry identity.")
        control = _inspect_monitoring(store, _IDENTIFIER.validate_python(tenant_id)).control
        if control is None:
            raise MonitoringNotBootstrapped()
        if identity.tenant_id != control.tenant_id:
            raise ApiFailure(422, "invalid_target", "The target is outside the deployment tenant.")
        if identity.epoch != control.epoch:
            raise MonitoringConflict()
        if control.maintenance:
            raise ApiFailure(
                503, "monitoring_maintenance",
                "Monitoring is in maintenance. New investigations are not admitted.",
            )
        expected_workload = {"powerbi_triage": "powerbi", "pipeline_sweep": "fabric_pipeline"}.get(kind)
        if identity.workload != expected_workload:
            raise ApiFailure(422, "invalid_target", "The command kind does not match the target workload.")
        target = store.resolve_target(identity)
        if (
            target is None or target.identity != identity or target.state != "current"
            or not target.observation.enabled
        ):
            raise ApiFailure(
                422, "invalid_target", "The target was removed, paused or is not admitted for observation.",
            )
        return target


async def _with_service(
    request: Request, operation: Callable[[MonitoringService], _Result],
) -> _Result:
    def invoke() -> _Result:
        with _store_errors():
            runtime = getattr(request.app.state, "service", None)
            monitoring = getattr(runtime, "monitoring", None)
            if not isinstance(monitoring, MonitoringService):
                raise ApiFailure(
                    503, "monitoring_unavailable",
                    "The monitoring service is not configured. No success is being reported.",
                )
            return operation(monitoring)

    # Lazy construction can inspect SQL, so resolving the service also belongs
    # off the API event loop, not just the subsequent store operation.
    return await asyncio.to_thread(invoke)


def create_monitoring_router(
    actor_dependency: Callable[..., Actor | Awaitable[Actor]],
    *, bootstrap_reader: Callable[[Actor], BootstrapResponse] | None = None,
) -> APIRouter:
    """Use ``create_monitoring_router(authenticated_actor)`` before the static mount."""
    router = APIRouter(prefix="/api/monitoring", tags=["monitoring"])
    actor_parameter = Depends(actor_dependency)

    @router.get("/bootstrap")
    async def bootstrap(
        request: Request, actor: Actor = actor_parameter,
    ) -> BootstrapResponse:
        require(actor, "reader")
        if bootstrap_reader is not None:
            return await asyncio.to_thread(bootstrap_reader, actor)
        return await _with_service(request, lambda service: service.bootstrap(actor))

    @router.get("/snapshot")
    async def snapshot(
        request: Request, actor: Actor = actor_parameter,
    ) -> MonitoringSnapshotResponse:
        return await _with_service(request, lambda service: service.snapshot(actor))

    @router.get("/scopes")
    async def scopes(
        request: Request, actor: Actor = actor_parameter,
        limit: int = Query(100, ge=1, le=1_000),
        cursor: str | None = Query(None, min_length=1, max_length=4_096),
    ) -> RecordPage[ScopePolicy]:
        return await _with_service(
            request, lambda service: service.list_scopes(actor, limit=limit, cursor=cursor),
        )

    @router.get("/inventory")
    async def inventory(
        request: Request, actor: Actor = actor_parameter,
        workload: Workload | None = None, workspace_id: CanonicalId | None = None,
        limit: int = Query(100, ge=1, le=1_000),
        cursor: str | None = Query(None, min_length=1, max_length=4_096),
    ) -> RecordPage[InventoryItem]:
        return await _with_service(
            request, lambda service: service.list_inventory(
                actor, workload=workload, workspace_id=workspace_id, limit=limit, cursor=cursor,
            ),
        )

    @router.get("/targets")
    async def targets(
        request: Request, actor: Actor = actor_parameter,
        workload: Workload | None = None, workspace_id: CanonicalId | None = None,
        include_inactive: bool = False, limit: int = Query(100, ge=1, le=1_000),
        cursor: str | None = Query(None, min_length=1, max_length=4_096),
    ) -> RecordPage[MonitoringTarget]:
        return await _with_service(
            request, lambda service: service.list_targets(
                actor, workload=workload, workspace_id=workspace_id,
                include_inactive=include_inactive, limit=limit, cursor=cursor,
            ),
        )

    @router.get("/workspaces")
    async def workspaces(
        request: Request, actor: Actor = actor_parameter,
        limit: int = Query(100, ge=1, le=1_000),
        cursor: str | None = Query(None, min_length=1, max_length=4_096),
    ) -> RecordPage[InventoryWorkspace]:
        return await _with_service(
            request, lambda service: service.list_workspaces(actor, limit=limit, cursor=cursor),
        )

    @router.get("/domains")
    async def domains(
        request: Request, actor: Actor = actor_parameter,
        limit: int = Query(100, ge=1, le=1_000),
        cursor: str | None = Query(None, min_length=1, max_length=4_096),
    ) -> RecordPage[InventoryDomain]:
        return await _with_service(
            request, lambda service: service.list_domains(actor, limit=limit, cursor=cursor),
        )

    @router.get("/connectors")
    async def connectors(
        request: Request, actor: Actor = actor_parameter,
        limit: int = Query(100, ge=1, le=1_000),
        cursor: str | None = Query(None, min_length=1, max_length=4_096),
    ) -> RecordPage[OwnedConnectorManifest]:
        return await _with_service(
            request, lambda service: service.list_connectors(actor, limit=limit, cursor=cursor),
        )

    @router.post("/scopes/preview")
    async def preview(
        request: Request, value: ScopePreviewRequest, actor: Actor = actor_parameter,
    ) -> ActivationPlan:
        return await _with_service(request, lambda service: service.preview_scope(value, actor))

    @router.get("/plans/{plan_id}")
    async def plan(
        request: Request, plan_id: CanonicalId, actor: Actor = actor_parameter,
    ) -> ActivationPlan:
        return await _with_service(request, lambda service: service.get_plan(plan_id, actor))

    @router.post("/plans/{plan_id}/activate")
    async def activate(
        request: Request, plan_id: CanonicalId, value: ScopeActivationInput,
        actor: Actor = actor_parameter,
    ) -> ActivationReceipt:
        return await _with_service(
            request, lambda service: service.activate_scope(plan_id, value, actor),
        )

    @router.get("/activations/{idempotency_id}")
    async def activation(
        request: Request, idempotency_id: CanonicalId, actor: Actor = actor_parameter,
    ) -> ActivationReceipt:
        return await _with_service(
            request, lambda service: service.get_activation(idempotency_id, actor),
        )

    @router.post("/inventory/refresh")
    async def refresh_inventory(
        request: Request, value: InventoryRefreshInput, actor: Actor = actor_parameter,
    ) -> InventoryRefreshReceipt:
        return await _with_service(request, lambda service: service.refresh_inventory(value, actor))

    @router.post("/safety-reviews")
    async def safety_review(
        request: Request, value: SafetyReviewRequest, actor: Actor = actor_parameter,
    ) -> SafetyReview:
        return await _with_service(request, lambda service: service.record_safety_review(value, actor))

    @router.get("/safety-reviews/{review_id}")
    async def saved_safety_review(
        request: Request, review_id: CanonicalId, actor: Actor = actor_parameter,
    ) -> SafetyReview:
        return await _with_service(request, lambda service: service.get_safety_review(review_id, actor))

    @router.get("/safety-review-operations/{request_id}")
    async def safety_review_operation(
        request: Request, request_id: CanonicalId, actor: Actor = actor_parameter,
    ) -> SafetyReviewOperationResponse:
        receipt = await _with_service(
            request, lambda service: service.get_safety_review_operation(request_id, actor),
        )
        return SafetyReviewOperationResponse(request_id=receipt.request_id, review=receipt.review)

    return router
