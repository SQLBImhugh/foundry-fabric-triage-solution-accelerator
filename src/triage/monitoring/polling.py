"""Evidence-only REST polling and the durable monitoring worker tick.

No collector invokes an agent or submits a refresh, rerun, grant or schedule
change. Each accepted REST chunk is a RestPageRequest transaction. Work leases,
current admission, cutoff, source deduplication and finalization remain shared
store/controller responsibilities.

Use ``MonitoringCollector(...).run_once()`` from the worker. All synchronous store
and request-budget operations run off the event loop. Retry-After releases work
to its durable due queue; there is deliberately no HTTP retry/sleep loop here.

The shared ``request_discovery`` contract submits explicit ``discovery_selector``
intent. Producers persist workspace/domain observations, raw polling progress and
immutable handoffs. The controller publishes probes, admission and future work.
Final REST acceptance completes poll work only after its intake is durable.
Inventory continuation reads use retained generation catalogues and their counts;
every collector commit carries its work fence and expected generation position.
Power BI identity rows are staged in SQL until the complete source window resolves
refresh/request aliases. They are not executable observations in a page-local list.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol, TypeVar
from uuid import UUID, uuid5

from pydantic import Field, TypeAdapter, ValidationError

from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringStore,
    MonitoringStoreError,
)
from triage.monitoring.inventory import (
    FabricInventoryClient,
    RestReadError,
    RestRoute,
    TenantBoundRestClient,
    _assert_payload_tenant,
    _raw_id,
    bounded_gaps,
    collection_rows,
    definition_fingerprint,
    next_page_url,
    validate_rest_url,
)
from triage.monitoring.models import (
    CapabilityObservation,
    CollectionCommit,
    CoverageGap,
    Cursor,
    InventoryBatch,
    InventoryCommit,
    InventoryDomain,
    InventoryGeneration,
    InventoryItem,
    InventoryWorkspace,
    LeaseRenewal,
    MonitoringContext,
    MonitoringModel,
    MonitoringTarget,
    MonitoringWork,
    MonitoringWorkDraft,
    ObservationWindow,
    PageQuery,
    PowerBIWindowRow,
    QuarantineDisposition,
    RecordPage,
    RegistryVersion,
    RestCheckpoint,
    RestPageReceipt,
    RestPageRequest,
    ScopePolicy,
    ScopeSelector,
    SourceExecutionIdentity,
    SourceRunObservation,
    TargetIdentity,
    TargetQuery,
    UtcDateTime,
    WorkClaimRequest,
    WorkDispositionRequest,
)
from triage.pipeline_models import PIPELINE_JOB_TYPES, PipelineTarget, canonical_id
from triage.tools.fabric_pipeline import LiveFabricPipelineClient, PipelineApiError

logger = logging.getLogger("triage.monitoring.polling")

_CURSOR_ADAPTER = TypeAdapter(Cursor)
QueryT = TypeVar("QueryT", bound=PageQuery)
RecordT = TypeVar("RecordT", bound=MonitoringModel)
_PIPELINE_STATUSES = {
    "NotStarted": "not_started", "InProgress": "running", "Completed": "succeeded",
    "Failed": "failed", "Cancelled": "cancelled", "Deduped": "unknown",
}
_POWERBI_STATUSES = {
    "NotStarted": "not_started", "InProgress": "running", "Completed": "succeeded",
    "Failed": "failed", "Cancelled": "cancelled", "Disabled": "unknown", "Unknown": "unknown",
}
_POWERBI_INVOCATIONS = {
    "Scheduled": "scheduled", "OnDemand": "manual", "ViaApi": "manual",
    "ViaEnhancedApi": "manual", "ViaXmlaEndpoint": "manual",
}
_TRANSIENT_INVENTORY_GAPS = {
    "inventory_in_progress", "request_budget_exhausted", "service_throttled",
    "service_throttled_invalid_retry_after", "transport_error", "credential_unavailable",
}


def _id(namespace: str, value: str) -> str:
    return str(uuid5(UUID(namespace), value))


def _time(raw: Any, name: str) -> datetime | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        raise RestReadError("malformed_timestamp", f"Source {name} must be an ISO UTC timestamp")
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RestReadError("malformed_timestamp", f"Source {name} is not a valid timestamp") from exc
    # Both history APIs document UTC, including samples without an explicit suffix.
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _refresh_id(raw: Any) -> str | None:
    if raw is None:
        return None
    if type(raw) is int and raw > 0:
        return str(raw)
    if isinstance(raw, str) and raw.isascii() and raw.isdecimal() and int(raw) > 0:
        return str(int(raw))
    raise RestReadError("missing_identity", "Power BI history ID is not a positive decimal identifier")


def _request_id(raw: Any) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise RestReadError("missing_identity", "Power BI request ID is not a source execution GUID")
    try:
        return canonical_id(raw)
    except ValueError as exc:
        raise RestReadError("missing_identity", "Power BI request ID is not a source execution GUID") from exc


def _assert_powerbi_target(raw: Mapping[str, Any], target: TargetIdentity) -> None:
    _assert_payload_tenant(raw, target)
    for name, expected in (("workspaceId", target.workspace_id), ("datasetId", target.item_id)):
        if raw.get(name) is not None and _raw_id(raw, name) != expected:
            raise RestReadError("wrong_target", "Power BI history belongs to another target")


class _PowerBIAliases:
    def __init__(self, rows: Sequence[Any], target: TargetIdentity) -> None:
        self.refresh_to_requests: dict[str, set[str]] = {}
        self.request_to_refreshes: dict[str, set[str]] = {}
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            try:
                _assert_powerbi_target(raw, target)
                refresh = _refresh_id(raw.get("id"))
                request = _request_id(raw.get("requestId"))
            except RestReadError:
                continue
            if request is not None:
                self.request_to_refreshes.setdefault(request, set())
                if refresh is not None:
                    self.request_to_refreshes[request].add(refresh)
                    self.refresh_to_requests.setdefault(refresh, set()).add(request)

    def resolve(self, refresh: str | None, request: str | None) -> str:
        candidates = self.refresh_to_requests.get(refresh, set()) if refresh is not None else set()
        if request is not None:
            candidates = candidates | {request}
        if len(candidates) != 1:
            raise RestReadError(
                "ambiguous_execution",
                "Power BI source refresh ID has no unique REST-proven request-ID alias",
            )
        resolved = next(iter(candidates))
        refreshes = self.request_to_refreshes.get(resolved, set())
        if len(refreshes) > 1 or any(
            len(self.refresh_to_requests[value]) > 1 for value in refreshes
        ):
            raise RestReadError(
                "ambiguous_execution", "Power BI refresh/request identities disagree in REST history",
            )
        return resolved


def resolve_powerbi_execution(
    target: TargetIdentity, run_id: str, run_id_kind: Literal["powerbi_refresh", "powerbi_request"],
    rows: Sequence[Mapping[str, Any]],
) -> SourceExecutionIdentity:
    """Resolve an alert alias by exact fields, never by time/name similarity.

    All admitted Power BI observations use ``powerbi_request``. Numeric-only
    history without a REST-proven request alias is quarantined, not admitted
    under a second namespace that could replenish an incident/action budget.
    """
    requested = SourceExecutionIdentity(target=target, run_id=run_id, run_id_kind=run_id_kind)
    candidates = [
        row for row in rows
        if (
            run_id_kind == "powerbi_refresh" and _refresh_id(row.get("id")) == requested.run_id
        ) or (
            run_id_kind == "powerbi_request" and _request_id(row.get("requestId")) == requested.run_id
        )
    ]
    if not candidates:
        raise RestReadError("ambiguous_execution", "The exact Power BI source execution was not in REST history")
    for candidate in candidates:
        _assert_powerbi_target(candidate, target)
    aliases = _PowerBIAliases(rows, target)
    resolved = {
        aliases.resolve(_refresh_id(row.get("id")), _request_id(row.get("requestId")))
        for row in candidates
    }
    if len(resolved) != 1:
        raise RestReadError("ambiguous_execution", "Power BI REST evidence did not resolve one exact execution")
    return SourceExecutionIdentity(target=target, run_id_kind="powerbi_request", run_id=resolved.pop())


def _source_gap(code: str, detail: str, target: TargetIdentity) -> CoverageGap:
    return CoverageGap(code=code, detail=detail, workspace_id=target.workspace_id, item_id=target.item_id)


def normalize_pipeline_run(
    raw: Mapping[str, Any], target: TargetIdentity, *, observed_at: datetime,
) -> tuple[SourceRunObservation, tuple[CoverageGap, ...]]:
    if target.workload != "fabric_pipeline":
        raise ValueError("Fabric jobs require a pipeline target")
    _assert_payload_tenant(raw, target)
    if raw.get("workspaceId") is not None and _raw_id(raw, "workspaceId") != target.workspace_id:
        raise RestReadError("wrong_workspace", "Fabric job evidence belongs to another workspace")
    try:
        run = LiveFabricPipelineClient._run(
            dict(raw), PipelineTarget(
                name="Source evidence", workspace_id=target.workspace_id, pipeline_id=target.item_id,
            ),
        )
    except (PipelineApiError, ValidationError) as exc:
        raise RestReadError("malformed_job", "Fabric job failed the existing typed source-evidence contract") from exc
    if run.job_type not in PIPELINE_JOB_TYPES:
        raise RestReadError("unsupported_job_type", "Fabric job type has no pipeline detector contract")
    status = _PIPELINE_STATUSES[run.status]
    invocation = {"Scheduled": "scheduled", "Manual": "manual", "OnDemand": "manual"}.get(
        run.invoke_type, "unknown",
    )
    if status in {"succeeded", "failed", "cancelled"} and (
        run.start_time is None or run.end_time is None
    ):
        raise RestReadError("incomplete_job", "Terminal Fabric job lacks exact start/completion evidence")
    gaps: list[CoverageGap] = []
    if status == "unknown":
        gaps.append(_source_gap("source_status_unknown", "Fabric Deduped is not a terminal failure", target))
    if invocation == "unknown":
        gaps.append(_source_gap(
            "source_invocation_unknown", "Fabric invocation is not a verified scheduled/manual invocation", target,
        ))
    reason = raw.get("failureReason") or {}
    truncated = len(reason.get("message") or "") > 4_000 or len(reason.get("errorCode") or "") > 200
    observation = SourceRunObservation(
        execution=SourceExecutionIdentity(target=target, run_id_kind="fabric_job", run_id=run.id),
        origin="poll", authority="rest", observed_at=observed_at,
        started_at=run.start_time, ended_at=run.end_time,
        status=status, invocation=invocation, job_type=run.job_type,
        error_code=run.error_code or None, failure_reason=run.failure_reason or None,
        evidence={
            "source_status": run.status, "source_invocation": run.invoke_type,
            "item_id": run.item_id, "job_type": run.job_type,
        },
        evidence_truncated=truncated,
    )
    return observation, tuple(gaps)


def normalize_powerbi_run(
    raw: Mapping[str, Any], target: TargetIdentity, *, observed_at: datetime,
    aliases: _PowerBIAliases | None = None,
) -> tuple[SourceRunObservation, tuple[CoverageGap, ...]]:
    if target.workload != "powerbi":
        raise ValueError("Power BI history requires a semantic-model target")
    _assert_powerbi_target(raw, target)
    refresh = _refresh_id(raw.get("id"))
    request = (aliases or _PowerBIAliases([dict(raw)], target)).resolve(
        refresh, _request_id(raw.get("requestId")),
    )
    return _powerbi_observation(
        raw, target, observed_at=observed_at, refresh_id=refresh,
        execution=SourceExecutionIdentity(target=target, run_id_kind="powerbi_request", run_id=request),
    )


def normalize_powerbi_window_row(
    raw: Mapping[str, Any], target: TargetIdentity, *, observed_at: datetime,
) -> tuple[PowerBIWindowRow, tuple[CoverageGap, ...]]:
    """Keep both documented identities; only the durable window resolves aliases."""
    if target.workload != "powerbi":
        raise ValueError("Power BI history requires a semantic-model target")
    _assert_powerbi_target(raw, target)
    refresh = _refresh_id(raw.get("id"))
    request = _request_id(raw.get("requestId"))
    if request is None and refresh is None:
        raise RestReadError("ambiguous_execution", "Power BI history has no refresh or request identity")
    execution = SourceExecutionIdentity(
        target=target, run_id_kind="powerbi_request" if request is not None else "powerbi_refresh",
        run_id=request if request is not None else refresh,
    )
    observation, gaps = _powerbi_observation(
        raw, target, observed_at=observed_at, execution=execution, refresh_id=refresh,
    )
    return PowerBIWindowRow(observation=observation, refresh_id=refresh), gaps


def _powerbi_observation(
    raw: Mapping[str, Any], target: TargetIdentity, *, observed_at: datetime,
    execution: SourceExecutionIdentity, refresh_id: str | None,
) -> tuple[SourceRunObservation, tuple[CoverageGap, ...]]:
    source_status = raw.get("status")
    refresh_type = raw.get("refreshType")
    if not isinstance(source_status, str) or source_status not in _POWERBI_STATUSES:
        raise RestReadError("missing_status", "Power BI refresh has no supported explicit status")
    if not isinstance(refresh_type, str) or not refresh_type:
        raise RestReadError("missing_invocation", "Power BI refresh has no refreshType evidence")
    status = _POWERBI_STATUSES[source_status]
    invocation = _POWERBI_INVOCATIONS.get(refresh_type, "unknown")
    started = _time(raw.get("startTime"), "startTime")
    ended = _time(raw.get("endTime"), "endTime")
    if status in {"succeeded", "failed", "cancelled"} and (started is None or ended is None):
        raise RestReadError("incomplete_refresh", "Terminal Power BI refresh lacks start/completion evidence")
    exception = raw.get("serviceExceptionJson")
    error: dict[str, Any] = {}
    if exception not in (None, ""):
        if not isinstance(exception, str):
            raise RestReadError("malformed_error", "Power BI exception evidence is not the documented JSON string")
        try:
            decoded = json.loads(exception)
        except (ValueError, RecursionError) as exc:
            raise RestReadError("malformed_error", "Power BI exception evidence is not readable JSON") from exc
        if not isinstance(decoded, dict):
            raise RestReadError("malformed_error", "Power BI exception evidence is not an object")
        error = decoded
    code = error.get("errorCode")
    message = error.get("message")
    if any(value is not None and not isinstance(value, str) for value in (code, message)):
        raise RestReadError("malformed_error", "Power BI exception code/message must be strings")
    gaps: list[CoverageGap] = []
    if status == "unknown":
        gaps.append(_source_gap(
            "source_status_unknown", "Power BI Unknown/Disabled is not proof of success or failure", target,
        ))
    if invocation == "unknown":
        gaps.append(_source_gap(
            "source_invocation_unknown", "Power BI refreshType is not a known scheduled/non-scheduled type", target,
        ))
    observation = SourceRunObservation(
        execution=execution,
        origin="poll", authority="rest", observed_at=observed_at, started_at=started, ended_at=ended,
        status=status, invocation=invocation, job_type="Refresh",
        error_code=code[:200] if code else None, failure_reason=message[:4_000] if message else None,
        evidence={
            "refresh_id": refresh_id,
            "request_id": execution.run_id if execution.run_id_kind == "powerbi_request" else None,
            "source_status": source_status, "refresh_type": refresh_type,
        },
        evidence_truncated=bool(
            code and len(code) > 200 or message and len(message) > 4_000
            or error.keys() - {"errorCode", "message"}
        ),
    )
    return observation, tuple(gaps)


def _pack_ids(identities: set[str]) -> str:
    return base64.urlsafe_b64encode(b"".join(
        UUID(value).bytes for value in sorted(identities)
    )).decode("ascii")


def _unpack_ids(value: str) -> set[str]:
    try:
        raw = base64.b64decode(value, altchars=b"-_", validate=True)
        if len(raw) % 16 or len(raw) > 1_600:
            raise ValueError("Retained source identities exceeded their bound")
        return {str(UUID(bytes=raw[index:index + 16])) for index in range(0, len(raw), 16)}
    except (ValueError, binascii.Error) as exc:
        raise RestReadError("invalid_continuation", "Poll continuation has invalid retained-run identities") from exc


class _PollCursor(MonitoringModel):
    version: Literal[1] = 1
    target_key: str
    window: ObservationWindow
    url: str | None = None
    offset: int = Field(default=0, ge=0)
    page_hash: str | None = None
    terminal_ids: str = ""
    oldest_terminal_at: UtcDateTime | None = None
    incomplete: bool = False
    trail: tuple[str, ...] = ()
    retry_serial: int = Field(default=0, ge=0)
    last_read_gap: CoverageGap | None = None

    def encode(self) -> str:
        try:
            return _CURSOR_ADAPTER.validate_python(self.model_dump_json(exclude_none=True))
        except ValidationError as exc:
            raise RestReadError(
                "continuation_state_too_large", "Poll continuation exceeded the durable cursor bound",
            ) from exc


@dataclass(frozen=True)
class HistoryReadPage:
    observations: tuple[SourceRunObservation, ...]
    quarantines: tuple[QuarantineDisposition, ...]
    next_cursor: str | None
    received_count: int
    window_complete: bool
    retention_exhausted: bool
    gaps: tuple[CoverageGap, ...]
    source_page_hash: str
    powerbi_rows: tuple[PowerBIWindowRow, ...] = ()
    powerbi_window_complete: bool = False


class DefinitionReader(Protocol):
    """Optional explicitly authorized reader, using the same collector principal."""

    context: MonitoringContext
    collector_identity_id: str

    async def read(self, target: TargetIdentity) -> Mapping[str, Any]: ...


class _HistoryClient:
    workload: Literal["fabric_pipeline", "powerbi"]
    retained_completed_limit: int

    def __init__(
        self, rest: TenantBoundRestClient, *, definition_reader: DefinitionReader | None = None,
    ) -> None:
        self.rest = rest
        self.definition_reader = definition_reader
        if definition_reader is not None and (
            definition_reader.context != rest.context
            or canonical_id(definition_reader.collector_identity_id) != rest.collector_identity_id
        ):
            raise ValueError("Definition reads must use the same pinned collector identity")

    def _check(self, target: TargetIdentity) -> None:
        if target.workload != self.workload or (
            target.tenant_id, target.epoch,
        ) != (self.rest.context.tenant_id, self.rest.context.epoch):
            raise RestReadError("wrong_target", "History target differs from the pinned workload/tenant/epoch")

    def _route(self, target: TargetIdentity) -> RestRoute:
        if target.workload == "fabric_pipeline":
            return RestRoute(
                "fabric", "fabric.jobs",
                f"/workspaces/{target.workspace_id}/items/{target.item_id}/jobs/instances",
            )
        return RestRoute(
            "powerbi", "powerbi.refreshes",
            f"/groups/{target.workspace_id}/datasets/{target.item_id}/refreshes",
            query=(("$top", "60"),),
        )

    async def read_page(
        self, target: TargetIdentity, window: ObservationWindow, *,
        observed_at: datetime, cursor: str | None = None,
    ) -> HistoryReadPage:
        self._check(target)
        try:
            state = _PollCursor.model_validate_json(cursor) if cursor else _PollCursor(
                target_key=target.key, window=window,
            )
        except ValidationError as exc:
            raise RestReadError("invalid_continuation", "Poll continuation is malformed") from exc
        if state.target_key != target.key or state.window != window:
            raise RestReadError("invalid_continuation", "Poll continuation belongs to another target/window")
        state = state.model_copy(update={"last_read_gap": None})
        retained_ids = _unpack_ids(state.terminal_ids)
        route = self._route(target)
        payload = await self.rest.get(route, continuation=state.url)
        rows = collection_rows(payload, route)
        next_url = next_page_url(payload, route)
        current_url = validate_rest_url(route, state.url or route.url)
        current_hash = hashlib.sha256(current_url.encode()).hexdigest()[:16]
        if next_url is not None and hashlib.sha256(next_url.encode()).hexdigest()[:16] in (
            *state.trail, current_hash,
        ):
            raise RestReadError("pagination_cycle", "History repeated a continuation position")
        page_hash = hashlib.sha256(json.dumps(
            rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode()).hexdigest()
        offset = state.offset
        gaps: list[CoverageGap] = []
        if offset and state.page_hash != page_hash:
            offset = 0
            gaps.append(_source_gap(
                "source_page_changed",
                "An interrupted history page changed; reread its prefix instead of skipping observations",
                target,
            ))
        if offset > len(rows):
            raise RestReadError("invalid_continuation", "Poll continuation exceeds its source page")
        end_offset = min(len(rows), offset + 200)
        observations: list[SourceRunObservation] = []
        powerbi_rows: list[PowerBIWindowRow] = []
        quarantines: list[QuarantineDisposition] = []
        incomplete = state.incomplete or bool(gaps)
        oldest = state.oldest_terminal_at
        # Prefix normalization reconstructs retention state after a partial-page
        # restart. The cursor stores only state from preceding complete API pages.
        for index, raw in enumerate(rows[:end_offset]):
            try:
                if not isinstance(raw, dict):
                    raise RestReadError("malformed_observation", "History returned a non-object execution")
                staged = None
                if target.workload == "fabric_pipeline":
                    observation, row_gaps = normalize_pipeline_run(raw, target, observed_at=observed_at)
                else:
                    staged, row_gaps = normalize_powerbi_window_row(raw, target, observed_at=observed_at)
                    observation = staged.observation
                if observation.status in {"succeeded", "failed", "cancelled"}:
                    if len(retained_ids) < self.retained_completed_limit and observation.execution.run_id_kind != "powerbi_refresh":
                        retained_ids.add(observation.execution.run_id)
                    if observation.ended_at is not None:
                        oldest = min(oldest, observation.ended_at) if oldest else observation.ended_at
                incomplete |= bool(row_gaps)
                if index >= offset:
                    if staged is None:
                        observations.append(observation)
                    else:
                        powerbi_rows.append(staged)
                    gaps.extend(row_gaps)
            except (RestReadError, ValidationError) as exc:
                incomplete = True
                if index < offset:
                    continue
                code = exc.code if isinstance(exc, RestReadError) else "malformed_observation"
                detail = exc.detail if isinstance(exc, RestReadError) else "Source execution failed its typed evidence contract"
                gaps.append(_source_gap(code, detail, target))
                quarantines.append(QuarantineDisposition(
                    observation_id=_id(target.epoch, f"{target.key}:{page_hash}:{index}"),
                    reason="ambiguous_execution" if code == "ambiguous_execution" else
                    "wrong_tenant" if code == "wrong_tenant" else
                    "unsupported" if code == "unsupported_job_type" else "malformed",
                    detail=detail, metadata={"source_page_hash": page_hash, "row_index": index, "code": code},
                ))
        complete_api_page = end_offset == len(rows)
        exhausted = (
            complete_api_page and next_url is None
            and len(retained_ids) >= self.retained_completed_limit
            and (oldest is None or oldest > window.start_at)
        )
        if exhausted:
            gaps.append(_source_gap(
                "retention_exhausted",
                f"The retained {self.retained_completed_limit}-completed-run window does not cover the promised lookback",
                target,
            ))
        if not complete_api_page:
            following = state.model_copy(update={
                "offset": end_offset, "page_hash": page_hash, "incomplete": incomplete,
            })
        elif next_url is not None:
            following = state.model_copy(update={
                "url": next_url, "offset": 0, "page_hash": None,
                "terminal_ids": _pack_ids(retained_ids), "oldest_terminal_at": oldest,
                "incomplete": incomplete, "trail": (*state.trail, current_hash),
            })
        else:
            following = None
        if incomplete and not gaps:
            gaps.append(_source_gap(
                "previous_page_incomplete", "A preceding history page has unverified execution evidence", target,
            ))
        if gaps and (observations or powerbi_rows):
            first = observations[0] if observations else powerbi_rows[0].observation
            annotated = SourceRunObservation.model_validate({
                **first.model_dump(),
                "evidence": {
                    **first.evidence,
                    "poll_coverage": {
                        "window": window.model_dump(mode="json"),
                        "retention_exhausted": exhausted,
                        "gaps": [gap.model_dump(mode="json") for gap in bounded_gaps(gaps, limit=20)],
                    },
                },
            })
            if observations:
                observations[0] = annotated
            else:
                powerbi_rows[0] = PowerBIWindowRow(
                    observation=annotated, refresh_id=powerbi_rows[0].refresh_id,
                )
        return HistoryReadPage(
            observations=tuple(observations), quarantines=tuple(quarantines),
            next_cursor=following.encode() if following is not None else None,
            received_count=end_offset - offset,
            window_complete=following is None and not incomplete and not exhausted,
            retention_exhausted=exhausted, gaps=bounded_gaps(gaps), source_page_hash=page_hash,
            powerbi_rows=tuple(powerbi_rows),
            powerbi_window_complete=target.workload == "powerbi" and following is None,
        )

    async def probe(
        self, target: TargetIdentity, *, inventory_generation: str,
        checked_at: datetime, ttl_seconds: int = 3_600,
    ) -> CapabilityObservation:
        self._check(target)
        route = (
            RestRoute(
                "fabric", "fabric.item", f"/workspaces/{target.workspace_id}/items/{target.item_id}", None,
            ) if target.workload == "fabric_pipeline" else RestRoute(
                "powerbi", "powerbi.dataset", f"/groups/{target.workspace_id}/datasets/{target.item_id}", None,
            )
        )
        status: Literal["verified", "denied", "unknown", "blocked"] = "unknown"
        gaps: list[CoverageGap] = []
        definition_hash: str | None = None
        try:
            item = await self.rest.get(route)
            if _raw_id(item) != target.item_id:
                raise RestReadError("wrong_target", "Source metadata identifies another item")
            if target.workload == "powerbi":
                _assert_powerbi_target(item, target)
            if target.workload == "fabric_pipeline" and (
                item.get("type") != "DataPipeline"
                or _raw_id(item, "workspaceId") != target.workspace_id
            ):
                raise RestReadError("wrong_target", "Source metadata is not the expected DataPipeline")
            name = item.get("displayName" if target.workload == "fabric_pipeline" else "name")
            if not isinstance(name, str) or not name.strip():
                raise RestReadError("metadata_incomplete", "Source metadata has no display name")
            history = await self.read_page(
                target, ObservationWindow(start_at=checked_at, end_at=checked_at), observed_at=checked_at,
            )
            gaps.extend(gap for gap in history.gaps if gap.code != "retention_exhausted")
            status = "unknown" if history.quarantines else "verified"
        except RestReadError as exc:
            status = "denied" if exc.status_code in {401, 403} else "blocked"
            gaps.append(exc.gap(target.workspace_id, target.item_id))
        if status == "verified" and self.definition_reader is not None:
            try:
                definition_hash = definition_fingerprint(await self.definition_reader.read(target))
            except (RestReadError, ValidationError, ValueError) as exc:
                gaps.append(_source_gap(
                    "definition_unverified", "Authorized source definition could not be read and fingerprinted", target,
                ))
                logger.warning("Source definition probe failed (%s)", type(exc).__name__)
        return CapabilityObservation(
            capability_id=_id(target.epoch, f"capability:{target.key}:{checked_at.isoformat()}"),
            target=target, inventory_generation=inventory_generation,
            collector_identity_id=self.rest.collector_identity_id, read_status=status,
            event_status="unknown", action_status="unknown", exact_action_correlation=False,
            definition_hash=definition_hash, checked_at=checked_at,
            expires_at=checked_at + timedelta(seconds=ttl_seconds),
            required_permissions=(
                ("Fabric item read and job-history access",)
                if target.workload == "fabric_pipeline" else
                ("Power BI semantic model Write permission for refresh history",)
            ),
            gaps=bounded_gaps(gaps),
        )


class FabricPipelinePollingClient(_HistoryClient):
    workload = "fabric_pipeline"
    retained_completed_limit = 100


class PowerBIPollingClient(_HistoryClient):
    workload = "powerbi"
    retained_completed_limit = 60


@dataclass(frozen=True)
class CollectorWorkResult:
    work_id: str
    state: Literal["recorded", "deferred", "superseded", "lease_lost"]
    pages: int = 0
    observations: int = 0
    gaps: tuple[CoverageGap, ...] = ()


@dataclass(frozen=True)
class CollectorRunResult:
    claimed: int
    results: tuple[CollectorWorkResult, ...] = ()
    gaps: tuple[CoverageGap, ...] = ()


class MonitoringCollector:
    """Bounded async worker tick; the store arbitrates cross-replica work shares.

    Inventory can start from a queued ``discovery_selector`` before any scope
    exists. Scope-only work expands into explicit selector jobs, never a legacy
    environment target list. Successful collection schedules a durable next
    cycle; crashes leave the current lease/page recoverable.
    """

    def __init__(
        self, store: MonitoringStore, context: MonitoringContext,
        inventory_client: FabricInventoryClient,
        pipeline_client: FabricPipelinePollingClient,
        powerbi_client: PowerBIPollingClient,
        collector_identity_id: str, owner_id: str, *,
        batch_size: int = 4, per_workspace_limit: int = 2,
        pages_per_work: int = 4, lease_seconds: int = 120,
        inventory_seconds: int = 3_600, retry_seconds: int = 60,
        lookback_seconds: int = 86_400,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.store = store
        self.context = MonitoringContext.model_validate(context)
        self.inventory_client = inventory_client
        self.pipeline_client = pipeline_client
        self.powerbi_client = powerbi_client
        self.collector_identity_id = canonical_id(collector_identity_id)
        self.owner_id = canonical_id(owner_id)
        self._claim = WorkClaimRequest(
            **self.context.model_dump(), owner_id=self.owner_id,
            kinds=("inventory", "capability_probe", "poll"), limit=batch_size,
            per_workspace_limit=per_workspace_limit, lease_seconds=lease_seconds,
        )
        if not 1 <= pages_per_work <= 100 or any(
            type(value) is not int or not 15 <= value <= 86_400
            for value in (inventory_seconds, retry_seconds, lookback_seconds)
        ):
            raise ValueError("Collector page/time budgets are outside the supported bounds")
        for client in (inventory_client, pipeline_client, powerbi_client):
            if client.rest.context != self.context or (
                client.rest.collector_identity_id != self.collector_identity_id
            ):
                raise ValueError("Every collector adapter must use the same tenant, epoch and identity")
        self._pages_per_work = pages_per_work
        self._inventory_seconds = inventory_seconds
        self._retry_seconds = retry_seconds
        self._lookback_seconds = lookback_seconds
        self._clock = clock

    async def _version(self) -> RegistryVersion:
        snapshot = await asyncio.to_thread(self.store.snapshot, self.context)
        return RegistryVersion(
            **self.context.model_dump(), revision=snapshot.control.revision,
        )

    async def _renew(self, work: MonitoringWork) -> MonitoringWork:
        if work.lease is None:
            raise MonitoringLeaseLost("Collector work has no active lease")
        lease = await asyncio.to_thread(
            self.store.renew_lease,
            LeaseRenewal(lease=work.lease, lease_seconds=self._claim.lease_seconds),
        )
        # The guarded SQL renewal also advances the work revision. Reusing the
        # pre-renewal revision fences the first inventory commit before any GET.
        return await self._fresh_work(work.model_copy(update={"lease": lease}))

    async def _fresh_work(self, work: MonitoringWork) -> MonitoringWork:
        current = await asyncio.to_thread(self.store.get_work, self.context, work.work_id)
        if (
            current is None or current.lease is None or work.lease is None
            or current.lease.owner_id != work.lease.owner_id or current.lease.fence != work.lease.fence
        ):
            raise MonitoringLeaseLost("Collector work ownership changed")
        return current

    async def _disposition(
        self, work: MonitoringWork, detail: str, *, retry_at: datetime | None = None,
    ) -> None:
        current = await self._fresh_work(work)
        await asyncio.to_thread(self.store.disposition_work, WorkDispositionRequest(
            **self.context.model_dump(),
            request_id=_id(
                work.work_id, f"disposition:{current.revision}:{retry_at.isoformat() if retry_at else detail}",
            ),
            work_id=work.work_id, expected_work_revision=current.revision,
            lease=current.lease, disposition="retry" if retry_at is not None else "superseded",
            detail=detail, retry_at=retry_at,
        ))

    async def _enqueue(self, draft: MonitoringWorkDraft) -> None:
        existing = await asyncio.to_thread(self.store.get_work, self.context, draft.work_id)
        if existing is not None:
            if (existing.kind, existing.target, existing.scope_id, existing.discovery_selector) != (
                draft.kind, draft.target, draft.scope_id, draft.discovery_selector,
            ):
                raise MonitoringConflict("Collector work identity was reused for different work")
            return
        await asyncio.to_thread(self.store.enqueue_work, draft)

    async def _records(
        self, reader: Callable[[QueryT], RecordPage[RecordT]], query: QueryT,
    ) -> tuple[RecordT, ...]:
        result: list[RecordT] = []
        seen: set[str] = set()
        version: RegistryVersion | None = None
        while True:
            page = await asyncio.to_thread(reader, query)
            if (page.version.tenant_id, page.version.epoch) != (
                self.context.tenant_id, self.context.epoch,
            ) or version is not None and page.version != version:
                raise MonitoringConflict("Shared catalogue context/revision changed during pagination")
            version = page.version
            result.extend(page.items)
            if page.next_cursor is None:
                return tuple(result)
            if page.next_cursor in seen:
                raise MonitoringConflict("Shared catalogue pagination repeated its continuation")
            seen.add(page.next_cursor)
            query = query.model_copy(update={"cursor": page.next_cursor})

    async def _scopes(self) -> tuple[ScopePolicy, ...]:
        return await self._records(
            self.store.list_scopes, PageQuery(**self.context.model_dump(), limit=1_000),
        )

    async def _inventory_records(
        self, generation: InventoryGeneration,
    ) -> tuple[tuple[InventoryItem, ...], tuple[InventoryWorkspace, ...], tuple[InventoryDomain, ...]]:
        items, workspaces, domains = await asyncio.gather(
            self._records(
                lambda query: self.store.list_inventory(query, generation_id=generation.generation_id),
                TargetQuery(**self.context.model_dump(), limit=1_000),
            ),
            self._records(
                lambda query: self.store.list_workspaces(query, generation_id=generation.generation_id),
                PageQuery(**self.context.model_dump(), limit=1_000),
            ),
            self._records(
                lambda query: self.store.list_domains(query, generation_id=generation.generation_id),
                PageQuery(**self.context.model_dump(), limit=1_000),
            ),
        )
        if (
            len(items) != generation.recorded_item_count
            or len(workspaces) != generation.recorded_workspace_count
            or len(domains) != generation.recorded_domain_count
            or any(value.generation_id != generation.generation_id for value in (*items, *workspaces, *domains))
        ):
            raise RestReadError(
                "continuation_catalogue_missing",
                "The durable generation catalogue does not match its recorded item/workspace/domain counts",
            )
        return items, workspaces, domains

    def _inventory_commit(
        self, work: MonitoringWork, generation: InventoryGeneration | None,
    ) -> InventoryCommit:
        if work.lease is None:
            raise MonitoringLeaseLost("Inventory commit has no current work lease")
        return InventoryCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_generation_revision=generation.revision if generation else 0,
            expected_continuation=generation.continuation if generation else None,
        )

    async def _record_inventory(self, batch: InventoryBatch) -> InventoryGeneration:
        try:
            return await asyncio.to_thread(self.store.record_inventory, batch)
        except MonitoringCommitUncertain:
            # The same validated request is safe to reconcile, not a new generation.
            return await asyncio.to_thread(self.store.record_inventory, batch)

    async def _inventory(self, work: MonitoringWork) -> CollectorWorkResult:
        selector = work.discovery_selector
        if selector is None and work.target is not None:
            selector = ScopeSelector(
                tenant_id=self.context.tenant_id, kind="item",
                workspace_id=work.target.workspace_id, item_id=work.target.item_id,
            )
        if selector is None:
            raise MonitoringConflict("The controller must expand saved-scope inventory into explicit selectors before dispatch")
        generation = await asyncio.to_thread(
            self.store.get_inventory_generation, self.context, work.work_id,
        )
        if generation is None:
            generation = InventoryGeneration(
                **self.context.model_dump(), generation_id=work.work_id, selector=selector,
                adapter=self.inventory_client.adapter, authority=self.inventory_client.authority,
                completeness="partial", started_at=self._clock(),
                continuation=self.inventory_client.initial_cursor(self.context, selector, work.work_id),
                gaps=(CoverageGap(code="inventory_in_progress", detail="Inventory generation is still being collected"),),
            )
            generation = await self._record_inventory(InventoryBatch(
                request_id=_id(work.work_id, "start"), expected=await self._version(),
                generation=generation, items=(), workspaces=(), domains=(),
                commit=self._inventory_commit(work, None),
            ))
        if generation.selector != selector:
            raise MonitoringConflict("Inventory work and its durable generation select different resources")
        pages = 0
        retry_at: datetime | None = None
        for _ in range(self._pages_per_work):
            if generation.completed_at is not None:
                break
            work = await self._renew(work)
            try:
                items, workspaces, domains = await self._inventory_records(generation)
            except RestReadError as exc:
                gap = exc.gap()
                failed = generation.model_copy(update={
                    "completeness": "partial", "completed_at": None,
                    "gaps": bounded_gaps((*generation.gaps, gap)),
                })
                generation = await self._record_inventory(InventoryBatch(
                    request_id=_id(work.work_id, f"catalogue-gap:{generation.revision}"),
                    expected=await self._version(), generation=failed, items=(),
                    commit=self._inventory_commit(work, generation),
                ))
                await self._disposition(
                    work, "Generation catalogue is incomplete; no empty membership was inferred",
                    retry_at=self._clock() + timedelta(seconds=self._retry_seconds),
                )
                return CollectorWorkResult(work.work_id, "deferred", pages=pages, gaps=generation.gaps)
            now = self._clock()
            result = await self.inventory_client.read_page(
                self.context, selector, generation_id=generation.generation_id, observed_at=now,
                continuation=generation.continuation, workspaces=workspaces, domains=domains, known_items=items,
            )
            work = await self._renew(work)
            gaps = bounded_gaps((
                *(gap for gap in generation.gaps if gap.code not in _TRANSIENT_INVENTORY_GAPS),
                *result.gaps,
            ))
            if not result.finished and not gaps:
                gaps = (CoverageGap(code="inventory_in_progress", detail="Inventory has a durable continuation"),)
            blocking = tuple(gap for gap in gaps if gap.code != "unsupported_item_type")
            updated = InventoryGeneration(
                **self.context.model_dump(), generation_id=generation.generation_id,
                selector=selector, adapter=generation.adapter, authority=generation.authority,
                completeness="complete" if result.finished and not blocking else "partial",
                started_at=generation.started_at, completed_at=now if result.finished else None,
                continuation=result.continuation,
                discovered_count=len({
                    (item.workspace_id, item.item_id) for item in (*items, *result.items)
                }),
                completed_pages=generation.completed_pages + result.completed_pages, gaps=gaps,
                revision=generation.revision,
                next_scan_at=now + timedelta(seconds=self._inventory_seconds) if result.finished else None,
            )
            batch = InventoryBatch(
                request_id=_id(work.work_id, f"page:{hashlib.sha256(updated.model_dump_json().encode()).hexdigest()}"),
                expected=await self._version(), generation=updated, items=result.items,
                workspaces=result.workspaces, domains=result.domains,
                commit=self._inventory_commit(work, generation),
            )
            generation = await self._record_inventory(batch)
            pages += result.completed_pages
            if result.retry_at is not None or any(
                gap.code in _TRANSIENT_INVENTORY_GAPS - {"inventory_in_progress"} for gap in result.gaps
            ):
                retry_at = result.retry_at or now + timedelta(seconds=self._retry_seconds)
                break
        if generation.completed_at is None:
            await self._disposition(
                work, "Inventory coverage is partial; resume its durable continuation",
                retry_at=retry_at or self._clock() + timedelta(seconds=15),
            )
            return CollectorWorkResult(work.work_id, "deferred", pages=pages, gaps=generation.gaps)
        await self._disposition(work, "Inventory and its bounded next-scan request are durable; controller publication is separate")
        return CollectorWorkResult(work.work_id, "recorded", pages=pages, gaps=generation.gaps)

    def _history(self, target: TargetIdentity) -> _HistoryClient:
        return self.pipeline_client if target.workload == "fabric_pipeline" else self.powerbi_client

    async def _capability(self, work: MonitoringWork) -> CollectorWorkResult:
        if work.target is None:
            raise MonitoringConflict("Capability work has no target")
        items = await self._records(
            self.store.list_inventory,
            TargetQuery(**self.context.model_dump(), workspace_id=work.target.workspace_id, limit=1_000),
        )
        item = next((value for value in items if value.target == work.target), None)
        if item is None:
            await self._disposition(work, "Capability target is not in current inventory")
            return CollectorWorkResult(work.work_id, "superseded")
        work = await self._renew(work)
        probe = await self._history(work.target).probe(
            work.target, inventory_generation=item.generation_id, checked_at=self._clock(),
        )
        work = await self._renew(work)
        await asyncio.to_thread(
            self.store.record_capability, await self._version(), probe,
            commit=CollectionCommit(work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision),
        )
        if probe.read_status != "verified":
            retry_at = max(
                [self._clock() + timedelta(seconds=self._retry_seconds)]
                + [gap.retry_at for gap in probe.gaps if gap.retry_at is not None],
            )
            await self._disposition(work, "Collector source capability remains unverified", retry_at=retry_at)
            return CollectorWorkResult(work.work_id, "deferred", gaps=probe.gaps)
        await self._disposition(work, "Collector source evidence is durable; only the controller publishes capability")
        return CollectorWorkResult(work.work_id, "recorded", gaps=probe.gaps)

    async def _record_rest(self, request: RestPageRequest) -> RestPageReceipt:
        try:
            return await asyncio.to_thread(self.store.record_rest_page, request)
        except MonitoringCommitUncertain:
            receipt = await asyncio.to_thread(self.store.get_rest_page, self.context, request.page_id)
            if receipt is None:
                raise
            if (
                receipt.checkpoint.target != request.target
                or receipt.checkpoint.last_page_id != request.page_id
                or receipt.checkpoint.window != request.window
            ):
                raise MonitoringConflict("REST acceptance receipt belongs to another page") from None
            return receipt

    async def _read_failure(
        self, work: MonitoringWork, target: MonitoringTarget, window: ObservationWindow,
        checkpoint: RestCheckpoint | None, error: RestReadError, now: datetime,
    ) -> CoverageGap:
        gap = error.gap(target.identity.workspace_id, target.identity.item_id)
        state = _PollCursor.model_validate_json(checkpoint.cursor) if (
            checkpoint is not None and checkpoint.cursor is not None
        ) else _PollCursor(target_key=target.key, window=window)
        following = state.model_copy(update={
            "retry_serial": state.retry_serial + 1, "last_read_gap": gap,
        })
        revision = checkpoint.revision if checkpoint is not None else 0
        # A failed request has no source rows to admit. Persist its explicit gap
        # and unchanged source position, not a fictitious empty completed window
        # or renewed read-capability proof from an HTTP failure.
        await self._record_rest(RestPageRequest(
            page_id=_id(work.work_id, f"read-gap:{revision}:{following.encode()}"),
            target=target.identity, policy_revision=target.policy_revision,
            poll_work_id=work.work_id, lease=work.lease,
            expected_checkpoint_revision=revision,
            expected_cursor=checkpoint.cursor if checkpoint is not None else None,
            next_cursor=following.encode(), window=window, received_count=0,
            observed_at=now, window_complete=False,
        ))
        retry_at = max(error.retry_at or now, now + timedelta(seconds=self._retry_seconds))
        await self._disposition(
            work, f"REST coverage gap {error.code}: {error.detail}", retry_at=retry_at,
        )
        logger.warning("Collector REST coverage gap (%s, %s)", work.work_id, error.code)
        return gap

    async def _poll(self, work: MonitoringWork) -> CollectorWorkResult:
        if work.target is None:
            raise MonitoringConflict("Poll work has no target")
        target = await asyncio.to_thread(self.store.resolve_target, work.target)
        if target is None or not target.observation.enabled:
            await self._disposition(work, "Poll target no longer has current observation admission")
            return CollectorWorkResult(work.work_id, "superseded")
        progress = await asyncio.to_thread(self.store.get_poll_progress, target.identity)
        checkpoint = progress.checkpoint if progress is not None else None
        validated = await asyncio.to_thread(self.store.get_rest_checkpoint, target.identity)
        snapshot = await asyncio.to_thread(self.store.snapshot, self.context)
        start_at = work.due_at - timedelta(seconds=self._lookback_seconds)
        if validated is not None and validated.coverage_through is not None:
            start_at = min(start_at, validated.coverage_through)
        window = checkpoint.window if checkpoint is not None and checkpoint.cursor is not None else ObservationWindow(
            start_at=max(snapshot.control.activation_cutoff, start_at),
            end_at=max(work.due_at, snapshot.control.activation_cutoff),
        )
        pages = 0
        observations = 0
        gaps: list[CoverageGap] = []
        # A final page committed before a process crash does not start a fresh
        # window under the same due-work cycle on replay.
        already_recorded = (
            checkpoint is not None and checkpoint.cursor is None and checkpoint.window == window
        )
        if already_recorded:
            await self._disposition(work, "This poll cycle already has a durable REST window disposition")
            return CollectorWorkResult(work.work_id, "recorded")
        if not already_recorded:
            for _ in range(self._pages_per_work):
                work = await self._renew(work)
                now = self._clock()
                try:
                    page = await self._history(target.identity).read_page(
                        target.identity, window, observed_at=now,
                        cursor=checkpoint.cursor if checkpoint is not None else None,
                    )
                except RestReadError as exc:
                    gap = await self._read_failure(work, target, window, checkpoint, exc, now)
                    return CollectorWorkResult(
                        work.work_id, "deferred", pages, observations, bounded_gaps([*gaps, gap]),
                    )
                expected_revision = checkpoint.revision if checkpoint is not None else 0
                expected_cursor = checkpoint.cursor if checkpoint is not None else None
                request = RestPageRequest(
                    page_id=_id(
                        work.work_id,
                        f"rest:{expected_revision}:{window.model_dump_json()}:{page.source_page_hash}",
                    ),
                    target=target.identity, policy_revision=target.policy_revision,
                    poll_work_id=work.work_id, lease=work.lease,
                    expected_checkpoint_revision=expected_revision,
                    expected_cursor=expected_cursor, next_cursor=page.next_cursor, window=window,
                    received_count=page.received_count, observations=page.observations,
                    powerbi_rows=page.powerbi_rows, powerbi_window_complete=page.powerbi_window_complete,
                    quarantines=page.quarantines, window_complete=page.window_complete,
                    retention_exhausted=page.retention_exhausted, observed_at=now,
                )
                receipt = await self._record_rest(request)
                checkpoint = receipt.checkpoint
                pages += 1
                observations += len(page.observations) + len(page.powerbi_rows)
                gaps.extend(page.gaps)
                if receipt.powerbi_window is not None:
                    gaps.extend(receipt.powerbi_window.gaps)
                if page.next_cursor is None:
                    # The final intake transaction finishes collection. Publishing
                    # source authority and the next poll remains controller work.
                    finished = await asyncio.to_thread(self.store.get_work, self.context, work.work_id)
                    if finished is None or finished.state != "completed":
                        raise MonitoringConflict("Final REST acceptance did not durably complete its poll work")
                    return CollectorWorkResult(
                        work.work_id, "recorded", pages, observations, bounded_gaps(gaps),
                    )
        await self._disposition(
            work, "REST page acceptance is durable; resume continuation",
            retry_at=self._clock() + timedelta(seconds=15),
        )
        return CollectorWorkResult(
            work.work_id, "deferred", pages, observations, bounded_gaps(gaps),
        )

    async def _collect(self, work: MonitoringWork) -> CollectorWorkResult:
        try:
            work = await self._renew(work)
            if work.kind == "inventory":
                return await self._inventory(work)
            if work.kind == "capability_probe":
                return await self._capability(work)
            return await self._poll(work)
        except (MonitoringConflict, MonitoringLeaseLost) as exc:
            frames = traceback.extract_tb(exc.__traceback__)
            location = f"{frames[-1].name}:{frames[-1].lineno}" if frames else "unknown"
            logger.warning("Collector work fenced (%s, %s, %s)", work.work_id, type(exc).__name__, location)
            return CollectorWorkResult(work.work_id, "lease_lost", gaps=(CoverageGap(
                code="collector_work_fenced", detail="Current shared ownership or policy changed; work was not finalized",
            ),))

    async def run_once(self) -> CollectorRunResult:
        snapshot = await asyncio.to_thread(self.store.snapshot, self.context)
        if snapshot.control.maintenance:
            return CollectorRunResult(0, gaps=(CoverageGap(
                code="monitoring_maintenance", detail="Monitoring intake is paused by deployment control",
            ),))
        work = await asyncio.to_thread(self.store.claim_work, self._claim)
        results = await asyncio.gather(*(self._collect(value) for value in work), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                if isinstance(result, MonitoringStoreError):
                    logger.error("Collector shared state failed closed (%s)", type(result).__name__)
                raise result
        return CollectorRunResult(len(work), tuple(results))
