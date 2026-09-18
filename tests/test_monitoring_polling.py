from __future__ import annotations

import asyncio
import json
import threading
from collections import Counter
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from test_monitoring_inventory import (
    CHILD,
    CONTEXT,
    DOMAIN,
    GENERATION,
    IDENTITY,
    ITEM,
    NOW,
    RULE,
    SCOPE,
    TENANT,
    WORKSPACE,
    Clock,
    Credential,
)

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringCommitUncertain, MonitoringUnavailable
from triage.monitoring.inventory import (
    FabricInventoryClient,
    InventoryApiOptions,
    RestReadError,
    TenantBoundRestClient,
)
from triage.monitoring.memory import InMemoryMonitoringState, InMemoryMonitoringStore
from triage.monitoring.polling import (
    FabricPipelinePollingClient,
    MonitoringCollector,
    PowerBIPollingClient,
    normalize_pipeline_run,
    normalize_powerbi_run,
    resolve_powerbi_execution,
)
from triage.monitoring.rate_limit import InMemoryRateBudget, RatePolicy

RUN = str(UUID(int=500))
OWNER = str(UUID(int=501))
OTHER_OWNER = str(UUID(int=502))
REQUEST = str(UUID(int=503))


def target(workload="fabric_pipeline", **changes):
    return m.TargetIdentity.model_validate({
        **CONTEXT.model_dump(), "workload": workload, "workspace_id": WORKSPACE,
        "item_id": ITEM, **changes,
    })


def pipeline_row(**changes):
    return {
        "id": RUN, "itemId": ITEM, "status": "Failed", "jobType": "Pipeline",
        "invokeType": "Scheduled", "startTimeUtc": (NOW - timedelta(minutes=5)).isoformat(),
        "endTimeUtc": (NOW - timedelta(minutes=1)).isoformat(),
        "failureReason": {"errorCode": "SourceReadFailed", "message": "Synthetic source failure"},
        **changes,
    }


def powerbi_row(**changes):
    return {
        "id": 23, "requestId": RUN, "status": "Failed", "refreshType": "Scheduled",
        "startTime": (NOW - timedelta(minutes=5)).isoformat(),
        "endTime": (NOW - timedelta(minutes=1)).isoformat(),
        "serviceExceptionJson": json.dumps({"errorCode": "SourceReadFailed", "message": "Synthetic source failure"}),
        **changes,
    }


def window(*, seconds=86_400):
    return m.ObservationWindow(start_at=NOW - timedelta(seconds=seconds), end_at=NOW)


@pytest.fixture
async def rest_factory():
    clients = []

    def create(handler, *, clock=None, budget=None, **kwargs):
        clock = clock or Clock()
        rest = TenantBoundRestClient(
            CONTEXT, IDENTITY, Credential(clock), budget or InMemoryRateBudget(clock=clock),
            transport=httpx.MockTransport(handler), clock=clock, **kwargs,
        )
        clients.append(rest)
        return rest

    yield create
    for rest in clients:
        await rest.close()


@pytest.mark.parametrize(
    ("status", "invocation", "normalized_status", "normalized_invocation", "eligible"),
    [
        ("Failed", "Scheduled", "failed", "scheduled", True),
        ("Failed", "Manual", "failed", "manual", False),
        ("Failed", "OnDemand", "failed", "manual", False),
        ("Cancelled", "Scheduled", "cancelled", "scheduled", False),
        ("Completed", "Scheduled", "succeeded", "scheduled", False),
        ("InProgress", "Scheduled", "running", "scheduled", False),
        ("NotStarted", "Scheduled", "not_started", "scheduled", False),
        ("Deduped", "Scheduled", "unknown", "scheduled", False),
        ("Failed", "UnexpectedInvocation", "failed", "unknown", False),
    ],
)
def test_pipeline_status_and_invocation_are_real_fields(
    status, invocation, normalized_status, normalized_invocation, eligible,
):
    observation, gaps = normalize_pipeline_run(
        pipeline_row(status=status, invokeType=invocation, scheduleEnabled=True),
        target(), observed_at=NOW,
    )
    assert observation.status == normalized_status and observation.invocation == normalized_invocation
    assert observation.failed_scheduled_pipeline is eligible
    assert observation.execution.run_id_kind == "fabric_job"
    assert observation.origin == "poll" and observation.authority == "rest"
    assert bool(gaps) is (normalized_status == "unknown" or normalized_invocation == "unknown")


@pytest.mark.parametrize("job_type", ["Pipeline", "Execute"])
def test_both_supported_pipeline_job_types_preserve_exact_failure_evidence(job_type):
    observation, gaps = normalize_pipeline_run(pipeline_row(jobType=job_type), target(), observed_at=NOW)
    assert observation.job_type == job_type and observation.failed_scheduled_pipeline
    assert observation.error_code == "SourceReadFailed" and not gaps


@pytest.mark.parametrize("field", ["id", "itemId", "status", "jobType", "invokeType", "endTimeUtc", "startTimeUtc"])
def test_pipeline_missing_evidence_is_not_admitted(field):
    raw = pipeline_row()
    del raw[field]
    with pytest.raises(RestReadError):
        normalize_pipeline_run(raw, target(), observed_at=NOW)


@pytest.mark.parametrize(
    "changes",
    [
        {"itemId": str(UUID(int=999))}, {"workspaceId": str(UUID(int=999))},
        {"tenantId": str(UUID(int=999))}, {"jobType": "Notebook"},
        {"status": "NewUnrecognizedState"}, {"failureReason": "unreadable"},
        {"startTimeUtc": (NOW + timedelta(days=1)).isoformat()},
    ],
)
def test_pipeline_malformed_wrong_target_or_unsupported_evidence_fails_closed(changes):
    with pytest.raises((RestReadError, ValueError)):
        normalize_pipeline_run(pipeline_row(**changes), target(), observed_at=NOW)


def test_powerbi_aliases_normalize_to_request_id_without_timestamp_matching():
    source = target("powerbi")
    raw = powerbi_row(id="00023", requestId=RUN.upper())
    observation, gaps = normalize_powerbi_run(raw, source, observed_at=NOW)
    numeric = resolve_powerbi_execution(source, "23", "powerbi_refresh", [raw])
    request = resolve_powerbi_execution(source, RUN, "powerbi_request", [raw])
    assert observation.execution == numeric == request
    assert numeric.run_id_kind == "powerbi_request" and numeric.run_id == RUN and not gaps
    assert observation.evidence["refresh_id"] == "23"
    with pytest.raises(RestReadError, match="exact Power BI"):
        resolve_powerbi_execution(
            source, str(UUID(int=800)), "powerbi_request",
            [powerbi_row(startTime=raw["startTime"], endTime=raw["endTime"])],
        )


@pytest.mark.parametrize(
    "rows",
    [
        [powerbi_row(requestId=None)],
        [powerbi_row(), powerbi_row(requestId=str(UUID(int=801)))],
        [powerbi_row(), powerbi_row(id=24)],
    ],
)
def test_powerbi_unproven_or_conflicting_alias_is_never_admitted(rows):
    with pytest.raises(RestReadError, match="alias|identities"):
        resolve_powerbi_execution(target("powerbi"), "23", "powerbi_refresh", rows)


@pytest.mark.parametrize(
    ("status", "refresh_type", "expected_status", "expected_invocation"),
    [
        ("Completed", "Scheduled", "succeeded", "scheduled"),
        ("Failed", "OnDemand", "failed", "manual"),
        ("Cancelled", "ViaApi", "cancelled", "manual"),
        ("Unknown", "Scheduled", "unknown", "scheduled"),
        ("Disabled", "Scheduled", "unknown", "scheduled"),
        ("Failed", "ViaEnhancedApi", "failed", "manual"),
        ("Failed", "NewRefreshType", "failed", "unknown"),
    ],
)
def test_powerbi_does_not_infer_state_or_invocation_from_schedule(
    status, refresh_type, expected_status, expected_invocation,
):
    observation, _ = normalize_powerbi_run(
        powerbi_row(status=status, refreshType=refresh_type, scheduleEnabled=True),
        target("powerbi"), observed_at=NOW,
    )
    assert observation.status == expected_status and observation.invocation == expected_invocation


@pytest.mark.parametrize(
    "changes",
    [
        {"status": None}, {"refreshType": None}, {"id": True}, {"requestId": "missing"},
        {"id": None, "requestId": None}, {"serviceExceptionJson": "{} bad"},
        {"endTime": None}, {"serviceExceptionJson": '{"errorCode": 123}'},
    ],
)
def test_powerbi_malformed_run_is_explicit(changes):
    with pytest.raises(RestReadError):
        normalize_powerbi_run(powerbi_row(**changes), target("powerbi"), observed_at=NOW)


async def test_pipeline_history_exhausts_valid_pages_and_preserves_cancelled_manual_jobs(rest_factory):
    requests = []

    def handler(request):
        requests.append(request)
        assert request.method == "GET"
        if "continuationToken" not in request.url.params:
            return httpx.Response(200, json={
                "value": [pipeline_row()],
                "continuationToken": "two",
                "continuationUri": f"https://api.fabric.microsoft.com{request.url.path}?continuationToken=two",
            })
        return httpx.Response(200, json={"value": [
            pipeline_row(id=str(UUID(int=601)), status="Cancelled"),
            pipeline_row(id=str(UUID(int=602)), invokeType="Manual"),
        ]})

    client = FabricPipelinePollingClient(rest_factory(handler))
    first = await client.read_page(target(), window(), observed_at=NOW)
    assert first.next_cursor is not None and not first.window_complete
    second = await client.read_page(target(), window(), observed_at=NOW, cursor=first.next_cursor)
    assert second.next_cursor is None and second.window_complete
    assert [row.status for row in second.observations] == ["cancelled", "failed"]
    assert not any(row.failed_scheduled_pipeline for row in second.observations)
    assert len(requests) == 2


@pytest.mark.parametrize("workload,retained", [("fabric_pipeline", 100), ("powerbi", 60)])
async def test_retained_window_exhaustion_is_not_claimed_as_full_lookback(rest_factory, workload, retained):
    rows = [
        pipeline_row(id=str(UUID(int=1_000 + index))) if workload == "fabric_pipeline" else
        powerbi_row(id=index + 1, requestId=str(UUID(int=1_000 + index)))
        for index in range(retained)
    ]
    requests = []
    rest = rest_factory(lambda request: requests.append(request) or httpx.Response(200, json={"value": rows}))
    client = FabricPipelinePollingClient(rest) if workload == "fabric_pipeline" else PowerBIPollingClient(rest)
    result = await client.read_page(target(workload), window(), observed_at=NOW)
    assert result.retention_exhausted and not result.window_complete
    assert "retention_exhausted" in {gap.code for gap in result.gaps}
    observation = result.observations[0] if workload == "fabric_pipeline" else result.powerbi_rows[0].observation
    assert observation.evidence["poll_coverage"]["retention_exhausted"]
    if workload == "powerbi":
        assert requests[0].url.params["$top"] == "60"


async def test_powerbi_odata_paging_carries_retention_state_across_restart(rest_factory):
    def handler(request):
        page = 1 if "$skip" in request.url.params else 0
        rows = [
            powerbi_row(id=1 + index, requestId=str(UUID(int=1_000 + index)))
            for index in range(page * 30, (page + 1) * 30)
        ]
        return httpx.Response(200, json={
            "value": rows,
            **({"@odata.nextLink": f"https://api.powerbi.com{request.url.path}?$skip=30"} if page == 0 else {}),
        })

    rest = rest_factory(handler)
    first = await PowerBIPollingClient(rest).read_page(target("powerbi"), window(), observed_at=NOW)
    assert not first.retention_exhausted and not first.window_complete
    second = await PowerBIPollingClient(rest).read_page(
        target("powerbi"), window(), observed_at=NOW, cursor=first.next_cursor,
    )
    assert second.retention_exhausted and second.received_count == 30


async def test_retention_count_deduplicates_runs_and_only_counts_terminal_jobs(rest_factory):
    rows = [pipeline_row(id=str(UUID(int=1_000 + index))) for index in range(50)]
    page = 0

    def handler(request):
        nonlocal page
        page += 1
        return httpx.Response(200, json={
            "value": rows, **({"continuationToken": "two"} if page == 1 else {}),
        })

    client = FabricPipelinePollingClient(rest_factory(handler))
    first = await client.read_page(target(), window(), observed_at=NOW)
    second = await client.read_page(target(), window(), observed_at=NOW, cursor=first.next_cursor)
    assert not second.retention_exhausted and second.window_complete
    running_client = FabricPipelinePollingClient(rest_factory(lambda _: httpx.Response(
        200, json={"value": [pipeline_row(id=str(UUID(int=index + 5_000)), status="InProgress", endTimeUtc=None)
                            for index in range(100)]},
    )))
    running = await running_client.read_page(target(), window(), observed_at=NOW)
    assert not running.retention_exhausted


async def test_reaching_the_requested_window_is_not_a_retention_gap(rest_factory):
    rows = [
        pipeline_row(
            id=str(UUID(int=index + 1_000)),
            startTimeUtc=(NOW - timedelta(days=2, minutes=5)).isoformat(),
            endTimeUtc=(NOW - timedelta(days=2)).isoformat(),
        ) for index in range(100)
    ]
    client = FabricPipelinePollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": rows})))
    result = await client.read_page(target(), window(), observed_at=NOW)
    assert not result.retention_exhausted and result.window_complete


async def test_partial_page_dispositions_cover_every_received_row(rest_factory):
    rows = [pipeline_row(), {"id": RUN}, "unreadable", pipeline_row(id=str(UUID(int=999)), jobType="Notebook")]
    client = FabricPipelinePollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": rows})))
    result = await client.read_page(target(), window(), observed_at=NOW)
    assert result.received_count == 4
    assert len(result.observations) == 1 and len(result.quarantines) == 3
    assert not result.window_complete
    assert {row.reason for row in result.quarantines} == {"malformed", "unsupported"}


async def test_alias_only_duplicates_use_one_source_execution_before_admission(rest_factory):
    rows = [powerbi_row(requestId=None), powerbi_row()]
    client = PowerBIPollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": rows})))
    result = await client.read_page(target("powerbi"), window(), observed_at=NOW)
    assert result.observations == () and len(result.powerbi_rows) == 2 and not result.quarantines
    store, _, _ = make_store(targets=(target("powerbi"),))
    work = claim_poll(store)
    receipt = commit_history_page(store, work, result, request_id=str(UUID(int=40_001)))
    assert receipt.powerbi_window.state == "validated"
    assert len(receipt.intake.work_ids) == 1
    assert store.get_work(CONTEXT, receipt.intake.work_ids[0]).execution.run_id == RUN


async def test_conflicting_aliases_quarantine_all_candidates(rest_factory):
    rows = [powerbi_row(), powerbi_row(requestId=str(UUID(int=999)))]
    client = PowerBIPollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": rows})))
    result = await client.read_page(target("powerbi"), window(), observed_at=NOW)
    assert result.observations == () and len(result.powerbi_rows) == 2
    store, _, _ = make_store(targets=(target("powerbi"),))
    receipt = commit_history_page(store, claim_poll(store), result, request_id=str(UUID(int=40_002)))
    assert receipt.powerbi_window.state == "quarantined"
    assert receipt.powerbi_window.quarantined_count == 2
    assert receipt.intake.work_ids == () and receipt.checkpoint.coverage_through is None


async def test_another_tenant_row_cannot_supply_a_missing_refresh_alias(rest_factory):
    rows = [powerbi_row(requestId=None), powerbi_row(tenantId=str(UUID(int=999)))]
    client = PowerBIPollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": rows})))
    result = await client.read_page(target("powerbi"), window(), observed_at=NOW)
    assert result.observations == () and len(result.powerbi_rows) == 1
    assert {value.reason for value in result.quarantines} == {"wrong_tenant"}
    store, _, _ = make_store(targets=(target("powerbi"),))
    receipt = commit_history_page(store, claim_poll(store), result, request_id=str(UUID(int=40_003)))
    assert receipt.powerbi_window.state == "quarantined"
    assert receipt.powerbi_window.quarantined_count == 2
    assert receipt.intake.work_ids == ()


async def test_long_api_page_uses_200_row_atomic_chunks_and_replays_changed_prefix(rest_factory):
    rows = [pipeline_row(id=str(UUID(int=index + 1_000))) for index in range(201)]
    client = FabricPipelinePollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": rows})))
    first = await client.read_page(target(), window(), observed_at=NOW)
    assert first.received_count == 200 and first.next_cursor is not None
    second = await client.read_page(target(), window(), observed_at=NOW, cursor=first.next_cursor)
    assert second.received_count == 1 and second.next_cursor is None
    rows.insert(0, pipeline_row(id=str(UUID(int=9_999))))
    changed = await client.read_page(target(), window(), observed_at=NOW, cursor=first.next_cursor)
    assert changed.received_count == 200 and changed.observations[0].execution.run_id == str(UUID(int=9_999))
    assert "source_page_changed" in {gap.code for gap in changed.gaps}
    assert not changed.window_complete


@pytest.mark.parametrize(
    "extra",
    [
        {"@odata.nextLink": "https://example.invalid/other"},
        {"continuationUri": f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE}/items/{str(UUID(int=999))}/jobs/instances?continuationToken=x"},
        {"hasMore": True},
    ],
)
async def test_untrusted_pagination_does_not_silently_accept_a_healthy_empty_page(rest_factory, extra):
    client = FabricPipelinePollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": [], **extra})))
    with pytest.raises(RestReadError):
        await client.read_page(target(), window(), observed_at=NOW)


async def test_pagination_cycle_is_detected_across_worker_restarts(rest_factory):
    client = FabricPipelinePollingClient(rest_factory(lambda _: httpx.Response(
        200, json={"value": [pipeline_row()], "continuationToken": "repeat"},
    )))
    first = await client.read_page(target(), window(), observed_at=NOW)
    with pytest.raises(RestReadError, match="repeated"):
        await FabricPipelinePollingClient(client.rest).read_page(
            target(), window(), observed_at=NOW, cursor=first.next_cursor,
        )


@pytest.mark.parametrize("workload", ["fabric_pipeline", "powerbi"])
async def test_probe_uses_collector_identity_and_cannot_prove_action_or_event_access(rest_factory, workload):
    calls = []

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        if request.url.path.endswith(("instances", "refreshes")):
            return httpx.Response(200, json={"value": []})
        return httpx.Response(200, json={
            "id": ITEM, "workspaceId": WORKSPACE, "type": "DataPipeline",
            "displayName": "Pipeline", "name": "Model",
        })

    rest = rest_factory(handler)
    client = FabricPipelinePollingClient(rest) if workload == "fabric_pipeline" else PowerBIPollingClient(rest)
    probe = await client.probe(target(workload), inventory_generation=GENERATION, checked_at=NOW)
    assert probe.collector_identity_id == IDENTITY and probe.read_status == "verified"
    assert probe.action_status == probe.event_status == "unknown"
    assert not probe.exact_action_correlation and probe.definition_hash is None
    assert len(calls) == 2
    if workload == "powerbi":
        assert "Write" in probe.required_permissions[0]


@pytest.mark.parametrize("status", [401, 403, 429])
async def test_probe_denial_and_throttling_are_explicit(rest_factory, status):
    client = FabricPipelinePollingClient(rest_factory(lambda _: httpx.Response(
        status, headers={"Retry-After": "700"},
    )))
    probe = await client.probe(target(), inventory_generation=GENERATION, checked_at=NOW)
    assert probe.read_status == ("denied" if status in {401, 403} else "blocked")
    assert probe.gaps and probe.action_status == "unknown"
    if status == 429:
        assert probe.gaps[0].retry_at == NOW + timedelta(seconds=700)


async def test_probe_refuses_conflicting_workspace_metadata(rest_factory):
    client = PowerBIPollingClient(rest_factory(lambda _: httpx.Response(200, json={
        "id": ITEM, "name": "Model", "workspaceId": str(UUID(int=999)),
    })))
    probe = await client.probe(target("powerbi"), inventory_generation=GENERATION, checked_at=NOW)
    assert probe.read_status == "blocked" and probe.gaps[0].code == "wrong_target"


async def test_authorized_definition_reader_hashes_content_without_inferring_action(rest_factory):
    class Reader:
        context = CONTEXT
        collector_identity_id = IDENTITY

        async def read(self, target):
            return {"parts": [{"path": "pipeline-content.json", "payload": "fixture-business-content"}]}

    def handler(request):
        return httpx.Response(200, json={"value": []} if request.url.path.endswith("instances") else {
            "id": ITEM, "workspaceId": WORKSPACE, "type": "DataPipeline", "displayName": "Pipeline",
        })

    rest = rest_factory(handler)
    probe = await FabricPipelinePollingClient(rest, definition_reader=Reader()).probe(
        target(), inventory_generation=GENERATION, checked_at=NOW,
    )
    assert len(probe.definition_hash) == 64 and probe.action_status == "unknown"
    reader = Reader()
    reader.collector_identity_id = str(UUID(int=999))
    with pytest.raises(ValueError, match="same pinned"):
        FabricPipelinePollingClient(rest, definition_reader=reader)


class TrackingStore(InMemoryMonitoringStore):
    def __init__(self, *, clock, state):
        super().__init__(clock=clock, state=state)
        self.requests = []
        self.claims = []
        self.worker_threads = []
        self.capture_threads = False
        self.uncertain_before = False
        self.uncertain_after = False
        self.unavailable = False

    def claim_work(self, request):
        self.claims.append(request)
        if self.capture_threads:
            self.worker_threads.append(threading.get_ident())
        return super().claim_work(request)

    def record_rest_page(self, request):
        self.requests.append(request)
        if self.capture_threads:
            self.worker_threads.append(threading.get_ident())
        if self.unavailable:
            raise MonitoringUnavailable("Fixture SQL outage")
        if self.uncertain_before:
            self.uncertain_before = False
            raise MonitoringCommitUncertain("rest_page", request.page_id)
        receipt = super().record_rest_page(request)
        if self.uncertain_after:
            self.uncertain_after = False
            raise MonitoringCommitUncertain("rest_page", request.page_id)
        return receipt


def make_store(*, clock=None, targets=None):
    clock = clock or Clock()
    targets = tuple(targets or (target(),))
    control = m.DeploymentControl(
        **CONTEXT.model_dump(), revision=0, activation_cutoff=NOW - timedelta(days=3),
        maintenance=False, updated_at=NOW,
    )
    state = InMemoryMonitoringState.empty(control)
    store = TrackingStore(clock=clock, state=state)
    version = m.RegistryVersion(**CONTEXT.model_dump(), revision=0)
    items = tuple(m.InventoryItem(
        **CONTEXT.model_dump(), generation_id=GENERATION,
        workspace_id=value.workspace_id, item_id=value.item_id, name=f"Fixture {index}",
        item_type="DataPipeline" if value.workload == "fabric_pipeline" else "Dataset",
        workload=value.workload, observed_at=NOW,
    ) for index, value in enumerate(targets))
    store.record_inventory(m.InventoryBatch(
        request_id=REQUEST, expected=version,
        generation=m.InventoryGeneration(
            **CONTEXT.model_dump(), generation_id=GENERATION,
            selector=m.ScopeSelector(tenant_id=TENANT, kind="tenant"), adapter="explicit_fixture",
            authority="fixture", completeness="complete", started_at=NOW, completed_at=NOW,
            discovered_count=len(items), completed_pages=1,
        ), items=items,
    ))
    for index, value in enumerate(targets):
        store.record_capability(version, m.CapabilityObservation(
            capability_id=str(UUID(int=index + 10_000)), target=value, inventory_generation=GENERATION,
            collector_identity_id=IDENTITY, read_status="verified", checked_at=NOW,
            expires_at=NOW + timedelta(hours=1),
        ))
    plan = store.preview_scope(m.ScopePreviewRequest(
        expected=version, idempotency_id=str(UUID(int=20_000)),
        scope=m.ScopeDefinition(
            **CONTEXT.model_dump(), scope_id=SCOPE, name="Fixture admitted inventory",
            rules=(m.ScopeRule(
                rule_id=RULE, selector=m.ScopeSelector(tenant_id=TENANT, kind="tenant"),
                effect="include", auto_enrol_detection_only=True,
            ),),
        ),
    ))
    store.activate_scope(m.ActivateScopeRequest(
        expected=version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    for work in store.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("capability_probe",),
        limit=200, per_workspace_limit=200,
    )):
        store.complete_collection_work(
            CONTEXT, work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
        )
    store.claims.clear()
    store.capture_threads = True
    return store, state, clock


def collector(store, rest, clock, **kwargs):
    return MonitoringCollector(
        store, CONTEXT, FabricInventoryClient(rest),
        FabricPipelinePollingClient(rest), PowerBIPollingClient(rest),
        IDENTITY, kwargs.pop("owner_id", OWNER), clock=clock, **kwargs,
    )


def claim_poll(store):
    return store.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("poll",), limit=1, per_workspace_limit=1,
    ))[0]


async def test_renewal_reads_the_sql_advanced_work_revision(rest_factory, monkeypatch):
    store, _, clock = make_store()
    work = claim_poll(store)
    lease = work.lease.model_copy(update={"expires_at": work.lease.expires_at + timedelta(seconds=30)})
    refreshed = work.model_copy(update={"lease": lease, "revision": work.revision + 1})
    monkeypatch.setattr(store, "renew_lease", lambda request: lease)
    monkeypatch.setattr(store, "get_work", lambda context, work_id: refreshed)
    service = collector(store, rest_factory(lambda request: httpx.Response(200, json={})), clock)
    result = await service._renew(work)
    assert result == refreshed and result.revision == work.revision + 1


def commit_history_page(store, work, page, *, request_id, checkpoint=None):
    return store.record_rest_page(m.RestPageRequest(
        page_id=request_id, target=work.target, policy_revision=work.policy_revision,
        poll_work_id=work.work_id, lease=work.lease,
        expected_checkpoint_revision=checkpoint.revision if checkpoint else 0,
        expected_cursor=checkpoint.cursor if checkpoint else None,
        next_cursor=page.next_cursor, window=window(), received_count=page.received_count,
        observations=page.observations, powerbi_rows=page.powerbi_rows,
        powerbi_window_complete=page.powerbi_window_complete,
        quarantines=page.quarantines, window_complete=page.window_complete,
        retention_exhausted=page.retention_exhausted, observed_at=NOW,
    ))


@pytest.mark.parametrize("conflict", [False, True])
async def test_powerbi_aliases_are_validated_across_pages_before_any_source_work(rest_factory, conflict):
    request_b = str(UUID(int=999)) if conflict else RUN
    calls = []
    def handler(request):
        calls.append(request)
        if "$skip" not in request.url.params:
            return httpx.Response(200, json={
                "value": [powerbi_row(requestId=None if not conflict else RUN)],
                "@odata.nextLink": f"https://api.powerbi.com{request.url.path}?$skip=1",
            })
        return httpx.Response(200, json={"value": [powerbi_row(requestId=request_b)]})

    store, state, clock = make_store(targets=(target("powerbi"),))
    work = claim_poll(store)
    rest = rest_factory(handler, clock=clock)
    first = await PowerBIPollingClient(rest).read_page(target("powerbi"), window(), observed_at=NOW)
    accepted = commit_history_page(store, work, first, request_id=str(UUID(int=40_010)))
    assert accepted.powerbi_window.state == "collecting"
    assert accepted.intake.work_ids == ()
    assert store.get_source(m.SourceExecutionIdentity(target=target("powerbi"), run_id_kind="powerbi_request", run_id=RUN)) is None
    assert not store.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OTHER_OWNER, kinds=("triage",), limit=1, per_workspace_limit=1,
    ))
    restarted = TrackingStore(clock=clock, state=state)
    second = await PowerBIPollingClient(rest).read_page(
        target("powerbi"), window(), observed_at=NOW, cursor=accepted.checkpoint.cursor,
    )
    finished = commit_history_page(
        restarted, work, second, request_id=str(UUID(int=40_011)), checkpoint=accepted.checkpoint,
    )
    aliases = restarted.list_powerbi_aliases(
        m.PageQuery(**CONTEXT.model_dump()), window_id=finished.powerbi_window.window_id,
    )
    refresh = next(value for value in aliases.items if value.namespace == "refresh")
    assert refresh.identifier == "23"
    if conflict:
        assert refresh.mapped_ids == tuple(sorted((RUN, request_b)))
        assert finished.powerbi_window.state == "quarantined"
        assert finished.intake.work_ids == ()
        assert finished.checkpoint.coverage_through is None
    else:
        assert refresh.mapped_ids == (RUN,)
        assert finished.powerbi_window.state == "validated"
        assert len(finished.intake.work_ids) == 1
        assert restarted.get_work(CONTEXT, finished.intake.work_ids[0]).execution.run_id == RUN
    assert len(calls) == 2


async def test_worker_accepts_pages_atomically_and_shared_store_finishes_and_schedules(rest_factory):
    store, _, clock = make_store()
    calls = []
    rest = rest_factory(lambda request: calls.append(request) or httpx.Response(
        200, json={"value": [pipeline_row()]},
    ), clock=clock)
    result = await collector(store, rest, clock).run_once()
    assert result.claimed == 1 and result.results[0].state == "recorded"
    assert result.results[0].observations == 1 and len(calls) == 1
    request = store.requests[0]
    assert request.received_count == 1 and request.window_complete
    assert request.lease.owner_id == OWNER
    work = store.get_work(CONTEXT, request.poll_work_id)
    assert work.state == "completed" and work.lease is None
    checkpoint = store.get_rest_checkpoint(target())
    assert checkpoint.coverage_through == NOW and checkpoint.last_page_id == request.page_id
    assert store.resolve_target(target()).next_poll_at == NOW + timedelta(seconds=300)
    assert all(identifier != threading.get_ident() for identifier in store.worker_threads)
    receipt = store.get_rest_page(CONTEXT, request.page_id)
    assert len(receipt.intake.work_ids) == 1


async def test_duplicate_runs_across_pages_create_one_source_work_item(rest_factory):
    store, _, clock = make_store()

    def handler(request):
        return httpx.Response(200, json={
            "value": [pipeline_row()],
            **({"continuationToken": "second"} if "continuationToken" not in request.url.params else {}),
        })

    result = await collector(store, rest_factory(handler, clock=clock), clock).run_once()
    assert result.results[0].pages == 2
    work_ids = {
        work_id for request in store.requests
        for work_id in store.get_rest_page(CONTEXT, request.page_id).intake.work_ids
    }
    assert len(work_ids) == 1
    assert store.requests[1].expected_checkpoint_revision == 1
    assert store.requests[1].expected_cursor == store.requests[0].next_cursor


async def test_rest_ambiguous_commit_is_reconciled_by_exact_page_receipt(rest_factory):
    store, _, clock = make_store()
    store.uncertain_after = True
    result = await collector(
        store, rest_factory(lambda _: httpx.Response(200, json={"value": [pipeline_row()]}), clock=clock), clock,
    ).run_once()
    assert result.results[0].state == "recorded"
    assert len(store.requests) == 1
    assert store.get_rest_checkpoint(target()).revision == 1
    assert store.get_work(CONTEXT, store.requests[0].poll_work_id).state == "completed"


async def test_unacknowledged_uncommitted_page_does_not_advance_then_replays_under_new_fence(rest_factory):
    store, _, clock = make_store()
    store.uncertain_before = True
    rest = rest_factory(lambda _: httpx.Response(200, json={"value": [pipeline_row()]}), clock=clock)
    worker = collector(store, rest, clock)
    with pytest.raises(MonitoringCommitUncertain):
        await worker.run_once()
    assert store.get_rest_checkpoint(target()) is None
    first = store.requests[0]
    assert store.get_work(CONTEXT, first.poll_work_id).state == "leased"
    clock.advance(121)
    result = await collector(store, rest, clock, owner_id=OTHER_OWNER).run_once()
    assert result.results[0].state == "recorded"
    assert store.requests[-1].lease.fence > first.lease.fence
    assert store.get_rest_checkpoint(target()).revision == 1


async def test_store_outage_leaves_work_unfinished_and_no_local_acceptance(rest_factory):
    store, _, clock = make_store()
    store.unavailable = True
    rest = rest_factory(lambda _: httpx.Response(200, json={"value": [pipeline_row()]}), clock=clock)
    with pytest.raises(MonitoringUnavailable):
        await collector(store, rest, clock).run_once()
    assert store.get_rest_checkpoint(target()) is None
    assert store.get_work(CONTEXT, store.requests[0].poll_work_id).state == "leased"


async def test_partial_page_restart_accepts_every_row_before_the_cursor_advances(rest_factory):
    store, state, clock = make_store()
    rows = [pipeline_row(id=str(UUID(int=index + 1_000)), status="Completed") for index in range(201)]
    rest = rest_factory(lambda _: httpx.Response(200, json={"value": rows}), clock=clock)
    first = await collector(store, rest, clock, pages_per_work=1).run_once()
    assert first.results[0].state == "deferred" and store.requests[0].received_count == 200
    checkpoint = store.get_rest_checkpoint(target())
    assert checkpoint.cursor is not None and checkpoint.coverage_through is None
    clock.advance(15)
    restarted = TrackingStore(clock=clock, state=state)
    second = await collector(restarted, rest, clock, owner_id=OTHER_OWNER, pages_per_work=1).run_once()
    assert second.results[0].state == "recorded" and restarted.requests[0].received_count == 1
    assert restarted.requests[0].expected_cursor == checkpoint.cursor
    assert len({
        observation.key for request in [*store.requests, *restarted.requests]
        for observation in request.observations
    }) == 201


async def test_lease_expiry_during_rest_read_cannot_accept_a_page(rest_factory):
    store, _, clock = make_store()

    def handler(request):
        clock.advance(121)
        return httpx.Response(200, json={"value": [pipeline_row()]})

    result = await collector(store, rest_factory(handler, clock=clock), clock).run_once()
    assert result.results[0].state == "lease_lost"
    assert store.get_rest_checkpoint(target()) is None
    assert store.get_work(CONTEXT, store.requests[0].poll_work_id).state == "leased"


@pytest.mark.parametrize("status", [401, 403, 429, 503])
async def test_http_failure_persists_a_partial_gap_without_renewing_capability_or_sleeping(rest_factory, status):
    store, _, clock = make_store()
    calls = []
    rest = rest_factory(lambda request: calls.append(request) or httpx.Response(
        status, headers={"Retry-After": "700"} if status == 429 else {},
    ), clock=clock)
    result = await collector(store, rest, clock).run_once()
    assert result.results[0].state == "deferred" and result.results[0].gaps
    checkpoint = store.get_rest_checkpoint(target())
    assert checkpoint.coverage_through is None and checkpoint.cursor is not None
    state = json.loads(checkpoint.cursor)
    assert state["last_read_gap"]["code"] == ("service_throttled" if status == 429 else f"http_{status}")
    assert store.requests[0].received_count == 0 and not store.requests[0].window_complete
    work = store.get_work(CONTEXT, store.requests[0].poll_work_id)
    assert work.state == "waiting" and work.lease is None
    assert work.due_at == NOW + timedelta(seconds=700 if status == 429 else 60)
    assert store.resolve_target(target()).capability_id == str(UUID(int=10_000))
    assert len(calls) == 1


async def test_throttled_page_resumes_same_window_after_shared_cooldown(rest_factory):
    store, _, clock = make_store()
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "700"}) if len(calls) == 1 else httpx.Response(
            200, json={"value": [pipeline_row(status="Completed")]},
        )

    rest = rest_factory(handler, clock=clock)
    worker = collector(store, rest, clock)
    first = await worker.run_once()
    checkpoint = store.get_rest_checkpoint(target())
    assert first.results[0].state == "deferred"
    assert (await worker.run_once()).claimed == 0
    clock.advance(700)
    second = await worker.run_once()
    assert second.results[0].state == "recorded" and len(calls) == 2
    assert store.requests[-1].window == checkpoint.window
    assert store.requests[-1].expected_cursor == checkpoint.cursor
    assert store.get_rest_checkpoint(target()).coverage_through == checkpoint.window.end_at


async def test_two_collector_instances_share_sql_fair_work_and_service_budget(rest_factory):
    targets = [
        target(workspace_id=str(UUID(int=40 + index)), item_id=str(UUID(int=100 + index))) for index in range(8)
    ]
    store, state, clock = make_store(targets=targets)
    second_store = TrackingStore(clock=clock, state=state)
    budget = InMemoryRateBudget(clock=clock)
    calls = []

    async def handler(request):
        calls.append(request)
        await asyncio.sleep(0)
        return httpx.Response(200, json={"value": []})

    rest = rest_factory(
        handler, clock=clock, budget=budget, service_policies={"fabric": RatePolicy(2, 60)},
    )
    first, second = await asyncio.gather(
        collector(store, rest, clock).run_once(),
        collector(second_store, rest, clock, owner_id=OTHER_OWNER).run_once(),
    )
    assert first.claimed + second.claimed == 8
    assert len({value.work_id for value in (*first.results, *second.results)}) == 8
    assert len(calls) == 2
    assert sum(result.state == "deferred" for result in (*first.results, *second.results)) == 6


async def test_200_targets_in_10_workspaces_receive_bounded_fair_polling(rest_factory):
    targets = [
        target(workspace_id=str(UUID(int=40 + workspace)), item_id=str(UUID(int=1_000 + workspace * 20 + index)))
        for workspace in range(10) for index in range(20)
    ]
    store, _, clock = make_store(targets=targets)
    calls = []

    def handler(request):
        calls.append(request)
        assert request.method == "GET"
        return httpx.Response(200, json={"value": []})

    rest = rest_factory(
        handler, clock=clock,
        service_policies={"fabric": RatePolicy(1_000, 60)},
        api_policies={"fabric.jobs": RatePolicy(1_000, 60)},
    )
    worker = collector(store, rest, clock)
    for _ in range(50):
        before = len(calls)
        result = await worker.run_once()
        assert result.claimed <= 4
        batch = calls[before:]
        shares = Counter(request.url.path.split("/")[3] for request in batch)
        assert max(shares.values(), default=0) <= 2
        clock.advance(1)
    workspace_ids = [request.url.path.split("/")[3] for request in calls]
    assert len(calls) == 200
    assert len(set(workspace_ids[:12])) == 10
    assert set(Counter(workspace_ids).values()) == {20}
    assert all(
        request.kinds == ("inventory", "capability_probe", "poll") and request.per_workspace_limit == 2
        for request in store.claims
    )


async def test_explicit_inventory_work_before_any_scope_is_not_replaced_with_legacy_targets(rest_factory):
    clock = Clock()
    control = m.DeploymentControl(
        **CONTEXT.model_dump(), revision=0, activation_cutoff=NOW, maintenance=False, updated_at=NOW,
    )
    store = TrackingStore(clock=clock, state=InMemoryMonitoringState.empty(control))
    selector = m.ScopeSelector(tenant_id=TENANT, kind="tenant")
    work = store.request_discovery(
        m.RegistryVersion(**CONTEXT.model_dump(), revision=0), selector, request_id=REQUEST,
    )
    assert work.discovery_selector == selector
    calls = []
    rest = rest_factory(lambda request: calls.append(request) or httpx.Response(
        200, json={"domains": []} if request.url.path.endswith("/domains") else {"workspaces": []},
    ), clock=clock)
    worker = MonitoringCollector(
        store, CONTEXT, FabricInventoryClient(
            rest, options=InventoryApiOptions(admin_workspaces=True, admin_domains=True, admin_items_preview=True),
        ), FabricPipelinePollingClient(rest), PowerBIPollingClient(rest),
        IDENTITY, OWNER, clock=clock, pages_per_work=8,
    )
    result = await worker.run_once()
    assert result.results[0].state == "recorded"
    generation = store.get_inventory_generation(CONTEXT, work.work_id)
    assert generation.selector == selector and generation.completeness == "complete"
    assert generation.discovered_count == 0
    assert store.list_scopes(m.PageQuery(**CONTEXT.model_dump())).items == ()
    assert all(request.method == "GET" for request in calls)


async def test_domain_inventory_round_trips_named_containers_without_inflating_items(rest_factory):
    clock = Clock()
    control = m.DeploymentControl(
        **CONTEXT.model_dump(), revision=0, activation_cutoff=NOW, maintenance=False, updated_at=NOW,
    )
    store = TrackingStore(clock=clock, state=InMemoryMonitoringState.empty(control))
    selector = m.ScopeSelector(tenant_id=TENANT, kind="domain", domain_id=DOMAIN, include_descendants=True)
    work = store.request_discovery(
        m.RegistryVersion(**CONTEXT.model_dump(), revision=0), selector, request_id=REQUEST,
    )

    def handler(request):
        assert request.method == "GET"
        path = request.url.path
        if path == "/v1/admin/domains":
            return httpx.Response(200, json={"domains": [
                {"id": DOMAIN, "displayName": "Parent"},
                {"id": CHILD, "displayName": "Child", "parentDomainId": DOMAIN},
            ]})
        if path == "/v1/admin/workspaces":
            return httpx.Response(200, json={"workspaces": [{"id": WORKSPACE, "name": "Named workspace"}]})
        if path == f"/v1/admin/domains/{DOMAIN}/workspaces":
            return httpx.Response(200, json={"value": []})
        if path == f"/v1/admin/domains/{CHILD}/workspaces":
            return httpx.Response(200, json={"value": [{"id": WORKSPACE}]})
        if path == "/v1/admin/items":
            return httpx.Response(200, json={"itemEntities": [
                {"id": ITEM, "workspaceId": WORKSPACE, "name": "Pipeline", "type": "DataPipeline"},
                {"id": str(UUID(int=900)), "workspaceId": WORKSPACE, "name": "Notebook", "type": "Notebook"},
            ]})
        if path.endswith("/jobs/instances"):
            return httpx.Response(200, json={"value": []})
        if path == f"/v1/workspaces/{WORKSPACE}/items/{ITEM}":
            return httpx.Response(200, json={
                "id": ITEM, "workspaceId": WORKSPACE, "displayName": "Pipeline", "type": "DataPipeline",
            })
        pytest.fail(f"Unexpected fixture route {path}")

    rest = rest_factory(handler, clock=clock)
    worker = MonitoringCollector(
        store, CONTEXT, FabricInventoryClient(
            rest, options=InventoryApiOptions(True, True, True, False),
        ), FabricPipelinePollingClient(rest), PowerBIPollingClient(rest),
        IDENTITY, OWNER, clock=clock, pages_per_work=2,
    )
    for _ in range(4):
        await worker.run_once()
        generation = store.get_inventory_generation(CONTEXT, work.work_id)
        if generation.completed_at is not None:
            break
        clock.advance(15)
    assert generation.completeness == "complete" and generation.discovered_count == 2
    assert store.list_workspaces(m.PageQuery(**CONTEXT.model_dump())).items[0].name == "Named workspace"
    domains = store.list_domains(m.PageQuery(**CONTEXT.model_dump())).items
    assert {value.name for value in domains} == {"Parent", "Child"}
    items = store.list_inventory(m.TargetQuery(**CONTEXT.model_dump())).items
    assert len(items) == 2 and all(value.domain_ancestor_ids == (DOMAIN,) for value in items)
    assert store.coverage(CONTEXT).unsupported_count == 1


async def test_inventory_checks_database_lease_again_after_the_source_read(rest_factory):
    clock = Clock()
    control = m.DeploymentControl(
        **CONTEXT.model_dump(), revision=0, activation_cutoff=NOW, maintenance=False, updated_at=NOW,
    )
    store = TrackingStore(clock=clock, state=InMemoryMonitoringState.empty(control))
    store.request_discovery(
        m.RegistryVersion(**CONTEXT.model_dump(), revision=0),
        m.ScopeSelector(tenant_id=TENANT, kind="tenant"), request_id=REQUEST,
    )

    def handler(request):
        clock.advance(121)
        return httpx.Response(200, json={"domains": []})

    rest = rest_factory(handler, clock=clock)
    worker = MonitoringCollector(
        store, CONTEXT, FabricInventoryClient(rest, options=InventoryApiOptions(admin_domains=True)),
        FabricPipelinePollingClient(rest), PowerBIPollingClient(rest),
        IDENTITY, OWNER, clock=clock,
    )
    result = await worker.run_once()
    assert result.results[0].state == "lease_lost"
    generation = store.get_inventory_generation(CONTEXT, REQUEST)
    assert generation.completed_pages == 0 and generation.completed_at is None


async def test_overlapping_collectors_keep_generation_membership_and_exclusions(rest_factory):
    clock = Clock()
    control = m.DeploymentControl(
        **CONTEXT.model_dump(), revision=0, activation_cutoff=NOW, maintenance=False, updated_at=NOW,
    )
    store = TrackingStore(clock=clock, state=InMemoryMonitoringState.empty(control))
    version = m.RegistryVersion(**CONTEXT.model_dump(), revision=0)
    store.record_inventory(m.InventoryBatch(
        request_id=str(UUID(int=60_000)), expected=version, items=(),
        generation=m.InventoryGeneration(
            **CONTEXT.model_dump(), generation_id=GENERATION,
            selector=m.ScopeSelector(tenant_id=TENANT, kind="tenant"), adapter="explicit_empty_fixture",
            authority="fixture", completeness="complete", started_at=NOW, completed_at=NOW,
        ),
    ))
    plan = store.preview_scope(m.ScopePreviewRequest(
        expected=version, idempotency_id=str(UUID(int=60_001)),
        scope=m.ScopeDefinition(
            **CONTEXT.model_dump(), scope_id=SCOPE, name="Tenant excluding one domain",
            rules=(
                m.ScopeRule(rule_id=RULE, effect="include", auto_enrol_detection_only=True,
                            selector=m.ScopeSelector(tenant_id=TENANT, kind="tenant")),
                m.ScopeRule(rule_id=str(UUID(int=60_002)), effect="exclude",
                            selector=m.ScopeSelector(tenant_id=TENANT, kind="domain", domain_id=DOMAIN)),
            ),
        ),
    ))
    activated = store.activate_scope(m.ActivateScopeRequest(
        expected=version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    first_work = store.request_discovery(
        activated.version, m.ScopeSelector(tenant_id=TENANT, kind="tenant"), request_id=str(UUID(int=60_003)),
    )
    calls = []
    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/v1/admin/domains":
            return httpx.Response(200, json={"domains": [{"id": DOMAIN, "displayName": "Excluded domain"}]})
        if request.url.path == "/v1/admin/workspaces":
            return httpx.Response(200, json={"workspaces": [{"id": WORKSPACE, "name": "Tenant catalogue workspace"}]})
        if request.url.path == f"/v1/workspaces/{WORKSPACE}":
            return httpx.Response(200, json={"id": WORKSPACE, "displayName": "Overlapping workspace catalogue"})
        if request.url.path == f"/v1/admin/domains/{DOMAIN}/workspaces":
            return httpx.Response(200, json={"value": [{"id": WORKSPACE}]})
        if request.url.path == "/v1/admin/items":
            return httpx.Response(200, json={"itemEntities": [
                {"id": ITEM, "workspaceId": WORKSPACE, "name": "Excluded pipeline", "type": "DataPipeline"},
            ]})
        pytest.fail(f"Unexpected inventory route: {request.url.path}")

    rest = rest_factory(handler, clock=clock)
    def worker(owner, pages):
        return MonitoringCollector(
            store, CONTEXT, FabricInventoryClient(rest, options=InventoryApiOptions(True, True, True, False)),
            FabricPipelinePollingClient(rest), PowerBIPollingClient(rest),
            IDENTITY, owner, clock=clock, pages_per_work=pages,
        )
    assert (await worker(OWNER, 2).run_once()).results[0].state == "deferred"
    first_generation = store.get_inventory_generation(CONTEXT, first_work.work_id)
    assert first_generation.recorded_domain_count == first_generation.recorded_workspace_count == 1
    clock.advance(1)
    second_work = store.request_discovery(
        activated.version, m.ScopeSelector(tenant_id=TENANT, kind="workspace", workspace_id=WORKSPACE),
        request_id=str(UUID(int=60_004)),
    )
    await worker(OTHER_OWNER, 2).run_once()
    assert store.list_domains(m.PageQuery(**CONTEXT.model_dump())).items[0].generation_id == second_work.work_id
    assert store.list_workspaces(m.PageQuery(**CONTEXT.model_dump())).items[0].generation_id == second_work.work_id
    clock.advance(14)
    await worker(OWNER, 4).run_once()
    generation = store.get_inventory_generation(CONTEXT, first_work.work_id)
    assert generation.completeness == "complete"
    assert f"/v1/admin/domains/{DOMAIN}/workspaces" in calls
    item = store.list_inventory(m.TargetQuery(**CONTEXT.model_dump()), generation_id=first_work.work_id).items[0]
    assert item.domain_ids == (DOMAIN,)
    assert store.resolve_target(target(), include_inactive=True) is None


async def test_missing_continuation_catalogue_is_unknown_not_empty_membership(rest_factory, monkeypatch):
    clock = Clock()
    control = m.DeploymentControl(
        **CONTEXT.model_dump(), revision=0, activation_cutoff=NOW, maintenance=False, updated_at=NOW,
    )
    store = TrackingStore(clock=clock, state=InMemoryMonitoringState.empty(control))
    work = store.request_discovery(
        m.RegistryVersion(**CONTEXT.model_dump(), revision=0),
        m.ScopeSelector(tenant_id=TENANT, kind="tenant"), request_id=REQUEST,
    )
    calls = []
    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=(
            {"domains": [{"id": DOMAIN, "displayName": "Known domain"}]}
            if request.url.path.endswith("/domains") else
            {"workspaces": [{"id": WORKSPACE, "name": "Known workspace"}]}
        ))
    rest = rest_factory(handler, clock=clock)
    worker = MonitoringCollector(
        store, CONTEXT, FabricInventoryClient(rest, options=InventoryApiOptions(True, True, True, False)),
        FabricPipelinePollingClient(rest), PowerBIPollingClient(rest),
        IDENTITY, OWNER, clock=clock, pages_per_work=2,
    )
    await worker.run_once()
    assert len(calls) == 2
    original = store.list_domains
    def missing(query, *, generation_id=None):
        return original(query, generation_id=generation_id).model_copy(update={"items": ()})
    monkeypatch.setattr(store, "list_domains", missing)
    clock.advance(15)
    result = await worker.run_once()
    assert result.results[0].state == "deferred"
    assert "continuation_catalogue_missing" in {gap.code for gap in result.results[0].gaps}
    assert len(calls) == 2
    generation = store.get_inventory_generation(CONTEXT, work.work_id)
    assert generation.recorded_domain_count == 1
    assert generation.completeness == "partial"
    assert store.list_inventory(m.TargetQuery(**CONTEXT.model_dump())).items == ()


async def test_collector_conflicting_powerbi_pages_never_publish_earlier_execution(rest_factory):
    store, _, clock = make_store(targets=(target("powerbi"),))
    second_id = str(UUID(int=999))
    def handler(request):
        if "$skip" not in request.url.params:
            return httpx.Response(200, json={
                "value": [powerbi_row()],
                "@odata.nextLink": f"https://api.powerbi.com{request.url.path}?$skip=1",
            })
        assert store.get_source(m.SourceExecutionIdentity(
            target=target("powerbi"), run_id_kind="powerbi_request", run_id=RUN,
        )) is None
        return httpx.Response(200, json={"value": [powerbi_row(requestId=second_id)]})
    rest = rest_factory(handler, clock=clock)
    first = await collector(store, rest, clock, pages_per_work=1).run_once()
    assert first.results[0].state == "deferred"
    first_receipt = store.get_rest_page(CONTEXT, store.requests[0].page_id)
    assert first_receipt.intake.work_ids == ()
    clock.advance(15)
    second = await collector(store, rest, clock, pages_per_work=1, owner_id=OTHER_OWNER).run_once()
    assert second.results[0].state == "recorded"
    assert "powerbi_alias_validation_incomplete" in {gap.code for gap in second.results[0].gaps}
    last = store.get_rest_page(CONTEXT, store.requests[-1].page_id)
    assert last.powerbi_window.state == "quarantined"
    assert last.intake.work_ids == ()
    assert store.get_rest_checkpoint(target("powerbi")).coverage_through is None


async def test_powerbi_conflict_in_later_atomic_chunk_never_publishes_earlier_chunk(rest_factory):
    rows = [
        powerbi_row(id=index + 1, requestId=str(UUID(int=1_000 + index)))
        for index in range(200)
    ]
    rows.append(powerbi_row(id=23, requestId=str(UUID(int=9_999))))
    store, _, clock = make_store(targets=(target("powerbi"),))
    work = claim_poll(store)
    client = PowerBIPollingClient(rest_factory(lambda _: httpx.Response(200, json={"value": rows}), clock=clock))
    first = await client.read_page(target("powerbi"), window(), observed_at=NOW)
    assert first.received_count == 200 and not first.powerbi_window_complete
    staged = commit_history_page(store, work, first, request_id=str(UUID(int=61_001)))
    assert staged.intake.work_ids == ()
    assert staged.powerbi_window.row_count == 200
    second = await client.read_page(target("powerbi"), window(), observed_at=NOW, cursor=first.next_cursor)
    assert second.received_count == 1 and second.powerbi_window_complete
    finished = commit_history_page(
        store, work, second, request_id=str(UUID(int=61_002)), checkpoint=staged.checkpoint,
    )
    assert finished.powerbi_window.row_count == 201
    assert finished.powerbi_window.state == "quarantined"
    assert finished.intake.work_ids == ()
    assert finished.checkpoint.coverage_through is None


async def test_powerbi_request_mapped_to_two_refresh_ids_across_pages_is_quarantined(rest_factory):
    def handler(request):
        return httpx.Response(200, json={
            "value": [powerbi_row(id=24 if "$skip" in request.url.params else 23)],
            **({"@odata.nextLink": f"https://api.powerbi.com{request.url.path}?$skip=1"}
               if "$skip" not in request.url.params else {}),
        })
    store, _, clock = make_store(targets=(target("powerbi"),))
    work = claim_poll(store)
    client = PowerBIPollingClient(rest_factory(handler, clock=clock))
    first = await client.read_page(target("powerbi"), window(), observed_at=NOW)
    staged = commit_history_page(store, work, first, request_id=str(UUID(int=61_010)))
    second = await client.read_page(target("powerbi"), window(), observed_at=NOW, cursor=first.next_cursor)
    finished = commit_history_page(
        store, work, second, request_id=str(UUID(int=61_011)), checkpoint=staged.checkpoint,
    )
    assert finished.powerbi_window.state == "quarantined"
    assert finished.intake.work_ids == ()
    aliases = store.list_powerbi_aliases(
        m.PageQuery(**CONTEXT.model_dump()), window_id=finished.powerbi_window.window_id,
    )
    assert next(value for value in aliases.items if value.namespace == "request").mapped_ids == ("23", "24")


async def test_capability_checks_database_lease_again_before_persisting_proof(rest_factory):
    store, _, clock = make_store()
    for work in store.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("poll",), limit=1, per_workspace_limit=1,
    )):
        store.disposition_work(m.WorkDispositionRequest(
            **CONTEXT.model_dump(), request_id=str(UUID(int=30_000)),
            work_id=work.work_id, expected_work_revision=work.revision, lease=work.lease,
            disposition="superseded", detail="Fixture isolates the capability work",
        ))
    store.enqueue_work(m.MonitoringWorkDraft(
        **CONTEXT.model_dump(), work_id=str(UUID(int=30_001)), kind="capability_probe",
        policy_revision=store.snapshot(CONTEXT).control.revision,
        due_at=NOW, created_at=NOW, reason="Explicit fixture capability probe", target=target(),
    ))

    def handler(request):
        if request.url.path.endswith("instances"):
            clock.advance(121)
            return httpx.Response(200, json={"value": []})
        return httpx.Response(200, json={
            "id": ITEM, "workspaceId": WORKSPACE, "type": "DataPipeline", "displayName": "Pipeline",
        })

    result = await collector(store, rest_factory(handler, clock=clock), clock).run_once()
    assert result.results[0].state == "lease_lost"
    assert store.resolve_target(target()).capability_id == str(UUID(int=10_000))
