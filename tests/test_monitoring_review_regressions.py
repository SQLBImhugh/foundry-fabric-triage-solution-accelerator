from __future__ import annotations

from uuid import UUID

import httpx
import pytest
from test_monitoring_inventory import CONTEXT, IDENTITY, NOW, Clock, Credential
from test_monitoring_polling import pipeline_row, target, window

from triage.monitoring.inventory import RestReadError, RestRoute, TenantBoundRestClient
from triage.monitoring.polling import FabricPipelinePollingClient
from triage.monitoring.rate_limit import InMemoryRateBudget, RatePolicy


def test_denied_api_does_not_spend_any_service_allowance():
    clock = Clock()
    budget = InMemoryRateBudget(clock=clock)
    service = RatePolicy(120, 60)
    api = RatePolicy(1, 3600)
    assert budget.acquire(CONTEXT, "api:fabric.domains", api).allowed
    for _ in range(120):
        result = budget.acquire_many(CONTEXT, (
            ("service:fabric", service), ("api:fabric.domains", api),
        ))
        assert not result.allowed
    assert all(budget.acquire(CONTEXT, "service:fabric", service).allowed for _ in range(120))
    assert not budget.acquire(CONTEXT, "service:fabric", service).allowed


@pytest.mark.asyncio
async def test_exhausted_domain_budget_does_not_block_unrelated_physical_get():
    clock = Clock()
    budget = InMemoryRateBudget(clock=clock)
    methods = []

    def handler(request):
        methods.append(request.method)
        return httpx.Response(200, json={"value": []})

    rest = TenantBoundRestClient(
        CONTEXT, IDENTITY, Credential(clock), budget,
        transport=httpx.MockTransport(handler), clock=clock,
        service_policies={"fabric": RatePolicy(2, 60), "powerbi": RatePolicy(2, 60)},
        api_policies={"fabric.domains": RatePolicy(1, 3600), "fabric.jobs": RatePolicy(2, 60)},
    )
    try:
        assert budget.acquire(CONTEXT, "api:fabric.domains", RatePolicy(1, 3600)).allowed
        for _ in range(20):
            with pytest.raises(RestReadError, match="budget"):
                await rest.get(RestRoute("fabric", "fabric.domains", "/admin/domains"))
        assert methods == []
        identity = target()
        result = await rest.get(RestRoute(
            "fabric", "fabric.jobs",
            f"/workspaces/{identity.workspace_id}/items/{identity.item_id}/jobs/instances",
        ))
        assert result == {"value": []}
        assert methods == ["GET"]
    finally:
        await rest.close()


@pytest.mark.asyncio
async def test_changed_chunk_keeps_window_incomplete_through_its_last_chunk():
    rows = [
        pipeline_row(
            id=str(UUID(int=1000 + index)),
            **({} if index < 98 or index == 200 else {"status": "InProgress", "endTimeUtc": None}),
        )
        for index in range(201)
    ]
    clock = Clock()

    def handler(_request):
        return httpx.Response(200, json={"value": rows})

    rest = TenantBoundRestClient(
        CONTEXT, IDENTITY, Credential(clock), InMemoryRateBudget(clock=clock),
        transport=httpx.MockTransport(handler), clock=clock,
    )
    client = FabricPipelinePollingClient(rest)
    try:
        first = await client.read_page(target(), window(), observed_at=NOW)
        assert first.next_cursor and not first.gaps
        rows[-1] = pipeline_row(
            id=str(UUID(int=9999)), status="InProgress", endTimeUtc=None,
        )
        changed = await client.read_page(target(), window(), observed_at=NOW, cursor=first.next_cursor)
        assert "source_page_changed" in {gap.code for gap in changed.gaps}
        assert changed.next_cursor
        final = await client.read_page(target(), window(), observed_at=NOW, cursor=changed.next_cursor)
        assert final.next_cursor is None
        assert not final.retention_exhausted
        assert not final.window_complete
        assert "previous_page_incomplete" in {gap.code for gap in final.gaps}
    finally:
        await rest.close()
