from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from triage.tools.powerbi import LivePowerBIClient

WORKSPACE = "11111111-1111-4111-8111-111111111111"
DATASET = "22222222-2222-4222-8222-222222222222"
RUN = "33333333-3333-4333-8333-333333333333"
OTHER_RUN = "44444444-4444-4444-8444-444444444444"
BASE = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE}/datasets/{DATASET}/refreshes"


@pytest.fixture
def client_for(monkeypatch):
    client_type = httpx.AsyncClient

    def build(handler, *, timeout=0.05):
        transport = httpx.MockTransport(handler)
        monkeypatch.setattr(
            httpx, "AsyncClient", lambda **kwargs: client_type(transport=transport, **kwargs),
        )
        credential = SimpleNamespace(
            get_token=lambda scope: SimpleNamespace(token="offline-test", expires_on=9999999999),
        )
        return LivePowerBIClient(
            tenant_id=WORKSPACE, client_id="", client_secret="",
            credential=credential, poll_seconds=0, poll_timeout_seconds=timeout,
        )

    return build


@pytest.mark.asyncio
async def test_submission_returns_exact_id_without_polling(client_for):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(202, headers={"Location": f"{BASE}/{RUN}"})

    result = await client_for(handler).submit_refresh(WORKSPACE, DATASET)
    assert result.status == "Submitted"
    assert result.request_id == RUN
    assert result.submission_state == "submitted"
    assert not result.succeeded
    assert [request.method for request in requests] == ["POST"]


@pytest.mark.asyncio
async def test_verification_ignores_a_concurrent_completed_refresh(client_for):
    calls = []

    def handler(request):
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(202, headers={"x-ms-request-id": RUN})
        return httpx.Response(200, json={"value": [
            {"requestId": OTHER_RUN, "status": "Completed"},
            {"requestId": RUN, "status": "Failed", "serviceExceptionJson": "Expected failure"},
        ]})

    result = await client_for(handler).refresh_dataset(WORKSPACE, DATASET)
    assert result.status == "Failed"
    assert result.request_id == RUN
    assert calls.count("POST") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [
    {},
    {"x-ms-request-id": "not-a-refresh-id"},
    {"x-ms-request-id": "00000000-0000-0000-0000-000000000000"},
    {"Location": f"https://example.invalid/refreshes/{RUN}", "x-ms-request-id": RUN},
    {"Location": f"{BASE.replace(DATASET, OTHER_RUN)}/{RUN}", "x-ms-request-id": RUN},
    {"Location": f"{BASE}/{RUN}?unexpected=true"},
    {"Location": f"{BASE}/{RUN}/another"},
    {"Location": "https://[invalid"},
])
async def test_missing_or_invalid_correlation_never_uses_first_unseen_run(client_for, headers):
    methods = []

    def handler(request):
        methods.append(request.method)
        if request.method == "POST":
            return httpx.Response(202, headers=headers)
        return httpx.Response(200, json={"value": [
            {"requestId": OTHER_RUN, "status": "Completed"},
        ]})

    result = await client_for(handler).refresh_dataset(WORKSPACE, DATASET)
    assert result.status == "Unknown"
    assert result.submission_state == "uncertain"
    assert not result.request_id
    assert methods == ["POST"]


@pytest.mark.asyncio
async def test_location_identifies_submission_not_an_unrelated_trace_header(client_for):
    def handler(request):
        return httpx.Response(
            202, headers={"Location": f"{BASE}/{RUN}", "x-ms-request-id": OTHER_RUN},
        )

    result = await client_for(handler).submit_refresh(WORKSPACE, DATASET)
    assert result.request_id == RUN


@pytest.mark.asyncio
async def test_lost_submission_acknowledgement_is_uncertain_and_not_retried(client_for):
    methods = []

    def handler(request):
        methods.append(request.method)
        raise httpx.ReadTimeout("No acknowledgement", request=request)

    result = await client_for(handler).refresh_dataset(WORKSPACE, DATASET)
    assert result.status == "Unknown"
    assert result.submission_state == "uncertain"
    assert methods == ["POST"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "outcome", "submission"), [
    (429, "Throttled", "rejected"),
    (403, "Failed", "rejected"),
    (503, "Unknown", "uncertain"),
])
async def test_rejection_is_distinct_from_uncertain_server_failure(
    client_for, status, outcome, submission,
):
    result = await client_for(lambda request: httpx.Response(
        status, headers={"Retry-After": "42"},
    )).submit_refresh(WORKSPACE, DATASET)
    assert result.status == outcome
    assert result.submission_state == submission
    if status == 429:
        assert result.retry_after_seconds == 42


@pytest.mark.asyncio
async def test_read_only_recovery_preserves_exact_id_without_resubmitting(client_for):
    methods = []

    def handler(request):
        methods.append(request.method)
        return httpx.Response(200, json={"value": [
            {"requestId": RUN, "status": "Completed"},
        ]})

    result = await client_for(handler).verify_refresh(WORKSPACE, DATASET, RUN)
    assert result.succeeded
    assert result.request_id == RUN
    assert methods == ["GET"]


@pytest.mark.asyncio
async def test_verification_timeout_keeps_submission_id(client_for):
    client = client_for(lambda request: httpx.Response(200, json={"value": []}), timeout=0)
    result = await client.verify_refresh(WORKSPACE, DATASET, RUN)
    assert result.status == "Unknown"
    assert result.request_id == RUN
    assert result.submission_state == "submitted"


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{}, {"value": None}, {"value": ["bad"]}])
async def test_malformed_history_is_not_a_healthy_empty_list(client_for, payload):
    client = client_for(lambda request: httpx.Response(200, json=payload))
    with pytest.raises(ValueError, match="value array"):
        await client.get_refresh_history(WORKSPACE, DATASET)


@pytest.mark.asyncio
async def test_gateway_rebind_verifies_exact_reviewed_datasources(client_for):
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(202)
        return httpx.Response(200, json={"value": [{
            "datasourceId": RUN, "gatewayId": OTHER_RUN,
            "connectionDetails": {"server": "unneeded-source-details"},
        }]})

    result = await client_for(handler).rebind_gateway(WORKSPACE, DATASET, OTHER_RUN, [RUN])
    assert result.succeeded
    assert result.configuration == {"gateway_id": OTHER_RUN, "datasource_ids": [RUN]}
    assert result.request_id == ""
    assert [request.method for request in requests] == ["POST", "GET"]
    assert b"datasourceObjectIds" in requests[0].content
    assert "unneeded-source-details" not in str(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [
    [],
    [{"datasourceId": RUN, "gatewayId": WORKSPACE}],
    [{"datasourceId": RUN}],
    [{"datasourceId": OTHER_RUN, "gatewayId": OTHER_RUN}],
])
async def test_gateway_acknowledgement_does_not_prove_binding(client_for, rows):
    methods = []

    def handler(request):
        methods.append(request.method)
        return httpx.Response(202) if request.method == "POST" else httpx.Response(
            200, json={"value": rows},
        )

    result = await client_for(handler).rebind_gateway(WORKSPACE, DATASET, OTHER_RUN, [RUN])
    assert not result.succeeded
    assert result.submission_state == "submitted"
    assert methods == ["POST", "GET"]


@pytest.mark.asyncio
async def test_gateway_recovery_is_read_only(client_for):
    methods = []

    def handler(request):
        methods.append(request.method)
        return httpx.Response(200, json={"value": [{"datasourceId": RUN, "gatewayId": OTHER_RUN}]})

    result = await client_for(handler).verify_gateway_binding(WORKSPACE, DATASET, OTHER_RUN, [RUN])
    assert result.succeeded
    assert methods == ["GET"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("observed", "succeeded"), [(True, True), (False, False)])
async def test_schedule_acknowledgement_requires_matching_readback(client_for, observed, succeeded):
    methods = []

    def handler(request):
        methods.append(request.method)
        return httpx.Response(202) if request.method == "PATCH" else httpx.Response(
            200, json={"enabled": observed},
        )

    result = await client_for(handler).set_refresh_schedule_enabled(WORKSPACE, DATASET, True)
    assert result.succeeded is succeeded
    assert result.configuration == {"enabled": observed}
    assert result.submission_state == "submitted"
    assert result.request_id == ""
    assert methods == ["PATCH", "GET"]


@pytest.mark.asyncio
async def test_schedule_readback_failure_keeps_the_write_uncertain(client_for):
    methods = []

    def handler(request):
        methods.append(request.method)
        return httpx.Response(202) if request.method == "PATCH" else httpx.Response(403)

    result = await client_for(handler).set_refresh_schedule_enabled(WORKSPACE, DATASET, True)
    assert result.status == "Unknown"
    assert result.submission_state == "submitted"
    assert methods == ["PATCH", "GET"]


@pytest.mark.asyncio
async def test_configuration_submission_timeout_is_not_retried(client_for):
    methods = []

    def handler(request):
        methods.append(request.method)
        raise httpx.ReadTimeout("Acknowledgement lost", request=request)

    result = await client_for(handler).set_refresh_schedule_enabled(WORKSPACE, DATASET, True)
    assert result.status == "Unknown"
    assert result.submission_state == "uncertain"
    assert methods == ["PATCH"]


@pytest.mark.asyncio
async def test_schedule_requires_an_actual_boolean(client_for):
    client = client_for(lambda request: httpx.Response(200, json={"enabled": "false"}))
    with pytest.raises(ValueError, match="boolean"):
        await client.get_refresh_schedule(WORKSPACE, DATASET)
