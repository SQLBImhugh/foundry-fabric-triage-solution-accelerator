from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest

from triage.pipeline_models import PipelineTarget
from triage.tools.fabric_pipeline import LiveFabricPipelineClient, PipelineApiError

TENANT = "90000000-0000-0000-0000-000000000009"
WORKSPACE = "10000000-0000-0000-0000-000000000001"
PIPELINE = "20000000-0000-0000-0000-000000000002"
RUN = "30000000-0000-0000-0000-000000000003"
NEW_RUN = "40000000-0000-0000-0000-000000000004"
PATH = f"/v1/workspaces/{WORKSPACE}/items/{PIPELINE}/jobs/instances"
TARGET = PipelineTarget(
    name="Orders", workspace_id=WORKSPACE, pipeline_id=PIPELINE,
    rerun_safe=True, rerun_parameters={},
)


def _raw(**overrides):
    return {
        "id": RUN, "itemId": PIPELINE, "jobType": "Pipeline",
        "invokeType": "Scheduled", "status": "Failed",
        "startTimeUtc": "2026-09-11T08:00:00.1234567",
        "endTimeUtc": "2026-09-11T08:01:00.1234567",
        "failureReason": {"errorCode": "ServiceUnavailable", "message": "Temporary service failure"},
        **overrides,
    }


class _Credential:
    def get_token(self, scope):
        assert scope == "https://api.fabric.microsoft.com/.default"
        return SimpleNamespace(token="offline-test-token", expires_on=9999999999)


def _client(handler, *, stub_item=True, **kwargs):
    def route(request):
        if stub_item and request.url.path == f"/v1/workspaces/{WORKSPACE}/items/{PIPELINE}":
            return httpx.Response(200, json={
                "id": PIPELINE, "type": "DataPipeline", "workspaceId": WORKSPACE,
            })
        return handler(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(route))
    return LiveFabricPipelineClient(
        tenant_id=TENANT, credential=_Credential(), http_client=http, **kwargs,
    ), http


async def test_all_history_pages_are_read_and_ids_are_deduplicated() -> None:
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, json={
                "value": [_raw()], "continuationToken": "next",
                "continuationUri": f"https://api.fabric.microsoft.com{PATH}?continuationToken=next",
            })
        assert request.url.params["continuationToken"] == "next"
        return httpx.Response(200, json={"value": [_raw(), _raw(id=NEW_RUN)]})

    client, http = _client(handler)
    async with http:
        runs = await client.list_runs(TARGET)
    assert [run.id for run in runs] == [RUN, NEW_RUN]
    assert runs[0].failed_scheduled
    assert runs[0].start_time.utcoffset().total_seconds() == 0
    assert len(calls) == 2


@pytest.mark.parametrize("payload", [
    {},
    {"value": None},
    {"value": [_raw(itemId=WORKSPACE)]},
    {"value": [_raw(status="SomeNewStatus")]},
    {"value": [_raw(startTimeUtc=1000)]},
    {"value": [_raw(failureReason={"errorCode": {"unexpected": "SqlConnectionIsClosed"}})]},
    {"value": [_raw()], "continuationUri": "https://elsewhere.example/jobs?continuationToken=x"},
])
async def test_bad_history_never_looks_like_no_failed_runs(payload) -> None:
    client, http = _client(lambda _: httpx.Response(200, json=payload))
    with pytest.raises(PipelineApiError):
        async with http:
            await client.list_runs(TARGET)


async def test_repeated_pagination_token_is_reported_not_looped() -> None:
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"value": [], "continuationToken": "same"})

    client, http = _client(handler)
    with pytest.raises(PipelineApiError, match="repeated"):
        async with http:
            await client.list_runs(TARGET)
    assert len(calls) == 2


async def test_encoded_continuation_is_not_double_encoded() -> None:
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, json={
                "value": [], "continuationToken": "page%3Dtwo",
                "continuationUri": f"https://api.fabric.microsoft.com{PATH}?continuationToken=page%3Dtwo",
            })
        assert request.url.params["continuationToken"] == "page=two"
        return httpx.Response(200, json={"value": []})

    client, http = _client(handler)
    async with http:
        assert await client.list_runs(TARGET) == []
    assert len(calls) == 2


async def test_page_limit_reports_incomplete_coverage() -> None:
    client, http = _client(
        lambda _: httpx.Response(200, json={"value": [_raw()], "continuationToken": "next"}),
        max_pages=1,
    )
    with pytest.raises(PipelineApiError, match="incomplete"):
        async with http:
            await client.list_runs(TARGET)


async def test_forbidden_is_a_monitor_error_not_a_pipeline_failure() -> None:
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(403, json={"errorCode": "InsufficientPrivileges"})

    client, http = _client(handler)
    with pytest.raises(PipelineApiError, match="HTTP 403"):
        async with http:
            await client.list_runs(TARGET)
    assert len(calls) == 1


async def test_read_throttling_respects_retry_after() -> None:
    calls, waits = [], []

    async def sleep(seconds):
        waits.append(seconds)

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "4"})
        return httpx.Response(200, json={"value": []})

    client, http = _client(handler, sleep=sleep)
    async with http:
        assert await client.list_runs(TARGET) == []
    assert waits == [4]
    assert len(calls) == 2


async def test_long_retry_after_stops_without_retrying_early() -> None:
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "120"})

    client, http = _client(handler)
    with pytest.raises(PipelineApiError, match="Retry-After=120"):
        async with http:
            await client.list_runs(TARGET)
    assert len(calls) == 1


async def test_new_run_uses_documented_path_and_correlated_location() -> None:
    calls = []

    def handler(request):
        calls.append(request)
        assert request.url.path == f"/v1/workspaces/{WORKSPACE}/items/{PIPELINE}/jobs/Pipeline/instances"
        assert request.method == "POST"
        assert json.loads(request.content) == {
            "executionData": {"parameters": {"Window": "yesterday", "Count": 2, "DryRun": True}},
        }
        return httpx.Response(202, headers={
            "Location": f"https://api.fabric.microsoft.com{PATH}/{NEW_RUN}", "Retry-After": "60",
        })

    client, http = _client(handler)
    target = TARGET.model_copy(update={"rerun_parameters": {"Window": "yesterday", "Count": 2, "DryRun": True}})
    async with http:
        submitted = await client.rerun(target)
    assert submitted.run_id == NEW_RUN
    assert submitted.retry_after_seconds == 60
    assert len(calls) == 1


@pytest.mark.parametrize("response", [
    httpx.Response(503),
    httpx.Response(429, headers={"Retry-After": "0"}),
    httpx.Response(202),
    httpx.Response(200, json={"status": "Completed"}),
    httpx.Response(202, headers={"Location": "https://elsewhere.example/run"}),
    httpx.Response(202, headers={"Location": f"https://api.fabric.microsoft.com{PATH}/not-a-job"}),
])
async def test_submissions_are_never_retried_or_inferred_from_an_ambiguous_reply(response) -> None:
    calls = []

    def handler(request):
        calls.append(request)
        return response

    client, http = _client(handler)
    with pytest.raises(PipelineApiError):
        async with http:
            await client.rerun(TARGET)
    assert len(calls) == 1


async def test_activity_evidence_excludes_inputs_and_outputs() -> None:
    def handler(request):
        assert request.method == "POST"
        assert request.url.path.endswith(f"/datapipelines/pipelineruns/{RUN}/queryactivityruns")
        return httpx.Response(200, json=[{
            "pipelineRunId": RUN, "activityName": "CopyOrders", "activityType": "Copy",
            "status": "Failed", "input": {"private": "unvetted input"}, "output": {"private": "unvetted output"},
            "error": {"errorCode": "SqlOperationFailed", "message": "The source query failed"},
        }])

    client, http = _client(handler)
    run = client._run(_raw(), TARGET)
    async with http:
        activities = await client.activity_runs(TARGET, run)
    assert activities[0].error_code == "SqlOperationFailed"
    assert "unvetted" not in activities[0].model_dump_json()


async def test_another_job_cannot_supply_activity_evidence() -> None:
    client, http = _client(lambda _: httpx.Response(200, json=[{"pipelineRunId": NEW_RUN}]))
    with pytest.raises(PipelineApiError, match="correlated"):
        async with http:
            await client.activity_runs(TARGET, client._run(_raw(), TARGET))


async def test_generic_execute_jobs_require_a_verified_pipeline_item() -> None:
    client, http = _client(
        lambda _: httpx.Response(200, json={"id": PIPELINE, "type": "Notebook", "workspaceId": WORKSPACE}),
        stub_item=False,
    )
    with pytest.raises(PipelineApiError, match="DataPipeline"):
        async with http:
            await client.list_runs(TARGET)


async def test_documented_execute_variant_is_read_without_mapping_it_to_a_schedule() -> None:
    client, http = _client(lambda _: httpx.Response(200, json={"value": [_raw(jobType="Execute")]}))
    async with http:
        runs = await client.list_runs(TARGET)
    assert runs[0].job_type == "Execute"
    assert runs[0].failed_scheduled
