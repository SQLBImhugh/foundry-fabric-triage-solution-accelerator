"""Fabric pipeline job access, separated from triage decisions."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import parse_qs, unquote, urlparse
from uuid import NAMESPACE_URL, uuid5

import httpx
from pydantic import ValidationError

from triage.pipeline_models import (
    PIPELINE_JOB_STATUSES,
    PIPELINE_JOB_TYPES,
    PIPELINE_TERMINAL_STATUSES,
    PipelineActivity,
    PipelineRun,
    PipelineSubmission,
    PipelineTarget,
    canonical_id,
)

logger = logging.getLogger("triage.tools.fabric_pipeline")
_API = "https://api.fabric.microsoft.com/v1"
_SCOPE = "https://api.fabric.microsoft.com/.default"


class PipelineApiError(RuntimeError):
    """Monitoring failed; this is not evidence that the pipeline failed."""


class FabricPipelineClient(Protocol):
    async def list_runs(self, target: PipelineTarget) -> list[PipelineRun]: ...
    async def get_run(self, target: PipelineTarget, run_id: str) -> PipelineRun: ...
    async def rerun(self, target: PipelineTarget) -> PipelineSubmission: ...
    async def activity_runs(self, target: PipelineTarget, run: PipelineRun) -> list[PipelineActivity]: ...
    async def close(self) -> None: ...


class MockFabricPipelineClient:
    """The same run facts and effects as the live interface, with no network."""

    def __init__(
        self, runs: list[PipelineRun] | None = None, *, rerun_status: str = "Completed",
        activities: list[PipelineActivity] | None = None,
        rerun_activities: list[PipelineActivity] | None = None,
    ) -> None:
        self.runs = list(runs or [])
        self.rerun_status = rerun_status
        self.calls: list[tuple[str, str]] = []
        self.activities = list(activities or [])
        self.rerun_activities = rerun_activities
        self._rerun_ids: set[str] = set()

    async def list_runs(self, target: PipelineTarget) -> list[PipelineRun]:
        self.calls.append(("list_runs", target.pipeline_id))
        return [run for run in self.runs if run.item_id == target.pipeline_id]

    async def get_run(self, target: PipelineTarget, run_id: str) -> PipelineRun:
        self.calls.append(("get_run", run_id))
        for run in self.runs:
            if run.item_id == target.pipeline_id and run.id == run_id:
                return run
        raise PipelineApiError("Pipeline run was not found")

    async def rerun(self, target: PipelineTarget) -> PipelineSubmission:
        self.calls.append(("rerun", target.pipeline_id))
        prior = max(
            (run for run in self.runs if run.item_id == target.pipeline_id),
            key=lambda run: run.start_time or datetime.min.replace(tzinfo=UTC),
        )
        started = (prior.end_time or prior.start_time or datetime.now(UTC)) + timedelta(seconds=1)
        run_id = str(uuid5(NAMESPACE_URL, f"{target.key}:{prior.id}:rerun"))
        self._rerun_ids.add(run_id)
        self.runs.append(PipelineRun(
            id=run_id, item_id=target.pipeline_id, status=self.rerun_status,
            job_type="Pipeline", invoke_type="Manual", start_time=started,
            end_time=started if self.rerun_status in PIPELINE_TERMINAL_STATUSES else None,
            error_code="MockRerunFailure" if self.rerun_status == "Failed" else "",
        ))
        return PipelineSubmission(run_id=run_id)

    async def close(self) -> None:
        return None

    async def activity_runs(self, target: PipelineTarget, run: PipelineRun) -> list[PipelineActivity]:
        self.calls.append(("activity_runs", run.id))
        if run.id in self._rerun_ids:
            return list(self.rerun_activities) if self.rerun_activities is not None else [
                PipelineActivity(name="MockPipeline", activity_type="Wait", status="Succeeded")
            ]
        return list(self.activities)


class LiveFabricPipelineClient:
    """Bounded Fabric REST reads and non-retrying, approval-controlled submissions.

    The current Job Scheduler API uses jobs/Pipeline/instances for POST.
    GET remains jobs/instances. Location identifies the new execution; never
    infer it from the latest run or retry POST after an uncertain response.
    """

    def __init__(
        self, *, tenant_id: str, client_id: str = "", max_pages: int = 10,
        credential: Any = None, http_client: httpx.AsyncClient | None = None,
        max_read_retries: int = 2, sleep=asyncio.sleep,
    ) -> None:
        self._tenant_id = canonical_id(tenant_id)
        self._client_id = client_id
        self._max_pages = max_pages
        self._credential = credential
        self._owns_credential = credential is None
        self._http = http_client
        self._owns_http = http_client is None
        self._max_retries = max_read_retries
        self._sleep = sleep
        self._token = ""
        self._token_expires = 0.0
        self._validated_targets: set[str] = set()
        if max_pages < 1:
            raise ValueError("Pipeline pagination limit must be positive")

    async def _get_token(self) -> str:
        if self._token and self._token_expires > datetime.now(UTC).timestamp() + 60:
            return self._token
        if self._credential is None:
            from azure.identity import DefaultAzureCredential

            self._credential = DefaultAzureCredential(
                exclude_environment_credential=True,
                exclude_cli_credential=True,
                exclude_developer_cli_credential=True,
                exclude_powershell_credential=True,
                exclude_interactive_browser_credential=True,
                exclude_shared_token_cache_credential=True,
                exclude_visual_studio_code_credential=True,
                exclude_broker_credential=True,
                managed_identity_client_id=self._client_id or None,
            )
        token = await asyncio.to_thread(self._credential.get_token, _SCOPE)
        self._token, self._token_expires = token.token, token.expires_on
        return self._token

    @staticmethod
    def _retry_after(response: httpx.Response) -> int:
        value = response.headers.get("Retry-After", "0")
        try:
            seconds = int(value)
        except ValueError as exc:
            raise PipelineApiError("Fabric returned an invalid Retry-After header") from exc
        if seconds < 0:
            raise PipelineApiError("Fabric returned a negative Retry-After header")
        return seconds

    async def _request(
        self, method: str, path: str, *, read_only: bool = True, **kwargs,
    ) -> httpx.Response:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30, follow_redirects=False)
        token = await self._get_token()
        for attempt in range(self._max_retries + 1):
            try:
                response = await self._http.request(
                    method, f"{_API}{path}",
                    headers={"Authorization": f"Bearer {token}"}, **kwargs,
                )
            except httpx.HTTPError as exc:
                # Read failures are safe to retry. A submission failure is not.
                if read_only and attempt < self._max_retries:
                    logger.warning("Fabric pipeline read transport error; retrying (%s)", type(exc).__name__)
                    await self._sleep(2 ** attempt)
                    continue
                raise PipelineApiError(f"Fabric pipeline transport failed ({type(exc).__name__})") from exc
            if 200 <= response.status_code < 300:
                return response
            retry_after = self._retry_after(response)
            if (
                read_only and response.status_code in {429, 502, 503, 504}
                and attempt < self._max_retries and retry_after <= 30
            ):
                logger.warning("Fabric pipeline read HTTP %d; backing off", response.status_code)
                await self._sleep(max(retry_after, 2 ** attempt))
                continue
            raise PipelineApiError(
                f"Fabric pipeline API HTTP {response.status_code}; "
                f"Retry-After={retry_after}s. Check item permissions, capacity and network access."
            )
        raise PipelineApiError("Fabric pipeline read retry limit exhausted")

    @staticmethod
    def _object(response: httpx.Response) -> dict[str, Any]:
        try:
            value = response.json()
        except ValueError as exc:
            raise PipelineApiError("Fabric returned non-JSON job evidence") from exc
        if not isinstance(value, dict):
            raise PipelineApiError("Fabric returned an unexpected job response shape")
        return value

    @staticmethod
    def _run(raw: Any, target: PipelineTarget, *, retry_after: int = 0) -> PipelineRun:
        if not isinstance(raw, dict):
            raise PipelineApiError("Fabric returned a non-object job")
        if any(
            not isinstance(raw.get(name), str) or not raw[name]
            for name in ("id", "itemId", "status", "jobType", "invokeType")
        ):
            raise PipelineApiError("Fabric job identity/state fields must be non-empty strings")
        if any(
            raw.get(name) is not None and not isinstance(raw[name], str)
            for name in ("startTimeUtc", "endTimeUtc")
        ):
            raise PipelineApiError("Fabric job timestamps must use the documented UTC string format")
        reason = raw.get("failureReason")
        if reason is None:
            reason = {}
        if not isinstance(reason, dict):
            raise PipelineApiError("Fabric returned an invalid failureReason")
        if any(reason.get(name) is not None and not isinstance(reason[name], str) for name in ("errorCode", "message")):
            raise PipelineApiError("Fabric failure code/message must be strings")
        try:
            run = PipelineRun(
                id=raw["id"], item_id=raw["itemId"], status=raw["status"],
                job_type=raw["jobType"], invoke_type=raw["invokeType"],
                start_time=raw.get("startTimeUtc"), end_time=raw.get("endTimeUtc"),
                error_code=str(reason.get("errorCode") or "")[:200],
                failure_reason=str(reason.get("message") or "")[:4000],
                retry_after_seconds=retry_after,
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise PipelineApiError("Fabric returned incomplete or malformed job evidence") from exc
        if run.item_id != target.pipeline_id:
            raise PipelineApiError("Fabric returned a job for another pipeline")
        if run.status not in PIPELINE_JOB_STATUSES:
            raise PipelineApiError("Fabric returned an unknown job status; monitoring coverage is incomplete")
        if run.status == "Completed" and run.end_time is None:
            raise PipelineApiError("Completed pipeline job has no completion timestamp")
        return run

    @staticmethod
    def _next_token(payload: dict[str, Any], path: str) -> str | None:
        token = payload.get("continuationToken")
        uri = payload.get("continuationUri")
        if uri:
            if not isinstance(uri, str):
                raise PipelineApiError("Invalid Fabric continuation URI")
            parsed = urlparse(uri)
            if (
                parsed.scheme != "https" or parsed.netloc != "api.fabric.microsoft.com"
                or parsed.path != f"/v1{path}"
            ):
                raise PipelineApiError("Refusing a continuation outside the configured Fabric resource")
            uri_token = parse_qs(parsed.query).get("continuationToken", [None])[0]
            if not uri_token:
                raise PipelineApiError("Continuation URI has no continuation token")
            if token is not None and (
                not isinstance(token, str) or token != uri_token and unquote(token) != uri_token
            ):
                raise PipelineApiError("Fabric continuation token and URI disagree")
            token = uri_token
        if token is not None and (not isinstance(token, str) or not token):
            raise PipelineApiError("Invalid Fabric continuation token")
        return token

    async def _ensure_pipeline(self, target: PipelineTarget) -> None:
        if target.key in self._validated_targets:
            return
        response = await self._request(
            "GET", f"/workspaces/{target.workspace_id}/items/{target.pipeline_id}",
        )
        item = self._object(response)
        if (
            item.get("id") != target.pipeline_id
            or item.get("type") != "DataPipeline"
            or item.get("workspaceId") != target.workspace_id
        ):
            raise PipelineApiError("Configured target is not the expected Fabric DataPipeline")
        self._validated_targets.add(target.key)

    async def list_runs(self, target: PipelineTarget) -> list[PipelineRun]:
        await self._ensure_pipeline(target)
        path = f"/workspaces/{target.workspace_id}/items/{target.pipeline_id}/jobs/instances"
        token: str | None = None
        seen: set[str] = set()
        runs: dict[str, PipelineRun] = {}
        for _ in range(self._max_pages):
            response = await self._request("GET", path, params={"continuationToken": token} if token else {})
            payload = self._object(response)
            values = payload.get("value")
            if not isinstance(values, list):
                raise PipelineApiError("Fabric job list has no value array")
            for value in values:
                run = self._run(value, target)
                runs[run.id] = run
            token = self._next_token(payload, path)
            if token is None:
                return list(runs.values())
            if token in seen:
                raise PipelineApiError("Fabric repeated a job continuation token")
            seen.add(token)
        raise PipelineApiError("Job pagination limit reached; monitoring coverage is incomplete")

    async def get_run(self, target: PipelineTarget, run_id: str) -> PipelineRun:
        await self._ensure_pipeline(target)
        run_id = canonical_id(run_id)
        response = await self._request(
            "GET", f"/workspaces/{target.workspace_id}/items/{target.pipeline_id}/jobs/instances/{run_id}",
        )
        run = self._run(self._object(response), target, retry_after=self._retry_after(response))
        if run.id != run_id or run.job_type not in PIPELINE_JOB_TYPES:
            raise PipelineApiError("Fabric job response does not match the requested pipeline run")
        return run

    async def rerun(self, target: PipelineTarget) -> PipelineSubmission:
        if not target.permits_rerun:
            raise ValueError("Pipeline replay was not reviewed in configuration")
        await self._ensure_pipeline(target)
        # Pipeline parameters use this plain nested object in Microsoft's
        # Fabric CLI. The Core API's top-level typed array is not equivalent.
        body = {"executionData": {"parameters": target.rerun_parameters}} if target.rerun_parameters else {}
        response = await self._request(
            "POST", f"/workspaces/{target.workspace_id}/items/{target.pipeline_id}/jobs/Pipeline/instances",
            read_only=False, **({"json": body} if body else {}),
        )
        if response.status_code != 202:
            raise PipelineApiError("Pipeline submission returned no 202 acknowledgement")
        location = urlparse(response.headers.get("Location", ""))
        expected_path = f"/v1/workspaces/{target.workspace_id}/items/{target.pipeline_id}/jobs/instances/"
        if (
            location.scheme != "https" or location.netloc != "api.fabric.microsoft.com"
            or not location.path.startswith(expected_path) or location.query or location.fragment
        ):
            raise PipelineApiError("Submission has no trustworthy job-instance Location")
        try:
            return PipelineSubmission(
                run_id=location.path[len(expected_path):],
                retry_after_seconds=self._retry_after(response),
            )
        except ValidationError as exc:
            raise PipelineApiError("Submission Location contains no valid job ID") from exc

    async def activity_runs(self, target: PipelineTarget, run: PipelineRun) -> list[PipelineActivity]:
        await self._ensure_pipeline(target)
        if run.item_id != target.pipeline_id or run.end_time is None:
            raise ValueError("Activity diagnostics require the matching finished job")
        path = f"/workspaces/{target.workspace_id}/datapipelines/pipelineruns/{run.id}/queryactivityruns"
        after = run.start_time or run.end_time - timedelta(days=1)
        body: dict[str, Any] = {
            "lastUpdatedAfter": (after - timedelta(minutes=1)).isoformat(),
            "lastUpdatedBefore": (run.end_time + timedelta(minutes=1)).isoformat(),
            "filters": [],
            "orderBy": [{"orderBy": "ActivityRunStart", "order": "DESC"}],
        }
        activities: list[PipelineActivity] = []
        seen: set[str] = set()
        for _ in range(self._max_pages):
            response = await self._request("POST", path, json=body)
            try:
                payload = response.json()
            except ValueError as exc:
                raise PipelineApiError("Fabric returned non-JSON activity evidence") from exc
            rows = payload if isinstance(payload, list) else payload.get("value") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise PipelineApiError("Fabric activity response has no array")
            for row in rows:
                if not isinstance(row, dict) or row.get("pipelineRunId") != run.id:
                    raise PipelineApiError("Activity evidence is not correlated to the failed job")
                error = row.get("error")
                if error is None:
                    error = {}
                if not isinstance(error, dict):
                    raise PipelineApiError("Fabric returned an invalid activity error")
                if any(error.get(name) is not None and not isinstance(error[name], str) for name in ("errorCode", "message")):
                    raise PipelineApiError("Fabric activity error code/message must be strings")
                try:
                    activities.append(PipelineActivity(
                        name=row["activityName"], activity_type=row["activityType"],
                        status=row["status"], error_code=str(error.get("errorCode") or "")[:200],
                        message=str(error.get("message") or "")[:2000],
                    ))
                except (KeyError, ValueError) as exc:
                    raise PipelineApiError("Fabric returned malformed activity evidence") from exc
            token = self._next_token(payload, path) if isinstance(payload, dict) else None
            if token is None:
                return activities
            if token in seen:
                raise PipelineApiError("Fabric repeated an activity continuation token")
            seen.add(token)
            body["continuationToken"] = token
        raise PipelineApiError("Activity pagination limit reached; diagnostics are incomplete")

    async def close(self) -> None:
        if self._owns_http and self._http is not None:
            await self._http.aclose()
        if self._owns_credential and self._credential is not None:
            await asyncio.to_thread(self._credential.close)
