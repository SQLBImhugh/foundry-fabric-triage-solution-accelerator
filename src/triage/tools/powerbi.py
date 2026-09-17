"""Power BI REST — the one remediation action in this demo.

Two implementations behind one interface. ``MockPowerBIClient`` is what you
rehearse against; ``LivePowerBIClient`` is what you demo against.

Auth note: dataset refresh needs a token the *dataset* accepts. A service
principal works; so does a managed identity, which can be added to a Fabric /
Power BI workspace like any other principal and avoids secret rotation entirely
when the caller runs in Azure. Either way the tenant setting *"Allow service
principals to use Power BI APIs"* must be enabled and the principal must be a
workspace member. That pair is the most common surprise when moving from demo to
production, so it is stated here rather than discovered on stage.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import UUID

from triage.pipeline_models import canonical_id

logger = logging.getLogger("triage.powerbi")

_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
_API = "https://api.powerbi.com/v1.0/myorg"


@dataclass
class RefreshOutcome:
    status: str  # "Completed" | "Failed" | "Throttled" | "Unknown"
    request_id: str = ""
    duration_ms: int = 0
    detail: str = ""
    #: Seconds the service asked us to wait, from the 429 Retry-After header.
    #: Zero means it did not say, not "retry immediately".
    retry_after_seconds: int = 0
    submission_state: Literal["not_submitted", "submitted", "uncertain", "rejected"] = "not_submitted"
    configuration: dict[str, str | bool | list[str]] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status == "Completed"

    @property
    def throttled(self) -> bool:
        return self.status == "Throttled"


class PowerBIClient(Protocol):
    async def refresh_dataset(self, workspace_id: str, dataset_id: str) -> RefreshOutcome: ...
    async def submit_refresh(self, workspace_id: str, dataset_id: str) -> RefreshOutcome: ...
    async def verify_refresh(
        self, workspace_id: str, dataset_id: str, request_id: str,
    ) -> RefreshOutcome: ...
    async def get_refresh_history(
        self, workspace_id: str, dataset_id: str, top: int = 5
    ) -> list[dict[str, Any]]: ...
    async def rebind_gateway(
        self, workspace_id: str, dataset_id: str, gateway_id: str,
        datasource_ids: list[str] | None = None,
    ) -> RefreshOutcome: ...
    async def verify_gateway_binding(
        self, workspace_id: str, dataset_id: str, gateway_id: str,
        datasource_ids: list[str] | None = None,
    ) -> RefreshOutcome: ...
    async def get_refresh_schedule(
        self, workspace_id: str, dataset_id: str
    ) -> dict[str, Any]: ...
    async def set_refresh_schedule_enabled(
        self, workspace_id: str, dataset_id: str, enabled: bool
    ) -> RefreshOutcome: ...
    async def verify_refresh_schedule(
        self, workspace_id: str, dataset_id: str, enabled: bool,
    ) -> RefreshOutcome: ...


# ---------------------------------------------------------------------------
# Mock
# ---------------------------------------------------------------------------


@dataclass
class MockPowerBIClient:
    """Scripted client. Deterministic, records every call for assertions."""

    refresh_result: str = "Completed"
    history: list[dict[str, Any]] = field(default_factory=list)
    latency_ms: int = 400
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    # Power BI disables a refresh schedule itself after four consecutive
    # failures. Nothing re-arms it, so the report quietly stops updating even
    # after somebody fixes the underlying cause.
    schedule_enabled: bool = True
    #: What a throttled service asks for. Only used when refresh_result is
    #: "Throttled"; zero means the service did not say.
    retry_after_seconds: int = 0
    gateway_id: str = ""
    datasource_ids: list[str] = field(default_factory=list)

    async def refresh_dataset(self, workspace_id: str, dataset_id: str) -> RefreshOutcome:
        self.calls.append(
            ("refresh_dataset", {"workspace_id": workspace_id, "dataset_id": dataset_id})
        )
        await asyncio.sleep(self.latency_ms / 1000)
        outcome = RefreshOutcome(
            status=self.refresh_result,
            request_id=f"mock-refresh-{len(self.calls)}",
            duration_ms=self.latency_ms,
            retry_after_seconds=(
                self.retry_after_seconds if self.refresh_result == "Throttled" else 0
            ),
            detail=(
                "Refresh completed successfully."
                if self.refresh_result == "Completed"
                else (
                    "The capacity rejected the refresh because it has exceeded its "
                    "resource limits."
                    if self.refresh_result == "Throttled"
                    else f"Refresh ended with status {self.refresh_result}."
                )
            ),
            submission_state="rejected" if self.refresh_result == "Throttled" else "submitted",
        )
        # Reflect the refresh in history so a follow-up read is consistent.
        self.history.insert(
            0,
            {
                "requestId": outcome.request_id,
                "status": outcome.status,
                "refreshType": "ViaApi",
                "startTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )
        return outcome

    async def submit_refresh(self, workspace_id: str, dataset_id: str) -> RefreshOutcome:
        return await self.refresh_dataset(workspace_id, dataset_id)

    async def verify_refresh(
        self, workspace_id: str, dataset_id: str, request_id: str,
    ) -> RefreshOutcome:
        row = next((row for row in self.history if row.get("requestId") == request_id), None)
        return RefreshOutcome(
            status=str(row.get("status", "Unknown")) if row else "Unknown",
            request_id=request_id,
            submission_state="submitted",
            detail="" if row else "The submitted refresh is absent from the mock history.",
        )

    async def get_refresh_history(
        self, workspace_id: str, dataset_id: str, top: int = 5
    ) -> list[dict[str, Any]]:
        self.calls.append(("get_refresh_history", {"top": top}))
        return self.history[:top]

    async def rebind_gateway(
        self, workspace_id: str, dataset_id: str, gateway_id: str,
        datasource_ids: list[str] | None = None,
    ) -> RefreshOutcome:
        self.calls.append(("rebind_gateway", {"gateway_id": gateway_id}))
        await asyncio.sleep(self.latency_ms / 1000)
        self.gateway_id = gateway_id
        self.datasource_ids = list(datasource_ids or self.datasource_ids)
        return RefreshOutcome(
            status="Completed",
            request_id=f"mock-rebind-{len(self.calls)}",
            duration_ms=self.latency_ms,
            detail=f"Dataset rebound to gateway {gateway_id}.",
            submission_state="submitted",
            configuration={"gateway_id": gateway_id, "datasource_ids": sorted(self.datasource_ids)},
        )

    async def verify_gateway_binding(
        self, workspace_id: str, dataset_id: str, gateway_id: str,
        datasource_ids: list[str] | None = None,
    ) -> RefreshOutcome:
        matches = self.gateway_id == gateway_id and (
            datasource_ids is None or set(datasource_ids) == set(self.datasource_ids)
        )
        return RefreshOutcome(
            status="Completed" if matches else "Unknown", submission_state="submitted",
            configuration={"gateway_id": self.gateway_id, "datasource_ids": sorted(self.datasource_ids)},
            detail="Mock gateway binding was checked.",
        )

    async def get_refresh_schedule(
        self, workspace_id: str, dataset_id: str
    ) -> dict[str, Any]:
        self.calls.append(("get_refresh_schedule", {}))
        return {
            "enabled": self.schedule_enabled,
            "days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
            "times": ["06:00"],
            "localTimeZoneId": "UTC",
        }

    async def set_refresh_schedule_enabled(
        self, workspace_id: str, dataset_id: str, enabled: bool
    ) -> RefreshOutcome:
        self.calls.append(("set_refresh_schedule_enabled", {"enabled": enabled}))
        await asyncio.sleep(self.latency_ms / 1000)
        self.schedule_enabled = enabled
        return RefreshOutcome(
            status="Completed",
            request_id=f"mock-schedule-{len(self.calls)}",
            duration_ms=self.latency_ms,
            detail=f"Refresh schedule {'enabled' if enabled else 'disabled'}.",
            submission_state="submitted",
            configuration={"enabled": enabled},
        )

    async def verify_refresh_schedule(
        self, workspace_id: str, dataset_id: str, enabled: bool,
    ) -> RefreshOutcome:
        return RefreshOutcome(
            status="Completed" if self.schedule_enabled is enabled else "Unknown",
            submission_state="submitted", configuration={"enabled": self.schedule_enabled},
            detail="Mock refresh schedule was checked.",
        )


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------


def _require_ids(workspace_id: str, dataset_id: str) -> None:
    """Fail loudly rather than calling Power BI with empty path segments.

    An empty id produces `/groups//datasets//refreshes`, which returns 404.
    A 404 is easy for a model to rationalise into a confident conclusion, so
    the run reads as successful while resting on no evidence at all. That
    happened: the agent correctly said "needs human" for entirely the wrong
    reason.
    """
    missing = [
        label
        for label, value in (("workspace_id", workspace_id), ("dataset_id", dataset_id))
        if not (value or "").strip()
    ]
    if missing:
        raise ValueError(
            "Cannot call Power BI without "
            + " and ".join(missing)
            + ". Select a monitored semantic model with verified workspace and item IDs."
        )
    canonical_id(workspace_id)
    canonical_id(dataset_id)


def _submission_id(
    headers: Mapping[str, str], workspace_id: str, dataset_id: str,
) -> tuple[str, str]:
    location = headers.get("Location", "")
    value = headers.get("x-ms-request-id") or headers.get("RequestId", "")
    if location:
        try:
            parsed = urlsplit(location)
        except ValueError:
            return "", "The refresh acknowledgement contains an invalid Location."
        prefix = f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/"
        if (
            parsed.scheme != "https"
            or parsed.netloc.casefold() not in {"api.powerbi.com", "api.powerbi.com:443"}
            or parsed.query
            or parsed.fragment
            or not parsed.path.casefold().startswith(prefix.casefold())
        ):
            return "", "The refresh Location does not identify this Power BI target."
        value = parsed.path[len(prefix):]
    try:
        parsed_id = UUID(value)
    except (ValueError, AttributeError, TypeError):
        return "", "The acknowledgement has no valid exact refresh identifier."
    if not parsed_id.int:
        return "", "The acknowledgement contains an empty refresh identifier."
    return str(parsed_id), ""


class LivePowerBIClient:
    """Client-credentials flow against the Power BI REST API.

    Uses httpx directly rather than MSAL so the base install stays dependency
    light; swap in MSAL if you need token caching across processes.
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        poll_seconds: float = 5,
        poll_timeout_seconds: float = 300,
        credential: Any = None,
    ):
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._poll_seconds = poll_seconds
        self._poll_timeout = poll_timeout_seconds
        self._credential = credential
        self._token: str = ""
        self._token_expires_at: float = 0.0

    async def _get_token(self) -> str:
        import httpx

        if self._token and time.time() < self._token_expires_at - 60:
            return self._token

        if not self._client_secret:
            # Credential-free path. Power BI accepts an Entra agent identity as
            # a workspace principal -- unlike Exchange, which rejects it -- so
            # when this runs as a hosted Foundry agent it triggers refreshes as
            # itself with no secret stored anywhere.
            #
            # Every human credential is excluded from the chain deliberately.
            # DefaultAzureCredential would otherwise fall back to the
            # developer's az login, and an unattended agent that can quietly
            # act as a person is a worse failure than one that cannot start.
            import asyncio

            credential = self._credential
            if credential is None:
                from azure.identity import DefaultAzureCredential

                credential = DefaultAzureCredential(
                    exclude_cli_credential=True,
                    exclude_developer_cli_credential=True,
                    exclude_interactive_browser_credential=True,
                    exclude_shared_token_cache_credential=True,
                    exclude_visual_studio_code_credential=True,
                    managed_identity_client_id=self._client_id or None,
                )
            token = await asyncio.to_thread(credential.get_token, _SCOPE)
            self._token = token.token
            try:
                self._token_expires_at = float(token.expires_on)
            except (AttributeError, TypeError, ValueError):
                self._token_expires_at = 0.0
            return self._token

        url = f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": _SCOPE,
                },
            )
            resp.raise_for_status()
            payload = resp.json()

        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    async def refresh_dataset(self, workspace_id: str, dataset_id: str) -> RefreshOutcome:
        submission = await self.submit_refresh(workspace_id, dataset_id)
        if submission.status != "Submitted":
            return submission
        return await self.verify_refresh(workspace_id, dataset_id, submission.request_id)

    async def submit_refresh(self, workspace_id: str, dataset_id: str) -> RefreshOutcome:
        """Submit once; the caller persists correlation before verifying completion."""
        _require_ids(workspace_id, dataset_id)
        import httpx

        started = time.monotonic()
        token = await self._get_token()
        headers = {"Authorization": f"Bearer {token}"}
        base = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

        async with httpx.AsyncClient(timeout=60) as client:
            try:
                resp = await client.post(
                    base, headers=headers, json={"notifyOption": "NoNotification"},
                )
            except httpx.RequestError as exc:
                logger.warning("Power BI refresh submission is uncertain (%s)", type(exc).__name__)
                return RefreshOutcome(
                    status="Unknown", submission_state="uncertain",
                    duration_ms=int((time.monotonic() - started) * 1000),
                    detail=f"Refresh submission acknowledgement was lost ({type(exc).__name__}). Do not resubmit.",
                )
            if resp.status_code == 429:
                # Throttling is not failure. Reporting it as failure would send
                # the agent looking for a fault in a model that is fine, and
                # invites the one response that makes a saturated capacity
                # worse: retrying into it.
                try:
                    retry_after = int(resp.headers.get("Retry-After", "0"))
                except (TypeError, ValueError):
                    retry_after = 0
                logger.warning(
                    "Refresh throttled by capacity; Retry-After=%s", retry_after or "(absent)"
                )
                return RefreshOutcome(
                    status="Throttled",
                    submission_state="rejected",
                    duration_ms=int((time.monotonic() - started) * 1000),
                    retry_after_seconds=max(0, retry_after),
                    detail=(
                        "The capacity rejected the refresh because it has exceeded its "
                        f"resource limits. {resp.text[:300]}"
                    ),
                )
            if resp.status_code not in (200, 202):
                return RefreshOutcome(
                    status="Unknown" if resp.status_code >= 500 else "Failed",
                    submission_state="uncertain" if resp.status_code >= 500 else "rejected",
                    duration_ms=int((time.monotonic() - started) * 1000),
                    detail=f"HTTP {resp.status_code}: {resp.text[:500]}",
                )

            target_id, error = _submission_id(resp.headers, workspace_id, dataset_id)
            if error:
                logger.warning("Power BI refresh accepted without usable correlation: %s", error)
            return RefreshOutcome(
                status="Submitted" if target_id else "Unknown",
                request_id=target_id,
                submission_state="submitted" if target_id else "uncertain",
                duration_ms=int((time.monotonic() - started) * 1000),
                detail=error or "Refresh accepted; exact completion has not yet been verified.",
            )

    async def verify_refresh(
        self, workspace_id: str, dataset_id: str, request_id: str,
    ) -> RefreshOutcome:
        """Read only the submitted refresh; another newly observed run proves nothing."""
        _require_ids(workspace_id, dataset_id)
        target_id = str(UUID(request_id))
        if not UUID(target_id).int:
            raise ValueError("An exact non-empty refresh identifier is required")
        import httpx

        started = time.monotonic()
        token = await self._get_token()
        headers = {"Authorization": f"Bearer {token}"}
        base = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        async with httpx.AsyncClient(timeout=60) as client:
            deadline = time.monotonic() + self._poll_timeout
            while time.monotonic() < deadline:
                await asyncio.sleep(self._poll_seconds)
                rows = await self._history(client, base, headers, 60)
                row = next(
                    (r for r in rows if str(r.get("requestId", "")).casefold() == target_id),
                    None,
                )
                if row is None:
                    continue

                status = row.get("status", "Unknown")
                if status in ("Completed", "Failed", "Disabled"):
                    return RefreshOutcome(
                        status=status,
                        request_id=target_id,
                        submission_state="submitted",
                        duration_ms=int((time.monotonic() - started) * 1000),
                        detail=str(row.get("serviceExceptionJson", ""))[:500],
                    )

        return RefreshOutcome(
            status="Unknown",
            request_id=target_id,
            submission_state="submitted",
            duration_ms=int((time.monotonic() - started) * 1000),
            detail=f"The exact submitted refresh was not verified within {self._poll_timeout}s. Do not resubmit.",
        )

    @staticmethod
    async def _history(client, base: str, headers: dict, top: int) -> list[dict[str, Any]]:
        resp = await client.get(f"{base}?$top={top}", headers=headers)
        resp.raise_for_status()
        return LivePowerBIClient._history_rows(resp.json())

    @staticmethod
    def _history_rows(payload: Any, *, label: str = "refresh history") -> list[dict[str, Any]]:
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("value"), list)
            or any(not isinstance(row, dict) for row in payload["value"])
        ):
            logger.error("Power BI returned malformed %s", label)
            raise ValueError(f"Power BI {label} must contain a value array of records")
        return payload["value"]

    async def get_refresh_history(
        self, workspace_id: str, dataset_id: str, top: int = 5
    ) -> list[dict[str, Any]]:
        _require_ids(workspace_id, dataset_id)
        import httpx

        token = await self._get_token()
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            return self._history_rows(resp.json())

    async def rebind_gateway(
        self, workspace_id: str, dataset_id: str, gateway_id: str,
        datasource_ids: list[str] | None = None,
    ) -> RefreshOutcome:
        """Bind the dataset to a different gateway.

        Reached only after a human has approved it — the controller enforces
        that, not this client. Kept here rather than in the gate so the gate has
        no idea what it is authorising, which is the correct separation: the
        gate decides *whether*, the client decides *how*.
        """
        _require_ids(workspace_id, dataset_id)
        gateway_id = canonical_id(gateway_id)
        if datasource_ids is not None:
            if not datasource_ids:
                raise ValueError("Reviewed datasource IDs must not be empty")
            datasource_ids = sorted({canonical_id(value) for value in datasource_ids})
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/Default.BindToGateway"
        body: dict[str, Any] = {"gatewayObjectId": gateway_id}
        if datasource_ids is not None:
            body["datasourceObjectIds"] = datasource_ids
        submission = await self._submit_configuration("POST", url, body)
        if submission.status != "Submitted":
            return submission
        return await self.verify_gateway_binding(
            workspace_id, dataset_id, gateway_id, datasource_ids,
        )

    async def _submit_configuration(
        self, method: str, url: str, body: dict[str, Any],
    ) -> RefreshOutcome:
        import httpx

        started = time.monotonic()
        token = await self._get_token()
        async with httpx.AsyncClient(timeout=60) as client:
            try:
                response = await client.request(
                    method, url, headers={"Authorization": f"Bearer {token}"}, json=body,
                )
            except httpx.RequestError as exc:
                logger.warning("Power BI configuration submission is uncertain (%s)", type(exc).__name__)
                return RefreshOutcome(
                    status="Unknown", submission_state="uncertain",
                    detail="Configuration submission acknowledgement was lost. Do not repeat the write.",
                )
        if response.status_code in {200, 202}:
            return RefreshOutcome(
                status="Submitted", submission_state="submitted",
                duration_ms=int((time.monotonic() - started) * 1000),
                detail="Configuration request accepted; its resulting state must be verified.",
            )
        uncertain = response.status_code >= 500
        retry_after = 0
        if response.status_code == 429:
            try:
                retry_after = max(0, int(response.headers.get("Retry-After", "0")))
            except (TypeError, ValueError):
                retry_after = 0
        return RefreshOutcome(
            status="Unknown" if uncertain else (
                "Throttled" if response.status_code == 429 else "Failed"
            ),
            submission_state="uncertain" if uncertain else "rejected",
            duration_ms=int((time.monotonic() - started) * 1000),
            retry_after_seconds=retry_after,
            detail=f"Configuration request returned HTTP {response.status_code}.",
        )

    async def verify_gateway_binding(
        self, workspace_id: str, dataset_id: str, gateway_id: str,
        datasource_ids: list[str] | None = None,
    ) -> RefreshOutcome:
        """Verify only the reviewed binding identities, without another mutation."""
        _require_ids(workspace_id, dataset_id)
        gateway_id = canonical_id(gateway_id)
        if datasource_ids is not None and not datasource_ids:
            raise ValueError("Reviewed datasource IDs must not be empty")
        expected = sorted({canonical_id(value) for value in datasource_ids}) if datasource_ids else None
        import httpx

        token = await self._get_token()
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/datasources"
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(url, headers={"Authorization": f"Bearer {token}"})
                response.raise_for_status()
                rows = self._history_rows(response.json(), label="datasource bindings")
            bindings: dict[str, str] = {}
            incomplete = False
            for row in rows:
                source_id, bound_gateway = row.get("datasourceId"), row.get("gatewayId")
                if not source_id or not bound_gateway:
                    incomplete = True
                    continue
                if not isinstance(source_id, str) or not isinstance(bound_gateway, str):
                    raise ValueError("Datasource binding identifiers must be strings")
                source_id, bound_gateway = canonical_id(source_id), canonical_id(bound_gateway)
                if source_id in bindings and bindings[source_id] != bound_gateway:
                    raise ValueError("Datasource binding evidence contradicts itself")
                bindings[source_id] = bound_gateway
            selected = expected if expected is not None else sorted(bindings)
            matches = bool(selected) and all(bindings.get(key) == gateway_id for key in selected)
            if expected is None and incomplete:
                matches = False
            configuration: dict[str, str | bool | list[str]] = {}
            if matches:
                configuration = {"gateway_id": gateway_id, "datasource_ids": selected}
            return RefreshOutcome(
                status="Completed" if matches else "Unknown", submission_state="submitted",
                configuration=configuration,
                detail=(
                    "The exact reviewed datasource bindings match the requested gateway."
                    if matches else "The requested gateway configuration has not been verified."
                ),
            )
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            logger.warning("Gateway binding verification is unavailable (%s)", type(exc).__name__)
            return RefreshOutcome(
                status="Unknown", submission_state="submitted",
                detail="The binding request remains unverified; do not repeat the write.",
            )

    async def get_refresh_schedule(
        self, workspace_id: str, dataset_id: str
    ) -> dict[str, Any]:
        """Read the schedule, including whether Power BI has disabled it.

        Power BI switches a refresh schedule off by itself after four
        consecutive failures. Nothing switches it back on, so a model can be
        fixed and still never refresh again -- which presents to the business
        as a report that is simply, quietly, always a day behind.
        """
        _require_ids(workspace_id, dataset_id)
        import httpx

        token = await self._get_token()
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            resp.raise_for_status()
            value = resp.json()
            if not isinstance(value, dict) or type(value.get("enabled")) is not bool:
                logger.error("Power BI returned an invalid refresh schedule")
                raise ValueError("Refresh schedule has no explicit boolean enabled state")
            return value

    async def set_refresh_schedule_enabled(
        self, workspace_id: str, dataset_id: str, enabled: bool
    ) -> RefreshOutcome:
        """Turn the refresh schedule on or off.

        Reached only after a human has approved it and the controller has
        confirmed the most recent refresh actually succeeded. Re-arming a
        schedule whose cause is unfixed just fails four more times and disables
        it again, having burned a remediation and told somebody it was handled.
        """
        _require_ids(workspace_id, dataset_id)
        if type(enabled) is not bool:
            raise ValueError("Schedule enabled must be an explicit boolean")
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
        submission = await self._submit_configuration("PATCH", url, {"value": {"enabled": enabled}})
        if submission.status != "Submitted":
            return submission
        return await self.verify_refresh_schedule(workspace_id, dataset_id, enabled)

    async def verify_refresh_schedule(
        self, workspace_id: str, dataset_id: str, enabled: bool,
    ) -> RefreshOutcome:
        if type(enabled) is not bool:
            raise ValueError("Schedule enabled must be an explicit boolean")
        import httpx

        try:
            schedule = await self.get_refresh_schedule(workspace_id, dataset_id)
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("Schedule verification is unavailable (%s)", type(exc).__name__)
            return RefreshOutcome(
                status="Unknown", submission_state="submitted",
                detail="The schedule request remains unverified; do not repeat the write.",
            )
        matches = schedule["enabled"] is enabled
        return RefreshOutcome(
            status="Completed" if matches else "Unknown", submission_state="submitted",
            configuration={"enabled": schedule["enabled"]},
            detail="The requested schedule state is verified." if matches else "The schedule state does not yet match the request.",
        )
