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
from dataclasses import dataclass, field
from typing import Any, Protocol

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

    @property
    def succeeded(self) -> bool:
        return self.status == "Completed"

    @property
    def throttled(self) -> bool:
        return self.status == "Throttled"


class PowerBIClient(Protocol):
    async def refresh_dataset(self, workspace_id: str, dataset_id: str) -> RefreshOutcome: ...
    async def get_refresh_history(
        self, workspace_id: str, dataset_id: str, top: int = 5
    ) -> list[dict[str, Any]]: ...
    async def rebind_gateway(
        self, workspace_id: str, dataset_id: str, gateway_id: str
    ) -> RefreshOutcome: ...
    async def get_refresh_schedule(
        self, workspace_id: str, dataset_id: str
    ) -> dict[str, Any]: ...
    async def set_refresh_schedule_enabled(
        self, workspace_id: str, dataset_id: str, enabled: bool
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

    async def get_refresh_history(
        self, workspace_id: str, dataset_id: str, top: int = 5
    ) -> list[dict[str, Any]]:
        self.calls.append(("get_refresh_history", {"top": top}))
        return self.history[:top]

    async def rebind_gateway(
        self, workspace_id: str, dataset_id: str, gateway_id: str
    ) -> RefreshOutcome:
        self.calls.append(("rebind_gateway", {"gateway_id": gateway_id}))
        await asyncio.sleep(self.latency_ms / 1000)
        return RefreshOutcome(
            status="Completed",
            request_id=f"mock-rebind-{len(self.calls)}",
            duration_ms=self.latency_ms,
            detail=f"Dataset rebound to gateway {gateway_id}.",
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
            + ". Set POWERBI_WORKSPACE_ID / POWERBI_DATASET_ID, or include the ids in the alert."
        )


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
        poll_seconds: int = 5,
        poll_timeout_seconds: int = 300,
    ):
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._poll_seconds = poll_seconds
        self._poll_timeout = poll_timeout_seconds
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
        _require_ids(workspace_id, dataset_id)
        import httpx

        started = time.time()
        token = await self._get_token()
        headers = {"Authorization": f"Bearer {token}"}
        base = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

        async with httpx.AsyncClient(timeout=60) as client:
            # Capture the newest existing refresh id BEFORE triggering, so a
            # previously-completed run can never be mistaken for this one.
            # Polling `$top=1` alone will happily return yesterday's success
            # during the window before the new refresh appears.
            prior_ids = {
                r.get("requestId")
                for r in (await self._history(client, base, headers, 5))
                if r.get("requestId")
            }

            resp = await client.post(base, headers=headers, json={"notifyOption": "NoNotification"})
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
                    duration_ms=int((time.time() - started) * 1000),
                    retry_after_seconds=retry_after,
                    detail=(
                        "The capacity rejected the refresh because it has exceeded its "
                        f"resource limits. {resp.text[:300]}"
                    ),
                )
            if resp.status_code not in (200, 202):
                return RefreshOutcome(
                    status="Failed",
                    duration_ms=int((time.time() - started) * 1000),
                    detail=f"HTTP {resp.status_code}: {resp.text[:500]}",
                )

            # The 202 carries the new request id in a header on most tenants.
            # Fall back to "the first id we have not seen before".
            target_id = resp.headers.get("RequestId") or resp.headers.get("x-ms-request-id") or ""

            deadline = time.time() + self._poll_timeout
            while time.time() < deadline:
                await asyncio.sleep(self._poll_seconds)
                rows = await self._history(client, base, headers, 10)

                row = None
                if target_id:
                    row = next((r for r in rows if r.get("requestId") == target_id), None)
                else:
                    row = next(
                        (r for r in rows if r.get("requestId") not in prior_ids), None
                    )
                if row is None:
                    continue

                status = row.get("status", "Unknown")
                if status in ("Completed", "Failed", "Disabled"):
                    return RefreshOutcome(
                        status=status,
                        request_id=row.get("requestId", ""),
                        duration_ms=int((time.time() - started) * 1000),
                        detail=str(row.get("serviceExceptionJson", ""))[:500],
                    )

        return RefreshOutcome(
            status="Unknown",
            duration_ms=int((time.time() - started) * 1000),
            detail=f"Refresh did not reach a terminal state within {self._poll_timeout}s",
        )

    @staticmethod
    async def _history(client, base: str, headers: dict, top: int) -> list[dict[str, Any]]:
        resp = await client.get(f"{base}?$top={top}", headers=headers)
        resp.raise_for_status()
        return resp.json().get("value", [])

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
            return resp.json().get("value", [])

    async def rebind_gateway(
        self, workspace_id: str, dataset_id: str, gateway_id: str
    ) -> RefreshOutcome:
        """Bind the dataset to a different gateway.

        Reached only after a human has approved it — the controller enforces
        that, not this client. Kept here rather than in the gate so the gate has
        no idea what it is authorising, which is the correct separation: the
        gate decides *whether*, the client decides *how*.
        """
        import httpx

        started = time.time()
        token = await self._get_token()
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/Default.BindToGateway"
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json={"gatewayObjectId": gateway_id},
            )
            ok = resp.status_code in (200, 202)
            return RefreshOutcome(
                status="Completed" if ok else "Failed",
                duration_ms=int((time.time() - started) * 1000),
                detail=(
                    f"Dataset rebound to gateway {gateway_id}."
                    if ok
                    else f"HTTP {resp.status_code}: {resp.text[:400]}"
                ),
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
            if resp.status_code >= 400:
                logger.warning(
                    "Could not read refresh schedule: HTTP %s %s",
                    resp.status_code,
                    resp.text[:200],
                )
                # Unknown is not the same as enabled. Returning True here would
                # let the controller conclude there is nothing to fix.
                return {"enabled": None, "error": f"HTTP {resp.status_code}"}
            return resp.json()

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
        import httpx

        started = time.time()
        token = await self._get_token()
        url = f"{_API}/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.patch(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json={"value": {"enabled": enabled}},
            )
            ok = resp.status_code in (200, 202)
            return RefreshOutcome(
                status="Completed" if ok else "Failed",
                duration_ms=int((time.time() - started) * 1000),
                detail=(
                    f"Refresh schedule {'enabled' if enabled else 'disabled'}."
                    if ok
                    else f"HTTP {resp.status_code}: {resp.text[:400]}"
                ),
            )
