"""Optional GET-only readiness projections for one explicitly owned canary scope.

No names, users, response bodies, tokens, permissions changes or workload POSTs
are emitted. Each proof is independent; preview inventory is not transport
acceptance, source telemetry, complete inventory or remediation authorization.
Standalone input: MONITORING_READINESS_PROBE_INPUT with workspaceId, pipelineId,
modelId. Identity bindings use the existing probe's nonsecret environment fields.

https://learn.microsoft.com/rest/api/power-bi/admin/groups-get-group-as-admin
https://learn.microsoft.com/rest/api/fabric/admin/items/list-items
https://learn.microsoft.com/rest/api/power-bi/datasets/get-refresh-history-in-group
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from scripts.hybrid_platform_probe import assert_identity, canonical_id

LOG = logging.getLogger("triage.monitoring_readiness_probe")
MAX_RESPONSE_BYTES = 262_144
PROOF_NAMES = (
    "pipeline_item_read_verified",
    "pipeline_history_read_verified",
    "model_refresh_history_read_verified",
    "powerbi_admin_workspace_read_verified",
    "fabric_admin_items_preview_read_verified",
)
SCOPES = {
    "fabric": "https://api.fabric.microsoft.com/.default",
    "powerbi": "https://analysis.windows.net/powerbi/api/.default",
}
AUDIENCES = {
    "fabric": {"https://api.fabric.microsoft.com"},
    "powerbi": {
        "https://analysis.windows.net/powerbi/api",
        "00000009-0000-0000-c000-000000000000",
    },
}


class ReadinessError(ValueError):
    """Only a bounded machine code, never input or response text."""


def owned_id(value: object) -> str:
    try:
        result = canonical_id(value)
        if UUID(result).int == 0:
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ReadinessError("owned_id_invalid") from None


@dataclass(frozen=True)
class OwnedScope:
    workspace_id: str
    pipeline_id: str
    model_id: str

    def __post_init__(self) -> None:
        for name in ("workspace_id", "pipeline_id", "model_id"):
            object.__setattr__(self, name, owned_id(getattr(self, name)))

    @classmethod
    def parse(cls, value: object) -> OwnedScope:
        if not isinstance(value, dict) or set(value) != {"workspaceId", "pipelineId", "modelId"}:
            raise ReadinessError("owned_scope_fields_invalid")
        return cls(*(owned_id(value[key]) for key in ("workspaceId", "pipelineId", "modelId")))


@dataclass(frozen=True)
class Route:
    proof: str
    service: str
    url: str


def routes(scope: OwnedScope) -> tuple[Route, ...]:
    fabric = "https://api.fabric.microsoft.com/v1"
    powerbi = "https://api.powerbi.com/v1.0/myorg"
    item = f"{fabric}/workspaces/{scope.workspace_id}/items/{scope.pipeline_id}"
    return (
        Route(PROOF_NAMES[0], "fabric", item),
        Route(PROOF_NAMES[1], "fabric", item + "/jobs/instances"),
        Route(
            PROOF_NAMES[2],
            "powerbi",
            f"{powerbi}/groups/{scope.workspace_id}/datasets/{scope.model_id}/refreshes?$top=1",
        ),
        Route(PROOF_NAMES[3], "powerbi", f"{powerbi}/admin/groups/{scope.workspace_id}"),
        Route(PROOF_NAMES[4], "fabric", f"{fabric}/admin/items?workspaceId={scope.workspace_id}"),
    )


@dataclass(frozen=True)
class Reply:
    status: int
    body: object


class Credential(Protocol):
    async def get_token(self, *scopes: str, **kwargs: object): ...
    async def close(self) -> None: ...


def projection_gap(code: str) -> dict:
    return {
        "stage": "readiness_projection",
        "proofs": dict.fromkeys(PROOF_NAMES, False),
        "checks": [{"proof": name, "verified": False, "gap": code} for name in PROOF_NAMES],
        "normal_worker_ready": False,
        "transport_acceptance_affected": False,
        "inventory_complete": False,
        "remediation_authorized": False,
    }


def project(proof: str, value: object, scope: OwnedScope) -> dict:
    if not isinstance(value, dict):
        raise ReadinessError("response_shape_invalid")
    if proof == "powerbi_admin_workspace_read_verified":
        if owned_id(value.get("id")) != scope.workspace_id:
            raise ReadinessError("workspace_binding_mismatch")
        return {}
    if proof == "pipeline_item_read_verified":
        if (
            owned_id(value.get("id")) != scope.pipeline_id
            or value.get("type") != "DataPipeline"
            or (
                value.get("workspaceId") is not None
                and owned_id(value["workspaceId"]) != scope.workspace_id
            )
        ):
            raise ReadinessError("pipeline_binding_mismatch")
        return {}
    key = "itemEntities" if proof == "fabric_admin_items_preview_read_verified" else "value"
    rows = value.get(key)
    if not isinstance(rows, list) or len(rows) > 10_000:
        raise ReadinessError("collection_shape_invalid")
    ids = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ReadinessError("collection_row_invalid")
        if key == "itemEntities":
            if owned_id(row.get("workspaceId")) != scope.workspace_id:
                raise ReadinessError("inventory_scope_mismatch")
            ids.add(owned_id(row.get("id")))
        elif proof == "pipeline_history_read_verified" and row.get("itemId") is not None:
            if owned_id(row["itemId"]) != scope.pipeline_id:
                raise ReadinessError("history_scope_mismatch")
    result = {"records_in_page": len(rows)}
    if key == "itemEntities":
        result.update(
            preview=True,
            has_continuation=bool(value.get("continuationToken") or value.get("continuationUri")),
            owned_pipeline_seen=scope.pipeline_id in ids,
            owned_model_seen=scope.model_id in ids,
        )
    return result


def token_binding(token, service: str, identity: dict) -> str:
    try:
        raw = token.token
        if not isinstance(raw, str) or len(raw) > 32_768:
            raise ValueError
        assert_identity(raw, **identity)
        part = raw.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
        if (
            not isinstance(claims, dict)
            or not isinstance(claims.get("aud"), str)
            or claims["aud"].rstrip("/") not in AUDIENCES[service]
            or type(claims.get("exp")) is not int
            or min(claims["exp"], token.expires_on) <= time.time()
        ):
            raise ValueError
        return raw
    except (ValueError, KeyError, TypeError, AttributeError, IndexError):
        raise ReadinessError("managed_identity_token_binding_failed") from None


async def probe_readiness(
    environment: Mapping[str, str],
    scope: OwnedScope,
    *,
    credential: Credential | None = None,
    get: Callable[[str, str], Awaitable[Reply]] | None = None,
    deadline_seconds: float = 25,
) -> dict:
    """At most five fixed GETs, no retries/redirects; every failure is advisory."""
    if not 0 < deadline_seconds <= 30:
        raise ReadinessError("readiness_deadline_invalid")
    report = projection_gap("not_checked")
    checks = {value["proof"]: value for value in report["checks"]}
    created_credential = False
    session = None
    try:
        identity = {
            "tenant_id": owned_id(environment.get("AZURE_TENANT_ID")),
            "client_id": owned_id(environment.get("AZURE_CLIENT_ID")),
            "object_id": owned_id(environment.get("MONITORING_IDENTITY_OBJECT_ID")),
            "subscription_id": owned_id(environment.get("AZURE_SUBSCRIPTION_ID")),
            "identity_resource_id": environment.get("MONITORING_IDENTITY_RESOURCE_ID", ""),
        }
        if credential is None:
            from azure.identity.aio import ManagedIdentityCredential

            credential = ManagedIdentityCredential(client_id=identity["client_id"], retry_total=0)
            created_credential = True
        if get is None:
            import aiohttp

            session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5), trust_env=False)

            async def get(url: str, token: str) -> Reply:
                async with session.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    allow_redirects=False,
                ) as response:
                    if response.status != 200:
                        return Reply(response.status, None)
                    body = bytearray()
                    async for chunk in response.content.iter_chunked(8192):
                        if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
                            raise ReadinessError("response_size_limit")
                        body.extend(chunk)
                    try:
                        return Reply(response.status, json.loads(body))
                    except (ValueError, UnicodeError, RecursionError):
                        raise ReadinessError("response_json_invalid") from None

        tokens = {}
        token_errors = {}
        async with asyncio.timeout(deadline_seconds):
            for route in routes(scope):
                check = checks[route.proof]
                try:
                    if route.service in token_errors:
                        raise ReadinessError(token_errors[route.service])
                    if route.service not in tokens:
                        try:
                            async with asyncio.timeout(5):
                                token = await credential.get_token(SCOPES[route.service])
                            tokens[route.service] = token_binding(token, route.service, identity)
                        except Exception:
                            token_errors[route.service] = "managed_identity_token_binding_failed"
                            raise ReadinessError(token_errors[route.service]) from None
                    async with asyncio.timeout(5):
                        response = await get(route.url, tokens[route.service])
                    check["http_status"] = response.status
                    if response.status != 200:
                        raise ReadinessError(f"http_{response.status}")
                    check.update(project(route.proof, response.body, scope))
                    check.update(verified=True)
                    check.pop("gap", None)
                    report["proofs"][route.proof] = True
                except Exception as exc:
                    code = (
                        str(exc) if isinstance(exc, ReadinessError) else "readiness_request_failed"
                    )
                    check["gap"] = code
                    LOG.warning("readiness_capability_gap proof=%s code=%s", route.proof, code)
    except TimeoutError:
        for check in checks.values():
            if check.get("gap") == "not_checked":
                check["gap"] = "readiness_deadline_exhausted"
    except Exception as exc:
        code = str(exc) if isinstance(exc, ReadinessError) else "readiness_unavailable"
        for check in checks.values():
            if check.get("gap") == "not_checked":
                check["gap"] = code
        LOG.warning("readiness_capability_gap code=%s", code)
    finally:
        if session is not None:
            try:
                async with asyncio.timeout(1):
                    await session.close()
            except Exception:
                report["cleanup_gap"] = "readiness_http_cleanup_failed"
        if created_credential:
            try:
                async with asyncio.timeout(1):
                    await credential.close()
            except Exception:
                report["cleanup_gap"] = "readiness_identity_cleanup_failed"
    return report


def main() -> int:
    logging.getLogger("azure").setLevel(logging.CRITICAL)
    try:
        raw = os.environ.get("MONITORING_READINESS_PROBE_INPUT", "")
        if len(raw.encode("utf-8")) > 4096:
            raise ReadinessError("owned_scope_input_too_large")
        scope = OwnedScope.parse(json.loads(raw))
        report = asyncio.run(probe_readiness(os.environ, scope))
    except Exception:
        report = projection_gap("owned_scope_or_readiness_invalid")
    print(json.dumps(report), flush=True)
    return 0 if all(report["proofs"].values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())
