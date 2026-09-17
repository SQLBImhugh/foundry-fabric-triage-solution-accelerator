"""Finite owned-source transport canary, independent of the worker/SQL package.

MONITORING_TRANSPORT_PROBE_INPUT contains only endpoint (the public probe's seven
nonsecret fields), sourceWorkspaceId, sourceItemId and seconds (120-240).
Optional readinessModelId enables independent GET-only access projections; their
capability gaps never turn an actual transport receipt into a failed acceptance.
Identity bindings are the same nonsecret environment settings used by the worker.
The public receive helper owns identity checks, WSS443 and event projection.
No SQL, provisioning POST, key API, fixture fallback or retry loop is present.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from uuid import UUID

from scripts.hybrid_platform_probe import Endpoint, ProbeError, canonical_id, receive
from scripts.monitoring_readiness_probe import OwnedScope, probe_readiness, projection_gap

LOG = logging.getLogger("triage.monitoring_transport_probe")


@dataclass(frozen=True)
class TransportProbeInput:
    endpoint: Endpoint
    source_item_id: str
    seconds: int
    readiness_model_id: str | None = None

    @classmethod
    def parse(cls, text: str) -> TransportProbeInput:
        if len(text.encode("utf-8")) > 4096:
            raise ProbeError("Transport probe input exceeds the size bound")
        try:
            value = json.loads(text)
        except (ValueError, UnicodeError):
            raise ProbeError("Transport probe input is not JSON") from None
        required = {
            "endpoint",
            "sourceWorkspaceId",
            "sourceItemId",
            "seconds",
        }
        if not isinstance(value, dict) or not required <= set(value) <= required | {
            "readinessModelId"
        }:
            raise ProbeError("Transport probe input has unknown or missing fields")
        endpoint = Endpoint.from_document(value["endpoint"])
        workspace = canonical_id(value["sourceWorkspaceId"])
        item = canonical_id(value["sourceItemId"])
        if UUID(workspace).int == 0 or UUID(item).int == 0:
            raise ProbeError("Owned source IDs must be nonempty UUIDs")
        if workspace != endpoint.workspace_id:
            raise ProbeError(
                "This finite canary requires its owned source and transport in the same workspace"
            )
        seconds = value["seconds"]
        if type(seconds) is not int or not 120 <= seconds <= 240:
            raise ProbeError("The finite transport canary must run for 120-240 seconds")
        model = None
        if "readinessModelId" in value:
            model = canonical_id(value["readinessModelId"])
            if UUID(model).int == 0:
                raise ProbeError("The readiness model must be a nonempty owned UUID")
        return cls(endpoint, item, seconds, model)


async def run_probe(
    environment: Mapping[str, str],
    *,
    receiver: Callable[..., Awaitable[int]] = receive,
    readiness: Callable[..., Awaitable[dict]] = probe_readiness,
) -> int:
    spec = TransportProbeInput.parse(environment.get("MONITORING_TRANSPORT_PROBE_INPUT", ""))
    identity = {}
    for parameter, name in (
        ("tenant_id", "AZURE_TENANT_ID"),
        ("client_id", "AZURE_CLIENT_ID"),
        ("object_id", "MONITORING_IDENTITY_OBJECT_ID"),
        ("subscription_id", "AZURE_SUBSCRIPTION_ID"),
    ):
        identifier = canonical_id(environment.get(name))
        if UUID(identifier).int == 0:
            raise ProbeError("Managed identity bindings must be nonempty UUIDs")
        identity[parameter] = identifier
    resource = environment.get("MONITORING_IDENTITY_RESOURCE_ID")
    if not resource:
        raise ProbeError("An explicit managed identity resource binding is required")
    print(
        json.dumps(
            {
                "stage": "transport_probe_starting",
                "seconds": spec.seconds,
                "normal_worker_ready": False,
                "sql_acceptance_proven": False,
            }
        ),
        flush=True,
    )

    async def advisory_readiness():
        try:
            async with asyncio.timeout(30):
                return await readiness(
                    environment,
                    OwnedScope(
                        spec.endpoint.workspace_id,
                        spec.source_item_id,
                        spec.readiness_model_id,
                    ),
                )
        except Exception as exc:
            LOG.warning("readiness_capability_gap error_class=%s", type(exc).__name__)
            return projection_gap("readiness_unavailable")

    readiness_task = (
        asyncio.create_task(advisory_readiness()) if spec.readiness_model_id is not None else None
    )
    try:
        count = await receiver(
            spec.endpoint,
            **identity,
            identity_resource_id=resource,
            item_id=spec.source_item_id,
            seconds=spec.seconds,
            max_output_records=50,
        )
    finally:
        if readiness_task is not None:
            try:
                report = await readiness_task
                print(json.dumps(report), flush=True)
            except asyncio.CancelledError:
                readiness_task.cancel()
                await asyncio.gather(readiness_task, return_exceptions=True)
                raise
    if type(count) is not int or count <= 0:
        raise ProbeError("No owned event receipt was verified")
    return count


def main() -> int:
    # The helper emits bounded projected records; suppress SDK exception/body logs.
    logging.getLogger("azure").setLevel(logging.CRITICAL)
    logging.getLogger("uamqp").setLevel(logging.CRITICAL)
    try:
        asyncio.run(run_probe(os.environ))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        LOG.error("transport_probe_blocked error_class=%s", type(exc).__name__)
        print(
            json.dumps(
                {
                    "stage": "transport_probe_blocked",
                    "error_class": type(exc).__name__,
                    "normal_worker_ready": False,
                    "sql_acceptance_proven": False,
                    "durable_checkpoint_written": False,
                }
            ),
            flush=True,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
