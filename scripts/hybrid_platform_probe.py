"""Prepare isolated canaries and probe managed-identity Eventstream consumption.

``emit`` is offline and only prints proposed create-request bodies. These bodies
still require a live definition round-trip and delivery proof. ``receive`` is an
explicit live, transport-only probe: it never creates resources, submits jobs,
queries keys, writes SQL, or advances a durable checkpoint.

The published destination /connection API returns accessKeys, so this probe does
not call it. Supply only the nonsecret Entra endpoint fields, with owned item IDs.
The API source-type enum and documented CloudEvents type names differ; preserve
that distinction until the owned canary establishes the actual wire contract.

References:
https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-topology
https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-destination-connection
https://learn.microsoft.com/fabric/real-time-hub/explore-fabric-job-events
https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/semantic-model-definition
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import hashlib
import json
import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

LOG = logging.getLogger("triage.hybrid_platform_probe")
API_EVENT_TYPES = tuple(
    f"Microsoft.Fabric.JobEvents.{suffix}"
    for suffix in ("ItemJobCreated", "ItemJobStatusChanged", "ItemJobSucceeded", "ItemJobFailed")
)
WIRE_EVENT_TYPES = tuple(value.replace(".JobEvents.", ".") for value in API_EVENT_TYPES)
# These exact names were observed through the owned managed-identity canary.
# Keep the original type in receipts; do not normalize arbitrary namespaces.
WIRE_EVENT_TYPES += (
    "Microsoft.Fabric.JobEvents.ItemJobCreated",
    "Microsoft.Fabric.JobEvents.ItemJobFailed",
    "Microsoft.Fabric.JobEvents.ItemJobSucceeded",
    "Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
)
MAX_EVENT_BYTES = 64 * 1024


class ProbeError(ValueError):
    """A probe prerequisite or evidence contract was not met."""


def canonical_id(value: object) -> str:
    if not isinstance(value, str):
        raise ProbeError("An explicit UUID is required")
    try:
        return str(UUID(value))
    except ValueError:
        raise ProbeError("An explicit UUID is required") from None


def canary_name(value: str) -> str:
    if not re.fullmatch(r"hybrid-canary-[a-z0-9][a-z0-9-]{0,63}", value):
        raise ProbeError("Use a hybrid-canary- name with lowercase letters, digits or hyphens")
    return value


def definition(parts: Mapping[str, object], *, format_name: str | None = None) -> dict:
    result: dict = {
        "parts": [
            {
                "path": path,
                "payload": base64.b64encode(
                    json.dumps(content, separators=(",", ":")).encode("utf-8")
                ).decode("ascii"),
                "payloadType": "InlineBase64",
            }
            for path, content in parts.items()
        ]
    }
    if format_name is not None:
        result["format"] = format_name
    return result


def eventstream_body(name: str, workspace_id: str, item_id: str) -> dict:
    """Candidate POST /workspaces/{workspaceId}/eventstreams body, not live proof."""
    source_name, stream_name = "owned-job-source", "owned-job-stream"
    topology = {
        "compatibilityLevel": "1.1",
        "sources": [{
            "name": source_name,
            "type": "FabricJobEvents",
            "properties": {
                "eventScope": "Item",
                "workspaceId": canonical_id(workspace_id),
                "itemId": canonical_id(item_id),
                "includedEventTypes": list(API_EVENT_TYPES),
            },
        }],
        "streams": [{
            "name": stream_name,
            "type": "DefaultStream",
            "properties": {},
            "inputNodes": [{"name": source_name}],
        }],
        "destinations": [{
            "name": "owned-entra-consumer",
            "type": "CustomEndpoint",
            "properties": {},
            "inputNodes": [{"name": stream_name}],
        }],
        "operators": [],
    }
    return {
        "displayName": canary_name(name),
        "description": "Owned hybrid monitoring canary. No business data or remediation.",
        "definition": definition({
            "eventstream.json": topology,
            "eventstreamProperties.json": {
                "retentionTimeInDays": 1,
                "eventThroughputLevel": "Low",
            },
        }, format_name="eventstream"),
    }


def pipeline_body(name: str, variant: str) -> dict:
    """POST /workspaces/{workspaceId}/items body; only Fail or Wait activities."""
    if variant == "failure":
        activity = {
            "name": "OwnedCanaryFailure",
            "type": "Fail",
            "typeProperties": {
                "message": "Owned hybrid monitoring canary failure",
                "errorCode": "HYBRID_CANARY",
            },
        }
    elif variant in ("success", "cancellation"):
        activity = {
            "name": "OwnedCanaryWait",
            "type": "Wait",
            "typeProperties": {"waitTimeInSeconds": 300 if variant == "cancellation" else 1},
        }
    else:
        raise ProbeError("Pipeline variant must be failure, success or cancellation")
    return {
        "displayName": canary_name(name),
        "type": "DataPipeline",
        "description": "Owned canary containing only a Fail or Wait activity.",
        "definition": definition({
            "pipeline-content.json": {
                "name": name,
                "properties": {"activities": [activity]},
            },
        }),
    }


def semantic_model_body(name: str) -> dict:
    """POST /workspaces/{workspaceId}/semanticModels body; no external data source."""
    model = {
        "compatibilityLevel": 1600,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "tables": [{
                "name": "Canary",
                "columns": [{
                    "name": "Value", "dataType": "int64", "sourceColumn": "Value",
                    "summarizeBy": "none",
                }],
                "partitions": [{
                    "name": "Canary",
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": (
                            "Function.InvokeAfter("
                            "() => #table(type table [Value = Int64.Type], {{1}}),"
                            " #duration(0, 0, 0, 20))"
                        ),
                    },
                }],
            }],
        },
    }
    return {
        "displayName": canary_name(name),
        "description": "Owned refresh-correlation canary; one inline row, no external source.",
        "definition": definition({
            "model.bim": model,
            "definition.pbism": {"version": "5.0", "settings": {}},
        }, format_name="TMSL"),
    }


@dataclass(frozen=True)
class Endpoint:
    namespace: str
    event_hub: str
    consumer_group: str
    workspace_id: str
    eventstream_id: str
    destination_id: str

    @classmethod
    def from_document(cls, value: object) -> Endpoint:
        expected_keys = {
            "type", "fullyQualifiedNamespace", "eventHubName", "consumerGroupName",
            "workspaceId", "eventstreamId", "destinationId",
        }
        if not isinstance(value, dict) or set(value) != expected_keys:
            raise ProbeError("Provide only the seven nonsecret endpoint and owned-item fields")
        if value["type"] != "CustomEndpoint":
            raise ProbeError("Only a CustomEndpoint destination is supported")
        namespace = value["fullyQualifiedNamespace"]
        if not isinstance(namespace, str) or not re.fullmatch(
            r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.servicebus\.windows\.net", namespace
        ):
            raise ProbeError("Expected a public TLS Event Hubs hostname without a URL or credentials")
        for key in ("eventHubName", "consumerGroupName"):
            if not isinstance(value[key], str) or not re.fullmatch(r"[\w$.-]{1,256}", value[key], re.ASCII):
                raise ProbeError("Expected an explicit nonsecret entity and consumer group")
        return cls(
            namespace, value["eventHubName"], value["consumerGroupName"],
            canonical_id(value["workspaceId"]), canonical_id(value["eventstreamId"]),
            canonical_id(value["destinationId"]),
        )


def assert_identity(
    token: str, *, tenant_id: str, client_id: str, object_id: str,
    subscription_id: str, identity_resource_id: str,
) -> None:
    """Check identity provenance of an SDK token; this is not JWT authorization."""
    try:
        part = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
    except (IndexError, ValueError, UnicodeDecodeError, binascii.Error):
        raise ProbeError("The managed-identity token could not be checked") from None
    if not isinstance(claims, dict):
        raise ProbeError("The managed-identity token could not be checked")
    expected = (canonical_id(tenant_id), canonical_id(client_id), canonical_id(object_id))
    actual = tuple(canonical_id(value) for value in (
        claims.get("tid"), claims.get("appid") or claims.get("azp"), claims.get("oid"),
    ))
    if actual != expected:
        raise ProbeError("The SDK returned an unexpected tenant or managed identity")
    identity_path = (
        rf"/subscriptions/{re.escape(canonical_id(subscription_id))}"
        r"/resourcegroups/[^/]+/providers/microsoft\.managedidentity/userassignedidentities/[^/]+"
    )
    if not re.fullmatch(identity_path, identity_resource_id, re.IGNORECASE):
        raise ProbeError("The UAMI resource ID is outside the pinned subscription")
    mirid = claims.get("xms_mirid")
    if not isinstance(mirid, str) or mirid.casefold() != identity_resource_id.casefold():
        raise ProbeError("The SDK token does not identify the expected Azure UAMI resource")


def event_receipt(raw: bytes, *, tenant_id: str, workspace_id: str, item_id: str) -> dict:
    if len(raw) > MAX_EVENT_BYTES:
        raise ProbeError("Event exceeds the canary evidence size limit")
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise ProbeError("The received event is not a JSON CloudEvent") from None
    if not isinstance(event, dict) or not isinstance(event.get("data"), dict):
        raise ProbeError("Expected one JSON CloudEvent with a data object")
    data = event["data"]
    if event.get("specversion") != "1.0" or event.get("type") not in WIRE_EVENT_TYPES:
        raise ProbeError("The observed event type or schema needs explicit platform verification")
    tenant, workspace, item = map(canonical_id, (tenant_id, workspace_id, item_id))
    if (
        canonical_id(event.get("source")) != tenant
        or canonical_id(data.get("workspaceId")) != workspace
        or canonical_id(data.get("itemId")) != item
    ):
        raise ProbeError("The event is outside the owned canary")
    job_id = canonical_id(data.get("jobInstanceId"))
    if event.get("subject") != f"/workspaces/{workspace}/items/{item}/jobs/instances/{job_id}":
        raise ProbeError("The event subject disagrees with its job identity")
    event_id = event.get("id")
    if not isinstance(event_id, str) or not re.fullmatch(r"[\w.-]{1,256}", event_id, re.ASCII):
        raise ProbeError("The event has no bounded original event ID")
    timestamp = event.get("time")
    if not isinstance(timestamp, str) or len(timestamp) > 64:
        raise ProbeError("The event has no bounded timestamp")
    try:
        parsed_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        raise ProbeError("The event timestamp is invalid") from None
    if parsed_time.tzinfo is None:
        raise ProbeError("The event timestamp has no timezone")
    observed = {}
    for key in ("jobStatus", "jobType", "jobInovkeType", "jobInvokeType"):
        value = data.get(key)
        if value is not None:
            if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value):
                raise ProbeError("The event has an unexpected job metadata value")
            observed[key] = value
    return {
        "event_id": event_id, "source": event["source"], "type": event["type"],
        "workspace_id": workspace, "item_id": item, "job_instance_id": job_id,
        "event_time": timestamp, "observed_job_metadata": observed,
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
        "source_rest_verified": False, "sql_acceptance_proven": False,
    }


def envelope_shape(raw: bytes) -> dict[str, object]:
    """Describe known wire fields without logging arbitrary event values."""
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeError, RecursionError):
        return {"json_valid": False}
    if not isinstance(event, dict):
        return {"json_valid": True, "root_type": type(event).__name__}
    data = event.get("data")
    event_type = event.get("type")
    return {
        "json_valid": True,
        "root_type": "object",
        "specversion_1_0": event.get("specversion") == "1.0",
        "dataschemaversion_1_0": event.get("dataschemaversion") == "1.0",
        "recognized_type": event_type if event_type in (*API_EVENT_TYPES, *WIRE_EVENT_TYPES) else None,
        "known_root_fields": [
            key for key in ("specversion", "specVersion", "dataschemaversion", "type", "id", "source", "subject", "time", "data")
            if key in event
        ],
        "data_type": type(data).__name__,
        "known_data_fields": [
            key for key in ("workspaceId", "itemId", "jobInstanceId", "jobStatus", "jobType", "jobInvokeType", "jobInovkeType")
            if isinstance(data, dict) and key in data
        ],
    }


def bounded_event_body(sections: bytes | Iterable[bytes]) -> tuple[bytes | None, int, str]:
    """Retain at most 64 KiB, while hashing/counting the complete broker body."""
    if isinstance(sections, bytes):
        sections = (sections,)
    chunks = []
    size = 0
    digest = hashlib.sha256()
    for section in sections:
        if not isinstance(section, bytes):
            raise ProbeError("Expected an AMQP data body with byte sections")
        size += len(section)
        digest.update(section)
        if size <= MAX_EVENT_BYTES:
            chunks.append(section)
        else:
            chunks.clear()
    return b"".join(chunks) if size <= MAX_EVENT_BYTES else None, size, digest.hexdigest()


async def receive(
    endpoint: Endpoint, *, tenant_id: str, client_id: str, object_id: str,
    subscription_id: str, identity_resource_id: str, item_id: str, seconds: int,
    max_output_records: int = 50,
) -> int:
    from azure.core.exceptions import AzureError
    from azure.eventhub import TransportType
    from azure.eventhub.aio import EventHubConsumerClient
    from azure.identity.aio import ManagedIdentityCredential

    tenant_id, client_id, object_id, item_id = map(
        canonical_id, (tenant_id, client_id, object_id, item_id)
    )
    if not 1 <= seconds <= 900:
        raise ProbeError("Receive duration must be between 1 and 900 seconds")
    if type(max_output_records) is not int or not 1 <= max_output_records <= 200:
        raise ProbeError("Probe output limit must be between 1 and 200 records")
    count = 0
    rejected = 0
    output_count = 0
    receiver_ready = False
    failure = asyncio.get_running_loop().create_future()

    async def on_error(partition: Any, error: Exception) -> None:
        if not failure.done():
            failure.set_result(type(error).__name__)

    async with ManagedIdentityCredential(client_id=client_id, retry_total=0) as credential:
        class PinnedCredential:
            async def get_token(self, *scopes: str, **kwargs: Any) -> Any:
                token = await credential.get_token(*scopes, **kwargs)
                assert_identity(
                    token.token, tenant_id=tenant_id, client_id=client_id, object_id=object_id,
                    subscription_id=subscription_id, identity_resource_id=identity_resource_id,
                )
                return token

        async def on_event(partition: Any, event: Any) -> None:
            nonlocal count, rejected, output_count, receiver_ready
            if not receiver_ready:
                receiver_ready = True
                print(json.dumps({
                    "stage": "receiver_ready",
                    "basis": "event" if event is not None else "receive_poll",
                    "sql_acceptance_proven": False, "event_delivery_verified": False,
                }), flush=True)
            if event is None:
                return
            body_metadata = {}
            raw = None
            try:
                raw, size, digest = bounded_event_body(event.body)
                body_metadata = {"payload_bytes": size, "payload_sha256": digest}
                if raw is None:
                    raise ProbeError("Event exceeds the canary evidence size limit")
                if (
                    not isinstance(partition.partition_id, str)
                    or not re.fullmatch(r"[0-9]{1,10}", partition.partition_id)
                    or type(event.sequence_number) is not int or event.sequence_number < 0
                    or not re.fullmatch(r"[0-9]{1,256}", str(event.offset))
                ):
                    raise ProbeError("The received event has invalid broker position metadata")
                receipt = event_receipt(
                    raw, tenant_id=tenant_id, workspace_id=endpoint.workspace_id, item_id=item_id,
                )
            except ProbeError as exc:
                rejected += 1
                if output_count < max_output_records:
                    print(json.dumps({
                        "stage": "envelope_refused",
                        "reason": str(exc)[:256],
                        **body_metadata,
                        **({"envelope_shape": envelope_shape(raw)} if raw is not None else {}),
                    }), flush=True)
                    output_count += 1
                return
            count += 1
            if output_count < max_output_records:
                print(json.dumps({
                    "stage": "transport_receipt", "partition": partition.partition_id,
                    "sequence_number": event.sequence_number, "offset": event.offset,
                    "payload_bytes": size, "envelope_shape": envelope_shape(raw), **receipt,
                }), flush=True)
                output_count += 1
            # A received envelope is not a SQL commit. Do not checkpoint here.

        try:
            async with EventHubConsumerClient(
                fully_qualified_namespace=endpoint.namespace, eventhub_name=endpoint.event_hub,
                consumer_group=endpoint.consumer_group, credential=PinnedCredential(),
                transport_type=TransportType.AmqpOverWebsocket, logging_enable=False,
                retry_total=0,
            ) as consumer:
                receive_task = None
                timeout = asyncio.timeout(seconds)
                try:
                    async with timeout:
                        partitions = await consumer.get_partition_ids()
                        if not partitions:
                            raise ProbeError("The owned endpoint returned no partitions")
                        print(json.dumps({
                            "stage": "managed_identity_connected", "partition_count": len(partitions),
                            "sql_acceptance_proven": False,
                        }), flush=True)
                        receive_task = asyncio.create_task(consumer.receive(
                            on_event=on_event, starting_position="-1",
                            max_wait_time=5, prefetch=20, on_error=on_error,
                        ))
                        done, _ = await asyncio.wait(
                            (receive_task, failure), return_when=asyncio.FIRST_COMPLETED,
                        )
                        if failure in done:
                            raise ProbeError(f"Managed-identity receive failed ({failure.result()})")
                        await receive_task
                except TimeoutError:
                    if not timeout.expired():
                        raise
                finally:
                    if receive_task is not None:
                        receive_task.cancel()
                        await asyncio.gather(receive_task, return_exceptions=True)
        except AzureError as exc:
            raise ProbeError(f"Managed-identity transport failed ({type(exc).__name__})") from None
    print(json.dumps({
        "stage": "transport_probe_finished", "owned_envelopes_received": count,
        "rejected_envelopes": rejected,
        "suppressed_records": max(0, count + rejected - output_count),
        "sql_acceptance_proven": False, "source_rest_verified": False,
        "durable_checkpoint_written": False,
    }), flush=True)
    if count == 0:
        raise ProbeError("No owned job envelope was received; event readiness is unproved")
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    emit = commands.add_parser("emit", help="Print a proposed body without making network requests")
    emit.add_argument("kind", choices=("eventstream", "pipeline", "semantic-model"))
    emit.add_argument("--name", required=True)
    emit.add_argument("--workspace-id")
    emit.add_argument("--item-id")
    emit.add_argument("--variant", default="failure", choices=("failure", "success", "cancellation"))
    consume = commands.add_parser("receive", help="Live UAMI transport-only probe; no SQL writes")
    consume.add_argument("--endpoint-metadata", required=True, type=Path)
    for name in (
        "tenant-id", "client-id", "object-id", "subscription-id", "identity-resource-id", "item-id",
    ):
        consume.add_argument(f"--{name}", required=True)
    consume.add_argument("--seconds", type=int, default=120)
    args = parser.parse_args(argv)
    try:
        if args.command == "emit":
            if args.kind == "eventstream":
                body = eventstream_body(args.name, args.workspace_id, args.item_id)
            elif args.kind == "pipeline":
                body = pipeline_body(args.name, args.variant)
            else:
                body = semantic_model_body(args.name)
            print(json.dumps(body, indent=2))
        else:
            endpoint = Endpoint.from_document(json.loads(args.endpoint_metadata.read_text("utf-8")))
            asyncio.run(receive(
                endpoint, tenant_id=args.tenant_id, client_id=args.client_id,
                object_id=args.object_id, item_id=args.item_id, seconds=args.seconds,
                subscription_id=args.subscription_id, identity_resource_id=args.identity_resource_id,
            ))
    except (ProbeError, OSError, json.JSONDecodeError) as exc:
        LOG.error("Canary probe failed: %s", exc)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
