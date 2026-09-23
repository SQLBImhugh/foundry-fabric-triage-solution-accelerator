"""Durable, MI-owned Eventstream source reconciliation.

Stored definitions use {"parts": {path: decoded JSON}, "component_ids":
{"sources/name": server ID, ...}}. decode_snapshot builds that bounded document
from Get Definition plus Get Topology; only parts are sent to Update Definition.
An operator binds the initial observed baseline and approved nonsecret endpoint
metadata to the owned connector. This module never adopts an unrecorded topology.

Only the controller publishes desired topology. The worker applies that exact
intent and records restricted observations, not source ownership or readiness
authority. A separate observation receipt records submission intent before POST.
After an uncertain write, recover that original receipt and the exact remote
definition/status; neither current-state equality nor a retry establishes success.

https://learn.microsoft.com/rest/api/fabric/eventstream/items/update-eventstream-definition
https://learn.microsoft.com/rest/api/fabric/eventstream/topology/get-eventstream-topology
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import copy
import json
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import NamedTuple, Protocol
from uuid import UUID, uuid5

import httpx
from pydantic import TypeAdapter, ValidationError

from triage.monitoring.contracts import (
    ControllerMonitoringStore,
    FixedDiagnosticError,
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringReader,
    MonitoringStoreError,
    MonitoringUnavailable,
    WorkerMonitoringStore,
)
from triage.monitoring.engine import inventory_confirms_deletion, policy_removes_target
from triage.monitoring.events import (
    WIRE_TO_SUBSCRIPTION_TYPE,
    ConnectorBinding,
    EventContractError,
)
from triage.monitoring.inventory import (
    RestReadError,
    RestRoute,
    TenantBoundRestClient,
    _retry_delay,
    validate_rest_url,
)
from triage.monitoring.models import (
    SUPERSESSION_EVIDENCE_TTL_SECONDS,
    CapabilityObservation,
    CollectionCommit,
    ConnectorObservationResult,
    ConnectorPresenceInspection,
    ConnectorPublicationRequest,
    ConnectorPublicationResult,
    ConnectorSource,
    ConnectorSourceProposal,
    CoverageGap,
    EventCapabilityEvidence,
    JsonObject,
    LeaseRenewal,
    MonitoringContext,
    MonitoringTarget,
    MonitoringWork,
    MonitoringWorkDraft,
    OwnedConnectorManifest,
    PageQuery,
    PendingSourceRemoval,
    RegistryVersion,
    SourceRemovalIntent,
    TargetIdentity,
    TargetQuery,
    ValidationFrontier,
    WorkClaimRequest,
    WorkDispositionRequest,
    _digest,
    connector_collection_eligible,
    connector_definition_hash,
    validate_connector_definition,
)
from triage.monitoring.rate_limit import RatePolicy
from triage.monitoring.records import stable_id
from triage.pipeline_models import canonical_id

LOG = logging.getLogger("triage.monitoring.provisioning")
JSON_OBJECT = TypeAdapter(JsonObject)
SOURCE_EVENTS = tuple(dict.fromkeys(WIRE_TO_SUBSCRIPTION_TYPE.values()))
FABRIC_ROOT = "https://api.fabric.microsoft.com/v1"
PROVISIONING_POLICIES = {
    "fabric.eventstream.item": RatePolicy(120, 60),
    "fabric.eventstream.definition": RatePolicy(60, 60),
    "fabric.eventstream.topology": RatePolicy(120, 60),
    "fabric.eventstream.update": RatePolicy(30, 60),
    "fabric.operations": RatePolicy(120, 60),
}
INTENT_GAP = "definition_update_submitted_or_unknown"
PRESENCE_GAP = "pending_source_removal_presence_observed"


class ProvisioningReview(FixedDiagnosticError):
    """A fixed diagnostic code, never raw remote definition or HTTP content."""


class UpdateNotSent(RestReadError):
    """Failure before an update request was sent."""


class UpdateUncertain(RestReadError):
    """The remote definition may have changed; reconcile, do not replay POST."""


@dataclass(frozen=True)
class ProvisioningReply:
    status: int
    body: dict
    operation_id: str | None = None
    retry_at: datetime | None = None


def _identifier(value: object) -> str:
    try:
        return canonical_id(value)
    except (ValueError, TypeError):
        raise ProvisioningReview("remote_identifier_unverified") from None


def _bounded_json(raw: bytes) -> dict:
    try:
        value = json.loads(raw) if raw else {}
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise ProvisioningReview("remote_json_unverified") from None


class ProvisioningRestClient(TenantBoundRestClient):
    """Reuse tenant/OID token validation, HTTP lifetime and shared REST budgets."""

    def __init__(self, *args, **kwargs) -> None:
        policies = PROVISIONING_POLICIES | dict(kwargs.pop("api_policies", {}) or {})
        super().__init__(*args, api_policies=policies, **kwargs)

    async def _request(
        self,
        route: RestRoute,
        *,
        method: str = "GET",
        body: dict | None = None,
        update: bool = False,
    ) -> ProvisioningReply:
        url = validate_rest_url(route, route.url)
        if route.api not in PROVISIONING_POLICIES or route.service != "fabric":
            raise ProvisioningReview("provisioning_route_unverified")
        guid = r"[0-9a-f-]{36}"
        resource = rf"/workspaces/{guid}/eventstreams/{guid}"
        routes = {
            "fabric.eventstream.item": ("GET", resource, ()),
            "fabric.eventstream.definition": (
                "POST",
                resource + "/getDefinition",
                (("format", "eventstream"),),
            ),
            "fabric.eventstream.topology": ("GET", resource + "/topology", ()),
            "fabric.eventstream.update": (
                "POST",
                resource + "/updateDefinition",
                (("updateMetadata", "false"),),
            ),
            "fabric.operations": ("GET", rf"/operations/{guid}(?:/result)?", ()),
        }
        expected_method, pattern, query = routes[route.api]
        if (
            method != expected_method
            or re.fullmatch(pattern, route.path) is None
            or route.query != query
            or update != (route.api == "fabric.eventstream.update")
        ):
            raise ProvisioningReview("provisioning_route_unverified")
        try:
            token = await self._token("fabric")
            policies = (
                ("service:fabric", self._service_policies["fabric"]),
                (f"api:{route.api}", self._api_policies[route.api]),
            )
            decision = await asyncio.to_thread(self._budget.acquire_many, self.context, policies)
            if not decision.allowed:
                raise RestReadError(
                    "request_budget_exhausted",
                    "Shared provisioning request budget is exhausted",
                    retry_at=decision.retry_at,
                )
        except (RestReadError, MonitoringStoreError) as exc:
            if update:
                if isinstance(exc, RestReadError):
                    raise UpdateNotSent(exc.code, exc.detail, retry_at=exc.retry_at) from None
                raise UpdateNotSent(
                    "request_budget_unavailable",
                    "Shared request admission was unavailable",
                ) from None
            raise
        try:
            async with self._http.stream(
                method,
                url,
                json=body,
                headers={"Authorization": f"Bearer {token}"},
            ) as response:
                retry_at = None
                if response.status_code == 429 or (
                    response.status_code in {502, 503, 504} and "Retry-After" in response.headers
                ):
                    seconds, valid = _retry_delay(response.headers, decision.checked_at)
                    deferred = [
                        await asyncio.to_thread(
                            self._budget.defer,
                            self.context,
                            bucket,
                            policy,
                            seconds=seconds,
                        )
                        for bucket, policy in policies
                    ]
                    retry_at = max(value.retry_at for value in deferred)
                    failure = UpdateUncertain if update else RestReadError
                    raise failure(
                        "service_throttled" if valid else "service_throttled_invalid_retry_after",
                        "Provisioning response requires shared backoff",
                        status_code=response.status_code,
                        retry_at=retry_at,
                    )
                if response.status_code not in {200, 202}:
                    failure = UpdateUncertain if update else RestReadError
                    raise failure(
                        f"provisioning_http_{response.status_code}",
                        "Provisioning response was not successful",
                        status_code=response.status_code,
                    )
                tenant = response.headers.get("x-ms-tenant-id")
                if tenant is not None and _identifier(tenant) != self.context.tenant_id:
                    raise ProvisioningReview("response_tenant_mismatch")
                raw = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(raw) + len(chunk) > self._max_bytes:
                        raise ProvisioningReview("definition_response_too_large")
                    raw.extend(chunk)
                operation_id = None
                if response.status_code == 202:
                    operation_id = _identifier(response.headers.get("x-ms-operation-id"))
                    if (
                        response.headers.get("Location")
                        != f"{FABRIC_ROOT}/operations/{operation_id}"
                    ):
                        raise ProvisioningReview("operation_location_unverified")
                if "Retry-After" in response.headers:
                    seconds, valid = _retry_delay(response.headers, decision.checked_at)
                    if not valid:
                        raise ProvisioningReview("operation_retry_after_unverified")
                    retry_at = decision.checked_at + timedelta(seconds=seconds)
                return ProvisioningReply(
                    response.status_code,
                    _bounded_json(bytes(raw)),
                    operation_id,
                    retry_at,
                )
        except httpx.HTTPError:
            failure = UpdateUncertain if update else RestReadError
            raise failure(
                "definition_update_outcome_unknown" if update else "provisioning_transport_error",
                "Provisioning transport did not establish a result",
            ) from None
        except ProvisioningReview as exc:
            if update:
                raise UpdateUncertain(
                    "definition_update_outcome_unknown",
                    "Update acknowledgement could not be verified",
                ) from None
            raise exc

    def _item_route(
        self, connector: OwnedConnectorManifest, api: str, suffix: str = ""
    ) -> RestRoute:
        if connector.tenant_id != self.context.tenant_id or connector.epoch != self.context.epoch:
            raise ProvisioningReview("connector_context_mismatch")
        workspace = _identifier(connector.workspace_id)
        item = _identifier(connector.eventstream_id)
        return RestRoute(
            "fabric", api, f"/workspaces/{workspace}/eventstreams/{item}{suffix}", None
        )

    async def operation(self, operation_id: str, *, result: bool = False) -> ProvisioningReply:
        return await self._request(
            RestRoute(
                "fabric",
                "fabric.operations",
                f"/operations/{_identifier(operation_id)}" + ("/result" if result else ""),
                None,
            )
        )

    async def inspect(
        self,
        connector: OwnedConnectorManifest,
        renew: Callable[[], Awaitable[None]],
    ) -> tuple[dict, dict]:
        item = await self._request(self._item_route(connector, "fabric.eventstream.item"))
        if (
            _identifier(item.body.get("id")) != connector.eventstream_id
            or _identifier(item.body.get("workspaceId")) != connector.workspace_id
            or item.body.get("type") != "Eventstream"
        ):
            raise ProvisioningReview("owned_eventstream_resource_mismatch")
        route = self._item_route(connector, "fabric.eventstream.definition", "/getDefinition")
        route = RestRoute(route.service, route.api, route.path, None, (("format", "eventstream"),))
        reply = await self._request(route, method="POST")
        for _ in range(4):
            if reply.status == 200:
                break
            operation_id = reply.operation_id
            if operation_id is None:
                raise ProvisioningReview("definition_operation_unverified")
            delay = max(0, ((reply.retry_at or self._clock()) - self._clock()).total_seconds())
            if delay > 30:
                raise RestReadError(
                    "definition_read_pending",
                    "Definition read requires later reconciliation",
                    retry_at=reply.retry_at,
                )
            await renew()
            if delay:
                await asyncio.sleep(delay)
            status = await self.operation(operation_id)
            if status.body.get("status") == "Succeeded":
                reply = await self.operation(operation_id, result=True)
                break
            if status.body.get("status") not in {"NotStarted", "Running"}:
                raise ProvisioningReview("definition_read_operation_failed")
            reply = ProvisioningReply(202, {}, operation_id, status.retry_at)
        if reply.status != 200:
            raise RestReadError(
                "definition_read_pending",
                "Definition read has not completed",
                retry_at=reply.retry_at,
            )
        topology = await self._request(
            self._item_route(connector, "fabric.eventstream.topology", "/topology")
        )
        return decode_snapshot(reply.body, topology.body), topology.body

    async def update(self, connector: OwnedConnectorManifest, snapshot: dict) -> ProvisioningReply:
        route = self._item_route(connector, "fabric.eventstream.update", "/updateDefinition")
        route = RestRoute(
            route.service, route.api, route.path, None, (("updateMetadata", "false"),)
        )
        return await self._request(
            route, method="POST", body=encode_definition(snapshot), update=True
        )


def _normal(value):
    if isinstance(value, dict):
        return {
            key: sorted(child)
            if key == "includedEventTypes"
            and isinstance(child, list)
            and all(isinstance(item, str) for item in child)
            else _normal(child)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_normal(child) for child in value]
    return value


def _document(snapshot: dict) -> dict:
    try:
        value = JSON_OBJECT.validate_python(snapshot)
        if set(value) != {"parts", "component_ids"}:
            raise ValueError
        if not isinstance(value["parts"], dict) or "eventstream.json" not in value["parts"]:
            raise ValueError
        if not isinstance(value["component_ids"], dict):
            raise ValueError
        return value
    except (ValueError, TypeError):
        raise ProvisioningReview("recorded_definition_baseline_required") from None


def _nodes(topology: dict, kind: str) -> dict[str, dict]:
    raw = topology.get(kind)
    if not isinstance(raw, list):
        raise ProvisioningReview("topology_collection_missing")
    result = {}
    for node in raw:
        if not isinstance(node, dict) or not isinstance(node.get("name"), str):
            raise ProvisioningReview("topology_node_unverified")
        name = node["name"]
        if not name or len(name) > 200 or name in result:
            raise ProvisioningReview("topology_node_identity_ambiguous")
        result[name] = node
    return result


def decode_snapshot(definition: dict, topology: dict) -> dict:
    """Build the canonical stored baseline without base64 truncation or secret APIs."""
    try:
        raw_parts = definition["definition"]["parts"]
        if not isinstance(raw_parts, list) or not 1 <= len(raw_parts) <= 3:
            raise ValueError
        parts = {}
        for part in raw_parts:
            path = part["path"]
            if path not in {"eventstream.json", "eventstreamProperties.json", ".platform"}:
                raise ValueError
            if path in parts or part["payloadType"] != "InlineBase64":
                raise ValueError
            payload = base64.b64decode(part["payload"], validate=True)
            if len(payload) > 65_536:
                raise ValueError
            parts[path] = JSON_OBJECT.validate_python(json.loads(payload))
        if "eventstream.json" not in parts:
            raise ValueError
        graph = parts["eventstream.json"]
        if topology.get("compatibilityLevel") != graph.get("compatibilityLevel") or topology.get(
            "operators"
        ) != graph.get("operators"):
            raise ValueError
        ids = {}
        for kind in ("sources", "streams", "destinations"):
            defined = _nodes(parts["eventstream.json"], kind)
            observed = _nodes(topology, kind)
            if set(defined) != set(observed):
                raise ValueError
            for name, node in observed.items():
                ids[f"{kind}/{name}"] = _identifier(node.get("id"))
                definition_node = defined[name]
                if any(
                    key not in node or _normal(node[key]) != _normal(value)
                    for key, value in definition_node.items()
                    if key not in {"id", "status", "error", "inputSchemas"}
                ):
                    raise ValueError
                if (
                    "id" in definition_node
                    and _identifier(definition_node["id"]) != ids[f"{kind}/{name}"]
                ):
                    raise ValueError
        return _document({"parts": parts, "component_ids": ids})
    except (ValueError, TypeError, KeyError, binascii.Error, RecursionError):
        raise ProvisioningReview("definition_topology_mismatch") from None


def encode_definition(snapshot: dict) -> dict:
    value = _document(snapshot)
    return {
        "definition": {
            "format": "eventstream",
            "parts": [
                {
                    "path": path,
                    "payloadType": "InlineBase64",
                    "payload": base64.b64encode(
                        json.dumps(part, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                    ).decode("ascii"),
                }
                for path, part in value["parts"].items()
            ],
        }
    }


@dataclass(frozen=True)
class DefinitionPlan:
    desired: dict
    target_by_name: dict[str, TargetIdentity]
    new_names: frozenset[str]
    source_proposals: tuple[ConnectorSourceProposal, ...] = ()
    source_removals: tuple[SourceRemovalIntent, ...] = ()
    deferred_targets: tuple[TargetIdentity, ...] = ()


def plan_definition(
    connector: OwnedConnectorManifest,
    baseline: dict,
    targets: tuple[MonitoringTarget, ...],
    *,
    removal_targets: tuple[TargetIdentity, ...] = (),
) -> DefinitionPlan:
    """Missing read admission is not removal authority for an owned source."""
    baseline = _document(baseline)
    result = copy.deepcopy(baseline)
    graph = result["parts"]["eventstream.json"]
    source_nodes = _nodes(graph, "sources")
    streams = _nodes(graph, "streams")
    destinations = _nodes(graph, "destinations")
    if graph.get("operators") != [] or len(streams) != 1 or len(destinations) != 1:
        raise ProvisioningReview("unowned_or_complex_topology_requires_review")
    stream = next(iter(streams.values()))
    destination = next(iter(destinations.values()))
    if stream.get("type") != "DefaultStream" or destination.get("type") != "CustomEndpoint":
        raise ProvisioningReview("unsupported_owned_transport_shape")
    if destination.get("inputNodes") != [{"name": stream["name"]}]:
        raise ProvisioningReview("destination_routing_requires_review")
    if (
        baseline["component_ids"].get(f"destinations/{destination['name']}")
        != connector.destination_id
    ):
        raise ProvisioningReview("destination_identity_changed")
    owned = {source.source_id: source for source in connector.sources}
    proposals = {source.node_name: source for source in connector.source_proposals}
    pending_sources = {
        removal.source_id for removal in connector.source_removals if removal.source_id is not None
    }
    pending_proposals = {
        removal.proposal_id for removal in connector.source_removals if removal.proposal_id is not None
    }
    removals = [removal.intent() for removal in connector.source_removals]
    desired_targets = {target.identity.key: target.identity for target in targets}
    authorized_removals = {target.key for target in removal_targets}
    if authorized_removals.intersection(desired_targets):
        raise ProvisioningReview("source_removal_conflicts_with_current_admission")
    deferred = {
        removal.target.key: removal.target for removal in connector.source_removals
        if removal.target.key in desired_targets
    }
    by_name = {}
    retained = []
    retained_proposals = list(connector.source_proposals)
    seen_owned = set()
    seen_proposals = set()
    removed_names = set()
    for name, node in source_nodes.items():
        source_id = baseline["component_ids"].get(f"sources/{name}")
        registered = owned.get(source_id) if source_id is not None else proposals.get(name)
        if registered is None or node.get("type") != "FabricJobEvents":
            raise ProvisioningReview("unowned_source_requires_review")
        if isinstance(registered, ConnectorSourceProposal):
            if "id" in node:
                raise ProvisioningReview("proposal_has_unverified_physical_identity")
            seen_proposals.add(registered.proposal_id)
        else:
            if node.get("id", source_id) != source_id:
                raise ProvisioningReview("owned_source_definition_drift")
            seen_owned.add(source_id)
        properties = node.get("properties", {})
        if (
            properties.get("eventScope") != "Item"
            or _identifier(properties.get("workspaceId")) != registered.target.workspace_id
            or _identifier(properties.get("itemId")) != registered.target.item_id
            or set(properties.get("includedEventTypes", ())) != set(registered.event_types)
        ):
            raise ProvisioningReview("owned_source_definition_drift")
        removing = (
            registered.proposal_id in pending_proposals
            if isinstance(registered, ConnectorSourceProposal)
            else registered.source_id in pending_sources
        )
        if registered.target.key not in desired_targets and not removing and registered.target.key not in authorized_removals:
            deferred[registered.target.key] = registered.target
        if removing or registered.target.key in authorized_removals:
            if not removing:
                selector = {
                    "source_id": None if isinstance(registered, ConnectorSourceProposal) else registered.source_id,
                    "proposal_id": registered.proposal_id if isinstance(registered, ConnectorSourceProposal) else None,
                }
                removals.append(SourceRemovalIntent(
                    removal_id=str(uuid5(
                        UUID(connector.ownership_id),
                        "source-removal:" + _digest({
                            **selector, "binding": registered.model_dump(mode="json"),
                        }),
                    )),
                    **selector,
                    detail="Source is no longer admitted by the current controller scope",
                ))
            removed_names.add(name)
            result["component_ids"].pop(f"sources/{name}", None)
            continue
        retained.append(node)
        by_name[name] = registered.target
    if set(owned) - pending_sources != seen_owned - pending_sources or (
        {source.proposal_id for source in connector.source_proposals} - pending_proposals
        != seen_proposals - pending_proposals
    ):
        raise ProvisioningReview("owned_source_identity_missing")
    inputs = stream.get("inputNodes")
    if (
        not isinstance(inputs, list)
        or any(
            not isinstance(item, dict) or set(item) != {"name"} or item["name"] not in source_nodes
            for item in inputs
        )
        or {item["name"] for item in inputs} != set(source_nodes)
    ):
        raise ProvisioningReview("stream_routing_requires_review")
    if deferred:
        LOG.warning(
            "connector_topology_deferred connector_id=%s unverified_targets=%d",
            connector.connector_id, len(deferred),
        )
        original_targets = {}
        for name in source_nodes:
            source_id = baseline["component_ids"].get(f"sources/{name}")
            original_targets[name] = owned[source_id].target if source_id is not None else proposals[name].target
        return DefinitionPlan(
            copy.deepcopy(baseline), original_targets,
            frozenset(), connector.source_proposals,
            tuple(removal.intent() for removal in connector.source_removals),
            tuple(deferred.values()),
        )
    stream["inputNodes"] = [item for item in inputs if item["name"] not in removed_names]
    existing_targets = {
        source.target.key for source in (*connector.sources, *connector.source_proposals)
    }
    new_names = set()
    reserved_names = (
        set(source_nodes) | set(streams) | set(destinations)
        | {removal.node_name for removal in connector.source_removals}
    )
    for key, target in sorted(desired_targets.items()):
        if key in existing_targets:
            continue
        name = "monitor-job-" + uuid5(UUID(connector.ownership_id), key).hex
        if name in reserved_names:
            raise ProvisioningReview("new_source_name_collision")
        new_names.add(name)
        retained_proposals.append(ConnectorSourceProposal(
            proposal_id=str(uuid5(UUID(connector.ownership_id), "source-proposal:" + _digest({
                "target": target.model_dump(mode="json"),
                "event_types": list(SOURCE_EVENTS),
                "event_source": connector.tenant_id,
                "publication_base_revision": connector.revision,
            }))),
            node_name=name,
            source_id=None,
            target=target,
            event_types=SOURCE_EVENTS,
            event_source=connector.tenant_id,
        ))
        retained.append(
            {
                "name": name,
                "type": "FabricJobEvents",
                "properties": {
                    "eventScope": "Item",
                    "workspaceId": target.workspace_id,
                    "itemId": target.item_id,
                    "includedEventTypes": list(SOURCE_EVENTS),
                },
            }
        )
        stream["inputNodes"].append({"name": name})
        by_name[name] = target
    graph["sources"] = retained
    return DefinitionPlan(
        _document(result), by_name, frozenset(new_names), tuple(retained_proposals),
        tuple(removals),
    )


def matches_update(observed: dict, desired: dict) -> bool:
    """Allow omitted ID fields only when their actual topology binding agrees."""
    observed, desired = _document(observed), _document(desired)
    old_ids = desired["component_ids"]
    if any(observed["component_ids"].get(key) != value for key, value in old_ids.items()):
        return False
    left = copy.deepcopy(observed["parts"])
    right = copy.deepcopy(desired["parts"])
    expected_sources = _nodes(right["eventstream.json"], "sources")
    actual_sources = _nodes(left["eventstream.json"], "sources")
    if set(expected_sources) != set(actual_sources):
        return False
    for name, node in actual_sources.items():
        physical = observed["component_ids"].get(f"sources/{name}")
        if "id" in node and node["id"] != physical:
            return False
        if "id" not in expected_sources[name]:
            node.pop("id", None)
    return _normal(left) == _normal(right)


def binding_observation(observed: dict, desired: dict) -> dict:
    """Keep proved physical IDs outside the exact approved logical definition.

    Fabric can add source ID fields to definition parts. The v2 binding receipt
    compares parts exactly, so normalize only after the complete round-trip
    comparison; the actual IDs remain in component_ids.
    """
    if not matches_update(observed, desired):
        raise ProvisioningReview("updated_definition_or_component_id_mismatch")
    _complete_component_map(observed)
    return _document({
        "parts": copy.deepcopy(desired["parts"]),
        "component_ids": copy.deepcopy(observed["component_ids"]),
    })


def _complete_component_map(snapshot: dict) -> None:
    value = _document(snapshot)
    graph = value["parts"]["eventstream.json"]
    keys = {
        f"{kind}/{name}"
        for kind in ("sources", "streams", "destinations")
        for name in _nodes(graph, kind)
    }
    bindings = value["component_ids"]
    if set(bindings) != keys or len(set(bindings.values())) != len(bindings):
        raise ProvisioningReview("complete_owned_component_map_required")
    for kind in ("sources", "streams", "destinations"):
        for name, node in _nodes(graph, kind).items():
            identifier = bindings[f"{kind}/{name}"]
            if _identifier(identifier) != identifier or node.get("id", identifier) != identifier:
                raise ProvisioningReview("complete_owned_component_map_required")


def _remote_absent(snapshot: dict, removal: PendingSourceRemoval) -> bool:
    """Compare exact node, source ID, observed ID and every stream reference."""
    value = _document(snapshot)
    identifiers = {
        identifier for identifier in (removal.source_id, removal.last_observed_source_id)
        if identifier is not None
    }
    bindings = value["component_ids"]
    graph = value["parts"]["eventstream.json"]
    return (
        f"sources/{removal.node_name}" not in bindings
        and not identifiers.intersection(bindings.values())
        and all(
            node["name"] != removal.node_name and node.get("id") not in identifiers
            for node in _nodes(graph, "sources").values()
        )
        and all(
            entry.get("name") != removal.node_name
            for stream in _nodes(graph, "streams").values()
            for entry in stream.get("inputNodes", ())
        )
    )


def publication_sources(
    connector: OwnedConnectorManifest, plan: DefinitionPlan,
) -> tuple[ConnectorSource, ...]:
    """Retain ownership while typed removal intents change only desired intake."""
    existing = {source.target.key: source for source in connector.sources}
    desired = {target.key for target in plan.target_by_name.values()}
    removed_sources = {removal.source_id for removal in plan.source_removals}
    removed_proposals = {removal.proposal_id for removal in plan.source_removals}
    expected = {
        source.target.key for source in connector.sources if source.source_id not in removed_sources
    } | {
        proposal.target.key for proposal in plan.source_proposals
        if proposal.proposal_id not in removed_proposals
    }
    if desired != expected:
        raise ProvisioningReview("published_proposal_target_mismatch")
    retained = {proposal.proposal_id: proposal for proposal in plan.source_proposals}
    if any(
        retained.get(proposal.proposal_id) != proposal for proposal in connector.source_proposals
    ):
        raise ProvisioningReview("published_proposal_ownership_missing")
    return tuple(existing[source.target.key] for source in connector.sources)


def _owned_connectors(
    store: MonitoringReader, context: MonitoringContext,
) -> tuple[OwnedConnectorManifest, ...]:
    result = []
    cursor = None
    seen = set()
    version = None
    for _ in range(100):
        page = store.list_connectors(PageQuery(
            tenant_id=context.tenant_id, epoch=context.epoch, limit=100, cursor=cursor,
        ))
        if version is not None and page.version != version:
            raise MonitoringConflict("Connector listing changed revision")
        version = page.version
        result.extend(page.items)
        if page.next_cursor is None:
            return tuple(result)
        if page.next_cursor in seen:
            raise ProvisioningReview("connector_pagination_incomplete")
        seen.add(page.next_cursor)
        cursor = page.next_cursor
    raise ProvisioningReview("connector_pagination_budget_exhausted")


class OwnedEventCapabilityProbe:
    """Read the registered source through the collector identity, without receiving."""

    def __init__(
        self, store: WorkerMonitoringStore, context: MonitoringContext,
        rest: ProvisioningRestClient, binding: ConnectorBinding, *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if rest.context != context or binding.tenant_id != context.tenant_id:
            raise ValueError("Event capability must use the collector's pinned context")
        self.store, self.context, self.rest, self.binding = store, context, rest, binding
        self.clock = clock

    async def verify(
        self, observation: CapabilityObservation, renew: Callable[[], Awaitable[None]],
    ) -> CapabilityObservation:
        if (
            observation.target.tenant_id != self.context.tenant_id
            or observation.target.epoch != self.context.epoch
            or observation.collector_identity_id != self.rest.collector_identity_id
        ):
            raise MonitoringConflict("Event capability belongs to another collector or deployment")
        if observation.target.workload != "fabric_pipeline" or observation.read_status != "verified":
            return observation
        owned_source = False
        try:
            before = await asyncio.to_thread(self.store.snapshot, self.context)
            connectors = await asyncio.to_thread(_owned_connectors, self.store, self.context)
            matches = [value for value in connectors if value.connector_id == self.binding.connector_id]
            if len(matches) != 1:
                raise ProvisioningReview("registered_event_connector_required")
            connector = matches[0]
            self.binding.check_resource(connector, before.control)
            if (
                connector.state in {"blocked", "deleting", "deleted"}
                or connector.source_proposals or connector.source_removals
                or connector.observed_definition is None
            ):
                raise ProvisioningReview("owned_event_baseline_unverified")
            sources = [source for source in connector.sources if source.target == observation.target]
            if len(sources) != 1:
                raise ProvisioningReview("owned_event_source_unverified")
            source = sources[0]
            owned_source = True
            if source.event_source != self.context.tenant_id:
                raise ProvisioningReview("owned_event_source_tenant_mismatch")
            validate_connector_definition(connector.sources, connector.desired_definition)
            _complete_component_map(connector.desired_definition)
            if not matches_update(connector.observed_definition, connector.desired_definition):
                raise ProvisioningReview("owned_event_baseline_unverified")
            await renew()
            observed, topology = await self.rest.inspect(connector, renew)
            observed = binding_observation(observed, connector.desired_definition)
            graph = observed["parts"]["eventstream.json"]
            nodes = [
                node for name, node in _nodes(graph, "sources").items()
                if observed["component_ids"][f"sources/{name}"] == source.source_id
            ]
            if len(nodes) != 1:
                raise ProvisioningReview("owned_event_source_id_mismatch")
            properties = nodes[0].get("properties", {})
            destination = next(iter(_nodes(graph, "destinations").values()))
            if (
                properties.get("workspaceId") != observation.target.workspace_id
                or properties.get("itemId") != observation.target.item_id
                or set(properties.get("includedEventTypes", ())) != set(source.event_types)
                or observed["component_ids"][f"destinations/{destination['name']}"]
                != self.binding.destination_id
                or any(
                    node.get("status") != "Running"
                    for kind in ("sources", "streams", "destinations")
                    for node in _nodes(topology, kind).values()
                )
            ):
                raise ProvisioningReview("owned_event_path_not_verified_running")
            after = await asyncio.to_thread(self.store.snapshot, self.context)
            current = await asyncio.to_thread(_owned_connectors, self.store, self.context)
            if after.control != before.control or not any(value == connector for value in current):
                raise MonitoringConflict("Owned event capability changed during its read probe")
            return CapabilityObservation.model_validate({
                **observation.model_dump(),
                "event_status": "verified",
                "event_evidence": EventCapabilityEvidence(
                    connector_id=connector.connector_id, ownership_id=connector.ownership_id,
                    source_id=source.source_id, eventstream_id=self.binding.eventstream_id,
                    destination_id=self.binding.destination_id,
                    definition_hash=_digest(observed),
                    endpoint_hash=_digest(self.binding.endpoint.model_dump(mode="json")),
                    event_types=source.event_types, observed_at=self.clock(),
                ),
            })
        except (ProvisioningReview, EventContractError, RestReadError, ValueError) as exc:
            code = exc.code if isinstance(exc, (ProvisioningReview, RestReadError)) else "owned_event_binding_unverified"
            LOG.warning("event_capability_unverified code=%s", code)
            return CapabilityObservation.model_validate({
                **observation.model_dump(), "event_status": "unknown", "event_evidence": None,
                "gaps": (*observation.gaps[:199], CoverageGap(
                    code=code, detail="The current registered source and running event path could not be verified",
                    retry_at=(
                        exc.retry_at if isinstance(exc, RestReadError) and exc.retry_at is not None
                        else self.clock() + timedelta(seconds=60) if owned_source else None
                    ),
                )),
            })


class PublicationState(NamedTuple):
    """One resolution of everything a connector publication is authorized against.

    Resolving this twice, and then taking a third snapshot only to read a clock,
    is what let presence evidence expire between the check that admitted a
    handoff and the check that authorized it. Each resolution computes
    estate-wide coverage.
    """

    version: RegistryVersion
    work: MonitoringWork
    frontier: ValidationFrontier
    connector: OwnedConnectorManifest
    connectors: tuple[OwnedConnectorManifest, ...]


def _publication_state(
    store: ControllerMonitoringStore,
    work: MonitoringWork,
    connector_id: str,
) -> PublicationState:
    if store.component != "controller":
        raise MonitoringComponentDenied("Only the controller may plan desired connector publication")
    context = MonitoringContext(tenant_id=work.tenant_id, epoch=work.epoch)
    current_work = store.get_work(context, work.work_id)
    if (
        work.kind != "reconcile_state" or current_work is None
        or current_work.kind != "reconcile_state" or current_work.state != "leased"
        or work.lease is None or current_work.lease is None
        or current_work.lease.owner_id != work.lease.owner_id
        or current_work.lease.fence != work.lease.fence
        or current_work.reconcile_request_id is None or current_work.reconcile_producer is None
    ):
        raise MonitoringLeaseLost("Controller publication requires the current reconciliation lease")
    snapshot = store.snapshot(context)
    control = snapshot.control
    if control.maintenance:
        raise MonitoringConflict("Maintenance stops connector publication")
    version = RegistryVersion(**context.model_dump(), revision=control.revision)
    producer = store.get_reconciliation_request(
        context, current_work.reconcile_request_id, producer=current_work.reconcile_producer,
    )
    if producer is None or producer.work_id != current_work.work_id:
        raise MonitoringConflict("Controller publication lost its original reconciliation producer")
    frontier = store.get_validation_frontier(context, producer.frontier_key)
    if frontier is None:
        raise MonitoringConflict("Controller publication requires the protected validation frontier")
    connectors = _owned_connectors(store, context)
    matches = [value for value in connectors if value.connector_id == canonical_id(connector_id)]
    if len(matches) != 1:
        raise ProvisioningReview("owned_connector_not_unique")
    return PublicationState(version, current_work, frontier, matches[0], connectors)


def prepare_connector_publication(
    store: ControllerMonitoringStore,
    work: MonitoringWork,
    connector_id: str,
    *,
    request_id: str,
    max_sources: int = 100,
    eligible_targets: tuple[MonitoringTarget, ...] | None = None,
    removal_targets: tuple[TargetIdentity, ...] = (),
) -> ConnectorPublicationRequest:
    """Retain unavailable sources; only affirmative removal evidence contracts topology."""
    if not 1 <= max_sources <= 1000:
        raise ValueError("Connector publication source bound is invalid")
    version, current_work, frontier, connector, connectors = _publication_state(
        store, work, connector_id,
    )
    context = MonitoringContext(tenant_id=version.tenant_id, epoch=version.epoch)
    excluded = {
        source.target.key for other in connectors
        if other.connector_id != connector.connector_id and other.state != "deleted"
        for source in (*other.sources, *other.source_proposals)
    }
    targets = []
    removals = {target.key: target for target in removal_targets}
    eligible_keys = {target.key for target in eligible_targets} if eligible_targets is not None else None
    cursor = None
    seen = set()
    for _ in range(100):
        page = store.list_targets(TargetQuery(
            **context.model_dump(), workload="fabric_pipeline", include_inactive=True, limit=100, cursor=cursor,
        ))
        if page.version != version:
            raise MonitoringConflict("Admission changed during controller source planning")
        for candidate in page.items:
            current = store.resolve_target(candidate.identity)
            if (
                current is not None and current.identity.workload == "fabric_pipeline"
                and current.state == "current" and current.observation.enabled
                and current.policy_revision == version.revision and current.key not in excluded
                and (eligible_keys is None or current.key in eligible_keys)
            ):
                targets.append(current)
        if page.next_cursor is None:
            break
        if page.next_cursor in seen:
            raise ProvisioningReview("target_pagination_incomplete")
        seen.add(page.next_cursor)
        cursor = page.next_cursor
    else:
        raise ProvisioningReview("target_pagination_budget_exhausted")
    if len(targets) > max_sources:
        raise ProvisioningReview("approved_shard_metadata_required")
    # Raw observed topology is not authority for the next desired definition.
    plan = plan_definition(
        connector, connector.desired_definition, tuple(targets), removal_targets=tuple(removals.values()),
    )
    if plan.deferred_targets and connector.policy_revision != version.revision:
        raise ProvisioningReview("source_removal_authority_unverified_for_current_policy")
    return publication_from_plan(
        version, current_work, frontier, connector, plan, request_id=request_id,
    )


def publication_from_plan(
    version: RegistryVersion,
    work: MonitoringWork,
    frontier: ValidationFrontier,
    connector: OwnedConnectorManifest,
    plan: DefinitionPlan,
    *,
    request_id: str,
) -> ConnectorPublicationRequest:
    """Pure DTO construction shared by controller callers and atomic reconciliation."""
    return ConnectorPublicationRequest(
        request_id=request_id, expected=version, work_id=work.work_id,
        lease=work.lease, expected_work_revision=work.revision,
        expected_frontier_revision=frontier.accepted_revision,
        connector_id=connector.connector_id, ownership_id=connector.ownership_id,
        expected_connector_revision=connector.revision, name=connector.name,
        sources=publication_sources(connector, plan), source_proposals=plan.source_proposals,
        source_removals=plan.source_removals,
        desired_definition=plan.desired,
        detail=(
            "Retain existing desired topology; unavailable source admission does not authorize removal"
            if plan.deferred_targets else
            "Publish sources derived from current admitted targets under the controller work fence"
        ),
    )


def _materialized_sources(
    request: ConnectorPublicationRequest, result: ConnectorPublicationResult,
) -> tuple[ConnectorSource, ...]:
    """Validate the guarded publication result, not a latest worker observation.

    SQL validates observation_receipt_id under ownership chaining. The controller
    cannot read worker-private receipt views and must not bypass that boundary.
    """
    observed = _document(result.connector.desired_definition)
    desired = _document(request.desired_definition)
    if observed["parts"] != desired["parts"]:
        raise MonitoringUnavailable("Physical publication changed the approved logical definition")
    if any(
        observed["component_ids"].get(key) != value
        for key, value in desired["component_ids"].items()
    ):
        raise MonitoringUnavailable("Physical publication replaced an established component")
    _complete_component_map(observed)
    removals = {removal.removal_id: removal for removal in request.source_removals}
    retired = {retirement.removal_id: retirement for retirement in result.retired_sources}
    if set(retired) != set(removals):
        raise MonitoringUnavailable("Physical publication omitted or invented a requested retirement")
    receipt_bindings = set()
    for identifier, retirement in retired.items():
        intent = removals[identifier]
        original = next((
            source for source in request.sources if source.source_id == intent.source_id
        ), None) if intent.source_id is not None else next((
            proposal for proposal in request.source_proposals if proposal.proposal_id == intent.proposal_id
        ), None)
        if (
            retirement.original_binding != original
            or retirement.original_removal.intent() != intent
            or retirement.confirmation_request_id != request.request_id
            or retirement.work_id != request.work_id or retirement.work_fence != request.lease.fence
            or retirement.policy_revision != request.expected.revision
            or retirement.observation_receipt_id != request.observation_receipt_id
            or not _remote_absent(observed, retirement.original_removal)
        ):
            raise MonitoringUnavailable("Retirement does not preserve its exact receipt-bound ownership")
        receipt_bindings.add((
            retirement.observation_fingerprint, retirement.observation_binding_hash,
            retirement.observation_receipt_hash, retirement.observed_definition_hash,
        ))
    if len(receipt_bindings) > 1:
        raise MonitoringUnavailable("One physical confirmation returned conflicting observation evidence")
    ids = observed["component_ids"]
    removed_ids = {removal.source_id for removal in request.source_removals}
    removed_proposals = {removal.proposal_id for removal in request.source_removals}
    sources = [source for source in request.sources if source.source_id not in removed_ids]
    seen = {source.source_id for source in sources}
    for proposal in request.source_proposals:
        if proposal.proposal_id in removed_proposals:
            continue
        physical = ids.get(f"sources/{proposal.node_name}")
        if not isinstance(physical, str) or _identifier(physical) != physical or physical in seen:
            raise MonitoringUnavailable("Physical publication is incomplete or reuses a source ID")
        seen.add(physical)
        sources.append(ConnectorSource(
            source_id=physical, target=proposal.target, event_types=proposal.event_types,
            event_source=proposal.event_source,
        ))
    return tuple(sources)


def prepare_connector_binding(
    store: ControllerMonitoringStore, work: MonitoringWork, connector_id: str, *, request_id: str,
) -> ConnectorPublicationRequest:
    """Confirm additions/removals through the original observation, not readiness."""
    state = _publication_state(store, work, connector_id)
    version, current, frontier, connector = state.version, state.work, state.frontier, state.connector
    if current.reconcile_producer != "worker" or not (
        connector.source_proposals or connector.source_removals
    ):
        raise ProvisioningReview("physical_binding_requires_worker_changes")
    original = store.get_connector_observation(version, current.reconcile_request_id)
    if original is not None and original.inspection is not None:
        # Hand the resolved state down rather than letting supersession resolve
        # it again. Each resolution is an estate-wide snapshot, and the presence
        # inspection it is checked against is only valid for 300 seconds.
        return prepare_connector_supersession(
            store, current, connector_id, request_id=request_id,
            original_observation=original.observation, state=state,
        )
    request = ConnectorPublicationRequest(
        request_id=request_id, expected=version, work_id=current.work_id,
        lease=current.lease, expected_work_revision=current.revision,
        expected_frontier_revision=frontier.accepted_revision,
        connector_id=connector.connector_id, ownership_id=connector.ownership_id,
        expected_connector_revision=connector.revision, name=connector.name,
        sources=connector.sources, source_proposals=connector.source_proposals,
        source_removals=tuple(removal.intent() for removal in connector.source_removals),
        desired_definition=connector.desired_definition,
        observation_receipt_id=current.reconcile_request_id,
        detail="Confirm actual component bindings and remote absence from the exact owned worker observation",
    )
    return request


def _project_source_nodes(snapshot: dict, names: set[str]) -> dict:
    projected = copy.deepcopy(_document(snapshot))
    graph = projected["parts"]["eventstream.json"]
    graph["sources"] = [node for node in graph["sources"] if node["name"] in names]
    for stream in _nodes(graph, "streams").values():
        stream["inputNodes"] = [entry for entry in stream["inputNodes"] if entry["name"] in names]
    projected["component_ids"] = {
        key: value for key, value in projected["component_ids"].items()
        if not key.startswith("sources/") or key.removeprefix("sources/") in names
    }
    _complete_component_map(projected)
    return projected


def _restored_source_definition(
    connector: OwnedConnectorManifest, observed: dict, superseded: tuple[PendingSourceRemoval, ...],
) -> dict:
    """Restore only exact retained physical nodes from complete original evidence."""
    if not superseded or connector.source_proposals:
        raise ProvisioningReview("physical_source_supersession_required")
    observed, desired = _document(observed), _document(connector.desired_definition)
    _complete_component_map(observed)
    desired_nodes = _nodes(desired["parts"]["eventstream.json"], "sources")
    observed_nodes = _nodes(observed["parts"]["eventstream.json"], "sources")
    owned = {source.source_id: source for source in connector.sources}
    pending = {removal.node_name: removal for removal in connector.source_removals}
    seen_sources = []
    for name in observed_nodes:
        source_id = observed["component_ids"][f"sources/{name}"]
        source = owned.get(source_id)
        if source is None:
            raise ProvisioningReview("unowned_presence_source_requires_review")
        if name not in desired_nodes:
            removal = pending.get(name)
            if removal is None or removal.source_id != source_id or removal.target != source.target:
                raise ProvisioningReview("retained_presence_source_binding_mismatch")
        seen_sources.append(source)
    try:
        validate_connector_definition(tuple(seen_sources), observed)
    except ValueError:
        raise ProvisioningReview("retained_presence_definition_unverified") from None
    for name, node in observed_nodes.items():
        source = owned[observed["component_ids"][f"sources/{name}"]]
        properties = node["properties"]
        if (
            properties["workspaceId"] != source.target.workspace_id
            or properties["itemId"] != source.target.item_id
            or set(properties["includedEventTypes"]) != set(source.event_types)
        ):
            raise ProvisioningReview("retained_presence_source_target_mismatch")
    projected = _project_source_nodes(observed, set(desired_nodes))
    if not matches_update(projected, desired):
        raise ProvisioningReview("retained_presence_changes_approved_topology")
    restored_names = set(desired_nodes)
    for removal in superseded:
        if (
            removal.source_id is None or removal not in connector.source_removals
            or removal.node_name not in observed_nodes
            or observed["component_ids"].get(f"sources/{removal.node_name}") != removal.source_id
        ):
            raise ProvisioningReview("superseded_source_presence_unverified")
        restored_names.add(removal.node_name)
    return _project_source_nodes(observed, restored_names)


def prepare_connector_supersession(
    store: ControllerMonitoringStore, work: MonitoringWork, connector_id: str, *,
    request_id: str, original_observation: OwnedConnectorManifest | None = None,
    state: PublicationState | None = None,
) -> ConnectorPublicationRequest:
    """Propose reinstatement from original worker evidence, never current-state equality."""
    if state is None:
        state = _publication_state(store, work, connector_id)
    version, current, frontier, connector = state.version, state.work, state.frontier, state.connector
    if current.reconcile_producer != "worker" or not connector.source_removals:
        raise ProvisioningReview("supersession_requires_original_worker_observation")
    original = store.get_connector_observation(version, current.reconcile_request_id)
    if original is None:
        raise MonitoringUnavailable("Original retained-source presence observation is unavailable")
    if original_observation is not None and original_observation != original.observation:
        raise MonitoringUnavailable("Supersession input differs from the original observation projection")
    original_observation = original.observation
    inspection = original.inspection
    if (
        inspection is None or original.connector_id != connector_id
        or original.reconcile_work_id != current.work_id or not original.collection_completion_eligible
        or original.observed_definition_hash != inspection.definition_hash
    ):
        raise ProvisioningReview("supersession_requires_explicit_original_inspection")
    # Read the clock here, not from the snapshot above. The kernel re-checks the
    # same 300-second window against its own SYSUTCDATETIME immediately after
    # this, and a kernel refusal aborts the transaction, so it cannot be turned
    # into a durable rejection. Checking against an older coverage timestamp
    # made this pre-check the weaker of the two and let expiry surface there
    # instead, where the only outcome is a rollback and a retry.
    now = store.now()
    if not now - timedelta(seconds=SUPERSESSION_EVIDENCE_TTL_SECONDS) <= inspection.observed_at <= now:
        raise ProvisioningReview("supersession_presence_inspection_expired")
    if (
        original_observation.state != "degraded" or original_observation.observed_definition is None
        or original_observation.revision != connector.revision
        or original_observation.operation_id is not None
        or any(getattr(original_observation, field) is not None for field in (
            "identity_verified_at", "delivery_verified_at", "delivery_proof",
        ))
        or any(getattr(original_observation, field) != getattr(connector, field) for field in (
            "tenant_id", "epoch", "connector_id", "ownership_id", "policy_revision",
            "sources", "source_proposals", "source_removals", "desired_definition",
            "workspace_id", "eventstream_id", "destination_id", "endpoint",
        ))
        or any(gap.code in {INTENT_GAP, "definition_update_outcome_unknown"} for gap in original_observation.gaps)
    ):
        raise ProvisioningReview("supersession_original_observation_mismatch")
    superseded = []
    for removal in connector.source_removals:
        if removal.source_id is None:
            continue
        target = store.resolve_target(removal.target)
        if (
            target is not None and target.state == "current" and target.observation.enabled
            and target.policy_revision == version.revision
        ):
            if inspection.observed_at <= removal.requested_at:
                raise ProvisioningReview("supersession_observation_predates_removal")
            superseded.append(removal)
    restored = _restored_source_definition(connector, original_observation.observed_definition, tuple(superseded))
    superseded_ids = {removal.removal_id for removal in superseded}
    return ConnectorPublicationRequest.model_validate({
        "request_id": request_id, "expected": version, "work_id": current.work_id,
        "lease": current.lease, "expected_work_revision": current.revision,
        "expected_frontier_revision": frontier.accepted_revision,
        "connector_id": connector.connector_id, "ownership_id": connector.ownership_id,
        "expected_connector_revision": connector.revision, "name": connector.name,
        "sources": connector.sources, "source_proposals": connector.source_proposals,
        "source_removals": tuple(
            removal.intent() for removal in connector.source_removals if removal.removal_id not in superseded_ids
        ),
        "source_removal_supersessions": tuple({
            "removal_id": removal.removal_id, "source_id": removal.source_id,
        } for removal in superseded),
        "desired_definition": restored,
        "observation_receipt_id": current.reconcile_request_id,
        "readiness_receipt_id": None,
        "detail": "Supersede only receipt-proven retained physical sources under current read admission; readiness remains unverified",
    })


def publish_connector_intent(
    store: ControllerMonitoringStore, request: ConnectorPublicationRequest,
) -> ConnectorPublicationResult:
    """Synchronous controller adapter; never installed in the worker maintenance loop."""
    if store.component != "controller":
        raise MonitoringComponentDenied("Only the controller may publish desired connector intent")

    def original_publication() -> ConnectorPublicationResult | None:
        original = store.get_connector_publication(request.expected, request.request_id)
        if original is None:
            return None
        receipt = store.get_operation_receipt(
            request.expected, "connector_publication", request.request_id,
        )
        if (
            receipt is None or receipt.operation != "connector_publication"
            or receipt.request_id != request.request_id
            or receipt.tenant_id != request.expected.tenant_id or receipt.epoch != request.expected.epoch
            or receipt.fingerprint != _digest(request.model_dump(mode="json"))
        ):
            raise MonitoringUnavailable("Original connector publication receipt does not match")
        return original

    result = original_publication()
    prior = None
    if result is None:
        prior = next((
            value for value in _owned_connectors(store, request.expected)
            if value.connector_id == request.connector_id
        ), None)
        supersessions = {value.removal_id: value.source_id for value in request.source_removal_supersessions}
        if supersessions and (
            prior is None or any(
                not any(removal.removal_id == identifier and removal.source_id == source_id for removal in prior.source_removals)
                for identifier, source_id in supersessions.items()
            )
            or request.source_proposals != prior.source_proposals
            or {removal.removal_id: removal for removal in request.source_removals}
            != {removal.removal_id: removal.intent() for removal in prior.source_removals if removal.removal_id not in supersessions}
        ):
            raise ProvisioningReview("supersession_must_select_exact_pending_physical_sources")
        if prior is not None and (
            prior.sources != request.sources
            or any(
                proposal not in request.source_proposals for proposal in prior.source_proposals
            )
            or any(
                removal.intent() not in request.source_removals and removal.removal_id not in supersessions
                for removal in prior.source_removals
            )
        ):
            raise ProvisioningReview("source_ownership_must_be_retained_until_confirmation")
        try:
            result = store.publish_connector(request)
        except MonitoringCommitUncertain as exc:
            if (
                exc.operation not in {"connector_publication", "controller.publish_connector"}
                or exc.idempotency_id != request.request_id
            ):
                raise
            result = original_publication()
            if result is None:
                raise
    if (
        not isinstance(result, ConnectorPublicationResult)
        or result.connector_id != request.connector_id
        or result.connector.tenant_id != request.expected.tenant_id
        or result.connector.epoch != request.expected.epoch
        or result.connector.ownership_id != request.ownership_id
        or result.connector.revision != request.expected_connector_revision + 1
        or result.observation_receipt_id != request.observation_receipt_id
        or not request.source_removal_supersessions and result.superseded_source_removals
    ):
        raise MonitoringUnavailable("Original connector publication returned a different intent")
    if request.source_removal_supersessions:
        selected = {value.removal_id: value.source_id for value in request.source_removal_supersessions}
        originals = {removal.removal_id: removal for removal in result.superseded_source_removals}
        if (
            {identifier: removal.source_id for identifier, removal in originals.items()} != selected
            or not result.desired_changed or result.state == "ready" or result.retired_sources
            or result.connector.sources != request.sources
            or result.connector.source_proposals != request.source_proposals
            or result.connector.desired_definition != request.desired_definition
            or {removal.removal_id: removal.intent() for removal in result.pending_removals}
            != {removal.removal_id: removal for removal in request.source_removals}
            or any(value is not None for value in (
                result.connector.identity_verified_at, result.connector.delivery_verified_at,
                result.connector.delivery_proof,
            ))
        ):
            raise MonitoringUnavailable("Supersession did not return exact original removals and unready retained ownership")
        sources = {source.source_id: source for source in request.sources}
        for removal in originals.values():
            source = sources.get(removal.source_id)
            if (
                source is None or removal.target != source.target or removal.proposal_id is not None
                or result.connector.desired_definition["component_ids"].get(f"sources/{removal.node_name}") != removal.source_id
                or prior is not None and removal not in prior.source_removals
            ):
                raise MonitoringUnavailable("Supersession changed an original pending removal or its retained physical binding")
        if prior is not None and any(
            getattr(prior, field) != getattr(result.connector, field)
            for field in ("workspace_id", "eventstream_id", "destination_id", "endpoint")
        ):
            raise MonitoringUnavailable("Supersession changed an established transport resource")
    elif request.observation_receipt_id is None:
        if (
            result.connector.sources != request.sources
            or result.connector.source_proposals != request.source_proposals
            or result.connector.desired_definition != request.desired_definition
            or result.retired_sources
            or {removal.removal_id: removal.intent() for removal in result.pending_removals}
            != {removal.removal_id: removal for removal in request.source_removals}
        ):
            raise MonitoringUnavailable("Connector publication changed the requested source intent")
        for pending in result.pending_removals:
            binding = next((
                source for source in request.sources if source.source_id == pending.source_id
            ), None) if pending.source_id is not None else next((
                proposal for proposal in request.source_proposals if proposal.proposal_id == pending.proposal_id
            ), None)
            if binding is None or pending.target != binding.target:
                raise MonitoringUnavailable("Pending removal identifies a different owned target")
    else:
        expected_sources = _materialized_sources(request, result)
        if (
            {source.source_id: source for source in result.connector.sources}
            != {source.source_id: source for source in expected_sources}
            or result.connector.source_proposals
            or result.pending_removals or result.connector.source_removals
            or result.state == "ready"
            or result.connector.identity_verified_at is not None
            or result.connector.delivery_verified_at is not None
        ):
            raise MonitoringUnavailable("Connector publication did not return the exact observed bindings")
    if result.desired_changed:
        identifier = stable_id(
            request.expected, f"connector:{request.connector_id}:publication:{result.connector.revision}",
        )
        existing = store.get_work(request.expected, identifier)
        if existing is None:
            store.enqueue_work(
                MonitoringWorkDraft(
                    tenant_id=request.expected.tenant_id,
                    epoch=request.expected.epoch,
                    work_id=identifier,
                    kind="connector_reconcile",
                    connector_id=request.connector_id,
                    policy_revision=result.connector.policy_revision,
                    created_at=result.connector.updated_at,
                    due_at=result.connector.updated_at,
                    reason="Apply the exact controller-published owned Eventstream intent",
                ),
            )
        elif existing.kind != "connector_reconcile" or existing.connector_id != request.connector_id:
            raise MonitoringConflict("Publication follow-up identifies different work")
    return result


class SourceProbe(Protocol):
    async def probe(
        self,
        target: TargetIdentity,
        *,
        inventory_generation: str,
        checked_at: datetime,
        ttl_seconds: int = 3600,
    ) -> CapabilityObservation: ...


@dataclass(frozen=True)
class ReconcileResult:
    work_id: str
    state: str
    code: str


@dataclass(frozen=True)
class ReconcileRun:
    claimed: int
    results: tuple[ReconcileResult, ...]


class ConnectorReconciler:
    """Worker observations and Fabric effects for already-published controller intent."""

    def __init__(
        self,
        store: WorkerMonitoringStore,
        context: MonitoringContext,
        rest: ProvisioningRestClient,
        pipeline_probe: SourceProbe,
        owner_id: str,
        connector_id: str,
        *,
        batch_size: int = 2,
        max_sources: int = 100,
        retry_seconds: int = 30,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if rest.context != context:
            raise ValueError("Provisioning and worker contexts must match")
        if (
            not 1 <= max_sources <= 1000
            or not 1 <= retry_seconds <= 300
        ):
            raise ValueError("Provisioning bounds are invalid")
        self.store = store
        self.context = context
        self.rest = rest
        self.pipeline_probe = pipeline_probe
        self.connector_id = canonical_id(connector_id)
        self.claim = WorkClaimRequest(
            **context.model_dump(),
            owner_id=canonical_id(owner_id),
            kinds=("connector_reconcile",),
            limit=batch_size,
            per_workspace_limit=1,
            lease_seconds=120,
        )
        self.max_sources = max_sources
        self.retry_seconds = retry_seconds
        self.clock = clock

    async def _version(self) -> RegistryVersion:
        snapshot = await asyncio.to_thread(self.store.snapshot, self.context)
        if snapshot.control.maintenance:
            raise MonitoringConflict("Maintenance stops connector provisioning")
        return RegistryVersion(**self.context.model_dump(), revision=snapshot.control.revision)

    async def _connectors(self) -> tuple[OwnedConnectorManifest, ...]:
        return await asyncio.to_thread(_owned_connectors, self.store, self.context)

    async def _connector(self, identifier: str) -> OwnedConnectorManifest:
        matches = [value for value in await self._connectors() if value.connector_id == identifier]
        if len(matches) != 1:
            raise ProvisioningReview("owned_connector_not_unique")
        return matches[0]

    async def _fresh_work(self, work: MonitoringWork) -> MonitoringWork:
        current = await asyncio.to_thread(self.store.get_work, self.context, work.work_id)
        if (
            current is None
            or current.lease is None
            or work.lease is None
            or (
                current.lease.owner_id != work.lease.owner_id
                or current.lease.fence != work.lease.fence
            )
        ):
            raise MonitoringLeaseLost("Connector work ownership changed")
        return current

    async def _renew(self, work: MonitoringWork) -> None:
        current = await self._fresh_work(work)
        await asyncio.to_thread(
            self.store.renew_lease,
            LeaseRenewal(lease=current.lease, lease_seconds=120),
        )

    async def _save(
        self,
        work: MonitoringWork,
        prior: OwnedConnectorManifest,
        *,
        expected_policy_revision: int | None = None,
        inspection: ConnectorPresenceInspection | None = None,
        **changes,
    ) -> OwnedConnectorManifest:
        allowed = {
            "observed_definition", "operation_id", "state", "identity_verified_at",
            "delivery_verified_at", "delivery_proof", "gaps",
        }
        if changes.keys() - allowed:
            raise ProvisioningReview("worker_cannot_publish_connector_intent")
        version = await self._version()
        if expected_policy_revision is not None and version.revision != expected_policy_revision:
            raise MonitoringConflict("Connector plan is no longer bound to the current policy")
        # Metadata-only/uncertain reports must not reissue an inherited snapshot
        # as fresh SQL-hashed evidence of remote absence.
        desired = (
            prior.model_dump(mode="python")
            | {"observed_definition": None}
            | changes
            | {
                "revision": prior.revision + 1,
                "policy_revision": version.revision,
                "updated_at": self.clock(),
            }
        )
        value = OwnedConnectorManifest.model_validate(desired)
        current_work = await self._fresh_work(work)
        commit = CollectionCommit(
            work_id=current_work.work_id, lease=current_work.lease, expected_work_revision=current_work.revision,
        )
        try:
            effective = await asyncio.to_thread(
                self.store.record_connector,
                version,
                value,
                expected_connector_revision=prior.revision,
                commit=commit,
                **({"inspection": inspection} if inspection is not None else {}),
            )
        except MonitoringCommitUncertain as exc:
            if exc.operation not in {"connector", "worker.observe_connector"}:
                raise
            receipt = await asyncio.to_thread(
                self.store.get_operation_receipt, self.context, exc.operation, exc.idempotency_id,
            )
            if receipt is None:
                raise
            original_request = {
                "expected": version.model_dump(mode="json"),
                "manifest": value.model_dump(mode="json"),
                "expected_connector_revision": prior.revision,
                "commit": commit.model_dump(mode="json"),
            }
            if inspection is not None:
                original_request["inspection"] = inspection.model_dump(mode="json")
            fingerprint = _digest(original_request)
            if (
                receipt.tenant_id != self.context.tenant_id or receipt.epoch != self.context.epoch
                or receipt.operation != exc.operation or receipt.request_id != exc.idempotency_id
                or receipt.fingerprint != fingerprint
            ):
                raise MonitoringUnavailable("Original connector observation receipt does not match") from None
            try:
                if exc.operation == "worker.observe_connector":
                    original = ConnectorObservationResult.model_validate(receipt.result)
                    if (original.observed_definition_hash is None) != (value.observed_definition is None):
                        raise MonitoringUnavailable("Original observation freshness does not match the request")
                    if (
                        original.work_id != commit.work_id or original.work_owner_id != commit.lease.owner_id
                        or original.work_fence != commit.lease.fence or original.work_revision != commit.expected_work_revision
                        or original.collection_completion_eligible != connector_collection_eligible(value)
                        or original.observation.policy_revision != version.revision
                        or original.observation.tenant_id != version.tenant_id or original.observation.epoch != version.epoch
                        or original.inspection != inspection
                    ):
                        raise MonitoringUnavailable("Original observation receipt belongs to another collection fence")
                    effective = original.connector
                else:
                    effective = OwnedConnectorManifest.model_validate(receipt.result)
            except ValidationError:
                raise MonitoringUnavailable("Original connector observation receipt is invalid") from None
        if (
            not isinstance(effective, OwnedConnectorManifest)
            or effective.connector_id != prior.connector_id
            or effective.tenant_id != prior.tenant_id or effective.epoch != prior.epoch
            or effective.revision != prior.revision + 1
            or effective.state == "ready" and prior.state != "ready"
            or effective.identity_verified_at != prior.identity_verified_at
            or effective.delivery_verified_at != prior.delivery_verified_at
            or effective.delivery_proof != prior.delivery_proof
            or any(
                getattr(effective, name) != getattr(prior, name)
                for name in (
                    "ownership_id", "name", "sources", "source_proposals", "source_removals", "desired_definition",
                    "workspace_id", "eventstream_id", "destination_id", "endpoint",
                )
            )
        ):
            raise MonitoringUnavailable("Connector observation returned a different owned intent")
        return effective

    async def _targets(
        self,
        connector: OwnedConnectorManifest,
        *,
        probe: bool,
        work: MonitoringWork,
    ) -> tuple[MonitoringTarget, ...]:
        desired = _document(connector.desired_definition)
        nodes = _nodes(desired["parts"]["eventstream.json"], "sources")
        if len(nodes) > self.max_sources:
            raise ProvisioningReview("approved_shard_metadata_required")
        published = {
            (source.target.workspace_id, source.target.item_id): source
            for source in (*connector.sources, *connector.source_proposals)
        }
        result = []
        version = await self._version()
        for name, node in nodes.items():
            if node.get("type") != "FabricJobEvents":
                raise ProvisioningReview("unowned_source_requires_review")
            properties = node.get("properties", {})
            source = published.get((
                _identifier(properties.get("workspaceId")),
                _identifier(properties.get("itemId")),
            ))
            if source is None:
                raise ProvisioningReview("published_source_intent_missing")
            if (
                properties.get("eventScope") != "Item"
                or source.target.workload != "fabric_pipeline"
                or properties.get("includedEventTypes") != list(source.event_types)
                or not set(source.event_types).issubset(SOURCE_EVENTS)
            ):
                raise ProvisioningReview("published_source_intent_mismatch")
            physical = desired["component_ids"].get(f"sources/{name}")
            if isinstance(source, ConnectorSourceProposal):
                if source.node_name != name or physical is not None or "id" in node:
                    raise ProvisioningReview("proposal_has_unverified_physical_identity")
            elif physical != source.source_id or node.get("id", physical) != physical:
                raise ProvisioningReview("published_source_binding_mismatch")
            current = await asyncio.to_thread(self.store.resolve_target, source.target)
            if (
                current is None or current.state != "current" or not current.observation.enabled
                or current.policy_revision != version.revision
            ):
                continue
            if probe:
                await self._renew(work)
                proof = await self.pipeline_probe.probe(
                    current.identity,
                    inventory_generation=current.inventory_generation,
                    checked_at=self.clock(),
                )
                if (
                    proof.target != current.identity
                    or proof.collector_identity_id != self.rest.collector_identity_id
                ):
                    raise ProvisioningReview("source_probe_identity_mismatch")
                if proof.read_status == "denied":
                    # Connector work cannot impersonate a fenced capability probe.
                    # Its observation handoff asks the controller to re-evaluate.
                    raise ProvisioningReview("source_read_denied_requires_controller_publication")
                if proof.read_status != "verified":
                    retry_at = next((gap.retry_at for gap in proof.gaps if gap.retry_at), None)
                    raise RestReadError(
                        "source_read_probe_incomplete", "Source access could not be established",
                        retry_at=retry_at,
                    )
            result.append(current)
        return tuple(result)

    async def _removal_hold(self, connector: OwnedConnectorManifest) -> str | None:
        """Re-admission cannot cancel an immutable removal or license another POST."""
        if not connector.source_removals:
            return None
        version = await self._version()
        policies = []
        cursor = None
        seen = set()
        for _ in range(10):
            page = await asyncio.to_thread(
                self.store.list_scopes, PageQuery(**self.context.model_dump(), limit=100, cursor=cursor),
            )
            if page.version != version:
                raise MonitoringConflict("Scope changed while checking pending source removal")
            policies.extend(page.items)
            if page.next_cursor is None:
                break
            if page.next_cursor in seen:
                raise ProvisioningReview("scope_pagination_incomplete")
            seen.add(page.next_cursor)
            cursor = page.next_cursor
        else:
            raise ProvisioningReview("scope_pagination_budget_exhausted")
        for removal in connector.source_removals:
            target = await asyncio.to_thread(self.store.resolve_target, removal.target)
            if target is not None and target.state == "current" and target.observation.enabled:
                return "pending_source_removal_readmitted_requires_supersession"
            recorded = await asyncio.to_thread(
                self.store.resolve_target, removal.target, include_inactive=True,
            )
            if policy_removes_target(removal.target, recorded, policies):
                continue
            if not await self._confirmed_deletion(removal.target, version):
                return "pending_source_removal_authority_unverified"
        return None

    async def _confirmed_deletion(self, target: TargetIdentity, version: RegistryVersion) -> bool:
        cursor = None
        seen = set()
        for _ in range(100):
            page = await asyncio.to_thread(
                self.store.list_inventory, TargetQuery(
                    **self.context.model_dump(), workspace_id=target.workspace_id,
                    workload=target.workload, include_inactive=True, limit=100, cursor=cursor,
                ),
            )
            if page.version != version:
                raise MonitoringConflict("Policy changed while checking source deletion evidence")
            for item in page.items:
                if item.target == target:
                    generation = await asyncio.to_thread(
                        self.store.get_inventory_generation, self.context, item.generation_id,
                    )
                    return inventory_confirms_deletion(target, item, generation)
            if page.next_cursor is None:
                return False
            if page.next_cursor in seen:
                raise ProvisioningReview("removal_inventory_pagination_incomplete")
            seen.add(page.next_cursor)
            cursor = page.next_cursor
        raise ProvisioningReview("removal_inventory_pagination_budget_exhausted")

    async def _observe_retained_presence(
        self, work: MonitoringWork, connector: OwnedConnectorManifest, observed: dict, topology: dict,
        *, observed_at: datetime,
    ) -> ReconcileResult:
        if connector.source_proposals or connector.operation_id is not None:
            return await self._defer(work, "pending_removal_effect_state_requires_review")
        _complete_component_map(observed)
        source_nodes = _nodes(observed["parts"]["eventstream.json"], "sources")
        version = await self._version()
        verified = 0
        for removal in connector.source_removals:
            if removal.source_id is None:
                continue
            current = await asyncio.to_thread(self.store.resolve_target, removal.target)
            if current is None or not current.observation.enabled or current.policy_revision != version.revision:
                continue
            node = source_nodes.get(removal.node_name)
            owned = next((source for source in connector.sources if source.source_id == removal.source_id), None)
            if (
                node is None or owned is None or owned.target != removal.target
                or observed["component_ids"].get(f"sources/{removal.node_name}") != removal.source_id
                or node.get("type") != "FabricJobEvents"
                or node.get("properties", {}).get("eventScope") != "Item"
                or node["properties"].get("workspaceId") != removal.target.workspace_id
                or node["properties"].get("itemId") != removal.target.item_id
                or set(node["properties"].get("includedEventTypes", ())) != set(owned.event_types)
            ):
                raise ProvisioningReview("retained_source_presence_binding_mismatch")
            await self._renew(work)
            read = await self.pipeline_probe.probe(
                current.identity, inventory_generation=current.inventory_generation, checked_at=self.clock(),
            )
            if read.target != current.identity or read.collector_identity_id != self.rest.collector_identity_id:
                raise ProvisioningReview("source_probe_identity_mismatch")
            if read.read_status != "verified":
                return await self._defer(work, "pending_removal_source_read_unverified")
            verified += 1
        if not verified:
            return await self._defer(work, "pending_removal_readmission_unverified")
        if any(
            node.get("status") != "Running"
            for kind in ("sources", "streams", "destinations")
            for node in _nodes(topology, kind).values()
        ):
            return await self._defer(work, "retained_source_topology_not_running")
        saved = await self._save(
            work, connector, expected_policy_revision=version.revision,
            observed_definition=observed, state="degraded",
            identity_verified_at=None, delivery_verified_at=None, delivery_proof=None,
            gaps=(CoverageGap(
                code=PRESENCE_GAP,
                detail="Read-only complete owned-source presence; only the controller may supersede removal",
            ),),
            inspection=ConnectorPresenceInspection(
                read_only=True, observed_at=observed_at,
                definition_hash=connector_definition_hash(observed),
                component_states={
                    _identifier(node["id"]): node["status"]
                    for kind in ("sources", "streams", "destinations")
                    for node in _nodes(topology, kind).values()
                },
            ),
        )
        await self._finish(work, saved, PRESENCE_GAP)
        return ReconcileResult(work.work_id, "completed", PRESENCE_GAP)

    async def _defer(
        self, work: MonitoringWork, code: str, retry_at: datetime | None = None
    ) -> ReconcileResult:
        current = await self._fresh_work(work)
        when = max(retry_at or self.clock(), self.clock() + timedelta(seconds=self.retry_seconds))
        await asyncio.to_thread(
            self.store.disposition_work,
            WorkDispositionRequest(
                **self.context.model_dump(),
                request_id=str(
                    uuid5(UUID(work.work_id), f"retry:{current.revision}:{when.isoformat()}")
                ),
                work_id=work.work_id,
                expected_work_revision=current.revision,
                lease=current.lease,
                disposition="retry",
                retry_at=when,
                detail=code,
            ),
        )
        return ReconcileResult(work.work_id, "waiting", code)

    async def _finish(
        self,
        work: MonitoringWork,
        connector: OwnedConnectorManifest,
        code: str,
    ) -> ReconcileResult:
        # record_connector already staged an immutable controller handoff.
        # Only that controller may publish the next desired scope or enqueue it.
        current = await self._fresh_work(work)
        try:
            await asyncio.to_thread(
                self.store.complete_collection_work,
                self.context,
                work_id=work.work_id,
                lease=current.lease,
                expected_work_revision=current.revision,
            )
        except MonitoringCommitUncertain:
            observed = await asyncio.to_thread(self.store.get_work, self.context, work.work_id)
            if observed is None or observed.state != "completed":
                raise
        return ReconcileResult(work.work_id, "completed", code)

    async def _block(
        self, work: MonitoringWork, connector: OwnedConnectorManifest, code: str
    ) -> ReconcileResult:
        # Blocking intake is separate from forgetting component ownership. Keep
        # known IDs so review/removal cannot accidentally adopt or orphan nodes.
        saved = await self._save(
            work, connector,
            state="blocked",
            gaps=(
                CoverageGap(code=code, detail=code),
                *(gap for gap in connector.gaps if gap.code == INTENT_GAP),
            ),
        )
        await self._finish(work, saved, code)
        return ReconcileResult(work.work_id, "blocked", code)

    def _submission_gap(
        self, connector: OwnedConnectorManifest, work: MonitoringWork,
    ) -> CoverageGap:
        return CoverageGap(
            code=INTENT_GAP,
            detail=json.dumps({
                "work_id": work.work_id,
                "fence": work.lease.fence,
                "observation_receipt_id": stable_id(
                    self.context, f"connector:{connector.connector_id}:{connector.revision}",
                ),
                "desired_sha256": _digest(connector.desired_definition),
            }, sort_keys=True, separators=(",", ":")),
        )

    async def _submitted_definition(self, connector: OwnedConnectorManifest) -> dict:
        markers = [gap for gap in connector.gaps if gap.code == INTENT_GAP]
        if len(markers) != 1:
            raise ProvisioningReview("submission_observation_receipt_required")
        try:
            marker = json.loads(markers[0].detail)
            if set(marker) != {"work_id", "fence", "observation_receipt_id", "desired_sha256"}:
                raise ValueError
            request_id = _identifier(marker["observation_receipt_id"])
            receipt = await asyncio.to_thread(
                self.store.get_operation_receipt, self.context, "connector", request_id,
            )
            if receipt is None:
                raise MonitoringUnavailable("The original submission observation receipt is unavailable")
            original = OwnedConnectorManifest.model_validate(receipt.result)
            if (
                receipt.operation != "connector" or receipt.request_id != request_id
                or original.connector_id != connector.connector_id
                or original.ownership_id != connector.ownership_id
                or original.tenant_id != connector.tenant_id or original.epoch != connector.epoch
                or any(
                    getattr(original, name) != getattr(connector, name)
                    for name in ("workspace_id", "eventstream_id", "destination_id", "endpoint")
                )
                or _digest(original.desired_definition) != marker["desired_sha256"]
                or not any(
                    gap.code == INTENT_GAP and gap.detail == markers[0].detail for gap in original.gaps
                )
            ):
                raise ValueError
            return original.desired_definition
        except (ValueError, TypeError, KeyError):
            raise ProvisioningReview("submission_observation_receipt_mismatch") from None

    async def _verified(
        self,
        work: MonitoringWork,
        connector: OwnedConnectorManifest,
        observed: dict,
        topology: dict,
        *,
        submitted_definition: dict | None = None,
    ) -> ReconcileResult:
        expected = connector.desired_definition if submitted_definition is None else submitted_definition
        current_intent_matches = matches_update(observed, connector.desired_definition)
        if current_intent_matches and any(
            not _remote_absent(observed, removal) for removal in connector.source_removals
        ):
            raise ProvisioningReview("owned_source_remote_absence_unverified")
        observed = binding_observation(observed, expected)
        verified_version = await self._version()
        targets = await self._targets(connector, probe=True, work=work)
        observed_sources = []
        graph = observed["parts"]["eventstream.json"]
        statuses = []
        for name, node in _nodes(graph, "sources").items():
            properties = node.get("properties", {})
            key = (
                _identifier(properties.get("workspaceId")),
                _identifier(properties.get("itemId")),
            )
            # Retain ownership of published components until their remote
            # removal is verified. Current SQL target admission independently
            # refuses excluded/paused sources; ownership is not permission.
            target = TargetIdentity(
                **self.context.model_dump(),
                workload="fabric_pipeline",
                workspace_id=key[0],
                item_id=key[1],
            )
            observed_sources.append(
                ConnectorSource(
                    source_id=observed["component_ids"][f"sources/{name}"],
                    target=target,
                    event_types=tuple(properties["includedEventTypes"]),
                    event_source=self.context.tenant_id,
                )
            )
            statuses.append(_nodes(topology, "sources")[name].get("status"))
        for kind in ("streams", "destinations"):
            statuses.extend(node.get("status") for node in _nodes(topology, kind).values())
        if observed_sources and any(status != "Running" for status in statuses):
            return await self._defer(work, "published_topology_not_running")
        desired_keys = {target.identity.key for target in targets}
        observed_keys = {source.target.key for source in observed_sources}
        changed_scope = desired_keys != observed_keys
        current = {
            (source.source_id, source.target.key, source.event_types)
            for source in connector.sources
        }
        bindings_changed = bool(connector.source_proposals) or current != {
            (source.source_id, source.target.key, source.event_types)
            for source in observed_sources
        }
        intent_changed = not matches_update(observed, connector.desired_definition)
        delivery = await asyncio.to_thread(
            self.store.get_connector_delivery, self.context, connector.connector_id,
            self.rest.collector_identity_id,
        )
        ready = bool(
            observed_sources and not changed_scope and not bindings_changed and not intent_changed
            and not connector.source_removals
            and delivery is not None
        )
        code = (
            "controller_source_retirement_required" if connector.source_removals and not intent_changed
            else "controller_source_binding_required" if bindings_changed
            else "controller_scope_publication_required" if changed_scope or intent_changed
            else "no_admitted_pipeline_sources" if not observed_sources
            else "awaiting_source_delivery_proof"
        )
        saved = await self._save(
            work, connector,
            expected_policy_revision=verified_version.revision,
            observed_definition=observed,
            state="ready" if ready else "degraded",
            identity_verified_at=delivery.identity_verified_at if ready else connector.identity_verified_at,
            delivery_verified_at=delivery.received_at if ready else connector.delivery_verified_at,
            delivery_proof=delivery if ready else connector.delivery_proof,
            gaps=() if ready else (CoverageGap(code=code, detail=code),),
        )
        waiting = (
            bindings_changed or changed_scope or intent_changed or bool(connector.source_removals)
            or ready and saved.state != "ready"
        )
        result_code = (
            "awaiting_controller_publication" if ready and saved.state != "ready"
            else code if waiting else "definition_verified"
        )
        await self._finish(work, saved, result_code)
        return ReconcileResult(work.work_id, "waiting" if waiting else "completed", result_code)

    async def _process(self, work: MonitoringWork) -> ReconcileResult:
        connector = await self._connector(work.connector_id)
        if (
            connector.workspace_id is None
            or connector.eventstream_id is None
            or connector.destination_id is None
            or connector.endpoint is None
        ):
            return await self._block(
                work, connector, "approved_nonsecret_connector_metadata_required"
            )
        if connector.connector_id != self.connector_id:
            return await self._defer(work, "explicit_shard_assignment_required")
        if connector.state in {"deleting", "deleted"}:
            current = await self._fresh_work(work)
            await asyncio.to_thread(
                self.store.disposition_work,
                WorkDispositionRequest(
                    **self.context.model_dump(),
                    request_id=str(uuid5(UUID(work.work_id), f"cancel:{current.revision}")),
                    work_id=work.work_id,
                    expected_work_revision=current.revision,
                    lease=current.lease,
                    disposition="cancelled",
                    detail="Connector is being removed",
                ),
            )
            return ReconcileResult(work.work_id, "completed", "connector_removed")
        if not connector.desired_definition:
            return await self._block(work, connector, "controller_definition_baseline_required")
        await self._renew(work)
        # Completed LRO IDs remain evidence. Only the receipt-bound submission
        # intent denotes an unfinished effect; new physical IDs alone do not.
        pending = any(
            gap.code in {INTENT_GAP, "definition_update_outcome_unknown"}
            for gap in connector.gaps
        )
        submitted = await self._submitted_definition(connector) if pending else None
        if pending and connector.operation_id is not None:
            reply = await self.rest.operation(connector.operation_id)
            state = reply.body.get("status")
            if state in {"Running", "NotStarted"}:
                return await self._defer(work, "definition_update_pending", reply.retry_at)
            if state != "Succeeded":
                return await self._block(work, connector, "definition_operation_failed_review")
        observed, topology = await self.rest.inspect(connector, lambda: self._renew(work))
        observed_at = self.clock()
        if submitted is not None:
            if matches_update(observed, submitted):
                return await self._verified(
                    work, connector, observed, topology, submitted_definition=submitted,
                )
            # An intent without acknowledgement may have reached the service.
            # A mismatch cannot establish rollback or authorize a second POST.
            return await self._defer(work, "definition_update_outcome_unknown")
        if matches_update(observed, connector.desired_definition):
            return await self._verified(work, connector, observed, topology)
        baseline = connector.observed_definition
        if not baseline:
            return await self._block(work, connector, "controller_definition_baseline_required")
        _complete_component_map(baseline)
        if not matches_update(observed, baseline):
            return await self._block(work, connector, "owned_definition_drift_review")
        planned_version = await self._version()
        if connector.policy_revision != planned_version.revision:
            return await self._defer(work, "current_controller_publication_required")
        hold = await self._removal_hold(connector)
        if hold is not None:
            if hold == "pending_source_removal_readmitted_requires_supersession":
                return await self._observe_retained_presence(
                    work, connector, observed, topology, observed_at=observed_at,
                )
            return await self._defer(work, hold)
        targets = await self._targets(connector, probe=True, work=work)
        desired_count = len(_nodes(_document(connector.desired_definition)["parts"]["eventstream.json"], "sources"))
        if len(targets) != desired_count:
            return await self._block(work, connector, "controller_scope_publication_required")
        current_targets = await self._targets(connector, probe=False, work=work)
        if {target.identity.key for target in current_targets} != {
            target.identity.key for target in targets
        }:
            raise MonitoringConflict("Admission changed after source read probes")
        intent_work = await self._fresh_work(work)
        intent = await self._save(
            intent_work, connector,
            expected_policy_revision=planned_version.revision,
            observed_definition=None,
            state="provisioning",
            operation_id=None,
            gaps=(self._submission_gap(connector, intent_work),),
        )
        # Recheck shared policy and this manifest immediately before the POST.
        await self._renew(work)
        current = await self._connector(intent.connector_id)
        version = await self._version()
        if current.revision != intent.revision or version.revision != intent.policy_revision:
            if current.revision == intent.revision:
                await self._save(
                    work, intent,
                    observed_definition=observed,
                    state="degraded",
                    gaps=(
                        CoverageGap(
                            code="policy_changed_before_update", detail="No update was sent"
                        ),
                    ),
                )
            return await self._defer(work, "policy_changed_before_definition_update")
        hold = await self._removal_hold(current)
        if hold is not None:
            await self._save(
                work, intent, observed_definition=observed, state="degraded", operation_id=None,
                gaps=(CoverageGap(code=hold, detail="No definition update was sent; current removal authority is unverified"),),
            )
            return await self._defer(work, hold)
        try:
            reply = await self.rest.update(intent, intent.desired_definition)
        except UpdateNotSent as exc:
            reset = await self._save(
                work, intent,
                observed_definition=observed,
                state="degraded",
                gaps=(CoverageGap(code=exc.code, detail=exc.detail, retry_at=exc.retry_at),),
            )
            return await self._defer(work, reset.gaps[0].code, exc.retry_at)
        except UpdateUncertain as exc:
            LOG.error(
                "connector_update_uncertain connector_id=%s work_id=%s",
                intent.connector_id,
                work.work_id,
            )
            return await self._defer(work, "definition_update_outcome_unknown", exc.retry_at)
        if reply.status == 202:
            # The state/definition intent remains durable even if this receipt
            # write fails; recovery reads the exact remote definition, never POSTs.
            acknowledged = await self._save(
                work, intent,
                operation_id=reply.operation_id,
                gaps=(
                    intent.gaps[0].model_copy(update={"retry_at": reply.retry_at}),
                ),
            )
            LOG.info(
                "connector_operation_recorded connector_id=%s operation_id=%s",
                acknowledged.connector_id,
                acknowledged.operation_id,
            )
            return await self._defer(work, "definition_update_pending", reply.retry_at)
        updated, status = await self.rest.inspect(intent, lambda: self._renew(work))
        return await self._verified(work, intent, updated, status)

    async def run_once(self) -> ReconcileRun:
        work_items = await asyncio.to_thread(self.store.claim_work, self.claim)
        results = []
        for work in work_items:
            if work.kind != "connector_reconcile" or work.connector_id is None:
                raise MonitoringConflict("Provisioning claim returned a different work kind")
            try:
                results.append(await self._process(work))
            except MonitoringLeaseLost:
                LOG.warning("connector_lease_lost work_id=%s", work.work_id)
                results.append(ReconcileResult(work.work_id, "lease_lost", "lease_lost"))
            except ProvisioningReview as exc:
                current = await self._connector(work.connector_id)
                results.append(await self._block(work, current, exc.code))
            except (RestReadError, MonitoringConflict) as exc:
                code = exc.code if isinstance(exc, RestReadError) else "shared_revision_changed"
                LOG.warning("connector_reconcile_deferred work_id=%s code=%s", work.work_id, code)
                results.append(await self._defer(work, code, getattr(exc, "retry_at", None)))
            except MonitoringStoreError:
                # A persistence outage leaves its durable claim/intent intact.
                LOG.error("connector_store_unavailable work_id=%s", work.work_id)
                raise
        return ReconcileRun(len(work_items), tuple(results))
