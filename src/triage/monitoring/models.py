"""Monitoring records and wire contracts, independent of either UI and any live SDK.

Times are explicit UTC instants; stores use database time for authorization and
leases. Validation bounds evidence but does not redact it. Persistence adapters
must redact inside their transaction boundary and serialize in Pydantic JSON mode.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    JsonValue,
    SerializerFunctionWrapHandler,
    StrictBool,
    StringConstraints,
    field_validator,
    model_serializer,
    model_validator,
)

from triage.models import Incident
from triage.pipeline_models import (
    PIPELINE_JOB_TYPES,
    PipelineActivity,
    PipelineTarget,
    canonical_id,
)
from triage.store.retries import MAX_ATTEMPTS

MONITORING_SCHEMA_VERSION = 1
MAX_JSON_BYTES = 65_536
MAX_JSON_DEPTH = 16
MAX_JSON_NODES = 4_096
MAX_INTAKE_BATCH = 200
MAX_POWERBI_WINDOW_ROWS = 5_000
MAX_RECONCILIATION_BINDINGS = MAX_POWERBI_WINDOW_ROWS + MAX_INTAKE_BATCH + 1

Workload = Literal["powerbi", "fabric_pipeline"]
RuntimeComponent = Literal["worker", "web", "controller", "fixture"]
ProducerComponent = Literal["worker", "web"]
WORKER_WORK_KINDS = frozenset({"inventory", "capability_probe", "poll", "connector_reconcile"})
CONTROLLER_WORK_KINDS = frozenset({"triage", "deferred_retry", "verify_action", "finalize", "reconcile_state"})
ActionKind = Literal[
    "powerbi_refresh", "pipeline_rerun", "rebind_dataset_gateway", "reenable_refresh_schedule",
]
ConfigurationAction = Literal["rebind_dataset_gateway", "reenable_refresh_schedule"]
ACTION_TO_TOOL: dict[ActionKind, str] = {
    "powerbi_refresh": "refresh_powerbi_dataset",
    "pipeline_rerun": "rerun_fabric_pipeline",
    "rebind_dataset_gateway": "rebind_dataset_gateway",
    "reenable_refresh_schedule": "reenable_refresh_schedule",
}
Completeness = Literal["complete", "partial", "unknown", "blocked"]
CapabilityStatus = Literal["verified", "denied", "unknown", "unsupported", "blocked"]
RunIdKind = Literal["fabric_job", "powerbi_request", "powerbi_refresh"]
WorkKind = Literal[
    "inventory", "capability_probe", "poll", "connector_reconcile",
    "triage", "deferred_retry", "verify_action", "finalize", "reconcile_state",
]
ActionState = Literal[
    "reserved", "rejected", "submitted", "uncertain", "verified_succeeded", "verified_failed",
]
SourceDisposition = Literal[
    "triaged", "duplicate", "historical", "refused", "failed",
]


def _opaque_text(value: str) -> str:
    if not value.strip() or value != value.strip():
        raise ValueError("An opaque identifier must be nonblank and have no surrounding whitespace")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("An opaque identifier must not contain control characters")
    return value


def _datetime_input(value: object) -> object:
    if not isinstance(value, (datetime, str)):
        raise ValueError("Use a timezone-aware datetime or an ISO 8601 string with an offset")
    return value


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


def _json_object(value: object) -> object:
    if not isinstance(value, dict):
        raise ValueError("A JSON object is required")
    remaining = MAX_JSON_NODES

    def visit(node: object, depth: int) -> None:
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > MAX_JSON_DEPTH:
            raise ValueError("JSON evidence exceeds the node or nesting limit")
        if isinstance(node, dict):
            for key, child in node.items():
                if not isinstance(key, str) or not key.strip() or len(key) > 256:
                    raise ValueError("JSON object keys must contain 1-256 nonblank characters")
                visit(child, depth + 1)
        elif isinstance(node, list):
            for child in node:
                visit(child, depth + 1)
        elif isinstance(node, float):
            if not math.isfinite(node):
                raise ValueError("JSON numbers must be finite")
        elif isinstance(node, str):
            if len(node) > MAX_JSON_BYTES:
                raise ValueError(f"JSON evidence exceeds {MAX_JSON_BYTES} bytes")
        elif node is not None and not isinstance(node, (str, bool, int)):
            raise ValueError("Only JSON values are allowed; implicit object conversion is forbidden")

    visit(value, 0)
    try:
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=False).encode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Evidence must be valid UTF-8 JSON") from exc
    if len(encoded) > MAX_JSON_BYTES:
        raise ValueError(f"JSON evidence exceeds {MAX_JSON_BYTES} bytes")
    return value


def _parameters(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    PipelineTarget.validate_parameters(value)
    return value


def _digest(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


CanonicalId = Annotated[str, Field(strict=True, max_length=64), AfterValidator(canonical_id)]
Revision = Annotated[int, Field(strict=True, ge=0)]
PositiveRevision = Annotated[int, Field(strict=True, ge=1)]
Count = Annotated[int, Field(strict=True, ge=0)]
Label = Annotated[
    str, StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=200),
    AfterValidator(_opaque_text),
]
OpaqueId = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=256), AfterValidator(_opaque_text),
]
StateKey = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=1_024),
    AfterValidator(_opaque_text),
]
Cursor = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=4_096),
    AfterValidator(_opaque_text),
]
Detail = Annotated[
    str, StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=4_096),
]
Fingerprint = Annotated[
    str, StringConstraints(strict=True, pattern=r"^[0-9a-fA-F]{64}$"), AfterValidator(str.lower),
]
UtcDateTime = Annotated[
    AwareDatetime, BeforeValidator(_datetime_input), AfterValidator(_utc),
]
JsonObject = Annotated[dict[str, JsonValue], BeforeValidator(_json_object)]
Parameters = Annotated[JsonObject, AfterValidator(_parameters)]
LeaseSeconds = Annotated[int, Field(strict=True, ge=15, le=900)]
BatchSize = Annotated[int, Field(strict=True, ge=1, le=MAX_INTAKE_BATCH)]


class MonitoringModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, validate_default=True, revalidate_instances="always",
    )


class MonitoringContext(MonitoringModel):
    tenant_id: CanonicalId
    epoch: CanonicalId


def _same_context(left: MonitoringContext, right: MonitoringContext) -> None:
    if (left.tenant_id, left.epoch) != (right.tenant_id, right.epoch):
        raise ValueError("Records belong to different monitoring tenants or epochs")


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _action_workload(action: ActionKind, workload: Workload) -> None:
    if (action == "pipeline_rerun") != (workload == "fabric_pipeline"):
        raise ValueError("The action does not match the target workload")


class RegistryVersion(MonitoringContext):
    revision: Revision


class DeploymentControl(RegistryVersion):
    schema_version: Literal[1] = MONITORING_SCHEMA_VERSION
    activation_cutoff: UtcDateTime
    maintenance: StrictBool = True
    updated_at: UtcDateTime

    @field_validator("schema_version", mode="before")
    @classmethod
    def strict_schema_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("Schema version must be an integer")
        return value


class BootstrapInspection(MonitoringModel):
    """A read-only deployment diagnosis, never an instruction to create tables."""

    status: Literal["ready", "maintenance", "missing", "incompatible", "wrong_tenant", "kernel_incomplete"]
    expected_tenant_id: CanonicalId
    found_schema_version: Revision | None = None
    control: DeploymentControl | None = None
    detail: Detail
    missing_operations: Annotated[tuple[OpaqueId, ...], Field(max_length=50)] = ()

    @model_validator(mode="after")
    def validate_status(self) -> BootstrapInspection:
        if self.status == "kernel_incomplete":
            if (
                self.control is None or self.control.tenant_id != self.expected_tenant_id
                or self.found_schema_version != MONITORING_SCHEMA_VERSION or not self.missing_operations
            ):
                raise ValueError("An incomplete kernel must identify the current baseline and its missing operations")
            return self
        if self.missing_operations:
            raise ValueError("Missing guarded operations cannot be reported as a ready baseline")
        if self.status in {"ready", "maintenance"}:
            if self.control is None or self.found_schema_version != MONITORING_SCHEMA_VERSION:
                raise ValueError("A ready inspection requires the supported deployment control")
            if self.control.tenant_id != self.expected_tenant_id:
                raise ValueError("A different deployment tenant cannot be ready")
            if self.control.maintenance != (self.status == "maintenance"):
                raise ValueError("Inspection and deployment maintenance state disagree")
        elif self.status == "missing":
            if self.control is not None or self.found_schema_version is not None:
                raise ValueError("Missing bootstrap cannot include initialized control state")
        elif self.status == "wrong_tenant":
            if self.control is None or self.control.tenant_id == self.expected_tenant_id:
                raise ValueError("Wrong-tenant inspection requires a different pinned tenant")
        elif self.found_schema_version in {None, MONITORING_SCHEMA_VERSION}:
            raise ValueError("Incompatible bootstrap requires an unsupported schema version")
        return self


class TargetIdentity(MonitoringContext):
    workload: Workload
    workspace_id: CanonicalId
    item_id: CanonicalId

    @property
    def key(self) -> str:
        return (
            f"monitor:v1:{self.epoch}:{self.tenant_id}:"
            f"{self.workload}:{self.workspace_id}:{self.item_id}"
        )

    def execution_key(self, run_id: str, run_id_kind: RunIdKind) -> str:
        return SourceExecutionIdentity(target=self, run_id=run_id, run_id_kind=run_id_kind).key

    def incident_key(self, signature: str) -> str:
        return IncidentIdentity(target=self, signature=signature).key


class SourceExecutionIdentity(MonitoringModel):
    """Power BI history IDs and request IDs occupy different namespaces.

    Intake adapters resolve aliases with REST to one common authoritative ID
    before admission. Equal-looking IDs in different namespaces are not proof.
    """

    target: TargetIdentity
    run_id_kind: RunIdKind
    run_id: OpaqueId

    @model_validator(mode="after")
    def validate_run_id(self) -> SourceExecutionIdentity:
        is_pipeline = self.target.workload == "fabric_pipeline"
        if is_pipeline != (self.run_id_kind == "fabric_job"):
            raise ValueError("Source execution ID kind does not match the workload")
        if self.run_id_kind == "powerbi_refresh":
            if not self.run_id.isascii() or not self.run_id.isdecimal() or int(self.run_id) <= 0:
                raise ValueError("Power BI refresh history IDs must be positive decimal strings")
            normalized = str(int(self.run_id))
        else:
            normalized = canonical_id(self.run_id)
        object.__setattr__(self, "run_id", normalized)
        return self

    @property
    def key(self) -> str:
        return f"{self.target.key}:run:{self.run_id_kind}:{self.run_id}"


class IncidentIdentity(MonitoringModel):
    """The signature comes from the existing deterministic signature implementation."""

    target: TargetIdentity
    signature: OpaqueId

    @property
    def key(self) -> str:
        return f"{self.target.key}:incident:{_digest(self.signature)}"


class PollCadence(MonitoringModel):
    poll_seconds: Annotated[int, Field(strict=True, ge=15, le=86_400)] = 300
    reconciliation_seconds: Annotated[int, Field(strict=True, ge=15, le=86_400)] = 900


class ScopeSelector(MonitoringModel):
    tenant_id: CanonicalId
    kind: Literal["tenant", "domain", "workspace", "item"]
    domain_id: CanonicalId | None = None
    workspace_id: CanonicalId | None = None
    item_id: CanonicalId | None = None
    include_descendants: StrictBool = False

    @model_validator(mode="after")
    def validate_selector(self) -> ScopeSelector:
        present = (
            self.domain_id is not None, self.workspace_id is not None, self.item_id is not None,
        )
        expected = {
            "tenant": (False, False, False), "domain": (True, False, False),
            "workspace": (False, True, False), "item": (False, True, True),
        }
        if present != expected[self.kind]:
            raise ValueError("Scope IDs do not match the selector kind")
        if self.include_descendants and self.kind != "domain":
            raise ValueError("Descendants apply only to a domain selector")
        return self


class ScopeRule(MonitoringModel):
    rule_id: CanonicalId
    selector: ScopeSelector
    effect: Literal["include", "exclude"]
    workloads: Annotated[tuple[Workload, ...], Field(min_length=1, max_length=2)] = (
        "powerbi", "fabric_pipeline",
    )
    auto_enrol_detection_only: StrictBool = False

    @model_validator(mode="after")
    def validate_rule(self) -> ScopeRule:
        _unique(self.workloads, "Workloads")
        if self.effect == "exclude" and self.auto_enrol_detection_only:
            raise ValueError("An exclusion cannot automatically enrol targets")
        return self


class ScopeDefinition(MonitoringContext):
    scope_id: CanonicalId
    name: Label
    enabled: StrictBool = True
    rules: Annotated[tuple[ScopeRule, ...], Field(max_length=1_000)] = ()
    cadence: PollCadence = Field(default_factory=PollCadence)

    @model_validator(mode="after")
    def validate_rules(self) -> ScopeDefinition:
        _unique(tuple(rule.rule_id for rule in self.rules), "Scope rule IDs")
        if any(rule.selector.tenant_id != self.tenant_id for rule in self.rules):
            raise ValueError("All scope rules must select the deployment tenant")
        return self


class ScopePolicy(ScopeDefinition):
    revision: Revision
    updated_at: UtcDateTime | None = None


class CoverageGap(MonitoringModel):
    code: OpaqueId
    detail: Detail
    workspace_id: CanonicalId | None = None
    item_id: CanonicalId | None = None
    retry_at: UtcDateTime | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> CoverageGap:
        if self.item_id is not None and self.workspace_id is None:
            raise ValueError("An item coverage gap also needs its workspace")
        return self


class InventoryGeneration(MonitoringContext):
    generation_id: CanonicalId
    selector: ScopeSelector
    adapter: Label
    authority: Literal["tenant_admin", "caller_visible", "fixture"]
    enumeration: Literal["items", "workspaces", "domains"] = "items"
    completeness: Completeness
    started_at: UtcDateTime
    completed_at: UtcDateTime | None = None
    continuation: Cursor | None = None
    discovered_count: Count = 0
    completed_pages: Count = 0
    revision: Revision = 0
    recorded_item_count: Count = 0
    recorded_workspace_count: Count = 0
    recorded_domain_count: Count = 0
    next_scan_at: UtcDateTime | None = None
    gaps: Annotated[tuple[CoverageGap, ...], Field(max_length=200)] = ()

    @model_validator(mode="after")
    def validate_completion(self) -> InventoryGeneration:
        if self.selector.tenant_id != self.tenant_id:
            raise ValueError("Inventory selector belongs to a different tenant")
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("Inventory cannot finish before it starts")
        if self.completeness == "complete":
            if self.completed_at is None or self.continuation is not None:
                raise ValueError("Complete inventory requires a finished scan without continuation")
            if self.selector.kind == "tenant" and self.authority == "caller_visible":
                raise ValueError("Caller-visible enumeration does not prove complete tenant inventory")
        elif not self.gaps:
            raise ValueError("Incomplete inventory must expose its coverage gap")
        if self.next_scan_at is not None and (
            self.completed_at is None
            or not 60 <= (self.next_scan_at - self.completed_at).total_seconds() <= 86_400
        ):
            raise ValueError("Periodic inventory requires a finished generation and a bounded next scan")
        return self


class InventoryWorkspace(MonitoringContext):
    generation_id: CanonicalId
    workspace_id: CanonicalId
    name: Label
    domain_id: CanonicalId | None = None
    capacity_id: CanonicalId | None = None
    state: Literal["present", "deleted", "unknown"] = "present"
    observed_at: UtcDateTime


class InventoryDomain(MonitoringContext):
    generation_id: CanonicalId
    domain_id: CanonicalId
    name: Label
    parent_domain_id: CanonicalId | None = None
    state: Literal["present", "deleted", "unknown"] = "present"
    observed_at: UtcDateTime

    @model_validator(mode="after")
    def validate_parent(self) -> InventoryDomain:
        if self.parent_domain_id == self.domain_id:
            raise ValueError("A domain cannot be its own parent")
        return self


class InventoryItem(MonitoringContext):
    generation_id: CanonicalId
    workspace_id: CanonicalId
    item_id: CanonicalId
    name: Label
    item_type: Label
    workload: Workload | None = None
    unsupported_reason: Detail | None = None
    domain_ids: Annotated[tuple[CanonicalId, ...], Field(max_length=100)] = ()
    domain_ancestor_ids: Annotated[tuple[CanonicalId, ...], Field(max_length=100)] = ()
    state: Literal["present", "deleted", "unknown"] = "present"
    observed_at: UtcDateTime
    definition_hash: Fingerprint | None = None

    @model_validator(mode="after")
    def validate_workload(self) -> InventoryItem:
        _unique(self.domain_ids, "Domain IDs")
        _unique(self.domain_ancestor_ids, "Domain ancestor IDs")
        if self.workload is None:
            if self.unsupported_reason is None:
                raise ValueError("Unsupported inventory must explain the missing workload contract")
        else:
            supported = {"powerbi": {"SemanticModel", "Dataset"}, "fabric_pipeline": {"DataPipeline"}}
            if self.item_type not in supported[self.workload] or self.unsupported_reason is not None:
                raise ValueError("Inventory item type and supported workload disagree")
        return self

    @property
    def target(self) -> TargetIdentity | None:
        if self.workload is None:
            return None
        return TargetIdentity(
            tenant_id=self.tenant_id, epoch=self.epoch, workload=self.workload,
            workspace_id=self.workspace_id, item_id=self.item_id,
        )


class CollectionCommit(MonitoringModel):
    work_id: CanonicalId
    lease: LeaseToken
    expected_work_revision: Revision


class InventoryCommit(CollectionCommit):
    """A collector commit fence and the exact generation position it read."""

    expected_generation_revision: Revision
    expected_continuation: Cursor | None = None


class InventoryBatch(MonitoringModel):
    request_id: CanonicalId
    expected: RegistryVersion
    generation: InventoryGeneration
    items: Annotated[tuple[InventoryItem, ...], Field(max_length=1_000)]
    workspaces: Annotated[tuple[InventoryWorkspace, ...], Field(max_length=1_000)] = ()
    domains: Annotated[tuple[InventoryDomain, ...], Field(max_length=1_000)] = ()
    commit: InventoryCommit | None = None

    @model_validator(mode="after")
    def validate_items(self) -> InventoryBatch:
        _same_context(self.expected, self.generation)
        if self.commit is not None:
            _lease_for(self.commit.lease, self.expected, work_key(self.expected, self.commit.work_id))
        keys = []
        for item in self.items:
            _same_context(self.generation, item)
            if item.generation_id != self.generation.generation_id:
                raise ValueError("Inventory item belongs to a different generation")
            if item.state == "deleted" and self.generation.completeness != "complete":
                raise ValueError("Incomplete inventory cannot establish deletion")
            keys.append(f"{item.workspace_id}:{item.item_id}")
        _unique(tuple(keys), "Inventory identities")
        for entries in (self.workspaces, self.domains):
            for entry in entries:
                _same_context(self.generation, entry)
                if entry.generation_id != self.generation.generation_id:
                    raise ValueError("Catalogue metadata belongs to a different generation")
                if entry.state == "deleted" and self.generation.completeness != "complete":
                    raise ValueError("Incomplete inventory cannot establish container deletion")
        _unique(tuple(entry.workspace_id for entry in self.workspaces), "Workspace identities")
        _unique(tuple(entry.domain_id for entry in self.domains), "Domain identities")
        return self


class EventCapabilityEvidence(MonitoringModel):
    connector_id: CanonicalId
    ownership_id: CanonicalId
    source_id: OpaqueId
    eventstream_id: CanonicalId
    destination_id: OpaqueId
    definition_hash: Fingerprint
    endpoint_hash: Fingerprint
    event_types: Annotated[tuple[OpaqueId, ...], Field(min_length=1, max_length=20)]
    observed_at: UtcDateTime


class CapabilityObservation(MonitoringModel):
    capability_id: CanonicalId
    target: TargetIdentity
    inventory_generation: CanonicalId
    collector_identity_id: CanonicalId
    read_status: CapabilityStatus
    event_status: CapabilityStatus = "unknown"
    event_evidence: EventCapabilityEvidence | None = None
    action_status: CapabilityStatus = "unknown"
    exact_action_correlation: StrictBool = False
    configuration_verification: StrictBool = False
    definition_hash: Fingerprint | None = None
    checked_at: UtcDateTime
    expires_at: UtcDateTime
    required_permissions: Annotated[tuple[Label, ...], Field(max_length=100)] = ()
    gaps: Annotated[tuple[CoverageGap, ...], Field(max_length=200)] = ()

    @model_validator(mode="after")
    def validate_probe(self) -> CapabilityObservation:
        if self.expires_at <= self.checked_at:
            raise ValueError("Capability evidence must expire after the probe")
        if self.event_evidence is not None and (
            self.event_status != "verified" or self.read_status != "verified"
            or not self.checked_at <= self.event_evidence.observed_at < self.expires_at
        ):
            raise ValueError("Event evidence requires a fresh source read and bounded transport inspection")
        if self.action_status == "verified" and (
            self.read_status != "verified"
            or not (
                self.exact_action_correlation
                or (self.target.workload == "powerbi" and self.configuration_verification)
            )
        ):
            raise ValueError("Action capability requires source access and exact correlation proof")
        return self


class ObservationPolicy(MonitoringModel):
    enabled: StrictBool = False
    events_enabled: StrictBool = False
    cadence: PollCadence = Field(default_factory=PollCadence)

    @model_validator(mode="after")
    def validate_events(self) -> ObservationPolicy:
        if self.events_enabled and not self.enabled:
            raise ValueError("Event intake cannot be enabled while observation is disabled")
        return self


class ActionPolicy(MonitoringModel):
    enabled: StrictBool = False
    action: ActionKind | None = None
    review_id: CanonicalId | None = None
    review_revision: PositiveRevision | None = None

    @model_validator(mode="after")
    def validate_review(self) -> ActionPolicy:
        if (self.review_id is None) != (self.review_revision is None):
            raise ValueError("Safety-review identity and revision must be supplied together")
        if self.enabled and (self.action is None or self.review_id is None):
            raise ValueError("Action admission requires an explicit action and safety-review reference")
        return self


class MonitoringTarget(MonitoringModel):
    identity: TargetIdentity
    name: Label
    scope_ids: Annotated[tuple[CanonicalId, ...], Field(min_length=1, max_length=1_000)]
    admitted_rule_ids: Annotated[tuple[CanonicalId, ...], Field(min_length=1, max_length=1_000)]
    inventory_generation: CanonicalId
    capability_id: CanonicalId
    policy_revision: Revision
    admitted_at: UtcDateTime
    state: Literal["current", "review_required", "paused", "removed"]
    admission_basis: Literal["reviewed", "auto_detection_only", "pending_review"]
    reason: Detail
    observation: ObservationPolicy = Field(default_factory=ObservationPolicy)
    action: ActionPolicy = Field(default_factory=ActionPolicy)
    next_poll_at: UtcDateTime | None = None

    @model_validator(mode="after")
    def validate_admission(self) -> MonitoringTarget:
        _unique(self.scope_ids, "Admitting scopes")
        _unique(self.admitted_rule_ids, "Admitting rules")
        if self.state != "current" and (self.observation.enabled or self.action.enabled):
            raise ValueError("An inactive target cannot have effective observation or action admission")
        if self.admission_basis == "pending_review" and self.state == "current":
            raise ValueError("Pending review does not admit a target")
        if self.action.action is not None:
            _action_workload(self.action.action, self.identity.workload)
        if self.action.enabled and (
            not self.observation.enabled or self.admission_basis != "reviewed"
        ):
            raise ValueError("Automatic detection-only admission never grants action capability")
        return self

    @property
    def key(self) -> str:
        return self.identity.key


class TargetChange(MonitoringModel):
    identity: TargetIdentity
    change: Literal["admit", "pause", "remove", "retain"]
    reason: Detail
    basis: Literal["explicit_policy", "completed_inventory", "uncertain_inventory"]

    @model_validator(mode="after")
    def validate_removal(self) -> TargetChange:
        if self.change == "remove" and self.basis == "uncertain_inventory":
            raise ValueError("Uncertain inventory may pause, but cannot remove, an admission")
        return self


class ScopePreviewRequest(MonitoringModel):
    expected: RegistryVersion
    idempotency_id: CanonicalId
    scope: ScopeDefinition
    requested_by: OpaqueId | None = Field(
        default=None, description="Audit identity supplied by the verified server Actor, not browser input.",
    )

    @model_validator(mode="after")
    def validate_context(self) -> ScopePreviewRequest:
        _same_context(self.expected, self.scope)
        return self


class ActivationPlan(ScopePreviewRequest):
    plan_id: CanonicalId
    created_at: UtcDateTime
    expires_at: UtcDateTime
    inventory_generations: Annotated[tuple[CanonicalId, ...], Field(max_length=1_000)] = ()
    inventory_completeness: Completeness
    inventory_revision: Revision = 0
    status: Literal["ready", "blocked"]
    changes: Annotated[tuple[TargetChange, ...], Field(max_length=5_000)] = ()
    required_permissions: Annotated[tuple[Label, ...], Field(max_length=100)] = ()
    poll_count_delta: Annotated[int, Field(strict=True)]
    subscription_count_delta: Annotated[int, Field(strict=True)]
    gaps: Annotated[tuple[CoverageGap, ...], Field(max_length=200)] = ()

    @model_validator(mode="after")
    def validate_plan(self) -> ActivationPlan:
        if self.expires_at <= self.created_at:
            raise ValueError("Activation plan must expire after creation")
        _unique(self.inventory_generations, "Inventory generations")
        _unique(tuple(change.identity.key for change in self.changes), "Planned targets")
        for change in self.changes:
            _same_context(self.expected, change.identity)
        if (self.status == "blocked" or self.inventory_completeness != "complete") and not self.gaps:
            raise ValueError("Blocked or incomplete plans must expose their gaps")
        return self

    @property
    def scope_hash(self) -> str:
        return _digest(self.scope.model_dump(mode="json"))


class ActivateScopeRequest(MonitoringModel):
    expected: RegistryVersion
    plan_id: CanonicalId
    idempotency_id: CanonicalId


class ActivationReceipt(MonitoringModel):
    plan_id: CanonicalId
    idempotency_id: CanonicalId
    version: RegistryVersion
    scope: ScopePolicy
    activated_at: UtcDateTime
    state: Literal["configuring", "active"]
    requested_by: OpaqueId | None = None
    queued_work_ids: Annotated[tuple[CanonicalId, ...], Field(max_length=10_000)] = ()

    @model_validator(mode="after")
    def validate_version(self) -> ActivationReceipt:
        _same_context(self.version, self.scope)
        if self.version.revision != self.scope.revision:
            raise ValueError("Activated policy must carry the committed registry revision")
        _unique(self.queued_work_ids, "Provisioning work IDs")
        return self


class CoverageView(RegistryVersion):
    """Deployment-wide inventory counters; scope-preview readiness is separate."""

    as_of: UtcDateTime
    inventory_completeness: Completeness
    capability_completeness: Completeness
    scope_item_count: Count | None
    discovered_count: Count
    access_verified_count: Count
    admitted_count: Count
    current_count: Count
    action_enabled_count: Count
    unsupported_count: Count
    backlog_count: Count
    last_inventory_completed_at: UtcDateTime | None = None
    last_poll_window_end: UtcDateTime | None = None
    last_receiver_activity_at: UtcDateTime | None = None
    checkpoint_lag_seconds: Count | None = None
    next_due_at: UtcDateTime | None = None
    gaps: Annotated[tuple[CoverageGap, ...], Field(max_length=200)] = ()

    @model_validator(mode="after")
    def validate_counts(self) -> CoverageView:
        if not (
            self.action_enabled_count <= self.current_count <= self.admitted_count
            <= self.discovered_count
        ):
            raise ValueError("Action/current/admitted/discovered coverage counts are inconsistent")
        if self.access_verified_count > self.discovered_count:
            raise ValueError("Verified access cannot exceed discovered inventory")
        if self.unsupported_count + self.admitted_count > self.discovered_count:
            raise ValueError("Unsupported items cannot count as admitted")
        if self.scope_item_count is not None and self.discovered_count > self.scope_item_count:
            raise ValueError("Discovered inventory exceeds the known scope denominator")
        if self.inventory_completeness == "complete" and self.scope_item_count is None:
            raise ValueError("Complete inventory requires a known scope denominator")
        if (
            self.inventory_completeness != "complete" or self.capability_completeness != "complete"
        ) and not self.gaps:
            raise ValueError("Incomplete coverage must expose its gaps")
        return self


class MonitoringSnapshot(MonitoringModel):
    """A read projection, not the persisted representation of the registry."""

    control: DeploymentControl
    coverage: CoverageView

    @model_validator(mode="after")
    def validate_version(self) -> MonitoringSnapshot:
        _same_context(self.control, self.coverage)
        if self.control.revision != self.coverage.revision:
            raise ValueError("A snapshot must use one registry revision")
        return self


class OperationReceipt(MonitoringContext):
    operation: OpaqueId
    request_id: OpaqueId
    fingerprint: Fingerprint
    recorded_at: UtcDateTime
    result: dict[str, JsonValue]


class PageQuery(MonitoringContext):
    limit: Annotated[int, Field(strict=True, ge=1, le=1_000)] = 100
    cursor: Cursor | None = None


class TargetQuery(PageQuery):
    workload: Workload | None = None
    workspace_id: CanonicalId | None = None
    include_inactive: StrictBool = False


RecordT = TypeVar("RecordT", bound=MonitoringModel)


class RecordPage(MonitoringModel, Generic[RecordT]):
    version: RegistryVersion
    as_of: UtcDateTime
    items: Annotated[tuple[RecordT, ...], Field(max_length=1_000)]
    next_cursor: Cursor | None = None


class ConnectorSource(MonitoringModel):
    source_id: OpaqueId
    target: TargetIdentity
    event_types: Annotated[tuple[OpaqueId, ...], Field(min_length=1, max_length=20)]
    event_source: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=2_048)] | None = None

    @model_validator(mode="after")
    def validate_events(self) -> ConnectorSource:
        _unique(self.event_types, "Connector event types")
        return self


class ConnectorSourceProposal(MonitoringModel):
    proposal_id: CanonicalId
    node_name: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_.-]+$")]
    source_id: None
    target: TargetIdentity
    event_types: Annotated[tuple[OpaqueId, ...], Field(min_length=1, max_length=20)]
    event_source: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=2_048)] | None = None

    @model_validator(mode="after")
    def validate_events(self) -> ConnectorSourceProposal:
        _unique(self.event_types, "Proposed connector event types")
        return self


SqlPayloadHash = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9A-F]{64}$")]


class SourceRemovalIntent(MonitoringModel):
    removal_id: CanonicalId
    source_id: OpaqueId | None
    proposal_id: CanonicalId | None
    detail: Detail

    @model_validator(mode="after")
    def validate_selector(self) -> SourceRemovalIntent:
        if (self.source_id is None) == (self.proposal_id is None):
            raise ValueError("Removal must select exactly one owned physical source or logical proposal")
        if self.source_id is not None and len(self.source_id.encode("utf-16-le")) > 512:
            raise ValueError("Owned physical source identity exceeds its SQL bound")
        if len(self.detail.encode("utf-16-le")) > 4000:
            raise ValueError("Source removal detail exceeds its SQL bound")
        return self


class PendingSourceRemoval(SourceRemovalIntent):
    node_name: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=256)]
    last_observed_source_id: CanonicalId | None
    target: TargetIdentity
    binding_hash: SqlPayloadHash
    policy_revision: Revision
    request_id: CanonicalId
    publication_id: OpaqueId
    requested_at: UtcDateTime
    state: Literal["pending_remote_absence"]

    @model_validator(mode="after")
    def validate_binding(self) -> PendingSourceRemoval:
        if len(self.node_name.encode("utf-16-le")) > 512 or len(self.publication_id.encode("utf-16-le")) > 256:
            raise ValueError("Pending source removal exceeds its node or publication identity bound")
        return self

    def intent(self) -> SourceRemovalIntent:
        return SourceRemovalIntent.model_validate(self.model_dump(include=set(SourceRemovalIntent.model_fields)))


class SourceRemovalSupersession(MonitoringModel):
    removal_id: CanonicalId
    source_id: OpaqueId

    @model_validator(mode="after")
    def validate_physical_selector(self) -> SourceRemovalSupersession:
        if len(self.source_id.encode("utf-16-le")) > 512:
            raise ValueError("Superseded physical source identity exceeds its SQL bound")
        return self


class ConnectorSourceRetirement(MonitoringModel):
    connector_id: CanonicalId
    ownership_id: CanonicalId
    removal_id: CanonicalId
    source_id: OpaqueId | None
    proposal_id: CanonicalId | None
    node_name: Annotated[str, StringConstraints(strict=True, min_length=1, max_length=256)]
    original_binding: ConnectorSource | ConnectorSourceProposal
    original_removal: PendingSourceRemoval
    observation_receipt_id: CanonicalId
    observation_fingerprint: Fingerprint
    observation_binding_hash: SqlPayloadHash
    observation_receipt_hash: SqlPayloadHash
    observed_definition_hash: SqlPayloadHash
    confirmation_request_id: CanonicalId
    work_id: CanonicalId
    work_fence: PositiveRevision
    policy_revision: Revision
    retired_at: UtcDateTime
    state: Literal["retired_verified"]

    @model_validator(mode="after")
    def validate_retirement(self) -> ConnectorSourceRetirement:
        original = self.original_removal
        if (
            (self.removal_id, self.source_id, self.proposal_id, self.node_name)
            != (original.removal_id, original.source_id, original.proposal_id, original.node_name)
            or self.original_binding.target != original.target or self.retired_at < original.requested_at
        ):
            raise ValueError("Retirement must retain its exact original binding and removal")
        if isinstance(self.original_binding, ConnectorSource):
            if self.original_binding.source_id != self.source_id or self.proposal_id is not None:
                raise ValueError("Physical retirement must identify the original physical source")
        elif self.original_binding.proposal_id != self.proposal_id or self.source_id is not None:
            raise ValueError("Logical withdrawal must identify the original proposal")
        return self


class EndpointMetadata(MonitoringModel):
    """Nonsecret Event Hubs protocol metadata, not a connection string."""

    namespace: Annotated[
        str, StringConstraints(strict=True, max_length=253, pattern=r"^[A-Za-z0-9.-]+$"),
    ]
    entity: OpaqueId
    consumer_group: OpaqueId

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        parts = value.split(".")
        if len(parts) < 2 or any(
            not part or len(part) > 63 or part.startswith("-") or part.endswith("-")
            for part in parts
        ):
            raise ValueError("Namespace must be a DNS host without a scheme, port, path or credentials")
        return value.lower()


def connector_definition_hash(definition: JsonObject) -> str:
    """Match the canonical NVARCHAR definition fragment persisted by the store."""
    document = json.dumps(definition, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(document.encode("utf-16-le")).hexdigest().upper()


class EventTransportEvidence(MonitoringModel):
    """Receiver evidence, bound to the original intake request, not readiness authority."""

    request_id: CanonicalId
    ownership_id: CanonicalId
    policy_revision: Revision
    workspace_id: CanonicalId
    eventstream_id: CanonicalId
    destination_id: OpaqueId
    endpoint: EndpointMetadata
    definition_hash: SqlPayloadHash
    source_id: OpaqueId
    collector_identity_id: CanonicalId
    identity_verified_at: UtcDateTime

    def matches_connector(self, connector: OwnedConnectorManifest, policy_revision: int) -> bool:
        return (
            self.ownership_id == connector.ownership_id
            and self.policy_revision == connector.policy_revision == policy_revision
            and (self.workspace_id, self.eventstream_id, self.destination_id)
            == (connector.workspace_id, connector.eventstream_id, connector.destination_id)
            and self.endpoint == connector.endpoint
            and self.definition_hash == connector_definition_hash(connector.desired_definition)
        )


class ConnectorDeliveryProof(MonitoringModel):
    """A reference to immutable accepted transport evidence, never a heartbeat."""

    request_id: CanonicalId
    receipt_key: StateKey
    collector_identity_id: CanonicalId
    received_at: UtcDateTime
    identity_verified_at: UtcDateTime

    @model_validator(mode="after")
    def validate_identity_time(self) -> ConnectorDeliveryProof:
        if self.identity_verified_at > self.received_at:
            raise ValueError("Delivery identity evidence cannot follow its original received event")
        return self


class OwnedConnectorManifest(MonitoringContext):
    """Retained physical/logical ownership, separate from the effective desired graph."""

    connector_id: CanonicalId
    ownership_id: CanonicalId
    revision: Revision
    policy_revision: Revision
    workspace_id: CanonicalId | None = None
    eventstream_id: CanonicalId | None = None
    destination_id: OpaqueId | None = None
    name: Label
    sources: Annotated[tuple[ConnectorSource, ...], Field(max_length=1_000)]
    source_proposals: Annotated[tuple[ConnectorSourceProposal, ...], Field(max_length=1_000)] = ()
    source_removals: Annotated[tuple[PendingSourceRemoval, ...], Field(max_length=1_000)] = ()
    desired_definition: JsonObject
    observed_definition: JsonObject | None = None
    endpoint: EndpointMetadata | None = None
    operation_id: OpaqueId | None = None
    state: Literal["planned", "provisioning", "ready", "degraded", "blocked", "deleting", "deleted"]
    updated_at: UtcDateTime
    identity_verified_at: UtcDateTime | None = None
    delivery_verified_at: UtcDateTime | None = None
    delivery_proof: ConnectorDeliveryProof | None = None
    last_receiver_activity_at: UtcDateTime | None = None
    gaps: Annotated[tuple[CoverageGap, ...], Field(max_length=200)] = ()

    @model_validator(mode="after")
    def validate_owned_topology(self) -> OwnedConnectorManifest:
        _unique(tuple(source.source_id for source in self.sources), "Connector source IDs")
        _unique(tuple(source.target.key for source in self.sources), "Connector target subscriptions")
        _unique(tuple(source.proposal_id for source in self.source_proposals), "Connector proposal IDs")
        _unique(tuple(source.node_name for source in self.source_proposals), "Connector proposal node names")
        _unique(tuple(source.target.key for source in (*self.sources, *self.source_proposals)), "Desired connector targets")
        for source in (*self.sources, *self.source_proposals):
            _same_context(self, source.target)
        _unique(tuple(removal.removal_id for removal in self.source_removals), "Pending source removal IDs")
        for removal in self.source_removals:
            _same_context(self, removal.target)
            if not any(
                source.source_id == removal.source_id and source.target == removal.target
                for source in self.sources
            ) and not any(
                proposal.proposal_id == removal.proposal_id and proposal.target == removal.target
                for proposal in self.source_proposals
            ):
                raise ValueError("Pending removal must retain its physical source or logical proposal ownership")
        if self.state == "ready" and (
            self.workspace_id is None or self.eventstream_id is None or self.destination_id is None
            or not self.sources or self.source_proposals or self.source_removals
            or self.endpoint is None or self.observed_definition is None
            or _digest(self.desired_definition) != _digest(self.observed_definition)
            or self.identity_verified_at is None or self.delivery_verified_at is None
        ):
            raise ValueError("Ready requires round-tripped topology and identity/delivery proof")
        if self.state in {"blocked", "degraded"} and not self.gaps:
            raise ValueError("Blocked or degraded connectors must expose their gaps")
        if self.delivery_proof is not None and (
            self.delivery_verified_at != self.delivery_proof.received_at
            or self.identity_verified_at != self.delivery_proof.identity_verified_at
            or self.identity_verified_at > self.delivery_proof.received_at
        ):
            raise ValueError("Delivery proof must retain its original receive and identity times")
        return self


class ConnectorPublicationRequest(MonitoringModel):
    """Controller intent, not an assertion of physical bindings or readiness.

    Keep current ownership in sources/source_proposals and request exclusions
    through source_removals. An original observation receipt can bind proposals
    or retire absent sources, so the returned ownership may differ from this input.
    """

    request_id: CanonicalId
    expected: RegistryVersion
    work_id: CanonicalId
    lease: LeaseToken
    expected_work_revision: PositiveRevision
    expected_frontier_revision: PositiveRevision
    connector_id: CanonicalId
    ownership_id: CanonicalId
    expected_connector_revision: Revision
    name: Label
    sources: Annotated[tuple[ConnectorSource, ...], Field(max_length=1_000)]
    source_proposals: Annotated[tuple[ConnectorSourceProposal, ...], Field(max_length=1_000)] = ()
    source_removals: Annotated[tuple[SourceRemovalIntent, ...], Field(max_length=1_000)] = ()
    source_removal_supersessions: Annotated[tuple[SourceRemovalSupersession, ...], Field(max_length=1_000)] = ()
    desired_definition: JsonObject
    observation_receipt_id: CanonicalId | None = None
    readiness_receipt_id: CanonicalId | None = None
    detail: Detail

    @model_validator(mode="after")
    def validate_publication(self) -> ConnectorPublicationRequest:
        _lease_for(self.lease, self.expected, work_key(self.expected, self.work_id))
        _unique(tuple(source.source_id for source in self.sources), "Published source identities")
        _unique(tuple(source.proposal_id for source in self.source_proposals), "Published proposal IDs")
        _unique(tuple(source.node_name for source in self.source_proposals), "Published proposal node names")
        _unique(tuple(source.target.key for source in (*self.sources, *self.source_proposals)), "Published source targets")
        for source in (*self.sources, *self.source_proposals):
            _same_context(self.expected, source.target)
        if self.observation_receipt_id is not None and self.readiness_receipt_id is not None:
            raise ValueError("Physical proposal binding and readiness are separate receipt-bound publications")
        if self.readiness_receipt_id is not None and (self.source_proposals or self.source_removals):
            raise ValueError("Readiness requires all proposals bound and removals verified")
        supersessions = self.source_removal_supersessions
        if supersessions and (self.observation_receipt_id is None or self.readiness_receipt_id is not None):
            raise ValueError("Source-removal supersession requires original observation evidence, never readiness")
        _unique(tuple(item.removal_id for item in supersessions), "Superseded removal IDs")
        _unique(tuple(item.source_id for item in supersessions), "Superseded physical source IDs")
        if (
            {item.removal_id for item in supersessions} & {item.removal_id for item in self.source_removals}
            or {item.source_id for item in supersessions} & {
                item.source_id for item in self.source_removals if item.source_id is not None
            }
            or not {item.source_id for item in supersessions}.issubset({item.source_id for item in self.sources})
        ):
            raise ValueError("Supersession must select retained physical sources separately from pending removals")
        _unique(tuple(removal.removal_id for removal in self.source_removals), "Source removal IDs")
        _unique(tuple(
            f"source:{removal.source_id}" if removal.source_id is not None else f"proposal:{removal.proposal_id}"
            for removal in self.source_removals
        ), "Source removal selectors")
        removed_sources = {removal.source_id for removal in self.source_removals if removal.source_id is not None}
        removed_proposals = {removal.proposal_id for removal in self.source_removals if removal.proposal_id is not None}
        if not removed_sources.issubset({source.source_id for source in self.sources}) or not removed_proposals.issubset(
            {proposal.proposal_id for proposal in self.source_proposals}
        ):
            raise ValueError("Source removal selects ownership absent from the retained request")
        validate_connector_definition(
            tuple(source for source in self.sources if source.source_id not in removed_sources),
            self.desired_definition,
            proposals=tuple(proposal for proposal in self.source_proposals if proposal.proposal_id not in removed_proposals),
        )
        if self.observation_receipt_id is None:
            nodes = self.desired_definition["parts"]["eventstream.json"]["sources"]
            for proposal in self.source_proposals:
                if f"sources/{proposal.node_name}" in self.desired_definition.get("component_ids", {}) or any(
                    node["name"] == proposal.node_name and node.get("id") is not None for node in nodes
                ):
                    raise ValueError("An unresolved proposal cannot introduce a physical ID without its original observation receipt")
        return self

    @model_serializer(mode="wrap")
    def preserve_original_request_shape(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        value = handler(self)
        # Original operation fingerprints predate this optional recovery intent.
        # Omit only its empty default; an explicit selection must remain bound.
        if not self.source_removal_supersessions:
            value.pop("source_removal_supersessions", None)
        return value


class ConnectorPublicationPlan(MonitoringModel):
    """Immutable desired intent and its exact producer, work and observation bindings."""

    connector_id: CanonicalId
    ownership_id: CanonicalId
    work_id: CanonicalId
    lease_owner_id: CanonicalId
    lease_fence: PositiveRevision
    expected_work_revision: PositiveRevision
    expected_connector_revision: Revision
    policy_revision: Revision
    producer_request_id: CanonicalId
    producer_fingerprint: Fingerprint
    frontier_key: StateKey
    frontier_revision: PositiveRevision
    name: Label
    sources: Annotated[tuple[ConnectorSource, ...], Field(max_length=1_000)]
    source_proposals: Annotated[tuple[ConnectorSourceProposal, ...], Field(max_length=1_000)] = ()
    source_removals: Annotated[tuple[SourceRemovalIntent, ...], Field(max_length=1_000)] = ()
    source_removal_supersessions: Annotated[tuple[SourceRemovalSupersession, ...], Field(max_length=1_000)] = ()
    desired_definition: JsonObject
    observation_receipt_id: CanonicalId | None = None
    readiness_receipt_id: CanonicalId | None = None
    detail: Detail


class ConnectorPublicationResult(MonitoringModel):
    connector_id: CanonicalId
    connector: OwnedConnectorManifest
    state: Literal["planned", "provisioning", "ready", "degraded", "blocked", "deleting", "deleted"]
    desired_changed: StrictBool
    pending_removals: Annotated[tuple[PendingSourceRemoval, ...], Field(max_length=1_000)]
    retired_sources: Annotated[tuple[ConnectorSourceRetirement, ...], Field(max_length=1_000)]
    observation_receipt_id: CanonicalId | None
    superseded_source_removals: Annotated[tuple[PendingSourceRemoval, ...], Field(max_length=1_000)] = ()

    @model_validator(mode="after")
    def validate_result(self) -> ConnectorPublicationResult:
        if self.connector_id != self.connector.connector_id or self.state != self.connector.state:
            raise ValueError("Connector publication result identifies another connector or state")
        if self.desired_changed and self.state == "ready":
            raise ValueError("Changed desired scope cannot retain earlier readiness proof")
        if self.pending_removals != self.connector.source_removals:
            raise ValueError("Publication result must expose the exact retained pending removals")
        _unique(tuple(retirement.removal_id for retirement in self.retired_sources), "Retirement IDs")
        for retirement in self.retired_sources:
            if (
                retirement.connector_id != self.connector_id or retirement.ownership_id != self.connector.ownership_id
                or retirement.observation_receipt_id != self.observation_receipt_id
            ):
                raise ValueError("Retirement belongs to another connector, owner or observation")
        superseded = self.superseded_source_removals
        _unique(tuple(removal.removal_id for removal in superseded), "Superseded removal IDs")
        _unique(tuple(removal.source_id for removal in superseded), "Superseded physical source IDs")
        if superseded:
            if (
                not self.desired_changed or self.state == "ready" or self.observation_receipt_id is None
                or any(value is not None for value in (
                    self.connector.identity_verified_at, self.connector.delivery_verified_at,
                    self.connector.delivery_proof,
                ))
                or {removal.removal_id for removal in superseded} & {
                    removal.removal_id for removal in (*self.pending_removals, *self.retired_sources)
                }
            ):
                raise ValueError("Supersession changes desired state without readiness or retirement authority")
            sources = {source.source_id: source for source in self.connector.sources}
            for removal in superseded:
                source = sources.get(removal.source_id)
                if source is None or removal.proposal_id is not None or source.target != removal.target:
                    raise ValueError("Supersession must retain the exact original physical source and target")
        return self


CONNECTOR_PENDING_GAP_CODES = frozenset({
    "definition_update_submitted_or_unknown", "definition_update_outcome_unknown", "definition_update_pending",
})

#: How long a source-presence inspection may authorize a removal supersession.
#:
#: Stale presence evidence must never authorize supersession, so the window is
#: deliberately short. It was written as a bare ``300`` in both the Python
#: preparation path and the generated SQL guard, which is two places to change
#: and no way to notice when only one of them moves.
#:
#: Consuming it has to be faster than this: a controller that claims the work
#: after the window can never succeed, however often it retries.
SUPERSESSION_EVIDENCE_TTL_SECONDS = 300


def connector_collection_eligible(observation: OwnedConnectorManifest) -> bool:
    """Only original ready evidence or an explicit gap disposition finishes collection."""
    return (
        observation.state == "ready" and observation.observed_definition is not None
        or observation.state in {"blocked", "degraded"}
        and any(gap.code not in CONNECTOR_PENDING_GAP_CODES for gap in observation.gaps)
    )


class ConnectorPresenceInspection(MonitoringModel):
    """Explicit GET-only evidence for an original source-presence observation."""

    read_only: Literal[True]
    observed_at: UtcDateTime
    definition_hash: SqlPayloadHash
    component_states: Annotated[dict[CanonicalId, Literal["Running"]], Field(min_length=3, max_length=1_002)]

    @field_validator("read_only", mode="before")
    @classmethod
    def validate_read_only(cls, value: object) -> object:
        if value is not True:
            raise ValueError("Presence inspection requires an explicit read-only boolean")
        return value

    @field_validator("component_states", mode="before")
    @classmethod
    def validate_component_identities(cls, value: object) -> object:
        if not isinstance(value, dict) or any(
            not isinstance(key, str) or canonical_id(key) != key for key in value
        ):
            raise ValueError("Presence inspection requires canonical physical component identities")
        return value


class ConnectorObservationResult(MonitoringModel):
    connector_id: CanonicalId
    connector: OwnedConnectorManifest
    observation: OwnedConnectorManifest
    observed_definition_hash: SqlPayloadHash | None
    authority: Literal["observed_not_action_authority"]
    reconcile_work_id: CanonicalId
    frontier_key: StateKey
    frontier_revision: PositiveRevision
    work_id: CanonicalId
    work_owner_id: CanonicalId
    work_fence: PositiveRevision
    work_revision: PositiveRevision
    collection_completion_eligible: StrictBool
    inspection: ConnectorPresenceInspection | None = None

    @model_validator(mode="after")
    def validate_observation(self) -> ConnectorObservationResult:
        if self.connector_id != self.connector.connector_id or self.connector_id != self.observation.connector_id:
            raise ValueError("Connector observation receipt identifies another connector")
        if self.connector.revision != self.observation.revision:
            raise ValueError("Reported and effective connector revisions must match")
        for name in ("tenant_id", "epoch", "ownership_id", "policy_revision", "name", "sources", "source_proposals", "source_removals", "desired_definition",
                     "workspace_id", "eventstream_id", "destination_id", "endpoint"):
            if getattr(self.connector, name) != getattr(self.observation, name):
                raise ValueError("Connector observation cannot change desired or physical identity")
        if self.observed_definition_hash is not None and self.observation.observed_definition is None:
            raise ValueError("An explicit definition hash cannot refer to an absent observation")
        if self.work_id == self.reconcile_work_id:
            raise ValueError("Collection work cannot adopt its controller reconciliation handoff")
        if self.collection_completion_eligible and (
            not connector_collection_eligible(self.observation)
            or self.observation.state == "ready" and self.observed_definition_hash is None
        ):
            raise ValueError("Collection completion requires original ready evidence or an explicit gap disposition")
        if self.inspection is not None:
            observed = self.observation
            components = (observed.observed_definition or {}).get("component_ids")
            if (
                not self.collection_completion_eligible or observed.state != "degraded"
                or observed.observed_definition is None or not isinstance(components, dict)
                or self.inspection.definition_hash != self.observed_definition_hash
                or self.inspection.definition_hash != connector_definition_hash(observed.observed_definition)
                or set(self.inspection.component_states) != set(components.values())
                or self.inspection.observed_at > observed.updated_at
                or any(value is not None for value in (
                    observed.operation_id, observed.identity_verified_at, observed.delivery_verified_at,
                    observed.delivery_proof,
                ))
            ):
                raise ValueError("Presence inspection must bind explicit complete unready observation evidence")
        return self


def validate_connector_definition(
    sources: tuple[ConnectorSource, ...], definition: dict, *, proposals: tuple[ConnectorSourceProposal, ...] = (),
) -> None:
    """Validate the normalized per-item transport shape used by the guarded kernel."""
    parts = definition.get("parts")
    graph = parts.get("eventstream.json") if isinstance(parts, dict) else None
    if not isinstance(graph, dict) or graph.get("operators") != []:
        raise ValueError("Desired connector must contain the normalized operator-free eventstream definition")
    nodes, streams, destinations = (graph.get(key) for key in ("sources", "streams", "destinations"))
    if not all(isinstance(value, list) for value in (nodes, streams, destinations)):
        raise ValueError("Desired connector requires explicit source, stream and destination arrays")
    if len(streams) != 1 or len(destinations) != 1 or len(nodes) != len(sources) + len(proposals):
        raise ValueError("Desired connector must have one stream, one endpoint and its exact source set")
    stream, destination = streams[0], destinations[0]
    if (
        not isinstance(stream, dict) or not isinstance(destination, dict)
        or stream.get("type") != "DefaultStream" or destination.get("type") != "CustomEndpoint"
        or not isinstance(stream.get("name"), str) or not stream["name"].strip()
        or not isinstance(destination.get("name"), str) or not destination["name"].strip()
        or destination.get("inputNodes") != [{"name": stream["name"]}]
    ):
        raise ValueError("Desired routing must retain the one DefaultStream to CustomEndpoint path")
    by_target = {(source.target.workspace_id, source.target.item_id): source for source in (*sources, *proposals)}
    names, targets = [], []
    allowed = {
        "Microsoft.Fabric.JobEvents.ItemJobCreated", "Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
        "Microsoft.Fabric.JobEvents.ItemJobSucceeded", "Microsoft.Fabric.JobEvents.ItemJobFailed",
    }
    for node in nodes:
        if not isinstance(node, dict) or node.get("type") != "FabricJobEvents":
            raise ValueError("Only per-item FabricJobEvents sources may be published")
        props = node.get("properties")
        if not isinstance(props, dict) or props.get("eventScope") != "Item":
            raise ValueError("Each desired event source must have explicit item scope")
        target = (props.get("workspaceId"), props.get("itemId"))
        if any(not isinstance(value, str) or canonical_id(value) != value for value in target):
            raise ValueError("Desired source item/workspace IDs must already be canonical")
        source = by_target.get(target)
        events = props.get("includedEventTypes")
        name = node.get("name")
        if (
            source is None or not isinstance(name, str) or not name.strip()
            or not isinstance(events, list) or any(not isinstance(value, str) for value in events)
            or len(events) != len(set(events)) or set(events) != set(source.event_types)
            or not set(events).issubset(allowed)
            or isinstance(source, ConnectorSourceProposal) and name != source.node_name
        ):
            raise ValueError("Desired event nodes must match exact approved target/event identities")
        names.append(name)
        targets.append(target)
    if len(set(names)) != len(names) or len(set(targets)) != len(targets):
        raise ValueError("Desired source names and item identities must be unique")
    inputs = stream.get("inputNodes")
    if (
        not isinstance(inputs, list) or any(not isinstance(value, dict) or set(value) != {"name"} for value in inputs)
        or any(not isinstance(value["name"], str) for value in inputs)
        or len(inputs) != len(names) or {value["name"] for value in inputs} != set(names)
    ):
        raise ValueError("Every desired source must route exactly once through the owned stream")
    component_ids = definition.get("component_ids", {})
    if not isinstance(component_ids, dict) or any(
        not isinstance(value, str) or not value for value in component_ids.values()
    ):
        raise ValueError("Known physical component bindings must be a mapping of nonblank IDs")


class ConnectorDesiredState(MonitoringModel):
    connector_id: CanonicalId
    ownership_id: CanonicalId
    publication_id: CanonicalId
    policy_revision: Revision
    sources_hash: Fingerprint
    definition_hash: Fingerprint
    published_at: UtcDateTime
    supersession_request_id: CanonicalId | None = None


class SourceRunObservation(MonitoringModel):
    execution: SourceExecutionIdentity
    origin: Literal["poll", "event", "mail", "operator", "deferred_retry", "fixture"]
    authority: Literal["transport", "rest", "fixture"]
    observed_at: UtcDateTime
    started_at: UtcDateTime | None = None
    ended_at: UtcDateTime | None = None
    status: Literal["not_started", "running", "succeeded", "failed", "cancelled", "unknown"]
    invocation: Literal["scheduled", "manual", "unknown"] = "unknown"
    job_type: Label | None = None
    error_code: OpaqueId | None = None
    failure_reason: Detail | None = None
    failure_signature: OpaqueId | None = None
    evidence: JsonObject = Field(default_factory=dict)
    evidence_truncated: StrictBool = False

    @model_validator(mode="after")
    def validate_times(self) -> SourceRunObservation:
        if self.started_at is not None and self.ended_at is not None:
            if self.ended_at < self.started_at:
                raise ValueError("A source execution cannot end before it starts")
        if self.authority == "fixture" and self.origin != "fixture":
            raise ValueError("Fixture authority must be explicitly labelled as a fixture observation")
        return self

    @property
    def key(self) -> str:
        return self.execution.key

    @property
    def failed_scheduled_pipeline(self) -> bool:
        return (
            self.execution.target.workload == "fabric_pipeline"
            and self.authority in {"rest", "fixture"}
            and self.status == "failed" and self.invocation == "scheduled"
            and self.job_type in PIPELINE_JOB_TYPES
            and self.started_at is not None and self.ended_at is not None
        )


class QuarantineDisposition(MonitoringModel):
    observation_id: OpaqueId
    reason: Literal[
        "malformed", "oversized", "wrong_tenant", "unknown_connector", "unsupported",
        "out_of_scope", "before_cutoff", "ambiguous_execution", "unverified_provenance",
    ]
    detail: Detail
    metadata: JsonObject = Field(default_factory=dict)
    replayable: StrictBool = False


class TransportDeliveryIdentity(MonitoringContext):
    connector_id: CanonicalId
    event_source: Annotated[
        str, StringConstraints(strict=True, min_length=1, max_length=2_048),
        AfterValidator(_opaque_text),
    ]
    event_id: OpaqueId

    @property
    def key(self) -> str:
        return f"delivery:v1:{self.epoch}:{self.tenant_id}:{self.connector_id}:" + _digest(
            [self.event_source, self.event_id],
        )


class PartitionIdentity(MonitoringContext):
    connector_id: CanonicalId
    consumer_group: OpaqueId
    partition_id: OpaqueId

    @property
    def key(self) -> str:
        return f"partition:v1:{self.epoch}:{self.tenant_id}:{self.connector_id}:" + _digest(
            [self.consumer_group, self.partition_id],
        )


class StreamPosition(MonitoringModel):
    offset: OpaqueId
    sequence_number: Count
    enqueued_at: UtcDateTime


class SignalReceipt(MonitoringModel):
    delivery: TransportDeliveryIdentity
    partition: PartitionIdentity
    position: StreamPosition
    received_at: UtcDateTime
    event_type: OpaqueId | None = None
    status: Literal["accepted", "quarantined"]
    observation: SourceRunObservation | None = None
    quarantine: QuarantineDisposition | None = None
    transport: EventTransportEvidence | None = None

    @model_validator(mode="after")
    def validate_receipt(self) -> SignalReceipt:
        _same_context(self.delivery, self.partition)
        if self.delivery.connector_id != self.partition.connector_id:
            raise ValueError("Transport delivery and partition use different connectors")
        if self.status == "accepted":
            if self.observation is None or self.quarantine is not None:
                raise ValueError("Accepted receipts require an observation and no quarantine")
            _same_context(self.delivery, self.observation.execution.target)
            if self.observation.origin != "event":
                raise ValueError("A stream receipt must retain event provenance")
        elif self.quarantine is None:
            raise ValueError("Quarantined receipts require a bounded disposition")
        if self.transport is not None and (
            self.transport.endpoint.consumer_group != self.partition.consumer_group
            or self.transport.identity_verified_at > self.received_at
        ):
            raise ValueError("Transport evidence must match the receiving group and original identity time")
        return self


class LeaseToken(MonitoringContext):
    resource_key: StateKey
    owner_id: CanonicalId
    fence: PositiveRevision
    acquired_at: UtcDateTime
    expires_at: UtcDateTime

    @model_validator(mode="after")
    def validate_expiry(self) -> LeaseToken:
        if self.expires_at <= self.acquired_at:
            raise ValueError("Lease expiry must follow acquisition")
        return self


def _lease_for(lease: LeaseToken, context: MonitoringContext, resource_key: str) -> None:
    _same_context(lease, context)
    if lease.resource_key != resource_key:
        raise ValueError("Lease belongs to a different resource")


def work_key(context: MonitoringContext, work_id: str) -> str:
    return f"work:v1:{context.epoch}:{context.tenant_id}:{canonical_id(work_id)}"


class LeaseRenewal(MonitoringModel):
    lease: LeaseToken
    lease_seconds: LeaseSeconds = 120


class MonitoringWorkDraft(MonitoringContext):
    work_id: CanonicalId
    kind: WorkKind
    policy_revision: Revision
    due_at: UtcDateTime
    created_at: UtcDateTime
    reason: Detail
    target: TargetIdentity | None = None
    execution: SourceExecutionIdentity | None = None
    scope_id: CanonicalId | None = None
    discovery_selector: ScopeSelector | None = None
    connector_id: CanonicalId | None = None
    action_reservation_id: CanonicalId | None = None
    reconcile_request_id: CanonicalId | None = None
    reconcile_producer: ProducerComponent | None = None

    @model_validator(mode="after")
    def validate_work(self) -> MonitoringWorkDraft:
        if self.target is not None:
            _same_context(self, self.target)
        if self.discovery_selector is not None:
            if self.kind != "inventory" or self.discovery_selector.tenant_id != self.tenant_id:
                raise ValueError("Inventory selectors belong only to inventory work in this tenant")
        if self.kind == "inventory" and self.discovery_selector is None and self.scope_id is None:
            raise ValueError("Inventory work requires an explicit selector or an existing scope reference")
        if self.execution is not None and self.execution.target != self.target:
            raise ValueError("Work source execution must belong to its target")
        if self.kind in {"capability_probe", "poll", "triage", "deferred_retry", "verify_action", "finalize"}:
            if self.target is None:
                raise ValueError("Target work requires a canonical target")
        if self.kind in {"triage", "deferred_retry", "verify_action", "finalize"}:
            if self.execution is None:
                raise ValueError("Controller work requires an exact source execution")
        if self.kind == "connector_reconcile" and self.connector_id is None:
            raise ValueError("Connector work requires a connector identity")
        if self.kind == "verify_action" and self.action_reservation_id is None:
            raise ValueError("Read-only action verification requires the existing action fence")
        if self.kind == "reconcile_state":
            if self.reconcile_request_id is None or self.reconcile_producer is None:
                raise ValueError("Reconciliation needs its immutable producer request identity")
            if any(value is not None for value in (
                self.execution, self.action_reservation_id, self.scope_id, self.connector_id,
            )):
                raise ValueError("Reconciliation carries only its producer request, never executable lineage")
        elif self.reconcile_request_id is not None or self.reconcile_producer is not None:
            raise ValueError("Producer reconciliation references belong only to reconcile_state work")
        return self

    @property
    def key(self) -> str:
        return work_key(self, self.work_id)


class MonitoringWork(MonitoringWorkDraft):
    revision: Revision
    attempts: Count = 0
    #: Assigned by the store, never by enqueue callers. A prior deferral counts as attempt one.
    retry_attempt: Annotated[int, Field(strict=True, ge=0, le=MAX_ATTEMPTS)] = 0
    retry_of: CanonicalId | None = None
    state: Literal["queued", "leased", "waiting", "finalizing", "completed", "dispositioned"]
    lease: LeaseToken | None = None
    disposition: Detail | None = None
    completed_at: UtcDateTime | None = None
    finalization_id: CanonicalId | None = None

    @model_validator(mode="after")
    def validate_state(self) -> MonitoringWork:
        if self.kind == "reconcile_state" and (
            self.retry_attempt or self.retry_of is not None or self.finalization_id is not None
        ):
            raise ValueError("Reconciliation cannot acquire retry or incident-finalization lineage")
        if self.kind == "deferred_retry" and self.retry_attempt < 1:
            raise ValueError("Deferred work must carry its store-assigned attempt number")
        if self.retry_of is not None and (
            self.retry_attempt < 1 or self.kind not in {"deferred_retry", "verify_action", "finalize"}
        ):
            raise ValueError("A successor retry must retain its bounded deferred-attempt identity")
        if self.lease is not None:
            _lease_for(self.lease, self, self.key)
        if (self.state in {"leased", "finalizing"}) != (self.lease is not None):
            raise ValueError("Only leased/finalizing work carries an active lease")
        terminal = self.state in {"completed", "dispositioned"}
        if terminal != (self.completed_at is not None):
            raise ValueError("Only terminal work carries a completion time")
        if self.state == "dispositioned" and self.disposition is None:
            raise ValueError("Non-executing completion requires an explicit disposition")
        if self.state == "completed" and self.kind in {
            "triage", "deferred_retry", "verify_action", "finalize",
        } and self.finalization_id is None:
            raise ValueError("Controller work cannot finish without durable incident finalization")
        if self.finalization_id is not None and self.state != "completed":
            raise ValueError("A finalization receipt belongs only to completed work")
        return self


class WorkClaimRequest(MonitoringContext):
    owner_id: CanonicalId
    kinds: Annotated[tuple[WorkKind, ...], Field(min_length=1, max_length=9)]
    limit: BatchSize = 20
    per_workspace_limit: BatchSize = 2
    lease_seconds: LeaseSeconds = 120

    @model_validator(mode="after")
    def validate_limits(self) -> WorkClaimRequest:
        _unique(self.kinds, "Work kinds")
        if self.per_workspace_limit > self.limit:
            raise ValueError("Per-workspace share cannot exceed the whole batch")
        return self


class WorkDispositionRequest(MonitoringContext):
    request_id: CanonicalId
    work_id: CanonicalId
    expected_work_revision: Revision
    lease: LeaseToken
    disposition: Literal["retry", "cancelled", "superseded", "out_of_scope", "historical", "unsupported"]
    detail: Detail
    retry_at: UtcDateTime | None = None

    @model_validator(mode="after")
    def validate_disposition(self) -> WorkDispositionRequest:
        _lease_for(self.lease, self, work_key(self, self.work_id))
        if (self.disposition == "retry") != (self.retry_at is not None):
            raise ValueError("Only a retry disposition carries a retry time")
        return self


class ObservationWindow(MonitoringModel):
    start_at: UtcDateTime
    end_at: UtcDateTime

    @model_validator(mode="after")
    def validate_window(self) -> ObservationWindow:
        if self.end_at < self.start_at:
            raise ValueError("Observation window ends before it starts")
        return self


class PowerBIWindowRow(MonitoringModel):
    """A typed, non-executable refresh-history row awaiting whole-window alias validation."""

    observation: SourceRunObservation
    refresh_id: OpaqueId | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> PowerBIWindowRow:
        execution = self.observation.execution
        if execution.target.workload != "powerbi" or self.observation.authority == "transport":
            raise ValueError("Power BI staging requires exact-target REST or explicit fixture evidence")
        if self.refresh_id is not None:
            normalized = SourceExecutionIdentity(
                target=execution.target, run_id_kind="powerbi_refresh", run_id=self.refresh_id,
            ).run_id
            object.__setattr__(self, "refresh_id", normalized)
        if execution.run_id_kind == "powerbi_refresh" and self.refresh_id != execution.run_id:
            raise ValueError("A numeric-only staged row must retain its exact refresh ID")
        return self


class PowerBIAliasState(MonitoringModel):
    window_id: CanonicalId
    namespace: Literal["refresh", "request"]
    identifier: OpaqueId
    mapped_ids: Annotated[tuple[OpaqueId, ...], Field(max_length=MAX_POWERBI_WINDOW_ROWS)] = ()

    @model_validator(mode="after")
    def validate_mappings(self) -> PowerBIAliasState:
        if tuple(sorted(set(self.mapped_ids))) != self.mapped_ids:
            raise ValueError("Alias mappings must be sorted and distinct")
        requests = self.mapped_ids if self.namespace == "refresh" else (self.identifier,)
        refreshes = (self.identifier,) if self.namespace == "refresh" else self.mapped_ids
        if any(canonical_id(value) != value for value in requests):
            raise ValueError("Stored request aliases must be canonical nonempty GUIDs")
        if any(
            not value.isascii() or not value.isdecimal() or int(value) <= 0 or str(int(value)) != value
            for value in refreshes
        ):
            raise ValueError("Stored refresh aliases must be canonical positive decimal IDs")
        return self


class PowerBIWindowState(MonitoringModel):
    window_id: CanonicalId
    target: TargetIdentity
    poll_work_id: CanonicalId
    window: ObservationWindow
    revision: PositiveRevision
    state: Literal["collecting", "validated", "quarantined"]
    row_count: Annotated[int, Field(strict=True, ge=0, le=MAX_POWERBI_WINDOW_ROWS)]
    quarantined_count: Count = 0
    gaps: Annotated[tuple[CoverageGap, ...], Field(max_length=200)] = ()
    updated_at: UtcDateTime

    @model_validator(mode="after")
    def validate_state(self) -> PowerBIWindowState:
        if self.target.workload != "powerbi":
            raise ValueError("Power BI staging cannot represent another workload")
        if self.state == "validated" and self.quarantined_count:
            raise ValueError("A validated alias window cannot contain quarantined identities")
        if self.state == "quarantined" and (not self.quarantined_count or not self.gaps):
            raise ValueError("A quarantined alias window requires explicit counts and coverage gaps")
        return self


class RestPageRequest(MonitoringModel):
    page_id: CanonicalId
    target: TargetIdentity
    policy_revision: Revision
    poll_work_id: CanonicalId
    lease: LeaseToken
    expected_checkpoint_revision: Revision
    window: ObservationWindow
    expected_cursor: Cursor | None = None
    next_cursor: Cursor | None = None
    received_count: Annotated[int, Field(strict=True, ge=0, le=MAX_INTAKE_BATCH)]
    observations: Annotated[tuple[SourceRunObservation, ...], Field(max_length=MAX_INTAKE_BATCH)] = ()
    powerbi_rows: Annotated[tuple[PowerBIWindowRow, ...], Field(max_length=MAX_INTAKE_BATCH)] = ()
    powerbi_window_complete: StrictBool = False
    quarantines: Annotated[tuple[QuarantineDisposition, ...], Field(max_length=MAX_INTAKE_BATCH)] = ()
    window_complete: StrictBool = False
    retention_exhausted: StrictBool = False
    observed_at: UtcDateTime

    @model_validator(mode="after")
    def validate_page(self) -> RestPageRequest:
        _lease_for(self.lease, self.target, work_key(self.target, self.poll_work_id))
        if self.received_count != len(self.observations) + len(self.powerbi_rows) + len(self.quarantines):
            raise ValueError("Every received REST observation needs a durable acceptance/disposition")
        if self.target.workload == "powerbi":
            if self.observations:
                raise ValueError("Power BI poll observations must use typed window staging before admission")
            if self.powerbi_window_complete != (self.next_cursor is None):
                raise ValueError("Power BI source-window completion must match its final continuation boundary")
        elif self.powerbi_rows or self.powerbi_window_complete:
            raise ValueError("Power BI alias staging cannot be attached to another workload")
        if any(row.observation.execution.target != self.target for row in self.powerbi_rows):
            raise ValueError("Staged Power BI rows belong to a different target")
        if any(row.observation.origin not in {"poll", "fixture"} for row in self.powerbi_rows):
            raise ValueError("Staged Power BI rows must retain polling or explicit fixture provenance")
        if any(observation.execution.target != self.target for observation in self.observations):
            raise ValueError("REST page contains a different target")
        if any(observation.origin not in {"poll", "fixture"} for observation in self.observations):
            raise ValueError("REST pages must retain polling or explicit fixture provenance")
        if any(observation.authority == "transport" for observation in self.observations):
            raise ValueError("A REST page cannot claim unverified transport evidence as a REST read")
        if self.window_complete and (self.next_cursor is not None or self.retention_exhausted):
            raise ValueError("A continuation or retention gap cannot establish complete coverage")
        if self.next_cursor is not None and self.next_cursor == self.expected_cursor:
            raise ValueError("A REST page cannot advance to its own prior cursor")
        return self


class RestCheckpoint(MonitoringModel):
    target: TargetIdentity
    revision: Revision
    window: ObservationWindow
    cursor: Cursor | None = None
    coverage_through: UtcDateTime | None = None
    last_page_id: CanonicalId
    updated_at: UtcDateTime
    powerbi_window_id: CanonicalId | None = None

    @model_validator(mode="after")
    def validate_coverage(self) -> RestCheckpoint:
        if self.coverage_through is not None and self.coverage_through > self.window.end_at:
            raise ValueError("Coverage cannot advance beyond the observed window")
        return self


class IntakeReceipt(MonitoringContext):
    request_id: CanonicalId
    recorded_at: UtcDateTime
    receipt_keys: Annotated[tuple[StateKey, ...], Field(max_length=MAX_INTAKE_BATCH)]
    work_ids: Annotated[tuple[CanonicalId, ...], Field(max_length=MAX_POWERBI_WINDOW_ROWS)]
    replayed: StrictBool = False
    publication_status: Literal["pending_validation", "published"] = "published"

    @model_validator(mode="after")
    def validate_ids(self) -> IntakeReceipt:
        _unique(self.work_ids, "Accepted work IDs")
        return self


class RestPageReceipt(MonitoringModel):
    intake: IntakeReceipt
    checkpoint: RestCheckpoint
    powerbi_window: PowerBIWindowState | None = None

    @model_validator(mode="after")
    def validate_page(self) -> RestPageReceipt:
        _same_context(self.intake, self.checkpoint.target)
        if self.intake.request_id != self.checkpoint.last_page_id:
            raise ValueError("REST acceptance and checkpoint must name the same durable page")
        if self.powerbi_window is not None and (
            self.powerbi_window.window_id != self.checkpoint.powerbi_window_id
            or self.powerbi_window.target != self.checkpoint.target
            or self.powerbi_window.window != self.checkpoint.window
        ):
            raise ValueError("REST receipt and Power BI staging window disagree")
        return self


class StreamReceiptBatch(MonitoringModel):
    request_id: CanonicalId
    partition: PartitionIdentity
    lease: LeaseToken
    receipts: Annotated[tuple[SignalReceipt, ...], Field(min_length=1, max_length=MAX_INTAKE_BATCH)]

    @model_validator(mode="after")
    def validate_partition(self) -> StreamReceiptBatch:
        _lease_for(self.lease, self.partition, self.partition.key)
        positions = []
        for receipt in self.receipts:
            if receipt.partition != self.partition:
                raise ValueError("Stream batch contains a different partition")
            if receipt.transport is not None and receipt.transport.request_id != self.request_id:
                raise ValueError("Transport evidence belongs to another original intake request")
            positions.append(receipt.position.sequence_number)
        if positions != sorted(set(positions)):
            raise ValueError("Stream positions must be distinct and ordered")
        return self


class StreamCheckpoint(MonitoringModel):
    partition: PartitionIdentity
    position: StreamPosition
    revision: Revision
    updated_at: UtcDateTime


class StreamCheckpointAdvance(MonitoringModel):
    request_id: CanonicalId
    partition: PartitionIdentity
    lease: LeaseToken
    expected_revision: Revision
    through: StreamPosition

    @model_validator(mode="after")
    def validate_lease(self) -> StreamCheckpointAdvance:
        _lease_for(self.lease, self.partition, self.partition.key)
        return self


class PartitionClaimRequest(MonitoringModel):
    partition: PartitionIdentity
    owner_id: CanonicalId
    lease_seconds: LeaseSeconds = 120
    initial_sequence_number: Count = 0


class PartitionOwnershipResult(MonitoringModel):
    partition_key: StateKey
    partition: PartitionIdentity
    lease: LeaseToken | None
    last_owner_id: CanonicalId
    last_fence: PositiveRevision
    ownership_revision: PositiveRevision
    etag: OpaqueId
    modified_at: UtcDateTime

    @model_validator(mode="after")
    def validate_partition(self) -> PartitionOwnershipResult:
        if self.partition.key != self.partition_key:
            raise ValueError("Native ownership identity disagrees with its partition key")
        if self.lease is not None:
            _lease_for(self.lease, self.partition, self.partition_key)
            if (self.lease.owner_id, self.lease.fence) != (self.last_owner_id, self.last_fence):
                raise ValueError("Active ownership must retain its actual native owner/fence")
        if self.etag != f"ownership:{self.ownership_revision}:{self.last_fence}":
            raise ValueError("Native ownership ETag must bind its committed revision/fence")
        return self


class PartitionStartResult(MonitoringModel):
    partition_key: StateKey
    partition: PartitionIdentity
    first_sequence_number: Count
    ownership_revision: PositiveRevision
    start: StreamStartRecord
    last_owner_id: CanonicalId
    last_fence: PositiveRevision

    @model_validator(mode="after")
    def validate_start(self) -> PartitionStartResult:
        if self.partition.key != self.partition_key or self.start.partition != self.partition or (
            self.start.first_sequence_number != self.first_sequence_number
        ):
            raise ValueError("Native start result must retain its exact partition and original boundary")
        return self


class StreamStartRecord(MonitoringModel):
    partition_key: StateKey
    partition: PartitionIdentity
    first_sequence_number: Count
    broker_observed_at: UtcDateTime
    recorded_at: UtcDateTime
    history_before_start: Literal["unobserved"]
    gaps: tuple[CoverageGap, ...]

    @model_validator(mode="after")
    def validate_start(self) -> StreamStartRecord:
        if self.partition.key != self.partition_key or self.broker_observed_at > self.recorded_at:
            raise ValueError("Pinned broker start must retain its exact identity and observed/recorded chronology")
        if not any(gap.code == "unobserved_stream_history" for gap in self.gaps):
            raise ValueError("Pinned broker history before the boundary remains explicitly unobserved")
        return self


class StreamGapRecord(MonitoringModel):
    partition: PartitionIdentity
    pinned_start: Count
    expected_sequence: Count
    first_available_sequence_number: Count
    missing_from: Count | None
    missing_through: Count | None
    checkpoint_revision: Revision
    owner_id: CanonicalId
    fence: PositiveRevision
    observed_at: UtcDateTime
    recorded_at: UtcDateTime
    gap: CoverageGap

    @model_validator(mode="after")
    def validate_range(self) -> StreamGapRecord:
        if self.observed_at > self.recorded_at:
            raise ValueError("A retention observation cannot postdate its durable recording")
        if self.gap.code == "stream_retention_gap":
            if self.first_available_sequence_number <= self.expected_sequence or (
                self.missing_from != self.expected_sequence
                or self.missing_through != self.first_available_sequence_number - 1
            ):
                raise ValueError("Retention evidence must identify the exact missing sequence range")
        elif self.gap.code == "stream_boundary_regressed":
            if self.first_available_sequence_number >= self.pinned_start or (
                self.missing_from is not None or self.missing_through is not None
            ):
                raise ValueError("Regressed broker history is not a forward retention range")
        else:
            raise ValueError("Unknown native stream-history disposition")
        return self


class StreamRetentionResult(MonitoringModel):
    partition: PartitionIdentity
    start: StreamStartRecord
    checkpoint: StreamCheckpointRecord | None
    observation: StreamGapRecord | None
    state: Literal["no_new_gap", "gap_recorded"]

    @model_validator(mode="after")
    def validate_history(self) -> StreamRetentionResult:
        if self.start.partition != self.partition or (
            self.checkpoint is not None and self.checkpoint.partition != self.partition
        ) or (self.state == "gap_recorded") != (self.observation is not None):
            raise ValueError("Retention result must bind its original partition/start/checkpoint")
        if self.observation is not None and (
            self.observation.partition != self.partition
            or self.observation.pinned_start != self.start.first_sequence_number
            or self.observation.checkpoint_revision != (self.checkpoint.revision if self.checkpoint else 0)
            or self.observation.gap not in self.start.gaps
        ):
            raise ValueError("Retention gap must be present in its exact pinned history")
        return self


class StreamPositionRecord(MonitoringModel):
    offset: Annotated[OpaqueId, Field(max_length=256)]
    enqueued_at: UtcDateTime
    receipt_key: StateKey
    receipt_kind: Literal["identified", "unidentified"]
    payload_hash: Fingerprint
    batch_id: CanonicalId


class StreamAcceptanceResult(MonitoringModel):
    batch_id: CanonicalId
    partition_key: StateKey
    partition: PartitionIdentity
    position_count: Annotated[int, Field(strict=True, ge=1, le=MAX_INTAKE_BATCH)]
    reconcile_work_id: CanonicalId
    frontier_key: StateKey
    frontier_revision: PositiveRevision
    state: Literal["accepted_for_reconciliation"]
    positions: Annotated[tuple[StreamAcceptedPosition, ...], Field(min_length=1, max_length=MAX_INTAKE_BATCH)]
    receipt_keys: Annotated[tuple[StateKey, ...], Field(min_length=1, max_length=MAX_INTAKE_BATCH)]

    @model_validator(mode="after")
    def validate_positions(self) -> StreamAcceptanceResult:
        sequences = [position.sequence_number for position in self.positions]
        if self.partition.key != self.partition_key or self.position_count != len(self.positions) or (
            sequences != sorted(set(sequences))
            or self.receipt_keys != tuple(position.receipt_key for position in self.positions)
        ):
            raise ValueError("Original stream acceptance must retain every ordered broker position and receipt key")
        return self


class StreamAcceptedPosition(MonitoringModel):
    sequence_number: Count
    offset: Annotated[OpaqueId, Field(max_length=256)]
    enqueued_at: UtcDateTime
    receipt_kind: Literal["identified", "unidentified"]
    receipt_key: StateKey
    first_committed_batch_id: CanonicalId
    original_payload_hash: Fingerprint


class StreamCheckpointRecord(MonitoringModel):
    partition_key: StateKey
    partition: PartitionIdentity
    position: StreamPosition
    revision: PositiveRevision
    sequence_number: Count
    offset: Annotated[OpaqueId, Field(max_length=256)]
    updated_at: UtcDateTime

    @model_validator(mode="after")
    def validate_position(self) -> StreamCheckpointRecord:
        if self.partition.key != self.partition_key or (
            self.position.sequence_number != self.sequence_number or self.position.offset != self.offset
        ):
            raise ValueError("Checkpoint must retain its exact partition and original broker position")
        return self


def _validate_configuration(
    action: ConfigurationAction, values: dict[str, JsonValue], *, intent: bool,
) -> None:
    if action == "rebind_dataset_gateway":
        if set(values) != {"gateway_id", "datasource_ids"}:
            raise ValueError("Gateway configuration requires only gateway_id and datasource_ids")
        gateway = values["gateway_id"]
        datasources = values["datasource_ids"]
        if not isinstance(gateway, str) or not isinstance(datasources, list) or not datasources:
            raise ValueError("Gateway configuration requires explicit nonempty binding identities")
        if canonical_id(gateway) != gateway:
            raise ValueError("Gateway configuration IDs must already be canonical")
        if any(not isinstance(value, str) or canonical_id(value) != value for value in datasources):
            raise ValueError("Datasource configuration IDs must already be canonical")
        if datasources != sorted(set(datasources)):
            raise ValueError("Datasource configuration IDs must be sorted and distinct")
    else:
        if set(values) != {"enabled"} or type(values["enabled"]) is not bool:
            raise ValueError("Schedule configuration must contain exactly one boolean enabled field")
        if intent and values["enabled"] is not True:
            raise ValueError("Schedule re-enablement review must be exactly {'enabled': true}")


class ConfigurationVerification(MonitoringModel):
    """An exact REST configuration snapshot for an existing non-job remediation."""

    target: TargetIdentity
    action: ConfigurationAction
    expected_hash: Fingerprint
    configuration: Parameters
    observed_at: UtcDateTime
    authority: Literal["rest", "fixture"]

    @model_validator(mode="after")
    def validate_configuration(self) -> ConfigurationVerification:
        _action_workload(self.action, self.target.workload)
        _validate_configuration(self.action, self.configuration, intent=False)
        return self

    @property
    def matches(self) -> bool:
        return _digest(self.configuration) == self.expected_hash


class SafetyReview(MonitoringModel):
    review_id: CanonicalId
    target: TargetIdentity
    revision: PositiveRevision
    policy_revision: Revision
    action: ActionKind
    state: Literal["pending", "verified", "revoked", "unverifiable"]
    reviewer_id: CanonicalId
    reviewed_at: UtcDateTime
    expires_at: UtcDateTime
    revoked_at: UtcDateTime | None = None
    definition_hash: Fingerprint | None = None
    configuration_hash: Fingerprint | None = None
    parameters: Parameters | None = None
    parameter_hash: Fingerprint | None = None
    parameters_redacted: StrictBool = False
    replay_safe: StrictBool = False
    exact_correlation_verified: StrictBool = False
    detail: Detail
    requested_state: Literal["pending", "verified", "revoked", "unverifiable"] | None = None
    publication_status: Literal["pending_validation", "published"] = "published"

    @model_validator(mode="after")
    def validate_review(self) -> SafetyReview:
        if self.publication_status == "pending_validation" and self.state != "pending":
            raise ValueError("Accepted review intent is pending, not published technical verification")
        if self.publication_status == "pending_validation" and (
            self.requested_state is None or self.exact_correlation_verified
        ):
            raise ValueError("Pending review intent retains requested state, not technical correlation proof")
        _action_workload(self.action, self.target.workload)
        if self.parameters_redacted:
            if self.parameters is not None or self.parameter_hash is None:
                raise ValueError("Redacted replay parameters retain only their original fingerprint")
            if self.state == "verified":
                raise ValueError("A redacted parameter set is unavailable for verified replay")
        else:
            expected_hash = _digest(self.parameters)
            if self.parameter_hash is not None and self.parameter_hash != expected_hash:
                raise ValueError("Replay parameters disagree with their reviewed fingerprint")
            object.__setattr__(self, "parameter_hash", expected_hash)
        if self.action in {"rebind_dataset_gateway", "reenable_refresh_schedule"} and self.parameters is not None:
            _validate_configuration(self.action, self.parameters, intent=True)
        revocation = self.state == "revoked" or (
            self.state == "pending" and self.publication_status == "pending_validation"
            and self.requested_state == "revoked"
        )
        if self.expires_at <= self.reviewed_at and not revocation:
            raise ValueError("Safety review must expire after it is made")
        if (self.state == "revoked") != (self.revoked_at is not None):
            raise ValueError("Revoked reviews require a revocation timestamp")
        if self.revoked_at is not None and self.revoked_at < self.reviewed_at:
            raise ValueError("Revocation cannot precede the review")
        if self.state == "verified":
            if self.action in {"pipeline_rerun", "powerbi_refresh"} and not self.exact_correlation_verified:
                raise ValueError("Verified review requires exact action-correlation proof")
            if self.action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}:
                if self.configuration_hash is None or self.parameters is None:
                    raise ValueError("Non-job review requires its exact desired configuration")
                if self.parameter_hash != self.configuration_hash:
                    raise ValueError("Desired configuration disagrees with the reviewed fingerprint")
            if self.action == "pipeline_rerun" and (
                not self.replay_safe or self.parameters is None or self.definition_hash is None
            ):
                raise ValueError("Pipeline replay needs reviewed safety, parameters and definition")
        return self


class SafetyReviewRequest(MonitoringModel):
    request_id: CanonicalId
    expected: RegistryVersion
    expected_review_revision: Revision
    review: SafetyReview

    @model_validator(mode="after")
    def validate_revision(self) -> SafetyReviewRequest:
        _same_context(self.expected, self.review.target)
        if self.review.policy_revision != self.expected.revision:
            raise ValueError("Safety review must bind to the expected current policy")
        if self.review.revision != self.expected_review_revision + 1:
            raise ValueError("Safety-review mutation must advance its revision exactly once")
        if (
            self.review.publication_status == "published"
            and self.review.requested_state not in {None, self.review.state}
        ):
            raise ValueError("A new safety intent cannot reuse contradictory prior requested-state metadata")
        return self


class SafetyReviewOperationReceipt(MonitoringModel):
    """An immutable committed operation result, not the latest review or action admission."""

    request_id: CanonicalId
    target: TargetIdentity
    action: ActionKind
    expected: RegistryVersion
    expected_review_revision: Revision
    new_review_revision: PositiveRevision
    fingerprint: Fingerprint
    recorded_at: UtcDateTime
    review: SafetyReview
    requested_state: Literal["pending", "verified", "revoked", "unverifiable"] | None = None
    publication_status: Literal["pending_validation", "published"] = "published"

    @model_validator(mode="after")
    def validate_operation(self) -> SafetyReviewOperationReceipt:
        _same_context(self.expected, self.target)
        if self.target != self.review.target or self.action != self.review.action:
            raise ValueError("Safety-review operation target/action disagree with its committed review")
        if (
            self.new_review_revision != self.expected_review_revision + 1
            or self.review.revision != self.new_review_revision
            or self.review.policy_revision != self.expected.revision
        ):
            raise ValueError("Safety-review operation revisions disagree with its committed review")
        if (
            self.requested_state != self.review.requested_state
            or self.publication_status != self.review.publication_status
        ):
            raise ValueError("Safety-review operation must retain the original publication state")
        return self


class ApprovalReference(MonitoringModel):
    """A reference to authoritative approval state, not a caller-supplied yes."""

    approval_id: Annotated[OpaqueId, Field(max_length=200)]
    fingerprint: OpaqueId


class ActionReservationRequest(MonitoringModel):
    idempotency_id: CanonicalId
    expected: RegistryVersion
    work_id: CanonicalId
    lease: LeaseToken
    source_execution: SourceExecutionIdentity
    incident: IncidentIdentity
    expected_incident_revision: Revision
    action: ActionKind
    review_id: CanonicalId
    expected_review_revision: PositiveRevision
    definition_hash: Fingerprint | None = None
    configuration_hash: Fingerprint | None = None
    parameter_hash: Fingerprint
    arguments: Parameters = Field(default_factory=dict)
    approval: ApprovalReference | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> ActionReservationRequest:
        _same_context(self.expected, self.source_execution.target)
        _lease_for(self.lease, self.expected, work_key(self.expected, self.work_id))
        if self.source_execution.target != self.incident.target:
            raise ValueError("Source execution and incident belong to different targets")
        _action_workload(self.action, self.incident.target.workload)
        if self.action == "pipeline_rerun" and self.definition_hash is None:
            raise ValueError("Pipeline reservations require the reviewed definition fingerprint")
        if self.action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}:
            if self.configuration_hash is None:
                raise ValueError("Non-job reservations require a reviewed configuration fingerprint")
            if self.configuration_hash != self.parameter_hash:
                raise ValueError("Non-job reservation configuration must match the reviewed parameter hash")
        return self


class ApprovalBinding(MonitoringModel):
    """Immutable monitoring provenance recorded before publishing an approval."""

    reference: ApprovalReference
    expected: RegistryVersion
    work_id: CanonicalId
    source_execution: SourceExecutionIdentity
    incident: IncidentIdentity
    action: ActionKind
    review_id: CanonicalId
    review_revision: PositiveRevision
    definition_hash: Fingerprint | None
    configuration_hash: Fingerprint | None
    parameter_hash: Fingerprint
    arguments_hash: Fingerprint
    created_at: UtcDateTime
    expires_at: UtcDateTime

    @model_validator(mode="after")
    def validate_binding(self) -> ApprovalBinding:
        _same_context(self.expected, self.source_execution.target)
        if self.incident.target != self.source_execution.target or self.expires_at <= self.created_at:
            raise ValueError("Approval binding requires one target and a future approval expiry")
        _action_workload(self.action, self.source_execution.target.workload)
        return self


class ActionRejectionEvidence(MonitoringModel):
    """A definitive no-effect response, not an inference from a timeout or polling."""

    reason: Literal["throttled", "definitive_client_error"]
    attempted_at: UtcDateTime
    rejected_at: UtcDateTime
    retry_after_seconds: Annotated[int, Field(strict=True, ge=0, le=2_147_483_647)] = 0

    @model_validator(mode="after")
    def validate_response(self) -> ActionRejectionEvidence:
        if self.rejected_at < self.attempted_at:
            raise ValueError("Rejection cannot precede the submission attempt")
        return self


class ActionRejectionRequest(MonitoringContext):
    request_id: CanonicalId
    reservation_id: CanonicalId
    expected_reservation_revision: PositiveRevision
    action_fence: PositiveRevision
    work_id: CanonicalId
    lease: LeaseToken
    evidence: ActionRejectionEvidence
    detail: Detail

    @model_validator(mode="after")
    def validate_owner(self) -> ActionRejectionRequest:
        _lease_for(self.lease, self, work_key(self, self.work_id))
        return self


class ActionReservation(MonitoringModel):
    """The reservation fences the source failure, not just the submitted action ID."""

    reservation_id: CanonicalId
    request: ActionReservationRequest
    revision: PositiveRevision
    fence: PositiveRevision
    state: ActionState
    reserved_at: UtcDateTime
    updated_at: UtcDateTime
    submitted_execution: SourceExecutionIdentity | None = None
    submitted_at: UtcDateTime | None = None
    configuration: ConfigurationVerification | None = None
    next_verification_at: UtcDateTime | None = None
    rejection: ActionRejectionEvidence | None = None
    retry_attempt: Annotated[int, Field(strict=True, ge=0, le=MAX_ATTEMPTS)] = 0
    retry_of: CanonicalId | None = None
    retry_work_id: CanonicalId | None = None
    retry_reservation_id: CanonicalId | None = None
    detail: Detail

    @model_validator(mode="after")
    def validate_submission(self) -> ActionReservation:
        if (self.state == "rejected") != (self.rejection is not None):
            raise ValueError("Only a confirmed rejected action carries no-effect evidence")
        if self.state == "rejected" and (
            self.submitted_execution is not None or self.submitted_at is not None
            or self.configuration is not None or self.next_verification_at is not None
        ):
            raise ValueError("A rejected action has no submitted execution, configuration effect or verification poll")
        if self.rejection is not None and self.rejection.attempted_at < self.reserved_at:
            raise ValueError("The rejected attempt must follow its durable reservation")
        if self.retry_of is not None and self.retry_attempt < 1:
            raise ValueError("A successor reservation must retain its deferred-attempt number")
        if self.retry_work_id is not None and (
            self.state != "rejected" or self.request.action != "powerbi_refresh"
            or self.rejection.reason != "throttled" or self.retry_attempt >= MAX_ATTEMPTS
        ):
            raise ValueError("Only a bounded throttled refresh rejection can create a successor")
        if self.retry_reservation_id is not None and self.retry_work_id is None:
            raise ValueError("A successor reservation must belong to the recorded successor work")
        if self.updated_at < self.reserved_at:
            raise ValueError("Action update cannot precede its reservation")
        if self.submitted_execution is not None:
            if self.submitted_execution.target != self.request.source_execution.target:
                raise ValueError("Submitted action belongs to a different target")
            if self.submitted_execution == self.request.source_execution:
                raise ValueError("The source failure is not the controller's submitted action")
        if self.state in {"submitted", "verified_succeeded", "verified_failed"}:
            if self.submitted_at is None:
                raise ValueError("Submitted actions require a submission time")
            if self.request.action in {"pipeline_rerun", "powerbi_refresh"} and self.submitted_execution is None:
                raise ValueError("Submitted/verified actions require exact submission identity and time")
        if self.request.action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}:
            if self.submitted_execution is not None:
                raise ValueError("Configuration mutations do not submit workload jobs")
            if self.state.startswith("verified_") and self.configuration is None:
                raise ValueError("Verified non-job mutations require configuration evidence")
        if self.configuration is not None:
            if (
                self.configuration.target != self.request.source_execution.target
                or self.configuration.action != self.request.action
                or self.configuration.expected_hash != self.request.parameter_hash
            ):
                raise ValueError("Configuration evidence must match the reserved action")
        if self.state == "reserved" and (
            self.submitted_execution is not None or self.submitted_at is not None
        ):
            raise ValueError("A reserved action has not yet recorded a submission")
        if self.submitted_at is not None and self.submitted_at < self.reserved_at:
            raise ValueError("Submission cannot precede the durable reservation")
        if self.state in {"submitted", "uncertain"} and self.next_verification_at is None:
            raise ValueError("Submitted or uncertain actions require durable verification scheduling")
        return self


class ActionReservationDecision(MonitoringModel):
    status: Literal["reserved", "replayed", "denied"]
    reservation: ActionReservation | None = None
    denial: Literal[
        "maintenance", "stale_epoch", "stale_policy", "out_of_scope", "observation_only",
        "stale_review", "expired_review", "unverifiable_definition", "approval_required",
        "approval_denied", "approval_expired", "approval_used", "fingerprint_mismatch",
        "lease_lost", "target_owned", "budget_exhausted", "before_cutoff", "source_ineligible",
        "policy_blocked", "pending_validation",
    ] | None = None
    detail: Detail

    @model_validator(mode="after")
    def validate_decision(self) -> ActionReservationDecision:
        if self.status == "denied":
            if self.denial is None or self.reservation is not None:
                raise ValueError("Denied reservations require a reason and cannot carry an action fence")
        elif self.reservation is None or self.denial is not None:
            raise ValueError("Successful reservation decisions require the durable reservation")
        return self


class ActionTransitionResult(MonitoringModel):
    """The original guarded transition result, including its SQL-created successor."""

    reservation_id: CanonicalId
    reservation: ActionReservation
    retry_work: MonitoringWork | None

    @model_validator(mode="after")
    def validate_successor(self) -> ActionTransitionResult:
        action = self.reservation
        if self.reservation_id != action.reservation_id:
            raise ValueError("Action transition result identifies another reservation")
        if self.retry_work is None:
            if action.state == "rejected" and action.retry_work_id is not None:
                raise ValueError("A linked rejection result must include its original successor")
            return self
        work = self.retry_work
        if (
            action.state != "rejected" or action.request.action != "powerbi_refresh"
            or action.rejection.reason != "throttled" or work.kind != "deferred_retry"
            or work.work_id != action.retry_work_id or work.retry_of != action.reservation_id
            or work.execution != action.request.source_execution or work.target != action.request.source_execution.target
            or work.retry_attempt != action.retry_attempt + 1 or work.state != "queued"
            or work.lease is not None or work.action_reservation_id is not None
        ):
            raise ValueError("Returned retry work is not the single initial linked successor")
        return self


class ReservationValidation(MonitoringModel):
    verified: StrictBool = True
    policy_revision: Revision
    work_fence: PositiveRevision
    source_key: StateKey
    parameter_hash: Fingerprint
    review_id: CanonicalId
    review_revision: PositiveRevision
    expires_at: UtcDateTime
    frontier_digest: Fingerprint
    exact_action_correlation: StrictBool = False
    definition_hash: Fingerprint | None = None
    configuration_hash: Fingerprint | None = None

    @model_validator(mode="after")
    def require_verified(self) -> ReservationValidation:
        if not self.verified:
            raise ValueError("A reservation validation record must contain positive controller verification")
        return self


class ActionSubmissionRequest(MonitoringContext):
    request_id: CanonicalId
    reservation_id: CanonicalId
    expected_reservation_revision: PositiveRevision
    action_fence: PositiveRevision
    state: Literal["submitted", "uncertain"]
    submitted_execution: SourceExecutionIdentity | None = None
    configuration_action: ConfigurationAction | None = None
    correlation: Literal["response_run_id", "request_id_lookup"] | None = None
    submitted_at: UtcDateTime
    next_verification_at: UtcDateTime
    detail: Detail

    @model_validator(mode="after")
    def validate_correlation(self) -> ActionSubmissionRequest:
        if self.submitted_execution is not None:
            _same_context(self, self.submitted_execution.target)
        if (self.submitted_execution is None) != (self.correlation is None):
            raise ValueError("A submission identity needs an exact correlation mechanism")
        if self.configuration_action is not None and (
            self.submitted_execution is not None or self.correlation is not None
        ):
            raise ValueError("Non-job submission cannot fabricate a job identity or correlation")
        if self.state == "submitted" and self.submitted_execution is None and self.configuration_action is None:
            raise ValueError("A POST without exact correlation remains uncertain")
        if self.next_verification_at < self.submitted_at:
            raise ValueError("Verification cannot be due before submission")
        return self


class ActionOutcomeRequest(MonitoringContext):
    request_id: CanonicalId
    reservation_id: CanonicalId
    expected_reservation_revision: PositiveRevision
    action_fence: PositiveRevision
    disposition: Literal["verified_succeeded", "verified_failed", "uncertain"]
    submitted_execution: SourceExecutionIdentity | None = None
    observation: SourceRunObservation | None = None
    configuration: ConfigurationVerification | None = None
    activities: Annotated[tuple[PipelineActivity, ...], Field(max_length=200)] = ()
    activities_complete: StrictBool = False
    observed_at: UtcDateTime
    detail: Detail

    @model_validator(mode="after")
    def validate_exact_outcome(self) -> ActionOutcomeRequest:
        if self.configuration is not None:
            _same_context(self, self.configuration.target)
            if self.submitted_execution is not None or self.observation is not None or self.activities:
                raise ValueError("Configuration and job verification are separate evidence contracts")
            if self.disposition == "verified_succeeded" and (
                not self.configuration.matches
                or (
                    self.configuration.action == "reenable_refresh_schedule"
                    and self.configuration.configuration["enabled"] is not True
                )
            ):
                raise ValueError("Configuration does not prove the requested mutation succeeded")
            if self.disposition == "verified_failed":
                raise ValueError("A configuration snapshot mismatch is unverified, not proof of terminal mutation failure")
            return self
        if self.submitted_execution is not None:
            _same_context(self, self.submitted_execution.target)
        if self.observation is not None:
            if self.observation.execution != self.submitted_execution:
                raise ValueError("Outcome evidence must identify exactly the submitted execution")
        if self.disposition != "uncertain":
            if self.observation is None or self.submitted_execution is None:
                raise ValueError("Verified outcomes require exact action-execution evidence")
            expected_statuses = {"succeeded"} if self.disposition == "verified_succeeded" else {"failed"}
            if (
                self.disposition == "verified_failed"
                and self.submitted_execution.target.workload == "fabric_pipeline"
            ):
                expected_statuses.add("cancelled")
            activity_failure = (
                self.disposition == "verified_failed"
                and self.submitted_execution.target.workload == "fabric_pipeline"
                and self.observation.status == "succeeded" and self.activities_complete
                and any(activity.status == "Failed" for activity in self.activities)
            )
            if (
                self.observation.status not in expected_statuses and not activity_failure
            ) or self.observation.authority == "transport":
                raise ValueError("Verified outcome disagrees with authoritative run evidence")
            if (
                self.observation.started_at is None or self.observation.ended_at is None
                or self.observation.evidence_truncated
            ):
                raise ValueError("Verified action evidence must be terminal and complete")
            if self.submitted_execution.target.workload == "fabric_pipeline":
                # A terminal cancellation is a verified non-success, not evidence
                # that every activity succeeded or that its partial effects are safe.
                if not self.activities_complete:
                    raise ValueError("Pipeline verification requires complete activity evidence")
                if self.disposition == "verified_succeeded" and (
                    not self.activities
                    or any(activity.status not in {"Succeeded", "Skipped"} for activity in self.activities)
                ):
                    raise ValueError("Incomplete or failed activities cannot prove a successful rerun")
        return self


class ActionOutcomeValidation(MonitoringModel):
    reservation_id: CanonicalId
    outcome: Literal["verified_succeeded", "verified_failed"]
    verified: StrictBool = True
    work_fence: PositiveRevision
    expires_at: UtcDateTime
    submitted_execution: SourceExecutionIdentity | None = None
    configuration: ConfigurationVerification | None = None
    request: ActionOutcomeRequest
    commit: CollectionCommit

    @model_validator(mode="after")
    def validate_correlation(self) -> ActionOutcomeValidation:
        _lease_for(self.commit.lease, self.request, work_key(self.request, self.commit.work_id))
        if (
            not self.verified or self.outcome != self.request.disposition
            or self.reservation_id != self.request.reservation_id
            or self.work_fence != self.commit.lease.fence
            or self.submitted_execution != self.request.submitted_execution
            or self.configuration != self.request.configuration
        ):
            raise ValueError("Controller outcome validation must retain its exact request and current work commit")
        return self


class WorkFinalizationRequest(MonitoringContext):
    """Persist the existing Incident payload and processed source before finishing work."""

    finalization_id: CanonicalId
    work_id: CanonicalId
    expected_work_revision: Revision
    lease: LeaseToken
    source_execution: SourceExecutionIdentity
    incident_identity: IncidentIdentity
    incident: Incident
    source_disposition: SourceDisposition
    action_reservation_id: CanonicalId | None = None

    @model_validator(mode="after")
    def validate_finalization(self) -> WorkFinalizationRequest:
        _lease_for(self.lease, self, work_key(self, self.work_id))
        _same_context(self, self.source_execution.target)
        if self.incident_identity.target != self.source_execution.target:
            raise ValueError("Finalization source and incident belong to different targets")
        if self.incident.signature != self.incident_identity.signature:
            raise ValueError("Existing Incident payload must retain the canonical incident signature")
        if self.incident.pipeline_failure is not None:
            failure = self.incident.pipeline_failure
            if (
                self.source_execution.target.workload != "fabric_pipeline"
                or failure.target.workspace_id != self.source_execution.target.workspace_id
                or failure.target.pipeline_id != self.source_execution.target.item_id
            ):
                raise ValueError("Pipeline incident payload belongs to a different target")
            if self.source_disposition == "triaged" and failure.run.id != self.source_execution.run_id:
                raise ValueError("Triaged pipeline evidence must identify the exact source execution")
        return self


class FinalizationReceipt(MonitoringContext):
    finalization_id: CanonicalId
    work_id: CanonicalId
    source_execution: SourceExecutionIdentity
    incident_identity: IncidentIdentity
    source_disposition: SourceDisposition
    incident_payload_hash: Fingerprint
    persisted_at: UtcDateTime
    incident_id: Annotated[OpaqueId, Field(max_length=200)] | None = None
    state: Literal["completed", "persisted_waiting_verification"] = "completed"

    @model_validator(mode="after")
    def validate_identity(self) -> FinalizationReceipt:
        _same_context(self, self.source_execution.target)
        if self.incident_identity.target != self.source_execution.target:
            raise ValueError("Finalization receipt source and incident must share a target")
        return self


class FinalizationPlan(MonitoringModel):
    """Immutable candidate and original payload CAS; SQL derives occurrence/staleness."""

    work_id: CanonicalId
    expected_work_revision: PositiveRevision
    lease_owner_id: CanonicalId
    lease_fence: PositiveRevision
    incident_id: Annotated[OpaqueId, Field(max_length=200)]
    incident_key: StateKey
    incident_identity: IncidentIdentity
    signature: OpaqueId
    source_key: StateKey
    source_execution: SourceExecutionIdentity
    source_started_at: UtcDateTime
    prior_incident_hash: Fingerprint | None
    merged_incident: Incident
    source_disposition: SourceDisposition

    @model_validator(mode="after")
    def validate_identity(self) -> FinalizationPlan:
        if (
            self.incident_key != self.incident_identity.key
            or self.signature != self.incident_identity.signature
            or self.source_key != self.source_execution.key
            or self.source_execution.target != self.incident_identity.target
            or self.merged_incident.id != self.incident_id
            or self.merged_incident.signature != self.signature
        ):
            raise ValueError("Finalization candidate cannot change canonical incident/source identity")
        return self


class FinalizationResult(MonitoringModel):
    work_id: CanonicalId
    finalization_id: CanonicalId
    incident_id: Annotated[OpaqueId, Field(max_length=200)]
    state: Literal["completed", "persisted_waiting_verification"]
    incident: Incident
    source_disposition: SourceDisposition

    @model_validator(mode="after")
    def validate_identity(self) -> FinalizationResult:
        if self.incident.id != self.incident_id:
            raise ValueError("Finalization result identifies another persisted incident")
        return self


class IncidentState(MonitoringModel):
    identity: IncidentIdentity
    incident_id: Annotated[OpaqueId, Field(max_length=200)]
    revision: Revision
    #: Occupied incident slots, not POST count. A bounded linked retry retains its rejected predecessor's slot.
    action_count: Count = 0
    latest_execution: SourceExecutionIdentity | None = None
    latest_started_at: UtcDateTime | None = None
    updated_at: UtcDateTime


class ProcessedSourceRecord(MonitoringModel):
    execution: SourceExecutionIdentity
    disposition: Literal[
        "triaged", "duplicate", "historical", "refused", "failed", "out_of_scope",
        "unsupported", "cancelled", "superseded",
    ]
    recorded_at: UtcDateTime
    work_id: CanonicalId | None = None
    disposition_request_id: CanonicalId | None = None
    finalization_id: CanonicalId | None = None
    incident_identity: IncidentIdentity | None = None
    detail: Detail


class SourcePublicationResult(MonitoringModel):
    source_key: StateKey
    observation: SourceRunObservation

    @model_validator(mode="after")
    def validate_source(self) -> SourcePublicationResult:
        if self.source_key != self.observation.key:
            raise ValueError("Source publication result must retain the exact source identity")
        return self


class SourceDispositionResult(MonitoringModel):
    source_key: StateKey
    disposition: ProcessedSourceRecord
    work: MonitoringWork

    @model_validator(mode="after")
    def validate_source(self) -> SourceDispositionResult:
        if self.source_key != self.disposition.execution.key or (
            self.disposition.disposition_request_id is None
            or self.disposition.work_id is None
            or self.disposition.finalization_id is not None
        ):
            raise ValueError("Non-effect disposition must retain its exact owner, source and original operation")
        _same_context(self.work, self.disposition.execution.target)
        return self


class EvidenceBinding(MonitoringModel):
    kind: OpaqueId
    key: StateKey
    revision: PositiveRevision
    payload_hash: Fingerprint


class ValidationFrontier(MonitoringContext):
    frontier_key: StateKey
    target: TargetIdentity | None = None
    window: ObservationWindow | None = None
    accepted_revision: PositiveRevision
    validated_revision: Revision = 0
    latest_request_id: CanonicalId
    updated_at: UtcDateTime

    @model_validator(mode="after")
    def validate_frontier(self) -> ValidationFrontier:
        if self.target is not None:
            _same_context(self, self.target)
        if self.window is not None and self.target is None:
            raise ValueError("A window validation frontier requires its exact target")
        if self.validated_revision > self.accepted_revision:
            raise ValueError("Validation cannot outrun the accepted raw-intake frontier")
        return self

    @property
    def pending(self) -> bool:
        return self.validated_revision < self.accepted_revision


class ReconciliationRequest(MonitoringContext):
    request_id: CanonicalId
    producer: ProducerComponent
    topic: Literal["inventory", "capability", "scope", "review", "discovery", "rest_page", "stream_intake", "connector"]
    reference_id: StateKey
    fingerprint: Fingerprint
    policy_revision: Revision
    work_id: CanonicalId
    producer_commit: CollectionCommit | None = None
    target: TargetIdentity | None = None
    window: ObservationWindow | None = None
    frontier_key: StateKey
    frontier_revision: PositiveRevision
    created_at: UtcDateTime
    evidence: Annotated[tuple[EvidenceBinding, ...], Field(max_length=MAX_RECONCILIATION_BINDINGS)] = ()
    request_payload: JsonObject

    @model_validator(mode="after")
    def validate_binding(self) -> ReconciliationRequest:
        if self.target is not None:
            _same_context(self, self.target)
        if self.window is not None and self.target is None:
            raise ValueError("Window reconciliation requires an exact target")
        if (self.topic in {"scope", "review", "discovery"}) != (self.producer == "web"):
            raise ValueError("Reconciliation topic belongs to another producer component")
        if self.producer_commit is not None:
            if self.producer != "worker":
                raise ValueError("Human intent cannot carry a worker collection fence")
            _lease_for(
                self.producer_commit.lease, self, work_key(self, self.producer_commit.work_id),
            )
        _unique(tuple(f"{item.kind}:{item.key}" for item in self.evidence), "Evidence bindings")
        return self


class ReconcileStateRequest(MonitoringContext):
    request_id: CanonicalId
    work_id: CanonicalId
    lease: LeaseToken
    expected_work_revision: Revision
    expected_policy_revision: Revision
    expected_frontier_revision: PositiveRevision
    reject_whole_window: StrictBool = False
    detail: Detail | None = None

    @model_validator(mode="after")
    def validate_owner(self) -> ReconcileStateRequest:
        _lease_for(self.lease, self, work_key(self, self.work_id))
        if self.reject_whole_window and self.detail is None:
            raise ValueError("Whole-window rejection requires an explicit controller reason")
        return self


def _validate_window_resolution(
    state: str, scope: str, rejection_id: str | None, resolution_id: str | None, resolution_state: str | None,
) -> None:
    if scope == "window_acknowledgement":
        if state not in {"published", "rejected"} or resolution_id is None or resolution_state != state:
            raise ValueError("A window acknowledgement must retain the exact original terminal resolution")
        if rejection_id != (resolution_id if state == "rejected" else None):
            raise ValueError("Only a rejected-window acknowledgement has a rejection-specific reference")
    elif any(value is not None for value in (rejection_id, resolution_id, resolution_state)):
        raise ValueError("Only a sibling acknowledgement references a prior window resolution")
    if scope == "window" and state != "rejected":
        raise ValueError("Whole-window rejection must retain its rejected outcome")


class ReconciliationResult(MonitoringContext):
    request_id: CanonicalId
    work_id: CanonicalId
    producer_request_id: CanonicalId
    policy_revision: Revision
    frontier_key: StateKey
    frontier_revision: PositiveRevision
    state: Literal["published", "rejected", "pending_validation"]
    detail: Detail
    published_at: UtcDateTime
    resolution_scope: Literal["handoff", "window", "window_acknowledgement", "handoff_acknowledgement"] = "handoff"
    handoff_revision: PositiveRevision | None = None
    handoff_resolution_request_id: CanonicalId | None = None
    handoff_resolution_work_fence: PositiveRevision | None = None
    frontier_resolution_request_id: CanonicalId | None = None
    frontier_resolution_revision: PositiveRevision | None = None
    window_rejection_request_id: CanonicalId | None = None
    window_resolution_request_id: CanonicalId | None = None
    window_resolution_state: Literal["published", "rejected"] | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> ReconciliationResult:
        _validate_window_resolution(
            self.state, self.resolution_scope, self.window_rejection_request_id,
            self.window_resolution_request_id, self.window_resolution_state,
        )
        _validate_handoff_resolution(
            self.state, self.resolution_scope, self.handoff_revision, self.frontier_revision,
            self.handoff_resolution_request_id, self.handoff_resolution_work_fence,
            self.frontier_resolution_request_id, self.frontier_resolution_revision,
        )
        return self


class ConnectorPublicationContext(MonitoringModel):
    """Controller-only inputs collected before reconciliation releases its work lease.

    eligible_targets is supplied only when event-capability evidence narrows the
    current admission set. None lets the provisioner perform its normal bounded
    current-admission enumeration.
    """

    request_id: CanonicalId
    phase: Literal["desired", "binding"]
    expected: RegistryVersion
    work: MonitoringWork
    frontier: ValidationFrontier
    connector: OwnedConnectorManifest
    eligible_targets: Annotated[tuple[MonitoringTarget, ...], Field(max_length=1_000)] | None = None
    removal_targets: Annotated[tuple[TargetIdentity, ...], Field(max_length=1_000)] = ()

    @model_validator(mode="after")
    def validate_publication_owner(self) -> ConnectorPublicationContext:
        for value in (self.work, self.frontier, self.connector):
            _same_context(self.expected, value)
        if (
            self.work.kind != "reconcile_state" or self.work.state != "leased" or self.work.lease is None
            or self.work.policy_revision != self.expected.revision
        ):
            raise ValueError("Connector orchestration requires its current leased reconciliation work")
        if self.phase == "binding" and (self.eligible_targets is not None or self.removal_targets):
            raise ValueError("Physical binding uses original proposals, not a replacement target list")
        _unique(tuple(target.key for target in self.removal_targets), "Affirmative removal targets")
        for target in self.removal_targets:
            _same_context(self.expected, target)
            if target.workload != "fabric_pipeline":
                raise ValueError("Connector removal authority requires pipeline target identities")
        if self.eligible_targets is not None:
            _unique(tuple(target.key for target in self.eligible_targets), "Eligible connector targets")
            for target in self.eligible_targets:
                _same_context(self.expected, target.identity)
                if (
                    target.identity.workload != "fabric_pipeline" or target.state != "current"
                    or not target.observation.enabled or target.policy_revision != self.expected.revision
                ):
                    raise ValueError("Connector planning requires current admitted pipeline targets")
        return self


class PollProgress(MonitoringModel):
    checkpoint: RestCheckpoint
    producer_request_id: CanonicalId
    powerbi_window_complete: StrictBool = False
    window_complete: StrictBool = False
    retention_exhausted: StrictBool = False


class PollSchedule(MonitoringModel):
    target: TargetIdentity
    policy_revision: Revision
    next_poll_at: UtcDateTime
    updated_at: UtcDateTime


class CollectionAcceptance(MonitoringContext):
    """Original API result material, bound as evidence by every native intake part."""

    operation: Literal["inventory", "capability", "rest_page"]
    request_id: CanonicalId
    fingerprint: Fingerprint
    producer_commit: CollectionCommit
    part_ids: Annotated[tuple[CanonicalId, ...], Field(min_length=1, max_length=100)]
    reference_id: StateKey
    target: TargetIdentity | None = None
    window: ObservationWindow | None = None
    reconciliation_payload: JsonObject
    result: InventoryGeneration | CapabilityObservation | RestPageReceipt

    @model_validator(mode="after")
    def validate_result(self) -> CollectionAcceptance:
        _lease_for(self.producer_commit.lease, self, work_key(self, self.producer_commit.work_id))
        _unique(self.part_ids, "Native intake part IDs")
        if self.part_ids[-1] != self.request_id:
            raise ValueError("The closing native receipt retains the original API request ID")
        expected_type = {
            "inventory": InventoryGeneration, "capability": CapabilityObservation, "rest_page": RestPageReceipt,
        }[self.operation]
        if not isinstance(self.result, expected_type):
            raise ValueError("Accepted result material does not match its operation")
        if self.target is not None:
            _same_context(self, self.target)
        if isinstance(self.result, InventoryGeneration):
            _same_context(self, self.result)
        elif isinstance(self.result, CapabilityObservation):
            if self.result.target != self.target:
                raise ValueError("Capability acceptance belongs to another target")
        elif self.result.checkpoint.target != self.target or self.result.intake.request_id != self.request_id:
            raise ValueError("REST acceptance belongs to another target or page")
        return self


class ValidationHandoff(MonitoringModel):
    frontier_key: StateKey
    frontier_revision: PositiveRevision
    producer: ProducerComponent
    producer_request_id: CanonicalId
    producer_operation: OpaqueId
    producer_fingerprint: Fingerprint
    producer_binding_hash: Fingerprint
    work_id: CanonicalId
    policy_revision: Revision
    evidence_digest: Fingerprint
    requires_window: StrictBool


class FrontierValidation(MonitoringModel):
    validation_id: CanonicalId
    work_id: CanonicalId
    lease_owner_id: CanonicalId
    lease_fence: PositiveRevision
    expected_work_revision: PositiveRevision
    policy_revision: Revision
    frontier_key: StateKey
    through_revision: PositiveRevision
    producer_request_id: CanonicalId
    producer_fingerprint: Fingerprint
    evidence_digest: Fingerprint
    decision: Literal["published", "rejected"]
    detail: Detail
    window_complete: StrictBool = False
    closing_request_id: CanonicalId | None = None
    reject_whole_window: StrictBool = False
    acknowledge_handoff: StrictBool = False

    @model_validator(mode="after")
    def validate_closure(self) -> FrontierValidation:
        if self.acknowledge_handoff and (
            self.reject_whole_window or self.window_complete or self.closing_request_id is not None
        ):
            raise ValueError("Handoff acknowledgement cannot request window publication or rejection")
        if self.reject_whole_window and (self.decision != "rejected" or not self.window_complete):
            raise ValueError("Whole-window rejection must be an explicit terminal rejection")
        if self.window_complete and not self.reject_whole_window and self.closing_request_id is None:
            raise ValueError("Window publication must retain the accepted closing request identity")
        return self


class FrontierResolution(MonitoringModel):
    work_id: CanonicalId
    work_fence: PositiveRevision
    producer_request_id: CanonicalId
    frontier_key: StateKey
    frontier_revision: PositiveRevision
    handoff_revision: PositiveRevision
    validated_revision: Revision
    state: Literal["published", "rejected", "pending_validation"]
    handoff_decision: Literal["published", "rejected", "pending_validation"]
    resolution_scope: Literal["handoff", "window", "window_acknowledgement", "handoff_acknowledgement"]
    handoff_resolution_request_id: CanonicalId | None
    handoff_resolution_work_fence: PositiveRevision | None
    frontier_resolution_request_id: CanonicalId | None
    frontier_resolution_revision: PositiveRevision | None
    window_rejection_request_id: CanonicalId | None
    window_resolution_request_id: CanonicalId | None
    window_resolution_state: Literal["published", "rejected"] | None

    @model_validator(mode="after")
    def validate_frontier(self) -> FrontierResolution:
        if max(self.handoff_revision, self.validated_revision) > self.frontier_revision:
            raise ValueError("Resolved validation cannot exceed accepted intake")
        if self.resolution_scope == "handoff_acknowledgement":
            if (
                self.state != self.handoff_decision or self.validated_revision != self.frontier_resolution_revision
                or self.handoff_resolution_work_fence is None or self.handoff_resolution_work_fence > self.work_fence
            ):
                raise ValueError("Handoff acknowledgement must retain its original decision, fence and committed prefix")
        elif self.state != "pending_validation" and self.validated_revision != self.frontier_revision:
            raise ValueError("Terminal frontier resolution requires the full accepted prefix")
        _validate_handoff_resolution(
            self.state, self.resolution_scope, self.handoff_revision, self.frontier_revision,
            self.handoff_resolution_request_id, self.handoff_resolution_work_fence,
            self.frontier_resolution_request_id, self.frontier_resolution_revision,
        )
        _validate_window_resolution(
            self.state, self.resolution_scope, self.window_rejection_request_id,
            self.window_resolution_request_id, self.window_resolution_state,
        )
        return self


def _validate_handoff_resolution(
    state: str, scope: str, handoff_revision: int | None, accepted_revision: int,
    original_id: str | None, original_fence: int | None, frontier_id: str | None, frontier_revision: int | None,
) -> None:
    references = (original_id, original_fence, frontier_id, frontier_revision)
    if scope == "handoff_acknowledgement":
        if (
            state not in {"published", "rejected"} or any(value is None for value in references)
            or handoff_revision is None or not handoff_revision <= frontier_revision <= accepted_revision
        ):
            raise ValueError("Handoff acknowledgement requires exact original decision and committed-prefix references")
    elif any(value is not None for value in references):
        raise ValueError("Only a non-window handoff acknowledgement references an earlier handoff and prefix")
