"""Pure deployer/backend contracts for the component-scoped SQL kernel.

Every RPC returns exactly one row/column named ``result_json``. Mutating RPCs
must be called with ``db.query`` inside the existing synchronous
``AzureSqlDatabase.transaction()``; never interpret EXEC rowcount as success.
JSON parameters are canonical request text and must be retained for replay.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from triage.monitoring.schema import resolve_tables
from triage.store.azure_sql import quote_identifier

Component = Literal["worker", "web", "controller"]
COMPONENTS: tuple[Component, ...] = ("worker", "web", "controller")
KERNEL_VERSION = 2
WORKER_WORK_KINDS = ("inventory", "capability_probe", "poll", "connector_reconcile")
ACTION_WORK_KINDS = ("triage", "deferred_retry", "verify_action", "finalize")
CONTROLLER_WORK_KINDS = (*ACTION_WORK_KINDS, "reconcile_state")
FRONTIER_KINDS = (
    "validation_frontier", "validation_window", "validation_handoff", "frontier_commit",
    "reconcile_acceptance",
)
CATALOGUE_KINDS = (
    "domain", "domain_seen", "generation", "inventory", "inventory_seen", "workspace", "workspace_seen",
)
EVIDENCE_KINDS = (
    "capability", "rest_observation", "rest_powerbi_row", "powerbi_window_row",
    "powerbi_window_quarantine", "quarantine",
)
TELEMETRY_KINDS = ("poll_schedule", "poll_progress", "intake_disposition")
RPC_FACT_KINDS = ("signal", "unidentified_signal", "receiver_heartbeat")
FACT_KINDS = (*CATALOGUE_KINDS, *EVIDENCE_KINDS, *TELEMETRY_KINDS, *RPC_FACT_KINDS)
SOURCE_DISPOSITIONS = ("triaged", "duplicate", "historical", "refused", "failed")
WORK_FACT_KINDS = {
    "inventory": (*CATALOGUE_KINDS, "capability", "quarantine", "intake_disposition"),
    "capability_probe": ("capability", "quarantine", "intake_disposition"),
    "poll": (
        "rest_observation", "rest_powerbi_row", "powerbi_window_row",
        "powerbi_window_quarantine", "quarantine", *TELEMETRY_KINDS,
    ),
    "connector_reconcile": ("quarantine", "intake_disposition"),
}
CONTROLLER_PROJECTION_KINDS = (
    "target", "target_capability", "review", "controller_validation",
    "source_work", "powerbi_alias", "powerbi_window", "rest_checkpoint",
)
SOURCE_KINDS = ("source", "source_head", "source_disposition")
CONTROLLER_IMMUTABLE_KINDS = (
    "approval_binding", "finalization_plan", "frontier_validation", "connector_publication",
)
CONTROLLER_ACTION_KINDS = (
    "action", "action_owner", "action_rejection", "action_outcome", "submitted_action", "incident_state",
    "incident_occurrence",
)
IDENTITY_COLUMNS = ("tenant_id", "epoch", "record_kind", "key_hash", "full_key")
RECORD_COLUMNS = (
    *IDENTITY_COLUMNS, "revision", "status", "workload", "workspace_id", "item_id",
    "target_hash", "target_key", "parent_hash", "parent_key", "work_kind", "generation_id",
    "due_at", "sequence_number", "payload",
)
MUTABLE_FACT_COLUMNS = tuple(column for column in RECORD_COLUMNS if column not in IDENTITY_COLUMNS)


@dataclass(frozen=True)
class WorkPolicy:
    component: Component
    dispatch_route: Literal["collection", "reconcile_state", "exact_source"]
    requires_target: bool
    requires_execution: bool
    action_ownership: bool
    permits_action_promotion: bool


def work_policy(work: Mapping[str, object]) -> WorkPolicy:
    """Classify before any ordinary source/target validation.

    A target on reconciliation is a reference, never action ownership. SQL
    repeats this classification against the stored kind, not a caller flag.
    """
    kind = work.get("kind")
    if kind == "reconcile_state":
        if any(work.get(field) is not None for field in (
            "execution", "action_reservation_id", "retry_of", "finalization_id",
        )) or type(work.get("retry_attempt", 0)) is not int or work.get("retry_attempt", 0) != 0:
            raise ValueError("Reconciliation cannot carry action, execution or retry lineage")
        return WorkPolicy("controller", "reconcile_state", False, False, False, False)
    if kind in ACTION_WORK_KINDS:
        return WorkPolicy("controller", "exact_source", True, True, True, True)
    if kind in WORKER_WORK_KINDS:
        return WorkPolicy("worker", "collection", kind in {"poll", "capability_probe"}, False, False, False)
    raise ValueError("Unknown stored monitoring work family")


@dataclass(frozen=True)
class SqlNames:
    tables: Mapping[str, str]
    suffix: str

    @classmethod
    def from_tables(cls, tables: Mapping[str, str] | None = None) -> SqlNames:
        names = resolve_tables(tables=tables)
        names.setdefault("monitoring_rate_budget", "triage_monitoring_rate_budget")
        for value in names.values():
            quote_identifier(value)
        if len({value.casefold() for value in names.values()}) != len(names):
            raise ValueError("SQL kernel logical objects must have distinct physical table names")
        identity = "\n".join(f"{key}={names[key]}" for key in sorted(names))
        return cls(names, hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12])

    def table(self, logical: str) -> str:
        return quote_identifier(self.tables[logical])

    def name(self, logical: str) -> str:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,70}", logical):
            raise ValueError("Kernel object names must be fixed logical identifiers")
        return f"triage_mon_{logical}_{self.suffix}"

    def object(self, logical: str) -> str:
        return f"[dbo].[{self.name(logical)}]"

    def role(self, component: Component) -> str:
        if component not in COMPONENTS:
            raise ValueError("Unknown SQL kernel component")
        return self.name(f"role_{component}")


@dataclass(frozen=True)
class RpcParameter:
    name: str
    sql_type: str
    nullable: bool = False

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", self.name):
            raise ValueError("Invalid RPC parameter name")
        if not re.fullmatch(r"(?:n?varchar\((?:max|[0-9]+)\)|char\([0-9]+\)|bigint|int|bit|datetime2\(6\))", self.sql_type):
            raise ValueError("RPC parameter type is outside the fixed kernel contract")


@dataclass(frozen=True)
class RpcContract:
    operation: str
    object_name: str
    parameters: tuple[RpcParameter, ...]
    components: tuple[Component, ...]
    mutating: bool
    description: str
    implemented: bool = True
    blocked_reason: str | None = None
    blocked_cases: tuple[str, ...] = ()
    result_columns: tuple[str, ...] = ("result_json",)
    result_fields: tuple[str, ...] = ()
    not_acquired_fields: tuple[str, ...] = ()
    statuses: tuple[str, ...] = ("applied", "replayed")

    def bind(self, arguments: Mapping[str, object]) -> tuple[str, tuple[object, ...]]:
        expected = {parameter.name for parameter in self.parameters}
        if set(arguments) != expected:
            raise ValueError(f"{self.operation} requires exactly {sorted(expected)}")
        values = []
        for parameter in self.parameters:
            value = arguments[parameter.name]
            if value is None:
                if not parameter.nullable:
                    raise ValueError(f"{parameter.name} cannot be null")
            elif parameter.sql_type in {"bigint", "int"}:
                bits = 64 if parameter.sql_type == "bigint" else 32
                if type(value) is not int or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                    raise ValueError(f"{parameter.name} needs a strict {bits}-bit integer")
            elif parameter.sql_type == "bit":
                if type(value) is not bool:
                    raise ValueError(f"{parameter.name} needs a strict boolean")
            elif parameter.sql_type.startswith("datetime2"):
                if not isinstance(value, datetime) or value.utcoffset() is None:
                    raise ValueError(f"{parameter.name} needs a timezone-aware datetime")
                value = value.astimezone(UTC).replace(tzinfo=None)
            else:
                if not isinstance(value, str):
                    raise ValueError(f"{parameter.name} needs text")
                encoding = "utf-16-le" if parameter.sql_type.startswith("nvarchar") else "ascii"
                try:
                    units = len(value.encode(encoding)) // (2 if encoding == "utf-16-le" else 1)
                except UnicodeError as exc:
                    raise ValueError(f"{parameter.name} is not valid {encoding} text") from exc
                maximum = re.search(r"\(([0-9]+)\)", parameter.sql_type)
                if maximum and units > int(maximum[1]):
                    raise ValueError(f"{parameter.name} exceeds its SQL parameter width")
            values.append(value)
        assignments = ", ".join(f"@{parameter.name} = ?" for parameter in self.parameters)
        return (
            f"EXEC {self.object_name} {assignments};",
            tuple(values),
        )


@dataclass(frozen=True)
class KernelObject:
    logical_name: str
    name: str
    kind: Literal["role", "view", "procedure", "function"]
    ddl: str


@dataclass(frozen=True)
class PermissionKernel:
    names: SqlNames
    objects: tuple[KernelObject, ...]
    rpcs: Mapping[str, RpcContract]
    grants: Mapping[Component, tuple[str, ...]]
    preconditions: tuple[str, ...] = ()

    @property
    def statements(self) -> tuple[str, ...]:
        return (*self.preconditions, *(obj.ddl for obj in self.objects), *(
            statement for component in COMPONENTS for statement in self.grants[component]
        ))

    def catalogue(self) -> tuple[dict[str, str], ...]:
        return tuple({
            "logical_name": obj.logical_name, "name": obj.name, "kind": obj.kind,
            "sha256": hashlib.sha256(obj.ddl.encode("utf-8")).hexdigest(),
        } for obj in self.objects)

    @property
    def unresolved_cases(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            (name, reason) for name, rpc in self.rpcs.items() for reason in rpc.blocked_cases
        )


def decode_rpc_result(contract: RpcContract, rows: Sequence[Sequence[object]]) -> dict:
    if len(rows) != 1 or len(rows[0]) != 1 or not isinstance(rows[0][0], str):
        raise ValueError(f"{contract.operation} did not return one result_json cell")
    result = json.loads(rows[0][0])
    if (
        not isinstance(result, dict) or type(result.get("kernel_version")) is not int
        or result["kernel_version"] != KERNEL_VERSION
    ):
        raise ValueError("SQL kernel result version/shape is invalid")
    if result.get("operation") != contract.operation:
        raise ValueError("SQL kernel result belongs to another operation")
    if result.get("status") not in contract.statuses:
        raise ValueError("SQL kernel returned an unsupported result status")
    if type(result.get("affected_rows")) is not int or result["affected_rows"] < 0:
        raise ValueError("SQL kernel affected-row evidence is invalid")
    if result["status"] in {"replayed", "read"} and result["affected_rows"] != 0:
        raise ValueError("Read/replayed SQL results cannot report a new mutation")
    if not isinstance(result.get("result"), dict):
        raise ValueError("SQL kernel result payload must be an object")
    required = contract.not_acquired_fields if result["status"] == "not_acquired" else contract.result_fields
    if set(required) - set(result["result"]):
        raise ValueError(f"{contract.operation} result omitted required fields")
    return result


def parameter(name: str, sql_type: str, *, nullable: bool = False) -> RpcParameter:
    return RpcParameter(name, sql_type, nullable)


CONTEXT = (parameter("tenant_id", "nvarchar(36)"), parameter("epoch", "nvarchar(36)"))
REQUEST = (
    parameter("request_id", "nvarchar(256)"), parameter("fingerprint", "char(64)"),
    parameter("expected_revision", "bigint"),
)
WORK_FENCE = (
    parameter("work_id", "nvarchar(128)"), parameter("owner_id", "nvarchar(128)"),
    parameter("fence", "bigint"), parameter("work_revision", "bigint"),
)
PARTITION = (
    parameter("connector_id", "nvarchar(128)"), parameter("consumer_group", "nvarchar(50)"),
    parameter("partition_id", "nvarchar(32)"),
)


def rpc_contracts(tables: Mapping[str, str] | None = None) -> dict[str, RpcContract]:
    """Stable integration interface, independent of deployment or a SQL connection."""
    names = SqlNames.from_tables(tables)
    result: dict[str, RpcContract] = {}

    def add(operation: str, args: tuple[RpcParameter, ...], components: tuple[Component, ...],
            description: str, *, mutating: bool = True) -> None:
        logical = operation.replace(".", "_")
        result[operation] = RpcContract(
            operation, names.object(logical), args, components, mutating, description,
        )

    add("inspect", CONTEXT, COMPONENTS, "Read current control and kernel surfaces.", mutating=False)
    add("lock_context", CONTEXT, COMPONENTS, "Lock current control without UPDATE authority.")
    add("web.commit_intent", CONTEXT + REQUEST + (
        parameter("intent_kind", "varchar(24)"), parameter("intent_id", "nvarchar(128)"),
        parameter("expected_intent_revision", "bigint"), parameter("intent_json", "nvarchar(max)"),
    ), ("web",), "Commit typed scope/review/discovery intent, immediately advance policy revision, and enqueue clean reconciliation.")
    add("worker.accept_facts", CONTEXT + REQUEST + WORK_FENCE + (
        parameter("facts_json", "nvarchar(max)"),
        parameter("window_start_at", "datetime2(6)", nullable=True),
        parameter("window_end_at", "datetime2(6)", nullable=True),
        parameter("collection_complete", "bit"),
    ), ("worker",), "Accept a bounded collection batch with immutable receipt bindings; orphan rows are not accepted evidence.")
    add("controller.inspect_frontiers", CONTEXT + (
        parameter("target_key", "nvarchar(1024)", nullable=True),
    ), ("controller",), "Lock control and read the applicable deny-only frontier/window snapshot and its SQL-computed digest.")
    add("controller.resolve_frontier", CONTEXT + REQUEST + WORK_FENCE + (
        parameter("expected_frontier_revision", "bigint"),
        parameter("validation_id", "nvarchar(128)"), parameter("validation_hash", "char(64)"),
    ), ("controller",), "Publish or durably reject an exact producer handoff using its own reconcile_state lease and immutable controller proof; never action ownership.")
    add("controller.publish_connector", CONTEXT + REQUEST + WORK_FENCE + (
        parameter("publication_id", "nvarchar(128)"), parameter("publication_hash", "char(64)"),
    ), ("controller",), "Publish fixed current-policy desired intent, receipt-bound source binding/retirement or readiness; retain physical ownership until original verified remote absence.")
    add("controller.enqueue_work", CONTEXT + REQUEST + (
        parameter("work_id", "nvarchar(128)"), parameter("draft_json", "nvarchar(max)"),
    ), ("controller",), "Insert one clean admitted work draft; never replace or update existing controller work.")
    for component in ("worker", "controller"):
        add(f"{component}.claim_work", CONTEXT + (
            parameter("work_id", "nvarchar(128)"), parameter("owner_id", "nvarchar(128)"),
            parameter("lease_seconds", "int"),
        ), (component,), "Claim only the stored component work family, with conditional lease ownership.")
        add(f"{component}.transition_work", CONTEXT + REQUEST + WORK_FENCE + (
            parameter("transition", "varchar(24)"), parameter("lease_seconds", "int", nullable=True),
            parameter("retry_at", "datetime2(6)", nullable=True),
            parameter("detail", "nvarchar(2000)", nullable=True),
            parameter("finalization_id", "nvarchar(128)", nullable=True),
        ), (component,), "Apply a legal family-bound transition using current owner/fence/revision.")
    add("worker.partition", CONTEXT + REQUEST + PARTITION + (
        parameter("transition", "varchar(24)"),
        parameter("expected_owner_id", "nvarchar(128)", nullable=True),
        parameter("expected_fence", "bigint", nullable=True),
        parameter("expected_ownership_revision", "bigint"),
        parameter("new_owner_id", "nvarchar(128)", nullable=True),
        parameter("lease_seconds", "int", nullable=True),
        parameter("first_sequence_number", "bigint", nullable=True),
        parameter("broker_observed_at", "datetime2(6)", nullable=True),
    ), ("worker",), "Atomically maintain partition lease/journal and pin broker start once.")
    add("worker.commit_positions", CONTEXT + REQUEST + PARTITION + (
        parameter("owner_id", "nvarchar(128)"), parameter("fence", "bigint"),
        parameter("positions_json", "nvarchar(max)"),
    ), ("worker",), "Commit position/evidence/receipt linkage under current partition ownership.")
    add("worker.advance_checkpoint", CONTEXT + REQUEST + PARTITION + (
        parameter("owner_id", "nvarchar(128)"), parameter("fence", "bigint"),
        parameter("expected_checkpoint_revision", "bigint"),
        parameter("through_sequence_number", "bigint"), parameter("through_offset", "nvarchar(256)"),
    ), ("worker",), "Advance only through contiguous committed positions and exact original offset.")
    add("worker.observe_retention", CONTEXT + REQUEST + PARTITION + (
        parameter("owner_id", "nvarchar(128)"), parameter("fence", "bigint"),
        parameter("expected_checkpoint_revision", "bigint"),
        parameter("first_available_sequence_number", "bigint"), parameter("observed_at", "datetime2(6)"),
    ), ("worker",), "Record a later actual broker retention/boundary gap without moving the original pin or checkpoint.")
    add("worker.observe_connector", CONTEXT + REQUEST + WORK_FENCE + (
        parameter("connector_id", "nvarchar(128)"),
        parameter("expected_connector_revision", "bigint"),
        parameter("ownership_id", "nvarchar(128)"),
        parameter("observation_json", "nvarchar(max)"),
    ), ("worker",), "Accept a restricted observation under its actual collection work fence; derive completion eligibility and the explicit definition hash without publishing desired ownership or readiness.")
    add("worker.rate_budget", CONTEXT + (
        parameter("bucket", "nvarchar(128)"), parameter("delay_seconds", "int", nullable=True),
    ), ("worker",), "Acquire/defer only an already provisioned budget policy; never reset/configure a limit.")
    add("worker.record_heartbeat", CONTEXT + REQUEST + (
        parameter("worker_id", "nvarchar(128)"), parameter("connector_id", "nvarchar(128)"),
        parameter("state", "varchar(16)"), parameter("transport_connected", "bit"),
        parameter("accepted_positions", "bigint"),
        parameter("last_delivery_at", "datetime2(6)", nullable=True),
        parameter("last_maintenance_at", "datetime2(6)", nullable=True),
        parameter("error_code", "nvarchar(128)", nullable=True),
    ), ("worker",), "Record typed non-authoritative health and its accepted binding without changing connector readiness.")
    add("controller.open_approval", CONTEXT + REQUEST + (
        parameter("approval_id", "nvarchar(200)"), parameter("channel", "varchar(16)"),
        parameter("approval_fingerprint", "nvarchar(256)"),
        parameter("expires_at", "datetime2(6)"), parameter("proposal_json", "nvarchar(max)"),
    ), ("controller",), "Open a new immutable pending approval; never reopen a used request.")
    add("web.decide_approval", CONTEXT + REQUEST + (
        parameter("approval_id", "nvarchar(200)"), parameter("channel", "varchar(16)"),
        parameter("approval_fingerprint", "nvarchar(256)"), parameter("decision", "varchar(16)"),
        parameter("responder", "nvarchar(200)"), parameter("reason", "nvarchar(2000)"),
    ), ("web",), "Record explicit matched-channel unexpired unused approval/decline only.")
    add("controller.consume_approval", CONTEXT + REQUEST + (
        parameter("approval_id", "nvarchar(200)"), parameter("channel", "varchar(16)"),
        parameter("approval_fingerprint", "nvarchar(256)"),
    ), ("controller",), "Consume once within the caller's reservation transaction; no refund/reset operation.")
    add("controller.reserve_action", CONTEXT + REQUEST + WORK_FENCE + (
        parameter("reservation_id", "nvarchar(128)"), parameter("reservation_json", "nvarchar(max)"),
    ), ("controller",), "Create a policy-revision-fenced reservation only; no generic controller-state setter.")
    add("controller.transition_action", CONTEXT + REQUEST + WORK_FENCE + (
        parameter("reservation_id", "nvarchar(128)"), parameter("expected_action_revision", "bigint"),
        parameter("transition", "varchar(24)"), parameter("transition_json", "nvarchar(max)"),
    ), ("controller",), "Transition an existing reservation while retaining original lineage after revocation.")
    add("controller.finalize", CONTEXT + REQUEST + WORK_FENCE + (
        parameter("finalization_id", "nvarchar(128)"), parameter("finalization_json", "nvarchar(max)"),
    ), ("controller",), "Commit incident/budget/source/work finalization under the existing fence.")
    source_args = (
        parameter("observation_json", "nvarchar(max)"),
        parameter("evidence_kind", "varchar(40)", nullable=True),
        parameter("evidence_key", "nvarchar(1024)", nullable=True),
        parameter("alias_window_id", "nvarchar(128)", nullable=True),
    )
    add("controller.publish_source", CONTEXT + REQUEST + WORK_FENCE + source_args, ("controller",),
        "Publish exact accepted collection evidence or a fresh actual work/target-leased REST observation; preserve source/head monotonicity.")
    add("controller.disposition_source", CONTEXT + REQUEST + WORK_FENCE + source_args + (
        parameter("disposition", "varchar(24)"), parameter("detail", "nvarchar(2000)"),
        parameter("subject_work_id", "nvarchar(128)", nullable=True),
        parameter("expected_subject_revision", "bigint", nullable=True),
    ), ("controller",), "Atomically persist a proven non-effect source disposition, processed marker and original receipt; never bypass a reservation.")
    from dataclasses import replace

    result["controller.reserve_action"] = replace(
        result["controller.reserve_action"],
        description="reservation_json is {request: ActionReservationRequest, incident_id: canonical ID}; creates one current-policy reservation, never arbitrary state.",
    )
    result["controller.finalize"] = replace(
        result["controller.finalize"],
        description="finalization_json is {plan_key, plan_hash}; commits only the immutable controller-owned finalization_plan under its exact work fence and prior incident hash.",
    )
    result["controller.enqueue_work"] = replace(
        result["controller.enqueue_work"],
        description="Insert clean admitted work. deferred_retry resolves only the exact SQL-owned successor minted atomically by transition_action(rejected); it never creates an unlinked retry.",
    )
    fields = {
        "inspect": ("tenant_id", "epoch", "revision", "maintenance", "observed_at"),
        "lock_context": ("tenant_id", "epoch", "revision", "maintenance", "observed_at"),
        "web.commit_intent": ("request_id", "intent_id", "state", "policy_revision"),
        "worker.accept_facts": ("batch_id", "work_id", "work_fence", "reconcile_work_id", "state", "facts", "frontier_key", "frontier_revision"),
        "controller.inspect_frontiers": ("target_key", "frontier_digest", "pending", "frontiers"),
        "controller.resolve_frontier": (
            "work_id", "producer_request_id", "frontier_key", "frontier_revision", "state",
            "validated_revision", "resolution_scope", "window_rejection_request_id",
            "window_resolution_request_id", "window_resolution_state",
            "work_fence", "handoff_decision", "handoff_revision",
            "handoff_resolution_request_id", "handoff_resolution_work_fence",
            "frontier_resolution_request_id", "frontier_resolution_revision",
        ),
        "controller.publish_connector": (
            "connector_id", "connector", "state", "desired_changed",
            "pending_removals", "retired_sources", "observation_receipt_id",
        ),
        "controller.enqueue_work": ("work_id", "work"),
        "worker.claim_work": ("work", "lease"),
        "controller.claim_work": ("work", "lease"),
        "worker.transition_work": ("work_id", "work"),
        "controller.transition_work": ("work_id", "work"),
        "worker.partition": ("partition_key", "partition", "ownership_revision", "last_owner_id", "last_fence"),
        "worker.commit_positions": ("batch_id", "partition_key", "partition", "position_count", "positions", "receipt_keys", "reconcile_work_id", "state", "frontier_key", "frontier_revision"),
        "worker.advance_checkpoint": ("partition_key", "partition", "position", "revision", "sequence_number", "offset", "updated_at"),
        "worker.observe_retention": ("partition", "start", "checkpoint", "observation", "state"),
        "worker.observe_connector": (
            "connector_id", "connector", "observation", "authority", "observed_definition_hash",
            "reconcile_work_id", "frontier_key", "frontier_revision",
            "work_id", "work_owner_id", "work_fence", "work_revision", "collection_completion_eligible",
        ),
        "worker.rate_budget": ("allowed", "used", "request_limit", "window_seconds", "window_ends_at", "blocked_until"),
        "worker.record_heartbeat": ("worker_id", "connector_id", "state", "observed_at", "transport_connected"),
        "controller.open_approval": ("approval_id", "state", "channel"),
        "web.decide_approval": ("approval_id", "decision", "channel"),
        "controller.consume_approval": ("approval_id", "state", "channel"),
        "controller.reserve_action": ("reservation_id", "reservation"),
        "controller.transition_action": ("reservation_id", "reservation", "retry_work"),
        "controller.finalize": ("work_id", "finalization_id", "incident_id", "state", "incident", "source_disposition"),
        "controller.publish_source": ("source_key", "observation"),
        "controller.disposition_source": ("source_key", "disposition"),
    }
    for operation, required in fields.items():
        not_acquired = (
            ("reason",) if operation.endswith(".claim_work")
            else required if operation == "worker.rate_budget" else ()
        )
        result[operation] = replace(
            result[operation], result_fields=required, not_acquired_fields=not_acquired,
            statuses=(
                ("read",) if operation in {"inspect", "lock_context", "controller.inspect_frontiers"}
                else ("applied", "not_acquired") if not_acquired else ("applied", "replayed")
            ),
        )
    return result
