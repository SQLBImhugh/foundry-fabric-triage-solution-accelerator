"""Deployer-only SQL component boundary and pure backend integration catalogue.

No function connects to SQL or installs schema at runtime. Call
``schema_statements`` only from the parent's reviewed baseline installer after
the physical monitoring/legacy tables exist. Existing backend integration is a
separate change: do not continue using broad ``runtime_table_permissions``.

RPC use::

    contract = rpc_contracts()["worker.claim_work"]
    sql, values = contract.bind(arguments)
    with database.transaction():
        result = decode_rpc_result(contract, database.query(sql, *values))

Do not await, nest transactions or use ``execute(...).rowcount`` for RPC success.
An SQL exception or uncertain commit is not a second mutation opportunity.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from uuid import UUID

from triage.monitoring.sql_kernel_contracts import (
    COMPONENTS,
    KERNEL_VERSION,
    WORK_FACT_KINDS,
    Component,
    KernelObject,
    PermissionKernel,
    RpcContract,
    RpcParameter,
    SqlNames,
    WorkPolicy,
    decode_rpc_result,
    rpc_contracts,
    work_policy,
)

__all__ = [
    "COMPONENTS", "KERNEL_VERSION", "Component", "KernelObject", "PermissionKernel",
    "RpcContract", "RpcParameter", "build_permission_kernel", "decode_rpc_result",
    "object_catalogue", "rpc_contracts", "runtime_grants", "schema_statements",
    "budget_policy_statements", "integration_contract",
    "WorkPolicy", "work_policy",
]


def build_permission_kernel(tables: Mapping[str, str] | None = None) -> PermissionKernel:
    from triage.monitoring.sql_kernel_schema import build_kernel

    return build_kernel(SqlNames.from_tables(tables), rpc_contracts(tables))


def schema_statements(tables: Mapping[str, str] | None = None) -> tuple[str, ...]:
    return build_permission_kernel(tables).statements


def object_catalogue(tables: Mapping[str, str] | None = None) -> tuple[dict[str, str], ...]:
    return build_permission_kernel(tables).catalogue()


def runtime_grants(component: Component, tables: Mapping[str, str] | None = None) -> tuple[str, ...]:
    if component not in COMPONENTS:
        raise ValueError("Unknown SQL kernel component")
    return build_permission_kernel(tables).grants[component]


def budget_policy_statements(
    tenant_id: str, policies: Mapping[str, tuple[int, int]],
    tables: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Deployer-only initial budget policies; never rewrite counters or limits.

    Values are ``bucket -> (request_limit, window_seconds)``. Freeze the actual
    REST-adapter bucket catalogue before calling this. An existing different
    policy is a conflict, not permission to reset a service's allowance.
    """
    parsed_tenant = UUID(tenant_id)
    if parsed_tenant.int == 0:
        raise ValueError("Budget policy needs an explicit tenant")
    tenant = str(parsed_tenant)
    names = SqlNames.from_tables(tables)
    table = names.table("monitoring_rate_budget")
    statements = []
    for bucket, policy in sorted(policies.items()):
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", bucket):
            raise ValueError("Budget bucket is outside the reviewed identifier set")
        if len(policy) != 2 or any(type(value) is not int for value in policy):
            raise ValueError("Budget policy must contain two strict integers")
        limit, seconds = policy
        if not 1 <= limit <= 1_000_000 or not 1 <= seconds <= 86_400:
            raise ValueError("Budget policy exceeds the supported bounds")
        digest = hashlib.sha256(bucket.encode("ascii")).hexdigest()
        statements.append(f"""IF EXISTS (
    SELECT 1 FROM {table} WHERE tenant_id='{tenant}' AND bucket_hash='{digest}'
      AND (request_limit<>{limit} OR window_seconds<>{seconds}))
    THROW 51072, 'Existing service budget policy differs; no reset performed', 1;
IF NOT EXISTS (SELECT 1 FROM {table} WHERE tenant_id='{tenant}' AND bucket_hash='{digest}')
    INSERT INTO {table} (tenant_id,bucket_hash,request_limit,window_seconds,window_ends_at,used)
    VALUES ('{tenant}','{digest}',{limit},{seconds},DATEADD(second,{seconds},SYSUTCDATETIME()),0);""")
    return tuple(statements)


def integration_contract(tables: Mapping[str, str] | None = None) -> dict:
    """Machine-readable handoff; it describes implementation, not native proof."""
    kernel = build_permission_kernel(tables)
    return {
        "kernel_version": KERNEL_VERSION,
        "roles": {component: kernel.names.role(component) for component in COMPONENTS},
        "read_routes": {
            "worker_records": kernel.names.object("worker_read"),
            "web_records": kernel.names.object("web_read"),
            "controller_records": kernel.names.object("controller_read"),
            "accepted_worker_facts": kernel.names.object("accepted_worker_facts"),
            "control": kernel.names.object("control_read"),
            "controller_web_approvals": kernel.names.object("approval_read"),
            "controller_web_incidents": kernel.names.object("incident_read"),
            "controller_processed": kernel.names.object("processed_read"),
        },
        "write_routes": {
            "worker_catalogue": kernel.names.object("worker_catalogue"),
            "worker_evidence": kernel.names.object("worker_evidence"),
            "worker_telemetry": kernel.names.object("worker_telemetry"),
            "web_drafts": kernel.names.object("web_drafts"),
            "controller_projections": kernel.names.object("controller_projections"),
            "controller_immutable": kernel.names.object("controller_immutable"),
        },
        "accepted_fact_families": {kind: list(facts) for kind, facts in WORK_FACT_KINDS.items()},
        "work_policy": {
            kind: {
                "dispatch_route": work_policy({"kind": kind}).dispatch_route,
                "requires_target": work_policy({"kind": kind}).requires_target,
                "requires_execution": work_policy({"kind": kind}).requires_execution,
                "action_ownership": work_policy({"kind": kind}).action_ownership,
                "permits_action_promotion": work_policy({"kind": kind}).permits_action_promotion,
            }
            for kind in (*WORK_FACT_KINDS, "triage", "deferred_retry", "verify_action", "finalize", "reconcile_state")
        },
        "ddl_call": (
            "Execute each schema_statements() entry as its own batch on the deployer's connection. "
            "Do not concatenate CREATE VIEW/FUNCTION/PROCEDURE statements into a single batch. "
            "All data objects use dbo ownership chaining; functions have no runtime direct grants."
        ),
        "rpc_call": (
            "sql, values = contract.bind(arguments); inside existing db.transaction(), "
            "call db.query(sql, *values), then decode_rpc_result(contract, rows). "
            "Never use EXEC rowcount; never await/nest/reconnect within the transaction."
        ),
        "result": "Exactly one result_json cell: kernel_version, operation, status, affected_rows, result object.",
        "json_replay": (
            "Retain the original canonical JSON parameter text and typed arguments. "
            "The original fingerprint and a SQL-computed UTF-16-LE binding hash are both checked on replay."
        ),
        "identifiers_and_times": (
            "Context, operation-request, work, owner, connector and reservation IDs use canonical "
            "nonempty lower-case GUIDs; event source/id and incident signatures remain opaque. "
            "Canonical JSON parameters use sorted object keys. SQL JSON dates are UTC with Z. "
            "bind() requires aware datetime values and converts datetime2 parameters to naive UTC."
        ),
        "hashes": {
            "record_keys": "SHA-256 of UTF-8 text, matching key_digest",
            "accepted_payload_hash": "Uppercase SHA-256 hex of SQL NVARCHAR UTF-16-LE bytes",
            "accepted_row_hash": (
                "Protected SQL-generated hash of all record columns, not caller data. "
                "Any payload or promoted-field change hides a fact until a new accepted binding."
            ),
            "opaque_identity": (
                "json_identity_string implements json.dumps(string, ensure_ascii=True), including "
                "UTF-16 surrogate escaping, without escaped slashes. Pair digests match models._digest."
            ),
            "incident_prior_hash": "Uppercase SHA-256 hex of original SQL NVARCHAR UTF-16-LE bytes",
        },
        "rpcs": {
            name: {
                "object_name": rpc.object_name,
                "parameters": [
                    {"name": p.name, "sql_type": p.sql_type, "nullable": p.nullable}
                    for p in rpc.parameters
                ],
                "components": list(rpc.components),
                "mutating": rpc.mutating,
                "implemented": rpc.implemented,
                "description": rpc.description,
                "blocked_cases": list(rpc.blocked_cases),
                "result_fields": list(rpc.result_fields),
                "not_acquired_fields": list(rpc.not_acquired_fields),
                "statuses": list(rpc.statuses),
            }
            for name, rpc in kernel.rpcs.items()
        },
        "payload_contracts": {
            "web.commit_intent.intent_kind": "preview | scope | review | discovery",
            "web.commit_intent.scope": (
                "ScopeDefinition JSON: tenant_id, epoch, scope_id, name, enabled, rules, cadence. "
                "No target/action projections. Scope/review commits increment current revision immediately."
            ),
            "web.commit_intent.review": (
                "review_id, target, action, requested_state(pending|verified|revoked), reviewer_id, "
                "expires_at, parameters, parameter_hash, definition_hash, configuration_hash, "
                "replay_safe, detail. No exact_correlation_verified or resolved technical state."
            ),
            "worker.accept_facts.facts_json": (
                "1-200 descriptors [{kind,key,revision,payload_hash}]. Rows must already exist in the "
                "worker view and the stored work's fact family under the same target/generation/work lease. "
                "Inventory generation routing uses the work ID. Acceptance writes protected all-column "
                "bindings and its receipt; controller reads exclude orphans or subsequently changed rows. "
                "Transport receipts and heartbeat facts have their own RPCs, never a raw-view bypass."
            ),
            "worker.accept_facts.collection": (
                "window_start_at/window_end_at are required for poll and null for other families. "
                "collection_complete is the producer's end-of-enumeration fact, not validation. "
                "Poll and inventory create a protected validation_window on the first accepted page. "
                "Pages after producer closure or controller window rejection are refused, except original receipt replay."
            ),
            "controller.inspect_frontiers": (
                "Returns the locked applicable global/target frontier snapshot, pending flag and uppercase "
                "UTF-16-LE frontier_digest. Copy the digest into protected controller_validation only after "
                "deterministic admission validation. reserve_action recomputes it for every origin, including "
                "mail/operator/event/poll and any later supported retry path."
            ),
            "controller.resolve_frontier": (
                "validation_id/hash reference an INSERT-only frontier_validation proof. The stored work must "
                "be reconcile_state; only its own work lease, current policy, immutable producer receipt, raw "
                "bindings and exact accepted frontier revision participate. No controller:{target} lease. "
                "The RPC does not complete work: pending_validation must retry/wait, not become technical success."
            ),
            "controller.resolve_frontier.whole_window_rejection": (
                "Use a NEW request/proof under the current policy/work fence with decision=rejected and "
                "reject_whole_window=true. This resolves the unfinished window through the exact committed "
                "frontier, without republishing obsolete pages or changing any immutable handoff decision. "
                "All original intake receipts must be present. Original pending receipts still replay unchanged."
            ),
            "controller.resolve_frontier.sibling_acknowledgement": (
                "For a terminal published or rejected window, use a NEW request/proof for the sibling's own current work "
                "lease/fence, current policy and exact frontier revision, with decision=rejected and "
                "reject_whole_window omitted/false. SQL verifies the protected original window outcome and "
                "returns that state (published is never mislabeled rejected), resolution_scope=window_acknowledgement, "
                "window_resolution_request_id/state and rejection-specific reference only for rejected windows. "
                "It changes neither page decisions nor window/frontier/commit "
                "state. Its own terminal receipt permits transition_work complete/disposition for that same "
                "work/fence. Earlier pending receipts remain immutable and cannot provide completion authority."
            ),
            "controller.publish_connector": (
                "publication_id/publication_hash reference INSERT-only connector_publication. The RPC uses "
                "only the reconcile_state work lease and current control/intent/frontier CAS. It creates a "
                "planned connector from an empty connector baseline, or publishes fixed desired fields with "
                "revision CAS; established ownership, resource IDs, endpoint and retained source bindings "
                "cannot be replaced. Optional readiness_receipt_id promotes only that exact owned worker "
                "observation after controller review; desired scope changes invalidate earlier readiness. "
                "Typed source_removals revoke desired membership but retain physical ownership. Only "
                "observation_receipt_id with original complete remote-absence evidence can retire it. "
                "The separate source_removal_supersessions path restores only never-submitted physical "
                "removals under fresh receipt-bound running inspection, current reviewed scope/read "
                "admission and terminal collection fencing; it never manufactures readiness."
            ),
            "worker.commit_positions.positions_json": (
                "1-200 [{receipt_kind:identified|unidentified, receipt_key, receipt:{partition,position,status,...}}]. "
                "Identified payload is a SignalReceipt with original opaque delivery source/id and transport "
                "authority only; receipt_key must match its original canonical delivery digest. Unidentified "
                "key is partition_key + ':unidentified:' + sequence and must be quarantined. Redelivery "
                "retains the first event receipt while recording each original broker offset/enqueued_at. "
                "Only received_at, observed_at, partition and position are excluded from event-content equality. "
                "Opaque source/id, event-type, receipt-key and offset equality uses BIN2 plus DATALENGTH; "
                "no case folding or SQL trailing-padding equivalence is allowed. Accepted membership excludes "
                "pending removals and requires current target observation admission. Original receipts replay "
                "before these new-operation checks. The existing connector-state/partition-lease gates still apply."
            ),
            "worker.partition": (
                "transition claim|renew|release|pin_start; expected_owner_id/fence plus "
                "expected_ownership_revision are required CAS evidence (null owner/fence only for first claim). "
                "The requested consumer_group must exactly match the configured endpoint using BIN2 plus "
                "DATALENGTH before any partition digest/key/result is derived. No case-folded or padded aliases. "
                "Returns full partition identity, active lease or null, ownership_revision, etag and last_owner_id/"
                "last_fence. The release tombstone uses the POST-mutation stored fence, not the released token's "
                "old fence. pin_start returns the full original start including unobserved-history gaps."
            ),
            "worker.observe_retention": (
                "Current partition lease plus expected checkpoint revision, actual first_available_sequence_number "
                "and nonfuture observed_at. Appends durable retention/regression evidence, returns full start/"
                "checkpoint/observation; never advances or rewinds the pinned boundary or checkpoint."
            ),
            "worker.commit_positions.result": (
                "Includes full partition, ordered receipt_keys and positions mapping (original offset/enqueued_at, "
                "receipt kind/key, first_committed_batch_id and original payload hash). Recover the new request "
                "from this immutable result, not journal.batch_id or caller-retained body."
            ),
            "worker.observe_connector.inspection": (
                "Optional observation_json.inspection is separate read-only evidence, never a manifest "
                "field or readiness claim. Exactly observed_at, read_only=true, definition_hash and "
                "component_states (3-1002 canonical physical GUID keys with value Running). The "
                "SQL NVARCHAR definition hash and complete physical ID set must match the original "
                "explicit snapshot; full node/ID binding is checked separately. Evidence "
                "is at most 300 seconds old. It is returned as optional result.inspection; original "
                "receipts without it replay unchanged and cannot authorize supersession."
            ),
            "controller.reserve_action.arguments": (
                "Full tool arguments and approval fingerprints are preserved. Closed SQL canonicalization permits "
                "only justification for powerbi_refresh; the real PipelineToolContext.approval_arguments shape "
                "(failed_run_id, justification, parameter_hash, parameter_preview, pipeline_id, workspace_id) "
                "for pipeline_rerun; and "
                "justification+configuration for gateway/schedule actions. Full arguments_hash differs from "
                "reviewed parameter/configuration hash. Power BI review.parameters must explicitly be null or {}; "
                "their hashes remain distinct. Pipeline IDs must match the exact reserved target/source; "
                "parameter_preview is preserved approval text, never technical authority. Other actions require "
                "the exact protected reviewed set."
            ),
            "controller.publish_source": (
                "Actual current work lease and typed SourceRunObservation. Reconcile work additionally names "
                "accepted evidence_kind/evidence_key and optional validated alias_window_id; source bytes must "
                "match that accepted window after verified alias normalization. Action work requires its actual "
                "controller target lease and REST authority. Existing reserved effects may refresh under changed "
                "policy/maintenance; new action authority is never granted by publication. Source/head are no "
                "longer writable through controller projections."
            ),
            "controller.disposition_source": (
                "Same typed source/work binding plus fixed non-effect disposition/detail. Atomically persists "
                "source disposition, processed marker and receipt, refusing any reservation/effect. Optional "
                "subject_work_id/expected_subject_revision supports exact unclaimed queued/waiting work cleanup "
                "under a real reconciliation lease. Owned non-effect source work is dispositioned atomically."
            ),
            "web.commit_intent.review_timestamps": (
                "reviewed_at is required and retained verbatim in original_intent; SQL acceptance time is separate. "
                "Revocation of an existing review must retain the original reviewed_at and expires_at even after "
                "expiry. Publication metadata stays explicit pending validation, not synthesized verification."
            ),
            "controller.enqueue_work.draft_json": (
                "MonitoringWorkDraft without state/revision/attempt/lease/retry/finalization fields. "
                "Controller may enqueue clean worker/triage work or exact existing-action follow-up, "
                "never update an existing row. deferred_retry only looks up the exact existing SQL-created "
                "successor by its returned work ID and matching source/target; it cannot create unlinked work."
            ),
            "controller.reserve_action.reservation_json": "{request: ActionReservationRequest, incident_id: canonical incident ID}",
            "controller.transition_action.transition_json": (
                "Only submitted_execution, submitted_at, next_verification_at, configuration, rejection, detail. "
                "A verified outcome requires a matching controller_validation row keyed by reservation_id. "
                "A rejected transition uses the original unsubmitted reservation owner and returns retry_work "
                "(nullable); an eligible powerbi_refresh/throttled rejection creates and links that work "
                "atomically before saving the original rejection receipt."
            ),
            "controller.transition_action.correlation": (
                "Every transition with an exact submitted_execution inserts or verifies immutable submitted_action "
                "correlation in the same transaction as the action update and receipt. Collision is checked both "
                "against other action.submitted_execution values and the index; no rebinding is allowed. The "
                "ActionOwner-shaped correlation payload remains immutable after terminal verification. An absent "
                "index never means no effect: disposition_source also checks action.submitted_execution directly."
            ),
            "controller.finalize.finalization_json": (
                "{plan_key: finalization_id, plan_hash: protected finalization_plan UTF-16-LE hash}. "
                "SQL derives historical classification from the exact source and protected incident chronology. "
                "For an older source it retains the original incident payload except its once-only occurrence_count "
                "increment, preserves newer latest_execution/latest_started_at and appends historical source metadata."
            ),
            "worker.observe_connector": (
                "Supply the actual CollectionCommit as mandatory work_id, owner_id, fence, work_revision "
                "after expected_revision. SQL requires the current leased connector_reconcile work for this "
                "connector and refuses target/execution/action/finalization lineage. Return connector "
                "(effective persisted state), observation (original reported snapshot), work_id, work_owner_id, "
                "work_fence, work_revision and SQL-derived collection_completion_eligible. Explicit ready "
                "topology or a blocked/degraded gap disposition can complete this collection; provisioning "
                "and submission/uncertain-only metadata cannot. "
                "Identity/delivery timestamps are observations, not writable readiness authority. A reported "
                "ready state cannot upgrade a non-ready connector; controller.publish_connector must publish "
                "the matched receipt. Worker cannot change desired fields, proposals, removals or established "
                "physical bindings. Required nullable observed_definition_hash is computed by SQL over the "
                "explicit observation_json.observed_definition NVARCHAR bytes. Null means this operation did "
                "not supply a definition; an inherited matching snapshot is not fresh observation evidence. "
                "transition_work complete accepts only an eligible original receipt with this connector, "
                "context, current policy and exact current work/owner/fence; same-fence renewal is allowed. "
                "A reclaimed work fence or policy change requires a new observation; the work's creation "
                "policy and the older receipt are not relabeled. Completion does not clear controller "
                "validation, publish readiness or borrow another work's accepted facts."
            ),
        },
        "controller_publication_contracts": {
            "frontier_validation": (
                "INSERT-only keyed by validation_id: work_id, lease_owner_id, lease_fence, expected_work_revision, "
                "policy_revision, frontier_key, through_revision, producer_request_id, producer_fingerprint, "
                "evidence_digest(from protected validation_handoff), decision(published|rejected), detail. "
                "Successful window closure additionally needs window_complete=true and the exact closing_request_id. "
                "Whole-window rejection instead uses decision=rejected and reject_whole_window=true, with a "
                "fresh current-policy proof. It preserves earlier page decisions, including a published page "
                "from an obsolete policy, and does not claim technical verification. SQL derives all "
                "validated revisions and requires every accepted handoff in the contiguous prefix to be "
                "acknowledged for publication, or every original intake receipt present for whole-window rejection. "
                "Per-page proof and producer completion alone never clear the fence."
            ),
            "nonwindow_handoff_acknowledgement": (
                "acknowledge_handoff is an optional strict JSON boolean on frontier_validation, mutually "
                "exclusive with window publication/rejection fields. Set decision to the unchanged stored "
                "terminal handoff decision and use the actual current work/policy/frontier fence. SQL derives "
                "the original own-handoff resolution and exact protected committed prefix, including every "
                "original producer receipt. Return resolution_scope=handoff_acknowledgement, handoff_revision, "
                "handoff_resolution_request_id/work_fence and frontier_resolution_request_id/revision. "
                "The validated prefix may precede later pending intake. Append only the current acceptance "
                "and operation receipt; never change an earlier handoff, receipt, frontier, window, policy, "
                "source, action, budget or approval. Complete/disposition validates both original references. "
                "The caller routes acknowledgement before stale publication callbacks. No window is created."
            ),
            "connector_publication": (
                "INSERT-only plan: connector_id, ownership_id, work_id, lease_owner_id, lease_fence, "
                "expected_work_revision, expected_connector_revision(0 only when absent), policy_revision, "
                "producer_request_id/fingerprint, frontier_key/revision, name, sources, source_proposals, source_removals, desired_definition, "
                "optional observation_receipt_id/readiness_receipt_id, detail. New sources are logical proposals "
                "with proposal_id/node_name/target/event_types/event_source and source_id=null, never fake IDs. "
                "sources contains all established bindings, including pending removals; omitting ownership "
                "is refused. observation_receipt_id resolves matching approved proposals and pending removals "
                "from the original complete definition/component_ids receipt, without caller-assigned IDs. "
                "Every unresolved proposal, new or existing, forbids a caller-supplied component_ids entry "
                "or nonnull source-node id without that receipt. An existing logical proposal is not physical "
                "ownership; its observed-but-unbound ID remains evidence only until receipt-derived binding. "
                "Readiness waits until no proposals or removals remain. No caller state, endpoint or proof timestamps. "
                "Definition is the existing normalized parts/eventstream.json document; only explicit per-item "
                "FabricJobEvents sources, one owned DefaultStream and one CustomEndpoint are accepted. Source "
                "targets require current controller observation/event capability. connector_desired records the "
                "protected desired-publication hashes/time; readiness must postdate that version."
            ),
            "reservation_validation": (
                "controller_validation keyed by work_id: verified=true, policy_revision, work_fence, "
                "source_key, parameter_hash, definition_hash when applicable, review_id, review_revision, expires_at; "
                "frontier_digest from controller.inspect_frontiers, exact_action_correlation for job actions "
                "and configuration_hash for configuration actions."
            ),
            "outcome_validation": (
                "controller_validation keyed by reservation_id: reservation_id, outcome(verified_succeeded|verified_failed), "
                "verified=true, work_fence, expires_at, submitted_execution and configuration when applicable."
            ),
            "finalization_plan": (
                "INSERT-only keyed by finalization_id: work_id, expected_work_revision, lease_owner_id, lease_fence, "
                "incident_id, incident_key, incident_identity, signature, source_key, source_execution, source_started_at, "
                "prior_incident_hash(null only for absent incident), merged_incident, source_disposition. "
                "The controller computes the existing validated candidate before publishing this immutable plan. "
                "SQL ignores a historical candidate's mutable evidence/terminal fields, patching the original "
                "SQL payload instead. incident_occurrence markers prevent double counting across pending-effect "
                "finalization, new receipt IDs and later verification."
            ),
        },
        "connector_removal_contract": {
            "request_field": "ConnectorPublicationRequest/Plan.source_removals: at most 1000 SourceRemovalIntent objects.",
            "source_removal_intent": {
                "removal_id": "Canonical nonempty lower-case GUID, immutable and never reused after retirement.",
                "source_id": "Exact owned opaque physical ID, or explicit null for a proposal withdrawal.",
                "proposal_id": "Exact owned logical proposal GUID, or explicit null for physical removal.",
                "detail": "Nonempty string, at most 4000 NVARCHAR bytes; immutable for this removal ID.",
            },
            "selector_rule": (
                "Exactly these four fields, with exactly one nonnull selector. The original source/proposal "
                "must already belong to the same connector. A new removal cannot be introduced while "
                "reconciling an earlier observation receipt."
            ),
            "pending_manifest_fields": (
                "OwnedConnectorManifest.source_removals contains server-derived PendingSourceRemoval: "
                "removal_id, source_id, proposal_id, node_name, last_observed_source_id(nullable), target, "
                "binding_hash, policy_revision, request_id, publication_id, requested_at, detail, "
                "state=pending_remote_absence. binding_hash is uppercase SHA-256 of the original binding's "
                "SQL NVARCHAR bytes. last_observed_source_id is deny-only remembered evidence, never a "
                "materialized physical identity for an unbound proposal."
            ),
            "ownership_and_desired": (
                "Request.sources must equal all prior ownership in original order. Prior unresolved "
                "proposals and pending removal identities cannot be omitted or changed except through the "
                "separate proved never-submitted supersession path. SQL retains original "
                "binding bytes/order and appends only new proposals. Effective desired sources are "
                "(owned sources minus pending physical removals) plus (proposals minus pending withdrawals). "
                "The reviewed desired graph is validated against this effective set, not the ownership list."
            ),
            "worker_path": (
                "Plan remote changes using BOTH retained ownership and effective desired topology. Use only "
                "the published current intent. After an uncertain update acknowledgement, recover the original "
                "operation and read the remote definition/topology; do not repost by default. Record the "
                "explicit complete snapshot through worker.observe_connector. The worker cannot clear "
                "removals, change ownership or publish readiness."
            ),
            "confirmation": (
                "controller.publish_connector observation_receipt_id must equal its reconcile work's original "
                "producer request and match current tenant/epoch/policy/ownership/connector revision and "
                "source/proposal/removal intent. The protected observed_definition_hash must match the exact "
                "explicit snapshot. Require matching approved parts and a complete one-to-one canonical "
                "node/ID map for all sources, streams and destinations. Every removed node, stream input, "
                "old physical ID and remembered observed ID must be absent; renamed/rebound IDs are refused. "
                "Retained resource/component IDs cannot change. Confirmation is all-or-none for this desired "
                "version: incomplete, blocked, stale or mismatched observations leave all ownership retained."
            ),
            "retirement": (
                "In the caller-owned transaction append connector_source_retirement records, update the "
                "connector with only proved retirements/materializations, and save the original controller "
                "receipt. A receipt failure rolls back every change. Tombstone full_key is "
                "connector_id + ':removal:' + removal_id; parent_key is connector_id. Only the static "
                "publication RPC inserts them; controller_read exposes them without any runtime write grant."
            ),
            "retirement_fields": (
                "connector_id, ownership_id, removal_id, source_id, proposal_id, node_name, original_binding, "
                "original_removal, observation_receipt_id, observation_fingerprint, observation_binding_hash, "
                "observation_receipt_hash, observed_definition_hash, confirmation_request_id, work_id, "
                "work_fence, policy_revision, retired_at, state=retired_verified. Receipt/definition hashes "
                "are SQL-computed uppercase NVARCHAR SHA-256, not caller assertions."
            ),
            "publication_result_fields": {
                "pending_removals": "The resulting immutable pending-removal entries; equals connector.source_removals.",
                "retired_sources": "Only the retirement tombstones committed by this operation, including logical withdrawals.",
                "observation_receipt_id": "The original observation used for binding/retirement, or explicit null.",
                "superseded_source_removals": (
                    "Complete unchanged original PendingSourceRemoval objects superseded only by this "
                    "operation. New ordinary results emit []; historical immutable receipts may omit "
                    "the field. This is original intent history, not a retirement claim."
                ),
            },
            "model_and_adapter_delta": (
                "Add SourceRemovalIntent, PendingSourceRemoval and ConnectorSourceRetirement DTOs; forward "
                "source_removals in original request fingerprints and immutable plans with explicit null "
                "selectors. Add result fields and nullable ConnectorObservationResult.observed_definition_hash. "
                "Validate definition sources against effective desired membership while retaining all physical "
                "ownership. Require a not-ready state while proposals/removals remain. Verify result changes "
                "against the original receipt and returned retirements/materialized proposals; neither exact "
                "request.sources==result.sources nor unconditional acceptance is correct."
            ),
            "preserved_boundaries": (
                "Same 27 RPCs, 50 objects and 56 runtime grants. Connector observation additionally requires "
                "its four explicit collection work/fence parameters. No base-table DML, "
                "new role or generic setter. No action, approval, budget, frontier, partition or checkpoint "
                "mutation from retirement. Original operation replay does not re-raise removal or intake fences. "
                "Pending removal cannot be silently cancelled. Already dispatched or uncertain removal "
                "must reconcile through the existing remote-absence path; reenrolment after retirement "
                "requires a new reviewed proposal. Intake may pause under the existing provisioning-state gate; "
                "this contract does not claim continuous receiver/checkpoint liveness during reconfiguration."
            ),
        },
        "connector_supersession_contract": {
            "request": (
                "Request/Plan.source_removal_supersessions contains at most 1000 exact "
                "{removal_id,source_id} selectors. Both identities are unique, source_id is nonnull "
                "and at most 512 NVARCHAR bytes. Select only existing pending physical removals; "
                "keep all original source/proposal ownership and all nonselected removal intents. "
                "Selectors are disjoint from source_removals. No mixed additions, bindings or retirements."
            ),
            "original_evidence": (
                "The global observation_receipt_id must be this reconcile_state work's original worker "
                "producer. SQL decodes its protected worker_reconcile_request.request_payload using "
                "OPENJSON NVARCHAR(MAX), verifies the original receipt binding/hash, exact current "
                "connector revision/context and explicit snapshot/inspection, and checks the original "
                "removal publication. No inherited snapshot, caller proof flag, partial map or gap text "
                "can substitute. Every observed physical source must match retained ownership, node "
                "identity, target and event set; all currently desired source nodes must remain present "
                "unchanged apart from their separately verified optional IDs. The complete observed "
                "graph remains operator-free and routes each source exactly once through the owned "
                "stream. Inspection must postdate removal and be at most 300 seconds old."
            ),
            "never_submitted": (
                "Each connector revision since each original removal must have exactly one retained "
                "publication/observation receipt with unchanged ownership/removal lineage. Missing or "
                "ambiguous history blocks recovery. Any applicable original write-ahead submission "
                "gap or operation ID, including one inherited by the removal publication, forbids "
                "supersession even after a later clean presence observation. This path does not "
                "adjudicate definitive-not-applied effects or pre-boundary writers."
            ),
            "scope_and_work": (
                "Require current observation admission (reviewed or auto_detection_only, never pending_review), "
                "explicit enabled tenant/workspace/item inclusion, "
                "no matching exclusion or unresolved domain exclusion, and fresh current service "
                "read capability. Observation admission is not action approval and this publication changes "
                "neither target action authority nor remediation budgets. Only unknown/missing event status may be waived for selected retained "
                "sources; explicit denial and new sources keep ordinary gates. Original collection "
                "must be completed under its exact released owner/fence and completion receipt. "
                "Active or nonterminal previously attempted connector work blocks supersession. "
                "A queued candidate must have attempts=0, retry_attempt=0, no execution/action/retry/"
                "finalization lineage, and no lease payload or physical lease row, including an expired tombstone."
            ),
            "atomic_publication": (
                "Under the existing control/connector/work/frontier transaction, disposition only "
                "never-claimed queued connector work with this publication request ID in its disposition, "
                "publish a new desired revision/time, clear "
                "identity/delivery proof and retain physical resources. Return all original removal "
                "objects in the new immutable receipt without changing old receipts or labelling "
                "present sources retired. A receipt failure rolls back the whole change."
            ),
            "intake_and_readiness": (
                "Protected connector_desired.supersession_request_id retains the first recovery "
                "receipt across later ordinary and supersession publications. Require complete unique "
                "connector-revision receipts from that anchor and check every original supersession "
                "audit for still-owned exact physical source/target bindings; a later recovery cannot "
                "erase an earlier source's fence. Verified retirement of the old physical binding does "
                "not fence a subsequently admitted different source ID. Restored targets cannot "
                "accept new events merely because a worker later reports degraded: require current "
                "enabled observation admission, verified read/event "
                "capability checked after publication, and matched post-publication transport identity "
                "and nonfuture receive/enqueue times. Do not require events_enabled, which is published "
                "only after readiness; the first qualifying event supplies delivery proof. "
                "Readiness still requires the separate original accepted delivery "
                "proof. For a marked connector, readiness additionally requires the current matching "
                "capability checked_at and the original worker.commit_positions receipt's native "
                "recorded_at to fall between current publication and SQL now; future self-reported "
                "event times cannot rebind an older accepted batch. Original operation replay remains "
                "immutable. Quarantine, ordinary source intake and action budgets are unchanged."
            ),
        },
        "linked_retry_contract": {
            "creation": "Only controller.transition_action(rejected) creates a clean linked successor; no extra RPC or caller retry flags.",
            "eligibility": "Original owner, definitive no-effect throttled Power BI rejection, current epoch/policy/scope/review/capability. Other 4xx, exhaustion and revoked/expired policy produce no successor without undoing the rejection.",
            "attempts": "Original retry_attempt=0; existing MAX_ATTEMPTS=3 permits deferred attempts 1,2,3 (at most four total reservations).",
            "due_time": "SQL now + positive Retry-After, otherwise the existing backoff_seconds(1..3): 900/1800/3600 seconds.",
            "dequeue": "Wait for the predecessor work's completed finalization, then current admission; revoked/expired retries are dispositioned without touching budgets or approvals.",
            "reservation": "The exact unused predecessor retry_work_id and matching technical action/source/incident/parameter/configuration/definition identity, current work lease and all ordinary fresh guards are required. Free justification may change per attempt; each attempt's full arguments are independently canonicalized/fingerprinted and any fresh approval must match them. Parent retry_reservation_id is single-assignment in the same transaction.",
            "budget": "Keep the existing incident action_count slot (no debit for a linked successor, no refund). Each invocation still permits only one reservation.",
            "approval": "Never copy or reuse a consumed approval. Power BI's existing approval policy is unchanged; if a new request supplies an approval, ordinary fresh matched unconsumed approval checks still apply.",
        },
        "integration_gates": [
            "Create the physical baseline, then deploy this role/view/procedure catalogue and exact grants.",
            "Use actual distinct component SQL users; remove only reviewed legacy broad grants after cutover proof.",
            "Replace existing base-table routing and U-only runtime schema checks; never keep them as fallback.",
            "All checked-view writes also acquire lock_context in the same existing transaction; raw facts remain non-authoritative until their acceptance RPC commits.",
            "Controller reconciliation/publication and API accepted/configuring adapters remain separate integration work.",
            "Core controller dispatch must call work_policy on the stored work before ordinary exact-source validation; dispatch reconcile_state directly to deterministic reconciliation, including targetless tenant/domain/review/inventory requests.",
            "validation_frontier/window/handoff, frontier_commit and reconcile_acceptance are kernel-only mutations. Controller publication writes its immutable frontier_validation proof and calls resolve_frontier; do not use the old generic record route.",
            "Single-target stream batches raise a target frontier; mixed or quarantined batches conservatively raise a tenant-wide frontier. Connector observations also require reconciliation; heartbeats and checkpoints do not clear or advance validation.",
            "After any new accepted intake, even if the producer completes, new reservations need a fresh matching frontier digest and a committed correlated controller resolution with applicable full-window validation or durable rejection. Reserved effects retain verification/finalization without these new-action gates.",
            "Controller desired-connector creation/update/readiness uses publish_connector; worker.observe_connector records evidence only. Replace any worker-side ready/desired writes with this explicit receipt-bound controller publication path.",
            "Source removal is a two-stage controller intent/receipt-verified retirement, not a sources-list deletion. Forward the fixed removal DTOs, preserve ownership during uncertain remote updates, and adopt only receipt-explained result changes. Binding and readiness remain separate publications.",
            "Use action-specific technical validation while preserving full arguments. Power BI explicitly supports its valid null/{} review contracts with distinct hashes; pipeline/configuration parameters cannot be null or redacted.",
            "Provision the approved rate bucket policies with budget_policy_statements; runtime cannot initialize limits.",
            "Use updated returned work/partition/receipt JSON contracts; no caller assumes old EXEC rowcount or old payload shape.",
            "On an uncertain claim commit, read the existing work/lease and reconcile its exact owner/fence rather than blindly claiming again. An uncertain rate debit remains spent.",
            "reserve_action consumes its bound approval atomically; do not pre-call consume_approval for that path. finalize also completes terminal work and releases its leases.",
            "record_action_rejection adapters must use the returned reservation/retry_work from transition_action; creation, fence release and receipt are one atomic RPC. Do not mint a second retry from a generic deferred draft.",
            "The existing Teams callback needs an explicitly reviewed integration/grant to the decision surface; no fourth broad role is created automatically.",
            "Native SQL role/DDL/transaction proof is still required; offline generation tests are not platform acceptance.",
            "Kernel envelope version is 2 for this coherent revision. Archive ab377 remains unchanged; old evidence is not relabeled. Native03 created/bound the old objects then rolled back after unsupported principal syntax; no role/updatability/procedure-execution acceptance is claimed.",
        ],
        "public_sources": [
            "https://learn.microsoft.com/en-us/sql/t-sql/statements/create-view-transact-sql?view=fabric-sqldb",
            "https://learn.microsoft.com/en-us/sql/relational-databases/security/authentication-access/database-level-roles?view=fabric-sqldb",
        ],
        "unresolved_cases": [{"operation": name, "reason": reason} for name, reason in kernel.unresolved_cases],
        "native_sql_proven": False,
    }
