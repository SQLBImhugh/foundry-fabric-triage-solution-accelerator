"""Synchronous durable-store boundary shared by monitoring adapters and controllers.

Live implementations use Azure SQL transactions, unique constraints and fenced
conditional writes. They never initialize/upgrade a schema or fall back to local
state. An explicit fixture implementation may use an injected deterministic clock.
All persisted evidence is redacted here, not by callers. Serialize records using
``model_dump(mode="json")`` or ``model_dump_json()``, including nested Incidents.
Revalidate records at this boundary; frozen Pydantic models do not make nested
JSON dictionaries immutable. Preserve original fingerprints before redaction.

Human roles remain validated Entra app-role claims at the API/controller boundary.
There is no SQL human ACL. This contract adds no agent, policy ledger or action
executor: the existing controller remains the only workload-action component.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from triage.models import Incident
from triage.monitoring.models import (
    ActionOutcomeRequest,
    ActionRejectionRequest,
    ActionReservation,
    ActionReservationDecision,
    ActionReservationRequest,
    ActionSubmissionRequest,
    ActivateScopeRequest,
    ActivationPlan,
    ActivationReceipt,
    ApprovalBinding,
    BootstrapInspection,
    CanonicalId,
    CapabilityObservation,
    CollectionCommit,
    ConnectorDeliveryProof,
    ConnectorDesiredState,
    ConnectorPublicationContext,
    ConnectorPublicationRequest,
    ConnectorPublicationResult,
    CoverageView,
    FinalizationReceipt,
    IncidentIdentity,
    IncidentState,
    IntakeReceipt,
    InventoryBatch,
    InventoryDomain,
    InventoryGeneration,
    InventoryItem,
    InventoryWorkspace,
    LeaseRenewal,
    LeaseToken,
    MonitoringContext,
    MonitoringSnapshot,
    MonitoringTarget,
    MonitoringWork,
    MonitoringWorkDraft,
    OperationReceipt,
    OwnedConnectorManifest,
    PageQuery,
    PartitionClaimRequest,
    PartitionIdentity,
    PollProgress,
    PowerBIAliasState,
    PowerBIWindowState,
    ProcessedSourceRecord,
    ProducerComponent,
    ReconcileStateRequest,
    ReconciliationRequest,
    ReconciliationResult,
    RecordPage,
    RegistryVersion,
    RestCheckpoint,
    RestPageReceipt,
    RestPageRequest,
    Revision,
    RuntimeComponent,
    SafetyReview,
    SafetyReviewOperationReceipt,
    SafetyReviewRequest,
    ScopePolicy,
    ScopePreviewRequest,
    ScopeSelector,
    SourceExecutionIdentity,
    SourceRunObservation,
    StreamCheckpoint,
    StreamCheckpointAdvance,
    StreamReceiptBatch,
    TargetIdentity,
    TargetQuery,
    ValidationFrontier,
    WorkClaimRequest,
    WorkDispositionRequest,
    WorkFinalizationRequest,
)


class MonitoringStoreError(RuntimeError):
    """A visible monitoring persistence failure, never a successful empty result."""


class MonitoringUnavailable(MonitoringStoreError):
    """Shared state could not be reached; intake, ownership and actions fail closed."""


class MonitoringNotBootstrapped(MonitoringStoreError):
    """Deployment tooling has not created the current baseline."""


class MonitoringSchemaMismatch(MonitoringStoreError):
    """The runtime and deployment schema cannot operate together."""


class MonitoringConflict(MonitoringStoreError):
    """The epoch/revision or idempotent request content no longer matches."""


class MonitoringLeaseLost(MonitoringConflict):
    """The database-time lease expired or another owner won a newer fence."""


class MonitoringComponentDenied(MonitoringStoreError):
    """The selected component cannot request this state transition."""


class MonitoringKernelUnsupported(MonitoringStoreError):
    """A required guarded SQL operation is unavailable; no base-table fallback exists."""


class MonitoringCommitUncertain(MonitoringStoreError):
    """Re-read the exact operation receipt; a timeout does not establish rollback."""

    def __init__(self, operation: str, idempotency_id: str) -> None:
        self.operation = operation
        self.idempotency_id = idempotency_id
        super().__init__(f"Commit uncertain for {operation}; reconcile receipt {idempotency_id}")


class ConnectorPublisher(Protocol):
    """Trusted synchronous controller composition, never a model-supplied callback."""

    def __call__(
        self, store: MonitoringStore, context: ConnectorPublicationContext,
    ) -> ConnectorPublicationResult | None: ...


@runtime_checkable
class MonitoringStore(Protocol):
    """The SQL and explicit offline-fixture implementations share these semantics.

    Every runtime operation checks bootstrap, pinned tenant and epoch. Reads used
    for admission are current shared-state reads, never process caches. Revision
    conflicts, missing bootstrap and unavailable SQL raise the typed errors above.
    Same idempotency ID and same validated content return the original receipt;
    different content under that ID raises MonitoringConflict.

    Only deployment tooling owns DDL, reset and initial control creation. There
    is deliberately no bootstrap, migration, credential or human-ACL method.
    """

    component: RuntimeComponent

    def inspect_bootstrap(self, *, expected_tenant_id: CanonicalId) -> BootstrapInspection:
        """Inspect without DDL; an unreachable database raises MonitoringUnavailable."""
        ...

    def snapshot(self, context: MonitoringContext) -> MonitoringSnapshot:
        """Read control and coverage at one consistent registry revision."""
        ...

    def list_scopes(self, query: PageQuery) -> RecordPage[ScopePolicy]: ...

    def preview_scope(self, request: ScopePreviewRequest) -> ActivationPlan:
        """Persist an expiring dry run without Fabric writes, grants or workload actions.

        Resolve all overlapping policies with exclusions winning. Bind the plan
        to its inventory generations and revision; unknown inventory cannot delete
        known items. New/automatic admissions are detection-only. Surface all
        incomplete enumeration, unsupported items and required service permissions.
        """
        ...

    def get_plan(self, context: MonitoringContext, plan_id: CanonicalId) -> ActivationPlan | None: ...

    def activate_scope(self, request: ActivateScopeRequest) -> ActivationReceipt:
        """Atomically validate the stored plan, expiry, idempotency and current revision.

        Commit accepted intent, its immutable reconciliation handoff and receipt;
        increment the protected registry revision immediately. Return configuring
        until the controller publishes admission. The producer cannot publish
        targets or reschedule existing controller work.
        """
        ...

    def get_activation(
        self, context: MonitoringContext, idempotency_id: CanonicalId,
    ) -> ActivationReceipt | None: ...

    def record_inventory(self, batch: InventoryBatch) -> InventoryGeneration:
        """Commit this page/generation with retained known inventory and explicit gaps.

        A denied/interrupted/budget-limited scan cannot establish deletion. The
        controller reconciles admissions from accepted inventory or explicit policy;
        the worker cannot publish targets, probes or action authority.
        Live batches require InventoryCommit: validate current work identity,
        lease/fence, work revision and the expected generation revision/cursor in
        this transaction. Only the explicitly constructed fixture backend permits
        unowned seeding. Store-assigned generation revisions and catalogue counts
        describe the committed snapshot.
        """
        ...

    def list_inventory(
        self, query: TargetQuery, *, generation_id: CanonicalId | None = None,
    ) -> RecordPage[InventoryItem]:
        """Read the latest UI projection or one retained generation, never filter latest into a snapshot."""
        ...

    def list_workspaces(
        self, query: PageQuery, *, generation_id: CanonicalId | None = None,
    ) -> RecordPage[InventoryWorkspace]:
        """Read named containers, optionally from a retained generation, outside workload counts."""
        ...

    def list_domains(
        self, query: PageQuery, *, generation_id: CanonicalId | None = None,
    ) -> RecordPage[InventoryDomain]:
        """Read latest named domains or exactly the requested retained generation."""
        ...

    def get_inventory_generation(
        self, context: MonitoringContext, generation_id: CanonicalId,
    ) -> InventoryGeneration | None: ...

    def record_capability(
        self, expected: RegistryVersion, observation: CapabilityObservation,
        *, commit: CollectionCommit | None = None,
    ) -> CapabilityObservation:
        """Accept fenced raw probes, not target/action authority.

        Worker calls require the current capability work lease/revision. Only
        explicit fixture setup may omit commit. Controller reconciliation publishes
        target_capability after checking the immutable accepted evidence binding.
        """
        ...

    def record_connector(
        self, expected: RegistryVersion, manifest: OwnedConnectorManifest,
        *, expected_connector_revision: Revision, commit: CollectionCommit | None = None,
    ) -> OwnedConnectorManifest:
        """CAS restricted worker observations of an existing controller-owned topology.

        Return effective persisted state, not the reported readiness assertion.
        A new reported ready state remains provisioning until receipt-bound
        controller publication. Ownership never transfers by matching a label.
        Runtime observations require the actual connector collection work lease
        and revision. The original receipt alone establishes completion eligibility;
        it cannot publish or clear the controller frontier. Fixture setup may omit commit.
        """
        ...

    def list_connectors(self, query: PageQuery) -> RecordPage[OwnedConnectorManifest]: ...

    def get_connector_desired(
        self, context: MonitoringContext, connector_id: CanonicalId,
    ) -> ConnectorDesiredState | None:
        """Read protected controller publication, distinct from registered metadata."""
        ...

    def get_connector_delivery(
        self, context: MonitoringContext, connector_id: CanonicalId, collector_identity_id: CanonicalId,
    ) -> ConnectorDeliveryProof | None:
        """Read original accepted stream evidence for this current owned definition.

        Revalidate the immutable intake, broker position, source, endpoint, pinned
        identity and current desired-publication boundary. Quarantine, old policy,
        cache counters and receiver heartbeats never establish delivery.
        """
        ...

    def publish_connector(self, request: ConnectorPublicationRequest) -> ConnectorPublicationResult:
        """Controller-only desired-manifest CAS under the reconciliation work lease.

        Derive producer/frontier provenance from protected state, not caller claims.
        Connector resource bindings remain unchanged. Retain source ownership in
        sources/source_proposals; source_removals excludes desired membership
        immediately without releasing those IDs. Only an original complete
        observation receipt can bind proposals or retire exactly absent sources.
        Its result can therefore differ from the requested source collections.
        Readiness is a separate original-receipt publication against the current
        desired definition; worker observations alone never create new readiness.
        Metadata-only registration stays dormant until its first nonempty,
        admitted event-source publication. That first publication is never an
        unchanged no-op and cannot retire retained physical sources.
        """
        ...

    def get_connector_publication(
        self, context: MonitoringContext, request_id: CanonicalId,
    ) -> ConnectorPublicationResult | None:
        """Read the immutable original publication, never the latest connector."""
        ...

    def resolve_target(
        self, identity: TargetIdentity, *, include_inactive: bool = False,
    ) -> MonitoringTarget | None:
        """Return current admission by default; inactive records allow read-only follow-up."""
        ...

    def list_targets(self, query: TargetQuery) -> RecordPage[MonitoringTarget]: ...

    def coverage(self, context: MonitoringContext) -> CoverageView: ...

    def enqueue_work(self, work: MonitoringWorkDraft) -> MonitoringWork:
        """Idempotently enqueue under current admission, epoch and activation cutoff.

        Deduplicate exact source execution across poll/event/mail/operator/retry.
        Separate executions still share the canonical incident budget. Already
        submitted actions may enqueue read-only verification after scope disable.
        Inventory work uses the typed discovery_selector or an existing scope_id;
        an absent selector never expands to an unconstrained scan.
        """
        ...

    def get_work(self, context: MonitoringContext, work_id: CanonicalId) -> MonitoringWork | None: ...

    def claim_work(self, request: WorkClaimRequest) -> tuple[MonitoringWork, ...]:
        """Claim bounded, due work fairly across workspaces and command/intake classes.

        Use database time, conditional ownership and increasing fencing tokens;
        respect shared per-workspace shares across replicas. Recheck current
        admission at dequeue. Expired effectful work resumes verification or
        finalization, never another POST.
        """
        ...

    def renew_lease(self, request: LeaseRenewal) -> LeaseToken:
        """Renew only the current owner/fence, using database time."""
        ...

    def disposition_work(self, request: WorkDispositionRequest) -> MonitoringWork:
        """Atomically defer or disposition non-executing work and its source receipt.

        Preserve action fences. A controller terminal result, crash or refusal
        that needs an Incident must use finalize_work instead. Historical evidence
        counts an occurrence without overwriting newer evidence or reopening a
        verified resolution. Rejection consumes no approval or remediation budget.
        Before any reservation, deterministic historical/out-of-scope/unsupported/
        cancelled/superseded evidence may atomically disposition source work and
        its processed marker without inventing an Incident. It cannot dispose a
        submitted action execution or replace verification/finalization.
        """
        ...

    def record_rest_page(self, request: RestPageRequest) -> RestPageReceipt:
        """Atomically persist every raw row, immutable handoff and validation frontier
        before advancing producer poll_progress under the current poll owner/fence.

        Persist the page ID for ambiguous-commit recovery. A partial page can move
        continuation, never the completed-window watermark. A retention gap is
        incomplete coverage. Do not authorize a target from the page's own claims.
        Power BI uses typed powerbi_rows and a powerbi_window_complete boundary.
        Persist raw rows across every page/chunk. Only the controller publishes
        bidirectional aliases, rest_checkpoint and source work after terminal
        window validation. The original intake receipt remains pending_validation;
        read the current published projection separately.
        Conflicts/unresolved identities quarantine the window and cannot advance
        complete coverage. Alias state never travels in an opaque continuation.
        """
        ...

    def get_rest_page(
        self, context: MonitoringContext, page_id: CanonicalId,
    ) -> RestPageReceipt | None: ...

    def get_rest_checkpoint(self, target: TargetIdentity) -> RestCheckpoint | None: ...

    def get_poll_progress(self, target: TargetIdentity) -> PollProgress | None:
        """Raw collector cursor, separate from the controller-validated REST watermark."""
        ...

    def get_powerbi_window(
        self, context: MonitoringContext, window_id: CanonicalId,
    ) -> PowerBIWindowState | None: ...

    def list_powerbi_aliases(
        self, query: PageQuery, *, window_id: CanonicalId,
    ) -> RecordPage[PowerBIAliasState]: ...

    def claim_partition(self, request: PartitionClaimRequest) -> LeaseToken | None:
        """Arbitrate the SQL-backed consumer partition lease with database time."""
        ...

    def record_stream_receipts(self, request: StreamReceiptBatch) -> IntakeReceipt:
        """Commit receipts/quarantine, immutable reconciliation work and the protected
        deny-only frontier before any checkpoint. No producer source/head publication.

        Verify owned connector provenance, scope, tenant/epoch and cutoff. Original
        event source+ID deduplicates transport; source execution deduplicates work.
        A quarantine is durable but never authorizes investigation/remediation.
        Its pending-validation receipt may identify controller-owned reconcile_state
        work, which only validates/rejects intake and cannot call an agent or act.
        Pending source removal denies new acceptance even if a later worker
        observation reports degraded health. Persist such input as quarantine,
        retaining evidence and ownership; replay original receipts before this check.
        """
        ...

    def get_stream_acceptance(
        self, context: MonitoringContext, request_id: CanonicalId,
    ) -> IntakeReceipt | None:
        """Recover every original accepted position, including positions replayed in a new batch.

        None means no original native receipt exists. V2 reads the complete original
        positions/receipt_keys mapping from that receipt, never from a position's
        first batch_id or a caller-retained body. Missing or inconsistent mappings
        fail closed rather than returning an incomplete receipt or fake absence.
        """
        ...

    def advance_stream_checkpoint(self, request: StreamCheckpointAdvance) -> StreamCheckpoint:
        """CAS only through contiguous durably accepted/quarantined partition positions.

        Validate the current lease/fence and expected checkpoint revision. A later
        accepted batch cannot skip an earlier uncommitted event. Ambiguous intake
        commits must be reconciled before advancing; agent completion is irrelevant.
        """
        ...

    def get_stream_checkpoint(self, partition: PartitionIdentity) -> StreamCheckpoint | None: ...

    def record_safety_review(self, request: SafetyReviewRequest) -> SafetyReview:
        """        Accept requested safety intent, immediately fence new actions and enqueue
        immutable controller reconciliation. Return pending_validation with the
        original requested_state; only the controller publishes technical proof.

        Preserve reviewed parameters until persistence redaction; bind verification
        to definition/parameter fingerprints and revision. Never copy attestation
        onto automatically discovered items. This is not a human-permission grant.
        Revocation is deny-only and remains valid after the old expiry. Retain
        original reviewed_at and expires_at; the operation receipt's recorded_at
        is the separate SQL acceptance time. Never backdate review time or extend
        expiry to make revocation fit a validity window.
        If redaction removes replay values, retain the original parameter_hash,
        set parameters=None/parameters_redacted=True and mark the review unverifiable;
        redaction placeholders must never become executable replay parameters.
        """
        ...

    def get_safety_review(
        self, context: MonitoringContext, review_id: CanonicalId,
    ) -> SafetyReview | None: ...

    def get_safety_review_operation(
        self, context: MonitoringContext, request_id: CanonicalId,
    ) -> SafetyReviewOperationReceipt | None:
        """Read the original committed operation, never reconstruct it from the latest review.

        Return the immutable redacted result and its request/revision binding.
        Absence means not observed, not proof that an uncertain transaction rolled back.
        """
        ...

    def bind_approval(self, request: ActionReservationRequest) -> ApprovalBinding:
        """Bind the real still-unanswered approval before publishing it to a human.

        This records immutable epoch/policy/review/source provenance, not consent
        or a fabricated approval revision. It requires the existing approval row
        and current work ownership. Publish/wait only after binding succeeds.
        SQL joins lease-checked publication of the exact recorded REST source to
        its immutable binding insertion in one transaction; a partial pair cannot
        be repaired by replay under a different owner.
        reserve_action checks the binding and consumes the actual approval row in
        the reservation transaction; callers must not pre-consume it.
        """
        ...

    def reserve_action(self, request: ActionReservationRequest) -> ActionReservationDecision:
        """The atomic pre-POST linearization point shared by Power BI and pipelines.

        In ONE transaction validate current tenant/epoch, cutoff, maintenance,
        effective scope/action admission, exact source eligibility, policy and
        definition/parameter/safety revisions, review expiry/revocation, work
        owner/fence and target-level ownership, explicit matched unused unexpired
        approval and its immutable monitoring binding when controller policy requires it, and the durable
        incident's remaining budget. Persist reservation, approval consumption and
        budget use together ONLY when accepted. Caller fields cannot assert consent
        or a remaining budget. Serialize scope/safety changes against this operation.
        Pending accepted intent or raw-window validation blocks every new action,
        independently of producer work completion or missing first projections.

        Preserve the actual caller's complete approval arguments, including pipeline
        identity and parameter_preview. A linked retry may change its justification,
        but not its source/incident or definition/parameter/configuration identity.
        Each gated attempt needs its own full-argument approval binding; comparing
        technical identity never licenses reuse of the predecessor's approval hash.

        If revocation wins there is no new action; if reservation wins it remains
        committed/in-flight. A subsequent disable cannot retract an external POST.
        Never expire/release an uncertain effect to manufacture a new opportunity.
        """
        ...

    def get_action_reservation(
        self, context: MonitoringContext, reservation_id: CanonicalId,
    ) -> ActionReservation | None: ...

    def get_action_by_request(
        self, context: MonitoringContext, idempotency_id: CanonicalId,
    ) -> ActionReservationDecision | None: ...

    def record_action_submission(
        self, request: ActionSubmissionRequest, *, commit: CollectionCommit | None = None,
    ) -> ActionReservation:
        """Persist the fenced submission or uncertainty even if scope was since disabled.

        Match the existing reservation/target and CAS its revision/fence. Keep the
        source failure separate from the controller's submitted execution. Missing
        exact correlation stays uncertain; never use the first unseen refresh.
        Runtime callers must provide their current work_id, lease and revision in
        commit; omission is allowed only by the explicit fixture implementation.
        """
        ...

    def record_action_rejection(self, request: ActionRejectionRequest) -> ActionReservation:
        """Atomically record a definitive no-effect response from the original owner.

        Only reserved, never accepted/uncertain/verified, actions may be rejected.
        Retain the incident budget slot and any consumed approval; release only
        target mutation ownership. A throttled Power BI refresh may create one
        bounded successor work item linked to this rejection. That successor
        reuses this slot only after the parent's durable finalization and fresh
        scope/source/review/lease checks. Never mint a fresh source opportunity.
        The existing three-deferred-attempt cap and exponential backoff apply;
        an initial deferred invocation is already attempt one. Retry-After wins
        when supplied. Ordinary 4xx and configuration rejections have no
        automatic successor. Every invocation still spends its PolicyLedger
        attempt, and an approval is never unconsumed or transferred to a retry.
        The SQL adapter validates transition_action.retry_work (nullable), but
        never creates a successor itself. Deferred enqueue only retrieves the
        exact work ID that the rejection already recorded.
        """
        ...

    def record_action_outcome(
        self, request: ActionOutcomeRequest, *, commit: CollectionCommit | None = None,
    ) -> ActionReservation:
        """Match evidence to the reservation's exact submitted execution before update.

        Preserve existing workload-specific completion/activity checks. Uncertainty
        cannot authorize another POST. This write alone does not finish controller
        work: its Incident and processed-source outcome still require finalization.
        Exact REST pipeline cancellation with complete activity evidence is
        verified_failed while the observation retains status=cancelled. This
        does not infer Power BI cancellation support. Release only the terminal target action owner;
        preserve the incident budget, approval consumption and source/submitted IDs.
        Runtime callers supply the same current-work commit envelope as submission,
        including when verification is owned by a later work item.
        """
        ...

    def finalize_work(self, request: WorkFinalizationRequest) -> FinalizationReceipt:
        """Atomically persist redacted Incident, processed-source outcome and completed work.

        Use the current fenced owner and idempotent finalization ID, including crashes,
        refusals and historical occurrences. Scope disable does not prevent durable
        terminal persistence. Older occurrences cannot overwrite latest evidence or
        reopen a verified resolution. Human tracking closure resets no operational
        state. Keep the existing Incident payload usable; its payload hash is SHA-256
        over the exact persisted SQL NVARCHAR JSON encoded UTF-16 LE.

        Return a receipt only after durable commit/reconciliation. A timeout leaves
        work unfinished and retains its action fence until get_finalization proves
        otherwise. Recovery resumes finalization or read-only action verification.
        state=persisted_waiting_verification confirms only incident persistence;
        it does not claim work completion. SQL derives historical classification
        and occurrence counts. Hash the original receipt's NVARCHAR incident
        fragment, checked against the actual stored bytes in the commit transaction,
        never a parsed/re-serialized incident or a later mutable incident read.
        """
        ...

    def get_finalization(
        self, context: MonitoringContext, finalization_id: CanonicalId,
    ) -> FinalizationReceipt | None: ...

    def request_discovery(
        self, expected: RegistryVersion, selector: ScopeSelector, *, request_id: CanonicalId,
    ) -> MonitoringWork:
        """Accept explicit discovery intent and return its initial reconcile_state work.

        Controller publication queues selector-bound inventory work. No active
        scope is required, and producer replay cannot mutate existing controller work.
        """
        ...

    def complete_collection_work(
        self, context: MonitoringContext, *, work_id: CanonicalId, lease: LeaseToken,
        expected_work_revision: Revision,
    ) -> MonitoringWork:
        """Complete accepted collection under the same producer work fence.

        Completion neither publishes authority nor clears a validation frontier.
        It is never an alternative to incident persistence for effectful work.
        """
        ...

    def observe_source(
        self, observation: SourceRunObservation, *, work_id: CanonicalId, lease: LeaseToken,
    ) -> SourceRunObservation:
        """Persist fresh authoritative source/history evidence under current work ownership."""
        ...

    def get_source(self, execution: SourceExecutionIdentity) -> SourceRunObservation | None: ...

    def get_source_disposition(self, execution: SourceExecutionIdentity) -> ProcessedSourceRecord | None: ...

    def get_incident_state(self, identity: IncidentIdentity) -> IncidentState | None: ...

    def get_incident(self, identity: IncidentIdentity) -> Incident | None: ...

    def get_operation_receipt(
        self, context: MonitoringContext, operation: str, request_id: str,
    ) -> OperationReceipt | None:
        """Reconcile the exact operation/id reported by MonitoringCommitUncertain."""
        ...

    def get_reconciliation_request(
        self, context: MonitoringContext, request_id: CanonicalId, *, producer: ProducerComponent,
    ) -> ReconciliationRequest | None: ...

    def get_validation_frontier(
        self, context: MonitoringContext, frontier_key: str,
    ) -> ValidationFrontier | None: ...

    def reconcile_state(self, request: ReconcileStateRequest) -> ReconciliationResult:
        """Controller-only deterministic publication in the current work transaction.

        CAS the work lease/revision, current configuration, immutable intent/raw
        bindings and protected validation frontier. No target-action lease, model
        call, source promotion, action reservation or workload submission is used.
        Partial windows retain their deny-only frontier. Only correlated terminal
        publication or rejection clears it. Producer completion cannot do so.
        reject_whole_window requests a separate new current-policy operation.
        It preserves earlier handoff decisions and original pending receipts,
        including a page published under a preceding policy revision.
        A sibling of a terminal window receives its own window_acknowledgement,
        bound to window_resolution_request_id/state and the exact original prefix.
        Published stays published; only rejection carries window_rejection_request_id.
        It completes only that owned work;
        it cannot rewrite the window, frontier, page decisions or old receipts.
        """
        ...

    def reconcile_work(
        self, work: MonitoringWork, *, connector_publisher: ConnectorPublisher | None = None,
    ) -> ReconciliationResult:
        """Runner entrypoint for claimed reconcile_state, before exact-source checks.

        Read the current configuration/frontier and build the bound publication
        request inside the same transaction. Keep the original work identity/fence;
        terminal work is recovered through its original reconciliation receipt.
        The synchronous connector publisher runs after admission projection but
        before frontier/work completion. Its bounded reads, connector publication
        and connector-only follow-up enqueue join this transaction; they never
        open a nested transaction, reserve an action or borrow a target lease.
        None selects the same standard controller publisher used by the runner.
        """
        ...
