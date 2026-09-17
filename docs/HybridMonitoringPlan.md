# Hybrid monitoring implementation plan

Status: Approved; implementation in progress. Deployment acceptance remains outstanding.

Date: 2026-09-15.

State-platform decision revised on 2026-09-16: all application state uses one
Azure SQL Database, not Fabric SQL. The Command Center is the operational UI;
the retained Rayfin sample is not a deployment or state dependency. This is a
clean prototype start with no migration or compatibility path.

Network direction revised on 2026-09-17: all shipped infrastructure uses normal
public networking with Entra authentication. Private endpoints, VNets, NAT
Gateways and private DNS are not prerequisites. Earlier private-network
investigations remain historical, not the current deployment plan.

This plan combines UI-managed discovery and REST polling with Fabric Eventstream
delivery to an Azure consumer. Together they provide hybrid monitoring. Neither
Activator nor Power Automate is required.

## Decisions and boundaries

| Decision | Planned behavior |
|---|---|
| Clean start | Discard this prototype's application-owned incidents and history during a controlled deployment reset. Start with an empty monitoring configuration. |
| No migration or compatibility | No state import, schema upgrade path, old target-format reader, environment-target fallback, dual writes or mixed-version operation. |
| One tenant | Pin the deployment tenant. Tenant-wide discovery means that tenant, not every tenant the administrator can access. |
| Public event transport | Use the Eventstream custom-endpoint destination over TLS with Entra authentication. This was explicitly selected after reviewing its lack of Private Link support. |
| Public service boundaries | Public SQL, Foundry, registry and web endpoints retain Entra authorization, TLS and service-specific controls. No PE/VNet/NAT/private-DNS prerequisites. The consumer still has no ingress. |
| Shared state | One Azure SQL application database owns every operational store, including configuration, admission, work, checkpoints, incidents, commands, collaboration, receipts and action fences. It is independent of either UI. |
| SQL authentication | Configure Microsoft Entra-only authentication on the logical server. Do not configure a SQL login/password or credential string. Server Entra administration is not a Command Center Admin capability. |
| Database topology and cost | One S1 application database and an optional Basic proof database share the logical server in the shipped template. Do not split application stores by component or add an elastic pool. This is not a final sizing/pricing recommendation. |
| SQL network admission | `allowAzureServices=true` defaults to the special start/end `0.0.0.0` firewall rule for Azure-hosted callers, including other subscriptions, not all Internet IPs. Optional `clientFirewallRules` specify exact IPv4 ranges; SQL identity permissions still apply. |
| Governed evaluation access | Ordinary exception-tag maps are empty/optional. The approved SQL `SecurityControl=Ignore` exception is resource-scoped with reason/review tags for one 14-day period; re-adding does not reset it. Longer tests need an approved exclusion. |
| Operational UI | Deploy the Command Center; Rayfin is an optional retained read-only sample, with no verified Azure SQL binding or state-copy path. |
| SQL write separation | Worker observations and web intents cannot directly overwrite controller authorization or action state. Use checked single-writer views and static guarded procedures; no broad runtime table grants. |
| Human authorization | Keep validated Entra app-role claims. Monitoring configuration is operational data, not a replacement human permissions database. |
| Action authority | Discovery and events never authorize remediation. Retain controller allowlists, evidence checks, explicit approvals and durable reservations. |
| Offline execution | Provide a deterministic fixture-backed monitoring store and fake connectors. They are explicit test/demo implementations, never live fallback stores. |

This document records the implementation requirements, not deployment acceptance. The
[pipeline guide](PipelineTriage.md) and [command-center guide](CommandCenter.md)
describe current source behavior and explicitly identify unproved deployment gates.
On 2026-09-17, scoped evaluation SQL/registry public access was enabled and read
back. SQL retains Entra-only authentication, TLS/auditing/TDE and its special
Azure-services firewall rule; registry admin/anonymous access remains disabled.
The public Foundry controller path is retained. Its earlier private replacement's
preflight error is not a blocker for this public architecture.

The original SQL `STARTED` proof receipt is under guarded recovery; completion,
full-schema commit, runtime permissions and worker recovery remain gates.
The live app/controller remain the prior release, with no history migration/wipe,
normal-worker rollout, hybrid push or current-release UI screenshots.
Earlier private SQL/image checks, Fabric SQL checks and transport receipts remain
historical bounded evidence, not acceptance of the new public deployment.
See [release gates](DeploymentGuide.md#release-gates).
Environment-specific records hold identifiers and exception approvals; they do
not belong in this public document or override the current public baseline.

## Functional scope

| Workload or signal | Scope of this implementation |
|---|---|
| Failed scheduled Fabric pipelines | Discovery, configuration, polling, native job-event intake and the existing activity-aware triage/rerun policy. |
| Power BI semantic-model refresh failures | Discovery and refresh-history polling, feeding the existing Power BI triage boundary. Resolve an exact refresh attempt before any action; an ambiguous history match is not an executable incident. |
| Pipeline notebook activities | Retain them as pipeline evidence. |
| Standalone notebooks and other Fabric item types | Show discovered items as unsupported unless a detector contract exists. Do not silently treat them as pipelines. Standalone triage/remediation is outside this implementation. |
| Missing expected starts and disabled schedules | Outside the initial hybrid release. These need a separate expected-slot detector, timezone handling and workload-specific grace rules. Event silence alone is insufficient. |
| Successful-but-stale or incorrect data | Preserve existing explicitly configured deterministic checks; do not advertise tenant-wide quality monitoring. Broader checks need their own expectations and data access. |
| Audit, report usage, gateways and capacity | Not a universal activity-monitoring feature. Additional collectors would be separate work. |
| Mailbox and operator investigations | Retain supported entry points, with shared target admission and exact-run deduplication where the same source execution is identified. Do not widen the mailbox filter. |

Native semantic-model job events are not an acceptance dependency: current public
documentation is inconsistent about their support. Power BI polling remains
required. A native `ItemJobFailed` event may represent a cancelled or stuck job,
so it cannot substitute for the controller's failed-scheduled-run test. [S1]

## Target architecture

```text
Entra-authenticated Command Center
    |
    | Admin submits monitoring-scope intent (pending publication)
    v
Azure SQL pending intents and controller-published monitoring registry
    |
    +--> inventory and capability reconciliation
    |        |
    |        +--> tenant/domain/workspace/item admission
    |        +--> due REST polls
    |        +--> owned Eventstream source configuration
    |
    +--> Fabric Job events
             |
             v
        Eventstream custom-endpoint destination (public TLS, Entra)
             |
             v
        Azure monitoring worker (outbound consumer, no HTTP ingress)
             |
REST observations ---------+--> Azure SQL durable inbox and work queue
                                      |
                                      v
                         Existing Foundry-hosted controller
                                      |
                         scope + exact execution evidence
                                      |
                         existing policy / approval / reservation
                                      |
                         correlated completion verification
```

The worker collects evidence and manages monitoring resources. It does not run
an agent or submit a workload remediation. The controller remains the only
component that executes an approved workload action. Its tool-free
`reconcile_state` path validates worker observations and web intents before
publishing authority. All records in this diagram use the same application
catalog; Power BI/Fabric workloads and Eventstream transport remain in their
existing services.

Use the existing deployment tooling rather than introducing another orchestrator.
The worker template uses one Azure Container App in a public Consumption
environment, with no ingress, keyless Azure Monitor routing and one continuously
running replica. No VNet or NAT input is required. Start with a proposed
0.5 vCPU / 1 GiB allocation and a maximum of two replicas; resource availability,
measured memory and cost must be checked before provisioning. Database arbitration
must work with two replicas even if the prototype initially runs one.

Retain the existing App Service and Foundry controller. Do not add a separate
Azure Event Hubs namespace merely because the Fabric endpoint uses the Event Hubs
protocol. Use its nonsecret namespace, entity and consumer-group metadata. [S2]
No Eventhouse is included in the selected public transport.

## Platform proof before committing the event implementation

Complete a small, disposable canary before building the Eventstream management UI.
A canary pass requires actual receipt and durable handling of events, not HTTP
success from item creation.

| Gate | Required proof | Failure behavior |
|---|---|---|
| Provisioning | Under the intended identity, create a complete Eventstream definition with a Fabric Job source and custom-endpoint destination; handle asynchronous operations and round-trip the definition. | Mark event provisioning blocked. Do not leave a shell item labelled ready. |
| Source scope | Prove the documented per-item subscription, exact event names and fields, scheduled/manual distinction and behavior when adding another item. | Provision only verified scopes. A shared API enum is not proof of tenant/workspace wildcard behavior. |
| Identity | Consume from the destination using the deployed user-assigned managed identity and current SDK, without a client secret, SAS key or credential-bearing connection string. | Stop the event rollout; no developer-credential or shared-key fallback. |
| Endpoint discovery | Obtain namespace/entity/consumer-group metadata programmatically without retrieving keys. Bind it to an app-owned Eventstream. | Treat missing supported metadata retrieval as an automation gap, not an instruction to log or copy keys. |
| Networking | Demonstrate public outbound TLS from the actual worker, including DNS, service-firewall admission and required identity/platform settings. | Report the failing service leg; do not grant broader identity permissions or alter unrelated resources. |
| Delivery | Receive success/failure/manual/cancelled test observations and preserve the original event identity and exact job identity through SQL acceptance. | No claim of supported failure detection from a transport-only heartbeat. |
| Recovery | Restart the consumer and interrupt SQL access; resume without losing accepted work or creating another remediation opportunity. | Keep polling active but report the hybrid event layer as blocked/degraded, not complete. |
| Provisioning lifecycle | Add, remove and reconcile an owned source without disturbing another source or a user-owned item. Measure topology limits. | Stop admitting more sources and expose the capacity/configuration gap. |
| Power BI action correlation | Establish which supported response/evidence can identify the controller's own refresh submission, including concurrent external refreshes. Use an explicitly approved disposable model for any effectful canary. | Keep that action capability disabled or its submitted result uncertain when exact correlation is unavailable; detection can still operate. |

The public documentation supports Entra consumption, but its managed-identity
walkthrough demonstrates producing events. Actual managed-identity consumption
from the deployed host is therefore an explicit gate, not an assumed equivalence.
[S2][S3]

Custom endpoints do not support tenant/workspace Private Link. An Azure resource
tag cannot change a Fabric tenant setting or workspace network policy. If Fabric
blocks public access to the selected transport workspace, its authorized
administrator must permit the explicitly approved public topology or the event
layer remains blocked. Do not disable tenant-wide controls as an automatic fix. [S4]

## Discovery and scope resolution

The UI supports four selectors: deployment tenant, domain, workspace and item.
A selection produces a versioned scope policy, not an unconstrained list of
executable IDs.

| Concern | Rule |
|---|---|
| Inventory authority | Prefer authorized tenant inventory/scanner APIs for tenant scope. A caller-visible workspace list must be labelled as such, not presented as a complete tenant. |
| Preview APIs | Fabric Admin List Items is preview. Do not silently make it mandatory for a production claim; expose the selected inventory adapter, its status and limitations. |
| Domains | Resolve domain IDs and optional descendants into workspace membership. Reconcile changes; a domain does not grant resource access. |
| Includes/excludes | Explicit exclusions win. Store which rule and inventory generation admitted each target. |
| Overlapping rules | One effective target and poll schedule. Overlap never creates another incident budget or an extra event subscription. |
| Future resources | Default to review required. An administrator can explicitly enable automatic detection-only admission within a selected scope. |
| Unsupported items | Keep them visible with the unsupported workload reason; do not include them in the monitored count. |
| Partial enumeration | Preserve known inventory and expose gaps. A failed page, denied workspace or unavailable domain API is not evidence of deletion. |
| Removal or movement | Recompute eligibility using completed inventory and explicit policy changes. Pause uncertain admissions rather than granting broader access. |
| Access checks | Perform required read probes with the execution/collection identity, not just the signed-in administrator. |
| Visibility | Existing application Readers can see admitted incident records. Tenant-wide monitoring broadens this dataset; it does not introduce per-workspace human ACLs. Review that exposure before activation. |

Exhaust valid continuation tokens within a controlled workload budget. When a
budget is reached, persist continuation state and show incomplete coverage; do
not return a successful empty or silently truncated list.

Reuse the previously examined resource-picker behavior: display names with
ID-backed selection, clear dependent choices, cancel stale requests and save
validated batches with an expected revision. Do not copy another application's
credential fallbacks, silent permission failures or fixed pagination caps.

The UI reads a server-owned inventory snapshot. A refresh request queues
discovery work; it does not run tenant scans inside a browser request or adopt
the browser's identity as proof of service access.

## Monitoring registry and state ownership

Create one typed `MonitoringRegistry` boundary used by the API, collector,
controller, human-command worker and remediation prerequisites.

| Logical record | Minimum purpose |
|---|---|
| Deployment control | Schema version, pinned tenant, monitoring epoch, activation cutoff and maintenance state. |
| Scope policy | Inclusion/exclusion rules, workload capabilities, cadence, future-resource behavior and edit revision. |
| Inventory and admission | Complete/incomplete scan generation, discovered identity, admission reason, capability-probe results and effective policy revision. |
| Connector manifest | App-owned workspace/item/source/destination identities, desired/observed definitions, provisioning operation IDs and health. |
| Monitoring work | Due time, reason, source execution identity, owner, lease/fencing token, attempts and terminal disposition. |
| Signal receipt | Original event identity, connector provenance, bounded normalized metadata and acceptance/quarantine result. |
| Checkpoints | Poll coverage/continuations and stream partition offsets; independent of agent completion. |
| Safety review | Target, reviewed definition/parameter fingerprint, policy revision, reviewer, expiry/revocation and verification status. |
| Coverage | Last completed inventory/poll window, last receiver activity, checkpoint lag, backlog, known gaps and next due time. |

The epoch and tenant are included in state keys consistently across commands,
claims, deduplication, approvals and rerun reservations. This is a new baseline,
not a transformation of old records.

Define distinct canonical identities:

| Identity | Construction |
|---|---|
| Target | Epoch, tenant, workload, workspace and item IDs. |
| Source execution | Canonical target plus the authoritative workload-specific execution/refresh ID. |
| Incident | Canonical target plus the normalized failure signature. |
| Transport delivery | Verified connector provenance plus original event source and event ID; separate from source execution identity. |

Display names are labels, never identity components. Two models named "Sales"
in different workspaces must not share an incident; renaming one item must not
replenish its budget. Apply the same keys to mail, polling, events, operator
commands, deferred retries and run history.

Azure SQL is reached through TDS with Entra authentication. Configure
`AZURE_SQL_SERVER=<server>.database.windows.net` and `AZURE_SQL_DATABASE` from
the Azure deployment, not Fabric database item properties. Configure the
logical server's Entra-only authentication explicitly: Azure SQL supports other
authentication modes, but this deployment has no SQL-password or credential
fallback. Runtime identities receive contained database users with only
required checked-view DML and procedure-execution permissions.
Schema creation and reset belong to deployment tooling; runtime reads and writes
must not call schema-creation helpers or require DDL.

Use one shared application catalog for SQL transactions, cross-store receipts,
unique constraints and atomic conditional operations.
Direct statements identify the winner through affected row count; monitoring
procedures return an explicitly decoded typed result, not an `EXEC` rowcount.
If SQL is unavailable, configuration admission,
signal acceptance, action reservations, queue ownership and incident/processed
state fail closed. Remove live in-memory degradation from existing operational
stores used by these paths, not only from new monitoring stores.

Implement a real transaction/procedure boundary for multi-record operations.
Calling the existing autocommit SQL helper several times does not form a
transaction. `AzureSqlDatabase.transaction()` provides synchronous thread-bound
ownership; no awaits, nested transactions or reconnects are permitted inside it.
Rollback behavior and ambiguous-commit reconciliation remain W1 proof obligations.

Reads that authorize work must observe current shared state. A successful cached
read from an earlier invocation is not enough. Check the active epoch, policy and
safety-review revision at dequeue and again after any approval wait. These reads
do not replace the atomic action-reservation boundary defined below.

### SQL component boundary correction

Implementation review found that generic monitoring tables combine collection
state with action reservations, incident budgets, review proof and source
authority. Python predicates do not make a table-level SQL grant narrower.
No worker or web write grants may be applied until the database boundary is
implemented and proved.

Keep the physical monitoring tables and synchronous transaction machinery.
Use checked updatable views only for genuinely single-writer row families.
Use static, ownership-chained procedures for cross-role transitions, control
locking, work and partition leases, typed intent/intake commits, connector
patches, budgets and approval decisions. Derive permission from the SQL
principal and its grants, not a supplied role string or session flag. No
runtime principal receives base-table DML, broad database roles, schema ALTER
or impersonation permissions.

| Component | Write authority |
|---|---|
| Worker | Collection observations and health/progress; guarded collection work, partition and intake commits. No resolved admission, review proof, source-head or action/budget publication. |
| Web/API | Human scope and review intent, drafts and guarded configuration/decision commits. No controller work updates or action/finalization state. |
| Controller | Deterministic publication of validated target/review/source projections, controller work and existing action/budget/finalization state. |
| Deployer | Initial schema, grants, maintenance, epoch/cutoff and the separately guarded prototype reset. |

Add deterministic `reconcile_state` controller work to consume immutable worker
observations and web intents. It reuses controller validation and reconciliation
logic, but cannot call an agent, reserve remediation or submit a workload job.
Producers can request initial clean reconciliation work, never update already
leased/completed controller work or attach reservations, retry lineage or
finalization IDs. Raw poll progress is separate from validated source/alias
watermarks.

Reconciliation requests may be targetless, including tenant/domain changes.
They use their own work lease and control/intent/evidence publication CAS, not
the target-action ownership lease. Dispatch them before ordinary exact-source
triage validation and prohibit promotion into action-capable work.

Accepted raw intake also raises a protected, deny-only validation frontier.
Every new action must require controller validation through the committed
target/window frontier, including terminal validation of an applicable multi-page
window. Producer completion, an intake receipt or the absence of a first
published window is not clearance. Only correlated controller publication or
durable rejection clears that fence; verification/finalization of existing
reservations remains available.

Scope and review changes return configuring/pending-validation until their
current projections are published. Their accepted intent and original
fingerprint remain distinct from technical verification. Original operation
receipts remain immutable; later publication is not an excuse to return a newer
operation as the original result.

Revocation must not wait for that asynchronous publication. Committing a scope
or review change immediately fences new reservations through the current
configuration revision or an equivalent protected latest-intent check. Pending
validation fails closed. Already reserved effects retain read-only verification
and finalization; their budgets and consumed approvals are not restored.

The SQL boundary must enforce actual stored work families, immutable identities,
expected revisions, live owner/fence/expiry, legal transitions and atomic
receipt/checkpoint coupling. A shared `work:` key prefix or generic privileged
JSON setter cannot provide those guarantees. Prove negative role cases with
actual deployed identities, not only a translated SQL test backend.

Azure SQL supports `CREATE USER ... WITHOUT LOGIN` and `EXECUTE AS USER` for
database-scoped tests. Those tests cannot prove deployed-MI sign-in,
network/firewall admission or reconnect/recovery. Verify service-principal/UAMI SQL SIDs from
client-ID GUID bytes in little-endian order against native Azure SQL; Azure
RBAC uses principal object IDs instead. Runtime roles must not receive
impersonation or directory-administration permissions.

## Durable intake and execution

### Event receipt

Validate a bounded event envelope, transport provenance, tenant and connector.
Preserve CloudEvents `source + id`; an action/activation ID is not a replacement
for the original event identity. Derive canonical workspace/item/job identity
from verified fields and REST evidence, not an event-supplied URL.

Use one SQL transaction to accept the receipt and create eligible monitoring
work. Only then advance the stream checkpoint. A checkpoint means "safely stored
for processing", not "the agent finished" or "the problem is resolved".

Advance only through a contiguous set of durably accepted or durably quarantined
partition positions. A failed earlier event cannot be skipped because a later
batch completed. On an ambiguous SQL commit, re-read the idempotency key before
deciding whether to checkpoint. Never assume rollback from a timeout.

Malformed, oversized, unsupported and out-of-scope signals receive bounded,
redacted dispositions. A replayable quarantine entry is not itself permission
to investigate or act. Replaying it rechecks current scope and the activation
cutoff.

Use a SQL-backed implementation of the supported consumer checkpoint/ownership
interface. Partition leases use database time and fencing, not process memory.
No default blob checkpoint account or shared key is required by this plan.

### REST polling and common admission

Pollers produce the same normalized source-execution observations as the event
consumer. The controller re-reads the exact source execution before reasoning
and obtains workload-specific diagnostics.

Event identity removes duplicate deliveries. Source execution identity removes
poll/event/mail overlap. Incident signatures still correlate separate executions
without replenishing the open incident's remediation allowance.

Each REST page/window follows the same acceptance ordering as the stream:
durably accept or explicitly disposition every observation before advancing its
continuation or coverage watermark. Use a durable page identity, expected prior
cursor, owner/fence and epoch. Reconcile an ambiguous commit from that identity;
do not resume beyond observations held only in process memory.

Distinguish a partial-page receipt from a complete observation window. A 200
response, a full page or a newly saved continuation does not establish complete
coverage. Duplicate page processing is safe; skipping uncommitted observations
is not.

Preserve the existing pipeline eligibility rules, including supported job-type
distinctions, terminal failure and scheduled invocation. Preserve activity-based
completion checks. A submitted job is not a successful remediation.

If a Power BI alert cannot be bound unambiguously to an admitted refresh attempt,
record an explicit diagnostic problem and do not guess which attempt to remediate.
Power BI and Fabric identifiers must not be treated as interchangeable.

### Scheduling and recovery

Use durable due work and bounded, fair batches across workspaces. Replace the
static 50-target loader and sequential single-failure sweep as the scaling
boundary; simply increasing their limits is insufficient.

Proposed initial operating values, to be measured rather than treated as platform
guarantees:

| Setting | Initial target |
|---|---|
| Full inventory refresh | Hourly, with resumable pages and per-API budgets. |
| Domain membership refresh | Every 15 minutes, budgeted independently. |
| REST-only failure polling | Every 5 minutes; shorten for high-run-rate targets when the service budget permits. |
| Event-enabled reconciliation polling | Every 15 minutes, with explicit retention-gap detection. |
| Controller queue heartbeat | Every minute; bounded work and fair shares for human commands and automatic intake. |
| Concurrent external reads | Start at four globally and two per workspace, with tighter service-specific limits where required. |
| Receiver heartbeat | Every 30 seconds; stale after two minutes. |
| Local acceptance objective | Receipt-to-SQL acceptance p95 below five seconds under the admitted canary load. |
| Dispatch objective | Accepted eligible work begins processing within two minutes under the admitted load; approval wait and external delivery latency are excluded. |

Honor `Retry-After` and coordinate limits across replicas. A target whose
run-history retention is shorter than the polling interval is not fully covered.
Expose that gap; do not claim that a 24-hour lookback retrieves 24 hours of history.

The existing controller heartbeat is the initial wake mechanism. The consumer
does not need a new public webhook or a human API token. The heartbeat drains
durable work after consumer/controller restarts. Coalesced wake-up calls are not
required for the first release.

Deferred Power BI retries are another effectful entry point, not a separate
exception. Persist their canonical target/source identity and originating policy
revision. Route them through current admission, observation-only/action capability,
epoch/maintenance checks and the same target-level action ownership. An obsolete
or revoked retry receives a durable non-executing disposition; it cannot call the
refresh client directly around the common controller boundary.

Expired or ambiguous effectful work is not automatically reissued. Continue
read-only reconciliation of already submitted jobs even if their monitoring
scope is disabled, subject to remaining access.

### Durable finalization

Finish a monitoring work item only when its terminal incident/outcome and
processed-source disposition are durable. Tie these records and work completion
together with a transaction, or an explicitly recoverable staged state machine.
An in-memory incident result is not proof of successful finalization.

After a persistence timeout, keep the work unfinished and retain any existing
action fence. Recovery re-reads durable state and resumes finalization or
read-only action verification; it does not rerun an uncertain effect. Claims,
terminal crashes and refusals follow this rule as well.

## Remediation review

Monitoring activation defaults to observation only. Automatic discovery never
copies a remediation attestation to a new item.

For pipeline reruns, retain reviewed parameters and replay safety, and bind the
review to the current target definition/configuration fingerprint when the
platform exposes a verifiable definition. If current definition safety cannot
be established, keep reruns disabled rather than assume the review is current.

An approval remains explicit, fingerprint-matched, unexpired and unused. Scope
or safety changes during approval invalidate execution. A denial consumes no
remediation budget.

Create the action reservation in one transaction/procedure that validates the
active epoch, maintenance state, effective admission, action capability, current
safety/approval revision and expiry, work ownership and remaining action budget.
Serialize scope/safety changes against that reservation. A preliminary fresh
read followed by an independent insert still permits a revocation race.

The reservation is the decision point: if revocation wins, no new effect is
authorized; if the reservation wins first, show the action as committed/in-flight.
Do not promise that a subsequent disable can retract a request already committed
to external execution. Rejected admission/reservation must not consume an approval
or remediation budget.

Both pipeline reruns and Power BI refreshes require a durable pre-POST action
fence, persisted submission state and recovery of uncertain writes. Keep the
workload-specific controller policy for whether human approval is required.
The worker or a generic action-enabled flag cannot bypass that policy.

Pipeline completion is checked against the stored rerun ID and activity evidence.
For Power BI, separately correlate the source failure and the controller's own
remediation refresh. Remove "the first refresh not seen before POST" as proof:
an unrelated concurrent refresh can satisfy that test. If exact submission
correlation is unavailable, preserve an uncertain/unverified outcome and prohibit
another automatic POST.

Human tracking resolution and tool-free discussion continue to have no authority
to reset these controls.

## Admin and operator experience

Add **Monitoring setup** without replacing **Access & permissions**.
Retain the current font, colors, geometry, logo and permission-refresh lock.

| Surface | Planned behavior |
|---|---|
| Scope builder | Tenant/domain/workspace/item selection, includes/excludes, workload capability filter and future-resource rule. |
| Preview | Exact additions/removals, required permissions, unsupported items, inventory completeness and expected subscription/poll changes. |
| Activation | An Admin saves an expected revision and a preview identity. Queue provisioning; show "Configuring" until its evidence checks succeed. |
| Target detail | Discovery source, scope reason, service access, poll/event status, last evidence, backlog and separate remediation-review state. |
| Coverage overview | Separate discovered, access-verified, current and action-enabled counts; denominators include incomplete/unknown scope. |
| Connector operations | Health, throttling, source drift, checkpoint age, quarantined receipts and explicit reconciliation controls. |
| Incident links | Open the existing full incident page. Preserve notes, safe Markdown, tracking resolution and observer boundaries for new records. |

Proposed API families are `/api/monitoring/inventory`, `/api/monitoring/scopes`,
`/api/monitoring/plans`, `/api/monitoring/coverage` and
`/api/monitoring/connectors`. These are design names, not existing routes.

Only Admin may edit monitoring configuration or request provisioning.
Readers may inspect appropriate coverage information; Operator and Approver
remain separate action roles. Server-side authorization is mandatory even if
the UI hides a control.

A dry-run plan performs no Fabric writes, grants or remediation. Activation uses
an idempotency ID, expiry and expected registry revision. A lost response is
reconciled by that ID, not by creating another plan/subscription blindly.

## Identity, networking and operational ownership

| Component | Required authority |
|---|---|
| Web API | Existing human JWT verification and limited SQL access for configuration/commands. No directory-wide Graph access. |
| Monitoring worker | Required inventory and source-read operations, stream consumption, limited SQL intake/checkpoint access, and provisioning restricted to app-owned monitoring items. |
| Controller | Existing diagnostics, policy-gated action access and durable controller state. |
| Deployment operator | Explicit schema DDL, Azure/Fabric provisioning, prerequisite consent/tenant settings and reviewed resource grants. |
| Reasoning agents | No new service permissions. |

An API that reads data can require a broader role: Power BI refresh history
requires dataset Write permission, and Eventstream documentation currently
describes workspace-level permissions for consumption. State those requirements
honestly and use a dedicated monitoring workspace. Do not call a principal
"read-only" solely because this code issues GET requests. [S3][S5]

Tenant admin inventory permissions, source item access, stream workspace access
and application roles are different grants. Runtime code must use an explicitly
selected deployed identity and tenant, not a developer credential fallback.
Keep the special read-only admin API service-principal registration rules
separate from delegated permissions. [S6]

The consumer's public outbound event connection does not require public inbound
access to the worker or another App Service allow rule. The public Consumption
environment supplies outbound connectivity; verify DNS/TLS and destination
admission rather than requiring a dedicated VNet/subnet/NAT.

Preserve the existing deliberate browser/SCM access rules during code deployment.
Do not rerun an infrastructure default that silently replaces them.

Where a supported governance exemption is necessary, apply only the documented
exception to the exact Azure resource that needs it, record why, read the actual
result back, and track expiry. `sqlNetworkExceptionTags`, `registryExceptionTags`
and `accountNetworkExceptionTags` must not spread to unrelated resources.
The MCAPS SQL exception's single 14-day period is not renewed by removing and
re-adding its tag; longer tests require an approved exclusion. Do not assume
Azure tags apply to Fabric items or make a temporary exception permanent.
See [governed evaluation exceptions](DeploymentGuide.md#governed-evaluation-exceptions).

Define alerts for worker/controller absence, missed inventory deadlines, SQL
unavailability, API denial/throttling, checkpoint age, queue backlog, source drift
and exemption expiry. Logs/spans contain metadata only. Store redaction remains
inside the persistence boundary.

## Controlled prototype reset

The user permits deleting existing prototype incidents and history and starting
with an empty Azure SQL application database. This is a deployment operation,
not a command exposed to the model, browser or default test runner. The current
bootstrap/reset tooling targets Azure SQL only; it does not read, migrate or
reset the previous Fabric SQL item. Old-state disposal is separately scoped
cleanup after the same writer/effect checks.

1. Inventory the exact application-owned tables, local operational files,
   schedulers and invocation entry points. Display a manifest and verify the
   target tenant, database and ownership immediately before the operation.
   Derive required writer/action-target coverage from authoritative SQL and
   deployment bindings. A supplied preflight list is only a selector: checking
   it against itself cannot prove completeness. Refuse omitted registered
   targets and writers whose actual database binding differs from the reset
   target.
2. Enter maintenance mode. Stop intake, scheduled invocations, consumers and
   human command submission. Drain or explicitly cancel safe queued work.
3. Wait for in-flight controllers and workload actions. Reconcile any submitted
   or uncertain write against the external service. An unresolved action blocks
   clearing its fence; permission to discard history is not proof that it is
   safe to submit the action again.
4. Disable old controller versions and workers before the new schema is writable.
   Do not run old and new code against shared operational state concurrently.
5. After confirming the exact deletion manifest, discard only old
   application-owned state through the scoped cleanup and create the new Azure
   SQL application schema/baseline with the deployment identity. A fresh target
   uses initialization; an already initialized Azure SQL target uses the guarded
   reset. No data-copy or old-schema upgrade logic is introduced.
6. Create a new monitoring epoch and activation cutoff, an empty registry and
   fresh baseline records. Bootstrap current desired configuration through the
   new UI/fixture contracts, not an import of old environment targets.
7. Verify schema/access and start the new release in observation-only mode.
   Re-enable only the intended current schedulers. Then enable selected monitors.

The reset scope includes incidents, execution/tool history, notes/discussion,
tracking resolutions, approvals, retries, commands, processed-source state,
claims/reservations, notification state and app-owned detector baselines.
Deployment infrastructure, Entra users/groups/app roles, source workspaces,
business pipelines/models/data and shared external logs are not reset.

External job history, old messages and sent approval links still exist after SQL
is cleared. Automatic admission starts at the new epoch's cutoff; old observations
must not become fresh action opportunities. Explicitly distinguish source
execution time from arrival time. Older evidence can be reported as baseline
context, but is not automatically remediated.

No reset or schema bootstrap runs on application startup. A missing or incompatible
schema is a visible deployment error. Failure during rollout leaves intake stopped
rather than falling back to the previous target list or old schema.

Give the destructive deployment operation its own expected epoch, manifest hash
and receipt. If its acknowledgement is lost, inspect that receipt and the schema
before continuing. Retrying deployment must not erase records already written by
the new release. Recovery is a forward fix or a newly approved clean reset, not an
automatic import/rollback path.

## Implementation work packages

| Package | Deliverables | Dependency and completion gate |
|---|---|---|
| W0: Platform and reset proof | Disposable event canary, public Azure SQL endpoint/firewall/Entra identity proof, Power BI correlation contract, new-schema contract, deletion manifest and resource inventory. | First. Resolve unsupported event/action assumptions before promising automated setup. No prototype reset yet. |
| W1: Shared monitoring domain | One Azure SQL application catalog, typed registry, canonical target/execution/incident keys, epoch/admission model, deployment-only schema, real transactions, atomic action reservation/finalization and explicit fixture implementation. | W0 contracts. Independent instances see the same edits; all affected live operational stores fail closed. |
| W2: Inventory and polling | Tenant/domain/workspace/item resolution, capability catalogue, service probes, fair work scheduling and pipeline/Power BI adapters. | W1. Incomplete discovery and retained-history gaps remain visible. |
| W3: Monitoring UI and API | Scope builder, dry-run/activation, coverage and target detail, revision conflict handling and current-role enforcement. | Can run alongside W2 after W1 API contracts. No browser-triggered remediation or grants. |
| W4: Public Eventstream intake | Owned topology provisioning/reconciliation, managed-identity consumer, durable receipts, SQL partition leases/checkpoints and quarantine. | W0 event proof and W1. Can run alongside W2/W3. Actual delivery and restart proof required. |
| W5: Common controller admission | Poll/event/mail/operator/deferred-retry convergence, atomic scope/action admission, durable Power BI and pipeline action lifecycle, exact-run correlation, fair draining and completion follow-up. | W2/W4 contracts and W1 transactions. Duplicate observations do not multiply incidents, notifications or actions; uncertain results cannot cause another effect. |
| W6: Clean deployment | Quiesce old release, reconcile outstanding effects, perform approved reset, bootstrap schema, deploy code/resources and activate detection. | W1-W5 complete, quotas/cost/access confirmed, explicit execution approval. No mixed-version or rollback compatibility. |
| W7: Acceptance and documentation | Offline regressions, separate live proofs, sustained canary observation, screenshots and updated operator/contributor/deployment guides. | Required for completion of the hybrid release, not an optional follow-up. |

W2, W3 and W4 can proceed in parallel after agreeing W1 contracts. Assign one
integration owner for the runner, schema and shared API/store contracts; parallel
contributors should not independently rewrite those shared surfaces.

### Code ownership map

These are proposed change areas, not claims that the new modules exist.

| Area | Expected surfaces |
|---|---|
| Monitoring domain | New `src\triage\monitoring\` models, registry, inventory, admission, polling, event and coverage modules. |
| Shared state | `src\triage\store\azure_sql.py`, monitoring store and deployment-owned schema; integrate every command/claim/rerun and ancillary store with one catalog, real transactions and fail-closed durability. |
| Controller | `src\triage\runner.py`, `src\triage\signature.py`, `src\app.py`, deferred retries, Power BI refresh correlation, pipeline actions and atomic tool prerequisites. |
| Command center backend | `src\triage\command_center\api.py`, `models.py`, `service.py`, `worker.py`; new monitoring API/service module where appropriate. |
| Command center frontend | Navigation, API types/client and a new Monitoring setup workspace using existing shared controls. |
| Worker host | New worker entry point/container; optional live SDK dependencies remain outside the base offline installation. |
| Deployment | Existing `azure.yaml`, `infra\` and operator scripts; new host resources, scheduler coordination and explicit reset/bootstrap procedure. |
| Retirement | Remove static live target configuration and duplicated web/controller resolution, including `FABRIC_PIPELINE_TARGETS`, plus Fabric SQL runtime/configuration names; no compatibility loader or state import. |
| Documentation | `PipelineTriage.md`, `CommandCenter.md`, `DeploymentGuide.md`, architecture/operations references and identical contributor contracts when their rules change. |

Pin the existing Foundry hosting library exactly. This work does not require a
model/provider replacement or a rewrite of the policy ledger.

## Acceptance cases

Every automated case remains offline. Use fakes for Fabric, Power BI, Eventstream,
SQL protocol behavior and clocks. Live checks are separate, explicit operator
procedures against owned canary resources.

| Area | Required cases |
|---|---|
| Scope | Domain descendants/moves, overlapping includes, exclusion precedence, explicit future-resource behavior, unknown/unsupported types and wrong tenant. |
| Inventory | Multiple pages, interrupted pagination, 401/403/429, deleted items, incomplete domains and a complete empty scope. Incomplete must differ from genuinely empty. |
| Shared configuration | Two independent instances, conflicting edits, removed targets, changed safety reviews, process restart, unavailable SQL and a revocation racing action reservation. |
| SQL role boundary | Worker/web attempts to rewrite actions, budgets, resolved review/source/admission state, controller work, receipts or protected control are denied. Accepted intent fences immediately while publication is pending; existing effects retain verification. |
| Canonical identity | Same display name in different workspaces, item rename, two distinct runs of one failure, source/submission identity separation and every intake path using the same target key. |
| Intake | Duplicate event IDs, different event IDs for one run, poll/event/mail overlap, reversed delivery order, delayed old runs, oversized/malformed input and unknown connector. |
| Checkpointing | Stream and REST crash-before-commit, ambiguous commit, crash-before-checkpoint, partial batch/page, expired lease, partition rebalance, interrupted continuation and replay. |
| Controller | Stale scope after approval, denied approval, revoked deferred retry, shared target ownership, newer/active runs, unknown activity effects, uncertain POST and exact completion. |
| Power BI actions | Crash after accepted refresh POST, missing correlation, concurrent external refresh, deferred retry racing an operator command and no first-unseen-refresh success fallback. |
| Finalization | Incident/processed write failure, recovery before work completion, process exit, ambiguous terminal commit and no in-memory success or repeated effect. |
| Clean reset | Empty new state, wrong-target or misbound-writer refusal, omitted registered target/writer blocking, in-flight effect blocking, old approval/message/event replay, ambiguous reset acknowledgement and no old-schema/config fallback. |
| Coverage | Retention exhausted, quiet but disconnected source, stale worker, paused capacity, backlog, throttling and a busy workspace that cannot starve another. |
| UI | Admin-only changes, Reader coverage, stale-response cancellation, revision conflicts, token-refresh lock, empty/incomplete states and current styling. |
| Existing behavior | Existing offline scenarios, pipeline policy tests, Power BI triage, notes/resolution hashing, safe Markdown and tool-free observer behavior. |

Exercise at least 200 admitted targets across 10 workspaces in an offline
fairness/load test. This is a test envelope, not a claim about tenant capacity.
Measure the actual live service budget and alert when an admitted scope cannot
meet its requested cadence.

Live acceptance must prove native Azure SQL identity/SID binding, checked-view
and static-RPC permissions, independent-instance SQL
arbitration, source provisioning, actual public endpoint consumption, restart
recovery and the controller's exact-job evidence. Use observation-only canaries
before an explicitly approved safe rerun.

Run a sustained canary covering an overnight governance/availability window.
Capture the Monitoring setup, scope preview, coverage gaps, event/poll convergence
and resulting full incident page. Verify the served build and persistent records,
not only build success or an HTTP acknowledgement.

## Rubber-duck review

An independent read-only review challenged the completed draft against the
existing controller, signature, retry, Power BI client and SQL store code.
It found six high-priority implementation gaps. All six became explicit
requirements and have since guided the source implementation. This table records
the design review, not a claim that deployment acceptance has passed.

| Finding | Counterexample | Plan correction |
|---|---|---|
| Incident identity used display names | Equally named models in different workspaces share an incident, or a rename grants another budget. | Canonical target/execution/incident identities; display names remain labels across every intake path. |
| Deferred retries bypass common admission | A previously queued refresh runs after scope revocation or competes with an operator command. | W5 includes deferred retries, current policy checks and shared target/action ownership. |
| Power BI lacked the pipeline action guarantees | A successful POST loses its receipt; a concurrent refresh is mistaken for the controller's completion. | Durable Power BI action fencing and exact submission correlation; uncertain results stay uncertain. |
| Fresh reads left a revocation race | Scope changes between the final read and independent action reservation. | One atomic reservation boundary serializes admission, current policy, ownership and action fencing against configuration edits. |
| Poll checkpoints could skip unstored observations | Continuation advances before every observation from the page is durable. | REST acceptance/checkpoint ordering, page identities, real SQL transactions and ambiguous-commit reconciliation. |
| Existing SQL fallbacks could lose terminal incidents | In-memory incident success is followed by durable work completion and a process exit. | Remove affected live fallbacks; make outcome/processed disposition/work finalization durable and recoverable. |

The review also confirmed that event identity/network/provisioning claims are
properly gated and that the reset must stop old code and reconcile outstanding
effects before erasing fences. The final reset procedure additionally guards
against repeating a destructive reset after an ambiguous deployment response.

Residual gates are empirical: deployed-UAMI consumption, full topology
provisioning, Power BI submission correlation, actual service permissions,
network/governance behavior, quotas and sustainable rate/cost. A plan review
cannot establish those live results. Polling-only success is not completion of
the requested hybrid release.

## Public platform references

- [S1: Fabric job events and payload semantics](https://learn.microsoft.com/fabric/real-time-hub/explore-fabric-job-events)
- [S2: Eventstream custom-endpoint destinations](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-destination-custom-app)
- [S3: Eventstream Entra authentication and consumption](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/custom-endpoint-entra-id-auth)
- [Managed-identity walkthrough; demonstrates event production](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/connect-using-managed-identity)
- [S4: Eventstream Private Link support matrix](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/set-up-tenant-workspace-private-links)
- [S5: Power BI refresh-history permissions](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-history-in-group)
- [S6: Service principals and read-only admin APIs](https://learn.microsoft.com/en-us/fabric/admin/enable-service-principal-admin-apis)
- [Fabric domain APIs](https://learn.microsoft.com/en-us/rest/api/fabric/admin/domains)
- [Fabric admin item inventory and preview status](https://learn.microsoft.com/en-us/rest/api/fabric/admin/items/list-items)
- [Power BI metadata scanning](https://learn.microsoft.com/en-us/fabric/governance/metadata-scanning-run)
- [Fabric job-history retention](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/list-item-job-instances)
- [Azure SQL Entra-only authentication](https://learn.microsoft.com/azure/azure-sql/database/authentication-azure-ad-only-authentication)
- [Azure SQL firewall rules](https://learn.microsoft.com/azure/azure-sql/database/firewall-configure)
- [Azure SQL connectivity architecture](https://learn.microsoft.com/azure/azure-sql/database/connectivity-architecture)
- [Optional private-hardening reference: Azure SQL private endpoints](https://learn.microsoft.com/azure/azure-sql/database/private-endpoint-overview)
- [Azure SQL auditing](https://learn.microsoft.com/azure/azure-sql/database/auditing-overview)
- [CREATE USER and Entra principal SIDs](https://learn.microsoft.com/sql/t-sql/statements/create-user-transact-sql#arguments)

Fabric platform documentation was reviewed on 2026-09-15. The Azure SQL links
support the revised provisioning requirements, not a new live acceptance
record. Documented support is not a substitute for the deployment-specific
proof gates above.
