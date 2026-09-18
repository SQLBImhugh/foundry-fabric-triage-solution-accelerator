# Scheduled Fabric pipeline triage

The hybrid implementation discovers Fabric items, admits selected pipelines
through a shared monitoring registry, and collects job evidence through REST
and an owned Eventstream. The controller can triage a verified failed scheduled
execution and request approval for a bounded full-pipeline rerun. Discovery and
collection do not authorize remediation.

All accelerator application state, including pipeline admission, approvals and
rerun reservations, uses one shared Azure SQL Database. The pipeline jobs,
activity evidence and native Eventstream transport remain Fabric services.

This guide describes the independently reviewed SQL contract and in-progress
adapter/controller integration, not completed deployment acceptance. Isolated
MI transport has received the four observed wire types across manual failure,
scheduled failure, success and cancellation; that does not prove SQL durable
handling or normal-worker readiness. The shipped network baseline is public,
with scoped evaluation SQL/registry access enabled and verified and the public
Foundry path retained. It requires no VNet, NAT or private endpoint; the worker
still has no ingress. SQL recovery and proof/application schema commits are
complete; append-only adjudication preserves the original failed receipt.
The initialization maintenance capture is historical: current maintenance is
`false`, Command Center and the hosted controller use Azure SQL with
the correct acting-identity grants. The deployed heartbeat completed bounded
discovery-intent reconciliation, published its frontier `1/1` and queued one
inventory item without actions or receipt/control changes.
The earlier private Foundry preflight error does not block this public architecture.
The deployed collector-only worker has durably accepted 332 workspace metadata
records. Item/source-access coverage is partial, including permission and
request-budget/throttling gaps; no scope or remediation was auto-admitted.
This mode runs no Eventstream receiver/provisioner. Three real recurrences of
the reused heartbeat schedule completed, and paired started/completed heartbeat
metadata is now queryable in Application Insights. Full event/source-access/
restart/sustained coverage remains separate acceptance work.
Earlier private-network and Fabric SQL checks remain historical.
Keep those boundaries distinct from the successful controller invocation as described in
[DeploymentGuide.md](DeploymentGuide.md#12-hybrid-monitoring-worker-and-eventstream).

## Monitoring contract

In live mode, `bi-triage pipelines` and the hosted `pipeline sweep` request queue
read-only polling work for currently admitted registry targets. A queued response
is not a completed scan or remediation. The worker performs collection; the
controller's `heartbeat` dispatches eligible source work and human commands.
Do not add another static-target polling scheduler.

The worker persists observations, not an admitted source or authoritative head.
Deterministic controller `reconcile_state` work validates the original intake
and current scope before publication; it invokes neither an agent nor a
remediation. Live stores select their SQL component explicitly and use checked
views/static RPCs. An accepted intent or page remains pending until the required
controller publication, not a successful scan inferred from an HTTP reply.

The collector reads the
[Job Scheduler run list](https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/list-item-job-instances).
The controller re-reads the exact execution and requires a supported
`DataPipeline` job, `invokeType=Scheduled`, `status=Failed`, and trustworthy
start/completion evidence. Documented `Pipeline` and `Execute` job types are
recognized for intake; intake support is not permission to choose another
submission route. Preserve the controller's job-type checks and fixed Core
`Pipeline` rerun route.

The controller obtains activity diagnostics through the
[pipeline activity-run API](https://learn.microsoft.com/fabric/data-factory/pipeline-rest-api-capabilities#query-activity-runs).
Names, types, statuses and error details are evidence. Activity inputs and
outputs are not sent to the model or persisted as diagnostics. Notebook
activities remain pipeline evidence; this is not standalone notebook monitoring.

Canonical target identity includes tenant, epoch, workload, workspace and item
IDs. Display names are labels. The exact job identity deduplicates
poll/event/operator overlap, while normalized failure signatures correlate
distinct executions without replenishing an open incident's remediation budget.
Historical failures add occurrences without replacing newer evidence or reopening
a verified resolution. That is incident correlation, not historical backfill.

SQL ownership and fencing protect across processes and invocations. Each REST
page has a durable identity, expected prior cursor/checkpoint, observation window
and current work fence. All observations are accepted or explicitly dispositioned
before that page advances. A partial page is not a completed observation window.
After a lost commit acknowledgement, reconcile that exact receipt.
Decode the typed RPC result rather than inferring success from EXEC rowcount.
Current source reads/publication use `controller.publish_source` under the
required work/target leases; no-effect dispositions and processed markers use
`controller.disposition_source`. Raw source/head/disposition writes are not an
alternate live route.

Malformed responses, unknown invocation/status, permission failures, throttling,
expired ownership and unfinished pagination are coverage gaps, not healthy empty
lists. The list API generally retains only the most recent 100 completed jobs
plus active jobs. A retained window that does not reach the requested lookback
is incomplete even when every returned page was read.

The initial hybrid detector does not infer a failure from a schedule's existence,
a disabled schedule, a job that never started, a manual execution or a cancelled
execution. Missing expected starts need a separate expected-slot/grace detector.
Power BI's scheduled-refresh deactivation rule is not a pipeline rule.

## Collection and scope

| Method | Role | Limit |
|---|---|---|
| Core Job Scheduler polling | Authoritative execution evidence and reconciliation | Recent completed-job history is count-limited. Persist continuations and expose retention gaps. |
| [Fabric Job events](https://learn.microsoft.com/fabric/real-time-hub/explore-fabric-job-events) through Eventstream | Lower-latency intake for verified per-item sources | `ItemJobFailed` can include stuck/cancelled jobs; REST must establish eligible failed scheduled execution. |
| [Workspace monitoring/KQL](https://learn.microsoft.com/fabric/data-factory/workspace-monitoring) | Separate workspace operational analysis | Its support, retention and private-link limits are not guarantees of this collector. |
| Native scheduled-failure email | Optional signal through the approved mailbox filter | Email cannot choose arbitrary executable targets or replace exact source evidence. |

Activator and Power Automate are not required. The chosen Custom Endpoint uses
public outbound Entra-authenticated transport and does not support Private Link.
It needs neither inbound HTTP to the consumer nor another Azure Event Hubs
namespace. Reconciliation polling remains necessary when events are quiet,
delayed or disconnected.

Scopes select tenant, domain, workspace or item metadata. Explicit exclusions
win. Domain descendants and workspace moves must be reconciled; domain membership
does not grant resource access. Core listings are caller-visible, not
tenant-complete. Admin Items is preview and needs explicit adapter selection and
read-admin prerequisites. Unsupported items remain visible with a reason and
do not count as monitored pipelines.

An Admin previews and activates a versioned scope. New resources require review
unless automatic **detection-only** enrolment was explicitly selected. Neither
choice copies a safety review or enables actions. Overlapping includes must not
produce duplicate targets, poll schedules or action budgets.

## Failure knowledge and scenarios

Pipeline playbooks are separate from Power BI playbooks. Retrieval is capped at
three entries, while deterministic rerun checks consider all matching blockers
and every failed activity. An unknown cause does not become retryable because
another activity had a transient error.

| Failure class | Evidence and initial behavior |
|---|---|
| ADLS internal service failure | `ADLSGen2OperationFailed` plus `InternalServerError` is a retry candidate; the wrapper alone is unknown. |
| SQL connection failure | `SqlOpenConnectionTimeout` or `SqlConnectionIsClosed` is a candidate, subject to replay safety and approval. Generic connection errors are insufficient. |
| Request/capacity throttling | Distinguish monitor-read throttling from activity failure. Honor backoff; do not add workload executions to a saturated service. |
| Authentication/authorization | `LSROBOTokenFailure`, `SqlUnauthorizedAccess` or confirmed login denial requires identity/connection correction. |
| Gateway/private path | Verify gateway health and approved routing; do not enable public access or disable TLS checks as a repair. |
| Missing storage object | Establish source/sink/path and data window; do not manufacture empty replacement data. |
| Text or SQL schema mismatch | Compare mappings, columns and values; require correction before replay. |
| Notebook/code/resource failure | Inspect the exact execution and failed stage; no notebook regeneration or schema mutation is exposed. |
| Write timeout/concurrent writer | Commit state may be partial or unknown. Reconcile it before replay, even after an earlier replay-safety review. |
| Nested/dependency failure | Use child/activity evidence; unknown child effects block replay. No automatic parent/child repair is implemented. |
| Cancelled, queued or running | Not an eligible failed scheduled execution; do not reverse cancellation by starting again. |
| Disabled/expired schedule or missing start | Requires separate expectations, schedule evidence and grace rules; not inferred from this run list. |
| Repeated observation/new failed run | An exact job is processed once; distinct jobs can add occurrences without restoring the incident allowance. |

Public sources are carried in
[`playbooks.py`](../src/triage/knowledge/playbooks.py). Relevant references include
[activity retries](https://learn.microsoft.com/fabric/data-factory/activity-retries),
[pipeline monitoring](https://learn.microsoft.com/fabric/data-factory/monitor-pipeline-runs),
and [idempotent ELT guidance](https://learn.microsoft.com/fabric/data-factory/migration-best-practices).
A custom Fail activity does not gain platform-error authority by containing a
known error string; it needs an explicitly scoped custom playbook.

| Executable fixture | Expected behavior |
|---|---|
| `scenario9-pipeline-authentication` | Escalate without a futile rerun. |
| `scenario10-pipeline-rerun-approved` | One approved rerun, with verified completion. |
| `scenario11-pipeline-rerun-denied` | No submission and no remediation allowance consumed by denial. |
| `scenario12-pipeline-schema-mismatch` | Replay-safety configuration does not override a persistent schema error. |
| `scenario13-pipeline-rerun-pending` | Submission remains unverified, not resolved. |
| `scenario14-pipeline-write-timeout` | Require commit reconciliation; do not infer rollback. |

Use `MONITORING_MODE=fixture`, `TRIAGE_TOOL_MODE=mock` and
`TRIAGE_PROVIDER_MODE=mock` for offline fixtures. Fixtures do not import live
targets or replace an unavailable live backend.

## Configuration

Live registry access requires:

```dotenv
MONITORING_MODE=live
MONITORING_TENANT_ID=<tenant-guid>
AZURE_SQL_SERVER=<server>.database.windows.net
AZURE_SQL_DATABASE=<database-name>
```

Use the database name from the Azure deployment, not a Fabric database item.
Configure Entra-only server authentication, public firewall admission, TLS 1.2
minimum, Proxy/TCP 1433, auditing and TDE. The default `allowAzureServices=true`
uses the special start/end `0.0.0.0` SQL rule for Azure-hosted callers, including
other subscriptions, not all Internet IPs. Optional client rules specify exact
IPv4 ranges. Network admission never replaces SQL identity permissions.
The worker, controller and Command Center use
the same application catalog with separate SQL component permissions; no SQL
password, credential-string fallback or Fabric SQL compatibility path exists.

No private-network infrastructure is required by the accelerator's shipped
pipeline-monitoring host. Monitored pipelines and their data connections retain
their own network requirements; do not change them to fit the host's baseline.
Any MCAPS public-access exception is resource-scoped; the SQL exception's single
14-day period is not restarted by re-adding its tag. Longer tests need an
approved exclusion. See [governed evaluation exceptions](DeploymentGuide.md#governed-evaluation-exceptions).

Inventory and REST polling can start before native event transport:

```powershell
.\.venv\Scripts\python.exe -m triage.monitoring.worker --collector-only
```

Supply the selected MI and SQL environment; omit all connector/eventstream
settings. This explicit mode records non-transport health only and cannot
claim event delivery. Use the [deployment quickstart](DeploymentGuide.md#collector-only-quickstart)
for the full helper parameters. Event mode omits the flag and requires the
complete owned connector binding. Workspace metadata is not proof of pipeline
history access or replay permission.

Remove `FABRIC_PIPELINE_TARGETS` from live configuration. Static targets and
compatibility loaders are retired; there is no import or alternate target format.
`PIPELINE_SWEEP_ENABLED`, `PIPELINE_LOOKBACK_HOURS` and
`PIPELINE_MAX_RUNS_PER_SWEEP` retain fixture-sweep semantics, not live admission
or cadence control. `PIPELINE_MAX_PAGES` also bounds the existing pipeline
client's paged reads; it does not replace collector continuation or coverage
state. Live cadence belongs to the registry's observation policy.

Select the operator identity explicitly for live SQL inspection:

```powershell
.\.venv\Scripts\bi-triage.exe pipelines --preflight
.\.venv\Scripts\bi-triage.exe --sql-identity broker --operator-domain "<operator-domain>" pipelines --targets
.\.venv\Scripts\bi-triage.exe --sql-identity broker --operator-domain "<operator-domain>" pipelines
```

`--preflight` is configuration-only. `--targets` reads registry targets in live
mode; the last command queues source observations. Neither proves that a
collector read a page or that the controller finished work. The alternative
global `--sql-identity managed` requires an explicit `AZURE_CLIENT_ID`.

The collector and controller need their own required source permissions.
[Job reads](https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/get-item-job-instance)
and [job submissions](https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job)
document identity/scope requirements separately. Delegated scopes do not replace
item/workspace authorization. Verify the deployed acting identity and pipeline
connections/activities, not just an operator's successful GET.
Domain/read-admin access, stream-workspace access and human app roles are
separate grants.

## Approval-gated reruns

Live targets start observation-only. A current immutable safety review binds
the canonical target, policy revision, definition/parameter fingerprints,
reviewed replay safety and exact-correlation capability. An empty reviewed
parameter object means no overrides; it does not reconstruct the failed run's
original parameters.

A rerun uses the current definition and reviewed configuration. Review defaults,
date windows, watermarks, sink writes and every reachable effect, including
scripts, notebooks, external calls and child pipelines. Missing output or zero
copied rows does not establish that no side effects occurred. A transient error
does not establish replay safety.

The controller uses the bounded `{"executionData":{"parameters":{...}}}` contract.
See [pipeline REST capabilities](https://learn.microsoft.com/fabric/data-factory/pipeline-rest-api-capabilities).
Do not replace it with another API's parameter shape or switch endpoints after
an uncertain response. A full rerun is not rollback, resume from a failed
activity, notebook repair, connection repair or schedule re-enablement.

Before approval and again before execution, checks require:

- Exact failed scheduled execution and complete activity evidence, with a
  retry-candidate playbook and no deterministic blocker.
- Current registry admission, an unchanged authoritative source head and no
  conflicting active/newer execution.
- A current safety review and reachable shared incident/action state.
- Explicit, fingerprint-matched, unexpired, unused approval bound to the
  proposed action and source.

The model proposes a justification, not workspace/item/run IDs or replay
parameters. Power BI refresh/schedule/deferred-retry tools are outside the
pipeline workload allowlist.
Preserve the full tool argument object and its approval fingerprint; do not
strip fields into a smaller technical payload to satisfy an older SQL binding.
The current RPC validates that full proposal and its action-specific schema.

The store atomically validates the tenant/epoch, maintenance state, admission,
source evidence, review, approval and current work/target fence before reserving
the incident slot. A prior successful read cannot prevent a concurrent
revocation race. Denial consumes neither approval nor remediation allowance.

The submission ID comes from the validated response, not the newest history
row. HTTP 202 means accepted, not resolved. Exact job completion and activity
evidence must agree; a pipeline can report `Completed` after a failure-handling
branch while a Copy activity remains failed.

An uncertain submission retains its fence and resumes read-only reconciliation,
not another POST. Even a confirmed no-effect rejection does not globally refund
the incident budget or reuse a consumed approval. Only its explicitly bound,
single-use retry path may reuse the reserved slot after durable parent
finalization and fresh checks. Do not delete a reservation to manufacture a
retry opportunity.
Scope or maintenance changes still block new reservations, but must not erase
an existing reservation's exact source-read leases, verification or finalization
path. The already-submitted effect remains the controller's responsibility.

External users and Fabric's scheduler do not take this controller's SQL fence.
They can start work between a read and submission. Configure workload concurrency
and replay semantics accordingly; the controller does not lock all Fabric users.

## Scheduling and deployment

The following commands are for an approved future cutover after the
[release gates](DeploymentGuide.md#release-gates), not evidence that this release
has replaced the existing controller.

Review the registry, collector source access and durable state before activation.
Re-register after a prompt/tool change and deploy the current controller
separately:

```powershell
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py
azd deploy bi-triage-controller --no-prompt
```

For a new deployment, prepare the common heartbeat schedule disabled. If the
one-minute command scheduler already exists, update its reviewed command to
`heartbeat` rather than creating a second timer; that is the current deployed
pattern. It does not enable mailbox ingestion or change a silent-sweep schedule:

```powershell
az account set --subscription "<subscription-name-or-id>"
az deployment group create -g "<resource-group>" `
  --template-file infra\scheduled-sweep.json `
  --parameters name=bi-triage-monitoring-heartbeat projectEndpoint="<project-endpoint>" `
    command="heartbeat" frequency=Minute interval=1 enabled=false owner="<owner>" `
    costCenter="<cost-center>" environment="evaluation" dataClassification="<classification>"
```

Use the invocation-role grant in [DeploymentGuide.md](DeploymentGuide.md).
Enable only after current-release collection, SQL acceptance and controller
proof; disable overlapping old timers. This schedule invokes the controller,
not source pipelines. Human approval consumes its timeout in addition to the
triage budget; keep caller and durable ownership deadlines consistent.

## Operational state

Shared monitoring records/receipts hold canonical source deduplication, work,
reviews, action reservations and finalization. Existing incident/run/activity
projections remain consumer views. The historical `triage_pipeline_reruns`
table is not a substitute for current admission or the common action fence.
Component-scoped checked views and static RPCs enforce the ownership contract;
generic base-table writes are not a fallback for unfinished adapters.

Pipeline Eventstream sources follow the same logical-proposal, original
observation and controller-binding path as other supported sources. Desired
removal fences intake without forgetting component ownership. Retire a binding
only when the original complete current observation proves remote absence;
do not treat a cancelled source job or an incomplete inventory as removal proof.

Terminal incident/outcome, processed-source disposition and work completion
must be durable together. An in-memory answer does not finish work after a
persistence error. Reconcile the original finalization receipt after a lost
acknowledgement; do not repeat an uncertain effect.

The command-center Fabric pipeline filter selects records, not monitors.
**New investigation** queues an observation for an admitted target; it neither
runs the pipeline nor approves a rerun.

**Resolved by user** is append-only tracking bound to the original SQL NVARCHAR
payload hash over UTF-16 LE. It is not a verified job result and does not reset
budgets, approvals, notification counts or action fences. New evidence
invalidates the closure. Tool-free discussion cannot dispatch actions.

Human reconciliation of a command entry is not external-execution proof or
permission to clear its action reservation. Follow the controlled prototype
reset procedure only for an explicitly approved clean start; ordinary
deployment and investigation do not erase these protections.
