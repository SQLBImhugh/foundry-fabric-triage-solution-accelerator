# Scheduled Fabric pipeline triage

The controller can monitor explicitly configured Fabric Data Factory pipelines,
triage failed scheduled jobs, and request approval for a bounded full-pipeline
rerun. It uses the existing Foundry agent, policy ledger, approval channel and
Fabric SQL incident store. The Power BI mailbox and silent-failure paths remain
separate.

## Monitoring contract

`bi-triage pipelines` performs one bounded sweep. A scheduler invokes the hosted
controller with `pipeline sweep`; it does not send synthetic failure emails.

The sweep reads the
[Job Scheduler run list](https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/list-item-job-instances),
validates that the configured item is a `DataPipeline`, selects
`invokeType=Scheduled`, `status=Failed` with an end time inside the lookback,
and re-reads each selected job. Documented `Pipeline` and `Execute` job types
are recognized for intake. Reruns use the verified Core `Pipeline` route;
`Execute` jobs are triaged but not automatically mapped onto that route.
It retrieves activity
diagnostics through the
[pipeline activity-run API](https://learn.microsoft.com/fabric/data-factory/pipeline-rest-api-capabilities#query-activity-runs).
Activity names, types, statuses and error details are evidence; activity inputs
and outputs are not sent to the agent or stored.

Notebook activities are included as pipeline evidence. This is not standalone
notebook-job monitoring, and a notebook cannot be added as an independent
monitoring target through the command center.

The source run ID prevents processing the same job on every poll. A separate
signature, scoped to workspace and pipeline IDs, groups new runs with the same
failure into one open incident. A pipeline-scoped SQL claim serializes this
controller's work; notification deduplication remains controller-enforced.
Historical failures discovered after a newer occurrence are counted without
replacing the latest evidence or reopening its verified resolution. This is
incident correlation, not a claim that older data windows were backfilled.

An API failure, malformed response, unknown trigger, pagination limit, or missing
evidence is not a healthy result. Monitoring faults are persisted separately as
`fabric_pipeline_monitor` incidents. The CLI returns a nonzero status for an
incomplete sweep; the hosted pipeline path propagates that fault to its host.
Disabled and unconfigured monitors report those states explicitly.

The list API generally retains only the most recent 100 completed jobs, plus
active jobs. The configured lookback is a filter, not a guarantee that older
history remains available. A full retained window that does not reach the
lookback produces a coverage warning. Poll frequently enough for the pipeline's
run rate, or use a longer-lived monitoring log.

This path does not infer failures from a disabled schedule, a job that never
started, a manual execution, or a cancelled execution. It does not apply Power
BI's scheduled-refresh deactivation rule to pipelines.

## Monitoring options

| Method | Use | Limits |
|---|---|---|
| Core Job Scheduler polling | Implemented baseline; minimal additional infrastructure and explicit scheduled-run attribution | Recent-job retention is count-limited. Persist run keys and report gaps rather than assuming complete history. |
| [Fabric Job events](https://learn.microsoft.com/fabric/real-time-hub/explore-fabric-job-events) and Activator | Lower-latency notification; pipelines are a documented source | `ItemJobFailed` also covers stuck/cancelled jobs. Enrich the event's job ID through REST before triage. This repository does not provision an event subscription or claim a native Foundry webhook trigger. |
| [Workspace monitoring/KQL](https://learn.microsoft.com/fabric/data-factory/workspace-monitoring) | Workspace-wide run/activity analysis and longer-lived operational trends | Preview; the [monitoring overview](https://learn.microsoft.com/fabric/fundamentals/workspace-monitoring-overview) documents 30-day retention and private-link limitations. The pipeline-specific page also limits error details/diagnostics. |
| Native scheduled-failure email | An existing operations mailbox can receive the notification | Email is a signal, not authoritative run evidence. It must not bypass the inbox filter or choose executable target IDs. |

For eventing, the [private-link support matrix](https://learn.microsoft.com/fabric/security/security-private-links-overview#activator)
distinguishes direct Fabric events to Activator from Eventstream to Activator.
Do not assume that a working private polling path proves either eventing path.
Reconciliation polling is still needed for missed or paused event delivery.

## Failure knowledge and scenarios

Pipeline playbooks are separate from Power BI playbooks. At most three matching
entries are shown to the model, while the deterministic rerun gate considers
all matching blockers and every failed activity. An unknown cause does not
become retryable because another activity had a transient error.

| Failure class | Evidence and initial behavior |
|---|---|
| ADLS internal service failure | `ADLSGen2OperationFailed` plus `InternalServerError` is a retry candidate; the wrapper alone is unknown. |
| SQL connection failure | `SqlOpenConnectionTimeout` or `SqlConnectionIsClosed` is a candidate, subject to replay safety and approval. Generic connection errors are not enough. |
| Request/capacity throttling | Distinguish monitor-read throttling from activity failure. Honor read backoff; do not add executions to a queue. Capacity recovery is not established by this controller, so it escalates. |
| Authentication/authorization | `LSROBOTokenFailure`, `SqlUnauthorizedAccess`, or a confirmed login denial requires identity/connection correction. |
| Gateway/private path | Verify gateway health and approved routing. Do not enable public access or disable TLS checks as a repair. |
| Missing storage object | Establish the failing source/sink/path and data window. Do not manufacture empty replacement data. |
| Text or SQL schema mismatch | Compare mappings, columns and values; require correction before replay. |
| Notebook/code/resource failure | Inspect the exact execution and failed stage. No notebook regeneration or schema mutation is exposed. |
| Write timeout/concurrent writer | Commit state may be partial or unknown. Reconcile it before replay, even if an earlier configuration declared the pipeline rerunnable. |
| Nested/dependency failure | Use the failed child/activity evidence when available; unknown child effects block replay. No automatic parent/child repair is implemented. |
| Cancelled, queued or running | Not an eligible failed scheduled execution. Never reverse a cancellation by automatically starting again. |
| Disabled/expired schedule or missing start | Requires a separate expected-slot detector, schedule snapshot and grace period. Not inferred from this run list. |
| Repeated observation/new failed run | One job ID is processed once; distinct jobs can add occurrences without replenishing an open incident's remediation allowance. |

The public sources are carried on each entry in
[`playbooks.py`](../src/triage/knowledge/playbooks.py). Additional scenario
guidance comes from
[activity retries](https://learn.microsoft.com/fabric/data-factory/activity-retries),
[pipeline monitoring](https://learn.microsoft.com/fabric/data-factory/monitor-pipeline-runs),
and [migration/idempotent ELT guidance](https://learn.microsoft.com/fabric/data-factory/migration-best-practices).
A custom Fail activity does not gain platform-error authority merely by
containing a known error string; admitting one requires an explicitly scoped
custom playbook.

The executable fixtures are:

| Scenario | Expected result |
|---|---|
| `scenario9-pipeline-authentication` | Escalate without requesting a futile rerun. |
| `scenario10-pipeline-rerun-approved` | One approved rerun, verified completion. |
| `scenario11-pipeline-rerun-denied` | No submission and no remediation budget consumed. |
| `scenario12-pipeline-schema-mismatch` | Escalate; replay-safety configuration does not override a persistent schema error. |
| `scenario13-pipeline-rerun-pending` | Submission is recorded but not reported as resolution. |
| `scenario14-pipeline-write-timeout` | Require commit reconciliation; do not infer rollback. |

Run these with `TRIAGE_TOOL_MODE=mock`. Live mode refuses pipeline fixtures rather
than returning mock results as if Fabric had executed them.

## Configuration

Start with observation only:

```dotenv
FABRIC_TENANT_ID=<tenant-guid>
FABRIC_CLIENT_ID=
PIPELINE_SWEEP_ENABLED=true
FABRIC_PIPELINE_TARGETS=[{"name":"Orders load","workspace_id":"<workspace-guid>","pipeline_id":"<pipeline-guid>"}]
PIPELINE_LOOKBACK_HOURS=24
PIPELINE_MAX_PAGES=10
PIPELINE_MAX_RUNS_PER_SWEEP=1
PIPELINE_RERUN_TABLE_NAME=triage_pipeline_reruns
```

Both `FABRIC_SQL_SERVER` and `FABRIC_SQL_DATABASE` are required for a live sweep.
The runtime uses managed/workload identity, not a client secret or an implicit
developer login. `FABRIC_CLIENT_ID` optionally selects a managed identity.
An operator running a local verification must supply an explicit credential;
the live client does not fall back to the operator's Azure CLI session.

```powershell
.\.venv\Scripts\bi-triage.exe pipelines --targets
.\.venv\Scripts\bi-triage.exe pipelines --preflight
.\.venv\Scripts\bi-triage.exe pipelines
```

`--preflight` checks configuration without connecting. A live sweep is the
reachability check. `TRIAGE_TOOL_MODE=mock` never queries Fabric.

The controller identity needs access to each configured item. The
[job read API](https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/get-item-job-instance)
documents user, service-principal and managed-identity support; delegated reads
use `Item.Read.All` or the applicable item-specific scope. The
[run API](https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job)
documents execute scopes and identity support separately. Delegated scopes do
not replace workspace/item authorization, and app-only access also depends on
tenant settings and the pipeline's connections and activities. Verify the
deployed controller identity, not just a successful call made as an operator.
For observation, workspace Viewer is the documented role option; executing
pipelines requires additional authorization, with Contributor or higher as the
workspace-role option. Prefer validated item-level grants where available.

## Approval-gated reruns

Reruns are disabled for a target unless an operator supplies both
`"rerun_safe": true` and a reviewed `rerun_parameters` object. `{}` explicitly
means no parameter overrides. It does not reconstruct the failed execution's
original parameters. A rerun uses the current pipeline definition and reviewed
configuration, so review defaults, date windows, watermarks and sink writes.
This is an operator attestation, not an automatic proof of idempotency. Review
the current revision and all reachable effects, including scripts, notebooks,
external calls and invoked pipelines. Missing output or zero copied rows does
not establish that no side effects occurred.

Parameterized Core submissions use
`{"executionData":{"parameters":{...}}}`, as in
[Microsoft's Fabric CLI implementation](https://github.com/microsoft/fabric-cli/blob/b7af89ba878b06cc96d9427e6d07516df5bf9678/src/fabric_cli/utils/fab_cmd_job_utils.py#L349-L421).
Do not replace that with the generic Core API's top-level typed parameter array,
or switch endpoints after an ambiguous submission. Both parameter-free and
parameterized requests still start a new full run, not an exact historical
replay.

The controller requires all of the following before asking for approval, and
rechecks execution prerequisites after approval:

- A confirmed failed scheduled pipeline job and available activity diagnostics.
- A matching retry-candidate playbook, with no matched non-retryable cause.
- No open incident already handling the failure, no active job and no newer job.
- Explicit replay-safety configuration and a reachable durable rerun journal.
- An explicit, matching, unexpired, unused approval for the target, source run
  and parameter fingerprint.

The model supplies a justification only. It cannot choose the workspace,
pipeline, failed run or parameters. Dataset refresh, dataset schedule and
Power BI deferred-retry tools are excluded from pipeline runs.

`rerun_fabric_pipeline` starts the whole pipeline. It is not rollback, resume
from the failed activity, notebook repair, connection repair or schedule
re-enablement. The pipeline may already have committed partial output. A
transient cause alone does not establish safe replay.

Before POST, the controller inserts a unique reservation into
`triage_pipeline_reruns`. A second reservation loses at the SQL primary key.
The submission is never automatically retried, including after transport
failure or a missing acknowledgement. `reserved` and `unknown` rows require
operator reconciliation; they do not expire into permission to submit again.
The normal reset command does not clear this journal.

The new job ID comes from the validated `Location` response header, never from
guessing which history row is newest. `Retry-After` is retained before polling.
HTTP 202 means submitted, not resolved. The correlated job must complete and
its activity evidence must show no failed or unverified activities before the
controller accepts resolution. A pipeline can report `Completed` after a
failure-handling branch; that does not make its failed Copy activity successful.

A still-running rerun remains unverified. Later pipeline sweeps poll its stored
job ID, including after controller reconstruction. A verified completion closes
the matching incident only if a newer failed occurrence has not replaced it.
A failed rerun remains an investigation and does not trigger another rerun.

External users and Fabric's scheduler do not acquire this controller's SQL
claim. They can start a job between the last history read and submission.
Configure pipeline concurrency and replay semantics accordingly; the controller
does not claim an atomic lock over all Fabric executions.

## Scheduling and deployment

Configure the target list and enable `PIPELINE_SWEEP_ENABLED` in the deployment
environment. Re-register the prompt agent because its tool catalog and procedure
changed, then redeploy the hosted controller:

```powershell
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py
azd deploy bi-triage-controller --no-prompt
azd ai agent invoke bi-triage-controller "pipeline sweep"
```

Deploy the existing scheduler template with its new supported command:

```powershell
az account set --subscription "<subscription>"
az deployment group create -g <resource-group> `
  --template-file infra\scheduled-sweep.json `
  --parameters name=bi-triage-pipeline-sweep projectEndpoint=<project-endpoint> `
               command="pipeline sweep" frequency=Minute interval=5 owner=<owner>
```

Use the invocation-role grant in [DeploymentGuide.md](DeploymentGuide.md).
The template's managed identity invokes the controller; the controller's
identity reads and runs pipelines. Do not grant data access to the prompt
agents. No new scheduler or monitored pipeline is created by setting the
configuration alone.

The default is one triaged failure per sweep. Rerun verification and skipped
historical observations do not consume that limit. Human approval can consume
the approval timeout in addition to the triage budget. Increase scheduler
timeouts and claim budgets deliberately before raising the per-sweep limit.

## Operational state

`triage_incidents.payload.pipeline_failure` contains the latest run and activity
evidence for a pipeline incident. The existing incident list and monitoring
cockpit include these rows without a second database or a new data path.
`triage_processed_messages` also stores namespaced processed pipeline-run keys.
`triage_pipeline_reruns` stores submission and verification state separately.

The [command center](CommandCenter.md#incident-records) uses the **Fabric
pipeline** workload filter and opens full incident pages from the queue
inspector. The filter remains available when there are no matching records;
selecting it does not configure monitoring. **New investigation** enqueues a
sweep of a configured pipeline target. It does not itself run the pipeline or
approve a rerun.

Operator or Admin users can append notes and record **Resolved by user** against
the current source revision. That is a tracking decision, not a verified Fabric
job result. It does not reset policy counters, release a rerun reservation or
authorize another submission. New controller evidence invalidates the closure.
Read-only discussion is retained with the incident and cannot dispatch tools.
These application roles do not grant the user or controller Fabric permissions;
the deployed controller identity still needs the item access described above.

Do not delete a rerun reservation to make a retry possible. First identify the
submitted job, check its activity results and sink effects, and reconcile the
incident. Automatic retries after an ambiguous acknowledgement can duplicate
data even when an HTTP client saw only an error.

Command-center **Human reconciliation** applies to interrupted or uncertain
command-queue entries. It records an administrator's review without executing
the command; it does not modify or clear `triage_pipeline_reruns`. Closing
incident tracking is not a substitute for either reconciliation.
