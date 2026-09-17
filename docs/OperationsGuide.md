# Operations

Operating the hybrid monitoring implementation: due work, coverage, off switches,
stored state and failure investigation. This is sample code, not a supported
service. All live application state uses one shared Azure SQL Database;
Power BI and Fabric remain the monitored workload/event services. The Command
Center is the operational UI, not the retained Rayfin sample. See
[DeploymentGuide.md](DeploymentGuide.md) for setup.

**Current readiness:** the SQL ownership contract is independently reviewed
offline; final component-store, controller and connector acceptance remains
separate. An isolated MI transport canary received the four observed wire
event types across manual failure, scheduled failure, success and cancellation.
It did not prove SQL durable handling or normal-worker readiness. The current
network baseline is public with Entra authentication; scoped SQL/registry
evaluation access is enabled and verified. The worker still has no ingress.
The original SQL `STARTED` proof receipt is under guarded recovery; completion,
full bootstrap commit and runtime permissions remain gates. Do not clear that
receipt or start a new operation to bypass uncertainty.

The public Foundry path is retained. The earlier private Foundry service error
is historical and does not block this architecture. The live app/controller
have not been cut over; no history migration/wipe, normal-worker rollout,
hybrid application push or current-release UI screenshots are complete.
Earlier private-network and Fabric SQL proof remains historical.
Keep the normal worker and controller heartbeat gated; see
[native proof status](DeploymentGuide.md#native-proof-and-bootstrap-status).

## What runs, and when

| Component/input | Work | Stop or limit |
|---|---|---|
| Monitoring worker | Due inventory/capability/poll work, owned-connector reconciliation and Eventstream consumption | Stop its deployment; use reviewed scope changes for per-target admission |
| Controller `reconcile_state` work | Deterministically validate worker observations/web intents and publish current authority; no agent or remediation | Current policy, work lease and original receipts govern publication |
| Controller `heartbeat` | Bounded fair draining of monitoring work and authenticated human commands | Disable its scheduler; deployment maintenance blocks new intake/actions |
| `pipeline sweep` / `bi-triage pipelines` in live mode | Queue observations for admitted registry pipelines | Disable/pause the scope; this is not a separate static target loader |
| `command sweep` | Drain human commands only | Stop the command drain; the web request itself does not run remediation |
| Optional `sweep` | Mailbox processing and due retries through current admission | Disable its timer; unset `GRAPH_MAILBOX` to stop mailbox intake |
| Optional `silent sweep` | Explicit semantic-health probes | Disable its timer or set `SILENT_SWEEP_ENABLED=false` |
| Web approval reply | Persist an authenticated, fingerprint-bound decision | No execution without current admission and valid approval; a recorded decision is not undone by closing the UI |

`infra\scheduled-sweep.json` defaults to a **disabled one-minute heartbeat**:
`command="heartbeat"`, `frequency=Minute`, `interval=1`, `enabled=false`.
Enabling a monitor setting does not create or start a timer. Prepare the workflow
disabled and grant its managed identity the reviewed Foundry invocation scope.
Only after current-release proof, enable the same reviewed deployment and verify
actual responses and durable work. Do not run overlapping old and new timers.

The heartbeat gives each queue bounded opportunities rather than allowing a busy
workspace or human-command backlog to consume every slot. Target poll cadence
belongs to the registry. The collector uses durable continuations, leases and
service/API budgets across replicas; a local semaphore is not a shared limit.

The scheduler's `PT15M` caller timeout exceeds the default combined
triage/approval/worker allowance (`300 + 300 + 30` seconds). A shorter timeout can
abandon a caller while an approval remains valid. HTTP invocation retries are
disabled: an ambiguous POST must not create overlapping work.

The permitted scheduler commands are `heartbeat`, `sweep`, `silent sweep`,
`pipeline sweep` and `command sweep`. Do not substitute free text; unrecognized
controller text can be interpreted as an alert.

### Native routines and optional timers

Foundry routines remain declared but disabled. Earlier routine state/dispatch
acknowledgements did not establish actual invocations, and code deployment did
not reliably apply enabled-state changes. Treat those as failure lessons, not
a claim about every current tenant. Reverify native dispatch before choosing
that trigger. A successful `azd deploy` is not timer readiness.

Mailbox and silent-failure scans are separate optional jobs. The mailbox filter
and mailbox confinement must be verified before enabling its timer. Silent
probes require their own business expectations and source permissions; event
silence is not a substitute for them. Avoid increasing probe cadence without
accounting for Power BI execute-query budgets and capacity load.

See [DeploymentGuide.md](DeploymentGuide.md#6c-scheduled-sweeps) for the reviewed
disabled-template command and invocation-role boundary.

## Modes and startup

Use `MONITORING_MODE=fixture`, `TRIAGE_TOOL_MODE=mock` and
`TRIAGE_PROVIDER_MODE=mock` for explicit offline fixtures. The runtime does not
select fixtures because live SQL, a connector or a source permission failed.

Live API/controller settings use `MONITORING_MODE=live`, the pinned
`MONITORING_TENANT_ID`, `AZURE_SQL_SERVER` and `AZURE_SQL_DATABASE`. Use the
`<server>.database.windows.net` hostname and catalog from the Azure deployment,
not Fabric item properties. Epoch, activation cutoff
and maintenance come from shared deployment control. Missing or incompatible
state fails closed. Static `FABRIC_PIPELINE_TARGETS` and compatibility loaders
are retired; an empty registry admits no workload.

The worker's `MONITORING_INVENTORY_MODE` defaults to `caller_visible`.
`tenant_admin_preview` explicitly selects the admin workspace/domain and preview
Admin Items adapters; the deploy helper exposes the same choice as
`-InventoryMode`. This is API selection, not a grant or proof of complete
tenant-wide operational visibility.

The MI consumer is a separate live-only process:

```powershell
.\.venv\Scripts\python.exe -m triage.monitoring.worker
```

It requires the explicit identity, owned connector, nonsecret endpoint and SQL
environment listed in
[DeploymentGuide.md](DeploymentGuide.md#worker-configuration-and-checks).
It does not read `.env`, accept a developer/secret fallback or expose an inbound
HTTP health endpoint. A blocked startup must remain visible, not silently become
a transport probe.

### SQL component and publication boundaries

Live `build_monitoring_store` and `AzureSqlMonitoringStore` construction must
select `worker`, `web` or `controller` explicitly. That selection is routing,
not a grant; the authenticated SQL principal must hold only its reviewed
component role. Checked views and static RPCs separate raw observations, human
intents and controller publication. Do not give a producer raw control,
source-head, action or receipt-table write permissions.

An accepted intent or intake receipt is durable pending work, not published
authority. The controller validates the original evidence and current policy
before publication. Missing/incompatible schema, `kernel_incomplete` or an
unsupported adapter must remain visible failures, not select fixture state or
another component's SQL route.

RPCs return typed envelopes with `status`, `affected_rows` and `result`. The
caller must decode the original result; an EXEC rowcount cannot establish
success. Conditional rowcounts inside the SQL implementation remain guards,
not the client RPC protocol.

All operational stores use the same Azure SQL catalog and
`AzureSqlDatabase.transaction()` for cross-store atomic work. Runtime identities
never install or repair schema. Configure Entra-only server authentication,
public firewall admission, TLS 1.2 minimum, Proxy/TCP 1433, auditing and TDE.
The default Azure-services rule has start/end `0.0.0.0`; it admits Azure-hosted
callers, including other subscriptions, not all Internet IPs. SQL permissions
remain mandatory. Azure SQL supports other authentication modes, so a token
login alone does not prove Entra-only policy.
The server Entra administrator is separate from the Command Center Admin role.
There is no SQL-password or Fabric SQL compatibility fallback.

### Separate canary and reconciliation modes

```powershell
.\.venv\Scripts\python.exe -m triage.monitoring.worker --transport-probe `
  --probe-workspace-id "<owned-source-workspace-uuid>" `
  --probe-item-id "<owned-source-pipeline-uuid>" --probe-seconds 120

.\.venv\Scripts\python.exe -m triage.monitoring.worker --reconcile-once
```

The finite transport probe is read-only and omits SQL acceptance/checkpointing.
Its source workspace can differ from the transport workspace. Receipt proves
only the checked transport/envelope boundary, not source REST evidence, durable
acceptance, controller execution or normal worker health. Never configure it as
an always-restarted worker command.

`--reconcile-once` drains a bounded shared connector-work batch and can mutate
an **owned monitoring** Eventstream definition. It cannot repair a business
pipeline, refresh a model or grant a role. It is not a generic readiness probe.
A pending or uncertain update keeps its original operation identity for
readback; do not create another connector or repeat POST blindly.

### Adding and removing Eventstream sources

Controller publication first records a logical source proposal with
`source_id=null`; no unresolved proposal may invent a physical ID. The worker
applies only the owned definition and records the actual observation. A later
controller publication passes the original `observation_receipt_id` so SQL can
validate that receipt and bind the returned component IDs. Controller code must
not query a worker-private receipt view to bypass this boundary.

A desired removal immediately fences new intake but retains the source's
ownership, node and ID bindings. It remains pending until an original, complete,
current observation proves the exact node, physical ID and stream-route absence.
Only then may controller publication retire it and record the immutable
retirement evidence. A failed update, partial page, null
`observed_definition_hash` or inherited snapshot cannot prove removal.

Keep `pending_removals`, `retired_sources` and `observation_receipt_id` distinct
when interpreting a publication result. Neither a saved removal request nor
a worker-reported `ready` state is published readiness. Key-free endpoint
automation remains unproved; retain the manual Entra-tab bootstrap.

## Coverage and recovery

Keep these observations distinct:

| Evidence | What it does not prove |
|---|---|
| Admin inventory/read metadata | Source access, complete operational history or action permission |
| Core workspace/item listing | Tenant-complete enumeration |
| A domain selection | A resource grant |
| A recent worker heartbeat | Event delivery, successful polling or complete coverage |
| An accepted event/REST page | Controller publication, agent completion or remediation |
| A complete returned history page | The requested lookback survived count-limited retention |
| A submitted refresh/rerun | Completion of the controller's own exact job |
| A human tracking closure | Verified repair, reset budget or released action fence |

Fabric histories generally retain 100 completed jobs; Power BI refresh history
uses the explicit 60-entry request window. Retention exhaustion, interrupted
pagination, 401/403/429, malformed records and unknown state must remain coverage
gaps. A failed page or incomplete domain inventory is not evidence of deletion.
Honor shared `Retry-After`; release work to its due queue rather than sleeping
past a lease.
The worker reports retention through `worker.observe_retention`; it cannot use
a complete-looking page to publish source-head or action authority.

Event acceptance preserves connector provenance and original event source/ID.
Source execution identity separately deduplicates poll/event/mail/operator
overlap. Only after durable acceptance or quarantine may the contiguous
checkpoint advance. A stream checkpoint is not an agent-completion marker.

Live state cannot degrade to an empty process cache. SQL unavailability blocks
admission, ownership, action reservation and completion. After recovery, read
current shared state and the original receipt. Preserve existing action fences;
resume verification/finalization rather than repeating an uncertain effect.

## Budgets and action boundaries

| Setting | Default | Bound |
|---|---|---|
| `TRIAGE_MAX_LLM_TURNS` | 14 | Reasoning turns per incident |
| `TRIAGE_MAX_TOOL_CALLS` | 20 | Tool calls per incident |
| `TRIAGE_MAX_WRITE_ACTIONS` | 1 | Remediations per incident |
| `TRIAGE_MAX_TOKENS` | 80,000 | Tokens across agents in an incident |
| `TRIAGE_TIMEOUT_SECONDS` | 300 | Incident reasoning/execution deadline |

These are controller limits, not prompt suggestions. Do not raise a policy
limit to make a scenario pass. Distinct source executions sharing a failure
signature do not automatically receive new incident allowances.

Targets default to observation only. Action requires current scope, immutable
review/fingerprints, authoritative source-head and active-job checks, any
required explicit approval, and an atomic target/action reservation. Recheck
after approval waits. Scope/review revocation prevents a new reservation; it
cannot retract a request already committed to the external service.
Existing reservations must retain their exact leased source-read,
verification and finalization paths after a policy or maintenance change.

An uncertain result retains its fence. A confirmed no-effect rejection is not
a global budget refund or permission to reuse an approval. Only the
store-authorized, single-use retry path can reuse its incident slot after
durable parent finalization and fresh admission.

Verify the exact submitted job and activity evidence, or the exact intended
configuration readback for a non-job action. A newly seen external refresh is
not proof that the controller's submission succeeded. Persist the terminal
incident, processed-source disposition and work completion through the shared
finalization boundary.

## Inspecting state

Configuration-only checks do not contact SQL or Fabric:

```powershell
.\.venv\Scripts\bi-triage.exe preflight
.\.venv\Scripts\bi-triage.exe pipelines --preflight
.\.venv\Scripts\bi-triage.exe health --preflight
```

Select the live SQL identity before the subcommand:

```powershell
$sqlIdentity = @("--sql-identity", "broker", "--operator-domain", "<operator-domain>")
.\.venv\Scripts\bi-triage.exe @sqlIdentity incidents
.\.venv\Scripts\bi-triage.exe @sqlIdentity approvals
.\.venv\Scripts\bi-triage.exe @sqlIdentity retries
.\.venv\Scripts\bi-triage.exe @sqlIdentity commands
.\.venv\Scripts\bi-triage.exe @sqlIdentity pipelines --targets
.\.venv\Scripts\bi-triage.exe @sqlIdentity preflight --check-sql
```

Managed-identity operator mode instead uses `--sql-identity managed` with an
explicit `AZURE_CLIENT_ID`. `--check-sql` proves a connection/`SELECT 1`, not
all object grants or the worker/controller identities.

Treat `retries --drain`, `commands --drain`, live `pipelines`, and `health --accept`
as operational requests, not read-only diagnostics. The first two can execute
eligible work, live pipelines queue observations, and accepting a health
baseline changes what future scans consider normal.

## Access and incident collaboration

The command-center Incidents page retains evidence, run history, append-only
notes, tool-free discussion and human tracking decisions. **Needs investigation**
includes wire status `needs_review`. Presentation labels do not rewrite stored
status values.

Operator or Admin can add notes or record **Resolved by user**. The decision
binds to the original SQL NVARCHAR payload hash over UTF-16 LE and its tracking
version. On a revision conflict, refresh and review the new evidence. A tracking
closure does not verify repair, change approvals/budgets/notifications or remove
an uncertain action fence. New evidence invalidates the prior closure.

Access & permissions reports validated token roles, not current group membership.
Ordinary Entra groups supply Reader, Operator, Approver and Admin; Operator and
Approver are separate. Admin is an application role, not directory or controller
service permission. There is no SQL membership authority or in-app directory
writer.

After **Refresh permissions**, actions remain locked until a snapshot under the
new token-refresh generation succeeds. Token renewal does not immediately revoke
other issued tokens or prove directory propagation. Do not repair access by
clearing incidents or restoring a retired SQL ACL.

Human reconciliation of an interrupted command is distinct from exact external
effect verification. A pending question may already be durable after a lost
reply; refresh its saved discussion instead of blindly repeating the mutation.
Observer answers cannot approve or execute anything.

## Controlled prototype reset

Ordinary releases preserve state. The approved prototype clean start is a
separate deployment operation: exact object manifest, expected epoch,
explicit operator confirmation and fresh live ownership/quiescence/action
evidence. No migration/import or old-target fallback is performed.

Use `scripts\reset_monitoring_state.py` as documented in
[DeploymentGuide.md](DeploymentGuide.md#3a-hybrid-registry-and-controlled-prototype-reset).
Its default is read-only planning. On the new Azure SQL target, install the
current application schema and initialize empty maintenance/control state.
The tool targets Azure SQL only and neither imports nor clears a prior Fabric
SQL database. Dispose of old prototype history through separately scoped cleanup.
Any reset of an already initialized Azure SQL target is a later, separately
confirmed step after **all** writers and uncertain effects are reconciled.

The same pinned operator may prepare and execute; no new signing authority,
certificate or second person is required. Caller booleans and saved observation
documents are not live quiescence proof. Protected deployment registration joins
actual SQL authority to independently observed writer resources; an operator's
profile cannot define completeness. Unknown module/trigger/invoker paths must
refuse. Reset receipts, registration/capture evidence and API rate budgets
survive; unrelated objects, Entra groups, infrastructure, endpoint namespaces
and business data do not belong to the wipe.

Retain original manifests, operation IDs and receipts after a timeout. A repeat
must reconcile the prior receipt, not wipe new-epoch rows. The tool leaves
maintenance enabled and starts no services. `bi-triage reset` refuses live mode.

## Failure investigation

**No event arrived.** Distinguish quiet source, disabled/changed source
configuration, denied consumer identity, endpoint mismatch, network failure and
SQL rejection. A transport heartbeat is not delivery proof. Obtain only
nonsecret Custom Endpoint metadata from the Entra tab; never call the
key-returning connection API or substitute a SAS connection string.

**A pipeline request remains queued.** Confirm current registry admission,
worker collection, complete source evidence and the separate controller
heartbeat. A web/CLI acknowledgement is durable intent, not execution.

**A target is paused or incomplete.** Inspect capability and inventory-generation
gaps. Verify the actual service identity. Domain membership/read-admin permissions
do not imply history access, event consumption or controller action permission.

**A prompt change had no effect.** Re-register the Foundry definitions before
deploying the controller. Local files do not replace an existing registered
version:

```powershell
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py
```

**A hosted response says success at HTTP level.** Inspect the Responses body
`status` and `error`, then the durable operation/finalization record. HTTP 200,
CLI exit status or an accepted deployment alone does not establish a completed
business outcome. The scheduler validates the body, including supported
base64 `$content` wrappers, rather than treating a wrapper as success.

**Mail was ignored.** Check the approved sender/subject filter and denied canary
scope. Invalid patterns fail closed. Send a matching synthetic alert; never
widen the security filter to make something trigger.

**A capacity incident deferred work.** Inspect the existing retry and action
state. Respect service backoff and correlated no-effect/uncertain dispositions;
do not add another immediate submission or clear its reservation.

## Telemetry and cost

Logs/spans carry metadata only, never prompts, completions or raw business
payloads. Worker heartbeats, scheduler history, API health, source reads and SQL
receipts are different signals. Configure alerting for stale workers, missing
inventory deadlines, denied/throttled reads, checkpoint lag, backlog and failed
finalization rather than relying on recent incidents alone.

The existing telemetry helper consumes `APPLICATIONINSIGHTS_CONNECTION_STRING`
without explicitly configuring an Entra exporter. A portal Insights link does
not establish secretless telemetry. Use the separately reviewed exporter/network
path; do not add a credential to make a dashboard appear healthy.

Budget for continuously allocated worker compute, the hosted controller, model
usage, Azure SQL compute/storage/backups/auditing, Fabric workload capacity,
public Basic registry, data transfer and logging. No NAT/private-endpoint
resources are required by the baseline. The application and temporary proof
databases share one server; the shipped template has no elastic pool and is
not a final sizing/pricing recommendation. Service
request budgets and source deduplication complement the per-incident token limit.
Investigate repeated near-identical incidents rather than raising limits.

Where the public Custom Endpoint topology requires a tenant-specific exception,
record the approved scope, owner and review/expiry operation. Tags and review
dates do not enforce shutdown or override Fabric policy. Recheck actual outbound
DNS/TLS and availability after governance changes; do not broaden unrelated
resource access.

In the approved MCAPS evaluation, the SQL server's resource-specific
`SecurityControl=Ignore` plus reason/review tags permits one 14-day period.
Removing/re-adding the tag does not restart it; a longer test requires an
approved exclusion. Registry/account exception maps are separate and scoped,
not defaults for ordinary customers. A public-access readback does not prove
an exception will remain active. Monitor expiry and fail closed on lost SQL
access rather than repeatedly flipping a governed setting.

Optional Command Center app/SCM caller filters are persistent and independent.
Empty lists mean public network reachability, not anonymous authorization.
Preserve approved filters during code-only deployments; there is no automatic
restore-to-private step. Retained private test infrastructure was not deleted
by this network change and needs separate ownership/cost review.
