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
SQL recovery is complete through append-only adjudication; the original failed
`STARTED` receipt remains unchanged. Schema and bootstrap proof captures are
historical. Current maintenance is `false`: the web app and hosted controller
use Azure SQL with correct acting-identity mappings, kernel roles
and application grants. Do not restore historical maintenance or grant state.

The no-ingress worker is deployed in explicit collector-only mode. Native
Fabric domain/workspace reads accepted 332 workspaces into SQL; authenticated
Include and Inventory selectors show 333 enabled options including the
placeholder. Item scanning remains partial, with source-permission 401s and
budget/throttling gaps. This is real metadata collection, not source-access or
remediation proof. No scope or action was automatically admitted.

Use `reconcile` for schema receipts and SELECT-only `reconcile-recovery` for
recovery acknowledgements. Historical recovery lookup keeps the original request,
hashes and, when needed, staged artifact data. `MISSING` or `CONFLICT` is not
permission to restart. Never clear a receipt or invent a new ID to bypass uncertainty.

The public Foundry path is retained. The earlier private Foundry service error
is historical and does not block this architecture. Event-enabled collection,
full RPC/source-access/restart coverage and sustained operation remain
separate acceptance work. The bounded collector/heartbeat evidence does not
prove those paths.
Earlier private-network and Fabric SQL proof remains historical.
Do not infer unattended monitoring from a successful controller invocation; see
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

The current deployment reused its existing one-minute command scheduler, changing
only its command to `heartbeat`. It created no second timer, enabled no mailbox
path and left the separate silent-sweep schedule unchanged. Three actual
recurrence responses were decoded and verified completed, not inferred from
HTTP status or CLI exit code.

The heartbeat uses two automatic and one human-command concurrent slots under
an 840-second monotonic clock beginning before lock acquisition. Slots refill
within bounded queue quotas while the remaining window covers the execution
allowance. Lock-wait expiry defers only that caller; it never cancels the holder.
Insufficient remaining allowance prevents a new claim; admitted work settles
under its existing policy and fences. Target poll cadence
belongs to the registry. The collector uses durable continuations, leases and
service/API budgets across replicas; a local semaphore is not a shared limit.

The scheduler submits one stored background response and polls the returned ID
for up to `PT15M`. This avoids the 120-second limit on a single Consumption HTTP
request. Scheduler runs are serialized. A pending response is not success;
polling-limit exhaustion or a non-completed terminal status fails the run.
HTTP invocation retries are disabled: an ambiguous POST must not create
overlapping work. Read failures retry only the original response ID, and
application recovery remains tied to SQL work/receipts rather than response
availability.

The existing platform alerts evaluate `RunsSucceeded < 1` over 15 minutes and
`RunsFailed > 0` over 5 minutes, every minute. Do not assume absent metric
samples become zero. Optional `applicationInsightsResourceId` and
`applicationInsightsLocation` add a runtime log-absence alert: completed-heartbeat
traces in 15 minutes are summarized into one count row, including zero when
none match, and `<1` triggers the alert.

The native healthy query returned 14 and the isolated empty query 0.
The isolated validation alert fired, then resolved after restoring its healthy
query; that validation rule is now disabled. The production runtime-absence
rule is enabled with no actions, and the platform alerts are unchanged.
No actual controller stop or Action Group/email/webhook delivery was tested.
Inspect Azure Monitor and scheduler history directly. See
[controller-health-alerts.bicep](../infra/controller-health-alerts.bicep).

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

Start the live-only MI worker without Eventstream dependencies when inventory
and REST collection are the intended mode:

```powershell
.\.venv\Scripts\python.exe -m triage.monitoring.worker --collector-only
```

It requires the explicit identity and SQL environment listed in
[DeploymentGuide.md](DeploymentGuide.md#worker-configuration-and-checks).
It does not read `.env`, accept a developer/secret fallback or expose an inbound
HTTP health endpoint. Collector-only mode rejects partial/residual event settings,
constructs no receiver/provisioner and writes a non-transport heartbeat with
`connector_id=null`. It cannot claim deliveries or connector readiness.
The deployment helper requires `-CollectorOnly` or `-ConnectorBootstrapFile`
exclusively; Bicep defaults to `collectorOnly=true`.

For event mode, omit `--collector-only` and supply the entire owned connector
and nonsecret endpoint binding. Missing settings must fail, not silently select
collector-only, fixture state or a transport probe. See the
[collector-only quickstart](DeploymentGuide.md#collector-only-quickstart).

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

For a previously created app-owned Eventstream, first use the separate
[metadata registrar](DeploymentGuide.md#register-existing-app-owned-connector-metadata).
It is SQL-connected, not local export: `--prepare --capture ... --output ...`
performs read-only checks, `--apply --plan ... --confirm-manifest-hash ...`
registers planned physical metadata with an immutable receipt, and
`--reconcile --plan ...` only reads that original receipt. Use the same explicit
server/database/tenant/deployer/credential flags throughout.

Capture the reviewed original create/ownership evidence and fresh complete
item/definition/topology with exact source IDs and nonsecret endpoint metadata.
The 15-minute capture window and hashes do not prove collector-MI access or
event delivery. Registration creates no schema or roles, changes no maintenance,
queues no work and publishes no admission, protected desired state or Ready.
Its receipt remains `registered_metadata_only` with identity/delivery unverified.
Native read-only preflight is not native registration-apply acceptance.

Retain the original plan after SQL or result-file uncertainty. The output writer
publishes a fully written/fsynced temporary file by exclusive hard link, so it
does not expose a partial final file or overwrite another result. This is not
a directory-entry power-loss guarantee. Missing/conflicting receipt evidence
does not license a new request or another blind write.

### First desired publication and delivery proof

Metadata-only physical ownership with no protected desired publication is
dormant when its sources have no current admitted read/event capability; ordinary
reconciliation must not remove those retained sources just because admission
is empty. Once a reviewed scope and fresh same-collector-MI read/event probes
are available, controller reconciliation can publish first desired state at
the same policy revision if desired state was absent. Do not manufacture a
new scope edit solely to force that first publication.

Fresh owned topology, source-Running and read probes establish event capability
only, never action capability. A degraded event receiver may collect delivery
proof only against the matching current protected publication, ownership,
policy, definition, source and endpoint. Reverify on publication-identity
change. Record actual identity-check time separately from later receive time.

Only a matching original durable stream receipt/position and accepted payload
hash can support controller readiness publication. Bind actual work/context,
owner/fence/revision and eligible original observation evidence; neither
connector equality nor inferred global lease state is proof. Heartbeats,
empty streams, quarantine and stale proof do not establish Ready.
Native registrar apply and new event-runtime readiness/durable-delivery
acceptance remain open.

### Source additions and revocations

Physical contraction requires affirmative current `removal_targets` from
effective disable/exclude/deletion authority. Resolve overlapping includes and
exclusions first; unknown domain/scope authority holds the decision. Expired
READ capability, incomplete inventory or absence from the current eligible list
does not authorize removing an owned source. Keep its identity and report the
gap while ordinary intake/action/readiness fences remain in force.

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

### Held removals and receipt-bound restoration

The restoration boundary is implemented and offline-tested but remains under
review. It is not a live-restoration or resumed-receiver acceptance record.
A source can still be physically present while its pending removal correctly
keeps intake held; a paused/degraded worker or fresh-looking manifest does not
make that intent safe to erase.

Only controller `source_removal_supersessions` may supersede an exact pending
physical `{removal_id, source_id}` under the current policy and work fences.
It is not a browser override, registrar operation, SQL bootstrap recovery or
reset command. Operators should distinguish:

| Observation | Required interpretation |
|---|---|
| Unknown/expired capability or incomplete inventory | Hold uncertainty; do not create physical removal authority |
| Exact original read-only presence receipt | Evidence to evaluate restoration, not permission or Ready by itself |
| Any applicable operation ID, retained pre-POST `INTENT_GAP`, missing/ambiguous history or attempted work | Possible dispatch/effect remains unresolved; a later present-source GET cannot waive it |
| Successful guarded supersession | New unready desired publication with preserved original audit and physical identity; fresh post-publication proof is still required |

The original complete `worker.observe_connector` receipt must carry an explicit
`ConnectorPresenceInspection`: read-only GET time, SQL-compatible definition
hash and the complete canonical physical-GUID/`Running` map. It must follow the
removal and be within 300 seconds. Do not substitute the latest connector
projection, a copied receipt, heartbeat, partial topology or caller proof flag.

READ must be freshly verified and the current observation admission must be
`reviewed` or `auto_detection_only`; neither grants action permission.
Restoration currently supports directly matched tenant/workspace/item includes,
not domain-only admission. Matching exclusions and unresolved domain-exclusion
authority block it. Explicit denied/blocked capability cannot be waived.
The selected retained source's unknown event status is not an exemption for
new sources or other failed capability checks.

Quiescence is connector-specific and receipt-bound. The original GET collection
must be completed under its exact released lease and completion receipt.
Other active or nonterminal attempted connector work blocks restoration.
A queued row is never-claimed only if it has zero attempts and retry attempts,
no lease payload or physical lease row/tombstone, and no execution/action/retry/
finalization lineage or target binding. An expired tombstone is still history; `attempts=0` alone
is insufficient.

The new immutable result retains original pending-removal objects in
`superseded_source_removals`; old receipts/history and source IDs are not rewritten.
Its desired publication has a new identity/time and cleared identity/delivery
proofs. Require new post-publication read/event capability, actual collector
identity/OID and receive/enqueue evidence before the first accepted delivery
receipt can establish Ready. Do not require Ready or `events_enabled` for that
first qualifying delivery, and do not bypass any other admission/provenance
check. Unknown or unsupported conditions remain held.

Never delete the intent or its history, issue operator SQL DML, re-register,
reset, or restore an old proof merely to turn a status green. Use the current
controller path and preserve the original evidence after uncertainty. See the
[full source contract](TechnicalArchitecture.md#receipt-bound-source-removal-restoration).

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

Distinguish estate coverage from scoped preview readiness. Snapshot counts are
deployment/estate-wide; **Inventory total** remains Unknown until all latest
discovery generations are complete. Complete A with three items plus partial B
with five does not establish a complete total of eight. A preview for A may be
ready while estate coverage remains Partial/Unknown because of B.
Workspace/domain metadata outages still block expansion, but explicit
disable/contraction can use stored admissions.

Activation revalidation may tolerate only unrelated data-revision drift after
the original scope is re-evaluated under the transaction lock with identical
reviewed material effects. New targets, capabilities, subscriptions, gaps,
permissions, TTL, epoch or policy changes refuse. A conflict or missing original
operation lookup does not license blind replay; native activation acceptance
for this source/UI correction remains pending.

A bounded priority proof
processed an original fresh discovery request on attempt 1 without manual
requeue behind 579 waiting/207 queued items. The selected workspace completed
ten pages with three items, including one unsupported Eventstream.
This does not prove first-scope activation or eliminate partial-window replay
churn, which remains unoptimized.

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

### Local SQL candidate checks

`scripts\prepare_azure_sql.py --request <request.json> --output <new-directory>`
prepares current source, ordered SQL, a bundle, Dockerfile and strict manifest
without credentials, network access, SQL or a build. Its separate
`--verify <candidate-directory>` mode checks local hashes and the readback model
only. The result remains `candidate_not_authorized`, with
`native_sql_proven=false` and `image_built=false`.

Verification also binds the candidate to the current trusted preparation
sources and regenerated source/SQL/check/ABI expectations, not merely to a
self-consistent manifest. Payload Python is never executed. Preserve a
historical artifact rather than rehashing it to fit current verification.

The current complete export includes 19 tenant-bound static service/API/
provisioning budget seed batches and the fixed `budget_policies` readback:
164 batches and 115 checks at this revision. Matching existing policies preserve
usage, window and cooldown state; policy-definition drift refuses. The readback
excludes those mutable counters and refuses missing, changed or extra policies.
Do not replay bootstrap or reset counters to repair a running deployment's
missing policies; use a separately reviewed one-off policy installation.
Earlier 145-batch/114-readback native evidence is not proof of this new candidate.

Use a new output directory and the explicitly reviewed target/bootstrap-MI
request; preserve existing candidates. Preparation does not grant runtime
access, initialize control, register writers or approve a recovery baseline.
Do not mistake a local verification result for fresh native evidence or
approval of current live grants. See
[local SQL candidate preparation](DeploymentGuide.md#local-sql-candidate-preparation).

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

The optional `dbo.triage_sql_bootstrap_receipts` and
`dbo.triage_sql_bootstrap_recoveries` journals are retained in place.
Reset validates their exact supported structure and rejects foreign keys or
runtime mutation authority when they exist; it neither creates absent journals
nor deletes their rows. They may appear only in reset `retained_counts`, never
`deleted_counts`. Unknown/lookalike accelerator objects are not exempted.

Ancillary-writer classification and writer quiescence are reset-only controls,
not routine startup requirements or approval of current runtime grants.
The reset classifier requires the declared role's actual transitive membership,
anchor-RPC grant and verified module/ownership boundary, not a matching principal
name. Approval-table SELECT is the reviewed surface; raw approval writes remain
unsafe. Do not stop or reconfigure an active service merely to make a reset-only
profile look empty.

Retain original manifests, operation IDs and receipts after a timeout. A repeat
must reconcile the prior receipt, not wipe new-epoch rows. The tool leaves
maintenance enabled and starts no services. `bi-triage reset` refuses live mode.

For failed bootstrap rollback adjudication, collect and independently review
the full empty/security baseline using the current trusted
`bootstrap.recovery_baseline_fingerprint(db, artifact.bundle)` reader.
Preserve target/receipt identity, source hash, collected evidence and its digest
separately; do not derive the expected baseline from unreviewed failed-target
metadata or a generic empty database. Local export does not supply that approval.
Fresh recovery checks the separately approved `empty_baseline_sha256` inside
the transaction before writing, with a current request window of at most
15 minutes. See
[empty rollback baseline evidence](DeploymentGuide.md#empty-rollback-baseline-evidence).

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

**Hosted metadata telemetry is application-owned.**
`TRIAGE_TELEMETRY_CONNECTION_STRING` supplies the locator and hosted export uses
managed identity. The standard `APPLICATIONINSIGHTS_CONNECTION_STRING` is
reserved for Foundry project monitoring; it was empty in the actual runtime
without a project connection even when a version definition displayed a value.
The CLI standard setting is unchanged and is not a hosted fallback.

Keep Foundry project tracing disconnected: connecting Application Insights
enables project-wide traces that can contain prompts/responses/tool content.
The app-owned public Insights resource is Entra-only and has optional
resource-scoped publisher assignments, with no project connection.
Hosted startup forces content capture off, disables the host's default
observability callback and exports only allowlisted metadata loggers.
Diagnostics retain counts, error types and sanitized source locations, not raw
exception text or SDK payloads.

The SDK also parsed an empty platform locator despite an explicit custom value.
Hosted configuration requires the explicit app-owned locator, then removes only
an exact empty platform value before SDK configuration. A nonempty value is
never removed/masked, the reserved variable is never redeclared and there is no
hosted fallback. CLI behavior is unchanged.

Native Application Insights queries now contain paired `heartbeat_started` and
`heartbeat_finished` records with completed status, elapsed times approximately
6,000-26,000 ms, queue counts and zero exporter failure/warning counters in the
captured runs. Console output, configuration and portal links alone still are
not ingestion receipts. Preserve this as bounded evidence, not a guarantee of
all future delivery. See [telemetry setup and public sources](DeploymentGuide.md#8-observability).

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
