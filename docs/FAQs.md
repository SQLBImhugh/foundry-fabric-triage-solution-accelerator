# FAQs

## Can I run it without an Azure subscription?

Yes. With no configuration the accelerator runs against a deterministic
provider and explicit fixture state. Select those modes when a local environment
also contains live settings:

```powershell
$env:MONITORING_MODE = "fixture"
$env:TRIAGE_PROVIDER_MODE = "mock"
$env:TRIAGE_TOOL_MODE = "mock"
.\.venv\Scripts\bi-triage.exe list
.\.venv\Scripts\bi-triage.exe run scenario1-transient
```

That is the same path the test suite uses. No credentials, no network.

## Where is application state stored?

All live accelerator application state uses **one shared Azure SQL Database**:
monitoring configuration, intake/checkpoints, incidents, approvals, retries,
claims, action fences, commands, run history, notes and tool-free discussion.
Keeping the stores in one catalog lets receipts and cross-store finalization
commit atomically. Runtime identities use separate checked SQL interfaces;
they do not need separate databases.

Configure `AZURE_SQL_SERVER=<server>.database.windows.net` and
`AZURE_SQL_DATABASE=<database-name>` from the Azure deployment. Entra-only
authentication must be configured on the logical server; it is not Azure SQL's
only supported authentication mode. The public endpoint uses explicit firewall
admission, TLS 1.2 minimum, Proxy/TCP 1433, auditing and TDE, with no SQL
password/login configuration or credential-string fallback.

The prototype takes a clean start with no Fabric SQL compatibility, migration,
dual writes or old history/target import. The application and temporary isolated
proof databases are provisioned on the same logical server. That evaluation
topology is not a final sizing or pricing recommendation. The shipped template
uses one S1 application database and an optional Basic proof database, not an
elastic pool. SQL recovery is complete and both schemas are committed.
The initialization capture of `maintenance=true`, revision `0` is historical.
Maintenance is now `false`, and Command Center, the hosted controller and a
collector-only worker use Azure SQL with their reviewed identities.
Workspace metadata is durably populated, but item/source-access coverage is
partial and full event/hybrid acceptance remains outstanding.

Power BI models, Fabric pipelines, business data and native Eventstream transport
remain in their services. Moving **application state** does not move or replace
the workloads being monitored. Fabric capacity is not needed to host the Azure
SQL database, but is still required for the relevant Fabric workloads.

## What does public networking allow?

All shipped Bicep uses normal public networking without private endpoints,
VNets, NAT Gateways or private DNS. The worker and manual jobs still have no
ingress. Public reachability does not grant anonymous service access: SQL is
Entra-only, Foundry disables local authentication, ACR disables admin/anonymous
access, and the Command Center validates API roles and uses Entra for SCM.

SQL's default `allowAzureServices=true` creates the special firewall rule whose
start and end are both `0.0.0.0`. It admits Azure-hosted callers, including other
subscriptions, **not all Internet IPs**. It is not an identity or tenant
allowlist; the connecting principal still needs SQL permissions. Optional
`clientFirewallRules` admit exact IPv4 ranges. Web app and SCM client filters
are separate controls: their empty default lists make those network endpoints
public without granting API or publishing access.

Ordinary deployments contain no baked-in MCAPS exemption. In the approved MCAPS
evaluation, the resource-specific SQL `SecurityControl=Ignore` exception and
reason/review tags permit one 14-day period; removing/re-adding the tag does
not restart it. Longer-running tests require an approved exclusion. Registry
and Foundry exception maps must be separately scoped, not copied across the
resource group. See [network controls and exceptions](DeploymentGuide.md#governed-evaluation-exceptions).

## What does it actually change in my tenant?

Workload remediation requires current target/action admission, an allowlisted
action and available policy budget. By default that is one remediation per
incident. The Power BI path can refresh, rebind a gateway, re-enable a disabled
refresh schedule or defer a retry. Full-pipeline reruns additionally require
reviewed definition/parameters, replay-safety attestation, deterministic evidence
and explicit approval. Uncertain submissions are not automatically reissued.

Monitoring setup is separate: an Admin can change scope policies and queue
inventory or app-owned Eventstream configuration. The worker collects evidence
and manages those owned connectors; it does not execute a workload remediation.

Set `TRIAGE_MAX_WRITE_ACTIONS=0` to disable remediation while evaluating it.
Incident records, monitoring configuration/intake, reporting and audit writes
still occur. This setting is not a read-only switch for every application write.

## What does Monitoring setup configure?

An Admin selects the deployment tenant, a domain, a workspace or an item using
server-returned metadata. A scope has a positive include rule, exclusions,
supported workloads, polling/reconciliation cadence and an explicit choice
about future detection-only enrollment. Exclusions win; domain descendants are
optional. A domain or an inventory result is not a service-access grant.

Tenant/domain/workspace/item choices define inventory and polling scope. They
do not subscribe to every operational event at those levels. Supported native
Fabric Job-event sources are configured separately.

Preview the changes, permissions and gaps before activating the current plan.
Activation saves a versioned policy and queues work; it does not prove collection
is ready. Readers can inspect coverage and existing target safety reviews.
See [Monitoring setup](CommandCenter.md#monitoring-setup).

## Does discovery cover all Fabric operational telemetry?

No. Discovery lists resource metadata. Failure monitoring currently has
contracts for Power BI semantic models/datasets and scheduled Fabric Data
Factory pipelines. Other item types, including Notebook, Report, Lakehouse and
Warehouse, remain visible with an unsupported reason; they are not counted as
monitored simply because the service can list them.

Notebook activity failures are evidence inside a monitored pipeline.
Standalone notebook-job monitoring is not implemented. Missing expected starts,
disabled pipeline schedules, report usage, audit, capacity and tenant-wide data
quality need separate detectors or collectors. Existing silent-health probes
remain explicitly configured business checks.

## Can I populate workspace selectors before configuring Eventstream?

Yes. Use the worker's explicit `--collector-only` mode, or
`-CollectorOnly` in `scripts\deploy_monitoring_worker.ps1`. It performs durable
inventory/REST collection with no Eventstream receiver or provisioner.
The helper accepts that switch or `-ConnectorBootstrapFile`, exclusively.
Partial/residual event settings are rejected, not treated as a fallback.
See the [collector-only quickstart](DeploymentGuide.md#collector-only-quickstart).

In the verified deployment, native Fabric metadata reads accepted 332
workspaces. Both authenticated workspace selectors had 333 enabled options
including the placeholder, with no selector error alerts. This is not proof
that the worker can read every dataset: item enumeration remains partial,
Power BI dataset reads can return 401 without source permissions, and budgets/
throttling still bound collection. Latest partial coverage remains partial
even with zero configured scopes.

Metadata, source access, current observations and remediation authority are
separate. No scope or remediation was automatically admitted, and no extra
Fabric grants were added to make the selectors work. Collector-only heartbeats
have `connector_id=null` and cannot claim transport/delivery.

## How are live targets configured now?

Use the monitoring registry and **Monitoring setup**, not
`FABRIC_PIPELINE_TARGETS`. That static live target setting is retired. There is
no old target-list reader, migration or environment-target fallback.

`MONITORING_MODE=fixture` selects explicit offline state.
`MONITORING_MODE=live` requires the pinned `MONITORING_TENANT_ID`,
`AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, Entra service access and the
deployment-owned schema. The live web app requires the monitoring tenant to match its Entra
tenant. Missing or incompatible state is a visible failure, never an empty
successful fixture store. The normal event worker is live-only.

The approved prototype cutover creates a new epoch and activation cutoff; it
does not import old operational records. Reset is a separately authorized
deployment procedure, never a browser or startup action. See the
[controlled prototype reset](HybridMonitoringPlan.md#controlled-prototype-reset).

## What do Configuring, Partial and Blocked mean?

They are not synonyms for monitoring enabled. **Configuring** includes accepted
intent waiting for controller validation/publication, as well as connector
provisioning that still lacks its required result. **Partial** means
the reported coverage has gaps or incomplete inventory/capability evidence.
**Blocked** identifies a reported prerequisite or deployment failure.

Read the separate discovered, access-verified, admitted, current and
action-enabled counts. An unknown denominator must remain unknown.
Caller-visible enumeration is not proof of a complete tenant inventory.
Collector permissions are separate from the signed-in User's app roles;
Power BI refresh-history reads, for example, require semantic-model Write
permission on the service identity.

## Does a safety review approve a remediation?

No. An Admin configures one target action profile. An Approver separately
approves or denies one eligible, fingerprint-bound, unexpired, single-use
proposal. Admin includes Approver permissions, but saving configuration is not
that individual approval.

The safety-review form supports refresh, gateway rebind, schedule re-enable
and full-pipeline rerun profiles. A pipeline needs the current definition hash
and explicit reviewed parameters; `{}` is valid, missing parameters are not.
Verified replay also requires a fresh human replay-safety attestation. Gateway
intent contains canonical, sorted, distinct datasource IDs and the gateway ID;
schedule intent is exactly `{enabled: true}`.

An accepted review intent returns `state=pending`,
`publication_status=pending_validation` and the original `requested_state`.
It is not a published technical result. The current review can later become
`published`, with an actual result different from the request, such as
`unverifiable` after a request for `verified`. These response fields are
service-owned metadata, not browser-supplied proof.

A committed revocation intent immediately fences **new** action reservations,
even while publication is pending. It does not cancel an already-reserved
effect or its verification/finalization obligations. A pending revocation may
refer to an expired review and has no published `revoked_at` yet; that is not
evidence that the request failed.

The UI shows requested, recorded and publication state separately. Pending,
unverifiable and revoked records remain visible. Current capability,
definition/correlation or configuration proof and action admission are still
required. A review does not grant directory roles, widen observation scope or
replace the controller's budget, reservation and individual approval checks.

## What if a configuration save loses its response?

Keep the original request identity and reconcile its operation receipt. Do not
repeat the save with a new ID. A `404`, timeout or GET of an older current review
does not prove that the write failed.

The browser retains nonsecret recovery identifiers and the submitted
target/action/state/revision binding for safety reviews, never parameter text.
An operation receipt confirms the original committed request, not the latest
published authority. Its review can remain pending intent after the current
review has been published. The Reader-authorized operation endpoint returns
`{request_id, review}`; current-review lookup is a different read and cannot
substitute for that receipt.

A fresh current review and target snapshot are still needed before another
edit. Missing or mismatched receipts leave changes locked. Readers can inspect
and reconcile; only Admin with a successful current permission-generation
snapshot can mutate. Source implementation is not deployed recovery proof; see
[configuration receipt recovery](CommandCenter.md#configuration-receipt-recovery).

## How does the current review change from profile A to profile B?

The UI temporarily follows an accepted profile while publication catches up.
That lookup choice is separate from its original operation receipt.

Once A has a matching current published assignment, its temporary pin can
retire. A newer assigned B can also replace the pinned lookup, but only after
the UI verifies B's published target, action, review revision and current
policy binding. An old assignment or merely pending B does not establish the
change. Automatic and manual refresh preserve the same checks and reject
out-of-order results.

A's immutable receipt remains A's receipt after lookup moves to B. The switch
does not grant roles or bypass the permission-refresh lock; it only selects
the validated current profile for inspection and any separately authorized edit.
See [current profile and lookup freshness](CommandCenter.md#current-profile-and-lookup-freshness).

## Does hybrid monitoring need Activator, Power Automate or Eventhouse?

No. The selected path combines REST polling with native Fabric Job events
through an Eventstream custom endpoint and an outbound managed-identity worker.
Power BI polling remains required. The custom endpoint uses public outbound
TLS with Entra authentication; this does not authorize public inbound access
to the worker or grant access to unrelated services.

A Logic App can schedule the controller heartbeat. It is a queue wake-up
mechanism, not a replacement event collector or proof of healthy monitoring.
Entra custom-endpoint bootstrap and its nonsecret connection metadata still
require explicit operator setup. Fully automatic Entra endpoint bootstrap is
not an implemented UI guarantee; keys and shared-secret connection strings
are not a substitute for that remaining automation gap.
See [HybridMonitoringPlan.md](HybridMonitoringPlan.md).

## Which hybrid deployment checks are complete?

Historical W0 work verified managed-identity Eventstream creation/inspection and an
operator-identity Power BI refresh. An isolated transport probe then received
nine receipts across four exact wire event types. That is bounded transport
evidence, not durable SQL intake or proof of the normal monitoring worker.

An earlier six-finding SQL permission-kernel review closed offline. Final
Azure SQL integration acceptance is not declared complete. Offline closure is not native
managed-identity SQL acceptance, transaction/recovery proof or authorization to
apply runtime grants.

The public-network contract is implemented, and scoped evaluation SQL/registry
public access has been enabled and read back. SQL retains Entra-only auth,
TLS/auditing/TDE and its special Azure-services firewall rule. Registry
admin/anonymous access remains disabled, and the retained public Foundry path
has local authentication disabled.

Recovery is complete without changing the original failed `STARTED` receipt.
Append-only operator adjudication binds its proved empty rollback to one
approved replacement. Proof and application schemas each committed 145 DDL
batches and passed 114 readbacks. Independent initialization readback captured
`maintenance=true`, revision `0` and the bootstrap receipt; that is historical,
not current control state.

Eight bounded native SQL role/effect cases using `EXECUTE AS` and cleanup
passed. The later 19 native SELECT-predicate controls are also bounded; neither
set is the full 27-RPC or end-to-end acceptance matrix.
A separate no-VNet public-route job connected over encrypted TCP with FEDERATED
bootstrap-MI authentication and passed 114 application readbacks without changing
maintenance at that capture time. Proof jobs were quiesced and human SQL
administration restored afterward.
Earlier private bootstrap/image checks and module-hash controls remain bounded
historical evidence. The private Foundry preflight error does not block the
chosen public architecture or establish that private hosted agents are
categorically unsupported.

The web app and hosted controller have since cut over to Azure SQL with the
correct acting-identity mapping, kernel roles and application grants.
Maintenance is `false`. After reviewed SQL-only corrections, the deployed
heartbeat completed the original discovery-intent work and published its
frontier `1/1`. Independent SQL readback confirmed one inventory-worker item
queued, zero actions and unchanged original receipt/control.

The deployed collector-only worker subsequently accepted 332 workspaces.
Broader item/source-access coverage remains partial and the mode runs no
Eventstream receiver/provisioner. Three actual recurrences of the reused
one-minute heartbeat scheduler completed after response decoding. Portal-only
missing/failed-heartbeat alerts were deployed, but no notification delivery or
absence canary is claimed. Earlier screenshots remain historical.

Hosted telemetry's reserved platform locator was empty without a project
tracing connection. The source now uses a separate metadata-only app channel
and normalizes only an exact empty platform value before SDK configuration.
Native Application Insights queries now contain `heartbeat_started` and
completed `heartbeat_finished` metadata, with queue counts and zero exporter
failure/warning counters in the captured runs. This bounded proof does not
justify enabling project-wide prompt/content traces.

Full event/source-access coverage, checkpoint/restart recovery and sustained
operation remain acceptance gates. The final frontend test/lint/build
rerun follows the final integration handoff, not each intermediate source
change. Earlier-release screenshots remain labelled as earlier-release
evidence and must not be presented as the current deployment. See the
[current native proof status](DeploymentGuide.md#native-proof-and-bootstrap-status).

## Which model should I use?

Any model that supports function calling. `FOUNDRY_AGENT_MODEL` selects it.

The controller, not the model, decides what is permitted. Model choice affects
classification and explanations, not authority. When evaluating a swap, run the
scenarios and compare the tool sequences, not the prose.

Keep a second agent pair registered on a fallback model if you depend on this
running unattended. Model deployments can be throttled or retired.

## Why is the policy in code instead of the prompt?

Because a prompt is a request and code is a control. A model can be argued out of
prompt wording by unusual input; it cannot be argued past a function that refuses
to dispatch an action that is not on a list.

Every limit in `PolicyLedger` has a test proving it fires.

## Why does the data quality agent not fix anything?

It reports; the controller decides. Splitting investigation from authority means
a wrong diagnosis produces a wrong *recommendation*, not a wrong *action*, and it
keeps the permission surface on one component instead of two.

## What happens if nobody answers an approval?

Nothing happens. A timeout is a refusal, and so is an error, a malformed reply
and having no approval gate configured at all. Silence is never read as consent.

A denial does not consume the remediation budget, so one "no" does not disarm the
agent for the rest of the incident.

## Why does it poll the mailbox instead of subscribing?

Graph change notifications need a public HTTPS endpoint that answers a validation
handshake, plus subscription renewal before expiry. Polling has no such
dependencies. With a healthy scheduler and no backlog, arrival-to-next-poll
delay is up to one interval; evenly distributed arrivals average half an
interval. Processing time and backlog add further delay.

`GRAPH_INGESTION_MODE=subscription` is rejected at startup rather than silently
polling, because believing you have push while getting a poll is a latency
assumption nothing will correct.

## Why is the scheduled trigger a Logic App rather than a Foundry routine?

Foundry routines did not fire in the evaluation verified on 2026-09-02, six days
after registration: the routine reported itself enabled, accepted dispatches,
produced no runs, and telemetry showed agent activity only in hours when a
person invoked it by hand. `azd deploy` does not manage routines either.

Both routines are declared in `azure.yaml` and ship disabled, with the evidence
in the file. The earlier release used
[`infra/scheduled-sweep.json`](../infra/scheduled-sweep.json). Its current
`heartbeat` command drains both monitoring work and human commands in bounded
rounds. Create the scheduler disabled and verify the new deployment before
enabling it. Re-test routines in your own tenant; earlier scheduler evidence
does not prove the hybrid event path.

## What is a "silent failure" and why does it need its own detector?

A refresh that reports success while the source never landed, or a table that
loads a tenth of its rows. No alert is raised, so no alert can be triaged, and
the report is simply wrong until somebody notices.

The detector queries semantic models directly on a schedule and compares against
a recorded baseline. It uses none of the agent tools, deliberately: a model asked
whether a 60% row drop is acceptable will sometimes say yes.

It is off until `SILENT_HEALTH_PROBES` is configured, because "fresh" is a
business question per model and guessing it produces the false positives that get
a detector muted.

## Which models can the detector not see?

Direct Lake models, which do not support app-only callers, and models relying on
single sign-on or row-level security. Those are reported as detector faults
rather than as healthy, so an unmonitorable model is visible rather than assumed
fine.

## Can I use this for something other than Power BI?

Scheduled Fabric Data Factory pipeline triage is implemented for
registry-admitted targets. It reads failed scheduled jobs and activity
evidence; it does not infer missing starts or disabled schedules. Notebook
activities can be evidence within a pipeline, but standalone notebook-job
monitoring is not implemented. See [PipelineTriage.md](PipelineTriage.md).

The policy ledger, allowlists, approval gate, signature and suppression logic,
incident model and outcome validation can also be reused for another domain.
Domain-specific tools, playbooks and parsing still need implementation.

See [`CustomizationGuide.md`](CustomizationGuide.md), which has a section on
moving to a different domain.

## What does the command center add?

The [command center](CommandCenter.md) provides Monitoring setup, coverage and
target safety reviews alongside the authenticated queue, full incident pages,
run history, tool timelines, durable notes, tool-free discussion and approvals.
It is the operational UI over the same Azure SQL application database as the
controller and worker; it does not own that database. The retained read-only
Rayfin Fabric App is not a deployment or state dependency. Its earlier
semantic-model binding does not establish a current Azure SQL read path.

Teams is optional. **New investigation** records a request for an admitted
target; the controller heartbeat drains that human-command queue alongside
automatic monitoring work. `command sweep` remains a command-only entry point.
A queued receipt, an approval decision or a completed observer answer is not
proof of remediation.
An empty target list never selects a resource implicitly.

## Who can use the application?

The API validates Entra app-role claims on every authenticated request. Reader
can view records, monitoring coverage and existing safety reviews, and ask
read-only questions. Operator adds investigations,
notes and human tracking resolution. Approver adds approval/denial decisions,
not Operator permissions. Admin has all app capabilities, including scenario
validation, monitoring/safety-review configuration and uncertainty
reconciliation, but no Entra directory
administration or SQL server Entra-administrator authority. These roles grant
no direct Azure, Fabric or SQL access.

Use the four ordinary Entra security groups assigned to those roles; group
owners manage membership and authorized IT administrators manage assignments.
Group-based assignment requires Entra P1/P2 and does not cascade through nested
groups. See [Entra-managed groups](CommandCenter.md#entra-managed-groups).

## How do I change or refresh permissions?

Request the appropriate group membership from its owner or IT in Entra.
**Access & permissions** is a Reader-visible, read-only view of the current
token's roles and issued/expiry timestamps. It does not read current group
membership or offer Add/Edit user, invite or local-grant operations. The SQL
permission editor and importer are retired; old `?view=admin` links open this
read-only page.

After a change, select **Refresh permissions** to request a fresh API token and
reload access information and the snapshot. Mutation controls remain locked until
the new permission-refresh generation has a successful snapshot; an older page
snapshot cannot restore capability. Entra changes may take time to propagate; refreshing
one session does not revoke other sessions' already-issued tokens. Profile
photos use a separate delegated Graph `User.Read` token, not a directory access
or authorization-management permission.

## What does Resolved by user mean?

An Operator or Admin explicitly closed human incident tracking with a reason.
It is an append-only decision bound to the reviewed source revision and tracking
version, not an agent-verified repair. It does not reset remediation budgets,
approve a proposal or clear an execution uncertainty block.

A changed source payload invalidates the older closure without deleting its
history. The source revision hashes the original SQL NVARCHAR payload using
SHA-256 over UTF-16 LE, not a reserialized model. A conflicting resolution
request returns `409` and requires a fresh
review. Notes and the saved question/answer thread remain available; the
read-only observer cannot act on requests to repair or approve anything.

## What does Scenario validation prove?

It checks the canonical cases advertised by the API against application code,
using synthetic tools and isolated state. Mock validation is deterministic;
Foundry validation also
uses registered agents and model calls. Both preserve production incident state.
A pass means the case's expectations matched, including expected refusals,
escalation or pending verification; it does not mean a production repair
succeeded.

The page is Admin-only. At most two cases run concurrently. **Stop queue** stops
new requests, not already accepted work. Review the durable result and run ID
after an uncertain response; cases are not automatically retried. See
[validation and evidence](CommandCenter.md#validation-and-evidence).

## How do I know it is still running?

Inspect scheduler run history, controller responses, durable worker/SQL
heartbeats and coverage separately. The deployed Azure Monitor alerts use
`RunsSucceeded < 1` over 15 minutes and `RunsFailed > 0` over 5 minutes, evaluated
every minute. Their empty action lists create portal alerts only: no Action
Group, email or webhook delivery destination has been configured. No
missing-heartbeat canary or external notification delivery is claimed.
The legacy `alertWebhookUrl` is not a prerequisite or the current alert route.

This is not hypothetical. An unpinned dependency once crash-looped the container
at startup, and because nothing was watching, the agent answered nothing for
hours until someone invoked it by hand. The hosting library is pinned exactly
now, and a test fails if that pin is loosened.

Inspect the monitoring coverage timestamps, completed poll window, receiver
activity, checkpoint lag, backlog and next due work as well as scheduler
history. `/api/health` is process liveness, not evidence that SQL, polling,
events or action verification work.

The incident list is useful for inspecting recorded outcomes:

```powershell
bi-triage incidents
```

A quiet incident list or an idle heartbeat is not proof of health. There may
be no failures, no configured targets, incomplete inventory or a disconnected
collector; coverage and its explicit gaps distinguish those cases.

The existing one-minute command schedule was changed to `heartbeat`, not
duplicated. Three real recurrences were decoded as completed. The controller
has an 840-second monotonic admission window, including lock wait, with two
automatic and one human-command concurrent slots. Refills obey queue quotas
and require enough remaining execution allowance; the timer never cancels
already admitted work.

## Why is Foundry project tracing disconnected?

Microsoft's [hosted telemetry documentation](https://learn.microsoft.com/azure/foundry/agents/how-to/configure-hosted-agent-telemetry)
reserves `APPLICATIONINSIGHTS_CONNECTION_STRING` for project monitoring.
Connecting Application Insights to the project enables traces across its agents
that can include prompts/responses/tool content; see
[tracing and data handling](https://learn.microsoft.com/azure/foundry/observability/concepts/trace-data).
That conflicts with this accelerator's metadata-only telemetry rule.

Hosted code instead uses `TRIAGE_TELEMETRY_CONNECTION_STRING` and managed identity
for a separate Entra-only application telemetry resource. It disables the
host's default observability callback, forces message-content capture off and
exports only approved metadata. CLI telemetry keeps its standard setting; there
is no hosted fallback to it. Console diagnostics and successful SDK configuration
are not cloud-ingestion receipts. The current proof uses records actually
queried from Application Insights: paired started/completed heartbeat metadata.
It does not establish every span, overnight delivery or full hybrid acceptance.
