# Hosted architecture and identity

The App Service command center, Foundry hosted controller and Container App
monitoring worker are separate deployment boundaries. All application state
shares one Azure SQL Database. The Command Center is the operational UI;
the retained read-only Rayfin cockpit is not a deployment/state dependency or
an alternative command writer.

This describes the independently reviewed SQL contract and in-progress hybrid
source integration, not a completed deployment. An isolated MI transport
canary received all four observed wire types across manual failure, scheduled
failure, success and cancellation. SQL durable handling and normal-worker
readiness remain unproved. All shipped infrastructure now uses public networking
with Entra authentication and no PE/VNet/NAT/private-DNS prerequisites.
Scoped evaluation SQL/registry public access has been enabled and read back;
SQL remains Entra-only with TLS/auditing/TDE, and registry admin/anonymous
access is disabled. The public Foundry controller path is retained with local
authentication disabled.

SQL recovery is complete through append-only operator adjudication; the original
failed `STARTED` receipt is unchanged. Proof and application schemas each
committed 145 DDL batches and passed 114 readbacks. The independent initialization
capture of `maintenance=true`, revision `0` and its bootstrap receipt is
historical. A separate no-VNet public-route job used encrypted TCP/FEDERATED
bootstrap-MI authentication and passed 114 application readbacks without
changing maintenance at that capture time. Proof jobs were quiesced and human
SQL administration restored afterward.

Command Center and the hosted controller have cut over to Azure SQL.
The controller uses the correct acting `ServiceIdentity`; three runtime EXTERNAL
users have kernel roles and reviewed application DML. Maintenance is `false`.
After reviewed SQL-only corrections, a deployed heartbeat completed the original
discovery-intent work, published its frontier `1/1` and queued one inventory
item, with zero actions and unchanged original receipt/control.
No further controller deployment was needed for that SQL fix.

The collector-only worker is deployed with its dedicated MI and no ingress.
Native Fabric metadata reads durably accepted 332 workspaces; authenticated
selectors are enabled. Broader item coverage remains partial, including dataset
401s without source permissions and budget/throttling gaps. No scopes or
remediation were auto-admitted. Three actual recurrences of the reused
one-minute heartbeat scheduler completed; portal-only alerts have no configured
delivery destination.
Eight bounded role cases and 19 SELECT controls remain distinct from full
27-RPC/event/restart acceptance. Earlier private bootstrap/image/metadata checks and the private Foundry service
preflight failure are historical. Microsoft tracing of that private attempt
is not a prerequisite for this public path, nor does the error establish
categorical lack of private-hosting support.
The completed collector-only and web/controller deployment does not establish
event delivery, complete source access or sustained recovery. Native paired
heartbeat metadata is now queryable in the app-owned telemetry resource.
Earlier Fabric SQL checks remain historical.
See the [release gates](../DeploymentGuide.md#release-gates).

## Components

```text
Browser -- Entra authorization code + PKCE --> App Service command center
                                                |
                                                |-- validated app-role claims
                                                |-- pending scope/review intents
                                                |-- durable human commands
                                                `-- notes/tracking/tool-free observer

Fabric Job events --> owned Eventstream Custom Endpoint
                              |
                              | public outbound TLS, Entra
                              v
                   monitoring worker (no ingress)
                              |
                              |-- inventory and capability probes
                              |-- due REST polling
                              |-- owned connector reconciliation
                              `-- raw SQL observations/receipts/work

One reviewed scheduler -- MI --> Foundry controller "heartbeat"
                                                |
                                                |-- monitoring + human queues
                                                |-- deterministic reconcile_state publication
                                                |-- exact source re-read
                                                |-- reasoning and policy
                                                |-- atomic action reservation
                                                `-- exact verification/finalization

Azure SQL Database (one catalog): tenant/epoch/control, registry, receipts,
            work, incidents, approvals, action fences, detector state,
            commands/run history, collaboration and deployment registration
```

The worker collects evidence and manages only owned monitoring topology. It
does not run an agent or submit a business workload action. The controller is
the action boundary. Prompt agents interpret supplied facts and propose tools
without holding workload permissions. The browser records authorized intent;
it cannot turn an answer or a button click into a direct Fabric/Power BI call.

The optional tool-free observer explains authorized recorded evidence. Neither
its answer nor its run journal proves that a repair occurred.

Worker observations and web intents are producers, not published authority.
The tool-free `reconcile_state` controller path validates them before publishing
registry/source/connector state. Only eligible source work proceeds to reasoning
and action. An accepted/configuring response must not be displayed as completed
publication or service readiness.

## Identity boundaries

| Principal | Responsibility | Not implied |
|---|---|---|
| Deployment operator | Explicit DDL/bootstrap/reset, deployment and reviewed external grants | Runtime identities do not inherit deployment authority |
| App Service UAMI | Required SQL configuration/history/command/approval/collaboration operations; optional observer invocation | Directory administration, workload remediation, runtime DDL |
| Monitoring worker UAMI | Verified inventory/source reads, stream consumption, SQL intake/checkpoints and owned monitoring reconciliation | Controller action authority or universal tenant visibility |
| Hosted controller identity | Invoke reasoning agents, execute permitted workload tools and persist shared outcomes | Automatic permission from a discovery result or human app role |
| Scheduler MI | Invoke the reviewed controller scope | SQL, Fabric workload or directory administration |
| Prompt agents | Reason about supplied evidence | Service grants to perform their proposed actions |
| Browser user | Present delegated API access with assigned `CommandCenter.*` roles | Collector/controller service access |

Read-admin inventory, source item access, stream-workspace access, controller
action access and human Entra roles are different permission planes. A domain
is metadata, not a grant. Core workspace/item lists are caller-visible; Admin
Items is preview and must be explicitly selected and authorized. Reading Power
BI refresh history requires semantic-model Write permission, despite using GET.

Azure SQL access requires an Entra-authenticated contained database user and
reviewed SQL permissions. Azure management-plane RBAC and Fabric item/workspace
access do not grant database DML. Configure the logical server's Entra
administrator and Entra-only authentication separately; Azure SQL also supports
SQL authentication, which this deployment must disable. A Command Center Admin
is not the server's Entra administrator.

Keep schema/reset DDL with a selected deployer. Runtime stores select an
explicit `worker`, `web` or `controller` component and use its checked
views/static RPCs. SQL roles, not the component argument, enforce that boundary.
Broad database/schema roles or raw monitoring-table DML defeat it; missing
adapters must fail rather than borrow another component's route.

### Command-center authorization

The API validates the Entra access token's signature, issuer, audience, tenant,
identity and lifetime. It requires delegated `access_as_user` and recognized
`CommandCenter.Reader`, `.Operator`, `.Approver` or `.Admin` claims. Operator
and Approver are separate. Admin includes app operations, not directory or
controller administration.

Four ordinary Entra security groups normally supply these roles. Group owners
and IT manage memberships externally; group-based assignment requires the
applicable Entra licensing and nested membership does not cascade. Assignment
and delegated consent are separate prerequisites.

Access & permissions reports effective token roles, not current directory
membership. Refresh permissions requests a new API token and keeps actions
locked until a snapshot from that refresh generation succeeds. It cannot revoke
already-issued tokens or prove directory propagation.

No SQL ACL or Graph directory membership lookup authorizes app requests.
Optional profile photos use separate delegated Graph `User.Read`. Never grant
directory-write permissions to the web UAMI to implement this page. The retired
`COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true` setting remains rejected.

### Workload and operator identity

Inspect each actual deployed identity after creation/recreation. Foundry's
`instance_identity` identifies the hosted agent; the explicit
`bi-triage identity --check-scope` operator command performs directory inspection.
Neither is a runtime human-authorization dependency.

A Foundry hosted agent may receive workload federation rather than IMDS managed
identity. Do not replace its verified credential path with MI-only assumptions.
The separate monitoring consumer deliberately uses its selected UAMI and checks
tenant, client, principal/object and resource bindings; it cannot fall back to
a developer or client secret.

Operator SQL commands require explicit identity selection. `bi-triage` uses
global `--sql-identity broker --operator-domain` or `--sql-identity managed`;
managed mode requires `AZURE_CLIENT_ID`. The deployment reset tool separately
supports explicit Azure CLI, Broker and MI selection, with tenant/principal
checks. A reset does not require a separate signer or another person.

For an Azure SQL Entra service-principal/UAMI user, the SID uses the **client
ID** in little-endian GUID bytes; users/groups use object IDs. Azure RBAC uses
the **principal object ID**, not that SQL client-ID encoding.
`CREATE USER ... WITH SID` avoids directory name lookup but does not verify
identity for the operator. Prove each runtime component's native stored SID
and actual MI sign-in on Azure SQL. Correct runtime mappings and grants are
installed, and web/controller plus bounded collector-only SQL paths have been
exercised. Event-mode and broader permission/effect coverage still require
their own proof. See
[CREATE USER](https://learn.microsoft.com/sql/t-sql/statements/create-user-transact-sql#arguments).

## Optional mailbox boundary

Mailbox support requires separate identity and confinement proof. A Graph
directory read does not establish Exchange-backed mail access; this difference
caused earlier unattended mailbox failures. Leave mailbox ingestion disabled
unless a supported secretless reader is verified. A client secret is not a
workaround for an unsupported identity path.

App-only mail access must be confined to the approved mailbox. The denied
canary must return 403; a successful read, 401/404, missing canary or error does
not prove isolation. The hosted reader also rejects a delegated `upn` token.
Entra group membership for app roles is a different boundary.

Keep the sender/subject filter unchanged during tests. Invalid patterns fail
closed, and ignored-message auditing must not widen intake when persistence
fails. Send a matching synthetic alert rather than making arbitrary mail
actionable. See [mailbox setup](../DeploymentGuide.md#2-optional-mailbox-ingestion)
and [Exchange RBAC for Applications](https://learn.microsoft.com/exchange/permissions-exo/application-rbac).

## Durable state and intake

`MONITORING_MODE=live` requires the shared Azure SQL baseline,
`AZURE_SQL_SERVER=<server>.database.windows.net`, `AZURE_SQL_DATABASE` and pinned
`MONITORING_TENANT_ID`. The hostname/catalog come from the Azure deployment,
not a Fabric database item. Tenant, epoch, activation cutoff and maintenance are
read from deployment control. `MONITORING_MODE=fixture` selects explicit
offline implementations, never a fallback from unavailable SQL. There is no
Fabric SQL compatibility layer, credential-string fallback or history import.

Static `FABRIC_PIPELINE_TARGETS` and compatibility loaders are retired. Scopes,
target admission, observation cadence and immutable safety reviews live in the
registry. Default admission is observation-only. Future resources require review
unless automatic detection-only enrolment was explicitly chosen.

Canonical target keys include tenant, epoch, workload, workspace and item IDs.
Source executions use authoritative workload-specific IDs; event transport uses
its original source/ID separately. A rename or duplicate delivery cannot create
another incident allowance.

REST pages and event receipts are accepted or explicitly quarantined before
advancing checkpoints. Continuation is not completed coverage. Durable stream
checkpoints advance through contiguous accepted/dispositioned positions, not
through agent completions. Quarantine is not execution permission.

All application stores share the catalog. SQL transactions, current shared
reads, uniqueness and conditional row-count winners enforce ownership. The
database connection is thread-bound; do not await inside
`AzureSqlDatabase.transaction()` or nest another transaction. Multiple
autocommit statements do not form one atomic operation.

`RpcContract.bind` preserves the exact named arguments, and callers use
`database.query` plus `decode_rpc_result`. The returned envelope's `status`,
`affected_rows` and typed `result` establish the RPC outcome; EXEC rowcount
does not. Current source publication and no-effect disposition use their
dedicated controller RPCs, not raw source/head/disposition writes.

Every live operational store fails closed rather than retaining an in-memory
substitute. Reconnection must read current state: an empty recovered cache
could answer "no open incident" and license another action. After a lost
acknowledgement, reconcile the original receipt/fence rather than replaying an
uncertain write.

Terminal incident/outcome, processed-source disposition and work completion
must be durably finalized. In-memory success cannot mark work complete after
SQL failure. Recovery resumes finalization or read-only verification.

## Execution and concurrency

The controller routes the latest inbound message:

| Input | Work |
|---|---|
| `heartbeat` | Bounded fair draining of monitoring work and human commands |
| `pipeline sweep` | Queue reads for registry-admitted pipelines in live mode |
| `command sweep` | Drain authenticated human commands |
| `silent sweep` | Run explicit semantic-health probes |
| `sweep` / `scheduled sweep` | Optional mailbox and due retry processing |
| Other alert text | Resolve through current target/source admission before action |

These are operational inputs, not interchangeable health checks. Empty input
selects the mailbox path. Routing by the whole conversation or message length
can re-triage a prior alert; the controller uses the current input instead.

All live action paths must retain current scope, exact source evidence,
authoritative source-head/active-job checks, immutable reviews and required
explicit approvals. Recheck after waits. Reserve the target/action in the same
atomic boundary that validates those prerequisites; a prior read cannot prevent
a revocation race.

Verify the controller's exact submitted job, including activity evidence for
pipelines, or the intended configuration readback for non-job actions. A
different concurrent refresh is not evidence of completion. Uncertain effects
retain their fences.

A confirmed no-effect rejection does not globally refund a budget or reuse an
approval. Its restricted retry may reuse only its bound incident slot after
durable parent finalization and fresh checks. Discovery, events and human
tracking never reset this state.

Human resolution appends tracking activity bound to the original SQL NVARCHAR
payload hash over UTF-16 LE. New evidence invalidates that closure; it does not
change automated budgets, approvals, claims or notification counts.

## Eventstream and network constraints

The deployed starting mode is `--collector-only`: durable inventory and REST
polling without an Eventstream receiver/provisioner. It rejects partial or
residual event metadata and records non-transport health with `connector_id=null`,
not an event-delivery assertion. The deployment helper requires `-CollectorOnly`
or a complete `-ConnectorBootstrapFile`, exclusively; Bicep defaults to
`collectorOnly=true`. See the [quickstart](../DeploymentGuide.md#collector-only-quickstart).
The worker's explicit zero-write/empty-allowlist policy is separate from controller
settings. Event mode omits the flag and retains the strict owned connector binding.

Azure SQL uses a public endpoint with Entra-only authentication, TLS 1.2 minimum,
Proxy/TCP 1433, auditing and TDE. `allowAzureServices=true` defaults to SQL's
special start/end `0.0.0.0` firewall rule. It admits Azure-hosted callers,
including other subscriptions, not all Internet IPs; SQL identity permissions
remain mandatory. Optional client rules specify exact IPv4 ranges.

The public [Foundry foundation](../../infra/foundry.bicep) disables local auth
and has no managed-network injection, private endpoint or network-approver
dependency. Its [capability-host template](../../infra/foundry-capabilities.bicep)
is optional and inspect-first. The
[monitoring prerequisites](../../infra/monitoring-prerequisites.bicep) create a
dedicated UAMI and public Basic ACR with scoped `AcrPull`, admin/anonymous access
disabled, Entra ARM authentication and `LegacyRegistryPermissions`.
The public Consumption environment uses keyless Azure Monitor routing, not
VNet/NAT prerequisites.

The selected transport is an owned Eventstream Custom Endpoint over public
outbound TLS with Entra authentication. It does not support tenant/workspace
Private Link. The worker has no inbound HTTP/TCP endpoint;
AMQP-over-WebSockets does not require a separate Azure Event Hubs namespace.
Optional private hardening of SQL or a model would not make this transport private.

Command Center uses public HTTPS with Entra API roles and Entra-authenticated
SCM publishing. Separate optional app/SCM CIDR lists are persistent; empty
lists leave the network public, not authorization anonymous. Basic publishing
and FTPS remain disabled.

Ordinary deployments contain no baked-in MCAPS exemption. Optional
`sqlNetworkExceptionTags`, `registryExceptionTags` and `accountNetworkExceptionTags`
are resource-scoped. The approved SQL `SecurityControl=Ignore` plus reason/review
tags permits one 14-day period; removing/re-adding does not restart it.
Longer tests need an approved exclusion. See
[governed evaluation exceptions](../DeploymentGuide.md#governed-evaluation-exceptions).

Obtain only nonsecret destination metadata from the **Microsoft Entra ID** tab.
The key-returning connection API must not be called. Supply namespace, entity,
consumer group and exact owned item/destination IDs through the reviewed
bootstrap/publication path; environment strings alone are not a connector
record. Key-free endpoint automation remains unproved, and another application's
Eventstream cannot be adopted as a fallback.

The [owned metadata registrar](../DeploymentGuide.md#register-existing-app-owned-connector-metadata)
uses explicit target/deployer flags for read-only prepare, exact-hash apply and
SELECT-only original-plan reconciliation. It records only planned physical
ownership and an immutable receipt, not scope admission, desired publication,
collector-MI verification or delivery. A fresh complete operator capture is
hash-bound evidence, not network proof; native read-only preflight does not
establish native registration apply.

Metadata-only sources without admitted current read/event-capable targets are
dormant before first protected desired publication, not automatically removed.
With current admission and fresh same-collector-MI event probes, first desired
can be published at the same policy revision when absent. A degraded receiver
then needs matching protected publication/ownership/policy/definition/source/
endpoint bindings and actual original durable receipt/position/hash evidence.
Keep identity-check and receive times distinct. Only the controller can publish
Ready; the new native event-mode/delivery gate remains open.

Sources start as logical proposals with `source_id=null`. Only controller
publication bound to the original complete worker observation may assign
returned physical IDs. The caller supplies `observation_receipt_id`; SQL
validates the worker receipt without giving the controller caller direct access
to worker-private receipt views.

Desired removal fences intake but retains ownership and IDs while pending.
The original current receipt must prove exact node/ID/stream-route absence
before an immutable retirement record is published. A null
`observed_definition_hash`, an uncertain response or an inherited snapshot
cannot prove absence or create readiness for a changed source set.

Environment-only deployment requires no worker image, SQL or endpoint metadata.
After that stage, collector-only startup needs the selected identity and ready
SQL/collector contracts, but no event endpoint. For a later event-mode deployment,
use a finite UAMI canary to create/inspect the owned definition, then bind the
nonsecret endpoint and perform separate reception/durable acceptance checks.
A create/readback result is not managed-identity consumption. No missing
contract may silently select a transport-only probe or another execution mode.

`--transport-probe` is a bounded, read-only worker mode with no SQL checkpoint
or normal-health claim. `--reconcile-once` can update owned monitoring definitions
through durable work and is not a read-only health probe. Never install the
finite probe as an always-restarted consumer.

Where tenant/workspace settings require an approved public exception, record
its actual scope, owner and expiry/review operation. An Azure tag cannot change
a Fabric network policy. Review dates do not enforce automatic shutdown.

Public references:
[Custom Endpoint destinations](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-destination-custom-app),
[Entra consumption](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/custom-endpoint-entra-id-auth),
[Private Link support](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/set-up-tenant-workspace-private-links),
and [read-only admin APIs](https://learn.microsoft.com/fabric/admin/enable-service-principal-admin-apis).

## Foundry routine observations

Native routines remain disabled by default. Earlier enabled-state and dispatch
responses did not establish real invocations, and code deployment did not
reliably apply routine enabled-state. Preserve that failure lesson without
assuming the same behavior in every current tenant.

The separate `infra\scheduled-sweep.json` defaults to a disabled one-minute
`heartbeat`. It uses MI, records run history, disables HTTP retries and checks
the Responses body. Enable only after proving the current controller and
durable execution path. Do not run overlapping old/new timers or treat an
HTTP/deployment acknowledgement as schedule readiness.

The existing one-minute command scheduler was reused by changing only its command
to `heartbeat`; no second timer or mailbox path was enabled, and the silent
schedule was unchanged. Three actual recurrence responses were decoded and
verified completed. Controller admission is 840 monotonic seconds including
lock acquisition, with two automatic and one human-command concurrent slots.
Refills obey queue quotas and need sufficient remaining execution allowance.
Insufficient allowance prevents new claims; it does not cancel the lock holder
or already admitted work.

Portal alerts cover `RunsSucceeded < 1` over 15 minutes and `RunsFailed > 0`
over 5 minutes at one-minute evaluation. Empty action lists mean no Action
Group/email/webhook delivery. Optional `applicationInsightsResourceId` and
`applicationInsightsLocation` add a runtime log-absence rule whose summarizing
query returns one zero-count row when no completed heartbeat exists.
Metric no-data is not assumed zero. An isolated query canary fired and resolved,
then was disabled; production log-absence stays enabled with no actions.
This did not stop the actual controller or test external delivery.

## Hosting and deployment checks

Keep `agent-framework-foundry-hosting` pinned exactly. Its date-stamped beta
releases can break startup without a major-version warning. Other source
constraints remain important:

- Foundry function tools use the flat Responses schema, not a nested
  `function` object.
- Registered prompt/tool changes require re-registration; local files do not
  replace an existing agent version.
- Some reasoning deployments reject `temperature`; successful registration
  does not prove invocation compatibility.
- Hosted `run()` returns an async iterable; making it a coroutine changes the
  host contract.
- Display-side Authorization redaction is not evidence of a corrupt token.

Preserve the App Service's approved ingress/SCM controls during code-only
updates. Verify public backend DNS, firewall admission and identity independently
from web liveness.
Keep optional telemetry metadata-only; a portal Insights link is not an
Entra-authenticated exporter or verified ingestion configuration.

Foundry injects the reserved `APPLICATIONINSIGHTS_CONNECTION_STRING` from project
monitoring; it was empty in the actual container without that connection.
Project tracing would collect potentially sensitive content across project
agents, so keep it disconnected. Hosted code instead uses app-owned
`TRIAGE_TELEMETRY_CONNECTION_STRING` with managed identity, metadata-only logger
allowlists, forced message-content capture off and the host default observability
callback disabled. The CLI standard variable remains separate, not a hosted fallback.

The public [application telemetry template](../../infra/application-telemetry.bicep)
creates Entra-only Insights against an existing workspace and optional publisher
roles, outputs only a resource ID and makes no project connection.
After requiring the explicit app-owned locator, the SDK correction unsets only
an exactly empty platform value, never a nonempty value. It adds no standard
variable redeclaration or hosted fallback. Native Application Insights queries
now contain started/completed heartbeat metadata with queue counts and zero
captured exporter failure/warning counters. Diagnostics expose only metadata,
not raw content. This is bounded ingestion proof, not full hybrid or overnight
acceptance. See the authoritative
[hosted variable](https://learn.microsoft.com/azure/foundry/agents/how-to/configure-hosted-agent-telemetry)
and [trace-privacy](https://learn.microsoft.com/azure/foundry/observability/concepts/trace-data)
documentation, plus
[Entra-authenticated Insights ingestion](https://learn.microsoft.com/azure/azure-monitor/app/azure-ad-authentication).

Deployment-only initialization/reset is separate from normal startup. Initial
maintenance bootstrap initializes the new Azure SQL target; it does not copy
history from the prior Fabric SQL store. Old-store disposal is separately
scoped cleanup, not a function of the Azure SQL reset tool. The approved
prototype reset requires an exact manifest/epoch, live writer/effect checks and
atomic receipts; ordinary releases do not erase operational state. Retain the
original operation after an ambiguous reply and never reset new-epoch rows.
Protected deployment registration and its capture receipts remain separate
from the resettable operational records and the kernel's physical-table map.

The bootstrap job remains read-only by default. The accepted multi-bundle image
keeps proof and application files distinct; select the exact `bundlePath`,
matching target, operation ID and SHA-256 rather than relabelling a bundle.
`recover` appends adjudication only; a separately approved `apply` follows.
Fresh mutation requires current runner/source, both approvals, a current
timezone-aware window of at most 15 minutes and `empty_baseline_sha256` binding
the full native empty/security catalogue. Approval does not waive unexpected
object/role/trigger checks.

`reconcile` reads schema receipts only. `reconcile-recovery` uses fixed SELECTs
with the original recovery request/hash, no mutation approvals, and returns
`MATCHING`/exit `0` or `MISSING`/`CONFLICT`/exit `2`. Historical expired requests
remain readable; missing/conflicting evidence does not authorize restart.
Only this read-only mode accepts `artifactRoot`/`--artifact-root`: stage the
original archive/request outside the new image and point `bundlePath` inside
the archive. The current trusted reader verifies original bundle/source/SQL
bytes as data; it never imports or executes archived code/SQL or rehashes an
original bundle to fit a newer runner. All other modes reject that flag.
Recovery files are operator-staged, not image-baked or environment payloads.
See the
[bootstrap operator interface](../DeploymentGuide.md#sql-bootstrap-operator-interface).

Native permission/updatability proof must use actual Entra MIs on an approved
isolated Azure SQL target. `WITHOUT LOGIN` and `EXECUTE AS USER` are supported
database-scoped test tools, not substitutes for MI authentication, network
admission or recovery proof. The provisioned temporary proof database shares
the logical server with the application database. That evaluation topology is
not a final sizing or pricing recommendation. The shipped SQL template uses
one S1 application database and an optional Basic proof database without an
elastic pool. Earlier bootstrap-MI proof
does not establish runtime-component users, memberships or permissions.

Follow [DeploymentGuide.md](../DeploymentGuide.md) for concrete operator flags,
worker preparation, endpoint binding and the controlled clean-start sequence.
The deployed collector and heartbeat have proved bounded metadata acceptance
and recurrence behavior. Event receipt, full source access, correlated executions,
restart and sustained recovery still need separate acceptance before full
unattended hybrid coverage is claimed.
