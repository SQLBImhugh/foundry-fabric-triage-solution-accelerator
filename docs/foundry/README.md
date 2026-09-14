# Hosted architecture and identity

This describes the current deployment boundaries and the earlier runtime
observations that explain them. Historical observations are not a guarantee
that a preview limitation still applies in every tenant. Reverify platform
behavior and permissions before changing the corresponding guard.

The App Service command center and Foundry hosted controller are separate
deployments. The command center runs a Python 3.13 API and a Vite UI; the
controller runs `src\app.py` in a Foundry-hosted Python 3.13 container. They
share a standalone Fabric SQL Database. The read-only Rayfin cockpit remains
a separate client and does not own the state database.

## Components

```text
Browser -- Entra authorization code + PKCE --> App Service command center
                                                |
                                                |-- validated API app-role claims
                                                |-- Fabric SQL: history, queue,
                                                |   approvals, incident activity
                                                `-- optional tool-free observer

Logic App schedules -- managed identity --> Foundry hosted controller
                                                |
                                                |-- command sweep: durable queue
                                                |-- pipeline sweep: explicit targets
                                                |-- silent sweep: configured probes
                                                |-- mailbox sweep: optional, scoped
                                                |
                                                |-- bi-triage: reasoning
                                                |-- bi-data-quality: evidence report
                                                |-- Power BI / Fabric tools
                                                `-- Fabric SQL: durable state

Foundry routines in azure.yaml: declared, disabled pending verified scheduling
Rayfin cockpit: separate read-only view; no app/database ownership transfer
```

The controller decides and executes remediation. Prompt agents propose or
interpret; they have no workload permissions. The web API records authenticated
operator intent and human collaboration, but does not turn a browser request
or observer answer into a direct Power BI/Fabric action.

The optional `bi-triage-observer` receives authorized recorded evidence with
no tools. A records-based explanation is also available and is the default.
Its run journal is not proof that remediation occurred.

## Identity boundaries

| Principal | Responsibility | Not granted for this responsibility |
|---|---|---|
| Deployment operator | Register the app, provision groups, install schema, assign external permissions and deploy code | These privileges do not transfer to an app role or runtime identity |
| App Service UAMI | Read permitted SQL objects, persist queue/history/approval/collaboration operations, invoke an enabled observer | Directory administration, mailbox access, core-incident writes, runtime DDL |
| Hosted controller identity | Invoke reasoning agents, execute permitted workload tools and maintain durable controller state | App/group provisioning authority |
| Scheduler managed identity | Invoke the hosted controller at a reviewed Foundry scope | SQL, Power BI or directory administration |
| Prompt agents | Reason about supplied evidence | Power BI/Fabric SQL/mailbox permissions |
| Browser user | Present delegated API access with assigned `CommandCenter.*` roles | Workload service permissions merely by receiving an app role |

Fabric SQL access combines Fabric item authorization and SQL permissions; it
is not an Azure RBAC role named "Fabric SQL". The web UAMI needs Read item
access and the object-scoped grants in
[DeploymentGuide.md](../DeploymentGuide.md#web-uami-object-permissions).
Broad workspace or database roles can bypass that intended scope.

### Command-center authorization

The live API validates an RS256 Entra access token's signature, issuer,
audience, tenant, identity and lifetime. It requires delegated `access_as_user`
and recognized `CommandCenter.Reader`, `.Operator`, `.Approver` or `.Admin`
claims. Operator and Approver are separate capabilities; both include Reader.
Admin includes all app capabilities, not directory administration.

Four ordinary security groups normally supply those roles. The secretless
registration script preserves role IDs and enforces
`appRoleAssignmentRequired=true`. The group provisioning script plans offline
unless explicitly run with `--apply` by an authorized operator. It checks
licensing, ownership/name collisions, owners, administrator membership and
assignment readback without deleting existing direct assignments.

Group owners and IT manage membership outside the app. Group-based application
assignment requires Entra P1/P2; nested groups do not cascade. The selected
administrator's license check is not an audit of every user's entitlement.
Assignment and delegated consent are separate requirements.

Access & permissions is read-only. It reports the presented API token's
effective roles and dates, not current directory membership or which group
supplied a role. Refresh permissions forces a fresh API-token request and
reloads the snapshot, but cannot revoke already-issued tokens. Entra
propagation and token renewal still apply.

There are no runtime Graph directory membership calls/writes and no SQL
permission-table authority. Entra signing-key discovery is not a directory
membership lookup. The optional browser profile photo uses a separate
delegated Graph `User.Read` token. Do not grant the web UAMI
`AppRoleAssignment.ReadWrite.All` or `Group.ReadWrite.All`.

`COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true` is rejected. For an older
deployment, follow the [ordered cutover](../DeploymentGuide.md#existing-deployment-cutover):
prove Entra Admin before disabling the old flag, remove a reviewed direct
bootstrap grant only after group setup, then prove fresh-token group access
before retiring the legacy permission table. Preserve operational data.

### Foundry agent identity observations

The evaluated Foundry agents had first-class Entra agent identities. Directory
inspection used the subtype collection
`GET /v1.0/servicePrincipals/microsoft.graph.agentIdentity`; the attempted
top-level `/agentIdentities` path returned
`Resource not found for the segment`.

Earlier identity inspection observed:

| Property | Observation, not a hardcoded guarantee |
|---|---|
| Blueprint credentials | `keyCredentials=0`, `passwordCredentials=0` |
| Authentication | A federated credential named `fmi-fic`, audience `api://AzureADTokenExchange` |
| Accountability | Foundry populated sponsors at creation |
| Client ID and object ID | Equal for those agent identities; ordinary service principals differ |

`bi-triage identity --check-scope` is an explicit operator directory inspection,
not a runtime web authorization dependency. If that Graph inspection is
unavailable, the hosted definition's `instance_identity` identifies the
controller without a directory query. Recheck after creation/recreation rather
than assuming an earlier identity or sponsor is still current.

For Fabric SQL, a service principal/UAMI's SID is its **client ID**, converted
to little-endian GUID bytes. Users/groups use their directory object IDs.
`CREATE USER ... WITH SID ..., TYPE = E` avoids a directory name lookup but
does not verify the identity for the operator. See
[Fabric SQL authentication](https://learn.microsoft.com/fabric/database/sql/authentication).

## Mailbox compatibility and scope

Mailbox ingestion is optional. Earlier hosted tests isolated a service
compatibility difference: the same agent token returned 200 for a Graph
directory read and 401 for Exchange-backed mail, with `Mail.Read` present,
a real mailbox and an Application Access Policy. Power BI/Fabric and SQL
accepted the configured controller identity, but that did not prove mail access.

The source still retains a conventional mailbox client-secret fallback and
`GRAPH_CLIENT_SECRET` environment wiring. This is a legacy integration, **not
the current secretless command-center prerequisite**. Leave it unconfigured
and the mailbox schedule off unless a supported secretless reader has been
verified separately. Do not create a secret to work around the identity
boundary or describe a Graph directory success as mailbox proof.

App-only Entra `Mail.Read` is tenant-wide unless restricted. An earlier scope
test showed that an integration intended for one mailbox could read an
unrelated administrator mailbox. The scope control must therefore be proved,
not inferred from configuration.

For the legacy Application Access Policy path, the scope is a mail-enabled
security group containing the approved mailbox. A managed identity's app ID
can also be used where that service path is supported. For new mail
integrations, review
[Exchange RBAC for Applications](https://learn.microsoft.com/exchange/permissions-exo/application-rbac);
its scoped grants do not cancel separate unscoped Entra permissions.

The controller's checks fail closed:

- The actual reader must read the intended mailbox and be denied a distinct
  canary mailbox. Only **403** proves the configured canary refusal; 200,
  401/404, missing canary or a check error does not.
- A token carrying `upn` is refused by the hosted mailbox path; an operator's
  delegated identity is not unattended reader evidence.
- The sender allowlist and subject filter must match. Invalid regexes also
  fail closed. A sender address alone is not proof that the body is trusted.

Do not broaden the filter to make a test mail trigger. Earlier unfiltered
ingestion treated unrelated directory-security digests as BI incidents; the
filter is an injection boundary, not inbox housekeeping. Ignored-message
counts and reasons make a misconfigured filter diagnosable.

## Durable state

State is authoritative across invocations, not process-local convenience.
Open-incident lookup prevents a repeated alert from licensing another
remediation. Keeping state only in a hosted object's fields or its filesystem
does not survive reconstruction/recycling.

Fabric SQL replaced the older key-value store for these reasons:

- Claims/leases use one conditional statement, such as
  `UPDATE ... WHERE expires_at < SYSUTCDATETIME()`, whose `rowcount` identifies
  the winner. Expiry uses server time, not each container's clock. Earlier live
  contention testing with eight threads produced exactly one winner.
- SQL accepts Entra tokens, with no SQL-login/shared-key fallback.
- `mssql-python` supplies the driver in its wheel, including the evaluated
  Python 3.13 Linux build. The Foundry remote build installs Python
  requirements; it does not provide an apt step for `pyodbc`'s separate
  `msodbcsql18` dependency.

The connection layer uses one connection per thread and autocommit for its
single-statement operations. Serializing one connection across concurrent
callers previously produced `OperationalError`, then `InterfaceError` after
that connection was torn down.

Recovery is part of the store contract. The older backend became unreachable
after public access was disabled; its one-time startup fallback stayed
in-memory after connectivity returned, losing three invocations before a
redeploy. Stores now retry on use and reload after recovery. An empty recovered
cache would still answer "no open incident" incorrectly.

Different state has different failure handling. Some core stores degrade
loudly; claims/approvals must fail closed rather than guess permission to act.
The web command/history and incident collaboration stores require durable SQL
and do not report an in-memory substitute as successful persistence. Schema
installation is an operator task for the web path.

Human resolution appends a tracking decision to `triage_incident_activity`.
It binds to the original NVARCHAR payload's SHA-256 revision (UTF-16 LE bytes)
and tracking version. A later evidence change invalidates the closure without
resetting the automated incident, action budget, notification count, claims,
approvals or retry history. It is not an agent-verified repair.

## Execution and concurrency

`src\app.py` routes on the latest inbound message, not the whole conversation:

| Input | Work |
|---|---|
| `sweep` or `scheduled sweep` | Drain mailbox and due retries |
| `silent sweep` | Run configured semantic-health probes |
| `pipeline sweep` | Inspect explicitly configured scheduled Fabric pipeline runs and verify submitted reruns |
| `command sweep` | Drain authenticated operator work from the durable queue |
| Other text | Interactive alert triage |

These are operational commands, not interchangeable health probes. An empty
message is a mailbox sweep. Routing by message length was removed because
Foundry supplies conversation history: a short sweep input could otherwise
re-triage a previous alert.

The command center does not monitor arbitrary pipelines or standalone
notebook jobs. `PIPELINE_SWEEP_ENABLED`, explicit target configuration and a
deployed schedule are all required for unattended pipeline monitoring.
Full-pipeline reruns additionally require reviewed replay safety, approved
parameters, an explicit human decision and durable reservation. Submission
is not recovery; correlated job/activity evidence must confirm it.

The process-local lock does not protect concurrent hosted instances.
Mailbox/retry paths use durable claims and silent sweeps use a durable lease.
The **interactive Playground path remains unclaimed**: it lacks a stable
mail message ID, and the signature is not known until the runner derives it.
SQL open-incident reads suppress already-persisted work, but two callers can
pass that read before either persists.

Avoid overlapping live interactive and scheduled remediation for the same
failure. A complete fix would claim the signature inside the runner across
all entry paths; this documentation does not imply that change has been made.
Queued operator commands have their own durable arbitration and explicit
reconciliation; do not automatically replay interrupted work.

## Hosting and network constraints

The command-center Bicep is private by default, with separate inbound Private
Link and outbound VNet integration, explicit NAT and private DNS for app/SCM.
Read back the actual site property
`outboundVnetRouting.allTraffic=true`; the old inline route-all setting did
not persist as expected. Preserve governance-attached NSGs. SCM and FTP basic
publishing credentials remain disabled.

This protects web ingress; it does not create private backend connectivity to
Foundry or Fabric. Runtime DNS/routes, Foundry managed network isolation,
supported Fabric Private Link scope and service permissions need separate
proof. NAT's public IP provides outbound SNAT only.

A code-only App Service update uses the existing host and an
Entra-authenticated CLI/Kudu upload, not a default full infrastructure
redeployment. Preserve explicitly approved persistent client ingress and keep
SCM independently deny-all outside an authorized deployment window. Require
stable read-only SCM readiness before ZIP upload, reconcile ambiguous accepted
writes, and restore temporary changes in `finally`. The helper's post-upload
health check is not a pre-upload SCM stability gate.

Check SKU quota/admission, model availability and hosted container admission
before deployment. An evaluation exemption such as `CostControl=Ignore` needs
a finite review/removal date; tags do not automatically stop resources.

### Runtime lessons

- Foundry function tools use the flat Responses schema: `name` at the top
  level, not nested under `function`. The latter returned
  `Invalid payload: Required property 'name' is missing`.
- Agent versions are created with `POST /agents/{name}/versions`; `PUT`
  returned 405.
- Some reasoning deployments reject `temperature` with
  `Unsupported parameter: 'temperature' is not supported with this model`.
  Registration can succeed before invocation exposes the problem. The
  registered definitions omit it; deterministic checks enforce the facts.
- Hosted `run()` is synchronous and returns an async iterable for streaming.
  Making it `async def` returned a coroutine and failed with
  `'coroutine' object has no attribute '__anext__'`.
- A hosted agent can receive federated workload identity rather than IMDS
  managed identity. `ManagedIdentityCredential` alone was insufficient.
  Workload clients exclude human credentials so they cannot silently use an
  operator's cached login. Local SQL operator commands deliberately allow
  developer credentials, so their tenant context must be asserted separately.
- Keep `agent-framework-foundry-hosting` pinned exactly. Date-stamped beta
  releases have broken startup without a major-version warning.
- Display tooling can render Authorization headers as asterisks. That is
  display-side redaction, not evidence of a corrupt source header.

Telemetry is optional. The current helper consumes
`APPLICATIONINSIGHTS_CONNECTION_STRING` without explicitly supplying an Entra
exporter credential; the App Service template's Insights resource link is not
telemetry configuration. Do not claim secretless telemetry or private ingestion
from a portal link. Metadata-only spans, runtime logs, durable outcomes and
scheduler history must be checked independently.

## Foundry routine observations

The native routines remain declared but disabled. Historical verification on
2026-09-02, six days after registration, found:

| Check | Observation |
|---|---|
| `azd ai routine show` | Reported an enabled five-minute cron |
| `azd ai routine list` | Returned `{"value": null}` |
| `azd ai routine run list` | No runs |
| Manual dispatch | Returned identifiers without a run or container activity |
| Application Insights | Activity in two of twenty-four hours, both containing manual invocations |

On 2026-09-02 and 2026-09-03, `azd deploy` also failed to apply routine
`enabled` changes or create a newly declared routine. A successful controller
deployment is therefore not evidence that a schedule exists or fires.

Read-only inspection:

```powershell
azd ai routine show bi-triage-schedule
azd ai routine list
azd ai routine run list bi-triage-schedule
```

If revalidating a supported native routine, manage it explicitly with
`azd ai routine create <name> --file <routine-manifest.yaml>` and verify actual
runs. Without `--file`, creation could not set the action's `input`, so a
health routine sent empty input and drained the mailbox instead. Changing
the trigger returned `UserError: Routine trigger cannot be changed after creation`;
a changed cron/timezone required deliberate recreation.

The supported separate scheduler is `infra\scheduled-sweep.json`. It invokes
the same controller with a managed identity, keeps run history, disables
HTTP retries and uses `PT15M` to exceed the default combined
triage/approval/worker allowance of `300 + 300 + 30` seconds. It creates an
enabled workflow, so deploy each job only after its prerequisites are ready.
Transport completion still needs comparison with the durable business outcome.

## Deployment and verification

Follow [DeploymentGuide.md](../DeploymentGuide.md) for existing-environment
updates, object grants, app registration/groups and the Entra cutover.
The relevant offline/operator commands include:

```powershell
.\.venv\Scripts\bi-triage.exe preflight
.\.venv\Scripts\bi-triage.exe pipelines --preflight
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py --dry-run
```

For a separately authorized live release, reassert the intended subscription
and tenant before registering changed definitions or deploying
`bi-triage-controller`. Use the existing azd environment and set both
`AZURE_AI_PROJECT_ENDPOINT` and `FOUNDRY_PROJECT_ENDPOINT`. Deploy App Service
code and schedules separately; never deploy a routine merely to imply that a
timer works.

`preflight --check-sql` proves a connection and `SELECT 1` as the operator, not
the hosted identity's entire grant set. `/api/health` proves the web process,
not SQL or Foundry. Verify runtime-written records through an independent
authorized read, real token-role decisions, and the specific configured
schedule. Keep private identifiers and tokens out of public evidence.
