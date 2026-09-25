# Deployment guide

Provisioning, identity and release requirements for the hybrid monitoring
implementation. Start with [AzureAccountSetUp.md](AzureAccountSetUp.md); the
explicit fixture path remains fully offline.

The command center is an independent **Python 3.13 App Service API and Vite UI**.
The Foundry hosted controller and monitoring worker remain separate deployments.
All application state uses **one shared Azure SQL Database**. The Command Center
is the operational UI. The retained read-only Rayfin cockpit is not a deployment,
state or authorization dependency.

Use [CommandCenter.md](CommandCenter.md) for the web workflow and
[PipelineTriage.md](PipelineTriage.md) for scheduled Fabric pipeline monitoring.
Web approvals and notifications do not require Teams, a mailbox or the legacy
approval callback.

**Release boundary:** the [hybrid plan](HybridMonitoringPlan.md) and current SQL
ownership contract have independent offline review, but live adapter/controller
integration and deployment acceptance remain separate. The procedures below
describe a future approved rollout, not an authorization to deploy while a gate
is blocked.

The selected network baseline is now **public networking with Entra
authentication**. The shipped Bicep and deployment helpers do not require a
private endpoint, VNet, NAT Gateway, subnet or private DNS. Scoped SQL/registry
evaluation access has been enabled and read back. Network access alone is not
schema or runtime acceptance; the completed SQL bootstrap and remaining
activation/cutover gates are distinguished below.

### Release gates

| Gate | Current evidence/status | Not established |
|---|---|---|
| SQL ownership contract | Independent review, eight bounded native `EXECUTE AS` role/effect cases and 19 native SELECT-predicate controls passed. Three correctly mapped EXTERNAL users have kernel roles and reviewed application DML grants | These bounded cases are not the full 27-RPC matrix or normal-worker execution proof |
| Event transport | A historical isolated MI canary received all four observed wire event types across manual failure, scheduled failure, success and cancellation | Azure SQL durable handling, normal-worker recovery or application cutover |
| Controller reconciliation | A bounded priority correction processed the original fresh web-discovery request on attempt 1 without manual requeue despite an existing partial-inventory backlog; one selected workspace completed a ten-page, three-item scan, including one unsupported Eventstream | This does not resolve partial-window replay churn, complete tenant inventory or prove first-scope activation/event readiness |
| Collector-only inventory | A no-ingress worker is deployed in a public Consumption environment with its dedicated MI and explicit `tenant_admin_preview` selection. Fabric domain/workspace reads succeeded and 332 workspaces were durably accepted; authenticated workspace selectors show 333 options including the placeholder | Broader item enumeration is partial. Power BI dataset reads returned 401 without source permissions, and request-budget/throttling gaps remain visible. No scopes or remediation were automatically admitted |
| Data-quality flag persistence | Source selects `AzureSqlFlagTable` for live runs; its table is included in the committed application schema | Actual controller-MI append/readback and runtime persistence acceptance remain outstanding |
| Public SQL/registry access | Approved resource-scoped evaluation exceptions and public access were read back on 2026-09-17. SQL remains Entra-only with TLS 1.2 and the special Azure-services firewall rule; registry access is public with default Allow, bypass None and admin/anonymous access disabled | Network access does not establish SQL schema, runtime permissions, application acceptance or an indefinite governance exemption |
| Earlier private image proof | A historical cold pull succeeded with registry public access Disabled, default Deny and bypass None; the reviewed shipping contents, private DNS and native imports matched | Not proof of the current public-network source bundle, a new application release or runtime recovery |
| Recovery and native schema | Recovery is complete through append-only operator adjudication; the original failed `STARTED` receipt is unchanged. Proof and application schema operations each committed 145 DDL batches and passed 114 readbacks | Not a rewrite of the original receipt, permission to replay committed bundles or full runtime acceptance |
| Bootstrap control and public route | Historical initialization readback captured `maintenance=true`, revision `0` and a bootstrap receipt; a separate no-VNet bootstrap-MI job passed 114 application readbacks over encrypted TCP/FEDERATED authentication | This capture is not current maintenance state or authority to restore old settings/grants |
| Public Foundry path | The retained Foundry account/controller path is public with local authentication disabled; `infra/foundry.bicep` implements that baseline | The earlier managed private-endpoint preflight failure is historical and does not block the selected public architecture |
| Current web/controller cutover | Command Center and the hosted controller use Azure SQL and the reviewed acting-identity grants; maintenance is released (`false`). The API preserves latest-generation partial coverage even with no configured scopes | Event receiver/provisioner operation, full inventory/source access, restart recovery and end-to-end hybrid acceptance remain open |
| Scheduler and alerts | The existing one-minute scheduler runs `heartbeat`. Platform metric alerts remain in place; the optional runtime log-absence alert is enabled with no actions. An isolated query canary fired and resolved, and its rule was then disabled | Not a real controller-stop test, notification delivery proof or permission to retry workload effects |
| Owned connector registration | Public prepare/apply/reconcile tooling is implemented; native SQL SELECT-only preflight passed | No native registration apply or new event-runtime readiness/durable-delivery proof is accepted yet; earlier transport canaries do not establish it |
| Application telemetry | The active controller's app-owned Entra channel has native Application Insights `heartbeat_started` and completed `heartbeat_finished` records with queue counts and zero captured exporter failure/warning counters; project tracing remains disconnected | Bounded ingestion is not proof of every span, future delivery, an alert destination, overnight stability or end-to-end hybrid operation |

Do not undo the completed web/controller cutover or restore maintenance from a
historical bootstrap capture. The collector-only deployment proves bounded
inventory progress, not complete tenant health or event transport. Unattended
hybrid coverage still requires separate acceptance. The evaluation has one Azure SQL application database and
a temporary isolated proof database on the same logical server. This is not a
final sizing or pricing recommendation. The SQL template uses one S1 application
database and an optional Basic proof database, with no elastic pool. Approve
compute, storage, backup and network changes separately.
Fabric capacity remains a workload/Eventstream prerequisite, not a state-store
capacity gate.

### Public networking baseline

| Surface | Shipped template and boundary |
|---|---|
| Application state | [state-sql.bicep](../infra/state-sql.bicep): public SQL endpoint, Entra-only authentication, TLS 1.2 minimum, Proxy/TCP 1433, auditing and TDE |
| Foundry | [foundry.bicep](../infra/foundry.bicep): public account/project/model, local authentication disabled; no managed-network injection, private endpoint or network-approver prerequisite |
| Worker identity and registry | [monitoring-prerequisites.bicep](../infra/monitoring-prerequisites.bicep): dedicated UAMI, public Basic ACR and registry-scoped `AcrPull`; no existing network inputs |
| Worker environment | [monitoring-environment.bicep](../infra/monitoring-environment.bicep): public Consumption environment with keyless Azure Monitor routing, not a VNet/NAT deployment |
| Collector | [monitoring-worker.bicep](../infra/monitoring-worker.bicep): immutable image and one selected MI; outbound worker with no ingress |
| Command Center | [command-center.bicep](../infra/command-center.bicep): public HTTPS, Entra API roles and independent optional app/SCM client filters; no basic publishing authentication |
| Bootstrap and probes | [state-sql-bootstrap.bicep](../infra/state-sql-bootstrap.bicep), [probe-job.bicep](../infra/probe-job.bicep) and [transport-probe-job.bicep](../infra/transport-probe-job.bicep): manual, no-ingress jobs in an existing public environment |

Public reachability is not anonymous service access. SQL still requires an
authorized Entra database principal, ACR disables admin/anonymous access, Foundry
disables local authentication, and the API validates its delegated token and
app-role claims. A worker in a public environment still exposes no HTTP/TCP
listener.

### Governed evaluation exceptions

Ordinary deployments have no baked-in governance exemption.
`sqlNetworkExceptionTags={}` applies only to the SQL logical server.
`registryExceptionTags` is optional/nullable and applies only to the selected
registry. `accountNetworkExceptionTags={}` is an explicit Foundry-account option,
not a requirement for every customer. Do not propagate these tags to a resource
group or unrelated resources.

For the approved MCAPS evaluation, the SQL server uses the resource-specific
`SecurityControl=Ignore` exception with its approved reason/review tags; the
registry exception is scoped separately. This permits one 14-day period.
Removing and re-adding the tag does not restart it. A longer-running test needs
an approved exclusion, not repeated tagging or a policy workaround. This is
tenant-governance behavior, not a SQL feature or a Bicep shutdown timer.
Read back the actual service settings and track the review date. Exceptions do
not disable Entra authorization, auditing, TLS, Defender or database permissions.

The verified evaluation readback does not make the exception permanent.
Keep network exceptions separate from cost-control tags and from any Fabric
workspace/tenant approval; Azure tags cannot override Fabric network policy.

### Native proof and bootstrap status

**Recovery is complete and both SQL schemas are committed.** Append-only
operator adjudication binds the proved empty rollback to one approved
replacement; the original failed `STARTED` receipt remains unchanged.
The proof replacement and the separately approved application operation each
committed 145 DDL batches and passed 114 readbacks.

Those counts describe the earlier native artifact. The current preparer adds
the complete 19-policy static budget seed set and one fixed policy readback,
producing 164 batches and 115 checks at this source revision. That newer
candidate has local validation, not a new managed-build/image/native SQL proof.
Missing policies in an existing deployment require a separately reviewed
one-off policy installation, not bootstrap replay or a change to original receipts.

At initialization, independent application-control readback captured
`maintenance=true`, revision `0` and the persisted bootstrap receipt. This is
historical evidence: maintenance has since been released and is now `false`.
Eight bounded native role/effect cases using `EXECUTE AS`, including cleanup,
passed; the later 19 native SELECT-predicate controls are also bounded. Neither
set establishes the full 27-RPC acceptance matrix or the normal worker path.

A separate public-route job in a no-VNet Container Apps environment connected
over encrypted TCP with FEDERATED bootstrap-MI authentication and passed all
114 application readbacks without changing maintenance at that capture time.
Proof jobs were quiesced and human SQL administration restored afterward.

Command Center has since cut over to live monitoring and Azure SQL. Controller
operation uses Azure SQL settings and the correct acting identity,
not a substituted Foundry account MI. Three correctly mapped EXTERNAL SQL users
have their kernel roles and reviewed application DML grants.

After reviewed SQL procedure corrections and narrowly bound handoff-metadata
repairs, the deployed controller heartbeat returned `response.completed`.
Independent SQL readback confirmed the original discovery-intent work completed,
its validation frontier was published `1/1`, one inventory-worker item was queued
and no actions were taken. The original receipt and control were unchanged.
The SQL-only corrections did not require another controller deployment.

The subsequent collector-only worker has durably accepted 332 workspace records
from native Fabric metadata reads. The authenticated Include and Inventory
workspace selectors are enabled with 333 options each, including the placeholder,
without selector error alerts. This is not an all-tenant health verdict:
item enumeration remains partial, Power BI source reads can return 401 and
budget/throttling gaps remain visible. The 19 static REST budget policies were
installed through a separate deployment-only repair and verified natively,
without resetting counters or replaying bootstrap. The new preparer/image still
needs its own proof; the earlier 145/114 capture is not its acceptance record.

Collector-only mode has no Eventstream receiver or provisioner. Event intake,
full source access, restart/sustained coverage and end-to-end acceptance remain
open. Bounded native application-heartbeat telemetry is now proved; its reserved
environment-variable fix, privacy boundary and limits are described under
[Observability](#8-observability).

The workstation's proxy path produced changing public egress addresses across
SQL attempts. Use the approved Azure-hosted execution path for repeatable native
proof rather than chasing those addresses, widening firewall ranges or changing
the network posture. A workstation connection failure does not establish a
platform limitation.

The following records the **earlier private-network proof**, not proof of the
current public application release. Its image matched the reviewed
251 shipping files, directory layout, source and bundle; private DNS and native
Python/SQL-driver imports were verified. This is image and bootstrap-host evidence,
not an application release.

The first full-schema apply failed before journal creation because driver row
objects did not satisfy the shared query tuple contract. That contract and its
regressions were corrected, and the corrective image was proved. The corrected
apply executed 145 DDL batches and transaction checks, then failed the expected
native view-module hash check. At that checkpoint, DDL rollback was proved:
only the bootstrap journal with the original `STARTED` receipt remained in the
isolated proof database. The receipt was retained rather than deleted, cleared
or used to justify a blind retry.

The corrected metadata helper preserves module bodies exactly and models only SQL's
`CREATE OR ALTER` header stripping. Native checks of the exact view and
procedure/function controls matched the corrected hashes. All six diagnostic
objects were rolled back; the original receipt and full catalogue remained
unchanged, and bootstrap authority was restored to the authorized human operator.
SQL/readback fences were not loosened. This proves metadata formatting only.
Those metadata checks alone did not resolve the original receipt or commit a
schema. The later append-only recovery and separate replacement/application
applies established the committed state above; they did not erase the original
evidence or authorize automatic replay.

The prepared SQL surfaces are [state infrastructure](../infra/state-sql.bicep),
[identity/manual bootstrap job](../infra/state-sql-bootstrap.bicep) and the
[bootstrap operator](../scripts/bootstrap_azure_sql.py). The operator defaults
to read-only preflight. `reconcile` reads schema receipts; `reconcile-recovery`
reads recovery acknowledgements, including immutable historical evidence.
See the [operator interface](#sql-bootstrap-operator-interface) for the five
modes, explicit artifact/bundle selection and separately approved mutation.

## 0. Establish the operator context

All names and identifiers below are placeholders or synthetic examples. Keep
filled-in settings, deployment evidence and tokens out of the repository.

```powershell
$subscription = "<subscription-name-or-id>"
$tenantId = "<tenant-id>"
az account set --subscription $subscription
az account show --subscription $subscription --query "{subscription:id,tenant:tenantId}" -o json

.\.venv\Scripts\bi-triage.exe preflight
```

Confirm the reported tenant before continuing. Reassert the subscription before
each live sequence and after reauthentication: other shells can change shared
CLI state. Fabric has no subscription argument; the token selects the tenant.
An unfamiliar workspace list can be a valid response from the wrong tenant.
An operator credential must be explicitly selected; the live SQL CLI does not
silently fall back to the current developer login.

If the CLI token cache is unavailable, an explicitly tenant-pinned operator
token, for example from `azureauth`, can authorize appropriate REST calls.
That does not authenticate unrelated registration/deployment scripts. Use an
approved operator flow, never a copied token file or a new client secret as a
shortcut. `bi-triage` accepts the global flags `--sql-identity broker` with
`--operator-domain`, or `--sql-identity managed` with an explicit
`AZURE_CLIENT_ID`. Put these flags before the subcommand.

Plain `preflight` checks configuration without network access. A `configured`
SQL result is not proof of connectivity; use `preflight --check-sql` explicitly
when a live connection is intended.

## 1. Power BI workload identity

The identity that executes a refresh must be accepted by Power BI. In the
hosted path this is the controller's identity, not a prompt agent or the browser
user. For Azure-hosted adapters, prefer managed identity or supported workload
federation. Do not provision a client secret for this deployment.

A managed identity can be added to a Fabric/Power BI workspace as a principal.
Assign the required role explicitly; do not assume a workspace identity has
an automatic Contributor grant. The local live clients deliberately exclude
human CLI credentials from their workload credential chains, so `az login`
alone does not turn a laptop into the hosted controller.

### Tenant setting and workspace access

Have a tenant administrator enable the applicable service-principal API
setting, scoped to a security group containing the acting principal. Power BI
documentation calls this **Allow service principals to use Power BI APIs**;
Fabric also exposes **Service principals can call Fabric public APIs**.

Assign the narrowest supported workload access and verify the intended
operation. Reading Power BI refresh history itself requires semantic-model
Write permission; GET-only code is not proof of a read-only principal.
Contributor carries Build for the semantic-model probes described in section
7b. The web UAMI does not need workload action grants merely to queue an
investigation. Discovery, collection, event consumption and controller actions
have separate permission requirements.

Verify target IDs, a read of refresh history and an explicitly approved test
operation as the actual runtime identity. A successful delegated operator read
does not prove the controller can act. Do not diagnose solely from the HTTP
status: tenant restrictions, identity type, workspace access and model
configuration can all produce 401/403, and some missing permissions appear as
404. See
[Power BI service-principal access](https://learn.microsoft.com/power-bi/developer/embedded/embed-service-principal).

## 2. Optional mailbox ingestion

Skip this section for the Teams-independent command-center path. Do not enable
a mailbox schedule until its identity, mailbox scope and filter have been
verified.

An unattended mailbox reader needs app-only access, not a signed-in user's
delegated token. The older hosted implementation observed the same Entra agent
token returning 200 for Graph directory access and 401 for the Exchange-backed
mail endpoint, despite a `Mail.Read` role and a scoped mailbox policy. That is
an observed compatibility limit, not proof that every Graph API accepts every
agent identity.

The source still exposes `GRAPH_CLIENT_SECRET` and a conventional
client-secret fallback in `azure.yaml` and the inbox client. **That legacy
path is not part of the secretless deployment.** Do not fill it in to make
preflight green. Use a separately verified, supported secretless mail identity
or leave mailbox ingestion off. Expiring secrets, including tenant automation
that removes them, would otherwise stop an unattended trigger after deployment.

### Scope the reader to the alerts mailbox

An unscoped Entra application `Mail.Read` grant can read every mailbox.
Successful access to one mailbox does not prove isolation.

For an existing Application Access Policy deployment, the Exchange operator's
verification commands are:

```powershell
New-ApplicationAccessPolicy -AppId <mail-reader-client-id> `
  -PolicyScopeGroupId <mail-enabled-security-group> `
  -AccessRight RestrictAccess `
  -Description "BI triage alerts mailbox only"

Test-ApplicationAccessPolicy -Identity bi-alerts@contoso.com `
  -AppId <mail-reader-client-id>
```

The policy scope is a mail-enabled security group containing the permitted
mailboxes, not an arbitrary mailbox address. This Exchange scope group is
separate from the command center's four ordinary app-role groups.

For new integrations, review
[Exchange RBAC for Applications](https://learn.microsoft.com/exchange/permissions-exo/application-rbac),
which replaces Application Access Policies. Exchange RBAC and existing
unscoped Entra grants are independent/additive; a narrow RBAC assignment does
not cancel a broad grant. Revalidate the application's actual mailbox requests
when adopting a different permission model rather than assuming the current
scope checker covers it.

Set `GRAPH_CANARY_MAILBOX` to a distinct existing mailbox that the reader must
not access. The application requires the canary read to return **403**.
Missing configuration, a failed check, 401/404, or a successful canary read
does not prove confinement and is refused. The hosted path also refuses a
token carrying `upn`; a delegated operator token is not unattended proof.

The Outlook/Office 365 connector catalog is not used by this code. Catalog
availability and per-user OAuth consent are not substitutes for the verified
app-only mailbox path.

## 3. Azure SQL Database

Incidents, processed messages, approvals, deferred retries, claims, leases,
baselines, run history, commands and incident collaboration must survive
invocations. A restarted controller that forgets an open incident can remediate
the same failure twice.

Provision **one shared Azure SQL Database** on an Azure SQL logical server.
The web, worker, controller and all ancillary stores use the same catalog so
receipts, incident/source disposition and work completion can share a
transaction. Do not create per-component databases or put any application state
in a Rayfin data service. A temporary proof database is isolated test state only.

Read the server hostname and database name from the Azure deployment, not the
Fabric portal or Fabric database REST endpoints:

```powershell
az account set --subscription $subscription
az sql server show --resource-group "<resource-group>" --name "<logical-server>" `
  --query fullyQualifiedDomainName -o tsv
az sql db show --resource-group "<resource-group>" --server "<logical-server>" `
  --name "<database-name>" --query name -o tsv
```

These are readback commands after provisioning, not database creation. Configure:

```text
AZURE_SQL_SERVER=<server>.database.windows.net
AZURE_SQL_DATABASE=<database-name>
```

The command-center deployment helper requires a hostname without protocol or
port. Copy the Azure database name exactly; it is not a Fabric item name or ID.
These settings are not a username/password connection string. Where deployment
templates accept SQL parameters, use `azureSqlServer` and `azureSqlDatabase`.

Configure **Microsoft Entra-only authentication** on the logical server and
verify it before starting runtimes. Azure SQL also supports SQL authentication;
the deployment must disable that option rather than assume the platform is
token-only. Do not configure a SQL login, password or credential-bearing
connection string. The server's configured Entra administrator is a deployment
principal, not a Command Center Admin app role.

`infra\state-sql.bicep` enables `publicNetworkAccess`, requires TLS 1.2 or later
and selects Proxy connectivity on TCP 1433. Enable SQL auditing and TDE at
provision time. Use the normal `<server>.database.windows.net` hostname with
certificate validation; no private endpoint, VNet or private DNS is required.
Preserve governance diagnostics and Defender settings.

`allowAzureServices` defaults to `true`. It creates Azure SQL's special firewall
rule with **both start and end set to `0.0.0.0`**. That permits Azure-hosted
callers, including callers in other subscriptions; it is not an Internet-wide
`0.0.0.0/0` rule. It is a broad network allowance, not a tenant or identity
allowlist. Every connection still requires an authorized Entra SQL principal.
If this allowance is unsuitable, set it to false and supply reviewed
`clientFirewallRules` with exact IPv4 ranges for the required callers.
Additional client ranges are optional; they do not replace database permissions.
Verify public DNS, TCP/TLS, firewall admission and the actual runtime identity
separately. See
[SQL firewall rules](https://learn.microsoft.com/azure/azure-sql/database/firewall-configure),
[connectivity architecture](https://learn.microsoft.com/azure/azure-sql/database/connectivity-architecture),
[Entra-only authentication](https://learn.microsoft.com/azure/azure-sql/database/authentication-azure-ad-only-authentication),
and [SQL auditing](https://learn.microsoft.com/azure/azure-sql/database/auditing-overview).
Use [governed evaluation exceptions](#governed-evaluation-exceptions) only when
the target environment requires an approved resource-scoped exception.

Select `MONITORING_MODE=fixture` for explicit offline fixtures and
`MONITORING_MODE=live` with `MONITORING_TENANT_ID` for shared monitoring.
Missing SQL settings in live mode are an error, not a request for JSON state
under `runs/`. The live API, collector and controller fail closed when their
shared state is unavailable; a recycling container must not create another
remediation opportunity by forgetting its history.
There is no Fabric SQL compatibility adapter, legacy setting alias, state import
or dual-write mode. The approved prototype clean start creates a new epoch and
imports neither old operational history nor old target configuration.

### Schema and ownership

The current schema has these default tables:

| Purpose | Tables |
|---|---|
| Incident and mailbox state | `triage_incidents`, `triage_processed_messages`, `triage_inbox_audit` |
| Approval and retry state | `triage_approvals`, `triage_deferred_retries`, `triage_pipeline_reruns` |
| Detector and concurrency state | `triage_semantic_health`, `triage_sweep_leases`, `triage_claims` |
| Data-quality flags | `triage_data_quality_flags` |
| Command-center history and queue | `triage_agent_runs`, `triage_agent_events`, `triage_agent_commands` |
| Append-only collaboration | `triage_incident_activity` |
| Monitoring deployment control | `triage_monitoring_control` |
| Registry, inventory, work, source/action state | `triage_monitoring_records` |
| Monitoring ownership and operation receipts | `triage_monitoring_leases`, `triage_monitoring_receipts` |
| Shared service/API request budgets | `triage_monitoring_rate_budget` |
| Protected deployer registration | `triage_deployment_registration`, `triage_deployment_writers` |

Use the corresponding supported `*_TABLE_NAME` settings consistently across
components when sharing a database. The prototype reset tool supports its exact
declared catalogue, not arbitrary table mappings. Registration names are selected
separately and must never be added to the kernel's physical-table map.
`schema_statements()` in
`src\triage\store\azure_sql.py` includes the command/history and incident
activity schema and the legacy approval procedure. It contains no application
membership permission table.

Schema creation is deployment-only. Runtime stores never install, reset or
upgrade schema. `src\triage\monitoring\schema.py` creates only its declared
monitoring tables/indexes and deployment control; it does not recreate the
application tables or approval procedure. The rate-budget module supplies its
own deployment-only DDL. Apply the application schema separately, then the
monitoring baseline. Missing/incompatible bootstrap is a visible error.
Do not grant a runtime identity `db_ddladmin` to hide a missed deployment step.

The current SQL ownership contract also declares checked views, functions,
static RPCs and three component roles through
`triage.monitoring.sql_permissions.object_catalogue()` and
`schema_statements()`. Install the exact reviewed batches with the deployer only,
after the physical baseline; do not reimplement that catalogue as a copied
table/grant list. Operator registration adds a separate protected authority view
and append-only registration/capture evidence.

Preserve the database independently of the web applications. Ordinary
authorization changes and code releases do not reset operational data. The
explicitly approved prototype clean start in section 3a is a separate,
manifest-confirmed deployment operation, not a migration or startup fallback.

### Azure SQL identity and database users

The principal needs an Azure SQL contained database user and the SQL permissions
appropriate to its component. Azure resource-management roles do not grant
database DML, and Fabric item/workspace permissions do not grant Azure SQL
access. Use the configured server Entra administrator or a separately authorized
SQL deployment principal for schema and user/role setup. Do not make a runtime
identity the server administrator or infer SQL access from a human app role.

Give the deployment operator the reviewed DDL permissions and runtime identities
only their component's checked-view/static-RPC permissions plus the reviewed
object/column grants required by their ancillary application stores.
`runtime_table_permissions()` is retired and raises instead of emitting broad
monitoring-table DML. Use the exact `runtime_grants(component)` contract and the
role returned by the current kernel generator. The runtime has no DDL,
directory-administration or user-membership management responsibility.

`CREATE USER ... FROM EXTERNAL PROVIDER` resolves the identity through Entra;
directory permissions or Conditional Access can prevent that lookup. For an
operator-verified identity, the explicit SID form avoids the lookup:

```powershell
$sid = '0x' + (([guid]::Parse('<runtime-client-id>').ToByteArray() |
  ForEach-Object { $_.ToString('X2') }) -join '')
```

```sql
CREATE USER [<runtime-sql-user>] WITH SID = 0x<verified-sid-bytes>, TYPE = E;
```

For an Entra service principal or UAMI, use its **client ID**, converted to
little-endian GUID bytes, not its directory object ID. Azure RBAC assignments
instead use the **principal object ID**. For a user/group, SQL uses the object
ID; groups use `TYPE = X`. The SID form does not validate the name, so verify the
identity before creation, then prove its sign-in and the stored principal SID
on the native Azure SQL target. Correct runtime mappings and grants are now
installed, and web/controller SQL paths have been exercised with the intended
identities. Normal-worker execution and broader permission/effect coverage
remain separate proof obligations. Do not generalize an observed equality of agent object/client IDs
to ordinary service principals. See
[CREATE USER](https://learn.microsoft.com/sql/t-sql/statements/create-user-transact-sql#arguments).

### Component SQL permissions

| Component | Permitted responsibility | Not permitted |
|---|---|---|
| `worker` | Raw inventory/source observations, retention, intake/checkpoints and owned connector observations | Publish admission/source heads, new readiness, action reservations or human intent |
| `web` | Human scope/review/discovery intent, drafts and authenticated decisions | Promote its own intent into controller authority or dispatch remediation |
| `controller` | Deterministic publication, fenced source reads, action/budget/approval/finalization state | Bypass current guards with generic raw-table writes |
| Deployer | Reviewed installation, component-role assignment, maintenance/bootstrap/reset and protected registration | Pass DDL/control privileges to runtime identities |

Live stores require an explicit `component="worker"`, `"web"` or `"controller"`.
That argument selects routing, not permission: the authenticated SQL principal
must have its corresponding reviewed role. Use
`build_permission_kernel(...).names.role(component)` and
`runtime_grants(component, ...)`; do not guess generated role/procedure names.

Keep run/history, commands and incident collaboration in the release's complete
live-store review as well. A missing adapter or unbound ancillary operation is a
rollout blocker, not a reason to restore the old raw-table grant recipe. Human
closure appends activity and does not UPDATE the core incident. Check inherited
schema/database roles and callable module/trigger paths as well as
explicit grants; `db_datawriter`, `db_owner` and runtime DDL defeat the boundary.

A generic monitoring role alone is not a complete runtime grant profile.
Ancillary store calls need their reviewed object/column permissions too; for
example, web command expiry updates `finished_at` as well as state/summary.
Verify the current call contract rather than copying an older grant list.

Static RPC calls use their exact named contracts:

```python
from triage.monitoring.sql_permissions import decode_rpc_result, rpc_contracts

contract = rpc_contracts()["worker.claim_work"]
sql, values = contract.bind(arguments)
with database.transaction():
    reply = decode_rpc_result(contract, database.query(sql, *values))
```

The example owns the transaction; an adapter already inside that transaction
must join it rather than nest another one. Supply every required argument and
explicit nullable value. `status`, `affected_rows` and the typed `result` are the
RPC outcome, never `execute(...).rowcount`. Preserve the original receipt/result
after an uncertain commit rather than reconstructing success.

Native acceptance must use separate connections authenticated as the actual
Entra MIs on Azure SQL. Azure SQL supports `CREATE USER ... WITHOUT LOGIN` and
`EXECUTE AS USER` for database-scoped tests; these do not prove MI sign-in,
network/firewall admission or reconnect behavior. Runtime identities still receive no
impersonation permissions. CREATE/binding success, offline fixtures and a T-SQL
parser do not prove the component roles, updatable views or procedure
transactions. Earlier Fabric SQL CREATE/rollback evidence remains historical.

### Local SQL candidate preparation

[`scripts\prepare_azure_sql.py`](../scripts/prepare_azure_sql.py) exports a
current-source bootstrap candidate entirely locally. It uses explicit request
values, not environment-derived target or identity defaults. It acquires no
credentials and performs no network, SQL, subprocess/cloud-tooling, image-build
or deployment operation.

Create an operator-owned UTF-8 JSON request outside committed source. The
following template shows the complete request shape; replace every placeholder
with the reviewed value before use:

```json
{
  "version": 1,
  "operation_id": "<reviewed-operation-uuid>",
  "target": {
    "server": "<server>.database.windows.net",
    "application_database": "<application-database>",
    "database": "<application-database>-proof",
    "kind": "proof"
  },
  "identity": {
    "tenant_id": "<tenant-uuid>",
    "client_id": "<bootstrap-uami-client-uuid>",
    "object_id": "<bootstrap-uami-object-uuid>"
  },
  "base_image": "<reviewed-registry>/<sdk-native-image>@sha256:<64-lowercase-hex-digest>"
}
```

The request is strict: version `1`, nonzero UUID operation/identity fields,
the bootstrap runner's exact target model and a digest-pinned SDK/native-library
base image. Extra fields and duplicate JSON keys are refused. The logical-server
hostname must be lowercase. The database must equal `application_database` for
`kind="application"` or that name plus `-proof` for `kind="proof"`; system
catalogues are refused.

The identity is the dedicated bootstrap UAMI, not the controller's acting
identity or a runtime-grant request. Omit the sole optional target field,
`private_ip`, for the public baseline; an explicit value must be a reviewed
RFC1918 IPv4 address with exact DNS matching. No credentials, connection strings,
runtime-user/membership requests, recovery approvals, policy overrides or
arbitrary SQL/readback inputs are accepted. The base image must be reviewed
separately; preparation does not resolve, pull, build or approve it.

From the repository root:

```powershell
.\.venv\Scripts\python.exe scripts\prepare_azure_sql.py `
  --request "<operator-owned-directory>\candidate.json" `
  --output .azure\sql-candidate

.\.venv\Scripts\python.exe scripts\prepare_azure_sql.py --verify .azure\sql-candidate
```

Exactly one of `--request` or `--verify` is required. `--request` requires
`--output`; `--verify` takes no `--output`. The output directory must not exist,
even as an empty directory, and cannot be under the repository's `src` tree.
Source drift detected during preparation refuses the candidate before writing.
Existing evidence is never overwritten. The result is:

```text
sql-candidate\
  manifest.json
  context\
    src\                         current Python source bytes
    scripts\bootstrap_azure_sql.py
    sql\NNN.sql                  ordered generated batches
    bundle.json
    Dockerfile
```

Each request exports one bound bundle. Prepare proof and application candidates
in separate new directories with their respective reviewed requests; do not
rename a proof bundle or edit its target to make it an application artifact.

The exporter preserves every current `src` Python file and the bootstrap runner
as exact bytes, without newline normalization. It emits one driver batch per
numbered SQL file, without `GO`. The strict bundle records version, operation,
`ddl_owner="dbo"`, target, identity, the canonical relative POSIX source-path
hash map, ordered batch paths/hashes and fixed checks. `source_sha256` is the
runner's canonical source-map digest, not a value inferred from a Git revision.

The ordered application/monitoring/rate-budget/kernel SQL, static budget seeds,
metadata expectations and ABI come from current helpers. The current complete
output is **164 batches and 115 fixed checks**; these are generated counts, not
an ABI assumption. `kernel.statements` already includes component grants;
do not append `kernel.grants` a second time.
Previously native-verified primitive tuples inform the metadata model, including
`datetime2` scales 3, 6 and 7. New types/scales/view shapes refuse rather than
guessing precision or copying `ColumnSpec` storage widths. Module expectations
use the existing `native_module_hash` helper.

The strict manifest has `status="candidate_not_authorized"`. It records the
request, `bundle_sha256`, `source_sha256`, `kernel_contract_hash`,
`preparation_sources`, the exact `files` map with sizes/hashes, `context_sha256`,
`batch_count` and `metadata_check_count`. `preparation_sources` records the
prepare/reset script hashes. Context-file keys are canonical relative POSIX
paths; `context_sha256` hashes canonical JSON of all file size/hash records,
including the Dockerfile. The manifest is outside the context and is not
self-hashed. A payload-only hash or image tag is not an exact build-context binding.

The generated Dockerfile uses the reviewed digest-pinned base, explicitly copies
the source/runner/SQL/bundle into `/opt/state-sql-bootstrap`, makes the payload
read-only and selects user `65532:65532`. Its entry point is the current runner
through `python3 -I -B`. These Linux image paths are intentional; they do not
change the Windows operator command paths or prove the image has been built.

Preparation and `--verify` use the existing `load_artifact` validation offline.
Verification checks the exact file map, context hash, generated Dockerfile and
bundle/manifest bindings; missing, changed, extra or symlinked payload files
and paths escaping the context are refused. It verifies the local artifact and
readback model, **not native SQL readbacks**. Successful output still says `native_sql_proven=false` and
`image_built=false`.

Verification also requires matching current trusted preparation-source hashes
and regenerates the expected source map, ordered SQL, checks and kernel ABI
from that checkout. A self-consistent edited manifest is not enough to establish
provenance. Payload Python stays inert data; verification never imports or
executes it. Current-source drift refuses the candidate. Preserve historical
evidence unchanged and use the separate SELECT-only historical recovery reader,
not `--verify` or rehashing, when its source version differs.

Treat the candidate as immutable. Review the target, bootstrap identity, ordered
SQL, metadata expectations and all file hashes before a separately authorized
build using the exact generated `context` and its Dockerfile. Image-digest
approval and current native SQL/image/readback/role proof remain separate.
The exporter does not initialize monitoring control, create runtime users or
memberships, assign administrator authority, register reset writers, authorize
an apply/recovery or generate `empty_baseline_sha256`.

Do not edit a candidate manifest to bless changed payload bytes, reuse a
committed operation ID with a different bundle, or mint a new ID to evade an
uncertain write. Use the [bootstrap operator interface](#sql-bootstrap-operator-interface)
and the [empty rollback baseline process](#empty-rollback-baseline-evidence)
for those separate boundaries.

#### Static REST budget seeds and readback

`prepare_azure_sql.static_budget_policies()` derives the complete current
`SERVICE_POLICIES` (2), `API_POLICIES` (12) and `PROVISIONING_POLICIES` (5).
Buckets use `service:<name>` or `api:<name>`; overlapping sources are refused.
`budget_policy_statements()` adds 19 deployment-only seed batches for
`request.identity.tenant_id`. Bucket identities are SHA-256 of their UTF-8
names; the supported names are ASCII.

Seeds insert missing policies only. A matching existing policy retains its
`used`, `window_ends_at` and `blocked_until` state. A changed `request_limit`
or `window_seconds` throws instead of resetting it. This adds no runtime
upsert, extra grant pass or relaxed RPC guard.

The runner's `budget_policies` check is a fixed, tenant-bound
`SELECT TOP (2049)` over `bucket_hash`, `request_limit` and `window_seconds`
from `dbo.triage_monitoring_rate_budget`, ordered by the bucket hash with BIN2
collation. The bundle binds its argument to `identity.tenant_id` and requires
nonempty, sorted, unique lowercase SHA-256 rows with bounded integer limits
and windows. It accepts no caller-supplied SQL.

The readback deliberately excludes `used`, `window_ends_at` and `blocked_until`.
`apply` and schema `reconcile` refuse missing, changed or extra policies; neither
requires clearing live counters, windows or cooldowns to match the bundle.
Do not replay bootstrap to install these seeds into a running deployment.
Such repair needs a separately reviewed one-off policy install that preserves
live state and original operation evidence. The new local candidate and its
offline tests do not prove that repair, a managed image build or native acceptance.

### SQL bootstrap operator interface

`infra\state-sql-bootstrap.bicep` and `scripts\bootstrap_azure_sql.py` expose
`preflight` (the read-only default), `apply`, `recover`, `reconcile` and
`reconcile-recovery`.
Keep the saved job read-only and job-start/override permission with the
authorized deployment operator. Use the dedicated bootstrap UAMI and its
explicit authority window, not a controller `ServiceIdentity` or a runtime
app-role claim. `deployJob=false` is identity-only preparation; enabling the job
does not change its default `mode=preflight` or its zero automatic retries.

| Mode | Effect and approval |
|---|---|
| `preflight` | Read-only validation; no application schema batches |
| `recover` | Fresh, doubly approved append-only adjudication for an exact original `STARTED` receipt and proved empty rollback/security catalogue; zero application schema batches |
| `apply` | Separately approved application of the selected bundle's schema batches and readbacks |
| `reconcile` | Read-only schema-receipt lookup under its normal bundle/source checks; it does not inspect recovery-record acknowledgements |
| `reconcile-recovery` | Fixed SELECT-only lookup using the original recovery file and hash; no mutation approvals. `MATCHING` exits `0`; `MISSING` or `CONFLICT` exits `2` |

The image may contain separate proof and application bundles. Select the exact
file, matching `targetKind`, bundle SHA-256 and bundle operation ID; do not
rename, copy over or relabel the proof bundle as an application bundle.

| Bicep parameter | Default and runner mapping |
|---|---|
| `bundlePath` | `/opt/state-sql-bootstrap/bundle.json` -> `--bundle`; the accepted multi-bundle layout keeps the application at `/opt/state-sql-bootstrap/application-bundle.json` |
| `recoveryPath` | Empty -> `--recovery`, emitted for `recover` and `reconcile-recovery` |
| `recoverySha256` | Empty -> `--recovery-sha256`, emitted for `recover` and `reconcile-recovery` |
| `approvedRecoverySha256` | Empty -> `--approve-recovery-sha256`, emitted only when `mode=recover` |
| `artifactRoot` | Empty -> `--artifact-root`, emitted only for `reconcile-recovery` when nonempty; original archived artifacts are verified as data |

For both `apply` and `recover`, `approvedFingerprint` must equal the selected,
reviewed `bundleSha256`; in the recovery flow this is the replacement bundle.
The runner flag is `--approve-fingerprint`.
For `recover`, `approvedRecoverySha256` must also exactly match
`recoverySha256`. `operationId` is the replacement bundle's operation ID, not
the original failed operation's ID; the hash-reviewed recovery request binds
the original receipt separately. Read-only recovery acknowledgement uses the
recorded replacement bundle/operation bound by that original request.
For `reconcile-recovery`, leave approval parameters empty and omit approval
flags in direct CLI calls. Pass only the original recovery path/hash and, when
needed, the historical artifact root. There is no environment-variable change.

The approved UTF-8 recovery JSON must be operator-staged **inside the execution
before the runner starts**. The runner validates exact file bytes; neither the
runner nor the template stages or decodes the request from an environment
variable. Fresh `recover` requests require timezone-aware `observed_at` and
`expires_at` with a positive, current window of at most 15 minutes.
The required `empty_baseline_sha256` binds the full native empty/security
catalogue before any write. Hash approval does not waive the allowed-baseline
checks or permit unexpected objects, roles, triggers or permissions.
Native other-session, schema and principal checks remain mandatory.

#### Empty rollback baseline evidence

Use the current trusted
`bootstrap.recovery_baseline_fingerprint(db, artifact.bundle)` reader to collect
the expected baseline for an independently reviewed empty target with the same
original receipt catalogue. It executes the bounded, SELECT-only
`EMPTY_BASELINE_SQL` contract with complete standard-security validation.
`MAX_BASELINE_ROWS=20,000` is one shared total row budget across all categories;
each query consumes the remaining budget. It is not a per-category allowance.
An empty `sys.objects` result alone is not an empty or safe recovery baseline.

Preserve the target and original-receipt identity, trusted source hash, collected
catalogue/security evidence and returned digest in a separate operator evidence
record. Review that evidence independently of the failed operation's claims.
The digest binds the server, database and physical catalogue: it is not a
generic empty-database hash and cannot be guessed or copied from an unrelated
target. Never accept the failed target's current metadata as its own expected
baseline without the independent empty-baseline review.

Put the separately approved digest into `recovery.empty_baseline_sha256`.
Fresh `recover` compares the full native baseline inside its transaction
**before any recovery DDL or write**, alongside its original-receipt,
identity/session and current evidence-window checks. Local
`scripts\prepare_azure_sql.py` export deliberately does not generate or authorize
this digest. Artifact preparation, native evidence collection and mutation
approval are separate steps.

The same explicitly authorized operator may review, prepare and execute.
Independent baseline evidence does not require a second person, signer,
certificate or new key. A mismatch, unexpected authority or incomplete
catalogue is a refusal, not a request to approve a different hash.

#### Recovery execution and acknowledgement lookup

Use a reviewed one-shot execution override, not a stale approval baked into an
image. Mutations still require the exact current runner/source and approvals.
Historical requests may be expired when read with `reconcile-recovery`; keep
their original bytes, timestamps and hash rather than refreshing or rewriting
them for the new reader.

The commands below describe separately authorized invocations inside the approved
Linux execution, not workstation PowerShell or commands to replay the completed
proof. Set the common values for each invocation from its reviewed bundle:

```bash
runner=/opt/state-sql-bootstrap/scripts/bootstrap_azure_sql.py
bundle=/opt/state-sql-bootstrap/bundle.json
bundle_sha="<reviewed-bundle-sha256>"
operation_id="<bundle-operation-id>"
args=(--bundle "$bundle" --bundle-sha256 "$bundle_sha" --operation-id "$operation_id")
python3 -I -B "$runner" "${args[@]}"
```

For an approved failed operation with a fresh staged recovery request, recovery
records adjudication only:

```bash
python3 -I -B "$runner" "${args[@]}" --mode recover \
  --approve-fingerprint "<approved-replacement-bundle-sha256>" \
  --recovery "<absolute-recovery-json-path-in-execution>" \
  --recovery-sha256 "<reviewed-recovery-sha256>" \
  --approve-recovery-sha256 "<approved-recovery-sha256>"
```

Only after adjudication is durable, a **separately approved** apply uses the same
replacement bundle. A fresh application operation uses its distinct application
bundle path, matching target environment, SHA-256 and operation ID:

```bash
python3 -I -B "$runner" "${args[@]}" --mode apply \
  --approve-fingerprint "<approved-bundle-sha256>"
```

Already committed operations require no further recovery/apply. For a schema
receipt, `reconcile` uses its normal bundle/source checks; it is not a
recovery-acknowledgement query and does not accept `--artifact-root`:

```bash
python3 -I -B "$runner" "${args[@]}" --mode reconcile
```

For a recovery acknowledgement with matching current-source artifacts, use the
original request and its hash, with no approval arguments:

```bash
python3 -I -B "$runner" "${args[@]}" --mode reconcile-recovery \
  --recovery "<operator-staged-original-recovery-json>" \
  --recovery-sha256 "<original-recovery-sha256>"
```

`MATCHING` confirms the exact recorded recovery binding, not current application
authority or end-to-end readiness. `MISSING` or `CONFLICT` does not authorize a
restart, a new request ID or another mutation. Keep the original evidence.

If the original bundle belongs to an older runner, stage its immutable artifact
directory and original recovery request outside the new image. Point
`bundlePath` inside that archive and use `artifactRoot` only with this SELECT-only
mode. Always run the **current trusted reader and adapter**, never an archived
script. They verify the original bundle, source and SQL bytes as data and execute
fixed SELECTs; they do not import or execute archived code/SQL. Do not rewrite
or assign new hashes to the original bundle to make it match a newer runner.

```bash
archive_root="<operator-staged-original-artifact-directory>"
python3 -I -B "$runner" --mode reconcile-recovery \
  --artifact-root "$archive_root" \
  --bundle "$archive_root/<recorded-bundle-file>" \
  --bundle-sha256 "<recorded-replacement-bundle-sha256>" \
  --operation-id "<recorded-replacement-operation-id>" \
  --recovery "<operator-staged-original-recovery-json>" \
  --recovery-sha256 "<original-recovery-sha256>"
```

All other CLI modes reject `--artifact-root`; leave the Bicep parameter empty
for them. This is read-only historical evidence compatibility, not application
state migration, schema downgrade or permission to mutate with archived code.

For a normal public target, omit `target.private_ip` or leave it `null`.
An explicit pin must be a reviewed RFC1918 address and match actual DNS exactly.
The approved FQDN, application/proof database relationship, tenant/client/object
identity, encrypted certificate-validated SQL and required bootstrap authority
remain bound. There is no private-IP environment variable or connection-secret
fallback. A refusal or uncertain write is not a retry opportunity: preserve
original IDs, fingerprints and evidence and reconcile durable records.

### Verify persistence without clearing shared state

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[azure]"
az account set --subscription $subscription
$env:MONITORING_MODE = "live"
$env:MONITORING_TENANT_ID = $tenantId
.\.venv\Scripts\bi-triage.exe --sql-identity broker --operator-domain "<operator-domain>" preflight --check-sql
.\.venv\Scripts\bi-triage.exe --sql-identity broker --operator-domain "<operator-domain>" incidents
```

`--check-sql` opens a real connection and runs `SELECT 1`; it does not prove
every object grant or runtime identity. Read back an explicitly authorized
runtime-written record and retain permission/denial evidence separately.

The `run` command uses explicit fixture execution, not a live target import or
shared-state reset. `--keep-incidents` is a fixture history choice;
`--reset-shared-state` is not a supported command flag. `bi-triage reset` refuses
live monitoring mode. Never use scenario/reset commands as deployment preflight.

The SQL driver uses per-thread connections and atomic conditional statements.
One shared connection failed under concurrent use. Inside the SQL implementation,
conditional rowcounts guard a single winner without a read-then-write race;
the caller still decodes the RPC envelope rather than an EXEC rowcount.
Connection failure causes a reconnect on later use, with a 30-second
unreachable-backend cooldown.
Live stores recover by retrying shared state rather than continuing with an
empty local cache. Registry admission, claims, reservations, command/history,
incident and collaboration writes fail explicitly. A timeout does not establish
rollback: reconcile the original operation/finalization receipt before retrying.

## 3a. Hybrid registry and controlled prototype reset

These are operator procedures for a fresh Azure SQL deployment or a separately
authorized reset, not instructions to repeat the completed initialization.
Recovery and proof/application schema commits are complete. The initialization
capture of `maintenance=true`, revision `0` is historical; the current deployment
has `maintenance=false`, installed runtime users/grants and live web/controller
services. The original failed proof receipt remains unchanged with append-only
adjudication. Do not restore old maintenance, schema or grant state from that
capture. Existing exact-manifest, epoch, writer and effect guards remain required
for any later reset; normal-worker and broader acceptance remain separate
[release gates](#release-gates). No history migration or wipe occurred.

The selected switch is a clean start, not an upgrade of the prior Fabric SQL
database. Install the current application schema in Azure SQL, then initialize
empty monitoring control. These tools neither read nor reset a Fabric SQL item.
Any disposal of prior prototype history is a separately scoped old-resource
cleanup after writer/effect quiescence; it must not delete monitored Fabric
workloads or import their old application records into the new database.

The registry owns scopes, inventory generations, capability evidence, connector
ownership, due work, checkpoints, source identities, reviews and action fences.
Its deployment control pins the tenant, epoch, activation cutoff and maintenance
state. An empty registry admits nothing; it does not load
`FABRIC_PIPELINE_TARGETS` or another retired environment target list.

Only an authorized deployment operator initializes or resets this state. The
same operator may prepare and execute; no separate signer, certificate or second
person is required. The reset tool defaults to a read-only manifest and uses an
injected live observer for writer/action checks, not a caller boolean or an
observation document.

Use an explicitly selected identity for the operator tool. Azure CLI mode pins
the subscription and verifies the returned tenant/principal; Broker mode requires
an explicit account domain. Managed-identity mode is also available for a
deployer identity with the required SQL permissions.

```powershell
$resetArgs = @(
  "--server", "<server>.database.windows.net", "--database", "<database-name>",
  "--tenant-id", "<tenant-id>", "--deployer-object-id", "<operator-object-id>",
  "--credential", "azure-cli", "--subscription-id", "<subscription-id>"
)
$initializeManifest = "<operator-owned-directory>\initialize.json"
$resetManifest = "<operator-owned-directory>\reset.json"
$registrationManifest = "<operator-owned-directory>\registration.json"

.\.venv\Scripts\python.exe scripts\reset_monitoring_state.py @resetArgs `
  --plan-initialization --output $initializeManifest
```

Review the exact manifest before this explicit schema-only initialization:

```powershell
.\.venv\Scripts\python.exe scripts\reset_monitoring_state.py @resetArgs `
  --initialize --manifest $initializeManifest `
  --confirm-manifest-hash "<initialization-manifest-hash>" --expected-uninitialized
```

The Azure SQL application objects must match the current declared schema.
Initialization adds an empty maintenance/control/receipt baseline and the
declared rate-budget object; it does not delete, copy or reinterpret old
application history in that target. On a fresh target these tables are empty;
no old Fabric SQL rows are copied into them. There is no automatic runtime DDL
or schema-upgrade path.
Retain the original initialization manifest and receipt after any lost reply.

### Optional retained bootstrap journals

`dbo.triage_sql_bootstrap_receipts` and
`dbo.triage_sql_bootstrap_recoveries` are optional, preserved operator evidence.
Their absence is permitted; reset neither creates them nor requires them for
normal startup. If present, their exact columns, declared BIN2 collations,
nullability, primary/unique keys, status check, UTC defaults, table features and
`dbo` ownership must match the supported contract. Foreign keys and runtime
mutation, DDL or unreviewed-module authority are not permitted.
Unknown, lookalike or case-variant accelerator objects remain blocked; there is
no `triage_*` exemption. An incompatible journal is refused, not repaired or replaced.

Initialization and reset never delete journal rows or rewrite an original
receipt/recovery record. A reset result that claims either journal in
`deleted_counts` is invalid; they may appear only in `retained_counts`.
Offline bootstrap-apply/initialization coverage verifies
receipt preservation, not a newly built image or native deployment.
The bootstrap runner still creates its receipt table under its explicit operator
protocol; only the approved `recover` path creates the optional recovery journal.
Reset's optional-journal recognition does not transfer either responsibility.

### Reset-only writer registration

Install the reviewed permission kernel with the explicit deployer helper
`initialize_monitoring_permission_kernel`; runtime stores never call it.
Inspect the separate registration DDL without connecting to SQL:

```powershell
.\.venv\Scripts\python.exe scripts\register_monitoring_writers.py --ddl
```

Registration preparation can report old broad roles and running writers, but
such a preparation is not protected acceptance. Retire only the reviewed legacy
grants, stop writers and reconcile effects before accepting a fresh registration.

Ancillary-writer classification belongs to this reset procedure only.
`deployment_schema.ancillary_table_permissions()` describes accessed-table
profiles, not permission to mutate every listed table. Its current profiles
cover 13 controller, 7 web and 0 worker ancillary tables; approval access is
SELECT-only. Source-reviewed object/column writes count as writer authority
and still require discovery, quiescence and registration for a reset.

Classification requires actual transitive membership in a declared `dbo`-owned
kernel role with its real anchor-RPC grant and verified module/ownership
boundary. Runtime principal names alone do not qualify. Broad roles/grants,
raw approval mutation, unrestricted monitoring/control/receipt writes, grant
option, triggers/cascades and escalation paths still refuse.
This classification neither grants permissions nor endorses current live grants.
Do not turn reset registration, writer quiescence or a zero-writer result into
a normal web, controller or worker startup dependency.

The separate installation and acceptance operations are explicit:

```powershell
.\.venv\Scripts\python.exe scripts\register_monitoring_writers.py @resetArgs `
  --install --confirm-ddl-hash "<reviewed-registration-ddl-hash>"

.\.venv\Scripts\python.exe scripts\register_monitoring_writers.py @resetArgs `
  --prepare --binding-id "<deployment-binding-id>" `
  --allow-identity-association-preview --output $registrationManifest

.\.venv\Scripts\python.exe scripts\register_monitoring_writers.py @resetArgs `
  --accept --plan $registrationManifest --confirm-manifest-hash "<registration-manifest-hash>" `
  --allow-identity-association-preview
```

Before the separately authorized wipe, stop **all** writers: old hosted versions,
API/command writers, timers, collectors and provisioning jobs. Drain leases and
queued/in-flight work, and reconcile every uncertain external effect. Disabling
one timer, changing an Entra group or presenting a caller's `stopped=true` value
does not prove quiescence. Protected registration joins actual SQL principals,
transitive permissions and module/trigger authority to independently enumerated
deployment/identity bindings. The live observer reads those writers and exact
Fabric/Power BI executions with the pinned operator. A supplied
`ObservationProfile` is only a selector, never expected completeness or
quiescence evidence. Unknown bindings and unresolved relays refuse execution.

Create and review a **new** reset manifest after those preparations:

```powershell
.\.venv\Scripts\python.exe scripts\reset_monitoring_state.py @resetArgs `
  --allow-identity-association-preview --output $resetManifest

# Run only after approval of this exact manifest and existing epoch.
.\.venv\Scripts\python.exe scripts\reset_monitoring_state.py @resetArgs `
  --execute --manifest $resetManifest --confirm-manifest-hash "<reset-manifest-hash>" `
  --expected-epoch "<existing-epoch>" --allow-identity-association-preview
```

This clears only the tool's exact declared operational objects. It preserves
unrelated objects, Entra users/groups, infrastructure, business Fabric data,
service-rate budgets, protected registration/capture evidence and valid
reset/bootstrap receipts. Endpoint namespaces, keys and secrets are not reset
artifacts. The new control, empty operational
state and reset receipt commit together, with a new epoch/cutoff and maintenance
still enabled. Old operational history is discarded, not migrated or imported.

After an ambiguous result, use the original manifest, operation ID and expected
epoch to inspect/reconcile the existing receipt. Never generate another reset
identity to bypass uncertainty or erase rows written by the new release.
For an uncertain registration append, use
`register_monitoring_writers.py --reconcile --plan` with the original preparation
file and the same explicit target/credential flags. Do not mint a new request
merely because its acknowledgement was lost.
Bootstrap acknowledgement and runtime readiness are different states. The
deployment owner must explicitly release maintenance after current-release
schema, identity and persistence proof; the reset tool neither starts services
nor enables a heartbeat.

## 3b. Monitored mailbox and filter

Use a shared or licensed alerts mailbox, for example `bi-alerts@contoso.com`.
The reader does not mark messages as read; deduplication uses message IDs in
the processed-message store. Check that another process is not moving or
consuming alerts before this reader sees them.

`GRAPH_SENDER_ALLOWLIST` and `GRAPH_SUBJECT_PATTERN` are security controls,
not presentation filters. Invalid patterns fail closed. Never broaden either
to make a test message trigger; send a synthetic message that matches the
approved configuration. The ignored-message audit records why mail was
refused without weakening the filter if that audit fails.

Only polling is implemented. `GRAPH_INGESTION_MODE=subscription` is rejected,
not silently treated as push delivery. `GRAPH_POLL_SECONDS` defaults to 30 for
the local watch loop; a hosted schedule has its own cadence.

## 4. Foundry project and prompt registration

Provide a project endpoint and a model deployment available in that project.
Set `FOUNDRY_AGENT_MODEL` to the deployment name, not an assumed global model
alias. Model catalog availability is not quota or admission proof.

If the operator must create a project, the CLI requires a location even when
the parent account already has one:

```powershell
az account set --subscription $subscription
az cognitiveservices account project create `
  -n <account> -g <resource-group> --project-name <project> -l <region>
```

The public [Foundry foundation](../infra/foundry.bicep) creates the account,
project and model with `publicNetworkAccess=Enabled`, network ACL default Allow
and `disableLocalAuth=true`. It creates no managed-network injection, private
endpoint or Network Connection Approver dependency. Its three scoped Foundry
User assignments and metadata-only metrics do not grant SQL or Fabric workload
access. `accountNetworkExceptionTags` is an explicit, resource-scoped option;
ordinary deployments need no baked-in MCAPS tags.

The separate [capability-host template](../infra/foundry-capabilities.bicep) is
optional. Inspect the selected scope and existing capability host before
creating one; it is not mandatory for the basic public path. Model availability,
quota, identity and actual inference still need their own checks.

### Optional private-network hardening: historical investigation

This section preserves findings from the earlier private deployment attempt.
The selected public Foundry path does not depend on resolving that attempt's
managed private-endpoint service error. The private resources created during
that investigation have not been deleted; they are not prerequisites for the
public templates.

Foundry managed private-endpoint rules also require
`Microsoft.MachineLearningServices` registration in the target subscription,
even when this accelerator creates no customer-owned machine-learning workspace.
Check and register that supporting provider before deploying outbound rules;
a successful ARM validation alone does not establish the prerequisite.
[Resource provider registration](https://learn.microsoft.com/azure/azure-resource-manager/management/resource-providers-and-types#register-resource-provider)
enables the required provider and can add its service application. Do not grant
broad subscription Reader or Contributor access as a substitute. If the service
still explicitly reports a registration-read permission failure after registration
is verified, identify the exact Microsoft service application rather than a
similarly named app. A custom role limited to
`Microsoft.Resources/subscriptions/providers/read` addresses that metadata
permission without granting resource/data access. Verify the role, assignment
and propagation before retrying the original rule creation.

Managed-network injection can create the account capability host and active
network automatically. Read their actual names and state before deployment.
Keep the existing network read-only when adding outbound rules rather than
replacing its platform-managed metadata. Wait for project/model creation before
adding the account's web private endpoint; those child operations can temporarily
put the parent back into `Accepted`.

In that private attempt, the account, project, model and web private endpoint
were created, but managed private-endpoint service preflight failed after
provider registration and exact metadata-only read permission were verified.
An independent read-only review found no supported customer-side correction
for that observed response. Microsoft tracing would be needed to continue that
investigation, **not to deploy the chosen public architecture**. Do not infer
that private hosted agents are categorically unsupported or try broader
Reader/Contributor roles, speculative permissions or a managed-network rewrite.

For any separately designed private deployment, a web private endpoint does not
supply the hosted container's managed self endpoint for model/project calls or
its SQL/image-registry paths. Inspect current supported platform behavior and
role definitions rather than copying an older sample's broad grants. These are
optional hardening considerations, not resources emitted by the public templates.

### Foundry roles

Use **Foundry User** for agent development, **Foundry Project Manager** when
project management is needed, and **Foundry Agent Consumer** for invoke-only
callers. The project's own managed identity also needs its Foundry role.
Subscription Owner alone does not supply Foundry data-plane permissions.

The portal can assign Foundry User to the creator and project identity if the
creator may assign roles. CLI/IaC creation must not assume those assignments.
Missing access has appeared as:

```text
403 ... does not have permissions for
Microsoft.CognitiveServices/accounts/AIServices/agents/read
```

Do not use a `Cognitive Services` role or `Azure AI Developer` as a substitute
for Foundry project access. The Foundry User, Owner, Account Owner and Project
Manager roles were previously named Azure AI User, Owner, Account Owner and
Project Manager; their role IDs did not change with the rename. See
[Foundry RBAC](https://learn.microsoft.com/azure/foundry/concepts/rbac-foundry).

### Register the definitions

```powershell
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py --dry-run
az account set --subscription $subscription
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py
```

The definitions include `bi-triage`, `bi-data-quality` and the tool-free
`bi-triage-observer`. **Re-register after a prompt or tool change**: a deployed
prompt agent does not load local files. The script compares normalized
definitions and creates a new version only when needed. Remote null fields
are normalized so they do not cause false version churn.

Registration and code deployment are separate operations. The triage and data
quality agents reason; the observer explains recorded evidence. None needs
Power BI, Azure SQL or directory administration grants.

## 5. Power BI workspace and sample model

Choose a configured workspace and semantic model that the acting controller
may access. A report is optional. For a synthetic evaluation, seed an isolated
model from `mock\data\daily_sales.csv` or `daily_sales_clean.csv`, depending
on the scenario.

Do not replace an existing model's data or repoint a production workload merely
to run a scenario. The command center accepts server-configured targets, not
arbitrary workspace/model IDs supplied by the browser.

## 6. Optional Teams notifications

For the secretless web path, set `NOTIFICATION_CHANNEL=web` and
`APPROVAL_DELIVERY_MODE=web` on the controller. Teams delivery is optional and
its absence must not prevent a web decision.

The legacy notifier uses a Power Automate Workflows webhook carrying an
Adaptive Card envelope. It does not provision an identity-authenticated Teams
bot. Do not use the retired Office 365 Incoming Webhook connector as a new
deployment dependency; confirm current availability and supported migration
with [Teams webhook guidance](https://learn.microsoft.com/microsoftteams/platform/webhooks-and-connectors/what-are-webhooks-and-connectors).

A workflow URL is a bearer credential. Do not create or publish one as part of
this secretless deployment. If maintaining an already approved legacy
integration, protect the URL, verify the tenant before creating or editing the
workflow, and never print the value for confirmation. Browser SSO can select a
different organization while the workflow appears to have been created
successfully. Workflows posts use the Workflows bot; custom connector names,
icons and interactive MessageCard buttons do not carry over.

App-only Graph channel posting is not a general replacement: ordinary channel
message application access is restricted to migration scenarios. A separately
designed bot or delegated flow needs its own review.

## 6b. Legacy approval callback

The hybrid deployment does not support the legacy bearer-link approval path or
import its state. Do not deploy `infra\approval-callback.json` as a compatibility
shortcut. The command-center web API
records decisions as the validated Entra actor, and the shared approval gate
still checks fingerprint, expiry and single use.

The older callback explains why authenticated decisions are a separate boundary:

| Component | Method and boundary |
|---|---|
| Confirmation workflow | GET renders confirmation only; it has no SQL connection or write action. |
| Recording workflow | POST calls `dbo.triage_record_approval_decision` through the SQL managed connector and its managed identity. |

The split prevents link previewers, scanners and prefetchers from approving
on GET. Request triggers accept one method; an undeclared method defaults to
POST and rejects GET with `TriggerRequestMethodNotValid`. An incoming-webhook
card has no bot to handle `Action.Submit`, so the legacy card uses
`Action.OpenUrl` to the confirmation workflow.

The SQL connector's `oauthMI` parameter set uses managed identity; the callback
URL itself is still a bearer credential. Recreating a workflow can change its
identity/SID; do not drop/recreate a database user blindly during an ordinary
code update or grant a compatibility role to make the new release run.

All decision fields are procedure parameters, never concatenated SQL. A
conditional update and `@@ROWCOUNT` distinguish a recorded decision from a
refusal. The procedure refuses web-delivery proposals. Its link's responder
text is not an authenticated identity; do not use it to bypass web authorization.

An answer arriving after the waiting run has ended is not automatically
resumed. `APPROVAL_TIMEOUT_SECONDS` defaults to 300. The waiting gate still
rejects missing, malformed, expired, reused or mismatched approval. For an
older callback-free configuration, the card displayed a request ID for the
operator CLI. That fallback is not a hybrid approval or migration contract.

## 6c. Scheduled sweeps

Nothing runs on a timer merely because a monitor setting is enabled. The
monitoring worker collects inventory, polls and receives events; the controller
heartbeat drains durable source work and authenticated human commands. It does
not replace collection with a polling timer for every target.

`infra\scheduled-sweep.json` is the separate Consumption Logic App scheduler.
Its defaults are `command="heartbeat"`, `frequency=Minute`, `interval=1` and
`enabled=false`. The system-assigned identity invokes the hosted controller.
Keep the workflow disabled while preparing the new baseline and release:

| Command | Purpose | Example cadence |
|---|---|---|
| `heartbeat` | Bounded fair draining of monitoring work and human commands | Every minute, only after release proof |
| `sweep` | Mailbox drain and due retries; only after mailbox scope/filter checks | Every five minutes |
| `silent sweep` | Configured semantic-health probes | Hourly |
| `pipeline sweep` | Queue observations for currently admitted registry pipelines | Operator request; not a second collector |
| `command sweep` | Drain authenticated human commands only | Optional diagnostic/operator drain |

For a **new deployment**, prepare the workflow disabled after its prerequisites
are ready. For an existing deployment, update its reviewed scheduler rather than
creating an overlapping timer. The current deployment reused the existing
one-minute command scheduler and changed only its command to `heartbeat`;
mailbox processing was not enabled and the separate silent-sweep schedule was
unchanged. Three actual recurrence invocations produced decoded completed
responses. That evidence is scoped to those recorded invocations, not every
later controller version.

New-deployment example:

```powershell
az account set --subscription $subscription
az deployment group create -g "<resource-group>" -n sched-heartbeat `
  --template-file infra\scheduled-sweep.json `
  --parameters name=bi-triage-monitoring-heartbeat `
    projectEndpoint="<project-endpoint>" command="heartbeat" enabled=false `
    frequency=Minute interval=1 owner="<owner>" costCenter="<cost-center>" `
    environment="evaluation" dataClassification="<classification>"

az role assignment create --assignee-object-id "<scheduler-principal-id>" `
  --assignee-principal-type ServicePrincipal `
  --role "Foundry Agent Consumer" --scope "<project-resource-id>"
```

Enable the same reviewed deployment with `enabled=true` only after the current
controller, schema, invocation identity and durable execution path are proved.
Disable older overlapping timers first. Until the invoke grant propagates, runs
may fail with 403. The project-scoped grant permits that project's agent
endpoints, not just the named controller; use a narrower supported scope when
required. A workflow definition or successful deployment is not invocation proof.

The workflow requests `background=true`, `store=true`, `stream=false`, retains
the returned response ID, and polls only that ID. Its `PT15M` polling budget is
not an HTTP timeout override: Consumption HTTP requests still have a 120-second
limit. Scheduler runs are serialized, POST retries remain disabled, and only GET
status reads may retry. Install a hosting version that supports stored background
Responses and verify submit/disconnect/poll behavior before enabling the timer.

The controller uses an 840-second monotonic
admission window starting before lock acquisition, with two automatic and one
human-command concurrent slots. Slots refill within bounded queue quotas only
when the remaining window covers the execution allowance. Exhausted lock-wait
time defers that caller without cancelling the lock holder; insufficient
remaining allowance prevents new claims. Already admitted work settles under its existing policy/fences rather
than being cancelled by the admission timer. A completed heartbeat is not a
promise that every queued item ran. HTTP invocation retries remain disabled:
an ambiguous POST must not replay a possibly executed action.

The workflow records failed invocations/invalid responses as `SweepFailed`.
Its run history is not an alerting channel. The optional `alertWebhookUrl`
is a legacy bearer-URL integration; use an approved alerting route without
making it a prerequisite for the web deployment. Review both schedule history
and durable triage outcomes: a transport-completed response is not necessarily
a healthy business result.

The scheduler validates response identity on every poll, then requires completed
status without an error. Queued/in-progress/failed/cancelled/incomplete responses
cannot pass that final check, and reaching the polling limit fails the run.
Supported base64 `$content` envelopes are decoded; HTTP 200 or CLI exit zero is
not sufficient. Stored background submission is not automatic process-loss replay
of the custom controller; durable application recovery remains in Azure SQL.
[controller-health-alerts.bicep](../infra/controller-health-alerts.bicep) targets
the existing heartbeat workflow, not a second schedule:

| Alert | Condition | Window |
|---|---|---|
| Missing heartbeat | `RunsSucceeded < 1` | 15 minutes |
| Failed heartbeat | `RunsFailed > 0` | 5 minutes |

Both use one-minute evaluation. `actionGroupResourceIds` defaults to an empty
array, so alerts are portal-only. No Action Group, email address or webhook
destination has been configured or inferred.

Platform metric no-data is not assumed to mean zero successful runs. The
template also accepts optional `applicationInsightsResourceId` and
`applicationInsightsLocation` (defaulting to the workflow region). When the
resource ID is supplied, it adds a runtime log-absence alert scoped to the
application-owned Insights resource:

```kusto
traces
| where timestamp > ago(15m)
| where message startswith "heartbeat_finished status=completed "
| summarize completed_heartbeats=count()
```

`summarize` returns one zero-count row when no matching heartbeat exists.
The rule tests `completed_heartbeats < 1` over 15 minutes, evaluated each minute;
it does not rely on an absent metric sample being treated as zero.
The native healthy query returned 14 and the isolated empty query returned 0.
An isolated validation rule reached **Fired** at 09:02:23 UTC on 2026-09-18,
then **Resolved** at 09:13:23 UTC after its healthy query was restored.
The validation rule was verified disabled afterward; the production log-absence
rule remains enabled with no actions, and the existing platform alerts are unchanged.

This proves the isolated query/alert transition, not an actual controller
shutdown, external notification or end-to-end monitoring outage response.
Runtime absence can reflect scheduler, controller or telemetry failure; inspect
those boundaries rather than replaying an uncertain workload action.

Live pipeline eligibility and cadence come from the monitoring registry, not
`PIPELINE_SWEEP_ENABLED` or `FABRIC_PIPELINE_TARGETS`. Discovery searches only
semantic models and Data Pipelines, using type-filtered Fabric requests and the
typed Power BI datasets API. Operational inventory excludes older unsupported
observations without deleting their original evidence. Only admitted workload
targets are polled and dispatched.
A notebook activity remains pipeline evidence, not standalone notebook
monitoring. Full-pipeline reruns require current review, explicit approval and
atomic reservation; HTTP submission is not recovery. See
[PipelineTriage.md](PipelineTriage.md).

Foundry routines in `azure.yaml` also remain disabled. Earlier routine state
did not establish actual dispatch, and deployment did not reliably update
routine enabled-state. Reverify in the target tenant before adopting that
trigger; do not enable both routine and Logic App heartbeats accidentally.

## 7. Data quality flags

The runner selects `AzureSqlFlagTable` through the `FlagStore` protocol for live
runs, using the same Azure SQL application catalog as the other stores.
`DataQualityFlagTable` at `runs\dq_flags.csv` is fixture-only; an unavailable
SQL backend never selects local storage.

`DATA_QUALITY_FLAG_TABLE_NAME` defaults to `triage_data_quality_flags`, including
when an azd substitution is empty. The physical-table key is
`data_quality_flags`, and the deployment reset catalogue includes it. Only the
controller receives runtime append permission; worker/web identities do not.
Schema installation and shared-state reset remain deployment-owned, and the
live SQL store refuses runtime `reset()`.

Flag IDs bind the request and deterministic evidence. Atomic conditional insert
and original-row readback reconcile duplicate or uncertain appends; a
conflicting record is rejected. Both CSV and SQL redact inside persistence,
and the tool returns the actually stored redacted flag. The flag table is now
part of the committed application schema. Actual controller-MI grants/logins,
append/readback and cross-instance/restart persistence remain release gates;
schema installation is not runtime acceptance.

## 7b. Silent-failure detector

`SILENT_SWEEP_ENABLED` alone watches nothing. Configure
`SILENT_HEALTH_PROBES` explicitly because freshness expectations are business
rules, not model guesses:

```json
[{
  "name": "sales-invoices",
  "workspace_id": "<workspace-id>",
  "dataset_id": "<dataset-id>",
  "table": "fact_sales_invoice",
  "date_table": "dim_date",
  "date_column": "date",
  "report_name": "Sales invoices",
  "expected_lag_hours": 24,
  "min_absolute_drop": 40,
  "watch_schema": true,
  "load_weekdays": [1, 2, 3, 4, 5]
}]
```

```powershell
.\.venv\Scripts\bi-triage.exe health --preflight
.\.venv\Scripts\bi-triage.exe health --probes
```

`--preflight` rejects missing IDs, duplicate names for the same model and
invalid confirmation thresholds. Duplicate names overwrite one baseline;
`confirmations: 0` would disable the false-positive guard.

Use `date_table` when the fact table contains a date key. Reading the calendar
dimension alone can return a future date rather than the latest loaded fact:
the synthetic failure example has a calendar ending `2030-12-31` while facts
stop at `2024-12-23`. Check `min_absolute_drop` against table size: its default
1,000 cannot detect a drop in a 400-row table. Use `load_weekdays` for feeds
that do not load on weekends.

Two scans are required by default (`confirmations=2`). The first records
suspicion; it does not announce a confirmed finding. Probes run sequentially
with `SILENT_PROBE_PACE_SECONDS=1`, and one durable sweep lease prevents
parallel sweeps from counting the same observation twice.

### Schema checks and platform limits

`watch_schema=true` adds a query for visible columns and measures; removals
matter, additions are ignored. The implementation uses DAX `INFO.VIEW` rather
than a separate XMLA client stack. This is a compatibility check to prove on
the actual model: the public execute-queries contract excludes INFO functions.
Do not treat a successful tenant-specific test as universal API support.
An unreadable schema is a detector fault, never evidence that every column
was deleted.

The public
[execute-queries contract](https://learn.microsoft.com/rest/api/power-bi/datasets/execute-queries)
requires read/Build permission, the tenant setting, and allows 120 requests
per minute per user across datasets. It excludes service principals for
SSO-enabled and RLS models. An observed Direct Lake SSO failure returned
`401 PowerBINotAuthorizedException`; granting broader workspace rights did
not fix it. Review a supported fixed-identity connection or an Import model
where appropriate, and verify before enabling probes. Do not generalize that
failure to every Direct Lake configuration.

Contributor was the evaluated workspace role carrying Build for an app-only
probe. Viewer plus a per-dataset Build grant was not usable through the tested
dataset-users API, which rejected `principalType: App` with
`API supported only for User or Group principal types`. Verify the current
supported permission route rather than escalating to Admin.

Missing workspace access can appear as `PowerBIEntityNotFound` (404), while
tenant settings and unsupported SSO can both appear as
`PowerBINotAuthorizedException` (401). A failed measurement is not healthy data.

### Baseline changes and repeated faults

```powershell
.\.venv\Scripts\bi-triage.exe health --baselines
.\.venv\Scripts\bi-triage.exe health --accept sales-invoices
```

`--accept` is an explicit operational write: it clears suspicion for the named
planned change and lets the next scan establish the new baseline. `--accept all`
affects every probe; it is not deployment preflight. Do not reset unrelated
baselines to acknowledge one intentional change.

After five consecutive faults by default, a probe is parked for 60 minutes.
Time permits a new attempt; one successful reading clears the error streak.
This bounds repeated load from a persistent permission or unsupported-model
failure without requiring a redeploy after the cause is fixed.

## 8. Observability

Spans carry metadata only, never prompt/completion content. Container logs,
durable run/events, the authenticated API and scheduler history provide
different evidence; none alone proves all dependencies are healthy.

**Bounded native ingestion is verified.** In the 2026-09-18 capture, the active
hosted controller version 18 produced both `heartbeat_started` and
`heartbeat_finished` records in actual Application Insights queries. Captured
finished records had `status=completed`, elapsed times of approximately
6,000-26,000 ms, queue counts and zero exporter failure/warning counters.
This is backend ingestion evidence, not a conclusion drawn only from console
logs or SDK configuration. It is not an overnight, all-spans or full hybrid
acceptance claim.

The earlier missing-telemetry failure had a specific cause: a value present on
an agent-version definition was empty in the hosted process because the
Foundry project had no tracing connection. Microsoft documents
`APPLICATIONINSIGHTS_CONNECTION_STRING` as platform-reserved and injected from
project monitoring, not an agent override. See
[hosted telemetry settings](https://learn.microsoft.com/azure/foundry/agents/how-to/configure-hosted-agent-telemetry#how-hosted-agents-emit-opentelemetry).

Do not fix application health by linking Application Insights to the Foundry
project. That enables tracing across all project agents and can collect prompts,
responses and tool content, contrary to this accelerator's metadata-only rule.
Keep project tracing disconnected. See Microsoft's
[tracing and data handling](https://learn.microsoft.com/azure/foundry/observability/concepts/trace-data).

The hosted application uses `TRIAGE_TELEMETRY_CONNECTION_STRING`, read through
the `repr=False` setting `triage_telemetry_connection_string`, with managed-identity
authentication. `azure.yaml` maps that custom variable from the operator's azd
`${APPLICATIONINSIGHTS_CONNECTION_STRING}` locator value. This is not a runtime
fallback to the platform-reserved variable. The CLI continues to use its
separate standard `APPLICATIONINSIGHTS_CONNECTION_STRING` setting.
Keep locator values out of committed files and never add a secret-based fallback.

The public [application-telemetry.bicep](../infra/application-telemetry.bicep)
creates Entra-only Application Insights (`DisableLocalAuth=true`) against an
existing Log Analytics workspace. Optional `publisherPrincipalIds` assign
resource-scoped Monitoring Metrics Publisher to the actual emitting identities.
It outputs only the resource ID and creates no Foundry project connection.
Ordinary public deployments have no baked-in MCAPS exemption. Existing network
settings, tags, publisher grants and governance diagnostics must be preserved.
The verified resource has `DisableLocalAuth=true`; its existing scoped agent
publisher assignment, public networking, tags and Log Analytics binding were
preserved. See [Microsoft Entra authentication for Application Insights](https://learn.microsoft.com/azure/azure-monitor/app/azure-ad-authentication).

Hosted startup forces `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=false`
before SDK configuration and agent/host construction. It passes
`configure_observability=None` to `ResponsesHostServer`, keeping the platform
host's content-capable default pipeline out of this application-owned route.
Only the allowlisted `triage.telemetry` logger family is exported. Failure
diagnostics contain counters, exception types and sanitized source
basename/function/line, never raw exception text, SDK responses, prompts or
completions.

An empty platform `APPLICATIONINSIGHTS_CONNECTION_STRING` also caused the Azure
Monitor SDK to raise a parse `ValueError` even with a valid explicit custom
locator: the parser still examined the empty environment value. Hosted
configuration now requires the explicit app-owned locator before normalizing
only an **exactly empty** standard value to unset. Never redeclare the reserved
variable, delete/mask a nonempty value or use it as a hosted fallback.
CLI standard-variable handling is unchanged.

The correction was followed by the native paired-heartbeat evidence above,
without creating a Foundry Application Insights connection or enabling content
tracing. A `configured` result alone still does not prove ingestion; keep
console diagnostics, durable SQL outcomes, scheduler responses and queried
Application Insights records as separate evidence.

The web deployment helper's `-ApplicationInsightsResourceId` is only a portal
link. Neither it nor an existing publisher role proves telemetry ingestion.
Preserve governance-created diagnostic settings; add separate diagnostics
rather than deleting them, and do not disable content filters or Defender.

Use `azd ai agent monitor bi-triage-controller --tail 300` for hosted
diagnostics. The default 50 lines can hide the error behind SDK output; 300 is
the observed CLI maximum. Do not infer log delivery from a few visible spans.
Keep the hosting library pinned to the exact checked-in version: floating
date-stamped betas previously broke container startup.

## 9. Foundry hosted controller deployment

The hosted controller is deployed on Azure SQL with its correct
acting identity. The following is a reference for separately reviewed future
deployments, not an instruction to redeploy the working controller or reverse
the completed cutover. The earlier bounded reconciliation fix was SQL-only;
the later app-owned telemetry startup correction is in the active hosted build.
The Foundry project, model and standalone database must already exist. Deploying
the controller neither deploys the App Service UI nor creates a working timer.
Use the existing azd environment for updates; create a new one only for a
separate deployment.

```powershell
az account set --subscription $subscription
azd config set auth.useAzCliAuth true
azd env set AZURE_SUBSCRIPTION_ID "<subscription-id>"
azd env set AZURE_TENANT_ID "<tenant-id>"
azd env set AZURE_LOCATION "<region>"
azd env set AZURE_AI_PROJECT_ENDPOINT "<project-endpoint>"
azd env set AZURE_AI_PROJECT_ID "<project-resource-id>"
azd env set FOUNDRY_PROJECT_ENDPOINT "<project-endpoint>"
azd env set FOUNDRY_AGENT_MODEL "<model-deployment>"
azd env set AZURE_SQL_SERVER "<server>.database.windows.net"
azd env set AZURE_SQL_DATABASE "<database-name>"
azd env set RUN_HISTORY_ENABLED true
azd env set APPROVAL_DELIVERY_MODE web
azd env set NOTIFICATION_CHANNEL web
azd env set COMMAND_CENTER_URL "https://<app-name>.azurewebsites.net"
azd deploy bi-triage-controller --no-prompt
```

The hosted manifest sets `MONITORING_MODE=live`, real tools and the Foundry
provider, and maps `MONITORING_TENANT_ID` from `AZURE_TENANT_ID`. Keep the API,
worker and controller bound to that same tenant and SQL deployment control.
The epoch/cutoff are read from SQL, not copied from an environment target list.
Set both project-endpoint variables. Setting only
`AZURE_AI_PROJECT_ENDPOINT` previously failed dependency resolution with
`Foundry dependencies are not ready: FOUNDRY_PROJECT_ENDPOINT is not set`.
Running an unrelated provision operation does not fix a missing setting.
Keep optional legacy credential variables unconfigured; review the effective
environment rather than treating all historical `azure.yaml` entries as
required integrations.

Inspect `instance_identity` on the actual hosted agent definition after
creation/recreation. `bi-triage identity --check-scope` reads the directory;
when that operator query is unavailable, the Foundry definition supplies
`instance_identity` without a Graph lookup. New identities start without grants;
ordinary updates are not a reason to assume the identity always changes.

| Actor/scope | Permission and reason |
|---|---|
| Hosted controller / Azure SQL | Token-authenticated contained user and reviewed controller SQL role for the shared catalog; see section 3 |
| Hosted controller / Fabric | Only the source item/workspace and workload permissions required by its tools, separate from SQL access |
| Hosted controller / Power BI or configured pipeline | Only the workload permissions needed by its tools |
| Hosted controller / Foundry project | Foundry Agent Consumer to invoke its reasoning agents |
| Web UAMI / Foundry project | Foundry Agent Consumer if model-backed observer or permitted model validation is enabled |
| Scheduler / Foundry project | Foundry Agent Consumer to invoke the hosted controller |
| Prompt agents | No Power BI, SQL, mailbox or directory grants |

Verify a harmless registry-admitted read before an approved live action. A
container-written row read back through an independent authorized connection
proves more than a local scenario does. Do not issue `sweep` as a generic
health probe: it drains mail and due retries. `heartbeat` and `command sweep`
execute eligible durable work; `pipeline sweep` queues source reads and silent
sweeps update detector state. None is a generic process-health probe.

`azd deploy` with no effective change can finish without restarting the
container. Verify the deployed version and observed restart rather than
assuming a successful deploy cleared process state. See
[the hosted architecture](foundry/README.md) for concurrency and routine limits.

## 10. Command-center Entra authorization

The API validates RS256 v2 Entra access tokens for the configured tenant and
API client ID, including signature, issuer, audience, tenant, required
timestamps and `oid`. It requires delegated `access_as_user` plus recognized
app-role claims. An ID token, an app-only token, a SQL grant or a client-supplied
actor header is not an alternative.

| App role | Capability |
|---|---|
| `CommandCenter.Reader` | Read authorized records, health/history and ask tool-free questions |
| `CommandCenter.Operator` | Reader capabilities, configured investigations, notes and human tracking resolution |
| `CommandCenter.Approver` | Reader capabilities and explicit pending approval/denial decisions |
| `CommandCenter.Admin` | All app capabilities, isolated validation and reconciliation; no directory administration |

Operator and Approver do not imply each other. App roles do not grant the
person Azure/Fabric service permissions.

### Register or update the SPA/API

Run the offline plan first:

```powershell
.\.venv\Scripts\python.exe scripts\register_command_center.py `
  --subscription "<subscription-name-or-id>" --tenant-id "<tenant-id>" `
  --display-name "Example Triage Command Center" `
  --webapp-origin "https://<app-name>.azurewebsites.net" --dry-run
```

Remove `--dry-run` only for an authorized registration operation.
For an existing application, supply `--application-id "<application-client-id>"`.
The script preserves role/scope IDs, rejects ambiguous or unmarked same-name
applications unless explicitly identified, and refuses registrations with
existing passwords/certificates rather than deleting them. It configures
single-tenant authorization code + PKCE, disables implicit grants and enforces
`appRoleAssignmentRequired=true` on the enterprise application.

Optional flags are distinct choices:

| Flag | Effect |
|---|---|
| `--localhost-redirect-uri` | Adds an explicit loopback development redirect |
| `--admin-current-user` or `--admin-user-object-id` | Adds a direct Admin assignment as an optional bootstrap |
| `--authorize-azure-cli` | Preauthorizes Microsoft's CLI client for this API's scope only |
| `--grant-admin-consent` | Grants `access_as_user` for the selected administrator, for the SPA and opted-in CLI client; not `AllPrincipals` |
| `--grant-profile-consent` | Grants delegated Graph `User.Read` for only that selected user and SPA; not a Graph application permission |

Consent flags require a selected administrator and separately authorized
directory/consent privileges. The selection flags also assign that user's
direct Admin bootstrap role; do not use them for routine end-user onboarding.
Assignment-required apps need administrator consent; selected-user grants do
not consent future group members. IT must arrange their approved consent
process. Remove evaluation CLI access when no longer needed.

### Provision the four groups

```powershell
.\.venv\Scripts\python.exe scripts\configure_command_center_groups.py `
  --subscription "<subscription-name-or-id>" --tenant-id "<tenant-id>" `
  --app-id "<application-client-id>" --group-prefix "Example Triage" `
  --admin-current-user
```

This defaults to an **offline plan**, unlike registration without `--dry-run`.
Add `--apply` only for privileged operator provisioning. It checks the enabled
single-tenant enterprise application, role definitions, administrator and
active P1/P2 plan. It creates or reconciles four ordinary security groups,
rejects name/ownership-marker collisions and role-assignable groups, assigns
the roles, and reads back owners, the administrator's direct Admin-group
membership and every app-role assignment.

The selected administrator owns each group and is added as a member of the
Administrators group. Ownership alone is not app access. The script does
**not** remove existing direct assignments, delete groups or change operational
SQL data. A delayed/ambiguous directory write must be reconciled before retrying.

Entra ID P1/P2 is required for group-based assignment. Nested group membership
does not cascade. IT manages app assignments; group owners manage membership
externally. The application has no roster or membership editor and needs no
`AppRoleAssignment.ReadWrite.All` or `Group.ReadWrite.All` on its UAMI. See
[Entra group assignment](https://learn.microsoft.com/entra/identity/enterprise-apps/assign-user-or-group-access-portal).

### Historical authorization-only cutover

The sequence below describes the earlier SQL-ACL-to-Entra authorization change,
not the current Azure SQL clean start. It preserves the reason for the old-mode
deployment guard; it is not a legacy-state migration or a prerequisite to import
old memberships. The new baseline reads only validated Entra app-role claims.
Do not silently disable an active old authority while retiring that deployment.

1. Inventory the exact existing app, role IDs, direct assignments, group
   assignments, old flag and operational tables. Preserve existing records and
   an operator-controlled copy of any legacy access evidence.
2. Establish the Entra app roles, administrator group, direct group membership
   and group role assignment. Before disabling the old flag, prove a freshly
   issued, cryptographically validated API token contains Entra Admin for the
   correct tenant/audience/scope. An old SQL-authorized Admin display is not
   this proof. Retain a reviewed bootstrap path until cutover is confirmed.
3. Explicitly set the old live app's
   `COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED` to false after that proof.
   The deployment helper refuses both a requested true value and an existing
   true value; it does not perform the authority switch for you. New code
   rejects `access_management_enabled=true` at startup.
4. Deploy the Entra-only code. Verify `/api/access` reports
   `source=entra_app_roles`, Admin operations work for the intended actor and
   lower-role operations remain refused. Preserve all operational state.
5. Only after successful group setup/readback, remove the specifically reviewed
   direct bootstrap assignment. Obtain another fresh API token and prove Admin
   through the group-based path. Do not remove every direct assignment or
   assume the group script already removed them.
6. Only after fresh-token group access and operational continuity are confirmed,
   retire the separately identified legacy permission table under the approved
   retention process. Do not drop, recreate or clear the standalone database
   or any incident, approval, command, history or collaboration table.

The old SQL access service/store and its initialization script have been
removed. There is no dual SQL/Entra authority to re-enable.

### Access display and human closure

Access & permissions calls `/api/access`: it reports the presented token's
effective roles, tenant/application and issue/expiry dates. It does not read
SQL membership, query Graph directory membership or identify the supplying
group. Refresh permissions forces a new API-token request, then reloads
access and the command-center snapshot. If interaction is required, sign in
explicitly; the refresh does not silently open consent.

Changes depend on Entra propagation and token renewal. Refreshing one session
does not revoke anyone else's already-issued token. The optional browser
profile-photo request uses a separate delegated Graph `User.Read` token.

Human closure is append-only tracking, not verified remediation. It binds to
the original SQL NVARCHAR payload hash (SHA-256 over UTF-16 LE), plus the
tracking version. New evidence invalidates the closure without resetting
controller incidents, budgets, notifications, approvals or claims. This is why
the web UAMI inserts into `triage_incident_activity` but cannot update
`triage_incidents`.

## 11. App Service infrastructure and code releases

`infra\command-center.bicep` provisions the independent public HTTPS web host and
its managed identity. The resource group, Foundry services and Azure SQL
database/firewall/auditing are separate prerequisites. No VNet, NAT, private
endpoint or private DNS is required. Rayfin is not required.

### Infrastructure helper

Inspect the real command help and validate before provisioning:

```powershell
Get-Help .\scripts\deploy_command_center.ps1 -Detailed

.\scripts\deploy_command_center.ps1 `
  -Subscription "<subscription-name-or-id>" -ResourceGroup "<resource-group>" `
  -Location "<region>" -AppName "<app-name>" `
  -TenantId "<tenant-id>" -ApplicationClientId "<application-client-id>" `
  -CostCenter "<cost-center>" -Owner "<owner>" -Environment "evaluation" `
  -DataClassification "synthetic" `
  -ApplicationSettingsFile "<operator-owned-settings.json>" -ValidateOnly
```

Without `-Deploy`, the helper runs ARM validation and resource-ID-only what-if,
not provisioning. The settings file is a JSON object of string values, not an
`.env` file. It requires `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE` and
`FOUNDRY_PROJECT_ENDPOINT`; credentials and managed-setting overrides are
rejected. Do not commit it.

For new infrastructure, use `-Deploy -ProvisionOnly` to create the UAMI/host,
then grant its external permissions and install SQL schema separately.
`-Deploy` without `-ProvisionOnly` also builds and uploads code. This helper
does not request quota, register agents, grant directory/RBAC permissions or
prove backend firewall admission, identity access or application readiness.

Verify the selected App Service SKU's regional quota **and admission**, Python
3.13 availability, Azure SQL regional admission, Foundry hosting/model capacity
and Fabric workload/Eventstream capacity.
Supported helper SKUs include B1/B2/B3, S1/S2/S3, P0v3/P1v3; do not assume the
default B1 can be provisioned merely because the region lists it.

### Network and governance invariants

The template always uses public HTTPS. `publicAccessClientCidrs` and
`scmAccessClientCidrs` are independent optional ingress filters; empty arrays
leave their respective network endpoints public. They do not grant anonymous
access to protected API routes or deployment permission.

The helper exposes these as persistent `-PublicAccessClientCidr` and
`-ScmAccessClientCidr` values. Supply only the reviewed caller ranges when a
filter is wanted; neither endpoint inherits the other's list. For example,
add `-PublicAccessClientCidr "<browser-egress-ip>/32"` and
`-ScmAccessClientCidr "<deployment-egress-ip>/32"` to the reviewed helper command.

Omitting both uses the normal public baseline. There is no temporary-public
mode or automatic restoration to private networking. Entra app roles continue
to govern the API, and Entra/Azure RBAC independently governs SCM publishing.

Verify public SQL firewall admission, TLS and actual Entra sign-in separately
from web liveness. Foundry inference and Fabric item access are separate
service permissions, not consequences of opening the web endpoint.
SCM basic publishing and FTPS remain disabled. Use Entra-authenticated CLI
(Azure CLI 2.48.1 or later) or Kudu REST, never publishing passwords/profiles.
`/api/health` proves only that the web process answers in live mode. Verify
authenticated API, SQL, observer and command/scheduler paths independently.

If a filtered deployment uses Global Secure Access or another proxy, verify
the source addresses actually observed by each endpoint. A main-site allowlist
does not establish SCM reachability or uniquely identify a device. Keep Entra
authorization in place rather than broadening a filter to hide an identity
failure. See [App Service access restrictions](https://learn.microsoft.com/azure/app-service/app-service-ip-restrictions).

All taggable resources and the resource group need `CostCenter`, `Owner`,
`Environment` and `DataClassification`. The App Service plan, SQL, registry,
worker, Fabric capacity and hosted/model use incur separate costs. The baseline
does not add NAT or private-endpoint costs. For a finite
approved evaluation, `-Evaluation` enables admin-only isolated validation;
`-EvaluationCostExemption` adds `CostControl=Ignore` and requires a future
`-EvaluationExpiresOn`. Those tags do not shut anything down. Disable validation
and remove the exemption by the recorded review date.

### Code-only update to an existing app

**Do not run a default full infrastructure deployment for a code-only change.**
Empty default filter arrays can replace an approved persistent ingress policy.
Preserve the existing app, UAMI, database, runtime settings and independently
approved app/SCM filters. Retained infrastructure from earlier private tests is
not deleted or repurposed as part of a code upload.

Build a new artifact outside the repository; do not restore dependencies unless
needed:

```powershell
$package = Join-Path $env:TEMP "triage-command-center-release.zip"
.\scripts\package_command_center.ps1 -OutputPath $package
```

The packager type-checks/builds the UI and includes source, scenarios, mock
inputs and `command-center\dist`, with generated `requirements.txt` containing
`.[web,azure]`. It excludes environment files, credentials, caches and
publishing material and runs the repository credential gate. Use a new output
name or explicit `-Force`; it will not overwrite an existing artifact silently.

Before any ZIP POST, confirm public app **and SCM** reachability from the
deployment host, including any configured endpoint-specific caller filters.
If an operator separately authorizes a one-off filter adjustment, record the
original values and restore those values afterward. The helper does not open a
temporary public window or revert the app to private networking.

Require repeated successful **Entra-authenticated, read-only** SCM probes,
such as `GET /api/deployments`, before uploading. Restriction propagation can
differ between SCM frontends; one 200 response or an ARM update result is not
stable readiness. If different resolved frontends are used, establish
consistent results with the correct hostname/TLS SNI before the write.
The current deployment helper has a post-upload health probe, not this
pre-upload stability gate; the operator must establish it first.

Once readiness and identity are confirmed:

```powershell
az account set --subscription $subscription
az webapp deploy --subscription $subscription -g <resource-group> -n <app-name> `
  --src-path $package --type zip --clean true --restart true `
  --async false --track-status false --timeout 1800000
```

Control-plane startup tracking previously stalled after Kudu completed and the
API served requests, so inspect Kudu deployment status and the actual
application, not just the CLI wait. An accepted POST, timeout or lost response
can leave a deployment running: inspect its existing deployment ID/status and
reconcile before another upload. Do not blindly retry ambiguous writes.

Verify the intended persistent app/SCM filters after the operation, and restore
any separately approved one-off filter adjustment even if the upload fails.
Do not change SQL or other service access as a side effect. Then prove authenticated Entra
roles, operational data continuity and the specific deployed feature. A
successful ZIP upload is not proof of SQL authorization or backend readiness.

See [deployment without basic authentication](https://learn.microsoft.com/azure/app-service/configure-basic-auth-disable)
and [App Service access restrictions](https://learn.microsoft.com/azure/app-service/app-service-ip-restrictions).

For optional private hardening only, the earlier deployment found that the
inline `vnetRouteAllEnabled` setting did not establish effective routing;
site-level `outboundVnetRouting.allTraffic=true` needed readback. Existing
governance NSGs must not be detached by an infrastructure redeployment.
Those historical findings do not add network inputs to the public template.
See [App Service routing](https://learn.microsoft.com/azure/app-service/configure-vnet-integration-routing)
and [private endpoints](https://learn.microsoft.com/azure/app-service/overview-private-endpoint).

## 12. Hybrid monitoring worker and Eventstream

The worker supports two explicit modes. The deployed starting point is
`python -m triage.monitoring.worker --collector-only`: durable inventory and
REST polling without Eventstream configuration. Event mode omits that flag and
requires the complete owned connector binding before enabling receiver and
provisioner behavior. The controller separately publishes validated authority.
Neither worker mode runs a reasoning agent, refreshes a semantic model, reruns a
business pipeline or grants permissions.

The worker uses a dedicated zero-write policy with empty action allowlists,
not controller settings or an implicit permissive default. Collector-only
configuration rejects partial or residual event metadata; missing metadata is
not an automatic mode switch. Its durable heartbeat has `connector_id=null`
and cannot assert transport connectivity, delivery or connector readiness.

### Stage the environment before the consumer

`scripts\deploy_monitoring_worker.ps1` separates environment preparation from
the full worker. `-EnvironmentOnly` needs no image, SQL connection metadata or
Eventstream destination. That breaks the dependency cycle in which endpoint
metadata could not exist until a managed-identity canary had run.

The example below uses an existing resource group and Log Analytics workspace.
The environment template creates a public Consumption environment with keyless
Azure Monitor routing. No existing VNet, subnet, NAT Gateway or private DNS is
required. The full worker can instead use a verified existing public
Consumption environment with the expected logging configuration.

The separate [monitoring prerequisites](../infra/monitoring-prerequisites.bicep)
create only a dedicated UAMI, public Basic ACR and registry-scoped `AcrPull`.
Registry admin and anonymous access remain disabled, Entra ARM authentication
is enabled and the permission mode is `LegacyRegistryPermissions`.
`registryExceptionTags` is optional and scoped only to that registry; it is not
an exemption for the environment or other resources.

Prepare locally before any cloud operation:

```powershell
Get-Help .\scripts\deploy_monitoring_worker.ps1 -Detailed

.\scripts\deploy_monitoring_worker.ps1 -EnvironmentOnly `
  -SubscriptionId "<subscription-id>" -TenantId "<tenant-id>" `
  -ResourceGroup "<resource-group>" -Location "<region>" `
  -EnvironmentName "<monitoring-environment-name>" `
  -LogAnalyticsWorkspaceResourceId "<log-workspace-resource-id>" `
  -CostCenter "<cost-center>" -Owner "<owner>" -Environment "evaluation" `
  -DataClassification "<classification>" `
  -OutputDirectory "<new-operator-output-directory>" -Mode Prepare
```

`Prepare` compiles locally with `--no-restore`; it does not contact Azure,
Fabric or SQL. `-BicepPath` selects an already-installed compiler. `Preflight`,
`Validate` and `WhatIf` are explicit cloud-reading/validation modes and require
`-AzureConfigDirectory` pointing at a separately authenticated, nonshared CLI
profile. Only `-Mode WhatIf -Execute` deploys the reviewed change. Environment-only
bootstrap creates no worker and does not adopt/rewrite another environment.

If a **new Azure environment** needs a specifically approved exception, the
helper accepts `-EnvironmentExceptionTagsFile`. Its string-valued map must
include `ExceptionReason` and a future `ExceptionReviewAfter`. These are review
metadata, not automatic expiry; the helper does not apply them to the worker,
other resources or Fabric, and does not retag an existing environment.

For the full worker, select a new public environment with `-EnvironmentName`
or a verified existing public Consumption environment with
`-ExistingEnvironmentResourceId`; do not supply both. Supply the worker name, its dedicated existing
UAMI, ACR resource ID, immutable image digest and SQL hostname/catalog. Choose
`-CollectorOnly` or `-ConnectorBootstrapFile`, never both. The helper neither builds/pushes an image nor grants its external
permissions. Build `Dockerfile.monitoring` for `linux/amd64` through the approved
build path; ACR admin authentication stays disabled and image pull uses the
selected UAMI.

The worker has no HTTP/TCP ingress or health endpoint. Its template uses one
container, single-revision mode and bounded replicas, not backlog autoscaling.
Verify MI image pull, public DNS, outbound TLS/SQL firewall admission, quotas and measured
resource use; ARM success is not worker or hybrid acceptance.

### Collector-only quickstart

Use this path to populate the workspace inventory before configuring an
Eventstream. The existing SQL baseline, dedicated MI, reviewed SQL/Fabric
permissions, registry image and public environment remain prerequisites; the
script does not grant access or auto-admit monitoring scopes.

```powershell
.\scripts\deploy_monitoring_worker.ps1 -CollectorOnly `
  -SubscriptionId "<subscription-id>" -TenantId "<tenant-id>" `
  -ResourceGroup "<resource-group>" -Location "<region>" `
  -WorkerName "<worker-name>" `
  -WorkerIdentityResourceId "<dedicated-uami-resource-id>" `
  -RegistryResourceId "<registry-resource-id>" `
  -Image "<registry>/<repository>@sha256:<reviewed-image-digest>" `
  -ExistingEnvironmentResourceId "<public-consumption-environment-resource-id>" `
  -LogAnalyticsWorkspaceResourceId "<log-workspace-resource-id>" `
  -AzureSqlServer "<server>.database.windows.net" `
  -AzureSqlDatabase "<application-database>" `
  -InventoryMode caller_visible `
  -CostCenter "<cost-center>" -Owner "<owner>" -Environment "evaluation" `
  -DataClassification "<classification>" `
  -OutputDirectory "<new-operator-output-directory>" -Mode Prepare
```

`Prepare` is local. Review its output, then use the helper's explicit cloud modes
with an isolated `-AzureConfigDirectory`; only `-Mode WhatIf -Execute` permits
the reviewed deployment. For authorized tenant-admin inventory, explicitly
select `-InventoryMode tenant_admin_preview`. This selects admin domains/
workspaces and preview Admin Items; it grants no permission and does not prove
source telemetry access.

The Bicep default is `collectorOnly=true` with nullable `connectorBootstrap`.
The PowerShell helper requires the explicit `-CollectorOnly` switch or the
complete event-mode bootstrap file, avoiding accidental event-mode omission.
Leave all `MONITORING_CONNECTOR_ID`/`MONITORING_EVENTSTREAM_*` values absent in
collector-only mode. When event transport has its own reviewed ownership and
identity proof, select event mode explicitly (`collectorOnly=false`) with the
full nonsecret binding; do not weaken the event-mode validation.

That operator binding must identify the owned connector, transport workspace,
Eventstream and destination IDs, plus namespace, entity and consumer group.
Verify the selected worker MI's stream access and the controller-published
connector authority; a file of IDs does not grant ownership or access.
Obtain the nonsecret values through the reviewed Entra endpoint/topology path,
never a key-returning API. Switching modes still requires actual event receipt,
durable acceptance/checkpoint and restart proof before claiming hybrid coverage.

After deployment, distinguish worker liveness, durable metadata acceptance,
source-access probes, configured/admitted scopes and action authority.
The verified snapshot accepted 332 workspaces and enabled both authenticated
workspace selectors with 333 options including the placeholder. Broader item
coverage is partial; dataset 401s and request-budget/throttling gaps remain
visible. Snapshot counts are deployment/estate-wide, and **Inventory total**
must remain Unknown until all latest discovery generations are complete.
Zero scopes do not make partial inventory complete. No fake workspace list,
automatic scope admission or extra Fabric grants are part of this quickstart.

Selector-aware scope preview is separate: complete workspace A can be ready for
review while partial B keeps the estate snapshot Partial/Unknown. Do not report
A's three items plus B's five as a complete inventory total of eight.
Workspace/domain metadata outages still block expansion; explicit disable/
contraction can operate on stored admissions. The source revalidation helper
permits unrelated data-revision drift only when the original scope is evaluated
inside the locked activation transaction and its reviewed material effects are
identical. Target, capability, subscription, gap, permission, TTL, epoch and
policy changes refuse. Combined source/UI review and new native activation
acceptance remain pending.

The bounded priority proof processed an original fresh web-discovery request
on attempt 1 without manual requeue behind a snapshot of 579 waiting and 207
queued items. Its selected workspace scan completed ten pages and three items,
including one unsupported Eventstream. Partial-window replay churn remains
unoptimized; this is not a general backlog/SLA or scope-activation acceptance.

### Owned canary and nonsecret endpoint binding

Run `scripts\monitoring_eventstream_canary.py` only in an explicitly authorized
finite managed-identity canary job. It consumes the bounded
`MONITORING_CANARY_INPUT` contract, with `create`, `inspect` or `resume` intent.
It does not need worker endpoint metadata or SQL settings. Journal the create
intent before starting the job and disable job retries: the request ID is
correlation, not a server idempotency guarantee. After an uncertain create,
inspect the owned item or resume its recorded operation; do not submit another
create blindly.

Creation and definition/topology readback do not prove consumption. The current
key-returning destination connection API must **not** be called to obtain
endpoint metadata. Use the owned Custom Endpoint destination's **Microsoft
Entra ID** tab to obtain only the nonsecret namespace, entity and consumer-group
values. Confirm the workspace, Eventstream and destination IDs against the
owned definition/topology. Do not copy connection strings, shared keys or SAS.
This manual bootstrap remains required. Key-free endpoint automation is
unproved; a successful isolated MI reception test does not establish it.

The event-mode helper expects `ConnectorBootstrapFile` to contain exactly:

```json
{
  "connectorId": "<connector-uuid>",
  "workspaceId": "<transport-workspace-uuid>",
  "eventstreamId": "<owned-eventstream-uuid>",
  "destinationId": "<custom-endpoint-destination-uuid>",
  "fullyQualifiedNamespace": "<nonsecret-namespace-host>",
  "eventHubName": "<nonsecret-entity-name>",
  "consumerGroup": "<consumer-group>"
}
```

The file identifies transport metadata; it does not configure monitored targets,
publish SQL ownership or authorize actions. Keep populated files outside the
repository. The reviewed initial definition/topology and endpoint must also
cross the controller's guarded publication/observation contract. Supplying
environment strings alone does not create an `OwnedConnectorManifest`, bind
physical source IDs or establish readiness.

### Register existing app-owned connector metadata

[`scripts\register_monitoring_connector.py`](../scripts/register_monitoring_connector.py)
is a separate **SQL-connected operator workflow**, not local candidate export.
It registers reviewed physical ownership for an already-created app-owned
Eventstream. It never calls Fabric or a key-returning endpoint, creates schema,
grants roles, changes maintenance, queues work, admits targets or publishes
protected desired state/readiness.

| Mode | Contract |
|---|---|
| `--prepare --capture ...` | SELECT-only SQL checks against the explicit target/identity, then an original hash-bound plan |
| `--apply --plan ... --confirm-manifest-hash ...` | Atomic metadata-only registration and immutable receipt, using the exact prepared target/deployer and reviewed plan hash |
| `--reconcile --plan ...` | SELECT-only original-receipt lookup after uncertainty; no write is retried |

The operator-owned capture is a closed, explicitly reviewed input. Extra or
duplicate JSON fields and credential-bearing data are refused. It must bind:

| Capture evidence | Required meaning |
|---|---|
| `version`, `provenance`, `review` | Version `1`, exactly `operator_reviewed_capture` and `original_creation_and_complete_current_readbacks_reviewed` |
| `request_id`, `expected`, `expected_maintenance`, `expected_connector_revision`, `connector_id`, `ownership_id`, `name` | Original request plus current tenant/epoch/policy and exact prior connector state |
| `creation_request`, `creation_request_sha256`, `creation_receipt`, `creation_receipt_sha256` | The actual app-owned create intent, completion and ownership marker, not adoption by display name |
| `item`, `definition_response`, `topology_response`, `observed_at`, `readback_sha256` | Fresh complete readbacks bound to that same item and component identities, within the 15-minute preparation/apply window |
| `sources`, `endpoint` | Exact retained physical source IDs/current pipeline target identities and explicit nonsecret namespace/entity/consumer-group metadata |

The current capture contract supports pipeline sources; it does not make native
semantic-model event support an assumption. Hashes bind reviewed evidence but
do not prove its network origin, the collector MI's service access or delivery.
The seven-field event worker bootstrap file alone is not this ownership capture.
Only an absent connector or the exact current unbound planned connector without
desired-publication/work/effect history is eligible. An established or conflicting
binding requires reconciliation, not replacement.

Use the same explicit target and identity flags for all operations. This example
selects an already-authorized Azure CLI operator; Broker selection instead needs
`--operator-domain`, and managed-identity selection needs
`--managed-identity-client-id`. No credential is created:

```powershell
az account set --subscription "<subscription-id>"
$connectorArgs = @(
  "--server", "<server>.database.windows.net",
  "--database", "<application-database>",
  "--tenant-id", "<tenant-id>",
  "--deployer-object-id", "<operator-object-id>",
  "--credential", "azure-cli",
  "--subscription-id", "<subscription-id>"
)
$capture = "<operator-owned-directory>\connector-capture.json"
$plan = "<operator-owned-directory>\connector-plan.json"

.\.venv\Scripts\python.exe scripts\register_monitoring_connector.py @connectorArgs `
  --prepare --capture $capture --output $plan
```

After reviewing the exact plan and while its capture is still current:

```powershell
.\.venv\Scripts\python.exe scripts\register_monitoring_connector.py @connectorArgs `
  --apply --plan $plan --confirm-manifest-hash "<reviewed-plan-hash>" `
  --output "<operator-owned-directory>\registration-result.json"
```

After an uncertain acknowledgement or failed result-file publication, retain
the original plan and request identity:

```powershell
.\.venv\Scripts\python.exe scripts\register_monitoring_connector.py @connectorArgs `
  --reconcile --plan $plan
```

An absent or conflicting receipt leaves registration unestablished; do not
mint a new request ID or repeat an unconfirmed write. Existing output paths
are refused; use an existing operator-owned parent directory and a new filename.
Output publication writes and fsyncs a temporary file, then uses
an exclusive hard link to publish complete bytes without overwriting another
result: the final name is complete or absent under a process interruption.
This is not a directory-entry power-loss durability guarantee.

A successful receipt says `registered_metadata_only`, requires controller
publication and retains `identity_verified=false`/`delivery_verified=false`.
The connector remains planned/gated. Native read-only preflight has passed;
that is not native `--apply`, runtime transport or readiness acceptance.
The older preflight plan was never applied, expired and is retained as evidence.
Use a fresh complete capture and newly prepared plan from the current source;
do not rewrite or reuse that historical plan as current authorization.

### Registration to event-mode readiness

Registration and event readiness are separate gates:

1. Register the exact owned physical metadata with the original creation and
   fresh complete readbacks. It grants no scope admission or capability.
2. Establish a reviewed admitted scope and current read/event capability.
   Fresh topology/source-Running and read probes must use the same collector MI.
   They establish only event capability; remediation/action capability is unchanged.
3. Let ordinary controller reconciliation publish the first protected desired
   state. If current admission/capability already exists and desired publication
   is absent, it can publish at the **same policy revision**. Do not make a
   meaningless scope edit just to force publication.
4. An event-mode receiver may collect proof while degraded only when current
   ownership, policy, definition, source and endpoint bindings match the protected
   publication. Reverify when that publication identity changes; matching a cached
   connector object is not sufficient.
5. Accept an actual original stream receipt and position durably, preserving the
   accepted payload hash and full provenance. Actual identity-check time and later
   receive time are distinct evidence; do not replace one with the other.
6. Only the controller publishes readiness after those checks. Work completion
   and publication bind the actual work/context, owner, fence, revision and
   eligible original receipt. Heartbeats, an empty stream, quarantine or stale
   proof cannot establish Ready.

Before the first protected desired publication, registered physical sources
without current admitted read/event-capable targets remain **dormant**.
They are not automatically removed merely because admission is empty.
After desired state has been published, later revocation still immediately
fences new intake and retains physical ownership until complete original
observation proves exact remote absence.

The current handoff has no accepted native registrar apply or new event-runtime
durable-delivery/Ready proof. The earlier transport-only canary and read-only
registrar preflight do not close these gates.

### Controller publication and source lifecycle

Workers report observations and web callers submit intents. Deterministic
controller reconciliation publishes validated state through the static SQL
contract. An accepted/configuring intent remains pending until that publication;
it is not an instruction to update raw monitoring tables.

New Eventstream sources begin as logical proposals with `source_id=null`.
The worker records the original complete owned definition after the remote
operation. Controller publication supplies `observation_receipt_id`, and SQL
validates that exact receipt before binding returned physical IDs. This also
applies to already-existing unresolved proposals. Do not fabricate IDs or read
worker-private receipt views from a controller caller.

A source removal includes `removal_id`, `source_id`, `proposal_id` and `detail`;
both selector keys must be present and exactly one nonnull. Desired removal
fences intake immediately while retaining ownership and established IDs.
`pending_removals` remains pending until an original complete current receipt
proves absence of the exact node, ID and stream routing. Only then does
publication return `retired_sources` with immutable retirement evidence.
An uncertain/inherited observation or null SQL-derived
`observed_definition_hash` is not absence proof.

Keep desired state, actual observations, receipt-bound materialization and
readiness separate. The deployed collector-only worker and controller have
proved bounded metadata collection and reconciliation, not the event-mode
connector lifecycles. A populated workspace selector does not establish source
materialization, event delivery or source removal.

### Worker configuration and checks

The deployment template supplies these exact nonsecret settings:

| Group | Settings |
|---|---|
| Tenant and mode | `MONITORING_MODE=live`, `MONITORING_TENANT_ID`, `AZURE_TENANT_ID` |
| Inventory selection | `MONITORING_INVENTORY_MODE=caller_visible` by default; explicit `tenant_admin_preview` opt-in |
| Pinned UAMI | `AZURE_SUBSCRIPTION_ID`, `AZURE_CLIENT_ID`, `MONITORING_IDENTITY_OBJECT_ID`, `MONITORING_IDENTITY_RESOURCE_ID` |
| Durable state | `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`; the same application catalog as web/controller |
| Owned connector, event mode only | `MONITORING_CONNECTOR_ID`, `MONITORING_EVENTSTREAM_WORKSPACE_ID`, `MONITORING_EVENTSTREAM_ID`, `MONITORING_EVENTSTREAM_DESTINATION_ID`; absent in collector-only mode |
| Entra endpoint, event mode only | `MONITORING_EVENTSTREAM_NAMESPACE`, `MONITORING_EVENTSTREAM_ENTITY`, `MONITORING_EVENTSTREAM_CONSUMER_GROUP`; absent in collector-only mode |

The worker reads its process environment, not `.env`. It rejects fixture mode and
credential-bearing environment settings. Normal startup requires ready shared
control, the collector and the store's `EventPersistence` operations; it never
falls back to an in-memory store or transport-only probe.

With the required process environment supplied, these are separate operations:

```powershell
.\.venv\Scripts\python.exe -m triage.monitoring.worker --collector-only

.\.venv\Scripts\python.exe -m triage.monitoring.worker --transport-probe `
  --probe-workspace-id "<owned-source-workspace-uuid>" `
  --probe-item-id "<owned-source-pipeline-uuid>" --probe-seconds 120

.\.venv\Scripts\python.exe -m triage.monitoring.worker --reconcile-once
```

The finite transport probe does not construct SQL state, advance a durable
checkpoint, verify source REST evidence or establish normal worker health.
Its source workspace may differ from the transport workspace. Never run that
finite command as an always-restarted production container.
`--reconcile-once` uses shared SQL work and can update an owned Eventstream
definition; it is not a harmless connectivity check or proof of event delivery.
Explicit `--collector-only` starts durable inventory/polling and non-transport
health only, with no receiver/provisioner. `python -m triage.monitoring.worker`
without that flag remains the strict event-enabled consumer/maintenance path.

Lease renewal re-reads the authoritative work revision rather than replaying a
stale local revision. New inventory rows are accepted through bounded checked-view
batches of at most 50 rows/950 parameters under the same lease and atomic
acceptance transaction. A short affected-row count rolls the write back.
Receipt-backed catalogue metadata reads retain original identity, fingerprint,
payload and row-hash evidence; their paging optimization does not publish
admission or broaden worker authority.

### Coverage, identity and network limits

Tenant read-admin APIs provide inventory metadata, not operational visibility
or source access. Core workspace/item lists are caller-visible. Admin Items is
preview and requires explicit adapter selection and appropriate admin settings;
granting permission alone does not change an adapter. The worker setting
`MONITORING_INVENTORY_MODE=tenant_admin_preview`, or the deployment helper's
`-InventoryMode tenant_admin_preview`, explicitly selects the admin
workspace/domain adapters and preview Admin Items. Default `caller_visible`
does not promise a tenant-complete inventory. Denied or incomplete admin reads
remain gaps; neither mode establishes all-tenant operational telemetry.

Verify collector source reads with the actual UAMI, including Power BI's Write
requirement for refresh history. Verify stream-workspace access separately.
The controller's action permissions and the browser user's ordinary Entra
app-role groups are other boundaries. Default target admission is observation
only; neither discovery nor an event grants replay safety or action authority.

The chosen Custom Endpoint is public outbound transport. It does not support
tenant/workspace Private Link; an optional private SQL/model path would not
make that endpoint private. The consumer uses AMQP over WebSockets from the
public Consumption environment. It needs outbound TLS, not NAT/VNet prerequisites,
inbound HTTP ingress or a separate Azure Event Hubs namespace.
If tenant/workspace settings require an exception for this topology,
obtain the narrowly scoped approval, record its review/expiry and verify the
actual route. An Azure tag cannot override a Fabric network policy, and a review
date does not automatically revoke an exception.

Before accepting normal consumption and unattended hybrid operation, prove actual
UAMI receipt, owned-envelope validation, durable acceptance before checkpoint,
restart/replica recovery, source correlation, denied operations and terminal
shared-state persistence. Treat missing proof as blocked/incomplete coverage,
not a successful empty stream.

Public references:
[Custom Endpoint destinations](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-destination-custom-app),
[Entra consumption](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/custom-endpoint-entra-id-auth),
[Eventstream Private Link support](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/set-up-tenant-workspace-private-links),
[read-only admin APIs](https://learn.microsoft.com/fabric/admin/enable-service-principal-admin-apis),
and [Admin Items preview](https://learn.microsoft.com/rest/api/fabric/admin/items/list-items).
