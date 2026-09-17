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
evaluation access has been enabled and read back. That network change does not
complete SQL recovery or cut over the live application.

### Release gates

| Gate | Current evidence/status | Not established |
|---|---|---|
| SQL ownership contract | Independently reviewed offline, including the current correction set | Native role evaluation, checked-view updatability or MI procedure execution |
| Event transport | A historical isolated MI canary received all four observed wire event types across manual failure, scheduled failure, success and cancellation | Azure SQL durable handling, normal-worker recovery or application cutover |
| Live source integration | Final component-store, controller and connector acceptance, including source acknowledgement and work completion, remains required | A test-only fixture or passing local scenario is not the live path |
| Data-quality flag persistence | Source now selects `AzureSqlFlagTable` for live runs and CSV only for fixtures | Native Azure SQL table/grant/identity and persistence proof remain outstanding; source closure is not deployment acceptance |
| Public SQL/registry access | Approved resource-scoped evaluation exceptions and public access were read back on 2026-09-17. SQL remains Entra-only with TLS 1.2 and the special Azure-services firewall rule; registry access is public with default Allow, bypass None and admin/anonymous access disabled | Network access does not establish SQL schema, runtime permissions, application acceptance or an indefinite governance exemption |
| Earlier private image proof | A historical cold pull succeeded with registry public access Disabled, default Deny and bypass None; the reviewed shipping contents, private DNS and native imports matched | Not proof of the current public-network source bundle, a new application release or runtime recovery |
| Native schema and bootstrap | The original `STARTED` proof receipt is under guarded recovery. Earlier full-schema DDL rollback and subsequent exact module-metadata checks remain bounded evidence | Recovery completion, full-catalogue bootstrap commit, runtime SQL users/memberships, allowed/denied operations and effect proof are not accepted |
| Public Foundry path | The retained Foundry account/controller path is public with local authentication disabled; `infra/foundry.bicep` implements that baseline | The earlier managed private-endpoint preflight failure is historical and does not block the selected public architecture |
| Current-release cutover | Not performed; the live app/controller remain the prior release | No accepted application baseline/runtime-permission proof, history migration/wipe, normal-worker rollout, hybrid application push or current-release UI screenshots |

Keep the worker and controller heartbeat disabled until the current release
passes its gates. The evaluation now has one Azure SQL application database and
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

The original SQL proof receipt is under guarded recovery; completion is not
accepted. The following records the **earlier private-network proof**, not the
current public deployment. Its image matched the reviewed
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
It did not resolve the original receipt or establish a committed application
schema, runtime permissions or safe retry. Current receipt recovery remains a
separate gate; do not substitute a fresh apply or a new operation ID for it.

The prepared SQL surfaces are [state infrastructure](../infra/state-sql.bicep),
[identity/manual bootstrap job](../infra/state-sql-bootstrap.bicep) and the
[bootstrap operator](../scripts/bootstrap_azure_sql.py). The operator defaults
to preflight; apply requires an explicit reviewed fingerprint. Source availability
does not establish bootstrap completion.

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
only their component's checked-view/static-RPC permissions.
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
on the native Azure SQL target. Earlier bootstrap-MI login and authority checks
are bounded historical evidence; runtime-component users, memberships and
permission/effect proof remain outstanding. Do not generalize an observed equality of agent object/client IDs
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

These are prepared operator procedures for Azure SQL only. The application and
isolated proof databases are provisioned, with public evaluation access enabled.
The original proof receipt is under guarded recovery; completion and a current
application baseline are not accepted here. The earlier rolled-back proof
journal was not a monitoring baseline. The [release gates](#release-gates),
receipt resolution, committed native bootstrap and exact reset approval must
precede execution. No history migration or wipe has occurred.

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

Install the reviewed permission kernel with the explicit deployer helper
`initialize_monitoring_permission_kernel`; runtime stores never call it.
Inspect the separate registration DDL without connecting to SQL:

```powershell
.\.venv\Scripts\python.exe scripts\register_monitoring_writers.py --ddl
```

Registration preparation can report old broad roles and running writers, but
such a preparation is not protected acceptance. Retire only the reviewed legacy
grants, stop writers and reconcile effects before accepting a fresh registration.
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

For example, after its prerequisites are ready:

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

The caller timeout is `PT15M`, covering the default combined triage and
approval budgets plus worker allowance (`300 + 300 + 30` seconds). `PT10M`
covered the approval window alone but not the combined deadline. A shorter
caller timeout can abandon work while a person's approval is still valid.
HTTP invocation retries are disabled: an ambiguous POST must not replay a
possibly executed action.

The workflow records failed invocations/invalid responses as `SweepFailed`.
Its run history is not an alerting channel. The optional `alertWebhookUrl`
is a legacy bearer-URL integration; use an approved alerting route without
making it a prerequisite for the web deployment. Review both schedule history
and durable triage outcomes: a transport-completed response is not necessarily
a healthy business result.

Live pipeline eligibility and cadence come from the monitoring registry, not
`PIPELINE_SWEEP_ENABLED` or `FABRIC_PIPELINE_TARGETS`. Discovery may identify
unsupported items, but only admitted workload targets are polled and dispatched.
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
and the tool returns the actually stored redacted flag. These are implemented
source behaviors, not native deployment proof. Azure SQL table installation,
controller grants, identity access and cross-instance/restart persistence
remain release gates.

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

The existing `configure_telemetry` helper is optional and consumes
`APPLICATIONINSIGHTS_CONNECTION_STRING`; without it telemetry is a no-op.
It does not explicitly pass an Entra credential to the exporter. The
command-center deployment helper rejects connection-string settings, and its
`-ApplicationInsightsResourceId` only adds a portal link. **Neither setting
proves secretless telemetry is configured.** An Entra-authorized exporter needs
separate implementation/configuration and proof; do not generate a key or claim
the portal link does this. Private ingestion would be optional additional
hardening, not a prerequisite for the public baseline.

When Entra-authenticated telemetry is configured, grant only the needed
monitoring role, such as Monitoring Metrics Publisher, to the exporting
identity. Preserve governance-created diagnostic settings; add separate
diagnostics rather than deleting them. Do not disable content filters or
Defender to make an evaluation succeed.

Use `azd ai agent monitor bi-triage-controller --tail 300` for hosted
diagnostics. The default 50 lines can hide the error behind SDK output; 300 is
the observed CLI maximum. Do not infer log delivery from a few visible spans.
Keep the hosting library pinned to the exact checked-in version: floating
date-stamped betas previously broke container startup.

## 9. Foundry hosted controller deployment

Run this procedure only after the [release gates](#release-gates) and approved
cutover. It has not been performed for the current hybrid release.
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

The normal worker entry point is `python -m triage.monitoring.worker`; a
current-release worker has not been deployed. Its contract is to collect raw
evidence, durably accept intake and reconcile app-owned monitoring topology.
The controller separately publishes validated authority. The worker must not
run a reasoning agent, refresh a semantic model, rerun a business pipeline,
grant permissions or publish readiness from its own observations.

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
UAMI, ACR resource ID, immutable image digest, SQL hostname/catalog and connector
bootstrap file. The helper neither builds/pushes an image nor grants its external
permissions. Build `Dockerfile.monitoring` for `linux/amd64` through the approved
build path; ACR admin authentication stays disabled and image pull uses the
selected UAMI.

The worker has no HTTP/TCP ingress or health endpoint. Its template uses one
container, single-revision mode and bounded replicas, not backlog autoscaling.
Verify MI image pull, public DNS, outbound TLS/SQL firewall admission, quotas and measured
resource use; ARM success is not worker or hybrid acceptance.

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

The full-worker helper expects `ConnectorBootstrapFile` to contain exactly:

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
readiness separate. The current adapter/controller/worker wiring is still being
completed; a passing fixture does not establish that the normal deployed path
performs these steps.

### Worker configuration and checks

The deployment template supplies these exact nonsecret settings:

| Group | Settings |
|---|---|
| Tenant and mode | `MONITORING_MODE=live`, `MONITORING_TENANT_ID`, `AZURE_TENANT_ID` |
| Inventory selection | `MONITORING_INVENTORY_MODE=caller_visible` by default; explicit `tenant_admin_preview` opt-in |
| Pinned UAMI | `AZURE_SUBSCRIPTION_ID`, `AZURE_CLIENT_ID`, `MONITORING_IDENTITY_OBJECT_ID`, `MONITORING_IDENTITY_RESOURCE_ID` |
| Durable state | `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`; the same application catalog as web/controller |
| Owned connector | `MONITORING_CONNECTOR_ID`, `MONITORING_EVENTSTREAM_WORKSPACE_ID`, `MONITORING_EVENTSTREAM_ID`, `MONITORING_EVENTSTREAM_DESTINATION_ID` |
| Entra endpoint | `MONITORING_EVENTSTREAM_NAMESPACE`, `MONITORING_EVENTSTREAM_ENTITY`, `MONITORING_EVENTSTREAM_CONSUMER_GROUP` |

The worker reads its process environment, not `.env`. It rejects fixture mode and
credential-bearing environment settings. Normal startup requires ready shared
control, the collector and the store's `EventPersistence` operations; it never
falls back to an in-memory store or transport-only probe.

With the required process environment supplied, these are separate operations:

```powershell
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
Normal `python -m triage.monitoring.worker` starts the long-running consumer
and maintenance loops.

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

Before enabling normal consumption and the controller heartbeat, prove actual
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
