# Azure and tenant prerequisites

Complete these checks before [DeploymentGuide.md](DeploymentGuide.md). The
command center is a Python 3.13 App Service API with a Vite UI. It uses an
existing Foundry project and one shared Azure SQL Database for all application
state. The Foundry hosted controller and monitoring worker remain separate
services. The Command Center is the operational UI; the retained read-only
Rayfin cockpit is not a deployment or state prerequisite.

This is a prerequisite guide, not a deployment-acceptance report. The current
SQL contract has independent offline review, and a historical isolated MI
transport canary received the four observed wire event types across manual/scheduled
failure, success and cancellation. That does not establish SQL durable handling,
normal-worker readiness or cutover. The selected baseline now uses public
networking with Entra authentication, not private-network prerequisites.
Scoped evaluation SQL/registry public access has been enabled and read back;
SQL remains Entra-only with TLS/auditing/TDE, and registry admin/anonymous access
is disabled. The public Foundry path is retained. Earlier private-network
proofs and its managed private-endpoint error are historical, not current gates.
SQL recovery is complete through append-only adjudication; the original failed
receipt is unchanged. Proof/application schemas are committed. The initialization
capture of `maintenance=true`, revision `0` is historical: maintenance is now
`false`, Command Center and the hosted controller use Azure SQL
with the correct acting identity. Three runtime EXTERNAL SQL users have kernel
roles and reviewed application grants.
The collector-only worker is deployed and has durably accepted 332 workspace
metadata records; authenticated selectors are enabled. Broader item scanning
remains partial, including source-access 401s and budget/throttling gaps.
Metadata is not source access or remediation authority.
The existing one-minute scheduler was reused for `heartbeat`, with three real
completed recurrences; alerts are portal-only. Event-mode and end-to-end
acceptance remain open. Native Application Insights queries now contain paired
started/completed heartbeat metadata from the app-owned Entra channel, with
zero exporter failure/warning counters in the captured records. Project-wide
content tracing remains disconnected; bounded ingestion is not full hybrid
or overnight proof. See
[release gates](DeploymentGuide.md#release-gates).

## Subscription and tenant context

Select the intended subscription explicitly and confirm its tenant before
provisioning, acquiring service tokens or changing permissions:

```powershell
$subscription = "<subscription-name-or-id>"
$tenantId = "<tenant-id>"
az account set --subscription $subscription
az account show --subscription $subscription --query "{subscription:id,tenant:tenantId}" -o json
```

Repeat the selection before a live command sequence and after reauthentication.
Azure CLI state can be shared by other shells. Fabric is tenant-scoped, not
subscription-scoped: a token from the wrong tenant can return a valid but
unfamiliar workspace list instead of an authentication error. Live operator
commands require an explicitly selected identity, not a developer-credential
fallback. `bi-triage` has global `--sql-identity broker --operator-domain` or
`--sql-identity managed` options. The deployment reset tool additionally supports
explicit Azure CLI subscription selection; each path must match the intended
tenant/principal.

For ARM provisioning, the operator needs permission to create the scoped
resources and to assign their required roles. Contributor does not grant role
assignment rights; use an appropriately scoped Role Based Access Control
Administrator, User Access Administrator or Owner assignment where necessary.
Do not give the application's runtime identity the operator's permissions.

## Foundry

Provide an existing project endpoint and an available model deployment.
Registering prompt agents needs Foundry data-plane access. **Foundry User**
is the developer role; **Foundry Project Manager** also permits project
management. Subscription Owner alone does not supply those data actions.
Invoke-only callers use **Foundry Agent Consumer** at the reviewed project
or narrower supported scope.

The project's managed identity also needs its Foundry role. The portal can
assign this automatically when the creator may assign roles; CLI/IaC creation
must not assume it happened. The roles formerly named Azure AI User, Owner,
Account Owner and Project Manager retain their IDs under the Foundry names.
See [Foundry RBAC](https://learn.microsoft.com/azure/foundry/concepts/rbac-foundry).

Use Entra authentication and disable local authentication. The public
[Foundry template](../infra/foundry.bicep) requires no managed-network injection,
private endpoint or network-approver role. Its optional
[capability-host template](../infra/foundry-capabilities.bicep) is inspect-first,
not a prerequisite for the basic public path. Model availability, model quota and hosted
agent admission must be checked separately. A model catalog entry is not proof
that a deployment or hosted container will be admitted.

## Command-center identity and groups

Use a dedicated single-tenant, secretless SPA/API registration. The browser
uses authorization code with PKCE; the API validates its own delegated
`access_as_user` access token and `CommandCenter.*` app-role claims. App Service
EasyAuth, SQL membership rows and browser-supplied actor fields are not the
authorization authority.

The provisioning operator needs the directory roles and delegated Microsoft
Graph permissions for the operations they choose: application registration,
ordinary security-group creation, ownership/membership setup and app-role
assignment. Additional consent operations require their own consent authority.
The scripts do not grant these privileges to their caller or to the web
user-assigned managed identity (UAMI).

Four ordinary security groups map to Reader, Operator, Approver and Admin.
Group-based enterprise-application assignment requires **Entra ID P1 or P2**;
nested membership does not cascade. The group script checks the selected
administrator's active P1/P2 plan, not every user's licensing entitlement.
Have IT verify the applicable licensing for all assigned users. See
[assign users and groups](https://learn.microsoft.com/entra/identity/enterprise-apps/assign-user-or-group-access-portal).

`scripts\register_command_center.py` preserves existing role IDs and enforces
`appRoleAssignmentRequired=true`. `scripts\configure_command_center_groups.py`
plans offline by default; `--apply` provisions and reads back the groups,
owners, administrator membership and assignments. Group owners and IT manage
membership outside the application. A command-center Admin role grants app
capabilities, not directory administration.

Application assignment and delegated consent are separate requirements.
Assignment-required applications need administrator consent. The registration
script's optional `--grant-admin-consent` grants only the selected user's API
consent, not tenant-wide consent for future group members. Arrange the remaining
users' consent through the organization's approved process.

The optional profile-photo feature requests delegated Graph `User.Read` for the
signed-in user. It is not a directory roster permission or an API app-role grant.
Never grant `AppRoleAssignment.ReadWrite.All` or `Group.ReadWrite.All` to the
web UAMI. See the exact flag meanings and historical authorization-only cutover in
[DeploymentGuide.md](DeploymentGuide.md#10-command-center-entra-authorization).

## Azure SQL application state

Provision **one shared Azure SQL Database** on an Azure SQL logical server.
All application stores use that catalog, including monitoring, incidents,
commands, approvals, collaboration, receipts and action fences. Splitting them
by component would break the shared transaction boundary. An optional temporary
isolated proof database may use the same logical server; it is not an additional
operational store. The shipped template uses one S1 application database and an
optional Basic proof database without an elastic pool. This evaluation topology
is not a final sizing or pricing recommendation.

Configure the logical server's **Microsoft Entra-only authentication** explicitly.
Azure SQL also supports SQL authentication; token-only access is a deployment
setting, not an inherent platform restriction. Configure a server Entra
administrator for deployment operations without creating a SQL password/login
configuration. A Command Center Admin is not the server's Entra administrator.
Runtime identities use Entra tokens and contained database users, not database
item permissions, app-role claims or an Azure RBAC assignment alone.

Set `AZURE_SQL_SERVER=<server>.database.windows.net` and
`AZURE_SQL_DATABASE=<database-name>` from the Azure deployment. Do not obtain
these values from Fabric item properties or a Fabric SQL REST API. The SQL
template enables public networking with TLS 1.2 minimum, Proxy/TCP 1433,
auditing and TDE. No VNet, private endpoint or private DNS is required.

`allowAzureServices` defaults to true and adds the special SQL firewall rule
whose start and end are both `0.0.0.0`. This admits Azure-hosted callers,
including other subscriptions, not the whole Internet. It is not a tenant or
identity allowlist; Entra authentication and SQL permissions remain mandatory.
Optional `clientFirewallRules` specify exact IPv4 ranges. Verify public DNS,
firewall admission and each runtime identity separately.
See [Entra-only authentication](https://learn.microsoft.com/azure/azure-sql/database/authentication-azure-ad-only-authentication),
[SQL firewall rules](https://learn.microsoft.com/azure/azure-sql/database/firewall-configure)
and [SQL auditing](https://learn.microsoft.com/azure/azure-sql/database/auditing-overview).

Separate schema and runtime authority. The deployment operator installs the
application schema and initializes the monitoring maintenance/control baseline.
Runtime API, collector and controller stores require their deployed schema and
fail closed rather than creating it or substituting local JSON. Give them only
their reviewed `web`, `worker` or `controller` checked-view/static-RPC permissions,
never DDL to repair startup. Explicit live component selection is required but
does not grant SQL rights. The old `runtime_table_permissions()` broad-DML
surface is retired; unfinished bindings cannot be repaired by granting direct
control, source/head, action or receipt-table writes.

The isolated Azure SQL target is provisioned, and earlier bootstrap-MI login
and authority checks remain bounded evidence. Runtime role and updatable-view proof still requires
separate connections authenticated as the actual component managed identities.
Azure SQL supports `CREATE USER ... WITHOUT LOGIN` and `EXECUTE AS USER` for
database-scoped tests, but these do not prove managed-identity authentication,
network/firewall admission or reconnect behavior. A successful CREATE, `SELECT 1` or
offline parser run is not permission/transaction proof. Earlier Fabric SQL
CREATE/rollback results are historical, not Azure SQL acceptance.

For an Entra service principal or UAMI, encode the Azure SQL user's SID from its
**client ID** as little-endian GUID bytes; Azure RBAC uses the **principal object
ID**. Users and groups use their object IDs for SQL. Verify the actual Azure SQL
principal SID and sign-in under the deployed identity before accepting grants;
the explicit SID form skips directory name lookup, not identity verification.
See [CREATE USER](https://learn.microsoft.com/sql/t-sql/statements/create-user-transact-sql#arguments).

## Fabric and workload access

Fabric capacity and workspace/item permissions are still required for monitored
Fabric pipelines and native Eventstream transport. Power BI semantic-model
refresh monitoring retains its own service permissions. None of these services
moves into Azure SQL, and Fabric capacity does not provision or limit the
application-state database.

Keep these permission planes separate:

| Operation | Identity/access to verify |
|---|---|
| Tenant workspace/domain/item inventory | Collector identity and applicable read-admin service-principal/MI tenant settings; Admin Items preview is explicit |
| Job/refresh evidence | Source item/workspace access for the actual collector; Power BI refresh-history reads require semantic-model Write |
| Eventstream consumption | Selected worker UAMI and the documented stream-workspace access, independently of source access |
| Workload remediation | Controller identity, current action capability/review and required source service permissions |
| Monitoring setup and human decisions | Validated command-center Entra app roles, not SQL memberships or service grants |

A domain is metadata, not permission. Core listings are caller-visible, not
tenant-complete; administrative inventory is not operational telemetry.
Power BI scanning does not replace refresh-history reads. Verify every intended
operation under the deployed identity rather than relying on the operator's
successful request. See
[read-only admin APIs](https://learn.microsoft.com/fabric/admin/enable-service-principal-admin-apis),
[Admin Items preview](https://learn.microsoft.com/rest/api/fabric/admin/items/list-items)
and [refresh-history permissions](https://learn.microsoft.com/rest/api/power-bi/datasets/get-refresh-history-in-group).

Targets, exclusions, cadence and safety reviews live in the monitoring registry.
`MONITORING_MODE=live` requires the pinned tenant and SQL control; explicit
`fixture` mode is offline, never a live fallback. `FABRIC_PIPELINE_TARGETS`
and compatibility loaders are retired. Default admission is observation-only.
Notebook failures within pipeline activity evidence do not establish standalone
notebook monitoring.

## Monitoring worker and clean-start prerequisites

Provision the dedicated environment before requiring a worker image or
Eventstream endpoint. `scripts\deploy_monitoring_worker.ps1 -EnvironmentOnly`
creates a public Consumption environment using the selected logging workspace,
without creating a worker or requiring a VNet/subnet/NAT. Logging uses keyless
Azure Monitor routing. Its default `-Mode Prepare` is local preparation; cloud validation
and execution are separate explicit modes with an isolated Azure CLI profile.

Use a dedicated worker UAMI. The
[monitoring prerequisites](../infra/monitoring-prerequisites.bicep) create only
that UAMI, a public Basic ACR and scoped `AcrPull`; registry admin/anonymous
access is disabled and Entra ARM authentication remains enabled.
Build/publish the Linux image separately and use an immutable ACR digest with
MI image pull. The worker still has no ingress. A finite
owned canary can create/inspect an Eventstream without SQL or endpoint settings.
Keep creation/inspection, isolated MI reception, SQL durable acceptance and
normal-worker readiness as distinct gates.

For initial discovery/polling, use `-CollectorOnly`, without an Eventstream
bootstrap file. The Bicep default is `collectorOnly=true`; the helper requires
`-CollectorOnly` or `-ConnectorBootstrapFile`, exclusively. Collector-only mode
rejects partial event settings, creates no receiver/provisioner and cannot claim
event transport in its heartbeat. See the
[customer quickstart](DeploymentGuide.md#collector-only-quickstart).
Use `caller_visible` inventory by default; `tenant_admin_preview` is an explicit
authorized API choice, not a permission grant.

The Custom Endpoint's key-returning connection API must not be called.
Obtain only namespace, entity, consumer group and owned IDs from the
**Microsoft Entra ID** tab and reviewed topology, then bind them to shared
connector state. Manual nonsecret endpoint bootstrap remains required.
Key-free automatic endpoint discovery is not a proved deployment capability.

For an already-created app-owned Eventstream, use the
[metadata registrar](DeploymentGuide.md#register-existing-app-owned-connector-metadata)
with the original creation evidence and fresh complete readbacks. This
SQL-connected operator step establishes planned physical ownership only; it
does not grant scope admission, event capability, desired publication or Ready.
Fresh same-collector-MI probes, current admitted scope, first controller desired
publication and an actual original durable stream receipt are separate gates.

For the approved prototype clean start, provision the Azure SQL application
schema and initialize empty maintenance/control/receipt state through the
deployer. Do not import earlier Fabric SQL history or target settings. Old
prototype-state disposal and any reset of an already initialized Azure SQL
target require the exact manifest/epoch and fresh live writer/effect evidence.
The current reset tooling targets Azure SQL only; it is not a legacy-store
loader or cross-platform cleanup tool. The same authorized operator may prepare
and execute; no second signer or new key workflow is required. Stop all old writers
and reconcile uncertain effects before the wipe. Keep original receipts after
lost acknowledgements; do not reset rows created by the new release.
Protected writer registration must join current SQL authority to independently
observed deployment/identity bindings; a supplied profile is not the complete
inventory. Registration/capture evidence and rate budgets are preserved across
the operational reset. No old history or target configuration is imported.

Normal worker startup needs ready shared control and compatible event
persistence. A future authorized reset leaves maintenance enabled until its
reviewed release transition; this does not describe the current, released
maintenance state or authorize reversing the completed web/controller cutover.
The disabled-by-default scheduler still needs separate verification before
unattended use. See
[DeploymentGuide.md](DeploymentGuide.md#3a-hybrid-registry-and-controlled-prototype-reset).

## Optional Microsoft 365 integrations

A mailbox and Exchange administrator are needed only for mailbox ingestion.
App-only mailbox access must be scoped to the intended mailbox and verified
with a denied canary read before enabling its schedule. The older hosted mail
path encountered Exchange rejection of an Entra agent identity; do not assume
that a Graph directory token proves mailbox access.

The source retains legacy mailbox-secret, Teams webhook and bearer-link
callback configuration. These are **not prerequisites for the secretless
command-center deployment**. Keep those integrations unconfigured unless a
separately approved authentication path has been verified. Web notifications
and web approvals need neither Teams nor the legacy callback. See
[DeploymentGuide.md](DeploymentGuide.md#2-optional-mailbox-ingestion).

## Network, quota and cost

Hosted telemetry has its own boundary. Use the app-owned
`TRIAGE_TELEMETRY_CONNECTION_STRING` locator and managed identity; the standard
platform variable is injected from Foundry project monitoring, not an agent
override. Keep the project tracing connection absent because linking it enables
project-wide prompt/content traces. The separate
[application telemetry template](../infra/application-telemetry.bicep) provisions
public Entra-only Insights against an existing Log Analytics workspace, with
optional resource-scoped publisher assignments and resource-ID-only output.
No project connection or ordinary-customer MCAPS exemption is added.
See [setup, privacy sources and unproved ingestion](DeploymentGuide.md#8-observability).

All shipped Bicep uses public networking without PE/VNet/NAT/private-DNS
prerequisites. `infra\command-center.bicep` uses public HTTPS, Entra API roles,
managed identity and Entra-authenticated SCM; basic publishing and FTPS are
disabled. Optional `publicAccessClientCidrs` and `scmAccessClientCidrs` are
independent persistent network filters. Empty lists keep the network public,
not the protected API or publishing operations anonymous.

Public web reachability does not prove SQL firewall admission, Foundry
inference or Fabric item access. Verify each with its actual runtime identity.
Optional private hardening is a separate design; retained private test resources
do not become prerequisites for this public baseline. See
[App Service access restrictions](https://learn.microsoft.com/azure/app-service/app-service-ip-restrictions)
and [optional Fabric private-link scope](TechnicalArchitecture.md#optional-fabric-private-link-scopes).

Ordinary SQL/account exception-tag maps default to `{}`; registry exception
tags are optional/nullable. Apply `sqlNetworkExceptionTags` only to the SQL
server, `registryExceptionTags` only to the selected registry and
`accountNetworkExceptionTags` only to a specifically approved Foundry account.
For the MCAPS evaluation, the SQL `SecurityControl=Ignore` exception plus
reason/review tags permits one 14-day period. Removing/re-adding the tag does
not reset that period; longer tests need an approved exclusion. Do not copy
the exception onto a whole resource group. See
[governed evaluation exceptions](DeploymentGuide.md#governed-evaluation-exceptions).

The selected Eventstream Custom Endpoint does **not** support tenant/workspace
Private Link. The worker uses public outbound TLS with Entra and no inbound
HTTP/TCP ingress. It requires no dedicated NAT or separate Azure Event Hubs
namespace. Public transport does not bypass API/SQL/model authorization.
Where the tenant/workspace requires an approved public exception, record its
exact scope, owner and review/expiry operation. Azure tags cannot override Fabric
network controls. See
[Custom Endpoint destinations](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/add-destination-custom-app)
and [Eventstream Private Link support](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/set-up-tenant-workspace-private-links).

Before provisioning, verify the regional App Service SKU quota and admission,
Python 3.13 availability, Azure SQL regional quota/admission, Fabric workload
capacity and Foundry model/hosting limits.
The helper supports Basic, Standard and selected Premium v3 SKUs; B1 being the
default does not prove it has capacity in your subscription.

Budget for the App Service plan, public Basic registry, monitoring worker,
SQL compute/storage/backups/auditing, Fabric workload capacity, hosted controller,
model inference, data transfer, schedules and telemetry. These are
not all request-priced or free while idle. Apply
`CostCenter`, `Owner`, `Environment` and `DataClassification` tags to the
resource group and resources.

The baseline adds no NAT/private-endpoint charges. Private resources retained
from the earlier evaluation have not been deleted and still need separate
ownership/cost review.

Governance may stop or resize resources overnight. Use the documented,
approved cost-control exemption only when needed for unattended operation.
`-EvaluationCostExemption` requires `-Evaluation` and a future
`-EvaluationExpiresOn`; its `CostControl=Ignore` and `EvaluationExpiresOn`
tags are **not shutdown timers**. Assign a finite review/cleanup date, disable
validation and remove the exemption at evaluation end.

For cleanup, inventory ownership first. Stop only the evaluation's schedules
and remove only its resources. Do not use a blanket resource-group deletion or
`azd down` as a substitute for reviewing the shared Foundry project, standalone
database and retained operational data.
