# Azure and tenant prerequisites

Complete these checks before [DeploymentGuide.md](DeploymentGuide.md). The
command center is a Python 3.13 App Service API with a Vite UI. It uses an
existing Foundry project and a standalone Fabric SQL Database; it does not
replace the separate Foundry hosted controller or the read-only Rayfin cockpit.
The database is not owned by either web application.

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
unfamiliar workspace list instead of an authentication error. Local
`DefaultAzureCredential` can also use the selected CLI identity.

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

Use Entra authentication, disable local authentication and plan the required
Foundry managed network isolation. Model availability, model quota and hosted
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
web UAMI. See the exact flag meanings and existing-deployment cutover in
[DeploymentGuide.md](DeploymentGuide.md#10-command-center-entra-authorization).

## Fabric and workload access

Provide a Fabric workspace on a capacity and a standalone **Fabric SQL
Database**. Runtime authentication uses Entra tokens, not SQL passwords or
connection secrets. Item access and SQL object permissions are separate
controls. The web UAMI needs the database's Read item permission plus the
object-scoped SQL grants in the deployment guide; broad workspace or database
roles can defeat that boundary.

For a service principal or UAMI, the Fabric SQL user's SID is its **client ID**
converted to little-endian GUID bytes, not its directory object ID. Users and
groups use their object IDs. See
[Fabric SQL authentication](https://learn.microsoft.com/fabric/database/sql/authentication).

Grant Power BI/Fabric workload access to the controller that executes tools,
not to the prompt agents or web UI. Enable the relevant tenant settings for
service-principal APIs and, when used, semantic-model execute queries.
Scheduled Fabric pipeline monitoring inspects only explicitly configured
pipeline targets. It does not discover arbitrary targets or monitor standalone
notebook jobs.

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

`infra\command-center.bicep` is **private by default**: an App Service private
endpoint and private DNS for app/SCM ingress, a separate integration subnet,
explicit NAT egress, and basic publishing authentication disabled. Preserve
governance-attached NSGs. Verify the actual site property
`outboundVnetRouting.allTraffic=true`; the old inline route-all setting was not
reliable in provisioning.

Private web ingress does not establish private connectivity to Foundry or
Fabric. Arrange backend DNS, routes, peering and the supported Fabric Private
Link scope separately, and verify from the runtime. Fabric Private Link
protects inbound Fabric access, not Fabric-to-source egress. See
[App Service private endpoints](https://learn.microsoft.com/azure/app-service/overview-private-endpoint)
and [Fabric Private Link](https://learn.microsoft.com/fabric/security/security-private-links-overview).

Before provisioning, verify the regional App Service SKU quota and admission,
Python 3.13 availability, Fabric capacity and Foundry model/hosting limits.
The helper supports Basic, Standard and selected Premium v3 SKUs; B1 being the
default does not prove it has capacity in your subscription.

Budget for the App Service plan, NAT Gateway and public egress IP, Private
Link, Fabric capacity, hosted controller, model inference, schedules and
telemetry. These are not all request-priced or free while idle. Apply
`CostCenter`, `Owner`, `Environment` and `DataClassification` tags to the
resource group and resources.

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
