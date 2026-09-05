# Azure account setup

What must be true of your Azure subscription and Microsoft 365 tenant before you
start [`DeploymentGuide.md`](DeploymentGuide.md).

## Azure subscription

An [Azure subscription](https://azure.microsoft.com/free/) where you hold:

- **Contributor** at subscription level, to create the resource group and its
  resources
- **Role Based Access Control Administrator** (or **User Access Administrator**,
  or **Owner**), to assign roles. Several components authenticate by managed
  identity or agent identity, and each needs a role assignment. Without this you
  can create the resources and none of them will be able to reach each other.
- **Foundry Project Manager** (or **Foundry Owner**) on the Foundry *account*.
  Subscription Owner is not enough: Foundry agents are a data-plane resource,
  and the roles beginning `Cognitive Services` do not grant access to them even
  though they carry `Microsoft.CognitiveServices/*` data actions. A project
  created in the Foundry portal grants this to its creator automatically; one
  created with the CLI or from IaC does not. See
  [`DeploymentGuide.md`](DeploymentGuide.md) section 4.
- permission to create **app registrations** in Entra ID, or someone who can
  create one for you. Exactly one is required, for mailbox access.

## Microsoft Fabric

- A **Fabric workspace on a capacity** (trial or an F SKU), holding a **Fabric
  SQL Database** for durable state. The identity the controller runs as needs a
  workspace role and a database user — see
  [`DeploymentGuide.md`](DeploymentGuide.md) section 3.
- There is no connection secret to manage. Fabric SQL accepts Microsoft Entra
  tokens only.

## Microsoft 365 tenant

- A **mailbox** that receives Power BI failure alerts. A shared mailbox is fine
  and is the usual choice.
- **Exchange Online administrator** access, or an administrator willing to run
  one command. App-only `Mail.Read` is tenant-wide until it is scoped by an
  Exchange `ApplicationAccessPolicy`, so this is not optional — see
  [`DeploymentGuide.md`](DeploymentGuide.md) section 2.
- A **Power BI tenant setting** allowing service principals to use the Power BI
  APIs, and workspace access for the identity the controller runs as. This is the
  item with the longest lead time in most organizations, because it is usually
  owned by a different team.
- Optionally, a **Teams channel** for notifications.

## Regions and model availability

The accelerator deploys agents into an Azure AI Foundry project and keeps its
state in a Microsoft Fabric SQL Database. It does not create the project, the
database or Application Insights — see
[`DeploymentGuide.md`](DeploymentGuide.md) for what must exist first. Not every
model is available in every region, and quota is per region and per model. Check
availability before choosing a region:

```powershell
az cognitiveservices account list-models `
  --name <foundry-account> --resource-group <rg> `
  --query "[].{model:name, version:version, sku:skus[0].name}" -o table
```

If deployment fails with a quota error, request an increase for that model in
that region, or pick a different region. See
[Azure AI Foundry quotas and limits](https://learn.microsoft.com/azure/ai-foundry/quotas-limits).

## Cost control

Everything here is pay-as-you-go. The recurring cost while idle is small — table
storage and a Logic App that fires on a schedule — and the variable cost is model
inference, which scales with how many alerts arrive.

Delete the resource group or run `azd down` when you are finished evaluating.

If your tenant applies governance automation that stops or resizes idle
resources overnight, expect it to act on this deployment too, and tag the
resource group according to your organization's exemption process if it needs to
keep running unattended.
