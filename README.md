# BI Triage Solution Accelerator

<img src="./command-center/public/triage-logo.png" alt="BI Triage logo" width="44" height="52" />

The BI Triage Solution Accelerator is a multi-agent operations loop for business
intelligence failures. For a refresh failure on an admitted Power BI target, it
gathers evidence, consults a specialist agent, and either remediates within
controller-enforced policy or escalates with the evidence attached. Deterministic probes also detect
configured freshness, row-count and schema regressions when no failure alert
arrives.

The hybrid monitoring source combines UI-managed inventory and REST polling
with native Fabric Job events delivered through an Eventstream custom endpoint
to an outbound managed-identity worker. Power BI semantic-model refresh history and
scheduled Fabric Data Factory pipeline jobs have separate workload adapters.
Pipeline reruns require reviewed replay safety and human approval; an accepted
job is not a verified repair. Standalone notebook monitoring is not implemented.
See [monitoring scope and limits](#hybrid-monitoring-scope-and-limits).

The [command-center source](./docs/CommandCenter.md) provides **Monitoring
setup**, a queue and inspector, approvals, queued investigations and full run
history. Incident records support append-only notes, persisted tool-free
discussion and **Resolved by user** tracking. Human closure is not a verified
repair and does not reset the controller's remediation budget. **All application
state uses one shared Azure SQL Database**, independent of the UI. The Command
Center is the operational UI; the retained read-only Rayfin cockpit is not a
deployment or state dependency. Power BI and Fabric remain the monitored
services, including native Fabric Eventstream delivery.

> [!IMPORTANT]
> SQL contract review and source integration are not deployment acceptance.
> Earlier Fabric SQL CREATE/rollback checks and an isolated managed-identity
> event canary are historical evidence, not proof of the Azure SQL target.
> The canary received all four observed wire event types across manual failure,
> scheduled failure, success and cancellation; it did not prove durable SQL
> handling or normal-worker recovery.
>
> The selected baseline is **public networking with Entra authentication**.
> Shipped Bicep requires no private endpoint, VNet, NAT Gateway or private DNS.
> Scoped evaluation SQL/registry public access has been enabled and read back;
> SQL remains Entra-only with auditing/TDE, and registry admin/anonymous access
> stays disabled. The public Foundry path is retained with local auth disabled.
>
> The original SQL proof receipt is under guarded recovery; completion,
> full-schema bootstrap, runtime permissions, worker recovery and cutover are
> not accepted. Earlier private image/network proofs remain historical. The
> private Foundry preflight error does not block this public architecture.
> The live app/controller remain the prior release; no history migration/wipe,
> normal-worker rollout, hybrid application push or current-release UI
> screenshots are complete.
> Follow the [release gates](./docs/DeploymentGuide.md#release-gates) and the
> [approved implementation and acceptance plan](./docs/HybridMonitoringPlan.md).

**Key use cases and customization:**

- **Power BI operations use case**: A semantic model refresh fails at 06:00. The
  accelerator triages the cause, applies an allowlisted fix or requests approval
  for an allowlisted gated action, deduplicates repeat occurrences, and records
  a terminal outcome for every run. Approval never permits an off-allowlist action.
- **Reusability and customization**: The policy ledger, action allowlists,
  approval gate and typed evidence provide patterns for other domains; tools and
  evidence checks remain workload-specific. See
  [How to customize](#how-to-customize).

<br/>

<div align="center">

[**SOLUTION OVERVIEW**](#solution-overview) \| [**QUICK DEPLOY**](#quick-deploy) \| [**BUSINESS SCENARIO**](#business-use-case) \| [**SUPPORTING DOCUMENTATION**](#supporting-documentation)

</div>
<br/>

<h2 id="solution-overview">Solution overview</h2>

This solution accelerator uses Azure AI Foundry, Azure SQL Database, Power BI,
Microsoft Fabric and optional Microsoft Graph mailbox ingestion. A controller
orchestrates the loop, a data quality agent investigates data-shaped failures,
and every agent-proposed tool call is checked against an allowlist enforced in code.

It runs **fully offline** with mock providers and mock tools, so you can read it,
run it and evaluate its behaviour before it touches a tenant. That is also how
the test suite runs: no credentials, no network.

**Prepare the Foundry project, Azure SQL state and monitored Fabric workspaces
separately.** Before starting any runtime, provision the shared application
database, public firewall admission, Entra-only authentication, auditing and
reviewed schema/permissions. No private-network infrastructure is required by
the shipped templates. Publishing an agent or web build does not establish those
prerequisites or provision Application Insights.
[`docs/DeploymentGuide.md`](./docs/DeploymentGuide.md) lists every
prerequisite in the order its lead time demands, and
[`docs/AzureAccountSetUp.md`](./docs/AzureAccountSetUp.md) covers the
subscription and tenant permissions you need first.

### Solution architecture

The diagram below illustrates the earlier release's agent and mailbox paths;
it does not show the new hybrid intake topology or prove its deployment.
For the current boundaries, see the
[architecture description](./docs/TechnicalArchitecture.md) and
[hybrid monitoring plan](./docs/HybridMonitoringPlan.md).

| ![Solution architecture](./docs/images/readme/solution-architecture.png) |
| ------------------------------------------------------------------------ |

The hybrid source path is:

```text
Command Center scope/review requests -> durable web intents (pending)
REST inventory/polling and native Job events -> worker observations + receipts
Controller heartbeat -> deterministic reconcile_state (no agent or action)
  -> published registry/source/connector authority
Controller heartbeat -> eligible source work and human-command queues
  -> current admission, policy, approval and atomic action reservation
  -> exact execution/configuration verification
```

The worker collects evidence and reconciles only app-owned monitoring topology.
Worker observations and web intents do not publish admission, source heads or
action authority. Deterministic controller reconciliation publishes validated
state before actionable triage. Live stores select an explicit `worker`, `web`
or `controller` component and use checked views and static SQL RPCs, not broad
monitoring-table DML. RPC success comes from the typed result, never EXEC rowcount.

**Preview notice:** Some platform capabilities used in this solution are
currently in preview, including Foundry hosted agents, Entra agent identity and
Foundry routines. These features are provided "as-is" and may change without
notice. Foundry routines did not fire in the recorded tenant verification, so
the supported scheduler is a Logic App — see
[Scheduling](#scheduling-what-does-and-does-not-work) below.

### How to customize

1. Read [`docs/TechnicalArchitecture.md`](./docs/TechnicalArchitecture.md) to see
   where decisions are made. The short version: the controller decides, the
   agents advise, and policy is enforced in code rather than in a prompt.
2. Review [`src/triage/tools/registry.py`](./src/triage/tools/registry.py). Every
   tool the agents can call is declared there, and every action is on one of
   three allowlists. Anything not on a list is refused before dispatch.
3. Review [`src/triage/policy.py`](./src/triage/policy.py) for the budgets and
   limits, and set yours. A limit that exists only as prompt wording is not a
   limit.
4. Review [`src/triage/knowledge/playbooks.py`](./src/triage/knowledge/playbooks.py)
   and replace the playbooks with the failure modes your estate actually has.
5. Review the scenarios in [`scenarios/`](./scenarios) — each is a reproducible
   end-to-end case with an `expect` block that acts as its test — and the sample
   data in [`mock/`](./mock).
6. Work through [`docs/CustomizationGuide.md`](./docs/CustomizationGuide.md),
   which covers adding a tool, an approval gate, a playbook, a detector, an
   agent, and moving the whole thing to a different domain.

## Features

<details>
  <summary>Feature details</summary>

- **Triage controller** <br/>Orchestrates the loop: parses the alert, gathers
  evidence, calls the specialist agent, decides an action, and persists a
  terminal outcome for every incident including crashes and refusals.

- **Data quality agent** <br/>Investigates data-shaped failures — currently
  duplicate keys on a declared grain — and reports typed findings. It reports;
  the controller decides. Row-collapse and schema drift are found by the
  silent-failure detector below, which is deterministic rather than model-driven.

- **Policy ledger** <br/>Turn, tool-call, token, write-action and wall-clock
  budgets, shared across all agents in a run rather than per agent. Enforced in
  code, with a test proving each limit fires.

- **Action allowlists** <br/>Every tool is on a remediation, reporting or
  diagnostic list. Anything else is refused before dispatch. This is the property
  the whole design rests on.

- **Human approval gate** <br/>Tier 2 actions require an explicit,
  fingerprint-matched, unexpired, unused approval. Timeouts, errors, malformed
  replies and "no gate configured" are all treated as a refusal. Silence is never
  consent, and a denial does not consume the remediation budget. An approval
  decides whether a *permitted* action runs; it never authorises one that is off
  the allowlist.

- **Silent-failure detector** <br/>Finds models that failed without telling
  anyone: a refresh that reports success while the source never landed, or a
  table that loads a tenth of its rows. Deterministic by design — it uses none of
  the agent tools, because a model asked whether a 60% row drop is acceptable
  will sometimes say yes.

- **Scheduled pipeline triage** <br/>Reads failed scheduled Fabric pipeline jobs
  and activity diagnostics, deduplicates run IDs, and separates pipeline tools
  from dataset tools. A durable submission journal prevents repeating an
  approved rerun after an ambiguous response.

- **Hybrid monitoring setup** <br/>An Admin previews and submits versioned tenant,
  domain, workspace or item scopes, with exclusions, supported-workload filters,
  polling cadence and explicit future detection-only enrollment. Readers can
  inspect coverage and existing safety reviews. Configuration, verified service
  access, current observation and action authority are reported separately.
  An accepted/configuring intent remains pending until controller publication.

- **Evidence and redaction** <br/>Deterministic evidence outranks model output,
  and disagreements are logged. Redaction happens inside the store boundary, so a
  call site cannot forget it.

- **Agent command center** <br/>An Azure-hosted operator interface
  (`command-center/`) for monitoring configuration, authenticated approvals, queued investigations,
  full run history, incident notes and read-only questions. Four Entra app roles
  control access; IT and group owners manage the mapped security groups in
  Entra. The **Access & permissions** page displays effective token roles, not
  a membership roster, and cannot edit access. See
  [setup and operation](./docs/CommandCenter.md).

- **Read-only cockpit sample** <br/>The retained Fabric App (`cockpit/`) illustrates
  incidents, approvals, deferred retries, baselines and claims through a semantic
  model. Its earlier-release Direct Lake binding is not a verified Azure SQL
  read path. It introduces no operational store and is not required to deploy
  or operate the Command Center.

</details>

### Hybrid monitoring scope and limits

Discovery is an inventory of resources, not all Fabric operational telemetry.
Only semantic models/datasets and Data Pipelines have failure-monitoring
contracts in this implementation. Other discovered types, such as Notebook,
Report, Lakehouse and Warehouse, stay visible with an unsupported reason.
Notebook activities can be evidence within a monitored pipeline; standalone
notebook jobs are not monitored. Missing expected starts, disabled pipeline
schedules, tenant-wide data quality, report usage, audit and capacity monitoring
need additional detectors or collectors.

Scopes use the deployment tenant and named, ID-backed domain/workspace/item
metadata. Domain descendants are optional; exclusions win. A domain is not an
access grant. Caller-visible inventory and incomplete scans must not be
presented as complete tenant coverage. An inventory refresh queues work; it
does not prove discovery, source access or delivery completed.

An Admin's target safety review configures one action profile. It is not an
Approver's fingerprint-bound, unexpired, single-use decision for an individual
remediation. A saved or requested `verified` review does not replace current
service capability and definition checks. New targets never inherit another
target's replay attestation. See
[Monitoring setup](./docs/CommandCenter.md#monitoring-setup) and
[pipeline triage](./docs/PipelineTriage.md).

The hybrid path requires neither Activator, Power Automate nor Eventhouse.
It uses REST plus native Fabric Job Eventstream delivery; Power BI polling
remains required. The selected custom endpoint uses public outbound TLS with
Entra authentication. This does not require public inbound access to the worker
or opening the SQL, model or web resources.

Source additions begin as logical proposals with no physical source ID. Only
the original verified worker observation can bind the returned owned component
IDs. Desired removal immediately fences intake but retains ownership until an
original complete receipt proves the exact node, ID and stream-route absence;
retirement leaves an immutable record. A timeout or inherited snapshot is not
removal proof.

Key-free endpoint automation remains unproved. Initial nonsecret endpoint
metadata must be taken from the Custom Endpoint's **Microsoft Entra ID** tab;
the key-returning connection API must not be called.

<br /><br />

<h2 id="quick-deploy">Quick deploy</h2>

Run it offline first. It needs no Azure subscription and no credentials:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

$env:MONITORING_MODE = "fixture"
$env:TRIAGE_PROVIDER_MODE = "mock"
$env:TRIAGE_TOOL_MODE = "mock"
.\.venv\Scripts\python.exe -m pytest -q          # the offline suite
.\.venv\Scripts\bi-triage.exe list               # the scenarios
.\.venv\Scripts\bi-triage.exe run scenario1-transient
```

`MONITORING_MODE=fixture` selects explicit offline monitoring state. Live
deployment requires `MONITORING_MODE=live`, the pinned
`MONITORING_TENANT_ID`, `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, service identity
access and deployment-owned schema. A live failure never selects fixture state.
`FABRIC_PIPELINE_TARGETS` is retired: live targets and reviews come from the
monitoring registry, with no old environment-target loader or migration.

For deployment preparation, follow the [deployment guide](./docs/DeploymentGuide.md)
and the [hybrid plan's cutover gates](./docs/HybridMonitoringPlan.md#controlled-prototype-reset).
The new baseline does not provide a Fabric SQL compatibility layer, data
migration, dual writes or mixed-version operation. The approved prototype clean
start discards old application history and imports no target configuration.
An exact, guarded reset is deployment work, not a browser or startup action.

### Prerequisites and costs

To deploy this solution accelerator, ensure you have access to an
[Azure subscription](https://azure.microsoft.com/free/) with the following
permissions:

- **Contributor** role at the subscription level
- **Role Based Access Control (RBAC)** permissions to assign roles at the
  subscription and/or resource group level
- Ability to create resource groups, resources, and app registrations

For detailed setup instructions, see
[Azure Account Set Up](./docs/AzureAccountSetUp.md).
Use one Azure SQL application database. A temporary isolated proof database may
share its logical server, but not operational records. Select and approve SQL
compute, storage, backup and network costs before provisioning or resizing.
The shipped SQL template uses one S1 application database and an optional Basic
proof database, with no elastic pool. This evaluation topology is not a final
sizing or pricing recommendation.
Fabric capacity is still needed for the monitored Fabric
workloads and Eventstream, not for application-state storage.

The table below lists the major Microsoft products used.

> **Note:** This pricing overview is not comprehensive. Actual costs vary with
> your selected SKUs, usage scale, customizations and tenant integrations. Use
> these estimates as a starting point.

<br/>

| Product | Description | Cost |
|---|---|---|
| [Azure AI Foundry](https://learn.microsoft.com/azure/ai-foundry/) | Hosts the reasoning agents and the deployed controller, and issues the agent identity the controller authenticates as. | [Pricing](https://azure.microsoft.com/pricing/details/ai-foundry/) |
| [Power BI](https://learn.microsoft.com/power-bi/) | The estate being monitored. The accelerator reads refresh history, triggers refreshes, manages refresh schedules and queries semantic models. | [Pricing](https://www.microsoft.com/power-platform/products/power-bi/pricing) |
| [Azure SQL Database](https://learn.microsoft.com/azure/azure-sql/database/) | One shared application database for configuration, intake, checkpoints, incidents, approvals, collaboration and action fences; public firewall admission, Entra-only authentication, TLS, auditing and TDE. | [Pricing](https://azure.microsoft.com/pricing/details/azure-sql-database/) |
| [Microsoft Fabric](https://learn.microsoft.com/fabric/) | Monitored pipeline workloads and native Job Eventstream delivery. Fabric data stays in Fabric; it is not the accelerator's application-state platform. | [Pricing](https://azure.microsoft.com/pricing/details/microsoft-fabric/) |
| [Azure Container Apps](https://learn.microsoft.com/azure/container-apps/) | The no-ingress monitoring worker in a public Consumption environment. Compute, data transfer and logging are costs; creation is not proof of event consumption. | [Pricing](https://azure.microsoft.com/pricing/details/container-apps/) |
| [Azure Container Registry](https://learn.microsoft.com/azure/container-registry/) | Public Basic registry with scoped managed-identity image pull; admin and anonymous access disabled. | [Pricing](https://azure.microsoft.com/pricing/details/container-registry/) |
| [Azure Logic Apps](https://learn.microsoft.com/azure/logic-apps/) | Schedules controller heartbeat and optional mailbox/health work. Consumption tier with managed identity. | [Pricing](https://azure.microsoft.com/pricing/details/logic-apps/) |
| [Azure App Service](https://learn.microsoft.com/azure/app-service/) | Public HTTPS Command Center host with Entra authorization and separate optional app/SCM caller filters; no baseline private endpoint or NAT Gateway. | [Pricing](https://azure.microsoft.com/pricing/details/app-service/linux/) |
| [Application Insights](https://learn.microsoft.com/azure/azure-monitor/app/app-insights-overview) | Optional. Traces every run as spans carrying metadata only. | [Pricing](https://azure.microsoft.com/pricing/details/monitor/) |
| [Microsoft 365 / Exchange Online](https://learn.microsoft.com/exchange/exchange-online) | Optional mailbox ingestion for Power BI failure alerts; not a dependency of hybrid polling or Eventstream intake. | [Pricing](https://www.microsoft.com/microsoft-365/business/compare-all-microsoft-365-business-products) |
| [Microsoft Teams](https://learn.microsoft.com/microsoftteams/) | Optional. Receives notification and approval cards. | [Pricing](https://www.microsoft.com/microsoft-teams/compare-microsoft-teams-business-options) |

<br/>

> ⚠️ **Important:** Remove resources created for an evaluation when it ends.
> Review the scope of `azd down` or a resource-group deletion first: the Foundry
> project, Fabric workspace and state database may predate this deployment.
> The optional command center is deployed separately and needs separate cleanup.
> Review ownership of the monitoring worker and connector items separately;
> source workspaces, models, pipelines and business data are not disposable
> monitoring infrastructure.

<h3 id="scheduling-what-does-and-does-not-work">Scheduling: what does and does not work</h3>

Foundry routines are the native scheduled trigger. In verification on
2026-09-02, six days after registration, the routine reported itself enabled
with its cron, accepted dispatches, produced no runs, and telemetry showed agent
activity in two of twenty-four hours — both of them hours when a person invoked
it by hand. `azd deploy` does not manage routines at all.

Both routines are therefore declared in `azure.yaml` and **ship disabled**, with
the evidence in the file. The scheduled trigger the accelerator actually supports
is [`infra/scheduled-sweep.json`](./infra/scheduled-sweep.json), a Consumption
Logic App with a managed identity. Its earlier-release evaluation does not
establish hybrid deployment acceptance.

For hybrid operation, that template defaults to `heartbeat`. The hosted
controller uses bounded rounds to drain both admitted monitoring work and human
commands. The separate worker owns discovery, polling, connector reconciliation
and event intake; do not create another controller timer for every target.
Create the scheduler disabled, verify its identity and database prerequisites,
then enable it as part of the approved cutover. A successful idle heartbeat is
not proof of fresh inventory, polling coverage or event delivery.

Re-test routines in your own tenant before enabling them; the observed behavior
may be regional or fixed in a later preview release. An enabled declaration is
not evidence that a schedule has executed.

<br /><br />

<h2 id="business-use-case">Business use case</h2>

A scheduled refresh on an admitted model fails at 06:02 and sends an email to
the monitored mailbox.
The next configured sweep reads the alert, checks refresh history and
deterministic data evidence, and proposes an action. The controller either
permits it within policy, requests approval for a gated action, or records an
escalation. Detection time depends on the configured schedule and service
availability; it is not a response-time guarantee.

**Key use cases by role:**

| Role | Capabilities |
|---|---|
| **BI / data platform operations** | Triage and first-line remediation of refresh failures without a human in the loop for routine cases; an auditable terminal outcome for every incident. |
| **On-call engineer** | Reviews evidence and approves or denies in the command center, or through the configured Teams/CLI channel. |
| **Data steward** | Learns about silent data-quality regressions — stale models, collapsed row counts, dropped columns — that no alert would ever have reported. |

> ⚠️ **Note:** The sample data in this repository is synthetic and intended for
> evaluation only.

### Operational behavior

<details>
  <summary>Operational behavior details</summary>

- **Scheduled processing** <br/>The loop can start unattended at the next
  successful sweep, rather than waiting for an engineer to read the mailbox.

- **Alert fatigue reduction** <br/>Repeat occurrences of one incident are
  announced once, not once per occurrence. Deduplication that stops the
  remediation but not the notification produces exactly the fatigue the
  accelerator exists to remove.

- **Bounded autonomy** <br/>The agent's authority is a short, readable list.
  Anything outside it is refused before dispatch and escalated with evidence, so
  changes to available actions require a code and policy review.

- **Silent-failure coverage** <br/>Configured probes check whether a successful
  refresh also meets expected freshness, row-count and schema conditions.

</details>

<br /><br />

<h2 id="supporting-documentation">Supporting documentation</h2>

| Document | What it covers |
|---|---|
| [Technical architecture](./docs/TechnicalArchitecture.md) | Controller and agent boundaries, state, approvals, command-center authorization, incident collaboration and network scope. |
| [Deployment guide](./docs/DeploymentGuide.md) | Everything that must exist in a tenant, ordered by lead time. |
| [Azure account setup](./docs/AzureAccountSetUp.md) | Subscription, permissions and quota prerequisites. |
| [Operations guide](./docs/OperationsGuide.md) | What runs on a schedule, off switches, budgets, inspecting state, telemetry, cost control. |
| [Customization guide](./docs/CustomizationGuide.md) | Adding a tool, an approval gate, a playbook, a detector or an agent. |
| [Scheduled Fabric pipeline triage](./docs/PipelineTriage.md) | Configured pipeline monitoring, replay safety, approval and execution verification. |
| [Hybrid monitoring plan](./docs/HybridMonitoringPlan.md) | Approved scope, ownership, public event transport, controlled cutover and outstanding empirical gates. |
| [Command center](./docs/CommandCenter.md) | Monitoring setup, coverage and target reviews, Entra roles, incident workflows and isolated scenario validation. |
| [Foundry component](./docs/foundry/README.md) | Hosted components, identity boundaries, SQL ownership and platform limits. |
| [FAQs](./docs/FAQs.md) | Common questions. |

## Guidance

### Security guidelines

This accelerator authenticates with [Managed Identity](https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/overview)
and Entra agent identity wherever the platform allows it. The controller reaches
Power BI and the shared Azure SQL Database as itself, with no key or stored SQL
password. Azure SQL supports more than Entra authentication; this deployment
must explicitly enable **Microsoft Entra-only authentication** on the logical
server. The application has no SQL-login or credential-string fallback.
The public SQL endpoint uses TLS 1.2 minimum, Proxy/TCP 1433, auditing and TDE.
The default `allowAzureServices=true` creates the special SQL firewall rule
whose start and end are both `0.0.0.0`. It admits Azure-hosted callers, including
other subscriptions, not all Internet IPs; Entra SQL permissions remain
mandatory. Optional exact IPv4 client ranges provide additional network admission.

Ordinary templates contain no baked-in MCAPS exemptions. In the approved MCAPS
evaluation, `SecurityControl=Ignore` and reason/review tags are scoped to the
SQL server, with a separate registry exception. The single 14-day period does
not restart when a tag is removed/re-added; longer tests need an approved
exclusion. See [network controls and exceptions](./docs/DeploymentGuide.md#governed-evaluation-exceptions).
Public reachability does not grant anonymous API, SQL, registry or Foundry access.

The command center is a secretless SPA/API. The backend validates the delegated
API token and its `CommandCenter.Reader`, `CommandCenter.Operator`,
`CommandCenter.Approver` or `CommandCenter.Admin` roles on every request.
Ordinary Entra security groups supply these roles; there is no SQL ACL or
editable in-app access manager. App roles grant no Azure, Fabric or directory
permissions to the user, controller or reasoning agents.

**Refresh permissions** requests a fresh token for this API and reloads access
details and the command-center snapshot. Mutation controls stay locked until
the new permission-refresh generation has a successful snapshot; earlier
records cannot restore permission. It does not revoke already-issued
tokens in other sessions: roles can remain valid until token expiry or renewal,
with the backend's 30-second validation leeway. Authorization needs no runtime
Graph directory permission. The optional profile photo uses a separate delegated
Graph `User.Read` token.

The source retains these credential-bearing inputs for older optional mailbox
and Teams integrations. They are not supported shortcuts for the secretless
hybrid deployment:

| Credential | Why it exists | Scope |
|---|---|---|
| Mailbox app registration client secret | The tested hosted Entra agent identity was rejected by Exchange for app-only mailbox reads. This is separate from command-center sign-in. | One mailbox, enforced by an Exchange `ApplicationAccessPolicy`. |
| Teams Workflows webhook URL | The URL *is* the credential; there is no identity on an incoming webhook. | One channel. |

Do not populate them to make hybrid preflight appear ready. Keep any existing
values out of the repository and track their expiry/failure during retirement.
A secret-based mailbox path fails in tenants that remove app secrets on a
30-day schedule: use a separately verified workload-identity path or leave mail
off. Legacy Teams approval links are not Entra-authenticated decisions; web
proposals require the command-center decision path. No legacy state or target
configuration is imported.

Design rules worth keeping if you adapt this:

- **Grant permissions to the component that acts, not the one that reasons.** The
  prompt agents hold no permissions at all.
- **The inbox filter is a security control.** An agent that acts on every message
  it receives is steerable by anyone who can email it. It fails closed, including
  when its own pattern is invalid.
- **Never commit a filled-in `.env`, a webhook URL, or any credential.** A
  credential scanner runs in CI on every change.

To ensure continued best practices in your own repository, enable
[GitHub secret scanning](https://docs.github.com/code-security/secret-scanning/about-secret-scanning).

You may also want to consider
[Microsoft Defender for Cloud](https://learn.microsoft.com/azure/defender-for-cloud/).

<br/>

### Frequently asked questions

[Click here](./docs/FAQs.md) to learn more about common questions about this
solution.

<br/>

### Cross references

Check out similar solution accelerators.

| Solution Accelerator | Description |
|---|---|
| [Microsoft IQ Solution Accelerator](https://github.com/microsoft/microsoft-iq-solution-accelerator) | Unifies enterprise data, knowledge and workflows across Fabric IQ, Foundry IQ and Work IQ to support operational decisions. |
| [Real-Time Intelligence for Operations](https://github.com/microsoft/real-time-intelligence-operations-solution-accelerator) | A real-time intelligence platform for manufacturing operations, with anomaly detection and a conversational data agent. |
| [Unified Data Foundation with Microsoft Fabric](https://github.com/microsoft/unified-data-foundation-with-fabric-solution-accelerator) | Unified data foundation with options to integrate Azure Databricks and Microsoft Purview. |

<br/>

## Provide feedback

Have questions, find a bug, or want to request a feature?
[Submit a new issue](https://github.com/SQLBImhugh/foundry-fabric-triage-solution-accelerator/issues)
on this repo and we'll connect.

<br/>

## Responsible AI Transparency FAQ

Please refer to [Transparency FAQ](./TRANSPARENCY_FAQ.md) for responsible AI
transparency details of this solution accelerator.

<br/>

## Disclaimers

This is an independently maintained sample. It is not a Microsoft product, is
not affiliated with or endorsed by Microsoft, and carries no support or service
level agreement of any kind. It is provided "as is", per the [MIT licence](./LICENSE).

Using this code does not grant you any right to use Microsoft products or
services. Your use of Azure AI Foundry, Power BI, Microsoft Graph, Microsoft 365
and any other Microsoft service remains governed by the Product Terms applicable
to those services, and nothing here supersedes, amends or modifies them.

You must comply with all domestic and international export laws and regulations
that apply to the software, including restrictions on destinations, end users and
end use. See [https://aka.ms/exporting](https://aka.ms/exporting).

This software is not designed, intended or made available as a medical device,
nor as a substitute for professional medical, financial, legal or safety advice,
diagnosis, treatment or judgment. It has not been assessed against SOC 1 or SOC 2.

**The controller can make policy-admitted changes to monitored BI resources.**
By deploying it you accept responsibility for the actions it executes on your
behalf. Set your own policy limits, keep the approval gate configured, and
evaluate it offline before pointing it at anything that matters.

BY ACCESSING OR USING THE SOFTWARE, YOU ACKNOWLEDGE THAT IT IS NOT DESIGNED OR INTENDED TO SUPPORT ANY USE IN WHICH A SERVICE INTERRUPTION, DEFECT, ERROR, OR OTHER FAILURE OF THE SOFTWARE COULD RESULT IN THE DEATH OR SERIOUS BODILY INJURY OF ANY PERSON OR IN PHYSICAL OR ENVIRONMENTAL DAMAGE (COLLECTIVELY, "HIGH-RISK USE"), AND THAT YOU WILL ENSURE THAT, IN THE EVENT OF ANY INTERRUPTION, DEFECT, ERROR, OR OTHER FAILURE OF THE SOFTWARE, THE SAFETY OF PEOPLE, PROPERTY, AND THE ENVIRONMENT ARE NOT REDUCED BELOW A LEVEL THAT IS REASONABLY, APPROPRIATE, AND LEGAL, WHETHER IN GENERAL OR IN A SPECIFIC INDUSTRY. BY ACCESSING THE SOFTWARE, YOU FURTHER ACKNOWLEDGE THAT YOUR HIGH-RISK USE OF THE SOFTWARE IS AT YOUR OWN RISK.
