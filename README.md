# BI Triage Solution Accelerator

The BI Triage Solution Accelerator is a multi-agent operations loop for business
intelligence failures. When a Power BI refresh fails, it gathers evidence,
consults a specialist agent, decides what it is allowed to do, and either
remediates within policy or escalates to a human with the evidence attached. It
also finds the failures that never raise an alert at all, which is the class of
problem no inbox will ever tell you about.

**Key use cases and customization:**

- **Power BI operations use case**: A semantic model refresh fails at 06:00. The
  accelerator triages the cause, applies a permitted fix or requests approval for
  one that is not permitted, deduplicates repeat occurrences, and records a
  terminal outcome for every incident.
- **Reusability and customization**: The policy ledger, action allowlists,
  approval gate and evidence model are domain-independent. See
  [How to customize](#how-to-customize).

<br/>

<div align="center">

[**SOLUTION OVERVIEW**](#solution-overview) \| [**QUICK DEPLOY**](#quick-deploy) \| [**BUSINESS SCENARIO**](#business-use-case) \| [**SUPPORTING DOCUMENTATION**](#supporting-documentation)

</div>
<br/>

<h2 id="solution-overview">Solution overview</h2>

This solution accelerator is built on Azure AI Foundry, Power BI and Microsoft
Graph. A controller agent orchestrates the loop, a data quality agent
investigates data-shaped failures, and every action the system can take is on an
allowlist enforced in code.

It runs **fully offline** with mock providers and mock tools, so you can read it,
run it and evaluate its behaviour before it touches a tenant. That is also how
the test suite runs: no credentials, no network.

**Bring your own Foundry project and Fabric workspace.** This repository deploys
the agents and the supporting Logic App *into* an Azure AI Foundry project and a
Microsoft Fabric workspace you already have; it does not provision the project,
the Fabric SQL Database or Application Insights for you.
[`docs/DeploymentGuide.md`](./docs/DeploymentGuide.md) lists every
prerequisite in the order its lead time demands, and
[`docs/AzureAccountSetUp.md`](./docs/AzureAccountSetUp.md) covers the
subscription and tenant permissions you need first.

### Solution architecture

The diagram below illustrates the solution architecture. For a detailed
description, see the [architecture description](./docs/TechnicalArchitecture.md).

| ![Solution architecture](./docs/images/readme/solution-architecture.png) |
| ------------------------------------------------------------------------ |

**Preview notice:** Some platform capabilities used in this solution are
currently in preview, including Foundry hosted agents, Entra agent identity and
Foundry routines. These features are provided "as-is" and may change without
notice. One of them does not currently work as documented, and the accelerator
ships around it rather than pretending otherwise — see
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
  <summary>Click to learn more about the key features this solution enables</summary>

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

- **Evidence and redaction** <br/>Deterministic evidence outranks model output,
  and disagreements are logged. Redaction happens inside the store boundary, so a
  call site cannot forget it.

- **Monitoring cockpit** <br/>A read-only Fabric App (`cockpit/`) over the
  controller's own state: incidents, approvals, deferred retries, semantic-health
  baselines, and the claims and leases that stop two invocations acting on the
  same alert. It reads a Direct Lake semantic model over the state database, so
  it adds no writer and no second copy of the truth — nothing in it can change
  the system it watches.

</details>

<br /><br />

<h2 id="quick-deploy">Quick deploy</h2>

Run it offline first. It needs no Azure subscription and no credentials:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

.\.venv\Scripts\python.exe -m pytest -q          # the offline suite
.\.venv\Scripts\bi-triage.exe list               # the scenarios
.\.venv\Scripts\bi-triage.exe run scenario1-transient
```

To deploy into your environment, follow the
[deployment guide](./docs/DeploymentGuide.md).

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

The table below lists the major Microsoft products used.

> **Note:** This pricing overview is not comprehensive. Actual costs vary with
> your selected SKUs, usage scale, customizations and tenant integrations. Use
> these estimates as a starting point.

<br/>

| Product | Description | Cost |
|---|---|---|
| [Azure AI Foundry](https://learn.microsoft.com/azure/ai-foundry/) | Hosts the reasoning agents and the deployed controller, and issues the agent identity the controller authenticates as. | [Pricing](https://azure.microsoft.com/pricing/details/ai-foundry/) |
| [Power BI](https://learn.microsoft.com/power-bi/) | The estate being monitored. The accelerator reads refresh history, triggers refreshes, manages refresh schedules and queries semantic models. | [Pricing](https://www.microsoft.com/power-platform/products/power-bi/pricing) |
| [Microsoft Fabric](https://learn.microsoft.com/fabric/) | Provides the capacity that Power BI semantic models run on, where capacity throttling originates, and the Fabric SQL Database holding durable state: incidents, processed messages, approvals, deferred retries, sweep leases and semantic health baselines. | [Pricing](https://azure.microsoft.com/pricing/details/microsoft-fabric/) |
| [Azure Logic Apps](https://learn.microsoft.com/azure/logic-apps/) | The scheduled trigger. Consumption tier, managed identity, no keys. | [Pricing](https://azure.microsoft.com/pricing/details/logic-apps/) |
| [Application Insights](https://learn.microsoft.com/azure/azure-monitor/app/app-insights-overview) | Optional. Traces every run as spans carrying metadata only. | [Pricing](https://azure.microsoft.com/pricing/details/monitor/) |
| [Microsoft 365 / Exchange Online](https://learn.microsoft.com/exchange/exchange-online) | Supplies the monitored mailbox that Power BI failure alerts arrive in. | [Pricing](https://www.microsoft.com/microsoft-365/business/compare-all-microsoft-365-business-products) |
| [Microsoft Teams](https://learn.microsoft.com/microsoftteams/) | Optional. Receives notification and approval cards. | [Pricing](https://www.microsoft.com/microsoft-teams/compare-microsoft-teams-business-options) |

<br/>

> ⚠️ **Important:** To avoid unnecessary costs, remember to take down your
> deployment when it is no longer in use, either by deleting the resource group
> in the Portal or running `azd down`.

<h3 id="scheduling-what-does-and-does-not-work">Scheduling: what does and does not work</h3>

Foundry routines are the native scheduled trigger, and they **do not fire**.
Verified six days after registration: the routine reported itself enabled with
its cron, accepted dispatches, produced no runs, and telemetry showed agent
activity in two of twenty-four hours — both of them hours when a person invoked
it by hand. `azd deploy` does not manage routines at all.

Both routines are therefore declared in `azure.yaml` and **ship disabled**, with
the evidence in the file. The scheduled trigger the accelerator actually supports
is [`infra/scheduled-sweep.json`](./infra/scheduled-sweep.json), a Consumption
Logic App with a managed identity, verified end to end.

An accelerator whose subject is failures that never announce themselves must not
ship a scheduler that silently does nothing. Re-test routines in your own tenant
before enabling them — this may be regional, or already fixed.

<br /><br />

<h2 id="business-use-case">Business use case</h2>

A scheduled refresh fails at 06:02. An email lands in a shared mailbox that
already receives dozens a week. Nobody is on shift. The report is wrong when the
business opens, and the first anyone hears of it is a question from a user.

This accelerator closes that gap. It reacts to the alert within minutes, works
out what actually happened from refresh history rather than from the subject
line, and either fixes it within a policy an operations team wrote, or asks a
named human for permission with the evidence already gathered.

**Key use cases by role:**

| Role | Capabilities |
|---|---|
| **BI / data platform operations** | Triage and first-line remediation of refresh failures without a human in the loop for routine cases; an auditable terminal outcome for every incident. |
| **On-call engineer** | Receives an approval request with evidence attached rather than a bare alert, and approves or denies from Teams or the CLI. |
| **Data steward** | Learns about silent data-quality regressions — stale models, collapsed row counts, dropped columns — that no alert would ever have reported. |

> ⚠️ **Note:** The sample data in this repository is synthetic and intended for
> evaluation only.

### Business value

<details>
  <summary>Click to learn more about what value this solution provides</summary>

- **Time to first action** <br/>The loop starts within minutes of the alert
  rather than at the start of the next working day.

- **Alert fatigue reduction** <br/>Repeat occurrences of one incident are
  announced once, not once per occurrence. Deduplication that stops the
  remediation but not the notification produces exactly the fatigue the
  accelerator exists to remove.

- **Bounded autonomy** <br/>The agent's authority is a short, readable list.
  Anything outside it is refused before dispatch and escalated with evidence, so
  adopting it is a policy decision rather than an act of faith.

- **Failures nobody reports** <br/>The silent-failure detector covers the gap
  between "the refresh succeeded" and "the data is right", which is where the
  most damaging BI incidents live.

</details>

<br /><br />

<h2 id="supporting-documentation">Supporting documentation</h2>

| Document | What it covers |
|---|---|
| [Technical architecture](./docs/TechnicalArchitecture.md) | The flow, the agent boundary, outcome validation, signatures and suppression, approvals, providers, observability. |
| [Deployment guide](./docs/DeploymentGuide.md) | Everything that must exist in a tenant, ordered by lead time. |
| [Azure account setup](./docs/AzureAccountSetUp.md) | Subscription, permissions and quota prerequisites. |
| [Operations guide](./docs/OperationsGuide.md) | What runs on a schedule, off switches, budgets, inspecting state, telemetry, cost control. |
| [Customization guide](./docs/CustomizationGuide.md) | Adding a tool, an approval gate, a playbook, a detector or an agent. |
| [Foundry component](./docs/foundry/README.md) | What runs where, which identity does it, and the platform behaviours this repo has already paid for. |
| [FAQs](./docs/FAQs.md) | Common questions. |

## Guidance

### Security guidelines

This accelerator authenticates with [Managed Identity](https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/overview)
and Entra agent identity wherever the platform allows it. The controller reaches
Power BI and its Fabric SQL Database as itself, with no key and no secret —
Fabric SQL accepts Entra tokens only, so there is no connection string to leak
and no local-authentication fallback to switch off.

Two bearer credentials remain, and each is a deliberate exception:

| Credential | Why it exists | Scope |
|---|---|---|
| App registration client secret | Exchange does not yet accept an Entra agent identity for app-only mailbox reads. | One mailbox, enforced by an Exchange `ApplicationAccessPolicy`. |
| Teams Workflows webhook URL | The URL *is* the credential; there is no identity on an incoming webhook. | One channel. |

Both live in the azd environment, which is gitignored, and never in the
repository. Only the first expires, and its expiry will stop mail ingestion
silently, so track it.

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

**It grants an autonomous agent permission to change a production BI estate.**
By deploying it you accept responsibility for the actions it takes on your
behalf. Set your own policy limits, keep the approval gate configured, and
evaluate it offline before pointing it at anything that matters.

BY ACCESSING OR USING THE SOFTWARE, YOU ACKNOWLEDGE THAT IT IS NOT DESIGNED OR INTENDED TO SUPPORT ANY USE IN WHICH A SERVICE INTERRUPTION, DEFECT, ERROR, OR OTHER FAILURE OF THE SOFTWARE COULD RESULT IN THE DEATH OR SERIOUS BODILY INJURY OF ANY PERSON OR IN PHYSICAL OR ENVIRONMENTAL DAMAGE (COLLECTIVELY, "HIGH-RISK USE"), AND THAT YOU WILL ENSURE THAT, IN THE EVENT OF ANY INTERRUPTION, DEFECT, ERROR, OR OTHER FAILURE OF THE SOFTWARE, THE SAFETY OF PEOPLE, PROPERTY, AND THE ENVIRONMENT ARE NOT REDUCED BELOW A LEVEL THAT IS REASONABLY, APPROPRIATE, AND LEGAL, WHETHER IN GENERAL OR IN A SPECIFIC INDUSTRY. BY ACCESSING THE SOFTWARE, YOU FURTHER ACKNOWLEDGE THAT YOUR HIGH-RISK USE OF THE SOFTWARE IS AT YOUR OWN RISK.
