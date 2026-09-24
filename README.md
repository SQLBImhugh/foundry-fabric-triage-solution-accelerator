# The BI Triage Solution Accelerator

<img src="./command-center/public/triage-logo.png" alt="BI Triage logo" width="44" height="52" />

The **BI Triage Solution Accelerator** is a developer sample built on Azure AI
Foundry, Azure SQL Database, Power BI and Microsoft Fabric. It shows how to
build a multi-agent operations loop that triages business intelligence failures
without giving a language model the authority to break things. This document
explains the problem the solution addresses, describes how the solution is put
together and why each part is shaped the way it is, and gives step-by-step
instructions for running it on a local workstation before it touches a tenant.

The whole solution runs **fully offline** with mock providers and mock tools. No
Azure subscription, no credentials and no network access are required to read
it, run it and evaluate its behaviour. That is also how its test suite runs.

> **Note:** This is an independently maintained sample. It is not a Microsoft
> product and carries no support agreement. See [Disclaimers](#disclaimers).

## Table of contents

- [Introduction](#introduction)
  - [What makes BI failure triage hard](#what-makes-bi-failure-triage-hard)
  - [The authority problem](#the-authority-problem)
  - [What this accelerator demonstrates](#what-this-accelerator-demonstrates)
- [Solution architecture](#solution-architecture)
  - [Understanding the triage controller](#understanding-the-triage-controller)
  - [Understanding the triage agent and the tool dispatcher](#understanding-the-triage-agent-and-the-tool-dispatcher)
  - [Understanding the data quality agent](#understanding-the-data-quality-agent)
  - [Understanding the silent-failure detector](#understanding-the-silent-failure-detector)
  - [Understanding the monitoring worker](#understanding-the-monitoring-worker)
  - [Understanding the command center](#understanding-the-command-center)
  - [Understanding the stores](#understanding-the-stores)
- [Design rules worth understanding](#design-rules-worth-understanding)
- [Run the solution on your workstation](#run-the-solution-on-your-workstation)
  - [Set up the Python environment](#set-up-the-python-environment)
  - [Run the offline test suite](#run-the-offline-test-suite)
  - [Run a scenario](#run-a-scenario)
  - [Understanding the scenario files](#understanding-the-scenario-files)
- [Deploy into a tenant](#deploy-into-a-tenant)
  - [Prerequisites and costs](#prerequisites-and-costs)
  - [Scheduling: what does and does not work](#scheduling-what-does-and-does-not-work)
  - [Hybrid monitoring scope and limits](#hybrid-monitoring-scope-and-limits)
- [Security guidelines](#security-guidelines)
- [Verification status](#verification-status)
- [Customize the solution](#customize-the-solution)
- [Supporting documentation](#supporting-documentation)
- [Feedback, transparency and disclaimers](#feedback-transparency-and-disclaimers)

## Introduction

A business intelligence estate fails quietly and often. A scheduled semantic
model refresh fails at 06:02 and emails somebody. A Fabric data pipeline fails
overnight and nobody looks until a report is wrong. A refresh reports success
while the source system never landed its data, so the model is stale and no
alert fires at all.

The work of responding to these failures is repetitive and well understood. An
operator reads the alert, opens the refresh history, looks at the error, decides
whether it is the kind of failure a retry fixes, and either retries it or
escalates it to whoever owns the data. Most of that is mechanical. It is also
slow, because it waits for a person to read an email.

### What makes BI failure triage hard

The obvious idea is to give a language model access to the Power BI REST API and
ask it to fix refresh failures. That idea fails for reasons worth being explicit
about, because each one shapes part of this solution.

**Most failure text is ambiguous.** `DM_GWPipeline_Gateway_MashupDataAccessError`
can mean an expired credential, a gateway that is offline, or a source database
that is refusing connections. Retrying helps in one of those cases and wastes
time in the other two. Deciding correctly needs evidence — the refresh history,
the gateway status, the last successful run — not a guess from the error string.

**Some failures are invisible.** A refresh that succeeds against an empty source
table reports success. A pipeline that loads a tenth of its usual rows reports
success. There is no alert to triage, so anything built purely as an
alert-response loop will never see the most damaging class of failure.

**A wrong action is expensive.** Triggering a refresh on a model that is already
refreshing, or rerunning a pipeline that writes to a production table, is worse
than doing nothing. Actions in this domain are not freely retryable.

**The same failure arrives many times.** One broken gateway produces a refresh
failure every thirty minutes. An operations loop that responds to each occurrence
individually generates exactly the alert fatigue it was built to remove.

### The authority problem

Now consider what happens when a language model is given the ability to act.

A model proposes a tool call. The tool call runs. If the model is wrong about
which tool to call, or about the arguments, the effect happens anyway. Prompt
wording such as "only refresh a dataset once" is not a limit: it is a request,
and models do not always honour requests. Anything that can be expressed in a
prompt can be talked out of by input the model reads later — including the text
of the alert it is triaging.

There are three separate questions hidden in "can the agent do this?", and they
need three separate mechanisms:

| Question | Mechanism | Where it lives |
|---|---|---|
| Is this action one the system is ever allowed to take? | An allowlist | Code, checked before dispatch |
| Has this run already used its budget for actions of this kind? | A policy ledger | Code, shared across all agents in the run |
| Does a human agree to this specific action, right now? | An approval gate | A durable, fingerprint-matched record |

None of those is a prompt. The design principle running through this whole
solution is that **the reasoning component proposes and the controller
decides**. The agents hold no permissions at all. The component that holds
credentials is the one that checks the rules.

### What this accelerator demonstrates

The solution shows one complete, readable implementation of that separation,
covering the requirements that are common when building agentic operations
loops:

- Triaging a Power BI refresh failure from evidence rather than error text
- Enforcing budgets and allowlists in code, with a test proving each limit fires
- Requiring explicit human approval for a defined tier of actions
- Detecting failures that produce no alert
- Triaging scheduled Fabric pipeline failures, where a rerun is not freely repeatable
- Discovering and monitoring a tenant's Power BI and Fabric estate
- Recording an auditable terminal outcome for every incident, including crashes
- Announcing a repeating incident once rather than once per occurrence

## Solution architecture

The solution is built on a shared **Azure SQL Database** that holds all durable
application state. Around that database sit a hosted controller, a monitoring
worker, a set of agents, and an operator web application.

| ![Solution architecture](./docs/images/readme/solution-architecture.png) |
| ------------------------------------------------------------------------ |

> **Note:** The diagram illustrates the earlier release's agent and mailbox
> paths. It does not show the hybrid intake topology or prove its deployment.
> For current boundaries, see the
> [technical architecture](./docs/TechnicalArchitecture.md) and the
> [hybrid monitoring plan](./docs/HybridMonitoringPlan.md).

Let us begin with a brief description of each part.

- **Triage controller** (`src/triage/runner.py`): orchestrates the loop. Builds
  clients and stores, computes failure signatures, looks up open incidents,
  constructs agents and persists the terminal outcome.
- **Triage agent** (`src/triage/agents/triage_agent.py`): the reasoning loop.
  Proposes tool calls, which the dispatcher in `src/triage/tools/registry.py`
  checks before anything runs.
- **Data quality agent** (`src/triage/agents/data_quality_agent.py`): a separate
  typed agent, exposed to the orchestrator as a single tool, that investigates
  data-shaped failures.
- **Silent-failure detector**: deterministic probes for freshness, row-count and
  schema regressions that produce no alert.
- **Monitoring worker** (`src/triage/monitoring/worker.py`): discovers the
  estate, polls service history and receives native Fabric Job events.
- **Command center** (`command-center/`): a React front end over a FastAPI
  service, used by operators for configuration, approvals and history.
- **Stores** (`src/triage/store/`): incidents, approvals, retries, claims,
  baselines, monitoring state and command-center state.

Now we will look at each of these in more detail.

### Understanding the triage controller

The controller owns everything that is a decision rather than a judgement. When
an alert arrives, it:

1. Parses the alert and computes a **failure signature** — a stable identity for
   "this failure, on this target" that survives wording changes in the message.
2. Looks up whether an incident with that signature is already open. If one is,
   the occurrence is recorded but the incident is not announced again. This is
   enforced against a `notified_count` in the controller, not in prompt wording,
   because deduplication that suppresses the remediation but not the notification
   produces the alert fatigue the accelerator exists to remove.
3. Constructs the agents and the shared policy ledger for the run.
4. Persists a terminal outcome.

That last step is unconditional. Every run ends in exactly one of twelve
outcomes, including the ones nobody plans for:

```text
resolved · deferred_retry · needs_human · flagged_data_quality ·
duplicate_suppressed · declared_failed · approval_denied · policy_blocked ·
budget_exceeded · max_turns_exceeded · timed_out · agent_crashed
```

An incident with no recorded outcome is indistinguishable from one that never
happened, so crashes and refusals are persisted just as carefully as successes.

### Understanding the triage agent and the tool dispatcher

The triage agent runs the reasoning loop, and it is the part of the system with
the least authority. It cannot call anything directly. When the model proposes a
tool call, the `ToolDispatcher`:

1. **Charges the shared `PolicyLedger`.** Turn, tool-call, token, write-action
   and wall-clock budgets are shared across every agent in the run, not per
   agent, so a second agent cannot refresh a budget the first one spent.
2. **Checks the allowlist.** Every tool belongs to `REMEDIATION_ACTIONS`,
   `REPORTING_ACTIONS` or `DIAGNOSTIC_ACTIONS`, all declared in
   `src/triage/policy.py`. Anything not on a list is refused before dispatch.
   This is the property the whole design rests on: the set of things the system
   can possibly do is a short list you can read.
3. **Checks deterministic preconditions**, such as whether the target is
   already refreshing.
4. **Checks approval state** for gated actions.

Only then does the call run. Afterwards, the agent's proposed final outcome is
validated against the actions and evidence actually recorded, so an agent cannot
report success for something that did not happen.

Two details in the tool layer are worth copying. The first is that **tools fail
loudly rather than return something interpretable**: calling Power BI with an
empty dataset ID returns HTTP 404, and a model handed a 404 will confidently
conclude the dataset was deleted. The second is that **redaction happens inside
the store boundary**, not at call sites, because a call site can forget.

### Understanding the data quality agent

Some refresh failures are not infrastructure failures. A model that fails on a
duplicate key in a dimension has a data problem, and the fix is owned by whoever
produces that data.

The data quality agent is a separate, typed agent with its own provider and
prompt. Its controller scans the registered tables first, deterministically, and
then makes a model call **with no tools at all**. The scan establishes the facts;
the agent interprets and reports them; the orchestrator decides what happens
next. It is exposed to the orchestrator as a single tool and returns a typed
Pydantic result.

The separation matters because a model that can both gather and interpret its own
evidence can also, in effect, choose its evidence. Deterministic collection with
a tool-free interpreter removes that option. The same shape is the recommended
pattern for adding any new agent to this solution.

### Understanding the silent-failure detector

The detector finds the failures that send no alert: a refresh that reports
success while the source never landed, or a table that loads a tenth of its rows.
It compares current freshness, row counts and schema against configured
expectations and recorded baselines.

It is deterministic by design and uses none of the agent tools. The reason is
specific: a model asked whether a 60% row drop is acceptable will sometimes say
yes. Where a model claim and a scan disagree, the scan wins and the disagreement
is logged.

### Understanding the monitoring worker

The worker is the component that talks to the estate. It discovers supported
targets, polls service history, reconciles app-owned Eventstream sources and
receives native Fabric Job events with a pinned managed identity.

The important boundary is what the worker is *not* allowed to conclude. A worker
observation is evidence, not authority:

- **An event is a source reference, not proof of a failed execution.** REST
  evidence and controller admission are still required.
- **Discovery is not permission.** Finding an item in a tenant grants neither
  service access nor any right to remediate it.
- **Worker observations and web intents do not publish** admission, source heads
  or action authority. Only deterministic controller reconciliation does.

This is enforced in SQL, not by convention. Live stores select an explicit
`worker`, `web` or `controller` component and reach the database through checked
views and fixed stored procedures, never broad table DML. Success of an RPC comes
from its typed result; an `EXEC` rowcount is not the procedure's outcome.

The source path through the system reads:

```text
Command Center scope/review requests -> durable web intents (pending)
Collector-only REST inventory/polling -> worker observations + receipts
Optional event-enabled mode -> native Job receipts with an owned binding
Controller heartbeat -> deterministic reconcile_state (no agent or action)
  -> published registry/source/connector authority
Controller heartbeat -> eligible source work and human-command queues
  -> current admission, policy, approval and atomic action reservation
  -> exact execution/configuration verification
```

Start with [collector-only setup](./docs/DeploymentGuide.md#collector-only-quickstart)
for inventory and REST polling; it needs no Eventstream metadata. Event intake is
a separately configured mode, not a fallback selected by missing settings.

### Understanding the command center

The command center is the operational UI: a secretless single-page application
over a FastAPI service. It provides monitoring setup, a work queue and inspector,
authenticated approvals, queued investigations and full run history. Incident
records support append-only notes, persisted tool-free discussion and
**Resolved by user** tracking.

It queues work for the controller rather than dispatching remediation from a
browser request. Two consequences follow, and both are deliberate:

- **Human closure is not a verified repair.** Marking an incident resolved is
  tracking. It does not reset the controller's remediation budget, approvals,
  claims or notification counts, and new evidence invalidates that closure.
- **Observers cannot act.** Questions and notes are untrusted annotations, not
  diagnostics or tool instructions. The observer agent has no tools and its
  answers render without raw HTML, images or unsafe links.

Access comes only from validated Entra app-role claims:
`CommandCenter.Reader`, `CommandCenter.Operator`, `CommandCenter.Approver` and
`CommandCenter.Admin`, supplied by ordinary Entra security groups. Operator and
Approver are deliberately separate. Admin permits all application operations but
grants no directory administration, no SQL server Entra-administrator role and no
controller service permissions. The read-only **Access & permissions** page shows
effective token roles, not a directory membership roster, and cannot edit access.

A separate Fabric App sample is retained in `cockpit/` as a read-only
illustration of incidents, approvals, retries, baselines and claims through a
semantic model. Its earlier-release Direct Lake binding is not a verified Azure
SQL read path, and it is not a deployment or state dependency.

See [command center setup and operation](./docs/CommandCenter.md).

### Understanding the stores

All durable application state lives in **one shared Azure SQL Database**:
configuration, intake, checkpoints, incidents, processed messages, approvals,
retries, claims, semantic-health baselines, the inbox-filter audit, pipeline
rerun reservations and command-center state. Sharing one database is what allows
cross-store receipts and finalization to commit atomically.

Three properties of these stores are worth understanding before adapting them.

**They fail closed.** A store that cannot reach its backend reports failure and
re-checks on use. It does not fall back to memory. An in-memory fallback answers
"no open incident" to every question, which silently licenses a second
remediation on an incident that already had one.

**Claims and leases are atomic.** They use a single conditional database
operation, never a read-then-write. The expiry test runs on the server, because
two containers with skewed clocks would otherwise disagree about whether a lease
had expired and both could take it.

**Nothing durable lives on an object.** A hosted agent is rebuilt for every
request — eight hours of telemetry recorded 67 distinct role instances for 64
heartbeats — so an instance attribute is always empty on arrival, and a
process-local lock spans nothing beyond the call it is taken in. Duplicate
remediation is prevented by the durable per-alert claim on the shared database,
or not at all.

Offline, JSON and CSV implementations keep local runs reproducible. They are
explicit offline implementations selected by configuration, never a fallback for
unavailable live state.

## Design rules worth understanding

These are the rules the implementation actually enforces. Each exists because of
a specific failure, and each is the part most worth keeping if you adapt this
solution to another domain.

1. **Policy is enforced in the controller, not the prompt.** A limit that exists
   only as prompt wording is not a limit. Every budget in `src/triage/policy.py`
   has a test proving it fires.
2. **Every tool is on an allowlist.** Anything else is refused before dispatch.
3. **An approval is only a yes if it is explicit, fingerprint-matched, unexpired
   and unused.** Everything else — a timeout, an error, a malformed reply, no
   gate configured at all — is a no. Silence is never consent.
4. **A denial must not consume the remediation budget.** Otherwise one "no"
   silently disarms the agent for the rest of the incident.
5. **An approval decides whether a permitted action runs.** It can never
   authorise an action that is off the allowlist.
6. **Deterministic evidence outranks model output.** When a scan and a model
   claim disagree, the scan wins and the disagreement is logged.
7. **Grant permissions to the component that acts, not the one that reasons.**
   If a reasoning agent appears to need a permission, the design is wrong.
8. **The inbox filter is a security control.** An agent that acts on every
   message it receives is steerable by anyone who can email it. The filter fails
   closed, including when its own pattern is invalid. Never widen it to make
   something trigger — send a matching message instead.
9. **Tools fail loudly rather than return something interpretable.**
10. **Spans carry metadata only.** Prompt and completion content is never
    attached to telemetry.
11. **An incident is announced once, not once per occurrence.**
12. **Scenarios are reproducible.** Same input, same tool sequence, same numbers.
    When a live model and the mock diverge, the *controller* is changed so both
    agree — never the policy limit, and never the scenario's expectations.

## Run the solution on your workstation

Running offline requires no Azure subscription, no credentials and no network
access. This is the recommended way to evaluate the solution's behaviour.

### Set up the Python environment

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

Then select the offline modes. Each is an explicit choice, not a default reached
by leaving settings blank:

```powershell
$env:MONITORING_MODE = "fixture"
$env:TRIAGE_PROVIDER_MODE = "mock"
$env:TRIAGE_TOOL_MODE = "mock"
```

`MONITORING_MODE=fixture` selects explicit offline monitoring state.
`TRIAGE_PROVIDER_MODE=mock` selects a scripted state machine in place of a
language model, which is what makes runs reproducible.
`TRIAGE_TOOL_MODE=mock` replaces the Power BI and Fabric tools with fixtures.

### Run the offline test suite

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe scripts\scan_secrets.py
```

The suite makes no network calls and needs no credentials. A test that needs a
tenant is not a test. `scan_secrets.py` is the credential gate that also runs in
CI on every change.

The web front end has its own offline suite:

```powershell
npm --prefix .\command-center test
npm --prefix .\command-center run lint
npm --prefix .\command-center run build
```

### Run a scenario

```powershell
.\.venv\Scripts\bi-triage.exe list
.\.venv\Scripts\bi-triage.exe run scenario1-transient
```

Other commands are available for inspecting what a run did and what the system
would do against a tenant:

| Command | What it does |
|---|---|
| `bi-triage list` | Lists available scenarios |
| `bi-triage run <name>` | Runs a scenario end to end (`--verbose` shows agent reasoning) |
| `bi-triage tools` | Prints the agent tool schemas |
| `bi-triage flags` | Shows the data quality flag table |
| `bi-triage incidents` | Shows the incident store, including terminal outcomes |
| `bi-triage retries` | Shows retries the agent postponed (`--drain` performs the due ones) |
| `bi-triage commands` | Inspects or drains operator commands |
| `bi-triage preflight` | Verifies configuration, including Azure SQL |
| `bi-triage watch` | Polls a mailbox and triages each new message |
| `bi-triage serve` | Runs the operator command center locally |
| `bi-triage reset` | Clears flags and incidents |

### Understanding the scenario files

The 15 files in [`scenarios/`](./scenarios) are executable specifications rather
than samples. Each describes an alert, the mock service responses it should meet,
and an `expect` block stating the tool sequence and terminal outcome that must
result. `TriageRunner` wires those mock inputs into the same controller path the
deployed application uses, and `tests/test_scenarios.py` checks every `expect`
block. The `expect` block *is* the test.

They are also the fastest way to read the system's behaviour:

| Scenario | Terminal outcome | What it demonstrates |
|---|---|---|
| `scenario1-transient` | `resolved` | A refresh times out. The agent checks for a known incident, consults the data quality agent (which finds nothing), confirms the failure is isolated and applies its single permitted retry. |
| `scenario2-data-quality` | `flagged_data_quality` | A duplicate-key error that does not say which rows are at fault. The data quality agent scans the source table deterministically and returns evidence. |
| `scenario2b-known-issue` | `duplicate_suppressed` | The same alert arrives twice. The second run matches the open incident by signature and stops: it does not re-flag and does not re-notify. |
| `scenario3-policy-block` | `needs_human` | Identical to scenario 1 until the agent, having already refreshed once, decides to refresh again. The controller refuses, and the refusal is returned to the agent as a tool result so it can still finish properly. |
| `scenario4-unknown-action` | `needs_human` | The agent proposes `delete_dataset`, which is not on any allowlist. It is never dispatched; the agent receives a refusal and adapts. |
| `scenario5-approval-granted` | `resolved` | The same gateway failure three days running. Another refresh would reproduce it, so the agent proposes a rebind — a fix with a wider blast radius than it may apply on its own — and a human approves. |
| `scenario6-approval-denied` | `approval_denied` | Identical to scenario 5 until the decision. The human says no and the fix is never applied. The run ends as `approval_denied`, not as a failure or a false success. |
| `scenario7-schedule-reenable` | `resolved` | Power BI disabled a refresh schedule itself after four consecutive failures and nothing switched it back on. |
| `scenario8-capacity-backoff` | `deferred_retry` | The capacity is saturated. Retrying is the obvious move and the wrong one, because it adds load to the thing already over its limits. |
| `scenario9-pipeline-authentication` | `needs_human` | A scheduled SQL copy is denied access. An unchanged rerun cannot correct authorization. |
| `scenario10-pipeline-rerun-approved` | `resolved` | An ADLS internal service error. Reviewed replay parameters plus an explicit approval permit exactly one new run. |
| `scenario11-pipeline-rerun-denied` | `approval_denied` | A retry-candidate failure and a reviewed configuration do not override a human denial. |
| `scenario12-pipeline-schema-mismatch` | `needs_human` | The source file has more columns than the mapping. Approval is not even requested for a rerun that would reproduce the mismatch. |
| `scenario13-pipeline-rerun-pending` | `needs_human` | Fabric accepts an approved rerun but has not completed it. An accepted job is not a resolution. |
| `scenario14-pipeline-write-timeout` | `needs_human` | An earlier activity succeeded and a SQL batch write timed out. Missing commit evidence must not be read as rollback. |

## Deploy into a tenant

Offline evaluation needs nothing. A live deployment needs several things that
have lead times, and publishing an agent or a web build does not establish any
of them.

**Prepare the Foundry project, the Azure SQL state database and the monitored
Fabric workspaces separately, before starting any runtime.** Provision the shared
application database, its public firewall admission, Entra-only authentication,
auditing and the reviewed schema and permissions first.
[`docs/DeploymentGuide.md`](./docs/DeploymentGuide.md) lists every prerequisite
in the order its lead time demands, and
[`docs/AzureAccountSetUp.md`](./docs/AzureAccountSetUp.md) covers the
subscription and tenant permissions needed before that.

A live deployment requires `MONITORING_MODE=live`, a pinned
`MONITORING_TENANT_ID`, `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, service
identity access and a deployment-owned schema. A live failure never falls back to
fixture state. `FABRIC_PIPELINE_TARGETS` is retired: live targets and safety
reviews come from the monitoring registry, with no environment-target loader and
no migration path from one.

The shipped baseline is **public networking with Entra authentication**. The
Bicep templates require no private endpoint, VNet, NAT Gateway or private DNS.
The prototype takes a clean start: no Fabric SQL compatibility layer, no state
migration, no dual writes and no mixed-version operation. An exact, guarded reset
is deployment work, not a browser or startup action.

> **Preview notice:** Some platform capabilities used here are in preview,
> including Foundry hosted agents, Entra agent identity and Foundry routines.
> These are provided "as-is" and may change without notice.

### Prerequisites and costs

To deploy, you need an [Azure subscription](https://azure.microsoft.com/free/)
with:

- **Contributor** role at the subscription level
- **Role Based Access Control (RBAC)** permissions to assign roles at the
  subscription and/or resource group level
- The ability to create resource groups, resources and app registrations

Use one Azure SQL application database. A temporary isolated proof database may
share its logical server, but never its operational records. Select and approve
SQL compute, storage, backup and network costs before provisioning or resizing.
The shipped SQL template uses one S1 application database and an optional Basic
proof database, with no elastic pool; an elastic pool needs a measured cost and
load justification, not a default. This evaluation topology is not a final sizing
or pricing recommendation. Fabric capacity is still needed for the monitored
Fabric workloads and Eventstream, but not for application-state storage.

> **Note:** This pricing overview is not comprehensive. Actual costs vary with
> your selected SKUs, usage scale, customizations and tenant integrations. Use
> these estimates as a starting point.

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
| [Application Insights](https://learn.microsoft.com/azure/azure-monitor/app/app-insights-overview) | Entra-authenticated, app-owned metadata telemetry; paired heartbeat records have been queried natively. Foundry project content tracing remains disconnected. | [Pricing](https://azure.microsoft.com/pricing/details/monitor/) |
| [Microsoft 365 / Exchange Online](https://learn.microsoft.com/exchange/exchange-online) | Optional mailbox ingestion for Power BI failure alerts; not a dependency of hybrid polling or Eventstream intake. | [Pricing](https://www.microsoft.com/microsoft-365/business/compare-all-microsoft-365-business-products) |
| [Microsoft Teams](https://learn.microsoft.com/microsoftteams/) | Optional. Receives notification and approval cards. | [Pricing](https://www.microsoft.com/microsoft-teams/compare-microsoft-teams-business-options) |

> ⚠️ **Important:** Remove resources created for an evaluation when it ends.
> Review the scope of `azd down` or a resource-group deletion first: the Foundry
> project, Fabric workspace and state database may predate this deployment.
> The optional command center is deployed separately and needs separate cleanup.
> Review ownership of the monitoring worker and connector items separately;
> source workspaces, models, pipelines and business data are not disposable
> monitoring infrastructure.

### Scheduling: what does and does not work

Foundry routines are the native scheduled trigger, and in this tenant they did
not work. In verification on 2026-09-02, six days after registration, the routine
reported itself enabled with its cron, accepted dispatches, produced no runs, and
telemetry showed agent activity in two of twenty-four hours — both of them hours
when a person invoked it by hand. `azd deploy` does not manage routines at all.

Both routines are therefore declared in `azure.yaml` and **ship disabled**, with
the evidence recorded in the file. The scheduled trigger the accelerator actually
supports is [`infra/scheduled-sweep.json`](./infra/scheduled-sweep.json), a
Consumption Logic App with a managed identity, which defaults to the `heartbeat`
command. Re-test routines in your own tenant before enabling them; the observed
behaviour may be regional or fixed in a later preview release. An enabled
declaration is not evidence that a schedule has executed.

Create the scheduler disabled, verify its identity and database prerequisites,
then enable it as part of the approved cutover. A successful idle heartbeat is
not proof of fresh inventory, polling coverage or event delivery.

The hosted controller drains both admitted monitoring work and human commands in
bounded rounds. The separate worker owns discovery, polling, connector
reconciliation and event intake, so there is no need to create another controller
timer per target.

The scheduler submits a **stored background response** and retains its response
ID in the workflow run. It polls that exact ID until completion, rather than
holding one HTTP request open. A queued or in-progress response is acceptance,
not success. Only a completed response without an error passes the final gate.
POST retries are disabled; a failed status read retries only the original ID.
The polling window is 15 minutes and scheduler runs are serialized.

Two deadlines govern a heartbeat, and they answer different questions:

- `HEARTBEAT_BUDGET_SECONDS` (default 840) is the **admission** deadline: whether
  there is still time for an admitted unit to finish. It is monotonic and
  includes lock wait. A value too short to hold one work allowance is refused at
  startup rather than silently admitting nothing.
- `HEARTBEAT_RESPONSE_SECONDS` (default 100) is a **soft stop for starting more
  work**. It does not bound an operation already running and is not a substitute
  for asynchronous hand-off. Logic Apps Consumption caps a synchronous outbound
  request at 120 seconds regardless of an action's configured timeout.

Reaching either deadline stops new claims; it never cancels admitted work, which
is protected by its lease. The heartbeat runs two automatic slots — one leading
deterministic reconciliation, one leading actions — and one human-command slot,
refilled within queue limits and the remaining allowance.

Background execution survives the submitting client's disconnect. It does not
provide automatic replay of this custom controller after process loss; recovery
still follows the original SQL work, receipts and fences. Never resubmit an
uncertain action because a response is missing.

Portal-only heartbeat health alerts include an optional runtime log query that
returns a zero-count row when no completed heartbeat is present; absent platform
metric samples are not assumed to be zero.

### Hybrid monitoring scope and limits

Discovery is an inventory of resources, not all Fabric operational telemetry.
Being precise about the edges of that is more useful than a feature list.

**Only semantic models/datasets and Data Pipelines have failure-monitoring
contracts** in this implementation. Other discovered types — Notebook, Report,
Lakehouse, Warehouse — stay visible with an unsupported reason. Notebook
activities can be evidence within a monitored pipeline, but standalone notebook
jobs are not monitored. Missing expected starts, disabled pipeline schedules,
tenant-wide data quality, report usage, audit and capacity monitoring all need
additional detectors or collectors.

**Scopes use the deployment tenant** and named, ID-backed domain, workspace and
item metadata. Domain descendants are optional and exclusions win. A domain is
not an access grant. Caller-visible inventory and incomplete scans must not be
presented as complete tenant coverage, and an inventory refresh queues work
rather than proving discovery, source access or delivery completed.

**Names do not identify targets.** Identity is tenant, epoch, workload, workspace
and item, and a source execution ID is kept distinct from the controller's
submitted job ID.

**A safety review is not an approval.** An Admin's target safety review
configures one action profile. It is not an Approver's fingerprint-bound,
unexpired, single-use decision for an individual remediation. A saved or
requested `verified` review does not replace current service capability and
definition checks, and a new target never inherits another target's replay
attestation.

**A submitted job is not a resolution.** Full-pipeline reruns require reviewed
replay safety, human approval and a durable reservation before the POST, and the
correlated job and activity evidence must then be verified.

**Source removal is fenced, not immediate.** Source additions begin as logical
proposals with no physical source ID, and only the original verified worker
observation can bind the returned owned component IDs. A desired removal fences
intake at once but retains ownership until an original complete receipt proves
the exact node, ID and stream-route absence; retirement leaves an immutable
record. A timeout or an inherited snapshot is not removal proof.

The hybrid path requires neither Activator, Power Automate nor Eventhouse. It
uses REST plus native Fabric Job Eventstream delivery, and Power BI polling
remains required. The selected custom endpoint uses public outbound TLS with
Entra authentication, which does not require public inbound access to the worker
or opening the SQL, model or web resources.

Key-free endpoint automation remains unproved. Take initial nonsecret endpoint
metadata from the Custom Endpoint's **Microsoft Entra ID** tab; do not call the
key-returning connection API.

See [Monitoring setup](./docs/CommandCenter.md#monitoring-setup) and
[pipeline triage](./docs/PipelineTriage.md).

## Security guidelines

The accelerator authenticates with
[Managed Identity](https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/overview)
and Entra agent identity wherever the platform allows it. The controller reaches
Power BI and the shared Azure SQL Database as itself, with no key and no stored
SQL password.

**Azure SQL does not impose Entra-only authentication by default.** This
deployment must explicitly enable **Microsoft Entra-only authentication** on the
logical server. The application has no SQL-login or credential-string fallback.
The public SQL endpoint uses TLS 1.2 minimum, Proxy/TCP 1433, auditing and TDE.

The default `allowAzureServices=true` creates the special SQL firewall rule whose
start and end addresses are both `0.0.0.0`. That rule admits Azure-hosted callers,
including ones in other subscriptions — it does not admit all Internet IPs, and
Entra SQL permissions remain mandatory regardless. Optional exact IPv4 client
ranges provide additional network admission. Public reachability does not grant
anonymous API, SQL, registry or Foundry access.

The command center backend validates the delegated API token and its role claims
on every request. App roles grant no Azure, Fabric or directory permissions to
the user, the controller or the reasoning agents. **Refresh permissions** requests
a fresh token and reloads access details and the snapshot; mutation controls stay
locked until the new permission-refresh generation has a successful snapshot, and
earlier records cannot restore a capability. It does not revoke tokens already
issued in other sessions, so roles can remain valid until token expiry or
renewal, within the backend's 30-second validation leeway. Authorization needs no
runtime Graph directory permission; the optional profile photo uses a separate
delegated Graph `User.Read` token.

Hosted telemetry is kept separate from Foundry project tracing. The application
uses `TRIAGE_TELEMETRY_CONNECTION_STRING` with managed identity and metadata-only
instrumentation, and does not fall back to the platform-reserved locator.
Connecting Application Insights to the Foundry project would enable tracing across
agents that may contain prompts and responses, so that connection stays absent.
See [metadata-only telemetry and bounded native proof](./docs/DeploymentGuide.md#8-observability).

Ordinary templates contain no baked-in MCAPS exemptions. In the approved MCAPS
evaluation, `SecurityControl=Ignore` and its reason and review tags are scoped to
the SQL server, with a separate registry exception. The single 14-day period does
not restart when a tag is removed and re-added; longer tests need an approved
exclusion. See
[network controls and exceptions](./docs/DeploymentGuide.md#governed-evaluation-exceptions).

The source retains two credential-bearing inputs for older optional integrations.
They are not supported shortcuts for the secretless hybrid deployment:

| Credential | Why it exists | Scope |
|---|---|---|
| Mailbox app registration client secret | The tested hosted Entra agent identity was rejected by Exchange for app-only mailbox reads. This is separate from command-center sign-in. | One mailbox, enforced by an Exchange `ApplicationAccessPolicy`. |
| Teams Workflows webhook URL | The URL *is* the credential; there is no identity on an incoming webhook. | One channel. |

Do not populate them to make hybrid preflight appear ready. Keep any existing
values out of the repository and track their expiry and failure during
retirement. A secret-based mailbox path fails in tenants that remove app secrets
on a 30-day schedule: use a separately verified workload-identity path, or leave
mail off. Legacy Teams approval links are not Entra-authenticated decisions; web
proposals require the command-center decision path.

**Never commit a filled-in `.env`, a webhook URL, or any credential.** A
credential scanner runs in CI on every change. For your own repository, enable
[GitHub secret scanning](https://docs.github.com/code-security/secret-scanning/about-secret-scanning),
and consider
[Microsoft Defender for Cloud](https://learn.microsoft.com/azure/defender-for-cloud/).

> ⚠️ **Note:** The sample data in this repository is synthetic and intended for
> evaluation only.

## Verification status

This section states what has and has not been proved in the reference
deployment, so that nothing here is read as an acceptance claim.

**SQL contract review and source integration are not deployment acceptance.**
Earlier Fabric SQL CREATE and rollback checks, and an isolated managed-identity
event canary, are historical evidence rather than proof of the Azure SQL target.
The canary received all four observed wire event types across manual failure,
scheduled failure, success and cancellation; it did not prove durable SQL
handling or normal-worker recovery.

**SQL recovery is complete.** Append-only adjudication preserves the original
failed `STARTED` receipt and binds its approved replacement. Schemas are
committed, and the command center and hosted controller use Azure SQL with the
correct acting-identity grants. Maintenance is now `false`; the earlier
`maintenance=true`, revision `0` bootstrap capture is historical.

**A no-ingress collector-only worker is deployed.** Native Fabric metadata reads
durably accepted 332 workspaces, and the authenticated workspace selectors are
enabled. Item coverage remains partial, including source-access 401s and
request-budget and throttling gaps. Metadata discovery grants neither source
access nor remediation authority; no scopes or actions were auto-admitted.

**Scheduling and telemetry are partially proved.** The existing one-minute
scheduler calls `heartbeat`, with three actual recurrence responses verified
completed. Portal-only missing and failed heartbeat alerts have no configured
delivery destination. The runtime log-absence rule was tested with an isolated
query canary that fired and resolved, not by stopping the controller; the
production rule is enabled with no actions, and no actual controller stop or
email, webhook or Action Group delivery was tested. Native Application Insights
queries contain `heartbeat_started` and completed `heartbeat_finished` metadata,
with queue counts and zero exporter failure and warning counters in the captured
runs. That proves bounded metadata ingestion, not full event, overnight or
end-to-end acceptance.

**Event intake acceptance remains open.** Collector-only mode runs no Eventstream
receiver or provisioner. Eight bounded role cases and 19 SELECT controls are not
the full 27-RPC matrix. Earlier private proofs and screenshots remain historical,
and the private Foundry error does not block this public architecture.

Follow the [release gates](./docs/DeploymentGuide.md#release-gates) and the
[approved implementation and acceptance plan](./docs/HybridMonitoringPlan.md).

## Customize the solution

The policy ledger, action allowlists, approval gate and typed evidence are
reusable patterns. The tools and evidence checks are workload-specific and will
need replacing for another domain.

1. Read [`docs/TechnicalArchitecture.md`](./docs/TechnicalArchitecture.md) to see
   where decisions are made. The short version: the controller decides, the
   agents advise, and policy is enforced in code rather than in a prompt.
2. Review [`src/triage/tools/registry.py`](./src/triage/tools/registry.py). Every
   tool the agents can call is declared there in `TRIAGE_TOOLS`, and
   `ToolDispatcher` is what refuses anything that is not on an allowlist.
3. Review [`src/triage/policy.py`](./src/triage/policy.py) for the budgets and
   limits, and for the three allowlists — `REMEDIATION_ACTIONS`,
   `REPORTING_ACTIONS` and `DIAGNOSTIC_ACTIONS` — and set yours.
4. Review [`src/triage/knowledge/playbooks.py`](./src/triage/knowledge/playbooks.py)
   and replace the playbooks with the failure modes your estate actually has.
   Each needs triggers, a `retry_useful` verdict and a **public** Microsoft Learn
   source. Retrieval is capped at three: if a new entry matters more than an
   existing one, raise its trigger specificity rather than the cap.
5. Review the scenarios in [`scenarios/`](./scenarios) and the sample data in
   [`mock/`](./mock).
6. Work through [`docs/CustomizationGuide.md`](./docs/CustomizationGuide.md),
   which covers adding a tool, an approval gate, a playbook, a detector, an
   agent, and moving the whole solution to a different domain.

Adding a remediation tool follows a fixed path: a schema in `TRIAGE_TOOLS`, a
branch in `ToolDispatcher._execute`, the name on an action allowlist, a scenario,
and a test. Prompts are hashed onto triage incidents, so if you change a prompt
or a tool schema while running in Foundry mode, **re-register the agents** or the
change has no effect.

## Supporting documentation

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

### Similar solution accelerators

| Solution Accelerator | Description |
|---|---|
| [Microsoft IQ Solution Accelerator](https://github.com/microsoft/microsoft-iq-solution-accelerator) | Unifies enterprise data, knowledge and workflows across Fabric IQ, Foundry IQ and Work IQ to support operational decisions. |
| [Real-Time Intelligence for Operations](https://github.com/microsoft/real-time-intelligence-operations-solution-accelerator) | A real-time intelligence platform for manufacturing operations, with anomaly detection and a conversational data agent. |
| [Unified Data Foundation with Microsoft Fabric](https://github.com/microsoft/unified-data-foundation-with-fabric-solution-accelerator) | Unified data foundation with options to integrate Azure Databricks and Microsoft Purview. |

## Feedback, transparency and disclaimers

Have questions, find a bug, or want to request a feature?
[Submit a new issue](https://github.com/SQLBImhugh/foundry-fabric-triage-solution-accelerator/issues)
on this repo and we'll connect.

See [Transparency FAQ](./TRANSPARENCY_FAQ.md) for responsible AI transparency
details of this solution accelerator.

### Disclaimers

This is an independently maintained sample. It is not a Microsoft product, is
not affiliated with or endorsed by Microsoft, and carries no support or service
level agreement of any kind. It is provided "as is", per the
[MIT licence](./LICENSE).

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
