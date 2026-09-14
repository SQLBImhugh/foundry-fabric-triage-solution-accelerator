# BI Triage Solution Accelerator: Responsible AI FAQ

## What is the BI Triage Solution Accelerator?

This solution accelerator is a multi-agent operations loop for business
intelligence failures. It reacts to Power BI refresh failure alerts, determines
the cause from refresh history and semantic model state, and either applies a
remediation permitted by an explicit policy or escalates to a human with the
evidence attached. It also runs deterministic probes to find models that failed
without raising an alert. An optional monitor triages failures in explicitly
configured Fabric pipelines; it is not automatic discovery of every workspace
item or standalone notebook job.

## What can this Solution Accelerator do?

It supports evaluation and extension for Power BI operations and configured
Fabric pipelines. It can:

- classify the failure against a set of playbooks sourced from public Microsoft
  documentation
- read refresh history and semantic model state to establish what happened
- investigate data-shaped failures through a specialist data quality agent
- retry a refresh, re-enable a disabled refresh schedule, or defer a retry when a
  capacity is throttled — each only when the action is on an allowlist and within
  budget
- inspect configured pipeline runs and activity evidence, and request a rerun
  only after replay-safety review and explicit approval
- request explicit human approval for allowlisted actions that require it;
  approval cannot authorize an unknown or disallowed tool
- persist terminal outcomes and optionally notify Teams, with repeated incident
  announcements suppressed by the controller

The separate [agent command center](docs/CommandCenter.md) adds authenticated
approvals, queued investigations, incident notes, run history and persisted
read-only discussion. Recording **Resolved by user** is a tracking decision,
not proof of repair: it does not reset the remediation budget, release a claim
or approve an action, and newer evidence invalidates the older closure.
The existing [Fabric cockpit](cockpit/README.md) remains read-only.

## What is this Solution Accelerator's intended use?

It is intended for teams operating Power BI and explicitly configured Fabric
pipelines who want bounded first-line triage with recorded evidence. It is
MIT-licensed sample code, provided to be read, adapted and evaluated in your own
environment. It is not a supported product.

## How was the Solution Accelerator evaluated? What metrics are used to measure performance?

The canonical suite consists of 15 scenarios, each a reproducible case with
expected actions and outcomes, and those expectations are executed as tests.
The suite runs entirely offline against deterministic providers and mock tools,
so behaviour is reproducible rather than sampled. Policy limits, approvals,
redaction, action allowlists, silent-failure detection and pipeline rerun
controls have dedicated tests, including negative controls that confirm a check
fails when the property it guards is removed. The command center's
administrator-only scenario validation also uses synthetic tools and isolated
state; it does not remediate monitored resources.

Evaluation covers the tested decision logic, not live model quality or every
tenant configuration. If you change the model or prompts, re-run the offline
scenarios and separately evaluate live behaviour against your own estate.

## Does this Solution Accelerator use Generative AI?

Yes, when a live provider is selected. Triage and data-quality reasoning agents
can use Azure AI Foundry or the direct Azure OpenAI chat completions path.
`FOUNDRY_AGENT_MODEL` selects the model for registered Foundry agents; the direct
path uses `AZURE_OPENAI_DEPLOYMENT`. The command center also has a separate
read-only observer for questions about recorded evidence. The default offline
mock provider makes no model calls.

Generative AI is used for classification and explanation. It is **not** used to
decide what the system is permitted to do. Action allowlists, budgets, the
approval gate and the silent-failure thresholds are enforced in code, and
deterministic evidence outranks model output whenever the two disagree.
The observer has no remediation tools, and an answer is not an approval.

## What are the limitations of the Solution Accelerator? How can users minimize the impact of these limitations?

- **A model can be wrong about a cause.** Remediation is therefore bounded by an
  allowlist and a write-action budget, and every incident records the evidence it
  acted on.
- **The alert may not contain the reason.** The accelerator reads refresh history
  rather than trusting the subject line, and a tool that cannot get an answer
  fails loudly rather than returning something a model can misread.
- **The silent-failure detector cannot see every model.** Its app-only probes
  do not cover Direct Lake models or models relying on single sign-on or
  row-level security. Unsupported targets are reported as detector faults
  rather than as healthy.
- **Pipeline reruns can repeat downstream writes.** A transient error alone is
  not enough to permit a rerun. Targets must be configured and reviewed for
  replay safety, and a rerun needs explicit approval. An accepted request is
  not evidence that the rerun completed; uncertain write outcomes require
  reconciliation rather than another automatic attempt.
- **Scheduled Foundry routines ship disabled** because the recorded evaluation
  did not establish that they fired reliably. A Logic App scheduler is supplied
  instead; verify scheduling in the target deployment rather than assuming it.
- **The current live mailbox controller requires a conventional app registration
  and `GRAPH_CLIENT_SECRET`.** The evaluated Exchange path did not accept the
  Entra agent identity for app-only mailbox access. This is a credential-lifecycle
  dependency, not a secretless deployment guarantee: expiry or tenant policies
  that purge client secrets will interrupt ingestion. Review that limitation
  before unattended use.
- **The Fabric cockpit is a delayed read model.** Mirroring, semantic-model
  visibility and query caching can lag controller state. It does not replace the
  command center's authenticated action controls.
- **Permission changes are not instant revocation.** The command center checks
  Entra application roles from access tokens. Already-issued tokens may retain
  earlier roles until replaced or expired; refreshing one user's session does
  not revoke another user's token.

Minimize impact by setting policy limits deliberately, keeping the approval gate
configured, restricting the sender allowlist and subject filter to the alert
sources you trust, and reviewing terminal outcomes.

## What operational factors and settings allow for effective and responsible use of the Solution Accelerator?

- Keep every action on an allowlist, and add to it deliberately.
- Keep `TRIAGE_MAX_WRITE_ACTIONS` low. The default is one action per incident.
- Configure the documented web approval path or the Teams/CLI approval path.
  Web proposals cannot be answered through the legacy bearer-link callback.
  Only explicit, fingerprint-matched, unexpired and unused approval authorizes
  a gated action; a denial does not consume the remediation budget.
- Restrict `GRAPH_SENDER_ALLOWLIST` and `GRAPH_SUBJECT_PATTERN` to real alert
  senders. Widening them to make something trigger makes the agent steerable.
- Scope mailbox access with an Exchange ApplicationAccessPolicy, and keep the
  canary mailbox check enabled so a missing policy fails startup.
- Keep durable hosted state in the standalone Fabric SQL database, accessed
  with Entra tokens. Neither web app owns its lifetime; do not delete it when
  replacing an app.
- Manage command-center access through Entra security groups assigned to
  Reader, Operator, Approver or Admin app roles. **Access & permissions** is
  read-only; there is no SQL-backed user editor or runtime directory-management
  permission. Group-based application assignment requires Entra P1/P2 and does
  not cascade through nested groups.
- Review telemetry. Spans carry metadata only; prompt and completion content is
  never attached.

See the [deployment guide](docs/DeploymentGuide.md),
[pipeline triage guide](docs/PipelineTriage.md) and
[command-center guide](docs/CommandCenter.md) for configuration and limits.
