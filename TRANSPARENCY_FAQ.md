# BI Triage Solution Accelerator: Responsible AI FAQ

## What is the BI Triage Solution Accelerator?

This solution accelerator is a multi-agent operations loop for business
intelligence failures. It reacts to Power BI refresh failure alerts, determines
the cause from refresh history and semantic model state, and either applies a
remediation permitted by an explicit policy or escalates to a human with the
evidence attached. It also runs deterministic probes to find models that failed
without raising an alert.

## What can this Solution Accelerator do?

It is ready to deploy for evaluation, adoption and extension in Power BI
operations. Given a failure alert it can:

- classify the failure against a set of playbooks sourced from public Microsoft
  documentation
- read refresh history and semantic model state to establish what happened
- investigate data-shaped failures through a specialist data quality agent
- retry a refresh, re-enable a disabled refresh schedule, or defer a retry when a
  capacity is throttled — each only when the action is on an allowlist and within
  budget
- request explicit human approval for actions outside that allowlist
- notify a Teams channel and persist a terminal outcome for every incident

## What is this Solution Accelerator's intended use?

It is intended for teams operating a Power BI estate who want first-line triage
of refresh failures to happen automatically and auditably. It is sample code,
provided to be read, adapted and deployed into your own environment. It is not a
supported product.

## How was the Solution Accelerator evaluated? What metrics are used to measure performance?

The accelerator ships nine end-to-end scenarios, each a reproducible case with an
expected tool sequence and outcome, and those expectations are executed as tests.
The suite runs entirely offline against a deterministic provider, so behaviour is
reproducible rather than sampled. Policy limits, the approval gate, redaction,
the action allowlists and the silent-failure detector each have dedicated tests,
including negative controls that confirm a check fails when the property it
guards is removed.

Evaluation covers the decision logic, not model quality. If you change the model
or the prompts, re-run the scenarios and evaluate against your own estate.

## Does this Solution Accelerator use Generative AI?

Yes. Two prompt-based agents run on Azure AI Foundry: a triage controller agent
and a data quality agent. The default model is configurable
(`FOUNDRY_AGENT_MODEL`), and an Azure OpenAI chat completions path is also
supported.

Generative AI is used for classification and explanation. It is **not** used to
decide what the system is permitted to do. Action allowlists, budgets, the
approval gate and the silent-failure thresholds are enforced in code, and
deterministic evidence outranks model output whenever the two disagree.

## What are the limitations of the Solution Accelerator? How can users minimize the impact of these limitations?

- **A model can be wrong about a cause.** Remediation is therefore bounded by an
  allowlist and a write-action budget, and every incident records the evidence it
  acted on.
- **The alert may not contain the reason.** The accelerator reads refresh history
  rather than trusting the subject line, and a tool that cannot get an answer
  fails loudly rather than returning something a model can misread.
- **The silent-failure detector cannot see every model.** Direct Lake models do
  not support app-only callers, and models relying on single sign-on or
  row-level security are out of scope. Those are reported as detector faults
  rather than as healthy.
- **Foundry routines do not currently fire**, so the native scheduled trigger
  ships disabled and a Logic App is provided instead.
- **Mailbox ingestion needs a conventional app registration**, because Exchange
  does not yet accept an Entra agent identity for app-only mailbox access. That
  credential expires, and it is the only one in the deployment.

Minimize impact by setting policy limits deliberately, keeping the approval gate
configured, restricting the sender allowlist and subject filter to the alert
sources you trust, and reviewing terminal outcomes.

## What operational factors and settings allow for effective and responsible use of the Solution Accelerator?

- Keep every action on an allowlist, and add to it deliberately.
- Keep `TRIAGE_MAX_WRITE_ACTIONS` low. The default is one action per incident.
- Configure `APPROVAL_CALLBACK_URL` or answer approvals from the CLI. With no
  gate configured, every gated action is refused rather than allowed.
- Restrict `GRAPH_SENDER_ALLOWLIST` and `GRAPH_SUBJECT_PATTERN` to real alert
  senders. Widening them to make something trigger makes the agent steerable.
- Scope mailbox access with an Exchange ApplicationAccessPolicy, and keep the
  canary mailbox check enabled so a missing policy fails startup.
- Review telemetry. Spans carry metadata only; prompt and completion content is
  never attached.
