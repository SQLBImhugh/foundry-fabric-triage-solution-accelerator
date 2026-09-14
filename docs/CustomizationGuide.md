# Customization

Adapt the failure types, tools and policy while preserving the separation of
responsibilities: reasoning agents interpret evidence without service
permissions, the controller enforces action limits, and deterministic detectors
produce measurements. Live components authenticate with their own Entra
identities; a command-center user's app role is not a controller service grant.

## Decide the tier before writing any code

Classify the proposed action before exposing it. The first column is a value
of `TriageClassification.tier`, available through `TriageResult.classification`
when a classification was recorded. There is no `TriageResult.tier` field or
`tier_3` value. Classification does not itself authorize a tool:

| `tier` value | Meaning | Where it lands |
|---|---|---|
| `tier_1` | Transient and idempotent. Safe unattended. | `REMEDIATION_ACTIONS` |
| `tier_2` | Deterministic fix, real blast radius. Human approves first. | `REMEDIATION_ACTIONS` + an approval gate |
| `needs_human` | No suitable permitted automation. Escalate with evidence. | Reporting tools only; do not add the unsafe action to an allowlist |

An approval gate decides *whether a permitted action runs*. It never authorises
an action that is off the allowlist: approval-gated actions are a subset of
`REMEDIATION_ACTIONS`, so there is no path by which saying yes widens what the
agent can do.

Evaluate the consequence of a wrong diagnosis before classifying an action as
safe unattended. Retrying a confirmed transient failure differs from deleting
data that might require restoration from backup. Duplicate deletion is outside
the allowlist here: the agent cannot know which row is authoritative. It can
report and flag duplicate evidence, not repair the underlying records.

## Add a remediation tool

Five steps, all required:

1. **Schema** in `TRIAGE_TOOLS` (`tools/registry.py`). Describe what it does and
   what it affects — the model sees only this.
2. **Branch** in `ToolDispatcher._execute`. Validate inputs at the boundary and
   fail loudly. Calling Power BI with an empty id returned 404, and the model
   turned that into a confident, wrong conclusion. A plausible answer built on a
   failed call is the worst available outcome.
3. **Allowlist** — add the name to exactly one of `REMEDIATION_ACTIONS`,
   `REPORTING_ACTIONS` or `DIAGNOSTIC_ACTIONS`. Anything not listed is refused
   before dispatch.
4. **Scenario** in `scenarios/`, with an `expect` block.
5. **Test**, including a negative control: a test that fails if the guard is
   removed.

Give the tool its own mock. The offline path is the evaluation path; a tool that
only works live cannot be demonstrated or tested.

Pipeline requests use `PIPELINE_ACTIONS` as well as the action taxonomy. Add an
action to that workload's set only after reviewing its effect on pipelines;
do not make dataset tools available to pipeline requests. The existing monitor
handles explicitly configured scheduled pipeline jobs. Notebook failures
inside their activity evidence do not imply standalone notebook monitoring.

## Add a preconditioned action

Some actions must not run when something else is true — the deferred-retry
window is the worked example: while a retry is postponed, an immediate refresh
is refused.

Add the name to `_PRECONDITIONED_ACTIONS` and implement `_precondition_failure()`.
The budget is charged **after** the precondition passes, not before. Charging
first means a refusal still spends the write budget, which silently disarms the
agent for the rest of the incident — the same bug the approval path already had.

## Add an approval gate

Approval is fail-closed by construction. A yes must be explicit, fingerprint-
matched to the exact proposed action, unexpired and unused. Everything else is a
no.

State the **blast radius** on the card. An approval that hides the consequence is
a rubber stamp, and the person clicking it is accountable for the result.

A denial must not consume the remediation budget, or one "no" disarms the agent
for the rest of the incident.

The current gated remediations are `rebind_dataset_gateway`,
`reenable_refresh_schedule` and `rerun_fabric_pipeline`. Schedule re-enablement
also requires successful refresh evidence. A pipeline rerun requires a reviewed
target and complete replay-parameter set, rechecked prerequisites after approval,
and a durable reservation before submission. Its correlated job and activity
evidence must confirm success; HTTP acceptance is not a resolution.

For web decisions, use the authenticated command-center API and its exact,
fingerprint-bound decision method. Do not accept a responder identity from a
request body or adapt the legacy bearer-link callback into an authentication
mechanism. App Approver or Admin permission controls who may answer, while the
controller still decides whether the approved action may execute.

## Add a playbook

Entries live in `knowledge/playbooks.py`: triggers, a `retry_useful` verdict, and
a public Microsoft Learn source.

`retry_useful` distinguishes retry candidates from failures needing a different
response. A transient timeout, expired credentials and capacity throttling can
all surface as "refresh failed": credentials need correction, throttling needs
backoff, and only a permitted transient failure justifies an immediate retry.

Retrieval is capped at three. If a new entry matters more than an existing one,
raise its trigger specificity rather than the cap — a larger prompt is not
retrieval.

**Source from public documentation.** Internal engineering runbooks are more
detailed, but they are written for engineers debugging the service and carry
owner and incident-management references. Use them to decide what matters; write
the entry from public docs. A test enforces this.

## Add a detector

Detectors are deterministic and are not agents. A model asked whether a 60% row
drop is acceptable will sometimes say yes.

`detectors/silent_failures.py` is the pattern: measure, compare against a stored
baseline, and return a typed finding. The controller decides what to do about it.

**False positives are the failure mode that matters.** A detector that cries wolf
gets turned off, and then it is not detecting anything. Design accordingly:

- Suspect, then confirm — two signals before raising.
- Use relative *and* absolute thresholds. A 50% drop from 4 rows is noise.
- Advance baselines only from healthy readings, or a degradation becomes the new
  normal.
- Keep detector faults strictly separate from data findings. "I could not check"
  is not "everything is fine", and must never be reported as such.

## Add an agent

Mirror `DataQualityAgent`'s boundary: a separate provider and prompt, a typed
Pydantic result, and one tool exposed to the orchestrator. Its current
implementation scans every registered table deterministically, then makes one
tool-free model call to interpret the selected evidence. A specialist does not
need a model-driven tool loop merely to qualify as an agent.

It **reports**; the orchestrator decides. Do not give a reasoning agent the
ability to act — permissions belong to the component that acts, not the one that
reasons. A separate identity does not imply a service grant. If a new reasoning
agent seems to need a permission, reconsider that boundary.

Specialists called by triage share its `PolicyLedger`, so a second agent does
not double the run's ceiling. Incident suppression and durable claims govern
repeat work across invocations; a new agent must not bypass them.

The command-center observer is separate from actionable triage. It answers from
authorized records, has no tools, and cannot approve, queue or execute work.
Keep human notes and prior answers in its untrusted evidence context, not in
system instructions. Persist discussion through the incident activity store.

## Extend the command center

Keep the React/Vite interface behind `triage.command_center.api`. New actions
need typed HTTP contracts, backend permission checks, durable command or
activity records and offline frontend/backend tests. A disabled button is not
an authorization boundary.

Reader can inspect records and ask questions. Operator adds investigations,
notes and user resolutions; Approver can answer approval requests. Operator and
Approver do not imply one another. Admin includes all app operations, scenario
validation and reconciliation, but no directory administration.

Access is managed through four ordinary Entra security groups mapped to the
existing `CommandCenter.*` app roles. Use
`scripts/register_command_center.py` and
`scripts/configure_command_center_groups.py` for registration and group setup,
with an authorized operator; neither grants directory authority to the app.
The Access & permissions page reports validated token roles, not memberships.
There is no SQL ACL to customize. The retired
`COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true` setting is rejected.

After **Refresh permissions**, keep actions disabled until a snapshot loaded
under the new token-refresh generation succeeds. A token's role claims are not
a continuous directory check, and group changes do not revoke existing tokens
immediately. Profile-photo consent uses a separate delegated Graph `User.Read`
token; it must not become a requirement for app authorization.

Human **Resolved by user** decisions append tracking activity bound to the
original SQL NVARCHAR payload's SHA-256 hash over UTF-16 LE bytes. Do not hash a
re-serialized model: defaults or whitespace can change the revision. New
controller evidence invalidates the closure, and neither closure nor notes may
reset incidents, approvals, claims, notification counts or remediation budgets.
Use the same tracking projection for the queue, counts and full incident record.

Preserve the shared safe Markdown renderer for explanations and answers:
no raw HTML, images or unsafe links. Presentation tokens live in
`command-center/src/styles.css`; the current design uses self-hosted DejaVu
Serif Condensed, Onyx dark/light colors and Ink-style geometry and shadows.
Keep the supplied PNG logo and packaged font assets unless a deliberate branding
change updates the asset checks too. The separate Rayfin cockpit remains
read-only and is not an alternative command writer.

## Change the model

Use `FOUNDRY_AGENT_MODEL` when registering Foundry agents and
`AZURE_OPENAI_DEPLOYMENT` for the direct path. Changing a local Foundry model
setting does not update an already registered agent. See [`FAQs.md`](FAQs.md)
for deployment availability and [`CommandCenter.md`](CommandCenter.md) for the
optional observer.

After any prompt or tool-schema change, re-register:

```powershell
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py
```

A Foundry-registered agent does not pick up local changes. Without this the run
looks unaltered, which is worse than an error.

## Change the trigger

The mailbox is one entry point. `TriageRunner.run_request` is also used by
interactive and queued requests; `pipeline_sweep` handles configured scheduled
pipeline failures. A webhook, queue or Fabric event would need an adapter with
equivalent authentication, target validation, filtering, claims and persistence.
These are extension points, not preconfigured triggers.

Whatever the trigger, keep an equivalent of the inbox filter. It fails closed,
including when its own pattern is invalid, and counts what it ignored rather
than dropping it silently. Without it, anyone who can reach the trigger can
steer the agent.

## Move to a different domain

Reuse the mechanisms in `policy.py`, `approvals.py`, `signature.py`,
`redaction.py`, the incident store and outcome validation. Their current action
names, signatures and evidence checks contain workload-specific choices;
review those rather than assuming they are domain-free.

What you replace: the tools, the playbooks, the detectors and the prompts.

Retain controller-owned limits, fail-closed approvals, durable concurrency
controls and deterministic outcome validation. Prompt wording cannot enforce
any of these boundaries.
