# Customization

Adapting this to your failures, your tools and your risk tolerance.

The structure generalises past Power BI. What makes it work is the separation:
reasoning agents hold prompts and no permissions, the controller holds every
limit and every credential, and deterministic detectors produce the evidence.
Keep that split and most of this document is mechanical.

## Decide the tier before writing any code

Every action belongs in one of three tiers, and the tier decides where the code
goes:

| Tier | Meaning | Where it lands |
|---|---|---|
| 1 | Transient and idempotent. Safe unattended. | `REMEDIATION_ACTIONS` |
| 2 | Deterministic fix, real blast radius. Human approves first. | `REMEDIATION_ACTIONS` + an approval gate |
| 3 | Never automate. Report with evidence. | `REPORTING_ACTIONS` |

Getting this wrong is the expensive mistake. The test: if this action ran at
03:00 with nobody watching and the diagnosis was wrong, what is the damage? If
the answer is "we retry it", Tier 1. If it is "we restore from backup", Tier 3.

Duplicate rows are Tier 3 here for that reason. Deleting the wrong duplicate is
unrecoverable, and the agent cannot know which row is authoritative.

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

## Add a playbook

Entries live in `knowledge/playbooks.py`: triggers, a `retry_useful` verdict, and
a public Microsoft Learn source.

`retry_useful` is the field that changes the decision. "Credentials expired" and
"capacity throttled" both surface as "refresh failed", but retrying fixes one and
wastes capacity on the other.

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

Mirror `DataQualityAgent`: its own provider, prompt and tools, returning a typed
Pydantic model, exposed to the orchestrator as a single tool.

It **reports**; the orchestrator decides. Do not give a reasoning agent the
ability to act — permissions belong to the component that acts, not the one that
reasons. Each agent gets its own identity, so each needs its own grant. If a new
reasoning agent seems to need a permission, the design is wrong.

New agents share the same `PolicyLedger`. Budgets are per incident, not per
agent, so a second agent does not double the ceiling.

## Change the model

`FOUNDRY_AGENT_MODEL` for the Foundry path, `AZURE_OPENAI_DEPLOYMENT` for the
direct path. See [`FAQs.md`](FAQs.md) for what the choice
affects and what to do when a deployment is unavailable.

After any prompt or tool-schema change, re-register:

```powershell
python scripts\register_foundry_agents.py
```

A Foundry-registered agent does not pick up local changes. Without this the run
looks unaltered, which is worse than an error.

## Change the trigger

The mailbox is one entry point, not the design. `runner.py` exposes triage as a
function; anything that can call it can be a trigger — a webhook, a queue, a
Fabric event, a schedule.

Whatever the trigger, keep an equivalent of the inbox filter. It fails closed,
including when its own pattern is invalid, and counts what it ignored rather
than dropping it silently. Without it, anyone who can reach the trigger can
steer the agent.

## Move to a different domain

The reusable parts are `policy.py`, `approvals.py`, `signature.py`,
`redaction.py`, the incident store and outcome validation. None of them know
anything about Power BI.

What you replace: the tools, the playbooks, the detectors and the prompts.

What you should not replace: the rule that the controller owns every limit. A
limit that exists only as prompt wording is not a limit, and the first thing a
capable model does with an ambiguous instruction is find the reading that lets
it proceed.
