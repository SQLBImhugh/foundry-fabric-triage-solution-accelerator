# FAQs

## Can I run it without an Azure subscription?

Yes, and you should start there. With no configuration at all the accelerator
runs against a deterministic provider and in-memory tools:

```powershell
.\.venv\Scripts\bi-triage.exe list
.\.venv\Scripts\bi-triage.exe run scenario1-transient
```

That is the same path the test suite uses. No credentials, no network.

## What does it actually change in my tenant?

Only what is on an allowlist, and only within budget. By default that is one
write action per incident, drawn from a short list: trigger a refresh, re-enable
a disabled refresh schedule, defer a retry. Everything else is refused before
dispatch and escalated to a human.

Set `TRIAGE_MAX_WRITE_ACTIONS=0` to make it read-only while you evaluate it.

## Which model should I use?

Any model that supports function calling. `FOUNDRY_AGENT_MODEL` selects it.

The choice matters less than it looks, because the controller — not the model —
decides what is permitted. A stronger model classifies causes and writes
explanations better; it does not gain authority. When evaluating a swap, run the
scenarios and compare the tool sequences, not the prose.

Keep a second agent pair registered on a fallback model if you depend on this
running unattended. Model deployments can be throttled or retired.

## Why is the policy in code instead of the prompt?

Because a prompt is a request and code is a control. A model can be argued out of
prompt wording by unusual input; it cannot be argued past a function that refuses
to dispatch an action that is not on a list.

Every limit in `PolicyLedger` has a test proving it fires.

## Why does the data quality agent not fix anything?

It reports; the controller decides. Splitting investigation from authority means
a wrong diagnosis produces a wrong *recommendation*, not a wrong *action*, and it
keeps the permission surface on one component instead of two.

## What happens if nobody answers an approval?

Nothing happens. A timeout is a refusal, and so is an error, a malformed reply
and having no approval gate configured at all. Silence is never read as consent.

A denial does not consume the remediation budget, so one "no" does not disarm the
agent for the rest of the incident.

## Why does it poll the mailbox instead of subscribing?

Graph change notifications need a public HTTPS endpoint that answers a validation
handshake, plus subscription renewal before expiry. Polling has no such
dependencies and its worst case is half the poll interval.

`GRAPH_INGESTION_MODE=subscription` is rejected at startup rather than silently
polling, because believing you have push while getting a poll is a latency
assumption nothing will correct.

## Why is the scheduled trigger a Logic App rather than a Foundry routine?

Because Foundry routines do not currently fire. Verified six days after
registration: the routine reported itself enabled, accepted dispatches, produced
no runs, and telemetry showed agent activity only in hours when a person invoked
it by hand. `azd deploy` does not manage routines either.

Both routines are declared in `azure.yaml` and ship disabled, with the evidence
in the file. Use [`infra/scheduled-sweep.json`](../infra/scheduled-sweep.json),
which is verified end to end. Re-test routines in your own tenant before
enabling them.

## What is a "silent failure" and why does it need its own detector?

A refresh that reports success while the source never landed, or a table that
loads a tenth of its rows. No alert is raised, so no alert can be triaged, and
the report is simply wrong until somebody notices.

The detector queries semantic models directly on a schedule and compares against
a recorded baseline. It uses none of the agent tools, deliberately: a model asked
whether a 60% row drop is acceptable will sometimes say yes.

It is off until `SILENT_HEALTH_PROBES` is configured, because "fresh" is a
business question per model and guessing it produces the false positives that get
a detector muted.

## Which models can the detector not see?

Direct Lake models, which do not support app-only callers, and models relying on
single sign-on or row-level security. Those are reported as detector faults
rather than as healthy, so an unmonitorable model is visible rather than assumed
fine.

## Can I use this for something other than Power BI?

Yes. The policy ledger, allowlists, approval gate, signature and suppression
logic, incident model and outcome validation are domain-independent. The Power BI
specifics are the tools, the playbooks and the parsing.

See [`CustomizationGuide.md`](CustomizationGuide.md), which has a section on
moving to a different domain.

## How do I know it is still running?

Nothing alerts on the agent having stopped unless you configure it to. Set
`alertWebhookUrl` when deploying the scheduled sweep and a failed run posts to
Teams; the Logic App also keeps its own run history, and a failed sweep
terminates as failed rather than being handled quietly.

This is not hypothetical. An unpinned dependency once crash-looped the container
at startup, and because nothing was watching, the agent answered nothing for
hours until someone invoked it by hand. The hosting library is pinned exactly
now, and a test fails if that pin is loosened.

The cheapest independent check is the incident store's newest timestamp:

```powershell
bi-triage incidents
```
