# Operations

Running it in production: what runs on a schedule, how to turn each
part off, and what to look at when it behaves unexpectedly.

For first-time setup see [`DeploymentGuide.md`](DeploymentGuide.md).

## What runs, and when

| Trigger | What it does | Off switch |
|---|---|---|
| `bi-triage-mailbox-sweep` Logic App, every 5 min | Filters new mail, triages, acts or escalates | Disable the Logic App, or unset `GRAPH_MAILBOX` |
| `bi-triage-silent-sweep` Logic App, hourly | Drains due retries, runs the silent-failure scan | Disable the Logic App, or `SILENT_SWEEP_ENABLED=false` |
| Approval reply | Applies or abandons a proposed Tier 2 action | Unset `APPROVAL_CALLBACK_URL` — with no gate configured, every gated action is refused |

Neither Logic App exists until you deploy it, and nothing runs on a timer until
you do.

**Foundry routines do not fire.** The routines declared in `azure.yaml` **ship
disabled**: a routine reports `enabled`, accepts dispatches, and never invokes
the agent. Verified 2026-09-02, six days after registration, by three
independent checks; see [`foundry/README.md`](foundry/README.md).
Re-test in your own tenant before enabling — this may be regional, or already
fixed.

The trigger the accelerator actually supports is a Logic App:
[`infra/scheduled-sweep.json`](../infra/scheduled-sweep.json). It authenticates
with a system-assigned managed identity, so there is no key anywhere, and it
keeps its own run history, so a sweep that fails is visible afterwards rather
than being a thing that quietly stopped.

Deploy it once per cadence. The two sweeps answer different questions:

```powershell
$ep = (azd env get-values | Select-String AZURE_AI_PROJECT_ENDPOINT) -replace '.*="(.*)"','$1'

# hourly: find models that failed without telling anyone
az deployment group create -g <rg> --template-file infra/scheduled-sweep.json `
  --parameters name=bi-triage-silent-sweep projectEndpoint=$ep `
               command="silent sweep" frequency=Hour interval=1 owner=<you>

# every 5 min: drain the mailbox, perform due retries
az deployment group create -g <rg> --template-file infra/scheduled-sweep.json `
  --parameters name=bi-triage-mailbox-sweep projectEndpoint=$ep `
               command="sweep" frequency=Minute interval=5 owner=<you>
```

Each deployment outputs a `principalId`, and that identity needs permission to
invoke the agent before it will do anything — see
[`DeploymentGuide.md`](DeploymentGuide.md). Until the grant lands, runs fail with 403,
which is the correct behaviour and looks exactly like it should in run history.

The mailbox sweep does **not** run the health scan — "what arrived" and "what is
quietly wrong" are different questions, and only the first has an alert behind
it. Deploy both or the detector never runs.

Hourly is enough for the second: a freshness probe on a daily model answers a
question that changes once a day, and `executeQueries` is capped at 120/minute
per user across every dataset, so polling hard makes the detector load on the
capacity it is watching.

The `command` parameter is constrained to the two values the controller
recognises. That constraint matters: the agent routes anything it does not
recognise to "triage this text as an alert", so a typo would not fail — it would
quietly triage the word `sweeep` as though it were a Power BI failure report,
every five minutes, and look like it was working.

The HTTP call does not retry. A sweep that times out is picked up by the next
scheduled run instead, because a retry overlapping an in-flight triage can post
twice before the first marks the message processed.

Anything that can make an authenticated HTTPS call will do instead — Windows
Task Scheduler, a cron job, a GitHub Actions schedule, an Azure Function timer.
Prefer one that **reports its own failures**; a scheduler that stops silently
reproduces the problem it was brought in to solve. The endpoint is in the azd
environment:

```powershell
azd env get-values | Select-String AGENT_BI_TRIAGE_CONTROLLER_RESPONSES_ENDPOINT
```

The agent is unchanged either way: the scheduled path and the interactive path
are the same code, so nothing needs rewriting when the platform catches up.

**Check that it is actually running.** Set `alertWebhookUrl` when deploying and
a failed sweep posts a card to Teams; leave it empty and failures are visible in
run history but nothing announces them. That gap is not hypothetical: an
unpinned dependency crash-looped the container at startup, and because nothing
watches, the agent answered nothing for hours until someone invoked it by hand.
The cheapest independent check is the incident store's newest timestamp:

```powershell
bi-triage incidents        # nothing new since yesterday on a busy mailbox is a signal
```

**Routine enabled-state is not managed by `azd deploy`.** Measured
2026-09-02, both directions: deploying with `enabled: false` in `azure.yaml` left
an enabled routine enabled, and a full rebuild left a disabled one disabled. A
newly declared routine is not created either — measured 2026-09-03. An earlier
version of this document said a deploy would silently re-enable a disabled
routine; that was observed once and no longer reproduces. Manage routines with
the CLI, and check afterwards rather than assuming:

```powershell
azd ai routine create <name> --file <manifest.yaml>   # --file is the only way to set `input`
azd ai routine disable bi-triage-schedule
azd ai routine show bi-triage-schedule -o json        # confirm; the deploy will not do it for you
```

**The silent-sweep off switch is configuration, not routine state**, for the same
reason. A deploy resets routine state; it does not reset an environment variable
you set deliberately.

## Budgets

Each run is bounded. These are charged by the controller before an action, so no
prompt wording raises them.

| Setting | Default | What it bounds |
|---|---|---|
| `TRIAGE_MAX_LLM_TURNS` | 14 | Reasoning turns per incident |
| `TRIAGE_MAX_TOOL_CALLS` | 20 | Tool calls per incident |
| `TRIAGE_MAX_WRITE_ACTIONS` | 1 | Remediations per incident |
| `TRIAGE_MAX_TOKENS` | 80,000 | Tokens per incident, across every agent |
| `TRIAGE_TIMEOUT_SECONDS` | 300 | Wall clock per incident |

Raising `TRIAGE_MAX_WRITE_ACTIONS` above 1 removes the property that most of the
safety argument rests on. If a scenario seems to need it, the action is probably
mis-tiered — a Tier 2 action needing a second step should be a single tool that
does both, so it is approved once with its real blast radius stated.

## Inspecting state

```powershell
bi-triage incidents            # what was seen, its signature, occurrence count
bi-triage approvals            # actions awaiting a human decision
bi-triage retries              # postponed retries and when they are due
bi-triage retries --drain      # perform the ones whose window has passed
bi-triage health               # scan for failures that raised no alert
bi-triage health --probes      # what is watched, and how
bi-triage health --baselines   # what healthy looked like last time
bi-triage health --preflight   # configuration that would silently detect nothing
bi-triage health --accept all  # accept a planned change as the new normal
bi-triage flags                # data quality findings, reported not fixed
bi-triage preflight            # configured vs missing, printing no secret values
```

Everything printed is already redacted: redaction happens inside the store
boundary, so a display path cannot forget it.

## When it behaves unexpectedly

**It did nothing when mail arrived.** The inbox filter is a security control and
fails closed. Check the sender against `GRAPH_SENDER_ALLOWLIST` and the subject
against `GRAPH_SUBJECT_PATTERN`. The run reports what it ignored and why, rather
than dropping it silently. Do not widen the filter to make it find something —
send a message that matches. An agent that acts on every message is steerable by
anyone who can email it.

**It triaged, but took no action.** Expected for anything above Tier 1. Check
`bi-triage approvals`: a Tier 2 action waits for an explicit human yes, and
timeout, error, malformed reply and no-gate-configured all read as a decline.

**It reported `needs_human` when it looked successful.** Outcome validation
downgraded it: the agent claimed a result the evidence does not support. The
incident records the claim and the contradiction.

**The same alert produced no second action.** Signature suppression. The second
occurrence increments a counter. Notification is deduplicated too — an incident
is announced once, not once per occurrence.

**A refresh was not attempted during a capacity incident.** Deliberate. A
throttled retry is postponed with exponential backoff, capped at three attempts,
rather than retried immediately and made worse. `bi-triage retries` shows when.

**The hosted agent starts and immediately fails.** Check the environment
variables it was deployed with. pydantic-settings JSON-decodes complex field
types in the environment source *before* any validator runs, so a malformed
value for such a field crashes the process at import — taking down mail triage,
approvals and remediation over one optional feature. This is why
`SILENT_HEALTH_PROBES` is typed `str` and parsed afterwards. Keep new
configuration fields simple for the same reason.

**A prompt or tool change had no effect.** Foundry-registered agents do not pick
up local changes. Re-register:

```powershell
python scripts\register_foundry_agents.py
```

## Verifying a deployment actually deployed

```powershell
azd deploy bi-triage-controller --no-prompt
azd ai agent invoke bi-triage-controller "sweep"
azd ai agent monitor bi-triage-controller
```

A deploy that finishes in ~25 seconds instead of the usual minute and a half
detected no source change and shipped nothing. Exit code 0 is not proof; invoke
it and read the result.

## Telemetry

With `APPLICATIONINSIGHTS_CONNECTION_STRING` set, each run emits OpenTelemetry
GenAI spans: the incident, each agent, each tool call, the policy decisions and
the terminal outcome.

Spans carry **metadata only** — never prompt or completion content. Traces are
retained and widely readable inside a tenant, and prompt content routinely
contains customer data pasted into an alert.

Without the connection string the instrumentation is a no-op, so nothing needs
disabling to run offline.

## Cost control

Model tokens dominate. The per-run token budget is the direct control: it bounds
the cost of a single incident, and signature suppression bounds how many times
the same failure can be paid for.

If cost rises unexpectedly, look for a signature that is not matching — a
failure whose error text varies on every occurrence defeats deduplication and
gets triaged from scratch each time. `bi-triage incidents` shows occurrence
counts; many near-identical incidents with a count of 1 is the symptom.
