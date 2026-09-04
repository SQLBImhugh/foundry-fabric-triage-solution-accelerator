# Architecture

## The flow, mapped

The requested triage flow, and where each box lives in this repo.

| Flow box | Implementation | Notes |
|---|---|---|
| BI Request Inbox | `tools/inbox.py` — `MockInbox` \| `GraphInbox` | Poll or Graph subscription; same `BIRequest` either way |
| Data Quality Issue? | `consult_data_quality_agent` → `agents/data_quality_agent.py` | A separate agent, reached through a tool |
| Is There a Known Related Issue? | `signature.py` + `store/incidents.py::find_open` | 16-char signature over a normalized error |
| Wait for Resolution, Then Continue | outcome `duplicate_suppressed` | Increments the parent incident; no second remediation |
| Does It Qualify as Tier 1? | `TriageClassification.tier` | Model classifies; controller constrains what follows |
| Agentic Resolution | `refresh_powerbi_dataset` | The single allowlisted remediation |
| Is Issue Resolved? | `TriageAgent._validate_outcome` | Checks the claim against the evidence |
| Send Resolution Summary | `notify_teams` → `tools/teams.py` | Report, error, action, outcome, timestamp |
| Human Involvement | outcome `needs_human` | The branch itself is out of scope; the exit is wired |

## Run sequence

```
BIRequest
   |
   v
[runner] compute signature ---> [store] find_open(signature)
   |                                       |
   v                                       v
[TriageAgent.run]  <-------------- known incident (or None)
   |
   |  loop, each pass charged against PolicyLedger
   |
   +--> get_request_context
   +--> get_known_incidents ----------> known? -> notify -> duplicate_suppressed
   +--> consult_data_quality_agent ---> [DataQualityAgent]
   |                                        |
   |                                        +--> check_duplicates (deterministic CSV scan)
   |                                        +--> reconcile(model claim, scan evidence)
   |                                        v
   |                                    DataQualityFinding
   |
   +--> has_issue? -> write_data_quality_flag -> notify -> flagged_data_quality
   +--> else -> get_dataset_refresh_history
   +--> refresh_powerbi_dataset   [charged: 1 of 1 remediation]
   +--> notify_teams
   +--> report_resolution
   |
   v
[_validate_outcome]  downgrade any claim the evidence does not support
   |
   v
TriageResult ---> [store] record()  (redact -> dedup -> persist)
```

## Why the controller owns the loop

There are two ways to build this.

**Prompt-orchestrated.** Give the model the tools and instructions and let it
decide. Fast to build, and everything above is a suggestion. "Only take one
action" competes with every other sentence in the prompt, and loses whenever the
model's reasoning finds a good argument against it.

**Controller-orchestrated.** The model proposes; a Python loop decides whether to
dispatch. That is `PolicyLedger`:

```python
ledger.charge_llm_turn()          # max_llm_turns
ledger.charge_tokens(n)           # max_tokens
ledger.charge_tool_call(name)     # allowlist, max_tool_calls, max_write_actions
```

Each raises `PolicyViolation` rather than returning a boolean, so a forgotten
check is a failing test rather than a silent budget overrun.

### The asymmetry in how violations are handled

Not every violation should end the run:

| Kind | Handling | Why |
|---|---|---|
| `policy_blocked` | Returned to the model **as a tool result** | The agent can still escalate. Silence is the worse failure |
| `timed_out` | Propagates, ends the run | Allowance spent |
| `budget_exceeded` | Propagates, ends the run | Allowance spent |
| `max_turns_exceeded` | Propagates, ends the run | Allowance spent |

This is why `scenario3-policy-block` ends in `needs_human` with a Teams message,
rather than in a stack trace.

### Two action classes

```python
REMEDIATION_ACTIONS = {"refresh_powerbi_dataset"}                     # budgeted
REPORTING_ACTIONS   = {"write_data_quality_flag", "notify_teams",
                       "report_resolution"}                           # audited, not budgeted
```

Reporting is deliberately exempt. If posting to Teams consumed the same budget as
fixing something, the agent would go quiet exactly when it most needs to speak.

## The agent boundary

The Data Quality agent is a real agent — own provider, own prompt, own tool, own
loop — not a function on the Triage agent. The handoff is a typed
`DataQualityFinding`, so the boundary is testable without either model.

It reports; it does not decide. `recommended_action` is a recommendation. The
Triage agent owns the decision. Add a third agent later and the flow does not
change shape.

### Evidence outranks assertion

`check_duplicates` is a plain CSV scan. The model writes the sentence; the scan
produces the numbers. `_reconcile` enforces this:

```python
truth = evidence.duplicate_row_count > 0
if claimed is not None and bool(claimed) != truth:
    logger.warning("... deferring to the scan.")
```

An agent that can talk itself out of its own evidence is not deployable, and the
inverse — an agent inventing findings that aren't there — writes a false row into
a table someone acts on. Both directions are tested.

## Outcome validation

The agent's self-report is a hypothesis, not a result:

- `resolved` with no successful remediation → downgraded to `needs_human`
- `flagged_data_quality` with no positive scan → downgraded to `needs_human`
- an unrecognized outcome string → `needs_human`

A production deployment shipped an autonomous recovery agent that reported
"Fixed" three times consecutively while the underlying notebook kept failing,
because nothing compared the claim to the evidence. That is the bug this
prevents.

## Signatures and suppression

```python
signature = sha1(source | artifact_kind | artifact_name | exception_class | normalized_error)[:16]
```

Normalization strips GUIDs, timestamps, line numbers, URL paths, IPs, hex
suffixes, temp paths, long hashes and request IDs. Case is preserved — SQL
identifier case is significant in some dialects, and folding it merges genuinely
distinct failures.

`artifact_name` is in the payload on purpose: the same error class on two
different reports stays two incidents, because suppressing across unrelated
reports would hide a real second outage.

**Only open incidents suppress.** A resolved incident recurring is new
information and must be allowed to trigger action again.

**A suppressed duplicate increments its parent.** It does not write a parallel
row — that would produce one incident per alert, which is the state the signature
exists to prevent.

**An incident is announced once, not once per occurrence.** The controller
checks `notified_count` on the matching open incident before delivering a Teams
card and declines if it is already above zero. The tool call is still recorded,
so the audit trail shows the agent asked and the controller refused.

This is enforced in `ToolDispatcher._execute`, not in the prompt, for the usual
reason: a limit that exists only as prompt wording is not a limit. It was added
after a five-minute routine over two unread alerts posted roughly 24 identical
cards an hour into a real channel — dedup was stopping the *remediation* but not
the *notification*, which is the alert fatigue this system exists to remove.

**Already-triaged mail is tracked in the agent's own store**, in
`store/processed.py`, keyed by a hash of the Graph message id. It cannot be
tracked in the mailbox: the agent holds `Mail.Read` and deliberately cannot mark
a message read or move it. A message is marked only *after* its outcome is
persisted, so a crash mid-run re-triages rather than dropping the alert.

## Human approval

`rebind_dataset_gateway` is the only action in `APPROVAL_REQUIRED_ACTIONS`. Its
blast radius covers every dataset bound to that gateway, so the decision belongs
to someone who knows what else is on it. Membership is a code change and a
review — that is the difference between "the agent was told to ask" and "the
agent cannot proceed without an answer".

The gate sits in front of dispatch in `ToolDispatcher`, so an unapproved action
is never executed regardless of what the model asked for.

**Where a decision lives.** `store/approvals.py`, one row per request, updated
in place. The agent writes the request before posting the card; a human writes
the answer from somewhere else entirely; the agent reads it back on a later
poll. It has to be durable shared state — the writer and the reader are
different processes, and on a hosted agent often different invocations.

**Who can answer.** Two writers, and the agent cannot tell them apart:

| Channel | Needs | Use |
|---|---|---|
| `bi-triage approve` / `deny` | nothing | Offline, and how an on-call engineer would actually answer |
| The card's buttons | `APPROVAL_CALLBACK_URL` | A click in Teams |

The buttons are `Action.OpenUrl`, not `Action.Submit`. A card posted through an
incoming webhook has no bot behind it, so a submit button renders a control that
silently does nothing — which looks exactly like a recorded decision.

**What the buttons point at.** A Consumption Logic App
(`infra/approval-callback.json`) with an HTTP trigger, which writes the decision
to the approvals table using its own managed identity. Not a Power Automate
flow: the Azure Table connector authenticates with a shared account key and
`allowSharedKeyAccess` is `false` here by tenant policy, and an HTTP-triggered
flow is a premium trigger. The Logic App needs no key and no licence.

It refuses a request that does not exist, one already answered, one that has
expired, and one whose fingerprint does not match the link — then the agent
revalidates all of it independently. The callback URL is a bearer credential:
anyone holding the link can answer. The fingerprint binding limits it to that
one action, it lives in the azd environment rather than the repo, and the
handoff scanner treats it as a secret.

**The clock stops while a person decides.** `PolicyLedger.awaiting_human()`
excludes that time from the wall clock. The run timeout and the approval timeout
both default to 300s, so charging the agent for reading time would fail the run
as `timed_out` at the moment the approval was granted. Turns, tool calls and
tokens stay charged — those are the agent's consumption, not the human's.

Validation is unchanged and stays on the reading side: explicit, fingerprint
matches the exact action *and* arguments, unexpired, single-use. Anything else
is not approved.

**The card cannot show that it was answered.** `Action.OpenUrl` is a link — it
cannot alter the card it sits on — and an incoming webhook cannot edit a message
it already posted. So the original card keeps its Approve/Decline buttons
forever. A second click is refused by the callback and again by the agent, but
nothing on the card itself says so.

The controller therefore posts a short acknowledgement naming who decided and
what happens next. That is what a channel reading back over an outage actually
needs. It goes straight to the notifier rather than through `notify_teams`: that
path is deduplicated per incident, so routing an acknowledgement through it
would consume the incident's one announcement and silence the real outcome.

## Capacity throttling and deferred retries

Throttling is the one failure in the set where the obvious fix is actively
harmful. The capacity has already exceeded its resource limits, so a retry adds
load to the cause; across several datasets at once that turns contention into an
outage caused by the system meant to be helping.

`defer_refresh_retry` schedules the work instead of doing it. It is a
**reporting** action, not a remediation: it touches nothing in Power BI, and it
must not charge the remediation budget — if postponing spent the run's one
remediation, the retry could never be performed when its window arrived.

**The controller refuses an immediate refresh while a deferral is open**, in
`ToolDispatcher._precondition_failure`. A model that is merely told not to retry
can be argued out of it. That refusal also does not spend the budget: any action
with a precondition has its remediation charge deferred until the check passes,
for the same reason an approval denial does not.

**Bounded.** Each deferral doubles the wait (15, 30, 60 minutes) and increments
an attempt count. After three the row is marked `exhausted` and the outcome
becomes `needs_human` — repeated throttling is a capacity scheduling problem, not
a retry problem. An agent that defers indefinitely has invented a patient way of
doing nothing.

**Something has to drain it.** `TriageRunner.drain_due_retries()` runs at the
start of every sweep, before the mailbox is read, so a retry that succeeds closes
its incident before a fresh alert for the same signature is judged against it.
The drain is deterministic and model-free: the decision is already on disk, and
re-running triage would trip the known-incident check and suppress the very work
it was sent to do. A successful retry marks the incident resolved — an incident
left open after the fix keeps suppressing genuine recurrences.

`bi-triage retries` shows what is postponed; `--drain` performs what is due.

## Silent failures: the ones that never send an alert

Every other path here begins with Power BI reporting a failure. The failures
that hurt most report nothing: the refresh succeeds and the data is wrong
anyway. The analyst's problem is "a report
that looks normal but is a day stale", and until the detector existed that was
the one case an alert-driven system could not see.

`detectors/silent_failures.py` asks three questions of a semantic model:

| Question | Failure it catches |
|---|---|
| Did the watermark advance? | The pipeline ran and loaded nothing |
| Is the row count near its baseline? | A partial load; every total silently wrong |
| Can the probe still run? | A column or measure changed under the report |

**Deterministic, not a third prompt agent.** Every question is a measurement —
a maximum, a count, a comparison — and invariant 4 says measured evidence
outranks model output. A model asked whether a 60% row drop is acceptable will
sometimes say yes, which is precisely the judgement this must not make. The
Triage agent writes the explanation; the scanner decides what is true.

**The model never writes DAX.** Queries are generated from stored probe
configuration, so a prompt injection in an alert email cannot turn a read-only
detector into an arbitrary query engine against the finance model.

**False positives are the failure mode that matters.** An alert that fires
wrongly gets the channel muted, and then the real one is missed too. The bounds:

- A single anomalous reading is `suspect` and says nothing. A finding needs the
  condition to survive a confirmation scan — a probe running mid-refresh sees a
  half-loaded table.
- Row collapse needs **both** a relative and an absolute threshold. Relative
  alone makes small tables permanently noisy (7 rows to 4 is a 57% drop);
  absolute alone never fires on one that genuinely emptied.
- Freshness is configured per probe, never inferred. A T+3 finance model is
  legitimately three days behind.
- **Baselines only ever advance from healthy readings.** Accepting a suspect
  reading as the new normal teaches the detector that the failure is fine.
- "We cannot see this model" is `detector_fault`, never a data finding. A
  permissions change must not read as a data outage.

A confirmed finding becomes an incident with a normal signature, so the existing
deduplication applies: a detector polling every fifteen minutes announces once.

`bi-triage health` runs a sweep; `--baselines` shows what healthy looked like.
The hosted agent answers `silent sweep` as a second sentinel alongside `sweep`.

## The incident store

Every terminal outcome is persisted:

```
resolved · flagged_data_quality · duplicate_suppressed · deferred_retry
approval_denied · needs_human · declared_failed · agent_crashed
timed_out · budget_exceeded · max_turns_exceeded · policy_blocked
```

The original production gate was `status == "fixed"`. Ten Foundry agent
crashes over two weeks left zero trace in the queue operators actually read.

`requires_investigation` is set for crashes, budget exhaustion, escalations, and
**any run containing a blocked attempt** — a refusal is a signal about the gap
between what the agent wanted and what it was allowed to do, which is precisely
the population you mine to decide what to automate next.

Redaction happens *inside* `record()`, not at call sites, so a new code path
cannot forget it.

## Claims: only one invocation acts

The store above is checked *before* the work and written *after* it, and the
processed-message log has the same shape. Both are correct for one process and
wrong for two.

A hosted agent can be invoked manually while a schedule fires, or run as more
than one replica. Both invocations then see the same alert as untriaged and no
open incident, and both dispatch the remediation. The write-action budget does
not help: it is per run, and these are two runs. The only lock that existed was
an `asyncio.Lock` on the agent instance — process-local, and a hosted agent is
rebuilt per request, so it did not even span two requests to one replica.

`store/claims.py` supplies the missing primitive. `create_entity` on an Azure
Table fails with `ResourceExistsError` when the row already exists, which is an
atomic compare-and-set against shared state and all a lease needs. The controller
takes a claim keyed on the message id before doing anything with real effect, and
releases it afterwards.

Claims expire, so a container that dies mid-remediation does not hold one for
ever — that would turn a duplicate-work bug into a lost-alert bug. Stealing an
expired claim is itself conditional on the ETag, so two callers racing to take
over the same dead claim cannot both succeed.

Unlike the incident and processed stores, this one does **not** degrade quietly
to in-memory when storage is unreachable. Those degrade because losing them makes
the agent noisy; losing this one makes it act twice.

## Providers

One interface, three implementations:

| Mode | Class | Use |
|---|---|---|
| `mock` | `ScriptedProvider` | Offline evaluation, tests, live fallback |
| `direct` | `AzureOpenAIProvider` | Chat completions, client-side tools |
| `foundry` | `FoundryAgentProvider` | Foundry agents, both handoff shapes |

`ScriptedProvider` is a fixed state machine, not an agent, and the docstring says
so. It exists so the repo runs with nothing but pydantic installed, and so the
tests assert on orchestration rather than model output.

Foundry is reached over REST with `DefaultAzureCredential` rather than through a
client SDK. Preview SDKs churn; a deployment that breaks because a package minor-bumped
the week before is a bad outcome. It also puts the wire format on screen, which is
what was asked for.

## Observability

Every LLM call emits an OTel GenAI span: `gen_ai.system`, `gen_ai.request.model`,
`gen_ai.operation.name`, `agent.name`, token counts, finish reason. Tool calls
emit `tool.*` spans — that is what makes the handoff visible in a trace.

Without the OTel SDK installed, every helper is a no-op. Telemetry is not allowed
to be a hard dependency of the accelerator running.

**Metadata only.** No prompt or completion content. In a multi-tenant system,
content recording ingests customer data and secrets into a telemetry store with
different access controls than the source system.

## Extending it

**A new remediation**: add the tool schema to `TRIAGE_TOOLS`, add a branch to
`ToolDispatcher._execute`, add the name to `REMEDIATION_ACTIONS`, and add a
scenario. Adding a capability is a code review, not a prompt edit — which is the
property that makes the allowlist worth anything.

**A new agent**: mirror `DataQualityAgent`. Own provider, own prompt, own tools,
returns a typed model. Expose it to the orchestrator as one tool.

**A durable store**: implement `find_open`, `record`, `list_all`, `reset` against
Cosmos or SQL. Keep redaction inside `record`.

**A real flag table**: replace `DataQualityFlagTable` with three methods against
the real table. Keep the CSV path for evaluation — a table you can open in Excel is
easier to show than a query result.
