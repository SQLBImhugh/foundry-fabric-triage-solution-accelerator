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
| `bi-triage approve` / `deny` | nothing | Offline, and how an on-call engineer holding the repo would answer |
| The card's buttons | `APPROVAL_CALLBACK_URL` | A click in Teams |

The buttons are `Action.OpenUrl`, not `Action.Submit`. A card posted through an
incoming webhook has no bot behind it, so a submit button renders a control that
silently does nothing — which looks exactly like a recorded decision.

**What the buttons point at.** `infra/approval-callback.json`, which deploys
**two** Consumption Logic Apps. The split is forced by the platform rather than
by taste: a Request trigger accepts exactly one HTTP method. With none declared
it takes POST only, and a GET is rejected with `TriggerRequestMethodNotValid`
before the workflow starts. `triggerOutputs()` has no `method` property either —
only `headers`, `queries` and `body`.

| Workflow | Method | Can it change anything? |
|---|---|---|
| `bi-triage-approval-confirm` | GET | No. Holds no connection; every action is a `Response`. |
| `bi-triage-approval-callback` | POST | Yes. The only one that writes. |

The card links to the GET workflow, so what preview generators, link scanners
and prefetchers fetch is a workflow with nothing to change. An approval a link
preview can grant is not an approval.

An earlier single-workflow version tried to do both by branching on
`toupper(triggerOutputs()['method'])`. That expression can never evaluate, so
the run died with `InvalidTemplate` — and a GET never reached it in the first
place. The test guarding it asserted the template *contained* that expression,
so it passed for exactly as long as the feature was broken. Its replacements
assert properties instead: that no definition references a trigger method, and
that the GET workflow holds no connection and no `ApiConnection` action.

**How it writes.** Fabric SQL has no REST data plane, so the recording workflow
uses the SQL managed connector as its own system-assigned identity. Managed
identity lives in the connector's `oauthMI` parameter value set, whose only
parameter is a token constrained to `location: "logicapp"` — supplied by the
workflow at run time, so the connection holds no credential. The database user
is granted `EXECUTE` on one stored procedure and nothing else; it cannot read an
incident.

The write is `dbo.triage_record_approval_decision`, parameterised, never SQL
assembled from the URL — everything in that URL is editable by anyone holding
the link. The procedure makes the whole decision in one statement:

```sql
UPDATE triage_approvals SET decision = @decision, ...
 WHERE request_id = @request_id
   AND (decision IS NULL OR decision = '')           -- unanswered, exactly once
   AND (@fingerprint IS NULL OR ... = @fingerprint)  -- bound to this action
   AND (... expires_at > SYSDATETIMEOFFSET())        -- still open
```

`@@ROWCOUNT` tells the workflow whether it won. Zero means unknown, already
answered, expired, or fingerprint mismatch; all four render as a refusal and
none changed anything. A failed write renders as a failure rather than falling
through to a success page. The agent revalidates all of it independently.

Verified end to end against a live Fabric SQL Database: GET renders the page and
changes nothing, POST records, a second POST is refused with the first decision
intact, and mismatched-fingerprint, expired and unknown requests are all
refused.

**The clock stops while a person decides.** `PolicyLedger.awaiting_human()`

**The clock stops while a person decides.** `PolicyLedger.awaiting_human()`
excludes that time from the wall clock. The run timeout and the approval timeout
both default to 300s, so charging the agent for reading time would fail the run
as `timed_out` at the moment the approval was granted. Turns, tool calls and
tokens stay charged — those are the agent's consumption, not the human's.

The controller posts a short acknowledgement naming who decided and what happens
next. That is what a channel reading back over an outage actually needs. It goes
straight to the notifier rather than through `notify_teams`: that path is
deduplicated per incident, so routing an acknowledgement through it would
consume the incident's one announcement and silence the real outcome.

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

## The state store

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

### Why a Fabric SQL Database

State lives in a **Fabric SQL Database**, in the same workspace as the semantic
models being triaged. It replaced Azure Table Storage, and the reasons were
practical rather than tidy:

* **The state is relational.** An incident has occurrences, an approval belongs
  to an action, a deferred retry belongs to a signature. An operator asking
  "which reports failed most this quarter, and were they the ones we retried"
  can answer it in one query against the same estate they already report on,
  instead of exporting a key-value table first.
* **A conditional `UPDATE` is atomic on its own.** Claims and leases used to be
  read-then-write guarded by an ETag: three round trips and a race the code had
  to reason about explicitly. `UPDATE ... WHERE expires_at < SYSUTCDATETIME()`
  is one statement, `rowcount` says whether this caller won, and the expiry is
  evaluated on the server, so it does not depend on any container's clock. A
  primary-key `INSERT` raising `IntegrityError` gives the same compare-and-set
  the old code got from `ResourceExistsError`.
* **There is no key to leak.** Fabric SQL accepts Entra tokens and nothing else.
  There is no SQL-authentication fallback to switch off, so "no local auth" is
  the platform default rather than a setting governance has to keep reverting.
  The storage account it replaced arrived with shared-key access already
  disabled by policy; this removes the argument entirely.

Eight tables, created on startup by `ensure_schema` rather than by a migration
step, so an adopter pointing at an empty database gets a working system with no
extra command:

| Table | Holds |
|---|---|
| `triage_incidents` | every terminal outcome, keyed by incident id, indexed on (signature, status) because `find_open` runs before every remediation |
| `triage_processed_messages` | which alert mail has already been triaged |
| `triage_approvals` | approval requests and the decisions written against them |
| `triage_deferred_retries` | work postponed by capacity backoff |
| `triage_semantic_health` | silent-failure baselines |
| `triage_sweep_leases` | one sweep at a time, across instances |
| `triage_claims` | one invocation acts, across instances |
| `triage_inbox_audit` | what the inbox filter refused, and why |

Each table carries promoted columns an operator can filter on plus a `payload`
column holding the authoritative JSON. The payload is what the code reads back,
so adding a field to a model never needs a migration.

### The driver choice is a container constraint

`mssql-python`, not `pyodbc`. The controller runs as a Foundry hosted agent: a
managed Linux image built with `dependency_resolution: remote_build`, which
installs `src/requirements.txt` with pip and nothing else. `pyodbc` needs the
`msodbcsql18` **system** driver, which is an apt package, so it cannot work
there at all. `mssql-python` ships the driver inside the wheel as an ordinary
pip dependency and publishes a cp313 manylinux build matching the declared
`python_3_13` runtime.

Connections are **per thread**. One shared connection behind a lock was tried
first and failed under eight concurrent callers with an `OperationalError`
followed by `InterfaceError` on every subsequent use.

### Degradation must be temporary

Stores degrade to in-memory rather than refusing to start — an accelerator that
cannot reach its database should still triage, loudly degraded, rather than fail
to start in front of an audience. What changed is that the degradation now ends
when the outage does.

The previous implementation opened its client once in `__init__` and, on
failure, stayed in-memory for the life of the process. Tenant policy disabled
public network access on the state store minutes after it was created; the
container started while it was unreachable, and then reported healthy triage
outcomes while persisting none of them. Restoring connectivity changed nothing,
because nothing ever tried again. Three invocations were lost before a forced
redeploy fixed it.

Recovery has to include a **reload**, not just a reconnect. `find_open` is what
stops the agent remediating the same failure twice, and an empty cache answers
"no open incident" to everything. `tests/test_store_sql.py` holds the regression
test, and reverting the fix makes it fail.

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

`store/claims.py` supplies the missing primitive, in two statements:

```sql
INSERT INTO triage_claims (claim_key, ...) VALUES (?, ...)      -- I hold it
UPDATE triage_claims SET owner = ?                              -- or I steal it,
 WHERE claim_key = ? AND expires_at < SYSUTCDATETIME()          -- if it is dead
```

The insert raises `IntegrityError` when somebody already holds the claim. The
update reports through `rowcount` whether this caller won, and two racers
cannot both get 1. Verified against the live database with eight concurrent
threads: exactly one winner.

Claims expire, so a container that dies mid-remediation does not hold one for
ever — that would turn a duplicate-work bug into a lost-alert bug.

Unlike the incident and processed stores, this one does **not** degrade quietly
to in-memory when the database is unreachable. Those degrade because losing them
makes the agent noisy; losing this one makes it act twice.

### Which paths are claimed, and one that is not

Three entry paths can reach a real action, and they are not equally protected:

| Path | Claim key | Covered |
|---|---|---|
| Mailbox sweep | `message:{request_id}` | Yes |
| Deferred retry drain | `retry:{signature}` | Yes |
| Interactive alert pasted into the Playground | — | **No** |

The retry drain was unclaimed until recently, which was the sharper of the two
gaps: `due()` and `complete()` are separate statements, so two replicas
draining at the same moment both saw the same row as due and both issued a
dataset refresh. The claim is held until the row is completed or re-deferred,
not merely until the refresh returns — releasing at the refresh would let a
second drainer see the row as still due.

The interactive path remains unclaimed and is documented rather than fixed.
There is no message id to key on, and the signature is not known until the
request has been run. `find_open` reading through to SQL on every check narrows
it — an incident already opened by a sweep is visible and suppresses — but two
callers can still pass that check before either persists. Closing it properly
means claiming on the signature inside `TriageRunner`, which would cover all
three paths uniformly and is a larger change than it appears, because the
offline scenarios drive the same runner.

## The monitoring cockpit

`cockpit/` is a read-only [Fabric App](https://github.com/microsoft/rayfin) over
the controller's own state — incidents, approvals, deferred retries,
semantic-health baselines, and the claims and leases that stop two invocations
acting on the same alert. It has no trigger buttons, no reset, and no scripted
scenarios: nothing in it can change the system it watches.

### Why it reads a semantic model rather than the database

A Fabric App can only reach Fabric data through a semantic model. That is a
property of the SDK, not a preference: `@microsoft/fabric-app-data`'s
`FabricClient` exposes `semanticModel()` and nothing else, and although
`IFabricApiProxy` *declares* `lakehouse.executeSql` and `warehouse.executeSql`,
the embedded host ships no implementation of either. So the chain is:

```
Fabric SQL Database        the controller writes here, over TDS
      |  auto-mirrored to OneLake
      v
SQL analytics endpoint     types itself MirroredWarehouse
      |  Direct Lake
      v
Semantic model             bi-triage-state
      |  DAX, through the Fabric embed proxy
      v
Fabric App                 the cockpit
```

Every hop is read-only, so the state database keeps exactly one writer. The
alternative — projecting rows into the app's own store — was rejected because a
deployed Fabric app accepts Fabric SSO only, leaving no headless credential for
the controller to write with, and because a second copy of the truth is a second
thing that can be wrong.

Direct Lake rather than import: a monitoring surface that lags a scheduled
refresh is describing an estate that no longer exists.

### Two things that will bite the next person

**The `.dark` class is load-bearing, not cosmetic.** The dashboard kit resolves
its chart palette with `base: root.classList.contains("dark") ? "dark" : "light"`.
The Fabric portal sets `data-appearance` on `<html>`, and the kit's `useAppTheme`
follows it — so in a light-themed portal the class comes off and every chart
renders on a white canvas inside otherwise dark cards. CSS variable overrides
cannot fix that, because the chart library resolves its own palette from the
class. `useCanonicalDarkTheme` pins it and re-asserts it through a
`MutationObserver` rather than racing the host.

**Chart and table specs fail silently when their shape is wrong.** Graphein
discriminates on `type`, not Vega-Lite's `mark`, and its table columns are
`{ field, title }`, not `{ key, label }`. Neither mistake is a type error; both
produce an empty card or a render-time crash visible only in the browser
console. `npm run preview -- --spec s.json --query <alias> --dax-file q.dax`
renders a single visual headlessly against live data and catches them before a
deploy does.

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

## Network isolation

This accelerator ships public endpoints and relies on Entra identity plus
governance tags. That is the right default for an evaluation, and the wrong one
for production. What follows is the shape to move to, and — more usefully — why
the obvious pattern does not transfer.

### Why you cannot copy the Azure SQL pattern

The reference implementation this project was compared against
([ZacharyZurloMSFT/agentic-pbi-error-triage](https://github.com/ZacharyZurloMSFT/agentic-pbi-error-triage))
isolates its state with a textbook Azure design: a VNet, a private endpoint on
the SQL server, a `privatelink.database.windows.net` private DNS zone, and
delegated subnets for the Function App and the Foundry agent runtime. It is a
good model and it is worth reading.

**It does not port here.** That design isolates `Microsoft.Sql/servers`, an ARM
resource that takes a private endpoint. This accelerator's state is a **Fabric
SQL Database** — a Fabric item, not an ARM resource. It has no
`Microsoft.Network/privateEndpoints` of its own and no
`privatelink.database.windows.net` zone to link. Copying the Bicep would produce
a VNet protecting nothing.

### The Fabric equivalent

Fabric secures **inbound** access with private links at two scopes:

| Scope | Effect | Use when |
|---|---|---|
| [Tenant-level](https://learn.microsoft.com/fabric/security/security-private-links-overview) | Network policy across the entire tenant | **This accelerator** — the only scope that covers a Fabric SQL Database |
| [Workspace-level](https://learn.microsoft.com/fabric/security/security-workspace-level-private-links-overview) | One workspace mapped to a VNet; others stay public | Workspaces built from supported items — which this one is not |

**Workspace-level is the obvious choice and it is the wrong one here.** Two
entries on Microsoft's
[supported-scenarios list](https://learn.microsoft.com/fabric/security/security-workspace-level-private-links-support)
rule it out, and they are precisely the two items this design is built on:

- **SQL databases** — "Tenant-level private links are available for SQL
  database, but currently, workspace-level private links are not available in
  SQL database." The state store is the one thing most worth isolating, and it
  is the one thing workspace-level scope does not reach.
- **Semantic models** — "Power BI semantic models aren't supported in workspaces
  with workspace-level private links enabled. If a workspace contains any Power
  BI semantic models, you can't enable workspace-level private links for that
  workspace." The cockpit reads through a semantic model, because a Fabric App
  has no other way to query. Its presence does not merely go unprotected: it
  blocks the feature being turned on at all.

So a workspace holding the SQL database, the semantic model and the cockpit
cannot have workspace-level private links enabled, and **tenant-level is the
only scope that applies to this architecture**. That is a heavier change — it is
a tenant-wide network policy, not a per-workspace one — and worth knowing before
it is planned as a workspace-scoped task.

An earlier revision of this document recommended the opposite, having reasoned
from what the feature is for rather than from its support matrix. It is recorded
here rather than quietly corrected because the mistake is the instructive part:
the two unsupported item types were exactly the two in use.

Two settings in the admin portal govern the tenant-level behaviour — **Azure
Private Links** and **Block Public Internet Access** — and the second is the one
that actually closes the door. With private links configured but public access
still allowed, the workspace is reachable both ways; Microsoft's own guidance
calls that a testing configuration rather than a production one, because it
provides no inbound protection.

### The half it does not cover

**A private endpoint secures traffic *into* Fabric. It does nothing for traffic
*out* of Fabric.** The controller calls Power BI, Microsoft Graph and Azure
OpenAI, and every one of those egress paths is unaffected by anything above.
Securing them is a separate exercise in firewall rules and data-source
configuration.

That asymmetry is worth stating plainly, because "we enabled Private Link" is
routinely heard as "the agent is network-isolated", and for an agent — which is
mostly an egress client — the inbound half is the smaller half.

### Not shipped as Bicep, deliberately

There is no `network.bicep` in this repository. Workspace-level private links
are configured against Fabric, not ARM, and shipping a template that had never
been applied would be a claim rather than a capability. The rule in `AGENTS.md`
is to verify against the platform before asserting; this section documents the
shape and cites the source, and stops there.

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

**A different durable store**: subclass the in-memory store and override
`_load`, `_persist` and `_on_reset`, as `FabricSqlIncidentStore` does. Keep
redaction inside `record`, and make sure a failed open can recover rather than
degrading for the life of the process.

**A real flag table**: replace `DataQualityFlagTable` with three methods against
the real table. Keep the CSV path for evaluation — a table you can open in Excel is
easier to show than a query result.
