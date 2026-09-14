# Architecture

## Entry points and controller flow

`TriageRunner` owns client and store construction, signatures, open-incident
lookup, agent construction and terminal persistence. `TriageAgent` proposes
tool calls through a provider; `ToolDispatcher` and the shared `PolicyLedger`
decide which calls may execute.

| Entry point | Implementation | Boundary |
|---|---|---|
| Power BI alert mailbox | `tools/inbox.py`, CLI watch loop and `src/app.py` | Filtered Graph polling and processed-message tracking |
| Interactive alert | `src/app.py::TriageControllerAgent._triage_text` | Same runner, with the concurrency limitation documented below |
| Silent-failure sweep | `TriageRunner.silent_sweep` | Configured deterministic semantic-model probes |
| Scheduled pipeline monitor | `TriageRunner.pipeline_sweep` | Explicit pipeline targets, scheduled failed jobs and activity evidence |
| Command-center investigation | `command_center/api.py` and `command_center/worker.py` | Authenticated request stored in SQL; a controller command sweep executes it |

The Power BI triage decisions map to these components:

| Flow box | Implementation | Notes |
|---|---|---|
| BI Request Inbox | `tools/inbox.py` — `MockInbox` \| `GraphInbox` | Mock files or filtered polling, normalized to `BIRequest` |
| Data Quality Issue? | `consult_data_quality_agent` → `agents/data_quality_agent.py` | A separate agent, reached through a tool |
| Is There a Known Related Issue? | `signature.py` + `store/incidents.py::find_open` | 16-char signature over a normalized error |
| Wait for Resolution, Then Continue | outcome `duplicate_suppressed` | Increments the parent incident; no second remediation |
| Does It Qualify as Tier 1? | `TriageClassification.tier` | Model classifies; controller constrains what follows |
| Agentic Resolution | `ToolDispatcher` and `PolicyLedger` | Refresh, gateway binding or schedule restoration, subject to the action's gates |
| Is Issue Resolved? | `TriageAgent._validate_outcome` | Checks the claim against the evidence |
| Send Resolution Summary | `notify_teams` and recorded terminal result | Report, error, action, outcome, timestamp; web delivery does not require Teams |
| Human Involvement | Approval gate or outcome `needs_human` | A person can approve an allowlisted proposal or investigate; no unrestricted human repair workflow is automated |

## Run sequence

The typical transient Power BI path is shown below. Gated remediation,
deferred retry and pipeline paths use the same policy boundary.

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

## Controller-owned policy

Prompt instructions do not enforce action limits. The model proposes; a Python
loop checks the allowlist, budgets, deterministic prerequisites and approval
state before dispatch. `PolicyLedger` tracks consumption:

```python
ledger.charge_llm_turn()          # max_llm_turns
ledger.charge_tokens(n)           # max_tokens
ledger.charge_tool_call(name)     # allowlist, max_tool_calls, max_write_actions
```

Each raises `PolicyViolation` rather than returning a boolean, so a forgotten
check is a failing test rather than a silent budget overrun.

Tool-call attempts and remediation writes are separate charges. A gated or
preconditioned action is charged as a write only after those checks pass, so a
denial does not spend the remediation budget. The default allows one write in
a run, shared by all participating agents. Incident lookup and claims control
repeat work across invocations; a human tracking closure does not reset them.

### Policy violations

Not every violation should end the run:

| Kind | Handling | Why |
|---|---|---|
| `policy_blocked` | Returned to the model **as a tool result** | The agent can still escalate. Silence is the worse failure |
| `timed_out` | Propagates, ends the run | Allowance spent |
| `budget_exceeded` | Propagates, ends the run | Allowance spent |
| `max_turns_exceeded` | Propagates, ends the run | Allowance spent |

This is why `scenario3-policy-block` ends in `needs_human` with a Teams message,
rather than in a stack trace.

### Action allowlists

| Allowlist | Current tools | Budget |
|---|---|---|
| `REMEDIATION_ACTIONS` | `refresh_powerbi_dataset`, `rebind_dataset_gateway`, `reenable_refresh_schedule`, `rerun_fabric_pipeline` | Remediation and tool-call budgets |
| `REPORTING_ACTIONS` | `write_data_quality_flag`, `notify_teams`, `report_resolution`, `defer_refresh_retry` | Tool-call budget, not remediation budget |
| `DIAGNOSTIC_ACTIONS` | `get_request_context`, `get_known_incidents`, `consult_data_quality_agent`, `check_duplicates`, `get_dataset_refresh_history`, `get_refresh_schedule`, `get_pipeline_run_evidence`, `get_pipeline_rerun_status` | Tool-call budget |

Reporting is deliberately exempt. If posting to Teams consumed the same budget as
fixing something, the agent would go quiet exactly when it most needs to speak.
Pipeline requests additionally use `PIPELINE_ACTIONS`; a registered dataset
tool remains unavailable in that workload.

## The agent boundary

`DataQualityAgent` has its own provider and prompt. Its controller scans all
registered tables before making one tool-free model call. The model interprets
the selected evidence; it does not choose or run a scan. The Triage agent
reaches this component through `consult_data_quality_agent` and receives a
typed `DataQualityFinding`.

`recommended_action` is a recommendation, not authorization. The Triage agent
proposes the next step and the controller enforces what may execute.

### Evidence outranks assertion

`check_duplicates` is a plain CSV scan. The model writes the sentence; the scan
produces the numbers. `_reconcile` enforces this:

```python
truth = evidence.duplicate_row_count > 0
if claimed is not None and bool(claimed) != truth:
    logger.warning("... deferring to the scan.")
```

This prevents both a model dismissing measured duplicates and a model
inventing a defect that would write a false flag. Both directions are tested.

## Outcome validation

The agent's self-report is a hypothesis, not a result:

- `resolved` with no successful remediation, or with only ungranted approvals
  → downgraded to `needs_human`
- `flagged_data_quality` with no positive scan or no written flag
  → downgraded to `needs_human`
- `deferred_retry` without scheduled retry state → `needs_human`
- `duplicate_suppressed` without a matching open incident → `needs_human`
- `approval_denied` without a recorded ungranted approval → `needs_human`
- `resolved` or `flagged_data_quality` with recorded `ledger.blocked_attempts`
  → `needs_human`, even if another action succeeded
- an unrecognized outcome string → `needs_human`

A previous recovery system reported "Fixed" three times consecutively while
the underlying notebook kept failing,
because nothing compared the claim to the evidence. That is the bug this
validation prevents. It is not a claim that this accelerator monitors
standalone notebooks.

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

This refers to controller incident state. **Resolved by user** is a separate
command-center tracking projection, not a change to that state. It cannot
license another remediation by making an open incident disappear from the
controller's lookup.

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

`APPROVAL_REQUIRED_ACTIONS` contains `rebind_dataset_gateway`,
`reenable_refresh_schedule` and `rerun_fabric_pipeline`. Changing this set
requires a code review; approval cannot add an action to an allowlist.

Gateway rebinding changes the selected dataset's gateway binding, not every
dataset on the gateway. Its data-source access and dependent reports still
require human review. Re-enabling a schedule restores unattended execution and
requires successful refresh evidence first. A pipeline rerun can repeat writes
across the whole pipeline, so it also requires reviewed replay safety and a
complete, fingerprinted parameter set.

The gate sits in front of dispatch in `ToolDispatcher`, so an unapproved action
is never executed regardless of what the model asked for.

**Where a decision lives.** `store/approvals.py`, one row per request, updated
in place. The agent writes the request before posting the card; a human writes
the answer from somewhere else entirely; the agent reads it back on a later
poll. It has to be durable shared state — the writer and the reader are
different processes, and on a hosted agent often different invocations.

**Who can answer.** Web and legacy approval channels have different trust
boundaries:

| Channel | Needs | Use |
|---|---|---|
| Command-center decision controls | Valid delegated API token with Approver or Admin app permission | Authenticated, fingerprint-bound web decisions; responder comes from the token |
| `bi-triage approve` / `deny` | Local state access offline, or the operator's Entra SQL permissions live | Operator/legacy channel, not browser authentication |
| Legacy Teams card buttons | `APPROVAL_CALLBACK_URL` | Bearer-link callback; supplied responder text is not an Entra-verified person |

`APPROVAL_DELIVERY_MODE=web` uses the command-center decision path and does not
require Teams. Optional Teams delivery links to that web proposal. The legacy
SQL callback procedure explicitly refuses web proposals.

The legacy buttons are `Action.OpenUrl`, not `Action.Submit`. A card posted
through an incoming webhook has no bot behind it, so a submit button renders a
control that silently does nothing — which looks exactly like a recorded decision.

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

**How it writes.** The recording workflow uses the SQL managed connector
against Fabric SQL as its own system-assigned identity. Managed
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
   AND COALESCE(JSON_VALUE(payload, '$.delivery_channel'), 'teams') <> 'web'
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

**Retry draining.** `TriageRunner.drain_due_retries()` runs at the
start of each mailbox sweep, before mail is read, so a retry that succeeds closes
its incident before a fresh alert for the same signature is judged against it.
The drain is deterministic and model-free: the decision is already on disk, and
re-running triage would trip the known-incident check and suppress the very work
it was sent to do. A successful retry marks the incident resolved — an incident
left open after the fix keeps suppressing genuine recurrences.

`bi-triage retries` shows what is postponed; `--drain` performs what is due.

## Silent-failure detection

An alert-driven path cannot detect a refresh that reports success while its
data remains stale or incomplete. Configured semantic-model probes provide
that separate entry point; they do not inspect arbitrary models automatically.

`detectors/silent_failures.py` asks three questions of a semantic model:

| Question | Failure it catches |
|---|---|
| Did the watermark advance? | The pipeline ran and loaded nothing |
| Is the row count near its baseline? | A partial load; every total silently wrong |
| Can the probe still run? | A column or measure changed under the report |

**Deterministic, not a third prompt agent.** Every question is a measurement —
a maximum, a count, a comparison — and the controller's evidence rule says
measurements outrank model output. A model asked whether a 60% row drop is
acceptable will sometimes say yes. The scanner supplies the detail, and
`TriageRunner._record_silent_finding` records a `needs_human` result without an
LLM call. An observer can explain that recorded finding later.

**The model never writes DAX.** Queries are generated from stored probe
configuration, so a prompt injection in an alert email cannot turn a read-only
detector into an arbitrary query engine against the finance model.

**False positives are the failure mode that matters.** An alert that fires
wrongly gets the channel muted, and then the real one is missed too. The bounds:

- A single anomalous reading is `suspect`: it appears in the sweep summary but
  does not open an incident or send a notification. A finding needs the
  condition to survive a confirmation scan — a probe running mid-refresh can
  see a half-loaded table.
- Row collapse needs **both** a relative and an absolute threshold. Relative
  alone makes small tables permanently noisy (7 rows to 4 is about a 43% drop);
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

Every terminal outcome enters the incident recording path:

```
resolved · flagged_data_quality · duplicate_suppressed · deferred_retry
approval_denied · needs_human · declared_failed · agent_crashed
timed_out · budget_exceeded · max_turns_exceeded · policy_blocked
```

The original production gate was `status == "fixed"`. Ten Foundry agent
crashes over two weeks left zero trace in the queue operators actually read.

`requires_investigation` is set for crashes, budget exhaustion, escalations,
approval denials, notification failures and any result with recorded
`blocked_attempts`. A refusal records the gap between what the agent proposed
and what policy allowed, which can inform a review of future automation.

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

`ensure_schema` and its delegated schema builders define the tables below.
The controller can install them when it has the required grants. Install the
schema as an administrator before using the command center: its more restricted
web identity performs no schema installation.

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
| `triage_pipeline_reruns` | one approved submission per failed pipeline run, plus correlated execution verification |
| `triage_agent_runs` | individual run metadata and the full typed terminal result |
| `triage_agent_events` | redacted progress and tool-result events, linked to a run |
| `triage_agent_commands` | idempotent operator requests, conditional execution state and reconciliation audit |
| `triage_incident_activity` | append-only notes, source-revision-bound human resolutions, questions and answers |

Most record tables carry promoted filter columns plus a JSON `payload`. Claims
and leases use dedicated columns. Command execution columns changed by
conditional SQL statements take precedence over stale payload copies when a
record is reconstructed.

The SQL access-grant table and service are retired. App authorization does not
read operational tables to discover user roles. The state database remains an
independent Fabric item; changing command-center authentication or removing a
web frontend must not remove incidents, approvals or execution journals.

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

The controller's record stores can degrade to in-memory while reporting the
outage. That is not durable success, and recovery must re-check the backend and
reload its state. Coordination and live web stores fail closed instead:
claims, pipeline reservations and command-center collaboration cannot substitute
process-local state for a shared database.

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

Entry paths that can reach a real action are not equally protected:

| Path | Claim key | Covered |
|---|---|---|
| Mailbox sweep | `message:{request_id}` | Yes |
| Deferred retry drain | `retry:{signature}` | Yes |
| Scheduled pipeline triage and rerun verification | `pipeline:{target.key}` | Yes, plus a durable submission reservation per failed run |
| Queued command-center investigation | `command-target:{target_id.casefold()}` and command-row ownership | Yes, among commands for that target; pipeline work also takes its pipeline claim |
| Interactive alert pasted into the Playground | — | **No** |

The retry drain was unclaimed until recently, which was the sharper of the two
gaps: `due()` and `complete()` are separate statements, so two replicas
draining at the same moment both saw the same row as due and both issued a
dataset refresh. The claim is held until the row is completed or re-deferred,
not merely until the refresh returns — releasing at the refresh would let a
second drainer see the row as still due.

The interactive path remains unclaimed and is documented rather than fixed.
There is no external mailbox message id to claim, and signature construction
belongs inside the runner rather than the hosted text adapter. `find_open`
reading through to SQL on every check narrows the gap — an incident already
opened by a sweep is visible and suppresses — but two
callers can still pass that check before either persists. Closing it properly
means defining shared signature-claim ownership inside `TriageRunner`, rather
than assuming the existing path-specific keys serialize one another. That
affects the offline scenarios too, so the gap remains explicit.

## Scheduled Fabric pipeline failures

An explicit pipeline monitor feeds the same `TriageAgent` and `ToolDispatcher`.
It reads scheduled failed job instances and activity evidence, using immutable
workspace/pipeline/run identifiers rather than model-supplied targets.
`PIPELINE_ACTIONS` excludes Power BI dataset actions from this path.
Notebook activity failures contribute evidence within these configured
pipelines. There is no standalone notebook monitor or notebook-editing tool.

The pipeline-scoped claim serializes this controller's work. A separate,
non-expiring SQL reservation prevents a second POST for the same failed run,
including after an ambiguous transport failure. Approval covers the configured
target and parameter fingerprint. A correlated new run and its activity
evidence must be verified before accepting success.

See [PipelineTriage.md](PipelineTriage.md) for configuration, failure boundaries,
API sources, scheduling and operator reconciliation.

## The Azure-hosted command center

`command-center/` is a React/Vite application served with the FastAPI API in
`triage.command_center.api`. The browser calls that API, not SQL or the
actionable Foundry agent endpoint. Its primary workspace is a queue and
inspector; a separate Incidents workspace provides searchable full records.
The UI uses self-hosted DejaVu Serif Condensed, Onyx dark/light colors,
Ink-style geometry and offset shadows, and the supplied triage PNG logo.

### Entra authorization

The backend verifies the signature, tenant, issuer, audience, validity interval,
delegated `access_as_user` scope and known app roles of the custom-API token.
App-only tokens and tokens with no recognized role are refused. The signed
token, not a browser actor header, SQL grant or live group lookup, is the
authority for each request.

| App-role claim | App permissions |
|---|---|
| `CommandCenter.Reader` | Read records, access details and history; ask tool-free questions |
| `CommandCenter.Operator` | Reader access, queued investigations, incident notes and human tracking resolution |
| `CommandCenter.Approver` | Reader access and approval/denial decisions |
| `CommandCenter.Admin` | All app operations, isolated scenario validation and command reconciliation |

Operator and Approver are independent. Admin is an application role, not an
Entra directory role, Azure role or Fabric permission. The controller and web
service retain separate service-identity grants, and reasoning agents acquire
no remediation permissions from an app-user role.

Four ordinary Entra security groups map to these roles. IT manages the
enterprise application's assignments; group owners manage membership.
`scripts/register_command_center.py` configures the secretless SPA/API and
`scripts/configure_command_center_groups.py` configures the groups using an
already authorized operator. Group-based application assignment requires
Entra P1/P2. These setup permissions are not runtime app permissions.

**Access & permissions** replaces the editable SQL Admin center. `/api/access`
returns effective token roles, identity metadata and token issue/expiry times.
It does not return a user roster, memberships or which group supplied a role.
The retired admin access endpoints return `410 managed_in_entra`, and
`COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true` is rejected at startup rather
than reviving SQL authorization.

**Refresh permissions** forces renewal of the current user's custom-API token
and reloads access details and the command-center snapshot. UI actions remain
locked until a snapshot from the new permission-refresh generation succeeds.
A failed renewal or stale snapshot cannot restore the preceding capabilities.
No sign-in or consent popup is opened automatically by this control.

Already-issued JWT roles can remain effective until expiry or renewal; the
verifier allows 30 seconds of clock leeway. A group change is not an immediate
revocation guarantee. Authorization needs no runtime Graph directory permission.
The optional profile photo uses a separate delegated Graph `User.Read` token.

### Commands, approvals and history

The API records commands and web decisions in the standalone Fabric SQL
database. A controller command sweep performs the work. Target-level claims
prevent overlapping command executions; durable interrupted rows retain
uncertainty after a timeout or process loss. An administrator must reconcile
the actual target state before clearing that barrier. Reconciliation records
an audit entry and never executes a tool.

Web proposals are excluded from the legacy approval procedure. Optional Teams
notification is concurrent with web polling, so it cannot spend the approval
window before a person can answer.

The history wrapper stores each run's complete typed result and safe progress
events, including redacted final explanations. It does not capture raw provider
request/response transcripts or hidden reasoning. Tracing remains metadata-only.
Earlier aggregate incident rows cannot reconstruct run events that were never
captured; enable `RUN_HISTORY_ENABLED` on the controller for full run capture.

The admin scenario harness reads the same YAML catalog as the CLI. It uses
separate synthetic tools and state even when its outer result is persisted in
live history. Foundry-backed validation can call a live model, but its
remediation tools remain synthetic. Live validation also requires the explicit
evaluation setting; it is not part of the offline test path.

### Incident collaboration

The full incident record adds notes, a persisted observer discussion and human
tracking resolution without replacing the controller incident. Its
`triage_incident_activity` journal is append-only. SQL reads go to the backend;
mutations do not fall back to local state or automatically replay an
unconfirmed write.

**Resolved by user** is an operator decision about tracking, not an
agent-verified repair. A resolution must match both the tracking version and
`source_revision`: SHA-256 of the original SQL NVARCHAR payload encoded as
UTF-16 LE, matching SQL `HASHBYTES('SHA2_256', payload)`. Hashing a re-serialized
Pydantic model is incorrect because defaults or whitespace can change the
bytes. Any later payload change, including a new occurrence, invalidates the
closure. Core outcomes, approvals, claims, notification counts and remediation
budgets are unchanged.

The queue, incident list, counts and full record use the same tracking
projection. `resolved_by_user` remains distinct from controller `resolved`;
API `needs_review` records appear under the UI's Needs investigation label.
Closed records remain searchable, and a new evidence revision can return them
to the open tracking view.

The observer receives only authorized recorded context, including bounded
human annotations marked as untrusted. It has no tools and cannot execute,
approve or queue work. Discussion reserves the question before calling the
observer; an interrupted or failed request is retained rather than replayed
automatically. Explanations and answers use the shared safe Markdown renderer:
raw HTML, images and unsafe links are not rendered.

See [CommandCenter.md](CommandCenter.md) for deployment, role grants, private
access, operator workflows and validation boundaries.

## The monitoring cockpit

`cockpit/` is a read-only [Fabric App](https://github.com/microsoft/rayfin) over
the controller's own state — incidents, approvals, deferred retries,
semantic-health baselines, and the claims and leases that stop two invocations
acting on the same alert. It has no trigger buttons, no reset, and no scripted
scenarios: nothing in it can change the system it watches.

### Why it reads a semantic model rather than the database

This cockpit uses `@microsoft/fabric-app-data`'s
`FabricClient.semanticModel()`. In the tested embed host,
`IFabricApiProxy` declared `lakehouse.executeSql` and `warehouse.executeSql`
without working implementations. The selected read path therefore remains:

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

The cockpit adds no writer; controller, web and legacy callback grants remain
scoped to their respective operations. The alternative — projecting rows into
the app's own store — was rejected because a
deployed Fabric app accepts Fabric SSO only, leaving no headless credential for
the controller to write with, and because a second copy of the truth is a second
thing that can be wrong.

Direct Lake avoids a separate import-refresh schedule, but mirroring and
semantic-model framing still introduce read latency. The command center reads
the operational SQL store directly through its API; the cockpit does not.

### Cockpit rendering constraints

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
| `mock` | `ScriptedProvider`, `ScriptedDataQualityProvider` | Explicit offline evaluation and tests; not a fallback for a failed live provider |
| `direct` | `AzureOpenAIProvider` | Chat completions, client-side tools |
| `foundry` | `FoundryAgentProvider` | Foundry agents, both handoff shapes |

`ScriptedProvider` is a fixed state machine. It runs with the base package
dependencies and lets tests assert on orchestration rather than model output.
The records-only command-center observer does not need a mock model provider.

Foundry is reached over REST with `DefaultAzureCredential` rather than through a
client SDK. Preview SDKs churn; a deployment that breaks because a package minor-bumped
the week before is a deployment failure. The REST implementation also keeps the
wire contract explicit.

## Network isolation

The command-center template `infra/command-center.bicep` is private by default:
App Service public access is disabled, a private endpoint handles inbound
access, and a separate VNet integration subnet handles outbound traffic.
`defaultOutboundAccess=false` requires the supplied NAT gateway for explicit
public egress; private destinations still need working routes and DNS. The NAT
public IP is outbound-only, not an application listener.

This template does not create or isolate the existing Foundry project and
Fabric workspace. Configure their supported private-network paths separately,
retain Entra-only service authentication and Foundry managed-network isolation,
and verify connectivity from each executing identity. A governance exemption
tag is not a substitute for network controls.

### Fabric SQL and Azure SQL network boundaries

The reference implementation this project was compared against
([ZacharyZurloMSFT/agentic-pbi-error-triage](https://github.com/ZacharyZurloMSFT/agentic-pbi-error-triage))
isolates its state with a textbook Azure design: a VNet, a private endpoint on
the SQL server, a `privatelink.database.windows.net` private DNS zone, and
delegated subnets for the Function App and the Foundry agent runtime.

**It does not port here.** That design isolates `Microsoft.Sql/servers`, an ARM
resource that takes a private endpoint. This accelerator's state is a **Fabric
SQL Database** — a Fabric item, not an ARM resource. It has no
`Microsoft.Network/privateEndpoints` of its own and no
`privatelink.database.windows.net` zone to link. Copying the Bicep would produce
a VNet protecting nothing.

### Fabric private-link scopes

Fabric secures **inbound** access with private links at two scopes:

| Scope | Effect | Use when |
|---|---|---|
| [Tenant-level](https://learn.microsoft.com/fabric/security/security-private-links-overview) | Network policy across the entire tenant | **This accelerator** — the only scope that covers a Fabric SQL Database |
| [Workspace-level](https://learn.microsoft.com/fabric/security/security-workspace-level-private-links-overview) | One workspace mapped to a VNet; others stay public | Workspaces built from supported items — which this one is not |

Two entries on Microsoft's
[supported-scenarios list](https://learn.microsoft.com/fabric/security/security-workspace-level-private-links-support)
rule out workspace-level private links for this combination of items:

- **SQL databases** — "Tenant-level private links are available for SQL
  database, but currently, workspace-level private links are not available in
  SQL database." The state store is the one thing most worth isolating, and it
  is the one thing workspace-level scope does not reach.
- **Semantic models** — "Power BI semantic models aren't supported in workspaces
  with workspace-level private links enabled. If a workspace contains any Power
  BI semantic models, you can't enable workspace-level private links for that
  workspace." This cockpit's read path uses a semantic model, whose presence
  blocks workspace-level private links for the workspace.

So a workspace holding the SQL database, the semantic model and the cockpit
cannot have workspace-level private links enabled, and **tenant-level is the
only scope that covers this combination of items**. It is a tenant-wide network
policy, not a per-workspace change. Recheck the support matrix when planning a
deployment because platform support can change.

An earlier revision of this document recommended the opposite, having reasoned
from what the feature is for rather than from its support matrix. It is recorded
here because the unsupported item types were exactly the ones in use; the
feature's purpose did not establish support for the chosen resources.

Two settings in the admin portal govern the tenant-level behaviour — **Azure
Private Links** and **Block Public Internet Access** — and the second is the one
that actually closes the door. With private links configured but public access
still allowed, the workspace is reachable both ways; Microsoft's own guidance
calls that a testing configuration rather than a production one, because it
provides no inbound protection.

### Outbound connectivity

**A private endpoint secures traffic *into* Fabric. It does nothing for traffic
*out* of Fabric or the hosted applications.** The controller calls Power BI,
Fabric APIs, optional Microsoft Graph and Foundry/Azure OpenAI. Each needs its
own supported endpoint, DNS and egress policy. Enabling Fabric inbound Private
Link does not isolate those clients.

### Network provisioning scope

`infra/command-center.bicep` provisions the web app's Azure network resources,
not tenant-level Fabric Private Link or Foundry networking. There is no generic
`network.bicep` that protects the whole system. Fabric SQL is a Fabric item,
not an Azure SQL server resource, and an ARM private endpoint targeting an
unrelated SQL server would protect none of this state.

Temporary public access in the command-center deployment helper is explicitly
scoped to client host addresses and restored to Disabled afterward. It is a
deployment/verification exception, not the steady-state design. Existing
private endpoint DNS, peering and tenant settings still need to be supplied
and verified by the deployment operator.

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

**A new agent**: mirror `DataQualityAgent`'s typed boundary. Use its own provider
and prompt, controller-collected evidence and a tool-free interpretation call
where no additional tools are needed. Expose it to the orchestrator as one tool.
Re-register Foundry agents after prompt or tool-schema changes.

**A different durable store**: subclass the in-memory store and override
`_load`, `_persist` and `_on_reset`, as `FabricSqlIncidentStore` does. Keep
redaction inside `record`, and make sure a failed open can recover rather than
degrading for the life of the process. Coordination and live web stores instead
require fail-closed shared state; do not copy a record-store fallback into them.

**A real flag table**: replace `DataQualityFlagTable` with three methods against
the real table. Keep the CSV path for evaluation — a table you can open in Excel is
easier to show than a query result.
