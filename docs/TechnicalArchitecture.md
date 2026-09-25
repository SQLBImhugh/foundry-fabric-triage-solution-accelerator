# Architecture

All accelerator application state targets one shared **Azure SQL Database**.
Power BI semantic-model refreshes, scheduled Fabric pipelines and native Fabric
Job-event transport remain the monitored services. The Command Center is the
operational UI; Rayfin is not a state or deployment dependency. The shipped
infrastructure now uses public networking with Entra authentication. Scoped
evaluation SQL/registry access is verified. SQL recovery and proof/application
schema commits are complete; append-only adjudication leaves the original
failed receipt unchanged. The initialization maintenance capture is historical.
Current maintenance is `false`; Command Center and the hosted controller are
live on Azure SQL with the correct acting identity, kernel roles and application
grants. A bounded deployed heartbeat completed discovery-intent reconciliation,
published its frontier `1/1` and queued one inventory item with no actions or
receipt/control changes. A collector-only worker is now deployed and has durably
accepted 332 workspace records from native metadata reads. Broader item coverage
remains partial; enabled selectors do not prove source access or remediation
authority. Three actual recurrences of the reused heartbeat schedule completed.
Event-mode acceptance and sustained coverage remain open. Native paired
heartbeat metadata is now queryable through the app-owned Entra telemetry
channel with project content tracing disconnected. Earlier private-network
proofs and the private Foundry preflight error are historical, not prerequisites
or blockers for the chosen public architecture.
See the [release gates](DeploymentGuide.md#release-gates).

## Entry points and controller flow

`TriageRunner` owns client and store construction, signatures, open-incident
lookup, agent construction and terminal persistence. `TriageAgent` proposes
tool calls through a provider; `ToolDispatcher` and the shared `PolicyLedger`
decide which calls may execute.

| Entry point | Implementation | Boundary |
|---|---|---|
| Power BI alert mailbox | `tools/inbox.py`, CLI watch loop and `src/app.py` | Filtered Graph polling and processed-message tracking |
| Interactive alert | `src/app.py::TriageControllerAgent._triage_text` | Same runner and canonical target admission; an unbound diagnostic is not permission to act |
| Silent-failure sweep | `TriageRunner.silent_sweep` | Configured deterministic semantic-model probes |
| REST monitoring collector | `monitoring/inventory.py` and `monitoring/polling.py` | Registry-backed discovery, bounded history reads and durable source/work admission |
| Fabric Job events | `monitoring/events.py` and `monitoring/worker.py` | Managed-identity receiver, original event receipts and SQL checkpoints; REST verifies the referenced execution |
| Scheduled pipeline monitor | `TriageRunner.pipeline_sweep` | Admitted registry targets, scheduled failed jobs and activity evidence |
| Command-center investigation | `command_center/api.py` and `command_center/worker.py` | Authenticated request stored in SQL; the controller heartbeat drains queued work |

The Power BI triage decisions map to these components:

| Flow box | Implementation | Notes |
|---|---|---|
| BI Request Inbox | `tools/inbox.py` — `MockInbox` \| `GraphInbox` | Mock files or filtered polling, normalized to `BIRequest` |
| Data Quality Issue? | `consult_data_quality_agent` → `agents/data_quality_agent.py` | A separate agent, reached through a tool |
| Is There a Known Related Issue? | `monitoring/runtime.py::target_signature`, `signature.py` and the incident store | Canonical target identity plus a 16-char normalized failure signature |
| Wait for Resolution, Then Continue | outcome `duplicate_suppressed` | Increments the parent incident; no second remediation |
| Does It Qualify as Tier 1? | `TriageClassification.tier` | Model classifies; controller constrains what follows |
| Agentic Resolution | `ToolDispatcher` and `PolicyLedger` | Refresh, gateway binding or schedule restoration, subject to the action's gates |
| Is Issue Resolved? | `TriageAgent._validate_outcome` | Checks the claim against the evidence |
| Send Resolution Summary | `notify_teams` and recorded terminal result | Report, error, action, outcome, timestamp; web delivery does not require Teams |
| Human Involvement | Approval gate or outcome `needs_human` | A person can approve an allowlisted proposal or investigate; no unrestricted human repair workflow is automated |

## Shared monitoring registry

`triage.monitoring` separates inventory, source evidence, coverage and action
authority. Discovering an item does not establish that its history is readable,
that its monitoring is current or that the controller may remediate it.

| Component | Responsibility |
|---|---|
| Models and store contracts | Typed target/execution/incident identities, tenant/epoch/revision, scopes, work, reviews and operation receipts |
| Inventory collector | Named tenant/domain/workspace/item metadata, pagination generations, access probes and supported-workload classification |
| REST poller | Bounded pipeline/Power BI history windows, source ID correlation, durable cursors and explicit retention/completeness gaps |
| Eventstream reconciler | Desired versus observed sources in app-owned topology; existing component IDs and destination metadata remain bound to that ownership |
| Event receiver | Pinned managed identity, original CloudEvent receipts, partition ownership, quarantine and checkpoint-after-acceptance |
| Controller | Fresh source verification, common admission, policy/approval checks, atomic action reservation and durable finalization |
| Monitoring API/UI | Readable coverage, scope preview/activation, safety reviews and original-request reconciliation; no browser remediation or permission grants |

The worker has an explicit collector-only mode for inventory/REST polling before
Eventstream setup. Its connector may be absent only in that mode; partial event
metadata is rejected. It creates no receiver/provisioner, uses a zero-write
policy with empty action allowlists and emits a durable heartbeat with
`connector_id=null`, which cannot assert transport or delivery. Event mode
retains complete owned-binding checks.

Inventory acceptance uses bounded checked-view inserts of at most 50 new rows/
950 parameters under the same lease and atomic transaction; a short rowcount
rolls back. Lease renewal re-reads the authoritative work revision. Receipt-backed
catalogue reads retain exact accepted-fact identity, fingerprint, payload and
row-hash evidence while avoiding repeated full-catalogue work. This changes
metadata-read cost, not admission or permissions.

Scopes can include or exclude tenant, domain, workspace or item selections.
Exclusions take precedence; future-resource admission is explicit. Domain
membership is metadata, not a permission grant. A complete empty scope differs
from one that could not be enumerated. Unsupported item types remain visible
as unsupported rather than being treated as operationally monitored.

Power BI refreshes and scheduled Fabric pipeline executions are the supported
failure workloads. Notebook activities can supply pipeline evidence; this is
not standalone notebook monitoring or a universal Fabric audit subscription.
Native events and REST polling are complementary. Neither Activator, Power
Automate nor Eventhouse is required by this path.

Live configuration is held in Azure SQL. `MONITORING_MODE=fixture` selects an
explicit offline registry; it is never a fallback for a failed live connection.
Static live pipeline target environment lists are not imported or retained as
a second source of truth.

Every deployment has a tenant, epoch and activation cutoff. A reset creates a
new epoch, not a migration of prototype history. Old mail, events, job history
and approval links do not become fresh action opportunities merely because
their local records were cleared.

The hybrid implementation and its live acceptance are tracked in
[HybridMonitoringPlan.md](HybridMonitoringPlan.md). Successful infrastructure
provisioning does not establish event delivery or durable end-to-end operation.

### Monitoring engine and persistence adapters

`monitoring/engine.py` owns the shared monitoring rules and the synchronous
operation transaction. `monitoring/records.py` defines the record interface.
`monitoring/adapters.py` defines the required semantic operations: native receipt
reads, evidence projections, guarded mutations and reconciliation. The explicit
offline adapter is in `monitoring/memory.py`; the Azure SQL adapter is in
`monitoring/sql_store.py`.

The public store constructors select an adapter and retain the existing caller
interface. They do not override shared rules. Every semantic operation is
abstract until that adapter implements it, so adding an operation without its
SQL implementation fails construction rather than inheriting an offline default.
This addresses the earlier overtaken-observation check that read an
offline-only record family and silently found nothing in a restricted SQL view.

SQL retains its native procedures, checked views, receipt formats and transaction
rules. The offline adapter does not stand in for native SQL permission acceptance.
Tests across both adapters remain necessary: they exercise shared safety
invariants while preserving legitimate differences in original-receipt and
pending-frontier handling.

Callers receive a role-specific interface. `MonitoringReader` carries shared
queries; `WebMonitoringStore` adds configuration intents, `WorkerMonitoringStore`
adds collection and intake, and `ControllerMonitoringStore` adds publication and
guarded actions. Worker and controller interfaces share work ownership operations.
Receiver ownership/health uses the existing `EventPersistence` interface.
The store factory advertises the selected role in its return type, and web tests
use a web-only fake rather than a fake with controller methods.

The aggregate `MonitoringStore` contract remains for explicit fixture setup and
compatibility. These interfaces improve caller locality; they grant no authority.
Runtime operation checks and native SQL permissions remain mandatory, including
when Python code bypasses an interface through dynamic attribute access.

Queue selection uses a separate `controller_queue_read` view containing only
controller work and the action records needed to respect verification deadlines.
It retains the current tenant/epoch join and has a controller-only SELECT grant.
It does not expose raw worker evidence or permit queue DML.

The mixed `controller_read` view validates accepted worker facts, including
original receipt hashes. An actual queue query plan evaluated that evidence
branch despite returning no queue rows from it. Queue selection must not pay
that cost or weaken those evidence checks. Empty selections now return before
reconstructing fairness from historical work/transition receipts; nonempty
selections retain the existing workspace rotation, quotas and native claim.
Install the new view and read grant before deploying a caller that requires it;
a missing projection fails bootstrap rather than selecting the expensive route.

Heartbeat context lookup, queue claims and synchronous reconciliation run off
the event loop. The human-command drain uses the same cancellation-settlement
helper for its SQL reads and writes. Cancelling the awaiting coroutine stops new
work but does not cancel or replay a submitted synchronous operation: the helper
observes its original executor future before propagating cancellation. A failure
that arrives during settlement remains visible as metadata; transaction and
receipt recovery still belong to the store. Process termination is distinct
from cooperative cancellation and still requires durable recovery.

### SQL writer boundary

Runtime monitoring stores require an explicit component. Database permissions
separate producer evidence from controller authority; a Python class or a
table-level DML grant over mixed state is not that boundary.

| Component | Permitted write path |
|---|---|
| Worker | Checked inventory, REST, stream and topology observations, with lease and receipt checks |
| Web | Validated human configuration/review intents and their immutable operation receipts |
| Controller | Deterministic publication, admission, source disposition, action reservation and terminal finalization |
| Deployment operator | Schema installation, protected writer registration and separately reviewed reset |

Checked single-writer views and fixed ownership-chained procedures expose these
paths. Runtime identities receive no general monitoring-table DML or schema
creation rights. The SQL adapter binds named arguments through the current
kernel contract and decodes its typed result; an `EXEC` rowcount cannot establish
success. Native permission acceptance must use separate real Entra connections
to Azure SQL. Azure SQL supports `CREATE USER ... WITHOUT LOGIN` and
`EXECUTE AS USER` for database-scoped testing, but those do not prove deployed
MI sign-in, network/firewall admission or reconnect behavior. Runtime principals
receive no impersonation grants. Earlier Fabric SQL CREATE/rollback results
are historical and do not establish the new platform's acceptance.

The heartbeat processes targetless `reconcile_state` work before execution
admission. Configuration reconciliation does not invent an execution target.
Completing producer collection does not validate its facts: a protected pending
source-validation frontier continues to fence new actions until the controller
publishes the original evidence.

A page decision and window completion are separate facts. Inventory pagination
can advance a generation after the controller has published an earlier page.
Retrying that page must not reinterpret the newer generation as a reason to
replace the original decision. While the same-policy window remains unfinished,
`pending_window_acknowledgement` proves the original intake and page-decision
receipts under the current work fence, returns `pending_validation`, and defers
the work. It does not change the handoff, window, frontier, source or action
authority. A later complete page or an explicit current whole-window rejection
still has to close the window. Missing or contradictory original receipts fail
closed; an acknowledgement is not a repair route.

An accepted safety-review intent has `publication_status=pending_validation`;
it is not a verified review. Pending revocation immediately denies new action
reservations, while existing reservations can still verify and finalize without
refunding approval or remediation budgets. Reconciliation of an original request
returns its original receipt, not a later review that happens to use the same
target or profile. Current published authority is a separate lookup.

A structurally valid verification intent may be accepted after its review window
has expired, but the controller publishes it as `unverifiable` and leaves actions
disabled. An expired revocation remains valid denial intent and retains the
original `reviewed_at` and `expires_at`; it cannot renew authority by rewriting
those timestamps.

### Eventstream source publication

The controller calls `prepare_connector_reconciliation` with its current
reconciliation context. This preparation module owns desired-versus-binding
selection, unchanged-intent detection, one publication-state read and the
original observation lookup. Resolved preparation state stays private; callers
do not pass cached state between preparation functions.

Source-removal supersession checks the authoritative clock before and after
target reads. Evidence that expires during those reads takes the existing
durable rejection path without attempting publication. The native publication
guard still checks freshness in its own transaction; no Python handler writes
after a failed native guard, and the 300-second evidence lifetime is unchanged.

Already-created app-owned transport uses the
[operator metadata registrar](DeploymentGuide.md#register-existing-app-owned-connector-metadata).
Its reviewed original create and fresh complete item/definition/topology capture
bind physical ownership; hashes do not establish collector identity, network
access or delivery. It publishes neither protected desired state nor admission
or readiness, and does not queue controller work.

Before first protected desired publication, retained physical sources remain
dormant without current admitted read/event-capable targets. They are not
automatically removed because that admission set is empty. Once current
admission and capability exist, first publication can occur at the same policy
revision when desired state was absent; no artificial scope edit is required.
Fresh same-collector-MI topology/source-Running and read probes establish event
capability only, leaving action capability unchanged.

Source additions begin as logical proposals without caller-invented physical
component IDs. The controller publishes desired topology while holding its
work lease. The worker applies only that published intent and records the
original remote operation and observed definition. The controller then binds
the observed physical IDs through the original observation receipt; a worker's
successful API call alone cannot publish effective readiness.

A degraded receiver can collect first-delivery proof only against the matching
current protected publication, ownership, policy, definition, source and endpoint.
Publication-identity change requires re-verification. Identity-check time and
subsequent receive time are separate facts. Controller Ready requires the exact
original durable stream receipt/position and accepted hash, bound to actual
work/context, owner, fence and revision; a heartbeat, empty/quarantined stream,
stale receipt, connector equality or inferred global lease is insufficient.
These requirements do not constitute new native registration/apply or
event-runtime acceptance.

Desired removal immediately fences intake but retains physical ownership.
Only a current, complete, original definition observation proving exact absence
of the owned node, physical ID and stream reference permits retirement and an
immutable tombstone. Partial, inherited or uncertain observations do not prove
absence. Mixed additions/removals preserve unrelated owners, and replay of an
old receipt cannot replace newer state.

### Affirmative removal authority

Desired planning receives explicit `removal_targets` from effective current
policy or recorded deletion authority. A disabled/excluded target can authorize
contraction only after overlap and exclusion rules are resolved; a still-valid
overlapping include is not a removal instruction. Unknown domain authority
holds the decision rather than inventing permission.

Expired or unknown read capability, incomplete inventory and omission from an
eligible-target list are uncertainty, not physical removal authority. Retain
the owned source and expose the gap while intake, action and readiness guards
remain enforced. A source's physical presence likewise does not grant intake.

### Receipt-bound source-removal restoration

This is an implemented, offline-tested source contract still under review,
not a claim that a live pending removal has been restored or a receiver resumed.
Only the controller can submit `source_removal_supersessions` through its
existing publication boundary. Each selector is exactly
`{removal_id, source_id}` for a retained physical source and its original
`pending_remote_absence` record. Selected identities are unique and disjoint
from remaining removal requests. Preserve all unselected pending intents and
source ownership; do not mix restoration with new additions, proposal binding
or retirement. Unresolved logical source proposals block this narrow path.

Restoration needs the **original**, complete `worker.observe_connector` receipt
and its producer/work binding, not a current manifest or an inherited snapshot.
The separate `ConnectorPresenceInspection` contains `read_only=true`, the actual
GET `observed_at`, a SQL-compatible `definition_hash`, and a complete
`component_states` map from canonical physical GUIDs to `Running`. The inspection
must postdate the selected removal and be no more than 300 seconds old. Its
definition hash must match the original SQL-derived observation result.

The publication transaction checks all of these boundaries:

| Boundary | Required evidence |
|---|---|
| Original ownership | Exact removal/source/node/target/transport binding and original removal publication/receipt; the full current owned graph must be accounted for |
| Never dispatched | Complete, unique retained revision receipts from the original removal, with no applicable operation ID or write-ahead `INTENT_GAP`/unknown-write marker |
| Inspection completion | Original read-only collection completed under its exact released owner/fence and completion receipt |
| Other connector work | No active or unreconciled attempted work; a queued candidate has `attempts=0`, `retry_attempt=0`, no lease payload, no physical lease row or tombstone, and no target/execution/action/retry/finalization lineage |
| Current scope/read | Current enabled observation admission, `reviewed` or `auto_detection_only`, fresh verified READ capability and an affirmative directly matched current scope; no explicit denied/blocked capability |

`attempts=0` alone does not prove a queued operation was never claimed.
The completed inspection's retained released lease is evidence for that
inspection, not permission to ignore a lease tombstone on other queued work.
Because `INTENT_GAP` is persisted before POST, it represents possible submission;
even a later clean GET cannot erase that historical uncertainty. This path
does not adjudicate already dispatched/possibly applied removals or older
writers outside the recorded boundary.

The current memory/SQL restoration checks directly match tenant, workspace or
item includes bound to the admitted scope/rule. Domain-only admission does not
qualify; domain exclusions are conservatively blocking in this path.
Unknown scope/ancestry must remain held, not bypassed by widening a selector.
Only missing/unknown event capability for the selected retained source may
defer to fresh post-publication proof. Explicit denial, blocking or unsupported
capability is not waived, and new additions retain their ordinary gates.
Restoration grants no action capability or remediation budget.

Acceptance writes a **new unready desired publication and timestamp** and an
immutable receipt containing `superseded_source_removals`: the exact original
pending-removal audit records. Original receipts/history and physical source
identity remain unchanged; present sources are not labelled retired. Only
proved never-claimed queued connector work may be dispositioned in the same
transaction. Identity/delivery proofs are cleared, and receipt failure rolls
back the whole publication.

Rearming is separate. Restored sources need current admission plus new
post-publication verified read/event capability, matching collector identity/OID,
transport identity and valid receive/enqueue times. An actual accepted original
stream receipt/position/hash then supports controller readiness publication.
The first qualifying delivery must not require already-published Ready or
`events_enabled`, because that delivery supplies the readiness proof.
Later publication cannot discard earlier restoration audit/fences.
Never erase a pending intent, patch SQL, re-register the connector or reset
state to force a healthy result.

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

An admitted target is identified by its tenant, epoch, workload, workspace and
item IDs. Its display name is a label. The current target-key wire format is
`monitor:v1:{epoch}:{tenant_id}:{workload}:{workspace_id}:{item_id}`.

The deterministic failure signature remains:

```text
sha1(source | artifact_kind | canonical_target_key | exception_class | normalized_error)[:16]
```

`monitoring.runtime.target_signature` supplies this immutable key to
`compute_signature`. An execution identity adds an explicitly typed source-run
ID; an incident identity combines the target with its normalized failure
signature. Power BI numeric history IDs and request GUIDs are different
namespaces, and adapters must resolve their aliases with authoritative evidence.
The controller's own remediation job ID is a separate identity again.

Normalization strips GUIDs, timestamps, line numbers, URL paths, IPs, hex
suffixes, temp paths, long hashes and request IDs. Case is preserved — SQL
identifier case is significant in some dialects, and folding it merges genuinely
distinct failures.

Two equally named items in different workspaces cannot share a live incident,
and a rename cannot grant a new remediation budget. Diagnostic records without
an admitted execution can still use an artifact label, but that diagnostic
signature does not authorize a live action.

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

**Who can answer.** Web and optional Teams approval channels have different trust
boundaries:

| Channel | Needs | Use |
|---|---|---|
| Command-center decision controls | Valid delegated API token with Approver or Admin app permission | Authenticated, fingerprint-bound web decisions; responder comes from the token |
| `bi-triage approve` / `deny` | Local state access offline, or the operator's Entra SQL permissions live | Operator channel, not browser authentication |
| Teams callback buttons | `APPROVAL_CALLBACK_URL` | Bearer-link callback; supplied responder text is not an Entra-verified person |

`APPROVAL_DELIVERY_MODE=web` uses the command-center decision path and does not
require Teams. Optional Teams delivery links to that web proposal. The
SQL callback procedure explicitly refuses web proposals and any row without an
explicit `teams` delivery channel.

The callback buttons are `Action.OpenUrl`, not `Action.Submit`. A card posted
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

**Historical callback write path.** The earlier recording workflow used the SQL
managed connector against Fabric SQL as its own system-assigned identity. This
optional bearer-link integration is not part of the new secretless cutover and
has not been proved against Azure SQL. Managed
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
   AND JSON_VALUE(payload, '$.delivery_channel') = 'teams'
   AND @decision IN ('approve', 'decline')
   AND NULLIF(LTRIM(RTRIM(@responder)), '') IS NOT NULL
   AND (decision IS NULL OR decision = '')           -- unanswered, exactly once
   AND DATALENGTH(@fingerprint) = 128                -- required 64-char fingerprint
   AND (... COLLATE Latin1_General_100_BIN2
        = @fingerprint COLLATE Latin1_General_100_BIN2)
   AND NULLIF(JSON_VALUE(payload, '$.consumed_at'), '') IS NULL
   AND (... expires_at > SYSDATETIMEOFFSET())        -- explicit, unexpired window
```

`@@ROWCOUNT` tells the workflow whether it won. Zero means unknown, already
answered/consumed, expired, invalid or fingerprint mismatch; these render as a refusal and
none changed anything. A failed write renders as a failure rather than falling
through to a success page. The agent revalidates all of it independently.

The preceding callback deployment was verified against a live Fabric SQL
Database: GET renders the page and
changes nothing, POST records, a second POST is refused with the first decision
intact, and mismatched-fingerprint, expired and unknown requests are all
refused. That historical result proves neither the Azure SQL deployment nor a
changed procedure definition; the stricter current predicate requires its own
deployment/readback proof.

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
In live mode a due retry still enters canonical monitoring admission. A prior
deferral cannot bypass a revoked scope, changed safety review, newer source
execution or an existing action fence.

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

### Shared Azure SQL application state

All live application state lives in **one Azure SQL Database** on an Azure SQL
logical server. It is an Azure resource, independent of Fabric workspaces and
the UIs. The selected prototype switch imports no Fabric SQL history and keeps
no compatibility adapter, legacy setting aliases or mixed-version path.

SQL replaced Azure Table Storage in the earlier design. Its relational and
atomic-operation requirements still apply:

* **The state is relational.** An incident has occurrences, an approval belongs
  to an action, a deferred retry belongs to a signature. An operator asking
  "which reports failed most this quarter, and were they the ones we retried"
  can answer it in one query against the application database,
  instead of exporting a key-value table first.
* **A conditional `UPDATE` is atomic on its own.** Claims and leases used to be
  read-then-write guarded by an ETag: three round trips and a race the code had
  to reason about explicitly. `UPDATE ... WHERE expires_at < SYSUTCDATETIME()`
  is one statement, `rowcount` says whether this caller won, and the expiry is
  evaluated on the server, so it does not depend on any container's clock. A
  primary-key `INSERT` raising `IntegrityError` gives the same compare-and-set
  the old code got from `ResourceExistsError`.
* **Cross-store commits share one catalog.** Incident persistence, processed
  source disposition, monitoring work and their receipts need one transaction.
  Separate web/worker/controller databases would split that boundary. SQL
  roles and checked interfaces separate their authority within the same database.
* **Authentication is configured as Entra-only.** Azure SQL also supports SQL
  authentication. The logical server must explicitly enable Microsoft
  Entra-only authentication; runtime configuration has no SQL login, password
  or credential-bearing connection string. This preserves the no-shared-key
  requirement that the earlier storage design encountered under governance.

Set `AZURE_SQL_SERVER=<server>.database.windows.net` and
`AZURE_SQL_DATABASE=<database-name>` from the Azure deployment. Configure the
server Entra administrator separately from application roles, configure the
public SQL firewall and enable auditing before runtime startup. The Command
Center Admin role grants no SQL server administration.

An optional temporary proof database may share the logical server but never
operational state. One database per component is not the design. The provisioned
evaluation topology is not a final sizing or pricing recommendation. The shipped
template uses one S1 application database and an optional Basic proof database,
not an elastic pool.

The deployment operator installs the application schema and explicit monitoring
baseline. Neither the controller, monitoring worker nor web identity creates,
upgrades or repairs schemas at startup. Missing or incompatible state is a
visible deployment failure, not a fallback to local files.

| Table | Holds |
|---|---|
| `triage_incidents` | every terminal outcome, keyed by incident id, indexed on (signature, status) because `find_open` runs before every remediation |
| `triage_processed_messages` | which alert mail has already been triaged |
| `triage_approvals` | approval requests and the decisions written against them |
| `triage_deferred_retries` | work postponed by capacity backoff |
| `triage_semantic_health` | silent-failure baselines |
| `triage_data_quality_flags` | redacted flags with deterministic request/evidence identity; controller-only runtime append |
| `triage_sweep_leases` | one sweep at a time, across instances |
| `triage_claims` | one invocation acts, across instances |
| `triage_inbox_audit` | what the inbox filter refused, and why |
| `triage_pipeline_reruns` | one approved submission per failed pipeline run, plus correlated execution verification |
| `triage_agent_runs` | individual run metadata and the full typed terminal result |
| `triage_agent_events` | redacted progress and tool-result events, linked to a run |
| `triage_agent_commands` | idempotent operator requests, conditional execution state and reconciliation audit |
| `triage_incident_activity` | append-only notes, source-revision-bound human resolutions, questions and answers |
| `triage_monitoring_control` | singleton tenant, schema version, epoch, registry revision, activation cutoff, maintenance and bootstrap identity |
| `triage_monitoring_records` | typed scopes, inventory snapshots, targets, work, source evidence, reviews, action state, coverage and connector records |
| `triage_monitoring_leases` | shared owner/fence/expiry state |
| `triage_monitoring_receipts` | original operation identities, fingerprints and committed results |
| `triage_monitoring_rate_budget` | shared service/API request budgets, retained across a prototype state reset |
| `triage_deployment_registration`, `triage_deployment_writers` | protected deployment/identity bindings and registration evidence, retained across operational resets |

Most record tables carry promoted filter columns plus a JSON `payload`. Claims
and leases use dedicated columns. Command execution columns changed by
conditional SQL statements take precedence over stale payload copies when a
record is reconstructed.

The SQL access-grant table and service are retired. App authorization does not
read operational tables to discover user roles. The state database remains an
independent Azure SQL database; changing command-center authentication or removing
a web frontend must not remove incidents, approvals or execution journals.

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

### Fail-closed durability and transaction ownership

`store/durability.py` concentrates database selection and persistence-health
interpretation for the runner, monitoring runtime and Command Center. Explicit
fixture selection constructs no SQL handle; live selection requires a shared
handle or complete connection settings. It never tests connectivity to choose
between SQL and a local adapter.

Selection and confirmation are different facts. The legacy `is_durable`
property can become false after a read or write fails. Callers therefore use
`persistence_confirmed` or `require_shared_persistence` at the point where they
need shared state, not only at startup. A malformed health value is an error,
not truthy success. These checks do not prove a particular write committed:
affected-row checks, original receipts and authoritative recovery remain in
the adapters. Live runner construction also rejects an injected local incident
store before it can authorize work.

All affected live record and coordination stores fail closed. An unreachable
database cannot be replaced by process-local incidents, approvals, processed
messages, retries, baselines, claims or command-center state. Recovery retries
the connection on use and reads authoritative state; it does not replay a
cached mutation whose original commit may have succeeded.

The previous implementation opened its client once in `__init__` and, on
failure, stayed in-memory for the life of the process. Tenant policy disabled
public network access on the state store minutes after it was created; the
container started while it was unreachable, and then reported healthy triage
outcomes while persisting none of them. Restoring connectivity changed nothing,
because nothing ever tried again. Three invocations were lost before a forced
redeploy fixed it.

Recovery requires a fresh read, not just a reconnect. `find_open` is what
stops the agent remediating the same failure twice, and an empty cache answers
"no open incident" to everything. The SQL store tests cover recovery and
fail-closed behavior rather than treating an in-memory result as success.

`AzureSqlDatabase.transaction()` is synchronous and thread-bound. Store calls
inside it share the same connection; no await, nested transaction or reconnect
is allowed. A caught statement failure or interruption still aborts the
transaction. An uncertain commit discards the connection and requires durable
receipt reconciliation, not blind replay. Interrupted commit/rollback
acknowledgements preserve cancellation or process-exit propagation, add an
explicit reconciliation note and never restore autocommit on the unresolved
connection.

Action reservation checks current scope, review, source head, work lease,
approval and budget in one transaction. Terminal finalization similarly binds
the incident, source disposition and work completion. A persistence timeout
leaves work unfinished and retains any action fence; recovery finalizes or
verifies the existing action instead of issuing another one.

## Claims: only one invocation acts

The original incident and processed-message paths checked state before doing
work and wrote it afterward. That was sufficient for one process, not two.

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
cannot both get 1. A previous live check with eight concurrent threads produced
exactly one winner; the monitoring registry's additional transaction boundaries
require separate current-release proof.

Claims expire so a failed controller does not hold work forever. An action
reservation does not expire merely because its worker lease does: another
worker may verify or finalize the original effect, not repeat it.

Claims and the other live stores now share the fail-closed rule. Losing incident
or processed-source state can also authorize repeated effects, not merely
produce extra notifications.

### Common admission across entry points

Path-specific claims still limit duplicate scheduling, but they are not the
cross-entry-point action boundary:

| Path | Scheduling identity | Action authority |
|---|---|---|
| Mailbox sweep | Message identity | Canonical admitted target and source execution |
| Deferred retry drain | Retry/signature identity | Fresh common admission and existing action-fence check |
| Pipeline and Power BI polling | Durable history position and source execution | Verified current source state, not the observation's arrival order |
| Native events | Original event identity and stream position | REST-verified execution; an event alone authorizes no action |
| Queued human investigation | Command identity and command-row ownership | Current target scope/review and shared reservation |
| Interactive alert | Runner-resolved target and source evidence | Same shared admission; unresolved context remains diagnostic only |

The retry drain was unclaimed until recently, which was the sharper of the two
gaps: `due()` and `complete()` are separate statements, so two replicas
draining at the same moment both saw the same row as due and both issued a
dataset refresh. The claim is held until the row is completed or re-deferred,
not merely until the refresh returns — releasing at the refresh would let a
second drainer see the row as still due.

The former interactive-alert gap came from treating path-specific keys as if
they serialized one another. A Playground request has no mailbox message ID,
and a command ID is not a source-run ID. The runner now binds native work to
canonical monitoring identities before it reaches the dispatcher.

Reservation is the action decision point. It atomically checks tenant/epoch,
maintenance, current admission, safety review, current source head, work
ownership, approval and budget. If revocation wins, the effect is refused
without consuming its approval or remediation budget. If reservation wins,
a later disable cannot retract the already committed external action.

Power BI refresh and pipeline rerun submissions retain the exact returned job
identity. A concurrent unrelated refresh cannot establish success. Gateway
rebinding and schedule restoration instead retain a reviewed configuration
intent and verify the exact bindings or schedule through GET readback; they
do not invent a job ID for a configuration mutation.

A lost acknowledgement, 5xx response or transport error preserves uncertainty.
Only explicitly classified definitive rejection can enter the store's bounded
linked-successor policy. It does not restore an already consumed approval or
turn an unverified action into a successful remediation.

## Scheduled Fabric pipeline failures

Registry-admitted pipeline monitoring feeds the same `TriageAgent` and `ToolDispatcher`.
It reads scheduled failed job instances and activity evidence, using immutable
workspace/pipeline/run identifiers rather than model-supplied targets.
`PIPELINE_ACTIONS` excludes Power BI dataset actions from this path.
Notebook activity failures contribute evidence within these configured
pipelines. There is no standalone notebook monitor or notebook-editing tool.

Canonical monitoring admission and the pipeline-scoped claim serialize work.
A non-expiring SQL action reservation prevents a second POST for the same failed run,
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
The UI prefers system Georgia, with DejaVu Serif and self-hosted DejaVu Serif
Condensed fallbacks, Onyx dark/light colors,
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

Monitoring setup uses the same roles: Readers inspect scopes, coverage and
existing safety reviews; Admins preview/activate configuration and save/revoke
reviews. An Admin safety review enables an action capability, not an individual
approval. Approver remains a separate per-proposal decision role. Uncertain
configuration writes are reconciled by their original operation receipt; an
older current review cannot confirm that a newer revocation committed.

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

The API records commands and web decisions in the shared Azure SQL
database. A controller command sweep performs the work. Target-level claims
prevent overlapping command executions; durable interrupted rows retain
uncertainty after a timeout or process loss. An administrator must reconcile
the actual target state before clearing that barrier. Reconciliation records
an audit entry and never executes a tool.

Web proposals are excluded from the Teams callback procedure. Optional Teams
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

See [CommandCenter.md](CommandCenter.md) for deployment, role grants, public
HTTPS and optional caller filters, operator workflows and validation boundaries.

## The monitoring cockpit

`cockpit/` is a retained read-only [Fabric App](https://github.com/microsoft/rayfin)
sample for incidents, approvals, deferred retries, semantic-health baselines,
claims and leases. It has no trigger buttons, reset or scripted scenarios.
It is not required for the Azure SQL deployment, and its earlier semantic-model
binding has not been retargeted or verified against the new application state.

### Historical semantic-model read path

This cockpit uses `@microsoft/fabric-app-data`'s
`FabricClient.semanticModel()`. In the tested embed host,
`IFabricApiProxy` declared `lakehouse.executeSql` and `warehouse.executeSql`
without working implementations. The earlier release therefore used:

```
Fabric SQL Database        earlier-release controller state, over TDS
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

The cockpit adds no writer. In that release, projecting rows into the app's own
store was rejected: the tested Fabric app accepted Fabric SSO only, without a
headless controller write path, and a second operational copy could diverge.
Rayfin `data.enabled` remains false.

Azure SQL provisioning does not reproduce that Fabric SQL auto-mirroring path
or create a new semantic-model binding. No state-copy or compatibility path is
included. An existing cockpit can still display prior-release data; that is not
current operational evidence. A future read-only reporting integration would
need separate source, network, permissions and latency verification.

Direct Lake avoids a separate import-refresh schedule, but mirroring and
semantic-model framing still introduce read latency. The command center reads
the operational Azure SQL store directly through its API; the cockpit does not.
The mirroring latency description applies only to the historical read path.

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

## Public networking

All shipped Bicep uses normal public networking. No private endpoint, VNet,
subnet, NAT Gateway or private DNS is a deployment prerequisite. Authentication
and authorization remain independent of network reachability.

| Component | Network and identity boundary |
|---|---|
| Azure SQL | Public endpoint, Entra-only authentication, TLS 1.2 minimum, Proxy/TCP 1433, explicit firewall admission, auditing and TDE |
| Foundry | Public account/project/model with local authentication disabled; no managed-network injection or network-approver dependency |
| Registry | Public Basic ACR with admin/anonymous access disabled, Entra ARM authentication and scoped MI pull permission |
| Command Center | Public HTTPS; validated API app roles and Entra-authenticated SCM publishing, with independent optional app/SCM client filters |
| Worker | Public Consumption environment and keyless Azure Monitor routing; one selected MI and no ingress |

`publicAccessClientCidrs` and `scmAccessClientCidrs` are separate optional arrays.
An empty list leaves that endpoint's network public; it does not grant an API
role or deployment access. Public worker networking does not create a listener.

Fabric Eventstream custom-endpoint destinations do not support tenant/workspace
Private Link. The selected receiver uses public outbound TLS with Entra, without
a separate Azure Event Hubs namespace. This transport does not alter the source
workload's own access requirements.

Endpoint namespace, entity and consumer group are nonsecret metadata. The
documented connection endpoint can return keys alongside those fields, so it
is not used as a supposedly key-free discovery shortcut. Bootstrap from the
Entra-only metadata surface and bind the values to the owned destination;
automated key-free metadata discovery remains a platform proof gate.

### Public SQL firewall and governed exceptions

The application-state logical server is `Microsoft.Sql/servers`, with public
network access enabled. Use `<server>.database.windows.net` with certificate
validation. `allowAzureServices=true` is the default: it creates Azure SQL's
special start/end `0.0.0.0` firewall rule. That permits Azure-hosted callers,
including other subscriptions, **not all Internet IPs**. It is not an
identity or tenant boundary; Entra authentication and database permissions
remain mandatory. Optional `clientFirewallRules` admit exact IPv4 ranges.
See [SQL firewall rules](https://learn.microsoft.com/azure/azure-sql/database/firewall-configure)
and [connectivity architecture](https://learn.microsoft.com/azure/azure-sql/database/connectivity-architecture).

Ordinary deployments do not inherit MCAPS exemptions. Optional
`sqlNetworkExceptionTags`, `registryExceptionTags` and
`accountNetworkExceptionTags` apply only to the explicitly selected resource.
The approved MCAPS SQL exception uses `SecurityControl=Ignore` with reason/review
tags for one 14-day period; removing/re-adding the tag does not restart it.
Longer-running tests require an approved exclusion. These exceptions change
neither SQL identity permissions nor TLS/auditing. See
[governed evaluation exceptions](DeploymentGuide.md#governed-evaluation-exceptions).

On 2026-09-17, scoped evaluation SQL/registry public access was enabled and read
back; the public Foundry path is retained. This is network-access evidence, not
schema recovery, runtime permission or application-cutover proof.

### Optional private-network hardening: historical scope

Private endpoints and private DNS may be part of a separately reviewed hardened
deployment, but are not emitted by this accelerator's public templates.
For Azure SQL, that design would use the `sqlServer` private-endpoint subresource,
`privatelink.database.windows.net` DNS links and independently verified runtime
routes. It must keep the normal server hostname and certificate validation.
See [Azure SQL private endpoints](https://learn.microsoft.com/azure/azure-sql/database/private-endpoint-overview).

That optional Azure pattern also appears in the public reference
[ZacharyZurloMSFT/agentic-pbi-error-triage](https://github.com/ZacharyZurloMSFT/agentic-pbi-error-triage).
Its resource layout does not prove this deployment's network path. The previous
Fabric SQL state store was a Fabric item, so Azure SQL private-endpoint Bicep
could not isolate it. That historical limitation no longer describes the
selected application-state platform.

The earlier private Foundry attempt created foundation resources but encountered
a managed private-endpoint service preflight failure after verified
prerequisites. That investigation is historical; it does not block the retained
public Foundry controller path or establish categorical lack of private-hosting
support. A separately designed private hosted path must account for its managed
self endpoint for model/project calls, SQL and registry access independently
from web ingress. Do not broaden roles or rewrite platform-managed networking
to guess at a fix. Earlier private infrastructure has not been deleted.

### Optional Fabric private-link scopes

Fabric offers **inbound** workload hardening at two scopes. These are separate
from the accelerator's public Azure state/hosting baseline:

| Scope | Effect | Use when |
|---|---|---|
| [Tenant-level](https://learn.microsoft.com/fabric/security/security-private-links-overview) | Network policy across the entire tenant | Required Fabric workload/API support and tenant-wide policy have been reviewed |
| [Workspace-level](https://learn.microsoft.com/fabric/security/security-workspace-level-private-links-overview) | One workspace mapped to a VNet; others stay public | Only the currently supported workload/item combinations are present |

The earlier Fabric SQL/cockpit design could not use workspace-level private
links: the reviewed support matrix excluded both SQL database items and
workspaces containing Power BI semantic models. Tenant-level support was the
available scope for that combination, not a per-workspace switch. An earlier
recommendation had incorrectly inferred support from the feature's purpose.
Keep that lesson, but do not use it to choose networking for Azure SQL.

Recheck the public
[supported-scenarios list](https://learn.microsoft.com/fabric/security/security-workspace-level-private-links-support)
for the Fabric items actually monitored or queried. The retained cockpit's
historical topology is not a deployment prerequisite. The selected Eventstream
Custom Endpoint still lacks tenant/workspace Private Link and uses the
separately reviewed public outbound TLS path.

Two settings in the admin portal govern the tenant-level behaviour — **Azure
Private Links** and **Block Public Internet Access** — and the second is the one
that actually closes the door. With private links configured but public access
still allowed, the workspace is reachable both ways. For a private-only rollout,
Microsoft describes that as a testing configuration: it does not establish
private-only inbound protection. This caveat does not make private networking a
prerequisite for the selected public accelerator.

### Outbound connectivity

The controller calls Power BI, Fabric APIs, optional Microsoft Graph and
Foundry/Azure OpenAI through their public service endpoints. Verify DNS, TLS,
service firewall admission and each executing identity. Standard public
Consumption hosting supplies the worker's outbound path; no dedicated NAT is
required by this baseline.

For optional private hardening, a private endpoint secures traffic *into*
Fabric, not traffic *out* of Fabric or the hosted applications. Enabling Fabric
inbound Private Link does not isolate those outbound clients.

### Network provisioning scope

`infra/command-center.bicep`, `infra/foundry.bicep`, `infra/state-sql.bicep` and
the monitoring templates have separate public service and identity boundaries.
The deployment helper's optional app/SCM filters are persistent; it has no
temporary-public mode or automatic return to private access. Preserve approved
filters during code-only updates.

Additional private hardening is a separate architecture and deployment task.
Do not treat an older private plan, retained resources or historical image
proof as authority to reintroduce those dependencies into the public baseline.

## Observability

The application instruments LLM calls with OTel GenAI metadata:
`gen_ai.system`, `gen_ai.request.model`, `gen_ai.operation.name`, `agent.name`,
token counts and finish reason. Tool calls emit `tool.*` metadata spans.
Instrumentation is not evidence that a cloud backend ingested them.

Without the OTel SDK installed, every helper is a no-op. Telemetry is not allowed
to be a hard dependency of the accelerator running.

Foundry reserves `APPLICATIONINSIGHTS_CONNECTION_STRING` and injects it from
project monitoring. Without that project connection, the runtime value was
empty despite a nonempty value in the version definition. Connecting the
project to Application Insights would enable tracing across all agents and may
collect prompts/responses/tool content, so project tracing stays disconnected.
See [hosted telemetry](https://learn.microsoft.com/azure/foundry/agents/how-to/configure-hosted-agent-telemetry)
and [tracing data handling](https://learn.microsoft.com/azure/foundry/observability/concepts/trace-data).

Hosted configuration instead reads `TRIAGE_TELEMETRY_CONNECTION_STRING` through
an app-owned `repr=False` setting and uses managed identity. The azd manifest
maps the custom variable from the operator's standard locator value; hosted
runtime has no fallback to the platform variable. CLI handling of the standard
setting is unchanged. The separate Entra-only public telemetry resource has
optional resource-scoped publisher roles and no Foundry project connection.

Startup forces `OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=false`
before SDK configuration and agent/host construction, and disables the host's
default callback with `configure_observability=None`. Only approved metadata
logger families are exported. Diagnostic output is bounded to counters,
exception types and sanitized source basename/function/line, not exception
messages, SDK responses, prompts or completions.

An empty platform locator caused SDK parsing failure even with an explicit
custom locator. Hosted configuration requires the app-owned locator, then
unsets only an exactly empty standard value before SDK configuration. It never
deletes/masks a nonempty value, redeclares the platform variable or uses a hosted
fallback; CLI handling is unchanged.

Native Application Insights queries now contain both `heartbeat_started` and
completed `heartbeat_finished` metadata, queue counts and zero captured exporter
failure/warning counters. This proves bounded ingestion from the active build.
Console metrics, source corrections and `configured` status alone still do not
establish an ingestion receipt, sustained delivery or end-to-end acceptance.

**Metadata only.** No prompt or completion content. In a multi-tenant system,
content recording ingests customer data and secrets into a telemetry store with
different access controls than the source system.

Monitoring state records worker/receiver heartbeats, inventory completeness,
poll/event freshness, checkpoint positions, queue backlog and connector drift.
An idle stream is not proof that its source is healthy. Coverage must retain
unknown permissions, interrupted pagination and retention gaps instead of
subtracting them from its denominator.

The controller heartbeat has an 840-second monotonic admission clock including
lock wait, with two automatic and one human-command concurrent slots.
Refills obey queue quotas and require enough remaining execution allowance.
It stops new claims without cancelling a lock holder or already admitted work.
The existing one-minute scheduler was reused, and three actual
recurrences completed after response decoding. Portal-only missing/failed-run
alerts are separate signals with empty action lists. The optional runtime
log-absence rule summarizes to one zero-count row for an empty window rather
than treating metric no-data as zero. An isolated query canary fired and
resolved, then was disabled; production log-absence remains enabled.
No real controller-stop or external notification test is claimed.

Service-wide and API-specific read budgets are acquired together. A denied API
budget must not consume the service allowance when no request was sent.
Throttling and missed cadence are operational states, not successful empty
history. Deployment alert rules and sustained canary evidence must be verified
separately from these stored health projections.

## Extending it

**A new remediation**: add the tool schema to `TRIAGE_TOOLS`, add a branch to
`ToolDispatcher._execute`, add the name to `REMEDIATION_ACTIONS`, and add a
scenario. Adding a capability is a code review, not a prompt edit — which is the
property that makes the allowlist worth anything.

**A new agent**: mirror `DataQualityAgent`'s typed boundary. Use its own provider
and prompt, controller-collected evidence and a tool-free interpretation call
where no additional tools are needed. Expose it to the orchestrator as one tool.
Re-register Foundry agents after prompt or tool-schema changes.

**A different durable store**: preserve the synchronous transaction and
fail-closed shared-state contract. Keep redaction inside persistence methods,
re-read after reconnect, and reconcile original receipts after uncertain writes.
Prove independent-instance arbitration and actual backend persistence in
addition to offline protocol tests. An in-memory fixture is a separate explicit
implementation, not runtime recovery for a failed database.

**Data-quality flag persistence**: `FlagStore` separates live
`AzureSqlFlagTable` from fixture-only CSV `DataQualityFlagTable`. Live flags use
the same application catalog, with `DATA_QUALITY_FLAG_TABLE_NAME` defaulting to
`triage_data_quality_flags` and physical-table key `data_quality_flags`.
Only the controller may append at runtime. Conditional insert and original-row
readback preserve deterministic request/evidence identity across retries and
reject conflicts. Both stores redact at persistence, and tools return the
stored flag. SQL has no local fallback and refuses runtime reset; the deployment
reset catalogue includes the flag table. The application schema, including
this table, is committed, and the corrected controller identity has reviewed
application grants. The discovery-intent heartbeat did not exercise flag
append/readback or full runtime persistence; those remain separate acceptance cases.
