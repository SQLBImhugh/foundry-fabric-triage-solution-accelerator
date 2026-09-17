# Customization

Adapt the failure types, tools and policy while preserving the separation of
responsibilities: reasoning agents interpret evidence without service
permissions, the controller enforces action limits, and deterministic detectors
produce measurements. Live components authenticate with their own Entra
identities; a command-center user's app role is not a controller service grant.
All live application state belongs in one shared Azure SQL Database, while
Power BI and Fabric remain the monitored workload/event platforms.

The current SQL ownership contract is independently reviewed offline; final
component-store, controller and connector acceptance remains separate.
Isolated MI event transport has been demonstrated across manual and
scheduled failure, success and cancellation; SQL durable handling and normal
worker readiness have not. The shipped infrastructure now uses public networking
with Entra authentication and no PE/VNet/NAT/private-DNS prerequisites.
Scoped evaluation SQL/registry public access is verified, but the original SQL
proof receipt is under recovery and bootstrap/runtime acceptance remains gated.
Earlier private-network proofs and the private Foundry preflight error are
historical, not blockers for the retained public Foundry path.
The live app/controller remain the prior release; normal-worker rollout,
history migration/wipe and hybrid cutover are not complete. Keep those gates distinct; see
[DeploymentGuide.md](DeploymentGuide.md#release-gates).

## Decide the tier before writing any code

Classify the proposed action before exposing it. The first column is a value
of `TriageClassification.tier`, available through `TriageResult.classification`
when a classification was recorded. There is no `TriageResult.tier` field or
`tier_3` value. Classification does not itself authorize a tool:

| `tier` value | Meaning | Where it lands |
|---|---|---|
| `tier_1` | Transient/idempotent candidate, subject to current admission, review and controller policy | `REMEDIATION_ACTIONS` |
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

Required changes:

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
6. **Shared action contract** for a live mutation: typed action/review/argument
   fingerprints, source-head and active-job prerequisites, atomic reservation,
   submission state and exact verification. Do not add a second pre-POST path
   outside the common monitoring store/controller.
7. **Recovery and finalization**: preserve an uncertain effect's fence and prove
   terminal incident, processed-source disposition and work completion are
   durable. A local result or an unrelated new job is not completion.

Give the tool its own mock. The offline path is the evaluation path; a tool that
only works live cannot be demonstrated or tested.

Targets default to observation-only; classification or adding a tool schema
does not enable an action in the registry.

Pipeline requests use `PIPELINE_ACTIONS` as well as the action taxonomy. Add an
action to that workload's set only after reviewing its effect on pipelines;
do not make dataset tools available to pipeline requests. The existing monitor
handles registry-admitted scheduled pipeline jobs. Notebook failures
inside their activity evidence do not imply standalone notebook monitoring.

## Add a preconditioned action

Some actions must not run when something else is true — the deferred-retry
window is the worked example: while a retry is postponed, an immediate refresh
is refused.

Add the name to `_PRECONDITIONED_ACTIONS` and implement `_precondition_failure()`.
The budget is charged **after** the precondition passes, not before. Charging
first means a refusal still spends the write budget, which silently disarms the
agent for the rest of the incident — the same bug the approval path already had.

The in-run ledger is not the cross-invocation action boundary. A live action
must also pass the store's atomic current-epoch, admission, review, source-head,
approval and owner/fence checks. Do not split those into a successful read and
an independent reservation write.

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
and a durable reservation before submission. Its exact submitted job and activity
evidence must confirm success; HTTP acceptance is not a resolution. Power BI
refresh also needs exact own-submission correlation. Gateway/schedule actions
need the intended configuration read back through their typed verification path.
Keep the full tool arguments and approval fingerprint. Do not strip arguments
to fit an older RPC shape or replace an original hash with a smaller technical
subset. Empty `{}` and absent/null reviewed parameters are not interchangeable.

Never globally refund an incident budget or reuse a consumed approval because
a response proves no effect. Preserve the reserved incident slot. Any allowed
retry must use the store's bound, single-use path after durable parent
finalization and fresh scope/source/review checks.

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

## Extend monitoring discovery or intake

Use the typed models and synchronous `MonitoringStore` contract under
`triage.monitoring`. `MONITORING_MODE=live` selects durable SQL and the pinned
`MONITORING_TENANT_ID`; `MONITORING_MODE=fixture` selects explicit offline state.
No missing setting, unavailable backend or partial response may select a
fixture or restore `FABRIC_PIPELINE_TARGETS`. Static live targets and
compatibility loaders are retired.

Discovery work uses `discovery_selector`; API-triggered discovery delegates
revision/idempotency handling to `request_discovery(expected, selector,
request_id=...)`. Do not add an alias or perform an API preflight that rejects
the original request after an idempotent mutation has already committed.
Scopes, metadata generations and display names are not execution authority.
An accepted web intent remains pending/configuring until deterministic controller
publication. Workers own observations, not source heads or resolved admission.
Use `controller.publish_source` for current-fenced source publication and
`controller.disposition_source` for a no-effect disposition with its processed
marker; no raw source/head/disposition write alternative is part of the contract.

Keep these contracts when adding an inventory provider or workload:

- Enumerate valid pages within durable budgets. Persist continuation and gaps;
  failed or incomplete enumeration must not delete known inventory.
- Store named workspaces/domains separately from workload items. Reconcile
  ancestors/descendants and moves, with exclusions winning. Unknown domain
  membership must not bypass a policy exclusion.
- Label caller-visible versus authorized tenant-admin inventory. Domain
  metadata grants nothing; Admin Items preview must be explicitly selected.
  Power BI scanning is inventory, not operational refresh telemetry.
- Perform source/capability probes with the deployed collection/execution
  identity. Reading history cannot establish event, write or exact-correlation
  capability. No access grants are made by a collector.
- Use canonical tenant/epoch/workload/workspace/item identity and authoritative
  execution IDs. Resolve Power BI ID aliases from source evidence, never from
  timestamps or display-name similarity.
- Accept/disposition every source observation before advancing a REST page or
  contiguous stream checkpoint. Preserve original transport source/ID and
  connector provenance separately from execution deduplication.

Target identity, source-execution identity and incident identity are distinct.
`target_signature` supplies the immutable target key to the existing
`compute_signature` normalization/digest logic; it is not a new canonical-ID
wire format. Do not confuse that failure signature with the SHA-256 source
revision used for human tracking. Power BI numeric history IDs
(`powerbi_refresh`) and request IDs (`powerbi_request`) remain separate
namespaces; only authoritative REST evidence can establish their alias.

Collectors remain evidence-only. Unsupported item types stay visible with
reasons; standalone notebooks do not become pipelines. A failed-job stream
cannot detect a job that never existed. Missing expected starts need explicit
schedule/timezone/grace contracts, and broad data-quality monitoring needs
business expectations and source data access.

The current Eventstream path uses public outbound Custom Endpoint transport
with Entra. Do not call the key-returning connection API, introduce a SAS/blob
checkpoint fallback or claim automatic endpoint discovery. Initial nonsecret
metadata comes from the Entra tab and must be bound to the owned topology.
Reconciliation may alter only app-owned monitoring definitions. It cannot
adopt an unrecorded user-owned Eventstream or authorize a workload remediation.

### Connector proposals, observations and retirement

Additions use a typed logical `ConnectorSourceProposal`: `proposal_id`,
`node_name`, target and event types, with `source_id=null`. An unresolved
proposal, including one already stored, cannot supply an invented physical ID.
The worker records the original complete owned definition; controller
`publish_connector` passes its `observation_receipt_id` to let SQL validate and
bind only actual returned component IDs. Do not query worker-private receipt
views from controller caller code.

`SourceRemovalIntent` carries `removal_id`, `source_id`, `proposal_id` and
`detail`. Both selector keys are required and exactly one must be nonnull.
Desired removal fences intake but retains ownership and existing IDs while
remote absence is pending. Only a complete original current receipt proving
absence of the selected node, physical ID and stream routing may retire the
source and append its immutable tombstone. An uncertain or inherited snapshot,
or a null SQL-derived `observed_definition_hash`, cannot establish absence.

Preserve `pending_removals`, `retired_sources` and `observation_receipt_id` in
the typed publication result. Do not infer retirement from an empty latest list,
reuse earlier readiness for a changed source set, or turn worker observations
into published authority.

## Extend shared state

State crossing invocations belongs in a durable store, not a worker/controller
instance field. All live stores share the Azure SQL application catalog so
cross-store acceptance, finalization and receipts can be atomic. The live store
must fail closed when SQL is unavailable and recover from current shared records.
A recovered empty cache can incorrectly
license another remediation.

Live construction must select the component explicitly:
`build_monitoring_store(settings, db=db, component="controller")`, or the
corresponding `worker`/`web` component. `AzureSqlMonitoringStore(db=db,
component=...)` has no permissive live default. Fixture component views are
explicit offline objects, not evidence that a deployed identity has SQL access.
The component argument selects routing; actual SQL roles enforce authorization.

Seed offline state inside a narrow `fixture_setup(store)` context, passing its
yielded fixture to the seeding helpers. Close that context before awaiting or
running the controller. Use `fixture_component` to share fixture state through
separate restricted worker, web and controller views. SQL protocol doubles live
in tests, not production adapters or live fallback paths.

Use `triage.monitoring.sql_permissions` for the checked-view, static-RPC and
deployer-grant contract. Bind every named argument, including required nulls,
with `RpcContract.bind`, execute through `db.query`, and use
`decode_rpc_result`. Read the returned `status`, `affected_rows` and typed
`result`; never treat EXEC rowcount as success. Preserve original receipts
after uncertain commits rather than reconstructing a success-shaped result.
`runtime_table_permissions()` is retired and raises; do not replace missing
component routes with broad base-table DML.

Use `AzureSqlDatabase.transaction` for multi-record atomic work. It is
synchronous and thread-bound: no awaits, no nested transaction, and no claim
that several autocommit calls form one transaction. Async collectors offload
blocking persistence. Use database time, conditional ownership and fences,
shared service/API budgets and fair workspace shares across replicas.

Use `AZURE_SQL_SERVER` and `AZURE_SQL_DATABASE`, with the hostname and catalog
from the Azure deployment. The public SQL endpoint requires Entra-only
authentication, TLS, auditing/TDE and explicit firewall admission. The default
`allowAzureServices=true` is SQL's special start/end `0.0.0.0` rule for
Azure-hosted callers, including other subscriptions, not an Internet-wide rule
or an identity grant. Optional client rules specify exact IPv4 ranges.
Do not introduce a SQL login, credential string, Fabric SQL adapter or legacy
configuration alias. A temporary proof database may share the logical server;
the shipped template has one application database and no elastic pool.

Keep normal public networking as the shipped baseline. Optional resource
exception maps must not become baked-in customer defaults. For MCAPS testing,
the SQL server's approved `SecurityControl=Ignore` plus reason/review tags has
one 14-day period; re-adding the tag does not renew it. Longer tests need an
approved exclusion. Do not introduce a VNet, NAT or private-endpoint dependency
as a workaround for an identity/permission failure.

Creation/bootstrap/reset belong to explicit deployment tooling. Runtime stores
must not acquire DDL, upgrade schema, import old state or delete an uncertain
action journal. Preserve original operation IDs after a timeout and reconcile
their receipts. The deployment-only prototype reset is not a recovery fallback.

Add deterministic fake transport/clock/transaction cases for denial, malformed
or partial evidence, retention exhaustion, duplicate source aliases, stale
scope/review, lease loss, ambiguous commit, restart and cross-workspace fairness.
Keep them offline even when the operator's environment contains live settings.
Deployment proof is a separate authorized procedure.
Keep unfinished adapters or `kernel_incomplete` failures explicit until all
required live routes are functional and independently proved. Native permission
proof requires real Entra identities on an approved isolated Azure SQL target.
Azure SQL supports `WITHOUT LOGIN` and `EXECUTE AS USER` for database-scoped
tests; these and offline fixtures do not prove MI sign-in, firewall admission,
reconnect or end-to-end persistence.

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
`command-center/src/styles.css`; the current design prefers system Georgia,
with DejaVu Serif and self-hosted DejaVu Serif Condensed fallbacks, Onyx
dark/light colors and Ink-style geometry and shadows.
Keep the supplied PNG logo and packaged font assets unless a deliberate branding
change updates the asset checks too. The Command Center is the operational UI.
The separate Rayfin cockpit remains a read-only sample, not an alternative
command writer or a deployment/state dependency. Its earlier model binding
is not a verified Azure SQL integration.

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

The mailbox is one entry point. Interactive, queued, polled, event and deferred
retry observations must converge on current registry admission and exact source
identity. In live mode, `pipeline_sweep` queues observations for admitted targets;
the collector reads them and the controller heartbeat drains durable work.
Do not turn a new trigger into a direct tool dispatcher.

The Eventstream worker and disabled-by-default one-minute heartbeat are the
hybrid mechanisms. A new webhook or queue adapter still needs equivalent
authentication, provenance, scope/cutoff validation, idempotent intake and
receipt-before-checkpoint ordering. Its successful transport response is not
controller completion.

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
