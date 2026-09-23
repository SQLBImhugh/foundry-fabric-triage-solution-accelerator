# Contributor and agent contract

Read this before changing anything.

This file exists at two paths: `AGENTS.md` for tools and people that look there,
and `.github/copilot-instructions.md` because Copilot loads that one
automatically. **They are byte-for-byte identical and a test enforces it.** Edit
either and copy it over the other.

The repository previously carried a pointer at one path and the contract at the
other, which delivered nothing to a tool reading only `AGENTS.md`. Before that it
carried two real copies, and they diverged: three safety invariants, including
"a denial must not consume the remediation budget", went missing from one while
the other kept them.

## What this is

A multi-agent triage loop for Power BI refresh failures and admitted scheduled
Fabric pipeline failures on Azure AI Foundry, with a shared monitoring registry,
REST polling and native Fabric Job-event intake. It is published as a public
MIT-licensed **solution accelerator**. It is sample code, not a
supported product. Optimise for legibility and for being able to explain any line
of it out loud, over cleverness.

It runs **fully offline** with mock providers and mock tools. Keep it that way:
that is how it is evaluated and how the tests run.

All live application state belongs in one shared **Azure SQL Database**.
Fabric remains the monitored workload and native event platform, not the
application state store. This prototype takes a clean start: no Fabric SQL
compatibility layer, state migration, dual writes or mixed-version operation.

The shipped deployment defaults to public networking, protected by Entra
authentication, TLS, SQL firewall admission and application-role authorization.
Private endpoints, VNets and private DNS are not accelerator prerequisites.
Tenant-specific network-policy exceptions belong to the test deployment, not
the default customer architecture.

## Commands

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

.\.venv\Scripts\python.exe -m pytest -q                  # the offline suite -- no network
.\.venv\Scripts\python.exe -m pytest -q tests\test_policy.py::test_second_remediation_is_refused
.\.venv\Scripts\python.exe -m pytest -q "tests\test_scenarios.py::test_scenario_meets_its_expectations[scenario1-transient]"
.\.venv\Scripts\python.exe -m pytest -q tests\test_monitoring_store.py tests\test_monitoring_sql_store.py
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe scripts\scan_secrets.py       # the CI credential gate
.\.venv\Scripts\python.exe -m pip install -e ".[azure]"  # needed for any live command
.\.venv\Scripts\python.exe -m pip install -e ".[azure,monitoring]" # event worker
.\.venv\Scripts\bi-triage.exe run scenario1-transient
.\.venv\Scripts\bi-triage.exe preflight                  # incl. Azure SQL configuration
.\.venv\Scripts\bi-triage.exe pipelines --preflight      # registry settings only, no network
.\.venv\Scripts\bi-triage.exe identity --check-scope     # who the agents are

npm --prefix .\command-center test                    # offline frontend suite
npm --prefix .\command-center run lint
npm --prefix .\command-center run build               # types, bundle and assets

azd deploy bi-triage-controller --no-prompt              # hosted controller
azd ai agent invoke bi-triage-controller "heartbeat"
azd ai agent monitor bi-triage-controller
```

## Architecture

- `triage.cli:main` is the offline and operator entry point. `src/app.py` wraps
  the same `TriageRunner` as the Foundry hosted controller for interactive
  alerts, mailbox sweeps, silent-failure sweeps and scheduled pipeline triage.
  Its heartbeat drains monitoring work and queued human commands.
- `TriageRunner` owns orchestration outside the model: client and store
  construction, failure signatures, open-incident lookup, durable run state,
  agent construction and persistence of the terminal outcome.
- `TriageAgent` owns the reasoning loop. The provider proposes tool calls;
  `ToolDispatcher` charges the shared `PolicyLedger`, checks the allowlist,
  deterministic preconditions and approval state, then dispatches permitted
  calls. The agent's final outcome is validated against recorded actions and
  evidence before it is accepted.
- `DataQualityAgent` is a separate typed agent exposed to the orchestrator as
  one tool. Its controller scans the registered tables before a tool-free model
  call. Deterministic scans and silent-failure detectors establish facts; agents
  interpret and report them. The controller decides what action follows.
- Pipeline jobs use a separate workload allowlist and the shared monitoring
  registry. Full-pipeline reruns require reviewed replay safety, human
  approval and a durable reservation before POST. A submitted job is not a
  resolution; its correlated job and activity evidence must be verified.
  Notebook activity failures are pipeline evidence, not standalone notebook
  monitoring.
- `triage.monitoring` owns typed target, execution and incident identities,
  epoch/revision checks, scopes, inventory generations, coverage, work leases,
  action reservations and receipts. Live configuration comes from Azure SQL,
  not an environment target list. `MONITORING_MODE=fixture` is an explicit
  offline mode, never a fallback for unavailable live state.
  Its shared engine composes a required `MonitoringAdapter`; SQL and offline
  semantic operations are explicit implementations, not private-method
  overrides. New semantic operations require both adapters and contract tests.
- The monitoring worker discovers supported targets, polls service history,
  reconciles app-owned Eventstream sources and receives events with a pinned
  managed identity. An event is a source reference, not proof of a failed or
  remediable execution. REST evidence and controller admission remain required.
- Scope activation and remediation authority are separate. Admins configure
  monitoring and safety reviews; Approvers decide individual gated actions.
  Discovery grants neither service access nor permission to remediate.
- Monitoring SQL separates `worker`, `web` and `controller` callers. Workers
  submit observations and the web submits intents through checked views and
  fixed procedures; only the controller publishes admission, source and action
  authority. Neither producer receives unrestricted monitoring-table DML.
- Providers are selected per role through `triage.providers.get_provider`:
  `mock` is a scripted state machine for offline evaluation, `direct` uses Azure
  OpenAI, and `foundry` invokes registered Foundry agents. Live imports are
  deferred so the base install and test path stay Azure-free.
- Stores under `triage.store` hold incidents, processed messages, approvals,
  retries, claims, semantic-health baselines, the inbox-filter audit and pipeline
  rerun reservations.
  JSON/CSV implementations keep local runs reproducible; Azure SQL
  implementations provide hosted durability. All live stores share one
  application database so cross-store receipts and finalization can commit
  atomically. State that crosses invocations belongs in these stores, not on
  an agent. Runtime identities never install or upgrade schemas.
- YAML files in `scenarios/` are executable specifications. `TriageRunner`
  wires their mock inputs into the same controller path used by the application,
  and each `expect` block is checked by `tests/test_scenarios.py`.
- `command-center/` is a React/Vite frontend for the FastAPI API under
  `triage.command_center`. It queues work for the controller rather than
  dispatching remediation from a browser request. It is the operational UI.
  The separate Rayfin `cockpit/` remains a read-only sample, not a deployment
  or state dependency; its earlier semantic-model binding does not establish
  an Azure SQL read path.
- Command-center access comes only from validated Entra app-role claims.
  Reader, Operator, Approver and Admin map to ordinary Entra security groups.
  The read-only Access & permissions page shows effective token roles, not
  directory memberships. Runtime authorization has no SQL ACL or Graph
  directory dependency; optional profile photos use separate delegated User.Read.
- Full incident records add append-only notes, human tracking resolutions and
  persisted tool-free observer discussion in `triage_incident_activity`.
  They do not replace or reset the controller's operational state.

## Rules that are not negotiable

1. **Tests never touch the network.** No live calls, no credentials, no Azure. A
   test that needs a tenant is not a test.
2. **Policy is enforced in the controller, not the prompt.** New limits go in
   `PolicyLedger` with a test proving they fire. A limit that exists only as
   prompt wording is not a limit.
3. **Every tool is on an allowlist** (`REMEDIATION_ACTIONS`, `REPORTING_ACTIONS`
   or `DIAGNOSTIC_ACTIONS`). Anything else is refused before dispatch. That is
   the property the whole design rests on.
4. **An approval is only a yes if it is explicit, fingerprint-matched, unexpired
   and unused.** Everything else — timeout, error, malformed reply, no gate
   configured — is a no. Silence never reads as consent.
5. **A denial must not consume the remediation budget.** Otherwise one "no"
   silently disarms the agent for the rest of the incident.
6. **Deterministic evidence outranks model output.** If a model claim and a scan
   disagree, the scan wins and the disagreement is logged.
7. **Redaction stays inside the store boundary.** Never move it to call sites; a
   call site can forget.
8. **Every terminal outcome is persisted**, including crashes and refusals.
9. **Spans carry metadata only.** Never attach prompt or completion content.
10. **The inbox filter is a security control.** An agent that acts on every
    message is steerable by anyone who can email it. Never widen the filter to
    make something trigger — send a matching message instead.
11. **Agent identity posture is read from the directory, never configured.**
    A hardcoded claim about security posture will eventually be false silently.
    This operator identity check is separate from command-center authorization,
    which trusts validated Entra app-role claims, not a runtime directory lookup.
12. **Grant permissions to the component that acts, not the one that reasons.**
    The prompt agents hold nothing. If a reasoning agent seems to need a
    permission, the design is wrong.
13. **Tools fail loudly rather than return something interpretable.** Calling
    Power BI with an empty id returned 404, and the model turned that into a
    confident, wrong conclusion.
14. **Scenarios are reproducible.** Same input, same tool sequence, same numbers.
    When a live model and the mock diverge, make the *controller* decide so both
    agree.
15. **Never weaken a policy limit to make a scenario pass** — change the
    scenario.
16. **State that must survive an invocation goes in a store, never on an
    object.** A hosted agent is rebuilt for every request, so an instance
    attribute is always empty on arrival.
17. **An incident is announced once, not once per occurrence.** Enforced in the
    controller against `notified_count`, never in prompt wording.
18. **Pin the hosting library exactly.** `agent-framework-foundry-hosting` ships
    date-stamped betas that make breaking changes without a major bump; a
    floating floor once crash-looped the deployed container at startup, and
    nothing reported it.
19. **Live stores fail closed.** An in-memory fallback is not durable success.
    A store that cannot reach its backend reports failure and re-checks on use.
    Recovery reads authoritative state rather than replaying a stale cache, because
    an empty cache answers "no open incident" to everything and licenses a
    second remediation. Incidents, processed messages, approvals, retries,
    baselines, claims, action reservations and command-center state all require
    shared durability. In-memory stores are explicit offline implementations.
20. **All durable application state lives in one Azure SQL Database.** Configure
    Entra-only authentication on its logical server; Azure SQL does not impose
    that setting by default. Use `AZURE_SQL_SERVER` with
    `<server>.database.windows.net` and `AZURE_SQL_DATABASE`, never a
    credential-bearing connection string, SQL login or shared key. Enable
    auditing at provision time and explicitly admit the required public
    callers through the SQL firewall. Claims and
    leases use an atomic conditional database operation, never read-then-write.
    Direct statements use affected `rowcount`; monitoring procedures return a
    typed result that must be decoded. An `EXEC` rowcount is not the
    procedure's outcome.
21. **Command-center roles are managed in Entra, never in SQL or the browser.**
    Operator and Approver are separate; Admin permits all app operations but
    grants no directory administration, SQL server Entra-administrator role
    or controller service permissions.
    `COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true` is retired and must fail.
22. **Resolved by user is tracking, not verified remediation.** The append-only
    resolution binds to the original SQL NVARCHAR payload's SHA-256 hash using
    UTF-16 LE. New evidence invalidates that closure without resetting incident
    outcomes, approvals, claims, notification counts or remediation budgets.
23. **Observers cannot act.** Questions and notes are untrusted annotations,
    not diagnostics or tool instructions. Keep the observer tool-free and
    render its answers without raw HTML, images or unsafe links.
24. **Token renewal is not immediate revocation.** Keep actions locked until a
    snapshot from the new permission-refresh generation succeeds. Do not restore
    capabilities from the preceding snapshot after requesting a fresh API token.
25. **Names do not identify monitored targets.** Use tenant, epoch, workload,
    workspace and item identity; keep source execution IDs distinct from the
    controller's submitted job IDs. Every live intake path uses the same
    admission and action-fencing boundary.
26. **Admission and reservation are atomic.** Validate the current epoch,
    maintenance state, scope, safety review, source head, approval and work
    fence in the reservation transaction. Revocation cannot race an independent
    unchecked insert. An already committed external action cannot be retracted
    by a later scope edit.
27. **An uncertain write is not a retry opportunity.** Keep its original
    request ID and action fence. Reconcile durable receipts and exact external
    evidence before another effect; neither a timeout nor a successful HTTP
    acknowledgement establishes the final outcome.
28. **Checkpoints follow durable acceptance.** Persist receipts, source
    dispositions and related work before advancing stream or REST positions.
    Incomplete discovery, retention gaps and unreadable metadata remain unknown
    or incomplete, never a healthy empty result.
29. **SQL transactions remain synchronous and thread-bound.** Use
    `AzureSqlDatabase.transaction()` for atomic work; no nesting, awaits or
    reconnects inside it. An uncertain commit requires receipt reconciliation.
    Incident/source disposition/work completion must not split into independent
    success claims.
30. **Accepted intent is not published authority.** Keep original operation
    receipts immutable. Pending revocation and unvalidated source observations
    fence new actions; they do not cancel an already reserved effect or release
    its budget. Desired source removal stops intake but retains physical
    ownership until an original complete observation proves exact remote absence.

## Style

- `from __future__ import annotations` at the top of every module
- `str | None`, not `Optional[str]`
- Pydantic models for anything crossing an agent boundary
- Per-module loggers: `logging.getLogger("triage.<module>")`
- Comment the **why**, not the what — especially where a design choice traces to
  a real production failure. Those comments are the most valuable content in the
  repository
- Printed output stays ASCII where practical; Windows consoles mangle the rest

## Adding things

- **A remediation tool**: schema in `TRIAGE_TOOLS` → branch in
  `ToolDispatcher._execute` → name in an action allowlist → a scenario → a test.
- **A durable store**: follow the fail-closed SQL adapters and shared monitoring
  transaction boundary. Keep redaction inside persistence methods. Reconnect and
  read authoritative state after failure; never replay an uncertain mutation
  from a local cache. Add independent-instance and lost-acknowledgement cases,
  plus a separate live Azure SQL proof. The offline store must remain explicit.
  Do not split stores into per-component databases. A temporary isolated proof
  database may use the same logical server; it is not another operational store.
  An elastic pool requires a measured cost/load justification, not a default.
- **Offline fixture state**: seed through a narrow `fixture_setup(store)` context,
  then use the requested `fixture_component` view for execution. Close setup
  before awaiting or running the controller. SQL protocol doubles belong in
  tests, never in a production adapter or live fallback.
- **An agent**: mirror `DataQualityAgent`'s boundary. Use a separate provider and
  prompt, deterministic evidence collection and a typed Pydantic result,
  exposed to the orchestrator as one tool. Its model call needs no tools or
  service permissions. It reports; the orchestrator decides.
- **A scenario**: a YAML in `scenarios/` with an `expect` block, added to the
  parametrized list in `tests/test_scenarios.py`. The `expect` block is the test.
- **A playbook**: a `Playbook` in `knowledge/playbooks.py` with triggers, a
  `retry_useful` verdict and a **public** Microsoft Learn source, then a test
  that it fires on realistic error text. Retrieval is capped at 3 — if a new
  entry matters more than an existing one, raise its trigger specificity rather
  than the cap.
- **A prompt or tool-schema change**: prompts are hashed onto triage incidents.
  If running in Foundry mode, **re-register the agents** or the change has no
  effect.
- **Anything touching durable state**: prove it live as well as offline. The
  offline suite uses fakes, and a fake cannot tell you that a connection dies
  under concurrency or that a managed identity has no database user.

> **Sourcing rule.** Playbook content must come from public documentation. This
> repository is shared publicly, so internal engineering guides may inform *what
> matters* but must not be quoted or cited. A test enforces this.

## Never

- Commit a filled-in `.env`, a webhook URL, or any credential
- Add a network call to the default test path
- Put a customer's name, or an internal project name, in a committed file
- Claim a capability without verifying it against the platform first
