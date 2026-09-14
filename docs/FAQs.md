# FAQs

## Can I run it without an Azure subscription?

Yes. With no configuration the accelerator runs against a deterministic
provider and in-memory tools:

```powershell
.\.venv\Scripts\bi-triage.exe list
.\.venv\Scripts\bi-triage.exe run scenario1-transient
```

That is the same path the test suite uses. No credentials, no network.

## What does it actually change in my tenant?

Only what is on an allowlist, and only within budget. By default that is one
remediation per incident. The Power BI path can trigger a refresh, re-enable a
disabled refresh schedule or defer a retry. The separate pipeline path can
submit a full-pipeline rerun only after reviewed replay-safety configuration,
deterministic prerequisites and explicit approval. Everything else is refused
before dispatch and escalated to a human.

Set `TRIAGE_MAX_WRITE_ACTIONS=0` to disable remediation while evaluating it.
Incident records, reporting and audit writes still occur.

## Which model should I use?

Any model that supports function calling. `FOUNDRY_AGENT_MODEL` selects it.

The controller, not the model, decides what is permitted. Model choice affects
classification and explanations, not authority. When evaluating a swap, run the
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
dependencies. With a healthy scheduler and no backlog, arrival-to-next-poll
delay is up to one interval; evenly distributed arrivals average half an
interval. Processing time and backlog add further delay.

`GRAPH_INGESTION_MODE=subscription` is rejected at startup rather than silently
polling, because believing you have push while getting a poll is a latency
assumption nothing will correct.

## Why is the scheduled trigger a Logic App rather than a Foundry routine?

Foundry routines did not fire in the evaluation verified on 2026-09-02, six days
after registration: the routine reported itself enabled, accepted dispatches,
produced no runs, and telemetry showed agent activity only in hours when a
person invoked it by hand. `azd deploy` does not manage routines either.

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

Scheduled Fabric Data Factory pipeline triage is already implemented for
explicitly configured targets. It reads failed scheduled jobs and activity
evidence; it does not infer missing starts or disabled schedules. Notebook
activities can be evidence within a pipeline, but standalone notebook-job
monitoring is not implemented. See [PipelineTriage.md](PipelineTriage.md).

The policy ledger, allowlists, approval gate, signature and suppression logic,
incident model and outcome validation can also be reused for another domain.
Domain-specific tools, playbooks and parsing still need implementation.

See [`CustomizationGuide.md`](CustomizationGuide.md), which has a section on
moving to a different domain.

## What does the command center add?

The [command center](CommandCenter.md) provides an authenticated queue and
inspector, full incident pages, run history and tool timelines, durable notes
and read-only discussion, approvals and a durable investigation queue. It reads
the same Fabric SQL state as the controller; it does not own the database.
The existing read-only Fabric App can remain deployed.

Teams is optional. **New investigation** records a request for a configured
target; a separate `command sweep` worker executes it. A queued receipt, an
approval decision or a completed observer answer is not proof of remediation.
An empty target list never selects a resource implicitly.

## Who can use the application?

The API validates Entra app-role claims on every authenticated request. Reader
can view records and ask read-only questions. Operator adds investigations,
notes and human tracking resolution. Approver adds approval/denial decisions,
not Operator permissions. Admin has all app capabilities, including scenario
validation and uncertainty reconciliation, but no Entra directory
administration. These roles grant no direct Azure, Fabric or SQL access.

Use the four ordinary Entra security groups assigned to those roles; group
owners manage membership and authorized IT administrators manage assignments.
Group-based assignment requires Entra P1/P2 and does not cascade through nested
groups. See [Entra-managed groups](CommandCenter.md#entra-managed-groups).

## How do I change or refresh permissions?

Request the appropriate group membership from its owner or IT in Entra.
**Access & permissions** is a Reader-visible, read-only view of the current
token's roles and issued/expiry timestamps. It does not read current group
membership or offer Add/Edit user, invite or local-grant operations. The SQL
permission editor and importer are retired; old `?view=admin` links open this
read-only page.

After a change, select **Refresh permissions** to request a fresh API token and
reload access information and the snapshot. Actions remain locked until fresh
records confirm permission. Entra changes may take time to propagate; refreshing
one session does not revoke other sessions' already-issued tokens. Profile
photos use a separate delegated Graph `User.Read` token, not a directory access
or authorization-management permission.

## What does Resolved by user mean?

An Operator or Admin explicitly closed human incident tracking with a reason.
It is an append-only decision bound to the reviewed source revision and tracking
version, not an agent-verified repair. It does not reset remediation budgets,
approve a proposal or clear an execution uncertainty block.

A changed source payload invalidates the older closure without deleting its
history. A conflicting resolution request returns `409` and requires a fresh
review. Notes and the saved question/answer thread remain available; the
read-only observer cannot act on requests to repair or approve anything.

## What does Scenario validation prove?

It checks the 15 canonical cases against deployed code, using synthetic tools
and isolated state. Mock validation is deterministic; Foundry validation also
uses registered agents and model calls. Both preserve production incident state.
A pass means the case's expectations matched, including expected refusals,
escalation or pending verification; it does not mean a production repair
succeeded.

The page is Admin-only. At most two cases run concurrently. **Stop queue** stops
new requests, not already accepted work. Review the durable result and run ID
after an uncertain response; cases are not automatically retried. See
[validation and evidence](CommandCenter.md#validation-and-evidence).

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
