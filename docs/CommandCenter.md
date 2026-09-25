# Agent command center

`command-center/` is a React application served by a FastAPI API on Azure App
Service. It reads and writes the shared Azure SQL application state and adds
authenticated monitoring configuration, target safety reviews, approvals, a durable
investigation queue, full run history, incident notes and tool-free questions.
Teams is optional. The Command Center is the operational UI. The retained
read-only Fabric App in `cockpit/` is not a deployment or state dependency; its
earlier semantic-model binding does not establish an Azure SQL read path.
All application stores use one database, independent of either UI.

The interface prefers the system Georgia font, with DejaVu Serif and
self-hosted DejaVu Serif Condensed fallbacks, and uses Onyx colors in its
dark/light themes, with Ink geometry: 2px corners and 7px offset shadows.
Font licensing is included with the frontend assets.
The supplied triage artwork appears in the sidebar, sign-in view and favicon.

Workspace choices come from the collector's accepted catalogue, not the
signed-in user's directory access. The scope editor retains the last accepted
snapshot during same-generation background refreshes. A permission/context
change clears it immediately; errors or stale permissions still lock setup.

## Hybrid implementation and acceptance status

The source tree includes registry-backed Monitoring setup, REST collection,
Eventstream provisioning/intake and the shared controller path. This is not a
claim that the new hybrid application has completed deployment acceptance.

The earlier six-finding SQL permission-kernel review closed **offline** on
2026-09-16. That bounded review is not final Azure SQL integration acceptance;
the [release gates](DeploymentGuide.md#release-gates) distinguish source checks
from native schema and runtime acceptance. Offline closure does not
prove native managed-identity SQL permissions, transaction behavior or recovery.

Historical W0 work verified managed-identity Eventstream creation/inspection and an
operator-identity Power BI refresh. The isolated transport probe subsequently
received nine receipts covering four exact wire event types. This is bounded transport
evidence, not SQL durability, checkpoint/restart proof or proof of the normal
monitoring worker.

The current baseline is public HTTPS and public Azure service endpoints with
Entra authorization, not PE/VNet/NAT/private-DNS prerequisites. Scoped evaluation
SQL/registry public access has been enabled and read back; SQL remains Entra-only
with TLS/auditing/TDE and registry admin/anonymous access remains disabled.
The template uses one S1 application database and an optional Basic proof database
on the same logical server, without an elastic pool or a final sizing claim.

SQL recovery is complete: append-only operator adjudication preserves the
original failed `STARTED` receipt and binds its approved replacement.
Proof and application schemas each committed 145 DDL batches and passed 114
readbacks. The independent initialization capture of `maintenance=true`,
revision `0` and its bootstrap receipt is historical. A separate no-VNet public-route job
passed 114 application readbacks using encrypted TCP/FEDERATED bootstrap-MI
authentication without changing maintenance at that capture time.

Command Center has cut over to live monitoring and Azure SQL. The hosted
controller uses the correct acting `ServiceIdentity`; three runtime EXTERNAL
SQL users have kernel roles and reviewed application DML grants. Maintenance is
now `false`. After reviewed SQL corrections, the deployed heartbeat completed
the original discovery-intent work, published its frontier `1/1` and queued one
inventory-worker item. It took no actions; the original receipt/control were
unchanged. No additional controller deployment was needed for that SQL-only fix.

A no-ingress collector-only worker is now deployed with its dedicated MI and
explicit `tenant_admin_preview` selection. Native Fabric domain/workspace reads
durably accepted 332 workspaces. Authenticated browser checks found both the
Include and Inventory workspace selectors enabled with 333 options including
the placeholder, without selector error alerts.

The broader item scan remains partial: Power BI dataset reads returned 401
without source permissions, and request-budget/throttling gaps remain visible.
Snapshot counts remain deployment/estate-wide. **Inventory total** stays
**Unknown** until all latest discovery generations are complete, including when
no scopes are configured. No static/fake workspaces, automatic scope admission,
remediation or extra Fabric grants were added.

A separately bounded priority proof processed an original fresh discovery
request on attempt 1 without manual requeue behind existing partial-inventory
work. Its selected workspace completed ten pages with three items, including
one unsupported Eventstream. Selector-aware preview readiness can use that
complete workspace without declaring the estate-wide snapshot complete.
First-scope activation remains separate acceptance work; partial-window replay
churn is not claimed resolved.

Three actual recurrences of the reused one-minute heartbeat scheduler completed
after response-envelope validation. Portal-only missing/failed-heartbeat alerts
have empty action lists. Collector-only mode runs no Eventstream receiver or
provisioner; neither inventory selectors nor bounded SQL cases prove full hybrid
operation. Native Application Insights queries now contain app-owned
`heartbeat_started` and completed `heartbeat_finished` metadata, queue counts
and zero exporter failure/warning counters in the captured runs. Foundry project
content tracing remains disconnected. This proves bounded ingestion, not full
collection or overnight stability; see [Observability](DeploymentGuide.md#8-observability).
Earlier private image and network checks remain historical, and the private
Foundry preflight failure does not block the retained public Foundry path.
The completed web/controller and collector-only deployment does not imply full
source/event coverage or turn the earlier screenshots into current end-to-end evidence.

The [approved hybrid plan](./HybridMonitoringPlan.md) defines the remaining
event-enabled collection, durable intake/recovery, coverage and end-to-end acceptance
gates. All screenshots below remain earlier-release evidence, including its
synthetic validation. They must not be relabelled as the current release.

## Operator workflows

| Page or control | Behavior |
|---|---|
| Command center | Pending approvals and work needing investigation, with a persistent detail inspector |
| Incidents | Searchable incident register and a full-page record with evidence, notes, discussion and user resolution |
| Run history | Individual controller runs, recorded tool steps, outcomes and policy counters |
| Approve / deny | Authenticated, fingerprint-bound, unexpired, single-use decisions |
| New investigation | Enqueues a command for a currently admitted Power BI or pipeline target from the monitoring registry |
| Ask | Explains recorded evidence through a separate observer; cannot call remediation tools |
| Knowledge | Public-source troubleshooting playbooks |
| Scenario validation | Administrator-only execution of the canonical scenarios with synthetic tools and isolated state |
| System overview | Signed-in user, monitored resources, agent team and service health |
| User menu | Profile photo, or initials when unavailable, and Sign out |
| Access & permissions | Reader-visible, read-only effective application roles, token freshness and Entra management guidance |
| Monitoring setup | Reader inspection of inventory, coverage, targets and existing reviews; Admin scope configuration, preview/activation and per-target safety review |

The queue and recent-history projections are bounded. They are operational
windows, not a replacement for an unrestricted SQL report. Earlier incident
aggregates cannot reconstruct tool steps that were never persisted.

### Queue filters

**Needs investigation** includes incidents with the API status `needs_review`.
The queue and inspector use the same display label as the filter without
changing the stored status. Attention-summary tiles select a status group in
the command-center queue and clear search and workload filters, because their
counts are not scoped to those filters. Selecting an already active,
unconstrained tile returns to **All work**.

The workload filter always offers **Power BI** and **Fabric pipeline**, even
when the snapshot has no records for one of them. A workload with no matching
records shows an empty state; its presence does not configure or start
monitoring. Notebook activity failures appear under their parent pipeline.
Standalone notebook-job monitoring is not implemented.

### Incident records

The command center retains its queue and inspector. **See full incident details**
opens the linked record in **Incidents**, which has its own paged register and
full-page workspace rather than a second copy of the queue.

Users with Operator or Admin permission can append notes and record a resolution
with a reason. **Resolved by user** is distinct from an agent-verified repair.
The tracking record does not replace the controller's evidence, reset its
remediation budget, release a claim or approve a pending action. A closure is
bound to the evidence reviewed; a newer occurrence cannot remain hidden behind
that older closure.

Choose **Review human resolution**, review the reason and evidence, then confirm
that only human tracking is being closed. The API conditionally appends the
decision against both the tracking version and the original stored incident
payload's source revision. A `409` means the evidence or tracking changed:
refresh and review again. Notes and questions do not advance the tracking
version. No mutation is automatically retried after an uncertain response.

Notes, resolution history and read-only agent discussion persist in Azure SQL.
An unanswered or failed question remains visible rather than appearing as a
successful reply. The observer has no remediation tools. Questions and answers
remain separate from action proposals and their explicit approval controls.
Closed incidents remain searchable, and their evidence and discussion remain
readable.

Execution history labels each run by its agent, with the run ID and outcome
metadata kept separate from the explanation. The newest explanation opens by
default; older explanations can be expanded without losing that choice on
refresh. The shared Markdown renderer uses 15px prose for execution explanations,
run details and observer answers, including headings, lists, emphasis and code.
Raw HTML, embedded images and non-HTTPS links are not rendered. A completed run
does not by itself establish that its incident was repaired.

![Earlier-release synthetic incident record with notes, discussion and user resolution](./images/command-center/local-incident-record.png)

## Safety boundaries

The browser has no SQL credential and cannot bypass target admission or dispatch
an arbitrary remediation tool.
The API derives the User from a validated Entra access token, checks application
roles and writes only controlled state transitions. The hosted controller
drains commands and retains its existing allowlists, policy ledger and evidence
validation.

Web approval proposals cannot be answered through the legacy bearer-link
callback. Opening a Teams deep link only selects a proposal; it never approves
one. Optional Teams delivery runs independently of the web approval window, so
a slow or failed Teams post does not disable web decisions.

An investigation is not the same as an authorization to remediate. Pipeline
reruns still require reviewed replay safety and explicit approval. A denial
does not spend the remediation budget.

## Monitoring setup

**Monitoring setup** manages operational scope, not human permissions.
**Access & permissions** remains the read-only view of Entra app-role claims.
The API authorizes every request; neither SQL monitoring records nor a browser
control can grant an app role.

### Scope preview and activation

1. Inspect deployment status. Missing schema, maintenance, incompatibility and
   wrong-tenant diagnostics block setup rather than creating tables or loading
   old target configuration. Wrong-tenant diagnostics omit foreign control and
   schema metadata.
2. Queue inventory discovery for the deployment tenant or selected workspace.
   This is asynchronous work, not a completed scan. The UI reads server-owned
   inventory and its named domains, workspaces and items; it does not enumerate
   Fabric using the browser's identity.
3. Define one positive include rule for the tenant, domain, workspace or item,
   then add exclusions. Domain descendants are optional. Exclusions win, and
   overlapping scopes must not create a second effective target or action budget.
4. Select supported workloads and polling/reconciliation cadence. Defaults are
   300 seconds for polling and 900 seconds for event reconciliation; the form
   accepts whole-second intervals from 15 to 86400. These settings are not a
   detection-latency guarantee. Events do not remove the need for REST polling.
5. Choose the future-resource policy explicitly. Future items require review
   unless automatic detection-only enrollment is selected. That option never
   copies a safety review or enables an action on a new target.
6. Select **Preview scope changes**. Inspect admissions, pauses, removals,
   required service permissions, inventory completeness, gaps and poll/event
   subscription changes. A preview can persist a plan, but performs no Fabric
   provisioning, permission grant or workload remediation.
7. Activate the current, unexpired preview using its expected tenant, epoch and
   registry revision and its original idempotency ID. Preview and activation
   have separate receipt namespaces; generating a new activation ID would
   break the stored-plan binding and be refused. Activation queues work.
   **Configuring** is not a readiness
   result; refresh coverage and connector proof after the worker processes it.

These choices bound inventory and polling scope. They are not wildcard
subscriptions to every operational event in a tenant, domain or workspace.
Native Fabric Job-event sources require their own supported, owned configuration.

Preview readiness is selector-aware, unlike the estate-wide coverage snapshot.
A complete workspace A can support a ready preview while workspace B keeps
estate coverage Partial and **Inventory total** Unknown. Missing workspace/domain
metadata still blocks expansion. Explicit disable/contraction operates on stored
admissions and is not blocked merely by such an outage.

The source activation contract permits an unrelated data-revision change only
after re-evaluating the original scope inside the locked activation transaction
and confirming identical reviewed material effects. New targets, changed
capabilities/subscriptions, gaps or permissions, expired TTL, epoch drift or
policy changes refuse activation. This is not a blanket stale-plan retry rule.
Native SQL receipt replay and a workspace-scoped UI activation have been
verified with the original preview identity. This does not establish native
Eventstream delivery, domain-only admission, source restoration or completion
of the broader native acceptance matrix.

The UI clears dependent selections and invalidates stale reads/previews.
Native SQL scope projections may return `updated_at=null`; this is an unknown
display timestamp, not a missing policy or permission decision. The UI retains
the versioned scope and does not invent an update time. If one catalogue request
fails, its unfinished sibling page reads are cancelled before another polling
batch, while setup remains locked on the reported error.
An existing policy can be submitted for disabling with its saved include/exclude
rules unchanged even if workspace metadata is now unknown or missing. New
rules, expansion and re-enabling still require current metadata, and the backend
must accept the versioned transition. Pausing monitoring cannot retract an
action already committed to external execution.

Tenant-wide scope means the one deployment tenant, not every tenant an
administrator can access. A domain is metadata, not a data-access grant.
Existing application Readers can inspect admitted incident records, so broader
monitoring can broaden the dataset visible to those Readers. Review that
exposure before activation; this app does not add per-workspace human ACLs.

### Coverage, inventory and connector proof

Coverage separates deployment/estate-wide **discovered**, **supported**,
**access verified**, **admitted**, **current** and **action enabled** counts.
Unsupported items are reported separately. The UI denominator is **Inventory
total**, not a scope denominator: it remains **Unknown** until every latest
discovery generation is complete. For example, a complete A with three items
plus a partial B with five cannot make eight a known complete inventory total.
A caller-visible listing is not complete tenant inventory. Partial enumeration
preserves known records and reports gaps rather than treating unreadable pages
as deletion.

Admitted/current counts describe target state, not proof that every poll window
or event delivery is current. Read their accompanying timestamps and gaps.

Canonical target, source-execution and incident identities are distinct;
display names are not identity keys. Failure grouping uses the existing
normalized failure signature with the immutable target key as input, not a new
digest or wire version. Power BI refresh-history IDs and request IDs occupy
different namespaces. Equal-looking values are not proof of the same execution;
their correlation requires authoritative source evidence.

Only semantic models/datasets and Data Pipelines have the current failure
detector contracts. Other returned item types, including Notebook, Report,
Lakehouse and Warehouse, remain visible with their unsupported reason.
Notebook activity evidence belongs to its monitored pipeline; standalone
notebook-job monitoring is not implemented.

Discovery does not provide all Fabric operational telemetry. Missing expected
starts, disabled pipeline schedules, tenant-wide data quality, audit, report
usage and capacity monitoring require separate expectations or collectors.
Existing silent-health probes remain explicitly configured.

Inspect the completed inventory/poll timestamps, receiver activity, checkpoint
lag, backlog, next due work and gaps. A retained-history limit can leave a poll
window incomplete despite successful HTTP pages. An idle controller heartbeat
does not prove collection is healthy.

Read readiness in layers:

| Evidence | What it establishes | What it does not establish |
|---|---|---|
| Accepted request or queued inventory work | Durable intent | Completed collection |
| Persisted workspace/domain metadata | Selectable IDs/names from the service | Source item/history access |
| Successful source-access probe | That operation under the collector identity | Current full coverage or remediation permission |
| Current complete observation window | Coverage of that bounded window | Replay safety or an approved action |
| Current scope, review, explicit approval and reservation | Permission for one bounded action when all guards pass | Verification that the external action succeeded |

The deployed collector-only heartbeat has no connector and cannot report event
delivery. Any latest partial generation keeps estate completeness partial and
**Inventory total** Unknown, including with zero scopes. A ready scoped preview
for a complete chosen workspace does not narrow those snapshot counts or grant
permissions. An empty target list is not evidence that the tenant is healthy.

Connector records show owned source subscriptions, desired/observed topology,
identity and delivery proof timestamps, and explicit gaps. Planned connectors
can have unassigned topology IDs without being labelled ready. **Blocked** and
**Partial** are not healthy coverage. A capability ID on a target is a reference
to evidence, not a fresh access test.

Service permissions are separate from human app roles. Power BI refresh-history
reads require semantic-model Write permission on the collection identity.
An Admin who can browse an item does not prove that the worker can read it.
Likewise, a configured event subscription or receiver heartbeat does not prove
an eligible failure reached durable SQL intake.

### Physical removal and restoration status

Expired or unknown capability, incomplete inventory and a target missing from
an eligible list are not physical source-removal instructions. Desired planning
needs affirmative effective-policy disable/exclude/deletion authority, with
overlap and unknown-domain checks. The UI must keep uncertainty visible while
intake/action/readiness fences remain enforced.

A physically present source may still have a valid pending-removal fence.
The source contract now supports controller-only receipt-bound supersession,
not an operator/browser cancel or force-Ready control. It requires the exact
original fresh complete GET-only worker presence inspection, original
removal/history and current work/lease proof that the removal was neither
dispatched nor possibly applied. Current manifest equality and `attempts=0`
alone are not sufficient evidence.

The current narrow restoration guard requires fresh verified READ and current
reviewed or `auto_detection_only` admission under directly matched
tenant/workspace/item includes. Domain-only admission does not qualify; domain
exclusion/unknown authority can hold recovery. Explicit denied/blocked capability
cannot be overridden, and no action capability is granted.

A new receipt preserves the superseded pending-removal audit and physical
identity, but publishes new unready desired state with old identity/delivery
proofs cleared. Fresh post-publication capability, collector identity and actual
delivery evidence must precede controller Ready. Notes, human tracking
resolution, re-registration, manual SQL or reset cannot bypass these gates.
This describes implemented/in-review source behavior, not a claim of current
live restoration. See
[held removals and restoration](OperationsGuide.md#held-removals-and-receipt-bound-restoration).

### Per-target safety review

Readers can open an existing safety review from target details. Only Admin can
create or revise one action profile for the selected target.

| Profile | Explicit reviewed intent |
|---|---|
| Power BI refresh | Optional reviewed JSON parameter object; blank/null is distinct from an explicit object |
| Full pipeline rerun | Current inventory definition hash, an explicit parameter object (`{}` is valid) and a fresh replay-safety attestation for verified replay |
| Gateway rebind | `gateway_id` and sorted, distinct, canonical `datasource_ids`, without credentials |
| Schedule re-enable | Exactly `{enabled: true}`, not an immediate schedule change |

A review binds its target, action, policy revision, next review revision, expiry
and reason. The server replaces the reviewer identity with the authenticated
User. Definition, service identity, correlation and configuration proof remain
server-owned. The UI cannot invent a missing pipeline definition hash.

The UI separates the requested state, recorded state and publication status:

| Record | Meaning |
|---|---|
| Accepted intent | `state=pending`, `publication_status=pending_validation` and `requested_state` holding the original choice. Technical correlation is not asserted and `revoked_at` remains null. |
| Current published review | `publication_status=published` with the controller's actual result. The original requested state can remain different, such as a request for `verified` published as `unverifiable`. |
| Original operation receipt | Immutable evidence of the original committed request. It can remain pending intent after the current review has been published. |

Publication metadata is returned by the service, not supplied by the browser as
a human proof claim. A requested `verified` state, accepted intent, or published
review alone does not authorize a remediation. Current target/action admission,
policy and any required individual approval remain separate.

**A committed revocation intent immediately fences new action reservations.**
It does not cancel an action already reserved or committed to external execution;
that action retains its submission, verification and finalization obligations.
An expired review can still be the subject of a pending revocation. Its old
expiry and null `revoked_at` do not mean the revocation request failed;
the controller supplies the published revocation state and timestamp.

Pending or unverifiable profiles are not action-enabled authority. If store
redaction removed parameters, their fingerprint is not executable replay input.
Never copy redaction placeholders into a new review. The current store requires
a currently admitted target for review mutations, including revocation; paused,
removed and review-required targets expose that limitation rather than a
visual-only switch.

An Admin's profile configuration is not an Approver's individual decision.
Approval remains fingerprint-matched, unexpired and single-use, and a denial
spends no remediation budget. The controller still enforces admission,
allowlists, deterministic evidence and a durable action reservation. A submitted
job or saved review is not a verified repair.

### Current profile and lookup freshness

After accepting profile A, the UI temporarily follows A while publication and
target assignment catch up. This lookup pin is not the original operation
receipt and must not become a permanent selector.

The pin can retire when A has a matching current published assignment. If a
different profile B supersedes A while the pin remains, the UI adopts B only
after validating a newer current-policy assignment and B's published
target/action/review-revision/policy binding. Old assignment snapshots or a
merely pending B do not establish that change. Automatic and manual refresh
use the same checks.

Changing the current lookup does not rewrite A's immutable receipt. Stale
responses cannot overwrite newer publication or another target's draft.
Readers can inspect the current assigned review and reconcile an original
operation, but save/revoke controls still require Admin and a successful
snapshot from the current permission-refresh generation.

### Configuration receipt recovery

After a lost response, keep the original request identity. Do not repeat a save
with a new ID or clear an unresolved submission from a GET of an older review.

Scope activation has a plan lookup and an original-idempotency-ID receipt
lookup at `GET /api/monitoring/activations/{idempotency_id}`. A confirmed
activation still requires fresh coverage/provisioning evidence.

Safety-review recovery retains the original request/review IDs and a nonsecret
target/action/requested-state/review-revision/policy-revision binding across
reload. Parameter text is never persisted to browser local or session storage.
The frontend requires an original-request operation receipt:

```text
GET /api/monitoring/safety-review-operations/{request_id}
Response: { request_id, review: SafetyReview }
```

The current backend router exposes this Reader-authorized HTTP projection of
the immutable operation receipt. It returns the original request ID and
committed, redacted review, not the latest review. The stored receipt also binds
the original target/action and revisions. A `404` means that operation has not
been observed, not that a write is safe to repeat.

Missing or mismatched receipts keep changes locked. Current-review lookup at
`GET /api/monitoring/safety-reviews/{review_id}` is not a substitute, even if
its version looks plausible. Once the original operation is confirmed, current
review/target admission and the permission-refresh generation must be loaded
before another edit. If the receipt contains pending intent, use the current
review lookup to observe publication; do not mutate the receipt or issue a new
blind request ID. These source contracts and the completed SQL bootstrap do not
prove deployed API receipt recovery, production runtime SQL permissions or
full-cutover acceptance.

## Local preview

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev,web]"
npm --prefix .\command-center ci
npm --prefix .\command-center run build
$env:MONITORING_MODE = "fixture"
.\.venv\Scripts\bi-triage.exe serve --mode demo --host 127.0.0.1 --port 8058
```

Open `http://127.0.0.1:8058`. Demo mode uses synthetic state and mock tools even
when a local `.env` contains live settings. It rejects non-loopback clients and
refuses to start inside an Azure-hosted web app.

The pending demo approvals belong to running mock-controller flows. Approving
or denying them changes those flows, rather than editing detached example
rows. Restarting the local demo creates fresh isolated fixtures.

Fixture mode is an explicit offline store, not a fallback for a live SQL error.
The normal monitoring/event worker requires live configuration and service
identity; it cannot be used as a fixture-mode shortcut.

![Earlier-release local synthetic command-center preview](./images/command-center/local-command-center.png)

This image is a local synthetic preview, not evidence of a production action.

The following local examples show a real mock-controller approval gate and a
records-based question. The web controls, not a Teams callback, supplied the
decision. All tools and data in these images are synthetic.

| Approval review | Read-only question |
|---|---|
| ![Synthetic approval review](./images/command-center/local-approval-review.png) | ![Synthetic records-based question](./images/command-center/local-read-only-question.png) |

## Authentication and roles

The SPA uses authorization code with PKCE through MSAL. The API validates an
RS256 Entra JWT against a fixed tenant, issuer, application audience, expiry,
issued/not-before times, object ID, delegated `access_as_user` scope and
application roles. It does not trust a browser-supplied responder or an
`X-MS-CLIENT-PRINCIPAL` header.

| App role | Permissions |
|---|---|
| `CommandCenter.Reader` | View state, monitoring coverage and existing safety reviews; ask read-only questions |
| `CommandCenter.Operator` | Reader access, enqueue admitted-target investigations, add incident notes and record user resolutions |
| `CommandCenter.Approver` | Reader access and approve or deny eligible proposals |
| `CommandCenter.Admin` | All application permissions, monitoring/safety-review configuration, scenario validation and explicit uncertainty reconciliation |

Operator and Approver are separate: neither includes the other. Admin includes
both but grants no Entra directory administration.

The web app uses a user-assigned managed identity for SQL and Foundry. These
service permissions are separate from the human application roles.

The profile menu obtains the signed-in user's photo with a separate Microsoft
Graph **delegated `User.Read`** token. The command-center API token is never sent
to Graph. No directory-wide read/write permission is required. A missing photo
uses initials; unavailable consent or a failed photo request does not sign the
user out or silently grant a permission.

### Entra-managed groups

Entra app-role claims are the sole application permission authority. The API
validates them on every request; SQL incident state cannot grant access.
**Access & permissions** is read-only and shows the signed-in user's effective
roles, not a locally maintained roster or unverified group-membership claim.
It is available to Reader and all higher application roles. The page reports
**Token issued** and **Token expires** from verified API-token claims, alongside
when those details were received. Times use the browser's local time zone.
Missing claim metadata is not inferred from configuration or the current clock.

The application does not perform a runtime Graph lookup of group membership,
identify which group supplied a role, or offer Add, Edit, invite or local-grant
operations. The web identity has no directory-management permission.

Use ordinary Entra security groups assigned to the existing app roles:

| Default group name | Assigned app role |
|---|---|
| BI Triage Readers | `CommandCenter.Reader` |
| BI Triage Operators | `CommandCenter.Operator` |
| BI Triage Approvers | `CommandCenter.Approver` |
| BI Triage Administrators | `CommandCenter.Admin` |

Group owners or authorized IT administrators add existing users by name or
sign-in address in Microsoft Entra. Group-based application assignment requires
Entra P1/P2 and does not include nested group membership.
[Microsoft Learn: application assignments](https://learn.microsoft.com/entra/identity/enterprise-apps/assign-user-or-group-access-portal).
Application permissions do not grant direct Azure, Fabric or SQL access.

After registering the application, an authorized operator can prepare the group
configuration below. It is an offline plan unless `--apply` is added:

```powershell
.\.venv\Scripts\python.exe scripts\configure_command_center_groups.py `
  --subscription "<subscription>" --tenant-id "<tenant-guid>" `
  --app-id "<application-client-id>" --admin-current-user
```

With `--apply`, the helper creates owned ordinary security groups, assigns the
current operator as their owner and as a member of the administrator group, and
binds the app roles. It refuses unmarked name collisions and does not delete
existing assignments, including direct user assignments.
Creating groups and app-role assignments requires appropriate permissions on
the operator's directory identity; none are granted to the web application.
Manage subsequent membership in Entra, not through this helper.

**Refresh permissions** requests a fresh API token for the current session.
It then reloads effective access and the command-center snapshot. Actions stay
locked until a snapshot loaded after that renewal confirms current permissions;
an older successful snapshot cannot unlock them. Renewal failure remains an
explicit error, and sign-in or consent is never opened automatically by refresh.
Entra changes can take time to propagate, and already-issued tokens may retain
old roles until they expire or are replaced. Refreshing one session does not
revoke another user's token. Deployments needing a shorter revocation window
require a separately designed current-state authorization check; this app does
not silently substitute a stale local permission cache.

Earlier versions used a SQL membership table and an in-app permission editor.
The SQL authorization store, service, table schema and importer are retired;
there is no database authorization fallback. Authenticated calls to the old
`GET`/`POST /api/admin/users` and `GET /api/admin/audit` routes return `410` with
`managed_in_entra`. Old `?view=admin` links open the read-only **Access &
permissions** page instead.

Do not enable `COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED`; the app rejects that
retired mode. The deployment helper also refuses to overwrite a deployment
where it is still enabled. Verify the existing Entra assignments and a fresh
administrator session before removing or disabling that setting. That earlier
authorization-only cutover did not require deleting incidents, notes, approvals
or run history. Do not confuse it with the separately authorized hybrid
prototype reset. Neither cleaning up an old authorization table nor deploying
the web app is consent to erase operational state.

## Deployment

Complete the existing [deployment prerequisites](./DeploymentGuide.md) first.
Provision the shared Azure SQL database and prepare the Foundry project
independently. The command-center template does not create, replace or delete
them. Require SQL public-firewall admission, Entra-only authentication, TLS,
auditing and TDE; web reachability alone does not establish SQL access.

These steps are a reference for separately reviewed deployments and updates.
The current web/controller cutover is complete; do not repeat initialization,
restore historical maintenance/grants or redeploy merely because older proof
captures differ. Do not run old and new writers against shared state or use an
older target/schema loader as a rollback path. The remaining
[hybrid acceptance gates](./HybridMonitoringPlan.md) cover event-enabled operation,
full source collection/recovery, coverage and observability; a web deployment or
earlier frontend pass does not close them.

### 1. Register the SPA/API

```powershell
.\.venv\Scripts\python.exe scripts\register_command_center.py `
  --subscription "<subscription>" `
  --tenant-id "<tenant-guid>" `
  --display-name "BI Triage Command Center" `
  --webapp-origin "https://<app-name>.azurewebsites.net" `
  --admin-current-user
```

The script creates no secret or certificate. Save the returned application
client ID. Reruns can specify `--application-id` to reuse that registration.
`--authorize-azure-cli --grant-admin-consent` is an explicit evaluation option:
it grants delegated consent for the selected administrator, not the entire
tenant. It is not required for normal browser use.

The registration requests delegated Graph `User.Read` for the profile menu.
`--grant-profile-consent` separately grants that scope only for the selected
administrator and this SPA. It creates no Graph application permission or
tenant-wide grant. Other users follow the tenant's normal consent process.

### 2. Provision the public HTTPS web host

Create an operator-owned JSON settings file outside the repository. Values must
be strings. Live hybrid operation requires `MONITORING_MODE` set to `live`,
`MONITORING_TENANT_ID` matching the application's Entra tenant,
`AZURE_SQL_SERVER` (`<server>.database.windows.net`, without protocol or `,1433`),
`AZURE_SQL_DATABASE` (the database name from the Azure deployment) and
`FOUNDRY_PROJECT_ENDPOINT`. Common optional keys are
`FOUNDRY_TRIAGE_AGENT_NAME`, `FOUNDRY_DQ_AGENT_NAME`,
`FOUNDRY_OBSERVER_AGENT_NAME` and `COMMAND_CENTER_QUESTION_PROVIDER`.
`FABRIC_PIPELINE_TARGETS` is retired. The live target list comes from the shared
monitoring registry, not environment fallback or an imported old target file.
No connection secret, webhook or filled-in `.env` is accepted by the deployment
script. There are no Fabric SQL setting aliases, legacy-store adapters or
cross-platform state migration.

```powershell
.\scripts\deploy_command_center.ps1 `
  -Subscription "<subscription>" -ResourceGroup "<resource-group>" `
  -Location "<region>" -AppName "<app-name>" -PlanSku P0v3 `
  -TenantId "<tenant-guid>" -ApplicationClientId "<application-client-id>" `
  -CostCenter "<cost-center>" -Owner "<owner>" `
  -Environment "Evaluation" -DataClassification "Synthetic" `
  -ApplicationSettingsFile "<absolute-path-to-settings.json>" `
  -ValidateOnly
```

Review the what-if result, then use the same parameters with
`-Deploy -ProvisionOnly`. This creates a Linux App Service plan/app, user-assigned
identity and public HTTPS host, without VNet/NAT/private-endpoint resources. Select a
region/SKU that passes actual ARM validation: advertised quota alone does not
prove worker admission.

The template always uses public HTTPS and disables basic SCM publishing and
FTPS. Entra API roles, the web MI and Entra-authenticated deployment remain
required. Optional `publicAccessClientCidrs` and `scmAccessClientCidrs` provide
separate persistent network filters; empty arrays leave the respective network
endpoint public without granting anonymous API or deployment permission.

The helper exposes these as `-PublicAccessClientCidr` and
`-ScmAccessClientCidr`. Omit both for the normal public baseline, or supply the
independently approved caller ranges. An app filter does not apply to SCM.
There is no temporary-public switch or automatic restoration to private access.
See [public network controls](./DeploymentGuide.md#network-and-governance-invariants)
and [resource-scoped exceptions](./DeploymentGuide.md#governed-evaluation-exceptions).

### 3. Install the schema and grant service access

Initialize the new baseline with deployment-owned tooling, not a runtime
`ensure_schema_once` call. The
[monitoring schema](../src/triage/monitoring/schema.py) defines control, records,
leases and operation receipts alongside the existing incident, approval,
command, run/event and incident-activity tables. Runtime store construction
checks the deployed schema, tenant, epoch and activation cutoff; it performs no
DDL and has no live in-memory fallback.

For the approved clean start, use the
[controlled reset plan](./HybridMonitoringPlan.md#controlled-prototype-reset)
and [deployment reset tool](../scripts/reset_monitoring_state.py). Prepare and
inspect the ownership/deletion manifest first, stop old writers, reconcile
in-flight or uncertain actions, then execute only the explicitly authorized
reset and initialize the new epoch. A fresh Azure SQL database uses the
initial-bootstrap path after its application schema is installed, not a reset
or upgrade of the previous Fabric SQL store. The current reset tool targets
Azure SQL only; prior-store disposal is separately scoped cleanup after
quiescence. No operational-record migration or legacy configuration import is
provided.

One explicitly authorized operator may prepare, bootstrap and execute this
procedure. It does not require a separate signer, certificate or new signing
key. Exact target/manifest checks, observed writer/action state and durable
operation receipt reconciliation are still required. An uncertain reset must
not be repeated as another delete.

Do not reuse the earlier release's direct table-DML grant list for the new
monitoring runtime. The current boundary separates component writers through
checked views and static guarded procedures:

| Component | Intended write authority |
|---|---|
| Web/API | Human scope/review intent and controlled decisions, not published controller authority or action/finalization state |
| Worker | Raw observations, collection progress and guarded intake, not validated admission, review proof or action budgets |
| Controller | Deterministic publication and guarded controller work, action and finalization state |
| Deployment operator | Approved schema, component grants, maintenance and separately authorized bootstrap/reset |

Runtime identities must not receive unrestricted monitoring-base-table DML,
broad database roles, schema ALTER or impersonation permission. Ancillary
application tables receive only the reviewed object/column grants required by
their current callers. Human app roles remain authoritative
in Entra; this SQL separation is for service components, not a human ACL table.
Use the reviewed component-specific permission contract and prove its allowed
and denied operations under the actual managed identities on an approved
isolated proof target before applying runtime grants to the shared deployment.
See the
[SQL component boundary](./HybridMonitoringPlan.md#sql-component-boundary-correction).
Offline kernel review closure does not open that deployment gate.

Incident collaboration remains append-only and does not update the controller's
incident outcome or budget. Set `INCIDENT_ACTIVITY_TABLE_NAME` when using a
distinct deployment prefix. Its source-revision check hashes the original SQL NVARCHAR
payload with SHA-256 over UTF-16 LE bytes, matching SQL `HASHBYTES`.
Reserialization or new controller evidence therefore invalidates an older user
closure; hashing a reserialized Pydantic model would not reliably match the
stored payload.

For an **Azure SQL** Entra service principal or managed identity,
`CREATE USER ... WITH SID ..., TYPE = E` encodes its **client ID** as
little-endian GUID bytes, not its principal object ID. Azure RBAC and Fabric
role assignments use the **principal object ID** instead.
See the public [CREATE USER documentation](https://learn.microsoft.com/sql/t-sql/statements/create-user-transact-sql#arguments).
Do not treat these IDs as interchangeable. Verify each runtime component's
native Azure SQL SID and actual MI sign-in before accepting its binding.
The current runtime mappings, kernel roles and ancillary grants are installed,
and web/controller SQL paths have been exercised with the correct identities.
The normal-worker path and broader allowed/denied operation coverage remain
separate proof obligations; a generic monitoring role does not cover all
application-store calls.
Azure SQL supports database-scoped `WITHOUT LOGIN`/`EXECUTE AS USER` tests, but
those do not prove the deployed identity or network/firewall admission.

Azure SQL does not inherently restrict authentication to Entra: configure the
logical server's Entra-only setting explicitly. Its Entra administrator installs
the reviewed users/schema/grants; the application's Admin role grants none of
that authority. Runtime access uses token-authenticated contained users and SQL
component roles, not directory lookups, Fabric item grants or human app roles.

Grant the identity only the Foundry access needed to invoke the configured
agents. The current provider posts to the **project Responses API** with an
`agent_reference`; this is not the same authorization boundary as a published
agent endpoint. In live verification, endpoint-consumer and restricted custom
roles could read agent metadata but could not perform inference.
[Foundry User](https://learn.microsoft.com/azure/foundry/concepts/rbac-foundry)
(`53ca6127-db72-4b80-b1b0-d745d6d5456d`) at the dedicated Foundry account scope
was verified for this provider.

That role includes agent-development permissions, not just invocation. Keep the
account dedicated to this accelerator and do not present it as an
invocation-only grant. The reasoning agents still receive no Power BI or Fabric
permissions. Endpoint-only callers such as the scheduler use the narrower
Foundry Agent Consumer role. Prove the actual operation under each managed
identity; a role assignment's existence is not an access test.

Fabric item authorization, Foundry inference and SQL firewall admission are
separate prerequisites. The SQL default `allowAzureServices=true` uses the
special start/end `0.0.0.0` rule for Azure-hosted callers, including other
subscriptions, not all Internet IPs. It does not replace an authorized Entra
database user. Verify each backend under its actual runtime identity.

### 4. Configure and deploy the controller

The controller and web app must point at the same state database, monitoring
tenant and registry. Epoch and target admission come from that registry, not
an environment target list. Set these in the selected azd environment:

```powershell
azd env set MONITORING_MODE live
azd env set MONITORING_TENANT_ID "<tenant-guid>"
azd env set RUN_HISTORY_ENABLED true
azd env set APPROVAL_DELIVERY_MODE web
azd env set NOTIFICATION_CHANNEL web
azd env set COMMAND_CENTER_URL "https://<app-name>.azurewebsites.net"
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py
azd deploy bi-triage-controller --no-prompt
```

Registration updates the triage definition and adds the tool-free observer.
Local prompt edits alone do not change a registered Foundry agent.

The monitoring worker is a separate outbound service using an explicitly
selected managed identity. Its live configuration binds the Azure/monitoring
tenant, identity and SQL database. Start with `-CollectorOnly` for workspace
inventory and REST polling, without event metadata; see the
[collector-only quickstart](DeploymentGuide.md#collector-only-quickstart).
Event mode instead requires the complete app-owned endpoint binding.
Provide nonsecret namespace/entity/consumer-group identifiers, never keys or
credential-bearing connection strings. The
[worker source](../src/triage/monitoring/worker.py) defines its required settings.
Entra custom-endpoint bootstrap and its nonsecret connection metadata still
require explicit operator setup. The UI does not provide a verified automatic
end-to-end Entra endpoint bootstrap; this remains an automation gap. Do not
substitute retrieved keys or a shared-secret connection string.
The worker's `--reconcile-once` mode performs bounded connector work; its
`--transport-probe` mode does not establish SQL acceptance or worker readiness.
Neither is a substitute for normal durable intake and restart proof.

The selected hybrid topology uses REST plus native Fabric Job Eventstream
delivery to that worker. It needs no Activator, Power Automate, Eventhouse or
separate Azure Event Hubs namespace. Public outbound TLS for the custom endpoint
does not create a worker ingress or grant access to unrelated services.

### 5. Package and deploy the web app

Run the deployment script again with `-Deploy`, without `-ProvisionOnly`.
It typechecks/builds the frontend, includes the canonical scenarios and mock
assets, scans the staged package for credentials, and uploads a ZIP through
Entra-authenticated deployment. Python dependencies are installed by Oryx.
The helper waits for ZIP/Oryx completion, then checks `/api/health` directly.
It disables the CLI's separate startup-status tracker, which can keep waiting
after Kudu and the actual web process are ready. This liveness check does not
replace authenticated SQL and Foundry checks.

The deployment host must reach the public app and SCM hostnames. If optional
caller filters are configured, use the separately approved app and publisher
CIDRs with `-PublicAccessClientCidr` and `-ScmAccessClientCidr`. These filters
persist; the script does not revert the app to private access. Keep the
existing independent filters during a code-only release rather than replacing
them with empty defaults.

With a proxy, the address reported by a generic IP-echo service can differ from
the address App Service sees. Inspect `x-ms-forbidden-ip` on a controlled SCM
request rejected by a configured network filter, then supply the explicitly approved proxy egress
addresses as separate `/32` entries. Do not widen the rule to an entire network.
Global Secure Access can produce this difference. Its egress addresses can
change or be shared, so an IP allowlist does not uniquely identify a device.
Keep Entra authorization enforced and refresh only verified client addresses;
do not disable Global Secure Access to make the application reachable.

### 6. Schedule command draining

For hybrid operation, deploy `infra/scheduled-sweep.json` with
`command="heartbeat"`, and grant its managed identity permission to invoke the
hosted controller. Create it disabled, verify identity/schema prerequisites,
then explicitly enable it during cutover. Use the same workflow as
[scheduled sweeps](./DeploymentGuide.md#6c-scheduled-sweeps), with a distinct
Logic App name. A manual equivalent is:

```powershell
azd ai agent invoke bi-triage-controller "heartbeat"
```

The heartbeat gives both durable monitoring work and human commands a bounded
slot per round and propagates either queue's failures. The separate worker
collects inventory, REST history and events; the web process does not do those
scans or execute production investigations. `command sweep` remains available
when only human-command draining is wanted.

The scheduler template submits a stored background response, retains its ID,
and polls that exact response for up to 15 minutes. It does not hold a single
HTTP request open for that duration; Consumption caps that request at 120 seconds.
Only a completed response without an error is success. POST retries are disabled,
GET retries retain the original ID, and scheduler runs are serialized.
Heartbeat admission has an
840-second monotonic budget including lock acquisition, with two automatic and
one human-command concurrent slots. Refills obey queue limits and the remaining
execution allowance. Insufficient allowance stops new claims;
lock-wait timeout does not cancel the lock holder, and already admitted work
settles under its own fences. This is not a guarantee that a whole backlog fits
one invocation. A missing response is not evidence that no work executed.
The scheduler also validates the Responses body: HTTP 200 is insufficient when
`status` is failed or an error is present. Those runs remain failed even without
a Teams failure webhook.
Logic Apps' binary `$content` wrapper is decoded first when present, including
responses carrying `Content-Encoding: identity`.

The existing one-minute command scheduler was reused by changing only its
command to `heartbeat`; no second timer or mailbox path was enabled, and the
separate silent schedule was unchanged. Three real recurrence responses were
verified completed. The deployed missing/failed-heartbeat alerts are portal-only:
`RunsSucceeded < 1` over 15 minutes and `RunsFailed > 0` over 5 minutes.
The optional app-owned Insights log-absence rule uses a summarizing query that
returns one zero-count row when no completed heartbeat is present; metric
no-data is not treated as zero. Its isolated validation rule fired and resolved,
then was disabled. The production log rule remains enabled with no actions.
No actual controller stop or Action Group/email/webhook delivery was tested.

Without the controller drain, admitted work remains queued. Without the
collector/receiver, no fresh collection is implied by a successful heartbeat.
Configure scopes and inspect current coverage explicitly; an empty target list
must not select an arbitrary model or pipeline.

## Interrupted commands and reconciliation

Commands are claimed conditionally in SQL. Separate command IDs for the same
normalized target also share a target claim. An expired execution becomes
`interrupted`, not queued again. A timeout after entering execution, an
unconfirmed remediation or an uncertain finalization blocks further command
execution for that target.

Candidate queries exclude blocked targets before applying their limit, so a
large blocked backlog cannot hide unrelated eligible work. The worker checks
its remaining execution budget again after command acquisition; acquiring a
command cannot extend the deadline or authorize a late dispatch.

An administrator must inspect the target's actual state, then record a
reconciliation reason. Reconciliation marks the interrupted command as terminal
failed, preserves its idempotency identity and audit fields, and performs no
remediation. Submit a new investigation only after that review.

The hybrid source path adds shared tenant/epoch/target/execution admission and
action fences across intake and controller work. It does not retroactively
protect an older deployed writer: cutover must stop old versions rather than
assume they honor the new registry. See the
[controller and state boundaries](./TechnicalArchitecture.md).

## Validation and evidence

Enable `-Evaluation -EvaluationExpiresOn "<YYYY-MM-DD>"` only for a controlled
evaluation. `-EvaluationCostExemption` additionally opts into a finite
cost-control exemption; the review-date tag is not an automatic shutdown.
Governance can stop or resize resources after an exemption ends.

The administrator Scenario validation page runs the canonical cases
advertised by the API with at most two concurrent requests. Each run records
expectations, outcome and a durable run ID. Mock-provider validation checks
deployed controller behavior. Foundry-provider validation also exercises
registered agents and real model calls. **Both use synthetic tools and isolated
scenario state**, not production remediation, and neither resets shared
incident tables. Demo mode offers only the mock provider.

**Stop queue** prevents new requests; it cannot cancel work already accepted by
the server. Assertion failures do not stop the remaining cases, but transport
uncertainty or lost authorization stops further scheduling. Requests are not
automatically retried. Review recorded results before submitting a case again.

Use separate live checks for identity access, SQL arbitration, scheduling and
real Fabric job/activity correlation. `/api/health` proves only process
liveness. SQL errors and failed model calls must remain explicit errors, never
invented healthy state or a silent mock fallback.

The current web/controller cutover, collector-only workspace inventory and
recorded heartbeat recurrences and native paired-heartbeat telemetry are verified.
Event intake, full source collection/recovery and sustained operation remain
separate acceptance work. Validate each final artifact after integration; an
earlier frontend pass does not validate later source changes. Screenshots must
identify the actual deployment and scope they show, rather than implying
worker or end-to-end acceptance from a successful web page.

Foundry validation consumes the model deployment's rate limits. If a batch
reports 429, pace cases rather than increasing controller policy budgets.
Review the recorded exception and rerun the affected cases after the quota
window resets. SQL-unavailable errors retain a redacted connection cause so an
operator can distinguish a login/import failure from a connection timeout even
when a hosted session's earlier console output is no longer available.
The reconnect cooldown starts only after an actual connection failure. Its
never-failed sentinel is `None`: using zero incorrectly blocked first
connections during a fresh hosted sandbox's first 30 seconds of monotonic
uptime, without attempting SQL or producing a connection exception.

Disable evaluation and remove temporary access/exemptions when finished.
Full prompts, completions and reasoning are not written to run events or
telemetry; recorded evidence is redacted at the store boundary. Explicit
read-only questions and observer answers are retained as redacted discussion
and run records, not as telemetry.

## Earlier-release deployment validation record

This section records the previous Fabric SQL release's evaluation, not
acceptance of Azure SQL or the new hybrid monitoring deployment. The current
delivery/intake/cutover gates remain listed under
[hybrid implementation and acceptance status](#hybrid-implementation-and-acceptance-status).

The 2026-09-12 evaluation ran all 15 canonical cases on the deployed web
application with both provider modes. Each terminal result and its events were
read back from Fabric SQL. **Pass means the scenario's expectations matched,
not that every incident was resolved.** Denial, suppression, escalation and
pending verification are expected outcomes in their respective cases.

Both modes used synthetic tools and isolated scenario state. Foundry mode also
used the registered agents and real model calls. These results do not claim
that a production dataset or pipeline was remediated.

| Scenario | Verified outcome | Mock | Foundry |
|---|---|---|---|
| `scenario1-transient` | `resolved` | Pass | Pass |
| `scenario2-data-quality` | `flagged_data_quality` | Pass | Pass |
| `scenario2b-known-issue` | `duplicate_suppressed` | Pass | Pass |
| `scenario3-policy-block` | `needs_human` | Pass | Pass |
| `scenario4-unknown-action` | `needs_human` | Pass | Pass |
| `scenario5-approval-granted` | `resolved` | Pass | Pass |
| `scenario6-approval-denied` | `approval_denied` | Pass | Pass |
| `scenario7-schedule-reenable` | `resolved` | Pass | Pass |
| `scenario8-capacity-backoff` | `deferred_retry` | Pass | Pass |
| `scenario9-pipeline-authentication` | `needs_human` | Pass | Pass |
| `scenario10-pipeline-rerun-approved` | `resolved` | Pass | Pass |
| `scenario11-pipeline-rerun-denied` | `approval_denied` | Pass | Pass |
| `scenario12-pipeline-schema-mismatch` | `needs_human` | Pass | Pass |
| `scenario13-pipeline-rerun-pending` | `needs_human` | Pass | Pass |
| `scenario14-pipeline-write-timeout` | `needs_human` | Pass | Pass |

The model deployment allowed 50,000 tokens and 50 requests per minute.
Concurrent validation hit 429 limits; the affected cases passed when run one
at a time with 75 seconds between cases. The validation client refreshed its
delegated token before each authenticated request. Controller policy budgets
were not increased, and failed attempts remain in history.

Separate integration checks confirmed Entra browser sign-in, the web managed
identity's SQL reads and writes, conditional approval/command arbitration,
read-only Foundry answers, and a fresh hosted-controller SQL connection.
The command scheduler completed with a decoded `status=completed`, null error
and its narrow project-scoped consumer role. No production monitoring targets
were selected implicitly; an unconfigured deployment shows an empty target
list and disables new investigations.

### Earlier-release screenshot evidence

Personal identity labels are hidden. The complete case list and tool timeline
were expanded only for capture; their recorded data was not changed.

| Evidence | Screenshot |
|---|---|
| Deployed command center and explicit unconfigured-target state | [Command center](./images/command-center/deployed-command-center.png) |
| All 15 cases in the final deployed deterministic UI batch | [Complete scenario list](./images/command-center/deployed-all-scenarios.png) |
| Foundry-backed approved pipeline rerun | [Approved and verified](./images/command-center/deployed-foundry-pipeline-approved.png) |
| Foundry-backed rerun that must remain unresolved | [Pending verification](./images/command-center/deployed-foundry-pipeline-pending.png) |
| Controller refusal of a second remediation | [Policy refusal](./images/command-center/deployed-foundry-policy-refusal.png) |
| Durable counters, outcome and full tool timeline | [Run timeline](./images/command-center/deployed-run-timeline.png) |
| Tool-free observer using recorded incident evidence | [Read-only question](./images/command-center/deployed-read-only-question.png) |
| Database-item Read without additional data-sharing grants | [Managed identity item access](./images/command-center/managed-identity-item-read.png) |

### Review corrections

| Finding | Correction |
|---|---|
| New work hidden behind terminal history or blocked-target backlog | Filter eligible states and target barriers before applying limits |
| Command acquisition could exhaust its execution deadline | Check the deadline again after command claim, before dispatch |
| Lost acknowledgement could allow another target command | Persist interruption and require audited reconciliation; never auto-requeue |
| Optional Teams delivery could spend the web approval window | Notify concurrently without blocking decision polling |
| SQL work inside async web handlers could block approvals | Offload database work, including validation history, to worker threads |
| Fresh sandbox uptime entered a cooldown that never started | Use a nullable last-failure timestamp and test clock-zero boundaries |
| HTTP 200 or CLI exit 0 concealed a failed agent response | Validate terminal response status and error; decode Logic Apps binary envelopes |
| Earlier private App Service deployment ignored legacy routing configuration | Set site-level all-traffic VNet routing and verify actual ARM state; historical, not a public-template prerequisite |
| Earlier private redeployment could detach a governance-added NSG | Preserve existing subnet NSG bindings in that topology; the public template adds no subnets |
