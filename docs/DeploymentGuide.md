# Deployment guide

Provisioning, identity and release requirements for the live paths. Start with
[AzureAccountSetUp.md](AzureAccountSetUp.md); the default local scenario path
remains fully offline.

The command center is an independent **Python 3.13 App Service API and Vite UI**.
The Foundry hosted controller remains a separate deployment. Both use a
standalone Fabric SQL Database. The read-only Rayfin cockpit is another client,
not the owner of that database and not the command center's authorization layer.

Use [CommandCenter.md](CommandCenter.md) for the web workflow and
[PipelineTriage.md](PipelineTriage.md) for scheduled Fabric pipeline monitoring.
Web approvals and notifications do not require Teams, a mailbox or the legacy
approval callback.

## 0. Establish the operator context

All names and identifiers below are placeholders or synthetic examples. Keep
filled-in settings, deployment evidence and tokens out of the repository.

```powershell
$subscription = "<subscription-name-or-id>"
$tenantId = "<tenant-id>"
az account set --subscription $subscription
az account show --subscription $subscription --query "{subscription:id,tenant:tenantId}" -o json

.\.venv\Scripts\bi-triage.exe preflight
```

Confirm the reported tenant before continuing. Reassert the subscription before
each live sequence and after reauthentication: other shells can change shared
CLI state. Fabric has no subscription argument; the token selects the tenant.
An unfamiliar workspace list can be a valid response from the wrong tenant.
`DefaultAzureCredential` can use the same CLI identity for local SQL commands.

If the CLI token cache is unavailable, an explicitly tenant-pinned operator
token, for example from `azureauth`, can authorize appropriate REST calls.
That does not authenticate CLI-backed registration/deployment scripts or change
the identity used by `DefaultAzureCredential`. Use an approved interactive
operator flow, never a copied token file or a new client secret as a shortcut.

Plain `preflight` checks configuration without network access. A `configured`
SQL result is not proof of connectivity; use `preflight --check-sql` explicitly
when a live connection is intended.

## 1. Power BI workload identity

The identity that executes a refresh must be accepted by Power BI. In the
hosted path this is the controller's identity, not a prompt agent or the browser
user. For Azure-hosted adapters, prefer managed identity or supported workload
federation. Do not provision a client secret for this deployment.

A managed identity can be added to a Fabric/Power BI workspace as a principal.
Assign the required role explicitly; do not assume a workspace identity has
an automatic Contributor grant. The local live clients deliberately exclude
human CLI credentials from their workload credential chains, so `az login`
alone does not turn a laptop into the hosted controller.

### Tenant setting and workspace access

Have a tenant administrator enable the applicable service-principal API
setting, scoped to a security group containing the acting principal. Power BI
documentation calls this **Allow service principals to use Power BI APIs**;
Fabric also exposes **Service principals can call Fabric public APIs**.

Assign the narrowest supported workload access and verify the intended
operation. Member was used for refresh evaluation; an earlier hosted test used
Admin. Neither observation makes Admin a required default. Contributor carries
Build for the semantic-model probes described in section 7b. The web UAMI does
not need these workload grants merely to queue an investigation.

Verify target IDs, a read of refresh history and an explicitly approved test
operation as the actual runtime identity. A successful delegated operator read
does not prove the controller can act. Do not diagnose solely from the HTTP
status: tenant restrictions, identity type, workspace access and model
configuration can all produce 401/403, and some missing permissions appear as
404. See
[Power BI service-principal access](https://learn.microsoft.com/power-bi/developer/embedded/embed-service-principal).

## 2. Optional mailbox ingestion

Skip this section for the Teams-independent command-center path. Do not enable
a mailbox schedule until its identity, mailbox scope and filter have been
verified.

An unattended mailbox reader needs app-only access, not a signed-in user's
delegated token. The older hosted implementation observed the same Entra agent
token returning 200 for Graph directory access and 401 for the Exchange-backed
mail endpoint, despite a `Mail.Read` role and a scoped mailbox policy. That is
an observed compatibility limit, not proof that every Graph API accepts every
agent identity.

The source still exposes `GRAPH_CLIENT_SECRET` and a conventional
client-secret fallback in `azure.yaml` and the inbox client. **That legacy
path is not part of the secretless deployment.** Do not fill it in to make
preflight green. Use a separately verified, supported secretless mail identity
or leave mailbox ingestion off. Expiring secrets, including tenant automation
that removes them, would otherwise stop an unattended trigger after deployment.

### Scope the reader to the alerts mailbox

An unscoped Entra application `Mail.Read` grant can read every mailbox.
Successful access to one mailbox does not prove isolation.

For an existing Application Access Policy deployment, the Exchange operator's
verification commands are:

```powershell
New-ApplicationAccessPolicy -AppId <mail-reader-client-id> `
  -PolicyScopeGroupId <mail-enabled-security-group> `
  -AccessRight RestrictAccess `
  -Description "BI triage alerts mailbox only"

Test-ApplicationAccessPolicy -Identity bi-alerts@contoso.com `
  -AppId <mail-reader-client-id>
```

The policy scope is a mail-enabled security group containing the permitted
mailboxes, not an arbitrary mailbox address. This Exchange scope group is
separate from the command center's four ordinary app-role groups.

For new integrations, review
[Exchange RBAC for Applications](https://learn.microsoft.com/exchange/permissions-exo/application-rbac),
which replaces Application Access Policies. Exchange RBAC and existing
unscoped Entra grants are independent/additive; a narrow RBAC assignment does
not cancel a broad grant. Revalidate the application's actual mailbox requests
when adopting a different permission model rather than assuming the current
scope checker covers it.

Set `GRAPH_CANARY_MAILBOX` to a distinct existing mailbox that the reader must
not access. The application requires the canary read to return **403**.
Missing configuration, a failed check, 401/404, or a successful canary read
does not prove confinement and is refused. The hosted path also refuses a
token carrying `upn`; a delegated operator token is not unattended proof.

The Outlook/Office 365 connector catalog is not used by this code. Catalog
availability and per-user OAuth consent are not substitutes for the verified
app-only mailbox path.

## 3. Fabric SQL Database

Incidents, processed messages, approvals, deferred retries, claims, leases,
baselines, run history, commands and incident collaboration must survive
invocations. A restarted controller that forgets an open incident can remediate
the same failure twice.

Create or use a standalone **Fabric SQL Database** in a workspace on a
capacity. Read the item's connection properties in Fabric or through
`GET /v1/workspaces/<workspace-id>/sqlDatabases`. Configure the exact hostname
and catalog name:

```text
FABRIC_SQL_SERVER=<serverFqdn>
FABRIC_SQL_DATABASE=<databaseName>
```

The command-center deployment helper requires a hostname without protocol or
port. Copy the database name exactly, including any item identifier. These
settings are not a username/password connection string.

With both settings empty, local runs use JSON state under `runs/`. That is
appropriate offline, not in a recycling hosted container. The live web API
requires SQL and does not substitute an empty local permission or history store.
Fabric SQL accepts Entra authentication; there is no SQL-authentication fallback.
See [Fabric SQL authentication](https://learn.microsoft.com/fabric/database/sql/authentication).

### Schema and ownership

The current schema has these default tables:

| Purpose | Tables |
|---|---|
| Incident and mailbox state | `triage_incidents`, `triage_processed_messages`, `triage_inbox_audit` |
| Approval and retry state | `triage_approvals`, `triage_deferred_retries`, `triage_pipeline_reruns` |
| Detector and concurrency state | `triage_semantic_health`, `triage_sweep_leases`, `triage_claims` |
| Command-center history and queue | `triage_agent_runs`, `triage_agent_events`, `triage_agent_commands` |
| Append-only collaboration | `triage_incident_activity` |

Use the corresponding `*_TABLE_NAME` settings consistently on both deployments
when sharing a database. `schema_statements()` in
`src\triage\store\fabric_sql.py` includes the command/history and incident
activity schema and the legacy approval procedure. It contains no application
membership permission table.

The controller's store initialization can install missing schema when its
identity has DDL rights. The web history and incident workflow stores do **not**
install schema at runtime; apply the reviewed statements as a schema operator
before deploying web code. Do not grant the web identity `db_ddladmin` to hide
a missed deployment step.

Preserve the database independently of the web applications. An Entra
authorization cutover or code release must not reset incidents, processed
messages, approvals, retries, claims, leases, baselines, rerun journals, run
history, commands or incident activity.

### Fabric access and SQL users

The principal needs Fabric access to the database item and the SQL permissions
appropriate to its work. For the object-scoped web identity, use **Read item**
access, not a broad workspace role or a blanket ReadData grant that bypasses
the intended SQL boundary. Workspace roles may imply broader database rights.

The existing controller provisioning pattern uses workspace Contributor
(principal type `ServicePrincipal`) and a database user with
`db_datareader`, `db_datawriter` and `db_ddladmin` to support its automatic schema
installation. Those broad controller grants are not the web identity's grant
set and are not a production least-privilege claim.

`CREATE USER ... FROM EXTERNAL PROVIDER` resolves the identity through Entra;
directory permissions or Conditional Access can prevent that lookup. For an
operator-verified identity, the explicit SID form avoids the lookup:

```powershell
$sid = '0x' + (([guid]::Parse('<runtime-client-id>').ToByteArray() |
  ForEach-Object { $_.ToString('X2') }) -join '')
```

```sql
CREATE USER [<runtime-sql-user>] WITH SID = 0x<verified-sid-bytes>, TYPE = E;
```

For a service principal or UAMI, use its **client ID**, converted to
little-endian GUID bytes, not its directory object ID. For a user/group,
Fabric SQL uses the object ID; groups use `TYPE = X`. The SID form does not
validate the name, so independently verify the target identity before creating
the user. Do not generalize an observed equality of agent object/client IDs
to ordinary service principals.

### Web UAMI object permissions

After the schema exists, the current web operations require these grants on
the configured objects. The names below are the defaults:

```sql
GRANT SELECT ON OBJECT::dbo.triage_incidents TO [<web-sql-user>];
GRANT SELECT ON OBJECT::dbo.triage_pipeline_reruns TO [<web-sql-user>];
GRANT SELECT, UPDATE ON OBJECT::dbo.triage_approvals TO [<web-sql-user>];
GRANT SELECT, INSERT, UPDATE ON OBJECT::dbo.triage_agent_runs TO [<web-sql-user>];
GRANT SELECT, INSERT ON OBJECT::dbo.triage_agent_events TO [<web-sql-user>];
GRANT SELECT, INSERT, UPDATE ON OBJECT::dbo.triage_agent_commands TO [<web-sql-user>];
GRANT SELECT, INSERT ON OBJECT::dbo.triage_incident_activity TO [<web-sql-user>];
```

Run/history writes include read-only observer requests and isolated validation
results. Command updates include expiry/reconciliation, so the queue is not
SELECT/INSERT-only. Human closure appends activity; it does **not** UPDATE the
core incident. Grant no permission-table rights, core-incident writes,
`db_datawriter`, `db_owner` or runtime DDL to the web UAMI. Check inherited
workspace/database roles as well as explicit grants.

### Verify persistence without clearing shared state

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[azure]"
az account set --subscription $subscription
.\.venv\Scripts\bi-triage.exe preflight --check-sql
.\.venv\Scripts\bi-triage.exe incidents
```

`--check-sql` opens a real connection and runs `SELECT 1`; it does not prove
every object grant or runtime identity. Read back an explicitly authorized
runtime-written record and retain permission/denial evidence separately.

Scenarios normally reset their incident store. With Fabric SQL configured,
`run` refuses that shared reset unless you explicitly choose
`--keep-incidents` or `--reset-shared-state`. Prefer isolated mock state for
validation; `--keep-incidents` preserves history but does not make a live
scenario side-effect-free. Never use `bi-triage reset` as deployment preflight.

The SQL driver uses per-thread connections and atomic conditional statements.
One shared connection failed under concurrent use; server-side `rowcount`
names the claim/lease winner without a read-then-write race. Connection failure
causes a reconnect on later use, with a 30-second unreachable-backend cooldown.
Stores reload after recovery: reconnecting with an empty cache would still
answer "no open incident" incorrectly. Command/history and collaboration
writes fail explicitly rather than reporting a local fallback as durable.

## 3b. Monitored mailbox and filter

Use a shared or licensed alerts mailbox, for example `bi-alerts@contoso.com`.
The reader does not mark messages as read; deduplication uses message IDs in
the processed-message store. Check that another process is not moving or
consuming alerts before this reader sees them.

`GRAPH_SENDER_ALLOWLIST` and `GRAPH_SUBJECT_PATTERN` are security controls,
not presentation filters. Invalid patterns fail closed. Never broaden either
to make a test message trigger; send a synthetic message that matches the
approved configuration. The ignored-message audit records why mail was
refused without weakening the filter if that audit fails.

Only polling is implemented. `GRAPH_INGESTION_MODE=subscription` is rejected,
not silently treated as push delivery. `GRAPH_POLL_SECONDS` defaults to 30 for
the local watch loop; a hosted schedule has its own cadence.

## 4. Foundry project and prompt registration

Provide a project endpoint and a model deployment available in that project.
Set `FOUNDRY_AGENT_MODEL` to the deployment name, not an assumed global model
alias. Model catalog availability is not quota or admission proof.

If the operator must create a project, the CLI requires a location even when
the parent account already has one:

```powershell
az account set --subscription $subscription
az cognitiveservices account project create `
  -n <account> -g <resource-group> --project-name <project> -l <region>
```

Review the account's Entra-only authentication and managed network isolation
before use. Creating a project does not establish private connectivity for
every dependent service.

### Foundry roles

Use **Foundry User** for agent development, **Foundry Project Manager** when
project management is needed, and **Foundry Agent Consumer** for invoke-only
callers. The project's own managed identity also needs its Foundry role.
Subscription Owner alone does not supply Foundry data-plane permissions.

The portal can assign Foundry User to the creator and project identity if the
creator may assign roles. CLI/IaC creation must not assume those assignments.
Missing access has appeared as:

```text
403 ... does not have permissions for
Microsoft.CognitiveServices/accounts/AIServices/agents/read
```

Do not use a `Cognitive Services` role or `Azure AI Developer` as a substitute
for Foundry project access. The Foundry User, Owner, Account Owner and Project
Manager roles were previously named Azure AI User, Owner, Account Owner and
Project Manager; their role IDs did not change with the rename. See
[Foundry RBAC](https://learn.microsoft.com/azure/foundry/concepts/rbac-foundry).

### Register the definitions

```powershell
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py --dry-run
az account set --subscription $subscription
.\.venv\Scripts\python.exe scripts\register_foundry_agents.py
```

The definitions include `bi-triage`, `bi-data-quality` and the tool-free
`bi-triage-observer`. **Re-register after a prompt or tool change**: a deployed
prompt agent does not load local files. The script compares normalized
definitions and creates a new version only when needed. Remote null fields
are normalized so they do not cause false version churn.

Registration and code deployment are separate operations. The triage and data
quality agents reason; the observer explains recorded evidence. None needs
Power BI, Fabric SQL or directory administration grants.

## 5. Power BI workspace and sample model

Choose a configured workspace and semantic model that the acting controller
may access. A report is optional. For a synthetic evaluation, seed an isolated
model from `mock\data\daily_sales.csv` or `daily_sales_clean.csv`, depending
on the scenario.

Do not replace an existing model's data or repoint a production workload merely
to run a scenario. The command center accepts server-configured targets, not
arbitrary workspace/model IDs supplied by the browser.

## 6. Optional Teams notifications

For the secretless web path, set `NOTIFICATION_CHANNEL=web` and
`APPROVAL_DELIVERY_MODE=web` on the controller. Teams delivery is optional and
its absence must not prevent a web decision.

The legacy notifier uses a Power Automate Workflows webhook carrying an
Adaptive Card envelope. It does not provision an identity-authenticated Teams
bot. Do not use the retired Office 365 Incoming Webhook connector as a new
deployment dependency; confirm current availability and supported migration
with [Teams webhook guidance](https://learn.microsoft.com/microsoftteams/platform/webhooks-and-connectors/what-are-webhooks-and-connectors).

A workflow URL is a bearer credential. Do not create or publish one as part of
this secretless deployment. If maintaining an already approved legacy
integration, protect the URL, verify the tenant before creating or editing the
workflow, and never print the value for confirmation. Browser SSO can select a
different organization while the workflow appears to have been created
successfully. Workflows posts use the Workflows bot; custom connector names,
icons and interactive MessageCard buttons do not carry over.

App-only Graph channel posting is not a general replacement: ordinary channel
message application access is restricted to migration scenarios. A separately
designed bot or delegated flow needs its own review.

## 6b. Legacy approval callback

The command center does not need `infra\approval-callback.json`. Its web API
records decisions as the validated Entra actor, and the shared approval gate
still checks fingerprint, expiry and single use.

For operators maintaining the older callback, preserve these constraints:

| Component | Method and boundary |
|---|---|
| Confirmation workflow | GET renders confirmation only; it has no SQL connection or write action. |
| Recording workflow | POST calls `dbo.triage_record_approval_decision` through the SQL managed connector and its managed identity. |

The split prevents link previewers, scanners and prefetchers from approving
on GET. Request triggers accept one method; an undeclared method defaults to
POST and rejects GET with `TriggerRequestMethodNotValid`. An incoming-webhook
card has no bot to handle `Action.Submit`, so the legacy card uses
`Action.OpenUrl` to the confirmation workflow.

The recording identity needs only the necessary Fabric item access and
`EXECUTE` on the decision procedure, not incident read/write permissions.
The SQL connector's `oauthMI` parameter set uses managed identity; the callback
URL itself is still a bearer credential. Recheck the identity after workflow
recreation, which can change its SID. Do not drop/recreate a database user
blindly on every ordinary code deployment.

All decision fields are procedure parameters, never concatenated SQL. A
conditional update and `@@ROWCOUNT` distinguish a recorded decision from a
refusal. The procedure refuses web-delivery proposals. Its link's responder
text is not an authenticated identity; do not use it to bypass web authorization.

An answer arriving after the waiting run has ended is not automatically
resumed. `APPROVAL_TIMEOUT_SECONDS` defaults to 300. The waiting gate still
rejects missing, malformed, expired, reused or mismatched approval. For an
approved legacy configuration without a callback, the card displays a request
ID for the operator CLI; the web path does not rely on that fallback.

## 6c. Scheduled sweeps

Nothing runs on a timer merely because a monitor setting is enabled.
Foundry routines in `azure.yaml` ship disabled after the preview produced no
runs despite reporting an enabled schedule. Keep that historical limitation
qualified and reverify in the target tenant before using routines; see
[foundry/README.md](foundry/README.md#foundry-routine-observations).

`infra\scheduled-sweep.json` is the separate Consumption Logic App scheduler.
Its system-assigned identity invokes the hosted controller with one explicit
command. Choose each job and cadence independently:

| Command | Purpose | Example cadence |
|---|---|---|
| `sweep` | Mailbox drain and due retries; only after mailbox scope/filter checks | Every five minutes |
| `silent sweep` | Configured semantic-health probes | Hourly |
| `pipeline sweep` | Failed scheduled runs of configured Fabric pipelines and correlated rerun verification | Every five minutes |
| `command sweep` | Durable authenticated operator investigations | Every minute |

For example, after its prerequisites are ready:

```powershell
az account set --subscription $subscription
az deployment group create -g <resource-group> -n sched-commands `
  --template-file infra\scheduled-sweep.json `
  --parameters name=bi-triage-command-sweep `
    projectEndpoint="<project-endpoint>" command="command sweep" `
    frequency=Minute interval=1 owner="<owner>" costCenter="<cost-center>" `
    environment="evaluation"

az role assignment create --assignee-object-id <scheduler-principal-id> `
  --assignee-principal-type ServicePrincipal `
  --role "Foundry Agent Consumer" --scope <project-resource-id>
```

The template creates the workflow **enabled**; it has no disabled-state
parameter. Do not deploy a mailbox schedule before its prerequisites are ready.
Until the invoke grant propagates, runs may fail with 403. The project-scoped
grant permits that project's agent endpoints, not just the named controller;
use a narrower supported scope when required.

The caller timeout is `PT15M`, covering the default combined triage and
approval budgets plus worker allowance (`300 + 300 + 30` seconds). `PT10M`
covered the approval window alone but not the combined deadline. A shorter
caller timeout can abandon work while a person's approval is still valid.
HTTP invocation retries are disabled: an ambiguous POST must not replay a
possibly executed action.

The workflow records failed invocations/invalid responses as `SweepFailed`.
Its run history is not an alerting channel. The optional `alertWebhookUrl`
is a legacy bearer-URL integration; use an approved alerting route without
making it a prerequisite for the web deployment. Review both schedule history
and durable triage outcomes: a transport-completed response is not necessarily
a healthy business result.

Pipeline monitoring additionally requires `PIPELINE_SWEEP_ENABLED=true`,
explicit `FABRIC_PIPELINE_TARGETS` and shared SQL state. It neither discovers
arbitrary targets nor monitors standalone notebook jobs. A notebook activity
inside a configured pipeline may provide evidence. A full-pipeline rerun needs
reviewed replay safety/parameters, explicit approval and a durable reservation;
a submitted job is not verified recovery. See [PipelineTriage.md](PipelineTriage.md).

## 7. Data quality flags

The default flag table is `runs\dq_flags.csv`. It is reproducible local output,
not a durable hosted database. A Fabric/SQL implementation must provide the
same `read_all`, `append` and `reset` contract and retain store-boundary
redaction. Do not present a container-local CSV as durable evidence.

## 7b. Silent-failure detector

`SILENT_SWEEP_ENABLED` alone watches nothing. Configure
`SILENT_HEALTH_PROBES` explicitly because freshness expectations are business
rules, not model guesses:

```json
[{
  "name": "sales-invoices",
  "workspace_id": "<workspace-id>",
  "dataset_id": "<dataset-id>",
  "table": "fact_sales_invoice",
  "date_table": "dim_date",
  "date_column": "date",
  "report_name": "Sales invoices",
  "expected_lag_hours": 24,
  "min_absolute_drop": 40,
  "watch_schema": true,
  "load_weekdays": [1, 2, 3, 4, 5]
}]
```

```powershell
.\.venv\Scripts\bi-triage.exe health --preflight
.\.venv\Scripts\bi-triage.exe health --probes
```

`--preflight` rejects missing IDs, duplicate names for the same model and
invalid confirmation thresholds. Duplicate names overwrite one baseline;
`confirmations: 0` would disable the false-positive guard.

Use `date_table` when the fact table contains a date key. Reading the calendar
dimension alone can return a future date rather than the latest loaded fact:
the synthetic failure example has a calendar ending `2030-12-31` while facts
stop at `2024-12-23`. Check `min_absolute_drop` against table size: its default
1,000 cannot detect a drop in a 400-row table. Use `load_weekdays` for feeds
that do not load on weekends.

Two scans are required by default (`confirmations=2`). The first records
suspicion; it does not announce a confirmed finding. Probes run sequentially
with `SILENT_PROBE_PACE_SECONDS=1`, and one durable sweep lease prevents
parallel sweeps from counting the same observation twice.

### Schema checks and platform limits

`watch_schema=true` adds a query for visible columns and measures; removals
matter, additions are ignored. The implementation uses DAX `INFO.VIEW` rather
than a separate XMLA client stack. This is a compatibility check to prove on
the actual model: the public execute-queries contract excludes INFO functions.
Do not treat a successful tenant-specific test as universal API support.
An unreadable schema is a detector fault, never evidence that every column
was deleted.

The public
[execute-queries contract](https://learn.microsoft.com/rest/api/power-bi/datasets/execute-queries)
requires read/Build permission, the tenant setting, and allows 120 requests
per minute per user across datasets. It excludes service principals for
SSO-enabled and RLS models. An observed Direct Lake SSO failure returned
`401 PowerBINotAuthorizedException`; granting broader workspace rights did
not fix it. Review a supported fixed-identity connection or an Import model
where appropriate, and verify before enabling probes. Do not generalize that
failure to every Direct Lake configuration.

Contributor was the evaluated workspace role carrying Build for an app-only
probe. Viewer plus a per-dataset Build grant was not usable through the tested
dataset-users API, which rejected `principalType: App` with
`API supported only for User or Group principal types`. Verify the current
supported permission route rather than escalating to Admin.

Missing workspace access can appear as `PowerBIEntityNotFound` (404), while
tenant settings and unsupported SSO can both appear as
`PowerBINotAuthorizedException` (401). A failed measurement is not healthy data.

### Baseline changes and repeated faults

```powershell
.\.venv\Scripts\bi-triage.exe health --baselines
.\.venv\Scripts\bi-triage.exe health --accept sales-invoices
```

`--accept` is an explicit operational write: it clears suspicion for the named
planned change and lets the next scan establish the new baseline. `--accept all`
affects every probe; it is not deployment preflight. Do not reset unrelated
baselines to acknowledge one intentional change.

After five consecutive faults by default, a probe is parked for 60 minutes.
Time permits a new attempt; one successful reading clears the error streak.
This bounds repeated load from a persistent permission or unsupported-model
failure without requiring a redeploy after the cause is fixed.

## 8. Observability

Spans carry metadata only, never prompt/completion content. Container logs,
durable run/events, the authenticated API and scheduler history provide
different evidence; none alone proves all dependencies are healthy.

The existing `configure_telemetry` helper is optional and consumes
`APPLICATIONINSIGHTS_CONNECTION_STRING`; without it telemetry is a no-op.
It does not explicitly pass an Entra credential to the exporter. The
command-center deployment helper rejects connection-string settings, and its
`-ApplicationInsightsResourceId` only adds a portal link. **Neither setting
proves secretless telemetry is configured.** An Entra-authorized exporter and
private ingestion path need separate implementation/configuration and proof;
do not generate a key or claim the portal link does this.

When Entra-authenticated telemetry is configured, grant only the needed
monitoring role, such as Monitoring Metrics Publisher, to the exporting
identity. Preserve governance-created diagnostic settings; add separate
diagnostics rather than deleting them. Do not disable content filters or
Defender to make an evaluation succeed.

Use `azd ai agent monitor bi-triage-controller --tail 300` for hosted
diagnostics. The default 50 lines can hide the error behind SDK output; 300 is
the observed CLI maximum. Do not infer log delivery from a few visible spans.
Keep the hosting library pinned to the exact checked-in version: floating
date-stamped betas previously broke container startup.

## 9. Foundry hosted controller deployment

The Foundry project, model and standalone database already exist. Deploying
the controller neither deploys the App Service UI nor creates a working timer.
Use the existing azd environment for updates; create a new one only for a
separate deployment.

```powershell
az account set --subscription $subscription
azd config set auth.useAzCliAuth true
azd env set AZURE_SUBSCRIPTION_ID "<subscription-id>"
azd env set AZURE_TENANT_ID "<tenant-id>"
azd env set AZURE_LOCATION "<region>"
azd env set AZURE_AI_PROJECT_ENDPOINT "<project-endpoint>"
azd env set AZURE_AI_PROJECT_ID "<project-resource-id>"
azd env set FOUNDRY_PROJECT_ENDPOINT "<project-endpoint>"
azd env set FOUNDRY_AGENT_MODEL "<model-deployment>"
azd env set FABRIC_SQL_SERVER "<serverFqdn>"
azd env set FABRIC_SQL_DATABASE "<databaseName>"
azd env set RUN_HISTORY_ENABLED true
azd env set APPROVAL_DELIVERY_MODE web
azd env set NOTIFICATION_CHANNEL web
azd env set COMMAND_CENTER_URL "https://<app-name>.azurewebsites.net"
azd deploy bi-triage-controller --no-prompt
```

Set both project-endpoint variables. Setting only
`AZURE_AI_PROJECT_ENDPOINT` previously failed dependency resolution with
`Foundry dependencies are not ready: FOUNDRY_PROJECT_ENDPOINT is not set`.
Running an unrelated provision operation does not fix a missing setting.
Keep optional legacy credential variables unconfigured; review the effective
environment rather than treating all historical `azure.yaml` entries as
required integrations.

Inspect `instance_identity` on the actual hosted agent definition after
creation/recreation. `bi-triage identity --check-scope` reads the directory;
when that operator query is unavailable, the Foundry definition supplies
`instance_identity` without a Graph lookup. New identities start without grants;
ordinary updates are not a reason to assume the identity always changes.

| Actor/scope | Permission and reason |
|---|---|
| Hosted controller / Fabric | Item/workspace access and SQL rights for durable state; see section 3 |
| Hosted controller / Power BI or configured pipeline | Only the workload permissions needed by its tools |
| Hosted controller / Foundry project | Foundry Agent Consumer to invoke its reasoning agents |
| Web UAMI / Foundry project | Foundry Agent Consumer if model-backed observer or permitted model validation is enabled |
| Scheduler / Foundry project | Foundry Agent Consumer to invoke the hosted controller |
| Prompt agents | No Power BI, SQL, mailbox or directory grants |

Verify a harmless configured read before an approved live action. A
container-written row read back through an independent authorized connection
proves more than a local scenario does. Do not issue `sweep` as a generic
health probe: it drains mail and due retries. `command sweep` executes queued
work, and pipeline/silent sweeps also have operational effects.

`azd deploy` with no effective change can finish without restarting the
container. Verify the deployed version and observed restart rather than
assuming a successful deploy cleared process state. See
[the hosted architecture](foundry/README.md) for concurrency and routine limits.

## 10. Command-center Entra authorization

The API validates RS256 v2 Entra access tokens for the configured tenant and
API client ID, including signature, issuer, audience, tenant, required
timestamps and `oid`. It requires delegated `access_as_user` plus recognized
app-role claims. An ID token, an app-only token, a SQL grant or a client-supplied
actor header is not an alternative.

| App role | Capability |
|---|---|
| `CommandCenter.Reader` | Read authorized records, health/history and ask tool-free questions |
| `CommandCenter.Operator` | Reader capabilities, configured investigations, notes and human tracking resolution |
| `CommandCenter.Approver` | Reader capabilities and explicit pending approval/denial decisions |
| `CommandCenter.Admin` | All app capabilities, isolated validation and reconciliation; no directory administration |

Operator and Approver do not imply each other. App roles do not grant the
person Azure/Fabric service permissions.

### Register or update the SPA/API

Run the offline plan first:

```powershell
.\.venv\Scripts\python.exe scripts\register_command_center.py `
  --subscription "<subscription-name-or-id>" --tenant-id "<tenant-id>" `
  --display-name "Example Triage Command Center" `
  --webapp-origin "https://<app-name>.azurewebsites.net" --dry-run
```

Remove `--dry-run` only for an authorized registration operation.
For an existing application, supply `--application-id "<application-client-id>"`.
The script preserves role/scope IDs, rejects ambiguous or unmarked same-name
applications unless explicitly identified, and refuses registrations with
existing passwords/certificates rather than deleting them. It configures
single-tenant authorization code + PKCE, disables implicit grants and enforces
`appRoleAssignmentRequired=true` on the enterprise application.

Optional flags are distinct choices:

| Flag | Effect |
|---|---|
| `--localhost-redirect-uri` | Adds an explicit loopback development redirect |
| `--admin-current-user` or `--admin-user-object-id` | Adds a direct Admin assignment as an optional bootstrap |
| `--authorize-azure-cli` | Preauthorizes Microsoft's CLI client for this API's scope only |
| `--grant-admin-consent` | Grants `access_as_user` for the selected administrator, for the SPA and opted-in CLI client; not `AllPrincipals` |
| `--grant-profile-consent` | Grants delegated Graph `User.Read` for only that selected user and SPA; not a Graph application permission |

Consent flags require a selected administrator and separately authorized
directory/consent privileges. The selection flags also assign that user's
direct Admin bootstrap role; do not use them for routine end-user onboarding.
Assignment-required apps need administrator consent; selected-user grants do
not consent future group members. IT must arrange their approved consent
process. Remove evaluation CLI access when no longer needed.

### Provision the four groups

```powershell
.\.venv\Scripts\python.exe scripts\configure_command_center_groups.py `
  --subscription "<subscription-name-or-id>" --tenant-id "<tenant-id>" `
  --app-id "<application-client-id>" --group-prefix "Example Triage" `
  --admin-current-user
```

This defaults to an **offline plan**, unlike registration without `--dry-run`.
Add `--apply` only for privileged operator provisioning. It checks the enabled
single-tenant enterprise application, role definitions, administrator and
active P1/P2 plan. It creates or reconciles four ordinary security groups,
rejects name/ownership-marker collisions and role-assignable groups, assigns
the roles, and reads back owners, the administrator's direct Admin-group
membership and every app-role assignment.

The selected administrator owns each group and is added as a member of the
Administrators group. Ownership alone is not app access. The script does
**not** remove existing direct assignments, delete groups or change operational
SQL data. A delayed/ambiguous directory write must be reconciled before retrying.

Entra ID P1/P2 is required for group-based assignment. Nested group membership
does not cascade. IT manages app assignments; group owners manage membership
externally. The application has no roster or membership editor and needs no
`AppRoleAssignment.ReadWrite.All` or `Group.ReadWrite.All` on its UAMI. See
[Entra group assignment](https://learn.microsoft.com/entra/identity/enterprise-apps/assign-user-or-group-access-portal).

### Existing-deployment cutover

Do not treat the removal of the SQL ACL implementation as a data migration or
silently disable an active old authority during provisioning.

1. Inventory the exact existing app, role IDs, direct assignments, group
   assignments, old flag and operational tables. Preserve existing records and
   an operator-controlled copy of any legacy access evidence.
2. Establish the Entra app roles, administrator group, direct group membership
   and group role assignment. Before disabling the old flag, prove a freshly
   issued, cryptographically validated API token contains Entra Admin for the
   correct tenant/audience/scope. An old SQL-authorized Admin display is not
   this proof. Retain a reviewed bootstrap path until cutover is confirmed.
3. Explicitly set the old live app's
   `COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED` to false after that proof.
   The deployment helper refuses both a requested true value and an existing
   true value; it does not perform the authority switch for you. New code
   rejects `access_management_enabled=true` at startup.
4. Deploy the Entra-only code. Verify `/api/access` reports
   `source=entra_app_roles`, Admin operations work for the intended actor and
   lower-role operations remain refused. Preserve all operational state.
5. Only after successful group setup/readback, remove the specifically reviewed
   direct bootstrap assignment. Obtain another fresh API token and prove Admin
   through the group-based path. Do not remove every direct assignment or
   assume the group script already removed them.
6. Only after fresh-token group access and operational continuity are confirmed,
   retire the separately identified legacy permission table under the approved
   retention process. Do not drop, recreate or clear the standalone database
   or any incident, approval, command, history or collaboration table.

The old SQL access service/store and its initialization script have been
removed. There is no dual SQL/Entra authority to re-enable.

### Access display and human closure

Access & permissions calls `/api/access`: it reports the presented token's
effective roles, tenant/application and issue/expiry dates. It does not read
SQL membership, query Graph directory membership or identify the supplying
group. Refresh permissions forces a new API-token request, then reloads
access and the command-center snapshot. If interaction is required, sign in
explicitly; the refresh does not silently open consent.

Changes depend on Entra propagation and token renewal. Refreshing one session
does not revoke anyone else's already-issued token. The optional browser
profile-photo request uses a separate delegated Graph `User.Read` token.

Human closure is append-only tracking, not verified remediation. It binds to
the original SQL NVARCHAR payload hash (SHA-256 over UTF-16 LE), plus the
tracking version. New evidence invalidates the closure without resetting
controller incidents, budgets, notifications, approvals or claims. This is why
the web UAMI inserts into `triage_incident_activity` but cannot update
`triage_incidents`.

## 11. App Service infrastructure and code releases

`infra\command-center.bicep` provisions only the independent web host and its
network/identity resources. The existing resource group, Foundry services and
Fabric SQL Database are separate prerequisites.

### Infrastructure helper

Inspect the real command help and validate before provisioning:

```powershell
Get-Help .\scripts\deploy_command_center.ps1 -Detailed

.\scripts\deploy_command_center.ps1 `
  -Subscription "<subscription-name-or-id>" -ResourceGroup "<resource-group>" `
  -Location "<region>" -AppName "<app-name>" `
  -TenantId "<tenant-id>" -ApplicationClientId "<application-client-id>" `
  -CostCenter "<cost-center>" -Owner "<owner>" -Environment "evaluation" `
  -DataClassification "synthetic" `
  -ApplicationSettingsFile "<operator-owned-settings.json>" -ValidateOnly
```

Without `-Deploy`, the helper runs ARM validation and resource-ID-only what-if,
not provisioning. The settings file is a JSON object of string values, not an
`.env` file. It requires `FABRIC_SQL_SERVER`, `FABRIC_SQL_DATABASE` and
`FOUNDRY_PROJECT_ENDPOINT`; credentials and managed-setting overrides are
rejected. Do not commit it.

For new infrastructure, use `-Deploy -ProvisionOnly` to create the UAMI/host,
then grant its external permissions and install SQL schema separately.
`-Deploy` without `-ProvisionOnly` also builds and uploads code. This helper
does not request quota, register agents, grant directory/RBAC permissions or
prove private backend connectivity.

Verify the selected App Service SKU's regional quota **and admission**, Python
3.13 availability, Foundry hosting/model capacity and Fabric capacity.
Supported helper SKUs include B1/B2/B3, S1/S2/S3, P0v3/P1v3; do not assume the
default B1 can be provisioned merely because the region lists it.

### Network and governance invariants

The template sets `publicNetworkAccess=Disabled` by default, with app/SCM
private DNS, separate inbound-private-endpoint and outbound-integration
subnets, `defaultOutboundAccess=false` and an explicit NAT Gateway. The NAT
public IP is for outbound SNAT, not an inbound listener.

Read back `properties.outboundVnetRouting.allTraffic=true` from the deployed
site. The earlier inline `vnetRouteAllEnabled` setting returned false after
provisioning; a declared flag is not runtime evidence. Preserve governance
NSG bindings on both subnets; the helper reads existing bindings before
redeployment. See
[App Service routing](https://learn.microsoft.com/azure/app-service/configure-vnet-integration-routing).

Private web ingress does not provide private routes/DNS to Foundry or Fabric.
Configure those separately, including Foundry managed network isolation and
the supported Fabric Private Link scope. Tenant-level Fabric Private Link
supports SQL TDS; review current workspace-level limitations instead of
assuming every item is covered. Fabric-to-source egress is a separate control.
See [Fabric Private Link](https://learn.microsoft.com/fabric/security/security-private-links-overview).

SCM/FTP basic publishing remain disabled. Use Entra-authenticated CLI
(Azure CLI 2.48.1 or later) or Kudu REST, never publishing passwords/profiles.
`/api/health` proves only that the web process answers in live mode. Verify
authenticated API, SQL, observer and command/scheduler paths independently.

The helper's `-TemporaryPublicAccess` requires explicit client `/32` or `/128`
CIDRs, defaults other traffic to Deny and restores `publicNetworkAccess=Disabled`
in `finally`, including after failure. It is not a persistent user-access mode.

An explicitly requested persistent client/Global Secure Access exception must
use verified single-host egress addresses on the **main endpoint** plus Entra
authentication. Keep **SCM independent and deny-all** outside a deployment
window. Do not infer SCM trust from an approved main-site allowlist.

All taggable resources and the resource group need `CostCenter`, `Owner`,
`Environment` and `DataClassification`. NAT, Private Link, the App Service plan,
Fabric capacity and hosted/model use incur separate costs. For a finite
approved evaluation, `-Evaluation` enables admin-only isolated validation;
`-EvaluationCostExemption` adds `CostControl=Ignore` and requires a future
`-EvaluationExpiresOn`. Those tags do not shut anything down. Disable validation
and remove the exemption by the recorded review date.

### Code-only update to an existing app

**Do not run a default full infrastructure deployment for a code-only change.**
It can reset an approved persistent main-site access policy and reintroduce
shared SCM restrictions. Preserve the existing app, UAMI, database, runtime
settings, NSGs, private endpoints and approved ingress.

Build a new artifact outside the repository; do not restore dependencies unless
needed:

```powershell
$package = Join-Path $env:TEMP "triage-command-center-release.zip"
.\scripts\package_command_center.ps1 -OutputPath $package
```

The packager type-checks/builds the UI and includes source, scenarios, mock
inputs and `command-center\dist`, with generated `requirements.txt` containing
`.[web,azure]`. It excludes environment files, credentials, caches and
publishing material and runs the repository credential gate. Use a new output
name or explicit `-Force`; it will not overwrite an existing artifact silently.

Before any ZIP POST, confirm private app **and SCM** DNS/routing from the
deployment host. If a separate temporary SCM window is authorized, capture the
exact current access configuration and restore it in a PowerShell `finally`
block. Preserve approved persistent main-site access; do not replace it with
an unrelated default.

Require repeated successful **Entra-authenticated, read-only** SCM probes,
such as `GET /api/deployments`, before uploading. Restriction propagation can
differ between SCM frontends; one 200 response or an ARM update result is not
stable readiness. If different resolved frontends are used, establish
consistent results with the correct hostname/TLS SNI before the write.
The current deployment helper has a post-upload health probe, not this
pre-upload stability gate; the operator must establish it first.

Once readiness and identity are confirmed:

```powershell
az account set --subscription $subscription
az webapp deploy --subscription $subscription -g <resource-group> -n <app-name> `
  --src-path $package --type zip --clean true --restart true `
  --async false --track-status false --timeout 1800000
```

Control-plane startup tracking previously stalled after Kudu completed and the
API served requests, so inspect Kudu deployment status and the actual
application, not just the CLI wait. An accepted POST, timeout or lost response
can leave a deployment running: inspect its existing deployment ID/status and
reconcile before another upload. Do not blindly retry ambiguous writes.

Restore temporary SCM/network changes in `finally` and verify the readback even
when packaging, upload or readiness fails. Then prove authenticated Entra
roles, operational data continuity and the specific deployed feature. A
successful ZIP upload is not proof of SQL authorization or private backends.

See [deployment without basic authentication](https://learn.microsoft.com/azure/app-service/configure-basic-auth-disable)
and [App Service private endpoints](https://learn.microsoft.com/azure/app-service/overview-private-endpoint).
