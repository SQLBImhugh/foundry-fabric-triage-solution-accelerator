# Provisioning

What must exist in your tenant before the live scenarios will run. Ordered
by lead time, not by importance — **item 1 is the one that blocks everything else.**

Verify progress at any point with:

```powershell
.\.venv\Scripts\bi-triage.exe preflight
```

---

## 1. Power BI service principal access ← start here

Dataset refresh needs a token the *dataset* accepts. Two identity options, and
the choice is not obvious:

| | Service principal | Managed identity |
|---|---|---|
| Callable from | Anywhere | Azure-hosted compute only |
| Secret handling | You rotate it, and it expires | Platform-managed, no secret |
| Workspace membership | Supported | Also supported — add it like any other principal |

A managed identity **can** be added to a Fabric/Power BI workspace via *Manage
access → Add people or groups*. If your orchestrator runs in Azure (Functions,
Container Apps, Automation), prefer it — there is no secret to rotate. Use a
service principal when the caller is outside Azure, which is the case for a local run
driven from a laptop.

Note also that Fabric removed the default Contributor grant for workspace
identities in 2025; assign the role explicitly rather than assuming it.

Either way, two approvals are needed.

### Tenant setting (tenant admin)

Power BI admin portal → Tenant settings → Developer settings →
**Allow service principals to use Power BI APIs** → Enabled, scoped to a
security group containing the principal.

This is the item that has blocked more first runs than everything else combined. It is
a tenant admin action, and until it is on, refresh returns 401 with a message
that does not mention it.

### Workspace membership

Add the principal to the workspace as **Member** (Contributor cannot trigger
refresh on all dataset types; Member avoids the ambiguity).

### Verify

```powershell
$body = @{
  grant_type="client_credentials"; client_id=$env:POWERBI_CLIENT_ID
  client_secret=$env:POWERBI_CLIENT_SECRET
  scope="https://analysis.windows.net/powerbi/api/.default"
}
$tok = (Invoke-RestMethod -Method Post -Body $body `
  "https://login.microsoftonline.com/$env:POWERBI_TENANT_ID/oauth2/v2.0/token").access_token

Invoke-RestMethod -Headers @{Authorization="Bearer $tok"} `
  "https://api.powerbi.com/v1.0/myorg/groups/$env:POWERBI_WORKSPACE_ID/datasets"
```

Datasets listed = both approvals landed. 401 = tenant setting. 403 = workspace
membership.

---

## 2. App registration for Microsoft Graph (inbox trigger)

**Verified working on a work tenant, 2026-08-27** — see
[`foundry/README.md`](foundry-native-architecture.md#trigger--notification--work-tenant-spike-2026-08-27).

- Permission: **`Mail.Read`** — *Application*, not Delegated
- **Admin consent granted** (the agent runs with no signed-in user)
- Identity: prefer a **managed identity or federated credential**. A client
  secret works for evaluation, but it expires and then the trigger silently stops

Application permission is the right choice specifically *because* the agent is
unattended. Delegated permission needs a signed-in user; there isn't one when a
routine fires at 05:00.

### Scope it to one mailbox — do this at the same time

> **`Mail.Read` as an application permission is tenant-wide.** Verified: a spike
> app created to read one mailbox successfully read the global
> administrator's inbox. Unscoped, "an agent that reads the BI alerts inbox" is
> an agent that can read every mailbox in the organisation.

```powershell
# Exchange Online PowerShell
New-ApplicationAccessPolicy -AppId <app-id> `
  -PolicyScopeGroupId bi-alerts@contoso.com `
  -AccessRight RestrictAccess `
  -Description "BI triage: BI alerts mailbox only"

Test-ApplicationAccessPolicy -Identity bi-alerts@contoso.com -AppId <app-id>
```

Treat this as part of the app registration, not a follow-up task. It is the
difference between a scoped integration and a tenant-wide mailbox read.

### Verify

```powershell
$body = @{
  grant_type="client_credentials"; client_id=$env:GRAPH_CLIENT_ID
  client_secret=$env:GRAPH_CLIENT_SECRET; scope="https://graph.microsoft.com/.default"
}
$tok = (Invoke-RestMethod -Method Post -Body $body `
  "https://login.microsoftonline.com/$env:GRAPH_TENANT_ID/oauth2/v2.0/token").access_token

Invoke-RestMethod -Headers @{Authorization="Bearer $tok"} `
  "https://graph.microsoft.com/v1.0/users/$env:GRAPH_MAILBOX/mailFolders/inbox/messages?`$top=1"
```

A successful read with a token carrying `roles: Mail.Read` and **no** `upn`
claim confirms the unattended path.

### Not the Outlook connector

The Foundry catalog exposes `outlook` (consumer/MSA, `oauth2generic`) and
`office365` (work/school, `aadcertificate`). Neither is used here:

- the connector catalog returned no entries in the tenant tested, and
- gateway connectors authenticate by **per-user OAuth consent**, which has no
  meaning for an unattended agent.

---

## 3. Monitored mailbox

A shared mailbox or licensed user, e.g. `bi-alerts@<tenant>.onmicrosoft.com`.

The accelerator **does not mark messages as read** — runs must be repeatable, and
dedup is by message id held in memory. Confirm nothing else is auto-processing
the mailbox.

Send the sample emails from `mock/emails/` into it ahead of time, or send them
live during the session for effect. Live is better; have the pre-sent ones as
backup.

---

## 4. Azure AI Foundry project

- A Foundry project; note the endpoint
- A model deployment (`gpt-4o` or similar) — set `FOUNDRY_AGENT_MODEL`
- Your identity needs project-level rights to create agent versions

```powershell
az login
python scripts\register_foundry_agents.py --dry-run
python scripts\register_foundry_agents.py --handoff client
```

**Re-register after any prompt or tool change.** A Foundry-registered agent does
not pick up local edits — it keeps running the previous definition, silently. Put
this on the pre-flight checklist, not in someone's memory.

---

## 5. Power BI workspace + semantic model

- A workspace, with the SP as Member (item 1)
- A semantic model over the well production table, so the report is real
- A report — optional for the flow, but it makes the failure concrete on screen

Seed the model from `mock/data/daily_sales.csv` (with duplicates) or
`daily_sales_clean.csv`, depending on which scenario you are staging.

---

## 6. Teams channel + notification path

> **Do not use "Channel → Connectors → Incoming Webhook".** Office 365
> connectors, including Teams Incoming Webhooks, were **retired on 22 May 2026**
> and no longer deliver. Any guide that still describes that flow predates the
> retirement.

**Use the Power Automate Workflows webhook.** In the channel: **⋯ → Workflows →
"Post to a channel when a webhook request is received"**. Complete the template
and copy the generated HTTP POST URL into `TEAMS_WEBHOOK_URL`.

**On automating this — don't, without checking which tenant you land in.** It
was tested rather than assumed. A browser automation driving Edge reaches Power
Automate fully signed in with no credentials, because Windows SSO applies, but
it signs in to the **corporate** tenant rather than the one you are deploying
to. A webhook created that way lands in the wrong place and still looks like it
worked. Create it by hand, or verify the tenant before trusting the result.

The URL is a bearer credential: anyone holding it can post to that channel. Keep
it in the azd environment, which is gitignored, and never in the repository. If
you echo it for confirmation, print a fingerprint (length and last six
characters) rather than the value, so it does not end up in a terminal buffer or
a screen share.

The payload shape is standard — the notifier posts an Adaptive Card envelope —
so only the URL source moved. Two differences worth knowing:

- posts appear as the **Workflows bot**; custom name and icon are not carried over
- interactive MessageCard buttons are not supported; use Adaptive Card actions

## 6b. Approval callback

The approval card's Approve/Decline buttons are `Action.OpenUrl` links. They
have to be: a card posted through an incoming webhook has no bot behind it, so
`Action.Submit` renders a button that silently does nothing.

They point at a Consumption Logic App that writes the decision into the same
`approvals` table the agent polls:

```powershell
az deployment group create -g BITriageDemo -n approval-callback `
  --template-file infra\approval-callback.json `
  --parameters tableEndpoint=https://<account>.table.core.windows.net

# grant its identity table access - no keys involved
az role assignment create --assignee-object-id <principalId from the output> `
  --assignee-principal-type ServicePrincipal `
  --role "Storage Table Data Contributor" --scope <storage account id>

# capture the trigger URL into the azd environment (gitignored)
azd env set APPROVAL_CALLBACK_URL "<listCallbackUrl value>"
azd deploy
```

**Why a Logic App and not a Power Automate flow.** The Azure Table connector
authenticates with a shared account key, and `allowSharedKeyAccess` is `false`
on this storage account by tenant policy — correctly, and not worth fighting.
An HTTP-triggered flow is also a premium trigger the tenant may not be
licensed for. The Logic App uses a system-assigned managed identity against the
Table REST API, so there is no key anywhere.

**The callback URL is a bearer credential.** Anyone holding the link can answer
an approval. It lives in the azd environment, never in the repo, and
`scripts/scan_secrets.py` treats it as a secret. The fingerprint in the link binds it
to one action, and the Logic App refuses a request that is unknown, already
answered, expired, or whose fingerprint does not match — after which the agent
revalidates all of it anyway.

**Leaving it unset is a valid configuration.** The card then shows the request
id and says to answer with `bi-triage approve <request>`, which needs no
infrastructure at all.

Treat the URL as a secret: anyone holding it can post to the channel. That is
acceptable in a test tenant — say so out loud rather than letting someone assume
otherwise.

**Production path** — post via Graph with an app registration so messages are
attributable to an identity. Note that app-only posting to channel messages is
restricted (it is gated behind protected APIs / migration scenarios), so most
production designs use a bot or a delegated flow rather than raw app-only Graph.
Confirm the path against current docs before committing to it.

## 6c. Scheduled sweeps

Nothing runs on a timer until you deploy this. Foundry routines are declared in
`azure.yaml` but ship disabled because they do not fire — see
[`foundry/README.md`](foundry/README.md) for the evidence.

Deploy [`infra/scheduled-sweep.json`](../infra/scheduled-sweep.json) once per
cadence. It is a Consumption Logic App with a system-assigned managed identity
that POSTs to the agent's responses endpoint:

```powershell
$ep = "https://<account>.services.ai.azure.com/api/projects/<project>"

az deployment group create -g BITriageDemo -n sched-silent `
  --template-file infra\scheduled-sweep.json `
  --parameters name=bitriage-silent-sweep projectEndpoint=$ep `
               command="silent sweep" frequency=Hour interval=1 owner=<you>

# grant its identity permission to invoke the agent
az role assignment create --assignee-object-id <principalId from the output> `
  --assignee-principal-type ServicePrincipal `
  --role "Cognitive Services User" `
  --scope <the project resource id, not the account>
```

Repeat with `name=bitriage-mailbox-sweep`, `command="sweep"`,
`frequency=Minute interval=5` for the mailbox drain. The two are separate jobs:
the mailbox sweep does not run the health scan.

`Cognitive Services User` is the role that carries data-plane access to a
`Microsoft.CognitiveServices` account, which is what a Foundry project is.
`Azure AI Developer` is not sufficient — its data actions are scoped to the
`OpenAI`, `SpeechServices`, `ContentSafety` and `MaaS` sub-paths, and invoking a
hosted agent is none of them. There is no `Azure AI User` role in this
subscription despite the name appearing in some docs.

Scope the assignment to the **project**, not the account, so the schedule can
invoke this agent and nothing else.

Until the grant propagates, runs fail with 403. That is correct behaviour and it
is visible: the workflow terminates with `SweepFailed` rather than reporting
success, so a failed sweep stays in run history as evidence.

Set `alertWebhookUrl` to a Teams incoming webhook if a failed sweep should also
announce itself. Without it, failures are recorded but nobody is told.

---

## 7. Data quality flag table

Default is `runs/dq_flags.csv` — visible, diffable, openable in Excel, and
easy to show before and after.

For a Fabric or SQL table instead, replace `DataQualityFlagTable` with three
methods (`read_all`, `append`, `reset`) against the real table.

Keep the CSV available regardless. Showing a spreadsheet gain a row is a better
signal than showing a query result change.

---

## 7b. Silent-failure detector (optional)

The detector reads semantic models directly rather than waiting for an alert.
It is off until `SILENT_HEALTH_PROBES` is configured, which is the right
default: "fresh" is a business question and guessing it produces the false
positives that get a detector muted.

### What a probe looks like

```json
[{"name":"sales-invoices",
  "workspace_id":"<guid>",
  "dataset_id":"<guid>",
  "table":"fact_sales_invoice",
  "date_table":"dim_date",
  "date_column":"date",
  "report_name":"Sales invoices",
  "expected_lag_hours":24,
  "min_absolute_drop":40,
  "watch_schema":true,
  "load_weekdays":[1,2,3,4,5]}]
```

Check it before trusting it:

```powershell
bi-triage health --preflight   # mistakes that would silently detect nothing
bi-triage health --probes      # what is watched, and how
```

`--preflight` exits non-zero on a configuration that cannot detect anything —
a probe with no ids, a duplicate name on the same model (both overwrite the same
baseline, so neither accumulates history), or `confirmations: 0`, which disables
the guard against false positives. That failure mode is the dangerous one: it
looks like monitoring and reports nothing, indefinitely.

**Set `date_table` whenever the measured table holds a date *key* rather than a
date**, which is what a star schema looks like. Without it the only way to get a
watermark is to point the probe at the date dimension, and a calendar dimension
is populated years ahead: on a real model that returned `2030-12-31` while the
data stopped at `2024-12-23`. The probe would have called a two-year-stale model
fresh for ever, and looked like it was working.

**Check `min_absolute_drop` against the table's real size.** It defaults to
1,000 rows, so on a 400-row table row-loss can never fire.

**Set `load_weekdays` for anything that does not load daily.** A weekday-only
feed read on a Sunday is legitimately two days behind; reporting that every
weekend is how a detector earns a filter rule and stops being read.

Two scans are needed before anything is announced (`confirmations`, default 2).
A single sweep marks a probe suspect and says nothing — that is the
suspect-then-confirm rule, not a fault.

### Schema drift

`watch_schema: true` adds a second query per sweep that lists visible columns
and measures, and reports anything that **disappears**. Additions are ignored on
purpose: models gain columns constantly, and a detector that fires on ordinary
development gets switched off, taking the removal case with it.

It uses DAX `INFO.VIEW` functions over the same read-only endpoint rather than
XMLA. XMLA would need ADOMD or TOM, which in practice means a .NET dependency
and a Windows host; the controller runs in a Linux container. A schema that
cannot be read is a detector fault, never "every column was deleted".

### Accepting a planned change

When a change is intentional — a table renamed, a feed genuinely halved — accept
it rather than waiting for the alert to be ignored:

```powershell
bi-triage health --accept sales-invoices
bi-triage health --accept all
```

This clears the suspicion, not the data: the next sweep records what it finds and
compares from there. Without it, the only alternatives are to let a standing
finding train people to skim past the output, or to reset the whole store and
lose every other baseline with it.

### When a probe keeps failing

After `max_consecutive_errors` faults (default 5) a probe is **parked** for
`circuit_cooldown_minutes` (default 60) and reports that it stopped. Time
reopens it, so a fixed permission is picked up without a redeploy, and one
successful reading clears it immediately.

This exists because it was needed: a probe pointed at a Direct Lake model, which
app-only callers cannot query at all, reached twelve consecutive 401s — pure load
on the capacity being watched, and a fault line in every sweep.

### Cost of watching

Probes run sequentially with `SILENT_PROBE_PACE_SECONDS` (default 1) between
them. `executeQueries` is throttled per user across *all* datasets, so a
detector that fires them concurrently becomes the capacity incident it exists to
watch for. Only one sweep runs at a time across instances, arbitrated by a lease
in the state table — two concurrent sweeps would each increment the same suspect
count and confirm a finding on its first real occurrence, turning the
false-positive guard into a source of them.

### Permissions, and the limit that will actually stop you

**Direct Lake models cannot be probed by an app-only caller.** A Direct Lake
model reaches OneLake as the *caller*, and Microsoft does not support service
principals for that, so `executeQueries` is refused whatever permission the
identity holds. Verified against a real Fabric medallion workspace: every table
Direct Lake, the identity already workspace Contributor, and the response a bare
`401 PowerBINotAuthorizedException` with an empty parameter bag and no message.

Nothing in that response mentions Direct Lake, OneLake or SSO, and the obvious
reading — grant it more access — does not work. To probe a Direct Lake model,
set its OneLake connection to a **fixed identity** rather than SSO. Import-mode
models have no such restriction.

The detector reports this as a **detector fault**, never as a data finding. "I
could not check" is not "everything is fine", and reporting it as bad data would
tell somebody their numbers are wrong because of a platform limitation.

Beyond that, the identity that runs the sweep needs **workspace Contributor**,
not Viewer. Viewer plus Build permission on the one dataset would be the
least-privileged combination, and it is not available to an app-only caller: the
dataset users API rejects `principalType: App` with *"API supported only for User
or Group principal types"*, and `executeQueries` requires Build. Contributor is
the least-privileged workspace role that carries Build for a service principal.

Also confirm, under **Admin portal → Tenant settings**:

| Setting | Needs |
|---|---|
| *Semantic Model Execute Queries REST API* | Enabled, and the identity inside any security group it is scoped to |
| *Service principals can call Fabric public APIs* | Enabled, same caveat |

Neither failure status describes its own cause. Missing workspace permission
reports `PowerBIEntityNotFound` (HTTP 404) rather than 403, so it reads as a
wrong id; the Direct Lake and tenant-setting refusals both report
`PowerBINotAuthorizedException` (HTTP 401) with no detail at all. The detector
attaches a hint to each rather than passing the bare platform error along.

```powershell
bi-triage health              # what the probes found
bi-triage health --baselines  # what healthy looked like last time
```

---

## 8. Application Insights (optional)

Set `APPLICATIONINSIGHTS_CONNECTION_STRING` and install the extra:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[azure]"
```

Spans appear within roughly five minutes. Check the **failed-run** view, not
just the healthy one — that is the question that gets asked.

Without the connection string every telemetry helper is a no-op. Telemetry is
never a hard dependency of the accelerator running.

---

## Final check

```powershell
.\.venv\Scripts\bi-triage.exe preflight
.\.venv\Scripts\bi-triage.exe reset

# Real side effects, deterministic reasoning - isolates tool problems from model problems
$env:TRIAGE_PROVIDER_MODE="mock"; $env:TRIAGE_TOOL_MODE="live"
.\.venv\Scripts\bi-triage.exe run scenario1-transient

# Then the real thing
$env:TRIAGE_PROVIDER_MODE="foundry"
.\.venv\Scripts\bi-triage.exe run scenario1-transient
.\.venv\Scripts\bi-triage.exe run scenario2-data-quality --show-data
```

Run the mixed mode first. When something breaks, it tells you immediately
whether the problem is the tools or the model — otherwise the two present
identically and you lose twenty minutes.

---

## Security notes

- **Never commit a filled-in `.env`.** `.gitignore` covers it; check anyway.
- Secrets are redacted at the persistence boundary (`redaction.py`, 11 patterns)
  before anything reaches an incident, a Teams message or a trace attribute.
- Traces carry metadata only — no prompt or completion content.
- The Teams webhook URL is a bearer credential in URL form. Rotate it after the
  exposure if the recording is shared.

---

## 9. Hosted deployment (the production shape)

Everything above describes the components. This section is how they run
unattended in Azure rather than from a laptop.

### What gets created

| Resource | Purpose |
|---|---|
| Foundry hosted agent `bi-triage-controller` | The orchestration loop, as a container |
| Foundry routine `bi-triage-schedule` | Wakes it on a cron schedule |
| Storage account + `incidents` table | Durable incident state across restarts |
| Application Insights | Traces from the container |

### Deploy

```powershell
azd config set auth.useAzCliAuth true          # reuse the az login; no browser flow
azd env new bitriage
azd env set AZURE_SUBSCRIPTION_ID       (az account show --query id -o tsv)
azd env set AZURE_LOCATION              eastus
azd env set AZURE_AI_PROJECT_ENDPOINT   "<project endpoint>"
azd env set AZURE_AI_PROJECT_ID         "<project ARM id>"

# Mailbox ingestion credentials. Never committed; .azure/ is gitignored.
azd env set GRAPH_CLIENT_ID     "<ingestion app id>"
azd env set GRAPH_CLIENT_SECRET "<ingestion secret>"

azd deploy bi-triage-controller --no-prompt
azd deploy bi-triage-schedule   --no-prompt
```

### Grant the controller identity what it needs

Deploying creates a **new** Entra agent identity for the controller, with no
permissions. Each agent gets its own identity, so each needs its own grants —
that is least privilege working, not a misconfiguration. Find it with:

```powershell
.\.venv\Scripts\bi-triage.exe identity --check-scope
```

| Scope | Role | Why |
|---|---|---|
| Storage account | `Storage Table Data Contributor` | Persist incidents |
| Application Insights | `Monitoring Metrics Publisher` | Emit traces; without it the log floods with 403s |
| Power BI workspace | Admin, principal type `App` | Read refresh history, trigger a retry |

**Do not grant it `Mail.Read`.** Exchange rejects Entra agent identities for
app-only mailbox access — verified as a 401 against the same token Graph's
directory endpoint accepted with a 200. Mail goes through the app registration
from section 2, which is why that one secret still exists.

### Verify

```powershell
azd ai agent invoke bi-triage-controller "sweep"      # reads the mailbox
azd ai agent monitor bi-triage-controller             # shows what it ignored, and why
.\.venv\Scripts\bi-triage.exe incidents             # written by the container, read from here
```

The last one is the real proof: an incident written by the container in Azure,
read back on another machine, means the container authenticated to storage as
itself with no secret in the deployment.

### Governance notes

Both encountered rather than anticipated, and both worth expecting again:

- The storage account is created with **shared key access already disabled**.
  Do not try to re-enable it; the identity-based path is the supported one.
- **Public network access is disabled within minutes** by tenant policy. Resolve
  with the supported `SecurityControl=Ignore` exemption tag plus a written
  justification, not by fighting the control:

```powershell
az storage account update -n <account> -g <rg> `
  --set tags.SecurityControl=Ignore tags.Justification="<why>"
az storage account update -n <account> -g <rg> --public-network-access Enabled
```

### Restarting the container

`azd deploy` with no code change completes in seconds and does **not** restart
the container, so its in-memory incident cache survives. To force a genuine
restart, change something in the service definition (an environment variable is
enough) and redeploy.
