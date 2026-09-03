# Hosted architecture: what runs where, and which identity does it

This describes the deployed system, and — more usefully — the things that turned
out not to work the way the documentation implied. Every claim below was
verified against a live tenant. Where something is inferred rather than
observed, it says so.

## The shape

```
Foundry routine  (bi-triage-schedule, cron)   <- declared, SHIPS DISABLED, does not fire
        |                                        (preview defect; see below)
        v
Hosted agent: bi-triage-controller          <- container, Python, its own agent identity
        |
        |-- Microsoft Graph ------------> alerts mailbox   (app registration, scoped)
        |-- Prompt agent: bi-triage -----> reasoning       (no permissions at all)
        |-- Prompt agent: bi-data-quality-> evidence       (no permissions at all)
        |-- Power BI REST --------------> refresh history + retry  (agent identity)
        |-- Azure Table ----------------> incident store   (agent identity)
        `-- App Insights ---------------> traces           (agent identity)
```

The controller is the only component that acts. The two prompt agents reason and
return typed results; they hold **no permissions whatsoever**. That is not an
oversight, it is the design: a component that cannot act cannot be tricked into
acting.

## Identities

Foundry creates a first-class **Microsoft Entra agent identity** for every agent,
automatically, including the hosted controller. Verified via
`GET /v1.0/servicePrincipals/microsoft.graph.agentIdentity` — note that agent
identities and their blueprints are OData *subtypes*, not top-level collections.
Querying `/agentIdentities` returns "Resource not found for the segment", which
reads like the feature is missing when it is simply a different URL shape.

What that gives us, all confirmed live:

| Property | Observed |
|---|---|
| Credentials on the blueprint | `keyCredentials=0`, `passwordCredentials=0` |
| How it authenticates | federated credential `fmi-fic`, audience `api://AzureADTokenExchange` |
| Accountable human | `sponsors` already populated — Foundry sets it at creation |
| Object id vs app id | identical, unlike an ordinary service principal |

There is no secret to rotate because there is no secret. That matters
concretely here: tenant governance purges Entra app secrets on a schedule, so a
secret-based integration starts failing roughly a month after it ships.

`bi-triage identity --check-scope` prints all of the above from the live
directory rather than from configuration.

### What the identity can and cannot reach

This is the part worth knowing before designing around it.

| Service | Accepts an Entra agent identity? | Evidence |
|---|---|---|
| Azure RBAC (Storage Tables) | **Yes** | Container wrote an incident; read back from another machine |
| App Insights | **Yes** | After granting `Monitoring Metrics Publisher` |
| Microsoft Graph — directory | **Yes** | `GET /v1.0/users` returned **200** from inside the container |
| Microsoft Graph — **mail** | **No** | Same token, same container: **401** |
| Power BI / Fabric | **Yes** | Added as a workspace Admin, HTTP 200 |

The mail result is the important one, and it was isolated deliberately: the
identical token that Graph's directory endpoint accepted (200) was rejected by
the Exchange-backed mail endpoint (401), with `Mail.Read` present in the token's
`roles` claim, a real mailbox at the far end, and an `ApplicationAccessPolicy`
in force. **Graph accepts agent identities; Exchange does not yet accept them
for app-only mailbox access.**

A 401 rather than a 403 is the tell. A policy denial returns 403; this is the
token being refused outright.

So mail ingestion falls back to a conventional app registration. That app
registration's client secret is **the only credential in the entire
deployment**, and it expires — which is precisely the argument for agent
identities everywhere else.

## Mailbox scoping is the load-bearing control

App-only `Mail.Read` is **tenant-wide by default**. We proved that the
uncomfortable way during an earlier spike: an app created to read one
mailbox happily read the global administrator's inbox. It was deleted
immediately.

The control is an Exchange `ApplicationAccessPolicy`:

```
bi-alerts@…    (the alerts mailbox)   Granted
anyone-else@…                         Denied
admin@…                               Denied
```

Worth knowing: the policy accepts a **managed identity's** appId too, so the
control is unchanged whichever identity type ends up calling Graph.

Two guards enforce this in code rather than in a runbook:

- `GraphInbox.verify_scope()` attempts to read a mailbox the agent must *not*
  be able to read. A **403 is the passing result**. A 200 means the policy is
  missing, and the controller refuses to ingest.
- `verify()` rejects any token carrying a `upn` claim — that would be a
  delegated *user* token, not the agent's own identity.

## The inbox filter is a security control

An agent that triages every message in a mailbox is steerable by anyone who can
send mail to it. There is no authentication on an inbox: the sender field is
forgeable and the body goes straight into a model prompt.

This was not hypothetical. Pointed at a real mailbox with no filter, the agent
triaged Microsoft Entra ID Protection and PIM weekly digests and crashed on
several of them. With the filter:

```
Ignored 10 message(s) that were not Power BI refresh alerts
```

The filter fails closed, including when its own regex is invalid — a typo that
silently disabled filtering would reopen the injection surface while appearing
configured.

## Durable incident state

Incidents live in Azure Table Storage. This is not about history: incident state
is what makes the agent **idempotent**. It is how a second alert about a problem
already being worked is recognised as a duplicate rather than triggering a
second remediation. A container that forgets on restart is a container licensed
to act twice.

The store degrades to in-memory and says so loudly rather than refusing to
start. The agent should survive storage going away.

Two governance notes, both encountered rather than anticipated:

- The storage account was created with **shared key access already disabled**.
  That validates the identity-based design rather than fighting it.
- **Public network access was disabled within minutes of creation.** Resolved
  with the supported `SecurityControl=Ignore` exemption tag plus a
  justification — not by arguing with the control.

## Two entry paths, one code path

The controller answers to both:

- **Scheduled** — the routine invokes it with the sentinel `sweep`; it drains
  the mailbox.
- **Interactive** — the Playground invokes it with the text of an alert; it
  triages that alert.

Routing was originally inferred from message length. That was wrong: the host
passes conversation history, so a five-character sweep request arrived as
several hundred characters of a previous alert and got re-triaged. It is now an
explicit sentinel, and only the most recent inbound message is read.

## Things that will bite the next person

- **Agent tool schemas use the flat Responses shape.** `name` at the top level,
  not nested under `function`.
- **Agent versions are `POST /agents/{name}/versions`.** A `PUT` returns 405.
- **`gpt-5.6-luna` rejects `temperature` entirely.** It lives on the agent
  definition, so the agent registers cleanly and then 400s on every invoke.
- **`run()` must be sync** and return an async iterable when the host asks for a
  stream. As an `async def` it returns a coroutine and the host fails with
  `'coroutine' object has no attribute '__anext__'`.
- **The container federates a workload identity**, not an IMDS managed identity.
  `ManagedIdentityCredential` alone returns a token Graph rejects. The code uses
  a credential chain with every *human* credential excluded, so it cannot
  quietly authenticate as whoever last ran `az login`.
- **Each agent gets its own identity, so each needs its own grants.** Deploying
  the controller created a new identity with no permissions; storage writes
  failed silently until it was granted. That is least privilege working, not a
  bug.
- **The `Authorization` header lines in `inbox.py` and `powerbi.py` display as
  asterisks through some tooling.** That is display-side redaction, not file
  content. Do not "fix" it — verified by measuring the line rather than reading
  it.
- **`azd ai routine list` may report "No routines found"** for a routine that
  exists and dispatches successfully. That inconsistency turned out to be a
  symptom, not a cosmetic bug: **the routine never fires.** Registered with a
  five-minute cron, `enabled: true`, dispatch returns ids — and zero runs, with
  no container activity after a dispatch. Treat scheduled triggering as
  unavailable in preview and drive the agent from a scheduler you already trust.
  Nothing else depends on it, because the scheduled and interactive paths are
  the same code.

  Re-verified 2026-09-02, six days after registration, and unchanged. Three
  independent checks agreed: `azd ai routine run list` reports no runs, a fresh
  dispatch produced none, and Application Insights recorded activity in only two
  of the previous twenty-four hours — both of them hours when somebody invoked
  the agent by hand. A five-minute cron would appear in all twenty-four.

  ```powershell
  azd ai routine show bi-triage-schedule      # cron */5 * * * *; reported enabled while it was
  azd ai routine list                         # {"value": null}
  azd ai routine run list bi-triage-schedule  # no runs
  ```

- **`azd deploy` does not manage routines at all.** Measured three ways on
  2026-09-02 and 2026-09-03: it does not apply `enabled:` from `azure.yaml` in
  either direction, and it does not create a routine newly declared there. A
  second routine added to the file stayed absent from the project through a
  successful deploy, and `routine show` reported "not found" until it was
  created explicitly.

  So the declarations in `azure.yaml` are a statement of intent that survives
  into version control; the project's actual routines are managed with the CLI:

  ```powershell
  azd ai routine create <name> --file <manifest.yaml>
  azd ai routine disable <name>
  azd ai routine show <name> -o json          # always confirm; the deploy will not
  ```

  Two traps in `create`. Without `--file` you cannot supply the action's
  `input`, and a routine with no input sends an empty message — which this
  agent treats as *drain the mailbox*, so a health-sweep routine would silently
  run the wrong sweep. And the trigger is immutable: changing a cron or a
  timezone returns `UserError: Routine trigger cannot be changed after
  creation`, so amending one means delete and recreate.

## Reproducing it

```powershell
azd env set AZURE_AI_PROJECT_ENDPOINT "<project endpoint>"
azd env set GRAPH_CLIENT_ID     "<ingestion app id>"
azd env set GRAPH_CLIENT_SECRET "<ingestion secret>"   # never committed

azd deploy bi-triage-controller --no-prompt
azd deploy bi-triage-schedule   --no-prompt

azd ai agent invoke bi-triage-controller "sweep"
bi-triage identity --check-scope
bi-triage incidents
```

Grants the controller's agent identity needs:

| Scope | Role |
|---|---|
| Storage account | `Storage Table Data Contributor` |
| Application Insights | `Monitoring Metrics Publisher` |
| Power BI workspace | Admin (principal type `App`) |
