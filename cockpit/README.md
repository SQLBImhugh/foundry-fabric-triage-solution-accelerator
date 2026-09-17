# Read-only Fabric cockpit

This retained React 19 + Vite sample displays semantic-model projections of
triage state inside a Fabric Data App. It was built from the Rayfin Universal
App template; the analytics pack is already installed and `CockpitPage` replaces the starter
home page. It is part of the public MIT-licensed solution accelerator, not a
supported product.

The Azure-hosted [Command Center](../docs/CommandCenter.md) is the accelerator's
operational UI for authenticated approvals, investigations, incident notes and
run history. This cockpit is not a deployment or state dependency. It remains
read-only: it cannot approve an action, start a run, resolve an incident or reset
controller state. All live application state now targets one Azure SQL Database;
this sample has not been rebound or proved against that database.

## Data and authentication

The **earlier-release** read path was:

```text
Controller -> standalone Fabric SQL Database -> mirrored SQL analytics endpoint
           -> Direct Lake semantic model -> Fabric embed proxy -> cockpit
```

DAX queries use the `triageState` connection alias in `fabric.yaml`.
`src/lib/fabric-client.ts` communicates with the Fabric host through its embed
proxy. Fabric authenticates that access; the absence of an `AuthProvider` in
`src/main.tsx` does not make the model public.

That Fabric SQL mirroring path is historical. Azure SQL provisioning does not
create or retarget the `triageState` semantic model, and no automatic mirror,
state-copy or compatibility path is provided. An existing cockpit deployment
may still show prior-release data; it is not current Azure SQL operational proof.

The Azure SQL application database is provisioned independently of either app.
Keep the Rayfin `data` service disabled in `rayfin/rayfin.yml`. Do not move state
into an app-owned database, apply Rayfin entity migrations to the controller's
tables, or delete/recreate the database when redeploying an app.

The page queries on load, not on a polling timer. Reload it to request new
results. Mirroring and semantic-model visibility can lag the SQL write; this
is not a real-time view or an unrestricted history report. These latency notes
describe the historical semantic-model path, not a new Azure SQL integration.

## Panels

| Panel | Recorded state |
|---|---|
| Headline counts | Open incidents, unanswered approvals, pending retries, held claims and suspect probes |
| Recent incidents / incidents by status | Incident IDs, failure signatures, timestamps and recorded statuses |
| Claims and leases | Unexpired holders that coordinate controller work |
| Deferred retries | Retry status, due time and attempts |
| Approvals | Requests and recorded decisions; a blank decision is not consent |
| Semantic health baselines | Watermarks, row counts and unconfirmed suspect probes |
| Ignored mail | Sender, subject and the inbox filter's rejection reason |

Recent incidents, approvals and ignored mail use `TOPN(25)` queries. The cockpit
does not expose the command center's full incident workspace or execution
timeline.

## Optional sample configuration and deployment

Skip this section when deploying the accelerator; use the Command Center and
the [deployment guide](../docs/DeploymentGuide.md). To evaluate this separate
sample, supply a separately reviewed read-only semantic model. It must expose
the tables and columns referenced by `src/queries/triage.ts`, and the intended
viewers must have the required Fabric/model access.

From the repository root, install the locked frontend dependencies:

```powershell
Set-Location .\cockpit
npm ci
```

Use Node.js 22 LTS or 24 LTS, matching the Rayfin CLI's supported runtimes.
The Rayfin packages are pinned together at `1.35.0`: the published `1.35.1`
CLI and auth-provider manifests refer to an unavailable `rayfin-lib@1.35.1`.
Do not upgrade one member without resolving and validating the complete set.

The npm overrides patch vulnerable leaves in Rayfin's pinned OpenTelemetry
tree without changing its SDK API: vulnerable 2.x `core` and
`propagator-jaeger` versions move to patched 2.x releases, and the `0.217.0`
transformer's protobuf dependency moves to a patched 8.x release. Remove these
overrides when upstream constraints admit secure versions, then rerun the
offline tests, Fabric build and `npm audit`.

Set the `triageState` workspace and semantic-model item in `fabric.yaml` to your
deployment. Regenerate `src/fabric.generated.ts` through the build rather than
editing generated output:

```powershell
npm run build:fabric
```

When a Fabric deployment is intended, sign in to Rayfin for the target tenant
and workspace, review that target, then deploy and inspect its status:

```powershell
npx --no-install rayfin login
npm run rayfin:up
npx --no-install rayfin up status
```

Open the deployed app inside the Fabric portal for authenticated model queries.
Do not commit credentials or deployment-specific configuration changes.

## Local commands

Run these from `cockpit`.

| Command | Behavior |
|---|---|
| `npm run gallery` | Start Vite locally; does not supply a Fabric portal session or synthetic controller state |
| `npm run preview -- --spec <file>` | Render one Graphein spec to a PNG and JSON report; not a web server |
| `npm run dev` | Deploy non-static Rayfin services, then start Vite; not an offline-only command |
| `npm run build` | Refresh Rayfin environment configuration, then type-check and build with Vite |
| `npm run build:fabric` | Generate model configuration, type-check and build for Fabric |
| `npm run lint` | Run ESLint |
| `npm test` | Run the Vitest suite |
| `npm run rayfin:up` | Deploy the Fabric app |

Headless preview can use inline data without a network connection. Adding
`--query` performs a live semantic-model query. Its bundled font setup is not
proof of parity with the cockpit's system-font theme; inspect the deployed
surface when changing typography.

## Source and reference guides

| Path | Purpose |
|---|---|
| `src/App.tsx`, `src/pages/CockpitPage.tsx` | Provider tree and controller-state panels |
| `src/queries/triage.ts` | DAX queries and state-schema assumptions |
| `src/hooks/use-semantic-model-query.ts`, `src/lib/fabric-client.ts` | Query state and Fabric embed transport |
| `src/global.css` | Canonical dark theme and chart tokens |
| `fabric.yaml`, `src/fabric.generated.ts` | Model aliases and generated configuration |
| `rayfin/rayfin.yml` | Optional sample services; no app-owned operational state |
| `.agents/skills/` | Reusable template and platform reference material |

Read [the cockpit contributor guide](AGENTS.md) before changing this app.
Bundled skills describe several possible template configurations, not the
current cockpit. Do not reapply `pack:add` as a setup step: it can overwrite
customized kit files even when it preserves the app entry points.
