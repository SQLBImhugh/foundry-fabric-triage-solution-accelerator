# Read-only Fabric cockpit contributor guide

The [repository contributor contract](../AGENTS.md) applies here. This directory
contains the retained React/Vite monitoring cockpit, not an unconfigured
Universal App starter. It is not required for the accelerator deployment or
application state. Read [README.md](README.md) for its historical data binding
and optional deployment/command reference.

## Scope and safety boundaries

- Keep this app read-only. It queries controller state through a semantic model;
  it has no remediation, approval, reset or incident-resolution controls.
  Operator workflows belong to the separate
  [Azure-hosted command center](../docs/CommandCenter.md).
- All live accelerator application state belongs in one shared Azure SQL
  Database, independent of this sample. Rayfin `data.enabled` is `false`.
  Do not enable an app-owned data service, copy the controller's tables through
  Rayfin or delete the database as an app deployment step. The existing
  semantic-model alias is an earlier-release binding, not evidence that the
  sample reads the new Azure SQL state.
- Preserve Fabric-hosted authentication. `src/lib/fabric-client.ts` uses the
  Fabric embed proxy; no app `AuthProvider` is needed for that path. A local
  Vite server does not provide the Fabric host or authorize model access.
- Keep the default test path offline. Never make a test depend on tenant access
  or a live semantic model. Use synthetic fixtures for automated checks.
- Do not add credentials, live environment IDs, personal data or customer data
  to source, screenshots or documentation.

## Current implementation

`src/main.tsx` renders `App`, which mounts the theme, sketch, selection and filter
providers around `src/pages/CockpitPage.tsx`. The page reads the `triageState`
alias through `useSemanticModelQuery`. DAX and state-schema assumptions live in
`src/queries/triage.ts`; cards use the dashboard kit's `toChartData` and `toTable`
mapping helpers.

Queries run on load; the page does not periodically refresh itself. Preserve
loading, empty and error states, and do not describe cached or mirrored data as
instantaneous SQL state. A blank approval decision must never be presented as
approval.

The cockpit has a canonical dark theme in `src/global.css`, system fonts
(Segoe UI and Cascadia Mono) and no theme-toggle control. Preserve that design
unless a redesign is explicitly requested. The command center has a separate
theme; its DejaVu Serif Condensed/Onyx styling does not redefine this app.
Check both `src/main.tsx` and `index.html` before making a claim about font
downloads.

## Bundled skills

`.agents/skills/` retains reusable template and platform reference material.
Statements there about a hello-world page, enabled Rayfin data, starter fonts,
auth wiring or demo slicers describe template variants, not the deployed
cockpit. Use the current source and this guide for app-specific decisions;
resolve platform API questions against the installed package documentation.

| Work | References |
|---|---|
| Model aliases and query transport | [fabric-data](.agents/skills/fabric-data/SKILL.md) |
| DAX grain, filtering and result shapes | [dax](.agents/skills/dax/SKILL.md) |
| Dashboard cards, tables and charts | [visuals](.agents/skills/visuals/SKILL.md) |
| Headless spec rendering | [headless-preview](.agents/skills/headless-preview/SKILL.md) |
| Layout and visual tokens | [app-design](.agents/skills/app-design/SKILL.md), adapted to the existing dark theme |
| Rayfin API and deployment details | [rayfin](.agents/skills/rayfin/SKILL.md) |

The analytics pack is already installed. Do not run `npm run pack:add` to begin
a routine cockpit change: it re-copies kit-owned files and can overwrite local
customizations even when seed protection preserves `App.tsx` and `main.tsx`.
Use the [capability router](.agents/skills/capability-router/SKILL.md) and
[pack manifest reference](.agents/skills/capability-router/pack-manifest.md) only
for an explicitly requested scaffolding or capability change, with the copy
plan reviewed first.

Do not rewrite versioned vendor guidance merely to match this app. Keep
copyright and license notices intact, and report conflicting platform guidance
rather than guessing which API behavior is correct.

## Rayfin documentation

Never answer Rayfin APIs from memory. Use the version-matched `rayfin` MCP server
configured in `.mcp.json`, when available, or run the installed CLI from
`cockpit` so it resolves this project's packages:

```powershell
npx --no-install rayfin docs search "<topic>" --module guide
npx --no-install rayfin docs get --symbol "<symbol>"
```

Use `discover_packages` or `rayfin docs discover <topic>` when installed
documentation does not cover the task. Install dependencies only for a required
manifest change or a confirmed missing dependency.

## Verification and deployment

For cockpit code changes, run the relevant Vitest tests, ESLint and the
type-check/build from this directory:

```powershell
npm test
npm run lint
npm run build:fabric
```

`npm run preview -- --spec <file>` renders a PNG and report; it does not serve a
home page. Inline-data specs can run offline. `--query` uses a live model and
is not part of the default offline suite. The preview's bundled Inter setup
does not establish font parity with the system-font cockpit.

`npm run dev` and `npm run rayfin:up` perform deployment work. Use them only when
a deployment is part of the task, after checking the target tenant and
workspace. Verify the deployed app inside Fabric, where the embed proxy is
available. Local Vite rendering alone cannot verify authenticated model reads.
