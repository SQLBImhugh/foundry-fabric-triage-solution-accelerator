# Command center frontend

React 19, TypeScript, and Vite client for the same-origin `/api` command-center
contract. This directory is independent of `cockpit` and contains no backend,
deployment, secret, or client-side demo dataset.

Operator workflows and deployment are documented in
[CommandCenter.md](../docs/CommandCenter.md).

## Local commands

Run from this directory with Node 22.12 or later:

```powershell
npm ci
npm run dev
npm run test
npm run typecheck
npm run lint
npm run build
```

Vite serves `http://127.0.0.1:5173` and proxies `/api` to
`http://127.0.0.1:8000`. Set `COMMAND_CENTER_API_ORIGIN` before `npm run dev` to
use a different local backend origin. This is a development proxy setting, not
a browser-visible credential. Production serves `dist` and `/api` on the same
origin. A static-only preview without that API reports an error; it never
substitutes demo records.

The build checks that both font files and their license are present in `dist`,
match the source assets, and are referenced by the production CSS and HTML.
The supplied triage artwork is used for the sidebar, sign-in view and browser
icon. Its transparent margins were cropped for the UI; build verification checks
the PNG assets match their source files and remain referenced in the output.
Tests stub network access and do not contact Azure.

## Authentication and modes

`GET /api/config` supplies the mode, application name, Entra tenant, public
client ID, and delegated API scope. When authentication is enabled,
`src/api/auth.ts` initializes MSAL Browser, handles its redirect, and acquires
access tokens silently for API requests. Interaction-required failures ask
the operator to sign in again rather than redirecting from a polling loop.
API calls use bearer tokens and omit cookies.

Register the served application origin/path as a **SPA redirect URI** in the
public-client Entra registration. The frontend uses that same origin/path
for sign-in and sign-out redirects. No client secret or API key is supported.
Tokens use MSAL's tab-scoped session cache; the theme is separate React state
and is not written to storage.

Optional Teams links use `/?approval=<URL-encoded request_id>`. After the first
successful snapshot, the inspector selects that approval rather than the
default queue item. Later refreshes do not override the operator's selection.
Approval IDs outside a bounded snapshot are resolved through the detail API;
missing or unauthorized records show an error rather than another approval.
MSAL carries the approval ID in its application state through sign-in and
restores the query parameter without changing the registered SPA redirect URI.
The URL only selects a record. It never supplies consent or authorization.

Demo mode is supplied by the server and displays
`Synthetic demo / No Azure effects`. Live and demo snapshot/config modes must
agree. Fetch failure leaves an explicit error and, if available, a labelled
last-received snapshot. It does not change modes.

## Interaction semantics

- Snapshots, selected records, and visible run pages poll sequentially every
  five seconds. Replaced requests are aborted and cannot replace a newer
  selection. The paged incident register polls every ten seconds; a full incident
  record polls every five seconds when no mutation is in flight. Filters and
  decision text survive record refreshes.
- Attention-summary tiles select the global command-center status group and
  clear search/workload constraints. Selecting the same unconstrained group
  again returns to All work. A tile is not shown as active while additional
  constraints or the incidents-only view apply.
- The API status `needs_review` belongs to the Needs investigation filter and
  uses that label in queue and inspector badges. Wire statuses remain intact.
  Workload choices always include Power BI and Fabric pipeline, plus any
  additional workload observed in the snapshot. Empty workloads remain
  selectable. Notebook activity failures belong to the parent pipeline;
  standalone notebook-job monitoring is not implemented.
- `resolved_by_user` belongs to the resolved filter but has its own label and
  informational badge. It is a tracking decision, not proof of a verified repair.
- The queue inspector links to a dedicated full-page incident record. Incident
  IDs use same-origin query-string navigation and namespaced sign-in state;
  opening a link never writes a note, resolves a case or grants consent.
- Approval requires capability, current server permission, a valid future
  expiry, a fingerprint, and an operator reason. A changed fingerprint must
  be reviewed again. A `409` refreshes the records and shows an error.
- `decision_recorded` is only a decision receipt. `queued` is only a command
  receipt. Neither is displayed as a successful action or verified resolution.
- Investigation targets and command kinds come only from server configuration.
  Retrying an unchanged form reuses its UUID idempotency key. Closing the
  dialog retains the draft and retry key. Mutations are never auto-retried.
- Ask is record-scoped and read-only from this UI. Its `incident_id` payload is
  `item.incident_id ?? item.source_id`, so a pending approval can supply context
  before an incident exists. The API must resolve that context without
  dispatching triage. `response.mode` explicitly labels records-based versus
  model-generated answers; an unknown mode is rejected rather than inferred.
- Interrupted or uncertain commands expose **Human reconciliation** only to
  server-reported administrators. It requires a nonempty reason and a separate
  confirmation that external job history and target state were reviewed.
  `POST /api/commands/{source_id}/reconcile` records that human review and clears
  the target uncertainty block; it does not execute or retry the command.
  Changed command details require confirmation again. Failed submissions are
  never automatically retried, and their response is not shown as execution
  success. The API must enforce administrator access and current command state.

The API owns identity, authorization, redaction, policy, durable state, and
execution. Frontend controls are not an authorization boundary.

## User interface terminology

**System overview** names the read-only view of monitored resources, the agent
team and service health. The signed-in person is labelled **User**. Internal
API/audit fields may retain `actor` for wire compatibility; it is not a displayed
role or a claim about the person.

The user menu reads the signed-in user's profile photo with a separate delegated
Microsoft Graph `User.Read` token, never the API's bearer token. Missing photos
use initials. Photo permissions are requested only through an explicit user
interaction when silent acquisition cannot complete. Photo access does not
grant directory administration or supply group membership for authorization.

## Incident and access workspaces

The incident feature has its own API client, paged register and full-page
workspace. Notes and resolution require server capabilities, current evidence
and durable confirmation. Discussion displays persisted pending, completed and
failed replies. Polling must not replace an operator's draft or move a late
response to a different incident.

Human resolution requires a nonempty reason, explicit confirmation,
`expected_version` and `source_revision`. The API conditionally appends it
against the current tracking version and original persisted SQL NVARCHAR
payload, hashed as SHA-256 over UTF-16 LE bytes. A `409` locks that review until
the incident is refreshed and reviewed again. The UI never reserializes evidence
to invent a revision. **Resolved by user** remains separate from the controller's
outcome and does not reset budgets, approve work or clear claims/reservations.
Later controller evidence invalidates the older closure without deleting it.

Execution explanations, observer answers and run details render Markdown as
structured text with 15px prose. Raw HTML, embedded images and non-HTTPS links are
not rendered. Run-list previews contain only phrasing elements so their selection
buttons cannot contain nested links or block content. The newest incident
execution explanation is expanded initially; an operator's expansion choices
survive refreshes. Agent name, run ID, execution state and outcome metadata
remain separate from the explanation.

**Access & permissions** is read-only and available to Reader and higher app
roles. `GET /api/access` reports `current_user`, the four-entry `role_catalog`,
the verified tenant/application IDs and token-issued/expiry times. Its source
must be `entra_app_roles` in live mode or `synthetic_demo` in demo mode.
Missing timestamps are not inferred; displayed times use the browser's local
time zone. An invalid response does not fall back to earlier permissions.

Entra app-role claims are the sole live authorization authority. Reader can
read and ask; Operator adds investigations, notes and human resolution;
Approver adds decisions but not Operator permissions; Admin includes all app
capabilities and validation/reconciliation, not directory administration.
The application does not query a user roster or current group membership,
derive membership from configuration, or identify which group supplied a role.
Group owners and IT manage membership and assignments externally in Entra.
There are no Add/Edit user, invite or local-grant controls.

**Refresh permissions** forces MSAL to request a new custom-API token, then
reloads effective access and the command-center snapshot. Mutating controls
remain locked until a post-renewal snapshot confirms capabilities. The refresh
does not automatically open sign-in/consent popups, replay failed writes or
promise instant revocation of other sessions' tokens. An error preserves
read-only records while preventing stale permissions from enabling actions.

The old `?view=admin` URL aliases this read-only view. Authenticated requests to
retired `GET`/`POST /api/admin/users` and `GET /api/admin/audit` return
`410 managed_in_entra`. There is no SQL authorization store or importer to
restore, and the authorization cutover does not reset incidents or history.

## Administrator scenario validation

The navigation exposes **Scenario validation** only when the server snapshot's
`actor.roles` includes `admin`. The validation API must enforce that role
independently; hiding a navigation item is not authorization.

- `GET /api/validation/scenarios` supplies the 15 canonical case names, titles,
  descriptions, and available `mock` / `foundry` providers. Demo mode advertises
  only `mock`.
- `POST /api/validation/scenarios/{name}` runs one explicitly selected case and
  provider. Run-all executes the complete advertised catalog with at most two
  concurrent requests. Assertion failures do not prevent the remaining cases.
  Transport uncertainty or authorization failure stops further scheduling.
- `GET /api/validation/results` supplies recorded case results. Each result can
  open the existing run-history inspector through its `run_id`.

The page labels this as **deployed-code validation with synthetic tools and
isolated state**, not production remediation. Foundry validation still uses
model calls. A response counts as passing only when its `passed` boolean is
true and its failure list is empty.

The Stop queue control prevents new requests, not execution already in flight.
Navigation within the application retains the batch, including when opening
a recorded run. Closing/reloading the application or losing administrator
access stops scheduling more cases; it cannot cancel requests already accepted
by the server. No scenario request is automatically retried.

## Appearance and font license

`src/styles.css` centralizes Onyx dark/light colors, Ink two-pixel geometry and
hard shadows. Georgia is the preferred system font, followed by DejaVu Serif
and DejaVu Serif Condensed. DejaVu Serif Condensed regular and bold remain
self-hosted under `public/fonts`; there are no external webfont requests. The unmodified
distribution license is `public/fonts/LICENSE.txt`, retrieved from:

<https://github.com/dejavu-fonts/dejavu-fonts/blob/master/LICENSE>
