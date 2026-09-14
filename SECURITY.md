# Security

This is an independently maintained, MIT-licensed solution accelerator:
sample code, not a Microsoft product or a supported service. It is not covered
by the Microsoft Security Response Center (MSRC) or a Microsoft support
agreement.

## Reporting a vulnerability

Do not report vulnerabilities through public GitHub issues. Use this
repository's **Security > Report a vulnerability** private reporting channel.
Include the affected version and file, the impact, and sanitized reproduction
steps. Do not attach access tokens, credentials, private tenant identifiers,
mailbox contents or customer incident data.

Reports are handled on a best-effort basis, with no guaranteed response time
or bug bounty. For a vulnerability in Microsoft Foundry, Power BI, Fabric,
Entra or another Microsoft service rather than this sample, report it to
[MSRC](https://aka.ms/SECURITY.md).

## Application authorization

The live command center accepts validated, tenant-specific Entra access tokens
for its API, with delegated `access_as_user` and at least one recognized
`CommandCenter.*` role. Signature, issuer, audience, tenant, lifetime and
identity are checked on the server. ID tokens, app-only tokens and caller
supplied identity/role headers are not substitutes.

The four roles are Reader, Operator, Approver and Admin. All include Reader
capabilities; Operator and Approver do not imply each other. Admin includes
all app capabilities, not Azure, Fabric or Entra directory administration.
Normal assignments come through ordinary Entra security groups. Provisioning
is a privileged operator operation; group owners and IT manage membership
outside the application.

Entra app-role claims are the **only live permission authority**. There is no
SQL membership permission store or runtime Graph directory lookup/write.
`COMMAND_CENTER_ACCESS_MANAGEMENT_ENABLED=true` is rejected. Retired access
management routes cannot change grants; the read-only Access & permissions
page reports the roles and dates of the token presented to the API.

Refresh permissions requests a fresh API token and reloads effective access.
It does not establish current group membership or revoke another user's
already-issued token. Changes depend on Entra propagation and token renewal;
do not promise immediate revocation. The optional browser profile photo uses
a separate delegated Graph `User.Read` token, never a directory administration
permission.

See [DeploymentGuide.md](docs/DeploymentGuide.md#10-command-center-entra-authorization)
for registration, group licensing, selected-user consent and the ordered
cutover from an older SQL-authorized deployment.

## Runtime identities and data

The App Service UAMI, hosted controller identity, prompt agents and provisioning
operator are separate principals. Grant permissions to the component that
acts. Prompt agents hold no workload grants. The web UAMI must not receive
`AppRoleAssignment.ReadWrite.All`, `Group.ReadWrite.All` or mailbox permissions
to support the Access page.

State lives in a standalone Fabric SQL Database, authenticated with Entra
tokens. The web UAMI has object-scoped access to the operational records it
uses, including SELECT/INSERT on `triage_incident_activity`, not DDL or
UPDATE/DELETE on core incidents. Schema changes belong to the operator.
The database is not owned by the App Service or the separate read-only Rayfin
cockpit.

Human resolution is an append-only **tracking decision**, not verified
remediation. It binds to the original SQL NVARCHAR payload's SHA-256 hash
using UTF-16 LE bytes. New evidence invalidates that closure; it does not
reset controller incidents, remediation budgets, notifications, approvals,
claims or retry history.

Approval requires an explicit, fingerprint-matched, unexpired, unused decision.
A denial must not consume the remediation budget. Tools are allowlisted and
limits are enforced by the controller, not the prompt. A submitted refresh or
pipeline job is not proof of recovery. Scheduled pipeline monitoring accepts
only configured targets and does not monitor standalone notebook jobs.

## Network and deployment

The command-center template defaults to private ingress with separate VNet
egress integration, NAT and private DNS. SCM and FTP basic publishing
credentials stay disabled; deployment uses Entra-authenticated CLI/Kudu.
Private web ingress alone does not prove private Foundry or Fabric connectivity.
Verify backend routing, DNS, managed network isolation and runtime permissions
separately.

An explicitly approved client-access exception must stay limited to verified
single-host egress addresses and Entra authentication. Keep SCM independently
deny-all except for a scoped deployment window; restore temporary changes in
`finally`. A code-only release must not reset an approved persistent ingress
configuration through a default full infrastructure redeployment.

Before ZIP upload, require stable authenticated read-only SCM readiness.
After an ambiguous accepted write, inspect the existing deployment and reconcile
its result instead of blindly submitting another write.

## Limits and legacy integrations

The inbox filter is a security control and fails closed. Never broaden it to
make a test message trigger. Mail ingestion also needs a verified denied
canary read and an app-only identity accepted by the mailbox service.

The source retains optional client-secret mail, Teams webhook and bearer-link
approval paths. They are not the secretless command-center deployment path.
Bearer-link callbacks do not authenticate the claimed responder, and the
legacy approval procedure rejects web-delivery proposals. Do not restore a
legacy path to bypass Entra roles or create secrets to satisfy old examples.

The interactive Foundry Playground alert path still lacks a distributed
signature claim. An interactive request overlapping a sweep can pass the
open-incident check before either caller persists. Avoid overlapping live
interactive and scheduled remediation; see
[the hosted architecture](docs/foundry/README.md#execution-and-concurrency).

Redaction belongs at persistence boundaries, but it does not authorize
publishing operational data. Spans contain metadata only, not prompts or
completions. Never commit filled-in environment files, tokens, webhooks,
publishing profiles or private deployment details. Read
[TechnicalArchitecture.md](docs/TechnicalArchitecture.md) and review the
limits, data access and retention requirements before enabling live actions.
