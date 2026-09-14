# Support

This is an independently maintained, MIT-licensed solution accelerator: sample
code to read, adapt and deploy in your environment. It is not a supported
Microsoft product and has no service-level agreement or guaranteed response
time.

## Bugs and feature requests

Search this repository's GitHub Issues before opening a new issue. Include the
commit/version, runtime, affected component, sanitized error and the smallest
reproduction. Prefer an offline scenario when possible. State whether the
failure occurred in the command-center App Service, Foundry hosted controller,
Fabric SQL store, configured pipeline monitor or separate read-only cockpit.
These are different deployment and permission boundaries.

Do not post access tokens, credentials, filled-in configuration, private tenant
or resource identifiers, mailbox contents, customer data or internal sources.
Synthetic names and redacted logs are sufficient for a public report.
Vulnerabilities belong in the private process in [SECURITY.md](SECURITY.md),
not a public issue.

## Deployment and access problems

Start with [AzureAccountSetUp.md](docs/AzureAccountSetUp.md) and
[DeploymentGuide.md](docs/DeploymentGuide.md). A healthy `/api/health` response
proves the web process is running, not that SQL, Foundry, private backend
connectivity or a schedule works.

For command-center access, ask IT or the appropriate Entra group owner to
review membership and app-role assignment. Access & permissions is read-only;
it cannot create users, change membership or identify which group supplied a
role. After a change, Refresh permissions requests a fresh API token. Entra
propagation and previously issued token lifetimes still apply. Do not enable
the retired SQL access-management flag as a workaround.

Group provisioning, directory consent, Azure role assignments, regional
quota/admission, private DNS and Fabric tenant settings require the relevant
administrator. The application does not grant these permissions to itself.
Platform incidents belong with the relevant Azure, Fabric, Microsoft 365 or
Entra support channel under your own service agreement.

The deployment guide also separates code-only App Service updates from
infrastructure changes. Preserve approved ingress, SCM restrictions and
governance NSGs, and reconcile ambiguous deployment writes before retrying.
Neither a code update nor the Entra authorization cutover requires clearing
incidents, approvals, run history or the standalone database.
