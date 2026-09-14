---
name: Bug report
about: Something behaves differently from what the docs say
labels: bug
---

## What happened

## What you expected

## Which mode
- [ ] Offline (`TRIAGE_PROVIDER_MODE=mock`, `TRIAGE_TOOL_MODE=mock`)
- [ ] Live tools (`TRIAGE_TOOL_MODE=live`)
- [ ] Direct provider (`TRIAGE_PROVIDER_MODE=direct`)
- [ ] Foundry provider (`TRIAGE_PROVIDER_MODE=foundry`)
- [ ] Hosted agent (`azd ai agent invoke`)

## Affected surface
- [ ] Power BI refresh triage or silent-failure detection
- [ ] Configured Fabric pipeline triage
- [ ] Read-only Fabric cockpit (`cockpit`)
- [ ] Azure-hosted command center (`command-center`)

## Reproduce

```powershell
# the exact command
```

## Environment

Output of `bi-triage preflight`. It reports what is configured and what is
missing without printing any secret value. Review it before posting and redact
mailbox addresses and live tenant, workspace, item or resource identifiers.
For frontend issues, include the affected app, browser, route and build/commit.

## Logs

Redact before pasting. Webhook URLs, approval callback URLs and client secrets
are all bearer credentials -- anyone holding one can post to your channel,
answer an approval, or authenticate as the app.
Do not attach access tokens, a filled-in `.env`, private incident content or
unredacted screenshots. For command-center permission issues, report the
displayed role names and token freshness, not the token itself.

## Does the offline suite pass?

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

An offline pass does not prove that live integration works. Tenant configuration,
permissions, network access, provider responses or integration code can differ.
See the [deployment guide](../../docs/DeploymentGuide.md) and, for web issues,
the [command-center guide](../../docs/CommandCenter.md).
