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
- [ ] Foundry provider (`TRIAGE_PROVIDER_MODE=foundry`)
- [ ] Hosted agent (`azd ai agent invoke`)

## Reproduce

```powershell
# the exact command
```

## Environment

Output of `triage-demo preflight`. It reports what is configured and what is
missing without printing any secret value.

## Logs

Redact before pasting. Webhook URLs, approval callback URLs and client secrets
are all bearer credentials -- anyone holding one can post to your channel,
answer an approval, or authenticate as the app.

## Does the offline suite pass?

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

If it passes offline and fails live, the difference is tenant configuration.
`docs/provisioning.md` lists what must exist.
