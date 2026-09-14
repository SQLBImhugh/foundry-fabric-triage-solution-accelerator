## What changed

## Why

## Verification

- [ ] `python -m pytest -q` passes offline, with no credentials and no tenant
- [ ] `python -m ruff check .` clean
- [ ] `python scripts\scan_secrets.py` passes
- [ ] No network call added to the default test path
- [ ] No credential, webhook URL or callback URL committed
- [ ] For frontend changes, the affected app's tests, lint and build pass

Run frontend checks from `cockpit` or `command-center`, as applicable:
`npm test`, `npm run lint`, and `npm run build:fabric` (cockpit) or
`npm run build` (command center). State which app was checked. A Fabric
deployment is not part of the offline test suite.

State what you **verified**, not what you intended. Exit code 0 is not proof of
success -- a run can exit clean having done nothing.

## If this touches the safety surface

- [ ] New tool is on exactly one action allowlist, chosen deliberately
- [ ] New limit lives in `PolicyLedger` with a test proving it fires
- [ ] Preconditions are enforced in the dispatcher, not in prompt wording
- [ ] A denied approval does not consume remediation budget
- [ ] A **negative control** exists: disabling the guard fails a named test

Which named test fails when the guard is removed?

## If this touches the command center or durable state

- [ ] Entra app-role claims remain the application permission authority
- [ ] Read-only questions and user resolutions cannot authorize remediation or reset its budget
- [ ] State remains in the standalone Fabric SQL database, independent of app lifetime
- [ ] Durable-state changes have separate live evidence as well as offline tests, with sensitive values redacted

## If this changes a prompt or a tool schema

- [ ] For Foundry deployments, affected agents re-registered (`python scripts\register_foundry_agents.py`)

A Foundry-registered agent does not pick up local prompt or tool changes.
Without re-registering, the change has no effect and the run looks unaltered.

## If this changes docs

- [ ] Counts, commands and dates still accurate
- [ ] Relative links, heading anchors and linked assets resolve
- [ ] Any renamed file's inbound links updated
- [ ] The read-only Fabric cockpit is not confused with the Azure-hosted command center
