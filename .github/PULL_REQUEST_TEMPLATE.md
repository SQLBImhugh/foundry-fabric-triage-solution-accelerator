## What changed

## Why

## Verification

- [ ] `python -m pytest -q` passes offline, with no credentials and no tenant
- [ ] `python -m ruff check .` clean
- [ ] No network call added to the default test path
- [ ] No credential, webhook URL or callback URL committed

State what you **verified**, not what you intended. Exit code 0 is not proof of
success -- a run can exit clean having done nothing.

## If this touches the safety surface

- [ ] New tool is on exactly one action allowlist, chosen deliberately
- [ ] New limit lives in `PolicyLedger` with a test proving it fires
- [ ] Preconditions are enforced in the dispatcher, not in prompt wording
- [ ] A **negative control** exists: disabling the guard fails a named test

Which named test fails when the guard is removed?

## If this changes a prompt or a tool schema

- [ ] Agents re-registered (`python scripts/register_foundry_agents.py`)

A Foundry-registered agent does not pick up local prompt or tool changes.
Without re-registering, the change has no effect and the run looks unaltered.

## If this changes docs

- [ ] Counts, commands and dates still accurate
- [ ] Any renamed file's inbound links updated
