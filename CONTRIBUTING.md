# Contributing

Contributions are welcome. There is no Contributor License Agreement: by opening
a pull request you agree that your contribution is licensed under the repository
[MIT licence](LICENSE).

## Before you open a pull request

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe scripts\scan_secrets.py
```

All three must pass. The test suite runs entirely offline, and a change that
makes a test need credentials or a tenant will fail CI.

## What the review will look for

[`AGENTS.md`](AGENTS.md) is the contract. The parts that most often come back
with comments:

- **A new limit belongs in `PolicyLedger` with a test proving it fires.** A limit
  that exists only as prompt wording is not a limit.
- **A new tool belongs on an action allowlist.** Anything not on one is refused
  before dispatch, and that is the property the whole design rests on.
- **A new test needs a negative control.** Break the thing it guards and watch it
  fail, then restore. A check that has never failed is not a check.
- **Comment the why, not the what**, especially where a design choice traces to a
  real failure. Those comments are the most valuable content in this repository.

## Reporting bugs

Use GitHub Issues, and search first. If it fails against a tenant but passes
offline, say so — the difference is almost always tenant configuration, and
[`docs/DeploymentGuide.md`](docs/DeploymentGuide.md) lists what must exist.

For anything security-sensitive, follow [`SECURITY.md`](SECURITY.md) instead of
opening an issue.
