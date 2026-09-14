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

For changes under `command-center/`, also run its offline tests, lint and
production build (including TypeScript and packaged-asset checks):

```powershell
npm --prefix .\command-center test
npm --prefix .\command-center run lint
npm --prefix .\command-center run build
```

CI runs the Python offline suite and credential gate, plus a separate
command-center job with the locked npm dependencies, Node 22, UI tests,
TypeScript, ESLint and production asset verification.

Documentation checks live in `tests/test_docs_consistency.py`. Keep `AGENTS.md`
and `.github/copilot-instructions.md` byte-for-byte identical when changing the
contributor contract.

## What the review will look for

[`AGENTS.md`](AGENTS.md) is the contract. The parts that most often come back
with comments:

- **A new limit belongs in `PolicyLedger` with a test proving it fires.** A limit
  that exists only as prompt wording is not a limit.
- **A new tool belongs on an action allowlist.** Anything not on one is refused
  before dispatch. Pipeline requests also have a workload-specific allowlist;
  approval never widens it.
- **Human command-center permissions belong in Entra.** Do not add SQL-backed
  human permission grants, a
  browser-owned actor or a runtime group lookup. Keep Operator and Approver
  separate, and do not confuse app Admin with directory administration.
- **Human closure must not rewrite controller evidence.** Incident notes,
  resolutions and observer discussion are append-only. A resolution names the
  current source revision and does not reset an action budget.
- **Prompt and tool-schema edits require Foundry re-registration.** Local files
  do not update a registered agent. Retain tool-free interpretation for the data
  quality agent and command-center observer.
- **A new test needs a negative control.** Break the thing it guards and watch it
  fail, then restore. A check that has never failed is not a check.
- **Comment the why, not the what**, especially where a design choice traces to a
  real failure. Those comments are the most valuable content in this repository.

## Reporting bugs

Use GitHub Issues, and search first. If it fails against a tenant but passes
offline, report that distinction and the sanitized error. Offline fakes do not
prove live identity, network or SQL behavior. The
[`deployment guide`](docs/DeploymentGuide.md) lists prerequisites; the
[`command-center guide`](docs/CommandCenter.md) covers Entra access and web
operations. Do not include tokens, filled-in environment files, live identifiers
or private incident content in an issue.

For anything security-sensitive, follow [`SECURITY.md`](SECURITY.md) instead of
opening an issue.
