# Contributor and agent contract

Read this before changing anything.

This file exists at two paths: `AGENTS.md` for tools and people that look there,
and `.github/copilot-instructions.md` because Copilot loads that one
automatically. **They are byte-for-byte identical and a test enforces it.** Edit
either and copy it over the other.

The repository previously carried a pointer at one path and the contract at the
other, which delivered nothing to a tool reading only `AGENTS.md`. Before that it
carried two real copies, and they diverged: three safety invariants, including
"a denial must not consume the remediation budget", went missing from one while
the other kept them.

## What this is

A multi-agent triage loop for Power BI failures on Azure AI Foundry, published
as a public MIT-licensed **solution accelerator**. It is sample code, not a
supported product. Optimise for legibility and for being able to explain any line
of it out loud, over cleverness.

It runs **fully offline** with mock providers and mock tools. Keep it that way:
that is how it is evaluated and how the tests run.

## Commands

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"

.\.venv\Scripts\python.exe -m pytest -q                  # the offline suite -- no network
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe scripts\scan_secrets.py       # the CI credential gate
.\.venv\Scripts\bi-triage.exe run scenario1-transient
.\.venv\Scripts\bi-triage.exe identity --check-scope     # who the agents are

azd deploy bi-triage-controller --no-prompt              # hosted controller
azd ai agent invoke bi-triage-controller "sweep"
azd ai agent monitor bi-triage-controller
```

## Rules that are not negotiable

1. **Tests never touch the network.** No live calls, no credentials, no Azure. A
   test that needs a tenant is not a test.
2. **Policy is enforced in the controller, not the prompt.** New limits go in
   `PolicyLedger` with a test proving they fire. A limit that exists only as
   prompt wording is not a limit.
3. **Every tool is on an allowlist** (`REMEDIATION_ACTIONS`, `REPORTING_ACTIONS`
   or `DIAGNOSTIC_ACTIONS`). Anything else is refused before dispatch. That is
   the property the whole design rests on.
4. **An approval is only a yes if it is explicit, fingerprint-matched, unexpired
   and unused.** Everything else — timeout, error, malformed reply, no gate
   configured — is a no. Silence never reads as consent.
5. **A denial must not consume the remediation budget.** Otherwise one "no"
   silently disarms the agent for the rest of the incident.
6. **Deterministic evidence outranks model output.** If a model claim and a scan
   disagree, the scan wins and the disagreement is logged.
7. **Redaction stays inside the store boundary.** Never move it to call sites; a
   call site can forget.
8. **Every terminal outcome is persisted**, including crashes and refusals.
9. **Spans carry metadata only.** Never attach prompt or completion content.
10. **The inbox filter is a security control.** An agent that acts on every
    message is steerable by anyone who can email it. Never widen the filter to
    make something trigger — send a matching message instead.
11. **Identity claims are read from the directory, never configured.** A
    hardcoded claim about security posture will eventually be false silently.
12. **Grant permissions to the component that acts, not the one that reasons.**
    The prompt agents hold nothing. If a reasoning agent seems to need a
    permission, the design is wrong.
13. **Tools fail loudly rather than return something interpretable.** Calling
    Power BI with an empty id returned 404, and the model turned that into a
    confident, wrong conclusion.
14. **Scenarios are reproducible.** Same input, same tool sequence, same numbers.
    When a live model and the mock diverge, make the *controller* decide so both
    agree.
15. **Never weaken a policy limit to make a scenario pass** — change the
    scenario.
16. **State that must survive an invocation goes in a store, never on an
    object.** A hosted agent is rebuilt for every request, so an instance
    attribute is always empty on arrival.
17. **An incident is announced once, not once per occurrence.** Enforced in the
    controller against `notified_count`, never in prompt wording.
18. **Pin the hosting library exactly.** `agent-framework-foundry-hosting` ships
    date-stamped betas that make breaking changes without a major bump; a
    floating floor once crash-looped the deployed container at startup, and
    nothing reported it.

## Style

- `from __future__ import annotations` at the top of every module
- `str | None`, not `Optional[str]`
- Pydantic models for anything crossing an agent boundary
- Per-module loggers: `logging.getLogger("triage.<module>")`
- Comment the **why**, not the what — especially where a design choice traces to
  a real production failure. Those comments are the most valuable content in the
  repository
- Printed output stays ASCII where practical; Windows consoles mangle the rest

## Adding things

- **A remediation tool**: schema in `TRIAGE_TOOLS` → branch in
  `ToolDispatcher._execute` → name in an action allowlist → a scenario → a test.
- **An agent**: mirror `DataQualityAgent`. Own provider, prompt and tools,
  returns a typed Pydantic model, exposed to the orchestrator as one tool. It
  reports; the orchestrator decides.
- **A scenario**: a YAML in `scenarios/` with an `expect` block, added to the
  parametrized list in `tests/test_scenarios.py`. The `expect` block is the test.
- **A playbook**: a `Playbook` in `knowledge/playbooks.py` with triggers, a
  `retry_useful` verdict and a **public** Microsoft Learn source, then a test
  that it fires on realistic error text. Retrieval is capped at 3 — if a new
  entry matters more than an existing one, raise its trigger specificity rather
  than the cap.
- **A prompt change**: prompts are hashed onto every incident. If running in
  Foundry mode, **re-register the agents** or the change has no effect.

> **Sourcing rule.** Playbook content must come from public documentation. This
> repository is shared publicly, so internal engineering guides may inform *what
> matters* but must not be quoted or cited. A test enforces this.

## Never

- Commit a filled-in `.env`, a webhook URL, or any credential
- Add a network call to the default test path
- Put a customer's name, or an internal project name, in a committed file
- Claim a capability without verifying it against the platform first
