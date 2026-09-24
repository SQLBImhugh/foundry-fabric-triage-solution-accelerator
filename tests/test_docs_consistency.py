"""Tests that documentation still describes the software that exists.

Docs drift silently. Nobody notices a README claiming a command that was
renamed, or a run sheet quoting a scenario count from three commits ago -- until
it is read aloud in front of a customer and the command errors.

These check the claims that are cheap to verify and expensive to get wrong. The
test count is deliberately *not* checked here: asserting it from inside the test
suite is circular, and a number that has to be updated whenever a test is added
trains people to update it without looking.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
from pathlib import Path

import pytest

from triage.cli import build_parser

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Every prose document that makes checkable claims, discovered rather than
#: listed. A hardcoded list silently stops covering the file somebody adds next,
#: which is the same class of defect these tests exist to catch.
DOCS = sorted(
    path
    for path in REPO_ROOT.rglob("*.md")
    if not any(part in {".venv", ".git", "node_modules"} for part in path.parts)
)

CLI_REFERENCE = re.compile(r"bi-triage(?:\.exe)?\s+([a-z][a-z\-]*)")


def _cli_commands() -> set[str]:
    parser = build_parser()
    for action in parser._actions:  # noqa: SLF001 - argparse offers no public API
        if isinstance(action, argparse._SubParsersAction):
            return set(action.choices)
    return set()


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_documented_cli_commands_exist(doc: Path) -> None:
    """Every `bi-triage <command>` in the docs is a real subcommand.

    Catches the rename that nobody propagated, which surfaces as a customer
    watching a command fail.
    """
    commands = _cli_commands()
    # Flags and prose fragments are not commands.
    referenced = {
        name for name in CLI_REFERENCE.findall(_read(doc))
        if not name.startswith("-")
    }
    unknown = referenced - commands
    assert not unknown, f"{doc.name} references non-existent commands: {sorted(unknown)}"


#: Small numbers are spelled out in this repo's prose, so a digits-only check
#: misses the claims most likely to be written. "all seven scenarios" survived
#: two scenario additions unnoticed for exactly this reason.
WORD_NUMBERS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
}


def _as_int(token: str) -> int:
    return int(token) if token.isdigit() else WORD_NUMBERS[token.lower()]


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_scenario_count_claims_match_reality(doc: Path) -> None:
    """A doc claiming "N scenarios" must agree with the scenarios directory."""
    actual = len(list((REPO_ROOT / "scenarios").glob("*.yaml")))
    words = "|".join(WORD_NUMBERS)
    number = rf"(?:\d+|{words})"
    # Only unambiguous claims about the whole suite. A doc may legitimately say
    # "the two scenarios as specified" or "failed two scenarios on run two"
    # without asserting a total, and a check that flags those is one somebody
    # switches off.
    text = _read(doc)
    claimed = {
        _as_int(n)
        for n in re.findall(rf"\b(?:all|of) ({number}) scenarios\b", text, re.I)
    }
    wrong = {n for n in claimed if n != actual}
    assert not wrong, f"{doc.name} claims {sorted(wrong)} scenarios; there are {actual}"


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_internal_doc_links_resolve(doc: Path) -> None:
    """Relative links between docs point at files that exist.

    The pattern deliberately allows a ``#fragment``. An earlier version excluded
    ``#`` from the whole match, so ``](foo.md#section)`` matched nothing at all
    and a link to a deleted document survived a rename with the suite green.
    """
    broken: list[str] = []
    for target in re.findall(r"\]\(([^)\s#]+\.md)(?:#[^)]*)?\)", _read(doc)):
        if target.startswith("http"):
            continue
        resolved = (doc.parent / target).resolve()
        if not resolved.exists():
            # Docs sometimes link as docs/x.md from the root and x.md from docs/.
            alt = (REPO_ROOT / target).resolve()
            if not alt.exists():
                broken.append(target)
    assert not broken, f"{doc.name} has broken links: {broken}"


#: Non-markdown things the docs point at: the README's architecture diagram, the
#: diagrams, templates and scripts the docs point at. The link
#: check above only covers ``.md``, so a renamed image broke nothing visible in
#: the test suite while breaking the first thing a visitor to the repository
#: sees.
LINKED_ASSET = re.compile(r"\]\(([^)#]+\.(?:png|svg|jpg|jpeg|html|yaml|yml|py))\)|src=\"([^\"]+)\"")


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_linked_assets_exist(doc: Path) -> None:
    """Images and pages the docs point at are really there."""
    broken: list[str] = []
    for match in LINKED_ASSET.finditer(_read(doc)):
        target = match.group(1) or match.group(2)
        if not target or target.startswith(("http", "data:", "mailto:")):
            continue
        # Prose showing the *shape* of a path rather than a real one.
        if "..." in target or "<" in target:
            continue
        if not (doc.parent / target).resolve().exists():
            if not (REPO_ROOT / target).resolve().exists():
                broken.append(target)
    assert not broken, f"{doc.name} points at missing assets: {broken}"


# ---------------------------------------------------------------------------
# Drift the audit found: claims that were true once
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: p.name)
def test_terminal_outcome_lists_are_complete(doc: Path) -> None:
    """A doc that *presents the list* of terminal outcomes must present all of it.

    Both `architecture.md` and `faq.md` had listed ten for months while the code
    had twelve -- `approval_denied` and `deferred_retry` arrived with their
    features and nobody went back. A partial list is worse than none: it reads
    as authoritative while omitting the two outcomes a reader most needs to know
    exist.

    Scoped to the `·`-separated block that is the house style for this listing.
    Prose mentioning two or three outcomes in passing is not a listing, and a
    check that flags it is one somebody deletes.
    """
    import typing

    from triage.models import TerminalOutcome

    outcomes = set(typing.get_args(TerminalOutcome))
    # Chunks separated by blank lines, rather than paired code fences: pairing
    # ``` markers goes wrong the moment a document also has a ```python block,
    # which silently made this check pass by examining nothing.
    blocks = [
        chunk
        for chunk in re.split(r"\n\s*\n", _read(doc))
        if "·" in chunk and sum(o in chunk for o in outcomes) >= 4
    ]
    if not blocks:
        pytest.skip("does not present the outcome list")

    for block in blocks:
        missing = sorted(o for o in outcomes if o not in block)
        assert not missing, f"{doc.name} presents the outcome list but omits {missing}"


def test_no_developer_machine_paths_are_committed() -> None:
    """A hardcoded profile path is both a broken script and a leaked username.

    A helper once read `C:\\Users\\<name>\\AppData\\Local\\Temp\\...` at import
    time, so the module could not be imported on any other machine, and the name
    would have shipped in a public repository.
    """
    offenders: list[str] = []
    pattern = re.compile(r"[A-Za-z]:\\+Users\\+(?!<)[A-Za-z0-9._-]+", re.I)
    for path in sorted((REPO_ROOT / "src").rglob("*.py")) + sorted(
        (REPO_ROOT / "scripts").rglob("*.py")
    ):
        for number, line in enumerate(
            path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1
        ):
            if pattern.search(line):
                offenders.append(f"{path.name}:{number}")

    assert not offenders, f"developer-specific paths committed: {offenders}"


def test_infra_templates_do_not_hardcode_an_owner() -> None:
    """An `Owner` tag with a literal name in it is a personal identifier.

    One template shipped an `Owner` tag containing an individual's alias. It is
    only a tag, so nothing breaks, which is why it survived a scrub that grepped
    the source and the docs but not `infra/`.

    Asserting the tag is a parameter reference, rather than grepping for a
    particular name, keeps this from becoming a list of names in a public repo.
    """
    offenders: list[str] = []
    for path in sorted((REPO_ROOT / "infra").glob("*.json")):
        template = json.loads(path.read_text(encoding="utf-8"))
        for resource in template.get("resources", []):
            owner = resource.get("tags", {}).get("Owner")
            if owner is not None and not owner.startswith("[parameters("):
                offenders.append(f"{path.name}: Owner={owner!r}")

    assert not offenders, (
        f"infra templates hardcode an owner: {offenders}. Make it a parameter -- "
        "whoever deploys this is not whoever wrote it."
    )


def test_the_broken_routines_ship_disabled() -> None:
    """A scheduler that silently does nothing must not ship enabled.

    Foundry routines are in preview and do not fire: verified six days after
    registration by three independent checks, most decisively that telemetry
    showed activity in two of twenty-four hours, both of them hours when a
    person invoked the agent by hand.

    Left enabled, an adopter deploys, reads `enabled: true`, and never learns
    that their triage agent has not run since the day they installed it. That is
    the exact class of failure this accelerator exists to detect, so shipping it
    would be self-contradicting.

    The declarations stay because the shape is right and costs nothing while
    disabled. If you have verified they fire in your tenant, enable them there --
    but flipping this file back to `true` without that evidence is what this
    test is here to stop.
    """
    text = (REPO_ROOT / "azure.yaml").read_text(encoding="utf-8")
    routines = re.findall(
        r"^\s{4}([a-z][\w-]*):\n(?:.*\n)*?\s+host:\s*azure\.ai\.routine\n(?:.*\n)*?\s+enabled:\s*(\w+)",
        text,
        re.M,
    )
    assert routines, "no routine declarations found in azure.yaml"

    enabled = [name for name, flag in routines if flag != "false"]
    assert not enabled, (
        f"routines enabled: {enabled}. Foundry routines were last verified not to "
        "fire. Re-verify with `azd ai routine run list <name>` and update the "
        "evidence in azure.yaml and docs/foundry/README.md before enabling."
    )


def test_routine_inputs_reach_a_command_the_agent_handles() -> None:
    """A routine's input word must match a command, or it is triaged as an alert.

    The hosted agent routes on the message text: known words run a mailbox
    sweep or a health sweep, and anything else falls through to "triage this as
    an incoming alert". A typo in `input:` would therefore not fail -- it would
    quietly triage the literal string "silent sweep" as though it were a Power
    BI failure report, and look like it was working.

    The command sets are read out of the source with `ast` rather than imported.
    Importing `app` pulls in agent_framework, which the container has and the
    offline test environment deliberately does not -- so an earlier version of
    this test passed locally and failed every CI run.
    """
    tree = ast.parse((REPO_ROOT / "src" / "app.py").read_text(encoding="utf-8"))
    commands: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            name = getattr(target, "id", None)
            if name in ("_SWEEP_COMMANDS", "_SILENT_COMMANDS", "_PIPELINE_COMMANDS", "_WEB_COMMANDS", "_HEARTBEAT_COMMANDS"):
                # Both are frozenset({...}) rather than bare literals, so unwrap
                # the call before evaluating its argument.
                value = node.value
                if isinstance(value, ast.Call) and getattr(value.func, "id", "") in (
                    "frozenset",
                    "set",
                ):
                    value = value.args[0]
                commands[name] = set(ast.literal_eval(value))

    assert set(commands) == {"_SWEEP_COMMANDS", "_SILENT_COMMANDS", "_PIPELINE_COMMANDS", "_WEB_COMMANDS", "_HEARTBEAT_COMMANDS"}, (
        f"could not read the command sets out of src/app.py, found {sorted(commands)}. "
        "If they were renamed or built dynamically, update this test rather than "
        "letting it pass without checking anything."
    )

    handled = {c.lower() for values in commands.values() for c in values}
    inputs = re.findall(r'^\s+input:\s*"([^"]*)"', (REPO_ROOT / "azure.yaml").read_text(
        encoding="utf-8"), re.M)
    assert inputs, "no routine inputs found in azure.yaml"

    unhandled = [i for i in inputs if i.strip().lower() not in handled]
    assert not unhandled, (
        f"routine inputs no command handles: {unhandled}. These would be triaged "
        f"as alert text instead of running a sweep. Handled: {sorted(handled)}"
    )

    # The scheduler that actually fires carries the same risk in a second file.
    # infra/scheduled-sweep.json constrains `command` with allowedValues, which
    # stops a typo at deployment time -- but only while those values are still
    # commands the controller recognises.
    template = json.loads(
        (REPO_ROOT / "infra" / "scheduled-sweep.json").read_text(encoding="utf-8")
    )
    allowed = template["parameters"]["command"]["allowedValues"]
    assert allowed, "infra/scheduled-sweep.json no longer constrains 'command'"

    unhandled = [c for c in allowed if c.strip().lower() not in handled]
    assert not unhandled, (
        f"scheduled-sweep.json offers commands nothing handles: {unhandled}. A "
        f"Logic App deployed with one would post it every interval and the agent "
        f"would triage the literal string as a Power BI alert. "
        f"Handled: {sorted(handled)}"
    )


def test_every_setting_is_actually_read_somewhere() -> None:
    """A configuration knob nothing consumes is a promise nothing keeps.

    Two shipped this way. `GRAPH_INGESTION_MODE` offered poll or subscription
    and was read by nothing, so setting it to subscription silently kept
    polling. `GRAPH_POLL_SECONDS` was documented as the poll interval while the
    watch loop used its own hardcoded default, so changing it did nothing at
    all. Both were advertised in `.env.example`.

    That is worse than a missing feature: the operator makes a decision, the
    system ignores it, and nothing reports the disagreement.
    """
    import sys

    sys.path.insert(0, str(REPO_ROOT / "src"))
    from triage.settings import Settings

    # scripts/ counts: register_foundry_agents.py legitimately consumes several
    # settings that no runtime module touches.
    sources = [
        f.read_text(encoding="utf-8", errors="ignore")
        for root in ("src", "scripts")
        for f in (REPO_ROOT / root).rglob("*.py")
        if f.name != "settings.py"
    ]
    blob = "\n".join(sources)

    unread = [name for name in Settings.model_fields if name not in blob]
    assert not unread, (
        f"settings nothing reads: {sorted(unread)}. Either consume them or remove "
        "them -- an operator who sets one and sees no effect has no way to tell "
        "the difference between a broken feature and a misunderstood one."
    )


def test_the_hosting_library_stays_pinned_to_an_exact_version() -> None:
    """The container's hosting library must be pinned, not floated.

    agent-framework-foundry-hosting publishes date-stamped betas that make
    breaking changes without a major bump. With a `>=1.0.0b1` floor, the
    2026-09-03 release changed the default `history_source` to 'agent_server',
    which refuses to construct against this repo's custom SupportsAgentRun
    implementation. The next rebuild picked it up and the deployed container
    crash-looped at startup, answering nothing until someone invoked it by hand.

    Loosening this pin re-arms that failure, so it fails the suite instead.
    """
    requirements = (REPO_ROOT / "src" / "requirements.txt").read_text(
        encoding="utf-8"
    )
    pins = [
        line.strip()
        for line in requirements.splitlines()
        if line.strip().startswith("agent-framework-foundry-hosting")
    ]
    assert pins, "agent-framework-foundry-hosting is missing from src/requirements.txt"
    assert all("==" in pin for pin in pins), (
        f"the hosting library must be pinned exactly, found: {pins}. A floating "
        "beta took the deployed agent down at startup once already. Bump the pin "
        "deliberately and redeploy instead of removing it."
    )


def test_env_example_and_settings_agree_exactly() -> None:
    """`.env.example` is the only configuration reference an adopter reads.

    Two failure modes, both silent. A key advertised here that no `Settings`
    field backs is a knob the operator sets and nothing reads. A field that
    exists but is not advertised is a capability nobody discovers, and the
    inbox sender allowlist and subject pattern -- the controls that stop the
    agent being steerable by anyone who can email it -- were both in that
    second category.

    Checked in both directions, because only one direction was ever wrong at a
    time and the other kept passing.
    """
    import sys

    sys.path.insert(0, str(REPO_ROOT / "src"))
    from triage.settings import Settings

    advertised = set(
        re.findall(r"^([A-Z][A-Z0-9_]*)=", _read(REPO_ROOT / ".env.example"), re.M)
    )
    supported = {name.upper() for name in Settings.model_fields}
    from triage.command_center.models import WebSettings

    supported |= {f"COMMAND_CENTER_{name.upper()}" for name in WebSettings.model_fields}

    assert not advertised - supported, (
        f"advertised in .env.example but no Settings field reads them: "
        f"{sorted(advertised - supported)}. Remove them, or implement them."
    )
    assert not supported - advertised, (
        f"real settings missing from .env.example: {sorted(supported - advertised)}. "
        "An undocumented knob is one nobody finds."
    )


def test_the_contributor_contract_is_delivered_identically_to_both_paths() -> None:
    """`AGENTS.md` and `.github/copilot-instructions.md` must be byte-identical.

    Two paths are needed: Copilot loads the `.github` one automatically, and
    other tools and people look for `AGENTS.md`. Two copies is the only way to
    serve both, so the copies are pinned equal rather than trusted to stay equal.

    This has failed twice in different ways. First as two real copies that
    diverged -- three safety invariants, including "a denial must not consume the
    remediation budget", went missing from one while the other kept them, so
    whichever file an agent happened to read became the whole contract. Then as a
    pointer at one path, which stopped the drift by delivering no contract at all
    to anything reading only `AGENTS.md`.
    """
    agents = (REPO_ROOT / "AGENTS.md").read_bytes()
    copilot = (REPO_ROOT / ".github" / "copilot-instructions.md").read_bytes()

    assert agents == copilot, (
        "AGENTS.md and .github/copilot-instructions.md have diverged. They must be "
        "byte-for-byte identical: edit one and copy it over the other. A reader "
        "gets exactly one of these files, and it has to be the whole contract."
    )


#: Names that belonged to the repository this accelerator was extracted from.
#: Each maps to what it should be now, so the failure message tells you the fix.
#:
#: This exists because a rename was run, and then more files were copied in
#: afterwards. `.github/` never went through the transform, so the issue template
#: still asked for the output of a command that no longer exists, and pointed at
#: a document that had been renamed. Every individual test passed.
FORBIDDEN_TOKENS = {
    "triage_demo": "the package is `triage`",
    "triage-demo": "the CLI is `bi-triage`",
    "docs/provisioning.md": "renamed to docs/DeploymentGuide.md",
    "docs/operations.md": "renamed to docs/OperationsGuide.md",
    "docs/architecture.md": "renamed to docs/TechnicalArchitecture.md",
    "docs/customization.md": "renamed to docs/CustomizationGuide.md",
    "docs/hosted-architecture.md": "renamed to docs/foundry/README.md",
    "foundry-native-architecture": "that document is not part of the accelerator",
    "docs/history": "project history is not part of the accelerator",
    "demo/scripts": "presenter scripts are not part of the accelerator",
    "demo/walkthrough": "the walkthrough is not part of the accelerator",
    "BITriageDemo": "use a <resource-group> placeholder",
}


def _tracked_files() -> list[Path]:
    """Every file git would publish, so the check covers what a reader receives."""
    import subprocess

    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=str(REPO_ROOT), capture_output=True, text=True, check=True,
    )
    return [REPO_ROOT / rel for rel in result.stdout.split("\0") if rel]


def test_no_file_refers_to_the_repository_this_was_extracted_from() -> None:
    """Nothing published may name a path, package or command that no longer exists.

    Scoped to tracked files of a text kind, and skips this file, which has to
    contain the tokens in order to look for them.
    """
    text_suffixes = {".py", ".md", ".yaml", ".yml", ".json", ".toml", ".txt", ".cfg", ""}
    offenders: list[str] = []

    for path in _tracked_files():
        if path.name == Path(__file__).name or path.suffix.lower() not in text_suffixes:
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for number, line in enumerate(content.splitlines(), start=1):
            for token, remedy in FORBIDDEN_TOKENS.items():
                if token in line:
                    rel = path.relative_to(REPO_ROOT)
                    offenders.append(f"{rel}:{number} has {token!r} -- {remedy}")

    assert not offenders, "stale references to the source repository:\n" + "\n".join(
        f"  {item}" for item in offenders
    )


def _approval_workflows() -> tuple[dict, dict]:
    """Return (recording, confirmation) workflow definitions from the template."""
    template = json.loads(
        (REPO_ROOT / "infra" / "approval-callback.json").read_text(encoding="utf-8")
    )
    workflows = [
        r for r in template["resources"] if r["type"] == "Microsoft.Logic/workflows"
    ]
    assert len(workflows) == 2, "expected a recording workflow and a confirmation one"

    by_method = {}
    for wf in workflows:
        trigger = wf["properties"]["definition"]["triggers"]["manual"]["inputs"]
        by_method[trigger.get("method", "POST").upper()] = wf["properties"]["definition"]

    assert "GET" in by_method and "POST" in by_method, (
        f"expected one GET workflow and one POST workflow, got {sorted(by_method)}"
    )
    return by_method["POST"], by_method["GET"]


def test_no_workflow_branches_on_a_trigger_method() -> None:
    """`triggerOutputs()['method']` does not exist, and silently fails the run.

    This is the test that was missing. An earlier version of the callback was a
    single workflow that rendered a confirmation page on GET and wrote on POST,
    branching on `toupper(triggerOutputs()['method'])`. A Consumption Request
    trigger exposes only `headers`, `queries` and `body` -- there is no `method`
    -- so that expression fails at run time with `InvalidTemplate` and the whole
    run dies before reaching either branch. A GET never even got that far: the
    trigger accepts one method, and rejects anything else with
    `TriggerRequestMethodNotValid` before the workflow starts.

    The old test asserted that the template *contained* that expression, so it
    passed for as long as the feature was broken. Shape is not behaviour.
    """
    template = json.loads(
        (REPO_ROOT / "infra" / "approval-callback.json").read_text(encoding="utf-8")
    )
    definitions = json.dumps(
        [
            r["properties"]["definition"]
            for r in template["resources"]
            if r["type"] == "Microsoft.Logic/workflows"
        ]
    )
    assert "triggerOutputs()['method']" not in definitions, (
        "a Request trigger has no 'method' property; branching on it fails the run"
    )


def test_the_workflow_a_link_preview_reaches_cannot_change_anything() -> None:
    """A link in a Teams message is fetched by things that are not people.

    Preview generators, link scanners and prefetchers all issue GET. The card's
    Action.OpenUrl link therefore has to point at a workflow with nothing to
    change -- not at one that decides whether to change something.
    """
    _recording, confirmation = _approval_workflows()

    actions = confirmation["actions"]
    kinds = {a.get("type") for a in actions.values()}
    assert kinds == {"Response"}, (
        f"the GET workflow has non-Response actions {sorted(kinds)}; it must only render"
    )
    assert "$connections" not in confirmation.get("parameters", {}), (
        "the GET workflow holds a connection, so it is capable of writing"
    )
    assert "ApiConnection" not in json.dumps(confirmation)


def test_the_recording_workflow_is_not_reachable_by_get() -> None:
    """Only the POST side writes, and it must not be widened to accept GET."""
    recording, _confirmation = _approval_workflows()

    trigger = recording["triggers"]["manual"]["inputs"]
    assert trigger.get("method", "POST").upper() == "POST", (
        "the recording workflow accepts GET, so a link preview could write"
    )


def test_the_decision_is_a_parameterised_procedure_call() -> None:
    """Everything in the callback URL is attacker-controllable.

    Anyone holding the link can edit the request id, decision and responder, so
    they are procedure parameters. A query assembled from them by string
    concatenation would be an injection point reachable by anyone who can read
    a Teams channel.
    """
    recording, _confirmation = _approval_workflows()
    record = recording["actions"]["Record_the_decision"]["inputs"]

    assert "/procedures/" in record["path"], "the write is not a stored procedure call"
    assert "/query/sql" not in record["path"], "the write runs raw SQL"
    for field in ("request_id", "decision", "responder", "fingerprint"):
        assert field in record["body"], f"{field} is not passed as a parameter"


def test_the_decision_write_is_conditional_and_checked() -> None:
    """Zero rows changed is a refusal, and must be rendered as one.

    The procedure moves the row from unanswered to answered in one statement, so
    a second click matches nothing. If the workflow ignored the row count it
    would tell the second clicker their decision was recorded when the first
    one still stands.
    """
    recording, _confirmation = _approval_workflows()
    branch = recording["actions"]["Was_it_recorded"]

    expression = json.dumps(branch["expression"])
    assert "recorded" in expression, "the workflow does not check the row count"
    assert "Refuse" in json.dumps(branch["else"]), "there is no refusal branch"
    assert "Could_not_reach_the_database" in recording["actions"], (
        "a failed write has no branch, so it would fall through as success"
    )


def test_the_scheduler_waits_longer_than_the_approval_window() -> None:
    """The stored-response polling window, not one HTTP call, covers approval."""
    import re
    import sys

    sys.path.insert(0, str(REPO_ROOT / "src"))
    from triage.settings import Settings

    template = json.loads(
        (REPO_ROOT / "infra" / "scheduled-sweep.json").read_text(encoding="utf-8")
    )
    timeout = (
        template["resources"][0]["properties"]["definition"]["actions"]
        ["Wait_for_agent"]["limit"]["timeout"]
    )
    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", timeout)
    assert match, f"unparsable timeout {timeout!r}"
    hours, minutes, seconds = (int(part or 0) for part in match.groups())
    budget = hours * 3600 + minutes * 60 + seconds

    settings = Settings()
    approval = settings.approval_timeout_seconds
    assert budget > approval, (
        f"the scheduler gives up after {budget}s but an approval may take "
        f"{approval}s. Raise the Logic App timeout, or lower "
        "APPROVAL_TIMEOUT_SECONDS -- they have to move together."
    )
    command_budget = settings.triage_timeout_seconds + approval + 30
    assert budget > command_budget, (
        f"The scheduler timeout {budget}s must outlast the combined command-worker "
        f"execution deadline of {command_budget}s, including time awaiting approval."
    )
    failure_card = json.dumps(
        template["resources"][0]["properties"]["definition"]["actions"]["Did_it_fail"]
    )
    assert "may have executed" in failure_card
    assert "Nothing was triaged" not in failure_card


def test_command_center_public_network_defaults_keep_authentication_and_bounded_rule_descriptions() -> None:
    import re

    template = (REPO_ROOT / "infra" / "command-center.bicep").read_text(encoding="utf-8")
    assert "outboundVnetRouting:" not in template
    assert "virtualNetworkSubnetId:" not in template
    assert "publicNetworkAccess: 'Enabled'" in template
    assert "httpsOnly: true" in template
    assert template.count("allow: false") == 2
    helper = (REPO_ROOT / "scripts" / "deploy_command_center.ps1").read_text(encoding="utf-8")
    for name in ("integrationSubnetNsgId", "privateEndpointSubnetNsgId"):
        assert name not in template and name not in helper
    for name in ("clientAccessRules", "scmAccessRules"):
        rules = template.split(f"var {name} =", 1)[1].split("\n}]", 1)[0]
        description = re.search(r"description: '([^']*)'", rules)
        assert description is not None
        assert len(description.group(1)) <= 64
    assert "properties.publicNetworkAccess=Disabled" not in helper


def test_scheduler_rejects_failed_responses_even_when_http_succeeds() -> None:
    template = json.loads(
        (REPO_ROOT / "infra" / "scheduled-sweep.json").read_text(encoding="utf-8")
    )
    actions = template["resources"][0]["properties"]["definition"]["actions"]
    assert actions["Invoke_the_agent"]["inputs"]["body"] == {
        "input": "@{parameters('command')}",
        "background": True, "store": True, "stream": False,
    }
    check = actions["Validate_agent_response"]
    assert check["type"] == "ParseJson"
    assert check["runAfter"] == {"Wait_for_agent": ["Succeeded"]}
    assert check["inputs"]["content"] == "@variables('Agent_response')"
    schema = check["inputs"]["schema"]
    assert "status" in schema["required"]
    assert schema["properties"]["status"]["enum"] == ["completed"]
    assert schema["properties"]["error"]["type"] == "null"
    after = actions["Did_it_fail"]["runAfter"]
    assert set(after["Invoke_the_agent"]) == {"Succeeded", "Failed", "TimedOut"}
    assert set(after["Validate_agent_response"]) == {"Failed", "TimedOut", "Skipped"}
    assert actions["Fail_the_run"]["inputs"]["runStatus"] == "Failed"
    assert set(actions["Fail_the_run"]["runAfter"]["Did_it_fail"]) == {
        "Succeeded", "Failed", "TimedOut",
    }
