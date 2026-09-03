"""Register the demo's Foundry agents, tools and guardrail from source.

Run this before any demo in `foundry` mode. It is idempotent: it hashes the
local prompt + tool schema, compares against the registered definition, and
posts a new version only when something actually changed.

Why this script exists
----------------------
In `mock` and `direct` mode the local files ARE the agent — a prompt edit takes
effect on the next process start. In `foundry` mode the agent definition lives
in the Foundry control plane, and a local edit changes nothing until it is
pushed. That gap silently ships a stale agent: the tool is added, the tests pass
against the local path, and the registered agent goes on running the previous
definition.

Treat "did I re-register?" as part of the deploy checklist, not a detail.

Usage
-----
    az login
    python scripts/register_foundry_agents.py --dry-run
    python scripts/register_foundry_agents.py
    python scripts/register_foundry_agents.py --with-guardrail
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from triage.prompts import load_prompt  # noqa: E402
from triage.settings import Settings  # noqa: E402
from triage.tools.registry import TRIAGE_TOOLS  # noqa: E402

API_VERSION = "v1"
SCOPE = "https://ai.azure.com"

# Read through Settings rather than os.environ so this script sees the same .env
# the application does. It previously read raw environment variables behind a
# hardcoded endpoint default, which meant it appeared to work while ignoring
# .env entirely -- the default was doing the work, not the configuration.
_settings = Settings()
DEFAULT_ENDPOINT = _settings.foundry_project_endpoint
DEFAULT_MODEL = _settings.foundry_agent_model


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:12]


def _strip_nulls(obj: Any) -> Any:
    """Drop null-valued keys at every level.

    The control plane echoes a definition back with its own defaults filled in
    as nulls -- every function tool comes back carrying ``"strict": null``,
    which nothing local ever sets. An earlier version of this stripped those
    only at the top level of the definition, so the tool list always differed
    and the triage agent was given a new version on *every* run of this script,
    whether or not anything had changed. It reached version 9 that way while
    the data quality agent, which has no tools, sat correctly at version 3.

    That is worse than cosmetic. Version churn destroys the signal this script
    exists to provide: if it always says "changed", nobody can tell when
    something actually did.

    Applied to both sides, so a key the server reports as null compares equal to
    one that is absent. A key the server reports with a *real* value still
    differs from one that is absent -- which is what catches a field being
    removed locally while a stale version keeps serving it.
    """
    if isinstance(obj, dict):
        return {k: _strip_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_strip_nulls(v) for v in obj]
    return obj


def definitions_match(local: dict[str, Any], remote: dict[str, Any]) -> bool:
    """True when a registered definition already matches the local one."""
    return _hash(_strip_nulls(local)) == _hash(_strip_nulls(remote))


def _token() -> str:
    out = subprocess.run(
        ["az", "account", "get-access-token", "--resource", SCOPE,
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True, shell=True,
    )
    return out.stdout.strip()


def _to_function_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert our chat-completions tool schemas to the Foundry agent shape.

    Chat completions nests under ``function``; the Foundry agent definition
    wants the **flat** Responses-style shape with ``name`` at the top level.
    Sending the nested form fails with:
        Invalid payload: Required property 'name' is missing
        param: definition.tools[0].name
    """
    out: list[dict[str, Any]] = []
    for t in tools:
        fn = t.get("function") or {}
        name = fn.get("name") or t.get("name")
        if not name:
            continue
        out.append(
            {
                "type": "function",
                "name": name,
                "description": fn.get("description", t.get("description", "")),
                "parameters": fn.get("parameters")
                or t.get("parameters")
                or {"type": "object", "properties": {}, "required": []},
            }
        )
    return out


# ---------------------------------------------------------------------------
# agent definitions
# ---------------------------------------------------------------------------


def build_definitions(model: str) -> dict[str, dict[str, Any]]:
    # NOTE: no `temperature`. Reasoning models such as gpt-5.6-luna reject it
    # outright — "Unsupported parameter: 'temperature' is not supported with
    # this model" — and it is set on the AGENT definition, so the failure
    # surfaces later at invoke time rather than at registration. Determinism
    # here comes from the controller and the deterministic scan, not sampling.
    return {
        "triage": {
            "kind": "prompt",
            "model": model,
            "instructions": load_prompt("triage_system.md"),
            "tools": _to_function_tools(TRIAGE_TOOLS),
        },
        "data_quality": {
            "kind": "prompt",
            "model": model,
            "instructions": load_prompt("data_quality_system.md"),
            # The Data Quality agent's scan runs in the orchestrator and its
            # evidence is passed in, so the agent itself needs no tools. That
            # is deliberate: the numbers must come from a deterministic scan,
            # not from something the model can talk itself out of.
            "tools": [],
        },
    }


# ---------------------------------------------------------------------------
# REST
# ---------------------------------------------------------------------------


def get_agent(client, endpoint: str, name: str) -> dict[str, Any] | None:
    resp = client.get(f"{endpoint}/agents/{name}", params={"api-version": API_VERSION})
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def put_agent(client, endpoint: str, name: str, definition: dict) -> dict:
    """Create or version an agent.

    The control plane exposes agent versions as a POST collection, not a PUT on
    the agent itself — a PUT to /agents/{name} returns 405.
    """
    resp = client.post(
        f"{endpoint}/agents/{name}/versions",
        params={"api-version": API_VERSION},
        json={"definition": definition},
    )
    if resp.status_code >= 400:
        print(f"    !! {resp.status_code}: {resp.text[:400]}", file=sys.stderr)
    resp.raise_for_status()
    return resp.json()


def sync_agent(client, endpoint: str, name: str, definition: dict, *, dry_run: bool) -> str:
    local = _hash(definition)
    existing = get_agent(client, endpoint, name)

    if existing is not None:
        latest = (existing.get("versions") or {}).get("latest") or {}
        remote_def = latest.get("definition") or {}
        if definitions_match(definition, remote_def):
            return f"  {name}: in sync (version {latest.get('version', '?')}, {local})"

    if dry_run:
        state = "does not exist" if existing is None else "differs from local"
        return f"  {name}: WOULD POST a new version ({state}, local={local})"

    created = put_agent(client, endpoint, name, definition)
    version = created.get("version") or (
        (created.get("versions") or {}).get("latest") or {}
    ).get("version", "?")
    return f"  {name}: registered version {version} ({local})"


# ---------------------------------------------------------------------------
# guardrail
# ---------------------------------------------------------------------------

GUARDRAIL_BODY = {
    "properties": {
        "basePolicyName": "Microsoft.Default",
        "mode": "Blocking",
        "contentFilters": [
            {"name": "Hate", "enabled": True, "blocking": True,
             "severityThreshold": "Medium", "source": "Prompt"},
            {"name": "Hate", "enabled": True, "blocking": True,
             "severityThreshold": "Medium", "source": "Completion"},
            {"name": "Violence", "enabled": True, "blocking": True,
             "severityThreshold": "Medium", "source": "Prompt"},
            {"name": "Violence", "enabled": True, "blocking": True,
             "severityThreshold": "Medium", "source": "Completion"},
            # The two that matter for this scenario. The agent reads email,
            # which is attacker-influenceable text, so indirect prompt
            # injection is the realistic attack — not someone typing a
            # jailbreak into a chat box.
            {"name": "Jailbreak", "enabled": True, "blocking": True, "source": "Prompt"},
            {"name": "Indirect Attack", "enabled": True, "blocking": True, "source": "Prompt"},
        ],
    }
}


def sync_guardrail(*, account: str, resource_group: str, name: str, dry_run: bool) -> str:
    sub = subprocess.run(
        ["az", "account", "show", "--query", "id", "-o", "tsv"],
        capture_output=True, text=True, check=True, shell=True,
    ).stdout.strip()

    url = (
        f"https://management.azure.com/subscriptions/{sub}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.CognitiveServices/accounts/{account}/raiPolicies/{name}"
        f"?api-version=2024-10-01"
    )
    if dry_run:
        return f"  guardrail {name}: WOULD PUT ({len(GUARDRAIL_BODY['properties']['contentFilters'])} filters)"

    body_path = REPO_ROOT / "runs" / "_guardrail.json"
    body_path.parent.mkdir(parents=True, exist_ok=True)
    body_path.write_text(json.dumps(GUARDRAIL_BODY), encoding="utf-8")
    try:
        out = subprocess.run(
            ["az", "rest", "--method", "PUT", "--url", url, "--body", f"@{body_path}"],
            capture_output=True, text=True, shell=True,
        )
        if out.returncode != 0:
            return f"  guardrail {name}: FAILED — {out.stderr.strip()[:300]}"
        return f"  guardrail {name}: applied"
    finally:
        body_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--triage-name", default=_settings.foundry_triage_agent_name)
    parser.add_argument("--dq-name", default=_settings.foundry_dq_agent_name)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--print-definitions", action="store_true")
    parser.add_argument("--with-guardrail", action="store_true",
                        help="Also create/update the RAI guardrail policy")
    parser.add_argument("--account", default=os.environ.get("FOUNDRY_ACCOUNT_NAME", ""))
    parser.add_argument("--resource-group", default=os.environ.get("AZURE_RESOURCE_GROUP", ""))
    parser.add_argument("--guardrail-name", default=_settings.foundry_guardrail_name)
    args = parser.parse_args(argv)

    definitions = build_definitions(args.model)

    if args.print_definitions:
        print(json.dumps(definitions, indent=2))
        return 0

    try:
        import httpx
    except ImportError:
        print("ERROR: httpx required. pip install -e '.[azure]'", file=sys.stderr)
        return 2

    endpoint = args.endpoint.rstrip("/")
    if not endpoint:
        print(
            "ERROR: no project endpoint. Set FOUNDRY_PROJECT_ENDPOINT in .env "
            "or pass --endpoint. See .env.example.",
            file=sys.stderr,
        )
        return 2
    headers = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}

    print(f"project: {endpoint}")
    print(f"model:   {args.model}\n")
    print("agents:")
    with httpx.Client(timeout=90, headers=headers) as client:
        # Data Quality first — the triage agent's prompt references it, and in
        # a future a2a wiring the reference must resolve.
        print(sync_agent(client, endpoint, args.dq_name, definitions["data_quality"], dry_run=args.dry_run))
        print(sync_agent(client, endpoint, args.triage_name, definitions["triage"], dry_run=args.dry_run))

    if args.with_guardrail:
        print("\nguardrail:")
        print(sync_guardrail(
            account=args.account,
            resource_group=args.resource_group,
            name=args.guardrail_name,
            dry_run=args.dry_run,
        ))

    print("\nReminder: a Foundry-registered agent does not pick up local prompt or")
    print("tool changes. Re-run this after ANY edit to the prompts or TRIAGE_TOOLS.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
