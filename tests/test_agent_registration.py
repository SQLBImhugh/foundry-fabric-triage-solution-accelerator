"""Tests for agent registration drift detection.

`scripts/register_foundry_agents.py` decides whether a Foundry-registered agent
still matches the local prompt and tool schemas. That decision is the only thing
standing between a prompt edit and a run that silently uses the old one, so it
has to be right in both directions: it must not miss a real change, and it must
not report a change that did not happen.

The second half turned out to matter as much as the first. The control plane
echoes definitions back with its own defaults as nulls -- every function tool
returns carrying ``"strict": null`` -- and an earlier comparison stripped those
only at the top level. The tool list therefore always differed, so the triage
agent was given a new version on every run of the script and reached version 9
while the data quality agent, which has no tools, sat correctly at version 3.
A drift check that always says "drifted" tells you nothing.

Offline: these exercise the comparison directly and never call the API.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from register_foundry_agents import _strip_nulls, definitions_match  # noqa: E402


def _tool(name: str, **extra: object) -> dict:
    return {
        "type": "function",
        "name": name,
        "description": f"does {name}",
        "parameters": {"type": "object", "properties": {}},
        **extra,
    }


def test_server_added_nulls_inside_tools_do_not_count_as_drift() -> None:
    """The exact shape the control plane returns must compare as in sync.

    This is the regression: `strict` is added by the server, nothing local ever
    sets it, and treating it as a difference caused a new agent version on every
    single run.
    """
    local = {"model": "gpt-5.6-luna", "tools": [_tool("refresh_powerbi_dataset")]}
    remote = {
        "model": "gpt-5.6-luna",
        "tools": [_tool("refresh_powerbi_dataset", strict=None)],
        "temperature": None,
    }
    assert definitions_match(local, remote)


def test_a_changed_tool_description_is_still_drift() -> None:
    """Negative control: the check must still catch a real edit.

    A tool description is the only thing the model sees about what a tool does,
    so an edited description that never reaches the registered agent changes
    nothing at runtime while appearing to.
    """
    local = {"tools": [_tool("refresh_powerbi_dataset")]}
    remote = {"tools": [_tool("refresh_powerbi_dataset")]}
    remote["tools"][0]["description"] = "something else entirely"
    assert not definitions_match(local, remote)


def test_an_added_tool_is_drift() -> None:
    local = {"tools": [_tool("a"), _tool("b")]}
    remote = {"tools": [_tool("a")]}
    assert not definitions_match(local, remote)


def test_removing_a_field_locally_is_drift() -> None:
    """Dropping a field must not read as "in sync" while the old value serves.

    Removing `temperature` for a reasoning model is the real case: if the check
    only looked at keys present locally, the registered agent would keep its
    temperature and the local definition would look identical.
    """
    local = {"model": "gpt-5.6-luna"}
    remote = {"model": "gpt-5.6-luna", "temperature": 0.2}
    assert not definitions_match(local, remote)


def test_a_field_the_server_reports_as_null_is_treated_as_absent() -> None:
    local = {"model": "gpt-5.6-luna"}
    remote = {"model": "gpt-5.6-luna", "temperature": None}
    assert definitions_match(local, remote)


def test_instructions_change_is_drift() -> None:
    """Prompts are hashed onto every incident; an unregistered edit is invisible."""
    assert not definitions_match(
        {"instructions": "You are a triage agent."},
        {"instructions": "You are a triage agent. Always retry."},
    )


def test_strip_nulls_recurses_through_lists_and_nested_dicts() -> None:
    stripped = _strip_nulls(
        {"a": None, "b": {"c": None, "d": 1}, "e": [{"f": None, "g": 2}]}
    )
    assert stripped == {"b": {"d": 1}, "e": [{"g": 2}]}


def test_strip_nulls_keeps_falsey_values() -> None:
    """Only null is a server default. Zero, empty string and False are content."""
    assert _strip_nulls({"a": 0, "b": "", "c": False, "d": [], "e": None}) == {
        "a": 0, "b": "", "c": False, "d": [],
    }
