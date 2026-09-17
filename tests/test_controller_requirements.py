"""The hosted controller's container must declare everything its imports reach.

`src/requirements.txt` is written by hand and is deliberately narrower than the
repo's extras, so it drifts silently: the container is built by pip from that
file alone, and a missing package is not visible until an invocation fails. It
happened -- the monitoring release routed "heartbeat" through
`triage.command_center.worker`, which imports the FastAPI router module for
`resolve_command_target`, and the deployed agent answered every request with
"No module named 'fastapi'". There was no telemetry and no run record, so the
agent looked deployed and healthy while doing nothing.

This walks the import graph from the container's entry point, including imports
deferred inside functions, and fails on any third-party package the file does
not declare.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
ENTRY_POINT = SRC / "app.py"
REQUIREMENTS = SRC / "requirements.txt"

#: Modules the container's own configuration never imports. `get_provider`
#: imports the direct Azure OpenAI provider only when TRIAGE_PROVIDER_MODE is
#: "direct", and azure.yaml deploys the controller with "foundry". Switching
#: that mode means adding the openai package to the container's requirements.
CONFIGURATION_GATED = {"triage.providers.azure_openai"}

#: Import name -> the distribution that provides it. Explicit rather than
#: introspected: the dev virtualenv installs the full extras, so asking the
#: environment what provides a module would pass even when the container's
#: requirements file does not ship it.
PROVIDED_BY = {
    "agent_framework": "agent-framework-core",
    "agent_framework_foundry_hosting": "agent-framework-foundry-hosting",
    "azure": "azure-identity",
    "fastapi": "fastapi",
    "httpx": "httpx",
    "jwt": "pyjwt",
    "mssql_python": "mssql-python",
    "opentelemetry": "azure-monitor-opentelemetry",
    "pydantic": "pydantic",
    "pydantic_settings": "pydantic-settings",
    "rich": "rich",
    "yaml": "pyyaml",
}


def _declared() -> set[str]:
    names = set()
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        for separator in ("[", ">", "<", "=", "!", "~", ";"):
            line = line.split(separator)[0]
        names.add(line.strip().lower())
    return names


def _module_path(module: str) -> Path | None:
    candidates = (SRC / (module.replace(".", "/") + ".py"), SRC / module.replace(".", "/") / "__init__.py")
    return next((path for path in candidates if path.is_file()), None)


def _imported_top_levels() -> dict[str, str]:
    """Top-level third-party import -> the first triage module that reaches it."""
    pending, visited, found = [("app", ENTRY_POINT)], set(), {}
    while pending:
        module, path = pending.pop()
        if module in visited:
            continue
        visited.add(module)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            else:
                continue
            for name in names:
                top = name.split(".")[0]
                if top == "triage":
                    if name in CONFIGURATION_GATED:
                        continue
                    child = _module_path(name)
                    if child is not None:
                        pending.append((name, child))
                elif top not in sys.stdlib_module_names and top != "__future__":
                    found.setdefault(top, module)
    return found


def test_controller_requirements_cover_every_imported_package() -> None:
    declared = _declared()
    missing = {
        top: reached_by
        for top, reached_by in _imported_top_levels().items()
        if PROVIDED_BY.get(top, top).lower() not in declared
    }
    assert not missing, (
        "src/requirements.txt does not declare: "
        + ", ".join(f"{top} (reached by {module})" for top, module in sorted(missing.items()))
    )


def test_every_import_is_mapped_to_a_distribution() -> None:
    """An unmapped import would otherwise be compared against its module name."""
    unmapped = sorted(set(_imported_top_levels()) - set(PROVIDED_BY))
    assert not unmapped, f"Add these to PROVIDED_BY: {unmapped}"
