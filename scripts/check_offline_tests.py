"""Reject direct live credential imports and request helpers in offline tests."""

from __future__ import annotations

import ast
from pathlib import Path

REQUEST_HELPERS = {"get", "post", "put", "patch", "delete", "head", "options", "request", "stream"}


def violations(source: str) -> list[tuple[int, str]]:
    tree = ast.parse(source)
    modules = {"httpx": "httpx", "requests": "requests"}
    findings: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "azure.identity" or alias.name.startswith("azure.identity."):
                    findings.add((node.lineno, "live credential import"))
                if alias.name in {"httpx", "requests"}:
                    modules[alias.asname or alias.name] = alias.name
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "azure.identity" or module.startswith("azure.identity.") or (
                module == "azure" and any(alias.name == "identity" for alias in node.names)
            ):
                findings.add((node.lineno, "live credential import"))
            if module in {"httpx", "requests"} and any(
                alias.name in REQUEST_HELPERS or alias.name == "*" for alias in node.names
            ):
                findings.add((node.lineno, "direct network helper import"))

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and function.value.id in modules
            and function.attr in REQUEST_HELPERS
        ):
            findings.add((node.lineno, "direct network helper call"))
        dynamic_import = (
            isinstance(function, ast.Name) and function.id == "__import__"
        ) or (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and (function.value.id, function.attr) in {
                ("importlib", "import_module"), ("pytest", "importorskip"),
            }
        )
        if dynamic_import and node.args and isinstance(node.args[0], ast.Constant):
            name = node.args[0].value
            if isinstance(name, str) and (name == "azure.identity" or name.startswith("azure.identity.")):
                findings.add((node.lineno, "live credential import"))
    return sorted(findings)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    findings = []
    for path in sorted((root / "tests").rglob("*.py")):
        try:
            found = violations(path.read_text(encoding="utf-8-sig"))
        except (OSError, SyntaxError) as exc:
            print(f"{path.relative_to(root)}: cannot inspect tests ({type(exc).__name__})")
            return 1
        findings.extend((path.relative_to(root), line, reason) for line, reason in found)
    for path, line, reason in findings:
        print(f"{path}:{line}: {reason}")
    if findings:
        return 1
    print("No direct live credential imports or network helpers in the test path.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
