from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import transport_probe_image_entry as entry

ROOT = Path(__file__).resolve().parents[1]


def test_pinned_async_setup_is_explicit_bounded_and_has_no_retries(tmp_path):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    entry.install_async_transport(str(tmp_path / "dependencies"), run=run)
    command, options = calls[0]
    assert command[-1] == "aiohttp==3.14.3"
    assert command[command.index("--index-url") + 1] == "https://pypi.org/simple"
    assert command[command.index("--retries") + 1] == "0"
    assert "--isolated" in command and "--only-binary=:all:" in command
    assert options["timeout"] == 90
    assert options["capture_output"] is True
    assert len(calls) == 1


def test_pip_failure_does_not_echo_captured_output(tmp_path):
    def run(*args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="DO_NOT_PRINT", stderr="DO_NOT_PRINT")

    with pytest.raises(entry.BundleError) as error:
        entry.install_async_transport(str(tmp_path), run=run)
    assert str(error.value) == "async_transport_install_failed"


@pytest.mark.parametrize("install", [False, True])
def test_image_entry_does_not_assume_a_post_snapshot_module(monkeypatch, capsys, install):
    monkeypatch.setenv("MONITORING_PROBE_INSTALL_AIOHTTP", str(install).lower())
    installations = []
    imports = []
    monkeypatch.setattr(entry, "install_async_transport", installations.append)
    monkeypatch.setattr(entry.importlib, "import_module", lambda name: imports.append(name))
    monkeypatch.setattr(entry.importlib.metadata, "version", lambda name: "test-version")
    monkeypatch.setattr(entry, "load_public_bundle", lambda env: SimpleNamespace(main=lambda: 0))
    original = list(sys.path)
    try:
        assert entry.main() == 0
    finally:
        sys.path[:] = original
    assert bool(installations) is install
    assert imports == ["azure.eventhub.aio", "azure.identity.aio", "aiohttp"]
    document = json.loads(capsys.readouterr().out)
    assert document["stage"] == "transport_image_dependencies_verified"
    assert document["normal_worker_ready"] is False


def test_missing_async_dependency_is_not_operator_auth_or_a_success(monkeypatch, capsys):
    monkeypatch.setenv("MONITORING_PROBE_INSTALL_AIOHTTP", "false")

    def load(name):
        if name == "aiohttp":
            raise ModuleNotFoundError("aiohttp")

    monkeypatch.setattr(entry.importlib, "import_module", load)
    assert entry.main() == 2
    result = json.loads(capsys.readouterr().out)
    assert result["code"] == "image_async_sdk_dependency_missing"
    assert result["normal_worker_ready"] is False


def test_current_public_bundle_loads_without_worker_code_or_live_auth(tmp_path):
    environment = dict(os.environ)
    for module, variable in entry.MODULES:
        source = ROOT / "scripts" / (module.rsplit(".", 1)[1] + ".py")
        environment[variable] = base64.b64encode(source.read_bytes()).decode()
    environment["MONITORING_TRANSPORT_PROBE_INPUT"] = "{}"
    program = (
        "import os, sys; "
        f"sys.path.insert(0, {str(ROOT)!r}); "
        "from scripts.transport_probe_image_entry import load_public_bundle; "
        "raise SystemExit(load_public_bundle(os.environ).main())"
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", program],
        env=environment,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 2, result.stderr
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert records[0]["stage"] == "public_probe_bundle_loaded"
    assert len(records[0]["bundle_sha256"]) == 64
    assert records[-1]["stage"] == "transport_probe_blocked"
    assert records[-1]["sql_acceptance_proven"] is False


def test_compiled_image_mode_contains_only_public_bundle_files():
    path = os.environ.get("MONITORING_TRANSPORT_JOB_COMPILED_TEMPLATE")
    if not path:
        pytest.skip("Supply the compiled transport job template.")
    template = json.loads(Path(path).read_text("utf-8-sig"))
    resources = template["resources"]
    if isinstance(resources, dict):
        resources = list(resources.values())
    job = next(item for item in resources if item["type"] == "Microsoft.App/jobs")
    environment = {
        value["name"]: value["value"]
        for value in job["properties"]["template"]["containers"][0]["env"]
    }
    for module, key in entry.MODULES:
        value = environment[key]
        if value.startswith("[variables('"):
            name = value.removeprefix("[variables('").removesuffix("')]")
            value = template["variables"][name]
        assert (
            base64.b64decode(value)
            == (ROOT / "scripts" / (module.rsplit(".", 1)[1] + ".py")).read_bytes()
        )
    assert job["properties"]["configuration"]["replicaRetryLimit"] == 0
    assert not any(name.startswith("AZURE_SQL_") for name in environment)
