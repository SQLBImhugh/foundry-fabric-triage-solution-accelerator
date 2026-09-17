"""Finite public probe bundle for an already-built diagnostic image.

The job supplies only compiled public helper code, nonsecret endpoint/identity
input and an explicit optional aiohttp install switch. No worker/SQL module,
private journal, operator credential or Fabric POST is used.
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
import types
from collections.abc import Mapping
from pathlib import Path

MODULES = (
    ("scripts.hybrid_platform_probe", "PUBLIC_TRANSPORT_HELPER"),
    ("scripts.monitoring_readiness_probe", "PUBLIC_READINESS_HELPER"),
    ("scripts.monitoring_transport_probe", "PUBLIC_TRANSPORT_RUNNER"),
)


class BundleError(RuntimeError):
    """Only bounded failure codes, never dependency stdout or credentials."""


def install_async_transport(directory: str, *, run=subprocess.run) -> None:
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--isolated",
        "--disable-pip-version-check",
        "--no-cache-dir",
        "--no-input",
        "--retries",
        "0",
        "--timeout",
        "15",
        "--index-url",
        "https://pypi.org/simple",
        "--only-binary=:all:",
        "--target",
        directory,
        "aiohttp==3.14.3",
    ]
    try:
        result = run(command, capture_output=True, text=True, timeout=90, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise BundleError("async_transport_install_unavailable") from None
    if result.returncode != 0:
        raise BundleError("async_transport_install_failed")


def load_public_bundle(environment: Mapping[str, str]) -> types.ModuleType:
    package = types.ModuleType("scripts")
    package.__path__ = []
    sys.modules["scripts"] = package
    digest = hashlib.sha256()
    for name, key in MODULES:
        value = environment.get(key, "")
        if not value or len(value) > 180_000:
            raise BundleError("public_probe_bundle_missing_or_oversized")
        try:
            source = base64.b64decode(value, validate=True)
        except ValueError:
            raise BundleError("public_probe_bundle_invalid") from None
        if len(source) > 128_000:
            raise BundleError("public_probe_bundle_missing_or_oversized")
        digest.update(name.encode())
        digest.update(source)
        module = types.ModuleType(name)
        module.__file__ = f"<public-bundle:{name}>"
        sys.modules[name] = module
        exec(compile(source, module.__file__, "exec"), module.__dict__)
    print(
        json.dumps(
            {
                "stage": "public_probe_bundle_loaded",
                "bundle_sha256": digest.hexdigest(),
                "normal_worker_ready": False,
                "sql_acceptance_proven": False,
            }
        ),
        flush=True,
    )
    return sys.modules["scripts.monitoring_transport_probe"]


def main() -> int:
    install = os.environ.get("MONITORING_PROBE_INSTALL_AIOHTTP", "false")
    if install not in {"false", "true"}:
        print(
            json.dumps({"stage": "transport_probe_blocked", "code": "invalid_install_switch"}),
            flush=True,
        )
        return 2
    try:
        with tempfile.TemporaryDirectory(prefix="public-transport-probe-") as temporary:
            if install == "true":
                dependencies = str(Path(temporary) / "dependencies")
                install_async_transport(dependencies)
                sys.path.insert(0, dependencies)
                importlib.invalidate_caches()
            for module in ("azure.eventhub.aio", "azure.identity.aio", "aiohttp"):
                try:
                    importlib.import_module(module)
                except ImportError:
                    raise BundleError("image_async_sdk_dependency_missing") from None
            print(
                json.dumps(
                    {
                        "stage": "transport_image_dependencies_verified",
                        "azure_eventhub": importlib.metadata.version("azure-eventhub"),
                        "azure_identity": importlib.metadata.version("azure-identity"),
                        "aiohttp": importlib.metadata.version("aiohttp"),
                        "startup_async_transport_install": install == "true",
                        "normal_worker_ready": False,
                    }
                ),
                flush=True,
            )
            return load_public_bundle(os.environ).main()
    except Exception as exc:
        print(
            json.dumps(
                {
                    "stage": "transport_probe_blocked",
                    "code": str(exc)
                    if isinstance(exc, BundleError)
                    else "public_probe_entry_failed",
                    "error_class": type(exc).__name__,
                    "normal_worker_ready": False,
                    "sql_acceptance_proven": False,
                }
            ),
            flush=True,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
