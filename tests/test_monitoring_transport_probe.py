from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from test_hybrid_platform_probe import (
    CLIENT,
    IDENTITY,
    ITEM,
    OBJECT,
    SUBSCRIPTION,
    TENANT,
    WORKSPACE,
    envelope,
    metadata,
)

from scripts.hybrid_platform_probe import Endpoint, ProbeError, bounded_event_body, receive
from scripts.monitoring_transport_probe import TransportProbeInput, run_probe

ROOT = Path(__file__).resolve().parents[1]
FILES = (
    "Dockerfile.monitoring-transport-probe",
    "requirements.monitoring-transport-probe.txt",
    r"scripts\hybrid_platform_probe.py",
    r"scripts\monitoring_transport_probe.py",
    r"scripts\monitoring_readiness_probe.py",
)


def probe_input(seconds=120):
    return {
        "endpoint": metadata(),
        "sourceWorkspaceId": WORKSPACE,
        "sourceItemId": ITEM,
        "seconds": seconds,
    }


def environment(seconds=120):
    return {
        "MONITORING_TRANSPORT_PROBE_INPUT": json.dumps(probe_input(seconds)),
        "AZURE_TENANT_ID": TENANT,
        "AZURE_CLIENT_ID": CLIENT,
        "AZURE_SUBSCRIPTION_ID": SUBSCRIPTION,
        "MONITORING_IDENTITY_OBJECT_ID": OBJECT,
        "MONITORING_IDENTITY_RESOURCE_ID": IDENTITY,
    }


@pytest.mark.parametrize("seconds", [120, 180, 240])
async def test_probe_delegates_exact_owned_metadata_without_sql_or_worker_inputs(seconds):
    calls = []

    async def receiver(endpoint, **kwargs):
        calls.append((endpoint, kwargs))
        return 1

    assert await run_probe(environment(seconds), receiver=receiver) == 1
    endpoint, arguments = calls[0]
    assert endpoint == Endpoint.from_document(metadata())
    assert arguments == {
        "tenant_id": TENANT,
        "client_id": CLIENT,
        "object_id": OBJECT,
        "subscription_id": SUBSCRIPTION,
        "identity_resource_id": IDENTITY,
        "item_id": ITEM,
        "seconds": seconds,
        "max_output_records": 50,
    }


@pytest.mark.parametrize("seconds", [0, 119, 241, 900, True, "120"])
def test_probe_duration_is_explicitly_bounded(seconds):
    with pytest.raises(ProbeError, match="120-240"):
        TransportProbeInput.parse(json.dumps(probe_input(seconds)))


def test_probe_refuses_secret_fields_and_cross_workspace_rebinding():
    value = probe_input()
    value["endpoint"]["accessKeys"] = {}
    with pytest.raises(ProbeError):
        TransportProbeInput.parse(json.dumps(value))
    value = probe_input()
    value["sourceWorkspaceId"] = ITEM
    with pytest.raises(ProbeError, match="same workspace"):
        TransportProbeInput.parse(json.dumps(value))


@pytest.mark.parametrize("count", [0, None, False])
async def test_no_receipt_is_not_a_successful_probe(count):
    async def receiver(*args, **kwargs):
        return count

    with pytest.raises(ProbeError, match="No owned event receipt"):
        await run_probe(environment(), receiver=receiver)


def test_complete_oversized_body_is_hashed_without_retaining_it():
    body = [b"a" * 40_000, b"b" * 40_000]
    raw, size, digest = bounded_event_body(body)
    assert raw is None
    assert size == 80_000
    assert digest == hashlib.sha256(b"".join(body)).hexdigest()


@pytest.mark.parametrize("failure", [False, True])
async def test_receive_bounds_output_and_fails_after_sdk_error_without_retry(
    monkeypatch, capsys, failure
):
    claims = {"tid": TENANT, "appid": CLIENT, "oid": OBJECT, "xms_mirid": IDENTITY}
    encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")

    class Credential:
        def __init__(self, **kwargs):
            assert kwargs == {"client_id": CLIENT, "retry_total": 0}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_token(self, *args, **kwargs):
            return SimpleNamespace(token=f"fixture.{encoded}.fixture")

    class Consumer:
        def __init__(self, **kwargs):
            assert kwargs["retry_total"] == 0
            assert "checkpoint_store" not in kwargs
            self.credential = kwargs["credential"]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_partition_ids(self):
            await self.credential.get_token("https://eventhubs.azure.net/.default")
            return ["0"]

        async def receive(self, *, on_event, on_error, **kwargs):
            assert kwargs["max_wait_time"] == 5
            partition = SimpleNamespace(partition_id="0")
            await on_event(partition, None)
            for sequence in range(5):
                await on_event(
                    partition,
                    SimpleNamespace(
                        body=[json.dumps(envelope()).encode()],
                        sequence_number=sequence,
                        offset=str(sequence),
                    ),
                )
            if failure:
                await on_error(partition, RuntimeError("NEVER_PRINT_SDK_BODY"))

    modules = {}
    for name in (
        "azure",
        "azure.core",
        "azure.core.exceptions",
        "azure.eventhub",
        "azure.eventhub.aio",
        "azure.identity",
        "azure.identity.aio",
    ):
        modules[name] = ModuleType(name)
        monkeypatch.setitem(sys.modules, name, modules[name])
    modules["azure.core.exceptions"].AzureError = type("FakeAzureError", (Exception,), {})
    modules["azure.eventhub"].TransportType = SimpleNamespace(AmqpOverWebsocket="wss443")
    modules["azure.eventhub.aio"].EventHubConsumerClient = Consumer
    modules["azure.identity.aio"].ManagedIdentityCredential = Credential
    kwargs = {
        "tenant_id": TENANT,
        "client_id": CLIENT,
        "object_id": OBJECT,
        "subscription_id": SUBSCRIPTION,
        "identity_resource_id": IDENTITY,
        "item_id": ITEM,
        "seconds": 1,
        "max_output_records": 2,
    }
    if failure:
        with pytest.raises(ProbeError, match="receive failed"):
            await receive(Endpoint.from_document(metadata()), **kwargs)
    else:
        assert await receive(Endpoint.from_document(metadata()), **kwargs) == 5
    output = capsys.readouterr().out
    assert "NEVER_PRINT_SDK_BODY" not in output
    records = [json.loads(line) for line in output.splitlines()]
    assert len([record for record in records if record["stage"] == "transport_receipt"]) == 2
    assert any(record["stage"] == "receiver_ready" for record in records)
    if not failure:
        assert records[-1]["suppressed_records"] == 3
        assert records[-1]["durable_checkpoint_written"] is False


def test_probe_image_has_no_repository_or_worker_dependency():
    docker = (ROOT / FILES[0]).read_text("utf-8")
    assert "python:3.13-slim-trixie" in docker
    assert "--index-url https://pypi.org/simple --only-binary=:all:" in docker
    assert "USER 10001:10001" in docker
    assert "COPY src" not in docker and "pyproject.toml" not in docker
    assert (
        "sql"
        not in "\n".join(line for line in docker.splitlines() if not line.startswith("#")).lower()
    )
    assert 'ENTRYPOINT ["python", "-m", "scripts.monitoring_transport_probe"]' in docker
    requirements = (ROOT / FILES[1]).read_text("utf-8")
    pins = [line for line in requirements.splitlines() if line and not line.startswith("#")]
    assert pins == [
        "azure-core==1.41.0",
        "azure-identity==1.25.3",
        "azure-eventhub==5.15.1",
        "aiohttp==3.14.3",
    ]


def test_package_contains_only_public_probe_build_inputs(tmp_path):
    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell is required for the offline packaging test")
    output = tmp_path / "probe-context"
    result = subprocess.run(
        [
            pwsh,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(ROOT / "scripts" / "package_monitoring_transport_probe.ps1"),
            "-OutputDirectory",
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    expected = {name.replace("\\", "/") for name in FILES} | {"transport-probe-package.json"}
    assert {
        path.relative_to(output).as_posix() for path in output.rglob("*") if path.is_file()
    } == expected
    manifest = json.loads((output / "transport-probe-package.json").read_text("utf-8-sig"))
    assert manifest["imageBuilt"] is False
    for entry in manifest["files"]:
        content = (output / Path(entry["path"].replace("\\", "/"))).read_bytes()
        assert hashlib.sha256(content).hexdigest() == entry["sha256"]


def test_compiled_transport_job_is_finite_identity_only_and_has_no_sql():
    path = os.environ.get("MONITORING_TRANSPORT_JOB_COMPILED_TEMPLATE")
    if not path:
        pytest.skip("Supply the locally compiled transport job template.")
    template = json.loads(Path(path).read_text("utf-8-sig"))
    resources = template["resources"]
    if isinstance(resources, dict):
        resources = list(resources.values())
    jobs = [resource for resource in resources if resource["type"] == "Microsoft.App/jobs"]
    assert len(jobs) == 1
    job = jobs[0]
    assert job["identity"]["type"] == "UserAssigned"
    config = job["properties"]["configuration"]
    assert config["triggerType"] == "Manual"
    assert config["replicaRetryLimit"] == 0
    assert config["manualTriggerConfig"] == {"parallelism": 1, "replicaCompletionCount": 1}
    assert config["replicaTimeout"] == (
        "[add(parameters('probeSeconds'), if(and(equals(parameters('receiverMode'), "
        "'bundled-public-helper'), parameters('installPinnedAsyncTransport')), 180, 60))]"
    )
    assert config["registries"][0]["identity"] == "[parameters('workerIdentityResourceId')]"
    assert "secrets" not in config and "ingress" not in config
    parameters = template["parameters"]
    assert parameters["readinessModelId"]["defaultValue"] == ""
    assert parameters["receiverMode"]["defaultValue"] == "isolated"
    assert parameters["installPinnedAsyncTransport"]["defaultValue"] is False
    assert parameters["probeSeconds"]["minValue"] == 120
    assert parameters["probeSeconds"]["maxValue"] == 240
    container = job["properties"]["template"]["containers"][0]
    assert container["command"].startswith(
        "[if(equals(parameters('receiverMode'), 'isolated'), "
        "createArray('python', '-m', 'scripts.monitoring_transport_probe'), "
        "createArray('python', '-c', variables("
    )
    assert not any(item["name"].startswith("AZURE_SQL_") for item in container["env"])
    assert template["outputs"]["normalWorkerReady"]["value"] is False
    assert template["outputs"]["eventConsumptionVerified"]["value"] is False
    assert not re.search(r"listKeys|listCredentials|passwordSecretRef", json.dumps(template))
