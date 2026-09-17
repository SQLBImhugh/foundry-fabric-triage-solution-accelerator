from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_monitoring_infra import GROUP, ROOT, _deployed_resources, _fixture, _run

from scripts.monitoring_eventstream_canary import (
    FABRIC_ROOT,
    Canary,
    CanaryError,
    CanaryPending,
    CanarySpec,
    FabricClient,
    Reply,
    managed_identity_token,
)

CANARY_WORKSPACE = "55555555-5555-4555-8555-555555555555"
CANARY_PIPELINE = "66666666-6666-4666-8666-666666666666"
CANARY_INTENT = "77777777-7777-4777-8777-777777777777"
CANARY_ITEM = "88888888-8888-4888-8888-888888888888"
CANARY_OPERATION = "99999999-9999-4999-8999-999999999999"


def environment_fixture(tmp_path: Path, **overrides):
    parameters, _, responses = _fixture(tmp_path, **overrides)
    for name in (
        "WorkerName",
        "WorkerIdentityResourceId",
        "RegistryResourceId",
        "Image",
        "AzureSqlServer",
        "AzureSqlDatabase",
        "ConnectorBootstrapFile",
    ):
        parameters.pop(name)
    parameters["EnvironmentOnly"] = True
    environment_id = (
        f"{GROUP}/providers/Microsoft.App/managedEnvironments/{parameters['EnvironmentName']}"
    )
    diagnostic_id = f"{environment_id}/providers/Microsoft.Insights/diagnosticSettings/{parameters['EnvironmentName']}-monitoring"
    old_diagnostic_id = (
        f"{environment_id}/providers/Microsoft.Insights/diagnosticSettings/worker-sample-monitoring"
    )
    diagnostics = responses[f"https://management.azure.com{old_diagnostic_id}"].copy()
    diagnostics["id"] = diagnostic_id
    responses[f"https://management.azure.com{diagnostic_id}"] = diagnostics
    outputs = {
        "environmentResourceId": environment_id,
        "diagnosticSettingResourceId": diagnostic_id,
        "logAnalyticsWorkspaceResourceId": parameters["LogAnalyticsWorkspaceResourceId"],
        "workerDeployed": False,
        "eventTransportVerified": False,
    }
    responses["deployment group create"]["properties"]["outputs"] = {
        key: {"value": value} for key, value in outputs.items()
    }
    responses["deployment group what-if"]["changes"] = [
        {"resourceId": environment_id, "changeType": "Create"},
        {"resourceId": diagnostic_id, "changeType": "Create"},
    ]
    return parameters, responses


def test_environment_prepare_needs_no_image_identity_sql_or_endpoint(tmp_path: Path):
    parameters, responses = environment_fixture(tmp_path)
    result, calls, report = _run(tmp_path, parameters, {}, responses)
    assert result.returncode == 0, result.stderr
    assert [call["arguments"][:2] for call in calls] == [["bicep", "build"]]
    assert report["status"] == "EnvironmentPrepared"
    assert report["artifactKind"] == "monitoring-environment"
    assert report["workerDeployed"] is False
    assert report["hybridAcceptanceVerified"] is False
    assert "workerResourceId" not in report
    document = json.loads(
        (Path(parameters["OutputDirectory"]) / "monitoring-environment.parameters.json").read_text(
            encoding="utf-8-sig",
        )
    )
    assert set(document["parameters"]) == {
        "environmentName",
        "location",
        "tags",
        "logAnalyticsWorkspaceResourceId",
        "diagnosticSettingName",
        "environmentExceptionTags",
    }


def test_environment_only_does_not_accept_worker_or_other_environment_parameters(tmp_path: Path):
    parameters, responses = environment_fixture(tmp_path)
    parameters["WorkerName"] = "must-not-be-created"
    result, calls, _ = _run(tmp_path, parameters, {}, responses)
    assert result.returncode != 0
    assert "parameter set" in result.stderr.lower()
    assert calls == []


def test_environment_what_if_cannot_change_any_application(tmp_path: Path):
    parameters, responses = environment_fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses["deployment group what-if"]["changes"].append(
        {
            "resourceId": f"{GROUP}/providers/Microsoft.App/containerApps/unrelated",
            "changeType": "Modify",
        }
    )
    result, calls, report = _run(tmp_path, parameters, {}, responses)
    assert result.returncode != 0
    assert "outside this worker deployment" in result.stderr
    assert ["deployment", "group", "create"] not in [call["arguments"][:3] for call in calls]
    assert report["workerDeployed"] is False


def test_environment_only_does_not_adopt_an_existing_environment(tmp_path: Path):
    parameters, responses = environment_fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses["deployment group what-if"]["changes"][0]["changeType"] = "Modify"
    result, calls, report = _run(tmp_path, parameters, {}, responses)
    assert result.returncode != 0
    assert "does not adopt or rewrite" in result.stderr
    assert ["deployment", "group", "create"] not in [call["arguments"][:3] for call in calls]
    assert report["deployed"] is False


@pytest.mark.parametrize("execute", [False, True])
def test_environment_path_verifies_only_supplied_logging_resources(tmp_path: Path, execute):
    parameters, responses = environment_fixture(tmp_path, Mode="WhatIf", Execute=execute)
    result, calls, report = _run(tmp_path, parameters, {}, responses)
    assert result.returncode == 0, result.stderr
    assert report["status"] == ("EnvironmentInfrastructureVerified" if execute else "WhatIfPassed")
    assert report["workerDeployed"] is False
    assert report["deployed"] is execute
    assert report["hybridAcceptanceVerified"] is False
    all_arguments = json.dumps([call["arguments"] for call in calls])
    assert "Microsoft.ManagedIdentity" not in all_arguments
    assert "Microsoft.ContainerRegistry" not in all_arguments
    assert "Microsoft.Web" not in all_arguments
    assert "Microsoft.Network" not in all_arguments
    assert "/containerApps/" not in all_arguments
    assert ["acr", "repository", "show"] not in [call["arguments"][:3] for call in calls]


def test_environment_module_is_reused_without_worker_placeholders():
    environment = (ROOT / "infra" / "monitoring-environment.bicep").read_text("utf-8")
    worker = (ROOT / "infra" / "monitoring-worker.bicep").read_text("utf-8")
    assert "module newEnvironment './monitoring-environment.bicep'" in worker
    assert "'Microsoft.App/managedEnvironments@" not in worker
    for forbidden in (
        "param image",
        "param tenantId",
        "connectorBootstrap",
        "azureSql",
        "registry",
    ):
        assert forbidden not in environment
    assert "output workerDeployed bool = false" in environment
    assert "destination: 'azure-monitor'" in environment
    assert "vnetConfiguration:" not in environment
    assert "workerSubnetResourceId" not in environment


def test_compiled_environment_contains_only_environment_and_keyless_logging():
    path = os.environ.get("MONITORING_ENVIRONMENT_COMPILED_TEMPLATE")
    if not path:
        pytest.skip("Supply the locally compiled environment template for this offline assertion.")
    template = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    resources = _deployed_resources(template)
    assert {item["type"] for item in resources} == {
        "Microsoft.App/managedEnvironments",
        "Microsoft.Insights/diagnosticSettings",
    }
    assert template["outputs"]["workerDeployed"]["value"] is False
    assert template["outputs"]["eventTransportVerified"]["value"] is False
    for resource in resources:
        assert "identity" not in resource
    assert "sharedKey" not in json.dumps(template)


def canary_spec(action="create"):
    value = {
        "action": action,
        "intentId": CANARY_INTENT,
        "workspaceId": CANARY_WORKSPACE,
        "pipelineId": CANARY_PIPELINE,
        "timeoutSeconds": 240,
    }
    if action == "resume":
        value["operationId"] = CANARY_OPERATION
    return CanarySpec.parse(json.dumps(value))


class FakeFabric:
    def __init__(self, spec, *, existing=False, asynchronous=False):
        self.spec = spec
        self.calls = []
        self.existing = existing
        self.asynchronous = asynchronous
        self.poll_count = 0
        self.error_on_create = False
        self.empty_create_body = False
        self.item = {
            "id": CANARY_ITEM,
            "workspaceId": spec.workspace_id,
            "type": "Eventstream",
            "displayName": spec.name,
            "description": spec.request_body()["description"],
        }
        self.definition = {"definition": spec.request_body()["definition"]}
        self.inventory_override = None

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        root = f"/workspaces/{self.spec.workspace_id}"
        if method == "GET" and path == root:
            return Reply(200, {}, {"id": self.spec.workspace_id})
        if method == "GET" and path == f"{root}/items/{self.spec.pipeline_id}":
            return Reply(200, {}, {"id": self.spec.pipeline_id, "type": "DataPipeline"})
        if method == "GET" and path == f"{root}/eventstreams":
            return Reply(
                200, {}, self.inventory_override or {"value": [self.item] if self.existing else []}
            )
        if method == "POST" and path == f"{root}/eventstreams":
            if self.error_on_create:
                raise CanaryError("http_outcome_unknown")
            self.existing = True
            if self.asynchronous:
                return Reply(
                    202,
                    {
                        "x-ms-operation-id": CANARY_OPERATION,
                        "location": f"{FABRIC_ROOT}/operations/{CANARY_OPERATION}",
                        "retry-after": "3",
                    },
                    {},
                )
            return Reply(201, {}, {} if self.empty_create_body else self.item)
        if method == "GET" and path == f"/operations/{CANARY_OPERATION}":
            self.poll_count += 1
            return Reply(
                200,
                {"retry-after": "1"},
                {"status": "Running" if self.poll_count == 1 else "Succeeded"},
            )
        if method == "GET" and path == f"/operations/{CANARY_OPERATION}/result":
            return Reply(200, {}, self.item)
        if method == "GET" and path == f"{root}/eventstreams/{CANARY_ITEM}":
            return Reply(200, {}, self.item)
        if (
            method == "POST"
            and path == f"{root}/eventstreams/{CANARY_ITEM}/getDefinition?format=eventstream"
        ):
            return Reply(200, {}, self.definition)
        raise AssertionError(f"Unmapped offline canary operation: {method} {path}")


def run_canary(spec, client):
    receipts = []
    elapsed = [0.0]
    delays = []

    def sleep(seconds):
        delays.append(seconds)
        elapsed[0] += seconds

    runner = Canary(spec, client, receipts.append, clock=lambda: elapsed[0], sleep=sleep)
    return runner, receipts, delays


@pytest.mark.parametrize("asynchronous", [False, True])
def test_canary_performs_one_scoped_create_then_verifies_ownership_and_definition(asynchronous):
    spec = canary_spec()
    client = FakeFabric(spec, asynchronous=asynchronous)
    runner, receipts, delays = run_canary(spec, client)
    result = runner.run()
    creates = [
        call
        for call in client.calls
        if call[:2] == ("POST", f"/workspaces/{CANARY_WORKSPACE}/eventstreams")
    ]
    assert len(creates) == 1
    assert result["creation_identity_proven_by_this_execution"] is True
    assert result["eventstream_id"] == CANARY_ITEM
    assert result["definition_verified"] is True
    assert all(receipt["worker_ready"] is False for receipt in receipts)
    assert all(receipt["endpoint_metadata_retrieved"] is False for receipt in receipts)
    assert not any(
        "/connection" in call[1] or "/jobs/instances" in call[1] for call in client.calls
    )
    if asynchronous:
        assert delays == [3, 1]
        assert any(receipt["stage"] == "operation_accepted" for receipt in receipts)


def test_canary_never_retries_an_uncertain_create():
    spec = canary_spec()
    client = FakeFabric(spec)
    client.error_on_create = True
    runner, receipts, _ = run_canary(spec, client)
    with pytest.raises(CanaryError, match="http_outcome_unknown"):
        runner.run()
    assert len([call for call in client.calls if call[0] == "POST"]) == 1
    assert receipts[-1]["stage"] == "create_outcome_unverified"
    assert receipts[-1]["worker_ready"] is False


def test_bodyless_creation_reconciles_owned_item_without_reposting():
    spec = canary_spec()
    client = FakeFabric(spec)
    client.empty_create_body = True
    runner, _, _ = run_canary(spec, client)
    result = runner.run()
    assert result["eventstream_id"] == CANARY_ITEM
    assert result["creation_identity_proven_by_this_execution"] is True
    assert len([
        call for call in client.calls
        if call[:2] == ("POST", f"/workspaces/{CANARY_WORKSPACE}/eventstreams")
    ]) == 1


@pytest.mark.parametrize("status", [201, 202])
def test_null_acknowledgement_preserves_lro_headers(status):
    client = FabricClient(canary_spec(), "not-a-real-token")

    class Response:
        headers = {
            "x-ms-operation-id": CANARY_OPERATION,
            "location": f"{FABRIC_ROOT}/operations/{CANARY_OPERATION}",
            "retry-after": "3",
        }

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _limit):
            return b"null"

    response = Response()
    response.status = status
    client._opener = SimpleNamespace(open=lambda *_args, **_kwargs: response)
    result = client.request("POST", f"/workspaces/{CANARY_WORKSPACE}/eventstreams", {})
    assert result.body == {}
    assert result.headers["x-ms-operation-id"] == CANARY_OPERATION
    assert result.headers["retry-after"] == "3"


def test_null_read_payload_is_not_a_valid_resource():
    client = FabricClient(canary_spec(), "not-a-real-token")

    class Response:
        status = 200
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _limit):
            return b"null"

    client._opener = SimpleNamespace(open=lambda *_args, **_kwargs: Response())
    with pytest.raises(CanaryError, match="invalid_response_shape"):
        client.request("GET", f"/workspaces/{CANARY_WORKSPACE}")


@pytest.mark.parametrize("action", ["create", "inspect", "resume"])
def test_recovery_is_not_claimed_as_a_new_managed_identity_creation(action):
    spec = canary_spec(action)
    client = FakeFabric(spec, existing=True)
    runner, _, _ = run_canary(spec, client)
    result = runner.run()
    assert result["creation_identity_proven_by_this_execution"] is False
    assert not any(
        call[:2] == ("POST", f"/workspaces/{CANARY_WORKSPACE}/eventstreams")
        for call in client.calls
    )


def test_inspect_never_creates_a_missing_eventstream():
    spec = canary_spec("inspect")
    client = FakeFabric(spec)
    runner, _, _ = run_canary(spec, client)
    with pytest.raises(CanaryPending, match="not_observed"):
        runner.run()
    assert not any(call[0] == "POST" for call in client.calls)


def test_canary_refuses_foreign_ownership_even_when_display_name_matches():
    spec = canary_spec()
    client = FakeFabric(spec, existing=True)
    client.item["description"] = "Someone else's item"
    runner, _, _ = run_canary(spec, client)
    with pytest.raises(CanaryError, match="owned_eventstream_binding_failed"):
        runner.run()
    assert not any(call[0] == "POST" for call in client.calls)


def test_canary_fails_closed_on_incomplete_inventory_or_wrong_definition():
    spec = canary_spec()
    client = FakeFabric(spec)
    client.inventory_override = {"value": [], "continuationUri": "https://example.invalid/next"}
    runner, _, _ = run_canary(spec, client)
    with pytest.raises(CanaryError, match="continuation_unverified"):
        runner.run()
    assert not any(call[0] == "POST" for call in client.calls)
    client = FakeFabric(spec)
    parts = client.definition["definition"]["parts"]
    topology = json.loads(base64.b64decode(parts[0]["payload"]))
    topology["sources"][0]["properties"]["itemId"] = CANARY_ITEM
    parts[0]["payload"] = base64.b64encode(json.dumps(topology).encode()).decode()
    runner, _, _ = run_canary(spec, client)
    with pytest.raises(CanaryError, match="round_trip_mismatch"):
        runner.run()


def test_lro_location_is_not_an_arbitrary_authenticated_url():
    spec = canary_spec()
    runner, _, _ = run_canary(spec, FakeFabric(spec))
    with pytest.raises(CanaryError, match="unverified_operation_location"):
        runner.complete(
            Reply(
                202,
                {
                    "x-ms-operation-id": CANARY_OPERATION,
                    "location": "https://example.invalid/steal",
                },
                {},
            ),
            "create",
        )


@pytest.mark.parametrize(
    ("method", "path"),
    [
        (
            "GET",
            f"/workspaces/{CANARY_WORKSPACE}/eventstreams/{CANARY_ITEM}/destinations/x/connection",
        ),
        ("POST", f"/workspaces/{CANARY_WORKSPACE}/items/{CANARY_PIPELINE}/jobs/instances"),
        ("DELETE", f"/workspaces/{CANARY_WORKSPACE}/eventstreams/{CANARY_ITEM}"),
        ("GET", f"/workspaces/{CANARY_ITEM}"),
    ],
)
def test_http_allowlist_blocks_keys_remediation_deletion_and_other_workspaces(method, path):
    client = FabricClient(canary_spec(), "not-a-real-token")

    class NoNetwork:
        def open(self, *args, **kwargs):
            raise AssertionError("No disallowed request may reach the network")

    client._opener = NoNetwork()
    with pytest.raises(CanaryError, match="outside_canary_allowlist"):
        client.request(method, path)


def test_cli_auth_selects_the_uami_in_a_fresh_profile_and_checks_its_token():
    from test_monitoring_infra import CLIENT, PRINCIPAL, SUBSCRIPTION, TENANT

    resource = f"{GROUP}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/canary"
    env = {
        "AZURE_TENANT_ID": TENANT,
        "AZURE_CLIENT_ID": CLIENT,
        "AZURE_SUBSCRIPTION_ID": SUBSCRIPTION,
        "MONITORING_IDENTITY_OBJECT_ID": PRINCIPAL,
        "MONITORING_IDENTITY_RESOURCE_ID": resource,
    }
    claims = {
        "tid": TENANT,
        "appid": CLIENT,
        "oid": PRINCIPAL,
        "xms_mirid": resource,
        "aud": "https://api.fabric.microsoft.com",
        "exp": int(time.time()) + 3600,
    }
    encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    token = f"offline.{encoded}.not-a-real-token"
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(
            returncode=0, stdout="" if command[1] == "login" else json.dumps({"accessToken": token})
        )

    actual, identity = managed_identity_token(env, run=run)
    assert actual == token
    assert identity["object_id"] == PRINCIPAL
    assert calls[0][0][:5] == ["az", "login", "--identity", "--client-id", CLIENT]
    assert all("--tenant" not in command for command, _ in calls)
    assert calls[0][1]["env"]["AZURE_CONFIG_DIR"] == calls[1][1]["env"]["AZURE_CONFIG_DIR"]
    assert "AZURE_CONFIG_DIR" not in env


def test_compiled_canary_job_is_manual_bounded_and_packages_only_public_helpers(tmp_path: Path):
    path = os.environ.get("MONITORING_PROBE_JOB_COMPILED_TEMPLATE")
    if not path:
        pytest.skip("Supply the locally compiled canary-job template.")
    template = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    resources = _deployed_resources(template)
    assert [item["type"] for item in resources] == ["Microsoft.App/jobs"]
    job = resources[0]
    config = job["properties"]["configuration"]
    assert config == {
        "triggerType": "Manual",
        "replicaTimeout": 600,
        "replicaRetryLimit": 0,
        "manualTriggerConfig": {"parallelism": 1, "replicaCompletionCount": 1},
    }
    assert job["properties"]["environmentId"] == "[parameters('environmentResourceId')]"
    assert job["identity"]["type"] == "UserAssigned"
    assert template["outputs"]["workerReady"]["value"] is False
    assert template["parameters"]["action"]["defaultValue"] == "inspect"
    container = job["properties"]["template"]["containers"][0]

    def constant(value):
        match = re.fullmatch(r"\[variables\('([^']+)'\)\]", value)
        return constant(template["variables"][match[1]]) if match else value

    values = {item["name"]: constant(item["value"]) for item in container["env"]}
    assert (
        base64.b64decode(values["PUBLIC_PROBE_HELPER"])
        == (ROOT / "scripts" / "hybrid_platform_probe.py").read_bytes()
    )
    assert (
        base64.b64decode(values["PUBLIC_CANARY_RUNNER"])
        == (ROOT / "scripts" / "monitoring_eventstream_canary.py").read_bytes()
    )
    assert not any(key.startswith("AZURE_SQL_") for key in values)
    assert not any(key.startswith("MONITORING_EVENTSTREAM_") for key in values)
    environment = {
        **os.environ,
        "PUBLIC_PROBE_HELPER": values["PUBLIC_PROBE_HELPER"],
        "PUBLIC_CANARY_RUNNER": values["PUBLIC_CANARY_RUNNER"],
        "MONITORING_CANARY_INPUT": "{}",
    }
    result = subprocess.run(
        [sys.executable, "-c", constant(container["args"][1])],
        env=environment,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 2, result.stderr
    assert json.loads(result.stdout)["code"] == "unexpected_input_fields"
