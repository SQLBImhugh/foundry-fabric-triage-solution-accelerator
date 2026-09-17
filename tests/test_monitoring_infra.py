from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BICEP = ROOT / "infra" / "monitoring-worker.bicep"
ENVIRONMENT_BICEP = ROOT / "infra" / "monitoring-environment.bicep"
SCRIPT = ROOT / "scripts" / "deploy_monitoring_worker.ps1"
DOCKERFILE = ROOT / "Dockerfile.monitoring"
SUBSCRIPTION = "11111111-1111-4111-8111-111111111111"
TENANT = "22222222-2222-4222-8222-222222222222"
CLIENT = "33333333-3333-4333-8333-333333333333"
PRINCIPAL = "44444444-4444-4444-8444-444444444444"
GROUP = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/worker-tests"


def test_template_deploys_only_the_worker_environment_and_owned_logging() -> None:
    worker = BICEP.read_text(encoding="utf-8")
    environment = ENVIRONMENT_BICEP.read_text(encoding="utf-8")
    text = worker + environment
    declarations = re.findall(r"^resource \w+ '([^@']+)@[^']+'( existing)?", text, re.M)
    assert {kind for kind, existing in declarations if not existing} == {
        "Microsoft.App/managedEnvironments",
        "Microsoft.App/containerApps",
        "Microsoft.Insights/diagnosticSettings",
    }
    assert {kind for kind, existing in declarations if existing} == {
        "Microsoft.ManagedIdentity/userAssignedIdentities",
        "Microsoft.ContainerRegistry/registries",
    }
    assert worker.count("= if (empty(existingEnvironmentResourceId))") == 1
    assert "module newEnvironment './monitoring-environment.bicep'" in worker
    assert "scope: environment" in environment
    assert "vnetConfiguration:" not in text
    assert "infrastructureSubnetId:" not in text
    assert "workerSubnetResourceId" not in text
    assert "tags: union(environmentExceptionTags, tags)" in text
    assert "param environmentExceptionTags object = {}" in text


def test_template_uses_identity_only_without_ingress_or_a_secret_scaler() -> None:
    text = BICEP.read_text(encoding="utf-8") + ENVIRONMENT_BICEP.read_text(encoding="utf-8")
    assert "ingress: null" in text
    assert "activeRevisionsMode: 'Single'" in text
    assert "identity: workerIdentityResourceId" in text
    assert "type: 'UserAssigned'" in text
    assert "cpu: json('0.5')" in text
    assert "memory: '1Gi'" in text
    assert "param minReplicas int = 1" in text
    assert "maxReplicas: 2" in text
    assert "rules: []" in text
    assert "destination: 'azure-monitor'" in text
    assert "workspaceId: logAnalyticsWorkspaceResourceId" in text
    for forbidden in (
        "listKeys(",
        "listCredentials(",
        "sharedKey:",
        "passwordSecretRef:",
        "secretRef:",
        "logAnalyticsConfiguration:",
        "FABRIC_PIPELINE_TARGETS",
        "CONNECTION_STRING",
        "SecurityControl: 'Ignore'",
        "CostControl: 'Ignore'",
    ):
        assert forbidden not in text


def test_dockerfile_has_a_narrow_copy_boundary_and_driver_prerequisites() -> None:
    text = DOCKERFILE.read_text(encoding="utf-8")
    assert "python:3.13-slim-trixie" in text
    assert "libltdl7 libkrb5-3 libgssapi-krb5-2" in text
    assert 'python -m pip install --no-cache-dir ".[azure,monitoring]"' in text
    assert "import mssql_python; import azure.eventhub" in text
    assert "find_spec('triage.monitoring.worker')" in text
    assert "USER 10001:10001" in text
    assert "STOPSIGNAL SIGTERM" in text
    assert json.loads(re.search(r"^ENTRYPOINT (.+)$", text, re.M)[1]) == [
        "python",
        "-m",
        "triage.monitoring.worker",
    ]
    assert re.findall(r"^COPY .+$", text, re.M) == [
        "COPY pyproject.toml README.md LICENSE ./",
        "COPY src/triage ./src/triage",
        "COPY scripts/hybrid_platform_probe.py ./scripts/hybrid_platform_probe.py",
    ]
    for forbidden in ("EXPOSE ", "HEALTHCHECK ", "pyodbc", "msodbcsql", "AZURE_CLIENT_SECRET"):
        assert forbidden not in text


def test_compiled_template_contract_when_supplied() -> None:
    """Compilation is an explicit local gate, never an Azure CLI call from pytest."""
    path = os.environ.get("MONITORING_INFRA_COMPILED_TEMPLATE")
    if not path:
        pytest.skip("Set MONITORING_INFRA_COMPILED_TEMPLATE to a locally compiled ARM template.")
    template = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    deployed = _deployed_resources(template)
    assert {resource["type"] for resource in deployed} == {
        "Microsoft.App/managedEnvironments",
        "Microsoft.App/containerApps",
        "Microsoft.Insights/diagnosticSettings",
    }
    app = next(
        resource for resource in deployed if resource["type"] == "Microsoft.App/containerApps"
    )
    properties = app["properties"]
    assert properties["configuration"]["ingress"] is None
    assert "secrets" not in properties["configuration"]
    assert properties["configuration"]["registries"][0]["identity"] == (
        "[parameters('workerIdentityResourceId')]"
    )
    assert properties["template"]["scale"] == {
        "minReplicas": "[parameters('minReplicas')]",
        "maxReplicas": 2,
        "rules": [],
    }
    assert properties["template"]["containers"][0]["resources"] == {
        "cpu": "[json('0.5')]",
        "memory": "1Gi",
    }
    assert template["parameters"]["minReplicas"]["defaultValue"] == 1
    assert template["parameters"]["environmentExceptionTags"]["defaultValue"] == {}


def _deployed_resources(template: dict) -> list[dict]:
    resources = template["resources"]
    if isinstance(resources, dict):
        resources = list(resources.values())
    result = []
    for resource in resources:
        if resource.get("existing", False):
            continue
        if resource["type"] == "Microsoft.Resources/deployments":
            assert resource["properties"]["mode"] == "Incremental"
            result.extend(_deployed_resources(resource["properties"]["template"]))
        else:
            result.append(resource)
    return result


def _fixture(tmp_path: Path, **overrides: object) -> tuple[dict, dict, dict]:
    tags = {
        "CostCenter": "sample",
        "Owner": "sample",
        "Environment": "test",
        "DataClassification": "public",
    }
    bootstrap = {
        "connectorId": "88888888-8888-4888-8888-888888888888",
        "workspaceId": "55555555-5555-4555-8555-555555555555",
        "eventstreamId": "66666666-6666-4666-8666-666666666666",
        "destinationId": "77777777-7777-4777-8777-777777777777",
        "fullyQualifiedNamespace": "sample.servicebus.windows.net",
        "eventHubName": "owned-destination",
        "consumerGroup": "$Default",
    }
    profile = tmp_path / "isolated-azure"
    profile.mkdir()
    parameters = {
        "SubscriptionId": SUBSCRIPTION,
        "TenantId": TENANT,
        "ResourceGroup": "worker-tests",
        "Location": "eastus",
        "WorkerName": "worker-sample",
        "WorkerIdentityResourceId": f"{GROUP}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/worker-identity",
        "RegistryResourceId": f"{GROUP}/providers/Microsoft.ContainerRegistry/registries/sampleregistry",
        "Image": "sampleregistry.azurecr.io/monitoring@sha256:" + "a" * 64,
        "EnvironmentName": "worker-environment",
        "LogAnalyticsWorkspaceResourceId": f"{GROUP}/providers/Microsoft.OperationalInsights/workspaces/sample-logs",
        "AzureSqlServer": "sample.database.windows.net",
        "AzureSqlDatabase": "monitoring",
        "ConnectorBootstrapFile": str(tmp_path / "connector.json"),
        "OutputDirectory": str(tmp_path / "prepared"),
        "AzureConfigDirectory": str(profile),
        **tags,
        **overrides,
    }
    env_id = parameters.get("ExistingEnvironmentResourceId") or (
        f"{GROUP}/providers/Microsoft.App/managedEnvironments/{parameters['EnvironmentName']}"
    )
    app_id = f"{GROUP}/providers/Microsoft.App/containerApps/{parameters['WorkerName']}"
    diag_id = f"{env_id}/providers/Microsoft.Insights/diagnosticSettings/worker-sample-monitoring"
    variables = {
        "AZURE_SUBSCRIPTION_ID": SUBSCRIPTION,
        "AZURE_TENANT_ID": TENANT,
        "AZURE_CLIENT_ID": CLIENT,
        "MONITORING_IDENTITY_OBJECT_ID": PRINCIPAL,
        "MONITORING_IDENTITY_RESOURCE_ID": parameters["WorkerIdentityResourceId"],
        "MONITORING_MODE": "live",
        "MONITORING_INVENTORY_MODE": parameters.get("InventoryMode", "caller_visible"),
        "MONITORING_TENANT_ID": TENANT,
        "AZURE_SQL_SERVER": parameters["AzureSqlServer"],
        "AZURE_SQL_DATABASE": parameters["AzureSqlDatabase"],
        "MONITORING_CONNECTOR_ID": bootstrap["connectorId"],
        "MONITORING_EVENTSTREAM_WORKSPACE_ID": bootstrap["workspaceId"],
        "MONITORING_EVENTSTREAM_ID": bootstrap["eventstreamId"],
        "MONITORING_EVENTSTREAM_DESTINATION_ID": bootstrap["destinationId"],
        "MONITORING_EVENTSTREAM_NAMESPACE": bootstrap["fullyQualifiedNamespace"],
        "MONITORING_EVENTSTREAM_ENTITY": bootstrap["eventHubName"],
        "MONITORING_EVENTSTREAM_CONSUMER_GROUP": bootstrap["consumerGroup"],
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    }

    def resource(resource_id: str, **properties: object) -> dict:
        return {
            "id": resource_id,
            "name": resource_id.rsplit("/", 1)[1],
            "location": "eastus",
            "tags": tags,
            "properties": {"provisioningState": "Succeeded", **properties},
        }

    outputs = {
        "workerResourceId": app_id,
        "environmentResourceId": env_id,
        "diagnosticSettingResourceId": ""
        if parameters.get("ExistingEnvironmentResourceId")
        else diag_id,
        "workerIdentityResourceId": parameters["WorkerIdentityResourceId"],
        "workerIdentityClientId": CLIENT,
        "workerIdentityPrincipalId": PRINCIPAL,
        "registryLoginServer": "sampleregistry.azurecr.io",
        "deployedImage": parameters["Image"],
        "configuredWorkerEnvironment": variables,
    }
    app = resource(
        app_id,
        environmentId=env_id,
        workloadProfileName="Consumption",
        runningStatus="Running",
        latestRevisionName="worker-sample--one",
        latestReadyRevisionName="worker-sample--one",
        configuration={
            "activeRevisionsMode": "Single",
            "ingress": None,
            "secrets": [],
            "registries": [
                {
                    "server": "sampleregistry.azurecr.io",
                    "identity": parameters["WorkerIdentityResourceId"],
                }
            ],
        },
        template={
            "containers": [
                {
                    "name": "monitoring",
                    "image": parameters["Image"],
                    "command": ["python", "-m", "triage.monitoring.worker"],
                    "resources": {"cpu": 0.5, "memory": "1Gi"},
                    "env": [{"name": name, "value": value} for name, value in variables.items()],
                }
            ],
            "scale": {
                "minReplicas": parameters.get("MinReplicas", 1),
                "maxReplicas": 2,
                "rules": [],
            },
        },
    )
    app["identity"] = {
        "type": "UserAssigned",
        "userAssignedIdentities": {parameters["WorkerIdentityResourceId"]: {}},
    }
    diag = resource(
        diag_id,
        workspaceId=parameters["LogAnalyticsWorkspaceResourceId"],
        logs=[
            {"category": name, "enabled": True}
            for name in ("ContainerAppConsoleLogs", "ContainerAppSystemLogs")
        ],
    )
    resources = [
        resource(
            parameters["WorkerIdentityResourceId"],
            tenantId=TENANT,
            clientId=CLIENT,
            principalId=PRINCIPAL,
        ),
        resource(
            parameters["RegistryResourceId"],
            adminUserEnabled=False,
            anonymousPullEnabled=False,
            publicNetworkAccess="Enabled",
            loginServer="sampleregistry.azurecr.io",
            policies={"azureADAuthenticationAsArmPolicy": {"status": "enabled"}},
        ),
        resource(parameters["LogAnalyticsWorkspaceResourceId"]),
        resource(
            env_id,
            workloadProfiles=[{"name": "Consumption", "workloadProfileType": "Consumption"}],
            appLogsConfiguration={"destination": "azure-monitor"},
        ),
        diag,
        app,
    ]
    responses = {
        "account show": {
            "id": SUBSCRIPTION,
            "tenantId": TENANT,
            "environmentName": "AzureCloud",
            "state": "Enabled",
        },
        "group show": {"id": GROUP, "tags": tags},
        "acr repository show": {"digest": "sha256:" + "a" * 64},
        "deployment group validate": {"properties": {"provisioningState": "Succeeded"}},
        "deployment group what-if": {
            "status": "Succeeded",
            "changes": [{"resourceId": app_id, "changeType": "Create"}],
        },
        "deployment group create": {
            "properties": {
                "provisioningState": "Succeeded",
                "outputs": {key: {"value": value} for key, value in outputs.items()},
            },
        },
        f"https://management.azure.com{env_id}/providers/Microsoft.Insights/diagnosticSettings": {
            "value": [diag]
        },
        **{f"https://management.azure.com{item['id']}": item for item in resources},
    }
    return parameters, bootstrap, responses


# A function intercepts every CLI invocation, including Bicep compilation. An
# unmapped operation fails the test instead of falling through to the real az.
FAKE_CLI = r"""
param([string]$ScriptPath, [string]$ParametersPath, [string]$ResponsesPath, [string]$CallsPath)
$ErrorActionPreference = 'Stop'
$global:MonitoringOfflineResponses = Get-Content -LiteralPath $ResponsesPath -Raw | ConvertFrom-Json -AsHashtable
$global:MonitoringOfflineCallsPath = $CallsPath
function global:az {
    $arguments = @($args | ForEach-Object { [string]$_ })
    @{ arguments = $arguments; profile = $env:AZURE_CONFIG_DIR } |
        ConvertTo-Json -Compress | Add-Content -LiteralPath $global:MonitoringOfflineCallsPath
    $global:LASTEXITCODE = 0
    if (($arguments[0..1] -join ' ') -ceq 'bicep build') {
        $file = $arguments[[array]::IndexOf($arguments, '--outfile') + 1]
        '{"resources":[],"parameters":{}}' | Set-Content -LiteralPath $file
        return
    }
    if (($arguments[0..1] -join ' ') -ceq 'account set') { return }
    if ($arguments[0] -ceq 'rest') {
        $key = $arguments[[array]::IndexOf($arguments, '--url') + 1].Split('?')[0]
    } elseif ($arguments[0] -cin @('deployment', 'acr')) {
        $key = $arguments[0..2] -join ' '
    } else { $key = $arguments[0..1] -join ' ' }
    if (-not $global:MonitoringOfflineResponses.Contains($key)) { throw "Unmapped offline az operation: $key" }
    $response = $global:MonitoringOfflineResponses[$key]
    if ($response.Contains('__cli_exit')) {
        $global:LASTEXITCODE = $response['__cli_exit']
        return
    }
    return ($response | ConvertTo-Json -Depth 50 -Compress)
}
try {
    $parameters = Get-Content -LiteralPath $ParametersPath -Raw | ConvertFrom-Json -AsHashtable
    & $ScriptPath @parameters
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    [Console]::Error.WriteLine($_.ScriptStackTrace)
    exit 1
}
"""


def _run(
    tmp_path: Path, parameters: dict, bootstrap: dict, responses: dict
) -> tuple[subprocess.CompletedProcess, list[dict], dict | None]:
    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell 7 is needed for the offline operator-script tests.")
    if "ConnectorBootstrapFile" in parameters:
        Path(parameters["ConnectorBootstrapFile"]).write_text(
            json.dumps(bootstrap), encoding="utf-8"
        )
    parameter_path = tmp_path / "arguments.json"
    parameter_path.write_text(json.dumps(parameters), encoding="utf-8")
    response_path = tmp_path / "responses.json"
    response_path.write_text(json.dumps(responses), encoding="utf-8")
    wrapper = tmp_path / "offline-cli.ps1"
    wrapper.write_text(FAKE_CLI, encoding="utf-8")
    calls_path = tmp_path / "calls.jsonl"
    result = subprocess.run(
        [
            pwsh,
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(wrapper),
            "-ScriptPath",
            str(SCRIPT),
            "-ParametersPath",
            str(parameter_path),
            "-ResponsesPath",
            str(response_path),
            "-CallsPath",
            str(calls_path),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    calls = (
        [json.loads(line) for line in calls_path.read_text(encoding="utf-8-sig").splitlines()]
        if calls_path.exists()
        else []
    )
    artifact = (
        "monitoring-environment" if parameters.get("EnvironmentOnly") else "monitoring-worker"
    )
    report_path = Path(parameters["OutputDirectory"]) / f"{artifact}.readiness.json"
    report = (
        json.loads(report_path.read_text(encoding="utf-8-sig")) if report_path.exists() else None
    )
    return result, calls, report


def test_prepare_is_local_and_persists_nonsecret_parameters(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path)
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode == 0, result.stderr
    assert [call["arguments"][:2] for call in calls] == [["bicep", "build"]]
    assert "--no-restore" in calls[0]["arguments"]
    assert report["status"] == "Prepared"
    assert report["deployed"] is False
    assert report["deploymentAttempted"] is False
    assert report["azurePropertiesVerified"] is False
    assert report["hybridAcceptanceVerified"] is False
    generated = json.loads(
        (Path(parameters["OutputDirectory"]) / "monitoring-worker.parameters.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert generated["parameters"]["connectorBootstrap"]["value"] == bootstrap
    assert generated["parameters"]["environmentExceptionTags"]["value"] == {}
    assert generated["parameters"]["minReplicas"]["value"] == 1
    assert "NatGatewayResourceId" not in generated["parameters"]
    assert "workerSubnetResourceId" not in generated["parameters"]
    assert all(
        not any("/Microsoft.Network/" in str(part) for part in call["arguments"])
        for call in calls
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"Execute": True}, "-Execute requires -Mode WhatIf"),
        ({"Image": "sampleregistry.azurecr.io/monitoring:latest"}, "sha256 digest"),
        ({"AzureSqlServer": "server;Password=fake"}, "DNS hostname"),
        ({"TenantId": "00000000-0000-0000-0000-000000000000"}, "nonempty GUIDs"),
        (
            {"LogAnalyticsWorkspaceResourceId": f"{GROUP}/providers/Microsoft.Web/sites/not-a-workspace"},
            "Microsoft.OperationalInsights/workspaces",
        ),
        ({"Mode": "Preflight", "AzureConfigDirectory": ""}, "separately authenticated"),
    ],
)
def test_invalid_inputs_never_reach_azure(tmp_path: Path, overrides: dict, message: str) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, **overrides)
    result, calls, _ = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert message in result.stderr
    assert calls == []


def test_bootstrap_rejects_extra_credential_or_target_fields(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path)
    bootstrap["sharedKey"] = "not-a-credential"
    result, calls, _ = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "exactly the documented nonsecret fields" in result.stderr
    assert calls == []


def test_existing_output_is_not_overwritten(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path)
    output = Path(parameters["OutputDirectory"])
    output.mkdir()
    previous = output / "operator.txt"
    previous.write_text("keep", encoding="utf-8")
    result, calls, _ = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "new or empty" in result.stderr
    assert previous.read_text(encoding="utf-8") == "keep"
    assert calls == []


def test_exception_tags_are_explicit_and_scoped_to_a_new_environment(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path)
    tags = {
        "NetworkException": "reviewed",
        "ExceptionReason": "Approved environment-specific connectivity review",
        "ExceptionReviewAfter": "2099-12-31",
    }
    path = tmp_path / "environment-exception.json"
    path.write_text(json.dumps(tags), encoding="utf-8")
    parameters["EnvironmentExceptionTagsFile"] = str(path)
    result, _, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode == 0, result.stderr
    generated = json.loads(
        (Path(parameters["OutputDirectory"]) / "monitoring-worker.parameters.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert generated["parameters"]["environmentExceptionTags"]["value"] == tags
    assert "NetworkException" not in generated["parameters"]["tags"]["value"]
    assert report["deployed"] is False


@pytest.mark.parametrize(
    "exception",
    [
        {"NetworkException": "reviewed"},
        {"ExceptionReason": "review", "ExceptionReviewAfter": "2000-01-01"},
        {"ExceptionReason": "review", "ExceptionReviewAfter": "2099-12-31", "Owner": "override"},
    ],
)
def test_exception_tags_require_a_current_review_and_preserve_governance(
    tmp_path: Path, exception: dict
) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path)
    path = tmp_path / "invalid-exception.json"
    path.write_text(json.dumps(exception), encoding="utf-8")
    parameters["EnvironmentExceptionTagsFile"] = str(path)
    result, calls, _ = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert calls == []


def test_wrong_tenant_stops_before_resource_reads(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses["account show"]["tenantId"] = PRINCIPAL
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "does not match" in result.stderr
    assert [call["arguments"][:2] for call in calls] == [
        ["bicep", "build"],
        ["account", "set"],
        ["account", "show"],
    ]
    assert calls[-1]["profile"] == parameters["AzureConfigDirectory"]
    assert report["status"] == "Failed"
    assert report["deployed"] is False


def test_what_if_never_deploys_without_execute(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf")
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode == 0, result.stderr
    operations = [call["arguments"][:3] for call in calls]
    assert ["deployment", "group", "validate"] in operations
    assert ["deployment", "group", "what-if"] in operations
    assert ["deployment", "group", "create"] not in operations
    assert report["status"] == "WhatIfPassed"
    assert report["deployed"] is False
    assert report["hybridAcceptanceVerified"] is False


def test_arm_region_display_names_match_canonical_location_codes(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(
        tmp_path, Location="eastus2", Mode="WhatIf", Execute=True
    )
    for response in responses.values():
        if isinstance(response, dict) and "location" in response:
            response["location"] = "East US 2"
    result, _, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode == 0, result.stderr
    assert report["status"] == "InfrastructureVerified"


def test_tenant_admin_inventory_mode_reaches_parameters_and_runtime_env(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(
        tmp_path,
        InventoryMode="tenant_admin_preview",
        Mode="WhatIf",
        Execute=True,
    )
    result, _, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode == 0, result.stderr
    assert report["status"] == "InfrastructureVerified"
    document = json.loads(
        (Path(parameters["OutputDirectory"]) / "monitoring-worker.parameters.json").read_text(
            encoding="utf-8-sig",
        )
    )
    assert document["parameters"]["inventoryMode"]["value"] == "tenant_admin_preview"


def test_invalid_inventory_mode_is_rejected_before_azure(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, InventoryMode="all")
    result, calls, _ = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert calls == []


@pytest.mark.parametrize(
    "change",
    [
        {
            "resourceId": f"{GROUP}/providers/Microsoft.Web/sites/existing-ui",
            "changeType": "Modify",
        },
        {
            "resourceId": f"{GROUP}/providers/Microsoft.App/containerApps/worker-sample",
            "changeType": "Delete",
        },
        {
            "resourceId": f"{GROUP}/providers/Microsoft.App/containerApps/worker-sample",
            "changeType": "Unsupported",
        },
    ],
)
def test_execute_refuses_unsafe_what_if_changes(tmp_path: Path, change: dict) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses["deployment group what-if"]["changes"] = [change]
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "What-if" in result.stderr
    assert ["deployment", "group", "create"] not in [call["arguments"][:3] for call in calls]
    assert report["deployed"] is False


def test_what_if_requires_a_change_manifest_even_with_success_status(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses["deployment group what-if"]["changes"] = None
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "change manifest" in result.stderr
    assert ["deployment", "group", "create"] not in [call["arguments"][:3] for call in calls]
    assert report["deploymentAttempted"] is False


@pytest.mark.parametrize(
    ("resource_parameter", "property_name", "value", "message"),
    [
        ("RegistryResourceId", "adminUserEnabled", True, "admin authentication"),
        ("RegistryResourceId", "anonymousPullEnabled", True, "anonymous pull"),
        ("RegistryResourceId", "anonymousPullEnabled", None, "anonymous pull"),
        ("RegistryResourceId", "publicNetworkAccess", "Disabled", "public network endpoint"),
        (
            "RegistryResourceId",
            "policies",
            {"azureADAuthenticationAsArmPolicy": {"status": "disabled"}},
            "ARM-audience authentication",
        ),
    ],
)
def test_preflight_fails_closed_on_unsafe_prerequisites(
    tmp_path: Path, resource_parameter: str, property_name: str, value: object, message: str
) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses[f"https://management.azure.com{parameters[resource_parameter]}"]["properties"][
        property_name
    ] = value
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert message in result.stderr
    assert ["deployment", "group", "create"] not in [call["arguments"][:3] for call in calls]
    assert report["deployed"] is False


@pytest.mark.parametrize(
    "operation",
    ["deployment group validate", "deployment group what-if", "deployment group create"],
)
def test_exit_zero_is_not_success_evidence(tmp_path: Path, operation: str) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses[operation] = {"status": "Failed", "properties": {"provisioningState": "Failed"}}
    result, _, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    expected = {
        "deployment group validate": "ARM validation did not return Succeeded",
        "deployment group what-if": "What-if did not return a successful",
        "deployment group create": "Deployment did not return Succeeded",
    }
    assert expected[operation] in result.stderr
    assert report["status"] == "Failed"
    assert report["deployed"] is False


def test_cli_failure_is_not_replaced_with_empty_success(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses["deployment group validate"] = {"__cli_exit": 9}
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "exit code 9" in result.stderr
    assert ["deployment", "group", "create"] not in [call["arguments"][:3] for call in calls]
    assert report["deployed"] is False


def test_a_lost_deployment_reply_preserves_the_attempt_receipt(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    responses["deployment group create"] = {"__cli_exit": 9}
    result, _, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "exit code 9" in result.stderr
    assert report["deployed"] is False
    assert report["deploymentAttempted"] is True
    assert report["deploymentAcknowledged"] is False


@pytest.mark.parametrize(
    "problem", ["internal", "consumption-only", "no-logs", "wrong-log-workspace", "unreviewed-tags"]
)
def test_reuse_requires_a_public_preconfigured_environment(tmp_path: Path, problem: str) -> None:
    env_id = f"{GROUP}/providers/Microsoft.App/managedEnvironments/existing-env"
    parameters, bootstrap, responses = _fixture(
        tmp_path,
        Mode="WhatIf",
        Execute=True,
        EnvironmentName="",
        ExistingEnvironmentResourceId=env_id,
    )
    environment = responses[f"https://management.azure.com{env_id}"]["properties"]
    settings_key = (
        f"https://management.azure.com{env_id}/providers/Microsoft.Insights/diagnosticSettings"
    )
    if problem == "internal":
        environment["vnetConfiguration"] = {"internal": True}
    elif problem == "consumption-only":
        environment["workloadProfiles"] = []
    elif problem == "no-logs":
        responses[settings_key]["value"] = []
    elif problem == "wrong-log-workspace":
        responses[settings_key]["value"][0]["properties"]["workspaceId"] = "different"
    else:
        parameters["EnvironmentExceptionTagsFile"] = str(tmp_path / "not-read.json")
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert ["deployment", "group", "create"] not in [call["arguments"][:3] for call in calls]
    if report:
        assert report["deployed"] is False


def test_reuse_accepts_an_existing_all_logs_route_without_changing_it(tmp_path: Path) -> None:
    env_id = f"{GROUP}/providers/Microsoft.App/managedEnvironments/existing-env"
    parameters, bootstrap, responses = _fixture(
        tmp_path,
        Mode="WhatIf",
        Execute=True,
        EnvironmentName="",
        ExistingEnvironmentResourceId=env_id,
    )
    settings_key = (
        f"https://management.azure.com{env_id}/providers/Microsoft.Insights/diagnosticSettings"
    )
    responses[settings_key]["value"][0]["properties"]["logs"] = [
        {"categoryGroup": "allLogs", "enabled": True}
    ]
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode == 0, result.stderr
    assert report["deployed"] is True
    assert all(
        call["arguments"][call["arguments"].index("--method") + 1] == "get"
        for call in calls
        if call["arguments"][0] == "rest"
    )


def test_execute_verifies_actual_properties_not_only_outputs(tmp_path: Path) -> None:
    parameters, bootstrap, responses = _fixture(tmp_path, Mode="WhatIf", Execute=True)
    app_id = f"{GROUP}/providers/Microsoft.App/containerApps/worker-sample"
    responses[f"https://management.azure.com{app_id}"]["properties"]["configuration"]["ingress"] = {
        "external": True
    }
    result, _, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode != 0
    assert "no HTTP or TCP ingress" in result.stderr
    assert report["deployed"] is False
    assert report["deploymentAttempted"] is True
    assert report["deploymentAcknowledged"] is True


@pytest.mark.parametrize("reuse_environment", [False, True])
def test_execute_verifies_infrastructure_but_not_hybrid_acceptance(
    tmp_path: Path, reuse_environment: bool
) -> None:
    overrides = {"Mode": "WhatIf", "Execute": True, "MinReplicas": 2}
    if reuse_environment:
        overrides.update(
            EnvironmentName="",
            ExistingEnvironmentResourceId=f"/subscriptions/{SUBSCRIPTION}/resourceGroups/shared-worker-env/providers/Microsoft.App/managedEnvironments/shared-env",
        )
    parameters, bootstrap, responses = _fixture(tmp_path, **overrides)
    result, calls, report = _run(tmp_path, parameters, bootstrap, responses)
    assert result.returncode == 0, result.stderr
    assert report["status"] == "InfrastructureVerified"
    assert report["deployed"] is True
    assert report["hybridAcceptanceVerified"] is False
    operations = [call["arguments"][:3] for call in calls]
    assert operations.count(["deployment", "group", "create"]) == 1
    create_index = operations.index(["deployment", "group", "create"])
    assert operations[create_index - 2][:2] == ["account", "set"]
    assert operations[create_index - 1][:2] == ["account", "show"]
    assert all("--tenant" not in call["arguments"] for call in calls)
