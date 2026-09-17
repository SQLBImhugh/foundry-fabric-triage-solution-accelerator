from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FOUNDATION = ROOT / "infra" / "foundry.bicep"
CAPABILITIES = ROOT / "infra" / "foundry-capabilities.bicep"
FOUNDRY_USER = "53ca6127-db72-4b80-b1b0-d745d6d5456d"


def _source(path: Path, name: str) -> str:
    match = re.search(
        rf"^resource {re.escape(name)} '[^']+'(?: existing)? = (.*?)(?=^resource |^output |\Z)",
        path.read_text(encoding="utf-8"), re.M | re.S,
    )
    assert match is not None
    return match[1]


def _resources(template: dict, kind: str) -> list[dict]:
    resources = template["resources"]
    if isinstance(resources, dict):
        resources = list(resources.values())
    return [
        resource for resource in resources
        if resource["type"] == kind and not resource.get("existing", False)
    ]


def test_foundation_has_public_networking_and_entra_only_authentication() -> None:
    account = _source(FOUNDATION, "account")
    assert "publicNetworkAccess: 'Enabled'" in account
    assert "disableLocalAuth: true" in account
    assert "defaultAction: 'Allow'" in account
    assert "bypass: 'None'" in account
    assert "type: 'SystemAssigned'" in account
    assert "tags: union(accountNetworkExceptionTags, tags)" in account
    text = FOUNDATION.read_text(encoding="utf-8")
    assert "param accountNetworkExceptionTags object = {}" in text
    for forbidden in ("networkInjections:", "managedNetworks", "Microsoft.Network/", "subnetArmId"):
        assert forbidden not in text


def test_foundry_roles_are_scoped_and_do_not_grant_network_or_sql_authority() -> None:
    text = FOUNDATION.read_text(encoding="utf-8")
    assert FOUNDRY_USER in text
    assert text.count("Microsoft.Authorization/roleAssignments@") == 3
    assert "principalType: 'User'" in _source(FOUNDATION, "operatorProjectAccess")
    assert "principalId: webIdentityObjectId" in _source(FOUNDATION, "webProjectAccess")
    assert "principalId: project.identity.principalId" in _source(FOUNDATION, "projectModelAccess")
    assert "scope: account" in _source(FOUNDATION, "projectModelAccess")
    for name in ("operatorProjectAccess", "webProjectAccess"):
        assert "scope: project" in _source(FOUNDATION, name)
    assert "b556d68e-0be0-4f35-a333-ad7ee1ce17ea" not in text
    assert "Microsoft.Sql/" not in text


def test_model_and_telemetry_remain_explicit_and_content_safe() -> None:
    model = _source(FOUNDATION, "modelDeployment")
    assert "name: modelName" in model and "version: modelVersion" in model
    assert "capacity: modelCapacity" in model
    assert "raiPolicyName: 'Microsoft.DefaultV2'" in model
    assert "versionUpgradeOption: 'NoAutoUpgrade'" in model
    metrics = _source(FOUNDATION, "accountMetrics")
    assert "category: 'AllMetrics'" in metrics
    assert "logs:" not in metrics
    assert "output networkAndRuntimeAcceptanceRequired bool = true" in FOUNDATION.read_text()


def test_optional_capability_hosts_require_explicit_inspected_scope_and_names() -> None:
    text = CAPABILITIES.read_text(encoding="utf-8")
    for name in ("createAccountCapabilityHost", "createProjectCapabilityHost"):
        assert re.search(rf"^param {name} bool$", text, re.M)
    for name in ("accountCapabilityHostName", "projectCapabilityHostName"):
        assert re.search(rf"^param {name} string$", text, re.M)
    assert _source(CAPABILITIES, "accountCapabilityHost").startswith("if (createAccountCapabilityHost)")
    project = _source(CAPABILITIES, "projectCapabilityHost")
    assert project.startswith("if (createProjectCapabilityHost)")
    assert "properties: {}" in project and "capabilityHostKind:" not in project
    assert "name: 'default'" not in text
    assert "Microsoft.Network/" not in text


@pytest.mark.parametrize("forbidden", [
    "listKeys(", "listSecrets(", "AZURE_CLIENT_SECRET", "password:", "credentials:",
    "connectionString", "Microsoft.DocumentDB/", "Microsoft.Storage/storageAccounts",
    "Microsoft.Search/searchServices", "Microsoft.Web/", "Microsoft.App/",
])
def test_foundry_templates_do_not_add_credentials_or_unrelated_services(forbidden: str) -> None:
    assert forbidden not in FOUNDATION.read_text() + CAPABILITIES.read_text()


@pytest.fixture
def compiled() -> dict[str, dict]:
    folder = os.environ.get("PUBLIC_INFRA_COMPILED_DIRECTORY")
    if not folder:
        pytest.skip("Supply locally compiled public templates; default tests never invoke Azure.")
    return {
        name: json.loads((Path(folder) / f"{name}.arm.json").read_text(encoding="utf-8-sig"))
        for name in ("foundry", "foundry-capabilities")
    }


def test_compiled_public_account_and_model_are_the_actual_declared_contract(compiled: dict) -> None:
    template = compiled["foundry"]
    account, = _resources(template, "Microsoft.CognitiveServices/accounts")
    assert account["identity"] == {"type": "SystemAssigned"}
    assert account["properties"] == {
        "allowProjectManagement": True,
        "customSubDomainName": "[parameters('accountName')]",
        "disableLocalAuth": True,
        "publicNetworkAccess": "Enabled",
        "networkAcls": {"defaultAction": "Allow", "bypass": "None", "ipRules": [], "virtualNetworkRules": []},
    }
    assert template["parameters"]["accountNetworkExceptionTags"]["defaultValue"] == {}
    model, = _resources(template, "Microsoft.CognitiveServices/accounts/deployments")
    assert model["properties"] == {
        "model": {"format": "OpenAI", "name": "[parameters('modelName')]", "version": "[parameters('modelVersion')]"},
        "raiPolicyName": "Microsoft.DefaultV2",
        "versionUpgradeOption": "NoAutoUpgrade",
    }
    roles = _resources(template, "Microsoft.Authorization/roleAssignments")
    assert len(roles) == 3
    assert sum("projects" in role["scope"] for role in roles) == 2
    assert all("accounts" in role["scope"] for role in roles)
    metrics, = _resources(template, "Microsoft.Insights/diagnosticSettings")
    assert metrics["properties"]["metrics"] == [{"category": "AllMetrics", "enabled": True}]
    assert "logs" not in metrics["properties"]
    assert template["outputs"]["networkAndRuntimeAcceptanceRequired"]["value"] is True


def test_compiled_capability_host_creation_remains_opt_in(compiled: dict) -> None:
    hosts = [
        resource for resource in compiled["foundry-capabilities"]["resources"]
        if resource["type"].endswith("/capabilityHosts")
    ]
    assert len(hosts) == 2
    assert {host["condition"] for host in hosts} == {
        "[parameters('createAccountCapabilityHost')]",
        "[parameters('createProjectCapabilityHost')]",
    }
