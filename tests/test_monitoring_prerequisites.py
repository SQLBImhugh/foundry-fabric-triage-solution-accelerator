from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BICEP = ROOT / "infra" / "monitoring-prerequisites.bicep"
OWNED_TYPES = {
    "Microsoft.ManagedIdentity/userAssignedIdentities",
    "Microsoft.ContainerRegistry/registries",
    "Microsoft.Authorization/roleAssignments",
}


def test_prerequisites_write_only_the_three_public_network_resources() -> None:
    text = BICEP.read_text("utf-8")
    declarations = re.findall(r"^resource \w+ '([^@']+)@[^']+'( existing)?", text, re.M)
    assert {kind for kind, existing in declarations if not existing} == OWNED_TYPES
    assert not any(existing for _, existing in declarations)
    assert "targetScope = 'resourceGroup'" in text
    assert "Microsoft.Network/" not in text
    for forbidden in (
        "Microsoft.Web/", "Microsoft.App/containerApps@", "Microsoft.App/managedEnvironments@",
        "resourceGroups@", "subnets: [", "app-integration", "scmIpSecurityRestrictions",
    ):
        assert forbidden not in text


def test_prerequisite_parameters_have_no_private_deployment_defaults() -> None:
    text = BICEP.read_text("utf-8")
    for name in (
        "location", "workerIdentityName", "registryName",
    ):
        assert re.search(rf"^param {name} string$", text, re.M)
    assert "param tags GovernanceTags" in text
    assert "param registryExceptionTags RegistryExceptionTags?" in text
    assert re.findall(r"^output (\w+) ", text, re.M) == [
        "resourceGroupId", "workerIdentityResourceId", "workerIdentityClientId",
        "workerIdentityPrincipalId",
        "registryResourceId", "registryLoginServer", "acrPullRoleAssignmentId",
    ]


def test_registry_uses_entra_pull_and_narrowly_scoped_exceptions() -> None:
    text = BICEP.read_text("utf-8")
    assert "name: 'Basic'" in text
    assert "adminUserEnabled: false" in text
    assert "anonymousPullEnabled: false" in text
    assert "publicNetworkAccess: 'Enabled'" in text
    assert "roleAssignmentMode: 'LegacyRegistryPermissions'" in text
    assert "azureADAuthenticationAsArmPolicy:" in text
    assert "scope: registry" in text
    assert "principalId: workerIdentity.properties.principalId" in text
    assert "principalType: 'ServicePrincipal'" in text
    assert "'7f951dda-4ed3-4680-a7ca-43fe172d538d'" in text
    assert text.count("tags: union(registryExceptionTags ?? {}, tags)") == 1
    assert text.count("tags: tags") == 1
    for forbidden in (
        "listKeys(", "listCredentials(", "password:", "secret:", "connectionString",
        "CostControl", "networkRuleSet:", "anonymousPullEnabled: true",
    ):
        assert forbidden not in text


def test_compiled_prerequisites_when_supplied() -> None:
    """The default suite never invokes Azure CLI or restores a Bicep package."""
    path = os.environ.get("MONITORING_PREREQUISITES_COMPILED_TEMPLATE")
    if not path:
        pytest.skip("Supply the locally compiled prerequisite ARM template for this gate.")
    template = json.loads(Path(path).read_text("utf-8-sig"))
    resources = template["resources"]
    if isinstance(resources, dict):
        resources = list(resources.values())
    deployed = [resource for resource in resources if not resource.get("existing", False)]
    assert len(deployed) == 3
    assert {resource["type"] for resource in deployed} == OWNED_TYPES
    by_type = {resource["type"]: resource for resource in deployed}
    registry = by_type["Microsoft.ContainerRegistry/registries"]
    assert registry["sku"] == {"name": "Basic"}
    assert registry["properties"] == {
        "adminUserEnabled": False,
        "anonymousPullEnabled": False,
        "publicNetworkAccess": "Enabled",
        "roleAssignmentMode": "LegacyRegistryPermissions",
        "policies": {"azureADAuthenticationAsArmPolicy": {"status": "enabled"}},
    }
    assert not any(resource["type"].startswith("Microsoft.Network/") for resource in resources)
    role = by_type["Microsoft.Authorization/roleAssignments"]
    assert role["properties"]["principalType"] == "ServicePrincipal"
    assert role["properties"]["roleDefinitionId"] == "[variables('acrPullRoleId')]"
    assert "Microsoft.ContainerRegistry/registries" in role["scope"]
    assert "workerIdentity" in role["properties"]["principalId"]
    exception_parameter = template["parameters"]["registryExceptionTags"]
    assert exception_parameter["nullable"] is True
    assert "defaultValue" not in exception_parameter
    governance = template["definitions"]["GovernanceTags"]
    assert set(governance["properties"]) == {
        "Owner", "CostCenter", "Environment", "DataClassification",
    }
    assert governance["additionalProperties"] is False
    assert all(value == {"type": "string"} for value in governance["properties"].values())
    exception = template["definitions"]["RegistryExceptionTags"]
    assert exception["additionalProperties"] is False
    assert exception["properties"]["SecurityControl"]["allowedValues"] == ["Ignore"]
    assert set(exception["properties"]) == {
        "SecurityControl", "ExceptionReason", "ExceptionReviewAfter",
    }
    serialized = json.dumps(template).casefold()
    for forbidden in ("listkeys(", "listcredentials(", "password", "sharedaccesskey"):
        assert forbidden not in serialized
