from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra"


@pytest.mark.parametrize("path", sorted(INFRA.glob("*.bicep")), ids=lambda path: path.name)
def test_every_shipped_template_uses_public_networking_without_private_prerequisites(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert "publicNetworkAccess: 'Disabled'" not in text
    for forbidden in (
        "Microsoft.Network/privateEndpoints@", "Microsoft.Network/privateDnsZones",
        "Microsoft.Network/virtualNetworks", "Microsoft.Network/natGateways",
        "Microsoft.Network/publicIPAddresses", "networkInjections:", "internal: true",
    ):
        assert forbidden not in text, (path.name, forbidden)
    for forbidden in ("listKeys(", "listCredentials(", "administratorLoginPassword:", "adminUserEnabled: true"):
        assert forbidden not in text


def test_retired_private_only_templates_are_not_shipped_as_customer_prerequisites() -> None:
    for name in (
        "foundry-private-state.bicep", "foundry-private-state-network.bicep",
        "foundry-private-state-capabilities.bicep", "registry-private-state.bicep",
        "monitoring-worker-network.bicep",
    ):
        assert not (INFRA / name).exists()


def test_public_registry_prerequisites_create_only_identity_registry_and_pull_grant() -> None:
    text = (INFRA / "monitoring-prerequisites.bicep").read_text(encoding="utf-8")
    declarations = re.findall(r"^resource \w+ '([^@']+)@[^']+'( existing)?", text, re.M)
    assert {kind for kind, _ in declarations} == {
        "Microsoft.ManagedIdentity/userAssignedIdentities",
        "Microsoft.ContainerRegistry/registries",
        "Microsoft.Authorization/roleAssignments",
    }
    assert not any(existing for _, existing in declarations)
    assert "name: 'Basic'" in text
    assert "publicNetworkAccess: 'Enabled'" in text
    assert "adminUserEnabled: false" in text and "anonymousPullEnabled: false" in text
    assert "roleAssignmentMode: 'LegacyRegistryPermissions'" in text
    assert "scope: registry" in text
    assert "param registryExceptionTags RegistryExceptionTags?" in text
    assert "tags: union(registryExceptionTags ?? {}, tags)" in text


def test_public_web_networking_does_not_enable_basic_publishing_or_remove_auth_settings() -> None:
    text = (INFRA / "command-center.bicep").read_text(encoding="utf-8")
    assert "publicNetworkAccess: 'Enabled'" in text and "httpsOnly: true" in text
    assert text.count("allow: false") == 2
    assert "COMMAND_CENTER_TENANT_ID: tenantId" in text
    assert "COMMAND_CENTER_CLIENT_ID: applicationClientId" in text
    assert "type: 'UserAssigned'" in text
    assert "ipSecurityRestrictionsDefaultAction: empty(publicAccessClientCidrs) ? 'Allow' : 'Deny'" in text
    assert "scmIpSecurityRestrictionsDefaultAction: empty(scmAccessClientCidrs) ? 'Allow' : 'Deny'" in text
    assert "scmIpSecurityRestrictionsUseMain: false" in text


def test_deployment_helpers_do_not_require_or_restore_private_networking() -> None:
    web = (ROOT / "scripts" / "deploy_command_center.ps1").read_text(encoding="utf-8")
    worker = (ROOT / "scripts" / "deploy_monitoring_worker.ps1").read_text(encoding="utf-8")
    for forbidden in ("TemporaryPublicAccess", "VnetAddressPrefix", "PrivateEndpointSubnetPrefix", "properties.publicNetworkAccess=Disabled"):
        assert forbidden not in web
    for forbidden in ("WorkerSubnetResourceId", "NatGatewayResourceId", "defaultOutboundAccess"):
        assert forbidden not in worker
    assert "Assert-ManagedAccessContinuity" in web
    assert "Confirm-WhatIf" in worker and "Confirm-WorkerProperties" in worker


def test_compiled_web_public_admission_and_no_private_resources() -> None:
    folder = os.environ.get("PUBLIC_INFRA_COMPILED_DIRECTORY")
    if not folder:
        pytest.skip("Supply a locally compiled public template; no Azure calls in tests.")
    template = json.loads((Path(folder) / "command-center.arm.json").read_text(encoding="utf-8-sig"))
    resources = template["resources"]
    resources = list(resources.values()) if isinstance(resources, dict) else resources
    assert all(not resource["type"].startswith("Microsoft.Network/") for resource in resources)
    web, = [resource for resource in resources if resource["type"] == "Microsoft.Web/sites"]
    assert web["properties"]["publicNetworkAccess"] == "Enabled"
    assert "virtualNetworkSubnetId" not in web["properties"]
    assert "outboundVnetRouting" not in web["properties"]
    site = web["properties"]["siteConfig"]
    assert site["ftpsState"] == "Disabled" and site["minTlsVersion"] == site["scmMinTlsVersion"] == "1.2"
    assert template["parameters"]["publicAccessClientCidrs"]["defaultValue"] == []
    assert template["parameters"]["scmAccessClientCidrs"]["defaultValue"] == []
