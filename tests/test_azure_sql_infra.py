from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BICEP = ROOT / "infra" / "state-sql.bicep"


def _source_resource(name: str) -> str:
    text = BICEP.read_text(encoding="utf-8")
    match = re.search(rf"^resource {re.escape(name)} '[^']+' = (.*?)^\}}", text, re.M | re.S)
    assert match is not None, f"Missing declared resource: {name}"
    return match[1]


def test_template_only_creates_sql_auditing_and_public_firewall_admission() -> None:
    text = BICEP.read_text(encoding="utf-8")
    declarations = re.findall(r"^resource \w+ '([^@']+)@[^']+'( existing)?", text, re.M)
    assert {kind for kind, existing in declarations if not existing} == {
        "Microsoft.Sql/servers",
        "Microsoft.Sql/servers/connectionPolicies",
        "Microsoft.Sql/servers/auditingSettings",
        "Microsoft.Sql/servers/databases",
        "Microsoft.Sql/servers/databases/backupShortTermRetentionPolicies",
        "Microsoft.Sql/servers/databases/transparentDataEncryption",
        "Microsoft.Insights/diagnosticSettings",
        "Microsoft.Sql/servers/firewallRules",
    }
    assert {kind for kind, existing in declarations if existing} == {
        "Microsoft.Sql/servers/databases",
    }
    assert "module " not in text
    assert "targetScope = 'resourceGroup'" in text


def test_server_is_public_and_entra_only_in_the_create_request() -> None:
    server = _source_resource("sqlServer")
    assert "publicNetworkAccess: 'Enabled'" in server
    assert "minimalTlsVersion: '1.2'" in server
    assert "administrators: {" in server
    assert "administratorType: 'ActiveDirectory'" in server
    assert "azureADOnlyAuthentication: true" in server
    assert "login: entraAdminDisplayName" in server
    assert "sid: entraAdminObjectId" in server
    assert "tenantId: tenantId" in server
    text = BICEP.read_text(encoding="utf-8")
    assert "'Group'\n  'User'" in text
    for name in ("entraAdminDisplayName", "entraAdminObjectId", "tenantId"):
        assert re.search(rf"^param {name} string$", text, re.M)
    assert "connectionType: 'Proxy'" in _source_resource("connectionPolicy")


@pytest.mark.parametrize(
    "forbidden",
    [
        "administratorLogin:",
        "administratorLoginPassword:",
        "listKeys(",
        "listCredentials(",
        "storageAccountAccessKey:",
        "storageEndpoint:",
        "connectionString",
        "CONNECTION_STRING",
        "AZURE_CLIENT_SECRET",
        "virtualNetworkRules@",
        "roleAssignments@",
        "userAssignedIdentities@",
        "deploymentScripts@",
        "elasticPools@",
        "elasticPoolId:",
        "SecurityControl",
        "CostControl",
        "SetByMCAPSGovPolicy_",
    ],
)
def test_template_has_no_credentials_or_unrelated_authority(forbidden: str) -> None:
    assert forbidden not in BICEP.read_text(encoding="utf-8")


def test_application_database_and_optional_proof_keep_state_in_one_catalog() -> None:
    text = BICEP.read_text(encoding="utf-8")
    application = _source_resource("applicationDatabase")
    proof = _source_resource("proofDatabase")
    assert "param databaseSkuName string = 'S1'" in text
    assert "param databaseMaxSizeGiB int = 10" in text
    assert "S1: 20" in text
    assert "name: databaseName" in application
    assert "tier: 'Standard'" in application
    assert "capacity: dtuCapacity[databaseSkuName]" in application
    assert "param enableProofDatabase bool = false" in text
    assert "var proofDatabaseName = '${databaseName}-proof'" in text
    assert proof.startswith("if (enableProofDatabase)")
    assert "name: proofDatabaseName" in proof
    assert "name: 'Basic'" in proof
    assert "capacity: 5" in proof
    assert "maxSizeBytes: 2147483648" in proof
    assert len(re.findall(r"^resource \w+ 'Microsoft.Sql/servers/databases@[^']+' =", text, re.M)) == 2


def test_both_databases_preserve_unicode_and_explicit_backup_encryption_defaults() -> None:
    text = BICEP.read_text(encoding="utf-8")
    assert "var databaseCollation = 'SQL_Latin1_General_CP1_CI_AS'" in text
    for name in ("applicationDatabase", "proofDatabase"):
        resource = _source_resource(name)
        assert "collation: databaseCollation" in resource
        assert "catalogCollation: 'DATABASE_DEFAULT'" in resource
        assert "requestedBackupStorageRedundancy: 'Local'" in resource
        assert "isLedgerOn: false" in resource
        assert "zoneRedundant: false" in resource
        assert "createMode: 'Default'" in resource
        assert "autoPauseDelay:" not in resource
        assert "compatibilityLevel:" not in resource
    for name in ("applicationBackups", "proofBackups"):
        resource = _source_resource(name)
        assert "retentionDays: 7" in resource
        assert "diffBackupIntervalInHours: 24" in resource
    for name in ("applicationEncryption", "proofEncryption"):
        assert "state: 'Enabled'" in _source_resource(name)
    for name in ("proofBackups", "proofEncryption"):
        assert _source_resource(name).startswith("if (enableProofDatabase)")
    assert "output requiredCompatibilityLevel int = 160" in text
    assert "output liveAcceptanceRequired bool = true" in text


def test_auditing_reaches_azure_monitor_before_application_catalog_creation() -> None:
    audit = _source_resource("serverAudit")
    diagnostics = _source_resource("auditDiagnostics")
    assert "state: 'Enabled'" in audit
    assert "isAzureMonitorTargetEnabled: true" in audit
    assert "scope: masterDatabase" in diagnostics
    assert "category: 'SQLSecurityAuditEvents'" in diagnostics
    assert "workspaceId: logAnalyticsWorkspaceResourceId" in diagnostics
    assert "enabled: true" in diagnostics
    assert "auditDiagnostics" in audit.split("dependsOn:", 1)[1]
    for name in ("applicationDatabase", "proofDatabase"):
        assert "serverAudit" in _source_resource(name).split("dependsOn:", 1)[1]
    assert "serverAudit" in _source_resource("azureServicesFirewallRule").split("dependsOn:", 1)[1]


def test_public_firewall_admission_is_explicit_without_private_infrastructure() -> None:
    text = BICEP.read_text(encoding="utf-8")
    rule = _source_resource("azureServicesFirewallRule")
    assert rule.startswith("if (allowAzureServices)")
    assert "param allowAzureServices bool = true" in text
    assert "startIpAddress: '0.0.0.0'" in rule
    assert "endIpAddress: '0.0.0.0'" in rule
    assert "other Azure subscriptions" in text
    assert "param clientFirewallRules SqlFirewallRule[] = []" in text
    assert "startIpAddress: rule.startIpAddress" in text
    assert "endIpAddress: rule.endIpAddress" in text
    assert "255.255.255.255" not in text and "0.0.0.0/0" not in text
    assert "Microsoft.Network/" not in text


def test_all_tag_capable_resources_use_the_existing_governance_contract() -> None:
    text = BICEP.read_text(encoding="utf-8")
    assert "import { GovernanceTags } from './monitoring-environment.bicep'" in text
    assert "param tags GovernanceTags" in text
    assert "tags: union(sqlNetworkExceptionTags, tags)" in _source_resource("sqlServer")
    assert "param sqlNetworkExceptionTags object = {}" in text
    for name in ("applicationDatabase", "proofDatabase"):
        assert "tags: tags" in _source_resource(name)


@pytest.fixture
def compiled_template() -> dict:
    """Only read an explicitly supplied local build; pytest never invokes Azure."""
    path = os.environ.get("AZURE_SQL_INFRA_COMPILED_TEMPLATE")
    if not path:
        pytest.skip("Set AZURE_SQL_INFRA_COMPILED_TEMPLATE to the locally compiled ARM template.")
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


@pytest.mark.parametrize("removed_input", [
    "endpointVirtualNetworkName", "privateEndpointSubnetName", "monitoringVirtualNetworkName",
])
def test_public_sql_has_no_mandatory_private_network_inputs(removed_input) -> None:
    assert removed_input not in BICEP.read_text(encoding="utf-8")


def _compiled_resources(template: dict, kind: str) -> list[dict]:
    resources = template["resources"]
    if isinstance(resources, dict):
        resources = list(resources.values())
    return [
        resource for resource in resources
        if resource["type"] == kind and not resource.get("existing", False)
    ]


def test_compiled_server_authentication_and_tag_parameters(compiled_template: dict) -> None:
    server, = _compiled_resources(compiled_template, "Microsoft.Sql/servers")
    properties = server["properties"]
    assert properties["publicNetworkAccess"] == "Enabled"
    assert properties["minimalTlsVersion"] == "1.2"
    assert properties["administrators"] == {
        "administratorType": "ActiveDirectory",
        "azureADOnlyAuthentication": True,
        "login": "[parameters('entraAdminDisplayName')]",
        "principalType": "[parameters('entraAdminPrincipalType')]",
        "sid": "[parameters('entraAdminObjectId')]",
        "tenantId": "[parameters('tenantId')]",
    }
    assert "administratorLogin" not in properties
    assert "administratorLoginPassword" not in properties
    assert "identity" not in server
    parameters = compiled_template["parameters"]
    assert parameters["entraAdminPrincipalType"]["allowedValues"] == ["Group", "User"]
    for name in ("entraAdminDisplayName", "entraAdminObjectId", "tenantId"):
        assert "defaultValue" not in parameters[name]
    definition = compiled_template["definitions"]["GovernanceTags"]
    assert compiled_template["languageVersion"] == "2.0"
    assert definition["properties"] == {
        name: {"type": "string"}
        for name in ("CostCenter", "Owner", "Environment", "DataClassification")
    }


def test_compiled_database_is_always_on_and_proof_is_separately_gated(compiled_template: dict) -> None:
    databases = _compiled_resources(compiled_template, "Microsoft.Sql/servers/databases")
    assert len(databases) == 2
    application, = [database for database in databases if "condition" not in database]
    proof, = [database for database in databases if "condition" in database]
    assert application["sku"]["name"] == "[parameters('databaseSkuName')]"
    assert application["sku"]["tier"] == "Standard"
    assert proof["condition"] == "[parameters('enableProofDatabase')]"
    assert proof["sku"] == {"name": "Basic", "tier": "Basic", "capacity": 5}
    assert proof["properties"]["maxSizeBytes"] == 2147483648
    assert compiled_template["parameters"]["enableProofDatabase"]["defaultValue"] is False
    assert compiled_template["parameters"]["databaseSkuName"]["defaultValue"] == "S1"
    for database in databases:
        assert database["properties"]["requestedBackupStorageRedundancy"] == "Local"
        assert database["properties"]["catalogCollation"] == "DATABASE_DEFAULT"
        assert database["tags"] == "[parameters('tags')]"
        assert "elasticPoolId" not in database["properties"]
        assert "compatibilityLevel" not in database["properties"]
    assert compiled_template["outputs"]["requiredCompatibilityLevel"]["value"] == 160
    assert compiled_template["outputs"]["liveAcceptanceRequired"]["value"] is True


def test_compiled_audit_route_and_network_gates(compiled_template: dict) -> None:
    audit, = _compiled_resources(compiled_template, "Microsoft.Sql/servers/auditingSettings")
    assert audit["properties"]["state"] == "Enabled"
    assert audit["properties"]["isAzureMonitorTargetEnabled"] is True
    diagnostics, = _compiled_resources(compiled_template, "Microsoft.Insights/diagnosticSettings")
    assert "master" in diagnostics["scope"]
    assert diagnostics["properties"]["workspaceId"] == "[parameters('logAnalyticsWorkspaceResourceId')]"
    assert diagnostics["properties"]["logs"] == [{"category": "SQLSecurityAuditEvents", "enabled": True}]
    assert "sqlServer" in diagnostics["dependsOn"]
    assert "auditDiagnostics" in audit["dependsOn"]
    for database in _compiled_resources(compiled_template, "Microsoft.Sql/servers/databases"):
        assert "serverAudit" in database["dependsOn"]
    rules = _compiled_resources(compiled_template, "Microsoft.Sql/servers/firewallRules")
    azure, = [rule for rule in rules if "condition" in rule]
    assert azure["condition"] == "[parameters('allowAzureServices')]"
    assert azure["properties"] == {"startIpAddress": "0.0.0.0", "endIpAddress": "0.0.0.0"}
    assert "serverAudit" in azure["dependsOn"]
    assert "connectionPolicy" in azure["dependsOn"]
    assert compiled_template["parameters"]["allowAzureServices"]["defaultValue"] is True
    assert compiled_template["parameters"]["clientFirewallRules"]["defaultValue"] == []
    assert not any(resource["type"].startswith("Microsoft.Network/") for resource in compiled_template["resources"].values())


@pytest.mark.parametrize("proof_enabled", [False, True])
@pytest.mark.parametrize("azure_allowed", [False, True])
def test_compiled_resource_inventory_for_each_opt_in(
    compiled_template: dict, proof_enabled: bool, azure_allowed: bool,
) -> None:
    conditions = {
        "[parameters('enableProofDatabase')]": proof_enabled,
        "[parameters('allowAzureServices')]": azure_allowed,
    }
    resources = [
        resource for resource in compiled_template["resources"].values()
        if not resource.get("existing", False) and "copy" not in resource
    ]
    for resource in resources:
        if "condition" in resource:
            assert resource["condition"] in conditions
    active = [
        resource for resource in resources
        if "condition" not in resource or conditions[resource["condition"]]
    ]
    kinds = [resource["type"] for resource in active]
    assert kinds.count("Microsoft.Sql/servers") == 1
    assert kinds.count("Microsoft.Sql/servers/databases") == 1 + proof_enabled
    assert kinds.count("Microsoft.Sql/servers/databases/backupShortTermRetentionPolicies") == 1 + proof_enabled
    assert kinds.count("Microsoft.Sql/servers/databases/transparentDataEncryption") == 1 + proof_enabled
    assert kinds.count("Microsoft.Sql/servers/firewallRules") == azure_allowed
    assert not any(kind.startswith("Microsoft.Network/") for kind in kinds)
    assert len(active) == 7 + 3 * proof_enabled + azure_allowed
