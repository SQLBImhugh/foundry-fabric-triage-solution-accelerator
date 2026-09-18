from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_hosted_metadata_uses_an_application_owned_setting_not_platform_tracing():
    deployment = yaml.safe_load((ROOT / "azure.yaml").read_text(encoding="utf-8"))
    settings = {
        item["name"]: item["value"]
        for item in deployment["services"]["bi-triage-controller"]["environmentVariables"]
    }
    assert "APPLICATIONINSIGHTS_CONNECTION_STRING" not in settings
    assert settings["TRIAGE_TELEMETRY_CONNECTION_STRING"] == "${APPLICATIONINSIGHTS_CONNECTION_STRING}"
    assert settings["OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"] == "false"
    source = (ROOT / "infra" / "application-telemetry.bicep").read_text(encoding="utf-8")
    assert "DisableLocalAuth: true" in source
    assert "publicNetworkAccessForIngestion: 'Enabled'" in source
    assert "publicNetworkAccessForQuery: 'Enabled'" in source
    assert "scope: insights" in source
    assert "3913510d-42f4-4e42-8a64-420c390055eb" in source
    assert "/connections@" not in source
    assert "output applicationInsightsResourceId string = insights.id" in source
    assert "output connectionString" not in source


def test_compiled_telemetry_does_not_enable_project_content_tracing():
    folder = os.environ.get("PUBLIC_INFRA_COMPILED_DIRECTORY")
    if not folder:
        pytest.skip("Supply locally compiled templates; tests never invoke Azure.")
    template = json.loads((Path(folder) / "application-telemetry.arm.json").read_text(encoding="utf-8-sig"))
    resources = template["resources"]
    resources = list(resources.values()) if isinstance(resources, dict) else resources
    assert {resource["type"] for resource in resources} == {
        "Microsoft.Insights/components", "Microsoft.Authorization/roleAssignments",
    }
    insights = next(resource for resource in resources if resource["type"] == "Microsoft.Insights/components")
    assert insights["properties"]["DisableLocalAuth"] is True
    assert insights["properties"]["publicNetworkAccessForIngestion"] == "Enabled"
    assert template["parameters"]["publisherPrincipalIds"]["defaultValue"] == []
