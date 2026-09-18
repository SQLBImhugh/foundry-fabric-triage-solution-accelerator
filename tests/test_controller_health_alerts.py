from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_controller_alerts_are_scoped_and_do_not_infer_notification_recipients():
    source = (ROOT / "infra" / "controller-health-alerts.bicep").read_text(encoding="utf-8")
    assert "param actionGroupResourceIds string[] = []" in source
    assert "metric: 'RunsSucceeded'" in source and "operator: 'LessThan'" in source
    assert "metric: 'RunsFailed'" in source and "operator: 'GreaterThan'" in source
    assert "window: 'PT15M'" in source and "window: 'PT5M'" in source
    assert "timeAggregation: 'Total'" in source
    assert "targetResourceType: 'Microsoft.Logic/workflows'" in source
    assert "enabled: true" in source
    assert "email" not in source.split("resource alerts", 1)[1]
    assert "webhook" not in source.split("resource alerts", 1)[1]
    assert "Microsoft.Authorization/" not in source


def test_compiled_controller_alerts_use_only_the_selected_workflow():
    folder = os.environ.get("PUBLIC_INFRA_COMPILED_DIRECTORY")
    if not folder:
        pytest.skip("Supply locally compiled templates; tests never invoke Azure.")
    template = json.loads((Path(folder) / "controller-health-alerts.arm.json").read_text(encoding="utf-8-sig"))
    resources = template["resources"]
    resources = list(resources.values()) if isinstance(resources, dict) else resources
    assert len(resources) == 1 and resources[0]["type"] == "Microsoft.Insights/metricAlerts"
    properties = resources[0]["properties"]
    assert properties["scopes"] == ["[parameters('heartbeatWorkflowResourceId')]"]
    assert properties["targetResourceType"] == "Microsoft.Logic/workflows"
    assert properties["evaluationFrequency"] == "PT1M"
    assert properties["enabled"] is True and properties["autoMitigate"] is True
    assert template["parameters"]["actionGroupResourceIds"]["defaultValue"] == []
    checks = template["variables"]["checks"]
    assert {(row["metric"], row["operator"], row["threshold"], row["window"]) for row in checks} == {
        ("RunsSucceeded", "LessThan", 1, "PT15M"), ("RunsFailed", "GreaterThan", 0, "PT5M"),
    }
