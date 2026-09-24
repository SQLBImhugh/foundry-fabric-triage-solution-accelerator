from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def template():
    return json.loads((ROOT / "infra" / "scheduled-sweep.json").read_text("utf-8"))


def test_heartbeat_scheduler_defaults_to_safe_disabled_single_timer():
    value = template()
    parameters = value["parameters"]
    assert parameters["command"]["defaultValue"] == "heartbeat"
    assert "heartbeat" in parameters["command"]["allowedValues"]
    assert parameters["interval"]["defaultValue"] == 1
    assert parameters["interval"]["minValue"] == 1
    assert parameters["enabled"]["defaultValue"] is False
    workflow = value["resources"][0]
    assert workflow["properties"]["state"] == "[if(parameters('enabled'), 'Enabled', 'Disabled')]"
    assert len(workflow["properties"]["definition"]["triggers"]) == 1


def test_heartbeat_authentication_and_ambiguous_write_guards_are_preserved():
    from triage.monitoring.controller import HEARTBEAT_BUDGET_SECONDS

    workflow = template()["resources"][0]
    invoke = workflow["properties"]["definition"]["actions"]["Invoke_the_agent"]
    assert workflow["identity"]["type"] == "SystemAssigned"
    assert invoke["inputs"]["authentication"]["type"] == "ManagedServiceIdentity"
    assert invoke["inputs"]["retryPolicy"]["type"] == "none"
    assert workflow["properties"]["definition"]["actions"]["Wait_for_agent"]["limit"]["timeout"] == "PT15M"
    assert invoke["inputs"]["body"]["background"] is True
    assert invoke["inputs"]["body"]["store"] is True
    assert HEARTBEAT_BUDGET_SECONDS == 840 < 900
    assert workflow["tags"]["DataClassification"] == "[parameters('dataClassification')]"
    assert template()["parameters"]["dataClassification"]["defaultValue"] != "synthetic"
