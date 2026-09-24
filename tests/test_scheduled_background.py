from __future__ import annotations

import json
from pathlib import Path


def definition():
    template = json.loads((Path(__file__).resolve().parents[1] / "infra" / "scheduled-sweep.json").read_text())
    return template["resources"][0]["properties"]["definition"]


def test_scheduler_submits_one_stored_background_response():
    invoke = definition()["actions"]["Invoke_the_agent"]["inputs"]
    assert invoke["body"] == {
        "input": "@{parameters('command')}",
        "background": True, "store": True, "stream": False,
    }
    assert invoke["retryPolicy"] == {"type": "none"}
    assert invoke["authentication"]["type"] == "ManagedServiceIdentity"


def test_scheduler_polls_only_the_original_response_before_reporting_completion():
    actions = definition()["actions"]
    accepted = actions["Validate_accepted_response"]["inputs"]["schema"]
    assert set(accepted["required"]) >= {"id", "status"}
    assert {"queued", "in_progress", "completed"} <= set(accepted["properties"]["status"]["enum"])
    waiting = actions["Wait_for_agent"]
    assert waiting["type"] == "Until"
    assert waiting["operationOptions"] == "FailWhenLimitsReached"
    assert waiting["limit"] == {"count": 90, "timeout": "PT15M"}
    polling = waiting["actions"]["If_still_running"]["actions"]
    read = polling["Read_agent_response"]["inputs"]
    assert read["method"] == "GET"
    assert "encodeUriComponent(variables('Agent_response_id'))" in read["uri"]
    assert read["authentication"]["type"] == "ManagedServiceIdentity"
    assert polling["Validate_polled_response"]["inputs"]["schema"]["properties"]["id"]["enum"] == [
        "@variables('Agent_response_id')"
    ]
    final = actions["Validate_agent_response"]
    assert final["runAfter"] == {"Wait_for_agent": ["Succeeded"]}
    assert final["inputs"]["content"] == "@variables('Agent_response')"
    assert final["inputs"]["schema"]["properties"]["status"]["enum"] == ["completed"]


def test_polling_cannot_submit_another_response_or_replace_its_identity():
    actions = definition()["actions"]

    def nested(values):
        for name, action in values.items():
            yield name, action
            yield from nested(action.get("actions", {}))
            yield from nested(action.get("else", {}).get("actions", {}))

    polls = list(nested(actions["Wait_for_agent"]["actions"]))
    assert all(action.get("inputs", {}).get("method") != "POST" for _, action in polls)
    writes = [action["inputs"]["name"] for _, action in polls if action["type"] == "SetVariable"]
    assert writes == ["Agent_response"]
    assert actions["Invoke_the_agent"]["inputs"]["retryPolicy"] == {"type": "none"}
    assert definition()["triggers"]["Every_interval"]["runtimeConfiguration"]["concurrency"]["runs"] == 1


def test_pending_failed_and_malformed_responses_cannot_pass_the_final_gate():
    actions = definition()["actions"]
    final = actions["Validate_agent_response"]["inputs"]["schema"]
    assert final["required"] == ["status"]
    assert final["properties"]["status"]["type"] == "string"
    assert final["properties"]["status"]["enum"] == ["completed"]
    assert final["properties"]["error"] == {"type": "null"}
    assert actions["Did_it_fail"]["runAfter"]["Validate_agent_response"] == ["Failed", "TimedOut", "Skipped"]
    assert actions["Fail_the_run"]["inputs"]["runStatus"] == "Failed"


def test_parse_json_uses_only_supported_identifier_checks():
    def check(value):
        if isinstance(value, dict):
            assert not {"pattern", "patternProperties"} & value.keys(), (
                "Logic Apps rejects regex schema keywords at runtime even after ARM validation"
            )
            for child in value.values():
                check(child)
        elif isinstance(value, list):
            for child in value:
                check(child)

    check(definition()["actions"])
    identifier = definition()["actions"]["Validate_accepted_response"]["inputs"]["schema"]["properties"]["id"]
    assert identifier == {"type": "string", "minLength": 1, "maxLength": 256}
