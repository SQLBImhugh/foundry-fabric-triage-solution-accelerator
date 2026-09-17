from __future__ import annotations

import json

import pytest
from test_hybrid_platform_probe import ITEM, JOB, TENANT, WORKSPACE, envelope

from scripts.hybrid_platform_probe import (
    API_EVENT_TYPES,
    ProbeError,
    envelope_shape,
    event_receipt,
)


@pytest.mark.parametrize("event_type", [API_EVENT_TYPES[-1], "Microsoft.Fabric.ItemJobFailed"])
def test_wire_diagnostics_identify_only_documented_type_constants(event_type):
    result = envelope_shape(json.dumps({
        "specversion": "1.0", "type": event_type, "id": "DO_NOT_LOG_THIS_VALUE",
        "source": "DO_NOT_LOG_THIS_VALUE", "subject": "DO_NOT_LOG_THIS_VALUE",
        "DO_NOT_LOG_THIS_KEY": "DO_NOT_LOG_THIS_VALUE",
        "data": {"jobStatus": "DO_NOT_LOG_THIS_VALUE", "itemId": "DO_NOT_LOG_THIS_VALUE"},
    }).encode())
    assert result["specversion_1_0"] is True
    assert result["recognized_type"] == event_type
    assert result["known_data_fields"] == ["itemId", "jobStatus"]
    assert "DO_NOT_LOG" not in json.dumps(result)


def test_unknown_type_and_schema_values_are_not_copied_to_diagnostics():
    result = envelope_shape(json.dumps({
        "specversion": "DO_NOT_LOG_THIS_VALUE", "type": "DO_NOT_LOG_THIS_VALUE",
        "data": "DO_NOT_LOG_THIS_VALUE",
    }).encode())
    assert result["recognized_type"] is None
    assert result["specversion_1_0"] is False and result["data_type"] == "str"
    assert "DO_NOT_LOG" not in json.dumps(result)


@pytest.mark.parametrize("raw", [b"not JSON", b"\xff", b"[" * 1500])
def test_malformed_wire_diagnostics_remain_bounded(raw):
    assert envelope_shape(raw) == {"json_valid": False}


def test_array_wire_diagnostics_do_not_copy_contents():
    assert envelope_shape(b'["DO_NOT_LOG_THIS_VALUE"]') == {
        "json_valid": True, "root_type": "list",
    }


@pytest.mark.parametrize("event_type", [
    "Microsoft.Fabric.JobEvents.ItemJobCreated",
    "Microsoft.Fabric.JobEvents.ItemJobFailed",
    "Microsoft.Fabric.JobEvents.ItemJobSucceeded",
    "Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
])
def test_observed_jobevents_names_preserve_original_transport_identity(event_type):
    raw = json.dumps(envelope() | {"type": event_type}).encode()
    receipt = event_receipt(raw, tenant_id=TENANT, workspace_id=WORKSPACE, item_id=ITEM)
    assert receipt["type"] == event_type
    assert receipt["event_id"] == "original-event-1" and receipt["source"] == TENANT
    assert receipt["job_instance_id"] == JOB
    assert receipt["source_rest_verified"] is False and receipt["sql_acceptance_proven"] is False
    assert receipt["observed_job_metadata"]["jobInovkeType"] == "Manual"


@pytest.mark.parametrize("change", [
    {"type": "Microsoft.Fabric.JobEvents.ItemJobCancelled"},
    {"type": "Microsoft.Fabric.JobEvents.ItemJobUnknown"},
    {"type": "Microsoft.Fabric.JobEvents.ItemJobStatusChanged.extra"},
    {"type": "microsoft.fabric.jobevents.itemjobstatuschanged"},
    {"type": "other.Microsoft.Fabric.JobEvents.ItemJobStatusChanged"},
    {"type": "Microsoft.Fabric.JobEvents.ItemJobSucceeded.extra"},
    {"type": "microsoft.fabric.jobevents.itemjobsucceeded"},
    {"type": "other.Microsoft.Fabric.JobEvents.ItemJobFailed"},
    {"type": "microsoft.fabric.jobevents.itemjobfailed"},
    {"specversion": "0.3"},
    {"source": WORKSPACE},
    {"subject": f"/workspaces/{WORKSPACE}/items/{ITEM}/jobs/instances/{ITEM}"},
])
def test_observed_types_do_not_relax_version_scope_or_namespace_matching(change):
    event = envelope() | {"type": "Microsoft.Fabric.JobEvents.ItemJobFailed"} | change
    with pytest.raises(ProbeError):
        event_receipt(
            json.dumps(event).encode(), tenant_id=TENANT, workspace_id=WORKSPACE, item_id=ITEM,
        )


@pytest.mark.parametrize("event_type,job_status", [
    ("Microsoft.Fabric.JobEvents.ItemJobSucceeded", "Completed"),
    ("Microsoft.Fabric.JobEvents.ItemJobStatusChanged", "InProgress"),
    ("Microsoft.Fabric.JobEvents.ItemJobFailed", "Cancelled"),
])
def test_diagnostic_recognition_is_not_source_execution_proof(event_type, job_status):
    value = envelope() | {
        "type": event_type,
        "dataschemaversion": "1.0",
    }
    value["data"]["jobStatus"] = job_status
    value["data"]["jobInvokeType"] = "Manual"
    shape = envelope_shape(json.dumps(value).encode())
    assert shape["recognized_type"] == value["type"]
    assert shape["specversion_1_0"] is True
    assert shape["dataschemaversion_1_0"] is True
    assert "jobInvokeType" in shape["known_data_fields"]
    assert "job_instance_id" not in shape
    assert "source_rest_verified" not in shape
