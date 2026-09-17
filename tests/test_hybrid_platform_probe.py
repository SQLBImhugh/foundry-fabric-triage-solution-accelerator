from __future__ import annotations

import base64
import copy
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from scripts.hybrid_platform_probe import (
    API_EVENT_TYPES,
    MAX_EVENT_BYTES,
    Endpoint,
    ProbeError,
    assert_identity,
    event_receipt,
    eventstream_body,
    main,
    pipeline_body,
    receive,
    semantic_model_body,
)

TENANT = "11111111-1111-4111-8111-111111111111"
WORKSPACE = "22222222-2222-4222-8222-222222222222"
ITEM = "33333333-3333-4333-8333-333333333333"
JOB = "44444444-4444-4444-8444-444444444444"
CLIENT = "55555555-5555-4555-8555-555555555555"
OBJECT = "66666666-6666-4666-8666-666666666666"
SUBSCRIPTION = "77777777-7777-4777-8777-777777777777"
IDENTITY = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/owned-canary"
    "/providers/Microsoft.ManagedIdentity/userAssignedIdentities/owned-canary"
)


def decode_parts(body: dict) -> dict:
    return {
        part["path"]: json.loads(base64.b64decode(part["payload"], validate=True))
        for part in body["definition"]["parts"]
    }


def metadata() -> dict:
    return {
        "type": "CustomEndpoint",
        "fullyQualifiedNamespace": "owned-canary.servicebus.windows.net",
        "eventHubName": "es_owned_canary",
        "consumerGroupName": "$Default",
        "workspaceId": WORKSPACE,
        "eventstreamId": ITEM,
        "destinationId": JOB,
    }


def envelope() -> dict:
    return {
        "specversion": "1.0", "source": TENANT, "id": "original-event-1",
        "type": "Microsoft.Fabric.ItemJobFailed", "time": "2026-09-15T00:00:00Z",
        "subject": f"/workspaces/{WORKSPACE}/items/{ITEM}/jobs/instances/{JOB}",
        "data": {
            "workspaceId": WORKSPACE, "itemId": ITEM, "jobInstanceId": JOB,
            "itemName": "Do not emit names or diagnostic text",
            "jobStatus": "Cancelled",
            "jobInovkeType": "Manual",
        },
    }


def test_eventstream_is_a_complete_owned_per_item_candidate() -> None:
    body = eventstream_body("hybrid-canary-events", WORKSPACE, ITEM)
    parts = decode_parts(body)
    topology = parts["eventstream.json"]
    assert len(API_EVENT_TYPES) == len(set(API_EVENT_TYPES)) == 4
    assert topology["compatibilityLevel"] == "1.1"
    assert topology["sources"][0]["properties"] == {
        "eventScope": "Item", "workspaceId": WORKSPACE, "itemId": ITEM,
        "includedEventTypes": list(API_EVENT_TYPES),
    }
    assert topology["destinations"][0]["type"] == "CustomEndpoint"
    assert topology["destinations"][0]["properties"] == {}
    assert topology["destinations"][0]["inputNodes"] == [{"name": topology["streams"][0]["name"]}]
    assert topology["streams"][0]["inputNodes"] == [{"name": topology["sources"][0]["name"]}]
    assert topology["operators"] == []
    assert parts["eventstreamProperties.json"] == {
        "retentionTimeInDays": 1, "eventThroughputLevel": "Low",
    }
    assert "tenantId" not in topology["sources"][0]["properties"]


@pytest.mark.parametrize("variant,activity_type,seconds", [
    ("failure", "Fail", None), ("success", "Wait", 1), ("cancellation", "Wait", 300),
])
def test_pipeline_has_no_external_side_effects(variant: str, activity_type: str, seconds: int | None) -> None:
    body = pipeline_body("hybrid-canary-pipeline", variant)
    content = decode_parts(body)["pipeline-content.json"]
    assert body["type"] == "DataPipeline"
    assert len(content["properties"]["activities"]) == 1
    activity = content["properties"]["activities"][0]
    assert activity["type"] == activity_type
    if seconds is not None:
        assert activity["typeProperties"]["waitTimeInSeconds"] == seconds
    else:
        assert activity["typeProperties"]["errorCode"] == "HYBRID_CANARY"


def test_semantic_model_is_import_without_external_data_access() -> None:
    body = semantic_model_body("hybrid-canary-refresh")
    parts = decode_parts(body)
    assert body["definition"]["format"] == "TMSL"
    assert set(parts) == {"model.bim", "definition.pbism"}
    model = parts["model.bim"]["model"]
    assert "dataSources" not in model
    partition = model["tables"][0]["partitions"][0]
    assert partition["mode"] == "import"
    assert partition["source"]["expression"] == (
        "Function.InvokeAfter("
        "() => #table(type table [Value = Int64.Type], {{1}}),"
        " #duration(0, 0, 0, 20))"
    )


@pytest.mark.parametrize("name", ["business-pipeline", "hybrid-canary-", "../hybrid-canary-test"])
def test_non_canary_names_are_refused(name: str) -> None:
    with pytest.raises(ProbeError):
        pipeline_body(name, "failure")


@pytest.mark.parametrize("value", ["", "any", "*", None])
def test_missing_or_wildcard_scope_is_refused(value: object) -> None:
    with pytest.raises(ProbeError):
        eventstream_body("hybrid-canary-events", value, ITEM)


def test_metadata_contains_only_nonsecret_owned_endpoint_fields() -> None:
    endpoint = Endpoint.from_document(metadata())
    assert endpoint.namespace == "owned-canary.servicebus.windows.net"
    assert endpoint.workspace_id == WORKSPACE
    assert endpoint.consumer_group == "$Default"


@pytest.mark.parametrize("change", [
    {"accessKeys": {}},
    {"fullyQualifiedNamespace": "https://owned-canary.servicebus.windows.net"},
    {"fullyQualifiedNamespace": "owned-canary.servicebus.windows.net.attacker.test"},
    {"eventHubName": "entity;extra=value"},
    {"consumerGroupName": ""},
    {"workspaceId": "*"},
    {"type": "KafkaEndpoint"},
])
def test_unsafe_or_ambiguous_metadata_is_refused(change: dict) -> None:
    with pytest.raises(ProbeError):
        Endpoint.from_document(metadata() | change)


def test_identity_pin_checks_tenant_and_both_uami_identifiers() -> None:
    expected = {
        "tenant_id": TENANT, "client_id": CLIENT, "object_id": OBJECT,
        "subscription_id": SUBSCRIPTION, "identity_resource_id": IDENTITY,
    }
    claims = {"tid": TENANT, "appid": CLIENT, "oid": OBJECT, "xms_mirid": IDENTITY}
    for key in ("tid", "appid", "oid", None):
        candidate = copy.copy(claims)
        if key:
            candidate[key] = WORKSPACE
        encoded = base64.urlsafe_b64encode(json.dumps(candidate).encode()).decode().rstrip("=")
        synthetic_token = f"fixture.{encoded}.fixture"
        if key:
            with pytest.raises(ProbeError, match="unexpected"):
                assert_identity(synthetic_token, **expected)
        else:
            assert_identity(synthetic_token, **expected)


@pytest.mark.parametrize("token", ["", "malformed", "fixture.e30.fixture", "fixture.W10.fixture"])
def test_malformed_identity_is_refused_without_token_output(token: str) -> None:
    with pytest.raises(ProbeError) as error:
        assert_identity(
            token, tenant_id=TENANT, client_id=CLIENT, object_id=OBJECT,
            subscription_id=SUBSCRIPTION, identity_resource_id=IDENTITY,
        )
    assert token not in str(error.value) or not token


def test_transport_receipt_preserves_identity_without_claiming_durable_or_failed_job_proof() -> None:
    receipt = event_receipt(
        json.dumps(envelope()).encode(), tenant_id=TENANT, workspace_id=WORKSPACE, item_id=ITEM,
    )
    assert receipt["event_id"] == "original-event-1"
    assert receipt["source"] == TENANT
    assert receipt["job_instance_id"] == JOB
    assert receipt["sql_acceptance_proven"] is False
    assert receipt["source_rest_verified"] is False
    assert len(receipt["payload_sha256"]) == 64
    assert receipt["observed_job_metadata"] == {
        "jobStatus": "Cancelled", "jobInovkeType": "Manual",
    }
    assert "Do not emit" not in json.dumps(receipt)


@pytest.mark.parametrize("change", [
    {"source": WORKSPACE},
    {"subject": "https://attacker.test/job"},
    {"type": "Microsoft.Fabric.JobEvents.ItemJobCancelled"},
    {"type": "Microsoft.Fabric.JobEvents.ItemJobUnknown"},
    {"type": "Microsoft.Fabric.JobEvents.ItemJobStatusChanged.extra"},
    {"type": "microsoft.fabric.jobevents.itemjobstatuschanged"},
    {"type": "other.Microsoft.Fabric.JobEvents.ItemJobStatusChanged"},
    {"type": "Microsoft.Fabric.JobEvents.ItemJobSucceeded.extra"},
    {"type": "microsoft.fabric.jobevents.itemjobsucceeded"},
    {"type": "Microsoft.Fabric.Job.Completed"},
    {"specversion": "0.3"},
    {"id": None},
    {"time": "2026-09-15T00:00:00"},
    {"data": {"workspaceId": WORKSPACE, "itemId": JOB, "jobInstanceId": JOB}},
])
def test_unknown_or_mismatched_event_contract_is_refused(change: dict) -> None:
    with pytest.raises(ProbeError):
        event_receipt(
            json.dumps(envelope() | change).encode(),
            tenant_id=TENANT, workspace_id=WORKSPACE, item_id=ITEM,
        )


@pytest.mark.parametrize("event_type,job_status", [
    ("Microsoft.Fabric.JobEvents.ItemJobSucceeded", "Completed"),
    ("Microsoft.Fabric.JobEvents.ItemJobStatusChanged", "InProgress"),
    ("Microsoft.Fabric.JobEvents.ItemJobFailed", "Cancelled"),
])
def test_observed_wire_types_preserve_owned_manual_status_metadata(event_type, job_status) -> None:
    value = envelope()
    value["type"] = event_type
    value["dataschemaversion"] = "1.0"
    value["data"].pop("jobInovkeType")
    value["data"].update(jobStatus=job_status, jobInvokeType="Manual")
    receipt = event_receipt(
        json.dumps(value).encode(), tenant_id=TENANT, workspace_id=WORKSPACE, item_id=ITEM,
    )
    assert receipt["type"] == value["type"]
    assert receipt["source"] == value["source"]
    assert receipt["event_id"] == value["id"]
    assert receipt["job_instance_id"] == value["data"]["jobInstanceId"]
    assert receipt["observed_job_metadata"] == {
        "jobStatus": job_status, "jobInvokeType": "Manual",
    }
    assert receipt["source_rest_verified"] is False
    assert receipt["sql_acceptance_proven"] is False


@pytest.mark.parametrize(
    "raw", [b"not json", b"[]", b"x" * (MAX_EVENT_BYTES + 1)],
    ids=["not-json", "array", "oversized"],
)
def test_unbounded_or_malformed_event_is_refused(raw: bytes) -> None:
    with pytest.raises(ProbeError):
        event_receipt(raw, tenant_id=TENANT, workspace_id=WORKSPACE, item_id=ITEM)


def test_emit_cli_is_offline(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["emit", "pipeline", "--name", "hybrid-canary-test"]) == 0
    body = json.loads(capsys.readouterr().out)
    assert body["displayName"] == "hybrid-canary-test"
    assert decode_parts(body)["pipeline-content.json"]["properties"]["activities"][0]["type"] == "Fail"


def test_emit_rejects_missing_eventstream_scope(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["emit", "eventstream", "--name", "hybrid-canary-test"]) == 2
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("mirid", [None, IDENTITY.replace(SUBSCRIPTION, WORKSPACE)])
def test_uami_resource_pin_refuses_missing_or_wrong_subscription(mirid: str | None) -> None:
    claims = {"tid": TENANT, "appid": CLIENT, "oid": OBJECT, "xms_mirid": mirid}
    encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    with pytest.raises(ProbeError, match="UAMI resource"):
        assert_identity(
            f"fixture.{encoded}.fixture", tenant_id=TENANT, client_id=CLIENT, object_id=OBJECT,
            subscription_id=SUBSCRIPTION, identity_resource_id=IDENTITY,
        )


@pytest.mark.parametrize("event_count", [0, 1])
async def test_receiver_uses_explicit_uami_tls_and_never_checkpoints(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], event_count: int,
) -> None:
    calls: dict = {}
    claims = {"tid": TENANT, "appid": CLIENT, "oid": OBJECT, "xms_mirid": IDENTITY}
    encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")

    class FakeCredential:
        def __init__(self, *, client_id: str, retry_total: int) -> None:
            calls["client_id"] = client_id
            calls["identity_retry_total"] = retry_total

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            pass

        async def get_token(self, *scopes, **kwargs):
            calls["scopes"] = scopes
            return SimpleNamespace(token=f"fixture.{encoded}.fixture", expires_on=1)

    class FakeConsumer:
        def __init__(self, **kwargs) -> None:
            calls["consumer"] = kwargs
            self.credential = kwargs["credential"]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            pass

        async def get_partition_ids(self):
            await self.credential.get_token("https://eventhubs.azure.net/.default")
            return ["0"]

        async def receive(self, *, on_event, starting_position, **kwargs):
            calls["starting_position"] = starting_position
            calls["receive_options"] = kwargs
            await on_event(SimpleNamespace(partition_id="0"), None)
            for _ in range(event_count):
                await on_event(
                    SimpleNamespace(partition_id="0"),
                    SimpleNamespace(
                        body=[json.dumps(envelope()).encode()], sequence_number=1, offset="10",
                    ),
                )

    modules = {}
    for name in (
        "azure", "azure.core", "azure.core.exceptions", "azure.eventhub",
        "azure.eventhub.aio", "azure.identity", "azure.identity.aio",
    ):
        modules[name] = ModuleType(name)
        monkeypatch.setitem(sys.modules, name, modules[name])
    modules["azure.core.exceptions"].AzureError = type("FakeAzureError", (Exception,), {})
    modules["azure.eventhub"].TransportType = SimpleNamespace(AmqpOverWebsocket="tls-websocket")
    modules["azure.eventhub.aio"].EventHubConsumerClient = FakeConsumer
    modules["azure.identity.aio"].ManagedIdentityCredential = FakeCredential
    kwargs = {
        "tenant_id": TENANT, "client_id": CLIENT, "object_id": OBJECT,
        "subscription_id": SUBSCRIPTION, "identity_resource_id": IDENTITY,
        "item_id": ITEM, "seconds": 1,
    }
    if event_count:
        assert await receive(Endpoint.from_document(metadata()), **kwargs) == 1
    else:
        with pytest.raises(ProbeError, match="No owned job envelope"):
            await receive(Endpoint.from_document(metadata()), **kwargs)
    assert calls["client_id"] == CLIENT
    assert calls["scopes"] == ("https://eventhubs.azure.net/.default",)
    assert calls["consumer"]["transport_type"] == "tls-websocket"
    assert calls["consumer"]["logging_enable"] is False
    assert calls["consumer"]["retry_total"] == 0
    assert calls["identity_retry_total"] == 0
    assert calls["receive_options"]["max_wait_time"] == 5
    assert "checkpoint_store" not in calls["consumer"]
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[-1]["durable_checkpoint_written"] is False
    assert lines[-1]["sql_acceptance_proven"] is False
    assert lines[-1]["owned_envelopes_received"] == event_count
    assert any(line["stage"] == "receiver_ready" and line["event_delivery_verified"] is False for line in lines)
