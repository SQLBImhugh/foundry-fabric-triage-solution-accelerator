from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from scripts.hybrid_platform_probe import WIRE_EVENT_TYPES
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringUnavailable,
)
from triage.monitoring.events import (
    WIRE_TO_SUBSCRIPTION_TYPE,
    ConnectorBinding,
    EventContractError,
    EventProtocolError,
    IdentityBinding,
    PartitionOwnership,
    PinnedAsyncCredential,
    SqlCheckpointStore,
    StreamHistoryGap,
    StreamStart,
    UnidentifiedSignal,
    parse_native_job_event,
    summarize_body,
)
from triage.monitoring.models import (
    BootstrapInspection,
    ConnectorSource,
    CoverageGap,
    CoverageView,
    DeploymentControl,
    EndpointMetadata,
    IntakeReceipt,
    LeaseToken,
    MonitoringContext,
    MonitoringSnapshot,
    OwnedConnectorManifest,
    PartitionIdentity,
    RecordPage,
    RegistryVersion,
    SignalReceipt,
    StreamCheckpoint,
    StreamPosition,
    TargetIdentity,
)
from triage.monitoring.provisioning import SOURCE_EVENTS

TENANT = "11111111-1111-4111-8111-111111111111"
EPOCH = "22222222-2222-4222-8222-222222222222"
CONNECTOR = "33333333-3333-4333-8333-333333333333"
WORKSPACE = "44444444-4444-4444-8444-444444444444"
ITEM = "55555555-5555-4555-8555-555555555555"
JOB = "66666666-6666-4666-8666-666666666666"
DESTINATION_WORKSPACE = "77777777-7777-4777-8777-777777777777"
EVENTSTREAM = "88888888-8888-4888-8888-888888888888"
DESTINATION = "99999999-9999-4999-8999-999999999999"
OWNER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
OTHER_OWNER = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
NOW = datetime(2026, 9, 15, 22, tzinfo=UTC)


def native_event(**changes) -> dict:
    value = {
        "source": TENANT,
        "id": "native-original-id",
        "subject": f"/workspaces/{WORKSPACE}/items/{ITEM}/jobs/instances/{JOB}",
        "type": "Microsoft.Fabric.ItemJobFailed",
        "specversion": "1.0",
        "dataschemaversion": "1.0",
        "time": (NOW - timedelta(seconds=10)).isoformat(),
        "data": {
            "workspaceId": WORKSPACE,
            "itemId": ITEM,
            "itemKind": "Pipeline",
            "itemName": "DO_NOT_PERSIST_THIS_BODY",
            "jobInstanceId": JOB,
            "jobStatus": "Failed",
            "jobType": "Pipeline",
            "jobInovkeType": "Scheduled",
            "jobStartTime": (NOW - timedelta(minutes=2)).isoformat(),
            "jobEndTime": (NOW - timedelta(minutes=1)).isoformat(),
        },
    }
    value.update(changes)
    return value


def body(value: dict):
    return summarize_body(json.dumps(value).encode())


def position(sequence: int = 100) -> StreamPosition:
    return StreamPosition(
        sequence_number=sequence,
        offset=str(sequence * 20),
        enqueued_at=NOW - timedelta(seconds=5),
    )


class MemoryBackend:
    """Only a test double for the required SAME-backend store extension."""

    def __init__(self) -> None:
        self.now = NOW
        self.context = MonitoringContext(tenant_id=TENANT, epoch=EPOCH)
        self.control = DeploymentControl(
            **self.context.model_dump(),
            revision=1,
            maintenance=False,
            activation_cutoff=NOW - timedelta(hours=1),
            updated_at=NOW,
        )
        self.target = TargetIdentity(
            **self.context.model_dump(),
            workload="fabric_pipeline",
            workspace_id=WORKSPACE,
            item_id=ITEM,
        )
        self.manifest = OwnedConnectorManifest(
            **self.context.model_dump(),
            connector_id=CONNECTOR,
            ownership_id=OWNER,
            revision=1,
            policy_revision=1,
            workspace_id=DESTINATION_WORKSPACE,
            eventstream_id=EVENTSTREAM,
            destination_id=DESTINATION,
            name="owned-connector",
            sources=(
                ConnectorSource(
                    source_id="owned-source",
                    target=self.target,
                    event_types=tuple(dict.fromkeys(WIRE_TO_SUBSCRIPTION_TYPE.values())),
                ),
            ),
            desired_definition={"sources": ["owned-source"]},
            observed_definition={"sources": ["owned-source"]},
            endpoint=EndpointMetadata(
                namespace="sample.servicebus.windows.net",
                entity="owned-events",
                consumer_group="$Default",
            ),
            state="degraded",
            updated_at=NOW,
            gaps=(
                CoverageGap(code="transport_unverified", detail="Explicit offline test connector"),
            ),
        )
        self.binding = ConnectorBinding(
            tenant_id=TENANT,
            connector_id=CONNECTOR,
            workspace_id=DESTINATION_WORKSPACE,
            eventstream_id=EVENTSTREAM,
            destination_id=DESTINATION,
            endpoint=self.manifest.endpoint,
        )
        self.owners = {}
        self.starts = {}
        self.positions = {}
        self.intakes = {}
        self.checkpoints = {}
        self.work = set()
        self.action_fences = {"existing-action-fence"}
        self.heartbeats = []
        self.record_calls = 0
        self.fail = None
        self.etag = 0
        self.threads = []
        self.lock = threading.RLock()

    def _called(self) -> None:
        self.threads.append(threading.get_ident())

    def inspect_bootstrap(self, *, expected_tenant_id):
        self._called()
        return BootstrapInspection(
            status="ready",
            expected_tenant_id=expected_tenant_id,
            found_schema_version=1,
            control=self.control,
            detail="Explicit offline test bootstrap",
        )

    def snapshot(self, context):
        self._called()
        if context != self.context:
            raise MonitoringConflict("Changed epoch")
        coverage = CoverageView(
            **self.context.model_dump(),
            revision=self.control.revision,
            as_of=self.now,
            inventory_completeness="complete",
            capability_completeness="complete",
            scope_item_count=1,
            discovered_count=1,
            access_verified_count=1,
            admitted_count=1,
            current_count=1,
            action_enabled_count=0,
            unsupported_count=0,
            backlog_count=len(self.work),
        )
        return MonitoringSnapshot(control=self.control, coverage=coverage)

    def list_connectors(self, query):
        self._called()
        return RecordPage[OwnedConnectorManifest](
            version=RegistryVersion(**self.context.model_dump(), revision=self.control.revision),
            as_of=self.now,
            items=(self.manifest,),
        )

    def _row(self, partition):
        return self.owners.get(partition.key)

    def _put_owner(self, partition, lease):
        self.etag += 1
        row = PartitionOwnership(
            partition=partition,
            lease=lease,
            etag=str(self.etag),
            modified_at=self.now,
        )
        self.owners[partition.key] = row
        return row

    def list_partition_ownership(self, scope):
        self._called()
        if self.fail == "ownership":
            raise MonitoringUnavailable("SQL unavailable")
        return tuple(self.owners.values())

    def change_partition_ownership(self, request):
        self._called()
        with self.lock:
            row = self._row(request.partition)
            if request.expected_etag != (None if row is None else row.etag):
                return None
            if request.release is not None:
                if (
                    row is None
                    or row.lease is None
                    or (
                        row.lease.owner_id != request.release.owner_id
                        or row.lease.fence != request.release.fence
                    )
                ):
                    return None
                return self._put_owner(request.partition, None)
            prior = None if row is None else row.lease
            claim = request.claim
            fence = 1 if prior is None else prior.fence + int(prior.owner_id != claim.owner_id)
            lease = LeaseToken(
                **self.context.model_dump(),
                resource_key=request.partition.key,
                owner_id=claim.owner_id,
                fence=fence,
                acquired_at=self.now,
                expires_at=self.now + timedelta(seconds=claim.lease_seconds),
            )
            return self._put_owner(request.partition, lease)

    def _verify_lease(self, lease):
        row = self.owners.get(lease.resource_key)
        if (
            row is None
            or row.lease is None
            or (
                row.lease.owner_id != lease.owner_id
                or row.lease.fence != lease.fence
                or self.now >= row.lease.expires_at
            )
        ):
            raise MonitoringLeaseLost("Current database-time fence is required")
        return row

    def renew_lease(self, request):
        self._called()
        with self.lock:
            row = self._verify_lease(request.lease)
            lease = row.lease.model_copy(
                update={
                    "expires_at": self.now + timedelta(seconds=request.lease_seconds),
                }
            )
            self._put_owner(row.partition, lease)
            return lease

    def ensure_stream_start(self, request):
        self._called()
        self._verify_lease(request.lease)
        if request.partition.key not in self.starts:
            self.starts[request.partition.key] = StreamStart(
                partition=request.partition,
                first_sequence_number=request.first_available_sequence_number,
                recorded_at=self.now,
                gaps=(
                    CoverageGap(
                        code="unobserved_stream_history",
                        detail=f"History before sequence {request.first_available_sequence_number} was not observed",
                    ),
                ),
            )
        return self.starts[request.partition.key]

    def get_stream_start(self, partition):
        self._called()
        return self.starts.get(partition.key)

    def get_stream_checkpoint(self, partition):
        self._called()
        return self.checkpoints.get(partition.key)

    def get_stream_acceptance(self, context, request_id):
        self._called()
        if self.fail == "acceptance_read":
            raise MonitoringUnavailable("SQL read failed")
        return self.intakes.get(request_id)

    def _record(self, request_id, lease, receipts):
        self._verify_lease(lease)
        if self.fail == "before_record":
            raise MonitoringUnavailable("SQL write failed")
        self.record_calls += 1
        keys = []
        for receipt in receipts:
            identity = (receipt.partition.key, receipt.position.sequence_number)
            self.positions[identity] = receipt
            if isinstance(receipt, SignalReceipt):
                keys.append(receipt.delivery.key)
                if receipt.status == "accepted":
                    self.work.add(receipt.observation.key)
            else:
                keys.append(
                    f"unidentified:{receipt.partition.key}:{receipt.position.sequence_number}"
                )
        intake = IntakeReceipt(
            **self.context.model_dump(),
            request_id=request_id,
            recorded_at=self.now,
            receipt_keys=tuple(keys),
            work_ids=(),
        )
        self.intakes[request_id] = intake
        if self.fail == "after_record":
            raise MonitoringCommitUncertain("stream_intake", request_id)
        return intake

    def record_stream_receipts(self, request):
        self._called()
        with self.lock:
            return self._record(request.request_id, request.lease, request.receipts)

    def record_unidentified_receipts(self, request):
        self._called()
        with self.lock:
            return self._record(request.request_id, request.lease, (request.receipt,))

    def advance_stream_checkpoint(self, request):
        self._called()
        with self.lock:
            self._verify_lease(request.lease)
            current = self.checkpoints.get(request.partition.key)
            if request.expected_revision != (0 if current is None else current.revision):
                raise MonitoringConflict("Checkpoint CAS lost")
            first = (
                self.starts[request.partition.key].first_sequence_number
                if current is None
                else current.position.sequence_number + 1
            )
            if any(
                (request.partition.key, sequence) not in self.positions
                for sequence in range(first, request.through.sequence_number + 1)
            ):
                raise StreamHistoryGap("Uncommitted earlier position")
            checkpoint = StreamCheckpoint(
                partition=request.partition,
                position=request.through,
                revision=request.expected_revision + 1,
                updated_at=self.now,
            )
            self.checkpoints[request.partition.key] = checkpoint
            if self.fail == "after_checkpoint":
                raise MonitoringCommitUncertain("stream_checkpoint", request.request_id)
            return checkpoint

    def record_receiver_heartbeat(self, heartbeat):
        self._called()
        self.heartbeats.append(heartbeat)
        return heartbeat


def adapter(backend: MemoryBackend, **kwargs) -> SqlCheckpointStore:
    result = SqlCheckpointStore(
        backend,
        persistence=backend,
        binding=backend.binding,
        context=backend.context,
        clock=lambda: backend.now,
        **kwargs,
    )
    result.bind_partitions(("0",))
    result.set_partition_properties(
        "0",
        {
            "id": "0",
            "eventhub_name": "owned-events",
            "beginning_sequence_number": 100,
            "last_enqueued_sequence_number": 120,
            "is_empty": False,
        },
    )
    return result


def ownership_request(checkpoints, *, owner=OWNER, etag=None):
    return {
        "fully_qualified_namespace": checkpoints.binding.endpoint.namespace,
        "eventhub_name": checkpoints.binding.endpoint.entity,
        "consumer_group": checkpoints.binding.endpoint.consumer_group,
        "partition_id": "0",
        "owner_id": owner,
        "etag": etag,
    }


async def claim_and_start(checkpoints, *, owner=OWNER, etag=None, first=100):
    checkpoints.set_partition_properties(
        "0",
        {
            "id": "0",
            "eventhub_name": checkpoints.binding.endpoint.entity,
            "beginning_sequence_number": first,
            "last_enqueued_sequence_number": first + 20,
            "is_empty": False,
        },
    )
    result = await checkpoints.claim_ownership(
        [ownership_request(checkpoints, owner=owner, etag=etag)]
    )
    assert len(result) == 1
    await checkpoints.initialize_partition(
        "0",
        {
            "id": "0",
            "eventhub_name": checkpoints.binding.endpoint.entity,
            "beginning_sequence_number": first,
            "last_enqueued_sequence_number": first + 20,
            "is_empty": False,
        },
    )


async def checkpoint(checkpoints, sequence=100):
    value = ownership_request(checkpoints)
    value.pop("owner_id")
    value.pop("etag")
    value.update(sequence_number=sequence, offset=position(sequence).offset)
    await checkpoints.update_checkpoint(value)


def parsed(value=None):
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    return parse_native_job_event(
        body(native_event() if value is None else value),
        partition=checkpoints.partition("0"),
        position=position(),
        received_at=NOW,
        control=backend.control,
        manifest=backend.manifest,
    )


def test_native_failure_is_only_candidate_evidence_and_preserves_identity():
    result = parsed()
    assert isinstance(result, SignalReceipt)
    assert result.status == "accepted"
    assert result.delivery.event_id == "native-original-id"
    assert result.delivery.event_source == TENANT
    assert result.observation.execution.run_id == JOB
    assert result.observation.authority == "transport"
    assert result.observation.failed_scheduled_pipeline is False
    assert result.observation.observed_at == NOW
    assert result.position.enqueued_at != result.observation.observed_at
    assert result.observation.evidence["native_subject"] == native_event()["subject"]
    assert "DO_NOT_PERSIST_THIS_BODY" not in result.model_dump_json()


@pytest.mark.parametrize(
    "kind",
    [
        "Microsoft.Fabric.JobEvents.ItemJobSucceeded.extra",
        "microsoft.fabric.jobevents.itemjobsucceeded",
        "other.Microsoft.Fabric.JobEvents.ItemJobSucceeded",
        "Microsoft.Fabric.JobEvents.ItemJobCancelled",
        "Microsoft.Fabric.JobEvents.ItemJobUnknown",
        "Microsoft.Fabric.JobEvents.ItemJobStatusChanged.extra",
        "microsoft.fabric.jobevents.itemjobstatuschanged",
        "other.Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
        "Microsoft.Fabric.JobEvents.ItemJobFailed.extra",
        "other.ItemJobFailed",
        "Microsoft.Fabric.NewKind.ItemJobFailed",
        "microsoft.fabric.ItemJobFailed",
    ],
)
def test_unknown_wire_types_are_not_suffix_aliases(kind):
    result = parsed(native_event(type=kind))
    assert result.status == "quarantined"
    assert result.quarantine.reason == "unsupported"
    assert result.observation is None
    assert result.quarantine.metadata["native_event_type"] == kind


PROVEN_API_WIRE_TYPES = (
    "Microsoft.Fabric.JobEvents.ItemJobCreated",
    "Microsoft.Fabric.JobEvents.ItemJobFailed",
    "Microsoft.Fabric.JobEvents.ItemJobSucceeded",
    "Microsoft.Fabric.JobEvents.ItemJobStatusChanged",
)


def test_only_the_four_observed_api_namespace_literals_are_registered():
    assert {
        name for name in WIRE_TO_SUBSCRIPTION_TYPE if name.startswith("Microsoft.Fabric.JobEvents.")
    } == set(PROVEN_API_WIRE_TYPES)
    assert set(WIRE_TO_SUBSCRIPTION_TYPE.values()) == set(PROVEN_API_WIRE_TYPES)
    assert set(WIRE_EVENT_TYPES) == set(WIRE_TO_SUBSCRIPTION_TYPE)
    assert len(WIRE_EVENT_TYPES) == 8
    assert set(SOURCE_EVENTS) == set(PROVEN_API_WIRE_TYPES)
    assert len(SOURCE_EVENTS) == len(set(SOURCE_EVENTS)) == 4


@pytest.mark.parametrize("wire_type", PROVEN_API_WIRE_TYPES)
def test_replay_proven_wire_literals_preserve_original_identity_and_candidate_authority(wire_type):
    """Both version attributes are 1.0, matching the reported owned replay."""
    value = native_event(type=wire_type)
    value["data"].pop("jobInovkeType")
    value["data"]["jobInvokeType"] = "Manual"
    result = parsed(value)
    assert WIRE_TO_SUBSCRIPTION_TYPE[wire_type] == wire_type
    assert result.status == "accepted"
    assert result.delivery.event_source == value["source"]
    assert result.delivery.event_id == value["id"]
    assert result.observation.execution.run_id == value["data"]["jobInstanceId"]
    assert result.observation.evidence["native_subject"] == value["subject"]
    assert result.observation.evidence["native_event_type"] == wire_type
    assert result.observation.evidence["subscription_event_type"] == wire_type
    assert result.observation.evidence["observed_job_metadata"]["jobInvokeType"] == "Manual"
    assert result.observation.origin == "event"
    assert result.observation.authority == "transport"
    assert result.observation.invocation != "scheduled"
    assert not result.observation.failed_scheduled_pipeline


def test_failed_before_created_across_partitions_preserves_each_zero_position():
    backend = MemoryBackend()
    receipts = []
    for name, partition_id, status in (("Failed", "1", "Failed"), ("Created", "0", "NotStarted")):
        value = native_event(
            type=f"Microsoft.Fabric.JobEvents.ItemJob{name}",
            id=f"original-{name.lower()}-event",
        )
        value["data"].pop("jobInovkeType")
        value["data"]["jobInvokeType"] = "Manual"
        value["data"]["jobStatus"] = status
        if name == "Created":
            value["time"] = (NOW - timedelta(seconds=20)).isoformat()
            value["data"].pop("jobStartTime")
            value["data"].pop("jobEndTime")
        receipt = parse_native_job_event(
            body(value),
            partition=PartitionIdentity(
                **backend.context.model_dump(),
                connector_id=CONNECTOR,
                consumer_group="$Default",
                partition_id=partition_id,
            ),
            position=StreamPosition(sequence_number=0, offset="0", enqueued_at=NOW),
            received_at=NOW,
            control=backend.control,
            manifest=backend.manifest,
        )
        assert receipt.status == "accepted"
        assert receipt.delivery.event_source == TENANT
        assert receipt.delivery.event_id == value["id"]
        assert receipt.position.sequence_number == 0
        assert receipt.position.offset == "0"
        assert receipt.observation.evidence["native_event_type"] == value["type"]
        assert receipt.observation.evidence["observed_job_metadata"]["jobInvokeType"] == "Manual"
        assert receipt.observation.authority == "transport"
        assert not receipt.observation.failed_scheduled_pipeline
        receipts.append(receipt)
    assert receipts[0].partition.key != receipts[1].partition.key
    assert receipts[0].delivery.key != receipts[1].delivery.key
    assert receipts[0].observation.execution.key == receipts[1].observation.execution.key


@pytest.mark.parametrize("wire_type", PROVEN_API_WIRE_TYPES)
def test_proven_wire_type_does_not_make_a_manual_failure_eligible(wire_type):
    value = native_event(type=wire_type)
    value["data"]["jobInovkeType"] = "Manual"
    value["data"]["jobInvokeType"] = "Manual"
    result = parsed(value)
    assert result.observation.invocation == "manual"
    assert result.observation.status == "failed"
    assert not result.observation.failed_scheduled_pipeline


@pytest.mark.parametrize("invocation", ["Manual", "Scheduled"])
def test_observed_succeeded_type_is_not_a_failure_or_remediation_authority(invocation):
    value = native_event(type="Microsoft.Fabric.JobEvents.ItemJobSucceeded")
    value["data"].update(
        jobStatus="Completed", jobInvokeType=invocation, jobInovkeType=invocation,
    )
    result = parsed(value)
    assert result.status == "accepted"
    assert result.delivery.event_source == value["source"]
    assert result.delivery.event_id == value["id"]
    assert result.observation.execution.run_id == value["data"]["jobInstanceId"]
    assert result.observation.evidence["native_subject"] == value["subject"]
    assert result.observation.evidence["native_event_type"] == value["type"]
    assert result.observation.evidence["subscription_event_type"] == value["type"]
    assert result.observation.evidence["observed_job_metadata"]["jobInvokeType"] == invocation
    assert result.observation.status == "succeeded"
    assert result.observation.invocation == invocation.lower()
    assert result.observation.authority == "transport"
    assert not result.observation.failed_scheduled_pipeline


def test_observed_failed_event_for_cancelled_manual_job_remains_noneligible():
    value = native_event(type="Microsoft.Fabric.JobEvents.ItemJobFailed")
    value["data"].pop("jobInovkeType")
    value["data"].update(jobStatus="Cancelled", jobInvokeType="Manual")
    result = parsed(value)
    assert result.status == "accepted"
    assert result.delivery.event_source == value["source"]
    assert result.delivery.event_id == value["id"]
    assert result.observation.execution.run_id == value["data"]["jobInstanceId"]
    assert result.observation.evidence["native_subject"] == value["subject"]
    assert result.observation.evidence["native_event_type"] == value["type"]
    assert result.observation.evidence["subscription_event_type"] == value["type"]
    assert result.observation.evidence["observed_job_metadata"] == {
        "jobStatus": "Cancelled", "jobType": "Pipeline", "jobInvokeType": "Manual",
    }
    assert result.observation.status == "cancelled"
    assert result.observation.invocation != "scheduled"
    assert result.observation.origin == "event"
    assert result.observation.authority == "transport"
    assert result.observation.evidence["source_rest_verified"] is False
    assert not result.observation.failed_scheduled_pipeline


@pytest.mark.parametrize("wire_type", PROVEN_API_WIRE_TYPES)
@pytest.mark.parametrize("field", ["specversion", "dataschemaversion"])
@pytest.mark.parametrize("version", ["missing", None, "2.0", 1.0])
def test_proven_wire_literals_do_not_relax_either_version_guard(wire_type, field, version):
    value = native_event(type=wire_type)
    if version == "missing":
        value.pop(field)
    else:
        value[field] = version
    result = parsed(value)
    assert result.status == "quarantined"
    assert result.quarantine.reason == "unsupported"
    assert result.observation is None


@pytest.mark.parametrize("wire_type", PROVEN_API_WIRE_TYPES)
@pytest.mark.parametrize("mismatch", ["connector", "target", "event_filter", "missing_source"])
def test_proven_wire_literals_still_require_the_matching_owned_manifest(wire_type, mismatch):
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    manifest = backend.manifest
    if mismatch == "connector":
        manifest = manifest.model_copy(update={"connector_id": OTHER_OWNER})
    elif mismatch == "target":
        source = manifest.sources[0]
        manifest = manifest.model_copy(
            update={
                "sources": (
                    source.model_copy(
                        update={"target": source.target.model_copy(update={"item_id": OTHER_OWNER})}
                    ),
                )
            }
        )
    elif mismatch == "event_filter":
        source = manifest.sources[0]
        manifest = manifest.model_copy(
            update={
                "sources": (
                    source.model_copy(
                        update={
                            "event_types": tuple(
                                event_type
                                for event_type in PROVEN_API_WIRE_TYPES
                                if event_type != wire_type
                            )
                        }
                    ),
                )
            }
        )
    else:
        manifest = manifest.model_copy(update={"sources": ()})
    result = parse_native_job_event(
        body(native_event(type=wire_type)),
        partition=checkpoints.partition("0"),
        position=position(),
        received_at=NOW,
        control=backend.control,
        manifest=manifest,
    )
    assert result.status == "quarantined"
    assert result.quarantine.reason == (
        "unknown_connector" if mismatch == "connector" else "out_of_scope"
    )
    assert result.observation is None


@pytest.mark.parametrize(
    ("status", "normalized"),
    [
        ("Cancelled", "cancelled"),
        ("Stuck", "unknown"),
        ("Failed", "failed"),
    ],
)
def test_item_job_failed_does_not_assert_controller_eligibility(status, normalized):
    value = native_event()
    value["data"]["jobStatus"] = status
    result = parsed(value)
    assert result.observation.status == normalized
    assert not result.observation.failed_scheduled_pipeline


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"source": OTHER_OWNER}, "wrong_tenant"),
        ({"subject": "/untrusted/job"}, "malformed"),
        ({"dataschemaversion": "2.0"}, "unsupported"),
        ({"time": "2026-09-15T18:00:00+00:00"}, "before_cutoff"),
    ],
)
def test_bad_provenance_is_quarantined_without_raw_error_content(change, reason):
    result = parsed(native_event(**change))
    assert result.status == "quarantined"
    assert result.quarantine.reason == reason
    assert "DO_NOT_PERSIST_THIS_BODY" not in result.model_dump_json()


def test_source_start_before_cutoff_is_not_a_new_execution_on_late_delivery():
    value = native_event()
    value["data"]["jobStartTime"] = "2026-09-15T18:00:00+00:00"
    result = parsed(value)
    assert result.quarantine.reason == "before_cutoff"


def test_item_kind_is_metadata_not_an_inferred_wire_enum_or_workload_grant():
    value = native_event()
    value["data"]["itemKind"] = "DataPipeline"
    result = parsed(value)
    assert result.status == "accepted"
    assert result.observation.execution.target.workload == "fabric_pipeline"
    assert result.observation.evidence["native_item_kind"] == "DataPipeline"
    assert result.observation.authority == "transport"
    assert not result.observation.failed_scheduled_pipeline


def test_missing_original_identity_is_explicit_not_a_fabricated_cloud_event():
    value = native_event()
    del value["id"]
    result = parsed(value)
    assert isinstance(result, UnidentifiedSignal)
    assert not hasattr(result, "delivery")
    assert result.quarantine.reason == "malformed"
    assert result.quarantine.metadata["payload_bytes"] > 0


@pytest.mark.parametrize(
    "raw",
    [
        b'{"source":"one","source":"two"}',
        b'{"untrusted":NaN}',
        b'{"x":' + b'{"x":' * 18 + b"1" + b"}" * 19,
        b"not-json-with-private-content",
        b'{"x":"' + b"x" * 65536 + b'"}',
    ],
    ids=["duplicate-keys", "nonfinite", "deep-json", "invalid-json", "oversized"],
)
def test_malformed_and_oversized_bodies_have_only_bounded_dispositions(raw):
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    summary = summarize_body([raw[:10], raw[10:]])
    result = parse_native_job_event(
        summary,
        partition=checkpoints.partition("0"),
        position=position(),
        received_at=NOW,
        control=backend.control,
        manifest=backend.manifest,
    )
    assert isinstance(result, UnidentifiedSignal)
    assert len(result.model_dump_json()) < 2000
    assert "private-content" not in repr(result)
    assert result.quarantine.metadata["payload_bytes"] == len(raw)
    if len(raw) > 65536:
        assert summary.data is None
        assert result.quarantine.reason == "oversized"


async def test_all_store_calls_leave_the_event_loop_and_initial_start_is_not_zero():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints, first=300)
    await checkpoints.accept("0", position(300), body(native_event()))
    await checkpoint(checkpoints, 300)
    start = backend.starts[checkpoints.partition("0").key]
    assert start.first_sequence_number == 300
    assert start.history_before_start == "unobserved"
    assert start.gaps
    assert backend.checkpoints[checkpoints.partition("0").key].position.sequence_number == 300
    assert all(value != threading.get_ident() for value in backend.threads)


async def test_repeat_delivery_ids_at_different_sequences_have_no_checkpoint_hole():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    for sequence in (100, 101):
        await checkpoints.accept("0", position(sequence), body(native_event()))
        await checkpoint(checkpoints, sequence)
    assert len(backend.positions) == 2
    assert len(backend.work) == 1
    assert len({receipt.delivery.key for receipt in backend.positions.values()}) == 1
    assert backend.checkpoints[checkpoints.partition("0").key].position.sequence_number == 101


async def test_unidentified_quarantine_is_durable_before_checkpoint_without_work():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    result = await checkpoints.accept("0", position(), summarize_body(b"malformed"))
    assert isinstance(result, UnidentifiedSignal)
    assert len(backend.intakes) == 1
    assert not backend.checkpoints
    assert not backend.work
    await checkpoint(checkpoints)
    assert backend.checkpoints


async def test_quarantine_acceptance_cannot_claim_queued_controller_work():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    original = backend.record_unidentified_receipts

    def invalid_receipt(request):
        return original(request).model_copy(update={"work_ids": (OTHER_OWNER,)})

    backend.record_unidentified_receipts = invalid_receipt
    with pytest.raises(EventProtocolError, match="must not enqueue controller work"):
        await checkpoints.accept("0", position(), summarize_body(b"malformed"))
    assert not backend.checkpoints


async def test_quarantine_acceptance_allows_pending_validation_work_without_executing_it():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    original = backend.record_unidentified_receipts
    recorded = []

    def pending_receipt(request):
        pending = original(request).model_copy(update={
            "work_ids": (OTHER_OWNER,), "publication_status": "pending_validation",
        })
        backend.intakes[request.request_id] = pending
        recorded.append(request.request_id)
        return pending

    backend.record_unidentified_receipts = pending_receipt
    receipt = await checkpoints.accept("0", position(), summarize_body(b"malformed"))
    assert isinstance(receipt, UnidentifiedSignal)
    assert not backend.work
    await checkpoints.accept("0", position(), summarize_body(b"malformed"))
    assert len(recorded) == 1
    await checkpoint(checkpoints)
    assert backend.checkpoints


async def test_later_durable_receipt_cannot_skip_an_earlier_position():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    await checkpoints.accept("0", position(101), body(native_event()))
    with pytest.raises(StreamHistoryGap):
        await checkpoint(checkpoints, 101)
    assert not backend.checkpoints


@pytest.mark.parametrize("failure", ["before_record", "acceptance_read"])
async def test_sql_failure_never_becomes_an_empty_success_or_checkpoint(failure):
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    backend.fail = failure
    with pytest.raises(MonitoringUnavailable):
        await checkpoints.accept("0", position(), body(native_event()))
    with pytest.raises(EventProtocolError):
        await checkpoint(checkpoints)
    assert not backend.checkpoints
    assert not backend.work


@pytest.mark.parametrize("failure", ["after_record", "after_checkpoint"])
async def test_ambiguous_commit_is_reconciled_from_the_durable_receipt(failure):
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    backend.fail = failure
    await checkpoints.accept("0", position(), body(native_event()))
    await checkpoint(checkpoints)
    assert backend.record_calls == 1
    assert len(backend.work) == 1
    assert backend.checkpoints[checkpoints.partition("0").key].position.sequence_number == 100


async def test_restart_recovers_acceptance_without_rebuilding_idempotent_content():
    backend = MemoryBackend()
    first = adapter(backend)
    await claim_and_start(first)
    await first.accept("0", position(), body(native_event()))
    backend.now += timedelta(seconds=1)
    second = adapter(backend)
    row = backend.owners[first.partition("0").key]
    await claim_and_start(second, owner=OTHER_OWNER, etag=row.etag)
    await second.accept("0", position(), body(native_event()))
    await checkpoint(second)
    assert backend.record_calls == 1
    assert len(backend.work) == 1
    with pytest.raises(MonitoringLeaseLost):
        await checkpoint(first)
    assert backend.action_fences == {"existing-action-fence"}


async def test_cancellation_during_sql_commit_never_advances_a_checkpoint():
    started = threading.Event()
    proceed = threading.Event()
    completed = threading.Event()

    class DelayedBackend(MemoryBackend):
        def record_stream_receipts(self, request):
            started.set()
            if not proceed.wait(3):
                raise MonitoringUnavailable("The offline commit gate was not released")
            try:
                return super().record_stream_receipts(request)
            finally:
                completed.set()

    backend = DelayedBackend()
    first = adapter(backend)
    await claim_and_start(first)
    accepting = asyncio.create_task(first.accept("0", position(), body(native_event())))
    assert await asyncio.to_thread(started.wait, 2)
    accepting.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await accepting
    finally:
        proceed.set()
    assert await asyncio.to_thread(completed.wait, 2)
    assert backend.intakes
    assert not backend.checkpoints
    second = adapter(backend)
    row = backend.owners[first.partition("0").key]
    await claim_and_start(second, owner=OTHER_OWNER, etag=row.etag)
    await second.accept("0", position(), body(native_event()))
    await checkpoint(second)
    assert backend.record_calls == 1
    assert backend.action_fences == {"existing-action-fence"}


async def test_sdk_ownership_is_database_timed_cas_and_release_is_partition_only():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    initial = await checkpoints.claim_ownership([ownership_request(checkpoints)])
    assert isinstance(initial[0]["last_modified_time"], float)
    assert initial[0]["last_modified_time"] == backend.now.timestamp()
    stale = await checkpoints.claim_ownership([ownership_request(checkpoints, owner=OTHER_OWNER)])
    assert stale == []
    other = adapter(backend)
    changed = await other.claim_ownership(
        [
            ownership_request(other, owner=OTHER_OWNER, etag=initial[0]["etag"]),
        ]
    )
    assert changed[0]["owner_id"] == OTHER_OWNER
    released = await other.claim_ownership(
        [
            ownership_request(other, owner="", etag=changed[0]["etag"]),
        ]
    )
    assert released[0]["owner_id"] == ""
    assert backend.action_fences == {"existing-action-fence"}


async def test_checkpoint_methods_accept_real_sdk_shape_and_reject_cross_endpoint():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    await checkpoints.accept("0", position(), body(native_event()))
    await checkpoint(checkpoints)
    records = await checkpoints.list_checkpoints(
        backend.binding.endpoint.namespace,
        backend.binding.endpoint.entity,
        "$Default",
    )
    assert records[0]["sequence_number"] == 100
    assert records[0]["offset"] == position().offset
    with pytest.raises(EventProtocolError):
        await checkpoints.list_ownership("other.servicebus.windows.net", "owned-events", "$Default")


async def test_quiet_empty_partition_uses_actual_next_position():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await checkpoints.claim_ownership([ownership_request(checkpoints)])
    await checkpoints.initialize_partition(
        "0",
        {
            "id": "0",
            "eventhub_name": "owned-events",
            "is_empty": True,
            "beginning_sequence_number": 500,
            "last_enqueued_sequence_number": 499,
        },
    )
    assert backend.starts[checkpoints.partition("0").key].first_sequence_number == 500
    assert checkpoints.last_delivery_at is None


async def test_retention_gap_and_stale_connector_are_not_silent_restarts():
    backend = MemoryBackend()
    checkpoints = adapter(backend)
    await claim_and_start(checkpoints)
    with pytest.raises(StreamHistoryGap):
        await checkpoints.initialize_partition(
            "0",
            {
                "id": "0",
                "eventhub_name": "owned-events",
                "is_empty": False,
                "beginning_sequence_number": 200,
                "last_enqueued_sequence_number": 220,
            },
        )
    backend.manifest = backend.manifest.model_copy(update={"destination_id": OTHER_OWNER})
    with pytest.raises(EventContractError):
        await checkpoints.accept("0", position(), body(native_event()))
    assert not backend.intakes


async def test_ownership_failures_are_reported_to_supervisor_not_hidden_by_sdk():
    backend = MemoryBackend()
    faults = []
    checkpoints = adapter(backend, on_failure=faults.append)
    backend.fail = "ownership"
    with pytest.raises(MonitoringUnavailable):
        await checkpoints.list_ownership(
            backend.binding.endpoint.namespace,
            backend.binding.endpoint.entity,
            "$Default",
        )
    assert len(faults) == 1


def test_missing_shared_store_extensions_fail_explicitly():
    backend = MemoryBackend()
    with pytest.raises(EventContractError, match="EventPersistence is required"):
        SqlCheckpointStore(
            backend,
            persistence=object(),
            binding=backend.binding,
            context=backend.context,
        )


def identity_binding():
    return IdentityBinding(
        tenant_id=TENANT,
        client_id=ITEM,
        object_id=OWNER,
        subscription_id=EPOCH,
        resource_id=f"/subscriptions/{EPOCH}/resourceGroups/sample/providers/Microsoft.ManagedIdentity/userAssignedIdentities/worker",
    )


def identity_token(binding, **changes):
    import base64

    claims = {
        "tid": binding.tenant_id,
        "appid": binding.client_id,
        "oid": binding.object_id,
        "xms_mirid": binding.resource_id,
        **changes,
    }
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"offline.{payload}.not-a-real-token"


@pytest.mark.parametrize("claim", ["tid", "appid", "oid", "xms_mirid"])
async def test_managed_identity_token_binding_is_checked_without_logging_token(claim, caplog):
    binding = identity_binding()
    token = identity_token(binding, **{claim: OTHER_OWNER})

    class Credential:
        async def get_token(self, *scopes, **kwargs):
            return SimpleNamespace(token=token, expires_on=2_000_000_000)

        async def close(self):
            pass

    credential = PinnedAsyncCredential(Credential(), binding)
    with pytest.raises(EventProtocolError) as failure:
        await credential.get_token("https://eventhubs.azure.net/.default")
    assert token not in str(failure.value)
    assert token not in caplog.text
