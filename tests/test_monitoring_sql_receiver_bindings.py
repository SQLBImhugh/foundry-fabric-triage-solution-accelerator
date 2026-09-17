from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, timedelta

import pytest
from pydantic import ValidationError
from test_monitoring_sql_review9_bindings import Review9Database
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringUnavailable,
)
from triage.monitoring.events import (
    ConnectorBinding,
    ConnectorScope,
    OwnershipChange,
    ReceiverHeartbeat,
    SqlCheckpointStore,
    StreamStartRequest,
    UnidentifiedReceiptBatch,
    UnidentifiedSignal,
    summarize_body,
)
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.sql_kernel_common import receipt_content_expression
from triage.monitoring.sql_kernel_contracts import KERNEL_VERSION
from triage.monitoring.sql_kernel_removals import source_is_pending_removal_sql
from triage.monitoring.sql_kernel_retention import retention_classification_sql
from triage.monitoring.sql_store import AzureSqlMonitoringStore
from triage.store.azure_sql import SqlCommitUncertain


class ReceiverAbiDatabase(Review9Database):
    """Named RPC/role/transaction harness, not native full-procedure acceptance."""

    def __init__(self, h):
        super().__init__(h)
        self.principal = "worker"
        self.partition_leases = {}
        self.fail_receiver_receipt = False
        self.guards.create_function(
            "DATALENGTH", 1, lambda value: None if value is None else len(value.encode("utf-16-le")),
        )

    @contextmanager
    def transaction(self):
        leases, failure = deepcopy(self.partition_leases), self.fail_commit
        try:
            with super().transaction():
                yield self
        except BaseException as exc:
            if not (isinstance(exc, SqlCommitUncertain) and failure == "after"):
                self.partition_leases = leases
            raise

    def apply_rpc(self, operation, args):
        if operation not in {"worker.partition", "worker.commit_positions", "worker.advance_checkpoint", "worker.observe_retention"}:
            return super().apply_rpc(operation, args)
        prior = self.receipts.get((operation, args["request_id"]))
        if prior is not None:
            if prior["fingerprint"] != args["fingerprint"]:
                raise RuntimeError("Original receiver fingerprint changed (51072)")
            return {"kernel_version": KERNEL_VERSION, "operation": operation, "status": "replayed",
                    "affected_rows": 0, "result": prior["payload"]["result"]}
        partition = m.PartitionIdentity.model_validate({
            key: args[key] for key in ("tenant_id", "epoch", "connector_id", "consumer_group", "partition_id")
        })
        connector = self.model("connector", partition.connector_id, m.OwnedConnectorManifest)
        if connector is None or connector.endpoint.consumer_group != partition.consumer_group:
            raise RuntimeError("Partition belongs to another owned endpoint (51072)")
        if operation != "worker.partition" or args["transition"] != "release":
            if self.control.maintenance or connector.state not in {"ready", "degraded"}:
                raise RuntimeError("Partition intake is blocked (51071)")
        if operation != "worker.partition" and args["expected_revision"] != self.control.revision:
            raise RuntimeError("Current receiver policy changed (51072)")
        if operation == "worker.partition":
            result = self.partition(args, partition)
        elif operation == "worker.commit_positions":
            result = self.positions(args, partition)
        elif operation == "worker.observe_retention":
            result = self.retention(args, partition)
        else:
            result = self.checkpoint(args, partition)
        reply = self.reply(operation, args, result)
        if self.fail_receiver_receipt:
            self.fail_receiver_receipt = False
            raise RuntimeError("Injected receiver receipt failure (51072)")
        return reply

    def partition(self, args, partition):
        prior = self.records.get(("partition_ownership", partition.key))
        lease = self.partition_leases.get(partition.key)
        if (prior.version if prior else 0) != args["expected_ownership_revision"] or (
            (lease.owner_id if lease else None) != args["expected_owner_id"]
            or (lease.fence if lease else None) != args["expected_fence"]
        ):
            raise RuntimeError("Partition CAS failed (51074)")
        transition = args["transition"]
        if transition == "pin_start":
            if lease is None or lease.expires_at <= self.clock():
                raise RuntimeError("Pinned start lost ownership (51074)")
            first = args["first_sequence_number"]
            start = self.records.get(("stream_start", partition.key))
            if start is not None and start.sequence_number != first:
                raise RuntimeError("Pinned start is immutable (51072)")
            if start is None:
                self.native_put("stream_start", partition.key, {
                    "partition_key": partition.key, "partition": partition.model_dump(mode="json"),
                    "first_sequence_number": first,
                    "broker_observed_at": args["broker_observed_at"].replace(tzinfo=UTC).isoformat(),
                    "recorded_at": self.clock().isoformat(), "history_before_start": "unobserved",
                    "gaps": [{"code": "unobserved_stream_history", "detail": "History before the actual pinned broker boundary was not observed."}],
                }, sequence_number=first, parent_key=partition.connector_id)
            return {"partition_key": partition.key, "first_sequence_number": first,
                    "partition": partition.model_dump(mode="json"),
                    "ownership_revision": prior.version, "last_owner_id": lease.owner_id, "last_fence": lease.fence,
                    "start": json.loads(self.records[("stream_start", partition.key)].payload)}
        if transition == "renew" and (
            lease is None or lease.owner_id != args["new_owner_id"] or lease.expires_at <= self.clock()
        ):
            raise RuntimeError("Partition renewal lost ownership (51074)")
        if transition == "claim" and lease and lease.expires_at > self.clock() and lease.owner_id != args["new_owner_id"]:
            raise RuntimeError("Partition has an unexpired other owner (51074)")
        if transition == "release":
            updated = lease.model_copy(update={
                "fence": lease.fence + 1,
                "expires_at": max(self.clock(), lease.acquired_at + timedelta(microseconds=1)),
            })
            exposed = None
        else:
            updated = m.LeaseToken(
                **self.context.model_dump(), resource_key=partition.key, owner_id=args["new_owner_id"],
                fence=lease.fence if transition == "renew" else (lease.fence + 1 if lease else 1),
                acquired_at=self.clock(), expires_at=self.clock() + timedelta(seconds=args["lease_seconds"]),
            )
            exposed = updated.model_dump(mode="json")
        self.partition_leases[partition.key] = updated
        result = {
            "partition_key": partition.key, "lease": exposed, "ownership_revision": (prior.version if prior else 0) + 1,
            "partition": partition.model_dump(mode="json"), "last_owner_id": updated.owner_id,
            "last_fence": updated.fence,
            "etag": f"ownership:{(prior.version if prior else 0) + 1}:{updated.fence}",
            "modified_at": self.clock().isoformat(),
        }
        self.native_put("partition_ownership", partition.key, result, status="owned" if exposed else "released",
                        parent_key=partition.connector_id)
        return result

    def retention(self, args, partition):
        self.owner(args, partition)
        start = self.records[("stream_start", partition.key)]
        checkpoint = self.records.get(("stream_checkpoint", partition.key))
        if (checkpoint.version if checkpoint else 0) != args["expected_checkpoint_revision"]:
            raise RuntimeError("Retention checkpoint revision differs (51072)")
        if args["observed_at"].replace(tzinfo=UTC) > self.clock():
            raise RuntimeError("Broker observation is in the future (51073)")
        expected = checkpoint.sequence_number + 1 if checkpoint else start.sequence_number
        code = self.guards.execute("SELECT " + retention_classification_sql(), {
            "first_available_sequence_number": args["first_available_sequence_number"],
            "expected_sequence": expected, "pinned": start.sequence_number,
        }).fetchone()[0]
        observation = None
        if code:
            key = f"{partition.key}:gap:{code}:{expected}:{args['first_available_sequence_number']}"
            prior = self.records.get(("stream_gap", key))
            if prior:
                observation = json.loads(prior.payload)
            else:
                gap = {"code": code, "detail": "The broker history changed; no checkpoint was advanced."}
                observation = {
                    "partition": partition.model_dump(mode="json"), "pinned_start": start.sequence_number,
                    "expected_sequence": expected, "first_available_sequence_number": args["first_available_sequence_number"],
                    "missing_from": expected if code == "stream_retention_gap" else None,
                    "missing_through": args["first_available_sequence_number"] - 1 if code == "stream_retention_gap" else None,
                    "checkpoint_revision": args["expected_checkpoint_revision"], "owner_id": args["owner_id"],
                    "fence": args["fence"], "observed_at": args["observed_at"].replace(tzinfo=UTC).isoformat(),
                    "recorded_at": self.clock().isoformat(), "gap": gap,
                }
                self.native_put("stream_gap", key, observation, parent_key=partition.key, status=code)
                payload = json.loads(start.payload)
                payload["gaps"].append(gap)
                self.native_put("stream_start", partition.key, payload, sequence_number=start.sequence_number,
                                parent_key=partition.connector_id)
        return {
            "partition": partition.model_dump(mode="json"),
            "start": json.loads(self.records[("stream_start", partition.key)].payload),
            "checkpoint": json.loads(checkpoint.payload) if checkpoint else None,
            "observation": observation, "state": "gap_recorded" if code else "no_new_gap",
        }

    def owner(self, args, partition):
        lease = self.partition_leases.get(partition.key)
        if lease is None or lease.expires_at <= self.clock() or (
            lease.owner_id, lease.fence,
        ) != (args["owner_id"], args["fence"]):
            raise RuntimeError("Native partition lease lost (51074)")

    def content(self, document):
        return self.guards.execute(
            "SELECT " + receipt_content_expression("@document"), {"document": json.dumps(document)},
        ).fetchone()[0]

    def positions(self, args, partition):
        self.owner(args, partition)
        start = self.records.get(("stream_start", partition.key))
        if start is None:
            raise RuntimeError("Actual broker start is missing (51072)")
        positions = json.loads(args["positions_json"])
        assert 1 <= len(positions) <= 200
        descriptors = []
        targets = []
        for entry in positions:
            receipt = entry["receipt"]
            sequence = receipt["position"]["sequence_number"]
            if sequence < start.sequence_number or receipt["partition"] != partition.model_dump(mode="json"):
                raise RuntimeError("Position identity/start mismatch (51072)")
            kind = "signal" if entry["receipt_kind"] == "identified" else "unidentified_signal"
            if kind == "signal" and receipt["status"] == "accepted":
                signal = m.SignalReceipt.model_validate(receipt)
                manifest = self.model("connector", partition.connector_id, m.OwnedConnectorManifest)
                if signal.observation.authority != "transport" or not any(
                    source.target == signal.observation.execution.target
                    and source.event_source == signal.delivery.event_source
                    and signal.event_type in source.event_types
                    and not self.guards.execute(
                        "SELECT " + source_is_pending_removal_sql("@source", "@removals").replace("OPENJSON(", "json_each("),
                        {"source": source.model_dump_json(),
                         "removals": json.dumps([removal.model_dump(mode="json") for removal in manifest.source_removals])},
                    ).fetchone()[0]
                    for source in manifest.sources
                ):
                    raise RuntimeError("Receipt is outside the owned source (51072)")
                targets.append(signal.observation.execution.target)
            prior = self.records.get((kind, entry["receipt_key"]))
            if prior is None:
                prior = self.native_put(kind, entry["receipt_key"], receipt, parent_key=partition.connector_id,
                                        status=receipt["status"])
            elif self.content(json.loads(prior.payload)) != self.content(receipt):
                raise RuntimeError("Original event identity was rebound (51072)")
            payload_hash = hashlib.sha256(prior.payload.encode("utf-16-le")).hexdigest().upper()
            position_key = f"{partition.key}:position:{sequence}"
            journal = self.records.get(("stream_position", position_key))
            value = {
                "offset": receipt["position"]["offset"], "enqueued_at": receipt["position"]["enqueued_at"],
                "receipt_key": entry["receipt_key"], "receipt_kind": entry["receipt_kind"],
                "payload_hash": payload_hash, "batch_id": args["request_id"],
            }
            if journal is None:
                self.native_put("stream_position", position_key, value, status=receipt["status"],
                                parent_key=partition.key, sequence_number=sequence)
            elif any(json.loads(journal.payload)[key] != value[key] for key in value if key != "batch_id"):
                raise RuntimeError("Original position was rebound (51072)")
            self.native_put("accepted_fact", f"accepted:{args['request_id']}:{kind}:{sequence}", {
                "batch_id": args["request_id"], "batch_fingerprint": args["fingerprint"],
                "fact_kind": kind, "fact_key": prior.key, "fact_revision": prior.version,
                "payload_hash": payload_hash, "row_hash": self.row_hash(prior),
            }, parent_key=partition.key)
            descriptors.append({"kind": kind, "key": prior.key, "revision": prior.version, "payload_hash": payload_hash.lower()})
        handoff = self.handoff(
            "worker.commit_positions", args, topic="stream_intake", reference=partition.connector_id,
            target=targets[0] if targets and len(targets) == len(positions) and len(set(targets)) == 1 else None,
        )
        key = ("worker_reconcile_request", args["request_id"])
        row = self.records[key]
        value = json.loads(row.payload)
        value["evidence"] = descriptors
        self.records[key] = replace(row, payload=json.dumps(value))
        mappings = []
        for entry in positions:
            position = entry["receipt"]["position"]
            journal = json.loads(self.records[("stream_position", f"{partition.key}:position:{position['sequence_number']}")].payload)
            mappings.append({
                **position, "receipt_kind": entry["receipt_kind"], "receipt_key": entry["receipt_key"],
                "first_committed_batch_id": journal["batch_id"], "original_payload_hash": journal["payload_hash"],
            })
        return {
            "batch_id": args["request_id"], "partition_key": partition.key, "position_count": len(positions),
            "partition": partition.model_dump(mode="json"), "positions": mappings,
            "receipt_keys": [item["receipt_key"] for item in mappings], "state": "accepted_for_reconciliation", **handoff,
        }

    def checkpoint(self, args, partition):
        self.owner(args, partition)
        start = self.records.get(("stream_start", partition.key))
        prior = self.records.get(("stream_checkpoint", partition.key))
        if start is None or (prior.version if prior else 0) != args["expected_checkpoint_revision"]:
            raise RuntimeError("Checkpoint start/revision differs (51072)")
        first = prior.sequence_number + 1 if prior else start.sequence_number
        last = args["through_sequence_number"]
        if last < first:
            raise RuntimeError("Checkpoint cannot regress (51073)")
        for sequence in range(first, last + 1):
            position = self.records.get(("stream_position", f"{partition.key}:position:{sequence}"))
            if position is None:
                raise RuntimeError("Checkpoint would skip a position (51072)")
            value = json.loads(position.payload)
            if ("worker.commit_positions", value["batch_id"]) not in self.receipts:
                raise RuntimeError("Original position receipt is absent (51072)")
        if value["offset"] != args["through_offset"]:
            raise RuntimeError("Checkpoint offset differs (51072)")
        result = {
            "partition_key": partition.key, "revision": args["expected_checkpoint_revision"] + 1,
            "partition": partition.model_dump(mode="json"),
            "position": {"sequence_number": last, "offset": args["through_offset"], "enqueued_at": value["enqueued_at"]},
            "sequence_number": last, "offset": args["through_offset"], "updated_at": self.clock().isoformat(),
        }
        self.native_put("stream_checkpoint", partition.key, result, sequence_number=last, parent_key=partition.connector_id)
        return result


@pytest.fixture
def receiver():
    h = Harness()
    h.seed()
    h.activate()
    h.connector()
    h.signal(100)
    db = ReceiverAbiDatabase(h)
    store = AzureSqlMonitoringStore(db=db, component="worker")
    return h, db, store


def claim(h, store):
    request = OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner),
    )
    return request, store.change_partition_ownership(request)


def start(h, store, lease, *, first=100):
    return store.ensure_stream_start(StreamStartRequest(
        partition=h.partition, lease=lease, first_available_sequence_number=first, observed_at=h.clock(),
    ))


def batch(h, lease, *signals):
    return m.StreamReceiptBatch(request_id=h.next_id(), partition=h.partition, lease=lease, receipts=signals)


def unidentified(h, lease, sequence=100):
    return UnidentifiedReceiptBatch(
        request_id=h.next_id(), lease=lease, receipt=UnidentifiedSignal(
            partition=h.partition, position=h.signal(sequence).position, received_at=h.clock(),
            quarantine=m.QuarantineDisposition(
                observation_id=f"fixture:unidentified:{sequence}", reason="malformed",
                detail="The original envelope has no usable source/id.",
            ),
        ),
    )


def checkpoint(h, store, lease, signal, *, revision=0):
    return store.advance_stream_checkpoint(m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=lease,
        expected_revision=revision, through=signal.position,
    ))


def test_partition_claim_renew_release_and_reclaim_preserve_native_identity_and_fence(receiver):
    h, db, store = receiver
    scope = ConnectorScope(**h.context(), connector_id=h.connector_id, consumer_group="$Default")
    assert store.list_partition_ownership(scope) == ()
    request, ownership = claim(h, store)
    assert ownership.lease.fence == 1 and ownership.modified_at == h.clock()
    assert store.change_partition_ownership(request) == ownership
    assert store.list_partition_ownership(scope) == (ownership,)
    h.clock.advance(1)
    renewed = store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=ownership.etag, claim=request.claim,
    ))
    assert renewed.etag != ownership.etag and renewed.lease.fence == ownership.lease.fence
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "maintenance": True})
    released = store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=renewed.etag, release=renewed.lease,
    ))
    assert released.lease is None and db.partition_leases[h.partition.key].fence == 2
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "maintenance": False})
    reclaimed = store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=released.etag, claim=request.claim,
    ))
    assert reclaimed.lease.fence == 3
    assert store.list_partition_ownership(scope) == (reclaimed,)


@pytest.mark.parametrize("failure", ["before", "after"])
def test_partition_receipt_recovery_precedes_new_revision_and_ownership_cas(receiver, failure):
    h, db, store = receiver
    request = OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner),
    )
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain):
        store.change_partition_ownership(request)
    if failure == "after":
        db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2, "maintenance": True})
    fresh = AzureSqlMonitoringStore(db=db, component="worker")
    result = fresh.change_partition_ownership(request)
    assert result.lease.owner_id == h.owner and result.lease.fence == 1
    assert db.records[("partition_ownership", h.partition.key)].version == 1


def test_actual_broker_start_is_pinned_and_later_history_gaps_do_not_move_it(receiver):
    h, db, store = receiver
    _, owner = claim(h, store)
    pinned = start(h, store, owner.lease)
    assert pinned.first_sequence_number == 100
    assert pinned.history_before_start == "unobserved"
    assert [gap.code for gap in pinned.gaps] == ["unobserved_stream_history"]
    assert store.get_stream_start(h.partition) == pinned
    original_start = db.records[("stream_start", h.partition.key)]
    for first in (99, 101):
        saved = start(h, store, owner.lease, first=first)
        assert saved.first_sequence_number == pinned.first_sequence_number
    assert {gap.code for gap in saved.gaps} == {
        "unobserved_stream_history", "stream_retention_gap", "stream_boundary_regressed",
    }
    assert db.records[("stream_start", h.partition.key)].sequence_number == original_start.sequence_number
    assert store.get_stream_checkpoint(h.partition) is None


@pytest.mark.parametrize("unidentified_input", [False, True])
def test_receiver_commits_original_receipts_and_work_before_checkpoint(receiver, unidentified_input):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    signal = h.signal(100)
    request = unidentified(h, owner.lease) if unidentified_input else batch(h, owner.lease, signal)
    before = deepcopy({key: row for key, row in db.records.items() if row.kind in {"target", "source", "source_head", "review"}})
    result = (store.record_unidentified_receipts if unidentified_input else store.record_stream_receipts)(request)
    assert result.publication_status == "pending_validation"
    assert len(result.receipt_keys) == len(result.work_ids) == 1
    work = db.model("work", result.work_ids[0], m.MonitoringWork)
    assert work.kind == "reconcile_state" and work.lease is None and work.execution is None
    assert store.get_stream_checkpoint(h.partition) is None
    assert store.get_stream_acceptance(h.version, request.request_id) == result
    saved = checkpoint(h, store, owner.lease, signal)
    assert saved.position == signal.position
    assert store.get_stream_checkpoint(h.partition) == saved
    assert all(db.records[key] == row for key, row in before.items())
    assert not any(method == "execute" for method, _, _ in db.calls)


def test_every_delivery_position_survives_event_deduplication_and_replay_fence_is_unchanged(receiver):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    first = h.signal(100)
    second = m.SignalReceipt.model_validate({**h.signal(101).model_dump(), "delivery": first.delivery})
    request = batch(h, owner.lease, first, second)
    result = store.record_stream_receipts(request)
    assert result.receipt_keys == (first.delivery.key, first.delivery.key)
    assert len([row for row in db.records.values() if row.kind == "signal"]) == 1
    assert len([row for row in db.records.values() if row.kind == "stream_position"]) == 2
    before = deepcopy((db.records, db.receipts))
    assert store.record_stream_receipts(request) == result
    assert store.get_stream_acceptance(h.version, request.request_id) == result
    assert (db.records, db.receipts) == before
    assert checkpoint(h, store, owner.lease, second).position == second.position


def test_new_batch_replaying_existing_position_recovers_its_exact_original_acceptance(receiver):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    signal = h.signal(100)
    original = batch(h, owner.lease, signal)
    store.record_stream_receipts(original)
    repeated = batch(h, owner.lease, signal)
    result = store.record_stream_receipts(repeated)
    assert result.receipt_keys == (signal.delivery.key,)
    before = deepcopy((db.records, db.receipts))
    assert store.get_stream_acceptance(h.version, repeated.request_id) == result
    assert store.record_stream_receipts(repeated) == result
    assert (db.records, db.receipts) == before
    assert store.get_stream_acceptance(h.version, h.next_id()) is None


@pytest.mark.parametrize("backend", ["memory", "sql"])
def test_unidentified_redelivery_keeps_the_same_original_position_receipt(receiver, backend):
    h, db, sql_store = receiver
    store = sql_store if backend == "sql" else InMemoryMonitoringStore(clock=h.clock, state=h.state, component="worker")
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    original = unidentified(h, owner.lease)
    first = store.record_unidentified_receipts(original)
    h.clock.advance(1)
    repeated = UnidentifiedReceiptBatch(
        request_id=h.next_id(), lease=owner.lease,
        receipt=original.receipt.model_copy(update={"received_at": h.clock()}),
    )
    second = store.record_unidentified_receipts(repeated)
    assert second.receipt_keys == first.receipt_keys == (f"{h.partition.key}:unidentified:100",)
    assert store.record_unidentified_receipts(original) == first
    bad = UnidentifiedReceiptBatch(
        request_id=h.next_id(), lease=owner.lease,
        receipt=repeated.receipt.model_copy(update={
            "quarantine": repeated.receipt.quarantine.model_copy(update={"detail": "Different original evidence."}),
        }),
    )
    with pytest.raises(MonitoringConflict):
        store.record_unidentified_receipts(bad)
    assert store.get_stream_checkpoint(h.partition) is None


@pytest.mark.parametrize("unidentified_input", [False, True])
@pytest.mark.parametrize("failure", ["before", "after"])
def test_original_new_position_receipt_recovers_lost_ack_without_advancing_checkpoint(receiver, failure, unidentified_input):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    request = unidentified(h, owner.lease) if unidentified_input else batch(h, owner.lease, h.signal(100))
    before = deepcopy((db.records, db.receipts))
    db.fail_commit = failure
    with pytest.raises(MonitoringCommitUncertain) as uncertain:
        (store.record_unidentified_receipts if unidentified_input else store.record_stream_receipts)(request)
    assert uncertain.value.operation == "worker.commit_positions"
    assert uncertain.value.idempotency_id == request.request_id
    fresh = AzureSqlMonitoringStore(db=db, component="worker")
    result = fresh.get_stream_acceptance(h.version, request.request_id)
    if failure == "before":
        assert result is None and (db.records, db.receipts) == before
    else:
        assert result.request_id == request.request_id and result.publication_status == "pending_validation"
    assert fresh.get_stream_checkpoint(h.partition) is None


def test_checkpoint_refuses_holes_rebound_offsets_and_forged_enqueue_times(receiver):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    first, last = h.signal(100), h.signal(102)
    store.record_stream_receipts(batch(h, owner.lease, first, last))
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringConflict):
        checkpoint(h, store, owner.lease, last)
    for change in ({"offset": "not-the-original"}, {"enqueued_at": h.clock() - timedelta(seconds=1)}):
        through = m.StreamPosition.model_validate({**first.position.model_dump(), **change})
        with pytest.raises(MonitoringConflict, match="exact durable"):
            store.advance_stream_checkpoint(m.StreamCheckpointAdvance(
                request_id=h.next_id(), partition=h.partition, lease=owner.lease, expected_revision=0, through=through,
            ))
    assert (db.records, db.receipts) == before


def test_old_checkpoint_receipt_replays_after_later_progress_and_policy_change(receiver):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    first, second = h.signal(100), h.signal(101)
    store.record_stream_receipts(batch(h, owner.lease, first, second))
    request = m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=owner.lease, expected_revision=0, through=first.position,
    )
    original = store.advance_stream_checkpoint(request)
    checkpoint(h, store, owner.lease, second, revision=1)
    db.control = m.DeploymentControl.model_validate({**db.control.model_dump(), "revision": 2, "maintenance": True})
    h.clock.advance(300)
    assert AzureSqlMonitoringStore(db=db, component="worker").advance_stream_checkpoint(request) == original
    assert store.get_stream_checkpoint(h.partition).position == second.position


@pytest.mark.parametrize("failure", ["receipt_failure", "wrong_owner", "expired_owner", "rest_authority"])
def test_receiver_refusals_do_not_publish_partial_work_or_fences(receiver, failure):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    signal, lease = h.signal(100), owner.lease
    if failure == "receipt_failure":
        db.fail_receiver_receipt = True
    elif failure == "wrong_owner":
        lease = lease.model_copy(update={"owner_id": uid(999)})
    elif failure == "expired_owner":
        h.clock.advance(121)
    else:
        signal = signal.model_copy(update={"observation": signal.observation.model_copy(update={"authority": "rest"})})
    before = deepcopy((db.records, db.receipts))
    with pytest.raises((MonitoringConflict, MonitoringLeaseLost, ValidationError)):
        store.record_stream_receipts(batch(h, lease, signal))
    assert (db.records, db.receipts) == before


@pytest.mark.parametrize("component", ["web", "controller"])
def test_receiver_methods_do_not_gain_authority_from_another_component(receiver, component):
    h, db, _ = receiver
    db.principal = component
    store = AzureSqlMonitoringStore(db=db, component=component)
    with pytest.raises(MonitoringComponentDenied):
        store.claim_partition(m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner))
    with pytest.raises(MonitoringComponentDenied):
        store.get_stream_start(h.partition)
    with pytest.raises(MonitoringComponentDenied):
        store.get_stream_acceptance(h.version, h.next_id())


@pytest.mark.parametrize("identified_input", [False, True])
def test_controller_consumes_native_stream_handoff_without_producer_authority(receiver, identified_input):
    h, db, worker = receiver
    _, owner = claim(h, worker)
    start(h, worker, owner.lease)
    if identified_input:
        first = h.signal(100)
        second = m.SignalReceipt.model_validate({**h.signal(101).model_dump(), "delivery": first.delivery})
        result = worker.record_stream_receipts(batch(h, owner.lease, first, second))
    else:
        result = worker.record_unidentified_receipts(unidentified(h, owner.lease))
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(980), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    producer = controller.get_reconciliation_request(h.version, result.request_id, producer="worker")
    assert len(producer.evidence) == 1
    assert producer.request_payload["receipt_keys"] or producer.request_payload["unidentified_keys"]
    outcome = controller.reconcile_work(work)
    assert outcome.state == ("published" if identified_input else "rejected")
    assert controller.get_work(h.version, work.work_id).state == "completed"
    assert not controller.get_validation_frontier(h.version, outcome.frontier_key).pending
    assert not any(row.kind == "action" for row in db.records.values())


def test_historical_stream_disposition_and_processed_marker_precede_frontier_completion(receiver):
    h, db, worker = receiver
    _, owner = claim(h, worker)
    start(h, worker, owner.lease)
    old = m.SourceRunObservation.model_validate({
        **h.observation().model_dump(), "started_at": h.control.activation_cutoff - timedelta(minutes=2),
        "ended_at": h.control.activation_cutoff - timedelta(minutes=1),
    })
    receipt = worker.record_stream_receipts(batch(h, owner.lease, h.signal(100, old)))
    db.principal = "controller"
    controller = AzureSqlMonitoringStore(db=db, component="controller")
    work = controller.claim_work(m.WorkClaimRequest(
        **h.context(), owner_id=uid(980), kinds=("reconcile_state",), limit=1, per_workspace_limit=1,
    ))[0]
    result = controller.reconcile_work(work)
    assert result.state == "published"
    disposition = controller.get_source_disposition(old.execution)
    assert disposition.disposition == "historical" and disposition.disposition_request_id is not None
    assert old.key in db.processed
    producer = controller.get_reconciliation_request(h.version, receipt.request_id, producer="worker")
    assert not controller.get_validation_frontier(h.version, producer.frontier_key).pending
    assert controller.get_work(h.version, work.work_id).state == "completed"


def test_receiver_result_corruption_rolls_back_native_acceptance(receiver, monkeypatch):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    original = db.positions

    def corrupt(args, partition):
        return {**original(args, partition), "position_count": True}

    monkeypatch.setattr(db, "positions", corrupt)
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringUnavailable):
        store.record_stream_receipts(batch(h, owner.lease, h.signal(100)))
    assert (db.records, db.receipts) == before


def test_stale_journal_cannot_replace_the_native_partition_lease_check(receiver):
    h, db, store = receiver
    _, owner = claim(h, store)
    start(h, store, owner.lease)
    db.partition_leases[h.partition.key] = owner.lease.model_copy(update={"owner_id": uid(999), "fence": 2})
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringLeaseLost):
        store.record_stream_receipts(batch(h, owner.lease, h.signal(100)))
    assert (db.records, db.receipts) == before


def test_worker_component_name_cannot_impersonate_the_sql_principal(receiver):
    h, db, store = receiver
    db.principal = "controller"
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringComponentDenied):
        claim(h, store)
    assert (db.records, db.receipts) == before


@pytest.mark.parametrize("group", ["$default", "$DEFAULT"])
def test_consumer_group_authority_is_exact_before_partition_identity(receiver, group):
    h, db, store = receiver
    partition = h.partition.model_copy(update={"consumer_group": group})
    before = deepcopy((db.records, db.receipts))
    with pytest.raises(MonitoringConflict, match="consumer group"):
        store.change_partition_ownership(OwnershipChange(
            partition=partition, expected_etag=None,
            claim=m.PartitionClaimRequest(partition=partition, owner_id=h.owner),
        ))
    assert (db.records, db.receipts) == before


def test_stream_contract_accepts_exactly_two_hundred_positions_and_refuses_two_hundred_one(receiver):
    h, _, store = receiver
    _, owner = claim(h, store)
    signals = tuple(h.signal(number) for number in range(100, 301))
    request = batch(h, owner.lease, *signals[:200])
    assert len(request.receipts) == 200
    with pytest.raises(ValidationError):
        batch(h, owner.lease, *signals)


def test_receiver_heartbeat_uses_native_health_only_including_stopping(receiver):
    h, db, store = receiver
    before = db.records[("connector", h.connector_id)]
    request = ReceiverHeartbeat(
        **h.context(), worker_id=h.owner, connector_id=h.connector_id, observed_at=h.clock(),
        state="running", transport_connected=True,
    )
    result = store.record_receiver_heartbeat(request)
    assert result.state == "running" and result.observed_at == h.clock()
    assert db.records[("connector", h.connector_id)] == before
    stopping = store.record_receiver_heartbeat(request.model_copy(update={"state": "stopping"}))
    assert stopping.state == "stopping"
    assert db.records[("connector", h.connector_id)] == before


async def test_real_checkpoint_facade_uses_all_v2_receiver_bindings_without_fixture_authority():
    h = Harness()
    h.seed()
    h.activate()
    manifest = h.connector(
        endpoint=m.EndpointMetadata(namespace="sample.servicebus.windows.net", entity="owned-events", consumer_group="$Default"),
        destination_id=uid(44),
    )
    h.signal(100)
    db = ReceiverAbiDatabase(h)
    store = AzureSqlMonitoringStore(db=db, component="worker")
    context = m.MonitoringContext(**h.context())
    binding = ConnectorBinding(
        tenant_id=context.tenant_id, connector_id=manifest.connector_id, workspace_id=manifest.workspace_id,
        eventstream_id=manifest.eventstream_id, destination_id=manifest.destination_id, endpoint=manifest.endpoint,
    )
    checkpoints = SqlCheckpointStore(store, persistence=store, binding=binding, context=context, clock=h.clock)
    checkpoints.bind_partitions(("0",))
    properties = {
        "id": "0", "eventhub_name": manifest.endpoint.entity, "beginning_sequence_number": 100,
        "last_enqueued_sequence_number": 102, "is_empty": False,
    }
    checkpoints.set_partition_properties("0", properties)
    owned = await checkpoints.claim_ownership([{
        "fully_qualified_namespace": manifest.endpoint.namespace, "eventhub_name": manifest.endpoint.entity,
        "consumer_group": manifest.endpoint.consumer_group, "partition_id": "0", "owner_id": h.owner, "etag": None,
    }])
    assert len(owned) == 1
    await checkpoints.initialize_partition("0", properties)
    position = h.signal(100).position
    accepted = await checkpoints.accept("0", position, summarize_body(b"not-json"))
    assert isinstance(accepted, UnidentifiedSignal)
    intake = store.get_stream_acceptance(context, checkpoints._pending["0"].request_id)
    assert intake.publication_status == "pending_validation" and len(intake.work_ids) == 1
    work = m.MonitoringWork.model_validate_json(db.records[("work", intake.work_ids[0])].payload)
    assert work.kind == "reconcile_state" and work.target is None and work.execution is None
    assert store.get_stream_checkpoint(h.partition) is None
    await checkpoints.update_checkpoint({
        "fully_qualified_namespace": manifest.endpoint.namespace, "eventhub_name": manifest.endpoint.entity,
        "consumer_group": manifest.endpoint.consumer_group, "partition_id": "0",
        "offset": position.offset, "sequence_number": position.sequence_number,
    })
    assert store.get_stream_checkpoint(h.partition).position == position
    assert store.component == "worker"
