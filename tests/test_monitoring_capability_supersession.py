"""Capability evidence for a replaced inventory generation is a decision, not an outage.

The worker probes service access against an item's current inventory
generation. The controller publishes that evidence later and refuses it if a
newer generation has re-stamped the item in between. The refusal was raised
inside the reconciliation transaction, so the handoff was never dispositioned:
its lease expired and it was claimed again. A deployed controller retried 244
such handoffs up to 15 times each over two days. About a third of its
heartbeats failed, and every admitted target stayed paused because no current
access evidence could be published.

Nothing can make superseded evidence current. The defined outcome is rejection
of the handoff plus one probe of the generation the item now has. These tests
drive the worker and controller through the memory adapter and the SQL
protocol double.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_inventory import CONTEXT, IDENTITY
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.records import stable_id


async def discover(fixture: PublicationHarness) -> None:
    fixture.use("web").request_discovery(
        fixture.version("web"), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=fixture.h.next_id(),
    )
    await fixture.drain()


def record_generation(fixture: PublicationHarness) -> str:
    """Record one complete generation that contains the fixture's item."""
    work, = fixture.claim("worker", "inventory")
    generation = m.InventoryGeneration(
        **CONTEXT.model_dump(), generation_id=work.work_id, selector=work.discovery_selector,
        adapter="explicit_offline_inventory", authority="tenant_admin", completeness="complete",
        started_at=fixture.h.clock(), completed_at=fixture.h.clock(), discovered_count=1, completed_pages=1,
    )
    fixture.use("worker").record_inventory(m.InventoryBatch(
        request_id=fixture.h.next_id(), expected=fixture.version("worker"), generation=generation,
        items=(m.InventoryItem(
            **CONTEXT.model_dump(), generation_id=generation.generation_id,
            workspace_id=fixture.identity.workspace_id, item_id=fixture.identity.item_id,
            name="Synthetic pipeline", item_type="DataPipeline", workload="fabric_pipeline",
            observed_at=fixture.h.clock(),
        ),),
        commit=m.InventoryCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_generation_revision=0,
        ),
    ))
    fixture.complete_collection(work)
    return generation.generation_id


def record_probe(fixture: PublicationHarness, generation_id: str) -> m.MonitoringWork:
    """Execute the one queued probe, taken against ``generation_id``."""
    probe, = fixture.claim("worker", "capability_probe")
    fixture.use("worker").record_capability(
        fixture.version("worker"),
        m.CapabilityObservation(
            capability_id=fixture.h.next_id(), target=fixture.identity, inventory_generation=generation_id,
            collector_identity_id=IDENTITY, read_status="verified", event_status="verified",
            checked_at=fixture.h.clock(), expires_at=fixture.h.clock() + timedelta(hours=1),
        ),
        commit=m.CollectionCommit(work_id=probe.work_id, lease=probe.lease, expected_work_revision=probe.revision),
    )
    fixture.complete_collection(probe)
    return probe


def claim_reconciliation(fixture: PublicationHarness) -> dict[str, m.MonitoringWork]:
    controller = fixture.use("controller")
    claimed = controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=uid(90_101), kinds=("reconcile_state",), limit=20,
        per_workspace_limit=20, lease_seconds=120,
    ))
    return {
        controller.get_reconciliation_request(
            CONTEXT, work.reconcile_request_id, producer=work.reconcile_producer,
        ).topic: work
        for work in claimed
    }


def access_verified(fixture: PublicationHarness) -> int:
    return fixture.use("controller").snapshot(CONTEXT).coverage.access_verified_count


@pytest.fixture
async def superseded(request, tmp_path):
    """Capability evidence for G1 is accepted, then the worker re-stamps the item into G2."""
    fixture = PublicationHarness(request.param, tmp_path, register_transport=False)
    await discover(fixture)
    first = record_generation(fixture)
    await fixture.drain()
    await discover(fixture)
    record_probe(fixture, first)
    second = record_generation(fixture)
    return fixture, first, second


@pytest.mark.parametrize("superseded", ["memory", "sql"], indirect=True)
async def test_superseded_capability_is_rejected_once_and_the_current_generation_is_probed(superseded):
    fixture, _, second = superseded
    handoffs = claim_reconciliation(fixture)
    assert set(handoffs) == {"capability", "inventory"}

    line = await fixture.execute(handoffs["capability"])

    assert line.endswith("deterministic reconciliation rejected.")
    assert fixture.use("controller").get_work(CONTEXT, handoffs["capability"].work_id).state == "completed"
    # The newer generation is not published yet, so this probe comes from the rejection itself.
    probe = fixture.use("worker").get_work(CONTEXT, stable_id(CONTEXT, f"probe:{second}:{fixture.identity.key}"))
    assert probe is not None and probe.kind == "capability_probe" and probe.state == "queued"
    assert probe.target == fixture.identity
    assert access_verified(fixture) == 0


@pytest.mark.parametrize("superseded", ["memory", "sql"], indirect=True)
async def test_evidence_for_the_current_generation_still_publishes_after_a_rejection(superseded):
    fixture, _, second = superseded
    handoffs = claim_reconciliation(fixture)
    await fixture.execute(handoffs["capability"])
    await fixture.execute(handoffs["inventory"])

    record_probe(fixture, second)
    await fixture.drain()

    assert access_verified(fixture) == 1
