"""Collection completion follows the same evidence rules on both adapters.

A worker completes collection work only when the evidence it collected is
durable and current:

- an inventory work item needs a finished, complete pass for every selector it
  declared, started after the work was created;
- a capability probe needs a capability observation checked after the probe
  was created.

The memory adapter enforced both. The SQL adapter only required an accepted
batch under the work fence, so a partial or unfinished pass, or a capability
observation older than its probe, completed the work there. Production closes
finished passes with a ``superseded`` disposition, so the SQL gap was latent.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from test_monitoring_controller_publication_integration import IDENTITY, PublicationHarness
from test_monitoring_inventory import CONTEXT
from test_monitoring_inventory_retention import record_scan_page
from test_monitoring_reinventory_admission import start_pass

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringConflict


def rows(fixture: PublicationHarness) -> dict:
    return {(row.kind, row.key): (row.version, row.status, row.payload) for row in fixture.rows()}


@pytest.mark.parametrize("adapter", ["memory", "sql"])
@pytest.mark.parametrize("finished,complete", [(True, False), (False, True)], ids=["partial", "unfinished"])
async def test_an_incomplete_inventory_pass_cannot_complete_its_discovery(adapter, finished, complete, tmp_path):
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    work = await start_pass(fixture)
    record_scan_page(fixture, work, items=(fixture.identity.item_id,), finished=finished, complete=complete)
    before = rows(fixture)

    with pytest.raises(MonitoringConflict, match="declared inventory is durable"):
        fixture.complete_collection(work)

    assert fixture.use("worker").get_work(CONTEXT, work.work_id).state == "leased"
    assert rows(fixture) == before


@pytest.mark.parametrize("adapter", ["memory", "sql"])
async def test_a_complete_inventory_pass_completes_its_discovery(adapter, tmp_path):
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    work = await start_pass(fixture)
    record_scan_page(fixture, work, items=(fixture.identity.item_id,), finished=True)

    assert fixture.complete_collection(work).state == "completed"


async def _probe(fixture: PublicationHarness) -> tuple[m.MonitoringWork, str]:
    work = await start_pass(fixture)
    record_scan_page(fixture, work, items=(fixture.identity.item_id,), finished=True)
    fixture.complete_collection(work)
    await fixture.drain()
    fixture.h.clock.advance(60)
    probe, = fixture.claim("worker", "capability_probe")
    return probe, work.work_id


def _record_capability(fixture: PublicationHarness, probe: m.MonitoringWork, generation: str, *, checked_at) -> None:
    fixture.use("worker").record_capability(
        fixture.version("worker"),
        m.CapabilityObservation(
            capability_id=fixture.h.next_id(), target=fixture.identity, inventory_generation=generation,
            collector_identity_id=IDENTITY, read_status="verified", event_status="verified",
            checked_at=checked_at, expires_at=checked_at + timedelta(hours=1),
        ),
        commit=m.CollectionCommit(work_id=probe.work_id, lease=probe.lease, expected_work_revision=probe.revision),
    )


@pytest.mark.parametrize("adapter", ["memory", "sql"])
async def test_a_capability_checked_before_its_probe_cannot_complete_the_probe(adapter, tmp_path):
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    probe, generation = await _probe(fixture)
    _record_capability(fixture, probe, generation, checked_at=probe.created_at - timedelta(seconds=1))
    before = rows(fixture)

    with pytest.raises(MonitoringConflict, match="no durable current probe result"):
        fixture.complete_collection(probe)

    assert fixture.use("worker").get_work(CONTEXT, probe.work_id).state == "leased"
    assert rows(fixture) == before


@pytest.mark.parametrize("adapter", ["memory", "sql"])
async def test_a_current_capability_completes_its_probe(adapter, tmp_path):
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    probe, generation = await _probe(fixture)
    _record_capability(fixture, probe, generation, checked_at=fixture.h.clock())

    assert fixture.complete_collection(probe).state == "completed"
