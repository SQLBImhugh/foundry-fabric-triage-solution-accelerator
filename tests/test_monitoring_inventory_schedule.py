"""One periodic inventory chain per selector.

A finished pass carries ``next_scan_at``, and the controller queued the next
pass for that selector when it published the pass. Each pass scheduled its own
successor, so every extra pass of a selector (a discovery request or a scope
activation) started another chain. In the MorkNet lab two chains scanned the
monitored workspace about 20 minutes apart from 2026-09-25, doubling scans and
the pass records retention must delete.

The controller now queues a successor only when no other active inventory work
scans the same selector. A chain continues through its own queued successor;
an extra pass finishes without starting another.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_inventory_retention import close, record_scan_page
from test_monitoring_reinventory_admission import selector, start_pass

from triage.monitoring import models as m

HOUR = 3_600


def scheduled(fixture: PublicationHarness) -> list[m.MonitoringWork]:
    """Active inventory work for the monitored workspace's selector."""
    wanted = selector(fixture)
    works = [
        m.MonitoringWork.model_validate_json(row.payload) for row in fixture.rows()
        if row.kind == "work" and row.work_kind == "inventory" and row.status in m.ACTIVE_WORK_STATES
    ]
    return sorted((work for work in works if work.discovery_selector == wanted), key=lambda work: work.due_at)


async def finish(fixture: PublicationHarness, work: m.MonitoringWork, *, close_first: bool = True) -> None:
    record_scan_page(fixture, work, items=(fixture.identity.item_id,), finished=True, next_scan_in=HOUR)
    if close_first:
        close(fixture, work)
        await fixture.drain()
    else:
        # The controller can publish a finished pass while its own work is
        # still leased; that work must not count as the next pass.
        await fixture.drain()
        close(fixture, work)


@pytest.mark.parametrize("adapter", ["memory", "sql"])
async def test_an_extra_pass_of_a_selector_does_not_start_a_second_chain(adapter, tmp_path):
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    await finish(fixture, await start_pass(fixture))
    first, = scheduled(fixture)
    assert first.due_at == fixture.h.clock() + timedelta(seconds=HOUR)

    fixture.h.clock.advance(20 * 60)
    await finish(fixture, await start_pass(fixture))

    assert [work.work_id for work in scheduled(fixture)] == [first.work_id]


@pytest.mark.parametrize("adapter", ["memory", "sql"])
@pytest.mark.parametrize("close_first", [True, False], ids=["closed", "still_leased"])
async def test_the_chain_continues_through_its_own_successor(adapter, close_first, tmp_path):
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    await finish(fixture, await start_pass(fixture), close_first=close_first)
    for _ in range(3):
        successor, = scheduled(fixture)
        fixture.h.clock.advance(HOUR)
        work, = fixture.claim("worker", "inventory")
        assert work.work_id == successor.work_id
        await finish(fixture, work, close_first=close_first)
        following, = scheduled(fixture)
        assert following.work_id != successor.work_id and following.state == "queued"
