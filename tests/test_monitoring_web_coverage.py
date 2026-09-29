"""The Command Center must report the coverage the controller sees.

The web reads monitoring state only through its kernel views, and web_read did
not include queue work, REST checkpoints, their Power BI windows or stream
starts. Live, the Command Center always showed an empty backlog, no completed
poll window and no stream gaps, while the controller saw all three. The SQL
test double served every record to every component, so each offline test
passed.

The web may count queue work but never read it: work rows carry lease fences
and action reservations, which the web has no reason to hold.
"""

from __future__ import annotations

import pytest
from test_monitoring_publication import drain, poll_page, ready
from test_monitoring_sql_abi import AbiDatabase
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringNotBootstrapped
from triage.monitoring.events import StreamStartRequest
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.monitoring.sql_store import AzureSqlMonitoringStore


def observed_estate(workload: m.Workload):
    """Validated poll coverage, queued work and a stream-start gap, in memory."""
    h, stores = ready(workload)
    poll_page(h, stores, workload=workload, complete=True)
    assert drain(stores, h)[0].state == "published"
    h.connector()
    partition = h.signal(101).partition
    lease = stores["worker"].claim_partition(m.PartitionClaimRequest(partition=partition, owner_id=uid(620)))
    stores["worker"].ensure_stream_start(StreamStartRequest(
        partition=partition, lease=lease, first_available_sequence_number=101, observed_at=h.clock(),
    ))
    return h, stores


@pytest.mark.parametrize("workload", ["fabric_pipeline", "powerbi"])
def test_the_command_center_reports_the_coverage_the_controller_sees(workload):
    h, memory = observed_estate(workload)
    context = m.MonitoringContext(**h.context())
    expected = memory["controller"].coverage(context)
    assert expected.backlog_count > 0
    assert expected.last_poll_window_end is not None
    assert "unobserved_stream_history" in {gap.code for gap in expected.gaps}

    db = AbiDatabase(h, principal="controller")
    db.seed_published_fixture(h)
    for component in ("controller", "web"):
        db.principal = component
        assert AzureSqlMonitoringStore(db=db, component=component).coverage(context) == expected, component


def test_the_web_counts_queue_work_without_reading_it():
    h, _ = observed_estate("fabric_pipeline")
    db = AbiDatabase(h, principal="web")
    db.seed_published_fixture(h)
    web = AzureSqlMonitoringStore(db=db, component="web")
    work = next(row for row in db.records.values() if row.kind == "work")

    assert web.get_work(m.MonitoringContext(**h.context()), work.key) is None
    kernel = build_permission_kernel()
    status = kernel.names.object("web_work_status")
    ddl = next(obj.ddl for obj in kernel.objects if obj.name == status)
    assert "[payload]" not in ddl
    assert "[full_key]" not in ddl


def test_a_web_without_its_work_status_projection_fails_closed():
    h, _ = observed_estate("fabric_pipeline")
    db = AbiDatabase(h, principal="web")
    db.seed_published_fixture(h)
    db.missing_procedures.add(build_permission_kernel().names.object("web_work_status"))

    with pytest.raises(MonitoringNotBootstrapped, match="web_work_status"):
        AzureSqlMonitoringStore(db=db, component="web").coverage(m.MonitoringContext(**h.context()))
