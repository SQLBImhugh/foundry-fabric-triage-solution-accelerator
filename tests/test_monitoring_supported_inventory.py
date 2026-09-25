"""Unsupported historical observations are not operational monitoring inventory."""

from __future__ import annotations

import pytest
from test_monitoring_sql_store import SqlHarness
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m


@pytest.fixture(params=("memory", "sql"))
def historical_inventory(request, tmp_path):
    h = Harness() if request.param == "memory" else SqlHarness(tmp_path / "supported-inventory.sqlite")
    generation_id = h.next_id()
    h.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=generation_id,
            selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            adapter="historic_broad_inventory", authority="fixture", completeness="partial",
            started_at=h.clock(), continuation="unfinished",
            gaps=(
                m.CoverageGap(code="unsupported_item_type", detail="No detector contract for Fabric Report",
                              workspace_id=uid(100), item_id=uid(1001)),
                m.CoverageGap(code="http_401", detail="REST evidence read returned HTTP 401", workspace_id=uid(101)),
            ),
        ),
        items=(
            m.InventoryItem(
                **h.context(), generation_id=generation_id, workspace_id=uid(100), item_id=uid(1000),
                name="Supported pipeline", item_type="DataPipeline", workload="fabric_pipeline", observed_at=h.clock(),
            ),
            m.InventoryItem(
                **h.context(), generation_id=generation_id, workspace_id=uid(100), item_id=uid(1001),
                name="Retained report", item_type="Report", unsupported_reason="No report detector",
                observed_at=h.clock(),
            ),
        ),
    ))
    return h, generation_id


def test_operational_inventory_omits_retained_unsupported_rows_without_rewriting_generation(historical_inventory):
    h, generation_id = historical_inventory
    query = m.TargetQuery(**h.context(), limit=1)
    page = h.store.list_inventory(query)
    assert [item.name for item in page.items] == ["Supported pipeline"]
    assert page.next_cursor is None
    original = h.store.list_inventory(m.TargetQuery(**h.context()), generation_id=generation_id)
    assert {item.name for item in original.items} == {"Supported pipeline", "Retained report"}


def test_coverage_counts_only_supported_inventory_and_preserves_real_access_gaps(historical_inventory):
    h, _ = historical_inventory
    coverage = h.store.coverage(m.MonitoringContext(**h.context()))
    assert coverage.discovered_count == 1
    assert coverage.scope_item_count is None and coverage.inventory_completeness == "partial"
    codes = {gap.code for gap in coverage.gaps}
    assert "http_401" in codes
    assert "unsupported_item_type" not in codes


def test_complete_supported_scan_does_not_claim_an_unsearched_report_was_deleted(historical_inventory):
    h, original_id = historical_inventory
    context = m.MonitoringContext(**h.context())
    original_work = h.store.get_work(context, original_id)
    if original_work is not None and original_work.lease is not None:
        h.store.disposition_work(m.WorkDispositionRequest(
            **h.context(), request_id=h.next_id(), work_id=original_work.work_id,
            expected_work_revision=original_work.revision, lease=original_work.lease,
            disposition="superseded", detail="The historical fixture scan is no longer running.",
        ))
    h.clock.advance(1)
    generation_id = h.next_id()
    retained = h.store.list_inventory(m.TargetQuery(**h.context()), generation_id=original_id).items
    supported, = [item for item in retained if item.workload is not None]
    h.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=generation_id,
            selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            adapter="supported_types", authority="fixture", completeness="complete",
            started_at=h.clock(), completed_at=h.clock(), completed_pages=2, discovered_count=1,
        ),
        items=(supported.model_copy(update={"generation_id": generation_id, "observed_at": h.clock()}),),
    ))
    with h.store._backend.transaction(write=False, operation="verify_retained_audit", request_id="read"):
        row = h.store._backend.get("inventory", f"{uid(100)}:{uid(1001)}", context)
    assert row is not None and row.status == "present"
    assert m.InventoryItem.model_validate_json(row.payload).generation_id == original_id
