"""A re-inventory in progress must not pause targets its previous pass verified.

Every periodic inventory pass re-stamps the workspace and item records page by
page. While the pass was still collecting, the workspace entry pointed at an
unfinished generation, so scope membership read as unverified: the controller
paused every target in the workspace, the worker stopped polling, and the
Command Center showed them paused. Live, all three MorkNet targets paused for
about four minutes in every refresh (23:32 to 23:36 UTC on 2026-09-28).

A pass that is still collecting neither confirms nor refutes membership. Until
it finishes, membership stays as the previous finished pass for the same
selector verified it, for at most INVENTORY_COLLECTION_GRACE. Explicit deletions
observed on a page still apply at once, and a pass that finishes incomplete
leaves membership unverified.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_inventory import CONTEXT, IDENTITY
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.engine import INVENTORY_COLLECTION_GRACE


def claim_all(fixture: PublicationHarness, component: str, kind: str) -> tuple[m.MonitoringWork, ...]:
    return fixture.use(component).claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=uid(90_201), kinds=(kind,), limit=20, per_workspace_limit=20,
        lease_seconds=120,
    ))


def selector(fixture: PublicationHarness) -> m.ScopeSelector:
    return m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="workspace", workspace_id=fixture.identity.workspace_id)


async def start_pass(fixture: PublicationHarness, chosen: m.ScopeSelector | None = None) -> m.MonitoringWork:
    """Request one inventory pass (of the monitored workspace by default) and claim its collection work."""
    fixture.use("web").request_discovery(
        fixture.version("web"), chosen or selector(fixture), request_id=fixture.h.next_id(),
    )
    await fixture.drain()
    work, = fixture.claim("worker", "inventory")
    return work


def record_page(
    fixture: PublicationHarness, work: m.MonitoringWork, prior: m.InventoryGeneration | None, *,
    finished: bool, complete: bool = True, workspace_state: str = "present",
) -> m.InventoryGeneration:
    """Record one page that observes the workspace entry and the monitored item."""
    now = fixture.h.clock()
    generation_id = work.work_id
    in_progress = (m.CoverageGap(code="inventory_in_progress", detail="Inventory has a durable continuation"),)
    generation = m.InventoryGeneration(
        **CONTEXT.model_dump(), generation_id=generation_id, selector=work.discovery_selector,
        adapter="explicit_offline_inventory", authority="tenant_admin",
        completeness="complete" if finished and complete else "partial",
        started_at=prior.started_at if prior else now, completed_at=now if finished else None,
        continuation=None if finished else f"page-{(prior.completed_pages if prior else 0) + 2}",
        discovered_count=1, completed_pages=(prior.completed_pages if prior else 0) + 1,
        gaps=() if finished and complete else in_progress if not finished else (
            m.CoverageGap(code="http_401", detail="A page of the workspace could not be read."),
        ),
    )
    return fixture.use("worker").record_inventory(m.InventoryBatch(
        request_id=fixture.h.next_id(), expected=fixture.version("worker"), generation=generation,
        workspaces=(m.InventoryWorkspace(
            **CONTEXT.model_dump(), generation_id=generation_id, workspace_id=fixture.identity.workspace_id,
            name="Monitored workspace", state=workspace_state, observed_at=now,
        ),),
        items=(m.InventoryItem(
            **CONTEXT.model_dump(), generation_id=generation_id, workspace_id=fixture.identity.workspace_id,
            item_id=fixture.identity.item_id, name="Monitored pipeline", item_type="DataPipeline",
            workload="fabric_pipeline", observed_at=now,
        ),) if workspace_state == "present" else (),
        commit=m.InventoryCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_generation_revision=prior.revision if prior else 0,
            expected_continuation=prior.continuation if prior else None,
        ),
    ))


async def finish_pass(fixture: PublicationHarness, work, prior, *, complete: bool = True) -> m.InventoryGeneration:
    """Record the final page and close the work the way the polling worker does."""
    generation = record_page(fixture, work, prior, finished=True, complete=complete)
    worker = fixture.use("worker")
    current = worker.get_work(CONTEXT, work.work_id)
    worker.disposition_work(m.WorkDispositionRequest(
        **CONTEXT.model_dump(), request_id=fixture.h.next_id(), work_id=current.work_id,
        expected_work_revision=current.revision, lease=current.lease, disposition="superseded",
        detail="Inventory and its bounded next-scan request are durable; controller publication is separate",
    ))
    await fixture.drain()
    return generation


def record_probe(fixture: PublicationHarness, generation_id: str, *, hours: int = 1) -> None:
    probe = next(
        work for work in claim_all(fixture, "worker", "capability_probe")
        if work.target == fixture.identity
    )
    fixture.use("worker").record_capability(
        fixture.version("worker"),
        m.CapabilityObservation(
            capability_id=fixture.h.next_id(), target=fixture.identity, inventory_generation=generation_id,
            collector_identity_id=IDENTITY, read_status="verified", event_status="verified",
            checked_at=fixture.h.clock(), expires_at=fixture.h.clock() + timedelta(hours=hours),
        ),
        commit=m.CollectionCommit(work_id=probe.work_id, lease=probe.lease, expected_work_revision=probe.revision),
    )
    fixture.complete_collection(probe)


async def activate(fixture: PublicationHarness) -> None:
    web = fixture.use("web")
    preview = web.preview_scope(m.ScopePreviewRequest(
        expected=fixture.version("web"), idempotency_id=fixture.h.next_id(),
        scope=m.ScopeDefinition(
            **CONTEXT.model_dump(), scope_id=fixture.h.next_id(), name="Workspace observation scope",
            rules=(m.ScopeRule(
                rule_id=fixture.h.next_id(), selector=selector(fixture), effect="include",
                workloads=("fabric_pipeline",),
            ),),
        ),
    ))
    web.activate_scope(m.ActivateScopeRequest(
        expected=preview.expected, plan_id=preview.plan_id, idempotency_id=preview.idempotency_id,
    ))
    await fixture.drain()


def observed(fixture: PublicationHarness) -> dict[str, bool]:
    """Whether each component currently treats the target as observed."""
    views = {}
    for component in ("controller", "worker"):
        current = fixture.use(component).resolve_target(fixture.identity)
        views[component] = current is not None and current.observation.enabled
    listed = fixture.use("web").list_targets(m.TargetQuery(**CONTEXT.model_dump())).items
    views["web"] = any(
        target.identity == fixture.identity and target.state == "current" and target.observation.enabled
        for target in listed
    )
    return views


ALL = {"controller": True, "worker": True, "web": True}
NONE = {"controller": False, "worker": False, "web": False}


@pytest.fixture
async def observed_target(request, tmp_path):
    """A target admitted by a finished pass, then a second pass records its first page."""
    fixture = PublicationHarness(request.param, tmp_path, register_transport=False)
    first = await start_pass(fixture)
    finished = await finish_pass(fixture, first, None)
    record_probe(fixture, finished.generation_id, hours=3)
    await fixture.drain()
    await activate(fixture)
    assert observed(fixture) == ALL
    fixture.h.clock.advance(60)
    return fixture


@pytest.mark.parametrize("observed_target", ["memory", "sql"], indirect=True)
async def test_targets_stay_observed_while_a_re_inventory_is_collecting(observed_target):
    fixture = observed_target
    second = await start_pass(fixture)
    page = record_page(fixture, second, None, finished=False)

    await fixture.drain()

    assert observed(fixture) == ALL
    await finish_pass(fixture, second, page)
    assert observed(fixture) == ALL


@pytest.mark.parametrize("observed_target", ["memory", "sql"], indirect=True)
async def test_a_pass_with_no_finished_predecessor_for_its_selector_is_not_trusted(observed_target):
    # Only a finished pass of the same selector stands in for a collecting one.
    # The first tenant-wide pass re-stamps the workspace with no such pass, so
    # membership stays unverified until it finishes.
    fixture = observed_target
    tenant = await start_pass(fixture, m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"))
    page = record_page(fixture, tenant, None, finished=False)
    await fixture.drain()

    assert observed(fixture) == NONE
    await finish_pass(fixture, tenant, page)
    assert observed(fixture) == ALL


@pytest.mark.parametrize("observed_target", ["memory", "sql"], indirect=True)
async def test_a_collecting_page_that_cannot_confirm_the_workspace_applies_at_once(observed_target):
    # Only complete inventory can establish deletion, but a page that reports
    # the workspace unknown is not overridden by the previous pass.
    fixture = observed_target
    second = await start_pass(fixture)
    record_page(fixture, second, None, finished=False, workspace_state="unknown")

    await fixture.drain()

    assert observed(fixture) == NONE


@pytest.mark.parametrize("observed_target", ["memory", "sql"], indirect=True)
async def test_a_pass_collecting_beyond_the_grace_period_is_not_trusted(observed_target):
    fixture = observed_target
    second = await start_pass(fixture)
    record_page(fixture, second, None, finished=False)
    await fixture.drain()
    grace = int(INVENTORY_COLLECTION_GRACE.total_seconds())

    fixture.h.clock.advance(grace - 60)
    assert observed(fixture) == ALL
    fixture.h.clock.advance(120)
    assert observed(fixture) == NONE


@pytest.mark.parametrize("observed_target", ["memory", "sql"], indirect=True)
async def test_a_collecting_pass_defers_only_to_the_latest_finished_pass(observed_target):
    # An older complete pass must not outvote a newer one that finished
    # incomplete: membership stays unverified until a pass completes again.
    fixture = observed_target
    second = await start_pass(fixture)
    page = record_page(fixture, second, None, finished=False)
    await finish_pass(fixture, second, page, complete=False)
    assert observed(fixture) == NONE

    fixture.h.clock.advance(60)
    third = await start_pass(fixture)
    page = record_page(fixture, third, None, finished=False)
    await fixture.drain()
    assert observed(fixture) == NONE
    await finish_pass(fixture, third, page)
    assert observed(fixture) == ALL
