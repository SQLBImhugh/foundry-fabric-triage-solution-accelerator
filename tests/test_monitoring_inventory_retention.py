"""Old inventory pass records are retired once nothing reads them.

Every scan writes one pass record, plus one sighting per workspace and item it
saw. The controller reads every pass record for each admission check, against
a 5,000-record budget. In the MorkNet lab 363 had accumulated in eleven days,
and 3 of them still had a reader.

The controller now deletes a pass, its sightings and their accepted-evidence
bindings once the pass is more than seven days old and none of these applies:

- it is the latest, latest finished or latest complete pass of its scan;
- it is the finished pass a scan still collecting defers to;
- a workspace, domain or item record names it;
- its inventory work is still active, or its collection window is still open.

A deleted item names the pass that proved its deletion, but every later
complete scan that covers the item proves it again and takes over, so that
pass is always the latest complete one. Each deleted row adds to a per-kind
offset, so a change counter never returns to an earlier value and an
activation dry run cannot miss a change.

One operation runs per retirement interval; later heartbeats in the interval
replay its stored answer. An operation deletes a bounded number of sightings,
so a large pass is retired over several operations.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta

import pytest
from test_monitoring_controller_publication_integration import PublicationHarness
from test_monitoring_inventory import CONTEXT
from test_monitoring_reinventory_admission import ALL, activate, observed, record_probe, start_pass
from test_monitoring_store import uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringComponentDenied, MonitoringConflict
from triage.monitoring.controller import HeartbeatBudget
from triage.monitoring.engine import MonitoringEngine
from triage.monitoring.sql_kernel_inventory_retention import inventory_retirement_eligible_sql
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.runner import TriageRunner
from triage.settings import Settings

DAY = 86_400
EXTRA = uid(77_001)
OTHER_WORKSPACE = uid(77_002)
SIGHTINGS = {"workspace_seen", "inventory_seen", "domain_seen"}


def tenant_scan() -> m.ScopeSelector:
    return m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant")


def item_scan(fixture: PublicationHarness, item_id: str | None = None) -> m.ScopeSelector:
    return m.ScopeSelector(
        tenant_id=CONTEXT.tenant_id, kind="item",
        workspace_id=fixture.identity.workspace_id, item_id=item_id or fixture.identity.item_id,
    )


def record_scan_page(
    fixture: PublicationHarness, work: m.MonitoringWork, *, items: tuple[str, ...], finished: bool,
    complete: bool = True, workspaces: tuple[str, ...] = (),
) -> m.InventoryGeneration:
    """Record a pass's first page: its workspace entries and the given pipelines."""
    now = fixture.h.clock()
    gaps = () if finished and complete else (
        m.CoverageGap(code="inventory_in_progress", detail="Inventory has a durable continuation"),
    ) if not finished else (m.CoverageGap(code="http_401", detail="A page of the scan could not be read."),)
    generation = m.InventoryGeneration(
        **CONTEXT.model_dump(), generation_id=work.work_id, selector=work.discovery_selector,
        adapter="explicit_offline_inventory", authority="tenant_admin",
        completeness="complete" if finished and complete else "partial", started_at=now,
        completed_at=now if finished else None, continuation=None if finished else "page-2",
        discovered_count=len(items), completed_pages=1, gaps=gaps,
    )
    return fixture.use("worker").record_inventory(m.InventoryBatch(
        request_id=fixture.h.next_id(), expected=fixture.version("worker"), generation=generation,
        workspaces=tuple(m.InventoryWorkspace(
            **CONTEXT.model_dump(), generation_id=work.work_id, workspace_id=workspace_id,
            name=f"Workspace {workspace_id[-4:]}", observed_at=now,
        ) for workspace_id in workspaces or (fixture.identity.workspace_id,)),
        items=tuple(m.InventoryItem(
            **CONTEXT.model_dump(), generation_id=work.work_id, workspace_id=fixture.identity.workspace_id,
            item_id=item_id, name=f"Pipeline {item_id[-4:]}", item_type="DataPipeline",
            workload="fabric_pipeline", observed_at=now,
        ) for item_id in items),
        commit=m.InventoryCommit(
            work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            expected_generation_revision=0, expected_continuation=None,
        ),
    ))


def close(fixture: PublicationHarness, work: m.MonitoringWork, *, retry_days: int | None = None) -> None:
    """Close a pass's work as the polling worker does, or leave it waiting to resume."""
    worker = fixture.use("worker")
    current = worker.get_work(CONTEXT, work.work_id)
    worker.disposition_work(m.WorkDispositionRequest(
        **CONTEXT.model_dump(), request_id=fixture.h.next_id(), work_id=current.work_id,
        expected_work_revision=current.revision, lease=current.lease,
        disposition="retry" if retry_days else "superseded",
        retry_at=fixture.h.clock() + timedelta(days=retry_days) if retry_days else None,
        detail="Retention fixture pass",
    ))


async def scan(
    fixture: PublicationHarness, *, chosen: m.ScopeSelector | None = None,
    items: tuple[str, ...] | None = None, finished: bool = True, complete: bool = True,
    retry_days: int | None = None, workspaces: tuple[str, ...] = (),
) -> str:
    work = await start_pass(fixture, chosen)
    record_scan_page(
        fixture, work, items=items or (fixture.identity.item_id,), finished=finished, complete=complete,
        workspaces=workspaces,
    )
    close(fixture, work, retry_days=retry_days)
    await fixture.drain()
    return work.work_id


def retire(fixture: PublicationHarness, *, limit: int = 25, component: str = "controller"):
    request = m.InventoryRetirementRequest(**CONTEXT.model_dump(), request_id=fixture.h.next_id(), limit=limit)
    return request, fixture.use(component).retire_inventory_history(request)


def pass_rows(fixture: PublicationHarness, generation_id: str) -> set[str]:
    """Record kinds still stored for one pass: its record and its sightings."""
    return {
        row.kind for row in fixture.rows()
        if (row.kind == "generation" and row.key == generation_id)
        or (row.kind in SIGHTINGS and row.parent_key == generation_id)
    }


def bindings(fixture: PublicationHarness, generation_id: str) -> list[dict]:
    """Accepted-evidence bindings for one pass's record and sightings (SQL only)."""
    values = [json.loads(row.payload) for row in fixture.rows() if row.kind == "accepted_fact"]
    return [
        value for value in values
        if (value["fact_kind"] == "generation" and value["fact_key"] == generation_id)
        or (value["fact_kind"] in SIGHTINGS and value["fact_key"].startswith(f"{generation_id}:"))
    ]


def inventory_revision(fixture: PublicationHarness) -> int:
    web = fixture.use("web")
    plan = web.preview_scope(m.ScopePreviewRequest(
        expected=fixture.version("web"), idempotency_id=fixture.h.next_id(),
        scope=m.ScopeDefinition(
            **CONTEXT.model_dump(), scope_id=fixture.h.next_id(), name="Revision probe",
            rules=(m.ScopeRule(rule_id=fixture.h.next_id(), selector=tenant_scan(), effect="include"),),
        ),
    ))
    return plan.inventory_revision


def coverage(fixture: PublicationHarness, component: str) -> dict:
    view = fixture.use(component).coverage(CONTEXT)
    return {
        "completeness": view.inventory_completeness, "completed": view.last_inventory_completed_at,
        "discovered": view.discovered_count, "current": view.current_count,
        "gaps": sorted(gap.code for gap in view.gaps),
    }


@pytest.fixture
async def history(request, tmp_path):
    """Eight days of scans. Every old pass except A, A2 and B still has a reader.

    A, A2, B: old workspace scans nothing reads.            -> retired
    T: an old tenant scan; only the entry for a workspace
       the newer tenant scan T2 no longer lists names it.   -> catalogue record
    R: an old workspace scan whose work is still waiting.   -> active work
    W: an abandoned scan whose window never closed.         -> open window
    P: an old item scan that X, still collecting, defers to.
    Z: the only scan of the item B then found deleted.      -> latest of its scan
    C2 and C8: workspace scans inside the seven days; T2, X and F: today's scans.
    """
    fixture = PublicationHarness(request.param, tmp_path, register_transport=False)
    item, workspace = fixture.identity.item_id, fixture.identity.workspace_id
    passes = {"A": await scan(fixture, items=(item, EXTRA))}
    record_probe(fixture, passes["A"], hours=24 * 60)
    await fixture.drain()
    await activate(fixture)
    for name, options in (
        ("T", {"chosen": tenant_scan(), "items": (item, EXTRA), "workspaces": (workspace, OTHER_WORKSPACE)}),
        ("A2", {"items": (item, EXTRA)}),
        ("R", {"items": (item, EXTRA), "retry_days": 60}),
        ("W", {"finished": False}),
        ("P", {"chosen": item_scan(fixture)}),
        ("Z", {"chosen": item_scan(fixture, EXTRA), "items": (EXTRA,)}),
    ):
        fixture.h.clock.advance(600)
        passes[name] = await scan(fixture, **options)
    fixture.h.clock.advance(DAY - 3_600)
    passes["B"] = await scan(fixture)
    for day, advance in ((2, DAY), (8, 6 * DAY)):
        fixture.h.clock.advance(advance)
        passes[f"C{day}"] = await scan(fixture)
    for name, options in (
        ("T2", {"chosen": tenant_scan()}),
        ("X", {"chosen": item_scan(fixture), "finished": False, "retry_days": 60}),
        ("F", {"chosen": item_scan(fixture)}),
    ):
        fixture.h.clock.advance(600)
        passes[name] = await scan(fixture, **options)
    fixture.h.clock.advance(600)
    assert observed(fixture) == ALL
    return fixture, passes


async def activate_item_scope(fixture: PublicationHarness) -> None:
    web = fixture.use("web")
    preview = web.preview_scope(m.ScopePreviewRequest(
        expected=fixture.version("web"), idempotency_id=fixture.h.next_id(),
        scope=m.ScopeDefinition(
            **CONTEXT.model_dump(), scope_id=fixture.h.next_id(), name="Item observation scope",
            rules=(m.ScopeRule(
                rule_id=fixture.h.next_id(), selector=item_scan(fixture), effect="include",
                workloads=("fabric_pipeline",),
            ),),
        ),
    ))
    web.activate_scope(m.ActivateScopeRequest(
        expected=preview.expected, plan_id=preview.plan_id, idempotency_id=preview.idempotency_id,
    ))
    await fixture.drain()


def catalogue_passes(fixture: PublicationHarness) -> dict[tuple[str, str], str]:
    return {
        (row.kind, row.key): row.generation_id for row in fixture.rows()
        if row.kind in {"inventory", "workspace", "domain"}
    }


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_old_passes_nothing_reads_are_retired_with_their_sightings(history):
    fixture, passes = history
    before = {component: coverage(fixture, component) for component in ("controller", "web")}
    revision = inventory_revision(fixture)
    named = catalogue_passes(fixture)

    _, result = retire(fixture)

    retired = {passes["A"], passes["A2"], passes["B"]}
    assert set(result.retired_generation_ids) == retired
    assert result.refused_generation_ids == result.deferred_generation_ids == ()
    assert result.retired_sightings == 8
    for generation_id in retired:
        assert pass_rows(fixture, generation_id) == set()
        assert fixture.use("controller").get_inventory_generation(CONTEXT, generation_id) is None
        with pytest.raises(MonitoringConflict, match="does not exist"):
            fixture.use("controller").list_workspaces(m.PageQuery(**CONTEXT.model_dump()), generation_id=generation_id)
    for name in ("T", "R", "W", "P", "Z", "C2", "C8", "T2", "X", "F"):
        assert pass_rows(fixture, passes[name]) == {"generation", "workspace_seen", "inventory_seen"}, name
    # Every catalogue record still resolves the pass that last saw it, including
    # the deleted item, whose deletion the newest complete tenant scan proved.
    assert catalogue_passes(fixture) == named
    assert named[("workspace", OTHER_WORKSPACE)] == passes["T"]
    assert named[("inventory", f"{fixture.identity.workspace_id}:{EXTRA}")] == passes["T2"]
    for generation_id in set(named.values()):
        assert fixture.use("controller").get_inventory_generation(CONTEXT, generation_id) is not None
    assert observed(fixture) == ALL
    assert {component: coverage(fixture, component) for component in ("controller", "web")} == before
    # A deleted row must still count as a change, never lower the counter.
    assert inventory_revision(fixture) > revision
    if fixture.db:
        assert result.retired_bindings > 0
        assert all(bindings(fixture, generation_id) == [] for generation_id in retired)
        assert bindings(fixture, passes["T"])
        # The per-page acceptance record is kept as the pass's summary.
        assert any(row.kind == "intake_disposition" and row.generation_id == passes["A"] for row in fixture.rows())
    else:
        assert result.retired_bindings == 0


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_the_heartbeat_step_reports_what_it_retired(history, caplog):
    fixture, passes = history
    runner = TriageRunner.__new__(TriageRunner)
    runner.fixture = False
    runner._monitoring_store = fixture.use("controller")
    runner.settings = Settings(_env_file=None, monitoring_mode="live", monitoring_tenant_id=CONTEXT.tenant_id)

    with caplog.at_level("INFO", logger="triage.telemetry.heartbeat"):
        first = await runner.retire_monitoring_history()
        again = await runner.retire_monitoring_history()

    assert len(first) == 1 and "retired 3 inventory passes older than 7 days" in first[0]
    assert again == []
    assert "inventory_retention retired=3 refused=0 deferred=0 sightings=8" in caplog.text
    assert pass_rows(fixture, passes["A"]) == set()


async def test_the_heartbeat_step_rechecks_its_deadline_before_the_durable_write():
    """The context read can end after the admission deadline; retirement must not start then."""
    now = [0.0]
    started = []

    class Store:
        def retire_inventory_history(self, request):
            started.append(request)
            raise AssertionError("Retirement started after the admission deadline")

    class Runner(TriageRunner):
        @property
        def monitoring_context(self):
            now[0] = 100.0
            return CONTEXT

    runner = Runner.__new__(Runner)
    runner.fixture = False
    runner._monitoring_store = Store()
    budget = HeartbeatBudget(deadline=50.0, work_seconds=10.0, clock=lambda: now[0])

    assert await runner.retire_monitoring_history(budget) == []
    assert started == []


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_each_interval_runs_one_operation_and_later_calls_replay_it(history):
    """Heartbeats in one interval share a request ID, so only the first one deletes.

    A request ID per heartbeat lost its identity when a commit was uncertain,
    and an unrecorded no-op let the same ID delete later. Now the answer of
    every completed operation is stored, including one that retired nothing.
    """
    fixture, passes = history
    controller = fixture.use("controller")
    now = fixture.h.clock()
    request = m.InventoryRetirementRequest.for_interval(CONTEXT, now)
    assert request == m.InventoryRetirementRequest.for_interval(
        CONTEXT, now - (now - datetime.fromtimestamp(0, UTC)) % m.INVENTORY_RETIREMENT_INTERVAL,
    )

    first = controller.retire_inventory_history(request)
    replay = controller.retire_inventory_history(request)

    assert not first.replayed and replay.replayed
    assert replay.model_copy(update={"replayed": False}) == first
    assert set(first.retired_generation_ids) == {passes["A"], passes["A2"], passes["B"]}

    fixture.h.clock.advance(int(m.INVENTORY_RETIREMENT_INTERVAL.total_seconds()))
    later = m.InventoryRetirementRequest.for_interval(CONTEXT, fixture.h.clock())
    assert later.request_id != request.request_id
    empty = controller.retire_inventory_history(later)
    fixture.h.clock.advance(60)
    stored = controller.retire_inventory_history(later)

    assert empty.retired_generation_ids == empty.refused_generation_ids == empty.deferred_generation_ids == ()
    assert not empty.replayed and stored.replayed
    # The stored answer, not a new operation: its retention boundary did not move.
    assert stored.retain_after == empty.retain_after


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_retirement_takes_the_oldest_passes_first_and_then_nothing(history):
    fixture, passes = history

    _, first = retire(fixture, limit=1)
    _, second = retire(fixture)
    _, third = retire(fixture)

    assert first.retired_generation_ids == (passes["A"],)
    assert set(second.retired_generation_ids) == {passes["A2"], passes["B"]}
    assert third.retired_generation_ids == third.refused_generation_ids == third.deferred_generation_ids == ()
    assert third.retired_sightings == third.retired_bindings == 0


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_a_large_pass_loses_its_sightings_over_several_operations(history, monkeypatch):
    """One operation deletes a bounded number of sightings; the pass record goes with the last.

    A tenant pass can have a sighting for every workspace and item in the
    tenant, and the control lock is held while they are deleted.
    """
    fixture, passes = history
    monkeypatch.setattr(m, "INVENTORY_RETIREMENT_SIGHTINGS", 4)
    before = {component: coverage(fixture, component) for component in ("controller", "web")}
    named = catalogue_passes(fixture)
    revision = inventory_revision(fixture)

    _, first = retire(fixture)

    # A has three sightings and goes whole; A2 loses one of its three; B waits.
    assert first.retired_generation_ids == (passes["A"],)
    assert set(first.deferred_generation_ids) == {passes["A2"], passes["B"]}
    assert first.refused_generation_ids == () and first.retired_sightings == 4
    assert pass_rows(fixture, passes["A"]) == set()
    remaining = [
        row.kind for row in fixture.rows() if row.kind in SIGHTINGS and row.parent_key == passes["A2"]
    ]
    assert len(remaining) == 2
    assert fixture.use("controller").get_inventory_generation(CONTEXT, passes["A2"]) is not None
    assert pass_rows(fixture, passes["B"]) == {"generation", "workspace_seen", "inventory_seen"}
    assert {component: coverage(fixture, component) for component in ("controller", "web")} == before
    assert catalogue_passes(fixture) == named and observed(fixture) == ALL
    assert inventory_revision(fixture) > revision

    _, second = retire(fixture)
    _, third = retire(fixture)

    assert set(second.retired_generation_ids) == {passes["A2"], passes["B"]}
    assert second.deferred_generation_ids == () and second.retired_sightings == 4
    assert third.retired_generation_ids == third.deferred_generation_ids == ()
    assert pass_rows(fixture, passes["A2"]) == pass_rows(fixture, passes["B"]) == set()
    assert {component: coverage(fixture, component) for component in ("controller", "web")} == before
    if fixture.db:
        assert all(bindings(fixture, passes[name]) == [] for name in ("A", "A2", "B"))


@pytest.mark.parametrize("adapter", ["memory", "sql"])
@pytest.mark.parametrize("complete_first", [True, False])
@pytest.mark.parametrize("reverse_reads", [False, True])
async def test_passes_that_start_together_are_ordered_by_pass_id_everywhere(
    adapter, complete_first, reverse_reads, tmp_path, monkeypatch,
):
    """Coverage, scope previews and retention name the same latest pass on a tie.

    With equal start times, backend order chose the latest pass for coverage,
    so coverage could read an older partial pass that retention then deleted
    as superseded, and coverage turned complete without new evidence. Reading
    the passes in both orders shows that backend order no longer decides.
    """
    if reverse_reads:
        read = MonitoringEngine._all

        def reversed_passes(self, kind, *args, **kwargs):
            rows = read(self, kind, *args, **kwargs)
            return rows[::-1] if kind == "generation" else rows
        monkeypatch.setattr(MonitoringEngine, "_all", reversed_passes)
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    tie = {}
    for complete in (complete_first, not complete_first):
        tie[complete] = await scan(fixture, complete=complete)
    controller = fixture.use("controller")
    started = {controller.get_inventory_generation(CONTEXT, value).started_at for value in tie.values()}
    assert len(started) == 1
    later = max(tie.values())
    fixture.h.clock.advance(8 * DAY)
    expected = "complete" if later == tie[True] else "partial"
    before = {component: coverage(fixture, component) for component in ("controller", "web")}
    assert {view["completeness"] for view in before.values()} == {expected}
    preview = fixture.use("web").preview_scope(m.ScopePreviewRequest(
        expected=fixture.version("web"), idempotency_id=fixture.h.next_id(),
        scope=m.ScopeDefinition(
            **CONTEXT.model_dump(), scope_id=fixture.h.next_id(), name="Tie probe",
            rules=(m.ScopeRule(rule_id=fixture.h.next_id(), selector=tenant_scan(), effect="include"),),
        ),
    ))
    assert later in preview.inventory_generations
    assert min(tie.values()) not in preview.inventory_generations

    _, result = retire(fixture)

    # Only an earlier partial pass is superseded; an earlier complete one stays
    # the latest complete pass of its scan.
    assert result.retired_generation_ids == ((min(tie.values()),) if later == tie[True] else ())
    assert {component: coverage(fixture, component) for component in ("controller", "web")} == before


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_a_repeated_request_returns_its_original_result(history):
    fixture, passes = history
    request, first = retire(fixture)

    replay = fixture.use("controller").retire_inventory_history(request)
    assert replay.replayed and replay.model_copy(update={"replayed": False}) == first
    changed = request.model_copy(update={"limit": 3})
    with pytest.raises(MonitoringConflict):
        fixture.use("controller").retire_inventory_history(changed)


@pytest.mark.parametrize("adapter", ["memory", "sql"])
@pytest.mark.parametrize("component", ["web", "worker"])
def test_only_the_controller_retires_inventory_history(adapter, component, tmp_path):
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)

    with pytest.raises(MonitoringComponentDenied):
        retire(fixture, component=component)


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_a_proposed_pass_that_is_still_needed_is_refused_not_deleted(history, monkeypatch):
    fixture, passes = history
    needed = [passes[name] for name in ("T", "R", "W", "P", "Z", "C2")]
    monkeypatch.setattr(
        MonitoringEngine, "_inventory_retirement_candidates",
        lambda self, control, *, now, limit: [passes["A"], *needed],
    )

    _, result = retire(fixture)

    assert result.retired_generation_ids == (passes["A"],)
    assert set(result.refused_generation_ids) == set(needed)
    for generation_id in needed:
        assert pass_rows(fixture, generation_id) == {"generation", "workspace_seen", "inventory_seen"}


@pytest.mark.parametrize("adapter", ["memory", "sql"])
async def test_each_scan_keeps_its_latest_latest_finished_and_latest_complete_pass(adapter, tmp_path, monkeypatch):
    """Each pass here is kept by exactly one of the three rules, even when proposed.

    L: the only workspace scan; unfinished, and its window was rejected when a
       scope activation changed policy.                -> latest of its scan
    O1: finished incomplete; O2, newer, still collecting. -> latest finished
    K1: complete; K2, newer, finished incomplete.        -> latest complete
    M: a later tenant scan that sees every item and the workspace again.
    """
    fixture = PublicationHarness(adapter, tmp_path, register_transport=False)
    item = fixture.identity.item_id
    await scan(fixture, chosen=tenant_scan(), items=(item, EXTRA))
    fixture.h.clock.advance(60)
    passes = {"L": await scan(fixture, finished=False)}
    # A scope change while L's window is open rejects the whole window.
    await activate_item_scope(fixture)
    fixture.h.clock.advance(16)
    await fixture.drain()
    for name, options in (
        ("O1", {"chosen": item_scan(fixture), "complete": False}),
        ("O2", {"chosen": item_scan(fixture), "finished": False, "retry_days": 60}),
        ("K1", {"chosen": item_scan(fixture, EXTRA), "items": (EXTRA,)}),
        ("K2", {"chosen": item_scan(fixture, EXTRA), "items": (EXTRA,), "complete": False}),
        ("M", {"chosen": tenant_scan(), "items": (item, EXTRA)}),
    ):
        fixture.h.clock.advance(60)
        passes[name] = await scan(fixture, **options)
    fixture.h.clock.advance(8 * DAY)
    kept = [passes[name] for name in ("L", "O1", "K1")]
    monkeypatch.setattr(
        MonitoringEngine, "_inventory_retirement_candidates", lambda self, control, *, now, limit: kept,
    )

    _, result = retire(fixture)

    assert result.retired_generation_ids == ()
    assert set(result.refused_generation_ids) == set(kept)


@pytest.mark.parametrize("history", ["memory", "sql"], indirect=True)
async def test_maintenance_refuses_retirement_without_recording_it(history):
    fixture, passes = history
    control = fixture.use("controller").snapshot(CONTEXT).control

    def set_control(value: m.DeploymentControl) -> None:
        if fixture.db:
            fixture.db.control = value
        else:
            fixture.h.state.control_row = value.model_dump(mode="json")

    set_control(control.model_copy(update={"maintenance": True}))
    request, refused = retire(fixture)

    assert refused.status == "maintenance" and not refused.replayed
    assert refused.retired_generation_ids == refused.refused_generation_ids == ()
    assert pass_rows(fixture, passes["A"]) == {"generation", "workspace_seen", "inventory_seen"}

    # The refusal was not recorded, so the same request runs after maintenance.
    set_control(control)
    result = fixture.use("controller").retire_inventory_history(request)

    assert result.status == "completed" and not result.replayed
    assert set(result.retired_generation_ids) == {passes["A"], passes["A2"], passes["B"]}


def test_the_sql_kernel_lets_only_the_controller_retire_inventory_history():
    kernel = build_permission_kernel()
    contract = kernel.rpcs["controller.retire_inventory"]
    ddl = next(obj.ddl for obj in kernel.objects if obj.logical_name == "controller.retire_inventory")
    views = {obj.logical_name: obj.ddl for obj in kernel.objects if obj.kind == "view"}

    assert contract.components == ("controller",) and contract.mutating
    assert [grant for grants in kernel.grants.values() for grant in grants if contract.object_name in grant] == [
        f"GRANT EXECUTE ON OBJECT::{contract.object_name} TO [{kernel.names.role('controller')}];",
    ]
    # The retention window and collection grace are fixed in the kernel, not chosen by a caller.
    assert "DATEADD(day,-7,@now)" in ddl and "DATEADD(second,-3600,@now)" in ddl
    assert f"SELECT TOP ({m.INVENTORY_RETIREMENT_SIGHTINGS}) s.record_kind" in ddl
    assert inventory_retirement_eligible_sql(
        kernel.names, passes="@passes", generation="c.generation_id", key_hash="c.key_hash",
    ) in ddl
    # VARCHAR columns compared with N'...' literals lose their index seeks.
    assert not re.search(r"record_kind\s*(=|IN \()\s*N'", ddl)
    assert not re.search(r"status IN \(N'", ddl)
    deleted = set(re.findall(r"DELETE (\w+) OUTPUT", ddl))
    assert deleted == {"b", "d"}
    assert "record_kind='accepted_fact'" in ddl
    assert "record_kind IN ('workspace_seen', 'inventory_seen', 'domain_seen')" in ddl
    # Every component reads the retirement offsets; none can write them through a view.
    for name in ("web_read", "worker_read", "controller_read"):
        assert "N'record_retirement'" in views[name], name
    for name in (
        "worker_catalogue", "worker_evidence", "worker_telemetry", "web_drafts",
        "controller_projections", "controller_immutable",
    ):
        assert "record_retirement" not in views[name], name
