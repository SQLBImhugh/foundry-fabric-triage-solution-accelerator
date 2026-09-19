from __future__ import annotations

from uuid import UUID

import pytest
from test_monitoring_provisioning import (
    CONNECTOR,
    CONTEXT,
    OWNER,
    connector,
    factory,
    setup_store,
    target,
    version,
)
from test_monitoring_provisioning_admission import capability
from test_monitoring_provisioning_orchestration import (
    drain_controller,
    runner_for,
    seed_existing_publication,
)
from test_monitoring_sql_store import SqlHarness
from test_monitoring_store import Harness

from triage.monitoring import models as m
from triage.monitoring.memory import InMemoryMonitoringStore, inventory_confirms_deletion, stable_id
from triage.monitoring.provisioning import (
    plan_definition,
    prepare_connector_publication,
    publication_from_plan,
)

__all__ = ["factory"]

DOMAIN = str(UUID(int=170_001))
CHILD = str(UUID(int=170_002))


def record_hierarchy(
    seed, clock, *, unknown=None, complete=True, empty=False, include_workspace=True, identity=None,
    writer=None,
):
    identity = identity or target()
    clock.advance(1)
    generation_id = stable_id(CONTEXT, f"domain-fixture:{clock().isoformat()}")
    return (writer or seed.record_inventory)(m.InventoryBatch(
        request_id=stable_id(CONTEXT, f"domain-fixture-request:{generation_id}"),
        expected=version(seed), items=(),
        generation=m.InventoryGeneration(
            **CONTEXT.model_dump(), generation_id=generation_id,
            selector=m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
            enumeration="domains", adapter="explicit_domain_fixture", authority="tenant_admin",
            completeness="complete" if complete else "partial",
            started_at=clock(), completed_at=clock(), completed_pages=1,
            discovered_count=0 if empty else 2 if unknown not in {"domain", "ancestor"} else 1,
            gaps=() if complete else (
                m.CoverageGap(code="metadata_unavailable", detail="Domain membership is incomplete"),
            ),
        ),
        workspaces=(m.InventoryWorkspace(
            **CONTEXT.model_dump(), generation_id=generation_id,
            workspace_id=identity.workspace_id, domain_id=CHILD,
            name="Fixture workspace", observed_at=clock(),
            state="unknown" if unknown == "workspace" else "present",
        ),) if include_workspace and not empty else (),
        domains=tuple(m.InventoryDomain(
            **CONTEXT.model_dump(), generation_id=generation_id, domain_id=domain_id,
            parent_domain_id=DOMAIN if domain_id == CHILD else None,
            name="Fixture domain", observed_at=clock(),
            state="unknown" if (
                unknown == "domain" and domain_id == DOMAIN
                or unknown == "ancestor" and domain_id == CHILD
            ) else "present",
        ) for domain_id in (() if empty else (DOMAIN, CHILD))),
    ))


async def domain_case(factory, tmp_path):
    seed, state, clock, remote = setup_store()
    seed_existing_publication(seed, clock)
    record_hierarchy(seed, clock)
    policy, = seed.list_scopes(m.PageQuery(**CONTEXT.model_dump())).items
    scope = m.ScopeDefinition.model_validate(policy.model_dump(exclude={"revision", "updated_at"}))
    scope = scope.model_copy(update={"rules": (
        scope.rules[0].model_copy(update={"selector": m.ScopeSelector(
            tenant_id=CONTEXT.tenant_id, kind="domain", domain_id=DOMAIN, include_descendants=True,
        )}),
    )})
    plan = seed.preview_scope(m.ScopePreviewRequest(
        expected=version(seed), idempotency_id=stable_id(CONTEXT, "domain-scope"), scope=scope,
    ))
    assert plan.status == "ready"
    seed.activate_scope(m.ActivateScopeRequest(
        expected=plan.expected, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    clock.advance(1)
    capability(seed, clock, seconds=3600)
    web = InMemoryMonitoringStore(state=state, clock=clock, component="web")
    controller = InMemoryMonitoringStore(state=state, clock=clock, component="controller")
    web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=stable_id(CONTEXT, "domain-initial-reconcile"),
    )
    await drain_controller(runner_for(controller, tmp_path), controller)
    await factory(seed, clock, remote).run_once()
    await drain_controller(runner_for(controller, tmp_path), controller)
    assert controller.resolve_target(target()).observation.enabled
    assert connector(seed).policy_revision == version(seed).revision
    assert connector(seed).source_removals == ()
    return seed, controller, web, clock, remote


def planning_work(seed, controller, web, *, suffix):
    queued = web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=stable_id(CONTEXT, f"domain-planning:{suffix}"),
    )
    return next(work for work in controller.claim_work(m.WorkClaimRequest(
        **CONTEXT.model_dump(), owner_id=OWNER, kinds=("reconcile_state",),
        limit=100, per_workspace_limit=100,
    )) if work.work_id == queued.work_id)


@pytest.mark.parametrize("unknown", ["domain", "ancestor", "workspace", "partial", "empty_partial"])
async def test_unknown_hierarchy_fences_admission_without_removing_owned_source(
    factory, tmp_path, unknown,
):
    seed, controller, web, clock, remote = await domain_case(factory, tmp_path)
    prior = seed.resolve_target(target(), include_inactive=True)
    original = connector(seed)
    original_version = version(seed)
    updates = len(remote.update_bodies)
    record_hierarchy(seed, clock, unknown=unknown, complete=False, empty=unknown == "empty_partial")
    clock.advance(1)
    capability(seed, clock, seconds=3600)
    paused = seed.resolve_target(target(), include_inactive=True)
    assert paused.state == "paused"
    assert paused.scope_ids == prior.scope_ids and paused.admitted_rule_ids == prior.admitted_rule_ids
    assert paused.admission_basis == prior.admission_basis
    assert not paused.observation.enabled and not paused.action.enabled
    web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=stable_id(CONTEXT, "domain-unknown-reconcile"),
    )
    await drain_controller(runner_for(controller, tmp_path), controller)
    assert connector(seed).desired_definition == original.desired_definition
    assert connector(seed).sources == original.sources and connector(seed).source_removals == ()
    assert version(seed) == original_version and len(remote.update_bodies) == updates
    record_hierarchy(seed, clock)
    clock.advance(1)
    capability(seed, clock, seconds=3600)
    web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=stable_id(CONTEXT, "domain-recovered-reconcile"),
    )
    await drain_controller(runner_for(controller, tmp_path), controller)
    current = controller.resolve_target(target())
    assert current is not None and current.observation.enabled and not current.action.enabled
    assert current.scope_ids == prior.scope_ids and current.admitted_rule_ids == prior.admitted_rule_ids
    assert connector(seed).desired_definition == original.desired_definition
    assert connector(seed).source_removals == ()
    assert version(seed) == original_version and len(remote.update_bodies) == updates


@pytest.mark.parametrize("pending", [False, True])
async def test_recorded_removed_projection_is_not_authority_under_unknown_domain(
    factory, tmp_path, pending,
):
    seed, controller, web, clock, remote = await domain_case(factory, tmp_path)
    original = connector(seed)
    work = planning_work(seed, controller, web, suffix="poisoned-projection")
    if pending:
        producer = controller.get_reconciliation_request(CONTEXT, work.reconcile_request_id, producer="web")
        frontier = controller.get_validation_frontier(CONTEXT, producer.frontier_key)
        plan = plan_definition(original, original.desired_definition, (), removal_targets=(target(),))
        controller.publish_connector(publication_from_plan(
            version(seed), work, frontier, original, plan,
            request_id=stable_id(CONTEXT, "historical-domain-removal"),
        ))
    record_hierarchy(seed, clock, unknown="domain", complete=False)
    recorded = seed.resolve_target(target(), include_inactive=True)
    current_version = version(seed)
    # Model the previously published false tombstone; its reason is not authority.
    with seed._backend.transaction(write=True, operation="fixture_poisoned_target", request_id=None):
        seed._save_target(m.MonitoringTarget.model_validate({
            **recorded.model_dump(), "state": "removed", "policy_revision": current_version.revision,
            "reason": "Explicit exclusion, completed removal or no current inclusion.",
            "observation": m.ObservationPolicy(), "action": m.ActionPolicy(),
        }))
    updates = len(remote.update_bodies)
    if pending:
        before = connector(seed)
        result = await factory(seed, clock, remote).run_once()
        assert result.results[0].code == "pending_source_removal_authority_unverified"
        assert connector(seed) == before
        assert len(remote.update_bodies) == updates
    else:
        request = prepare_connector_publication(
            controller, work, CONNECTOR, request_id=stable_id(CONTEXT, "hold-poisoned-domain"),
        )
        assert request.desired_definition == original.desired_definition
        assert request.source_removals == () and request.sources == original.sources
        assert len(remote.update_bodies) == updates


def scope_definition(seed):
    policy, = seed.list_scopes(m.PageQuery(**CONTEXT.model_dump())).items
    return m.ScopeDefinition.model_validate(policy.model_dump(exclude={"revision", "updated_at"}))


def activate_scope(store, seed, scope, suffix):
    plan = store.preview_scope(m.ScopePreviewRequest(
        expected=version(seed), idempotency_id=stable_id(CONTEXT, f"domain-activation:{suffix}"),
        scope=scope,
    ))
    assert plan.status == "ready"
    return store.activate_scope(m.ActivateScopeRequest(
        expected=plan.expected, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))


@pytest.mark.parametrize("revocation", ["disabled_domain", "direct_exclusion"])
async def test_explicit_policy_revocation_survives_incomplete_hierarchy(factory, tmp_path, revocation):
    seed, controller, web, clock, remote = await domain_case(factory, tmp_path)
    scope = scope_definition(seed)
    if revocation == "disabled_domain":
        record_hierarchy(seed, clock, unknown="domain", complete=False, empty=True)
        scope = scope.model_copy(update={"enabled": False})
    else:
        scope = scope.model_copy(update={"rules": (*scope.rules, m.ScopeRule(
            rule_id=stable_id(CONTEXT, "direct-exclusion"), effect="exclude",
            selector=m.ScopeSelector(
                tenant_id=CONTEXT.tenant_id, kind="item",
                workspace_id=target().workspace_id, item_id=target().item_id,
            ),
        ))})
    activate_scope(web, seed, scope, revocation)
    if revocation == "direct_exclusion":
        record_hierarchy(seed, clock, unknown="domain", complete=False)
        clock.advance(1)
        capability(seed, clock, seconds=3600)
        assert seed.resolve_target(target(), include_inactive=True).state == "removed"
    await drain_controller(runner_for(controller, tmp_path), controller)
    pending = connector(seed)
    assert len(pending.source_removals) == 1 and pending.sources
    assert pending.desired_definition["parts"]["eventstream.json"]["sources"] == []
    updates = len(remote.update_bodies)
    result = await factory(seed, clock, remote).run_once()
    assert result.results[0].code == "controller_source_retirement_required"
    assert len(remote.update_bodies) == updates + 1 and remote.graph["sources"] == []


async def test_disabled_domain_does_not_remove_overlapping_active_admission(factory, tmp_path):
    seed, controller, web, clock, remote = await domain_case(factory, tmp_path)
    original_scope = scope_definition(seed)
    overlap = m.ScopeDefinition(
        **CONTEXT.model_dump(), scope_id=stable_id(CONTEXT, "overlap"), name="Overlapping direct scope",
        rules=(m.ScopeRule(
            rule_id=stable_id(CONTEXT, "overlap-rule"), effect="include",
            selector=m.ScopeSelector(
                tenant_id=CONTEXT.tenant_id, kind="workspace", workspace_id=target().workspace_id,
            ),
        ),),
    )
    activate_scope(web, seed, overlap, "overlap")
    await drain_controller(runner_for(controller, tmp_path), controller)
    original = connector(seed)
    record_hierarchy(seed, clock, unknown="domain", complete=False, include_workspace=False)
    activate_scope(web, seed, original_scope.model_copy(update={"enabled": False}), "disable-overlap")
    await drain_controller(runner_for(controller, tmp_path), controller)
    current = controller.resolve_target(target())
    assert current is not None and current.observation.enabled
    assert current.scope_ids == (overlap.scope_id,)
    assert current.admitted_rule_ids == (overlap.rules[0].rule_id,)
    assert not current.action.enabled
    assert connector(seed).desired_definition == original.desired_definition
    assert connector(seed).source_removals == ()
    assert await factory(seed, clock, remote)._removal_hold(connector(seed)) is None


def record_absence(seed, clock, scope_kind):
    clock.advance(1)
    generation_id = stable_id(CONTEXT, f"absence:{clock().isoformat()}:{scope_kind}")
    selector = m.ScopeSelector(
        tenant_id=CONTEXT.tenant_id, kind=scope_kind,
        workspace_id=target().workspace_id if scope_kind in {"workspace", "item"} else None,
        item_id=target().item_id if scope_kind == "item" else None,
        domain_id=DOMAIN if scope_kind == "domain" else None,
        include_descendants=scope_kind == "domain",
    )
    return seed.record_inventory(m.InventoryBatch(
        request_id=stable_id(CONTEXT, f"absence-request:{generation_id}"),
        expected=version(seed), items=(),
        generation=m.InventoryGeneration(
            **CONTEXT.model_dump(), generation_id=generation_id, selector=selector,
            adapter="complete_absence_fixture", authority="fixture", enumeration="items",
            completeness="complete", started_at=clock(), completed_at=clock(),
            discovered_count=0, completed_pages=1,
        ),
    ))


@pytest.mark.parametrize("scope_kind", ["tenant", "workspace", "item", "domain"])
async def test_only_complete_physical_item_absence_authorizes_deletion(factory, tmp_path, scope_kind):
    seed, controller, web, clock, remote = await domain_case(factory, tmp_path)
    original = connector(seed)
    updates = len(remote.update_bodies)
    record_absence(seed, clock, scope_kind)
    item, = seed.list_inventory(m.TargetQuery(**CONTEXT.model_dump(), include_inactive=True)).items
    assert item.state == "deleted"
    web.request_discovery(
        version(seed), m.ScopeSelector(tenant_id=CONTEXT.tenant_id, kind="tenant"),
        request_id=stable_id(CONTEXT, f"confirmed-absence:{scope_kind}"),
    )
    await drain_controller(runner_for(controller, tmp_path), controller)
    if scope_kind == "domain":
        assert seed.resolve_target(target(), include_inactive=True).state == "removed"
        assert connector(seed).source_removals == ()
        assert connector(seed).desired_definition == original.desired_definition
        assert len(remote.update_bodies) == updates
    else:
        assert seed.resolve_target(target(), include_inactive=True).state == "removed"
        assert len(connector(seed).source_removals) == 1
        result = await factory(seed, clock, remote).run_once()
        assert result.results[0].code == "controller_source_retirement_required"
        assert len(remote.update_bodies) == updates + 1 and remote.graph["sources"] == []


@pytest.mark.parametrize("gap", [
    "missing", "partial", "metadata_only", "other_workspace", "old_generation", "domain_membership",
])
def test_deletion_evidence_requires_exact_complete_item_generation(gap):
    seed, _, clock, _ = setup_store()
    original, = seed.list_inventory(m.TargetQuery(**CONTEXT.model_dump())).items
    generation = record_absence(seed, clock, "workspace")
    deleted = original.model_copy(update={
        "generation_id": generation.generation_id, "state": "deleted", "observed_at": clock(),
    })
    assert inventory_confirms_deletion(target(), deleted, generation)
    if gap == "missing":
        generation = None
    elif gap == "partial":
        generation = generation.model_copy(update={
            "completeness": "partial",
            "gaps": (m.CoverageGap(code="incomplete", detail="Deletion remains unproven"),),
        })
    elif gap == "metadata_only":
        generation = generation.model_copy(update={"enumeration": "workspaces"})
    elif gap == "other_workspace":
        generation = generation.model_copy(update={"selector": m.ScopeSelector(
            tenant_id=CONTEXT.tenant_id, kind="workspace", workspace_id=str(UUID(int=170_099)),
        )})
    elif gap == "old_generation":
        generation = generation.model_copy(update={"generation_id": original.generation_id})
    else:
        generation = generation.model_copy(update={"selector": m.ScopeSelector(
            tenant_id=CONTEXT.tenant_id, kind="domain", domain_id=DOMAIN,
        )})
    assert not inventory_confirms_deletion(target(), deleted, generation)


@pytest.mark.parametrize("backend", ["memory", "sql"])
@pytest.mark.parametrize("unknown", ["domain", "ancestor", "workspace", "partial"])
def test_capability_reconciliation_preserves_domain_admission_identity_in_both_stores(
    tmp_path, backend, unknown,
):
    h = SqlHarness(tmp_path / "domain-authority.sqlite") if backend == "sql" else Harness()
    h.seed()
    identity = h.targets[0]
    record_hierarchy(h.store, h.clock, identity=identity, writer=h.record_inventory)
    h.activate(m.ScopeDefinition(
        **h.context(), scope_id=h.next_id(), name="Reviewed domain scope",
        rules=(m.ScopeRule(
            rule_id=h.next_id(), effect="include",
            selector=m.ScopeSelector(
                tenant_id=identity.tenant_id, kind="domain", domain_id=DOMAIN, include_descendants=True,
            ),
        ),),
    ))
    before = h.store.resolve_target(identity)
    expected = h.version
    incomplete = record_hierarchy(
        h.store, h.clock, identity=identity, unknown=unknown, complete=False, writer=h.record_inventory,
    )
    h.clock.advance(1)
    h.capability(identity)
    paused = h.store.resolve_target(identity, include_inactive=True)
    assert paused.state == "paused" and not paused.observation.enabled and not paused.action.enabled
    assert paused.scope_ids == before.scope_ids and paused.admitted_rule_ids == before.admitted_rule_ids
    assert paused.admission_basis == before.admission_basis
    assert h.version == expected
    if backend == "sql":
        work = h.store.get_work(m.MonitoringContext(**h.context()), incomplete.generation_id)
        h.store.disposition_work(m.WorkDispositionRequest(
            **h.context(), request_id=h.next_id(), work_id=work.work_id, lease=work.lease,
            expected_work_revision=work.revision, disposition="superseded",
            detail="A new scan will replace this incomplete collection without claiming it completed.",
        ))
    record_hierarchy(h.store, h.clock, identity=identity, writer=h.record_inventory)
    h.clock.advance(1)
    h.capability(identity)
    restored = h.store.resolve_target(identity)
    assert restored.observation.enabled and not restored.action.enabled
    assert restored.scope_ids == before.scope_ids and restored.admitted_rule_ids == before.admitted_rule_ids
    assert h.version == expected


@pytest.mark.parametrize("scope_kind", ["workspace", "domain"])
async def test_partial_metadata_does_not_reopen_complete_scoped_absence(
    factory, tmp_path, scope_kind,
):
    seed, controller, web, clock, remote = await domain_case(factory, tmp_path)
    original = connector(seed)
    work = planning_work(seed, controller, web, suffix="scoped-absence")
    producer = controller.get_reconciliation_request(CONTEXT, work.reconcile_request_id, producer="web")
    frontier = controller.get_validation_frontier(CONTEXT, producer.frontier_key)
    plan = plan_definition(original, original.desired_definition, (), removal_targets=(target(),))
    controller.publish_connector(publication_from_plan(
        version(seed), work, frontier, original, plan,
        request_id=stable_id(CONTEXT, "historical-scoped-removal"),
    ))
    record_absence(seed, clock, scope_kind)
    record_hierarchy(seed, clock, unknown="domain", complete=False)
    assert seed.resolve_target(target(), include_inactive=True).state == "removed"
    updates = len(remote.update_bodies)
    before = connector(seed)
    result = await factory(seed, clock, remote).run_once()
    if scope_kind == "workspace":
        assert result.results[0].code == "controller_source_retirement_required"
        assert len(remote.update_bodies) == updates + 1
    else:
        assert result.results[0].code == "pending_source_removal_authority_unverified"
        assert connector(seed) == before and len(remote.update_bodies) == updates
