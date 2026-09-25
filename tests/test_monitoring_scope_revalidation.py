"""Reviewed scope effects remain binding while unrelated inventory advances."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import pytest
from test_monitoring_sql_abi import AbiDatabase
from test_monitoring_store import Harness, uid

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringCommitUncertain, MonitoringConflict
from triage.monitoring.memory import InMemoryMonitoringStore, StoredRecord, key_digest
from triage.monitoring.sql_store import AzureSqlMonitoringStore


class ScopeCase:
    def __init__(self, sql):
        h = self.h = Harness()
        h.seed(count=2, workspaces=2)
        self.target = h.targets[0]
        h.clock.advance(1)
        h.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version, items=(),
            generation=m.InventoryGeneration(
                **h.context(), generation_id=h.next_id(),
                selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
                adapter="fixture", authority="tenant_admin", completeness="partial",
                started_at=h.clock(), continuation="tenant-next",
                gaps=(m.CoverageGap(code="inventory_in_progress", detail="Other workspaces remain."),),
            ),
        ))
        h.clock.advance(1)
        selector = m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=self.target.workspace_id)
        self.generation_id = h.next_id()
        item = h.store.list_inventory(m.TargetQuery(**h.context(), workspace_id=self.target.workspace_id)).items[0]
        h.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version,
            items=(item.model_copy(update={"generation_id": self.generation_id, "observed_at": h.clock()}),),
            generation=m.InventoryGeneration(
                **h.context(), generation_id=self.generation_id, selector=selector,
                adapter="fixture", authority="tenant_admin", completeness="complete",
                started_at=h.clock(), completed_at=h.clock(), completed_pages=1, discovered_count=1,
            ),
        ))
        h.generation_id = self.generation_id
        h.capability(self.target)
        self.db = AbiDatabase(h, principal="web") if sql else None
        if self.db:
            self.db.seed_published_fixture(h)
        self.web = self.component("web")
        self.scope = m.ScopeDefinition(
            **h.context(), scope_id=uid(20), name="Workspace A",
            rules=(m.ScopeRule(rule_id=uid(21), selector=selector, effect="include"),),
        )
        controller = self.component("controller")
        self.draft = controller.enqueue_work(m.MonitoringWorkDraft(
            **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=0,
            discovery_selector=m.ScopeSelector(
                tenant_id=uid(1), kind="workspace", workspace_id=h.targets[1].workspace_id,
            ),
            created_at=h.clock(), due_at=h.clock(), reason="Continue independent Workspace B inventory.",
        ))
        worker = self.component("worker")
        self.collection = worker.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(99), kinds=("inventory",), limit=1, per_workspace_limit=1,
        ))[0]
        self.other_generation = worker.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version, items=(),
            generation=m.InventoryGeneration(
                **h.context(), generation_id=self.draft.work_id, selector=self.draft.discovery_selector,
                adapter="fixture", authority="tenant_admin", completeness="partial",
                started_at=h.clock(), continuation="B-next",
                gaps=(m.CoverageGap(code="inventory_in_progress", detail="Workspace B is still collecting."),),
            ),
            commit=m.InventoryCommit(
                work_id=self.collection.work_id, lease=self.collection.lease,
                expected_work_revision=self.collection.revision, expected_generation_revision=0,
            ),
        ))
        self.component("web")

    def component(self, name):
        if self.db:
            self.db.principal = name
            return AzureSqlMonitoringStore(db=self.db, component=name)
        return InMemoryMonitoringStore(clock=self.h.clock, state=self.h.state, component=name)

    def row(self, kind, key):
        return (
            self.db.records.get((kind, key)) if self.db else
            self.h.state.records.get((*self.h.context().values(), kind, key_digest(key)))
        )

    def put(self, kind, key, value):
        prior = self.row(kind, key)
        row = replace(prior, version=prior.version + 1, payload=value.model_dump_json()) if prior else StoredRecord(
            kind=kind, key=key, context=m.MonitoringContext(**self.h.context()),
            version=1, payload=value.model_dump_json(),
        )
        if isinstance(value, m.InventoryItem):
            row = replace(
                row, workload=value.workload, status=value.state, workspace_id=value.workspace_id,
                item_id=value.item_id, generation_id=value.generation_id,
            )
        if self.db:
            self.db.records[(kind, key)] = row
            self.db.fixture_facts[(kind, key)] = self.db.row_hash(row)
        else:
            self.h.state.records[(*self.h.context().values(), kind, key_digest(key))] = row

    def snapshot(self):
        if self.db:
            return deepcopy((self.db.control, self.db.records, self.db.receipts))
        return deepcopy((self.h.state.control_row, self.h.state.records, self.h.state.receipts))

    def preview(self):
        self.component("web")
        self.request = m.ScopePreviewRequest(
            expected=self.h.version, scope=self.scope, idempotency_id=self.h.next_id(),
            requested_by="fixture-verified-actor",
        )
        self.plan = self.web.preview_scope(self.request)
        self.activation = m.ActivateScopeRequest(
            expected=self.request.expected, plan_id=self.plan.plan_id, idempotency_id=self.request.idempotency_id,
        )
        assert self.plan.status == "ready" and self.plan.inventory_completeness == "complete"
        return self.plan

    def advance_other_workspace(self):
        h = self.h
        h.clock.advance(1)
        worker = self.component("worker")
        self.other_generation = worker.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version,
            generation=self.other_generation.model_copy(update={"completed_pages": 1, "continuation": "B-more"}),
            items=(m.InventoryItem(
                **h.context(), generation_id=self.draft.work_id,
                workspace_id=h.targets[1].workspace_id, item_id=h.next_id(), name="New unrelated B item",
                item_type="DataPipeline", workload="fabric_pipeline", observed_at=h.clock(),
            ),),
            commit=m.InventoryCommit(
                work_id=self.collection.work_id, lease=self.collection.lease,
                expected_work_revision=self.collection.revision,
                expected_generation_revision=self.other_generation.revision,
                expected_continuation=self.other_generation.continuation,
            ),
        ))
        self.component("web")


@pytest.fixture(params=[False, True], ids=["memory", "sql_abi"])
def case(request):
    return ScopeCase(request.param)


@pytest.mark.parametrize("new_page", [False, True], ids=["partial-unrelated-generation", "unrelated-page-commit"])
def test_unrelated_inventory_does_not_replace_or_invalidate_the_reviewed_scope(case, new_page, monkeypatch):
    plan = case.preview()
    original = case.row("plan", plan.plan_id)
    if new_page:
        case.advance_other_workspace()
    evaluations = []
    evaluate = case.web._evaluate_scope_plan

    def observed(request, control):
        backend = case.web._backend
        assert backend.transaction_active
        if case.db:
            assert case.db.active
            assert case.web._sql.collection.operation == "activation"
            assert any("triage_mon_lock_context" in sql for _, sql, _ in case.db.calls[calls:])
        value = evaluate(request, control)
        evaluations.append(value)
        return value

    monkeypatch.setattr(case.web, "_evaluate_scope_plan", observed)
    calls = len(case.db.calls) if case.db else 0
    result = case.web.activate_scope(case.activation)
    assert len(evaluations) == 1
    assert (evaluations[0].inventory_revision > plan.inventory_revision) is new_page
    assert result.plan_id == plan.plan_id and result.idempotency_id == plan.idempotency_id
    assert result.scope.rules == plan.scope.rules and result.scope.scope_id == plan.scope.scope_id
    assert result.requested_by == plan.requested_by
    assert result.version.revision == 1 and result.state == "configuring"
    assert len(result.queued_work_ids) == 1
    assert case.row("plan", plan.plan_id) == original
    assert case.web.get_plan(case.h.version, plan.plan_id) == plan
    if case.db:
        activation_calls = case.db.calls[calls:]
        assert not any(method == "execute" for method, _, _ in activation_calls)
        commits = [sql for _, sql, _ in activation_calls if sql.startswith("EXEC ") and "web_commit_intent" in sql]
        assert len(commits) == 1
    assert case.web.activate_scope(case.activation) == result
    assert case.web.get_activation(case.h.version, plan.idempotency_id) == result
    assert case.web.preview_scope(case.request) == plan
    assert len(evaluations) == 1
    case.h.clock.advance(901)
    assert case.component("web").activate_scope(case.activation) == result


@pytest.mark.parametrize("change", [
    "new_target", "removed_target", "moved_target", "unknown_item", "read_capability",
    "event_capability", "permissions", "required_coverage", "subscription", "prior_admission",
])
def test_material_review_changes_still_refuse_activation_without_replacing_the_plan(case, change):
    plan = case.preview()
    h, target = case.h, case.target
    h.clock.advance(1)
    item_key = f"{target.workspace_id}:{target.item_id}"
    item = m.InventoryItem.model_validate_json(case.row("inventory", item_key).payload)
    if change in {"new_target", "removed_target", "moved_target", "unknown_item"}:
        if change == "new_target":
            new = item.model_copy(update={"item_id": h.next_id(), "observed_at": h.clock()})
            case.put("inventory", f"{new.workspace_id}:{new.item_id}", new)
        elif change == "moved_target":
            case.put("inventory", item_key, item.model_copy(update={"state": "deleted", "observed_at": h.clock()}))
            moved = item.model_copy(update={"workspace_id": uid(555), "observed_at": h.clock()})
            case.put("inventory", f"{moved.workspace_id}:{moved.item_id}", moved)
        else:
            case.put("inventory", item_key, item.model_copy(update={
                "state": "deleted" if change == "removed_target" else "unknown", "observed_at": h.clock(),
            }))
    elif change in {"read_capability", "event_capability", "permissions"}:
        probe = m.CapabilityObservation.model_validate_json(case.row("target_capability", target.key).payload)
        fields = (
            {"read_status": "unknown", "event_status": "unknown", "action_status": "unknown"} if change == "read_capability" else
            {"event_status": "unknown"} if change == "event_capability" else {"required_permissions": ("Workspace.Read.All",)}
        )
        case.put("target_capability", target.key, m.CapabilityObservation.model_validate({
            **probe.model_dump(), **fields, "checked_at": h.clock(),
        }))
    elif change == "required_coverage":
        generation = m.InventoryGeneration.model_validate_json(case.row("generation", case.generation_id).payload)
        case.put("generation", case.generation_id, m.InventoryGeneration.model_validate({
            **generation.model_dump(), "completeness": "unknown",
            "gaps": (m.CoverageGap(code="unreadable", detail="Required workspace coverage is unknown."),),
        }))
    elif change == "subscription":
        manifest = m.OwnedConnectorManifest(
            **h.context(), connector_id=h.next_id(), ownership_id=h.next_id(), revision=1,
            policy_revision=0, name="Existing owned subscription",
            sources=(m.ConnectorSource(
                source_id="fixture-source", target=target,
                event_types=("ItemJobFailed",), event_source="fixture:jobs",
            ),), desired_definition={}, state="planned", updated_at=h.clock(),
        )
        case.put("connector", manifest.connector_id, manifest)
    else:
        probe = m.CapabilityObservation.model_validate_json(case.row("target_capability", target.key).payload)
        case.put("target", target.key, m.MonitoringTarget(
            identity=target, name=item.name, scope_ids=(case.scope.scope_id,), admitted_rule_ids=(uid(21),),
            inventory_generation=case.generation_id, capability_id=probe.capability_id,
            policy_revision=0, admitted_at=h.clock(), state="current", admission_basis="reviewed",
            reason="A changed current admission alters both the reviewed change and poll delta.",
            observation=m.ObservationPolicy(enabled=True),
        ))
    before = case.snapshot()
    with pytest.raises(MonitoringConflict, match="changed after the dry run"):
        case.web.activate_scope(case.activation)
    assert case.snapshot() == before
    assert case.web.get_plan(h.version, plan.plan_id) == plan
    assert case.web.get_activation(h.version, plan.idempotency_id) is None


def test_domain_membership_change_is_a_material_change_not_an_unrelated_counter(case):
    h = case.h
    domain_id = uid(777)
    case.put("workspace", case.target.workspace_id, m.InventoryWorkspace(
        **h.context(), generation_id=case.generation_id, workspace_id=case.target.workspace_id,
        domain_id=domain_id, name="Workspace A", observed_at=h.clock(),
    ))
    selector = m.ScopeSelector(tenant_id=uid(1), kind="domain", domain_id=domain_id)
    generation = m.InventoryGeneration.model_validate_json(case.row("generation", case.generation_id).payload)
    case.put("generation", case.generation_id, generation.model_copy(update={"selector": selector}))
    case.scope = case.scope.model_copy(update={"rules": (
        m.ScopeRule(rule_id=uid(21), selector=selector, effect="include"),
    )})
    case.preview()
    h.clock.advance(1)
    workspace = m.InventoryWorkspace.model_validate_json(case.row("workspace", case.target.workspace_id).payload)
    case.put("workspace", case.target.workspace_id, workspace.model_copy(update={
        "domain_id": uid(778), "observed_at": h.clock(),
    }))
    before = case.snapshot()
    with pytest.raises(MonitoringConflict, match="changed after the dry run"):
        case.web.activate_scope(case.activation)
    assert case.snapshot() == before


def test_capability_expiry_is_rechecked_even_without_a_counter_change(case):
    probe = m.CapabilityObservation.model_validate_json(case.row("target_capability", case.target.key).payload)
    case.put("target_capability", case.target.key, probe.model_copy(update={
        "expires_at": case.h.clock() + timedelta(seconds=30),
    }))
    case.preview()
    case.h.clock.advance(31)
    before = case.snapshot()
    with pytest.raises(MonitoringConflict, match="changed after the dry run"):
        case.web.activate_scope(case.activation)
    assert case.snapshot() == before


def test_equal_revision_sums_cannot_hide_replacement_target_identity(case, monkeypatch):
    plan = case.preview()
    old_key = f"{case.target.workspace_id}:{case.target.item_id}"
    row = case.row("inventory", old_key)
    item = m.InventoryItem.model_validate_json(row.payload)
    replacement = item.model_copy(update={"item_id": case.h.next_id()})
    if case.db:
        case.db.records.pop(("inventory", old_key))
    else:
        case.h.state.records.pop((*case.h.context().values(), "inventory", key_digest(old_key)))
    for _ in range(row.version):
        case.put("inventory", f"{replacement.workspace_id}:{replacement.item_id}", replacement)
    evaluate = case.web._evaluate_scope_plan
    evaluated = []

    def same_counter(request, control):
        current = evaluate(request, control)
        assert current.inventory_revision == plan.inventory_revision
        assert current.changes != plan.changes
        evaluated.append(current)
        return current

    monkeypatch.setattr(case.web, "_evaluate_scope_plan", same_counter)
    before = case.snapshot()
    with pytest.raises(MonitoringConflict, match="changed after the dry run"):
        case.web.activate_scope(case.activation)
    assert len(evaluated) == 1 and case.snapshot() == before


def test_truncated_gaps_do_not_authorize_effect_equivalence_after_inventory_changes(case):
    h = case.h
    for _ in range(201):
        item = m.InventoryItem(
            **h.context(), generation_id=case.generation_id,
            workspace_id=case.target.workspace_id, item_id=h.next_id(), name="Unverified fixture pipeline",
            item_type="DataPipeline", workload="fabric_pipeline", observed_at=h.clock(),
        )
        case.put("inventory", f"{item.workspace_id}:{item.item_id}", item)
    plan = case.preview()
    assert plan.gaps[-1].code == "additional_gaps"
    case.advance_other_workspace()
    before = case.snapshot()
    with pytest.raises(MonitoringConflict, match="changed after the dry run"):
        case.web.activate_scope(case.activation)
    assert case.snapshot() == before


def test_scope_comparison_uses_the_store_redaction_boundary(case, monkeypatch):
    h = case.h
    monkeypatch.setattr(case.web, "_redactor", lambda text: text.replace("private-fixture-detail", "[REDACTED]"))
    probe = m.CapabilityObservation.model_validate_json(case.row("target_capability", case.target.key).payload)
    case.put("target_capability", case.target.key, probe.model_copy(update={
        "required_permissions": ("private-fixture-detail",),
    }))
    plan = case.preview()
    assert "private-fixture-detail" not in plan.model_dump_json()
    assert "[REDACTED]" in plan.model_dump_json()
    case.advance_other_workspace()
    result = case.web.activate_scope(case.activation)
    assert result.plan_id == plan.plan_id and case.web.get_plan(h.version, plan.plan_id) == plan


@pytest.mark.parametrize("changed", [
    "expired", "epoch", "policy", "idempotency_id", "plan_id", "current_policy", "maintenance",
])
def test_original_activation_context_and_time_checks_remain_strict(case, changed):
    case.preview()
    request = case.activation
    if changed == "expired":
        case.h.clock.advance(900)
    elif changed in {"epoch", "policy"}:
        request = request.model_copy(update={"expected": m.RegistryVersion(
            **(case.h.context() | ({"epoch": uid(999)} if changed == "epoch" else {})),
            revision=1 if changed == "policy" else 0,
        )})
    elif changed in {"current_policy", "maintenance"}:
        fields = {"revision": 1} if changed == "current_policy" else {"maintenance": True}
        if case.db:
            case.db.control = m.DeploymentControl.model_validate({**case.db.control.model_dump(), **fields})
        else:
            case.h.state.control_row.update(fields)
    else:
        request = request.model_copy(update={changed: uid(999)})
    before = case.snapshot()
    with pytest.raises(MonitoringConflict):
        case.web.activate_scope(request)
    assert case.snapshot() == before


def test_original_receipt_rejects_same_id_with_different_activation_arguments(case):
    case.preview()
    case.advance_other_workspace()
    case.web.activate_scope(case.activation)
    before = case.snapshot()
    with pytest.raises(MonitoringConflict):
        case.web.activate_scope(case.activation.model_copy(update={"plan_id": uid(999)}))
    assert case.snapshot() == before


def test_lost_sql_ack_replays_original_activation_without_recalculation():
    case = ScopeCase(True)
    plan = case.preview()
    case.advance_other_workspace()
    case.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        case.web.activate_scope(case.activation)
    assert case.db.control.revision == 1
    reopened = case.component("web")
    original = reopened.get_activation(case.h.version, plan.idempotency_id)
    assert original is not None and original.plan_id == plan.plan_id
    assert reopened.activate_scope(case.activation) == original
    assert reopened.get_plan(case.h.version, plan.plan_id) == plan
