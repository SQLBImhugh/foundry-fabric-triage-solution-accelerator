from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from test_monitoring_api import no_network as no_network
from test_monitoring_sql_store import SqlHarness, SqliteAzureDatabase, SqlProtocolFixtureStore
from test_monitoring_store import Harness, uid

from triage.command_center.api import create_app
from triage.command_center.models import Actor, WebSettings
from triage.command_center.monitoring import MonitoringService
from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.monitoring.schema import resolve_tables


@pytest.fixture(params=["memory", "sql"])
def phase(request, tmp_path: Path):
    h = SqlHarness(tmp_path / "phase.sqlite") if request.param == "sql" else Harness()
    h.seed()
    h.activate()
    return h


def reopened(h):
    if isinstance(h, SqlHarness):
        return SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    return InMemoryMonitoringStore(state=h.state, clock=h.clock)


@pytest.mark.parametrize("storage", ["memory", "sql"])
def test_complete_selected_workspace_does_not_certify_partially_discovered_other_items(storage, tmp_path):
    h = SqlHarness(tmp_path / "coverage.sqlite") if storage == "sql" else Harness()
    h.seed(count=3, workspaces=1)
    selector = m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=uid(100))
    h.activate(m.ScopeDefinition(
        **h.context(), scope_id=h.next_id(), name="Selected workspace",
        rules=(m.ScopeRule(rule_id=h.next_id(), selector=selector, effect="include"),),
    ))
    h.clock.advance(1)
    generation = h.next_id()
    selected = h.store.list_inventory(m.TargetQuery(**h.context())).items
    h.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=generation, selector=selector,
            adapter="fixture", authority="tenant_admin", completeness="complete",
            started_at=h.clock(), completed_at=h.clock(), completed_pages=1, discovered_count=3,
        ),
        items=tuple(item.model_copy(update={
            "generation_id": generation, "observed_at": h.clock(),
        }) for item in selected),
    ))
    h.clock.advance(1)
    other_generation = h.next_id()
    h.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version,
        generation=m.InventoryGeneration(
            **h.context(), generation_id=other_generation,
            selector=m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=uid(101)),
            adapter="fixture", authority="tenant_admin", completeness="partial",
            started_at=h.clock(), continuation="more-items", completed_pages=1,
            discovered_count=5,
            gaps=(m.CoverageGap(code="inventory_in_progress", detail="Other items remain."),),
        ),
        items=tuple(m.InventoryItem(
            **h.context(), generation_id=other_generation, workspace_id=uid(101),
            item_id=uid(2_000 + index), workload="fabric_pipeline", item_type="DataPipeline",
            name=f"Other item {index}", observed_at=h.clock(),
        ) for index in range(5)),
    ))
    coverage = reopened(h).coverage(m.MonitoringContext(**h.context()))
    assert coverage.discovered_count == 8
    assert coverage.inventory_completeness == "partial"
    assert coverage.scope_item_count is None
    preview = h.store.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=h.scope,
    ))
    assert preview.status == "ready" and preview.inventory_completeness == "complete"
    assert {change.identity.workspace_id for change in preview.changes} == {uid(100)}


def review_request(h, prior=None, *, revoked=False, parameters=None):
    fields = prior.model_dump() if prior is not None else {
        "review_id": h.next_id(), "target": h.targets[0], "action": "pipeline_rerun",
        "reviewer_id": uid(5), "definition_hash": h.definition_hash,
        "parameters": parameters if parameters is not None else {},
        "replay_safe": True, "exact_correlation_verified": True,
    }
    return m.SafetyReviewRequest(
        request_id=h.next_id(), expected=h.version,
        expected_review_revision=prior.revision if prior is not None else 0,
        review=m.SafetyReview.model_validate({
            **fields, "revision": prior.revision + 1 if prior is not None else 1,
            "policy_revision": h.version.revision, "state": "revoked" if revoked else "verified",
            "reviewed_at": h.clock(), "expires_at": h.clock() + timedelta(hours=1),
            "revoked_at": h.clock() if revoked else None, "detail": "Explicit fixture review.",
        }),
    )


def test_review_operation_is_not_inferred_from_an_older_current_review(phase):
    h = phase
    first = review_request(h)
    saved = h.store.record_safety_review(first)
    pending = review_request(h, saved, revoked=True)
    other = reopened(h)
    context = m.MonitoringContext(**h.context())
    assert other.get_safety_review(context, saved.review_id).revision == 1
    assert other.get_safety_review_operation(context, pending.request_id) is None
    assert other.get_safety_review_operation(context, first.request_id).review == saved


def test_original_review_operation_survives_later_revisions_and_registry_changes(phase):
    h = phase
    first = review_request(h)
    original = h.store.record_safety_review(first)
    revoke = review_request(h, original, revoked=True)
    revoked = h.store.record_safety_review(revoke)
    replacement = review_request(h, revoked)
    latest = h.store.record_safety_review(replacement)
    h.activate(m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}))
    other = reopened(h)
    context = m.MonitoringContext(**h.context())
    receipt = other.get_safety_review_operation(context, revoke.request_id)
    assert receipt.request_id == revoke.request_id
    assert receipt.target == revoked.target and receipt.action == revoked.action
    assert receipt.expected == revoke.expected
    assert (receipt.expected_review_revision, receipt.new_review_revision) == (1, 2)
    assert receipt.review == revoked and receipt.review.state == "revoked"
    assert receipt.recorded_at == h.clock()
    assert receipt.fingerprint == other.get_operation_receipt(
        context, "safety_review", revoke.request_id,
    ).fingerprint
    assert other.get_safety_review(context, revoked.review_id) == latest
    assert other.get_safety_review_operation(context, first.request_id).review == original


@pytest.mark.parametrize("commit", ["before", "after"])
def test_sql_uncertain_revocation_is_reconciled_by_original_operation_only(tmp_path: Path, commit):
    h = SqlHarness(tmp_path / "uncertain-review.sqlite")
    h.seed()
    h.activate()
    first = review_request(h)
    original = h.store.record_safety_review(first)
    revoke = review_request(h, original, revoked=True)
    h.db.fail_commit = commit
    with pytest.raises(MonitoringCommitUncertain):
        h.store.record_safety_review(revoke)
    other = reopened(h)
    context = m.MonitoringContext(**h.context())
    receipt = other.get_safety_review_operation(context, revoke.request_id)
    if commit == "before":
        assert receipt is None
        assert other.get_safety_review(context, original.review_id) == original
    else:
        assert receipt.review.state == "revoked"
        assert receipt.new_review_revision == 2 and receipt.expected_review_revision == 1
        assert other.record_safety_review(revoke) == receipt.review
    assert other.get_safety_review_operation(context, first.request_id).review == original


@pytest.mark.parametrize("commit", ["before", "after"])
def test_exact_http_operation_envelope_uses_durable_receipt_after_sql_restart(tmp_path: Path, commit):
    h = SqlHarness(tmp_path / "receipt-http.sqlite")
    h.seed()
    h.activate()
    original = h.store.record_safety_review(review_request(h))
    revoke = review_request(h, original, revoked=True)
    h.db.fail_commit = commit
    with pytest.raises(MonitoringCommitUncertain):
        h.store.record_safety_review(revoke)
    other = reopened(h)
    context = m.MonitoringContext(**h.context())
    receipt = other.get_safety_review_operation(context, revoke.request_id)
    if receipt is not None:
        other.record_safety_review(review_request(h, receipt.review))

    class ReaderVerifier:
        def verify(self, _token):
            return Actor(id=uid(5), display_name="Fixture reader", roles=["reader"])

    runtime = SimpleNamespace(
        monitoring=MonitoringService(other, tenant_id=h.control.tenant_id), demo_tasks=[],
    )
    app = create_app(
        runtime, token_verifier=ReaderVerifier(),
        web_settings=WebSettings(
            _env_file=None, mode="live", tenant_id=h.control.tenant_id,
            client_id=uid(45), static_dir=str(tmp_path / "not-built"),
            access_management_enabled=False,
        ),
    )
    with TestClient(app) as client:
        headers = {"Authorization": "Bearer " + "offline-reader"}
        response = client.get(
            f"/api/monitoring/safety-review-operations/{revoke.request_id}", headers=headers,
        )
        current = client.get(f"/api/monitoring/safety-reviews/{original.review_id}", headers=headers)
    assert response.headers["Cache-Control"] == "no-store"
    if commit == "before":
        assert response.status_code == 404 and response.json()["code"] == "not_found"
        assert current.json()["revision"] == 1
    else:
        assert response.status_code == 200
        assert set(response.json()) == {"request_id", "review"}
        assert response.json() == {
            "request_id": revoke.request_id, "review": receipt.review.model_dump(mode="json"),
        }
        assert response.json()["review"]["state"] == "revoked"
        assert response.json()["review"]["revision"] == 2
        assert current.json()["revision"] == 3


def test_operation_receipt_returns_only_redacted_persisted_review_and_original_fingerprints(phase):
    h = phase
    secret = "AKIA" + "IOSFODNN7EXAMPLE"
    request = review_request(h, parameters={"source": secret})
    saved = h.store.record_safety_review(request)
    receipt = reopened(h).get_safety_review_operation(
        m.MonitoringContext(**h.context()), request.request_id,
    )
    assert receipt.review == saved
    assert receipt.review.parameters is None and receipt.review.parameters_redacted
    assert receipt.review.state == "unverifiable"
    assert receipt.review.parameter_hash == request.review.parameter_hash
    assert secret not in receipt.model_dump_json()
    assert receipt.target == request.review.target
    assert receipt.new_review_revision == request.review.revision


def test_sql_operation_lookup_outage_is_not_reported_as_absence(tmp_path: Path):
    h = SqlHarness(tmp_path / "operation-outage.sqlite")
    h.seed()
    h.activate()
    request = review_request(h)
    h.store.record_safety_review(request)
    h.db.fail_statement = lambda sql, _params: (
        sql.lstrip().startswith("SELECT") and "monitoring_receipts" in sql
    )
    context = m.MonitoringContext(**h.context())
    with pytest.raises(MonitoringUnavailable):
        h.store.get_safety_review_operation(context, request.request_id)
    assert reopened(h).get_safety_review_operation(context, request.request_id) is not None


@pytest.mark.parametrize("field", ["tenant_id", "epoch"])
def test_operation_lookup_never_crosses_the_current_context(phase, field):
    request = review_request(phase)
    phase.store.record_safety_review(request)
    context = m.MonitoringContext(**(phase.context() | {field: uid(999)}))
    with pytest.raises(MonitoringConflict):
        reopened(phase).get_safety_review_operation(context, request.request_id)


def test_corrupt_operation_payload_is_unavailable_not_not_found_or_latest_review(phase):
    h = phase
    request = review_request(h)
    saved = h.store.record_safety_review(request)
    if isinstance(h, SqlHarness):
        table = resolve_tables(h.db)["monitoring_receipts"]
        h.db.execute(
            f"UPDATE [dbo].[{table}] SET payload = ? WHERE operation = ? AND request_id = ?",
            '{"revision":1}', "safety_review", request.request_id,
        )
    else:
        key = next(
            key for key, receipt in h.state.receipts.items()
            if receipt.operation == "safety_review" and receipt.request_id == request.request_id
        )
        h.state.receipts[key] = replace(h.state.receipts[key], payload='{"revision":1}')
    other = reopened(h)
    context = m.MonitoringContext(**h.context())
    assert other.get_safety_review(context, saved.review_id) == saved
    with pytest.raises(MonitoringUnavailable, match="receipt"):
        other.get_safety_review_operation(context, request.request_id)


@pytest.mark.parametrize("changes", [
    {"expected_review_revision": 1}, {"new_review_revision": 2},
    {"action": "powerbi_refresh"}, {"fingerprint": "invalid"},
    {"target": {"tenant_id": uid(1)}}, {"parameters": {"must_not": "be_an_operation_field"}},
])
def test_operation_contract_rejects_inconsistent_bindings(phase, changes):
    request = review_request(phase)
    phase.store.record_safety_review(request)
    receipt = phase.store.get_safety_review_operation(
        m.MonitoringContext(**phase.context()), request.request_id,
    )
    with pytest.raises(ValidationError):
        m.SafetyReviewOperationReceipt.model_validate(receipt.model_dump() | changes)


def incomplete_catalogue(h, enumeration):
    h.clock.advance(1)
    generation_id = h.next_id()
    workspace = h.targets[0].workspace_id
    domain = uid(55)
    return h.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, items=(),
        generation=m.InventoryGeneration(
            **h.context(), generation_id=generation_id,
            selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            enumeration=enumeration, adapter="incomplete_catalogue", authority="tenant_admin",
            completeness="partial", started_at=h.clock(),
            gaps=(m.CoverageGap(code="catalogue_unavailable", detail="Named metadata is incomplete."),),
        ),
        workspaces=(m.InventoryWorkspace(
            **h.context(), generation_id=generation_id, workspace_id=workspace,
            domain_id=domain, name="Unknown workspace", state="unknown", observed_at=h.clock(),
        ),) if enumeration == "workspaces" else (),
        domains=(m.InventoryDomain(
            **h.context(), generation_id=generation_id, domain_id=domain,
            name="Unknown domain", state="unknown", observed_at=h.clock(),
        ),) if enumeration == "domains" else (),
    ))


@pytest.mark.parametrize("enumeration", ["workspaces", "domains"])
def test_explicit_disable_stops_known_admissions_with_incomplete_catalogues(phase, enumeration):
    h = phase
    incomplete_catalogue(h, enumeration)
    disabled = m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False})
    preview = h.store.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=disabled,
    ))
    assert preview.status == "ready" and preview.inventory_completeness == "partial"
    assert {change.change for change in preview.changes} <= {"pause", "retain"}
    assert "inventory_incomplete" in {gap.code for gap in preview.gaps}
    h.clock.advance(1)
    h.capability(h.targets[0])
    receipt = h.store.activate_scope(m.ActivateScopeRequest(
        expected=h.version, plan_id=preview.plan_id, idempotency_id=preview.idempotency_id,
    ))
    assert not receipt.scope.enabled
    other = reopened(h)
    assert other.resolve_target(h.targets[0]) is None
    stopped = other.resolve_target(h.targets[0], include_inactive=True)
    assert stopped.state == "paused"
    assert not stopped.observation.enabled and not stopped.action.enabled
    assert stopped.policy_revision == receipt.version.revision


def test_disable_catches_automatic_admissions_arriving_after_preview(phase):
    h = phase
    automatic = m.ScopeDefinition.model_validate({
        **h.scope.model_dump(), "rules": [
            {**rule.model_dump(), "auto_enrol_detection_only": True} for rule in h.scope.rules
        ],
    })
    h.activate(automatic)
    disabled = m.ScopeDefinition.model_validate({**automatic.model_dump(), "enabled": False})
    plan = h.store.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=disabled,
    ))
    h.clock.advance(1)
    h.seed(count=2, workspaces=2)
    late = h.targets[1]
    assert h.store.resolve_target(late) is not None
    h.store.activate_scope(m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    ))
    assert all(h.store.resolve_target(target) is None for target in h.targets)
    assert h.store.resolve_target(late, include_inactive=True).state == "paused"


def test_disable_retains_only_preexisting_overlapping_admissions(phase):
    h = phase
    original = h.scope
    overlap = m.ScopeDefinition(
        **h.context(), scope_id=uid(75), name="Overlapping scope",
        rules=(m.ScopeRule(
            rule_id=uid(76), selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            effect="include",
        ),),
    )
    h.activate(overlap)
    incomplete_catalogue(h, "domains")
    h.activate(m.ScopeDefinition.model_validate({**original.model_dump(), "enabled": False}))
    target = h.store.resolve_target(h.targets[0])
    assert target is not None and target.observation.enabled
    assert target.scope_ids == (overlap.scope_id,)
    assert target.admitted_rule_ids == (uid(76),)
    assert not target.action.enabled
    assert all(change.change != "admit" for change in h.plan.changes)


def test_disable_does_not_admit_targets_when_an_exclusion_stops_matching(phase):
    h = phase
    include = h.scope
    exclusion = m.ScopeDefinition(
        **h.context(), scope_id=uid(85), name="Explicit exclusion",
        rules=(m.ScopeRule(
            rule_id=uid(86), selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            effect="exclude",
        ),),
    )
    h.activate(exclusion)
    assert h.store.resolve_target(h.targets[0]) is None
    incomplete_catalogue(h, "domains")
    h.activate(m.ScopeDefinition.model_validate({**exclusion.model_dump(), "enabled": False}))
    assert h.store.resolve_target(h.targets[0]) is None
    assert all(change.change != "admit" for change in h.plan.changes)
    assert next(scope for scope in h.store.list_scopes(m.PageQuery(**h.context())).items
                if scope.scope_id == include.scope_id).enabled


def test_incomplete_inventory_still_blocks_expanding_or_reenabling_a_scope(phase):
    h = phase
    original = h.scope
    h.activate(m.ScopeDefinition.model_validate({**original.model_dump(), "enabled": False}))
    incomplete_catalogue(h, "domains")
    plan = h.store.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=original,
    ))
    assert plan.status == "blocked" and plan.inventory_completeness == "partial"
    with pytest.raises(MonitoringConflict):
        h.store.activate_scope(m.ActivateScopeRequest(
            expected=h.version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
        ))
    assert h.store.resolve_target(h.targets[0]) is None


def test_sql_disable_rolls_back_policy_and_admissions_together_on_write_failure(tmp_path: Path):
    h = SqlHarness(tmp_path / "disable-rollback.sqlite")
    h.seed(count=2)
    h.activate()
    incomplete_catalogue(h, "domains")
    disabled = m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False})
    plan = h.store.preview_scope(m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), scope=disabled,
    ))
    request = m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=plan.idempotency_id,
    )
    h.db.fail_statement = lambda sql, params: (
        sql.startswith("UPDATE") and "monitoring_records" in sql and "paused" in params
    )
    with pytest.raises(MonitoringUnavailable):
        h.store.activate_scope(request)
    other = reopened(h)
    context = m.MonitoringContext(**h.context())
    assert other.snapshot(context).control.revision == h.version.revision
    assert other.list_scopes(m.PageQuery(**h.context())).items[0].enabled
    assert all(other.resolve_target(target) is not None for target in h.targets)
    assert other.get_activation(context, request.idempotency_id) is None
