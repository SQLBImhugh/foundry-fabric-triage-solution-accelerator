from __future__ import annotations

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from test_monitoring_api import no_network as no_network
from test_monitoring_store import Harness, uid

from triage.command_center.api import create_app
from triage.command_center.models import Actor, WebSettings
from triage.command_center.monitoring import MonitoringService
from triage.command_center.service import CommandCenterService
from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringCommitUncertain
from triage.monitoring.memory import InMemoryMonitoringStore
from triage.store.approvals import InMemoryApprovalChannel
from triage.store.command_center import InMemoryCommandCenterStore

PREFIX = "/api/monitoring"
ADMIN = Actor(id=uid(5), display_name="Fixture administrator", roles=["admin"])
READER = Actor(id=uid(6), display_name="Fixture reader", roles=["reader"])
OPERATOR = Actor(id=uid(7), display_name="Fixture operator", roles=["operator"])
OTHER_ADMIN = Actor(id=uid(8), display_name="Other administrator", roles=["admin"])


class NoSql:
    def query(self, *_args, **_kwargs):
        raise AssertionError("The component-memory API fixture must not query live SQL")

    def execute(self, *_args, **_kwargs):
        raise AssertionError("The component-memory API fixture must not write live SQL")


class Verifier:
    def verify(self, token):
        return {"admin": ADMIN, "reader": READER, "operator": OPERATOR, "other-admin": OTHER_ADMIN}[token]


def headers(role="reader"):
    return {"Authorization": "Bearer " + role}


@pytest.fixture
def authority(test_settings):
    h = Harness()
    h.seed()
    h.activate()
    web_store = InMemoryMonitoringStore(state=h.state, clock=h.clock, component="web")
    controller = InMemoryMonitoringStore(state=h.state, clock=h.clock, component="controller")
    settings = test_settings.model_copy(update={
        "monitoring_mode": "live", "monitoring_tenant_id": h.control.tenant_id,
        "azure_sql_server": "fixture.database.invalid", "azure_sql_database": "fixture",
    })
    web = WebSettings(
        _env_file=None, mode="live", tenant_id=h.control.tenant_id,
        client_id=uid(90), access_management_enabled=False,
    )
    service = CommandCenterService(
        settings, web, monitoring_store=web_store, db=NoSql(),
        history=InMemoryCommandCenterStore(), approvals=InMemoryApprovalChannel(),
    )
    return h, web_store, controller, service


def client_for(service):
    return TestClient(create_app(
        service, web_settings=service.web, token_verifier=Verifier(),
    ), raise_server_exceptions=False)


def intent(authority, prior=None, *, desired="verified", expires_at=None):
    h, web_store, _, _ = authority
    control = web_store.snapshot(m.MonitoringContext(**h.context())).control
    revoking = prior is not None and desired == "revoked"
    return m.SafetyReviewRequest(
        request_id=h.next_id(),
        expected=m.RegistryVersion(**h.context(), revision=control.revision),
        expected_review_revision=prior.revision if prior is not None else 0,
        review=m.SafetyReview(
            review_id=prior.review_id if prior is not None else h.next_id(),
            target=h.targets[0], revision=prior.revision + 1 if prior is not None else 1,
            policy_revision=control.revision, action="pipeline_rerun", state=desired,
            reviewer_id=uid(999), reviewed_at=prior.reviewed_at if revoking else h.clock(),
            expires_at=(
                expires_at if expires_at is not None
                else prior.expires_at if revoking else h.clock() + timedelta(hours=1)
            ),
            revoked_at=h.clock() if desired == "revoked" else None,
            definition_hash=h.definition_hash, parameters={}, replay_safe=True,
            exact_correlation_verified=True, detail="Human replay intent, not technical proof.",
        ),
    )


def publish(authority):
    h, _, controller, _ = authority
    return controller.drain_reconciliation(
        m.MonitoringContext(**h.context()), owner_id=uid(900),
    )


def test_real_web_acceptance_is_pending_and_only_controller_publishes(authority):
    h, web_store, controller, service = authority
    request = intent(authority)
    body = request.model_dump(mode="json", exclude={
        "review": {"requested_state", "publication_status"},
    })
    assert "requested_state" not in body["review"] and "publication_status" not in body["review"]
    with client_for(service) as client:
        response = client.post(
            f"{PREFIX}/safety-reviews", headers=headers("admin"),
            json=body,
        )
        assert response.status_code == 200, response.text
        pending = m.SafetyReview.model_validate(response.json())
        assert pending.state == "pending" and pending.requested_state == "verified"
        assert pending.publication_status == "pending_validation"
        assert not pending.exact_correlation_verified and pending.revoked_at is None
        assert pending.reviewer_id == ADMIN.id
        assert pending.parameter_hash == request.review.parameter_hash
        assert service.target_views(READER) == []
        assert web_store.resolve_target(h.targets[0]) is None
        command = {
            "kind": "pipeline_sweep", "target_id": h.targets[0].key,
            "idempotency_key": h.next_id(),
        }
        blocked = client.post("/api/commands", headers=headers("operator"), json=command)
        assert blocked.status_code == 422 and service.history.commands() == []
        operation = client.get(
            f"{PREFIX}/safety-review-operations/{request.request_id}", headers=headers(),
        )
        assert operation.json() == {"request_id": request.request_id, "review": response.json()}
        assert set(operation.json()) == {"request_id", "review"}
        assert all(result.state == "published" for result in publish(authority))
        current = client.get(
            f"{PREFIX}/safety-reviews/{pending.review_id}", headers=headers(),
        )
        published = m.SafetyReview.model_validate(current.json())
        assert published.state == "verified" and published.publication_status == "published"
        assert published.exact_correlation_verified
        assert controller.resolve_target(h.targets[0]).action.enabled
        assert client.get(
            f"{PREFIX}/safety-review-operations/{request.request_id}", headers=headers(),
        ).json() == operation.json()
        replay = client.post(
            f"{PREFIX}/safety-reviews", headers=headers("admin"),
            json=request.model_dump(mode="json"),
        )
        assert replay.json() == response.json()
        assert client.post("/api/commands", headers=headers("operator"), json=command).status_code == 200
    assert web_store.component == "web" and controller.component == "controller"


def test_original_revocation_receipt_stays_pending_after_publication_and_reload(authority):
    h, web_store, controller, service = authority
    first = intent(authority)
    with client_for(service) as client:
        accepted = client.post(
            f"{PREFIX}/safety-reviews", json=first.model_dump(mode="json"), headers=headers("admin"),
        )
        assert accepted.status_code == 200
        publish(authority)
        current = controller.get_safety_review(m.MonitoringContext(**h.context()), first.review.review_id)
        revoke = intent(authority, current, desired="revoked")
        pending = client.post(
            f"{PREFIX}/safety-reviews", json=revoke.model_dump(mode="json"), headers=headers("admin"),
        )
        assert pending.status_code == 200, pending.text
        assert pending.json()["state"] == "pending"
        assert pending.json()["requested_state"] == "revoked"
        assert pending.json()["publication_status"] == "pending_validation"
        assert pending.json()["revoked_at"] is None
        assert not pending.json()["exact_correlation_verified"]
        assert web_store.resolve_target(h.targets[0]) is None
        publish(authority)
    service._monitoring_service = None
    restored = InMemoryMonitoringStore(state=h.state, clock=h.clock, component="web")
    service._monitoring_service = MonitoringService(restored, tenant_id=h.control.tenant_id)
    with client_for(service) as client:
        operation = client.get(
            f"{PREFIX}/safety-review-operations/{revoke.request_id}", headers=headers(),
        )
        current_response = client.get(
            f"{PREFIX}/safety-reviews/{current.review_id}", headers=headers(),
        )
        assert operation.json() == {"request_id": revoke.request_id, "review": pending.json()}
        assert current_response.json()["state"] == "revoked"
        assert current_response.json()["publication_status"] == "published"
        assert current_response.json()["revoked_at"] is not None
        assert client.post(
            f"{PREFIX}/safety-reviews", json=revoke.model_dump(mode="json"), headers=headers(),
        ).status_code == 403


def test_uncertain_web_save_reconciles_the_original_pending_intent_without_retry(authority, monkeypatch):
    h, web_store, _, service = authority
    request = intent(authority)
    original = web_store.record_safety_review
    writes = []

    def lost_ack(value):
        saved = original(value)
        writes.append(saved)
        raise MonitoringCommitUncertain("safety_review", value.request_id)

    monkeypatch.setattr(web_store, "record_safety_review", lost_ack)
    with client_for(service) as client:
        failed = client.post(
            f"{PREFIX}/safety-reviews", json=request.model_dump(mode="json"), headers=headers("admin"),
        )
        assert failed.status_code == 503 and failed.json()["code"] == "monitoring_commit_uncertain"
        observed = client.get(
            f"{PREFIX}/safety-review-operations/{request.request_id}", headers=headers(),
        )
        assert observed.json() == {
            "request_id": request.request_id, "review": writes[0].model_dump(mode="json"),
        }
        assert observed.json()["review"]["publication_status"] == "pending_validation"
        assert len(writes) == 1
        assert client.get(
            f"{PREFIX}/safety-review-operations/{h.next_id()}", headers=headers(),
        ).status_code == 404


@pytest.mark.parametrize("desired", ["verified", "revoked"])
def test_expiry_does_not_turn_revocation_into_a_new_grant(authority, desired):
    h, web_store, controller, service = authority
    context = m.MonitoringContext(**h.context())
    request = intent(authority)
    with client_for(service) as client:
        assert client.post(
            f"{PREFIX}/safety-reviews", json=request.model_dump(mode="json"), headers=headers("admin"),
        ).status_code == 200
        assert all(result.state == "published" for result in publish(authority))
        prior = controller.get_safety_review(context, request.review.review_id)
        assert prior.state == "verified" and controller.resolve_target(h.targets[0]).action.enabled
        h.clock.advance(3_601)
        h.version = m.RegistryVersion(
            **h.context(), revision=web_store.snapshot(context).control.revision,
        )
        # Keep capability proof current so expiry is the only reason a grant cannot publish.
        capability = h.capability(h.targets[0])
        assert capability.expires_at > h.clock() > prior.expires_at
        if desired == "verified":
            candidate = intent(authority, prior)
            body = candidate.model_dump(mode="json")
            body["review"]["expires_at"] = prior.expires_at.isoformat()
            body["review"]["reviewed_at"] = prior.reviewed_at.isoformat()
        else:
            body = intent(authority, prior, desired="revoked", expires_at=prior.expires_at).model_dump(mode="json")
        response = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert response.status_code == 200, response.text
        pending = m.SafetyReview.model_validate(response.json())
        assert pending.state == "pending" and pending.requested_state == desired
        assert pending.publication_status == "pending_validation"
        assert not pending.exact_correlation_verified and pending.revoked_at is None
        assert pending.expires_at == prior.expires_at and pending.reviewed_at == prior.reviewed_at
        assert pending.revision == prior.revision + 1 and pending.reviewer_id == ADMIN.id
        assert web_store.resolve_target(h.targets[0]) is None
        receipt_path = f"{PREFIX}/safety-review-operations/{body['request_id']}"
        original_receipt = client.get(receipt_path, headers=headers())
        assert original_receipt.json() == {"request_id": body["request_id"], "review": response.json()}
        assert all(result.state == "published" for result in publish(authority))
        saved = controller.get_safety_review(context, prior.review_id)
        assert saved.publication_status == "published"
        assert saved.expires_at == prior.expires_at and saved.reviewed_at == prior.reviewed_at
        assert saved.revision == prior.revision + 1
        if desired == "verified":
            assert saved.state == "unverifiable"
            assert saved.revoked_at is None
        else:
            assert saved.state == "revoked" and saved.revoked_at == h.clock()
        assert not controller.resolve_target(h.targets[0]).action.enabled
        replay = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert replay.status_code == 200 and replay.json() == response.json()
        assert client.get(receipt_path, headers=headers()).json() == original_receipt.json()
        assert controller.get_safety_review(context, prior.review_id) == saved


@pytest.mark.parametrize("desired", ["pending", "verified", "unverifiable", None])
def test_pending_non_revocations_do_not_exempt_an_invalid_expiry_window(authority, desired):
    h, web_store, _, service = authority
    request = intent(authority)
    body = request.model_dump(mode="json")
    body["review"].update(
        state="pending", requested_state=desired, publication_status="pending_validation",
        exact_correlation_verified=False, expires_at=body["review"]["reviewed_at"],
    )
    with client_for(service) as client:
        response = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
    assert response.status_code == 422
    assert web_store.get_safety_review_operation(
        m.MonitoringContext(**h.context()), request.request_id,
    ) is None


@pytest.mark.parametrize("representation", ["intent", "pending"])
@pytest.mark.parametrize("changed_field", ["reviewed_at", "expires_at"])
def test_revocation_rejects_window_rewrites_in_both_intent_representations(
    authority, representation, changed_field,
):
    h, web_store, controller, service = authority
    context = m.MonitoringContext(**h.context())
    initial = intent(authority)
    with client_for(service) as client:
        assert client.post(
            f"{PREFIX}/safety-reviews", json=initial.model_dump(mode="json"), headers=headers("admin"),
        ).status_code == 200
        assert all(result.state == "published" for result in publish(authority))
        prior = controller.get_safety_review(context, initial.review.review_id)
        h.clock.advance(3_601)
        revoke = intent(authority, prior, desired="revoked")
        body = revoke.model_dump(mode="json")
        if representation == "pending":
            body["review"].update(
                state="pending", requested_state="revoked", publication_status="pending_validation",
                exact_correlation_verified=False, revoked_at=None,
            )
        body["review"][changed_field] = (
            h.clock() if changed_field == "reviewed_at" else h.clock() + timedelta(hours=1)
        ).isoformat()
        response = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert response.status_code == 409
        assert response.json()["code"] == "monitoring_conflict"
        assert web_store.get_safety_review_operation(context, revoke.request_id) is None
        assert controller.get_safety_review(context, prior.review_id) == prior


def test_explicit_pending_revocation_keeps_expired_window_and_original_receipt(authority):
    h, web_store, controller, service = authority
    context = m.MonitoringContext(**h.context())
    initial = intent(authority)
    with client_for(service) as client:
        assert client.post(
            f"{PREFIX}/safety-reviews", json=initial.model_dump(mode="json"), headers=headers("admin"),
        ).status_code == 200
        assert all(result.state == "published" for result in publish(authority))
        prior = controller.get_safety_review(context, initial.review.review_id)
        h.clock.advance(3_601)
        revoke = intent(authority, prior, desired="revoked")
        body = revoke.model_dump(mode="json")
        body["review"].update(
            state="pending", requested_state="revoked", publication_status="pending_validation",
            exact_correlation_verified=False, revoked_at=None,
        )
        accepted = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert accepted.status_code == 200, accepted.text
        pending = m.SafetyReview.model_validate(accepted.json())
        assert pending.state == "pending" and pending.requested_state == "revoked"
        assert pending.publication_status == "pending_validation"
        assert pending.reviewed_at == prior.reviewed_at and pending.expires_at == prior.expires_at
        assert pending.expires_at < h.clock() and pending.revision == prior.revision + 1
        assert web_store.resolve_target(h.targets[0]) is None
        assert all(result.state == "published" for result in publish(authority))
        current = controller.get_safety_review(context, prior.review_id)
        assert current.state == "revoked" and current.publication_status == "published"
        assert current.reviewed_at == prior.reviewed_at and current.expires_at == prior.expires_at
        assert not controller.resolve_target(h.targets[0], include_inactive=True).action.enabled
        receipt = client.get(
            f"{PREFIX}/safety-review-operations/{revoke.request_id}", headers=headers(),
        )
        assert receipt.json() == {"request_id": revoke.request_id, "review": accepted.json()}
        replay = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert replay.status_code == 200 and replay.json() == accepted.json()
        assert controller.get_safety_review(context, prior.review_id) == current


def test_original_revocation_replay_does_not_revoke_a_newer_review_window(authority):
    h, _, controller, service = authority
    context = m.MonitoringContext(**h.context())
    first = intent(authority)
    with client_for(service) as client:
        assert client.post(
            f"{PREFIX}/safety-reviews", json=first.model_dump(mode="json"), headers=headers("admin"),
        ).status_code == 200
        assert all(result.state == "published" for result in publish(authority))
        prior = controller.get_safety_review(context, first.review.review_id)
        h.clock.advance(1)
        revoke = intent(authority, prior, desired="revoked")
        body = revoke.model_dump(mode="json")
        accepted = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert accepted.status_code == 200
        assert all(result.state == "published" for result in publish(authority))
        revoked = controller.get_safety_review(context, prior.review_id)
        h.clock.advance(1)
        replacement = intent(authority, revoked)
        assert client.post(
            f"{PREFIX}/safety-reviews", json=replacement.model_dump(mode="json"), headers=headers("admin"),
        ).status_code == 200
        assert all(result.state == "published" for result in publish(authority))
        current = controller.get_safety_review(context, prior.review_id)
        assert current.revision == 3 and current.state == "verified"
        assert current.reviewed_at != prior.reviewed_at and current.expires_at != prior.expires_at
        replay = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert replay.status_code == 200 and replay.json() == accepted.json()
        original = client.get(
            f"{PREFIX}/safety-review-operations/{revoke.request_id}", headers=headers(),
        )
        assert original.json() == {"request_id": revoke.request_id, "review": accepted.json()}
        assert controller.get_safety_review(context, prior.review_id) == current
        assert controller.resolve_target(h.targets[0]).action.enabled


def test_browser_claims_do_not_supply_controller_capability_evidence(authority):
    h, _, controller, service = authority
    h.capability(h.targets[0], action_status="denied", exact_action_correlation=False)
    request = intent(authority)
    with client_for(service) as client:
        response = client.post(
            f"{PREFIX}/safety-reviews", json=request.model_dump(mode="json"), headers=headers("admin"),
        )
        assert response.status_code == 200
        assert response.json()["state"] == "pending"
        assert not response.json()["exact_correlation_verified"]
        publish(authority)
        current = client.get(
            f"{PREFIX}/safety-reviews/{request.review.review_id}", headers=headers(),
        )
        assert current.json()["state"] == "unverifiable"
        assert current.json()["publication_status"] == "published"
        assert not controller.resolve_target(h.targets[0]).action.enabled


@pytest.mark.parametrize("change", [
    {"component": "controller"},
    {"published_at": "2035-01-01T12:00:00Z"},
    {"state": "verified", "publication_status": "pending_validation"},
    {"state": "pending", "requested_state": "verified", "publication_status": "published"},
])
def test_invalid_publication_and_component_claims_are_rejected_before_acceptance(authority, change):
    h, web_store, _, service = authority
    request = intent(authority)
    body = request.model_dump(mode="json")
    body["review"].update(change)
    with client_for(service) as client:
        response = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert response.status_code == 422
    assert web_store.get_safety_review_operation(m.MonitoringContext(**h.context()), request.request_id) is None


def test_admin_role_cannot_turn_a_controller_component_into_a_web_writer(authority):
    h, web_store, controller, service = authority
    request = intent(authority)
    service._monitoring_service = MonitoringService(controller, tenant_id=h.control.tenant_id)
    with client_for(service) as client:
        response = client.post(
            f"{PREFIX}/safety-reviews", json=request.model_dump(mode="json"), headers=headers("admin"),
        )
    assert response.status_code == 503
    assert response.json()["code"] == "monitoring_component_mismatch"
    assert web_store.get_safety_review_operation(
        m.MonitoringContext(**h.context()), request.request_id,
    ) is None


def test_actor_binding_idempotency_and_explicit_pending_representation(authority):
    h, web_store, _, service = authority
    request = intent(authority)
    body = request.model_dump(mode="json")
    body["review"].update(
        state="pending", requested_state="verified", publication_status="pending_validation",
        exact_correlation_verified=False,
    )
    with client_for(service) as client:
        first = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        replay = client.post(f"{PREFIX}/safety-reviews", json=body, headers=headers("admin"))
        assert first.status_code == replay.status_code == 200
        assert first.json() == replay.json() and first.json()["reviewer_id"] == ADMIN.id
        assert first.json()["publication_status"] == "pending_validation"
        different_actor = client.post(
            f"{PREFIX}/safety-reviews", json=body, headers=headers("other-admin"),
        )
        assert different_actor.status_code == 409
        contradictory = request.model_dump(mode="json")
        conflict = client.post(f"{PREFIX}/safety-reviews", json=contradictory, headers=headers("admin"))
        assert conflict.status_code == 409
        operation = web_store.get_safety_review_operation(
            m.MonitoringContext(**h.context()), request.request_id,
        )
        assert operation.review == m.SafetyReview.model_validate(first.json())
        assert operation.expected == request.expected


def test_web_discovery_queues_only_a_handoff_until_controller_publication(authority):
    h, web_store, controller, service = authority
    context = m.MonitoringContext(**h.context())
    request_id = h.next_id()
    selector = m.ScopeSelector(
        tenant_id=h.control.tenant_id, kind="workspace", workspace_id=h.targets[0].workspace_id,
    )
    body = {
        "expected": h.version.model_dump(mode="json"), "idempotency_id": request_id,
        "selector": selector.model_dump(mode="json"),
    }
    with client_for(service) as client:
        response = client.post(f"{PREFIX}/inventory/refresh", json=body, headers=headers("admin"))
        assert response.status_code == 200, response.text
        replay = client.post(f"{PREFIX}/inventory/refresh", json=body, headers=headers("admin"))
        assert replay.json() == response.json()
        handoff = web_store.get_work(context, response.json()["work_id"])
        assert handoff.kind == "reconcile_state" and handoff.target is None
        assert handoff.reconcile_request_id == request_id
        assert not controller.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=uid(902), kinds=("triage",), limit=1, per_workspace_limit=1,
        ))
        assert all(result.state == "published" for result in publish(authority))
        assert web_store.get_work(context, handoff.work_id).state == "completed"
        produced = [row for row in h.state.records.values() if row.kind == "work" and row.work_kind == "inventory"]
        assert len(produced) == 1
        work = m.MonitoringWork.model_validate_json(produced[0].payload)
        assert work.discovery_selector == selector


def test_scope_disable_accepts_intent_but_command_selection_waits_for_publication(authority):
    from test_monitoring_phase_regressions import incomplete_catalogue

    h, web_store, _, service = authority
    incomplete_catalogue(h, "domains")
    preview_request = m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(),
        scope=m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}),
    )
    with client_for(service) as client:
        preview = client.post(
            f"{PREFIX}/scopes/preview", json=preview_request.model_dump(mode="json"),
            headers=headers("admin"),
        )
        assert preview.status_code == 200
        assert preview.json()["status"] == "ready"
        assert preview.json()["inventory_completeness"] == "partial"
        activation = client.post(
            f"{PREFIX}/plans/{preview.json()['plan_id']}/activate",
            json={
                "expected": h.version.model_dump(mode="json"),
                "idempotency_id": preview_request.idempotency_id,
            },
            headers=headers("admin"),
        )
        assert activation.status_code == 200, activation.text
        assert activation.json()["state"] == "configuring"
        assert web_store.resolve_target(h.targets[0]) is None
        assert service.target_views(READER) == []
        assert all(result.state == "published" for result in publish(authority))
        assert web_store.resolve_target(h.targets[0], include_inactive=True).state == "paused"
        assert service.target_views(READER) == []
        assert service.history.commands() == []
