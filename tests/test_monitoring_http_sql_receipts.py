from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from test_monitoring_api import no_network as no_network
from test_monitoring_phase_regressions import reopened, review_request
from test_monitoring_sql_store import SqlHarness
from test_monitoring_store import uid

from triage.command_center.api import create_app
from triage.command_center.models import Actor, WebSettings
from triage.command_center.monitoring import (
    MonitoringService,
    SafetyReviewOperationResponse,
    create_monitoring_router,
)
from triage.monitoring import models as m

PREFIX = "/api/monitoring"


def api(harness, tmp_path, *, store=None, role="admin"):
    service = MonitoringService(
        store or harness.store, tenant_id=harness.control.tenant_id,
    )
    assert service.store.component == "fixture"
    app = create_app(
        SimpleNamespace(monitoring=service, demo_tasks=[]),
        web_settings=WebSettings(
            _env_file=None, mode="live", tenant_id=harness.control.tenant_id,
            client_id=uid(90), static_dir=str(tmp_path / "not-built"),
        ),
    )
    # This is the explicit legacy SQL protocol fixture, not the production
    # permission kernel. Web/controller publication is tested separately.
    # JWT validation is covered by test_monitoring_api.
    app.router.routes.clear()
    app.include_router(create_monitoring_router(
        lambda: Actor(id=uid(5), display_name="Fixture operator", roles=[role]),
    ))
    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("commit", ["before", "after"])
def test_http_lost_revocation_reconciles_only_the_original_sql_receipt(tmp_path, commit):
    h = SqlHarness(tmp_path / "http-receipts.sqlite")
    h.seed()
    h.activate()
    first = review_request(h)
    with api(h, tmp_path) as client:
        created = client.post(f"{PREFIX}/safety-reviews", json=first.model_dump(mode="json"))
        assert created.status_code == 200, created.text
        saved = m.SafetyReview.model_validate(created.json())
        revoke = review_request(h, saved, revoked=True)

        def interrupt_review_commit(sql, params):
            if (
                sql.lstrip().startswith("INSERT INTO") and "monitoring_receipts" in sql
                and "safety_review" in params and revoke.request_id in params
            ):
                h.db.fail_commit = commit
            return False

        # Bootstrap inspection commits its own read transaction before the
        # mutation. Interrupt the receipt transaction, not that earlier read.
        h.db.fail_statement = interrupt_review_commit
        response = client.post(
            f"{PREFIX}/safety-reviews", json=revoke.model_dump(mode="json"),
        )
        assert response.status_code == 503
        assert response.json()["code"] == "monitoring_commit_uncertain"
        assert revoke.request_id in response.json()["message"]

    other = reopened(h)
    other._backend.db.statements.clear()
    with api(h, tmp_path, store=other, role="reader") as client:
        original = client.get(f"{PREFIX}/safety-review-operations/{first.request_id}")
        assert original.status_code == 200
        assert set(original.json()) == {"request_id", "review"}
        assert original.json()["review"] == created.json()
        result = client.get(f"{PREFIX}/safety-review-operations/{revoke.request_id}")
        current = client.get(f"{PREFIX}/safety-reviews/{saved.review_id}")
        assert current.status_code == 200
        if commit == "before":
            assert result.status_code == 404 and result.json()["code"] == "not_found"
            assert current.json()["revision"] == 1
        else:
            assert result.status_code == 200, result.text
            wire = SafetyReviewOperationResponse.model_validate(result.json())
            assert set(result.json()) == {"request_id", "review"}
            receipt = other.get_safety_review_operation(
                m.MonitoringContext(**h.context()), revoke.request_id,
            )
            assert wire.request_id == receipt.request_id == revoke.request_id
            assert wire.review == receipt.review and wire.review.state == "revoked"
            assert (receipt.expected_review_revision, receipt.new_review_revision) == (1, 2)
            assert current.json() == receipt.review.model_dump(mode="json")
            assert result.headers["Cache-Control"] == "no-store"
    assert not any(
        sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for sql, _ in other._backend.db.statements
    )


def test_http_reader_gets_immutable_sql_revocation_after_later_review_and_scope_change(tmp_path):
    h = SqlHarness(tmp_path / "later-review.sqlite")
    h.seed()
    h.activate()
    first = review_request(h)
    with api(h, tmp_path) as client:
        response = client.post(f"{PREFIX}/safety-reviews", json=first.model_dump(mode="json"))
        assert response.status_code == 200, response.text
        saved = m.SafetyReview.model_validate(response.json())
        revoke = review_request(h, saved, revoked=True)
        response = client.post(f"{PREFIX}/safety-reviews", json=revoke.model_dump(mode="json"))
        assert response.status_code == 200, response.text
        revoked = m.SafetyReview.model_validate(response.json())
        replacement = review_request(h, revoked)
        response = client.post(
            f"{PREFIX}/safety-reviews", json=replacement.model_dump(mode="json"),
        )
        assert response.status_code == 200, response.text
        assert response.json()["revision"] == 3
    h.activate(m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}))

    other = reopened(h)
    with api(h, tmp_path, store=other, role="reader") as client:
        result = client.get(f"{PREFIX}/safety-review-operations/{revoke.request_id}")
        assert result.status_code == 200, result.text
        wire = SafetyReviewOperationResponse.model_validate(result.json())
        assert set(result.json()) == {"request_id", "review"}
        receipt = other.get_safety_review_operation(
            m.MonitoringContext(**h.context()), revoke.request_id,
        )
        assert wire.request_id == receipt.request_id == revoke.request_id
        assert wire.review == receipt.review == revoked
        assert receipt.expected == revoke.expected
        current = client.get(f"{PREFIX}/safety-reviews/{saved.review_id}")
        assert current.status_code == 200 and current.json()["revision"] == 3
        denied = client.post(
            f"{PREFIX}/safety-reviews", json=replacement.model_dump(mode="json"),
        )
        assert denied.status_code == 403
