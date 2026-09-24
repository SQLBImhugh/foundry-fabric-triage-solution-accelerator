from __future__ import annotations

from test_monitoring_queue_selection import QueueFixture
from test_monitoring_workspace_fairness import protected_completion

from triage.monitoring.sql_permissions import build_permission_kernel


def test_controller_claim_does_not_enumerate_worker_evidence(monkeypatch):
    queue = QueueFixture(True)
    expected = queue.discovery()
    query = queue.db.query

    def without_evidence_enumeration(sql, *parameters):
        if "WITH recent AS" in sql or "WITH projected AS" in sql:
            assert f"FROM {queue.db.names.object('controller_read')} AS w" not in sql, (
                "Fairness must not expand the accepted-worker-evidence UNION"
            )
            assert f"FROM {queue.db.names.object('controller_read')} AS r" not in sql, (
                "Queue ranking must not expand the accepted-worker-evidence UNION"
            )
        return query(sql, *parameters)

    monkeypatch.setattr(queue.db, "query", without_evidence_enumeration)
    claimed = queue.claim()
    assert [work.work_id for work in claimed] == [expected.work_id]
    assert claimed[0].lease is not None


def test_empty_claim_does_not_scan_completed_work_or_transition_history(monkeypatch):
    queue = QueueFixture(True)
    for _ in range(30):
        protected_completion(queue, queue.work())
    query = queue.db.query

    def without_history_scan(sql, *parameters):
        assert "WITH recent AS" not in sql, "An empty queue must not reconstruct its fairness history"
        return query(sql, *parameters)

    monkeypatch.setattr(queue.db, "query", without_history_scan)
    assert queue.claim() == ()


def test_queue_projection_is_controller_read_only_and_excludes_raw_evidence():
    kernel = build_permission_kernel()
    view = next(obj for obj in kernel.objects if obj.logical_name == "controller_queue_read")
    assert "r.record_kind='work'" in view.ddl
    assert "r.record_kind='action'" in view.ddl
    assert "c.tenant_id=r.tenant_id AND c.epoch=r.epoch" in view.ddl
    assert "UNION" not in view.ddl
    assert "accepted_worker_facts" not in view.ddl
    for role in ("worker", "web"):
        assert not any(view.name in grant for grant in kernel.grants[role])
    grants = [grant for grant in kernel.grants["controller"] if view.name in grant]
    assert grants == [f"GRANT SELECT ON OBJECT::{view.name} TO [{kernel.names.role('controller')}];"]
