from __future__ import annotations

from datetime import timedelta

import pytest
from test_monitoring_sql_store import SqlHarness
from test_monitoring_store import Harness

from triage.monitoring import models as m
from triage.monitoring.contracts import MonitoringConflict


@pytest.mark.parametrize("backend", ("memory", "sql"))
def test_configuration_outcome_hash_is_bound_to_reserved_intent(tmp_path, backend) -> None:
    h = Harness() if backend == "memory" else SqlHarness(tmp_path / "configuration.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    review = h.review("reenable_refresh_schedule", {"enabled": True})
    h.store.observe_source(h.observation(
        execution=m.SourceExecutionIdentity(
            target=h.targets[0], run_id_kind="powerbi_request", run_id=h.next_id(),
        ),
        status="succeeded", started_at=h.clock() - timedelta(minutes=2),
        ended_at=h.clock() - timedelta(minutes=1),
    ), work_id=h.work.work_id, lease=h.work.lease)
    request = h.reserve_request(review)
    action = h.store.reserve_action(request).reservation
    submitted = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state="submitted", configuration_action="reenable_refresh_schedule",
        submitted_at=h.clock(), next_verification_at=h.clock() + timedelta(seconds=1),
        detail="Configuration request accepted, not a workload job.",
    ))

    def report(expected_hash):
        return m.ActionOutcomeRequest(
            **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
            expected_reservation_revision=submitted.revision, action_fence=action.fence,
            disposition="uncertain", observed_at=h.clock(), detail="Schedule readback.",
            configuration=m.ConfigurationVerification(
                target=h.targets[0], action="reenable_refresh_schedule",
                expected_hash=expected_hash, configuration={"enabled": False},
                observed_at=h.clock(), authority="rest",
            ),
        )

    # A caller cannot redefine the approved intent to make a disabled schedule
    # match. Rejection must leave both action state and budget unchanged.
    with pytest.raises(MonitoringConflict, match="reserved mutation"):
        h.store.record_action_outcome(report(m._digest({"enabled": False})))
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id) == submitted
    assert h.store.get_incident_state(action.request.incident).action_count == 1

    current = h.store.record_action_outcome(report(submitted.request.parameter_hash))
    assert current.state == "uncertain"
    assert current.configuration.configuration == {"enabled": False}
    assert current.configuration.expected_hash == review.parameter_hash
    assert not current.configuration.matches
    assert current.submitted_execution is None
    assert h.store.get_incident_state(action.request.incident).action_count == 1
