"""Presence evidence that expires during preparation is a decision, not an outage.

A source-removal supersession is authorized by a presence inspection valid for
SUPERSESSION_EVIDENCE_TTL_SECONDS. The reconciliation path checks that once, up
front, and then prepares the publication -- which performs three estate-wide
monitoring snapshots before it re-checks the same inspection
(``provisioning.py`` ``_publication_state`` twice, then the TTL read). Evidence
that was fresh at the first check can be expired by the last one.

The refusal raised there is a ``ProvisioningReview``. Nothing caught it, so on
the SQL store the transaction catch-all reported ``MonitoringUnavailable``
-- "shared state could not be reached" -- and the work was retried rather than
resolved. Nothing can make the evidence younger, so every retry produced the
identical refusal. A deployed controller repeated one of these 48 times across
23 hours with event intake fenced behind it.

Expiry between the two checks is an ordinary outcome with a defined resolution:
reject the handoff and queue exactly one bounded re-observation, which is what
the early check already does. These tests drive the real controller path with a
virtual clock, and cover the baseline and freshness matrix the review required.
"""

from __future__ import annotations

from datetime import timedelta

from test_monitoring_provisioning import (
    CONTEXT,
    connector,
    factory,
)
from test_monitoring_provisioning_admission import capability, prepared_supersession

from triage.monitoring import models as m
from triage.monitoring.memory import MonitoringEngine, stable_id

__all__ = ["factory"]

#: Seconds of virtual time each estate-wide snapshot is made to consume. Three
#: snapshots run between the early freshness check and the TTL re-check, so any
#: value above a third of the remaining margin expires the evidence in flight.
SNAPSHOT_SECONDS = 3


def slow_snapshots(monkeypatch, clock, seconds: int = SNAPSHOT_SECONDS) -> None:
    """Make preparation consume time the way the real snapshots do.

    The three snapshots compute estate-wide coverage, not a clock value. This
    charges each one a fixed cost instead of pretending preparation is free,
    which is the only difference between the early check passing and the late
    check failing.
    """
    original = MonitoringEngine.snapshot

    def snapshot(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        clock.advance(seconds)
        return result

    monkeypatch.setattr(MonitoringEngine, "snapshot", snapshot)


def reobservation(store, producer_request_id: str):
    return store.get_work(CONTEXT, stable_id(CONTEXT, f"connector-reobserve:{producer_request_id}"))


async def test_unchanged_baseline_with_fresh_evidence_publishes(factory, monkeypatch):
    """Positive control: the same path with time to spare still publishes."""
    seed, controller, clock, remote, _, old_result, work, _ = await prepared_supersession(factory)
    clock.advance(1)
    capability(seed, clock, event_status="unknown", seconds=3600)
    slow_snapshots(monkeypatch, clock)

    result = controller.reconcile_work(work)

    current = connector(seed)
    assert result.state == "published"
    assert current.source_removals == () and current.sources == old_result.connector.sources
    assert controller.get_work(CONTEXT, work.work_id).state == "completed"
    assert reobservation(controller, work.reconcile_request_id) is None
    assert len(remote.update_bodies) == 1


async def test_evidence_expiring_during_preparation_is_rejected_not_an_outage(factory, monkeypatch):
    """The reproduction: fresh at the early check, expired at the TTL re-check."""
    seed, controller, clock, remote, _, _, work, _ = await prepared_supersession(factory)
    capability(seed, clock, event_status="unknown", seconds=3600)
    before = connector(seed)
    inspection = controller.get_connector_observation(CONTEXT, work.reconcile_request_id).inspection
    # Leave less margin than a single snapshot consumes, so the inspection is
    # inside its window at the early check and outside it at the re-check.
    remaining = inspection.observed_at + timedelta(seconds=m.SUPERSESSION_EVIDENCE_TTL_SECONDS) - clock()
    clock.advance(int(remaining.total_seconds()) - 1)
    assert clock() - inspection.observed_at < timedelta(seconds=m.SUPERSESSION_EVIDENCE_TTL_SECONDS)
    slow_snapshots(monkeypatch, clock)

    result = controller.reconcile_work(work)

    assert result.state == "rejected"
    assert result.producer_request_id == work.reconcile_request_id
    # The handoff is resolved, so the work stops being retried.
    assert controller.get_work(CONTEXT, work.work_id).state == "completed"
    # Exactly one bounded follow-up, keyed on the rejected handoff.
    followup = reobservation(controller, work.reconcile_request_id)
    assert followup is not None and followup.kind == "connector_reconcile"
    # Nothing was published, no removal fence cleared, no extra remote call.
    current = connector(seed)
    assert current.source_removals == before.source_removals
    assert current.revision == before.revision
    assert len(remote.update_bodies) == 1


async def test_late_expiry_follow_up_is_idempotent_across_attempts(factory, monkeypatch):
    """A repeat of the same rejection must not accumulate observations.

    One re-observation is queued per rejected producer request, so a second
    attempt on the same handoff has to resolve to the same draft rather than
    adding another. Unbounded competing observations advance the baseline and
    overtake otherwise fresh candidates.
    """
    seed, controller, clock, _, _, _, work, _ = await prepared_supersession(factory)
    capability(seed, clock, event_status="unknown", seconds=3600)
    clock.advance(m.SUPERSESSION_EVIDENCE_TTL_SECONDS + 1)
    slow_snapshots(monkeypatch, clock)

    first = controller.reconcile_work(work)
    queued = reobservation(controller, work.reconcile_request_id)
    assert first.state == "rejected" and queued is not None

    # The recorded resolution replays; it does not re-run preparation.
    assert controller.reconcile_work(work) == first
    assert reobservation(controller, work.reconcile_request_id) == queued


async def test_expiry_during_preparation_leaves_original_receipts_intact(factory, monkeypatch):
    """Rejection preserves the earlier publication receipt and its evidence."""
    seed, controller, clock, _, old_request, old_result, work, _ = await prepared_supersession(factory)
    capability(seed, clock, event_status="unknown", seconds=3600)
    original = controller.get_connector_observation(CONTEXT, work.reconcile_request_id)
    clock.advance(m.SUPERSESSION_EVIDENCE_TTL_SECONDS + 1)
    slow_snapshots(monkeypatch, clock)

    assert controller.reconcile_work(work).state == "rejected"

    assert controller.get_connector_publication(CONTEXT, old_request.request_id) == old_result
    assert controller.get_connector_observation(CONTEXT, work.reconcile_request_id) == original
    assert connector(seed).source_removals[0] == old_result.connector.source_removals[0]


async def test_preparation_does_not_repeat_the_estate_snapshot_three_times(factory, monkeypatch):
    """Cost bound: the shorter the window, the less evidence expires inside it.

    Preparation used to resolve publication state twice and then take a third
    snapshot only to read a clock. Each one computes estate-wide coverage.
    """
    seed, controller, clock, _, _, _, work, _ = await prepared_supersession(factory)
    clock.advance(1)
    capability(seed, clock, event_status="unknown", seconds=3600)
    calls = []
    original = MonitoringEngine.snapshot

    def snapshot(self, *args, **kwargs):
        calls.append(kwargs.get("context", args[0] if args else None))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(MonitoringEngine, "snapshot", snapshot)

    assert controller.reconcile_work(work).state == "published"

    assert len(calls) <= 1, f"preparation took {len(calls)} estate-wide snapshots"
