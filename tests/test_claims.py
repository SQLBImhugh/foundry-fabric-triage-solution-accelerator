"""Exactly one caller may act on an alert, even when two are looking at it.

The controller checks `seen()` when it reads the mail and calls
`mark_processed` only after the outcome is persisted. Everything between those
two points is a window in which a second invocation sees the same message as
untriaged.

That window is reachable without anything exotic: a manual `azd ai agent invoke`
overlapping a scheduled sweep, or two hosted replicas. Both would trigger the
same refresh. The write-action budget does not help, because it is per run and
these are two runs.
"""

from __future__ import annotations

import time

from triage.store.claims import DEFAULT_LEASE_SECONDS, InMemoryClaimStore, build_claim_store


def test_only_the_first_caller_gets_the_claim() -> None:
    """The property the whole module exists for."""
    claims = InMemoryClaimStore()

    assert claims.claim("message:abc") is True
    assert claims.claim("message:abc") is False, "two callers both got the claim"


def test_different_messages_do_not_contend() -> None:
    """A claim is per alert, not a global lock: sweeps stay parallel."""
    claims = InMemoryClaimStore()

    assert claims.claim("message:one") is True
    assert claims.claim("message:two") is True


def test_releasing_lets_the_next_caller_proceed() -> None:
    """A retry of a failed run must not wait out the lease."""
    claims = InMemoryClaimStore()

    assert claims.claim("message:abc") is True
    claims.release("message:abc")
    assert claims.claim("message:abc") is True


def test_an_expired_claim_is_available_again() -> None:
    """A container that crashes mid-remediation must not hold a lock for ever.

    Without expiry, one crash would block that alert permanently, which turns a
    duplicate-work bug into a lost-alert bug.
    """
    claims = InMemoryClaimStore()

    assert claims.claim("message:abc", lease_seconds=0) is True
    time.sleep(0.01)
    assert claims.claim("message:abc") is True


def test_a_live_claim_is_not_stolen_early() -> None:
    """The negative control for expiry: a held lease must actually hold."""
    claims = InMemoryClaimStore()

    assert claims.claim("message:abc", lease_seconds=DEFAULT_LEASE_SECONDS) is True
    assert claims.claim("message:abc") is False


def test_no_database_configured_yields_an_in_process_store() -> None:
    """Offline, one process is all there is, so in-memory is the honest answer."""
    claims = build_claim_store(db=None)

    assert isinstance(claims, InMemoryClaimStore)
    assert claims.is_durable is False


def test_a_simulated_race_produces_exactly_one_winner() -> None:
    """Many threads, one message, one winner.

    Written with real threads rather than by asserting on the implementation,
    because the bug being guarded is a race and a single-threaded test would not
    have caught it.
    """
    import threading

    claims = InMemoryClaimStore()
    won: list[int] = []
    barrier = threading.Barrier(16)

    def contend(index: int) -> None:
        barrier.wait()
        if claims.claim("message:contested"):
            won.append(index)

    threads = [threading.Thread(target=contend, args=(i,)) for i in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(won) == 1, f"{len(won)} callers claimed the same message"


# --- the deferred-retry drain -------------------------------------------------


class _RefusingClaims:
    """A claim store where somebody else already holds everything."""

    def __init__(self) -> None:
        self.asked: list[str] = []
        self.released: list[str] = []

    def claim(self, key: str, **_kw) -> bool:
        self.asked.append(key)
        return False

    def release(self, key: str) -> None:  # pragma: no cover - never reached
        self.released.append(key)


def _retry_runner(refreshes: list[str], tmp_path, settings):
    """Exercise the real retry admission path with explicit fixture collaborators."""
    from triage.runner import TriageRunner
    from triage.store.incidents import InMemoryIncidentStore
    from triage.store.retries import InMemoryRetryStore
    from triage.tools.powerbi import MockPowerBIClient

    class _PowerBI(MockPowerBIClient):
        async def refresh_dataset(self, workspace_id: str, dataset_id: str):
            refreshes.append(dataset_id)
            return await super().refresh_dataset(workspace_id, dataset_id)

    fake = TriageRunner(settings, base_dir=tmp_path, store=InMemoryIncidentStore())
    fake.retries = InMemoryRetryStore()
    fake.retries.defer(
        signature="sig-1", dataset_id="ds-1", workspace_id="ws-1",
        report_name="R", request_id="r-1",
    )
    fake.retries._items["sig-1"]["due_at"] = "2000-01-01T00:00:00+00:00"
    fake.build_powerbi = lambda: _PowerBI(latency_ms=0)
    return fake, TriageRunner.drain_due_retries


def test_a_claimed_retry_is_not_drained_twice(tmp_path, test_settings) -> None:
    """A deferred retry issues a real dataset refresh, and `due` and `complete`
    are separate statements — so two replicas draining at the same moment both
    see the row as due. The mailbox path has always claimed per message; the
    retry path acts without a message and needs the same guard.
    """
    import asyncio

    refreshes: list[str] = []
    fake, drain = _retry_runner(refreshes, tmp_path, test_settings)
    claims = _RefusingClaims()

    lines = asyncio.run(drain(fake, claims=claims))

    assert claims.asked == ["retry:sig-1"], "the drain did not try to claim"
    assert refreshes == [], "refreshed a dataset another invocation had claimed"
    assert lines == []


def test_an_unclaimed_retry_still_runs_and_releases(tmp_path, test_settings) -> None:
    """The guard must not stop the ordinary single-instance path."""
    import asyncio

    refreshes: list[str] = []
    fake, drain = _retry_runner(refreshes, tmp_path, test_settings)
    claims = InMemoryClaimStore()

    lines = asyncio.run(drain(fake, claims=claims))

    from triage.monitoring.runtime import fixture_target

    assert refreshes == [fixture_target("powerbi", "ws-1", "ds-1").item_id]
    assert any("deferred retry completed" in ln for ln in lines)
    # Released, so a later drain in the same process is not locked out.
    assert claims.claim("retry:sig-1") is True


def test_the_retry_claim_is_held_until_the_row_is_finished(tmp_path, test_settings) -> None:
    """Releasing at the refresh would let a second drainer see the row as still
    due and fire again before it was marked complete."""
    import asyncio

    held: list[bool] = []
    claims = InMemoryClaimStore()

    refreshes: list[str] = []
    fake, drain = _retry_runner(refreshes, tmp_path, test_settings)

    original = fake.retries.complete

    def complete(*a, **kw):
        # A second drainer trying to claim at this exact moment must lose.
        held.append(InMemoryClaimStore.claim(claims, "retry:sig-1") is False)
        return original(*a, **kw)

    fake.retries.complete = complete
    asyncio.run(drain(fake, claims=claims))

    assert held == [True], "the claim was released before the row was completed"
