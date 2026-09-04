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


def test_no_storage_endpoint_yields_an_in_process_store() -> None:
    """Offline, one process is all there is, so in-memory is the honest answer."""
    claims = build_claim_store(endpoint="")

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
