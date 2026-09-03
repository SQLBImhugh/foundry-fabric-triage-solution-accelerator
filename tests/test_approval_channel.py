"""Tests for the approval path a human can actually use.

The gate, the fingerprint binding and the fail-closed behaviour were already
tested. What was missing was anything a person could answer: ``runner`` only
ever built the scripted gates, and ``TeamsCardApprovalGate`` was constructed
nowhere but the tests, with no decision source to poll. The approval branch was
demonstrated by a YAML field, not a decision.

These cover the parts that make it real: a decision written by somebody else,
in another process, arriving mid-poll -- and the two ways that could quietly
fail to work at all.
"""

from __future__ import annotations

import asyncio

import pytest

from triage.approvals import (
    ApprovalRequest,
    TeamsCardApprovalGate,
)
from triage.policy import PolicyLedger, PolicyViolation, TriagePolicy
from triage.store.approvals import (
    InMemoryApprovalChannel,
    JsonFileApprovalChannel,
)
from triage.tools.teams import MockTeamsNotifier

REBIND = "rebind_dataset_gateway"


def make_request(**overrides) -> ApprovalRequest:
    base = {
        "action": REBIND,
        "arguments": {"target_gateway": "gw-onprem-02"},
        "justification": "Third consecutive gateway timeout.",
        "request_id": "req-approval-1",
        "report_name": "Orders Daily Rollup",
        "impact": "Affects every dataset bound to gw-onprem-02.",
    }
    base.update(overrides)
    return ApprovalRequest(**base)


# ---------------------------------------------------------------------------
# The channel: somebody else writes the answer
# ---------------------------------------------------------------------------


async def test_a_request_has_no_answer_until_somebody_gives_one() -> None:
    channel = InMemoryApprovalChannel()
    request = make_request()
    channel.open(request)

    assert await channel.poll(request.request_id) is None
    assert [r["request_id"] for r in channel.pending()] == [request.request_id]

    channel.decide(request.request_id, decision="approve", responder="priya")

    answer = await channel.poll(request.request_id)
    assert answer is not None
    assert answer["decision"] == "approve"
    assert answer["responder"] == "priya"
    assert channel.pending() == []


def test_a_decision_against_an_unknown_request_is_refused() -> None:
    """Otherwise it sits in the table looking authoritative.

    The first request that happened to reuse the id would consume it.
    """
    with pytest.raises(KeyError):
        InMemoryApprovalChannel().decide("never-asked", decision="approve", responder="x")


def test_answering_twice_is_refused() -> None:
    """The second answer is not the one that took effect, so say so."""
    channel = InMemoryApprovalChannel()
    request = make_request()
    channel.open(request)
    channel.decide(request.request_id, decision="decline", responder="sam")

    with pytest.raises(ValueError, match="already answered"):
        channel.decide(request.request_id, decision="approve", responder="sam")


async def test_a_decision_written_by_another_process_is_visible(tmp_path) -> None:
    """The CLI and the agent are not the same process. That is the point."""
    path = tmp_path / "approvals.json"
    request = make_request()

    JsonFileApprovalChannel(path).open(request)
    # A separate instance, as `bi-triage approve` would construct.
    JsonFileApprovalChannel(path).decide(
        request.request_id, decision="approve", responder="priya"
    )

    answer = await JsonFileApprovalChannel(path).poll(request.request_id)
    assert answer is not None and answer["decision"] == "approve"


# ---------------------------------------------------------------------------
# The gate, end to end
# ---------------------------------------------------------------------------


async def test_the_gate_returns_a_decision_that_arrives_while_it_waits() -> None:
    """The whole path: card posted, agent polling, human answers, agent proceeds."""
    channel = InMemoryApprovalChannel()
    notifier = MockTeamsNotifier()
    gate = TeamsCardApprovalGate(notifier, decision_source=channel, poll_seconds=0.01)
    request = make_request(timeout_seconds=5)

    async def answer_shortly() -> None:
        for _ in range(200):
            if channel.get(request.request_id) is not None:
                channel.decide(request.request_id, decision="approve", responder="priya")
                return
            await asyncio.sleep(0.01)
        raise AssertionError("the gate never registered the request")

    decision, _ = await asyncio.gather(gate.request_approval(request), answer_shortly())

    assert decision.granted
    assert decision.decided_by == "priya"
    valid, why = decision.is_valid_for(request)
    assert valid, why
    assert notifier.cards or notifier.messages, "the human was never actually asked"


async def test_a_decline_is_a_decline() -> None:
    channel = InMemoryApprovalChannel()
    gate = TeamsCardApprovalGate(
        MockTeamsNotifier(), decision_source=channel, poll_seconds=0.01
    )
    request = make_request(timeout_seconds=5)

    async def answer_shortly() -> None:
        for _ in range(200):
            if channel.get(request.request_id) is not None:
                channel.decide(
                    request.request_id,
                    decision="decline",
                    responder="sam",
                    reason="Rebinding during month end.",
                )
                return
            await asyncio.sleep(0.01)

    decision, _ = await asyncio.gather(gate.request_approval(request), answer_shortly())

    assert not decision.granted
    assert decision.outcome == "denied"
    assert "month end" in decision.reason


async def test_the_card_with_its_buttons_is_what_actually_gets_posted() -> None:
    """The gate falls back to a plain summary when the notifier cannot take a card.

    That is exactly what happened: ``WorkflowsWebhookTeamsNotifier`` had no
    ``post_card``, so the approval card was built, tested, and then silently
    replaced with a text summary on the way out. The human got a notification
    with nothing to click, and every test still passed because they all
    inspected ``build_card`` rather than what was delivered.
    """
    notifier = MockTeamsNotifier()
    channel = InMemoryApprovalChannel()
    gate = TeamsCardApprovalGate(
        notifier,
        decision_source=channel,
        poll_seconds=0.01,
        callback_url="https://example.invalid/flow",
    )
    request = make_request(timeout_seconds=1)

    await gate.request_approval(request)

    assert notifier.cards, "the card was not delivered; a summary went instead"
    actions = notifier.cards[0]["attachments"][0]["content"]["actions"]
    assert [a["title"] for a in actions] == ["Approve", "Decline"]
    assert request.fingerprint in actions[0]["url"]


async def test_the_gate_registers_the_request_before_asking() -> None:
    """Without this a decision can arrive against a request nobody recorded.

    It also means `bi-triage approvals` shows what is waiting even if the
    card never reached the channel.
    """
    channel = InMemoryApprovalChannel()
    gate = TeamsCardApprovalGate(
        MockTeamsNotifier(), decision_source=channel, poll_seconds=0.01
    )
    request = make_request(timeout_seconds=1)

    await gate.request_approval(request)

    assert channel.get(request.request_id) is not None


async def test_an_unanswered_request_times_out_and_is_not_granted() -> None:
    channel = InMemoryApprovalChannel()
    gate = TeamsCardApprovalGate(
        MockTeamsNotifier(), decision_source=channel, poll_seconds=0.01
    )
    decision = await gate.request_approval(make_request(timeout_seconds=1))

    assert not decision.granted
    assert decision.outcome == "timed_out"


# ---------------------------------------------------------------------------
# The card has to offer a control that works
# ---------------------------------------------------------------------------


def test_without_a_callback_the_card_offers_no_dead_buttons() -> None:
    """An incoming webhook has no bot, so Action.Submit does nothing.

    A button that silently does nothing is worse than no button: it looks like
    the decision was recorded.
    """
    card = TeamsCardApprovalGate(MockTeamsNotifier()).build_card(make_request())
    content = card["attachments"][0]["content"]

    assert content["actions"] == []
    assert "bi-triage approve" in str(content), "the card must say how to answer"


def test_with_a_callback_the_buttons_are_links_carrying_the_binding() -> None:
    gate = TeamsCardApprovalGate(
        MockTeamsNotifier(), callback_url="https://example.invalid/flow?api-version=1"
    )
    request = make_request()
    actions = gate.build_card(request)["attachments"][0]["content"]["actions"]

    assert [a["type"] for a in actions] == ["Action.OpenUrl", "Action.OpenUrl"]
    approve = next(a for a in actions if a["title"] == "Approve")

    assert request.fingerprint in approve["url"]
    assert request.request_id in approve["url"]
    assert "decision=approve" in approve["url"]
    # The callback already had a query string; it must not be broken.
    assert "?api-version=1&" in approve["url"]


# ---------------------------------------------------------------------------
# Waiting on a person must not spend the agent's budget
# ---------------------------------------------------------------------------


class _Clock:
    """Manual clock, so the test does not actually sleep for five minutes."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_human_wait_does_not_consume_the_wall_clock() -> None:
    """The failure this prevents is subtle and total.

    With the run timeout and the approval timeout both at 300s, an honest
    approval kills the run as `timed_out` at the moment it was granted -- so
    the gated action can never succeed, for a reason that has nothing to do
    with the decision.
    """
    clock = _Clock()
    ledger = PolicyLedger(TriagePolicy(wall_clock_timeout_seconds=300), clock=clock)

    clock.now = 20.0
    with ledger.awaiting_human():
        clock.now = 260.0  # a person took four minutes to answer

    assert ledger.human_wait_seconds == pytest.approx(240.0)
    assert ledger.elapsed_seconds == pytest.approx(20.0)
    ledger.check_deadline()  # must not raise

    clock.now = 561.0  # 301s of actual agent time
    with pytest.raises(PolicyViolation, match="timed_out|Wall-clock"):
        ledger.check_deadline()


def test_the_agent_is_still_charged_for_everything_else_while_waiting() -> None:
    """Only the clock pauses. Turns and tokens are the agent's consumption."""
    clock = _Clock()
    ledger = PolicyLedger(TriagePolicy(max_llm_turns=2), clock=clock)

    ledger.charge_llm_turn()
    with ledger.awaiting_human():
        clock.now = 120.0
    ledger.charge_llm_turn()

    with pytest.raises(PolicyViolation, match="max_llm_turns"):
        ledger.charge_llm_turn()
