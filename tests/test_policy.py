"""Policy is the safety story. These tests are the proof it is real."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from triage.policy import (
    REMEDIATION_ACTIONS,
    REPORTING_ACTIONS,
    PolicyLedger,
    PolicyViolation,
    TriagePolicy,
)
from triage.settings import Settings


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_single_remediation_is_permitted() -> None:
    ledger = PolicyLedger(TriagePolicy(max_write_actions=1))
    ledger.charge_tool_call("refresh_powerbi_dataset")
    assert ledger.write_actions == 1


def test_second_remediation_is_refused() -> None:
    ledger = PolicyLedger(TriagePolicy(max_write_actions=1))
    ledger.charge_tool_call("refresh_powerbi_dataset")

    with pytest.raises(PolicyViolation) as exc:
        ledger.charge_tool_call("refresh_powerbi_dataset")

    assert exc.value.kind == "policy_blocked"
    assert ledger.write_actions == 1
    assert ledger.blocked_attempts == ["refresh_powerbi_dataset"]


def test_reporting_actions_do_not_consume_the_remediation_budget() -> None:
    """Reporting must survive a spent budget, or the agent goes silent when it matters."""
    ledger = PolicyLedger(TriagePolicy(max_write_actions=1))
    ledger.charge_tool_call("refresh_powerbi_dataset")

    for action in ("write_data_quality_flag", "notify_teams", "report_resolution"):
        ledger.charge_tool_call(action)

    assert ledger.write_actions == 1
    assert ledger.blocked_attempts == []


def test_unknown_action_is_refused_and_recorded() -> None:
    ledger = PolicyLedger(TriagePolicy())

    with pytest.raises(PolicyViolation) as exc:
        ledger.charge_tool_call("delete_dataset")

    assert exc.value.kind == "policy_blocked"
    assert "not on the allowlist" in exc.value.message
    assert ledger.blocked_attempts == ["delete_dataset"]


def test_unknown_action_never_consumes_a_tool_call_slot() -> None:
    """Allowlist check precedes budget accounting, so a refusal costs nothing."""
    ledger = PolicyLedger(TriagePolicy(max_tool_calls=2))
    with pytest.raises(PolicyViolation):
        ledger.charge_tool_call("delete_dataset")
    assert ledger.tool_calls == 0


def test_turn_limit_is_enforced() -> None:
    ledger = PolicyLedger(TriagePolicy(max_llm_turns=2))
    ledger.charge_llm_turn()
    ledger.charge_llm_turn()

    with pytest.raises(PolicyViolation) as exc:
        ledger.charge_llm_turn()
    assert exc.value.kind == "max_turns_exceeded"


def test_tool_call_limit_is_enforced() -> None:
    ledger = PolicyLedger(TriagePolicy(max_tool_calls=2))
    ledger.charge_tool_call("get_request_context")
    ledger.charge_tool_call("get_known_incidents")

    with pytest.raises(PolicyViolation) as exc:
        ledger.charge_tool_call("notify_teams")
    assert exc.value.kind == "budget_exceeded"


def test_token_budget_is_enforced() -> None:
    ledger = PolicyLedger(TriagePolicy(max_tokens=1000))
    ledger.charge_tokens(600)

    with pytest.raises(PolicyViolation) as exc:
        ledger.charge_tokens(600)
    assert exc.value.kind == "budget_exceeded"


def test_wall_clock_timeout_is_enforced() -> None:
    clock = FakeClock()
    ledger = PolicyLedger(TriagePolicy(wall_clock_timeout_seconds=60), clock=clock)
    ledger.charge_llm_turn()

    clock.advance(61)
    with pytest.raises(PolicyViolation) as exc:
        ledger.charge_llm_turn()
    assert exc.value.kind == "timed_out"


def test_policy_reads_settings_rather_than_hardcoding() -> None:
    """A limit an operator can set but the code ignores is worse than no limit."""
    settings = Settings(
        triage_max_llm_turns=3,
        triage_max_tool_calls=4,
        triage_max_write_actions=2,
        triage_max_tokens=1234,
        triage_timeout_seconds=45,
    )
    policy = TriagePolicy.from_settings(settings)

    assert policy.max_llm_turns == 3
    assert policy.max_tool_calls == 4
    assert policy.max_write_actions == 2
    assert policy.max_tokens == 1234
    assert policy.wall_clock_timeout_seconds == 45


def test_action_taxonomy_is_disjoint() -> None:
    """An action counted as both remediation and reporting would be ambiguous."""
    assert not (REMEDIATION_ACTIONS & REPORTING_ACTIONS)


def test_policy_is_immutable() -> None:
    policy = TriagePolicy()
    with pytest.raises(FrozenInstanceError):
        policy.max_write_actions = 99  # type: ignore[misc]
