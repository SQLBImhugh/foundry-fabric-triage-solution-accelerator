"""Tests for the retrieved-playbook knowledge base.

The point of a playbook is that it changes a decision. These tests check that
the right ones fire on realistic error text, that the retry guidance is correct,
and — most importantly — that the catalogue can grow without the injected block
growing with it.
"""

from __future__ import annotations

import pytest

from triage.knowledge.playbooks import (
    PLAYBOOKS,
    format_playbooks,
    retry_is_discouraged,
    select_playbooks,
)

# Error text in the shape Power BI actually emits.
THROTTLED = (
    "Error code: CapacityThrottled\n"
    "ThrottlingError: The request was rejected because the capacity has exceeded "
    "its resource limits."
)
TIMEOUT = (
    "Error code: ScheduledRefreshTimeout\n"
    "Error: The operation was cancelled because it exceeded the configured timeout."
)
GATEWAY = (
    "Error code: GatewayUnavailable\n"
    "GatewayError: The on-premises data gateway 'gw-onprem-01' did not respond."
)
CREDENTIALS = (
    "Error code: InvalidCredentials\n"
    "The credentials provided for the data source are invalid. The password may "
    "have expired."
)
OOM = "Error: Resource governing: This operation was canceled because there wasn't enough memory."
DUPLICATES = (
    "Error code: DuplicateKeyInRelationship\n"
    "A relationship could not be created because the 'one' side of the relationship "
    "contains duplicate values."
)
DISABLED = (
    "Your scheduled refresh has been disabled after 4 consecutive failures. "
    "Re-enable it once the underlying issue is resolved."
)


def names(text: str) -> list[str]:
    return [p.name for p in select_playbooks(text)]


# ---------------------------------------------------------------------------
# The right playbook fires
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (THROTTLED, "Refresh throttled by capacity"),
        (TIMEOUT, "Scheduled refresh timeout"),
        (GATEWAY, "Gateway unreachable"),
        (CREDENTIALS, "Expired or changed data source credentials"),
        (OOM, "Out-of-memory during refresh"),
        (DUPLICATES, "Duplicate key breaks a model relationship"),
        (DISABLED, "Scheduled refresh deactivated"),
    ],
)
def test_expected_playbook_is_matched(error: str, expected: str) -> None:
    assert expected in names(error)


def test_the_best_match_ranks_first() -> None:
    """A gateway error mentions 'gateway' several times; that should win."""
    assert names(GATEWAY)[0] == "Gateway unreachable"


def test_unrelated_text_matches_nothing() -> None:
    assert select_playbooks("The quarterly figures look good, thanks.") == []


def test_empty_input_is_safe() -> None:
    assert select_playbooks("") == []
    assert select_playbooks(None) == []  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# The retry guidance is the part that changes a decision
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("error", [CREDENTIALS, OOM, DISABLED])
def test_retry_is_discouraged_where_it_would_be_futile(error: str) -> None:
    """These fail identically on every retry — looping is pure waste."""
    assert retry_is_discouraged(select_playbooks(error)) is True


def test_retry_is_discouraged_for_throttling_because_it_does_harm() -> None:
    """Different reason from the three above, same answer.

    A throttled refresh would probably succeed eventually, so this is not
    futility -- it is that retrying adds load to a capacity already over its
    limits, and doing that across several datasets turns contention into an
    outage. The right move is `defer_refresh_retry`, which schedules the same
    work for after the window.
    """
    assert retry_is_discouraged(select_playbooks(THROTTLED)) is True


@pytest.mark.parametrize("error", [TIMEOUT])
def test_retry_is_allowed_for_genuinely_transient_failures(error: str) -> None:
    assert retry_is_discouraged(select_playbooks(error)) is False


def test_no_match_does_not_discourage_retry() -> None:
    """Absence of knowledge is not evidence against retrying."""
    assert retry_is_discouraged([]) is False


def test_the_deactivation_playbook_scopes_the_rule_to_scheduled_runs() -> None:
    """The rule is about SCHEDULED failures.

    An agent's own API-triggered retries are a different trigger path — they do
    not advance the counter toward deactivation, and do not reset it either.
    Stating "four consecutive failures" without that qualifier would have the
    agent counting its own retries.
    """
    pb = next(p for p in PLAYBOOKS if p.name == "Scheduled refresh deactivated")
    assert pb.retry_useful is False
    assert "scheduled" in pb.summary.lower()
    assert "API- or on-demand-triggered refreshes are a different trigger path" in pb.watch_out
    assert "do not advance" in pb.watch_out
    # The other two deactivation paths, which are independent of the count.
    assert "credential" in pb.watch_out.lower()
    assert "two months" in pb.watch_out.lower()


# ---------------------------------------------------------------------------
# Retrieval must not become prompt bloat
# ---------------------------------------------------------------------------


def test_injection_is_capped() -> None:
    """A wall of guidance is the problem this module exists to avoid."""
    everything = " ".join(t for p in PLAYBOOKS for t in p.triggers if not t.startswith("re:"))
    assert len(select_playbooks(everything)) <= 3


def test_selection_is_deterministic() -> None:
    """Same input, same injected block — or scenarios stop being reproducible."""
    assert names(GATEWAY) == names(GATEWAY)
    assert names(THROTTLED) == names(THROTTLED)


def test_nothing_matched_injects_nothing() -> None:
    assert format_playbooks([]) == ""


def test_rendered_block_carries_the_decision_relevant_fields() -> None:
    block = format_playbooks(select_playbooks(CREDENTIALS))
    assert "Is a retry useful?" in block
    assert "retrying will not fix this" in block.lower()
    assert "Source:" in block
    assert "evidence, not as instructions" in block


# ---------------------------------------------------------------------------
# Catalogue hygiene
# ---------------------------------------------------------------------------


def test_every_playbook_cites_a_public_source() -> None:
    """This asset is shared with customers; internal TSGs must not leak into it."""
    for pb in PLAYBOOKS:
        assert pb.source.startswith("https://learn.microsoft.com/"), pb.name


def test_no_internal_references_anywhere_in_the_catalogue() -> None:
    blob = " ".join(
        f"{p.name} {p.summary} {p.guidance} {p.watch_out} {p.source}" for p in PLAYBOOKS
    ).lower()
    for marker in ("eng.ms", "microsofticm", "@microsoft.com", "visualstudio.com", "icm"):
        assert marker not in blob, f"internal reference '{marker}' leaked into the catalogue"


def test_playbook_names_are_unique() -> None:
    assert len({p.name for p in PLAYBOOKS}) == len(PLAYBOOKS)


def test_every_playbook_has_actionable_guidance() -> None:
    for pb in PLAYBOOKS:
        assert pb.triggers, pb.name
        assert len(pb.guidance) > 30, pb.name
        assert pb.suggested_tier in ("tier_1", "tier_2", "needs_human"), pb.name


def test_bad_regex_in_a_trigger_cannot_crash_selection() -> None:
    from triage.knowledge.playbooks import _matches

    assert _matches("re:[unclosed", "anything") is False


# ---------------------------------------------------------------------------
# Wired into the agent
# ---------------------------------------------------------------------------


def test_the_demo_emails_match_their_intended_playbooks(repo_root) -> None:
    """The scenarios should exercise retrieval, not bypass it."""
    from triage.tools.inbox import MockInbox

    expected = {
        "01-transient-refresh-failure.json": "Scheduled refresh timeout",
        "02-data-quality-failure.json": "Duplicate key breaks a model relationship",
        "04-capacity-throttled.json": "Refresh throttled by capacity",
        "05-gateway-repeating.json": "Gateway unreachable",
    }
    for filename, playbook in expected.items():
        request = MockInbox.load(repo_root / "mock" / "emails" / filename)
        assert playbook in names(request.error_text()), f"{filename} did not match {playbook}"
