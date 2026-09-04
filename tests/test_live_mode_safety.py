"""Live mode must fail loudly rather than quietly do nothing.

Every test here guards a property that was false at some point, and every one of
them would have passed against the broken code if written less specifically.

The theme: a mock is the right thing offline and a lie in a live deployment.
``MockPowerBIClient`` reports a refresh as ``Completed`` and ``MockTeamsNotifier``
reports ``delivered: True``, so a live deployment that fell back to them recorded
"resolved, refresh succeeded, Teams notified" having done none of it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from triage.runner import TriageRunner
from triage.settings import Settings
from triage.tools.inbox import mailbox_scope_refusal
from triage.tools.teams import MockTeamsNotifier, UnconfiguredTeamsNotifier

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Every setting these tests care about, blanked. `Settings` reads `.env`, so a
#: developer with a configured tenant would otherwise see different results from
#: CI -- this suite passed in a repository with no `.env` and failed in one with
#: a real GRAPH_CLIENT_ID, having asserted on which variables were reported
#: missing. A test whose result depends on the machine it runs on is not a test.
_BLANK: dict[str, object] = {
    "graph_tenant_id": "",
    "graph_client_id": "",
    "graph_client_secret": "",
    "graph_mailbox": "",
    "graph_canary_mailbox": "",
    "powerbi_tenant_id": "",
    "powerbi_client_id": "",
    "powerbi_client_secret": "",
    "powerbi_workspace_id": "",
    "powerbi_dataset_id": "",
    "teams_webhook_url": "",
    "incident_table_endpoint": "",
    "approval_callback_url": "",
}


def _live(**overrides: object) -> Settings:
    """Settings that ask for live tools, with nothing configured by default."""
    return Settings(
        triage_tool_mode="live",
        triage_provider_mode="mock",
        **{**_BLANK, **overrides},
    )


def _runner(settings: Settings) -> TriageRunner:
    return TriageRunner(settings=settings, base_dir=REPO_ROOT)


def test_live_power_bi_without_a_tenant_refuses_to_build() -> None:
    """Missing configuration must not silently produce a mock Power BI client.

    The mock reports every refresh as ``Completed``. A live deployment missing
    POWERBI_TENANT_ID would have triggered no refresh, reported success, and
    persisted a terminal outcome that was a fabrication.
    """
    runner = _runner(_live())

    with pytest.raises(ValueError) as error:
        runner.build_powerbi()

    assert "POWERBI_TENANT_ID" in str(error.value)
    assert "report success for work that never happened" in str(error.value)


def test_live_inbox_without_graph_configuration_refuses_to_build() -> None:
    """Every Graph setting is required, not just the tenant.

    The original check tested only ``graph_tenant_id``, so a deployment with a
    tenant and no client secret still got a MockInbox reading sample emails off
    disk -- and would have triaged them as though they had just arrived.
    """
    runner = _runner(_live(graph_tenant_id="11111111-1111-1111-1111-111111111111"))

    with pytest.raises(ValueError) as error:
        runner.build_inbox()

    message = str(error.value)
    assert "GRAPH_CLIENT_ID" in message
    assert "GRAPH_CLIENT_SECRET" in message


def test_live_health_client_without_a_tenant_refuses_to_build() -> None:
    """A mock silent-failure detector reports every model healthy."""
    runner = _runner(_live())

    with pytest.raises(ValueError) as error:
        runner.build_health_client()

    assert "POWERBI_TENANT_ID" in str(error.value)


def test_mock_mode_still_builds_everything_without_configuration() -> None:
    """The offline path must stay configuration-free. It is the evaluation path."""
    runner = _runner(Settings(triage_tool_mode="mock", **_BLANK))

    assert runner.build_powerbi() is not None
    assert runner.build_inbox() is not None
    assert runner.build_health_client() is not None
    assert isinstance(runner.build_teams(), MockTeamsNotifier)


async def test_live_teams_without_a_webhook_reports_not_delivered() -> None:
    """Not delivering is survivable. Claiming to have delivered is not.

    This slot held ``MockTeamsNotifier``, which returns ``delivered: True``. The
    controller announces an incident once, counted against ``notified_count``,
    so a fabricated delivery consumed the single announcement and suppressed the
    *first real* notification after someone fixed the webhook.
    """
    runner = _runner(_live(teams_webhook_url=""))
    notifier = runner.build_teams()

    assert isinstance(notifier, UnconfiguredTeamsNotifier)

    result = await notifier.post_card({"type": "AdaptiveCard"})
    assert result["delivered"] is False
    assert "TEAMS_WEBHOOK_URL" in result["reason"]


# ---------------------------------------------------------------------------
# Mailbox scope: all three unproven cases must refuse
# ---------------------------------------------------------------------------

MAILBOX = "bi-alerts@contoso.com"
CANARY = "someone-else@contoso.com"


def test_scope_check_refuses_when_no_canary_is_configured() -> None:
    """Unset is the shipped default, so this was the common case.

    App-only Mail.Read is tenant-wide until Exchange scopes it. With no canary
    the check was skipped entirely and mail was read anyway, while the comment
    above it and the documentation both said it failed closed.
    """
    refusal = mailbox_scope_refusal(scope={}, canary_mailbox="", mailbox=MAILBOX)

    assert refusal is not None
    assert "GRAPH_CANARY_MAILBOX" in refusal


def test_scope_check_refuses_when_the_check_did_not_complete() -> None:
    """Inconclusive is not proof. It previously fell through to reading mail."""
    refusal = mailbox_scope_refusal(
        scope={"checked": False, "reason": "Graph returned 503"},
        canary_mailbox=CANARY,
        mailbox=MAILBOX,
    )

    assert refusal is not None
    assert "did not complete" in refusal
    assert "503" in refusal


def test_scope_check_refuses_when_the_agent_can_read_the_canary() -> None:
    """The case that always worked: proof the app registration is unscoped."""
    refusal = mailbox_scope_refusal(
        scope={"checked": True, "scoped": False, "reason": "read 3 messages"},
        canary_mailbox=CANARY,
        mailbox=MAILBOX,
    )

    assert refusal is not None
    assert "not confined" in refusal


def test_scope_check_allows_mail_only_when_scoping_is_proven() -> None:
    """The one path that may read mail, and the negative control for the rest."""
    assert (
        mailbox_scope_refusal(
            scope={"checked": True, "scoped": True},
            canary_mailbox=CANARY,
            mailbox=MAILBOX,
        )
        is None
    )


# ---------------------------------------------------------------------------
# Untrusted ids
# ---------------------------------------------------------------------------


def test_configured_ids_outrank_ids_supplied_by_the_alert() -> None:
    """The alert is an email, and the sender of an email is trivially forged.

    An attacker who gets one message past the inbox filter could otherwise name
    any workspace or dataset and have the agent act on it, bounded only by what
    the controller's identity happens to reach.
    """
    resolved = TriageRunner._resolve_id(
        "",                                          # no scenario
        "configured-workspace",                      # configuration
        "workspace-from-the-email",                  # the alert
        untrusted="workspace-from-the-email",
        label="workspace",
    )

    assert resolved == "configured-workspace"


def test_the_alert_is_still_used_when_nothing_is_configured() -> None:
    """A deployment watching many models leaves the ids unset.

    That case is steerable by construction, so it must keep working rather than
    break, and the disagreement is logged instead of hidden.
    """
    resolved = TriageRunner._resolve_id(
        "",
        "",
        "workspace-from-the-email",
        untrusted="workspace-from-the-email",
        label="workspace",
    )

    assert resolved == "workspace-from-the-email"


def test_a_scenario_still_outranks_configuration() -> None:
    """Scenario files are local and trusted; they pin the whole run."""
    resolved = TriageRunner._resolve_id(
        "scenario-workspace",
        "configured-workspace",
        "workspace-from-the-email",
        untrusted="workspace-from-the-email",
        label="workspace",
    )

    assert resolved == "scenario-workspace"
