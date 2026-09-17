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
    "monitoring_tenant_id": "",
    "teams_webhook_url": "",
    "azure_sql_server": "",
    "azure_sql_database": "",
    "approval_callback_url": "",
}


def _live(**overrides: object) -> Settings:
    """Settings that ask for live tools, with nothing configured by default."""
    return Settings(
        _env_file=None,
        monitoring_mode="live",
        triage_tool_mode="live",
        triage_provider_mode="mock",
        **{**_BLANK, **overrides},
    )


def _runner(settings: Settings) -> TriageRunner:
    if settings.triage_tool_mode == "live":
        # Isolate client-builder contracts from deployed-store bootstrap.
        # The core monitoring tests exercise complete runner construction.
        runner = TriageRunner.__new__(TriageRunner)
        runner.settings = settings
        runner._teams = None
        return runner
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

    assert "MONITORING_TENANT_ID" in str(error.value)
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

    assert "MONITORING_TENANT_ID" in str(error.value)


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


def test_static_live_target_settings_and_precedence_loader_are_removed() -> None:
    assert not {"powerbi_workspace_id", "powerbi_dataset_id", "fabric_pipeline_targets"} & Settings.model_fields.keys()
    assert not hasattr(TriageRunner, "_resolve_id")


def test_state_settings_read_azure_sql_without_a_fabric_setting_fallback(monkeypatch) -> None:
    monkeypatch.delenv("AZURE_SQL_SERVER", raising=False)
    monkeypatch.delenv("AZURE_SQL_DATABASE", raising=False)
    monkeypatch.setenv("FABRIC_SQL_SERVER", "retired.database.fabric.microsoft.com")
    monkeypatch.setenv("FABRIC_SQL_DATABASE", "retired_state")
    unconfigured = Settings(_env_file=None)
    assert unconfigured.azure_sql_server == unconfigured.azure_sql_database == ""
    assert not {"fabric_sql_server", "fabric_sql_database"} & Settings.model_fields.keys()
    monkeypatch.setenv("AZURE_SQL_SERVER", "state.database.windows.net")
    monkeypatch.setenv("AZURE_SQL_DATABASE", "triage_state")
    configured = Settings(_env_file=None)
    assert configured.azure_sql_server == "state.database.windows.net"
    assert configured.azure_sql_database == "triage_state"


def test_the_sql_adapter_has_no_retired_import_alias() -> None:
    from importlib.util import find_spec

    from triage.store.azure_sql import AzureSqlDatabase

    assert AzureSqlDatabase.__module__ == "triage.store.azure_sql"
    assert find_spec("triage.store.fabric_sql") is None


def test_untrusted_non_native_ids_cannot_be_live_target_identities() -> None:
    from triage.monitoring.models import TargetIdentity

    with pytest.raises(ValueError):
        TargetIdentity(
            tenant_id="11111111-1111-1111-1111-111111111111",
            epoch="22222222-2222-2222-2222-222222222222",
            workload="powerbi", workspace_id="workspace-from-email", item_id="dataset-from-email",
        )


def test_synthetic_identity_mapping_is_explicit_and_deterministic() -> None:
    from triage.monitoring.runtime import fixture_target

    identity = fixture_target("powerbi", "scenario-workspace", "scenario-dataset")
    assert fixture_target("powerbi", "scenario-workspace", "scenario-dataset") == identity
    assert fixture_target("powerbi", "other-workspace", "scenario-dataset").key != identity.key


def test_preflight_does_not_connect_unless_asked(monkeypatch) -> None:
    """`preflight` is the first command an adopter runs, often before any
    credential exists. It must not open a connection, or the offline path stops
    being offline."""
    import triage.cli as cli

    calls: list[str] = []
    monkeypatch.setattr(
        cli, "_probe_azure_sql", lambda *_args: calls.append("connected") or ("", "", "")
    )
    monkeypatch.setattr(cli.settings, "azure_sql_server", "srv")
    monkeypatch.setattr(cli.settings, "azure_sql_database", "db")

    args = cli.build_parser().parse_args(["preflight"])
    assert cli.cmd_preflight(args) == 0
    assert calls == [], "preflight connected to SQL without --check-sql"


def test_check_sql_opts_into_a_real_connection(monkeypatch) -> None:
    """The opt-in flag is the only thing that makes the connection row appear.

    Without it the table reports `configured`, which says two settings are set
    and nothing about whether the database is reachable.
    """
    import triage.cli as cli

    calls: list[str] = []

    def fake_probe(*_args) -> tuple[str, str, str]:
        calls.append("connected")
        return ("  probe", "SELECT 1 succeeded", "ok")

    monkeypatch.setattr(cli, "_probe_azure_sql", fake_probe)
    monkeypatch.setattr(cli.settings, "azure_sql_server", "srv")
    monkeypatch.setattr(cli.settings, "azure_sql_database", "db")

    args = cli.build_parser().parse_args(["preflight", "--check-sql"])
    assert cli.cmd_preflight(args) == 0
    assert calls == ["connected"]


def test_the_sql_probe_reports_failure_rather_than_raising(monkeypatch) -> None:
    """A diagnostic that aborts the table is worse than one that prints red."""
    import triage.cli as cli
    from triage.store import azure_sql

    def boom(**_kwargs):
        raise RuntimeError("no route to host")

    monkeypatch.setattr(azure_sql, "AzureSqlDatabase", boom)
    monkeypatch.setattr(cli, "_operator_credential", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli.settings, "azure_sql_server", "srv")
    monkeypatch.setattr(cli.settings, "azure_sql_database", "db")

    name, detail, status = cli._probe_azure_sql()
    assert "failed" in status
    assert "RuntimeError" in detail


def test_a_scenario_will_not_silently_wipe_a_durable_incident_store(monkeypatch, capsys) -> None:
    """`run` clears incidents so a scenario is reproducible. That is right for a
    JSON file under runs/ and wrong for Azure SQL, where the same table is the
    hosted controller's live state.

    Found by doing it: one `bi-triage run scenario1-transient` against the live
    database took the incident table from seven rows to one.
    """
    import triage.cli as cli

    class _DurableStore:
        is_durable = True

    ran: list[str] = []

    class _Runner:
        store = _DurableStore()
        flag_table = None

        def __init__(self, *a, **kw) -> None:
            pass

        async def run_scenario(self, *_a, **_kw):
            ran.append("ran")
            return []

    monkeypatch.setattr(cli, "TriageRunner", _Runner)

    args = cli.build_parser().parse_args(["run", "scenario1-transient"])
    code = cli.cmd_run(args)

    assert code == 2, "ran against a durable store without being asked"
    assert ran == [], "the scenario executed anyway"
    assert "Refusing to run" in capsys.readouterr().out


def test_the_guard_does_not_block_a_local_file_store(monkeypatch) -> None:
    """Offline is the default path and must be unaffected."""
    import triage.cli as cli

    class _FileStore:
        is_durable = False

    ran: list[str] = []

    class _Runner:
        store = _FileStore()
        flag_table = None

        def __init__(self, *a, **kw) -> None:
            pass

        async def run_scenario(self, *_a, **_kw):
            ran.append("ran")
            return []

    monkeypatch.setattr(cli, "TriageRunner", _Runner)

    args = cli.build_parser().parse_args(["run", "scenario1-transient"])
    # Rendering an empty artifact list is not what this test is about, so any
    # failure past the guard is irrelevant -- reaching run_scenario is the
    # assertion.
    try:
        cli.cmd_run(args)
    except Exception:  # noqa: BLE001
        pass
    assert ran == ["ran"], "the guard blocked the offline path"
