"""Tests for the detector that finds failures nobody was told about.

Every other path here reacts to an alert. These failures send none: the
refresh reports success and the data is wrong anyway, so the report is trusted
until somebody notices the numbers by eye. An analyst describes
exactly this as the analyst's problem -- "a report that looks normal but is a
day stale" -- and until now it was the one case the system could not see.

The tests below weight heavily toward *not* alerting. A detector that fires
wrongly gets muted, and a muted detector is worse than none: it looks like
coverage. So most of what follows checks that plausible-but-innocent readings
stay quiet.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta

import pytest
from rich.markup import escape

from triage.detectors.silent_failures import (
    HealthProbe,
    SilentFailureScanner,
    load_probes,
)
from triage.store.semantic_health import (
    InMemorySemanticHealthStore,
    JsonFileSemanticHealthStore,
    ProbeState,
)
from triage.tools.semantic_health import (
    MockSemanticHealthClient,
    _permission_hint,
    build_probe_dax,
)

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def _probe(**overrides) -> HealthProbe:
    base = {
        "name": "sales-freshness",
        "workspace_id": "ws-1",
        "dataset_id": "ds-1",
        "table": "FactSales",
        "report_name": "Orders Daily Rollup",
        "date_column": "BusinessDate",
        "min_absolute_drop": 1_000,
    }
    base.update(overrides)
    return HealthProbe(**base)


def _scanner(client, store=None, *, now=NOW) -> SilentFailureScanner:
    return SilentFailureScanner(client, store or InMemorySemanticHealthStore(), now=now)


def _yesterday() -> str:
    return (NOW - timedelta(hours=20)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# The query is built from configuration, never from the model
# ---------------------------------------------------------------------------


def test_the_probe_query_is_generated_not_supplied() -> None:
    """The model never writes DAX.

    If it could, a prompt injection in an alert email would turn a read-only
    detector into an arbitrary query engine against the finance model.
    """
    dax = build_probe_dax(
        table="FactSales", date_column="BusinessDate", control_measures=("Net Sales",)
    )

    assert "MAX('FactSales'[BusinessDate])" in dax
    assert "COUNTROWS('FactSales')" in dax
    assert "[Net Sales]" in dax
    assert dax.startswith("EVALUATE")


def test_a_probe_without_a_table_is_refused() -> None:
    with pytest.raises(ValueError):
        build_probe_dax(table="  ")


def test_quotes_in_configuration_cannot_reshape_the_query() -> None:
    """Configuration is edited by people, and people mistype.

    A stray quote should produce a broken query, not a differently-scoped one.
    """
    dax = build_probe_dax(table="Fact'Sales", date_column="Date]Col")

    assert "Fact''Sales" in dax
    assert "Date]]Col" in dax


def test_a_star_schema_takes_its_watermark_through_the_relationship() -> None:
    """The common shape: a date key on the fact, the date on a dimension.

    Measured live against a real model on 2026-09-01. The fact held
    ``invoice_date_key`` as an integer and no date column at all, so there was
    nothing on it to take a MAX of.
    """
    dax = build_probe_dax(
        table="fact_sales_invoice", date_table="dim_date", date_column="date"
    )

    assert "MAXX('fact_sales_invoice', RELATED('dim_date'[date]))" in dax
    # Still counted on the fact -- the dimension's row count is the calendar's.
    assert "COUNTROWS('fact_sales_invoice')" in dax


def test_the_star_schema_workaround_that_silently_never_fires() -> None:
    """Negative control for the bug that motivated ``date_table``.

    Without it, the only way to get a watermark from a star schema is to point
    the probe at the date dimension. On the real model that returned
    2030-12-31 -- a calendar populated years ahead -- while the data stopped at
    2024-12-23. The probe would have called a two-year-stale model fresh, for
    ever, and looked like it was working.

    So: measuring the dimension must not be mistakable for measuring the fact.
    """
    wrong = build_probe_dax(table="dim_date", date_column="date")
    right = build_probe_dax(
        table="fact_sales_invoice", date_table="dim_date", date_column="date"
    )

    assert "MAX('dim_date'[date])" in wrong
    assert "RELATED" not in wrong
    assert wrong != right
    # The counted table is the giveaway: one counts the calendar, one the data.
    assert "COUNTROWS('dim_date')" in wrong
    assert "COUNTROWS('fact_sales_invoice')" in right


def test_a_date_table_without_a_column_is_refused() -> None:
    """Half-configured is refused rather than silently ignored.

    Ignoring it would fall back to ``MAX('fact'[...])`` on a column that does
    not exist, and the detector would report a fault it could not explain.
    """
    with pytest.raises(ValueError):
        build_probe_dax(table="fact_sales_invoice", date_table="dim_date")


def test_quotes_in_the_date_table_are_escaped_too() -> None:
    dax = build_probe_dax(
        table="Fact'Sales", date_table="Dim'Date", date_column="Date]Col"
    )

    assert "Dim''Date" in dax
    assert "Fact''Sales" in dax
    assert "Date]]Col" in dax


# ---------------------------------------------------------------------------
# Healthy readings stay silent and advance the baseline
# ---------------------------------------------------------------------------


async def test_a_healthy_model_is_silent_and_updates_the_baseline() -> None:
    store = InMemorySemanticHealthStore()
    client = MockSemanticHealthClient(max_date=_yesterday(), row_count=10_000)

    finding = await _scanner(client, store).scan(_probe())

    assert finding.status == "healthy"
    state = store.get("ws-1", "ds-1", "sales-freshness")
    assert state.last_row_count == 10_000
    assert state.last_max_date == _yesterday()


async def test_a_first_observation_is_never_a_collapse() -> None:
    """With no baseline there is nothing to have dropped from.

    Otherwise every newly configured probe alerts on its first run, which
    trains people to ignore the detector on day one.
    """
    client = MockSemanticHealthClient(max_date=_yesterday(), row_count=12)

    finding = await _scanner(client).scan(_probe())

    assert finding.status == "healthy"


# ---------------------------------------------------------------------------
# Stale but successful: the case with no alert at all
# ---------------------------------------------------------------------------


async def test_stale_data_is_suspected_then_confirmed() -> None:
    """One odd reading is not a finding.

    A probe that runs mid-refresh sees a half-loaded model. Announcing that
    would page somebody about a model that was fine ninety seconds later.
    """
    store = InMemorySemanticHealthStore()
    client = MockSemanticHealthClient(max_date="2026-08-25", row_count=10_000)
    scanner = _scanner(client, store)

    first = await scanner.scan(_probe())
    assert first.status == "suspect"
    assert not first.actionable

    second = await scanner.scan(_probe())
    assert second.status == "confirmed"
    assert second.actionable
    assert second.kind == "stale"
    assert "reported success" in second.detail


async def test_a_suspect_reading_never_becomes_the_baseline() -> None:
    """The failure that would make the detector useless.

    Accepting a stale reading as the new normal means it never alerts again --
    a detector reporting health it has not established.
    """
    store = InMemorySemanticHealthStore()
    store.put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_max_date=_yesterday(),
            last_row_count=10_000,
        )
    )
    client = MockSemanticHealthClient(max_date="2026-08-25", row_count=9_800)

    await _scanner(client, store).scan(_probe())

    assert store.get("ws-1", "ds-1", "sales-freshness").last_max_date == _yesterday()


async def test_a_model_inside_its_expected_lag_is_not_stale() -> None:
    """A daily model holding yesterday's data is working correctly."""
    client = MockSemanticHealthClient(max_date=_yesterday(), row_count=10_000)

    finding = await _scanner(client).scan(_probe(expected_lag_hours=24))

    assert finding.status == "healthy"


async def test_a_model_that_is_legitimately_days_behind_is_not_stale() -> None:
    """T+3 finance data is not a failure. Freshness cannot be a constant."""
    client = MockSemanticHealthClient(
        max_date=(NOW - timedelta(days=3)).strftime("%Y-%m-%d"), row_count=10_000
    )

    finding = await _scanner(client).scan(_probe(expected_lag_hours=96))

    assert finding.status == "healthy"


async def test_an_unparseable_date_is_not_reported_as_stale() -> None:
    """A formatting change is not a data outage."""
    client = MockSemanticHealthClient(max_date="not-a-date", row_count=10_000)

    finding = await _scanner(client).scan(_probe())

    assert finding.status == "healthy"


# ---------------------------------------------------------------------------
# Row-count collapse, and the noise it must not report
# ---------------------------------------------------------------------------


async def test_a_row_count_collapse_is_confirmed_and_described() -> None:
    store = InMemorySemanticHealthStore()
    store.put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_row_count=100_000,
            last_max_date=_yesterday(),
        )
    )
    client = MockSemanticHealthClient(max_date=_yesterday(), row_count=40_000)
    scanner = _scanner(client, store)

    await scanner.scan(_probe())
    finding = await scanner.scan(_probe())

    assert finding.status == "confirmed"
    assert finding.kind == "row_collapse"
    assert "40,000" in finding.detail and "100,000" in finding.detail


async def test_a_small_table_losing_a_few_rows_is_not_a_collapse() -> None:
    """7 rows becoming 4 is a 57% drop and almost always noise.

    Relative thresholds alone make small tables permanently noisy, which is
    how a detector earns a mail rule.
    """
    store = InMemorySemanticHealthStore()
    store.put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_row_count=7,
            last_max_date=_yesterday(),
        )
    )
    client = MockSemanticHealthClient(max_date=_yesterday(), row_count=4)

    finding = await _scanner(client, store).scan(_probe())

    assert finding.status == "healthy"


async def test_a_large_table_losing_a_small_fraction_is_not_a_collapse() -> None:
    """Absolute thresholds alone fire on normal variation in a big table."""
    store = InMemorySemanticHealthStore()
    store.put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_row_count=10_000_000,
            last_max_date=_yesterday(),
        )
    )
    client = MockSemanticHealthClient(max_date=_yesterday(), row_count=9_990_000)

    finding = await _scanner(client, store).scan(_probe())

    assert finding.status == "healthy"


async def test_a_growing_table_is_never_a_collapse() -> None:
    store = InMemorySemanticHealthStore()
    store.put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_row_count=10_000,
            last_max_date=_yesterday(),
        )
    )
    client = MockSemanticHealthClient(max_date=_yesterday(), row_count=11_000)

    assert (await _scanner(client, store).scan(_probe())).status == "healthy"


async def test_switching_symptom_restarts_the_confirmation_count() -> None:
    """Two unrelated single anomalies must not add up to one finding."""
    store = InMemorySemanticHealthStore()
    store.put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_row_count=100_000,
            last_max_date=_yesterday(),
        )
    )
    scanner = _scanner(MockSemanticHealthClient(max_date="2026-08-01", row_count=100_000), store)
    assert (await scanner.scan(_probe())).status == "suspect"

    collapse = _scanner(
        MockSemanticHealthClient(max_date=_yesterday(), row_count=10_000), store
    )
    second = await collapse.scan(_probe())

    assert second.status == "suspect", "a different symptom must not inherit the count"


# ---------------------------------------------------------------------------
# A blind detector is not a broken model
# ---------------------------------------------------------------------------


async def test_a_probe_that_cannot_run_is_a_detector_fault_not_a_finding() -> None:
    """"We cannot see this model" and "this model is stale" need different people.

    Merging them means a permissions change reads as a data outage.
    """
    client = MockSemanticHealthClient(
        ok=False, error="HTTP 403: dataset uses RLS", detector_fault=True
    )

    finding = await _scanner(client).scan(_probe())

    assert finding.status == "detector_fault"
    assert not finding.actionable


async def test_a_detector_fault_does_not_touch_the_baseline() -> None:
    store = InMemorySemanticHealthStore()
    store.put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_row_count=10_000,
            last_max_date=_yesterday(),
        )
    )
    client = MockSemanticHealthClient(ok=False, error="HTTP 401", detector_fault=True)

    await _scanner(client, store).scan(_probe())

    state = store.get("ws-1", "ds-1", "sales-freshness")
    assert state.last_row_count == 10_000
    assert state.consecutive_errors == 1


async def test_a_client_that_raises_is_caught_and_reported() -> None:
    class _Broken:
        async def run_probe(self, *args, **kwargs):
            raise TimeoutError("gateway timeout")

    finding = await _scanner(_Broken()).scan(_probe())

    assert finding.status == "detector_fault"
    assert "TimeoutError" in finding.detail


# ---------------------------------------------------------------------------
# Baselines have to outlive the process
# ---------------------------------------------------------------------------


def test_a_baseline_survives_a_new_store_instance(tmp_path) -> None:
    """A hosted agent is rebuilt per invocation.

    An in-memory baseline compares every reading against nothing, so the
    detector could never conclude that something failed to move.
    """
    path = tmp_path / "semantic_health.json"
    JsonFileSemanticHealthStore(path).put(
        ProbeState(
            workspace_id="ws-1",
            dataset_id="ds-1",
            probe_name="sales-freshness",
            last_row_count=10_000,
        )
    )

    reloaded = JsonFileSemanticHealthStore(path).get("ws-1", "ds-1", "sales-freshness")
    assert reloaded is not None and reloaded.last_row_count == 10_000


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_one_bad_probe_does_not_stop_the_others() -> None:
    """A detector that refuses to start because of a typo is a detector that is off."""
    probes = load_probes(
        [
            {"name": "good", "workspace_id": "w", "dataset_id": "d", "table": "T"},
            {"no_name_field": True},
        ]
    )

    assert [p.name for p in probes] == ["good"]


def test_no_configuration_means_no_probes() -> None:
    assert load_probes(None) == []
    assert load_probes([]) == []


@pytest.mark.parametrize("raw", ["", "   ", None])
def test_an_unset_probe_variable_does_not_stop_the_agent_starting(raw) -> None:
    """The whole agent died at import because of one empty string.

    pydantic-settings JSON-decodes complex annotations inside the environment
    source, before any validator runs, so an unset SILENT_HEALTH_PROBES raised
    SettingsError at import and the container never reached readiness. Mail
    triage, approvals and remediation all went down for an optional detector
    that had no configuration.

    The field is therefore a plain string, parsed here where being wrong
    disables only the detector.
    """
    assert load_probes(raw) == []


def test_malformed_probe_configuration_disables_only_the_detector() -> None:
    """A typo in detector config must not take the rest of the system with it."""
    assert load_probes("{not json") == []
    assert load_probes('{"not": "a list"}') == []


def test_valid_probe_json_is_parsed() -> None:
    probes = load_probes(
        '[{"name":"p","workspace_id":"w","dataset_id":"d","table":"T"}]'
    )
    assert [p.name for p in probes] == ["p"]


def test_settings_accept_an_empty_probe_variable() -> None:
    """The exact shape that crashed the container."""
    from triage.settings import Settings

    assert load_probes(Settings(silent_health_probes="").silent_health_probes) == []


# ---------------------------------------------------------------------------
# End to end through the runner
# ---------------------------------------------------------------------------


async def test_the_sweep_is_silent_when_nothing_is_configured(runner) -> None:
    """Watching nothing is the correct default.

    Nobody has said what "fresh" means for any model yet, and guessing is how
    false positives start.
    """
    assert await runner.silent_sweep() == []


async def test_a_confirmed_finding_becomes_an_incident_and_one_card(runner) -> None:
    runner.settings.silent_health_probes = json.dumps(
        [
            {
                "name": "sales-freshness",
                "workspace_id": "ws-1",
                "dataset_id": "ds-1",
                "table": "FactSales",
                "date_column": "BusinessDate",
                "report_name": "Orders Daily Rollup",
            }
        ]
    )
    runner._health_client = MockSemanticHealthClient(max_date="2026-01-01", row_count=10_000)
    runner.build_health_client = lambda: runner._health_client  # type: ignore[method-assign]

    first = await runner.silent_sweep(now=NOW)
    assert first and "awaiting confirmation" in first[0]
    assert runner.store.list_all() == [], "a suspect reading must not create an incident"

    second = await runner.silent_sweep(now=NOW)
    assert second and "confirmed" in second[0]

    incidents = runner.store.list_all()
    assert len(incidents) == 1
    assert incidents[0].notified_count == 1

    # Polling every fifteen minutes must not announce every fifteen minutes.
    third = await runner.silent_sweep(now=NOW)
    assert third and "already announced" in third[0]
    assert runner.store.list_all()[0].notified_count == 1


async def test_the_detector_can_be_switched_off_in_configuration(runner) -> None:
    """The off switch cannot live in routine state: a deploy re-enables it."""
    runner.settings.silent_health_probes = json.dumps(
        [{"name": "p", "workspace_id": "w", "dataset_id": "d", "table": "T"}]
    )
    runner.settings.silent_sweep_enabled = False

    assert await runner.silent_sweep(now=NOW) == []

# ---------------------------------------------------------------------------
# Platform refusals that do not describe themselves
# ---------------------------------------------------------------------------


def test_the_tenant_setting_refusal_says_what_to_change() -> None:
    """401 PowerBINotAuthorizedException arrives with no message at all.

    Measured against a real tenant: the body is the code, an empty parameter
    bag, and nothing else. The natural reading -- "grant it more access" -- is
    wrong when the cause is Direct Lake, so the hint has to say so outright.
    """
    hint = _permission_hint(401, '{"error":{"code":"PowerBINotAuthorizedException"}}')

    assert "Direct Lake" in hint
    assert "fixed identity" in hint
    assert "will not fix it" in hint


def test_the_permission_refusal_is_not_mistaken_for_a_wrong_id() -> None:
    """404 is what insufficient workspace permission looks like here.

    The dataset exists and the ids are right; the caller cannot see it. Reading
    it as "wrong id" sends people to check GUIDs that were never wrong.
    """
    hint = _permission_hint(404, '{"error":{"code":"PowerBIEntityNotFound"}}')

    assert "not" in hint and "wrong id" in hint
    assert "Contributor" in hint


def test_no_hint_is_invented_for_an_unrelated_failure() -> None:
    """A hint attached to the wrong error is worse than none.

    It would send somebody to the admin portal over a throttling response.
    """
    assert _permission_hint(429, "TooManyRequests") == ""
    assert _permission_hint(500, "InternalServerError") == ""
    assert _permission_hint(401, "some other unauthorised thing") == ""

def test_a_detector_fault_shows_the_hint_not_the_json() -> None:
    """The sweep line is short; Power BI's payload would fill all of it.

    Truncating the raw detail cut the hint off entirely, so guidance that was
    present and correct never reached the person reading the output.
    """
    from triage.runner import _fault_summary
    from triage.tools.semantic_health import _permission_hint

    detail = ('HTTP 401: {"error":{"code":"PowerBINotAuthorizedException",'
              '"pbi.error":{"code":"PowerBINotAuthorizedException"}}}'
              + _permission_hint(401, "PowerBINotAuthorizedException"))
    summary = _fault_summary(detail)

    assert "Direct Lake" in summary
    assert "fixed identity" in summary
    assert "pbi.error" not in summary
    assert "\n" not in summary


def test_a_fault_without_a_hint_still_says_something() -> None:
    assert _fault_summary_plain().startswith("HTTP 500")


def _fault_summary_plain() -> str:
    from triage.runner import _fault_summary
    return _fault_summary("HTTP 500: InternalServerError, something broke upstream")

# ---------------------------------------------------------------------------
# Schema drift (Phase 3)
# ---------------------------------------------------------------------------

SCHEMA_V1 = ("column:FactSales[Amount]", "column:FactSales[BusinessDate]", "measure:Net Sales")


@pytest.mark.asyncio
async def test_the_first_schema_sighting_is_a_baseline_not_a_finding() -> None:
    client = MockSemanticHealthClient(max_date=_yesterday(), schema=SCHEMA_V1)
    store = InMemorySemanticHealthStore()
    finding = await _scanner(client, store).scan(_probe(watch_schema=True))

    assert finding.status == "healthy"
    state = store.get("ws-1", "ds-1", "sales-freshness")
    assert tuple(state.last_schema) == tuple(sorted(SCHEMA_V1))


@pytest.mark.asyncio
async def test_a_removed_column_is_reported() -> None:
    """Nothing in Power BI announces this, and every report built on it breaks."""
    store = InMemorySemanticHealthStore()
    probe = _probe(watch_schema=True, confirmations=1)

    first = MockSemanticHealthClient(max_date=_yesterday(), schema=SCHEMA_V1)
    await _scanner(first, store).scan(probe)

    dropped = tuple(e for e in SCHEMA_V1 if "Amount" not in e)
    second = MockSemanticHealthClient(max_date=_yesterday(), schema=dropped)
    finding = await _scanner(second, store).scan(probe)

    assert finding.kind == "schema_drift"
    assert finding.status == "confirmed"
    assert "FactSales[Amount]" in finding.detail


@pytest.mark.asyncio
async def test_added_columns_are_not_drift() -> None:
    """The negative control that decides whether anyone keeps this switched on.

    Models gain columns and measures constantly. A detector that reports every
    addition fires on ordinary development, and a detector that fires on
    ordinary development gets turned off -- taking the removal case with it.
    """
    store = InMemorySemanticHealthStore()
    probe = _probe(watch_schema=True, confirmations=1)

    first = MockSemanticHealthClient(max_date=_yesterday(), schema=SCHEMA_V1)
    await _scanner(first, store).scan(probe)

    grown = SCHEMA_V1 + ("column:FactSales[Discount]", "measure:Gross Sales")
    second = MockSemanticHealthClient(max_date=_yesterday(), schema=grown)
    finding = await _scanner(second, store).scan(probe)

    assert finding.status == "healthy"
    # The baseline moves on, so a later removal is measured against reality.
    assert tuple(store.get("ws-1", "ds-1", "sales-freshness").last_schema) == tuple(sorted(grown))


@pytest.mark.asyncio
async def test_an_unreadable_schema_is_never_reported_as_deletion() -> None:
    """"I could not look" must not become "everything was deleted"."""
    store = InMemorySemanticHealthStore()
    probe = _probe(watch_schema=True, confirmations=1)

    first = MockSemanticHealthClient(max_date=_yesterday(), schema=SCHEMA_V1)
    await _scanner(first, store).scan(probe)

    blind = MockSemanticHealthClient(
        max_date=_yesterday(), schema_ok=False, schema_error="HTTP 401"
    )
    finding = await _scanner(blind, store).scan(probe)

    assert finding.kind != "schema_drift"
    assert finding.status == "healthy"
    # The baseline is untouched, so the next readable sweep still compares.
    assert tuple(store.get("ws-1", "ds-1", "sales-freshness").last_schema) == tuple(sorted(SCHEMA_V1))


@pytest.mark.asyncio
async def test_schema_is_not_queried_unless_asked_for() -> None:
    """It costs a second query per sweep against the capacity being watched."""
    client = MockSemanticHealthClient(max_date=_yesterday(), schema=SCHEMA_V1)
    await _scanner(client, InMemorySemanticHealthStore()).scan(_probe(watch_schema=False))

    assert not any("INFO.VIEW" in c["query"] for c in client.calls)


@pytest.mark.asyncio
async def test_stale_data_is_reported_before_schema_drift() -> None:
    """A model that stopped loading is the more urgent report."""
    store = InMemorySemanticHealthStore()
    probe = _probe(watch_schema=True, confirmations=1)

    first = MockSemanticHealthClient(max_date=_yesterday(), schema=SCHEMA_V1)
    await _scanner(first, store).scan(probe)

    both_wrong = MockSemanticHealthClient(
        max_date="2020-01-01", schema=tuple(e for e in SCHEMA_V1 if "Amount" not in e)
    )
    finding = await _scanner(both_wrong, store).scan(probe)

    assert finding.kind == "stale"

def test_probe_spans_carry_no_observed_values() -> None:
    """Spans are metadata only.

    Traces are retained and widely readable inside a tenant, and a watermark or
    a row count is customer data. The status is enough to see the detector
    working; the numbers belong in the store, not the trace.
    """
    import inspect

    from triage.detectors import silent_failures as sf

    source = inspect.getsource(sf.SilentFailureScanner.scan)
    assert "tool_span" in source
    for leaked in ("max_date", "row_count", "result.", "detail"):
        assert f'span.set("probe.{leaked}' not in source
    # Whatever else changes, the values themselves must not be attached.
    assert "finding.observed" not in source
    assert "finding.detail" not in source

# ---------------------------------------------------------------------------
# Circuit breaker (Phase 5)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_probe_that_never_works_is_parked() -> None:
    """Observed: a Direct Lake probe reached twelve consecutive 401s.

    App-only callers cannot query Direct Lake at all, so every retry was pure
    cost against the capacity being watched.
    """
    store = InMemorySemanticHealthStore()
    probe = _probe(max_consecutive_errors=3)
    blind = MockSemanticHealthClient(ok=False, error="HTTP 401", detector_fault=True)

    for _ in range(3):
        assert (await _scanner(blind, store).scan(probe)).status == "detector_fault"

    calls_before = len(blind.calls)
    finding = await _scanner(blind, store).scan(probe)

    assert finding.status == "parked"
    assert len(blind.calls) == calls_before, "a parked probe must stop querying"


@pytest.mark.asyncio
async def test_a_parked_probe_tries_again_after_the_cooldown() -> None:
    """Reopened by time, not by anyone remembering to."""
    store = InMemorySemanticHealthStore()
    probe = _probe(max_consecutive_errors=2, circuit_cooldown_minutes=60)
    blind = MockSemanticHealthClient(ok=False, error="HTTP 401", detector_fault=True)

    for _ in range(2):
        await _scanner(blind, store).scan(probe)
    assert (await _scanner(blind, store).scan(probe)).status == "parked"

    healed = MockSemanticHealthClient(max_date=_yesterday())
    later = NOW + timedelta(minutes=61)
    finding = await _scanner(healed, store, now=later).scan(probe)

    assert finding.status == "healthy"
    assert healed.calls, "the cooldown must allow a real attempt"


@pytest.mark.asyncio
async def test_a_working_probe_is_never_parked() -> None:
    """The breaker must not fire on a detector that can see."""
    store = InMemorySemanticHealthStore()
    client = MockSemanticHealthClient(max_date=_yesterday())
    for _ in range(8):
        assert (await _scanner(client, store).scan(_probe())).status == "healthy"


@pytest.mark.asyncio
async def test_one_success_clears_the_breaker() -> None:
    """A fixed permission should not leave the probe parked until a timer ends."""
    store = InMemorySemanticHealthStore()
    probe = _probe(max_consecutive_errors=3)
    blind = MockSemanticHealthClient(ok=False, error="HTTP 401", detector_fault=True)
    for _ in range(2):
        await _scanner(blind, store).scan(probe)

    await _scanner(MockSemanticHealthClient(max_date=_yesterday()), store).scan(probe)
    state = store.get("ws-1", "ds-1", "sales-freshness")

    assert state.consecutive_errors == 0
    assert state.circuit_opened_at == ""

# ---------------------------------------------------------------------------
# Sweep lease and pacing (Phase 2 / 5)
# ---------------------------------------------------------------------------


def test_a_second_sweeper_is_refused_the_lease() -> None:
    """Two instances confirming the same probe would defeat suspect-then-confirm.

    Each would increment the suspect count for one real occurrence, so a rule
    that exists to prevent false positives would start producing them.
    """
    store = InMemorySemanticHealthStore()

    assert store.try_acquire_lease("silent-sweep", "instance-a", 300)
    assert not store.try_acquire_lease("silent-sweep", "instance-b", 300)


def test_the_lease_is_reusable_by_its_holder() -> None:
    """A retry from the same instance must not deadlock against itself."""
    store = InMemorySemanticHealthStore()
    assert store.try_acquire_lease("silent-sweep", "instance-a", 300)
    assert store.try_acquire_lease("silent-sweep", "instance-a", 300)


def test_an_expired_lease_does_not_block_for_ever() -> None:
    """An instance that dies mid-sweep must not stop every future sweep."""
    store = InMemorySemanticHealthStore()
    assert store.try_acquire_lease("silent-sweep", "crashed", 0)

    assert store.try_acquire_lease("silent-sweep", "instance-b", 300)


def test_releasing_is_owner_scoped() -> None:
    """A loser must not be able to release the winner's lease."""
    store = InMemorySemanticHealthStore()
    store.try_acquire_lease("silent-sweep", "instance-a", 300)

    store.release_lease("silent-sweep", "instance-b")

    assert not store.try_acquire_lease("silent-sweep", "instance-c", 300)


@pytest.mark.asyncio
async def test_probes_are_paced_rather_than_run_together() -> None:
    """executeQueries is throttled per user across every dataset.

    A detector that trips that limit becomes the capacity incident it exists to
    watch for, so the sweep is deliberately sequential.
    """
    client = MockSemanticHealthClient(max_date=_yesterday())
    scanner = SilentFailureScanner(client, InMemorySemanticHealthStore(), now=NOW)
    probes = [_probe(name=f"p{i}", dataset_id=f"ds-{i}") for i in range(3)]

    await scanner.sweep(probes)

    assert len(client.calls) == 3
    assert [c["dataset_id"] for c in client.calls] == ["ds-0", "ds-1", "ds-2"]

# ---------------------------------------------------------------------------
# Maintenance calendars (Phase 4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_weekday_model_is_not_stale_at_the_weekend() -> None:
    """The false positive that arrives every Saturday and mutes the detector."""
    sunday = datetime(2026, 8, 30, 9, 0, tzinfo=UTC)
    assert sunday.isoweekday() == 7
    friday_data = MockSemanticHealthClient(max_date="2026-08-28")
    probe = _probe(load_weekdays=(1, 2, 3, 4, 5))

    finding = await _scanner(friday_data, now=sunday).scan(probe)

    assert finding.status == "healthy"


@pytest.mark.asyncio
async def test_a_weekday_model_is_still_stale_on_a_working_day() -> None:
    """Negative control: the calendar must not silence a genuine failure.

    By Tuesday, a Friday watermark has missed Monday and Tuesday.
    """
    tuesday = datetime(2026, 9, 1, 9, 0, tzinfo=UTC)
    assert tuesday.isoweekday() == 2
    friday_data = MockSemanticHealthClient(max_date="2026-08-28")
    probe = _probe(load_weekdays=(1, 2, 3, 4, 5), confirmations=1)

    finding = await _scanner(friday_data, now=tuesday).scan(probe)

    assert finding.kind == "stale"


def test_probe_watermarks_survive_rich_markup() -> None:
    """``table[column]`` is DAX; Rich reads it as a style tag and eats it.

    The column name is the part somebody is checking, so losing it silently
    makes the listing worse than useless.
    """
    from rich.console import Console

    from triage.cli import build_parser  # noqa: F401  (import smoke)

    console = Console(file=io.StringIO(), width=200, force_terminal=False)
    console.print(escape("dim_date[date]"))

    assert "dim_date[date]" in console.file.getvalue()

# ---------------------------------------------------------------------------
# Probe preflight (Phase 5)
# ---------------------------------------------------------------------------


def _preflight(probes_json: str) -> tuple[int, str]:
    """Run `health --preflight` against a configuration, capturing its output.

    ``settings`` and ``console`` are module globals in the CLI, so both are
    swapped rather than passed -- Rich holds the stream it was built with, so
    redirecting stdout alone captures nothing.
    """
    from rich.console import Console

    from triage import cli
    from triage.settings import Settings

    args = cli.build_parser().parse_args(["health", "--preflight"])
    buffer = io.StringIO()
    original_settings, original_console = cli.settings, cli.console
    cli.settings = Settings(silent_health_probes=probes_json, triage_tool_mode="mock")
    cli.console = Console(file=buffer, width=200, force_terminal=False)
    try:
        code = cli.cmd_health(args)
    finally:
        cli.settings, cli.console = original_settings, original_console
    return code, buffer.getvalue()


def test_preflight_passes_a_sane_probe() -> None:
    code, _ = _preflight(json.dumps([{
        "name": "sales", "workspace_id": "ws", "dataset_id": "ds",
        "table": "f", "date_table": "d", "date_column": "date",
    }]))
    assert code == 0


def test_preflight_rejects_a_configuration_that_can_detect_nothing() -> None:
    """The failure that matters: it looks like monitoring and reports nothing.

    A probe with no ids never runs, and nothing else in the system will say so.
    """
    code, out = _preflight(json.dumps([
        {"name": "c", "workspace_id": "", "dataset_id": "", "table": ""},
    ]))
    assert code == 1
    assert "missing workspace or dataset id" in out


def test_preflight_catches_duplicate_probes_on_one_model() -> None:
    """State is keyed on workspace+dataset+name, so duplicates overwrite.

    Neither probe accumulates history, so neither can ever confirm anything.
    """
    code, out = _preflight(json.dumps([
        {"name": "a", "workspace_id": "ws", "dataset_id": "ds", "table": "f", "date_column": "d"},
        {"name": "a", "workspace_id": "ws", "dataset_id": "ds", "table": "f", "date_column": "d"},
    ]))
    assert code == 1
    assert "duplicate" in out


def test_preflight_rejects_confirmations_that_would_announce_immediately() -> None:
    """Suspect-then-confirm is the false-positive guard; 0 disables it."""
    code, out = _preflight(json.dumps([
        {"name": "a", "workspace_id": "ws", "dataset_id": "ds", "table": "f",
         "date_column": "d", "confirmations": 0},
    ]))
    assert code == 1
    assert "confirmations" in out


def test_preflight_warns_when_staleness_is_not_watched() -> None:
    """Not an error -- a row-count-only probe is legitimate -- but worth saying."""
    code, out = _preflight(json.dumps([
        {"name": "a", "workspace_id": "ws", "dataset_id": "ds", "table": "f"},
    ]))
    assert code == 0
    assert "staleness is not watched" in out

@pytest.mark.asyncio
async def test_a_stale_model_still_gets_a_schema_baseline() -> None:
    """Regression: schema watching silently never engaged on a broken model.

    The schema read used to be skipped whenever the data already looked wrong,
    so a model with a standing finding never recorded a baseline and drift
    could never be detected on it -- monitoring that quietly does nothing, on
    exactly the models most likely to be broken.
    """
    store = InMemorySemanticHealthStore()
    stale = MockSemanticHealthClient(max_date="2020-01-01", schema=SCHEMA_V1)

    finding = await _scanner(stale, store).scan(_probe(watch_schema=True))

    assert finding.kind == "stale", "the louder problem still wins the sentence"
    state = store.get("ws-1", "ds-1", "sales-freshness")
    assert tuple(state.last_schema) == tuple(sorted(SCHEMA_V1))


@pytest.mark.asyncio
async def test_drift_under_a_stale_model_is_not_learned_as_normal() -> None:
    """A removal must not be absorbed into the baseline while staleness is reported."""
    store = InMemorySemanticHealthStore()
    probe = _probe(watch_schema=True, confirmations=1)
    await _scanner(MockSemanticHealthClient(max_date=_yesterday(), schema=SCHEMA_V1), store).scan(probe)

    dropped = tuple(e for e in SCHEMA_V1 if "Amount" not in e)
    await _scanner(MockSemanticHealthClient(max_date="2020-01-01", schema=dropped), store).scan(probe)

    # Baseline still holds the removed column, so the drift is reported once
    # the staleness clears rather than being silently accepted.
    state = store.get("ws-1", "ds-1", "sales-freshness")
    assert "column:FactSales[Amount]" in state.last_schema

    healthy_again = MockSemanticHealthClient(max_date=_yesterday(), schema=dropped)
    finding = await _scanner(healthy_again, store).scan(probe)
    assert finding.kind == "schema_drift"

def test_a_finding_keeps_the_column_name_when_printed() -> None:
    """Rich eats DAX brackets, and the column name is the whole message.

    "an object the model used to expose is gone" is useless without saying
    which one. The detail is escaped at the point it is printed.
    """
    from rich.console import Console
    from rich.markup import escape as esc

    detail = ("The refresh reported success, but 1 object(s) the model used to "
              "expose are gone: fact_sales_invoice[source_system].")

    eaten = Console(file=io.StringIO(), width=200, force_terminal=False)
    eaten.print(detail)
    assert "source_system" not in eaten.file.getvalue(), "precondition: Rich drops it"

    kept = Console(file=io.StringIO(), width=200, force_terminal=False)
    kept.print(esc(detail))
    assert "fact_sales_invoice[source_system]" in kept.file.getvalue()
