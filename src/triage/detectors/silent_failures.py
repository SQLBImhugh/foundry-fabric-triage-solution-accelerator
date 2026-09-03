"""The detector for failures that send no alert.

Every other path in this system starts with Power BI telling us something
broke. These do not: the refresh reports success and the data is wrong anyway.
Nobody is emailed, so nobody looks, and the report is trusted for as long as it
takes someone to notice the numbers by eye.

Three questions catch most of it:

* **Did the data move?** A completed refresh whose watermark did not advance
  means the pipeline ran and loaded nothing.
* **Is it all there?** A table at a third of its usual size loads without error
  and makes every total quietly wrong.
* **Can we still ask?** A probe that fails because a column vanished is
  evidence the model's shape changed under the report.

Deterministic on purpose, and not a third prompt agent. Invariant 4 says
measured evidence outranks model output, and every question above is a
measurement -- a maximum, a count, a comparison. A model asked whether a 60%
row drop is acceptable will sometimes say yes, and that is precisely the
judgement this must not make. The Triage agent writes the explanation; the
scanner decides what is true.

False positives are the failure mode that matters. An alert that fires wrongly
teaches people to ignore the channel, and then the real one is ignored too. So
the bar is deliberately high: a single odd reading records suspicion and says
nothing, and only a condition that survives confirmation becomes a finding.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from triage.observability import tool_span
from triage.store.semantic_health import ProbeState, SemanticHealthStore
from triage.tools.semantic_health import ProbeResult, SemanticModelHealthClient

logger = logging.getLogger("triage.detectors.silent")

#: A drop past this fraction of the baseline is suspicious. Paired with an
#: absolute floor: 7 rows becoming 4 is a 57% drop and almost always noise.
DEFAULT_MAX_DROP_FRACTION = 0.30
DEFAULT_MIN_ABSOLUTE_DROP = 1_000

#: Consecutive detector faults before a probe is parked. Observed need: a probe
#: against a Direct Lake model reached twelve consecutive 401s, re-querying a
#: model it was never going to be allowed to read, on every sweep. A detector
#: that cannot see should say so once and stop, not generate load and noise for
#: ever.
DEFAULT_MAX_CONSECUTIVE_ERRORS = 5

#: How long a parked probe waits before trying again. Long enough that a broken
#: permission is fixed by a person rather than rediscovered every minute; short
#: enough that the fix is picked up without a redeploy.
DEFAULT_CIRCUIT_COOLDOWN_MINUTES = 60

DEFAULT_CONFIRMATIONS = 2

#: How long after the expected refresh before staleness is even considered.
DEFAULT_GRACE_MINUTES = 90


@dataclass(frozen=True)
class HealthProbe:
    """One configured question about one semantic model.

    Configuration rather than inference. A daily model and an hourly one differ,
    a T+1 finance model is legitimately a day behind, and a weekend-skipping
    operational model is legitimately stale on a Sunday. Guessing any of that
    produces exactly the false positives this design is built to avoid.
    """

    name: str
    workspace_id: str
    dataset_id: str
    table: str
    report_name: str = ""
    date_column: str = ""
    #: The dimension holding ``date_column``, when the measured table carries a
    #: date *key* rather than a date -- which is what a star schema looks like.
    #: Leave empty when the date lives on the measured table itself.
    date_table: str = ""
    #: Watch for columns and measures disappearing. Opt-in because it costs a
    #: second query per sweep, and because a model nobody reports on does not
    #: need it.
    watch_schema: bool = False
    row_count_table: str = ""
    control_measures: tuple[str, ...] = ()
    #: How far behind "now" the data is expected to be, in hours. A daily model
    #: loaded at 06:00 with yesterday's data has an expected lag of 24.
    expected_lag_hours: int = 24
    grace_minutes: int = DEFAULT_GRACE_MINUTES
    max_drop_fraction: float = DEFAULT_MAX_DROP_FRACTION
    min_absolute_drop: int = DEFAULT_MIN_ABSOLUTE_DROP
    confirmations: int = DEFAULT_CONFIRMATIONS
    #: Bounds on a probe that cannot see, rather than on the data.
    max_consecutive_errors: int = DEFAULT_MAX_CONSECUTIVE_ERRORS
    circuit_cooldown_minutes: int = DEFAULT_CIRCUIT_COOLDOWN_MINUTES
    #: Weekdays this model is expected to load, as ISO numbers (Monday=1).
    #: Empty means every day. A model fed by a weekday-only source is not stale
    #: on a Sunday, and reporting it as such every weekend is how a detector
    #: earns a filter rule in somebody's inbox.
    load_weekdays: tuple[int, ...] = ()

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> HealthProbe:
        return cls(
            name=str(raw["name"]),
            workspace_id=str(raw.get("workspace_id", "")),
            dataset_id=str(raw.get("dataset_id", "")),
            table=str(raw.get("table", "")),
            report_name=str(raw.get("report_name", "")),
            date_column=str(raw.get("date_column", "")),
            date_table=str(raw.get("date_table", "")),
            watch_schema=bool(raw.get("watch_schema", False)),
            row_count_table=str(raw.get("row_count_table", "")),
            control_measures=tuple(raw.get("control_measures") or ()),
            expected_lag_hours=int(raw.get("expected_lag_hours", 24)),
            grace_minutes=int(raw.get("grace_minutes", DEFAULT_GRACE_MINUTES)),
            max_drop_fraction=float(raw.get("max_drop_fraction", DEFAULT_MAX_DROP_FRACTION)),
            min_absolute_drop=int(raw.get("min_absolute_drop", DEFAULT_MIN_ABSOLUTE_DROP)),
            confirmations=int(raw.get("confirmations", DEFAULT_CONFIRMATIONS)),
            max_consecutive_errors=int(
                raw.get("max_consecutive_errors", DEFAULT_MAX_CONSECUTIVE_ERRORS)
            ),
            circuit_cooldown_minutes=int(
                raw.get("circuit_cooldown_minutes", DEFAULT_CIRCUIT_COOLDOWN_MINUTES)
            ),
            load_weekdays=tuple(int(d) for d in (raw.get("load_weekdays") or ())),
        )


@dataclass
class HealthFinding:
    """What a sweep concluded about one probe.

    ``status`` is one of:

    ``healthy``          the reading matched expectations; baseline advanced
    ``suspect``          something looks wrong, once. Recorded, not announced
    ``confirmed``        wrong across enough scans to act on
    ``detector_fault``   we could not see. Never reported as a data problem
    """

    probe: str
    status: str
    kind: str = ""
    detail: str = ""
    report_name: str = ""
    workspace_id: str = ""
    dataset_id: str = ""
    observed: dict[str, Any] = field(default_factory=dict)
    baseline: dict[str, Any] = field(default_factory=dict)
    suspect_count: int = 0

    @property
    def actionable(self) -> bool:
        return self.status == "confirmed"


def _parse_date(value: str) -> datetime | None:
    for pattern in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            trimmed = value[:19] if "T" in value else value[:10]
            return datetime.strptime(trimmed, pattern).replace(tzinfo=UTC)
        except (TypeError, ValueError):
            continue
    return None


def _parse_timestamp(value: str) -> datetime | None:
    """Read an ISO timestamp written by this module, tolerating a missing one."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _expected_loads_between(
    watermark: date, today: date, weekdays: tuple[int, ...]
) -> int:
    """How many expected load days have passed since the data last moved.

    Counts the days *after* the watermark up to and including today, so a model
    that loaded on its most recent expected day scores zero and is not stale.
    """
    allowed = set(weekdays)
    days = (today - watermark).days
    if days <= 0:
        return 0
    return sum(
        1
        for offset in range(1, days + 1)
        if (watermark + timedelta(days=offset)).isoweekday() in allowed
    )


class SilentFailureScanner:
    """Compares a semantic model against its own history.

    ``now`` is injectable for the same reason ``PolicyLedger`` takes a clock:
    staleness is a statement about elapsed time, and a test that has to wait a
    day to assert it is not a test.
    """

    def __init__(
        self,
        client: SemanticModelHealthClient,
        store: SemanticHealthStore,
        *,
        now: datetime | None = None,
        pace_seconds: float = 0.0,
    ) -> None:
        self._client = client
        self._store = store
        self._now = now
        self._pace_seconds = pace_seconds

    def _clock(self) -> datetime:
        return self._now or datetime.now(UTC)

    async def scan(self, probe: HealthProbe) -> HealthFinding:
        """Take one reading and decide what it means."""
        with tool_span(
            "semantic_health_probe",
            probe_name=probe.name,
            workspace_id=probe.workspace_id,
            dataset_id=probe.dataset_id,
            watch_schema=probe.watch_schema,
        ) as span:
            finding = await self._scan(probe)
            # Metadata only, and deliberately no observed values: a watermark
            # or a row count is customer data, and traces are retained and
            # widely readable inside a tenant. Status and kind are enough to
            # see the detector working without publishing what it saw.
            span.set("probe.status", finding.status)
            span.set("probe.kind", finding.kind or "none")
            span.set("probe.suspect_count", finding.suspect_count)
            return finding

    async def _scan(self, probe: HealthProbe) -> HealthFinding:
        state = self._store.get(
            probe.workspace_id, probe.dataset_id, probe.name
        ) or ProbeState(
            workspace_id=probe.workspace_id,
            dataset_id=probe.dataset_id,
            probe_name=probe.name,
            report_name=probe.report_name,
        )

        parked = self._circuit_open(probe, state)
        if parked is not None:
            return parked

        try:
            result = await self._client.run_probe(
                probe.workspace_id,
                probe.dataset_id,
                table=probe.table,
                date_column=probe.date_column,
                date_table=probe.date_table,
                row_count_table=probe.row_count_table,
                control_measures=probe.control_measures,
            )
        except Exception as exc:  # noqa: BLE001 - an unreachable model is data
            result = ProbeResult(
                ok=False, error=f"{type(exc).__name__}: {exc}", detector_fault=True
            )

        if not result.ok:
            return self._record_fault(probe, state, result)

        state.consecutive_errors = 0
        state.last_error = ""
        # A reading that worked clears the breaker, so a fixed permission does
        # not leave the probe parked until its cooldown happens to elapse.
        state.circuit_opened_at = ""
        state.observations += 1

        kind, detail = self._judge(probe, state, result)

        # The schema is read even when the data already looks wrong. Skipping it
        # meant a model with a standing finding never established a schema
        # baseline at all, so drift could never be detected on precisely the
        # models most likely to be broken -- monitoring that silently does
        # nothing, which is the failure this component exists to prevent.
        #
        # Reporting order still favours the data: a model that stopped loading
        # is the more urgent sentence. A drift found underneath it is recorded
        # and surfaces once the louder problem clears.
        schema_kind, schema_detail = await self._check_schema(probe, state)
        if kind == "":
            kind, detail = schema_kind, schema_detail

        if kind == "":
            return self._record_healthy(probe, state, result)
        return self._record_suspect(probe, state, result, kind, detail)

    # --- judgement ---------------------------------------------------------

    def _judge(
        self, probe: HealthProbe, state: ProbeState, result: ProbeResult
    ) -> tuple[str, str]:
        """Return (kind, detail), or ("", "") when the reading looks fine.

        Order matters: staleness is checked first because a model that loaded
        nothing also has an unchanged row count, and reporting that as a
        collapse would describe the symptom rather than the failure.
        """
        stale = self._check_staleness(probe, result)
        if stale:
            return "stale", stale

        collapse = self._check_row_count(probe, state, result)
        if collapse:
            return "row_collapse", collapse

        return "", ""

    def _check_staleness(self, probe: HealthProbe, result: ProbeResult) -> str:
        if not probe.date_column or not result.max_date:
            return ""

        observed = _parse_date(result.max_date)
        if observed is None:
            # Unreadable is not stale. Saying otherwise would alert on a
            # formatting change.
            logger.warning("Probe %s returned unparseable date %r", probe.name, result.max_date)
            return ""

        # Compared as dates, not instants. A business-date watermark is a date:
        # it reads "2026-08-31", which parses to midnight and would otherwise
        # look almost a full day older than it is. Against a timestamp deadline
        # that makes a correctly-working daily model appear stale every single
        # day -- the detector would fire on health, which is the fastest
        # possible way to get it switched off.
        deadline = (
            self._clock() - timedelta(hours=probe.expected_lag_hours, minutes=probe.grace_minutes)
        ).date()
        if observed.date() >= deadline:
            return ""

        if probe.load_weekdays:
            # Only count days the model was expected to load. A weekday-only
            # feed read on a Monday is legitimately three days behind, and a
            # detector that calls that stale fires every weekend until somebody
            # mutes it -- taking the real findings with it.
            expected = _expected_loads_between(
                observed.date(), self._clock().date(), probe.load_weekdays
            )
            if expected == 0:
                return ""

        behind = self._clock().date() - observed.date()
        return (
            f"The refresh reported success, but the newest data is {result.max_date} "
            f"-- {behind.days} day(s) old, past the expected lag of "
            f"{probe.expected_lag_hours}h plus {probe.grace_minutes} minutes' grace."
        )

    def _check_row_count(
        self, probe: HealthProbe, state: ProbeState, result: ProbeResult
    ) -> str:
        baseline = state.last_row_count
        current = result.row_count
        if baseline is None or current is None or baseline <= 0:
            # Nothing to compare against yet. A first observation cannot be a
            # collapse, and treating it as one would alert on every new probe.
            return ""

        dropped = baseline - current
        if dropped <= 0:
            return ""

        # Both thresholds, deliberately. Relative alone alerts on tiny tables;
        # absolute alone never fires on a small one that genuinely emptied.
        if dropped < probe.min_absolute_drop:
            return ""
        if dropped / baseline < probe.max_drop_fraction:
            return ""

        return (
            f"The refresh reported success, but {probe.table} holds {current:,} rows "
            f"against a baseline of {baseline:,} -- a drop of {dropped:,} "
            f"({dropped / baseline:.0%})."
        )

    async def _check_schema(
        self, probe: HealthProbe, state: ProbeState
    ) -> tuple[str, str]:
        """Report things that vanished, not things that appeared.

        Checked after the data questions because a stale or collapsed model is
        the more urgent report, and because a removed column often explains a
        collapse that has already been raised.

        **Only removals count.** Models gain columns and measures constantly;
        treating that as drift would make the detector fire on ordinary
        development and get it switched off within a week. A column that
        disappears is different: every report and measure built on it is now
        broken or silently wrong, and nothing in Power BI announces it.

        A schema that cannot be read is a detector fault, never "everything was
        deleted".
        """
        if not probe.watch_schema:
            return "", ""

        reader = getattr(self._client, "read_schema", None)
        if reader is None:
            return "", ""

        try:
            reading = await reader(probe.workspace_id, probe.dataset_id)
        except Exception as exc:  # noqa: BLE001 - an unreadable schema is not a data finding
            logger.warning("Schema probe for %s raised %s", probe.name, type(exc).__name__)
            return "", ""

        if not reading.ok:
            logger.warning("Schema probe for %s could not run: %s", probe.name, reading.error[:200])
            return "", ""

        current = tuple(reading.entries)
        previous = tuple(state.last_schema)

        if not previous:
            # First sighting establishes the shape. A baseline is not a finding.
            state.last_schema = list(current)
            return "", ""

        missing = [e for e in previous if e not in current]
        if not missing:
            # Additions are normal evolution; keep the baseline current so a
            # later removal is measured against what is really there now.
            state.last_schema = list(current)
            return "", ""

        shown = ", ".join(m.split(":", 1)[-1] for m in missing[:5])
        more = f" and {len(missing) - 5} more" if len(missing) > 5 else ""
        return "schema_drift", (
            f"The refresh reported success, but {len(missing)} object(s) the model "
            f"used to expose are gone: {shown}{more}. Reports and measures built on "
            f"them are broken or silently wrong."
        )

    def _circuit_open(
        self, probe: HealthProbe, state: ProbeState
    ) -> HealthFinding | None:
        """Park a probe that keeps failing, and say so once.

        Returns a finding when the probe should be skipped, ``None`` to proceed.

        Without this, a probe that can never succeed re-queries on every sweep
        for ever. That is not hypothetical: a probe pointed at a Direct Lake
        model, which app-only callers are not permitted to query at all,
        reached twelve consecutive 401s. Retrying was pure cost -- load on the
        capacity being watched, and a fault line in every sweep that trains
        people to skim past the output.

        Reopened by time rather than by anyone remembering, so a fixed
        permission is picked up without a redeploy.
        """
        if state.consecutive_errors < probe.max_consecutive_errors:
            return None

        opened = _parse_timestamp(state.circuit_opened_at)
        if opened is None:
            state.circuit_opened_at = self._clock().isoformat(timespec="seconds")
            self._store.put(state)
            opened = self._clock()

        due = opened + timedelta(minutes=probe.circuit_cooldown_minutes)
        if self._clock() >= due:
            # Half-open: allow exactly one attempt. A success clears the count
            # and the timestamp; another failure re-arms the cooldown.
            state.circuit_opened_at = ""
            self._store.put(state)
            logger.info("Probe %s retrying after cooldown", probe.name)
            return None

        logger.info(
            "Probe %s parked after %d consecutive faults; next attempt %s",
            probe.name, state.consecutive_errors, due.isoformat(timespec="minutes"),
        )
        return HealthFinding(
            probe=probe.name,
            status="parked",
            kind="detector_fault",
            detail=(
                f"Parked after {state.consecutive_errors} consecutive failures. "
                f"Last error: {state.last_error[:200]} "
                f"Next attempt after {due.isoformat(timespec='minutes')}."
            ),
            report_name=probe.report_name,
            workspace_id=probe.workspace_id,
            dataset_id=probe.dataset_id,
        )

    # --- outcomes ----------------------------------------------------------

    def _record_healthy(
        self, probe: HealthProbe, state: ProbeState, result: ProbeResult
    ) -> HealthFinding:
        state.last_max_date = result.max_date or state.last_max_date
        if result.row_count is not None:
            state.last_row_count = result.row_count
        if result.control_totals:
            state.last_control_totals = dict(result.control_totals)
        state.last_healthy_at = self._clock().isoformat(timespec="seconds")
        state.suspect_count = 0
        state.first_suspect_at = ""
        state.suspect_kind = ""
        self._store.put(state)

        return HealthFinding(
            probe=probe.name,
            status="healthy",
            report_name=probe.report_name,
            workspace_id=probe.workspace_id,
            dataset_id=probe.dataset_id,
            observed={"max_date": result.max_date, "row_count": result.row_count},
        )

    def _record_suspect(
        self,
        probe: HealthProbe,
        state: ProbeState,
        result: ProbeResult,
        kind: str,
        detail: str,
    ) -> HealthFinding:
        # A different kind of wrongness restarts the count. Two unrelated
        # single anomalies should not add up to one confirmed finding.
        if state.suspect_kind != kind:
            state.suspect_count = 0
            state.first_suspect_at = self._clock().isoformat(timespec="seconds")
        state.suspect_kind = kind
        state.suspect_count += 1

        # Baseline deliberately NOT advanced. Accepting a suspect reading as
        # the new normal is how a detector learns to ignore the failure it
        # exists to find.
        self._store.put(state)

        confirmed = state.suspect_count >= probe.confirmations
        logger.info(
            "Probe %s %s (%d/%d): %s",
            probe.name,
            "confirmed" if confirmed else "suspect",
            state.suspect_count,
            probe.confirmations,
            detail,
        )
        return HealthFinding(
            probe=probe.name,
            status="confirmed" if confirmed else "suspect",
            kind=kind,
            detail=detail,
            report_name=probe.report_name,
            workspace_id=probe.workspace_id,
            dataset_id=probe.dataset_id,
            observed={"max_date": result.max_date, "row_count": result.row_count},
            baseline={
                "max_date": state.last_max_date,
                "row_count": state.last_row_count,
            },
            suspect_count=state.suspect_count,
        )

    def _record_fault(
        self, probe: HealthProbe, state: ProbeState, result: ProbeResult
    ) -> HealthFinding:
        """A probe that could not run says nothing about the data.

        Kept strictly apart from data findings. "We cannot see this model" and
        "this model is stale" need different people and different urgency, and
        merging them means a permissions change reads as a data outage.
        """
        state.consecutive_errors += 1
        state.last_error = result.error[:400]
        self._store.put(state)
        logger.warning(
            "Probe %s could not run (%d consecutive): %s",
            probe.name,
            state.consecutive_errors,
            result.error[:200],
        )
        return HealthFinding(
            probe=probe.name,
            status="detector_fault",
            kind="detector_fault",
            detail=result.error,
            report_name=probe.report_name,
            workspace_id=probe.workspace_id,
            dataset_id=probe.dataset_id,
        )

    async def sweep(self, probes: list[HealthProbe]) -> list[HealthFinding]:
        """Probe every configured model, paced so the detector stays small.

        Sequential on purpose. The obvious improvement is to run probes
        concurrently, and it is the wrong one: ``executeQueries`` is throttled
        per user across *all* datasets, and a detector that trips that limit
        becomes the capacity incident it was watching for. Sequential with a
        pause keeps the cost of watching proportional to the thing watched.
        """
        findings: list[HealthFinding] = []
        for index, probe in enumerate(probes):
            if index and self._pace_seconds:
                await asyncio.sleep(self._pace_seconds)
            findings.append(await self.scan(probe))
        return findings


def load_probes(raw: Any) -> list[HealthProbe]:
    """Read probe configuration, disabling only the detector when it is wrong.

    Accepts the raw JSON string that arrives from the environment, or an
    already-parsed list. Nothing here raises: a typo in detector configuration
    must never stop the agent starting, because the same process also handles
    mail triage, approvals and remediation. The proportionate response is a
    detector that watches nothing and says so in the log.

    That is not theoretical. An unset ``SILENT_HEALTH_PROBES`` once crashed the
    container at import, and every capability went down with it.
    """
    if raw is None:
        return []

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            logger.error(
                "SILENT_HEALTH_PROBES is not valid JSON, so the silent-failure "
                "detector will watch nothing. Everything else is unaffected."
            )
            return []

    if not isinstance(raw, list):
        logger.error("SILENT_HEALTH_PROBES must be a JSON list; watching nothing.")
        return []

    probes: list[HealthProbe] = []
    for entry in raw:
        try:
            probes.append(HealthProbe.from_dict(entry))
        except Exception as exc:  # noqa: BLE001
            # One bad probe must not stop the other models being checked.
            logger.warning("Skipping unreadable probe config (%s)", type(exc).__name__)
    return probes
