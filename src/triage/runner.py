"""Scenario runner — wires everything together for one triage run.

Responsibilities the agent deliberately does NOT have:

* computing the failure signature,
* looking up whether an open incident already exists,
* persisting the terminal outcome.

Keeping those here means the agent is a pure loop that can be tested without a
store, and the dedup/persistence behaviour can be tested without a model.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage_agent import EventHook, TriageAgent, TriageDeps
from triage.detectors.silent_failures import (
    HealthFinding,
    SilentFailureScanner,
    load_probes,
)
from triage.models import BIRequest, Incident, TriageResult
from triage.pipeline_models import (
    PIPELINE_JOB_TYPES,
    PIPELINE_TERMINAL_STATUSES,
    PipelineActivity,
    PipelineFailure,
    PipelineRun,
    PipelineTarget,
    load_pipeline_targets,
)
from triage.policy import TriagePolicy
from triage.providers import get_provider
from triage.signature import compute_signature, incident_id
from triage.store.approvals import JsonFileApprovalChannel
from triage.store.claims import ClaimStore, build_claim_store
from triage.store.incidents import IncidentStore, JsonFileIncidentStore
from triage.store.pipeline_reruns import (
    FabricSqlPipelineRerunStore,
    JsonFilePipelineRerunStore,
    PipelineRerunStore,
)
from triage.store.processed import JsonFileProcessedLog
from triage.store.retries import JsonFileRetryStore
from triage.store.semantic_health import JsonFileSemanticHealthStore
from triage.tools.dataset import DatasetSource
from triage.tools.fabric_pipeline import (
    FabricPipelineClient,
    MockFabricPipelineClient,
    PipelineApiError,
)
from triage.tools.flags import DataQualityFlagTable
from triage.tools.inbox import GraphInbox, MockInbox
from triage.tools.pipeline_actions import PipelineToolContext, verify_rerun
from triage.tools.powerbi import LivePowerBIClient, MockPowerBIClient
from triage.tools.semantic_health import (
    LiveSemanticHealthClient,
    MockSemanticHealthClient,
)
from triage.tools.teams import (
    MockTeamsNotifier,
    ResolutionSummary,
    WorkflowsWebhookTeamsNotifier,
)

logger = logging.getLogger("triage.runner")


def _pipeline_signature(failure: PipelineFailure) -> str:
    return compute_signature(
        source="fabric_pipeline_failure",
        error=failure.error_text() or "Unspecified pipeline failure",
        artifact_kind="pipeline", artifact_name=failure.target.key,
        exception_class=failure.run.error_code,
    )[0]


def _require_live_config(component: str, **values: str) -> None:
    """Refuse to build a live component whose configuration is missing.

    Every builder below used to read ``if live and <one setting>: live else:
    mock``, so a deployment that asked for live tools and lacked a setting got
    a mock and no warning.

    That is not a harmless default. ``MockPowerBIClient`` reports a refresh as
    ``Completed`` and ``MockTeamsNotifier`` reports ``delivered: True``, so the
    run records "resolved, refresh succeeded, Teams notified" having triggered
    no refresh and posted nothing. The incident store then carries a terminal
    outcome that is a fabrication, and the notification dedup counter suppresses
    the first real notification once the configuration is fixed.

    Fail at construction instead. A deployment that cannot do its job should say
    so on the way up, not report success on the way down.
    """
    missing = sorted(name.upper() for name, value in values.items() if not value)
    if missing:
        raise ValueError(
            f"TRIAGE_TOOL_MODE=live needs {', '.join(missing)} to build the "
            f"{component}. Set them, or set TRIAGE_TOOL_MODE=mock. Falling back "
            "to a mock here would report success for work that never happened."
        )


def _fault_summary(detail: str) -> str:
    """Show the operator what to do, not what the platform said.

    A detector fault gets one short line in the sweep output, and Power BI's
    error payload is long enough to fill it entirely with a JSON blob that says
    only ``PowerBINotAuthorizedException``. Truncating the raw detail therefore
    cut off the hint explaining the cause -- the guidance existed, was correct,
    and never reached the person reading the line.

    So the hint wins the space when there is one. The full platform error is
    still recorded on the probe state for anyone diagnosing it afterwards.
    """
    marker = "\nHint: "
    if marker in detail:
        head, _, hint = detail.partition(marker)
        code = head.split(":", 1)[0].strip()
        return f"{code} -- {' '.join(hint.split())}"
    return detail[:80]


@dataclass
class Expectation:
    """What a scenario asserts. Drives both the tests and the run sheet."""

    outcome: str = ""
    remediation_applied: bool | None = None
    flags_written: int | None = None
    dq_has_issue: bool | None = None
    blocked_attempts: int | None = None
    approval_requested: bool | None = None
    approval_granted: bool | None = None
    denied_actions: int | None = None
    pipeline_reruns: int | None = None
    incident_source: str = ""


@dataclass
class Scenario:
    name: str
    title: str = ""
    description: str = ""
    email: str = ""
    datasets: list[dict[str, Any]] = field(default_factory=list)
    workspace_id: str = "00000000-0000-0000-0000-000000000000"
    dataset_id: str = "00000000-0000-0000-0000-000000000000"
    refresh_result: str = "Completed"
    refresh_history: list[dict[str, Any]] = field(default_factory=list)
    retry_after_seconds: int = 0
    #: auto_approve | auto_deny | timeout | none
    approval: str = "auto_approve"
    approver: str = "m.hughes@contoso.com"
    approval_reason: str = ""
    rogue_second_refresh: bool = False
    rogue_unknown_action: bool = False
    #: Drive the scripted provider down the disabled-schedule branch. A flag
    #: rather than an unconditional check so the other six scenarios keep their
    #: exact tool sequences -- their expect blocks are the test.
    check_schedule: bool = False
    schedule_enabled: bool = True
    reset_flags: bool = True
    reset_incidents: bool = True
    repeat: int = 1
    expect: Expectation = field(default_factory=Expectation)
    narration: list[str] = field(default_factory=list)
    pipeline: dict[str, Any] | None = None

    @classmethod
    def load(cls, path: str | Path) -> Scenario:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        expect = Expectation(**(raw.pop("expect", None) or {}))
        pbi = raw.pop("powerbi", None) or {}
        provider = raw.pop("provider", None) or {}
        approval = raw.pop("approval", None) or {}
        return cls(
            expect=expect,
            workspace_id=pbi.get("workspace_id", cls.workspace_id),
            dataset_id=pbi.get("dataset_id", cls.dataset_id),
            refresh_result=pbi.get("refresh_result", "Completed"),
            refresh_history=list(pbi.get("refresh_history") or []),
            retry_after_seconds=int(pbi.get("retry_after_seconds", 0)),
            approval=approval.get("mode", "auto_approve"),
            approver=approval.get("approver", "m.hughes@contoso.com"),
            approval_reason=approval.get("reason", ""),
            rogue_second_refresh=bool(provider.get("rogue_second_refresh", False)),
            rogue_unknown_action=bool(provider.get("rogue_unknown_action", False)),
            check_schedule=bool(provider.get("check_schedule", False)),
            schedule_enabled=bool(pbi.get("schedule_enabled", True)),
            **raw,
        )


@dataclass
class RunArtifacts:
    """Everything a caller (CLI or test) needs after a run."""

    result: TriageResult
    incident: Incident | None
    request: BIRequest
    flag_rows_before: int
    flag_rows_after: int
    teams_messages: list[Any] = field(default_factory=list)
    powerbi_calls: list[Any] = field(default_factory=list)
    pipeline_calls: list[Any] = field(default_factory=list)
    run_id: str = ""


@dataclass
class PipelineSweepReport:
    status: str = "completed"
    checked: int = 0
    skipped: int = 0
    artifacts: list[RunArtifacts] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)
    faults: list[str] = field(default_factory=list)

    def summary(self) -> str:
        heading = (
            f"Pipeline sweep {self.status}: checked {self.checked} target(s), "
            f"triaged {len(self.artifacts)} run(s), skipped {self.skipped}."
        )
        return "\n".join([heading, *self.lines, *self.faults])


class TriageRunner:
    def __init__(
        self,
        settings,
        *,
        base_dir: Path,
        store: IncidentStore | None = None,
        on_event: EventHook | None = None,
        flag_table_path: Path | None = None,
        retry_store_path: Path | None = None,
        semantic_health_path: Path | None = None,
        command_center_store: Any = None,
    ):
        self.settings = settings
        self.base_dir = Path(base_dir)
        self.on_event = on_event
        # One database handle shared by every store. A hosted agent is
        # constructed fresh for each request, so six separate connections would
        # mean six Entra logins per alert rather than one.
        self._sql = self._build_sql()
        self.store: IncidentStore = store or self._build_store()
        self.flag_table = DataQualityFlagTable(
            flag_table_path or (self.base_dir / "runs" / "dq_flags.csv")
        )
        # Built once per runner rather than per run: a deferral written by one
        # run has to be visible to the precondition check of the next.
        self.retries = self.build_retry_store(retry_store_path)
        self.semantic_health = self.build_semantic_health_store(semantic_health_path)
        self._teams = None
        self._pipeline_claims: ClaimStore | None = None
        self._command_center_store = command_center_store

    # --- inbox -------------------------------------------------------------

    @staticmethod
    @staticmethod
    def _resolve_id(
        *candidates: str,
        untrusted: str = "",
        label: str = "id",
    ) -> str:
        """First non-empty id wins: scenario, then configuration, then the alert.

        Without a fallback the live path called Power BI with empty ids and got a
        404, and the agent reached a plausible-looking conclusion from a tool
        failure rather than from evidence. A wrong answer that reads correctly is
        the worst kind.

        Configuration deliberately outranks the alert. The alert is an email, and
        the sender of an email is trivially forged, so an attacker who gets one
        message past the inbox filter could otherwise name any workspace or
        dataset and have the agent act on it -- bounded only by what the
        controller's identity happens to reach.

        A deployment that watches many models leaves the configured ids empty and
        the alert is used, which is the same behaviour as before. That case is
        steerable by construction, so the disagreement is logged rather than
        hidden.
        """
        resolved = ""
        for candidate in candidates:
            if candidate and candidate.strip():
                resolved = candidate.strip()
                break

        if untrusted and untrusted.strip() and untrusted.strip() != resolved:
            logger.warning(
                "Ignoring %s id %r supplied by the alert; using configured %r",
                label,
                untrusted.strip(),
                resolved,
            )
        return resolved

    @property
    def sql(self):
        """The shared Fabric SQL handle, or None when running offline.

        Exposed so the hosted entry point can build its claim store on the same
        connection instead of opening a second one.
        """
        return self._sql

    def _build_sql(self):
        """Open the shared Fabric SQL handle, or return None when unconfigured.

        Returning None is the offline path: every store then falls back to its
        JSON file, which is correct on a laptop and needs no driver installed.
        The schema is created here rather than by a migration step, so an
        adopter pointing at an empty database gets a working system without a
        separate command.
        """
        server = getattr(self.settings, "fabric_sql_server", "")
        database = getattr(self.settings, "fabric_sql_database", "")
        if not server or not database:
            return None

        from triage.store.fabric_sql import FabricSqlDatabase

        db = FabricSqlDatabase(
            server=server, database=database, tables=self._sql_tables()
        )
        if not db.ensure_schema_once():
            logger.error(
                "Could not prepare the triage schema in %s. Stores will run "
                "degraded and will retry, including the schema, on use.",
                db.target,
            )
        return db

    def _sql_tables(self) -> dict[str, str]:
        s = self.settings
        return {
            "incidents": s.incident_table_name,
            "processed": s.processed_table_name,
            "approvals": s.approval_table_name,
            "retries": s.retry_table_name,
            "semantic_health": s.semantic_health_table_name,
            "leases": getattr(s, "lease_table_name", "triage_sweep_leases"),
            "claims": s.claim_table_name,
            "inbox_audit": getattr(s, "inbox_audit_table_name", "triage_inbox_audit"),
            "pipeline_reruns": s.pipeline_rerun_table_name,
            "agent_runs": s.agent_run_table_name,
            "agent_events": s.agent_event_table_name,
            "agent_commands": s.agent_command_table_name,
            "incident_activity": s.incident_activity_table_name,
        }

    def _build_store(self) -> IncidentStore:
        """Choose where incidents live.

        Falls back to the JSON file when no database is configured, so the
        offline rehearsal path is unchanged and needs no SQL driver.
        """
        if self._sql is None:
            return JsonFileIncidentStore(self.base_dir / "runs" / "incidents.json")

        from triage.store.sql_incidents import FabricSqlIncidentStore

        store = FabricSqlIncidentStore(
            db=self._sql, table=self.settings.incident_table_name
        )
        if not store.is_durable:
            logger.warning(
                "Incident table %s unavailable; incidents will not survive a restart",
                self.settings.incident_table_name,
            )
        return store

    def build_processed_log(self):
        """Where the record of already-triaged mail lives.

        Mirrors ``_build_store``: the database when one is configured, a JSON
        file otherwise. This has to outlive the process -- a hosted agent is
        rebuilt for every invocation, so anything held in memory here is always
        empty on arrival.
        """
        if self._sql is None:
            return JsonFileProcessedLog(self.base_dir / "runs" / "processed.json")

        from triage.store.processed import FabricSqlProcessedLog

        log = FabricSqlProcessedLog(
            db=self._sql, table=self.settings.processed_table_name
        )
        if not log.is_durable:
            logger.error(
                "Processed-message log %s is not durable; scheduled sweeps will "
                "re-triage the same mail and notify repeatedly",
                self.settings.processed_table_name,
            )
        return log

    def build_inbox_audit(self, path: Path | None = None):
        """Where refused messages are recorded.

        Durable when a database is configured, a JSON file otherwise. Losing it
        costs evidence rather than correctness — the filter refuses the message
        either way — so unlike the claim store this one never blocks ingestion.
        """
        from triage.store.inbox_audit import JsonFileInboxAudit

        if path is not None:
            return JsonFileInboxAudit(path)

        if self._sql is None:
            return JsonFileInboxAudit(self.base_dir / "runs" / "inbox_audit.json")

        from triage.store.inbox_audit import FabricSqlInboxAudit

        return FabricSqlInboxAudit(
            db=self._sql,
            table=getattr(self.settings, "inbox_audit_table_name", "triage_inbox_audit"),
        )

    def build_inbox(self):
        if self.settings.triage_tool_mode == "live":
            _require_live_config(
                "Graph inbox",
                graph_tenant_id=self.settings.graph_tenant_id,
                graph_client_id=self.settings.graph_client_id,
                graph_client_secret=self.settings.graph_client_secret,
                graph_mailbox=self.settings.graph_mailbox,
            )
            from triage.tools.mail_filter import MailFilter

            return GraphInbox(
                tenant_id=self.settings.graph_tenant_id,
                client_id=self.settings.graph_client_id,
                client_secret=self.settings.graph_client_secret,
                mailbox=self.settings.graph_mailbox,
                # Without this the agent acts on every message in the mailbox,
                # which makes it steerable by anyone who can email it.
                mail_filter=MailFilter.build(
                    senders=self.settings.graph_sender_allowlist,
                    subject_pattern=self.settings.graph_subject_pattern,
                ),
                processed_log=self.build_processed_log(),
                # Refusals are recorded, not just counted: "ignored 10 messages"
                # reads the same whether the filter is working or has gone deaf.
                audit_log=self.build_inbox_audit(),
            )
        return MockInbox(directory=self.base_dir / "mock" / "emails")

    # --- clients -----------------------------------------------------------

    def build_powerbi(self, scenario: Scenario | None = None):
        if self.settings.triage_tool_mode == "live":
            _require_live_config(
                "Power BI client",
                powerbi_tenant_id=self.settings.powerbi_tenant_id,
            )
            return LivePowerBIClient(
                tenant_id=self.settings.powerbi_tenant_id,
                client_id=self.settings.powerbi_client_id,
                client_secret=self.settings.powerbi_client_secret,
            )
        return MockPowerBIClient(
            refresh_result=(scenario.refresh_result if scenario else "Completed"),
            history=list(scenario.refresh_history) if scenario else [],
            schedule_enabled=(scenario.schedule_enabled if scenario else True),
            retry_after_seconds=(scenario.retry_after_seconds if scenario else 0),
        )

    async def drain_due_retries(
        self, *, now: datetime | None = None, claims: Any = None
    ) -> list[str]:
        """Perform the retries whose window has passed.

        Deliberately deterministic and model-free. The decision was already
        made and recorded -- "refresh this dataset after T" -- so re-running
        triage would ask a model to re-derive a conclusion that is already on
        disk, and would trip the known-incident check and suppress the very
        work it was sent to do.

        Nothing drains itself. Without this, ``defer_refresh_retry`` writes a
        row, reports scheduled work, and the retry never happens.

        ``now`` is injectable so the wait can be tested without waiting, the
        same reason :class:`PolicyLedger` takes a clock.

        ``claims`` is optional and should be supplied by any caller that can run
        concurrently with itself. A refresh is a real, effectful action, and
        ``due()`` and ``complete()`` are separate statements: two replicas
        draining at the same moment both see the same row as due and both issue
        the refresh. The mailbox path has always claimed per message; this is
        the same primitive applied to the path that acts without a message.
        """
        if self.retries is None:
            return []

        due = self.retries.due(now=now)
        if not due:
            return []

        lines: list[str] = []
        powerbi = self.build_powerbi()

        for row in due:
            signature = str(row.get("signature", ""))

            # Claimed on the signature, which is what identifies the work --
            # a retry row has no message id to key on.
            claim_key = f"retry:{signature}" if signature else ""
            if claims is not None and claim_key:
                if not claims.claim(claim_key):
                    logger.info(
                        "Skipping deferred retry for %s: claimed by another "
                        "invocation", signature
                    )
                    continue
            try:
                lines.extend(await self._drain_one_retry(row, powerbi))
            finally:
                # Held until the row is completed or re-deferred, not just until
                # the refresh returns: releasing at the refresh would let a
                # second drainer see the row as still due and fire again.
                if claims is not None and claim_key:
                    claims.release(claim_key)

        logger.info("Drained %d due retry/retries", len(due))
        return lines

    async def _drain_one_retry(self, row: dict, powerbi: Any) -> list[str]:
        """One due retry, start to finish. Caller owns the claim."""
        assert self.retries is not None
        lines: list[str] = []
        signature = str(row.get("signature", ""))
        report = str(row.get("report_name") or "the dataset")
        try:
            outcome = await powerbi.refresh_dataset(
                str(row.get("workspace_id", "")), str(row.get("dataset_id", ""))
            )
        except Exception as exc:  # noqa: BLE001 - a failed retry is data
            logger.warning(
                "Deferred retry for %s raised %s", signature, type(exc).__name__
            )
            self.retries.complete(signature, outcome=f"error:{type(exc).__name__}")
            return [f"- {report}: retry failed ({type(exc).__name__})"]

        if outcome.succeeded:
            self.retries.complete(signature, outcome="resolved")
            # Close the incident too. An incident left open after the thing
            # was fixed keeps suppressing new alerts, so a genuine
            # recurrence is silently swallowed.
            open_incident = self.store.find_open(signature)
            if open_incident is not None:
                self.store.mark(
                    open_incident.id,
                    "resolved",
                    "Deferred retry completed after the throttling window.",
                )
            lines.append(f"- {report}: deferred retry completed")
        elif outcome.throttled:
            # Still throttled. Back off further, or give up and say so --
            # the store enforces the attempt limit.
            again = self.retries.defer(
                signature=signature,
                request_id=str(row.get("request_id", "")),
                workspace_id=str(row.get("workspace_id", "")),
                dataset_id=str(row.get("dataset_id", "")),
                report_name=report,
                reason="Still throttled when the retry window arrived.",
                retry_after_seconds=outcome.retry_after_seconds,
            )
            if again.get("status") == "pending":
                lines.append(
                    f"- {report}: still throttled, retry {again.get('attempts')} "
                    f"due {again.get('due_at')}"
                )
            else:
                lines.append(
                    f"- {report}: still throttled after the deferral limit; "
                    "needs a human"
                )
        else:
            self.retries.complete(signature, outcome=f"failed:{outcome.status}")
            lines.append(f"- {report}: retry ran and failed ({outcome.status})")

        return lines

    async def silent_sweep(self, *, now: datetime | None = None) -> list[str]:
        """Look for failures that never sent an alert.

        Separate from the mailbox sweep because the trigger is different: there
        is nothing to react to, so this polls. A confirmed finding becomes an
        incident with the same signature discipline as an emailed failure, so
        the existing deduplication applies and a detector that polls every
        fifteen minutes announces once rather than every time it looks.

        Returns one line per probe that needed saying something about. Healthy
        models are silent; a detector that reports "still fine" every quarter
        hour is a detector people filter.
        """
        probes = load_probes(self.settings.silent_health_probes)
        if not probes:
            return []
        if not self.settings.silent_sweep_enabled:
            # Disabling one scheduler does not disable manual calls to the
            # controller. Every entry point must honor the same off switch.
            logger.info("Silent sweep is disabled in configuration; skipping %d probe(s)", len(probes))
            return []

        # One sweep at a time across every instance. A schedule can wake more
        # than one container, and two sweeps running together would each
        # increment the same probe's suspect count -- confirming a finding on
        # its first real occurrence and turning the rule that prevents false
        # positives into a source of them.
        owner = f"{socket.gethostname()}-{os.getpid()}"
        lease_ttl = max(60, self.settings.triage_timeout_seconds)
        if not self.semantic_health.try_acquire_lease("silent-sweep", owner, lease_ttl):
            logger.info("Another instance holds the sweep lease; skipping this run")
            return []

        try:
            scanner = SilentFailureScanner(
                self.build_health_client(),
                self.semantic_health,
                now=now,
                pace_seconds=self.settings.silent_probe_pace_seconds,
            )
            findings = await scanner.sweep(probes)
        finally:
            self.semantic_health.release_lease("silent-sweep", owner)

        lines: list[str] = []
        for finding in findings:
            if finding.status == "healthy":
                continue

            if finding.status == "suspect":
                # Deliberately quiet. One odd reading is not a finding, and
                # saying so out loud would be the false positive.
                lines.append(
                    f"- {finding.report_name or finding.probe}: possible {finding.kind}, "
                    f"awaiting confirmation ({finding.suspect_count})"
                )
                continue

            if finding.status == "detector_fault":
                lines.append(
                    f"- {finding.report_name or finding.probe}: probe could not run "
                    f"({_fault_summary(finding.detail)})"
                )
                continue

            lines.append(await self._record_silent_finding(finding))

        return lines

    async def _record_silent_finding(self, finding: HealthFinding) -> str:
        """Turn a confirmed finding into an incident, and say it once.

        The signature is built from the same components as an emailed failure,
        so a silent finding and a later alert about the same model collapse
        into one incident rather than two.
        """
        signature, _ = compute_signature(
            source="silent_failure",
            error=finding.detail,
            artifact_kind="semantic_model",
            artifact_name=finding.report_name or finding.probe,
            exception_class=finding.kind,
        )

        known = self.store.find_open(signature)
        already_announced = known is not None and known.notified_count > 0

        request = BIRequest(
            request_id=f"silent:{finding.probe}",
            sender="silent-failure-detector",
            subject=(
                f"Silent failure detected: {finding.report_name or finding.probe} "
                f"({finding.kind})"
            ),
            body=finding.detail,
            report_name=finding.report_name,
            dataset_id=finding.dataset_id,
            workspace_id=finding.workspace_id,
            error_code=finding.kind,
            source="detector",
        )

        result = TriageResult(
            outcome="needs_human",
            summary=finding.detail,
            request_id=request.request_id,
            signature=signature,
            root_cause=(
                "The refresh reported success, so no failure alert was raised. "
                "Found by measuring the model rather than by being told."
            ),
            action_taken="",
            notification_delivered=False,
        )

        delivered = False
        if not already_announced:
            summary = ResolutionSummary(
                title=f"Silent failure: {finding.report_name or finding.probe}",
                report_name=finding.report_name,
                error=f"{finding.kind} (no alert was raised)",
                action_taken="None. Detected by scan, not by alert.",
                outcome="needs_human",
                timestamp=datetime.now(UTC).isoformat(timespec="seconds"),
                detail=finding.detail,
                facts={
                    "Observed": str(finding.observed),
                    "Last healthy": str(finding.baseline),
                    "Confirmed after": f"{finding.suspect_count} consecutive scans",
                },
            )
            delivery = await self.build_teams().post(summary)
            delivered = bool(
                delivery.get("delivered") if isinstance(delivery, dict) else False
            )
            result = result.model_copy(update={"notification_delivered": delivered})

        self.store.record(
            result,
            report_name=finding.report_name,
            original_error=finding.detail,
            agent_name="SilentFailureScanner",
            source="silent_failure",
            notified=delivered,
        )

        state = "announced" if delivered else "already announced"
        return f"- {finding.report_name or finding.probe}: {finding.kind} confirmed, {state}"

    def build_semantic_health_store(self, path: Path | None = None):
        """Baselines for the silent-failure detector.

        Durable, or the detector cannot work at all: every sweep would start
        with no history and could never conclude that a watermark failed to
        advance, which is the only question it exists to answer.
        """
        if path is not None:
            return JsonFileSemanticHealthStore(path)

        if self._sql is None:
            return JsonFileSemanticHealthStore(self.base_dir / "runs" / "semantic_health.json")

        from triage.store.semantic_health import FabricSqlSemanticHealthStore

        store = FabricSqlSemanticHealthStore(
            db=self._sql,
            table=self.settings.semantic_health_table_name,
            lease_table=getattr(self.settings, "lease_table_name", "triage_sweep_leases"),
        )
        if not store.is_durable:
            logger.error(
                "Semantic health store %s is not durable; the silent-failure "
                "detector will start blind on every sweep and detect nothing",
                self.settings.semantic_health_table_name,
            )
        return store

    def build_health_client(self):
        if self.settings.triage_tool_mode == "live":
            _require_live_config(
                "semantic health client",
                powerbi_tenant_id=self.settings.powerbi_tenant_id,
            )
            return LiveSemanticHealthClient(
                tenant_id=self.settings.powerbi_tenant_id,
                client_id=self.settings.powerbi_client_id,
                client_secret=self.settings.powerbi_client_secret,
            )
        return MockSemanticHealthClient()

    def build_pipeline_client(self) -> FabricPipelineClient:
        if self.settings.triage_tool_mode != "live":
            return MockFabricPipelineClient()
        _require_live_config("Fabric pipeline client", fabric_tenant_id=self.settings.fabric_tenant_id)
        from triage.tools.fabric_pipeline import LiveFabricPipelineClient

        return LiveFabricPipelineClient(
            tenant_id=self.settings.fabric_tenant_id,
            client_id=self.settings.fabric_client_id,
            max_pages=self.settings.pipeline_max_pages,
        )

    def build_pipeline_rerun_store(self) -> PipelineRerunStore:
        if self._sql is not None:
            return FabricSqlPipelineRerunStore(
                db=self._sql, table=self.settings.pipeline_rerun_table_name,
            )
        return JsonFilePipelineRerunStore(self.base_dir / "runs" / "pipeline_reruns.json")

    def _pipeline_claim_store(self) -> ClaimStore:
        if self._pipeline_claims is None:
            self._pipeline_claims = build_claim_store(
                db=self._sql, table=self.settings.claim_table_name,
            )
        return self._pipeline_claims

    async def _triage_pipeline(
        self, failure: PipelineFailure, *, client: FabricPipelineClient,
        reruns: PipelineRerunStore, scenario: Scenario | None = None,
    ) -> RunArtifacts:
        signature = _pipeline_signature(failure)
        assert failure.run.end_time is not None
        request = BIRequest(
            request_id=f"pipeline:{failure.key}",
            received_at=failure.run.end_time.isoformat(),
            sender="fabric-job-monitor",
            subject=f"Scheduled Fabric pipeline failed: {failure.target.name}",
            body=failure.error_text() or "Fabric returned no failure reason.",
            report_name=failure.target.name,
            workspace_id=failure.target.workspace_id,
            error_code=failure.run.error_code,
            source="pipeline",
        )
        context = PipelineToolContext(
            failure=failure, client=client, reruns=reruns, signature=signature,
            live=self.settings.triage_tool_mode == "live",
        )
        timeout = self.settings.triage_timeout_seconds + self.settings.approval_timeout_seconds + 30
        try:
            async with asyncio.timeout(timeout):
                return await self.run_request(request, scenario=scenario, pipeline=context)
        except Exception as exc:
            logger.exception("Pipeline triage could not finish")
            result = TriageResult(
                outcome="timed_out" if isinstance(exc, TimeoutError) else "agent_crashed",
                request_id=request.request_id, signature=signature,
                summary=f"Pipeline triage failed ({type(exc).__name__}); inspect the rerun journal before replay.",
                exception_class=type(exc).__name__,
            )
            incident = self.store.record(
                result, report_name=failure.target.name,
                original_error=request.error_text(), source="fabric_pipeline_failure",
                agent_name="TriageAgent", pipeline_failure=failure,
            )
            return RunArtifacts(
                result=result, incident=incident, request=request,
                flag_rows_before=self.flag_table.row_count,
                flag_rows_after=self.flag_table.row_count,
                pipeline_calls=list(getattr(client, "calls", [])),
            )

    async def run_pipeline_failure(
        self, failure: PipelineFailure, *, client: FabricPipelineClient,
        reruns: PipelineRerunStore, scenario: Scenario | None = None,
    ) -> RunArtifacts | None:
        """Serialize this pipeline and re-check processed state inside the claim."""
        claims = self._pipeline_claim_store()
        claim_key = f"pipeline:{failure.target.key}"
        ttl = self.settings.triage_timeout_seconds + self.settings.approval_timeout_seconds + 90
        if not claims.claim(claim_key, lease_seconds=ttl):
            logger.info("Pipeline is already claimed: %s", failure.target.key)
            return None
        try:
            # Construct after acquisition: another invocation may have finished
            # while this one was reading the job history.
            processed = self.build_processed_log()
            event_key = f"pipeline:{failure.key}"
            if processed.seen(event_key):
                return None
            current = await client.get_run(failure.target, failure.run.id)
            if not current.failed_scheduled:
                logger.warning("Skipping pipeline run whose terminal/trigger evidence changed")
                return None
            try:
                activities = await client.activity_runs(failure.target, current)
                diagnostics_error = ""
            except PipelineApiError as exc:
                logger.warning("Activity diagnostics unavailable for pipeline run %s", current.id)
                activities, diagnostics_error = [], str(exc)
            failure = PipelineFailure(
                target=failure.target, run=current, recent_runs=failure.recent_runs,
                activities=activities, diagnostics_error=diagnostics_error,
            )
            signature = _pipeline_signature(failure)
            # Refresh this signature before considering old observations. A
            # bounded sweep can leave older failures in its backlog after a
            # newer failure has already been rerun and verified.
            self.store.find_open(signature)
            for resolved in (False, True):
                historical = self.store.note_pipeline_occurrence(
                    incident_id(signature, resolved=resolved), failure,
                )
                if historical is not None:
                    if self._sql is None or getattr(self.store, "is_durable", False):
                        processed.mark(event_key, received_at=failure.run.end_time.isoformat())
                    logger.info("Recorded historical pipeline observation without reopening %s", historical.id)
                    return None
            artifacts = await self._triage_pipeline(
                failure, client=client, reruns=reruns, scenario=scenario,
            )
            if self._sql is None or getattr(self.store, "is_durable", False):
                processed.mark(event_key, received_at=artifacts.request.received_at)
            else:
                logger.error("Pipeline outcome is not durable; leaving the source run unprocessed")
            return artifacts
        finally:
            claims.release(claim_key)

    async def _follow_pipeline_reruns(
        self, target: PipelineTarget, client: FabricPipelineClient,
        reruns: PipelineRerunStore,
    ) -> list[str]:
        claims = self._pipeline_claim_store()
        key = f"pipeline:{target.key}"
        if not claims.claim(key):
            return []
        lines: list[str] = []
        try:
            for record in reruns.pending(target.workspace_id, target.pipeline_id):
                if record.next_poll_at is not None and record.next_poll_at > datetime.now(UTC):
                    continue
                run = await client.get_run(target, record.rerun_id)
                if run.id != record.rerun_id or run.item_id != target.pipeline_id:
                    raise ValueError("Rerun evidence belongs to a different job or pipeline")
                if run.status not in PIPELINE_TERMINAL_STATUSES:
                    if not reruns.update(record.model_copy(update={
                        "next_poll_at": datetime.now(UTC) + timedelta(seconds=run.retry_after_seconds),
                    }), expected="submitted"):
                        raise RuntimeError("Could not retain the pipeline polling interval")
                    continue
                outcome = await verify_rerun(client, target, run)
                known = self.store.find_open(record.signature)
                if known is not None and (
                    known.pipeline_failure is not None
                    and known.pipeline_failure.run.id == record.failed_run_id
                ):
                    self.store.mark(
                        known.id, "resolved" if outcome.succeeded else "investigating",
                        f"Correlated pipeline rerun {run.id} verified {outcome.status}. {outcome.detail}",
                    )
                    if self._sql is not None and not getattr(self.store, "is_durable", False):
                        raise RuntimeError("Could not persist the verified pipeline rerun outcome")
                finished = record.model_copy(update={
                    "state": "completed" if outcome.succeeded else "failed",
                    "detail": outcome.detail,
                    "updated_at": datetime.now(UTC).isoformat(),
                })
                if not reruns.update(finished, expected="submitted"):
                    raise RuntimeError("Pipeline rerun changed while recording its final status")
                lines.append(f"- {target.name}: rerun {run.id} verified {outcome.status}")
        finally:
            claims.release(key)
        return lines

    def _record_pipeline_monitor_fault(self, target: PipelineTarget, exc: Exception) -> str:
        from triage.redaction import redact_text

        detail = redact_text(str(exc))[:1000]
        signature, _ = compute_signature(
            source="fabric_pipeline_monitor", error=f"{type(exc).__name__}: {detail}",
            artifact_kind="pipeline", artifact_name=target.key,
        )
        self.store.record(
            TriageResult(
                outcome="needs_human", signature=signature,
                request_id=f"pipeline-monitor:{target.key}",
                root_cause=f"Monitoring could not verify pipeline jobs: {detail}",
                summary="Monitor failure, not evidence of a failed pipeline run.",
            ),
            report_name=target.name, source="fabric_pipeline_monitor",
            agent_name="PipelineMonitor", original_error=detail,
        )
        logger.error("Pipeline monitor failed for %s: %s", target.key, detail)
        return f"- {target.name}: monitoring incomplete ({type(exc).__name__}: {detail})"

    async def pipeline_sweep(
        self, *, now: datetime | None = None,
        client: FabricPipelineClient | None = None,
        targets: list[PipelineTarget] | None = None,
    ) -> PipelineSweepReport:
        report = PipelineSweepReport()
        if not self.settings.pipeline_sweep_enabled:
            report.status = "disabled"
            return report
        targets = targets if targets is not None else load_pipeline_targets(self.settings.fabric_pipeline_targets)
        if not targets:
            report.status = "unconfigured"
            return report
        if self.settings.triage_tool_mode == "live" and self._sql is None:
            raise ValueError("Live pipeline sweeps require Fabric SQL state and claims")
        instant = now or datetime.now(UTC)
        if instant.tzinfo is None:
            raise ValueError("Pipeline sweep time must include a time zone")
        cutoff = instant - timedelta(hours=self.settings.pipeline_lookback_hours)
        api = client or self.build_pipeline_client()
        reruns = self.build_pipeline_rerun_store()
        try:
            for target in targets:
                try:
                    report.lines.extend(await self._follow_pipeline_reruns(target, api, reruns))
                    history = await api.list_runs(target)
                    report.checked += 1
                    completed = [run for run in history if run.end_time is not None]
                    if len(completed) >= 100 and min(run.end_time for run in completed) > cutoff:
                        report.faults.append(self._record_pipeline_monitor_fault(
                            target, PipelineApiError(
                                "The recent-job retention window may not cover the configured lookback. "
                                "Poll more frequently or use workspace monitoring for longer history."
                            ),
                        ))
                    candidates = sorted(
                        {
                            run.id: run for run in history
                            if run.failed_scheduled and run.end_time is not None
                            and cutoff <= run.end_time <= instant
                        }.values(),
                        key=lambda run: (run.end_time, run.id), reverse=True,
                    )
                    for run in history:
                        if run.status == "Failed" and (
                            run.invoke_type not in {"Scheduled", "Manual"} or run.end_time is None
                            or run.invoke_type == "Scheduled" and run.job_type not in PIPELINE_JOB_TYPES
                        ):
                            raise ValueError("A failed run has missing/unknown trigger or completion evidence")
                    for run in candidates:
                        if len(report.artifacts) >= self.settings.pipeline_max_runs_per_sweep:
                            report.lines.append("Pipeline triage limit reached; remaining runs stay eligible for the next sweep.")
                            break
                        failure = PipelineFailure(target=target, run=run, recent_runs=history)
                        artifacts = await self.run_pipeline_failure(
                            failure, client=api, reruns=reruns,
                        )
                        if artifacts is None:
                            report.skipped += 1
                            continue
                        report.artifacts.append(artifacts)
                        report.lines.append(f"- {target.name}: run {run.id}, {artifacts.result.outcome}")
                except Exception as exc:
                    report.faults.append(self._record_pipeline_monitor_fault(target, exc))
            if report.faults:
                report.status = "incomplete"
            return report
        finally:
            if client is None:
                await api.close()

    def build_retry_store(self, path: Path | None = None):
        """Where postponed retries live.

        Durable for the same reason as the others: the run that defers and the
        sweep that performs it are different processes. If this is in-memory on
        a hosted agent, every deferred retry is dropped the moment the run ends
        -- the agent would report scheduled work that can never happen.

        An explicit ``path`` forces the file-backed store, which is how tests
        stay isolated from each other and from a developer's real runs.
        """
        if path is not None:
            return JsonFileRetryStore(path)

        if self._sql is None:
            return JsonFileRetryStore(self.base_dir / "runs" / "retries.json")

        from triage.store.retries import FabricSqlRetryStore

        store = FabricSqlRetryStore(db=self._sql, table=self.settings.retry_table_name)
        if not store.is_durable:
            logger.error(
                "Retry store %s is not durable; deferred retries will be dropped "
                "rather than performed",
                self.settings.retry_table_name,
            )
        return store

    def build_approval_channel(self):
        """Where approval requests wait and decisions land.

        Same choice as the incident store: the database when one is configured,
        a JSON file otherwise. It has to be shared state either way -- the whole
        point is that a *different* process writes the answer.
        """
        if self._sql is None:
            return JsonFileApprovalChannel(self.base_dir / "runs" / "approvals.json")

        from triage.store.approvals import FabricSqlApprovalChannel

        channel = FabricSqlApprovalChannel(
            db=self._sql, table=self.settings.approval_table_name
        )
        if not channel.is_durable:
            logger.error(
                "Approval channel %s is not durable; no human can answer and every "
                "gated action will fail closed",
                self.settings.approval_table_name,
            )
        return channel

    def build_approval_gate(self, scenario: Scenario | None):
        """Choose the approval channel for this run.

        ``none`` is a real, testable configuration, not an oversight: an
        approval-required action with nowhere to send the request must be
        refused rather than quietly executed.

        A scenario always wins, because a rehearsal has to be reproducible and
        cannot wait on a person. Outside a scenario -- a live sweep, or the
        agent invoked from Foundry -- the real gate is used, which posts a card
        and waits for an actual decision.
        """
        from triage.approvals import (
            AutoApproveGate,
            AutoDenyGate,
            TeamsCardApprovalGate,
            TimeoutGate,
            WebApprovalGate,
        )

        if scenario is None:
            channel = self.build_approval_channel()
            if self.settings.approval_delivery_mode == "web":
                return WebApprovalGate(
                    channel,
                    notifier=self.build_teams() if self.settings.teams_webhook_url else None,
                    command_center_url=self.settings.command_center_url,
                )
            return TeamsCardApprovalGate(
                self.build_teams(),
                decision_source=channel,
                callback_url=self.settings.approval_callback_url,
            )

        mode = scenario.approval
        if mode == "none":
            return None
        if mode == "auto_deny":
            return AutoDenyGate(
                approver=scenario.approver,
                reason=(scenario.approval_reason or "Declined."),
            )
        if mode == "timeout":
            return TimeoutGate()
        return AutoApproveGate(approver=scenario.approver)

    def build_teams(self):
        """One notifier per runner, so what was posted stays inspectable.

        Returning a fresh instance each call meant the notifier that actually
        posted was unreachable afterwards, which made "show me the card that
        was sent" impossible to answer.
        """
        if self._teams is None:
            if self.settings.teams_mode != "webhook":
                # Rejected rather than quietly using the webhook. Graph app-only
                # channel posting is restricted to migration scenarios, so this
                # mode cannot work -- and an operator who thinks the agent posts
                # as itself has the wrong mental model of who is talking.
                raise ValueError(
                    f"TEAMS_MODE={self.settings.teams_mode!r} is not supported. Graph "
                    "app-only channel posting is restricted by Microsoft to migration "
                    "scenarios; use 'webhook' with a Power Automate Workflows URL."
                )
            if self.settings.triage_tool_mode == "live":
                if self.settings.teams_webhook_url:
                    self._teams = WorkflowsWebhookTeamsNotifier(self.settings.teams_webhook_url)
                else:
                    # Not a mock. A mock would report delivered=True and consume
                    # the incident's single announcement.
                    from triage.tools.teams import UnconfiguredTeamsNotifier

                    self._teams = UnconfiguredTeamsNotifier()
            else:
                self._teams = MockTeamsNotifier()
        return self._teams

    # --- scenarios ---------------------------------------------------------

    def prepare(self, scenario: Scenario, *, keep_incidents: bool = False) -> None:
        if scenario.reset_flags:
            self.flag_table.reset()
        if scenario.reset_incidents and not keep_incidents:
            self.store.reset()
            # Deferred retries are run state too. Leaving them behind means the
            # next scenario inherits an open backoff window and its refresh is
            # refused for a reason belonging to the previous run -- and repeated
            # rehearsals eventually exhaust the attempt limit.
            if self.retries is not None:
                self.retries.reset()

    async def run_scenario(
        self, scenario: Scenario, *, keep_incidents: bool = False
    ) -> list[RunArtifacts]:
        if scenario.pipeline is not None and self.settings.triage_tool_mode != "mock":
            raise ValueError(
                "Pipeline scenario fixtures require TRIAGE_TOOL_MODE=mock. "
                "Use the pipelines command for live job monitoring."
            )
        self.prepare(scenario, keep_incidents=keep_incidents)
        if scenario.pipeline is not None:
            target = PipelineTarget.model_validate(scenario.pipeline["target"])
            runs = [PipelineRun.model_validate(row) for row in scenario.pipeline["runs"]]
            client = MockFabricPipelineClient(
                runs, rerun_status=scenario.pipeline.get("rerun_status", "Completed"),
                activities=[
                    PipelineActivity.model_validate(row)
                    for row in scenario.pipeline.get("activities", [])
                ],
            )
            # Scenario inputs still traverse run_request and the same agent,
            # dispatcher, approval gate, stores and policy as a polled failure.
            # The per-scenario journal is isolated from operator runs.
            from triage.store.pipeline_reruns import InMemoryPipelineRerunStore

            journal = InMemoryPipelineRerunStore()
            pipeline_out: list[RunArtifacts] = []
            for run in runs:
                if run.failed_scheduled:
                    failure = PipelineFailure(
                        target=target, run=run, recent_runs=runs, activities=client.activities,
                    )
                    pipeline_out.append(await self._triage_pipeline(
                        failure, client=client, reruns=journal, scenario=scenario,
                    ))
            if not pipeline_out:
                raise ValueError("A pipeline triage scenario needs a failed scheduled job")
            return pipeline_out
        request = MockInbox.load(self.base_dir / scenario.email)

        datasets = {
            entry["name"]: DatasetSource(
                name=entry["name"],
                path=(self.base_dir / entry["path"]).resolve(),
                key_columns=list(entry.get("key_columns") or []),
            )
            for entry in scenario.datasets
        }

        out: list[RunArtifacts] = []
        for attempt in range(max(1, scenario.repeat)):
            # A repeat must look like a genuinely new email, or the dedup beat
            # is indistinguishable from simple idempotency.
            run_request = request.model_copy(
                update={"request_id": f"{request.request_id}-r{attempt + 1}"}
                if attempt
                else {}
            )
            out.append(await self.run_request(run_request, scenario=scenario, datasets=datasets))
        return out

    def build_command_center_store(self):
        if self._command_center_store is not None:
            return self._command_center_store
        from triage.store.command_center import (
            FabricSqlCommandCenterStore,
            JsonFileCommandCenterStore,
        )

        self._command_center_store = (
            FabricSqlCommandCenterStore(
                self._sql, run_table=self.settings.agent_run_table_name,
                event_table=self.settings.agent_event_table_name,
                command_table=self.settings.agent_command_table_name,
            )
            if self._sql is not None
            else JsonFileCommandCenterStore(self.base_dir / "runs" / "command_center.json")
        )
        return self._command_center_store

    async def run_request(
        self, request: BIRequest, *, scenario: Scenario | None = None,
        datasets: dict[str, DatasetSource] | None = None,
        pipeline: PipelineToolContext | None = None,
    ) -> RunArtifacts:
        if not self.settings.run_history_enabled:
            return await self._run_request_impl(
                request, scenario=scenario, datasets=datasets, pipeline=pipeline,
                event_hook=self.on_event,
            )
        from triage.store.command_center import RunEvent, RunRecord

        history = self.build_command_center_store()
        run_id = str(uuid4())
        signature = pipeline.signature if pipeline else compute_signature(
            source="powerbi_refresh_failure", error=request.error_text(),
            artifact_kind="dataset", artifact_name=request.report_name or request.dataset_id or "",
        )[0]
        history.start_run(RunRecord(
            id=run_id, request_id=request.request_id, signature=signature,
            target=request.report_name or request.dataset_id or "",
            workload="fabric_pipeline" if pipeline else "powerbi",
        ))
        sequence = 0
        allowed = {
            "triage_started", "tool_started", "tool_completed", "policy_violation",
            "agent_crashed", "outcome_downgraded", "notification_failed",
            "triage_finished", "notification",
        }

        def persist_event(kind: str, payload: dict[str, Any]) -> None:
            nonlocal sequence
            if kind not in allowed:
                return
            sequence += 1
            # Explicit metadata contract: never capture thinking/prompt content
            # or arbitrary tool arguments from the UI hook.
            history.append_event(RunEvent(
                run_id=run_id, sequence=sequence, kind=kind,
                label=str(payload.get("label") or payload.get("tool") or kind.replace("_", " ")),
                status=str(payload.get("status") or payload.get("outcome") or ""),
                detail=str(payload.get("detail") or payload.get("reason") or payload.get("message") or ""),
                tool_name=str(payload.get("tool") or ""),
            ))

        def emit(kind: str, payload: dict[str, Any]) -> None:
            persist_event(kind, payload)
            if self.on_event is not None:
                self.on_event(kind, payload)

        try:
            artifacts = await self._run_request_impl(
                request, scenario=scenario, datasets=datasets, pipeline=pipeline,
                event_hook=emit, run_id=run_id, notification_emit=persist_event,
            )
        except BaseException as exc:
            history.finish_run(run_id, TriageResult(
                outcome="timed_out" if isinstance(exc, (TimeoutError, asyncio.CancelledError)) else "agent_crashed",
                request_id=request.request_id, signature=signature,
                summary=f"Triage did not return a complete result ({type(exc).__name__}).",
                exception_class=type(exc).__name__,
            ))
            raise
        history.finish_run(
            run_id, artifacts.result,
            incident_id=artifacts.incident.id if artifacts.incident is not None else "",
        )
        artifacts.run_id = run_id
        return artifacts

    async def _run_request_impl(
        self,
        request: BIRequest,
        *,
        scenario: Scenario | None = None,
        datasets: dict[str, DatasetSource] | None = None,
        pipeline: PipelineToolContext | None = None,
        event_hook: EventHook | None = None,
        run_id: str = "",
        notification_emit: Any = None,
    ) -> RunArtifacts:
        if request.source == "pipeline" and pipeline is None:
            raise ValueError("Pipeline requests require controller-verified job evidence")
        if pipeline is not None:
            signature = pipeline.signature
        else:
            signature, _payload = compute_signature(
                source="powerbi_refresh_failure",
                error=request.error_text(),
                artifact_kind="dataset",
                artifact_name=request.report_name or request.dataset_id or "",
            )
        known = self.store.find_open(signature)

        powerbi = None if pipeline is not None else self.build_powerbi(scenario)
        if self.settings.notification_channel == "web":
            if notification_emit is None:
                raise ValueError("Web notifications require RUN_HISTORY_ENABLED")
            from triage.command_center.notifier import CommandCenterNotifier

            teams = CommandCenterNotifier(notification_emit)
        else:
            teams = self.build_teams()

        triage_provider = get_provider(
            "triage",
            self.settings,
            **(
                {
                    "rogue_second_refresh": scenario.rogue_second_refresh,
                    "rogue_unknown_action": scenario.rogue_unknown_action,
                    "check_schedule": scenario.check_schedule,
                }
                if scenario and self.settings.triage_provider_mode == "mock"
                else {}
            ),
        )
        # Against a real model the scripted flags have no effect - a
        # well-behaved agent simply never makes the bad request. Wrap it so the
        # refusal is demonstrated against whatever model is actually running.
        if scenario and self.settings.triage_provider_mode != "mock":
            if scenario.rogue_second_refresh or scenario.rogue_unknown_action:
                from triage.providers.chaos import ChaosProvider

                triage_provider = ChaosProvider(
                    inner=triage_provider,
                    rogue_second_refresh=scenario.rogue_second_refresh,
                    rogue_unknown_action=scenario.rogue_unknown_action,
                )
        dq_agent = (
            None if pipeline is not None
            else DataQualityAgent(get_provider("data_quality", self.settings))
        )

        agent = TriageAgent(
            triage_provider,
            policy=TriagePolicy.from_settings(self.settings),
            dq_agent=dq_agent,
            on_event=event_hook,
        )

        deps = TriageDeps(
            powerbi=powerbi,
            teams=teams,
            flag_table=self.flag_table,
            datasets=datasets or {},
            workspace_id=pipeline.failure.target.workspace_id if pipeline else self._resolve_id(
                scenario.workspace_id if scenario else "",
                self.settings.powerbi_workspace_id,
                request.workspace_id,
                untrusted=request.workspace_id,
                label="workspace",
            ),
            dataset_id="" if pipeline else self._resolve_id(
                scenario.dataset_id if scenario else "",
                self.settings.powerbi_dataset_id,
                request.dataset_id,
                untrusted=request.dataset_id,
                label="dataset",
            ),
            signature=signature,
            known_incident=known,
            approval_gate=self.build_approval_gate(scenario),
            approval_timeout_seconds=int(self.settings.approval_timeout_seconds),
            retries=self.retries,
            pipeline=pipeline,
            run_id=run_id,
        )

        flags_before = self.flag_table.row_count
        try:
            result = await agent.run(request, deps)
        finally:
            await agent.close()

        incident = self.store.record(
            result,
            report_name=request.report_name or "",
            original_error=request.error_text(),
            agent_name=agent.AGENT_NAME,
            prompt_version_hash=agent.prompt_hash,
            model_provider=agent.provider_name,
            model_name=agent.model_name,
            app_version=self.settings.app_version,
            # Only a card that actually went out counts. Passing "attempted"
            # here would let one failed delivery silence every future one.
            notified=result.notification_delivered,
            source="fabric_pipeline_failure" if pipeline else "powerbi_refresh_failure",
            pipeline_failure=pipeline.failure if pipeline else None,
        )

        return RunArtifacts(
            result=result,
            incident=incident,
            request=request,
            flag_rows_before=flags_before,
            flag_rows_after=self.flag_table.row_count,
            teams_messages=list(getattr(teams, "messages", [])),
            powerbi_calls=list(getattr(powerbi, "calls", [])),
            pipeline_calls=list(getattr(pipeline.client, "calls", [])) if pipeline else [],
        )


def check_expectations(scenario: Scenario, artifacts: RunArtifacts) -> list[str]:
    """Return a list of human-readable failures. Empty means the run matched."""
    expect = scenario.expect
    failures: list[str] = []
    result = artifacts.result

    if expect.outcome and result.outcome != expect.outcome:
        failures.append(f"outcome: expected '{expect.outcome}', got '{result.outcome}'")

    if expect.remediation_applied is not None:
        applied = any(a.is_remediation and not a.blocked for a in result.actions)
        if applied != expect.remediation_applied:
            failures.append(
                f"remediation_applied: expected {expect.remediation_applied}, got {applied}"
            )

    if expect.flags_written is not None:
        written = artifacts.flag_rows_after - artifacts.flag_rows_before
        if written != expect.flags_written:
            failures.append(f"flags_written: expected {expect.flags_written}, got {written}")

    if expect.dq_has_issue is not None:
        has_issue = bool(result.dq_finding and result.dq_finding.has_issue)
        if has_issue != expect.dq_has_issue:
            failures.append(f"dq_has_issue: expected {expect.dq_has_issue}, got {has_issue}")

    if expect.blocked_attempts is not None:
        blocked = len(result.blocked_attempts)
        if blocked != expect.blocked_attempts:
            failures.append(
                f"blocked_attempts: expected {expect.blocked_attempts}, got {blocked}"
            )

    if expect.approval_requested is not None:
        requested = bool(result.approvals)
        if requested != expect.approval_requested:
            failures.append(
                f"approval_requested: expected {expect.approval_requested}, got {requested}"
            )

    if expect.approval_granted is not None:
        granted = any(a.granted for a in result.approvals)
        if granted != expect.approval_granted:
            failures.append(
                f"approval_granted: expected {expect.approval_granted}, got {granted}"
            )

    if expect.denied_actions is not None:
        denied = len(result.denied_actions)
        if denied != expect.denied_actions:
            failures.append(f"denied_actions: expected {expect.denied_actions}, got {denied}")

    if expect.pipeline_reruns is not None:
        count = sum(call[0] == "rerun" for call in artifacts.pipeline_calls)
        if count != expect.pipeline_reruns:
            failures.append(f"pipeline_reruns: expected {expect.pipeline_reruns}, got {count}")
    if expect.incident_source and (
        artifacts.incident is None or artifacts.incident.source != expect.incident_source
    ):
        failures.append(f"incident_source: expected {expect.incident_source}")

    return failures


def discover_scenarios(directory: Path) -> list[Scenario]:
    return [Scenario.load(p) for p in sorted(Path(directory).glob("*.yaml"))]
