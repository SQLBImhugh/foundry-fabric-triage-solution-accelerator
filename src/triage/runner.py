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
import json
import logging
import os
import re
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage import EventHook, TriageAgent, TriageDeps
from triage.detectors.silent_failures import (
    HealthFinding,
    SilentFailureScanner,
    load_probes,
)
from triage.models import BIRequest, Incident, TriageResult
from triage.monitoring.contracts import (
    ControllerMonitoringStore,
    MonitoringConflict,
    MonitoringStoreError,
)
from triage.monitoring.controller import (
    HeartbeatBudget,
    MonitoringExecution,
    reconcile_monitoring_work,
)
from triage.monitoring.models import (
    SUPERSESSION_EVIDENCE_TTL_SECONDS,
    IncidentIdentity,
    LeaseRenewal,
    MonitoringContext,
    MonitoringWork,
    MonitoringWorkDraft,
    SourceExecutionIdentity,
    SourceRunObservation,
    TargetIdentity,
    WorkClaimRequest,
    WorkDispositionRequest,
    WorkFinalizationRequest,
)
from triage.monitoring.runtime import (
    FIXTURE_TENANT_ID,
    ScopedProcessedLog,
    build_monitoring_store,
    ensure_fixture_target,
    fixture_approvals,
    fixture_clock,
    fixture_id,
    fixture_setup,
    fixture_target,
    fixture_time,
    inspect_context,
    registered_targets,
    source_work_id,
    stable_id,
    target_signature,
)
from triage.pipeline_models import (
    PIPELINE_JOB_TYPES,
    PIPELINE_TERMINAL_STATUSES,
    PipelineActivity,
    PipelineFailure,
    PipelineRun,
    PipelineTarget,
    canonical_id,
)
from triage.policy import TriagePolicy
from triage.providers import get_provider
from triage.signature import SIGNATURE_VERSION, compute_signature
from triage.store.claims import ClaimStore, build_claim_store
from triage.store.incidents import IncidentStore, InMemoryIncidentStore, JsonFileIncidentStore
from triage.store.pipeline_reruns import (
    AzureSqlPipelineRerunStore,
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
from triage.tools.flags import AzureSqlFlagTable, DataQualityFlagTable, FlagStore
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

#: Lease for one deterministic reconciliation claim.
#:
#: Reconciliation is a single synchronous SQL transaction, so this only has to
#: cover that transaction, not a model call or an approval wait. It must stay
#: comfortably below SUPERSESSION_EVIDENCE_TTL_SECONDS: a failed attempt has to
#: become claimable again while the evidence that authorizes it is still valid,
#: or the retry is guaranteed to fail. Sharing the action lease broke exactly
#: that, and the work was then retried for 23 hours against evidence that had
#: expired eleven minutes into the first attempt.
RECONCILE_LEASE_SECONDS = 120
if RECONCILE_LEASE_SECONDS >= SUPERSESSION_EVIDENCE_TTL_SECONDS:  # pragma: no cover - contradiction guard
    raise AssertionError("A reconciliation lease must expire before the evidence it publishes")

#: The two automatic work pools the controller drains.
#:
#: They are claimed separately because they need different leases, and served
#: by separate heartbeat workers because they need independent progress.
#: Reconciliation publishes the evidence that clears fences for everything
#: else; actions include verifying and finalizing effects that have already
#: been submitted to a service, which cannot be left indefinitely unconfirmed.
MONITORING_POOLS: dict[str, tuple[str, ...]] = {
    "reconcile_state": ("reconcile_state",),
    "action": ("triage", "deferred_retry", "verify_action", "finalize"),
}


def _pipeline_signature(failure: PipelineFailure, identity: TargetIdentity) -> str:
    return target_signature(
        identity, failure.error_text() or "Unspecified pipeline failure",
        exception_class=failure.run.error_code,
    )


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
    monitoring_work_id: str = ""
    disposition: str = ""


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
        monitoring_store: ControllerMonitoringStore | None = None,
        credential: Any = None,
        fixture: bool | None = None,
    ):
        self.settings = settings
        self.fixture = settings.monitoring_mode == "fixture" if fixture is None else fixture
        if self.fixture and settings.triage_tool_mode == "live":
            raise ValueError("Live tools require MONITORING_MODE=live; fixture state cannot authorize live effects.")
        self._credential = credential
        self._monitoring_store = monitoring_store
        self._owns_monitoring = monitoring_store is None
        self.base_dir = Path(base_dir)
        self.on_event = on_event
        # One database handle shared by every store. A hosted agent is
        # constructed fresh for each request, so six separate connections would
        # mean six Entra logins per alert rather than one.
        self._sql = self._build_sql()
        if not self.fixture:
            inspect_context(self.monitoring, self.settings.monitoring_tenant_id)
        if self.fixture and store is not None:
            from triage.store.sql_incidents import AzureSqlIncidentStore

            if isinstance(store, AzureSqlIncidentStore):
                raise ValueError("Fixture runs cannot use a live SQL incident store.")
        self.store: IncidentStore = store or self._build_store()
        if flag_table_path is not None and not self.fixture:
            raise ValueError("A local data quality flag path is permitted only in fixture mode.")
        self.flag_table: FlagStore = (
            DataQualityFlagTable(flag_table_path or (self.base_dir / "runs" / "dq_flags.csv"))
            if self.fixture
            else AzureSqlFlagTable(self._sql, self.settings.data_quality_flag_table_name)
        )
        # Built once per runner rather than per run: a deferral written by one
        # run has to be visible to the precondition check of the next.
        self.retries = self.build_retry_store(retry_store_path)
        self.semantic_health = self.build_semantic_health_store(semantic_health_path)
        self._teams = None
        self._pipeline_claims: ClaimStore | None = None
        self._command_center_store = command_center_store

    # --- inbox -------------------------------------------------------------

    @property
    def monitoring(self) -> ControllerMonitoringStore:
        if self._monitoring_store is None:
            self._monitoring_store = build_monitoring_store(
                self.settings, db=self._sql, fixture=self.fixture, component="controller",
            )
        return self._monitoring_store

    @property
    def monitoring_context(self) -> MonitoringContext:
        return inspect_context(
            self.monitoring,
            FIXTURE_TENANT_ID if self.fixture else self.settings.monitoring_tenant_id,
        )

    def pipeline_targets(self, *, include_inactive: bool = False) -> list[PipelineTarget]:
        targets = registered_targets(
            self.monitoring, self.monitoring_context, workload="fabric_pipeline",
            include_inactive=include_inactive,
        )
        return [self._pipeline_target(item.identity) for item in targets]

    def _pipeline_target(self, identity: TargetIdentity) -> PipelineTarget:
        target = self.monitoring.resolve_target(identity, include_inactive=True)
        if target is None:
            raise MonitoringConflict("The registered pipeline target is unavailable.")
        review = (
            self.monitoring.get_safety_review(self.monitoring_context, target.action.review_id)
            if target.action.review_id is not None else None
        )
        return PipelineTarget(
            name=target.name, workspace_id=identity.workspace_id, pipeline_id=identity.item_id,
            rerun_safe=bool(review is not None and review.state == "verified" and review.replay_safe),
            rerun_parameters=review.parameters if review is not None else None,
        )

    def _request_target(
        self, request: BIRequest, scenario: Scenario | None,
        pipeline: PipelineToolContext | None, monitoring: MonitoringExecution | None,
    ) -> TargetIdentity | None:
        if monitoring is not None:
            return monitoring.incident.target
        if self.fixture:
            return fixture_target(
                "fabric_pipeline" if pipeline is not None else "powerbi",
                pipeline.failure.target.workspace_id if pipeline else scenario.workspace_id if scenario else request.workspace_id or "",
                pipeline.failure.target.pipeline_id if pipeline else scenario.dataset_id if scenario else request.dataset_id or "",
            )
        if not request.workspace_id or not request.dataset_id or pipeline is not None:
            return None
        try:
            identity = TargetIdentity(
                **self.monitoring_context.model_dump(), workload="powerbi",
                workspace_id=request.workspace_id, item_id=request.dataset_id,
            )
        except ValueError:
            logger.warning("The human report has no valid native target identity; diagnostics only")
            return None
        return identity if self.monitoring.resolve_target(identity) is not None else None

    @property
    def sql(self):
        """The shared Azure SQL handle, or None when running offline.

        Exposed so the hosted entry point can build its claim store on the same
        connection instead of opening a second one.
        """
        return self._sql

    def _build_sql(self):
        """Select explicit fixture state or a DML-only, credential-injected SQL handle."""
        if self.fixture:
            return None
        canonical_id(self.settings.monitoring_tenant_id)
        server = self.settings.azure_sql_server
        database = self.settings.azure_sql_database
        if not server or not database:
            raise ValueError("MONITORING_MODE=live requires AZURE_SQL_SERVER and AZURE_SQL_DATABASE.")

        from triage.store.azure_sql import AzureSqlDatabase

        return AzureSqlDatabase(
            server=server, database=database, tables=self._sql_tables(),
            credential=self._credential,
        )

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
            "data_quality_flags": s.data_quality_flag_table_name,
        }

    def _build_store(self) -> IncidentStore:
        """Choose where incidents live.

        Falls back to the JSON file when no database is configured, so the
        offline rehearsal path is unchanged and needs no SQL driver.
        """
        if self._sql is None:
            return JsonFileIncidentStore(self.base_dir / "runs" / "incidents.json")

        from triage.store.sql_incidents import AzureSqlIncidentStore

        return AzureSqlIncidentStore(
            db=self._sql, table=self.settings.incident_table_name
        )

    def build_processed_log(self):
        """Where the record of already-triaged mail lives.

        Mirrors ``_build_store``: the database when one is configured, a JSON
        file otherwise. This has to outlive the process -- a hosted agent is
        rebuilt for every invocation, so anything held in memory here is always
        empty on arrival.
        """
        if self._sql is None:
            return JsonFileProcessedLog(self.base_dir / "runs" / "processed.json")

        from triage.store.processed import AzureSqlProcessedLog

        log = AzureSqlProcessedLog(
            db=self._sql, table=self.settings.processed_table_name
        )
        return ScopedProcessedLog(log, self.monitoring_context)

    def build_inbox_audit(self, path: Path | None = None):
        """Where refused messages are recorded.

        Fixture files are explicit. Missing live evidence blocks ingestion.
        """
        from triage.store.inbox_audit import JsonFileInboxAudit

        if path is not None:
            if not self.fixture:
                raise ValueError("A local audit file cannot replace live shared state.")
            return JsonFileInboxAudit(path)

        if self._sql is None:
            return JsonFileInboxAudit(self.base_dir / "runs" / "inbox_audit.json")

        from triage.store.inbox_audit import AzureSqlInboxAudit

        return AzureSqlInboxAudit(
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
                monitoring_tenant_id=self.settings.monitoring_tenant_id,
            )
            return LivePowerBIClient(
                tenant_id=self.settings.monitoring_tenant_id,
                client_id=self.settings.azure_client_id or self.settings.powerbi_client_id,
                client_secret="",
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
        if self.fixture:
            with fixture_time(now):
                return await self._drain_due_retries(now=now, claims=claims)
        return await self._drain_due_retries(now=now, claims=claims)

    async def _drain_due_retries(
        self, *, now: datetime | None = None, claims: Any = None,
    ) -> list[str]:
        """Admit due rows, then execute through the same fenced dispatcher."""
        if self.retries is None:
            return []
        if now is not None and not self.fixture:
            raise ValueError("Live retry timing is owned by shared database time, not an operator-supplied clock.")
        due = self.retries.due(now=now)
        if not due:
            return []
        lines: list[str] = []
        held: list[str] = []
        admitted: set[str] = set()
        for row in due:
            key = f"retry:{row['signature']}"
            if claims is not None and not claims.claim(key):
                continue
            if claims is not None:
                held.append(key)
            admitted.add(row["signature"])
        try:
            context = self.monitoring_context
            for row in due:
                if row["signature"] not in admitted:
                    continue
                if self.fixture and "source_execution" not in row:
                    identity = fixture_target("powerbi", row.get("workspace_id", ""), row.get("dataset_id", ""))
                    with fixture_setup(self.monitoring) as setup:
                        ensure_fixture_target(
                            setup, identity, row.get("report_name") or "Synthetic retry",
                            action="powerbi_refresh",
                        )
                    source = SourceExecutionIdentity(
                        target=identity, run_id_kind="powerbi_request",
                        run_id=fixture_id(row.get("request_id") or row["signature"], kind=f"{identity.key}:retry-source"),
                    )
                    revision = self.monitoring.snapshot(context).control.revision
                else:
                    source = SourceExecutionIdentity.model_validate(row["source_execution"])
                    revision = row["policy_revision"]
                if source.target.epoch != context.epoch or source.target.tenant_id != context.tenant_id:
                    self.retries.complete(row["signature"], outcome="obsolete_monitoring_context")
                    lines.append(f"- {row.get('report_name') or 'dataset'}: obsolete retry closed without execution.")
                    continue
                target = self.monitoring.resolve_target(source.target)
                control = self.monitoring.snapshot(context).control
                if (
                    source.target.epoch != context.epoch or source.target.tenant_id != context.tenant_id
                    or revision != control.revision or control.maintenance
                    or target is None or not target.action.enabled
                ):
                    self.retries.complete(row["signature"], outcome="monitoring_admission_changed")
                    lines.append(f"- {row.get('report_name') or 'dataset'}: no new retry admitted; existing action fences are unchanged.")
                    continue
                identifier = row.get("monitoring_work_id") or stable_id(f"{source.key}:deferred-retry")
                queued = self.monitoring.get_work(context, identifier)
                if queued is None:
                    queued = self.monitoring.enqueue_work(MonitoringWorkDraft(
                        **context.model_dump(), work_id=identifier,
                        kind="deferred_retry", policy_revision=revision,
                        due_at=fixture_clock(self.monitoring)() if self.fixture and now is not None else datetime.fromisoformat(row["due_at"]),
                        created_at=datetime.fromisoformat(row["created_at"]), target=source.target, execution=source,
                        reason=f"Deferred incident {row['signature']}.",
                    ))
                elif queued.execution != source:
                    raise MonitoringConflict("The deferred work identity belongs to a different source.")
                if queued.state in {"completed", "dispositioned"}:
                    action = (
                        self.monitoring.get_action_reservation(context, queued.action_reservation_id)
                        if queued.action_reservation_id else None
                    )
                    if action is not None and action.state == "rejected":
                        self._project_rejected_retry(action)
                        lines.append(f"- {row.get('report_name') or 'dataset'}: confirmed rejection and linked retry reconciled.")
                        continue
                    self.retries.complete(row["signature"], outcome=queued.disposition or queued.state)
                    lines.append(f"- {row.get('report_name') or 'dataset'}: prior durable retry disposition reconciled.")
                    continue
                row["source_execution"] = source.model_dump(mode="json")
                row["policy_revision"] = revision
                row["monitoring_work_id"] = queued.work_id
            by_work = {row.get("monitoring_work_id"): row for row in due if row.get("monitoring_work_id")}
            for _ in range(len(by_work)):
                selected = self.monitoring.claim_work(WorkClaimRequest(
                    **context.model_dump(), owner_id=str(uuid4()), kinds=("deferred_retry",),
                    limit=1, per_workspace_limit=1, lease_seconds=900,
                ))
                if not selected:
                    break
                work = selected[0]
                row = by_work.get(work.work_id)
                if row is None:
                    raise MonitoringConflict("A due retry has no matching durable retry row.")
                if work.kind == "verify_action":
                    lines.append(await self.execute_monitoring_work(work))
                else:
                    lines.extend(await self._execute_deferred_work(work, row, self.build_powerbi(), fixture_due=now is not None))
        finally:
            if claims is not None:
                for key in held:
                    claims.release(key)
        return lines

    async def _execute_deferred_work(
        self, work: MonitoringWork, row: dict[str, Any], powerbi: Any, *, fixture_due: bool = False,
    ) -> list[str]:
        from triage.policy import PolicyLedger
        from triage.tools.registry import ToolContext, ToolDispatcher

        if work.execution is None or work.lease is None:
            raise MonitoringConflict("A retry needs current exact source work and its lease.")
        if self.fixture:
            instant = fixture_clock(self.monitoring)()
            prior_source = self.monitoring.get_source(work.execution)
            observation = prior_source.model_copy(update={"observed_at": instant}) if prior_source else SourceRunObservation(
                execution=work.execution, origin="fixture", authority="fixture",
                observed_at=instant, started_at=instant - timedelta(minutes=1),
                ended_at=instant - timedelta(seconds=30), status="failed", invocation="scheduled",
                failure_reason=row.get("reason") or "Synthetic throttled refresh", failure_signature=row["signature"],
            )
        else:
            observation = await self.observe_powerbi_execution(work.execution, powerbi, work=work)
            if target_signature(work.target, observation.failure_reason or "Unspecified failure") != row["signature"]:
                raise MonitoringConflict("The deferred source failure changed; no retry was submitted.")
        self.monitoring.observe_source(observation, work_id=work.work_id, lease=work.lease)
        execution = MonitoringExecution(
            store=self.monitoring, work=work,
            incident=IncidentIdentity(target=work.target, signature=row["signature"]),
            observation=observation, fixture=self.fixture, powerbi_client=powerbi,
            clock=fixture_clock(self.monitoring) if self.fixture else lambda: datetime.now(UTC),
        )

        async def refresh_source():
            return observation.model_copy(update={"observed_at": fixture_clock(self.monitoring)()}) if self.fixture else await self.observe_powerbi_execution(work.execution, powerbi)

        execution.refresh_source = refresh_source
        if not self.fixture:
            self._bind_powerbi_history(execution, powerbi)
        ledger = PolicyLedger(TriagePolicy.from_settings(self.settings))
        request = BIRequest(
            request_id=work.execution.key, source="interactive", sender="deferred-retry-controller",
            subject="Due deferred refresh", body=observation.failure_reason or "",
            workspace_id=work.target.workspace_id, dataset_id=work.target.item_id,
            report_name=row.get("report_name", ""),
        )
        ctx = ToolContext(
            request=request, ledger=ledger, powerbi=powerbi, teams=self.build_teams(),
            flag_table=self.flag_table, signature=row["signature"],
            workspace_id=work.target.workspace_id, dataset_id=work.target.item_id,
            monitoring=execution, retries=None if self.fixture and fixture_due else self.retries,
        )
        dispatcher = ToolDispatcher(ctx)
        response = await dispatcher.dispatch("refresh_powerbi_dataset", {"justification": "The persisted throttling window has elapsed."})
        if execution.persistence_error is not None:
            raise execution.persistence_error
        outcome = ctx.remediation_outcome
        report = row.get("report_name") or "the dataset"
        rejected = execution.reservation is not None and execution.reservation.state == "rejected"
        successor = execution.reservation.retry_work_id if rejected else None
        result = TriageResult(
            request_id=request.request_id, signature=row["signature"], signature_version=SIGNATURE_VERSION,
            outcome="deferred_retry" if successor else "resolved" if outcome is not None and outcome.succeeded else "needs_human",
            summary=(outcome.detail if outcome is not None else response.get("reason", "Retry was refused.")),
            actions=dispatcher.actions, write_actions=ledger.write_actions,
            tool_calls=ledger.tool_calls, blocked_attempts=ledger.blocked_attempts,
        )
        self._finalize_monitoring_result(execution, result, {
            "report_name": report, "source": "powerbi_refresh_failure", "agent_name": "DeferredRetryController",
        })
        if not rejected:
            self.retries.complete(row["signature"], outcome=result.outcome)
        if rejected:
            if successor:
                pending = self.monitoring.get_work(execution.context, successor)
                if pending is None:
                    raise MonitoringStoreError("Confirmed rejection has no recorded successor work.")
                return [f"- {report}: still throttled; linked retry {pending.retry_attempt} due {pending.due_at.isoformat()}. No second POST was issued in this invocation."]
            return [f"- {report}: request rejected without effect; no automatic retry remains or is currently admitted."]
        if self.fixture and result.outcome == "resolved":
            known = self.store.find_open(row["signature"])
            if known is not None:
                self.store.mark(known.id, "resolved", "Deferred refresh was verified after its backoff window.")
        return [f"- {report}: deferred retry completed" if result.outcome == "resolved" else f"- {report}: retry not resolved; {result.summary}"]

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
        if not self.fixture:
            context = self.monitoring_context
            for probe in probes:
                identity = TargetIdentity(
                    **context.model_dump(), workload="powerbi",
                    workspace_id=probe.workspace_id, item_id=probe.dataset_id,
                )
                if self.monitoring.resolve_target(identity) is None:
                    raise MonitoringConflict("A configured silent-health probe is outside current registry admission.")

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
        identity = (
            fixture_target("powerbi", finding.workspace_id, finding.dataset_id)
            if self.fixture else TargetIdentity(
                **self.monitoring_context.model_dump(), workload="powerbi",
                workspace_id=finding.workspace_id, item_id=finding.dataset_id,
            )
        )
        signature, _ = compute_signature(
            source="silent_failure",
            error=finding.detail,
            artifact_kind="semantic_model",
            target_key=identity.key,
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
            if not self.fixture:
                raise ValueError("A local baseline file cannot replace live shared state.")
            return JsonFileSemanticHealthStore(path)

        if self._sql is None:
            return JsonFileSemanticHealthStore(self.base_dir / "runs" / "semantic_health.json")

        from triage.store.semantic_health import AzureSqlSemanticHealthStore

        return AzureSqlSemanticHealthStore(
            db=self._sql,
            table=self.settings.semantic_health_table_name,
            lease_table=getattr(self.settings, "lease_table_name", "triage_sweep_leases"),
        )

    def build_health_client(self):
        if self.settings.triage_tool_mode == "live":
            _require_live_config(
                "semantic health client",
                monitoring_tenant_id=self.settings.monitoring_tenant_id,
            )
            return LiveSemanticHealthClient(
                tenant_id=self.settings.monitoring_tenant_id,
                client_id=self.settings.azure_client_id or self.settings.powerbi_client_id,
                client_secret="",
            )
        return MockSemanticHealthClient()

    def build_pipeline_client(self) -> FabricPipelineClient:
        if self.settings.triage_tool_mode != "live":
            return MockFabricPipelineClient()
        _require_live_config("Fabric pipeline client", monitoring_tenant_id=self.settings.monitoring_tenant_id)
        from triage.tools.fabric_pipeline import LiveFabricPipelineClient

        return LiveFabricPipelineClient(
            tenant_id=self.settings.monitoring_tenant_id,
            client_id=self.settings.azure_client_id or self.settings.fabric_client_id,
            max_pages=self.settings.pipeline_max_pages,
        )

    def build_pipeline_rerun_store(self) -> PipelineRerunStore:
        if self._sql is not None:
            return AzureSqlPipelineRerunStore(
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
        monitoring: MonitoringExecution | None = None,
    ) -> RunArtifacts:
        signature = monitoring.incident.signature if monitoring is not None else _pipeline_signature(
            failure, fixture_target("fabric_pipeline", failure.target.workspace_id, failure.target.pipeline_id),
        )
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
                return await self.run_request(request, scenario=scenario, pipeline=context, monitoring=monitoring)
        except MonitoringStoreError:
            raise
        except Exception as exc:
            if monitoring is not None and monitoring.persistence_error is not None:
                raise
            logger.exception("Pipeline triage could not finish")
            result = TriageResult(
                outcome="timed_out" if isinstance(exc, TimeoutError) else "agent_crashed",
                request_id=request.request_id, signature=signature, signature_version=SIGNATURE_VERSION,
                summary=f"Pipeline triage failed ({type(exc).__name__}); inspect the rerun journal before replay.",
                exception_class=type(exc).__name__,
            )
            provenance = dict(
                report_name=failure.target.name,
                original_error=request.error_text(), source="fabric_pipeline_failure",
                agent_name="TriageAgent", pipeline_failure=failure,
            )
            incident = (
                self._finalize_monitoring_result(monitoring, result, provenance, disposition="failed")
                if monitoring is not None else self.store.record(result, **provenance)
            )
            return RunArtifacts(
                result=result, incident=incident, request=request,
                flag_rows_before=self.flag_table.row_count,
                flag_rows_after=self.flag_table.row_count,
                pipeline_calls=list(getattr(client, "calls", [])),
                monitoring_work_id=monitoring.work.work_id if monitoring is not None else "",
            )

    async def run_pipeline_failure(
        self, failure: PipelineFailure, *, client: FabricPipelineClient,
        reruns: PipelineRerunStore, scenario: Scenario | None = None,
        monitoring: MonitoringExecution | None = None,
    ) -> RunArtifacts | None:
        """Serialize this pipeline and re-check processed state inside the claim."""
        if monitoring is not None:
            monitoring.current_work()
            return await self._triage_pipeline(
                failure, client=client, reruns=reruns, scenario=scenario, monitoring=monitoring,
            )
        if not self.fixture:
            identity = TargetIdentity(
                **self.monitoring_context.model_dump(), workload="fabric_pipeline",
                workspace_id=failure.target.workspace_id, item_id=failure.target.pipeline_id,
            )
            work = self.enqueue_execution_reference(SourceExecutionIdentity(
                target=identity, run_id_kind="fabric_job", run_id=failure.run.id,
            ))
            request = BIRequest(
                request_id=work.execution.key, source="pipeline", sender="pipeline-reference",
                subject=failure.target.name, body="Exact pipeline reference queued for controller verification.",
                workspace_id=identity.workspace_id, report_name=failure.target.name,
            )
            return RunArtifacts(
                result=TriageResult(
                    request_id=request.request_id, outcome="needs_human",
                    summary=f"Pipeline work {work.work_id} queued; no rerun was submitted.",
                ),
                incident=None, request=request,
                flag_rows_before=self.flag_table.row_count, flag_rows_after=self.flag_table.row_count,
                monitoring_work_id=work.work_id,
            )
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
            artifacts = await self._triage_pipeline(
                failure, client=client, reruns=reruns, scenario=scenario,
            )
            if self._sql is None or getattr(self.store, "is_durable", False):
                processed.mark(event_key, received_at=artifacts.request.received_at)
            else:
                logger.error("Pipeline outcome is not durable; leaving the source run unprocessed")
            return None if artifacts.disposition == "historical" else artifacts
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
        selection: TargetIdentity | None = None,
    ) -> PipelineSweepReport:
        report = PipelineSweepReport()
        if selection is not None and selection.workload != "fabric_pipeline":
            raise ValueError("Pipeline selection must identify a registered Fabric pipeline.")
        if selection is not None and targets is not None:
            raise ValueError("Choose a registry selection or explicit fixture targets, never both.")
        if self.fixture and selection is None and not self.settings.pipeline_sweep_enabled:
            report.status = "disabled"
            return report
        if not self.fixture:
            if targets is not None:
                raise ValueError("Live pipeline targets come only from the monitoring registry.")
            context = self.monitoring_context
            snapshot = self.monitoring.snapshot(context)
            if snapshot.control.maintenance:
                raise MonitoringConflict("Monitoring is in deployment maintenance.")
            if selection is not None:
                selected = self.monitoring.resolve_target(selection)
                if selected is None:
                    raise MonitoringConflict("The selected pipeline is not currently admitted.")
                admitted = [selected]
            else:
                admitted = registered_targets(self.monitoring, context, workload="fabric_pipeline")
            instant = now or datetime.now(UTC)
            for target in admitted:
                if not target.observation.enabled:
                    continue
                work = self.monitoring.enqueue_work(MonitoringWorkDraft(
                    **context.model_dump(),
                    work_id=stable_id(f"{target.key}:operator-poll:{instant.isoformat()}"),
                    kind="poll", policy_revision=snapshot.control.revision,
                    due_at=instant, created_at=instant, target=target.identity,
                    reason="Operator requested a registry-scoped pipeline observation.",
                ))
                report.lines.append(f"- {target.name}: polling work queued ({work.work_id}).")
            report.status = "queued" if report.lines else "unconfigured"
            return report
        targets = [self._pipeline_target(selection)] if selection is not None else targets
        targets = targets if targets is not None else self.pipeline_targets()
        if not targets:
            report.status = "unconfigured"
            return report
        if self.settings.triage_tool_mode == "live" and self._sql is None:
            raise ValueError("Live pipeline sweeps require Azure SQL state and claims")
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
            if not self.fixture:
                raise ValueError("A local retry file cannot replace live shared state.")
            return JsonFileRetryStore(path)

        if self._sql is None:
            return JsonFileRetryStore(self.base_dir / "runs" / "retries.json")

        from triage.store.retries import AzureSqlRetryStore

        return AzureSqlRetryStore(db=self._sql, table=self.settings.retry_table_name)

    def build_approval_channel(self):
        """Where approval requests wait and decisions land.

        Same choice as the incident store: the database when one is configured,
        a JSON file otherwise. It has to be shared state either way -- the whole
        point is that a *different* process writes the answer.
        """
        if self.fixture:
            return fixture_approvals(self.monitoring)

        from triage.store.approvals import AzureSqlApprovalChannel

        return AzureSqlApprovalChannel(
            db=self._sql, table=self.settings.approval_table_name
        )

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
            if self.fixture and self._owns_monitoring:
                self._monitoring_store = None
            # Deferred retries are run state too. Leaving them behind means the
            # next scenario inherits an open backoff window and its refresh is
            # refused for a reason belonging to the previous run -- and repeated
            # rehearsals eventually exhaust the attempt limit.
            if self.retries is not None:
                self.retries.reset()

    async def run_scenario(
        self, scenario: Scenario, *, keep_incidents: bool = False
    ) -> list[RunArtifacts]:
        if not self.fixture or self.settings.triage_tool_mode != "mock":
            raise ValueError(
                "Scenario fixtures require explicit fixture state and TRIAGE_TOOL_MODE=mock. "
                "Live source work uses the monitoring controller."
            )
        self.prepare(scenario, keep_incidents=keep_incidents)
        if scenario.pipeline is not None:
            target = PipelineTarget.model_validate(scenario.pipeline["target"])
            runs = [
                PipelineRun.model_validate({
                    **row, "id": stable_id(f"fixture:{scenario.name}:pipeline-run:{row['id']}"),
                })
                for row in scenario.pipeline["runs"]
            ]
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
            AzureSqlCommandCenterStore,
            JsonFileCommandCenterStore,
        )

        self._command_center_store = (
            AzureSqlCommandCenterStore(
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
        monitoring: MonitoringExecution | None = None,
        source_execution: SourceExecutionIdentity | None = None,
    ) -> RunArtifacts:
        target = self._request_target(request, scenario, pipeline, monitoring)
        if monitoring is None and not self.fixture:
            if source_execution is None and target is not None:
                match = re.search(
                    r"(?i)\bPower\s*BI\s+refresh\s+request\s+ID\s*[:=]\s*([0-9a-f-]{36})\b",
                    request.error_text(),
                )
                if match is not None:
                    source_execution = SourceExecutionIdentity(
                        target=target, run_id_kind="powerbi_request", run_id=match[1],
                    )
            if source_execution is not None:
                if source_execution.target.workload == "powerbi" and source_execution.run_id_kind == "powerbi_refresh":
                    expected = self.monitoring_context
                    if (source_execution.target.tenant_id, source_execution.target.epoch) != (expected.tenant_id, expected.epoch):
                        raise MonitoringConflict("Source alias belongs to another deployment.")
                    if self.monitoring.resolve_target(source_execution.target) is None:
                        raise MonitoringConflict("Source alias is outside current admission.")
                    observed = await self.observe_powerbi_execution(source_execution, self.build_powerbi())
                    source_execution = observed.execution
                work = self.enqueue_execution_reference(source_execution)
                return RunArtifacts(
                    result=TriageResult(
                        request_id=request.request_id, outcome="needs_human",
                        summary=f"Exact execution reference queued as monitoring work {work.work_id}; no remediation was performed by this request.",
                    ),
                    incident=None, request=request,
                    flag_rows_before=self.flag_table.row_count, flag_rows_after=self.flag_table.row_count,
                    monitoring_work_id=work.work_id,
                )
        if monitoring is not None:
            signature = monitoring.incident.signature
        elif self.fixture and target is not None:
            signature = target_signature(
                target, pipeline.failure.error_text() if pipeline else request.error_text(),
                exception_class=pipeline.failure.run.error_code if pipeline else None,
            )
        else:
            # An annotation is not a native execution and must not suppress its
            # incident or spend its budget when REST evidence arrives later.
            signature = compute_signature(
                source="human_report", error=request.error_text(),
                artifact_kind="diagnostic", artifact_name=request.request_id,
                target_key=target.key + ":diagnostic:" + request.request_id if target else None,
            )[0]
        if pipeline is not None:
            pipeline.signature = signature
        if self.fixture and monitoring is None and target is not None:
            monitoring = self._fixture_execution(request, target, signature, scenario, pipeline)
            target = monitoring.incident.target
        if not self.settings.run_history_enabled:
            return await self._run_request_impl(
                request, scenario=scenario, datasets=datasets, pipeline=pipeline,
                event_hook=self.on_event, signature=signature, target=target, monitoring=monitoring,
            )
        from triage.store.command_center import RunEvent, RunRecord

        history = self.build_command_center_store()
        run_id = str(uuid4())
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
                signature=signature, target=target, monitoring=monitoring,
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
        signature: str,
        target: TargetIdentity | None,
        monitoring: MonitoringExecution | None = None,
    ) -> RunArtifacts:
        if request.source == "pipeline" and pipeline is None:
            raise ValueError("Pipeline requests require controller-verified job evidence")
        if monitoring is not None and monitoring.observation is not None:
            state = self.monitoring.get_incident_state(monitoring.incident)
            prior = self.monitoring.get_incident(monitoring.incident)
            started = monitoring.observation.started_at
            if state and prior and state.latest_started_at and started and started < state.latest_started_at:
                result = TriageResult(
                    request_id=request.request_id, signature=signature, signature_version=SIGNATURE_VERSION,
                    outcome=prior.outcome, summary="Historical source counted without reopening newer incident evidence.",
                )
                incident = self._finalize_monitoring_result(
                    monitoring, result,
                    {"source": prior.source, "pipeline_failure": prior.pipeline_failure},
                    disposition="historical",
                )
                return RunArtifacts(
                    result=result, incident=incident, request=request,
                    flag_rows_before=self.flag_table.row_count, flag_rows_after=self.flag_table.row_count,
                    monitoring_work_id=monitoring.work.work_id, disposition="historical",
                )
        known = (
            self.monitoring.get_incident(monitoring.incident)
            if monitoring is not None else self.store.find_open(signature)
        )
        if known is not None and known.status not in {"open", "investigating"}:
            known = None

        powerbi = (
            None if pipeline is not None or target is None
            else monitoring.powerbi_client if monitoring is not None and monitoring.powerbi_client is not None
            else self.build_powerbi(scenario)
        )
        if monitoring is not None and powerbi is not None and not self.fixture:
            self._bind_powerbi_history(monitoring, powerbi)
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
            workspace_id=target.workspace_id if target is not None else "",
            dataset_id=target.item_id if target is not None and pipeline is None else "",
            signature=signature,
            known_incident=known,
            approval_gate=self.build_approval_gate(scenario),
            approval_timeout_seconds=int(self.settings.approval_timeout_seconds),
            retries=self.retries,
            pipeline=pipeline,
            run_id=run_id,
            monitoring=monitoring,
        )

        flags_before = self.flag_table.row_count
        try:
            result = await agent.run(request, deps)
        finally:
            await agent.close()

        result = result.model_copy(update={"signature_version": SIGNATURE_VERSION})
        provenance = dict(
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
            source=(
                "human_report" if not self.fixture and monitoring is None
                else "fabric_pipeline_failure" if pipeline else "powerbi_refresh_failure"
            ),
            pipeline_failure=pipeline.failure if pipeline else None,
        )
        if monitoring is None:
            incident = self.store.record(result, **provenance)
        else:
            incident = self._finalize_monitoring_result(monitoring, result, provenance)

        return RunArtifacts(
            result=result,
            incident=incident,
            request=request,
            flag_rows_before=flags_before,
            flag_rows_after=self.flag_table.row_count,
            teams_messages=list(getattr(teams, "messages", [])),
            powerbi_calls=list(getattr(powerbi, "calls", [])),
            pipeline_calls=list(getattr(pipeline.client, "calls", [])) if pipeline else [],
            monitoring_work_id=monitoring.work.work_id if monitoring else "",
        )

    def _finalize_monitoring_result(
        self, execution: MonitoringExecution, result: TriageResult, provenance: dict[str, Any],
        *, disposition: str | None = None,
    ) -> Incident:
        # An explicit candidate is not a live fallback. No terminal result is
        # returned until the registry commits incident, source and work together.
        candidate = InMemoryIncidentStore()
        prior = self.monitoring.get_incident(execution.incident)
        if prior is not None:
            candidate._items[prior.id] = prior
        if execution.work.kind == "verify_action" and prior is not None:
            incident = candidate.mark(
                prior.id, "resolved" if result.outcome == "resolved" else "investigating", result.summary,
            )
            if incident is None:
                raise MonitoringConflict("The draft lost its recorded incident.")
            incident.outcome = result.outcome
        else:
            incident = candidate.record(result, **provenance)
        if disposition == "historical":
            incident.status = "wont_fix"
            incident.requires_investigation = False
        work = execution.current_work()
        if work.lease is None or work.execution is None:
            raise MonitoringConflict("Finalization requires the current exact work lease.")
        request = WorkFinalizationRequest(
            **execution.context.model_dump(),
            finalization_id=stable_id(f"{work.key}:finalization"),
            work_id=work.work_id, expected_work_revision=work.revision,
            lease=work.lease, source_execution=work.execution,
            incident_identity=execution.incident, incident=incident,
            source_disposition=disposition or ("duplicate" if result.outcome == "duplicate_suppressed" else "triaged"),
            action_reservation_id=execution.reservation.reservation_id if execution.reservation else None,
        )
        self.monitoring.finalize_work(request)
        persisted = self.monitoring.get_incident(execution.incident)
        if persisted is None:
            raise MonitoringStoreError("Finalization returned no durable incident.")
        if self.fixture and isinstance(self.store, InMemoryIncidentStore):
            # Offline file output is an artifact of the fixture simulation, not
            # a second live authority or a source imported into registry state.
            with self.store._lock:
                self.store._items[persisted.id] = persisted.model_copy(deep=True)
                self.store._persist(persisted)
        if execution.reservation is not None and execution.reservation.state == "rejected":
            self._project_rejected_retry(execution.reservation)
        return persisted

    def _project_rejected_retry(self, action: Any) -> None:
        """Maintain the CLI retry view from durable linked work, never vice versa."""
        if self.retries is None:
            return
        context = action.request.expected
        signature = action.request.incident.signature
        if action.retry_work_id is None:
            self.retries.complete(signature, outcome="confirmed_rejection_no_successor")
            return
        work = self.monitoring.get_work(context, action.retry_work_id)
        if work is None or work.retry_of != action.reservation_id or work.execution != action.request.source_execution:
            raise MonitoringStoreError("Confirmed rejection successor is missing or belongs to another source.")
        if work.state in {"completed", "dispositioned"}:
            return
        target = self.monitoring.resolve_target(work.target, include_inactive=True)
        self.retries.record_linked_retry(
            signature=signature, work_id=work.work_id, retry_of=work.retry_of,
            attempt=work.retry_attempt, due_at=work.due_at, created_at=work.created_at,
            source_execution=work.execution.model_dump(mode="json"), policy_revision=work.policy_revision,
            report_name=target.name if target else "",
        )

    def _fixture_execution(
        self, request: BIRequest, identity: TargetIdentity, signature: str,
        scenario: Scenario | None, pipeline: PipelineToolContext | None,
    ) -> MonitoringExecution:
        action = "pipeline_rerun" if pipeline else "powerbi_refresh"
        parameters = pipeline.failure.target.rerun_parameters if pipeline else None
        if scenario is not None and pipeline is None:
            if scenario.check_schedule:
                action, parameters = "reenable_refresh_schedule", {"enabled": True}
            elif len([row for row in scenario.refresh_history if row.get("status") == "Failed"]) >= 2:
                action = "rebind_dataset_gateway"
                parameters = {
                    "gateway_id": fixture_id("gw-onprem-02", kind="gateway"),
                    "datasource_ids": [fixture_id(identity.item_id, kind="datasource")],
                }
        with fixture_setup(self.monitoring) as setup:
            target = ensure_fixture_target(
                setup, identity, request.report_name or "Synthetic target",
                action=action, parameters=parameters,
            )
        context = self.monitoring_context
        now = fixture_clock(self.monitoring)()
        source = SourceExecutionIdentity(
            target=identity, run_id_kind="fabric_job" if pipeline else "powerbi_request",
            run_id=pipeline.failure.run.id if pipeline else fixture_id(
                f"{scenario.name}:{request.request_id}" if scenario is not None else request.request_id,
                kind=f"{identity.key}:source",
            ),
        )
        observation = SourceRunObservation(
            execution=source, origin="fixture", authority="fixture", observed_at=now,
            started_at=pipeline.failure.run.start_time if pipeline else now - timedelta(minutes=1),
            ended_at=pipeline.failure.run.end_time if pipeline else now - timedelta(seconds=30),
            status="failed", invocation="scheduled", job_type="Pipeline" if pipeline else None,
            failure_reason=(pipeline.failure.error_text() if pipeline else request.error_text())[:4096] or None,
            failure_signature=signature,
            error_code=pipeline.failure.run.error_code or None if pipeline else None,
        )
        identifier = source_work_id(source)
        prior_work = self.monitoring.get_work(context, identifier)
        if prior_work is not None:
            raise MonitoringConflict("The fixture execution is already completed or owned.")
        queued = self.monitoring.enqueue_work(MonitoringWorkDraft(
            **context.model_dump(), work_id=identifier,
            kind="triage", policy_revision=self.monitoring.snapshot(context).control.revision,
            due_at=now, created_at=now, target=identity, execution=source,
            reason="Explicit deterministic scenario source.",
        ))
        claimed = self.monitoring.claim_work(WorkClaimRequest(
            **context.model_dump(), owner_id=stable_id(f"fixture-owner:{source.key}"),
            kinds=("triage",), limit=100, per_workspace_limit=100, lease_seconds=900,
        ))
        work = next((item for item in claimed if item.work_id == queued.work_id), None)
        if work is None:
            raise MonitoringConflict("The fixture execution is already completed or owned.")
        self.monitoring.observe_source(observation, work_id=work.work_id, lease=work.lease)
        if scenario is not None and scenario.check_schedule and scenario.refresh_history and scenario.refresh_history[0].get("status") == "Completed":
            self.monitoring.observe_source(SourceRunObservation(
                execution=SourceExecutionIdentity(
                    target=identity, run_id_kind="powerbi_request",
                    run_id=stable_id(f"{source.key}:healthy-fixture-head"),
                ),
                origin="fixture", authority="fixture", observed_at=now,
                started_at=now - timedelta(seconds=10), ended_at=now - timedelta(seconds=5),
                status="succeeded", invocation="manual",
            ), work_id=work.work_id, lease=work.lease)

        async def refresh_source() -> SourceRunObservation:
            return observation.model_copy(update={"observed_at": fixture_clock(self.monitoring)()})

        return MonitoringExecution(
            store=self.monitoring, work=work,
            incident=IncidentIdentity(target=target.identity, signature=signature),
            observation=observation, approval_channel=self.build_approval_channel(),
            fixture=True, refresh_source=refresh_source,
            clock=fixture_clock(self.monitoring),
        )

    async def observe_powerbi_execution(
        self, execution: SourceExecutionIdentity, client: Any, *,
        work: MonitoringWork | None = None, action: str | None = None,
    ) -> SourceRunObservation:
        target = execution.target
        observed_at = datetime.now(UTC)
        rows = await client.get_refresh_history(target.workspace_id, target.item_id, top=100)
        matches = [
            row for row in rows
            if (
                str(row.get("requestId", "")).lower() == execution.run_id
                if execution.run_id_kind == "powerbi_request"
                else str(row.get("id", "")) == execution.run_id
            )
        ]
        if len(matches) != 1:
            raise MonitoringConflict("Refresh history does not identify one exact source execution.")
        observations: list[SourceRunObservation] = []
        seen: set[str] = set()
        selected = None
        for row in rows:
            try:
                canonical = SourceExecutionIdentity(
                    target=target,
                    run_id_kind="powerbi_request" if row.get("requestId") else "powerbi_refresh",
                    run_id=str(row.get("requestId") or row.get("id") or ""),
                )
                started = datetime.fromisoformat(row["startTime"].replace("Z", "+00:00")) if row.get("startTime") else None
                ended = datetime.fromisoformat(row["endTime"].replace("Z", "+00:00")) if row.get("endTime") else None
                status = {
                    "Completed": "succeeded", "Failed": "failed", "Cancelled": "cancelled",
                    "Unknown": "running", "InProgress": "running", "NotStarted": "not_started",
                }.get(row.get("status"), "unknown")
                reason = row.get("serviceExceptionJson") or row.get("failureReason") or ""
                observation = SourceRunObservation(
                    execution=canonical, origin="fixture" if self.fixture else "poll",
                    authority="fixture" if self.fixture else "rest", observed_at=observed_at,
                    started_at=started, ended_at=ended, status=status,
                    invocation="scheduled" if row.get("refreshType") == "Scheduled" else "manual",
                    failure_reason=(json.dumps(reason) if isinstance(reason, dict) else str(reason))[:4096] or None,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise MonitoringConflict("Refresh history contains unidentifiable or malformed execution evidence.") from exc
            if observation.key in seen:
                raise MonitoringConflict("Refresh history contains duplicate or conflicting execution identities.")
            seen.add(observation.key)
            observations.append(observation)
            if row is matches[0]:
                selected = observation
        if selected is None or selected.execution.run_id_kind != "powerbi_request":
            raise MonitoringConflict("The exact refresh alias has no authoritative request ID.")
        if work is not None:
            if work.lease is None or work.target != target:
                raise MonitoringConflict("History reconciliation requires this target's current work lease.")
            # Record the whole returned window, not just the failure. Otherwise
            # source_head cannot see the newer or active runs we just read.
            for observation in observations:
                self.monitoring.observe_source(observation, work_id=work.work_id, lease=work.lease)
        if action is not None:
            if work is None:
                raise MonitoringConflict("Action history checks require owned shared work.")
            if selected.status != "failed" or selected.started_at is None or selected.ended_at is None:
                raise MonitoringConflict("The exact source is not a complete failed refresh.")
            for observation in observations:
                if observation.execution == selected.execution:
                    continue
                if observation.status in {"running", "not_started", "unknown"}:
                    raise MonitoringConflict("Another refresh is active or its state is unknown.")
                if observation.started_at is None:
                    raise MonitoringConflict("Refresh ordering cannot be established from incomplete history.")
                if observation.started_at > selected.started_at and not (
                    action == "reenable_refresh_schedule" and observation.status == "succeeded"
                ):
                    raise MonitoringConflict("A newer refresh exists; do not remediate the older failure.")
        return selected

    def _bind_powerbi_history(self, execution: MonitoringExecution, client: Any) -> None:
        async def refresh_history(action: str) -> SourceRunObservation:
            try:
                return await self.observe_powerbi_execution(
                    execution.work.execution, client, work=execution.current_work(), action=action,
                )
            except MonitoringConflict:
                raise
            except Exception as exc:
                execution.persistence_error = exc
                raise

        execution.refresh_history = refresh_history

    def enqueue_execution(self, observation: SourceRunObservation) -> MonitoringWork:
        context = self.monitoring_context
        if (observation.execution.target.tenant_id, observation.execution.target.epoch) != (context.tenant_id, context.epoch):
            raise MonitoringConflict("The source execution belongs to another deployment.")
        control = self.monitoring.snapshot(context).control
        if observation.started_at is None or observation.started_at < control.activation_cutoff:
            raise MonitoringConflict("The source execution has no current-epoch start time.")
        return self.monitoring.enqueue_work(MonitoringWorkDraft(
            **context.model_dump(), work_id=source_work_id(observation.execution),
            kind="triage", policy_revision=control.revision,
            due_at=datetime.now(UTC), created_at=datetime.now(UTC),
            target=observation.execution.target, execution=observation.execution,
            reason="REST-bound source execution accepted for the common controller.",
        ))

    def enqueue_execution_reference(self, execution: SourceExecutionIdentity) -> MonitoringWork:
        """Accept a reference, not asserted failure facts or remediation permission."""
        context = self.monitoring_context
        if (execution.target.tenant_id, execution.target.epoch) != (context.tenant_id, context.epoch):
            raise MonitoringConflict("Execution reference belongs to another deployment.")
        if self.monitoring.resolve_target(execution.target) is None:
            raise MonitoringConflict("Execution reference is outside current monitoring admission.")
        identifier = source_work_id(execution)
        existing = self.monitoring.get_work(context, identifier)
        if existing is not None:
            if existing.execution != execution:
                raise MonitoringConflict("The work identity names a different execution.")
            return existing
        return self.monitoring.enqueue_work(MonitoringWorkDraft(
            **context.model_dump(), work_id=identifier,
            kind="triage", policy_revision=self.monitoring.snapshot(context).control.revision,
            due_at=datetime.now(UTC), created_at=datetime.now(UTC),
            target=execution.target, execution=execution,
            reason="An operator or mail reference requires exact REST verification before reasoning or action.",
        ))

    async def drain_monitoring_work(
        self, *, limit: int = 20, budget: HeartbeatBudget | None = None,
        prefer: str = "reconcile_state",
    ) -> list[str]:
        if budget is not None and not budget.can_claim():
            return []
        context = self.monitoring_context
        owner = str(uuid4())
        lines: list[str] = []
        leases = {
            "reconcile_state": RECONCILE_LEASE_SECONDS,
            # Sized for a model call plus an approval wait.
            "action": min(
                900,
                max(15, self.settings.triage_timeout_seconds + self.settings.approval_timeout_seconds + 90),
            ),
        }
        # Each caller prefers one pool and borrows the other only when its own
        # is empty. Strict global priority was not enough: with reconciliation
        # continuously eligible it claimed every automatic turn, so verifying
        # and finalizing already-submitted effects never ran. The heartbeat
        # gives each pool its own worker, so preference here decides who leads,
        # not who is served.
        order = ("reconcile_state", "action")
        if prefer not in leases:
            raise ValueError(f"Unknown monitoring work pool {prefer!r}")
        if prefer == "action":
            order = ("action", "reconcile_state")
        for _ in range(max(1, min(limit, 100))):
            work = None
            for pool in order:
                # Every distinct claim is its own admission decision. Checking
                # once per round was not enough: a slow empty lookup in the
                # first pool could cross the cutoff, and the second pool still
                # leased work outside the window it was admitted under.
                if budget is not None and not budget.can_claim():
                    return lines
                # Claim only what can start now. Leasing a whole batch before
                # slow model/approval calls would let later leases expire in
                # our hands.
                claimed = self.monitoring.claim_work(WorkClaimRequest(
                    **context.model_dump(), owner_id=owner, kinds=MONITORING_POOLS[pool],
                    limit=1, per_workspace_limit=1, lease_seconds=leases[pool],
                ))
                if claimed:
                    work = claimed[0]
                    break
            if work is None:
                break
            lines.append(await self.execute_monitoring_work(work))
        return lines

    async def execute_monitoring_work(
        self, work: MonitoringWork, *, pipeline_client: FabricPipelineClient | None = None,
        powerbi_client: Any = None,
    ) -> str:
        from triage.monitoring.sql_kernel_contracts import work_policy

        policy = work_policy(work.model_dump(mode="json"))
        if policy.component != "controller":
            raise MonitoringConflict("Collection work cannot be dispatched as a controller action.")
        if policy.dispatch_route == "reconcile_state":
            result = await asyncio.to_thread(reconcile_monitoring_work, self.monitoring, work)
            return f"- {work.work_id}: deterministic reconciliation {result.state}."
        if work.execution is None or work.target is None or work.lease is None:
            raise MonitoringConflict("Controller dispatch requires exact leased source work.")
        context = MonitoringContext(tenant_id=work.tenant_id, epoch=work.epoch)
        reservation = (
            self.monitoring.get_action_reservation(context, work.action_reservation_id)
            if work.action_reservation_id is not None else None
        )
        source = self.monitoring.get_source(work.execution)
        if reservation is not None and reservation.state == "rejected":
            if source is None:
                raise MonitoringStoreError("Rejected action finalization has no original source evidence.")
            execution = MonitoringExecution(
                store=self.monitoring, work=work, incident=reservation.request.incident,
                observation=source, fixture=self.fixture, reservation=reservation,
                clock=fixture_clock(self.monitoring) if self.fixture else lambda: datetime.now(UTC),
            )
            result = TriageResult(
                request_id=work.execution.key, signature=execution.incident.signature,
                signature_version=SIGNATURE_VERSION,
                outcome="deferred_retry" if reservation.retry_work_id else "needs_human",
                summary="Definitive no-effect rejection recovered; only its linked successor may submit again.",
            )
            self._finalize_monitoring_result(execution, result, {
                "source": "powerbi_refresh_failure", "agent_name": "MonitoringController",
            })
            return f"- {work.work_id}: confirmed rejection finalized without another POST."
        owns_pipeline_client = pipeline_client is None
        powerbi = powerbi_client
        failure = None
        try:
            if work.target.workload == "fabric_pipeline":
                target = self._pipeline_target(work.target)
                pipeline_client = pipeline_client or self.build_pipeline_client()
                if reservation is None:
                    run = await pipeline_client.get_run(target, work.execution.run_id)
                    if run.id != work.execution.run_id or run.item_id != work.target.item_id:
                        raise MonitoringConflict("The exact pipeline response belongs to a different job or item.")
                    source = SourceRunObservation(
                        execution=work.execution,
                        authority="fixture" if self.fixture else "rest",
                        origin="fixture" if self.fixture else "poll",
                        observed_at=datetime.now(UTC), started_at=run.start_time, ended_at=run.end_time,
                        status={
                            "Failed": "failed", "Completed": "succeeded", "Cancelled": "cancelled",
                            "InProgress": "running", "NotStarted": "not_started",
                        }.get(run.status, "unknown"),
                        invocation="scheduled" if run.invoke_type == "Scheduled" else "manual" if run.invoke_type == "Manual" else "unknown",
                        job_type=run.job_type or None, error_code=run.error_code or None,
                        failure_reason=run.error_text()[:4096] or None,
                        evidence={"job_status": run.status},
                    )
                    source = self.monitoring.observe_source(source, work_id=work.work_id, lease=work.lease)
                    if not run.failed_scheduled or run.end_time is None:
                        return self._disposition_pipeline_candidate(work, source)
                    history = await pipeline_client.list_runs(target)
                    activities = await pipeline_client.activity_runs(target, run)
                    failure = PipelineFailure(target=target, run=run, recent_runs=history, activities=activities)
                    source = source.model_copy(update={"failure_reason": failure.error_text()[:4096] or None})
            else:
                powerbi = powerbi or self.build_powerbi()
                if reservation is None:
                    source = await self.observe_powerbi_execution(work.execution, powerbi, work=work)
            if source is None:
                raise MonitoringConflict("The controller cannot finalize without recorded source evidence.")
            identity = reservation.request.incident if reservation else IncidentIdentity(
                target=work.target, signature=target_signature(
                    work.target, source.failure_reason or source.error_code or "Unspecified failure",
                    exception_class=source.error_code,
                ),
            )
            if reservation is None:
                source = source.model_copy(update={"failure_signature": identity.signature})
                source = self.monitoring.observe_source(source, work_id=work.work_id, lease=work.lease)
            execution = MonitoringExecution(
                store=self.monitoring, work=work, incident=identity, observation=source,
                approval_channel=self.build_approval_channel(), fixture=self.fixture,
                reservation=reservation, powerbi_client=powerbi,
            )
            if powerbi is not None:
                self._bind_powerbi_history(execution, powerbi)
            elif failure is not None:
                async def refresh_source():
                    run = await pipeline_client.get_run(failure.target, work.execution.run_id)
                    return SourceRunObservation(
                        execution=work.execution, authority="rest", origin="poll",
                        observed_at=datetime.now(UTC), started_at=run.start_time, ended_at=run.end_time,
                        status="failed" if run.status == "Failed" else "unknown",
                        invocation="scheduled" if run.invoke_type == "Scheduled" else "manual",
                        job_type=run.job_type, error_code=run.error_code or None,
                        failure_reason=run.error_text()[:4096] or None,
                    )
                execution.refresh_source = refresh_source
            if reservation is not None:
                if reservation.state == "reserved":
                    execution._submission(execution=None, detail="A previous invocation reserved an action without a confirmed submission.")
                if execution.reservation.next_verification_at and execution.reservation.next_verification_at > datetime.now(UTC):
                    self.monitoring.renew_lease(LeaseRenewal(lease=work.lease, lease_seconds=15))
                    return f"- {work.work_id}: waiting for the recorded verification interval; no action resubmitted."
                if execution.reservation.state in {"verified_succeeded", "verified_failed"}:
                    from triage.tools.powerbi import RefreshOutcome

                    outcome = RefreshOutcome(
                        status="Completed" if execution.reservation.state == "verified_succeeded" else "Failed",
                        detail=execution.reservation.detail,
                    )
                elif pipeline_client is not None:
                    outcome = await execution.verify_pipeline(pipeline_client, target)
                elif execution.reservation.request.action in {"rebind_dataset_gateway", "reenable_refresh_schedule"}:
                    outcome = await execution.verify_configuration(powerbi)
                else:
                    outcome = await execution.verify_refresh(powerbi)
                if execution.reservation.state not in {"verified_succeeded", "verified_failed"}:
                    current = execution.current_work()
                    self.monitoring.renew_lease(LeaseRenewal(lease=current.lease, lease_seconds=15))
                    return f"- {work.work_id}: action remains uncertain; read-only reconciliation retained."
                result = TriageResult(
                    request_id=work.execution.key, signature=identity.signature,
                    signature_version=SIGNATURE_VERSION,
                    outcome="resolved" if outcome.succeeded else "needs_human",
                    summary=outcome.detail or f"Exact action verified {outcome.status}.",
                )
                self._finalize_monitoring_result(execution, result, {
                    "report_name": self.monitoring.resolve_target(work.target, include_inactive=True).name,
                    "source": "fabric_pipeline_failure" if pipeline_client else "powerbi_refresh_failure",
                    "agent_name": "MonitoringController",
                })
                return f"- {work.work_id}: {result.outcome}; exact action verification finalized."
            control = self.monitoring.snapshot(context).control
            if source.started_at is None or source.started_at < control.activation_cutoff:
                execution.incident = IncidentIdentity(
                    target=work.target,
                    signature=compute_signature(
                        source="historical_source", error=source.failure_reason or "Unknown source time",
                        target_key=work.target.key, artifact_name=work.execution.key,
                    )[0],
                )
                result = TriageResult(
                    request_id=work.execution.key, signature=execution.incident.signature,
                    outcome="needs_human", summary="Source evidence predates the monitoring cutoff or lacks a valid start time.",
                )
                self._finalize_monitoring_result(
                    execution, result, {"source": "monitoring_historical_baseline"},
                    disposition="historical",
                )
                return f"- {work.work_id}: historical baseline recorded; no native incident budget was consumed."
            if source.status != "failed":
                result = TriageResult(
                    request_id=work.execution.key, signature=identity.signature,
                    signature_version=SIGNATURE_VERSION, outcome="needs_human",
                    summary="The exact source is not a current failed execution; no action was submitted.",
                )
                self._finalize_monitoring_result(execution, result, {"source": "monitoring_diagnostic"})
                return f"- {work.work_id}: source no longer eligible; non-executing outcome finalized."
            if work.kind == "deferred_retry":
                if work.retry_of is not None:
                    parent = self.monitoring.get_action_reservation(context, work.retry_of)
                    if parent is None or parent.retry_work_id != work.work_id or parent.request.source_execution != work.execution:
                        raise MonitoringStoreError("Deferred retry has no matching rejected predecessor.")
                    row = {
                        "signature": parent.request.incident.signature,
                        "reason": parent.detail, "attempts": work.retry_attempt,
                        "report_name": self.monitoring.resolve_target(work.target, include_inactive=True).name,
                    }
                else:
                    row = self.retries.get(source.failure_signature or identity.signature)
                    if row is None:
                        raise MonitoringConflict("Deferred work has no authoritative retry row.")
                return "\n".join(await self._execute_deferred_work(work, row, powerbi))
            if failure is not None:
                artifacts = await self.run_pipeline_failure(
                    failure, client=pipeline_client, reruns=self.build_pipeline_rerun_store(),
                    monitoring=execution,
                )
            else:
                target_view = self.monitoring.resolve_target(work.target)
                request = BIRequest(
                    request_id=work.execution.key, source="interactive", sender="monitoring-controller",
                    received_at=source.ended_at.isoformat() if source.ended_at else "",
                    subject="Verified Power BI source failure", body=source.failure_reason or "Power BI reported failure.",
                    report_name=target_view.name if target_view else "",
                    workspace_id=work.target.workspace_id, dataset_id=work.target.item_id,
                )
                artifacts = await self.run_request(request, monitoring=execution)
            return f"- {work.work_id}: {artifacts.result.outcome}; incident and processed source finalized."
        finally:
            if pipeline_client is not None and owns_pipeline_client:
                await pipeline_client.close()

    def _disposition_pipeline_candidate(self, work: MonitoringWork, source: SourceRunObservation) -> str:
        """An event is only a candidate; REST-ineligible jobs do not enter triage."""
        detail = (
            f"Exact pipeline job is {source.status}, invocation={source.invocation}, "
            f"job_type={source.job_type}; no pipeline remediation was authorized."
        )
        context = MonitoringContext(tenant_id=work.tenant_id, epoch=work.epoch)
        current = self.monitoring.get_work(context, work.work_id)
        if current is None or current.lease is None or (
            current.lease.owner_id, current.lease.fence, current.lease.resource_key
        ) != (work.lease.owner_id, work.lease.fence, work.lease.resource_key):
            raise MonitoringConflict("Pipeline candidate disposition lost its work ownership.")
        if (
            source.invocation == "scheduled" and source.job_type in PIPELINE_JOB_TYPES
            and (
                source.evidence.get("job_status") not in PIPELINE_TERMINAL_STATUSES
                or source.status == "failed" and source.ended_at is None
            )
        ):
            self.monitoring.disposition_work(WorkDispositionRequest(
                **context.model_dump(), request_id=stable_id(f"{work.key}:candidate-wait:{current.attempts}"),
                work_id=work.work_id, expected_work_revision=current.revision, lease=current.lease,
                disposition="retry",
                retry_at=(fixture_clock(self.monitoring)() if self.fixture else datetime.now(UTC)) + timedelta(seconds=60),
                detail=detail,
            ))
            return f"- {work.work_id}: pipeline candidate is not terminal; read-only follow-up queued."
        identity = IncidentIdentity(
            target=work.target,
            signature=compute_signature(
                source="pipeline_candidate", error=f"{source.status}:{source.invocation}:{source.job_type}",
                target_key=work.target.key, artifact_kind="diagnostic",
            )[0],
        )
        execution = MonitoringExecution(
            store=self.monitoring, work=current, incident=identity, observation=source, fixture=self.fixture,
        )
        result = TriageResult(
            request_id=work.execution.key, signature=identity.signature,
            signature_version=SIGNATURE_VERSION, outcome="needs_human", summary=detail,
        )
        self._finalize_monitoring_result(
            execution, result, {"source": "monitoring_candidate", "agent_name": "MonitoringController"},
            disposition="refused",
        )
        return f"- {work.work_id}: ineligible pipeline candidate durably refused ({source.status}, {source.invocation})."


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
