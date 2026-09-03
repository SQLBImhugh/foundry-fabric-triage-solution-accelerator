"""Scenario runner — wires everything together for one triage run.

Responsibilities the agent deliberately does NOT have:

* computing the failure signature,
* looking up whether an open incident already exists,
* persisting the terminal outcome.

Keeping those here means the agent is a pure loop that can be tested without a
store, and the dedup/persistence behaviour can be tested without a model.
"""

from __future__ import annotations

import logging
import os
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage_agent import EventHook, TriageAgent, TriageDeps
from triage.detectors.silent_failures import (
    HealthFinding,
    SilentFailureScanner,
    load_probes,
)
from triage.models import BIRequest, Incident, TriageResult
from triage.policy import TriagePolicy
from triage.providers import get_provider
from triage.signature import compute_signature
from triage.store.approvals import JsonFileApprovalChannel
from triage.store.incidents import IncidentStore, JsonFileIncidentStore
from triage.store.processed import JsonFileProcessedLog
from triage.store.retries import JsonFileRetryStore
from triage.store.semantic_health import JsonFileSemanticHealthStore
from triage.tools.dataset import DatasetSource
from triage.tools.flags import DataQualityFlagTable
from triage.tools.inbox import GraphInbox, MockInbox
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
    ):
        self.settings = settings
        self.base_dir = Path(base_dir)
        self.on_event = on_event
        self.store: IncidentStore = store or self._build_store()
        self.flag_table = DataQualityFlagTable(
            flag_table_path or (self.base_dir / "runs" / "dq_flags.csv")
        )
        # Built once per runner rather than per run: a deferral written by one
        # run has to be visible to the precondition check of the next.
        self.retries = self.build_retry_store(retry_store_path)
        self.semantic_health = self.build_semantic_health_store(semantic_health_path)
        self._teams = None

    # --- inbox -------------------------------------------------------------

    @staticmethod
    def _resolve_id(*candidates: str) -> str:
        """First non-empty id wins: scenario, then the alert, then configuration.

        Without the configured fallback the live path called Power BI with
        empty ids and got a 404, and the agent reached a plausible-looking
        conclusion from a tool failure rather than from evidence. A wrong
        answer that reads correctly is the worst kind.
        """
        for candidate in candidates:
            if candidate and candidate.strip():
                return candidate.strip()
        return ""

    def _build_store(self) -> IncidentStore:
        """Choose where incidents live.

        Falls back to the JSON file when no table is configured, so the offline
        rehearsal path is unchanged and needs no Azure dependencies.
        """
        endpoint = self.settings.incident_table_endpoint
        if not endpoint:
            return JsonFileIncidentStore(self.base_dir / "runs" / "incidents.json")

        from triage.store.azure_table import AzureTableIncidentStore

        store = AzureTableIncidentStore(
            endpoint=endpoint, table_name=self.settings.incident_table_name
        )
        if not store.is_durable:
            logger.warning(
                "Incident table %s unavailable; incidents will not survive a restart",
                endpoint,
            )
        return store

    def build_processed_log(self):
        """Where the record of already-triaged mail lives.

        Mirrors ``_build_store``: the table when one is configured, a JSON file
        otherwise, so the offline path needs no Azure dependency. This has to
        outlive the process -- a hosted agent is rebuilt for every invocation,
        so anything held in memory here is always empty on arrival.
        """
        endpoint = self.settings.incident_table_endpoint
        if not endpoint:
            return JsonFileProcessedLog(self.base_dir / "runs" / "processed.json")

        from triage.store.processed import AzureTableProcessedLog

        log = AzureTableProcessedLog(
            endpoint=endpoint, table_name=self.settings.processed_table_name
        )
        if not log.is_durable:
            logger.error(
                "Processed-message log at %s is not durable; scheduled sweeps will "
                "re-triage the same mail and notify repeatedly",
                endpoint,
            )
        return log

    def build_inbox(self):
        if self.settings.triage_tool_mode == "live" and self.settings.graph_tenant_id:
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
            )
        return MockInbox(directory=self.base_dir / "mock" / "emails")

    # --- clients -----------------------------------------------------------

    def build_powerbi(self, scenario: Scenario | None = None):
        if self.settings.triage_tool_mode == "live" and self.settings.powerbi_tenant_id:
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

    async def drain_due_retries(self, *, now: datetime | None = None) -> list[str]:
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
                lines.append(f"- {report}: retry failed ({type(exc).__name__})")
                continue

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

        logger.info("Drained %d due retry/retries", len(due))
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
            # Configuration, not routine state. A deploy re-enables a disabled
            # routine, so the off switch cannot live there.
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

        endpoint = self.settings.incident_table_endpoint
        if not endpoint:
            return JsonFileSemanticHealthStore(self.base_dir / "runs" / "semantic_health.json")

        from triage.store.semantic_health import AzureTableSemanticHealthStore

        store = AzureTableSemanticHealthStore(
            endpoint=endpoint, table_name=self.settings.semantic_health_table_name
        )
        if not store.is_durable:
            logger.error(
                "Semantic health store at %s is not durable; the silent-failure "
                "detector will start blind on every sweep and detect nothing",
                endpoint,
            )
        return store

    def build_health_client(self):
        if self.settings.triage_tool_mode == "live" and self.settings.powerbi_tenant_id:
            return LiveSemanticHealthClient(
                tenant_id=self.settings.powerbi_tenant_id,
                client_id=self.settings.powerbi_client_id,
                client_secret=self.settings.powerbi_client_secret,
            )
        return MockSemanticHealthClient()

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

        endpoint = self.settings.incident_table_endpoint
        if not endpoint:
            return JsonFileRetryStore(self.base_dir / "runs" / "retries.json")

        from triage.store.retries import AzureTableRetryStore

        store = AzureTableRetryStore(
            endpoint=endpoint, table_name=self.settings.retry_table_name
        )
        if not store.is_durable:
            logger.error(
                "Retry store at %s is not durable; deferred retries will be dropped "
                "rather than performed",
                endpoint,
            )
        return store

    def build_approval_channel(self):
        """Where approval requests wait and decisions land.

        Same choice as the incident store: the table when one is configured, a
        JSON file otherwise. It has to be shared state either way -- the whole
        point is that a *different* process writes the answer.
        """
        endpoint = self.settings.incident_table_endpoint
        if not endpoint:
            return JsonFileApprovalChannel(self.base_dir / "runs" / "approvals.json")

        from triage.store.approvals import AzureTableApprovalChannel

        channel = AzureTableApprovalChannel(
            endpoint=endpoint, table_name=self.settings.approval_table_name
        )
        if not channel.is_durable:
            logger.error(
                "Approval channel at %s is not durable; no human can answer and every "
                "gated action will fail closed",
                endpoint,
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
        )

        if scenario is None:
            channel = self.build_approval_channel()
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
            if self.settings.triage_tool_mode == "live" and self.settings.teams_webhook_url:
                self._teams = WorkflowsWebhookTeamsNotifier(self.settings.teams_webhook_url)
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
        self.prepare(scenario, keep_incidents=keep_incidents)
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

    async def run_request(
        self,
        request: BIRequest,
        *,
        scenario: Scenario | None = None,
        datasets: dict[str, DatasetSource] | None = None,
    ) -> RunArtifacts:
        signature, _payload = compute_signature(
            source="powerbi_refresh_failure",
            error=request.error_text(),
            artifact_kind="dataset",
            artifact_name=request.report_name or request.dataset_id or "",
        )
        known = self.store.find_open(signature)

        powerbi = self.build_powerbi(scenario)
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
        dq_agent = DataQualityAgent(get_provider("data_quality", self.settings))

        agent = TriageAgent(
            triage_provider,
            policy=TriagePolicy.from_settings(self.settings),
            dq_agent=dq_agent,
            on_event=self.on_event,
        )

        deps = TriageDeps(
            powerbi=powerbi,
            teams=teams,
            flag_table=self.flag_table,
            datasets=datasets or {},
            workspace_id=self._resolve_id(
                scenario.workspace_id if scenario else "",
                request.workspace_id,
                self.settings.powerbi_workspace_id,
            ),
            dataset_id=self._resolve_id(
                scenario.dataset_id if scenario else "",
                request.dataset_id,
                self.settings.powerbi_dataset_id,
            ),
            signature=signature,
            known_incident=known,
            approval_gate=self.build_approval_gate(scenario),
            approval_timeout_seconds=int(self.settings.approval_timeout_seconds),
            retries=self.retries,
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
        )

        return RunArtifacts(
            result=result,
            incident=incident,
            request=request,
            flag_rows_before=flags_before,
            flag_rows_after=self.flag_table.row_count,
            teams_messages=list(getattr(teams, "messages", [])),
            powerbi_calls=list(getattr(powerbi, "calls", [])),
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

    return failures


def discover_scenarios(directory: Path) -> list[Scenario]:
    return [Scenario.load(p) for p in sorted(Path(directory).glob("*.yaml"))]
