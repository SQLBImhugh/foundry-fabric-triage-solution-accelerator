"""Command-center read models and authenticated operator operations."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from triage.command_center.auth import require
from triage.command_center.incident_workflow import IncidentWorkflowService
from triage.command_center.models import (
    Actor,
    ApiFailure,
    AskInput,
    CommandInput,
    DecisionInput,
    WebSettings,
)
from triage.command_center.monitoring import (
    BootstrapResponse,
    MonitoringService,
    _store_errors,
    resolve_command_target,
)
from triage.knowledge.playbooks import PLAYBOOKS
from triage.models import Incident, TriageResult
from triage.monitoring.contracts import (
    MonitoringComponentDenied,
    MonitoringConflict,
    WebMonitoringStore,
)
from triage.monitoring.runtime import (
    FIXTURE_TENANT_ID,
    build_monitoring_store,
    fixture_approvals,
)
from triage.pipeline_models import canonical_id
from triage.prompts import load_prompt
from triage.providers import get_provider
from triage.redaction import redact_text
from triage.store.approvals import AzureSqlApprovalChannel
from triage.store.azure_sql import AzureSqlDatabase, quote_identifier

logger = logging.getLogger("triage.command_center.service")
_LIMIT = 200


def _observer_message(question: str, evidence: dict[str, Any], collaboration: dict[str, Any] | None) -> str:
    """Keep annotations and valid JSON within the existing observer context budget."""
    limit = 20000
    payload: dict[str, Any] = {"question": question}
    if collaboration is not None:
        payload["collaboration"] = {**collaboration, "activity": list(collaboration["activity"])}
    payload["recorded_evidence"] = evidence

    def encoded() -> str:
        return json.dumps(payload, default=str, ensure_ascii=False)

    content = encoded()
    if len(content) <= limit:
        return content
    text = json.dumps(evidence, default=str, ensure_ascii=False)
    payload["recorded_evidence"] = {"truncated": True, "excerpt": ""}
    annotations = payload.get("collaboration")
    while len(encoded()) > limit and annotations and annotations["activity"]:
        annotations["activity"].pop(0)
        annotations["truncated"] = True
    if len(encoded()) > limit:
        raise ApiFailure(422, "observer_context_too_large", "The recorded question and tracking context exceed the observer limit.")
    low, high = 0, min(len(text), limit)
    while low < high:
        middle = (low + high + 1) // 2
        payload["recorded_evidence"]["excerpt"] = text[:middle]
        if len(encoded()) <= limit:
            low = middle
        else:
            high = middle - 1
    payload["recorded_evidence"]["excerpt"] = text[:low]
    return encoded()


def utcnow() -> str:
    return datetime.now(UTC).isoformat()


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except ValueError:
        return None


def approval_status(row: dict[str, Any]) -> str:
    if row.get("consumed_at"):
        return "consumed"
    if row.get("decision") == "approve":
        return "decision_recorded"
    if row.get("decision") in {"decline", "deny"}:
        return "rejected"
    if row.get("decision"):
        return "invalid"
    expiry = parse_time(row.get("expires_at"))
    if expiry is None:
        return "unavailable"
    return "pending" if expiry > datetime.now(UTC) else "expired"


def proposal_view(row: dict[str, Any], actor: Actor) -> dict[str, Any]:
    state = approval_status(row)
    return {
        **{name: row.get(name, "") for name in (
            "request_id", "action", "justification", "impact", "fingerprint",
            "requested_at", "expires_at", "decision", "responder", "reason",
        )},
        "arguments": row.get("arguments") or {},
        "status": state,
        "can_decide": state == "pending" and actor.permits("approver"),
    }


def run_summary(record) -> dict[str, Any]:
    result = record.result
    state = record.state
    started = parse_time(record.started_at)
    if state == "running" and started and started < datetime.now(UTC) - timedelta(minutes=20):
        state = "unconfirmed"
    return {
        "id": record.id, "request_id": record.request_id, "incident_id": record.incident_id,
        "signature": record.signature, "target": record.target, "workload": record.workload,
        "agent_name": record.agent_name, "state": state,
        "outcome": "answered" if record.workload == "question" and result else record.outcome,
        "summary": record.summary, "started_at": record.started_at,
        "finished_at": record.finished_at,
        "duration_ms": result.wall_clock_ms if result else 0,
        "tool_calls": result.tool_calls if result else 0,
        "tokens_used": result.tokens_used if result else 0,
        "write_actions": result.write_actions if result else 0,
    }


class CommandCenterService:
    def __init__(
        self, settings, web: WebSettings, *, history=None, incidents=None, approvals=None,
        db=None,
        incident_workflow: IncidentWorkflowService | None = None,
        monitoring_store: WebMonitoringStore | None = None,
    ):
        from triage.store.command_center import (
            AzureSqlCommandCenterStore,
            InMemoryCommandCenterStore,
        )
        from triage.store.incidents import InMemoryIncidentStore

        self.settings = settings
        self.web = web
        self.db = db
        self._monitoring_fixture = web.mode == "demo"
        self._monitoring_lock = threading.Lock()
        if self._monitoring_fixture:
            self._monitoring_tenant_id = FIXTURE_TENANT_ID
        else:
            if settings.monitoring_mode != "live":
                raise ValueError("Live command center requires MONITORING_MODE=live.")
            self._monitoring_tenant_id = canonical_id(web.tenant_id)
            if canonical_id(settings.monitoring_tenant_id) != self._monitoring_tenant_id:
                raise ValueError("The monitoring tenant must match the command-center Entra deployment tenant.")
        self._monitoring_service = (
            MonitoringService(monitoring_store, tenant_id=self._monitoring_tenant_id)
            if monitoring_store is not None else None
        )
        if web.mode == "live":
            if not settings.azure_sql_server or not settings.azure_sql_database:
                raise ValueError("Live command center requires Azure SQL server and database settings")
            self.db = db or AzureSqlDatabase(
                server=settings.azure_sql_server, database=settings.azure_sql_database,
            )
            self.history = history or AzureSqlCommandCenterStore(
                self.db, run_table=settings.agent_run_table_name,
                event_table=settings.agent_event_table_name, command_table=settings.agent_command_table_name,
            )
            self.approvals = approvals or AzureSqlApprovalChannel(
                db=self.db, table=settings.approval_table_name,
            )
            self.incidents = None
        else:
            # No Fabric constructor is reached in demo mode, even if .env has
            # real connection settings left over from an operator deployment.
            self.history = history or InMemoryCommandCenterStore()
            self.incidents = incidents or InMemoryIncidentStore()
            self.approvals = approvals if approvals is not None else fixture_approvals(self.monitoring.store)
        self.incident_workflow = incident_workflow or IncidentWorkflowService(
            self, activity_table=settings.incident_activity_table_name,
        )
        self.demo_runner = None
        self.demo_tasks: list[asyncio.Task] = []

    @property
    def monitoring(self) -> MonitoringService:
        # One explicit fixture authority is shared with DemoRunner. Failed live
        # construction is not cached, and never becomes a fixture on retry.
        with self._monitoring_lock:
            if self._monitoring_service is None:
                try:
                    store = build_monitoring_store(
                        self.settings, db=self.db, fixture=self._monitoring_fixture, component="web",
                    )
                    if not self._monitoring_fixture and store.component != "web":
                        raise MonitoringComponentDenied("The live API requires the web monitoring component.")
                except MonitoringConflict as exc:
                    raise ApiFailure(
                        503, "monitoring_bootstrap_mismatch",
                        "The monitoring factory could not validate the deployment context.",
                    ) from exc
                self._monitoring_service = MonitoringService(
                    store, tenant_id=self._monitoring_tenant_id,
                )
            return self._monitoring_service

    def validate_web_settings(self, web: WebSettings) -> None:
        if self.web.mode == "live" and (
            web.mode != "live" or canonical_id(web.tenant_id) != self._monitoring_tenant_id
        ):
            raise ValueError("Live API authentication must use the monitoring deployment's Entra tenant.")

    def monitoring_bootstrap(self, actor: Actor) -> BootstrapResponse:
        require(actor, "reader")
        with _store_errors():
            if self._monitoring_service is not None or self._monitoring_fixture:
                return self.monitoring.bootstrap(actor)
            from triage.monitoring.sql_store import AzureSqlMonitoringStore

            # The normal factory requires valid bootstrap. Inspect the same SQL
            # backend without caching this read-only handle as an admission store.
            store = AzureSqlMonitoringStore(db=self.db, component="web")
            return MonitoringService(store, tenant_id=self._monitoring_tenant_id).bootstrap(actor)

    def target_views(self, actor: Actor) -> list[dict[str, str]]:
        require(actor, "reader")
        with _store_errors():
            return [{
                "id": target.identity.key, "name": target.name,
                "kind": "powerbi_triage" if target.identity.workload == "powerbi" else "pipeline_sweep",
                "description": (
                    "Submit an alert for registry-scoped investigation; unbound reports are diagnostic-only."
                    if target.identity.workload == "powerbi"
                    else "Inspect failed scheduled runs; any permitted rerun still requires approval."
                ),
            } for target in self.monitoring.command_targets(actor)]

    def incident_rows(self) -> list[Incident]:
        if self.incidents is not None:
            return sorted(self.incidents.list_all(), key=lambda row: row.last_seen_at, reverse=True)[:_LIMIT]
        table = quote_identifier(self.settings.incident_table_name)
        return [
            Incident.model_validate_json(row[0])
            for row in self.db.query(f"SELECT TOP ({_LIMIT}) payload FROM {table} ORDER BY updated_at DESC")
        ]

    def incident(self, incident_id: str) -> Incident | None:
        if self.incidents is not None:
            return self.incidents.get(incident_id)
        table = quote_identifier(self.settings.incident_table_name)
        rows = self.db.query(f"SELECT payload FROM {table} WHERE incident_id = ?", incident_id)
        return Incident.model_validate_json(rows[0][0]) if rows else None

    def approval(self, request_id: str) -> dict[str, Any] | None:
        if isinstance(self.approvals, AzureSqlApprovalChannel):
            return self.approvals.get_exact(request_id)
        return self.approvals.get(request_id)

    @staticmethod
    def incident_item(incident: Incident) -> dict[str, Any]:
        status = "needs_review" if incident.requires_investigation and incident.status == "open" else incident.status
        return {
            "id": f"incident:{incident.id}", "source_id": incident.id, "kind": "incident",
            "title": incident.report_name or incident.id,
            "target": incident.report_name or "Unspecified target",
            "workload": "fabric_pipeline" if incident.source.startswith("fabric_pipeline") else "powerbi",
            "agent": incident.agent_name or "TriageAgent", "status": status,
            "severity": "high" if incident.requires_investigation else "medium",
            "summary": incident.diagnosed_root_cause or incident.original_error,
            "created_at": incident.first_seen_at, "updated_at": incident.last_seen_at,
            "expires_at": None, "incident_id": incident.id, "can_decide": False,
        }

    @staticmethod
    def approval_item(row: dict[str, Any], actor: Actor, incidents: list[Incident]) -> dict[str, Any]:
        linked = next((item for item in incidents if row.get("signature") and item.signature == row["signature"]), None)
        state = approval_status(row)
        return {
            "id": f"approval:{row['request_id']}", "source_id": row["request_id"], "kind": "approval",
            "title": row.get("action", "Review proposed action").replace("_", " "),
            "target": row.get("report_name") or "Proposed target",
            "workload": "fabric_pipeline" if row.get("action") == "rerun_fabric_pipeline" else "powerbi",
            "agent": "TriageAgent", "status": state, "severity": "high" if state == "pending" else "low",
            "summary": row.get("justification", ""), "created_at": row.get("requested_at", ""),
            "updated_at": row.get("decided_at") or row.get("requested_at", ""),
            "expires_at": row.get("expires_at"), "incident_id": linked.id if linked else None,
            "can_decide": state == "pending" and actor.permits("approver"),
        }

    @staticmethod
    def command_item(command, targets: list[dict[str, str]]) -> dict[str, Any]:
        target = next((item["name"] for item in targets if item["id"] == command.target_id), command.target_id)
        return {
            "id": f"command:{command.id}", "source_id": command.id, "kind": "command",
            "title": command.subject or command.kind.replace("_", " "),
            "target": target, "workload": "fabric_pipeline" if command.kind == "pipeline_sweep" else "powerbi",
            "agent": "TriageAgent", "status": command.state,
            "severity": "high" if command.state in {"failed", "interrupted"} else "low",
            "summary": command.summary or "An operator requested an investigation.",
            "created_at": command.created_at, "updated_at": command.finished_at or command.started_at or command.created_at,
            "expires_at": command.lease_expires_at, "incident_id": None, "can_decide": False,
        }

    def snapshot(self, actor: Actor) -> dict[str, Any]:
        require(actor, "reader")
        incidents = self.incident_rows()
        proposals = self.approvals.list_requests(_LIMIT)
        runs = self.history.list_runs(limit=50)
        self.history.expire_commands()
        active_commands = self.history.active_commands(limit=100)
        command_history = self.history.commands(limit=50)
        commands = list({command.id: command for command in [*active_commands, *command_history]}.values())
        targets = self.target_views(actor)
        items = (
            [self.approval_item(row, actor, incidents) for row in proposals]
            + [self.incident_item(row) for row in incidents]
            + [self.command_item(row, targets) for row in commands]
        )
        rank = {"pending": 0, "needs_review": 1, "interrupted": 1, "failed": 1, "queued": 2, "running": 2}
        items.sort(key=lambda row: (rank.get(row["status"], 3), row["updated_at"]), reverse=False)
        if self.db is not None:
            rerun_table = quote_identifier(self.settings.pipeline_rerun_table_name)
            verification = self.db.query(f"SELECT COUNT(*) FROM {rerun_table} WHERE state = 'submitted'")[0][0]
        elif self.demo_runner is not None:
            verification = sum(
                len(self.demo_runner.reruns.pending(target.workspace_id, target.pipeline_id))
                for target in self.demo_runner.pipeline_targets(include_inactive=True)
            )
        else:
            verification = 0
        health = [{
            "name": "State", "status": "ok",
            "detail": "Synthetic in-memory state; no Azure effects." if self.web.mode == "demo" else "Azure SQL queries completed.",
        }, {
            "name": "Approvals", "status": "ok" if self.settings.approval_delivery_mode == "web" else "warning",
            "detail": "Web decisions are authoritative; Teams is optional." if self.settings.approval_delivery_mode == "web" else "Controller must use APPROVAL_DELIVERY_MODE=web for Teams-independent decisions.",
        }, {
            "name": "Run history", "status": "ok" if self.settings.run_history_enabled else "warning",
            "detail": "Full run capture enabled." if self.settings.run_history_enabled else "Enable RUN_HISTORY_ENABLED on the controller; earlier aggregate incidents cannot reconstruct missing steps.",
        }]
        snapshot = {
            "schema_version": 1, "mode": self.web.mode, "as_of": utcnow(),
            "actor": actor.model_dump(include={"id", "display_name", "roles"}),
            "counts": {
                "pending_approvals": sum(approval_status(row) == "pending" for row in proposals),
                "needs_investigation": sum(row.requires_investigation and row.status != "resolved" for row in incidents),
                "running": sum(run_summary(row)["state"] == "running" for row in runs),
                "verification_pending": verification,
                "resolved": sum(row.status == "resolved" for row in incidents),
            },
            "work_items": items,
            "recent_runs": [run_summary(row) for row in runs],
            "agents": [
                {"name": "TriageAgent", "role": "controller", "description": "Evidence, policy and bounded remediation."},
                {"name": "DataQualityAgent", "role": "specialist", "description": "Reports deterministic data-quality findings."},
                {"name": "Incident observer", "role": "read-only", "description": "Explains authorized recorded evidence; cannot execute tools."},
            ],
            "health": health, "targets": targets,
            "capabilities": {
                "approve": actor.permits("approver"), "ask": actor.permits("reader"),
                "request_triage": actor.permits("operator"),
            },
            "truncated": len(incidents) == _LIMIT or len(proposals) == _LIMIT or len(runs) == 50 or len(active_commands) == 100,
        }
        return self.incident_workflow.project_snapshot(snapshot, actor)

    def detail(self, kind: str, source_id: str, actor: Actor) -> dict[str, Any]:
        require(actor, "reader")
        proposal, incident, runs = None, None, []
        evidence: list[dict[str, str]] = []
        notes = ""
        if kind == "approval":
            row = self.approval(source_id)
            if row is None:
                raise ApiFailure(404, "not_found", "Approval request not found.")
            proposal = proposal_view(row, actor)
            incidents = self.incident_rows()
            item = self.approval_item(row, actor, incidents)
            if item["incident_id"]:
                incident = self.incident(item["incident_id"])
            if row.get("run_id"):
                run = self.history.get_run(row["run_id"])
                runs = [run] if run else []
            elif row.get("signature"):
                runs = self.history.list_runs(limit=20, signature=row["signature"])
            evidence = [
                {"label": "Exact action", "value": row.get("action", "")},
                {"label": "Fingerprint", "value": row.get("fingerprint", "")},
                {"label": "Impact", "value": row.get("impact", "")},
                {"label": "Decision", "value": row.get("decision") or "No decision recorded"},
                {"label": "Decision recorded by", "value": row.get("responder") or "Not yet answered"},
            ]
        elif kind == "incident":
            incident = self.incident(source_id)
            if incident is None:
                raise ApiFailure(404, "not_found", "Incident not found.")
            item = self.incident_item(incident)
            runs = self.history.list_runs(limit=20, signature=incident.signature)
            evidence = [
                {"label": "Observed error", "value": incident.original_error},
                {"label": "Recorded cause", "value": incident.diagnosed_root_cause},
                {"label": "Recorded action", "value": incident.action_applied or "No remediation recorded"},
                {"label": "Outcome", "value": incident.outcome},
                {"label": "Occurrences", "value": str(incident.occurrence_count)},
                {"label": "Prompt version", "value": incident.prompt_version_hash or "Not recorded"},
            ]
            if incident.pipeline_failure:
                evidence.extend([
                    {"label": "Pipeline", "value": incident.pipeline_failure.target.pipeline_id},
                    {"label": "Failed run", "value": incident.pipeline_failure.run.id},
                    {"label": "Trigger", "value": incident.pipeline_failure.run.invoke_type},
                ])
            notes = incident.triage_notes
        elif kind == "command":
            command = self.history.get_command(source_id)
            if command is None:
                raise ApiFailure(404, "not_found", "Command not found.")
            item = self.command_item(command, self.target_views(actor))
            run = self.history.get_run(command.run_id) if command.run_id else None
            runs = [run] if run else []
            evidence = [
                {"label": "Requested by", "value": command.actor_name or command.actor_id},
                {"label": "Command", "value": command.kind},
                {"label": "Target", "value": command.target_id},
                {"label": "State", "value": command.state},
                {"label": "Result", "value": command.summary or "Waiting for the controller"},
            ]
        else:
            raise ApiFailure(422, "invalid_kind", "Select an approval, incident or command.")
        events = self.history.events(runs[0].id) if runs else []
        return {
            "item": item, "proposal": proposal,
            "incident": incident.model_dump(mode="json") if incident else None,
            "evidence": evidence, "runs": [run_summary(run) for run in runs],
            "timeline": [event.model_dump(mode="json") for event in events], "notes": notes,
        }

    def decide(self, value: DecisionInput, actor: Actor) -> dict[str, Any]:
        require(actor, "approver")
        try:
            row = self.approvals.decide_exact(
                value.request_id, decision="approve" if value.decision == "approve" else "decline",
                fingerprint=value.fingerprint, responder=actor.id, reason=value.reason,
            )
        except KeyError as exc:
            raise ApiFailure(404, "not_found", "Approval request not found.") from exc
        except ValueError as exc:
            raise ApiFailure(409, "decision_conflict", str(exc)) from exc
        return {"status": "decision_recorded", "request": proposal_view(row, actor)}

    def enqueue(self, value: CommandInput, actor: Actor) -> dict[str, Any]:
        from triage.store.command_center import CommandRecord

        require(actor, "operator")
        with _store_errors():
            value = CommandInput.model_validate(value.model_dump())
            registry = self.monitoring
            target = resolve_command_target(
                registry.store, tenant_id=registry.tenant_id,
                target_id=value.target_id, kind=value.kind,
            )
        if value.kind == "powerbi_triage" and not (value.subject.strip() or value.body.strip()):
            raise ApiFailure(422, "missing_alert", "Provide the alert to investigate.")
        try:
            command = self.history.enqueue(CommandRecord(
                id=value.idempotency_key, kind=value.kind, target_id=target.identity.key,
                subject=value.subject, body=value.body,
                actor_id=actor.id, actor_name=actor.display_name,
            ))
        except ValueError as exc:
            raise ApiFailure(409, "command_conflict", str(exc)) from exc
        return {"command_id": command.id, "status": command.state}

    def reconcile(self, command_id: str, reason: str, actor: Actor) -> dict[str, Any]:
        require(actor, "admin")
        try:
            self.history.reconcile_command(command_id, actor.id, reason)
        except KeyError as exc:
            raise ApiFailure(404, "not_found", "Command not found.") from exc
        except ValueError as exc:
            raise ApiFailure(409, "reconciliation_conflict", str(exc)) from exc
        return {"status": "reconciled", "command_id": command_id}

    async def ask(self, value: AskInput, actor: Actor) -> dict[str, Any]:
        from triage.store.command_center import RunEvent, RunRecord

        require(actor, "reader")
        incident = await asyncio.to_thread(self.incident, value.incident_id)
        proposal = None if incident else await asyncio.to_thread(self.approval, value.incident_id)
        if incident is None and proposal is None:
            raise ApiFailure(404, "not_found", "No authorized incident or request exists for this question.")
        context = (
            incident.model_dump(mode="json")
            if incident else {**proposal, "state": approval_status(proposal)}
        )
        collaboration = await asyncio.to_thread(
            self.incident_workflow.observer_context, incident.id, actor,
        ) if incident else None
        target = incident.report_name if incident else proposal.get("report_name", "Approval")
        run_id = str(uuid4())
        await asyncio.to_thread(self.history.start_run, RunRecord(
            id=run_id, request_id=f"question:{run_id}", incident_id=incident.id if incident else "",
            signature=incident.signature if incident else proposal.get("signature", ""),
            target=target, workload="question", agent_name="Incident observer",
        ))
        await asyncio.to_thread(self.history.append_event, RunEvent(
            run_id=run_id, sequence=1, kind="question", label="Read-only question",
            status="recorded", detail=value.question,
        ))
        mode = "records"
        try:
            if self.web.question_provider == "model" and self.web.mode == "live":
                provider = get_provider("observer", self.settings)
                try:
                    async with asyncio.timeout(90):
                        response = await provider.complete(messages=[
                            {"role": "system", "content": load_prompt("observer_system.md")},
                            {"role": "user", "content": _observer_message(
                                redact_text(value.question), context, collaboration,
                            )},
                        ], tools=[])
                    if response.wants_tools or not response.content.strip():
                        raise ApiFailure(502, "observer_refused", "The read-only observer did not return a tool-free answer. No action was executed.")
                    answer = redact_text(response.content.strip())
                    mode = "model"
                finally:
                    await provider.close()
            else:
                state = incident.status if incident else approval_status(proposal)
                cause = incident.diagnosed_root_cause if incident else proposal.get("justification", "")
                action = incident.action_applied if incident else proposal.get("action", "")
                answer = (
                    f"Recorded status for {target or value.incident_id}: {state}.\n\n"
                    f"{cause or 'No cause has been recorded.'}\n\n"
                    f"Recorded or proposed action: {action or 'none'}.\n\n"
                    "This is a records-based explanation. No action was executed. "
                    "Use the explicit proposal controls to approve or deny, or submit a new investigation."
                )
                if collaboration:
                    tracking = collaboration["tracking"]
                    if tracking["status"] == "resolved_by_user":
                        answer += (
                            "\n\nCase tracking: resolved by user. This is an operator decision, "
                            "not an agent-verified repair.\n"
                            f"Resolution note: {tracking['resolution_note'] or 'Not recorded.'}"
                        )
                    notes = [row["body"] for row in collaboration["activity"] if row["kind"] == "note"]
                    if notes:
                        answer += "\n\nRecent operator notes (not verified diagnostics):\n" + "\n".join(f"- {note}" for note in notes)
            await asyncio.to_thread(self.history.finish_run, run_id, TriageResult(
                outcome="resolved", request_id=f"question:{run_id}",
                summary=answer, action_taken="read_only_answer",
            ), incident_id=incident.id if incident else "")
        except Exception as exc:
            await asyncio.to_thread(self.history.finish_run, run_id, TriageResult(
                outcome="agent_crashed", request_id=f"question:{run_id}",
                summary="Read-only question could not be answered.",
                exception_class=type(exc).__name__,
            ))
            raise
        return {
            "answer": answer, "mode": mode, "question_id": run_id,
            "references": [{"id": value.incident_id, "label": target or value.incident_id, "kind": "incident" if incident else "approval"}],
        }

    @staticmethod
    def knowledge() -> dict[str, Any]:
        return {"items": [{
            "name": book.name, "workload": book.workload, "summary": book.summary,
            "retry_useful": book.retry_useful, "guidance": book.guidance,
            "source": book.source, "watch_out": book.watch_out,
        } for book in PLAYBOOKS]}
