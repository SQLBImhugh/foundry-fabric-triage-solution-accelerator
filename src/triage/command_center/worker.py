"""Drain durable operator commands through the existing controller, never the UI."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from uuid import uuid4

from triage.command_center.service import target_views
from triage.models import BIRequest
from triage.pipeline_models import load_pipeline_targets

logger = logging.getLogger("triage.command_center.worker")


class ExecutionUnconfirmed(RuntimeError):
    def __init__(self, message: str, run_id: str = ""):
        super().__init__(message)
        self.run_id = run_id


async def _execute(runner, command, target) -> tuple[str, str]:
    if command.kind == "pipeline_sweep":
        configured = next(
            item for item in load_pipeline_targets(runner.settings.fabric_pipeline_targets)
            if f"pipeline:{item.key}" == command.target_id
        )
        report = await runner.pipeline_sweep(targets=[configured])
        if report.status in {"disabled", "unconfigured", "incomplete"}:
            raise RuntimeError(report.summary())
        return report.summary(), report.artifacts[0].run_id if report.artifacts else ""
    request = BIRequest(
        request_id=f"web:{command.id}", sender=command.actor_id,
        subject=command.subject, body=command.body,
        workspace_id=runner.settings.powerbi_workspace_id,
        dataset_id=runner.settings.powerbi_dataset_id,
        report_name=target["name"], source="web",
    )
    artifacts = await runner.run_request(request)
    summary = f"{artifacts.result.outcome}: {artifacts.result.summary}"
    if artifacts.result.write_actions and artifacts.result.outcome != "resolved":
        raise ExecutionUnconfirmed(
            "A remediation was attempted without a verified resolution. "
            "Reconcile the target before another command.", artifacts.run_id,
        )
    return summary, artifacts.run_id


async def drain_commands(runner, *, limit: int = 1) -> list[str]:
    history = runner.build_command_center_store()
    history.expire_commands()
    claims = runner._pipeline_claim_store()
    worker_id = f"{socket.gethostname()}:{uuid4().hex[:12]}"
    targets = {target["id"]: target for target in target_views(runner.settings)}
    lines: list[str] = []
    for candidate in history.eligible_commands(limit=100):
        if len(lines) >= max(1, min(limit, 10)):
            break
        if candidate.state != "queued":
            continue
        target_key = f"command-target:{candidate.target_id.casefold()}"
        budget = runner.settings.triage_timeout_seconds + runner.settings.approval_timeout_seconds + 30
        began = time.monotonic()
        if not claims.claim(target_key, lease_seconds=budget + 60):
            continue
        try:
            if history.target_blocked(candidate.target_id):
                logger.warning("Command target requires reconciliation: %s", candidate.target_id)
                continue
            remaining = budget - (time.monotonic() - began)
            if remaining <= 0:
                continue
            command = history.claim_command(
                candidate.id, worker_id, lease_seconds=budget + 60,
            )
            if command is None:
                continue
            run_id = ""
            target = targets.get(command.target_id)
            entered_execution = False
            try:
                if target is None or target["kind"] != command.kind:
                    raise ValueError("The configured command target changed or was removed")
                remaining = budget - (time.monotonic() - began)
                if remaining <= 0:
                    raise TimeoutError("Command acquisition exhausted the execution budget before dispatch")
                entered_execution = True
                async with asyncio.timeout(remaining):
                    summary, run_id = await _execute(runner, command, target)
            except BaseException as exc:
                run_id = getattr(exc, "run_id", run_id)
                summary = (
                    f"Execution is unconfirmed ({type(exc).__name__}). "
                    "The target is blocked until an operator reconciles its state."
                    if entered_execution else f"Command was not executed: {exc}"
                )
                try:
                    if entered_execution:
                        history.interrupt_command(command.id, worker_id, summary, run_id=run_id)
                    else:
                        history.finish_command(command.id, worker_id, state="failed", summary=summary)
                except Exception:
                    logger.exception("Could not finalize command uncertainty; its running/expired row remains a target barrier")
                if not isinstance(exc, Exception):
                    raise
                logger.error("Operator command %s: %s", command.id, summary)
            else:
                try:
                    history.finish_command(
                        command.id, worker_id, state="completed", summary=summary, run_id=run_id,
                    )
                except Exception:
                    # Never turn a finalization failure into another execution
                    # or a second finish attempt with invalid ownership.
                    logger.exception("Command executed but finalization was not confirmed")
                    summary = f"Execution finished; finalization is unconfirmed. Review command {command.id} and run {run_id}."
            lines.append(f"- {command.id}: {summary}")
        finally:
            claims.release(target_key)
    return lines
