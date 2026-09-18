"""Drain durable operator commands through the existing controller, never the UI."""

from __future__ import annotations

import asyncio
import inspect
import logging
import socket
import time
from typing import TYPE_CHECKING
from uuid import uuid4

from triage.command_center.models import ApiFailure
from triage.command_center.monitoring import resolve_command_target
from triage.models import BIRequest
from triage.monitoring.models import MonitoringTarget

logger = logging.getLogger("triage.command_center.worker")

if TYPE_CHECKING:
    from triage.monitoring.controller import HeartbeatBudget


class ExecutionUnconfirmed(RuntimeError):
    def __init__(self, message: str, run_id: str = ""):
        super().__init__(message)
        self.run_id = run_id


def _target(runner, command) -> MonitoringTarget:
    return resolve_command_target(
        runner.monitoring, tenant_id=runner.monitoring_context.tenant_id,
        target_id=command.target_id, kind=command.kind,
    )


def _require_pipeline_selection(runner) -> None:
    # An unbound sweep would broaden one human selection to every pipeline.
    # Fail before execution until the core exposes its canonical selection binding.
    parameter = inspect.signature(runner.pipeline_sweep).parameters.get("selection")
    if parameter is None or parameter.kind not in {
        inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }:
        raise ApiFailure(
            503, "controller_binding_unavailable",
            "Selected pipeline commands require the core pipeline_sweep(selection: TargetIdentity) binding.",
        )


async def _execute(runner, command, target: MonitoringTarget) -> tuple[str, str]:
    if command.kind == "pipeline_sweep":
        report = await runner.pipeline_sweep(selection=target.identity)
        if report.status in {"disabled", "unconfigured", "incomplete"}:
            raise RuntimeError(report.summary())
        return report.summary(), report.artifacts[0].run_id if report.artifacts else ""
    request = BIRequest(
        request_id=f"web:{command.id}", sender=command.actor_id,
        subject=command.subject, body=command.body,
        workspace_id=target.identity.workspace_id,
        dataset_id=target.identity.item_id,
        report_name=target.name, source="web",
    )
    artifacts = await runner.run_request(request)
    summary = f"{artifacts.result.outcome}: {artifacts.result.summary}"
    if artifacts.result.write_actions and artifacts.result.outcome != "resolved":
        raise ExecutionUnconfirmed(
            "A remediation was attempted without a verified resolution. "
            "Reconcile the target before another command.", artifacts.run_id,
        )
    return summary, artifacts.run_id


async def drain_commands(runner, *, limit: int = 1, budget: HeartbeatBudget | None = None) -> list[str]:
    if budget is not None and not budget.can_claim():
        return []
    history = await asyncio.to_thread(runner.build_command_center_store)
    await asyncio.to_thread(history.expire_commands)
    claims = await asyncio.to_thread(runner._pipeline_claim_store)
    worker_id = f"{socket.gethostname()}:{uuid4().hex[:12]}"
    lines: list[str] = []
    for candidate in await asyncio.to_thread(history.eligible_commands, limit=100):
        if len(lines) >= max(1, min(limit, 10)) or budget is not None and not budget.can_claim():
            break
        if candidate.state != "queued":
            continue
        target_key = f"command-target:{candidate.target_id.casefold()}"
        execution_budget = runner.settings.triage_timeout_seconds + runner.settings.approval_timeout_seconds + 30
        began = time.monotonic()
        if not await asyncio.to_thread(claims.claim, target_key, lease_seconds=execution_budget + 60):
            continue
        try:
            if await asyncio.to_thread(history.target_blocked, candidate.target_id):
                logger.warning("Command target requires reconciliation: %s", candidate.target_id)
                continue
            if budget is not None and not budget.can_claim():
                break
            remaining = execution_budget - (time.monotonic() - began)
            if remaining <= 0:
                continue
            command = await asyncio.to_thread(
                history.claim_command,
                candidate.id, worker_id, lease_seconds=execution_budget + 60,
            )
            if command is None:
                continue
            run_id = ""
            entered_execution = False
            try:
                target = await asyncio.to_thread(_target, runner, command)
                if command.kind == "pipeline_sweep":
                    _require_pipeline_selection(runner)
                remaining = execution_budget - (time.monotonic() - began)
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
                        await asyncio.to_thread(
                            history.interrupt_command, command.id, worker_id, summary, run_id=run_id,
                        )
                    else:
                        await asyncio.to_thread(
                            history.finish_command, command.id, worker_id, state="failed", summary=summary,
                        )
                except Exception:
                    logger.exception("Could not finalize command uncertainty; its running/expired row remains a target barrier")
                if not isinstance(exc, Exception):
                    raise
                logger.error("Operator command %s: %s", command.id, summary)
            else:
                try:
                    await asyncio.to_thread(
                        history.finish_command,
                        command.id, worker_id, state="completed", summary=summary, run_id=run_id,
                    )
                except Exception:
                    # Never turn a finalization failure into another execution
                    # or a second finish attempt with invalid ownership.
                    logger.exception("Command executed but finalization was not confirmed")
                    summary = f"Execution finished; finalization is unconfirmed. Review command {command.id} and run {run_id}."
            lines.append(f"- {command.id}: {summary}")
        finally:
            await asyncio.to_thread(claims.release, target_key)
    return lines
