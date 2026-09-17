"""Deterministic prerequisites and submission fencing for pipeline tools."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from triage.knowledge.playbooks import pipeline_retry_is_allowed
from triage.pipeline_models import (
    PIPELINE_TERMINAL_STATUSES,
    PipelineActivity,
    PipelineFailure,
    PipelineRerunOutcome,
    PipelineRerunRecord,
    PipelineRun,
    PipelineTarget,
)
from triage.redaction import redact_text
from triage.store.pipeline_reruns import PipelineRerunStore
from triage.tools.fabric_pipeline import FabricPipelineClient, PipelineApiError

if TYPE_CHECKING:
    from triage.monitoring.controller import MonitoringExecution

logger = logging.getLogger("triage.tools.pipeline_actions")


async def verify_rerun(
    client: FabricPipelineClient, target: PipelineTarget, run: PipelineRun,
    *, activities: list[PipelineActivity] | None = None,
) -> PipelineRerunOutcome:
    """A handled activity failure can coexist with a Completed pipeline job."""
    detail = run.failure_reason
    status = run.status
    if status == "Completed":
        if activities is None:
            activities = await client.activity_runs(target, run)
        if not activities:
            raise PipelineApiError("Completed pipeline has no activity evidence to verify")
        failed = [activity for activity in activities if activity.status == "Failed"]
        if failed:
            status = "ActivityFailed"
            detail = "; ".join(f"{activity.name}: {activity.error_code}" for activity in failed)
        elif any(activity.status not in {"Succeeded", "Skipped"} for activity in activities):
            raise PipelineApiError("Completed pipeline has nonterminal or unknown activity evidence")
    return PipelineRerunOutcome(status=status, run_id=run.id, detail=detail)


@dataclass
class PipelineToolContext:
    failure: PipelineFailure
    client: FabricPipelineClient
    reruns: PipelineRerunStore
    signature: str
    live: bool = False
    evidence_read: bool = False
    reservation: PipelineRerunRecord | None = None
    outcome: PipelineRerunOutcome | None = None
    approval_parameter_hash: str = ""
    approved_target: PipelineTarget | None = None
    monitoring: MonitoringExecution | None = None

    async def refusal(self) -> str | None:
        target = self.failure.target
        if self.approval_parameter_hash and target.parameter_hash != self.approval_parameter_hash:
            return "Replay parameters changed after approval was requested."
        if self.failure.run.job_type != "Pipeline":
            return "This job uses a different pipeline execution API. Only Core Pipeline reruns are enabled."
        if not target.permits_rerun:
            return "Replay safety and the complete parameter set have not both been approved in configuration."
        if self.live and self.monitoring is None:
            return "A live pipeline rerun requires a shared monitoring work lease and atomic action reservation."
        if self.failure.diagnostics_error:
            return "Activity diagnostics are incomplete. Review the failure and sink state before replay."
        if any(
            activity.status not in {"Succeeded", "Failed", "Skipped", "Cancelled"}
            for activity in self.failure.activities
        ):
            return "An activity is still active or has an unknown state."
        if any(
            activity.activity_type in {"InvokePipeline", "ExecutePipeline"}
            for activity in self.failure.activities
        ):
            return "Nested pipeline executions require child-run and side-effect reconciliation before parent replay."
        if self.monitoring is None and self.reruns.get(self.failure.key) is not None:
            return "A rerun has already been reserved or submitted for this failed run. Reconcile it; do not submit again."
        if (
            not pipeline_retry_is_allowed(self.failure.error_text())
            or any(
                not pipeline_retry_is_allowed(
                    f"{activity.error_code}: {activity.message}", activity_type=activity.activity_type,
                )
                for activity in self.failure.activities if activity.status == "Failed"
            )
        ):
            return "The failure has no unambiguous retry-candidate playbook. Fix the cause and review sink state first."
        current = await self.client.get_run(target, self.failure.run.id)
        if not current.failed_scheduled or current.end_time is None:
            return "The source run is no longer a completed failed scheduled pipeline job."
        if current.error_text() != self.failure.run.error_text():
            return "The source run's error evidence changed. Triage the new evidence before requesting a rerun."
        if current.start_time is None:
            return "The failed job has no start time; a safe ordering of executions cannot be established."
        history = await self.client.list_runs(target)
        if not any(run.id == current.id for run in history):
            return "The source run is absent from the current history window."
        if any(run.status not in PIPELINE_TERMINAL_STATUSES for run in history):
            return "A pipeline job is active or its state is unknown."
        if any(run.id != current.id and (run.start_time is None or run.start_time > current.start_time) for run in history):
            return "A newer pipeline job exists. Do not replay an older failed run."
        return None

    def approval_arguments(self, justification: str) -> dict[str, str]:
        target = self.failure.target.model_copy(deep=True)
        self.approved_target = target
        self.approval_parameter_hash = target.parameter_hash
        return {
            "justification": justification,
            "workspace_id": target.workspace_id,
            "pipeline_id": target.pipeline_id,
            "failed_run_id": self.failure.run.id,
            "parameter_hash": target.parameter_hash,
            "parameter_preview": redact_text(json.dumps(target.rerun_parameters, sort_keys=True))[:1800],
        }

    def reserve(self) -> bool:
        if self.live:
            raise ValueError("Live pipeline reservations belong to the common monitoring admission boundary.")
        target = self.failure.target
        if target.parameter_hash != self.approval_parameter_hash:
            raise ValueError("Replay parameters changed after approval was requested")
        record = PipelineRerunRecord(
            workspace_id=target.workspace_id, pipeline_id=target.pipeline_id,
            failed_run_id=self.failure.run.id, signature=self.signature,
            parameter_hash=target.parameter_hash,
        )
        if not self.reruns.reserve(record):
            return False
        self.reservation = record
        return True

    async def evidence(self) -> dict[str, Any]:
        reason = await self.refusal()
        self.evidence_read = True
        return {
            "status": "ok",
            **self.failure.evidence(),
            "failure_summary": self.failure.error_text()[:8000] or "Fabric did not return a failure reason.",
            "may_propose_rerun": reason is None,
            "rerun_refusal": reason or "",
        }

    async def submit(self) -> dict[str, Any]:
        if self.monitoring is not None:
            if self.approved_target is None or self.failure.target.parameter_hash != self.approval_parameter_hash:
                raise ValueError("The reviewed pipeline parameters changed after approval.")
            self.outcome = await self.monitoring.submit_pipeline(self.client, self.approved_target)
            return {
                "status": self.outcome.status, "run_id": self.outcome.run_id,
                "succeeded": False, "detail": self.outcome.detail,
            }
        record = self.reservation
        if record is None:
            raise RuntimeError("Pipeline rerun has no durable submission reservation")
        if self.failure.target.parameter_hash != record.parameter_hash:
            raise ValueError("Replay parameters do not match the approved reservation")
        if self.approved_target is None or self.approved_target.parameter_hash != record.parameter_hash:
            raise ValueError("The approved replay profile is not available")
        try:
            submission = await self.client.rerun(self.approved_target)
        except Exception as exc:
            detail = f"Pipeline submission was not confirmed ({type(exc).__name__}). Check run history before any manual replay."
            unknown = record.model_copy(update={"state": "unknown", "detail": detail})
            try:
                if not self.reruns.update(unknown, expected="reserved"):
                    logger.error("Could not transition pipeline reservation to unknown")
            except Exception:
                logger.exception("Could not record ambiguous submission; reservation still prevents a second POST")
            self.outcome = PipelineRerunOutcome(status="Unknown", detail=detail)
            logger.exception("Pipeline submission failed or its acknowledgement was lost")
            return {"status": "submission_unknown", "detail": detail}

        submitted = record.model_copy(update={
            "state": "submitted", "rerun_id": submission.run_id,
            "next_poll_at": datetime.now(UTC) + timedelta(seconds=submission.retry_after_seconds),
            "updated_at": datetime.now(UTC).isoformat(),
        })
        self.outcome = PipelineRerunOutcome(
            status="Submitted", run_id=submission.run_id,
            detail="Fabric accepted the run. Execution has not yet been verified.",
        )
        try:
            attached = self.reruns.update(submitted, expected="reserved")
        except Exception as exc:
            raise RuntimeError(
                f"Fabric accepted job {submission.run_id}, but its correlation could not "
                "be persisted. The reservation remains; do not submit again."
            ) from exc
        if not attached:
            raise RuntimeError(
                f"Fabric accepted job {submission.run_id}, but its reservation changed. "
                "Reconcile the submitted job; do not submit again."
            )
        return {"status": "Submitted", "run_id": submission.run_id, "succeeded": False, "detail": self.outcome.detail}

    async def check_rerun(self) -> dict[str, Any]:
        if self.monitoring is not None:
            self.outcome = await self.monitoring.verify_pipeline(self.client, self.failure.target)
            return {
                "status": self.outcome.status, "run_id": self.outcome.run_id,
                "succeeded": self.outcome.succeeded, "detail": self.outcome.detail,
            }
        record = self.reruns.get(self.failure.key)
        if record is None or not record.rerun_id:
            raise ValueError("There is no correlated rerun to inspect")
        if record.next_poll_at is not None and record.next_poll_at > datetime.now(UTC):
            return {
                "status": "Submitted", "run_id": record.rerun_id, "succeeded": False,
                "detail": "Waiting for the service's Retry-After interval before polling.",
            }
        run = await self.client.get_run(self.failure.target, record.rerun_id)
        if run.id != record.rerun_id or run.item_id != self.failure.target.pipeline_id:
            raise ValueError("Rerun response does not match the approved pipeline job")
        self.outcome = await verify_rerun(self.client, self.failure.target, run)
        if run.status in PIPELINE_TERMINAL_STATUSES and record.state == "submitted":
            finished = record.model_copy(update={
                "state": "completed" if self.outcome.succeeded else "failed",
                "detail": self.outcome.detail,
                "updated_at": datetime.now(UTC).isoformat(),
            })
            if not self.reruns.update(finished, expected="submitted"):
                raise RuntimeError("Pipeline rerun state changed while recording completion")
        elif record.state == "submitted":
            next_read = record.model_copy(update={
                "next_poll_at": datetime.now(UTC) + timedelta(seconds=run.retry_after_seconds),
            })
            if not self.reruns.update(next_read, expected="submitted"):
                raise RuntimeError("Could not retain the pipeline polling interval")
        return {
            "status": self.outcome.status, "run_id": run.id,
            "succeeded": self.outcome.succeeded, "detail": self.outcome.detail,
        }
