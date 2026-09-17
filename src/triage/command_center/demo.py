"""Isolated synthetic data and real controller flows for local UI evaluation."""

from __future__ import annotations

import asyncio
import logging
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from triage.models import BIRequest, TriageAction, TriageResult
from triage.monitoring.runtime import (
    FIXTURE_TENANT_ID,
    ensure_fixture_target,
    fixture_component,
    fixture_setup,
    fixture_target,
)
from triage.pipeline_models import PipelineActivity, PipelineFailure, PipelineRun
from triage.runner import TriageRunner
from triage.store.pipeline_reruns import InMemoryPipelineRerunStore
from triage.store.processed import InMemoryProcessedLog
from triage.tools.fabric_pipeline import MockFabricPipelineClient
from triage.tools.powerbi import MockPowerBIClient

logger = logging.getLogger("triage.command_center.demo")
WORKSPACE = "10000000-0000-0000-0000-000000000001"
PIPELINE = "20000000-0000-0000-0000-000000000002"
DATASET = "60000000-0000-0000-0000-000000000006"


class DemoRunner(TriageRunner):
    def __init__(self, service, folder: Path):
        self.demo_service = service
        self.processed = InMemoryProcessedLog()
        self.reruns = InMemoryPipelineRerunStore()
        self.pipeline_client = MockFabricPipelineClient()
        super().__init__(
            service.settings, base_dir=folder, store=service.incidents,
            command_center_store=service.history,
            monitoring_store=fixture_component(service.monitoring.store, "controller"), fixture=True,
        )

    def build_approval_channel(self):
        return self.demo_service.approvals

    def build_processed_log(self):
        return self.processed

    def build_pipeline_rerun_store(self):
        return self.reruns

    def build_pipeline_client(self):
        return self.pipeline_client

    def build_powerbi(self, scenario=None):
        if scenario is not None:
            return super().build_powerbi(scenario)
        return MockPowerBIClient(latency_ms=75, history=[
            {"status": "Failed", "refreshType": "Scheduled", "serviceExceptionJson": "GatewayUnavailable"},
            {"status": "Failed", "refreshType": "Scheduled", "serviceExceptionJson": "GatewayUnavailable"},
        ])


async def start_demo(service) -> None:
    from triage.store.command_center import RunEvent, RunRecord

    service.settings = service.settings.model_copy(update={
        "azure_sql_server": "", "azure_sql_database": "",
        "triage_provider_mode": "mock", "triage_tool_mode": "mock",
        "monitoring_mode": "fixture", "monitoring_tenant_id": FIXTURE_TENANT_ID,
        "pipeline_sweep_enabled": True, "run_history_enabled": True,
        "approval_delivery_mode": "web", "notification_channel": "web",
        "teams_webhook_url": "", "applicationinsights_connection_string": "",
        "approval_timeout_seconds": 900,
    })
    registry = service.monitoring.store
    with fixture_setup(registry) as setup:
        ensure_fixture_target(
            setup, fixture_target("powerbi", WORKSPACE, DATASET), "Customer service performance",
        )
        ensure_fixture_target(
            setup, fixture_target("fabric_pipeline", WORKSPACE, PIPELINE), "Orders daily ingestion",
            action="pipeline_rerun", parameters={"businessDate": "2026-09-10"},
        )
    # This temporary folder belongs only to this explicitly labelled demo;
    # no developer runs/ or Fabric state is opened.
    temporary = tempfile.TemporaryDirectory(prefix="triage-command-center-demo-")
    folder = Path(temporary.name)
    runner = DemoRunner(service, folder)
    target = next(
        item for item in runner.pipeline_targets()
        if item.workspace_id == WORKSPACE and item.pipeline_id == PIPELINE
    )
    service.demo_runner = runner
    service.demo_temporary = temporary
    now = datetime.now(UTC)
    cases = [
        ("Stock availability", "demo-stock", "needs_human", "Source refresh reported invalid credentials.", "powerbi"),
        ("Regional sales", "demo-sales", "resolved", "A single transient refresh completed after retry.", "powerbi"),
        ("Warehouse settlement", "demo-warehouse", "needs_human", "A SQL batch write timed out; commit state needs reconciliation.", "fabric_pipeline"),
        ("Daily revenue", "demo-revenue", "needs_human", "The refresh schedule is disabled and needs an operator review.", "powerbi"),
    ]
    for index, (name, signature, outcome, summary, workload) in enumerate(cases):
        run_id = str(uuid4())
        started = now - timedelta(minutes=15 + index * 13)
        result = TriageResult(
            outcome=outcome, request_id=f"synthetic:{signature}", signature=signature,
            summary=summary, root_cause=summary,
            action_taken="refresh_powerbi_dataset" if outcome == "resolved" else "",
            started_at=started.isoformat(), finished_at=(started + timedelta(seconds=8)).isoformat(),
            tool_calls=4, llm_turns=4, tokens_used=1840 + index * 123, wall_clock_ms=8000,
            write_actions=1 if outcome == "resolved" else 0,
            actions=[
                TriageAction(tool_name="get_request_context", result_summary="Read the synthetic request"),
                TriageAction(tool_name="get_known_incidents", result_summary="Compared the incident signature"),
                TriageAction(tool_name="report_resolution", result_summary=summary),
            ],
        )
        incident = service.incidents.record(
            result, report_name=name, original_error=summary, agent_name="TriageAgent",
            source="fabric_pipeline_failure" if workload == "fabric_pipeline" else "powerbi_refresh_failure",
        )
        service.history.start_run(RunRecord(
            id=run_id, request_id=result.request_id, signature=signature,
            target=name, workload=workload, started_at=started.isoformat(),
        ))
        for sequence, action in enumerate(result.actions, start=1):
            service.history.append_event(RunEvent(
                run_id=run_id, sequence=sequence, kind="tool_completed",
                label=action.tool_name, tool_name=action.tool_name,
                status="recorded", detail=action.result_summary, timestamp=started.isoformat(),
            ))
        service.history.finish_run(run_id, result, incident_id=incident.id)

    pipeline_run = PipelineRun(
        id="30000000-0000-0000-0000-000000000003", item_id=PIPELINE,
        status="Failed", job_type="Pipeline", invoke_type="Scheduled",
        start_time=now - timedelta(minutes=3), end_time=now - timedelta(minutes=2),
        error_code="ADLSGen2OperationFailed", failure_reason="InternalServerError",
    )
    runner.pipeline_client = MockFabricPipelineClient(
        [pipeline_run], activities=[
            PipelineActivity(
                name="CopyOrders", activity_type="Copy", status="Failed",
                error_code="ADLSGen2OperationFailed", message="InternalServerError",
            ),
        ],
    )
    request = BIRequest(
        request_id=f"demo-gateway-{uuid4().hex[:8]}",
        report_name="Customer service performance",
        subject="GatewayUnavailable: repeated scheduled refresh failures",
        body="Two scheduled refreshes failed through the same unavailable gateway.",
        error_code="GatewayUnavailable", workspace_id=WORKSPACE, dataset_id=DATASET, source="mock",
    )

    async def guarded(coro):
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Synthetic command-center controller flow failed")

    service.demo_tasks.extend([
        asyncio.create_task(guarded(runner.run_request(request))),
        asyncio.create_task(guarded(runner.run_pipeline_failure(
            PipelineFailure(target=target, run=pipeline_run),
            client=runner.pipeline_client, reruns=runner.reruns,
        ))),
    ])
    if service.web.demo_worker:
        async def worker():
            from triage.command_center.worker import drain_commands

            while True:
                try:
                    await drain_commands(runner)
                except Exception:
                    logger.exception("Synthetic command drain failed")
                await asyncio.sleep(1)
        service.demo_tasks.append(asyncio.create_task(worker()))
    # Yield to the real synthetic flows so their requests are visible on the
    # first browser poll, rather than inventing pending rows detached from a gate.
    await asyncio.sleep(0.1)
