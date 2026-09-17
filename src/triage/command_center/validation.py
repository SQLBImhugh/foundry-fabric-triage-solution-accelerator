"""Admin-only, deployed-code scenario checks with isolated synthetic tools/state."""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from triage.command_center.auth import require
from triage.command_center.models import Actor, ApiFailure
from triage.models import TriageResult
from triage.runner import TriageRunner, check_expectations, discover_scenarios
from triage.store.incidents import InMemoryIncidentStore


def scenario_catalog(root: Path):
    return discover_scenarios(root / "scenarios")


async def validate_scenario(service, root: Path, name: str, provider: str, actor: Actor) -> dict:
    from triage.store.command_center import RunEvent, RunRecord

    require(actor, "admin")
    if service.web.mode == "live" and not service.web.validation_enabled:
        raise ApiFailure(403, "validation_disabled", "Deployed scenario validation is disabled.")
    if provider not in {"mock", "foundry"} or service.web.mode == "demo" and provider != "mock":
        raise ApiFailure(422, "invalid_provider", "Select an available validation provider.")
    scenario = next((item for item in scenario_catalog(root) if item.name == name), None)
    if scenario is None:
        raise ApiFailure(404, "not_found", "Scenario not found.")
    if provider == "foundry" and not service.settings.foundry_project_endpoint:
        raise ApiFailure(422, "model_unconfigured", "Configure the Foundry project before model-backed validation.")
    run_id = str(uuid4())
    history = service.history
    await asyncio.to_thread(history.start_run, RunRecord(
        id=run_id, request_id=f"validation:{run_id}", signature=f"validation:{name}:{provider}",
        target=name, workload="validation", agent_name="Scenario validation",
    ))
    started = time.monotonic()
    config = service.settings.model_copy(update={
        "triage_provider_mode": provider, "triage_tool_mode": "mock",
        "azure_sql_server": "", "azure_sql_database": "",
        "run_history_enabled": False, "notification_channel": "teams",
        "approval_delivery_mode": "teams", "teams_webhook_url": "",
        "applicationinsights_connection_string": "",
    })
    try:
        with tempfile.TemporaryDirectory(prefix="triage-validation-") as folder:
            temporary = Path(folder)
            runner = TriageRunner(
                config, base_dir=root, store=InMemoryIncidentStore(),
                flag_table_path=temporary / "flags.csv",
                retry_store_path=temporary / "retries.json",
                semantic_health_path=temporary / "health.json",
            )
            artifacts = await runner.run_scenario(scenario)
            result = artifacts[-1].result
            failures = check_expectations(scenario, artifacts[-1])
    except Exception as exc:
        result = TriageResult(
            outcome="agent_crashed", request_id=f"validation:{run_id}",
            summary="Deployed scenario validation failed.", exception_class=type(exc).__name__,
        )
        failures = [f"Scenario raised {type(exc).__name__}: {exc}"]
    duration = round((time.monotonic() - started) * 1000)
    report = {
        "scenario": name, "provider": provider, "passed": not failures,
        "failures": failures, "run_id": run_id, "outcome": result.outcome,
        "duration_ms": duration,
    }
    def persist_result() -> None:
        history.append_event(RunEvent(
            run_id=run_id, sequence=1, kind="validation_result", label=scenario.title,
            status="passed" if not failures else "failed", detail=json.dumps(report),
        ))
        for index, action in enumerate(result.actions, start=2):
            history.append_event(RunEvent(
                run_id=run_id, sequence=index, kind="tool_completed", label=action.tool_name,
                status="blocked" if action.blocked else "recorded",
                detail=action.result_summary, tool_name=action.tool_name,
            ))
        history.finish_run(run_id, result)

    await asyncio.to_thread(persist_result)
    return report


def validation_results(service) -> dict:
    items = []
    for record in service.history.list_runs(limit=100):
        if record.workload != "validation":
            continue
        for event in service.history.events(record.id):
            if event.kind == "validation_result":
                items.append(json.loads(event.detail))
                break
    return {"items": items}
