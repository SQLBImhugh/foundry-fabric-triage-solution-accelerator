"""Monitoring context adapter for the existing reasoning loop."""

from __future__ import annotations

from dataclasses import dataclass

from triage.agents.triage_agent import EventHook
from triage.agents.triage_agent import TriageAgent as ReasoningAgent
from triage.agents.triage_agent import TriageDeps as ReasoningDeps
from triage.models import BIRequest, TriageResult
from triage.monitoring.controller import MonitoringExecution, bind_execution

__all__ = ["EventHook", "TriageAgent", "TriageDeps"]


@dataclass
class TriageDeps(ReasoningDeps):
    monitoring: MonitoringExecution | None = None


class TriageAgent(ReasoningAgent):
    async def run(self, request: BIRequest, deps: TriageDeps) -> TriageResult:
        with bind_execution(deps.monitoring):
            result = await super().run(request, deps)
        if deps.monitoring is not None and deps.monitoring.persistence_error is not None:
            raise deps.monitoring.persistence_error
        return result
