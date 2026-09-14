"""Deliver operator notifications to the durable command-center run timeline."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from triage.tools.teams import ResolutionSummary


class CommandCenterNotifier:
    def __init__(self, emit: Callable[[str, dict[str, Any]], None]) -> None:
        self._emit = emit

    async def post(self, summary: ResolutionSummary) -> dict[str, Any]:
        # emit must raise if persistence fails. Delivered means recorded in the
        # operator inbox, not read by a person.
        self._emit("notification", {
            "label": summary.title,
            "status": "recorded",
            "detail": summary.to_markdown(),
        })
        return {"delivered": True, "transport": "command_center", "status": "recorded"}
