"""Hosted entry point for the BI triage controller.

This is the same controller the CLI runs, wrapped in the HTTP contract Foundry
expects from a hosted agent. Deploying it changes *where* the loop runs, not
what it does -- the policy ledger, approval gate, deterministic scans and
incident dedup are all the existing code paths.

Interactive alerts and explicit mailbox, silent-health, pipeline and queued
command sweeps reach the same controller. Hosting changes the process lifetime
and invocation contract, not the policy rules.

Service identities and permissions belong to the component making each call.
Power BI, Fabric and SQL integrations can use managed identity; the current
mailbox adapter still uses its separately configured Graph application
credentials. Hosting does not remove that credential dependency or prove
that all operations use one identity.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections.abc import AsyncIterator, Sequence
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from agent_framework import (
    AgentResponse,
    AgentResponseUpdate,
    BaseAgent,
    Message,
)
from agent_framework._agents import ResponseStream
from agent_framework_foundry_hosting import ResponsesHostServer

from triage.monitoring.controller import HEARTBEAT_BUDGET_SECONDS, controller_heartbeat
from triage.observability import configure_telemetry, telemetry_status
from triage.runner import TriageRunner
from triage.settings import settings
from triage.store.claims import build_claim_store
from triage.tools.inbox import BIRequest, mailbox_scope_refusal, parse_hints

logging.basicConfig(
    level=os.getenv("TRIAGE_LOG_LEVEL", "INFO"),
    format="%(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("triage.hosted")

REPO_ROOT = Path(__file__).resolve().parent.parent

# A scheduled run says this. Anything else is treated as an alert to triage.
# Deliberately an explicit sentinel rather than a heuristic: the first version
# inferred "no alert" from a short message, and the host turned out to pass
# conversation history, so a five-character sweep request arrived as several
# hundred characters of a previous alert and got re-triaged.
_SWEEP_COMMANDS = frozenset({"sweep", "scheduled sweep", "run", "check mail", ""})
#: The second sentinel. A different trigger from the mailbox sweep because
#: there is nothing to react to -- these failures never announce themselves, so
#: the only way to find them is to go and measure.
_SILENT_COMMANDS = frozenset({"silent sweep", "silent-sweep", "health sweep", "scan"})
_PIPELINE_COMMANDS = frozenset({"pipeline sweep", "pipeline-sweep"})
_WEB_COMMANDS = frozenset({"command sweep", "command-sweep"})
_HEARTBEAT_COMMANDS = frozenset({"heartbeat", "monitoring sweep", "monitoring-sweep"})


def _latest_text(messages: Any) -> str:
    """Return the most recent inbound message, not the whole conversation.

    The host may pass prior turns. Concatenating them makes every request look
    like the last alert we saw, which silently re-triages stale work.
    """
    if messages is None:
        return ""
    if isinstance(messages, str):
        return messages.strip()

    items = list(messages) if isinstance(messages, (list, tuple)) else [messages]
    if not items:
        return ""

    # Prefer the last user-authored message; fall back to the last of anything.
    def _role(item: Any) -> str:
        return str(getattr(item, "role", "") or "").lower()

    user_items = [i for i in items if _role(i) in ("user", "")]
    candidate = (user_items or items)[-1]
    return _text_of(candidate)


def _text_of(messages: Any) -> str:
    """Flatten whatever the host handed us into plain text."""
    if messages is None:
        return ""
    if isinstance(messages, str):
        return messages
    items = messages if isinstance(messages, (list, tuple)) else [messages]

    parts: list[str] = []
    for item in items:
        if isinstance(item, str):
            parts.append(item)
            continue
        text = getattr(item, "text", None)
        if isinstance(text, str) and text:
            parts.append(text)
            continue
        contents = getattr(item, "contents", None) or []
        for content in contents:
            inner = getattr(content, "text", None)
            if isinstance(inner, str) and inner:
                parts.append(inner)
    return "\n".join(p for p in parts if p).strip()


class TriageControllerAgent(BaseAgent):
    """The orchestration loop, exposed as a hosted Foundry agent."""

    def __init__(self) -> None:
        super().__init__(
            name="bi-triage-controller",
            description=(
                "Triages Power BI refresh failures: gathers deterministic evidence, "
                "consults the data quality agent, and remediates within policy."
            ),
        )
        self._runner = TriageRunner(settings, base_dir=REPO_ROOT)
        # Process-local, so it serialises work inside one container and
        # nothing more. A hosted agent is constructed fresh per request, so
        # this does not even span two requests to the same replica.
        self._lock = asyncio.Lock()
        # The one that actually prevents duplicate remediation. Durable when a
        # Azure SQL database is configured; in-process, and therefore honest
        # about what it can guarantee, when it is not. It shares the runner's
        # connection rather than opening a second one.
        self._claims = build_claim_store(
            db=self._runner.sql,
            table=settings.claim_table_name,
        )
        if not getattr(self._claims, "is_durable", False):
            logger.warning(
                "Claim store is not durable: set AZURE_SQL_SERVER and "
                "AZURE_SQL_DATABASE. Two concurrent invocations could triage "
                "the same alert twice."
            )

    def run(  # type: ignore[override]
        self,
        messages: Any = None,
        *,
        stream: bool = False,
        session: Any = None,
        **kwargs: Any,
    ) -> Any:
        """Entry point for both streaming and non-streaming callers.

        Deliberately *not* an ``async def``. The host calls this with
        ``stream=True`` and immediately iterates the result, so an async
        function -- which returns a coroutine -- fails with
        "'coroutine' object has no attribute '__anext__'". The contract is a
        sync method returning either an awaitable or an async iterable.

        Triage is not meaningfully incremental: it runs tools and returns a
        verdict. So the streaming path emits the finished summary as a single
        update rather than pretending to produce tokens.
        """
        if stream:
            return ResponseStream(self._stream(messages), finalizer=self._finalize)
        return self._run_once(messages)

    async def _stream(self, messages: Any) -> AsyncIterator[AgentResponseUpdate]:
        response = await self._run_once(messages)
        text = response.messages[0].text if response.messages else ""
        yield AgentResponseUpdate(
            role="assistant", contents=[{"type": "text", "text": text}]
        )

    @staticmethod
    def _finalize(updates: Sequence[AgentResponseUpdate]) -> AgentResponse[Any]:
        text = "".join(
            content.text
            for update in updates
            for content in (update.contents or [])
            if getattr(content, "text", None)
        )
        return AgentResponse(messages=[Message("assistant", [text])])

    async def _run_once(self, messages: Any) -> AgentResponse[Any]:
        started_at = time.monotonic()
        text = _latest_text(messages)
        command = text.strip().lower()
        is_sweep = command in _SWEEP_COMMANDS
        is_silent = command in _SILENT_COMMANDS
        is_pipeline = command in _PIPELINE_COMMANDS
        if command in _HEARTBEAT_COMMANDS:
            remaining = max(0.0, HEARTBEAT_BUDGET_SECONDS - (time.monotonic() - started_at))
            try:
                # Time out this waiter only, never the lock holder or admitted work.
                await asyncio.wait_for(self._lock.acquire(), timeout=remaining)
            except TimeoutError:
                logging.getLogger("triage.telemetry.heartbeat").warning(
                    "heartbeat_deferred reason=lock_budget_exhausted elapsed_ms=%d "
                    "automatic_calls=0 human_calls=0 budget_exhausted=True ingestion=unverified",
                    max(0, int((time.monotonic() - started_at) * 1000)),
                )
                return AgentResponse(messages=[Message("assistant", [
                    "Heartbeat deferred: admission budget exhausted while waiting for the controller lock. "
                    "No new work was claimed; queued work remains pending.",
                ])])
            try:
                summary = await self._heartbeat(started_at=started_at)
                return AgentResponse(messages=[Message("assistant", [summary])])
            finally:
                self._lock.release()
        if command in _WEB_COMMANDS:
            from triage.command_center.worker import drain_commands

            async with self._lock:
                lines = await drain_commands(self._runner)
                summary = "\n".join(lines) if lines else "No operator command executed; inspect pending or blocked work in the command center."
                return AgentResponse(messages=[Message("assistant", [summary])])

        if is_pipeline:
            # Propagate monitor/configuration faults to the host instead of
            # returning a success-shaped agent message to the scheduler.
            async with self._lock:
                report = await self._runner.pipeline_sweep()
                if report.status in {"incomplete", "unconfigured"}:
                    raise RuntimeError(report.summary())
                return AgentResponse(messages=[Message("assistant", [report.summary()])])

        # Order work inside this invocation. That is all this does: a hosted
        # agent is constructed fresh per request -- eight hours of telemetry
        # recorded 67 distinct role instances for 64 heartbeats -- so this lock
        # spans nothing beyond the call it is taken in. Duplicate remediation is
        # prevented by the durable per-alert claim taken in _drain_mailbox, on
        # the shared database, or not at all. Do not read this line as the
        # protection; tests/test_claims.py proves where the protection lives.
        async with self._lock:
            if is_silent:
                summary = await self._silent_sweep()
            elif is_sweep:
                summary = await self._drain_mailbox()
            else:
                summary = await self._triage_text(text)

        return AgentResponse(messages=[Message("assistant", [summary])])

    # --- the two entry paths ----------------------------------------------

    async def _heartbeat(self, *, started_at: float | None = None) -> str:
        """Give automatic source work and human commands one slot per round."""
        lines = await controller_heartbeat(self._runner, started_at=started_at)
        return "\n".join(lines) if lines else "No due controller or human command work; monitoring coverage is reported separately."

    async def _triage_text(self, text: str) -> str:
        """Unbound human text is diagnostic input, never native action authority."""
        subject, _, body = text.partition("\n")
        hints = parse_hints(subject, text)
        request = BIRequest(
            request_id=f"interactive-{hashlib.sha256(text.encode()).hexdigest()[:24]}",
            received_at="",
            sender="playground",
            subject=subject.strip() or "Interactive alert",
            body=body.strip() or text,
            report_name=hints["report_name"],
            dataset_id=hints["dataset_id"],
            workspace_id=hints["workspace_id"],
            error_code=hints["error_code"],
            source="interactive",
        )
        artifacts = await self._runner.run_request(request)
        return _summarise(artifacts)

    async def _silent_sweep(self) -> str:
        """Go looking for failures that never sent an alert.

        No mailbox involved. This is the path for the case the alert-driven
        design cannot see: a refresh that reported success while the data did
        not arrive.
        """
        lines = await self._runner.silent_sweep()
        if not lines:
            return (
                "No silent failures found. Every configured probe matched its "
                "baseline, or none are configured."
            )
        return f"Silent sweep found {len(lines)} thing(s) worth saying:\n" + "\n".join(lines)

    async def _drain_mailbox(self) -> str:
        """Triage everything new in the alerts mailbox."""
        inbox = self._runner.build_inbox()

        # Work the agent already agreed to do, whose window has now passed.
        # Done first: a retry that succeeds closes its incident, so a fresh
        # alert for the same signature arriving in this sweep is judged against
        # the current state rather than a stale open one.
        retried = await self._runner.drain_due_retries(claims=self._claims)

        # Say which identity we are actually presenting. A container can hold a
        # valid token for the wrong principal, which surfaces as a 401 and
        # looks like a missing permission rather than an identity mix-up.
        verify = getattr(inbox, "verify", None)
        if verify is not None:
            auth = await verify()
            logger.info(
                "Graph auth: ok=%s app_id=%s object_id=%s roles=%s has_upn=%s",
                auth.get("ok"),
                auth.get("app_id", ""),
                auth.get("object_id", ""),
                ",".join(auth.get("roles") or []) or "(none)",
                auth.get("has_upn"),
            )
            if auth.get("has_upn"):
                return (
                    "Refusing to read mail: this is a delegated user token, not "
                    "the agent's own identity."
                )

        # Fail closed, and that means all three ways this can be unproven: no
        # canary configured (the shipped default), a check that did not
        # complete, and a check that proved the agent can read it.
        #
        # The decision lives in triage.tools.inbox so it can be tested without
        # the hosting library. This used to refuse only the third case, while
        # the comment here and the documentation both said it failed closed.
        verify_scope = getattr(inbox, "verify_scope", None)
        if verify_scope is not None:
            scope = (
                await verify_scope(settings.graph_canary_mailbox)
                if settings.graph_canary_mailbox
                else {}
            )
            refusal = mailbox_scope_refusal(
                scope=scope,
                canary_mailbox=settings.graph_canary_mailbox,
                mailbox=settings.graph_mailbox,
            )
            if refusal:
                return refusal

        requests = await inbox.fetch(limit=10)
        if not requests:
            if retried:
                return "No new alerts. Deferred retries performed:\n" + "\n".join(retried)
            return "No new alerts."

        lines: list[str] = []
        contended = 0
        for request in requests:
            # Take a distributed claim before doing anything with real effect.
            #
            # `seen()` was checked when the message was read and `mark_processed`
            # happens after the outcome is persisted, so between those two points
            # a second invocation sees the same message as untriaged. A manual
            # invoke overlapping a scheduled sweep, or two hosted replicas, would
            # both trigger the same refresh. The write-action budget does not
            # help: it is per run, and these are two runs.
            context = self._runner.monitoring_context
            claim_key = f"message:{context.tenant_id}:{context.epoch}:{request.request_id}"
            if not self._claims.claim(claim_key):
                # Someone else has it. Skipping is right: they will mark it
                # processed, and if they die their lease expires and the next
                # sweep picks it up.
                contended += 1
                logger.info("Skipping %s: claimed by another invocation", request.request_id)
                continue

            try:
                artifacts = await self._runner.run_request(request)
                # Only after the outcome is persisted. A crash before this point
                # means the alert is triaged again next sweep, which is safe; a
                # crash after marking would lose it silently.
                inbox.mark_processed(request.request_id, received_at=request.received_at)
                lines.append(f"- {request.subject or '(no subject)'}: {_summarise(artifacts)}")
            finally:
                # Released whatever happened. The message is marked processed on
                # success, so releasing cannot cause a re-run; on failure it lets
                # the next sweep retry immediately instead of waiting out the
                # lease.
                self._claims.release(claim_key)

        if not lines:
            base = "No new alerts."
            if contended:
                base = f"No alerts triaged; {contended} claimed by another invocation."
            if retried:
                return base + " Deferred retries performed:\n" + "\n".join(retried)
            return base

        summary = f"Triaged {len(lines)} alert(s).\n" + "\n".join(lines)
        if contended:
            summary += f"\nSkipped {contended} claimed by another invocation."
        if retried:
            summary += "\nDeferred retries performed:\n" + "\n".join(retried)
        return summary


def _summarise(artifacts: Any) -> str:
    """One line a human can act on, not a dump of the run."""
    result = getattr(artifacts, "result", None)
    if result is None:
        return "completed"

    parts = [f"outcome={getattr(result, 'outcome', 'unknown')}"]
    actions = getattr(result, "actions_taken", None)
    if actions:
        parts.append(f"actions={', '.join(str(a) for a in actions)}")
    summary = getattr(result, "summary", "")
    if summary:
        parts.append(str(summary))
    return " | ".join(parts)


def _startup_telemetry_metadata(configured: bool) -> dict[str, Any]:
    exporters = {}
    allowed = {"none", "console", "otlp", "otlp_proto_http", "otlp_proto_grpc", "azuremonitor"}
    for name in ("OTEL_LOGS_EXPORTER", "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER"):
        value = os.environ.get(name)
        exporters[name] = (
            "unset" if value is None else value if value == "" or (
                len(value) <= 128 and all(part.strip().lower() in allowed for part in value.split(","))
            ) else "unrecognized"
        )
    providers = {"traces": "unavailable", "metrics": "unavailable", "logs": "unavailable"}
    try:
        from opentelemetry import metrics, trace
        from opentelemetry._logs import get_logger_provider

        for signal, provider in (
            ("traces", trace.get_tracer_provider()),
            ("metrics", metrics.get_meter_provider()),
            ("logs", get_logger_provider()),
        ):
            name = type(provider).__name__
            providers[signal] = name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name) else "unrecognized"
    except Exception as exc:
        logger.warning("Telemetry provider inspection unavailable error_type=%s", type(exc).__name__)
    versions = {}
    for package in (
        "agent-framework-foundry-hosting", "agent-framework-core",
        "azure-ai-agentserver-core", "azure-ai-agentserver-responses",
        "microsoft-opentelemetry", "azure-monitor-opentelemetry",
        "azure-monitor-opentelemetry-exporter", "opentelemetry-sdk",
    ):
        try:
            value = version(package)
            versions[package] = value if re.fullmatch(r"[0-9][0-9A-Za-z.+!_-]{0,63}", value) else "unrecognized"
        except PackageNotFoundError:
            versions[package] = "not_installed"
    state = telemetry_status()["configuration"]
    return {
        "configuration": state if state in {"unconfigured", "disabled", "configured", "failed"} else "unrecognized",
        "sdk_configured": configured, "ingestion": "unverified",
        "exporter_settings": exporters, "provider_classes": providers, "package_versions": versions,
    }


def main() -> None:
    # Core reads this flag outside its optional observability callback.
    # Metadata-only capture is an invariant, even with contradictory environment settings.
    os.environ["OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"] = "false"
    # Hosted startup needs its own configuration, not only the CLI's setup.
    # Foundry reserves the standard variable for project-wide content tracing;
    # this application-owned metadata channel has no fallback to that setting.
    configured = configure_telemetry(
        settings.triage_telemetry_connection_string, hosted=True,
        managed_identity_client_id=settings.azure_client_id,
    )
    health = telemetry_status()
    logging.getLogger("triage.telemetry.hosted").log(
        logging.INFO if configured else logging.WARNING,
        "hosted_telemetry configuration=%s sdk_configured=%s ingestion=unverified",
        health["configuration"], configured,
    )

    agent = TriageControllerAgent()
    logger.info(
        "Starting hosted triage controller (provider=%s, tools=%s, mailbox_configured=%s)",
        settings.triage_provider_mode,
        settings.triage_tool_mode,
        bool(settings.graph_mailbox),
    )
    # history_source='agent' because this is a custom SupportsAgentRun
    # implementation, not a RawAgent. The hosting library's default changed to
    # 'agent_server', which refuses at construction: "requires a RawAgent so
    # hosting can enforce downstream storage options".
    #
    # That default arrived through an unpinned dependency and took the deployed
    # container down at startup -- the agent answered nothing for hours, and
    # only a manual invocation found it. The version is pinned in
    # requirements.txt now for the same reason.
    # Keep one metadata-only pipeline; the host's default configures providers
    # again and may enable sensitive Agent Framework instrumentation.
    host = ResponsesHostServer(agent, history_source="agent", configure_observability=None)
    logging.getLogger("triage.telemetry.hosted").log(
        logging.INFO if configured else logging.WARNING,
        "hosted_telemetry_startup %s", json.dumps(_startup_telemetry_metadata(configured), sort_keys=True),
    )
    host.run()


if __name__ == "__main__":
    main()
