"""OpenTelemetry GenAI spans with a no-op fallback.

Follows the OTel GenAI semantic conventions so App Insights renders the spans
without custom mapping. If ``opentelemetry`` isn't installed, or no connection
string is configured, every helper degrades to a no-op — the demo must run
offline with zero telemetry deps.

**Metadata only.** Prompts and completions are never attached. In a
multi-tenant system that would ingest customer data and secrets into a
telemetry store with different access controls than the source system.
"""

from __future__ import annotations

import functools
import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

logger = logging.getLogger("triage.observability")
_health_logger = logging.getLogger("triage.telemetry.health")

_agent_name: ContextVar[str] = ContextVar("agent_name", default="")

# provider -> gen_ai.system. Deterministic mapping, no auto-instrumentation
# guesswork about which SDK produced the call.
_PROVIDER_GEN_AI_SYSTEM: dict[str, str] = {
    "azure": "az.ai.openai",
    "direct": "az.ai.openai",
    "foundry": "az.ai.agents",
    "mock": "mock.scripted",
}

try:  # pragma: no cover - import guard
    from opentelemetry import trace as _otel_trace

    _tracer = _otel_trace.get_tracer("triage")
    _OTEL_AVAILABLE = True
except Exception:  # pragma: no cover - the offline path
    _tracer = None
    _OTEL_AVAILABLE = False


def otel_available() -> bool:
    return _OTEL_AVAILABLE


class _TelemetryDiagnostics(logging.Handler):
    """Count SDK diagnostics without forwarding response text or exception content."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.state_lock = threading.Lock()
        self.configuration = "unconfigured"
        self.export_failures = 0
        self.export_warnings = 0
        self.last_error_type = ""
        self.configuration_error_source: tuple[str, str, int] = ("", "", 0)
        self.console_enabled = False
        self.last_report: float | None = None

    def emit(self, record: logging.LogRecord) -> None:
        with self.state_lock:
            if record.levelno >= logging.ERROR:
                self.export_failures += 1
            else:
                self.export_warnings += 1
            self.last_error_type = (
                type(record.exc_info[1]).__name__[:64] if record.exc_info and record.exc_info[1]
                else "ExporterDiagnostic"
            )
            now = time.monotonic()
            if not self.console_enabled or self.last_report is not None and now - self.last_report < 60:
                return
            self.last_report = now
            failures, warnings, error_type = self.export_failures, self.export_warnings, self.last_error_type
        # This logger never propagates to the exporter, so a failure cannot
        # generate an export/failure feedback loop.
        _health_logger.warning(
            "telemetry_export_diagnostic failures=%d warnings=%d error_type=%s ingestion=unverified",
            failures, warnings, error_type,
        )

    def snapshot(self) -> dict[str, str | int]:
        with self.state_lock:
            return {
                "configuration": self.configuration, "export_failures": self.export_failures,
                "export_warnings": self.export_warnings, "last_error_type": self.last_error_type,
                "ingestion": "unverified",
                "configuration_error_file": self.configuration_error_source[0],
                "configuration_error_function": self.configuration_error_source[1],
                "configuration_error_line": self.configuration_error_source[2],
            }


_diagnostics = _TelemetryDiagnostics()


def telemetry_status() -> dict[str, str | int]:
    """Configuration and observed export diagnostics, never an ingestion receipt."""
    return _diagnostics.snapshot()


def _configure_diagnostics(hosted: bool) -> None:
    _diagnostics.console_enabled = hosted
    _health_logger.propagate = False
    _health_logger.setLevel(logging.WARNING)
    if not _health_logger.handlers:
        _health_logger.addHandler(logging.StreamHandler())
    for name in ("azure.monitor.opentelemetry", "opentelemetry.sdk"):
        sdk_logger = logging.getLogger(name)
        sdk_logger.setLevel(logging.WARNING)
        sdk_logger.propagate = False
        if _diagnostics not in sdk_logger.handlers:
            sdk_logger.addHandler(_diagnostics)


def _configuration_error_source(exc: BaseException) -> tuple[str, str, int]:
    frame = exc.__traceback__
    if frame is None:
        return "unavailable", "unavailable", 0
    while frame.tb_next is not None:
        frame = frame.tb_next
    # Read code metadata only: no exception arguments, source lines or locals.
    file = Path(frame.tb_frame.f_code.co_filename).name
    function = frame.tb_frame.f_code.co_name
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", file):
        file = "unavailable"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}|<(?:module|lambda|listcomp|dictcomp|setcomp|genexpr)>", function):
        function = "unavailable"
    return file, function, frame.tb_lineno


def configure_telemetry(
    connection_string: str = "", *, hosted: bool = False, managed_identity_client_id: str = "",
) -> bool:
    """Return whether SDK configuration succeeded, not whether data was ingested.

    Hosted export uses managed identity only. Diagnostics contain counts,
    exception types and one sanitized source location, never connection strings,
    SDK responses or stack traces. Failure does not make the controller unavailable.
    """
    with _diagnostics.state_lock:
        _diagnostics.configuration_error_source = ("", "", 0)
    if not connection_string:
        _diagnostics.configuration = "disabled"
        return False
    _configure_diagnostics(hosted)
    try:
        if hosted and os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") == "":
            # Foundry injects an empty value when project tracing is off. The
            # exporter parses it even when the app supplies an explicit locator.
            os.environ.pop("APPLICATIONINSIGHTS_CONNECTION_STRING")
            logger.info("Empty platform telemetry locator normalized")
        from azure.monitor.opentelemetry import configure_azure_monitor

        options: dict[str, Any] = {
            "connection_string": connection_string,
            # Only this metadata-only logger family is exported, not ordinary
            # controller logs that may contain incident text or exceptions.
            "logger_name": "triage.telemetry",
            "enable_live_metrics": False,
            "enable_performance_counters": False,
            "disable_offline_storage": True,
            "logging_enabled": False,
            "instrumentation_options": {
                name: {"enabled": False} for name in (
                    "azure_sdk", "django", "fastapi", "flask", "psycopg2", "requests", "urllib", "urllib3",
                )
            },
        }
        if hosted:
            from azure.identity import ManagedIdentityCredential

            options["credential"] = ManagedIdentityCredential(
                client_id=managed_identity_client_id or None,
            )
        configure_azure_monitor(**options)
        _diagnostics.configuration = "configured"
        logger.info("Azure Monitor SDK configured ingestion=unverified")
        return True
    except Exception as exc:
        source = _configuration_error_source(exc)
        with _diagnostics.state_lock:
            _diagnostics.configuration = "failed"
            _diagnostics.configuration_error_source = source
        logger.warning(
            "Telemetry configuration failed error_type=%s source_file=%s source_function=%s "
            "source_line=%d ingestion=unverified; controller continues",
            type(exc).__name__, *source,
        )
        return False


@contextmanager
def agent_context(name: str):
    """Tag everything inside this block as belonging to one named agent."""
    token = _agent_name.set(name)
    try:
        yield
    finally:
        _agent_name.reset(token)


def current_agent() -> str:
    return _agent_name.get()


def with_agent_context(name: str):
    """Decorator form of :func:`agent_context`, async-aware."""

    def decorator(fn):
        if _is_coroutine(fn):

            @functools.wraps(fn)
            async def _async_wrapper(*args, **kwargs):
                with agent_context(name):
                    return await fn(*args, **kwargs)

            return _async_wrapper

        @functools.wraps(fn)
        def _sync_wrapper(*args, **kwargs):
            with agent_context(name):
                return fn(*args, **kwargs)

        return _sync_wrapper

    return decorator


def _is_coroutine(fn) -> bool:
    import inspect

    return inspect.iscoroutinefunction(fn)


class _SpanHandle:
    """Thin wrapper so callers use one API whether or not OTel is present."""

    def __init__(self, span: Any = None):
        self._span = span

    def set(self, key: str, value: Any) -> None:
        if self._span is not None and value is not None:
            try:
                self._span.set_attribute(key, value)
            except Exception:  # pragma: no cover
                pass

    def record_usage(self, prompt_tokens: int = 0, completion_tokens: int = 0) -> None:
        self.set("gen_ai.usage.prompt_tokens", int(prompt_tokens))
        self.set("gen_ai.usage.completion_tokens", int(completion_tokens))

    def record_finish(self, reason: str) -> None:
        self.set("gen_ai.response.finish_reasons", reason)


@contextmanager
def heartbeat_span():
    if not _OTEL_AVAILABLE or _tracer is None:
        yield _SpanHandle()
        return
    with _tracer.start_as_current_span(
        "triage.heartbeat", record_exception=False, set_status_on_exception=False,
    ) as span:
        yield _SpanHandle(span)


@contextmanager
def gen_ai_span(
    *,
    provider: str,
    model: str,
    operation: str = "chat",
    agent_name: str | None = None,
    **attributes: Any,
):
    """Open a ``gen_ai.chat`` span for one LLM call."""
    name = agent_name or current_agent() or "unknown"

    if not _OTEL_AVAILABLE or _tracer is None:
        yield _SpanHandle(None)
        return

    with _tracer.start_as_current_span(
        f"gen_ai.{operation}", record_exception=False, set_status_on_exception=False,
    ) as span:  # pragma: no cover
        handle = _SpanHandle(span)
        handle.set("gen_ai.system", _PROVIDER_GEN_AI_SYSTEM.get(provider, provider))
        handle.set("gen_ai.request.model", model)
        handle.set("gen_ai.operation.name", operation)
        handle.set("agent.name", name)
        for key, value in attributes.items():
            handle.set(key, value)
        try:
            yield handle
        except Exception as exc:
            handle.set("error.type", type(exc).__name__)
            raise


@contextmanager
def tool_span(tool_name: str, **attributes: Any):
    """Span around one tool execution — this is what makes a handoff visible."""
    if not _OTEL_AVAILABLE or _tracer is None:
        yield _SpanHandle(None)
        return

    with _tracer.start_as_current_span(
        f"tool.{tool_name}", record_exception=False, set_status_on_exception=False,
    ) as span:  # pragma: no cover
        handle = _SpanHandle(span)
        handle.set("tool.name", tool_name)
        handle.set("agent.name", current_agent() or "unknown")
        for key, value in attributes.items():
            handle.set(key, value)
        try:
            yield handle
        except Exception as exc:
            handle.set("error.type", type(exc).__name__)
            raise
