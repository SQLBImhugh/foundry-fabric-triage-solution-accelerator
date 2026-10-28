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

import asyncio
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
from uuid import UUID

from triage import __version__
from triage.monitoring.contracts import FixedDiagnosticError

logger = logging.getLogger("triage.observability")
_health_logger = logging.getLogger("triage.telemetry.health")

_agent_name: ContextVar[str] = ContextVar("agent_name", default="")

#: Longest identifier exported on telemetry logs or spans.
MAX_EXPORTED_FIELD = 128
#: Fixed diagnostic code pattern chosen in this repository.
_FIXED_CODE = re.compile(r"\A[a-z][a-z0-9_]{0,63}\Z")
_BUILD_VERSION = re.compile(r"\A[0-9]+\.[0-9]+(?:\.[0-9]+)?(?:(?:a|b|rc|dev|post)[0-9]+)?\Z")
_SAFE_TYPE_NAME = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
_SAFE_SPAN_KEY = re.compile(r"\A[A-Za-z][A-Za-z0-9_.-]{0,127}\Z")
_SAFE_ERROR_CAUSE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]{0,63}(?::[a-z][a-z0-9_]{0,63})?\Z")

SERVICE_NAMES: frozenset[str] = frozenset({
    "controller", "worker", "web", "fixture", "cli", "hosted",
})
WORK_KINDS: frozenset[str] = frozenset({
    "inventory", "capability_probe", "poll", "connector_reconcile",
    "triage", "deferred_retry", "verify_action", "finalize", "reconcile_state",
})
DECISION_MODES: frozenset[str] = frozenset({
    "handoff", "window", "handoff_acknowledgement",
    "pending_window_acknowledgement", "window_acknowledgement",
})
GUARD_CODES: frozenset[str] = frozenset({
    "51070", "51071", "51072", "51073", "51074", "51075", "51076", "51077",
})
CLASSIFIED_ERROR_TYPES: frozenset[str] = frozenset({
    "MonitoringUnavailable", "MonitoringConflict", "MonitoringLeaseLost",
    "MonitoringComponentDenied", "MonitoringNotBootstrapped",
    "MonitoringKernelUnsupported", "MonitoringCommitUncertain",
    "MonitoringSchemaMismatch",
})
ALLOWED_SQL_RPCS: frozenset[str] = frozenset({
    "inspect", "lock_context", "worker.claim_work", "controller.claim_work",
    "worker.transition_work", "controller.transition_work", "worker.accept_facts",
    "worker.observe_connector", "worker.partition", "worker.observe_retention",
    "worker.commit_positions", "worker.advance_checkpoint", "worker.record_heartbeat",
    "worker.rate_budget", "web.commit_intent", "controller.resolve_frontier",
    "controller.publish_connector", "controller.publish_source",
    "controller.disposition_source", "controller.enqueue_work",
    "controller.inspect_frontiers", "controller.reserve_action",
    "controller.transition_action", "controller.finalize", "controller.retire_inventory",
    "schema_check", "control_lock", "control_read", "record_read", "record_write",
    "due_scan", "fair_workspace", "receipt_read", "approval_read", "incident_read",
    "finalization_read", "definition_hash", "now",
})


def build_version() -> str:
    return __version__ if _BUILD_VERSION.fullmatch(__version__) else "unknown"


def validated_service(value: object, *, default: str = "none") -> str:
    return value if isinstance(value, str) and value in SERVICE_NAMES else default


def validated_build(value: object, *, default: str = "unknown") -> str:
    return value if isinstance(value, str) and _BUILD_VERSION.fullmatch(value) else default


def validated_uuid(value: object, *, allow_read: bool = False, default: str = "none") -> str:
    if allow_read and value == "read":
        return "read"
    if not isinstance(value, str) or len(value) != 36:
        return default
    try:
        parsed = UUID(value)
    except ValueError:
        return default
    canonical = str(parsed)
    return canonical if canonical == value.lower() else default


def validated_int_value(value: object) -> int | None:
    if type(value) is int and 0 <= value <= 1_000_000_000_000:
        return value
    return None


def validated_int(value: object, *, default: str = "none") -> str:
    checked = validated_int_value(value)
    return str(checked) if checked is not None else default


def validated_token(
    value: object, allowed: frozenset[str] | set[str], *, default: str = "none",
) -> str:
    return value if isinstance(value, str) and value in allowed else default


def validated_rpc(value: object, *, default: str = "none") -> str:
    return validated_token(value, ALLOWED_SQL_RPCS, default=default)


def validated_type_name(value: object, *, default: str = "none") -> str:
    name = value.__name__ if isinstance(value, type) else value if isinstance(value, str) else ""
    return name if _SAFE_TYPE_NAME.fullmatch(name) else default


def root_cause_code(exc: BaseException) -> str:
    """Return a bounded class name (and fixed diagnostic code when typed).

    Only ``FixedDiagnosticError`` subclasses carry repository-chosen literal
    codes; arbitrary exception ``.code`` attributes can contain server or row
    text and are never exported.
    """
    cause, depth = exc.__cause__, 0
    while cause is not None and depth < 5:
        cls_name = validated_type_name(type(cause).__name__, default="Exception")
        code = getattr(cause, "code", None) if isinstance(cause, FixedDiagnosticError) else None
        if isinstance(code, str) and _FIXED_CODE.match(code):
            return f"{cls_name}:{code}"[:MAX_EXPORTED_FIELD]
        if cause.__cause__ is None:
            return cls_name[:MAX_EXPORTED_FIELD]
        cause, depth = cause.__cause__, depth + 1
    if isinstance(exc, FixedDiagnosticError):
        code = getattr(exc, "code", None)
        if isinstance(code, str) and _FIXED_CODE.match(code):
            cls_name = validated_type_name(type(exc).__name__, default="FixedDiagnosticError")
            return f"{cls_name}:{code}"[:MAX_EXPORTED_FIELD]
    return ""

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


class WorkCorrelation:
    """Thread-safe, strictly validated correlation state for one monitoring work unit."""

    def __init__(self, **initial: object) -> None:
        self._lock = threading.Lock()
        self.service = "none"
        self.build = build_version()
        self.work_id = "none"
        self.work_kind = "none"
        self.request_id = "none"
        self.handoff_id = "none"
        self.revision = "none"
        self.handoff_revision = "none"
        self.fence = "none"
        self.decision_mode = "none"
        self.rpc = "none"
        self.guard_code = "none"
        self.update(**initial)

    def update(self, **fields: object) -> None:
        with self._lock:
            if (val := validated_service(fields.get("service"))) != "none":
                self.service = val
            if "build" in fields and fields["build"] is not None:
                self.build = validated_build(fields["build"])
            if (val := validated_uuid(fields.get("work_id"))) != "none":
                self.work_id = val
            if (val := validated_token(fields.get("work_kind"), WORK_KINDS)) != "none":
                self.work_kind = val
            if (val := validated_uuid(fields.get("request_id"), allow_read=True)) != "none":
                self.request_id = val
            if (val := validated_uuid(fields.get("handoff_id"))) != "none":
                self.handoff_id = val
            if (val := validated_int(fields.get("revision"))) != "none":
                self.revision = val
            if (val := validated_int(fields.get("handoff_revision"))) != "none":
                self.handoff_revision = val
            if (val := validated_int(fields.get("fence"))) != "none":
                self.fence = val
            if (val := validated_token(fields.get("decision_mode"), DECISION_MODES)) != "none":
                self.decision_mode = val
            if (val := validated_rpc(fields.get("rpc"))) != "none":
                self.rpc = val
            if (val := validated_token(fields.get("guard_code"), GUARD_CODES)) != "none":
                self.guard_code = val

    def snapshot(self) -> dict[str, str]:
        with self._lock:
            return {
                "service": self.service,
                "build": self.build,
                "work_id": self.work_id,
                "work_kind": self.work_kind,
                "request_id": self.request_id,
                "handoff_id": self.handoff_id,
                "revision": self.revision,
                "handoff_revision": self.handoff_revision,
                "fence": self.fence,
                "decision_mode": self.decision_mode,
                "rpc": self.rpc,
                "guard_code": self.guard_code,
            }


class HeartbeatCorrelation(WorkCorrelation):
    """Thread-safe correlation and phase timings for one controller heartbeat."""

    def __init__(self, **initial: object) -> None:
        self.context_ms = 0
        self.claim_ms = 0
        self.execute_ms = 0
        super().__init__(service="controller", **initial)

    def add_phase_ms(
        self,
        *,
        context_ms: int | None = None,
        claim_ms: int | None = None,
        execute_ms: int | None = None,
    ) -> None:
        with self._lock:
            if (val := validated_int_value(context_ms)) is not None:
                self.context_ms += val
            if (val := validated_int_value(claim_ms)) is not None:
                self.claim_ms += val
            if (val := validated_int_value(execute_ms)) is not None:
                self.execute_ms += val

    def phase_snapshot(self) -> dict[str, str | int]:
        snap: dict[str, str | int] = dict(self.snapshot())
        with self._lock:
            snap["context_ms"] = self.context_ms
            snap["claim_ms"] = self.claim_ms
            snap["execute_ms"] = self.execute_ms
        return snap


_current_work_correlation: ContextVar[WorkCorrelation | None] = ContextVar(
    "monitoring_work_correlation", default=None,
)
_current_heartbeat_correlation: ContextVar[HeartbeatCorrelation | None] = ContextVar(
    "monitoring_heartbeat_correlation", default=None,
)


def current_work_correlation() -> WorkCorrelation | None:
    return _current_work_correlation.get()


def current_heartbeat_correlation() -> HeartbeatCorrelation | None:
    return _current_heartbeat_correlation.get()


def bind_work_correlation(**fields: object) -> None:
    """Update active work and heartbeat correlation state across threads/tasks."""
    work = _current_work_correlation.get()
    if work is not None:
        work.update(**fields)
    heartbeat = _current_heartbeat_correlation.get()
    if heartbeat is not None:
        heartbeat.update(**fields)


def record_heartbeat_phase(
    *,
    context_ms: int | None = None,
    claim_ms: int | None = None,
    execute_ms: int | None = None,
) -> None:
    heartbeat = _current_heartbeat_correlation.get()
    if heartbeat is not None:
        heartbeat.add_phase_ms(context_ms=context_ms, claim_ms=claim_ms, execute_ms=execute_ms)


def _sanitize_span_attribute(key: str, value: Any) -> str | int | float | bool | None:
    if not isinstance(key, str) or not _SAFE_SPAN_KEY.fullmatch(key):
        return None
    if key in {
        "work_id", "monitoring.work_id", "handoff_id", "monitoring.handoff_id",
        "connector_id", "monitoring.connector_id",
    }:
        return validated_uuid(value)
    if key in {"request_id", "monitoring.request_id"}:
        return validated_uuid(value, allow_read=True)
    if key in {"work_kind", "monitoring.work_kind"}:
        return validated_token(value, WORK_KINDS)
    if key in {"decision_mode", "monitoring.decision_mode"}:
        return validated_token(value, DECISION_MODES)
    if key in {"service", "service.name"}:
        return validated_service(value)
    if key in {"build", "service.version"}:
        return validated_build(value)
    if key in {"guard_code", "sql.guard_code"}:
        return validated_token(value, GUARD_CODES)
    if key in {"classified_error_type", "sql.classified_error_type"}:
        return validated_token(value, CLASSIFIED_ERROR_TYPES)
    if key in {"rpc", "sql.rpc"}:
        return validated_rpc(value)
    if key in {
        "fence", "monitoring.fence", "revision", "monitoring.revision",
        "handoff_revision", "monitoring.handoff_revision",
    }:
        if value == "none":
            return "none"
        checked = validated_int_value(value)
        if checked is not None:
            return str(checked)
        if isinstance(value, str) and value.isdigit() and 0 <= int(value) <= 1_000_000_000_000:
            return value
        return "none"
    if key in {
        "elapsed_ms", "heartbeat.elapsed_ms", "monitoring.elapsed_ms", "sql.elapsed_ms",
        "heartbeat.context_ms", "heartbeat.claim_ms", "heartbeat.execute_ms",
        "heartbeat.automatic_calls", "heartbeat.human_calls",
        "heartbeat.automatic_results", "heartbeat.human_results",
        "gen_ai.usage.prompt_tokens", "gen_ai.usage.completion_tokens",
    }:
        return validated_int_value(value)
    if key == "error.type":
        return "" if value == "" else validated_type_name(value, default="")
    if key == "error.cause":
        if value == "":
            return ""
        if isinstance(value, str) and _SAFE_ERROR_CAUSE.fullmatch(value) and len(value) <= MAX_EXPORTED_FIELD:
            return value
        return ""
    if type(value) is bool:
        return value
    if type(value) is int:
        return value if -1_000_000_000_000 <= value <= 1_000_000_000_000 else None
    if type(value) is float:
        return value
    if isinstance(value, str):
        if len(value) > MAX_EXPORTED_FIELD or any(
            ch.isspace() or ch in {"'", '"'} or ord(ch) < 32 or ord(ch) == 127
            for ch in value
        ):
            return None
        return value
    return None


class _SpanHandle:
    """Thin wrapper so callers use one API whether or not OTel is present."""

    def __init__(self, span: Any = None, *, correlation: WorkCorrelation | None = None):
        self._span = span
        self._correlation = correlation

    def set(self, key: str, value: Any) -> None:
        if value is None:
            return
        if self._correlation is not None:
            short = key.removeprefix("monitoring.").removeprefix("sql.")
            if short == "service.name":
                short = "service"
            elif short == "service.version":
                short = "build"
            if short in {
                "service", "build", "work_id", "work_kind", "request_id", "handoff_id",
                "revision", "handoff_revision", "fence", "decision_mode", "rpc", "guard_code",
            }:
                self._correlation.update(**{short: value})
                bind_work_correlation(**{short: value})
        if self._span is not None:
            checked = _sanitize_span_attribute(key, value)
            if checked is not None:
                try:
                    self._span.set_attribute(key, checked)
                except Exception:  # pragma: no cover
                    pass

    def record_usage(self, prompt_tokens: int = 0, completion_tokens: int = 0) -> None:
        self.set("gen_ai.usage.prompt_tokens", int(prompt_tokens))
        self.set("gen_ai.usage.completion_tokens", int(completion_tokens))

    def record_finish(self, reason: str) -> None:
        self.set("gen_ai.response.finish_reasons", reason)


@contextmanager
def heartbeat_span(**attributes: Any):
    correlation = HeartbeatCorrelation(**attributes)
    token = _current_heartbeat_correlation.set(correlation)
    try:
        if not _OTEL_AVAILABLE or _tracer is None:
            yield _SpanHandle(None, correlation=correlation)
            return
        with _tracer.start_as_current_span(
            "triage.heartbeat", record_exception=False, set_status_on_exception=False,
        ) as span:
            handle = _SpanHandle(span, correlation=correlation)
            for key, value in attributes.items():
                handle.set(key, value)
            yield handle
    finally:
        _current_heartbeat_correlation.reset(token)


@contextmanager
def monitoring_work_span(**attributes: Any):
    """Metadata-only correlation span around one monitoring work unit."""
    existing = _current_work_correlation.get()
    if existing is not None:
        existing.update(**attributes)
        bind_work_correlation(**attributes)
        yield _SpanHandle(None, correlation=existing)
        return
    correlation = WorkCorrelation(**attributes)
    bind_work_correlation(**attributes)
    token = _current_work_correlation.set(correlation)
    began = time.monotonic()
    status, error_type, error_cause = "completed", "", ""
    try:
        if not _OTEL_AVAILABLE or _tracer is None:
            try:
                yield _SpanHandle(None, correlation=correlation)
            except BaseException as exc:
                status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
                error_type = validated_type_name(type(exc).__name__, default="")
                error_cause = root_cause_code(exc)
                raise
            return
        with _tracer.start_as_current_span(
            "triage.monitoring.work", record_exception=False, set_status_on_exception=False,
        ) as span:
            handle = _SpanHandle(span, correlation=correlation)
            try:
                yield handle
            except BaseException as exc:
                status = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
                error_type = validated_type_name(type(exc).__name__, default="")
                error_cause = root_cause_code(exc)
                raise
            finally:
                elapsed_ms = max(0, int((time.monotonic() - began) * 1000))
                snap = correlation.snapshot()
                for key, value in {
                    "service.name": snap["service"],
                    "service.version": snap["build"],
                    "monitoring.status": status,
                    "monitoring.elapsed_ms": elapsed_ms,
                    "monitoring.work_id": snap["work_id"],
                    "monitoring.work_kind": snap["work_kind"],
                    "monitoring.request_id": snap["request_id"],
                    "monitoring.handoff_id": snap["handoff_id"],
                    "monitoring.revision": snap["revision"],
                    "monitoring.handoff_revision": snap["handoff_revision"],
                    "monitoring.fence": snap["fence"],
                    "monitoring.decision_mode": snap["decision_mode"],
                    "sql.rpc": snap["rpc"],
                    "sql.guard_code": snap["guard_code"],
                    "error.type": error_type,
                    "error.cause": error_cause,
                }.items():
                    handle.set(key, value)
    finally:
        _current_work_correlation.reset(token)


@contextmanager
def monitoring_sql_span(operation: str, **attributes: Any):
    """Metadata-only correlation span around one monitoring SQL transaction."""
    if not _OTEL_AVAILABLE or _tracer is None:
        yield _SpanHandle(None)
        return
    with _tracer.start_as_current_span(
        "triage.monitoring.sql", record_exception=False, set_status_on_exception=False,
    ) as span:
        handle = _SpanHandle(span)
        token = "".join(ch for ch in operation if not ch.isspace())[:MAX_EXPORTED_FIELD] or "none"
        handle.set("sql.operation", token)
        for key, value in attributes.items():
            handle.set(key, value)
        yield handle


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
