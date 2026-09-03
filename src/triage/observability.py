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
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

logger = logging.getLogger("triage.observability")

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


def configure_telemetry(connection_string: str = "") -> bool:
    """Wire Azure Monitor if the SDK and a connection string are both present.

    Returns True when telemetry is live. Never raises — a broken telemetry
    config must not take down the agent.
    """
    if not connection_string:
        return False

    # Telemetry export failures must not appear on screen. The Azure Monitor
    # exporter logs a full traceback when it cannot reach the service --
    # including the live-metrics ping, which fires on a timer regardless of
    # whether anything is being traced. On a laptop with flaky wifi, or a demo
    # room with a captive portal, that puts a stack trace in the middle of a
    # scenario run in front of an audience.
    #
    # Silenced rather than lowered: there is nothing an operator can do about a
    # dropped span mid-demo, and the run itself is unaffected.
    for noisy in (
        "azure.monitor.opentelemetry.exporter",
        "azure.monitor.opentelemetry.exporter._quickpulse",
        "azure.core.pipeline.policies.http_logging_policy",
        "opentelemetry.sdk.trace.export",
    ):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)

    try:  # pragma: no cover - requires the azure extra
        from azure.monitor.opentelemetry import configure_azure_monitor

        configure_azure_monitor(connection_string=connection_string)
        logger.info("Azure Monitor telemetry configured")
        return True
    except Exception as exc:  # pragma: no cover
        logger.warning("Telemetry configuration failed, continuing without it: %s", exc)
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

    with _tracer.start_as_current_span(f"gen_ai.{operation}") as span:  # pragma: no cover
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

    with _tracer.start_as_current_span(f"tool.{tool_name}") as span:  # pragma: no cover
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
