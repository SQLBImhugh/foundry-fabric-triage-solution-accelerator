"""Exported diagnostics carry fixed identifiers, never arbitrary payload text.

Only the ``triage.telemetry`` family reaches Azure Monitor, so whatever this
boundary formats leaves the tenant. Redaction removes credential shapes, not
customer data: a SQL conversion or truncation error quotes the row that failed,
and an independent review captured a synthetic row value surviving redaction
into the exported message. Truncating to 400 characters does not make row text
metadata.

The same review found ``_root_cause`` copying any non-empty ``code`` attribute
from any exception class, including an 833-character value from a driver error.

Spans are covered by the same rule (contributor rule 9): metadata only.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from triage import observability
from triage.monitoring.contracts import (
    FixedDiagnosticError,
    MonitoringConflict,
    MonitoringUnavailable,
)
from triage.monitoring.controller import (
    _root_cause,
    controller_heartbeat,
)
from triage.monitoring.memory import EVIDENCE_REJECTION_REASONS, telemetry_logger
from triage.monitoring.models import LeaseToken, MonitoringWork
from triage.monitoring.sql_store import SqlBackend, _exported_failure_fields
from triage.runner import TriageRunner
from triage.settings import Settings

CANARY = "synthetic-row-canary-value"
CONNECTOR_ID = "250d1cca-de6a-550d-a7c2-aa625f40c86e"
TENANT_ID = "11111111-1111-4111-8111-111111111111"
EPOCH_ID = "22222222-2222-4222-8222-222222222222"
WORK_ID = "8440eccf-46b2-4c5d-8472-47dbe07a56da"
REQUEST_ID = "17eaaf46-1a0f-5913-9995-5dda4e6de26b"
HANDOFF_ID = "c13b2ce9-83f4-4f16-934e-7525358e499b"
OWNER_ID = "33333333-3333-4333-8333-333333333333"


class DriverLikeError(Exception):
    """A driver error that happens to expose a ``code`` attribute."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


def _wrapped(inner: BaseException) -> BaseException:
    try:
        try:
            raise inner
        except BaseException as exc:
            raise MonitoringUnavailable("shared state was not replaced") from exc
    except BaseException as outer:
        return outer


# --- exported SQL failure fields -------------------------------------------


def test_quoted_row_content_never_reaches_the_exported_fields() -> None:
    exc = DriverLikeError(
        "String or binary data would be truncated. "
        f"Truncated value: '{CANARY}'.",
    )

    fields = _exported_failure_fields("reconcile_work", exc)

    assert CANARY not in " ".join(f"{key}={value}" for key, value in fields.items())
    assert fields["error_type"] == "DriverLikeError"
    assert fields["guard_code"] == "none"


def test_a_recognized_guard_is_exported_as_its_fixed_code() -> None:
    exc = DriverLikeError(
        "Driver Error: Syntax error or access violation; DDBC Error: "
        "[Microsoft][SQL Server]A terminal handoff decision cannot be changed",
    )

    fields = _exported_failure_fields("reconcile_work", exc)

    assert fields["guard_code"] == "51072"
    # The guard's own wording is a fixed literal, but it is still not exported:
    # only the code an operator can act on.
    assert "terminal handoff decision" not in str(fields)


def test_exported_fields_are_bounded_and_free_of_separators() -> None:
    exc = DriverLikeError("x" * 5000)

    fields = _exported_failure_fields("a b\nc" * 400, exc)

    for key, value in fields.items():
        assert len(str(value)) <= 128, key
        assert "\n" not in str(value) and " " not in str(value), key


# --- root cause -------------------------------------------------------------


def test_a_fixed_diagnostic_code_is_still_reported() -> None:
    class Review(FixedDiagnosticError):
        pass

    assert _root_cause(_wrapped(Review("supersession_presence_inspection_expired"))) == (
        "Review:supersession_presence_inspection_expired"
    )


def test_an_unknown_exception_code_is_not_exported() -> None:
    cause = _root_cause(_wrapped(DriverLikeError("boom", code=f"value: '{CANARY}'")))

    assert CANARY not in cause
    assert cause == "DriverLikeError"


def test_an_oversized_or_malformed_fixed_code_is_refused() -> None:
    class Review(FixedDiagnosticError):
        pass

    assert _root_cause(_wrapped(Review("a" * 200))) == "Review"
    assert _root_cause(_wrapped(Review(f"quoted '{CANARY}'"))) == "Review"


def test_root_cause_remains_bounded() -> None:
    class Review(FixedDiagnosticError):
        pass

    assert len(_root_cause(_wrapped(Review("a" * 200)))) <= 128


# --- the exported logger family --------------------------------------------


@pytest.mark.parametrize("name", ["triage.telemetry", "triage.telemetry.sql"])
def test_nothing_in_the_exported_family_emits_row_content(
    name: str, caplog: pytest.LogCaptureFixture,
) -> None:
    exc = DriverLikeError(f"Truncated value: '{CANARY}'.")
    fields = _exported_failure_fields("reconcile_work", exc)

    with caplog.at_level(logging.ERROR, logger=name):
        logging.getLogger(name).error(
            "monitoring_sql_failed %s",
            " ".join(f"{key}={value}" for key, value in fields.items()),
        )

    assert CANARY not in caplog.text


# --- connector evidence rejection ------------------------------------------


def test_connector_evidence_rejection_is_exported_with_fixed_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fix for a fenced connector has to be visible, or it cannot be trusted.

    This decision used to be reported as MonitoringUnavailable. Without an
    exported line an operator cannot tell a resolved rejection from the outage
    it used to look like, because triage.monitoring.memory is not exported.
    """
    with caplog.at_level(logging.INFO, logger="triage.telemetry.monitoring"):
        telemetry_logger.info(
            "connector_evidence_unusable connector_id=%s reason=%s",
            CONNECTOR_ID, "inspection_expired_during_preparation",
        )

    assert "connector_id=" + CONNECTOR_ID in caplog.text
    assert "reason=inspection_expired_during_preparation" in caplog.text
    assert len(caplog.records) == 1


def test_every_rejection_reason_is_a_fixed_literal() -> None:
    """Only values from the closed set may be exported."""
    assert EVIDENCE_REJECTION_REASONS == {
        "overtaken", "inspection_expired", "inspection_expired_during_preparation",
    }
    assert all(
        reason.replace("_", "").isalnum() and reason.islower() and len(reason) <= 64
        for reason in EVIDENCE_REJECTION_REASONS
    )


def test_the_exported_family_covers_the_monitoring_logger() -> None:
    """configure_azure_monitor exports the triage.telemetry family by prefix."""
    assert telemetry_logger.name.startswith("triage.telemetry.")


# --- correlation metadata and span negative canaries -----------------------


@dataclass
class _CapturedSpan:
    name: str
    kwargs: dict[str, Any]
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[Any] = field(default_factory=list)


@pytest.fixture
def captured_spans(monkeypatch: pytest.MonkeyPatch) -> list[_CapturedSpan]:
    spans: list[_CapturedSpan] = []
    sdk_tracer = None
    sdk_exporter = None
    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

        sdk_exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(sdk_exporter))
        sdk_tracer = provider.get_tracer("triage.test")
    except Exception:
        sdk_tracer = None

    @contextmanager
    def start_as_current_span(name: str, **kwargs: Any):
        recorded = _CapturedSpan(name=name, kwargs=dict(kwargs))
        spans.append(recorded)
        if sdk_tracer is not None:
            with sdk_tracer.start_as_current_span(name, **kwargs) as real_span:
                class _DualSpan:
                    def set_attribute(self, key: str, value: Any) -> None:
                        recorded.attributes[key] = value
                        real_span.set_attribute(key, value)

                try:
                    yield _DualSpan()
                finally:
                    if sdk_exporter is not None:
                        for finished in sdk_exporter.get_finished_spans():
                            if finished.events:
                                recorded.events.extend(finished.events)
        else:
            yield SimpleNamespace(
                set_attribute=lambda key, value: recorded.attributes.__setitem__(key, value),
            )

    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
    monkeypatch.setattr(
        observability, "_tracer", SimpleNamespace(start_as_current_span=start_as_current_span),
    )
    return spans


def test_validated_correlation_fields_are_exported_for_sql_failures() -> None:
    exc = DriverLikeError(
        "Driver Error: Syntax error or access violation; DDBC Error: "
        "[Microsoft][SQL Server]A terminal handoff decision cannot be changed",
    )

    fields = _exported_failure_fields(
        "reconcile_work",
        exc,
        rpc="controller.resolve_frontier",
        service="controller",
        build="0.1.0",
        work_id=WORK_ID,
        work_kind="reconcile_state",
        request_id=REQUEST_ID,
        handoff_id=HANDOFF_ID,
        revision=12,
        handoff_revision=70,
        fence=3,
        decision_mode="handoff",
    )

    assert fields == {
        "operation": "reconcile_work",
        "rpc": "controller.resolve_frontier",
        "service": "controller",
        "build": "0.1.0",
        "error_type": "DriverLikeError",
        "classified_error_type": "MonitoringConflict",
        "guard_code": "51072",
        "work_id": WORK_ID,
        "work_kind": "reconcile_state",
        "request_id": REQUEST_ID,
        "handoff_id": HANDOFF_ID,
        "revision": "12",
        "handoff_revision": "70",
        "fence": "3",
        "decision_mode": "handoff",
    }


def test_untrusted_or_canary_correlation_fields_are_rejected() -> None:
    exc = DriverLikeError(f"Truncated value: '{CANARY}'.", code=f"value: '{CANARY}'")

    fields = _exported_failure_fields(
        "reconcile_work",
        exc,
        rpc=CANARY,
        service=CANARY,
        build=f"0-{CANARY}",
        work_id=CANARY,
        work_kind=CANARY,
        request_id=f"quoted '{CANARY}'",
        handoff_id=CANARY,
        revision=CANARY,
        handoff_revision=True,
        fence=-7,
        decision_mode=CANARY,
    )

    assert CANARY not in " ".join(f"{key}={value}" for key, value in fields.items())
    assert fields["rpc"] == "none"
    assert fields["service"] == "none"
    assert fields["build"] == "unknown"
    assert fields["work_id"] == "none"
    assert fields["work_kind"] == "none"
    assert fields["request_id"] == "none"
    assert fields["handoff_id"] == "none"
    assert fields["revision"] == "none"
    assert fields["handoff_revision"] == "none"
    assert fields["fence"] == "none"
    assert fields["decision_mode"] == "none"


def test_span_handle_rejects_untrusted_strings_and_malformed_correlation_attributes(
    captured_spans: list[_CapturedSpan],
) -> None:
    with observability.monitoring_work_span(
        service=CANARY,
        build=f"1.0-{CANARY}",
        work_id=CANARY,
        work_kind=CANARY,
        request_id=f"'{CANARY}'",
        handoff_id="a" * 200,
        revision=CANARY,
        handoff_revision=False,
        fence=-1,
        decision_mode=CANARY,
    ) as span:
        span.set("custom.quoted", f"Truncated value: '{CANARY}'.")
        span.set("custom.oversized", "x" * 200)
        span.set("error.cause", f"DriverLikeError:{CANARY}")

    assert len(captured_spans) == 1
    recorded = captured_spans[0]
    assert CANARY not in str(recorded.attributes)
    assert "x" * 200 not in str(recorded.attributes)
    assert "custom.quoted" not in recorded.attributes
    assert "custom.oversized" not in recorded.attributes
    assert recorded.attributes["service.name"] == "none"
    assert recorded.attributes["service.version"] == "unknown"
    assert recorded.attributes["monitoring.work_id"] == "none"
    assert recorded.attributes["monitoring.work_kind"] == "none"
    assert recorded.attributes["monitoring.request_id"] == "none"
    assert recorded.attributes["monitoring.handoff_id"] == "none"
    assert recorded.attributes["monitoring.revision"] == "none"
    assert recorded.attributes["monitoring.handoff_revision"] == "none"
    assert recorded.attributes["monitoring.fence"] == "none"
    assert recorded.attributes["monitoring.decision_mode"] == "none"
    assert recorded.attributes["error.cause"] == ""


@pytest.mark.parametrize(
    ("exc", "expected_cls", "expected_guard"),
    [
        (
            DriverLikeError(
                f"String or binary data would be truncated. Truncated value: '{CANARY}'.",
                code=f"value: '{CANARY}'",
            ),
            MonitoringUnavailable,
            "none",
        ),
        (
            DriverLikeError(
                "Driver Error: Syntax error or access violation; DDBC Error: "
                "[Microsoft][SQL Server]A terminal handoff decision cannot be changed",
                code="a" * 300,
            ),
            MonitoringConflict,
            "51072",
        ),
    ],
)
def test_sql_backend_failure_emits_correlated_log_and_span_without_row_or_code_canaries(
    exc: DriverLikeError,
    expected_cls: type[Exception],
    expected_guard: str,
    captured_spans: list[_CapturedSpan],
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FailingDb:
        @contextmanager
        def transaction(self):
            yield self

        def query(self, sql: str, *params: object) -> list[tuple[object, ...]]:
            if sql.startswith("SELECT OBJECT_ID"):
                return [(1, 2, 3, 4)]
            raise exc

    backend = SqlBackend(_FailingDb(), component="controller")  # type: ignore[arg-type]

    with (
        caplog.at_level(logging.INFO, logger="triage.telemetry"),
        pytest.raises(expected_cls),
        backend.transaction(write=True, operation="reconcile_work", request_id=REQUEST_ID),
    ):
        backend.bind_correlation(
            rpc="controller.resolve_frontier",
            work_id=WORK_ID,
            work_kind="reconcile_state",
            request_id=REQUEST_ID,
            handoff_id=HANDOFF_ID,
            revision=12,
            handoff_revision=70,
            fence=3,
            decision_mode="handoff",
        )
        backend.db.query("EXEC controller_resolve_frontier")

    exported_logs = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("triage.telemetry")
    )
    assert "monitoring_sql_failed " in exported_logs
    assert "operation=reconcile_work" in exported_logs
    assert "rpc=controller.resolve_frontier" in exported_logs
    assert "service=controller" in exported_logs
    assert f"guard_code={expected_guard}" in exported_logs
    assert f"classified_error_type={expected_cls.__name__}" in exported_logs
    assert f"work_id={WORK_ID}" in exported_logs
    assert "work_kind=reconcile_state" in exported_logs
    assert f"request_id={REQUEST_ID}" in exported_logs
    assert f"handoff_id={HANDOFF_ID}" in exported_logs
    assert "revision=12" in exported_logs
    assert "handoff_revision=70" in exported_logs
    assert "fence=3" in exported_logs
    assert "decision_mode=handoff" in exported_logs

    assert len(captured_spans) == 1
    sql_span = captured_spans[0]
    assert sql_span.name == "triage.monitoring.sql"
    assert sql_span.kwargs == {"record_exception": False, "set_status_on_exception": False}
    assert sql_span.events == []
    assert sql_span.attributes["service.name"] == "controller"
    assert sql_span.attributes["sql.operation"] == "reconcile_work"
    assert sql_span.attributes["sql.rpc"] == "controller.resolve_frontier"
    assert sql_span.attributes["sql.status"] == "failed"
    assert sql_span.attributes["sql.guard_code"] == expected_guard
    assert sql_span.attributes["sql.classified_error_type"] == expected_cls.__name__
    assert sql_span.attributes["error.type"] == "DriverLikeError"
    assert sql_span.attributes["monitoring.work_id"] == WORK_ID
    assert sql_span.attributes["monitoring.work_kind"] == "reconcile_state"
    assert sql_span.attributes["monitoring.request_id"] == REQUEST_ID
    assert sql_span.attributes["monitoring.handoff_id"] == HANDOFF_ID
    assert sql_span.attributes["monitoring.revision"] == "12"
    assert sql_span.attributes["monitoring.handoff_revision"] == "70"
    assert sql_span.attributes["monitoring.fence"] == "3"
    assert sql_span.attributes["monitoring.decision_mode"] == "handoff"

    combined = exported_logs + "\n" + str(sql_span.attributes) + "\n" + str(sql_span.events)
    assert CANARY not in combined
    assert "String or binary data would be truncated" not in combined
    assert "terminal handoff decision" not in combined
    assert "a" * 200 not in combined


@pytest.mark.parametrize(
    ("inner", "expected_cause"),
    [
        (
            DriverLikeError(
                f"String or binary data would be truncated. Truncated value: '{CANARY}'.",
                code=f"value: '{CANARY}'",
            ),
            "DriverLikeError",
        ),
        (
            type("Review", (FixedDiagnosticError,), {})("supersession_presence_inspection_expired"),
            "Review:supersession_presence_inspection_expired",
        ),
        (
            type("Review", (FixedDiagnosticError,), {})("a" * 200),
            "Review",
        ),
        (
            type("Review", (FixedDiagnosticError,), {})(f"quoted '{CANARY}'"),
            "Review",
        ),
    ],
)
async def test_controller_heartbeat_and_work_spans_exclude_canaries_and_preserve_correlation(
    inner: BaseException,
    expected_cause: str,
    captured_spans: list[_CapturedSpan],
    caplog: pytest.LogCaptureFixture,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 9, 22, 17, 0, tzinfo=UTC)
    leased_work = MonitoringWork(
        tenant_id=TENANT_ID,
        epoch=EPOCH_ID,
        work_id=WORK_ID,
        kind="reconcile_state",
        state="leased",
        revision=2,
        policy_revision=12,
        created_at=now,
        due_at=now,
        reconcile_request_id=HANDOFF_ID,
        reconcile_producer="worker",
        reason=f"Untrusted reason text '{CANARY}' must never enter telemetry",
        lease=LeaseToken(
            tenant_id=TENANT_ID,
            epoch=EPOCH_ID,
            resource_key=f"work:v1:{EPOCH_ID}:{TENANT_ID}:{WORK_ID}",
            owner_id=OWNER_ID,
            fence=7,
            acquired_at=now,
            expires_at=now + timedelta(minutes=2),
        ),
    )

    runner = TriageRunner(
        Settings(
            _env_file=None,
            monitoring_mode="fixture",
            triage_tool_mode="mock",
            triage_provider_mode="mock",
            azure_sql_server="",
            azure_sql_database="",
            applicationinsights_connection_string="",
        ),
        base_dir=tmp_path,
    )
    monkeypatch.setattr(runner.monitoring, "claim_work", lambda _req: (leased_work,))

    def failing_reconcile(work: MonitoringWork, *, connector_publisher: Any = None) -> Any:
        observability.bind_work_correlation(
            service="controller",
            work_id=work.work_id,
            work_kind=work.kind,
            request_id=REQUEST_ID,
            handoff_id=HANDOFF_ID,
            revision=12,
            handoff_revision=70,
            fence=7,
            decision_mode="handoff",
            rpc="controller.resolve_frontier",
        )
        raise _wrapped(inner)

    monkeypatch.setattr(runner.monitoring, "reconcile_work", failing_reconcile)
    monkeypatch.setattr(type(runner.monitoring), "component", property(lambda _self: "controller"))

    async def empty_human(_runner: Any, *, limit: int, budget: Any) -> list[str]:
        return []

    with (
        caplog.at_level(logging.INFO, logger="triage.telemetry"),
        pytest.raises(MonitoringUnavailable),
    ):
        await controller_heartbeat(runner, rounds=1, command_drain=empty_human)

    exported_logs = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("triage.telemetry")
    )
    assert "heartbeat_finished status=failed" in exported_logs
    assert f"error_type=MonitoringUnavailable error_cause={expected_cause}" in exported_logs
    assert f"work_id={WORK_ID}" in exported_logs
    assert "work_kind=reconcile_state" in exported_logs
    assert f"request_id={REQUEST_ID}" in exported_logs
    assert f"handoff_id={HANDOFF_ID}" in exported_logs
    assert "revision=12" in exported_logs
    assert "handoff_revision=70" in exported_logs
    assert "fence=7" in exported_logs
    assert "decision_mode=handoff" in exported_logs
    assert "rpc=controller.resolve_frontier" in exported_logs

    by_name = {span.name: span for span in captured_spans}
    assert {"triage.heartbeat", "triage.monitoring.work"} <= set(by_name)
    for span in captured_spans:
        assert span.kwargs == {"record_exception": False, "set_status_on_exception": False}
        assert span.events == []

    work_span = by_name["triage.monitoring.work"]
    assert work_span.attributes["service.name"] == "controller"
    assert work_span.attributes["monitoring.status"] == "failed"
    assert work_span.attributes["monitoring.work_id"] == WORK_ID
    assert work_span.attributes["monitoring.work_kind"] == "reconcile_state"
    assert work_span.attributes["monitoring.request_id"] == REQUEST_ID
    assert work_span.attributes["monitoring.handoff_id"] == HANDOFF_ID
    assert work_span.attributes["monitoring.revision"] == "12"
    assert work_span.attributes["monitoring.handoff_revision"] == "70"
    assert work_span.attributes["monitoring.fence"] == "7"
    assert work_span.attributes["monitoring.decision_mode"] == "handoff"
    assert work_span.attributes["sql.rpc"] == "controller.resolve_frontier"
    assert work_span.attributes["error.type"] == "MonitoringUnavailable"
    assert work_span.attributes["error.cause"] == expected_cause

    hb_span = by_name["triage.heartbeat"]
    assert hb_span.attributes["service.name"] == "controller"
    assert hb_span.attributes["heartbeat.status"] == "failed"
    assert hb_span.attributes["monitoring.work_id"] == WORK_ID
    assert hb_span.attributes["monitoring.work_kind"] == "reconcile_state"
    assert hb_span.attributes["monitoring.request_id"] == REQUEST_ID
    assert hb_span.attributes["monitoring.handoff_id"] == HANDOFF_ID
    assert hb_span.attributes["monitoring.revision"] == "12"
    assert hb_span.attributes["monitoring.handoff_revision"] == "70"
    assert hb_span.attributes["monitoring.fence"] == "7"
    assert hb_span.attributes["monitoring.decision_mode"] == "handoff"
    assert hb_span.attributes["sql.rpc"] == "controller.resolve_frontier"
    assert hb_span.attributes["error.type"] == "MonitoringUnavailable"
    assert hb_span.attributes["error.cause"] == expected_cause
    assert isinstance(hb_span.attributes["heartbeat.context_ms"], int)
    assert isinstance(hb_span.attributes["heartbeat.claim_ms"], int)
    assert isinstance(hb_span.attributes["heartbeat.execute_ms"], int)

    all_exported = (
        exported_logs
        + "\n"
        + "\n".join(f"{span.name}:{span.attributes}:{span.events}" for span in captured_spans)
    )
    assert CANARY not in all_exported
    assert "String or binary data would be truncated" not in all_exported
    assert "shared state was not replaced" not in all_exported
    assert "a" * 200 not in all_exported

