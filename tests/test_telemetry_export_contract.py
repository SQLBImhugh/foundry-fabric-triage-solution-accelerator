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

import pytest

from triage.monitoring.contracts import FixedDiagnosticError, MonitoringUnavailable
from triage.monitoring.controller import _root_cause
from triage.monitoring.sql_store import _exported_failure_fields

CANARY = "synthetic-row-canary-value"


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
