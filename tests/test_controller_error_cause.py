"""A wrapped policy refusal must be diagnosable from telemetry alone.

The SQL store boundary reports anything it did not expect as
MonitoringUnavailable, "shared state was not replaced". A deterministic refusal
wrapped that way looks exactly like a database outage in App Insights. A
deployed controller repeated one identical refusal for 23 hours while the
heartbeat recorded only ``error_type=MonitoringUnavailable``, so the one query
an operator would run said "the store is down" when the store was healthy and
the work could never succeed.
"""

from __future__ import annotations

from triage.monitoring.contracts import MonitoringUnavailable
from triage.monitoring.controller import _root_cause
from triage.monitoring.provisioning import ProvisioningReview


def _wrapped(inner: BaseException) -> BaseException:
    try:
        try:
            raise inner
        except BaseException as exc:
            raise MonitoringUnavailable(
                "Monitoring SQL operation reconcile_work failed; shared state was not replaced",
            ) from exc
    except BaseException as outer:
        return outer


def test_a_wrapped_provisioning_review_names_its_fixed_code() -> None:
    outer = _wrapped(ProvisioningReview("supersession_presence_inspection_expired"))

    assert _root_cause(outer) == "ProvisioningReview:supersession_presence_inspection_expired"


def test_an_unwrapped_error_reports_no_cause() -> None:
    assert _root_cause(MonitoringUnavailable("no cause")) == ""


def test_a_cause_without_a_code_reports_only_its_class() -> None:
    outer = _wrapped(ValueError("private-row-content"))

    assert _root_cause(outer) == "ValueError"


def test_the_cause_message_is_never_emitted() -> None:
    outer = _wrapped(ProvisioningReview("supersession_presence_inspection_expired"))
    inner = _wrapped(ValueError("private-token-response-text"))

    assert "private-token-response-text" not in _root_cause(inner)
    assert "shared state was not replaced" not in _root_cause(outer)


def test_a_long_cause_chain_terminates() -> None:
    error: BaseException = ValueError("root")
    for _ in range(50):
        error = _wrapped(error)

    # Bounded walk: it reports something without hanging on a deep chain.
    assert isinstance(_root_cause(error), str)


def test_a_self_referencing_cause_cannot_loop() -> None:
    first = MonitoringUnavailable("first")
    second = MonitoringUnavailable("second")
    first.__cause__ = second
    second.__cause__ = first

    assert isinstance(_root_cause(first), str)
