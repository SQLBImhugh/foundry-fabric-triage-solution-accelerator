"""Signature stability is what makes suppression trustworthy."""

from __future__ import annotations

from triage.signature import compute_signature, incident_id, normalize


def _sig(error: str, *, artifact: str = "Sales Daily Summary") -> str:
    return compute_signature(
        source="powerbi_refresh_failure", error=error, artifact_name=artifact
    )[0]


def test_identical_errors_share_a_signature() -> None:
    error = "RefreshError: The operation was cancelled after 1800 seconds."
    assert _sig(error) == _sig(error)


def test_guids_do_not_change_the_signature() -> None:
    a = "RefreshError: failed. RequestId: 4b8f2c1e-77a3-4d90-b6e2-9c0518aa3f77"
    b = "RefreshError: failed. RequestId: 91d3e7a8-2c14-4b6f-a0e9-5d8721cc4b12"
    assert _sig(a) == _sig(b)


def test_timestamps_do_not_change_the_signature() -> None:
    a = "RefreshError: failed at 2026-08-26T05:00:04Z"
    b = "RefreshError: failed at 2026-08-27T11:32:19Z"
    assert _sig(a) == _sig(b)


def test_ip_addresses_do_not_change_the_signature() -> None:
    a = "ConnectionError: no route to 10.1.2.3:1433"
    b = "ConnectionError: no route to 172.16.9.44:1433"
    assert _sig(a) == _sig(b)


def test_different_error_classes_do_not_collide() -> None:
    a = "RefreshError: the operation was cancelled."
    b = "AuthorizationError: the operation was cancelled."
    assert _sig(a) != _sig(b)


def test_the_same_error_on_two_reports_stays_two_incidents() -> None:
    """Suppressing across unrelated reports would hide a real second outage."""
    error = "RefreshError: the operation was cancelled."
    assert _sig(error, artifact="Report A") != _sig(error, artifact="Report B")


def test_case_is_preserved() -> None:
    """SQL identifier case is significant in some dialects."""
    a = "SchemaError: column 'WellId' not found"
    b = "SchemaError: column 'wellid' not found"
    assert _sig(a) != _sig(b)


def test_normalize_prefers_the_error_bearing_line() -> None:
    text = (
        "Hi team,\n"
        "The refresh did not complete.\n"
        "RefreshError: the operation was cancelled.\n"
        "- Power BI service"
    )
    assert normalize(text).startswith("RefreshError")


def test_normalize_handles_empty_input() -> None:
    assert normalize("") == ""
    assert compute_signature(source="s", error="")[0]


def test_signature_is_sixteen_chars() -> None:
    assert len(_sig("RefreshError: boom")) == 16


def test_resolved_and_unresolved_ids_are_separate_namespaces() -> None:
    """A fix and a prior crash for the same failure must coexist as distinct rows."""
    sig = "abc123def4567890"
    assert incident_id(sig, resolved=True) != incident_id(sig, resolved=False)
    assert incident_id(sig, resolved=True).startswith("sig:")
    assert incident_id(sig, resolved=False).startswith("usig:")
