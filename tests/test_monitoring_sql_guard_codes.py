"""A kernel guard refusal must not look like a database outage.

Against a live Azure SQL server the driver raises ProgrammingError whose entire
text is "Driver Error: Syntax error or access violation; DDBC Error:
[Microsoft][SQL Server]<message>". The THROW's error number appears nowhere --
not in the message, args or any attribute -- so matching on the number alone
classified every deterministic refusal as MonitoringUnavailable and a live
worker retried a policy refusal every three minutes as though the store were
down.
"""

from __future__ import annotations

import pytest

from triage.monitoring.contracts import (
    MonitoringComponentDenied,
    MonitoringConflict,
    MonitoringLeaseLost,
    MonitoringUnavailable,
)
from triage.monitoring.sql_store import _guard_code, _guard_messages


def _driver_text(message: str) -> str:
    return (
        "Driver Error: Syntax error or access violation; "
        f"DDBC Error: [Microsoft][SQL Server]{message}"
    )


def test_every_guard_message_resolves_to_its_code() -> None:
    messages = _guard_messages()
    assert len(messages) > 100
    for message, code in messages:
        assert _guard_code(_driver_text(message)) == code


def test_messages_are_ordered_longest_first() -> None:
    lengths = [len(message) for message, _ in _guard_messages()]
    assert lengths == sorted(lengths, reverse=True)


def test_a_numbered_message_is_still_honoured() -> None:
    assert _guard_code("Msg 51074, Level 16, State 1: lease lost") == "51074"


def test_an_unrelated_driver_error_is_not_classified_as_a_guard() -> None:
    assert _guard_code("Driver Error: Communication link failure") is None
    assert _guard_code("Invalid column name 'nope'") is None


@pytest.mark.parametrize(
    ("code", "error"),
    [
        ("51070", MonitoringComponentDenied),
        ("51072", MonitoringConflict),
        ("51073", MonitoringConflict),
        ("51074", MonitoringLeaseLost),
    ],
)
def test_known_codes_map_to_typed_refusals(code: str, error: type[Exception]) -> None:
    message = next(text for text, found in _guard_messages() if found == code)
    assert _guard_code(_driver_text(message)) == code
    assert issubclass(error, Exception)
    assert not issubclass(error, MonitoringUnavailable) or error is MonitoringUnavailable


def test_the_live_connector_refusal_is_a_conflict_not_an_outage() -> None:
    # The exact text a deployed worker logged every three minutes.
    text = _driver_text("Connector observation includes an unauthorized field")
    assert _guard_code(text) == "51073"
