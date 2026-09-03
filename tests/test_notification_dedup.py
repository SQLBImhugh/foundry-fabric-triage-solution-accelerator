"""Tests for the two defects that flooded a Teams channel.

Both were found in the demo tenant on 31 Aug 2026, not in review. A five-minute
routine swept a mailbox holding two unread Power BI alerts and posted two
identical cards every five minutes -- roughly 24 an hour -- driving one incident
to 130 occurrences. Two independent causes:

1. ``GraphInbox`` tracked already-seen messages in a set on the instance, and
   ``app.py::_drain_mailbox`` rebuilds the inbox on every invocation. The set
   was therefore always empty, so every sweep re-triaged the entire mailbox.

2. Every suppressed duplicate still posted a card. Deduplication stopped the
   *remediation* but not the *notification*, which is the alert fatigue this
   system exists to remove.

Neither was caught by the suite because nothing exercised a second sweep. These
tests do.
"""

from __future__ import annotations

import pytest
from test_agents import CannedProvider, _tool_response

from triage.agents.data_quality_agent import DataQualityAgent
from triage.agents.triage_agent import TriageAgent, TriageDeps
from triage.models import Incident
from triage.providers.mock import ScriptedDataQualityProvider
from triage.store.processed import (
    InMemoryProcessedLog,
    JsonFileProcessedLog,
    _fingerprint,
)
from triage.tools.flags import DataQualityFlagTable
from triage.tools.powerbi import MockPowerBIClient
from triage.tools.teams import MockTeamsNotifier

MESSAGE_ID = "AAMkAGI2TG93AAA=_long_graph_style_id/with+illegal?chars"


# ---------------------------------------------------------------------------
# 1. The processed-message log has to outlive the process
# ---------------------------------------------------------------------------


def test_in_memory_log_forgets_across_instances() -> None:
    """Pins the old behaviour, so the reason for the file-backed log is explicit.

    This is not a bug in ``InMemoryProcessedLog`` -- it is the correct
    behaviour for a single CLI run. It is only wrong when it is the *only*
    record a stateless hosted agent has.
    """
    first = InMemoryProcessedLog()
    first.mark(MESSAGE_ID)
    assert first.seen(MESSAGE_ID)

    assert not InMemoryProcessedLog().seen(MESSAGE_ID)


def test_file_log_survives_a_new_instance(tmp_path) -> None:
    """The property the hosted agent actually depends on."""
    path = tmp_path / "processed.json"

    JsonFileProcessedLog(path).mark(MESSAGE_ID, received_at="2026-08-31T14:35:23Z")

    # A brand new instance, exactly as the next invocation would build it.
    assert JsonFileProcessedLog(path).seen(MESSAGE_ID)
    assert not JsonFileProcessedLog(path).seen("some-other-message")


def test_reset_clears_the_file_log(tmp_path) -> None:
    path = tmp_path / "processed.json"
    log = JsonFileProcessedLog(path)
    log.mark(MESSAGE_ID)
    log.reset()

    assert not JsonFileProcessedLog(path).seen(MESSAGE_ID)


def test_fingerprint_is_key_safe_and_collision_free() -> None:
    """Graph ids contain characters Table Storage rejects in a key.

    Truncating to fit would let two different messages land on one row, and the
    second would be silently treated as already handled.
    """
    fingerprint = _fingerprint(MESSAGE_ID)

    assert len(fingerprint) == 64
    assert all(c in "0123456789abcdef" for c in fingerprint)
    assert _fingerprint(MESSAGE_ID) == fingerprint
    assert _fingerprint(MESSAGE_ID + "x") != fingerprint


# ---------------------------------------------------------------------------
# 2. An incident is announced once, not once per occurrence
# ---------------------------------------------------------------------------


def _deps(tmp_path, *, known_incident=None) -> TriageDeps:
    return TriageDeps(
        powerbi=MockPowerBIClient(latency_ms=0),
        teams=MockTeamsNotifier(),
        flag_table=DataQualityFlagTable(tmp_path / "flags.csv"),
        signature="testsig000000009",
        known_incident=known_incident,
    )


def _known(notified_count: int) -> Incident:
    return Incident(
        id="usig:testsig000000009",
        signature="testsig000000009",
        outcome="flagged_data_quality",
        status="open",
        occurrence_count=7,
        notified_count=notified_count,
    )


def _suppressing_provider() -> CannedProvider:
    return CannedProvider(
        responses=[
            _tool_response("get_known_incidents", {}),
            _tool_response(
                "notify_teams",
                {
                    "title": "Duplicate suppressed",
                    "action_taken": "none",
                    "outcome": "duplicate_suppressed",
                },
            ),
            _tool_response(
                "report_resolution",
                {
                    "outcome": "duplicate_suppressed",
                    "summary": "Already tracked.",
                    "root_cause": "Known.",
                },
            ),
        ]
    )


def _agent(provider) -> TriageAgent:
    return TriageAgent(provider, dq_agent=DataQualityAgent(ScriptedDataQualityProvider()))


async def test_first_sighting_of_a_known_incident_still_notifies(
    sample_request, tmp_path
) -> None:
    """Scenario 3 keeps its demo behaviour: you see the suppression card once."""
    deps = _deps(tmp_path, known_incident=_known(notified_count=0))
    result = await _agent(_suppressing_provider()).run(sample_request, deps)

    assert len(deps.teams.messages) == 1
    assert result.notification_delivered
    assert not result.notification_suppressed


async def test_an_already_announced_incident_does_not_notify_again(
    sample_request, tmp_path
) -> None:
    """The flood, in one assertion."""
    deps = _deps(tmp_path, known_incident=_known(notified_count=1))
    result = await _agent(_suppressing_provider()).run(sample_request, deps)

    assert deps.teams.messages == []
    assert result.notification_suppressed
    assert not result.notification_delivered


async def test_a_suppressed_notification_is_not_reported_as_a_failure(
    sample_request, tmp_path
) -> None:
    """Deduplication working must not look like a broken notifier.

    ``notification_failed`` drives a warning, an emitted event and a "was not
    delivered" line appended to the summary. Reusing it for a deliberate
    suppression would cry wolf on every duplicate.
    """
    deps = _deps(tmp_path, known_incident=_known(notified_count=1))
    result = await _agent(_suppressing_provider()).run(sample_request, deps)

    assert not result.notification_failed
    assert "not delivered" not in result.summary


async def test_the_refusal_is_still_recorded_as_a_tool_call(
    sample_request, tmp_path
) -> None:
    """The audit trail must show the agent asked and the controller declined."""
    deps = _deps(tmp_path, known_incident=_known(notified_count=1))
    result = await _agent(_suppressing_provider()).run(sample_request, deps)

    assert any(action.tool_name == "notify_teams" for action in result.actions)


# ---------------------------------------------------------------------------
# 3. The store counts announcements, so the decision above has an input
# ---------------------------------------------------------------------------


def test_only_a_delivered_card_counts_as_an_announcement(store, result_factory) -> None:
    """A failed delivery must not silence every future one."""
    store.record(result_factory(outcome="flagged_data_quality", action_taken=""))

    incident = store.record(
        result_factory(outcome="duplicate_suppressed", action_taken=""), notified=False
    )
    assert incident.notified_count == 0

    incident = store.record(
        result_factory(outcome="duplicate_suppressed", action_taken=""), notified=True
    )
    assert incident.notified_count == 1
    assert incident.last_notified_at


def test_suppressed_duplicates_still_count_occurrences(store, result_factory) -> None:
    """Silence is not the same as not noticing -- the count is the evidence."""
    store.record(result_factory(outcome="flagged_data_quality", action_taken=""))

    for _ in range(5):
        incident = store.record(
            result_factory(outcome="duplicate_suppressed", action_taken=""), notified=False
        )

    assert incident.occurrence_count == 6
    assert incident.notified_count == 0


@pytest.mark.parametrize("notified", [True, False])
def test_a_new_incident_records_whether_it_was_announced(
    store, result_factory, notified: bool
) -> None:
    incident = store.record(result_factory(outcome="needs_human"), notified=notified)
    assert incident.notified_count == (1 if notified else 0)


# ---------------------------------------------------------------------------
# 4. A second sweep must not re-ingest what the first one handled
# ---------------------------------------------------------------------------


class _FakeResponse:
    status_code = 200

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        """Always fine: these tests are about filtering, not transport."""


class _FakeClient:
    """Stands in for httpx so the suite never touches Graph."""

    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.calls = 0

    async def __aenter__(self) -> _FakeClient:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def get(self, url: str, headers: dict | None = None) -> _FakeResponse:
        self.calls += 1
        return _FakeResponse(self._payload)


ONE_ALERT = {
    "value": [
        {
            "id": MESSAGE_ID,
            "subject": "Power BI: Refresh failed for 'Orders Daily Rollup'",
            "body": {"content": "Error code: DM_GWPipeline_Gateway_TimeoutError"},
            "from": {"emailAddress": {"address": "no-reply-powerbi@microsoft.com"}},
            "receivedDateTime": "2026-08-31T14:35:23Z",
        }
    ]
}


def _inbox(log):
    from triage.tools.inbox import GraphInbox

    inbox = GraphInbox(
        tenant_id="tenant",
        client_id="client",
        client_secret="secret",
        mailbox="alerts@example.invalid",
        processed_log=log,
    )

    async def _token() -> str:
        return "not-a-real-token"

    inbox._get_token = _token  # type: ignore[method-assign]
    return inbox


@pytest.fixture
def _no_network(monkeypatch):
    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: _FakeClient(ONE_ALERT))


async def test_a_second_sweep_skips_an_already_triaged_message(
    tmp_path, _no_network
) -> None:
    """The flood's first cause, at the seam where it happened.

    The second inbox is built the way the next scheduled invocation builds it:
    a brand new object sharing nothing but the durable log.
    """
    path = tmp_path / "processed.json"

    first = _inbox(JsonFileProcessedLog(path))
    found = await first.fetch()
    assert len(found) == 1
    first.mark_processed(found[0].request_id, received_at=found[0].received_at)

    second = _inbox(JsonFileProcessedLog(path))
    assert await second.fetch() == []


async def test_fetching_alone_does_not_mark_a_message_processed(
    tmp_path, _no_network
) -> None:
    """At-least-once, deliberately.

    If ``fetch`` marked messages, a crash between reading the mail and
    recording the outcome would drop the alert with nothing to show for it.
    Re-triaging is the safer failure, and the signature dedup absorbs it.
    """
    path = tmp_path / "processed.json"

    found = await _inbox(JsonFileProcessedLog(path)).fetch()
    assert len(found) == 1

    # No mark_processed call: this stands in for a run that died mid-triage.
    assert len(await _inbox(JsonFileProcessedLog(path)).fetch()) == 1


async def test_an_in_memory_log_reproduces_the_original_flood(tmp_path, _no_network) -> None:
    """Pins the regression: without durable state every sweep re-ingests."""
    first = _inbox(InMemoryProcessedLog())
    found = await first.fetch()
    first.mark_processed(found[0].request_id)

    # A new instance, as the hosted agent builds one per invocation.
    assert len(await _inbox(InMemoryProcessedLog()).fetch()) == 1
