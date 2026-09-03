"""The incident store: dedup, suppression, and redaction at the boundary."""

from __future__ import annotations

from triage.store.incidents import InMemoryIncidentStore, JsonFileIncidentStore


def test_every_terminal_outcome_is_persisted(store: InMemoryIncidentStore, result_factory) -> None:
    """An earlier success-only gate recorded only successes and missed 10 crashes."""
    outcomes = [
        "resolved",
        "flagged_data_quality",
        "needs_human",
        "declared_failed",
        "agent_crashed",
        "timed_out",
        "budget_exceeded",
        "max_turns_exceeded",
        "policy_blocked",
    ]
    for idx, outcome in enumerate(outcomes):
        store.record(result_factory(outcome=outcome, signature=f"sig{idx:012d}"))

    assert len(store.list_all()) == len(outcomes)


def test_repeat_failure_increments_rather_than_duplicating(store, result_factory) -> None:
    for _ in range(3):
        store.record(result_factory(outcome="needs_human"))

    incidents = store.list_all()
    assert len(incidents) == 1
    assert incidents[0].occurrence_count == 3


def test_suppressed_duplicate_increments_the_parent_incident(store, result_factory) -> None:
    """A suppression must not fork a second row, or dedup achieves nothing."""
    parent = store.record(result_factory(outcome="flagged_data_quality", action_taken=""))
    assert parent.status == "open"

    store.record(result_factory(outcome="duplicate_suppressed", action_taken=""))

    incidents = store.list_all()
    assert len(incidents) == 1
    assert incidents[0].id == parent.id
    assert incidents[0].occurrence_count == 2
    assert incidents[0].outcome == "flagged_data_quality", "parent outcome must not be overwritten"


def test_resolved_and_unresolved_coexist(store, result_factory) -> None:
    store.record(result_factory(outcome="agent_crashed"))
    store.record(result_factory(outcome="resolved"))

    ids = {i.id for i in store.list_all()}
    assert len(ids) == 2


def test_only_open_incidents_suppress(store, result_factory) -> None:
    """A resolved incident recurring is new information, not a duplicate."""
    store.record(result_factory(outcome="resolved"))
    assert store.find_open("abc123def456") is None

    store.record(result_factory(outcome="needs_human", signature="def456abc123"))
    assert store.find_open("def456abc123") is not None


def test_secrets_are_redacted_before_persist(store, result_factory) -> None:
    incident = store.record(
        result_factory(outcome="agent_crashed", root_cause="failed with Bearer abcdefghijklmnopqrstuvwxyz012345"),
        original_error="AccountKey=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOP==",
    )

    assert "AccountKey=abcdefghij" not in incident.original_error
    assert "Bearer abcdefghij" not in incident.diagnosed_root_cause
    assert incident.redaction_applied is True
    assert incident.redaction_kinds


def test_escalations_and_refusals_are_marked_for_investigation(store, result_factory) -> None:
    crash = store.record(result_factory(outcome="agent_crashed", signature="a" * 16))
    escalation = store.record(result_factory(outcome="needs_human", signature="b" * 16))
    refused = store.record(
        result_factory(outcome="resolved", signature="c" * 16, blocked_attempts=["delete_dataset"])
    )
    clean = store.record(result_factory(outcome="resolved", signature="d" * 16))

    assert crash.requires_investigation
    assert escalation.requires_investigation
    assert refused.requires_investigation, "a refused action is a signal worth reading"
    assert not clean.requires_investigation


def test_json_store_survives_a_restart(tmp_path, result_factory) -> None:
    """Scenario 2b depends on the store outliving the process."""
    path = tmp_path / "incidents.json"

    first = JsonFileIncidentStore(path)
    first.record(result_factory(outcome="flagged_data_quality", action_taken=""))

    reopened = JsonFileIncidentStore(path)
    assert reopened.find_open("abc123def456") is not None


def test_json_store_tolerates_a_corrupt_file(tmp_path, result_factory) -> None:
    path = tmp_path / "incidents.json"
    path.write_text("{ not json", encoding="utf-8")

    store = JsonFileIncidentStore(path)
    assert store.list_all() == []


def test_reset_clears_everything(tmp_path, result_factory) -> None:
    store = JsonFileIncidentStore(tmp_path / "incidents.json")
    store.record(result_factory())
    store.reset()
    assert store.list_all() == []


def test_mark_updates_status_and_redacts_notes(store, result_factory) -> None:
    incident = store.record(result_factory(outcome="needs_human"))
    updated = store.mark(incident.id, "investigating", notes="see ghp_1234567890abcdefghijklmnopqrstuvwx")

    assert updated is not None
    assert updated.status == "investigating"
    assert "ghp_1234567890" not in updated.triage_notes
