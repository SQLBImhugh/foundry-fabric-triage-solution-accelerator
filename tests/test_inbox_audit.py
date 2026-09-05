"""The inbox audit: what the filter refused, and why.

The filter itself is covered by `test_mail_filter.py`. What matters here is the
evidence trail, because a count alone cannot tell an operator whether the filter
is correctly rejecting noise or has silently gone deaf to the real alerts.
"""

from __future__ import annotations

import pytest

from triage.store.inbox_audit import (
    DEFAULT_MAX_ROWS,
    InMemoryInboxAudit,
    JsonFileInboxAudit,
)
from triage.tools.inbox import GraphInbox
from triage.tools.mail_filter import MailFilter


def _msg(mid: str, sender: str, subject: str) -> dict:
    return {
        "id": mid,
        "subject": subject,
        "from": {"emailAddress": {"address": sender}},
        "body": {"content": "body"},
        "receivedDateTime": "2026-09-05T00:00:00Z",
    }


def _inbox(audit) -> GraphInbox:
    """A GraphInbox wired for classification only — it never reaches Graph."""
    inbox = GraphInbox.__new__(GraphInbox)
    inbox._processed = type("_P", (), {"seen": lambda self, m: False})()
    inbox._filter = MailFilter.build(
        senders="no-reply-powerbi@microsoft.com",
        subject_pattern=r"(?i)\brefresh\b",
    )
    inbox._audit = audit
    return inbox


# ---------------------------------------------------------------------------
# The record itself
# ---------------------------------------------------------------------------


def test_a_refused_message_is_recorded_with_its_reason() -> None:
    audit = InMemoryInboxAudit()
    audit.record(
        message_id="m1",
        sender="attacker@example.com",
        subject="Refresh failed",
        reason="sender attacker@example.com not on the allowlist",
    )

    rows = audit.recent()
    assert len(rows) == 1
    assert rows[0]["sender"] == "attacker@example.com"
    assert "not on the allowlist" in rows[0]["reason"]


def test_the_reason_distinguishes_the_two_rejection_paths() -> None:
    """Sender and subject rejections send an operator to different places."""
    audit = InMemoryInboxAudit()
    inbox = _inbox(audit)

    inbox._classify(_msg("a", "attacker@example.com", "Refresh failed"))
    inbox._classify(_msg("b", "no-reply-powerbi@microsoft.com", "Your weekly digest"))

    reasons = " | ".join(r["reason"] for r in audit.recent())
    assert "allowlist" in reasons
    assert "subject" in reasons


def test_an_accepted_message_is_not_audited() -> None:
    """The audit is a record of refusals, not a log of all traffic."""
    audit = InMemoryInboxAudit()
    inbox = _inbox(audit)

    result = inbox._classify(
        _msg("ok", "no-reply-powerbi@microsoft.com", "Refresh failed for Sales")
    )

    assert result not in ("filtered", "processed")
    assert audit.recent() == []


def test_the_same_message_is_audited_once() -> None:
    """A sweep re-reading the mailbox must not bury the row it already wrote."""
    audit = InMemoryInboxAudit()
    inbox = _inbox(audit)

    for _ in range(5):
        inbox._classify(_msg("dupe", "attacker@example.com", "Refresh failed"))

    assert audit.count == 1


# ---------------------------------------------------------------------------
# The control must outlive its evidence
# ---------------------------------------------------------------------------


def test_a_broken_audit_still_refuses_the_message() -> None:
    """Evidence is not the control.

    If recording the refusal could stop the refusal, an attacker could get a
    message accepted by making the audit fail.
    """

    class Exploding:
        def record(self, **_kwargs):
            raise RuntimeError("audit store is down")

    inbox = _inbox(Exploding())

    assert inbox._classify(_msg("x", "attacker@example.com", "Refresh failed")) == "filtered"


def test_no_audit_configured_is_still_a_refusal() -> None:
    inbox = _inbox(None)

    assert inbox._classify(_msg("x", "attacker@example.com", "Refresh failed")) == "filtered"


# ---------------------------------------------------------------------------
# Redaction and retention
# ---------------------------------------------------------------------------


def test_redaction_happens_inside_the_store() -> None:
    """A refused message is unvetted input, so it is the likeliest to carry a
    secret. Redaction stays at the persistence boundary, as everywhere else."""
    audit = InMemoryInboxAudit()
    audit.record(
        message_id="m",
        sender="x@example.com",
        subject="token AKIAIOSFODNN7EXAMPLE and more",
        reason="subject does not look like a Power BI refresh alert",
    )

    stored = audit.recent()[0]
    assert "AKIAIOSFODNN7EXAMPLE" not in stored["subject"]
    assert stored["redaction_applied"] is True


def test_the_reason_is_redacted_not_just_the_sender() -> None:
    """The allowlist rejection interpolates the sender into its reason text, so
    an unredacted reason would put back exactly what the sender column strips.

    Regression: `reason` was stored raw while `sender` and `subject` were
    redacted, which made the redaction on `sender` decorative for the one
    rejection path an attacker controls.
    """
    audit = InMemoryInboxAudit()
    secret = "AKIAIOSFODNN7EXAMPLE"
    audit.record(
        message_id="m",
        sender=f"{secret}@evil.example",
        subject="Refresh failed",
        reason=f"sender {secret}@evil.example not on the allowlist",
    )

    stored = audit.recent()[0]
    assert secret not in stored["sender"]
    assert secret not in stored["reason"], "the reason leaked what the sender hid"
    assert stored["redaction_applied"] is True


def test_the_audit_is_bounded() -> None:
    """Evidence, not history. An unbounded table nothing reads back is a leak."""
    audit = InMemoryInboxAudit(max_rows=10)
    for i in range(40):
        audit.record(message_id=f"m{i}", sender="a@b.c", subject="s", reason="r")

    assert audit.count == 10


def test_the_default_cap_is_applied() -> None:
    assert InMemoryInboxAudit()._max_rows == DEFAULT_MAX_ROWS


def test_pruning_keeps_the_newest_rows() -> None:
    audit = InMemoryInboxAudit(max_rows=3)
    for i in range(6):
        audit.record(
            message_id=f"m{i}", sender="a@b.c", subject=f"subject {i}", reason="r"
        )

    kept = {r["subject"] for r in audit.recent()}
    assert "subject 5" in kept
    assert "subject 0" not in kept


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


def test_the_file_store_survives_a_restart(tmp_path) -> None:
    path = tmp_path / "inbox_audit.json"
    first = JsonFileInboxAudit(path)
    first.record(message_id="m1", sender="a@b.c", subject="s", reason="r")

    assert JsonFileInboxAudit(path).count == 1


def test_reset_clears_the_file(tmp_path) -> None:
    path = tmp_path / "inbox_audit.json"
    audit = JsonFileInboxAudit(path)
    audit.record(message_id="m1", sender="a@b.c", subject="s", reason="r")
    audit.reset()

    assert JsonFileInboxAudit(path).count == 0


@pytest.mark.parametrize("limit", [1, 3, 50])
def test_recent_respects_its_limit(limit: int) -> None:
    audit = InMemoryInboxAudit()
    for i in range(5):
        audit.record(message_id=f"m{i}", sender="a@b.c", subject="s", reason="r")

    assert len(audit.recent(limit)) == min(limit, 5)
