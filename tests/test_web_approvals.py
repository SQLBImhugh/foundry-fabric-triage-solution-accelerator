from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from triage.approvals import ApprovalRequest, WebApprovalGate
from triage.store.approvals import FabricSqlApprovalChannel, InMemoryApprovalChannel


def request(**changes) -> ApprovalRequest:
    return ApprovalRequest(**{
        "action": "rerun_fabric_pipeline", "arguments": {"pipeline_id": "synthetic"},
        "justification": "A test request", "request_id": "approval-test",
        "signature": "test-signature", "run_id": "test-run",
    } | changes)


def test_strict_decision_requires_matching_unexpired_fingerprint() -> None:
    channel = InMemoryApprovalChannel()
    proposal = request()
    channel.open_exact(proposal)
    with pytest.raises(ValueError, match="changed"):
        channel.decide_exact("approval-test", decision="approve", responder="operator", fingerprint="wrong")
    row = channel.decide_exact(
        "approval-test", decision="approve", responder="operator", fingerprint=proposal.fingerprint,
    )
    assert row["decision"] == "approve"
    with pytest.raises(ValueError, match="answered"):
        channel.decide_exact("approval-test", decision="decline", responder="other", fingerprint=proposal.fingerprint)


def test_expired_proposal_cannot_be_answered_or_renewed_by_reopening() -> None:
    channel = InMemoryApprovalChannel()
    proposal = request(requested_at=datetime.now(UTC) - timedelta(minutes=10))
    channel.open_exact(proposal)
    with pytest.raises(ValueError, match="expired"):
        channel.decide_exact("approval-test", decision="approve", responder="operator", fingerprint=proposal.fingerprint)
    with pytest.raises(ValueError, match="expired"):
        channel.open_exact(request())


def test_strict_proposal_record_is_redacted_and_correlated() -> None:
    channel = InMemoryApprovalChannel()
    proposal = request(arguments={"value": "AKIAIOSFODNN7EXAMPLE"})
    channel.open_exact(proposal)
    stored = channel.get(proposal.request_id)
    assert stored["signature"] == "test-signature"
    assert stored["run_id"] == "test-run"
    assert "AKIAIOSFODNN7EXAMPLE" not in str(stored["arguments"])


async def test_teams_outage_does_not_abandon_a_web_decision() -> None:
    class Source(InMemoryApprovalChannel):
        def open_exact(self, proposal):
            super().open_exact(proposal)
            self.decide_exact(
                proposal.request_id, decision="approve", responder="operator",
                fingerprint=proposal.fingerprint,
            )

    class BrokenTeams:
        async def post_card(self, _card):
            raise RuntimeError("Teams unavailable")

    channel = Source()
    gate = WebApprovalGate(channel, BrokenTeams(), poll_seconds=0)
    decision = await gate.request_approval(request())
    assert decision.granted
    assert gate.consume(decision)
    assert channel.get("approval-test")["consumed_at"]
    assert not gate.consume(decision)


def test_consumption_is_one_use_across_gate_instances() -> None:
    channel = InMemoryApprovalChannel()
    proposal = request()
    channel.open_exact(proposal)
    channel.decide_exact("approval-test", decision="approve", responder="operator", fingerprint=proposal.fingerprint)
    assert channel.consume_exact("approval-test", proposal.fingerprint)
    assert not channel.consume_exact("approval-test", proposal.fingerprint)
    with pytest.raises(ValueError):
        channel.open_exact(proposal)


def test_web_deep_link_does_not_authorize_a_decision_from_get() -> None:
    gate = WebApprovalGate(InMemoryApprovalChannel(), command_center_url="https://example.com")
    actions = gate._actions(request())
    assert actions[0]["url"] == "https://example.com/?approval=approval-test"
    assert "decision=" not in actions[0]["url"]


async def test_blocked_optional_delivery_cannot_delay_a_web_decision() -> None:
    blocked = asyncio.Event()

    class SlowTeams:
        async def post_card(self, _card):
            await blocked.wait()

    class Source(InMemoryApprovalChannel):
        def open_exact(self, proposal):
            super().open_exact(proposal)
            self.decide_exact(
                proposal.request_id, decision="approve", responder="operator",
                fingerprint=proposal.fingerprint,
            )

    gate = WebApprovalGate(Source(), SlowTeams(), poll_seconds=0.01)
    decision = await asyncio.wait_for(gate.request_approval(request()), timeout=0.2)
    assert decision.granted
    assert not blocked.is_set()


def test_sql_queue_limit_cannot_hide_a_low_identifier_pending_request() -> None:
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE triage_approvals(request_id TEXT, decision TEXT, payload TEXT)")
    now = datetime.now(UTC)
    for index in range(200):
        row = {
            "request_id": f"z-history-{index:03}", "decision": "decline",
            "requested_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=10)).isoformat(),
        }
        connection.execute("INSERT INTO triage_approvals VALUES (?, ?, ?)", (row["request_id"], "decline", json.dumps(row)))
    pending = {
        "request_id": "a-new-pending", "decision": "",
        "requested_at": (now - timedelta(seconds=10)).isoformat(),
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
    }
    connection.execute("INSERT INTO triage_approvals VALUES (?, ?, ?)", (pending["request_id"], None, json.dumps(pending)))

    class Sql:
        def query(self, statement, *params):
            limit = re.search(r"TOP \((\d+)\)", statement).group(1)
            sql = re.sub(r"TOP \(\d+\) ", "", statement).replace("[dbo].", "")
            sql = sql.replace("JSON_VALUE", "json_extract")
            sql = re.sub(r"TRY_CAST\((json_extract\(payload, '[^']+'\)) AS DATETIMEOFFSET\)", r"julianday(\1)", sql)
            sql = sql.replace("SYSDATETIMEOFFSET()", "julianday('now')")
            return connection.execute(sql + f" LIMIT {limit}", params).fetchall()

    rows = FabricSqlApprovalChannel(db=Sql()).list_requests(200)
    assert len(rows) == 200
    assert rows[0]["request_id"] == "a-new-pending"
    connection.close()
