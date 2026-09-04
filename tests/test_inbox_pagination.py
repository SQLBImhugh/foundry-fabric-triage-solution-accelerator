"""The inbox must not be blockable by mail it is going to ignore anyway.

`fetch` used to request one page of `limit` messages ordered newest-first and
stop. Once `limit` messages the filter rejects sat at the top of the inbox,
every sweep retrieved exactly those, skipped them all, and returned nothing,
while the logs reported "No new alerts."

That is a denial of service anyone who can email the mailbox can trigger, with
no privilege and no exploit: ten newsletters is enough.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from triage.store.processed import InMemoryProcessedLog
from triage.tools.inbox import GraphInbox
from triage.tools.mail_filter import MailFilter

PBI = "no-reply-powerbi@microsoft.com"
NOISE = "newsletter@example.com"


def _message(index: int, sender: str, subject: str) -> dict[str, Any]:
    return {
        "id": f"msg-{index:04d}",
        "subject": subject,
        "receivedDateTime": f"2026-09-0{1 + index % 9}T06:00:00Z",
        "from": {"emailAddress": {"address": sender}},
        "body": {"content": "Workspace: ws-1\nDataset: ds-1\nError code: Timeout"},
    }


class FakeGraphPages:
    """Serves a mailbox as Graph pages, and records how many were requested."""

    def __init__(self, messages: list[dict[str, Any]], page_size: int = 50):
        self.messages = messages
        self.page_size = page_size
        self.requested: list[str] = []

    async def __aenter__(self) -> FakeGraphPages:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def get(self, url: str, headers: dict[str, str] | None = None) -> FakeGraphPages:
        self.requested.append(url)
        offset = 0
        if "$skiptoken=" in url:
            offset = int(url.rsplit("$skiptoken=", 1)[1])
        page = self.messages[offset : offset + self.page_size]
        nxt = offset + self.page_size
        self._payload = {"value": page}
        if nxt < len(self.messages):
            self._payload["@odata.nextLink"] = f"https://graph.test/messages?$skiptoken={nxt}"
        return self

    # The response half of the same object, so one fake serves both roles.
    status_code = 200
    text = ""

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        return None


@pytest.fixture()
def patched_httpx(monkeypatch: pytest.MonkeyPatch):
    """Swap httpx.AsyncClient for the fake, without touching the network."""

    def install(fake: FakeGraphPages) -> FakeGraphPages:
        import httpx

        monkeypatch.setattr(httpx, "AsyncClient", lambda **_: fake)
        return fake

    return install


def _inbox() -> GraphInbox:
    inbox = GraphInbox(
        tenant_id="t",
        client_id="c",
        client_secret="s",
        mailbox="bi-alerts@contoso.com",
        mail_filter=MailFilter.build(senders=PBI, subject_pattern=r"(?i)refresh"),
        processed_log=InMemoryProcessedLog(),
    )
    # No token round-trip: this test is about paging, not authentication.
    inbox._token = "fake"  # noqa: SLF001
    inbox._token_expires_at = 9_999_999_999.0  # noqa: SLF001
    return inbox


async def test_a_wall_of_ignored_mail_does_not_hide_a_real_alert(patched_httpx) -> None:
    """The regression, stated as data: 60 newsletters on top of one alert.

    Under the old single-page fetch this returned nothing, for ever. The alert
    is beyond both the old page size and the first page of the new one, so a
    fix that merely enlarged the page would still fail this.
    """
    messages = [_message(i, NOISE, "Weekly newsletter") for i in range(60)]
    messages.append(_message(999, PBI, "Power BI: Refresh failed for 'Sales Daily Summary'"))
    fake = patched_httpx(FakeGraphPages(messages))

    found = await _inbox().fetch(limit=10)

    assert len(found) == 1, "the alert under 60 ignored messages was not reached"
    assert found[0].sender == PBI
    assert len(fake.requested) > 1, "should have followed @odata.nextLink"


async def test_paging_stops_as_soon_as_the_limit_is_satisfied(patched_httpx) -> None:
    """Cheap when mail is plentiful: one page is enough for ten alerts."""
    messages = [
        _message(i, PBI, f"Power BI: Refresh failed for 'Model {i}'") for i in range(40)
    ]
    fake = patched_httpx(FakeGraphPages(messages))

    found = await _inbox().fetch(limit=10)

    assert len(found) == 10
    assert len(fake.requested) == 1, "took more pages than it needed"


async def test_paging_is_bounded_on_a_mailbox_with_nothing_to_find(patched_httpx) -> None:
    """A huge irrelevant mailbox must not make one sweep unbounded."""
    messages = [_message(i, NOISE, "Weekly newsletter") for i in range(5_000)]
    fake = patched_httpx(FakeGraphPages(messages))

    found = await _inbox().fetch(limit=10)

    assert found == []
    assert len(fake.requested) == GraphInbox._MAX_PAGES  # noqa: SLF001


async def test_already_triaged_mail_does_not_block_new_mail_either(patched_httpx) -> None:
    """The other half of the same bug: processed messages also filled the page."""
    processed = InMemoryProcessedLog()
    messages = [
        _message(i, PBI, f"Power BI: Refresh failed for 'Model {i}'") for i in range(60)
    ]
    for msg in messages:
        processed.mark(msg["id"])
    messages.append(_message(999, PBI, "Power BI: Refresh failed for 'New One'"))

    inbox = _inbox()
    inbox._processed = processed  # noqa: SLF001
    patched_httpx(FakeGraphPages(messages))

    found = await inbox.fetch(limit=10)

    assert len(found) == 1
    assert json.loads(json.dumps(found[0].request_id)) == "msg-0999"
