"""Tests for the credential scanner that runs in CI.

This scanner is the last thing standing between a public push and a leaked
secret, and it shipped broken: a leading word boundary meant
``GRAPH_CLIENT_SECRET=...`` never matched, because the underscore before
``CLIENT`` is itself a word character. It reported "clean" while being
incapable of detecting the single most likely leak.

A safety check that has never caught anything is not a safety check. These
tests exist so that stays true.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

scan_secrets = pytest.importorskip("scan_secrets")


def _detects(text: str) -> bool:
    return any(
        not scan_secrets.PLACEHOLDER.match(m.group("value"))
        for m in scan_secrets.SECRET_ASSIGNMENT.finditer(text)
    )


@pytest.mark.parametrize(
    "text",
    [
        # The exact form that slipped through: a prefixed environment variable.
        "GRAPH_CLIENT_SECRET=Abc123FakeSecretValue~9xQ",
        "POWERBI_CLIENT_SECRET=Zz9~anotherFakeSecret456",
        "client_secret: Xy9~verySecretValue123",
        "AZURE_OPENAI_API_KEY=sk-fake1234567890abcdef",
        'CONNECTION_STRING="InstrumentationKey=00000000-1111-2222-3333-444444444444"',
    ],
)
def test_real_looking_credentials_are_detected(text: str) -> None:
    assert _detects(text), f"scanner missed: {text}"


@pytest.mark.parametrize(
    "text",
    [
        # A template with no value is the whole point of .env.example.
        "GRAPH_CLIENT_SECRET=",
        "GRAPH_CLIENT_SECRET=<your-secret>",
        "GRAPH_CLIENT_SECRET=${GRAPH_CLIENT_SECRET}",
        # Documentation legitimately discusses these words.
        "passwordCredentials=0",
        "the client_secret is stored in the azd environment",
        "keyCredentials=0, passwordCredentials=0",
        # An empty assignment must not swallow the following line. This is the
        # shape of .env.example, and matching across the newline reported a
        # blank template as a leak.
        "GRAPH_CLIENT_SECRET=\nGRAPH_MAILBOX=bi-alerts@contoso.com",
        "TEAMS_WEBHOOK_URL=\nTEAMS_MODE=webhook",
    ],
)
def test_benign_mentions_are_not_flagged(text: str) -> None:
    """A scanner that cries wolf gets switched off by the first person in a hurry."""
    assert not _detects(text), f"scanner false-positived on: {text}"


def test_workflows_webhook_url_is_treated_as_a_credential() -> None:
    """The URL *is* the credential -- anyone holding it can post to the channel."""
    old_format = (
        "https://prod-12.westus.logic.azure.com/workflows/abc/triggers/"
        "manual/paths/invoke?sig=FAKEsigVALUE123"
    )
    # The format actually issued by Teams today. A scanner that only knew the
    # logic.azure.com form would have let this straight through.
    new_format = (
        "https://defaultedf144.f9.environment.api.powerplatform.com:443/powerautomate/"
        "automations/direct/cu/07/workflows/abc/triggers/manual/paths/invoke"
        "?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=FAKEsigVALUE123"
    )
    assert scan_secrets.WEBHOOK_URL.search(old_format)
    assert scan_secrets.WEBHOOK_URL.search(new_format)


def test_the_approval_callback_url_is_treated_as_a_credential() -> None:
    """Anyone holding this link can answer an approval.

    Caught twice by the URL rule and once by the name rule. The name rule
    matters on its own: a callback that never grew a ``sig`` parameter, or one
    behind a different host, would still be a credential in a config line.
    """
    url = (
        "https://prod-52.eastus.logic.azure.com:443/workflows/abc/triggers/manual/"
        "paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun"
        "&sv=1.0&sig=FAKEsigVALUE123"
    )
    assert scan_secrets.WEBHOOK_URL.search(url)
    assert _detects(f'APPROVAL_CALLBACK_URL="{url}"')
    assert _detects("approval_callback_url: https://example.invalid/abcdefghijklmnop")

    # ...and an unset one is still not a leak.
    assert not _detects('APPROVAL_CALLBACK_URL=""')
    assert not _detects("APPROVAL_CALLBACK_URL=<your-callback-url>")


def test_bearer_tokens_are_detected() -> None:
    assert scan_secrets.BEARER_TOKEN.search(
        "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9fake"
    )
