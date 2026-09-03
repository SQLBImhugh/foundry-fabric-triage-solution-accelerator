"""Redaction runs at the persistence boundary. If it leaks, everything downstream leaks."""

from __future__ import annotations

import pytest

from triage.redaction import MAX_CHARS, redact, redact_text

LEAKY_INPUTS = [
    ("azure_sas", "https://acct.blob.core.windows.net/c/b?sv=2021&sig=AbCdEf0123456789XyZ%2Fabc="),
    ("storage_key", "AccountKey=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOP=="),
    ("sql_conn", "Server=tcp:x.database.windows.net;User ID=admin;Password=Hunter2Hunter2;"),
    ("db_uri_creds", "postgresql://admin:s3cr3tpass@db.internal:5432/warehouse"),
    ("bearer_token", "Authorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789"),
    (
        "jwt",
        "token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
    ),
    ("aws_access_key", "aws key AKIAIOSFODNN7EXAMPLE in the log"),
    ("github_token", "token ghp_1234567890abcdefghijklmnopqrstuvwx"),
    ("generic_secret_kv", 'client_secret="abc123def456ghi789jkl"'),
    # --- added after review: realistic Power BI / Azure error payloads -----
    (
        "cosmos_key",
        "AccountEndpoint=https://x.documents.azure.com:443/;Key="
        + "A" * 86
        + "==;",
    ),
    ("basic_auth", "Authorization: Basic YWRtaW46aHVudGVyMnBhc3N3b3Jk"),
    ("generic_secret_kv", '{"clientSecret": "abc123def456ghi789jkl"}'),
    ("generic_secret_kv", '{"accessToken":"eyJabc123def456ghi789"}'),
    ("sql_conn", 'Server=tcp:x.database.windows.net;Password="Hunter2Hunter2";'),
    ("sql_conn", "Server=tcp:x.database.windows.net;Pwd='S3cr3tValue';"),
    (
        "azure_sas",
        "https://acct.blob.core.windows.net/c/b?sv=2021&sig=AbCdEf0123456789%2Fabc%3D",
    ),
]


@pytest.mark.parametrize(("name", "text"), LEAKY_INPUTS, ids=[n for n, _ in LEAKY_INPUTS])
def test_secret_is_redacted(name: str, text: str) -> None:
    redacted, fired = redact(text)
    assert name in fired, f"{name} pattern did not fire on: {text}"
    assert f"[REDACTED:{name}]" in redacted


def test_pem_private_key_is_redacted_across_lines() -> None:
    text = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MIIEowIBAAKCAQEA1234\nabcd\n"
        "-----END RSA PRIVATE KEY-----"
    )
    redacted, fired = redact(text)
    assert "pem_private_key" in fired
    assert "MIIEowIBAAKCAQEA" not in redacted


def test_clean_text_is_untouched() -> None:
    text = "RefreshError: the operation was cancelled after 1800 seconds."
    redacted, fired = redact(text)
    assert redacted == text
    assert fired == []


def test_truncation_happens_after_redaction() -> None:
    """A secret near the end must not survive by being cut into a non-matching fragment."""
    padding = "x" * (MAX_CHARS - 20)
    text = padding + " ghp_1234567890abcdefghijklmnopqrstuvwx"
    redacted, fired = redact(text)
    assert "github_token" in fired
    assert "ghp_1234567890" not in redacted


def test_output_is_capped() -> None:
    redacted, _ = redact("y" * (MAX_CHARS * 3))
    assert len(redacted) <= MAX_CHARS + 64


def test_empty_and_none_are_safe() -> None:
    assert redact(None) == ("", [])
    assert redact("") == ("", [])
    assert redact_text(None) == ""


def test_multiple_secrets_all_fire() -> None:
    text = "Bearer abcdefghijklmnopqrstuvwxyz0123456789 and AKIAIOSFODNN7EXAMPLE"
    _, fired = redact(text)
    assert {"bearer_token", "aws_access_key"} <= set(fired)


def test_redaction_does_not_destroy_the_surrounding_error() -> None:
    """Over-redaction is its own failure: an unreadable error helps nobody."""
    text = (
        "RefreshError: the gateway rejected the credential for "
        "Server=tcp:x.database.windows.net;Password=Hunter2Hunter2; "
        "while loading table daily_sales at 2026-08-26T05:00:04Z"
    )
    redacted, fired = redact(text)

    assert "sql_conn" in fired
    assert "Hunter2Hunter2" not in redacted
    for preserved in ("RefreshError", "daily_sales", "x.database.windows.net"):
        assert preserved in redacted, f"redaction destroyed '{preserved}'"
