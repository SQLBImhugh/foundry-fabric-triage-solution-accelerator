"""Secret redaction applied at the persistence boundary.

Error text from a BI platform routinely contains SAS tokens, connection
strings and bearer tokens. Anything that reaches durable storage — the
incident record, a Teams message, a trace attribute — goes through
:func:`redact` first.

Enforced at the *boundary* (the store), not at the call sites, so a new code
path cannot forget to call it.

Ported from a production Fabric operations platform's incident redaction module.
"""

from __future__ import annotations

import re

MAX_CHARS = 4000
_PLACEHOLDER = "[REDACTED:{name}]"

_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "azure_sas",
        # Raw and URL-encoded signatures both appear in Power BI error text.
        re.compile(r"[?&]s(?:i)?g=[A-Za-z0-9%+/=_-]{16,}", re.IGNORECASE),
    ),
    (
        "storage_key",
        re.compile(r"AccountKey=[A-Za-z0-9+/=]{40,}", re.IGNORECASE),
    ),
    (
        "cosmos_key",
        # 64 raw bytes base64-encoded is 88 chars ending in '=='. The lookbehind
        # deliberately excludes '=' so a legitimate `Key=<value>` prefix still
        # matches — an earlier version anchored on '=' too and never fired.
        re.compile(r"(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{86}==(?![A-Za-z0-9+/=])"),
    ),
    (
        "basic_auth",
        re.compile(r"Basic\s+[A-Za-z0-9+/]{8,}={0,2}", re.IGNORECASE),
    ),
    (
        "sql_conn",
        # Quoted and unquoted forms; Power BI surfaces both in error payloads.
        re.compile(
            r"(Password|Pwd)\s*=\s*(\"[^\"]{3,}\"|'[^']{3,}'|[^;\s\"']{3,})",
            re.IGNORECASE,
        ),
    ),
    (
        "db_uri_creds",
        re.compile(r"\b[a-z][a-z0-9+.-]*://[^:/\s]+:[^@/\s]+@", re.IGNORECASE),
    ),
    (
        "bearer_token",
        re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]{20,}", re.IGNORECASE),
    ),
    (
        "jwt",
        re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),
    ),
    (
        "aws_access_key",
        re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),
    ),
    (
        "github_token",
        re.compile(r"\b(gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    ),
    (
        "pem_private_key",
        re.compile(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
            re.DOTALL,
        ),
    ),
    (
        "generic_secret_kv",
        # Covers snake_case, camelCase, kebab-case and JSON quoted keys, with
        # either `=` or `:` as the separator, value optionally quoted.
        re.compile(
            r"[\"']?\b("
            r"client[_-]?[Ss]ecret|clientSecret"
            r"|api[_-]?[Kk]ey|apiKey"
            r"|access[_-]?[Tt]oken|accessToken"
            r"|refresh[_-]?[Tt]oken|refreshToken"
            r"|connection[_-]?[Ss]tring|connectionString"
            r"|secret|password|pwd"
            r")\b[\"']?\s*[:=]\s*[\"']?([A-Za-z0-9._~+/=-]{8,})[\"']?",
            re.IGNORECASE,
        ),
    ),
]


def redact(text: str | None) -> tuple[str, list[str]]:
    """Return ``(redacted_text, fired_pattern_names)``.

    Always truncates to :data:`MAX_CHARS`. Truncation happens *after*
    redaction so a secret near the end of a long message can't survive by
    being cut in half into something the patterns no longer match.
    """
    if not text:
        return "", []

    fired: list[str] = []
    out = str(text)
    for name, pattern in _PATTERNS:
        out, count = pattern.subn(_PLACEHOLDER.format(name=name), out)
        if count:
            fired.append(name)

    if len(out) > MAX_CHARS:
        out = out[:MAX_CHARS] + f"... [truncated {len(out) - MAX_CHARS} chars]"

    return out, fired


def redact_text(text: str | None) -> str:
    """Convenience wrapper when the audit list isn't needed."""
    return redact(text)[0]
