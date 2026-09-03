"""Deterministic incident signatures — the "Is There a Known Related Issue?" branch.

Two occurrences of the same underlying failure must produce the same 16-char
signature so the second one increments a counter instead of firing a second
remediation. Without this, an agent wired to an inbox will happily refresh the
same dataset forty times during an outage.

Normalization strips the high-cardinality tokens that would otherwise make
every occurrence look unique: GUIDs, timestamps, line numbers, URL paths, IPs,
hex suffixes, temp paths, long hashes.

**Case is preserved on purpose** — SQL identifier case is significant in some
dialects and folding it merges genuinely distinct failures.

Ported from a production Fabric operations platform's incident signature module.
"""

from __future__ import annotations

import hashlib
import re

SIGNATURE_VERSION = "v1"

_GUID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
_TIMESTAMP = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?"
)
_LINE_NUMBER = re.compile(r"\b(line|Cell In)\s*\[?\d+\]?")
_URL_PATH = re.compile(r"https?://([^/\s]+)/[^\s]*")
_IP = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?\b")
_HEX_SUFFIX = re.compile(r"-[a-z0-9]{8,}\b")
_TEMP_PATH = re.compile(r"/tmp/[^\s]+")
_HEX_HASH = re.compile(r"\b[a-f0-9]{32,}\b", re.IGNORECASE)
_REQUEST_ID = re.compile(r"\b(request|correlation|activity)[ _-]?id[:=]\s*\S+", re.IGNORECASE)
_WHITESPACE = re.compile(r"\s+")
_EXCEPTION_CLASS = re.compile(r"\b([A-Z][A-Za-z0-9]+(?:Error|Exception))\b")


def normalize(error: str) -> str:
    """Collapse an error message to its stable, comparable core."""
    lines = [ln.strip() for ln in (error or "").splitlines() if ln.strip()]
    root_line = next(
        (ln for ln in lines if "Error" in ln or "Exception" in ln),
        lines[0] if lines else "",
    )

    normalized = root_line
    normalized = _GUID.sub("<guid>", normalized)
    normalized = _TIMESTAMP.sub("<ts>", normalized)
    normalized = _LINE_NUMBER.sub("<line>", normalized)
    normalized = _URL_PATH.sub(r"<url:\1>", normalized)
    normalized = _IP.sub("<ip>", normalized)
    normalized = _TEMP_PATH.sub("<tmp>", normalized)
    normalized = _HEX_HASH.sub("<hash>", normalized)
    normalized = _HEX_SUFFIX.sub("-<suffix>", normalized)
    normalized = _REQUEST_ID.sub("<reqid>", normalized)
    normalized = _WHITESPACE.sub(" ", normalized).strip()
    return normalized


def infer_exception_class(error: str) -> str | None:
    match = _EXCEPTION_CLASS.search(error or "")
    return match.group(1) if match else None


def compute_signature(
    *,
    source: str,
    error: str,
    artifact_kind: str = "dataset",
    artifact_name: str | None = None,
    exception_class: str | None = None,
) -> tuple[str, str]:
    """Return ``(signature, normalized_payload)``.

    ``source`` is the triage entry point (e.g. ``"powerbi_refresh_failure"``).
    ``artifact_name`` scopes the signature to one report/dataset so the same
    error class on two different reports stays two incidents — which is what
    an operator actually wants when deciding whether to suppress.

    The normalized payload is returned for debugging and is deliberately NOT
    persisted; it can contain fragments of the original message.
    """
    normalized = normalize(error)
    exception_class = exception_class or infer_exception_class(error) or ""

    payload = "|".join(
        [
            source or "",
            artifact_kind or "",
            (artifact_name or "").strip(),
            exception_class,
            normalized,
        ]
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]  # noqa: S324
    return digest, payload


def incident_id(signature: str, *, resolved: bool) -> str:
    """Deterministic store id.

    The ``sig:``/``usig:`` prefix split keeps the resolved and unresolved
    namespaces independent, so a successful fix and a prior crash for the same
    signature coexist as separate rows instead of overwriting each other.
    """
    return f"{'sig' if resolved else 'usig'}:{signature}"
