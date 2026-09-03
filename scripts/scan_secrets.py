"""Fail if a credential is about to be published.

Runs in CI on every change. It answers one question: if this repository were
pushed right now, would a live credential go with it?

It scans **git-tracked files only**, because that is the question being asked.
A local `.env` or `.azure/` holds real secrets by design and is gitignored;
flagging them every run trains people to ignore the output.

Usage:
    python scripts/scan_secrets.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# A credential is a secret-ish key with a real value after it, not the mention
# of one. The docs legitimately discuss client secrets and say
# "passwordCredentials=0", and a scanner that cannot tell those apart gets
# switched off by the first person in a hurry -- which is exactly when it is
# needed.
SECRET_ASSIGNMENT = re.compile(
    r"""(?ix)
    # No leading \b: real names are prefixed, e.g. GRAPH_CLIENT_SECRET. A word
    # boundary before "client_secret" never matches there, because the
    # preceding underscore is itself a word character. That bug made this
    # scanner report "clean" while being incapable of detecting the single most
    # likely leak, which is worse than having no scanner at all.
    (client[_-]?secret | api[_-]?key | password | webhook[_-]?url | callback[_-]?url
     | connection[_-]?string)
    # Horizontal whitespace only. \s* spans newlines, which made an *empty*
    # "GRAPH_CLIENT_SECRET=" swallow the following line and report the next
    # setting as its value -- flagging a blank template as a leak.
    [ \t]* [=:] [ \t]*
    ["']?
    (?P<value> [^\s"'<>${},]{12,} )      # 12+ chars, not a placeholder
    """,
)

# A Workflows webhook URL is itself the credential. Two formats in the wild:
# the older logic.azure.com one, and the newer Power Platform environment host.
# The new format carries the same 'sig' bearer parameter and would have sailed
# past a check that only knew about the old one. The approval callback is the
# same shape again -- anyone holding that link can answer an approval.
WEBHOOK_URL = re.compile(
    r"https://[^\s\"']*(?:logic\.azure\.com|powerplatform\.com)[^\s\"']*sig=[^\s\"'&]+",
    re.I,
)
BEARER_TOKEN = re.compile(r"\bBearer\s+ey[A-Za-z0-9._-]{20,}")

# Placeholders that look like values but are not.
PLACEHOLDER = re.compile(
    r"^(your|placeholder|example|changeme|xxx+|\.\.\.|<.*>|\$\{.*\}|none|null|n/?a)$", re.I
)

#: A value that is obviously code rather than a credential. ``client_secret=
#: self._client_secret`` is a parameter being passed and ``password=
#: load_agent_identity(`` is a function call, not a secret being written down.
#: A scanner that cannot tell the difference produces pages of noise, which is
#: how it ends up ignored or switched off.
CODE_LIKE = re.compile(r"^[A-Za-z_][\w.]*(\[[^\]]*\])?[(),]?$")

#: Hosts reserved by RFC 2606 and RFC 6761 for documentation and testing. A URL
#: pointing at one cannot be a live credential, because the domain cannot exist.
RESERVED_HOST = re.compile(
    r"^https?://(localhost|127\.0\.0\.1|\[::1\]|[\w.-]*example\.(com|net|org|invalid|test))",
    re.I,
)

#: Files whose whole purpose is to contain realistic fake secrets: the scanner's
#: own patterns, its tests, and the redaction corpus. A check that flags its own
#: test fixtures is one somebody turns off.
SKIP_FILES = {"scan_secrets.py", "test_secret_scanner.py", "test_redaction.py"}


def scan_tree(root: Path) -> list[str]:
    """Return one line per suspected credential, empty when the tree is clean."""
    try:
        tracked = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=str(root), capture_output=True, text=True, check=True,
        ).stdout.split("\0")
    except (subprocess.CalledProcessError, FileNotFoundError):
        # No git available: fall back to walking the tree. Better to over-report
        # than to report "clean" because the enumeration failed.
        tracked = [str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()]

    leaks: list[str] = []
    for rel in tracked:
        if not rel or Path(rel).name in SKIP_FILES or Path(rel).suffix in {".png", ".svg"}:
            continue
        path = root / rel
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        for match in SECRET_ASSIGNMENT.finditer(text):
            value = match.group("value")
            if (
                PLACEHOLDER.match(value)
                or CODE_LIKE.match(value)
                or RESERVED_HOST.match(value)
            ):
                continue
            leaks.append(f"{rel}: {match.group(1)} has a value")
        for pattern, label in ((WEBHOOK_URL, "webhook URL"), (BEARER_TOKEN, "bearer token")):
            if pattern.search(text):
                leaks.append(f"{rel}: {label}")

    return list(dict.fromkeys(leaks))


def main() -> int:
    leaks = scan_tree(REPO)
    if leaks:
        print("Possible credentials in tracked files:")
        for leak in leaks:
            print(f"  {leak}")
        print("\nRemove them, or add the file to SKIP_FILES if it holds test fixtures.")
        return 1
    print("No credentials found in tracked files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
