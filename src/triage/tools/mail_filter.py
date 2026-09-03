"""Relevance filtering for inbound mail.

Why this is a security control and not housekeeping
---------------------------------------------------
An agent that triages every message in a mailbox is steerable by anyone who can
send mail to that mailbox. There is no authentication on an inbox: the sender
field is trivially forged, and the body goes straight into a model prompt. An
unfiltered agent inbox is a prompt-injection surface with an open door.

This was not theoretical. Pointed at a real mailbox with no filter, the demo
triaged Microsoft Entra ID Protection and PIM weekly digests, and crashed on
several of them -- burning model calls and filling the incident store with
noise that an operator would have to read.

The filter fails closed: a message must look like a Power BI refresh alert to
be acted on. Everything else is skipped and counted, never silently dropped.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger("triage.inbox.filter")


@dataclass(frozen=True)
class MailFilter:
    """Decides whether a message is a Power BI alert worth triaging."""

    sender_allowlist: frozenset[str]
    subject_pattern: re.Pattern[str] | None

    @classmethod
    def build(cls, *, senders: str, subject_pattern: str) -> MailFilter:
        allow = frozenset(
            s.strip().lower() for s in (senders or "").split(",") if s.strip()
        )
        pattern: re.Pattern[str] | None = None
        if subject_pattern:
            try:
                pattern = re.compile(subject_pattern)
            except re.error as exc:
                # A bad pattern must not silently disable the control.
                logger.error(
                    "Invalid subject pattern %r (%s); rejecting all mail until fixed",
                    subject_pattern,
                    exc,
                )
                pattern = re.compile(r"(?!)")  # matches nothing
        return cls(sender_allowlist=allow, subject_pattern=pattern)

    def accepts(self, *, sender: str, subject: str) -> tuple[bool, str]:
        """Return (accepted, reason). Reason explains a rejection."""
        address = (sender or "").strip().lower()

        if self.sender_allowlist and address not in self.sender_allowlist:
            return False, f"sender {address or '(none)'} not on the allowlist"

        if self.subject_pattern is not None and not self.subject_pattern.search(subject or ""):
            return False, "subject does not look like a Power BI refresh alert"

        return True, ""
