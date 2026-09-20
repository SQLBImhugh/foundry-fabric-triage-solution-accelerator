"""Atomic file replacement for the offline JSON stores.

Windows is the reason this module exists. ``os.replace`` is atomic on NTFS, but
it fails with ``ERROR_ACCESS_DENIED`` (WinError 5) or ``ERROR_SHARING_VIOLATION``
(WinError 32) when an antivirus scanner, search indexer or backup agent happens
to hold either file open for the moment between creating the temporary file and
renaming it. That produced random offline-suite failures in
``JsonFileCommandCenterStore._persist`` while the same tests passed when run on
their own, which is the worst possible first impression for someone evaluating
this accelerator.

The retry window is short and bounded. A permission error that survives it is
re-raised unchanged, so a real failure is never turned into silent success.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

logger = logging.getLogger("triage.store.atomic")

_ATTEMPTS = 10
_DELAY_SECONDS = 0.05


def replace_atomically(source: Path, target: Path) -> None:
    """Rename ``source`` over ``target``, retrying transient Windows failures."""
    for attempt in range(1, _ATTEMPTS + 1):
        try:
            source.replace(target)
            return
        except PermissionError:
            if attempt == _ATTEMPTS:
                raise
            logger.debug(
                "Retrying atomic replace of %s after a transient permission error (attempt %d)",
                target,
                attempt,
            )
            time.sleep(_DELAY_SECONDS)
