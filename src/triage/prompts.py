"""Prompt loading with a version hash for provenance."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path

_PROMPT_DIR = Path(__file__).parent / "agents" / "prompts"


@lru_cache(maxsize=16)
def load_prompt(name: str) -> str:
    path = _PROMPT_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Prompt '{name}' not found at {path}")
    return path.read_text(encoding="utf-8")


@lru_cache(maxsize=16)
def prompt_version_hash(name: str) -> str:
    """Short content hash — recorded on every incident.

    When behaviour changes between two runs, this is how you tell whether the
    prompt moved or the model did.
    """
    return hashlib.sha256(load_prompt(name).encode("utf-8")).hexdigest()[:12]
