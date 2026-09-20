"""Atomic replacement retries transient Windows failures without hiding real ones."""

from __future__ import annotations

from pathlib import Path

import pytest

from triage.store import atomic
from triage.store.atomic import replace_atomically


def test_replace_succeeds_without_retrying(tmp_path: Path) -> None:
    source = tmp_path / "source.tmp"
    target = tmp_path / "target.json"
    source.write_text("after", encoding="utf-8")
    target.write_text("before", encoding="utf-8")

    replace_atomically(source, target)

    assert target.read_text(encoding="utf-8") == "after"
    assert not source.exists()


def test_transient_permission_error_is_retried(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source.tmp"
    target = tmp_path / "target.json"
    source.write_text("after", encoding="utf-8")
    target.write_text("before", encoding="utf-8")

    real_replace = Path.replace
    attempts: list[int] = []

    def flaky(self: Path, other: Path) -> Path:
        attempts.append(1)
        if len(attempts) < 3:
            raise PermissionError(5, "Access is denied")
        return real_replace(self, other)

    monkeypatch.setattr(Path, "replace", flaky)
    monkeypatch.setattr(atomic, "_DELAY_SECONDS", 0)

    replace_atomically(source, target)

    assert len(attempts) == 3
    assert target.read_text(encoding="utf-8") == "after"


def test_persistent_permission_error_still_raises(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source.tmp"
    target = tmp_path / "target.json"
    source.write_text("after", encoding="utf-8")
    target.write_text("before", encoding="utf-8")

    attempts: list[int] = []

    def always_denied(self: Path, other: Path) -> Path:
        attempts.append(1)
        raise PermissionError(5, "Access is denied")

    monkeypatch.setattr(Path, "replace", always_denied)
    monkeypatch.setattr(atomic, "_DELAY_SECONDS", 0)

    with pytest.raises(PermissionError):
        replace_atomically(source, target)

    assert len(attempts) == atomic._ATTEMPTS
    assert target.read_text(encoding="utf-8") == "before"


def test_other_os_errors_are_not_retried(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source.tmp"
    target = tmp_path / "target.json"
    source.write_text("after", encoding="utf-8")

    attempts: list[int] = []

    def missing(self: Path, other: Path) -> Path:
        attempts.append(1)
        raise FileNotFoundError(2, "No such file")

    monkeypatch.setattr(Path, "replace", missing)
    monkeypatch.setattr(atomic, "_DELAY_SECONDS", 0)

    with pytest.raises(FileNotFoundError):
        replace_atomically(source, target)

    assert len(attempts) == 1
