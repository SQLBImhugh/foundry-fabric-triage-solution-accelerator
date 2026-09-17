"""Deterministic duplicate detection.

The Data Quality *agent* interprets and reports. It does not discover. The
discovery is this module: a plain CSV scan with no model in the loop.

That split is why Scenario 2 is reproducible on stage. An LLM asked to "find
duplicates" will find a different number of them on the third rehearsal.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path

from triage.models import DuplicateEvidence


@dataclass
class DatasetSource:
    """A table the Data Quality agent is allowed to inspect."""

    name: str
    path: Path
    key_columns: list[str] = field(default_factory=list)

    def exists(self) -> bool:
        return self.path.exists()


def read_rows(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def detect_duplicates(
    *,
    path: str | Path,
    key_columns: list[str],
    table_name: str,
    sample_limit: int = 5,
) -> DuplicateEvidence:
    """Count rows sharing a composite key.

    Returns key values and counts only — never full rows. The demo data is
    synthetic, but the customer will copy this shape into an environment where
    the rows are not.
    """
    rows = read_rows(path)
    total = len(rows)

    if not key_columns:
        return DuplicateEvidence(
            table=table_name, key_columns=[], total_row_count=total
        )

    groups: dict[tuple[str, ...], int] = {}
    for row in rows:
        key = tuple((row.get(col) or "").strip() for col in key_columns)
        groups[key] = groups.get(key, 0) + 1

    dupe_groups = {k: n for k, n in groups.items() if n > 1}
    # "Duplicate rows" = the redundant copies, not the whole group. A key
    # appearing 3 times contributes 2 duplicates, not 3.
    duplicate_rows = sum(n - 1 for n in dupe_groups.values())

    samples = [
        " | ".join(f"{col}={val}" for col, val in zip(key_columns, key, strict=True))
        for key in list(dupe_groups)[:sample_limit]
    ]

    return DuplicateEvidence(
        table=table_name,
        key_columns=list(key_columns),
        duplicate_group_count=len(dupe_groups),
        duplicate_row_count=duplicate_rows,
        total_row_count=total,
        sample_keys=samples,
    )


def render_table(path: str | Path, limit: int = 20) -> str:
    """Render a CSV for on-screen display — used for the before/after beat."""
    return render_rows(read_rows(path), limit)


def render_rows(rows: list[dict[str, str]], limit: int = 20) -> str:
    """Render table rows without requiring a local file."""
    if not rows:
        return "(empty)"

    headers = list(rows[0].keys())
    widths = {
        h: max(len(h), *(len(str(r.get(h, ""))) for r in rows[:limit])) for h in headers
    }
    out = [" | ".join(h.ljust(widths[h]) for h in headers)]
    out.append("-+-".join("-" * widths[h] for h in headers))
    for row in rows[:limit]:
        out.append(" | ".join(str(row.get(h, "")).ljust(widths[h]) for h in headers))
    if len(rows) > limit:
        out.append(f"... {len(rows) - limit} more row(s)")
    return "\n".join(out)
