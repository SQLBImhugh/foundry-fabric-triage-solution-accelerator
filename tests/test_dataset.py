"""Duplicate detection is deterministic. These tests pin the arithmetic."""

from __future__ import annotations

from triage.tools.dataset import detect_duplicates, read_rows, render_table


def test_seeded_duplicates_are_counted_exactly(repo_root) -> None:
    """The demo depends on these numbers. If they drift, the run sheet is wrong."""
    evidence = detect_duplicates(
        path=repo_root / "mock" / "data" / "daily_sales.csv",
        key_columns=["store_id", "sales_date"],
        table_name="daily_sales",
    )
    assert evidence.total_row_count == 14
    assert evidence.duplicate_group_count == 2
    assert evidence.duplicate_row_count == 4


def test_clean_dataset_has_no_duplicates(repo_root) -> None:
    evidence = detect_duplicates(
        path=repo_root / "mock" / "data" / "daily_sales_clean.csv",
        key_columns=["store_id", "sales_date"],
        table_name="daily_sales",
    )
    assert evidence.total_row_count == 10
    assert evidence.duplicate_group_count == 0
    assert evidence.duplicate_row_count == 0


def test_a_key_seen_three_times_contributes_two_duplicates(tmp_path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("k,v\na,1\na,2\na,3\nb,1\n", encoding="utf-8")

    evidence = detect_duplicates(path=path, key_columns=["k"], table_name="t")
    assert evidence.duplicate_group_count == 1
    assert evidence.duplicate_row_count == 2


def test_evidence_never_carries_row_contents(repo_root) -> None:
    """A privacy boundary: keys and counts leave the scan, values do not."""
    evidence = detect_duplicates(
        path=repo_root / "mock" / "data" / "daily_sales.csv",
        key_columns=["store_id", "sales_date"],
        table_name="daily_sales",
    )
    blob = evidence.model_dump_json()
    for leaked in ("Northgate", "1180", "Riverton", "Lakeside"):
        assert leaked not in blob


def test_sample_keys_are_capped(tmp_path) -> None:
    rows = "\n".join(f"k{i % 20},1\nk{i % 20},2" for i in range(60))
    path = tmp_path / "t.csv"
    path.write_text("k,v\n" + rows + "\n", encoding="utf-8")

    evidence = detect_duplicates(
        path=path, key_columns=["k"], table_name="t", sample_limit=5
    )
    assert len(evidence.sample_keys) == 5


def test_empty_key_columns_is_not_a_crash(tmp_path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("k,v\na,1\na,1\n", encoding="utf-8")

    evidence = detect_duplicates(path=path, key_columns=[], table_name="t")
    assert evidence.duplicate_row_count == 0
    assert evidence.total_row_count == 2


def test_headline_reads_correctly_at_zero(tmp_path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("k,v\na,1\nb,2\n", encoding="utf-8")
    evidence = detect_duplicates(path=path, key_columns=["k"], table_name="t")
    assert "No duplicate keys" in evidence.headline()


def test_bom_encoded_csv_is_readable(tmp_path) -> None:
    """Excel writes a BOM. A demo file round-tripped through Excel must still parse."""
    path = tmp_path / "t.csv"
    path.write_text("k,v\na,1\n", encoding="utf-8-sig")
    rows = read_rows(path)
    assert list(rows[0].keys()) == ["k", "v"]


def test_render_table_handles_empty(tmp_path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("k,v\n", encoding="utf-8")
    assert render_table(path) == "(empty)"
