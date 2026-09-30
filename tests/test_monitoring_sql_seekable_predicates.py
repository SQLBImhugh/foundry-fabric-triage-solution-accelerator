"""Kernel lookups on large tables must let SQL Server seek, not scan.

On the MorkNet database (S1, 20 DTU) every mutating kernel procedure holds the
control-row lock until its transaction commits, so a scanning statement both
spends the shared CPU and makes every other caller wait. Query Store over two
hours showed 13.8 million ms of control-row lock waits and these causes:

- The replay lookup compared the VARCHAR ``operation`` column with an N'...'
  literal. Under SQL_Latin1_General_CP1_CI_AS that forbids the seek on the
  receipts key, so each call read about 76,700 pages; 12 lookups took 15.9 s
  and 0 ms with a VARCHAR literal, returning the same rows.
- Receipt lookups matched ``request_id`` without the ``request_hash`` key.
- Source publication matched its accepted-evidence binding and its batch's
  handoff by JSON: 6.8 s per call. Seeking the binding by
  ``accepted_fact_key_hash``, the batch receipt by its key and the handoff
  through the frontier that receipt recorded takes 2.8 ms, with the same
  answers for 80 live evidence/work pairs.
- A batch read its own bindings by JSON batch id: 1.34 s per accepted page,
  2.8 ms through the parent index, with identical evidence for 40 live batches.
- Lookups by ``full_key`` without ``key_hash`` read every record of the kind.

Every added hash predicate is implied by the text predicate it accompanies:
stored hashes equal ``key_hash()`` of their text columns (the writable views
enforce this, and a live check found 0 exceptions in 144,921 records and
142,500 receipts). The offline suite cannot see query plans; the live proof is
the Query Store measurement recorded with this change.
"""

from __future__ import annotations

import re

from triage.monitoring.sql_permissions import build_permission_kernel

KERNEL = build_permission_kernel()
RECORDS = KERNEL.names.table("monitoring_records")
RECEIPTS = KERNEL.names.table("monitoring_receipts")
VARCHAR_COLUMNS = ("record_kind", "status", "work_kind", "workload", "operation")
#: Record kinds with thousands of rows per tenant, read on every reconciliation.
LARGE_KINDS = ("work", "validation_window", "validation_frontier", "frontier_commit", "validation_handoff")


def _statements(kinds: tuple[str, ...] = ("procedure", "view")):
    for obj in KERNEL.objects:
        if obj.kind in kinds:
            for statement in re.split(r";\s*\n", obj.ddl):
                yield obj.logical_name, statement


def _flat(statement: str) -> str:
    return " ".join(statement.split())[:220]


def test_no_varchar_column_is_compared_with_an_nvarchar_literal():
    pattern = re.compile(
        r"(?:(?<=\.)|(?<![@\w.]))(?:" + "|".join(VARCHAR_COLUMNS) + r")\s*(?:=|<>|(?:NOT\s+)?IN\s*\()\s*N'",
        re.I,
    )
    offenders = [
        (name, match.group(0)) for name, statement in _statements() for match in pattern.finditer(statement)
    ]
    assert offenders == []


def test_every_receipt_lookup_by_request_id_also_seeks_its_request_hash():
    offenders = []
    for name, statement in _statements():
        if RECEIPTS not in statement and "receipts_" not in statement:
            continue
        for match in re.finditer(r"(?:(\w+)\.)?request_id\s*=(?!=)", statement):
            if match.group(1) is None and match.start() > 0 and re.match(r"[\w@.]", statement[match.start() - 1]):
                continue
            alias = match.group(1)
            hashed = rf"\b{alias}\.request_hash\s*=" if alias else r"(?<![\w.@])request_hash\s*="
            if not re.search(hashed, statement):
                offenders.append((name, alias, _flat(statement[max(0, match.start() - 120):match.end() + 60])))
    assert offenders == []


def test_full_key_lookups_of_large_record_kinds_also_seek_their_key_hash():
    offenders = []
    for name, statement in _statements(("procedure",)):
        if RECORDS not in statement:
            continue
        for match in re.finditer(r"(?:(\w+)\.)?record_kind\s*=\s*'(\w+)'", statement):
            alias, kind = match.groups()
            if kind not in LARGE_KINDS:
                continue
            prefix = rf"\b{alias}\." if alias else r"(?<![\w.@])"
            if re.search(prefix + r"full_key\s*=", statement) and not re.search(prefix + r"key_hash\s*=", statement):
                offenders.append((name, alias, kind, _flat(statement[match.start():match.start() + 200])))
    assert offenders == []


def test_source_publication_seeks_its_evidence_record_and_binding():
    for operation in ("controller.publish_source", "controller.disposition_source"):
        ddl = next(obj.ddl for obj in KERNEL.objects if obj.logical_name == operation)
        lookup = ddl[ddl.index("SELECT @accepted_payload=raw.payload"):]
        lookup = lookup[:lookup.index("IF @accepted_payload IS NULL")]
        assert "raw.key_hash=" in lookup, operation
        assert "binding.accepted_fact_key_hash=raw.key_hash" in lookup, operation
        assert "own_handoff.parent_hash=input_handoff.parent_hash" in lookup, operation
        # The batch's own handoff is found through the frontier its receipt recorded.
        assert "accepted_receipt.operation IN ('worker.accept_facts','worker.commit_positions','worker.record_heartbeat')" in lookup
        assert "input_handoff.parent_hash=HASHBYTES('SHA2_256', CONVERT(varchar(max), (JSON_VALUE(accepted_receipt.payload,'$.result.frontier_key'))" in lookup
        assert "input_handoff.sequence_number=TRY_CONVERT(bigint,JSON_VALUE(accepted_receipt.payload,'$.result.frontier_revision'))" in lookup


def test_every_frontier_receipt_records_the_frontier_its_handoff_was_written_with():
    """Source publication finds a batch's handoff through these two receipt fields."""
    producers = [obj for obj in KERNEL.objects if obj.kind == "procedure" and "@frontier_handoff" in obj.ddl]
    assert {obj.logical_name for obj in producers} == {
        "web.commit_intent", "worker.accept_facts", "worker.commit_positions", "worker.observe_connector",
    }
    for obj in producers:
        handoff = obj.ddl.index("@frontier_handoff nvarchar(max)=")
        results = [match.start() for match in re.finditer(r"SET @result=\(SELECT", obj.ddl) if match.start() > handoff]
        assert results, obj.logical_name
        result = obj.ddl[results[0]:obj.ddl.index("FOR JSON PATH", results[0])]
        assert "@frontier_key AS frontier_key" in result and "@frontier_revision AS frontier_revision" in result, (
            obj.logical_name
        )


def test_a_batch_reads_its_own_bindings_through_the_parent_index():
    ddl = next(obj.ddl for obj in KERNEL.objects if obj.logical_name == "worker.accept_facts")
    batch_reads = [
        statement for statement in re.split(r";\s*\n", ddl)
        if "record_kind='accepted_fact'" in statement and "'$.batch_id')=@request_id" in statement
    ]
    assert len(batch_reads) == 2
    for statement in batch_reads:
        assert "parent_hash=HASHBYTES('SHA2_256', CONVERT(varchar(max), (@work_id)" in statement
        assert "parent_key=@work_id" in statement
