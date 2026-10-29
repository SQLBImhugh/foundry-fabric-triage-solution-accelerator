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
    for operation, parent in (
        ("worker.accept_facts", "@work_id"),
        ("worker.commit_positions", "@partition_key"),
    ):
        ddl = next(obj.ddl for obj in KERNEL.objects if obj.logical_name == operation)
        batch_reads = [
            statement for statement in re.split(r";\s*\n", ddl)
            if "record_kind='accepted_fact'" in statement and "'$.batch_id')=@request_id" in statement
        ]
        assert len(batch_reads) == 2, operation
        for statement in batch_reads:
            assert f"parent_hash=HASHBYTES('SHA2_256', CONVERT(varchar(max), ({parent})" in statement
            assert f"parent_key={parent}" in statement


def test_resolve_frontier_seeks_reconcile_request_handoff_and_evidence_bindings():
    ddl = next(obj.ddl for obj in KERNEL.objects if obj.logical_name == "controller.resolve_frontier")
    request_pos = ddl.index("SELECT @producer_request=payload")
    handoff_pos = ddl.index("SELECT @handoff=payload")
    assert request_pos < handoff_pos
    request_stmt = ddl[request_pos:ddl.index(";", request_pos)]
    assert "full_key=@producer_request_id" in request_stmt
    assert "key_hash=HASHBYTES('SHA2_256', CONVERT(varchar(max), (@producer_request_id)" in request_stmt
    handoff_stmt = ddl[handoff_pos:ddl.index(";", handoff_pos)]
    assert "parent_hash=HASHBYTES('SHA2_256', CONVERT(varchar(max), (JSON_VALUE(@producer_request,'$.frontier_key'))" in handoff_stmt
    assert "parent_key=JSON_VALUE(@producer_request,'$.frontier_key')" in handoff_stmt
    assert "sequence_number=TRY_CONVERT(bigint,JSON_VALUE(@producer_request,'$.frontier_revision'))" in handoff_stmt
    evidence_stmt = next(
        statement for statement in re.split(r";\s*\n", ddl)
        if "record_kind='accepted_fact'" in statement and "'$.batch_id')=@producer_request_id" in statement
    )
    assert "binding.parent_hash=HASHBYTES('SHA2_256', CONVERT(varchar(max), (@evidence_parent)" in evidence_stmt
    assert "binding.parent_key=@evidence_parent" in evidence_stmt
    for match in re.finditer(r"(?:(\w+)\.)?record_kind\s*=\s*'validation_handoff'", ddl):
        alias = match.group(1)
        prefix = rf"\b{alias}\." if alias else r"(?<![\w.@])"
        window = ddl[match.start():match.start() + 320]
        if re.search(prefix + r"parent_key\s*=", window):
            assert re.search(prefix + r"parent_hash\s*=", window) or re.search(prefix + r"key_hash\s*=", window), _flat(window)


def test_every_receipt_lookup_by_operation_uses_varchar_and_request_hash():
    from triage.monitoring.sql_kernel_frontiers import (
        nonwindow_handoff_authority_sql,
        pending_window_handoff_authority_sql,
    )
    from triage.monitoring.sql_kernel_work import reconciliation_completion_sql

    uncast_json = re.compile(
        r"\b(?:WHERE|AND|OR|ON|WHEN)\s+(?:\w+\.)?(?:"
        + "|".join(VARCHAR_COLUMNS)
        + r")\s*(?:=|<>|(?:NOT\s+)?IN\s*\()\s*(?:JSON_VALUE\(|@\w+\s*\+)",
        re.I,
    )
    offenders = [
        (name, match.group(0)) for name, statement in _statements() for match in uncast_json.finditer(statement)
    ]
    assert offenders == []

    for name, statement in _statements():
        if RECEIPTS not in statement:
            continue
        for match in re.finditer(
            r"(?:(\w+)\.)?operation\s*=\s*(?:'controller\.resolve_frontier'|CONVERT\(varchar\(40\),)",
            statement,
        ):
            alias = match.group(1)
            prefix = rf"\b{alias}\." if alias else r"(?<![\w.@])"
            if alias:
                assert re.search(prefix + r"request_id\s*=", statement), (name, alias, _flat(statement))
            assert re.search(prefix + r"request_hash\s*=", statement), (name, alias, _flat(statement))

    for helper in (pending_window_handoff_authority_sql, nonwindow_handoff_authority_sql):
        sql = helper(KERNEL.names)
        assert "acc.record_kind='reconcile_acceptance'" in sql
        assert "acc.parent_hash=f.key_hash AND acc.parent_key=f.full_key" in sql
        assert "original.request_id=acc.full_key AND original.request_hash=acc.key_hash" in sql

    completion = reconciliation_completion_sql(KERNEL.names)
    assert "req.record_kind=CONVERT(varchar(40),JSON_VALUE(@stored_work,'$.reconcile_producer')+'_reconcile_request')" in completion
    assert "req.key_hash=" in completion
    assert "acc.record_kind='reconcile_acceptance'" in completion
    assert "resolution.request_id=acc.full_key AND resolution.request_hash=acc.key_hash" in completion


