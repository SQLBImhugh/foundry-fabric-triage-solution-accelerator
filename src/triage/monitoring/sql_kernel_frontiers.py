"""Protected deny-only intake frontiers and controller-owned reconciliation.

Producer completion is not validation. A collecting window remains fenced even
when every page received so far has a controller acknowledgement. Only a
correlated full-window publication or durable rejection can close that fence.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    current_work,
    key_hash,
    payload_hash,
    procedure,
    record_hash,
    record_insert,
    save_receipt,
)
from triage.monitoring.sql_kernel_contracts import KernelObject, RpcContract, SqlNames


def raise_frontier_sql(
    names: SqlNames, *, producer: str, operation: str, topic: str, reference: str,
    target: str = "NULL", collection_id: str = "NULL", requires_window: str = "0",
    collection_complete: str = "1", window_start: str = "NULL", window_end: str = "NULL",
) -> str:
    records = names.table("monitoring_records")
    return f"""DECLARE @frontier_topic varchar(32)={topic},@frontier_reference nvarchar(1024)={reference},
    @frontier_target nvarchar(max)={target},@frontier_target_key nvarchar(1024),
    @frontier_collection nvarchar(128)={collection_id},@frontier_requires_window bit={requires_window},
    @frontier_collection_complete bit={collection_complete},
    @frontier_start datetime2(6)={window_start},@frontier_end datetime2(6)={window_end},
    @frontier_window nvarchar(max);
IF @frontier_target IS NOT NULL
BEGIN
    IF COALESCE(JSON_VALUE(@frontier_target,'$.tenant_id'),'')<>@tenant_id
       OR COALESCE(JSON_VALUE(@frontier_target,'$.epoch'),'')<>@epoch
       OR COALESCE(JSON_VALUE(@frontier_target,'$.workload'),'') NOT IN ('powerbi','fabric_pipeline')
       OR NOT ({canonical_guid("JSON_VALUE(@frontier_target,'$.workspace_id')")})
       OR NOT ({canonical_guid("JSON_VALUE(@frontier_target,'$.item_id')")})
        THROW 51073, 'Frontier target must retain the accepted current-context identity', 1;
    SET @frontier_target_key=N'monitor:v1:'+@epoch+N':'+@tenant_id+N':'
        +JSON_VALUE(@frontier_target,'$.workload')+N':'+JSON_VALUE(@frontier_target,'$.workspace_id')
        +N':'+JSON_VALUE(@frontier_target,'$.item_id');
END;
IF (@frontier_start IS NULL AND @frontier_end IS NOT NULL)
   OR (@frontier_start IS NOT NULL AND (@frontier_end IS NULL OR @frontier_end<@frontier_start
       OR @frontier_target_key IS NULL))
   OR (@frontier_requires_window=1 AND NOT ({canonical_guid('@frontier_collection')}))
    THROW 51073, 'A collection frontier requires its exact work/window identity', 1;
IF @frontier_start IS NOT NULL
    SET @frontier_window=(SELECT CONVERT(nvarchar(40),@frontier_start,127)+N'Z' AS start_at,
        CONVERT(nvarchar(40),@frontier_end,127)+N'Z' AS end_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
DECLARE @frontier_scope nvarchar(1024)=COALESCE(@frontier_target_key,N'tenant');
DECLARE @frontier_key nvarchar(1024)=N'validation:v1:'+@epoch+N':'+@tenant_id+N':'+@frontier_topic
    +N':'+LOWER(CONVERT(char(64),{key_hash('@frontier_reference')},2))
    +N':'+LOWER(CONVERT(char(64),{key_hash('@frontier_scope')},2))
    +N':'+COALESCE({payload_hash('@frontier_window')},N'none'),
    @frontier_prior nvarchar(max),@frontier_prior_revision bigint,@frontier_row_revision bigint,
    @frontier_validated bigint=0,@frontier_revision bigint,@frontier_payload nvarchar(max),
    @frontier_old_window nvarchar(max),@frontier_old_window_state varchar(40);
SELECT @frontier_prior=payload,@frontier_prior_revision=sequence_number,@frontier_row_revision=revision
FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_frontier'
  AND full_key=@frontier_key AND key_hash={key_hash('@frontier_key')};
IF @frontier_prior IS NOT NULL
BEGIN
    SET @frontier_validated=TRY_CONVERT(bigint,JSON_VALUE(@frontier_prior,'$.validated_revision'));
    IF @frontier_validated IS NULL OR @frontier_validated<0
       OR @frontier_prior_revision IS NULL OR @frontier_prior_revision<1
       OR @frontier_validated>@frontier_prior_revision
       OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@frontier_prior,'$.accepted_revision')),-1)<>@frontier_prior_revision
        THROW 51072, 'Malformed frontier is a denial, not an empty validation history', 1;
END;
SET @frontier_revision=COALESCE(@frontier_prior_revision,0)+1;
SELECT @frontier_old_window=payload,@frontier_old_window_state=status FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_window' AND full_key=@frontier_key;
IF (@frontier_prior IS NULL AND @frontier_old_window IS NOT NULL)
   OR (@frontier_prior IS NOT NULL AND @frontier_requires_window=1 AND @frontier_old_window IS NULL)
    THROW 51072, 'Collection/window frontier state is missing; runtime cannot recreate it', 1;
IF @frontier_old_window IS NOT NULL AND (
    @frontier_requires_window=0
    OR @frontier_old_window_state IN ('validated','rejected')
    OR COALESCE(JSON_VALUE(@frontier_old_window,'$.collection_id'),'')<>@frontier_collection
    OR COALESCE(JSON_VALUE(@frontier_old_window,'$.collection_complete'),'')<>'false')
    THROW 51072, 'A sealed or different collection cannot accept a new page; original replay only', 1;
IF @frontier_requires_window=1
BEGIN
    DECLARE @frontier_window_payload nvarchar(max)=(SELECT @frontier_key AS frontier_key,
        @frontier_collection AS collection_id,JSON_QUERY(@frontier_window) AS [window],
        @frontier_collection_complete AS collection_complete,
        CASE WHEN @frontier_collection_complete=1 THEN @request_id ELSE NULL END AS closing_request_id,
        CASE WHEN @frontier_collection_complete=1 THEN @frontier_revision ELSE NULL END AS closing_revision
        FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
    IF @frontier_old_window IS NULL
    BEGIN
        {record_insert(names, 'validation_window', '@frontier_key', '@frontier_window_payload', status="CASE WHEN @frontier_collection_complete=1 THEN N'awaiting_validation' ELSE N'collecting' END", target_key='@frontier_target_key', parent_key='@frontier_collection', sequence='@frontier_revision')}
    END
    ELSE UPDATE {records} SET revision=revision+1,sequence_number=@frontier_revision,
        status=CASE WHEN @frontier_collection_complete=1 THEN 'awaiting_validation' ELSE 'collecting' END,
        payload=@frontier_window_payload
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_window' AND full_key=@frontier_key;
END;
SET @frontier_payload=(SELECT @tenant_id AS tenant_id,@epoch AS epoch,@frontier_key AS frontier_key,
    JSON_QUERY(@frontier_target) AS target,JSON_QUERY(@frontier_window) AS [window],
    @frontier_revision AS accepted_revision,@frontier_validated AS validated_revision,
    @request_id AS latest_request_id,CONVERT(nvarchar(40),@now,127)+N'Z' AS updated_at
    FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
IF @frontier_prior IS NULL
BEGIN
    {record_insert(names, 'validation_frontier', '@frontier_key', '@frontier_payload', status="N'pending_validation'", target_key='@frontier_target_key', parent_key="CASE WHEN @frontier_requires_window=1 THEN @frontier_key ELSE NULL END", sequence='@frontier_revision')}
END
ELSE UPDATE {records} SET revision=revision+1,sequence_number=@frontier_revision,
    status='pending_validation',payload=@frontier_payload
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_frontier'
      AND full_key=@frontier_key AND revision=@frontier_row_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Accepted frontier advance lost its compare-and-set', 1;
DECLARE @frontier_evidence nvarchar(max)=(SELECT full_key AS binding_key,payload AS binding_payload
    FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='accepted_fact'
      AND JSON_VALUE(payload,'$.batch_id')=@request_id
    ORDER BY full_key COLLATE Latin1_General_100_BIN2 FOR JSON PATH);
-- FOR JSON PATH returns NULL, not an empty array, when no accepted_fact rows
-- match. Concatenating that NULL made the digest NULL, and the handoff payload
-- below is built without INCLUDE_NULL_VALUES, so the key vanished entirely and
-- ValidationHandoff.evidence_digest failed to decode on every controller read.
-- A web intent with no accepted evidence -- the first discovery a user queues --
-- hit this, so hash the empty set explicitly rather than propagating NULL.
DECLARE @frontier_evidence_digest char(64)={payload_hash("N'"+operation+"'+@binding_hash+COALESCE(@frontier_evidence,N'[]')")},
    @frontier_handoff_key nvarchar(1024)=@frontier_key+N':handoff:'+CONVERT(nvarchar(30),@frontier_revision);
DECLARE @frontier_handoff nvarchar(max)=(SELECT @frontier_key AS frontier_key,
    @frontier_revision AS frontier_revision,N'{producer}' AS producer,@request_id AS producer_request_id,
    N'{operation}' AS producer_operation,@fingerprint AS producer_fingerprint,
    @binding_hash AS producer_binding_hash,@reconcile_id AS work_id,@current_revision AS policy_revision,
    @frontier_evidence_digest AS evidence_digest,@frontier_requires_window AS requires_window
    FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
{record_insert(names, 'validation_handoff', '@frontier_handoff_key', '@frontier_handoff', status="N'pending_validation'", target_key='@frontier_target_key', parent_key='@frontier_key', sequence='@frontier_revision')}"""


def frontier_snapshot_sql(names: SqlNames, target: str) -> str:
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    pending = f"""CASE WHEN f.sequence_number IS NULL OR f.sequence_number<1
        OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(f.payload,'$.accepted_revision')),-1)<>f.sequence_number
        OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(f.payload,'$.validated_revision')),-1)<>f.sequence_number
        OR f.status NOT IN ('published','rejected')
        OR (f.parent_key IS NOT NULL AND (w.full_key IS NULL OR w.status NOT IN ('validated','rejected')))
        OR fc.sequence_number IS NULL OR fc.sequence_number<>f.sequence_number
        OR NOT EXISTS (SELECT 1 FROM {receipts} AS receipt
            WHERE receipt.tenant_id=f.tenant_id AND receipt.epoch=f.epoch
              AND receipt.operation='controller.resolve_frontier'
              AND receipt.request_id=JSON_VALUE(fc.payload,'$.request_id')
              AND JSON_VALUE(receipt.payload,'$.result.frontier_key')=f.full_key
              AND TRY_CONVERT(bigint,JSON_VALUE(receipt.payload,'$.result.validated_revision'))=f.sequence_number
              AND JSON_VALUE(receipt.payload,'$.result.state') IN ('published','rejected'))
        THEN 1 ELSE 0 END"""
    return f"""DECLARE @frontier_snapshot nvarchar(max)=(SELECT f.full_key AS frontier_key,
    f.sequence_number AS accepted_revision,
    TRY_CONVERT(bigint,JSON_VALUE(f.payload,'$.validated_revision')) AS validated_revision,
    f.revision AS revision,f.status,w.status AS window_state,
    JSON_VALUE(fc.payload,'$.request_id') AS resolution_receipt,
    CAST(({pending}) AS bit) AS pending
    FROM {records} AS f
    LEFT JOIN {records} AS w ON w.tenant_id=f.tenant_id AND w.epoch=f.epoch
      AND w.record_kind='validation_window' AND w.full_key=f.full_key
    LEFT JOIN {records} AS fc ON fc.tenant_id=f.tenant_id AND fc.epoch=f.epoch
      AND fc.record_kind='frontier_commit' AND fc.full_key=f.full_key
    WHERE f.tenant_id=@tenant_id AND f.epoch=@epoch AND f.record_kind='validation_frontier'
      AND (f.target_key IS NULL OR f.target_key={target})
    ORDER BY f.full_key COLLATE Latin1_General_100_BIN2
    FOR JSON PATH,INCLUDE_NULL_VALUES);
DECLARE @frontier_digest char(64)={payload_hash('@frontier_snapshot')},
    @frontier_pending bit=CASE WHEN EXISTS (SELECT 1 FROM OPENJSON(@frontier_snapshot)
        WHERE JSON_VALUE(value,'$.pending')='true') THEN 1 ELSE 0 END;"""


def reservation_frontier_guard(names: SqlNames) -> str:
    return f"""{frontier_snapshot_sql(names, '@stored_target_key')}
IF @frontier_pending=1
    THROW 51072, 'Pending accepted intake or an incomplete window forbids every new action reservation', 1;
IF COALESCE(JSON_VALUE(@validation,'$.frontier_digest'),'')<>@frontier_digest
    THROW 51072, 'Controller action validation does not cover the committed frontier snapshot', 1;"""


def frontier_can_close_sql() -> str:
    """Exact closing predicate, shared by SQL generation and offline decision tests."""
    return """(@whole_window_rejection=1 AND @prefix_committed=1) OR (
@whole_window_rejection=0 AND @all_resolved=1 AND (
   @window IS NULL OR (
       COALESCE(JSON_VALUE(@proof,'$.window_complete'),'')='true' AND (
                @decision='published'
                AND COALESCE(JSON_VALUE(@window,'$.collection_complete'),'')='true'
                AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@window,'$.closing_revision')),-1)=@accepted
                AND COALESCE(JSON_VALUE(@proof,'$.closing_request_id'),'')
                    =COALESCE(JSON_VALUE(@window,'$.closing_request_id'),'')
       )
   )
))"""


def page_publication_required_sql() -> str:
    return "@whole_window_rejection=0 AND @window_ack=0 AND @handoff_ack=0 AND @decision='published'"


def stale_page_policy_sql() -> str:
   return "@maintenance=1 OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@handoff,'$.policy_revision')),-1)<>@current_revision"


def handoff_decision_change_sql() -> str:
    return "@whole_window_rejection=0 AND @window_ack=0 AND @handoff_ack=0 AND @handoff_state IN ('published','rejected') AND @handoff_state<>@decision"


def nonwindow_handoff_authority_sql(names: SqlNames) -> str:
    """Original per-handoff decision covered by a protected committed prefix.

    The frontier may contain later pending intake. Acknowledging this work must
    neither close that intake nor republish the earlier policy's evidence.
    """
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    return f"""SELECT original.request_id AS handoff_resolution_request_id,
    TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.work_fence')) AS handoff_resolution_work_fence,
    root.request_id AS frontier_resolution_request_id,fc.sequence_number AS frontier_resolution_revision
FROM {records} AS f
JOIN {records} AS own_handoff ON own_handoff.tenant_id=f.tenant_id AND own_handoff.epoch=f.epoch
  AND own_handoff.record_kind='validation_handoff' AND own_handoff.parent_key=f.full_key
  AND own_handoff.full_key=@handoff_key AND own_handoff.sequence_number=@handoff_revision
  AND own_handoff.status=@decision
  AND JSON_VALUE(own_handoff.payload,'$.requires_window')='false'
  AND JSON_VALUE(own_handoff.payload,'$.work_id')=@work_id
  AND JSON_VALUE(own_handoff.payload,'$.producer_request_id')=@producer_request_id
  AND TRY_CONVERT(bigint,JSON_VALUE(own_handoff.payload,'$.frontier_revision'))=@handoff_revision
JOIN {receipts} AS original ON original.tenant_id=f.tenant_id AND original.epoch=f.epoch
  AND original.operation='controller.resolve_frontier'
  AND JSON_VALUE(original.payload,'$.result.work_id')=@work_id
  AND JSON_VALUE(original.payload,'$.result.producer_request_id')=@producer_request_id
  AND JSON_VALUE(original.payload,'$.result.frontier_key')=f.full_key
  AND JSON_VALUE(original.payload,'$.result.resolution_scope')='handoff'
  AND JSON_VALUE(original.payload,'$.result.handoff_decision')=own_handoff.status
  AND JSON_VALUE(original.payload,'$.result.state') IN ('pending_validation',own_handoff.status)
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.handoff_revision'))=@handoff_revision
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.work_fence')) BETWEEN 1 AND @fence
JOIN {records} AS fc ON fc.tenant_id=f.tenant_id AND fc.epoch=f.epoch
  AND fc.record_kind='frontier_commit' AND fc.full_key=f.full_key
  AND fc.status IN ('published','rejected') AND fc.sequence_number=@validated
  AND JSON_VALUE(fc.payload,'$.frontier_key')=f.full_key
  AND JSON_VALUE(fc.payload,'$.decision')=fc.status
  AND TRY_CONVERT(bigint,JSON_VALUE(fc.payload,'$.frontier_revision'))=fc.sequence_number
JOIN {receipts} AS root ON root.tenant_id=f.tenant_id AND root.epoch=f.epoch
  AND root.operation='controller.resolve_frontier'
  AND root.request_id=JSON_VALUE(fc.payload,'$.request_id')
  AND JSON_VALUE(root.payload,'$.result.resolution_scope')='handoff'
  AND JSON_VALUE(root.payload,'$.result.state')=fc.status
  AND JSON_VALUE(root.payload,'$.result.frontier_key')=f.full_key
  AND TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.frontier_revision'))=fc.sequence_number
  AND TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.validated_revision'))=fc.sequence_number
  AND TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.work_fence'))>0
JOIN {records} AS root_handoff ON root_handoff.tenant_id=f.tenant_id AND root_handoff.epoch=f.epoch
  AND root_handoff.record_kind='validation_handoff' AND root_handoff.parent_key=f.full_key
  AND root_handoff.sequence_number=TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.handoff_revision'))
  AND root_handoff.sequence_number BETWEEN 1 AND fc.sequence_number
  AND JSON_VALUE(root_handoff.payload,'$.requires_window')='false'
  AND JSON_VALUE(root_handoff.payload,'$.work_id')=JSON_VALUE(root.payload,'$.result.work_id')
  AND JSON_VALUE(root_handoff.payload,'$.producer_request_id')=JSON_VALUE(root.payload,'$.result.producer_request_id')
  AND root_handoff.status=JSON_VALUE(root.payload,'$.result.handoff_decision')
WHERE f.tenant_id=@tenant_id AND f.epoch=@epoch AND f.record_kind='validation_frontier'
  AND f.full_key=@frontier_key AND f.parent_key IS NULL AND f.sequence_number=@accepted
  AND TRY_CONVERT(bigint,JSON_VALUE(f.payload,'$.accepted_revision'))=@accepted
  AND TRY_CONVERT(bigint,JSON_VALUE(f.payload,'$.validated_revision'))=@validated
  AND @validated BETWEEN @handoff_revision AND @accepted
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.frontier_revision'))
      BETWEEN @handoff_revision AND fc.sequence_number
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.validated_revision'))
      BETWEEN 0 AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.frontier_revision'))
  AND NOT EXISTS (SELECT 1 FROM {records} AS w WHERE w.tenant_id=f.tenant_id AND w.epoch=f.epoch
      AND w.record_kind='validation_window' AND w.full_key=f.full_key)
  AND (SELECT COUNT_BIG(*) FROM {records} AS prefix
      WHERE prefix.tenant_id=f.tenant_id AND prefix.epoch=f.epoch
        AND prefix.record_kind='validation_handoff' AND prefix.parent_key=f.full_key
        AND prefix.sequence_number BETWEEN 1 AND fc.sequence_number
        AND prefix.status IN ('published','rejected')
        AND JSON_VALUE(prefix.payload,'$.requires_window')='false'
        AND TRY_CONVERT(bigint,JSON_VALUE(prefix.payload,'$.frontier_revision'))=prefix.sequence_number
        AND EXISTS (SELECT 1 FROM {receipts} AS producer_receipt
            WHERE producer_receipt.tenant_id=f.tenant_id AND producer_receipt.epoch=f.epoch
              AND producer_receipt.operation=JSON_VALUE(prefix.payload,'$.producer_operation')
              AND producer_receipt.request_id=JSON_VALUE(prefix.payload,'$.producer_request_id')
              AND producer_receipt.fingerprint=JSON_VALUE(prefix.payload,'$.producer_fingerprint')
              AND JSON_VALUE(producer_receipt.payload,'$.binding_hash')=JSON_VALUE(prefix.payload,'$.producer_binding_hash')
              AND JSON_VALUE(producer_receipt.payload,'$.result.reconcile_work_id')=JSON_VALUE(prefix.payload,'$.work_id')
              AND JSON_VALUE(producer_receipt.payload,'$.result.frontier_key')=f.full_key
              AND TRY_CONVERT(bigint,JSON_VALUE(producer_receipt.payload,'$.result.frontier_revision'))=prefix.sequence_number)
      )=fc.sequence_number"""


def closed_window_authority_sql(names: SqlNames) -> str:
    """A sibling acknowledges the exact protected terminal window outcome."""
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    return f"""SELECT rejected.request_id AS window_resolution_request_id,
    JSON_VALUE(rejected.payload,'$.result.state') AS window_resolution_state
FROM {records} AS f
JOIN {records} AS sibling ON sibling.tenant_id=f.tenant_id AND sibling.epoch=f.epoch
  AND sibling.record_kind='validation_handoff' AND sibling.parent_key=f.full_key
  AND sibling.full_key=@handoff_key AND sibling.sequence_number=@handoff_revision
  AND JSON_VALUE(sibling.payload,'$.work_id')=@work_id
  AND JSON_VALUE(sibling.payload,'$.producer_request_id')=@producer_request_id
JOIN {records} AS w ON w.tenant_id=f.tenant_id AND w.epoch=f.epoch
  AND w.record_kind='validation_window' AND w.full_key=f.full_key
JOIN {records} AS fc ON fc.tenant_id=f.tenant_id AND fc.epoch=f.epoch
  AND fc.record_kind='frontier_commit' AND fc.full_key=f.full_key
  AND fc.status=f.status AND fc.sequence_number=f.sequence_number
JOIN {receipts} AS rejected ON rejected.tenant_id=f.tenant_id AND rejected.epoch=f.epoch
  AND rejected.operation='controller.resolve_frontier'
  AND rejected.request_id=JSON_VALUE(fc.payload,'$.request_id')
WHERE f.tenant_id=@tenant_id AND f.epoch=@epoch AND f.record_kind='validation_frontier'
  AND f.full_key=@frontier_key AND f.sequence_number=@accepted
  AND TRY_CONVERT(bigint,JSON_VALUE(f.payload,'$.validated_revision'))=@accepted
  AND ((f.status='rejected' AND w.status='rejected'
        AND JSON_VALUE(rejected.payload,'$.result.state')='rejected'
        AND JSON_VALUE(rejected.payload,'$.result.resolution_scope')='window')
       OR (f.status='published' AND w.status='validated'
        AND JSON_VALUE(rejected.payload,'$.result.state')='published'
        AND JSON_VALUE(rejected.payload,'$.result.resolution_scope')='handoff'))
  AND JSON_VALUE(rejected.payload,'$.result.frontier_key')=f.full_key
  AND TRY_CONVERT(bigint,JSON_VALUE(rejected.payload,'$.result.frontier_revision'))=@accepted
  AND TRY_CONVERT(bigint,JSON_VALUE(rejected.payload,'$.result.validated_revision'))=@accepted"""


def rejected_window_authority_sql(names: SqlNames) -> str:
    return f"""SELECT authority.window_resolution_request_id AS window_rejection_request_id
FROM ({closed_window_authority_sql(names)}) AS authority WHERE authority.window_resolution_state='rejected'"""


def close_frontier_sql(names: SqlNames) -> str:
   return f"""UPDATE {names.table('monitoring_records')} SET revision=revision+1,status=@window_decision,
   payload=JSON_MODIFY(JSON_MODIFY(payload,'$.validated_revision',@validated),'$.updated_at',@resolved_at)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_frontier'
  AND full_key=@frontier_key AND sequence_number=@expected_frontier_revision;"""


def _resolve(names: SqlNames, contract: RpcContract) -> KernelObject:
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    body = f"""{current_work(names, ('reconcile_state',))}
IF @stored_work_revision<>@work_revision OR @stored_work_status<>'leased'
    THROW 51074, 'Reconciliation requires its own current work lease and revision', 1;
DECLARE @producer_request_id nvarchar(256)=JSON_VALUE(@stored_work,'$.reconcile_request_id'),
    @producer varchar(16)=JSON_VALUE(@stored_work,'$.reconcile_producer'),@handoff nvarchar(max),
    @handoff_key nvarchar(1024),@handoff_state varchar(40),@producer_request nvarchar(max),
    @frontier_key nvarchar(1024),@handoff_revision bigint,@frontier nvarchar(max),
    @accepted bigint,@validated bigint,@window nvarchar(max),@proof nvarchar(max),
    @decision varchar(24),@window_decision varchar(24),@can_close bit=0,
    @all_resolved bit=0,@result_state varchar(24),@whole_window_rejection bit=0,
    @prefix_committed bit=0,@window_state varchar(40),@window_ack bit=0,@handoff_ack bit=0,
    @handoff_resolution_request_id nvarchar(256),@handoff_resolution_work_fence bigint,
    @frontier_resolution_request_id nvarchar(256),@frontier_resolution_revision bigint,
    @window_rejection_request_id nvarchar(256),@window_resolution_request_id nvarchar(256),
    @window_resolution_state varchar(24);
SELECT @handoff=payload,@handoff_key=full_key,@handoff_state=status,
    @frontier_key=parent_key,@handoff_revision=sequence_number FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_handoff'
  AND JSON_VALUE(payload,'$.producer_request_id')=@producer_request_id
  AND JSON_VALUE(payload,'$.producer')=@producer AND JSON_VALUE(payload,'$.work_id')=@work_id;
SELECT @producer_request=payload FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind=@producer+'_reconcile_request'
  AND full_key=@producer_request_id;
IF @handoff IS NULL OR @producer_request IS NULL
   OR COALESCE(JSON_VALUE(@producer_request,'$.work_id'),'')<>@work_id
   OR COALESCE(JSON_VALUE(@producer_request,'$.frontier_key'),'')<>@frontier_key
   OR COALESCE(JSON_VALUE(@producer_request,'$.fingerprint'),'')
      <>COALESCE(JSON_VALUE(@handoff,'$.producer_fingerprint'),'')
   OR NOT EXISTS (SELECT 1 FROM {receipts} WHERE tenant_id=@tenant_id AND epoch=@epoch
       AND operation=JSON_VALUE(@handoff,'$.producer_operation') AND request_id=@producer_request_id
       AND fingerprint=JSON_VALUE(@handoff,'$.producer_fingerprint')
       AND JSON_VALUE(payload,'$.binding_hash')=JSON_VALUE(@handoff,'$.producer_binding_hash'))
    THROW 51072, 'Reconciliation lost its immutable intent/evidence/receipt binding', 1;
SELECT @frontier=payload,@accepted=sequence_number FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_frontier' AND full_key=@frontier_key;
SET @validated=TRY_CONVERT(bigint,JSON_VALUE(@frontier,'$.validated_revision'));
IF @frontier IS NULL OR @accepted<>@expected_frontier_revision OR @validated IS NULL
   OR @handoff_revision>@accepted OR @validated<0 OR @validated>@accepted
    THROW 51072, 'Accepted frontier changed; validation cannot outrun committed intake', 1;
SELECT @window=payload,@window_state=status FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_window' AND full_key=@frontier_key;
IF JSON_VALUE(@handoff,'$.requires_window')='true' AND @window IS NULL
    THROW 51072, 'First-page window fence is missing, not complete', 1;
SELECT @proof=payload FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='frontier_validation'
  AND full_key=@validation_id AND key_hash={key_hash('@validation_id')}
  AND {payload_hash('payload')}=@validation_hash;
SET @decision=JSON_VALUE(@proof,'$.decision');
IF @proof IS NULL OR COALESCE(@decision,'') NOT IN ('published','rejected')
   OR COALESCE(JSON_VALUE(@proof,'$.work_id'),'')<>@work_id
   OR COALESCE(JSON_VALUE(@proof,'$.lease_owner_id'),'')<>@owner_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@proof,'$.lease_fence')),-1)<>@fence
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@proof,'$.expected_work_revision')),-1)<>@work_revision
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@proof,'$.policy_revision')),-1)<>@current_revision
   OR COALESCE(JSON_VALUE(@proof,'$.frontier_key'),'')<>@frontier_key
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@proof,'$.through_revision')),-1)<>@accepted
   OR COALESCE(JSON_VALUE(@proof,'$.producer_request_id'),'')<>@producer_request_id
   OR COALESCE(JSON_VALUE(@proof,'$.producer_fingerprint'),'')<>COALESCE(JSON_VALUE(@handoff,'$.producer_fingerprint'),'')
   OR COALESCE(JSON_VALUE(@proof,'$.evidence_digest'),'')<>COALESCE(JSON_VALUE(@handoff,'$.evidence_digest'),'')
   OR NULLIF(JSON_VALUE(@proof,'$.detail'),'') IS NULL
    THROW 51072, 'Controller proof is not correlated to this work, intent and committed frontier', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@proof) WHERE [key]='acknowledge_handoff' AND type<>3)
   OR (SELECT COUNT(*) FROM OPENJSON(@proof) WHERE [key]='acknowledge_handoff')>1
    THROW 51073, 'Handoff acknowledgement mode must be one strict JSON boolean', 1;
IF JSON_VALUE(@proof,'$.acknowledge_handoff')='true'
BEGIN
    IF @window IS NOT NULL OR COALESCE(JSON_VALUE(@handoff,'$.requires_window'),'')<>'false'
       OR @handoff_state NOT IN ('published','rejected') OR @decision<>@handoff_state
       OR COALESCE(JSON_VALUE(@proof,'$.reject_whole_window'),'false')<>'false'
       OR COALESCE(JSON_VALUE(@proof,'$.window_complete'),'false')<>'false'
       OR JSON_VALUE(@proof,'$.closing_request_id') IS NOT NULL
        THROW 51072, 'Handoff acknowledgement retains one terminal non-window decision', 1;
    SELECT TOP (1) @handoff_resolution_request_id=authority.handoff_resolution_request_id,
        @handoff_resolution_work_fence=authority.handoff_resolution_work_fence,
        @frontier_resolution_request_id=authority.frontier_resolution_request_id,
        @frontier_resolution_revision=authority.frontier_resolution_revision
    FROM ({nonwindow_handoff_authority_sql(names)}) AS authority
    ORDER BY authority.handoff_resolution_work_fence,authority.handoff_resolution_request_id;
    IF @handoff_resolution_request_id IS NULL
        THROW 51072, 'Handoff acknowledgement lacks original decision and committed prefix receipts', 1;
    SET @handoff_ack=1;
END;
IF @window_state IN ('rejected','validated') AND COALESCE(JSON_VALUE(@proof,'$.reject_whole_window'),'false')<>'true'
BEGIN
    IF @window_state='rejected' AND @decision<>'rejected'
        THROW 51072, 'An already rejected window accepts terminal acknowledgement, not republication', 1;
    SELECT @window_resolution_request_id=authority.window_resolution_request_id,
        @window_resolution_state=authority.window_resolution_state
    FROM ({closed_window_authority_sql(names)}) AS authority;
    IF @window_resolution_request_id IS NULL
        THROW 51072, 'Sibling acknowledgement lacks the exact protected committed window outcome', 1;
    IF @window_resolution_state='rejected' SET @window_rejection_request_id=@window_resolution_request_id;
    SET @window_ack=1;
END;
IF COALESCE(JSON_VALUE(@proof,'$.reject_whole_window'),'false')='true'
BEGIN
    IF @window IS NULL OR @decision<>'rejected' OR @window_state NOT IN ('collecting','awaiting_validation')
        THROW 51072, 'Whole-window rejection is a separate current-fenced unfinished-window operation', 1;
    SET @whole_window_rejection=1;
    IF (SELECT COUNT_BIG(*) FROM {records} AS accepted_handoff
        WHERE accepted_handoff.tenant_id=@tenant_id AND accepted_handoff.epoch=@epoch
          AND accepted_handoff.record_kind='validation_handoff' AND accepted_handoff.parent_key=@frontier_key
          AND accepted_handoff.sequence_number BETWEEN 1 AND @accepted
          AND EXISTS (SELECT 1 FROM {receipts} AS original_receipt
              WHERE original_receipt.tenant_id=@tenant_id AND original_receipt.epoch=@epoch
                AND original_receipt.operation=JSON_VALUE(accepted_handoff.payload,'$.producer_operation')
                AND original_receipt.request_id=JSON_VALUE(accepted_handoff.payload,'$.producer_request_id')
                AND original_receipt.fingerprint=JSON_VALUE(accepted_handoff.payload,'$.producer_fingerprint')
                AND JSON_VALUE(original_receipt.payload,'$.binding_hash')
                    =JSON_VALUE(accepted_handoff.payload,'$.producer_binding_hash')))=@accepted
        SET @prefix_committed=1;
    IF @prefix_committed=0
        THROW 51072, 'Whole-window rejection must cover every committed original intake receipt', 1;
END;
IF {page_publication_required_sql()} AND (
    {stale_page_policy_sql()}
    OR EXISTS (SELECT 1 FROM {records} AS binding LEFT JOIN {records} AS fact
        ON fact.tenant_id=binding.tenant_id AND fact.epoch=binding.epoch
       AND fact.record_kind=JSON_VALUE(binding.payload,'$.fact_kind')
       AND fact.full_key=JSON_VALUE(binding.payload,'$.fact_key')
       AND fact.key_hash={key_hash("JSON_VALUE(binding.payload,'$.fact_key')")}
        WHERE binding.tenant_id=@tenant_id AND binding.epoch=@epoch AND binding.record_kind='accepted_fact'
          AND JSON_VALUE(binding.payload,'$.batch_id')=@producer_request_id
          AND (fact.full_key IS NULL
               OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(binding.payload,'$.fact_revision')),-1)<>fact.revision
               OR COALESCE(JSON_VALUE(binding.payload,'$.row_hash'),'')<>{record_hash('fact')})))
    THROW 51072, 'Publication evidence or policy changed; only explicit durable rejection is available', 1;
IF {page_publication_required_sql()} AND @producer='web' AND NOT EXISTS (
    SELECT 1 FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch
      AND record_kind=CASE JSON_VALUE(@producer_request,'$.topic')
            WHEN 'scope' THEN 'scope' WHEN 'review' THEN 'review_request'
            WHEN 'discovery' THEN 'discovery_request' END
      AND full_key=JSON_VALUE(@producer_request,'$.reference_id')
      AND JSON_VALUE(payload,'$.request_id')=@producer_request_id)
    THROW 51072, 'Producer intent changed before publication; its original binding must be rejected', 1;
IF {handoff_decision_change_sql()}
    THROW 51072, 'A terminal handoff decision cannot be changed', 1;
IF @whole_window_rejection=0 AND @window_ack=0 AND @handoff_ack=0
BEGIN
    UPDATE {records} SET status=@decision,revision=revision+1
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_handoff'
  AND full_key=@handoff_key AND status IN ('pending_validation',@decision);
    IF @@ROWCOUNT<>1 THROW 51072, 'Handoff acknowledgement lost its current state', 1;
END;
IF (SELECT COUNT_BIG(*) FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_handoff'
      AND parent_key=@frontier_key AND sequence_number BETWEEN 1 AND @accepted
      AND status IN ('published','rejected'))=@accepted
    SET @all_resolved=1;
SET @window_decision=@decision;
IF @window_ack=0 AND @handoff_ack=0 AND ({frontier_can_close_sql()}) SET @can_close=1;
IF @whole_window_rejection=1
    SET @window_decision='rejected';
-- Producer completion and per-page acknowledgement never close a collecting window.
SET @result_state=CASE WHEN @window_ack=1 THEN @window_resolution_state
    WHEN @handoff_ack=1 THEN @handoff_state
    WHEN @can_close=1 THEN @window_decision ELSE 'pending_validation' END;
IF @can_close=1
BEGIN
    SET @validated=@accepted;
    DECLARE @resolved_at nvarchar(40)=CONVERT(nvarchar(40),@now,127)+N'Z';
    {close_frontier_sql(names)}
    IF @@ROWCOUNT<>1 THROW 51072, 'Frontier validation compare-and-set failed', 1;
    IF @window IS NOT NULL
        UPDATE {records} SET revision=revision+1,status=CASE WHEN @window_decision='published' THEN 'validated' ELSE 'rejected' END
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='validation_window' AND full_key=@frontier_key;
    DECLARE @commit_payload nvarchar(max)=(SELECT @request_id AS request_id,@validation_id AS validation_id,
        @frontier_key AS frontier_key,@accepted AS frontier_revision,@window_decision AS decision
        FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
    IF NOT EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
        AND record_kind='frontier_commit' AND full_key=@frontier_key)
    BEGIN
        {record_insert(names, 'frontier_commit', '@frontier_key', '@commit_payload', status='@window_decision', sequence='@accepted')}
    END
    ELSE UPDATE {records} SET revision=revision+1,sequence_number=@accepted,status=@window_decision,payload=@commit_payload
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='frontier_commit' AND full_key=@frontier_key;
END;
SET @result=(SELECT @work_id AS work_id,@fence AS work_fence,@producer_request_id AS producer_request_id,
    @frontier_key AS frontier_key,@accepted AS frontier_revision,@validated AS validated_revision,
    @handoff_revision AS handoff_revision,
    @result_state AS state,
    CASE WHEN @whole_window_rejection=1 OR @window_ack=1 OR @handoff_ack=1 THEN @handoff_state ELSE @decision END AS handoff_decision,
    CASE WHEN @handoff_ack=1 THEN 'handoff_acknowledgement'
         WHEN @window_ack=1 THEN 'window_acknowledgement'
         WHEN @whole_window_rejection=1 THEN 'window' ELSE 'handoff' END AS resolution_scope,
    @handoff_resolution_request_id AS handoff_resolution_request_id,
    @handoff_resolution_work_fence AS handoff_resolution_work_fence,
    @frontier_resolution_request_id AS frontier_resolution_request_id,
    @frontier_resolution_revision AS frontier_resolution_revision,
    @window_rejection_request_id AS window_rejection_request_id
    ,@window_resolution_request_id AS window_resolution_request_id,@window_resolution_state AS window_resolution_state
    FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
DECLARE @acceptance_key nvarchar(1024)=@request_id;
{record_insert(names, 'reconcile_acceptance', '@acceptance_key', '@result', status='@result_state', parent_key='@frontier_key', sequence='@accepted')}
SET @affected=1;
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True, permit_maintenance=True)


def frontier_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    inspect = contracts["controller.inspect_frontiers"]
    inspect_body = f"""{frontier_snapshot_sql(names, '@target_key')}
SET @status='read';
SET @result=(SELECT @target_key AS target_key,@frontier_digest AS frontier_digest,
    @frontier_pending AS pending,JSON_QUERY(@frontier_snapshot) AS frontiers
    FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);"""
    return {
        inspect.operation: procedure(names, inspect, inspect_body, permit_maintenance=True),
        "controller.resolve_frontier": _resolve(names, contracts["controller.resolve_frontier"]),
    }
