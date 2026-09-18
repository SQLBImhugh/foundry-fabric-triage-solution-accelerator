"""Accepted evidence, connector observation, checkpoint and budget SQL."""

from __future__ import annotations

from triage.monitoring.models import CONNECTOR_PENDING_GAP_CODES
from triage.monitoring.sql_kernel_common import (
    current_work,
    exact_text_equal,
    key_hash,
    literals,
    partition_identity,
    partition_owner,
    payload_hash,
    procedure,
    receipt_content_hash,
    record_hash,
    record_insert,
    save_receipt,
)
from triage.monitoring.sql_kernel_connectors import (
    restore_worker_proof_expression,
    subscription_type_sql,
    worker_ready_upgrade_sql,
)
from triage.monitoring.sql_kernel_contracts import (
    CATALOGUE_KINDS,
    EVIDENCE_KINDS,
    TELEMETRY_KINDS,
    WORK_FACT_KINDS,
    WORKER_WORK_KINDS,
    KernelObject,
    RpcContract,
    SqlNames,
)
from triage.monitoring.sql_kernel_intents import handoff_sql
from triage.monitoring.sql_kernel_removals import source_is_pending_removal_sql


def _accept_facts(names: SqlNames, contract: RpcContract) -> KernelObject:
    records = names.table("monitoring_records")
    allowed = (*CATALOGUE_KINDS, *EVIDENCE_KINDS, *TELEMETRY_KINDS)
    families = "\n       OR ".join(
        f"(@stored_work_kind=N'{family}' AND f.fact_kind NOT IN ({literals(kinds)}))"
        for family, kinds in WORK_FACT_KINDS.items()
    )
    body = f"""{current_work(names, WORKER_WORK_KINDS)}
IF @stored_work_revision<>@work_revision OR @stored_work_status<>'leased'
    THROW 51074, 'Evidence batch lost its work revision', 1;
IF (@stored_work_kind='poll' AND (@window_start_at IS NULL OR @window_end_at IS NULL))
   OR (@stored_work_kind<>'poll' AND (@window_start_at IS NOT NULL OR @window_end_at IS NOT NULL))
   OR (@stored_work_kind NOT IN ('inventory','poll') AND @collection_complete<>1)
    THROW 51073, 'Collection/window completion must use the stored work family contract', 1;
IF ISJSON(@facts_json)<>1 OR LEFT(LTRIM(@facts_json),1)<>N'[' OR DATALENGTH(@facts_json)>1048576
    THROW 51073, 'A bounded fact descriptor array is required', 1;
DECLARE @facts TABLE (
    fact_kind varchar(40) NOT NULL,full_key nvarchar(1024) NOT NULL,
    fact_key_hash binary(32) NOT NULL,revision bigint NOT NULL,payload_hash char(64) NOT NULL,
    PRIMARY KEY(fact_kind,fact_key_hash));
IF (SELECT COUNT(*) FROM OPENJSON(@facts_json)) NOT BETWEEN 1 AND 200
    THROW 51073, 'Evidence batch size is outside the fixed bound', 1;
IF EXISTS (
    SELECT 1 FROM OPENJSON(@facts_json)
    WHERE COALESCE(JSON_VALUE(value,'$.kind'),'') NOT IN ({literals(allowed)})
       OR NULLIF(JSON_VALUE(value,'$.key'),'') IS NULL OR DATALENGTH(JSON_VALUE(value,'$.key'))>2048
       OR TRY_CONVERT(bigint,JSON_VALUE(value,'$.revision')) IS NULL
       OR TRY_CONVERT(bigint,JSON_VALUE(value,'$.revision'))<1
       OR LEN(COALESCE(JSON_VALUE(value,'$.payload_hash'),''))<>64
       OR JSON_VALUE(value,'$.payload_hash') COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9A-F]%'
) THROW 51073, 'Fact descriptor has invalid identity/revision/hash', 1;
INSERT INTO @facts SELECT JSON_VALUE(value,'$.kind'),JSON_VALUE(value,'$.key'),
    {key_hash("JSON_VALUE(value,'$.key')")},
    CONVERT(bigint,JSON_VALUE(value,'$.revision')),JSON_VALUE(value,'$.payload_hash')
FROM OPENJSON(@facts_json);
IF EXISTS (
    SELECT 1 FROM @facts AS f LEFT JOIN {records} AS r
      ON r.tenant_id=@tenant_id AND r.epoch=@epoch AND r.record_kind=f.fact_kind
     AND r.full_key=f.full_key AND r.key_hash={key_hash('f.full_key')}
    WHERE r.full_key IS NULL OR r.revision<>f.revision
       OR {payload_hash('r.payload')}<>f.payload_hash OR ISJSON(r.payload)<>1
       OR {families}
       OR (@stored_target_key IS NOT NULL AND COALESCE(r.target_key,'')<>@stored_target_key)
       OR (@stored_work_kind='inventory' AND COALESCE(r.generation_id,r.parent_key,r.full_key)<>@work_id)
       OR (JSON_VALUE(r.payload,'$.tenant_id') IS NOT NULL AND JSON_VALUE(r.payload,'$.tenant_id')<>@tenant_id)
       OR (JSON_VALUE(r.payload,'$.epoch') IS NOT NULL AND JSON_VALUE(r.payload,'$.epoch')<>@epoch)
       OR (@stored_target_key IS NOT NULL AND
           COALESCE(JSON_QUERY(r.payload,'$.target'),JSON_QUERY(r.payload,'$.execution.target'),
               JSON_QUERY(r.payload,'$.observation.execution.target')) IS NOT NULL AND
           COALESCE(JSON_QUERY(r.payload,'$.target'),JSON_QUERY(r.payload,'$.execution.target'),
               JSON_QUERY(r.payload,'$.observation.execution.target'))<>JSON_QUERY(@stored_work,'$.target'))
) THROW 51072, 'Batch cannot accept missing, changed or unrelated raw facts', 1;
INSERT INTO {records}
    (tenant_id,epoch,record_kind,key_hash,full_key,revision,parent_hash,parent_key,payload)
SELECT @tenant_id,@epoch,'accepted_fact',{key_hash("N'accepted:'+@request_id+N':'+f.fact_kind+N':'+LOWER(CONVERT(char(64),"+key_hash('f.full_key')+",2))")},
    N'accepted:'+@request_id+N':'+f.fact_kind+N':'+LOWER(CONVERT(char(64),{key_hash('f.full_key')},2)),
    1,{key_hash('@work_id')},@work_id,
    (SELECT @request_id AS batch_id,@fingerprint AS batch_fingerprint,
        f.fact_kind AS fact_kind,f.full_key AS fact_key,f.revision AS fact_revision,
        f.payload_hash AS payload_hash,{record_hash('r')} AS row_hash,
        @work_id AS work_id,@fence AS work_fence
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
FROM @facts AS f JOIN {records} AS r
  ON r.tenant_id=@tenant_id AND r.epoch=@epoch AND r.record_kind=f.fact_kind
 AND r.key_hash=f.fact_key_hash AND r.full_key=f.full_key;
SET @affected=@@ROWCOUNT;
{handoff_sql(names, producer='worker', operation='worker.accept_facts', topic="CASE @stored_work_kind WHEN 'poll' THEN 'rest_page' WHEN 'capability_probe' THEN 'capability' WHEN 'connector_reconcile' THEN 'connector' ELSE 'inventory' END", reference='@work_id', target="JSON_QUERY(@stored_work,'$.target')", collection_id='@work_id', requires_window="CASE WHEN @stored_work_kind IN ('inventory','poll') THEN 1 ELSE 0 END", collection_complete='@collection_complete', window_start='@window_start_at', window_end='@window_end_at')}
SET @result=(SELECT @request_id AS batch_id,@work_id AS work_id,@fence AS work_fence,
    @work_revision AS work_revision,@reconcile_id AS reconcile_work_id,
    @frontier_key AS frontier_key,@frontier_revision AS frontier_revision,
    'accepted_for_reconciliation' AS state,JSON_QUERY(@facts_json) AS facts
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True)


def _positions(names: SqlNames, contract: RpcContract) -> KernelObject:
    records = names.table("monitoring_records")
    quote = names.object("json_identity_string")
    event_pair = (
        f"N'['+{quote}(JSON_VALUE(p.payload,'$.delivery.event_source'))+N','"
        f"+{quote}(JSON_VALUE(p.payload,'$.delivery.event_id'))+N']'"
    )
    delivery_key = (
        "N'delivery:v1:'+@epoch+N':'+@tenant_id+N':'+@connector_id+N':'"
        f"+LOWER(CONVERT(char(64),{key_hash(event_pair)},2))"
    )
    body = f"""{partition_identity(names)}
{partition_owner(names)}
IF ISJSON(@positions_json)<>1 OR LEFT(LTRIM(@positions_json),1)<>N'[' OR DATALENGTH(@positions_json)>1048576
    THROW 51073, 'A bounded position batch is required', 1;
DECLARE @start bigint;
SELECT @start=sequence_number FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_start' AND full_key=@partition_key;
IF @start IS NULL THROW 51072, 'Pin the actual broker start before accepting positions', 1;
IF (SELECT COUNT(*) FROM OPENJSON(@positions_json)) NOT BETWEEN 1 AND 200
    THROW 51073, 'Position batch size is outside the fixed bound', 1;
DECLARE @positions TABLE (
    sequence_number bigint NOT NULL PRIMARY KEY,offset_value nvarchar(256) NOT NULL,
    receipt_kind varchar(16) NOT NULL,receipt_key nvarchar(1024) NOT NULL,
    fact_kind varchar(40) NOT NULL,payload nvarchar(max) NOT NULL,
    disposition varchar(16) NOT NULL,enqueued_at nvarchar(40) NOT NULL);
IF EXISTS (
    SELECT 1 FROM OPENJSON(@positions_json)
    WHERE COALESCE(JSON_VALUE(value,'$.receipt_kind'),'') NOT IN ('identified','unidentified')
       OR NULLIF(JSON_VALUE(value,'$.receipt_key'),'') IS NULL
       OR DATALENGTH(JSON_VALUE(value,'$.receipt_key'))>2048
       OR JSON_QUERY(value,'$.receipt') IS NULL
       OR TRY_CONVERT(bigint,JSON_VALUE(value,'$.receipt.position.sequence_number')) IS NULL
       OR TRY_CONVERT(bigint,JSON_VALUE(value,'$.receipt.position.sequence_number'))<@start
       OR NULLIF(JSON_VALUE(value,'$.receipt.position.offset'),'') IS NULL
       OR DATALENGTH(JSON_VALUE(value,'$.receipt.position.offset'))>512
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(value,'$.receipt.position.enqueued_at')) IS NULL
       OR LEN(JSON_VALUE(value,'$.receipt.position.enqueued_at'))>40
       OR COALESCE(JSON_VALUE(value,'$.receipt.partition.tenant_id'),'')<>@tenant_id
       OR COALESCE(JSON_VALUE(value,'$.receipt.partition.epoch'),'')<>@epoch
       OR COALESCE(JSON_VALUE(value,'$.receipt.partition.connector_id'),'')<>@connector_id
       OR COALESCE(JSON_VALUE(value,'$.receipt.partition.consumer_group'),'')<>@consumer_group
       OR COALESCE(JSON_VALUE(value,'$.receipt.partition.partition_id'),'')<>@partition_id
       OR COALESCE(JSON_VALUE(value,'$.receipt.status'),'') NOT IN ('accepted','quarantined')
) THROW 51073, 'Position receipt identity or original offset is invalid', 1;
INSERT INTO @positions
SELECT CONVERT(bigint,JSON_VALUE(value,'$.receipt.position.sequence_number')),
    JSON_VALUE(value,'$.receipt.position.offset'),JSON_VALUE(value,'$.receipt_kind'),
    JSON_VALUE(value,'$.receipt_key'),
    CASE JSON_VALUE(value,'$.receipt_kind') WHEN 'identified' THEN 'signal' ELSE 'unidentified_signal' END,
    JSON_QUERY(value,'$.receipt'),JSON_VALUE(value,'$.receipt.status'),
    JSON_VALUE(value,'$.receipt.position.enqueued_at')
FROM OPENJSON(@positions_json);
IF EXISTS (SELECT 1 FROM @positions WHERE receipt_kind='unidentified' AND disposition<>'quarantined')
    THROW 51073, 'Unidentified input can only be durable quarantine evidence', 1;
IF EXISTS (
    SELECT 1 FROM @positions
    WHERE receipt_kind='identified' AND (
        COALESCE(JSON_VALUE(payload,'$.delivery.tenant_id'),'')<>@tenant_id
        OR COALESCE(JSON_VALUE(payload,'$.delivery.epoch'),'')<>@epoch
        OR COALESCE(JSON_VALUE(payload,'$.delivery.connector_id'),'')<>@connector_id
        OR NULLIF(JSON_VALUE(payload,'$.delivery.event_source'),'') IS NULL
        OR NULLIF(JSON_VALUE(payload,'$.delivery.event_id'),'') IS NULL
        OR (disposition='quarantined' AND JSON_QUERY(payload,'$.quarantine') IS NULL)
        OR (disposition='accepted' AND (
             JSON_QUERY(payload,'$.observation') IS NULL
             OR COALESCE(JSON_VALUE(payload,'$.observation.authority'),'')<>'transport'
            OR COALESCE(JSON_VALUE(payload,'$.observation.origin'),'')<>'event'))
    )
) THROW 51073, 'Transport evidence cannot manufacture REST authority or lose original identity', 1;
IF EXISTS (
    SELECT 1 FROM @positions AS p WHERE p.receipt_kind='identified' AND (
        DATALENGTH(JSON_VALUE(p.payload,'$.delivery.event_source'))>4096
        OR DATALENGTH(JSON_VALUE(p.payload,'$.delivery.event_id'))>512
        OR NOT {exact_text_equal('p.receipt_key', delivery_key)}
    )
) THROW 51073, 'Delivery key must preserve the original opaque source/id JSON digest', 1;
IF EXISTS (
    SELECT 1 FROM @positions WHERE receipt_kind='unidentified'
      AND NOT {exact_text_equal('receipt_key', "@partition_key+N':unidentified:'+CONVERT(nvarchar(30),sequence_number)")}
) THROW 51073, 'Unidentified receipt identity must bind the exact original partition position', 1;
IF EXISTS (
    SELECT 1 FROM @positions AS p WHERE p.disposition='accepted' AND NOT EXISTS (
        SELECT 1 FROM OPENJSON(@connector,'$.sources') AS s
        WHERE JSON_VALUE(s.value,'$.target.tenant_id')=@tenant_id
          AND JSON_VALUE(s.value,'$.target.epoch')=@epoch
          AND JSON_VALUE(s.value,'$.target.workspace_id')=JSON_VALUE(p.payload,'$.observation.execution.target.workspace_id')
          AND JSON_VALUE(s.value,'$.target.item_id')=JSON_VALUE(p.payload,'$.observation.execution.target.item_id')
          AND JSON_VALUE(s.value,'$.target.workload')=JSON_VALUE(p.payload,'$.observation.execution.target.workload')
          AND JSON_VALUE(p.payload,'$.observation.execution.target.tenant_id')=@tenant_id
          AND JSON_VALUE(p.payload,'$.observation.execution.target.epoch')=@epoch
          AND NOT {source_is_pending_removal_sql('s.value', "JSON_QUERY(@connector,'$.source_removals')")}
          AND EXISTS (SELECT 1 FROM {records} AS admitted
              WHERE admitted.tenant_id=@tenant_id AND admitted.epoch=@epoch AND admitted.record_kind='target'
                AND admitted.full_key=N'monitor:v1:'+@epoch+N':'+@tenant_id+N':'
                    +JSON_VALUE(s.value,'$.target.workload')+N':'+JSON_VALUE(s.value,'$.target.workspace_id')
                    +N':'+JSON_VALUE(s.value,'$.target.item_id')
                AND JSON_VALUE(admitted.payload,'$.state')='current'
                AND JSON_VALUE(admitted.payload,'$.observation.enabled')='true'
                AND TRY_CONVERT(bigint,JSON_VALUE(admitted.payload,'$.policy_revision'))=@current_revision)
          AND {exact_text_equal("JSON_VALUE(s.value,'$.event_source')", "JSON_VALUE(p.payload,'$.delivery.event_source')")}
          AND EXISTS (SELECT 1 FROM OPENJSON(s.value,'$.event_types') AS e
                      WHERE {exact_text_equal("e.value", f"({subscription_type_sql('p.payload')})")})
    )
) THROW 51072, 'Accepted event is outside the owned desired source; quarantine explicitly', 1;
IF EXISTS (
    SELECT 1 FROM @positions AS p JOIN {records} AS prior
      ON prior.tenant_id=@tenant_id AND prior.epoch=@epoch AND prior.record_kind='signal'
     AND JSON_VALUE(prior.payload,'$.delivery.connector_id')=@connector_id
     AND {exact_text_equal("JSON_VALUE(prior.payload,'$.delivery.event_source')", "JSON_VALUE(p.payload,'$.delivery.event_source')")}
     AND {exact_text_equal("JSON_VALUE(prior.payload,'$.delivery.event_id')", "JSON_VALUE(p.payload,'$.delivery.event_id')")}
    WHERE p.receipt_kind='identified' AND
       (NOT {exact_text_equal('prior.full_key', 'p.receipt_key')}
        OR {receipt_content_hash('prior.payload')}<>{receipt_content_hash('p.payload')})
) THROW 51072, 'Original event source/id cannot be rebound', 1;
IF EXISTS (
    SELECT 1 FROM @positions AS p JOIN @positions AS other
      ON p.fact_kind=other.fact_kind AND {exact_text_equal('p.receipt_key', 'other.receipt_key')}
    WHERE {receipt_content_hash('p.payload')}<>{receipt_content_hash('other.payload')}
) THROW 51072, 'One delivery identity has conflicting event content in this batch', 1;
IF EXISTS (
    SELECT 1 FROM @positions AS p JOIN {records} AS r
      ON r.tenant_id=@tenant_id AND r.epoch=@epoch AND r.record_kind=p.fact_kind
     AND {exact_text_equal('r.full_key', 'p.receipt_key')}
    WHERE {receipt_content_hash('r.payload')}<>{receipt_content_hash('p.payload')}
) THROW 51072, 'Receipt key belongs to different event evidence', 1;
-- Preserve the first event receipt; each broker position retains its own original offset/time.
UPDATE p SET payload=COALESCE(prior.payload,first_delivery.payload)
FROM @positions AS p
OUTER APPLY (
    SELECT r.payload FROM {records} AS r
    WHERE r.tenant_id=@tenant_id AND r.epoch=@epoch AND r.record_kind=p.fact_kind
      AND r.key_hash={key_hash('p.receipt_key')} AND {exact_text_equal('r.full_key', 'p.receipt_key')}
) AS prior
CROSS APPLY (
    SELECT TOP (1) original.payload FROM @positions AS original
    WHERE original.fact_kind=p.fact_kind AND {exact_text_equal('original.receipt_key', 'p.receipt_key')}
    ORDER BY original.sequence_number
) AS first_delivery;
IF EXISTS (
    SELECT 1 FROM @positions AS p JOIN {records} AS prior
      ON prior.tenant_id=@tenant_id AND prior.epoch=@epoch AND prior.record_kind='stream_position'
     AND prior.parent_key=@partition_key AND prior.sequence_number=p.sequence_number
    WHERE NOT {exact_text_equal("JSON_VALUE(prior.payload,'$.offset')", 'p.offset_value')}
       OR JSON_VALUE(prior.payload,'$.enqueued_at')<>p.enqueued_at
       OR NOT {exact_text_equal("JSON_VALUE(prior.payload,'$.receipt_key')", 'p.receipt_key')}
       OR JSON_VALUE(prior.payload,'$.payload_hash')<>{payload_hash('p.payload')}
) THROW 51072, 'An original stream position or offset cannot be rewritten', 1;
INSERT INTO {records}
    (tenant_id,epoch,record_kind,key_hash,full_key,revision,status,parent_hash,parent_key,payload)
SELECT @tenant_id,@epoch,p.fact_kind,{key_hash('p.receipt_key')},p.receipt_key,1,p.disposition,
    {key_hash('@connector_id')},@connector_id,p.payload
FROM @positions AS p WHERE p.sequence_number=(
    SELECT MIN(first_sequence.sequence_number) FROM @positions AS first_sequence
    WHERE first_sequence.fact_kind=p.fact_kind AND {exact_text_equal('first_sequence.receipt_key', 'p.receipt_key')}
) AND NOT EXISTS (
    SELECT 1 FROM {records} AS r WHERE r.tenant_id=@tenant_id AND r.epoch=@epoch
      AND r.record_kind=p.fact_kind AND {exact_text_equal('r.full_key', 'p.receipt_key')} AND r.key_hash={key_hash('p.receipt_key')});
INSERT INTO {records}
    (tenant_id,epoch,record_kind,key_hash,full_key,revision,status,parent_hash,parent_key,sequence_number,payload)
SELECT @tenant_id,@epoch,'stream_position',
    {key_hash("@partition_key+N':position:'+CONVERT(nvarchar(30),p.sequence_number)")},
    @partition_key+N':position:'+CONVERT(nvarchar(30),p.sequence_number),1,p.disposition,
    {key_hash('@partition_key')},@partition_key,p.sequence_number,
    (SELECT p.offset_value AS offset,p.enqueued_at AS enqueued_at,
        p.receipt_key AS receipt_key,p.receipt_kind AS receipt_kind,
        {payload_hash('p.payload')} AS payload_hash,@request_id AS batch_id
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
FROM @positions AS p WHERE NOT EXISTS (
    SELECT 1 FROM {records} AS r WHERE r.tenant_id=@tenant_id AND r.epoch=@epoch
      AND r.record_kind='stream_position' AND r.parent_key=@partition_key AND r.sequence_number=p.sequence_number);
SET @affected=@@ROWCOUNT;
INSERT INTO {records}
    (tenant_id,epoch,record_kind,key_hash,full_key,revision,parent_hash,parent_key,payload)
SELECT @tenant_id,@epoch,'accepted_fact',
    {key_hash("N'accepted:'+@request_id+N':'+p.fact_kind+N':'+CONVERT(nvarchar(30),p.sequence_number)")},
    N'accepted:'+@request_id+N':'+p.fact_kind+N':'+CONVERT(nvarchar(30),p.sequence_number),1,
    {key_hash('@partition_key')},@partition_key,
    (SELECT @request_id AS batch_id,@fingerprint AS batch_fingerprint,p.fact_kind AS fact_kind,
        p.receipt_key AS fact_key,r.revision AS fact_revision,{payload_hash('p.payload')} AS payload_hash,
        {record_hash('r')} AS row_hash
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
FROM @positions AS p JOIN {records} AS r
  ON r.tenant_id=@tenant_id AND r.epoch=@epoch AND r.record_kind=p.fact_kind
 AND r.key_hash={key_hash('p.receipt_key')} AND {exact_text_equal('r.full_key', 'p.receipt_key')};
DECLARE @intake_target nvarchar(max);
IF NOT EXISTS (SELECT 1 FROM @positions WHERE disposition<>'accepted')
   AND (SELECT COUNT(DISTINCT CONVERT(nvarchar(1024),JSON_QUERY(payload,'$.observation.execution.target')))
        FROM @positions)=1
    SELECT TOP (1) @intake_target=JSON_QUERY(payload,'$.observation.execution.target') FROM @positions;
{handoff_sql(names, producer='worker', operation='worker.commit_positions', topic="N'stream_intake'", reference='@connector_id', target='@intake_target')}
SET @result=(SELECT @request_id AS batch_id,@partition_key AS partition_key,
    JSON_QUERY(@partition_json) AS partition,
    (SELECT COUNT(*) FROM @positions) AS position_count,@reconcile_id AS reconcile_work_id,
    @frontier_key AS frontier_key,@frontier_revision AS frontier_revision,
    JSON_QUERY((SELECT p.sequence_number,p.offset_value AS offset,p.enqueued_at,p.receipt_kind,p.receipt_key,
        JSON_VALUE(j.payload,'$.batch_id') AS first_committed_batch_id,
        JSON_VALUE(j.payload,'$.payload_hash') AS original_payload_hash
        FROM @positions AS p JOIN {records} AS j ON j.tenant_id=@tenant_id AND j.epoch=@epoch
          AND j.record_kind='stream_position' AND j.parent_key=@partition_key AND j.sequence_number=p.sequence_number
        ORDER BY p.sequence_number FOR JSON PATH)) AS positions,
    JSON_QUERY(N'['+(SELECT STRING_AGG(CONVERT(nvarchar(max),{quote}(receipt_key)),N',')
        WITHIN GROUP (ORDER BY sequence_number) FROM @positions)+N']') AS receipt_keys,
    'accepted_for_reconciliation' AS state FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True)


def _checkpoint(names: SqlNames, contract: RpcContract) -> KernelObject:
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    body = f"""{partition_identity(names)}
{partition_owner(names)}
DECLARE @first bigint,@prior_revision bigint,@prior_sequence bigint,@count bigint,@last_offset nvarchar(256),
    @last_enqueued_at nvarchar(40);
SELECT @first=sequence_number FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_start' AND full_key=@partition_key;
IF @first IS NULL THROW 51072, 'No pinned broker start exists', 1;
SELECT @prior_revision=revision,@prior_sequence=sequence_number FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_checkpoint' AND full_key=@partition_key;
IF COALESCE(@prior_revision,0)<>@expected_checkpoint_revision
    THROW 51072, 'Checkpoint revision changed', 1;
SET @first=COALESCE(@prior_sequence+1,@first);
IF @through_sequence_number<@first OR @through_sequence_number-@first>100000
    THROW 51073, 'Checkpoint range is regressive or exceeds the bounded scan', 1;
SELECT @count=COUNT_BIG(*) FROM {records} AS p
WHERE p.tenant_id=@tenant_id AND p.epoch=@epoch AND p.record_kind='stream_position'
  AND p.parent_key=@partition_key AND p.sequence_number BETWEEN @first AND @through_sequence_number
  AND p.status IN ('accepted','quarantined')
  AND EXISTS (SELECT 1 FROM {receipts} AS r
      WHERE r.tenant_id=@tenant_id AND r.epoch=@epoch AND r.operation='worker.commit_positions'
        AND r.request_id=JSON_VALUE(p.payload,'$.batch_id')
        AND JSON_VALUE(r.payload,'$.result.partition_key')=@partition_key
        AND EXISTS (
            SELECT 1 FROM {records} AS fact JOIN {records} AS accepted
              ON accepted.tenant_id=fact.tenant_id AND accepted.epoch=fact.epoch
             AND accepted.record_kind='accepted_fact'
             AND JSON_VALUE(accepted.payload,'$.fact_kind')=fact.record_kind
             AND JSON_VALUE(accepted.payload,'$.fact_key')=fact.full_key
             AND TRY_CONVERT(bigint,JSON_VALUE(accepted.payload,'$.fact_revision'))=fact.revision
            WHERE fact.tenant_id=@tenant_id AND fact.epoch=@epoch
              AND fact.record_kind=CASE JSON_VALUE(p.payload,'$.receipt_kind')
                    WHEN 'identified' THEN 'signal' WHEN 'unidentified' THEN 'unidentified_signal' END
              AND fact.full_key=JSON_VALUE(p.payload,'$.receipt_key')
              AND fact.key_hash={key_hash("JSON_VALUE(p.payload,'$.receipt_key')")}
              AND {payload_hash('fact.payload')}=JSON_VALUE(p.payload,'$.payload_hash')
              AND JSON_VALUE(accepted.payload,'$.batch_id')=r.request_id
              AND JSON_VALUE(accepted.payload,'$.batch_fingerprint')=r.fingerprint
              AND JSON_VALUE(accepted.payload,'$.row_hash')={record_hash('fact')}
        ));
IF @count<>@through_sequence_number-@first+1
    THROW 51072, 'Checkpoint would skip an uncommitted position or intake receipt', 1;
SELECT @last_offset=JSON_VALUE(payload,'$.offset'),@last_enqueued_at=JSON_VALUE(payload,'$.enqueued_at') FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_position'
  AND parent_key=@partition_key AND sequence_number=@through_sequence_number;
IF NOT {exact_text_equal('@last_offset', '@through_offset')}
    THROW 51072, 'Checkpoint offset differs from original accepted evidence', 1;
DECLARE @checkpoint nvarchar(max)=(
    SELECT @partition_key AS partition_key,JSON_QUERY(@partition_json) AS partition,
        @expected_checkpoint_revision+1 AS revision,
        @through_sequence_number AS sequence_number,@through_offset AS offset,
        JSON_QUERY((SELECT @through_sequence_number AS sequence_number,@through_offset AS offset,
            @last_enqueued_at AS enqueued_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER)) AS position,
        CONVERT(nvarchar(40),@now,127)+N'Z' AS updated_at
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
IF @prior_revision IS NULL
BEGIN
    {record_insert(names, 'stream_checkpoint', '@partition_key', '@checkpoint', parent_key='@connector_id', sequence='@through_sequence_number')}
END
ELSE
BEGIN
    UPDATE {records} SET revision=revision+1,sequence_number=@through_sequence_number,payload=@checkpoint
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_checkpoint'
      AND full_key=@partition_key AND key_hash={key_hash('@partition_key')} AND revision=@expected_checkpoint_revision;
    IF @@ROWCOUNT<>1 THROW 51072, 'Checkpoint compare-and-set failed', 1;
END;
SET @affected=1; SET @result=@checkpoint;
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True)


def connector_collection_work_invalid_sql() -> str:
    return """@stored_work_revision<>@work_revision OR @stored_work_status<>'leased'
   OR @stored_work_kind<>'connector_reconcile'
   OR COALESCE(JSON_VALUE(@stored_work,'$.connector_id'),'')<>@connector_id
   OR COALESCE(JSON_VALUE(@stored_work,'$.lease.owner_id'),'')<>@owner_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@stored_work,'$.lease.fence')),-1)<>@fence
   OR @stored_target_key IS NOT NULL
   OR EXISTS (SELECT 1 FROM OPENJSON(@stored_work) WHERE [key] IN ('target','execution') AND type<>0)
   OR JSON_VALUE(@stored_work,'$.action_reservation_id') IS NOT NULL
   OR JSON_VALUE(@stored_work,'$.retry_of') IS NOT NULL
   OR JSON_VALUE(@stored_work,'$.finalization_id') IS NOT NULL
   OR COALESCE(TRY_CONVERT(int,JSON_VALUE(@stored_work,'$.retry_attempt')),-1)<>0"""


def connector_collection_eligible_sql() -> str:
    pending = ",".join(f"'{code}'" for code in sorted(CONNECTOR_PENDING_GAP_CODES))
    return f"""(JSON_VALUE(@observation_json,'$.state')='ready'
    AND JSON_QUERY(@observation_json,'$.observed_definition') IS NOT NULL)
OR (JSON_VALUE(@observation_json,'$.state') IN ('blocked','degraded')
    AND EXISTS (SELECT 1 FROM OPENJSON(@observation_json,'$.gaps') AS gap
        WHERE gap.type=5 AND NULLIF(JSON_VALUE(gap.value,'$.code'),'') IS NOT NULL
          AND NULLIF(JSON_VALUE(gap.value,'$.detail'),'') IS NOT NULL
          AND JSON_VALUE(gap.value,'$.code') NOT IN ({pending})))"""


def _connector(names: SqlNames, contract: RpcContract) -> KernelObject:
    records = names.table("monitoring_records")
    body = f"""{current_work(names, ('connector_reconcile',))}
IF {connector_collection_work_invalid_sql()}
    THROW 51074, 'Connector observation requires its actual current collection work and fence', 1;
IF ISJSON(@observation_json)<>1 OR LEFT(LTRIM(@observation_json),1)<>N'{{'
   OR DATALENGTH(@observation_json)>1048576 OR EXISTS (
    SELECT 1 FROM OPENJSON(@observation_json) WHERE [key] NOT IN
       ('workspace_id','eventstream_id','destination_id','observed_definition','endpoint','operation_id',
        'state','identity_verified_at','delivery_verified_at','gaps'))
    THROW 51073, 'Connector observation includes an unauthorized field', 1;
DECLARE @prior nvarchar(max),@version bigint;
SELECT @prior=payload,@version=revision FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector'
  AND full_key=@connector_id AND key_hash={key_hash('@connector_id')};
IF @prior IS NULL OR @version<>@expected_connector_revision
   OR COALESCE(JSON_VALUE(@prior,'$.ownership_id'),'')<>@ownership_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@prior,'$.policy_revision')),-1)<>@current_revision
    THROW 51072, 'An existing current owned desired connector is required', 1;
IF NOT EXISTS (SELECT 1 FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector_desired'
      AND full_key=@connector_id AND JSON_VALUE(payload,'$.ownership_id')=@ownership_id
      AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.policy_revision'))=@current_revision)
    THROW 51072, 'Connector observation requires protected current desired-publication provenance', 1;
IF EXISTS (
    SELECT 1 FROM OPENJSON(@observation_json)
    WHERE [key] IN ('workspace_id','eventstream_id','destination_id')
      AND JSON_VALUE(@prior,N'$.'+[key]) IS NOT NULL
      AND (type=0 OR value<>JSON_VALUE(@prior,N'$.'+[key]))
) THROW 51072, 'Established connector bindings cannot be replaced or cleared', 1;
IF JSON_QUERY(@observation_json,'$.endpoint') IS NOT NULL AND EXISTS (
    SELECT 1 FROM OPENJSON(@observation_json,'$.endpoint')
    WHERE [key] NOT IN ('namespace','entity','consumer_group'))
    THROW 51073, 'Endpoint accepts nonsecret metadata only', 1;
IF JSON_QUERY(@prior,'$.endpoint') IS NOT NULL
   AND EXISTS (SELECT 1 FROM OPENJSON(@observation_json) WHERE [key]='endpoint')
   AND (JSON_QUERY(@observation_json,'$.endpoint') IS NULL
        OR {payload_hash("JSON_QUERY(@prior,'$.endpoint')")}
           <>{payload_hash("JSON_QUERY(@observation_json,'$.endpoint')")})
    THROW 51072, 'Established endpoint metadata cannot be replaced or cleared', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@observation_json) WHERE
    ([key] IN ('endpoint','observed_definition') AND type NOT IN (0,5))
    OR ([key]='gaps' AND type<>4))
    THROW 51073, 'Connector observation field has the wrong JSON type', 1;
IF JSON_QUERY(@observation_json,'$.endpoint') IS NOT NULL AND (
    NULLIF(JSON_VALUE(@observation_json,'$.endpoint.namespace'),'') IS NULL
    OR JSON_VALUE(@observation_json,'$.endpoint.namespace') COLLATE Latin1_General_100_BIN2 LIKE '%[^A-Za-z0-9.-]%'
    OR NULLIF(JSON_VALUE(@observation_json,'$.endpoint.entity'),'') IS NULL
    OR JSON_VALUE(@observation_json,'$.endpoint.entity') COLLATE Latin1_General_100_BIN2 LIKE '%[^A-Za-z0-9_./-]%'
    OR NULLIF(JSON_VALUE(@observation_json,'$.endpoint.consumer_group'),'') IS NULL
    OR JSON_VALUE(@observation_json,'$.endpoint.consumer_group') COLLATE Latin1_General_100_BIN2 LIKE '%[^A-Za-z0-9$_.-]%')
    THROW 51073, 'Endpoint fields must be nonsecret host/entity/group metadata, not credentials or URLs', 1;
DECLARE @next nvarchar(max)=@prior,@field nvarchar(128),@value nvarchar(max),@type int;
DECLARE fields CURSOR LOCAL FAST_FORWARD FOR SELECT [key],value,type FROM OPENJSON(@observation_json);
OPEN fields;
FETCH NEXT FROM fields INTO @field,@value,@type;
WHILE @@FETCH_STATUS=0
BEGIN
    SET @next=CASE WHEN @type IN (4,5) THEN JSON_MODIFY(@next,N'$.'+@field,JSON_QUERY(@value))
                   ELSE JSON_MODIFY(@next,N'$.'+@field,@value) END;
    FETCH NEXT FROM fields INTO @field,@value,@type;
END;
CLOSE fields; DEALLOCATE fields;
IF COALESCE(JSON_VALUE(@next,'$.state'),'') NOT IN ('provisioning','ready','degraded','blocked')
    THROW 51073, 'Worker cannot perform this connector state transition', 1;
IF JSON_VALUE(@next,'$.state')='ready' AND (
    JSON_QUERY(@next,'$.endpoint') IS NULL
    OR JSON_QUERY(@prior,'$.desired_definition') IS NULL
    OR (SELECT COUNT(*) FROM OPENJSON(@prior,'$.sources'))=0
    OR (SELECT COUNT(*) FROM OPENJSON(@prior,'$.source_proposals'))<>0
    OR (SELECT COUNT(*) FROM OPENJSON(@prior,'$.source_removals'))<>0
    OR NULLIF(JSON_VALUE(@next,'$.workspace_id'),'') IS NULL
    OR NULLIF(JSON_VALUE(@next,'$.eventstream_id'),'') IS NULL
    OR NULLIF(JSON_VALUE(@next,'$.destination_id'),'') IS NULL
    OR JSON_QUERY(@next,'$.observed_definition') IS NULL
    OR {payload_hash("JSON_QUERY(@next,'$.observed_definition')")}<>{payload_hash("JSON_QUERY(@prior,'$.desired_definition')")}
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@next,'$.identity_verified_at')) IS NULL
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@next,'$.delivery_verified_at')) IS NULL
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@next,'$.identity_verified_at'))>@now
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@next,'$.delivery_verified_at'))>@now)
    THROW 51072, 'Ready requires matched topology and nonfuture identity/delivery evidence', 1;
IF JSON_VALUE(@next,'$.state') IN ('blocked','degraded')
   AND (SELECT COUNT(*) FROM OPENJSON(@next,'$.gaps'))=0
    THROW 51073, 'A blocked or degraded connector observation requires explicit gaps', 1;
DECLARE @observation nvarchar(max)=JSON_MODIFY(JSON_MODIFY(@next,'$.revision',@version+1),
    '$.updated_at',CONVERT(nvarchar(40),@now,127)+N'Z');
DECLARE @collection_completion_eligible bit=CASE WHEN {connector_collection_eligible_sql()} THEN 1 ELSE 0 END;
-- These are worker observations, never authority to manufacture new readiness proof.
SET @next={restore_worker_proof_expression()};
IF {worker_ready_upgrade_sql()}
    SET @next=JSON_MODIFY(@next,'$.state','provisioning');
SET @next=JSON_MODIFY(JSON_MODIFY(@next,'$.revision',@version+1),'$.updated_at',CONVERT(nvarchar(40),@now,127)+N'Z');
UPDATE {records} SET revision=revision+1,status=JSON_VALUE(@next,'$.state'),payload=@next
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector'
  AND full_key=@connector_id AND key_hash={key_hash('@connector_id')} AND revision=@version;
IF @@ROWCOUNT<>1 THROW 51072, 'Connector observation lost its revision', 1;
SET @affected=1;
{handoff_sql(names, producer='worker', operation='worker.observe_connector', topic="N'connector'", reference='@connector_id')}
SET @result=(SELECT @connector_id AS connector_id,JSON_QUERY(@next) AS connector,
    JSON_QUERY(@observation) AS observation,
    @work_id AS work_id,@owner_id AS work_owner_id,@fence AS work_fence,
    @work_revision AS work_revision,@collection_completion_eligible AS collection_completion_eligible,
    {payload_hash("JSON_QUERY(@observation_json,'$.observed_definition')")} AS observed_definition_hash,
    'observed_not_action_authority' AS authority,@reconcile_id AS reconcile_work_id,
    @frontier_key AS frontier_key,@frontier_revision AS frontier_revision
    FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True)


def _budget(names: SqlNames, contract: RpcContract) -> KernelObject:
    table = names.table("monitoring_rate_budget")
    body = f"""IF LEN(@bucket) NOT BETWEEN 1 AND 128
   OR @bucket COLLATE Latin1_General_100_BIN2 LIKE '%[^A-Za-z0-9_.:-]%'
   OR (@delay_seconds IS NOT NULL AND @delay_seconds NOT BETWEEN 0 AND 2147483647)
    THROW 51073, 'Invalid rate bucket or cooldown', 1;
DECLARE @bucket_hash char(64)=LOWER(CONVERT(char(64),{key_hash('@bucket')},2)),
    @limit int,@seconds int,@window datetime2(6),@used int,@blocked datetime2(6),@allowed bit=0;
SELECT @limit=request_limit,@seconds=window_seconds,@window=window_ends_at,@used=used,@blocked=blocked_until
FROM {table} WITH (UPDLOCK,HOLDLOCK) WHERE tenant_id=@tenant_id AND bucket_hash=@bucket_hash;
IF @limit IS NULL
    THROW 51076, 'Deployer must provision this reviewed budget policy; runtime cannot choose its limit', 1;
IF @delay_seconds IS NOT NULL
BEGIN
    UPDATE {table} SET blocked_until=CASE WHEN blocked_until>DATEADD(second,@delay_seconds,@now)
        THEN blocked_until ELSE DATEADD(second,@delay_seconds,@now) END
    WHERE tenant_id=@tenant_id AND bucket_hash=@bucket_hash;
END
ELSE
BEGIN
    UPDATE {table} SET used=CASE WHEN window_ends_at<=@now THEN 1 ELSE used+1 END,
        window_ends_at=CASE WHEN window_ends_at<=@now THEN DATEADD(second,window_seconds,@now) ELSE window_ends_at END
    WHERE tenant_id=@tenant_id AND bucket_hash=@bucket_hash
      AND (blocked_until IS NULL OR blocked_until<=@now)
      AND (window_ends_at<=@now OR used<request_limit);
    IF @@ROWCOUNT=1 SET @allowed=1;
END;
SET @affected=CASE WHEN @allowed=1 OR @delay_seconds IS NOT NULL THEN 1 ELSE 0 END;
SET @status=CASE WHEN @allowed=1 THEN 'applied' ELSE 'not_acquired' END;
SET @result=(SELECT @allowed AS allowed,used,request_limit,window_seconds,
    CONVERT(nvarchar(40),window_ends_at,127)+N'Z' AS window_ends_at,
    CONVERT(nvarchar(40),blocked_until,127)+N'Z' AS blocked_until
    FROM {table} WHERE tenant_id=@tenant_id AND bucket_hash=@bucket_hash
    FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);"""
    return procedure(names, contract, body, permit_maintenance=True, check_revision=False)


def intake_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    factories = {
        "worker.accept_facts": _accept_facts,
        "worker.commit_positions": _positions,
        "worker.advance_checkpoint": _checkpoint,
        "worker.observe_connector": _connector,
        "worker.rate_budget": _budget,
        "worker.record_heartbeat": _heartbeat,
    }
    return {name: factory(names, contracts[name]) for name, factory in factories.items()}


def _heartbeat(names: SqlNames, contract: RpcContract) -> KernelObject:
    records = names.table("monitoring_records")
    body = f"""IF @state NOT IN ('starting','running','degraded','stopping','stopped','blocked')
   OR @accepted_positions<0 OR LEN(@worker_id)=0
   OR @last_delivery_at>@now OR @last_maintenance_at>@now
    THROW 51073, 'Heartbeat fields are invalid', 1;
IF @connector_id IS NULL AND (@transport_connected<>0 OR @accepted_positions<>0 OR @last_delivery_at IS NOT NULL)
    THROW 51073, 'Collector-only heartbeat cannot assert event delivery', 1;
IF @connector_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
    AND record_kind='connector' AND full_key=@connector_id)
    THROW 51072, 'Heartbeat requires an owned connector', 1;
DECLARE @heartbeat_key nvarchar(1024)=COALESCE(@connector_id,N'collector')+N':'+@worker_id,@heartbeat_revision bigint;
IF @maintenance=1 AND @state='running' SET @state='blocked';
SELECT @heartbeat_revision=revision FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='receiver_heartbeat' AND full_key=@heartbeat_key;
DECLARE @heartbeat nvarchar(max)=(
    SELECT @tenant_id AS tenant_id,@epoch AS epoch,@worker_id AS worker_id,@connector_id AS connector_id,
        CONVERT(nvarchar(40),@now,127)+N'Z' AS observed_at,
        @state AS state,
        @transport_connected AS transport_connected,@accepted_positions AS accepted_positions,
        CONVERT(nvarchar(40),@last_delivery_at,127)+N'Z' AS last_delivery_at,
        CONVERT(nvarchar(40),@last_maintenance_at,127)+N'Z' AS last_maintenance_at,@error_code AS error_code
    FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
IF @heartbeat_revision IS NULL
BEGIN
    {record_insert(names, 'receiver_heartbeat', '@heartbeat_key', '@heartbeat', status='@state', parent_key='@connector_id')}
END
ELSE UPDATE {records} SET revision=revision+1,status=@state,payload=@heartbeat
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='receiver_heartbeat'
      AND full_key=@heartbeat_key AND revision=@heartbeat_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Heartbeat revision changed', 1;
DECLARE @accepted_key nvarchar(1024)=N'accepted:'+@request_id+N':heartbeat';
DECLARE @accepted_json nvarchar(max)=(
    SELECT @request_id AS batch_id,@fingerprint AS batch_fingerprint,'receiver_heartbeat' AS fact_kind,
        @heartbeat_key AS fact_key,COALESCE(@heartbeat_revision,0)+1 AS fact_revision,
        {payload_hash('@heartbeat')} AS payload_hash,{record_hash('h')} AS row_hash
    FROM {records} AS h WHERE h.tenant_id=@tenant_id AND h.epoch=@epoch
      AND h.record_kind='receiver_heartbeat' AND h.full_key=@heartbeat_key
      AND h.key_hash={key_hash('@heartbeat_key')}
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{record_insert(names, 'accepted_fact', '@accepted_key', '@accepted_json', parent_key='@connector_id')}
SET @affected=1; SET @result=@heartbeat;
{save_receipt(names, contract.operation)}"""
    return procedure(
        names, contract, body, replay=True, permit_maintenance=True, check_revision=False,
    )
