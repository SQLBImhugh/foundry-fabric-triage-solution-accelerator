"""Durable negative transport evidence without moving a stream boundary."""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import (
    partition_identity,
    partition_owner,
    procedure,
    record_insert,
    save_receipt,
)
from triage.monitoring.sql_kernel_contracts import KernelObject, RpcContract, SqlNames


def retention_classification_sql() -> str:
    return """CASE WHEN @first_available_sequence_number>@expected_sequence THEN 'stream_retention_gap'
    WHEN @first_available_sequence_number<@pinned THEN 'stream_boundary_regressed' ELSE NULL END"""


def retention_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    contract = contracts["worker.observe_retention"]
    records = names.table("monitoring_records")
    body = f"""{partition_identity(names)}
{partition_owner(names)}
IF @first_available_sequence_number<0 OR @observed_at>@now OR @expected_checkpoint_revision<0
    THROW 51073, 'Retention observation requires an actual nonfuture broker boundary', 1;
DECLARE @start nvarchar(max),@pinned bigint,@checkpoint nvarchar(max),@checkpoint_revision bigint,
    @checkpoint_sequence bigint,@expected_sequence bigint,@code varchar(40),@gap nvarchar(max),
    @gap_key nvarchar(1024),@existing_gap nvarchar(max);
SELECT @start=payload,@pinned=sequence_number FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_start' AND full_key=@partition_key;
SELECT @checkpoint=payload,@checkpoint_revision=revision,@checkpoint_sequence=sequence_number
FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_checkpoint' AND full_key=@partition_key;
IF @start IS NULL OR @pinned IS NULL OR COALESCE(@checkpoint_revision,0)<>@expected_checkpoint_revision
    THROW 51072, 'Retention must bind the original start and current checkpoint revision', 1;
SET @expected_sequence=COALESCE(@checkpoint_sequence+1,@pinned);
SET @code={retention_classification_sql()};
IF @code IS NOT NULL
BEGIN
    SET @gap_key=@partition_key+N':gap:'+@code+N':'+CONVERT(nvarchar(30),@expected_sequence)
        +N':'+CONVERT(nvarchar(30),@first_available_sequence_number);
    SELECT @existing_gap=payload FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_gap' AND full_key=@gap_key;
    IF @existing_gap IS NULL
    BEGIN
        SET @gap=(SELECT @code AS code,
            CASE WHEN @code='stream_retention_gap' THEN
                N'Broker retention skipped uncheckpointed positions; the checkpoint was not advanced.'
                ELSE N'Broker history precedes the original pinned boundary; reconciliation is required.' END AS detail
            FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
        DECLARE @gap_record nvarchar(max)=(SELECT JSON_QUERY(@partition_json) AS partition,@pinned AS pinned_start,
            @expected_sequence AS expected_sequence,@first_available_sequence_number AS first_available_sequence_number,
            CASE WHEN @code='stream_retention_gap' THEN @expected_sequence END AS missing_from,
            CASE WHEN @code='stream_retention_gap' THEN @first_available_sequence_number-1 END AS missing_through,
            @expected_checkpoint_revision AS checkpoint_revision,@owner_id AS owner_id,@fence AS fence,
            CONVERT(nvarchar(40),@observed_at,127)+N'Z' AS observed_at,
            CONVERT(nvarchar(40),@now,127)+N'Z' AS recorded_at,JSON_QUERY(@gap) AS gap
            FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
        {record_insert(names, 'stream_gap', '@gap_key', '@gap_record', status='@code', parent_key='@partition_key')}
        UPDATE {records} SET revision=revision+1,payload=JSON_MODIFY(payload,'append $.gaps',JSON_QUERY(@gap))
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_start'
          AND full_key=@partition_key AND sequence_number=@pinned;
        IF @@ROWCOUNT<>1 THROW 51072, 'Original pinned stream boundary changed during gap recording', 1;
        SET @existing_gap=@gap_record; SET @affected=1;
    END;
END;
SELECT @start=payload FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_start' AND full_key=@partition_key;
SET @result=(SELECT JSON_QUERY(@partition_json) AS partition,JSON_QUERY(@start) AS start,
    JSON_QUERY(@checkpoint) AS [checkpoint],JSON_QUERY(@existing_gap) AS observation,
    CASE WHEN @code IS NULL THEN 'no_new_gap' ELSE 'gap_recorded' END AS state
    FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return {contract.operation: procedure(names, contract, body, replay=True, permit_maintenance=True)}
