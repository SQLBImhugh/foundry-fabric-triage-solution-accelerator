"""Server-derived incident occurrence merges; historical evidence never wins.

The original SQL NVARCHAR payload is the starting point for a historical merge.
Only occurrence_count can change there. Source disposition and occurrence-marker
rows append the older source's metadata in the same transaction.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import key_hash, record_insert
from triage.monitoring.sql_kernel_contracts import SqlNames


def historical_predicate() -> str:
    return """@budget IS NOT NULL
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@budget,'$.latest_started_at')) IS NOT NULL
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@plan,'$.source_started_at'))
    <TRY_CONVERT(datetimeoffset,JSON_VALUE(@budget,'$.latest_started_at'))"""


def historical_payload_expression() -> str:
    return "JSON_MODIFY(@prior,'$.occurrence_count',@next_occurrences)"


def occurrence_increment_expression() -> str:
    return "CASE WHEN @source_recorded=0 AND @stored_work_kind<>'verify_action' THEN 1 ELSE 0 END"


def retain_budget_head_expression() -> str:
    return """JSON_MODIFY(JSON_MODIFY(@budget,'$.revision',@budget_revision+1),
    '$.updated_at',@updated_at_text)"""


def save_incident_sql(names: SqlNames) -> str:
    return f"""UPDATE {names.table('incidents')} SET
    signature=CASE WHEN @historical=1 THEN signature ELSE @signature END,
    status=CASE WHEN @historical=1 THEN status ELSE JSON_VALUE(@merged,'$.status') END,
    updated_at=CASE WHEN @historical=1 THEN updated_at ELSE @updated_at_text END,payload=@merged
WHERE incident_id=@incident_id AND HASHBYTES('SHA2_256',payload)=@prior_payload_digest;"""


def prepare_incident_merge_sql(names: SqlNames) -> str:
    records = names.table("monitoring_records")
    return f"""DECLARE @historical bit=CASE WHEN {historical_predicate()} THEN 1 ELSE 0 END,
    @source_recorded bit=0,@prior_occurrences int,@next_occurrences int,
    @occurrence_key nvarchar(1024)=@incident_key+N':occurrence:'+LOWER(CONVERT(char(64),{key_hash('@source_key')},2)),
    @updated_at_text nvarchar(40)=CONVERT(nvarchar(40),@now,127)+N'Z';
IF @budget IS NOT NULL AND (
    COALESCE(TRY_CONVERT(int,JSON_VALUE(@budget,'$.action_count')),-1)<0
    OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@budget,'$.revision')),-1)<>@budget_revision)
    THROW 51072, 'Incident budget metadata is malformed, not a fresh action allowance', 1;
IF @historical=1 AND @prior IS NULL
    THROW 51072, 'Historical merge requires the original newer incident payload', 1;
IF @prior IS NOT NULL AND COALESCE(JSON_VALUE(@prior,'$.signature'),'')<>@signature
    THROW 51072, 'Original incident signature cannot be changed by finalization', 1;
IF EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
    AND ((record_kind='source_disposition' AND full_key=@source_key)
         OR (record_kind='incident_occurrence' AND full_key=@occurrence_key)))
   OR EXISTS (SELECT 1 FROM {names.table('processed')}
       WHERE fingerprint=LOWER(CONVERT(char(64),{key_hash('@source_key')},2)))
    SET @source_recorded=1;
IF @prior IS NOT NULL
BEGIN
    SET @prior_occurrences=TRY_CONVERT(int,JSON_VALUE(@prior,'$.occurrence_count'));
    IF @prior_occurrences IS NULL OR @prior_occurrences<1
       OR COALESCE(TRY_CONVERT(int,JSON_VALUE(@prior,'$.notified_count')),-1)<0
        THROW 51072, 'Original incident counters are missing or malformed', 1;
    SET @next_occurrences=@prior_occurrences+({occurrence_increment_expression()});
    IF @historical=1
        SET @merged={historical_payload_expression()};
    ELSE
    BEGIN
        SET @merged=JSON_MODIFY(@merged,'$.occurrence_count',@next_occurrences);
        SET @merged=JSON_MODIFY(@merged,'$.notified_count',
            CASE WHEN COALESCE(TRY_CONVERT(int,JSON_VALUE(@merged,'$.notified_count')),0)
                         >CONVERT(int,JSON_VALUE(@prior,'$.notified_count'))
                 THEN CONVERT(int,JSON_VALUE(@merged,'$.notified_count'))
                 ELSE CONVERT(int,JSON_VALUE(@prior,'$.notified_count')) END);
        SET @merged=JSON_MODIFY(@merged,'$.first_seen_at',JSON_VALUE(@prior,'$.first_seen_at'));
        IF TRY_CONVERT(datetimeoffset,JSON_VALUE(@merged,'$.last_seen_at')) IS NULL
           OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@prior,'$.last_seen_at')) IS NULL
            THROW 51072, 'Finalization needs comparable original incident timestamps', 1;
        IF TRY_CONVERT(datetimeoffset,JSON_VALUE(@prior,'$.last_seen_at'))
            >TRY_CONVERT(datetimeoffset,JSON_VALUE(@merged,'$.last_seen_at'))
            SET @merged=JSON_MODIFY(@merged,'$.last_seen_at',JSON_VALUE(@prior,'$.last_seen_at'));
    END;
END
ELSE
BEGIN
    SET @next_occurrences=CASE WHEN COALESCE(TRY_CONVERT(int,JSON_VALUE(@merged,'$.occurrence_count')),0)>1
        THEN CONVERT(int,JSON_VALUE(@merged,'$.occurrence_count')) ELSE 1 END;
    SET @merged=JSON_MODIFY(@merged,'$.occurrence_count',@next_occurrences);
END;"""


def occurrence_marker_sql(names: SqlNames) -> str:
    return f"""IF NOT EXISTS (SELECT 1 FROM {names.table('monitoring_records')}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='incident_occurrence'
      AND full_key=@occurrence_key)
BEGIN
    DECLARE @occurrence_payload nvarchar(max)=(SELECT @incident_id AS incident_id,
        @source_key AS source_key,@work_id AS first_work_id,@historical AS historical,
        @updated_at_text AS recorded_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
    {record_insert(names, 'incident_occurrence', '@occurrence_key', '@occurrence_payload', parent_key='@incident_key', target_key='@stored_target_key')}
END;"""
