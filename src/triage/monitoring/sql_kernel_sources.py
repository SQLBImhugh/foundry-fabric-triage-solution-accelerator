"""Lease-bound source publication and atomic non-effect source disposition."""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    current_work,
    key_hash,
    procedure,
    record_hash,
    record_insert,
    save_receipt,
)
from triage.monitoring.sql_kernel_contracts import (
    CONTROLLER_WORK_KINDS,
    KernelObject,
    RpcContract,
    SqlNames,
)
from triage.monitoring.sql_kernel_correlation import execution_reservations_sql


def stale_source_policy_sql() -> str:
    return "(@maintenance=1 OR @expected_revision<>@current_revision) AND @existing_effect IS NULL"


def disposition_records_sql(names: SqlNames) -> tuple[str, str]:
    return (
        record_insert(names, "source_disposition", "@source_key", "@prior_disposition",
                      status="@disposition", target_key="@source_target_key"),
        f"""INSERT INTO {names.table('processed')} (fingerprint,message_id,received_at)
VALUES (LOWER(CONVERT(char(64),{key_hash('@source_key')},2)),@source_key,@recorded_at_text);""",
    )


def source_context_sql(names: SqlNames, *, disposing: bool = False) -> str:
    records, leases = names.table("monitoring_records"), names.table("monitoring_leases")
    equal = names.object("json_equal")
    scope_disposal = f"""IF @evidence_kind IS NULL AND @evidence_key IS NULL AND @alias_window_id IS NULL
       AND @expected_revision=@current_revision AND @maintenance=0
       AND EXISTS (SELECT 1 FROM {records} AS scope_handoff
           WHERE scope_handoff.tenant_id=@tenant_id AND scope_handoff.epoch=@epoch
             AND scope_handoff.record_kind='validation_handoff'
             AND JSON_VALUE(scope_handoff.payload,'$.work_id')=@work_id
             AND TRY_CONVERT(bigint,JSON_VALUE(scope_handoff.payload,'$.policy_revision'))=@current_revision)
    BEGIN
        SELECT @accepted_payload=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
          AND record_kind='source' AND full_key=@source_key;
        IF @accepted_payload IS NULL OR {equal}(@accepted_payload,@observation)<>1
            THROW 51072, 'Scope cleanup needs the exact already-published source, not caller evidence', 1;
    END
    ELSE
    BEGIN""" if disposing else "BEGIN"
    return f"""{current_work(names, CONTROLLER_WORK_KINDS)}
IF @stored_work_revision<>@work_revision OR @stored_work_status NOT IN ('leased','finalizing')
    THROW 51074, 'Source operation lost its actual work revision/lease', 1;
IF ISJSON(@observation_json)<>1 OR DATALENGTH(@observation_json)>131072
   OR EXISTS (SELECT 1 FROM OPENJSON(@observation_json) WHERE [key] NOT IN
      ('execution','origin','authority','observed_at','started_at','ended_at','status','invocation',
       'job_type','error_code','failure_reason','failure_signature','evidence','evidence_truncated'))
    THROW 51073, 'A bounded typed source observation is required', 1;
DECLARE @observation nvarchar(max)=@observation_json,@execution nvarchar(max)=JSON_QUERY(@observation_json,'$.execution'),
    @source_target nvarchar(max)=JSON_QUERY(@observation_json,'$.execution.target'),
    @source_key nvarchar(1024),@source_target_key nvarchar(1024),@existing_effect nvarchar(max),
    @raw_observation nvarchar(max),@accepted_payload nvarchar(max);
IF @execution IS NULL OR @source_target IS NULL
   OR COALESCE(JSON_VALUE(@source_target,'$.tenant_id'),'')<>@tenant_id
   OR COALESCE(JSON_VALUE(@source_target,'$.epoch'),'')<>@epoch
   OR COALESCE(JSON_VALUE(@source_target,'$.workload'),'') NOT IN ('powerbi','fabric_pipeline')
   OR NOT ({canonical_guid("JSON_VALUE(@source_target,'$.workspace_id')")})
   OR NOT ({canonical_guid("JSON_VALUE(@source_target,'$.item_id')")})
   OR COALESCE(JSON_VALUE(@execution,'$.run_id_kind'),'') NOT IN ('powerbi_request','fabric_job')
   OR NOT ({canonical_guid("JSON_VALUE(@execution,'$.run_id')")})
   OR (JSON_VALUE(@source_target,'$.workload')='powerbi'
       AND JSON_VALUE(@execution,'$.run_id_kind')<>'powerbi_request')
   OR (JSON_VALUE(@source_target,'$.workload')='fabric_pipeline'
       AND JSON_VALUE(@execution,'$.run_id_kind')<>'fabric_job')
   OR COALESCE(JSON_VALUE(@observation,'$.authority'),'') NOT IN ('rest','transport')
   OR COALESCE(JSON_VALUE(@observation,'$.origin'),'') NOT IN ('poll','event','mail','operator','deferred_retry')
   OR COALESCE(JSON_VALUE(@observation,'$.status'),'') NOT IN ('not_started','running','succeeded','failed','cancelled','unknown')
   OR (JSON_VALUE(@observation,'$.started_at') IS NOT NULL
       AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.started_at')) IS NULL)
   OR (JSON_VALUE(@observation,'$.ended_at') IS NOT NULL
       AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.ended_at')) IS NULL)
   OR (JSON_VALUE(@observation,'$.started_at') IS NOT NULL AND JSON_VALUE(@observation,'$.ended_at') IS NOT NULL
       AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.ended_at'))
           <TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.started_at')))
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.observed_at')) IS NULL
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.observed_at'))>TODATETIMEOFFSET(@now,'+00:00')
    THROW 51073, 'Source identity, authority or observation time is invalid', 1;
SET @source_target_key=N'monitor:v1:'+@epoch+N':'+@tenant_id+N':'+JSON_VALUE(@source_target,'$.workload')
    +N':'+JSON_VALUE(@source_target,'$.workspace_id')+N':'+JSON_VALUE(@source_target,'$.item_id');
SET @source_key=@source_target_key+N':run:'+JSON_VALUE(@execution,'$.run_id_kind')+N':'+JSON_VALUE(@execution,'$.run_id');
IF @stored_work_kind='reconcile_state'
BEGIN
    {scope_disposal}
    IF @maintenance=1 OR @expected_revision<>@current_revision
       OR @evidence_kind NOT IN ('rest_observation','rest_powerbi_row','signal') OR @evidence_kind IS NULL
       OR @evidence_key IS NULL
        THROW 51072, 'Collection source publication requires current accepted evidence, not a role-only write', 1;
    SELECT @accepted_payload=raw.payload FROM {records} AS raw WITH (UPDLOCK,HOLDLOCK)
    WHERE raw.tenant_id=@tenant_id AND raw.epoch=@epoch AND raw.record_kind=@evidence_kind AND raw.full_key=@evidence_key
      AND EXISTS (SELECT 1 FROM {records} AS binding JOIN {records} AS input_handoff
          ON input_handoff.tenant_id=binding.tenant_id AND input_handoff.epoch=binding.epoch
         AND input_handoff.record_kind='validation_handoff'
         AND JSON_VALUE(input_handoff.payload,'$.producer_request_id')=JSON_VALUE(binding.payload,'$.batch_id')
          JOIN {records} AS own_handoff ON own_handoff.tenant_id=input_handoff.tenant_id
         AND own_handoff.epoch=input_handoff.epoch AND own_handoff.record_kind='validation_handoff'
         AND own_handoff.parent_key=input_handoff.parent_key AND JSON_VALUE(own_handoff.payload,'$.work_id')=@work_id
          JOIN {names.table('monitoring_receipts')} AS accepted_receipt ON accepted_receipt.tenant_id=@tenant_id
         AND accepted_receipt.epoch=@epoch
         AND accepted_receipt.operation=JSON_VALUE(input_handoff.payload,'$.producer_operation')
         AND accepted_receipt.request_id=JSON_VALUE(binding.payload,'$.batch_id')
         AND accepted_receipt.fingerprint=JSON_VALUE(binding.payload,'$.batch_fingerprint')
          WHERE binding.tenant_id=raw.tenant_id AND binding.epoch=raw.epoch AND binding.record_kind='accepted_fact'
            AND JSON_VALUE(binding.payload,'$.fact_kind')=raw.record_kind
            AND JSON_VALUE(binding.payload,'$.fact_key')=raw.full_key
            AND TRY_CONVERT(bigint,JSON_VALUE(binding.payload,'$.fact_revision'))=raw.revision
            AND JSON_VALUE(binding.payload,'$.row_hash')={record_hash('raw')}
            AND TRY_CONVERT(bigint,JSON_VALUE(own_handoff.payload,'$.policy_revision'))=@current_revision);
    IF @accepted_payload IS NULL
        THROW 51072, 'Source observation is not in this work window accepted receipt set', 1;
    SET @raw_observation=CASE WHEN @evidence_kind='rest_observation' THEN @accepted_payload
        ELSE JSON_QUERY(@accepted_payload,'$.observation') END;
    IF @evidence_kind='rest_powerbi_row' AND @alias_window_id IS NOT NULL
    BEGIN
        DECLARE @refresh_id nvarchar(256)=JSON_VALUE(@accepted_payload,'$.refresh_id'),@alias nvarchar(max),@reverse nvarchar(max);
        SELECT @alias=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
          AND record_kind='powerbi_alias' AND full_key=@alias_window_id+N':refresh:'+@refresh_id;
        SELECT @reverse=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
          AND record_kind='powerbi_alias' AND full_key=@alias_window_id+N':request:'+JSON_VALUE(@execution,'$.run_id');
        IF @refresh_id IS NOT NULL AND (@alias IS NULL
            OR (SELECT COUNT(*) FROM OPENJSON(@alias,'$.mapped_ids'))<>1
            OR JSON_VALUE(@alias,'$.mapped_ids[0]')<>JSON_VALUE(@execution,'$.run_id'))
            THROW 51072, 'Canonical source is not the reviewed unique refresh/request alias', 1;
        IF @reverse IS NULL OR (SELECT COUNT(*) FROM OPENJSON(@reverse,'$.mapped_ids'))>1
            THROW 51072, 'Canonical source reverse alias is ambiguous or absent', 1;
        IF @refresh_id IS NOT NULL AND ((SELECT COUNT(*) FROM OPENJSON(@reverse,'$.mapped_ids'))<>1
           OR JSON_VALUE(@reverse,'$.mapped_ids[0]')<>@refresh_id)
            THROW 51072, 'Canonical source forward and reverse aliases disagree', 1;
        IF NOT EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
            AND record_kind='powerbi_window' AND full_key=@alias_window_id AND status='validated'
            AND target_key=@source_target_key)
            THROW 51072, 'Canonical alias publication requires the complete exact target window', 1;
        SET @raw_observation=JSON_MODIFY(JSON_MODIFY(@raw_observation,'$.execution.run_id_kind','powerbi_request'),
            '$.execution.run_id',JSON_VALUE(@execution,'$.run_id'));
        SET @raw_observation=JSON_MODIFY(@raw_observation,'$.evidence.request_id',JSON_VALUE(@execution,'$.run_id'));
    END;
    IF {equal}(@raw_observation,@observation)<>1
        THROW 51072, 'Controller source publication differs from the accepted observation', 1;
    END;
END
ELSE
BEGIN
    IF @evidence_kind IS NOT NULL OR @evidence_key IS NOT NULL OR @alias_window_id IS NOT NULL
       OR JSON_VALUE(@observation,'$.authority')<>'rest'
       OR @source_target_key<>@stored_target_key
        THROW 51072, 'Fresh work source reads require exact owned-target REST evidence', 1;
    IF NOT EXISTS (SELECT 1 FROM {leases} WITH (UPDLOCK,HOLDLOCK)
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=N'controller:'+@stored_target_key
          AND owner_id=@work_id AND expires_at>@now)
        THROW 51074, 'Fresh source publication lost its actual controller target lease', 1;
    SELECT @existing_effect=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
      AND record_kind='action' AND full_key=JSON_VALUE(@stored_work,'$.action_reservation_id')
      AND target_key=@stored_target_key;
    IF {stale_source_policy_sql()}
        THROW 51072, 'Only an existing reserved effect may refresh under changed policy or maintenance', 1;
END;"""


def source_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    records = names.table("monitoring_records")
    publish = contracts["controller.publish_source"]
    publish_body = f"""{source_context_sql(names)}
DECLARE @prior_source nvarchar(max),@source_revision bigint,@head nvarchar(max),@head_revision bigint;
SELECT @prior_source=payload,@source_revision=revision FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source' AND full_key=@source_key;
IF @prior_source IS NOT NULL AND (
    (JSON_VALUE(@prior_source,'$.authority')='rest' AND JSON_VALUE(@observation,'$.authority')='transport')
    OR (JSON_VALUE(@prior_source,'$.authority')=JSON_VALUE(@observation,'$.authority')
        AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@prior_source,'$.observed_at'))
            >TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.observed_at'))))
    SET @observation=@prior_source;
ELSE IF @prior_source IS NULL
BEGIN
    {record_insert(names, 'source', '@source_key', '@observation', status="JSON_VALUE(@observation,'$.status')", target_key='@source_target_key', parent_key='@source_target_key')}
    SET @affected=1;
END
ELSE
BEGIN
    UPDATE {records} SET revision=revision+1,status=JSON_VALUE(@observation,'$.status'),payload=@observation
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source'
      AND full_key=@source_key AND revision=@source_revision;
    IF @@ROWCOUNT<>1 THROW 51072, 'Source observation revision changed during publication', 1;
    SET @affected=1;
END;
SELECT @head=payload,@head_revision=revision FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source_head' AND full_key=@source_target_key;
IF JSON_VALUE(@observation,'$.authority')='rest'
   AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.started_at')) IS NOT NULL
   AND (@head IS NULL OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@head,'$.started_at')) IS NULL
        OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.started_at'))
            >=TRY_CONVERT(datetimeoffset,JSON_VALUE(@head,'$.started_at'))
        OR {names.object('json_equal')}(JSON_QUERY(@head,'$.execution'),@execution)=1)
BEGIN
    IF @head IS NULL
    BEGIN
        {record_insert(names, 'source_head', '@source_target_key', '@observation', target_key='@source_target_key')}
    END
    ELSE UPDATE {records} SET revision=revision+1,payload=@observation
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source_head'
          AND full_key=@source_target_key AND revision=@head_revision;
    IF @@ROWCOUNT<>1 THROW 51072, 'Source head compare-and-set failed', 1;
END;
SET @result=(SELECT @source_key AS source_key,JSON_QUERY(@observation) AS observation
    FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, publish.operation)}"""
    dispose = contracts["controller.disposition_source"]
    disposition_body = f"""{source_context_sql(names, disposing=True)}
IF @disposition NOT IN ('historical','out_of_scope','unsupported','cancelled','superseded','refused','duplicate')
   OR NULLIF(@detail,'') IS NULL
    THROW 51073, 'A fixed non-effect source disposition and detail are required', 1;
IF @existing_effect IS NOT NULL OR JSON_VALUE(@stored_work,'$.action_reservation_id') IS NOT NULL
   OR EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='submitted_action'
       AND full_key=@source_key)
   OR EXISTS ({execution_reservations_sql(names)})
    THROW 51072, 'A reserved source retains incident verification/finalization, not non-effect disposition', 1;
IF @stored_work_kind<>'reconcile_state'
   AND {names.object('json_equal')}(JSON_QUERY(@stored_work,'$.execution'),@execution)<>1
    THROW 51072, 'Non-effect work disposition must identify its exact original source', 1;
IF (@subject_work_id IS NULL AND @expected_subject_revision IS NOT NULL)
   OR (@subject_work_id IS NOT NULL AND (@expected_subject_revision IS NULL OR @stored_work_kind<>'reconcile_state'))
    THROW 51073, 'Unclaimed source work cleanup needs a bound reconciliation owner and exact subject revision', 1;
IF @subject_work_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM {records} AS subject WHERE subject.tenant_id=@tenant_id AND subject.epoch=@epoch
      AND subject.record_kind='work' AND subject.full_key=@subject_work_id
      AND subject.revision=@expected_subject_revision AND subject.status IN ('queued','waiting')
      AND JSON_VALUE(subject.payload,'$.action_reservation_id') IS NULL
      AND {names.object('json_equal')}(JSON_QUERY(subject.payload,'$.execution'),@execution)=1
      AND NOT EXISTS (SELECT 1 FROM {names.table('monitoring_leases')} AS held
          WHERE held.tenant_id=@tenant_id AND held.epoch=@epoch
            AND held.full_key=N'work:v1:'+@epoch+N':'+@tenant_id+N':'+@subject_work_id
            AND held.expires_at>@now))
    THROW 51074, 'Subject source work is no longer unclaimed or changed its exact execution/revision', 1;
DECLARE @prior_disposition nvarchar(max),@processed bit=0;
SELECT @prior_disposition=payload FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source_disposition' AND full_key=@source_key;
IF EXISTS (SELECT 1 FROM {names.table('processed')} WITH (UPDLOCK,HOLDLOCK)
    WHERE fingerprint=LOWER(CONVERT(char(64),{key_hash('@source_key')},2))) SET @processed=1;
IF (@prior_disposition IS NULL AND @processed=1) OR (@prior_disposition IS NOT NULL AND @processed=0)
    THROW 51072, 'Source disposition and processed marker disagree; no runtime repair is permitted', 1;
IF @prior_disposition IS NULL
BEGIN
    IF @maintenance=1 OR @expected_revision<>@current_revision
        THROW 51072, 'New non-effect source disposition requires current policy', 1;
    IF @disposition='duplicate' THROW 51072, 'A duplicate must reference an existing committed disposition', 1;
    IF @disposition IN ('unsupported','cancelled','superseded','refused')
       AND JSON_VALUE(@observation,'$.authority')<>'rest'
        THROW 51072, 'Operational no-effect classification requires authoritative REST evidence', 1;
    IF @disposition='historical' AND (
        TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.started_at')) IS NULL
        OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.started_at'))>=TODATETIMEOFFSET(@cutoff,'+00:00'))
        THROW 51072, 'Historical non-effect disposition must predate activation', 1;
    IF @disposition='out_of_scope' AND EXISTS (SELECT 1 FROM {records}
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='target' AND full_key=@source_target_key
          AND JSON_VALUE(payload,'$.state')='current' AND JSON_VALUE(payload,'$.observation.enabled')='true'
          AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.policy_revision'))=@current_revision)
        THROW 51072, 'Current admitted source cannot be labelled out of scope', 1;
    IF @disposition='cancelled' AND COALESCE(JSON_VALUE(@observation,'$.status'),'')<>'cancelled'
        THROW 51072, 'Cancelled disposition needs exact cancelled evidence', 1;
    IF @disposition='superseded' AND NOT EXISTS (SELECT 1 FROM {records}
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source_head' AND full_key=@source_target_key
          AND TRY_CONVERT(datetimeoffset,JSON_VALUE(payload,'$.started_at'))
              >TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.started_at')))
        THROW 51072, 'Superseded disposition requires a newer protected source', 1;
    IF @disposition='unsupported' AND NOT (
        JSON_VALUE(@source_target,'$.workload')='fabric_pipeline' AND (
            COALESCE(JSON_VALUE(@observation,'$.invocation'),'')<>'scheduled'
            OR COALESCE(JSON_VALUE(@observation,'$.job_type'),'')<>'Pipeline'
            OR COALESCE(JSON_VALUE(@observation,'$.status'),'') NOT IN ('failed','unknown')))
        THROW 51072, 'Unsupported disposition requires ineligible pipeline evidence', 1;
    IF @disposition='refused' AND NOT EXISTS (SELECT 1 FROM {records}
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='controller_validation' AND full_key=@work_id
          AND JSON_VALUE(payload,'$.noneffect_disposition')='refused'
          AND JSON_VALUE(payload,'$.source_key')=@source_key
          AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.work_fence'))=@fence
          AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.policy_revision'))=@current_revision
          AND TRY_CONVERT(datetimeoffset,JSON_VALUE(payload,'$.expires_at'))>TODATETIMEOFFSET(@now,'+00:00'))
        THROW 51072, 'Refusal needs its current protected deterministic validation', 1;
    SET @prior_disposition=(SELECT JSON_QUERY(@execution) AS execution,@disposition AS disposition,@detail AS detail,
        @work_id AS work_id,@request_id AS disposition_request_id,CONVERT(nvarchar(40),@now,127)+N'Z' AS recorded_at
        FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
    DECLARE @recorded_at_text nvarchar(40)=CONVERT(nvarchar(40),@now,127)+N'Z';
    {disposition_records_sql(names)[0]}
    {disposition_records_sql(names)[1]}
    SET @affected=1;
END;
IF @subject_work_id IS NOT NULL
BEGIN
    UPDATE {records} SET revision=revision+1,status='dispositioned',
        payload=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(payload,'$.state','dispositioned'),
            '$.revision',revision+1),'$.completed_at',CONVERT(nvarchar(40),@now,127)+N'Z'),'$.disposition',@detail)
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
      AND full_key=@subject_work_id AND revision=@expected_subject_revision AND status IN ('queued','waiting');
    IF @@ROWCOUNT<>1 THROW 51074, 'Unclaimed source-work disposition lost its compare-and-set', 1;
END;
IF @stored_work_kind<>'reconcile_state'
BEGIN
    SET @stored_work=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@stored_work,
        '$.state','dispositioned'),'$.lease',NULL),'$.completed_at',CONVERT(nvarchar(40),@now,127)+N'Z'),
        '$.disposition',@detail),'$.revision',@work_revision+1);
    UPDATE {records} SET revision=revision+1,status='dispositioned',payload=@stored_work
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work' AND full_key=@work_id AND revision=@work_revision;
    IF @@ROWCOUNT<>1 THROW 51074, 'Non-effect work disposition lost its revision', 1;
    UPDATE {names.table('monitoring_leases')} SET expires_at=@now
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@work_key AND owner_id=@owner_id AND fence=@fence;
    IF @@ROWCOUNT<>1 THROW 51074, 'Non-effect work disposition lost its own lease', 1;
    UPDATE {names.table('monitoring_leases')} SET expires_at=@now
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=N'controller:'+@source_target_key AND owner_id=@work_id;
    IF @@ROWCOUNT<>1 THROW 51074, 'Non-effect disposition lost its actual target lease', 1;
END;
SET @result=(SELECT @source_key AS source_key,JSON_QUERY(@prior_disposition) AS disposition,
    JSON_QUERY(@stored_work) AS work
    FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, dispose.operation)}"""
    return {
        publish.operation: procedure(names, publish, publish_body, replay=True, permit_maintenance=True, check_revision=False),
        dispose.operation: procedure(names, dispose, disposition_body, replay=True, permit_maintenance=True, check_revision=False),
    }
