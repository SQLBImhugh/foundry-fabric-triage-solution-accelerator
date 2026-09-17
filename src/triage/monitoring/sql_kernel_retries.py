"""Controller-only linked successors of confirmed no-effect refresh rejection.

Attempt zero is the original reservation; the existing retry policy permits
three deferred attempts (1, 2, 3), with 15/30/60-minute fallback backoff.
Creating a successor never refunds its occupied incident slot or an approval.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import key_hash, record_insert
from triage.monitoring.sql_kernel_contracts import SqlNames
from triage.store.retries import MAX_ATTEMPTS, backoff_seconds


def retry_admission_sql(names: SqlNames, request: str, target: str) -> str:
    records = names.table("monitoring_records")
    return f"""DECLARE @retry_request nvarchar(max)={request},@retry_target nvarchar(max),
    @retry_review nvarchar(max),@retry_capability nvarchar(max);
SELECT @retry_target=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='target' AND full_key={target};
SELECT @retry_review=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='review' AND full_key=JSON_VALUE(@retry_request,'$.review_id');
SELECT @retry_capability=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='target_capability' AND full_key={target};"""


def retry_admission_predicate() -> str:
    return """@maintenance=0
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@retry_request,'$.expected.revision')),-1)=@current_revision
AND COALESCE(JSON_VALUE(@retry_request,'$.action'),'')='powerbi_refresh'
AND COALESCE(JSON_VALUE(@retry_target,'$.state'),'')='current'
AND COALESCE(JSON_VALUE(@retry_target,'$.admission_basis'),'')='reviewed'
AND COALESCE(JSON_VALUE(@retry_target,'$.observation.enabled'),'')='true'
AND COALESCE(JSON_VALUE(@retry_target,'$.action.enabled'),'')='true'
AND COALESCE(JSON_VALUE(@retry_target,'$.action.action'),'')='powerbi_refresh'
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@retry_target,'$.policy_revision')),-1)=@current_revision
AND COALESCE(JSON_VALUE(@retry_target,'$.action.review_id'),'')=COALESCE(JSON_VALUE(@retry_request,'$.review_id'),'missing')
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@retry_target,'$.action.review_revision')),-1)
    =COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@retry_request,'$.expected_review_revision')),-2)
AND COALESCE(JSON_VALUE(@retry_review,'$.state'),'')='verified'
AND COALESCE(JSON_VALUE(@retry_review,'$.publication_status'),'published')='published'
AND COALESCE(JSON_VALUE(@retry_review,'$.parameters_redacted'),'false')='false'
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@retry_review,'$.policy_revision')),-1)=@current_revision
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@retry_review,'$.revision')),-1)
    =COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@retry_request,'$.expected_review_revision')),-2)
AND COALESCE(JSON_VALUE(@retry_review,'$.action'),'')='powerbi_refresh'
AND COALESCE(JSON_VALUE(@retry_review,'$.parameter_hash'),'')=COALESCE(JSON_VALUE(@retry_request,'$.parameter_hash'),'missing')
AND COALESCE(JSON_QUERY(@retry_review,'$.target'),'') COLLATE Latin1_General_100_BIN2
    =COALESCE(JSON_QUERY(@retry_request,'$.source_execution.target'),'missing') COLLATE Latin1_General_100_BIN2
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@retry_review,'$.expires_at')) IS NOT NULL
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@retry_review,'$.expires_at'))>TODATETIMEOFFSET(@now,'+00:00')
AND COALESCE(JSON_VALUE(@retry_capability,'$.read_status'),'')='verified'
AND COALESCE(JSON_VALUE(@retry_capability,'$.action_status'),'')='verified'
AND COALESCE(JSON_VALUE(@retry_capability,'$.exact_action_correlation'),'')='true'
AND COALESCE(JSON_QUERY(@retry_capability,'$.target'),'') COLLATE Latin1_General_100_BIN2
    =COALESCE(JSON_QUERY(@retry_request,'$.source_execution.target'),'missing') COLLATE Latin1_General_100_BIN2
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@retry_capability,'$.expires_at')) IS NOT NULL
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@retry_capability,'$.expires_at'))>TODATETIMEOFFSET(@now,'+00:00')"""


def successor_predicate() -> str:
    """Additional reservation predicates; ordinary current source/policy checks still run."""
    return f"""@retry_parent IS NOT NULL AND @retry_parent_work IS NOT NULL
AND COALESCE(JSON_VALUE(@retry_parent,'$.state'),'')='rejected'
AND COALESCE(JSON_VALUE(@retry_parent,'$.rejection.reason'),'')='throttled'
AND JSON_QUERY(@retry_parent,'$.submitted_execution') IS NULL
AND JSON_VALUE(@retry_parent,'$.submitted_at') IS NULL
AND JSON_QUERY(@retry_parent,'$.configuration') IS NULL
AND COALESCE(JSON_VALUE(@retry_parent,'$.retry_work_id'),'')=@work_id
AND JSON_VALUE(@retry_parent,'$.retry_reservation_id') IS NULL
AND @retry_attempt BETWEEN 1 AND {MAX_ATTEMPTS}
AND COALESCE(TRY_CONVERT(int,JSON_VALUE(@retry_parent,'$.retry_attempt')),-1)+1=@retry_attempt
AND COALESCE(JSON_VALUE(@retry_parent_work,'$.state'),'')='completed'
AND JSON_VALUE(@retry_parent_work,'$.finalization_id') IS NOT NULL
AND COALESCE(JSON_VALUE(@retry_parent_work,'$.action_reservation_id'),'')=@retry_of
AND COALESCE(JSON_VALUE(@retry_parent,'$.request.action'),'')='powerbi_refresh'
AND COALESCE(JSON_VALUE(@request,'$.action'),'')='powerbi_refresh'
AND COALESCE(JSON_QUERY(@retry_parent,'$.request.source_execution'),'') COLLATE Latin1_General_100_BIN2
    =COALESCE(JSON_QUERY(@request,'$.source_execution'),'missing') COLLATE Latin1_General_100_BIN2
AND COALESCE(JSON_QUERY(@retry_parent,'$.request.incident'),'') COLLATE Latin1_General_100_BIN2
    =COALESCE(JSON_QUERY(@request,'$.incident'),'missing') COLLATE Latin1_General_100_BIN2
AND COALESCE(JSON_VALUE(@retry_parent,'$.request.parameter_hash'),'')
    =COALESCE(JSON_VALUE(@request,'$.parameter_hash'),'missing')
AND COALESCE(JSON_VALUE(@retry_parent,'$.request.definition_hash'),'')
    =COALESCE(JSON_VALUE(@request,'$.definition_hash'),'')
AND COALESCE(JSON_VALUE(@retry_parent,'$.request.configuration_hash'),'')
    =COALESCE(JSON_VALUE(@request,'$.configuration_hash'),'')
AND (JSON_VALUE(@request,'$.approval.approval_id') IS NULL
    OR COALESCE(JSON_VALUE(@retry_parent,'$.request.approval.approval_id'),'')
       <>JSON_VALUE(@request,'$.approval.approval_id'))"""


def link_reservation_sql(names: SqlNames) -> str:
    return f"""UPDATE {names.table('monitoring_records')} SET revision=revision+1,
    payload=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(payload,'$.retry_reservation_id',@reservation_id),
        '$.revision',revision+1),'$.updated_at',@updated_at_text)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action'
  AND full_key=@retry_of AND revision=@retry_parent_revision AND status='rejected'
  AND JSON_VALUE(payload,'$.retry_work_id')=@work_id
  AND JSON_VALUE(payload,'$.retry_reservation_id') IS NULL;"""


def budget_debit_expression() -> str:
    return """JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@budget,'$.action_count',@used+@budget_debit),
    '$.revision',@budget_revision+1),'$.updated_at',@updated_at_text)"""


def save_budget_sql(names: SqlNames) -> str:
    return f"""UPDATE {names.table('monitoring_records')} SET revision=revision+1,payload=@budget_json
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='incident_state'
  AND full_key=@incident_key AND revision=@budget_revision;"""


def enqueue_after_rejection_sql(names: SqlNames) -> str:
    """Only embedded in transition_action(rejected), before that operation's receipt."""
    records = names.table("monitoring_records")
    backoff = " ".join(
        f"WHEN {attempt} THEN {backoff_seconds(attempt)}" for attempt in range(1, MAX_ATTEMPTS + 1)
    )
    return f"""DECLARE @retry_work nvarchar(max)=NULL;
IF @transition='rejected'
BEGIN
    {retry_admission_sql(names, "JSON_QUERY(@action,'$.request')", '@stored_target_key')}
    DECLARE @prior_retry_attempt int=TRY_CONVERT(int,JSON_VALUE(@action,'$.retry_attempt')),
        @retry_after int=TRY_CONVERT(int,JSON_VALUE(@action,'$.rejection.retry_after_seconds'));
    IF @prior_retry_attempt IS NULL OR @prior_retry_attempt NOT BETWEEN 0 AND {MAX_ATTEMPTS}
       OR @retry_after IS NULL OR @retry_after<0
        THROW 51073, 'Rejection lost its bounded retry count or Retry-After evidence', 1;
    IF JSON_VALUE(@action,'$.rejection.reason')='throttled' AND @prior_retry_attempt<{MAX_ATTEMPTS}
       AND ({retry_admission_predicate()})
    BEGIN
        DECLARE @successor_id nvarchar(36)=LOWER(CONVERT(nvarchar(36),NEWID())),
            @next_attempt int=@prior_retry_attempt+1,@retry_wait int,@retry_due datetime2(6);
        SET @retry_wait=CASE WHEN @retry_after>0 THEN @retry_after
            ELSE CASE @next_attempt {backoff} END END;
        SET @retry_due=DATEADD(second,@retry_wait,@now);
        SET @retry_work=(SELECT @tenant_id AS tenant_id,@epoch AS epoch,@successor_id AS work_id,
            'deferred_retry' AS kind,@current_revision AS policy_revision,
            CONVERT(nvarchar(40),@now,127)+N'Z' AS created_at,
            CONVERT(nvarchar(40),@retry_due,127)+N'Z' AS due_at,
            JSON_QUERY(@action,'$.request.source_execution.target') AS target,
            JSON_QUERY(@action,'$.request.source_execution') AS execution,
            @reservation_id AS retry_of,@next_attempt AS retry_attempt,
            1 AS revision,0 AS attempts,'queued' AS state,
            'Single linked retry after confirmed no-effect throttling rejection' AS reason
            FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
        {record_insert(names, 'work', '@successor_id', '@retry_work', status="N'queued'", work_kind="N'deferred_retry'", due_at='@retry_due', target_key='@stored_target_key', parent_key='@reservation_id', workspace="JSON_VALUE(@action,'$.request.source_execution.target.workspace_id')", item="JSON_VALUE(@action,'$.request.source_execution.target.item_id')", workload="JSON_VALUE(@action,'$.request.source_execution.target.workload')")}
        DECLARE @retry_source_key nvarchar(1024)=@stored_target_key+N':run:'
            +JSON_VALUE(@action,'$.request.source_execution.run_id_kind')+N':'
            +JSON_VALUE(@action,'$.request.source_execution.run_id');
        DECLARE @retry_link_key nvarchar(1024)=N'deferred_retry:'+@retry_source_key,
            @prior_retry_link nvarchar(max);
        SELECT @prior_retry_link=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
          AND record_kind='source_work' AND full_key=@retry_link_key;
        IF @prior_retry_link IS NOT NULL AND COALESCE(JSON_VALUE(@prior_retry_link,'$.work_id'),'')<>@work_id
            THROW 51072, 'A linked retry cannot replace another source/work lineage', 1;
        DECLARE @retry_link nvarchar(max)=(SELECT @successor_id AS work_id,
            JSON_QUERY(@action,'$.request.source_execution') AS execution FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
        IF @prior_retry_link IS NULL
        BEGIN
            {record_insert(names, 'source_work', '@retry_link_key', '@retry_link', parent_key='@retry_source_key', target_key='@stored_target_key')}
        END
        ELSE UPDATE {records} SET revision=revision+1,payload=@retry_link
            WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source_work'
              AND full_key=@retry_link_key AND JSON_VALUE(payload,'$.work_id')=@work_id;
        IF @@ROWCOUNT<>1 THROW 51072, 'Retry source/work linkage lost its compare-and-set', 1;
        SET @action=JSON_MODIFY(@action,'$.retry_work_id',@successor_id);
    END;
    UPDATE {records} SET revision=revision+1,status='dispositioned',
        payload=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(payload,
            '$.state','dispositioned'),'$.completed_at',CONVERT(nvarchar(40),@now,127)+N'Z'),
            '$.revision',revision+1),'$.disposition','Confirmed no effect; no submitted execution to verify.')
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
      AND work_kind='verify_action' AND status IN ('queued','waiting')
      AND JSON_VALUE(payload,'$.action_reservation_id')=@reservation_id;
END;"""


def linked_retry_lookup_sql(names: SqlNames) -> str:
    records = names.table("monitoring_records")
    return f"""DECLARE @existing_retry nvarchar(max),@existing_parent nvarchar(max);
SELECT @existing_retry=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='work' AND full_key=@work_id AND key_hash={key_hash('@work_id')};
SELECT @existing_parent=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='action' AND full_key=JSON_VALUE(@existing_retry,'$.retry_of');
IF @existing_retry IS NULL OR @existing_parent IS NULL
   OR COALESCE(JSON_VALUE(@existing_parent,'$.state'),'')<>'rejected'
   OR COALESCE(JSON_VALUE(@existing_parent,'$.rejection.reason'),'')<>'throttled'
   OR COALESCE(JSON_VALUE(@existing_parent,'$.retry_work_id'),'')<>@work_id
   OR COALESCE(JSON_VALUE(@draft_json,'$.tenant_id'),'')<>@tenant_id
   OR COALESCE(JSON_VALUE(@draft_json,'$.epoch'),'')<>@epoch
   OR COALESCE(JSON_VALUE(@draft_json,'$.work_id'),'')<>@work_id
   OR JSON_VALUE(@draft_json,'$.action_reservation_id') IS NOT NULL
   OR COALESCE(JSON_QUERY(@draft_json,'$.target'),'') COLLATE Latin1_General_100_BIN2
      <>COALESCE(JSON_QUERY(@existing_retry,'$.target'),'missing') COLLATE Latin1_General_100_BIN2
   OR COALESCE(JSON_QUERY(@draft_json,'$.execution'),'') COLLATE Latin1_General_100_BIN2
      <>COALESCE(JSON_QUERY(@existing_retry,'$.execution'),'missing') COLLATE Latin1_General_100_BIN2
    THROW 51072, 'Deferred enqueue requires the exact successor already created by confirmed rejection', 1;
SET @result=(SELECT @work_id AS work_id,JSON_QUERY(@existing_retry) AS work
    FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);"""
