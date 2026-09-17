"""Operation-specific approval and action fences.

Controller reconciliation publication is separate. A reservation requires its
protected current projection and validation record, not a worker-supplied fact.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_arguments import action_arguments_guard, approval_arguments_guard
from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    current_work,
    key_hash,
    literals,
    payload_hash,
    procedure,
    record_insert,
    save_receipt,
)
from triage.monitoring.sql_kernel_contracts import (
    ACTION_WORK_KINDS,
    SOURCE_DISPOSITIONS,
    KernelObject,
    RpcContract,
    SqlNames,
)
from triage.monitoring.sql_kernel_correlation import record_submitted_correlation_sql
from triage.monitoring.sql_kernel_frontiers import reservation_frontier_guard
from triage.monitoring.sql_kernel_history import (
    occurrence_marker_sql,
    prepare_incident_merge_sql,
    retain_budget_head_expression,
    save_incident_sql,
)
from triage.monitoring.sql_kernel_retries import (
    budget_debit_expression,
    enqueue_after_rejection_sql,
    link_reservation_sql,
    retry_admission_predicate,
    retry_admission_sql,
    save_budget_sql,
    successor_predicate,
)


def _approval(names: SqlNames, contract: RpcContract, operation: str) -> KernelObject:
    table = names.table("approvals")
    common = f"""IF @channel NOT IN ('web','teams') OR LEN(@approval_id)=0 OR LEN(@approval_fingerprint)=0
    THROW 51073, 'Explicit approval identity, channel and fingerprint are required', 1;
DECLARE @approval nvarchar(max);
SELECT @approval=payload FROM {table} WITH (UPDLOCK,HOLDLOCK) WHERE request_id=@approval_id;
SET @now=SYSUTCDATETIME();"""
    if operation == "open":
        body = f"""{common}
IF @approval IS NOT NULL THROW 51072, 'Approval identity already exists; only original receipt replay is allowed', 1;
IF ISJSON(@proposal_json)<>1 OR DATALENGTH(@proposal_json)>1048576
   OR COALESCE(JSON_VALUE(@proposal_json,'$.request_id'),'')<>@approval_id
   OR COALESCE(JSON_VALUE(@proposal_json,'$.fingerprint'),'')<>@approval_fingerprint
   OR NULLIF(JSON_VALUE(@proposal_json,'$.action'),'') IS NULL
   OR COALESCE(JSON_VALUE(@proposal_json,'$.decision'),'')<>''
   OR COALESCE(JSON_VALUE(@proposal_json,'$.consumed_at'),'')<>''
   OR @expires_at<=@now OR @expires_at>DATEADD(day,1,@now)
    THROW 51073, 'Only a bounded new pending approval can be opened', 1;
SET @approval=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(
    @proposal_json,'$.delivery_channel',@channel),'$.requested_at',CONVERT(nvarchar(40),@now,127)+N'Z'),
    '$.expires_at',CONVERT(nvarchar(40),@expires_at,127)+N'Z'),'$.decision',''),'$.consumed_at','');
SET @approval=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@approval,'$.responder',''),'$.decided_at',''),'$.reason','');
INSERT INTO {table} (request_id,decision,responder,decided_at,payload)
VALUES (@approval_id,NULL,NULL,NULL,@approval);
SET @affected=1;
SET @result=(SELECT @approval_id AS approval_id,'pending' AS state,@channel AS channel
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
        return procedure(names, contract, body, replay=True)
    validation = """IF @approval IS NULL
   OR COALESCE(JSON_VALUE(@approval,'$.delivery_channel'),'')<>@channel
   OR COALESCE(JSON_VALUE(@approval,'$.fingerprint'),'')<>@approval_fingerprint
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.expires_at')) IS NULL
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.expires_at'))<=TODATETIMEOFFSET(@now,'+00:00')
   OR COALESCE(JSON_VALUE(@approval,'$.consumed_at'),'')<>''
    THROW 51072, 'Approval channel/fingerprint/expiry/unused guard failed', 1;"""
    if operation == "decide":
        body = f"""{common}
{validation}
IF @decision NOT IN ('approve','decline') OR LEN(@responder)=0
   OR COALESCE(JSON_VALUE(@approval,'$.decision'),'')<>''
    THROW 51072, 'An explicit single-assignment approval decision is required', 1;
UPDATE {table} SET decision=@decision,responder=@responder,
    decided_at=CONVERT(nvarchar(40),@now,127)+N'Z',
    payload=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(payload,
        '$.decision',@decision),'$.responder',@responder),'$.reason',@reason),
        '$.decided_at',CONVERT(nvarchar(40),@now,127)+N'Z')
WHERE request_id=@approval_id AND COALESCE(decision,'')=''
  AND COALESCE(JSON_VALUE(payload,'$.consumed_at'),'')=''
  AND JSON_VALUE(payload,'$.fingerprint')=@approval_fingerprint
  AND JSON_VALUE(payload,'$.delivery_channel')=@channel;
IF @@ROWCOUNT<>1 THROW 51072, 'Approval decision lost its original predicate', 1;
SET @affected=1;
SET @result=(SELECT @approval_id AS approval_id,@decision AS decision,@channel AS channel
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    else:
        body = f"""{common}
{validation}
IF COALESCE(JSON_VALUE(@approval,'$.decision'),'')<>'approve'
    THROW 51072, 'Only an explicit approval may be consumed', 1;
UPDATE {table} SET payload=JSON_MODIFY(payload,'$.consumed_at',CONVERT(nvarchar(40),@now,127)+N'Z')
WHERE request_id=@approval_id AND decision='approve'
  AND COALESCE(JSON_VALUE(payload,'$.consumed_at'),'')=''
  AND JSON_VALUE(payload,'$.fingerprint')=@approval_fingerprint
  AND JSON_VALUE(payload,'$.delivery_channel')=@channel;
IF @@ROWCOUNT<>1 THROW 51072, 'Approval consumption lost its original predicate', 1;
SET @affected=1;
SET @result=(SELECT @approval_id AS approval_id,'consumed' AS state,@channel AS channel
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True)


def _reserve(names: SqlNames, contract: RpcContract) -> KernelObject:
    table, approvals = names.table("monitoring_records"), names.table("approvals")
    body = f"""{current_work(names, ('triage','deferred_retry'), target_lease=True)}
IF NOT ({canonical_guid('@reservation_id')})
    THROW 51073, 'Reservation must be a canonical nonempty GUID', 1;
IF @stored_work_revision<>@work_revision OR @stored_work_status<>'leased'
   OR JSON_VALUE(@stored_work,'$.action_reservation_id') IS NOT NULL
    THROW 51072, 'Only current unreserved owned controller work may reserve a new action', 1;
IF COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@stored_work,'$.policy_revision')),-1)<>@current_revision
    THROW 51072, 'Owned work predates the current action policy', 1;
DECLARE @retry_of nvarchar(128)=JSON_VALUE(@stored_work,'$.retry_of'),
    @retry_attempt int=TRY_CONVERT(int,JSON_VALUE(@stored_work,'$.retry_attempt')),
    @retry_parent nvarchar(max),@retry_parent_work nvarchar(max),@retry_parent_revision bigint,
    @budget_debit int=1;
IF (@stored_work_kind='deferred_retry' AND @retry_of IS NULL)
   OR (@retry_of IS NOT NULL AND @stored_work_kind<>'deferred_retry')
   OR @retry_attempt IS NULL OR (@retry_of IS NULL AND @retry_attempt<>0)
    THROW 51072, 'New attempts must retain their controller-owned retry lineage', 1;
IF ISJSON(@reservation_json)<>1 OR DATALENGTH(@reservation_json)>1048576
   OR EXISTS (SELECT 1 FROM OPENJSON(@reservation_json) WHERE [key] NOT IN ('request','incident_id'))
   OR JSON_QUERY(@reservation_json,'$.request') IS NULL
    THROW 51073, 'Reservation accepts a typed request and canonical incident ID only', 1;
DECLARE @request nvarchar(max)=JSON_QUERY(@reservation_json,'$.request'),
    @incident_id nvarchar(200)=JSON_VALUE(@reservation_json,'$.incident_id');
DECLARE @action varchar(40)=JSON_VALUE(@request,'$.action'),
    @review_id nvarchar(128)=JSON_VALUE(@request,'$.review_id'),
    @review_revision bigint=TRY_CONVERT(bigint,JSON_VALUE(@request,'$.expected_review_revision')),
    @signature nvarchar(256)=JSON_VALUE(@request,'$.incident.signature'),
    @run_id nvarchar(256)=JSON_VALUE(@request,'$.source_execution.run_id'),
    @run_kind varchar(32)=JSON_VALUE(@request,'$.source_execution.run_id_kind'),
    @parameter_hash char(64)=JSON_VALUE(@request,'$.parameter_hash');
IF COALESCE(@action,'') NOT IN ('powerbi_refresh','pipeline_rerun','rebind_dataset_gateway','reenable_refresh_schedule')
   OR COALESCE(JSON_VALUE(@request,'$.idempotency_id'),'')<>@request_id
   OR COALESCE(JSON_VALUE(@request,'$.expected.tenant_id'),'')<>@tenant_id
   OR COALESCE(JSON_VALUE(@request,'$.expected.epoch'),'')<>@epoch
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@request,'$.expected.revision')),-1)<>@expected_revision
   OR COALESCE(JSON_VALUE(@request,'$.work_id'),'')<>@work_id
   OR COALESCE(JSON_VALUE(@request,'$.lease.owner_id'),'')<>@owner_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@request,'$.lease.fence')),-1)<>@fence
   OR COALESCE(JSON_VALUE(@request,'$.lease.resource_key'),'')<>@work_key
   OR NULLIF(@incident_id,'') IS NULL OR NULLIF(@run_id,'') IS NULL
   OR NULLIF(@signature,'') IS NULL
   OR COALESCE(JSON_QUERY(@request,'$.source_execution'),'')<>COALESCE(JSON_QUERY(@stored_work,'$.execution'),'')
   OR COALESCE(JSON_QUERY(@request,'$.incident.target'),'')<>COALESCE(JSON_QUERY(@stored_work,'$.target'),'')
   OR NULLIF(@review_id,'') IS NULL OR @review_revision IS NULL OR @review_revision<1
   OR LEN(COALESCE(@parameter_hash,''))<>64
    THROW 51073, 'Reservation identity, owner, source or canonical signature is invalid', 1;
DECLARE @source_key nvarchar(1024)=@stored_target_key+N':run:'+@run_kind+N':'+@run_id;
DECLARE @signature_json nvarchar(max)={names.object('json_identity_string')}(@signature);
DECLARE @incident_key nvarchar(1024)=@stored_target_key+N':incident:'+LOWER(CONVERT(char(64),{key_hash('@signature_json')},2));
DECLARE @target nvarchar(max),@review nvarchar(max),@validation nvarchar(max),@source nvarchar(max),@head nvarchar(max);
SELECT @target=payload FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='target' AND full_key=@stored_target_key;
SELECT @review=payload FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='review' AND full_key=@review_id;
SELECT @validation=payload FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='controller_validation' AND full_key=@work_id;
SELECT @source=payload FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='source' AND full_key=@source_key;
SELECT @head=payload FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='source_head' AND full_key=@stored_target_key;
DECLARE @latest_review_intent nvarchar(max);
SELECT @latest_review_intent=payload FROM {table}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='review_request' AND full_key=@review_id;
IF @latest_review_intent IS NULL
   OR COALESCE(JSON_VALUE(@latest_review_intent,'$.requested_state'),'')<>'verified'
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@latest_review_intent,'$.revision')),-1)<>@review_revision
   OR COALESCE(JSON_VALUE(@latest_review_intent,'$.parameter_hash'),'')<>@parameter_hash
    THROW 51072, 'Latest protected review intent is revoked, pending or different', 1;
IF @target IS NULL OR @review IS NULL OR @validation IS NULL OR @source IS NULL
    THROW 51072, 'Protected current controller admission/validation/source is missing', 1;
{reservation_frontier_guard(names)}
IF COALESCE(JSON_VALUE(@target,'$.state'),'')<>'current'
   OR COALESCE(JSON_VALUE(@target,'$.admission_basis'),'')<>'reviewed'
   OR COALESCE(JSON_VALUE(@target,'$.observation.enabled'),'')<>'true'
   OR COALESCE(JSON_VALUE(@target,'$.action.enabled'),'')<>'true'
   OR COALESCE(JSON_VALUE(@target,'$.action.action'),'')<>@action
   OR COALESCE(JSON_VALUE(@target,'$.action.review_id'),'')<>@review_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@target,'$.action.review_revision')),-1)<>@review_revision
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@target,'$.policy_revision')),-1)<>@current_revision
    THROW 51072, 'Current target projection does not authorize this action', 1;
IF COALESCE(JSON_VALUE(@review,'$.state'),'')<>'verified'
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@review,'$.revision')),-1)<>@review_revision
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@review,'$.policy_revision')),-1)<>@current_revision
   OR COALESCE(JSON_VALUE(@review,'$.action'),'')<>@action
   OR COALESCE(JSON_VALUE(@review,'$.parameter_hash'),'')<>@parameter_hash
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@review,'$.expires_at')) IS NULL
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@review,'$.expires_at'))<=TODATETIMEOFFSET(@now,'+00:00')
   OR COALESCE(JSON_QUERY(@review,'$.target'),'')<>COALESCE(JSON_QUERY(@stored_work,'$.target'),'')
    THROW 51072, 'Current review is expired, revoked or fingerprint-mismatched', 1;
{action_arguments_guard(names)}
IF COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@validation,'$.policy_revision')),-1)<>@current_revision
   OR COALESCE(JSON_VALUE(@validation,'$.verified'),'')<>'true'
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@validation,'$.work_fence')),-1)<>@fence
   OR COALESCE(JSON_VALUE(@validation,'$.source_key'),'')<>@source_key
   OR COALESCE(JSON_VALUE(@validation,'$.parameter_hash'),'')<>@parameter_hash
   OR COALESCE(JSON_VALUE(@validation,'$.review_id'),'')<>@review_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@validation,'$.review_revision')),-1)<>@review_revision
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@validation,'$.expires_at')) IS NULL
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@validation,'$.expires_at'))<=TODATETIMEOFFSET(@now,'+00:00')
    THROW 51072, 'Controller validation is stale or belongs to another request/fence', 1;
IF @action IN ('powerbi_refresh','pipeline_rerun') AND (
    COALESCE(JSON_VALUE(@review,'$.exact_correlation_verified'),'')<>'true'
    OR COALESCE(JSON_VALUE(@validation,'$.exact_action_correlation'),'')<>'true')
    THROW 51072, 'Action correlation must have protected controller verification', 1;
IF COALESCE(JSON_VALUE(@source,'$.authority'),'')<>'rest'
   OR COALESCE(JSON_VALUE(@source,'$.status'),'')<>'failed'
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.started_at')) IS NULL
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.ended_at')) IS NULL
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.started_at'))<@cutoff
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.observed_at'))<DATEADD(second,-300,@now)
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.observed_at'))>@now
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.observed_at')) IS NULL
    THROW 51072, 'Exact failed source must be re-read after the approval wait', 1;
IF @action<>'reenable_refresh_schedule'
   AND TRY_CONVERT(datetime2(6),JSON_VALUE(@head,'$.started_at'))>TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.started_at'))
    THROW 51072, 'A newer source execution must be reconciled first', 1;
IF @action='pipeline_rerun' AND (
    COALESCE(JSON_VALUE(@source,'$.invocation'),'')<>'scheduled'
    OR COALESCE(JSON_VALUE(@source,'$.job_type'),'')<>'Pipeline'
    OR COALESCE(JSON_VALUE(@review,'$.replay_safe'),'')<>'true'
    OR COALESCE(JSON_VALUE(@review,'$.definition_hash'),'')<>COALESCE(JSON_VALUE(@request,'$.definition_hash'),'')
    OR COALESCE(JSON_VALUE(@validation,'$.definition_hash'),'')<>COALESCE(JSON_VALUE(@request,'$.definition_hash'),'')
    OR NULLIF(JSON_VALUE(@request,'$.definition_hash'),'') IS NULL)
    THROW 51072, 'Pipeline replay is not the reviewed failed scheduled definition', 1;
IF @action='powerbi_refresh' AND @run_kind<>'powerbi_request'
    THROW 51072, 'Power BI action needs exact request identity, not history alias', 1;
IF @action='reenable_refresh_schedule' AND (
    COALESCE(JSON_VALUE(@head,'$.status'),'')<>'succeeded'
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@head,'$.started_at')) IS NULL
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@head,'$.started_at'))
        <=TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.started_at')))
    THROW 51072, 'Schedule restoration requires a newer successful source', 1;
IF @action IN ('rebind_dataset_gateway','reenable_refresh_schedule') AND (
    COALESCE(JSON_VALUE(@request,'$.configuration_hash'),'')<>@parameter_hash
    OR COALESCE(JSON_VALUE(@review,'$.configuration_hash'),'')<>@parameter_hash
    OR COALESCE(JSON_VALUE(@validation,'$.configuration_hash'),'')<>@parameter_hash)
    THROW 51072, 'Non-job action configuration hash differs', 1;
IF @retry_of IS NOT NULL
BEGIN
    {retry_admission_sql(names, '@request', '@stored_target_key')}
    IF NOT ({retry_admission_predicate()})
        THROW 51072, 'Retry lost current scope, review or action capability', 1;
    SELECT @retry_parent=payload,@retry_parent_revision=revision FROM {table}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action' AND full_key=@retry_of;
    SELECT @retry_parent_work=payload FROM {table}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
      AND full_key=JSON_VALUE(@retry_parent,'$.request.work_id');
    IF NOT ({successor_predicate()})
        THROW 51072, 'Retry is not the unused bounded successor of the exact finalized rejected action', 1;
    IF NOT EXISTS (SELECT 1 FROM {names.table('monitoring_receipts')}
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND operation='controller.finalize'
          AND request_id=JSON_VALUE(@retry_parent_work,'$.finalization_id')
          AND JSON_VALUE(payload,'$.result.work_id')=JSON_VALUE(@retry_parent,'$.request.work_id')
          AND JSON_VALUE(payload,'$.result.state')='completed')
        THROW 51072, 'The rejected predecessor has no committed original finalization', 1;
    SET @budget_debit=0;
END;
DECLARE @owner nvarchar(max),@budget nvarchar(max),@budget_revision bigint,
    @next_fence bigint=1,@used int=0,
    @updated_at_text nvarchar(40)=CONVERT(nvarchar(40),@now,127)+N'Z';
SELECT @owner=payload FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='action_owner' AND full_key=@stored_target_key;
IF COALESCE(JSON_VALUE(@owner,'$.active'),'false')='true'
    THROW 51072, 'Another active or uncertain action owns the target', 1;
IF @owner IS NOT NULL AND (
    COALESCE(JSON_VALUE(@owner,'$.active'),'') NOT IN ('true','false')
    OR TRY_CONVERT(bigint,JSON_VALUE(@owner,'$.fence')) IS NULL
    OR TRY_CONVERT(bigint,JSON_VALUE(@owner,'$.fence'))<1)
    THROW 51073, 'Action owner state is malformed, not unowned', 1;
SET @next_fence=COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@owner,'$.fence')),0)+1;
SELECT @budget=payload,@budget_revision=revision FROM {table}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='incident_state' AND full_key=@incident_key;
SET @used=COALESCE(TRY_CONVERT(int,JSON_VALUE(@budget,'$.action_count')),0);
IF @budget IS NOT NULL AND (
    TRY_CONVERT(int,JSON_VALUE(@budget,'$.action_count')) IS NULL
    OR TRY_CONVERT(int,JSON_VALUE(@budget,'$.action_count'))<0
    OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@budget,'$.revision')),-1)<>@budget_revision)
    THROW 51073, 'Incident budget is malformed, not unused', 1;
IF COALESCE(@budget_revision,0)<>COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@request,'$.expected_incident_revision')),-1)
   OR (@retry_of IS NULL AND @used>=1)
   OR (@retry_of IS NOT NULL AND (@budget IS NULL OR @used<>1))
    THROW 51072, 'Incident revision or one-action budget forbids a new reservation', 1;
IF @budget IS NOT NULL AND COALESCE(JSON_VALUE(@budget,'$.incident_id'),'')<>@incident_id
    THROW 51072, 'Incident identity is immutable', 1;
IF @action<>'reenable_refresh_schedule'
   AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@budget,'$.latest_started_at'))
       >TRY_CONVERT(datetimeoffset,JSON_VALUE(@source,'$.started_at'))
    THROW 51072, 'Older incident metadata cannot authorize another source action', 1;
DECLARE @approval_id nvarchar(200)=JSON_VALUE(@request,'$.approval.approval_id'),
    @approval_fingerprint nvarchar(256)=JSON_VALUE(@request,'$.approval.fingerprint'),
    @approval nvarchar(max),@binding nvarchar(max);
IF @action IN ('pipeline_rerun','rebind_dataset_gateway','reenable_refresh_schedule') AND @approval_id IS NULL
    THROW 51072, 'This action requires explicit human approval', 1;
IF @approval_id IS NOT NULL
BEGIN
    SELECT @binding=payload FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
      AND record_kind='approval_binding' AND full_key=@approval_id;
    SELECT @approval=payload FROM {approvals} WITH (UPDLOCK,HOLDLOCK) WHERE request_id=@approval_id;
    SET @now=SYSUTCDATETIME();
    IF @binding IS NULL OR @approval IS NULL
       OR COALESCE(JSON_VALUE(@binding,'$.reference.fingerprint'),'')<>@approval_fingerprint
       OR COALESCE(JSON_VALUE(@binding,'$.work_id'),'')<>@work_id
       OR COALESCE(JSON_VALUE(@binding,'$.parameter_hash'),'')<>@parameter_hash
       OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding,'$.expected.revision')),-1)<>@current_revision
       OR COALESCE(JSON_VALUE(@binding,'$.expected.tenant_id'),'')<>@tenant_id
       OR COALESCE(JSON_VALUE(@binding,'$.expected.epoch'),'')<>@epoch
       OR COALESCE(JSON_VALUE(@binding,'$.review_id'),'')<>@review_id
       OR COALESCE(JSON_VALUE(@binding,'$.action'),'')<>@action
       OR COALESCE(JSON_QUERY(@binding,'$.source_execution'),'') COLLATE Latin1_General_100_BIN2
          <>COALESCE(JSON_QUERY(@request,'$.source_execution'),'') COLLATE Latin1_General_100_BIN2
       OR COALESCE(JSON_QUERY(@binding,'$.incident'),'') COLLATE Latin1_General_100_BIN2
          <>COALESCE(JSON_QUERY(@request,'$.incident'),'') COLLATE Latin1_General_100_BIN2
       OR COALESCE(JSON_VALUE(@binding,'$.definition_hash'),'')<>COALESCE(JSON_VALUE(@request,'$.definition_hash'),'')
       OR COALESCE(JSON_VALUE(@binding,'$.configuration_hash'),'')<>COALESCE(JSON_VALUE(@request,'$.configuration_hash'),'')
       OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding,'$.review_revision')),-1)<>@review_revision
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@binding,'$.expires_at')) IS NULL
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@binding,'$.expires_at'))<=TODATETIMEOFFSET(@now,'+00:00')
       OR COALESCE(JSON_VALUE(@approval,'$.fingerprint'),'')<>@approval_fingerprint
       OR COALESCE(JSON_VALUE(@approval,'$.delivery_channel'),'') NOT IN ('web','teams')
       OR COALESCE(JSON_VALUE(@approval,'$.decision'),'')<>'approve'
       OR COALESCE(JSON_VALUE(@approval,'$.consumed_at'),'')<>''
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.expires_at')) IS NULL
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.expires_at'))<=TODATETIMEOFFSET(@now,'+00:00')
        THROW 51072, 'Approval is not the matched explicit unexpired unused request', 1;
    {approval_arguments_guard(names)}
END;
IF TRY_CONVERT(datetimeoffset,JSON_VALUE(@review,'$.expires_at'))<=TODATETIMEOFFSET(@now,'+00:00')
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@validation,'$.expires_at'))<=TODATETIMEOFFSET(@now,'+00:00')
   OR NOT EXISTS (SELECT 1 FROM {names.table('monitoring_leases')}
       WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@work_key
         AND owner_id=@owner_id AND fence=@fence AND expires_at>@now)
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source,'$.observed_at'))<DATEADD(second,-300,@now)
    THROW 51074, 'Approval wait outlived the current work or technical validation', 1;
IF EXISTS (SELECT 1 FROM {table} WHERE tenant_id=@tenant_id AND epoch=@epoch
    AND record_kind='action' AND full_key=@reservation_id)
    THROW 51072, 'Reservation identity already exists', 1;
DECLARE @action_json nvarchar(max)=(
    SELECT @reservation_id AS reservation_id,JSON_QUERY(@request) AS request,1 AS revision,
        @next_fence AS fence,'reserved' AS state,CONVERT(nvarchar(40),@now,127)+N'Z' AS reserved_at,
        CONVERT(nvarchar(40),@now,127)+N'Z' AS updated_at,@retry_attempt AS retry_attempt,@retry_of AS retry_of,
        'Durable pre-submission fence' AS detail FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{record_insert(names, 'action', '@reservation_id', '@action_json', status="N'reserved'", parent_key='@work_key', target_key='@stored_target_key')}
DECLARE @owner_json nvarchar(max)=(SELECT @reservation_id AS reservation_id,@next_fence AS fence,
    CAST(1 AS bit) AS active FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
IF @owner IS NULL
BEGIN
    {record_insert(names, 'action_owner', '@stored_target_key', '@owner_json', target_key='@stored_target_key')}
END
ELSE UPDATE {table} SET revision=revision+1,payload=@owner_json
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action_owner' AND full_key=@stored_target_key;
IF @@ROWCOUNT<>1 THROW 51072, 'Action owner write was not confirmed', 1;
DECLARE @budget_json nvarchar(max)=CASE WHEN @budget IS NULL THEN (
    SELECT JSON_QUERY(JSON_QUERY(@request,'$.incident')) AS [identity],@incident_id AS incident_id,
        1 AS revision,1 AS action_count,CONVERT(nvarchar(40),@now,127)+N'Z' AS updated_at
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) ELSE
    {budget_debit_expression()} END;
IF @budget IS NULL
BEGIN
    {record_insert(names, 'incident_state', '@incident_key', '@budget_json', target_key='@stored_target_key')}
END
ELSE {save_budget_sql(names)}
IF @@ROWCOUNT<>1 THROW 51072, 'Incident budget write was not confirmed', 1;
IF @retry_of IS NOT NULL
BEGIN
    {link_reservation_sql(names)}
    IF @@ROWCOUNT<>1 THROW 51072, 'Rejected predecessor successor link was already consumed or changed', 1;
END;
IF @approval_id IS NOT NULL
BEGIN
    UPDATE {approvals} SET payload=JSON_MODIFY(payload,'$.consumed_at',CONVERT(nvarchar(40),@now,127)+N'Z')
    WHERE request_id=@approval_id AND decision='approve' AND COALESCE(JSON_VALUE(payload,'$.consumed_at'),'')=''
      AND JSON_VALUE(payload,'$.fingerprint')=@approval_fingerprint;
    IF @@ROWCOUNT<>1 THROW 51072, 'Approval was consumed by another reservation', 1;
END;
SET @stored_work=JSON_MODIFY(JSON_MODIFY(@stored_work,'$.action_reservation_id',@reservation_id),'$.revision',@work_revision+1);
UPDATE {table} SET revision=revision+1,payload=@stored_work WHERE tenant_id=@tenant_id AND epoch=@epoch
    AND record_kind='work' AND full_key=@work_id AND revision=@work_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Work reservation attachment lost its revision', 1;
SET @affected=1;
SET @result=(SELECT @reservation_id AS reservation_id,JSON_QUERY(@action_json) AS reservation
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True)


def _transition(names: SqlNames, contract: RpcContract) -> KernelObject:
    records = names.table("monitoring_records")
    body = f"""{current_work(names, ACTION_WORK_KINDS, target_lease=True)}
IF @stored_work_revision<>@work_revision OR @stored_work_status NOT IN ('leased','finalizing')
    THROW 51074, 'Action transition lost current controller work ownership', 1;
DECLARE @action nvarchar(max),@action_revision bigint,@action_state varchar(40);
SELECT @action=payload,@action_revision=revision,@action_state=status
FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action' AND full_key=@reservation_id
  AND target_key=@stored_target_key;
IF @action IS NULL OR @action_revision<>@expected_action_revision
   OR COALESCE(JSON_VALUE(@stored_work,'$.action_reservation_id'),'')<>@reservation_id
    THROW 51072, 'Action lineage or revision differs from current work', 1;
IF ISJSON(@transition_json)<>1 OR DATALENGTH(@transition_json)>1048576
   OR EXISTS (SELECT 1 FROM OPENJSON(@transition_json) WHERE [key] NOT IN
       ('submitted_execution','submitted_at','next_verification_at','configuration','rejection','detail'))
    THROW 51073, 'Action transition accepts only its typed mutable result fields', 1;
IF @transition NOT IN ('submitted','uncertain','rejected','verified_succeeded','verified_failed')
    THROW 51073, 'Unsupported action transition', 1;
IF @action_state IN ('verified_succeeded','verified_failed','rejected')
    THROW 51072, 'A terminal action cannot be reopened', 1;
IF @transition='rejected' AND (
    @action_state<>'reserved' OR JSON_QUERY(@transition_json,'$.rejection') IS NULL
    OR @stored_work_kind NOT IN ('triage','deferred_retry')
    OR COALESCE(JSON_VALUE(@action,'$.request.work_id'),'')<>@work_id
    OR COALESCE(JSON_VALUE(@action,'$.request.lease.owner_id'),'')<>@owner_id
    OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@action,'$.request.lease.fence')),-1)<>@fence
    OR JSON_QUERY(@transition_json,'$.submitted_execution') IS NOT NULL
    OR JSON_VALUE(@transition_json,'$.submitted_at') IS NOT NULL
    OR JSON_QUERY(@transition_json,'$.configuration') IS NOT NULL
    OR JSON_VALUE(@transition_json,'$.next_verification_at') IS NOT NULL)
    THROW 51072, 'Only a confirmed no-effect rejection may close an unsubmitted reservation', 1;
IF @transition IN ('submitted','uncertain') AND
    TRY_CONVERT(datetime2(6),JSON_VALUE(@transition_json,'$.next_verification_at')) IS NULL
    THROW 51073, 'Submitted/uncertain effects require verification scheduling', 1;
IF JSON_QUERY(@action,'$.submitted_execution') IS NOT NULL
   AND EXISTS (SELECT 1 FROM OPENJSON(@transition_json) WHERE [key]='submitted_execution')
   AND (JSON_QUERY(@transition_json,'$.submitted_execution') IS NULL
        OR JSON_QUERY(@action,'$.submitted_execution') COLLATE Latin1_General_100_BIN2
           <>JSON_QUERY(@transition_json,'$.submitted_execution') COLLATE Latin1_General_100_BIN2)
    THROW 51072, 'Submitted execution identity cannot change', 1;
IF JSON_VALUE(@action,'$.submitted_at') IS NOT NULL
   AND EXISTS (SELECT 1 FROM OPENJSON(@transition_json) WHERE [key]='submitted_at')
   AND (TRY_CONVERT(datetimeoffset,JSON_VALUE(@transition_json,'$.submitted_at')) IS NULL
        OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@action,'$.submitted_at'))
           <>TRY_CONVERT(datetimeoffset,JSON_VALUE(@transition_json,'$.submitted_at')))
    THROW 51072, 'Original submission time cannot change or be cleared', 1;
IF JSON_QUERY(@transition_json,'$.submitted_execution') IS NOT NULL AND (
    COALESCE(JSON_QUERY(@transition_json,'$.submitted_execution.target'),'') COLLATE Latin1_General_100_BIN2
        <>COALESCE(JSON_QUERY(@action,'$.request.source_execution.target'),'') COLLATE Latin1_General_100_BIN2
    OR JSON_QUERY(@transition_json,'$.submitted_execution')=JSON_QUERY(@action,'$.request.source_execution')
    OR (JSON_VALUE(@action,'$.request.action')='powerbi_refresh'
        AND COALESCE(JSON_VALUE(@transition_json,'$.submitted_execution.run_id_kind'),'')<>'powerbi_request')
    OR (JSON_VALUE(@action,'$.request.action')='pipeline_rerun'
        AND COALESCE(JSON_VALUE(@transition_json,'$.submitted_execution.run_id_kind'),'')<>'fabric_job')
    OR NOT ({canonical_guid("JSON_VALUE(@transition_json,'$.submitted_execution.run_id')")}))
    THROW 51072, 'A submission must be a distinct exact execution of the same target', 1;
DECLARE @field nvarchar(128),@value nvarchar(max),@type int;
DECLARE changes CURSOR LOCAL FAST_FORWARD FOR SELECT [key],value,type FROM OPENJSON(@transition_json);
OPEN changes; FETCH NEXT FROM changes INTO @field,@value,@type;
WHILE @@FETCH_STATUS=0
BEGIN
    SET @action=CASE WHEN @type IN (4,5) THEN JSON_MODIFY(@action,N'$.'+@field,JSON_QUERY(@value))
                    ELSE JSON_MODIFY(@action,N'$.'+@field,@value) END;
    FETCH NEXT FROM changes INTO @field,@value,@type;
END;
CLOSE changes; DEALLOCATE changes;
IF (@transition<>'rejected' AND JSON_QUERY(@action,'$.rejection') IS NOT NULL)
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.submitted_at'))
      <TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.reserved_at'))
   OR (JSON_VALUE(@action,'$.request.action') IN ('rebind_dataset_gateway','reenable_refresh_schedule')
       AND JSON_QUERY(@action,'$.submitted_execution') IS NOT NULL)
    THROW 51073, 'Action state carries incompatible rejection/submission evidence', 1;
IF JSON_QUERY(@action,'$.configuration') IS NOT NULL AND (
    COALESCE(JSON_VALUE(@action,'$.configuration.action'),'')<>JSON_VALUE(@action,'$.request.action')
    OR COALESCE(JSON_VALUE(@action,'$.configuration.expected_hash'),'')<>JSON_VALUE(@action,'$.request.parameter_hash')
    OR COALESCE(JSON_QUERY(@action,'$.configuration.target'),'') COLLATE Latin1_General_100_BIN2
       <>COALESCE(JSON_QUERY(@action,'$.request.source_execution.target'),'') COLLATE Latin1_General_100_BIN2)
    THROW 51073, 'Configuration evidence differs from the original reserved target/action/hash', 1;
IF @transition='rejected' AND (
    COALESCE(JSON_VALUE(@action,'$.rejection.reason'),'') NOT IN ('throttled','definitive_client_error')
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.rejection.attempted_at')) IS NULL
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.rejection.rejected_at')) IS NULL
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.rejection.attempted_at'))
       <TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.reserved_at'))
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.rejection.rejected_at'))
       <TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.rejection.attempted_at'))
    OR TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.rejection.rejected_at'))>@now)
    THROW 51073, 'A no-effect rejection must retain the bounded actual attempt/response chronology', 1;
IF @transition='rejected'
    SET @action=JSON_MODIFY(@action,'$.next_verification_at',NULL);
IF @transition IN ('submitted','verified_succeeded','verified_failed') AND (
    TRY_CONVERT(datetime2(6),JSON_VALUE(@action,'$.submitted_at')) IS NULL
    OR (JSON_VALUE(@action,'$.request.action') IN ('powerbi_refresh','pipeline_rerun')
        AND JSON_QUERY(@action,'$.submitted_execution') IS NULL))
    THROW 51073, 'Submitted job outcomes require exact submission identity/time', 1;
IF @transition IN ('verified_succeeded','verified_failed')
BEGIN
    DECLARE @outcome_validation nvarchar(max);
    SELECT @outcome_validation=payload FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='controller_validation'
      AND full_key=@reservation_id;
    IF @outcome_validation IS NULL
       OR COALESCE(JSON_VALUE(@outcome_validation,'$.reservation_id'),'')<>@reservation_id
       OR COALESCE(JSON_VALUE(@outcome_validation,'$.outcome'),'')<>@transition
       OR COALESCE(JSON_VALUE(@outcome_validation,'$.verified'),'')<>'true'
       OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@outcome_validation,'$.work_fence')),-1)<>@fence
       OR TRY_CONVERT(datetime2(6),JSON_VALUE(@outcome_validation,'$.expires_at')) IS NULL
       OR TRY_CONVERT(datetime2(6),JSON_VALUE(@outcome_validation,'$.expires_at'))<=@now
       OR COALESCE(JSON_QUERY(@outcome_validation,'$.submitted_execution'),'')
          <>COALESCE(JSON_QUERY(@action,'$.submitted_execution'),'')
        THROW 51072, 'Technical outcome lacks its protected exact controller validation', 1;
    IF JSON_VALUE(@action,'$.request.action') IN ('rebind_dataset_gateway','reenable_refresh_schedule')
       AND (JSON_QUERY(@action,'$.configuration') IS NULL
            OR COALESCE(JSON_QUERY(@outcome_validation,'$.configuration'),'')
               <>COALESCE(JSON_QUERY(@action,'$.configuration'),''))
        THROW 51072, 'Configuration result differs from protected controller verification', 1;
END;
SET @action=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@action,'$.state',@transition),
    '$.revision',@action_revision+1),'$.updated_at',CONVERT(nvarchar(40),@now,127)+N'Z');
{record_submitted_correlation_sql(names)}
{enqueue_after_rejection_sql(names)}
UPDATE {records} SET revision=revision+1,status=@transition,payload=@action
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action'
  AND full_key=@reservation_id AND revision=@action_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Action transition compare-and-set failed', 1;
IF @transition IN ('rejected','verified_succeeded','verified_failed')
BEGIN
    UPDATE {records} SET revision=revision+1,payload=JSON_MODIFY(payload,'$.active',CAST(0 AS bit))
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action_owner'
      AND full_key=@stored_target_key AND JSON_VALUE(payload,'$.reservation_id')=@reservation_id
      AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.fence'))=TRY_CONVERT(bigint,JSON_VALUE(@action,'$.fence'))
      AND JSON_VALUE(payload,'$.active')='true';
    IF @@ROWCOUNT<>1 THROW 51074, 'Terminal action lost its original active owner fence', 1;
END;
SET @affected=1;
SET @result=(SELECT @reservation_id AS reservation_id,JSON_QUERY(@action) AS reservation,
    JSON_QUERY(@retry_work) AS retry_work FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    # Revocation blocks new reservations, not verification of an already reserved effect.
    return procedure(
        names, contract, body, replay=True, permit_maintenance=True, check_revision=False,
    )


def action_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    return {
        "controller.open_approval": _approval(names, contracts["controller.open_approval"], "open"),
        "web.decide_approval": _approval(names, contracts["web.decide_approval"], "decide"),
        "controller.consume_approval": _approval(names, contracts["controller.consume_approval"], "consume"),
        "controller.reserve_action": _reserve(names, contracts["controller.reserve_action"]),
        "controller.transition_action": _transition(names, contracts["controller.transition_action"]),
        "controller.finalize": _finalize(names, contracts["controller.finalize"]),
    }


def _finalize(names: SqlNames, contract: RpcContract) -> KernelObject:
    records, incidents, processed = (
        names.table("monitoring_records"), names.table("incidents"), names.table("processed"),
    )
    body = f"""{current_work(names, ACTION_WORK_KINDS, target_lease=True)}
IF @stored_work_revision<>@work_revision OR @stored_work_status NOT IN ('leased','finalizing')
    THROW 51074, 'Finalization lost current work ownership', 1;
IF @request_id<>@finalization_id OR ISJSON(@finalization_json)<>1
   OR EXISTS (SELECT 1 FROM OPENJSON(@finalization_json) WHERE [key] NOT IN ('plan_key','plan_hash'))
   OR COALESCE(JSON_VALUE(@finalization_json,'$.plan_key'),'')<>@finalization_id
    THROW 51073, 'Finalization accepts only its immutable protected plan reference', 1;
DECLARE @plan nvarchar(max),@plan_hash char(64);
SELECT @plan=payload,@plan_hash={payload_hash('payload')} FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='finalization_plan'
  AND full_key=@finalization_id AND key_hash={key_hash('@finalization_id')};
IF @plan IS NULL OR COALESCE(JSON_VALUE(@finalization_json,'$.plan_hash'),'')<>@plan_hash
   OR COALESCE(JSON_VALUE(@plan,'$.work_id'),'')<>@work_id
   OR COALESCE(JSON_VALUE(@plan,'$.lease_owner_id'),'')<>@owner_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.lease_fence')),-1)<>@fence
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.expected_work_revision')),-1)<>@work_revision
    THROW 51072, 'Finalization plan is missing, changed or belongs to another work fence', 1;
DECLARE @incident_id nvarchar(200)=JSON_VALUE(@plan,'$.incident_id'),
    @incident_key nvarchar(1024)=JSON_VALUE(@plan,'$.incident_key'),
    @source_key nvarchar(1024)=JSON_VALUE(@plan,'$.source_key'),
    @signature nvarchar(256)=JSON_VALUE(@plan,'$.signature'),
    @merged nvarchar(max)=JSON_QUERY(@plan,'$.merged_incident'),
    @prior nvarchar(max),@prior_hash char(64),@budget nvarchar(max),@budget_revision bigint;
IF NULLIF(@incident_id,'') IS NULL OR NULLIF(@signature,'') IS NULL OR @merged IS NULL
   OR @stored_target_key IS NULL OR @incident_key IS NULL OR @source_key IS NULL
   OR COALESCE(JSON_VALUE(@merged,'$.id'),'')<>@incident_id
   OR COALESCE(JSON_VALUE(@merged,'$.signature'),'')<>@signature
   OR COALESCE(JSON_QUERY(@plan,'$.source_execution'),'')<>COALESCE(JSON_QUERY(@stored_work,'$.execution'),'')
   OR @source_key<>@stored_target_key+N':run:'+COALESCE(JSON_VALUE(@plan,'$.source_execution.run_id_kind'),'')
       +N':'+COALESCE(JSON_VALUE(@plan,'$.source_execution.run_id'),'')
    THROW 51073, 'Finalization plan changes incident/source identity', 1;
DECLARE @requested_resolution bit=CASE WHEN JSON_VALUE(@merged,'$.status')='resolved'
    OR JSON_VALUE(@merged,'$.outcome')='resolved' THEN 1 ELSE 0 END;
DECLARE @signature_json nvarchar(max)={names.object('json_identity_string')}(@signature);
IF @incident_key<>@stored_target_key+N':incident:'+LOWER(CONVERT(char(64),{key_hash('@signature_json')},2))
    THROW 51073, 'Finalization incident key differs from its canonical signature', 1;
DECLARE @source_snapshot nvarchar(max);
SELECT @source_snapshot=payload FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='source' AND full_key=@source_key;
IF @source_snapshot IS NULL
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@plan,'$.source_started_at')) IS NULL
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@source_snapshot,'$.started_at')) IS NULL
   OR TRY_CONVERT(datetime2(6),JSON_VALUE(@plan,'$.source_started_at'))
      <>TRY_CONVERT(datetime2(6),JSON_VALUE(@source_snapshot,'$.started_at'))
    THROW 51072, 'Finalization needs the exact protected source chronology', 1;
SELECT @prior=payload,@prior_hash={payload_hash('payload')} FROM {incidents} WITH (UPDLOCK,HOLDLOCK)
WHERE incident_id=@incident_id;
DECLARE @prior_payload_digest varbinary(32)=CONVERT(varbinary(32),@prior_hash,2);
IF (@prior IS NULL AND JSON_VALUE(@plan,'$.prior_incident_hash') IS NOT NULL)
   OR (@prior IS NOT NULL AND COALESCE(JSON_VALUE(@plan,'$.prior_incident_hash'),'')<>@prior_hash)
    THROW 51072, 'Original SQL NVARCHAR incident payload changed before finalization', 1;
SELECT @budget=payload,@budget_revision=revision FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='incident_state' AND full_key=@incident_key;
IF @budget IS NOT NULL AND COALESCE(JSON_VALUE(@budget,'$.incident_id'),'')<>@incident_id
    THROW 51072, 'Incident budget identity is immutable', 1;
{prepare_incident_merge_sql(names)}
DECLARE @action_id nvarchar(128)=JSON_VALUE(@stored_work,'$.action_reservation_id'),@action_state varchar(40),
    @pending bit=0;
IF @action_id IS NOT NULL
BEGIN
    SELECT @action_state=status FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
      AND record_kind='action' AND full_key=@action_id AND target_key=@stored_target_key;
    IF @action_state IS NULL THROW 51072, 'Finalization lost its reserved effect', 1;
    SET @pending=CASE WHEN @action_state IN ('reserved','submitted','uncertain') THEN 1 ELSE 0 END;
    IF @action_state<>'verified_succeeded' AND @requested_resolution=1
        THROW 51072, 'An uncertain effect is not a verified incident resolution', 1;
END;
IF @prior IS NULL
    INSERT INTO {incidents} (incident_id,signature,status,updated_at,payload)
    VALUES (@incident_id,@signature,COALESCE(JSON_VALUE(@merged,'$.status'),'needs_review'),
        CONVERT(nvarchar(40),@now,127)+N'Z',@merged);
ELSE
    {save_incident_sql(names)}
IF @@ROWCOUNT<>1 THROW 51072, 'Incident finalization lost its original payload compare-and-set', 1;
{occurrence_marker_sql(names)}
DECLARE @next_budget nvarchar(max)=CASE WHEN @budget IS NULL THEN (
    SELECT JSON_QUERY(@plan,'$.incident_identity') AS [identity],@incident_id AS incident_id,
        0 AS action_count,1 AS revision,JSON_QUERY(@plan,'$.source_execution') AS latest_execution,
        JSON_VALUE(@plan,'$.source_started_at') AS latest_started_at,
        CONVERT(nvarchar(40),@now,127)+N'Z' AS updated_at
    FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER)
    WHEN @historical=1 THEN {retain_budget_head_expression()} ELSE
    JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@budget,'$.revision',@budget_revision+1),
        '$.latest_execution',JSON_QUERY(JSON_QUERY(@plan,'$.source_execution'))),
        '$.latest_started_at',JSON_VALUE(@plan,'$.source_started_at')),
        '$.updated_at',CONVERT(nvarchar(40),@now,127)+N'Z') END;
IF @budget IS NULL
BEGIN
    {record_insert(names, 'incident_state', '@incident_key', '@next_budget', target_key='@stored_target_key')}
END
ELSE UPDATE {records} SET revision=revision+1,payload=@next_budget
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='incident_state'
      AND full_key=@incident_key AND revision=@budget_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Incident budget revision changed; no reset/refund is permitted', 1;
IF @pending=0
BEGIN
    DECLARE @disposition varchar(40)=CASE WHEN @historical=1 THEN 'historical'
        ELSE JSON_VALUE(@plan,'$.source_disposition') END;
    IF COALESCE(@disposition,'') NOT IN ({literals(SOURCE_DISPOSITIONS)})
        THROW 51073, 'Unsupported terminal source disposition', 1;
    DECLARE @source_result nvarchar(max)=(SELECT JSON_QUERY(@plan,'$.source_execution') AS execution,
        @disposition AS disposition,@work_id AS work_id,@finalization_id AS finalization_id,
        CONVERT(nvarchar(40),@now,127)+N'Z' AS recorded_at
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
    IF NOT EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
        AND record_kind='source_disposition' AND full_key=@source_key)
    BEGIN
        {record_insert(names, 'source_disposition', '@source_key', '@source_result', status='@disposition', target_key='@stored_target_key')}
    END;
    INSERT INTO {processed} (fingerprint,message_id,received_at)
    SELECT LOWER(CONVERT(char(64),{key_hash('@source_key')},2)),@source_key,CONVERT(nvarchar(40),@now,127)+N'Z'
    WHERE NOT EXISTS (SELECT 1 FROM {processed} WITH (UPDLOCK,HOLDLOCK)
        WHERE fingerprint=LOWER(CONVERT(char(64),{key_hash('@source_key')},2)));
    SET @stored_work=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@stored_work,
        '$.state','completed'),'$.lease',NULL),'$.finalization_id',@finalization_id),
        '$.completed_at',CONVERT(nvarchar(40),@now,127)+N'Z');
    UPDATE {names.table('monitoring_leases')} SET expires_at=CASE WHEN @now<=acquired_at
        THEN DATEADD(microsecond,1,acquired_at) ELSE @now END
    WHERE tenant_id=@tenant_id AND epoch=@epoch
      AND ((full_key=@work_key AND owner_id=@owner_id AND fence=@fence)
           OR (full_key=N'controller:'+@stored_target_key AND owner_id=@work_id));
END
ELSE
    SET @stored_work=JSON_MODIFY(@stored_work,'$.state','finalizing');
SET @stored_work=JSON_MODIFY(@stored_work,'$.revision',@work_revision+1);
UPDATE {records} SET revision=revision+1,status=JSON_VALUE(@stored_work,'$.state'),payload=@stored_work
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
  AND full_key=@work_id AND revision=@work_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Finalization work update lost its revision', 1;
SET @affected=1;
SET @result=(SELECT @work_id AS work_id,@finalization_id AS finalization_id,@incident_id AS incident_id,
    CASE WHEN @pending=1 THEN 'persisted_waiting_verification' ELSE 'completed' END AS state,
    JSON_QUERY(@merged) AS incident,
    CASE WHEN @historical=1 THEN 'historical' ELSE JSON_VALUE(@plan,'$.source_disposition') END AS source_disposition
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(
        names, contract, body, replay=True, permit_maintenance=True, check_revision=False,
    )
