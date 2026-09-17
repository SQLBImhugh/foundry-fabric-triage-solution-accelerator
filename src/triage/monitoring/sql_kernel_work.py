"""Family-bound work and partition state machines with explicit SQL results."""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    current_work,
    key_hash,
    partition_identity,
    procedure,
    record_insert,
    reply,
    save_receipt,
)
from triage.monitoring.sql_kernel_contracts import (
    CONTROLLER_WORK_KINDS,
    WORKER_WORK_KINDS,
    KernelObject,
    RpcContract,
    SqlNames,
)
from triage.monitoring.sql_kernel_retries import (
    linked_retry_lookup_sql,
    retry_admission_predicate,
    retry_admission_sql,
)
from triage.store.retries import MAX_ATTEMPTS


def _claim(names: SqlNames, contract: RpcContract, *, controller: bool) -> KernelObject:
    records, leases = names.table("monitoring_records"), names.table("monitoring_leases")
    families = CONTROLLER_WORK_KINDS if controller else WORKER_WORK_KINDS
    promote = f"""DECLARE @linked_action nvarchar(128)=JSON_VALUE(@stored_work,'$.action_reservation_id');
IF @linked_action IS NOT NULL
BEGIN
    IF @stored_work_kind='reconcile_state'
        THROW 51070, 'Reconciliation cannot be promoted into action work', 1;
    DECLARE @action_state varchar(40);
    SELECT @action_state=status FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action'
      AND full_key=@linked_action AND target_key=@stored_target_key;
    IF @action_state IS NULL THROW 51072, 'Existing work lost its action lineage', 1;
    SET @stored_work_kind=CASE WHEN @action_state='rejected' OR @stored_work_status='finalizing'
        THEN 'finalize' ELSE 'verify_action' END;
    SET @stored_work=JSON_MODIFY(@stored_work,'$.kind',@stored_work_kind);
END;""" if controller else """IF JSON_VALUE(@stored_work,'$.action_reservation_id') IS NOT NULL
   OR JSON_VALUE(@stored_work,'$.retry_of') IS NOT NULL
   OR JSON_VALUE(@stored_work,'$.finalization_id') IS NOT NULL
    THROW 51070, 'Worker work cannot carry controller lineage', 1;"""
    target_guard = ""
    target_write = ""
    retry_guard = ""
    if controller:
        retry_guard = f"""IF @stored_work_kind='deferred_retry'
BEGIN
    DECLARE @claim_predecessor nvarchar(max),@claim_parent nvarchar(max);
    SELECT @claim_predecessor=payload FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action'
      AND full_key=JSON_VALUE(@stored_work,'$.retry_of');
    SELECT @claim_parent=payload FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
      AND full_key=JSON_VALUE(@claim_predecessor,'$.request.work_id');
    IF @claim_predecessor IS NULL
       OR COALESCE(JSON_VALUE(@claim_predecessor,'$.state'),'')<>'rejected'
       OR COALESCE(JSON_VALUE(@claim_predecessor,'$.rejection.reason'),'')<>'throttled'
       OR COALESCE(JSON_VALUE(@claim_predecessor,'$.retry_work_id'),'')<>@work_id
       OR JSON_VALUE(@claim_predecessor,'$.retry_reservation_id') IS NOT NULL
       OR COALESCE(TRY_CONVERT(int,JSON_VALUE(@stored_work,'$.retry_attempt')),0) NOT BETWEEN 1 AND {MAX_ATTEMPTS}
       OR COALESCE(TRY_CONVERT(int,JSON_VALUE(@claim_predecessor,'$.retry_attempt')),-1)+1
           <>COALESCE(TRY_CONVERT(int,JSON_VALUE(@stored_work,'$.retry_attempt')),-2)
        THROW 51072, 'Deferred claim lost its unused bounded rejected predecessor', 1;
    IF COALESCE(JSON_VALUE(@claim_parent,'$.state'),'')<>'completed'
    BEGIN
        SET @status='not_acquired'; SET @result=N'{{"reason":"predecessor_not_finalized"}}';
        {reply(contract.operation)}
        RETURN;
    END;
    {retry_admission_sql(names, "JSON_QUERY(@claim_predecessor,'$.request')", '@stored_target_key')}
    IF NOT ({retry_admission_predicate()})
    BEGIN
        UPDATE {records} SET revision=revision+1,status='dispositioned',
            payload=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(payload,
                '$.state','dispositioned'),'$.lease',NULL),'$.revision',revision+1),
                '$.completed_at',CONVERT(nvarchar(40),@now,127)+N'Z'),
                '$.disposition','Current scope/review no longer admits this linked retry.')
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
          AND full_key=@work_id AND revision=@stored_work_revision;
        IF @@ROWCOUNT<>1 THROW 51072, 'Retry disposition lost its current revision', 1;
        SET @affected=1; SET @status='not_acquired';
        SET @result=N'{{"reason":"retry_no_longer_admitted"}}';
        {reply(contract.operation)}
        RETURN;
    END;
END;"""
        target_guard = f"""DECLARE @target_lease_key nvarchar(1024), @target_fence bigint,
    @target_owner nvarchar(128), @target_expires datetime2(6);
IF @stored_work_kind<>'reconcile_state'
BEGIN
    IF @stored_target_key IS NULL THROW 51073, 'Controller execution has no exact target', 1;
    SET @target_lease_key=N'controller:'+@stored_target_key;
    SELECT @target_fence=fence,@target_owner=owner_id,@target_expires=expires_at
    FROM {leases} WITH (UPDLOCK,HOLDLOCK)
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@target_lease_key
      AND key_hash={key_hash('@target_lease_key')};
    IF @target_expires>@now AND @target_owner<>@work_id
    BEGIN
        SET @status='not_acquired';
        SET @result=N'{{"reason":"target_owned"}}';
        {reply(contract.operation)}
        RETURN;
    END;
END;"""
        target_write = f"""IF @target_lease_key IS NOT NULL
BEGIN
    IF @target_fence IS NULL
        INSERT INTO {leases} (tenant_id,epoch,key_hash,full_key,owner_id,fence,acquired_at,expires_at)
        VALUES (@tenant_id,@epoch,{key_hash('@target_lease_key')},@target_lease_key,@work_id,1,@now,@expires);
    ELSE
        UPDATE {leases} SET owner_id=@work_id,fence=fence+1,acquired_at=@now,expires_at=@expires
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@target_lease_key
          AND key_hash={key_hash('@target_lease_key')} AND fence=@target_fence
          AND (expires_at<=@now OR owner_id=@work_id);
    IF @@ROWCOUNT<>1 THROW 51074, 'Target lease compare-and-set failed', 1;
END;"""
    body = f"""{current_work(names, families, live_lease=False)}
IF @lease_seconds NOT BETWEEN 1 AND 86400 OR LEN(@owner_id)=0
    THROW 51073, 'Invalid work lease request', 1;
{promote}
IF @maintenance=1 AND @stored_work_kind NOT IN ('verify_action','finalize','reconcile_state')
    THROW 51071, 'Maintenance stops new work claims', 1;
IF @stored_work_status NOT IN ('queued','waiting','leased','finalizing')
BEGIN
    SET @status='not_acquired'; SET @result=N'{{"reason":"terminal_work"}}';
    {reply(contract.operation)}
    RETURN;
END;
DECLARE @prior_fence bigint,@prior_owner nvarchar(128),@prior_expires datetime2(6),
    @due datetime2(6),@expires datetime2(6)=DATEADD(second,@lease_seconds,@now);
SELECT @due=due_at FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work' AND full_key=@work_id;
SELECT @prior_fence=fence,@prior_owner=owner_id,@prior_expires=expires_at
FROM {leases} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@work_key AND key_hash={key_hash('@work_key')};
IF @due>@now OR @prior_expires>@now
BEGIN
    SET @status='not_acquired'; SET @result=N'{{"reason":"not_due_or_owned"}}';
    {reply(contract.operation)}
    RETURN;
END;
{retry_guard}
{target_guard}
IF @prior_fence IS NULL
    INSERT INTO {leases} (tenant_id,epoch,key_hash,full_key,owner_id,fence,acquired_at,expires_at)
    VALUES (@tenant_id,@epoch,{key_hash('@work_key')},@work_key,@owner_id,1,@now,@expires);
ELSE
    UPDATE {leases} SET owner_id=@owner_id,fence=fence+1,acquired_at=@now,expires_at=@expires
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@work_key
      AND key_hash={key_hash('@work_key')} AND fence=@prior_fence AND expires_at<=@now;
IF @@ROWCOUNT<>1 THROW 51074, 'Work lease compare-and-set failed', 1;
{target_write}
DECLARE @lease_json nvarchar(max)=(
    SELECT @tenant_id AS tenant_id,@epoch AS epoch,@work_key AS resource_key,@owner_id AS owner_id,
        COALESCE(@prior_fence,0)+1 AS fence,CONVERT(nvarchar(40),@now,127)+N'Z' AS acquired_at,
        CONVERT(nvarchar(40),@expires,127)+N'Z' AS expires_at
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
SET @stored_work=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@stored_work,
    '$.state','leased'),'$.lease',JSON_QUERY(@lease_json)),
    '$.attempts',COALESCE(TRY_CONVERT(int,JSON_VALUE(@stored_work,'$.attempts')),0)+1),
    '$.revision',@stored_work_revision+1);
UPDATE {records} SET revision=revision+1,status='leased',work_kind=@stored_work_kind,
    due_at=@expires,payload=@stored_work
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
  AND full_key=@work_id AND key_hash={key_hash('@work_id')} AND revision=@stored_work_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Work row compare-and-set failed', 1;
SET @affected=1;
SET @result=(SELECT JSON_QUERY(@stored_work) AS work,JSON_QUERY(@lease_json) AS lease
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);"""
    return procedure(names, contract, body, permit_maintenance=True, check_revision=False)


def reconciliation_completion_sql(names: SqlNames) -> str:
    receipts = names.table("monitoring_receipts")
    return f"""SELECT 1 FROM {receipts} AS resolution
WHERE resolution.tenant_id=@tenant_id AND resolution.epoch=@epoch
  AND resolution.operation='controller.resolve_frontier'
  AND JSON_VALUE(resolution.payload,'$.result.work_id')=@work_id
  AND TRY_CONVERT(bigint,JSON_VALUE(resolution.payload,'$.result.work_fence'))=@fence
  AND JSON_VALUE(resolution.payload,'$.result.state') IN ('published','rejected')
  AND (JSON_VALUE(resolution.payload,'$.result.resolution_scope')<>'handoff_acknowledgement'
       OR EXISTS ({handoff_acknowledgement_receipts_sql(names)}))
  AND (COALESCE(JSON_VALUE(resolution.payload,'$.result.resolution_scope'),'handoff')<>'window_acknowledgement'
       OR EXISTS (SELECT 1 FROM {receipts} AS rejected
           WHERE rejected.tenant_id=@tenant_id AND rejected.epoch=@epoch
             AND rejected.operation='controller.resolve_frontier'
             AND rejected.request_id=COALESCE(JSON_VALUE(resolution.payload,'$.result.window_resolution_request_id'),
                 JSON_VALUE(resolution.payload,'$.result.window_rejection_request_id'))
             AND JSON_VALUE(rejected.payload,'$.result.state')=JSON_VALUE(resolution.payload,'$.result.state')
             AND ((JSON_VALUE(rejected.payload,'$.result.state')='rejected'
                   AND JSON_VALUE(rejected.payload,'$.result.resolution_scope')='window')
                  OR (JSON_VALUE(rejected.payload,'$.result.state')='published'
                   AND JSON_VALUE(rejected.payload,'$.result.resolution_scope')='handoff'))
             AND JSON_VALUE(rejected.payload,'$.result.frontier_key')
                 =JSON_VALUE(resolution.payload,'$.result.frontier_key')
             AND TRY_CONVERT(bigint,JSON_VALUE(rejected.payload,'$.result.validated_revision'))
                 =TRY_CONVERT(bigint,JSON_VALUE(resolution.payload,'$.result.validated_revision'))))"""


def handoff_acknowledgement_receipts_sql(names: SqlNames) -> str:
    """Completion retains the acknowledgement's original receipts, not latest state."""
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    return f"""SELECT 1 FROM {receipts} AS original
JOIN {receipts} AS root ON root.tenant_id=original.tenant_id AND root.epoch=original.epoch
  AND root.operation='controller.resolve_frontier'
  AND root.request_id=JSON_VALUE(resolution.payload,'$.result.frontier_resolution_request_id')
  AND JSON_VALUE(root.payload,'$.result.resolution_scope')='handoff'
  AND JSON_VALUE(root.payload,'$.result.state') IN ('published','rejected')
  AND JSON_VALUE(root.payload,'$.result.frontier_key')=JSON_VALUE(resolution.payload,'$.result.frontier_key')
  AND TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.frontier_revision'))
      =TRY_CONVERT(bigint,JSON_VALUE(resolution.payload,'$.result.frontier_resolution_revision'))
  AND TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.validated_revision'))
      =TRY_CONVERT(bigint,JSON_VALUE(resolution.payload,'$.result.validated_revision'))
  AND TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.frontier_revision'))
      =TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.validated_revision'))
JOIN {records} AS own_handoff ON own_handoff.tenant_id=original.tenant_id AND own_handoff.epoch=original.epoch
  AND own_handoff.record_kind='validation_handoff'
  AND own_handoff.parent_key=JSON_VALUE(original.payload,'$.result.frontier_key')
  AND own_handoff.sequence_number=TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.handoff_revision'))
  AND JSON_VALUE(own_handoff.payload,'$.work_id')=JSON_VALUE(original.payload,'$.result.work_id')
  AND JSON_VALUE(own_handoff.payload,'$.producer_request_id')=JSON_VALUE(original.payload,'$.result.producer_request_id')
  AND JSON_VALUE(own_handoff.payload,'$.requires_window')='false'
  AND own_handoff.status=JSON_VALUE(original.payload,'$.result.handoff_decision')
JOIN {records} AS root_handoff ON root_handoff.tenant_id=root.tenant_id AND root_handoff.epoch=root.epoch
  AND root_handoff.record_kind='validation_handoff'
  AND root_handoff.parent_key=JSON_VALUE(root.payload,'$.result.frontier_key')
  AND root_handoff.sequence_number=TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.handoff_revision'))
  AND JSON_VALUE(root_handoff.payload,'$.work_id')=JSON_VALUE(root.payload,'$.result.work_id')
  AND JSON_VALUE(root_handoff.payload,'$.producer_request_id')=JSON_VALUE(root.payload,'$.result.producer_request_id')
  AND JSON_VALUE(root_handoff.payload,'$.requires_window')='false'
  AND root_handoff.status=JSON_VALUE(root.payload,'$.result.handoff_decision')
WHERE original.tenant_id=@tenant_id AND original.epoch=@epoch
  AND original.operation='controller.resolve_frontier'
  AND original.request_id=JSON_VALUE(resolution.payload,'$.result.handoff_resolution_request_id')
  AND JSON_VALUE(original.payload,'$.result.resolution_scope')='handoff'
  AND JSON_VALUE(original.payload,'$.result.work_id')=@work_id
  AND JSON_VALUE(original.payload,'$.result.producer_request_id')=JSON_VALUE(resolution.payload,'$.result.producer_request_id')
  AND JSON_VALUE(original.payload,'$.result.frontier_key')=JSON_VALUE(resolution.payload,'$.result.frontier_key')
  AND JSON_VALUE(original.payload,'$.result.handoff_decision')=JSON_VALUE(resolution.payload,'$.result.state')
  AND JSON_VALUE(resolution.payload,'$.result.handoff_decision')=JSON_VALUE(resolution.payload,'$.result.state')
  AND JSON_VALUE(original.payload,'$.result.state') IN ('pending_validation',JSON_VALUE(resolution.payload,'$.result.state'))
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.work_fence'))
      =TRY_CONVERT(bigint,JSON_VALUE(resolution.payload,'$.result.handoff_resolution_work_fence'))
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.work_fence')) BETWEEN 1 AND @fence
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.handoff_revision'))
      =TRY_CONVERT(bigint,JSON_VALUE(resolution.payload,'$.result.handoff_revision'))
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.handoff_revision'))
      BETWEEN 1 AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.frontier_revision'))
  AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.frontier_revision'))
      <=TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.validated_revision'))
  AND TRY_CONVERT(bigint,JSON_VALUE(root.payload,'$.result.validated_revision'))
      <=TRY_CONVERT(bigint,JSON_VALUE(resolution.payload,'$.result.frontier_revision'))"""


def connector_collection_completion_sql(names: SqlNames) -> str:
    """Original eligible observation; a same-fence renewal does not erase acceptance."""
    return f"""SELECT 1 FROM {names.table('monitoring_receipts')} AS observation
WHERE observation.tenant_id=@tenant_id AND observation.epoch=@epoch
  AND observation.operation='worker.observe_connector'
  AND @stored_work_kind='connector_reconcile'
  AND JSON_VALUE(observation.payload,'$.result.work_id')=@work_id
  AND JSON_VALUE(observation.payload,'$.result.work_owner_id')=@owner_id
  AND TRY_CONVERT(bigint,JSON_VALUE(observation.payload,'$.result.work_fence'))=@fence
  AND TRY_CONVERT(bigint,JSON_VALUE(observation.payload,'$.result.work_revision')) BETWEEN 1 AND @work_revision
  AND JSON_VALUE(observation.payload,'$.result.connector_id')=JSON_VALUE(@stored_work,'$.connector_id')
  AND JSON_VALUE(observation.payload,'$.result.observation.connector_id')=JSON_VALUE(@stored_work,'$.connector_id')
  AND JSON_VALUE(observation.payload,'$.result.observation.tenant_id')=@tenant_id
  AND JSON_VALUE(observation.payload,'$.result.observation.epoch')=@epoch
  AND TRY_CONVERT(bigint,JSON_VALUE(observation.payload,'$.result.observation.policy_revision'))=@current_revision
  AND JSON_VALUE(observation.payload,'$.result.collection_completion_eligible')='true'
  AND EXISTS (SELECT 1 FROM OPENJSON(observation.payload,'$.result') AS eligible
      WHERE eligible.[key]='collection_completion_eligible' AND eligible.type=3)"""


def _transition(names: SqlNames, contract: RpcContract, *, controller: bool) -> KernelObject:
    records, leases, receipts = (
        names.table("monitoring_records"), names.table("monitoring_leases"),
        names.table("monitoring_receipts"),
    )
    families = CONTROLLER_WORK_KINDS if controller else WORKER_WORK_KINDS
    completion = f"""IF @transition='complete' AND @stored_work_kind<>'reconcile_state'
BEGIN
    IF @finalization_id IS NULL OR NOT EXISTS (
        SELECT 1 FROM {receipts}
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND operation='controller.finalize'
          AND request_id=@finalization_id
          AND JSON_VALUE(payload,'$.result.work_id')=@work_id
          AND JSON_VALUE(payload,'$.result.state')='completed'
    ) THROW 51072, 'Controller completion requires its original finalization receipt', 1;
END;""" if controller else f"""IF @finalization_id IS NOT NULL OR JSON_VALUE(@stored_work,'$.action_reservation_id') IS NOT NULL
    THROW 51070, 'Worker cannot bind controller finalization or actions', 1;
IF @transition='complete' AND NOT EXISTS (
    SELECT 1 FROM {receipts}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND operation='worker.accept_facts'
      AND JSON_VALUE(payload,'$.result.work_id')=@work_id
      AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.result.work_fence'))=@fence
) AND NOT EXISTS (
    {connector_collection_completion_sql(names)}
) THROW 51072, 'Collection completion requires a committed accepted batch or eligible original connector observation', 1;"""
    if controller:
        completion += """
IF @transition='disposition' AND @stored_work_kind<>'reconcile_state'
    THROW 51072, 'Controller execution must use incident finalization, not bare disposition', 1;
IF @stored_work_kind='reconcile_state' AND @finalization_id IS NOT NULL
    THROW 51070, 'Reconciliation cannot attach incident finalization lineage', 1;"""
        completion += f"""
IF @stored_work_kind='reconcile_state' AND @transition IN ('complete','disposition') AND NOT EXISTS (
    {reconciliation_completion_sql(names)}
) THROW 51072, 'Reconciliation completion needs its own committed publication or rejection', 1;"""
    target_lease = ""
    if controller:
        target_lease = f"""IF @stored_work_kind<>'reconcile_state'
BEGIN
    DECLARE @target_key nvarchar(1024)=N'controller:'+@stored_target_key;
    IF NOT EXISTS (SELECT 1 FROM {leases} WHERE tenant_id=@tenant_id AND epoch=@epoch
        AND full_key=@target_key AND owner_id=@work_id AND expires_at>@now)
        THROW 51074, 'Controller target ownership was lost', 1;
    UPDATE {leases} SET expires_at=@new_expiry
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@target_key
      AND owner_id=@work_id AND expires_at>@now;
    IF @@ROWCOUNT<>1 THROW 51074, 'Controller target transition failed', 1;
END;"""
    body = f"""{current_work(names, families)}
IF @stored_work_revision<>@work_revision OR @stored_work_status NOT IN ('leased','finalizing')
    THROW 51074, 'Work revision/state no longer belongs to this operation', 1;
IF @transition NOT IN ('renew','retry','complete','disposition')
    THROW 51073, 'Unsupported ownership-controlled work transition', 1;
IF @transition='renew' AND (@lease_seconds IS NULL OR @lease_seconds NOT BETWEEN 1 AND 86400)
    THROW 51073, 'Renewal requires a bounded lifetime', 1;
IF @transition='retry' AND (@retry_at IS NULL OR @retry_at<=@now)
    THROW 51073, 'Retry requires a future due time', 1;
IF @transition='disposition' AND NULLIF(@detail,N'') IS NULL
    THROW 51073, 'Disposition requires bounded recorded detail', 1;
IF @transition IN ('retry','disposition') AND JSON_VALUE(@stored_work,'$.action_reservation_id') IS NOT NULL
    THROW 51072, 'An existing effect retains verification/finalization ownership', 1;
IF @transition='complete' AND @stored_work_kind NOT IN ('verify_action','finalize','reconcile_state')
   AND (@maintenance=1 OR @current_revision<>@expected_revision)
    THROW 51072, 'Current policy no longer admits collection completion', 1;
{completion}
DECLARE @new_expiry datetime2(6)=CASE WHEN @transition='renew'
    THEN DATEADD(second,@lease_seconds,@now) ELSE @now END;
UPDATE {leases} SET expires_at=@new_expiry
WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@work_key
  AND key_hash={key_hash('@work_key')} AND owner_id=@owner_id AND fence=@fence AND expires_at>@now;
IF @@ROWCOUNT<>1 THROW 51074, 'Work lease transition lost ownership', 1;
{target_lease}
IF @transition='renew'
    SET @stored_work=JSON_MODIFY(@stored_work,'$.lease.expires_at',CONVERT(nvarchar(40),@new_expiry,127)+N'Z');
ELSE
BEGIN
    SET @stored_work=JSON_MODIFY(@stored_work,'$.lease',NULL);
    SET @stored_work=JSON_MODIFY(@stored_work,'$.state',
        CASE @transition WHEN 'retry' THEN 'waiting' WHEN 'complete' THEN 'completed' ELSE 'dispositioned' END);
    IF @transition='retry'
        SET @stored_work=JSON_MODIFY(@stored_work,'$.due_at',CONVERT(nvarchar(40),@retry_at,127)+N'Z');
    ELSE
        SET @stored_work=JSON_MODIFY(@stored_work,'$.completed_at',CONVERT(nvarchar(40),@now,127)+N'Z');
    IF @detail IS NOT NULL SET @stored_work=JSON_MODIFY(@stored_work,'$.disposition',@detail);
    IF @finalization_id IS NOT NULL SET @stored_work=JSON_MODIFY(@stored_work,'$.finalization_id',@finalization_id);
END;
SET @stored_work=JSON_MODIFY(@stored_work,'$.revision',@work_revision+1);
UPDATE {records} SET revision=revision+1,status=JSON_VALUE(@stored_work,'$.state'),
    due_at=CASE WHEN @transition='renew' THEN @new_expiry WHEN @transition='retry' THEN @retry_at ELSE due_at END,
    payload=@stored_work
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
  AND full_key=@work_id AND key_hash={key_hash('@work_id')} AND revision=@work_revision;
IF @@ROWCOUNT<>1 THROW 51072, 'Work transition lost its revision', 1;
SET @affected=1;
SET @result=(SELECT @work_id AS work_id,JSON_QUERY(@stored_work) AS work FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(
        names, contract, body, replay=True, permit_maintenance=True, check_revision=False,
    )


def _partition(names: SqlNames, contract: RpcContract) -> KernelObject:
    records, leases = names.table("monitoring_records"), names.table("monitoring_leases")
    body = f"""{partition_identity(names)}
IF @transition NOT IN ('claim','renew','release','pin_start')
    THROW 51073, 'Unsupported partition transition', 1;
IF @expected_ownership_revision<0
   OR (@expected_owner_id IS NOT NULL AND NOT ({canonical_guid('@expected_owner_id')}))
   OR (@new_owner_id IS NOT NULL AND NOT ({canonical_guid('@new_owner_id')}))
   OR (@expected_fence IS NOT NULL AND @expected_fence<1)
   OR (@transition IN ('release','pin_start') AND @new_owner_id IS NOT NULL)
    THROW 51073, 'Partition transition has invalid identity or cross-operation fields', 1;
DECLARE @journal_revision bigint,@prior_owner nvarchar(128),@prior_fence bigint,
    @prior_expiry datetime2(6),@acquired datetime2(6);
SELECT @journal_revision=revision FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='partition_ownership'
  AND full_key=@partition_key AND key_hash={key_hash('@partition_key')};
SELECT @prior_owner=owner_id,@prior_fence=fence,@prior_expiry=expires_at,@acquired=acquired_at
FROM {leases} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@partition_key AND key_hash={key_hash('@partition_key')};
IF COALESCE(@journal_revision,0)<>@expected_ownership_revision
    THROW 51074, 'Partition ownership journal revision changed', 1;
IF (@prior_fence IS NULL AND @expected_fence IS NOT NULL)
   OR (@prior_fence IS NOT NULL AND (@expected_fence IS NULL OR @prior_fence<>@expected_fence
       OR @expected_owner_id IS NULL OR @prior_owner<>@expected_owner_id))
    THROW 51074, 'Partition owner/fence compare-and-set failed', 1;
IF @transition<>'release' AND (
    @maintenance=1 OR COALESCE(JSON_VALUE(@connector,'$.state'),'') NOT IN ('ready','degraded'))
    THROW 51071, 'Partition is not enabled for this operation', 1;
IF @transition='pin_start'
BEGIN
    IF @prior_fence IS NULL OR @prior_expiry<=@now OR @first_sequence_number IS NULL OR @first_sequence_number<0
        THROW 51074, 'Stream start requires current ownership and actual broker sequence', 1;
    IF @broker_observed_at IS NULL OR @broker_observed_at>@now
        THROW 51073, 'Initial broker boundary must carry its actual nonfuture observation time', 1;
    DECLARE @pinned bigint,@start_payload nvarchar(max);
    SELECT @pinned=sequence_number,@start_payload=payload FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='stream_start' AND full_key=@partition_key;
    IF @pinned IS NOT NULL AND @pinned<>@first_sequence_number
        THROW 51072, 'Pinned broker start cannot be reset; record a separate gap observation', 1;
    IF @pinned IS NULL
    BEGIN
        SET @start_payload=(
            SELECT @partition_key AS partition_key,JSON_QUERY(@partition_json) AS partition,
                @first_sequence_number AS first_sequence_number,
                CONVERT(nvarchar(40),@broker_observed_at,127)+N'Z' AS broker_observed_at,
                CONVERT(nvarchar(40),@now,127)+N'Z' AS recorded_at,'unobserved' AS history_before_start,
                JSON_QUERY(N'[{{"code":"unobserved_stream_history","detail":"History before the actual pinned broker boundary was not observed."}}]') AS gaps
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
        {record_insert(names, 'stream_start', '@partition_key', '@start_payload', sequence='@first_sequence_number', parent_key='@connector_id')}
        SET @affected=1;
    END;
    SET @result=(SELECT @partition_key AS partition_key,JSON_QUERY(@partition_json) AS partition,
        @first_sequence_number AS first_sequence_number,@expected_ownership_revision AS ownership_revision,
        JSON_QUERY(@start_payload) AS start,@prior_owner AS last_owner_id,@prior_fence AS last_fence
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
END
ELSE
BEGIN
    IF @transition IN ('claim','renew') AND (
        @new_owner_id IS NULL OR @lease_seconds IS NULL OR @lease_seconds NOT BETWEEN 1 AND 86400)
        THROW 51073, 'Partition ownership requires a bounded owner/lifetime', 1;
    IF @transition='claim' AND @prior_expiry>@now AND @prior_owner<>@new_owner_id
        THROW 51074, 'Another unexpired partition owner exists', 1;
    IF @transition='renew' AND (@prior_expiry<=@now OR @prior_owner<>@new_owner_id OR @prior_fence IS NULL)
        THROW 51074, 'Partition renewal lost its original owner', 1;
    IF @transition='release' AND @prior_fence IS NULL
        THROW 51074, 'No partition ownership exists to release', 1;
    DECLARE @next_fence bigint=CASE WHEN @transition='renew' THEN @prior_fence
            ELSE COALESCE(@prior_fence,0)+1 END,
        @expires datetime2(6)=CASE WHEN @transition='release' THEN @now ELSE DATEADD(second,@lease_seconds,@now) END,
        @lease_json nvarchar(max),@ownership_json nvarchar(max);
        IF @transition='release' AND @expires<=@acquired SET @expires=DATEADD(microsecond,1,@acquired);
    IF @prior_fence IS NULL
        INSERT INTO {leases} (tenant_id,epoch,key_hash,full_key,owner_id,fence,acquired_at,expires_at)
        VALUES (@tenant_id,@epoch,{key_hash('@partition_key')},@partition_key,@new_owner_id,@next_fence,@now,@expires);
    ELSE
        UPDATE {leases} SET owner_id=COALESCE(@new_owner_id,@prior_owner),fence=@next_fence,
            acquired_at=CASE WHEN @transition='release' THEN acquired_at ELSE @now END,expires_at=@expires
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@partition_key
          AND key_hash={key_hash('@partition_key')} AND owner_id=@prior_owner AND fence=@prior_fence;
    IF @@ROWCOUNT<>1 THROW 51074, 'Partition lease update lost its predicate', 1;
    SET @lease_json=CASE WHEN @transition='release' THEN NULL ELSE (
        SELECT @tenant_id AS tenant_id,@epoch AS epoch,@partition_key AS resource_key,
            @new_owner_id AS owner_id,@next_fence AS fence,
            CONVERT(nvarchar(40),@now,127)+N'Z' AS acquired_at,
            CONVERT(nvarchar(40),@expires,127)+N'Z' AS expires_at
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) END;
    SET @ownership_json=(SELECT @partition_key AS partition_key,JSON_QUERY(@partition_json) AS partition,
        JSON_QUERY(@lease_json) AS lease,COALESCE(@new_owner_id,@prior_owner) AS last_owner_id,
        @next_fence AS last_fence,
        @expected_ownership_revision+1 AS ownership_revision,
        N'ownership:'+CONVERT(nvarchar(30),@expected_ownership_revision+1)+N':'+CONVERT(nvarchar(30),@next_fence) AS etag,
        CONVERT(nvarchar(40),@now,127)+N'Z' AS modified_at
        FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
    IF @journal_revision IS NULL
    BEGIN
        {record_insert(names, 'partition_ownership', '@partition_key', '@ownership_json', status="CASE WHEN @transition='release' THEN N'released' ELSE N'owned' END", parent_key='@connector_id')}
    END
    ELSE
    BEGIN
        UPDATE {records} SET revision=revision+1,payload=@ownership_json,
            status=CASE WHEN @transition='release' THEN 'released' ELSE 'owned' END
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='partition_ownership'
          AND full_key=@partition_key AND key_hash={key_hash('@partition_key')} AND revision=@expected_ownership_revision;
        IF @@ROWCOUNT<>1 THROW 51074, 'Partition journal update lost its predicate', 1;
    END;
    SET @affected=1;
    SET @result=@ownership_json;
END;
{save_receipt(names, contract.operation)}"""
    return procedure(
        names, contract, body, replay=True, permit_maintenance=True, check_revision=False,
    )


def work_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    result = {}
    for component in ("worker", "controller"):
        result[f"{component}.claim_work"] = _claim(
            names, contracts[f"{component}.claim_work"], controller=component == "controller",
        )
        result[f"{component}.transition_work"] = _transition(
            names, contracts[f"{component}.transition_work"], controller=component == "controller",
        )
    result["worker.partition"] = _partition(names, contracts["worker.partition"])
    result["controller.enqueue_work"] = _enqueue(names, contracts["controller.enqueue_work"])
    return result


def _enqueue(names: SqlNames, contract: RpcContract) -> KernelObject:
    records = names.table("monitoring_records")
    body = f"""IF NOT ({canonical_guid('@work_id')})
    THROW 51073, 'Work must be a canonical nonempty GUID', 1;
IF ISJSON(@draft_json)<>1 OR LEFT(LTRIM(@draft_json),1)<>N'{{' OR DATALENGTH(@draft_json)>1048576
    THROW 51073, 'A bounded typed work draft is required', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@draft_json) WHERE [key] NOT IN
    ('tenant_id','epoch','work_id','kind','policy_revision','due_at','created_at','reason',
     'target','execution','scope_id','discovery_selector','connector_id','action_reservation_id'))
    THROW 51073, 'Work enqueue cannot supply ownership, retry, finalization or state fields', 1;
DECLARE @kind varchar(32)=JSON_VALUE(@draft_json,'$.kind'),
    @target nvarchar(max)=JSON_QUERY(@draft_json,'$.target'),
    @execution nvarchar(max)=JSON_QUERY(@draft_json,'$.execution'),
    @target_key nvarchar(1024),@due datetime2(6)=TRY_CONVERT(datetime2(6),JSON_VALUE(@draft_json,'$.due_at'));
IF @kind='deferred_retry'
BEGIN
    {linked_retry_lookup_sql(names)}
    {save_receipt(names, contract.operation)}
    {reply(contract.operation)}
    RETURN;
END;
IF COALESCE(JSON_VALUE(@draft_json,'$.tenant_id'),'')<>@tenant_id
   OR COALESCE(JSON_VALUE(@draft_json,'$.epoch'),'')<>@epoch
   OR COALESCE(JSON_VALUE(@draft_json,'$.work_id'),'')<>@work_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@draft_json,'$.policy_revision')),-1)<>@current_revision
   OR COALESCE(@kind,'') NOT IN ('inventory','capability_probe','poll','connector_reconcile','triage','verify_action','finalize')
   OR @due IS NULL
    THROW 51073, 'Work draft identity, family or due time is invalid', 1;
IF @maintenance=1 AND @kind NOT IN ('verify_action','finalize')
    THROW 51071, 'Maintenance forbids new non-followup work', 1;
IF @kind IN ('triage','verify_action','finalize','capability_probe','poll')
BEGIN
    IF @target IS NULL OR COALESCE(JSON_VALUE(@target,'$.tenant_id'),'')<>@tenant_id
       OR COALESCE(JSON_VALUE(@target,'$.epoch'),'')<>@epoch
       OR COALESCE(JSON_VALUE(@target,'$.workload'),'') NOT IN ('powerbi','fabric_pipeline')
       OR NOT ({canonical_guid("JSON_VALUE(@target,'$.workspace_id')")})
       OR NOT ({canonical_guid("JSON_VALUE(@target,'$.item_id')")})
        THROW 51073, 'Target work needs a canonical current-context target', 1;
    SET @target_key=N'monitor:v1:'+@epoch+N':'+@tenant_id+N':'+JSON_VALUE(@target,'$.workload')
        +N':'+JSON_VALUE(@target,'$.workspace_id')+N':'+JSON_VALUE(@target,'$.item_id');
END;
IF @kind IN ('triage','verify_action','finalize') AND (
    @execution IS NULL OR COALESCE(JSON_QUERY(@execution,'$.target'),'')<>@target
    OR NULLIF(JSON_VALUE(@execution,'$.run_id'),'') IS NULL)
    THROW 51073, 'Controller work requires the exact original execution', 1;
IF @kind IN ('verify_action','finalize')
BEGIN
    IF NOT EXISTS (SELECT 1 FROM {records}
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='action'
          AND full_key=JSON_VALUE(@draft_json,'$.action_reservation_id') AND target_key=@target_key
          AND JSON_QUERY(payload,'$.request.source_execution') COLLATE Latin1_General_100_BIN2
              =@execution COLLATE Latin1_General_100_BIN2)
        THROW 51072, 'Effect follow-up requires the existing action and original source execution', 1;
END
ELSE IF JSON_VALUE(@draft_json,'$.action_reservation_id') IS NOT NULL
    THROW 51073, 'Initial non-effect work cannot attach a reservation', 1;
IF @kind='inventory' AND (
    (JSON_QUERY(@draft_json,'$.discovery_selector') IS NULL
     AND NOT EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
         AND record_kind='scope' AND full_key=JSON_VALUE(@draft_json,'$.scope_id')))
    OR (JSON_QUERY(@draft_json,'$.discovery_selector') IS NOT NULL
        AND COALESCE(JSON_VALUE(@draft_json,'$.discovery_selector.tenant_id'),'')<>@tenant_id))
    THROW 51073, 'Inventory needs an explicit same-tenant selector or existing scope', 1;
IF @kind IN ('poll','triage') AND NOT EXISTS (
    SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='target'
      AND full_key=@target_key AND JSON_VALUE(payload,'$.state')='current'
      AND JSON_VALUE(payload,'$.observation.enabled')='true'
      AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.policy_revision'))=@current_revision)
    THROW 51072, 'New target work is not currently admitted', 1;
IF @kind='connector_reconcile' AND NOT EXISTS (SELECT 1 FROM {records}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector'
      AND full_key=JSON_VALUE(@draft_json,'$.connector_id'))
    THROW 51072, 'Connector work needs an owned desired manifest', 1;
IF EXISTS (SELECT 1 FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
    AND record_kind='work' AND full_key=@work_id)
    THROW 51072, 'Enqueue never updates or adopts an existing work row', 1;
DECLARE @clean nvarchar(max)=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(
    @draft_json,'$.state','queued'),'$.revision',1),'$.attempts',0),'$.retry_attempt',0),
    '$.created_at',CONVERT(nvarchar(40),@now,127)+N'Z');
{record_insert(names, 'work', '@work_id', '@clean', status="N'queued'", work_kind='@kind', due_at='@due', target_key='@target_key', workspace="JSON_VALUE(@target,'$.workspace_id')", item="JSON_VALUE(@target,'$.item_id')", workload="JSON_VALUE(@target,'$.workload')")}
SET @affected=1;
SET @result=(SELECT @work_id AS work_id,JSON_QUERY(@clean) AS work FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return procedure(names, contract, body, replay=True, permit_maintenance=True)
