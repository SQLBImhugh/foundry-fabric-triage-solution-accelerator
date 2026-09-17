"""Typed web intents and internal insert-only controller handoff SQL."""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    key_hash,
    procedure,
    record_insert,
    save_receipt,
)
from triage.monitoring.sql_kernel_contracts import KernelObject, RpcContract, SqlNames
from triage.monitoring.sql_kernel_frontiers import raise_frontier_sql


def _invalid_selector(expression: str) -> str:
    kind = f"JSON_VALUE({expression},'$.kind')"
    domain = f"JSON_VALUE({expression},'$.domain_id')"
    workspace = f"JSON_VALUE({expression},'$.workspace_id')"
    item = f"JSON_VALUE({expression},'$.item_id')"
    return f"""ISJSON({expression})<>1 OR LEFT(LTRIM({expression}),1)<>N'{{'
        OR COALESCE(JSON_VALUE({expression},'$.tenant_id'),'')<>@tenant_id
        OR COALESCE({kind},'') NOT IN ('tenant','domain','workspace','item')
        OR EXISTS (SELECT 1 FROM OPENJSON({expression}) WHERE [key] NOT IN
            ('tenant_id','kind','domain_id','workspace_id','item_id','include_descendants'))
        OR ({kind}='domain' AND NOT ({canonical_guid(domain)}))
        OR ({kind}<>'domain' AND {domain} IS NOT NULL)
        OR ({kind} IN ('workspace','item') AND NOT ({canonical_guid(workspace)}))
        OR ({kind} NOT IN ('workspace','item') AND {workspace} IS NOT NULL)
        OR ({kind}='item' AND NOT ({canonical_guid(item)}))
        OR ({kind}<>'item' AND {item} IS NOT NULL)
        OR COALESCE(JSON_VALUE({expression},'$.include_descendants'),'false') NOT IN ('true','false')
        OR ({kind}<>'domain' AND JSON_VALUE({expression},'$.include_descendants')='true')"""


def handoff_sql(
    names: SqlNames, *, producer: str, operation: str, topic: str, reference: str,
    target: str = "NULL", collection_id: str = "NULL", requires_window: str = "0",
    collection_complete: str = "1", window_start: str = "NULL", window_end: str = "NULL",
) -> str:
    if producer not in {"worker", "web"}:
        raise ValueError("Only fixed producer handoffs are supported")
    # No caller work payload: SQL constructs a clean initial reconciliation row.
    # Native FOR JSON can return NULL for an empty set; typed evidence is an array.
    request_kind = f"{producer}_reconcile_request"
    return f"""DECLARE @reconcile_id nvarchar(36)=LOWER(CONVERT(nvarchar(36),NEWID()));
{raise_frontier_sql(names, producer=producer, operation=operation, topic=topic, reference=reference, target=target, collection_id=collection_id, requires_window=requires_window, collection_complete=collection_complete, window_start=window_start, window_end=window_end)}
DECLARE @handoff_payload nvarchar(max)=(
    SELECT @tenant_id AS tenant_id,@epoch AS epoch,@request_id AS request_id,
           N'{producer}' AS producer,{topic} AS topic,{reference} AS reference_id,
           @fingerprint AS fingerprint,@current_revision AS policy_revision,@reconcile_id AS work_id,
           JSON_QUERY(@frontier_target) AS target,JSON_QUERY(@frontier_window) AS [window],
           @frontier_key AS frontier_key,@frontier_revision AS frontier_revision,
           JSON_QUERY(@binding_json) AS request_payload,
           JSON_QUERY(COALESCE((SELECT JSON_VALUE(payload,'$.fact_kind') AS kind,
               JSON_VALUE(payload,'$.fact_key') AS [key],
               TRY_CONVERT(bigint,JSON_VALUE(payload,'$.fact_revision')) AS revision,
               LOWER(JSON_VALUE(payload,'$.payload_hash')) AS payload_hash
               FROM {names.table('monitoring_records')}
               WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='accepted_fact'
                 AND JSON_VALUE(payload,'$.batch_id')=@request_id
               ORDER BY full_key COLLATE Latin1_General_100_BIN2 FOR JSON PATH),N'[]')) AS evidence,
           CONVERT(nvarchar(40),@now,127)+N'Z' AS created_at
    FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
{record_insert(names, request_kind, '@request_id', '@handoff_payload', status="N'pending'", target_key='@frontier_target_key', parent_key='@frontier_key', sequence='@frontier_revision')}
DECLARE @clean_work nvarchar(max)=(
    SELECT @tenant_id AS tenant_id,@epoch AS epoch,@reconcile_id AS work_id,
           'reconcile_state' AS kind,@current_revision AS policy_revision,
           CONVERT(nvarchar(40),@now,127)+N'Z' AS created_at,
           CONVERT(nvarchar(40),@now,127)+N'Z' AS due_at,
           'Accepted intent requires deterministic reconciliation' AS reason,
           1 AS revision,0 AS attempts,0 AS retry_attempt,'queued' AS state,
           @request_id AS reconcile_request_id,N'{producer}' AS reconcile_producer,
           JSON_QUERY(@frontier_target) AS target,
           CAST(NULL AS nvarchar(128)) AS action_reservation_id,
           CAST(NULL AS nvarchar(128)) AS retry_of,CAST(NULL AS nvarchar(128)) AS finalization_id,
           CAST(NULL AS nvarchar(max)) AS lease,CAST(NULL AS datetime2(6)) AS completed_at
    FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
{record_insert(names, 'work', '@reconcile_id', '@clean_work', status="N'queued'", work_kind="N'reconcile_state'", due_at='@now', parent_key='@request_id', target_key='@frontier_target_key')}"""


def intent_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    result = {}
    for operation in ("inspect", "lock_context"):
        body = """SET @status='read';
SET @result=(SELECT @tenant_id AS tenant_id,@epoch AS epoch,@current_revision AS revision,
    @maintenance AS maintenance,CONVERT(nvarchar(40),@now,127)+N'Z' AS observed_at
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);"""
        result[operation] = procedure(
            names, contracts[operation], body,
            permit_maintenance=True, check_revision=False,
        )
    table = names.table("monitoring_records")
    control = names.table("monitoring_control")
    body = f"""IF @intent_kind NOT IN ('preview','scope','review','discovery')
    THROW 51073, 'Unsupported typed web intent', 1;
IF ISJSON(@intent_json)<>1 OR LEFT(LTRIM(@intent_json),1)<>N'{{' OR DATALENGTH(@intent_json)>1048576
    THROW 51073, 'Web intent must be a bounded JSON object', 1;
IF @expected_intent_revision<0 OR NOT ({canonical_guid('@intent_id')})
    THROW 51073, 'Invalid intent identity/revision', 1;
DECLARE @record_kind varchar(40)=CASE @intent_kind WHEN 'preview' THEN 'plan'
    WHEN 'scope' THEN 'scope' WHEN 'review' THEN 'review_request' ELSE 'discovery_request' END;
DECLARE @prior_revision bigint, @prior_intent nvarchar(max);
SELECT @prior_revision=revision,@prior_intent=payload FROM {table}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind=@record_kind
  AND full_key=@intent_id AND key_hash={key_hash('@intent_id')};
IF COALESCE(@prior_revision,0)<>@expected_intent_revision
    THROW 51072, 'Intent revision changed', 1;
IF @intent_kind='scope'
BEGIN
    IF COALESCE(JSON_VALUE(@intent_json,'$.scope_id'),'')<>@intent_id
       OR COALESCE(JSON_VALUE(@intent_json,'$.enabled'),'') NOT IN ('true','false')
       OR JSON_QUERY(@intent_json,'$.rules') IS NULL
       OR LEFT(LTRIM(JSON_QUERY(@intent_json,'$.rules')),1)<>N'['
       OR EXISTS (SELECT 1 FROM OPENJSON(@intent_json)
                  WHERE [key] NOT IN ('tenant_id','epoch','scope_id','name','enabled','rules','cadence'))
       OR COALESCE(JSON_VALUE(@intent_json,'$.tenant_id'),'')<>@tenant_id
       OR COALESCE(JSON_VALUE(@intent_json,'$.epoch'),'')<>@epoch
       OR NULLIF(JSON_VALUE(@intent_json,'$.name'),'') IS NULL
       OR (SELECT COUNT(*) FROM OPENJSON(@intent_json,'$.rules'))>1000
       OR EXISTS (SELECT 1 FROM OPENJSON(@intent_json) WHERE [key]='enabled' AND type<>3)
        THROW 51073, 'Scope intent has unsupported fields', 1;
    IF EXISTS (
        SELECT 1 FROM OPENJSON(@intent_json,'$.rules') AS scope_rule
        WHERE JSON_QUERY(scope_rule.value,'$.selector') IS NULL
           OR {_invalid_selector("JSON_QUERY(scope_rule.value,'$.selector')")}
           OR NOT ({canonical_guid("JSON_VALUE(scope_rule.value,'$.rule_id')")})
           OR COALESCE(JSON_VALUE(scope_rule.value,'$.effect'),'') NOT IN ('include','exclude')
           OR EXISTS (SELECT 1 FROM OPENJSON(scope_rule.value) WHERE [key] NOT IN
                ('rule_id','selector','effect','workloads','auto_enrol_detection_only'))
           OR COALESCE(JSON_VALUE(scope_rule.value,'$.auto_enrol_detection_only'),'false') NOT IN ('true','false')
           OR (JSON_VALUE(scope_rule.value,'$.effect')='exclude'
               AND JSON_VALUE(scope_rule.value,'$.auto_enrol_detection_only')='true')
           OR JSON_QUERY(scope_rule.value,'$.workloads') IS NULL
           OR (SELECT COUNT(*) FROM OPENJSON(scope_rule.value,'$.workloads')) NOT BETWEEN 1 AND 2
           OR EXISTS (SELECT 1 FROM OPENJSON(scope_rule.value,'$.workloads')
               WHERE type<>1 OR value NOT IN ('powerbi','fabric_pipeline'))
    ) THROW 51073, 'Scope rules must be bounded typed current-tenant selectors', 1;
    IF EXISTS (SELECT JSON_VALUE(value,'$.rule_id') FROM OPENJSON(@intent_json,'$.rules')
        GROUP BY JSON_VALUE(value,'$.rule_id') HAVING COUNT(*)>1)
        THROW 51073, 'Scope rule identities must be unique', 1;
END;
IF @intent_kind='review'
BEGIN
    IF EXISTS (SELECT 1 FROM OPENJSON(@intent_json)
        WHERE [key] NOT IN ('review_id','target','action','requested_state','reviewer_id','reviewed_at',
            'expires_at','parameters','parameter_hash','definition_hash','configuration_hash',
            'replay_safe','detail'))
       OR COALESCE(JSON_VALUE(@intent_json,'$.review_id'),'')<>@intent_id
       OR COALESCE(JSON_VALUE(@intent_json,'$.target.tenant_id'),'')<>@tenant_id
       OR COALESCE(JSON_VALUE(@intent_json,'$.target.epoch'),'')<>@epoch
       OR COALESCE(JSON_VALUE(@intent_json,'$.target.workload'),'') NOT IN ('powerbi','fabric_pipeline')
       OR NOT ({canonical_guid("JSON_VALUE(@intent_json,'$.target.workspace_id')")})
       OR NOT ({canonical_guid("JSON_VALUE(@intent_json,'$.target.item_id')")})
       OR COALESCE(JSON_VALUE(@intent_json,'$.action'),'') NOT IN
            ('pipeline_rerun','powerbi_refresh','rebind_dataset_gateway','reenable_refresh_schedule')
       OR COALESCE(JSON_VALUE(@intent_json,'$.requested_state'),'') NOT IN ('pending','verified','revoked','unverifiable')
       OR NOT ({canonical_guid("JSON_VALUE(@intent_json,'$.reviewer_id')")})
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@intent_json,'$.reviewed_at')) IS NULL
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@intent_json,'$.reviewed_at'))>TODATETIMEOFFSET(@now,'+00:00')
       OR (JSON_VALUE(@intent_json,'$.requested_state')<>'revoked'
           AND TRY_CONVERT(datetime2(6),JSON_VALUE(@intent_json,'$.expires_at'))<=@now)
       OR TRY_CONVERT(datetime2(6),JSON_VALUE(@intent_json,'$.expires_at')) IS NULL
        THROW 51073, 'Review accepts human intent, not platform verification fields', 1;
    IF @prior_intent IS NOT NULL AND JSON_VALUE(@intent_json,'$.requested_state')='revoked' AND (
        TRY_CONVERT(datetimeoffset,JSON_VALUE(@prior_intent,'$.reviewed_at')) IS NULL
        OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@prior_intent,'$.reviewed_at'))
           <>TRY_CONVERT(datetimeoffset,JSON_VALUE(@intent_json,'$.reviewed_at'))
        OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@prior_intent,'$.expires_at'))
           <>TRY_CONVERT(datetimeoffset,JSON_VALUE(@intent_json,'$.expires_at')))
        THROW 51072, 'Revocation must retain the original review and expiry timestamps', 1;
    IF @prior_intent IS NOT NULL AND (
        COALESCE(JSON_QUERY(@prior_intent,'$.target'),'') COLLATE Latin1_General_100_BIN2
            <>COALESCE(JSON_QUERY(@intent_json,'$.target'),'') COLLATE Latin1_General_100_BIN2
        OR COALESCE(JSON_VALUE(@prior_intent,'$.action'),'')<>COALESCE(JSON_VALUE(@intent_json,'$.action'),''))
        THROW 51072, 'Review identity cannot move target or action', 1;
END;
IF @intent_kind='discovery' AND ({_invalid_selector('@intent_json')})
    THROW 51073, 'Discovery selector is not an explicit current-tenant intent', 1;
IF @intent_kind IN ('scope','review')
BEGIN
    UPDATE {control}
    SET revision=revision+1,updated_at=@now,
        payload=JSON_MODIFY(JSON_MODIFY(payload,'$.revision',revision+1),
            '$.updated_at',CONVERT(nvarchar(40),@now,127)+N'Z')
    WHERE singleton=1 AND tenant_id=@tenant_id AND epoch=@epoch AND revision=@expected_revision;
    IF @@ROWCOUNT<>1 THROW 51072, 'Policy revision advance lost its compare-and-set', 1;
    SET @current_revision=@current_revision+1;
END;
DECLARE @intent_state varchar(32)=CASE WHEN @intent_kind='review' THEN 'pending-validation' ELSE 'configuring' END;
DECLARE @stored_intent nvarchar(max)=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(
    @intent_json,'$.policy_revision',@current_revision),'$.revision',@expected_intent_revision+1),
    '$.request_id',@request_id);
IF @prior_revision IS NULL
BEGIN
    INSERT INTO {table}
        (tenant_id,epoch,record_kind,key_hash,full_key,revision,status,payload)
    VALUES (@tenant_id,@epoch,@record_kind,{key_hash('@intent_id')},@intent_id,1,@intent_state,@stored_intent);
END
ELSE
BEGIN
    UPDATE {table} SET revision=revision+1,status=@intent_state,payload=@stored_intent
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind=@record_kind
      AND full_key=@intent_id AND key_hash={key_hash('@intent_id')} AND revision=@expected_intent_revision;
    IF @@ROWCOUNT<>1 THROW 51072, 'Intent write lost its compare-and-set', 1;
END;
SET @affected=1;
IF @intent_kind<>'preview'
BEGIN
    {handoff_sql(names, producer='web', operation='web.commit_intent', topic='@intent_kind', reference='@intent_id', target="CASE WHEN @intent_kind='review' THEN JSON_QUERY(@intent_json,'$.target') ELSE NULL END")}
    SET @result=(SELECT @request_id AS request_id,@intent_id AS intent_id,@intent_kind AS intent_kind,
        @expected_intent_revision AS expected_intent_revision,
        @expected_intent_revision+1 AS new_intent_revision,@current_revision AS policy_revision,
        @intent_state AS state,@reconcile_id AS reconcile_work_id,
        @frontier_key AS frontier_key,@frontier_revision AS frontier_revision,
        CASE WHEN @intent_kind='review' THEN JSON_VALUE(@intent_json,'$.requested_state') END AS requested_state,
        CASE WHEN @intent_kind='review' THEN 'pending_validation' END AS publication_status,
        JSON_QUERY(@intent_json) AS original_intent
        FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
END
ELSE
    SET @result=(SELECT @request_id AS request_id,@intent_id AS intent_id,'draft' AS state,
        @current_revision AS policy_revision,JSON_QUERY(@intent_json) AS original_intent
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, 'web.commit_intent')}"""
    result["web.commit_intent"] = procedure(
        names, contracts["web.commit_intent"], body, replay=True,
    )
    return result
