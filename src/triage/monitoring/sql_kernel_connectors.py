"""Typed controller publication of desired transport scope and readiness.

The worker reports observations. Only this controller RPC can publish desired
scope or promote a matched, receipt-bound observation into readiness proof.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    current_work,
    exact_text_equal,
    key_hash,
    literals,
    payload_hash,
    procedure,
    record_insert,
    save_receipt,
)
from triage.monitoring.sql_kernel_contracts import KernelObject, RpcContract, SqlNames
from triage.monitoring.sql_kernel_proposals import proposal_binding_sql
from triage.monitoring.sql_kernel_removals import (
    retirement_records_sql,
    source_is_pending_removal_sql,
)

PUBLICATION_FIELDS = (
    "connector_id", "ownership_id", "work_id", "lease_owner_id", "lease_fence",
    "expected_work_revision", "expected_connector_revision", "policy_revision",
    "producer_request_id", "producer_fingerprint", "frontier_key", "frontier_revision",
    "name", "sources", "source_proposals", "source_removals", "desired_definition",
    "observation_receipt_id", "readiness_receipt_id", "detail",
)
SUBSCRIPTION_TYPES = tuple(
    f"Microsoft.Fabric.JobEvents.{name}"
    for name in ("ItemJobCreated", "ItemJobStatusChanged", "ItemJobSucceeded", "ItemJobFailed")
)


def subscription_type_sql(receipt: str) -> str:
    """Use the existing exact wire mapping, including typed receiver evidence."""
    wire = (
        f"COALESCE(JSON_VALUE({receipt},'$.event_type'),"
        f"JSON_VALUE({receipt},'$.observation.evidence.native_event_type'),"
        f"JSON_VALUE({receipt},'$.observation.evidence.subscription_event_type'))"
    )
    mappings = {
        value.replace(".JobEvents.", "."): value for value in SUBSCRIPTION_TYPES
    } | {value: value for value in SUBSCRIPTION_TYPES if value.endswith(("ItemJobCreated", "ItemJobFailed"))}
    clauses = " ".join(
        f"WHEN {exact_text_equal(wire, literals((source,)))} THEN N'{target}'"
        for source, target in mappings.items()
    )
    return f"CASE {clauses} ELSE NULL END"


def source_authorized_predicate() -> str:
    return """COALESCE(JSON_VALUE(@approved_target,'$.state'),'')='current'
AND COALESCE(JSON_VALUE(@approved_target,'$.observation.enabled'),'')='true'
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@approved_target,'$.policy_revision')),-1)=@current_revision
AND COALESCE(JSON_VALUE(@source_capability,'$.read_status'),'')='verified'
AND COALESCE(JSON_VALUE(@source_capability,'$.event_status'),'')='verified'
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@source_capability,'$.expires_at')) IS NOT NULL
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@source_capability,'$.expires_at'))>TODATETIMEOFFSET(@now,'+00:00')
AND COALESCE(JSON_QUERY(@source_capability,'$.target'),'') COLLATE Latin1_General_100_BIN2
    =COALESCE(@source_target,'missing') COLLATE Latin1_General_100_BIN2"""


def publish_update_sql(names: SqlNames) -> str:
    return f"""UPDATE {names.table('monitoring_records')} SET revision=revision+1,
    status=JSON_VALUE(@next,'$.state'),payload=@next
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector'
  AND full_key=@connector_id AND revision=@expected_connector_revision
  AND JSON_VALUE(payload,'$.ownership_id')=@ownership_id;"""


def desired_update_expression() -> str:
    return """JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@prior,'$.name',@name),
    '$.sources',JSON_QUERY(@sources)),'$.desired_definition',JSON_QUERY(@definition)),
    '$.policy_revision',@current_revision)"""


def invalidate_readiness_expression() -> str:
    return """JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@next,'$.state','provisioning'),
    '$.identity_verified_at',NULL),'$.delivery_verified_at',NULL)"""


def restore_worker_proof_expression() -> str:
    return """JSON_MODIFY(JSON_MODIFY(@next,
    '$.identity_verified_at',JSON_VALUE(@prior,'$.identity_verified_at')),
    '$.delivery_verified_at',JSON_VALUE(@prior,'$.delivery_verified_at'))"""


def worker_ready_upgrade_sql() -> str:
    return "JSON_VALUE(@next,'$.state')='ready' AND COALESCE(JSON_VALUE(@prior,'$.state'),'')<>'ready'"


def connector_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    contract = contracts["controller.publish_connector"]
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    allowed = ",".join(f"'{field}'" for field in PUBLICATION_FIELDS)
    prior_stream = """JSON_VALUE(@prior,'$.desired_definition.parts."eventstream.json".streams[0].name')"""
    prior_destination = """JSON_VALUE(@prior,'$.desired_definition.parts."eventstream.json".destinations[0].name')"""
    prior_sources = """OPENJSON(@prior,'$.desired_definition.parts."eventstream.json".sources')"""
    body = f"""{current_work(names, ('reconcile_state',))}
IF @stored_work_revision<>@work_revision OR @stored_work_status<>'leased'
    THROW 51074, 'Connector publication requires its own current reconciliation lease/revision', 1;
DECLARE @plan nvarchar(max);
SELECT @plan=payload FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector_publication'
  AND full_key=@publication_id AND key_hash={key_hash('@publication_id')}
  AND {payload_hash('payload')}=@publication_hash;
IF @plan IS NULL OR EXISTS (SELECT 1 FROM OPENJSON(@plan) WHERE [key] NOT IN ({allowed}))
   OR COALESCE(JSON_VALUE(@plan,'$.work_id'),'')<>@work_id
   OR COALESCE(JSON_VALUE(@plan,'$.lease_owner_id'),'')<>@owner_id
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.lease_fence')),-1)<>@fence
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.expected_work_revision')),-1)<>@work_revision
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.policy_revision')),-1)<>@current_revision
   OR COALESCE(JSON_VALUE(@plan,'$.producer_request_id'),'')
       <>COALESCE(JSON_VALUE(@stored_work,'$.reconcile_request_id'),'missing')
    THROW 51072, 'Connector publication is not the fixed current-work/controller plan', 1;
IF NOT EXISTS (SELECT 1 FROM {records} AS h JOIN {records} AS f
    ON f.tenant_id=h.tenant_id AND f.epoch=h.epoch AND f.record_kind='validation_frontier'
   AND f.full_key=h.parent_key
    WHERE h.tenant_id=@tenant_id AND h.epoch=@epoch AND h.record_kind='validation_handoff'
      AND JSON_VALUE(h.payload,'$.work_id')=@work_id
      AND JSON_VALUE(h.payload,'$.producer_request_id')=JSON_VALUE(@plan,'$.producer_request_id')
      AND JSON_VALUE(h.payload,'$.producer_fingerprint')=JSON_VALUE(@plan,'$.producer_fingerprint')
      AND TRY_CONVERT(bigint,JSON_VALUE(h.payload,'$.policy_revision'))=@current_revision
      AND h.parent_key=JSON_VALUE(@plan,'$.frontier_key')
      AND f.sequence_number=TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.frontier_revision'))
      AND EXISTS (SELECT 1 FROM {receipts} AS r WHERE r.tenant_id=@tenant_id AND r.epoch=@epoch
          AND r.operation=JSON_VALUE(h.payload,'$.producer_operation')
          AND r.request_id=JSON_VALUE(h.payload,'$.producer_request_id')
          AND r.fingerprint=JSON_VALUE(h.payload,'$.producer_fingerprint')
          AND JSON_VALUE(r.payload,'$.binding_hash')=JSON_VALUE(h.payload,'$.producer_binding_hash')))
    THROW 51072, 'Connector source-intent/frontier/receipt binding changed', 1;
DECLARE @connector_id nvarchar(128)=JSON_VALUE(@plan,'$.connector_id'),
    @ownership_id nvarchar(128)=JSON_VALUE(@plan,'$.ownership_id'),
    @expected_connector_revision bigint=TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.expected_connector_revision')),
    @name nvarchar(200)=JSON_VALUE(@plan,'$.name'),@sources nvarchar(max)=JSON_QUERY(@plan,'$.sources'),
    @definition nvarchar(max)=JSON_QUERY(@plan,'$.desired_definition'),
    @readiness_id nvarchar(256)=JSON_VALUE(@plan,'$.readiness_receipt_id'),
    @prior nvarchar(max),@version bigint,@next nvarchar(max),@desired nvarchar(max),@desired_changed bit=1;
IF NOT ({canonical_guid('@connector_id')}) OR NOT ({canonical_guid('@ownership_id')})
   OR @expected_connector_revision IS NULL OR @expected_connector_revision<0
   OR NULLIF(@name,'') IS NULL OR LEN(JSON_VALUE(@plan,'$.name'))>200
   OR @sources IS NULL OR LEFT(LTRIM(@sources),1)<>'[' OR DATALENGTH(@sources)>1048576
   OR @definition IS NULL OR LEFT(LTRIM(@definition),1)<>'{{' OR DATALENGTH(@definition)>1048576
   OR (SELECT COUNT(*) FROM OPENJSON(@sources))>1000
    THROW 51073, 'Desired connector fields are malformed or outside their typed bounds', 1;
SELECT @prior=payload,@version=revision FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector'
  AND full_key=@connector_id AND key_hash={key_hash('@connector_id')};
IF COALESCE(@version,0)<>@expected_connector_revision
   OR (@prior IS NOT NULL AND (COALESCE(JSON_VALUE(@prior,'$.ownership_id'),'')<>@ownership_id
       OR COALESCE(JSON_VALUE(@prior,'$.state'),'') IN ('deleting','deleted')))
    THROW 51072, 'Connector revision/ownership changed or the owned connector is retired', 1;
SELECT @desired=payload FROM {records} WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND record_kind='connector_desired' AND full_key=@connector_id;
IF @prior IS NOT NULL AND @desired IS NULL
    THROW 51072, 'Existing connector has no protected desired-publication provenance', 1;
{proposal_binding_sql(names)}
IF EXISTS (SELECT 1 FROM OPENJSON(@desired_sources) AS s
    WHERE JSON_QUERY(s.value,'$.target') IS NULL
       OR COALESCE(JSON_VALUE(s.value,'$.target.tenant_id'),'')<>@tenant_id
       OR COALESCE(JSON_VALUE(s.value,'$.target.epoch'),'')<>@epoch
       OR COALESCE(JSON_VALUE(s.value,'$.target.workload'),'') NOT IN ('fabric_pipeline','powerbi')
       OR NOT ({canonical_guid("JSON_VALUE(s.value,'$.target.workspace_id')")})
       OR NOT ({canonical_guid("JSON_VALUE(s.value,'$.target.item_id')")})
       OR (NULLIF(JSON_VALUE(s.value,'$.source_id'),'') IS NULL AND JSON_VALUE(s.value,'$.proposal_id') IS NULL)
       OR DATALENGTH(JSON_VALUE(s.value,'$.source_id'))>512
       OR JSON_QUERY(s.value,'$.event_types') IS NULL
       OR (SELECT COUNT(*) FROM OPENJSON(s.value,'$.event_types')) NOT BETWEEN 1 AND 20
       OR EXISTS (SELECT 1 FROM OPENJSON(s.value) WHERE [key] NOT IN ('source_id','proposal_id','node_name','target','event_types','event_source'))
       OR EXISTS (SELECT 1 FROM OPENJSON(s.value,'$.event_types') AS e
           WHERE e.type<>1 OR e.value COLLATE Latin1_General_100_BIN2 NOT IN
             ('Microsoft.Fabric.JobEvents.ItemJobCreated','Microsoft.Fabric.JobEvents.ItemJobStatusChanged',
              'Microsoft.Fabric.JobEvents.ItemJobSucceeded','Microsoft.Fabric.JobEvents.ItemJobFailed')))
    THROW 51073, 'Connector sources must be explicit typed current-tenant subscriptions', 1;
IF EXISTS (SELECT JSON_VALUE(value,'$.source_id') COLLATE Latin1_General_100_BIN2
    FROM OPENJSON(@sources) GROUP BY JSON_VALUE(value,'$.source_id') COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1)
   OR EXISTS (SELECT JSON_VALUE(value,'$.target.workspace_id'),JSON_VALUE(value,'$.target.item_id')
       FROM OPENJSON(@desired_sources) GROUP BY JSON_VALUE(value,'$.target.workspace_id'),JSON_VALUE(value,'$.target.item_id')
       HAVING COUNT(*)>1)
    THROW 51073, 'Connector source IDs and subscribed items must be unique', 1;
DECLARE @source_target nvarchar(max),@approved_target nvarchar(max),@source_capability nvarchar(max),
    @source_key nvarchar(1024);
DECLARE approved_sources CURSOR LOCAL FAST_FORWARD FOR SELECT JSON_QUERY(value,'$.target') FROM OPENJSON(@desired_sources);
OPEN approved_sources; FETCH NEXT FROM approved_sources INTO @source_target;
WHILE @@FETCH_STATUS=0
BEGIN
    SET @source_key=N'monitor:v1:'+@epoch+N':'+@tenant_id+N':'+JSON_VALUE(@source_target,'$.workload')
        +N':'+JSON_VALUE(@source_target,'$.workspace_id')+N':'+JSON_VALUE(@source_target,'$.item_id');
    SET @approved_target=NULL; SET @source_capability=NULL;
    SELECT @approved_target=payload FROM {records} WITH (UPDLOCK,HOLDLOCK)
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='target' AND full_key=@source_key;
    SELECT @source_capability=payload FROM {records} WITH (UPDLOCK,HOLDLOCK)
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='target_capability' AND full_key=@source_key;
    IF NOT ({source_authorized_predicate()})
        THROW 51072, 'Desired source is outside current approved observation/event capability', 1;
    FETCH NEXT FROM approved_sources INTO @source_target;
END;
CLOSE approved_sources; DEALLOCATE approved_sources;
IF @prior IS NOT NULL AND EXISTS (
    SELECT 1 FROM OPENJSON(@prior,'$.sources') AS old_source JOIN OPENJSON(@sources) AS new_source
      ON JSON_VALUE(old_source.value,'$.target.workspace_id')=JSON_VALUE(new_source.value,'$.target.workspace_id')
     AND JSON_VALUE(old_source.value,'$.target.item_id')=JSON_VALUE(new_source.value,'$.target.item_id')
    WHERE NOT {exact_text_equal("JSON_VALUE(old_source.value,'$.source_id')", "JSON_VALUE(new_source.value,'$.source_id')")})
    THROW 51072, 'Established source component bindings cannot be rebound', 1;
IF @prior IS NOT NULL AND EXISTS (
    SELECT 1 FROM OPENJSON(@prior,'$.sources') AS old_source JOIN OPENJSON(@sources) AS new_source
      ON {exact_text_equal("JSON_VALUE(old_source.value,'$.source_id')", "JSON_VALUE(new_source.value,'$.source_id')")}
    WHERE NOT {exact_text_equal("JSON_QUERY(old_source.value,'$.target')", "JSON_QUERY(new_source.value,'$.target')")})
    THROW 51072, 'An established source component cannot move to another target', 1;
DECLARE @graph nvarchar(max)=JSON_QUERY(@definition,'$.parts."eventstream.json"');
IF EXISTS (SELECT 1 FROM OPENJSON(@proposals) AS proposal
    WHERE NOT {source_is_pending_removal_sql('proposal.value', '@removals')} AND NOT EXISTS (
    SELECT 1 FROM OPENJSON(@graph,'$.sources') AS node
    WHERE {exact_text_equal("JSON_VALUE(proposal.value,'$.node_name')", "JSON_VALUE(node.value,'$.name')")}
      AND JSON_VALUE(proposal.value,'$.target.workspace_id')=JSON_VALUE(node.value,'$.properties.workspaceId')
      AND JSON_VALUE(proposal.value,'$.target.item_id')=JSON_VALUE(node.value,'$.properties.itemId')))
    THROW 51072, 'A logical proposal must bind its exact per-item definition node name', 1;
IF @prior IS NOT NULL AND EXISTS (
    SELECT 1 FROM OPENJSON(@prior,'$.source_proposals') AS old_proposal
    JOIN OPENJSON(@proposals) AS new_proposal
      ON JSON_VALUE(old_proposal.value,'$.proposal_id')=JSON_VALUE(new_proposal.value,'$.proposal_id')
    WHERE {names.object('json_equal')}(old_proposal.value,new_proposal.value)<>1)
    THROW 51072, 'An existing logical proposal identity cannot be rewritten or retargeted', 1;
IF @graph IS NULL OR JSON_QUERY(@graph,'$.sources') IS NULL
   OR JSON_QUERY(@graph,'$.operators') IS NULL OR (SELECT COUNT(*) FROM OPENJSON(@graph,'$.operators'))<>0
   OR (SELECT COUNT(*) FROM OPENJSON(@graph,'$.streams'))<>1
   OR (SELECT COUNT(*) FROM OPENJSON(@graph,'$.destinations'))<>1
   OR COALESCE(JSON_VALUE(@graph,'$.streams[0].type'),'')<>'DefaultStream'
   OR COALESCE(JSON_VALUE(@graph,'$.destinations[0].type'),'')<>'CustomEndpoint'
   OR (SELECT COUNT(*) FROM OPENJSON(@graph,'$.destinations[0].inputNodes'))<>1
   OR NOT {exact_text_equal("JSON_VALUE(@graph,'$.destinations[0].inputNodes[0].name')", "JSON_VALUE(@graph,'$.streams[0].name')")}
   OR (SELECT COUNT(*) FROM OPENJSON(@graph,'$.sources'))<>(SELECT COUNT(*) FROM OPENJSON(@desired_sources))
   OR EXISTS (SELECT 1 FROM OPENJSON(@graph,'$.sources') AS n
       WHERE COALESCE(JSON_VALUE(n.value,'$.type'),'')<>'FabricJobEvents'
          OR COALESCE(JSON_VALUE(n.value,'$.properties.eventScope'),'')<>'Item'
          OR NOT EXISTS (SELECT 1 FROM OPENJSON(@desired_sources) AS s
              WHERE JSON_VALUE(s.value,'$.target.workspace_id')=JSON_VALUE(n.value,'$.properties.workspaceId')
                AND JSON_VALUE(s.value,'$.target.item_id')=JSON_VALUE(n.value,'$.properties.itemId')
                AND NOT EXISTS (
                    SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(s.value,'$.event_types')
                    EXCEPT SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(n.value,'$.properties.includedEventTypes'))
                AND NOT EXISTS (
                    SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(n.value,'$.properties.includedEventTypes')
                    EXCEPT SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(s.value,'$.event_types'))))
    THROW 51072, 'Reviewed definition and explicit approved per-item source set differ', 1;
IF EXISTS (SELECT JSON_VALUE(value,'$.properties.workspaceId'),JSON_VALUE(value,'$.properties.itemId')
    FROM OPENJSON(@graph,'$.sources')
    GROUP BY JSON_VALUE(value,'$.properties.workspaceId'),JSON_VALUE(value,'$.properties.itemId')
    HAVING COUNT(*)>1)
   OR EXISTS (SELECT JSON_VALUE(value,'$.name') COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@graph,'$.sources')
       GROUP BY JSON_VALUE(value,'$.name') COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1)
   OR (SELECT COUNT(*) FROM OPENJSON(@graph,'$.streams[0].inputNodes'))<>(SELECT COUNT(*) FROM OPENJSON(@desired_sources))
   OR EXISTS (SELECT 1 FROM OPENJSON(@graph,'$.sources') AS source_node
       WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@graph,'$.streams[0].inputNodes') AS stream_input
           WHERE {exact_text_equal("JSON_VALUE(source_node.value,'$.name')", "JSON_VALUE(stream_input.value,'$.name')")}))
    THROW 51072, 'Reviewed source routing must be one-to-one through the owned default stream', 1;
IF @prior IS NOT NULL AND (
    NOT {exact_text_equal(prior_stream, "JSON_VALUE(@graph,'$.streams[0].name')")}
    OR NOT {exact_text_equal(prior_destination, "JSON_VALUE(@graph,'$.destinations[0].name')")})
    THROW 51072, 'Established stream/destination routing identities cannot be replaced', 1;
IF @prior IS NOT NULL AND EXISTS (
    SELECT 1 FROM {prior_sources} AS old_node JOIN OPENJSON(@graph,'$.sources') AS new_node
      ON JSON_VALUE(old_node.value,'$.properties.workspaceId')=JSON_VALUE(new_node.value,'$.properties.workspaceId')
     AND JSON_VALUE(old_node.value,'$.properties.itemId')=JSON_VALUE(new_node.value,'$.properties.itemId')
    WHERE NOT {exact_text_equal("JSON_VALUE(old_node.value,'$.name')", "JSON_VALUE(new_node.value,'$.name')")})
    THROW 51072, 'Established per-item source node names cannot be replaced', 1;
IF @prior IS NOT NULL AND EXISTS (
    SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.component_ids') AS old_binding
    WHERE (old_binding.[key] LIKE 'destinations/%' OR old_binding.[key] LIKE 'streams/%'
        OR EXISTS (SELECT 1 FROM OPENJSON(@graph,'$.sources') AS retained_node
            WHERE {exact_text_equal('old_binding.[key]', "N'sources/'+JSON_VALUE(retained_node.value,'$.name')")}))
      AND NOT EXISTS (SELECT 1 FROM OPENJSON(@definition,'$.component_ids') AS new_binding
          WHERE {exact_text_equal('old_binding.[key]', 'new_binding.[key]')}
            AND {exact_text_equal('old_binding.value', 'new_binding.value')}))
    THROW 51072, 'Established physical component IDs must remain bound to the same owned nodes', 1;
IF @prior IS NOT NULL AND TRY_CONVERT(bigint,JSON_VALUE(@prior,'$.policy_revision'))=@current_revision
   AND {exact_text_equal("JSON_VALUE(@prior,'$.name')", '@name')}
   AND {payload_hash("JSON_QUERY(@prior,'$.sources')")}={payload_hash('@sources')}
   AND {payload_hash("JSON_QUERY(@prior,'$.desired_definition')")}={payload_hash('@definition')}
   AND {names.object('json_equal')}(COALESCE(JSON_QUERY(@prior,'$.source_proposals'),N'[]'),@proposals)=1
   AND {names.object('json_equal')}(COALESCE(JSON_QUERY(@prior,'$.source_removals'),N'[]'),@removals)=1
    SET @desired_changed=0;
IF @prior IS NULL
    SET @next=(SELECT @tenant_id AS tenant_id,@epoch AS epoch,@connector_id AS connector_id,
        @ownership_id AS ownership_id,1 AS revision,@current_revision AS policy_revision,@name AS name,
        JSON_QUERY(@sources) AS sources,JSON_QUERY(@proposals) AS source_proposals,
        JSON_QUERY(@removals) AS source_removals,JSON_QUERY(@definition) AS desired_definition,
        'planned' AS state,CONVERT(nvarchar(40),@now,127)+N'Z' AS updated_at,JSON_QUERY(N'[]') AS gaps
        FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
ELSE
BEGIN
    SET @next={desired_update_expression()};
    SET @next=JSON_MODIFY(@next,'$.source_proposals',JSON_QUERY(@proposals));
    SET @next=JSON_MODIFY(@next,'$.source_removals',JSON_QUERY(@removals));
    IF @desired_changed=1
        SET @next={invalidate_readiness_expression()};
END;
IF @readiness_id IS NOT NULL
BEGIN
    IF @prior IS NULL OR @desired_changed=1
       OR (SELECT COUNT(*) FROM OPENJSON(@proposals))<>0
       OR (SELECT COUNT(*) FROM OPENJSON(@removals))<>0
       OR @readiness_id<>JSON_VALUE(@stored_work,'$.reconcile_request_id')
        THROW 51072, 'Readiness must reconcile the exact existing desired connector observation', 1;
    DECLARE @observation nvarchar(max);
    SELECT @observation=JSON_QUERY(payload,'$.result.observation') FROM {receipts}
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND operation='worker.observe_connector' AND request_id=@readiness_id
      AND JSON_VALUE(payload,'$.result.connector_id')=@connector_id
      AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.result.connector.revision'))=@expected_connector_revision;
    IF @observation IS NULL OR COALESCE(JSON_VALUE(@observation,'$.state'),'')<>'ready'
       OR COALESCE(JSON_VALUE(@observation,'$.ownership_id'),'')<>@ownership_id
       OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@observation,'$.policy_revision')),-1)<>@current_revision
       OR {payload_hash("JSON_QUERY(@observation,'$.observed_definition')")}<>{payload_hash('@definition')}
       OR JSON_QUERY(@observation,'$.observed_definition') IS NULL
       OR (SELECT COUNT(*) FROM OPENJSON(@sources))=0
       OR NOT {exact_text_equal("JSON_VALUE(@observation,'$.workspace_id')", "JSON_VALUE(@prior,'$.workspace_id')")}
       OR NOT {exact_text_equal("JSON_VALUE(@observation,'$.eventstream_id')", "JSON_VALUE(@prior,'$.eventstream_id')")}
       OR NOT {exact_text_equal("JSON_VALUE(@observation,'$.destination_id')", "JSON_VALUE(@prior,'$.destination_id')")}
       OR JSON_QUERY(@observation,'$.endpoint') IS NULL
       OR {payload_hash("JSON_QUERY(@observation,'$.endpoint')")}<>{payload_hash("JSON_QUERY(@prior,'$.endpoint')")}
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.identity_verified_at')) IS NULL
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.delivery_verified_at')) IS NULL
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.identity_verified_at'))>TODATETIMEOFFSET(@now,'+00:00')
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.delivery_verified_at'))>TODATETIMEOFFSET(@now,'+00:00')
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.identity_verified_at'))
           <TRY_CONVERT(datetimeoffset,JSON_VALUE(@desired,'$.published_at'))
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@observation,'$.delivery_verified_at'))
           <TRY_CONVERT(datetimeoffset,JSON_VALUE(@desired,'$.published_at'))
        THROW 51072, 'Readiness lacks current matched ownership/topology/identity/delivery evidence', 1;
    SET @next=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(@next,'$.state','ready'),
        '$.identity_verified_at',JSON_VALUE(@observation,'$.identity_verified_at')),
        '$.delivery_verified_at',JSON_VALUE(@observation,'$.delivery_verified_at'));
END;
SET @next=JSON_MODIFY(JSON_MODIFY(@next,'$.revision',@expected_connector_revision+1),
    '$.updated_at',CONVERT(nvarchar(40),@now,127)+N'Z');
{retirement_records_sql(names)}
IF @prior IS NULL
BEGIN
    {record_insert(names, 'connector', '@connector_id', '@next', status="N'planned'")}
END
ELSE
BEGIN
    {publish_update_sql(names)}
    IF @@ROWCOUNT<>1 THROW 51072, 'Desired connector publication lost its revision/ownership CAS', 1;
END;
IF @desired_changed=1
BEGIN
    DECLARE @desired_payload nvarchar(max)=(SELECT @connector_id AS connector_id,@ownership_id AS ownership_id,
        @publication_id AS publication_id,@current_revision AS policy_revision,
        {payload_hash('@sources')} AS sources_hash,{payload_hash('@definition')} AS definition_hash,
        CONVERT(nvarchar(40),@now,127)+N'Z' AS published_at FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
    IF @desired IS NULL
    BEGIN
        {record_insert(names, 'connector_desired', '@connector_id', '@desired_payload')}
    END
    ELSE UPDATE {records} SET revision=revision+1,payload=@desired_payload
        WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector_desired' AND full_key=@connector_id;
END;
SET @affected=1;
SET @result=(SELECT @connector_id AS connector_id,JSON_QUERY(@next) AS connector,
    JSON_VALUE(@next,'$.state') AS state,@desired_changed AS desired_changed
    ,JSON_QUERY(@removals) AS pending_removals,JSON_QUERY(@retired_json) AS retired_sources,
    @binding_receipt_id AS observation_receipt_id
    FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return {contract.operation: procedure(names, contract, body, replay=True)}
