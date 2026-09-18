"""Logical desired source proposals and receipt-derived physical bindings."""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import canonical_guid, exact_text_equal, payload_hash
from triage.monitoring.sql_kernel_contracts import SqlNames
from triage.monitoring.sql_kernel_removals import (
    confirm_removals_sql,
    desired_source_projection_sql,
    prepare_removals_sql,
)


def binding_receipt_sql(names: SqlNames) -> str:
    return f"""SELECT payload,fingerprint FROM {names.table('monitoring_receipts')}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND operation='worker.observe_connector'
  AND request_id=@binding_receipt_id AND JSON_VALUE(payload,'$.result.connector_id')=@connector_id
  AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.result.connector.revision'))=@expected_connector_revision"""


def binding_observation_matches_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")
    return f"""@binding_observation IS NOT NULL
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.tenant_id')", '@tenant_id')}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.epoch')", '@epoch')}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.connector_id')", '@connector_id')}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.ownership_id')", '@ownership_id')}
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding_observation,'$.policy_revision')),-1)=@current_revision
AND COALESCE(JSON_VALUE(@binding_observation,'$.state'),'') IN ('provisioning','ready','degraded')
AND {exact_text_equal("JSON_VALUE(@binding_receipt_payload,'$.result.observed_definition_hash')",
                      "CONVERT(nvarchar(64)," + payload_hash("JSON_QUERY(@binding_observation,'$.observed_definition')") + ")")}
AND {equal}(JSON_QUERY(@binding_observation,'$.desired_definition'),@definition)=1
AND {equal}(JSON_QUERY(@binding_observation,'$.sources'),JSON_QUERY(@prior,'$.sources'))=1
AND {equal}(COALESCE(JSON_QUERY(@binding_observation,'$.source_proposals'),N'[]'),@proposals)=1
AND {equal}(COALESCE(JSON_QUERY(@binding_observation,'$.source_removals'),N'[]'),
    COALESCE(JSON_QUERY(@prior,'$.source_removals'),N'[]'))=1
AND {equal}(JSON_QUERY(@binding_observation,'$.observed_definition.parts'),JSON_QUERY(@definition,'$.parts'))=1"""


def complete_component_map_sql() -> str:
    """A full normalized topology, not a partial/omitted map, establishes absence."""
    map_json = "OPENJSON(@binding_observation,'$.observed_definition.component_ids')"
    graph_json = """OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json"')"""
    kinds = "'sources','streams','destinations'"
    nodes = (
        "OPENJSON(CASE WHEN collection.[key] COLLATE Latin1_General_100_BIN2 "
        f"IN ({kinds}) AND collection.type=4 THEN collection.value ELSE N'[]' END)"
    )
    return f"""EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition')
    WHERE [key] COLLATE Latin1_General_100_BIN2='component_ids' AND type=5)
AND (SELECT COUNT(*) FROM {graph_json} AS collection
    WHERE collection.[key] COLLATE Latin1_General_100_BIN2 IN ({kinds}) AND collection.type=4)=3
AND NOT EXISTS (SELECT 1 FROM {map_json} AS component WHERE component.type<>1 OR NOT ({canonical_guid('component.value')}))
AND NOT EXISTS (SELECT component.[key] COLLATE Latin1_General_100_BIN2 FROM {map_json} AS component
    GROUP BY component.[key] COLLATE Latin1_General_100_BIN2,DATALENGTH(component.[key]) HAVING COUNT(*)>1)
AND NOT EXISTS (SELECT component.value COLLATE Latin1_General_100_BIN2 FROM {map_json} AS component
    GROUP BY component.value COLLATE Latin1_General_100_BIN2,DATALENGTH(component.value) HAVING COUNT(*)>1)
AND (SELECT COUNT(*) FROM {map_json})=(SELECT COUNT(*) FROM {graph_json} AS collection
    CROSS APPLY {nodes} AS node
    WHERE collection.[key] COLLATE Latin1_General_100_BIN2 IN ({kinds}))
AND NOT EXISTS (SELECT 1 FROM {graph_json} AS collection CROSS APPLY {nodes} AS node
    WHERE collection.[key] COLLATE Latin1_General_100_BIN2 IN ({kinds})
      AND (node.type<>5 OR NOT EXISTS (SELECT 1 FROM {map_json} AS component
          WHERE {exact_text_equal('component.[key]', "collection.[key]+N'/'+JSON_VALUE(node.value,'$.name')")}
            AND (JSON_VALUE(node.value,'$.id') IS NULL
                OR {exact_text_equal("JSON_VALUE(node.value,'$.id')", 'component.value')}))))"""


def proposal_binding_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")
    return f"""DECLARE @proposals nvarchar(max)=COALESCE(JSON_QUERY(@plan,'$.source_proposals'),N'[]'),
    @binding_receipt_id nvarchar(256)=JSON_VALUE(@plan,'$.observation_receipt_id'),
    @desired_sources nvarchar(max),@binding_observation nvarchar(max),
    @binding_receipt_payload nvarchar(max),@binding_receipt_fingerprint char(64);
IF LEFT(LTRIM(@proposals),1)<>'[' OR (SELECT COUNT(*) FROM OPENJSON(@proposals))>1000
   OR EXISTS (SELECT 1 FROM OPENJSON(@plan) WHERE [key]='source_proposals' AND type<>4)
    THROW 51073, 'Desired source proposals must be a bounded typed array', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@proposals) AS p WHERE
    EXISTS (SELECT 1 FROM OPENJSON(p.value) WHERE [key] NOT IN
        ('proposal_id','node_name','source_id','target','event_types','event_source'))
    OR NOT ({canonical_guid("JSON_VALUE(p.value,'$.proposal_id')")})
    OR NULLIF(JSON_VALUE(p.value,'$.node_name'),'') IS NULL
    OR DATALENGTH(JSON_VALUE(p.value,'$.node_name'))>512
    OR JSON_VALUE(p.value,'$.node_name') COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_.]%'
    OR NOT EXISTS (SELECT 1 FROM OPENJSON(p.value) WHERE [key]='source_id' AND type=0)
    OR JSON_VALUE(p.value,'$.source_id') IS NOT NULL)
    THROW 51073, 'A logical proposal has a node name and null physical source_id, never a fabricated ID', 1;
IF EXISTS (SELECT JSON_VALUE(value,'$.proposal_id') FROM OPENJSON(@proposals)
    GROUP BY JSON_VALUE(value,'$.proposal_id') HAVING COUNT(*)>1)
   OR EXISTS (SELECT JSON_VALUE(value,'$.node_name') COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@proposals)
    GROUP BY JSON_VALUE(value,'$.node_name') COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1)
    THROW 51073, 'Logical proposal identities and node names must be unique', 1;
{prepare_removals_sql(names)}
IF @binding_receipt_id IS NULL AND EXISTS (
    SELECT 1 FROM OPENJSON(@proposals) AS p
    WHERE EXISTS (SELECT 1 FROM OPENJSON(@definition,'$.component_ids') AS supplied_id
        WHERE {exact_text_equal('supplied_id.[key]', "N'sources/'+JSON_VALUE(p.value,'$.node_name')")})
      OR EXISTS (SELECT 1 FROM OPENJSON(@definition,'$.parts."eventstream.json".sources') AS node
          WHERE {exact_text_equal("JSON_VALUE(node.value,'$.name')", "JSON_VALUE(p.value,'$.node_name')")}
            AND EXISTS (SELECT 1 FROM OPENJSON(node.value) WHERE [key]='id' AND type<>0))
      OR (NOT EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.source_proposals') AS old_proposal
              WHERE JSON_VALUE(old_proposal.value,'$.proposal_id')=JSON_VALUE(p.value,'$.proposal_id'))
          AND EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.observed_definition.component_ids') AS observed_id
              WHERE {exact_text_equal('observed_id.[key]', "N'sources/'+JSON_VALUE(p.value,'$.node_name')")})))
    THROW 51072, 'An unresolved proposal cannot acquire a supplied ID or adopt an already observed unowned node without its original receipt', 1;
IF @binding_receipt_id IS NOT NULL
BEGIN
    IF @prior IS NULL OR @readiness_id IS NOT NULL
       OR @binding_receipt_id<>JSON_VALUE(@stored_work,'$.reconcile_request_id')
       OR {equal}(@sources,JSON_QUERY(@prior,'$.sources'))<>1
       OR {equal}(@proposals,COALESCE(JSON_QUERY(@prior,'$.source_proposals'),N'[]'))<>1
       OR {equal}(@definition,JSON_QUERY(@prior,'$.desired_definition'))<>1
        THROW 51072, 'Observed binding may resolve only the exact current approved proposals', 1;
    SELECT @binding_receipt_payload=payload,@binding_receipt_fingerprint=fingerprint
    FROM ({binding_receipt_sql(names)}) AS original_receipt;
    SET @binding_observation=JSON_QUERY(@binding_receipt_payload,'$.result.observation');
    IF NOT ({binding_observation_matches_sql(names)})
        THROW 51072, 'Physical binding needs the original owned observation of the exact desired definition', 1;
    IF NOT ({complete_component_map_sql()})
        THROW 51072, 'Physical reconciliation requires a complete exact node-to-ID observation, not a partial map', 1;
    DECLARE @bound TABLE (proposal_id nvarchar(36),node_name nvarchar(256),source_id nvarchar(36),source_json nvarchar(max));
    INSERT INTO @bound SELECT JSON_VALUE(p.value,'$.proposal_id'),JSON_VALUE(p.value,'$.node_name'),ids.value,
        JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(p.value,'$.source_id',ids.value),'$.proposal_id',NULL),'$.node_name',NULL)
    FROM OPENJSON(@proposals) AS p
    JOIN OPENJSON(@binding_observation,'$.observed_definition.component_ids') AS ids
      ON {exact_text_equal('ids.[key]', "N'sources/'+JSON_VALUE(p.value,'$.node_name')")}
    WHERE ids.type=1 AND {canonical_guid('ids.value')}
      AND NOT EXISTS (SELECT 1 FROM @pending_removals AS withdrawal
          WHERE withdrawal.proposal_id=JSON_VALUE(p.value,'$.proposal_id'));
    IF EXISTS (SELECT source_id FROM @bound GROUP BY source_id HAVING COUNT(*)>1)
       OR EXISTS (SELECT 1 FROM @bound AS b JOIN OPENJSON(@sources) AS s
           ON JSON_VALUE(s.value,'$.source_id')=b.source_id)
        THROW 51072, 'One returned physical source cannot bind multiple proposals or replace an existing source', 1;
    SET @sources=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),value),N',')
        WITHIN GROUP (ORDER BY ownership_order,position) FROM (
        SELECT value,0 AS ownership_order,CONVERT(int,[key]) AS position FROM OPENJSON(@sources)
        UNION ALL SELECT b.source_json,1,CONVERT(int,p.[key]) FROM @bound AS b
        JOIN OPENJSON(@proposals) AS p ON JSON_VALUE(p.value,'$.proposal_id')=b.proposal_id
    ) AS combined),N'')+N']';
    SET @proposals=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),p.value),N',')
        WITHIN GROUP (ORDER BY CONVERT(int,p.[key]))
        FROM OPENJSON(@proposals) AS p WHERE NOT EXISTS (SELECT 1 FROM @bound AS b
            WHERE b.proposal_id=JSON_VALUE(p.value,'$.proposal_id'))),N'')+N']';
    SET @definition=JSON_QUERY(@binding_observation,'$.observed_definition');
    {confirm_removals_sql(names)}
END;
IF @binding_receipt_id IS NULL AND EXISTS (SELECT 1 FROM OPENJSON(@sources) AS s
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.sources') AS old_source
        WHERE {exact_text_equal("JSON_VALUE(s.value,'$.source_id')", "JSON_VALUE(old_source.value,'$.source_id')")}
          AND {equal}(JSON_QUERY(s.value,'$.target'),JSON_QUERY(old_source.value,'$.target'))=1))
    THROW 51072, 'New physical sources require receipt-bound proposal resolution, not caller IDs', 1;
{desired_source_projection_sql()}"""
