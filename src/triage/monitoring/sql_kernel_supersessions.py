"""Receipt-bound recovery of an owned source whose removal was never dispatched."""

from __future__ import annotations

from triage.monitoring.models import CONNECTOR_PENDING_GAP_CODES, SUPERSESSION_EVIDENCE_TTL_SECONDS
from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    exact_text_equal,
    key_hash,
    literals,
    payload_hash,
)
from triage.monitoring.sql_kernel_contracts import SqlNames


def inspection_invalid_sql(inspection: str, observation: str) -> str:
    return f"""{inspection} IS NULL
OR (SELECT COUNT(*) FROM OPENJSON({inspection}))<>4
OR EXISTS (SELECT 1 FROM (
    SELECT N'observed_at' AS field_name,1 AS field_type UNION ALL SELECT N'definition_hash',1
    UNION ALL SELECT N'read_only',3 UNION ALL SELECT N'component_states',5
) AS required_field WHERE NOT EXISTS (SELECT 1 FROM OPENJSON({inspection}) AS supplied
    WHERE {exact_text_equal('supplied.[key]', 'required_field.field_name')} AND supplied.type=required_field.field_type))
OR COALESCE(JSON_VALUE({inspection},'$.read_only'),'')<>'true'
OR NOT {exact_text_equal(f"JSON_VALUE({inspection},'$.definition_hash')",
                        "CONVERT(nvarchar(64)," + payload_hash(f"JSON_QUERY({observation},'$.observed_definition')") + ")")}
OR TRY_CONVERT(datetimeoffset,JSON_VALUE({inspection},'$.observed_at')) IS NULL
OR TRY_CONVERT(datetimeoffset,JSON_VALUE({inspection},'$.observed_at'))>TODATETIMEOFFSET(@now,'+00:00')
OR TRY_CONVERT(datetimeoffset,JSON_VALUE({inspection},'$.observed_at'))<TODATETIMEOFFSET(DATEADD(second,-300,@now),'+00:00')
OR (SELECT COUNT(*) FROM OPENJSON({inspection},'$.component_states')) NOT BETWEEN 3 AND 1002
OR EXISTS (SELECT 1 FROM OPENJSON({inspection},'$.component_states') AS component WHERE
    component.type<>1 OR NOT ({canonical_guid('component.[key]')})
    OR NOT {exact_text_equal('component.value', "N'Running'")})
OR EXISTS (SELECT [key] COLLATE Latin1_General_100_BIN2 FROM OPENJSON({inspection},'$.component_states')
    GROUP BY [key] COLLATE Latin1_General_100_BIN2,DATALENGTH([key]) HAVING COUNT(*)>1)
OR (SELECT COUNT(*) FROM OPENJSON({inspection},'$.component_states'))
    <>(SELECT COUNT(*) FROM OPENJSON({observation},'$.observed_definition.component_ids'))
OR EXISTS (SELECT 1 FROM OPENJSON({inspection},'$.component_states') AS component WHERE NOT EXISTS (
    SELECT 1 FROM OPENJSON({observation},'$.observed_definition.component_ids') AS observed_component
    WHERE observed_component.type=1
      AND {exact_text_equal('component.[key]', 'observed_component.value')}))"""


def supersession_selector_invalid_sql() -> str:
    return f"""EXISTS (SELECT 1 FROM OPENJSON(@supersession_intents) AS requested WHERE
    requested.type<>5 OR (SELECT COUNT(*) FROM OPENJSON(requested.value))<>2
    OR EXISTS (SELECT 1 FROM (SELECT N'removal_id' AS field_name UNION ALL SELECT N'source_id') AS required_field
        WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(requested.value) AS supplied_field
            WHERE {exact_text_equal('supplied_field.[key]', 'required_field.field_name')} AND supplied_field.type=1))
    OR EXISTS (SELECT [key] COLLATE Latin1_General_100_BIN2 FROM OPENJSON(requested.value)
        GROUP BY [key] COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1)
    OR NOT ({canonical_guid("JSON_VALUE(requested.value,'$.removal_id')")})
    OR DATALENGTH(JSON_VALUE(requested.value,'$.source_id')) NOT BETWEEN 1 AND 512)"""


def prepare_supersession_selection_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")
    return f"""DECLARE @supersession_intents nvarchar(max)=COALESCE(JSON_QUERY(@plan,'$.source_removal_supersessions'),N'[]'),
    @superseded_json nvarchar(max)=N'[]';
DECLARE @supersessions TABLE (
    removal_id nvarchar(36) PRIMARY KEY,source_id nvarchar(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
    node_name nvarchar(256) NOT NULL,binding_json nvarchar(max) NOT NULL,payload nvarchar(max) NOT NULL);
DECLARE @supersession_queued_work TABLE (work_id nvarchar(36) PRIMARY KEY,revision bigint NOT NULL);
IF LEFT(LTRIM(@supersession_intents),1)<>'[' OR (SELECT COUNT(*) FROM OPENJSON(@supersession_intents))>1000
   OR EXISTS (SELECT 1 FROM OPENJSON(@plan) WHERE [key]='source_removal_supersessions' AND type<>4)
   OR {supersession_selector_invalid_sql()}
    THROW 51073, 'Source removal supersessions require bounded exact physical removal selectors', 1;
IF EXISTS (SELECT JSON_VALUE(value,'$.removal_id') FROM OPENJSON(@supersession_intents)
    GROUP BY JSON_VALUE(value,'$.removal_id') HAVING COUNT(*)>1)
   OR EXISTS (SELECT JSON_VALUE(value,'$.source_id') COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@supersession_intents)
       GROUP BY JSON_VALUE(value,'$.source_id') COLLATE Latin1_General_100_BIN2,
           DATALENGTH(JSON_VALUE(value,'$.source_id')) HAVING COUNT(*)>1)
    THROW 51073, 'Superseded removal and physical source identities must be unique', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@supersession_intents))
BEGIN
    IF @prior IS NULL OR @desired IS NULL OR @binding_receipt_id IS NULL OR @readiness_id IS NOT NULL
       OR COALESCE(JSON_VALUE(@stored_work,'$.reconcile_producer'),'')<>'worker'
       OR NOT {exact_text_equal('@binding_receipt_id', "JSON_VALUE(@stored_work,'$.reconcile_request_id')")}
       OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@prior,'$.policy_revision')),-1)<>@current_revision
       OR @publication_id=JSON_VALUE(@desired,'$.publication_id')
       OR {equal}(@sources,JSON_QUERY(@prior,'$.sources'))<>1
       OR {equal}(@proposals,COALESCE(JSON_QUERY(@prior,'$.source_proposals'),N'[]'))<>1
       OR NOT {exact_text_equal('@name', "JSON_VALUE(@prior,'$.name')")}
        THROW 51072, 'Supersession requires an original current observation and unchanged retained ownership', 1;
    IF EXISTS (SELECT 1 FROM OPENJSON(@supersession_intents) AS selected
        JOIN OPENJSON(@plan,'$.source_removals') AS pending
          ON JSON_VALUE(pending.value,'$.removal_id')=JSON_VALUE(selected.value,'$.removal_id')
          OR {exact_text_equal("JSON_VALUE(pending.value,'$.source_id')", "JSON_VALUE(selected.value,'$.source_id')")})
        THROW 51072, 'A physical removal cannot remain requested and be superseded together', 1;
    INSERT INTO @supersessions
    SELECT JSON_VALUE(selected.value,'$.removal_id'),JSON_VALUE(selected.value,'$.source_id'),
        JSON_VALUE(old_removal.value,'$.node_name'),owned.value,old_removal.value
    FROM OPENJSON(@supersession_intents) AS selected
    JOIN OPENJSON(@prior,'$.source_removals') AS old_removal
      ON JSON_VALUE(old_removal.value,'$.removal_id')=JSON_VALUE(selected.value,'$.removal_id')
     AND {exact_text_equal("JSON_VALUE(old_removal.value,'$.source_id')", "JSON_VALUE(selected.value,'$.source_id')")}
    JOIN OPENJSON(@prior,'$.sources') AS owned
      ON {exact_text_equal("JSON_VALUE(owned.value,'$.source_id')", "JSON_VALUE(selected.value,'$.source_id')")}
    WHERE JSON_VALUE(old_removal.value,'$.proposal_id') IS NULL
      AND JSON_VALUE(old_removal.value,'$.state')='pending_remote_absence'
      AND TRY_CONVERT(bigint,JSON_VALUE(old_removal.value,'$.policy_revision'))=@current_revision
      AND {equal}(JSON_QUERY(old_removal.value,'$.target'),JSON_QUERY(owned.value,'$.target'))=1
      AND {exact_text_equal("JSON_VALUE(old_removal.value,'$.binding_hash')", "CONVERT(nvarchar(64)," + payload_hash('owned.value') + ")")};
    IF (SELECT COUNT(*) FROM @supersessions)<>(SELECT COUNT(*) FROM OPENJSON(@supersession_intents))
        THROW 51072, 'Supersession must select exact current owned pending physical removals', 1;
END;"""


def supersession_original_removal_invalid_sql(names: SqlNames) -> str:
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    equal = names.object("json_equal")
    return f"""EXISTS (SELECT 1 FROM @supersessions AS selected WHERE NOT EXISTS (
    SELECT 1 FROM {receipts} AS original
    JOIN {records} AS publication ON publication.tenant_id=original.tenant_id AND publication.epoch=original.epoch
      AND publication.record_kind='connector_publication'
      AND publication.full_key=JSON_VALUE(selected.payload,'$.publication_id')
      AND publication.key_hash={key_hash("JSON_VALUE(selected.payload,'$.publication_id')")}
    WHERE original.tenant_id=@tenant_id AND original.epoch=@epoch
      AND original.operation='controller.publish_connector'
      AND original.request_id=JSON_VALUE(selected.payload,'$.request_id')
      AND original.request_hash={key_hash("JSON_VALUE(selected.payload,'$.request_id')")}
      AND JSON_VALUE(original.payload,'$.result.connector_id')=@connector_id
      AND JSON_VALUE(original.payload,'$.result.connector.ownership_id')=@ownership_id
      AND JSON_VALUE(publication.payload,'$.connector_id')=@connector_id
      AND JSON_VALUE(publication.payload,'$.ownership_id')=@ownership_id
      AND TRY_CONVERT(bigint,JSON_VALUE(publication.payload,'$.policy_revision'))=@current_revision
      AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.connector.policy_revision'))=@current_revision
      AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.connector.revision'))
          =TRY_CONVERT(bigint,JSON_VALUE(publication.payload,'$.expected_connector_revision'))+1
      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(selected.payload,'$.requested_at'))=TODATETIMEOFFSET(original.recorded_at,'+00:00')
      AND original.recorded_at<@supersession_observed_at
      AND EXISTS (SELECT 1 FROM OPENJSON(original.payload,'$.result.pending_removals') AS original_removal
          WHERE {equal}(original_removal.value,selected.payload)=1)
      AND EXISTS (SELECT 1 FROM OPENJSON(original.payload,'$.result.connector.sources') AS original_binding
          WHERE {equal}(original_binding.value,selected.binding_json)=1)
      AND EXISTS (SELECT 1 FROM OPENJSON(publication.payload,'$.source_removals') AS original_intent
          WHERE JSON_VALUE(original_intent.value,'$.removal_id')=selected.removal_id
            AND {exact_text_equal("JSON_VALUE(original_intent.value,'$.source_id')", 'selected.source_id')}
            AND JSON_VALUE(original_intent.value,'$.proposal_id') IS NULL
            AND {exact_text_equal("JSON_VALUE(original_intent.value,'$.detail')", "JSON_VALUE(selected.payload,'$.detail')")})))"""


def supersession_observation_matches_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")
    return f"""@binding_receipt_payload IS NOT NULL AND @binding_observation IS NOT NULL
AND @supersession_request IS NOT NULL AND @supersession_request_binding IS NOT NULL AND @supersession_patch IS NOT NULL
AND @supersession_observed_at IS NOT NULL
AND @supersession_observed_at BETWEEN DATEADD(second,-{SUPERSESSION_EVIDENCE_TTL_SECONDS},@now) AND @now
AND {exact_text_equal("JSON_VALUE(@binding_receipt_payload,'$.binding_hash')",
                      "CONVERT(nvarchar(64)," + payload_hash('@supersession_request_binding') + ")")}
AND {exact_text_equal("JSON_VALUE(@supersession_request,'$.fingerprint')", 'CONVERT(nvarchar(64),@binding_receipt_fingerprint)')}
AND {exact_text_equal("JSON_VALUE(@plan,'$.producer_fingerprint')", 'CONVERT(nvarchar(64),@binding_receipt_fingerprint)')}
AND JSON_VALUE(@supersession_request,'$.producer')='worker'
AND JSON_VALUE(@supersession_request,'$.topic')='connector'
AND JSON_VALUE(@supersession_request,'$.reference_id')=@connector_id
AND JSON_VALUE(@supersession_request,'$.work_id')=@work_id
AND JSON_VALUE(@supersession_request,'$.request_id')=@binding_receipt_id
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@supersession_request,'$.policy_revision')),-1)=@current_revision
AND JSON_VALUE(@binding_receipt_payload,'$.result.authority')='observed_not_action_authority'
AND JSON_VALUE(@binding_receipt_payload,'$.result.reconcile_work_id')=@work_id
AND {exact_text_equal("JSON_VALUE(@binding_receipt_payload,'$.result.frontier_key')", "JSON_VALUE(@plan,'$.frontier_key')")}
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding_receipt_payload,'$.result.frontier_revision')),-1)
    =COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@plan,'$.frontier_revision')),-2)
AND {exact_text_equal("JSON_VALUE(@supersession_request_binding,'$.tenant_id')", '@tenant_id')}
AND {exact_text_equal("JSON_VALUE(@supersession_request_binding,'$.epoch')", '@epoch')}
AND {exact_text_equal("JSON_VALUE(@supersession_request_binding,'$.connector_id')", '@connector_id')}
AND {exact_text_equal("JSON_VALUE(@supersession_request_binding,'$.ownership_id')", '@ownership_id')}
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@supersession_request_binding,'$.expected_revision')),-1)=@current_revision
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@supersession_request_binding,'$.expected_connector_revision')),-2)+1=@expected_connector_revision
AND {exact_text_equal("JSON_VALUE(@supersession_request_binding,'$.work_id')", "JSON_VALUE(@binding_receipt_payload,'$.result.work_id')")}
AND {exact_text_equal("JSON_VALUE(@supersession_request_binding,'$.owner_id')", "JSON_VALUE(@binding_receipt_payload,'$.result.work_owner_id')")}
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@supersession_request_binding,'$.fence')),-1)
    =COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding_receipt_payload,'$.result.work_fence')),-2)
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@supersession_request_binding,'$.work_revision')),-1)
    =COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding_receipt_payload,'$.result.work_revision')),-2)
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.tenant_id')", '@tenant_id')}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.epoch')", '@epoch')}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.connector_id')", '@connector_id')}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.ownership_id')", '@ownership_id')}
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding_observation,'$.policy_revision')),-1)=@current_revision
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding_observation,'$.revision')),-1)=@expected_connector_revision
AND JSON_VALUE(@binding_observation,'$.state')='degraded'
AND JSON_VALUE(@supersession_patch,'$.state')='degraded'
AND {equal}(JSON_QUERY(@binding_observation,'$.desired_definition'),JSON_QUERY(@prior,'$.desired_definition'))=1
AND {equal}(JSON_QUERY(@binding_observation,'$.sources'),JSON_QUERY(@prior,'$.sources'))=1
AND {equal}(COALESCE(JSON_QUERY(@binding_observation,'$.source_proposals'),N'[]'),@proposals)=1
AND {equal}(COALESCE(JSON_QUERY(@binding_observation,'$.source_removals'),N'[]'),JSON_QUERY(@prior,'$.source_removals'))=1
AND {equal}(JSON_QUERY(@binding_observation,'$.observed_definition'),JSON_QUERY(@supersession_patch,'$.observed_definition'))=1
AND {exact_text_equal("JSON_VALUE(@binding_receipt_payload,'$.result.observed_definition_hash')",
                      "CONVERT(nvarchar(64)," + payload_hash("JSON_QUERY(@supersession_patch,'$.observed_definition')") + ")")}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.workspace_id')", "JSON_VALUE(@prior,'$.workspace_id')")}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.eventstream_id')", "JSON_VALUE(@prior,'$.eventstream_id')")}
AND {exact_text_equal("JSON_VALUE(@binding_observation,'$.destination_id')", "JSON_VALUE(@prior,'$.destination_id')")}
AND {equal}(JSON_QUERY(@binding_observation,'$.endpoint'),JSON_QUERY(@prior,'$.endpoint'))=1
AND JSON_VALUE(@desired,'$.ownership_id')=@ownership_id
AND COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@desired,'$.policy_revision')),-1)=@current_revision
AND {exact_text_equal("JSON_VALUE(@desired,'$.definition_hash')",
                      "CONVERT(nvarchar(64)," + payload_hash("JSON_QUERY(@prior,'$.desired_definition')") + ")")}
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@desired,'$.published_at')) IS NOT NULL
AND TRY_CONVERT(datetimeoffset,JSON_VALUE(@desired,'$.published_at'))<TODATETIMEOFFSET(@supersession_observed_at,'+00:00')"""


def supersession_observation_receipt_sql(names: SqlNames) -> str:
    return f"""SELECT payload,fingerprint,recorded_at FROM {names.table('monitoring_receipts')}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND operation='worker.observe_connector'
  AND request_id=@binding_receipt_id AND request_hash={key_hash('@binding_receipt_id')}
  AND JSON_VALUE(payload,'$.result.connector_id')=@connector_id
  AND TRY_CONVERT(bigint,JSON_VALUE(payload,'$.result.connector.revision'))=@expected_connector_revision"""


def load_supersession_observation_sql(names: SqlNames) -> str:
    records = names.table("monitoring_records")
    return f"""DECLARE @supersession_request nvarchar(max),@supersession_request_binding nvarchar(max),
    @supersession_patch nvarchar(max),@supersession_observed_at datetime2(6);
SELECT @binding_receipt_payload=payload,@binding_receipt_fingerprint=fingerprint,@supersession_observed_at=recorded_at
FROM ({supersession_observation_receipt_sql(names)}) AS original_inspection;
SET @binding_observation=JSON_QUERY(@binding_receipt_payload,'$.result.observation');
SELECT @supersession_request=payload FROM {records}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='worker_reconcile_request'
  AND full_key=@binding_receipt_id AND key_hash={key_hash('@binding_receipt_id')};
SET @supersession_request_binding=JSON_QUERY(@supersession_request,'$.request_payload');
-- OPENJSON keeps the original NVARCHAR(MAX) input; JSON_VALUE truncates long patches.
SELECT @supersession_patch=value FROM OPENJSON(@supersession_request_binding)
WHERE [key] COLLATE Latin1_General_100_BIN2='observation_json' AND type=1;
IF CASE WHEN {supersession_observation_matches_sql(names)} THEN 1 ELSE 0 END<>1
    THROW 51072, 'Supersession requires the exact fresh original observation input and protected global handoff', 1;"""


def validate_supersession_sql(names: SqlNames, complete_component_map: str) -> str:
    equal = names.object("json_equal")
    return f"""IF EXISTS (SELECT 1 FROM @supersessions)
BEGIN
    {load_supersession_observation_sql(names)}
    DECLARE @supersession_inspection nvarchar(max)=JSON_QUERY(@binding_receipt_payload,'$.result.inspection');
    IF {inspection_invalid_sql('@supersession_inspection', '@binding_observation')}
       OR {equal}(@supersession_inspection,JSON_QUERY(@supersession_patch,'$.inspection'))<>1
       OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@supersession_inspection,'$.observed_at'))
           >TODATETIMEOFFSET(@supersession_observed_at,'+00:00')
       OR EXISTS (SELECT 1 FROM @supersessions AS selected
           WHERE TRY_CONVERT(datetimeoffset,JSON_VALUE(@supersession_inspection,'$.observed_at'))
               <=TRY_CONVERT(datetimeoffset,JSON_VALUE(selected.payload,'$.requested_at')))
        THROW 51072, 'Supersession needs fresh complete original read-only running inspection evidence', 1;
    IF CASE WHEN {complete_component_map} THEN 1 ELSE 0 END<>1
        THROW 51072, 'Supersession requires the complete original physical component map', 1;
    IF {supersession_original_removal_invalid_sql(names)}
        THROW 51072, 'Supersession lost the immutable original removal publication and binding', 1;
    IF {supersession_history_invalid_sql(names)}
        THROW 51072, 'Supersession history is missing or ambiguous; no unexecuted removal is proved', 1;
    IF {supersession_uncertain_write_sql(names)}
        THROW 51072, 'A retained possible definition-write intent forbids source removal supersession', 1;
    IF {supersession_presence_topology_invalid_sql(names)}
        THROW 51072, 'Supersession observation does not prove the exact retained physical source and transport', 1;
    IF {supersession_topology_delta_invalid_sql(names)}
        THROW 51072, 'Supersession may restore only the selected original physical nodes', 1;
    {supersession_collection_work_sql(names)}
    SET @superseded_json=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),payload),N',')
        WITHIN GROUP (ORDER BY removal_id) FROM @supersessions),N'')+N']';
END;"""


def supersession_presence_topology_invalid_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")
    return f"""({supersession_observed_sources_invalid_sql(names)})
OR EXISTS (SELECT 1 FROM @supersessions AS selected WHERE
    NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition.component_ids') AS component
        WHERE component.type=1
          AND {exact_text_equal('component.[key]', "N'sources/'+selected.node_name")}
          AND {exact_text_equal('component.value', 'selected.source_id')})
    OR (JSON_VALUE(selected.payload,'$.last_observed_source_id') IS NOT NULL
        AND NOT {exact_text_equal("JSON_VALUE(selected.payload,'$.last_observed_source_id')", 'selected.source_id')})
    OR NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json".sources') AS node
        WHERE {exact_text_equal("JSON_VALUE(node.value,'$.name')", 'selected.node_name')}
          AND JSON_VALUE(node.value,'$.type')='FabricJobEvents'
          AND JSON_VALUE(node.value,'$.properties.eventScope')='Item'
          AND NOT EXISTS (SELECT 1 FROM OPENJSON(node.value) AS node_id
              WHERE node_id.[key]='id' AND (node_id.type<>1 OR NOT {exact_text_equal('node_id.value', 'selected.source_id')}))
          AND {exact_text_equal("JSON_VALUE(node.value,'$.properties.workspaceId')", "JSON_VALUE(selected.binding_json,'$.target.workspace_id')")}
          AND {exact_text_equal("JSON_VALUE(node.value,'$.properties.itemId')", "JSON_VALUE(selected.binding_json,'$.target.item_id')")}
          AND NOT EXISTS (
              SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(node.value,'$.properties.includedEventTypes')
              EXCEPT SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(selected.binding_json,'$.event_types'))
          AND NOT EXISTS (
              SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(selected.binding_json,'$.event_types')
              EXCEPT SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(node.value,'$.properties.includedEventTypes')))
    OR NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json".streams[0].inputNodes') AS input_node
        WHERE {exact_text_equal("JSON_VALUE(input_node.value,'$.name')", 'selected.node_name')}))
OR EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.component_ids') AS owned_component
    WHERE (owned_component.[key] COLLATE Latin1_General_100_BIN2 LIKE 'streams/%'
        OR owned_component.[key] COLLATE Latin1_General_100_BIN2 LIKE 'destinations/%')
      AND NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition.component_ids') AS observed_component
          WHERE {exact_text_equal('observed_component.[key]', 'owned_component.[key]')}
            AND {exact_text_equal('observed_component.value', 'owned_component.value')}))
OR {equal}(
    JSON_QUERY(@binding_observation,'$.observed_definition.parts."eventstream.json".destinations'),
    JSON_QUERY(@prior,'$.desired_definition.parts."eventstream.json".destinations'))<>1
OR {equal}(
    JSON_MODIFY(JSON_QUERY(@binding_observation,'$.observed_definition.parts."eventstream.json".streams[0]'),'$.inputNodes',NULL),
    JSON_MODIFY(JSON_QUERY(@prior,'$.desired_definition.parts."eventstream.json".streams[0]'),'$.inputNodes',NULL))<>1
OR (SELECT COUNT(*) FROM OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json".streams'))<>1
OR (SELECT COUNT(*) FROM OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json".destinations'))<>1"""


def supersession_observed_sources_invalid_sql(names: SqlNames) -> str:
    """A selected source cannot hide drift or missing coverage elsewhere in the snapshot."""
    equal = names.object("json_equal")
    graph = """JSON_QUERY(@binding_observation,'$.observed_definition.parts."eventstream.json"')"""
    return f"""NOT EXISTS (SELECT 1 FROM OPENJSON({graph}) WHERE [key]='operators' AND type=4)
OR (SELECT COUNT(*) FROM OPENJSON({graph},'$.operators'))<>0
OR EXISTS (SELECT JSON_VALUE(value,'$.name') COLLATE Latin1_General_100_BIN2
    FROM OPENJSON({graph},'$.sources')
    GROUP BY JSON_VALUE(value,'$.name') COLLATE Latin1_General_100_BIN2,
        DATALENGTH(JSON_VALUE(value,'$.name')) HAVING COUNT(*)>1)
OR EXISTS (SELECT 1 FROM OPENJSON({graph},'$.sources') AS node
    JOIN OPENJSON(@binding_observation,'$.observed_definition.component_ids') AS component
      ON {exact_text_equal('component.[key]', "N'sources/'+JSON_VALUE(node.value,'$.name')")}
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.sources') AS owned_source
        WHERE {exact_text_equal("JSON_VALUE(owned_source.value,'$.source_id')", 'component.value')}
          AND JSON_VALUE(node.value,'$.type')='FabricJobEvents'
          AND JSON_VALUE(node.value,'$.properties.eventScope')='Item'
          AND {exact_text_equal("JSON_VALUE(node.value,'$.properties.workspaceId')", "JSON_VALUE(owned_source.value,'$.target.workspace_id')")}
          AND {exact_text_equal("JSON_VALUE(node.value,'$.properties.itemId')", "JSON_VALUE(owned_source.value,'$.target.item_id')")}
          AND NOT EXISTS (SELECT 1 FROM OPENJSON(node.value) AS node_id
              WHERE node_id.[key]='id' AND (node_id.type<>1 OR NOT {exact_text_equal('node_id.value', "JSON_VALUE(owned_source.value,'$.source_id')")}))
          AND EXISTS (SELECT 1 FROM OPENJSON(node.value,'$.properties') WHERE [key]='includedEventTypes' AND type=4)
          AND NOT EXISTS (SELECT 1 FROM OPENJSON(node.value,'$.properties.includedEventTypes') WHERE type<>1)
          AND NOT EXISTS (
              SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(node.value,'$.properties.includedEventTypes')
              EXCEPT SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(owned_source.value,'$.event_types'))
          AND NOT EXISTS (
              SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(owned_source.value,'$.event_types')
              EXCEPT SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(node.value,'$.properties.includedEventTypes'))
          AND (EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.component_ids') AS approved_node
              WHERE {exact_text_equal('approved_node.[key]', 'component.[key]')}
                AND {exact_text_equal('approved_node.value', 'component.value')})
            OR EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.source_removals') AS removal
                WHERE {exact_text_equal("JSON_VALUE(removal.value,'$.source_id')", 'component.value')}
                  AND {exact_text_equal("JSON_VALUE(removal.value,'$.node_name')", "JSON_VALUE(node.value,'$.name')")}))))
OR EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.parts."eventstream.json".sources') AS approved_node
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON({graph},'$.sources') AS observed_node
        WHERE {equal}(JSON_MODIFY(approved_node.value,'$.id',NULL),JSON_MODIFY(observed_node.value,'$.id',NULL))=1))
OR NOT EXISTS (SELECT 1 FROM OPENJSON({graph},'$.streams[0]') WHERE [key]='inputNodes' AND type=4)
OR (SELECT COUNT(*) FROM OPENJSON({graph},'$.streams[0].inputNodes'))
    <>(SELECT COUNT(*) FROM OPENJSON({graph},'$.sources'))
OR EXISTS (SELECT 1 FROM OPENJSON({graph},'$.sources') AS observed_node
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON({graph},'$.streams[0].inputNodes') AS input_node
        WHERE input_node.type=5
          AND {exact_text_equal("JSON_VALUE(input_node.value,'$.name')", "JSON_VALUE(observed_node.value,'$.name')")}))"""


def supersession_topology_delta_invalid_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")

    def fixed_parts(expression: str) -> str:
        return (
            f"JSON_MODIFY(JSON_MODIFY(JSON_MODIFY({expression},'$.component_ids',NULL),"
            """'$.parts."eventstream.json".sources',NULL),'$.parts."eventstream.json".streams[0].inputNodes',NULL)"""
        )

    return f"""{equal}({fixed_parts('@definition')},{fixed_parts("JSON_QUERY(@prior,'$.desired_definition')")})<>1
OR EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.parts."eventstream.json".sources') AS old_node
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@definition,'$.parts."eventstream.json".sources') AS new_node
        WHERE {equal}(old_node.value,new_node.value)=1))
OR EXISTS (SELECT 1 FROM OPENJSON(@definition,'$.parts."eventstream.json".sources') AS new_node
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.parts."eventstream.json".sources') AS old_node
            WHERE {equal}(old_node.value,new_node.value)=1)
      AND NOT EXISTS (SELECT 1 FROM @supersessions AS selected
          JOIN OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json".sources') AS observed_node
            ON {exact_text_equal("JSON_VALUE(observed_node.value,'$.name')", 'selected.node_name')}
          WHERE {equal}(JSON_MODIFY(new_node.value,'$.id',NULL),JSON_MODIFY(observed_node.value,'$.id',NULL))=1
            AND NOT EXISTS (SELECT 1 FROM OPENJSON(new_node.value) AS node_id
                WHERE node_id.[key]='id' AND (node_id.type<>1 OR NOT {exact_text_equal('node_id.value', 'selected.source_id')}))))
OR (SELECT COUNT(*) FROM OPENJSON(@definition,'$.parts."eventstream.json".sources'))
    <>(SELECT COUNT(*) FROM OPENJSON(@prior,'$.desired_definition.parts."eventstream.json".sources'))
       +(SELECT COUNT(*) FROM @supersessions)
OR EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.component_ids') AS old_component
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@definition,'$.component_ids') AS new_component
        WHERE {exact_text_equal('old_component.[key]', 'new_component.[key]')}
          AND {exact_text_equal('old_component.value', 'new_component.value')}))
OR EXISTS (SELECT 1 FROM OPENJSON(@definition,'$.component_ids') AS new_component
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.desired_definition.component_ids') AS old_component
        WHERE {exact_text_equal('old_component.[key]', 'new_component.[key]')}
          AND {exact_text_equal('old_component.value', 'new_component.value')})
      AND NOT EXISTS (SELECT 1 FROM @supersessions AS selected
          WHERE {exact_text_equal('new_component.[key]', "N'sources/'+selected.node_name")}
            AND {exact_text_equal('new_component.value', 'selected.source_id')}))
OR EXISTS (SELECT 1 FROM @supersessions AS selected WHERE NOT EXISTS (
    SELECT 1 FROM OPENJSON(@definition,'$.component_ids') AS restored
    WHERE {exact_text_equal('restored.[key]', "N'sources/'+selected.node_name")}
      AND {exact_text_equal('restored.value', 'selected.source_id')}))"""


def supersession_uncertain_write_sql(names: SqlNames) -> str:
    pending = literals(sorted(CONNECTOR_PENDING_GAP_CODES))
    return f"""JSON_VALUE(@prior,'$.operation_id') IS NOT NULL
OR JSON_VALUE(@binding_observation,'$.operation_id') IS NOT NULL
OR EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.gaps') AS gap WHERE JSON_VALUE(gap.value,'$.code') IN ({pending}))
OR EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.gaps') AS gap WHERE JSON_VALUE(gap.value,'$.code') IN ({pending}))
OR EXISTS (SELECT 1 FROM {names.table('monitoring_receipts')} AS effect
    WHERE effect.tenant_id=@tenant_id AND effect.epoch=@epoch
      AND effect.operation IN ('worker.observe_connector','controller.publish_connector')
      AND JSON_VALUE(effect.payload,'$.result.connector_id')=@connector_id
      AND EXISTS (SELECT 1 FROM @supersessions AS selected
          JOIN {names.table('monitoring_receipts')} AS original
            ON original.tenant_id=@tenant_id AND original.epoch=@epoch
           AND original.operation='controller.publish_connector'
           AND original.request_id=JSON_VALUE(selected.payload,'$.request_id')
          WHERE TODATETIMEOFFSET(effect.recorded_at,'+00:00')>=TRY_CONVERT(datetimeoffset,JSON_VALUE(selected.payload,'$.requested_at'))
             OR TRY_CONVERT(bigint,JSON_VALUE(effect.payload,'$.result.connector.revision'))
                 BETWEEN TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.connector.revision')) AND @expected_connector_revision)
      AND (JSON_VALUE(effect.payload,'$.result.observation.operation_id') IS NOT NULL
          OR JSON_VALUE(effect.payload,'$.result.connector.operation_id') IS NOT NULL
          OR EXISTS (SELECT 1 FROM OPENJSON(effect.payload,'$.result.observation.gaps') AS gap
              WHERE JSON_VALUE(gap.value,'$.code') IN ({pending}))
          OR EXISTS (SELECT 1 FROM OPENJSON(effect.payload,'$.result.connector.gaps') AS gap
              WHERE JSON_VALUE(gap.value,'$.code') IN ({pending}))))"""


def supersession_history_invalid_sql(names: SqlNames) -> str:
    """Every connector revision since each original removal must have one retained receipt."""
    receipts = names.table("monitoring_receipts")
    equal = names.object("json_equal")
    applicable = """history.tenant_id=@tenant_id AND history.epoch=@epoch
AND history.operation IN ('worker.observe_connector','controller.publish_connector')
AND JSON_VALUE(history.payload,'$.result.connector_id')=@connector_id
AND TRY_CONVERT(bigint,JSON_VALUE(history.payload,'$.result.connector.revision'))
    BETWEEN TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.connector.revision'))+1 AND @expected_connector_revision"""
    return f"""EXISTS (SELECT 1 FROM @supersessions AS selected WHERE NOT EXISTS (
    SELECT 1 FROM {receipts} AS original
    WHERE original.tenant_id=@tenant_id AND original.epoch=@epoch
      AND original.operation='controller.publish_connector'
      AND original.request_id=JSON_VALUE(selected.payload,'$.request_id')
      AND JSON_VALUE(original.payload,'$.result.connector_id')=@connector_id
      AND TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.connector.revision'))
          BETWEEN 1 AND @expected_connector_revision-1
      AND (SELECT COUNT_BIG(*) FROM {receipts} AS history WHERE {applicable})
          =@expected_connector_revision-TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.connector.revision'))
      AND (SELECT COUNT_BIG(DISTINCT TRY_CONVERT(bigint,JSON_VALUE(history.payload,'$.result.connector.revision')))
          FROM {receipts} AS history WHERE {applicable})
          =@expected_connector_revision-TRY_CONVERT(bigint,JSON_VALUE(original.payload,'$.result.connector.revision'))
      AND NOT EXISTS (SELECT 1 FROM {receipts} AS history WHERE {applicable} AND (
          COALESCE(JSON_VALUE(history.payload,'$.result.connector.ownership_id'),'')<>@ownership_id
          OR NOT EXISTS (SELECT 1 FROM OPENJSON(history.payload,'$.result.connector.sources') AS binding
              WHERE {equal}(binding.value,selected.binding_json)=1)
          OR NOT EXISTS (SELECT 1 FROM OPENJSON(history.payload,'$.result.connector.source_removals') AS removal
              WHERE {equal}(removal.value,selected.payload)=1)))))"""


def supersession_collection_work_sql(names: SqlNames) -> str:
    records, leases, receipts = (
        names.table("monitoring_records"), names.table("monitoring_leases"), names.table("monitoring_receipts"),
    )
    equal = names.object("json_equal")
    return f"""DECLARE @supersession_work_id nvarchar(36)=JSON_VALUE(@binding_receipt_payload,'$.result.work_id'),
    @supersession_work nvarchar(max),@supersession_work_revision bigint;
SELECT @supersession_work=payload,@supersession_work_revision=revision
FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
  AND full_key=@supersession_work_id AND key_hash={key_hash('@supersession_work_id')}
  AND work_kind='connector_reconcile' AND status='completed';
IF @supersession_work IS NULL
   OR COALESCE(JSON_VALUE(@supersession_work,'$.tenant_id'),'')<>@tenant_id
   OR COALESCE(JSON_VALUE(@supersession_work,'$.epoch'),'')<>@epoch
   OR COALESCE(JSON_VALUE(@supersession_work,'$.work_id'),'')<>@supersession_work_id
   OR COALESCE(JSON_VALUE(@supersession_work,'$.connector_id'),'')<>@connector_id
   OR COALESCE(JSON_VALUE(@supersession_work,'$.state'),'')<>'completed'
   OR COALESCE(JSON_VALUE(@supersession_work,'$.kind'),'')<>'connector_reconcile'
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@supersession_work,'$.revision')),-1)<>@supersession_work_revision
   OR EXISTS (SELECT 1 FROM OPENJSON(@supersession_work) WHERE [key] IN ('lease','target','execution','action_reservation_id','retry_of','finalization_id') AND type<>0)
   OR COALESCE(TRY_CONVERT(int,JSON_VALUE(@supersession_work,'$.retry_attempt')),-1)<>0
   OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@binding_receipt_payload,'$.result.work_revision')),-1) NOT BETWEEN 1 AND @supersession_work_revision-1
   OR COALESCE(JSON_VALUE(@binding_receipt_payload,'$.result.collection_completion_eligible'),'')<>'true'
   OR NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_receipt_payload,'$.result') WHERE [key]='collection_completion_eligible' AND type=3)
   OR NOT EXISTS (SELECT 1 FROM {leases} WITH (UPDLOCK,HOLDLOCK)
       WHERE tenant_id=@tenant_id AND epoch=@epoch
         AND full_key=N'work:v1:'+@epoch+N':'+@tenant_id+N':'+@supersession_work_id
         AND key_hash={key_hash("N'work:v1:'+@epoch+N':'+@tenant_id+N':'+@supersession_work_id")}
         AND owner_id=JSON_VALUE(@binding_receipt_payload,'$.result.work_owner_id')
         AND fence=TRY_CONVERT(bigint,JSON_VALUE(@binding_receipt_payload,'$.result.work_fence'))
         AND expires_at<=@now)
   OR NOT EXISTS (SELECT 1 FROM {receipts} AS completed
       WHERE completed.tenant_id=@tenant_id AND completed.epoch=@epoch AND completed.operation='worker.transition_work'
         AND JSON_VALUE(completed.payload,'$.result.work_id')=@supersession_work_id
         AND {equal}(JSON_QUERY(completed.payload,'$.result.work'),@supersession_work)=1
         AND completed.recorded_at>=@supersession_observed_at AND completed.recorded_at<=@now)
    THROW 51072, 'Supersession requires the original inspection collection to be terminal under its exact fence', 1;
IF EXISTS (SELECT 1 FROM {records} AS other_work WITH (UPDLOCK,HOLDLOCK)
    LEFT JOIN {leases} AS other_lease WITH (UPDLOCK,HOLDLOCK)
      ON other_lease.tenant_id=other_work.tenant_id AND other_lease.epoch=other_work.epoch
     AND other_lease.full_key=N'work:v1:'+@epoch+N':'+@tenant_id+N':'+other_work.full_key
    WHERE other_work.tenant_id=@tenant_id AND other_work.epoch=@epoch AND other_work.record_kind='work'
      AND other_work.work_kind='connector_reconcile'
      AND JSON_VALUE(other_work.payload,'$.connector_id')=@connector_id
      AND other_work.full_key<>@supersession_work_id
      AND (other_lease.expires_at>@now
          OR (other_work.status NOT IN ('completed','dispositioned')
              AND (other_work.status<>'queued' OR COALESCE(TRY_CONVERT(int,JSON_VALUE(other_work.payload,'$.attempts')),-1)<>0
                  OR other_lease.full_key IS NOT NULL
                  OR COALESCE(TRY_CONVERT(int,JSON_VALUE(other_work.payload,'$.retry_attempt')),-1)<>0
                  OR EXISTS (SELECT 1 FROM OPENJSON(other_work.payload)
                      WHERE [key] IN ('lease','target','execution','action_reservation_id','retry_of','finalization_id') AND type<>0)))))
    THROW 51072, 'Another connector worker is active or has an unreconciled attempted effect', 1;
INSERT INTO @supersession_queued_work SELECT full_key,revision
FROM {records} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work' AND work_kind='connector_reconcile'
  AND JSON_VALUE(payload,'$.connector_id')=@connector_id AND status='queued';"""


def supersession_disposition_work_sql(names: SqlNames) -> str:
    return f"""IF EXISTS (SELECT 1 FROM @supersessions)
BEGIN
    UPDATE work_record SET revision=work_record.revision+1,status='dispositioned',
        payload=JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(JSON_MODIFY(work_record.payload,'$.state','dispositioned'),
            '$.revision',work_record.revision+1),'$.completed_at',CONVERT(nvarchar(40),@now,127)+N'Z'),
            '$.disposition',N'Unclaimed connector work superseded by controller.publish_connector request '+@request_id)
    FROM {names.table('monitoring_records')} AS work_record
    JOIN @supersession_queued_work AS queued ON queued.work_id=work_record.full_key AND queued.revision=work_record.revision
    WHERE work_record.tenant_id=@tenant_id AND work_record.epoch=@epoch AND work_record.record_kind='work'
      AND work_record.work_kind='connector_reconcile' AND work_record.status='queued';
    IF @@ROWCOUNT<>(SELECT COUNT(*) FROM @supersession_queued_work)
        THROW 51072, 'Supersession lost the locked unclaimed connector work revision', 1;
END;"""


def direct_scope_rule_matches_sql(rule: str) -> str:
    return f"""JSON_VALUE({rule},'$.selector.tenant_id')=@tenant_id
AND EXISTS (SELECT 1 FROM OPENJSON({rule},'$.workloads') AS workload
    WHERE workload.type=1 AND workload.value=JSON_VALUE(@source_target,'$.workload'))
AND (JSON_VALUE({rule},'$.selector.kind')='tenant'
    OR (JSON_VALUE({rule},'$.selector.kind') IN ('workspace','item')
        AND JSON_VALUE({rule},'$.selector.workspace_id')=JSON_VALUE(@source_target,'$.workspace_id')
        AND (JSON_VALUE({rule},'$.selector.kind')='workspace'
            OR JSON_VALUE({rule},'$.selector.item_id')=JSON_VALUE(@source_target,'$.item_id'))))"""


def supersession_current_scope_sql(names: SqlNames) -> str:
    """Bound recovery to directly matched reviewed selectors; unknown ancestry denies."""
    records = names.table("monitoring_records")
    return f"""COALESCE(JSON_VALUE(@approved_target,'$.admission_basis'),'') IN ('reviewed','auto_detection_only')
AND EXISTS (SELECT 1 FROM {records} AS scope_record
    JOIN OPENJSON(@approved_target,'$.scope_ids') AS admitted_scope ON admitted_scope.value=scope_record.full_key
    CROSS APPLY OPENJSON(scope_record.payload,'$.rules') AS include_rule
    WHERE scope_record.tenant_id=@tenant_id AND scope_record.epoch=@epoch AND scope_record.record_kind='scope'
      AND JSON_VALUE(scope_record.payload,'$.enabled')='true'
      AND JSON_VALUE(include_rule.value,'$.effect')='include'
      AND ({direct_scope_rule_matches_sql('include_rule.value')})
      AND EXISTS (SELECT 1 FROM OPENJSON(@approved_target,'$.admitted_rule_ids') AS admitted_rule
          WHERE admitted_rule.value=JSON_VALUE(include_rule.value,'$.rule_id')))
AND NOT EXISTS (SELECT 1 FROM {records} AS scope_record
    CROSS APPLY OPENJSON(scope_record.payload,'$.rules') AS exclude_rule
    WHERE scope_record.tenant_id=@tenant_id AND scope_record.epoch=@epoch AND scope_record.record_kind='scope'
      AND JSON_VALUE(scope_record.payload,'$.enabled')='true'
      AND JSON_VALUE(exclude_rule.value,'$.effect')='exclude'
      AND EXISTS (SELECT 1 FROM OPENJSON(exclude_rule.value,'$.workloads') AS workload
          WHERE workload.type=1 AND workload.value=JSON_VALUE(@source_target,'$.workload'))
      AND (JSON_VALUE(exclude_rule.value,'$.selector.kind')='domain'
          OR ({direct_scope_rule_matches_sql('exclude_rule.value')})))"""


def restored_receipt_history_complete_sql(names: SqlNames) -> str:
    """The first recovery anchors later supersessions without forgetting earlier sources."""
    receipts = names.table("monitoring_receipts")
    first = "TRY_CONVERT(bigint,JSON_VALUE(publication.payload,'$.result.connector.revision'))"
    current = "TRY_CONVERT(bigint,JSON_VALUE(@connector,'$.revision'))"
    applicable = f"""history.tenant_id=@tenant_id AND history.epoch=@epoch
AND history.operation IN ('worker.observe_connector','controller.publish_connector')
AND JSON_VALUE(history.payload,'$.result.connector_id')=@connector_id
AND TRY_CONVERT(bigint,JSON_VALUE(history.payload,'$.result.connector.revision')) BETWEEN {first} AND {current}"""
    return f"""{first} BETWEEN 1 AND {current}
AND (SELECT COUNT_BIG(*) FROM {receipts} AS history WHERE {applicable})={current}-{first}+1
AND (SELECT COUNT_BIG(DISTINCT TRY_CONVERT(bigint,JSON_VALUE(history.payload,'$.result.connector.revision')))
    FROM {receipts} AS history WHERE {applicable})={current}-{first}+1
AND NOT EXISTS (SELECT 1 FROM {receipts} AS history WHERE {applicable} AND (
    COALESCE(JSON_VALUE(history.payload,'$.result.connector.ownership_id'),'')<>JSON_VALUE(@connector,'$.ownership_id')
    OR COALESCE(JSON_VALUE(history.payload,'$.result.connector.tenant_id'),'')<>@tenant_id
    OR COALESCE(JSON_VALUE(history.payload,'$.result.connector.epoch'),'')<>@epoch))"""


def restored_intake_invalid_sql(names: SqlNames) -> str:
    """A restored desired node does not itself re-enable event acceptance."""
    records, receipts = names.table("monitoring_records"), names.table("monitoring_receipts")
    equal = names.object("json_equal")
    return f"""EXISTS (SELECT 1 FROM @positions WHERE disposition='accepted')
AND EXISTS (SELECT 1 FROM {records} AS desired
    WHERE desired.tenant_id=@tenant_id AND desired.epoch=@epoch AND desired.record_kind='connector_desired'
      AND desired.full_key=@connector_id AND JSON_VALUE(desired.payload,'$.supersession_request_id') IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {receipts} AS publication
          WHERE publication.tenant_id=@tenant_id AND publication.epoch=@epoch
            AND publication.operation='controller.publish_connector'
            AND publication.request_id=JSON_VALUE(desired.payload,'$.supersession_request_id')
            AND JSON_VALUE(publication.payload,'$.result.connector_id')=@connector_id
            AND JSON_VALUE(publication.payload,'$.result.connector.ownership_id')=JSON_VALUE(@connector,'$.ownership_id')
            AND (SELECT COUNT(*) FROM OPENJSON(publication.payload,'$.result.superseded_source_removals'))>0
            AND TRY_CONVERT(datetimeoffset,JSON_VALUE(desired.payload,'$.published_at')) IS NOT NULL
            AND ({restored_receipt_history_complete_sql(names)})
            AND NOT EXISTS (SELECT 1 FROM @positions AS position
                CROSS JOIN {receipts} AS recovery
                CROSS APPLY OPENJSON(recovery.payload,'$.result.superseded_source_removals') AS restored
                JOIN OPENJSON(@connector,'$.sources') AS owned
                  ON {exact_text_equal("JSON_VALUE(owned.value,'$.source_id')", "JSON_VALUE(restored.value,'$.source_id')")}
                 AND {equal}(JSON_QUERY(owned.value,'$.target'),JSON_QUERY(restored.value,'$.target'))=1
                WHERE position.disposition='accepted'
                  AND recovery.tenant_id=@tenant_id AND recovery.epoch=@epoch
                  AND recovery.operation='controller.publish_connector'
                  AND JSON_VALUE(recovery.payload,'$.result.connector_id')=@connector_id
                  AND JSON_VALUE(recovery.payload,'$.result.connector.ownership_id')=JSON_VALUE(@connector,'$.ownership_id')
                  AND TRY_CONVERT(bigint,JSON_VALUE(recovery.payload,'$.result.connector.revision'))
                      BETWEEN TRY_CONVERT(bigint,JSON_VALUE(publication.payload,'$.result.connector.revision'))
                          AND TRY_CONVERT(bigint,JSON_VALUE(@connector,'$.revision'))
                  AND {equal}(JSON_QUERY(restored.value,'$.target'),JSON_QUERY(position.payload,'$.observation.execution.target'))=1
                  AND NOT EXISTS (
                    SELECT 1 FROM {records} AS capability JOIN {records} AS admitted
                      ON admitted.tenant_id=capability.tenant_id AND admitted.epoch=capability.epoch
                     AND admitted.record_kind='target' AND admitted.full_key=capability.full_key
                    WHERE capability.tenant_id=@tenant_id AND capability.epoch=@epoch
                      AND capability.record_kind='target_capability'
                      AND {equal}(JSON_QUERY(capability.payload,'$.target'),JSON_QUERY(restored.value,'$.target'))=1
                      AND JSON_VALUE(capability.payload,'$.read_status')='verified'
                      AND JSON_VALUE(capability.payload,'$.event_status')='verified'
                      AND JSON_VALUE(admitted.payload,'$.state')='current'
                      AND JSON_VALUE(admitted.payload,'$.observation.enabled')='true'
                      AND TRY_CONVERT(bigint,JSON_VALUE(admitted.payload,'$.policy_revision'))=@current_revision
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(capability.payload,'$.expires_at'))>TODATETIMEOFFSET(@now,'+00:00')
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(capability.payload,'$.checked_at'))
                          >=TRY_CONVERT(datetimeoffset,JSON_VALUE(desired.payload,'$.published_at'))
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(capability.payload,'$.checked_at'))<=TODATETIMEOFFSET(@now,'+00:00')
                      AND {exact_text_equal("JSON_VALUE(capability.payload,'$.collector_identity_id')", "JSON_VALUE(position.payload,'$.transport.collector_identity_id')")}
                      AND {exact_text_equal("JSON_VALUE(restored.value,'$.source_id')", "JSON_VALUE(position.payload,'$.transport.source_id')")}
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(position.payload,'$.transport.identity_verified_at'))
                          >=TRY_CONVERT(datetimeoffset,JSON_VALUE(desired.payload,'$.published_at'))
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(position.payload,'$.transport.identity_verified_at'))
                          <=TRY_CONVERT(datetimeoffset,JSON_VALUE(position.payload,'$.received_at'))
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(position.payload,'$.received_at'))<=TODATETIMEOFFSET(@now,'+00:00')
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(position.payload,'$.position.enqueued_at'))
                          >=TRY_CONVERT(datetimeoffset,JSON_VALUE(desired.payload,'$.published_at'))
                      AND TRY_CONVERT(datetimeoffset,JSON_VALUE(position.payload,'$.position.enqueued_at'))
                          <=TRY_CONVERT(datetimeoffset,JSON_VALUE(position.payload,'$.received_at'))))))"""
