"""Desired removal is not relinquishment of physical source ownership.

Only an original owned observation proving exact remote absence can retire a
binding. Pending removals revoke intake while keeping IDs available for remote
ownership validation, uncertain-ack recovery and an immutable retirement record.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    exact_text_equal,
    key_hash,
    payload_hash,
)
from triage.monitoring.sql_kernel_contracts import SqlNames


def source_is_pending_removal_sql(source: str, removals: str) -> str:
    return f"""EXISTS (SELECT 1 FROM OPENJSON({removals}) AS removal
WHERE ({exact_text_equal("JSON_VALUE(removal.value,'$.source_id')", f"JSON_VALUE({source},'$.source_id')")})
   OR ({exact_text_equal("JSON_VALUE(removal.value,'$.proposal_id')", f"JSON_VALUE({source},'$.proposal_id')")}))"""


def retained_ownership_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")
    return f"""IF @prior IS NOT NULL AND {equal}(@sources,JSON_QUERY(@prior,'$.sources'))<>1
    THROW 51072, 'Desired changes must retain all owned source bindings until receipt-verified retirement', 1;
IF @prior IS NOT NULL AND EXISTS (
    SELECT 1 FROM OPENJSON(@prior,'$.source_proposals') AS old_proposal
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@proposals) AS current_proposal
        WHERE JSON_VALUE(current_proposal.value,'$.proposal_id')=JSON_VALUE(old_proposal.value,'$.proposal_id')
          AND {equal}(current_proposal.value,old_proposal.value)=1))
    THROW 51072, 'Uncertain logical proposal ownership cannot be discarded without verified absence', 1;
IF @prior IS NOT NULL
BEGIN
    -- Keep original binding bytes/order: removal hashes must survive equivalent JSON serialization.
    SET @sources=JSON_QUERY(@prior,'$.sources');
    SET @proposals=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),value),N',')
        WITHIN GROUP (ORDER BY ownership_order,position) FROM (
            SELECT value,0 AS ownership_order,CONVERT(int,[key]) AS position
            FROM OPENJSON(@prior,'$.source_proposals')
            UNION ALL
            SELECT p.value,1,CONVERT(int,p.[key]) FROM OPENJSON(@proposals) AS p
            WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.source_proposals') AS old_proposal
                WHERE JSON_VALUE(old_proposal.value,'$.proposal_id')=JSON_VALUE(p.value,'$.proposal_id'))
        ) AS retained),N'')+N']';
END;"""


def removal_binding_sql() -> str:
    return f"""SELECT value FROM OPENJSON(@prior,'$.sources')
WHERE @remove_source_id IS NOT NULL
  AND {exact_text_equal("JSON_VALUE(value,'$.source_id')", '@remove_source_id')}
UNION ALL SELECT value FROM OPENJSON(@prior,'$.source_proposals')
WHERE @remove_source_id IS NULL AND @remove_proposal_id IS NOT NULL
  AND {exact_text_equal("JSON_VALUE(value,'$.proposal_id')", '@remove_proposal_id')}"""


def original_removal_matches_sql() -> str:
    source_match = exact_text_equal("JSON_VALUE(@old_removal,'$.source_id')", "@remove_source_id")
    proposal_match = exact_text_equal("JSON_VALUE(@old_removal,'$.proposal_id')", "@remove_proposal_id")
    return f"""(({source_match}) OR
    (JSON_VALUE(@old_removal,'$.source_id') IS NULL AND @remove_source_id IS NULL))
AND (({proposal_match}) OR
    (JSON_VALUE(@old_removal,'$.proposal_id') IS NULL AND @remove_proposal_id IS NULL))
AND {exact_text_equal("JSON_VALUE(@old_removal,'$.binding_hash')", 'CONVERT(nvarchar(64),@remove_binding_hash)')}
AND {exact_text_equal("JSON_VALUE(@old_removal,'$.detail')", "JSON_VALUE(@removal_intent,'$.detail')")}"""


def prepare_removals_sql(names: SqlNames) -> str:
    return f"""{retained_ownership_sql(names)}
DECLARE @removal_intents nvarchar(max)=COALESCE(JSON_QUERY(@plan,'$.source_removals'),N'[]'),
    @removals nvarchar(max),@retired_json nvarchar(max)=N'[]';
DECLARE @pending_removals TABLE (
    removal_id nvarchar(36) PRIMARY KEY,source_id nvarchar(256) COLLATE Latin1_General_100_BIN2,proposal_id nvarchar(36),
    node_name nvarchar(256) NOT NULL,binding_json nvarchar(max) NOT NULL,payload nvarchar(max) NOT NULL);
DECLARE @retired TABLE (removal_id nvarchar(36) PRIMARY KEY,payload nvarchar(max) NOT NULL);
DECLARE @candidate_nodes TABLE (node_name nvarchar(256));
IF LEFT(LTRIM(@removal_intents),1)<>'[' OR (SELECT COUNT(*) FROM OPENJSON(@removal_intents))>1000
   OR EXISTS (SELECT 1 FROM OPENJSON(@plan) WHERE [key]='source_removals' AND type<>4)
   OR EXISTS (SELECT 1 FROM OPENJSON(@removal_intents) WHERE type<>5)
    THROW 51073, 'Source removals must be a bounded typed intent array', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@removal_intents) AS requested WHERE
    (SELECT COUNT(*) FROM OPENJSON(requested.value))<>4
    OR EXISTS (SELECT 1 FROM (
        SELECT N'removal_id' AS field_name UNION ALL SELECT N'source_id'
        UNION ALL SELECT N'proposal_id' UNION ALL SELECT N'detail'
    ) AS required_field WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(requested.value) AS supplied_field
        WHERE {exact_text_equal('supplied_field.[key]', 'required_field.field_name')}))
    OR EXISTS (SELECT [key] COLLATE Latin1_General_100_BIN2 FROM OPENJSON(requested.value)
        GROUP BY [key] COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1)
    OR EXISTS (SELECT 1 FROM OPENJSON(requested.value)
        WHERE [key] COLLATE Latin1_General_100_BIN2 NOT IN ('removal_id','source_id','proposal_id','detail')
           OR ([key] IN ('removal_id','detail') AND type<>1)
           OR ([key] IN ('source_id','proposal_id') AND type NOT IN (0,1)))
    OR NOT ({canonical_guid("JSON_VALUE(requested.value,'$.removal_id')")})
    OR ((JSON_VALUE(requested.value,'$.source_id') IS NULL AND JSON_VALUE(requested.value,'$.proposal_id') IS NULL)
        OR (JSON_VALUE(requested.value,'$.source_id') IS NOT NULL AND JSON_VALUE(requested.value,'$.proposal_id') IS NOT NULL))
    OR (JSON_VALUE(requested.value,'$.source_id') IS NOT NULL
        AND DATALENGTH(JSON_VALUE(requested.value,'$.source_id')) NOT BETWEEN 1 AND 512)
    OR (JSON_VALUE(requested.value,'$.proposal_id') IS NOT NULL
        AND NOT ({canonical_guid("JSON_VALUE(requested.value,'$.proposal_id')")}))
    OR NULLIF(JSON_VALUE(requested.value,'$.detail'),'') IS NULL
    OR DATALENGTH(JSON_VALUE(requested.value,'$.detail'))>4000)
    THROW 51073, 'A removal selects exactly one already-owned source or proposal with a stable request ID', 1;
IF EXISTS (SELECT JSON_VALUE(value,'$.removal_id') FROM OPENJSON(@removal_intents)
    GROUP BY JSON_VALUE(value,'$.removal_id') HAVING COUNT(*)>1)
    THROW 51073, 'Removal identities must be unique', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@prior,'$.source_removals') AS old_removal
    WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(@removal_intents) AS requested
        WHERE JSON_VALUE(requested.value,'$.removal_id')=JSON_VALUE(old_removal.value,'$.removal_id')))
    THROW 51072, 'Pending removal cannot be cancelled by omitting its ownership record', 1;
DECLARE @removal_intent nvarchar(max),@removal_id nvarchar(36),@remove_source_id nvarchar(256),
    @remove_proposal_id nvarchar(36),@old_removal nvarchar(max),@remove_binding nvarchar(max),
    @remove_node_name nvarchar(256),@remove_payload nvarchar(max),@remove_binding_hash char(64),
    @remove_physical_id nvarchar(256);
DECLARE removal_requests CURSOR LOCAL FAST_FORWARD FOR SELECT value FROM OPENJSON(@removal_intents);
OPEN removal_requests; FETCH NEXT FROM removal_requests INTO @removal_intent;
WHILE @@FETCH_STATUS=0
BEGIN
    SET @removal_id=JSON_VALUE(@removal_intent,'$.removal_id');
    SET @remove_source_id=JSON_VALUE(@removal_intent,'$.source_id');
    SET @remove_proposal_id=JSON_VALUE(@removal_intent,'$.proposal_id');
    SET @old_removal=NULL; SET @remove_binding=NULL; SET @remove_node_name=NULL; SET @remove_physical_id=NULL;
    SELECT @old_removal=value FROM OPENJSON(@prior,'$.source_removals')
    WHERE JSON_VALUE(value,'$.removal_id')=@removal_id;
    SELECT @remove_binding=value FROM ({removal_binding_sql()}) AS owned_binding;
    IF @remove_binding IS NULL
        THROW 51072, 'Removal cannot target an unowned source or logical proposal', 1;
    SET @remove_binding_hash={payload_hash('@remove_binding')};
    IF @old_removal IS NOT NULL
    BEGIN
        IF NOT ({original_removal_matches_sql()})
            THROW 51072, 'An original removal identity cannot be rebound or rewritten', 1;
        SET @remove_node_name=JSON_VALUE(@old_removal,'$.node_name'); SET @remove_payload=@old_removal;
    END
    ELSE
    BEGIN
        IF @binding_receipt_id IS NOT NULL
            THROW 51072, 'An old observation cannot authorize a new removal intent', 1;
        IF EXISTS (SELECT 1 FROM {names.table('monitoring_records')} WHERE tenant_id=@tenant_id AND epoch=@epoch
            AND record_kind='connector_source_retirement'
            AND full_key=@connector_id+N':removal:'+@removal_id)
            THROW 51072, 'A retired removal identity cannot be reused', 1;
        IF @remove_proposal_id IS NOT NULL SET @remove_node_name=JSON_VALUE(@remove_binding,'$.node_name');
        ELSE
        BEGIN
            DELETE FROM @candidate_nodes;
            INSERT INTO @candidate_nodes SELECT SUBSTRING([key],9,256) COLLATE Latin1_General_100_BIN2 FROM (
                SELECT [key],value,type FROM OPENJSON(@prior,'$.observed_definition.component_ids')
                UNION ALL SELECT [key],value,type FROM OPENJSON(@prior,'$.desired_definition.component_ids')
            ) AS component_ids
            WHERE [key] COLLATE Latin1_General_100_BIN2 LIKE 'sources/%'
              AND type=1 AND DATALENGTH([key]) BETWEEN 18 AND 528
              AND {exact_text_equal('value', '@remove_source_id')}
            GROUP BY SUBSTRING([key],9,256) COLLATE Latin1_General_100_BIN2,DATALENGTH([key]);
            IF (SELECT COUNT(*) FROM @candidate_nodes)<>1
                THROW 51072, 'Physical removal requires its exact existing ID-to-node ownership mapping', 1;
            SELECT @remove_node_name=node_name FROM @candidate_nodes;
        END;
        IF NULLIF(@remove_node_name,'') IS NULL
            THROW 51072, 'Owned removal has no stable node identity', 1;
        SELECT @remove_physical_id=value FROM OPENJSON(@prior,'$.observed_definition.component_ids')
        WHERE {exact_text_equal('[key]', "N'sources/'+@remove_node_name")}
          AND type=1 AND {canonical_guid('value')};
        SET @remove_payload=(SELECT @removal_id AS removal_id,@remove_source_id AS source_id,
            @remove_proposal_id AS proposal_id,@remove_node_name AS node_name,
            @remove_physical_id AS last_observed_source_id,
            JSON_QUERY(@remove_binding,'$.target') AS target,@remove_binding_hash AS binding_hash,
            @current_revision AS policy_revision,@request_id AS request_id,@publication_id AS publication_id,
            CONVERT(nvarchar(40),@now,127)+N'Z' AS requested_at,
            JSON_VALUE(@removal_intent,'$.detail') AS detail,'pending_remote_absence' AS state
            FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
    END;
    INSERT INTO @pending_removals VALUES (@removal_id,@remove_source_id,@remove_proposal_id,
        @remove_node_name,@remove_binding,@remove_payload);
    FETCH NEXT FROM removal_requests INTO @removal_intent;
END;
CLOSE removal_requests; DEALLOCATE removal_requests;
IF EXISTS (SELECT source_id FROM @pending_removals WHERE source_id IS NOT NULL
    GROUP BY source_id,DATALENGTH(source_id) HAVING COUNT(*)>1)
   OR EXISTS (SELECT proposal_id FROM @pending_removals WHERE proposal_id IS NOT NULL GROUP BY proposal_id HAVING COUNT(*)>1)
    THROW 51073, 'One owned source/proposal cannot have multiple pending removal identities', 1;
SET @removals=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),payload),N',')
    WITHIN GROUP (ORDER BY removal_id) FROM @pending_removals),N'')+N']';"""


def remote_absence_predicate() -> str:
    """Both the exact node and physical ID must disappear from the original observation."""
    return f"""NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition.component_ids') AS component
    WHERE {exact_text_equal('component.[key]', "N'sources/'+@remove_node_name")}
       OR (@remove_source_id IS NOT NULL AND {exact_text_equal('component.value', '@remove_source_id')})
       OR (@remove_physical_id IS NOT NULL AND {exact_text_equal('component.value', '@remove_physical_id')}))
AND NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json".sources') AS source_node
    WHERE {exact_text_equal("JSON_VALUE(source_node.value,'$.name')", '@remove_node_name')}
       OR (@remove_source_id IS NOT NULL AND {exact_text_equal("JSON_VALUE(source_node.value,'$.id')", '@remove_source_id')})
       OR (@remove_physical_id IS NOT NULL AND {exact_text_equal("JSON_VALUE(source_node.value,'$.id')", '@remove_physical_id')}))
AND NOT EXISTS (SELECT 1 FROM OPENJSON(@binding_observation,'$.observed_definition.parts."eventstream.json".streams') AS stream
    CROSS APPLY OPENJSON(stream.value,'$.inputNodes') AS input_node
    WHERE {exact_text_equal("JSON_VALUE(input_node.value,'$.name')", '@remove_node_name')})"""


def confirm_removals_sql(names: SqlNames) -> str:
    return f"""DECLARE confirmed_removals CURSOR LOCAL FAST_FORWARD FOR
    SELECT removal_id,source_id,proposal_id,node_name,binding_json,payload FROM @pending_removals;
OPEN confirmed_removals;
FETCH NEXT FROM confirmed_removals INTO @removal_id,@remove_source_id,@remove_proposal_id,@remove_node_name,@remove_binding,@old_removal;
WHILE @@FETCH_STATUS=0
BEGIN
    SET @remove_physical_id=JSON_VALUE(@old_removal,'$.last_observed_source_id');
    IF NOT ({remote_absence_predicate()})
        THROW 51072, 'Original observation has not proved exact remote absence; physical ownership is retained', 1;
    DECLARE @retirement nvarchar(max)=(SELECT @connector_id AS connector_id,@ownership_id AS ownership_id,
        @removal_id AS removal_id,@remove_source_id AS source_id,@remove_proposal_id AS proposal_id,
        @remove_node_name AS node_name,JSON_QUERY(@remove_binding) AS original_binding,
        JSON_QUERY(@old_removal) AS original_removal,@binding_receipt_id AS observation_receipt_id,
        @binding_receipt_fingerprint AS observation_fingerprint,
        JSON_VALUE(@binding_receipt_payload,'$.binding_hash') AS observation_binding_hash,
        {payload_hash('@binding_receipt_payload')} AS observation_receipt_hash,
        {payload_hash("JSON_QUERY(@binding_observation,'$.observed_definition')")} AS observed_definition_hash,
        @request_id AS confirmation_request_id,@work_id AS work_id,@fence AS work_fence,
        @current_revision AS policy_revision,CONVERT(nvarchar(40),@now,127)+N'Z' AS retired_at,
        'retired_verified' AS state
        FOR JSON PATH,INCLUDE_NULL_VALUES,WITHOUT_ARRAY_WRAPPER);
    INSERT INTO @retired VALUES (@removal_id,@retirement);
    FETCH NEXT FROM confirmed_removals INTO @removal_id,@remove_source_id,@remove_proposal_id,@remove_node_name,@remove_binding,@old_removal;
END;
CLOSE confirmed_removals; DEALLOCATE confirmed_removals;
SET @sources=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),s.value),N',')
    WITHIN GROUP (ORDER BY CONVERT(int,s.[key])) FROM OPENJSON(@sources) AS s
    WHERE NOT EXISTS (SELECT 1 FROM @pending_removals AS removal JOIN @retired AS retired
        ON retired.removal_id=removal.removal_id
        WHERE {exact_text_equal("JSON_VALUE(s.value,'$.source_id')", 'removal.source_id')})),N'')+N']';
SET @proposals=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),p.value),N',')
    WITHIN GROUP (ORDER BY CONVERT(int,p.[key])) FROM OPENJSON(@proposals) AS p
    WHERE NOT EXISTS (SELECT 1 FROM @pending_removals AS removal JOIN @retired AS retired
        ON retired.removal_id=removal.removal_id
        WHERE JSON_VALUE(p.value,'$.proposal_id')=removal.proposal_id)),N'')+N']';
SET @retired_json=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),payload),N',')
    WITHIN GROUP (ORDER BY removal_id) FROM @retired),N'')+N']';
DELETE FROM @pending_removals WHERE removal_id IN (SELECT removal_id FROM @retired);
SET @removals=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),payload),N',')
    WITHIN GROUP (ORDER BY removal_id) FROM @pending_removals),N'')+N']';"""


def retirement_records_sql(names: SqlNames) -> str:
    return f"""INSERT INTO {names.table('monitoring_records')}
    (tenant_id,epoch,record_kind,key_hash,full_key,revision,parent_hash,parent_key,payload)
SELECT @tenant_id,@epoch,'connector_source_retirement',
    {key_hash("@connector_id+N':removal:'+retired.removal_id")},
    @connector_id+N':removal:'+retired.removal_id,1,{key_hash('@connector_id')},@connector_id,retired.payload
FROM @retired AS retired;"""


def desired_source_projection_sql() -> str:
    return f"""SET @desired_sources=N'['+COALESCE((SELECT STRING_AGG(CONVERT(nvarchar(max),value),N',')
    WITHIN GROUP (ORDER BY ownership_order,position) FROM (
    SELECT value,0 AS ownership_order,CONVERT(int,[key]) AS position FROM OPENJSON(@sources) AS owned_source
    WHERE NOT {source_is_pending_removal_sql('owned_source.value', '@removals')}
    UNION ALL SELECT value,1,CONVERT(int,[key]) FROM OPENJSON(@proposals) AS proposal
    WHERE NOT {source_is_pending_removal_sql('proposal.value', '@removals')}
) AS desired_set),N'')+N']';"""
