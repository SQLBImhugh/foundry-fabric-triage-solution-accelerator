"""Closed tool-argument canonicalization, separate from reviewed parameters.

Only the four supported action schemas are accepted. This does not canonicalize
arbitrary JSON or trust a caller's digest. Approval arguments retain every field.
"""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import canonical_guid, exact_text_equal, key_hash
from triage.monitoring.sql_kernel_contracts import KernelObject, SqlNames

PIPELINE_ARGUMENT_FIELDS = (
    "failed_run_id", "justification", "parameter_hash", "parameter_preview", "pipeline_id", "workspace_id",
)


def pipeline_shape_predicate() -> str:
    fields = ",".join(f"'{name}'" for name in PIPELINE_ARGUMENT_FIELDS)
    return f"""(SELECT COUNT(*) FROM OPENJSON(@arguments))=6
AND NOT EXISTS (SELECT 1 FROM OPENJSON(@arguments)
    WHERE type<>1 OR [key] COLLATE Latin1_General_100_BIN2 NOT IN ({fields}))
AND NOT EXISTS (SELECT [key] COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@arguments)
    GROUP BY [key] COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1)"""


def pipeline_identity_predicate() -> str:
    """Preview is retained as approval text, never compared as execution parameters."""
    return """COALESCE(JSON_VALUE(@request,'$.source_execution.target.workload'),'')='fabric_pipeline'
AND COALESCE(JSON_VALUE(@request,'$.source_execution.run_id_kind'),'')='fabric_job'
AND COALESCE(JSON_VALUE(@full_arguments,'$.workspace_id'),'')
    =COALESCE(JSON_VALUE(@request,'$.source_execution.target.workspace_id'),'missing')
AND COALESCE(JSON_VALUE(@full_arguments,'$.pipeline_id'),'')
    =COALESCE(JSON_VALUE(@request,'$.source_execution.target.item_id'),'missing')
AND COALESCE(JSON_VALUE(@full_arguments,'$.failed_run_id'),'')
    =COALESCE(JSON_VALUE(@request,'$.source_execution.run_id'),'missing')
AND COALESCE(JSON_VALUE(@full_arguments,'$.parameter_hash'),'')=@parameter_hash"""


def pipeline_canonical_expression(names: SqlNames) -> str:
    quote = names.object("json_identity_string")
    parts = []
    for field in PIPELINE_ARGUMENT_FIELDS:
        value = "@justification" if field == "justification" else (
            "@parameter_preview" if field == "parameter_preview" else f"JSON_VALUE(@arguments,'$.{field}')"
        )
        parts.append(f"N'\"{field}\":'+{quote}({value})")
    return "N'{'+" + "+N','+".join(parts) + "+N'}'"


def argument_function(names: SqlNames) -> KernelObject:
    name = names.object("canonical_action_arguments")
    quote = names.object("json_identity_string")
    ddl = f"""CREATE OR ALTER FUNCTION {name} (@action varchar(40),@arguments nvarchar(max))
RETURNS nvarchar(max)
WITH SCHEMABINDING
AS
BEGIN
    IF @arguments IS NULL OR ISJSON(@arguments)<>1 OR LEFT(LTRIM(@arguments),1)<>'{{'
       OR DATALENGTH(@arguments)>131072 RETURN NULL;
    IF @action NOT IN ('powerbi_refresh','pipeline_rerun','rebind_dataset_gateway','reenable_refresh_schedule')
        RETURN NULL;
    IF EXISTS (SELECT [key] COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@arguments)
        GROUP BY [key] COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1) RETURN NULL;
    IF EXISTS (SELECT 1 FROM OPENJSON(@arguments) WHERE [key] COLLATE Latin1_General_100_BIN2 NOT IN
        ('justification','parameter_hash','configuration','workspace_id','pipeline_id','failed_run_id','parameter_preview')) RETURN NULL;
    IF EXISTS (SELECT 1 FROM OPENJSON(@arguments) WHERE DATALENGTH([key])<>CASE [key] COLLATE Latin1_General_100_BIN2
        WHEN 'justification' THEN 26 WHEN 'parameter_hash' THEN 28 WHEN 'configuration' THEN 26
        WHEN 'workspace_id' THEN 24 WHEN 'pipeline_id' THEN 22 WHEN 'failed_run_id' THEN 26 WHEN 'parameter_preview' THEN 34 END) RETURN NULL;
    IF EXISTS (SELECT 1 FROM OPENJSON(@arguments) WHERE [key]='justification' AND type<>1) RETURN NULL;
    DECLARE @justification nvarchar(max),@has_justification bit=0,@configuration nvarchar(max),
        @canonical_config nvarchar(max),@parameter_preview nvarchar(max),@canonical nvarchar(max)=N'{{';
    SELECT @justification=value,@has_justification=1 FROM OPENJSON(@arguments) WHERE [key]='justification';
    IF @action='powerbi_refresh'
    BEGIN
        IF EXISTS (SELECT 1 FROM OPENJSON(@arguments) WHERE [key]<>'justification') RETURN NULL;
    END
    ELSE IF @action='pipeline_rerun'
    BEGIN
        IF NOT ({pipeline_shape_predicate()}) RETURN NULL;
        IF NOT EXISTS (SELECT 1 FROM OPENJSON(@arguments) WHERE [key]='parameter_hash' AND type=1
            AND DATALENGTH(value)=128 AND value COLLATE Latin1_General_100_BIN2 NOT LIKE '%[^0-9a-f]%') RETURN NULL;
        IF NOT ({canonical_guid("JSON_VALUE(@arguments,'$.workspace_id')")})
           OR NOT ({canonical_guid("JSON_VALUE(@arguments,'$.pipeline_id')")})
           OR NOT ({canonical_guid("JSON_VALUE(@arguments,'$.failed_run_id')")}) RETURN NULL;
        SELECT @parameter_preview=value FROM OPENJSON(@arguments) WHERE [key]='parameter_preview';
        IF DATALENGTH(@parameter_preview)>7200
           OR LEN((@parameter_preview+N'#') COLLATE Latin1_General_100_CI_AS_SC)-1>1800 RETURN NULL;
        RETURN {pipeline_canonical_expression(names)};
    END
    ELSE
    BEGIN
        IF EXISTS (SELECT 1 FROM OPENJSON(@arguments) WHERE [key] NOT IN ('justification','configuration')) RETURN NULL;
        SET @configuration=JSON_QUERY(@arguments,'$.configuration');
        IF @configuration IS NULL OR LEFT(LTRIM(@configuration),1)<>'{{' RETURN NULL;
        IF EXISTS (SELECT [key] FROM OPENJSON(@configuration) GROUP BY [key] HAVING COUNT(*)>1) RETURN NULL;
        IF @action='reenable_refresh_schedule'
        BEGIN
            IF (SELECT COUNT(*) FROM OPENJSON(@configuration))<>1
               OR NOT EXISTS (SELECT 1 FROM OPENJSON(@configuration) WHERE [key] COLLATE Latin1_General_100_BIN2='enabled'
                    AND DATALENGTH([key])=14 AND type=3 AND value='true')
                RETURN NULL;
            SET @canonical_config=N'{{"enabled":true}}';
        END
        ELSE
        BEGIN
            IF (SELECT COUNT(*) FROM OPENJSON(@configuration))<>2
               OR EXISTS (SELECT 1 FROM OPENJSON(@configuration) WHERE [key] COLLATE Latin1_General_100_BIN2 NOT IN ('gateway_id','datasource_ids')
                   OR DATALENGTH([key])<>CASE [key] COLLATE Latin1_General_100_BIN2 WHEN 'gateway_id' THEN 20 WHEN 'datasource_ids' THEN 28 END)
               OR NOT ({canonical_guid("JSON_VALUE(@configuration,'$.gateway_id')")})
               OR JSON_QUERY(@configuration,'$.datasource_ids') IS NULL
               OR LEFT(LTRIM(JSON_QUERY(@configuration,'$.datasource_ids')),1)<>'['
               OR (SELECT COUNT(*) FROM OPENJSON(@configuration,'$.datasource_ids'))=0
                RETURN NULL;
            IF EXISTS (SELECT 1 FROM OPENJSON(@configuration,'$.datasource_ids') WHERE type<>1
                OR NOT ({canonical_guid('value')})) RETURN NULL;
            IF EXISTS (SELECT value COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@configuration,'$.datasource_ids')
                GROUP BY value COLLATE Latin1_General_100_BIN2 HAVING COUNT(*)>1) RETURN NULL;
            IF EXISTS (SELECT 1 FROM OPENJSON(@configuration,'$.datasource_ids') AS a
                JOIN OPENJSON(@configuration,'$.datasource_ids') AS b ON CONVERT(int,b.[key])=CONVERT(int,a.[key])+1
                WHERE a.value COLLATE Latin1_General_100_BIN2>=b.value COLLATE Latin1_General_100_BIN2) RETURN NULL;
            DECLARE @array nvarchar(max);
            SELECT @array=STRING_AGG(CONVERT(nvarchar(max),{quote}(value)),N',')
                WITHIN GROUP (ORDER BY CONVERT(int,[key])) FROM OPENJSON(@configuration,'$.datasource_ids');
            SET @canonical_config=N'{{"datasource_ids":['+@array+N'],"gateway_id":'
                +{quote}(JSON_VALUE(@configuration,'$.gateway_id'))+N'}}';
        END;
        SET @canonical=@canonical+N'"configuration":'+@canonical_config;
    END;
    IF @has_justification=1
        SET @canonical=@canonical+CASE WHEN @canonical=N'{{' THEN N'' ELSE N',' END
            +N'"justification":'+{quote}(@justification);
    RETURN @canonical+N'}}';
END;"""
    return KernelObject("canonical_action_arguments", name, "function", ddl)


def action_arguments_guard(names: SqlNames) -> str:
    canonical = names.object("canonical_action_arguments")
    return f"""DECLARE @full_arguments nvarchar(max)=JSON_QUERY(@request,'$.arguments');
DECLARE @canonical_arguments nvarchar(max)={canonical}(@action,@full_arguments),
    @technical_arguments nvarchar(max),@review_parameters nvarchar(max)=JSON_QUERY(@review,'$.parameters');
IF @canonical_arguments IS NULL
    THROW 51073, 'Full tool arguments contain invalid or extra technical fields', 1;
IF COALESCE(JSON_VALUE(@review,'$.parameters_redacted'),'false')<>'false'
    THROW 51072, 'Redacted reviewed parameters cannot authorize an action', 1;
IF NOT EXISTS (SELECT 1 FROM OPENJSON(@review) WHERE [key]='parameters' AND type IN (0,5))
    THROW 51073, 'Reviewed parameters must explicitly be a JSON object or null', 1;
IF @action='powerbi_refresh'
BEGIN
    IF @review_parameters IS NOT NULL AND
       (LEFT(LTRIM(@review_parameters),1)<>'{{' OR (SELECT COUNT(*) FROM OPENJSON(@review_parameters))<>0)
        THROW 51072, 'Power BI refresh has no reviewed technical parameter overrides', 1;
    SET @technical_arguments=CASE WHEN @review_parameters IS NULL THEN N'null' ELSE N'{{}}' END;
    IF LOWER(CONVERT(char(64),{key_hash('@technical_arguments')},2))<>@parameter_hash
        THROW 51072, 'Power BI null and empty review parameter contracts are distinct', 1;
END
ELSE IF @action='pipeline_rerun'
BEGIN
    IF @review_parameters IS NULL OR LEFT(LTRIM(@review_parameters),1)<>'{{'
       OR NOT ({pipeline_identity_predicate()})
        THROW 51072, 'Pipeline tool hash must select the protected reviewed parameter set', 1;
END
ELSE
BEGIN
    SET @technical_arguments=JSON_QUERY(@canonical_arguments,'$.configuration');
    DECLARE @review_argument_wrapper nvarchar(max)=N'{{"configuration":'+COALESCE(@review_parameters,N'null')+N'}}';
    DECLARE @canonical_review nvarchar(max)={canonical}(@action,@review_argument_wrapper);
    IF @canonical_review IS NULL
       OR NOT {exact_text_equal('@technical_arguments', "JSON_QUERY(@canonical_review,'$.configuration')")}
       OR LOWER(CONVERT(char(64),{key_hash('@technical_arguments')},2))<>@parameter_hash
        THROW 51072, 'Technical configuration differs from the reviewed configuration and hash', 1;
END;
DECLARE @full_arguments_hash char(64)=LOWER(CONVERT(char(64),{key_hash('@canonical_arguments')},2));"""


def approval_arguments_guard(names: SqlNames) -> str:
    canonical = names.object("canonical_action_arguments")
    return f"""DECLARE @canonical_approval_arguments nvarchar(max)={canonical}(@action,JSON_QUERY(@approval,'$.arguments'));
IF @canonical_approval_arguments IS NULL
   OR NOT {exact_text_equal('@canonical_approval_arguments', '@canonical_arguments')}
   OR COALESCE(JSON_VALUE(@binding,'$.arguments_hash'),'')<>@full_arguments_hash
   OR COALESCE(JSON_VALUE(@approval,'$.signature'),'')<>@signature
   OR NULLIF(JSON_VALUE(@approval,'$.responder'),'') IS NULL
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.decided_at')) IS NULL
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.decided_at'))>TODATETIMEOFFSET(@now,'+00:00')
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.decided_at'))
        <TRY_CONVERT(datetimeoffset,JSON_VALUE(@binding,'$.created_at'))
   OR TRY_CONVERT(datetimeoffset,JSON_VALUE(@approval,'$.expires_at'))
        <>TRY_CONVERT(datetimeoffset,JSON_VALUE(@binding,'$.expires_at'))
   OR COALESCE(JSON_VALUE(@approval,'$.action'),'')<>CASE @action
       WHEN 'powerbi_refresh' THEN 'refresh_powerbi_dataset' WHEN 'pipeline_rerun' THEN 'rerun_fabric_pipeline'
       ELSE @action END
    THROW 51072, 'Approval must retain every original tool argument, not only technical parameters', 1;"""
