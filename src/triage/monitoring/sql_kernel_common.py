"""Static SQL composition; identifiers come only from the validated kernel map."""

from __future__ import annotations

from collections.abc import Iterable

from triage.monitoring.sql_kernel_contracts import (
    COMPONENTS,
    KERNEL_VERSION,
    RECORD_COLUMNS,
    KernelObject,
    RpcContract,
    SqlNames,
)


def literals(values: Iterable[str]) -> str:
    return ", ".join("N'" + value.replace("'", "''") + "'" for value in values)


def key_hash(expression: str) -> str:
    if expression == "NULL":
        return "NULL"
    return (
        "HASHBYTES('SHA2_256', CONVERT(varchar(max), "
        f"({expression}) COLLATE Latin1_General_100_BIN2_UTF8))"
    )


def payload_hash(expression: str) -> str:
    """Internal accepted-evidence hash over SQL NVARCHAR bytes, not API fingerprint."""
    return f"CONVERT(char(64), HASHBYTES('SHA2_256', CONVERT(varbinary(max), {expression})), 2)"


def exact_text_equal(left: str, right: str) -> str:
    """Opaque text equality, including SQL's normally ignored trailing padding."""
    return (
        f"({left} IS NOT NULL AND {right} IS NOT NULL "
        f"AND DATALENGTH({left})=DATALENGTH({right}) "
        f"AND {left} COLLATE Latin1_General_100_BIN2="
        f"{right} COLLATE Latin1_General_100_BIN2)"
    )


def record_hash(alias: str) -> str:
    """Bind promoted routing fields as well as payload, including exact nulls.

    Each field contributes a fixed-width hash or a non-hex null marker. A
    payload-only hash would let a view writer silently move accepted evidence
    by changing target/generation/status columns without a new batch.
    """
    values = []
    for column in RECORD_COLUMNS:
        value = f"{alias}.[{column}]"
        if column.endswith("_hash"):
            value = f"CONVERT(nvarchar(max),{value},2)"
        elif column == "due_at":
            value = f"CONVERT(nvarchar(max),{value},126)"
        else:
            value = f"CONVERT(nvarchar(max),{value})"
        values.append(f"COALESCE({payload_hash(value)},REPLICATE('-',64))")
    return payload_hash("CONCAT(CAST(N'' AS nvarchar(max))," + ",".join(values) + ")")


def receipt_content_expression(expression: str) -> str:
    """Transport redelivery changes receive time/position, not original event content."""
    stable = (
        f"JSON_MODIFY(JSON_MODIFY(JSON_MODIFY({expression},'$.received_at',NULL),"
        "'$.partition',NULL),'$.position',NULL)"
    )
    return (
        f"CASE WHEN JSON_QUERY({expression},'$.observation') IS NULL THEN {stable} "
        f"ELSE JSON_MODIFY({stable},'$.observation.observed_at',NULL) END"
    )


def receipt_content_hash(expression: str) -> str:
    return payload_hash(receipt_content_expression(expression))


def canonical_guid(expression: str) -> str:
    """SQL predicate for the existing nonempty, lower-case UUID convention."""
    return (
        f"TRY_CONVERT(uniqueidentifier,{expression}) IS NOT NULL "
        f"AND DATALENGTH({expression})=72 "
        f"AND {expression} COLLATE Latin1_General_100_BIN2="
        f"LOWER(CONVERT(nvarchar(36),TRY_CONVERT(uniqueidentifier,{expression}))) "
        "COLLATE Latin1_General_100_BIN2 "
        f"AND {expression}<>N'00000000-0000-0000-0000-000000000000'"
    )


def reply(operation: str) -> str:
    return f"""SELECT (
    SELECT {KERNEL_VERSION} AS kernel_version, N'{operation}' AS operation, @status AS status,
           @affected AS affected_rows, JSON_QUERY(COALESCE(@result, N'{{}}')) AS result
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
) AS result_json;"""


def role_guard(names: SqlNames, contract: RpcContract) -> str:
    one = " + ".join(
        f"CASE WHEN IS_ROLEMEMBER(N'{names.role(component)}') = 1 THEN 1 ELSE 0 END"
        for component in COMPONENTS
    )
    allowed = " OR ".join(
        f"IS_ROLEMEMBER(N'{names.role(component)}') = 1" for component in contract.components
    )
    return f"""IF ({one}) <> 1 OR NOT ({allowed})
    THROW 51070, 'SQL kernel component authority is absent or ambiguous', 1;"""


def procedure(
    names: SqlNames, contract: RpcContract, body: str, *,
    replay: bool = False, permit_maintenance: bool = False,
    check_revision: bool = True,
) -> KernelObject:
    params = ",\n    ".join(f"@{p.name} {p.sql_type}" for p in contract.parameters)
    required = "\n".join(
        f"IF @{p.name} IS NULL THROW 51073, 'Required SQL kernel parameter is null', 1;"
        for p in contract.parameters if not p.nullable
    )
    transaction = (
        "IF @@TRANCOUNT = 0 OR XACT_STATE() <> 1\n"
        "    THROW 51071, 'An active caller-owned SQL transaction is required', 1;"
        if contract.mutating else ""
    )
    hints = " WITH (UPDLOCK, HOLDLOCK)" if contract.mutating else ""
    context = f"""DECLARE @now datetime2(6) = SYSUTCDATETIME(),
    @current_revision bigint, @maintenance bit, @cutoff datetime2(6),
    @status varchar(20) = 'applied', @affected int = 0, @result nvarchar(max) = N'{{}}';
SELECT @current_revision=revision, @maintenance=maintenance, @cutoff=activation_cutoff
FROM {names.table('monitoring_control')}{hints}
WHERE singleton=1 AND tenant_id=@tenant_id AND epoch=@epoch;
IF @current_revision IS NULL
    THROW 51071, 'SQL kernel tenant or epoch does not match current control', 1;
SET @now=SYSUTCDATETIME();"""
    replay_sql = ""
    if replay:
        binding_columns = ", ".join(
            f"@{p.name} AS [{p.name}]" for p in contract.parameters
            if p.name not in {"request_id", "fingerprint"}
        )
        replay_sql = f"""IF NOT ({canonical_guid('@request_id')})
    THROW 51073, 'Operation request must be a canonical nonempty GUID', 1;
IF LEN(@fingerprint)<>64 OR @fingerprint COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9a-f]%'
    THROW 51073, 'A canonical original request fingerprint is required', 1;
DECLARE @binding_json nvarchar(max) = (SELECT {binding_columns} FOR JSON PATH, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER);
DECLARE @binding_hash char(64) = {payload_hash('@binding_json')},
    @prior_fingerprint char(64), @prior_payload nvarchar(max), @prior_request_id nvarchar(256);
SELECT @prior_fingerprint=fingerprint, @prior_payload=payload,@prior_request_id=request_id
FROM {names.table('monitoring_receipts')}
WHERE tenant_id=@tenant_id AND epoch=@epoch
  AND operation=N'{contract.operation}' AND request_hash={key_hash('@request_id')};
IF @prior_payload IS NOT NULL
BEGIN
    IF @prior_fingerprint<>@fingerprint OR @prior_request_id<>@request_id
       OR COALESCE(JSON_VALUE(@prior_payload, '$.binding_hash'),'')<>@binding_hash
       OR JSON_QUERY(@prior_payload,'$.result') IS NULL
        THROW 51072, 'Original operation identity was reused with different input', 1;
    SET @result=JSON_QUERY(@prior_payload, '$.result');
    SET @status='replayed';
    {reply(contract.operation)}
    RETURN;
END;"""
    revision = ""
    if check_revision and any(p.name == "expected_revision" for p in contract.parameters):
        revision = """IF @current_revision<>@expected_revision
    THROW 51072, 'Current policy revision differs from this new operation', 1;"""
    maintenance = "" if permit_maintenance or not contract.mutating else (
        "IF @maintenance=1 THROW 51071, 'Maintenance forbids this new operation', 1;"
    )
    # Original-receipt reconciliation precedes new-operation revision/maintenance checks.
    ddl = f"""CREATE OR ALTER PROCEDURE {contract.object_name}
    {params}
AS
BEGIN
SET NOCOUNT ON;
SET XACT_ABORT ON;
{role_guard(names, contract)}
{required}
{transaction}
IF NOT ({canonical_guid('@tenant_id')}) OR NOT ({canonical_guid('@epoch')})
    THROW 51073, 'Tenant and epoch must use canonical lower-case GUID identities', 1;
{context}
{replay_sql}
{revision}
{maintenance}
{body}
{reply(contract.operation)}
END;"""
    return KernelObject(contract.operation, contract.object_name, "procedure", ddl)


def save_receipt(names: SqlNames, operation: str) -> str:
    return f"""INSERT INTO {names.table('monitoring_receipts')}
    (tenant_id,epoch,operation,request_hash,request_id,fingerprint,recorded_at,payload)
VALUES (@tenant_id,@epoch,N'{operation}',{key_hash('@request_id')},@request_id,
    @fingerprint,@now,
    (SELECT @binding_hash AS binding_hash, JSON_QUERY(@result) AS result
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER));"""


def record_insert(
    names: SqlNames, kind: str, key: str, payload: str, *,
    status: str = "NULL", work_kind: str = "NULL", due_at: str = "NULL",
    parent_key: str = "NULL", sequence: str = "NULL", target_key: str = "NULL",
    workspace: str = "NULL", item: str = "NULL", workload: str = "NULL",
    revision: str = "1",
) -> str:
    return f"""INSERT INTO {names.table('monitoring_records')}
    (tenant_id,epoch,record_kind,key_hash,full_key,revision,status,workload,
     workspace_id,item_id,target_hash,target_key,parent_hash,parent_key,
     work_kind,due_at,sequence_number,payload)
VALUES (@tenant_id,@epoch,N'{kind}',{key_hash(key)},{key},{revision},{status},{workload},
    {workspace},{item},{key_hash(target_key)},{target_key},{key_hash(parent_key)},{parent_key},
    {work_kind},{due_at},{sequence},{payload});"""


def current_work(
    names: SqlNames, families: Iterable[str], *, live_lease: bool = True,
    target_lease: bool = False,
) -> str:
    lease_check = ""
    if live_lease:
        lease_check = f"""IF NOT EXISTS (
    SELECT 1 FROM {names.table('monitoring_leases')} WITH (UPDLOCK,HOLDLOCK)
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@work_key
      AND key_hash={key_hash('@work_key')} AND owner_id=@owner_id AND fence=@fence
      AND expires_at>@now
) THROW 51074, 'Current work owner or fence was lost', 1;"""
    target_check = ""
    if target_lease:
        target_check = f"""IF @stored_target_key IS NULL OR NOT EXISTS (
    SELECT 1 FROM {names.table('monitoring_leases')} WITH (UPDLOCK,HOLDLOCK)
    WHERE tenant_id=@tenant_id AND epoch=@epoch
      AND full_key=N'controller:'+@stored_target_key
      AND key_hash={key_hash("N'controller:'+@stored_target_key")}
      AND owner_id=@work_id AND expires_at>@now
) THROW 51074, 'Current controller target ownership was lost', 1;"""
    return f"""IF NOT ({canonical_guid('@work_id')}) OR NOT ({canonical_guid('@owner_id')})
    THROW 51073, 'Work and lease owner must be canonical nonempty GUIDs', 1;
DECLARE @work_key nvarchar(1024)=N'work:v1:'+@epoch+N':'+@tenant_id+N':'+@work_id,
    @stored_work nvarchar(max), @stored_work_kind varchar(32), @stored_work_revision bigint,
    @stored_work_status varchar(40), @stored_target_key nvarchar(1024);
SELECT @stored_work=payload,@stored_work_kind=work_kind,
       @stored_work_revision=revision,@stored_work_status=status,@stored_target_key=target_key
FROM {names.table('monitoring_records')} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='work'
  AND key_hash={key_hash('@work_id')} AND full_key=@work_id;
SET @now=SYSUTCDATETIME();
IF @stored_work IS NULL OR @stored_work_kind NOT IN ({literals(families)})
    THROW 51070, 'Stored work is not in this component family', 1;
IF COALESCE(JSON_VALUE(@stored_work,'$.kind'),'')<>@stored_work_kind
   OR COALESCE(JSON_VALUE(@stored_work,'$.work_id'),'')<>@work_id
   OR COALESCE(JSON_VALUE(@stored_work,'$.tenant_id'),'')<>@tenant_id
   OR COALESCE(JSON_VALUE(@stored_work,'$.epoch'),'')<>@epoch
    THROW 51073, 'Work promoted columns and payload disagree', 1;
IF @stored_work_kind='reconcile_state' AND (
    EXISTS (SELECT 1 FROM OPENJSON(@stored_work) WHERE [key]='execution' AND type<>0)
    OR JSON_VALUE(@stored_work,'$.action_reservation_id') IS NOT NULL
    OR JSON_VALUE(@stored_work,'$.retry_of') IS NOT NULL
    OR JSON_VALUE(@stored_work,'$.finalization_id') IS NOT NULL
    OR COALESCE(TRY_CONVERT(int,JSON_VALUE(@stored_work,'$.retry_attempt')),-1)<>0
    OR COALESCE(JSON_VALUE(@stored_work,'$.reconcile_producer'),'') NOT IN ('worker','web')
    OR NOT ({canonical_guid("JSON_VALUE(@stored_work,'$.reconcile_request_id')")}))
    THROW 51070, 'Reconciliation is not an action-capable work family', 1;
{lease_check}
{target_check}"""


def partition_identity(names: SqlNames) -> str:
    # SQL LIKE needs a leading hyphen here to keep it literal.
    pair_sql = """N'["'+@consumer_group+N'","'+@partition_id+N'"]'"""
    return f"""IF NOT ({canonical_guid('@connector_id')})
    THROW 51073, 'Connector must be a canonical nonempty GUID', 1;
IF @consumer_group COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9$_.]%'
   OR LEN(@consumer_group) NOT BETWEEN 1 AND 50
   OR @partition_id COLLATE Latin1_General_100_BIN2 LIKE '%[^0-9]%'
   OR LEN(@partition_id) NOT BETWEEN 1 AND 32
    THROW 51073, 'Unsupported broker group or partition identity', 1;
DECLARE @connector nvarchar(max),@connector_revision bigint;
SELECT @connector=payload,@connector_revision=revision
FROM {names.table('monitoring_records')}
WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='connector'
  AND full_key=@connector_id AND key_hash={key_hash('@connector_id')};
IF @connector IS NULL OR NOT {exact_text_equal("JSON_VALUE(@connector,'$.endpoint.consumer_group')", '@consumer_group')}
    THROW 51072, 'Partition does not belong to the owned connector', 1;
DECLARE @partition_digest char(64)=LOWER(CONVERT(char(64),
    {key_hash(pair_sql)},2));
DECLARE @partition_key nvarchar(1024)=N'partition:v1:'+@epoch+N':'+@tenant_id+N':'+@connector_id+N':'+@partition_digest;
DECLARE @partition_json nvarchar(max)=(SELECT @tenant_id AS tenant_id,@epoch AS epoch,
    @connector_id AS connector_id,@consumer_group AS consumer_group,@partition_id AS partition_id
    FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);"""


def partition_owner(names: SqlNames) -> str:
    return f"""    IF COALESCE(JSON_VALUE(@connector,'$.state'),'') NOT IN ('ready','degraded')
    THROW 51071, 'Connector is not enabled for intake', 1;
IF NOT EXISTS (
    SELECT 1 FROM {names.table('monitoring_leases')} WITH (UPDLOCK,HOLDLOCK)
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND full_key=@partition_key
      AND key_hash={key_hash('@partition_key')} AND owner_id=@owner_id AND fence=@fence
      AND expires_at>@now
) THROW 51074, 'Partition lease owner or fence was lost', 1;"""
