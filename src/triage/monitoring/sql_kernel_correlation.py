"""Immutable submitted-execution correlation, never a no-effect inference."""

from __future__ import annotations

from triage.monitoring.sql_kernel_common import key_hash, record_insert
from triage.monitoring.sql_kernel_contracts import SqlNames


def execution_reservations_sql(names: SqlNames) -> str:
    equal = names.object("json_equal")
    return f"""SELECT 1 FROM {names.table('monitoring_records')} AS reservation
WHERE reservation.tenant_id=@tenant_id AND reservation.epoch=@epoch AND reservation.record_kind='action'
  AND ({equal}(JSON_QUERY(reservation.payload,'$.request.source_execution'),@execution)=1
       OR {equal}(JSON_QUERY(reservation.payload,'$.submitted_execution'),@execution)=1)"""


def submitted_collision_sql(names: SqlNames) -> str:
    return f"""SELECT 1 FROM {names.table('monitoring_records')} AS another
WHERE another.tenant_id=@tenant_id AND another.epoch=@epoch AND another.record_kind='action'
  AND another.full_key<>@reservation_id
  AND {names.object('json_equal')}(JSON_QUERY(another.payload,'$.submitted_execution'),@correlated_execution)=1"""


def correlation_insert_sql(names: SqlNames) -> str:
    return record_insert(
        names, "submitted_action", "@correlated_key", "@correlation_payload",
        parent_key="@reservation_id", target_key="@stored_target_key",
    )


def record_submitted_correlation_sql(names: SqlNames) -> str:
    records = names.table("monitoring_records")
    return f"""DECLARE @correlated_execution nvarchar(max)=JSON_QUERY(@action,'$.submitted_execution');
IF @correlated_execution IS NOT NULL
BEGIN
    IF @transition='rejected'
        THROW 51072, 'A correlated submitted execution is not a no-effect rejection', 1;
    DECLARE @correlated_key nvarchar(1024)=@stored_target_key+N':run:'
        +JSON_VALUE(@correlated_execution,'$.run_id_kind')+N':'+JSON_VALUE(@correlated_execution,'$.run_id'),
        @existing_correlation nvarchar(max),@correlation_payload nvarchar(max);
    IF EXISTS ({submitted_collision_sql(names)})
        THROW 51072, 'Submitted execution already belongs to another action, even without an index', 1;
    SELECT @existing_correlation=payload FROM {records} WITH (UPDLOCK,HOLDLOCK)
    WHERE tenant_id=@tenant_id AND epoch=@epoch AND record_kind='submitted_action'
      AND full_key=@correlated_key AND key_hash={key_hash('@correlated_key')};
    IF @existing_correlation IS NOT NULL AND (
        COALESCE(JSON_VALUE(@existing_correlation,'$.reservation_id'),'')<>@reservation_id
        OR COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@existing_correlation,'$.fence')),-1)
            <>COALESCE(TRY_CONVERT(bigint,JSON_VALUE(@action,'$.fence')),-2)
        OR COALESCE(JSON_VALUE(@existing_correlation,'$.active'),'')<>'true')
        THROW 51072, 'Submitted correlation is immutable and cannot be rebound', 1;
    IF @existing_correlation IS NULL
    BEGIN
        SET @correlation_payload=(SELECT @reservation_id AS reservation_id,
            TRY_CONVERT(bigint,JSON_VALUE(@action,'$.fence')) AS fence,CAST(1 AS bit) AS active
            FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
        {correlation_insert_sql(names)}
        IF @@ROWCOUNT<>1 THROW 51072, 'Submitted correlation insertion was not confirmed', 1;
    END;
END;"""
