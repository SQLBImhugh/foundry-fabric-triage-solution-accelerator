"""Controller-only retirement of old inventory pass records.

Every scan adds a pass record, plus a sighting per workspace and item it saw,
and the controller reads every pass record for each admission check. The
controller proposes passes to retire; this procedure re-checks each one against
the same rules before it deletes anything, so a caller cannot choose the
retention window or remove a pass that a reader still needs.
"""

from __future__ import annotations

from triage.monitoring import models as m
from triage.monitoring.sql_kernel_common import (
    canonical_guid,
    key_hash,
    procedure,
    save_receipt,
    varchar_literals,
)
from triage.monitoring.sql_kernel_contracts import KernelObject, RpcContract, SqlNames

RETENTION_DAYS = m.INVENTORY_RETENTION.days
GRACE_SECONDS = int(m.INVENTORY_COLLECTION_GRACE.total_seconds())
SIGHTING_KINDS = ("workspace_seen", "inventory_seen", "domain_seen")
#: Catalogue records that name the pass which last saw them. A deleted item
#: names the complete pass that established its deletion.
REFERENCE_KINDS = ("inventory", "workspace", "domain")
OPEN_WINDOW_STATES = ("collecting", "awaiting_validation")


def inventory_passes_sql(source: str) -> str:
    """One row per pass: the facts retirement compares, read from ``source``.

    ``scan_hash`` identifies the scan (enumeration and selector) with a fixed
    size key. Comparing the selector JSON text itself made eligibility for 25
    passes take about 20 seconds on the MorkNet database.
    """
    return f"""SELECT g.full_key AS generation_id,
    HASHBYTES('SHA2_256',CONCAT(JSON_VALUE(g.payload,'$.enumeration'),N'|',JSON_QUERY(g.payload,'$.selector'))) AS scan_hash,
    TRY_CONVERT(datetime2(6),JSON_VALUE(g.payload,'$.started_at')) AS started_at,
    CASE WHEN JSON_VALUE(g.payload,'$.completed_at') IS NULL THEN 0 ELSE 1 END AS finished,
    CASE WHEN JSON_VALUE(g.payload,'$.completeness')=N'complete' THEN 1 ELSE 0 END AS complete
FROM {source} AS g
WHERE g.tenant_id=@tenant_id AND g.epoch=@epoch AND g.record_kind='generation'"""


def _same_scan(left: str, right: str) -> str:
    return f"{left}.scan_hash={right}.scan_hash"


def _newer(left: str, right: str) -> str:
    """``left`` started after ``right``; pass IDs break ties as the engine does."""
    return (
        f"({left}.started_at>{right}.started_at OR ({left}.started_at={right}.started_at "
        f"AND {left}.generation_id COLLATE Latin1_General_100_BIN2>{right}.generation_id))"
    )


def inventory_retirement_eligible_sql(names: SqlNames, *, passes: str, generation: str, key_hash: str) -> str:
    """True when no reader needs the pass; mirrors MonitoringEngine._inventory_pass_retirable.

    ``passes`` is a relation shaped by inventory_passes_sql, and @retain_after
    and @grace_start are the retention and collection-grace boundaries. A pass
    whose start time cannot be read never qualifies.
    """
    records = names.table("monitoring_records")
    newer_in_scan = f"SELECT 1 FROM {passes} AS n WHERE {_same_scan('n', 'g')} AND {_newer('n', 'g')}"
    return f"""EXISTS (
    SELECT 1 FROM {passes} AS g
    WHERE g.generation_id={generation} AND g.started_at<@retain_after
      AND EXISTS ({newer_in_scan})
      AND (g.finished=0 OR EXISTS ({newer_in_scan} AND n.finished=1))
      AND (g.complete=0 OR EXISTS ({newer_in_scan} AND n.complete=1))
      AND (g.finished=0 OR NOT EXISTS (
          SELECT 1 FROM {passes} AS x
          WHERE {_same_scan('x', 'g')} AND {_newer('x', 'g')} AND x.finished=0 AND x.started_at>=@grace_start
            AND NOT EXISTS (
                SELECT 1 FROM {passes} AS f
                WHERE {_same_scan('f', 'g')} AND f.finished=1 AND {_newer('f', 'g')} AND {_newer('x', 'f')}))))
AND NOT EXISTS (
    SELECT 1 FROM {records} AS r
    WHERE r.tenant_id=@tenant_id AND r.epoch=@epoch AND r.record_kind IN ({varchar_literals(REFERENCE_KINDS)})
      AND r.generation_id={generation})
AND NOT EXISTS (
    SELECT 1 FROM {records} AS w
    WHERE w.tenant_id=@tenant_id AND w.epoch=@epoch AND w.record_kind='work'
      AND w.key_hash={key_hash} AND w.full_key={generation}
      AND w.status IN ({varchar_literals(m.ACTIVE_WORK_STATES)}))
AND NOT EXISTS (
    SELECT 1 FROM {records} AS v
    WHERE v.tenant_id=@tenant_id AND v.epoch=@epoch AND v.record_kind='validation_window'
      AND v.parent_hash={key_hash} AND v.parent_key={generation}
      AND v.status IN ({varchar_literals(OPEN_WINDOW_STATES)}))"""


def _id_array(relation: str, alias: str, where: str = "") -> str:
    """A JSON array of canonical pass IDs, ordered as Python orders strings.

    Table variables use the database collation so they compare with record
    columns without a collation conflict; binary order is applied only here and
    in the comparisons that need it.
    """
    return (
        f"JSON_QUERY(COALESCE((SELECT N'['+STRING_AGG(CONVERT(nvarchar(max),N'\"'+{alias}.generation_id+N'\"'),N',') "
        f"WITHIN GROUP (ORDER BY {alias}.generation_id COLLATE Latin1_General_100_BIN2)+N']' "
        f"FROM {relation} AS {alias}{where}),N'[]'))"
    )


def _retirement_payload(kind: str, rows: str, offset: str) -> str:
    return (
        f"(SELECT @tenant_id AS tenant_id,@epoch AS epoch,{kind} AS record_kind,{rows} AS retired_rows,"
        f"{offset} AS counter_offset,CONVERT(nvarchar(40),@now,127)+N'Z' AS updated_at "
        "FOR JSON PATH,WITHOUT_ARRAY_WRAPPER)"
    )


def inventory_retention_procedures(names: SqlNames, contracts: dict[str, RpcContract]) -> dict[str, KernelObject]:
    contract = contracts["controller.retire_inventory"]
    records = names.table("monitoring_records")
    retirement_key = key_hash("t.record_kind")
    sightings = varchar_literals(SIGHTING_KINDS)
    body = f"""IF @limit NOT BETWEEN 1 AND {m.MAX_INVENTORY_RETIREMENT_BATCH}
    THROW 51073, 'Inventory retirement batch size is outside its fixed bound', 1;
IF ISJSON(@generation_ids_json)<>1 OR LEFT(LTRIM(@generation_ids_json),1)<>N'['
    THROW 51073, 'Inventory retirement requires a JSON array of pass IDs', 1;
IF EXISTS (SELECT 1 FROM OPENJSON(@generation_ids_json) WHERE type<>1 OR NOT ({canonical_guid('value')}))
    THROW 51073, 'Inventory retirement pass IDs must be canonical GUIDs', 1;
DECLARE @requested int=(SELECT COUNT(*) FROM OPENJSON(@generation_ids_json));
-- An empty proposal is a completed operation too: its receipt answers the
-- rest of the interval's heartbeats.
IF @requested>@limit
   OR @requested<>(SELECT COUNT(DISTINCT CONVERT(nvarchar(64),value)) FROM OPENJSON(@generation_ids_json))
    THROW 51073, 'Inventory retirement accepts at most limit distinct pass IDs', 1;
-- Fixed here, not chosen by the caller: seven days of history stay for troubleshooting.
DECLARE @retain_after datetime2(6)=DATEADD(day,-{RETENTION_DAYS},@now),
    @grace_start datetime2(6)=DATEADD(second,-{GRACE_SECONDS},@now);
DECLARE @candidates TABLE (generation_id nvarchar(64) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY,
    key_hash binary(32) NOT NULL, ordinal int NOT NULL);
INSERT INTO @candidates (generation_id,key_hash,ordinal)
SELECT CONVERT(nvarchar(64),value),{key_hash('value')},CONVERT(int,[key]) FROM OPENJSON(@generation_ids_json);
-- The passes the controller itself reads: accepted evidence only.
DECLARE @passes TABLE (generation_id nvarchar(64) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY,
    scan_hash binary(32) NOT NULL, started_at datetime2(6) NULL, finished bit NOT NULL, complete bit NOT NULL,
    INDEX ix_scan (scan_hash, started_at, generation_id));
INSERT INTO @passes (generation_id,scan_hash,started_at,finished,complete)
{inventory_passes_sql(names.object('accepted_worker_facts'))}
  AND EXISTS (SELECT 1 FROM @candidates);
DECLARE @eligible TABLE (generation_id nvarchar(64) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY,
    key_hash binary(32) NOT NULL, ordinal int NOT NULL);
INSERT INTO @eligible (generation_id,key_hash,ordinal)
SELECT c.generation_id,c.key_hash,c.ordinal FROM @candidates AS c
WHERE {inventory_retirement_eligible_sql(names, passes='@passes', generation='c.generation_id', key_hash='c.key_hash')};
DECLARE @facts TABLE (record_kind varchar(40) COLLATE DATABASE_DEFAULT NOT NULL, key_hash binary(32) NOT NULL,
    full_key nvarchar(1024) COLLATE DATABASE_DEFAULT NOT NULL, PRIMARY KEY (record_kind,key_hash));
-- At most {m.INVENTORY_RETIREMENT_SIGHTINGS} sightings per operation, oldest pass first, because the control
-- lock is held while they are deleted. A tenant pass can have one for every workspace and item.
INSERT INTO @facts (record_kind,key_hash,full_key)
SELECT TOP ({m.INVENTORY_RETIREMENT_SIGHTINGS}) s.record_kind,s.key_hash,s.full_key
FROM @eligible AS e JOIN {records} AS s
  ON s.tenant_id=@tenant_id AND s.epoch=@epoch AND s.record_kind IN ({sightings})
 AND s.parent_hash=e.key_hash AND s.parent_key=e.generation_id
ORDER BY e.ordinal,s.record_kind,s.key_hash;
-- A pass record goes with its last sighting. Until then it stays, and stays
-- eligible, so a later operation continues where this one stopped.
DECLARE @retired TABLE (generation_id nvarchar(64) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY,
    key_hash binary(32) NOT NULL);
INSERT INTO @retired (generation_id,key_hash)
SELECT e.generation_id,e.key_hash FROM @eligible AS e
WHERE NOT EXISTS (
    SELECT 1 FROM {records} AS s
    WHERE s.tenant_id=@tenant_id AND s.epoch=@epoch AND s.record_kind IN ({sightings})
      AND s.parent_hash=e.key_hash AND s.parent_key=e.generation_id
      AND NOT EXISTS (SELECT 1 FROM @facts AS f WHERE f.record_kind=s.record_kind AND f.key_hash=s.key_hash));
DECLARE @selected_sightings int=(SELECT COUNT(*) FROM @facts);
INSERT INTO @facts (record_kind,key_hash,full_key)
SELECT 'generation',r.key_hash,r.generation_id FROM @retired AS r;
DECLARE @deleted TABLE (record_kind varchar(40) COLLATE DATABASE_DEFAULT NOT NULL, revision bigint NOT NULL);
-- Bindings go with the rows they bind; no other accepted evidence is touched.
DELETE b OUTPUT deleted.record_kind,deleted.revision INTO @deleted
FROM {records} AS b JOIN @facts AS f
  ON b.tenant_id=@tenant_id AND b.epoch=@epoch AND b.record_kind='accepted_fact'
 AND b.accepted_fact_key_hash=f.key_hash
 AND JSON_VALUE(b.payload,'$.fact_kind')=f.record_kind
 AND JSON_VALUE(b.payload,'$.fact_key') COLLATE Latin1_General_100_BIN2=f.full_key;
DELETE d OUTPUT deleted.record_kind,deleted.revision INTO @deleted
FROM {records} AS d JOIN @facts AS f
  ON d.tenant_id=@tenant_id AND d.epoch=@epoch AND d.record_kind=f.record_kind
 AND d.key_hash=f.key_hash AND d.full_key=f.full_key;
IF (SELECT COUNT(*) FROM @deleted WHERE record_kind='generation')<>(SELECT COUNT(*) FROM @retired)
   OR (SELECT COUNT(*) FROM @deleted WHERE record_kind IN ({sightings}))<>@selected_sightings
    THROW 51072, 'A retired inventory pass changed during retirement', 1;
-- Each deleted row adds its revisions plus one to its kind's offset, so a
-- change counter (live revisions plus offset) rises instead of repeating.
DECLARE @retirement TABLE (record_kind varchar(40) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY, retired_rows bigint NOT NULL,
    offset_delta bigint NOT NULL);
INSERT INTO @retirement (record_kind,retired_rows,offset_delta)
SELECT record_kind,COUNT_BIG(*),SUM(revision)+COUNT_BIG(*) FROM @deleted GROUP BY record_kind;
UPDATE c SET revision=c.revision+1,sequence_number=c.sequence_number+t.offset_delta,
    payload={_retirement_payload(
        't.record_kind', "TRY_CONVERT(bigint,JSON_VALUE(c.payload,'$.retired_rows'))+t.retired_rows",
        'c.sequence_number+t.offset_delta',
    )}
FROM {records} AS c JOIN @retirement AS t
  ON c.tenant_id=@tenant_id AND c.epoch=@epoch AND c.record_kind='record_retirement'
 AND c.key_hash={retirement_key} AND c.full_key=t.record_kind;
INSERT INTO {records} (tenant_id,epoch,record_kind,key_hash,full_key,revision,sequence_number,payload)
SELECT @tenant_id,@epoch,N'record_retirement',{retirement_key},t.record_kind,1,t.offset_delta,
    {_retirement_payload('t.record_kind', 't.retired_rows', 't.offset_delta')}
FROM @retirement AS t
WHERE NOT EXISTS (
    SELECT 1 FROM {records} AS c
    WHERE c.tenant_id=@tenant_id AND c.epoch=@epoch AND c.record_kind='record_retirement'
      AND c.key_hash={retirement_key} AND c.full_key=t.record_kind);
SET @affected=(SELECT COUNT(*) FROM @deleted);
SET @result=(SELECT @tenant_id AS tenant_id,@epoch AS epoch,@request_id AS request_id,@limit AS [limit],
    CONVERT(nvarchar(40),@retain_after,127)+N'Z' AS retain_after,
    {_id_array('@retired', 'r')} AS retired_generation_ids,
    {_id_array('@candidates', 'c', ' WHERE NOT EXISTS (SELECT 1 FROM @eligible AS e WHERE e.generation_id=c.generation_id)')} AS refused_generation_ids,
    {_id_array('@eligible', 'e', ' WHERE NOT EXISTS (SELECT 1 FROM @retired AS r WHERE r.generation_id=e.generation_id)')} AS deferred_generation_ids,
    (SELECT COUNT(*) FROM @deleted WHERE record_kind IN ({sightings})) AS retired_sightings,
    (SELECT COUNT(*) FROM @deleted WHERE record_kind='accepted_fact') AS retired_bindings
    FOR JSON PATH,WITHOUT_ARRAY_WRAPPER);
{save_receipt(names, contract.operation)}"""
    return {contract.operation: procedure(names, contract, body, replay=True)}
