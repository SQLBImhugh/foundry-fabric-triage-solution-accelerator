from __future__ import annotations

import asyncio
import hashlib
import json
import re
import sqlite3
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError
from test_monitoring_store import (
    Clock,
    Harness,
    inventory_update_batch,
    owned_inventory_start,
    powerbi_staged_page,
    rejection_request,
    uid,
)

from triage.monitoring import models as m
from triage.monitoring.contracts import (
    MonitoringCommitUncertain,
    MonitoringConflict,
    MonitoringKernelUnsupported,
    MonitoringNotBootstrapped,
    MonitoringStore,
    MonitoringUnavailable,
)
from triage.monitoring.events import (
    ConnectorBinding,
    ConnectorScope,
    EventPersistence,
    OwnershipChange,
    ReceiverHeartbeat,
    SqlCheckpointStore,
    StreamStartRequest,
    UnidentifiedReceiptBatch,
    UnidentifiedSignal,
    summarize_body,
)
from triage.monitoring.memory import (
    InMemoryMonitoringState,
    MemoryBackend,
    MemoryMonitoringAdapter,
    MonitoringEngine,
    StoredReceipt,
    StoredRecord,
    key_digest,
)
from triage.monitoring.schema import (
    ACCEPTED_FACT_KEY_HASH_EXPRESSION,
    initialize_monitoring_schema,
    resolve_tables,
    runtime_table_permissions,
)
from triage.monitoring.sql_store import _read_time
from triage.store.approvals import AzureSqlApprovalChannel
from triage.store.azure_sql import AzureSqlDatabase, SqlCommitUncertain, quote_identifier


class DriverRow:
    """Driver-style indexed rows deliberately do not compare equal to tuples."""

    def __init__(self, values: tuple) -> None:
        self.values = values

    def __getitem__(self, index):
        return self.values[index]

    def __len__(self) -> int:
        return len(self.values)

    def __iter__(self):
        return iter(self.values)


class SqliteCursor:
    def __init__(self, owner: SqliteConnection) -> None:
        self.owner = owner
        self.native = owner.native.cursor()
        self.rows: list[DriverRow] | None = None
        self.rowcount = -1

    def execute(self, sql: str, *params: object) -> None:
        db = self.owner.db
        db.statements.append((sql, params))
        if re.match(r"\s*SELECT OBJECT_ID\(", sql, re.I):
            values = []
            for name in params:
                table = str(name).split(".")[-1]
                row = self.owner.native.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,),
                ).fetchone()
                values.append(1 if row else None)
            self.rows = [DriverRow(tuple(values))]
            self.rowcount = -1
            return
        translated = self.owner.translate(sql)
        bound = tuple(
            value.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="microseconds")
            if isinstance(value, datetime) and value.tzinfo is not None
            else value.isoformat(timespec="microseconds") if isinstance(value, datetime) else value
            for value in params
        )
        self.native.execute(translated, bound)
        self.rowcount = self.native.rowcount
        self.rows = None
        if db.fail_statement is not None and db.fail_statement(sql, params):
            db.fail_statement = None
            raise sqlite3.OperationalError("Injected SQL statement failure after execution")

    def fetchall(self) -> list[DriverRow]:
        return self.rows if self.rows is not None else [DriverRow(tuple(row)) for row in self.native.fetchall()]

    def close(self) -> None:
        self.native.close()


class SqliteConnection:
    def __init__(self, db: SqliteAzureDatabase) -> None:
        self.db = db
        self.native = sqlite3.connect(db.path, isolation_level=None, timeout=10, check_same_thread=False)
        self._autocommit = True
        self.native.create_function("SYSUTCDATETIME", 0, self.now)
        self.native.create_function("SYSDATETIMEOFFSET", 0, self.now)
        self.native.create_function("DATEADD", 3, self.dateadd)
        self.native.create_function("JSON_VALUE", 2, self.json_value)
        self.native.create_function("JSON_MODIFY", 3, self.json_modify)
        self.native.create_function("UTC_TIME", 1, self.utc_time)
        self.native.create_function("HASHBYTES", 2, self.hashbytes)
        self.native.create_function(
            "KEY_HASH", 1, lambda value: bytes.fromhex(key_digest(value)) if value is not None else None,
            deterministic=True,
        )

    def now(self) -> str:
        return self.db.clock().replace(tzinfo=None).isoformat(timespec="microseconds")

    @staticmethod
    def dateadd(unit: str, value: int, at: str) -> str:
        delta = timedelta(seconds=value) if unit == "second" else timedelta(microseconds=value)
        return (datetime.fromisoformat(at) + delta).isoformat(timespec="microseconds")

    @staticmethod
    def json_value(payload: str, path: str):
        return json.loads(payload).get(path.removeprefix("$."))

    @staticmethod
    def json_modify(payload: str, path: str, value: object) -> str:
        record = json.loads(payload)
        record[path.removeprefix("$.")] = value
        return json.dumps(record)

    @staticmethod
    def utc_time(value: str | None) -> str | None:
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return None
            return parsed.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="microseconds")
        except ValueError:
            return None

    @staticmethod
    def hashbytes(algorithm: str, payload: str) -> bytes:
        if algorithm != "SHA2_256":
            raise ValueError("Unexpected fixture hash algorithm")
        return hashlib.sha256(payload.encode("utf-16-le")).digest()

    @staticmethod
    def translate(sql: str) -> str:
        translated = sql.strip().replace(
            f"accepted_fact_key_hash AS {ACCEPTED_FACT_KEY_HASH_EXPRESSION} PERSISTED",
            "accepted_fact_key_hash BLOB GENERATED ALWAYS AS "
            "(CASE WHEN record_kind='accepted_fact' AND json_valid(payload)=1 THEN "
            "KEY_HASH(json_extract(payload,'$.fact_key')) END) STORED",
        )
        if translated.upper().startswith("IF "):
            create = re.search(r"\bCREATE (?:UNIQUE )?(?:TABLE|INDEX)\b", translated, re.I)
            if create is None:
                raise ValueError("Unsupported fixture DDL")
            translated = translated[create.start():]
            translated = re.sub(
                r"CREATE (UNIQUE )?(TABLE|INDEX) ",
                lambda match: f"CREATE {match[1] or ''}{match[2]} IF NOT EXISTS ",
                translated, count=1, flags=re.I,
            )
        translated = re.sub(r"\[dbo\]\.\[([A-Za-z_][A-Za-z0-9_]*)\]", r"[\1]", translated)
        translated = re.sub(r"WITH \((?:UPDLOCK|HOLDLOCK|ROWLOCK|READPAST)(?:,\s*(?:UPDLOCK|HOLDLOCK|ROWLOCK|READPAST))*\)", "", translated, flags=re.I)
        translated = re.sub(r"\bN'", "'", translated)
        translated = re.sub(r"\bNVARCHAR\(MAX\)", "TEXT", translated, flags=re.I)
        translated = re.sub(r"DATEADD\((second|microsecond),", r"DATEADD('\1',", translated, flags=re.I)
        translated = re.sub(
            r"TRY_CAST\((JSON_VALUE\([^)]*\))\s+AS DATETIMEOFFSET\)",
            r"UTC_TIME(\1)", translated, flags=re.I,
        )
        top = re.search(r"SELECT TOP \((\d+)\)", translated, re.I)
        if top:
            translated = re.sub(r"SELECT TOP \(\d+\)", "SELECT", translated, count=1, flags=re.I)
            translated = translated.rstrip(";") + f" LIMIT {top[1]}"
        return translated

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if not value and self._autocommit:
            self.native.execute("BEGIN IMMEDIATE")
        self._autocommit = value

    def cursor(self) -> SqliteCursor:
        return SqliteCursor(self)

    def commit(self) -> None:
        failure = self.db.fail_commit
        self.db.fail_commit = None
        if failure == "before":
            raise sqlite3.OperationalError("Injected failure before commit")
        self.native.commit()
        if failure == "after":
            raise sqlite3.OperationalError("Injected lost commit acknowledgement")

    def rollback(self) -> None:
        self.native.rollback()

    def close(self) -> None:
        self.native.close()


class SqliteAzureDatabase(AzureSqlDatabase):
    """Translate emitted DML into SQLite while running the real shared transaction helper."""

    def __init__(self, path: Path, clock: Clock, tables: dict[str, str] | None = None) -> None:
        super().__init__(server="fixture.invalid", database="fixture", tables=tables)
        self.path = path
        self.clock = clock
        self.statements: list[tuple[str, tuple]] = []
        self.fail_statement = None
        self.fail_commit: str | None = None

    def _connect(self) -> SqliteConnection:
        return SqliteConnection(self)

    def create_shared_fixture_tables(self) -> None:
        names = resolve_tables(self)
        with self.transaction():
            self.execute(
                f"CREATE TABLE [{names['incidents']}] ("
                "incident_id NVARCHAR(200) PRIMARY KEY, signature NVARCHAR(200) NOT NULL, "
                "status NVARCHAR(50) NOT NULL, updated_at NVARCHAR(40) NOT NULL, payload NVARCHAR(MAX) NOT NULL)",
            )
            self.execute(
                f"CREATE TABLE [{names['approvals']}] ("
                "request_id NVARCHAR(200) PRIMARY KEY, decision NVARCHAR(40), responder NVARCHAR(200), "
                "decided_at NVARCHAR(40), payload NVARCHAR(MAX) NOT NULL)",
            )
            self.execute(
                f"CREATE TABLE [{names['processed']}] (fingerprint NVARCHAR(64) PRIMARY KEY, "
                "message_id NVARCHAR(512), received_at NVARCHAR(40))",
            )


class SqlProtocolFixtureBackend(MemoryBackend):
    """Test-only row persistence oracle using the real synchronous transaction helper.

    This is not a runtime permission route. Kernel RPC/view routing is exercised
    separately; this fixture keeps the original rollback/concurrency cases without
    retaining a production base-table bypass.
    """

    def __init__(self, db: SqliteAzureDatabase, tables=None):
        super().__init__(InMemoryMonitoringState(), db.clock)
        self.db = db
        self.names = resolve_tables(db, tables)
        self.tables = {key: quote_identifier(name) for key, name in self.names.items()}

    @contextmanager
    def transaction(self, *, write, operation, request_id):
        self.operation_identity(operation, request_id)
        try:
            with self.db.transaction():
                self._load()
                before = {
                    name: deepcopy(getattr(self.state, name))
                    for name in ("control_row", "records", "receipts", "leases", "approvals", "incidents", "processed")
                }
                with super().transaction(write=write, operation=operation, request_id=request_id):
                    yield
                    if write:
                        self._flush(before)
        except SqlCommitUncertain as exc:
            raise MonitoringCommitUncertain(self._local.operation, self._local.request_id) from exc
        except (MonitoringConflict, MonitoringNotBootstrapped, MonitoringUnavailable, ValidationError, ValueError):
            raise
        except Exception as exc:
            raise MonitoringUnavailable("SQL protocol fixture statement failed") from exc

    def _load(self):
        exists = self.db.query("SELECT OBJECT_ID(?, 'U')", f"dbo.{self.names['monitoring_control']}")
        self.state.control_row = None
        for name in ("records", "receipts", "leases", "approvals", "incidents", "processed"):
            setattr(self.state, name, {})
        if not exists or exists[0][0] is None:
            return
        rows = self.db.query(f"SELECT payload FROM {self.tables['monitoring_control']} WHERE singleton = 1")
        self.state.control_row = json.loads(rows[0][0]) if rows else None
        for row in self.db.query(
            "SELECT tenant_id,epoch,record_kind,full_key,revision,status,workload,workspace_id,item_id,"
            f"target_key,parent_key,work_kind,generation_id,due_at,sequence_number,payload,key_hash FROM {self.tables['monitoring_records']}",
        ):
            if bytes(row[16]) != bytes.fromhex(key_digest(row[3])):
                raise MonitoringUnavailable("SQL protocol record digest disagrees with its full identity")
            context = m.MonitoringContext(tenant_id=row[0], epoch=row[1])
            record = StoredRecord(
                context=context, kind=row[2], key=row[3], version=row[4], status=row[5],
                workload=row[6], workspace_id=row[7], item_id=row[8], target_key=row[9],
                parent_key=row[10], work_kind=row[11], generation_id=row[12],
                due_at=_read_time(row[13]) if row[13] else None, sequence_number=row[14], payload=row[15],
            )
            self.state.records[(*context.model_dump().values(), record.kind, key_digest(record.key))] = record
        for row in self.db.query(
            f"SELECT tenant_id,epoch,operation,request_id,fingerprint,recorded_at,payload FROM {self.tables['monitoring_receipts']}",
        ):
            receipt = StoredReceipt(
                context=m.MonitoringContext(tenant_id=row[0], epoch=row[1]), operation=row[2],
                request_id=row[3], fingerprint=row[4], recorded_at=_read_time(row[5]), payload=row[6],
            )
            self.state.receipts[(row[0], row[1], row[2], key_digest(row[3]))] = receipt
        for row in self.db.query(
            f"SELECT tenant_id,epoch,full_key,owner_id,fence,acquired_at,expires_at FROM {self.tables['monitoring_leases']}",
        ):
            lease = m.LeaseToken(
                tenant_id=row[0], epoch=row[1], resource_key=row[2], owner_id=row[3],
                fence=row[4], acquired_at=_read_time(row[5]), expires_at=_read_time(row[6]),
            )
            self.state.leases[(row[0], row[1], key_digest(row[2]))] = lease
        self.state.approvals = {
            row[0]: json.loads(row[1]) for row in self.db.query(f"SELECT request_id,payload FROM {self.tables['approvals']}")
        }
        self.state.incidents = dict(tuple(row) for row in self.db.query(
            f"SELECT incident_id,payload FROM {self.tables['incidents']}",
        ))
        self.state.processed = dict(tuple(row) for row in self.db.query(
            f"SELECT fingerprint,received_at FROM {self.tables['processed']}",
        ))

    def _flush(self, before):
        control = self.state.control_row
        if control != before["control_row"]:
            assert self.db.execute(
                f"UPDATE {self.tables['monitoring_control']} SET revision=?,maintenance=?,updated_at=?,payload=? "
                "WHERE singleton=1 AND revision=?",
                control["revision"], control["maintenance"], control["updated_at"], json.dumps(control),
                before["control_row"]["revision"],
            ) == 1
        for key, record in self.state.records.items():
            prior = before["records"].get(key)
            if prior == record:
                continue
            values = (
                record.version, record.status, record.workload, record.workspace_id, record.item_id,
                bytes.fromhex(key_digest(record.target_key)) if record.target_key else None, record.target_key,
                bytes.fromhex(key_digest(record.parent_key)) if record.parent_key else None, record.parent_key,
                record.work_kind, record.generation_id, record.due_at, record.sequence_number, record.payload,
            )
            identity = (*key[:3], bytes.fromhex(key[3]))
            if prior is None:
                count = self.db.execute(
                    f"INSERT INTO {self.tables['monitoring_records']} (revision,status,workload,workspace_id,item_id,"
                    "target_hash,target_key,parent_hash,parent_key,work_kind,generation_id,due_at,sequence_number,payload,"
                    "tenant_id,epoch,record_kind,key_hash,full_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    *values, *identity, record.key,
                )
            else:
                count = self.db.execute(
                    f"UPDATE {self.tables['monitoring_records']} SET revision=?,status=?,workload=?,workspace_id=?,item_id=?,"
                    "target_hash=?,target_key=?,parent_hash=?,parent_key=?,work_kind=?,generation_id=?,due_at=?,"
                    "sequence_number=?,payload=? WHERE tenant_id=? AND epoch=? AND record_kind=? AND key_hash=? AND revision=?",
                    *values, *identity, prior.version,
                )
            assert count == 1
        for key, lease in self.state.leases.items():
            prior = before["leases"].get(key)
            if prior == lease:
                continue
            identity = (*key[:2], bytes.fromhex(key[2]))
            if prior is None:
                count = self.db.execute(
                    f"INSERT INTO {self.tables['monitoring_leases']} "
                    "(tenant_id,epoch,key_hash,full_key,owner_id,fence,acquired_at,expires_at) VALUES (?,?,?,?,?,?,?,?)",
                    *identity, lease.resource_key, lease.owner_id, lease.fence, lease.acquired_at, lease.expires_at,
                )
            else:
                expired = " AND expires_at <= SYSUTCDATETIME()" if (
                    prior.expires_at <= self.now() and lease.expires_at > self.now()
                ) else ""
                count = self.db.execute(
                    f"UPDATE {self.tables['monitoring_leases']} SET owner_id=?,fence=?,acquired_at=?,expires_at=? "
                    "WHERE tenant_id=? AND epoch=? AND key_hash=? AND owner_id = ? AND fence = ?" + expired,
                    lease.owner_id, lease.fence, lease.acquired_at, lease.expires_at,
                    *identity, prior.owner_id, prior.fence,
                )
            assert count == 1
        for request_id, row in self.state.approvals.items():
            prior = before["approvals"].get(request_id)
            if row != prior:
                assert prior and row.get("consumed_at")
                assert AzureSqlApprovalChannel(
                    db=self.db, table=self.names["approvals"],
                ).consume_exact(request_id, row["fingerprint"])
        for incident_id, payload in self.state.incidents.items():
            prior = before["incidents"].get(incident_id)
            if payload == prior:
                continue
            incident = json.loads(payload)
            if prior is None:
                count = self.db.execute(
                    f"INSERT INTO {self.tables['incidents']} (incident_id,signature,status,updated_at,payload) VALUES (?,?,?,?,?)",
                    incident_id, incident["signature"], incident["status"], self.now().isoformat(), payload,
                )
            else:
                count = self.db.execute(
                    f"UPDATE {self.tables['incidents']} SET status=?,updated_at=?,payload=? "
                    "WHERE incident_id=? AND HASHBYTES('SHA2_256', payload)=?",
                    incident["status"], self.now().isoformat(), payload, incident_id,
                    hashlib.sha256(prior.encode("utf-16-le")).digest(),
                )
            assert count == 1
        for fingerprint, at in self.state.processed.items():
            if fingerprint not in before["processed"]:
                source = next(
                    record.key for record in self.state.records.values()
                    if record.kind == "source_disposition" and key_digest(record.key) == fingerprint
                )
                assert self.db.execute(
                    f"INSERT INTO {self.tables['processed']} (fingerprint,message_id,received_at) VALUES (?,?,?)",
                    fingerprint, source, at,
                ) == 1
        for key, receipt in self.state.receipts.items():
            if key not in before["receipts"]:
                assert self.db.execute(
                    f"INSERT INTO {self.tables['monitoring_receipts']} "
                    "(tenant_id,epoch,operation,request_hash,request_id,fingerprint,recorded_at,payload) VALUES (?,?,?,?,?,?,?,?)",
                    *key[:3], bytes.fromhex(key[3]), receipt.request_id, receipt.fingerprint,
                    receipt.recorded_at, receipt.payload,
                ) == 1


class SqlProtocolFixtureStore(MonitoringEngine):
    def __init__(self, *, db, tables=None, policy=None, redactor=None):
        options = {"redactor": redactor} if redactor else {}
        super().__init__(
            SqlProtocolFixtureBackend(db, tables), component="fixture",
            adapter_factory=MemoryMonitoringAdapter, policy=policy, **options,
        )

    def record_inventory(self, batch):
        if batch.commit is None:
            raise MonitoringConflict("SQL protocol fixtures require current work ownership and generation position")
        return super().record_inventory(batch)


class SqlHarness(Harness):
    def __init__(self, path: Path, tables: dict[str, str] | None = None) -> None:
        clock = Clock()
        self.db = SqliteAzureDatabase(path, clock, tables)
        store = SqlProtocolFixtureStore(db=self.db)
        super().__init__(store=store, clock=clock)
        self.db.create_shared_fixture_tables()
        self.bootstrap_id = uid(90)
        initialize_monitoring_schema(self.db, control=self.control, bootstrap_id=self.bootstrap_id)
        self.inventory_requests: dict[str, m.InventoryBatch] = {}

    def record_inventory(self, batch: m.InventoryBatch) -> m.InventoryGeneration:
        if batch.request_id in self.inventory_requests:
            return self.store.record_inventory(self.inventory_requests[batch.request_id])
        context = m.MonitoringContext(**self.context())
        work = self.store.get_work(context, batch.generation.generation_id)
        if work is None:
            self.store.enqueue_work(m.MonitoringWorkDraft(
                **self.context(), work_id=batch.generation.generation_id, kind="inventory",
                policy_revision=batch.expected.revision, created_at=batch.generation.started_at,
                due_at=self.clock(), discovery_selector=batch.generation.selector,
                reason="Explicit SQL fixture collection.",
            ))
            work = self.claim(("inventory",))[0]
        if work.work_id != batch.generation.generation_id or work.lease is None:
            raise AssertionError("SQL fixture inventory needs its own current collector work")
        prior = self.store.get_inventory_generation(context, batch.generation.generation_id)
        committed = m.InventoryBatch.model_validate({
            **batch.model_dump(), "commit": m.InventoryCommit(
                work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
                expected_generation_revision=prior.revision if prior else 0,
                expected_continuation=prior.continuation if prior else None,
            ),
        })
        self.inventory_requests[batch.request_id] = committed
        result = self.store.record_inventory(committed)
        if result.completeness == "complete":
            self.store.complete_collection_work(
                context, work_id=work.work_id, lease=work.lease, expected_work_revision=work.revision,
            )
        return result

    def seed_approval(self, row: dict) -> None:
        table = resolve_tables(self.db)["approvals"]
        with self.db.transaction():
            self.db.execute(
                f"INSERT INTO [dbo].[{table}] (request_id, decision, responder, decided_at, payload) "
                "VALUES (?, ?, ?, ?, ?)",
                row["request_id"], row["decision"], row["responder"], row["decided_at"], json.dumps(row),
            )

    def approval_row(self, request_id: str) -> dict:
        table = resolve_tables(self.db)["approvals"]
        return json.loads(self.db.query(f"SELECT payload FROM [dbo].[{table}] WHERE request_id = ?", request_id)[0][0])

    def decide_approval(self, row: dict) -> None:
        table = resolve_tables(self.db)["approvals"]
        with self.db.transaction():
            changed = self.db.execute(
                f"UPDATE [dbo].[{table}] SET decision = ?, responder = ?, decided_at = ?, payload = ? "
                "WHERE request_id = ? AND (decision IS NULL OR decision = '')",
                row["decision"], row["responder"], row["decided_at"], json.dumps(row), row["request_id"],
            )
            assert changed == 1


@pytest.fixture
def sql_harness(tmp_path: Path) -> SqlHarness:
    h = SqlHarness(tmp_path / "monitoring.sqlite")
    h.seed()
    h.activate()
    return h


def test_sql_constructor_does_no_io_and_runtime_does_not_bootstrap(tmp_path: Path) -> None:
    db = SqliteAzureDatabase(tmp_path / "empty.sqlite", Clock())
    store = SqlProtocolFixtureStore(db=db)
    assert db.statements == []
    assert isinstance(store, MonitoringStore)
    assert store.inspect_bootstrap(expected_tenant_id=uid(1)).status == "missing"
    with pytest.raises(MonitoringNotBootstrapped):
        store.snapshot(m.MonitoringContext(tenant_id=uid(1), epoch=uid(2)))
    assert not any("CREATE " in sql or "ALTER " in sql for sql, _ in db.statements)


def test_deployment_bootstrap_receipt_replay_does_not_reset_new_records(sql_harness: SqlHarness) -> None:
    h = sql_harness
    current = initialize_monitoring_schema(h.db, control=h.control, bootstrap_id=h.bootstrap_id)
    assert current.revision == 1
    assert h.store.resolve_target(h.targets[0]) is not None
    with pytest.raises(MonitoringConflict):
        initialize_monitoring_schema(h.db, control=h.control, bootstrap_id=uid(91))
    assert h.store.resolve_target(h.targets[0]) is not None


def test_driver_rows_and_independent_instances_observe_current_sql(sql_harness: SqlHarness) -> None:
    h = sql_harness
    other_db = SqliteAzureDatabase(h.db.path, h.clock)
    other = SqlProtocolFixtureStore(db=other_db)
    assert DriverRow((1,)) != (1,)
    assert other.inspect_bootstrap(expected_tenant_id=uid(1)).status == "ready"
    assert other.resolve_target(h.targets[0]).identity == h.targets[0]
    h.activate(m.ScopeDefinition.model_validate({**h.scope.model_dump(), "enabled": False}))
    assert other.resolve_target(h.targets[0]) is None
    assert other.resolve_target(h.targets[0], include_inactive=True).state == "paused"


def test_sql_persists_typed_inventory_selection_before_any_scope(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "selectors.sqlite")
    selected = m.ScopeSelector(tenant_id=uid(1), kind="workspace", workspace_id=uid(100))
    work = h.store.enqueue_work(m.MonitoringWorkDraft(
        **h.context(), work_id=h.next_id(), kind="inventory", policy_revision=0,
        created_at=h.clock(), due_at=h.clock(), reason="Explicit read-only inventory.",
        discovery_selector=selected,
    ))
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    restored = other.get_work(m.MonitoringContext(**h.context()), work.work_id)
    assert restored.discovery_selector == selected
    assert other.list_scopes(m.PageQuery(**h.context())).items == ()


def test_sql_catalogue_names_are_paginated_and_tied_to_inventory_generation(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "catalogue.sqlite")
    generation_id = h.next_id()
    generation = m.InventoryGeneration(
        **h.context(), generation_id=generation_id,
        selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
        adapter="named_workspace_catalogue", authority="tenant_admin", enumeration="workspaces",
        completeness="complete", started_at=h.clock(), completed_at=h.clock(),
        discovered_count=2, completed_pages=1,
    )
    h.record_inventory(m.InventoryBatch(
        request_id=h.next_id(), expected=h.version, generation=generation, items=(),
        workspaces=tuple(m.InventoryWorkspace(
            **h.context(), generation_id=generation_id, workspace_id=uid(100 + index),
            name=f"Fixture workspace {index}", domain_id=uid(50 + index), observed_at=h.clock(),
        ) for index in range(2)),
        domains=tuple(m.InventoryDomain(
            **h.context(), generation_id=generation_id, domain_id=uid(50 + index),
            name=f"Fixture domain {index}", observed_at=h.clock(),
        ) for index in range(2)),
    ))
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    first = other.list_workspaces(m.PageQuery(**h.context(), limit=1))
    second = other.list_workspaces(m.PageQuery(**h.context(), limit=1, cursor=first.next_cursor))
    assert {first.items[0].name, second.items[0].name} == {"Fixture workspace 0", "Fixture workspace 1"}
    domains = other.list_domains(m.PageQuery(**h.context()))
    assert {domain.name for domain in domains.items} == {"Fixture domain 0", "Fixture domain 1"}
    assert all(domain.generation_id == generation_id for domain in domains.items)
    assert other.get_inventory_generation(m.MonitoringContext(**h.context()), generation_id).completeness == "complete"
    assert other.coverage(m.MonitoringContext(**h.context())).discovered_count == 0


@pytest.mark.parametrize("key", ["fixture-key", "fixture-key  ", "\u6d4b\u8bd5-key"])
def test_sql_protocol_fixture_derives_fact_hash_without_promoting_it(tmp_path: Path, key: str) -> None:
    h = SqlHarness(tmp_path / "derived-binding.sqlite")
    table = quote_identifier(resolve_tables(h.db)["monitoring_records"])
    with h.db.transaction():
        h.db.execute(
            f"INSERT INTO {table} (tenant_id,epoch,record_kind,key_hash,full_key,revision,payload) "
            "VALUES (?,?,?,?,?,?,?)",
            h.control.tenant_id, h.control.epoch, "accepted_fact", bytes.fromhex(key_digest("binding")),
            "binding", 1, json.dumps({"fact_key": key}),
        )
        assert h.db.query(
            f"SELECT accepted_fact_key_hash FROM {table} WHERE record_kind='accepted_fact'",
        ) == [(bytes.fromhex(key_digest(key)),)]
        h.db.execute(f"UPDATE {table} SET payload='{{}}' WHERE record_kind='accepted_fact'")
        assert h.db.query(
            f"SELECT accepted_fact_key_hash FROM {table} WHERE record_kind='accepted_fact'",
        ) == [(None,)]


def test_sql_inventory_commits_require_current_atomic_owner_and_position(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "inventory-fence.sqlite")
    old_work, initial = owned_inventory_start(h)
    stale = inventory_update_batch(h, old_work, initial, complete=True)
    with pytest.raises(MonitoringConflict, match="require current work ownership"):
        h.store.record_inventory(m.InventoryBatch.model_validate({**stale.model_dump(), "commit": None}))
    h.clock.advance(121)
    h.owner = uid(99)
    replacement = h.claim(("inventory",))[0]
    denied = h.store.record_inventory(inventory_update_batch(h, replacement, initial))
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    with pytest.raises(MonitoringConflict):
        other.record_inventory(stale)
    retained = other.get_inventory_generation(m.MonitoringContext(**h.context()), initial.generation_id)
    assert retained == denied
    assert retained.gaps[0].code == "http_403"


def test_sql_generation_catalogue_does_not_follow_latest_rows(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "generation-catalogue.sqlite")
    saved = []
    for label in ("first", "replacement"):
        generation_id = h.next_id()
        h.record_inventory(m.InventoryBatch(
            request_id=h.next_id(), expected=h.version, items=(),
            generation=m.InventoryGeneration(
                **h.context(), generation_id=generation_id,
                selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"), adapter="catalogue_fixture",
                authority="tenant_admin", enumeration="domains", completeness="complete",
                started_at=h.clock(), completed_at=h.clock(), discovered_count=1, completed_pages=1,
            ),
            domains=(m.InventoryDomain(
                **h.context(), generation_id=generation_id, domain_id=uid(50),
                name=label, observed_at=h.clock(),
            ),),
        ))
        saved.append(generation_id)
        h.clock.advance(1)
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    assert other.list_domains(m.PageQuery(**h.context())).items[0].name == "replacement"
    assert other.list_domains(m.PageQuery(**h.context()), generation_id=saved[0]).items[0].name == "first"


def test_sql_powerbi_alias_conflict_is_durable_before_any_source_admission(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "powerbi-alias-window.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    work = h.claim(("poll",))[0]
    source = h.observation()
    first = h.store.record_rest_page(powerbi_staged_page(h, work, source))
    assert first.powerbi_window.state == "collecting"
    assert first.intake.work_ids == ()
    assert h.store.get_source(source.execution) is None
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    second_source = h.observation(execution=m.SourceExecutionIdentity(
        target=h.targets[0], run_id_kind="powerbi_request", run_id=uid(30_001),
    ))
    second = powerbi_staged_page(h, work, second_source, checkpoint=first.checkpoint, final=True)
    finished = other.record_rest_page(second)
    assert finished.powerbi_window.state == "quarantined"
    assert finished.intake.work_ids == ()
    assert finished.checkpoint.coverage_through is None
    assert other.get_source(source.execution) is None
    assert other.get_source(second_source.execution) is None
    aliases = other.list_powerbi_aliases(
        m.PageQuery(**h.context()), window_id=finished.powerbi_window.window_id,
    )
    mapping = next(value for value in aliases.items if value.namespace == "refresh")
    assert mapping.mapped_ids == tuple(sorted((source.execution.run_id, second_source.execution.run_id)))
    assert other.record_rest_page(second) == finished


def test_sql_powerbi_window_finalization_rolls_back_alias_validation_and_work(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "powerbi-alias-rollback.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    work = h.claim(("poll",))[0]
    numeric = h.observation(execution=m.SourceExecutionIdentity(
        target=h.targets[0], run_id_kind="powerbi_refresh", run_id="23",
    ))
    first = h.store.record_rest_page(powerbi_staged_page(h, work, numeric))
    source = h.observation()
    final = powerbi_staged_page(h, work, source, checkpoint=first.checkpoint, final=True)
    h.db.fail_statement = lambda sql, params: "UPDATE" in sql and "powerbi_window" in params
    with pytest.raises(MonitoringUnavailable):
        h.store.record_rest_page(final)
    state = h.store.get_powerbi_window(m.MonitoringContext(**h.context()), first.powerbi_window.window_id)
    assert state.state == "collecting"
    assert state.row_count == 1
    assert h.store.get_source(source.execution) is None
    assert h.store.get_rest_checkpoint(h.targets[0]).revision == first.checkpoint.revision
    recovered = h.store.record_rest_page(final)
    assert recovered.powerbi_window.state == "validated"
    assert len(recovered.intake.work_ids) == 1
    assert h.store.get_work(m.MonitoringContext(**h.context()), recovered.intake.work_ids[0]).execution == source.execution


def test_sql_plan_and_activation_keep_the_verified_requester_audit(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "requester-audit.sqlite")
    h.seed()
    requested_by = uid(77)
    request = m.ScopePreviewRequest(
        expected=h.version, idempotency_id=h.next_id(), requested_by=requested_by,
        scope=m.ScopeDefinition(
            **h.context(), scope_id=uid(20), name="Audited SQL scope",
            rules=(m.ScopeRule(
                rule_id=uid(21), effect="include", selector=m.ScopeSelector(tenant_id=uid(1), kind="tenant"),
            ),),
        ),
    )
    plan = h.store.preview_scope(request)
    activation = m.ActivateScopeRequest(
        expected=h.version, plan_id=plan.plan_id, idempotency_id=request.idempotency_id,
    )
    receipt = h.store.activate_scope(activation)
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    assert other.get_plan(m.MonitoringContext(**h.context()), plan.plan_id).requested_by == requested_by
    assert other.get_activation(m.MonitoringContext(**h.context()), request.idempotency_id).requested_by == requested_by
    assert other.activate_scope(activation) == receipt
    audit = other.get_operation_receipt(m.MonitoringContext(**h.context()), "activation", request.idempotency_id)
    assert audit.result["requested_by"] == requested_by


def test_sql_fairness_and_database_time_arbitrate_two_instances(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "fair.sqlite")
    h.seed(count=40, workspaces=10)
    h.activate()
    other_db = SqliteAzureDatabase(h.db.path, h.clock)
    other = SqlProtocolFixtureStore(db=other_db)
    def claim(store, owner):
        return store.claim_work(m.WorkClaimRequest(
            **h.context(), owner_id=owner, kinds=("poll",), limit=10, per_workspace_limit=2,
        ))
    with ThreadPoolExecutor(max_workers=2) as pool:
        first_future = pool.submit(claim, h.store, uid(101))
        second_future = pool.submit(claim, other, uid(102))
        first, second = first_future.result(), second_future.result()
    assert len(first) == len(second) == 10
    assert {work.work_id for work in first}.isdisjoint(work.work_id for work in second)
    assert set(Counter(work.target.workspace_id for work in (*first, *second)).values()) == {2}
    assert first[0].lease.expires_at == h.clock() + timedelta(seconds=120)
    assert claim(h.store, uid(103)) == ()
    h.clock.advance(121)
    reclaimed = claim(other, uid(104))
    assert reclaimed
    claim(other, uid(105))
    h.clock.advance(121)
    reclaimed = claim(other, uid(106))
    assert all(work.lease.fence == 2 for work in reclaimed)
    assert any("expires_at <= SYSUTCDATETIME()" in sql for sql, _ in (*h.db.statements, *other_db.statements))


def test_sql_rest_page_failure_rolls_back_work_receipts_and_checkpoint(sql_harness: SqlHarness) -> None:
    h = sql_harness
    work = h.claim(("poll",))[0]
    page = m.RestPageRequest(
        page_id=h.next_id(), target=h.targets[0], policy_revision=h.version.revision,
        poll_work_id=work.work_id, lease=work.lease, expected_checkpoint_revision=0,
        window=m.ObservationWindow(start_at=h.clock() - timedelta(minutes=15), end_at=h.clock()),
        received_count=1, observations=(h.observation(),), window_complete=True, observed_at=h.clock(),
    )
    h.db.fail_statement = lambda sql, params: "INSERT INTO" in sql and "rest_checkpoint" in params
    with pytest.raises(MonitoringUnavailable):
        h.store.record_rest_page(page)
    assert h.store.get_rest_checkpoint(h.targets[0]) is None
    assert h.store.get_rest_page(m.MonitoringContext(**h.context()), page.page_id) is None
    assert h.store.get_source(h.observation().execution) is None
    assert h.store.get_work(m.MonitoringContext(**h.context()), work.work_id).state == "leased"
    receipt = h.store.record_rest_page(page)
    assert receipt.checkpoint.coverage_through == page.window.end_at
    assert len(receipt.intake.work_ids) == 1


def test_sql_stream_checkpoint_cannot_skip_uncommitted_positions(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.connector()
    zero, two = h.signal(0), h.signal(2)
    lease = h.start_partition(first_sequence_number=0)
    batch = m.StreamReceiptBatch(request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(zero, two))
    result = h.store.record_stream_receipts(batch)
    assert len(result.work_ids) == 1
    request = m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=lease, expected_revision=0, through=two.position,
    )
    with pytest.raises(MonitoringConflict):
        h.store.advance_stream_checkpoint(request)
    h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(h.signal(1),),
    ))
    checkpoint = h.store.advance_stream_checkpoint(request)
    assert checkpoint.position.sequence_number == 2
    assert h.store.advance_stream_checkpoint(request) == checkpoint


def test_sql_reservation_uses_actual_approval_and_rolls_back_consumption(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.source_work()
    request = h.reserve_request(h.review())
    h.db.fail_statement = lambda sql, params: "INSERT INTO" in sql and "action" in params
    with pytest.raises(MonitoringUnavailable):
        h.store.reserve_action(request)
    assert not h.approval_row(request.approval.approval_id)["consumed_at"]
    assert h.store.get_incident_state(request.incident) is None
    assert h.store.get_action_by_request(m.MonitoringContext(**h.context()), request.idempotency_id) is None
    decision = h.store.reserve_action(request)
    assert decision.status == "reserved"
    assert h.approval_row(request.approval.approval_id)["consumed_at"]
    assert h.store.get_incident_state(request.incident).action_count == 1
    assert any("JSON_MODIFY(payload, '$.consumed_at'" in sql for sql, _ in h.db.statements)


@pytest.mark.parametrize("commit", ["before", "after"])
def test_sql_ambiguous_reservation_commit_is_reconciled_by_original_identity(sql_harness: SqlHarness, commit: str) -> None:
    h = sql_harness
    h.source_work()
    request = h.reserve_request(h.review())
    h.db.fail_commit = commit
    with pytest.raises(MonitoringCommitUncertain) as failed:
        h.store.reserve_action(request)
    assert failed.value.idempotency_id == request.idempotency_id
    receipt = h.store.get_action_by_request(m.MonitoringContext(**h.context()), request.idempotency_id)
    generic = h.store.get_operation_receipt(
        m.MonitoringContext(**h.context()), failed.value.operation, failed.value.idempotency_id,
    )
    if commit == "before":
        assert receipt is None
        assert generic is None
        assert not h.approval_row(request.approval.approval_id)["consumed_at"]
    else:
        assert receipt.status == "reserved"
        assert generic.result["reservation"]["reservation_id"] == receipt.reservation.reservation_id
        assert h.store.reserve_action(request) == receipt
        assert h.store.get_incident_state(request.incident).action_count == 1


def test_sql_finalization_failure_cannot_finish_work_ahead_of_incident(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.source_work()
    h.db.fail_statement = lambda sql, params: "INSERT INTO" in sql and "finalization" in params
    with pytest.raises(MonitoringUnavailable):
        h.finalize()
    names = resolve_tables(h.db)
    assert h.db.query(f"SELECT COUNT(*) FROM [dbo].[{names['incidents']}]")[0][0] == 0
    assert h.db.query(f"SELECT COUNT(*) FROM [dbo].[{names['processed']}]")[0][0] == 0
    assert h.store.get_work(m.MonitoringContext(**h.context()), h.work.work_id).state == "leased"
    assert h.store.get_finalization(m.MonitoringContext(**h.context()), h.finalization_request.finalization_id) is None
    receipt = h.store.finalize_work(h.finalization_request)
    payload = h.db.query(
        f"SELECT payload FROM [dbo].[{names['incidents']}] WHERE incident_id = ?", receipt.incident_id,
    )[0][0]
    assert hashlib.sha256(payload.encode("utf-16-le")).hexdigest() == receipt.incident_payload_hash
    assert h.store.get_work(m.MonitoringContext(**h.context()), h.work.work_id).state == "completed"


def test_sql_lost_finalization_ack_is_not_a_reason_to_repeat_controller_work(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.source_work()
    request = h.finalization_input()
    h.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        h.store.finalize_work(request)
    receipt = h.store.get_finalization(m.MonitoringContext(**h.context()), request.finalization_id)
    assert receipt is not None
    assert h.store.finalize_work(request) == receipt
    assert h.claim(("triage",)) == ()


@pytest.mark.parametrize("exception_type", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_store_finalization_preserves_parent_transaction_interrupt_type_and_rolls_back(
    sql_harness: SqlHarness, exception_type: type[BaseException],
) -> None:
    h = sql_harness
    h.source_work()
    request = h.finalization_input()
    interruption = exception_type("Fixture client interruption")

    def interrupt_after_receipt(sql, params):
        if "INSERT INTO" in sql and "finalization" in params:
            h.db.fail_statement = None
            raise interruption
        return False

    h.db.fail_statement = interrupt_after_receipt
    with pytest.raises(exception_type) as caught:
        h.store.finalize_work(request)
    assert caught.value is interruption
    names = resolve_tables(h.db)
    assert h.db.query(f"SELECT COUNT(*) FROM [dbo].[{names['incidents']}]")[0][0] == 0
    assert h.db.query(f"SELECT COUNT(*) FROM [dbo].[{names['processed']}]")[0][0] == 0
    assert h.store.get_finalization(m.MonitoringContext(**h.context()), request.finalization_id) is None
    assert h.store.get_work(m.MonitoringContext(**h.context()), h.work.work_id).state == "leased"


def test_sql_large_keys_use_fixed_digest_indices_and_check_full_identity(sql_harness: SqlHarness) -> None:
    h = sql_harness
    backend = h.store._backend
    context = m.MonitoringContext(**h.context())
    key = "k" * 1_024
    with backend.transaction(write=True, operation="fixture_record", request_id=uid(99)):
        backend.put(StoredRecord(kind="fixture_record", key=key, context=context, payload='{"value":1}', version=1))
        assert backend.get("fixture_record", key, context).key == key
    table = resolve_tables(h.db)["monitoring_records"]
    row = h.db.query(
        f"SELECT key_hash, full_key FROM [dbo].[{table}] WHERE record_kind = ?", "fixture_record",
    )[0]
    assert len(row[0]) == 32
    assert len(row[1]) == 1_024
    with h.db.transaction():
        h.db.execute(
            f"UPDATE [dbo].[{table}] SET full_key = ? WHERE record_kind = ?", "different", "fixture_record",
        )
    with pytest.raises(MonitoringUnavailable):
        with backend.transaction(write=False, operation="fixture_read", request_id=uid(99)):
            backend.get("fixture_record", key, context)


def test_sql_honors_shared_table_overrides_and_needs_no_runtime_ddl(tmp_path: Path) -> None:
    tables = {
        "incidents": "fixture_incidents", "processed": "fixture_processed",
        "approvals": "fixture_approvals", "monitoring_records": "fixture_monitoring_records",
    }
    h = SqlHarness(tmp_path / "configured.sqlite", tables)
    h.seed()
    h.activate()
    h.source_work()
    request = h.reserve_request(h.review())
    action = h.store.reserve_action(request).reservation
    h.finalize(action=action)
    assert h.db.query("SELECT COUNT(*) FROM [dbo].[fixture_incidents]")[0][0] == 1
    with pytest.raises(MonitoringKernelUnsupported, match="Base-table runtime DML"):
        runtime_table_permissions(tables)
    assert any("[dbo].[fixture_monitoring_records]" in sql for sql, _ in h.db.statements)


def test_sql_rejection_and_successor_are_one_atomic_unit(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "rejected-atomic.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review("powerbi_refresh"), approval=False)).reservation
    request = rejection_request(h, action, retry_after=30)
    h.db.fail_statement = lambda sql, params: "INSERT INTO" in sql and "action_rejection" in params
    with pytest.raises(MonitoringUnavailable):
        h.store.record_action_rejection(request)
    retained = h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id)
    assert retained.state == "reserved"
    assert retained.retry_work_id is None
    assert h.store.get_operation_receipt(m.MonitoringContext(**h.context()), "action_rejection", request.request_id) is None
    rejected = h.store.record_action_rejection(request)
    assert rejected.state == "rejected"
    assert rejected.retry_work_id is not None
    assert h.store.get_incident_state(action.request.incident).action_count == 1
    assert h.store.record_action_rejection(request) == rejected


def test_sql_lost_rejection_ack_reconciles_the_same_successor(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "rejected-ack.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    action = h.store.reserve_action(h.reserve_request(h.review("powerbi_refresh"), approval=False)).reservation
    request = rejection_request(h, action)
    h.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        h.store.record_action_rejection(request)
    rejected = h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id)
    replayed = h.store.record_action_rejection(request)
    assert replayed.retry_work_id == rejected.retry_work_id
    assert h.store.get_incident_state(action.request.incident).action_count == 1
    successor = h.store.get_work(m.MonitoringContext(**h.context()), rejected.retry_work_id)
    assert successor.retry_of == action.reservation_id
    assert successor.due_at == h.clock() + timedelta(seconds=900)


def test_sql_configuration_outcome_uses_reserved_parameter_hash_and_retains_mismatch(tmp_path: Path) -> None:
    h = SqlHarness(tmp_path / "configuration-hash.sqlite")
    h.seed(workload="powerbi")
    h.activate()
    h.source_work()
    review = h.review("reenable_refresh_schedule", {"enabled": True})
    h.store.observe_source(h.observation(
        execution=m.SourceExecutionIdentity(target=h.targets[0], run_id_kind="powerbi_request", run_id=uid(48_000)),
        status="succeeded", started_at=h.clock() - timedelta(minutes=2), ended_at=h.clock() - timedelta(minutes=1),
    ), work_id=h.work.work_id, lease=h.work.lease)
    action = h.store.reserve_action(h.reserve_request(review)).reservation
    action = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        state="submitted", configuration_action="reenable_refresh_schedule", submitted_at=h.clock(),
        next_verification_at=h.clock() + timedelta(seconds=30), detail="The re-enable request was accepted.",
    ))
    evidence = m.ConfigurationVerification(
        target=h.targets[0], action="reenable_refresh_schedule",
        expected_hash=action.request.parameter_hash, configuration={"enabled": False},
        observed_at=h.clock(), authority="rest",
    )
    request = m.ActionOutcomeRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence,
        disposition="uncertain", configuration=evidence, observed_at=h.clock(),
        detail="Readback is still disabled; no successful re-enable is claimed.",
    )
    forged = m.ActionOutcomeRequest.model_validate({
        **request.model_dump(), "configuration": {
            **evidence.model_dump(), "expected_hash": m._digest({"enabled": False}),
        },
    })
    with pytest.raises(MonitoringConflict):
        h.store.record_action_outcome(forged)
    assert h.store.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id).state == "submitted"
    saved = h.store.record_action_outcome(request)
    assert saved.state == "uncertain"
    assert saved.configuration.configuration == {"enabled": False}
    assert saved.configuration.expected_hash == review.parameter_hash
    assert h.store.get_incident_state(action.request.incident).action_count == 1


@pytest.mark.parametrize("workload", ["fabric_pipeline", "powerbi"])
def test_sql_cancellation_is_terminal_only_for_complete_pipeline_evidence(tmp_path: Path, workload: m.Workload) -> None:
    h = SqlHarness(tmp_path / f"cancelled-{workload}.sqlite")
    h.seed(workload=workload)
    h.activate()
    h.source_work()
    review = h.review("pipeline_rerun" if workload == "fabric_pipeline" else "powerbi_refresh")
    intent = h.reserve_request(review)
    action = h.store.reserve_action(intent).reservation
    submitted = m.SourceExecutionIdentity(
        target=h.targets[0], run_id=uid(49_100),
        run_id_kind="fabric_job" if workload == "fabric_pipeline" else "powerbi_request",
    )
    action = h.store.record_action_submission(m.ActionSubmissionRequest(
        **h.context(), request_id=h.next_id(), reservation_id=action.reservation_id,
        expected_reservation_revision=action.revision, action_fence=action.fence, state="submitted",
        submitted_execution=submitted, correlation="response_run_id", submitted_at=h.clock(),
        next_verification_at=h.clock() + timedelta(seconds=30), detail="Accepted fixture execution.",
    ))
    began = h.clock()
    h.clock.advance(1)
    fields = {
        **h.context(), "request_id": h.next_id(), "reservation_id": action.reservation_id,
        "expected_reservation_revision": action.revision, "action_fence": action.fence,
        "disposition": "verified_failed", "submitted_execution": submitted,
        "observation": h.observation(execution=submitted, status="cancelled", started_at=began, ended_at=h.clock()),
        "observed_at": h.clock(), "activities_complete": True,
        "detail": "Exact REST pipeline cancellation is terminal non-success.",
    }
    if workload == "powerbi":
        with pytest.raises(ValidationError):
            m.ActionOutcomeRequest.model_validate(fields)
        unresolved = h.store.record_action_outcome(m.ActionOutcomeRequest.model_validate({
            **fields, "disposition": "uncertain",
        }))
        assert unresolved.state == "uncertain"
        assert unresolved.next_verification_at is not None
        assert h.store.get_incident_state(intent.incident).action_count == 1
        assert h.approval_row(intent.approval.approval_id)["consumed_at"]
        return
    outcome = m.ActionOutcomeRequest.model_validate(fields)
    h.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        h.store.record_action_outcome(outcome)
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    cancelled = other.get_action_reservation(m.MonitoringContext(**h.context()), action.reservation_id)
    assert cancelled.state == "verified_failed"
    assert other.record_action_outcome(outcome) == cancelled
    assert other.get_source(submitted).status == "cancelled"
    assert other.get_incident_state(intent.incident).action_count == 1
    assert h.approval_row(intent.approval.approval_id)["consumed_at"]
    h.finalize(action=cancelled, outcome="needs_human")
    assert other.get_incident(intent.incident).outcome == "needs_human"
    h.clock.advance(121)
    assert h.claim(("verify_action",)) == ()


def test_sql_partition_ownership_cas_and_modification_time_are_shared(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.connector()
    h.signal(100)
    assert isinstance(h.store, EventPersistence)
    initial = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner),
    ))
    h.clock.advance(5)
    other_db = SqliteAzureDatabase(h.db.path, h.clock)
    other = SqlProtocolFixtureStore(db=other_db)
    scope = ConnectorScope(**h.context(), connector_id=h.connector_id, consumer_group="$Default")
    assert other.list_partition_ownership(scope) == (initial,)
    changed = other.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=initial.etag,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(99)),
    ))
    assert changed.modified_at == h.clock()
    assert changed.lease.acquired_at == h.clock()
    assert changed.lease.fence > initial.lease.fence
    assert h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=initial.etag,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(98)),
    )) is None
    with pytest.raises(MonitoringConflict):
        h.store.renew_lease(m.LeaseRenewal(lease=initial.lease))
    released = other.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=changed.etag, release=changed.lease,
    ))
    assert released.lease is None
    claimed = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=released.etag,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(99)),
    ))
    assert claimed.lease.fence > changed.lease.fence
    assert any("owner_id = ? AND fence = ?" in sql for sql, _ in other_db.statements)


def test_sql_ownership_metadata_failure_rolls_back_partition_transfer(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.connector()
    h.signal(100)
    original = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner),
    ))
    h.db.fail_statement = lambda sql, params: "UPDATE" in sql and "partition_ownership" in params
    with pytest.raises(MonitoringUnavailable):
        h.store.change_partition_ownership(OwnershipChange(
            partition=h.partition, expected_etag=original.etag,
            claim=m.PartitionClaimRequest(partition=h.partition, owner_id=uid(99)),
        ))
    scope = ConnectorScope(**h.context(), connector_id=h.connector_id, consumer_group="$Default")
    assert h.store.list_partition_ownership(scope) == (original,)
    assert h.store.renew_lease(m.LeaseRenewal(lease=original.lease)).owner_id == h.owner


def test_sql_ownership_etag_race_has_one_winner_and_lost_ack_is_inspectable(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.connector()
    h.signal(100)
    original = h.store.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=None,
        claim=m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner),
    ))
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    def transfer(store, owner):
        return store.change_partition_ownership(OwnershipChange(
            partition=h.partition, expected_etag=original.etag,
            claim=m.PartitionClaimRequest(partition=h.partition, owner_id=owner),
        ))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(transfer, h.store, uid(90)), pool.submit(transfer, other, uid(91))]
        results = [future.result() for future in futures]
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    winner = winners[0]
    h.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        h.store.change_partition_ownership(OwnershipChange(
            partition=h.partition, expected_etag=winner.etag, release=winner.lease,
        ))
    scope = ConnectorScope(**h.context(), connector_id=h.connector_id, consumer_group="$Default")
    released = other.list_partition_ownership(scope)[0]
    assert released.lease is None
    assert released.etag != winner.etag
    assert other.change_partition_ownership(OwnershipChange(
        partition=h.partition, expected_etag=winner.etag, release=winner.lease,
    )) is None


def test_sql_unidentified_position_commit_is_recoverable_and_contiguous(sql_harness: SqlHarness) -> None:
    h = sql_harness
    h.connector()
    first = h.signal(100)
    lease = h.start_partition(first_sequence_number=100)
    h.store.record_stream_receipts(m.StreamReceiptBatch(
        request_id=h.next_id(), partition=h.partition, lease=lease, receipts=(first,),
    ))
    malformed = UnidentifiedSignal(
        partition=h.partition,
        position=m.StreamPosition(offset="1010", sequence_number=101, enqueued_at=h.clock()),
        received_at=h.clock(), quarantine=m.QuarantineDisposition(
            observation_id="fixture-malformed", reason="malformed",
            detail="The envelope has no validated CloudEvents identity.",
            metadata={"body_sha256": hashlib.sha256(b"invalid").hexdigest()},
        ),
    )
    request = UnidentifiedReceiptBatch(request_id=h.next_id(), lease=lease, receipt=malformed)
    h.db.fail_statement = lambda sql, params: "INSERT INTO" in sql and "stream_position" in params
    with pytest.raises(MonitoringUnavailable):
        h.store.record_unidentified_receipts(request)
    assert h.store.get_stream_acceptance(m.MonitoringContext(**h.context()), request.request_id) is None
    with pytest.raises(MonitoringConflict):
        h.store.advance_stream_checkpoint(m.StreamCheckpointAdvance(
            request_id=h.next_id(), partition=h.partition, lease=lease,
            expected_revision=0, through=malformed.position,
        ))
    h.db.fail_commit = "after"
    with pytest.raises(MonitoringCommitUncertain):
        h.store.record_unidentified_receipts(request)
    persisted = h.store.get_stream_acceptance(m.MonitoringContext(**h.context()), request.request_id)
    assert persisted.work_ids == ()
    assert h.store.record_unidentified_receipts(request) == persisted
    checkpoint = h.store.advance_stream_checkpoint(m.StreamCheckpointAdvance(
        request_id=h.next_id(), partition=h.partition, lease=lease,
        expected_revision=0, through=malformed.position,
    ))
    assert checkpoint.position.sequence_number == 101


def test_sql_stream_boundary_and_heartbeat_do_not_manufacture_delivery_proof(sql_harness: SqlHarness) -> None:
    h = sql_harness
    connector = h.connector()
    h.signal(500)
    lease = h.store.claim_partition(m.PartitionClaimRequest(partition=h.partition, owner_id=h.owner))
    assert h.store.get_stream_start(h.partition) is None
    start = h.store.ensure_stream_start(StreamStartRequest(
        partition=h.partition, lease=lease, first_available_sequence_number=500, observed_at=h.clock(),
    ))
    other = SqlProtocolFixtureStore(db=SqliteAzureDatabase(h.db.path, h.clock))
    assert other.get_stream_start(h.partition) == start
    h.clock.advance(1)
    heartbeat = ReceiverHeartbeat(
        **h.context(), connector_id=h.connector_id, worker_id=h.owner,
        observed_at=h.clock(), state="running", transport_connected=True,
        accepted_positions=1, last_delivery_at=h.clock(),
    )
    h.store.record_receiver_heartbeat(heartbeat)
    current = next(item for item in other.list_connectors(m.PageQuery(**h.context())).items if item.connector_id == h.connector_id)
    assert current.delivery_verified_at == connector.delivery_verified_at
    assert current.identity_verified_at == connector.identity_verified_at
    assert current == connector
    assert other.get_stream_checkpoint(h.partition) is None
    assert other.list_receiver_heartbeats(m.MonitoringContext(**h.context()), connector_id=h.connector_id) == (heartbeat,)


async def test_real_event_adapter_uses_shared_sql_ownership_unidentified_intake_and_start(sql_harness: SqlHarness) -> None:
    h = sql_harness
    endpoint = m.EndpointMetadata(namespace="fixture.servicebus.windows.net", entity="fixture-hub", consumer_group="$Default")
    manifest = h.connector(endpoint=endpoint, destination_id=uid(44))
    binding = ConnectorBinding(
        tenant_id=h.control.tenant_id, connector_id=manifest.connector_id,
        workspace_id=manifest.workspace_id, eventstream_id=manifest.eventstream_id,
        destination_id=manifest.destination_id, endpoint=endpoint,
    )
    adapter = SqlCheckpointStore(
        h.store, persistence=h.store, binding=binding,
        context=m.MonitoringContext(**h.context()), clock=h.clock,
    )
    adapter.bind_partitions(("0",))
    properties = {
        "id": "0", "eventhub_name": endpoint.entity, "beginning_sequence_number": 100,
        "last_enqueued_sequence_number": 100, "is_empty": False,
    }
    adapter.set_partition_properties("0", properties)
    identity = {
        "fully_qualified_namespace": endpoint.namespace, "eventhub_name": endpoint.entity,
        "consumer_group": endpoint.consumer_group, "partition_id": "0",
    }
    owners = await adapter.claim_ownership([{**identity, "owner_id": h.owner}])
    assert owners[0]["last_modified_time"] == h.clock().timestamp()
    assert type(owners[0]["last_modified_time"]) is float
    await adapter.initialize_partition("0", properties)
    position = m.StreamPosition(offset="2000", sequence_number=100, enqueued_at=h.clock())
    receipt = await adapter.accept("0", position, summarize_body(b"not-json"))
    assert isinstance(receipt, UnidentifiedSignal)
    await adapter.update_checkpoint({**identity, "sequence_number": 100, "offset": "2000"})
    assert h.store.get_stream_checkpoint(adapter.partition("0")).position == position
    assert adapter.accepted_positions == 1
