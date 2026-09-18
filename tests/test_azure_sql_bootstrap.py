from __future__ import annotations

import base64
import copy
import hashlib
import importlib.util
import json
import os
import socket
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError

from triage.store.azure_sql import AzureSqlDatabase, SqlCommitUncertain, SqlRollbackUncertain

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "bootstrap_azure_sql.py"
BICEP = ROOT / "infra" / "state-sql-bootstrap.bicep"
SPEC = importlib.util.spec_from_file_location("azure_sql_bootstrap_under_test", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
bootstrap = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bootstrap
SPEC.loader.exec_module(bootstrap)
OPERATION = UUID("11111111-1111-4111-8111-111111111111")
REPLACEMENT = UUID("55555555-5555-4555-8555-555555555555")
STARTED_AT = "2026-09-17T10:00:00.000001"
DDL = "CREATE TABLE dbo.sample (id INT NOT NULL)"


@pytest.fixture(autouse=True)
def forbid_live_transport(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Bootstrap tests must not use a live network")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)


@pytest.fixture
def payload(tmp_path):
    sources = {
        "src/triage/__init__.py": "from __future__ import annotations\n",
        "src/triage/redaction.py": "from __future__ import annotations\n",
        "src/triage/store/azure_sql.py": "from __future__ import annotations\n",
        "scripts/bootstrap_azure_sql.py": SCRIPT.read_text(encoding="utf-8"),
    }
    for relative, text in sources.items():
        path = tmp_path.joinpath(*relative.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
    (tmp_path / "schema.sql").write_bytes(DDL.encode("utf-8"))
    return {
        "version": 1, "operation_id": str(OPERATION), "ddl_owner": "dbo",
        "target": {
            "server": "state-test.database.windows.net",
            "application_database": "triage-state",
            "database": "triage-state-proof", "kind": "proof", "private_ip": "10.2.3.4",
        },
        "identity": {
            "tenant_id": "22222222-2222-4222-8222-222222222222",
            "client_id": "33333333-3333-4333-8333-333333333333",
            "object_id": "44444444-4444-4444-8444-444444444444",
        },
        "source_files": {
            path: hashlib.sha256(text.encode("utf-8")).hexdigest()
            for path, text in sources.items()
        },
        "batches": [{"path": "schema.sql", "sha256": hashlib.sha256(DDL.encode()).hexdigest()}],
        "checks": [
            {"kind": "object", "name": "sample-object", "argument": "dbo.sample",
             "expected": [["U", "dbo", None]]},
            {"kind": "columns", "name": "sample-columns", "argument": "dbo.sample",
             "expected": [["id", "int", 4, 10, 0, 0, 0, 0]]},
            {"kind": "principal", "name": "sample-role", "argument": "sample_reader",
             "expected": [["R", "NONE", None, None]]},
            {"kind": "permissions", "name": "sample-permissions", "argument": "sample_reader",
             "expected": [["OBJECT_OR_COLUMN", "dbo.sample", 0, "SELECT", "GRANT"]]},
            {"kind": "members", "name": "sample-members", "argument": "sample_reader",
             "expected": [["sample_user"]]},
        ],
    }


def write_bundle(root, payload):
    path = root / "bundle.json"
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest()


@pytest.fixture
def artifact(tmp_path, payload):
    path, digest = write_bundle(tmp_path, payload)
    return bootstrap.load_artifact(tmp_path, path, digest, OPERATION)


def deployed_environment(artifact):
    bundle = artifact.bundle
    return {
        "AZURE_SQL_SERVER": bundle.target.server,
        "AZURE_SQL_DATABASE": bundle.target.database,
        "BOOTSTRAP_APPLICATION_DATABASE": bundle.target.application_database,
        "BOOTSTRAP_TARGET_KIND": bundle.target.kind,
        "AZURE_TENANT_ID": str(bundle.identity.tenant_id),
        "AZURE_CLIENT_ID": str(bundle.identity.client_id),
        "BOOTSTRAP_IDENTITY_OBJECT_ID": str(bundle.identity.object_id),
    }


class Engine:
    """SQL protocol double; the production AzureSqlDatabase owns transactions."""

    def __init__(self, artifact):
        self.artifact = artifact
        self.state = {
            "receipt_table": False, "receipts": {}, "effects": [],
            "recovery_table": False, "recoveries": {},
            "catalogue": {name: [] for name in bootstrap.EMPTY_BASELINE_SQL},
        }
        self.state["catalogue"].update({
            "database": [(7, False, False, 1, "01")],
            "principals": [
                (0, "R", "NONE", "00", False, 1, "public", None),
                (1, "S", "INSTANCE", "01", False, 0, "dbo", "dbo"),
                (2, "S", "NONE", "02", False, 1, "guest", "guest"),
                (3, "S", "NONE", "03", False, 1, "INFORMATION_SCHEMA", None),
                (4, "S", "NONE", "04", False, 1, "sys", None),
                (16384, "R", "NONE", "05", True, 1, "db_owner", None),
            ],
            "permissions": [(0, 1, 0, 0, 0, "CONNECT", "G")],
            "memberships": [(1, 16384)],
            "schemas": [(1, "dbo", 1), (2, "guest", 2), (3, "INFORMATION_SCHEMA", 3), (4, "sys", 4)],
        })
        self.commands = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_commit = None
        self.fail_commit_before = False
        self.fail_rollback = False
        self.fail_batch = False
        self.bad_metadata = False
        self.lock_rows = [(0,)]
        self.transaction_rows = [(1, 1)]
        self.update_count = 1
        self.recovery_insert_count = 1
        self.other_sessions = []
        self.other_principals = []
        self.recovery_wrong_type = False
        self.unreadable_catalogue = None
        self.target_rows = [(
            "state-test", artifact.bundle.target.database, 5, "dbo", 1, "dbo", 42,
        )]

    def database(self, monkeypatch):
        db = AzureSqlDatabase(
            server=self.artifact.bundle.target.server, database=self.artifact.bundle.target.database,
        )
        monkeypatch.setattr(db, "_connect", lambda: Connection(self))
        return db


class Connection:
    def __init__(self, engine):
        self.engine = engine
        self.autocommit = True
        self.pending = None

    def cursor(self):
        return Cursor(self)

    def data(self):
        if self.autocommit:
            return self.engine.state
        if self.pending is None:
            self.pending = copy.deepcopy(self.engine.state)
        return self.pending

    def commit(self):
        self.engine.commits += 1
        if self.engine.fail_commit == self.engine.commits and self.engine.fail_commit_before:
            raise RuntimeError("Connection lost before native commit")
        if self.pending is not None:
            self.engine.state = self.pending
            self.pending = None
        if self.engine.fail_commit == self.engine.commits:
            raise RuntimeError("Commit acknowledgement lost")

    def rollback(self):
        self.engine.rollbacks += 1
        if self.engine.fail_rollback:
            raise RuntimeError("Rollback acknowledgement lost")
        self.pending = None

    def close(self):
        self.pending = None


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = -1

    def execute(self, sql, *params):
        engine = self.connection.engine
        state = self.connection.data()
        engine.commands.append((sql, params, self.connection.autocommit))
        if sql == bootstrap.TARGET_SQL:
            self.rows = engine.target_rows
        elif sql == bootstrap.LOCK_SQL:
            self.rows = engine.lock_rows
        elif sql == bootstrap.CREATE_RECEIPTS:
            state["receipt_table"] = True
        elif sql == bootstrap.CREATE_RECOVERIES:
            assert not state["recovery_table"]
            state["recovery_table"] = True
        elif sql == bootstrap.RECOVERY_OBJECT_SQL:
            self.rows = (
                [(None, 124)] if engine.recovery_wrong_type else
                [(123, 123)] if state["recovery_table"] else [(None, None)]
            )
        elif sql.startswith("SELECT OBJECT_ID"):
            table = "receipt_table" if params[0] == bootstrap.RECEIPT_TABLE else "recovery_table"
            self.rows = [(123 if state[table] else None,)]
        elif sql.startswith(f"SELECT {bootstrap.RECOVERY_COLUMNS}"):
            self.rows = [state["recoveries"][params[0]]] if params[0] in state["recoveries"] else []
        elif sql.startswith("SELECT original_operation_id"):
            self.rows = [(params[0],)] if params[0] in state["recoveries"] else []
        elif sql.startswith("SELECT fingerprint"):
            self.rows = [state["receipts"][params[0]]] if params[0] in state["receipts"] else []
            if "started_at" in sql:
                self.rows = [(*row, STARTED_AT, None) for row in self.rows]
        elif sql.startswith("SELECT COUNT_BIG"):
            if sql == f"SELECT COUNT_BIG(*) FROM {bootstrap.RECEIPT_TABLE}":
                self.rows = [(len(state["receipts"]),)]
            elif params:
                pending = 0
                for operation, row in state["receipts"].items():
                    if row[2] != "started":
                        continue
                    recovery = state["recoveries"].get(operation)
                    resolved = recovery and recovery[:2] == row[:2] and (
                        recovery[2:5] == params
                        or state["receipts"].get(recovery[2]) == (*recovery[3:5], "committed")
                    )
                    pending += not resolved
                self.rows = [(pending,)]
            else:
                self.rows = [(sum(row[2] == "started" for row in state["receipts"].values()),)]
        elif sql.startswith(f"INSERT INTO {bootstrap.RECOVERY_TABLE}"):
            assert params[0] not in state["recoveries"]
            self.rowcount = engine.recovery_insert_count
            if self.rowcount == 1:
                state["recoveries"][params[0]] = params[1:]
        elif sql.startswith(f"INSERT INTO {bootstrap.RECEIPT_TABLE}"):
            assert params[0] not in state["receipts"]
            state["receipts"][params[0]] = (*params[1:], "started")
            self.rowcount = 1
        elif sql.startswith("UPDATE "):
            previous = state["receipts"].get(params[0])
            self.rowcount = engine.update_count if previous == (*params[1:], "started") else 0
            if self.rowcount == 1:
                state["receipts"][params[0]] = (*params[1:], "committed")
        elif sql.startswith("SET "):
            pass
        elif sql == "SELECT @@TRANCOUNT, XACT_STATE()":
            self.rows = engine.transaction_rows
        elif sql == bootstrap.OTHER_SESSIONS_SQL:
            self.rows = engine.other_sessions
        elif sql == bootstrap.EMPTY_SCHEMA_SQL:
            self.rows = [("dbo", "sample", "U")] if state["effects"] else []
        elif sql == bootstrap.EMPTY_PRINCIPALS_SQL:
            self.rows = engine.other_principals
        elif any(sql == definition[1] for definition in bootstrap.EMPTY_BASELINE_SQL.values()):
            name = next(name for name, definition in bootstrap.EMPTY_BASELINE_SQL.items() if sql == definition[1])
            if engine.unreadable_catalogue == name:
                raise RuntimeError("Native catalogue query unavailable")
            self.rows = state["catalogue"][name]
        elif sql == DDL:
            if engine.fail_batch:
                raise ValueError("driver text containing a token must not be printed")
            state["effects"].append(sql)
        elif sql in bootstrap.CHECK_SQL.values():
            assert state["effects"], "Readback cannot pretend schema was already installed"
            expected = next(
                check.expected for check in engine.artifact.bundle.checks
                if bootstrap.CHECK_SQL[check.kind] == sql and check.argument == params[0]
            )
            self.rows = [] if engine.bad_metadata else [tuple(row) for row in expected]
        else:
            raise AssertionError("Unmodelled SQL; no native transport is allowed")

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def test_read_only_default_performs_no_statement_or_transaction(artifact, monkeypatch):
    engine = Engine(artifact)
    result = bootstrap.run(engine.database(monkeypatch), artifact, "preflight")
    assert result == {"status": "preflight_only", "schema_checked": False, "batches_executed": 0}
    assert [sql for sql, _, _ in engine.commands] == [bootstrap.TARGET_SQL]
    assert engine.commits == 0
    assert not engine.state["receipt_table"]


def test_apply_uses_real_shared_transaction_and_reads_committed_receipt(artifact, monkeypatch):
    engine = Engine(artifact)
    result = bootstrap.run(engine.database(monkeypatch), artifact, "apply", artifact.fingerprint)
    assert result["status"] == "committed_and_read_back"
    assert result["batches_executed"] == 1
    assert engine.commits == 2
    assert engine.state["receipts"][str(OPERATION)] == (
        artifact.fingerprint, artifact.source_sha256, "committed",
    )
    assert engine.state["effects"] == [DDL]
    assert not next(auto for sql, _, auto in engine.commands if sql == DDL)
    assert engine.commands[-1][2] is True


@pytest.mark.parametrize("mode,approval", [("apply", ""), ("apply", "0" * 64), ("retry", "")])
def test_unapproved_or_unknown_action_stops_before_sql(mode, approval, artifact, monkeypatch):
    engine = Engine(artifact)
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.run(engine.database(monkeypatch), artifact, mode, approval)
    assert not engine.commands


@pytest.mark.parametrize(
    "row",
    [
        ("other-server", "triage-state-proof", 5, "dbo", 1, "dbo", 42),
        ("state-test", "triage-state", 5, "dbo", 1, "dbo", 42),
        ("state-test", "triage-state-proof", 8, "dbo", 1, "dbo", 42),
        ("state-test", "triage-state-proof", 5, "runtime", 1, "dbo", 42),
        ("state-test", "triage-state-proof", 5, "dbo", 0, "dbo", 42),
        ("state-test", "triage-state-proof", 5, "dbo", 1, "runtime", 42),
        ("state-test", "triage-state-proof", 5, "dbo", 1, "dbo", None),
        ("state-test", "triage-state-proof"),
    ],
)
def test_wrong_server_database_engine_or_owner_never_writes(row, artifact, monkeypatch):
    engine = Engine(artifact)
    engine.target_rows = [row]
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.run(engine.database(monkeypatch), artifact, "apply", artifact.fingerprint)
    assert engine.commits == 0
    assert not engine.state["receipt_table"]


@pytest.mark.parametrize("commit_number", [1, 2])
def test_lost_ack_uses_original_durable_receipt_never_replays(commit_number, artifact, monkeypatch):
    engine = Engine(artifact)
    engine.fail_commit = commit_number
    with pytest.raises(SqlCommitUncertain):
        bootstrap.run(engine.database(monkeypatch), artifact, "apply", artifact.fingerprint)
    assert engine.state["effects"] == ([] if commit_number == 1 else [DDL])
    before = list(engine.state["effects"])
    independent = engine.database(monkeypatch)
    if commit_number == 1:
        with pytest.raises(bootstrap.BootstrapError, match="receipt_uncommitted"):
            bootstrap.run(independent, artifact, "reconcile")
        different = replace(artifact, bundle=artifact.bundle.model_copy(update={
            "operation_id": UUID("55555555-5555-4555-8555-555555555555"),
        }), fingerprint="f" * 64)
        for attempt in (artifact, different):
            with pytest.raises(bootstrap.BootstrapError, match="pending_operation"):
                bootstrap.run(independent, attempt, "apply", attempt.fingerprint)
    else:
        result = bootstrap.run(independent, artifact, "reconcile")
        assert result["status"] == "committed_and_read_back"
        assert bootstrap.run(independent, artifact, "apply", artifact.fingerprint)["batches_executed"] == 0
    assert engine.state["effects"] == before


@pytest.mark.parametrize("failure", ["batch", "metadata", "receipt", "transaction"])
def test_batch_readback_or_receipt_failure_keeps_durable_pending_fence(failure, artifact, monkeypatch):
    engine = Engine(artifact)
    engine.fail_batch = failure == "batch"
    engine.bad_metadata = failure == "metadata"
    engine.update_count = 0 if failure == "receipt" else 1
    engine.transaction_rows = [(0, 0)] if failure == "transaction" else [(1, 1)]
    with pytest.raises((ValueError, bootstrap.BootstrapError)):
        bootstrap.run(engine.database(monkeypatch), artifact, "apply", artifact.fingerprint)
    assert engine.commits == 1
    assert engine.rollbacks == 1
    assert engine.state["receipts"][str(OPERATION)][2] == "started"
    assert not engine.state["effects"]


def test_lost_rollback_is_uncertain_not_permission_to_restart(artifact, monkeypatch):
    engine = Engine(artifact)
    engine.fail_batch = True
    engine.fail_rollback = True
    with pytest.raises(SqlRollbackUncertain):
        bootstrap.run(engine.database(monkeypatch), artifact, "apply", artifact.fingerprint)
    assert engine.state["receipts"][str(OPERATION)][2] == "started"
    assert engine.commits == 1


@pytest.mark.parametrize("rows", [[(-1,)], [(False,)], [("0",)], [], [(0,), (0,)], [(0, 0)]])
def test_applock_requires_typed_single_result_not_exec_rowcount(rows, artifact, monkeypatch):
    engine = Engine(artifact)
    engine.lock_rows = rows
    with pytest.raises(bootstrap.BootstrapError, match="busy"):
        bootstrap.run(engine.database(monkeypatch), artifact, "apply", artifact.fingerprint)
    assert not engine.state["receipt_table"]


def test_reconcile_absence_or_changed_fingerprint_never_creates_state(artifact, monkeypatch):
    engine = Engine(artifact)
    db = engine.database(monkeypatch)
    with pytest.raises(bootstrap.BootstrapError, match="uncommitted"):
        bootstrap.run(db, artifact, "reconcile")
    assert not engine.state["receipt_table"]
    bootstrap.run(db, artifact, "apply", artifact.fingerprint)
    before = copy.deepcopy(engine.state)
    with pytest.raises(bootstrap.BootstrapError, match="conflict"):
        bootstrap.run(db, replace(artifact, fingerprint="a" * 64), "reconcile")
    assert engine.state == before


def test_committed_receipt_does_not_override_current_metadata_failure(artifact, monkeypatch):
    engine = Engine(artifact)
    db = engine.database(monkeypatch)
    bootstrap.run(db, artifact, "apply", artifact.fingerprint)
    engine.bad_metadata = True
    with pytest.raises(bootstrap.BootstrapError, match="metadata_mismatch"):
        bootstrap.run(engine.database(monkeypatch), artifact, "reconcile")
    assert engine.state["effects"] == [DDL]
    assert engine.state["receipts"][str(OPERATION)][2] == "committed"


@pytest.mark.parametrize("change", ["source", "sql", "extra-source", "operation", "manifest"])
def test_any_changed_bound_input_is_refused(change, tmp_path, payload):
    path, digest = write_bundle(tmp_path, payload)
    operation = OPERATION
    if change == "source":
        (tmp_path / "src" / "triage" / "redaction.py").write_text("changed", encoding="utf-8")
    elif change == "sql":
        (tmp_path / "schema.sql").write_text("changed", encoding="utf-8")
    elif change == "extra-source":
        (tmp_path / "src" / "unexpected.py").write_text("changed", encoding="utf-8")
    elif change == "operation":
        operation = UUID("55555555-5555-4555-8555-555555555555")
    else:
        path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.load_artifact(tmp_path, path, digest, operation)


@pytest.mark.parametrize("relative", [
    "../outside.sql", "/tmp/outside.sql", "C:\\outside.sql", "./schema.sql", "sql//schema.sql",
])
def test_bundle_paths_cannot_escape_image(relative, tmp_path, payload):
    payload["batches"][0]["path"] = relative
    path, digest = write_bundle(tmp_path, payload)
    with pytest.raises(bootstrap.BootstrapError, match="path"):
        bootstrap.load_artifact(tmp_path, path, digest, OPERATION)


@pytest.mark.parametrize(
    "key,value",
    [("kind", "application"), ("database", "master"), ("private_ip", "8.8.8.8"),
     ("private_ip", "127.0.0.1"), ("private_ip", "169.254.169.254"),
     ("server", "server.database.windows.net;Encrypt=no")],
)
def test_target_cannot_be_relabelled_or_widened(key, value, tmp_path, payload):
    payload["target"][key] = value
    path, digest = write_bundle(tmp_path, payload)
    with pytest.raises(ValidationError):
        bootstrap.load_artifact(tmp_path, path, digest, OPERATION)


def test_duplicate_json_and_caller_supplied_preflight_sql_are_refused(tmp_path, payload):
    path, _ = write_bundle(tmp_path, payload)
    raw = path.read_bytes().replace(b'"version":1', b'"version":1,"version":1')
    path.write_bytes(raw)
    with pytest.raises(bootstrap.BootstrapError, match="duplicate"):
        bootstrap.load_artifact(tmp_path, path, hashlib.sha256(raw).hexdigest(), OPERATION)
    payload["checks"][0]["sql"] = "DELETE FROM dbo.sample"
    path, digest = write_bundle(tmp_path, payload)
    with pytest.raises(ValidationError):
        bootstrap.load_artifact(tmp_path, path, digest, OPERATION)


@pytest.mark.parametrize("field", ["operation_id", "tenant_id", "client_id", "object_id"])
def test_identity_only_placeholders_cannot_be_executed(field, tmp_path, payload):
    zero = "00000000-0000-0000-0000-000000000000"
    if field == "operation_id":
        payload[field] = zero
    else:
        payload["identity"][field] = zero
    path, digest = write_bundle(tmp_path, payload)
    with pytest.raises(ValidationError, match="Nonempty"):
        bootstrap.load_artifact(tmp_path, path, digest, UUID(payload["operation_id"]))


@pytest.mark.parametrize("change", ["owner", "empty-columns", "no-objects", "duplicate-batch", "go"])
def test_schema_contract_is_explicit_and_batches_are_separate(change, tmp_path, payload):
    if change == "owner":
        payload["checks"][0]["expected"][0][1] = "runtime"
    elif change == "empty-columns":
        payload["checks"][1]["expected"] = []
    elif change == "no-objects":
        payload["checks"] = payload["checks"][1:]
    elif change == "duplicate-batch":
        payload["batches"].append(payload["batches"][0])
    else:
        content = (DDL + "\nGO\n").encode()
        (tmp_path / "schema.sql").write_bytes(content)
        payload["batches"][0]["sha256"] = hashlib.sha256(content).hexdigest()
    path, digest = write_bundle(tmp_path, payload)
    with pytest.raises((bootstrap.BootstrapError, ValidationError)):
        bootstrap.load_artifact(tmp_path, path, digest, OPERATION)


@pytest.mark.parametrize("key", [
    "AZURE_SQL_SERVER", "AZURE_SQL_DATABASE", "BOOTSTRAP_APPLICATION_DATABASE",
    "BOOTSTRAP_TARGET_KIND", "AZURE_TENANT_ID", "AZURE_CLIENT_ID", "BOOTSTRAP_IDENTITY_OBJECT_ID",
])
def test_missing_or_wrong_deployed_binding_fails_closed(key, artifact):
    environment = deployed_environment(artifact)
    bootstrap.check_environment(artifact.bundle, environment)
    environment.pop(key)
    with pytest.raises(bootstrap.BootstrapError, match="deployed"):
        bootstrap.check_environment(artifact.bundle, environment)


def token_for(artifact, **overrides):
    claims = {
        "tid": str(artifact.bundle.identity.tenant_id), "oid": str(artifact.bundle.identity.object_id),
        "appid": str(artifact.bundle.identity.client_id),
        "aud": "https://database.windows.net/", "exp": int(time.time()) + 3600,
    }
    claims.update(overrides)
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


@pytest.mark.parametrize("claim,value", [
    ("tid", "wrong"), ("oid", "wrong"), ("appid", "wrong"), ("azp", "wrong"),
    ("aud", "https://management.azure.com/"), ("exp", 0), ("exp", "9999999999"),
    ("nbf", 9999999999),
])
def test_managed_identity_token_claims_are_pinned(claim, value, artifact):
    bootstrap.check_token(token_for(artifact), artifact.bundle.identity)
    with pytest.raises(bootstrap.BootstrapError, match="claims"):
        bootstrap.check_token(token_for(artifact, **{claim: value}), artifact.bundle.identity)


@pytest.mark.parametrize("addresses", [["8.8.8.8"], ["10.2.3.4", "8.8.8.8"], ["10.2.3.5"]])
def test_dns_must_match_only_the_approved_private_ip(addresses, artifact, monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 1433)) for address in addresses
    ])
    with pytest.raises(bootstrap.BootstrapError, match="private_dns"):
        bootstrap.open_database(ROOT, artifact.bundle)


def test_credential_is_exact_uami_without_secret_or_developer_fallback(artifact, monkeypatch):
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.2.3.4", 1433)),
    ])
    captured = {}

    def credential(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(get_token=lambda scope: SimpleNamespace(token=token_for(artifact)))

    identity = ModuleType("azure.identity")
    identity.ManagedIdentityCredential = credential
    monkeypatch.setitem(sys.modules, "azure", ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    db = bootstrap.open_database(ROOT, artifact.bundle)
    assert isinstance(db, AzureSqlDatabase)
    assert captured == {
        "client_id": str(artifact.bundle.identity.client_id),
        "connection_timeout": 10, "read_timeout": 20, "retry_total": 0,
    }
    with pytest.raises(bootstrap.BootstrapError, match="scope"):
        db._credential.get_token("https://management.azure.com/.default")


def test_public_target_is_default_and_still_uses_pinned_identity(tmp_path, payload, monkeypatch):
    del payload["target"]["private_ip"]
    path, digest = write_bundle(tmp_path, payload)
    artifact = bootstrap.load_artifact(tmp_path, path, digest, OPERATION)
    assert artifact.bundle.target.private_ip is None
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("20.40.60.80", 1433)),
    ])
    captured = {}

    def credential(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(get_token=lambda scope: SimpleNamespace(token=token_for(artifact)))

    identity = ModuleType("azure.identity")
    identity.ManagedIdentityCredential = credential
    monkeypatch.setitem(sys.modules, "azure", ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    db = bootstrap.open_database(ROOT, artifact.bundle)
    assert isinstance(db, AzureSqlDatabase)
    assert db.target == "triage-state-proof on state-test.database.windows.net"
    assert captured["client_id"] == str(artifact.bundle.identity.client_id)


def test_no_dns_answer_is_not_a_public_network_fallback(artifact, monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [])
    with pytest.raises(bootstrap.BootstrapError, match="sql_dns_unavailable"):
        bootstrap.open_database(ROOT, artifact.bundle)


def test_old_installed_application_is_rejected_before_credential_creation(artifact, tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.2.3.4", 1433)),
    ])
    with pytest.raises(bootstrap.BootstrapError, match="stale_application"):
        bootstrap.open_database(tmp_path, artifact.bundle)


def test_cli_defaults_to_read_only_and_logs_hashes_not_payload(tmp_path, artifact, monkeypatch, capsys):
    monkeypatch.setattr(bootstrap, "ROOT", tmp_path)
    for key, value in deployed_environment(artifact).items():
        monkeypatch.setenv(key, value)
    engine = Engine(artifact)
    monkeypatch.setattr(bootstrap, "open_database", lambda *args: engine.database(monkeypatch))
    assert bootstrap.main([
        "--bundle", str(tmp_path / "bundle.json"), "--bundle-sha256", artifact.fingerprint,
        "--operation-id", str(OPERATION),
    ]) == 0
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[0]["status"] == "inputs_verified"
    assert output[0]["source_sha256"] == artifact.source_sha256
    assert output[1]["status"] == "sql_preflight_verified"
    assert output[1]["sql_identity"] == {
        "server_identity": "state-test", "database": "triage-state-proof",
        "engine_edition": 5, "principal": "dbo", "control_database": 1,
        "ddl_owner": "dbo", "session_id": 42,
    }
    assert output[-1]["status"] == "preflight_only"
    assert output[-1]["runtime_identity_acceptance"] == "not_performed"
    assert not engine.state["receipt_table"]
    assert DDL not in json.dumps(output)


def test_cli_uncertain_commit_retains_identity_and_reports_failure(tmp_path, artifact, monkeypatch, capsys):
    monkeypatch.setattr(bootstrap, "ROOT", tmp_path)
    for key, value in deployed_environment(artifact).items():
        monkeypatch.setenv(key, value)
    engine = Engine(artifact)
    engine.fail_commit = 2
    monkeypatch.setattr(bootstrap, "open_database", lambda *args: engine.database(monkeypatch))
    assert bootstrap.main([
        "--bundle", str(tmp_path / "bundle.json"), "--bundle-sha256", artifact.fingerprint,
        "--operation-id", str(OPERATION), "--mode", "apply",
        "--approve-fingerprint", artifact.fingerprint,
    ]) == 3
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[-1]["status"] == "failed"
    assert output[-1]["error_type"] == "SqlCommitUncertain"
    assert output[-1]["operation_id"] == str(OPERATION)
    assert output[-1]["fingerprint"] == artifact.fingerprint
    assert len(engine.state["effects"]) == 1


@pytest.fixture
def interrupted(artifact, monkeypatch, tmp_path):
    engine = Engine(artifact)
    engine.fail_batch = True
    with pytest.raises(ValueError):
        bootstrap.run(engine.database(monkeypatch), artifact, "apply", artifact.fingerprint)
    engine.fail_batch = False
    replacement = replace(
        artifact, fingerprint="d" * 64,
        bundle=artifact.bundle.model_copy(update={"operation_id": REPLACEMENT}),
    )
    now = datetime.now(UTC)
    job = (
        "/subscriptions/66666666-6666-4666-8666-666666666666/resourceGroups/test"
        "/providers/Microsoft.App/jobs/bootstrap"
    )
    request = {
        "version": 1, "original_operation_id": str(OPERATION),
        "original_fingerprint": artifact.fingerprint, "original_source_sha256": artifact.source_sha256,
        "original_started_at": STARTED_AT, "original_receipt_object_id": 123,
        "original_job_id": job, "rollback_evidence_sha256": "a" * 64,
        "empty_baseline_sha256": bootstrap.recovery_baseline_fingerprint(engine.database(monkeypatch), replacement.bundle),
        "replacement_operation_id": str(REPLACEMENT), "replacement_fingerprint": replacement.fingerprint,
        "replacement_source_sha256": replacement.source_sha256,
        "observed_at": (now - timedelta(seconds=10)).isoformat(),
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
        "jobs": [{
            "id": job, "definition_sha256": "b" * 64, "mode": "reconcile",
            "executions": [{"id": job + "/executions/bootstrap-old", "status": "Failed"}],
        }],
    }
    path = tmp_path / "recovery.json"
    raw = json.dumps(request).encode()
    path.write_bytes(raw)
    recovery = bootstrap.load_recovery(path, hashlib.sha256(raw).hexdigest(), replacement)
    return engine, replacement, recovery


def recover_interrupted(engine, artifact, recovery, monkeypatch):
    return bootstrap.run(
        engine.database(monkeypatch), artifact, "recover", artifact.fingerprint,
        recovery, recovery.fingerprint,
    )


def test_recovery_appends_resolution_and_only_licenses_reviewed_replacement(interrupted, artifact, monkeypatch):
    engine, replacement, recovery = interrupted
    original = copy.deepcopy(engine.state["receipts"])
    result = recover_interrupted(engine, replacement, recovery, monkeypatch)
    assert result["status"] == "empty_rollback_adjudicated"
    assert result["batches_executed"] == 0
    assert engine.state["receipts"] == original
    assert not engine.state["effects"]
    assert engine.state["recoveries"][str(OPERATION)] == bootstrap._recovery_values(recovery)
    wrong_replacement = replace(replacement, fingerprint="e" * 64)
    for attempt in (artifact, wrong_replacement):
        with pytest.raises(bootstrap.BootstrapError, match="pending_operation"):
            bootstrap.run(engine.database(monkeypatch), attempt, "apply", attempt.fingerprint)
    result = bootstrap.run(engine.database(monkeypatch), replacement, "apply", replacement.fingerprint)
    assert result["status"] == "committed_and_read_back"
    assert engine.state["receipts"][str(OPERATION)] == original[str(OPERATION)]
    assert engine.state["receipts"][str(REPLACEMENT)][2] == "committed"
    assert engine.state["effects"] == [DDL]


@pytest.mark.parametrize("missing", ["bundle-approval", "recovery", "recovery-approval", "wrong-mode"])
def test_recovery_requires_separate_explicit_approval_before_sql(missing, interrupted, monkeypatch):
    engine, replacement, recovery = interrupted
    engine.commands.clear()
    with pytest.raises(bootstrap.BootstrapError):
        bootstrap.run(
            engine.database(monkeypatch), replacement,
            "apply" if missing == "wrong-mode" else "recover",
            "" if missing == "bundle-approval" else replacement.fingerprint,
            None if missing == "recovery" else recovery,
            "" if missing == "recovery-approval" else recovery.fingerprint,
        )
    assert not engine.commands
    assert not engine.state["recovery_table"]


@pytest.mark.parametrize("field,value", [
    ("original_fingerprint", "e" * 64), ("original_source_sha256", "e" * 64),
    ("original_receipt_object_id", 124), ("original_started_at", "2026-09-17T10:00:00.000002"),
    ("original_operation_id", UUID("77777777-7777-4777-8777-777777777777")),
    ("replacement_fingerprint", "e" * 64),
])
def test_recovery_refuses_changed_original_or_replacement(field, value, interrupted, monkeypatch):
    engine, replacement, recovery = interrupted
    recovery = replace(recovery, request=recovery.request.model_copy(update={field: value}))
    before = copy.deepcopy(engine.state)
    with pytest.raises(bootstrap.BootstrapError, match="recovery_"):
        recover_interrupted(engine, replacement, recovery, monkeypatch)
    assert engine.state == before


@pytest.mark.parametrize("condition", ["schema", "principal", "session", "receipt", "expired", "insert"])
def test_recovery_proves_empty_schema_and_current_quiescence(condition, interrupted, monkeypatch):
    engine, replacement, recovery = interrupted
    if condition == "schema":
        engine.state["effects"].append(DDL)
    elif condition == "principal":
        engine.other_principals = [("runtime", "E")]
    elif condition == "session":
        engine.other_sessions = [(43,)]
    elif condition == "receipt":
        engine.state["receipts"][str(REPLACEMENT)] = ("e" * 64, "f" * 64, "started")
    elif condition == "expired":
        recovery = replace(recovery, request=recovery.request.model_copy(update={
            "expires_at": datetime.now(UTC) - timedelta(seconds=1),
        }))
    else:
        engine.recovery_insert_count = 0
    before = copy.deepcopy(engine.state)
    with pytest.raises(bootstrap.BootstrapError, match="recovery_"):
        recover_interrupted(engine, replacement, recovery, monkeypatch)
    assert engine.state == before


def test_recovery_lost_ack_reconciles_immutable_resolution_without_another_insert(interrupted, monkeypatch):
    engine, replacement, recovery = interrupted
    engine.fail_commit = engine.commits + 1
    with pytest.raises(SqlCommitUncertain):
        recover_interrupted(engine, replacement, recovery, monkeypatch)
    before = copy.deepcopy(engine.state)
    engine.commands.clear()
    result = recover_interrupted(engine, replacement, recovery, monkeypatch)
    assert result["status"] == "empty_rollback_adjudicated"
    assert engine.state == before
    assert not any(sql.startswith(("INSERT", "CREATE", "UPDATE", "DELETE")) for sql, _, _ in engine.commands)
    conflicting = replace(recovery, fingerprint="e" * 64)
    with pytest.raises(bootstrap.BootstrapError, match="recovery_receipt_conflict"):
        recover_interrupted(engine, replacement, conflicting, monkeypatch)
    assert engine.state == before


def test_failed_replacement_keeps_both_original_and_new_fences(interrupted, monkeypatch):
    engine, replacement, recovery = interrupted
    recover_interrupted(engine, replacement, recovery, monkeypatch)
    engine.fail_batch = True
    with pytest.raises(ValueError):
        bootstrap.run(engine.database(monkeypatch), replacement, "apply", replacement.fingerprint)
    assert all(row[2] == "started" for row in engine.state["receipts"].values())
    assert not engine.state["effects"]
    with pytest.raises(bootstrap.BootstrapError, match="pending_operation"):
        bootstrap.run(engine.database(monkeypatch), replacement, "apply", replacement.fingerprint)


@pytest.mark.parametrize("change", ["running", "apply", "wrong-execution", "unfenced", "stale-window", "naive"])
def test_quiescence_manifest_is_typed_and_bounded(change, interrupted):
    _, _, recovery = interrupted
    request = recovery.request.model_dump(mode="json")
    if change == "running":
        request["jobs"][0]["executions"][0]["status"] = "Running"
    elif change == "apply":
        request["jobs"][0]["mode"] = "apply"
    elif change == "wrong-execution":
        request["jobs"][0]["executions"][0]["id"] = "other/executions/wrong"
    elif change == "unfenced":
        request["original_job_id"] = "other-job"
    elif change == "stale-window":
        request["expires_at"] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    else:
        request["observed_at"] = "2026-09-17T10:00:00"
    with pytest.raises(ValidationError):
        bootstrap.RecoveryRequest.model_validate_json(json.dumps(request))


@pytest.fixture
def cli_interrupted(interrupted, tmp_path, monkeypatch):
    engine, _, recovery = interrupted
    document = json.loads((tmp_path / "bundle.json").read_bytes())
    document["operation_id"] = str(REPLACEMENT)
    path, digest = write_bundle(tmp_path, document)
    artifact = bootstrap.load_artifact(tmp_path, path, digest, REPLACEMENT)
    request = recovery.request.model_dump(mode="json")
    request.update(
        replacement_operation_id=str(REPLACEMENT), replacement_fingerprint=artifact.fingerprint,
        replacement_source_sha256=artifact.source_sha256,
    )
    raw = json.dumps(request).encode()
    recovery_path = tmp_path / "recovery.json"
    recovery_path.write_bytes(raw)
    recovery = bootstrap.load_recovery(recovery_path, hashlib.sha256(raw).hexdigest(), artifact)
    monkeypatch.setattr(bootstrap, "ROOT", tmp_path)
    for key, value in deployed_environment(artifact).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(bootstrap, "open_database", lambda *args: engine.database(monkeypatch))
    args = [
        "--bundle", str(path), "--bundle-sha256", artifact.fingerprint,
        "--operation-id", str(REPLACEMENT), "--recovery", str(recovery_path),
        "--recovery-sha256", recovery.fingerprint,
    ]
    return engine, artifact, recovery, args


def cli_recover(artifact, recovery, arguments):
    return bootstrap.main([
        *arguments, "--mode", "recover", "--approve-fingerprint", artifact.fingerprint,
        "--approve-recovery-sha256", recovery.fingerprint,
    ])


@pytest.mark.parametrize("catalogue,row", [
    ("permissions", (0, 1, 0, 0, 0, "CREATE TABLE", "G")),
    ("permissions", (0, 1, 3, 1, 0, "CONTROL", "G")),
    ("permissions", (0, 1, 1, 123, 2, "UPDATE", "G")),
    ("permissions", (0, 1, 0, 0, 0, "CONNECT", "W")),
    ("permissions", (0, 1, 0, 0, 0, "CONNECT", "D")),
    ("memberships", (2, 16384)),
    ("schemas", (20, "left_by_failed_ddl", 1)),
    ("triggers", (900, 0, 0, "database_ddl_trigger", False, False, False, None, "a" * 64)),
    ("credentials", (1, "leftover", "Managed Identity", "2026-09-17T10:00:00", "2026-09-17T10:00:00")),
    ("external_data_sources", (1, "leftover", "RDBMS", "server.example.invalid", 1)),
    ("external_file_formats", (1, "leftover", "DELIMITEDTEXT")),
    ("assemblies", (1, "leftover", 1, 3, True, "2026-09-17T10:00:00", "assembly")),
    ("types", (500, 1, "leftover", 56, True, False, False)),
    ("xml_schema_collections", (5, 1, "leftover")),
    ("certificates", (1, "leftover", 1)),
    ("symmetric_keys", (1, "leftover", 1)),
    ("asymmetric_keys", (1, "leftover", 1)),
    ("column_master_keys", (1, "leftover")),
    ("column_encryption_keys", (1, "leftover")),
    ("module_signatures", (1, 123, "aa", "SPVC")),
    ("queues", (200, True, True, "dbo.activation", 1, False)),
    ("plan_guides", (1, "leftover", False, 1)),
    ("objects", (123, 1, "triage_sql_bootstrap_receipts", "U", 0, 2, False, None, None, None)),
    ("columns", (123, 1, "operation_id", 56, 4, 10, 0, False, False, False, None)),
    ("indexes", (123, 1, "changed_key", 1, False, True, False, None)),
    ("index_columns", (123, 1, 1, 2, 1, False, False)),
    ("constraints", (125, 123, "unreviewed_receipt_check", "(1=1)", False)),
])
def test_cli_recovery_rejects_unrolled_security_or_catalogue_effects(
    catalogue, row, cli_interrupted, capsys,
):
    engine, artifact, recovery, arguments = cli_interrupted
    engine.state["catalogue"][catalogue].append(row)
    before = copy.deepcopy(engine.state)
    engine.commands.clear()
    assert cli_recover(artifact, recovery, arguments) == 2
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[-1]["status"] == "failed"
    assert output[-1]["code"].startswith(("recovery_empty_baseline_changed", "recovery_empty_security_not_standard"))
    assert engine.state == before
    assert not engine.state["recovery_table"]
    assert not any(sql in (bootstrap.CREATE_RECOVERIES,) or sql.startswith(
        f"INSERT INTO {bootstrap.RECOVERY_TABLE}"
    ) for sql, _, _ in engine.commands)
    queries = {definition[1] for definition in bootstrap.EMPTY_BASELINE_SQL.values()}
    assert all(not autocommit for sql, _, autocommit in engine.commands if sql in queries)
    assert any(sql == bootstrap.EMPTY_BASELINE_SQL[catalogue][1] for sql, _, _ in engine.commands)


@pytest.mark.parametrize("change", ["removed_public_permission", "schema_owner", "database_flags"])
def test_cli_recovery_compares_original_approved_empty_baseline(change, cli_interrupted, capsys):
    engine, artifact, recovery, arguments = cli_interrupted
    if change == "removed_public_permission":
        engine.state["catalogue"]["permissions"].clear()
    elif change == "schema_owner":
        engine.state["catalogue"]["schemas"][1] = (2, "guest", 1)
    else:
        engine.state["catalogue"]["database"][0] = (7, True, False, 1, "01")
    before = copy.deepcopy(engine.state)
    assert cli_recover(artifact, recovery, arguments) == 2
    assert engine.state == before
    assert not engine.state["recovery_table"]
    assert "recovery_" in capsys.readouterr().out


@pytest.mark.parametrize("catalogue", ["permissions", "memberships", "schemas", "triggers", "credentials", "assemblies"])
def test_unreadable_native_security_catalogue_never_becomes_empty(catalogue, cli_interrupted, capsys):
    engine, artifact, recovery, arguments = cli_interrupted
    engine.unreadable_catalogue = catalogue
    before = copy.deepcopy(engine.state)
    assert cli_recover(artifact, recovery, arguments) == 2
    assert engine.state == before
    assert not engine.state["recovery_table"]
    assert "bootstrap_failed" in capsys.readouterr().out


@pytest.mark.parametrize("rows", [[(0,)], [("malformed",) * 7], [(0, 1, 0, 0, 0, object(), "G")]])
def test_security_catalogue_shape_or_unknown_values_fail_closed(rows, interrupted, monkeypatch):
    engine, artifact, recovery = interrupted
    engine.state["catalogue"]["permissions"] = rows
    with pytest.raises(bootstrap.BootstrapError, match="recovery_"):
        recover_interrupted(engine, artifact, recovery, monkeypatch)
    assert not engine.state["recovery_table"]


def test_security_catalogue_budget_fails_closed(interrupted, monkeypatch):
    engine, artifact, recovery = interrupted
    engine.state["catalogue"]["permissions"] *= bootstrap.MAX_BASELINE_ROWS + 1
    with pytest.raises(bootstrap.BootstrapError, match="baseline_unreadable"):
        recover_interrupted(engine, artifact, recovery, monkeypatch)
    assert not engine.state["recovery_table"]


@pytest.mark.parametrize("catalogue,row", [
    ("permissions", (0, 1, 0, 0, 0, "CREATE TABLE", "G")),
    ("schemas", (20, "leftover", 1)),
    ("memberships", (2, 16384)),
    ("triggers", (900, 0, 0, "leftover", False, False, False, None, "a" * 64)),
    ("credentials", (1, "leftover", "Managed Identity", "2026-09-17T10:00:00", "2026-09-17T10:00:00")),
])
def test_current_unsafe_security_cannot_be_blessed_as_an_empty_baseline(
    catalogue, row, interrupted, monkeypatch,
):
    engine, artifact, _ = interrupted
    engine.state["catalogue"][catalogue].append(row)
    engine.commands.clear()
    with pytest.raises(bootstrap.BootstrapError, match="empty_security_not_standard"):
        bootstrap.recovery_baseline_fingerprint(engine.database(monkeypatch), artifact.bundle)
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)


def test_fresh_recovery_without_approved_baseline_never_writes(interrupted, monkeypatch):
    engine, artifact, recovery = interrupted
    recovery = replace(recovery, request=recovery.request.model_copy(update={"empty_baseline_sha256": None}))
    before = copy.deepcopy(engine.state)
    with pytest.raises(bootstrap.BootstrapError, match="approved_empty_baseline_required"):
        recover_interrupted(engine, artifact, recovery, monkeypatch)
    assert engine.state == before


def test_empty_baseline_is_checked_under_original_lock_before_recovery_dcl(interrupted, monkeypatch):
    engine, artifact, recovery = interrupted
    engine.commands.clear()
    recover_interrupted(engine, artifact, recovery, monkeypatch)
    commands = [sql for sql, _, _ in engine.commands]
    lock = commands.index(bootstrap.LOCK_SQL)
    create = commands.index(bootstrap.CREATE_RECOVERIES)
    for _, query in bootstrap.EMPTY_BASELINE_SQL.values():
        assert lock < commands.index(query) < create
    assert all(not auto for sql, _, auto in engine.commands if sql in {
        query for _, query in bootstrap.EMPTY_BASELINE_SQL.values()
    })


@pytest.mark.parametrize("before_commit", [True, False])
def test_cli_original_recovery_lookup_after_lost_ack_is_read_only(
    before_commit, cli_interrupted, monkeypatch, capsys,
):
    engine, artifact, recovery, arguments = cli_interrupted
    engine.fail_commit = engine.commits + 1
    engine.fail_commit_before = before_commit
    assert cli_recover(artifact, recovery, arguments) == 3
    capsys.readouterr()
    before = copy.deepcopy(engine.state)
    commits, rollbacks = engine.commits, engine.rollbacks
    engine.commands.clear()
    monkeypatch.setattr(bootstrap, "datetime", SimpleNamespace(
        now=lambda timezone: recovery.request.expires_at + timedelta(days=1),
    ))
    expected = "MISSING" if before_commit else "MATCHING"
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == (2 if before_commit else 0)
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[-1]["status"] == expected
    assert output[-1]["read_only"] is True
    assert output[-1]["recovery_sha256"] == recovery.fingerprint
    assert output[-1]["original_operation_id"] == str(OPERATION)
    assert engine.state == before
    assert (engine.commits, engine.rollbacks) == (commits, rollbacks)
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)


@pytest.mark.parametrize("existing_table", [False, True])
def test_recovery_lookup_missing_record_is_not_a_create_request(existing_table, interrupted, monkeypatch):
    engine, artifact, recovery = interrupted
    engine.state["recovery_table"] = existing_table
    engine.commands.clear()
    before = copy.deepcopy(engine.state)
    result = bootstrap.run(engine.database(monkeypatch), artifact, "reconcile-recovery", recovery=recovery)
    assert result["status"] == "MISSING"
    assert engine.state == before
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)


@pytest.mark.parametrize("change", ["fingerprint", "source", "replacement", "object_type", "original"])
def test_cli_recovery_lookup_reports_conflict_without_mutation(change, cli_interrupted, capsys):
    engine, artifact, recovery, arguments = cli_interrupted
    assert cli_recover(artifact, recovery, arguments) == 0
    capsys.readouterr()
    row = list(engine.state["recoveries"][str(OPERATION)])
    if change in {"fingerprint", "source", "replacement"}:
        index = {"fingerprint": 5, "source": 1, "replacement": 2}[change]
        row[index] = "e" * 64 if change != "replacement" else str(UUID(int=777))
        engine.state["recoveries"][str(OPERATION)] = tuple(row)
    elif change == "object_type":
        engine.recovery_wrong_type = True
    else:
        engine.state["receipts"][str(OPERATION)] = (recovery.request.original_fingerprint, "e" * 64, "started")
    before = copy.deepcopy(engine.state)
    engine.commands.clear()
    commits, rollbacks = engine.commits, engine.rollbacks
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == 2
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[-1]["status"] == "CONFLICT" and output[-1]["read_only"] is True
    assert output[-1]["code"].startswith("recovery_")
    assert engine.state == before
    assert (engine.commits, engine.rollbacks) == (commits, rollbacks)
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)


def test_expired_original_without_baseline_can_only_reconcile(interrupted, monkeypatch):
    engine, artifact, recovery = interrupted
    legacy = replace(recovery, request=recovery.request.model_copy(update={
        "empty_baseline_sha256": None,
        "observed_at": datetime.now(UTC) - timedelta(days=1, minutes=10),
        "expires_at": datetime.now(UTC) - timedelta(days=1),
    }))
    engine.state["recovery_table"] = True
    engine.state["recoveries"][str(OPERATION)] = bootstrap._recovery_values(legacy)
    before = copy.deepcopy(engine.state)
    engine.commands.clear()
    result = bootstrap.run(engine.database(monkeypatch), artifact, "reconcile-recovery", recovery=legacy)
    assert result["status"] == "MATCHING"
    assert engine.state == before
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)


def test_cli_preserves_hash_binding_for_expired_pre_baseline_recovery(
    cli_interrupted, tmp_path, capsys,
):
    engine, artifact, _, arguments = cli_interrupted
    path = tmp_path / "recovery.json"
    request = json.loads(path.read_bytes())
    request.pop("empty_baseline_sha256")
    request["observed_at"] = (datetime.now(UTC) - timedelta(days=1, minutes=10)).isoformat()
    request["expires_at"] = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    raw = json.dumps(request).encode()
    path.write_bytes(raw)
    fingerprint = hashlib.sha256(raw).hexdigest()
    original = bootstrap.load_recovery(path, fingerprint, artifact)
    engine.state["recovery_table"] = True
    engine.state["recoveries"][str(OPERATION)] = bootstrap._recovery_values(original)
    arguments[arguments.index("--recovery-sha256") + 1] = fingerprint
    before = copy.deepcopy(engine.state)
    engine.commands.clear()
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == 0
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[-1]["status"] == "MATCHING"
    assert output[-1]["recovery_sha256"] == fingerprint
    assert engine.state == before
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)
    path.write_bytes(raw + b" ")
    engine.commands.clear()
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == 2
    assert "recovery_hash_mismatch" in capsys.readouterr().out
    assert not engine.commands
    assert engine.state == before


@pytest.mark.parametrize("field", ["approval", "recovery_approval", "missing_request"])
def test_read_only_recovery_mode_requires_original_request_and_no_approvals(field, interrupted, monkeypatch):
    engine, artifact, recovery = interrupted
    engine.commands.clear()
    with pytest.raises(bootstrap.BootstrapError, match="lookup_requires_original"):
        bootstrap.run(
            engine.database(monkeypatch), artifact, "reconcile-recovery",
            approval=artifact.fingerprint if field == "approval" else "",
            recovery=None if field == "missing_request" else recovery,
            recovery_approval=recovery.fingerprint if field == "recovery_approval" else "",
        )
    assert not engine.commands


@pytest.fixture
def historical_recovery(interrupted, tmp_path, monkeypatch):
    engine, _, original = interrupted
    history = tmp_path / "historical"
    document = json.loads((tmp_path / "bundle.json").read_bytes())
    document["operation_id"] = str(REPLACEMENT)
    old_runner = (
        b"from __future__ import annotations\n"
        b"import argparse\n"
        b"parser = argparse.ArgumentParser()\n"
        b"parser.add_argument('--mode', choices=('preflight', 'apply', 'recover', 'reconcile'))\n"
        b"parser.parse_args()\n"
        b"raise AssertionError('Historical runner must never execute')\n"
    )
    for name in document["source_files"]:
        path = history.joinpath(*name.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = old_runner if name == "scripts/bootstrap_azure_sql.py" else (
            b"raise AssertionError('Historical Python must never be imported')\n"
        )
        path.write_bytes(raw)
        document["source_files"][name] = hashlib.sha256(raw).hexdigest()
    sql = b"THROW 51079, 'Historical SQL must never execute', 1;"
    (history / "schema.sql").write_bytes(sql)
    document["batches"][0]["sha256"] = hashlib.sha256(sql).hexdigest()
    bundle_path, fingerprint = write_bundle(history, document)
    artifact = bootstrap.load_artifact(history, bundle_path, fingerprint, REPLACEMENT)
    request = original.request.model_dump(mode="json")
    request.pop("empty_baseline_sha256")
    request.update(
        replacement_fingerprint=artifact.fingerprint, replacement_source_sha256=artifact.source_sha256,
        observed_at=(datetime.now(UTC) - timedelta(days=1, minutes=10)).isoformat(),
        expires_at=(datetime.now(UTC) - timedelta(days=1)).isoformat(),
    )
    request_path = history / "recovery.json"
    raw = json.dumps(request).encode()
    request_path.write_bytes(raw)
    recovery = bootstrap.load_recovery(request_path, hashlib.sha256(raw).hexdigest(), artifact)
    assert recovery.request.empty_baseline_sha256 is None
    assert document["source_files"]["scripts/bootstrap_azure_sql.py"] != hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    assert ROOT != history
    for key, value in deployed_environment(artifact).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(bootstrap, "ROOT", ROOT)
    monkeypatch.setattr(sys, "path", list(sys.path))
    credential_calls = []

    def credential(**kwargs):
        credential_calls.append(kwargs)
        return SimpleNamespace(get_token=lambda scope: SimpleNamespace(token=token_for(artifact)))

    identity = ModuleType("azure.identity")
    identity.ManagedIdentityCredential = credential
    monkeypatch.setitem(sys.modules, "azure", ModuleType("azure"))
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.2.3.4", 1433)),
    ])
    monkeypatch.setattr(AzureSqlDatabase, "_connect", lambda self: Connection(engine))
    engine.commands.clear()
    arguments = [
        "--bundle", str(bundle_path), "--bundle-sha256", artifact.fingerprint,
        "--operation-id", str(REPLACEMENT), "--recovery", str(request_path),
        "--recovery-sha256", recovery.fingerprint, "--artifact-root", str(history),
    ]
    return engine, artifact, recovery, arguments, history, credential_calls


@pytest.mark.parametrize("status", ["MATCHING", "MISSING", "CONFLICT"])
def test_current_cli_reads_historical_recovery_with_only_trusted_code(status, historical_recovery, capsys):
    engine, artifact, recovery, arguments, history, credential_calls = historical_recovery
    if status != "MISSING":
        engine.state["recovery_table"] = True
        values = bootstrap._recovery_values(recovery)
        engine.state["recoveries"][str(OPERATION)] = (
            (*values[:5], "e" * 64, values[6]) if status == "CONFLICT" else values
        )
    before = copy.deepcopy(engine.state)
    commits, rollbacks = engine.commits, engine.rollbacks
    files_before = {path: path.read_bytes() for path in history.rglob("*") if path.is_file()}
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == (0 if status == "MATCHING" else 2)
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[-1]["status"] == status and output[-1]["read_only"] is True
    assert output[-1]["fingerprint"] == artifact.fingerprint
    assert output[-1]["source_sha256"] == artifact.source_sha256
    assert output[-1]["recovery_sha256"] == recovery.fingerprint
    assert credential_calls[0]["client_id"] == str(artifact.bundle.identity.client_id)
    assert engine.state == before
    assert (engine.commits, engine.rollbacks) == (commits, rollbacks)
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)
    assert sys.path[0] == str((ROOT / "src").resolve())
    assert not any(Path(value).is_relative_to(history) for value in sys.path if value)
    for name in ("triage", "triage.redaction", "triage.store.azure_sql"):
        assert Path(sys.modules[name].__file__).resolve().is_relative_to(ROOT / "src")
    assert {path: path.read_bytes() for path in history.rglob("*") if path.is_file()} == files_before


@pytest.mark.parametrize("before_commit", [True, False])
def test_historical_cli_lookup_after_original_lost_ack_never_retries(
    before_commit, historical_recovery, monkeypatch, capsys,
):
    engine, _, recovery, arguments, _, _ = historical_recovery
    engine.fail_commit = engine.commits + 1
    engine.fail_commit_before = before_commit
    database = engine.database(monkeypatch)
    with pytest.raises(SqlCommitUncertain), database.transaction():
        database.execute(bootstrap.CREATE_RECOVERIES)
        database.execute(
            f"INSERT INTO {bootstrap.RECOVERY_TABLE} (original_operation_id,"
            "original_fingerprint,original_source_sha256,replacement_operation_id,"
            "replacement_fingerprint,replacement_source_sha256,recovery_sha256,"
            "original_receipt_object_id) VALUES (?,?,?,?,?,?,?,?)",
            str(OPERATION), *bootstrap._recovery_values(recovery),
        )
    before = copy.deepcopy(engine.state)
    commits, rollbacks = engine.commits, engine.rollbacks
    engine.commands.clear()
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == (2 if before_commit else 0)
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert output[-1]["status"] == ("MISSING" if before_commit else "MATCHING")
    assert output[-1]["recovery_sha256"] == recovery.fingerprint
    assert engine.state == before and (engine.commits, engine.rollbacks) == (commits, rollbacks)
    assert all(sql.lstrip().startswith("SELECT") and auto for sql, _, auto in engine.commands)


def test_historical_root_never_authorizes_an_already_imported_historical_adapter(
    historical_recovery, monkeypatch, capsys,
):
    engine, _, _, arguments, history, credential_calls = historical_recovery
    monkeypatch.setattr(
        sys.modules["triage.store.azure_sql"], "__file__",
        str(history / "src" / "triage" / "store" / "azure_sql.py"),
    )
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == 2
    assert "stale_application_module_refused" in capsys.readouterr().out
    assert not credential_calls and not engine.commands


@pytest.mark.parametrize("change,expected", [
    ("runner", "current_source_hash_mismatch"),
    ("source", "current_source_hash_mismatch"),
    ("sql", "sql_batch_hash_mismatch"),
    ("bundle", "bundle_hash_mismatch"),
    ("request", "recovery_hash_mismatch"),
    ("source-binding", "recovery_replacement_mismatch"),
    ("fingerprint-binding", "recovery_replacement_mismatch"),
    ("target-env", "deployed_target_or_identity_mismatch"),
    ("identity-env", "deployed_target_or_identity_mismatch"),
    ("operation", "operation_id_mismatch"),
    ("missing-source", "incomplete_current_source"),
])
def test_historical_reader_refuses_changed_evidence_before_credentials_or_sql(
    change, expected, historical_recovery, monkeypatch, capsys,
):
    engine, _, _, arguments, history, credential_calls = historical_recovery
    files = {
        "runner": history / "scripts" / "bootstrap_azure_sql.py",
        "source": history / "src" / "triage" / "redaction.py",
        "sql": history / "schema.sql", "bundle": history / "bundle.json", "request": history / "recovery.json",
    }
    if change in files:
        path = files[change]
        path.write_bytes(path.read_bytes() + b" ")
    elif change in {"source-binding", "fingerprint-binding"}:
        path = history / "recovery.json"
        request = json.loads(path.read_bytes())
        request["replacement_source_sha256" if change == "source-binding" else "replacement_fingerprint"] = "e" * 64
        raw = json.dumps(request).encode()
        path.write_bytes(raw)
        arguments[arguments.index("--recovery-sha256") + 1] = hashlib.sha256(raw).hexdigest()
    elif change in {"target-env", "identity-env"}:
        monkeypatch.setenv("AZURE_SQL_DATABASE" if change == "target-env" else "AZURE_CLIENT_ID", "different")
    elif change == "operation":
        arguments[arguments.index("--operation-id") + 1] = str(UUID(int=999))
    else:
        (history / "src" / "triage" / "redaction.py").unlink()
    before = copy.deepcopy(engine.state)
    assert bootstrap.main([*arguments, "--mode", "reconcile-recovery"]) == 2
    assert expected in capsys.readouterr().out
    assert not credential_calls and not engine.commands
    assert engine.state == before


@pytest.mark.parametrize("mode", ["preflight", "apply", "recover", "reconcile"])
def test_historical_artifact_root_is_rejected_in_every_other_cli_mode(mode, historical_recovery, capsys):
    engine, artifact, recovery, arguments, _, credential_calls = historical_recovery
    assert bootstrap.main([
        *arguments, "--mode", mode, "--approve-fingerprint", artifact.fingerprint,
        "--approve-recovery-sha256", recovery.fingerprint,
    ]) == 2
    assert "artifact_root_requires_reconcile_recovery" in capsys.readouterr().out
    assert not credential_calls and not engine.commands


@pytest.mark.parametrize("mode", ["apply", "recover"])
def test_mutating_modes_still_require_this_runners_source_hash(mode, cli_interrupted, tmp_path, capsys):
    engine, artifact, recovery, arguments = cli_interrupted
    (tmp_path / "scripts" / "bootstrap_azure_sql.py").write_text(
        "raise AssertionError('Different runner source')", encoding="utf-8",
    )
    before = copy.deepcopy(engine.state)
    engine.commands.clear()
    assert bootstrap.main([
        *arguments, "--mode", mode, "--approve-fingerprint", artifact.fingerprint,
        "--approve-recovery-sha256", recovery.fingerprint,
    ]) == 2
    assert "current_source_hash_mismatch" in capsys.readouterr().out
    assert not engine.commands and engine.state == before


def test_job_is_manual_finite_small_and_has_no_other_authority():
    text = BICEP.read_text(encoding="utf-8")
    assert "param deployJob bool = false" in text
    assert "param mode string = 'preflight'" in text
    assert "replicaRetryLimit: 0" in text
    assert "parallelism: 1" in text
    assert "replicaCompletionCount: 1" in text
    assert "replicaTimeout: timeoutSeconds" in text
    assert "@maxValue(900)" in text
    assert "cpu: json('0.25')" in text and "memory: '0.5Gi'" in text
    assert "scope: registry" in text
    assert "principalType: 'ServicePrincipal'" in text
    assert "'7f951dda-4ed3-4680-a7ca-43fe172d538d'" in text
    assert "identity: identity.id" in text
    assert "@sha256:${imageDigest}" in text
    assert "'-I'" in text and "'-B'" in text


@pytest.mark.parametrize("forbidden", [
    "Microsoft.Sql/", "Microsoft.Network/", "Microsoft.Graph/", "listKeys(",
    "listCredentials(", "password", "secretRef", "ingress:", "publicNetworkAccess:",
    "SecurityControl", "CostControl", "deploymentScripts", "scheduleTriggerConfig", "eventTriggerConfig",
])
def test_template_cannot_change_existing_services_or_use_credentials(forbidden):
    assert forbidden not in BICEP.read_text(encoding="utf-8")


def test_compiled_job_contract_when_parent_supplies_local_template():
    filename = os.environ.get("AZURE_SQL_BOOTSTRAP_COMPILED_TEMPLATE")
    if not filename:
        pytest.skip("Optional local compilation; default tests never invoke Azure CLI")
    template = json.loads(Path(filename).read_text(encoding="utf-8"))
    resources = template["resources"]
    resources = list(resources.values()) if isinstance(resources, dict) else resources
    assert {resource["type"] for resource in resources if not resource.get("existing")} == {
        "Microsoft.ManagedIdentity/userAssignedIdentities",
        "Microsoft.Authorization/roleAssignments", "Microsoft.App/jobs",
    }
    assert {resource["type"] for resource in resources if resource.get("existing")} == {
        "Microsoft.App/managedEnvironments", "Microsoft.ContainerRegistry/registries",
    }
    job = next(resource for resource in resources if resource["type"] == "Microsoft.App/jobs")
    assert job["condition"] == "[parameters('deployJob')]"
    assert job["properties"]["configuration"]["triggerType"] == "Manual"
    assert job["properties"]["configuration"]["replicaRetryLimit"] == 0
    assert job["properties"]["configuration"]["manualTriggerConfig"] == {
        "parallelism": 1, "replicaCompletionCount": 1,
    }
    containers = job["properties"]["template"]["containers"]
    assert len(containers) == 1
    assert containers[0]["resources"] == {"cpu": "[json('0.25')]", "memory": "0.5Gi"}
    assert containers[0]["command"] == ["python3", "-I", "-B"]
    assert "secrets" not in job["properties"]["configuration"]
    assert "ingress" not in job["properties"]["configuration"]
    parameters = template["parameters"]
    assert parameters["mode"]["defaultValue"] == "preflight"
    assert parameters["mode"]["allowedValues"] == [
        "preflight", "apply", "recover", "reconcile", "reconcile-recovery",
    ]
    assert parameters["bundlePath"]["defaultValue"] == "/opt/state-sql-bootstrap/bundle.json"
    for name in ("recoveryPath", "recoverySha256", "approvedRecoverySha256", "artifactRoot"):
        assert parameters[name]["defaultValue"] == ""
    assert containers[0]["args"] == (
        "[concat(createArray('/opt/state-sql-bootstrap/scripts/bootstrap_azure_sql.py', "
        "'--bundle', parameters('bundlePath'), '--bundle-sha256', parameters('bundleSha256'), "
        "'--operation-id', parameters('operationId'), '--mode', parameters('mode'), "
        "'--approve-fingerprint', parameters('approvedFingerprint')), "
        "if(contains(createArray('recover', 'reconcile-recovery'), parameters('mode')), "
        "createArray('--recovery', parameters('recoveryPath'), '--recovery-sha256', "
        "parameters('recoverySha256')), createArray()), "
        "if(equals(parameters('mode'), 'recover'), createArray('--approve-recovery-sha256', "
        "parameters('approvedRecoverySha256')), createArray()), "
        "if(and(equals(parameters('mode'), 'reconcile-recovery'), "
        "not(empty(parameters('artifactRoot')))), "
        "createArray('--artifact-root', parameters('artifactRoot')), createArray()))]"
    )
