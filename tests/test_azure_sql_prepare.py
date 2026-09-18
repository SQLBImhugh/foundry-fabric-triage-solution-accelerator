from __future__ import annotations

import copy
import hashlib
import json
import re
import socket
import subprocess
from datetime import timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError
from test_monitoring_reset import NOW, TARGET, TENANT, TransactionalSqlFake, initialize

from scripts import bootstrap_azure_sql as bootstrap
from scripts import prepare_azure_sql as prepare
from scripts import reset_monitoring_state as reset
from triage.monitoring.deployment_authority import read_authority
from triage.monitoring.deployment_schema import (
    DEPLOYMENT_JOURNAL_NAMES,
    DEPLOYMENT_JOURNAL_STATEMENTS,
    kernel_contract_hash,
    native_module_hash,
    unqualified,
)
from triage.monitoring.inventory import API_POLICIES, SERVICE_POLICIES
from triage.monitoring.provisioning import PROVISIONING_POLICIES
from triage.monitoring.sql_permissions import budget_policy_statements, build_permission_kernel
from triage.store.azure_sql import AzureSqlDatabase, SqlUnavailable

OPERATION = UUID(int=710)


@pytest.fixture(autouse=True)
def no_live_io(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Preparing a SQL payload must not use network, credentials, native SQL or cloud tooling")

    for name in ("getaddrinfo", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    for name in ("_connect", "query", "execute", "transaction"):
        monkeypatch.setattr(AzureSqlDatabase, name, forbidden)


def request_payload():
    return {
        "version": 1, "operation_id": str(OPERATION),
        "target": {
            "server": TARGET.server, "application_database": TARGET.database,
            "database": TARGET.database, "kind": "application",
        },
        "identity": {
            "tenant_id": TENANT, "client_id": str(UUID(int=711)), "object_id": str(UUID(int=712)),
        },
        "base_image": "example.invalid/native-sdk@sha256:" + "a" * 64,
    }


def request():
    return prepare.PreparationRequest.model_validate_json(json.dumps(request_payload()))


class JournalSqlFake(TransactionalSqlFake):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.journal_keys = {}
        self.journal_constraints = {}
        self.owners = {}

    def install_journal(self, logical):
        name = DEPLOYMENT_JOURNAL_NAMES[logical]
        self.tables[name] = []
        self.object_ids[name] = max(self.object_ids.values()) + 1
        self.safety[name] = (0, False, False, 0)
        recovery = logical == "sql_bootstrap_recoveries"
        self.journal_keys[name] = [
            (1, True, False, 1, True, False, False, "original_operation_id" if recovery else "operation_id", 1, False, False),
        ]
        self.journal_constraints[name] = [
            ("D", "resolved_at" if recovery else "started_at", "(sysutcdatetime())", False, False),
        ]
        if recovery:
            self.journal_keys[name].append((2, False, True, 2, True, False, False, "replacement_operation_id", 1, False, False))
        else:
            self.journal_constraints[name].append(("C", "status", "([status]='started' OR [status]='committed')", False, False))

    def query(self, sql, *params):
        if "deployment-journal:" in sql:
            name = params[0].removeprefix("dbo.")
            if "deployment-journal:keys" in sql:
                return self.journal_keys[name]
            if "deployment-journal:constraints" in sql:
                return self.journal_constraints[name]
            if "deployment-journal:safety" in sql:
                return [self.safety[name]]
        rows = super().query(sql, *params)
        if "monitoring-reset:objects" in sql:
            return [(*row[:4], self.owners.get(row[1], row[4]), *row[5:]) for row in rows]
        if "deployment-authority:objects" in sql:
            return [(*row[:4], self.owners.get(row[2], row[4]), *row[5:]) for row in rows]
        return rows


class BootstrapHandoffSqlFake(JournalSqlFake):
    """One offline database used by the actual bootstrap and initialization paths."""

    def __init__(self, artifact):
        super().__init__(initialized=False)
        self.artifact = artifact
        self.tables.clear()
        self.module_ids.clear()
        self.role_ids.clear()
        self.sql_writer_principals.clear()
        self.fks.clear()
        self.native_columns = {}
        self.native_grants = {}
        self.applied = []
        self.server_identity = TARGET.server.removesuffix(".database.windows.net")

    def query(self, sql, *params):
        if sql == bootstrap.TARGET_SQL:
            return [(self.server_identity, self.actual_database, 5, "dbo", 1, "dbo", 20)]
        if sql == bootstrap.LOCK_SQL:
            return [(0,)]
        if sql == "SELECT @@TRANCOUNT, XACT_STATE()":
            return [(1, 1)]
        if sql == bootstrap.RECOVERY_OBJECT_SQL:
            name = params[0].removeprefix("dbo.")
            value = self.object_ids[name] if name in self.tables else None
            return [(value, value)]
        if sql == "SELECT OBJECT_ID(?,N'U')":
            name = params[0].removeprefix("dbo.")
            return [(self.object_ids[name] if name in self.tables else None,)]
        if sql.startswith("SELECT fingerprint,source_sha256,status"):
            return [
                (row["fingerprint"], row["source_sha256"], row["status"])
                for row in self.tables[DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]]
                if row["operation_id"] == params[0]
            ]
        if sql == f"SELECT COUNT_BIG(*) FROM {bootstrap.RECEIPT_TABLE} WHERE status='started'":
            return [(sum(row["status"] == "started" for row in self.tables[DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]]),)]
        if sql == bootstrap.CHECK_SQL["object"]:
            name = unqualified(params[0])
            if name in self.tables:
                return [("U", "dbo", None)]
            if name in self.module_ids:
                item = next(value for value in self.catalogue.modules if value.name == name)
                return [({"procedure": "P", "view": "V", "function": "FN"}[item.kind], "dbo", self.native_module_hashes[name])]
            return []
        if sql == bootstrap.CHECK_SQL["columns"]:
            return [tuple(row) for row in self.native_columns[unqualified(params[0])]]
        if sql == bootstrap.CHECK_SQL["principal"]:
            return [("R", "NONE", None, None)] if params[0] in self.role_ids else []
        if sql == bootstrap.CHECK_SQL["members"]:
            return []
        if sql == bootstrap.CHECK_SQL["permissions"]:
            return sorted(set(self.native_grants[params[0]]))
        if sql == bootstrap.CHECK_SQL.get("budget_policies"):
            return sorted(
                (row["bucket_hash"], row["request_limit"], row["window_seconds"])
                for row in self.tables[self.table_name("rate_budget")]
                if row["tenant_id"] == params[0]
            )[:2049]
        return super().query(sql, *params)

    def execute(self, sql, *params):
        if sql == bootstrap.CREATE_RECEIPTS:
            if DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"] not in self.tables:
                self.install_journal("sql_bootstrap_receipts")
            table = self.catalogue.table("sql_bootstrap_receipts")
            self.native_columns[table.name] = prepare.native_columns(table)
            self.statements.append((sql, params))
            return 0
        if sql.startswith(f"INSERT INTO {bootstrap.RECEIPT_TABLE}"):
            self.add(
                "sql_bootstrap_receipts", operation_id=params[0], fingerprint=params[1],
                source_sha256=params[2], status="started", started_at=NOW, committed_at=None,
            )
            self.statements.append((sql, params))
            return 1
        if sql.startswith(f"UPDATE {bootstrap.RECEIPT_TABLE}"):
            row = self.tables[DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]][0]
            assert (row["operation_id"], row["fingerprint"], row["source_sha256"]) == params
            row.update(status="committed", committed_at=NOW)
            self.statements.append((sql, params))
            return 1
        if sql == "SET XACT_ABORT ON; SET LOCK_TIMEOUT 10000;":
            self.statements.append((sql, params))
            return 0
        if sql in self.artifact.sql:
            self.applied.append(sql)
            self.statements.append((sql, params))
            table = reset._table_body(sql)
            if table:
                self.tables.setdefault(table[0], [])
                declaration = next(value for value in self.catalogue.tables if value.name == table[0])
                self.native_columns[table[0]] = prepare.native_columns(declaration)
                for edge in self.catalogue.foreign_keys:
                    reference = ("dbo", edge.child, edge.child_column, "dbo", edge.parent, edge.parent_column, 0, 0, False)
                    if edge.child == table[0] and reference not in self.fks:
                        self.fks.append(reference)
            policy = re.search(
                r"VALUES \('([0-9a-f-]{36})','([0-9a-f]{64})',(\d+),(\d+),DATEADD\(second,(\d+),SYSUTCDATETIME\(\)\),0\);",
                sql,
            )
            if policy:
                tenant, bucket_hash, limit, seconds, window = policy.groups()
                assert window == seconds
                existing = [
                    row for row in self.tables[self.table_name("rate_budget")]
                    if (row["tenant_id"], row["bucket_hash"]) == (tenant, bucket_hash)
                ]
                if any((row["request_limit"], row["window_seconds"]) != (int(limit), int(seconds)) for row in existing):
                    raise SqlUnavailable("Existing service budget policy differs; no reset performed")
                if not existing:
                    self.add(
                        "rate_budget", tenant_id=tenant, bucket_hash=bucket_hash,
                        request_limit=int(limit), window_seconds=int(seconds),
                        window_ends_at=self.clock.now + timedelta(seconds=int(seconds)), used=0,
                        blocked_until=None,
                    )
            module = re.search(r"CREATE(?: OR ALTER)? (PROCEDURE|VIEW|FUNCTION)\s+(\[dbo\]\.\[\w+\]|dbo\.\w+)", sql)
            if module:
                name = unqualified(module[2])
                self.module_ids.setdefault(name, 300 + len(self.module_ids))
                self.native_module_hashes[name] = native_module_hash(sql)
                if module[1] == "VIEW":
                    self.native_columns[name] = prepare._view_columns(sql, self.native_columns)
            role = re.search(r"CREATE ROLE \[(\w+)\]", sql)
            if role:
                self.role_ids[role[1]] = 600 + len(self.role_ids)
                self.native_grants[role[1]] = []
            grant = re.fullmatch(r"GRANT .+ TO \[(\w+)\];", sql)
            if grant:
                self.native_grants[grant[1]].extend(
                    tuple(row) for row in prepare._grant_rows((sql,), grant[1], self.native_columns)
                )
            return 0
        return super().execute(sql, *params)


def test_public_preparation_is_byte_exact_reproducible_and_roundtrips(tmp_path):
    output = tmp_path / "candidate"
    manifest = prepare.prepare(request(), output)
    repeated = prepare.prepare(request(), tmp_path / "same-input")
    assert manifest == repeated
    context = output / "context"
    artifact = bootstrap.load_artifact(context, context / "bundle.json", manifest.bundle_sha256, OPERATION)
    statements, checks, _ = prepare.schema_payload(TENANT)
    assert artifact.sql == statements
    assert artifact.bundle.checks == checks
    assert manifest.kernel_contract_hash == kernel_contract_hash()
    assert manifest.status == "candidate_not_authorized"
    for name in artifact.bundle.source_files:
        assert context.joinpath(*name.split("/")).read_bytes() == prepare.ROOT.joinpath(*name.split("/")).read_bytes()
    kernel = build_permission_kernel()
    for grant in (grant for grants in kernel.grants.values() for grant in grants):
        assert artifact.sql.count(grant) == 1
    assert not any("CREATE USER" in ddl or "ADD MEMBER" in ddl for ddl in artifact.sql)
    assert bootstrap.CREATE_RECOVERIES not in artifact.sql
    dockerfile = (context / "Dockerfile").read_text()
    assert request().base_image in dockerfile
    assert '["python3", "-I", "-B", "/opt/state-sql-bootstrap/scripts/bootstrap_azure_sql.py"]' in dockerfile
    assert "USER 65532:65532" in dockerfile and "chmod -R a-w" in dockerfile
    assert all("\\" not in path for path in manifest.files)


def test_current_native_column_expectations_are_not_reset_storage_guesses():
    catalogue = reset.build_catalogue()
    claims = catalogue.table("claims")
    guessed = claims.model_copy(update={"columns": tuple(
        column.model_copy(update={"max_length": 999}) if column.data_type == "datetime2" else column
        for column in claims.columns
    )})
    assert prepare.native_columns(claims) == prepare.native_columns(guessed)
    row = next(row for row in prepare.native_columns(claims) if row[0] == "claimed_at")
    assert row == ["claimed_at", "datetime2", 7, 23, 3, 0, 0, 0]
    control = prepare.native_columns(catalogue.table("monitoring_control"))
    assert next(row for row in control if row[0] == "maintenance") == ["maintenance", "bit", 1, 1, 0, 0, 0, 0]
    assert next(row for row in control if row[0] == "activation_cutoff")[1:5] == ["datetime2", 8, 26, 6]
    unknown = claims.model_copy(update={"columns": (
        claims.columns[2].model_copy(update={"scale": 4}),
    )})
    with pytest.raises(prepare.PreparationError, match="native metadata"):
        prepare.native_columns(unknown)


@pytest.mark.parametrize("change", [
    "file", "extra", "missing", "manifest_extra", "traversal", "base_image", "target", "source",
    "kernel", "preparation_empty", "preparation_missing", "preparation_extra", "preparation_hash",
    "identity", "operation", "batch_count", "check_count",
])
def test_payload_manifest_refuses_any_file_or_shape_change(tmp_path, change, capsys):
    output = tmp_path / "candidate"
    prepare.prepare(request(), output)
    if change == "file":
        (output / "context" / "sql" / "001.sql").write_bytes(b"not the approved SQL")
    elif change == "extra":
        (output / "context" / "unexpected.env").write_bytes(b"not admitted")
    elif change == "missing":
        (output / "context" / "Dockerfile").unlink()
    else:
        path = output / "manifest.json"
        value = json.loads(path.read_bytes())
        if change == "manifest_extra":
            value["approval"] = True
        elif change == "traversal":
            value["files"]["../escape"] = value["files"].pop("Dockerfile")
        elif change == "base_image":
            value["request"]["base_image"] = "example.invalid/other-sdk@sha256:" + "b" * 64
        elif change == "target":
            value["request"]["target"]["server"] = "different.database.windows.net"
        elif change == "source":
            value["source_sha256"] = "0" * 64
        elif change == "kernel":
            value["kernel_contract_hash"] = "0" * 64
        elif change == "preparation_empty":
            value["preparation_sources"] = {}
        elif change == "preparation_missing":
            value["preparation_sources"].pop("scripts/reset_monitoring_state.py")
        elif change == "preparation_extra":
            value["preparation_sources"]["scripts/unreviewed.py"] = "0" * 64
        elif change == "preparation_hash":
            value["preparation_sources"]["scripts/prepare_azure_sql.py"] = "0" * 64
        elif change == "identity":
            value["request"]["identity"]["client_id"] = str(UUID(int=795))
        elif change == "operation":
            value["request"]["operation_id"] = str(UUID(int=796))
        elif change == "batch_count":
            value["batch_count"] += 1
        else:
            value["metadata_check_count"] += 1
        path.write_bytes(json.dumps(value).encode())
    with pytest.raises((prepare.PreparationError, ValidationError, bootstrap.BootstrapError)):
        prepare.verify_context(output)
    assert prepare.main(["--verify", str(output)]) == 1
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("changed", ["source", "sql", "checks", "batch_path"])
def test_rehashed_payload_cannot_claim_current_generator_provenance(tmp_path, changed, capsys):
    output = tmp_path / "candidate"
    prepare.prepare(request(), output)
    context = output / "context"
    manifest_path, bundle_path = output / "manifest.json", context / "bundle.json"
    manifest, bundle = json.loads(manifest_path.read_bytes()), json.loads(bundle_path.read_bytes())
    if changed == "source":
        name = "src/triage/__init__.py"
        raw = b'raise AssertionError("Payload code must never be imported or evaluated")\n'
        context.joinpath(*name.split("/")).write_bytes(raw)
        bundle["source_files"][name] = hashlib.sha256(raw).hexdigest()
    elif changed == "sql":
        batch = bundle["batches"][0]
        raw = b"SELECT 1;"
        context.joinpath(*batch["path"].split("/")).write_bytes(raw)
        batch["sha256"] = hashlib.sha256(raw).hexdigest()
    elif changed == "batch_path":
        batch = bundle["batches"][0]
        context.joinpath(*batch["path"].split("/")).rename(context / "sql" / "renamed.sql")
        batch["path"] = "sql/renamed.sql"
    else:
        check = next(check for check in bundle["checks"] if check["kind"] == "object")
        check["expected"][0][2] = "0" * 64
    bundle_path.write_bytes(prepare._encoded(bundle))
    manifest["bundle_sha256"] = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    artifact = bootstrap.load_artifact(context, bundle_path, manifest["bundle_sha256"], OPERATION)
    manifest["source_sha256"] = artifact.source_sha256
    files = prepare._context_files(context)
    manifest["files"] = {name: record.model_dump() for name, record in files.items()}
    manifest["context_sha256"] = prepare._context_hash(files)
    manifest_path.write_bytes(prepare._encoded(manifest))
    with pytest.raises(prepare.PreparationError, match="current trusted"):
        prepare.verify_context(output)
    assert prepare.main(["--verify", str(output)]) == 1
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("relative", [
    "src/triage/native.pyd", "src/triage/native.so", "src/unreviewed.pth",
    "src/.env", "src/extra.json", "sql/extra.sql", "scripts/extra.ps1",
])
def test_rehashed_extra_payload_file_is_refused_for_every_suffix(tmp_path, relative, capsys):
    output = tmp_path / "candidate"
    prepare.prepare(request(), output)
    context = output / "context"
    extra = context.joinpath(*relative.split("/"))
    extra.parent.mkdir(parents=True, exist_ok=True)
    extra.write_bytes(b"Unreviewed payload bytes; never load or execute.\n")
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    files = prepare._context_files(context)
    manifest["files"] = {name: record.model_dump() for name, record in files.items()}
    manifest["context_sha256"] = prepare._context_hash(files)
    manifest_path.write_bytes(prepare._encoded(manifest))
    with pytest.raises(prepare.PreparationError, match="file set"):
        prepare.verify_context(output)
    assert prepare.main(["--verify", str(output)]) == 1
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("changed", ["kernel", "preparation", "source"])
def test_old_candidate_fails_when_the_current_trusted_generator_changes(tmp_path, monkeypatch, changed):
    output = tmp_path / "candidate"
    prepare.prepare(request(), output)
    before = (output / "manifest.json").read_bytes()
    if changed == "kernel":
        original = prepare.schema_payload

        def changed_kernel(tenant_id):
            statements, checks, _ = original(tenant_id)
            return statements, checks, "0" * 64

        monkeypatch.setattr(prepare, "schema_payload", changed_kernel)
    elif changed == "preparation":
        original = prepare._preparation_sources()
        original["scripts/reset_monitoring_state.py"] = "0" * 64
        monkeypatch.setattr(prepare, "_preparation_sources", lambda: original)
    else:
        original = prepare._sources()
        original["src/triage/__init__.py"] += b"\n"
        monkeypatch.setattr(prepare, "_sources", lambda: original)
    with pytest.raises(prepare.PreparationError, match="current trusted"):
        prepare.verify_context(output)
    assert (output / "manifest.json").read_bytes() == before


def test_verification_refuses_source_changes_during_current_generation(tmp_path, monkeypatch):
    output = tmp_path / "candidate"
    prepare.prepare(request(), output)
    original = prepare._sources
    reads = []

    def changed():
        result = original()
        reads.append(True)
        if len(reads) > 1:
            result["src/triage/__init__.py"] += b"\n"
        return result

    monkeypatch.setattr(prepare, "_sources", changed)
    with pytest.raises(prepare.PreparationError, match="changed during verification"):
        prepare.verify_context(output)


def test_prepare_cli_strict_request_and_existing_evidence(tmp_path, capsys, monkeypatch):
    inputs = tmp_path / "request.json"
    inputs.write_text(json.dumps(request_payload()))
    output = tmp_path / "candidate"
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "ignored-fixture-not-an-input")
    monkeypatch.setenv("AZURE_SQL_DATABASE", "wrong-environment")
    assert prepare.main(["--request", str(inputs), "--output", str(output)]) == 0
    assert json.loads(capsys.readouterr().out)["native_sql_proven"] is False
    original = (output / "manifest.json").read_bytes()
    assert prepare.main(["--verify", str(output)]) == 0
    assert prepare.main(["--request", str(inputs), "--output", str(output)]) == 1
    assert (output / "manifest.json").read_bytes() == original
    for change in (
        {"credentials": "not accepted"}, {"base_image": "example.invalid/sdk:latest"},
        {"operation_id": str(UUID(int=0))},
    ):
        inputs.write_text(json.dumps(request_payload() | change))
        assert prepare.main(["--request", str(inputs), "--output", str(tmp_path / "refused")]) == 1
        assert not (tmp_path / "refused").exists()


def test_concurrent_source_change_refuses_before_creating_payload(tmp_path, monkeypatch):
    real = prepare._sources
    reads = []

    def changed():
        result = real()
        reads.append(True)
        if len(reads) == 2:
            result["src/triage/__init__.py"] += b"\n"
        return result

    monkeypatch.setattr(prepare, "_sources", changed)
    with pytest.raises(prepare.PreparationError, match="Source changed"):
        prepare.prepare(request(), tmp_path / "candidate")
    assert not (tmp_path / "candidate").exists()


def test_bootstrap_apply_to_initialization_preserves_actual_committed_receipt(tmp_path):
    output = tmp_path / "candidate"
    manifest = prepare.prepare(request(), output)
    artifact = bootstrap.load_artifact(output / "context", output / "context" / "bundle.json", manifest.bundle_sha256, OPERATION)
    db = BootstrapHandoffSqlFake(artifact)
    result = bootstrap.run(db, artifact, "apply", artifact.fingerprint)
    assert result["status"] == "committed_and_read_back" and tuple(db.applied) == artifact.sql
    receipt_table = DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]
    original = copy.deepcopy(db.tables[receipt_table])
    budgets = copy.deepcopy(db.tables[db.table_name("rate_budget")])
    assert len(budgets) == 19
    assert original[0]["status"] == "committed" and original[0]["operation_id"] == str(OPERATION)
    assert db.tables[db.table_name("monitoring_control")] == []
    operator = reset.SqlResetOperator(db, TARGET)
    plan = operator.plan_initialization()
    assert not any("unexpected_accelerator_object" in blocker for blocker in plan.snapshot.blockers)
    initialized = initialize(operator, plan)
    assert initialized.receipt.control.maintenance
    assert initialized.receipt.control.epoch == plan.new_epoch
    assert db.tables[receipt_table] == original and db.delete_calls == 0
    assert db.tables[db.table_name("rate_budget")] == budgets
    with db.transaction():
        authority = read_authority(db, TARGET, db.catalogue)
    assert "undeclared_accelerator_object" not in authority.gaps
    assert initialize(operator, plan).replayed
    assert db.tables[receipt_table] == original


def test_fresh_payload_seeds_all_static_adapter_budgets_once():
    expected = {
        **{f"service:{name}": (policy.requests, policy.window_seconds) for name, policy in SERVICE_POLICIES.items()},
        **{f"api:{name}": (policy.requests, policy.window_seconds) for name, policy in API_POLICIES.items()},
        **{f"api:{name}": (policy.requests, policy.window_seconds) for name, policy in PROVISIONING_POLICIES.items()},
    }
    assert len(expected) == 19 and prepare.static_budget_policies() == expected
    statements, _, _ = prepare.schema_payload(TENANT)
    seeds = budget_policy_statements(TENANT, expected)
    assert statements[-19:] == seeds
    for bucket, (limit, seconds) in expected.items():
        digest = hashlib.sha256(bucket.encode("utf-8")).hexdigest()
        selected = [sql for sql in seeds if f"bucket_hash='{digest}'" in sql]
        assert len(selected) == 1
        assert f"tenant_id='{TENANT}'" in selected[0]
        assert f"request_limit<>{limit} OR window_seconds<>{seconds}" in selected[0]
        assert "THROW 51072" in selected[0]
    assert all(not re.search(r"\b(?:UPDATE|DELETE|MERGE|TRUNCATE)\b", sql) for sql in seeds)


def test_overlapping_static_policy_sources_fail_before_export(monkeypatch):
    monkeypatch.setattr(prepare, "PROVISIONING_POLICIES", {"fabric.jobs": API_POLICIES["fabric.jobs"]})
    with pytest.raises(prepare.PreparationError, match="overlap"):
        prepare.static_budget_policies()


@pytest.fixture
def budget_artifact(tmp_path):
    output = tmp_path / "budget-candidate"
    manifest = prepare.prepare(request(), output)
    return bootstrap.load_artifact(output / "context", output / "context" / "bundle.json", manifest.bundle_sha256, OPERATION)


def test_budget_seed_and_reconcile_preserve_existing_counters_windows_and_backoff(budget_artifact):
    db = BootstrapHandoffSqlFake(budget_artifact)
    table = db.table_name("rate_budget")
    db.tables[table] = []
    bucket, (limit, seconds) = next(iter(prepare.static_budget_policies().items()))
    digest = hashlib.sha256(bucket.encode("utf-8")).hexdigest()
    retained = db.add(
        "rate_budget", tenant_id=TENANT, bucket_hash=digest, request_limit=limit,
        window_seconds=seconds, used=7, window_ends_at=NOW + timedelta(minutes=20),
        blocked_until=NOW + timedelta(minutes=10),
    )
    other_tenant = db.add(
        "rate_budget", tenant_id=str(UUID(int=890)), bucket_hash=digest,
        request_limit=1, window_seconds=1, used=1, window_ends_at=NOW,
        blocked_until=NOW + timedelta(days=1),
    )
    before = copy.deepcopy([retained, other_tenant])
    bootstrap.run(db, budget_artifact, "apply", budget_artifact.fingerprint)
    assert db.tables[table][:2] == before and len(db.tables[table]) == 20
    retained.update(used=9, window_ends_at=NOW + timedelta(hours=1), blocked_until=NOW + timedelta(hours=2))
    expected = copy.deepcopy(db.tables[table])
    writes = len(db.statements)
    assert bootstrap.reconcile(db, budget_artifact)["status"] == "committed_and_read_back"
    assert db.tables[table] == expected and len(db.statements) == writes
    with db.transaction():
        for sql in budget_policy_statements(TENANT, prepare.static_budget_policies()):
            db.execute(sql)
    assert db.tables[table] == expected


@pytest.mark.parametrize("changed", ["request_limit", "window_seconds"])
def test_mismatched_budget_policy_rolls_back_seed_without_resetting_any_budget(budget_artifact, changed):
    db = BootstrapHandoffSqlFake(budget_artifact)
    table = db.table_name("rate_budget")
    db.tables[table] = []
    bucket, (limit, seconds) = list(prepare.static_budget_policies().items())[9]
    original = db.add(
        "rate_budget", tenant_id=TENANT, bucket_hash=hashlib.sha256(bucket.encode("utf-8")).hexdigest(),
        request_limit=limit, window_seconds=seconds, used=5,
        window_ends_at=NOW + timedelta(hours=1), blocked_until=NOW + timedelta(hours=2),
    )
    original[changed] += 1
    before = copy.deepcopy(db.tables[table])
    with pytest.raises(SqlUnavailable, match="policy differs"):
        bootstrap.run(db, budget_artifact, "apply", budget_artifact.fingerprint)
    assert db.tables[table] == before
    assert set(db.tables) == {table, DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]}
    assert db.tables[DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]][0]["status"] == "started"
    with pytest.raises(bootstrap.BootstrapError, match="pending_operation"):
        bootstrap.run(db, budget_artifact, "apply", budget_artifact.fingerprint)
    assert db.tables[table] == before


@pytest.mark.parametrize("changed", ["missing", "limit", "window", "extra", "duplicate", "overflow"])
def test_fixed_budget_readback_detects_missing_or_changed_policy_without_repair(budget_artifact, changed):
    check = next(check for check in budget_artifact.bundle.checks if check.kind == "budget_policies")
    assert check.argument == TENANT
    assert check.expected == sorted(
        [hashlib.sha256(bucket.encode("utf-8")).hexdigest(), limit, seconds]
        for bucket, (limit, seconds) in prepare.static_budget_policies().items()
    )
    db = BootstrapHandoffSqlFake(budget_artifact)
    bootstrap.run(db, budget_artifact, "apply", budget_artifact.fingerprint)
    rows = db.tables[db.table_name("rate_budget")]
    if changed == "missing":
        rows.pop()
    elif changed == "extra":
        row = copy.deepcopy(rows[0])
        row["bucket_hash"] = "f" * 64
        rows.append(row)
    elif changed == "duplicate":
        rows.append(copy.deepcopy(rows[0]))
    elif changed == "overflow":
        template = copy.deepcopy(rows[0])
        rows.extend(
            template | {"bucket_hash": hashlib.sha256(f"overflow:{index}".encode()).hexdigest()}
            for index in range(2049)
        )
        assert len(db.query(bootstrap.CHECK_SQL["budget_policies"], TENANT)) == 2049
    else:
        rows[0]["request_limit" if changed == "limit" else "window_seconds"] += 1
    before, writes = copy.deepcopy(rows), len(db.statements)
    with pytest.raises(bootstrap.BootstrapError, match="metadata_mismatch"):
        bootstrap.reconcile(db, budget_artifact)
    assert rows == before and len(db.statements) == writes


def test_budget_readback_expected_rows_stop_below_the_overflow_sentinel(budget_artifact):
    raw = budget_artifact.bundle.model_dump(mode="json")
    check = next(check for check in raw["checks"] if check["kind"] == "budget_policies")
    check["expected"] = [[f"{index:064x}", 1, 60] for index in range(2048)]
    accepted = bootstrap.Bundle.model_validate_json(json.dumps(raw))
    assert len(next(check for check in accepted.checks if check.kind == "budget_policies").expected) == 2048
    check["expected"].append([f"{2048:064x}", 1, 60])
    with pytest.raises(ValidationError, match="at most 2048"):
        bootstrap.Bundle.model_validate_json(json.dumps(raw))


def test_legacy_bundle_contract_does_not_require_the_new_optional_budget_check(budget_artifact):
    raw = budget_artifact.bundle.model_dump(mode="json")
    raw["checks"] = [check for check in raw["checks"] if check["kind"] != "budget_policies"]
    legacy = bootstrap.Bundle.model_validate_json(json.dumps(raw))
    assert all(check.kind != "budget_policies" for check in legacy.checks)
    assert len(legacy.checks) == len(budget_artifact.bundle.checks) - 1


@pytest.mark.parametrize("changed", [
    "tenant", "empty", "hash", "limit", "window", "duplicate", "order", "mutable_fields",
])
def test_budget_readback_contract_cannot_change_tenant_or_request_mutable_state(budget_artifact, changed):
    raw = budget_artifact.bundle.model_dump(mode="json")
    check = next(check for check in raw["checks"] if check["kind"] == "budget_policies")
    if changed == "tenant":
        check["argument"] = str(UUID(int=891))
    elif changed == "empty":
        check["expected"] = []
    elif changed == "hash":
        check["expected"][0][0] = "not-a-bucket-hash"
    elif changed == "limit":
        check["expected"][0][1] = 0
    elif changed == "window":
        check["expected"][0][2] = 86_401
    elif changed == "duplicate":
        check["expected"].append(check["expected"][0])
    elif changed == "order":
        check["expected"].reverse()
    else:
        check["expected"][0].append(0)
    with pytest.raises(ValidationError, match="Budget"):
        bootstrap.Bundle.model_validate_json(json.dumps(raw))


def test_budget_metadata_query_is_fixed_bounded_read_only_and_excludes_counters():
    query = bootstrap.CHECK_SQL["budget_policies"]
    assert "TOP (2049)" in query and query.count("?") == 1
    assert "dbo.triage_monitoring_rate_budget" in query
    assert "tenant_id=CONVERT(UNIQUEIDENTIFIER,?)" in query
    assert all(word not in query for word in ("used", "window_ends_at", "blocked_until"))
    assert not re.search(r"\b(?:INSERT|UPDATE|DELETE|EXEC|MERGE)\b", query)


def test_optional_journal_declarations_match_the_public_runner():
    for logical, actual in zip(
        DEPLOYMENT_JOURNAL_STATEMENTS, (bootstrap.CREATE_RECEIPTS, bootstrap.CREATE_RECOVERIES), strict=True,
    ):
        name = DEPLOYMENT_JOURNAL_NAMES[logical]
        normalized = actual.replace(f"CREATE TABLE dbo.{name}", f"CREATE TABLE [dbo].[{name}]")
        assert re.sub(r"\s+", "", reset._table_body(normalized)[1]) == re.sub(
            r"\s+", "", reset._table_body(DEPLOYMENT_JOURNAL_STATEMENTS[logical])[1],
        )
        table = reset.build_catalogue().table(logical)
        assert table.optional and table.operation == "preserve_deployment_journal"
        assert table not in reset.build_catalogue().deletion_order()
