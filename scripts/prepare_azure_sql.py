"""Prepare a current-source Azure SQL bootstrap image context, without live I/O.

  python scripts\\prepare_azure_sql.py --request candidate.json --output .azure\\sql-candidate
  python scripts\\prepare_azure_sql.py --verify .azure\\sql-candidate

The strict request contains version=1, operation_id, target and identity (the
bootstrap runner's exact models), and base_image (an SDK/native-library image
pinned by sha256). No credentials, runtime users, memberships, administrator
changes, recovery approvals or environment-derived defaults are accepted.
The output directory must not exist. It contains context/ and manifest.json.
Review the target, managed identity, ordered SQL, metadata expectations and file
hashes before a separately authorized managed build and image-digest approval.

This creates a physical-schema/kernel candidate with the static REST budget
policy seeds for identity.tenant_id. Existing counters, windows and cooldowns
are never reset; a different stored policy is a conflict. It does not initialize
monitoring control, runtime access, reset registration or recovery approval.
kernel.statements already contains its component grants. Registration remains
a separate reset-only operator workflow; it is not a normal startup gate.

Recovery's approved empty_baseline_sha256 is not generated here. Use the current
trusted bootstrap.recovery_baseline_fingerprint(db, artifact.bundle) SELECT-only
reader on the independently reviewed empty target and original receipt catalogue.
It binds the exact target, fixed queries and complete bounded security metadata.
Retain that evidence for separate approval; the failed target must not approve
its own observed state. Fresh recovery requires the approved digest and quiescent
execution evidence expiring within 15 minutes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, ValidationError

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import bootstrap_azure_sql as bootstrap
from scripts import reset_monitoring_state as reset
from triage.monitoring import schema
from triage.monitoring.deployment_schema import (
    DEPLOYMENT_JOURNAL_NAMES,
    DEPLOYMENT_JOURNAL_STATEMENTS,
    kernel_contract_hash,
    module_definition,
    native_module_hash,
    unqualified,
)
from triage.monitoring.inventory import API_POLICIES, SERVICE_POLICIES
from triage.monitoring.provisioning import PROVISIONING_POLICIES
from triage.monitoring.rate_limit import schema_statements as rate_statements
from triage.monitoring.sql_permissions import budget_policy_statements, build_permission_kernel
from triage.store.azure_sql import DEFAULT_TABLES
from triage.store.azure_sql import schema_statements as application_statements

logger = logging.getLogger("triage.monitoring.prepare_sql")
ROOT = Path(__file__).resolve().parents[1]
IMAGE_ROOT = "/opt/state-sql-bootstrap"
DigestImage = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}$", max_length=512)]

# Native sys.columns metadata verified on Azure SQL for the released primitive
# types. ColumnSpec is a reset layout, not a native precision/length catalogue.
NATIVE_DATETIME2 = {3: (7, 23), 6: (8, 26), 7: (8, 27)}
NATIVE_PRECISION = {
    "bigint": 19, "int": 10, "bit": 1, "binary": 0,
    "char": 0, "varchar": 0, "nvarchar": 0, "uniqueidentifier": 0,
}


class PreparationError(RuntimeError):
    """A local preparation or exact-file verification failure."""


class PreparationRequest(bootstrap.StrictModel):
    version: Annotated[int, Field(ge=1, le=1)]
    operation_id: UUID
    target: bootstrap.Target
    identity: bootstrap.Identity
    base_image: DigestImage


class PayloadFile(bootstrap.StrictModel):
    size: Annotated[int, Field(ge=0)]
    sha256: bootstrap.SHA256


class PayloadManifest(bootstrap.StrictModel):
    version: Annotated[int, Field(ge=1, le=1)]
    status: Literal["candidate_not_authorized"]
    request: PreparationRequest
    bundle_sha256: bootstrap.SHA256
    source_sha256: bootstrap.SHA256
    kernel_contract_hash: bootstrap.SHA256
    preparation_sources: dict[str, bootstrap.SHA256]
    files: Annotated[dict[str, PayloadFile], Field(min_length=4, max_length=2048)]
    context_sha256: bootstrap.SHA256
    batch_count: Annotated[int, Field(ge=1)]
    metadata_check_count: Annotated[int, Field(ge=2)]


def _encoded(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_json(path: Path) -> bytes:
    if path.stat().st_size > 2 * 1024 * 1024:
        raise PreparationError("Preparation metadata exceeds its size limit")
    raw = path.read_bytes()
    json.loads(raw, object_pairs_hook=bootstrap._unique_keys)
    return raw


def _sources() -> dict[str, bytes]:
    paths = [*(ROOT / "src").rglob("*.py"), ROOT / "scripts" / "bootstrap_azure_sql.py"]
    result = {}
    for path in sorted(paths):
        if not path.resolve().is_relative_to(ROOT.resolve()):
            raise PreparationError("A source file resolves outside the current repository")
        result[path.relative_to(ROOT).as_posix()] = path.read_bytes()
    return result


def _preparation_sources() -> dict[str, str]:
    return {
        f"scripts/{name}": _sha((ROOT / "scripts" / name).read_bytes())
        for name in ("prepare_azure_sql.py", "reset_monitoring_state.py")
    }


def native_columns(table: reset.TableSpec) -> list[list[str | int | None]]:
    result = []
    for column in table.columns:
        if column.data_type == "datetime2":
            if column.scale not in NATIVE_DATETIME2:
                raise PreparationError("New datetime2 scale requires reviewed native metadata")
            length, precision = NATIVE_DATETIME2[column.scale]
        else:
            if column.data_type not in NATIVE_PRECISION or column.scale != 0:
                raise PreparationError("New SQL primitive requires reviewed native metadata")
            length, precision = column.max_length, NATIVE_PRECISION[column.data_type]
        result.append([
            column.name, column.data_type, length, precision, column.scale, int(column.nullable),
            0, int(column.computed_definition is not None),
        ])
    return result


def _view_columns(ddl: str, layouts: dict[str, list[list]]) -> list[list]:
    definition = module_definition(ddl)
    body = re.split(r"\bAS\s+(?=SELECT\b)", definition, maxsplit=1, flags=re.I)
    if len(body) != 2 or re.search(r"\b(?:LEFT|RIGHT|FULL)\s+(?:OUTER\s+)?JOIN\b", body[-1], re.I):
        raise PreparationError("New view shape requires explicit native metadata support")
    arms = []
    for arm in re.split(r"\bUNION\s+ALL\b", body[1], flags=re.I):
        match = re.match(r"\s*SELECT\s+(.+?)\s+FROM\s+(\[dbo\]\.\[\w+\])", arm, re.I | re.S)
        if match is None or unqualified(match[2]) not in layouts:
            raise PreparationError("View projection source is not a declared table")
        columns = {row[0]: row for row in layouts[unqualified(match[2])]}
        selected = []
        for expression in reset._parts(match[1]):
            identifier = re.fullmatch(r"(?:[a-z]\.)?(?:\[(\w+)\]|(\w+))", expression.strip(), re.I)
            if identifier is None or (identifier[1] or identifier[2]) not in columns:
                raise PreparationError("Computed or renamed view columns require reviewed native metadata")
            selected.append(list(columns[identifier[1] or identifier[2]]))
        arms.append(selected)
    if not arms or any(arm != arms[0] for arm in arms):
        raise PreparationError("UNION view column metadata differs between arms")
    return arms[0]


def _grant_rows(statements: tuple[str, ...], role: str, layouts: dict[str, list[list]]) -> list[list]:
    rows = set()
    for ddl in statements:
        grant = re.fullmatch(
            r"GRANT\s+(.+?)\s+ON\s+OBJECT::(\[dbo\]\.\[\w+\])\s+TO\s+\[(\w+)\];", ddl, re.I | re.S,
        )
        if grant is None or grant[3] != role:
            raise PreparationError("Kernel grant shape requires explicit metadata support")
        name = unqualified(grant[2])
        for part in reset._parts(grant[1]):
            permission = re.fullmatch(r"(SELECT|INSERT|UPDATE|DELETE|EXECUTE)(?:\s*\((.+)\))?", part, re.I | re.S)
            if permission is None:
                raise PreparationError("Unsupported kernel permission")
            minors = [0]
            if permission[2] is not None:
                columns = {row[0]: index for index, row in enumerate(layouts.get(name, []), 1)}
                names = [value.strip().strip("[]") for value in reset._parts(permission[2])]
                if any(column not in columns for column in names):
                    raise PreparationError("Kernel column grant has no declared native column")
                minors = [columns[column] for column in names]
            rows.update(
                ("OBJECT_OR_COLUMN", f"dbo.{name}", minor, permission[1].upper(), "GRANT")
                for minor in minors
            )
    return [list(row) for row in sorted(rows)]


def static_budget_policies() -> dict[str, tuple[int, int]]:
    """Use the same service/API buckets as the REST adapters, without overrides."""
    policies = {}
    for prefix, source in (
        ("service", SERVICE_POLICIES), ("api", API_POLICIES), ("api", PROVISIONING_POLICIES),
    ):
        for name, policy in source.items():
            bucket = f"{prefix}:{name}"
            if bucket in policies:
                raise PreparationError("Static REST budget buckets overlap; review the source policy catalogue")
            policies[bucket] = (policy.requests, policy.window_seconds)
    return dict(sorted(policies.items()))


def schema_payload(tenant_id: str) -> tuple[tuple[str, ...], list[bootstrap.Check], str]:
    """Generate the fresh schema, fixed readbacks and ABI from current helpers."""
    kernel = build_permission_kernel(schema.resolve_kernel_tables())
    policies = static_budget_policies()
    statements = (
        *application_statements(dict(DEFAULT_TABLES)), *schema.schema_statements(),
        *rate_statements(), *kernel.statements,
        *budget_policy_statements(tenant_id, policies, schema.resolve_kernel_tables()),
    )
    catalogue = reset.build_catalogue()
    tables = {table.name: table for table in catalogue.tables}
    objects, layouts, views = {}, {}, {}
    for ddl in statements:
        parsed = reset._table_body(ddl)
        if parsed is not None:
            name = parsed[0]
            layouts[name] = native_columns(tables[name])
            objects[name] = ["U", "dbo", None]
        match = re.search(
            r"\bCREATE(?:\s+OR\s+ALTER)?\s+(PROCEDURE|VIEW|FUNCTION)\s+(\[dbo\]\.\[\w+\]|dbo\.\w+)", ddl, re.I,
        )
        if match:
            kind, name = match[1].upper(), unqualified(match[2])
            if kind == "FUNCTION" and not re.search(r"\bRETURNS\s+(nvarchar|bit)\b", ddl, re.I):
                raise PreparationError("New function return type requires reviewed native metadata")
            objects[name] = [{"PROCEDURE": "P", "VIEW": "V", "FUNCTION": "FN"}[kind], "dbo", native_module_hash(ddl)]
            if kind == "VIEW":
                views[name] = ddl
    for logical, ddl in zip(
        DEPLOYMENT_JOURNAL_STATEMENTS, (bootstrap.CREATE_RECEIPTS, bootstrap.CREATE_RECOVERIES), strict=True,
    ):
        name = DEPLOYMENT_JOURNAL_NAMES[logical]
        normalized = ddl.replace(f"CREATE TABLE dbo.{name}", f"CREATE TABLE [dbo].[{name}]")
        actual = reset._table_body(normalized)
        expected = reset._table_body(DEPLOYMENT_JOURNAL_STATEMENTS[logical])
        if actual is None or expected is None or re.sub(r"\s+", "", actual[1]) != re.sub(r"\s+", "", expected[1]):
            raise PreparationError("Bootstrap journal DDL differs from its preserved operator contract")
    receipt_name = DEPLOYMENT_JOURNAL_NAMES["sql_bootstrap_receipts"]
    objects[receipt_name] = ["U", "dbo", None]
    layouts[receipt_name] = native_columns(tables[receipt_name])
    for name, ddl in views.items():
        layouts[name] = _view_columns(ddl, layouts)
    checks = [
        bootstrap.Check(kind="object", name=f"object-{name}", argument=f"dbo.{name}", expected=[value])
        for name, value in sorted(objects.items())
    ] + [
        bootstrap.Check(kind="columns", name=f"columns-{name}", argument=f"dbo.{name}", expected=value)
        for name, value in sorted(layouts.items())
    ]
    checks.extend(
        bootstrap.Check(
            kind="computed_columns", name=f"computed-columns-{table.name}", argument=f"dbo.{table.name}",
            expected=[
                [column.name, column.computed_definition, 1]
                for column in table.columns if column.computed_definition is not None
            ],
        )
        for table in catalogue.tables
        if any(column.computed_definition is not None for column in table.columns)
    )
    for component, grants in kernel.grants.items():
        role = kernel.names.role(component)
        checks.extend((
            bootstrap.Check(kind="principal", name=f"principal-{role}", argument=role, expected=[["R", "NONE", None, None]]),
            bootstrap.Check(kind="permissions", name=f"grants-{role}", argument=role, expected=_grant_rows(grants, role, layouts)),
            bootstrap.Check(kind="members", name=f"members-{role}", argument=role, expected=[]),
        ))
    checks.append(bootstrap.Check(
        kind="budget_policies", name="static-rest-budget-policies", argument=str(UUID(tenant_id)),
        expected=sorted(
            [hashlib.sha256(bucket.encode("utf-8")).hexdigest(), limit, seconds]
            for bucket, (limit, seconds) in policies.items()
        ),
    ))
    return statements, checks, kernel_contract_hash(schema.resolve_kernel_tables())


def _context_files(context: Path) -> dict[str, PayloadFile]:
    result = {}
    for path in sorted(context.rglob("*")):
        if path.is_symlink() or not path.resolve().is_relative_to(context.resolve()):
            raise PreparationError("Payload links are not permitted")
        if path.is_file():
            raw = path.read_bytes()
            result[path.relative_to(context).as_posix()] = PayloadFile(size=len(raw), sha256=_sha(raw))
    return result


def _context_hash(files: dict[str, PayloadFile]) -> str:
    return _sha(_encoded({name: value.model_dump() for name, value in files.items()}))


def _dockerfile(base_image: str) -> bytes:
    return f"""FROM {base_image}
USER 0:0
WORKDIR {IMAGE_ROOT}
COPY --chown=0:0 src/ src/
COPY --chown=0:0 scripts/bootstrap_azure_sql.py scripts/bootstrap_azure_sql.py
COPY --chown=0:0 sql/ sql/
COPY --chown=0:0 bundle.json bundle.json
RUN chmod -R a-w {IMAGE_ROOT}
USER 65532:65532
ENTRYPOINT ["python3", "-I", "-B", "{IMAGE_ROOT}/scripts/bootstrap_azure_sql.py"]
CMD []
""".encode()


def verify_context(output: Path) -> PayloadManifest:
    """Verify a current-source candidate; payload Python remains inert data."""
    manifest = PayloadManifest.model_validate_json(_read_json(output / "manifest.json"))
    preparation_sources = _preparation_sources()
    if manifest.preparation_sources != preparation_sources:
        raise PreparationError("Prepared provenance does not match current trusted preparation sources")
    context = output / "context"
    for name in manifest.files:
        path = PurePosixPath(name)
        if path.is_absolute() or path.as_posix() != name or "\\" in name or ":" in name or ".." in path.parts:
            raise PreparationError("Payload manifest path is not canonical")
    actual = _context_files(context)
    if actual != manifest.files or _context_hash(actual) != manifest.context_sha256:
        raise PreparationError("Prepared payload files changed")
    if (context / "Dockerfile").read_bytes() != _dockerfile(manifest.request.base_image):
        raise PreparationError("Prepared image recipe and manifest disagree")
    artifact = bootstrap.load_artifact(context, context / "bundle.json", manifest.bundle_sha256, manifest.request.operation_id)
    if (
        artifact.bundle.target != manifest.request.target or artifact.bundle.identity != manifest.request.identity
        or artifact.source_sha256 != manifest.source_sha256 or len(artifact.sql) != manifest.batch_count
        or len(artifact.bundle.checks) != manifest.metadata_check_count
    ):
        raise PreparationError("Prepared bundle and payload manifest disagree")
    current_sources = _sources()
    statements, checks, kernel_hash = schema_payload(str(artifact.bundle.identity.tenant_id))
    expected_files = set(current_sources) | {
        f"sql/{index:03d}.sql" for index in range(1, len(statements) + 1)
    } | {"bundle.json", "Dockerfile"}
    if set(actual) != expected_files:
        raise PreparationError("Prepared payload file set differs from current trusted generated files")
    if (
        manifest.kernel_contract_hash != kernel_hash
        or artifact.sql != statements or artifact.bundle.checks != checks
        or artifact.bundle.source_files != {name: _sha(raw) for name, raw in current_sources.items()}
    ):
        raise PreparationError("Prepared payload does not match current trusted generator and sources")
    if current_sources != _sources() or preparation_sources != _preparation_sources():
        raise PreparationError("Current trusted source changed during verification; prepare a new candidate")
    return manifest


def prepare(request: PreparationRequest, output: Path) -> PayloadManifest:
    if output.exists() or output.resolve().is_relative_to(ROOT / "src"):
        raise PreparationError("Use a new output directory outside the source tree; existing evidence is never overwritten")
    source_files = _sources()
    preparation_sources = _preparation_sources()
    statements, checks, kernel_hash = schema_payload(str(request.identity.tenant_id))
    payload = dict(source_files)
    batches = []
    for index, ddl in enumerate(statements, 1):
        path, raw = f"sql/{index:03d}.sql", ddl.encode("utf-8")
        payload[path] = raw
        batches.append(bootstrap.Batch(path=path, sha256=_sha(raw)))
    bundle = bootstrap.Bundle(
        version=1, operation_id=request.operation_id, ddl_owner="dbo",
        target=request.target, identity=request.identity,
        source_files={name: _sha(raw) for name, raw in source_files.items()},
        batches=batches, checks=checks,
    )
    payload["bundle.json"] = _encoded(bundle.model_dump(mode="json"))
    payload["Dockerfile"] = _dockerfile(request.base_image)
    if source_files != _sources() or preparation_sources != _preparation_sources():
        raise PreparationError("Source changed during preparation; review a new candidate")
    output.mkdir(parents=True, exist_ok=False)
    context = output / "context"
    for name, raw in payload.items():
        path = context.joinpath(*name.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(raw)
    artifact = bootstrap.load_artifact(context, context / "bundle.json", _sha(payload["bundle.json"]), request.operation_id)
    files = _context_files(context)
    manifest = PayloadManifest(
        version=1, status="candidate_not_authorized", request=request,
        bundle_sha256=artifact.fingerprint, source_sha256=artifact.source_sha256,
        kernel_contract_hash=kernel_hash, preparation_sources=preparation_sources,
        files=files, context_sha256=_context_hash(files),
        batch_count=len(artifact.sql), metadata_check_count=len(checks),
    )
    with (output / "manifest.json").open("xb") as stream:
        stream.write(_encoded(manifest.model_dump(mode="json")))
    return verify_context(output)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--request", type=Path)
    mode.add_argument("--verify", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if (args.request is not None) != (args.output is not None):
        parser.error("--output is required only with --request")
    try:
        manifest = (
            prepare(PreparationRequest.model_validate_json(_read_json(args.request)), args.output)
            if args.request else verify_context(args.verify)
        )
        print(json.dumps({
            "status": manifest.status, "bundle_sha256": manifest.bundle_sha256,
            "context_sha256": manifest.context_sha256,
            "kernel_contract_hash": manifest.kernel_contract_hash,
            "batch_count": manifest.batch_count, "metadata_check_count": manifest.metadata_check_count,
            "native_sql_proven": False, "image_built": False,
        }, sort_keys=True))
        return 0
    except (OSError, ValueError, ValidationError, PreparationError, bootstrap.BootstrapError, reset.ResetError) as exc:
        reason = str(exc) if isinstance(exc, (PreparationError, bootstrap.BootstrapError, reset.ResetError)) else (
            "Invalid or unreadable request/artifact; use --help for the local preparation contract"
        )
        logger.error("SQL payload preparation failed (%s): %s", type(exc).__name__, reason)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
