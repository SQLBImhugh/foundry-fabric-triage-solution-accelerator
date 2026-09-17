"""Read the actual SQL authority graph on the caller's synchronous transaction.

This deliberately over-approximates permission reachability. A DENY does not
repair ownership, DDL, a column grant, or an unmodelled module. Unsupported paths
are refusals, not evidence that a deployment has no writers. Schema grants and
DML triggers/cascades are followed outside the accelerator's owned catalogue.
SELECT reaches views, functions and synonyms, not stored procedures.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from triage.monitoring.deployment_contracts import DeploymentError, ResetTarget, fingerprint
from triage.monitoring.deployment_schema import (
    DEFAULT_REGISTRATION_NAMES,
    READ_ONLY_RPCS,
    RegistrationNames,
    kernel_contract_hash,
    native_module_hash,
    schema_statements,
    unqualified,
)
from triage.monitoring.sql_permissions import build_permission_kernel, integration_contract
from triage.store.azure_sql import DEFAULT_TABLES, AzureSqlDatabase
from triage.store.azure_sql import schema_statements as application_statements

MAX_SECURITY_ROWS = 20_000
DML_PERMISSIONS = frozenset({"INSERT", "UPDATE", "DELETE"})
FUNCTION_TYPES = frozenset({"FN", "IF", "TF", "FS", "FT", "AF"})
SELECT_CALLABLE_TYPES = frozenset({"V", "SN", *FUNCTION_TYPES})
CALLABLE_TYPES = SELECT_CALLABLE_TYPES | {"P", "PC"}
READ_PERMISSIONS = frozenset({
    "CONNECT", "SELECT", "REFERENCES", "VIEW DEFINITION", "VIEW DATABASE STATE",
    "VIEW DATABASE PERFORMANCE STATE", "VIEW DATABASE SECURITY STATE", "SHOWPLAN", "UNMASK",
    "VIEW ANY COLUMN ENCRYPTION KEY DEFINITION", "VIEW ANY COLUMN MASTER KEY DEFINITION",
})


@dataclass(frozen=True)
class SqlWriterPrincipal:
    principal_id: int
    sid: bytes

    @property
    def client_id(self) -> str:
        return str(UUID(bytes_le=self.sid))


@dataclass(frozen=True)
class AuthoritySnapshot:
    server_identity: str
    database_id: int
    operator_principal_id: int
    observed_at: datetime
    snapshot_hash: str
    kernel_hash: str
    writers: tuple[SqlWriterPrincipal, ...]
    gaps: tuple[str, ...]

    def require_protected(self) -> None:
        if self.gaps:
            raise DeploymentError("SQL registration authority is unproved: " + ", ".join(self.gaps))


def _rows(database: AzureSqlDatabase, sql: str, width: int) -> list[tuple]:
    rows = database.query(sql)
    if len(rows) > MAX_SECURITY_ROWS or any(len(row) != width for row in rows):
        raise DeploymentError("SQL authority metadata is incomplete or exceeds its bounded budget")
    return [tuple(row) for row in rows]


def _plain(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return value


def expected_modules(names: RegistrationNames = DEFAULT_REGISTRATION_NAMES) -> dict[str, tuple[str, str]]:
    """name -> (native NVARCHAR hash, classification), from the current generators."""
    kernel = build_permission_kernel()
    read_names = {unqualified(rpc.object_name) for key, rpc in kernel.rpcs.items() if key in READ_ONLY_RPCS}
    write_views = {unqualified(value) for value in integration_contract()["write_routes"].values()}
    result = {}
    for obj in kernel.objects:
        if obj.kind == "role":
            continue
        name = unqualified(obj.name)
        category = (
            "read" if name in read_names or obj.kind == "function"
            or obj.kind == "view" and name not in write_views else
            "procedure" if obj.kind == "procedure" else "view"
        )
        result[name] = (native_module_hash(obj.ddl), category)
    for ddl in application_statements(dict(DEFAULT_TABLES)):
        if "CREATE OR ALTER PROCEDURE" in ddl:
            import re

            match = re.search(r"CREATE OR ALTER PROCEDURE\s+((?:\[dbo\]\.\[\w+\])|(?:dbo\.\w+))", ddl)
            if not match:
                raise DeploymentError("Application module declaration needs explicit operator support")
            result[unqualified(match[1])] = (native_module_hash(ddl), "procedure")
    result[names.read_projection] = (native_module_hash(schema_statements(names)[2]), "read")
    return result


def read_authority(
    database: AzureSqlDatabase, target: ResetTarget, catalogue: Any, *,
    names: RegistrationNames = DEFAULT_REGISTRATION_NAMES,
) -> AuthoritySnapshot:
    if (
        (getattr(database, "_server", None), getattr(database, "_database", None)) != (target.server, target.database)
        or getattr(getattr(database, "_credential", None), "target", None) != target
        or catalogue.registration_names != names
    ):
        raise DeploymentError("SQL authority must use the exact pinned database and declared registration catalogue")
    identity = _rows(database, """/* deployment-authority:identity */
SELECT @@TRANCOUNT, HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),
       HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','CONTROL'), USER_ID(),
       CONVERT(NVARCHAR(512),SERVERPROPERTY('ServerName')), DB_NAME(), DB_ID(), SYSUTCDATETIME()
FROM sys.database_principals WHERE principal_id=USER_ID()
""", 8)
    if len(identity) != 1:
        raise DeploymentError("SQL authority identity is unreadable")
    transaction, visible, control, operator, server, db_name, db_id, now = identity[0]
    if (
        not isinstance(transaction, int) or transaction < 1 or visible != 1 or control != 1
        or type(operator) is not int or not server or db_name != target.database
        or type(db_id) is not int or db_id < 1 or not isinstance(now, datetime)
    ):
        raise DeploymentError(
            "Use the active explicit deployer transaction with complete SQL metadata visibility "
            "and existing operator CONTROL authority on the exact database"
        )
    now = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    principals = _rows(database, """/* deployment-authority:principals */
SELECT principal_id,type,sid,is_fixed_role,owning_principal_id,name
FROM sys.database_principals ORDER BY principal_id
""", 6)
    roles = _rows(database, """/* deployment-authority:roles */
SELECT member_principal_id,role_principal_id FROM sys.database_role_members
ORDER BY member_principal_id,role_principal_id
""", 2)
    permissions = _rows(database, """/* deployment-authority:permissions */
SELECT grantee_principal_id,grantor_principal_id,class,major_id,minor_id,permission_name,state
FROM sys.database_permissions
ORDER BY grantee_principal_id,class,major_id,minor_id,permission_name,state
""", 7)
    schemas = _rows(database, """/* deployment-authority:schemas */
SELECT schema_id,name,principal_id FROM sys.schemas ORDER BY schema_id
""", 3)
    objects = _rows(database, """/* deployment-authority:objects */
SELECT o.object_id,s.name,o.name,o.type,COALESCE(o.principal_id,s.principal_id),
 CONVERT(VARCHAR(64),HASHBYTES('SHA2_256',
   LTRIM(RTRIM(REPLACE(m.definition,CHAR(13)+CHAR(10),CHAR(10))))),2),
 m.execute_as_principal_id,
 (SELECT COUNT_BIG(*) FROM sys.crypt_properties cp WHERE cp.major_id=o.object_id)
FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id
LEFT JOIN sys.sql_modules m ON m.object_id=o.object_id
WHERE o.is_ms_shipped=0 ORDER BY o.object_id
""", 8)
    columns = _rows(database, """/* deployment-authority:columns */
SELECT s.name,o.name,c.name,t.name,c.max_length,c.scale,c.is_nullable,
 c.is_identity,c.is_computed,c.generated_always_type,c.encryption_type,c.collation_name
FROM sys.tables o JOIN sys.schemas s ON s.schema_id=o.schema_id
JOIN sys.columns c ON c.object_id=o.object_id JOIN sys.types t ON t.user_type_id=c.user_type_id
WHERE o.is_ms_shipped=0 ORDER BY s.name,o.name,c.column_id
""", 12)
    triggers = _rows(database, """/* deployment-authority:triggers */
SELECT object_id,parent_class,parent_id,is_disabled FROM sys.triggers
WHERE is_ms_shipped=0 ORDER BY object_id
""", 4)
    queues = _rows(database, """/* deployment-authority:queues */
SELECT object_id,is_activation_enabled,is_receive_enabled,activation_procedure,execute_as_principal_id
FROM sys.service_queues WHERE is_ms_shipped=0 ORDER BY object_id
""", 5)
    foreign_keys = _rows(database, """/* deployment-authority:cascades */
SELECT object_id,parent_object_id,referenced_object_id,
       delete_referential_action,update_referential_action,is_disabled
FROM sys.foreign_keys WHERE is_ms_shipped=0 ORDER BY object_id
""", 6)
    gaps: set[str] = set()
    by_id = {row[0]: row for row in principals}
    if len(by_id) != len(principals) or operator not in by_id:
        raise DeploymentError("SQL principal universe is malformed")
    public_ids = {row[0] for row in principals if row[5] == "public"}
    if len(public_ids) != 1:
        raise DeploymentError("SQL public role metadata is missing or duplicated")
    closure = {key: {key, *public_ids} for key in by_id}
    if any(member not in by_id or role not in by_id or by_id[role][1] != "R" for member, role in roles):
        raise DeploymentError("SQL role graph has an unresolved principal")
    for _ in range(128):
        changed = False
        for member, role in roles:
            addition = closure[role] - closure[member]
            if addition:
                closure[member].update(addition)
                changed = True
        if not changed:
            break
    else:
        raise DeploymentError("SQL transitive role graph exceeded its bounded closure")
    if any(member != role and member in closure[role] for member, role in roles):
        raise DeploymentError("SQL role membership is cyclic")
    modules = expected_modules(names)
    by_name = {(row[1], row[2]): row for row in objects}
    by_object = {row[0]: row for row in objects}
    by_schema = {row[0]: row for row in schemas}
    schema_names = {row[1] for row in schemas}
    if (
        len(by_object) != len(objects) or len(by_name) != len(objects)
        or len(by_schema) != len(schemas) or len(schema_names) != len(schemas)
        or any(row[1] not in schema_names for row in objects)
    ):
        raise DeploymentError("SQL object/schema authority identities are incomplete or duplicated")
    schema_objects = {
        schema[0]: tuple(row for row in objects if row[1] == schema[1])
        for schema in schemas
    }
    owned_names = set(catalogue.table_names) | set(modules)
    owned_ids = {row[0] for row in objects if row[1] == "dbo" and row[2] in owned_names}
    if any(
        row[1] not in {0, 1}
        or row[1] == 1 and (row[2] not in by_object or by_object[row[2]][3] not in {"U", "V"})
        for row in triggers
    ):
        raise DeploymentError("SQL trigger authority has an unresolved parent or activation scope")
    trigger_parents = {row[2] for row in triggers if row[1] == 1 and not row[3]}
    if trigger_parents & owned_ids or any(row[1] == 0 and not row[3] for row in triggers):
        gaps.add("unreviewed_trigger_authority")
    if any(row[1] or row[2] and row[3] for row in queues):
        gaps.add("autonomous_sql_writer")
    cascades: dict[int, list[tuple[int, int, int]]] = {}
    for _, child_id, parent_id, on_delete, on_update, disabled in foreign_keys:
        if (
            child_id not in by_object or parent_id not in by_object
            or by_object[child_id][3] != "U" or by_object[parent_id][3] != "U"
            or on_delete not in {0, 1, 2, 3} or on_update not in {0, 1, 2, 3}
        ):
            raise DeploymentError("SQL referential-action authority has an unresolved object or operation")
        if not disabled and (on_delete or on_update):
            cascades.setdefault(parent_id, []).append((child_id, on_delete, on_update))
            # A reviewed RPC's table effects also change when a new cascade is
            # attached. No current owned table declaration authorizes one.
            if parent_id in owned_ids:
                gaps.add("unreviewed_owned_cascade_authority")
    protected_names = {*names.tables.values(), names.read_projection, catalogue.table("monitoring_receipts").name}
    dbo = [row for row in schemas if row[1] == "dbo"]
    if len(dbo) != 1 or dbo[0][2] != 1:
        gaps.add("unsupported_schema_ownership")
    for name, (expected, _) in modules.items():
        row = by_name.get(("dbo", name))
        if row is None:
            gaps.add("declared_module_missing")
        elif (
            not isinstance(row[5], str) or row[5].lower() != expected
            or row[4] != 1 or row[6] is not None or row[7] != 0
        ):
            gaps.add("module_definition_or_authority_changed")
    for table in catalogue.tables:
        actual = [row[2:] for row in columns if row[:2] == ("dbo", table.name)]
        expected = [
            (c.name, c.data_type, c.max_length, c.scale, c.nullable, False, False, 0, None)
            for c in table.columns
        ]
        # SQL datetime2 storage metadata varies by endpoint; scale must still match.
        if len(actual) != len(expected) or any(
            a[:2] != e[:2] or a[3:9] != e[3:] or (
                a[2] not in {6, 7, 8} if e[1] == "datetime2" else a[2] != e[2]
            ) or column.collation is not None and a[9] != column.collation
            for a, e, column in zip(actual, expected, table.columns, strict=False)
        ):
            gaps.add("declared_table_missing_or_incompatible")
        obj = by_name.get(("dbo", table.name))
        if obj is None or obj[3] != "U" or obj[4] != 1:
            gaps.add("table_ownership_or_type_changed")
    kernel = build_permission_kernel()
    expected_roles = {kernel.names.role(component) for component in kernel.grants}
    present_roles = {row[5] for row in principals if row[1] == "R"}
    if not expected_roles <= present_roles:
        gaps.add("kernel_roles_missing")
    for row in objects:
        if row[2].startswith("triage_") and (row[1] != "dbo" or row[2] not in owned_names) and row[3] not in {
            "PK", "UQ", "C", "D", "F",
        }:
            gaps.add("undeclared_accelerator_object")

    def permission_objects(permission_class: int, major: int) -> tuple[tuple, ...] | None:
        if permission_class == 0:
            return tuple(objects)
        if permission_class == 3:
            return schema_objects.get(major)
        if permission_class == 1:
            return (by_object[major],) if major in by_object else None
        return None

    def reviewed_module(obj: tuple) -> str | None:
        declaration = modules.get(obj[2]) if obj[1] == "dbo" else None
        if (
            declaration is None or not isinstance(obj[5], str) or obj[5].lower() != declaration[0]
            or obj[4] != 1 or obj[6] is not None or obj[7] != 0
        ):
            return None
        return declaration[1]

    candidates = set()
    for principal_id, principal in by_id.items():
        if principal_id in {1, 3, 4, operator} or principal[1] == "R":
            continue
        reachable = closure[principal_id]
        role_names = {by_id[key][5] for key in reachable}
        persistent = False
        mutations: set[tuple[int, str]] = set()
        if role_names & {"db_owner", "db_ddladmin", "db_securityadmin", "db_accessadmin", "db_datawriter"}:
            gaps.add("legacy_or_privileged_role")
            persistent = True
        if any(row[2] in reachable for row in schemas) or any(
            row[4] in reachable for row in objects
        ) or any(row[1] == "R" and row[4] in reachable for row in principals):
            gaps.add("runtime_ownership_path")
            persistent = True
        for obj in objects:
            if obj[4] not in reachable:
                continue
            if obj[3] in {"U", "V"}:
                mutations.update((obj[0], operation) for operation in DML_PERMISSIONS)
            if obj[3] in CALLABLE_TYPES and (
                reviewed_module(obj) is None or obj[6] is not None or obj[7]
            ):
                gaps.add("unreviewed_owned_module_authority")
                persistent = True
        for grantee, _grantor, permission_class, major, _minor, permission, state in permissions:
            if state not in {"G", "W", "D", "R"}:
                gaps.add("unsupported_permission_state")
            if grantee not in reachable or state not in {"G", "W"}:
                continue
            if permission not in READ_PERMISSIONS | {
                "INSERT", "UPDATE", "DELETE", "EXECUTE",
            } and not permission.startswith(("CONTROL", "ALTER", "CREATE", "TAKE OWNERSHIP", "IMPERSONATE")):
                gaps.add("unmodelled_permission_authority")
                persistent = True
            if permission_class not in {0, 1, 3} and permission not in READ_PERMISSIONS:
                gaps.add("unmodelled_permission_class")
                persistent = True
            privilege = permission in {"CONTROL", "ALTER", "TAKE OWNERSHIP", "IMPERSONATE"} or (
                permission.startswith(("ALTER ANY ", "CREATE "))
            )
            if privilege and permission_class in {0, 1, 3, 4}:
                gaps.add("runtime_ddl_control_or_impersonation")
                persistent = True
            if state == "W" and permission_class in {0, 1, 3}:
                gaps.add("runtime_grant_option")
            if permission not in DML_PERMISSIONS | {"EXECUTE", "SELECT"}:
                continue
            targets = permission_objects(permission_class, major)
            if targets is None:
                gaps.add("uninspectable_permission_target")
                persistent = True
                continue
            if permission in DML_PERMISSIONS | {"EXECUTE"} and permission_class != 1:
                gaps.add("broad_runtime_write_grant")
                if permission == "EXECUTE" and permission_class == 3:
                    gaps.add("unreviewed_schema_execution")
                persistent = True
            if permission in DML_PERMISSIONS | {"EXECUTE"} and grantee in public_ids:
                gaps.add("public_write_authority")
            for obj in targets:
                if permission in DML_PERMISSIONS:
                    mutations.add((obj[0], permission))
                elif permission == "EXECUTE" or permission == "SELECT" and obj[3] in SELECT_CALLABLE_TYPES:
                    category = reviewed_module(obj)
                    if category is None:
                        gaps.add("unreviewed_module_authority")
                        persistent = True
                    elif permission == "EXECUTE" and category != "read":
                        persistent = True

        visited = set()
        while mutations:
            object_id, operation = mutations.pop()
            if (object_id, operation) in visited:
                continue
            visited.add((object_id, operation))
            obj = by_object[object_id]
            if object_id in trigger_parents:
                gaps.add("unreviewed_trigger_authority")
                persistent = True
            if object_id in owned_ids and obj[2] in protected_names:
                gaps.add("registration_or_capture_mutation_path")
                persistent = True
            elif object_id in owned_ids and obj[3] == "U":
                gaps.add("raw_operational_table_write")
                persistent = True
            elif obj[3] != "U":
                category = reviewed_module(obj)
                if category is None:
                    gaps.add("unreviewed_module_authority")
                elif category == "read":
                    gaps.add("unexpected_read_surface_write")
                persistent = True
            if operation in {"UPDATE", "DELETE"}:
                for child_id, on_delete, on_update in cascades.get(object_id, ()):
                    action = on_delete if operation == "DELETE" else on_update
                    if action:
                        child_operation = "DELETE" if operation == "DELETE" and action == 1 else "UPDATE"
                        mutations.add((child_id, child_operation))
        if persistent:
            candidates.add(principal_id)

    # Convert to the collector identity universe only after every permission,
    # implicit owner and reachable DML side effect has been classified.
    writers = []
    for principal_id in sorted(candidates):
        principal = by_id[principal_id]
        sid = principal[2]
        if principal[1] != "E" or not isinstance(sid, (bytes, bytearray, memoryview)) or len(sid) != 16:
            gaps.add("unsupported_group_or_sql_writer_identity")
            continue
        if principal_id == operator or bytes(sid) == UUID(target.deployer_object_id).bytes_le:
            gaps.add("operator_identity_reused_by_runtime")
            continue
        writers.append(SqlWriterPrincipal(principal_id, bytes(sid)))
    if len({writer.sid for writer in writers}) != len(writers):
        gaps.add("duplicate_sql_identity_sid")
    stable = {
        "server": server, "database": db_name, "database_id": db_id,
        "principals": [[_plain(cell) for cell in row] for row in principals],
        "roles": roles, "permissions": permissions, "schemas": schemas, "objects": objects,
        "columns": columns, "triggers": triggers, "queues": queues,
        "foreign_keys": foreign_keys, "catalogue": catalogue.declaration_hash,
        "kernel": kernel_contract_hash(),
    }
    return AuthoritySnapshot(
        server_identity=server, database_id=db_id, operator_principal_id=operator,
        observed_at=now, snapshot_hash=fingerprint(stable, domain="deployment.sql.authority.v1"),
        kernel_hash=kernel_contract_hash(), writers=tuple(sorted(writers, key=lambda item: item.principal_id)),
        gaps=tuple(sorted(gaps)),
    )
