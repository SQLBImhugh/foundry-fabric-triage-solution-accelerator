from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from triage.monitoring import deployment_schema
from triage.monitoring.deployment_contracts import CAPTURE_OPERATION
from triage.monitoring.schema import (
    initialize_monitoring_permission_kernel,
    permission_kernel_objects,
    resolve_kernel_tables,
    schema_statements,
)
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.monitoring.sql_store import AzureSqlMonitoringStore


def test_registration_contract_keeps_three_separate_deployer_batches_and_zero_runtime_grants():
    names = deployment_schema.RegistrationNames(
        registration="fixture_operator_registration",
        writers="fixture_operator_writers",
        read_projection="fixture_operator_authority",
    )
    contract = deployment_schema.integration_contract(names)
    statements = deployment_schema.schema_statements(names)
    objects = deployment_schema.object_catalogue(names)
    assert len(statements) == 3
    assert statements == contract["statements"]
    assert objects == contract["objects"]
    assert contract["runtime_grants"] == ()
    assert contract["preserved_receipt_operations"] == (CAPTURE_OPERATION,)
    assert [item["kind"] for item in objects] == ["table", "table", "view"]
    assert [item["reset_operation"] for item in objects] == [
        "preserve_registration", "preserve_registration", "retain_definition",
    ]
    for item, statement in zip(objects, statements, strict=True):
        assert item["sha256"] == hashlib.sha256(statement.encode("utf-8")).hexdigest()
    assert "LEFT JOIN" in statements[2]
    assert not any("GRANT " in statement for statement in statements)


def test_registration_names_do_not_change_kernel_namespace_or_physical_baseline():
    tables = {"monitoring_records": "fixture_monitoring_records", "monitoring_rate_budget": "fixture_rate_budget"}
    before = build_permission_kernel(resolve_kernel_tables(tables=tables))
    registration = deployment_schema.RegistrationNames(
        registration="fixture_registration",
        writers="fixture_writers",
        read_projection="fixture_writer_authority",
    )
    registration_objects = deployment_schema.object_catalogue(registration)
    after = build_permission_kernel(resolve_kernel_tables(tables=tables))
    assert before.names.suffix == after.names.suffix
    assert before.catalogue() == after.catalogue() == permission_kernel_objects(tables)
    assert not set(registration.tables) & set(after.names.tables)
    assert not {item["name"] for item in registration_objects} & {item["name"] for item in after.catalogue()}
    assert not any("fixture_registration" in statement for statement in schema_statements(tables))


@pytest.mark.parametrize("key", ["deployment_registration", "deployment_writers", "deployment_writer_authority", "rate_budget"])
def test_kernel_schema_and_runtime_reject_operator_keys_before_io(key):
    tables = {key: "fixture_operator_object"}
    database = SimpleNamespace(_tables=tables)
    for operation in (
        lambda: resolve_kernel_tables(database),
        lambda: permission_kernel_objects(tables),
        lambda: initialize_monitoring_permission_kernel(database),
        lambda: AzureSqlMonitoringStore(db=database, component="controller"),
    ):
        with pytest.raises(ValueError, match="separate object map"):
            operation()


def test_registration_capture_preservation_is_not_runtime_authority_or_a_completeness_flag():
    contract = deployment_schema.integration_contract()
    assert CAPTURE_OPERATION == "operator.deployment_capture"
    assert contract["runtime_grants"] == ()
    assert "complete" not in contract and "native_sql_proven" not in contract
    assert all(
        item["logical_name"] not in resolve_kernel_tables()
        for item in contract["objects"]
    )
