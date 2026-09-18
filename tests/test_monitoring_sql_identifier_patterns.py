from __future__ import annotations

import pytest

from triage.monitoring.sql_permissions import build_permission_kernel


def statement(operation: str) -> str:
    return next(
        obj.ddl for obj in build_permission_kernel().objects if obj.logical_name == operation
    )


@pytest.mark.parametrize(
    "operation",
    [
        "worker.partition",
        "worker.commit_positions",
        "worker.advance_checkpoint",
        "worker.observe_retention",
    ],
)
def test_native_broker_identity_guard_places_literal_hyphen_first(operation):
    sql = statement(operation)
    assert "@consumer_group COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9$_.]%'" in sql
    assert "%[^A-Za-z0-9$_.-]%" not in sql
    assert "LEN(@consumer_group) NOT BETWEEN 1 AND 50" in sql


def test_native_endpoint_guards_preserve_their_distinct_character_sets():
    sql = statement("worker.observe_connector")
    expected = {
        "namespace": "%[^-A-Za-z0-9.]%",
        "entity": "%[^-A-Za-z0-9_./]%",
        "consumer_group": "%[^-A-Za-z0-9$_.]%",
    }
    for field, pattern in expected.items():
        assert f"'$.endpoint.{field}') COLLATE Latin1_General_100_BIN2 LIKE '{pattern}'" in sql
    assert "Endpoint fields must be nonsecret host/entity/group metadata, not credentials or URLs" in sql


def test_native_proposal_node_name_has_literal_hyphen_not_a_range():
    sql = statement("controller.publish_connector")
    assert "'$.node_name') COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_.]%'" in sql
    assert "DATALENGTH(JSON_VALUE(p.value,'$.node_name'))>512" in sql


def test_native_rate_bucket_has_literal_hyphen_and_keeps_colon_explicit():
    sql = statement("worker.rate_budget")
    assert "@bucket COLLATE Latin1_General_100_BIN2 LIKE '%[^-A-Za-z0-9_.:]%'" in sql
