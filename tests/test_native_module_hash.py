from __future__ import annotations

import hashlib

import pytest

from triage.monitoring.deployment_contracts import fingerprint
from triage.monitoring.deployment_schema import (
    kernel_abi,
    kernel_contract_hash,
    module_definition,
    native_module_hash,
)
from triage.monitoring.schema import resolve_kernel_tables
from triage.monitoring.sql_permissions import build_permission_kernel
from triage.store.azure_sql import DEFAULT_TABLES
from triage.store.azure_sql import schema_statements as application_statements

NATIVE_SOURCE = (
    "CREATE OR ALTER VIEW [dbo].[__triage_module_diag_0297f2c1af0848adbb0892af506d5dc5] "
    "AS SELECT CAST(1 AS int) AS [probe_value];"
)
NATIVE_CATALOGUE = (
    "CREATE   VIEW [dbo].[__triage_module_diag_0297f2c1af0848adbb0892af506d5dc5] "
    "AS SELECT CAST(1 AS int) AS [probe_value];"
)
NATIVE_HASH = "b9f048cf3e9457133325de66f35b66c9304279d24ffbad14255476be4d9b75c2"


def _utf16_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-16-le")).hexdigest()


def test_exact_native_view_pair_changes_only_expected_metadata() -> None:
    assert _utf16_hash(NATIVE_SOURCE) == (
        "5b49e7eeccf31131f2dbd70dabce37d562ad446581ab2f32ab7e8b8b307d9aed"
    )
    assert len(NATIVE_CATALOGUE.encode("utf-16-le")) == 236
    assert _utf16_hash(NATIVE_CATALOGUE) == NATIVE_HASH
    assert native_module_hash(NATIVE_SOURCE) == NATIVE_HASH
    assert native_module_hash(NATIVE_CATALOGUE) == NATIVE_HASH
    assert module_definition(NATIVE_SOURCE) == NATIVE_SOURCE


def test_outer_padding_and_crlf_keep_existing_normalization_only() -> None:
    source = (
        "\r\n \tCREATE \tOR\r\nALTER\t ViEw [dbo].[v]\r\n"
        "AS\r\n    SELECT N'CREATE OR ALTER' AS [value]; \r\n\t"
    )
    expected = "CREATE \t\n\t ViEw [dbo].[v]\nAS\n    SELECT N'CREATE OR ALTER' AS [value];"
    assert native_module_hash(source) == _utf16_hash(expected)
    assert "CREATE OR ALTER" in expected
    assert module_definition(source).startswith("CREATE \tOR\nALTER\t ViEw")


@pytest.mark.parametrize(
    "kind,body",
    [
        ("VIEW", "AS SELECT 1 AS [value];"),
        ("PROCEDURE", "AS SELECT N'CREATE OR ALTER VIEW' AS [value];"),
        ("FUNCTION", "() RETURNS int AS BEGIN RETURN 1; END;"),
    ],
)
def test_supported_header_spellings_use_token_removal_not_body_canonicalization(kind, body):
    # PROCEDURE/FUNCTION here are source-format regressions, not new native evidence.
    source = f"cReAtE OR ALTER {kind} [dbo].[sample] {body}"
    expected = f"cReAtE   {kind} [dbo].[sample] {body}"
    assert native_module_hash(source) == _utf16_hash(expected)
    assert native_module_hash(expected) == _utf16_hash(expected)


@pytest.mark.parametrize(
    "old,new",
    [
        ("CAST(1", "CAST(2"),
        (" AS int", " AS INT"),
        (" AS SELECT", " AS  SELECT"),
        ("[probe_value]", "[different_value]"),
    ],
)
def test_native_body_or_case_changes_are_not_hidden(old, new):
    changed = NATIVE_SOURCE.replace(old, new)
    assert changed != NATIVE_SOURCE
    assert native_module_hash(changed) != NATIVE_HASH


def test_module_body_indentation_and_header_case_remain_significant() -> None:
    first = "CREATE OR ALTER VIEW dbo.v AS\n    SELECT 1 AS [value];"
    assert native_module_hash(first) != native_module_hash(first.replace("    SELECT", "\tSELECT"))
    assert native_module_hash(first) != native_module_hash(first.replace("CREATE", "create", 1))
    assert native_module_hash("CREATE VIEW dbo.v AS SELECT 1 AS v;") != native_module_hash(
        "CREATE OR ALTER VIEW dbo.v AS SELECT 1 AS v;",
    )


@pytest.mark.parametrize(
    "ddl",
    [
        "ALTER VIEW dbo.v AS SELECT 1 AS v;",
        "ALTER PROCEDURE dbo.p AS SELECT 1;",
        "ALTER FUNCTION dbo.f() RETURNS int AS BEGIN RETURN 1; END;",
        "CREATE OR ALTER PROC dbo.p AS SELECT 1;",
        "CREATE OR /* comment */ ALTER VIEW dbo.v AS SELECT 1 AS v;",
    ],
)
def test_unsupported_declarations_are_not_silently_reinterpreted(ddl):
    with pytest.raises(ValueError, match="Not a declared SQL module"):
        native_module_hash(ddl)


def test_current_application_procedure_uses_the_supported_full_header() -> None:
    procedures = [
        ddl for ddl in application_statements(dict(DEFAULT_TABLES))
        if "CREATE OR ALTER PROCEDURE" in ddl
    ]
    assert len(procedures) == 1
    definition = module_definition(procedures[0])
    assert definition.startswith("CREATE OR ALTER PROCEDURE dbo.triage_record_approval_decision")
    expected = definition.replace("CREATE OR ALTER PROCEDURE", "CREATE   PROCEDURE", 1)
    assert native_module_hash(procedures[0]) == _utf16_hash(expected)


def test_utf8_sql_catalogue_and_complete_kernel_abi_remain_unchanged() -> None:
    tables = resolve_kernel_tables()
    kernel = build_permission_kernel(tables)
    before = kernel.statements
    for obj in kernel.objects:
        if obj.kind != "role":
            native_module_hash(obj.ddl)
    assert kernel.statements == before
    # The receipt-bound pending-window acknowledgement changes only the
    # frontier resolver; native header projection cannot change source bytes.
    assert kernel_contract_hash(tables) == (
        "aecd875c50898006970a0df5c7b5ced46cbb6b0050550ba4c48918537bab094c"
    )
    assert fingerprint(kernel_abi(tables)) == kernel_contract_hash(tables)
    assert fingerprint(list(kernel.catalogue())) == (
        "5eaabac224b5c55dcd5e73fa81496de5716ce598bdb1e7bae1f98e7d37b5850a"
    )
