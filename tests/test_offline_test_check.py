from __future__ import annotations

import pytest

from scripts.check_offline_tests import violations


@pytest.mark.parametrize("source", [
    "import azure.identity",
    "import azure.identity.aio as identity",
    "from azure.identity import DefaultAzureCredential",
    "from azure import identity",
    "importlib.import_module('azure.identity')",
    "pytest.importorskip('azure.identity')",
    "__import__('azure.identity.aio')",
    "response = httpx.get('https://example.test')",
    "requests.post(\n    'https://example.test'\n)",
    "import requests as client\nclient.get('https://example.test')",
    "from httpx import get as fetch",
    "from requests import *",
])
def test_direct_live_imports_and_calls_are_refused(source: str) -> None:
    assert violations(source)


@pytest.mark.parametrize("source", [
    'identity = ModuleType("azure.identity")',
    'monkeypatch.setitem(sys.modules, "azure.identity", identity)',
    'monkeypatch.setattr("azure.identity.ManagedIdentityCredential", Credential)',
    'assert imports == ["azure.identity.aio"]',
    '# Do not call requests.get() or import azure.identity here.\npass',
    'source = "import azure.identity\\nhttpx.get(\'https://example.test\')"',
    'import httpx\nclient = httpx.Client(transport=httpx.MockTransport(handler))',
])
def test_offline_doubles_and_source_fixture_strings_remain_allowed(source: str) -> None:
    assert violations(source) == []


def test_failures_include_the_actual_code_line() -> None:
    assert violations("pass\nimport azure.identity\n") == [(2, "live credential import")]
