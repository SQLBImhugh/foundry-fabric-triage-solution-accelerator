"""Source adapter tests independent of SQL installation/generator availability."""

from __future__ import annotations

import json
import socket
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from test_monitoring_deployment_registration import (
    CALLBACK,
    CLIENT2,
    CONNECTION,
    ROOT,
    TIMER,
    InventorySources,
)
from test_monitoring_reset import (
    DEPLOYER,
    IDENTITY_RESOURCE,
    NOW,
    RUNTIME_CLIENT,
    SITE,
    SUBSCRIPTION,
    TARGET,
    OfflineCredential,
    identity_document,
    settings_document,
)

from scripts import reset_monitoring_state as reset
from triage.monitoring.deployment_authority import AuthoritySnapshot, SqlWriterPrincipal
from triage.monitoring.deployment_contracts import DeploymentError
from triage.monitoring.deployment_discovery import AzureDeploymentDiscovery, DiscoveryThrottled
from triage.monitoring.deployment_schema import declared_write_procedures


@pytest.fixture(autouse=True)
def no_live_connections(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *_: pytest.fail("Discovery unit tests are offline"))


def source_fixture(*, two_identities=False, **kwargs):
    source = InventorySources()
    clients = [RUNTIME_CLIENT, *([CLIENT2] if two_identities else [])]
    authority = AuthoritySnapshot(
        server_identity="fixture-sql-server", database_id=7, operator_principal_id=5,
        observed_at=NOW, snapshot_hash="a" * 64, kernel_hash="b" * 64,
        writers=tuple(SqlWriterPrincipal(11 + index, UUID(client).bytes_le) for index, client in enumerate(clients)),
        gaps=(),
    )
    collector = AzureDeploymentDiscovery(
        reset.PinnedDeployerCredential(OfflineCredential(), TARGET), TARGET,
        transport=httpx.MockTransport(source.handle), allow_identity_association_preview=True,
        clock=lambda: NOW, monotonic=lambda: 0.0, sleep=lambda _: None, **kwargs,
    )
    return source, collector, authority


@pytest.mark.parametrize("resource", ["site", "sql_callback", "foundry", "foundry_with_timer", "slot"])
def test_supported_resource_bindings_use_actual_metadata_and_keep_only_safe_projections(resource):
    source, collector, authority = source_fixture(two_identities=resource in {"sql_callback", "foundry", "foundry_with_timer"})
    if resource == "sql_callback":
        source.add_sql_callback()
    elif resource in {"foundry", "foundry_with_timer"}:
        source.add_foundry()
        if resource == "foundry_with_timer":
            source.add_timer()
    elif resource == "slot":
        slot = SITE + "/slots/staging"
        source.slots[SITE] = [slot]
        source.sites[slot] = {"state": "Stopped", "settings": settings_document(), "identity": identity_document()}
    capture = collector.collect(authority)
    assert all(item.state in {"stopped", "disabled", "inactive"} for item in capture.writers)
    assert "DO-NOT-PERSIST-THIS" not in capture.model_dump_json()
    assert "PRIVATE-REASON" not in capture.model_dump_json()
    if resource == "sql_callback":
        callback = next(item for item in capture.writers if item.writer.resource_id == CALLBACK)
        assert callback.expected_sql_sid == UUID(CLIENT2).bytes_le.hex()
        assert any(request.url.path == CONNECTION for request in source.requests)
    elif resource == "foundry_with_timer":
        timer = next(item for item in capture.writers if item.writer.resource_id == TIMER)
        assert timer.invokes_writer_id and timer.expected_sql_sid is None


@pytest.mark.parametrize("change", ["database", "credential", "procedure", "dynamic_path", "identity"])
def test_sql_connection_read_refuses_unproved_bindings(change):
    source, collector, authority = source_fixture(two_identities=True)
    source.add_sql_callback()
    props = source.callback["properties"]
    if change == "database":
        props["parameters"]["sqlDatabase"]["value"] = "another_database"
    elif change == "credential":
        source.connection_mode = "sqlAuthentication"
    elif change == "procedure":
        action = props["definition"]["actions"]["record"]["inputs"]
        action["path"] = action["path"].replace("triage_record_approval_decision", "unrelated_proc")
    elif change == "dynamic_path":
        props["definition"]["actions"]["record"]["inputs"]["path"] = "@triggerBody()['path']"
    else:
        props["parameters"]["$connections"]["value"]["sql"]["connectionProperties"]["authentication"]["identity"] = IDENTITY_RESOURCE
    with pytest.raises(DeploymentError):
        collector.collect(authority)
    assert all(request.method == "GET" or request.url.path.endswith(
        ("/config/appsettings/list", "/listAssociatedResources"),
    ) for request in source.requests)


@pytest.mark.parametrize("change", ["wrong_tenant", "foreign_page", "partial", "duplicate", "throttle", "unsupported_host", "federation", "reused_operator"])
def test_source_scope_identity_and_pagination_fail_closed(change):
    source, collector, authority = source_fixture()

    def response(request):
        if change == "wrong_tenant" and request.url.path == f"/subscriptions/{SUBSCRIPTION}":
            return httpx.Response(200, json={"tenantId": str(UUID(int=777)), "subscriptionId": SUBSCRIPTION, "state": "Enabled"})
        if request.url.path == ROOT + "/descendants":
            if change == "foreign_page":
                return httpx.Response(200, json={"value": [], "nextLink": "https://foreign.example/next"})
            if change == "partial":
                return httpx.Response(200, json={"value": [], "has_more": True})
            if change == "throttle":
                return httpx.Response(429, headers={"Retry-After": "19"})
        return None

    source.response_hook = response
    if change == "duplicate":
        source.resources.append(source.resources[0])
    elif change == "unsupported_host":
        source.resources.append({"id": SITE.replace("Microsoft.Web/sites", "Microsoft.Example/hosts"), "type": "Microsoft.Example/hosts"})
    elif change == "federation":
        source.federation = [{"id": IDENTITY_RESOURCE + "/federatedIdentityCredentials/external"}]
    elif change == "reused_operator":
        source.identities[RUNTIME_CLIENT] = DEPLOYER
    with pytest.raises(DeploymentError) as error:
        collector.collect(authority)
    if change == "throttle":
        assert isinstance(error.value, DiscoveryThrottled) and error.value.retry_after_seconds == 19


def test_sql_path_names_come_from_declared_contracts_without_executing_kernel_ddl():
    names = declared_write_procedures()
    assert "triage_record_approval_decision" in names
    assert "arbitrary_table_or_procedure" not in names


@pytest.mark.parametrize("suffix", [
    "", "/databases", "/elasticPools", "/administrators",
    "/azureADOnlyAuthentications", "/auditingSettings", "/databases/auditingSettings",
])
def test_azure_sql_resources_do_not_create_an_unmodelled_application_writer(suffix):
    source, collector, authority = source_fixture()
    kind = "Microsoft.Sql/servers" + suffix
    resource = SITE.split("/providers/")[0] + "/providers/Microsoft.Sql/servers/state"
    for child in suffix.split("/")[1:]:
        resource += f"/{child}/fixture"
    source.resources.append({"id": resource, "type": kind})
    capture = collector.collect(authority)
    assert {writer.writer.resource_id for writer in capture.writers} == {SITE}
    assert capture.gaps == ()


def test_sql_job_agents_are_not_hidden_by_the_passive_database_classification():
    source, collector, authority = source_fixture()
    resource = SITE.split("/providers/")[0] + "/providers/Microsoft.Sql/servers/state/jobAgents/scheduler"
    source.resources.append({"id": resource, "type": "Microsoft.Sql/servers/jobAgents"})
    with pytest.raises(DeploymentError, match="Unsupported resource type"):
        collector.collect(authority)


def test_explicit_preview_request_budget_and_timestamped_capture_not_saved_claims():
    source, collector, authority = source_fixture(max_requests=1)
    with pytest.raises(DeploymentError, match="budget"):
        collector.collect(authority)
    collector.max_requests = 100
    collector.allow_preview = False
    with pytest.raises(DeploymentError, match="preview"):
        collector.collect(authority)
    collector.allow_preview = True
    first = collector.collect(authority)
    collector.clock = lambda: NOW + timedelta(seconds=1)
    second = collector.collect(authority)
    assert first.capture_hash != second.capture_hash and first.binding_hash == second.binding_hash
    assert json.loads(first.model_dump_json())["finished_at"] != json.loads(second.model_dump_json())["finished_at"]
