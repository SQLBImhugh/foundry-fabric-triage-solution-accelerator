from __future__ import annotations

import copy
import sys
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from configure_command_center_groups import (  # noqa: E402
    SUFFIXES,
    configure_groups,
    group_spec,
    main,
)
from register_command_center import RegistrationError  # noqa: E402

TENANT = "10000000-0000-0000-0000-000000000001"
APP = "20000000-0000-0000-0000-000000000002"
USER = "30000000-0000-0000-0000-000000000003"
PRINCIPAL = "40000000-0000-0000-0000-000000000004"


class Graph:
    def __init__(self):
        self.roles = [{"id": str(uuid4()), "value": value, "isEnabled": True, "allowedMemberTypes": ["User"]} for value in SUFFIXES]
        self.groups = {}
        self.references = {}
        self.assignments = [{"id": "existing-direct", "principalId": USER, "appRoleId": self.roles[-1]["id"]}]
        self.writes = []
        self.licensed = True
        self.owner_tenant = TENANT

    def collection(self, path, **query):
        if path == "servicePrincipals":
            return [{"id": PRINCIPAL, "appId": APP, "appOwnerOrganizationId": self.owner_tenant, "signInAudience": "AzureADMyOrg", "accountEnabled": True, "appRoles": self.roles}]
        if path.endswith("/licenseDetails"):
            return [{"servicePlans": [{"servicePlanName": "AAD_PREMIUM_P2", "provisioningStatus": "Success"}]}] if self.licensed else []
        if path == "groups":
            name = query["$filter"].removeprefix("displayName eq '").removesuffix("'")
            return [group for group in self.groups.values() if group["displayName"] == name]
        if path.endswith("/appRoleAssignedTo"):
            return copy.deepcopy(self.assignments)
        return [{"id": value} for value in self.references.get(path, [])]

    def request(self, method, path, body=None):
        if method == "GET":
            if path.startswith("users/"):
                return {"id": USER, "accountEnabled": True}
            return copy.deepcopy(self.groups[path.split("/")[1].split("?")[0]])
        self.writes.append((method, path, copy.deepcopy(body)))
        if path == "groups":
            group = {key: value for key, value in body.items() if "@odata" not in key}
            group["id"] = str(uuid4())
            group["isAssignableToRole"] = False
            self.groups[group["id"]] = group
            for relation in ("owners", "members"):
                self.references[f"groups/{group['id']}/{relation}"] = [
                    value.rsplit("/", 1)[1] for value in body.get(f"{relation}@odata.bind", [])
                ]
            return copy.deepcopy(group)
        if path.endswith("/appRoleAssignedTo"):
            record = {"id": str(uuid4()), **body}
            self.assignments.append(record)
            return record
        relation = path.removesuffix("/$ref")
        self.references.setdefault(relation, []).append(body["@odata.id"].rsplit("/", 1)[1])
        return {}


def test_creates_only_owned_security_groups_and_retains_direct_assignment():
    graph = Graph()
    result = configure_groups(graph, tenant_id=TENANT, app_id=APP, administrator_id=USER)
    assert len(result["groups"]) == 4
    assert len(graph.writes) == 8
    assert graph.assignments[0]["id"] == "existing-direct"
    for group in result["groups"]:
        actual = graph.groups[group["id"]]
        assert actual["securityEnabled"] and not actual["mailEnabled"] and not actual["groupTypes"]
        assert graph.references[f"groups/{group['id']}/owners"] == [USER]
        assert graph.references[f"groups/{group['id']}/members"] == ([USER] if group["role"] == "CommandCenter.Admin" else [])
    assert not result["direct_assignments_removed"]
    configure_groups(graph, tenant_id=TENANT, app_id=APP, administrator_id=USER)
    assert len(graph.writes) == 8, "A reconciled rerun must not duplicate groups or grants"


def test_refuses_unmarked_name_collision_before_creating_any_group():
    graph = Graph()
    spec = group_spec(APP, "CommandCenter.Admin", USER, "BI Triage")
    graph.groups["other"] = {"id": str(uuid4()), **spec, "description": "Not owned by this app"}
    with pytest.raises(RegistrationError, match="not the expected"):
        configure_groups(graph, tenant_id=TENANT, app_id=APP, administrator_id=USER)
    assert graph.writes == []


@pytest.mark.parametrize("problem", ["license", "tenant"])
def test_refuses_unlicensed_or_wrong_tenant_before_writes(problem):
    graph = Graph()
    if problem == "license":
        graph.licensed = False
    else:
        graph.owner_tenant = str(uuid4())
    with pytest.raises(RegistrationError):
        configure_groups(graph, tenant_id=TENANT, app_id=APP, administrator_id=USER)
    assert graph.writes == []


def test_default_cli_is_an_offline_plan(capsys):
    assert main(["--subscription", "Example", "--tenant-id", TENANT, "--app-id", APP, "--admin-current-user"]) == 0
    output = capsys.readouterr().out
    assert "offline_dry_run" in output and '"writes": 0' in output
