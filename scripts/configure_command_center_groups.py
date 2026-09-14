"""Create owned Entra security groups and assign the command-center app roles.

Default execution is an offline plan. --apply requires an already authorized
operator, Entra P1/P2 and the directory permissions to create groups and assign
app roles. Nothing grants those permissions to the app or to this caller.
Existing direct assignments and operational data are never deleted here.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from typing import Any

from register_command_center import (
    GraphClient,
    RegistrationError,
    graph_token,
    guid,
    subscription_name,
    unique,
)

SUFFIXES = {
    "CommandCenter.Reader": "Readers",
    "CommandCenter.Operator": "Operators",
    "CommandCenter.Approver": "Approvers",
    "CommandCenter.Admin": "Administrators",
}
GROUP_SELECT = "id,displayName,description,mailEnabled,securityEnabled,groupTypes,isAssignableToRole"


def group_spec(app_id: str, role: str, owner_id: str, prefix: str) -> dict[str, Any]:
    app_id, owner_id = guid(app_id), guid(owner_id)
    if role not in SUFFIXES or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,100}", prefix):
        raise RegistrationError("Use a known app role and a simple nonempty group prefix.")
    result = {
        "displayName": f"{prefix} {SUFFIXES[role]}",
        "description": f"Command-center access group. App {app_id}; role {role}.",
        "mailEnabled": False, "securityEnabled": True, "groupTypes": [],
        "mailNickname": f"triage-{app_id[:8]}-{role.rsplit('.', 1)[1].lower()}",
        "owners@odata.bind": [f"https://graph.microsoft.com/v1.0/users/{owner_id}"],
    }
    if role == "CommandCenter.Admin":
        result["members@odata.bind"] = [f"https://graph.microsoft.com/v1.0/users/{owner_id}"]
    return result


def _check_group(group: dict[str, Any], spec: dict[str, Any]) -> None:
    if (
        group.get("description") != spec["description"]
        or group.get("securityEnabled") is not True or group.get("mailEnabled") is not False
        or group.get("groupTypes") not in ([], None) or group.get("isAssignableToRole") is True
    ):
        raise RegistrationError("An existing group has this name but is not the expected app-owned ordinary security group.")
    guid(group.get("id", ""))


def _wait_for_reference(graph: GraphClient, path: str, object_id: str) -> None:
    for attempt in range(6):
        if any(row.get("id") == object_id for row in graph.collection(path, **{"$select": "id"})):
            return
        if attempt < 5:
            time.sleep(2)
    raise RegistrationError(f"Directory reference did not become visible at {path}. Do not assume the write failed; reconcile before retrying.")


def configure_groups(
    graph: GraphClient, *, tenant_id: str, app_id: str, administrator_id: str,
    prefix: str = "BI Triage",
) -> dict[str, Any]:
    tenant_id, app_id, administrator_id = map(guid, (tenant_id, app_id, administrator_id))
    principal = unique(graph.collection("servicePrincipals", **{"$filter": f"appId eq '{app_id}'"}), "application principals")
    if (
        principal is None or principal.get("appOwnerOrganizationId") != tenant_id
        or principal.get("signInAudience") != "AzureADMyOrg" or principal.get("accountEnabled") is not True
    ):
        raise RegistrationError("Use the enabled single-tenant application in the asserted tenant.")
    user = graph.request("GET", f"users/{administrator_id}?$select=id,accountEnabled")
    if user.get("id") != administrator_id or user.get("accountEnabled") is not True:
        raise RegistrationError("The administrator must be an enabled existing Entra user.")
    licenses = graph.collection(f"users/{administrator_id}/licenseDetails")
    if not any(
        plan.get("servicePlanName") in {"AAD_PREMIUM", "AAD_PREMIUM_P2"}
        and plan.get("provisioningStatus") == "Success"
        for license in licenses for plan in license.get("servicePlans", [])
    ):
        raise RegistrationError("Group-based application assignment requires an active Entra P1/P2 plan.")
    role_ids = {}
    for value in SUFFIXES:
        role = unique([row for row in principal.get("appRoles", []) if row.get("value") == value], value)
        if role is None or not role.get("isEnabled") or role.get("allowedMemberTypes") != ["User"]:
            raise RegistrationError(f"Enable the user/group app role {value} before configuring its group.")
        role_ids[value] = guid(role["id"])
    planned = []
    for value in SUFFIXES:
        spec = group_spec(app_id, value, administrator_id, prefix)
        name = spec["displayName"].replace("'", "''")
        existing = unique(graph.collection("groups", **{
            "$filter": f"displayName eq '{name}'", "$select": GROUP_SELECT,
        }), spec["displayName"])
        if existing:
            _check_group(existing, spec)
        planned.append((value, spec, existing))
    assignments_path = f"servicePrincipals/{principal['id']}/appRoleAssignedTo"
    assignments = graph.collection(assignments_path)
    result = []
    for value, spec, existing in planned:
        group = existing or graph.request("POST", "groups", spec)
        group_id = guid(group.get("id", ""))
        actual = graph.request("GET", f"groups/{group_id}?$select={GROUP_SELECT}")
        _check_group(actual, spec)
        for relation in ("owners", "members") if value == "CommandCenter.Admin" else ("owners",):
            path = f"groups/{group_id}/{relation}"
            if existing and not any(row.get("id") == administrator_id for row in graph.collection(path, **{"$select": "id"})):
                graph.request("POST", f"{path}/$ref", {
                    "@odata.id": f"https://graph.microsoft.com/v1.0/directoryObjects/{administrator_id}",
                })
            _wait_for_reference(graph, path, administrator_id)
        if not any(row.get("principalId") == group_id and row.get("appRoleId") == role_ids[value] for row in assignments):
            graph.request("POST", assignments_path, {
                "principalId": group_id, "resourceId": principal["id"], "appRoleId": role_ids[value],
            })
        result.append({"name": spec["displayName"], "id": group_id, "role": value, "role_id": role_ids[value]})
    actual_assignments = graph.collection(assignments_path)
    if not all(any(row.get("principalId") == item["id"] and row.get("appRoleId") == item["role_id"] for row in actual_assignments) for item in result):
        raise RegistrationError("Not all app-role group assignments are visible. Reconcile directory state before continuing.")
    return {"application_id": app_id, "service_principal_id": principal["id"], "groups": result, "direct_assignments_removed": False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subscription", required=True, type=subscription_name)
    parser.add_argument("--tenant-id", required=True, type=guid)
    parser.add_argument("--app-id", required=True, type=guid)
    parser.add_argument("--group-prefix", default="BI Triage")
    parser.add_argument("--admin-current-user", action="store_true", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        group_spec(args.app_id, "CommandCenter.Admin", "00000000-0000-0000-0000-000000000001", args.group_prefix)
        if not args.apply:
            print(json.dumps({
                "mode": "offline_dry_run", "groups": [f"{args.group_prefix} {name}" for name in SUFFIXES.values()],
                "role_values": list(SUFFIXES), "writes": 0,
            }, indent=2))
            return 0
        graph = GraphClient(graph_token(args.subscription, args.tenant_id))
        administrator = guid(graph.request("GET", "me?$select=id").get("id", ""))
        print(json.dumps(configure_groups(
            graph, tenant_id=args.tenant_id, app_id=args.app_id,
            administrator_id=administrator, prefix=args.group_prefix,
        ), indent=2))
        return 0
    except (RegistrationError, argparse.ArgumentTypeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
