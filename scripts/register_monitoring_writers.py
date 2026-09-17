"""Explicit operator registration for prototype reset, not runtime bootstrap.

Order (use the same explicitly pinned target/operator arguments for each command):
  1. reset_monitoring_state.py --plan-initialization / --initialize creates the
     empty maintenance baseline. No old history is copied or deleted.
  2. This tool --ddl emits the exact two-table/view catalogue and its hash.
     Install the reviewed sql_permissions kernel separately with the deployer.
  3. --prepare --binding-id ID --allow-identity-association-preview --output plan.json
     is read-only. It can report old broad roles/running writers; such a plan
     cannot be accepted. Parent retires roles, stops every writer and reconciles
     in-flight effects. This tool neither grants permissions nor stops resources.
  4. --install --confirm-ddl-hash HASH creates only missing registration objects.
     Installation is also possible before role retirement, but nothing stored
     in that unprotected interval is accepted as proof. Prefer installing after
     retirement. Prepare a NEW plan after that cutover. Then --accept --plan plan.json
     --confirm-manifest-hash HASH --allow-identity-association-preview performs
     fresh complete observations and the atomic protected append.
  5. --reconcile --plan plan.json only reads the original receipt. Use it after a
     lost acknowledgement; never invent another request ID to repair uncertainty.
  6. reset_monitoring_state.py --allow-identity-association-preview --output reset.json
     uses this concrete reader automatically and derives its observer selectors.
     Execute only with that manifest's exact hash/epoch. New-epoch rows, registration
     rows, operator captures, reset receipts and rate limits are protected on replay.

Registration lasts at most one hour and is re-observed at every reset gate.
Root-management-group metadata, subscription-wide read visibility, no unresolved
deny assignments, operator-only Graph identity reads and supported resource/data-
plane reads are prerequisites. Reverse UAMI association is explicitly preview.
Supported bindings are App Service and slots, Container App revisions/jobs,
hosted Foundry versions, literal/parameter-bound Foundry Logic App invokers, and
the deployed oauthMI SQL procedure-action connection. Unknown hosting types,
unavailable scopes, external/federated identities, opaque SecureString endpoints
and unsupported expressions/connectors refuse coverage, including when a parent
resource is stopped. No caller-supplied complete flag is accepted. SQL CONTROL
and complete security metadata visibility are existing operator prerequisites;
this tool grants neither. SQL and Azure reads are not one atomic platform
snapshot, so writers must remain stopped and grants frozen until cutover ends.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import reset_monitoring_state as reset
from triage.monitoring.deployment_authority import AuthoritySnapshot
from triage.monitoring.deployment_contracts import DeploymentError, DiscoveryCapture, fingerprint
from triage.monitoring.deployment_discovery import AzureDeploymentDiscovery
from triage.monitoring.deployment_registry import (
    DeploymentRegistrationOperator,
    RegistrationPlan,
    RegistrationResult,
    install_registration,
    inventory_from_capture,
)
from triage.monitoring.deployment_schema import (
    DEFAULT_REGISTRATION_NAMES,
    RegistrationNames,
    integration_contract,
    object_catalogue,
)
from triage.store.azure_sql import SqlUnavailable


def live_quiescence_guard(
    operator: reset.SqlResetOperator, *,
    observer_factory: Callable = reset.LiveOperatorObserver,
) -> Callable[[RegistrationPlan, DiscoveryCapture, AuthoritySnapshot], None]:
    """Bind the real SQL target/action inventory to fresh resource/effect reads."""

    def verify(plan: RegistrationPlan, capture: DiscoveryCapture, authority: AuthoritySnapshot) -> None:
        snapshot = operator._snapshot()
        if snapshot.blockers or not snapshot.hazards_complete or snapshot.control is None:
            raise DeploymentError("Registration acceptance requires the exact ready maintenance schema and complete SQL hazard inventory")
        inventory = inventory_from_capture(
            capture, authority, operator.target, operator.catalogue,
            binding_id=plan.binding_id, revision=plan.expected_revision + 1, request_id=plan.operation_id,
        )
        profile = reset.ObservationProfile(
            target=operator.target, writers=tuple(item.writer for item in inventory.writers),
            action_targets=snapshot.required_action_targets,
        )
        _, _, requested_at = operator._identity()
        request = reset.PreflightRequest(
            challenge=str(uuid4()), manifest_hash=plan.manifest_hash, target=operator.target,
            expected_epoch=snapshot.control.epoch, ownership_hash=snapshot.state_hash,
            requested_at=requested_at, profile=profile, hazards=snapshot.hazards,
            required_action_targets=snapshot.required_action_targets, deployment_inventory=inventory,
        )
        observer = observer_factory(profile, operator.db._credential)
        try:
            observation = observer.observe(request)
            _, _, now = operator._identity()
            reset.validate_observation(observation, request, now)
        finally:
            observer.close()

    return verify


def main(
    argv: Sequence[str] | None = None, *,
    database_factory: Callable = reset.create_database,
    collector_factory: Callable = AzureDeploymentDiscovery,
    observer_factory: Callable = reset.LiveOperatorObserver,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--ddl", action="store_true")
    modes.add_argument("--install", action="store_true")
    modes.add_argument("--prepare", action="store_true")
    modes.add_argument("--accept", action="store_true")
    modes.add_argument("--reconcile", action="store_true")
    for flag in ("server", "database", "tenant-id", "deployer-object-id", "subscription-id", "managed-identity-client-id", "operator-domain"):
        parser.add_argument("--" + flag)
    parser.add_argument("--credential", choices=("azure-cli", "broker", "managed-identity"))
    parser.add_argument("--registration-names")
    parser.add_argument("--binding-id")
    parser.add_argument("--allow-identity-association-preview", action="store_true")
    parser.add_argument("--plan")
    parser.add_argument("--confirm-manifest-hash")
    parser.add_argument("--confirm-ddl-hash")
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    if not args.ddl and not all((args.server, args.database, args.tenant_id, args.deployer_object_id, args.credential)):
        parser.error("Every SQL operation requires an explicit target and selected operator credential")
    if args.install and not args.confirm_ddl_hash:
        parser.error("--install requires the exact --confirm-ddl-hash")
    if (args.accept or args.reconcile) and not args.plan:
        parser.error("--accept/--reconcile requires the original --plan file")
    if args.accept and not args.confirm_manifest_hash:
        parser.error("--accept requires the exact --confirm-manifest-hash")
    if not (args.accept or args.reconcile) and (args.plan or args.confirm_manifest_hash):
        parser.error("A plan/hash does not select execution; choose --accept or --reconcile explicitly")
    if not args.install and args.confirm_ddl_hash:
        parser.error("A DDL hash does not select installation; choose --install explicitly")
    if not (args.ddl or args.install or args.accept or args.reconcile) and not args.binding_id:
        parser.error("Read-only preparation requires a stable --binding-id")
    collector = None
    committed: RegistrationResult | None = None
    try:
        if args.output and Path(args.output).exists():
            raise DeploymentError("Use a new output file; existing approval/receipt files are never overwritten")
        names = RegistrationNames(**json.loads(reset._read_file(args.registration_names))) if args.registration_names else DEFAULT_REGISTRATION_NAMES
        if args.ddl:
            output = integration_contract(names) | {
                "ddl_hash": fingerprint(object_catalogue(names), domain="deployment.registration.ddl.v1"),
            }
        else:
            target = reset.ResetTarget(
                server=args.server, database=args.database, tenant_id=args.tenant_id,
                deployer_object_id=args.deployer_object_id,
            )
            selection = reset.CredentialSelection(
                mode=args.credential, managed_identity_client_id=args.managed_identity_client_id,
                subscription_id=args.subscription_id, operator_domain=args.operator_domain,
            )
            database = database_factory(target, selection)
            reset_operator = reset.SqlResetOperator(database, target, registration_names=names)
            if args.install:
                install_registration(database, reset_operator.catalogue, confirmed_ddl_hash=args.confirm_ddl_hash, names=names)
                output = {"mode": "installed", "ddl_hash": args.confirm_ddl_hash, "protected_acceptance": False}
            else:
                collector = collector_factory(
                    database._credential, target,
                    allow_identity_association_preview=args.allow_identity_association_preview,
                )
                operator = DeploymentRegistrationOperator(
                    database, target, reset_operator.catalogue, collector, names=names,
                    verify_quiescence=live_quiescence_guard(reset_operator, observer_factory=observer_factory),
                )
                if args.plan:
                    document = json.loads(reset._read_file(args.plan))
                    if not isinstance(document, dict) or set(document) != {"mode", "manifest_hash", "plan"} or document["mode"] != "prepare":
                        raise DeploymentError("Use the original read-only preparation document")
                    plan = RegistrationPlan.model_validate(document["plan"])
                    if document["manifest_hash"] != plan.manifest_hash:
                        raise DeploymentError("Preparation document hash does not match its content")
                    result = operator.reconcile(plan) if args.reconcile else operator.accept(
                        plan, confirmed_manifest_hash=args.confirm_manifest_hash,
                    )
                    committed = result
                    output = {"mode": "reconciled" if args.reconcile else "accepted", **result.model_dump(mode="json")}
                else:
                    plan = operator.prepare(binding_id=args.binding_id)
                    output = {"mode": "prepare", "manifest_hash": plan.manifest_hash, "plan": plan.model_dump(mode="json")}
        encoded = json.dumps(output, indent=2, ensure_ascii=True)
        if args.output:
            with Path(args.output).open("x", encoding="utf-8") as stream:
                stream.write(encoded + "\n")
        else:
            print(encoded)
        return 0
    except (DeploymentError, reset.ResetError, SqlUnavailable, ValueError, TypeError, OSError) as exc:
        error = {
            "error": type(exc).__name__,
            "detail": str(exc) if isinstance(exc, (DeploymentError, reset.ResetError)) else "Operator input, output or SQL operation failed",
        }
        if committed is not None:
            error["committed_receipt"] = committed.receipt.model_dump(mode="json")
        print(json.dumps(error), file=sys.stderr)
        return 1
    finally:
        if collector is not None:
            collector.close()


if __name__ == "__main__":
    raise SystemExit(main())
