"""Register reviewed physical Eventstream ownership; never activate runtime intake.

Prepare (SELECT-only SQL; capture is a closed, explicitly reviewed operator file):
  python scripts\\register_monitoring_connector.py --prepare --capture capture.json \
    --server HOST --database DB --tenant-id GUID --deployer-object-id GUID \
    --credential azure-cli --subscription-id GUID --output connector-plan.json
Apply with the SAME explicit identity/target flags:
  --apply --plan connector-plan.json --confirm-manifest-hash HASH
After any uncertain acknowledgement, retain the original plan and use:
  --reconcile --plan connector-plan.json

The operator capture includes the exact original app-owned creation request,
intent/receipt hash, owned item marker and fresh complete definition/topology
readbacks. Hashes bind reviewed evidence; they do not prove its network origin.
EndpointMetadata is supplied explicitly, never obtained from a key-returning API.
No mode calls Fabric, creates schema, grants roles, changes maintenance, enables
scopes or publishes connector_desired/readiness. Registered source IDs are owned
physical metadata, not admitted targets. Current reviewed source admission and
verified event capability must precede genuine controller publication, then
fresh identity and delivery proof before event-mode readiness. First publication
does not require an otherwise unnecessary policy revision. Registration does
not queue work.

Only an absent connector or the exact current unbound planned connector without
desired-publication/work/effect history is eligible. A different established
binding requires reconciliation, not adoption by display name or replacement.
One authorized deployer may prepare and apply. No extra signer is required.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Literal

from pydantic import model_validator

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import reset_monitoring_state as reset
from triage.monitoring.connector_bootstrap import (
    BootstrapPlan,
    ConnectorBootstrapOperator,
    ConnectorCapture,
)
from triage.monitoring.deployment_contracts import DeploymentError, OperatorModel, ResetTarget
from triage.monitoring.models import Fingerprint
from triage.store.azure_sql import SqlUnavailable


class PlanDocument(OperatorModel):
    mode: Literal["prepare"] = "prepare"
    manifest_hash: Fingerprint
    plan: BootstrapPlan

    @model_validator(mode="after")
    def validate_hash(self) -> PlanDocument:
        if self.manifest_hash != self.plan.manifest_hash:
            raise ValueError("The preparation document hash differs from its original plan")
        return self


def _unique(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise DeploymentError("Operator JSON contains duplicate keys")
        result[name] = value
    return result


def _document(path: str) -> str:
    raw = reset._read_file(path)
    json.loads(raw, object_pairs_hook=_unique)
    return raw


def _publish_output(path: Path, content: str) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as stream:
            temporary = Path(stream.name)
            if stream.write(content) != len(content):
                raise OSError("Operator output write was incomplete")
            stream.flush()
            os.fsync(stream.fileno())
        # Link only complete durable bytes; unlike replace, a competing original
        # output cannot be overwritten and a failed write leaves no final name.
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(
    argv: Sequence[str] | None = None, *, database_factory: Callable = reset.create_database,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    modes = parser.add_mutually_exclusive_group(required=True)
    for mode in ("prepare", "apply", "reconcile"):
        modes.add_argument("--" + mode, action="store_true")
    for name in ("server", "database", "tenant-id", "deployer-object-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--credential", choices=("azure-cli", "broker", "managed-identity"), required=True)
    for name in ("subscription-id", "managed-identity-client-id", "operator-domain", "capture", "plan", "confirm-manifest-hash", "output"):
        parser.add_argument("--" + name)
    args = parser.parse_args(argv)
    if (
        args.prepare and (not args.capture or args.plan or args.confirm_manifest_hash)
        or not args.prepare and (not args.plan or args.capture)
        or args.apply and not args.confirm_manifest_hash
        or args.reconcile and args.confirm_manifest_hash
    ):
        parser.error("Use --prepare --capture, --apply --plan --confirm-manifest-hash, or --reconcile --plan")
    result = None
    try:
        if args.output and Path(args.output).exists():
            raise DeploymentError("Use a new output path; never overwrite original evidence or receipts")
        target = ResetTarget(
            server=args.server, database=args.database, tenant_id=args.tenant_id,
            deployer_object_id=args.deployer_object_id,
        )
        selection = reset.CredentialSelection(
            mode=args.credential, subscription_id=args.subscription_id,
            managed_identity_client_id=args.managed_identity_client_id, operator_domain=args.operator_domain,
        )
        capture = ConnectorCapture.model_validate_json(_document(args.capture)) if args.prepare else None
        document = PlanDocument.model_validate_json(_document(args.plan)) if args.plan else None
        operator = ConnectorBootstrapOperator(database_factory(target, selection), target)
        if capture is not None:
            plan = operator.prepare(capture)
            output = PlanDocument(manifest_hash=plan.manifest_hash, plan=plan).model_dump(mode="json")
        else:
            result = operator.reconcile(document.plan) if args.reconcile else operator.apply(
                document.plan, confirmed_manifest_hash=args.confirm_manifest_hash,
            )
            output = {"mode": "reconcile" if args.reconcile else "apply", **result.model_dump(mode="json")}
        encoded = json.dumps(output, sort_keys=True, indent=2, ensure_ascii=True)
        if args.output:
            _publish_output(Path(args.output), encoded + "\n")
        else:
            print(encoded)
        return 0
    except (DeploymentError, reset.ResetError, SqlUnavailable, ValueError, TypeError, OSError) as exc:
        error = {
            "error": type(exc).__name__,
            "detail": str(exc) if isinstance(exc, (DeploymentError, reset.ResetError)) else "Connector input, output or SQL operation failed",
        }
        if result is not None:
            error["committed_receipt"] = result.receipt.model_dump(mode="json")
        print(json.dumps(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
