"""Deployer-only registration of an already-created app-owned Eventstream.

The closed operator capture is reviewed evidence, not a live Fabric observation
or proof of managed-identity access/delivery. This module never calls Fabric,
retrieves endpoint keys, installs schema or publishes controller authority.
The existing epoch-bound receipt table records one immutable operator result.
Explicit prototype reset remains a separate operation; never reuse its epoch.
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from pydantic import Field, model_validator

from triage.monitoring import models as m
from triage.monitoring.deployment_contracts import (
    DeploymentConflict,
    DeploymentError,
    DeploymentUncertain,
    OperatorModel,
    ResetTarget,
    canonical_json,
    fingerprint,
)
from triage.monitoring.provisioning import (
    ProvisioningReview,
    _complete_component_map,
    decode_snapshot,
    plan_definition,
)
from triage.monitoring.schema import DEFAULT_MONITORING_TABLES
from triage.redaction import redact_text
from triage.store.azure_sql import (
    AzureSqlDatabase,
    SqlCommitUncertain,
    SqlRollbackUncertain,
    SqlUnavailable,
    quote_identifier,
)

OPERATION = "operator.connector_bootstrap"
GATE = "gated_until_controller_publication"
MAX_CAPTURE_AGE = timedelta(minutes=15)
MAX_CONNECTORS = 100
RECORDS = quote_identifier(DEFAULT_MONITORING_TABLES["monitoring_records"])
RECEIPTS = quote_identifier(DEFAULT_MONITORING_TABLES["monitoring_receipts"])
CONTROL = quote_identifier(DEFAULT_MONITORING_TABLES["monitoring_control"])
_SECRET_KEY = re.compile(
    r"secret|password|connectionstring|accesskey|accountkey|primarykey|secondarykey|apikey|credentials?|sas|token|authorization",
    re.I,
)


def _nonsecret(value: object) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if _SECRET_KEY.search(re.sub(r"[^a-z]", "", key.lower())):
                raise DeploymentError("Connector capture contains a credential-bearing field")
            _nonsecret(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _nonsecret(child)
    elif isinstance(value, str) and redact_text(value) != value:
        raise DeploymentError("Connector metadata cannot be persisted without redaction")


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise DeploymentError("Connector registration requires a SQL timestamp")
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _key(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def _payload_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-16-le")).hexdigest()


def _definition_envelope(value: object) -> None:
    if not isinstance(value, dict) or set(value) - {"format", "parts"} or value.get("format", "eventstream") != "eventstream":
        raise ValueError("Definition evidence has unsupported envelope fields")
    parts = value.get("parts")
    if not isinstance(parts, list) or not 1 <= len(parts) <= 3 or any(
        not isinstance(part, dict) or set(part) != {"path", "payloadType", "payload"} for part in parts
    ):
        raise ValueError("Definition evidence must retain complete inline parts only")


class EventstreamItem(OperatorModel):
    id: m.CanonicalId
    workspaceId: m.CanonicalId
    type: Literal["Eventstream"]
    displayName: m.Label
    description: Annotated[str, Field(strict=True, min_length=1, max_length=512)]


class CreationReceipt(OperatorModel):
    """Nonsecret projection of the retained original create/LRO completion."""

    intent_id: m.CanonicalId
    workspace_id: m.CanonicalId
    eventstream_id: m.CanonicalId
    request_sha256: m.Fingerprint
    completed_at: m.UtcDateTime


class ConnectorCapture(OperatorModel):
    version: Literal[1] = 1
    provenance: Literal["operator_reviewed_capture"]
    review: Literal["original_creation_and_complete_current_readbacks_reviewed"]
    request_id: m.CanonicalId
    expected: m.RegistryVersion
    expected_maintenance: m.StrictBool
    connector_id: m.CanonicalId
    expected_connector_revision: m.Revision
    ownership_id: m.CanonicalId
    name: m.Label
    endpoint: m.EndpointMetadata
    sources: Annotated[tuple[m.ConnectorSource, ...], Field(min_length=1, max_length=100)]
    creation_request: m.JsonObject
    creation_request_sha256: m.Fingerprint
    creation_receipt: CreationReceipt
    creation_receipt_sha256: m.Fingerprint
    item: EventstreamItem
    definition_response: m.JsonObject
    topology_response: m.JsonObject
    readback_sha256: m.Fingerprint
    observed_at: m.UtcDateTime

    @model_validator(mode="after")
    def validate_evidence(self) -> ConnectorCapture:
        receipt, item = self.creation_receipt, self.item
        if set(self.creation_request) != {"displayName", "description", "definition"}:
            raise ValueError("Creation evidence must contain the exact submitted item/definition fields")
        definition = self.creation_request["definition"]
        if not isinstance(definition, dict) or set(self.definition_response) != {"definition"}:
            raise ValueError("Original creation definition is not a complete supported envelope")
        _definition_envelope(definition)
        _definition_envelope(self.definition_response["definition"])
        _nonsecret(self.topology_response)
        _nonsecret(self.item.model_dump(mode="json"))
        marker = f"Owned monitoring bootstrap; intent={self.ownership_id}; definition={fingerprint(definition)}"
        if (
            self.creation_request_sha256 != fingerprint(self.creation_request)
            or self.creation_receipt_sha256 != fingerprint(receipt.model_dump(mode="json"))
            or receipt.intent_id != self.ownership_id
            or (receipt.workspace_id, receipt.eventstream_id) != (item.workspaceId, item.id)
            or receipt.request_sha256 != self.creation_request_sha256
            or self.creation_request["description"] != marker or item.description != marker
            or self.creation_request["displayName"] != item.displayName
            or receipt.completed_at > self.observed_at
        ):
            raise ValueError("Original app-owned creation, marker and current physical item do not match")
        if self.readback_sha256 != fingerprint({
            "item": item.model_dump(mode="json"), "definition": self.definition_response,
            "topology": self.topology_response, "observed_at": self.observed_at.isoformat(),
        }):
            raise ValueError("Complete readback evidence hash does not match")
        if not re.fullmatch(r"[A-Za-z0-9_./-]{1,256}", self.endpoint.entity) or not re.fullmatch(
            r"[A-Za-z0-9$_.-]{1,256}", self.endpoint.consumer_group,
        ):
            raise ValueError("Endpoint fields must be explicit nonsecret protocol metadata")
        for source in self.sources:
            if (
                source.target.tenant_id != self.expected.tenant_id or source.target.epoch != self.expected.epoch
                or source.target.workload != "fabric_pipeline" or source.event_source != self.expected.tenant_id
                or m.canonical_id(source.source_id) != source.source_id
            ):
                raise ValueError("Sources require exact current pipeline identities and tenant event origin")
        self.connector(self.observed_at)
        return self

    def connector(self, at: datetime) -> m.OwnedConnectorManifest:
        try:
            original = decode_snapshot({"definition": self.creation_request["definition"]}, self.topology_response)
            observed = decode_snapshot(self.definition_response, self.topology_response)
            _complete_component_map(original)
            _complete_component_map(observed)
            if original["component_ids"] != observed["component_ids"]:
                raise ValueError("Original component identities changed")
            _nonsecret(original)
            _nonsecret(observed)
            _nonsecret(self.endpoint.model_dump(mode="json"))
            m.validate_connector_definition(self.sources, observed)
            destination = observed["parts"]["eventstream.json"]["destinations"][0]
            connector = m.OwnedConnectorManifest(
                tenant_id=self.expected.tenant_id, epoch=self.expected.epoch,
                connector_id=self.connector_id, ownership_id=self.ownership_id,
                revision=self.expected_connector_revision + 1, policy_revision=self.expected.revision,
                workspace_id=self.item.workspaceId, eventstream_id=self.item.id,
                destination_id=observed["component_ids"][f"destinations/{destination['name']}"],
                name=self.name, sources=self.sources, desired_definition=observed,
                observed_definition=observed, endpoint=self.endpoint, state="planned", updated_at=at,
                gaps=(m.CoverageGap(
                    code=GATE,
                    detail="Registered physical ownership only. Current reviewed source admission, verified event capability and controller publication are required.",
                ),),
            )
            # An empty target list validates every retained physical node without
            # inventing admissions. Its proposed removals are not executed or saved.
            plan_definition(connector, observed, ())
            _nonsecret(connector.model_dump(mode="json"))
            return connector
        except (ProvisioningReview, KeyError, TypeError, ValueError) as exc:
            raise DeploymentError("Owned connector definition, source identities or routing are unverified") from exc


class BootstrapPlan(OperatorModel):
    target: ResetTarget
    capture: ConnectorCapture
    server_identity: str
    database_id: Annotated[int, Field(strict=True, ge=1)]
    prepared_at: m.UtcDateTime
    expires_at: m.UtcDateTime
    control_sha256: m.Fingerprint
    prior_connector_sha256: m.Fingerprint | None
    connector: m.OwnedConnectorManifest

    @property
    def manifest_hash(self) -> str:
        return fingerprint(self.model_dump(mode="json"), domain="connector.bootstrap.plan.v1")

    @model_validator(mode="after")
    def validate_plan(self) -> BootstrapPlan:
        if (
            self.target.tenant_id != self.capture.expected.tenant_id
            or self.connector != self.capture.connector(self.prepared_at)
            or not self.capture.observed_at <= self.prepared_at < self.expires_at
            or self.expires_at != self.capture.observed_at + MAX_CAPTURE_AGE
            or (self.prior_connector_sha256 is None) != (self.capture.expected_connector_revision == 0)
        ):
            raise ValueError("Connector preparation is not bound to the original reviewed capture")
        return self


class BootstrapReceipt(OperatorModel):
    operation: Literal["operator.connector_bootstrap"] = OPERATION
    request_id: m.CanonicalId
    manifest_hash: m.Fingerprint
    capture_sha256: m.Fingerprint
    target: ResetTarget
    server_identity: str
    database_id: Annotated[int, Field(strict=True, ge=1)]
    connector: m.OwnedConnectorManifest
    recorded_at: m.UtcDateTime
    state: Literal["registered_metadata_only"] = "registered_metadata_only"
    controller_publication: Literal["required"] = "required"
    identity_verified: Literal[False] = False
    delivery_verified: Literal[False] = False


class BootstrapResult(OperatorModel):
    receipt: BootstrapReceipt
    replayed: bool = False
    reconciled_uncertain_commit: bool = False


class ConnectorBootstrapOperator:
    """Fixed synchronous SQL boundary; only an explicitly pinned deployer may call it."""

    def __init__(self, database: AzureSqlDatabase, target: ResetTarget) -> None:
        if (
            (getattr(database, "_server", None), getattr(database, "_database", None)) != (target.server, target.database)
            or not target.server.endswith(".database.windows.net")
            or getattr(getattr(database, "_credential", None), "target", None) != target
            or getattr(getattr(database, "_local", None), "conn", None) is not None
            or any(
                key not in DEFAULT_MONITORING_TABLES or value != DEFAULT_MONITORING_TABLES[key]
                for key, value in (getattr(database, "_tables", None) or {}).items()
            )
        ):
            raise DeploymentError("Use the exact default monitoring database and explicitly pinned deployer credential")
        self.db, self.target = database, target

    def _identity(self) -> tuple[str, int, datetime]:
        rows = self.db.query("""/* connector-bootstrap:identity */
SELECT @@TRANCOUNT,HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','CONTROL'),
 HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION'),
 CONVERT(NVARCHAR(512),SERVERPROPERTY('ServerName')),DB_NAME(),DB_ID(),SYSUTCDATETIME()
""")
        if (
            len(rows) != 1 or len(rows[0]) != 7 or type(rows[0][0]) is not int or rows[0][0] < 1
            or tuple(rows[0][1:3]) != (1, 1) or rows[0][4] != self.target.database
            or str(rows[0][3]).lower() not in {self.target.server, self.target.server.removesuffix(".database.windows.net")}
            or type(rows[0][5]) is not int or rows[0][5] < 1
        ):
            raise DeploymentError("Exact SQL target and existing deployer CONTROL/metadata authority are required")
        return rows[0][3], rows[0][5], _utc(rows[0][6])

    def _snapshot(self, capture: ConnectorCapture) -> tuple[str, str | None]:
        if capture.expected.tenant_id != self.target.tenant_id:
            raise DeploymentConflict("Capture tenant differs from the selected SQL deployment")
        rows = self.db.query(f"""/* connector-bootstrap:control */
SELECT tenant_id,epoch,revision,maintenance,schema_version,payload FROM {CONTROL}
WITH (UPDLOCK,HOLDLOCK) WHERE singleton=1
""")
        if len(rows) != 1 or len(rows[0]) != 6:
            raise DeploymentError("Existing monitoring control is required; bootstrap never installs it")
        row = rows[0]
        control = m.DeploymentControl.model_validate_json(row[5])
        if tuple(row[:5]) != (
            control.tenant_id, control.epoch, control.revision, control.maintenance, control.schema_version,
        ) or (
            control.tenant_id, control.epoch, control.revision, control.maintenance,
        ) != (
            capture.expected.tenant_id, capture.expected.epoch, capture.expected.revision, capture.expected_maintenance,
        ):
            raise DeploymentConflict("Current tenant, epoch, revision or maintenance state changed")
        safety = self.db.query("""/* connector-bootstrap:table-authority */
SELECT s.name,o.name,RTRIM(o.type),COALESCE(o.principal_id,s.principal_id),
 (SELECT COUNT_BIG(*) FROM sys.triggers t WHERE t.parent_id=o.object_id AND t.is_disabled=0)
FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id
WHERE o.object_id IN (OBJECT_ID(?),OBJECT_ID(?),OBJECT_ID(?)) ORDER BY o.name
""", *(f"dbo.{DEFAULT_MONITORING_TABLES[key]}" for key in ("monitoring_control", "monitoring_records", "monitoring_receipts")))
        expected_names = {DEFAULT_MONITORING_TABLES[key] for key in ("monitoring_control", "monitoring_records", "monitoring_receipts")}
        if (
            len(safety) != 3 or any(len(item) != 5 for item in safety)
            or {item[1] for item in safety} != expected_names
            or any((item[0], *item[2:]) != ("dbo", "U", 1, 0) for item in safety)
        ):
            raise DeploymentError("Monitoring tables must retain their declared dbo ownership and no enabled triggers")
        relationships = self.db.query("""/* connector-bootstrap:table-relationships */
SELECT TOP (1) object_id FROM sys.foreign_keys
WHERE parent_object_id IN (OBJECT_ID(?),OBJECT_ID(?),OBJECT_ID(?))
 OR referenced_object_id IN (OBJECT_ID(?),OBJECT_ID(?),OBJECT_ID(?))
""", *(f"dbo.{DEFAULT_MONITORING_TABLES[key]}" for key in (
            "monitoring_control", "monitoring_records", "monitoring_receipts",
            "monitoring_control", "monitoring_records", "monitoring_receipts",
        )))
        if relationships:
            raise DeploymentError("Monitoring table foreign-key effects are outside connector bootstrap")
        candidates = self.db.query(f"""/* connector-bootstrap:connectors */
SELECT tenant_id,epoch,full_key,key_hash,revision,status,payload FROM {RECORDS}
WITH (UPDLOCK,HOLDLOCK) WHERE record_kind='connector' ORDER BY tenant_id,epoch,key_hash
OFFSET 0 ROWS FETCH NEXT {MAX_CONNECTORS + 1} ROWS ONLY
""")
        if len(candidates) > MAX_CONNECTORS:
            raise DeploymentError("Connector ownership inventory exceeds the bounded registration limit")
        prior_hash = None
        proposed = capture.connector(capture.observed_at)
        for item in candidates:
            if len(item) != 7:
                raise DeploymentError("Connector inventory has an unreadable row shape")
            saved = m.OwnedConnectorManifest.model_validate_json(item[6])
            if tuple(item[:6]) != (
                saved.tenant_id, saved.epoch, saved.connector_id, _key(saved.connector_id), saved.revision, saved.state,
            ):
                raise DeploymentError("Connector promoted identity differs from its payload")
            selected = (saved.tenant_id, saved.epoch, saved.connector_id) == (
                capture.expected.tenant_id, capture.expected.epoch, capture.connector_id,
            )
            if selected:
                if (
                    saved.revision != capture.expected_connector_revision or saved.ownership_id != capture.ownership_id
                    or saved.name != capture.name or saved.policy_revision != capture.expected.revision
                    or saved.state != "planned" or saved.sources or saved.source_proposals or saved.source_removals
                    or saved.desired_definition or saved.gaps or any(getattr(saved, field) is not None for field in (
                        "workspace_id", "eventstream_id", "destination_id", "endpoint", "operation_id",
                        "observed_definition", "identity_verified_at", "delivery_verified_at", "delivery_proof",
                        "last_receiver_activity_at",
                    ))
                ):
                    raise DeploymentConflict("Only the exact current unbound planned connector may be assigned")
                prior_hash = _payload_hash(item[6])
            elif (
                saved.ownership_id == capture.ownership_id
                or (saved.workspace_id, saved.eventstream_id) == (proposed.workspace_id, proposed.eventstream_id)
                or saved.endpoint == proposed.endpoint
                or any(
                    (source.target.tenant_id, source.target.workspace_id, source.target.item_id)
                    == (owned.target.tenant_id, owned.target.workspace_id, owned.target.item_id)
                    for source in (*saved.sources, *saved.source_proposals) for owned in capture.sources
                )
            ):
                raise DeploymentConflict("Physical transport, source or ownership is already registered")
        if (prior_hash is None) != (capture.expected_connector_revision == 0):
            raise DeploymentConflict("Expected connector presence or revision changed")
        effects = self.db.query(f"""/* connector-bootstrap:effects */
SELECT TOP (1) record_kind FROM {RECORDS} WITH (UPDLOCK,HOLDLOCK)
WHERE tenant_id=? AND epoch=? AND (
 (record_kind='connector_desired' AND full_key=?)
 OR (record_kind<>'connector' AND (
  JSON_VALUE(payload,'$.connector_id')=? OR JSON_VALUE(payload,'$.reference_id')=?
  OR JSON_VALUE(payload,'$.request_payload.connector_id')=?)))
""", capture.expected.tenant_id, capture.expected.epoch, *(capture.connector_id for _ in range(4)))
        receipts = self.db.query(f"""/* connector-bootstrap:effect-receipts */
SELECT TOP (1) operation FROM {RECEIPTS} WITH (HOLDLOCK)
WHERE tenant_id=? AND epoch=? AND (
 JSON_VALUE(payload,'$.connector_id')=? OR JSON_VALUE(payload,'$.connector.connector_id')=?
 OR JSON_VALUE(payload,'$.result.connector_id')=?)
""", capture.expected.tenant_id, capture.expected.epoch, *(capture.connector_id for _ in range(3)))
        if effects or receipts:
            raise DeploymentConflict("Published desired state or existing connector work/evidence requires reconciliation, not bootstrap")
        return _payload_hash(row[5]), prior_hash

    def prepare(self, capture: ConnectorCapture) -> BootstrapPlan:
        capture = ConnectorCapture.model_validate_json(capture.model_dump_json())
        with self.db.transaction():
            server, database_id, _ = self._identity()
            control_hash, prior_hash = self._snapshot(capture)
            current_server, current_database, now = self._identity()
            if (current_server, current_database) != (server, database_id):
                raise DeploymentConflict("SQL deployment changed during preparation")
            if not capture.observed_at <= now < capture.observed_at + MAX_CAPTURE_AGE:
                raise DeploymentConflict("Complete operator readbacks are future-dated or older than fifteen minutes")
        return BootstrapPlan(
            target=self.target, capture=capture, server_identity=server, database_id=database_id,
            prepared_at=now, expires_at=capture.observed_at + MAX_CAPTURE_AGE,
            control_sha256=control_hash, prior_connector_sha256=prior_hash, connector=capture.connector(now),
        )

    def _receipt(self, plan: BootstrapPlan) -> BootstrapReceipt | None:
        rows = self.db.query(f"""/* connector-bootstrap:receipt */
SELECT request_id,fingerprint,recorded_at,payload FROM {RECEIPTS}
WHERE tenant_id=? AND epoch=? AND operation=? AND request_hash=?
""", plan.capture.expected.tenant_id, plan.capture.expected.epoch, OPERATION, _key(plan.capture.request_id))
        if not rows:
            return None
        if len(rows) != 1 or len(rows[0]) != 4:
            raise DeploymentError("Original connector bootstrap receipt is unreadable")
        row = rows[0]
        receipt = BootstrapReceipt.model_validate_json(row[3])
        if (
            row[0] != plan.capture.request_id or row[1] != plan.manifest_hash
            or receipt.request_id != plan.capture.request_id or receipt.manifest_hash != plan.manifest_hash
            or receipt.capture_sha256 != fingerprint(plan.capture.model_dump(mode="json"))
            or receipt.target != plan.target or receipt.connector != plan.connector
            or (receipt.server_identity, receipt.database_id) != (plan.server_identity, plan.database_id)
            or receipt.recorded_at != _utc(row[2])
        ):
            raise DeploymentConflict("Original connector request ID or immutable receipt conflicts")
        return receipt

    def reconcile(self, plan: BootstrapPlan) -> BootstrapResult:
        plan = BootstrapPlan.model_validate_json(plan.model_dump_json())
        if plan.target != self.target:
            raise DeploymentConflict("Original plan belongs to another target or deployer")
        with self.db.transaction():
            server, database_id, _ = self._identity()
            if (server, database_id) != (plan.server_identity, plan.database_id):
                raise DeploymentConflict("Original receipt belongs to another SQL deployment")
            receipt = self._receipt(plan)
        if receipt is None:
            raise DeploymentUncertain("Original connector registration is not established; no write was retried")
        return BootstrapResult(receipt=receipt, replayed=True)

    def apply(self, plan: BootstrapPlan, *, confirmed_manifest_hash: str) -> BootstrapResult:
        plan = BootstrapPlan.model_validate_json(plan.model_dump_json())
        if plan.target != self.target or plan.manifest_hash != confirmed_manifest_hash:
            raise DeploymentConflict("Registration requires the exact target/deployer and reviewed plan hash")
        try:
            with self.db.transaction():
                server, database_id, now = self._identity()
                if (server, database_id) != (plan.server_identity, plan.database_id):
                    raise DeploymentConflict("Prepared SQL deployment changed")
                locked = self.db.query(f"""/* connector-bootstrap:lock-control */
SELECT singleton FROM {CONTROL} WITH (UPDLOCK,HOLDLOCK) WHERE singleton=1
""")
                if locked != [(1,)]:
                    raise DeploymentError("Existing monitoring control is required; no schema or baseline was installed")
                original = self._receipt(plan)
                if original is not None:
                    return BootstrapResult(receipt=original, replayed=True)
                if not plan.prepared_at <= now < plan.expires_at:
                    raise DeploymentConflict("Reviewed connector capture expired; no write was attempted")
                if self._snapshot(plan.capture) != (plan.control_sha256, plan.prior_connector_sha256):
                    raise DeploymentConflict("Control or connector changed after preparation")
                current_server, current_database, now = self._identity()
                if (current_server, current_database) != (server, database_id) or not plan.prepared_at <= now < plan.expires_at:
                    raise DeploymentConflict("Reviewed deployment or capture freshness changed while acquiring SQL fences")
                # Never mutate redacted configuration into plausible topology.
                _nonsecret(plan.connector.model_dump(mode="json"))
                payload = canonical_json(plan.connector.model_dump(mode="json"))
                expected = plan.capture.expected
                key = (expected.tenant_id, expected.epoch, plan.capture.connector_id, _key(plan.capture.connector_id))
                if plan.prior_connector_sha256 is None:
                    changed = self.db.execute(f"""/* connector-bootstrap:insert */
INSERT INTO {RECORDS} (tenant_id,epoch,record_kind,full_key,key_hash,revision,status,payload)
SELECT ?,?,'connector',?,?,?,'planned',? WHERE NOT EXISTS (
 SELECT 1 FROM {RECORDS} WITH (UPDLOCK,HOLDLOCK)
 WHERE tenant_id=? AND epoch=? AND record_kind='connector' AND key_hash=?)
 AND SYSUTCDATETIME()<?
""", *key, plan.connector.revision, payload, expected.tenant_id, expected.epoch, key[3], plan.expires_at.replace(tzinfo=None))
                else:
                    changed = self.db.execute(f"""/* connector-bootstrap:assign */
UPDATE {RECORDS} SET revision=?,status='planned',payload=?
WHERE tenant_id=? AND epoch=? AND record_kind='connector' AND full_key=? AND key_hash=?
 AND revision=? AND HASHBYTES('SHA2_256',payload)=? AND SYSUTCDATETIME()<?
""", plan.connector.revision, payload, *key, plan.capture.expected_connector_revision,
                        bytes.fromhex(plan.prior_connector_sha256), plan.expires_at.replace(tzinfo=None))
                if changed != 1:
                    raise DeploymentConflict("Connector registration lost its conditional write")
                receipt = BootstrapReceipt(
                    request_id=plan.capture.request_id, manifest_hash=plan.manifest_hash,
                    capture_sha256=fingerprint(plan.capture.model_dump(mode="json")),
                    target=self.target, server_identity=server, database_id=database_id,
                    connector=plan.connector, recorded_at=now,
                )
                _nonsecret(receipt.model_dump(mode="json"))
                changed = self.db.execute(f"""/* connector-bootstrap:insert-receipt */
INSERT INTO {RECEIPTS} (tenant_id,epoch,operation,request_hash,request_id,fingerprint,recorded_at,payload)
VALUES (?,?,?,?,?,?,?,?)
""", expected.tenant_id, expected.epoch, OPERATION, _key(plan.capture.request_id),
                    plan.capture.request_id, plan.manifest_hash, now.replace(tzinfo=None),
                    canonical_json(receipt.model_dump(mode="json")))
                if changed != 1 or self._receipt(plan) != receipt:
                    raise DeploymentConflict("Immutable connector receipt was not confirmed")
        except (SqlCommitUncertain, SqlRollbackUncertain):
            try:
                saved = self.reconcile(plan)
            except (DeploymentError, SqlUnavailable, OSError) as read_error:
                raise DeploymentUncertain(
                    "Connector acknowledgement is uncertain; retain and reconcile the original plan",
                ) from read_error
            return saved.model_copy(update={"reconciled_uncertain_commit": True})
        return BootstrapResult(receipt=receipt)
