"""Finite UAMI-only Eventstream bootstrap, without worker endpoint prerequisites.

The deployment owner journals and serializes each intent before starting a job.
This helper submits at most one create request per execution and never retries
an uncertain POST. Use inspect or resume after an ambiguous result. Job retry
must be zero: the client request ID is correlation, not server idempotency.

Only public helper code and a bounded nonsecret input belong in the job bundle.
No private journal, SQL settings, source job execution, or key-returning
destination connection API is used. Success proves item ownership and definition
round-trip, not endpoint metadata, event delivery or full-worker readiness.

https://learn.microsoft.com/rest/api/fabric/eventstream/items/create-eventstream
https://learn.microsoft.com/rest/api/fabric/eventstream/items/get-eventstream-definition
https://learn.microsoft.com/rest/api/fabric/articles/long-running-operation
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from scripts.hybrid_platform_probe import assert_identity, eventstream_body

FABRIC_ROOT = "https://api.fabric.microsoft.com/v1"
MAX_RESPONSE_BYTES = 1_048_576


class CanaryError(RuntimeError):
    """Only a bounded machine code; never an HTTP body or credential."""


class CanaryPending(CanaryError):
    """A receipt exists but completion remains unverified."""


def canonical_uuid(value: object) -> str:
    try:
        if not isinstance(value, str) or UUID(value).int == 0:
            raise ValueError
        return str(UUID(value))
    except ValueError:
        raise CanaryError("invalid_uuid") from None


@dataclass(frozen=True)
class CanarySpec:
    action: str
    intent_id: str
    workspace_id: str
    pipeline_id: str
    timeout_seconds: int
    operation_id: str | None = None

    @classmethod
    def parse(cls, text: str) -> CanarySpec:
        if len(text.encode("utf-8")) > 4096:
            raise CanaryError("input_too_large")
        try:
            value = json.loads(text)
        except (ValueError, UnicodeError):
            raise CanaryError("invalid_input_json") from None
        required = {"action", "intentId", "workspaceId", "pipelineId", "timeoutSeconds"}
        if not isinstance(value, dict) or set(value) not in (required, required | {"operationId"}):
            raise CanaryError("unexpected_input_fields")
        if value["action"] not in {"create", "inspect", "resume"}:
            raise CanaryError("unsupported_canary_action")
        seconds = value["timeoutSeconds"]
        if type(seconds) is not int or not 30 <= seconds <= 480:
            raise CanaryError("invalid_deadline")
        operation = value.get("operationId") or None
        if (value["action"] == "resume") != (operation is not None):
            raise CanaryError("resume_requires_operation_id")
        return cls(
            value["action"],
            canonical_uuid(value["intentId"]),
            canonical_uuid(value["workspaceId"]),
            canonical_uuid(value["pipelineId"]),
            seconds,
            canonical_uuid(operation) if operation else None,
        )

    @property
    def name(self) -> str:
        return f"hybrid-canary-events-{self.intent_id.replace('-', '')}"

    def request_body(self) -> dict:
        value = eventstream_body(self.name, self.workspace_id, self.pipeline_id)
        definition_hash = hashlib.sha256(
            json.dumps(value["definition"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        value["description"] = (
            f"Owned monitoring bootstrap; intent={self.intent_id}; definition={definition_hash}"
        )
        return value


@dataclass(frozen=True)
class Reply:
    status: int
    headers: dict[str, str]
    body: dict


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise CanaryError("unexpected_http_redirect")


class FabricClient:
    """Fixed public Fabric origin; no redirects, key endpoints or command retries."""

    def __init__(self, spec: CanarySpec, token: str, *, clock=time.monotonic) -> None:
        self.spec = spec
        self._token = token
        self._clock = clock
        self.deadline = clock() + spec.timeout_seconds
        self._opener = urllib.request.build_opener(NoRedirect())

    def request(self, method: str, path: str, body: dict | None = None) -> Reply:
        url = FABRIC_ROOT + path
        parsed = urllib.parse.urlsplit(url)
        root = f"/v1/workspaces/{self.spec.workspace_id}"
        uuid = r"[0-9a-f-]{36}"
        allowed_get = (
            parsed.path == root
            or parsed.path == f"{root}/items/{self.spec.pipeline_id}"
            or parsed.path == f"{root}/eventstreams"
            or re.fullmatch(rf"{re.escape(root)}/eventstreams/{uuid}", parsed.path)
            or re.fullmatch(rf"/v1/operations/{uuid}(?:/result)?", parsed.path)
        )
        allowed_post = parsed.path == f"{root}/eventstreams" or re.fullmatch(
            rf"{re.escape(root)}/eventstreams/{uuid}/getDefinition", parsed.path
        )
        query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "api.fabric.microsoft.com"
            or parsed.fragment
            or set(query) - {"continuationToken", "format"}
            or not ((method == "GET" and allowed_get) or (method == "POST" and allowed_post))
        ):
            raise CanaryError("request_outside_canary_allowlist")
        remaining = self.deadline - self._clock()
        if remaining <= 0:
            raise CanaryPending("deadline_exhausted")
        payload = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        request = urllib.request.Request(
            url,
            data=payload,
            method=method,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "x-ms-client-request-id": self.spec.intent_id,
            },
        )
        try:
            with self._opener.open(request, timeout=min(30, remaining)) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise CanaryError("response_too_large")
                try:
                    value = json.loads(raw) if raw else {}
                except (ValueError, UnicodeError):
                    raise CanaryError("invalid_response_json") from None
                # Creation/LRO acknowledgements have no required JSON body.
                # Fabric can encode that absence as JSON null instead of no bytes.
                if value is None and response.status in {201, 202}:
                    value = {}
                if not isinstance(value, dict):
                    raise CanaryError("invalid_response_shape")
                return Reply(
                    response.status,
                    {key.lower(): value for key, value in response.headers.items()},
                    value,
                )
        except urllib.error.HTTPError as exc:
            # Do not read or print the error body; it can include tenant data.
            raise CanaryError(f"http_{exc.code}") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise CanaryError("http_outcome_unknown") from None


def managed_identity_token(environment: dict[str, str], *, run=subprocess.run) -> tuple[str, dict]:
    expected = {
        "tenant_id": canonical_uuid(environment.get("AZURE_TENANT_ID")),
        "client_id": canonical_uuid(environment.get("AZURE_CLIENT_ID")),
        "object_id": canonical_uuid(environment.get("MONITORING_IDENTITY_OBJECT_ID")),
        "subscription_id": canonical_uuid(environment.get("AZURE_SUBSCRIPTION_ID")),
        "identity_resource_id": environment.get("MONITORING_IDENTITY_RESOURCE_ID", ""),
    }
    with tempfile.TemporaryDirectory(prefix="monitoring-canary-az-") as directory:
        # This profile lives only in this job. Never use or copy an operator cache.
        cli_environment = {**environment, "AZURE_CONFIG_DIR": directory}
        commands = (
            [
                "az",
                "login",
                "--identity",
                "--client-id",
                expected["client_id"],
                "--allow-no-subscriptions",
                "--output",
                "none",
                "--only-show-errors",
            ],
            [
                "az",
                "account",
                "get-access-token",
                "--resource",
                "https://api.fabric.microsoft.com",
                "--output",
                "json",
                "--only-show-errors",
            ],
        )
        result = None
        for command in commands:
            try:
                result = run(
                    command,
                    env=cli_environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                raise CanaryError("managed_identity_cli_unavailable") from None
            if result.returncode != 0:
                raise CanaryError("managed_identity_authentication_failed")
        try:
            document = json.loads(result.stdout)
            token = document["accessToken"]
            if not isinstance(token, str) or len(token) > 32_768:
                raise ValueError
            assert_identity(token, **expected)
            part = token.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
            if claims.get("aud", "").rstrip("/") != "https://api.fabric.microsoft.com":
                raise ValueError
            if type(claims.get("exp")) is not int or claims["exp"] <= time.time() + 600:
                raise ValueError
        except (ValueError, KeyError, TypeError, IndexError):
            raise CanaryError("managed_identity_binding_failed") from None
        return token, expected


class Canary:
    def __init__(
        self,
        spec: CanarySpec,
        client,
        emit: Callable[[dict], None],
        *,
        clock=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        self.spec = spec
        self.client = client
        self.emit = emit
        self.clock = clock
        self.sleep = sleep
        self.deadline = clock() + spec.timeout_seconds
        self.expected = spec.request_body()
        self.created_in_this_execution = False

    def receipt(self, stage: str, **metadata) -> None:
        self.emit(
            {
                "stage": stage,
                "intent_id": self.spec.intent_id,
                **metadata,
                "create_submitted_by_this_execution": self.created_in_this_execution,
                "worker_ready": False,
                "event_delivery_verified": False,
                "endpoint_metadata_retrieved": False,
                "sql_acceptance_proven": False,
            }
        )

    def complete(self, reply: Reply, stage: str) -> dict:
        if reply.status in {200, 201}:
            return reply.body
        if reply.status != 202:
            raise CanaryError("unexpected_operation_status")
        operation = canonical_uuid(reply.headers.get("x-ms-operation-id"))
        location = reply.headers.get("location")
        if location != f"{FABRIC_ROOT}/operations/{operation}":
            raise CanaryError("unverified_operation_location")
        self.receipt("operation_accepted", operation_id=operation, operation_stage=stage)
        return self.poll(operation, initial_headers=reply.headers)

    def poll(self, operation: str, *, initial_headers: dict | None = None) -> dict:
        operation = canonical_uuid(operation)
        headers = {} if initial_headers is None else initial_headers
        for _ in range(100):
            try:
                delay = max(1, int(headers.get("retry-after", "2")))
            except (ValueError, TypeError):
                raise CanaryError("invalid_retry_after") from None
            if delay > self.deadline - self.clock():
                self.receipt("operation_pending", operation_id=operation)
                raise CanaryPending("operation_pending")
            self.sleep(delay)
            reply = self.client.request("GET", f"/operations/{operation}")
            if reply.status != 200:
                raise CanaryError("operation_state_unverified")
            state = reply.body.get("status")
            if state == "Succeeded":
                result = self.client.request("GET", f"/operations/{operation}/result")
                if result.status != 200:
                    raise CanaryError("operation_result_unverified")
                return result.body
            if state == "Failed":
                raise CanaryError("fabric_operation_failed")
            if state not in {"NotStarted", "Running"}:
                raise CanaryError("unknown_operation_state")
            headers = reply.headers
        raise CanaryPending("operation_poll_budget_exhausted")

    def find_owned(self) -> dict | None:
        path = f"/workspaces/{self.spec.workspace_id}/eventstreams"
        cursor = None
        seen = set()
        found = []
        for _ in range(50):
            query = (
                ""
                if cursor is None
                else "?" + urllib.parse.urlencode({"continuationToken": cursor})
            )
            reply = self.client.request("GET", path + query)
            if reply.status != 200 or not isinstance(reply.body.get("value"), list):
                raise CanaryError("eventstream_inventory_incomplete")
            for item in reply.body["value"]:
                if not isinstance(item, dict):
                    raise CanaryError("invalid_eventstream_inventory")
                if item.get("displayName") == self.spec.name:
                    found.append(item)
            cursor = reply.body.get("continuationToken")
            if reply.body.get("continuationUri") and cursor is None:
                raise CanaryError("inventory_continuation_unverified")
            if cursor is None:
                if len(found) > 1:
                    raise CanaryError("ambiguous_canary_ownership")
                return found[0] if found else None
            if not isinstance(cursor, str) or not cursor or len(cursor) > 4096 or cursor in seen:
                raise CanaryError("invalid_inventory_continuation")
            seen.add(cursor)
        raise CanaryError("eventstream_inventory_budget_exhausted")

    def verify_item(self, value: dict) -> str:
        item_id = canonical_uuid(value.get("id"))
        reply = self.client.request(
            "GET", f"/workspaces/{self.spec.workspace_id}/eventstreams/{item_id}"
        )
        item = reply.body
        if reply.status != 200 or (
            canonical_uuid(item.get("id")) != item_id
            or canonical_uuid(item.get("workspaceId")) != self.spec.workspace_id
            or item.get("type") != "Eventstream"
            or item.get("displayName") != self.spec.name
            or item.get("description") != self.expected["description"]
        ):
            raise CanaryError("owned_eventstream_binding_failed")
        self.receipt("ownership_verified", eventstream_id=item_id)
        return item_id

    @staticmethod
    def definition_parts(value: dict) -> dict:
        try:
            parts = value["definition"]["parts"]
            if not isinstance(parts, list) or len(parts) > 10:
                raise ValueError
            result = {}
            for part in parts:
                if part["path"] not in {"eventstream.json", "eventstreamProperties.json"}:
                    continue
                if part["path"] in result or part["payloadType"] != "InlineBase64":
                    raise ValueError
                raw = base64.b64decode(part["payload"], validate=True)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise ValueError
                result[part["path"]] = json.loads(raw)
            if set(result) != {"eventstream.json", "eventstreamProperties.json"}:
                raise ValueError
            return result
        except (ValueError, KeyError, TypeError, RecursionError):
            raise CanaryError("definition_unverified") from None

    @staticmethod
    def contains_expected(actual, expected) -> bool:
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(
                key in actual and Canary.contains_expected(actual[key], value)
                for key, value in expected.items()
            )
        if isinstance(expected, list):
            if not isinstance(actual, list) or len(actual) != len(expected):
                return False
            if all(isinstance(value, str) for value in (*actual, *expected)):
                return sorted(actual) == sorted(expected)
            return all(
                Canary.contains_expected(left, right)
                for left, right in zip(actual, expected, strict=True)
            )
        return type(actual) is type(expected) and actual == expected

    def run(self) -> dict:
        workspace = self.client.request("GET", f"/workspaces/{self.spec.workspace_id}")
        pipeline = self.client.request(
            "GET", f"/workspaces/{self.spec.workspace_id}/items/{self.spec.pipeline_id}"
        )
        if (
            workspace.status != 200
            or canonical_uuid(workspace.body.get("id")) != self.spec.workspace_id
            or pipeline.status != 200
            or canonical_uuid(pipeline.body.get("id")) != self.spec.pipeline_id
            or pipeline.body.get("type") != "DataPipeline"
        ):
            raise CanaryError("canary_scope_unverified")
        if self.spec.action == "resume":
            value = self.poll(self.spec.operation_id)
        else:
            value = self.find_owned()
            if value is None:
                if self.spec.action == "inspect":
                    raise CanaryPending("owned_eventstream_not_observed")
                self.receipt(
                    "create_intent",
                    request_sha256=hashlib.sha256(
                        json.dumps(self.expected, sort_keys=True, separators=(",", ":")).encode()
                    ).hexdigest(),
                )
                try:
                    self.created_in_this_execution = True
                    reply = self.client.request(
                        "POST", f"/workspaces/{self.spec.workspace_id}/eventstreams", self.expected
                    )
                except CanaryError:
                    self.receipt("create_outcome_unverified")
                    raise
                value = self.complete(reply, "create")
        if not value.get("id"):
            value = self.find_owned()
            if value is None:
                self.receipt("ownership_pending")
                raise CanaryPending("accepted_creation_not_yet_observed")
        item_id = self.verify_item(value)
        response = self.client.request(
            "POST",
            f"/workspaces/{self.spec.workspace_id}/eventstreams/{item_id}/getDefinition?format=eventstream",
        )
        definition = self.definition_parts(self.complete(response, "definition"))
        expected = self.definition_parts({"definition": self.expected["definition"]})
        if not self.contains_expected(definition, expected):
            raise CanaryError("definition_round_trip_mismatch")
        result = {
            "eventstream_id": item_id,
            "definition_verified": True,
            "ownership_verified": True,
            "creation_identity_proven_by_this_execution": self.created_in_this_execution,
        }
        self.receipt("canary_complete", **result)
        return result


def main() -> int:
    def emit(value):
        print(json.dumps(value, separators=(",", ":")), flush=True)

    try:
        spec = CanarySpec.parse(os.environ.get("MONITORING_CANARY_INPUT", ""))
        token, identity = managed_identity_token(dict(os.environ))
        emit({"stage": "managed_identity_verified", **identity})
        Canary(spec, FabricClient(spec, token), emit).run()
        return 0
    except CanaryPending as exc:
        emit({"stage": "pending", "code": str(exc), "worker_ready": False})
        return 4
    except CanaryError as exc:
        emit({"stage": "blocked", "code": str(exc), "worker_ready": False})
        return 2
    except Exception as exc:
        emit(
            {
                "stage": "blocked",
                "code": "unexpected_failure",
                "error_class": type(exc).__name__,
                "worker_ready": False,
            }
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
