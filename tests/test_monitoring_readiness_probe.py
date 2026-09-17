from __future__ import annotations

import asyncio
import base64
import json
import time
from types import SimpleNamespace

import pytest
from test_hybrid_platform_probe import (
    CLIENT,
    IDENTITY,
    ITEM,
    JOB,
    OBJECT,
    TENANT,
    WORKSPACE,
)
from test_monitoring_transport_probe import environment

from scripts.hybrid_platform_probe import ProbeError
from scripts.monitoring_readiness_probe import (
    PROOF_NAMES,
    SCOPES,
    OwnedScope,
    ReadinessError,
    Reply,
    probe_readiness,
    projection_gap,
    routes,
)
from scripts.monitoring_transport_probe import TransportProbeInput, run_probe

SCOPE = OwnedScope(WORKSPACE, ITEM, JOB)
SECRET_TEXT = "DO_NOT_EMIT_NAMES_OR_USER_PAYLOAD"


class Credential:
    def __init__(self, *, wrong_service=None, **changes):
        self.changes = changes
        self.wrong_service = wrong_service
        self.requested = []

    async def get_token(self, scope):
        self.requested.append(scope)
        audience = scope.removesuffix("/.default")
        if self.wrong_service is not None and SCOPES[self.wrong_service] == scope:
            audience = "https://wrong-audience.invalid"
        expires = int(time.time()) + 3600
        claims = {
            "tid": TENANT,
            "appid": CLIENT,
            "oid": OBJECT,
            "xms_mirid": IDENTITY,
            "aud": audience,
            "exp": expires,
            **self.changes,
        }
        encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        return SimpleNamespace(token=f"fixture.{encoded}.fixture", expires_on=expires)

    async def close(self):
        pass


def responses():
    values = (
        {"id": ITEM, "workspaceId": WORKSPACE, "type": "DataPipeline", "displayName": SECRET_TEXT},
        {"value": [{"id": JOB, "itemId": ITEM, "failureReason": SECRET_TEXT}]},
        {"value": [{"id": 1, "serviceExceptionJson": SECRET_TEXT}]},
        {"id": WORKSPACE, "name": SECRET_TEXT, "users": [{"displayName": SECRET_TEXT}]},
        {
            "itemEntities": [
                {"id": ITEM, "workspaceId": WORKSPACE, "name": SECRET_TEXT},
                {
                    "id": JOB,
                    "workspaceId": WORKSPACE,
                    "creatorPrincipal": {"displayName": SECRET_TEXT},
                },
            ],
            "continuationToken": "not-emitted",
        },
    )
    return {
        route.url: Reply(200, value) for route, value in zip(routes(SCOPE), values, strict=True)
    }


async def test_get_only_owned_readiness_emits_independent_safe_proofs():
    replies = responses()
    calls = []
    credential = Credential()

    async def get(url, token):
        calls.append(url)
        assert token.startswith("fixture.")
        return replies[url]

    report = await probe_readiness(environment(), SCOPE, credential=credential, get=get)
    assert all(report["proofs"].values())
    assert set(credential.requested) == set(SCOPES.values())
    assert len(calls) == 5
    assert f"/admin/groups/{WORKSPACE}" in calls[3]
    assert calls[4].endswith(f"/admin/items?workspaceId={WORKSPACE}")
    assert calls[2].endswith(f"/datasets/{JOB}/refreshes?$top=1")
    assert report["inventory_complete"] is False
    assert report["remediation_authorized"] is False
    assert report["transport_acceptance_affected"] is False
    assert report["checks"][4]["has_continuation"] is True
    assert report["checks"][4]["owned_model_seen"] is True
    assert SECRET_TEXT not in json.dumps(report)
    assert "not-emitted" not in json.dumps(report)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 501])
async def test_preview_inventory_failure_is_an_independent_capability_gap(status):
    replies = responses()
    replies[routes(SCOPE)[4].url] = Reply(status, {"message": SECRET_TEXT})

    async def get(url, token):
        return replies[url]

    report = await probe_readiness(environment(), SCOPE, credential=Credential(), get=get)
    assert report["proofs"][PROOF_NAMES[4]] is False
    assert all(report["proofs"][name] for name in PROOF_NAMES[:4])
    assert report["checks"][4]["gap"] == f"http_{status}"
    assert SECRET_TEXT not in json.dumps(report)


async def test_token_audiences_are_not_interchangeable_between_services():
    called = []

    async def get(url, token):
        called.append(url)
        return responses()[url]

    report = await probe_readiness(
        environment(),
        SCOPE,
        credential=Credential(wrong_service="powerbi"),
        get=get,
    )
    assert report["proofs"][PROOF_NAMES[0]] is True
    assert report["proofs"][PROOF_NAMES[1]] is True
    assert report["proofs"][PROOF_NAMES[4]] is True
    assert report["proofs"][PROOF_NAMES[2]] is False
    assert report["proofs"][PROOF_NAMES[3]] is False
    assert all("api.powerbi.com" not in url for url in called)


@pytest.mark.parametrize(
    "changes", [{"tid": ITEM}, {"oid": ITEM}, {"appid": ITEM}, {"xms_mirid": "wrong"}]
)
async def test_wrong_managed_identity_never_reaches_readiness_apis(changes, caplog):
    async def no_http(*args):
        raise AssertionError("Wrong identity must not reach any endpoint")

    report = await probe_readiness(
        environment(), SCOPE, credential=Credential(**changes), get=no_http
    )
    assert not any(report["proofs"].values())
    assert all(
        check["gap"] == "managed_identity_token_binding_failed" for check in report["checks"]
    )
    assert "fixture." not in caplog.text


@pytest.mark.parametrize(
    ("index", "body"),
    [
        (0, {"id": JOB, "type": "DataPipeline"}),
        (1, {"value": [{"itemId": JOB}]}),
        (2, {"value": None}),
        (3, {"id": ITEM}),
        (4, {"itemEntities": [{"id": ITEM, "workspaceId": JOB}]}),
    ],
)
async def test_wrong_scope_or_response_shape_does_not_prove_access(index, body):
    replies = responses()
    replies[routes(SCOPE)[index].url] = Reply(200, body)

    async def get(url, token):
        return replies[url]

    report = await probe_readiness(environment(), SCOPE, credential=Credential(), get=get)
    assert report["proofs"][PROOF_NAMES[index]] is False
    assert "gap" in report["checks"][index]


async def test_readiness_deadline_becomes_visible_gaps_without_an_exception():
    async def stalled(url, token):
        await asyncio.Event().wait()

    report = await probe_readiness(
        environment(),
        SCOPE,
        credential=Credential(),
        get=stalled,
        deadline_seconds=0.01,
    )
    assert not any(report["proofs"].values())
    assert any(check["gap"] == "readiness_deadline_exhausted" for check in report["checks"])


def test_readiness_input_accepts_only_owned_ids_not_urls_or_extra_commands():
    with pytest.raises(ReadinessError):
        OwnedScope.parse(
            {
                "workspaceId": WORKSPACE,
                "pipelineId": ITEM,
                "modelId": JOB,
                "url": "https://example.invalid",
            }
        )
    with pytest.raises(ReadinessError):
        OwnedScope("../other-workspace", ITEM, JOB)
    value = json.loads(environment()["MONITORING_TRANSPORT_PROBE_INPUT"])
    value["readinessModelId"] = JOB
    assert TransportProbeInput.parse(json.dumps(value)).readiness_model_id == JOB


async def test_readiness_is_disabled_by_default():
    calls = []

    async def receiver(*args, **kwargs):
        return 1

    async def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError("Readiness must be explicitly enabled")

    assert await run_probe(environment(), receiver=receiver, readiness=forbidden) == 1
    assert calls == []


@pytest.mark.parametrize("raises", [False, True])
async def test_readiness_gap_never_changes_successful_transport_acceptance(raises, capsys):
    values = environment()
    spec = json.loads(values["MONITORING_TRANSPORT_PROBE_INPUT"])
    spec["readinessModelId"] = JOB
    values["MONITORING_TRANSPORT_PROBE_INPUT"] = json.dumps(spec)

    async def receiver(*args, **kwargs):
        return 1

    async def readiness(env, scope):
        assert scope == SCOPE
        if raises:
            raise RuntimeError(SECRET_TEXT)
        return projection_gap("preview_api_unsupported")

    assert await run_probe(values, receiver=receiver, readiness=readiness) == 1
    output = capsys.readouterr().out
    assert SECRET_TEXT not in output
    projections = [json.loads(line) for line in output.splitlines()]
    assert projections[-1]["stage"] == "readiness_projection"
    assert projections[-1]["transport_acceptance_affected"] is False


async def test_successful_readiness_does_not_rescue_failed_transport():
    values = environment()
    spec = json.loads(values["MONITORING_TRANSPORT_PROBE_INPUT"])
    spec["readinessModelId"] = JOB
    values["MONITORING_TRANSPORT_PROBE_INPUT"] = json.dumps(spec)

    async def receiver(*args, **kwargs):
        raise ProbeError("No owned receipt")

    async def readiness(*args):
        result = projection_gap("not_checked")
        result["proofs"] = dict.fromkeys(PROOF_NAMES, True)
        return result

    with pytest.raises(ProbeError, match="No owned receipt"):
        await run_probe(values, receiver=receiver, readiness=readiness)
