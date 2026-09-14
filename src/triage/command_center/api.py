"""ASGI entry point for the operator command center."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from triage.command_center.access_models import (
    AccessCurrentUser,
    AccessResponse,
    VerifiedEntraIdentity,
)
from triage.command_center.auth import EntraTokenVerifier, TokenVerifier, require
from triage.command_center.incident_models import (
    IncidentDiscussionInput,
    IncidentNoteInput,
    IncidentResolutionInput,
)
from triage.command_center.models import (
    Actor,
    ApiFailure,
    AskInput,
    CommandInput,
    DecisionInput,
    ReconcileInput,
    WebSettings,
)
from triage.command_center.service import CommandCenterService, run_summary
from triage.settings import settings as core_settings

logger = logging.getLogger("triage.command_center.api")
ROOT = Path(__file__).resolve().parents[3]


class ValidationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: str = "mock"


async def authenticated_actor(
    request: Request, authorization: Annotated[str | None, Header()] = None,
) -> Actor:
    if request.app.state.web.mode == "demo":
        return Actor(
            id="00000000-0000-0000-0000-000000000002",
            display_name="Synthetic demo operator", roles=["admin", "reader"],
        )
    if not authorization or not authorization.startswith("Bearer "):
        raise ApiFailure(401, "unauthenticated", "Sign in with your organizational account.")
    identity = await asyncio.to_thread(request.app.state.verifier.verify, authorization[7:])
    if not isinstance(identity, Actor):
        raise ApiFailure(401, "unauthenticated", "The token verifier did not return a valid identity.")
    require(identity, "reader")
    return identity


ActorDependency = Annotated[Actor, Depends(authenticated_actor)]


def create_app(
    service: CommandCenterService | None = None, *, web_settings: WebSettings | None = None,
    token_verifier: TokenVerifier | None = None,
) -> FastAPI:
    web = web_settings or WebSettings()
    if web.mode == "demo" and any(os.getenv(name) for name in ("WEBSITE_SITE_NAME", "WEBSITE_INSTANCE_ID", "CONTAINER_APP_NAME")):
        raise RuntimeError("Demo mode cannot be enabled on an Azure-hosted command center")
    runtime = service or CommandCenterService(core_settings, web)
    verifier = token_verifier if token_verifier is not None else (
        EntraTokenVerifier(web) if web.mode == "live" else None
    )

    @asynccontextmanager
    async def lifespan(_app):
        if web.mode == "demo" and service is None:
            from triage.command_center.demo import start_demo

            await start_demo(runtime)
        try:
            yield
        finally:
            for task in runtime.demo_tasks:
                task.cancel()
            if runtime.demo_tasks:
                await asyncio.gather(*runtime.demo_tasks, return_exceptions=True)
            temporary = getattr(runtime, "demo_temporary", None)
            if temporary is not None:
                temporary.cleanup()

    app = FastAPI(title="Triage command center", docs_url=None, redoc_url=None, lifespan=lifespan)
    app.state.service = runtime
    app.state.web = web
    app.state.verifier = verifier

    @app.middleware("http")
    async def request_boundary(request: Request, call_next):
        if web.mode == "demo":
            peer = request.client.host if request.client else ""
            if peer not in {"127.0.0.1", "::1", "testclient"}:
                return JSONResponse(status_code=403, content={
                    "code": "demo_local_only", "message": "Demo mode accepts local connections only.",
                })
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
            "img-src 'self' data: blob:; font-src 'self'; style-src 'self' 'unsafe-inline'; "
            "script-src 'self'; connect-src 'self' https://login.microsoftonline.com https://graph.microsoft.com; "
            "frame-src 'self' https://login.microsoftonline.com"
        )
        if request.url.path.startswith("/api"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(ApiFailure)
    async def api_failure(_request, exc: ApiFailure):
        return JSONResponse(status_code=exc.status, content={"code": exc.code, "message": exc.message})

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_request, _exc):
        return JSONResponse(status_code=422, content={"code": "invalid_request", "message": "The request does not match the expected fields or values."})

    @app.exception_handler(Exception)
    async def backend_failure(_request, exc):
        logger.exception("Command-center request failed", exc_info=exc)
        return JSONResponse(status_code=503, content={
            "code": "service_unavailable",
            "message": "The command-center service could not complete this request. No success is being reported.",
        })

    @app.get("/api/config")
    async def config():
        return {
            "app_name": "Triage command center", "mode": web.mode,
            "auth": {
                "enabled": web.mode == "live", "tenant_id": web.tenant_id,
                "client_id": web.client_id, "scope": web.scope or f"api://{web.client_id}/access_as_user",
                "authorization_source": "entra_app_roles" if web.mode == "live" else "synthetic_demo",
            },
        }

    @app.get("/api/health")
    async def health():
        return {"status": "ready", "mode": web.mode}

    @app.get("/api/snapshot")
    async def snapshot(user: ActorDependency):
        return await asyncio.to_thread(runtime.snapshot, user)

    @app.get("/api/detail")
    async def detail(user: ActorDependency, kind: str, id: Annotated[str, Query(max_length=250)]):
        if kind == "incident":
            case = await asyncio.to_thread(runtime.incident_workflow.case, id, user)
            return case.detail
        return await asyncio.to_thread(runtime.detail, kind, id, user)

    @app.get("/api/incidents")
    async def incident_list(
        user: ActorDependency, limit: int = Query(25, ge=1, le=100),
        offset: int = Query(0, ge=0, le=2_147_483_647),
        query: str = Query("", max_length=200),
        status: str = "all", workload: str = "all",
    ):
        return await asyncio.to_thread(
            runtime.incident_workflow.list_incidents, user, limit=limit, offset=offset,
            query=query, status=status, workload=workload,
        )

    @app.get("/api/incidents/{incident_id}")
    async def incident_case(incident_id: str, user: ActorDependency):
        return await asyncio.to_thread(runtime.incident_workflow.case, incident_id, user)

    @app.post("/api/incidents/{incident_id}/notes")
    async def incident_note(incident_id: str, value: IncidentNoteInput, user: ActorDependency):
        return await asyncio.to_thread(runtime.incident_workflow.add_note, incident_id, value, user)

    @app.post("/api/incidents/{incident_id}/resolution")
    async def incident_resolution(incident_id: str, value: IncidentResolutionInput, user: ActorDependency):
        return await asyncio.to_thread(runtime.incident_workflow.resolve, incident_id, value, user)

    @app.post("/api/incidents/{incident_id}/discussion")
    async def incident_discussion(incident_id: str, value: IncidentDiscussionInput, user: ActorDependency):
        return await runtime.incident_workflow.discuss(incident_id, value, user)

    @app.get("/api/runs")
    async def runs(user: ActorDependency, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
        require(user, "reader")
        rows = await asyncio.to_thread(runtime.history.list_runs, limit=limit, offset=offset)
        total = await asyncio.to_thread(runtime.history.count_runs)
        return {"items": [run_summary(row) for row in rows], "total": total}

    @app.get("/api/runs/{run_id}")
    async def run_detail(run_id: str, user: ActorDependency):
        require(user, "reader")
        record = await asyncio.to_thread(runtime.history.get_run, run_id)
        if record is None:
            raise ApiFailure(404, "not_found", "Run not found.")
        events = await asyncio.to_thread(runtime.history.events, run_id)
        return {
            "run": run_summary(record),
            "events": [event.model_dump(mode="json") for event in events],
            "result": record.result.model_dump(mode="json") if record.result else None,
        }

    @app.get("/api/knowledge")
    async def knowledge(user: ActorDependency):
        require(user, "reader")
        return runtime.knowledge()

    @app.get("/api/access", response_model=AccessResponse)
    async def access(user: ActorDependency):
        require(user, "reader")
        identity = user if web.mode == "live" and isinstance(user, VerifiedEntraIdentity) else None
        return AccessResponse(
            source="entra_app_roles" if web.mode == "live" else "synthetic_demo",
            current_user=AccessCurrentUser(
                id=user.id, display_name=user.display_name, roles=user.roles,
            ),
            tenant_id=identity.tenant_id if identity else None,
            application_id=identity.application_id if identity else None,
            token_issued_at=identity.token_issued_at if identity else None,
            token_expires_at=identity.token_expires_at if identity else None,
        )

    @app.api_route("/api/admin/users", methods=["GET", "POST"])
    @app.get("/api/admin/audit")
    async def retired_access_management(user: ActorDependency):
        require(user, "reader")
        raise ApiFailure(
            410, "managed_in_entra",
            "Application permissions are managed in Microsoft Entra. "
            "Use /api/access to view the effective roles in your access token.",
        )

    @app.post("/api/decisions")
    async def decide(value: DecisionInput, user: ActorDependency):
        return await asyncio.to_thread(runtime.decide, value, user)

    @app.post("/api/commands")
    async def enqueue(value: CommandInput, user: ActorDependency):
        return await asyncio.to_thread(runtime.enqueue, value, user)

    @app.post("/api/commands/{command_id}/reconcile")
    async def reconcile(command_id: str, value: ReconcileInput, user: ActorDependency):
        return await asyncio.to_thread(runtime.reconcile, command_id, value.reason, user)

    @app.post("/api/ask")
    async def ask(value: AskInput, user: ActorDependency):
        return await runtime.ask(value, user)

    @app.get("/api/validation/scenarios")
    async def scenarios(user: ActorDependency):
        from triage.command_center.validation import scenario_catalog

        require(user, "admin")
        return {
            "items": [{"name": item.name, "title": item.title, "description": item.description} for item in scenario_catalog(ROOT)],
            "providers": ["mock"] if web.mode == "demo" else ["mock", "foundry"],
        }

    @app.get("/api/validation/results")
    async def results(user: ActorDependency):
        from triage.command_center.validation import validation_results

        require(user, "admin")
        return await asyncio.to_thread(validation_results, runtime)

    @app.post("/api/validation/scenarios/{name}")
    async def validate(name: str, value: ValidationInput, user: ActorDependency):
        from triage.command_center.validation import validate_scenario

        return await validate_scenario(runtime, ROOT, name, value.provider, user)

    static_dir = Path(web.static_dir) if web.static_dir else ROOT / "command-center" / "dist"
    if static_dir.is_dir():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="command-center")
    else:
        @app.get("/")
        async def not_built():
            return JSONResponse(status_code=503, content={"code": "frontend_not_built", "message": "Build command-center with npm run build before opening the application."})
    return app
