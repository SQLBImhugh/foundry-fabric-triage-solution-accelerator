from __future__ import annotations

import ast
import asyncio
import importlib.util
import json
import logging
import os
import socket
import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from triage import observability


@pytest.fixture(autouse=True)
async def isolated_telemetry(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Telemetry tests must never use live network or exporter transports")

    monkeypatch.delenv("APPLICATIONINSIGHTS_CONNECTION_STRING", raising=False)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(observability, "_diagnostics", observability._TelemetryDiagnostics())
    names = ("triage.telemetry.health", "azure.monitor.opentelemetry", "opentelemetry.sdk")
    saved = {
        name: (list(logging.getLogger(name).handlers), logging.getLogger(name).level,
               logging.getLogger(name).propagate)
        for name in names
    }
    yield
    for name, (handlers, level, propagate) in saved.items():
        logger = logging.getLogger(name)
        for handler in logger.handlers:
            if handler not in handlers:
                handler.close()
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate


@pytest.fixture
def sdk(monkeypatch):
    calls, credentials = [], []

    def credential(**kwargs):
        credentials.append(kwargs)
        return SimpleNamespace(identity="hosted-only")

    for name in ("azure", "azure.monitor", "azure.monitor.opentelemetry", "azure.identity"):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    monkeypatch.setattr(
        sys.modules["azure.monitor.opentelemetry"], "configure_azure_monitor",
        lambda **kwargs: calls.append(kwargs), raising=False,
    )
    monkeypatch.setattr(sys.modules["azure.identity"], "ManagedIdentityCredential", credential, raising=False)
    return calls, credentials


@pytest.fixture(
    params=[
        None, "",
        "InstrumentationKey=22222222-2222-4222-8222-222222222222;IngestionEndpoint=https://platform.invalid/",
        "private-malformed-locator", " ",
    ],
    ids=["absent", "empty", "valid", "malformed_nonempty", "whitespace_nonempty"],
)
def platform_locator(request, monkeypatch):
    if request.param is not None:
        monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", request.param)
    return request.param


def test_disabled_telemetry_does_not_construct_sdk_or_credential(sdk, platform_locator):
    calls, credentials = sdk
    assert not observability.configure_telemetry("", hosted=True)
    assert not calls and not credentials
    assert os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") == platform_locator
    assert observability.telemetry_status() == {
        "configuration": "disabled", "export_failures": 0, "export_warnings": 0,
        "last_error_type": "", "ingestion": "unverified",
        "configuration_error_file": "", "configuration_error_function": "", "configuration_error_line": 0,
    }


@pytest.mark.parametrize("hosted", [False, True])
def test_only_hosted_exact_empty_platform_locator_is_normalized(
    sdk, platform_locator, hosted, monkeypatch, caplog,
):
    calls, credentials = sdk
    observed = []

    def configure(**kwargs):
        calls.append(kwargs)
        value = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
        observed.append(value)
        if value in ("", "private-malformed-locator", " "):
            raise ValueError("private-parser-error")

    monkeypatch.setattr(sys.modules["azure.monitor.opentelemetry"], "configure_azure_monitor", configure)
    normalized = hosted and platform_locator == ""
    expected = None if normalized else platform_locator
    succeeds = expected not in ("", "private-malformed-locator", " ")
    with caplog.at_level(logging.INFO):
        assert observability.configure_telemetry("private-app-owned-locator", hosted=hosted) is succeeds
    assert observed == [expected]
    assert os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") == expected
    assert calls[0]["connection_string"] == "private-app-owned-locator"
    assert credentials == ([{"client_id": None}] if hosted else [])
    assert ("Empty platform telemetry locator normalized" in caplog.text) is normalized
    assert observability.telemetry_status()["configuration"] == ("configured" if succeeds else "failed")
    assert observability.telemetry_status()["ingestion"] == "unverified"
    if not succeeds:
        assert "error_type=ValueError" in caplog.text
    assert "private-" not in caplog.text and "https://" not in caplog.text


@pytest.mark.parametrize("client_id", ["", "11111111-1111-4111-8111-111111111111"])
def test_hosted_configuration_pins_mi_and_exports_only_explicit_metadata(sdk, client_id):
    calls, credentials = sdk
    assert observability.configure_telemetry(
        "synthetic-not-a-real-connection", hosted=True, managed_identity_client_id=client_id,
    )
    assert credentials == [{"client_id": client_id or None}]
    assert calls[0]["credential"].identity == "hosted-only"
    assert calls[0]["logger_name"] == "triage.telemetry"
    assert calls[0]["logging_enabled"] is False
    assert calls[0]["disable_offline_storage"] is True
    assert not any(option["enabled"] for option in calls[0]["instrumentation_options"].values())
    assert observability.telemetry_status()["configuration"] == "configured"
    assert observability.telemetry_status()["ingestion"] == "unverified"
    assert logging.getLogger("azure.monitor.opentelemetry").level == logging.WARNING


def test_export_diagnostics_are_bounded_metadata_only_and_not_reexported(sdk, monkeypatch, capsys, caplog):
    elapsed = [0.0]
    monkeypatch.setattr(observability, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))
    observability.configure_telemetry("private-config", hosted=True)
    exporter = logging.getLogger("azure.monitor.opentelemetry.exporter.export._base")
    for _ in range(10):
        try:
            raise RuntimeError("private-token-response-text")
        except RuntimeError:
            exporter.exception("Cannot export %s", "private-config")
    first = capsys.readouterr().err
    assert first.count("telemetry_export_diagnostic") == 1
    assert "error_type=RuntimeError" in first
    assert "private-config" not in first + caplog.text
    assert "private-token-response-text" not in first + caplog.text
    assert "Traceback" not in first + caplog.text
    assert observability.telemetry_status()["export_failures"] == 10
    assert observability._health_logger.propagate is False
    assert "telemetry_export_diagnostic" not in caplog.text
    elapsed[0] = 60
    logging.getLogger("opentelemetry.sdk._shared_internal").warning("Queue %s full", "private-content")
    second = capsys.readouterr().err
    assert second.count("telemetry_export_diagnostic") == 1
    assert "failures=10 warnings=1" in second
    assert "private-content" not in second
    assert observability.telemetry_status()["ingestion"] == "unverified"


def test_configuration_failure_reports_type_not_exception_or_connection(sdk, monkeypatch, caplog):
    def failed(**kwargs):
        raise ValueError("private-connection-and-response-body")

    monkeypatch.setattr(sys.modules["azure.monitor.opentelemetry"], "configure_azure_monitor", failed)
    with caplog.at_level(logging.WARNING):
        assert observability.configure_telemetry("private-connection", hosted=True) is False
    assert "error_type=ValueError" in caplog.text
    assert "private-connection" not in caplog.text
    assert observability.telemetry_status()["configuration"] == "failed"
    assert observability.telemetry_status()["ingestion"] == "unverified"


@pytest.mark.parametrize("stage", ["credential", "exporter"])
def test_configuration_failure_records_only_last_frame_metadata(stage, sdk, monkeypatch, tmp_path, caplog):
    file = "managed_identity.py" if stage == "credential" else "configurations.py"
    source_path = tmp_path / "private-connection-directory" / file
    namespace = {}
    exec(compile(
        "def failed(**kwargs):\n"
        "    private_headers = {'Authorization': 'private-header-token'}\n"
        "    raise ValueError('private-connection-and-response')\n",
        str(source_path), "exec",
    ), namespace)
    target = "azure.identity" if stage == "credential" else "azure.monitor.opentelemetry"
    function = "ManagedIdentityCredential" if stage == "credential" else "configure_azure_monitor"
    monkeypatch.setattr(sys.modules[target], function, namespace["failed"])
    with caplog.at_level(logging.WARNING):
        assert not observability.configure_telemetry("private-connection", hosted=True)
    status = observability.telemetry_status()
    assert status["configuration"] == "failed"
    assert status["configuration_error_file"] == file
    assert status["configuration_error_function"] == "failed"
    assert status["configuration_error_line"] == 3
    assert f"source_file={file} source_function=failed source_line=3" in caplog.text
    assert "error_type=ValueError" in caplog.text
    assert "private-" not in caplog.text + json.dumps(status)
    assert str(tmp_path) not in caplog.text and "Traceback" not in caplog.text
    observability.configure_telemetry("", hosted=True)
    assert observability.telemetry_status()["configuration_error_file"] == ""
    assert observability.telemetry_status()["configuration_error_line"] == 0


def test_configuration_failure_rejects_unsafe_frame_names(sdk, monkeypatch, caplog):
    namespace = {}
    exec(compile(
        "def failed(**kwargs):\n    raise ValueError('private-message')\n",
        "connection=private-token", "exec",
    ), namespace)
    failed = namespace["failed"]
    failed.__code__ = failed.__code__.replace(co_name="header=private-token")
    monkeypatch.setattr(sys.modules["azure.monitor.opentelemetry"], "configure_azure_monitor", failed)
    with caplog.at_level(logging.WARNING):
        assert not observability.configure_telemetry("private-connection", hosted=True)
    status = observability.telemetry_status()
    assert status["configuration_error_file"] == status["configuration_error_function"] == "unavailable"
    assert status["configuration_error_line"] == 2
    assert "private-" not in caplog.text + json.dumps(status)


def test_configuration_diagnostic_remains_visible_after_configured_return(sdk, monkeypatch, capsys):
    def configured_with_export_failure(**kwargs):
        logging.getLogger("azure.monitor.opentelemetry.exporter.export._base").error(
            "Synthetic private server response",
        )

    monkeypatch.setattr(
        sys.modules["azure.monitor.opentelemetry"], "configure_azure_monitor", configured_with_export_failure,
    )
    assert observability.configure_telemetry("private-connection", hosted=True)
    assert observability.telemetry_status()["configuration"] == "configured"
    assert observability.telemetry_status()["export_failures"] == 1
    assert "Synthetic private server response" not in capsys.readouterr().err


@pytest.mark.parametrize("kind", ["heartbeat", "model", "tool"])
def test_spans_never_capture_exception_text_or_stack_trace(kind, monkeypatch):
    calls, attributes = [], {}

    @contextmanager
    def start(name, **kwargs):
        calls.append((name, kwargs))
        yield SimpleNamespace(set_attribute=lambda key, value: attributes.update({key: value}))

    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", True)
    monkeypatch.setattr(observability, "_tracer", SimpleNamespace(start_as_current_span=start))
    span = {
        "heartbeat": observability.heartbeat_span,
        "model": lambda: observability.gen_ai_span(provider="mock", model="fixture"),
        "tool": lambda: observability.tool_span("fixture"),
    }[kind]
    with pytest.raises(RuntimeError), span() as handle:
        handle.set("elapsed_ms", 7)
        raise RuntimeError("private-prompt-and-completion")
    assert calls[0][1] == {"record_exception": False, "set_status_on_exception": False}
    assert attributes["elapsed_ms"] == 7
    assert "private-prompt-and-completion" not in str(attributes)


def test_heartbeat_span_is_azure_free_when_otel_is_absent(monkeypatch):
    monkeypatch.setattr(observability, "_OTEL_AVAILABLE", False)
    monkeypatch.setattr(observability, "_tracer", None)
    with observability.heartbeat_span() as span:
        span.set("elapsed_ms", 1)


@pytest.fixture
def hosted_module(monkeypatch):
    framework = ModuleType("agent_framework")

    class Message:
        def __init__(self, role, contents):
            self.role = role
            self.text = "".join(contents)

    framework.Message = Message
    framework.BaseAgent = object
    framework.AgentResponse = lambda **kwargs: SimpleNamespace(**kwargs)
    framework.AgentResponseUpdate = object
    internals = ModuleType("agent_framework._agents")
    internals.ResponseStream = object
    hosting = ModuleType("agent_framework_foundry_hosting")
    hosting.ResponsesHostServer = object
    for module in (framework, internals, hosting):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    path = Path(__file__).resolve().parents[1] / "src" / "app.py"
    spec = importlib.util.spec_from_file_location("hosted_telemetry_under_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def startup_providers(hosted_module, monkeypatch):
    class TraceProvider:
        def __str__(self):
            pytest.fail("Diagnostics must not stringify providers")

        @property
        def _active_span_processor(self):
            pytest.fail("Diagnostics must not inspect private exporter internals")

    class MetricProvider:
        pass

    class LogProvider:
        pass

    root = ModuleType("opentelemetry")
    trace = ModuleType("opentelemetry.trace")
    metrics = ModuleType("opentelemetry.metrics")
    logs = ModuleType("opentelemetry._logs")
    trace.get_tracer_provider = lambda: TraceProvider()
    metrics.get_meter_provider = lambda: MetricProvider()
    logs.get_logger_provider = lambda: LogProvider()
    root.trace, root.metrics = trace, metrics
    for module in (root, trace, metrics, logs):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(hosted_module, "version", lambda package: "1.0.0b1")
    for name in ("OTEL_LOGS_EXPORTER", "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER"):
        monkeypatch.delenv(name, raising=False)
    return {"traces": "TraceProvider", "metrics": "MetricProvider", "logs": "LogProvider"}


@pytest.mark.parametrize("capture_value", [None, "true", "TRUE", "1", "false"])
def test_hosted_main_enforces_metadata_only_even_when_configuration_fails(
    hosted_module, startup_providers, monkeypatch, caplog, capture_value,
):
    app = hosted_module
    configured, hosts = [], []
    app_locator, platform_locator = "private-config", "private-platform-unused"
    capture_flag = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"
    monkeypatch.setenv(capture_flag, capture_value or "false")
    if capture_value is None:
        monkeypatch.delenv(capture_flag)
    monkeypatch.setattr(app, "settings", SimpleNamespace(
        triage_telemetry_connection_string=app_locator,
        applicationinsights_connection_string=platform_locator,
        azure_client_id="11111111-1111-4111-8111-111111111111",
        triage_provider_mode="foundry", triage_tool_mode="live", graph_mailbox="private-mailbox",
    ))
    def configure(*args, **kwargs):
        assert app.os.environ[capture_flag] == "false"
        configured.append((args, kwargs))
        return False

    def agent():
        assert app.os.environ[capture_flag] == "false"
        return "agent"

    monkeypatch.setattr(app, "configure_telemetry", configure)
    monkeypatch.setattr(app, "telemetry_status", lambda: {"configuration": "failed"})
    monkeypatch.setattr(app, "TriageControllerAgent", agent)
    order = []

    def server(agent, **kwargs):
        assert app.os.environ[capture_flag] == "false"
        assert app.os.environ.get(capture_flag, "true").lower() in ("false", "0")
        order.append("constructed")

        def run():
            assert app.os.environ[capture_flag] == "false"
            assert "hosted_telemetry_startup" in caplog.text
            order.append("run")
            hosts.append((agent, kwargs))

        return SimpleNamespace(run=run)

    snapshot = app._startup_telemetry_metadata

    def after_construction(configured):
        assert order == ["constructed"]
        return snapshot(configured)

    monkeypatch.setattr(app, "ResponsesHostServer", server)
    monkeypatch.setattr(app, "_startup_telemetry_metadata", after_construction)
    with caplog.at_level(logging.INFO):
        app.main()
    assert configured[0][0] == ("private-config",)
    assert configured[0][1] == {"hosted": True, "managed_identity_client_id": app.settings.azure_client_id}
    assert hosts == [("agent", {"history_source": "agent", "configure_observability": None})]
    assert order == ["constructed", "run"]
    assert "configuration=failed sdk_configured=False ingestion=unverified" in caplog.text
    assert all(value not in caplog.text for value in ("private-config", "private-platform-unused", "private-mailbox"))
    record = next(record for record in caplog.records if record.getMessage().startswith("hosted_telemetry_startup "))
    data = json.loads(record.getMessage().removeprefix("hosted_telemetry_startup "))
    assert data["provider_classes"] == startup_providers
    assert set(data["exporter_settings"].values()) == {"unset"}
    assert data["configuration"] == "failed" and data["ingestion"] == "unverified"


@pytest.mark.parametrize("hosted_value", ["", "private-hosted-only"])
def test_hosted_main_uses_only_application_owned_setting(
    hosted_module, startup_providers, sdk, monkeypatch, caplog, hosted_value,
):
    app = hosted_module
    calls, credentials = sdk
    platform_locator = "private-platform-only"
    monkeypatch.setenv("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "true")
    monkeypatch.setattr(app, "settings", SimpleNamespace(
        triage_telemetry_connection_string=hosted_value,
        applicationinsights_connection_string=platform_locator,
        azure_client_id="11111111-1111-4111-8111-111111111111",
        triage_provider_mode="foundry", triage_tool_mode="live", graph_mailbox="",
    ))
    monkeypatch.setattr(app, "TriageControllerAgent", lambda: "agent")
    hosts = []
    monkeypatch.setattr(app, "ResponsesHostServer", lambda agent, **kwargs: SimpleNamespace(
        run=lambda: hosts.append(kwargs),
    ))
    with caplog.at_level(logging.INFO):
        app.main()
    if hosted_value:
        assert len(calls) == len(credentials) == 1
        assert calls[0]["connection_string"] == hosted_value
        assert credentials == [{"client_id": app.settings.azure_client_id}]
        assert observability.telemetry_status()["configuration"] == "configured"
        assert "configuration=configured sdk_configured=True" in caplog.text
        assert hosted_value not in caplog.text
    else:
        assert not calls and not credentials
        assert observability.telemetry_status()["configuration"] == "disabled"
        assert "configuration=disabled sdk_configured=False" in caplog.text
    assert "private-platform-only" not in caplog.text
    assert hosts == [{"history_source": "agent", "configure_observability": None}]
    assert app.os.environ["OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"] == "false"


def test_hosted_telemetry_setting_has_independent_env_binding_and_hidden_repr(monkeypatch):
    from triage.settings import Settings

    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "private-cli-locator")
    monkeypatch.delenv("TRIAGE_TELEMETRY_CONNECTION_STRING", raising=False)
    missing = Settings(_env_file=None)
    assert missing.triage_telemetry_connection_string == ""
    assert missing.applicationinsights_connection_string == "private-cli-locator"
    monkeypatch.setenv("TRIAGE_TELEMETRY_CONNECTION_STRING", "private-hosted-locator")
    configured = Settings(_env_file=None)
    assert configured.triage_telemetry_connection_string == "private-hosted-locator"
    assert configured.applicationinsights_connection_string == "private-cli-locator"
    assert "private-cli-locator" not in repr(configured)
    assert "private-hosted-locator" not in repr(configured)


@pytest.mark.parametrize("offline", [False, True])
def test_cli_keeps_standard_telemetry_setting_and_offline_gate(offline):
    path = Path(__file__).resolve().parents[1] / "src" / "triage" / "cli.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    branches = [
        node for node in ast.walk(tree) if isinstance(node, ast.If) and any(
            isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
            and isinstance(statement.value.func, ast.Name)
            and statement.value.func.id == "configure_telemetry"
            for statement in node.body
        )
    ]
    assert len(branches) == 1
    calls = []
    cli_locator, hosted_locator = "standard-cli-locator", "hosted-only-locator"
    namespace = {
        "settings": SimpleNamespace(
            applicationinsights_connection_string=cli_locator,
            triage_telemetry_connection_string=hosted_locator,
        ),
        "offline_command": offline,
        "configure_telemetry": calls.append,
    }
    exec(compile(ast.Module(body=branches, type_ignores=[]), str(path), "exec"), namespace)
    assert calls == ([] if offline else ["standard-cli-locator"])


def test_startup_diagnostic_has_only_whitelisted_metadata(hosted_module, startup_providers, monkeypatch):
    app = hosted_module
    monkeypatch.setattr(app, "telemetry_status", lambda: {"configuration": "configured"})
    monkeypatch.setenv("OTEL_LOGS_EXPORTER", "console,otlp")
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", " NONE ")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "IngestionEndpoint=https://private.invalid;token=private-token")
    for name in ("APPLICATIONINSIGHTS_CONNECTION_STRING", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS"):
        monkeypatch.setenv(name, "private-endpoint-header-token")
    data = app._startup_telemetry_metadata(True)
    assert set(data) == {
        "configuration", "sdk_configured", "ingestion", "exporter_settings", "provider_classes", "package_versions",
    }
    assert data["exporter_settings"] == {
        "OTEL_LOGS_EXPORTER": "console,otlp", "OTEL_TRACES_EXPORTER": " NONE ",
        "OTEL_METRICS_EXPORTER": "unrecognized",
    }
    assert data["provider_classes"] == startup_providers
    assert set(data["package_versions"]) == {
        "agent-framework-foundry-hosting", "agent-framework-core",
        "azure-ai-agentserver-core", "azure-ai-agentserver-responses",
        "microsoft-opentelemetry", "azure-monitor-opentelemetry",
        "azure-monitor-opentelemetry-exporter", "opentelemetry-sdk",
    }
    assert set(data["package_versions"].values()) == {"1.0.0b1"}
    assert "private-" not in json.dumps(data)
    assert data["sdk_configured"] is True and data["ingestion"] == "unverified"


def test_startup_diagnostic_base_install_defaults_are_explicit(hosted_module, startup_providers, monkeypatch, caplog):
    app = hosted_module

    def missing(package):
        raise app.PackageNotFoundError(package)

    monkeypatch.setattr(app, "version", missing)
    monkeypatch.setattr(app, "telemetry_status", lambda: {"configuration": "disabled"})
    monkeypatch.setitem(sys.modules, "opentelemetry", None)
    with caplog.at_level(logging.WARNING):
        data = app._startup_telemetry_metadata(False)
    assert set(data["package_versions"].values()) == {"not_installed"}
    assert set(data["provider_classes"].values()) == {"unavailable"}
    assert set(data["exporter_settings"].values()) == {"unset"}
    assert "inspection unavailable error_type=" in caplog.text
    assert data["configuration"] == "disabled" and data["sdk_configured"] is False


async def test_hosted_heartbeat_budget_begins_before_lock_wait(hosted_module, monkeypatch):
    app = hosted_module
    elapsed, starts = [100.0], []

    class SlowLock:
        async def acquire(self):
            elapsed[0] += 500
            return True

        def release(self):
            pass

    async def heartbeat(runner, *, started_at):
        starts.append(started_at)
        await asyncio.sleep(0)
        return []

    monkeypatch.setattr(app, "time", SimpleNamespace(monotonic=lambda: elapsed[0]))
    monkeypatch.setattr(app, "controller_heartbeat", heartbeat)
    agent = app.TriageControllerAgent.__new__(app.TriageControllerAgent)
    agent._lock = SlowLock()
    agent._runner = object()
    await agent._run_once("heartbeat")
    assert starts == [100.0]


async def test_hosted_heartbeat_lock_timeout_defers_without_cancelling_holder(
    hosted_module, monkeypatch, caplog,
):
    app = hosted_module
    entered, release = asyncio.Event(), asyncio.Event()
    holder_events, claims = [], []
    lock = asyncio.Lock()

    async def hold():
        async with lock:
            holder_events.append("entered")
            entered.set()
            await release.wait()
            holder_events.append("completed")

    async def unexpected_heartbeat(*args, **kwargs):
        claims.append("started queues")
        pytest.fail("A heartbeat that timed out acquiring the lock must not claim queue work")

    monkeypatch.setattr(app, "HEARTBEAT_BUDGET_SECONDS", 0.02)
    monkeypatch.setattr(app, "controller_heartbeat", unexpected_heartbeat)
    agent = app.TriageControllerAgent.__new__(app.TriageControllerAgent)
    agent._lock, agent._runner = lock, object()
    holder = asyncio.create_task(hold())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        with caplog.at_level(logging.WARNING, logger="triage.telemetry.heartbeat"):
            response = await asyncio.wait_for(agent._run_once("heartbeat"), 1)
        assert "Heartbeat deferred" in response.messages[0].text
        assert "queued work remains pending" in response.messages[0].text
        assert holder_events == ["entered"] and not holder.done() and lock.locked()
        assert claims == []
        assert "reason=lock_budget_exhausted" in caplog.text
        assert "automatic_calls=0 human_calls=0 budget_exhausted=True" in caplog.text
    finally:
        release.set()
        await asyncio.wait_for(holder, 1)
    assert holder_events == ["entered", "completed"] and not lock.locked()


async def test_hosted_lock_timeout_does_not_wrap_the_executing_heartbeat(hosted_module, monkeypatch):
    app = hosted_module
    completed = []

    async def heartbeat(*args, **kwargs):
        await asyncio.sleep(0.04)
        completed.append("original work settled")
        return ["completed once"]

    monkeypatch.setattr(app, "HEARTBEAT_BUDGET_SECONDS", 0.01)
    monkeypatch.setattr(app, "controller_heartbeat", heartbeat)
    agent = app.TriageControllerAgent.__new__(app.TriageControllerAgent)
    agent._lock, agent._runner = asyncio.Lock(), object()
    response = await asyncio.wait_for(agent._run_once("heartbeat"), 1)
    assert response.messages[0].text == "completed once"
    assert completed == ["original work settled"] and not agent._lock.locked()
