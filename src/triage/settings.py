"""Runtime configuration.

Every value has a default that keeps the demo runnable offline. Nothing here
is required to execute both scenarios end-to-end with mock tools.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ProviderMode = Literal["mock", "direct", "foundry"]
ToolMode = Literal["mock", "live"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Execution mode ----------------------------------------------------
    triage_provider_mode: ProviderMode = "mock"
    triage_tool_mode: ToolMode = "mock"

    # --- Foundry -----------------------------------------------------------
    foundry_project_endpoint: str = ""
    foundry_triage_agent_name: str = "bi-triage"
    foundry_dq_agent_name: str = "bi-data-quality"
    foundry_agent_model: str = "gpt-5.6-luna"
    # Which agent-to-agent handoff shape the runtime uses.
    # 'responses'  = invoke the other agent over /openai/v1/responses. Our code
    #                makes the call, so the PolicyLedger charges it. Verified.
    # 'a2a'        = Foundry's a2a_preview tool. Requires the callee to be a
    #                HOSTED agent speaking the 'a2a' protocol; a prompt agent
    #                publishes no agent card and the call fails at card fetch.
    foundry_handoff_mode: Literal["responses", "a2a"] = "responses"
    foundry_guardrail_name: str = "bi-triage-guardrail"

    # --- Azure OpenAI (direct mode) ---------------------------------------
    azure_openai_endpoint: str = ""
    azure_openai_deployment: str = "gpt-4o"
    azure_openai_api_version: str = "2024-10-21"

    # --- Graph -------------------------------------------------------------
    graph_tenant_id: str = ""
    graph_client_id: str = ""
    graph_client_secret: str = Field(default="", repr=False)
    graph_mailbox: str = "bi-alerts@contoso.com"
    #: How mail arrives. Only ``poll`` is implemented; ``subscription`` is
    #: rejected at startup rather than silently falling back, because an
    #: operator who believes they have push notifications and is actually
    #: getting a 30-second poll has a latency assumption nothing will correct.
    graph_ingestion_mode: Literal["poll", "subscription"] = "poll"
    #: Default seconds between polls for ``bi-triage watch``. Overridable per
    #: run with ``--interval``.
    graph_poll_seconds: int = 30
    # A mailbox the agent must NOT be able to read. `watch` probes it at startup
    # and refuses to run if the read succeeds, because that means the app
    # registration is not scoped and can read the whole tenant.
    graph_canary_mailbox: str = ""
    # Only messages that look like Power BI refresh alerts are triaged.
    #
    # This is a security control, not tidiness. An agent that acts on every
    # message in a mailbox is steerable by anyone who can send mail to it --
    # the inbox becomes an prompt-injection surface with no authentication.
    # Left unfiltered, this demo triaged Entra ID Protection and PIM digests
    # and crashed on several of them.
    #
    # Empty allowlist means "any sender", which is deliberately NOT the default.
    graph_sender_allowlist: str = "no-reply-powerbi@microsoft.com,no-reply@powerbi.com"
    graph_subject_pattern: str = r"(?i)\b(power\s*bi|fabric|refresh|semantic model|dataset)\b"

    # Durable incident store. Empty = JSON file next to the repo, which is the
    # right choice on a laptop and the wrong one in a container: the filesystem
    # goes away on recycle and takes the open incidents with it, which would let
    # the agent remediate the same failure twice after a restart.
    incident_table_endpoint: str = ""
    incident_table_name: str = "incidents"
    # Which alert mail has already been triaged. Shares the incident table's
    # endpoint because it shares its lifetime: both must outlive a hosted
    # agent invocation or a scheduled sweep re-triages everything it sees.
    processed_table_name: str = "processedmessages"
    # Where approval requests wait and decisions land. Same endpoint again:
    # the agent writes the request, a human writes the answer, and the agent
    # reads it back on a later poll -- possibly in a different process.
    approval_table_name: str = "approvals"
    #: Retries the agent postponed rather than performing. Same endpoint, same
    #: reason: the run that defers and the sweep that performs it are different
    #: processes, often different invocations.
    retry_table_name: str = "deferredretries"
    #: Baselines for the silent-failure detector. Same endpoint again.
    semantic_health_table_name: str = "semantichealth"
    #: The detector's off switch. Configuration rather than routine state,
    #: because `azd deploy` re-enables a disabled routine from azure.yaml.
    silent_sweep_enabled: bool = True
    #: Probe configuration, as raw JSON. Deliberately typed ``str`` rather than
    #: ``list[dict]``: pydantic-settings JSON-decodes complex annotations inside
    #: the environment source, *before* any validator runs, so an unset variable
    #: ("") raised SettingsError at import and the container never started. That
    #: took mail triage, approvals and remediation down over one empty string
    #: belonging to an optional detector. A plain string is never decoded, so
    #: parsing happens in ``load_probes`` where a bad value disables only the
    #: detector.
    silent_health_probes: str = ""
    #: Seconds between probes within one sweep. ``executeQueries`` is throttled
    #: per user across all datasets, so a detector that fires them back to back
    #: becomes load on the capacity it is watching.
    silent_probe_pace_seconds: float = 1.0
    # The URL behind the card's Approve/Decline buttons. An incoming webhook
    # has no bot behind it, so Action.Submit does nothing; the buttons have to
    # be links to something that records the decision. Empty means the card
    # shows the request without buttons and the CLI is the only channel.
    approval_callback_url: str = ""
    # How long a gated action waits for an answer before failing closed.
    approval_timeout_seconds: int = 300

    # --- Power BI ----------------------------------------------------------
    powerbi_tenant_id: str = ""
    powerbi_client_id: str = ""
    powerbi_client_secret: str = Field(default="", repr=False)
    powerbi_workspace_id: str = ""
    powerbi_dataset_id: str = ""

    # --- Teams -------------------------------------------------------------
    teams_webhook_url: str = Field(default="", repr=False)
    #: Only ``webhook`` works. Graph app-only channel posting is restricted by
    #: Microsoft to migration scenarios, so an unattended agent cannot post as
    #: itself; ``graph`` is rejected rather than silently falling back.
    teams_mode: Literal["webhook", "graph"] = "webhook"

    # --- Observability -----------------------------------------------------
    applicationinsights_connection_string: str = Field(default="", repr=False)

    # --- Policy ------------------------------------------------------------
    # Shared across every agent in a run, not per agent. See TriagePolicy.
    triage_max_llm_turns: int = 14
    triage_max_tool_calls: int = 20
    triage_max_write_actions: int = 1
    triage_max_tokens: int = 80_000
    triage_timeout_seconds: int = 300

    app_version: str = "0.1.0"


settings = Settings()
