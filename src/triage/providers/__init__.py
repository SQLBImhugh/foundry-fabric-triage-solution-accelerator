"""Provider factory."""

from __future__ import annotations

from typing import Literal

from triage.providers.base import BaseProvider, LLMResponse, ToolCall
from triage.providers.mock import ScriptedDataQualityProvider, ScriptedProvider

Role = Literal["triage", "data_quality"]

__all__ = [
    "BaseProvider",
    "LLMResponse",
    "ToolCall",
    "ScriptedProvider",
    "ScriptedDataQualityProvider",
    "get_provider",
]


def get_provider(role: Role, settings, **overrides) -> BaseProvider:
    """Build the provider for one agent role from settings.

    Imports for the live providers are deferred so the base install stays free
    of Azure dependencies — the mock path must work with nothing but pydantic.
    """
    mode = overrides.pop("mode", None) or settings.triage_provider_mode

    if mode == "mock":
        if role == "data_quality":
            return ScriptedDataQualityProvider()
        return ScriptedProvider(**overrides)

    if mode == "direct":
        from triage.providers.azure_openai import AzureOpenAIProvider

        return AzureOpenAIProvider(
            endpoint=settings.azure_openai_endpoint,
            deployment=settings.azure_openai_deployment,
            api_version=settings.azure_openai_api_version,
        )

    if mode == "foundry":
        from triage.providers.foundry import FoundryAgentProvider

        agent_name = (
            settings.foundry_dq_agent_name
            if role == "data_quality"
            else settings.foundry_triage_agent_name
        )
        return FoundryAgentProvider(
            project_endpoint=settings.foundry_project_endpoint,
            agent_name=agent_name,
            handoff_mode=overrides.pop(
                "handoff_mode", getattr(settings, "foundry_handoff_mode", "responses")
            ),
        )

    raise ValueError(f"Unknown provider mode '{mode}'")
