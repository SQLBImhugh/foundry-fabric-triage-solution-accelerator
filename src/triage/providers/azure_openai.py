"""Azure OpenAI provider — `direct` mode.

Chat completions with client-side tool execution. The controller owns the
loop, so policy is enforced in Python regardless of what the model asks for.

Auth is ``DefaultAzureCredential`` — no API keys. Under a governed tenant,
local auth is routinely disabled on a schedule, so a key-based demo works
until the night it doesn't.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from triage.observability import gen_ai_span
from triage.providers.base import LLMResponse, ToolCall

logger = logging.getLogger("triage.provider.azure")

_COGNITIVE_SCOPE = "https://cognitiveservices.azure.com/.default"


class AzureOpenAIProvider:
    provider_name = "direct"

    def __init__(
        self,
        *,
        endpoint: str,
        deployment: str,
        api_version: str = "2024-10-21",
    ):
        if not endpoint:
            raise ValueError("AZURE_OPENAI_ENDPOINT is required for direct mode")
        self.model_name = deployment
        self._endpoint = endpoint
        self._api_version = api_version
        self._client: Any = None

    def _get_client(self):
        if self._client is not None:
            return self._client

        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        from openai import AsyncAzureOpenAI

        token_provider = get_bearer_token_provider(DefaultAzureCredential(), _COGNITIVE_SCOPE)
        self._client = AsyncAzureOpenAI(
            azure_endpoint=self._endpoint,
            azure_ad_token_provider=token_provider,
            api_version=self._api_version,
        )
        return self._client

    async def complete(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float = 0.1,
    ) -> LLMResponse:
        client = self._get_client()

        with gen_ai_span(provider=self.provider_name, model=self.model_name) as span:
            kwargs: dict[str, Any] = {
                "model": self.model_name,
                "messages": _strip_internal_keys(messages),
                "temperature": temperature,
            }
            if tools:
                kwargs["tools"] = tools
                kwargs["tool_choice"] = "auto"

            resp = await client.chat.completions.create(**kwargs)
            choice = resp.choices[0]
            usage = getattr(resp, "usage", None)

            span.record_usage(
                getattr(usage, "prompt_tokens", 0) or 0,
                getattr(usage, "completion_tokens", 0) or 0,
            )
            span.record_finish(choice.finish_reason or "stop")

            return LLMResponse(
                content=choice.message.content or "",
                tool_calls=_parse_tool_calls(choice.message),
                finish_reason=choice.finish_reason or "stop",
                prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
                completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
            )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None


def _parse_tool_calls(message: Any) -> list[ToolCall]:
    out: list[ToolCall] = []
    for call in getattr(message, "tool_calls", None) or []:
        try:
            args = json.loads(call.function.arguments or "{}")
        except json.JSONDecodeError:
            logger.warning("Model emitted unparseable tool arguments for %s", call.function.name)
            args = {}
        out.append(ToolCall(id=call.id, name=call.function.name, arguments=args))
    return out


def _strip_internal_keys(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove keys the scripted provider needs but the API rejects."""
    allowed = {"role", "content", "tool_calls", "tool_call_id", "name"}
    return [{k: v for k, v in msg.items() if k in allowed} for msg in messages]
