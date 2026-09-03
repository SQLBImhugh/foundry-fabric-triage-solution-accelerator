"""Foundry provider — invokes a registered Foundry agent.

Talks to the Foundry data plane with ``httpx`` + ``DefaultAzureCredential``
rather than a client SDK. That is deliberate: two breaking details surfaced in a
single afternoon of testing (``?api-version=v1`` is rejected on the responses
route, and the ``agent`` property was replaced by ``agent_reference``). A demo
that breaks because a preview SDK minor-bumped is a bad demo, and REST puts the
wire format on screen — which is what the customer asked to see.

Handoff shapes
--------------
``responses`` (default, and what the demo runs)
    The orchestrator invokes the Data Quality agent over
    ``POST {project}/openai/v1/responses`` with an ``agent_reference``. It is a
    real call to a separate, independently versioned Foundry agent with its own
    managed identity — and because *our* code makes the call, the
    :class:`~triage.policy.PolicyLedger` charges it.

``a2a``
    Foundry's ``a2a_preview`` tool. **Requires the callee to be a hosted agent
    declaring ``container_protocol_versions: [{"protocol": "a2a"}]``.** Pointing
    it at a prompt agent fails at agent-card fetch with a 401 that reads like an
    auth problem and is not. Documented, not defaulted.

Verified 2026-08-27 against a live project.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from triage.observability import gen_ai_span
from triage.providers.base import LLMResponse, ToolCall

logger = logging.getLogger("triage.provider.foundry")

FOUNDRY_SCOPE = "https://ai.azure.com/.default"

#: Statuses worth trying again. A 400 is not here on purpose: a malformed
#: request will be malformed on the retry too.
_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


def _retry_after(resp: Any) -> float | None:
    """Honour a Retry-After header when the service sends one."""
    raw = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
    if not raw:
        return None
    try:
        return min(float(raw), 30.0)
    except (TypeError, ValueError):
        return None


class FoundryAgentProvider:
    """Invokes a named Foundry agent over the responses API."""

    provider_name = "foundry"

    def __init__(
        self,
        *,
        project_endpoint: str,
        agent_name: str,
        agent_version: str | None = None,
        handoff_mode: str = "responses",
        max_attempts: int = 3,
        backoff_base: float = 1.5,
    ):
        if not project_endpoint:
            raise ValueError("FOUNDRY_PROJECT_ENDPOINT is required for foundry mode")
        self._endpoint = project_endpoint.rstrip("/")
        self.model_name = agent_name
        self._agent_name = agent_name
        self._agent_version = agent_version
        self.handoff_mode = handoff_mode
        self._max_attempts = max(1, int(max_attempts))
        self._backoff_base = float(backoff_base)
        self._token: str = ""
        self._token_expires_at: float = 0.0
        self.last_content_filters: list[dict[str, Any]] = []

    # --- auth --------------------------------------------------------------

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token

        from azure.identity import DefaultAzureCredential

        token = DefaultAzureCredential().get_token(FOUNDRY_SCOPE)
        self._token = token.token
        self._token_expires_at = float(token.expires_on)
        return self._token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    # --- invocation --------------------------------------------------------

    async def complete(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        temperature: float = 0.1,
    ) -> LLMResponse:
        agent_ref: dict[str, Any] = {"type": "agent_reference", "name": self._agent_name}
        if self._agent_version:
            agent_ref["version"] = str(self._agent_version)

        # Tools live on the registered agent definition. Sending them per
        # request would shadow the registration and make the portal view a lie.
        payload: dict[str, Any] = {
            "agent_reference": agent_ref,
            "input": _to_input_items(messages),
        }

        with gen_ai_span(
            provider=self.provider_name,
            model=self.model_name,
            **{"gen_ai.agent.name": self._agent_name, "handoff.mode": self.handoff_mode},
        ) as span:
            body = await self._post_with_retry(payload)
            parsed = _parse_response(body)
            self.last_content_filters = body.get("content_filters", []) or []
            span.record_usage(parsed.prompt_tokens, parsed.completion_tokens)
            span.record_finish(parsed.finish_reason)
            return parsed

    async def _post_with_retry(self, payload: dict[str, Any]) -> dict[str, Any]:
        """POST the request, retrying transient failures with backoff.

        A live service occasionally returns 429 or a 5xx, and the connection can
        drop. Without this, roughly one run in four ended as ``agent_crashed`` —
        recorded honestly, but not something you want happening in front of a
        customer.

        Deliberately narrow: only rate limits, server errors and transport
        failures are retried. A 400 means the request is wrong and will be
        wrong again, so it fails immediately rather than burning the budget
        three times discovering that.
        """
        import asyncio

        import httpx

        # NOTE: this route rejects `?api-version=v1` — it requires the /v1 path
        # form. Discovered by probing; the error message is explicit.
        url = f"{self._endpoint}/openai/v1/responses"
        last_error: Exception | None = None

        for attempt in range(self._max_attempts):
            try:
                async with httpx.AsyncClient(timeout=180) as client:
                    resp = await client.post(url, headers=self._headers(), json=payload)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code in _RETRYABLE_STATUS and attempt < self._max_attempts - 1:
                    delay = _retry_after(resp) or self._backoff(attempt)
                    logger.warning(
                        "Foundry returned %s; retrying in %.1fs (attempt %d/%d)",
                        resp.status_code,
                        delay,
                        attempt + 1,
                        self._max_attempts,
                    )
                    await asyncio.sleep(delay)
                    continue

                logger.error(
                    "Foundry responses call failed %s: %s", resp.status_code, resp.text[:500]
                )
                resp.raise_for_status()

            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_error = exc
                if attempt >= self._max_attempts - 1:
                    break
                delay = self._backoff(attempt)
                logger.warning(
                    "Foundry call failed with %s; retrying in %.1fs (attempt %d/%d)",
                    type(exc).__name__,
                    delay,
                    attempt + 1,
                    self._max_attempts,
                )
                await asyncio.sleep(delay)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Foundry call exhausted retries without a response")

    def _backoff(self, attempt: int) -> float:
        """Exponential with a small deterministic jitter offset."""
        return min(self._backoff_base * (2**attempt), 20.0)

    def guardrail_summary(self) -> dict[str, bool]:
        """Flatten the last call's prompt-side filter results.

        Surfaced so the orchestrator can show that XPIA screening actually ran
        on untrusted email content, rather than asserting that it did.
        """
        out: dict[str, bool] = {}
        for cf in self.last_content_filters:
            if cf.get("source_type") != "prompt":
                continue
            for name, result in (cf.get("content_filter_results") or {}).items():
                if isinstance(result, dict) and "detected" in result:
                    out[name] = bool(result["detected"])
        return out

    async def close(self) -> None:
        return None


def _to_input_items(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Translate chat-style messages into responses input items.

    ``system`` turns are dropped: the registered agent already carries its
    instructions, so re-sending them would duplicate the prompt and silently
    diverge from what the portal shows.
    """
    out: list[dict[str, Any]] = []
    for msg in messages:
        role = msg.get("role")
        if role == "system":
            continue

        if role == "tool":
            out.append(
                {
                    "type": "function_call_output",
                    "call_id": msg.get("tool_call_id", ""),
                    "output": str(msg.get("content", "")),
                }
            )
            continue

        for call in msg.get("tool_calls") or []:
            fn = call.get("function") or {}
            out.append(
                {
                    "type": "function_call",
                    "call_id": call.get("id", ""),
                    "name": fn.get("name", ""),
                    "arguments": fn.get("arguments", "{}"),
                }
            )

        content = msg.get("content")
        if content:
            out.append({"role": role or "user", "content": str(content)})
    return out


def _parse_response(body: dict[str, Any]) -> LLMResponse:
    """Extract text + function calls from a responses payload."""
    text_parts: list[str] = []
    tool_calls: list[ToolCall] = []

    for item in body.get("output", []) or []:
        item_type = item.get("type")
        if item_type in ("function_call", "tool_call"):
            raw_args = item.get("arguments", "{}")
            try:
                args = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
            except json.JSONDecodeError:
                logger.warning("Foundry returned unparseable arguments for %s", item.get("name"))
                args = {}
            tool_calls.append(
                ToolCall(
                    id=item.get("call_id") or item.get("id", ""),
                    name=item.get("name", ""),
                    arguments=args,
                )
            )
        elif item_type == "message":
            for chunk in item.get("content", []) or []:
                if chunk.get("type") in ("output_text", "text"):
                    text_parts.append(chunk.get("text", ""))

    usage = body.get("usage") or {}
    return LLMResponse(
        content="\n".join(p for p in text_parts if p),
        tool_calls=tool_calls,
        finish_reason="tool_calls" if tool_calls else str(body.get("status") or "stop"),
        prompt_tokens=int(usage.get("input_tokens", 0) or 0),
        completion_tokens=int(usage.get("output_tokens", 0) or 0),
    )
