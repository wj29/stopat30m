# -*- coding: utf-8 -*-
"""
LLM adapter — wraps LiteLLM with tool-calling support for the agent layer.

Reuses the same config (model, keys, base URLs) as the existing LLMAnalyzer
so no duplicate configuration is needed.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from stopat30m.analysis.llm_transport import llm_request_options, safe_llm_error

logger = logging.getLogger(__name__)


@dataclass
class ToolCall:
    """Parsed tool call from LLM response."""
    id: str
    name: str
    arguments: Dict[str, Any]
    thought_signature: Optional[str] = None


@dataclass
class LLMResponse:
    """Unified LLM response."""
    content: str = ""
    tool_calls: List[ToolCall] = field(default_factory=list)
    provider: str = ""
    model: str = ""
    usage: Optional[Dict[str, int]] = None
    reasoning_content: Optional[str] = None


class LLMToolAdapter:
    """Wraps LiteLLM completion with tool-calling support.

    Reads configuration from ``config.yaml`` llm section, matching
    the existing LLMAnalyzer setup so there is zero duplicate config.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        from stopat30m.config import get
        cfg = config or get("llm") or {}

        self._model = cfg.get("model", "deepseek/deepseek-chat")
        self._temperature = cfg.get("temperature", 0.3)
        self._max_tokens = cfg.get("max_tokens", 4000)
        self._timeout = cfg.get("timeout", 90)
        self._num_retries = cfg.get("num_retries", 2)

        self._extra_params = llm_request_options(cfg)
        self._setup(cfg)

    def _setup(self, cfg: dict) -> None:
        try:
            import litellm
            litellm.drop_params = True
        except ImportError:
            logger.warning("litellm not installed — agent LLM calls will fail")
            return

    _TRANSIENT_PATTERNS = (
        "Expecting value: line 1 column 1",
        "LegacyAPIResponse",
        "RemoteDisconnected",
        "Connection aborted",
    )
    _TRANSIENT_RETRY_DELAY = 2.0
    _MAX_TRANSIENT_RETRIES = 2

    def call_with_tools(
        self,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]],
        timeout: Optional[float] = None,
    ) -> LLMResponse:
        """Call LLM with tool declarations; parse any tool_calls in response."""
        try:
            import litellm
        except ImportError:
            return LLMResponse(content="litellm not installed", provider="error")

        effective_timeout = timeout if timeout and timeout > 0 else self._timeout

        kwargs: Dict[str, Any] = {
            "model": self._model,
            "messages": self._clean_messages(messages),
            "temperature": self._temperature,
            "max_tokens": self._max_tokens,
            "timeout": effective_timeout,
            "num_retries": self._num_retries,
        }
        kwargs.update(self._extra_params)

        if tools:
            kwargs["tools"] = tools

        response = None
        last_error: Optional[Exception] = None

        for attempt in range(1 + self._MAX_TRANSIENT_RETRIES):
            try:
                response = litellm.completion(**kwargs)
                last_error = None
                break
            except Exception as e:
                last_error = e
                err_str = str(e)
                is_transient = any(pat in err_str for pat in self._TRANSIENT_PATTERNS)
                if is_transient and attempt < self._MAX_TRANSIENT_RETRIES:
                    logger.warning(
                        "LLM transient error (attempt %d/%d), retrying in %.1fs; type=%s",
                        attempt + 1, self._MAX_TRANSIENT_RETRIES + 1,
                        self._TRANSIENT_RETRY_DELAY, type(e).__name__,
                    )
                    time.sleep(self._TRANSIENT_RETRY_DELAY)
                    continue
                logger.warning("LLM call failed; type=%s", type(e).__name__)
                return LLMResponse(content=safe_llm_error(e), provider="error")

        if response is None:
            msg_str = safe_llm_error(last_error) if last_error else "AI服务没有返回有效响应。"
            return LLMResponse(content=msg_str, provider="error")

        choice = response.choices[0]
        msg = choice.message

        content = msg.content or ""
        reasoning = getattr(msg, "reasoning_content", None)
        usage = dict(response.usage) if response.usage else None
        model_name = getattr(response, "model", self._model) or self._model

        tool_calls: List[ToolCall] = []
        if msg.tool_calls:
            for tc in msg.tool_calls:
                fn = tc.function
                try:
                    args = json.loads(fn.arguments) if isinstance(fn.arguments, str) else fn.arguments
                except (json.JSONDecodeError, TypeError):
                    args = {}
                tool_calls.append(ToolCall(
                    id=tc.id or f"tc_{time.time_ns()}",
                    name=fn.name,
                    arguments=args or {},
                ))

        return LLMResponse(
            content=content,
            tool_calls=tool_calls,
            provider=model_name,
            model=model_name,
            usage=usage,
            reasoning_content=reasoning,
        )

    @staticmethod
    def _clean_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Ensure messages conform to the expected format for litellm."""
        cleaned = []
        for msg in messages:
            role = msg.get("role", "user")
            clean: Dict[str, Any] = {"role": role}

            if role == "tool":
                clean["content"] = msg.get("content", "")
                clean["tool_call_id"] = msg.get("tool_call_id", "")
                if msg.get("name"):
                    clean["name"] = msg["name"]
            elif role == "assistant" and msg.get("tool_calls"):
                clean["content"] = msg.get("content") or ""
                clean["tool_calls"] = [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["arguments"], ensure_ascii=False)
                            if isinstance(tc["arguments"], dict) else tc["arguments"],
                        },
                    }
                    for tc in msg["tool_calls"]
                ]
            else:
                clean["content"] = msg.get("content", "")

            cleaned.append(clean)
        return cleaned
