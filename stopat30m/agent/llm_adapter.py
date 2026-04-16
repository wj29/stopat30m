# -*- coding: utf-8 -*-
"""
LLM adapter — wraps LiteLLM with tool-calling support for the agent layer.

Reuses the same config (model, keys, base URLs) as the existing LLMAnalyzer
so no duplicate configuration is needed.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

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

        self._extra_params: Dict[str, Any] = {}
        self._setup(cfg)

    def _setup(self, cfg: dict) -> None:
        try:
            import litellm
            litellm.drop_params = True
        except ImportError:
            logger.warning("litellm not installed — agent LLM calls will fail")
            return

        key_env_map = {
            "deepseek_api_key": "DEEPSEEK_API_KEY",
            "openai_api_key": "OPENAI_API_KEY",
            "gemini_api_key": "GEMINI_API_KEY",
            "anthropic_api_key": "ANTHROPIC_API_KEY",
            "aihubmix_api_key": "AIHUBMIX_KEY",
        }
        base_url_env_map = {
            "deepseek_base_url": "DEEPSEEK_BASE_URL",
            "openai_base_url": "OPENAI_BASE_URL",
            "ollama_api_base": "OLLAMA_API_BASE",
        }

        for cfg_key, env_var in key_env_map.items():
            value = str(cfg.get(cfg_key, "")).strip()
            if value and not os.environ.get(env_var):
                os.environ[env_var] = value

        for cfg_key, env_var in base_url_env_map.items():
            value = str(cfg.get(cfg_key, "")).strip()
            if value and not os.environ.get(env_var):
                os.environ[env_var] = value

        model = self._model
        aihubmix_key = str(cfg.get("aihubmix_api_key", "")).strip()
        if aihubmix_key and self._is_openai_compat(model):
            if not os.environ.get("OPENAI_API_KEY"):
                os.environ["OPENAI_API_KEY"] = aihubmix_key
            self._extra_params["api_base"] = "https://aihubmix.com/v1"
        elif model.startswith("deepseek/"):
            base = str(cfg.get("deepseek_base_url", "")).strip()
            if base:
                self._extra_params["api_base"] = base
        elif self._is_openai_compat(model):
            base = str(cfg.get("openai_base_url", "")).strip()
            if base:
                self._extra_params["api_base"] = base
        elif model.startswith("ollama/"):
            base = str(cfg.get("ollama_api_base", "")).strip() or "http://localhost:11434"
            self._extra_params["api_base"] = base

    @staticmethod
    def _is_openai_compat(model: str) -> bool:
        return model.startswith("openai/") or "/" not in model

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
                        "LLM transient error (attempt %d/%d), retrying in %.1fs: %s",
                        attempt + 1, self._MAX_TRANSIENT_RETRIES + 1,
                        self._TRANSIENT_RETRY_DELAY, err_str[:200],
                    )
                    time.sleep(self._TRANSIENT_RETRY_DELAY)
                    continue
                logger.warning("LLM call failed: %s", e)
                return LLMResponse(content=str(e), provider="error")

        if response is None:
            msg_str = str(last_error) if last_error else "No response"
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
