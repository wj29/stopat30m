"""Per-request LLM routing and credentials; never mutate process environment."""
from __future__ import annotations

import os
from urllib.parse import urlsplit

_ALIASES = {
    "deepseek_api_key": ("DEEPSEEK_API_KEY",), "openai_api_key": ("OPENAI_API_KEY",),
    "gemini_api_key": ("GEMINI_API_KEY",), "anthropic_api_key": ("ANTHROPIC_API_KEY",),
    "aihubmix_api_key": ("AIHUBMIX_KEY",), "deepseek_base_url": ("DEEPSEEK_BASE_URL",),
    "openai_base_url": ("OPENAI_BASE_URL",), "ollama_api_base": ("OLLAMA_API_BASE",),
    "anthropic_base_url": ("ANTHROPIC_BASE_URL",),
}
_PROVIDERS = {"auto", "openai", "anthropic", "deepseek", "gemini", "ollama", "aihubmix"}
_PREFIX_PROVIDERS = {name: name for name in _PROVIDERS - {"auto"}}
_PREFIX_PROVIDERS["ollama_chat"] = "ollama"


def _value(cfg, key, environ, default=""):
    qualified = "STOPAT30M_LLM__" + key.upper()
    if qualified in environ:
        raw = environ[qualified]
    else:
        raw = next((environ[alias] for alias in _ALIASES.get(key, ()) if environ.get(alias)), cfg.get(key, default))
    return "" if raw is None else str(raw).strip()


def is_openai_compatible(model):
    return model.startswith("openai/") or "/" not in model


def _configured_provider(cfg, environ):
    provider = _value(cfg, "provider", environ, "auto")
    if provider not in _PROVIDERS:
        raise ValueError("AI服务商设置无效，请在系统设置重新选择服务商。")
    return provider


def _check_provider_model(provider, model):
    if not model:
        raise ValueError("AI模型名称不能为空，请在系统设置检查模型配置。")
    prefix = model.split("/", 1)[0] if "/" in model else None
    prefixed_provider = _PREFIX_PROVIDERS.get(prefix)
    allowed = {provider, "openai"} if provider == "aihubmix" else {provider}
    if prefixed_provider and prefixed_provider not in allowed:
        raise ValueError("AI服务商与模型前缀不一致，请核对服务商和模型名称后保存。")


def llm_effective_provider(cfg: dict, environ=None) -> str:
    """Return the selected routing provider, with no credentials in the result.

    ``auto`` retains legacy model inference, including AIHubMix-only fallback.
    Explicit providers select their own fields and reject conflicting known
    model prefixes. Other legacy LiteLLM provider prefixes remain usable in auto.
    """
    environ = os.environ if environ is None else environ
    provider = _configured_provider(cfg, environ)
    model = _value(cfg, "model", environ, "deepseek/deepseek-chat")
    if provider != "auto":
        _check_provider_model(provider, model)
        return provider
    if not model:
        raise ValueError("AI模型名称不能为空，请在系统设置检查模型配置。")
    if is_openai_compatible(model):
        if (not _value(cfg, "openai_api_key", environ)
                and not _value(cfg, "openai_base_url", environ)
                and "STOPAT30M_LLM__OPENAI_API_KEY" not in environ
                and "STOPAT30M_LLM__OPENAI_BASE_URL" not in environ
                and _value(cfg, "aihubmix_api_key", environ)):
            return "aihubmix"
        return "openai"
    prefix = model.split("/", 1)[0]
    return _PREFIX_PROVIDERS.get(prefix, prefix)


def llm_request_options(cfg: dict, environ=None) -> dict:
    """Resolve STOPAT30M fields > existing aliases > local config per request.

    ``responses`` uses LiteLLM's completion bridge so callers still receive a
    ModelResponse with choices/tool calls. It is an explicit per-call route,
    never a global switch and never inferred from a URL containing "codex".
    """
    environ = os.environ if environ is None else environ
    model = _value(cfg, "model", environ, "deepseek/deepseek-chat")
    provider = llm_effective_provider(cfg, environ)
    api_type = _value(cfg, "api_type", environ, "chat_completions")
    if not model:
        raise ValueError("AI模型名称不能为空，请在系统设置检查模型配置。")
    if api_type not in {"chat_completions", "responses"}:
        raise ValueError("AI调用协议仅支持 chat_completions 或 responses。")
    compatible = provider in {"openai", "aihubmix"}
    if api_type == "responses" and not compatible:
        raise ValueError("Responses协议仅支持OpenAI兼容模型，请核对模型前缀和调用协议。")
    key_field, base_field, api_key, api_base = None, None, "", ""
    if provider == "aihubmix":
        key_field, api_base = "aihubmix_api_key", "https://aihubmix.com/v1"
    elif provider == "openai":
        key_field, base_field = "openai_api_key", "openai_base_url"
    elif provider == "deepseek":
        key_field, base_field = "deepseek_api_key", "deepseek_base_url"
    elif provider == "gemini":
        key_field, api_base = "gemini_api_key", "https://generativelanguage.googleapis.com"
    elif provider == "anthropic":
        key_field, base_field = "anthropic_api_key", "anthropic_base_url"
    elif provider == "ollama":
        base_field = "ollama_api_base"
    if key_field and not api_key:
        api_key = _value(cfg, key_field, environ)
    if key_field and "STOPAT30M_LLM__" + key_field.upper() in environ and not api_key:
        # Omitting a blank explicit override would let the SDK silently fall
        # back to a lower-priority provider alias. Refuse that ambiguous call.
        raise ValueError("环境变量中的AI密钥为空，请先修正该环境设置。")
    if provider == "aihubmix" and not api_key:
        raise ValueError("AIHubMix未配置自己的密钥，请在系统设置填写AIHubMix密钥。")
    if base_field and not api_base:
        api_base = _value(cfg, base_field, environ)
        if not api_base and base_field == "ollama_api_base":
            api_base = "http://localhost:11434"
        elif not api_base and base_field == "openai_base_url":
            api_base = "https://api.openai.com/v1"
        elif not api_base and base_field == "deepseek_base_url":
            api_base = "https://api.deepseek.com"
        elif not api_base and base_field == "anthropic_base_url":
            api_base = "https://api.anthropic.com"
    if api_base:
        try:
            parts = urlsplit(api_base)
            if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
                raise ValueError
            _ = parts.port
            if provider == "anthropic" and parts.path.rstrip("/").endswith("/v1/messages"):
                raise ValueError
        except ValueError:
            raise ValueError("AI服务地址无效，请检查http/https地址，勿在地址中携带凭据。") from None
        # LiteLLM's native Anthropic handler appends /v1/messages itself.
        # Preserve a custom gateway prefix while accepting copied /v1 bases.
        if provider == "anthropic" and api_base.rstrip("/").endswith("/v1"):
            api_base = api_base.rstrip("/")[:-3]
    if provider in _PROVIDERS - {"auto"}:
        prefix, separator, remainder = model.partition("/")
        bare = remainder if separator and prefix in _PREFIX_PROVIDERS else model
        if compatible:
            bare = bare.removeprefix("responses/")
        if not bare:
            raise ValueError("AI模型名称不能为空。")
        # Unknown gateway model names need an explicit provider too. Protocol
        # selection is owned by api_type rather than a saved routing prefix.
        if compatible:
            routing_prefix = "openai/responses" if api_type == "responses" else "openai"
        else:
            routing_prefix = "ollama_chat" if provider == "ollama" and prefix == "ollama_chat" else provider
        model = routing_prefix + "/" + bare
    result = {"model": model}
    if api_key:
        result["api_key"] = api_key
    if api_base:
        result["api_base"] = api_base.rstrip("/")
    return result


class LLMTransportError(RuntimeError):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


def status_code(exc):
    value = getattr(exc, "status_code", None)
    if value is None:
        value = getattr(getattr(exc, "response", None), "status_code", None)
    return value if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599 else None


def safe_llm_error(exc):
    code = status_code(exc)
    if code == 401:
        return "AI/上游服务鉴权失败（HTTP 401），请在系统设置核对对应服务密钥和地址；这不是网页登录失效。"
    if code == 404:
        return "AI服务未找到模型或接口（HTTP 404），请在系统设置核对模型名称、API协议和服务地址。"
    if code == 429:
        return "AI服务请求过于频繁（HTTP 429），请等待服务端冷却后重试。"
    if code in {400, 422}:
        return "AI服务不接受当前请求参数，请核对模型支持的协议和参数。"
    return "AI服务调用失败，请核对服务状态、模型和连接配置后重试。"
