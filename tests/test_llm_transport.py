"""Offline transport selection, credentials, and error-safety regressions."""
import logging
import os
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from stopat30m.analysis.llm_transport import (
    LLMTransportError, llm_effective_provider, llm_request_options, safe_llm_error, status_code,
)


def test_stopat_override_wins_over_alias_and_config_without_mutation():
    cfg = {"model": "openai/config", "api_type": "chat_completions",
           "openai_api_key": "config-key", "openai_base_url": "https://config.invalid/v1"}
    env = {"OPENAI_API_KEY": "alias-key", "OPENAI_BASE_URL": "https://alias.invalid/v1",
           "STOPAT30M_LLM__MODEL": "priority-model", "STOPAT30M_LLM__API_TYPE": "responses",
           "STOPAT30M_LLM__OPENAI_API_KEY": "priority-key",
           "STOPAT30M_LLM__OPENAI_BASE_URL": "https://priority.invalid/codex/v1/"}
    before = dict(env)
    assert llm_request_options(cfg, env) == {
        "model": "openai/responses/priority-model", "api_key": "priority-key",
        "api_base": "https://priority.invalid/codex/v1"}
    assert env == before
    assert cfg["model"] == "openai/config"


def test_existing_alias_wins_over_config_without_cross_provider_credentials():
    cfg = {"model": "openai/local", "openai_api_key": "local", "openai_base_url": "https://local.invalid/v1"}
    env = {"OPENAI_API_KEY": "alias", "OPENAI_BASE_URL": "https://alias.invalid/v1",
           "DEEPSEEK_API_KEY": "other-provider"}
    assert llm_request_options(cfg, env) == {
        "model": "openai/local", "api_key": "alias", "api_base": "https://alias.invalid/v1"}
    assert llm_request_options({"model": "deepseek/deepseek-chat", "deepseek_api_key": "local-ds"}, env) == {
        "model": "deepseek/deepseek-chat", "api_key": "other-provider", "api_base": "https://api.deepseek.com"}


@pytest.mark.parametrize("model,api_type,expected", [
    ("gateway-model", "chat_completions", "openai/gateway-model"),
    ("openai/gateway-model", "chat_completions", "openai/gateway-model"),
    ("openai/responses/gateway-model", "chat_completions", "openai/gateway-model"),
    ("gateway-model", "responses", "openai/responses/gateway-model"),
    ("openai/gateway-model", "responses", "openai/responses/gateway-model"),
    ("openai/responses/gateway-model", "responses", "openai/responses/gateway-model"),
])
def test_model_route_is_explicit_and_does_not_depend_on_static_registry(model, api_type, expected):
    options = llm_request_options({"model": model, "api_type": api_type}, {})
    assert options["model"] == expected


def test_legacy_aihubmix_fallback_does_not_mix_keys_and_hosts():
    cfg = {"model": "openai/custom", "aihubmix_api_key": "hub-key"}
    assert llm_request_options(cfg, {}) == {
        "model": "openai/custom", "api_key": "hub-key", "api_base": "https://aihubmix.com/v1"}
    assert llm_request_options({**cfg, "openai_api_key": "openai-key"}, {}) == {
        "model": "openai/custom", "api_key": "openai-key", "api_base": "https://api.openai.com/v1"}
    assert llm_request_options(cfg, {"STOPAT30M_LLM__OPENAI_BASE_URL": ""}) == {
        "model": "openai/custom", "api_base": "https://api.openai.com/v1"}
    with pytest.raises(ValueError, match="密钥为空"):
        llm_request_options(cfg, {"STOPAT30M_LLM__OPENAI_API_KEY": "", "OPENAI_API_KEY": "alias"})


def test_explicit_aihubmix_uses_only_its_own_key_even_with_openai_env_and_config():
    cfg = {"provider": "aihubmix", "model": "openai/custom", "api_type": "responses",
           "openai_api_key": "openai-config-key", "openai_base_url": "https://openai-config.invalid/v1",
           "aihubmix_api_key": "hub-config-key"}
    env = {"OPENAI_API_KEY": "openai-env-key", "OPENAI_BASE_URL": "https://openai-env.invalid/v1"}
    assert llm_request_options(cfg, env) == {
        "model": "openai/responses/custom", "api_key": "hub-config-key", "api_base": "https://aihubmix.com/v1"}
    assert llm_effective_provider(cfg, env) == "aihubmix"
    assert cfg["openai_api_key"] == "openai-config-key" and env["OPENAI_API_KEY"] == "openai-env-key"
    assert llm_request_options(cfg, {**env, "AIHUBMIX_KEY": "hub-env"})["api_key"] == "hub-env"
    assert llm_request_options(cfg, {**env, "AIHUBMIX_KEY": "hub-env", "STOPAT30M_LLM__AIHUBMIX_API_KEY": "hub-priority"})["api_key"] == "hub-priority"


def test_explicit_aihubmix_does_not_borrow_openai_key_when_unconfigured():
    with pytest.raises(ValueError, match="AIHubMix未配置自己的密钥"):
        llm_request_options({"provider": "aihubmix", "model": "openai/custom", "openai_api_key": "other"},
                            {"OPENAI_API_KEY": "other-alias"})


def test_explicit_openai_disables_legacy_hub_fallback():
    cfg = {"provider": "openai", "model": "custom", "aihubmix_api_key": "hub-key"}
    assert llm_effective_provider(cfg, {}) == "openai"
    assert llm_request_options(cfg, {}) == {"model": "openai/custom", "api_base": "https://api.openai.com/v1"}


@pytest.mark.parametrize("provider,model,canonical", [
    ("openai", "custom", "openai/custom"), ("openai", "openai/custom", "openai/custom"),
    ("anthropic", "claude-custom", "anthropic/claude-custom"),
    ("deepseek", "deepseek-chat", "deepseek/deepseek-chat"),
    ("gemini", "gemini-custom", "gemini/gemini-custom"),
    ("ollama", "llama-custom", "ollama/llama-custom"),
    ("ollama", "ollama_chat/llama-custom", "ollama_chat/llama-custom"),
    ("aihubmix", "custom", "openai/custom"), ("aihubmix", "aihubmix/custom", "openai/custom"),
])
def test_explicit_provider_canonicalizes_bare_models(provider, model, canonical):
    options = llm_request_options({"provider": provider, "model": model, "aihubmix_api_key": "hub-test-key"}, {})
    assert options["model"] == canonical
    assert llm_effective_provider({"provider": provider, "model": model}, {}) == provider


@pytest.mark.parametrize("provider,model", [
    ("openai", "anthropic/claude-custom"), ("anthropic", "openai/custom"),
    ("deepseek", "gemini/custom"), ("gemini", "ollama/custom"),
    ("ollama", "deepseek/custom"), ("aihubmix", "anthropic/claude-custom"),
])
def test_conflicting_known_model_prefix_is_rejected_before_credentials_are_chosen(provider, model):
    with pytest.raises(ValueError, match="服务商与模型前缀不一致"):
        llm_request_options({"provider": provider, "model": model}, {})


@pytest.mark.parametrize("provider", ["anthropic", "deepseek", "gemini", "ollama"])
def test_responses_is_rejected_for_native_nonopenai_provider(provider):
    with pytest.raises(ValueError, match="Responses协议仅支持"):
        llm_request_options({"provider": provider, "model": "custom", "api_type": "responses"}, {})


def test_provider_env_override_has_priority_and_auto_retains_legacy_inference():
    cfg = {"provider": "openai", "model": "custom", "aihubmix_api_key": "hub-test-key"}
    env = {"STOPAT30M_LLM__PROVIDER": "aihubmix"}
    assert llm_effective_provider(cfg, env) == "aihubmix"
    assert llm_request_options(cfg, env)["api_key"] == "hub-test-key"
    assert llm_effective_provider({**cfg, "provider": "auto"}, {}) == "aihubmix"
    assert llm_effective_provider({**cfg, "provider": "auto", "openai_api_key": "other"}, {}) == "openai"
    with pytest.raises(ValueError, match="服务商设置无效"):
        llm_request_options({**cfg, "provider": "invalid"}, {})


@pytest.mark.parametrize("base,expected", [
    ("", "https://api.anthropic.com"),
    ("https://claude-proxy.invalid", "https://claude-proxy.invalid"),
    ("https://claude-proxy.invalid/v1/", "https://claude-proxy.invalid"),
    ("https://claude-proxy.invalid/gateway/v1", "https://claude-proxy.invalid/gateway"),
    ("https://claude-proxy.invalid/gateway", "https://claude-proxy.invalid/gateway"),
])
def test_anthropic_base_is_native_and_only_final_v1_is_removed(base, expected):
    cfg = {"provider": "anthropic", "model": "claude-custom", "anthropic_api_key": "claude-test-key",
           "anthropic_base_url": base, "openai_base_url": "https://ignored.invalid/v1", "openai_api_key": "ignored"}
    assert llm_request_options(cfg, {}) == {
        "model": "anthropic/claude-custom", "api_key": "claude-test-key", "api_base": expected}


def test_anthropic_base_priority_blank_override_and_endpoint_rejection():
    cfg = {"provider": "anthropic", "model": "claude-custom", "anthropic_base_url": "https://config.invalid/v1"}
    assert llm_request_options(cfg, {"ANTHROPIC_BASE_URL": "https://alias.invalid/prefix/v1"})["api_base"] == "https://alias.invalid/prefix"
    env = {"ANTHROPIC_BASE_URL": "https://alias.invalid", "ANTHROPIC_API_BASE": "https://hidden.invalid",
           "STOPAT30M_LLM__ANTHROPIC_BASE_URL": ""}
    assert llm_request_options(cfg, env)["api_base"] == "https://api.anthropic.com"
    with pytest.raises(ValueError):
        llm_request_options({**cfg, "anthropic_base_url": "https://config.invalid/v1/messages"}, {})


def test_gemini_official_base_is_explicit_and_does_not_borrow_other_provider_address():
    cfg = {"provider": "gemini", "model": "gemini-custom", "gemini_api_key": "gemini-test-key",
           "openai_base_url": "https://other.invalid/v1", "anthropic_base_url": "https://another.invalid"}
    assert llm_request_options(cfg, {"GEMINI_API_BASE": "https://unregistered.invalid"}) == {
        "model": "gemini/gemini-custom", "api_key": "gemini-test-key", "api_base": "https://generativelanguage.googleapis.com"}


@pytest.mark.parametrize("model,key,env_name", [
    ("deepseek/deepseek-chat", "deepseek_api_key", "DEEPSEEK_API_KEY"),
    ("gemini/gemini-test", "gemini_api_key", "GEMINI_API_KEY"),
    ("anthropic/claude-test", "anthropic_api_key", "ANTHROPIC_API_KEY"),
])
def test_provider_specific_direct_keys(model, key, env_name):
    options = llm_request_options({"model": model, key: "config"}, {env_name: "alias"})
    assert options["model"] == model and options["api_key"] == "alias"


@pytest.mark.parametrize("provider,base,ignored_alias", [
    ("openai", "https://api.openai.com/v1", "OPENAI_API_BASE"),
    ("deepseek", "https://api.deepseek.com", "DEEPSEEK_API_BASE"),
])
def test_empty_base_override_cannot_fall_through_to_an_unregistered_sdk_alias(provider, base, ignored_alias):
    env = {"STOPAT30M_LLM__" + provider.upper() + "_BASE_URL": "", ignored_alias: "https://unregistered.invalid"}
    assert llm_request_options({"model": provider + "/synthetic"}, env)["api_base"] == base


def test_ollama_uses_direct_base_without_cloud_key():
    assert llm_request_options({"model": "ollama/local"}, {"OPENAI_API_KEY": "other"}) == {
        "model": "ollama/local", "api_base": "http://localhost:11434"}


@pytest.mark.parametrize("cfg", [
    {"model": "deepseek/deepseek-chat", "api_type": "responses"},
    {"model": "gemini/test", "api_type": "responses"},
    {"model": "openai/test", "api_type": "unknown"},
    {"model": ""},
    {"model": "openai/"},
])
def test_invalid_protocol_or_model_is_rejected(cfg):
    with pytest.raises(ValueError):
        llm_request_options(cfg, {})


@pytest.mark.parametrize("url", ["https://user:private@invalid.test/v1", "https://invalid.test/v1?key=private",
                                     "https://invalid.test/v1#private", "file:///private", "https://invalid.test:broken"])
def test_url_rejection_does_not_echo_address_credentials(url):
    with pytest.raises(ValueError) as failure:
        llm_request_options({"model": "openai/test", "openai_base_url": url}, {})
    assert url not in str(failure.value) and "private" not in str(failure.value)


class UpstreamFailure(Exception):
    def __init__(self, code):
        super().__init__("sensitive-provider-fragment https://private.invalid/?key=private")
        self.status_code = code


@pytest.mark.parametrize("code,expected", [(401, "网页登录"), (404, "模型名称、API协议和服务地址"),
                                           (429, "冷却"), (400, "参数"), (422, "参数"), (500, "调用失败")])
def test_safe_upstream_errors_only_expose_fixed_guidance(code, expected):
    message = safe_llm_error(UpstreamFailure(code))
    assert expected in message
    assert "sensitive" not in message and "private" not in message


def test_status_extraction_accepts_only_http_integer():
    assert status_code(SimpleNamespace(response=SimpleNamespace(status_code=404))) == 404
    for value in [True, "401", 999, None]:
        assert status_code(SimpleNamespace(status_code=value)) is None


@pytest.fixture
def fake_litellm(monkeypatch):
    # No installed LiteLLM import, model-map download, provider or telemetry IO.
    module = ModuleType("litellm")
    module.completion = Mock()
    monkeypatch.setitem(sys.modules, "litellm", module)
    for key in list(os.environ):
        if key.startswith("STOPAT30M_LLM__") or key in {
            "OPENAI_API_KEY", "OPENAI_BASE_URL", "AIHUBMIX_KEY", "DEEPSEEK_API_KEY", "DEEPSEEK_BASE_URL",
            "GEMINI_API_KEY", "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "OLLAMA_API_BASE",
        }:
            monkeypatch.delenv(key)
    return module


def model_response(content="synthetic answer", tools=None):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(
        content=content, tool_calls=tools, reasoning_content=None))], model="upstream-model", usage={"total_tokens": 3})


def make_analyzer(monkeypatch, cfg):
    from stopat30m.analysis import llm_analyzer
    monkeypatch.setattr(llm_analyzer, "get", lambda key: cfg)
    return llm_analyzer.LLMAnalyzer()


def test_analyzer_responses_keeps_text_contract_and_does_not_inject_environment(monkeypatch, fake_litellm):
    before = dict(os.environ)
    analyzer = make_analyzer(monkeypatch, {"model": "openai/gateway", "api_type": "responses",
                                         "openai_api_key": "config-key", "openai_base_url": "https://offline.invalid/codex/v1"})
    fake_litellm.completion.return_value = model_response("  synthetic text  ")
    assert analyzer.generate_text("synthetic prompt", system="synthetic system", max_tokens=10) == "synthetic text"
    request = fake_litellm.completion.call_args.kwargs
    assert request["model"] == "openai/responses/gateway"
    assert request["api_key"] == "config-key" and request["api_base"] == "https://offline.invalid/codex/v1"
    assert request["max_tokens"] == 10 and request["messages"][0]["role"] == "system"
    assert analyzer._model == "openai/gateway" and dict(os.environ) == before


def test_agent_responses_keeps_tools_contract_and_does_not_inject_environment(fake_litellm):
    from stopat30m.agent.llm_adapter import LLMToolAdapter
    before = dict(os.environ)
    adapter = LLMToolAdapter({"model": "gateway", "api_type": "responses", "openai_api_key": "config-key",
                              "openai_base_url": "https://offline.invalid/v1"})
    tool = SimpleNamespace(id="call_1", function=SimpleNamespace(name="lookup", arguments='{"code":"SH510300"}'))
    fake_litellm.completion.return_value = model_response(tools=[tool])
    tools = [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}]
    response = adapter.call_with_tools([{"role": "user", "content": "synthetic"}], tools)
    assert response.content == "synthetic answer" and response.tool_calls[0].arguments == {"code": "SH510300"}
    request = fake_litellm.completion.call_args.kwargs
    assert request["model"] == "openai/responses/gateway" and request["api_key"] == "config-key"
    assert request["tools"] == tools and dict(os.environ) == before


@pytest.mark.parametrize("code", [401, 404])
def test_analyzer_and_agent_never_return_raw_upstream_error(monkeypatch, fake_litellm, caplog, code):
    from stopat30m.analysis import llm_analyzer
    from stopat30m.agent.llm_adapter import LLMToolAdapter
    cfg = {"model": "openai/offline", "openai_api_key": "config-key"}
    analyzer = make_analyzer(monkeypatch, cfg)
    fake_litellm.completion.side_effect = UpstreamFailure(code)
    with pytest.raises(LLMTransportError) as error:
        analyzer.generate_text("synthetic")
    assert error.value.status_code == code and "private" not in str(error.value)
    monkeypatch.setattr(llm_analyzer, "build_user_prompt", lambda *a, **k: "synthetic")
    result = analyzer.analyze(None)
    assert str(code) in result.summary and result.raw_response == ""
    with caplog.at_level(logging.WARNING):
        response = LLMToolAdapter(cfg).call_with_tools([], [])
    assert response.provider == "error" and str(code) in response.content
    assert "sensitive-provider-fragment" not in result.model_dump_json() + response.content + caplog.text


def test_targeted_transient_retry_remains_available_without_error_text_logging(monkeypatch, fake_litellm, caplog):
    from stopat30m.analysis import llm_analyzer
    from stopat30m.agent import llm_adapter
    cfg = {"model": "openai/offline", "openai_api_key": "synthetic-key"}
    analyzer = make_analyzer(monkeypatch, cfg)
    failure = RuntimeError("RemoteDisconnected sensitive-provider-fragment")
    fake_litellm.completion.side_effect = [failure, model_response()]
    monkeypatch.setattr(llm_analyzer.time, "sleep", lambda delay: None)
    assert analyzer.generate_text("synthetic") == "synthetic answer"
    assert fake_litellm.completion.call_count == 2
    fake_litellm.completion.reset_mock(side_effect=True)
    fake_litellm.completion.side_effect = [failure, model_response()]
    with caplog.at_level(logging.WARNING):
        assert llm_adapter.LLMToolAdapter(cfg).call_with_tools([], []).content == "synthetic answer"
    assert fake_litellm.completion.call_count == 2
    assert "sensitive-provider-fragment" not in caplog.text


def test_installed_litellm_responses_bridge_is_verified_with_network_disabled(tmp_path):
    # Isolate the real optional SDK from the fake module fixture and from every
    # credential in the test runner's environment. The Responses endpoint itself
    # is mocked, and socket IO is forbidden before importing LiteLLM.
    script = r'''
import importlib.util
import socket
from unittest.mock import patch
def forbidden(*args, **kwargs):
    raise AssertionError("Network is disabled for this test")
socket.socket.connect = forbidden
socket.socket.connect_ex = forbidden
socket.socket.sendto = forbidden
socket.getaddrinfo = forbidden
if importlib.util.find_spec("litellm") is None:
    raise SystemExit(77)
import litellm
from litellm.types.llms.openai import ResponsesAPIResponse
litellm.telemetry = False
litellm.suppress_debug_info = True
native = ResponsesAPIResponse(
    id="resp_offline", created_at=1, object="response", model="synthetic-gateway-model", status="completed",
    output=[{"type":"message", "id":"msg_offline", "status":"completed", "role":"assistant",
             "content":[{"type":"output_text", "text":"offline bridge text", "annotations":[]}]},
            {"type":"function_call", "id":"fc_offline", "call_id":"call_offline",
             "name":"lookup", "arguments":"{\"code\":\"SH510300\"}", "status":"completed"}],
    usage={"input_tokens":1,"output_tokens":2,"total_tokens":3},
)
with patch.object(litellm, "responses", return_value=native) as endpoint:
    result = litellm.completion(
        model="openai/responses/synthetic-gateway-model", messages=[{"role":"user", "content":"offline only"}],
        api_key="offline-test-key", api_base="https://offline.invalid/codex/v1", max_tokens=10,
        timeout=1, num_retries=0,
    )
assert result.choices[0].message.content == "offline bridge text"
assert result.choices[0].message.tool_calls[0].function.name == "lookup"
assert endpoint.call_count == 1
options = endpoint.call_args.kwargs
assert options["api_key"] == "offline-test-key"
assert options["api_base"] == "https://offline.invalid/codex/v1"
assert options["max_output_tokens"] == 10
print("bridge-ok")
'''
    result = subprocess.run([sys.executable, "-c", script], cwd=tmp_path, timeout=30, text=True,
                            env={"LITELLM_LOCAL_MODEL_COST_MAP": "True", "DO_NOT_TRACK": "1"},
                            capture_output=True)
    if result.returncode == 77:
        pytest.skip("optional litellm dependency not installed")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "bridge-ok" in result.stdout
