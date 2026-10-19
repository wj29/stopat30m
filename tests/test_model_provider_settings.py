"""Provider settings boundaries using temporary configuration and no external IO."""
import json

import pytest
import yaml

from stopat30m.settings_store import SettingsError, SettingsStore


PROVIDERS = {"auto", "openai", "anthropic", "deepseek", "gemini", "ollama", "aihubmix"}
PRIVATE_KEYS = {
    name: f"offline-only-{name}-credential-never-use"
    for name in ("openai", "anthropic", "deepseek", "gemini", "aihubmix")
}


@pytest.fixture
def store(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({
        "llm": {"enabled": True, "model": "openai/offline-model", "api_type": "chat_completions"},
        "auth": {"secret_key": ""},
    }), encoding="utf-8")
    return SettingsStore(config, environ={})


def fields(response):
    return {field["key"]: field for group in response["groups"] for field in group["fields"]}


def save(store, values, clear=None):
    return store.update(store.get()["revision"], values, clear or [], user_id=17, username="offline-admin")


def seed_private_settings(store):
    local = {"llm": {f"{name}_api_key": value for name, value in PRIVATE_KEYS.items()},
             "unrelated_private": {"credential": "offline-only-unrelated-credential"}}
    store.local_path.write_text(yaml.safe_dump(local), encoding="utf-8")
    return local


def assert_no_secrets(value):
    serialized = json.dumps(value, ensure_ascii=False)
    for secret in [*PRIVATE_KEYS.values(), "offline-only-unrelated-credential"]:
        assert secret not in serialized


def test_provider_enum_defaults_to_auto_without_migrating_legacy_configuration(store):
    view = fields(store.get())
    assert view["llm.provider"]["value"] == "auto"
    assert {item["value"] for item in view["llm.provider"]["options"]} == PROVIDERS
    assert view["llm.provider"]["type"] == "enum"
    assert not store.local_path.exists()


@pytest.mark.parametrize("provider,model", [
    ("openai", "offline-openai-model"), ("anthropic", "claude-offline"),
    ("deepseek", "deepseek-offline"), ("gemini", "gemini-offline"),
    ("ollama", "offline-local-model"), ("aihubmix", "offline-gateway-model"),
])
def test_explicit_provider_is_persistent_and_preserves_all_other_service_credentials(store, provider, model):
    original = seed_private_settings(store)
    public = store.base_path.read_bytes()
    result = save(store, {"llm.provider": provider, "llm.model": model})
    assert fields(result)["llm.provider"]["value"] == provider
    assert fields(result)["llm.provider"]["source"] == "local"
    reloaded = SettingsStore(store.base_path, environ={})
    assert fields(reloaded.get())["llm.provider"]["value"] == provider
    persisted = yaml.safe_load(store.local_path.read_text(encoding="utf-8"))
    for name, secret in PRIVATE_KEYS.items():
        field = fields(result)[f"llm.{name}_api_key"]
        assert field["type"] == "secret" and field["value"] is None and field["configured"] is True
        assert persisted["llm"][f"{name}_api_key"] == secret
    assert persisted["unrelated_private"] == original["unrelated_private"]
    assert store.base_path.read_bytes() == public
    assert set(result["changed_keys"]) == {"llm.provider", "llm.model"}
    assert reloaded.audit()[0]["secret_keys"] == []
    assert_no_secrets({"result": result, "reloaded": reloaded.get(), "audit": reloaded.audit()})


def test_switching_provider_does_not_change_blank_or_nonselected_keys_and_clear_is_explicit(store):
    seed_private_settings(store)
    save(store, {"llm.provider": "anthropic", "llm.model": "claude-offline",
                 "llm.openai_api_key": "", "llm.anthropic_api_key": ""})
    result = save(store, {"llm.provider": "openai", "llm.model": "offline-openai"},
                  clear=["llm.anthropic_api_key"])
    persisted = yaml.safe_load(store.local_path.read_text(encoding="utf-8"))
    assert persisted["llm"]["anthropic_api_key"] == ""
    assert persisted["llm"]["openai_api_key"] == PRIVATE_KEYS["openai"]
    assert persisted["llm"]["deepseek_api_key"] == PRIVATE_KEYS["deepseek"]
    assert fields(result)["llm.anthropic_api_key"]["configured"] is False
    assert fields(result)["llm.openai_api_key"]["configured"] is True
    assert store.audit()[0]["secret_keys"] == ["llm.anthropic_api_key"]
    assert_no_secrets({"view": result, "audit": store.audit()})


@pytest.mark.parametrize("env_name", ["STOPAT30M_LLM__ANTHROPIC_BASE_URL", "ANTHROPIC_BASE_URL"])
def test_claude_base_url_environment_override_is_visible_and_read_only(store, env_name):
    seed_private_settings(store)
    store.environ[env_name] = "https://claude-environment.invalid/v1"
    field = fields(store.get())["llm.anthropic_base_url"]
    assert field["value"] == "https://claude-environment.invalid/v1"
    assert field["source"] == "environment" and field["read_only"] is True
    before = store.local_path.read_bytes()
    with pytest.raises(SettingsError, match="read-only"):
        save(store, {"llm.anthropic_base_url": "https://another.invalid/v1"})
    assert store.local_path.read_bytes() == before and store.audit() == []
    assert_no_secrets(store.get())


def test_claude_url_is_persistent_and_requires_restart_for_existing_instances(store):
    result = save(store, {"llm.provider": "anthropic", "llm.model": "claude-offline",
                          "llm.anthropic_base_url": "https://claude-offline.invalid/v1"})
    assert fields(result)["llm.anthropic_base_url"]["value"] == "https://claude-offline.invalid/v1"
    assert "llm.anthropic_base_url" in result["restart_required"]
    assert fields(SettingsStore(store.base_path, environ={}).get())["llm.anthropic_base_url"]["value"] == "https://claude-offline.invalid/v1"


@pytest.mark.parametrize("provider", ["claude", "OpenAI", "", None, False, ["openai"]])
def test_invalid_provider_never_writes_configuration(store, provider):
    public = store.base_path.read_bytes()
    with pytest.raises(SettingsError):
        save(store, {"llm.provider": provider})
    assert not store.local_path.exists() and store.audit() == []
    assert store.base_path.read_bytes() == public


@pytest.mark.parametrize("url", [
    "ftp://claude.invalid/v1", "https:///no-host", "https://private-url-value@claude.invalid/v1",
    "https://claude.invalid/v1?key=private-url-value", "https://claude.invalid/v1#private-url-value",
    "https://claude.invalid:invalid/v1", "https://claude.invalid:70000/v1",
])
def test_invalid_claude_urls_reject_without_value_echo_or_partial_writes(store, url):
    seed_private_settings(store)
    before = store.local_path.read_bytes()
    with pytest.raises(SettingsError) as error:
        save(store, {"llm.provider": "anthropic", "llm.model": "claude-offline", "llm.anthropic_base_url": url})
    assert url not in str(error.value) and "private-url-value" not in str(error.value)
    assert store.local_path.read_bytes() == before and store.audit() == []
    assert_no_secrets(str(error.value))


@pytest.mark.parametrize("provider,model", [
    ("openai", "anthropic/claude-offline"), ("anthropic", "openai/offline-model"),
    ("deepseek", "gemini/offline-model"), ("gemini", "deepseek/offline-model"),
    ("ollama", "anthropic/offline-model"), ("aihubmix", "anthropic/offline-model"),
])
def test_provider_model_prefix_conflicts_do_not_commit(store, provider, model):
    seed_private_settings(store)
    before = store.local_path.read_bytes()
    with pytest.raises(SettingsError):
        save(store, {"llm.provider": provider, "llm.model": model})
    assert store.local_path.read_bytes() == before and store.audit() == []


@pytest.mark.parametrize("provider,model", [
    ("anthropic", "claude-offline"), ("deepseek", "deepseek-offline"),
    ("gemini", "gemini-offline"), ("ollama", "offline-local"),
    ("auto", "anthropic/claude-offline"), ("auto", "deepseek/offline"),
    ("auto", "gemini/offline"), ("auto", "ollama/offline"),
])
def test_responses_is_rejected_for_non_openai_compatible_providers(store, provider, model):
    with pytest.raises(SettingsError):
        save(store, {"llm.provider": provider, "llm.model": model, "llm.api_type": "responses"})
    assert not store.local_path.exists() and store.audit() == []


@pytest.mark.parametrize("provider,model", [
    ("openai", "offline-model"), ("aihubmix", "offline-model"),
    ("aihubmix", "openai/offline-model"), ("auto", "openai/offline-model"),
])
def test_responses_is_allowed_for_openai_and_aihubmix(store, provider, model):
    result = save(store, {"llm.provider": provider, "llm.model": model, "llm.api_type": "responses"})
    assert fields(result)["llm.provider"]["value"] == provider
    assert fields(result)["llm.api_type"]["value"] == "responses"


@pytest.mark.parametrize("model,legacy_hub", [
    ("openai/offline-model", False), ("deepseek/offline-model", False),
    ("anthropic/claude-offline", False), ("gemini/offline-model", False),
    ("ollama/offline-local", False), ("openai/offline-gateway", True),
])
def test_auto_preserves_legacy_model_identifiers_and_aihubmix_only_credentials(store, model, legacy_hub):
    base = yaml.safe_load(store.base_path.read_text(encoding="utf-8"))
    base["llm"]["model"] = model
    if legacy_hub:
        base["llm"]["aihubmix_api_key"] = PRIVATE_KEYS["aihubmix"]
    store.base_path.write_text(yaml.safe_dump(base), encoding="utf-8")
    public = store.base_path.read_bytes()
    result = save(store, {"research.min_interval": 4})
    assert fields(result)["llm.provider"]["value"] == "auto"
    assert fields(result)["llm.model"]["value"] == model
    persisted = yaml.safe_load(store.local_path.read_text(encoding="utf-8"))
    assert "llm" not in persisted
    assert store.base_path.read_bytes() == public
    assert_no_secrets({"view": result, "audit": store.audit()})
