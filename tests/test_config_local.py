import pytest
import re
import yaml

from stopat30m import config


def test_local_secrets_and_environment_precedence(tmp_path, monkeypatch):
    base = tmp_path / "config.yaml"
    base.write_text('llm:\n  openai_api_key: ""\n  model: base-model\nauth:\n  single_user: true\n')
    (tmp_path / "config.local.yaml").write_text("llm:\n  openai_api_key: local-test-value\n")
    monkeypatch.setattr(config, "_DEFAULT_CONFIG_PATH", base)
    monkeypatch.setattr(config, "_config_cache", None)
    assert config.load_config()["llm"] == {"openai_api_key": "local-test-value", "model": "base-model"}
    monkeypatch.setattr(config, "_config_cache", None)
    monkeypatch.setenv("STOPAT30M_LLM__OPENAI_API_KEY", "environment-test-value")
    assert config.load_config()["llm"]["openai_api_key"] == "environment-test-value"
    assert config.load_config()["auth"]["single_user"] is True


def test_parser_error_does_not_quote_secret_content(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text('secret_key: ["private-test-value"\n')
    with pytest.raises(ValueError) as error:
        config._read_config(path)
    assert "broken.yaml" in str(error.value)
    assert "private-test-value" not in str(error.value)


def test_public_default_config_has_no_credentials():
    defaults = yaml.safe_load((config.PROJECT_ROOT / "config.yaml").read_text())
    violations = []

    def check(mapping, prefix=""):
        if not isinstance(mapping, dict):
            return
        for key, value in mapping.items():
            name = f"{prefix}.{key}" if prefix else key
            if re.search(r"(?:api_key|token|password|secret_key)$", str(key), re.I) and isinstance(value, str) and value.strip():
                violations.append(name)
            check(value, name)

    check(defaults)
    # Report key paths, never credential values, even when this check fails.
    assert not violations, f"Use local config or environment for: {violations}"


def test_explicit_config_file_remains_active_after_a_web_settings_save(tmp_path, monkeypatch):
    from stopat30m.settings_store import SettingsStore
    base = tmp_path / "personal.yaml"
    base.write_text('llm:\n  model: "openai/before"\n')
    monkeypatch.setattr(config, "_DEFAULT_CONFIG_PATH", config._DEFAULT_CONFIG_PATH)
    monkeypatch.setattr(config, "_config_cache", None)
    config.load_config(base)
    store = SettingsStore(environ={})
    assert store.local_path == tmp_path / "personal.local.yaml"
    store.update(store.get()["revision"], {"llm.model": "openai/after"})
    assert config.get("llm", "model") == "openai/after"
    assert base.read_text() == 'llm:\n  model: "openai/before"\n'
