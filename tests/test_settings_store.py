"""Local-only settings persistence and secret-boundary regressions."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import stat

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
import yaml

from stopat30m.settings_store import SettingsError, SettingsStore

FIXTURE_SECRET = "offline-fixture-credential-never-use"


@pytest.fixture
def store(tmp_path):
    path = tmp_path / "config.yaml"
    base = {"llm": {"enabled": True, "model": "openai/fixture-model", "openai_api_key": ""},
        "auth": {"secret_key": ""}, "backtest": {"top_k": 10, "rebalance_freq": 5, "buy_cost": .0003, "sell_cost": .0013},
        "model": {"type": "lgbm", "params": {"lgbm": {"learning_rate": .05, "num_leaves": 31}}},
        "factors": {"base": "alpha158", "enable_momentum": True},
        "data": {"train_start": "2014-01-01", "train_end": "2018-12-31", "valid_start": "2019-01-01", "valid_end": "2020-12-31", "test_start": "2021-01-01"}}
    path.write_text(yaml.safe_dump(base))
    return SettingsStore(path, environ={})


def fields(response):
    return {item["key"]: item for group in response["groups"] for item in group["fields"]}


def save(store, values, clear=None, revision=None):
    return store.update(revision or store.get()["revision"], values, clear or [], user_id=7, username="fixture-admin")


def test_persistent_reload_atomic_permissions_audit_and_no_public_file_write(store, monkeypatch):
    import stopat30m.config as config_mod
    original = store.base_path.read_bytes()
    store.local_path.write_text(yaml.safe_dump({"llm": {"openai_api_key": FIXTURE_SECRET}, "custom_private": {"credential": FIXTURE_SECRET}}))
    monkeypatch.setattr(config_mod, "_config_cache", {"fixture": "old"})
    before = store.get()
    item = fields(before)["llm.openai_api_key"]
    assert item["value"] is None and item["configured"] is True and item["source"] == "local"
    updated = save(store, {"research.min_interval": 5.0, "llm.model": "openai/another-fixture"})
    assert updated["revision"] != before["revision"]
    assert updated["changed_keys"] == ["llm.model", "research.min_interval"]
    assert config_mod._config_cache is None
    reloaded = SettingsStore(store.base_path, environ={})
    assert fields(reloaded.get())["research.min_interval"]["value"] == 5
    actual = yaml.safe_load(store.local_path.read_text())
    assert actual["llm"]["openai_api_key"] == FIXTURE_SECRET
    assert actual["custom_private"]["credential"] == FIXTURE_SECRET
    assert store.base_path.read_bytes() == original
    assert stat.S_IMODE(store.local_path.stat().st_mode) == 0o600
    serialized = json.dumps({"view": reloaded.get(), "audit": reloaded.audit()})
    assert FIXTURE_SECRET not in serialized
    audit = reloaded.audit()[0]
    assert audit["user_id"] == 7 and audit["username"] == "fixture-admin" and audit["secret_keys"] == []
    assert audit["revision"] == updated["revision"]


def test_secret_empty_preserves_rotation_and_explicit_clear_overrides_base(store):
    base = yaml.safe_load(store.base_path.read_text())
    base["llm"]["openai_api_key"] = FIXTURE_SECRET
    store.base_path.write_text(yaml.safe_dump(base))
    before = store.get()
    unchanged = save(store, {"llm.openai_api_key": ""})
    assert unchanged["revision"] == before["revision"] and unchanged["changed_keys"] == []
    assert not store.local_path.exists()
    changed = save(store, {"llm.deepseek_api_key": FIXTURE_SECRET})
    assert "llm.deepseek_api_key" in changed["restart_required"]
    assert store.audit()[0]["secret_keys"] == ["llm.deepseek_api_key"]
    cleared = save(store, {}, ["llm.openai_api_key"])
    assert fields(cleared)["llm.openai_api_key"]["configured"] is False
    assert yaml.safe_load(store.local_path.read_text())["llm"]["openai_api_key"] == ""
    assert FIXTURE_SECRET not in json.dumps(cleared)
    assert FIXTURE_SECRET not in json.dumps(store.audit())


def test_search_preferences_are_private_reset_the_service_and_retain_blank_keys(store, monkeypatch):
    from stopat30m.analysis import search_service
    resets = []
    monkeypatch.setattr(search_service, "reset_search_service", lambda: resets.append(True))
    base = store.base_path.read_bytes()
    view = save(store, {"search.serpapi_keys": FIXTURE_SECRET, "search.searxng_base_urls": "https://search.example.invalid", "search.news_strategy_profile": "medium"})
    assert resets == [True]
    assert fields(view)["search.serpapi_keys"]["configured"]
    assert fields(view)["search.serpapi_keys"]["value"] is None
    assert fields(view)["search.searxng_public_instances_enabled"]["value"] is False
    assert not any(key.startswith("search.") for key in view["restart_required"])
    assert FIXTURE_SECRET not in json.dumps({"view": view, "audit": store.audit()})
    assert store.base_path.read_bytes() == base
    again = save(store, {"search.serpapi_keys": ""})
    assert fields(again)["search.serpapi_keys"]["configured"] and resets == [True]
    clear = save(store, {}, ["search.serpapi_keys"])
    assert not fields(clear)["search.serpapi_keys"]["configured"] and resets == [True, True]


@pytest.mark.parametrize("value", ["https://user:private-value@example.invalid", "https://example.invalid?token=private-value", "https://example.invalid, ftp://example.invalid", "https://example.invalid,"])
def test_private_searxng_urls_reject_credentials_queries_and_empty_entries(store, value):
    with pytest.raises(SettingsError) as exc:
        save(store, {"search.searxng_base_urls": value})
    assert "private-value" not in str(exc.value)
    assert not store.local_path.exists()


def test_protocol_is_explicit_and_base_url_cannot_contain_generation_endpoint(store):
    changed = save(store, {"llm.api_type": "responses"})
    assert fields(changed)["llm.api_type"]["value"] == "responses"
    before = store.local_path.read_bytes()
    for values in ({"llm.api_type": "anything"}, {"llm.model": "deepseek/fixture"}, {"llm.openai_base_url": "https://example.invalid/v1/responses"}, {"llm.openai_base_url": "https://example.invalid/v1/chat/completions"}):
        with pytest.raises(SettingsError):
            save(store, values)
        assert store.local_path.read_bytes() == before


def test_revision_conflict_and_concurrent_writers_preserve_integrity(store):
    revision = store.get()["revision"]
    def attempt(value):
        try:
            return save(SettingsStore(store.base_path, environ={}), {"research.min_interval": value}, revision=revision)
        except SettingsError as exc:
            return exc
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, [4, 5]))
    assert sum(isinstance(result, SettingsError) for result in results) == 1
    conflict = next(result for result in results if isinstance(result, SettingsError))
    assert conflict.status_code == 409
    assert fields(store.get())["research.min_interval"]["value"] in {4, 5}
    assert len(store.audit()) == 1
    assert stat.S_IMODE(store.local_path.stat().st_mode) == 0o600


@pytest.mark.parametrize("blank", [" ", "   ", "\u3000"])
def test_whitespace_secret_cannot_erase_an_existing_credential(store, blank):
    save(store, {"llm.openai_api_key": FIXTURE_SECRET})
    original = store.local_path.read_bytes()
    with pytest.raises(SettingsError):
        save(store, {"research.min_interval": 5, "llm.openai_api_key": blank})
    assert store.local_path.read_bytes() == original
    assert fields(store.get())["llm.openai_api_key"]["configured"] is True
    assert len(store.audit()) == 1


@pytest.mark.parametrize("values", [{"auth.secret_key": "submitted-fixture"}, {"storage.url": "anything"},
    {"trading.paper_trading": False}, {"trading.gateway": "anything"}, {"research.min_interval": 1},
    {"research.max_attempts": True}, {"llm.temperature": float("nan")}, {"llm.timeout": float("inf")},
    {"llm.openai_base_url": "https://user:submitted-fixture@example.invalid/v1"},
    {"backtest.buy_cost": 2}, {"risk.max_daily_loss": .5}, {"trading.max_position_pct": .8},
    {"llm.openai_api_key": {"submitted-fixture": "not-a-string"}}])
def test_invalid_inputs_have_no_writes_and_never_echo_submitted_secrets(store, values):
    base = store.base_path.read_bytes()
    with pytest.raises(SettingsError) as exc:
        save(store, values)
    assert "submitted-fixture" not in str(exc.value)
    assert not store.local_path.exists()
    assert store.base_path.read_bytes() == base
    assert store.audit() == []


def test_environment_fields_are_read_only_invalid_overrides_are_rejected(store):
    store.environ.update({"STOPAT30M_RESEARCH__MIN_INTERVAL": "8", "OPENAI_API_KEY": FIXTURE_SECRET})
    response = store.get()
    items = fields(response)
    assert items["research.min_interval"]["value"] == 8
    assert items["research.min_interval"]["read_only"] and items["research.min_interval"]["source"] == "environment"
    assert items["llm.openai_api_key"]["configured"] and items["llm.openai_api_key"]["value"] is None
    assert FIXTURE_SECRET not in json.dumps(response)
    for values, clear in (({"research.min_interval": 8}, []), ({}, ["llm.openai_api_key"])):
        with pytest.raises(SettingsError, match="read-only"):
            save(store, values, clear)
    assert not store.local_path.exists()
    store.environ["STOPAT30M_RESEARCH__MIN_INTERVAL"] = "nan"
    with pytest.raises(SettingsError) as exc:
        store.get()
    assert FIXTURE_SECRET not in str(exc.value)
    assert not store.local_path.exists()


def test_auth_secret_initialization_is_atomic_and_does_not_overwrite_existing_values(store):
    assert store.ensure_auth_secret() is True
    local = yaml.safe_load(store.local_path.read_text())
    generated = local["auth"]["secret_key"]
    assert isinstance(generated, str) and len(generated) >= 48
    assert store.ensure_auth_secret() is False
    assert yaml.safe_load(store.local_path.read_text())["auth"]["secret_key"] == generated
    assert generated not in json.dumps(store.audit())
    assert generated not in json.dumps(store.get())
    store.environ["STOPAT30M_AUTH__SECRET_KEY"] = FIXTURE_SECRET
    assert store.ensure_auth_secret() is False
    assert yaml.safe_load(store.local_path.read_text())["auth"]["secret_key"] == generated
    store.environ["STOPAT30M_AUTH__SECRET_KEY"] = ""
    with pytest.raises(SettingsError, match="empty"):
        store.ensure_auth_secret()
    assert yaml.safe_load(store.local_path.read_text())["auth"]["secret_key"] == generated


def test_legacy_model_updates_use_same_local_store_audit_env_and_split_validation(store):
    save(store, {"llm.openai_api_key": FIXTURE_SECRET})
    original_base = store.base_path.read_bytes()
    result = store.update_legacy({"model": {"params": {"lgbm": {"learning_rate": .1}}},
        "backtest": {"top_k": 15}}, user_id=7, username="fixture-admin")
    assert result["status"] == "ok"
    local = yaml.safe_load(store.local_path.read_text())
    assert local["llm"]["openai_api_key"] == FIXTURE_SECRET
    assert local["model"]["params"]["lgbm"]["learning_rate"] == .1
    assert store.base_path.read_bytes() == original_base
    before = store.local_path.read_bytes()
    for payload in ({"auth": {"secret_key": "submitted-fixture"}}, {"model": {"unknown": 1}},
                    {"data": {"train_end": "2021-01-01"}}, {"model": {"params": {"lgbm": {"learning_rate": float("nan")}}}}):
        with pytest.raises(SettingsError):
            store.update_legacy(payload)
        assert store.local_path.read_bytes() == before
    store.environ["STOPAT30M_BACKTEST__TOP_K"] = "20"
    with pytest.raises(SettingsError, match="read-only"):
        store.update_legacy({"backtest": {"top_k": 10}})
    assert FIXTURE_SECRET not in json.dumps(store.audit())


def test_admin_http_settings_and_research_preferences_no_secret_echo(store, monkeypatch):
    from types import SimpleNamespace
    from fastapi import HTTPException
    from stopat30m.api.v1.endpoints import settings, research
    from stopat30m.auth.deps import get_current_user
    from stopat30m import settings_store
    app = FastAPI()
    app.include_router(settings.router, prefix="/api/v1")
    app.include_router(research.router, prefix="/api/v1")
    app.dependency_overrides[settings.get_settings_store] = lambda: store
    def unauthorized():
        raise HTTPException(status_code=401, detail="Missing authentication")
    app.dependency_overrides[get_current_user] = unauthorized
    class Workspace:
        def start_download(self, **options):
            return options
    app.dependency_overrides[research.get_workspace] = Workspace
    monkeypatch.setattr(settings_store, "research_preferences", lambda: {"min_interval": 8, "max_attempts": 2, "request_timeout": 11})
    with TestClient(app) as client:
        assert client.get("/api/v1/system/settings").status_code == 401
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=7, username="fixture", role="user")
        assert client.get("/api/v1/system/settings/audit").status_code == 403
        app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=7, username="fixture", role="admin")
        response = client.get("/api/v1/system/settings")
        assert response.status_code == 200
        revision = response.json()["revision"]
        response = client.put("/api/v1/system/settings", json={"revision": revision,
            "values": {"llm.openai_api_key": FIXTURE_SECRET}, "clear_secrets": []})
        assert response.status_code == 200 and FIXTURE_SECRET not in response.text
        assert FIXTURE_SECRET not in client.get("/api/v1/system/settings/audit").text
        response = client.put("/api/v1/system/settings", json=FIXTURE_SECRET)
        assert response.status_code == 400 and FIXTURE_SECRET not in response.text
        response = client.post("/api/v1/research/download", json={})
        assert response.status_code == 202
        assert response.json()["min_interval"] == 8
        response = client.post("/api/v1/research/download", json={"min_interval": 3, "max_attempts": 4, "request_timeout": 15})
        assert response.json()["min_interval"] == 3


def test_runtime_config_cache_reload_restart_scope_and_atomic_failure(store, monkeypatch):
    from stopat30m import config as config_mod, settings_store
    monkeypatch.setattr(config_mod, "_DEFAULT_CONFIG_PATH", store.base_path)
    monkeypatch.setattr(config_mod, "_config_cache", None)
    assert config_mod.load_config()["llm"]["model"] == "openai/fixture-model"
    saved = save(store, {"llm.model": "openai/new-fixture", "llm.deepseek_api_key": FIXTURE_SECRET})
    assert config_mod.get("llm", "model") == "openai/new-fixture"
    assert "llm.deepseek_api_key" in saved["restart_required"]
    monkeypatch.setattr(settings_store, "_PROCESS_ID", "simulated-new-process")
    assert store.get()["restart_required"] == []
    original = store.local_path.read_bytes()
    old_audit = store.audit()
    def failed_replace(*args):
        raise OSError("synthetic atomic replace failure")
    monkeypatch.setattr(type(store.local_path), "replace", failed_replace)
    with pytest.raises(SettingsError) as exc:
        save(store, {"research.min_interval": 6})
    assert FIXTURE_SECRET not in str(exc.value)
    assert store.local_path.read_bytes() == original and store.audit() == old_audit
    assert not list(store.local_path.parent.glob(".settings-*"))


def test_legacy_http_endpoint_uses_local_preserves_credentials_and_records_actor(store, monkeypatch):
    from types import SimpleNamespace
    from stopat30m.api.v1.endpoints import system
    from stopat30m.auth.deps import get_current_user
    from stopat30m import settings_store
    save(store, {"llm.openai_api_key": FIXTURE_SECRET})
    original = store.base_path.read_bytes()
    monkeypatch.setattr(settings_store, "SettingsStore", lambda **kwargs: store)
    app = FastAPI()
    app.include_router(system.router, prefix="/api/v1")
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=12, username="legacy-admin", role="admin")
    with TestClient(app) as client:
        response = client.put("/api/v1/system/model-config", json={"backtest": {"top_k": 25}})
        assert response.status_code == 200 and FIXTURE_SECRET not in response.text
        response = client.put("/api/v1/system/model-config", json=FIXTURE_SECRET)
        assert response.status_code == 400 and FIXTURE_SECRET not in response.text
    assert store.base_path.read_bytes() == original
    local = yaml.safe_load(store.local_path.read_text())
    assert local["llm"]["openai_api_key"] == FIXTURE_SECRET and local["backtest"]["top_k"] == 25
    assert store.audit()[0]["user_id"] == 12 and store.audit()[0]["username"] == "legacy-admin"
