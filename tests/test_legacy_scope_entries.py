"""Legacy configuration and jobs cannot silently widen personal A-share scope."""
import builtins
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pandas as pd
import pytest
import yaml

import main
from stopat30m.settings_store import SettingsStore


@pytest.fixture
def legacy_api(tmp_path, monkeypatch):
    from stopat30m import settings_store
    from stopat30m.api.v1.endpoints import system
    from stopat30m.auth.deps import get_current_user
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({"data": {"universe": "csi300"}, "model": {"type": "lgbm"}}))
    store = SettingsStore(config, environ={})
    effective = {"universe": "csi300"}
    monkeypatch.setattr(settings_store, "SettingsStore", lambda **kwargs: store)
    monkeypatch.setattr(system, "get", lambda section, key=None, default=None: effective.get(key, default) if section == "data" else default)
    app = FastAPI()
    app.include_router(system.router, prefix="/api/v1")
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=1, username="scope-fixture", role="admin")
    with TestClient(app) as client:
        yield client, store, effective


@pytest.mark.parametrize("value", ["all", "csi500", "sz50", "union", "CSI300", "", None, ["csi300"], {"scope": "csi300"}])
def test_legacy_settings_api_rejects_unsupported_scope_without_writing_or_echoing_value(legacy_api, value):
    client, store, _ = legacy_api
    original = store.base_path.read_bytes()
    response = client.put("/api/v1/system/model-config", json={"data": {"universe": value}})
    assert response.status_code == 400
    assert "csi300" in response.json()["detail"] and "csi1000" in response.json()["detail"]
    assert store.base_path.read_bytes() == original and not store.local_path.exists()


@pytest.mark.parametrize("value", ["csi300", "csi1000"])
def test_allowed_scope_configuration_is_saved_without_rewriting_public_config_or_model_files(legacy_api, tmp_path, value):
    client, store, _ = legacy_api
    model = tmp_path / "existing-model.pkl"
    model.write_bytes(b"preserve existing model bytes")
    original = store.base_path.read_bytes()
    response = client.put("/api/v1/system/model-config", json={"data": {"universe": value}})
    assert response.status_code == 200
    assert yaml.safe_load(store.local_path.read_text())["data"]["universe"] == value
    assert store.base_path.read_bytes() == original and model.read_bytes() == b"preserve existing model bytes"


@pytest.mark.parametrize("value", ["all", "csi500", "csi300; false", "", None, ["csi300"]])
def test_direct_train_command_api_cannot_bypass_scope_validation(legacy_api, value):
    client, store, _ = legacy_api
    response = client.post("/api/v1/system/generate-train-command", json={"universe": value})
    assert response.status_code == 400 and "command" not in response.json()
    assert not store.local_path.exists()


def test_csi1000_configuration_does_not_unlock_unaudited_legacy_training(legacy_api):
    client, _, effective = legacy_api
    effective["universe"] = "csi1000"
    for payload in ({}, {"universe": "csi1000"}):
        response = client.post("/api/v1/system/generate-train-command", json=payload)
        assert response.status_code == 409 and "command" not in response.json()
        assert "不会自动改用 csi300" in response.json()["detail"]
    explicit = client.post("/api/v1/system/generate-train-command", json={"universe": "csi300"})
    assert explicit.status_code == 200 and "--universe csi300" in explicit.json()["command"]


def test_absent_api_universe_checks_effective_config_and_keeps_explicit_csi300_default(legacy_api):
    client, _, effective = legacy_api
    response = client.post("/api/v1/system/generate-train-command", json={})
    assert response.status_code == 200 and "--universe csi300" in response.json()["command"]
    effective["universe"] = "all"
    blocked = client.post("/api/v1/system/generate-train-command", json={})
    assert blocked.status_code == 400 and "command" not in blocked.json()


COMMANDS = {
    "train": [], "backtest": [], "cache-predictions": ["--model-path", "existing.pkl"],
    "signal-backtest": [], "account-backtest": [], "signal": ["--model-path", "existing.pkl"],
}


def block_model_runtime(monkeypatch):
    original = builtins.__import__
    blocked = {"stopat30m.data.provider", "stopat30m.factors.handler", "stopat30m.model.trainer",
        "stopat30m.backtest.common", "stopat30m.backtest.engine", "stopat30m.signal.generator"}
    def imported(name, *args, **kwargs):
        if name in blocked:
            pytest.fail("Rejected scope must fail before model or data runtime imports")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", imported)


@pytest.mark.parametrize("command", COMMANDS)
@pytest.mark.parametrize("scope", ["all", "csi500", "other", "csi1000"])
def test_cli_explicit_scope_cannot_bypass_gate_before_any_runtime_import(monkeypatch, command, scope):
    block_model_runtime(monkeypatch)
    result = CliRunner().invoke(main.cli, [command, *COMMANDS[command], "--universe", scope])
    assert result.exit_code == (1 if scope == "csi1000" else 2), result.output
    assert "不会自动改用 csi300" in result.output if scope == "csi1000" else "Invalid value for '--universe'" in result.output


@pytest.mark.parametrize("command", COMMANDS)
@pytest.mark.parametrize("effective", ["all", "csi500", "csi1000"])
def test_cli_omitted_scope_cannot_hide_invalid_config_or_environment(monkeypatch, command, effective):
    import stopat30m.config as config
    monkeypatch.setattr(config, "get", lambda section, key=None, default=None: effective if (section, key) == ("data", "universe") else default)
    block_model_runtime(monkeypatch)
    result = CliRunner().invoke(main.cli, [command, *COMMANDS[command]])
    assert result.exit_code == 1 and "csi300" in result.output, result.output


@pytest.mark.parametrize("metadata", [{}, {"universe": "all"}, {"universe": "csi500"}, {"universe": "csi1000"}])
def test_cached_prediction_metadata_cannot_bypass_requested_scope_or_start_new_backtest(tmp_path, monkeypatch, metadata):
    from stopat30m.backtest import common, engine
    cached = tmp_path / "existing-predictions.pkl"
    cached.write_bytes(b"preserve existing prediction bundle")
    monkeypatch.setattr(common, "load_prediction_source", lambda **kwargs: (pd.Series(dtype=float), None, metadata))
    monkeypatch.setattr(engine, "BacktestEngine", lambda **kwargs: pytest.fail("Unverified cached pool must not start a new backtest"))
    result = CliRunner().invoke(main.cli, ["backtest", "--pred-path", str(cached), "--universe", "csi300"])
    assert result.exit_code == 1, result.output
    assert cached.read_bytes() == b"preserve existing prediction bundle"


def test_existing_model_can_still_predict_only_the_explicit_csi300_subset(tmp_path, monkeypatch):
    from stopat30m.backtest import common
    model = tmp_path / "existing-model.pkl"
    model.write_bytes(b"unchanged existing model")
    prediction = pd.Series([0.1], index=pd.MultiIndex.from_tuples([(pd.Timestamp("2026-10-19"), "SH600000")], names=["datetime", "instrument"]))
    calls = []
    def load(**kwargs):
        calls.append(kwargs)
        return prediction, None, SimpleNamespace(instruments="csi300", num_features=1)
    monkeypatch.setattr(common, "load_model_predictions", load)
    monkeypatch.setattr(common, "save_prediction_bundle", lambda *args, **kwargs: tmp_path / "new-predictions.pkl")
    result = CliRunner().invoke(main.cli, ["cache-predictions", "--model-path", str(model), "--universe", "csi300"])
    assert result.exit_code == 0, result.output
    assert calls[0]["universe"] == "csi300" and model.read_bytes() == b"unchanged existing model"


@pytest.fixture
def backtest_http(tmp_path, monkeypatch):
    from stopat30m.api.v1.endpoints import backtest
    from stopat30m.auth.deps import get_current_user
    from stopat30m.backtest import common
    model_root, prediction_root = tmp_path / "models", tmp_path / "predictions"
    model_root.mkdir()
    prediction_root.mkdir()
    model = model_root / "existing.pkl"
    model.write_bytes(b"existing model must not be unpickled by request validation")
    cache = prediction_root / "existing.pkl"
    prediction = pd.Series([0.1], index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2024-01-02"), "SH600000")], names=["datetime", "instrument"]))
    def save_cache(metadata):
        common.save_prediction_bundle(prediction, metadata=metadata, path=cache)
    save_cache({"universe": "csi300"})
    monkeypatch.setattr(backtest, "MODEL_ROOT", model_root)
    monkeypatch.setattr(backtest, "PREDICTION_ROOT", prediction_root)
    effective, submitted = {"universe": "csi300"}, []
    monkeypatch.setattr(backtest, "get", lambda section, key=None, default=None: effective.get(key, default) if section == "data" else default)
    monkeypatch.setattr(backtest._executor, "submit", lambda *args: submitted.append(args))
    monkeypatch.setattr(common, "load_prediction_source", lambda **kwargs: pytest.fail("Rejected HTTP request must not load a model or data"))
    app = FastAPI()
    app.include_router(backtest.router, prefix="/api/v1")
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(id=1, username="scope-fixture", role="admin")
    backtest._tasks.clear()
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, api=backtest, common=common, effective=effective,
            model=model, cache=cache, save_cache=save_cache, submitted=submitted)
    backtest._tasks.clear()


@pytest.mark.parametrize("kind", ["backtest", "signal", "account"])
@pytest.mark.parametrize("scope", ["all", "csi500", "other", "csi1000"])
@pytest.mark.parametrize("from_config", [False, True])
def test_web_backtest_rejects_effective_unsupported_scope_before_scheduling_or_prediction_loading(backtest_http, kind, scope, from_config):
    fixture = backtest_http
    payload = {"kind": kind, "model_path": str(fixture.model)}
    if from_config:
        fixture.effective["universe"] = scope
    else:
        payload["universe"] = scope
    original = fixture.model.read_bytes()
    response = fixture.client.post("/api/v1/backtest/run", json=payload)
    assert response.status_code == (409 if scope == "csi1000" else 400)
    assert "csi300" in response.json()["detail"]
    assert fixture.submitted == [] and fixture.api._tasks == {}
    assert fixture.model.read_bytes() == original


@pytest.mark.parametrize("kind", ["backtest", "signal", "account"])
@pytest.mark.parametrize("metadata", [{}, {"universe": "all"}, {"universe": "csi500"}, {"universe": "csi1000"}])
def test_web_cached_predictions_require_declared_allowed_scope_before_model_or_data_loading(backtest_http, kind, metadata):
    fixture = backtest_http
    fixture.save_cache(metadata)
    original = fixture.cache.read_bytes()
    response = fixture.client.post("/api/v1/backtest/run", json={
        "kind": kind, "pred_path": str(fixture.cache), "universe": "csi300"})
    assert response.status_code == (409 if metadata.get("universe") == "csi1000" else 400)
    assert fixture.submitted == [] and fixture.api._tasks == {}
    assert fixture.cache.read_bytes() == original


@pytest.mark.parametrize("source", ["model", "cache"])
def test_web_allowed_scope_is_fixed_in_background_parameters_without_changing_existing_assets(backtest_http, source):
    fixture = backtest_http
    asset = fixture.model if source == "model" else fixture.cache
    original = asset.read_bytes()
    response = fixture.client.post("/api/v1/backtest/run", json={
        "kind": "backtest", "model_path" if source == "model" else "pred_path": str(asset)})
    assert response.status_code == 200
    _, task_id, params, user_id = fixture.submitted[0]
    assert params["universe"] == "csi300" and user_id == 1
    assert fixture.api._tasks[task_id]["status"] == "running"
    assert asset.read_bytes() == original


def test_background_worker_rechecks_asset_metadata_before_any_engine_after_cache_changes(backtest_http, monkeypatch):
    from stopat30m.data import provider
    fixture = backtest_http
    response = fixture.client.post("/api/v1/backtest/run", json={"kind": "backtest", "pred_path": str(fixture.cache)})
    assert response.status_code == 200
    _, task_id, params, user_id = fixture.submitted[0]
    fixture.save_cache({"universe": "all"})
    monkeypatch.setattr(provider, "init_qlib", lambda: None)
    calls = []
    def loaded(**kwargs):
        calls.append(kwargs)
        return fixture.common.load_prediction_bundle(kwargs["pred_path"])
    monkeypatch.setattr(fixture.common, "load_prediction_source", loaded)
    monkeypatch.setattr(fixture.api, "_run_basic_backtest", lambda *args: pytest.fail("Changed cache must not start an engine"))
    fixture.api._run_backtest_task(task_id, params, user_id)
    assert fixture.api._tasks[task_id]["status"] == "failed"
    assert "csi300" in fixture.api._tasks[task_id]["error"]
    assert calls[0]["universe"] == "csi300"
