"""Request and ownership regressions without executing a backtest or opening Qlib."""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from stopat30m.api.v1.endpoints import backtest as api
from stopat30m.storage.models import Base, BacktestRun


@pytest.fixture(autouse=True)
def reset_tasks():
    api._tasks.clear()
    yield
    api._tasks.clear()


@pytest.mark.parametrize("kwargs", [{"top_k": 0}, {"rebalance_freq": -1}, {"cash_reserve_pct": 1},
                                    {"initial_capital": -1}, {"method": "long_short"},
                                    {"slippage_bps": float("nan")}, {"tag": "../../escape"}])
def test_invalid_backtest_parameters_are_rejected(kwargs):
    with pytest.raises(ValidationError):
        api.BacktestRequest(kind="account", **kwargs)


def test_tasks_are_only_visible_to_owner():
    api._tasks["other"] = {"task_id": "other", "status": "running", "user_id": 2}
    user = SimpleNamespace(id=1, role="user")
    assert api.get_active_task(user)["status"] == "idle"
    with pytest.raises(HTTPException) as exc:
        api.get_backtest_task("other", user)
    assert exc.value.status_code == 404


def test_other_user_cannot_read_result_or_chart_and_legacy_rows_are_not_shared():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    user = SimpleNamespace(id=1, role="user")
    with Session(engine) as db:
        other = BacktestRun(kind="backtest", user_id=2)
        legacy = BacktestRun(kind="backtest", user_id=None)
        db.add_all([other, legacy])
        db.flush()
        for run_id in [other.id, legacy.id]:
            with pytest.raises(HTTPException) as exc:
                api.get_run_detail(run_id, user, db)
            assert exc.value.status_code == 404
        assert api.list_runs(limit=20, user=user, db=db) == []


def test_arbitrary_pickle_path_is_not_accepted(tmp_path, monkeypatch):
    outside = tmp_path / "outside.pkl"
    outside.write_bytes(b"not unpickled")
    monkeypatch.setattr(api, "MODEL_ROOT", tmp_path / "models")
    request = api.BacktestRequest(kind="backtest", model_path=str(outside))
    with pytest.raises(HTTPException) as exc:
        api.submit_backtest(request, SimpleNamespace(id=1))
    assert exc.value.status_code == 422


def test_basic_api_passes_selected_benchmark_and_keeps_metadata(monkeypatch, tmp_path):
    from stopat30m.backtest import engine
    captured = {}
    config = {}
    class FakeEngine:
        def __init__(self, **kwargs):
            captured.update(kwargs)
        def run(self, pred):
            return SimpleNamespace(config=config, metrics={}, save=lambda: tmp_path)
    monkeypatch.setattr(engine, "BacktestEngine", FakeEngine)
    monkeypatch.setattr(api, "_persist_run", lambda **kw: kw)
    result = api._run_basic_backtest("task", object(), {"benchmark": "SH000001"}, {"model_path": "model.pkl"}, 1, lambda *a: None)
    assert captured["benchmark"] == "SH000001"
    assert result["config"]["source_metadata"] == {"model_path": "model.pkl"}
