"""Nullable metrics must remain unknown in CLI and legacy Streamlit views."""
import ast
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from click.testing import CliRunner

from stopat30m.formatting import format_metric, metric_number


@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), -float("inf"), Decimal("NaN"), "not measured", True])
def test_missing_or_nonfinite_measurement_is_never_zero_or_a_rating(value):
    assert format_metric(value) == "—"
    assert metric_number(value) is None


def test_zero_percent_money_and_integer_counts_preserve_meaning():
    assert format_metric(0, ".2%") == "0.00%"
    assert format_metric(1234.5, ",.2f", prefix="¥") == "¥1,234.50"
    assert format_metric(12, "d") == "12"
    assert format_metric(None, ",.2f", prefix="¥") == "—"


@pytest.mark.parametrize("command", ["backtest", "account-backtest", "signal-backtest"])
def test_cli_prints_nullable_metrics_and_finishes(command, monkeypatch, tmp_path):
    import main
    from stopat30m.backtest import common, engine, account_backtest, signal_backtest
    monkeypatch.setattr(common, "load_prediction_source", lambda **kw: (pd.Series([1.0]), None, {}))
    missing = {key: None for key in ["annual_return", "sharpe", "sortino", "max_drawdown", "calmar",
                                    "win_rate", "profit_loss_ratio", "ic_5d_mean", "rank_ic_5d_mean"]}
    missing.update({"total_trades": 0, "observation_days": 2})
    result = SimpleNamespace(metrics=missing, report=missing, summary_metrics=missing,
                             config={"test_days": 2}, save=lambda *a, **kw: tmp_path)
    class FakeEngine:
        top_k, method, rebalance_freq, eval_horizon = 1, "top_k", 5, 5
        def __init__(self, **kw):
            pass
        def run(self, pred):
            return result
    monkeypatch.setattr(engine, "BacktestEngine", FakeEngine)
    monkeypatch.setattr(account_backtest, "AccountBacktestEngine", FakeEngine)
    monkeypatch.setattr(signal_backtest, "SignalBacktestEngine", FakeEngine)
    output = CliRunner().invoke(main.cli, [command, "--pred-path", "synthetic.pkl"])
    assert output.exit_code == 0, output.output + str(output.exception)
    assert "—" in output.output
    assert str(tmp_path) in output.output


class FakeStreamlit:
    def __init__(self):
        self.events = []
        self.session_state = {}
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def __getattr__(self, name):
        def record(*args, **kw):
            self.events.append((name, args))
            if name == "columns":
                return [self] * (args[0] if isinstance(args[0], int) else len(args[0]))
            if name in {"spinner", "expander"}:
                return self
        return record


def dashboard_functions(st, **extra):
    """Run the actual function bodies without importing/booting Streamlit."""
    source = Path(__file__).resolve().parents[1] / "stopat30m/web/dashboard.py"
    wanted = {"_rate", "_show_ic_assessment", "_show_backtest_assessment", "page_model_eval",
              "_get_prices_and_names", "_invalidate_price_cache", "_refresh_paper_quotes", "_quick_sell_paper"}
    tree = ast.parse(source.read_text())
    module = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted], type_ignores=[])
    namespace = {"st": st, "format_metric": format_metric, "metric_number": metric_number,
                 "pd": pd, "json": json, **extra}
    exec(compile(module, str(source), "exec"), namespace)
    return namespace


def test_legacy_dashboard_renders_unknown_metrics_and_refuses_missing_sample_rating(tmp_path):
    st = FakeStreamlit()
    (tmp_path / "backtest").mkdir()
    (tmp_path / "backtest/report.json").write_text(json.dumps({"sharpe": None, "calmar": None,
        "sortino": None, "profit_loss_ratio": None, "annual_return": 0.0, "max_drawdown": 0.0, "win_rate": 0.0}))
    (tmp_path / "metrics.json").write_text(json.dumps({"IC_mean": None, "RankIC_mean": None}))
    funcs = dashboard_functions(st, OUTPUT_DIR=tmp_path, _list_run_dirs=lambda kind: [], _show_actual_vs_backtest=lambda path: None)
    funcs["page_model_eval"]()
    metrics = [args for event, args in st.events if event == "metric"]
    assert ("夏普比率", "—") in metrics
    assert ("盈利日占比", "0.00%") in metrics
    assert ("成交笔数", "—") in metrics
    assert sum("无法综合评级" in args[0] for event, args in st.events if event == "info") == 2
    assert not any(event == "success" for event, args in st.events)


def test_legacy_session_price_cannot_bypass_central_quote_freshness(monkeypatch):
    from stopat30m.data import realtime
    st = FakeStreamlit()
    st.session_state["shared_prices"] = {"SH600000": 1}
    values = iter([{"SH600000": 10}, {}])
    calls = []
    def prices(inst):
        calls.append(inst)
        return next(values)
    monkeypatch.setattr(realtime, "fetch_spot_prices", prices)
    monkeypatch.setattr(realtime, "fetch_stock_names", lambda inst: {"SH600000": "test"})
    invalidations = []
    monkeypatch.setattr(realtime, "invalidate_cache", lambda: invalidations.append(True))
    funcs = dashboard_functions(st)
    assert funcs["_get_prices_and_names"](["600000"])[0] == {"SH600000": 10}
    assert funcs["_get_prices_and_names"](["600000"])[0] == {}
    assert len(calls) == 2
    assert "shared_prices" not in st.session_state
    funcs["_invalidate_price_cache"]()
    assert invalidations == [True]
    assert "shared_names" not in st.session_state



def test_legacy_paper_sell_preserves_source_time_and_does_not_extend_old_quote(monkeypatch, tmp_path):
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    from stopat30m.data import realtime
    from stopat30m.trading.broker import paper
    from stopat30m.trading.models import Order, OrderDirection, OrderType
    now = [datetime(2026, 10, 16, 10, tzinfo=ZoneInfo("Asia/Shanghai"))]
    monkeypatch.setattr(paper, "_now", lambda: now[0].isoformat())
    monkeypatch.setattr(paper, "_today", lambda: now[0].date())
    broker = paper.PaperBroker.init_account(10000, tmp_path)
    broker.update_prices({"SH600000": 10})  # Explicit offline setup price.
    broker.submit_order(Order("SH600000", OrderDirection.BUY, OrderType.MARKET, 100))
    now[0] = datetime(2026, 10, 19, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
    source_time = (now[0] - timedelta(seconds=100)).isoformat()
    quote = {"SH600000": {"price": 10, "as_of": source_time, "is_realtime": True, "status": "available"}}
    monkeypatch.setattr(realtime, "fetch_spot_quotes", lambda codes: quote)
    st = FakeStreamlit()
    funcs = dashboard_functions(st)
    assert funcs["_refresh_paper_quotes"](broker, ["SH600000"]) == {"SH600000": 10}
    assert broker._price_timestamps["SH600000"] == source_time
    now[0] += timedelta(seconds=31)
    funcs["_quick_sell_paper"](broker, "SH600000", 100, "MARKET", None)
    assert len(broker.get_fills()) == 1  # The stale sell never executes.
    assert broker.get_positions()["SH600000"].quantity == 100
    assert broker._get_market_price("SH600000") is None
    assert any(event == "error" and "实时价格" in args[0] for event, args in st.events)
