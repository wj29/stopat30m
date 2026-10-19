"""Offline regressions for portfolio accounting and honest backtest reporting."""
import sys
import types

import numpy as np
import pandas as pd
import pytest

from stopat30m.backtest import common
from stopat30m.backtest.account_backtest import AccountBacktestEngine
from stopat30m.backtest.engine import BacktestEngine
from stopat30m.backtest.historical_broker import HistoricalBroker
from stopat30m.backtest.signal_backtest import SignalBacktestEngine
from stopat30m.model.evaluator import compute_portfolio_metrics


def test_roundoff_in_constant_returns_does_not_create_huge_sharpe():
    returns = pd.Series([0.002 + (index % 2) * 1e-16 for index in range(126)])
    metrics = compute_portfolio_metrics(returns)
    assert metrics["annual_volatility"] == 0
    assert metrics["sharpe"] is None
    assert metrics["sortino"] is None
from stopat30m.trading.models import Position


def predictions(dates, values):
    idx = pd.MultiIndex.from_product([dates, ["SH600000", "SZ000001"]], names=["datetime", "instrument"])
    return pd.Series(np.asarray(values).ravel(), index=idx)


def test_metrics_compound_returns_include_initial_loss_and_actual_day_count():
    dates = pd.date_range("2026-01-05", periods=2)
    ret = pd.Series([-0.1, 1 / 9], index=dates)
    metrics = compute_portfolio_metrics(ret, ret.copy(), risk_free_rate=0)
    assert metrics["total_return"] == pytest.approx(0)
    assert metrics["annual_return"] == pytest.approx(0)
    assert metrics["max_drawdown"] == -0.1
    assert metrics["excess_return"] == pytest.approx(0)
    assert metrics["observation_days"] == 2
    assert "total_trades" not in metrics


def test_missing_benchmark_is_not_silently_replaced_by_cash():
    dates = pd.date_range("2026-01-05", periods=2)
    metrics = compute_portfolio_metrics(pd.Series([0.01, -0.01], index=dates), pd.Series(dtype=float))
    assert metrics["benchmark_available"] is False
    assert "excess_return" not in metrics
    assert compute_portfolio_metrics(pd.Series([np.nan])) == {}


@pytest.mark.parametrize("order", [("instrument", "datetime"), ("datetime", "instrument")])
def test_basic_price_pivot_uses_qlib_named_levels(monkeypatch, order):
    date = pd.Timestamp("2026-01-05")
    entry = {"instrument": "SH600000", "datetime": date}
    idx = pd.MultiIndex.from_tuples([tuple(entry[key] for key in order)], names=order)
    frame = pd.DataFrame({"$close": [10.0]}, index=idx)
    qlib = types.ModuleType("qlib")
    qlib.data = types.ModuleType("qlib.data")
    qlib.data.D = types.SimpleNamespace(features=lambda **kw: frame)
    monkeypatch.setitem(sys.modules, "qlib", qlib)
    monkeypatch.setitem(sys.modules, "qlib.data", qlib.data)
    result = BacktestEngine._fetch_prices(["SH600000"], date, date)
    assert result.loc[date, "SH600000"] == 10


def test_basic_portfolio_drifts_without_free_rebalancing(monkeypatch):
    dates = pd.date_range("2026-01-05", periods=3)
    close = pd.DataFrame({"SH600000": [10, 20, 20], "SZ000001": [10, 10, 20]}, index=dates)
    opens = pd.DataFrame(10.0, index=dates, columns=close.columns)
    engine = BacktestEngine(top_k=2, rebalance_freq=5, buy_cost=0, sell_cost=0)
    monkeypatch.setattr(engine, "_fetch_prices", lambda *a: opens if a[-1] == "$open" else close)
    monkeypatch.setattr(engine, "_fetch_benchmark_close", lambda *a: pd.Series([10, 10, 10], index=dates))
    result = engine.run(predictions(dates, [[2, 1]] * 3))
    assert result.equity_curve.iloc[-1] == pytest.approx(2)
    assert result.portfolio_returns.iloc[-1] == pytest.approx(1 / 3)
    assert result.metrics["total_trades"] == 2
    assert result.config["limitations"]


def test_basic_refuses_missing_held_price_instead_of_fabricating_zero_return(monkeypatch):
    dates = pd.date_range("2026-01-05", periods=3)
    prices = pd.DataFrame({"SH600000": [10, 10, np.nan], "SZ000001": [10, 10, 10]}, index=dates)
    engine = BacktestEngine(top_k=1, rebalance_freq=5)
    monkeypatch.setattr(engine, "_fetch_prices", lambda *a: prices)
    monkeypatch.setattr(engine, "_fetch_benchmark_close", lambda *a: pd.Series(dtype=float))
    with pytest.raises(ValueError, match="Missing or invalid price"):
        engine.run(predictions(dates, [[2, 1]] * 3))


def test_snapshot_marks_positions_at_execution_not_future_close():
    broker = HistoricalBroker(1000)
    broker.get_account().positions["SH600000"] = Position("SH600000", quantity=100, current_price=10)
    broker.set_market_snapshot("2026-01-06", {"SH600000": 10}, {"SH600000": 20}, {}, {}, {}, {})
    assert broker.get_positions()["SH600000"].current_price == 10


def test_account_keeps_first_fees_aligns_benchmark_and_obeys_frequency(monkeypatch, tmp_path):
    from stopat30m.backtest import account_backtest as module
    monkeypatch.chdir(tmp_path)
    dates = pd.date_range("2026-01-05", periods=4)
    prices = pd.DataFrame(10.0, index=dates, columns=["SH600000", "SZ000001"])
    fields = {field: prices.copy() for field in ["$open", "$close", "$high", "$low"]}
    fields["$volume"] = prices * 1_000_000
    monkeypatch.setattr(module, "fetch_price_fields", lambda *a: fields)
    monkeypatch.setattr(module, "fetch_benchmark_close", lambda *a: pd.Series([10, 11, 12, 13], index=dates))
    engine = AccountBacktestEngine(initial_capital=100000, top_k=1, rebalance_freq=3, enable_risk_manager=False)
    result = engine.run(predictions(dates, [[2, 1], [1, 2], [1, 2], [1, 2]]))
    assert len(result.nav) == len(dates)
    assert result.nav.iloc[0]["equity"] == 100000
    assert result.nav.iloc[1]["daily_return"] < 0
    assert len(result.fills) == 1  # No daily sell-and-rebuy despite changed ranking.
    assert result.report["total_trades"] == 1
    assert result.report["benchmark_available"] is True
    assert result.report["excess_return"] < result.report["annual_return"]
    assert result.nav.iloc[-1]["equity_cumulative"] == pytest.approx(result.report["ending_equity"] / 100000)


@pytest.mark.parametrize("kind", [AccountBacktestEngine, SignalBacktestEngine])
def test_unsupported_strategy_is_rejected(kind):
    with pytest.raises(ValueError, match="top_k only"):
        kind(method="long_short")


def test_result_directories_are_unique_and_tag_cannot_escape(monkeypatch, tmp_path):
    monkeypatch.setattr(common, "BACKTEST_ROOT", tmp_path)
    a = common.create_run_dir("backtest", "../../escape")
    b = common.create_run_dir("backtest", "../../escape")
    assert a != b
    assert a.parent == tmp_path / "backtest"
    assert b.parent == a.parent


def test_switch_at_open_compounds_old_overnight_and_new_intraday(monkeypatch):
    dates = pd.date_range("2026-01-05", periods=3)
    close = pd.DataFrame({"SH600000": [10, 10, 11], "SZ000001": [10, 10, 12]}, index=dates)
    opens = pd.DataFrame({"SH600000": [10, 10, 11], "SZ000001": [10, 10, 10]}, index=dates)
    engine = BacktestEngine(top_k=1, rebalance_freq=1, buy_cost=0, sell_cost=0)
    monkeypatch.setattr(engine, "_fetch_prices", lambda *a: opens if a[-1] == "$open" else close)
    monkeypatch.setattr(engine, "_fetch_benchmark_close", lambda *a: pd.Series([10, 10, 10], index=dates))
    result = engine.run(predictions(dates, [[2, 1], [1, 2], [1, 2]]))
    assert result.equity_curve.iloc[-1] == pytest.approx(1.32)


def test_actual_slippage_and_fees_cannot_take_cash_negative():
    from stopat30m.trading.models import Order, OrderDirection, OrderType, OrderStatus
    broker = HistoricalBroker(1006, slippage_bps=100)
    broker.set_market_snapshot("2026-01-05", {"SH600000": 10}, {"SH600000": 10}, {}, {}, {"SH600000": 1e6}, {"SH600000": 10})
    order = broker.submit_order(Order("SH600000", OrderDirection.BUY, OrderType.MARKET, 100))
    assert order.status == OrderStatus.REJECTED
    assert broker.get_account().cash == 1006
    assert not broker.get_fills()


def test_buy_fees_are_part_of_cost_and_roundtrip_profit_reconciles_to_equity():
    from stopat30m.trading.models import Order, OrderDirection, OrderType
    broker = HistoricalBroker(10000)
    broker.set_market_snapshot("2026-01-05", {"SH600000": 10}, {"SH600000": 10}, {}, {}, {"SH600000": 1e6}, {})
    broker.submit_order(Order("SH600000", OrderDirection.BUY, OrderType.MARKET, 100))
    buy = broker.get_fills()[0]
    assert broker.get_positions()["SH600000"].total_cost == pytest.approx(1000 + buy.total_fee)
    broker.settle_day("2026-01-05")
    broker.set_market_snapshot("2026-01-06", {"SH600000": 11}, {"SH600000": 11}, {}, {}, {"SH600000": 1e6}, {})
    broker.submit_order(Order("SH600000", OrderDirection.SELL, OrderType.MARKET, 100))
    account = broker.get_account()
    assert not broker.get_positions()
    assert account.realized_pnl == pytest.approx(account.equity - account.initial_capital)


def test_risk_tracks_nonrebalance_peaks_and_audits_pretrade_rejections(monkeypatch, tmp_path):
    from stopat30m.backtest import account_backtest as module
    from stopat30m.trading.risk import RiskManager
    monkeypatch.chdir(tmp_path)
    dates = pd.bdate_range("2026-01-05", periods=7)
    close = pd.DataFrame({"SH600000": [10, 10, 11, 12.1, 10.89, 9.8, 9.8], "SZ000001": [10] * 7}, index=dates)
    opens = pd.DataFrame({"SH600000": [10, 10, 10, 11, 12.1, 10.89, 9.8], "SZ000001": [10] * 7}, index=dates)
    fields = {"$open": opens, "$close": close, "$high": pd.DataFrame(20., index=dates, columns=close.columns),
              "$low": pd.DataFrame(1., index=dates, columns=close.columns), "$volume": close * 1e6}
    monkeypatch.setattr(module, "fetch_price_fields", lambda *a: fields)
    monkeypatch.setattr(module, "fetch_benchmark_close", lambda *a: pd.Series(10., index=dates))
    monkeypatch.setattr(module, "RiskManager", lambda **kw: RiskManager(max_concentration=1, max_position_pct=1,
                        max_total_position_pct=1, max_drawdown=.15, max_daily_loss=1))
    result = AccountBacktestEngine(initial_capital=100000, top_k=1, rebalance_freq=5).run(
        predictions(dates, [[2, 1]] * 5 + [[1, 2]] * 2))
    rejected = result.orders[result.orders["status"] == "REJECTED"]
    assert len(rejected) == 1
    assert "drawdown" in rejected.iloc[0]["reject_reason"]
    assert result.report["order_count"] == 3
    assert result.report["rejected_orders"] == 1
    assert result.report["fill_count"] == 2  # Entry and exit; new risk is blocked.
    assert (result.risk_events["event_type"] == "ORDER_REJECTED").sum() == 1
    assert len(result.turnover) == 6
    assert result.report["avg_daily_turnover"] == pytest.approx(round(result.turnover["turnover"].sum() / 6, 2))
