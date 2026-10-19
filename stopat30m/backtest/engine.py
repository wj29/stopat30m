"""
Lightweight backtest engine for alpha model validation.

Simulates a top-K equal-weight portfolio on the test segment of model
predictions.  Supports realistic execution at OPEN prices to account
for the signal-to-execution gap (signal generated at close, earliest
execution at next open).

Usage:
    engine = BacktestEngine()       # deal_price="open" by default
    result = engine.run(predictions)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from stopat30m.config import get
from stopat30m.model.evaluator import compute_portfolio_metrics
from stopat30m.backtest.common import BACKTEST_LIMITATIONS, create_run_dir


@dataclass
class BacktestResult:
    """Container for all backtest outputs."""

    portfolio_returns: pd.Series
    benchmark_returns: pd.Series
    equity_curve: pd.Series
    benchmark_curve: pd.Series
    trades: pd.DataFrame
    positions: pd.DataFrame
    metrics: dict[str, Any]
    config: dict = field(default_factory=dict)

    def save(self, output_dir: str | Path | None = None) -> Path:
        """Persist backtest results to disk."""
        out = Path(output_dir) if output_dir is not None else create_run_dir("backtest")
        out.mkdir(parents=True, exist_ok=True)

        returns_df = pd.DataFrame({
            "portfolio": self.portfolio_returns,
            "benchmark": self.benchmark_returns,
            "portfolio_cumulative": self.equity_curve,
            "benchmark_cumulative": self.benchmark_curve,
        })
        returns_df.index.name = "date"
        returns_df.to_csv(out / "returns.csv")

        if not self.trades.empty:
            self.trades.to_csv(out / "trades.csv", index=False)

        if not self.positions.empty:
            self.positions.to_csv(out / "positions.csv", index=False)

        report = {**self.metrics, "config": self.config}
        with open(out / "report.json", "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Backtest results saved to {out}")
        return out


class BacktestEngine:
    """Top-K equal-weight backtest on model predictions.

    On each rebalance date (every ``rebalance_freq`` trading days),
    selects the top-K instruments by predicted score and rebalances.

    Execution model (controlled by ``deal_price``):
      - "open":  trades execute at the rebalance day's OPEN price.
                  This is realistic — signal at T-1 close, trade at T open.
      - "close": trades execute at the previous day's CLOSE price.
                  This is optimistic and overstates returns.
    """

    VALID_DEAL_PRICES = ("open", "close")

    def __init__(
        self,
        top_k: int | None = None,
        rebalance_freq: int | None = None,
        buy_cost: float | None = None,
        sell_cost: float | None = None,
        benchmark: str | None = None,
        deal_price: str | None = None,
    ):
        cfg = get("backtest") or {}
        self.top_k = top_k if top_k is not None else cfg.get("top_k", 20)
        self.rebalance_freq = rebalance_freq if rebalance_freq is not None else cfg.get("rebalance_freq", 5)
        self.buy_cost = buy_cost if buy_cost is not None else cfg.get("buy_cost", 0.0003)
        self.sell_cost = sell_cost if sell_cost is not None else cfg.get("sell_cost", 0.0013)
        self.benchmark = benchmark or cfg.get("benchmark") or get("data", "benchmark", "SH000300")
        self.deal_price = deal_price or cfg.get("deal_price", "open")
        if self.top_k < 1 or self.rebalance_freq < 1:
            raise ValueError("top_k and rebalance_freq must be positive integers.")
        if not 0 <= self.buy_cost < 1 or not 0 <= self.sell_cost < 1:
            raise ValueError("Transaction cost rates must be between 0 and 1.")
        if self.deal_price not in self.VALID_DEAL_PRICES:
            raise ValueError(f"deal_price must be one of {self.VALID_DEAL_PRICES}")

    def run(self, predictions: pd.Series | pd.DataFrame) -> BacktestResult:
        """Execute backtest on predictions from the test segment.

        Args:
            predictions: Model output with MultiIndex (datetime, instrument).

        Returns:
            BacktestResult with daily returns, equity curve, trades, metrics.
        """
        if isinstance(predictions, pd.DataFrame):
            predictions = predictions.iloc[:, 0]

        pred = predictions.dropna()
        if pred.empty:
            raise ValueError("Predictions are empty after dropping NaN.")

        dates = sorted(pred.index.get_level_values(0).unique())
        instruments = sorted(pred.index.get_level_values(1).unique())

        logger.info(
            f"Backtest: {len(dates)} days, {len(instruments)} instruments, "
            f"top_k={self.top_k}, rebalance_freq={self.rebalance_freq}, "
            f"deal_price={self.deal_price}"
        )

        close_prices = self._fetch_prices(instruments, dates[0], dates[-1], "$close")
        benchmark_close = self._fetch_benchmark_close(dates[0], dates[-1])

        open_prices = None
        if self.deal_price == "open":
            open_prices = self._fetch_prices(instruments, dates[0], dates[-1], "$open")

        if len(dates) < 2:
            raise ValueError("Need at least two prediction dates for backtest.")
        bench_daily = benchmark_close.pct_change(fill_method=None)
        all_port_returns: list[tuple] = []
        all_bench_returns: list[tuple] = []
        trade_records: list[dict] = []
        position_records: list[dict] = []
        # Fractional shares keep this a portfolio research model, while cash and
        # share counts prevent free daily reweighting and preserve overnight P&L.
        shares: dict[str, float] = {}
        cash = 1.0
        previous_nav = 1.0

        def required_price(frame: pd.DataFrame, date, inst: str) -> float:
            price = _safe_price(frame, date, inst)
            if not np.isfinite(price) or price <= 0:
                raise ValueError(f"Missing or invalid price for {inst} on {date}; cannot value or execute backtest.")
            return price

        for day_idx, date in enumerate(dates[1:], 1):
            prev_date = dates[day_idx - 1]
            is_rebalance = (day_idx - 1) % self.rebalance_freq == 0
            if is_rebalance:
                day_pred = pred.xs(prev_date, level=0).dropna().sort_values(ascending=False)
                selected = day_pred.head(self.top_k).index.tolist()
                if not selected:
                    raise ValueError(f"No usable predictions for {prev_date}.")
                execution_frame = open_prices if self.deal_price == "open" else close_prices
                execution_date = date if self.deal_price == "open" else prev_date
                involved = set(shares) | set(selected)
                prices = {inst: required_price(execution_frame, execution_date, inst) for inst in involved}
                old_values = {inst: shares.get(inst, 0.0) * prices[inst] for inst in involved}
                execution_nav = cash + sum(old_values.values())
                weight = 1.0 / len(selected)
                # Account for both exits and changes to retained holdings.
                lower, upper = 0.0, execution_nav
                for _ in range(50):
                    investable = (lower + upper) / 2
                    targets = {inst: investable * weight if inst in selected else 0.0 for inst in involved}
                    fees = sum(max(targets[inst] - old_values[inst], 0) * self.buy_cost
                               + max(old_values[inst] - targets[inst], 0) * self.sell_cost
                               for inst in involved)
                    if investable + fees > execution_nav:
                        upper = investable
                    else:
                        lower = investable
                investable = lower
                targets = {inst: investable * weight if inst in selected else 0.0 for inst in involved}
                fees = 0.0
                for inst in sorted(involved):
                    delta = targets[inst] - old_values[inst]
                    if abs(delta) > 1e-12:
                        fee = abs(delta) * (self.buy_cost if delta > 0 else self.sell_cost)
                        fees += fee
                        trade_records.append({"date": str(date), "instrument": inst,
                                              "action": "BUY" if delta > 0 else "SELL",
                                              "weight": abs(delta) / execution_nav, "fee": fee})
                shares = {inst: targets[inst] / prices[inst] for inst in selected}
                cash = execution_nav - sum(targets.values()) - fees
            nav = cash + sum(quantity * required_price(close_prices, date, inst) for inst, quantity in shares.items())
            port_ret = nav / previous_nav - 1
            previous_nav = nav
            all_port_returns.append((date, port_ret))
            all_bench_returns.append((date, bench_daily.get(date, np.nan)))
            for inst, quantity in shares.items():
                position_records.append({"date": str(date), "instrument": inst,
                                         "weight": round(quantity * required_price(close_prices, date, inst) / nav, 6) if nav > 0 else 0.0})

        port_series = pd.Series(dict(all_port_returns), name="portfolio")
        bench_series = pd.Series(dict(all_bench_returns), name="benchmark", dtype=float)
        equity = (1 + port_series).cumprod()
        bench_cum = (1 + bench_series).cumprod(skipna=False)
        metrics = compute_portfolio_metrics(port_series, benchmark_returns=bench_series)
        metrics["total_trades"] = len(trade_records)
        cfg_snapshot = {
            "top_k": self.top_k, "rebalance_freq": self.rebalance_freq,
            "buy_cost": self.buy_cost, "sell_cost": self.sell_cost,
            "benchmark": self.benchmark, "deal_price": self.deal_price,
            "test_days": len(dates), "test_start": str(pd.Timestamp(dates[0]).date()),
            "test_end": str(pd.Timestamp(dates[-1]).date()), "instruments": len(instruments),
            "limitations": BACKTEST_LIMITATIONS + [
                "快速回测使用零碎股，不模拟停牌、涨跌停和成交量限制。",
            ] + (["收盘价模式假设信号产生时即可按同一收盘价成交，结果偏乐观。"] if self.deal_price == "close" else []),
        }

        return BacktestResult(
            portfolio_returns=port_series,
            benchmark_returns=bench_series,
            equity_curve=equity,
            benchmark_curve=bench_cum,
            trades=pd.DataFrame(trade_records),
            positions=pd.DataFrame(position_records),
            metrics=metrics,
            config=cfg_snapshot,
        )

    # ------------------------------------------------------------------
    # Data fetching helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_prices(
        instruments: list[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        field: str = "$close",
    ) -> pd.DataFrame:
        """Fetch price data via Qlib. Returns pivot table: index=date, columns=instrument."""
        import qlib.data

        df = qlib.data.D.features(
            instruments=instruments,
            fields=[field],
            start_time=pd.Timestamp(start_date),
            end_time=pd.Timestamp(end_date),
        )
        df = df.reset_index()
        if "datetime" not in df.columns or "instrument" not in df.columns:
            raise ValueError(f"Unexpected Qlib price columns: {list(df.columns)}")
        return df.pivot(index="datetime", columns="instrument", values=field).sort_index()

    def _fetch_benchmark_close(
        self,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> pd.Series:
        """Fetch benchmark close prices and return as Series."""
        import qlib.data

        try:
            df = qlib.data.D.features(
                instruments=[self.benchmark],
                fields=["$close"],
                start_time=pd.Timestamp(start_date),
                end_time=pd.Timestamp(end_date),
            )
            return df.droplevel("instrument")["$close"]
        except Exception as e:
            logger.warning(f"Could not fetch benchmark {self.benchmark}: {e}")
            return pd.Series(dtype=float)


def _safe_price(prices_df: pd.DataFrame, date, inst: str) -> float:
    if inst not in prices_df.columns or date not in prices_df.index:
        return 0.0
    val = prices_df.loc[date, inst]
    return 0.0 if np.isnan(val) else float(val)
