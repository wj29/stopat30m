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

import numpy as np
import pandas as pd
from loguru import logger

from stopat30m.config import get
from stopat30m.model.evaluator import compute_portfolio_metrics


@dataclass
class BacktestResult:
    """Container for all backtest outputs."""

    portfolio_returns: pd.Series
    benchmark_returns: pd.Series
    equity_curve: pd.Series
    benchmark_curve: pd.Series
    trades: pd.DataFrame
    positions: pd.DataFrame
    metrics: dict[str, float]
    config: dict = field(default_factory=dict)

    def save(self, output_dir: str | Path = "./output/backtest") -> Path:
        """Persist backtest results to disk."""
        out = Path(output_dir)
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
        self.top_k = top_k or cfg.get("top_k", 20)
        self.rebalance_freq = rebalance_freq or cfg.get("rebalance_freq", 5)
        self.buy_cost = buy_cost if buy_cost is not None else cfg.get("buy_cost", 0.0003)
        self.sell_cost = sell_cost if sell_cost is not None else cfg.get("sell_cost", 0.0013)
        self.benchmark = benchmark or cfg.get("benchmark") or get("data", "benchmark", "SH000300")
        self.deal_price = deal_price or cfg.get("deal_price", "open")
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

        close_ret = close_prices.pct_change()
        bench_daily = benchmark_close.pct_change()

        rebalance_dates = set(dates[i] for i in range(0, len(dates), self.rebalance_freq))

        all_port_returns: list[tuple] = []
        all_bench_returns: list[tuple] = []
        trade_records: list[dict] = []
        position_records: list[dict] = []

        current_holdings: dict[str, float] = {}

        for day_idx, date in enumerate(dates):
            if day_idx == 0:
                continue

            prev_date = dates[day_idx - 1]
            is_rebalance = date in rebalance_dates or day_idx == 1

            if is_rebalance:
                day_pred = pred.xs(prev_date, level=0).dropna().sort_values(ascending=False)
                new_top = day_pred.head(self.top_k).index.tolist()

                if not new_top:
                    new_holdings = current_holdings.copy()
                else:
                    weight = 1.0 / len(new_top)
                    new_holdings = {s: weight for s in new_top}

                sold = set(current_holdings) - set(new_holdings)
                bought = set(new_holdings) - set(current_holdings)
                held = set(current_holdings) & set(new_holdings)

                turnover_cost = 0.0
                for s in sold:
                    trade_records.append({
                        "date": str(date), "instrument": s,
                        "action": "SELL", "weight": current_holdings[s],
                    })
                    turnover_cost += current_holdings[s] * self.sell_cost
                for s in bought:
                    trade_records.append({
                        "date": str(date), "instrument": s,
                        "action": "BUY", "weight": new_holdings[s],
                    })
                    turnover_cost += new_holdings[s] * self.buy_cost

                if self.deal_price == "open" and open_prices is not None:
                    port_ret = self._rebalance_day_return_open(
                        date, prev_date,
                        current_holdings, new_holdings,
                        sold, bought, held,
                        close_prices, open_prices,
                    )
                else:
                    port_ret = sum(
                        w * _safe_ret(close_ret, date, inst)
                        for inst, w in new_holdings.items()
                    )

                port_ret -= turnover_cost
                current_holdings = new_holdings
            else:
                port_ret = sum(
                    w * _safe_ret(close_ret, date, inst)
                    for inst, w in current_holdings.items()
                )

            bench_ret = bench_daily.loc[date] if date in bench_daily.index else 0.0
            if np.isnan(bench_ret):
                bench_ret = 0.0

            all_port_returns.append((date, port_ret))
            all_bench_returns.append((date, bench_ret))

            for inst, w in current_holdings.items():
                position_records.append({
                    "date": str(date), "instrument": inst, "weight": round(w, 6),
                })

        port_series = pd.Series(dict(all_port_returns), name="portfolio")
        bench_series = pd.Series(dict(all_bench_returns), name="benchmark")

        equity = (1 + port_series).cumprod()
        bench_cum = (1 + bench_series).cumprod()

        metrics = compute_portfolio_metrics(port_series, benchmark_returns=bench_series)

        cfg_snapshot = {
            "top_k": self.top_k,
            "rebalance_freq": self.rebalance_freq,
            "buy_cost": self.buy_cost,
            "sell_cost": self.sell_cost,
            "benchmark": self.benchmark,
            "deal_price": self.deal_price,
            "test_days": len(dates),
            "instruments": len(instruments),
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

    @staticmethod
    def _rebalance_day_return_open(
        date: pd.Timestamp,
        prev_date: pd.Timestamp,
        old_holdings: dict[str, float],
        new_holdings: dict[str, float],
        sold: set[str],
        bought: set[str],
        held: set[str],
        close_prices: pd.DataFrame,
        open_prices: pd.DataFrame,
    ) -> float:
        """Compute portfolio return on a rebalance day using open-price execution.

        Timeline for rebalance day T (signal from T-1 close):
          1. Overnight: portfolio drifts from T-1 close to T open
          2. At T open: sell outgoing, buy incoming, adjust held weights
          3. Intraday: new portfolio earns T open → T close

        Decomposition:
          - SOLD positions: earn overnight return (close[T-1] → open[T])
          - BOUGHT positions: earn intraday return (open[T] → close[T])
          - HELD positions: earn full day return (close[T-1] → close[T])
            Split: overnight (to open) + intraday (to close) = full day
        """
        ret = 0.0

        for inst in sold:
            w = old_holdings[inst]
            ret += w * _open_vs_close(open_prices, close_prices, date, prev_date, inst, "overnight")

        for inst in bought:
            w = new_holdings[inst]
            ret += w * _open_vs_close(open_prices, close_prices, date, prev_date, inst, "intraday")

        for inst in held:
            w = new_holdings[inst]
            ret += w * _open_vs_close(open_prices, close_prices, date, prev_date, inst, "full")

        return ret

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
        df.columns = ["datetime", "instrument", "price"]
        return df.pivot(index="datetime", columns="instrument", values="price")

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


def _safe_ret(returns_df: pd.DataFrame, date, inst: str) -> float:
    """Get return value with NaN fallback."""
    if inst not in returns_df.columns:
        return 0.0
    val = returns_df.loc[date, inst] if date in returns_df.index else 0.0
    return 0.0 if np.isnan(val) else float(val)


def _open_vs_close(
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    date,
    prev_date,
    inst: str,
    mode: str,
) -> float:
    """Compute return components for a single instrument on a rebalance day.

    Modes:
      - "overnight": close[T-1] → open[T]  (for sold positions)
      - "intraday":  open[T] → close[T]    (for bought positions)
      - "full":      close[T-1] → close[T]  (for held positions)
    """
    prev_close = _safe_price(close_prices, prev_date, inst)
    today_open = _safe_price(open_prices, date, inst)
    today_close = _safe_price(close_prices, date, inst)

    if mode == "overnight":
        return (today_open / prev_close - 1) if prev_close > 0 else 0.0
    elif mode == "intraday":
        return (today_close / today_open - 1) if today_open > 0 else 0.0
    else:  # full
        return (today_close / prev_close - 1) if prev_close > 0 else 0.0


def _safe_price(prices_df: pd.DataFrame, date, inst: str) -> float:
    if inst not in prices_df.columns or date not in prices_df.index:
        return 0.0
    val = prices_df.loc[date, inst]
    return 0.0 if np.isnan(val) else float(val)
