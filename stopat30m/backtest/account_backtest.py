from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from stopat30m.backtest.common import (
    create_run_dir,
    fetch_benchmark_close,
    fetch_price_fields,
    infer_dates_and_instruments,
    normalize_prediction_series,
    write_json,
)
from stopat30m.backtest.historical_broker import HistoricalBroker
from stopat30m.model.evaluator import compute_portfolio_metrics
from stopat30m.signal.generator import SignalGenerator
from stopat30m.trading.executor import execute_plan
from stopat30m.trading.models import OrderType
from stopat30m.trading.rebalancer import compute_rebalance_plan, normalize_instrument
from stopat30m.trading.risk import RiskManager
from stopat30m.trading.rules import price_limit_range


@dataclass
class AccountBacktestResult:
    report: dict[str, Any]
    nav: pd.DataFrame
    orders: pd.DataFrame
    fills: pd.DataFrame
    positions: pd.DataFrame
    risk_events: pd.DataFrame
    turnover: pd.DataFrame
    config: dict[str, Any] = field(default_factory=dict)

    def save(self, output_dir: str | Path | None = None, tag: str = "") -> Path:
        run_dir = Path(output_dir) if output_dir else create_run_dir("account", tag=tag)
        run_dir.mkdir(parents=True, exist_ok=True)

        write_json(run_dir / "report.json", self.report)
        write_json(run_dir / "config.json", self.config)

        self.nav.to_csv(run_dir / "nav.csv", index=False)
        self.orders.to_csv(run_dir / "orders.csv", index=False)
        self.fills.to_csv(run_dir / "fills.csv", index=False)
        self.positions.to_csv(run_dir / "positions.csv", index=False)
        self.risk_events.to_csv(run_dir / "risk_events.csv", index=False)
        self.turnover.to_csv(run_dir / "turnover.csv", index=False)
        return run_dir


class AccountBacktestEngine:
    def __init__(
        self,
        initial_capital: float = 1_000_000,
        top_k: int = 10,
        method: str = "top_k",
        rebalance_freq: int = 5,
        execution_price: str = "open",
        order_type: str = "market",
        slippage_bps: float = 0.0,
        allow_partial_fill: bool = False,
        participation_rate: float = 0.1,
        cash_reserve_pct: float = 0.02,
        benchmark: str = "SH000300",
        enable_risk_manager: bool = True,
    ) -> None:
        if execution_price not in {"open", "close"}:
            raise ValueError("execution_price must be 'open' or 'close'")
        if order_type not in {"market", "limit"}:
            raise ValueError("order_type must be 'market' or 'limit'")
        self.initial_capital = initial_capital
        self.top_k = top_k
        self.method = method
        self.rebalance_freq = rebalance_freq
        self.execution_price = execution_price
        self.order_type = order_type
        self.slippage_bps = max(0.0, slippage_bps)
        self.allow_partial_fill = allow_partial_fill
        self.participation_rate = max(0.0, min(participation_rate, 1.0))
        self.cash_reserve_pct = cash_reserve_pct
        self.benchmark = benchmark
        self.enable_risk_manager = enable_risk_manager

    def run(self, predictions: pd.Series | pd.DataFrame) -> AccountBacktestResult:
        pred = normalize_prediction_series(predictions)
        dates, instruments = infer_dates_and_instruments(pred)
        if len(dates) < 2:
            raise ValueError("Need at least two prediction dates for account backtest.")

        price_fields = fetch_price_fields(
            instruments,
            dates[0] - pd.Timedelta(days=5),
            dates[-1],
            ["$open", "$close", "$high", "$low", "$volume"],
        )
        open_prices = price_fields["$open"]
        close_prices = price_fields["$close"]
        high_prices = price_fields["$high"]
        low_prices = price_fields["$low"]
        volumes = price_fields["$volume"]
        benchmark_close = fetch_benchmark_close(self.benchmark, dates[0], dates[-1])

        broker = HistoricalBroker(
            initial_capital=self.initial_capital,
            slippage_bps=self.slippage_bps,
            allow_partial_fill=self.allow_partial_fill,
            participation_rate=self.participation_rate,
        )
        target_weight = (1 - self.cash_reserve_pct) / max(1, self.top_k)
        risk_manager = (
            RiskManager(max_position_pct=max(0.05, min(1.0, target_weight * 1.1)))
            if self.enable_risk_manager
            else None
        )
        signal_gen = SignalGenerator(
            top_k=self.top_k,
            method=self.method,
            rebalance_freq=self.rebalance_freq,
        )

        order_rows: list[dict[str, Any]] = []
        fill_rows: list[dict[str, Any]] = []
        position_rows: list[dict[str, Any]] = []
        event_rows: list[dict[str, Any]] = []
        turnover_rows: list[dict[str, Any]] = []

        for day_idx in range(1, len(dates)):
            prev_date = dates[day_idx - 1]
            date = dates[day_idx]
            date_str = str(date.date())

            close_map = _row_to_price_map(close_prices, date)
            prev_close_map = _row_to_price_map(close_prices, prev_date)
            high_map = _row_to_price_map(high_prices, date)
            low_map = _row_to_price_map(low_prices, date)
            volume_map = _row_to_price_map(volumes, date)
            exec_map = _row_to_price_map(
                open_prices if self.execution_price == "open" else close_prices,
                date,
            )
            paused = _build_paused_map(exec_map, close_map, volume_map, instruments)
            buy_blocked, sell_blocked = _build_price_blocks(exec_map, prev_close_map, paused)
            broker.set_market_snapshot(
                date=date_str,
                execution_prices=exec_map,
                close_prices=close_map,
                high_prices=high_map,
                low_prices=low_map,
                volumes=volume_map,
                prev_closes=prev_close_map,
                paused=paused,
                buy_blocked=buy_blocked,
                sell_blocked=sell_blocked,
            )

            signals = signal_gen.generate(pred, date=prev_date)
            positions_df = _positions_to_df(broker.get_positions())
            tradable_prices = {
                normalize_instrument(str(inst)): price
                for inst, price in exec_map.items()
                if price > 0
            }

            plan = compute_rebalance_plan(
                signals=signals,
                positions=positions_df,
                total_capital=broker.get_account().equity,
                prices=tradable_prices,
                cash_reserve_pct=self.cash_reserve_pct,
            )

            for warning in plan.warnings:
                event_rows.append({
                    "date": date_str,
                    "event_type": "PLAN_WARNING",
                    "instrument": "",
                    "message": warning,
                })
            if not plan.errors.empty:
                for _, row in plan.errors.iterrows():
                    event_rows.append({
                        "date": date_str,
                        "event_type": "PLAN_ERROR",
                        "instrument": str(row.get("instrument", "")),
                        "message": str(row.get("reason", "")),
                    })

            order_before = len(broker.get_orders())
            fill_before = len(broker.get_fills())
            report = execute_plan(
                plan,
                broker,
                risk_manager=risk_manager,
                order_type=OrderType.LIMIT if self.order_type == "limit" else OrderType.MARKET,
                trade_date=date_str,
            )
            new_orders = broker.get_orders()[order_before:]
            new_fills = broker.get_fills()[fill_before:]

            for order in new_orders:
                order_rows.append({
                    "date": date_str,
                    **order.to_dict(),
                })
                if str(order.status.value) != "FILLED":
                    event_rows.append({
                        "date": date_str,
                        "event_type": "ORDER_REJECTED",
                        "instrument": order.instrument,
                        "message": order.reject_reason,
                    })

            for fill in new_fills:
                fill_rows.append({
                    "date": date_str,
                    **fill.to_dict(),
                })

            turnover_rows.append({
                "date": date_str,
                "turnover": round(sum(f.quantity * f.price for f in new_fills), 2),
                "fills": len(new_fills),
                "rejected": report.total_rejected,
            })

            broker.update_prices(close_map)
            snap = broker.settle_day(date_str)

            for inst, pos in broker.get_positions().items():
                position_rows.append({
                    "date": date_str,
                    "instrument": inst,
                    "quantity": pos.quantity,
                    "available_quantity": pos.available_quantity,
                    "avg_cost": round(pos.avg_cost, 6),
                    "current_price": round(pos.current_price, 6),
                    "market_value": round(pos.market_value, 2),
                    "unrealized_pnl": round(pos.unrealized_pnl, 2),
                    "realized_pnl": round(pos.realized_pnl, 2),
                })

        nav = pd.DataFrame([snap.to_dict() for snap in broker.get_nav_history()])
        if nav.empty:
            raise ValueError("Account backtest produced no NAV snapshots.")

        nav["date"] = pd.to_datetime(nav["date"])
        nav["daily_return"] = nav["equity"].pct_change().fillna(0.0)
        bench_ret = benchmark_close.pct_change().reindex(nav["date"]).fillna(0.0)
        metrics = compute_portfolio_metrics(
            nav["daily_return"].iloc[1:] if len(nav) > 1 else nav["daily_return"],
            benchmark_returns=bench_ret.iloc[1:] if len(bench_ret) > 1 else bench_ret,
        )

        orders = pd.DataFrame(order_rows)
        fills = pd.DataFrame(fill_rows)
        positions = pd.DataFrame(position_rows)
        risk_events = pd.DataFrame(event_rows)
        turnover = pd.DataFrame(turnover_rows)

        report = {
            **metrics,
            "initial_capital": self.initial_capital,
            "ending_equity": round(float(nav["equity"].iloc[-1]), 2),
            "total_fees": round(float(nav["total_fees"].iloc[-1]), 2),
            "order_count": int(len(orders)),
            "fill_count": int(len(fills)),
            "rejected_orders": int((orders["status"] == "REJECTED").sum()) if not orders.empty else 0,
            "partial_fills": int((orders["status"] == "PARTIALLY_FILLED").sum()) if not orders.empty else 0,
            "avg_daily_turnover": round(float(turnover["turnover"].mean()), 2) if not turnover.empty else 0.0,
            "avg_positions": round(float(nav["positions_count"].mean()), 2),
            "cash_utilization": round(
                float((1 - (nav["cash"] / nav["equity"].replace(0, pd.NA))).fillna(0).mean()),
                4,
            ),
        }

        config = {
            "initial_capital": self.initial_capital,
            "top_k": self.top_k,
            "method": self.method,
            "rebalance_freq": self.rebalance_freq,
            "execution_price": self.execution_price,
            "order_type": self.order_type,
            "slippage_bps": self.slippage_bps,
            "allow_partial_fill": self.allow_partial_fill,
            "participation_rate": self.participation_rate,
            "cash_reserve_pct": self.cash_reserve_pct,
            "benchmark": self.benchmark,
            "enable_risk_manager": self.enable_risk_manager,
            "effective_risk_max_position_pct": round(
                max(0.05, min(1.0, ((1 - self.cash_reserve_pct) / max(1, self.top_k)) * 1.1)),
                4,
            ) if self.enable_risk_manager else None,
            "test_start": str(dates[0].date()),
            "test_end": str(dates[-1].date()),
            "instruments": len(instruments),
        }

        nav["benchmark_cumulative"] = (1 + bench_ret.reindex(nav["date"]).fillna(0.0)).cumprod().values
        nav["equity_cumulative"] = nav["equity"] / float(nav["equity"].iloc[0])

        return AccountBacktestResult(
            report=report,
            nav=nav,
            orders=orders,
            fills=fills,
            positions=positions,
            risk_events=risk_events,
            turnover=turnover,
            config=config,
        )


def _row_to_price_map(frame: pd.DataFrame, date: pd.Timestamp) -> dict[str, float]:
    if date not in frame.index:
        return {}
    row = frame.loc[date].dropna()
    return {str(inst): float(price) for inst, price in row.items() if float(price) > 0}


def _positions_to_df(positions: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for inst, pos in positions.items():
        rows.append({
            "instrument": inst,
            "quantity": pos.quantity,
            "avg_cost": pos.avg_cost,
            "total_cost": pos.total_cost,
        })
    return pd.DataFrame(rows, columns=["instrument", "quantity", "avg_cost", "total_cost"])


def _build_price_blocks(
    execution_prices: dict[str, float],
    prev_closes: dict[str, float],
    paused: dict[str, bool] | None = None,
) -> tuple[dict[str, bool], dict[str, bool]]:
    buy_blocked: dict[str, bool] = {}
    sell_blocked: dict[str, bool] = {}
    for inst, price in execution_prices.items():
        if paused and paused.get(inst, False):
            buy_blocked[inst] = True
            sell_blocked[inst] = True
            continue
        prev_close = prev_closes.get(inst, 0.0)
        if prev_close <= 0:
            buy_blocked[inst] = price <= 0
            sell_blocked[inst] = price <= 0
            continue
        lower, upper = price_limit_range(prev_close, inst)
        buy_blocked[inst] = price >= upper or price <= 0
        sell_blocked[inst] = price <= lower or price <= 0
    return buy_blocked, sell_blocked


def _build_paused_map(
    execution_prices: dict[str, float],
    close_prices: dict[str, float],
    volumes: dict[str, float],
    instruments: list[str],
) -> dict[str, bool]:
    paused: dict[str, bool] = {}
    for inst in instruments:
        exec_price = execution_prices.get(inst, 0.0)
        close_price = close_prices.get(inst, 0.0)
        volume = volumes.get(inst, 0.0)
        paused[inst] = exec_price <= 0 or close_price <= 0 or volume <= 0
    return paused
