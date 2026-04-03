from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from stopat30m.backtest.common import (
    create_run_dir,
    fetch_benchmark_close,
    fetch_price_fields,
    infer_dates_and_instruments,
    normalize_prediction_series,
    write_json,
)
from stopat30m.backtest.engine import BacktestEngine
from stopat30m.signal.generator import SignalGenerator


@dataclass
class SignalBacktestResult:
    summary_metrics: dict[str, Any]
    topk_metrics: dict[str, Any]
    daily_ic: pd.DataFrame
    daily_rank_ic: pd.DataFrame
    horizon_stats: pd.DataFrame
    bucket_returns: pd.DataFrame
    topk_returns: pd.DataFrame
    turnover: pd.DataFrame
    coverage: pd.DataFrame
    signal_history: pd.DataFrame
    config: dict[str, Any] = field(default_factory=dict)

    def save(self, output_dir: str | Path | None = None, tag: str = "") -> Path:
        run_dir = Path(output_dir) if output_dir else create_run_dir("signal", tag=tag)
        run_dir.mkdir(parents=True, exist_ok=True)

        write_json(run_dir / "summary.json", self.summary_metrics)
        write_json(run_dir / "topk_report.json", self.topk_metrics)
        write_json(run_dir / "config.json", self.config)

        self.daily_ic.to_csv(run_dir / "daily_ic.csv", index=False)
        self.daily_rank_ic.to_csv(run_dir / "daily_rank_ic.csv", index=False)
        self.horizon_stats.to_csv(run_dir / "horizon_stats.csv", index=False)
        self.bucket_returns.to_csv(run_dir / "bucket_returns.csv", index=False)
        self.topk_returns.to_csv(run_dir / "topk_returns.csv", index=False)
        self.turnover.to_csv(run_dir / "turnover.csv", index=False)
        self.coverage.to_csv(run_dir / "coverage.csv", index=False)
        self.signal_history.to_csv(run_dir / "signal_history.csv", index=False)
        return run_dir


class SignalBacktestEngine:
    def __init__(
        self,
        top_k: int = 10,
        method: str = "top_k",
        rebalance_freq: int = 5,
        horizons: list[int] | None = None,
        group_count: int = 10,
        benchmark: str = "SH000300",
    ) -> None:
        self.top_k = top_k
        self.method = method
        self.rebalance_freq = rebalance_freq
        self.horizons = sorted(horizons or [1, 3, 5, 10, 20])
        self.group_count = group_count
        self.benchmark = benchmark
        self.eval_horizon = 5 if 5 in self.horizons else self.horizons[0]

    def run(self, predictions: pd.Series | pd.DataFrame) -> SignalBacktestResult:
        pred = normalize_prediction_series(predictions)
        dates, instruments = infer_dates_and_instruments(pred)
        if len(dates) < 2:
            raise ValueError("Need at least two prediction dates for signal backtest.")

        extended_end = dates[-1] + pd.Timedelta(days=max(self.horizons) * 3)
        price_fields = fetch_price_fields(instruments, dates[0], extended_end, ["$close"])
        close_prices = price_fields["$close"]
        benchmark_close = fetch_benchmark_close(self.benchmark, dates[0], extended_end)

        future_returns = {
            horizon: (close_prices.shift(-horizon) / close_prices - 1)
            for horizon in self.horizons
        }

        daily_ic_rows: list[dict[str, Any]] = []
        daily_rank_ic_rows: list[dict[str, Any]] = []
        coverage_rows: list[dict[str, Any]] = []

        for date in dates:
            day_pred = pred.xs(date, level=0).dropna()
            coverage_rows.append({
                "date": str(date.date()),
                "candidate_count": int(len(day_pred)),
            })

            ic_row: dict[str, Any] = {"date": str(date.date())}
            ric_row: dict[str, Any] = {"date": str(date.date())}
            for horizon, fwd in future_returns.items():
                actual = fwd.loc[date].dropna() if date in fwd.index else pd.Series(dtype=float)
                common = day_pred.index.intersection(actual.index)
                if len(common) >= 2:
                    score = day_pred.loc[common]
                    realized = actual.loc[common]
                    ic_row[f"ic_{horizon}d"] = float(score.corr(realized))
                    ric_row[f"rank_ic_{horizon}d"] = float(score.rank().corr(realized.rank()))
                else:
                    ic_row[f"ic_{horizon}d"] = None
                    ric_row[f"rank_ic_{horizon}d"] = None
            daily_ic_rows.append(ic_row)
            daily_rank_ic_rows.append(ric_row)

        signal_gen = SignalGenerator(
            top_k=self.top_k,
            method=self.method,
            rebalance_freq=self.rebalance_freq,
        )
        rebalance_dates = [dates[i] for i in range(0, len(dates), self.rebalance_freq)]

        signal_history_rows: list[dict[str, Any]] = []
        turnover_rows: list[dict[str, Any]] = []
        horizon_rows: list[dict[str, Any]] = []
        bucket_rows: list[dict[str, Any]] = []
        prev_selected: set[str] = set()

        for rebalance_date in rebalance_dates:
            day_pred = pred.xs(rebalance_date, level=0).dropna().sort_values(ascending=False)
            signals = signal_gen.generate(pred, date=rebalance_date)
            current_selected = set(str(v) for v in signals["instrument"].tolist())

            entered = len(current_selected - prev_selected)
            exited = len(prev_selected - current_selected)
            denom = max(1, len(current_selected | prev_selected))
            turnover_rows.append({
                "date": str(rebalance_date.date()),
                "selected_count": len(current_selected),
                "entered": entered,
                "exited": exited,
                "turnover": (entered + exited) / denom,
            })

            for _, row in signals.iterrows():
                signal_history_rows.append({
                    "date": str(rebalance_date.date()),
                    "instrument": str(row["instrument"]),
                    "score": float(row["score"]),
                    "signal": str(row.get("signal", "")),
                    "weight": float(row.get("weight", 0)),
                })

            # Per-horizon buy signal statistics
            buy_instruments = [str(v) for v in signals.loc[signals["signal"] == "BUY", "instrument"].tolist()]
            for horizon, fwd in future_returns.items():
                if rebalance_date not in fwd.index:
                    continue
                realized = fwd.loc[rebalance_date]
                realized = realized.reindex(buy_instruments).dropna()
                if realized.empty:
                    continue
                horizon_rows.append({
                    "date": str(rebalance_date.date()),
                    "horizon": horizon,
                    "selected_count": int(len(realized)),
                    "mean_return": float(realized.mean()),
                    "median_return": float(realized.median()),
                    "win_rate": float((realized > 0).mean()),
                })

            # Cross-sectional bucket returns using the primary evaluation horizon
            if rebalance_date in future_returns[self.eval_horizon].index and len(day_pred) >= self.group_count:
                realized = future_returns[self.eval_horizon].loc[rebalance_date].reindex(day_pred.index).dropna()
                score = day_pred.reindex(realized.index).dropna()
                if len(score) >= self.group_count:
                    ranked = pd.DataFrame({"score": score, "future_return": realized})
                    try:
                        ranked["bucket"] = pd.qcut(
                            ranked["score"],
                            self.group_count,
                            labels=False,
                            duplicates="drop",
                        ) + 1
                        grouped = ranked.groupby("bucket")["future_return"].mean()
                        for bucket, avg_return in grouped.items():
                            bucket_rows.append({
                                "date": str(rebalance_date.date()),
                                "horizon": self.eval_horizon,
                                "bucket": int(bucket),
                                "mean_return": float(avg_return),
                            })
                    except ValueError:
                        pass

            prev_selected = current_selected

        portfolio_engine = BacktestEngine(
            top_k=self.top_k,
            rebalance_freq=self.rebalance_freq,
            benchmark=self.benchmark,
        )
        portfolio_result = portfolio_engine.run(pred)
        topk_returns = pd.DataFrame({
            "date": portfolio_result.portfolio_returns.index.astype(str),
            "portfolio": portfolio_result.portfolio_returns.values,
            "benchmark": portfolio_result.benchmark_returns.reindex(portfolio_result.portfolio_returns.index).fillna(0).values,
            "portfolio_cumulative": portfolio_result.equity_curve.reindex(portfolio_result.portfolio_returns.index).values,
            "benchmark_cumulative": portfolio_result.benchmark_curve.reindex(portfolio_result.portfolio_returns.index).values,
        })

        daily_ic = pd.DataFrame(daily_ic_rows)
        daily_rank_ic = pd.DataFrame(daily_rank_ic_rows)
        horizon_stats = pd.DataFrame(
            horizon_rows,
            columns=["date", "horizon", "selected_count", "mean_return", "median_return", "win_rate"],
        )
        bucket_returns = pd.DataFrame(
            bucket_rows,
            columns=["date", "horizon", "bucket", "mean_return"],
        )
        turnover = pd.DataFrame(
            turnover_rows,
            columns=["date", "selected_count", "entered", "exited", "turnover"],
        )
        coverage = pd.DataFrame(
            coverage_rows,
            columns=["date", "candidate_count"],
        )
        signal_history = pd.DataFrame(
            signal_history_rows,
            columns=["date", "instrument", "score", "signal", "weight"],
        )

        summary_metrics = {
            "avg_candidate_count": round(float(coverage["candidate_count"].mean()), 2) if not coverage.empty else 0.0,
            "avg_turnover": round(float(turnover["turnover"].mean()), 4) if not turnover.empty else 0.0,
            "rebalance_count": int(len(turnover)),
            "signal_method": self.method,
            "top_k": self.top_k,
            "rebalance_freq": self.rebalance_freq,
            "benchmark": self.benchmark,
        }
        for horizon in self.horizons:
            col = f"ic_{horizon}d"
            rcol = f"rank_ic_{horizon}d"
            if col in daily_ic.columns:
                summary_metrics[f"{col}_mean"] = _safe_round(pd.to_numeric(daily_ic[col], errors="coerce").dropna().mean())
            if rcol in daily_rank_ic.columns:
                summary_metrics[f"{rcol}_mean"] = _safe_round(pd.to_numeric(daily_rank_ic[rcol], errors="coerce").dropna().mean())

            subset = horizon_stats[horizon_stats["horizon"] == horizon]
            if not subset.empty:
                summary_metrics[f"buy_mean_return_{horizon}d"] = _safe_round(subset["mean_return"].mean())
                summary_metrics[f"buy_win_rate_{horizon}d"] = _safe_round(subset["win_rate"].mean())

        if not bucket_returns.empty:
            pivot = bucket_returns.groupby("bucket")["mean_return"].mean()
            if not pivot.empty:
                summary_metrics["top_bottom_spread"] = round(float(pivot.max() - pivot.min()), 6)

        if not benchmark_close.empty:
            summary_metrics["benchmark_start"] = str(benchmark_close.index.min().date())
            summary_metrics["benchmark_end"] = str(benchmark_close.index.max().date())

        logger.info(
            f"Signal backtest complete: {len(turnover)} rebalance dates, "
            f"avg turnover {summary_metrics.get('avg_turnover', 0.0) * 100:.2f}%"
        )

        config = {
            "top_k": self.top_k,
            "method": self.method,
            "rebalance_freq": self.rebalance_freq,
            "horizons": self.horizons,
            "group_count": self.group_count,
            "benchmark": self.benchmark,
            "test_start": str(dates[0].date()),
            "test_end": str(dates[-1].date()),
            "instruments": len(instruments),
            "eval_horizon": self.eval_horizon,
        }

        return SignalBacktestResult(
            summary_metrics=summary_metrics,
            topk_metrics=portfolio_result.metrics,
            daily_ic=daily_ic,
            daily_rank_ic=daily_rank_ic,
            horizon_stats=horizon_stats,
            bucket_returns=bucket_returns,
            topk_returns=topk_returns,
            turnover=turnover,
            coverage=coverage,
            signal_history=signal_history,
            config=config,
        )


def _safe_round(value: Any, digits: int = 6) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), digits)
    except Exception:
        return 0.0
