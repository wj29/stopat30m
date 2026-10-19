"""
Model evaluation metrics for alpha models.

Key metrics: IC, Rank IC, annual return, Sharpe, max drawdown.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from loguru import logger


def evaluate_predictions(
    pred: pd.Series | pd.DataFrame,
    dataset: Any = None,
    label: pd.Series | pd.DataFrame | None = None,
) -> dict[str, Any]:
    """
    Compute alpha model evaluation metrics.

    Args:
        pred: Model predictions (MultiIndex: datetime x instrument).
        dataset: Qlib DatasetH (used to extract test labels if label is None).
        label: Ground truth labels. If None, extracted from dataset test segment.
    """
    if label is None and dataset is not None:
        try:
            label = dataset.prepare("test", col_set="label", data_key="infer")
            if isinstance(label, pd.DataFrame):
                label = label.iloc[:, 0]
        except Exception as e:
            logger.warning(f"Could not extract labels from dataset: {e}")
            return {}

    if label is None:
        logger.warning("No label available for evaluation.")
        return {}

    if isinstance(pred, pd.DataFrame):
        pred = pred.iloc[:, 0]

    common = pred.index.intersection(label.index)
    if len(common) == 0:
        logger.warning("No overlapping index between predictions and labels.")
        return {}

    p = pred.loc[common]
    l = label.loc[common]

    metrics = {}

    # IC (Information Coefficient)
    if isinstance(common, pd.MultiIndex):
        daily_ic = p.groupby(level=0).apply(lambda x: x.corr(l.loc[x.index]))
    else:
        daily_ic = pd.Series([p.corr(l)])

    daily_ic = daily_ic.dropna()
    metrics["IC_mean"] = round(float(daily_ic.mean()), 6)
    metrics["IC_std"] = round(float(daily_ic.std()), 6)
    metrics["ICIR"] = round(float(daily_ic.mean() / (daily_ic.std() + 1e-8)), 4)

    # Rank IC
    if isinstance(common, pd.MultiIndex):
        daily_ric = p.groupby(level=0).apply(
            lambda x: x.rank().corr(l.loc[x.index].rank())
        )
    else:
        daily_ric = pd.Series([p.rank().corr(l.rank())])

    daily_ric = daily_ric.dropna()
    metrics["RankIC_mean"] = round(float(daily_ric.mean()), 6)
    metrics["RankIC_std"] = round(float(daily_ric.std()), 6)
    metrics["RankICIR"] = round(float(daily_ric.mean() / (daily_ric.std() + 1e-8)), 4)

    logger.info(f"Evaluation: IC={metrics['IC_mean']:.4f}, RankIC={metrics['RankIC_mean']:.4f}, ICIR={metrics['ICIR']:.4f}")
    return metrics


def compute_portfolio_metrics(
    returns: pd.Series,
    benchmark_returns: pd.Series | None = None,
    risk_free_rate: float = 0.03,
    periods_per_year: int = 252,
) -> dict[str, Any]:
    """
    Compute portfolio performance metrics from daily returns.

    Metrics: annual return, annual volatility, Sharpe, Sortino, max drawdown,
             Calmar, win rate, profit/loss ratio.
    """
    if returns.empty:
        return {}

    ret = pd.to_numeric(returns, errors="coerce").dropna().astype(float)
    if ret.empty:
        return {}
    if not np.isfinite(ret).all() or (ret < -1).any():
        raise ValueError("Daily returns must be finite and cannot be below -100%.")
    n = len(ret)
    total_growth = float((1 + ret).prod())
    annual_ret = float(total_growth ** (periods_per_year / n) - 1)
    daily_vol = float(ret.std(ddof=1)) if n > 1 else 0.0
    # Repeated geometric returns can differ by floating-point roundoff. A
    # near-zero denominator is undefined risk, not a trillion-point Sharpe.
    tolerance = 1e-12 * max(1.0, float(ret.abs().max()))
    if daily_vol <= tolerance:
        daily_vol = 0.0
    annual_vol = daily_vol * np.sqrt(periods_per_year)
    rf_daily = (1 + risk_free_rate) ** (1 / periods_per_year) - 1
    excess_daily = ret - rf_daily
    sharpe = float(excess_daily.mean() / daily_vol * np.sqrt(periods_per_year)) if daily_vol > 0 else None

    downside_vol = float(np.sqrt(np.mean(np.minimum(excess_daily, 0) ** 2)))
    if downside_vol <= tolerance:
        downside_vol = 0.0
    sortino = float(excess_daily.mean() / downside_vol * np.sqrt(periods_per_year)) if downside_vol > 0 else None

    cum = (1 + ret).cumprod()
    # The starting investment is a peak too: a first-day loss is a drawdown.
    peak = cum.cummax().clip(lower=1.0)
    drawdown = (cum - peak) / peak
    max_dd = float(drawdown.min())

    calmar = annual_ret / abs(max_dd) if max_dd < 0 else None

    win_rate = float((ret > 0).sum() / n) if n > 0 else 0.0

    avg_win = float(ret[ret > 0].mean()) if (ret > 0).any() else 0.0
    avg_loss = float(ret[ret < 0].mean()) if (ret < 0).any() else 0.0
    pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else None

    excess_ret = None
    if benchmark_returns is not None:
        bench = pd.to_numeric(benchmark_returns.reindex(ret.index), errors="coerce")
        valid_bench = bench.notna() & np.isfinite(bench) & (bench > -1)
        if valid_bench.all():
            benchmark_growth = float((1 + bench).prod())
            excess_ret = float((total_growth / benchmark_growth) ** (periods_per_year / n) - 1)

    metrics = {
        "annual_return": round(annual_ret, 4),
        "annual_volatility": round(annual_vol, 4),
        "sharpe": round(sharpe, 4) if sharpe is not None else None,
        "sortino": round(sortino, 4) if sortino is not None else None,
        "max_drawdown": round(max_dd, 4),
        "calmar": round(calmar, 4) if calmar is not None else None,
        "win_rate": round(win_rate, 4),
        "profit_loss_ratio": round(pl_ratio, 4) if pl_ratio is not None else None,
        "total_return": round(total_growth - 1, 6),
        "observation_days": n,
        "benchmark_available": bool(benchmark_returns is not None and valid_bench.all()),
    }
    if excess_ret is not None:
        metrics["excess_return"] = round(excess_ret, 4)

    logger.info(
        f"Portfolio: AnnRet={annual_ret:.2%}, Sharpe={sharpe}, "
        f"MaxDD={max_dd:.2%}, WinRate={win_rate:.2%}"
    )
    return metrics
