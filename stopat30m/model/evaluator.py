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
) -> dict[str, float]:
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
) -> dict[str, float]:
    """
    Compute portfolio performance metrics from daily returns.

    Metrics: annual return, annual volatility, Sharpe, Sortino, max drawdown,
             Calmar, win rate, profit/loss ratio.
    """
    if returns.empty:
        return {}

    ret = returns.dropna()
    n = len(ret)

    annual_ret = float((1 + ret.mean()) ** periods_per_year - 1)
    annual_vol = float(ret.std() * np.sqrt(periods_per_year))
    sharpe = (annual_ret - risk_free_rate) / (annual_vol + 1e-8)

    downside = ret[ret < 0]
    downside_vol = float(downside.std() * np.sqrt(periods_per_year)) if len(downside) > 0 else 1e-8
    sortino = (annual_ret - risk_free_rate) / (downside_vol + 1e-8)

    cum = (1 + ret).cumprod()
    peak = cum.cummax()
    drawdown = (cum - peak) / peak
    max_dd = float(drawdown.min())

    calmar = annual_ret / (abs(max_dd) + 1e-8)

    win_rate = float((ret > 0).sum() / n) if n > 0 else 0.0

    avg_win = float(ret[ret > 0].mean()) if (ret > 0).any() else 0.0
    avg_loss = float(ret[ret < 0].mean()) if (ret < 0).any() else -1e-8
    pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

    excess_ret = None
    if benchmark_returns is not None:
        bench = benchmark_returns.reindex(ret.index).fillna(0)
        excess = ret - bench
        excess_annual = float((1 + excess.mean()) ** periods_per_year - 1)
        excess_ret = excess_annual

    metrics = {
        "annual_return": round(annual_ret, 4),
        "annual_volatility": round(annual_vol, 4),
        "sharpe": round(sharpe, 4),
        "sortino": round(sortino, 4),
        "max_drawdown": round(max_dd, 4),
        "calmar": round(calmar, 4),
        "win_rate": round(win_rate, 4),
        "profit_loss_ratio": round(pl_ratio, 4),
        "total_trades": n,
    }
    if excess_ret is not None:
        metrics["excess_return"] = round(excess_ret, 4)

    logger.info(
        f"Portfolio: AnnRet={annual_ret:.2%}, Sharpe={sharpe:.2f}, "
        f"MaxDD={max_dd:.2%}, WinRate={win_rate:.2%}"
    )
    return metrics
