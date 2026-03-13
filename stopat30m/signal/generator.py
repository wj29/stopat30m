"""
Signal generation: convert model predictions into tradeable signals.

Supports multiple signal construction methods and output to CSV / Redis.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from stopat30m.config import get


class SignalGenerator:
    """Generate trading signals from alpha model predictions."""

    def __init__(
        self,
        top_k: int | None = None,
        method: str | None = None,
        rebalance_freq: int | None = None,
        output_dir: str | None = None,
        output_format: str | None = None,
    ):
        cfg = get("signal") or {}
        self.top_k = top_k or cfg.get("top_k", 20)
        self.method = method or cfg.get("method", "top_k")
        self.rebalance_freq = rebalance_freq or cfg.get("rebalance_freq", 5)
        self.output_dir = Path(output_dir or cfg.get("output_dir", "./output/signals"))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_format = output_format or cfg.get("output_format", "csv")

    def generate(
        self,
        predictions: pd.Series | pd.DataFrame,
        date: str | None = None,
    ) -> pd.DataFrame:
        """
        Generate signals from predictions.

        Args:
            predictions: MultiIndex (datetime, instrument) -> score.
            date: Specific date to generate signals for. None = latest.

        Returns:
            DataFrame with columns: [instrument, score, signal, weight]
        """
        if isinstance(predictions, pd.DataFrame):
            pred = predictions.iloc[:, 0]
        else:
            pred = predictions

        if date is not None:
            pred = pred.xs(date, level=0)
        elif isinstance(pred.index, pd.MultiIndex):
            latest_date = pred.index.get_level_values(0).max()
            pred = pred.xs(latest_date, level=0)
            date = str(latest_date)

        pred = pred.dropna().sort_values(ascending=False)

        if self.method == "top_k":
            signals = self._top_k_signal(pred)
        elif self.method == "long_short":
            signals = self._long_short_signal(pred)
        elif self.method == "quantile":
            signals = self._quantile_signal(pred)
        else:
            raise ValueError(f"Unknown signal method: {self.method}")

        signals["date"] = date
        logger.info(f"Generated {len(signals)} signals for {date} (method={self.method})")
        return signals

    def _top_k_signal(self, pred: pd.Series) -> pd.DataFrame:
        """Select top K stocks with equal weight."""
        top = pred.head(self.top_k)
        n = len(top)
        return pd.DataFrame({
            "instrument": top.index,
            "score": top.values,
            "signal": "BUY",
            "weight": 1.0 / n if n > 0 else 0.0,
        })

    def _long_short_signal(self, pred: pd.Series) -> pd.DataFrame:
        """Long top K, short bottom K."""
        top = pred.head(self.top_k)
        bottom = pred.tail(self.top_k)
        n = len(top)

        long_df = pd.DataFrame({
            "instrument": top.index,
            "score": top.values,
            "signal": "BUY",
            "weight": 0.5 / n if n > 0 else 0.0,
        })
        short_df = pd.DataFrame({
            "instrument": bottom.index,
            "score": bottom.values,
            "signal": "SELL",
            "weight": 0.5 / n if n > 0 else 0.0,
        })
        return pd.concat([long_df, short_df], ignore_index=True)

    def _quantile_signal(self, pred: pd.Series) -> pd.DataFrame:
        """Score-weighted signals for top quantile."""
        threshold = pred.quantile(0.8)
        selected = pred[pred >= threshold]
        total_score = selected.sum()

        return pd.DataFrame({
            "instrument": selected.index,
            "score": selected.values,
            "signal": "BUY",
            "weight": selected.values / (total_score + 1e-8),
        })

    def save_signals(self, signals: pd.DataFrame, tag: str = "") -> Path:
        """Save signals to configured output."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{tag}" if tag else ""

        if self.output_format == "csv":
            path = self.output_dir / f"signal_{ts}{suffix}.csv"
            signals.to_csv(path, index=False)
        elif self.output_format == "json":
            path = self.output_dir / f"signal_{ts}{suffix}.json"
            signals.to_json(path, orient="records", force_ascii=False)
        else:
            raise ValueError(f"Unknown output format: {self.output_format}")

        logger.info(f"Signals saved to {path}")
        return path

    def publish_to_redis(self, signals: pd.DataFrame) -> None:
        """Publish signals to Redis for real-time consumption by vn.py."""
        import redis

        cfg = get("redis") or {}
        r = redis.Redis(
            host=cfg.get("host", "localhost"),
            port=cfg.get("port", 6379),
            db=cfg.get("db", 0),
        )
        channel = cfg.get("signal_channel", "alpha_signals")

        payload = signals.to_json(orient="records", force_ascii=False)
        r.publish(channel, payload)
        r.set("latest_signals", payload)
        logger.info(f"Published {len(signals)} signals to Redis channel '{channel}'")


def generate_daily_signals(
    predictions: pd.Series | pd.DataFrame,
    publish: bool = False,
) -> pd.DataFrame:
    """Convenience function: generate + save + optionally publish."""
    gen = SignalGenerator()
    signals = gen.generate(predictions)
    gen.save_signals(signals)

    if publish:
        try:
            gen.publish_to_redis(signals)
        except Exception as e:
            logger.error(f"Failed to publish to Redis: {e}")

    return signals
