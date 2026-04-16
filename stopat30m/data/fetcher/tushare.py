"""Tushare Pro data fetcher (paid, token required)."""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
from loguru import logger

from .base import DataFetcher


class TushareFetcher(DataFetcher):
    """Paid data from Tushare Pro. Requires API token.

    Rate limits (free tier): 50 calls/min, 8000 calls/day.
    Each stock needs 2 API calls (daily + adj_factor), so effective
    throughput is ~25 stocks/min with default delay=1.3s.
    """

    name = "tushare"

    def __init__(self, token: str, delay: float = 1.3):
        self._delay = delay
        self._call_times: list[float] = []
        self._max_per_min = 48  # stay under 50/min with margin
        try:
            import tushare as ts
        except ImportError:
            raise ImportError("tushare not installed. Run: pip install tushare")
        ts.set_token(token)
        self._pro = ts.pro_api()

    def _rate_limit(self) -> None:
        """Enforce per-minute rate limit."""
        now = time.time()
        self._call_times = [t for t in self._call_times if now - t < 60]
        if len(self._call_times) >= self._max_per_min:
            wait = 60 - (now - self._call_times[0]) + 0.5
            if wait > 0:
                logger.debug(f"[Tushare] rate limit: sleeping {wait:.1f}s")
                time.sleep(wait)
        self._call_times.append(time.time())
        time.sleep(self._delay)

    def fetch_stock_list(self) -> list[str]:
        self._rate_limit()
        df = self._pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol")
        return df["symbol"].astype(str).str.zfill(6).tolist()

    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        self._rate_limit()
        df = self._pro.trade_cal(
            start_date=start.replace("-", ""),
            end_date=end.replace("-", ""),
            is_open="1",
        )
        dates = pd.to_datetime(df["cal_date"])
        return sorted(dates.dt.strftime("%Y-%m-%d").tolist())

    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        ts_code = self._to_ts_code(symbol)
        try:
            self._rate_limit()
            df = self._pro.daily(
                ts_code=ts_code,
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
            )
            if df is None or df.empty:
                return None

            self._rate_limit()
            adj = self._pro.adj_factor(
                ts_code=ts_code,
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
            )
        except Exception as e:
            logger.debug(f"Tushare failed for {ts_code}: {e}")
            return None

        df = df.sort_values("trade_date").reset_index(drop=True)
        adj = adj.sort_values("trade_date").set_index("trade_date")

        factor_series = df["trade_date"].map(adj["adj_factor"]).fillna(1.0)
        factor_vals = factor_series.astype(float).values
        latest_factor = factor_vals[-1] if len(factor_vals) > 0 else 1.0

        with np.errstate(divide="ignore", invalid="ignore"):
            norm_factor = np.where(latest_factor > 0, factor_vals / latest_factor, 1.0)

        result = pd.DataFrame({
            "date": pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d"),
            "open": (df["open"].astype(float) * norm_factor),
            "close": (df["close"].astype(float) * norm_factor),
            "high": (df["high"].astype(float) * norm_factor),
            "low": (df["low"].astype(float) * norm_factor),
            "volume": df["vol"].astype(float) * 100,
            "change": df["change"].astype(float) * norm_factor,
            "factor": factor_vals,
        })
        return result.sort_values("date").reset_index(drop=True)

    def fetch_index_components(self, index_code: str) -> list[str]:
        ts_index = {
            "000300": "399300.SZ",
            "000905": "000905.SH",
            "000852": "000852.SH",
        }.get(index_code, index_code)
        try:
            df = self._pro.index_weight(index_code=ts_index)
            if df.empty:
                return []
            latest = df["trade_date"].max()
            codes = df[df["trade_date"] == latest]["con_code"]
            return [c.split(".")[0] for c in codes]
        except Exception as e:
            logger.warning(f"Failed to fetch index {index_code} components: {e}")
            return []

    @staticmethod
    def _to_ts_code(symbol: str) -> str:
        if "." in symbol:
            return symbol
        if symbol.startswith(("6",)):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"
