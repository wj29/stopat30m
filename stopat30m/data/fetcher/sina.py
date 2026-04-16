"""Sina Finance data fetcher — historical daily OHLCV via public HTTP.

Uses ``money.finance.sina.com.cn`` which is independent of east-money.
Classic, stable, but limited to ~last 1 year of daily data per call.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import requests
from loguru import logger

from .base import DataFetcher


class SinaFetcher(DataFetcher):
    """Historical daily data from Sina Finance HTTP API.

    Not suitable for bulk downloads (no stock-list / calendar endpoints),
    but reliable for single-stock OHLCV queries in the analysis pipeline.
    """

    name = "sina"

    def __init__(self, delay: float = 0.1):
        self._delay = delay
        self._session = requests.Session()
        self._session.headers.update({
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0",
        })

    @staticmethod
    def _to_sina_symbol(bare: str) -> str:
        bare = bare.zfill(6)
        if bare.startswith(("6", "9")):
            return f"sh{bare}"
        return f"sz{bare}"

    def fetch_stock_list(self) -> list[str]:
        return []

    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        return []

    def fetch_index_components(self, index_code: str) -> list[str]:
        return []

    @property
    def concurrency(self) -> int:
        return 1

    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        time.sleep(self._delay)
        sina_sym = self._to_sina_symbol(symbol)
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        trading_days = int((end_dt - start_dt).days * 0.72) + 20

        url = (
            "https://money.finance.sina.com.cn/quotes_service/api"
            "/json_v2.php/CN_MarketData.getKLineData"
            f"?symbol={sina_sym}&scale=240&ma=no&datalen={trading_days}"
        )
        try:
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.debug(f"Sina daily request failed for {symbol}: {e}")
            return None

        if not data or not isinstance(data, list):
            logger.debug(f"Sina no daily data for {symbol}")
            return None

        records = []
        for item in data:
            try:
                date_str = str(item.get("day", ""))[:10]
                dt = pd.Timestamp(date_str)
                if dt < start_dt or dt > end_dt:
                    continue
                records.append({
                    "date": dt.strftime("%Y-%m-%d"),
                    "open": float(item["open"]),
                    "close": float(item["close"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "volume": float(item["volume"]),
                })
            except (KeyError, ValueError, TypeError):
                continue

        if not records:
            return None

        df = pd.DataFrame(records)
        df["change"] = df["close"].pct_change() * 100
        df["factor"] = 1.0

        result_cols = ["date", "open", "close", "high", "low", "volume", "change", "factor"]
        return df[result_cols].sort_values("date").reset_index(drop=True)
