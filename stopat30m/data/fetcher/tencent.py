"""Tencent Finance data fetcher — historical daily OHLCV via public HTTP.

Uses ``web.ifzq.gtimg.cn`` (Tencent iFinance) which is independent of
east-money and generally very stable from mainland China.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import requests
from loguru import logger

from .base import DataFetcher


class TencentFetcher(DataFetcher):
    """Historical daily data from Tencent Finance HTTP API.

    Not suitable for bulk downloads (no stock-list / calendar endpoints),
    but excellent for single-stock OHLCV queries in the analysis pipeline.
    """

    name = "tencent"

    def __init__(self, delay: float = 0.1):
        self._delay = delay
        self._session = requests.Session()
        self._session.headers.update({
            "Referer": "https://stockapp.finance.qq.com",
            "User-Agent": "Mozilla/5.0",
        })

    @staticmethod
    def _to_tc_symbol(bare: str) -> str:
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
        tc_sym = self._to_tc_symbol(symbol)
        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)
        trading_days = int((end_dt - start_dt).days * 0.72) + 50

        df_qfq = self._fetch_kline(tc_sym, start, end, trading_days, fq="qfq")
        if df_qfq is None:
            return None

        df_qfq = df_qfq[(df_qfq["_ts"] >= start_dt) & (df_qfq["_ts"] <= end_dt)].copy()
        if df_qfq.empty:
            return None

        df_qfq["change"] = df_qfq["close"].pct_change() * 100

        df_raw = self._fetch_kline(tc_sym, start, end, trading_days, fq="")
        if df_raw is not None and len(df_raw) == len(df_qfq):
            raw_close = df_raw["close"].values
            hfq_close = df_qfq["close"].values
            with np.errstate(divide="ignore", invalid="ignore"):
                df_qfq["factor"] = np.where(raw_close > 0, hfq_close / raw_close, 1.0)
        else:
            df_qfq["factor"] = 1.0

        result_cols = ["date", "open", "close", "high", "low", "volume", "change", "factor"]
        return df_qfq[result_cols].sort_values("date").reset_index(drop=True)

    def _fetch_kline(
        self, tc_sym: str, start: str, end: str, count: int, fq: str,
    ) -> pd.DataFrame | None:
        """Fetch one kline series from Tencent JSONP endpoint.

        ``fq``: ``"qfq"`` for forward-adjusted, ``""`` for raw.
        """
        fq_tag = fq or "bfq"
        url = (
            f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            f"?_var=kline_day{fq_tag}&param={tc_sym},day,{start},{end},{count},{fq}"
        )
        try:
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            text = resp.text
        except Exception as e:
            logger.debug(f"Tencent kline request failed ({fq_tag}): {e}")
            return None

        json_str = text.partition("=")[2]
        if not json_str:
            return None

        import json
        try:
            body = json.loads(json_str)
        except json.JSONDecodeError:
            return None

        data = body.get("data", {})
        if not isinstance(data, dict):
            return None

        inner = data.get(tc_sym, {})
        if not isinstance(inner, dict):
            return None

        rows = inner.get(f"{fq_tag}day") or inner.get(f"{fq}day") or inner.get("day")
        if not rows:
            return None

        records = []
        for row in rows:
            if len(row) < 6:
                continue
            try:
                records.append({
                    "date": str(row[0])[:10],
                    "_ts": pd.Timestamp(str(row[0])),
                    "open": float(row[1]),
                    "close": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "volume": float(row[5]),
                })
            except (ValueError, IndexError):
                continue

        return pd.DataFrame(records) if records else None
