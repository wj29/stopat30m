"""Efinance data fetcher (free, east-money backend, more stable than AKShare)."""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
from loguru import logger

from .base import DataFetcher


class EfinanceFetcher(DataFetcher):
    """Free data from efinance (east-money backend).

    More stable API surface than AKShare for the same underlying data.
    Single-threaded due to rate limiting.
    """

    name = "efinance"

    def __init__(self, delay: float = 0.2):
        self._delay = delay
        try:
            import efinance  # noqa: F401
        except ImportError:
            raise ImportError("efinance not installed. Run: pip install efinance")

    def fetch_stock_list(self) -> list[str]:
        import efinance as ef

        try:
            df = ef.stock.get_realtime_quotes()
            if df is not None and not df.empty:
                col = "股票代码" if "股票代码" in df.columns else df.columns[0]
                codes = df[col].astype(str).str.zfill(6).tolist()
                return [c for c in codes if c[:1] in ("0", "3", "6")]
        except Exception as e:
            logger.debug(f"efinance stock list failed: {e}")

        return []

    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        # efinance has no calendar API; delegate to AKShare/BaoStock if available,
        # otherwise fall back to pandas bday (approximate, includes non-trading days)
        try:
            from .akshare import AKShareFetcher
            return AKShareFetcher(delay=0).fetch_trade_calendar(start, end)
        except Exception:
            pass
        try:
            from .baostock import BaoStockFetcher
            return BaoStockFetcher(workers=1).fetch_trade_calendar(start, end)
        except Exception:
            pass
        logger.warning("No real trade calendar available; using pandas bday_range (approximate)")
        import pandas as pd
        dates = pd.bdate_range(start, end)
        return sorted(dates.strftime("%Y-%m-%d").tolist())

    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        import efinance as ef

        time.sleep(self._delay)
        beg = start.replace("-", "")
        fin = end.replace("-", "")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                df_hfq = ef.stock.get_quote_history(
                    symbol, beg=beg, end=fin, fqt=1,
                )
                if df_hfq is not None and not df_hfq.empty:
                    break
                if attempt < max_retries - 1:
                    time.sleep(self._delay * (2 ** attempt))
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.debug(f"efinance retry {attempt + 1} for {symbol}: {e}")
                    time.sleep(self._delay * (2 ** attempt))
                else:
                    logger.debug(f"efinance failed for {symbol} after {max_retries} attempts: {e}")
                    return None
        else:
            return None

        if df_hfq is None or df_hfq.empty:
            return None

        col_map = {
            "日期": "date", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume", "涨跌额": "change",
        }
        available = {k: v for k, v in col_map.items() if k in df_hfq.columns}
        df_hfq = df_hfq.rename(columns=available)

        if "date" not in df_hfq.columns:
            return None

        for col in ["open", "close", "high", "low", "volume", "change"]:
            if col in df_hfq.columns:
                df_hfq[col] = pd.to_numeric(df_hfq[col], errors="coerce")

        # Compute factor: fetch raw (non-adjusted) close for ratio
        try:
            df_raw = ef.stock.get_quote_history(symbol, beg=beg, end=fin, fqt=0)
            if df_raw is not None and not df_raw.empty and len(df_raw) == len(df_hfq):
                raw_col = "收盘" if "收盘" in df_raw.columns else "close"
                raw_close = pd.to_numeric(df_raw[raw_col], errors="coerce").values
                hfq_close = df_hfq["close"].values
                with np.errstate(divide="ignore", invalid="ignore"):
                    factor = np.where(raw_close > 0, hfq_close / raw_close, 1.0)
                df_hfq["factor"] = factor
            else:
                df_hfq["factor"] = 1.0
        except Exception:
            df_hfq["factor"] = 1.0

        df_hfq["date"] = pd.to_datetime(df_hfq["date"]).dt.strftime("%Y-%m-%d")

        result_cols = ["date", "open", "close", "high", "low", "volume", "change", "factor"]
        for c in result_cols:
            if c not in df_hfq.columns:
                df_hfq[c] = np.nan if c != "date" else ""

        return df_hfq[result_cols].sort_values("date").reset_index(drop=True)

    def fetch_index_components(self, index_code: str) -> list[str]:
        # efinance doesn't have a direct index component API
        return []
