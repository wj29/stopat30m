"""AKShare data fetcher (free, east-money backend, no auth)."""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
from loguru import logger

from .base import DataFetcher


class AKShareFetcher(DataFetcher):
    """Free data from AKShare (east-money backend, no auth required)."""

    name = "akshare"

    def __init__(self, delay: float = 0.3):
        self._delay = delay
        try:
            import akshare  # noqa: F401
        except ImportError:
            raise ImportError("akshare not installed. Run: pip install akshare")

    def fetch_stock_list(self) -> list[str]:
        import akshare as ak

        for attempt_fn in [self._stock_list_spot, self._stock_list_info]:
            try:
                codes = attempt_fn()
                if codes:
                    return codes
            except Exception as e:
                logger.debug(f"Stock list attempt failed: {e}")

        logger.warning("All stock list APIs failed; falling back to index components")
        combined: set[str] = set()
        for idx in ("000300", "000905", "000852"):
            combined.update(self.fetch_index_components(idx))
        return sorted(combined)

    @staticmethod
    def _stock_list_spot() -> list[str]:
        import akshare as ak

        df = ak.stock_zh_a_spot_em()
        codes = df["代码"].astype(str).str.zfill(6).tolist()
        return [c for c in codes if c[:1] in ("0", "3", "6")]

    @staticmethod
    def _stock_list_info() -> list[str]:
        import akshare as ak

        frames = []
        for fn in [ak.stock_info_sh_name_code, ak.stock_info_sz_name_code]:
            try:
                df = fn()
                col = "证券代码" if "证券代码" in df.columns else df.columns[0]
                frames.append(df[col].astype(str).str.zfill(6))
            except Exception:
                pass
        if not frames:
            return []
        codes = pd.concat(frames, ignore_index=True).tolist()
        return [c for c in codes if c[:1] in ("0", "3", "6")]

    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        import akshare as ak

        df = ak.tool_trade_date_hist_sina()
        dates = pd.to_datetime(df["trade_date"])
        mask = (dates >= start) & (dates <= end)
        return sorted(dates[mask].dt.strftime("%Y-%m-%d").tolist())

    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        import akshare as ak

        s = start.replace("-", "")
        e = end.replace("-", "")
        max_retries = 3

        df_hfq = self._fetch_hist_with_retry(ak, symbol, s, e, "hfq", max_retries)
        if df_hfq is None or df_hfq.empty:
            return None

        result = pd.DataFrame({
            "date": pd.to_datetime(df_hfq["日期"]).dt.strftime("%Y-%m-%d"),
            "open": df_hfq["开盘"].astype(float),
            "close": df_hfq["收盘"].astype(float),
            "high": df_hfq["最高"].astype(float),
            "low": df_hfq["最低"].astype(float),
            "volume": df_hfq["成交量"].astype(float),
            "change": df_hfq["涨跌额"].astype(float),
        })

        df_raw = self._fetch_hist_with_retry(ak, symbol, s, e, "", max_retries)
        if df_raw is not None and not df_raw.empty and len(df_hfq) == len(df_raw):
            raw_close = df_raw["收盘"].astype(float).values
            hfq_close = df_hfq["收盘"].astype(float).values
            with np.errstate(divide="ignore", invalid="ignore"):
                factor = np.where(raw_close > 0, hfq_close / raw_close, 1.0)
            result["factor"] = factor
        else:
            if df_raw is not None and not df_raw.empty:
                logger.debug(f"AKShare {symbol}: HFQ rows={len(df_hfq)} != raw rows={len(df_raw)}, factor=1.0")
            result["factor"] = 1.0

        return result.sort_values("date").reset_index(drop=True)

    def _fetch_hist_with_retry(
        self, ak, symbol: str, start: str, end: str, adjust: str, max_retries: int,
    ) -> pd.DataFrame | None:
        for attempt in range(max_retries):
            time.sleep(self._delay * (2 ** attempt))
            try:
                df = ak.stock_zh_a_hist(
                    symbol=symbol, period="daily",
                    start_date=start, end_date=end, adjust=adjust,
                )
                if df is not None and not df.empty:
                    return df
                if attempt < max_retries - 1:
                    logger.debug(f"Empty response for {symbol} (adjust={adjust!r}), retry {attempt + 1}/{max_retries}")
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.debug(f"AKShare error for {symbol}: {e}, retry {attempt + 1}/{max_retries}")
                else:
                    logger.debug(f"AKShare failed for {symbol} after {max_retries} attempts: {e}")
        return None

    def fetch_index_components(self, index_code: str) -> list[str]:
        import akshare as ak

        try:
            df = ak.index_stock_cons_csindex(symbol=index_code)
            col = "成分券代码" if "成分券代码" in df.columns else df.columns[0]
            return df[col].astype(str).str.zfill(6).tolist()
        except Exception as e:
            logger.warning(f"Failed to fetch index {index_code} components: {e}")
            return []
