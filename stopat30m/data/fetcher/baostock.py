"""BaoStock data fetcher (free, no rate limit, bulk-friendly)."""

from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger

from .base import DataFetcher


class BaoStockFetcher(DataFetcher):
    """Free data from BaoStock -- no registration, no rate limiting.

    Best choice for bulk historical data downloads. Each subprocess creates
    its own TCP connection (the baostock module is a module-level singleton).
    """

    name = "baostock"

    def __init__(self, workers: int = 4):
        try:
            import baostock as bs
        except ImportError:
            raise ImportError("baostock not installed. Run: pip install baostock")
        self._bs = bs
        self._logged_in = False
        self._request_count = 0
        self._relogin_every = 200
        self._workers = max(1, workers)

    @property
    def concurrency(self) -> int:
        return self._workers

    def can_fetch(self, symbol: str) -> bool:
        """BaoStock has unreliable coverage for STAR Market (688xxx)."""
        return not symbol.startswith("688")

    def _login(self) -> None:
        if not self._logged_in:
            import io, sys
            old_stdout = sys.stdout
            sys.stdout = io.StringIO()
            try:
                self._bs.login()
            finally:
                sys.stdout = old_stdout
            self._logged_in = True
            self._request_count = 0

    def _maybe_relogin(self) -> None:
        self._request_count += 1
        if self._request_count >= self._relogin_every:
            self._logout()
            self._login()
            logger.debug(f"BaoStock re-login after {self._relogin_every} requests")

    def _logout(self) -> None:
        if self._logged_in:
            try:
                import io, sys
                old_stdout = sys.stdout
                sys.stdout = io.StringIO()
                try:
                    self._bs.logout()
                finally:
                    sys.stdout = old_stdout
            except Exception:
                pass
            self._logged_in = False

    def __del__(self) -> None:
        self._logout()

    @staticmethod
    def _to_bs_code(symbol: str) -> str:
        s = symbol.zfill(6)
        prefix = "sh" if s.startswith("6") else "sz"
        return f"{prefix}.{s}"

    @staticmethod
    def _from_bs_code(bs_code: str) -> str:
        return bs_code.split(".")[-1]

    def fetch_stock_list(self) -> list[str]:
        self._login()
        rs = self._bs.query_stock_basic()
        df = rs.get_data()
        if df.empty:
            return []
        if "type" in df.columns and "status" in df.columns:
            df = df[(df["type"] == "1") & (df["status"] == "1")]
        codes = [self._from_bs_code(c) for c in df["code"].tolist()]
        return [c for c in codes if c[:1] in ("0", "3", "6")]

    def fetch_stock_list_with_info(self) -> dict[str, dict]:
        """Fetch stock list with IPO/delist dates and status."""
        self._login()
        rs = self._bs.query_stock_basic()
        df = rs.get_data()
        if df.empty:
            return {}

        if "type" in df.columns:
            df = df[df["type"] == "1"]

        info: dict[str, dict] = {}
        for _, row in df.iterrows():
            code = self._from_bs_code(str(row.get("code", "")))
            if not code or code[:1] not in ("0", "3", "6"):
                continue

            ipo = str(row.get("ipoDate", "")).strip() or None
            out = str(row.get("outDate", "")).strip() or None
            if out == "" or out == "None":
                out = None
            bs_status = str(row.get("status", "1")).strip()
            status = "delisted" if (bs_status == "0" or out) else "active"

            info[code] = {
                "ipo_date": ipo,
                "delist_date": out,
                "status": status,
            }
        logger.info(
            f"Stock listing info: {len(info)} stocks "
            f"({sum(1 for v in info.values() if v['status'] == 'active')} active, "
            f"{sum(1 for v in info.values() if v['status'] == 'delisted')} delisted)"
        )
        return info

    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        self._login()
        rs = self._bs.query_trade_dates(start_date=start, end_date=end)
        df = rs.get_data()
        if df.empty:
            return []
        trading = df[df["is_trading_day"] == "1"]
        return sorted(trading["calendar_date"].tolist())

    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        self._login()
        self._maybe_relogin()
        bs_code = self._to_bs_code(symbol)

        fields = "date,open,high,low,close,volume,pctChg"
        rs = self._bs.query_history_k_data_plus(
            bs_code, fields,
            start_date=start, end_date=end,
            frequency="d", adjustflag="1",
        )
        if rs.error_code != "0":
            logger.debug(f"BaoStock error for {bs_code}: {rs.error_msg}")
            return None

        df_hfq = rs.get_data()
        if df_hfq is None or df_hfq.empty:
            return None

        with pd.option_context("future.no_silent_downcasting", True):
            df_hfq = df_hfq.replace("", np.nan).dropna(subset=["close"])
        if df_hfq.empty:
            return None

        rs_raw = self._bs.query_history_k_data_plus(
            bs_code, "date,close",
            start_date=start, end_date=end,
            frequency="d", adjustflag="3",
        )
        df_raw = rs_raw.get_data() if rs_raw.error_code == "0" else pd.DataFrame()

        result = pd.DataFrame({
            "date": df_hfq["date"].values,
            "open": df_hfq["open"].astype(float),
            "close": df_hfq["close"].astype(float),
            "high": df_hfq["high"].astype(float),
            "low": df_hfq["low"].astype(float),
            "volume": df_hfq["volume"].astype(float),
            "change": df_hfq["pctChg"].astype(float),
        })

        if not df_raw.empty and len(df_raw) == len(result):
            raw_close = df_raw["close"].astype(float).values
            hfq_close = result["close"].values
            with np.errstate(divide="ignore", invalid="ignore"):
                factor = np.where(raw_close > 0, hfq_close / raw_close, 1.0)
            result["factor"] = factor
        else:
            result["factor"] = 1.0

        return result.sort_values("date").reset_index(drop=True)

    def fetch_index_components(self, index_code: str) -> list[str]:
        self._login()
        try:
            if index_code == "000300":
                rs = self._bs.query_hs300_stocks()
            elif index_code == "000905":
                rs = self._bs.query_zz500_stocks()
            else:
                return []
            df = rs.get_data()
            if df.empty:
                return []
            return [self._from_bs_code(c) for c in df["code"].tolist()]
        except Exception as e:
            logger.warning(f"Failed to fetch index {index_code}: {e}")
            return []
