"""A-share data fetchers and Qlib format converter.

Supported sources:
  - akshare: Free, no auth. Uses east-money backend. Latest data available.
  - tushare: Paid (token required). Higher quality, richer fields.

Both sources download OHLCV daily data and convert it into Qlib's binary
format (calendars, instruments, features/*.day.bin).
"""

from __future__ import annotations

import struct
import threading
import time
from abc import ABC, abstractmethod
from typing import NamedTuple
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger


QLIB_FIELDS = ("open", "close", "high", "low", "volume", "change", "factor")

# Qlib instrument naming: SH600000, SZ000001
_SH_PREFIXES = ("6",)
_SZ_PREFIXES = ("0", "3")


class _FetchResult(NamedTuple):
    """Result from a subprocess fetch worker (must be picklable)."""
    code: str
    status: str  # "ok" | "empty" | "skip" | "error"
    data_start: str  # empty string if N/A
    data_end: str
    n_rows: int
    error: str  # empty string if no error


def _to_qlib_symbol(code: str) -> str:
    """Convert raw numeric stock code to Qlib symbol (e.g. '600000' -> 'SH600000')."""
    code = code.strip()
    if code.startswith(("SH", "SZ", "sh", "sz")):
        return code.upper()
    if code.startswith(_SH_PREFIXES):
        return f"SH{code}"
    if code.startswith(_SZ_PREFIXES):
        return f"SZ{code}"
    return f"SH{code}"


def _tushare_to_qlib_symbol(ts_code: str) -> str:
    """'600000.SH' -> 'SH600000'."""
    code, exchange = ts_code.split(".")
    return f"{exchange.upper()}{code}"


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class DataFetcher(ABC):
    """Base class for A-share data sources."""

    @abstractmethod
    def fetch_stock_list(self) -> list[str]:
        """Return list of raw stock codes (e.g. ['600000', '000001', ...])."""

    @abstractmethod
    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        """Return sorted list of trading dates as 'YYYY-MM-DD' strings."""

    @abstractmethod
    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        """Fetch daily OHLCV for one stock.

        Must return DataFrame with columns:
            date (str YYYY-MM-DD), open, high, low, close, volume, change, factor
        Sorted by date ascending. Returns None on failure.
        """

    @abstractmethod
    def fetch_index_components(self, index_code: str) -> list[str]:
        """Return list of raw stock codes belonging to the index."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable source name."""

    @property
    def concurrency(self) -> int:
        """Max parallel sub-workers for intra-source parallelism.

        Override in subclasses that support multiple independent connections
        (e.g. BaoStock with no rate limit). Sources with rate limits should
        keep the default of 1.
        """
        return 1


# ---------------------------------------------------------------------------
# AKShare (免费)
# ---------------------------------------------------------------------------

class AKShareFetcher(DataFetcher):
    """Free data from AKShare (east-money backend, no auth required).

    Install: pip install akshare
    """

    name = "akshare"

    def __init__(self, delay: float = 0.3):
        self._delay = delay
        try:
            import akshare  # noqa: F401
        except ImportError:
            raise ImportError("akshare not installed. Run: pip install akshare")

    def fetch_stock_list(self) -> list[str]:
        import akshare as ak
        # Try multiple sources for robustness
        for attempt_fn in [self._stock_list_spot, self._stock_list_info]:
            try:
                codes = attempt_fn()
                if codes:
                    return codes
            except Exception as e:
                logger.debug(f"Stock list attempt failed: {e}")
        # Last resort: combine known index components
        logger.warning("All stock list APIs failed; falling back to index components")
        combined = set()
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

        df_raw = self._fetch_hist_with_retry(ak, symbol, s, e, "", max_retries)
        if df_raw is None or df_raw.empty or len(df_hfq) != len(df_raw):
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
        raw_close = df_raw["收盘"].astype(float).values
        hfq_close = df_hfq["收盘"].astype(float).values
        with np.errstate(divide="ignore", invalid="ignore"):
            factor = np.where(raw_close > 0, hfq_close / raw_close, 1.0)
        result["factor"] = factor

        return result.sort_values("date").reset_index(drop=True)

    def _fetch_hist_with_retry(self, ak, symbol: str, start: str, end: str, adjust: str, max_retries: int) -> pd.DataFrame | None:
        """Call ak.stock_zh_a_hist with exponential backoff on empty/error results."""
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
        """index_code: '000300' (CSI300), '000905' (CSI500), '000852' (CSI1000)."""
        import akshare as ak
        try:
            df = ak.index_stock_cons_csindex(symbol=index_code)
            col = "成分券代码" if "成分券代码" in df.columns else df.columns[0]
            return df[col].astype(str).str.zfill(6).tolist()
        except Exception as e:
            logger.warning(f"Failed to fetch index {index_code} components: {e}")
            return []


# ---------------------------------------------------------------------------
# BaoStock (免费, 不限流, 适合批量下载)
# ---------------------------------------------------------------------------

class BaoStockFetcher(DataFetcher):
    """Free data from BaoStock — no registration, no rate limiting.

    Best choice for bulk historical data downloads.
    Install: pip install baostock

    Args:
        workers: Number of parallel sub-processes for fetching. Each subprocess
            creates its own BaoStock TCP connection (the baostock module is a
            module-level singleton, so threading doesn't work — multiprocessing
            is required for true parallelism). Default 4.
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

    def _login(self) -> None:
        if not self._logged_in:
            self._bs.login()
            self._logged_in = True
            self._request_count = 0

    def _maybe_relogin(self) -> None:
        """Periodically reconnect to avoid TCP session timeout."""
        self._request_count += 1
        if self._request_count >= self._relogin_every:
            try:
                self._bs.logout()
            except Exception:
                pass
            self._logged_in = False
            self._login()
            logger.debug(f"BaoStock re-login after {self._relogin_every} requests")

    def _logout(self) -> None:
        if self._logged_in:
            try:
                self._bs.logout()
            except Exception:
                pass
            self._logged_in = False

    def __del__(self) -> None:
        self._logout()

    @staticmethod
    def _to_bs_code(symbol: str) -> str:
        """'600000' -> 'sh.600000', '000001' -> 'sz.000001'."""
        s = symbol.zfill(6)
        prefix = "sh" if s.startswith("6") else "sz"
        return f"{prefix}.{s}"

    @staticmethod
    def _from_bs_code(bs_code: str) -> str:
        """'sh.600000' -> '600000'."""
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
        """Fetch stock list with IPO/delist dates and status.

        Returns {bare_code: {"ipo_date":..., "delist_date":..., "status":...}}.
        """
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
        logger.info(f"Stock listing info: {len(info)} stocks "
                     f"({sum(1 for v in info.values() if v['status'] == 'active')} active, "
                     f"{sum(1 for v in info.values() if v['status'] == 'delisted')} delisted)")
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


# ---------------------------------------------------------------------------
# Tushare Pro (付费)
# ---------------------------------------------------------------------------

class TushareFetcher(DataFetcher):
    """Paid data from Tushare Pro. Requires API token.

    Install: pip install tushare
    Get token: https://tushare.pro/register
    """

    name = "tushare"

    def __init__(self, token: str, delay: float = 0.12):
        self._delay = delay
        try:
            import tushare as ts
        except ImportError:
            raise ImportError("tushare not installed. Run: pip install tushare")
        ts.set_token(token)
        self._pro = ts.pro_api()

    def fetch_stock_list(self) -> list[str]:
        df = self._pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol")
        return df["symbol"].tolist()

    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        df = self._pro.trade_cal(
            start_date=start.replace("-", ""),
            end_date=end.replace("-", ""),
            is_open="1",
        )
        dates = pd.to_datetime(df["cal_date"])
        return sorted(dates.dt.strftime("%Y-%m-%d").tolist())

    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        time.sleep(self._delay)
        ts_code = self._to_ts_code(symbol)
        try:
            df = self._pro.daily(
                ts_code=ts_code,
                start_date=start.replace("-", ""),
                end_date=end.replace("-", ""),
            )
            if df is None or df.empty:
                return None

            time.sleep(self._delay)
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
            "volume": df["vol"].astype(float) * 100,  # tushare vol is in 手 (100 shares)
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
        """'600000' -> '600000.SH'."""
        if "." in symbol:
            return symbol
        if symbol.startswith(("6",)):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"


# ---------------------------------------------------------------------------
# Qlib Binary Format Writer
# ---------------------------------------------------------------------------

class QlibDumper:
    """Convert stock DataFrames to Qlib's binary directory structure.

    Target layout:
        <qlib_dir>/
            calendars/day.txt          — one date per line
            instruments/all.txt        — <SYMBOL>\\t<START>\\t<END> per line
            instruments/csi300.txt
            instruments/csi500.txt
            features/<symbol>/         — lowercase
                open.day.bin           — float32 array, one per calendar day
                close.day.bin
                high.day.bin
                low.day.bin
                volume.day.bin
                change.day.bin
                factor.day.bin
    """

    def __init__(self, qlib_dir: str | Path):
        self.qlib_dir = Path(qlib_dir)
        self._calendar: list[str] = []
        self._date_to_idx: dict[str, int] = {}
        self._instruments: dict[str, tuple[str, str]] = {}  # symbol -> (start, end)

    def set_calendar(self, dates: list[str]) -> None:
        """Set the trading calendar. Must be called before dump_stock."""
        self._calendar = sorted(dates)
        self._date_to_idx = {d: i for i, d in enumerate(self._calendar)}

    def dump_stock(self, qlib_symbol: str, df: pd.DataFrame) -> None:
        """Write one stock's data as Qlib binary files.

        Qlib binary format per field:
          - 4 bytes: float32 start_index (calendar offset of first data point)
          - N × 4 bytes: float32 values from start_index to end_index

        df must have columns: date, open, high, low, close, volume, change, factor
        """
        if df.empty:
            return

        # Find the calendar index range for this stock
        valid_indices = []
        for _, row in df.iterrows():
            idx = self._date_to_idx.get(row["date"])
            if idx is not None:
                valid_indices.append(idx)

        if not valid_indices:
            return

        start_idx = min(valid_indices)
        end_idx = max(valid_indices)
        span = end_idx - start_idx + 1

        sym_lower = qlib_symbol.lower()
        feat_dir = self.qlib_dir / "features" / sym_lower
        feat_dir.mkdir(parents=True, exist_ok=True)

        for field in QLIB_FIELDS:
            data = np.full(span, np.nan, dtype=np.float32)
            for _, row in df.iterrows():
                idx = self._date_to_idx.get(row["date"])
                if idx is not None:
                    data[idx - start_idx] = float(row[field])

            # Write: [start_index_as_float32] + [data...]
            header = np.array([start_idx], dtype=np.float32)
            bin_path = feat_dir / f"{field}.day.bin"
            with open(bin_path, "wb") as f:
                f.write(header.tobytes())
                f.write(data.tobytes())

        dates = df["date"].tolist()
        self._instruments[qlib_symbol] = (min(dates), max(dates))

    def write_calendar(self) -> None:
        cal_dir = self.qlib_dir / "calendars"
        cal_dir.mkdir(parents=True, exist_ok=True)
        (cal_dir / "day.txt").write_text("\n".join(self._calendar) + "\n")
        logger.info(f"Calendar: {len(self._calendar)} trading days "
                     f"({self._calendar[0]} ~ {self._calendar[-1]})")

    def write_instruments(
        self,
        csi300_codes: list[str] | None = None,
        csi500_codes: list[str] | None = None,
    ) -> None:
        inst_dir = self.qlib_dir / "instruments"
        inst_dir.mkdir(parents=True, exist_ok=True)

        # all.txt
        lines = []
        for sym in sorted(self._instruments):
            s, e = self._instruments[sym]
            lines.append(f"{sym}\t{s}\t{e}")
        (inst_dir / "all.txt").write_text("\n".join(lines) + "\n")
        logger.info(f"Instruments: {len(lines)} stocks written to all.txt")

        # Index subsets
        for filename, raw_codes in [("csi300.txt", csi300_codes), ("csi500.txt", csi500_codes)]:
            if not raw_codes:
                continue
            qlib_codes = {_to_qlib_symbol(c) for c in raw_codes}
            subset = []
            for sym in sorted(self._instruments):
                if sym in qlib_codes:
                    s, e = self._instruments[sym]
                    subset.append(f"{sym}\t{s}\t{e}")
            if subset:
                (inst_dir / filename).write_text("\n".join(subset) + "\n")
                logger.info(f"Index {filename}: {len(subset)} stocks")

    def finalize(
        self,
        csi300_codes: list[str] | None = None,
        csi500_codes: list[str] | None = None,
    ) -> None:
        """Write calendar and instruments files after all stocks are dumped."""
        self.write_calendar()
        self.write_instruments(csi300_codes, csi500_codes)


# ---------------------------------------------------------------------------
# Data meta: per-stock metadata + rolling trusted watermark
# ---------------------------------------------------------------------------

import json
from dataclasses import asdict, dataclass, field as dc_field


@dataclass
class StockMeta:
    """Per-stock metadata tracking data coverage and listing status."""
    code: str
    ipo_date: str | None = None
    delist_date: str | None = None
    data_start: str | None = None
    data_end: str | None = None
    status: str = "active"  # active / delisted / suspended


@dataclass
class DataMeta:
    """Persistent metadata for the local Qlib dataset.

    Per-stock watermark design: each stock's ``data_end`` IS its watermark.
    The global ``trusted_until`` is a **computed property** — always derived
    from per-stock ``data_end`` values, never manually set or stored as
    source of truth.

    Fields:
    - qlib_base_end: static date marking the end of Qlib's official snapshot.
    - last_append: timestamp of last append operation.
    - listing_updated: date when stock listing was last refreshed from API.
    - stocks: per-stock tracking keyed by bare code ("600000").
    """
    qlib_base_end: str = ""
    last_append: str = ""
    listing_updated: str = ""
    stocks: dict[str, StockMeta] = dc_field(default_factory=dict)

    # -- computed global watermark --

    _DELIST_TOLERANCE_DAYS = 5

    _OUTLIER_LAG_DAYS = 180

    @property
    def trusted_until(self) -> str:
        """Global watermark = min(data_end) across tradable active stocks.

        Excluded from the calculation:
        - Delisted stocks (tracked per-stock separately).
        - Stocks without ipo_date (likely index codes that slipped in).
        - Outlier stocks whose data_end lags the majority by >180 days
          (likely long-suspended; they shouldn't block the entire watermark).

        Returns "" if no qualifying stocks have data.
        """
        constraining: list[str] = []
        for sm in self.stocks.values():
            if not sm.data_end:
                continue
            if sm.status in ("delisted", "index"):
                continue
            if not sm.ipo_date:
                continue
            constraining.append(sm.data_end)
        if not constraining:
            return ""

        max_date = max(constraining)
        from datetime import timedelta
        cutoff = (
            datetime.strptime(max_date, "%Y-%m-%d")
            - timedelta(days=self._OUTLIER_LAG_DAYS)
        ).strftime("%Y-%m-%d")
        filtered = [d for d in constraining if d >= cutoff]
        return min(filtered) if filtered else min(constraining)

    def watermark_outliers(self) -> list[StockMeta]:
        """Return active stocks excluded from watermark due to lagging data_end."""
        dates: list[str] = []
        for sm in self.stocks.values():
            if not sm.data_end or sm.status in ("delisted", "index") or not sm.ipo_date:
                continue
            dates.append(sm.data_end)
        if not dates:
            return []
        max_date = max(dates)
        from datetime import timedelta
        cutoff = (
            datetime.strptime(max_date, "%Y-%m-%d")
            - timedelta(days=self._OUTLIER_LAG_DAYS)
        ).strftime("%Y-%m-%d")
        return [
            sm for sm in self.stocks.values()
            if sm.data_end
            and sm.status not in ("delisted", "index")
            and sm.ipo_date
            and sm.data_end < cutoff
        ]

    @classmethod
    def _delisted_complete(cls, sm: StockMeta) -> bool:
        """A delisted stock is complete if data_end is within tolerance of delist_date.

        The delist_date from exchanges is the administrative removal date,
        which is typically 1-3 days after the last actual trading day.
        A tolerance of 5 calendar days covers weekends/holidays around delisting.
        """
        if not sm.delist_date or not sm.data_end:
            return False
        from datetime import timedelta
        delist = datetime.strptime(sm.delist_date, "%Y-%m-%d")
        cutoff = (delist - timedelta(days=cls._DELIST_TOLERANCE_DAYS)).strftime("%Y-%m-%d")
        return sm.data_end >= cutoff

    # -- persistence --

    def save(self, path: Path) -> None:
        payload = {
            "qlib_base_end": self.qlib_base_end,
            "trusted_until": self.trusted_until,  # computed, saved for display/external tools
            "last_append": self.last_append,
            "listing_updated": self.listing_updated,
            "stocks": {code: asdict(sm) for code, sm in self.stocks.items()},
        }
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path) -> "DataMeta":
        raw = json.loads(path.read_text())
        stocks = {
            code: StockMeta(**vals)
            for code, vals in raw.get("stocks", {}).items()
        }
        meta = cls(
            qlib_base_end=raw.get("qlib_base_end", ""),
            last_append=raw.get("last_append", ""),
            listing_updated=raw.get("listing_updated", ""),
            stocks=stocks,
        )
        purged = meta._purge_index_codes()
        if purged:
            logger.info(f"Purged {purged} index codes from meta on load")
            meta.save(path)
        return meta

    def _purge_index_codes(self) -> int:
        """Mark any index codes (sh000xxx etc.) that slipped into meta."""
        count = 0
        for code, sm in self.stocks.items():
            if sm.status == "index":
                continue
            qlib_sh = f"sh{code}"
            qlib_sz = f"sz{code}"
            if _is_index_symbol(qlib_sh) and not sm.ipo_date:
                sm.status = "index"
                count += 1
            elif _is_index_symbol(qlib_sz) and not sm.ipo_date:
                sm.status = "index"
                count += 1
        return count

    # -- query helpers --

    @staticmethod
    def _fetch_end(sm: StockMeta, target_date: str) -> str:
        """Data completeness goal: delist_date for delisted, target_date for active."""
        if sm.status == "delisted" and sm.delist_date:
            return sm.delist_date
        return target_date

    def needs_fetch(self, code: str, target_date: str) -> tuple[bool, str | None, str]:
        """Decide whether *code* needs a fetch, from which start, and to which end.

        Returns (should_fetch, fetch_start_or_None, fetch_end).
        """
        sm = self.stocks.get(code)
        if sm is None:
            return (True, None, target_date)

        if sm.status == "index":
            return (False, None, target_date)

        fetch_target = self._fetch_end(sm, target_date)

        if sm.data_end and sm.data_end >= fetch_target:
            return (False, None, fetch_target)

        if sm.status == "delisted" and self._delisted_complete(sm):
            return (False, None, fetch_target)

        if sm.data_end:
            start = _next_day_str(sm.data_end)
        elif sm.ipo_date:
            start = sm.ipo_date
        else:
            start = None
        return (True, start, fetch_target)

    def needs_listing_refresh(self) -> bool:
        """True if stock listing hasn't been refreshed today."""
        today = datetime.now().strftime("%Y-%m-%d")
        return self.listing_updated != today


def _next_day_str(date_str: str) -> str:
    from datetime import timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    return d.strftime("%Y-%m-%d")


META_FILENAME = "data_meta.json"

# Shanghai exchange index codes live in sh000xxx directories.
# They are NOT stocks and should not participate in data tracking.
_INDEX_PREFIXES = ("sh000", "sz399")


def _is_index_symbol(qlib_sym: str) -> bool:
    """Return True if the Qlib symbol is a market index, not a stock."""
    return any(qlib_sym.startswith(p) for p in _INDEX_PREFIXES)


def build_meta_from_scan(
    data_dir: Path,
    stock_info: dict[str, dict] | None = None,
) -> DataMeta:
    """Build DataMeta by scanning existing binary files + optional listing info.

    Args:
        data_dir: Qlib data root (e.g. ~/.qlib/qlib_data/cn_data).
        stock_info: Optional {bare_code: {"ipo_date":..., "delist_date":..., "status":...}}.
    """
    cal_path = data_dir / "calendars" / "day.txt"
    if not cal_path.exists():
        raise FileNotFoundError(f"No calendar at {cal_path}")

    cal_dates = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]
    cal_len = len(cal_dates)
    last_cal_date = cal_dates[-1]

    feat_dir = data_dir / "features"
    stocks: dict[str, StockMeta] = {}

    if feat_dir.exists():
        for sym_dir in sorted(feat_dir.iterdir()):
            if not sym_dir.is_dir():
                continue
            qlib_sym = sym_dir.name  # e.g. "sh600000"
            if _is_index_symbol(qlib_sym):
                continue
            bare = qlib_sym[2:] if len(qlib_sym) > 2 else qlib_sym

            close_bin = sym_dir / "close.day.bin"
            data_start_date = None
            data_end_date = None

            if close_bin.exists() and close_bin.stat().st_size >= 8:
                try:
                    start_idx, data = _read_bin_file(close_bin)
                    end_idx = start_idx + len(data) - 1
                    if 0 <= start_idx < cal_len:
                        data_start_date = cal_dates[start_idx]
                    if 0 <= end_idx < cal_len:
                        data_end_date = cal_dates[end_idx]
                except Exception:
                    pass

            info = (stock_info or {}).get(bare, {})
            stocks[bare] = StockMeta(
                code=bare,
                ipo_date=info.get("ipo_date"),
                delist_date=info.get("delist_date"),
                data_start=data_start_date,
                data_end=data_end_date,
                status=info.get("status", "active"),
            )

    # Add stocks from stock_info that are NOT in local features
    if stock_info:
        local_missing = 0
        for bare, info in stock_info.items():
            if bare not in stocks:
                stocks[bare] = StockMeta(
                    code=bare,
                    ipo_date=info.get("ipo_date"),
                    delist_date=info.get("delist_date"),
                    data_start=None,
                    data_end=None,
                    status=info.get("status", "active"),
                )
                if info.get("status") != "delisted":
                    local_missing += 1
        if local_missing:
            logger.warning(f"{local_missing} active stocks have NO local data")

    no_data_active = sum(1 for sm in stocks.values() if not sm.data_end and sm.status != "delisted")

    today = datetime.now().strftime("%Y-%m-%d")
    meta = DataMeta(
        qlib_base_end=last_cal_date,
        last_append=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        listing_updated=today if stock_info else "",
        stocks=stocks,
    )

    logger.info(
        f"Built meta: {len(stocks)} stocks, "
        f"trusted_until={meta.trusted_until or '(none)'}"
        f"{f', {no_data_active} active stocks without local data' if no_data_active else ''}"
    )
    return meta


def fetch_stock_listing_info(fetcher: "DataFetcher") -> dict[str, dict]:
    """Fetch IPO/delist dates from a DataFetcher's stock list.

    Returns {bare_code: {"ipo_date":..., "delist_date":..., "status":...}}.
    """
    info: dict[str, dict] = {}
    try:
        if hasattr(fetcher, "fetch_stock_list_with_info"):
            return fetcher.fetch_stock_list_with_info()

        codes = fetcher.fetch_stock_list()
        for code in codes:
            info[code] = {"ipo_date": None, "delist_date": None, "status": "active"}
    except Exception as e:
        logger.warning(f"Failed to fetch stock listing info: {e}")
    return info


# ---------------------------------------------------------------------------
# Binary file helpers for append mode
# ---------------------------------------------------------------------------

def _read_bin_file(bin_path: Path) -> tuple[int, np.ndarray]:
    """Read a Qlib binary feature file. Returns (start_index, data_array)."""
    raw = bin_path.read_bytes()
    start_idx = int(struct.unpack("<f", raw[:4])[0])
    data = np.frombuffer(raw[4:], dtype=np.float32).copy()
    return start_idx, data


def _append_binary(
    feat_dir: Path,
    df: pd.DataFrame,
    cal_to_idx: dict[str, int],
) -> None:
    """Append new data points to an existing stock's binary files.

    Reads the existing header + data, extends the array with new calendar
    positions, and writes back.  New values overwrite NaN-filled gaps.
    """
    for field in QLIB_FIELDS:
        bin_path = feat_dir / f"{field}.day.bin"

        if bin_path.exists() and bin_path.stat().st_size >= 8:
            old_start, old_data = _read_bin_file(bin_path)
            old_end = old_start + len(old_data) - 1
        else:
            old_start = None
            old_data = np.array([], dtype=np.float32)
            old_end = -1

        new_points: dict[int, float] = {}
        for _, row in df.iterrows():
            idx = cal_to_idx.get(row["date"])
            if idx is not None:
                new_points[idx] = float(row[field])

        if not new_points:
            continue

        new_min = min(new_points)
        new_max = max(new_points)

        if old_start is not None:
            final_start = min(old_start, new_min)
            final_end = max(old_end, new_max)
        else:
            final_start = new_min
            final_end = new_max

        span = final_end - final_start + 1
        merged = np.full(span, np.nan, dtype=np.float32)

        if old_start is not None and len(old_data) > 0:
            off = old_start - final_start
            merged[off : off + len(old_data)] = old_data

        for idx, val in new_points.items():
            merged[idx - final_start] = val

        header = np.array([final_start], dtype=np.float32)
        with open(bin_path, "wb") as f:
            f.write(header.tobytes())
            f.write(merged.tobytes())


def _write_fresh_binary(
    feat_dir: Path,
    df: pd.DataFrame,
    cal_to_idx: dict[str, int],
) -> None:
    """Write Qlib binary files for a stock not previously in the dataset."""
    indices = [cal_to_idx[r["date"]] for _, r in df.iterrows() if r["date"] in cal_to_idx]
    if not indices:
        return

    start_idx = min(indices)
    span = max(indices) - start_idx + 1

    for field in QLIB_FIELDS:
        data = np.full(span, np.nan, dtype=np.float32)
        for _, row in df.iterrows():
            idx = cal_to_idx.get(row["date"])
            if idx is not None:
                data[idx - start_idx] = float(row[field])

        header = np.array([start_idx], dtype=np.float32)
        with open(feat_dir / f"{field}.day.bin", "wb") as f:
            f.write(header.tobytes())
            f.write(data.tobytes())


# ---------------------------------------------------------------------------
# Orchestration: full download
# ---------------------------------------------------------------------------

def download_with_source(
    fetcher: DataFetcher,
    target_dir: str | Path,
    start_date: str = "2005-01-01",
    end_date: str | None = None,
) -> None:
    """Download A-share data from the given source and write Qlib format.

    Args:
        fetcher: DataFetcher instance (AKShareFetcher or TushareFetcher)
        target_dir: Qlib data directory (e.g. ~/.qlib/qlib_data/cn_data)
        start_date: Start date for data download
        end_date: End date (defaults to today)
    """
    target = Path(target_dir).expanduser()
    target.mkdir(parents=True, exist_ok=True)

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Data source: {fetcher.name}")
    logger.info(f"Target: {target}")
    logger.info(f"Date range: {start_date} ~ {end_date}")

    # 1. Trading calendar
    logger.info("Fetching trading calendar...")
    calendar = fetcher.fetch_trade_calendar(start_date, end_date)
    if not calendar:
        raise RuntimeError("Empty trading calendar — check date range or network")
    logger.info(f"Calendar: {len(calendar)} days ({calendar[0]} ~ {calendar[-1]})")

    # 2. Stock list
    logger.info("Fetching stock list...")
    stock_codes = fetcher.fetch_stock_list()
    logger.info(f"Found {len(stock_codes)} stocks")

    # 3. Index components (best-effort)
    logger.info("Fetching index components...")
    csi300 = fetcher.fetch_index_components("000300")
    csi500 = fetcher.fetch_index_components("000905")
    logger.info(f"CSI300: {len(csi300)} stocks, CSI500: {len(csi500)} stocks")

    # 4. Initialize dumper
    dumper = QlibDumper(target)
    dumper.set_calendar(calendar)

    # 5. Download stock-by-stock
    total = len(stock_codes)
    success = 0
    failed = 0
    t0 = time.time()

    for i, code in enumerate(stock_codes, 1):
        if i % 100 == 0 or i == total:
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (total - i) / rate if rate > 0 else 0
            logger.info(
                f"[{i}/{total}] {success} ok, {failed} fail | "
                f"{rate:.1f} stocks/s, ETA {eta / 60:.0f}min"
            )

        try:
            df = fetcher.fetch_daily(code, start_date, end_date)
        except Exception as e:
            logger.debug(f"Exception for {code}: {e}")
            failed += 1
            continue

        if df is None or df.empty:
            failed += 1
            continue

        qlib_sym = _to_qlib_symbol(code)
        dumper.dump_stock(qlib_sym, df)
        success += 1

    elapsed_total = time.time() - t0
    logger.info(
        f"Download complete: {success}/{total} stocks in {elapsed_total / 60:.1f}min "
        f"({failed} failed)"
    )

    # 6. Write metadata
    dumper.finalize(csi300_codes=csi300, csi500_codes=csi500)
    logger.info(f"Qlib data written to {target}")


# ---------------------------------------------------------------------------
# Orchestration: incremental append
# ---------------------------------------------------------------------------

def _load_checkpoint(path: Path) -> tuple[str, str, set[str]]:
    """Load an append checkpoint file.

    Format:
        Line 0: fetch_start|end_date
        Lines 1+: one completed stock code per line

    Returns (fetch_start, end_date, done_codes).
    """
    lines = path.read_text().strip().split("\n")
    header_parts = lines[0].split("|")
    fetch_start, end_date = header_parts[0], header_parts[1]
    done_codes = {l.strip() for l in lines[1:] if l.strip()}
    return fetch_start, end_date, done_codes


def _write_checkpoint_header(path: Path, fetch_start: str, end_date: str) -> None:
    """Create a fresh checkpoint file with the date-range header."""
    path.write_text(f"{fetch_start}|{end_date}\n")


def _checkpoint_stock(path: Path, code: str) -> None:
    """Append a completed stock code to the checkpoint."""
    with open(path, "a") as f:
        f.write(code + "\n")


def _stock_needs_data(feat_dir: Path, cal_len: int) -> bool:
    """Check if a stock's binary data is shorter than the current calendar.

    Returns True only if the feature directory exists but close.day.bin
    doesn't cover the full calendar — i.e. the stock was partially
    downloaded and should be retried.  Stocks with no directory at all
    are skipped (they were never in the dataset).
    """
    if not feat_dir.exists():
        return False
    close_bin = feat_dir / "close.day.bin"
    if not close_bin.exists():
        return True
    try:
        start_idx, data = _read_bin_file(close_bin)
        return (start_idx + len(data)) < cal_len
    except Exception:
        return True


class AppendError(RuntimeError):
    """Raised on unrecoverable data-pipeline errors."""


@dataclass
class WorkerResult:
    """Outcome of a single parallel fetch worker."""
    source_name: str
    success: int = 0
    empty: int = 0
    resumed: int = 0
    errors: int = 0
    error_codes: list[str] = dc_field(default_factory=list)
    instruments: dict[str, tuple[str, str]] = dc_field(default_factory=dict)


# ---------------------------------------------------------------------------
# Subprocess worker for intra-source parallelism
# ---------------------------------------------------------------------------

def _subprocess_fetch_batch(
    source_name: str,
    work: list[tuple[str, str | None, str, bool]],
    cal_to_idx: dict[str, int],
    target_str: str,
    done_codes: list[str],
    fallback_start: str,
    worker_id: int,
) -> list[_FetchResult]:
    """Fetch a batch of stocks in a subprocess with its own data source connection.

    Each subprocess creates an independent fetcher instance (and thus an
    independent TCP/HTTP connection), enabling true parallel I/O.  This is
    necessary for BaoStock whose module-level singleton connection makes
    threading unsafe.

    Args:
        source_name: Fetcher type ("baostock", "akshare", etc.).
        work: List of (code, fetch_start_or_None, fetch_end, is_delisted).
        cal_to_idx: Calendar date → index mapping for binary writes.
        target_str: Qlib data directory path (str for pickling).
        done_codes: Already-completed codes (list for pickling).
        fallback_start: Default start date when stock has no prior data.
        worker_id: Worker index for logging.

    Returns:
        List of _FetchResult for each processed stock.
    """
    import sys as _sys
    from loguru import logger as _log
    _log.remove()
    _log.add(
        _sys.stderr, level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
        colorize=True,
    )

    if source_name == "baostock":
        fetcher = BaoStockFetcher(workers=1)
    elif source_name == "akshare":
        fetcher = AKShareFetcher()
    else:
        raise ValueError(f"Subprocess fetch not supported for source: {source_name}")

    tag = f"<{_SOURCE_COLOR.get(source_name, 'white')}>[{source_name}#{worker_id}]</{_SOURCE_COLOR.get(source_name, 'white')}>"

    target = Path(target_str)
    done_set = set(done_codes)
    results: list[_FetchResult] = []
    total = len(work)
    t0 = time.time()
    ok_count = 0
    err_count = 0

    def _pace() -> str:
        done = ok_count + err_count
        elapsed = time.time() - t0
        if done > 0 and elapsed > 0:
            rate = done / elapsed
            eta_m = (total - i) / rate / 60
            return f"{rate:.1f}/s ETA {eta_m:.0f}m"
        return ""

    for i, (code, stock_start, stock_end, is_delisted) in enumerate(work, 1):
        if code in done_set:
            continue

        fetch_from = stock_start or fallback_start
        if fetch_from > stock_end:
            results.append(_FetchResult(code, "skip", "", "", 0, ""))
            continue

        pct = i * 100 // total

        try:
            df = fetcher.fetch_daily(code, fetch_from, stock_end)
        except Exception as e:
            err_count += 1
            results.append(_FetchResult(code, "error", "", "", 0, str(e)))
            _log.opt(colors=True).warning(f"{tag} {pct:>3d}% {i}/{total} {code} error: {e}")
            continue

        if df is None or df.empty:
            if is_delisted:
                results.append(_FetchResult(code, "empty", "", stock_end, 0, ""))
                _log.opt(colors=True).info(f"{tag} {pct:>3d}% {i}/{total} {code} empty(delisted)")
                continue
            err_count += 1
            results.append(_FetchResult(
                code, "error", "", "", 0, "empty data for active stock",
            ))
            _log.opt(colors=True).warning(f"{tag} {pct:>3d}% {i}/{total} {code} error(empty active)")
            continue

        qlib_sym = _to_qlib_symbol(code)
        feat_dir = target / "features" / qlib_sym.lower()
        if feat_dir.exists():
            _append_binary(feat_dir, df, cal_to_idx)
        else:
            feat_dir.mkdir(parents=True, exist_ok=True)
            _write_fresh_binary(feat_dir, df, cal_to_idx)

        dates = sorted(df["date"].tolist())
        results.append(_FetchResult(
            code, "ok", dates[0], dates[-1], len(df), "",
        ))
        ok_count += 1
        _log.opt(colors=True).info(f"{tag} {pct:>3d}% {i}/{total} {code} ok +{len(df)}d {_pace()}")

    elapsed = time.time() - t0
    _log.opt(colors=True).info(
        f"{tag} batch done: {ok_count} ok, {err_count} errors in {elapsed / 60:.1f}min"
    )
    return results


# ---------------------------------------------------------------------------
# Subprocess-parallel coordinator
# ---------------------------------------------------------------------------

def _worker_fetch_with_subprocesses(
    fetcher: DataFetcher,
    work: list[tuple[str, str | None, str]],
    meta: "DataMeta",
    cal_to_idx: dict[str, int],
    target: Path,
    checkpoint_path: Path,
    meta_lock: threading.Lock,
    meta_path: Path,
    done_codes: set[str],
    fallback_start: str = "2005-01-01",
    save_interval: int = 50,
) -> "WorkerResult":
    """Fetch stocks using multiple sub-processes for a single source.

    Splits *work* into sub-chunks and dispatches each to a subprocess via
    ProcessPoolExecutor.  Each subprocess creates its own fetcher instance
    (own TCP connection), enabling true parallel I/O.

    Binary file writes are safe: each stock goes to its own feature directory,
    so there's no cross-process conflict.  Meta/checkpoint updates are
    serialized in this coordinator thread after each subprocess completes.
    """
    src = fetcher.name
    concurrency = min(fetcher.concurrency, len(work))
    result = WorkerResult(source_name=src)

    if concurrency <= 1:
        return _worker_fetch(
            fetcher, work, meta, cal_to_idx, target,
            checkpoint_path, meta_lock, meta_path,
            done_codes, fallback_start, save_interval,
        )

    work_with_flags: list[tuple[str, str | None, str, bool]] = []
    for code, start, end in work:
        sm = meta.stocks.get(code)
        is_delisted = sm is not None and sm.status == "delisted"
        work_with_flags.append((code, start, end, is_delisted))

    chunks: list[list[tuple[str, str | None, str, bool]]] = [[] for _ in range(concurrency)]
    for idx, item in enumerate(work_with_flags):
        chunks[idx % concurrency].append(item)

    ctag = _colored_src(src)
    logger.opt(colors=True).info(
        f"{ctag} Launching {len(chunks)} sub-processes "
        f"({len(work)} stocks total)"
    )

    done_list = sorted(done_codes)
    target_str = str(target)
    t0 = time.time()

    with ProcessPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(
                _subprocess_fetch_batch,
                src, chunk, cal_to_idx, target_str,
                done_list, fallback_start, wid,
            ): wid
            for wid, chunk in enumerate(chunks)
        }

        for future in as_completed(futures):
            wid = futures[future]
            try:
                batch = future.result()
            except Exception as e:
                logger.opt(colors=True).error(f"{_colored_src(f'{src}#{wid}')} subprocess crashed: {e}")
                result.errors += 1
                result.error_codes.append(f"subprocess#{wid}")
                continue

            batch_errors = 0
            with meta_lock:
                for fr in batch:
                    if fr.status == "ok":
                        result.success += 1
                        _checkpoint_stock(checkpoint_path, fr.code)

                        qlib_sym = _to_qlib_symbol(fr.code)
                        if qlib_sym in result.instruments:
                            old_s, old_e = result.instruments[qlib_sym]
                            result.instruments[qlib_sym] = (
                                min(old_s, fr.data_start),
                                max(old_e, fr.data_end),
                            )
                        else:
                            result.instruments[qlib_sym] = (
                                fr.data_start, fr.data_end,
                            )

                        sm = meta.stocks.get(fr.code)
                        if sm:
                            sm.data_end = fr.data_end
                            if not sm.data_start:
                                sm.data_start = fr.data_start

                    elif fr.status == "empty":
                        result.empty += 1
                        _checkpoint_stock(checkpoint_path, fr.code)
                        sm = meta.stocks.get(fr.code)
                        if sm and fr.data_end:
                            sm.data_end = fr.data_end

                    elif fr.status == "skip":
                        _checkpoint_stock(checkpoint_path, fr.code)

                    elif fr.status == "error":
                        batch_errors += 1
                        result.errors += 1
                        result.error_codes.append(fr.code)

                meta.last_append = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                meta.save(meta_path)

            elapsed = time.time() - t0
            logger.opt(colors=True).info(
                f"{_colored_src(f'{src}#{wid}')} batch collected: "
                f"{result.success} ok, {result.empty} empty, "
                f"{batch_errors} errors ({elapsed / 60:.1f}min)"
            )

    return result


# ---------------------------------------------------------------------------
# Per-worker fetch loop (sequential, single-connection)
# ---------------------------------------------------------------------------

_SOURCE_COLOR = {
    "baostock": "cyan",
    "akshare": "yellow",
    "tushare": "magenta",
}


def _colored_src(src: str) -> str:
    """Wrap source name in loguru color tags for terminal output."""
    color = _SOURCE_COLOR.get(src.split("#")[0], "white")
    return f"<{color}>[{src}]</{color}>"


def _log_stock(
    src: str, i: int, total: int, code: str, status: str,
    t0: float, result: "WorkerResult",
) -> None:
    """Log one-line progress for a single stock."""
    elapsed = time.time() - t0
    done = result.success + result.empty
    if done > 0 and elapsed > 0:
        rate = done / elapsed
        eta_m = (total - i) / rate / 60
        pace = f"{rate:.1f}/s ETA {eta_m:.0f}m"
    else:
        pace = ""
    pct = i * 100 // total
    tag = _colored_src(src)
    logger.opt(colors=True).info(f"{tag} {pct:>3d}% {i}/{total} {code} {status} {pace}")


def _worker_fetch(
    fetcher: DataFetcher,
    work: list[tuple[str, str | None, str]],
    meta: DataMeta,
    cal_to_idx: dict[str, int],
    target: Path,
    checkpoint_path: Path,
    meta_lock: threading.Lock,
    meta_path: Path,
    done_codes: set[str],
    fallback_start: str = "2005-01-01",
    save_interval: int = 50,
) -> WorkerResult:
    """Fetch assigned stocks using *fetcher*, writing binary features.

    If the fetcher's concurrency > 1, delegates to subprocess-parallel mode
    (ProcessPoolExecutor) for true parallel I/O.  Otherwise falls through to
    the sequential single-connection loop below.

    Thread-safe by design:
      - Each worker operates on a disjoint set of stock codes.
      - Binary writes go to per-stock directories (no cross-stock conflict).
      - meta.stocks[code] updates are on disjoint keys.
      - Checkpoint and meta file writes are serialized via *meta_lock*.
    """
    if fetcher.concurrency > 1 and len(work) > 1:
        return _worker_fetch_with_subprocesses(
            fetcher, work, meta, cal_to_idx, target,
            checkpoint_path, meta_lock, meta_path,
            done_codes, fallback_start, save_interval,
        )

    src = fetcher.name
    result = WorkerResult(source_name=src)
    total = len(work)
    t0 = time.time()

    for i, (code, stock_start, stock_end) in enumerate(work, 1):

        if code in done_codes:
            result.resumed += 1
            continue

        fetch_from = stock_start or fallback_start
        if fetch_from > stock_end:
            with meta_lock:
                _checkpoint_stock(checkpoint_path, code)
            continue

        try:
            df = fetcher.fetch_daily(code, fetch_from, stock_end)
        except Exception as e:
            result.errors += 1
            result.error_codes.append(code)
            _log_stock(src, i, total, code, f"error: {e}", t0, result)
            continue

        sm = meta.stocks.get(code)
        is_delisted = sm and sm.status == "delisted"

        if df is None or df.empty:
            if is_delisted:
                result.empty += 1
                if sm:
                    sm.data_end = stock_end
                with meta_lock:
                    _checkpoint_stock(checkpoint_path, code)
                _log_stock(src, i, total, code, "empty(delisted)", t0, result)
                continue

            result.errors += 1
            result.error_codes.append(code)
            _log_stock(src, i, total, code, "error(empty active)", t0, result)
            continue

        qlib_sym = _to_qlib_symbol(code)
        sym_lower = qlib_sym.lower()
        feat_dir = target / "features" / sym_lower

        if feat_dir.exists():
            _append_binary(feat_dir, df, cal_to_idx)
        else:
            feat_dir.mkdir(parents=True, exist_ok=True)
            _write_fresh_binary(feat_dir, df, cal_to_idx)

        dates_in_data = sorted(df["date"].tolist())
        if qlib_sym in result.instruments:
            old_s, old_e = result.instruments[qlib_sym]
            result.instruments[qlib_sym] = (
                min(old_s, dates_in_data[0]),
                max(old_e, dates_in_data[-1]),
            )
        else:
            result.instruments[qlib_sym] = (dates_in_data[0], dates_in_data[-1])

        if sm:
            sm.data_end = dates_in_data[-1]
            if not sm.data_start:
                sm.data_start = dates_in_data[0]

        result.success += 1
        with meta_lock:
            _checkpoint_stock(checkpoint_path, code)

        n_rows = len(df)
        _log_stock(src, i, total, code, f"ok +{n_rows}d", t0, result)

        if result.success % save_interval == 0:
            with meta_lock:
                meta.last_append = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                meta.save(meta_path)
                logger.debug(f"[{src}] Periodic meta save after {result.success} stocks")

    return result


# ---------------------------------------------------------------------------
# Stock partitioning across multiple fetchers
# ---------------------------------------------------------------------------

_SOURCE_WEIGHT = {
    "baostock": 4,
    "akshare": 2,
    "tushare": 2,
}


def _partition_work(
    work: list[tuple[str, str | None, str]],
    fetchers: list[DataFetcher],
) -> list[list[tuple[str, str | None, str]]]:
    """Distribute stocks across fetchers weighted by aggregate throughput.

    Weights trade off throughput and source diversification:
      BaoStock: ~4.5 stocks/s (4 sub-processes × ~1.2/s) → weight 4
      AkShare:  ~0.9 stocks/s (rate-limited, single connection) → weight 2
      TuShare:  ~1.5 stocks/s (API quota-limited) → weight 2

    Each source gets a contiguous chunk (better for checkpoint resume).
    """
    if len(fetchers) == 1:
        return [work]

    weights = [_SOURCE_WEIGHT.get(f.name, 3) for f in fetchers]
    total_w = sum(weights)
    n = len(work)

    partitions: list[list[tuple[str, str | None, str]]] = []
    offset = 0
    for i, w in enumerate(weights):
        if i == len(weights) - 1:
            chunk = work[offset:]
        else:
            count = round(n * w / total_w)
            chunk = work[offset:offset + count]
            offset += count
        partitions.append(chunk)

    return partitions


# ---------------------------------------------------------------------------
# Main entry point — 3-phase parallel append
# ---------------------------------------------------------------------------

def append_with_source(
    fetchers: list[DataFetcher] | DataFetcher,
    target_dir: str | Path,
    end_date: str | None = None,
) -> None:
    """Append new data to an existing Qlib dataset (per-stock incremental).

    Supports **multi-source parallel** fetching: pass a list of DataFetcher
    instances and stocks will be partitioned across them, one thread per source.

    For backward compatibility, a single DataFetcher is also accepted.

    Uses data_meta.json to determine per-stock fetch ranges:
    - Each stock is fetched from its own data_end+1 to its target date.
    - Delisted stocks with sufficient data are skipped.
    - API errors are logged and skipped; failed stocks retain their old
      data_end and will be retried on the next run.
    - Global trusted_until is auto-derived from per-stock data_end values.
    """
    from datetime import timedelta

    if isinstance(fetchers, DataFetcher):
        fetchers = [fetchers]

    primary = fetchers[0]

    target = Path(target_dir).expanduser()
    checkpoint_path = target / ".append_progress"
    meta_path = target / META_FILENAME

    cal_path = target / "calendars" / "day.txt"
    if not cal_path.exists():
        raise FileNotFoundError(
            f"No existing calendar at {cal_path}. "
            "Run a full download first: python main.py download --full"
        )

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    existing_cal = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]

    # ── Phase 1: Prepare (sequential) ──────────────────────────────────────

    # Extend calendar
    last_date = existing_cal[-1]
    next_day = (
        datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    calendar_extended = False
    if next_day <= end_date:
        new_dates = primary.fetch_trade_calendar(next_day, end_date)
        if new_dates:
            logger.info(f"Extending calendar: {len(new_dates)} new days ({new_dates[0]} ~ {new_dates[-1]})")
            merged_cal = sorted(set(existing_cal) | set(new_dates))
            cal_dir = target / "calendars"
            cal_dir.mkdir(parents=True, exist_ok=True)
            (cal_dir / "day.txt").write_text("\n".join(merged_cal) + "\n")
            existing_cal = merged_cal
            calendar_extended = True

    if not calendar_extended:
        merged_cal = existing_cal

    cal_to_idx = {d: i for i, d in enumerate(merged_cal)}

    # Load or build meta
    has_meta = meta_path.exists()
    if has_meta:
        meta = DataMeta.load(meta_path)
        logger.info(f"Meta loaded: {len(meta.stocks)} stocks, trusted_until={meta.trusted_until}")
    else:
        logger.warning("data_meta.json not found. Building from scan...")
        stock_info = fetch_stock_listing_info(primary)
        meta = build_meta_from_scan(target, stock_info=stock_info)
        meta.save(meta_path)
        logger.info(f"Meta created: {len(meta.stocks)} stocks, trusted_until={meta.trusted_until}")

    # Resume checkpoint (unified across all workers)
    done_codes: set[str] = set()
    if checkpoint_path.exists():
        _, cp_end, done_codes = _load_checkpoint(checkpoint_path)

        # Reconcile: checkpoint-done stocks may not have had their meta
        # saved (periodic save every N stocks). Write their progress into
        # meta NOW so it survives even if the checkpoint is discarded.
        reconciled = 0
        for code in done_codes:
            sm = meta.stocks.get(code)
            if not sm:
                continue
            goal = DataMeta._fetch_end(sm, cp_end)
            if not sm.data_end or sm.data_end < goal:
                sm.data_end = goal
                reconciled += 1
        if reconciled:
            meta.save(meta_path)
            logger.info(f"Reconciled {reconciled} checkpoint-done stocks into meta")

        if cp_end != end_date:
            logger.info(
                f"Checkpoint target date changed ({cp_end} → {end_date}). "
                f"Meta reconciled, discarding stale checkpoint."
            )
            checkpoint_path.unlink()
            done_codes = set()
        else:
            logger.info(f"Resuming: {len(done_codes)} stocks already done")

    # Refresh stock listing (once per day)
    if meta.needs_listing_refresh():
        logger.info("Refreshing stock listing info (daily)...")
        stock_info = fetch_stock_listing_info(primary)
        for code, info in stock_info.items():
            sm = meta.stocks.get(code)
            if sm is None:
                meta.stocks[code] = StockMeta(
                    code=code,
                    ipo_date=info.get("ipo_date"),
                    delist_date=info.get("delist_date"),
                    status=info.get("status", "active"),
                )
            else:
                sm.delist_date = info.get("delist_date") or sm.delist_date
                sm.status = info.get("status", sm.status)
                sm.ipo_date = info.get("ipo_date") or sm.ipo_date
        meta.listing_updated = datetime.now().strftime("%Y-%m-%d")
        meta.save(meta_path)
        logger.info(f"Listing updated: {len(meta.stocks)} stocks")
    else:
        logger.info(f"Listing already refreshed today ({meta.listing_updated}), using cache")

    # Index components (best-effort, from primary source)
    logger.info("Fetching index components...")
    csi300 = primary.fetch_index_components("000300")
    csi500 = primary.fetch_index_components("000905")
    logger.info(f"CSI300: {len(csi300)}, CSI500: {len(csi500)}")

    # Load existing instrument metadata
    instruments: dict[str, tuple[str, str]] = {}
    inst_path = target / "instruments" / "all.txt"
    if inst_path.exists():
        for line in inst_path.read_text().strip().split("\n"):
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                instruments[parts[0]] = (parts[1], parts[2])

    # Build work list
    all_codes = sorted(meta.stocks.keys())
    work: list[tuple[str, str | None, str]] = []
    skip_delisted = 0
    skip_complete = 0

    for code in all_codes:
        needs, start, stock_end = meta.needs_fetch(code, end_date)
        if not needs:
            sm = meta.stocks[code]
            if sm.status == "delisted":
                skip_delisted += 1
            else:
                skip_complete += 1
        else:
            work.append((code, start, stock_end))

    total_work = len(work)
    if not calendar_extended and total_work == 0:
        logger.info(
            f"Data already up to date (trusted_until={meta.trusted_until}). "
            f"Nothing to append."
        )
        return

    logger.info(
        f"Work plan: {total_work} stocks to fetch, "
        f"{skip_complete} already complete, {skip_delisted} delisted (skipped)"
    )

    # Write checkpoint header if new run
    if not checkpoint_path.exists():
        _write_checkpoint_header(checkpoint_path, meta.trusted_until or "0000-00-00", end_date)

    # ── Phase 2: Parallel fetch ────────────────────────────────────────────

    partitions = _partition_work(work, fetchers)
    meta_lock = threading.Lock()
    t0 = time.time()

    source_names = [f.name for f in fetchers]
    for fname, partition in zip(source_names, partitions):
        logger.opt(colors=True).info(f"  {_colored_src(fname)} {len(partition)} stocks assigned")

    if len(fetchers) == 1:
        results = [_worker_fetch(
            fetcher=fetchers[0],
            work=partitions[0],
            meta=meta,
            cal_to_idx=cal_to_idx,
            target=target,
            checkpoint_path=checkpoint_path,
            meta_lock=meta_lock,
            meta_path=meta_path,
            done_codes=done_codes,
            fallback_start=next_day,
        )]
    else:
        results: list[WorkerResult] = []
        with ThreadPoolExecutor(max_workers=len(fetchers)) as pool:
            futures = {
                pool.submit(
                    _worker_fetch,
                    fetcher=f,
                    work=p,
                    meta=meta,
                    cal_to_idx=cal_to_idx,
                    target=target,
                    checkpoint_path=checkpoint_path,
                    meta_lock=meta_lock,
                    meta_path=meta_path,
                    done_codes=done_codes,
                    fallback_start=next_day,
                ): f.name
                for f, p in zip(fetchers, partitions)
            }
            for future in as_completed(futures):
                src_name = futures[future]
                try:
                    r = future.result()
                    results.append(r)
                    err_msg = f", {r.errors} errors" if r.errors else ""
                    logger.opt(colors=True).info(
                        f"{_colored_src(src_name)} finished: {r.success} ok, {r.empty} empty{err_msg}"
                    )
                except Exception as exc:
                    logger.opt(colors=True).error(f"{_colored_src(src_name)} worker crashed: {exc}")
                    results.append(WorkerResult(source_name=src_name, errors=1, error_codes=["CRASH"]))

    # ── Phase 2.5: Cross-source retry for failed stocks ─────────────────

    if len(fetchers) > 1:
        # Collect failed stock codes and the source that failed them.
        # Skip pseudo-codes like "subprocess#1" or "CRASH".
        failed_by_source: dict[str, str] = {}
        for r in results:
            for code in r.error_codes:
                if code.startswith("subprocess#") or code == "CRASH":
                    continue
                failed_by_source[code] = r.source_name

        if failed_by_source:
            retry_work: list[tuple[str, str | None, str]] = []
            for code in failed_by_source:
                needs, start, stock_end = meta.needs_fetch(code, end_date)
                if needs:
                    retry_work.append((code, start, stock_end))

            if retry_work:
                source_map = {f.name: f for f in fetchers}
                # Group retry stocks by the alternative source to use
                alt_groups: dict[str, list[tuple[str, str | None, str]]] = {}
                for code, start, stock_end in retry_work:
                    failed_src = failed_by_source[code]
                    alt = next((f.name for f in fetchers if f.name != failed_src), None)
                    if alt:
                        alt_groups.setdefault(alt, []).append((code, start, stock_end))

                total_retry = sum(len(v) for v in alt_groups.values())
                logger.info(
                    f"Cross-source retry: {total_retry} stocks failed, "
                    f"retrying with alternate sources"
                )

                retry_results: list[WorkerResult] = []
                for src_name, retry_partition in alt_groups.items():
                    logger.opt(colors=True).info(
                        f"  {_colored_src(src_name)} retrying {len(retry_partition)} stocks"
                    )
                    rr = _worker_fetch(
                        fetcher=source_map[src_name],
                        work=retry_partition,
                        meta=meta,
                        cal_to_idx=cal_to_idx,
                        target=target,
                        checkpoint_path=checkpoint_path,
                        meta_lock=meta_lock,
                        meta_path=meta_path,
                        done_codes=set(),
                        fallback_start=next_day,
                    )
                    retry_results.append(rr)
                    logger.opt(colors=True).info(
                        f"  {_colored_src(src_name)} retry done: "
                        f"{rr.success} ok, {rr.empty} empty, {rr.errors} still failed"
                    )

                results.extend(retry_results)

    # ── Phase 3: Finalize ──────────────────────────────────────────────────

    # Merge instrument fragments from all workers
    for r in results:
        for sym, (s, e) in r.instruments.items():
            if sym in instruments:
                old_s, old_e = instruments[sym]
                instruments[sym] = (min(old_s, s), max(old_e, e))
            else:
                instruments[sym] = (s, e)

    # Write instruments files
    inst_dir = target / "instruments"
    inst_dir.mkdir(parents=True, exist_ok=True)

    lines = [f"{sym}\t{s}\t{e}" for sym, (s, e) in sorted(instruments.items())]
    (inst_dir / "all.txt").write_text("\n".join(lines) + "\n")

    for filename, raw_codes in [("csi300.txt", csi300), ("csi500.txt", csi500)]:
        if not raw_codes:
            continue
        qlib_codes = {_to_qlib_symbol(c) for c in raw_codes}
        subset = [f"{sym}\t{s}\t{e}" for sym, (s, e) in sorted(instruments.items()) if sym in qlib_codes]
        if subset:
            (inst_dir / filename).write_text("\n".join(subset) + "\n")

    # Aggregate stats
    elapsed_total = time.time() - t0
    total_success = sum(r.success for r in results)
    total_empty = sum(r.empty for r in results)
    total_resumed = sum(r.resumed for r in results)
    total_errors = sum(r.errors for r in results)
    all_error_codes = [c for r in results for c in r.error_codes]

    checkpoint_path.unlink(missing_ok=True)

    if total_errors:
        logger.warning(
            f"{total_errors} stocks failed (will retry next run): "
            f"{', '.join(all_error_codes[:20])}"
            f"{'...' if len(all_error_codes) > 20 else ''}"
        )

    meta.last_append = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta.save(meta_path)

    logger.info(
        f"Append done: "
        f"{total_success} updated, {total_empty} empty, "
        f"{total_errors} errors, {total_resumed} resumed "
        f"({len(fetchers)} source{'s' if len(fetchers) > 1 else ''}) "
        f"in {elapsed_total / 60:.1f}min"
    )
    logger.info(
        f"Calendar: {len(merged_cal)} days ({merged_cal[0]} ~ {merged_cal[-1]}), "
        f"trusted_until: {meta.trusted_until or '(none)'}"
    )
