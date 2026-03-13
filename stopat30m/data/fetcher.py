"""A-share data fetchers and Qlib format converter.

Supported sources:
  - akshare: Free, no auth. Uses east-money backend. Latest data available.
  - tushare: Paid (token required). Higher quality, richer fields.

Both sources download OHLCV daily data and convert it into Qlib's binary
format (calendars, instruments, features/*.day.bin).
"""

from __future__ import annotations

import struct
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger


QLIB_FIELDS = ("open", "close", "high", "low", "volume", "change", "factor")

# Qlib instrument naming: SH600000, SZ000001
_SH_PREFIXES = ("6",)
_SZ_PREFIXES = ("0", "3")


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
    """

    name = "baostock"

    def __init__(self):
        try:
            import baostock as bs
        except ImportError:
            raise ImportError("baostock not installed. Run: pip install baostock")
        self._bs = bs
        self._logged_in = False
        self._request_count = 0
        self._relogin_every = 200

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


def append_with_source(
    fetcher: DataFetcher,
    target_dir: str | Path,
    end_date: str | None = None,
    retry_skipped: bool = False,
) -> None:
    """Append new data to an existing Qlib dataset.

    Reads the existing calendar to find the last available date, then fetches
    only new data from (last_date + 1) to end_date.  Binary files for each
    stock are extended in place; new stocks get fresh files.

    Supports **resume on failure**: progress is tracked in a checkpoint file
    (.append_progress).  If the process is interrupted and restarted, already-
    completed stocks are skipped automatically.

    When ``retry_skipped=True``, re-attempts stocks whose binary data doesn't
    cover the full calendar range (likely skipped due to rate limiting in a
    previous run).

    Args:
        fetcher: DataFetcher instance
        target_dir: Existing Qlib data directory
        end_date: End date (defaults to today)
        retry_skipped: If True, retry stocks with incomplete data
    """
    from datetime import timedelta

    target = Path(target_dir).expanduser()
    checkpoint_path = target / ".append_progress"

    cal_path = target / "calendars" / "day.txt"
    if not cal_path.exists():
        raise FileNotFoundError(
            f"No existing calendar at {cal_path}. "
            "Run a full download first (without --append)."
        )

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # --- Resume or fresh start ---
    done_codes: set[str] = set()

    existing_cal = [d.strip() for d in cal_path.read_text().strip().split("\n") if d.strip()]

    if checkpoint_path.exists() and not retry_skipped:
        # Resume an interrupted run
        fetch_start, end_date, done_codes = _load_checkpoint(checkpoint_path)
        logger.info(
            f"Resuming interrupted append: {len(done_codes)} stocks already done, "
            f"range {fetch_start} ~ {end_date}"
        )
        merged_cal = existing_cal
    else:
        # Normal append: extend calendar if there are new dates
        last_date = existing_cal[-1]
        next_day = (
            datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        calendar_extended = False
        if next_day <= end_date:
            new_dates = fetcher.fetch_trade_calendar(next_day, end_date)
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

        if retry_skipped:
            # Retry mode: find the fetch_start from stock binary data gaps
            logger.info("Scanning stocks for incomplete data...")
            features_dir = target / "features"
            sample_start = None
            if features_dir.exists():
                for sym_dir in sorted(features_dir.iterdir())[:50]:
                    close_bin = sym_dir / "close.day.bin"
                    if close_bin.exists():
                        try:
                            si, data = _read_bin_file(close_bin)
                            data_end_idx = si + len(data)
                            if data_end_idx < len(merged_cal):
                                sample_start = merged_cal[data_end_idx]
                                break
                        except Exception:
                            continue
            fetch_start = sample_start or next_day
            logger.info(f"Retry-skipped mode: re-fetching incomplete stocks")
            logger.info(f"Fetch range: {fetch_start} ~ {end_date}")
            checkpoint_path.unlink(missing_ok=True)
            _write_checkpoint_header(checkpoint_path, fetch_start, end_date)
        else:
            # Pure append mode
            if not calendar_extended:
                logger.info(f"Data already up to date (last date: {last_date}). Nothing to append.")
                return
            fetch_start = next_day
            logger.info(f"Data source: {fetcher.name} (append mode)")
            logger.info(f"Existing data ends: {last_date}")
            logger.info(f"Fetching new data: {fetch_start} ~ {end_date}")
            _write_checkpoint_header(checkpoint_path, fetch_start, end_date)

    cal_to_idx = {d: i for i, d in enumerate(merged_cal)}

    # --- Stock list & index components ---
    logger.info("Fetching stock list...")
    stock_codes = fetcher.fetch_stock_list()
    logger.info(f"Active stocks: {len(stock_codes)}")

    logger.info("Fetching index components...")
    csi300 = fetcher.fetch_index_components("000300")
    csi500 = fetcher.fetch_index_components("000905")
    logger.info(f"CSI300: {len(csi300)}, CSI500: {len(csi500)}")

    # --- Load existing instrument metadata ---
    instruments: dict[str, tuple[str, str]] = {}
    inst_path = target / "instruments" / "all.txt"
    if inst_path.exists():
        for line in inst_path.read_text().strip().split("\n"):
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                instruments[parts[0]] = (parts[1], parts[2])

    # --- In retry mode, filter to only stocks that need data ---
    if retry_skipped:
        needs_retry = []
        for code in stock_codes:
            qlib_sym = _to_qlib_symbol(code)
            feat_dir = target / "features" / qlib_sym.lower()
            if _stock_needs_data(feat_dir, len(merged_cal)):
                needs_retry.append(code)
        logger.info(f"Stocks needing retry: {len(needs_retry)} / {len(stock_codes)}")
        stock_codes = needs_retry

    # --- Download & append stock-by-stock ---
    total = len(stock_codes)
    success = 0
    skipped = 0
    resumed = 0
    failed = 0
    t0 = time.time()
    log_interval = 20 if total < 500 else 100
    logger.info(f"Starting download: {total} stocks to process...")

    for i, code in enumerate(stock_codes, 1):
        if i % log_interval == 0 or i == total or i == 1:
            elapsed = time.time() - t0
            processed = success + skipped + failed
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = total - i
            eta = remaining / rate if rate > 0 else 0
            logger.info(
                f"[{i}/{total}] {success} ok, {skipped} skip, {failed} fail"
                f"{f', {resumed} resumed' if resumed else ''}"
                f" | {rate:.1f} stocks/s, ETA {eta / 60:.0f}min"
            )

        if code in done_codes:
            resumed += 1
            continue

        try:
            df = fetcher.fetch_daily(code, fetch_start, end_date)
        except Exception as e:
            logger.debug(f"Exception for {code}: {e}")
            failed += 1
            continue

        if df is None or df.empty:
            skipped += 1
            _checkpoint_stock(checkpoint_path, code)
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
        if qlib_sym in instruments:
            old_s, old_e = instruments[qlib_sym]
            instruments[qlib_sym] = (min(old_s, dates_in_data[0]), max(old_e, dates_in_data[-1]))
        else:
            instruments[qlib_sym] = (dates_in_data[0], dates_in_data[-1])

        success += 1
        _checkpoint_stock(checkpoint_path, code)

    # --- Finalize: write instruments ---
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

    # Remove checkpoint — job fully done
    checkpoint_path.unlink(missing_ok=True)

    elapsed_total = time.time() - t0
    logger.info(
        f"Append complete: {success} updated, {skipped} skipped, {failed} failed"
        f"{f', {resumed} resumed' if resumed else ''}"
        f" in {elapsed_total / 60:.1f}min"
    )
    logger.info(
        f"Calendar: {len(merged_cal)} days ({merged_cal[0]} ~ {merged_cal[-1]}), "
        f"Instruments: {len(instruments)} stocks"
    )
