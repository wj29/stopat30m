"""Immutable, source-aligned raw/adjusted daily prices for reproducible research.

These artifacts are research inputs, not a cash-account corporate-action ledger.
Integer-share executions must never use adjusted prices. No price or calendar
is forward-filled, inferred from weekdays, or borrowed from another provider.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import re
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .research_http import ResearchDownloadError, ResearchHTTPTransport

SCHEMA_VERSION = 1
_OHLC = ("open", "high", "low", "close")
_BAOSTOCK_LOCK = threading.RLock()  # The package's authenticated socket is a singleton.
_BAOSTOCK_DOCS = "https://www.baostock.com/"
_EASTMONEY_KLINE = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
_AKSHARE_ETF_ADAPTER = "https://github.com/akfamily/akshare/blob/main/akshare/fund/fund_etf_em.py"
_TENCENT_KLINE = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
_AKSHARE_TENCENT_ADAPTER = "https://github.com/akfamily/akshare/blob/main/akshare/stock_feature/stock_hist_tx.py"
LIMITATIONS = [
    "Adjusted prices and observed adjustment factors are research series, not executable market prices.",
    "Cash dividends, share distributions, rights issues and their effective dates are not reconstructed from price ratios.",
    "Corporate-action cash/share accounting is incomplete; execution_ready is false for integer-share account simulation.",
    "A user-specified static pool is not evidence of historical index membership or an unbiased selection process.",
    "Missing bars remain missing; trading status may be unknown and does not prove open-auction liquidity.",
    "Adjusted history is the provider vintage fetched for this snapshot; historical point-in-time adjustment vintages are not verified.",
]


class ResearchDataError(ValueError):
    """A provider response or cached artifact cannot support the stated semantics."""


class ResearchPriceProvider(Protocol):
    source: str
    volume_unit: str
    amount_unit: str
    def fetch_calendar(self, start: str, end: str) -> pd.DataFrame: ...
    def fetch_raw(self, instrument: str, start: str, end: str) -> pd.DataFrame: ...
    def fetch_adjusted(self, instrument: str, start: str, end: str, adjustment: str) -> pd.DataFrame: ...


def _day(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ResearchDataError("Dates must use canonical YYYY-MM-DD.")
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ResearchDataError("Invalid calendar date.") from exc


def _instrument(value: str) -> str:
    value = str(value).upper().strip()
    match = re.fullmatch(r"(SH|SZ)\.?([0-9]{6})", value)
    if match:
        return match[1] + match[2]
    match = re.fullmatch(r"([0-9]{6})\.(SH|SZ)", value)
    if match:
        return match[2] + match[1]
    if re.fullmatch(r"[036][0-9]{5}", value):
        return ("SH" if value.startswith("6") else "SZ") + value
    raise ResearchDataError("Use explicit SH/SZ codes for ETFs and benchmarks, e.g. SH510300 or SH000300.")


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _manifest_identity(manifest: dict[str, Any]) -> dict[str, Any]:
    # Retrieval time is descriptive. Every data/semantics field is pinned to
    # the content address, so editing metadata cannot relabel existing prices.
    return {key: value for key, value in manifest.items() if key not in {"artifact_id", "created_at"}}


class BaoStockResearchProvider:
    """Anonymous BaoStock raw flag 3 plus same-source HFQ flag 1/QFQ flag 2.

    Volume comes only from the unadjusted endpoint and is measured in shares;
    amount is CNY. Adjusted volume is deliberately never used. Index series are
    downloaded unadjusted twice and explicitly marked as identity adjustment.
    """
    source = "baostock"
    source_url = _BAOSTOCK_DOCS
    volume_unit = "shares"
    amount_unit = "CNY"
    calendar_source = "baostock.query_trade_dates"
    calendar_source_url = _BAOSTOCK_DOCS

    def __init__(self, client=None):
        if client is None:
            try:
                import baostock as client
            except ImportError as exc:
                raise ResearchDataError("Install baostock to download real daily research data.") from exc
        self._client = client
        self._logged_in = False
        self._info: dict[str, dict] = {}
        try:
            self.version = importlib.metadata.version("baostock")
        except importlib.metadata.PackageNotFoundError:
            self.version = "unknown"

    def _login(self) -> None:
        if not self._logged_in:
            response = self._client.login()
            if str(response.error_code) != "0":
                raise ResearchDataError(f"BaoStock login failed: {response.error_code} {response.error_msg}")
            self._logged_in = True

    @staticmethod
    def _frame(response, context: str) -> pd.DataFrame:
        if str(response.error_code) != "0":
            raise ResearchDataError(f"BaoStock {context} failed: {response.error_code} {response.error_msg}")
        # BaoStock 0.9.4 get_data() still calls the removed pandas.DataFrame.append
        # on its second page. Iterate the public row cursor instead; next() owns
        # page fetching and may update error_code at any page boundary.
        fields = list(response.fields)
        rows = []
        while response.next():
            if str(response.error_code) != "0":
                raise ResearchDataError(f"BaoStock {context} pagination failed: {response.error_code} {response.error_msg}")
            row = response.get_row_data()
            if len(row) != len(fields):
                raise ResearchDataError(f"BaoStock {context} returned a malformed paginated row.")
            rows.append(row)
        if str(response.error_code) != "0":
            raise ResearchDataError(f"BaoStock {context} pagination failed: {response.error_code} {response.error_msg}")
        frame = pd.DataFrame(rows, columns=fields)
        if frame is None or frame.empty:
            raise ResearchDataError(f"BaoStock {context} returned no rows.")
        return frame

    def instrument_info(self, instrument: str) -> dict:
        instrument = _instrument(instrument)
        with _BAOSTOCK_LOCK:
            self._login()
            if instrument not in self._info:
                code = instrument[:2].lower() + "." + instrument[2:]
                frame = self._frame(self._client.query_stock_basic(code=code), f"basic {code}")
                if "code" not in frame or len(frame) != 1 or str(frame.iloc[0]["code"]).lower() != code:
                    raise ResearchDataError(f"Unexpected BaoStock security metadata for {instrument}.")
                row = frame.iloc[0]
                kind = {"1": "stock", "2": "index", "5": "ETF"}.get(str(row.get("type", "")), "other")
                self._info[instrument] = {"asset_type": kind, "provider_type": str(row.get("type", "")),
                    "name": str(row.get("code_name", "")), "ipo_date": str(row.get("ipoDate", "")),
                    "delist_date": str(row.get("outDate", "")),
                    "adjustment_applied": "none_index" if kind == "index" else "provider_adjustment"}
            return dict(self._info[instrument])

    def fetch_calendar(self, start: str, end: str) -> pd.DataFrame:
        with _BAOSTOCK_LOCK:
            self._login()
            return self._frame(self._client.query_trade_dates(start_date=start, end_date=end), "trade calendar").rename(columns={"calendar_date": "date"})

    def _fetch(self, instrument: str, start: str, end: str, flag: str) -> pd.DataFrame:
        instrument = _instrument(instrument)
        with _BAOSTOCK_LOCK:
            info = self.instrument_info(instrument)
            code = instrument[:2].lower() + "." + instrument[2:]
            # Stock and ETF daily records have status flags. Index records do not.
            fields = "date,code,open,high,low,close,volume,amount,adjustflag"
            if info["asset_type"] in {"stock", "ETF"}:
                fields += ",tradestatus,isST"
            actual_flag = "3" if info["asset_type"] == "index" else flag
            frame = self._frame(self._client.query_history_k_data_plus(code, fields,
                start_date=start, end_date=end, frequency="d", adjustflag=actual_flag), f"daily {code} flag={actual_flag}")
            if "adjustflag" not in frame or not frame["adjustflag"].astype(str).eq(actual_flag).all():
                raise ResearchDataError(f"Provider returned an unexpected adjustment mode for {instrument}.")
            return frame

    def fetch_raw(self, instrument: str, start: str, end: str) -> pd.DataFrame:
        return self._fetch(instrument, start, end, "3")

    def fetch_adjusted(self, instrument: str, start: str, end: str, adjustment: str) -> pd.DataFrame:
        return self._fetch(instrument, start, end, {"hfq": "1", "qfq": "2"}[adjustment])

    def close(self) -> None:
        with _BAOSTOCK_LOCK:
            if self._logged_in:
                try:
                    self._client.logout()
                finally:
                    self._logged_in = False


class EastmoneyResearchProvider:
    """Explicit Eastmoney same-source raw/HFQ/QFQ daily prices.

    Endpoint modes follow AKShare's published fund_etf_hist_em adapter: fqt 0
    raw, 1 QFQ, 2 HFQ. Reported volume is 100-unit lots; raw_volume converts
    these to nominal shares/ETF units, preserving the reporting resolution in
    the manifest. This does not recover odd-lot precision absent at source.
    The exchange calendar and security type metadata come from BaoStock and
    retain separate provenance. There is no automatic source fallback.
    """
    source, version = "eastmoney", "kline-get-v1"
    source_url = _EASTMONEY_KLINE
    volume_unit, amount_unit = "shares", "CNY"
    volume_provenance = {"source_unit": "lots_of_100_units", "multiplier_to_shares": 100,
        "source_resolution": "as_reported_lots", "odd_lot_precision_verified": False,
        "semantics_reference": _AKSHARE_ETF_ADAPTER}
    adjustment_provenance = {"endpoint": _EASTMONEY_KLINE,
        "modes": {"raw": "fqt=0", "qfq": "fqt=1", "hfq": "fqt=2"},
        "adapter_reference": _AKSHARE_ETF_ADAPTER,
        "provider_mode_echo_available": False}

    def __init__(self, session=None, calendar_provider=None, *, timeout: float = 20,
                 cache_dir=None, refresh: bool = False, clock=None, sleep=None, random=None,
                 min_interval: float = 2, max_attempts: int = 4):
        if session is None:
            import requests
            session = requests.Session()
            self._owned_session = True
        else:
            self._owned_session = False
        self._session = session
        self._calendar_provider = calendar_provider or BaoStockResearchProvider()
        self._owned_calendar = calendar_provider is None
        calendar_name = str(self._calendar_provider.source)
        self.calendar_source = getattr(self._calendar_provider, "calendar_source", f"{calendar_name}.fetch_calendar")
        self.calendar_source_url = getattr(self._calendar_provider, "calendar_source_url", getattr(self._calendar_provider, "source_url", ""))
        self._timeout = timeout
        if not np.isfinite(timeout) or not 1 <= timeout <= 60:
            raise ResearchDataError("Eastmoney timeout must be between 1 and 60 seconds.")
        self._refresh_requested = refresh
        self._info: dict[str, dict] = {}
        self._transport = ResearchHTTPTransport(session, cache_dir=cache_dir, clock=clock, sleep=sleep,
            random=random, min_interval=min_interval, max_attempts=max_attempts)

    def _download_scope(self, request):
        return {"provider": {"name": self.source, "version": self.version, "endpoint": self.source_url}, "request": request}

    @contextmanager
    def download_lock(self, cache_dir):
        self._transport.configure_cache(Path(cache_dir) / "http" / self.source)
        try:
            with self._transport.download_lock():
                yield
        except ResearchDownloadError as exc:
            raise ResearchDataError(str(exc)) from exc

    def has_pending_download(self, cache_dir, request):
        self._transport.configure_cache(Path(cache_dir) / "http" / self.source)
        try:
            return self._transport.has_pending(self._download_scope(request))
        except ResearchDownloadError as exc:
            raise ResearchDataError(str(exc)) from exc

    def begin_download(self, cache_dir, request, *, refresh=False):
        self._transport.configure_cache(Path(cache_dir) / "http" / self.source)
        try:
            generation = self._transport.begin_request(self._download_scope(request), refresh=refresh or self._refresh_requested)
            self._refresh_requested = False
            return generation
        except ResearchDownloadError as exc:
            raise ResearchDataError(str(exc)) from exc

    def complete_download(self, artifact_id):
        try:
            self._transport.complete(artifact_id)
        except ResearchDownloadError as exc:
            raise ResearchDataError(str(exc)) from exc

    def download_provenance(self):
        return self._transport.provenance()

    def instrument_info(self, instrument: str) -> dict:
        instrument = _instrument(instrument)
        if instrument not in self._info:
            if not hasattr(self._calendar_provider, "instrument_info"):
                raise ResearchDataError("Eastmoney requires independently identified security types.")
            info = dict(self._calendar_provider.instrument_info(instrument))
            if info.get("asset_type") not in {"stock", "ETF", "index"}:
                raise ResearchDataError(f"Unverified security type for {instrument}.")
            info["metadata_source"] = str(self._calendar_provider.source)
            info["adjustment_applied"] = "none_index" if info["asset_type"] == "index" else "provider_adjustment"
            self._info[instrument] = info
        return dict(self._info[instrument])

    def fetch_calendar(self, start: str, end: str) -> pd.DataFrame:
        return self._calendar_provider.fetch_calendar(start, end)

    def _fetch(self, instrument: str, start: str, end: str, flag: str) -> pd.DataFrame:
        instrument = _instrument(instrument)
        info = self.instrument_info(instrument)
        market = 1 if instrument.startswith("SH") else 0
        actual_flag = "0" if info["asset_type"] == "index" else flag
        params = {"secid": f"{market}.{instrument[2:]}", "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61", "klt": "101",
            # Public application identifier used in AKShare's official adapter;
            # this is not a user's authentication token or secret.
            "ut": "7eea3edcaed734bea9cbfc24409ed989",
            "fqt": actual_flag, "beg": start.replace("-", ""), "end": end.replace("-", ""), "lmt": "100000"}
        parsed = None
        def validate(payload):
            nonlocal parsed
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(payload, dict) or str(payload.get("rc")) != "0" or not isinstance(data, dict) or not data.get("klines"):
                raise ResearchDataError(f"Eastmoney returned no valid daily data for {instrument}.")
            if str(data.get("code")) != instrument[2:] or str(data.get("market")) != str(market):
                raise ResearchDataError(f"Eastmoney returned the wrong instrument/market for {instrument}.")
            if data.get("truncated") is True:
                raise ResearchDataError(f"Eastmoney explicitly marked the interval truncated for {instrument}; "
                    "no verified paging contract is available, refusing a partial response.")
            for line in data["klines"]:
                values = line.split(",") if isinstance(line, str) else []
                if len(values) != 11:
                    raise ResearchDataError(f"Malformed Eastmoney daily bar for {instrument}.")
            decimal = data.get("decimal")
            if isinstance(decimal, bool) or not re.fullmatch(r"[0-8]", str(decimal)):
                raise ResearchDataError(f"Eastmoney did not declare usable OHLC decimal precision for {instrument}.")
            resolution = 10.0 ** -int(decimal)
            rows = [line.split(",") for line in data["klines"]]
            frame = pd.DataFrame(rows, columns=["date", "open", "close", "high", "low", "volume", "amount",
                "amplitude", "change_pct", "change", "turnover"])
            frame["code"] = instrument
            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce") * 100
            frame["tradestatus"], frame["isST"] = "", ""
            frame = _bars(frame, instrument, start, end, raw=actual_flag == "0")
            if info["asset_type"] != "index" and actual_flag == "0":
                volume, amount = frame["volume"], frame["amount"]
                vwap = amount / volume.replace(0, np.nan)
                tolerance = frame["high"] * 100 / volume.replace(0, np.nan) + resolution
                invalid = (volume > 0) & ((vwap < frame["low"] - tolerance) | (vwap > frame["high"] + tolerance))
                if invalid.any():
                    raise ResearchDataError(f"Eastmoney raw volume unit/amount does not match OHLC for {instrument}.")
            precision = self._info[instrument].setdefault("ohlc_resolution_by_fqt", {})
            if actual_flag in precision and precision[actual_flag] != resolution:
                raise ResearchDataError(f"Eastmoney OHLC precision changed across segments for {instrument}.")
            precision[actual_flag] = resolution
            self._info[instrument]["factor_semantics"] = "observed_close_ratio_approximate_under_declared_rounding"
            frame.attrs["price_resolution"] = resolution
            parsed = frame
        try:
            self._transport.get(params, timeout=self._timeout,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"}, validate=validate)
        except ResearchDownloadError as exc:
            raise ResearchDataError(str(exc)) from exc
        # No claims that fqt was echoed or every corporate action was applied;
        # discontinuity checks below are a rejection gate, not a repair.
        return parsed

    def fetch_raw(self, instrument: str, start: str, end: str) -> pd.DataFrame:
        return self._fetch(instrument, start, end, "0")

    def fetch_adjusted(self, instrument: str, start: str, end: str, adjustment: str) -> pd.DataFrame:
        return self._fetch(instrument, start, end, {"hfq": "2", "qfq": "1"}[adjustment])

    def close(self) -> None:
        if self._owned_session:
            self._session.close()
        if self._owned_calendar:
            self._calendar_provider.close()


class _TencentHTTPTransport(ResearchHTTPTransport):
    endpoint, label = _TENCENT_KLINE, "Tencent"

    def decode_response(self, response):
        text = response.text.strip()
        if text.startswith("{"):
            return json.loads(text)
        # The official public adapter uses _var and returns an assignment,
        # not executable input. Parse only a JSON object, never eval/JS.
        match = re.fullmatch(r"(?:var\s+)?[A-Za-z_$][\w$]*\s*=\s*(\{.*\})\s*;?", text, re.S)
        if match is None:
            raise ValueError("Invalid Tencent JSONP envelope")
        return json.loads(match[1])


class TencentResearchProvider(EastmoneyResearchProvider):
    """Explicit Tencent raw/HFQ pairs with overlapping two-year windows.

    Tencent returns up to 640 bars ending at the requested end, potentially
    before the requested start. Full overlapping rows must match exactly
    before deduplication; final cropping never invents a missing session.
    Volume/amount conventions follow AKShare's first-party adapter and are
    checked against raw OHLC. Calendars/security identities remain BaoStock.
    """
    source, version = "tencent", "newfqkline-v1"
    source_url = _TENCENT_KLINE
    volume_provenance = {"source_unit": "lots_for_ETF_and_stock_except_STAR_index_shares",
        "etf_multiplier_to_shares": 100, "index_multiplier_to_shares": 1,
        "amount_source_unit": "10000_CNY", "amount_multiplier_to_CNY": 10000,
        "semantics_reference": _AKSHARE_TENCENT_ADAPTER,
        "raw_ohlc_volume_amount_crosscheck": True}
    adjustment_provenance = {"endpoint": _TENCENT_KLINE,
        "modes": {"raw": "day", "hfq": "hfqday", "qfq": "qfqday"},
        "adapter_reference": _AKSHARE_TENCENT_ADAPTER,
        "provider_mode_echo_available": True,
        "window_policy": "two_calendar_years_640_bars_with_exact_overlap_checks",
        "ohlc_precision": "observed_fixed_decimal_serialization_not_exchange_tick",
        "known_action_probe": "SH510500 2015-04-15 consolidation adjusted discontinuity removed",
        "adjustment_transform": "affine_cash_accumulation",
        "issuer_evidence": "repository:docs/research/tencent_adjustment_evidence.json"}
    price_semantics = {"adjustment_transform": "affine_cash_accumulation",
        "adjustment_factor": "observed_close_ratio_only_not_a_uniform_OHLC_multiplier",
        "return_interpretation": "provider_adjusted_price_returns_not_dividend_reinvestment_total_return",
        "dividend_reinvestment_verified": False}
    limitations = [
        "Tencent HFQ is an affine cumulative-cash adjusted-price series; close ratios are not uniform OHLC multipliers.",
        "Returns on this provider series are not verified dividend-reinvestment total returns or investable share/cash-account profits.",
        "Three-decimal adjusted ETF serialization can materially affect short-horizon returns; prices are preserved, not repaired.",
        "Issuer action evidence checks selected intervals only; a complete three-ETF corporate-action event ledger is not yet reconstructed.",
    ]

    def __init__(self, session=None, calendar_provider=None, **kwargs):
        super().__init__(session=session, calendar_provider=calendar_provider, **kwargs)
        self._transport = _TencentHTTPTransport(self._session, cache_dir=kwargs.get("cache_dir"),
            clock=kwargs.get("clock"), sleep=kwargs.get("sleep"), random=kwargs.get("random"),
            min_interval=kwargs.get("min_interval", 2), max_attempts=kwargs.get("max_attempts", 4))

    def _fetch_window(self, inst, end, year, mode):
        info = self.instrument_info(inst)
        symbol = inst.lower()
        params = {"_var": f"kline_day{mode}{year}",
            "param": f"{symbol},day,{year}-01-01,{end},640,{mode}", "r": "0.8205512681390605"}
        parsed = None

        def validate(payload):
            nonlocal parsed
            if not isinstance(payload, dict) or str(payload.get("code")) != "0":
                raise ResearchDataError(f"Tencent returned invalid daily data for {inst}.")
            data = payload.get("data")
            data = data.get(symbol) if isinstance(data, dict) else None
            field = f"{mode}day" if mode else "day"
            rows = data.get(field) if isinstance(data, dict) else None
            if not isinstance(rows, list) or not rows:
                raise ResearchDataError(f"Tencent did not return explicit {field} mode for {inst}.")
            if any(not isinstance(row, list) or not 9 <= len(row) <= 11 for row in rows):
                raise ResearchDataError(f"Malformed Tencent daily row for {inst}.")
            prices = [str(value) for row in rows for value in row[1:5]]
            decimal_counts = {len(value.partition(".")[2]) for value in prices
                if re.fullmatch(r"[0-9]+\.[0-9]{1,8}", value)}
            if len(decimal_counts) != 1 or any(not re.fullmatch(r"[0-9]+\.[0-9]{1,8}", value) for value in prices):
                raise ResearchDataError(f"Tencent OHLC serialization precision is not uniform for {inst}.")
            resolution = 10.0 ** -next(iter(decimal_counts))
            frame = pd.DataFrame([{"date": row[0], "open": row[1], "close": row[2], "high": row[3],
                "low": row[4], "volume": row[5], "amount": row[8], "code": inst,
                "tradestatus": "", "isST": ""} for row in rows])
            multiplier = 1 if info["asset_type"] == "index" or inst.startswith("SH688") else 100
            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce") * multiplier
            frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce") * 10000
            earliest = min(_day(row[0]) for row in rows)
            frame = _bars(frame, inst, earliest, end, raw=not mode)
            if not mode and info["asset_type"] != "index":
                volume, amount = frame["volume"], frame["amount"]
                vwap = amount / volume.replace(0, np.nan)
                # Reported amount is rounded to .01万元; lots can carry .01手.
                tolerance = 50 / volume.replace(0, np.nan) + resolution
                invalid = (volume > 0) & ((vwap < frame["low"] - tolerance) | (vwap > frame["high"] + tolerance))
                if invalid.any():
                    raise ResearchDataError(f"Tencent raw volume/amount unit does not match OHLC for {inst}.")
            precision = self._info[inst].setdefault("ohlc_resolution_by_mode", {})
            if field in precision and precision[field] != resolution:
                raise ResearchDataError(f"Tencent OHLC precision changed across windows for {inst}.")
            precision[field] = resolution
            self._info[inst]["factor_semantics"] = "observed_close_ratio_approximate_under_serialized_rounding"
            self._info[inst].setdefault("response_versions_by_mode", {}).setdefault(field, [])
            versions = self._info[inst]["response_versions_by_mode"][field]
            version = str(data.get("version", "unknown"))
            if version not in versions:
                versions.append(version)
            frame.attrs["price_resolution"] = resolution
            frame.attrs["source_rows"] = {row[0]: json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows}
            parsed = frame

        try:
            self._transport.get(params, timeout=self._timeout, headers={"User-Agent": "Mozilla/5.0"}, validate=validate)
        except ResearchDownloadError as exc:
            raise ResearchDataError(str(exc)) from exc
        return parsed

    def _fetch(self, instrument, start, end, flag):
        inst, start, end = _instrument(instrument), _day(start), _day(end)
        mode = "" if self.instrument_info(inst)["asset_type"] == "index" else flag
        chunks, evidence, resolution = [], {}, None
        for year in range(int(start[:4]), int(end[:4]) + 1, 2):
            window_end = min(end, f"{year + 1}-12-31")
            frame = self._fetch_window(inst, window_end, year, mode)
            for day, row in frame.attrs["source_rows"].items():
                if day in evidence and evidence[day] != row:
                    raise ResearchDataError(f"Tencent overlapping {mode or 'raw'} history changed for {inst} on {day}; refusing mixed vintages.")
                evidence[day] = row
            resolution = frame.attrs["price_resolution"]
            chunks.append(frame)
        merged = pd.concat(chunks, ignore_index=True).drop_duplicates("date").sort_values("date")
        merged = merged[merged["date"].between(start, end)].reset_index(drop=True)
        if merged.empty:
            raise ResearchDataError(f"Tencent has no requested daily history for {inst}.")
        merged.attrs["price_resolution"] = resolution
        merged.attrs["adjustment_transform"] = "affine_cash_accumulation"
        return merged

    def fetch_raw(self, instrument, start, end):
        return self._fetch(instrument, start, end, "")

    def fetch_adjusted(self, instrument, start, end, adjustment):
        if adjustment not in {"hfq", "qfq"}:
            raise ResearchDataError("Tencent adjustment must be hfq or qfq.")
        return self._fetch(instrument, start, end, adjustment)


def _calendar(frame: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    frame = frame.rename(columns={"calendar_date": "date"}).copy()
    if not {"date", "is_trading_day"}.issubset(frame.columns):
        raise ResearchDataError("Calendar requires natural-day date/is_trading_day flags.")
    frame = frame[["date", "is_trading_day"]]
    frame["date"] = frame["date"].map(_day)
    flags = pd.to_numeric(frame["is_trading_day"], errors="coerce")
    if not flags.isin([0, 1]).all() or frame["date"].duplicated().any():
        raise ResearchDataError("Calendar flags are invalid or duplicated.")
    frame["is_trading_day"] = flags.astype(int)
    frame = frame.sort_values("date").reset_index(drop=True)
    expected = pd.date_range(start, end).strftime("%Y-%m-%d").tolist()
    if frame["date"].tolist() != expected:
        raise ResearchDataError("Provider calendar does not cover every requested natural day.")
    if not frame["is_trading_day"].any():
        raise ResearchDataError("Requested range has no confirmed trading sessions.")
    return frame


def _bars(frame: pd.DataFrame, inst: str, start: str, end: str, *, raw: bool) -> pd.DataFrame:
    required = {"date", *_OHLC} | ({"volume", "amount"} if raw else set())
    if frame is None or frame.empty or not required.issubset(frame.columns):
        raise ResearchDataError(f"Missing required {'raw' if raw else 'adjusted'} bars for {inst}.")
    frame = frame.copy()
    frame["date"] = frame["date"].map(_day)
    if frame["date"].duplicated().any() or not frame["date"].between(start, end).all():
        raise ResearchDataError(f"Duplicate or out-of-range daily prices for {inst}.")
    if "code" in frame and not frame["code"].map(_instrument).eq(inst).all():
        raise ResearchDataError(f"Provider returned the wrong instrument for {inst}.")
    frame = frame.sort_values("date").reset_index(drop=True)
    status = frame.get("tradestatus", pd.Series("", index=frame.index)).astype(str)
    if not status.isin(["", "0", "1", "nan", "None"]).all():
        raise ResearchDataError(f"Invalid trading status for {inst}.")
    frame["tradestatus"] = status.replace({"nan": "", "None": ""})
    suspended = frame["tradestatus"].eq("0")
    for field in [*_OHLC, *(["volume", "amount"] if raw else [])]:
        values = pd.to_numeric(frame[field], errors="coerce")
        invalid = values.isna() | ~np.isfinite(values) | (values < 0 if field in {"volume", "amount"} else values <= 0)
        if (invalid & ~suspended).any():
            raise ResearchDataError(f"Invalid {field} on a non-suspended/unknown session for {inst}.")
        # Zero/missing suspended OHLC is unknown, not a tradable zero price.
        frame[field] = values.mask(invalid)
    valid_price = frame[list(_OHLC)].notna().all(axis=1)
    if ((frame["high"] < frame[["open", "close", "low"]].max(axis=1)) & valid_price).any() or (
        (frame["low"] > frame[["open", "close", "high"]].min(axis=1)) & valid_price).any():
        raise ResearchDataError(f"Inconsistent OHLC range for {inst}.")
    if raw:
        volume = frame["volume"].dropna()
        if not np.isclose(volume, np.rint(volume), rtol=0, atol=1e-6).all():
            raise ResearchDataError(f"Raw volume is not an integer share count for {inst}.")
    return frame


def _pair(raw: pd.DataFrame, adjusted: pd.DataFrame, inst: str, start: str, end: str) -> pd.DataFrame:
    raw_tick, adj_tick = raw.attrs.get("price_resolution"), adjusted.attrs.get("price_resolution")
    transform = raw.attrs.get("adjustment_transform")
    if transform != adjusted.attrs.get("adjustment_transform"):
        raise ResearchDataError(f"Raw/adjusted transformation declarations differ for {inst}.")
    raw = _bars(raw, inst, start, end, raw=True)
    adjusted = _bars(adjusted, inst, start, end, raw=False)
    if raw["date"].tolist() != adjusted["date"].tolist():
        raise ResearchDataError(f"Raw/adjusted dates do not align for {inst}; refusing positional pairing.")
    if not raw["tradestatus"].equals(adjusted["tradestatus"]):
        raise ResearchDataError(f"Raw/adjusted trading status differs for {inst}.")
    if not raw[list(_OHLC)].isna().equals(adjusted[list(_OHLC)].isna()):
        raise ResearchDataError(f"Raw/adjusted price availability differs for {inst}.")
    result = pd.DataFrame({"date": raw["date"], "instrument": inst})
    for field in _OHLC:
        result[f"raw_{field}"] = raw[field]
        result[f"adjusted_{field}"] = adjusted[field]
    result["raw_volume"] = raw["volume"]
    result["raw_amount"] = raw["amount"]
    result["adjustment_factor"] = adjusted["close"] / raw["close"]
    if raw_tick is not None or adj_tick is not None:
        if any(tick is None or not np.isfinite(tick) or tick <= 0 for tick in (raw_tick, adj_tick)):
            raise ResearchDataError(f"Raw/adjusted OHLC precision must both be declared for {inst}.")
        # Each displayed OHLC is rounded within half its source-declared tick.
        # One common multiplier must lie in the intersection of all four
        # feasible intervals; this does not invent a percentage tolerance.
        raw_values = raw[list(_OHLC)].to_numpy(dtype=float)
        adj_values = adjusted[list(_OHLC)].to_numpy(dtype=float)
        valid = np.isfinite(raw_values).all(axis=1) & np.isfinite(adj_values).all(axis=1)
        if (raw_values[valid] <= raw_tick / 2).any():
            raise ResearchDataError(f"Raw prices are too small relative to declared precision for {inst}.")
        if transform == "affine_cash_accumulation":
            # Feasibility of one positive a and offset b for all four rounded
            # OHLC values: max_i(y_i-hy-a*(x_i+hx)) <=
            # min_j(y_j+hy-a*(x_j-hx)). All 16 inequalities are linear in a.
            # This verifies source alignment without inferring action events or
            # replacing the provider's prices with a fitted reconstruction.
            x, y = raw_values[valid], adj_values[valid]
            coefficients = x[:, None, :] - x[:, :, None] - raw_tick
            bounds = y[:, None, :] - y[:, :, None] + adj_tick
            ratios = np.divide(bounds, coefficients, out=np.zeros_like(bounds), where=np.abs(coefficients) > 1e-12)
            lower = np.maximum(0, np.where(coefficients < -1e-12, ratios, -np.inf).max(axis=(1, 2)))
            upper = np.where(coefficients > 1e-12, ratios, np.inf).min(axis=(1, 2))
            impossible_flat = ((np.abs(coefficients) <= 1e-12) & (bounds < -1e-12)).any(axis=(1, 2))
            if (impossible_flat | (upper <= 0) | (lower > upper + 1e-12)).any():
                raise ResearchDataError(f"Adjusted OHLC does not share a coherent positive affine transform within source precision for {inst}.")
        else:
            lower = ((adj_values[valid] - adj_tick / 2) / (raw_values[valid] + raw_tick / 2)).max(axis=1)
            upper = ((adj_values[valid] + adj_tick / 2) / (raw_values[valid] - raw_tick / 2)).min(axis=1)
            if (lower > upper + 1e-12).any():
                raise ResearchDataError(f"Adjusted OHLC does not share a coherent per-day factor within declared precision for {inst}.")
    else:
        for field in _OHLC:
            valid = result[f"raw_{field}"].notna() & result["adjustment_factor"].notna()
            if not np.isclose(result.loc[valid, f"raw_{field}"] * result.loc[valid, "adjustment_factor"],
                              result.loc[valid, f"adjusted_{field}"], rtol=1e-4, atol=1e-4).all():
                raise ResearchDataError(f"Adjusted OHLC does not share a coherent per-day factor for {inst}.")
    result["tradestatus"] = raw["tradestatus"]
    is_st = raw.get("isST", pd.Series("", index=raw.index)).fillna("").astype(str)
    if not is_st.isin(["", "0", "1"]).all():
        raise ResearchDataError(f"Invalid ST flag for {inst}.")
    result["is_st"] = is_st
    return result


def _price_quality(frame: pd.DataFrame, inst: str, info: dict) -> dict:
    """Reject conspicuous ETF adjustment failures without inventing action data."""
    close = frame["adjusted_close"]
    daily = close / close.shift(1) - 1
    overnight = frame["adjusted_open"] / close.shift(1) - 1
    intraday = close / frame["adjusted_open"] - 1
    limit = 0.4 if info.get("asset_type") == "ETF" else None
    if limit is not None:
        abnormal = daily.abs().gt(limit) | overnight.abs().gt(limit) | intraday.abs().gt(limit)
        if abnormal.any():
            dates = frame.loc[abnormal, "date"].tolist()[:5]
            raise ResearchDataError(f"Adjusted ETF price discontinuity exceeds 40% for {inst} on {dates}; "
                "source/corporate-action verification is required, prices were not repaired.")
    def extreme(series):
        values = series.dropna()
        return None if values.empty else float(values.abs().max())
    return {"status": "passed_rejection_checks", "etf_absolute_return_limit": limit,
        "max_absolute_adjusted_daily_return": extreme(daily),
        "max_absolute_adjusted_overnight_return": extreme(overnight),
        "max_absolute_adjusted_intraday_return": extreme(intraday),
        "corporate_action_completeness_proven": False}


@dataclass(frozen=True)
class ResearchPriceArtifact:
    path: Path
    manifest: dict[str, Any]
    prices: pd.DataFrame
    calendar_rows: pd.DataFrame
    manifest_sha256: str

    @property
    def calendar(self) -> pd.DatetimeIndex:
        return pd.DatetimeIndex(pd.to_datetime(self.calendar_rows.loc[self.calendar_rows["is_trading_day"].eq(1), "date"]), name="date")

    def wide(self, field: str) -> pd.DataFrame:
        if field not in self.prices or field in {"date", "instrument"}:
            raise ResearchDataError(f"Unknown price field: {field}")
        return self.prices.pivot(index="date", columns="instrument", values=field).reindex(
            index=self.calendar, columns=self.manifest["request"]["instruments"])

    @property
    def adjusted_close(self) -> pd.DataFrame:
        return self.wide("adjusted_close")

    @property
    def adjusted_open(self) -> pd.DataFrame:
        return self.wide("adjusted_open")

    @property
    def raw_prices(self) -> pd.DataFrame:
        columns = ["date", "instrument", *[f"raw_{field}" for field in _OHLC], "raw_volume", "raw_amount", "tradestatus", "is_st"]
        return self.prices[columns].copy()


def load_research_prices(path: str | Path) -> ResearchPriceArtifact:
    """Read an immutable snapshot and verify every content checksum before use."""
    root = Path(path).expanduser().resolve()
    root = root.parent if root.name == "manifest.json" else root
    try:
        manifest_bytes = (root / "manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
        if manifest.get("schema_version") != SCHEMA_VERSION:
            raise ResearchDataError("Unsupported research-price artifact schema.")
        if manifest.get("artifact_id") != _sha(_json_bytes(_manifest_identity(manifest))):
            raise ResearchDataError("Research manifest content address does not match its metadata.")
        if manifest.get("execution_ready") is not False:
            raise ResearchDataError("Research artifact must not claim executable cash-account semantics.")
        for filename in ("prices.csv", "calendar.csv"):
            if _sha((root / filename).read_bytes()) != manifest["files"][filename]["sha256"]:
                raise ResearchDataError(f"Research artifact checksum mismatch: {filename}")
        calendar = _calendar(pd.read_csv(root / "calendar.csv", dtype={"date": str}),
                             manifest["request"]["start_date"], manifest["request"]["end_date"])
        prices = pd.read_csv(root / "prices.csv", keep_default_na=False, float_precision="round_trip",
                             dtype={"date": str, "instrument": str, "tradestatus": str, "is_st": str})
        numeric = [*[f"{prefix}_{field}" for prefix in ("raw", "adjusted") for field in _OHLC],
                   "raw_volume", "raw_amount", "adjustment_factor"]
        for field in numeric:
            prices[field] = pd.to_numeric(prices[field], errors="coerce")
        prices["date"] = pd.to_datetime(prices["date"], format="%Y-%m-%d", errors="raise")
        if prices.duplicated(["date", "instrument"]).any():
            raise ResearchDataError("Research artifact contains duplicate daily prices.")
        return ResearchPriceArtifact(root, manifest, prices, calendar, _sha(manifest_bytes))
    except (OSError, KeyError, TypeError, json.JSONDecodeError, pd.errors.EmptyDataError) as exc:
        raise ResearchDataError(f"Cannot load research-price artifact: {exc}") from exc


def download_research_prices(
    instruments: list[str], start_date: str, end_date: str, cache_dir: str | Path,
    *, adjustment: str = "hfq", refresh: bool = False, provider: ResearchPriceProvider | None = None,
    progress: Callable[[str], None] | None = None,
) -> ResearchPriceArtifact:
    """Download one immutable artifact; a shared HTTP cache excludes concurrent writers."""
    kwargs = {"adjustment": adjustment, "refresh": refresh, "provider": provider, "progress": progress}
    if provider is not None and hasattr(provider, "download_lock"):
        with provider.download_lock(cache_dir):
            return _download_research_prices(instruments, start_date, end_date, cache_dir, **kwargs)
    return _download_research_prices(instruments, start_date, end_date, cache_dir, **kwargs)


def _download_research_prices(
    instruments: list[str], start_date: str, end_date: str, cache_dir: str | Path,
    *, adjustment: str = "hfq", refresh: bool = False, provider: ResearchPriceProvider | None = None,
    progress: Callable[[str], None] | None = None,
) -> ResearchPriceArtifact:
    """Download and cache an immutable same-source raw/adjusted research snapshot.

    Existing snapshots are never overwritten. A request index selects the latest
    validated snapshot for these exact instruments, dates, source and adjustment.
    Missing trading sessions are listed in coverage and remain NaN in wide data.
    """
    start, end = _day(start_date), _day(end_date)
    if start > end or end > datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat():
        raise ResearchDataError("Require start_date <= end_date <= today's Shanghai date.")
    symbols = sorted(set(_instrument(value) for value in instruments))
    if not symbols or len(symbols) > 1000 or adjustment not in {"hfq", "qfq"}:
        raise ResearchDataError("Require 1–1000 instruments and adjustment='hfq' or 'qfq'.")
    owned = provider is None
    provider = BaoStockResearchProvider() if provider is None else provider
    if provider.volume_unit != "shares" or provider.amount_unit != "CNY":
        raise ResearchDataError("Provider must explicitly supply raw volume in shares and raw amount in CNY.")
    source = str(provider.source)
    request = {"instruments": symbols, "start_date": start, "end_date": end, "adjustment": adjustment, "frequency": "1d"}
    provider_meta = {"name": source, "version": str(getattr(provider, "version", "unknown")),
                     "source_url": str(getattr(provider, "source_url", ""))}
    for name in ("volume_provenance", "adjustment_provenance"):
        if hasattr(provider, name):
            provider_meta[name] = getattr(provider, name)
    calendar_meta = {"source": str(getattr(provider, "calendar_source", f"{source}.fetch_calendar")),
                     "source_url": str(getattr(provider, "calendar_source_url", provider_meta["source_url"]))}
    request_key = _sha(_json_bytes({"schema_version": SCHEMA_VERSION, "provider": provider_meta,
                                  "calendar": calendar_meta, "request": request}))
    cache = Path(cache_dir).expanduser().resolve()
    index_path = cache / "requests" / f"{request_key}.json"
    effective_refresh = refresh or bool(getattr(provider, "_refresh_requested", False))
    pending = provider.has_pending_download(cache, request) if not effective_refresh and hasattr(provider, "has_pending_download") else False
    if index_path.exists() and not effective_refresh and not pending:
        try:
            index = json.loads(index_path.read_text())
            identifier = index["artifact_id"]
            if not re.fullmatch(r"[0-9a-f]{64}", identifier):
                raise ResearchDataError("Invalid cached research snapshot identifier.")
            artifact = load_research_prices(cache / "snapshots" / identifier)
            if (artifact.manifest_sha256 != index["manifest_sha256"] or artifact.manifest["request"] != request
                    or artifact.manifest["provider"] != provider_meta
                    or any(artifact.manifest["calendar"].get(key) != value for key, value in calendar_meta.items())):
                raise ResearchDataError("Cached request does not match its immutable manifest.")
            return artifact
        except (OSError, KeyError, json.JSONDecodeError) as exc:
            raise ResearchDataError(f"Invalid research request cache index: {exc}") from exc
    try:
        if hasattr(provider, "begin_download"):
            provider.begin_download(cache, request, refresh=effective_refresh)
        if progress:
            progress("Downloading confirmed exchange trading calendar")
        calendar = _calendar(provider.fetch_calendar(start, end), start, end)
        sessions = calendar.loc[calendar["is_trading_day"].eq(1), "date"].tolist()
        coverage: dict[str, Any] = {}
        frames = []
        infos: dict[str, Any] = {}
        for index, inst in enumerate(symbols, 1):
            if progress:
                progress(f"Downloading raw/adjusted pair {index}/{len(symbols)}: {inst}")
            info = provider.instrument_info(inst) if hasattr(provider, "instrument_info") else {"asset_type": "unknown"}
            infos[inst] = info
            frame = _pair(provider.fetch_raw(inst, start, end), provider.fetch_adjusted(inst, start, end, adjustment), inst, start, end)
            if hasattr(provider, "instrument_info"):
                infos[inst] = provider.instrument_info(inst)
            quality = _price_quality(frame, inst, info)
            dates = frame["date"].tolist()
            if not set(dates).issubset(sessions):
                raise ResearchDataError(f"Bars occur outside confirmed exchange sessions for {inst}.")
            missing = sorted(set(sessions) - set(dates))
            factors = frame["adjustment_factor"].dropna()
            coverage[inst] = {"row_count": len(frame), "first_date": dates[0], "last_date": dates[-1],
                "missing_session_count": len(missing), "missing_sessions": missing,
                "suspended_sessions": frame.loc[frame["tradestatus"].eq("0"), "date"].tolist(),
                "unknown_status_count": int(frame["tradestatus"].eq("").sum()),
                "missing_price_count": int(frame["raw_close"].isna().sum()),
                "factor_change_count": int((factors.pct_change(fill_method=None).abs() > 1e-4).sum()),
                "raw_adjusted_aligned": True, "quality": quality}
            frames.append(frame)
        prices = pd.concat(frames, ignore_index=True).sort_values(["date", "instrument"]).reset_index(drop=True)
        price_bytes = prices.to_csv(index=False, float_format="%.17g", lineterminator="\n", na_rep="").encode()
        calendar_bytes = calendar.to_csv(index=False, lineterminator="\n").encode()
        files = {"prices.csv": {"sha256": _sha(price_bytes), "rows": len(prices)},
                 "calendar.csv": {"sha256": _sha(calendar_bytes), "rows": len(calendar)}}
        identity = {"schema_version": SCHEMA_VERSION, "request": request, "provider": provider_meta, "files": files}
        manifest = {**identity, "created_at": datetime.now(timezone.utc).isoformat(),
            "execution_ready": False, "corporate_actions_complete": False,
            "point_in_time_adjustment_verified": False,
            "price_semantics": {"raw": "unadjusted", "adjusted": adjustment,
                "adjustment_factor": "same-source adjusted_close / raw_close, not a cash/share corporate-action ledger",
                "volume_unit": "shares", "amount_unit": "CNY", "no_forward_fill": True,
                **getattr(provider, "price_semantics", {})},
            "calendar": {**calendar_meta,
                "natural_day_count": len(calendar), "session_count": len(sessions)},
            "instruments": infos, "coverage": coverage, "limitations": [*LIMITATIONS, *getattr(provider, "limitations", [])]}
        if hasattr(provider, "download_provenance"):
            manifest["download_provenance"] = provider.download_provenance()
        identifier = _sha(_json_bytes(_manifest_identity(manifest)))
        manifest["artifact_id"] = identifier
        snapshots = cache / "snapshots"
        snapshots.mkdir(parents=True, exist_ok=True)
        target = snapshots / identifier
        if not target.exists():
            with tempfile.TemporaryDirectory(prefix=".research-", dir=snapshots) as temporary:
                staged = Path(temporary) / "snapshot"
                staged.mkdir()
                (staged / "prices.csv").write_bytes(price_bytes)
                (staged / "calendar.csv").write_bytes(calendar_bytes)
                (staged / "manifest.json").write_bytes(_json_bytes(manifest))
                try:
                    staged.rename(target)
                except OSError:
                    if not target.exists():
                        raise
        artifact = load_research_prices(target)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix=".request-", dir=index_path.parent, delete=False) as stream:
            temporary_index = Path(stream.name)
            stream.write(_json_bytes({"artifact_id": identifier, "manifest_sha256": artifact.manifest_sha256}))
        temporary_index.replace(index_path)
        if hasattr(provider, "complete_download"):
            provider.complete_download(identifier)
        return artifact
    finally:
        if owned:
            provider.close()
