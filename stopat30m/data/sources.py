"""Centralized data-source registry and failover logic.

All source resolution is driven by ``config.yaml`` key
``realtime_source_priority`` (comma-separated, tried left-to-right).

This module is the **single authority** for:
  1. Parsing the priority config
  2. Mapping source keys → concrete fetcher classes
  3. Building an ordered chain for spot-price queries
  4. Building an ordered chain for daily OHLCV queries
  5. Executing OHLCV failover with Qlib as final fallback

Upper-layer code (analysis pipeline, realtime service, etc.) should call the
public APIs here instead of maintaining their own source maps.
"""

from __future__ import annotations

import importlib
import os
import time
import numpy as np
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from loguru import logger

from stopat30m.data.normalize import bare_code, normalize_instrument

# ---------------------------------------------------------------------------
# Source registry  (key → module_path, class_name, default kwargs)
#
# ``capabilities`` marks what each source supports:
#   "spot"  — real-time / near-real-time price queries
#   "daily" — historical daily OHLCV (fetch_daily)
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, dict[str, Any]] = {
    "tencent": {
        "module": "stopat30m.data.fetcher.tencent",
        "class": "TencentFetcher",
        "kwargs": {"delay": 0.1},
        "capabilities": {"spot", "daily"},
        "label": "Tencent",
        "color": "blue",
        "download_weight": 1,
    },
    "sina": {
        "module": "stopat30m.data.fetcher.sina",
        "class": "SinaFetcher",
        "kwargs": {"delay": 0.1},
        "capabilities": {"spot", "daily"},
        "label": "Sina",
        "color": "red",
        "download_weight": 1,
    },
    "efinance": {
        "module": "stopat30m.data.fetcher.efinance",
        "class": "EfinanceFetcher",
        "kwargs": {"delay": 0.05},
        "capabilities": {"spot", "daily"},
        "label": "Efinance",
        "color": "green",
        "download_weight": 3,
    },
    "pytdx": {
        "module": None,
        "class": None,
        "kwargs": {},
        "capabilities": {"spot"},
        "label": "pytdx",
        "color": "white",
        "download_weight": 0,
    },
    "akshare": {
        "module": "stopat30m.data.fetcher.akshare",
        "class": "AKShareFetcher",
        "kwargs": {"delay": 0.1},
        "capabilities": {"spot", "daily"},
        "label": "AKShare",
        "color": "yellow",
        "download_weight": 2,
    },
    "baostock": {
        "module": "stopat30m.data.fetcher.baostock",
        "class": "BaoStockFetcher",
        "kwargs": {},
        "capabilities": {"daily"},
        "label": "BaoStock",
        "color": "cyan",
        "download_weight": 5,
    },
    "tushare": {
        "module": "stopat30m.data.fetcher.tushare",
        "class": "TushareFetcher",
        "kwargs": {},
        "capabilities": {"spot", "daily"},
        "label": "Tushare",
        "color": "magenta",
        "download_weight": 1,
    },
}

_DEFAULT_PRIORITY = "tencent,sina,efinance,pytdx,akshare"


# ---------------------------------------------------------------------------
# Priority resolution (reads config once, no caching to respect hot-reload)
# ---------------------------------------------------------------------------


def resolve_source_priority() -> list[str]:
    """Return ordered list of source keys from config, with auto tushare injection."""
    from stopat30m.config import get

    raw = get("realtime_source_priority", default="")
    if not raw or not raw.strip():
        raw = _DEFAULT_PRIORITY

    sources = [s.strip().lower() for s in raw.split(",") if s.strip()]

    tushare_token = (get("tushare", "token", "") or os.getenv("TUSHARE_TOKEN", "")).strip()
    if tushare_token and "tushare" not in sources:
        sources.insert(0, "tushare")
        logger.debug("Tushare token detected; auto-prepended 'tushare' to priority")

    return sources


def resolve_daily_sources() -> list[str]:
    """Return ordered source keys for OHLCV daily queries.

    Filters ``resolve_source_priority()`` to sources that have ``"daily"``
    capability, then appends ``baostock`` as a non-eastmoney safety net.
    """
    sources = [s for s in resolve_source_priority() if _has_capability(s, "daily")]
    if "baostock" not in sources:
        sources.append("baostock")
    return sources


def get_source_label(key: str) -> str:
    entry = _REGISTRY.get(key)
    return entry["label"] if entry else key


def get_source_color(key: str) -> str:
    entry = _REGISTRY.get(key)
    return entry["color"] if entry else "white"


def get_download_weight(key: str) -> int:
    entry = _REGISTRY.get(key)
    return entry["download_weight"] if entry else 0


# ---------------------------------------------------------------------------
# Fetcher instantiation (lazy import, import-error tolerant)
# ---------------------------------------------------------------------------


def _has_capability(key: str, cap: str) -> bool:
    entry = _REGISTRY.get(key)
    return entry is not None and cap in entry["capabilities"]


def create_fetcher(key: str, **override_kwargs):
    """Instantiate a registered fetcher by key.

    Subprocess-safe (only uses importlib).  Raises ``ValueError`` for
    unknown keys, ``ImportError`` if the backing package is missing.
    ``override_kwargs`` are merged on top of the registry defaults.
    """
    entry = _REGISTRY.get(key)
    if not entry or not entry["module"]:
        raise ValueError(f"No fetcher module registered for source '{key}'")

    # Auto-inject config-driven kwargs for sources that need API keys
    if key == "tushare" and "token" not in override_kwargs:
        from stopat30m.config import get
        token = (get("tushare", "token", "") or os.getenv("TUSHARE_TOKEN", "")).strip()
        if not token:
            raise ValueError("Tushare token not configured (config.yaml tushare.token)")
        override_kwargs.setdefault("token", token)
        override_kwargs.setdefault("delay", 1.3)

    mod = importlib.import_module(entry["module"])
    cls = getattr(mod, entry["class"])
    kwargs = {**entry["kwargs"], **override_kwargs}
    return cls(**kwargs)


def _try_load_fetcher(key: str):
    """Like ``create_fetcher`` but returns None on any error (for failover)."""
    try:
        return create_fetcher(key)
    except Exception as e:
        logger.debug(f"Cannot load fetcher '{key}': {e}")
        return None


# ---------------------------------------------------------------------------
# Public: fetch daily OHLCV with full failover
# ---------------------------------------------------------------------------


def fetch_daily_ohlcv(
    code: str,
    days: int = 120,
) -> tuple[pd.DataFrame | None, str]:
    """Fetch recent daily OHLCV for a single stock with automatic failover.

    Tries each source from ``resolve_daily_sources()`` in order, then
    local Qlib as final fallback.

    Returns ``(dataframe, source_key)``; ``source_key`` is ``""`` when
    all sources fail.
    """
    norm = normalize_instrument(code)
    bare = bare_code(norm)

    if days < 1:
        raise ValueError("days must be positive")
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    end = now.strftime("%Y-%m-%d")
    start = (now - pd.Timedelta(days=days * 2)).strftime("%Y-%m-%d")
    # Today is only the upper bound for valid bar dates. Without a complete
    # exchange calendar, do not invent a "last session" from business days.
    candidates: list[tuple[pd.DataFrame, str]] = []

    for src_key in resolve_daily_sources():
        fetcher = _try_load_fetcher(src_key)
        if fetcher is None:
            continue
        try:
            df = fetcher.fetch_daily(bare, start, end)
            df = _validate_daily_frame(df, end)
            if df is not None:
                logger.debug(f"OHLCV from {src_key} for {code}: {len(df)} rows")
                candidates.append((df, src_key))
                if str(df["date"].iloc[-1]) == end:
                    return _daily_result(df, src_key, days, now), src_key
        except Exception as e:
            logger.debug(f"{src_key} OHLCV failed for {code}: {e}")

    local = _validate_daily_frame(_fetch_ohlcv_from_qlib(norm, days), end)
    if local is not None:
        candidates.append((local, "qlib"))
    if not candidates:
        return None, ""
    best, source = max(candidates, key=lambda item: item[0]["date"].iloc[-1])
    return _daily_result(best, source, days, now), source


def _validate_daily_frame(df: pd.DataFrame | None, end: str) -> pd.DataFrame | None:
    """Normalize ordering and reject malformed bars instead of accepting nonempty data."""
    required = ["date", "open", "high", "low", "close", "volume"]
    if df is None or df.empty or any(col not in df.columns for col in required):
        return None
    frame = df.copy()
    dates = pd.to_datetime(frame["date"], errors="coerce", utc=True)
    frame["date"] = dates.dt.tz_convert("Asia/Shanghai").dt.strftime("%Y-%m-%d")
    for col in required[1:]:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    numbers = frame[required[1:]]
    valid = numbers.notna().all(axis=1) & np.isfinite(numbers).all(axis=1)
    valid &= (frame[["open", "close", "high", "low"]] > 0).all(axis=1)
    valid &= frame["volume"] >= 0
    valid &= frame["high"] >= frame[["open", "close", "low"]].max(axis=1)
    valid &= frame["low"] <= frame[["open", "close", "high"]].min(axis=1)
    valid &= frame["date"].notna() & (frame["date"] <= end)
    frame = frame.loc[valid].drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
    return frame if not frame.empty else None


def _daily_result(df: pd.DataFrame, source: str, days: int, now: datetime) -> pd.DataFrame:
    result = df.tail(days).copy()
    as_of = str(result["date"].iloc[-1])
    age_days = (now.date() - pd.Timestamp(as_of).date()).days
    result.attrs.update({"source": source, "as_of": as_of, "fetched_at": now.isoformat(),
                         "age_calendar_days": age_days, "is_realtime": False,
                         "freshness": "stale" if age_days > 7 else "recent",
                         "latest_session_verified": False,
                         "warnings": ["未核验交易所完整交易日历；数据日期不等于获取时间。"]})
    return result


def _fetch_ohlcv_from_qlib(
    norm_instrument: str,
    days: int,
) -> pd.DataFrame | None:
    """Last-resort fallback: read OHLCV from local Qlib binary data."""
    try:
        from stopat30m.data.provider import init_qlib
        import qlib.data

        init_qlib()
        start_ts = pd.Timestamp.now() - pd.Timedelta(days=days * 2)
        df = qlib.data.D.features(
            instruments=[norm_instrument],
            fields=["$open", "$close", "$high", "$low", "$volume"],
            start_time=start_ts,
        )
        if df is not None and not df.empty:
            df = df.droplevel("instrument").reset_index()
            df.columns = ["date", "open", "close", "high", "low", "volume"]
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
            logger.debug(f"OHLCV from Qlib for {norm_instrument}: {len(df)} rows")
            return df.tail(days)
    except Exception as e:
        logger.debug(f"Qlib OHLCV failed for {norm_instrument}: {e}")

    return None
