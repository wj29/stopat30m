"""Real-time and near-real-time stock price/name service for A-shares.

Source priority and registration live in ``stopat30m.data.sources``
(single authority).  This module provides spot-price and stock-name
queries with automatic failover and retry.  After all online sources,
the local Qlib last-close is used as a non-real-time fallback.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from threading import Lock

import pandas as pd
from loguru import logger

from stopat30m.data.normalize import bare_code, normalize_instrument

_cache_lock = Lock()
_price_cache: dict[str, float] = {}
_name_cache: dict[str, str] = {}
_cache_ts: float = 0.0
_CACHE_TTL_SECONDS = 30.0

_MAX_RETRIES = 2
_RETRY_BACKOFF = 0.5

from stopat30m.data.sources import get_source_label, resolve_source_priority

_SPOT_DISPATCH: dict[str, object] = {}


def _build_spot_dispatch() -> None:
    """Lazy init of key → fetch_fn mapping for spot-price queries."""
    if _SPOT_DISPATCH:
        return
    _SPOT_DISPATCH.update({
        "tencent": _fetch_tencent,
        "sina": _fetch_sina,
        "efinance": _fetch_efinance_batch,
        "pytdx": _fetch_pytdx,
        "akshare": None,  # needs closure, built per-call
        "tushare": _fetch_tushare,
    })


def _build_source_chain(
    needed: dict[str, str],
) -> list[tuple[str, object]]:
    """Build the ordered (name, fetch_fn) chain from centralized config."""
    _build_spot_dispatch()
    chain: list[tuple[str, object]] = []
    for key in resolve_source_priority():
        label = get_source_label(key)
        if key == "akshare":
            chain.append((label, lambda codes, _n=needed: _fetch_akshare_batch(codes, _n)))
        elif key in _SPOT_DISPATCH and _SPOT_DISPATCH[key] is not None:
            chain.append((label, _SPOT_DISPATCH[key]))
        else:
            logger.debug(f"Skipping source '{key}' for spot queries (no spot handler)")
    return chain


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_spot_prices(instruments: list[str], ttl: float | None = None) -> dict[str, float]:
    """Fetch latest spot prices with automatic source failover and retry.

    Source order is read from ``config.yaml`` ``realtime_source_priority``.
    Returns {normalized_instrument: price}.
    """
    global _price_cache, _name_cache, _cache_ts

    if not instruments:
        return {}

    cache_ttl = ttl if ttl is not None else _CACHE_TTL_SECONDS
    with _cache_lock:
        if _price_cache and (time.time() - _cache_ts) < cache_ttl:
            hit = {normalize_instrument(i): _price_cache[normalize_instrument(i)]
                   for i in instruments if normalize_instrument(i) in _price_cache}
            if len(hit) == len(instruments):
                return hit

    needed = {bare_code(inst): normalize_instrument(inst) for inst in instruments}
    prices: dict[str, float] = {}
    names: dict[str, str] = {}

    sources = _build_source_chain(needed)

    missing_codes = dict(needed)
    for source_name, fetch_fn in sources:
        if not missing_codes:
            break
        result = _call_with_retry(source_name, fetch_fn, list(missing_codes.keys()))
        if result:
            for code, (price, name) in result.items():
                inst = missing_codes.get(code)
                if inst and price > 0:
                    prices[inst] = price
                    if name:
                        names[inst] = name
            prev_missing = len(missing_codes)
            missing_codes = {c: n for c, n in missing_codes.items() if n not in prices}
            filled = prev_missing - len(missing_codes)
            if filled > 0:
                logger.info(f"{source_name}: got {filled} prices")

    if not prices:
        prices = fetch_prices_from_qlib(list(needed.values()))
        if prices:
            logger.info(f"Qlib fallback: got {len(prices)} prices")
        else:
            logger.error("All real-time price sources failed")
    elif len(prices) < len(needed):
        logger.warning(f"Got prices for {len(prices)}/{len(needed)} instruments")

    with _cache_lock:
        _price_cache.update(prices)
        _name_cache.update(names)
        _cache_ts = time.time()

    return prices


def fetch_stock_names(instruments: list[str]) -> dict[str, str]:
    """Fetch stock names. Uses in-memory cache, then disk cache, then APIs."""
    if not instruments:
        return {}

    with _cache_lock:
        if _name_cache:
            hit = {normalize_instrument(i): _name_cache[normalize_instrument(i)]
                   for i in instruments if normalize_instrument(i) in _name_cache}
            if len(hit) == len(instruments):
                return hit

    needed = {bare_code(inst): normalize_instrument(inst) for inst in instruments}
    disk_cache = _load_disk_name_cache()

    result: dict[str, str] = {}
    api_needed: dict[str, str] = {}
    for bare, inst in needed.items():
        if bare in disk_cache:
            result[inst] = disk_cache[bare]
        else:
            api_needed[bare] = inst

    if not api_needed:
        return result

    api_names: dict[str, str] = {}

    name_sources = _build_source_chain(api_needed)

    for src_name, fetch_fn in name_sources:
        if len(api_names) >= len(api_needed):
            break
        still_missing = {b: i for b, i in api_needed.items() if i not in api_names}
        if not still_missing:
            break
        fetch_result = _call_with_retry(src_name, fetch_fn, list(still_missing.keys()))
        if fetch_result:
            for code, (_, name) in fetch_result.items():
                if code in still_missing and name:
                    api_names[still_missing[code]] = name

    if api_names:
        result.update(api_names)
        for inst, name in api_names.items():
            disk_cache[bare_code(inst)] = name
        _save_disk_name_cache(disk_cache)

    with _cache_lock:
        _name_cache.update(result)

    return result


def invalidate_cache() -> None:
    """Clear the in-memory price/name cache."""
    global _price_cache, _name_cache, _cache_ts
    with _cache_lock:
        _price_cache.clear()
        _name_cache.clear()
        _cache_ts = 0.0


def fetch_prices_from_qlib(instruments: list[str]) -> dict[str, float]:
    """Fallback: fetch latest close prices from local Qlib data (not real-time)."""
    try:
        from stopat30m.data.provider import init_qlib
        import qlib.data

        init_qlib()
        norm = [normalize_instrument(c) for c in instruments]
        df = qlib.data.D.features(
            instruments=norm, fields=["$close"],
            start_time="2020-01-01", end_time="2099-12-31",
        )
        if df is None or df.empty:
            return {}
        latest = df.groupby(level="instrument").tail(1)
        prices: dict[str, float] = {}
        for (_, inst), row in latest.iterrows():
            p = float(row["$close"])
            if p > 0:
                prices[inst] = p
        return prices
    except Exception as e:
        logger.warning(f"Qlib price fallback failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------


def _call_with_retry(
    source_name: str,
    fetch_fn,
    codes: list[str],
    max_retries: int = _MAX_RETRIES,
) -> dict[str, tuple[float, str]] | None:
    """Call a fetch function with exponential backoff retry."""
    for attempt in range(max_retries + 1):
        try:
            result = fetch_fn(codes)
            if result:
                return result
        except Exception as e:
            if attempt < max_retries:
                wait = _RETRY_BACKOFF * (2 ** attempt)
                logger.debug(f"{source_name} attempt {attempt + 1} failed: {e}, retrying in {wait:.1f}s")
                time.sleep(wait)
            else:
                logger.warning(f"{source_name} failed after {max_retries + 1} attempts: {e}")
    return None


# ---------------------------------------------------------------------------
# Source: Efinance (batch, east-money backend)
# ---------------------------------------------------------------------------


def _fetch_efinance_batch(codes: list[str]) -> dict[str, tuple[float, str]]:
    """Fetch full-market spot data via efinance, filter to requested codes."""
    try:
        import efinance as ef
    except ImportError:
        return {}

    df = ef.stock.get_realtime_quotes()
    if df is None or df.empty:
        return {}

    code_col = _find_col(df, ["股票代码", "代码", "code"])
    price_col = _find_col(df, ["最新价", "现价", "close"])
    name_col = _find_col(df, ["股票名称", "名称", "name"])

    if code_col is None or price_col is None:
        return {}

    code_set = set(codes)
    result: dict[str, tuple[float, str]] = {}
    for _, row in df.iterrows():
        c = str(row[code_col]).strip().zfill(6)
        if c not in code_set:
            continue
        try:
            p = float(row[price_col])
        except (ValueError, TypeError):
            continue
        name = str(row[name_col]).strip() if name_col else ""
        if p > 0:
            result[c] = (p, name)

    return result


# ---------------------------------------------------------------------------
# Source: pytdx (TDX protocol direct connect)
# ---------------------------------------------------------------------------

_TDX_HOSTS = [
    ("119.147.212.81", 7709),
    ("14.17.75.71", 7709),
    ("218.75.126.9", 7709),
    ("115.238.90.165", 7709),
    ("124.160.88.183", 7709),
]


def _fetch_pytdx(codes: list[str]) -> dict[str, tuple[float, str]]:
    """Fetch real-time quotes via pytdx TDX protocol."""
    try:
        from pytdx.hq import TdxHq_API
    except ImportError:
        return {}

    api = TdxHq_API()
    connected = False
    for host, port in _TDX_HOSTS:
        try:
            api.connect(host, port, time_out=5)
            connected = True
            break
        except Exception:
            continue

    if not connected:
        return {}

    try:
        query = []
        for c in codes:
            market = 1 if c.startswith(("6", "9")) else 0
            query.append((market, c))

        batch_size = 80
        result: dict[str, tuple[float, str]] = {}

        for i in range(0, len(query), batch_size):
            batch = query[i:i + batch_size]
            data = api.get_security_quotes(batch)
            if not data:
                continue
            for item in data:
                c = item.get("code", "")
                price = item.get("price", 0)
                name = item.get("name", "")
                if c in codes and price and float(price) > 0:
                    result[c] = (float(price), name)

        return result
    except Exception:
        return {}
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Source: Sina HTTP
# ---------------------------------------------------------------------------


def _to_sina_symbol(bare: str) -> str:
    if bare.startswith(("6", "9")):
        return f"sh{bare}"
    return f"sz{bare}"


def _fetch_sina(codes: list[str]) -> dict[str, tuple[float, str]]:
    """Fetch real-time prices from Sina Finance HTTP API."""
    import requests

    symbols = [_to_sina_symbol(c) for c in codes]
    url = f"https://hq.sinajs.cn/list={','.join(symbols)}"
    headers = {"Referer": "https://finance.sina.com.cn"}

    resp = requests.get(url, headers=headers, timeout=10)
    resp.encoding = "gbk"

    result: dict[str, tuple[float, str]] = {}
    for line in resp.text.strip().split("\n"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        var_part, _, data_part = line.partition("=")
        symbol = var_part.split("_")[-1]
        bare = symbol[2:]
        data_part = data_part.strip('" ;')
        if not data_part:
            continue

        fields = data_part.split(",")
        if len(fields) < 4:
            continue

        name = fields[0]
        try:
            price = float(fields[3])
        except (ValueError, IndexError):
            continue

        if bare in codes and price > 0:
            result[bare] = (price, name)

    return result


# ---------------------------------------------------------------------------
# Source: Tencent HTTP
# ---------------------------------------------------------------------------


def _fetch_tencent(codes: list[str]) -> dict[str, tuple[float, str]]:
    """Fetch real-time prices from Tencent Finance HTTP API."""
    import requests

    symbols = [_to_sina_symbol(c) for c in codes]
    url = f"https://qt.gtimg.cn/q={','.join(symbols)}"

    resp = requests.get(url, timeout=10)
    resp.encoding = "gbk"

    result: dict[str, tuple[float, str]] = {}
    for line in resp.text.strip().split("\n"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        var_part, _, data_part = line.partition("=")
        data_part = data_part.strip('" ;')
        if not data_part:
            continue

        fields = data_part.split("~")
        if len(fields) < 5:
            continue

        name = fields[1]
        bare = fields[2]
        try:
            price = float(fields[3])
        except (ValueError, IndexError):
            continue

        if bare in codes and price > 0:
            result[bare] = (price, name)

    return result


# ---------------------------------------------------------------------------
# Source: AKShare batch
# ---------------------------------------------------------------------------


def _fetch_akshare_batch(
    codes: list[str], needed: dict[str, str],
) -> dict[str, tuple[float, str]]:
    df = _fetch_spot_dataframe()
    if df is None or df.empty:
        return {}
    prices, names = _extract_from_dataframe(df, needed)
    result: dict[str, tuple[float, str]] = {}
    for code, inst in needed.items():
        if inst in prices:
            result[code] = (prices[inst], names.get(inst, ""))
    return result


_last_fetch_error: str = ""


def get_last_fetch_error() -> str:
    return _last_fetch_error


def _fetch_spot_dataframe() -> pd.DataFrame | None:
    global _last_fetch_error

    try:
        import akshare as ak
    except ImportError:
        _last_fetch_error = "akshare not installed"
        return None

    endpoints = [
        ("stock_zh_a_spot_em", lambda: ak.stock_zh_a_spot_em()),
        ("stock_zh_a_spot", lambda: ak.stock_zh_a_spot()),
    ]

    for name, fetch_fn in endpoints:
        try:
            df = fetch_fn()
            if df is not None and not df.empty:
                _last_fetch_error = ""
                return df
        except Exception as e:
            _last_fetch_error = f"{name}: {e}"
            logger.debug(f"AKShare {name} failed: {e}")

    return None


def _extract_from_dataframe(
    df: pd.DataFrame, needed: dict[str, str],
) -> tuple[dict[str, float], dict[str, str]]:
    code_col = _find_col(df, ["代码", "code"])
    price_col = _find_col(df, ["最新价", "close", "当前价"])
    name_col = _find_col(df, ["名称", "name"])

    if code_col is None or price_col is None:
        return {}, {}

    prices: dict[str, float] = {}
    names: dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row[code_col]).strip()
        if code in needed:
            try:
                p = float(row[price_col])
                if p > 0:
                    prices[needed[code]] = p
            except (ValueError, TypeError):
                pass
            if name_col is not None:
                names[needed[code]] = str(row[name_col]).strip()

    return prices, names


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ---------------------------------------------------------------------------
# Source: Tushare (requires Pro token with sufficient credits)
# ---------------------------------------------------------------------------


def _fetch_tushare(codes: list[str]) -> dict[str, tuple[float, str]]:
    """Fetch real-time quotes via Tushare Pro daily API (uses latest trade date)."""
    try:
        import tushare as ts
    except ImportError:
        return {}

    from stopat30m.config import get
    import os

    token = (get("tushare", "token", "") or os.getenv("TUSHARE_TOKEN", "")).strip()
    if not token:
        return {}

    ts.set_token(token)
    pro = ts.pro_api()

    ts_codes = []
    bare_to_ts: dict[str, str] = {}
    for c in codes:
        suffix = "SH" if c.startswith(("6", "9")) else "SZ"
        ts_code = f"{c}.{suffix}"
        ts_codes.append(ts_code)
        bare_to_ts[c] = ts_code

    result: dict[str, tuple[float, str]] = {}
    try:
        df = pro.daily(ts_code=",".join(ts_codes), limit=len(ts_codes))
        if df is None or df.empty:
            return {}

        df = df.sort_values("trade_date", ascending=False).drop_duplicates("ts_code", keep="first")
        for _, row in df.iterrows():
            tc = row["ts_code"]
            bare = tc.split(".")[0]
            price = float(row.get("close", 0))
            if bare in codes and price > 0:
                result[bare] = (price, "")
    except Exception as e:
        logger.debug(f"Tushare realtime failed: {e}")

    return result


# ---------------------------------------------------------------------------
# Disk name cache
# ---------------------------------------------------------------------------


def _name_cache_path() -> Path:
    from stopat30m.data.provider import get_data_dir
    return get_data_dir() / "stock_names.json"


def _load_disk_name_cache() -> dict[str, str]:
    p = _name_cache_path()
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {}


def _save_disk_name_cache(cache: dict[str, str]) -> None:
    p = _name_cache_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
        tmp.replace(p)
    except Exception as e:
        logger.debug(f"Failed to save name cache: {e}")
