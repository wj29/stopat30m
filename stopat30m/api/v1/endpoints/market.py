"""Market overview API: sector heatmap, main indices.

Source priority: East Money (via AKShare, richer data) → Sina Finance (fallback).
Both are cached in-memory (5 min TTL).
East Money calls use a short timeout so that machines where push2 is
IP-blocked fall back to Sina within seconds.
"""

from __future__ import annotations

import re
import time as _time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeout
from threading import Lock
from typing import Any

import requests as _requests
from fastapi import APIRouter, Depends
from loguru import logger

from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import User

router = APIRouter(prefix="/market")

# ---------------------------------------------------------------------------
# Cache config
# ---------------------------------------------------------------------------
_CACHE_TTL = 300  # 5 minutes

_cache_lock = Lock()
_sector_cache: list[dict[str, Any]] = []
_sector_ts: float = 0
_index_cache: list[dict[str, Any]] = []
_index_ts: float = 0

_EM_TIMEOUT = 5  # seconds — EM normally responds in 1–3s; timeout triggers Sina fallback
_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="market")

_SINA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}


# ===================================================================
# East Money via AKShare (primary — richer data)
# ===================================================================

def _call_with_timeout(fn, timeout: float = _EM_TIMEOUT):
    fut: Future = _pool.submit(fn)
    return fut.result(timeout=timeout)


def _fetch_sectors_em() -> list[dict[str, Any]]:
    import akshare as ak
    import pandas as pd

    df = _call_with_timeout(ak.stock_board_industry_name_em)
    if df is None or df.empty:
        return []
    for col in ("涨跌幅", "总市值", "换手率", "上涨家数", "下跌家数", "领涨股票-涨跌幅"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    results: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        results.append({
            "name": str(row.get("板块名称", "")),
            "code": str(row.get("板块代码", "")),
            "change_pct": float(row.get("涨跌幅", 0) or 0),
            "market_cap": float(row.get("总市值", 0) or 0),
            "turnover_rate": float(row.get("换手率", 0) or 0),
            "rising": int(row.get("上涨家数", 0) or 0),
            "falling": int(row.get("下跌家数", 0) or 0),
            "top_stock": str(row.get("领涨股票", "") or ""),
            "top_stock_pct": float(row.get("领涨股票-涨跌幅", 0) or 0),
        })
    return results


def _fetch_indices_em() -> list[dict[str, Any]]:
    import akshare as ak
    import pandas as pd

    targets = {
        "sh000001": "上证指数", "sz399001": "深证成指", "sz399006": "创业板指",
        "sh000688": "科创50", "sh000300": "沪深300", "sh000016": "上证50",
    }
    df = _call_with_timeout(ak.stock_zh_index_spot_em)
    if df is None or df.empty:
        return []
    results: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        code = str(row.get("代码", ""))
        if code not in targets:
            continue
        change_pct = pd.to_numeric(row.get("涨跌幅", 0), errors="coerce") or 0.0
        results.append({
            "code": code, "name": targets[code],
            "current": float(row.get("最新价", 0) or 0),
            "change_pct": float(change_pct),
            "change_amount": float(row.get("涨跌额", 0) or 0),
            "volume": float(row.get("成交量", 0) or 0),
            "amount": float(row.get("成交额", 0) or 0),
        })
    order = list(targets.keys())
    results.sort(key=lambda x: order.index(x["code"]) if x["code"] in order else 999)
    return results


# ===================================================================
# Sina Finance (fallback — always available)
# ===================================================================

def _fetch_indices_sina() -> list[dict[str, Any]]:
    targets = {
        "s_sh000001": ("sh000001", "上证指数"),
        "s_sz399001": ("sz399001", "深证成指"),
        "s_sz399006": ("sz399006", "创业板指"),
        "s_sh000688": ("sh000688", "科创50"),
        "s_sh000300": ("sh000300", "沪深300"),
        "s_sh000016": ("sh000016", "上证50"),
    }
    symbols = ",".join(targets.keys())
    try:
        resp = _requests.get(
            f"https://hq.sinajs.cn/list={symbols}",
            headers=_SINA_HEADERS, timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"[Market] Sina 指数获取失败: {e}")
        return []

    results: list[dict[str, Any]] = []
    for line in resp.text.strip().splitlines():
        m = re.match(r'var hq_str_(s_\w+)="(.+)";', line)
        if not m:
            continue
        key = m.group(1)
        if key not in targets:
            continue
        code, display_name = targets[key]
        parts = m.group(2).split(",")
        if len(parts) < 6:
            continue
        # format: name, current, change_amount, change_pct(%), volume(手), amount(万元)
        try:
            results.append({
                "code": code,
                "name": display_name,
                "current": float(parts[1]),
                "change_pct": float(parts[3]),
                "change_amount": float(parts[2]),
                "volume": float(parts[4]),
                "amount": float(parts[5]) * 10000,
            })
        except (ValueError, IndexError, ZeroDivisionError):
            continue

    order = [v[0] for v in targets.values()]
    results.sort(key=lambda x: order.index(x["code"]) if x["code"] in order else 999)
    return results


def _fetch_sectors_sina() -> list[dict[str, Any]]:
    try:
        resp = _requests.get(
            "https://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php",
            headers=_SINA_HEADERS, timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"[Market] Sina 板块获取失败: {e}")
        return []

    m = re.search(r"=\s*(\{.+\})", resp.text, re.S)
    if not m:
        logger.warning("[Market] Sina 板块数据格式异常")
        return []

    raw = m.group(1)
    results: list[dict[str, Any]] = []
    for entry_m in re.finditer(r'"(\w+)":"([^"]+)"', raw):
        parts = entry_m.group(2).split(",")
        if len(parts) < 13:
            continue
        try:
            stock_count = int(parts[2])
            change_pct = float(parts[5])
            results.append({
                "name": parts[1],
                "code": parts[0],
                "change_pct": change_pct,
                "market_cap": 0,
                "turnover_rate": 0,
                "rising": stock_count if change_pct > 0 else 0,
                "falling": stock_count if change_pct < 0 else 0,
                "top_stock": parts[12] if len(parts) > 12 else "",
                "top_stock_pct": float(parts[9]) if parts[9] else 0,
            })
        except (ValueError, IndexError):
            continue
    return results


# ===================================================================
# Fetcher chains: East Money first → Sina fallback
# ===================================================================

def _fetch_sectors() -> tuple[list[dict[str, Any]], str]:
    try:
        data = _fetch_sectors_em()
        if data:
            logger.debug(f"[Market] 板块数据来自东财 ({len(data)} 个)")
            return data, "东方财富"
    except FuturesTimeout:
        logger.warning("[Market] 东财板块接口超时，切换 Sina")
    except Exception as e:
        logger.warning(f"[Market] 东财板块接口失败: {e}，切换 Sina")
    data = _fetch_sectors_sina()
    if data:
        logger.debug(f"[Market] 板块数据来自 Sina ({len(data)} 个)")
    return data, "新浪财经"


def _fetch_indices() -> tuple[list[dict[str, Any]], str]:
    try:
        data = _fetch_indices_em()
        if data:
            logger.debug(f"[Market] 指数数据来自东财 ({len(data)} 个)")
            return data, "东方财富"
    except FuturesTimeout:
        logger.warning("[Market] 东财指数接口超时，切换 Sina")
    except Exception as e:
        logger.warning(f"[Market] 东财指数接口失败: {e}，切换 Sina")
    data = _fetch_indices_sina()
    if data:
        logger.debug(f"[Market] 指数数据来自 Sina ({len(data)} 个)")
    return data, "新浪财经"


# ===================================================================
# Cached accessors
# ===================================================================

_sector_source: str = ""
_index_source: str = ""


def _get_sectors() -> tuple[list[dict[str, Any]], str]:
    global _sector_cache, _sector_ts, _sector_source
    now = _time.time()
    with _cache_lock:
        if now - _sector_ts < _CACHE_TTL and _sector_cache:
            return _sector_cache, _sector_source
    fresh, source = _fetch_sectors()
    if fresh:
        with _cache_lock:
            _sector_cache = fresh
            _sector_source = source
            _sector_ts = _time.time()
        return fresh, source
    with _cache_lock:
        return _sector_cache, _sector_source


def _get_indices() -> tuple[list[dict[str, Any]], str]:
    global _index_cache, _index_ts, _index_source
    now = _time.time()
    with _cache_lock:
        if now - _index_ts < _CACHE_TTL and _index_cache:
            return _index_cache, _index_source
    fresh, source = _fetch_indices()
    if fresh:
        with _cache_lock:
            _index_cache = fresh
            _index_source = source
            _index_ts = _time.time()
        return fresh, source
    with _cache_lock:
        return _index_cache, _index_source


# ===================================================================
# Endpoints
# ===================================================================

@router.get("/heatmap")
def get_sector_heatmap(user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Return all industry sectors for treemap heatmap display."""
    sectors, source = _get_sectors()
    return {"sectors": sectors, "count": len(sectors), "source": source}


@router.get("/indices")
def get_main_indices(user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Return main index realtime quotes."""
    indices, source = _get_indices()
    return {"indices": indices, "source": source}
