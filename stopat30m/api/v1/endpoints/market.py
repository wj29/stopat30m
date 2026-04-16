"""Market overview API: sector heatmap, main indices."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from loguru import logger

from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import User

router = APIRouter(prefix="/market")


def _fetch_sector_heatmap() -> list[dict[str, Any]]:
    """Fetch all industry sectors from East Money via AKShare."""
    import time
    import akshare as ak

    df = None
    for attempt in range(3):
        try:
            df = ak.stock_board_industry_name_em()
            break
        except Exception as e:
            logger.warning(f"[Market] 东财板块数据获取失败 (第{attempt + 1}次): {e}")
            if attempt < 2:
                time.sleep(1)
    if df is None:
        return []

    if df is None or df.empty:
        return []

    import pandas as pd

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


def _fetch_main_indices() -> list[dict[str, Any]]:
    """Fetch main A-share indices realtime quotes."""
    import time
    import akshare as ak

    targets = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sz399006": "创业板指",
        "sh000688": "科创50",
        "sh000300": "沪深300",
        "sh000016": "上证50",
    }
    results: list[dict[str, Any]] = []
    df = None
    for attempt in range(3):
        try:
            df = ak.stock_zh_index_spot_em()
            break
        except Exception as e:
            logger.warning(f"[Market] 指数行情获取失败 (第{attempt + 1}次): {e}")
            if attempt < 2:
                time.sleep(1)
    if df is None or df.empty:
        return results
    import pandas as pd
    for _, row in df.iterrows():
        code = str(row.get("代码", ""))
        if code not in targets:
            continue
        change_pct = pd.to_numeric(row.get("涨跌幅", 0), errors="coerce") or 0.0
        results.append({
            "code": code,
            "name": targets[code],
            "current": float(row.get("最新价", 0) or 0),
            "change_pct": float(change_pct),
            "change_amount": float(row.get("涨跌额", 0) or 0),
            "volume": float(row.get("成交量", 0) or 0),
            "amount": float(row.get("成交额", 0) or 0),
        })
    order = list(targets.keys())
    results.sort(key=lambda x: order.index(x["code"]) if x["code"] in order else 999)
    return results


@router.get("/heatmap")
def get_sector_heatmap(user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Return all industry sectors for treemap heatmap display."""
    sectors = _fetch_sector_heatmap()
    return {"sectors": sectors, "count": len(sectors)}


@router.get("/indices")
def get_main_indices(user: User = Depends(get_current_user)) -> dict[str, Any]:
    """Return main index realtime quotes."""
    indices = _fetch_main_indices()
    return {"indices": indices}
