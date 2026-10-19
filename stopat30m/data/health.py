"""Inspect local research data without loading Qlib or fetching market data."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def inspect_data(data_dir: Path, *, today: date | None = None) -> dict[str, Any]:
    """Return observable coverage, never equating retrieval time with freshness.

    A local Qlib calendar may end with the data, so it cannot independently
    establish the most recent exchange session. No network request is made.
    """
    today = today or datetime.now(ZoneInfo("Asia/Shanghai")).date()
    warnings: list[str] = []
    calendar_path = data_dir / "calendars" / "day.txt"
    calendar: list[date] = []
    if calendar_path.is_file():
        try:
            lines = [line.strip() for line in calendar_path.read_text().splitlines() if line.strip()]
            parsed = [_date(line) for line in lines]
            calendar = sorted({d for d in parsed if d is not None and d <= today})
            if any(d is None for d in parsed) or parsed != sorted(set(parsed), key=lambda d: d or date.min):
                warnings.append("交易日历含无效、重复或未排序日期，请检查数据文件。")
        except (OSError, UnicodeError):
            warnings.append("无法读取本地交易日历。")
    if not calendar:
        warnings.append("没有有效的本地交易日历。")

    features = data_dir / "features"
    feature_count = 0
    if features.is_dir():
        feature_count = sum(
            1 for path in features.glob("*/close.day.bin")
            if path.is_file() and path.stat().st_size >= 8
        )
    instruments = data_dir / "instruments"
    instruments_available = instruments.is_dir() and any(
        path.is_file() and path.stat().st_size > 0 for path in instruments.iterdir()
    )
    available = bool(calendar and feature_count and instruments_available)
    if not feature_count:
        warnings.append("没有包含有效收盘价数据的证券文件。")
    if not instruments_available:
        warnings.append("缺少有效股票池文件。")

    meta: dict[str, Any] = {}
    meta_path = data_dir / "data_meta.json"
    if meta_path.is_file():
        try:
            payload = json.loads(meta_path.read_text())
            if not isinstance(payload, dict) or not isinstance(payload.get("stocks", {}), dict):
                raise ValueError("invalid metadata shape")
            meta = payload
        except (OSError, UnicodeError, ValueError):
            warnings.append("数据元信息损坏，不能确认证券覆盖范围。")
    else:
        warnings.append("缺少证券覆盖元信息，无法确认每只股票的数据截止日。")

    stocks = list(meta.get("stocks", {}).values())
    active = [s for s in stocks if isinstance(s, dict) and s.get("status") not in ("delisted", "index")]
    dates = [_date(s.get("data_end")) for s in active]
    valid = [d for d in dates if d is not None and d <= today]
    latest_calendar = calendar[-1] if calendar else None
    last_data = max(valid) if valid else None
    # Missing metadata is unknown, rather than a claim of complete coverage.
    missing = sum(d is None or d > today for d in dates)
    lagging = sum(d < latest_calendar for d in valid) if latest_calendar else 0
    excluded = sum(not s.get("ipo_date") for s in active)
    if missing:
        warnings.append(f"{missing} 只证券缺少有效数据截止日。")
    if lagging:
        warnings.append(f"{lagging} 只证券的数据落后于本地日历末日。")
    if excluded:
        warnings.append(f"{excluded} 只证券缺少上市日期，可能未纳入原有可信水位。")
    age = (today - last_data).days if last_data else None
    if age is not None and age > 7:
        warnings.append(f"最近证券数据距今天已有 {age} 个自然日；请结合交易所休市安排核对并更新。")
    warnings.append("本地日历不是独立的最新交易日依据；数据日期已展示，尚未核验交易所最新完整交易日。")
    status = "available"
    if not available:
        status = "missing"
    elif age is not None and age > 7:
        status = "stale"
    elif not stocks or missing or lagging or not last_data:
        status = "degraded"
    covered = len(valid) - lagging
    return {
        "data_dir": str(data_dir), "data_available": available,
        "calendar_exists": calendar_path.is_file(), "status": status,
        "calendar_count": len(calendar), "latest_calendar_date": latest_calendar.isoformat() if latest_calendar else None,
        "last_data_date": last_data.isoformat() if last_data else None,
        "trusted_until": meta.get("trusted_until", ""), "last_append": meta.get("last_append", ""),
        "stock_count": len(stocks), "feature_stock_count": feature_count,
        "active_stock_count": len(active), "missing_stock_count": missing,
        "lagging_stock_count": lagging, "excluded_stock_count": excluded,
        "coverage_pct": round(100 * covered / len(active), 2) if active and latest_calendar else None,
        "age_calendar_days": age, "freshness_confirmed": False,
        "quality_scope": "local_structure_and_metadata", "warnings": warnings,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
