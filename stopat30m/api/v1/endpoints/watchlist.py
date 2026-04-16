"""Watchlist (自选股) API: CRUD, batch analysis trigger, schedule config."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import AnalysisHistory, User, WatchlistItem

router = APIRouter(prefix="/watchlist")


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class AddStockBody(BaseModel):
    code: str
    name: str = ""
    note: str = ""


class AddBatchBody(BaseModel):
    codes: list[str]


class UpdateItemBody(BaseModel):
    name: str | None = None
    note: str | None = None
    sort_order: int | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _item_to_dict(item: WatchlistItem, latest: dict[str, dict] | None = None) -> dict[str, Any]:
    d: dict[str, Any] = {
        "id": item.id,
        "code": item.code,
        "name": item.name,
        "note": item.note,
        "sort_order": item.sort_order,
        "created_at": item.created_at.isoformat() if item.created_at else "",
    }
    if latest and item.code in latest:
        d["latest_analysis"] = latest[item.code]
    return d


def _get_latest_analysis(db: Session, user_id: int, codes: list[str]) -> dict[str, dict]:
    """For each code, fetch the most recent analysis summary."""
    if not codes:
        return {}

    subq = (
        db.query(
            AnalysisHistory.code,
            func.max(AnalysisHistory.id).label("max_id"),
        )
        .filter(
            AnalysisHistory.code.in_(codes),
            (AnalysisHistory.user_id == user_id) | (AnalysisHistory.user_id == None),  # noqa: E711
        )
        .group_by(AnalysisHistory.code)
        .subquery()
    )
    rows = (
        db.query(AnalysisHistory)
        .join(subq, AnalysisHistory.id == subq.c.max_id)
        .all()
    )
    result: dict[str, dict] = {}
    for r in rows:
        result[r.code] = {
            "id": r.id,
            "analysis_date": r.analysis_date.isoformat() if r.analysis_date else "",
            "signal_score": r.signal_score,
            "buy_signal": r.buy_signal,
            "model_score": r.model_score,
            "model_percentile": r.model_percentile,
            "llm_operation_advice": r.llm_operation_advice,
            "llm_sentiment": r.llm_sentiment,
        }
    return result


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
def list_watchlist(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    """List user's watchlist with latest analysis summary."""
    items = (
        db.query(WatchlistItem)
        .filter(WatchlistItem.user_id == user.id)
        .order_by(WatchlistItem.sort_order, WatchlistItem.created_at)
        .all()
    )
    codes = [it.code for it in items]
    latest = _get_latest_analysis(db, user.id, codes)
    return {
        "items": [_item_to_dict(it, latest) for it in items],
        "count": len(items),
    }


@router.post("")
def add_stock(
    body: AddStockBody,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    """Add a stock to user's watchlist."""
    from stopat30m.data.normalize import normalize_instrument
    norm = normalize_instrument(body.code)
    if not norm:
        raise HTTPException(400, "无效的股票代码")

    existing = (
        db.query(WatchlistItem)
        .filter(WatchlistItem.user_id == user.id, WatchlistItem.code == norm)
        .first()
    )
    if existing:
        raise HTTPException(409, f"{norm} 已在自选股中")

    name = body.name
    if not name:
        try:
            from stopat30m.data.realtime import fetch_stock_names
            names = fetch_stock_names([norm])
            name = names.get(norm, "")
        except Exception:
            pass

    item = WatchlistItem(user_id=user.id, code=norm, name=name, note=body.note)
    db.add(item)
    db.flush()
    return _item_to_dict(item)


@router.post("/batch")
def add_batch(
    body: AddBatchBody,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    """Add multiple stocks at once. Skips duplicates."""
    from stopat30m.data.normalize import normalize_instrument

    codes = [normalize_instrument(c) for c in body.codes if normalize_instrument(c)]
    if not codes:
        raise HTTPException(400, "没有有效的股票代码")

    existing_codes = {
        r.code
        for r in db.query(WatchlistItem.code)
        .filter(WatchlistItem.user_id == user.id, WatchlistItem.code.in_(codes))
        .all()
    }

    names: dict[str, str] = {}
    new_codes = [c for c in codes if c not in existing_codes]
    if new_codes:
        try:
            from stopat30m.data.realtime import fetch_stock_names
            names = fetch_stock_names(new_codes)
        except Exception:
            pass

    added = []
    for c in new_codes:
        item = WatchlistItem(user_id=user.id, code=c, name=names.get(c, ""))
        db.add(item)
        added.append(c)

    db.flush()
    return {"added": added, "skipped": [c for c in codes if c in existing_codes]}



@router.delete("/{item_id}")
def remove_stock(
    item_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    """Remove a stock from watchlist."""
    item = db.query(WatchlistItem).filter(
        WatchlistItem.id == item_id, WatchlistItem.user_id == user.id
    ).first()
    if not item:
        raise HTTPException(404, "未找到该自选股")
    db.delete(item)
    return {"deleted": True, "code": item.code}


@router.post("/batch-delete")
def batch_remove(
    payload: dict,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    """Remove multiple stocks by IDs."""
    ids: list[int] = payload.get("ids", [])
    if not ids:
        raise HTTPException(400, "No IDs provided")
    deleted = (
        db.query(WatchlistItem)
        .filter(WatchlistItem.id.in_(ids), WatchlistItem.user_id == user.id)
        .delete(synchronize_session="fetch")
    )
    return {"deleted": deleted}


@router.put("/{item_id}")
def update_item(
    item_id: int,
    body: UpdateItemBody,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    """Update watchlist item (name, note, sort order)."""
    item = db.query(WatchlistItem).filter(
        WatchlistItem.id == item_id, WatchlistItem.user_id == user.id
    ).first()
    if not item:
        raise HTTPException(404, "未找到该自选股")
    if body.name is not None:
        item.name = body.name
    if body.note is not None:
        item.note = body.note
    if body.sort_order is not None:
        item.sort_order = body.sort_order
    db.flush()
    return _item_to_dict(item)


@router.post("/analyze/{item_id}")
def analyze_single(
    item_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    """Submit a single watchlist stock for analysis (upsert mode)."""
    item = db.query(WatchlistItem).filter(
        WatchlistItem.id == item_id, WatchlistItem.user_id == user.id
    ).first()
    if not item:
        raise HTTPException(404, "未找到该自选股")

    from stopat30m.analysis.task_queue import get_task_queue
    q = get_task_queue()
    accepted, duplicates = q.submit_tasks_batch([item.code], user_id=user.id, upsert=True)
    return {
        "submitted": len(accepted),
        "duplicates": len(duplicates),
        "code": item.code,
    }


@router.post("/analyze-all")
def analyze_all(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict[str, Any]:
    """Submit all watchlist stocks to the task queue for analysis."""
    items = (
        db.query(WatchlistItem)
        .filter(WatchlistItem.user_id == user.id)
        .all()
    )
    if not items:
        raise HTTPException(400, "自选股列表为空")

    from stopat30m.analysis.task_queue import get_task_queue
    q = get_task_queue()
    codes = [it.code for it in items]
    accepted, duplicates = q.submit_tasks_batch(codes, user_id=user.id, upsert=True)
    return {
        "submitted": len(accepted),
        "duplicates": len(duplicates),
        "codes": codes,
    }
