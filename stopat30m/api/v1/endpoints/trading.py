"""Personal account, cash movements and manually recorded executions."""

from __future__ import annotations

import math
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, StrictInt
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.storage.models import PortfolioCashFlow, PortfolioExecution, TradeRecord, User
from stopat30m.trading import portfolio

router = APIRouter(prefix="/trading")


class RequestBase(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    idempotency_key: str = Field(..., min_length=1, max_length=100)


class InitializeRequest(RequestBase):
    initial_capital: float = Field(..., ge=0, le=1e12)
    name: str = Field("个人账户", min_length=1, max_length=80)
    opened_date: str | None = None


class CashFlowRequest(RequestBase):
    direction: Literal["DEPOSIT", "WITHDRAW"]
    amount: float = Field(..., gt=0, le=1e12)
    effective_date: str | None = None
    note: str = Field("", max_length=500)


class ManualTradeRequest(RequestBase):
    instrument: str = Field(..., min_length=1, max_length=20)
    direction: Literal["BUY", "SELL"]
    quantity: StrictInt = Field(..., gt=0, le=1_000_000_000)
    price: float = Field(..., gt=0, le=1e6)
    trade_date: str | None = None
    commission: float | None = Field(None, ge=0, le=1e12)
    stamp_tax: float | None = Field(None, ge=0, le=1e12)
    transfer_fee: float | None = Field(None, ge=0, le=1e12)
    note: str = Field("", max_length=500)


def _invoke(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except portfolio.PortfolioError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


def _legacy_number(value):
    return value if value is not None and math.isfinite(value) else None


def _account_response(db: Session, user: User) -> dict:
    account = portfolio.get_account(db, user.id, required=False)
    counts = portfolio.legacy_counts(db, user.id, user.role == "admin")
    if account is None:
        return {"initialized": False, "account": None, **counts}
    positions = portfolio.holdings(db, account.id)
    instruments = [inst for inst, pos in positions.items() if pos.quantity > 0]
    quotes = {}
    if instruments:
        from stopat30m.data.realtime import fetch_spot_quotes
        try:
            quotes = fetch_spot_quotes(instruments)
        except Exception:
            # Quote failure must leave monetary facts readable and unknown
            # valuations explicitly null. It must not imply a zero share price.
            quotes = {}
    summary, _ = portfolio.valuation(db, account, quotes)
    return {"initialized": True, "account": summary, **counts}


@router.get("/account")
def account_summary(user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    return _invoke(_account_response, db, user)


@router.post("/account/initialize")
def initialize_account(req: InitializeRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    _, replayed = _invoke(portfolio.initialize_account, db, user.id, **req.model_dump())
    # Confirm durability before sending a financial receipt. A yield dependency
    # may otherwise commit only after the response has already been emitted.
    db.commit()
    return {**_account_response(db, user), "replayed": replayed}


@router.post("/cash-flows")
def record_cash_flow(req: CashFlowRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    row, replayed = _invoke(portfolio.record_cash_flow, db, user.id, **req.model_dump())
    db.commit()
    return portfolio.cash_flow_dict(row, replayed)


@router.get("/cash-flows")
def get_cash_flows(
    limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
    user: User = Depends(get_current_user), db: Session = Depends(get_db_session),
) -> dict:
    account = portfolio.get_account(db, user.id, required=False)
    if account is None:
        return {"items": [], "total": 0, "limit": limit, "offset": offset}
    query = db.query(PortfolioCashFlow).filter_by(account_id=account.id)
    total = query.count()
    rows = query.order_by(PortfolioCashFlow.id.desc()).offset(offset).limit(limit).all()
    return {"items": [portfolio.cash_flow_dict(row) for row in rows], "total": total, "limit": limit, "offset": offset}


@router.get("/positions")
def get_positions(user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    account = portfolio.get_account(db, user.id, required=False)
    counts = portfolio.legacy_counts(db, user.id, user.role == "admin")
    if account is None:
        return {"initialized": False, "positions": [], "total_value": None,
                "total_cost": 0, "total_pnl": None, "valuation_complete": False, **counts}
    positions = portfolio.holdings(db, account.id)
    instruments = [inst for inst, pos in positions.items() if pos.quantity > 0]
    quotes = {}
    if instruments:
        from stopat30m.data.realtime import fetch_spot_quotes
        try:
            quotes = fetch_spot_quotes(instruments)
        except Exception:
            quotes = {}
    summary, rows = _invoke(portfolio.valuation, db, account, quotes)
    return {"initialized": True, "positions": rows, "total_value": summary["market_value"],
            "total_cost": summary["total_cost"], "total_pnl": summary["unrealized_pnl"],
            "valuation_complete": summary["valuation_complete"], "as_of": summary["as_of"],
            "valuation_warnings": summary["valuation_warnings"], **counts}


@router.post("/trade")
def submit_trade(req: ManualTradeRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    row, replayed = _invoke(portfolio.record_trade, db, user.id, **req.model_dump())
    db.commit()
    return portfolio.execution_dict(row, replayed)


@router.get("/trades")
def get_trades(
    limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0), instrument: str | None = None,
    user: User = Depends(get_current_user), db: Session = Depends(get_db_session),
) -> dict:
    account = portfolio.get_account(db, user.id, required=False)
    if account is None:
        return {"items": [], "total": 0, "limit": limit, "offset": offset}
    query = db.query(PortfolioExecution).filter_by(account_id=account.id)
    if instrument:
        query = query.filter_by(instrument=_invoke(portfolio._instrument, instrument))
    total = query.count()
    rows = query.order_by(PortfolioExecution.id.desc()).offset(offset).limit(limit).all()
    return {"items": [portfolio.execution_dict(row) for row in rows], "total": total, "limit": limit, "offset": offset}


@router.get("/legacy-trades")
def get_legacy_trades(
    limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
    include_unassigned: bool = False, user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    if include_unassigned and user.role != "admin":
        raise HTTPException(status_code=403, detail="只有管理员可以查看无归属历史流水")
    query = db.query(TradeRecord)
    predicate = TradeRecord.user_id == user.id
    if include_unassigned:
        predicate = predicate | TradeRecord.user_id.is_(None)
    query = query.filter(predicate)
    total = query.count()
    rows = query.order_by(TradeRecord.trade_date.desc(), TradeRecord.id.desc()).offset(offset).limit(limit).all()
    items = []
    for row in rows:
        fees = [row.commission, row.stamp_tax, row.transfer_fee]
        total_fee = round(sum(fees), 2) if all(value is not None and math.isfinite(value) and value >= 0 for value in fees) else None
        valid_amount = row.amount is not None and math.isfinite(row.amount) and row.amount > 0
        net_amount = None
        if valid_amount and total_fee is not None and row.direction in {"BUY", "SELL"}:
            net_amount = round(-(row.amount + total_fee) if row.direction == "BUY" else row.amount - total_fee, 2)
        items.append({
            "id": row.id, "trade_date": row.trade_date.isoformat() if row.trade_date else None,
            "instrument": row.instrument, "direction": row.direction, "quantity": row.quantity,
            "price": _legacy_number(row.price), "amount": _legacy_number(row.amount),
            "commission": _legacy_number(row.commission),
            "stamp_tax": _legacy_number(row.stamp_tax), "transfer_fee": _legacy_number(row.transfer_fee),
            "total_fee": total_fee, "net_amount": net_amount,
            "note": row.note, "source": row.source, "legacy": True, "unassigned": row.user_id is None,
        })
    return {"items": items, "total": total, "limit": limit, "offset": offset}
