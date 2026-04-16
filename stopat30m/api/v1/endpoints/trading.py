"""Trading center API: positions, manual trades, rebalancing."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from stopat30m.api.deps import get_db_session
from stopat30m.auth.deps import get_current_user
from stopat30m.data.normalize import bare_code, normalize_instrument
from stopat30m.data.realtime import fetch_spot_prices, fetch_stock_names
from stopat30m.storage.models import TradeRecord, User

router = APIRouter(prefix="/trading")


class ManualTradeRequest(BaseModel):
    instrument: str
    direction: str = Field(..., pattern="^(BUY|SELL)$")
    quantity: int = Field(..., gt=0)
    price: float = Field(..., gt=0)
    note: str = ""


class RebalanceRequest(BaseModel):
    signal_file: str | None = None
    use_paper: bool = True


# -- Positions --

@router.get("/positions")
def get_positions(
    show_all: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> dict:
    """Get current portfolio positions derived from trade records."""
    query = db.query(TradeRecord).order_by(TradeRecord.trade_date)
    if not (show_all and user.role == "admin"):
        query = query.filter(
            (TradeRecord.user_id == user.id) | (TradeRecord.user_id == None)  # noqa: E711
        )
    trades = query.all()
    positions: dict[str, dict[str, Any]] = {}

    for t in trades:
        inst = normalize_instrument(t.instrument)
        if inst not in positions:
            positions[inst] = {"quantity": 0, "total_cost": 0.0, "realized_pnl": 0.0}

        pos = positions[inst]
        if t.direction == "BUY":
            pos["total_cost"] += t.amount + (t.commission or 0) + (t.stamp_tax or 0) + (t.transfer_fee or 0)
            pos["quantity"] += t.quantity
        elif t.direction == "SELL":
            if pos["quantity"] > 0:
                avg_cost = pos["total_cost"] / pos["quantity"]
                sell_cost = t.quantity * avg_cost
                pos["total_cost"] -= sell_cost
                pos["realized_pnl"] += t.amount - sell_cost - (t.commission or 0) - (t.stamp_tax or 0)
            pos["quantity"] -= t.quantity

    active = {k: v for k, v in positions.items() if v["quantity"] > 0}
    if not active:
        return {"positions": [], "total_value": 0, "total_cost": 0}

    instruments = list(active.keys())
    prices = fetch_spot_prices(instruments)
    names = fetch_stock_names(instruments)

    result = []
    total_value = 0.0
    total_cost = 0.0
    for inst, pos in active.items():
        price = prices.get(inst, 0)
        market_value = pos["quantity"] * price
        avg_cost = pos["total_cost"] / pos["quantity"] if pos["quantity"] > 0 else 0
        pnl = market_value - pos["total_cost"]
        pnl_pct = pnl / pos["total_cost"] * 100 if pos["total_cost"] > 0 else 0

        result.append({
            "instrument": inst,
            "code": bare_code(inst),
            "name": names.get(inst, ""),
            "quantity": pos["quantity"],
            "avg_cost": round(avg_cost, 4),
            "current_price": round(price, 4),
            "market_value": round(market_value, 2),
            "cost": round(pos["total_cost"], 2),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
        })
        total_value += market_value
        total_cost += pos["total_cost"]

    return {
        "positions": result,
        "total_value": round(total_value, 2),
        "total_cost": round(total_cost, 2),
        "total_pnl": round(total_value - total_cost, 2),
    }


# -- Manual trades --

@router.post("/trade")
def submit_trade(req: ManualTradeRequest, user: User = Depends(get_current_user), db: Session = Depends(get_db_session)) -> dict:
    """Record a manual trade."""
    inst = normalize_instrument(req.instrument)
    amount = req.quantity * req.price

    from stopat30m.trading.rules import calculate_fees
    from stopat30m.trading.models import OrderDirection

    direction = OrderDirection.BUY if req.direction == "BUY" else OrderDirection.SELL
    fees = calculate_fees(direction, req.quantity, req.price)

    record = TradeRecord(
        trade_date=datetime.utcnow(),
        instrument=inst,
        direction=req.direction,
        quantity=req.quantity,
        price=req.price,
        amount=round(amount, 2),
        commission=fees.commission,
        stamp_tax=fees.stamp_tax,
        transfer_fee=fees.transfer_fee,
        total_cost=round(amount + fees.commission + fees.stamp_tax + fees.transfer_fee, 2),
        note=req.note,
        source="manual_api",
        user_id=user.id,
    )
    db.add(record)
    db.flush()

    return {"id": record.id, "instrument": inst, "direction": req.direction, "quantity": req.quantity, "price": req.price}


# -- Trade history --

@router.get("/trades")
def get_trades(
    limit: int = 50,
    offset: int = 0,
    instrument: str | None = None,
    show_all: bool = False,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session),
) -> list[dict]:
    """List trade records."""
    query = db.query(TradeRecord).order_by(TradeRecord.trade_date.desc())
    if not (show_all and user.role == "admin"):
        query = query.filter(
            (TradeRecord.user_id == user.id) | (TradeRecord.user_id == None)  # noqa: E711
        )
    if instrument:
        query = query.filter(TradeRecord.instrument == normalize_instrument(instrument))
    records = query.offset(offset).limit(limit).all()
    return [
        {
            "id": r.id,
            "trade_date": r.trade_date.isoformat() if r.trade_date else "",
            "instrument": r.instrument,
            "direction": r.direction,
            "quantity": r.quantity,
            "price": r.price,
            "amount": r.amount,
            "commission": r.commission,
            "note": r.note,
            "source": r.source,
        }
        for r in records
    ]
