"""Transactional personal cash ledger, independent of legacy CSV/JSON accounts.

The caller owns commit/rollback. Idempotency keys are scoped to one account and
one operation family (initialization, cash movement, or execution). A write lock is acquired before reading account
state, so simultaneous SQLite requests cannot spend the same cash or shares.
Dates are execution dates in Asia/Shanghai, not server-local timestamps.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import update
from sqlalchemy.orm import Session

from stopat30m.storage.models import (
    PortfolioAccount, PortfolioCashFlow, PortfolioExecution, TradeRecord, User,
)
from .rules import calculate_fees

SHANGHAI = ZoneInfo("Asia/Shanghai")
CENT = Decimal("0.01")
ZERO = Decimal("0")
MAX_MONEY = Decimal("1000000000000")


class PortfolioError(ValueError):
    def __init__(self, message: str, status_code: int = 409):
        super().__init__(message)
        self.status_code = status_code


def today() -> date:
    return datetime.now(SHANGHAI).date()


def money(value: Any, label: str = "金额", *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise PortfolioError(f"{label}必须是有限数字", 400) from None
    if not number.is_finite() or number < 0 or number > MAX_MONEY:
        raise PortfolioError(f"{label}必须是合理范围内的非负有限数字", 400)
    number = number.quantize(CENT, rounding=ROUND_HALF_UP)
    if positive and number <= 0:
        raise PortfolioError(f"{label}必须大于零", 400)
    return number


def execution_date(value: str | date | None) -> date:
    try:
        if isinstance(value, str) and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("noncanonical date")
        parsed = value if isinstance(value, date) and not isinstance(value, datetime) else (
            date.fromisoformat(value) if value is not None else today()
        )
    except (ValueError, TypeError):
        raise PortfolioError("日期格式应为 YYYY-MM-DD", 400) from None
    if parsed > today():
        raise PortfolioError("不能记录未来日期的资金或成交", 400)
    return parsed


def _key(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 100:
        raise PortfolioError("每次操作必须提供 1–100 字符的幂等标识", 400)
    return value.strip()


def _payload(**values: Any) -> str:
    return json.dumps(values, default=str, ensure_ascii=False, sort_keys=True)


def _write_lock(db: Session, user_id: int) -> None:
    # An actual UPDATE acquires SQLite's write reservation before any balances
    # are read. SELECT FOR UPDATE alone is silently ineffective on SQLite.
    result = db.execute(update(User).where(User.id == user_id).values(id=User.id))
    if result.rowcount != 1:
        raise PortfolioError("用户不存在", 404)
    db.expire_all()


def get_account(db: Session, user_id: int, *, required: bool = True) -> PortfolioAccount | None:
    account = db.query(PortfolioAccount).filter_by(user_id=user_id).first()
    if account is None and required:
        raise PortfolioError("请先初始化个人账户")
    return account


def _chronology(account: PortfolioAccount, day: date) -> None:
    if day < account.opened_date:
        raise PortfolioError("日期不能早于账户开始日期")
    if day < account.last_event_date:
        raise PortfolioError(f"不能回填早于最新账本日期 {account.last_event_date} 的记录；请先核对账本")


def initialize_account(
    db: Session, user_id: int, initial_capital: Any, idempotency_key: str,
    name: str = "个人账户", opened_date: str | date | None = None,
) -> tuple[PortfolioAccount, bool]:
    capital, key = money(initial_capital), _key(idempotency_key)
    # Preserve omission in the request fingerprint, so a next-day retry is safe.
    day = execution_date(opened_date)
    name = name.strip()
    if not name or len(name) > 80:
        raise PortfolioError("账户名称应为 1–80 字符", 400)
    payload = _payload(initial_capital=capital, name=name, opened_date=opened_date)
    _write_lock(db, user_id)
    existing = get_account(db, user_id, required=False)
    if existing:
        if existing.initialization_key == key and existing.initialization_payload == payload:
            return existing, True
        raise PortfolioError("个人账户已初始化，不能覆盖已有资金与成交")
    account = PortfolioAccount(
        user_id=user_id, name=name, opened_date=day, last_event_date=day,
        initial_capital=capital, cash=capital, net_deposits=capital,
        initialization_key=key, initialization_payload=payload,
    )
    db.add(account)
    db.flush()
    db.add(PortfolioCashFlow(
        account_id=account.id, direction="INITIAL", amount=capital,
        effective_date=day, cash_after=capital, note="账户初始本金",
        # Initialization retries use the account's initialization_key. The
        # reserved empty cash-flow key cannot collide with a public request.
        idempotency_key="", request_payload=payload,
    ))
    db.flush()
    return account, False


def record_cash_flow(
    db: Session, user_id: int, direction: str, amount: Any, idempotency_key: str,
    effective_date: str | date | None = None, note: str = "",
) -> tuple[PortfolioCashFlow, bool]:
    if direction not in {"DEPOSIT", "WITHDRAW"}:
        raise PortfolioError("资金方向必须为 DEPOSIT 或 WITHDRAW", 400)
    value, key = money(amount, positive=True), _key(idempotency_key)
    day = execution_date(effective_date)
    payload = _payload(direction=direction, amount=value, effective_date=effective_date, note=note)
    _write_lock(db, user_id)
    account = get_account(db, user_id)
    previous = db.query(PortfolioCashFlow).filter_by(account_id=account.id, idempotency_key=key).first()
    if previous:
        if previous.request_payload != payload:
            raise PortfolioError("幂等标识已用于其他资金记录")
        return previous, True
    _chronology(account, day)
    delta = value if direction == "DEPOSIT" else -value
    if account.cash + delta < 0:
        raise PortfolioError("可用现金不足，不能出金")
    account.cash += delta
    account.net_deposits += delta
    account.last_event_date = day
    row = PortfolioCashFlow(
        account_id=account.id, direction=direction, amount=value,
        effective_date=day, cash_after=account.cash, note=note,
        idempotency_key=key, request_payload=payload,
    )
    db.add(row)
    db.flush()
    return row, False


@dataclass
class Holding:
    quantity: int = 0
    cost: Decimal = ZERO
    realized: Decimal = ZERO
    purchases: dict[date, int] = field(default_factory=dict)

    def available(self, day: date) -> int:
        return max(0, self.quantity - self.purchases.get(day, 0))


def holdings(db: Session, account_id: int) -> dict[str, Holding]:
    result: dict[str, Holding] = {}
    # IDs preserve append order for executions on the same exchange date.
    for row in db.query(PortfolioExecution).filter_by(account_id=account_id).order_by(PortfolioExecution.id):
        pos = result.setdefault(row.instrument, Holding())
        fees = row.commission + row.stamp_tax + row.transfer_fee
        if row.direction == "BUY":
            pos.cost += row.amount + fees
            pos.quantity += row.quantity
            pos.purchases[row.trade_date] = pos.purchases.get(row.trade_date, 0) + row.quantity
        else:
            if row.quantity > pos.quantity:
                raise PortfolioError("账本存在超卖记录，请先人工核对", 500)
            removed = pos.cost if row.quantity == pos.quantity else (
                pos.cost * row.quantity / pos.quantity
            ).quantize(CENT, rounding=ROUND_HALF_UP)
            pos.cost -= removed
            pos.quantity -= row.quantity
            pos.realized += row.amount - fees - removed
    return result


def _instrument(raw: str) -> str:
    raw = raw.strip().upper()
    if re.fullmatch(r"\d{6}", raw):
        exchange = "SH" if raw[0] in "569" else ("BJ" if raw[0] in "48" else "SZ")
        return exchange + raw
    match = re.fullmatch(r"(SH|SZ|BJ)\.?([0-9]{6})", raw)
    if match:
        return match.group(1) + match.group(2)
    match = re.fullmatch(r"([0-9]{6})\.(SH|SZ|BJ)", raw)
    if match:
        return match.group(2) + match.group(1)
    raise PortfolioError("证券代码应为 6 位数字或带 SH/SZ/BJ 交易所前后缀", 400)


def record_trade(
    db: Session, user_id: int, instrument: str, direction: str, quantity: int,
    price: Any, idempotency_key: str, trade_date: str | date | None = None,
    commission: Any | None = None, stamp_tax: Any | None = None,
    transfer_fee: Any | None = None, note: str = "",
) -> tuple[PortfolioExecution, bool]:
    inst, key = _instrument(instrument), _key(idempotency_key)
    if direction not in {"BUY", "SELL"}:
        raise PortfolioError("成交方向必须为 BUY 或 SELL", 400)
    if isinstance(quantity, bool) or not isinstance(quantity, int) or not 0 < quantity <= 1_000_000_000:
        raise PortfolioError("成交数量必须为合理范围内的正整数", 400)
    try:
        px = Decimal(str(price))
    except (InvalidOperation, ValueError, TypeError):
        raise PortfolioError("成交价必须是有限数字", 400) from None
    if not px.is_finite() or not ZERO < px <= Decimal("1000000"):
        raise PortfolioError("成交价必须为合理范围内的正有限数字", 400)
    px = px.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    if px <= 0:
        raise PortfolioError("成交价精度不能超过四位小数", 400)
    gross = money(px * quantity, "成交金额", positive=True)
    day = execution_date(trade_date)
    estimated = calculate_fees(direction, quantity, float(px))
    fee_values = [money(value if value is not None else default, label) for value, default, label in (
        (commission, estimated.commission, "佣金"),
        (stamp_tax, estimated.stamp_tax, "印花税"),
        (transfer_fee, estimated.transfer_fee, "过户费"),
    )]
    fees = sum(fee_values, ZERO)
    payload = _payload(instrument=inst, direction=direction, quantity=quantity, price=px,
                       trade_date=trade_date, fees=fee_values, note=note)
    _write_lock(db, user_id)
    account = get_account(db, user_id)
    previous = db.query(PortfolioExecution).filter_by(account_id=account.id, idempotency_key=key).first()
    if previous:
        if previous.request_payload != payload:
            raise PortfolioError("幂等标识已用于其他成交记录")
        return previous, True
    _chronology(account, day)
    positions = holdings(db, account.id)
    pos = positions.get(inst, Holding())
    realized = ZERO
    if direction == "BUY":
        delta = -(gross + fees)
        if -delta > account.cash:
            raise PortfolioError("可用现金不足以覆盖成交金额与全部费用")
    else:
        if quantity > pos.quantity:
            raise PortfolioError(f"持仓不足：持有 {pos.quantity} 股，不能卖出 {quantity} 股")
        if quantity > pos.available(day):
            raise PortfolioError(f"T+1 可卖不足：当日可卖 {pos.available(day)} 股")
        delta = gross - fees
        if account.cash + delta < 0:
            raise PortfolioError("卖出费用超过所得及可用现金")
        removed = pos.cost if quantity == pos.quantity else (
            pos.cost * quantity / pos.quantity
        ).quantize(CENT, rounding=ROUND_HALF_UP)
        realized = delta - removed
    account.cash += delta
    account.last_event_date = day
    row = PortfolioExecution(
        account_id=account.id, trade_date=day, instrument=inst, direction=direction,
        quantity=quantity, price=px, amount=gross, commission=fee_values[0],
        stamp_tax=fee_values[1], transfer_fee=fee_values[2], net_amount=delta,
        realized_pnl=realized, cash_after=account.cash, note=note,
        idempotency_key=key, request_payload=payload,
    )
    db.add(row)
    db.flush()
    return row, False


def legacy_counts(db: Session, user_id: int, admin: bool = False) -> dict[str, int]:
    return {
        "legacy_trade_count": db.query(TradeRecord).filter_by(user_id=user_id).count(),
        "legacy_unassigned_count": db.query(TradeRecord).filter(TradeRecord.user_id.is_(None)).count() if admin else 0,
    }


def valuation(
    db: Session, account: PortfolioAccount, quotes: dict[str, dict] | None = None,
) -> tuple[dict, list[dict]]:
    quotes = quotes or {}
    positions = holdings(db, account.id)
    rows: list[dict] = []
    warnings: list[str] = []
    priced_value, cost, realized, total_fees = ZERO, ZERO, ZERO, ZERO
    for execution in db.query(PortfolioExecution).filter_by(account_id=account.id):
        total_fees += execution.commission + execution.stamp_tax + execution.transfer_fee
    for inst, pos in positions.items():
        realized += pos.realized
        if not pos.quantity:
            continue
        cost += pos.cost
        quote = quotes.get(inst) or {}
        raw_price = quote.get("price")
        try:
            price = Decimal(str(raw_price)) if raw_price is not None else None
            if price is not None and (not price.is_finite() or price <= 0 or price > Decimal("1000000")):
                price = None
        except InvalidOperation:
            price = None
        last_known_price = price
        if quote.get("status") == "stale":
            warnings.append(f"{inst} 报价已过期，未纳入账户估值")
            price = None
        elif price is not None and not quote.get("as_of"):
            warnings.append(f"{inst} 报价源未提供行情时间")
        market_value = (price * pos.quantity).quantize(CENT, rounding=ROUND_HALF_UP) if price is not None else None
        pnl = market_value - pos.cost if market_value is not None else None
        if market_value is not None:
            priced_value += market_value
        available = pos.available(today())
        rows.append({
            "instrument": inst, "code": inst[2:], "name": quote.get("name", ""),
            "quantity": pos.quantity, "available_quantity": available,
            "frozen_quantity": pos.quantity - available,
            "avg_cost": round(float(pos.cost / pos.quantity), 4), "cost": float(pos.cost),
            "current_price": float(price) if price is not None else None,
            "last_known_price": float(last_known_price) if last_known_price is not None else None,
            "market_value": float(market_value) if market_value is not None else None,
            "pnl": float(pnl) if pnl is not None else None,
            "pnl_pct": round(float(pnl / pos.cost * 100), 4) if pnl is not None and pos.cost else None,
            "realized_pnl": float(pos.realized),
            "price_status": quote.get("status", "unavailable") if last_known_price is not None else "unavailable",
            "price_source": quote.get("source"), "price_as_of": quote.get("as_of"),
            "price_fetched_at": quote.get("fetched_at"),
        })
    priced = sum(row["current_price"] is not None for row in rows)
    complete = priced == len(rows)
    equity = account.cash + priced_value if complete else None
    summary = {
        "id": account.id, "name": account.name, "currency": account.currency,
        "timezone": "Asia/Shanghai", "opened_date": account.opened_date.isoformat(),
        "initial_capital": float(account.initial_capital), "net_deposits": float(account.net_deposits),
        "cash": float(account.cash), "total_cost": float(cost), "realized_pnl": float(realized),
        "total_fees": float(total_fees), "market_value": float(priced_value) if complete else None,
        "equity": float(equity) if equity is not None else None,
        "unrealized_pnl": float(priced_value - cost) if complete else None,
        "total_pnl": float(equity - account.net_deposits) if equity is not None else None,
        "valuation_complete": complete, "priced_positions": priced, "total_positions": len(rows),
        "valuation_warnings": warnings,
        "as_of": datetime.now(SHANGHAI).isoformat(),
    }
    return summary, rows


def execution_dict(row: PortfolioExecution, replayed: bool = False) -> dict:
    fees = row.commission + row.stamp_tax + row.transfer_fee
    return {
        "id": row.id, "trade_date": row.trade_date.isoformat(), "instrument": row.instrument,
        "direction": row.direction, "quantity": row.quantity, "price": float(row.price),
        "amount": float(row.amount), "commission": float(row.commission), "stamp_tax": float(row.stamp_tax),
        "transfer_fee": float(row.transfer_fee), "total_fee": float(fees), "fees": float(fees),
        "net_amount": float(row.net_amount), "realized_pnl": float(row.realized_pnl),
        "cash_after": float(row.cash_after), "note": row.note, "source": row.source,
        "created_at": row.created_at.replace(tzinfo=timezone.utc).isoformat(), "replayed": replayed,
    }


def cash_flow_dict(row: PortfolioCashFlow, replayed: bool = False) -> dict:
    return {
        "id": row.id, "direction": row.direction, "amount": float(row.amount),
        "effective_date": row.effective_date.isoformat(), "cash_after": float(row.cash_after),
        "note": row.note, "created_at": row.created_at.replace(tzinfo=timezone.utc).isoformat(),
        "replayed": replayed,
    }
