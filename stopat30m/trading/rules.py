"""
A-share trading rules: fees, lot sizing, price limits, T+1, order validation.

Pure functions — no state, no I/O. Every rule is individually callable
and composable via `validate_order`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time

from .models import Account, Order, OrderDirection, OrderType, Position

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOT_SIZE = 100

# Fee rates (as of 2023-08 A-share market)
DEFAULT_COMMISSION_RATE = 0.00025   # 万2.5 per side
MIN_COMMISSION = 5.0                # 最低5元
STAMP_TAX_RATE = 0.0005             # 印花税 万5, SELL only (halved Aug 2023)
TRANSFER_FEE_RATE = 0.00001         # 过户费 十万分之一, both sides

# Price-limit tiers
LIMIT_NORMAL = 0.10     # 主板 ±10%
LIMIT_GEM_STAR = 0.20   # 创业板(30x)/科创板(68x) ±20%
LIMIT_ST = 0.05         # ST/PT ±5%

# A-share trading sessions (CST)
MORNING_OPEN = time(9, 30)
MORNING_CLOSE = time(11, 30)
AFTERNOON_OPEN = time(13, 0)
AFTERNOON_CLOSE = time(15, 0)


# ---------------------------------------------------------------------------
# Trading session
# ---------------------------------------------------------------------------

def is_trading_time(now: datetime | None = None) -> bool:
    """Check if the given time falls within A-share continuous auction sessions."""
    t = (now or datetime.now()).time()
    return (MORNING_OPEN <= t <= MORNING_CLOSE) or (AFTERNOON_OPEN <= t <= AFTERNOON_CLOSE)


def is_trading_day(dt: datetime | None = None) -> bool:
    """Heuristic: weekdays are trading days. Does NOT check holidays."""
    d = (dt or datetime.now())
    return d.weekday() < 5


# ---------------------------------------------------------------------------
# Fee calculation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FeeResult:
    commission: float
    stamp_tax: float
    transfer_fee: float

    @property
    def total(self) -> float:
        return self.commission + self.stamp_tax + self.transfer_fee


def calculate_fees(
    direction: OrderDirection | str,
    quantity: int,
    price: float,
    commission_rate: float = DEFAULT_COMMISSION_RATE,
) -> FeeResult:
    """Compute all fees for a single trade."""
    if isinstance(direction, str):
        direction = OrderDirection(direction)

    amount = quantity * price
    commission = max(amount * commission_rate, MIN_COMMISSION)
    stamp_tax = amount * STAMP_TAX_RATE if direction == OrderDirection.SELL else 0.0
    transfer_fee = amount * TRANSFER_FEE_RATE
    return FeeResult(
        commission=round(commission, 2),
        stamp_tax=round(stamp_tax, 2),
        transfer_fee=round(transfer_fee, 2),
    )


# ---------------------------------------------------------------------------
# Lot sizing
# ---------------------------------------------------------------------------

def round_lot(quantity: int | float) -> int:
    """Floor to nearest lot (100 shares)."""
    return int(quantity) // LOT_SIZE * LOT_SIZE


def is_valid_lot(quantity: int) -> bool:
    return quantity > 0 and quantity % LOT_SIZE == 0


# ---------------------------------------------------------------------------
# Price limits
# ---------------------------------------------------------------------------

def _bare_code(instrument: str) -> str:
    """Strip SH/SZ prefix to get bare 6-digit code."""
    inst = instrument.strip().upper()
    if inst.startswith(("SH", "SZ")):
        return inst[2:]
    return inst


def _limit_pct(instrument: str) -> float:
    """Determine daily price-limit percentage for an instrument code."""
    bare = _bare_code(instrument)
    if bare.startswith("30") or bare.startswith("68"):
        return LIMIT_GEM_STAR
    return LIMIT_NORMAL


def price_limit_range(
    prev_close: float,
    instrument: str,
    is_st: bool = False,
) -> tuple[float, float]:
    """Return (lower_limit, upper_limit) for today's trading."""
    pct = LIMIT_ST if is_st else _limit_pct(instrument)
    lower = round(prev_close * (1 - pct), 2)
    upper = round(prev_close * (1 + pct), 2)
    return (lower, upper)


def is_price_within_limit(
    price: float,
    prev_close: float,
    instrument: str,
    is_st: bool = False,
) -> bool:
    lo, hi = price_limit_range(prev_close, instrument, is_st)
    return lo <= price <= hi


# ---------------------------------------------------------------------------
# T+1 validation
# ---------------------------------------------------------------------------

def validate_sell_quantity(position: Position, sell_qty: int) -> tuple[bool, str]:
    """Check if sell quantity respects T+1 frozen shares."""
    if sell_qty <= 0:
        return False, "卖出数量必须大于 0"
    if sell_qty > position.available_quantity:
        avail = position.available_quantity
        frozen = position.frozen_quantity
        return False, (
            f"可卖不足: 持仓 {position.quantity}, "
            f"T+1冻结 {frozen}, 可卖 {avail}, 委托 {sell_qty}"
        )
    return True, ""


# ---------------------------------------------------------------------------
# Composite order validation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    reason: str = ""


def validate_order(
    order: Order,
    account: Account,
    prev_closes: dict[str, float] | None = None,
    st_set: set[str] | None = None,
    market_price: float | None = None,
) -> ValidationResult:
    """Run all applicable checks on an order before execution.

    Checks (in order):
    1. Lot size
    2. T+1 for sells
    3. Price limits (if prev_close known and order is LIMIT)
    4. Cash sufficiency for buys
    """
    # 1. Lot size
    if not is_valid_lot(order.quantity):
        return ValidationResult(False, f"数量 {order.quantity} 不是 {LOT_SIZE} 的整数倍")

    inst = order.instrument
    position = account.positions.get(inst)

    # 2. T+1 for sells
    if order.direction == OrderDirection.SELL:
        if position is None or position.quantity <= 0:
            return ValidationResult(False, f"无持仓可卖: {inst}")
        ok, reason = validate_sell_quantity(position, order.quantity)
        if not ok:
            return ValidationResult(False, reason)

    # 3. Price limits (LIMIT orders only)
    if order.order_type == OrderType.LIMIT:
        if order.limit_price is None or order.limit_price <= 0:
            return ValidationResult(False, "限价单缺少有效限价")
        if prev_closes and inst in prev_closes:
            is_st = inst in (st_set or set())
            if not is_price_within_limit(order.limit_price, prev_closes[inst], inst, is_st):
                lo, hi = price_limit_range(prev_closes[inst], inst, is_st)
                return ValidationResult(
                    False,
                    f"委托价 {order.limit_price} 超出涨跌停范围 [{lo}, {hi}]",
                )

    # 4. Cash for buys
    if order.direction == OrderDirection.BUY:
        exec_price = (
            order.limit_price
            if order.order_type == OrderType.LIMIT
            else (
                market_price
                if market_price is not None and market_price > 0
                else (account.positions[inst].current_price if inst in account.positions else 0)
            )
        )
        if exec_price <= 0:
            return ValidationResult(False, f"无法确定执行价格: {inst}")
        fees = calculate_fees(OrderDirection.BUY, order.quantity, exec_price)
        required = order.quantity * exec_price + fees.total
        if required > account.cash:
            return ValidationResult(
                False,
                f"资金不足: 需要 ¥{required:,.2f}, 可用 ¥{account.cash:,.2f}",
            )

    return ValidationResult(True)
