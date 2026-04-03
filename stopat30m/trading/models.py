"""
Data models for the paper trading module.

All core domain objects — Order, Fill, Position, Account, NavSnapshot —
are plain dataclasses with JSON (de)serialization support. No external
dependencies beyond the standard library.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OrderDirection(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


# ---------------------------------------------------------------------------
# Order
# ---------------------------------------------------------------------------

@dataclass
class Order:
    """A single buy/sell order."""

    instrument: str
    direction: OrderDirection
    order_type: OrderType
    quantity: int
    limit_price: float | None = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: int = 0
    avg_fill_price: float = 0.0
    commission: float = 0.0
    stamp_tax: float = 0.0
    transfer_fee: float = 0.0
    reject_reason: str = ""
    note: str = ""
    id: str = field(default_factory=lambda: uuid4().hex[:12])
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    updated_at: str = ""

    @property
    def total_fee(self) -> float:
        return self.commission + self.stamp_tax + self.transfer_fee

    @property
    def turnover(self) -> float:
        return self.filled_quantity * self.avg_fill_price

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["direction"] = self.direction.value
        d["order_type"] = self.order_type.value
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Order:
        d = dict(d)
        d["direction"] = OrderDirection(d["direction"])
        d["order_type"] = OrderType(d["order_type"])
        d["status"] = OrderStatus(d["status"])
        return cls(**d)


# ---------------------------------------------------------------------------
# Fill (execution record)
# ---------------------------------------------------------------------------

@dataclass
class Fill:
    """A single trade execution (one fill per market order)."""

    order_id: str
    instrument: str
    direction: OrderDirection
    quantity: int
    price: float
    commission: float
    stamp_tax: float
    transfer_fee: float
    timestamp: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @property
    def total_fee(self) -> float:
        return self.commission + self.stamp_tax + self.transfer_fee

    @property
    def net_amount(self) -> float:
        """Cash impact: positive = cash inflow (sell), negative = cash outflow (buy)."""
        gross = self.quantity * self.price
        if self.direction == OrderDirection.SELL:
            return gross - self.total_fee
        return -(gross + self.total_fee)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["direction"] = self.direction.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Fill:
        d = dict(d)
        d["direction"] = OrderDirection(d["direction"])
        return cls(**d)


# ---------------------------------------------------------------------------
# Position
# ---------------------------------------------------------------------------

@dataclass
class Position:
    """A holding in a single instrument."""

    instrument: str
    quantity: int = 0
    frozen_quantity: int = 0
    avg_cost: float = 0.0
    total_cost: float = 0.0
    realized_pnl: float = 0.0
    current_price: float = 0.0

    @property
    def available_quantity(self) -> int:
        """Shares sellable today (excludes T+1 frozen)."""
        return max(0, self.quantity - self.frozen_quantity)

    @property
    def market_value(self) -> float:
        return self.quantity * self.current_price

    @property
    def unrealized_pnl(self) -> float:
        return self.market_value - self.total_cost

    @property
    def pnl_pct(self) -> float:
        if self.total_cost <= 0:
            return 0.0
        return self.unrealized_pnl / self.total_cost

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Position:
        return cls(**d)


# ---------------------------------------------------------------------------
# Account
# ---------------------------------------------------------------------------

@dataclass
class Account:
    """Full account state: cash + positions."""

    initial_capital: float
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)
    total_commission: float = 0.0
    total_stamp_tax: float = 0.0
    total_transfer_fee: float = 0.0
    realized_pnl: float = 0.0
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @property
    def active_positions(self) -> dict[str, Position]:
        return {k: v for k, v in self.positions.items() if v.quantity > 0}

    @property
    def market_value(self) -> float:
        return sum(p.market_value for p in self.positions.values() if p.quantity > 0)

    @property
    def equity(self) -> float:
        return self.cash + self.market_value

    @property
    def total_pnl(self) -> float:
        return self.equity - self.initial_capital

    @property
    def return_pct(self) -> float:
        if self.initial_capital <= 0:
            return 0.0
        return self.total_pnl / self.initial_capital

    @property
    def total_fees(self) -> float:
        return self.total_commission + self.total_stamp_tax + self.total_transfer_fee

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["positions"] = {k: v.to_dict() for k, v in self.positions.items()}
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Account:
        d = dict(d)
        raw_positions = d.pop("positions", {})
        positions = {k: Position.from_dict(v) for k, v in raw_positions.items()}
        return cls(positions=positions, **d)


# ---------------------------------------------------------------------------
# NAV Snapshot
# ---------------------------------------------------------------------------

@dataclass
class NavSnapshot:
    """Daily portfolio valuation record."""

    date: str
    equity: float
    cash: float
    market_value: float
    positions_count: int
    realized_pnl: float
    unrealized_pnl: float
    total_fees: float
    daily_return: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> NavSnapshot:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
