"""
Paper (simulated) broker for A-share trading.

Implements AbstractBroker with:
- Immediate fill for MARKET orders at current_price.
- LIMIT orders fill if executable, otherwise rejected.
- T+1: bought shares are frozen until next settle_day.
- Full fee model (commission, stamp tax, transfer fee).
- JSON-based persistence (atomic write).
- Append-only CSV trade log + NAV history.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from .. import rules
from ..models import (
    Account,
    Fill,
    NavSnapshot,
    Order,
    OrderDirection,
    OrderStatus,
    OrderType,
    Position,
)
from .base import AbstractBroker

_DEFAULT_DIR = Path("./output/paper")


class PaperBroker(AbstractBroker):
    """Simulated A-share broker backed by local JSON state."""

    def __init__(self, state_dir: str | Path = _DEFAULT_DIR) -> None:
        self._dir = Path(state_dir)
        self._state_path = self._dir / "paper_state.json"
        self._trades_csv = self._dir / "paper_trades.csv"
        self._nav_csv = self._dir / "paper_nav.csv"

        self._account: Account | None = None
        self._orders: list[Order] = []
        self._fills: list[Fill] = []
        self._nav_history: list[NavSnapshot] = []
        self._prev_closes: dict[str, float] = {}

        if self._state_path.exists():
            self._load()

    # -- Public factory ------------------------------------------------------

    @classmethod
    def init_account(
        cls,
        capital: float,
        state_dir: str | Path = _DEFAULT_DIR,
    ) -> "PaperBroker":
        """Create a brand-new paper trading account."""
        broker = cls(state_dir=state_dir)
        broker._account = Account(initial_capital=capital, cash=capital)
        broker._orders = []
        broker._fills = []
        broker._nav_history = []
        broker._prev_closes = {}
        broker._save()
        logger.info(f"Paper account initialized: ¥{capital:,.0f} at {broker._state_path}")
        return broker

    @property
    def is_initialized(self) -> bool:
        return self._account is not None

    # -- AbstractBroker implementation ---------------------------------------

    def submit_order(self, order: Order) -> Order:
        self._ensure_initialized()
        account = self._account

        # Resolve market price (independent of order type) for validation
        market_price = self._get_market_price(order.instrument)
        if market_price is None or market_price <= 0:
            order.status = OrderStatus.REJECTED
            order.reject_reason = f"无法确定市场价格: {order.instrument}"
            order.updated_at = _now()
            self._orders.append(order)
            self._save()
            return order

        # Validate via rules
        vr = rules.validate_order(
            order, account,
            prev_closes=self._prev_closes or None,
            market_price=market_price,
        )
        if not vr.ok:
            order.status = OrderStatus.REJECTED
            order.reject_reason = vr.reason
            order.updated_at = _now()
            self._orders.append(order)
            self._save()
            return order

        # Determine execution price
        if order.order_type == OrderType.LIMIT:
            if order.limit_price is None or order.limit_price <= 0:
                return self._reject(order, "限价单缺少有效限价")
            if order.direction == OrderDirection.BUY and order.limit_price < market_price:
                return self._reject(
                    order,
                    f"限价 {order.limit_price} 低于当前价 {market_price}, "
                    "手动模式不支持挂单",
                )
            if order.direction == OrderDirection.SELL and order.limit_price > market_price:
                return self._reject(
                    order,
                    f"限价 {order.limit_price} 高于当前价 {market_price}, "
                    "手动模式不支持挂单",
                )
            exec_price = order.limit_price
        else:
            exec_price = market_price

        # Execute fill
        fees = rules.calculate_fees(order.direction, order.quantity, exec_price)
        fill = Fill(
            order_id=order.id,
            instrument=order.instrument,
            direction=order.direction,
            quantity=order.quantity,
            price=exec_price,
            commission=fees.commission,
            stamp_tax=fees.stamp_tax,
            transfer_fee=fees.transfer_fee,
        )

        # Update account
        self._apply_fill(fill)

        # Update order
        order.status = OrderStatus.FILLED
        order.filled_quantity = order.quantity
        order.avg_fill_price = exec_price
        order.commission = fees.commission
        order.stamp_tax = fees.stamp_tax
        order.transfer_fee = fees.transfer_fee
        order.updated_at = _now()

        self._orders.append(order)
        self._fills.append(fill)
        self._append_trade_csv(fill)
        self._save()

        logger.info(
            f"Paper {order.direction.value} {order.instrument}: "
            f"{order.quantity} @ {exec_price:.4f}, "
            f"fees ¥{fees.total:.2f}, cash ¥{account.cash:,.0f}"
        )
        return order

    def cancel_order(self, order_id: str) -> bool:
        for o in self._orders:
            if o.id == order_id and o.status == OrderStatus.PENDING:
                o.status = OrderStatus.CANCELLED
                o.updated_at = _now()
                self._save()
                return True
        return False

    def get_account(self) -> Account:
        self._ensure_initialized()
        return self._account

    def get_positions(self) -> dict[str, Position]:
        self._ensure_initialized()
        return self._account.active_positions

    def get_order(self, order_id: str) -> Order | None:
        """Look up a single order by ID."""
        for o in self._orders:
            if o.id == order_id:
                return o
        return None

    def get_orders(self, status: OrderStatus | None = None) -> list[Order]:
        if status is None:
            return list(self._orders)
        return [o for o in self._orders if o.status == status]

    def get_fills(self, order_id: str | None = None) -> list[Fill]:
        if order_id is None:
            return list(self._fills)
        return [f for f in self._fills if f.order_id == order_id]

    def update_prices(self, prices: dict[str, float]) -> None:
        self._ensure_initialized()
        for inst, price in prices.items():
            pos = self._account.positions.get(inst)
            if pos:
                pos.current_price = price

    def settle_day(self, date: str) -> NavSnapshot:
        self._ensure_initialized()
        account = self._account

        # 1. Unfreeze all T+1
        for pos in account.positions.values():
            pos.frozen_quantity = 0

        # 2. Store prev closes
        self._prev_closes = {
            inst: pos.current_price
            for inst, pos in account.positions.items()
            if pos.quantity > 0 and pos.current_price > 0
        }

        # 3. Compute daily return
        prev_equity = self._nav_history[-1].equity if self._nav_history else account.initial_capital
        daily_ret = (account.equity - prev_equity) / prev_equity if prev_equity > 0 else 0.0

        # 4. Record NAV snapshot
        snap = NavSnapshot(
            date=date,
            equity=round(account.equity, 2),
            cash=round(account.cash, 2),
            market_value=round(account.market_value, 2),
            positions_count=len(account.active_positions),
            realized_pnl=round(account.realized_pnl, 2),
            unrealized_pnl=round(account.market_value - sum(
                p.total_cost for p in account.positions.values() if p.quantity > 0
            ), 2),
            total_fees=round(account.total_fees, 2),
            daily_return=round(daily_ret, 6),
        )
        self._nav_history.append(snap)
        self._append_nav_csv(snap)
        self._save()

        logger.info(
            f"Paper settle {date}: equity ¥{snap.equity:,.0f}, "
            f"return {snap.daily_return:+.2%}, "
            f"positions {snap.positions_count}"
        )
        return snap

    def reset(self) -> None:
        self._ensure_initialized()
        capital = self._account.initial_capital
        self._account = Account(initial_capital=capital, cash=capital)
        self._orders = []
        self._fills = []
        self._nav_history = []
        self._prev_closes = {}
        # Remove CSV files so they start fresh
        self._trades_csv.unlink(missing_ok=True)
        self._nav_csv.unlink(missing_ok=True)
        self._save()
        logger.info(f"Paper account reset to ¥{capital:,.0f}")

    def get_nav_history(self) -> list[NavSnapshot]:
        return list(self._nav_history)

    # -- Internal: fill application ------------------------------------------

    def _apply_fill(self, fill: Fill) -> None:
        account = self._account
        inst = fill.instrument

        if inst not in account.positions:
            account.positions[inst] = Position(instrument=inst)
        pos = account.positions[inst]

        if fill.direction == OrderDirection.BUY:
            cost = fill.quantity * fill.price
            pos.total_cost += cost
            pos.quantity += fill.quantity
            pos.avg_cost = pos.total_cost / pos.quantity if pos.quantity > 0 else 0
            pos.frozen_quantity += fill.quantity  # T+1 freeze
            pos.current_price = fill.price
            account.cash -= cost + fill.total_fee

        elif fill.direction == OrderDirection.SELL:
            actual_qty = min(fill.quantity, pos.quantity)
            if actual_qty <= 0:
                logger.warning(f"Sell fill for {inst} but no position to reduce")
                return

            realized = (fill.price - pos.avg_cost) * actual_qty - fill.total_fee
            account.realized_pnl += realized
            pos.realized_pnl += realized

            if pos.quantity > 0:
                sell_ratio = actual_qty / pos.quantity
                pos.total_cost *= (1 - sell_ratio)
            pos.quantity -= actual_qty
            if pos.quantity > 0:
                pos.avg_cost = pos.total_cost / pos.quantity
            else:
                pos.avg_cost = 0
                pos.total_cost = 0

            pos.current_price = fill.price
            account.cash += actual_qty * fill.price - fill.total_fee

        account.total_commission += fill.commission
        account.total_stamp_tax += fill.stamp_tax
        account.total_transfer_fee += fill.transfer_fee

    def _get_market_price(self, instrument: str) -> float | None:
        """Return latest known market price for an instrument.

        Checks position's current_price first, then prev_close.
        Independent of order type — this is the reference price for
        MARKET fills and LIMIT executability checks.
        """
        pos = self._account.positions.get(instrument)
        if pos and pos.current_price > 0:
            return pos.current_price
        return self._prev_closes.get(instrument)

    def _reject(self, order: Order, reason: str) -> Order:
        """Mark order as rejected, persist, and return it."""
        order.status = OrderStatus.REJECTED
        order.reject_reason = reason
        order.updated_at = _now()
        self._orders.append(order)
        self._save()
        return order

    # -- Persistence ---------------------------------------------------------

    def _ensure_initialized(self) -> None:
        if self._account is None:
            raise RuntimeError(
                "Paper account not initialized. Run: python main.py paper init --capital <amount>"
            )

    def _save(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "account": self._account.to_dict(),
            "orders": [o.to_dict() for o in self._orders],
            "fills": [f.to_dict() for f in self._fills],
            "nav_history": [n.to_dict() for n in self._nav_history],
            "prev_closes": self._prev_closes,
        }
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        tmp.replace(self._state_path)

    def _load(self) -> None:
        raw = json.loads(self._state_path.read_text())
        self._account = Account.from_dict(raw["account"])
        self._orders = [Order.from_dict(o) for o in raw.get("orders", [])]
        self._fills = [Fill.from_dict(f) for f in raw.get("fills", [])]
        self._nav_history = [NavSnapshot.from_dict(n) for n in raw.get("nav_history", [])]
        self._prev_closes = raw.get("prev_closes", {})

    def _append_trade_csv(self, fill: Fill) -> None:
        """Append a fill record to the human-readable CSV trade log."""
        self._dir.mkdir(parents=True, exist_ok=True)
        write_header = not self._trades_csv.exists()
        fields = [
            "timestamp", "instrument", "direction", "quantity", "price",
            "commission", "stamp_tax", "transfer_fee", "order_id",
        ]
        with open(self._trades_csv, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if write_header:
                writer.writeheader()
            writer.writerow({
                "timestamp": fill.timestamp,
                "instrument": fill.instrument,
                "direction": fill.direction.value,
                "quantity": fill.quantity,
                "price": round(fill.price, 4),
                "commission": round(fill.commission, 2),
                "stamp_tax": round(fill.stamp_tax, 2),
                "transfer_fee": round(fill.transfer_fee, 2),
                "order_id": fill.order_id,
            })

    def _append_nav_csv(self, snap: NavSnapshot) -> None:
        """Append a NAV snapshot to the CSV history."""
        self._dir.mkdir(parents=True, exist_ok=True)
        write_header = not self._nav_csv.exists()
        fields = [
            "date", "equity", "cash", "market_value", "positions_count",
            "realized_pnl", "unrealized_pnl", "total_fees", "daily_return",
        ]
        with open(self._nav_csv, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if write_header:
                writer.writeheader()
            writer.writerow(snap.to_dict())


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
