from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from stopat30m.trading import rules
from stopat30m.trading.broker.base import AbstractBroker
from stopat30m.trading.models import (
    Account,
    Fill,
    NavSnapshot,
    Order,
    OrderDirection,
    OrderStatus,
    OrderType,
    Position,
)


@dataclass
class MarketSnapshot:
    date: str
    execution_prices: dict[str, float]
    close_prices: dict[str, float]
    high_prices: dict[str, float]
    low_prices: dict[str, float]
    volumes: dict[str, float]
    prev_closes: dict[str, float]
    paused: dict[str, bool]
    buy_blocked: dict[str, bool]
    sell_blocked: dict[str, bool]


class HistoricalBroker(AbstractBroker):
    """In-memory broker driven by historical daily market snapshots."""

    def __init__(
        self,
        initial_capital: float,
        slippage_bps: float = 0.0,
        allow_partial_fill: bool = False,
        participation_rate: float = 0.1,
    ) -> None:
        self._initial_capital = initial_capital
        self._account = Account(initial_capital=initial_capital, cash=initial_capital)
        self._orders: list[Order] = []
        self._fills: list[Fill] = []
        self._nav_history: list[NavSnapshot] = []
        self._market: MarketSnapshot | None = None
        self._slippage_bps = max(0.0, slippage_bps)
        self._allow_partial_fill = allow_partial_fill
        self._participation_rate = max(0.0, min(participation_rate, 1.0))

    def set_market_snapshot(
        self,
        date: str,
        execution_prices: dict[str, float],
        close_prices: dict[str, float],
        high_prices: dict[str, float],
        low_prices: dict[str, float],
        volumes: dict[str, float],
        prev_closes: dict[str, float],
        paused: dict[str, bool] | None = None,
        buy_blocked: dict[str, bool] | None = None,
        sell_blocked: dict[str, bool] | None = None,
    ) -> None:
        self._market = MarketSnapshot(
            date=date,
            execution_prices=execution_prices,
            close_prices=close_prices,
            high_prices=high_prices,
            low_prices=low_prices,
            volumes=volumes,
            prev_closes=prev_closes,
            paused=paused or {},
            buy_blocked=buy_blocked or {},
            sell_blocked=sell_blocked or {},
        )
        self.update_prices(close_prices)

    def submit_order(self, order: Order) -> Order:
        if self._market is None:
            raise RuntimeError("Market snapshot is not set.")

        order.created_at = f"{self._market.date} 09:30:00"
        order.updated_at = order.created_at
        market_price = self._market.execution_prices.get(order.instrument, 0.0)
        if market_price <= 0:
            return self._reject(order, "无法确定历史成交价格")
        if self._market.paused.get(order.instrument, False):
            return self._reject(order, "停牌或无有效成交量")

        if order.direction == OrderDirection.BUY and self._market.buy_blocked.get(order.instrument, False):
            return self._reject(order, "涨停或无成交能力，买入受阻")
        if order.direction == OrderDirection.SELL and self._market.sell_blocked.get(order.instrument, False):
            return self._reject(order, "跌停或无成交能力，卖出受阻")

        executable_qty = self._resolve_fill_quantity(order.instrument, order.quantity)
        if executable_qty <= 0:
            return self._reject(order, "超过当日可成交量限制")
        if executable_qty < order.quantity and not self._allow_partial_fill:
            return self._reject(order, f"可成交 {executable_qty} 股，小于委托数量 {order.quantity}")

        check_order = Order(
            instrument=order.instrument,
            direction=order.direction,
            order_type=order.order_type,
            quantity=executable_qty if executable_qty < order.quantity else order.quantity,
            limit_price=order.limit_price,
            note=order.note,
            id=order.id,
            created_at=order.created_at,
            updated_at=order.updated_at,
        )
        vr = rules.validate_order(
            check_order,
            self._account,
            prev_closes=self._market.prev_closes or None,
            market_price=market_price,
        )
        if not vr.ok:
            return self._reject(order, vr.reason)

        exec_price = self._resolve_execution_price(order, market_price)
        if exec_price is None or exec_price <= 0:
            return self._reject(order, "限价单当日未触发成交")

        fill_qty = min(order.quantity, executable_qty)
        fees = rules.calculate_fees(order.direction, fill_qty, exec_price)
        fill = Fill(
            order_id=order.id,
            instrument=order.instrument,
            direction=order.direction,
            quantity=fill_qty,
            price=exec_price,
            commission=fees.commission,
            stamp_tax=fees.stamp_tax,
            transfer_fee=fees.transfer_fee,
            timestamp=f"{self._market.date} 09:30:00",
        )

        self._apply_fill(fill)

        order.status = OrderStatus.PARTIALLY_FILLED if fill_qty < order.quantity else OrderStatus.FILLED
        order.filled_quantity = fill_qty
        order.avg_fill_price = exec_price
        order.commission = fees.commission
        order.stamp_tax = fees.stamp_tax
        order.transfer_fee = fees.transfer_fee
        order.updated_at = fill.timestamp
        if fill_qty < order.quantity:
            order.reject_reason = f"部分成交: 请求 {order.quantity}，实际 {fill_qty}"

        self._orders.append(order)
        self._fills.append(fill)
        return order

    def cancel_order(self, order_id: str) -> bool:
        for order in self._orders:
            if order.id == order_id and order.status == OrderStatus.PENDING:
                order.status = OrderStatus.CANCELLED
                return True
        return False

    def get_account(self) -> Account:
        return self._account

    def get_positions(self) -> dict[str, Position]:
        return self._account.active_positions

    def get_orders(self, status: OrderStatus | None = None) -> list[Order]:
        if status is None:
            return list(self._orders)
        return [o for o in self._orders if o.status == status]

    def get_fills(self) -> list[Fill]:
        return list(self._fills)

    def update_prices(self, prices: dict[str, float]) -> None:
        for inst, price in prices.items():
            pos = self._account.positions.get(inst)
            if pos is not None:
                pos.current_price = price

    def settle_day(self, date: str) -> NavSnapshot:
        for pos in self._account.positions.values():
            pos.frozen_quantity = 0

        prev_equity = self._nav_history[-1].equity if self._nav_history else self._account.initial_capital
        daily_ret = (self._account.equity - prev_equity) / prev_equity if prev_equity > 0 else 0.0
        snap = NavSnapshot(
            date=date,
            equity=round(self._account.equity, 2),
            cash=round(self._account.cash, 2),
            market_value=round(self._account.market_value, 2),
            positions_count=len(self._account.active_positions),
            realized_pnl=round(self._account.realized_pnl, 2),
            unrealized_pnl=round(
                self._account.market_value
                - sum(p.total_cost for p in self._account.positions.values() if p.quantity > 0),
                2,
            ),
            total_fees=round(self._account.total_fees, 2),
            daily_return=round(daily_ret, 6),
        )
        self._nav_history.append(snap)
        return snap

    def reset(self) -> None:
        self._account = Account(initial_capital=self._initial_capital, cash=self._initial_capital)
        self._orders = []
        self._fills = []
        self._nav_history = []
        self._market = None

    def get_nav_history(self) -> list[NavSnapshot]:
        return list(self._nav_history)

    def _reject(self, order: Order, reason: str) -> Order:
        order.status = OrderStatus.REJECTED
        order.reject_reason = reason
        self._orders.append(order)
        return order

    def _resolve_fill_quantity(self, instrument: str, requested_qty: int) -> int:
        if self._market is None:
            return 0
        volume = float(self._market.volumes.get(instrument, 0.0))
        if volume <= 0:
            return 0
        max_qty = rules.round_lot(volume * self._participation_rate)
        if max_qty <= 0:
            return 0
        return min(requested_qty, max_qty)

    def _resolve_execution_price(self, order: Order, market_price: float) -> float | None:
        if self._market is None:
            return None
        if order.order_type == OrderType.MARKET:
            return self._apply_slippage(order.instrument, order.direction, market_price)

        limit_price = order.limit_price
        if limit_price is None or limit_price <= 0:
            return None

        day_low = self._market.low_prices.get(order.instrument, market_price)
        day_high = self._market.high_prices.get(order.instrument, market_price)

        if order.direction == OrderDirection.BUY:
            if day_low > limit_price:
                return None
            reference = min(market_price, limit_price)
        else:
            if day_high < limit_price:
                return None
            reference = max(market_price, limit_price)
        return reference

    def _apply_slippage(self, instrument: str, direction: OrderDirection, base_price: float) -> float:
        if base_price <= 0:
            return base_price
        slipped = base_price * (1 + self._slippage_bps / 10000.0) if direction == OrderDirection.BUY else (
            base_price * (1 - self._slippage_bps / 10000.0)
        )
        prev_close = self._market.prev_closes.get(instrument, 0.0) if self._market else 0.0
        if prev_close > 0:
            lower, upper = rules.price_limit_range(prev_close, instrument)
            slipped = min(max(slipped, lower), upper)
        return round(slipped, 4)

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
            pos.avg_cost = pos.total_cost / pos.quantity if pos.quantity > 0 else 0.0
            pos.frozen_quantity += fill.quantity
            pos.current_price = fill.price
            account.cash -= cost + fill.total_fee
        else:
            actual_qty = min(fill.quantity, pos.quantity)
            if actual_qty <= 0:
                logger.warning(f"Historical sell fill ignored for {inst}: no position")
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
                pos.avg_cost = 0.0
                pos.total_cost = 0.0

            pos.current_price = fill.price
            account.cash += actual_qty * fill.price - fill.total_fee

        account.total_commission += fill.commission
        account.total_stamp_tax += fill.stamp_tax
        account.total_transfer_fee += fill.transfer_fee
