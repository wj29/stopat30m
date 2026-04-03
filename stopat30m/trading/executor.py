"""
Bridge between RebalancePlan and broker order execution.

Converts a plan's trade rows into Order objects, submits them through
the broker (sells first to free cash, then buys), and returns results.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loguru import logger

from .broker.base import AbstractBroker
from .models import Order, OrderDirection, OrderStatus, OrderType
from .rebalancer import RebalancePlan
from .risk import RiskManager


@dataclass
class ExecutionReport:
    """Summary of a plan execution attempt."""

    filled: list[Order] = field(default_factory=list)
    rejected: list[Order] = field(default_factory=list)

    @property
    def total_filled(self) -> int:
        return len(self.filled)

    @property
    def total_rejected(self) -> int:
        return len(self.rejected)

    @property
    def sell_count(self) -> int:
        return sum(1 for o in self.filled if o.direction == OrderDirection.SELL)

    @property
    def buy_count(self) -> int:
        return sum(1 for o in self.filled if o.direction == OrderDirection.BUY)

    @property
    def total_commission(self) -> float:
        return sum(o.commission for o in self.filled)

    @property
    def total_stamp_tax(self) -> float:
        return sum(o.stamp_tax for o in self.filled)

    @property
    def total_turnover(self) -> float:
        return sum(o.turnover for o in self.filled)


def execute_plan(
    plan: RebalancePlan,
    broker: AbstractBroker,
    sells_only: bool = False,
    risk_manager: RiskManager | None = None,
    order_type: OrderType = OrderType.MARKET,
    trade_date: str | None = None,
) -> ExecutionReport:
    """Convert a RebalancePlan into broker orders and execute them.

    Execution order: sells first (to free cash), then buys.
    Error rows (direction == "-") in the plan are skipped.

    Args:
        plan: The rebalance plan from compute_rebalance_plan().
        broker: Any AbstractBroker implementation.
        sells_only: If True, only execute sell orders.

    Returns:
        ExecutionReport with filled and rejected orders.
    """
    report = ExecutionReport()

    if plan.trades.empty:
        return report

    # Push plan prices into broker so MARKET orders can resolve
    plan_prices = _extract_prices(plan.trades)
    if plan_prices:
        broker.update_prices(plan_prices)

    sells = plan.sells
    buys = plan.buys if not sells_only else sells.iloc[0:0]  # empty DF

    for _, row in sells.iterrows():
        order = _row_to_order(row, order_type=order_type)
        if order is None:
            continue
        result = broker.submit_order(order)
        _classify(result, report)

    for _, row in buys.iterrows():
        order = _row_to_order(row, order_type=order_type)
        if order is None:
            continue
        if risk_manager is not None:
            pos_values = {
                inst: p.market_value
                for inst, p in broker.get_positions().items()
            }
            approved, reason = risk_manager.check_order(
                symbol=order.instrument,
                direction=order.direction.value,
                volume=order.quantity,
                price=float(row.get("price", 0)),
                current_equity=broker.get_account().equity,
                current_positions=pos_values,
                current_date=trade_date,
            )
            if not approved:
                order.status = OrderStatus.REJECTED
                order.reject_reason = f"风控拒绝: {reason}"
                report.rejected.append(order)
                logger.warning(f"Risk rejected {order.instrument}: {reason}")
                continue
        result = broker.submit_order(order)
        _classify(result, report)

    logger.info(
        f"Plan executed: {report.total_filled} filled "
        f"({report.sell_count}S/{report.buy_count}B), "
        f"{report.total_rejected} rejected, "
        f"turnover ¥{report.total_turnover:,.0f}"
    )

    return report


def _classify(order: Order, report: ExecutionReport) -> None:
    if order.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
        report.filled.append(order)
    else:
        report.rejected.append(order)


def _extract_prices(trades_df) -> dict[str, float]:
    """Build {instrument: price} from plan trades for broker price injection."""
    prices: dict[str, float] = {}
    for _, row in trades_df.iterrows():
        inst = str(row.get("instrument", "")).strip().upper()
        price = float(row.get("price", 0))
        if inst and price > 0:
            prices[inst] = price
    return prices


def _row_to_order(row, order_type: OrderType = OrderType.MARKET) -> Order | None:
    """Convert a plan DataFrame row to an Order."""
    direction_str = str(row.get("direction", "")).upper()
    if direction_str not in ("BUY", "SELL"):
        return None

    instrument = str(row["instrument"]).strip().upper()
    quantity = int(row["quantity"])
    price = float(row.get("price", 0))

    if quantity <= 0:
        return None

    return Order(
        instrument=instrument,
        direction=OrderDirection(direction_str),
        order_type=order_type,
        quantity=quantity,
        limit_price=price if order_type == OrderType.LIMIT and price > 0 else None,
        note=str(row.get("reason", "")),
    )
