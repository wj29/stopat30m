"""
Paper (simulated) broker for A-share trading.

Implements AbstractBroker with:
- Immediate fill for MARKET orders at current_price.
- LIMIT orders fill if executable, otherwise rejected.
- T+1: bought shares are frozen until next settle_day.
- Full fee model (commission, stamp tax, transfer fee).
- JSON-based persistence (atomic write).
- Rebuildable CSV trade log + NAV history (JSON is authoritative).
"""

from __future__ import annotations

import csv
import json
import math
import os
from contextlib import contextmanager
from datetime import date, datetime
from functools import wraps
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

if os.name == "nt":
    import msvcrt
else:
    import fcntl

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
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _serialized(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        with self._mutation():
            return method(self, *args, **kwargs)
    return guarded


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
        self._prices: dict[str, float] = {}
        self._price_timestamps: dict[str, str] = {}
        self._frozen_lots: dict[str, dict[str, int]] = {}

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
        if not math.isfinite(capital) or capital <= 0:
            raise ValueError("初始本金必须是正有限数字")
        broker = cls(state_dir=state_dir)
        with broker._mutation():
            if broker.is_initialized:
                raise ValueError("模拟账户已存在；初始化不能覆盖已有账本")
            broker._account = Account(initial_capital=capital, cash=capital)
            broker._orders = []
            broker._fills = []
            broker._nav_history = []
            broker._prev_closes = {}
            broker._prices = {}
            broker._price_timestamps = {}
            broker._frozen_lots = {}
            broker._save()
        logger.info(f"Paper account initialized: ¥{capital:,.0f} at {broker._state_path}")
        return broker

    @property
    def is_initialized(self) -> bool:
        return self._account is not None

    # -- AbstractBroker implementation ---------------------------------------

    @_serialized
    def submit_order(self, order: Order) -> Order:
        self._ensure_initialized()
        account = self._account
        existing = self.get_order(order.id)
        if existing is not None:
            fields = ("instrument", "direction", "order_type", "quantity", "limit_price")
            if any(getattr(existing, key) != getattr(order, key) for key in fields):
                raise ValueError("委托标识已被另一笔委托使用")
            return existing
        if order.status != OrderStatus.PENDING:
            raise ValueError("只能提交尚未处理的委托")
        self._refresh_frozen(_today())

        # Resolve market price (independent of order type) for validation
        market_price = self._get_market_price(order.instrument)
        if market_price is None or not math.isfinite(market_price) or market_price <= 0:
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
            # An executable limit is a cap/floor, not a worse execution price.
            exec_price = market_price
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
            timestamp=_now(),
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
        self._save()

        logger.info(
            f"Paper {order.direction.value} {order.instrument}: "
            f"{order.quantity} @ {exec_price:.4f}, "
            f"fees ¥{fees.total:.2f}, cash ¥{account.cash:,.0f}"
        )
        return order

    @_serialized
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
        self._refresh_frozen(_today())
        return self._account

    def get_positions(self) -> dict[str, Position]:
        self._ensure_initialized()
        self._refresh_frozen(_today())
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

    @_serialized
    def update_prices(self, prices: dict[str, float]) -> None:
        """Explicit synthetic prices for simulations; online paths use update_quotes."""
        self._ensure_initialized()
        for inst, price in prices.items():
            if not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
                continue
            self._prices[inst] = price
            self._price_timestamps[inst] = _now()
            pos = self._account.positions.get(inst)
            if pos:
                pos.current_price = price
        self._save()

    @_serialized
    def update_quotes(self, quotes: dict[str, dict], instruments: list[str] | None = None) -> dict[str, float]:
        """Accept source-timed live quotes, invalidating missing execution prices.

        ``as_of`` remains the provider's timestamp. A recent fetch/plan time
        cannot make an old source price fresh enough for a simulated order.
        """
        self._ensure_initialized()
        accepted: dict[str, float] = {}
        for inst in (instruments if instruments is not None else quotes):
            self._prices.pop(inst, None)
            self._price_timestamps.pop(inst, None)
            quote = quotes.get(inst) or {}
            price, timestamp = quote.get("price"), quote.get("as_of")
            if (quote.get("is_realtime") is not True or quote.get("status") in {"stale", "unavailable"}
                    or not isinstance(price, (int, float)) or isinstance(price, bool)
                    or not math.isfinite(price) or price <= 0 or not isinstance(timestamp, str)):
                continue
            try:
                source_time = datetime.fromisoformat(timestamp)
                if source_time.tzinfo is None:
                    continue
            except ValueError:
                continue
            self._prices[inst] = float(price)
            self._price_timestamps[inst] = timestamp
            if self._get_market_price(inst) is None:
                self._prices.pop(inst, None)
                self._price_timestamps.pop(inst, None)
                continue
            accepted[inst] = float(price)
            pos = self._account.positions.get(inst)
            if pos:
                pos.current_price = float(price)
        self._save()
        return accepted

    @_serialized
    def reject_order(self, order: Order, reason: str) -> Order:
        """Persist a pre-execution rejection without falling back to old prices."""
        self._ensure_initialized()
        existing = self.get_order(order.id)
        if existing is not None:
            return existing
        return self._reject(order, reason)

    @_serialized
    def settle_day(self, date: str) -> NavSnapshot:
        self._ensure_initialized()
        account = self._account
        try:
            settle_date = datetime.strptime(date, "%Y-%m-%d").date()
            if date != settle_date.isoformat():
                raise ValueError("noncanonical date")
        except (ValueError, TypeError):
            raise ValueError("结算日期格式应为 YYYY-MM-DD") from None
        if settle_date > _today():
            raise ValueError("不能通过未来日期提前解冻持仓")
        if self._nav_history:
            last_date = self._nav_history[-1].date
            if date < last_date:
                raise ValueError("不能回退结算日期")
        same_day = bool(self._nav_history and date == self._nav_history[-1].date)
        latest_fill = max((fill.timestamp[:10] for fill in self._fills), default="")
        if latest_fill and date < latest_fill:
            raise ValueError("结算日期不能早于最新成交日期")

        # A same-day close does not make today's purchases sellable.
        self._refresh_frozen(settle_date)

        # 2. Store prev closes
        self._prev_closes = {
            inst: pos.current_price
            for inst, pos in account.positions.items()
            if pos.quantity > 0 and pos.current_price > 0
        }

        # 3. Compute daily return
        earlier = self._nav_history[:-1] if same_day else self._nav_history
        prev_equity = earlier[-1].equity if earlier else account.initial_capital
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
        if same_day:
            self._nav_history[-1] = snap
        else:
            self._nav_history.append(snap)
        self._save()

        logger.info(
            f"Paper settle {date}: equity ¥{snap.equity:,.0f}, "
            f"return {snap.daily_return:+.2%}, "
            f"positions {snap.positions_count}"
        )
        return snap

    @_serialized
    def reset(self) -> None:
        self._ensure_initialized()
        capital = self._account.initial_capital
        self._account = Account(initial_capital=capital, cash=capital)
        self._orders = []
        self._fills = []
        self._nav_history = []
        self._prev_closes = {}
        self._prices = {}
        self._price_timestamps = {}
        self._frozen_lots = {}
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
            pos.total_cost = round(pos.total_cost + cost + fill.total_fee, 2)
            pos.quantity += fill.quantity
            pos.avg_cost = pos.total_cost / pos.quantity if pos.quantity > 0 else 0
            pos.frozen_quantity += fill.quantity  # T+1 freeze
            lots = self._frozen_lots.setdefault(inst, {})
            day = fill.timestamp[:10]
            lots[day] = lots.get(day, 0) + fill.quantity
            pos.current_price = fill.price
            account.cash = round(account.cash - cost - fill.total_fee, 2)

        elif fill.direction == OrderDirection.SELL:
            actual_qty = min(fill.quantity, pos.quantity)
            if actual_qty <= 0:
                logger.warning(f"Sell fill for {inst} but no position to reduce")
                return

            removed_cost = pos.total_cost if actual_qty == pos.quantity else round(pos.total_cost * actual_qty / pos.quantity, 2)
            realized = round(fill.price * actual_qty - removed_cost - fill.total_fee, 2)
            account.realized_pnl = round(account.realized_pnl + realized, 2)
            pos.realized_pnl = round(pos.realized_pnl + realized, 2)

            if pos.quantity > 0:
                pos.total_cost = round(pos.total_cost - removed_cost, 2)
            pos.quantity -= actual_qty
            if pos.quantity > 0:
                pos.avg_cost = pos.total_cost / pos.quantity
            else:
                pos.avg_cost = 0
                pos.total_cost = 0

            pos.current_price = fill.price
            account.cash = round(account.cash + actual_qty * fill.price - fill.total_fee, 2)

        account.total_commission += fill.commission
        account.total_stamp_tax += fill.stamp_tax
        account.total_transfer_fee += fill.transfer_fee

    def _get_market_price(self, instrument: str) -> float | None:
        """Return latest known market price for an instrument.

        Only recent explicitly supplied quotes authorize a simulated fill.
        Valuation marks and previous closes are not execution quotes.
        """
        # Persisted marks/previous closes value holdings, but cannot authorize
        # a new execution. Require an explicitly supplied recent market quote.
        timestamp = self._price_timestamps.get(instrument)
        if timestamp is None:
            return None
        try:
            current = datetime.fromisoformat(_now())
            received = datetime.fromisoformat(timestamp)
            if current.tzinfo is None:
                current = current.replace(tzinfo=_SHANGHAI)
            if received.tzinfo is None:
                # update_prices is explicitly a synthetic, exchange-time input.
                received = received.replace(tzinfo=_SHANGHAI)
            age = (current - received).total_seconds()
        except (ValueError, TypeError):
            return None
        if not 0 <= age <= 120:
            return None
        return self._prices.get(instrument)

    def _reject(self, order: Order, reason: str) -> Order:
        """Mark order as rejected, persist, and return it."""
        order.status = OrderStatus.REJECTED
        order.reject_reason = reason
        order.updated_at = _now()
        self._orders.append(order)
        self._save()
        return order

    # -- Persistence ---------------------------------------------------------

    @contextmanager
    def _mutation(self):
        """Reload under an OS lock so separate UI/CLI instances cannot overwrite."""
        self._dir.mkdir(parents=True, exist_ok=True)
        with (self._dir / "paper_state.lock").open("a+") as lock:
            if os.name == "nt":
                if lock.tell() == 0:
                    lock.write("\0")
                    lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                if self._state_path.exists():
                    self._load()
                yield
            finally:
                if os.name == "nt":
                    lock.seek(0)
                    msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _refresh_frozen(self, day: date) -> None:
        for inst, pos in self._account.positions.items():
            lots = self._frozen_lots.get(inst, {})
            self._frozen_lots[inst] = {key: qty for key, qty in lots.items() if key >= day.isoformat()}
            pos.frozen_quantity = min(pos.quantity, sum(self._frozen_lots[inst].values()))

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
            "prices": self._prices,
            "price_timestamps": self._price_timestamps,
            "frozen_lots": self._frozen_lots,
        }
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        tmp.replace(self._state_path)
        # JSON is authoritative. CSVs are rebuildable exports, never a second
        # independently committed ledger. An export failure cannot undo a fill.
        try:
            self._sync_exports()
        except OSError as exc:
            logger.warning(f"Paper CSV export failed; JSON account remains saved: {exc}")

    def _load(self) -> None:
        raw = json.loads(self._state_path.read_text())
        self._account = Account.from_dict(raw["account"])
        self._orders = [Order.from_dict(o) for o in raw.get("orders", [])]
        self._fills = [Fill.from_dict(f) for f in raw.get("fills", [])]
        self._nav_history = [NavSnapshot.from_dict(n) for n in raw.get("nav_history", [])]
        self._prev_closes = raw.get("prev_closes", {})
        self._prices = raw.get("prices", {})
        self._price_timestamps = raw.get("price_timestamps", {})
        self._frozen_lots = raw.get("frozen_lots", {})
        if "frozen_lots" not in raw:
            # Old states lack purchase dates. Freeze remaining old quantities
            # conservatively for today rather than silently selling them T+0.
            self._frozen_lots = {
                inst: {_today().isoformat(): pos.frozen_quantity}
                for inst, pos in self._account.positions.items() if pos.frozen_quantity
            }

    def _sync_exports(self) -> None:
        trade_fields = ["timestamp", "instrument", "direction", "quantity", "price", "commission", "stamp_tax", "transfer_fee", "order_id"]
        trade_rows = []
        for fill in self._fills:
            row = fill.to_dict()
            trade_rows.append({key: row[key] for key in trade_fields})
        nav_fields = list(NavSnapshot.__dataclass_fields__)
        for path, fields, rows in (
            (self._trades_csv, trade_fields, trade_rows),
            (self._nav_csv, nav_fields, [snap.to_dict() for snap in self._nav_history]),
        ):
            temporary = path.with_suffix(".tmp")
            with temporary.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            temporary.replace(path)

def _now() -> str:
    return datetime.now(_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")


def _today() -> date:
    return datetime.now(_SHANGHAI).date()
