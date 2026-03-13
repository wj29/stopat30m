"""
vn.py portfolio strategy driven by Qlib alpha signals.

Reads signals from CSV/Redis and executes portfolio rebalancing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from stopat30m.config import get
from stopat30m.trading.risk import RiskManager


class AlphaPortfolioStrategy:
    """
    Portfolio strategy that consumes Qlib alpha signals.

    Can operate in:
    - Backtest mode (simulated execution)
    - Paper trading mode (log orders without execution)
    - Live mode (via vn.py gateway)
    """

    def __init__(
        self,
        risk_manager: RiskManager | None = None,
        paper_trading: bool | None = None,
    ):
        trading_cfg = get("trading") or {}
        self.paper_trading = paper_trading if paper_trading is not None else trading_cfg.get("paper_trading", True)
        self.risk_manager = risk_manager or RiskManager()

        self.positions: dict[str, dict[str, Any]] = {}
        self.pending_orders: list[dict] = []
        self.trade_log: list[dict] = []
        self.equity: float = 1_000_000.0

        self._engine: Any = None

    def set_engine(self, engine: Any) -> None:
        """Bind to a vn.py MainEngine instance."""
        self._engine = engine

    def load_signals(self, source: str | Path | None = None) -> pd.DataFrame:
        """
        Load latest signals from file or Redis.

        Args:
            source: Path to CSV/JSON file. If None, reads from Redis.
        """
        if source is not None:
            path = Path(source)
            if path.suffix == ".csv":
                return pd.read_csv(path)
            elif path.suffix == ".json":
                return pd.read_json(path)
            raise ValueError(f"Unsupported signal file format: {path.suffix}")

        return self._load_from_redis()

    def _load_from_redis(self) -> pd.DataFrame:
        import redis
        cfg = get("redis") or {}
        r = redis.Redis(
            host=cfg.get("host", "localhost"),
            port=cfg.get("port", 6379),
            db=cfg.get("db", 0),
        )
        raw = r.get("latest_signals")
        if raw is None:
            logger.warning("No signals found in Redis.")
            return pd.DataFrame()
        return pd.read_json(raw.decode("utf-8"))

    def rebalance(self, signals: pd.DataFrame) -> list[dict]:
        """
        Execute portfolio rebalancing based on signals.

        Calculates target positions, diffs against current, generates orders.
        """
        if signals.empty:
            logger.info("No signals to rebalance.")
            return []

        target = self._compute_target_positions(signals)
        orders = self._diff_positions(target)
        executed = self._execute_orders(orders)

        return executed

    def _compute_target_positions(self, signals: pd.DataFrame) -> dict[str, dict]:
        """Convert signals into target position map."""
        targets = {}
        buy_signals = signals[signals["signal"] == "BUY"]

        for _, row in buy_signals.iterrows():
            instrument = row["instrument"]
            weight = row["weight"]
            target_value = self.equity * weight

            targets[instrument] = {
                "target_value": target_value,
                "weight": weight,
                "score": row.get("score", 0),
            }

        return targets

    def _diff_positions(self, targets: dict[str, dict]) -> list[dict]:
        """Compare targets vs current positions, produce order list."""
        orders = []

        # Close positions not in target
        for symbol in list(self.positions.keys()):
            if symbol not in targets:
                pos = self.positions[symbol]
                orders.append({
                    "symbol": symbol,
                    "direction": "SELL",
                    "volume": pos["volume"],
                    "price": pos.get("current_price", pos["avg_price"]),
                    "reason": "not_in_target",
                })

        # Open or adjust positions in target
        for symbol, target in targets.items():
            current_value = 0.0
            if symbol in self.positions:
                pos = self.positions[symbol]
                current_value = pos["volume"] * pos.get("current_price", pos["avg_price"])

            diff_value = target["target_value"] - current_value
            if abs(diff_value) < 1000:
                continue

            # A-share lot size is 100
            est_price = self.positions.get(symbol, {}).get("current_price", 10.0)
            volume = max(int(abs(diff_value) / (est_price + 1e-8) / 100) * 100, 100)

            direction = "BUY" if diff_value > 0 else "SELL"
            orders.append({
                "symbol": symbol,
                "direction": direction,
                "volume": volume,
                "price": est_price,
                "reason": "rebalance",
            })

        return orders

    def _execute_orders(self, orders: list[dict]) -> list[dict]:
        """Execute orders with risk checks."""
        executed = []
        current_pos_values = {
            s: p["volume"] * p.get("current_price", p["avg_price"])
            for s, p in self.positions.items()
        }

        for order in orders:
            approved, reason = self.risk_manager.check_order(
                symbol=order["symbol"],
                direction=order["direction"],
                volume=order["volume"],
                price=order["price"],
                current_equity=self.equity,
                current_positions=current_pos_values,
            )

            if not approved:
                logger.warning(f"Order REJECTED: {order['symbol']} {order['direction']} - {reason}")
                order["status"] = "rejected"
                order["reject_reason"] = reason
                self.trade_log.append(order)
                continue

            if self.paper_trading:
                self._paper_execute(order)
            elif self._engine is not None:
                self._live_execute(order)
            else:
                logger.error("No engine bound and not in paper mode. Cannot execute.")
                continue

            order["status"] = "executed"
            executed.append(order)
            self.trade_log.append(order)

        logger.info(f"Executed {len(executed)}/{len(orders)} orders")
        return executed

    def _paper_execute(self, order: dict) -> None:
        """Simulate order execution."""
        symbol = order["symbol"]
        direction = order["direction"]
        volume = order["volume"]
        price = order["price"]

        if direction == "BUY":
            if symbol in self.positions:
                pos = self.positions[symbol]
                total_cost = pos["avg_price"] * pos["volume"] + price * volume
                total_vol = pos["volume"] + volume
                pos["avg_price"] = total_cost / total_vol
                pos["volume"] = total_vol
            else:
                self.positions[symbol] = {
                    "volume": volume,
                    "avg_price": price,
                    "current_price": price,
                }
            self.equity -= volume * price * 0.0003  # commission estimate
        elif direction == "SELL":
            if symbol in self.positions:
                pos = self.positions[symbol]
                sell_vol = min(volume, pos["volume"])
                pos["volume"] -= sell_vol
                if pos["volume"] <= 0:
                    del self.positions[symbol]
                self.equity -= sell_vol * price * 0.0013  # commission + stamp tax
        logger.info(f"[PAPER] {direction} {symbol} vol={volume} price={price:.2f}")

    def _live_execute(self, order: dict) -> None:
        """Send order through vn.py engine."""
        if self._engine is None:
            raise RuntimeError("Engine not bound")

        from vnpy.trader.constant import Direction, Offset, OrderType, Exchange

        # Map symbol to exchange
        exchange = self._resolve_exchange(order["symbol"])
        direction = Direction.LONG if order["direction"] == "BUY" else Direction.SHORT

        req = {
            "symbol": order["symbol"],
            "exchange": exchange,
            "direction": direction,
            "type": OrderType.LIMIT,
            "volume": order["volume"],
            "price": order["price"],
            "offset": Offset.OPEN if order["direction"] == "BUY" else Offset.CLOSE,
        }

        from vnpy.trader.object import OrderRequest
        order_req = OrderRequest(**req)
        gateway_name = get("trading", "gateway", "xtp")
        vt_orderid = self._engine.send_order(order_req, gateway_name)
        logger.info(f"[LIVE] Sent {order['direction']} {order['symbol']} -> {vt_orderid}")

    @staticmethod
    def _resolve_exchange(symbol: str) -> Any:
        """Determine exchange from stock code prefix."""
        from vnpy.trader.constant import Exchange
        if symbol.startswith(("6", "5")):
            return Exchange.SSE
        elif symbol.startswith(("0", "3")):
            return Exchange.SZSE
        elif symbol.startswith("8"):
            return Exchange.BSE
        return Exchange.SSE

    def check_stop_loss(self, market_prices: dict[str, float]) -> list[dict]:
        """Check all positions for stop-loss / take-profit triggers."""
        close_orders = []
        for symbol, pos in list(self.positions.items()):
            if symbol not in market_prices:
                continue

            current = market_prices[symbol]
            pos["current_price"] = current

            should_close, reason = self.risk_manager.check_stop_loss(
                symbol=symbol,
                entry_price=pos["avg_price"],
                current_price=current,
            )
            if should_close:
                close_orders.append({
                    "symbol": symbol,
                    "direction": "SELL",
                    "volume": pos["volume"],
                    "price": current,
                    "reason": reason,
                })
                logger.warning(f"Stop triggered: {symbol} {reason}")

        if close_orders:
            self._execute_orders(close_orders)

        return close_orders

    def get_portfolio_summary(self) -> dict:
        """Return current portfolio state."""
        total_value = sum(
            p["volume"] * p.get("current_price", p["avg_price"])
            for p in self.positions.values()
        )
        return {
            "equity": self.equity,
            "position_count": len(self.positions),
            "total_position_value": total_value,
            "positions": {
                s: {
                    "volume": p["volume"],
                    "avg_price": p["avg_price"],
                    "current_price": p.get("current_price", p["avg_price"]),
                    "pnl_pct": (p.get("current_price", p["avg_price"]) - p["avg_price"]) / p["avg_price"],
                }
                for s, p in self.positions.items()
            },
            "risk_status": self.risk_manager.get_status(),
        }
