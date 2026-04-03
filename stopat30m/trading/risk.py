"""
Risk management module.

Enforces position limits, drawdown limits, and circuit breakers.
Integrated into the trading engine as a pre-trade check layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from loguru import logger

from stopat30m.config import get


@dataclass
class RiskState:
    """Tracks running risk metrics."""
    peak_equity: float = 0.0
    daily_pnl: float = 0.0
    daily_start_equity: float = 0.0
    last_reset_date: str = ""
    total_positions: int = 0
    circuit_breaker_active: bool = False
    rejected_orders: int = 0


class RiskManager:
    """
    Pre-trade risk control engine.

    Checks every order against position limits, concentration limits,
    drawdown thresholds, and daily loss limits.
    """

    def __init__(
        self,
        max_drawdown: float | None = None,
        max_daily_loss: float | None = None,
        max_single_loss: float | None = None,
        max_concentration: float | None = None,
        max_position_pct: float | None = None,
        max_total_position_pct: float | None = None,
        circuit_breaker_loss: float | None = None,
    ):
        risk_cfg = get("risk") or {}
        trading_cfg = get("trading") or {}

        self.max_drawdown = max_drawdown or risk_cfg.get("max_drawdown", 0.15)
        self.max_daily_loss = max_daily_loss or risk_cfg.get("max_daily_loss", 0.03)
        self.max_single_loss = max_single_loss or risk_cfg.get("max_single_loss", 0.05)
        self.max_concentration = max_concentration or risk_cfg.get("max_concentration", 0.10)
        self.max_position_pct = max_position_pct or trading_cfg.get("max_position_pct", 0.05)
        self.max_total_position_pct = max_total_position_pct or trading_cfg.get("max_total_position_pct", 0.95)
        self.circuit_breaker_loss = circuit_breaker_loss or risk_cfg.get("circuit_breaker_loss", 0.08)

        self.state = RiskState()

    def update_equity(self, current_equity: float, current_date: str | None = None) -> None:
        """Update tracked equity; resets daily P&L at day boundary."""
        today = current_date or datetime.now().strftime("%Y-%m-%d")

        if self.state.last_reset_date != today:
            self.state.daily_start_equity = current_equity
            self.state.daily_pnl = 0.0
            self.state.last_reset_date = today
            self.state.circuit_breaker_active = False

        if current_equity > self.state.peak_equity:
            self.state.peak_equity = current_equity

        if self.state.daily_start_equity > 0:
            self.state.daily_pnl = (current_equity - self.state.daily_start_equity) / self.state.daily_start_equity

    def check_order(
        self,
        symbol: str,
        direction: str,
        volume: int,
        price: float,
        current_equity: float,
        current_positions: dict[str, float],
        current_date: str | None = None,
    ) -> tuple[bool, str]:
        """
        Validate an order against all risk rules.

        Returns:
            (approved, reason): True if approved, else False with rejection reason.
        """
        self.update_equity(current_equity, current_date=current_date)

        if self.state.circuit_breaker_active:
            return False, "circuit_breaker_active"

        # Drawdown check
        if self.state.peak_equity > 0:
            drawdown = (self.state.peak_equity - current_equity) / self.state.peak_equity
            if drawdown >= self.max_drawdown:
                self._trigger_circuit_breaker("max_drawdown_exceeded")
                return False, f"drawdown {drawdown:.2%} >= {self.max_drawdown:.2%}"

        # Daily loss check
        if self.state.daily_pnl <= -self.max_daily_loss:
            self._trigger_circuit_breaker("daily_loss_exceeded")
            return False, f"daily_loss {self.state.daily_pnl:.2%} <= -{self.max_daily_loss:.2%}"

        if direction.upper() == "BUY":
            order_value = volume * price

            # Single position concentration
            position_pct = order_value / (current_equity + 1e-8)
            existing = current_positions.get(symbol, 0.0)
            total_for_symbol = (existing + order_value) / (current_equity + 1e-8)
            if total_for_symbol > self.max_concentration:
                self.state.rejected_orders += 1
                return False, f"concentration {total_for_symbol:.2%} > {self.max_concentration:.2%}"

            # Single order size
            if position_pct > self.max_position_pct:
                self.state.rejected_orders += 1
                return False, f"order_size {position_pct:.2%} > {self.max_position_pct:.2%}"

            # Total exposure
            total_exposure = sum(current_positions.values()) + order_value
            total_pct = total_exposure / (current_equity + 1e-8)
            if total_pct > self.max_total_position_pct:
                self.state.rejected_orders += 1
                return False, f"total_exposure {total_pct:.2%} > {self.max_total_position_pct:.2%}"

        return True, "approved"

    def check_stop_loss(
        self,
        symbol: str,
        entry_price: float,
        current_price: float,
    ) -> tuple[bool, str]:
        """Check if a position should be stopped out."""
        trading_cfg = get("trading") or {}
        stop_loss = trading_cfg.get("stop_loss_pct", 0.08)
        take_profit = trading_cfg.get("take_profit_pct", 0.15)

        pnl_pct = (current_price - entry_price) / entry_price

        if pnl_pct <= -stop_loss:
            return True, f"stop_loss: {pnl_pct:.2%}"
        if pnl_pct >= take_profit:
            return True, f"take_profit: {pnl_pct:.2%}"

        return False, ""

    def _trigger_circuit_breaker(self, reason: str) -> None:
        self.state.circuit_breaker_active = True
        logger.warning(f"CIRCUIT BREAKER TRIGGERED: {reason}")

    def get_status(self) -> dict:
        return {
            "peak_equity": self.state.peak_equity,
            "daily_pnl": round(self.state.daily_pnl, 4),
            "circuit_breaker": self.state.circuit_breaker_active,
            "rejected_orders": self.state.rejected_orders,
        }
