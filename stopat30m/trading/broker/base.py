"""Abstract broker interface.

Every broker implementation — paper, live, or backtest — must conform
to this contract. The rest of the codebase depends only on this ABC.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import Account, Fill, NavSnapshot, Order, OrderStatus, Position


class AbstractBroker(ABC):
    """Unified broker interface for paper and live trading."""

    # -- Order management ----------------------------------------------------

    @abstractmethod
    def submit_order(self, order: Order) -> Order:
        """Validate and execute (or reject) an order.

        Returns the same Order instance with updated status/fills.
        """

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order. Returns True if successfully cancelled."""

    # -- Account & positions -------------------------------------------------

    @abstractmethod
    def get_account(self) -> Account:
        """Return a snapshot of current account state."""

    @abstractmethod
    def get_positions(self) -> dict[str, Position]:
        """Return all positions with quantity > 0."""

    # -- Order & fill history ------------------------------------------------

    @abstractmethod
    def get_orders(self, status: OrderStatus | None = None) -> list[Order]:
        """Return orders, optionally filtered by status."""

    @abstractmethod
    def get_fills(self) -> list[Fill]:
        """Return all historical fills."""

    # -- Market data ---------------------------------------------------------

    @abstractmethod
    def update_prices(self, prices: dict[str, float]) -> None:
        """Push latest market prices into the broker.

        Must be called before submitting orders so that MARKET orders
        have a reference price for execution.
        """

    # -- End-of-day ----------------------------------------------------------

    @abstractmethod
    def settle_day(self, date: str) -> NavSnapshot:
        """Run end-of-day settlement.

        - Unfreeze T+1 shares.
        - Record a NAV snapshot.
        - Store prev_closes for next day's price-limit checks.

        Returns the NAV snapshot.
        """

    # -- Lifecycle -----------------------------------------------------------

    @abstractmethod
    def reset(self) -> None:
        """Wipe all state and start fresh (keeps initial capital)."""

    # -- NAV history ---------------------------------------------------------

    @abstractmethod
    def get_nav_history(self) -> list[NavSnapshot]:
        """Return all daily NAV snapshots."""
