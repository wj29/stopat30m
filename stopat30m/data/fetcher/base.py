"""Abstract base class for A-share data fetchers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import NamedTuple

import pandas as pd


QLIB_FIELDS = ("open", "close", "high", "low", "volume", "change", "factor")


class _FetchResult(NamedTuple):
    """Result from a subprocess fetch worker (must be picklable)."""
    code: str
    status: str       # "ok" | "empty" | "skip" | "error"
    data_start: str   # empty string if N/A
    data_end: str
    n_rows: int
    error: str        # empty string if no error


class DataFetcher(ABC):
    """Base class for A-share data sources."""

    @abstractmethod
    def fetch_stock_list(self) -> list[str]:
        """Return list of raw stock codes (e.g. ['600000', '000001', ...])."""

    @abstractmethod
    def fetch_trade_calendar(self, start: str, end: str) -> list[str]:
        """Return sorted list of trading dates as 'YYYY-MM-DD' strings."""

    @abstractmethod
    def fetch_daily(self, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        """Fetch daily OHLCV for one stock.

        Must return DataFrame with columns:
            date (str YYYY-MM-DD), open, high, low, close, volume, change, factor
        Sorted by date ascending. Returns None on failure.
        """

    @abstractmethod
    def fetch_index_components(self, index_code: str) -> list[str]:
        """Return list of raw stock codes belonging to the index."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable source name."""

    @property
    def concurrency(self) -> int:
        """Max parallel sub-workers for intra-source parallelism."""
        return 1

    def can_fetch(self, symbol: str) -> bool:
        """Return False if this source is known to not support the symbol."""
        return True
