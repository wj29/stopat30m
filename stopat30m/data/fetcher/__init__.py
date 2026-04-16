"""A-share data fetchers and Qlib format converter.

Supported sources:
  - baostock:  Free, no rate limit. Best for bulk downloads.
  - efinance:  Free, east-money backend. Stable API, good for daily + realtime.
  - akshare:   Free, east-money backend. Wide coverage, rate-limited.
  - tushare:   Paid (token required). Higher quality.
"""

from .akshare import AKShareFetcher
from .base import DataFetcher
from .baostock import BaoStockFetcher
from .efinance import EfinanceFetcher
from .meta import (
    META_FILENAME,
    DataMeta,
    StockMeta,
    build_meta_from_scan,
    fetch_stock_listing_info,
)
from .orchestrator import append_with_source, download_with_source
from .qlib_dumper import QlibDumper
from .tushare import TushareFetcher

__all__ = [
    "DataFetcher",
    "AKShareFetcher",
    "BaoStockFetcher",
    "EfinanceFetcher",
    "TushareFetcher",
    "QlibDumper",
    "DataMeta",
    "StockMeta",
    "META_FILENAME",
    "build_meta_from_scan",
    "fetch_stock_listing_info",
    "download_with_source",
    "append_with_source",
]
