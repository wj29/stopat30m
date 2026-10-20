"""Serial BaoStock transport pacing, including SDK lazy page requests."""
from __future__ import annotations

from functools import wraps
import math
import threading
import time

from .baostock_bounded import BaostockDeadlineClient


class PacedBaostockDeadlineClient(BaostockDeadlineClient):
    """Share a network-start clock between SDK calls and their result pages.

    Ordinary row reads do not wait. A consumed full page requires a fresh
    paced request, even though the SDK hides it inside ``ResultData.next``.
    This adds no retries and does not alter returned data or mode flags.
    """

    def __init__(self, source, *, timeout=12, min_interval=0.6):
        super().__init__(source, timeout=timeout)
        if (isinstance(min_interval, bool) or not isinstance(min_interval, (int, float))
                or not math.isfinite(min_interval) or not 0.5 <= min_interval <= 60):
            raise ValueError("BaoStock network interval must be in [0.5, 60] seconds")
        self._pacing = {"last_start": None, "min_interval": float(min_interval)}

    def _network_required(self, name):
        if name.startswith("query_") or name in {"login", "logout"}:
            return True
        if name != "next":
            return False
        from baostock.common.contants import BAOSTOCK_PER_PAGE_COUNT

        data = getattr(self._source, "data", None)
        consumed = getattr(self._source, "cur_row_num", None)
        if data is None or consumed is None:
            # Unknown result implementations must not bypass the rate limit.
            return True
        return len(data) == BAOSTOCK_PER_PAGE_COUNT and consumed >= len(data)

    def __getattr__(self, name):
        value = super().__getattr__(name)
        if not callable(value):
            return value

        @wraps(value)
        def invoke(*args, **kwargs):
            if threading.current_thread() is not threading.main_thread():
                raise RuntimeError("BaoStock hard deadlines require the main thread")
            if name == "get_data":
                raise ValueError("Use next/get_row_data so every network page is paced")
            if self._network_required(name):
                previous = self._pacing["last_start"]
                if previous is not None:
                    remaining = self._pacing["min_interval"] - (time.monotonic() - previous)
                    if remaining > 0:
                        time.sleep(remaining)
                self._pacing["last_start"] = time.monotonic()
            result = value(*args, **kwargs)
            if isinstance(result, PacedBaostockDeadlineClient):
                result._pacing = self._pacing
            return result

        return invoke
