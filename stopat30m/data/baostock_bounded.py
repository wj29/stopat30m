"""A hard deadline around BaoStock SDK calls and lazy pagination.

The SDK can spin forever on an empty socket read, where socket timeouts no
longer help. This adapter interrupts that loop without changing source data or
retrying requests. It is intended for a serial POSIX command-line collector.
"""
from __future__ import annotations

from functools import wraps
import math
import signal
import threading


class _DeadlineExpired(BaseException):
    """Escape the SDK's broad ``except Exception`` without fabricating data."""


def _expire(_signum, _frame):
    raise _DeadlineExpired


class BaostockDeadlineClient:
    """Proxy an SDK module or result object with a per-call hard timeout.

    No retries, alternate servers, or request pacing are added. The collector
    retains responsibility for those policies. Result methods are wrapped too,
    because ``ResultData.next()`` can perform another network read.
    """

    def __init__(self, source, *, timeout=12):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValueError("Require a numeric BaoStock hard timeout")
        if not math.isfinite(timeout) or not 0 < timeout <= 60:
            raise ValueError("BaoStock hard timeout must be in (0, 60] seconds")
        self._source = source
        self._timeout = float(timeout)

    def __getattr__(self, name):
        value = getattr(self._source, name)
        if not callable(value):
            return value

        @wraps(value)
        def invoke(*args, **kwargs):
            if threading.current_thread() is not threading.main_thread():
                raise RuntimeError("BaoStock hard deadlines require the main thread")
            if not hasattr(signal, "setitimer") or not hasattr(signal, "SIGALRM"):
                raise RuntimeError("BaoStock hard deadlines require POSIX interval timers")
            if any(signal.getitimer(signal.ITIMER_REAL)):
                raise RuntimeError("Refusing to replace an active process alarm")
            previous = signal.getsignal(signal.SIGALRM)
            signal.signal(signal.SIGALRM, _expire)
            signal.setitimer(signal.ITIMER_REAL, self._timeout)
            try:
                result = value(*args, **kwargs)
            except _DeadlineExpired:
                raise TimeoutError("BaoStock SDK call exceeded its hard deadline") from None
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, previous)
            if hasattr(result, "get_row_data") and hasattr(result, "next"):
                return type(self)(result, timeout=self._timeout)
            return result

        return invoke
