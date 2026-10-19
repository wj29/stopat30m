"""Dependency-free formatting for optional numeric research metrics."""

from __future__ import annotations

import math
from typing import Any


def metric_number(value: Any) -> float | None:
    """Return a finite number; absent/invalid measurements stay unknown."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def format_metric(value: Any, spec: str = ".2f", *, prefix: str = "") -> str:
    """Format measured values and render missing/undefined metrics as an em dash."""
    number = metric_number(value)
    if number is None:
        return "—"
    if spec.endswith("d"):
        if not number.is_integer():
            return "—"
        return prefix + format(int(number), spec)
    return prefix + format(number, spec)
