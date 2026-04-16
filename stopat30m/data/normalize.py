"""Unified instrument code normalization for all A-share data flows.

All modules that deal with stock codes should import from here instead of
defining their own normalization logic. Supported input formats:

  SH600000, SZ000001        (Qlib / internal canonical)
  600000.SH, 000001.SZ      (Tushare / Wind style)
  sh600000, sz000001         (Qlib feature directory names)
  sh.600000, sz.000001       (BaoStock)
  600000, 000001             (bare 6-digit)
"""

from __future__ import annotations

_SH_LEADING = ("6", "9")


def normalize_instrument(code: str) -> str:
    """Normalize any instrument code variant to canonical ``SH600000`` / ``SZ000001`` format."""
    code = code.strip().upper()

    if code.startswith(("SH", "SZ")) and len(code) == 8 and code[2:].isdigit():
        return code

    if "." in code:
        parts = code.split(".", 1)
        left, right = parts[0], parts[1]
        if len(left) == 6 and left.isdigit() and right in {"SH", "SZ"}:
            return f"{right}{left}"
        if right and left in {"SH", "SZ"} and len(right) == 6 and right.isdigit():
            return f"{left}{right}"

    bare = code.replace(".", "").replace(" ", "")
    if bare.startswith(("SH", "SZ")) and len(bare) == 8:
        return bare

    digits = bare[-6:] if len(bare) > 6 else bare
    if len(digits) != 6 or not digits.isdigit():
        return code

    if digits.startswith(_SH_LEADING):
        return f"SH{digits}"
    return f"SZ{digits}"


def bare_code(instrument: str) -> str:
    """Strip exchange prefix to get bare 6-digit stock code."""
    inst = instrument.strip().upper()
    if inst.startswith(("SH", "SZ")) and len(inst) == 8:
        return inst[2:]
    if "." in inst:
        parts = inst.split(".", 1)
        for p in parts:
            if len(p) == 6 and p.isdigit():
                return p
    return inst


def to_qlib_symbol(code: str) -> str:
    """Convert any format to Qlib symbol (``SH600000``)."""
    return normalize_instrument(code)


def to_qlib_feature_dir(code: str) -> str:
    """Convert to lowercase Qlib feature directory name (``sh600000``)."""
    return normalize_instrument(code).lower()


def to_tushare_code(code: str) -> str:
    """Convert to Tushare format (``600000.SH``)."""
    norm = normalize_instrument(code)
    return f"{norm[2:]}.{norm[:2]}"


def to_baostock_code(code: str) -> str:
    """Convert to BaoStock format (``sh.600000``)."""
    norm = normalize_instrument(code)
    return f"{norm[:2].lower()}.{norm[2:]}"


def to_sina_symbol(code: str) -> str:
    """Convert to Sina quote API format (``sh600000``)."""
    norm = normalize_instrument(code)
    return norm.lower()


def is_index_symbol(qlib_sym: str) -> bool:
    """Return True if the Qlib symbol represents a market index, not a stock."""
    s = qlib_sym.lower()
    return s.startswith("sh000") or s.startswith("sz399")
