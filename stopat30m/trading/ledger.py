"""
CSV-based trade ledger for manual trade recording.

Stores trades in output/trades/trades.csv and computes positions,
average cost, and P&L from the raw trade history.

Thread-safe for single-process Streamlit usage via file locking.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

TRADES_DIR = Path("./output/trades")
TRADES_FILE = TRADES_DIR / "trades.csv"

COLUMNS = [
    "id", "date", "instrument", "direction", "quantity", "price",
    "commission", "note", "created_at",
]

DIRECTION_BUY = "BUY"
DIRECTION_SELL = "SELL"


def _ensure_file() -> Path:
    TRADES_DIR.mkdir(parents=True, exist_ok=True)
    if not TRADES_FILE.exists():
        pd.DataFrame(columns=COLUMNS).to_csv(TRADES_FILE, index=False)
    return TRADES_FILE


def load_trades() -> pd.DataFrame:
    """Load all trades from disk."""
    path = _ensure_file()
    df = pd.read_csv(path, dtype={"instrument": str})
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


def validate_trade(
    direction: str, quantity: int, price: float, instrument: str,
) -> tuple[bool, str]:
    """Basic sanity checks before recording a trade."""
    if direction.upper() not in (DIRECTION_BUY, DIRECTION_SELL):
        return False, f"无效方向: {direction}"
    if quantity <= 0:
        return False, f"数量必须大于 0: {quantity}"
    if price <= 0:
        return False, f"价格必须大于 0: {price}"
    bare = instrument.strip().upper()
    if not bare:
        return False, "证券代码不能为空"
    return True, ""


def add_trade(
    date: str,
    instrument: str,
    direction: str,
    quantity: int,
    price: float,
    commission: float = 0.0,
    note: str = "",
) -> pd.DataFrame:
    """Append a trade record and return updated trades."""
    ok, reason = validate_trade(direction, quantity, price, instrument)
    if not ok:
        raise ValueError(reason)

    df = load_trades()
    max_id = int(df["id"].max()) if not df.empty else 0
    new_row = pd.DataFrame([{
        "id": max_id + 1,
        "date": date,
        "instrument": instrument.strip().upper(),
        "direction": direction.upper(),
        "quantity": int(quantity),
        "price": round(float(price), 4),
        "commission": round(float(commission), 2),
        "note": note,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(TRADES_FILE, index=False)
    logger.info(f"Trade added: {direction} {quantity} {instrument} @ {price}")
    return df


def delete_trade(trade_id: int) -> pd.DataFrame:
    """Delete a trade by ID and return updated trades."""
    df = load_trades()
    df = df[df["id"] != trade_id]
    df.to_csv(TRADES_FILE, index=False)
    return df


def compute_positions(trades: pd.DataFrame | None = None) -> pd.DataFrame:
    """Compute current positions from trade history.

    Returns DataFrame with columns:
        instrument, quantity, avg_cost, total_cost, commission_total
    """
    if trades is None:
        trades = load_trades()
    if trades.empty:
        return pd.DataFrame(columns=[
            "instrument", "quantity", "avg_cost", "total_cost", "commission_total",
        ])

    positions: dict[str, dict] = {}

    for _, row in trades.sort_values("date").iterrows():
        inst = row["instrument"]
        qty = int(row["quantity"])
        price = float(row["price"])
        comm = float(row.get("commission", 0))

        if inst not in positions:
            positions[inst] = {"quantity": 0, "total_cost": 0.0, "commission_total": 0.0}

        pos = positions[inst]

        if row["direction"] == DIRECTION_BUY:
            pos["total_cost"] += qty * price
            pos["quantity"] += qty
        elif row["direction"] == DIRECTION_SELL:
            actual_sell = min(qty, pos["quantity"])
            if actual_sell <= 0:
                logger.warning(f"Sell {qty} {inst} but position is {pos['quantity']}, skipping")
                continue
            sell_ratio = actual_sell / pos["quantity"]
            pos["total_cost"] *= (1 - sell_ratio)
            pos["quantity"] -= actual_sell

        pos["commission_total"] += comm

    rows = []
    for inst, pos in sorted(positions.items()):
        q = pos["quantity"]
        if q == 0:
            continue
        rows.append({
            "instrument": inst,
            "quantity": q,
            "avg_cost": round(pos["total_cost"] / q, 4) if q > 0 else 0,
            "total_cost": round(pos["total_cost"], 2),
            "commission_total": round(pos["commission_total"], 2),
        })

    return pd.DataFrame(rows)


def add_trades_batch(
    trades_plan: pd.DataFrame,
    date: str | None = None,
    note: str = "",
) -> pd.DataFrame:
    """Record multiple trades from a rebalance plan at once.

    Args:
        trades_plan: DataFrame with columns [instrument, direction, quantity, price, commission].
        date: Trade date (default: today).
        note: Shared note for all trades.

    Returns:
        Updated trades DataFrame.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    actionable = trades_plan[trades_plan["direction"].isin(["BUY", "SELL"])].copy()
    if actionable.empty:
        logger.info("No actionable trades in plan")
        return load_trades()

    df = load_trades()
    max_id = int(df["id"].max()) if not df.empty else 0

    new_rows = []
    for _, row in actionable.iterrows():
        max_id += 1
        new_rows.append({
            "id": max_id,
            "date": date,
            "instrument": str(row["instrument"]).strip().upper(),
            "direction": str(row["direction"]).upper(),
            "quantity": int(row["quantity"]),
            "price": round(float(row["price"]), 4),
            "commission": round(float(row.get("commission", 0)), 2),
            "note": note,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df.to_csv(TRADES_FILE, index=False)
    logger.info(f"Batch recorded {len(new_rows)} trades for {date}")
    return df


# ---------------------------------------------------------------------------
# Portfolio snapshots (for tracking NAV over time)
# ---------------------------------------------------------------------------

SNAPSHOT_DIR = Path("./output/trades")
SNAPSHOT_FILE = SNAPSHOT_DIR / "portfolio_nav.csv"
SNAPSHOT_COLUMNS = ["date", "total_value", "total_cost", "cash", "unrealized_pnl"]


def save_portfolio_snapshot(
    date: str,
    total_value: float,
    total_cost: float,
    cash: float,
    unrealized_pnl: float,
) -> None:
    """Append a portfolio valuation snapshot for NAV tracking."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    row = pd.DataFrame([{
        "date": date,
        "total_value": round(total_value, 2),
        "total_cost": round(total_cost, 2),
        "cash": round(cash, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
    }])
    if SNAPSHOT_FILE.exists():
        existing = pd.read_csv(SNAPSHOT_FILE)
        existing = existing[existing["date"] != date]
        df = pd.concat([existing, row], ignore_index=True)
    else:
        df = row
    df.to_csv(SNAPSHOT_FILE, index=False)


def load_portfolio_nav() -> pd.DataFrame:
    """Load portfolio NAV history."""
    if not SNAPSHOT_FILE.exists():
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)
    df = pd.read_csv(SNAPSHOT_FILE)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def compute_daily_pnl(trades: pd.DataFrame | None = None) -> pd.Series:
    """Compute daily realized P&L from trade history.

    Only counts SELL trades. P&L = sell_qty * (sell_price - avg_cost_at_time).
    Returns a pd.Series indexed by date.
    """
    if trades is None:
        trades = load_trades()
    if trades.empty:
        return pd.Series(dtype=float)

    cost_basis: dict[str, dict] = {}
    daily_pnl: dict[str, float] = {}

    for _, row in trades.sort_values("date").iterrows():
        inst = row["instrument"]
        qty = int(row["quantity"])
        price = float(row["price"])
        comm = float(row.get("commission", 0))
        date = row["date"]

        if inst not in cost_basis:
            cost_basis[inst] = {"quantity": 0, "total_cost": 0.0}

        cb = cost_basis[inst]

        if row["direction"] == DIRECTION_BUY:
            cb["total_cost"] += qty * price
            cb["quantity"] += qty
        elif row["direction"] == DIRECTION_SELL:
            avg = cb["total_cost"] / cb["quantity"] if cb["quantity"] > 0 else 0
            realized = qty * (price - avg) - comm
            daily_pnl[date] = daily_pnl.get(date, 0.0) + realized
            if cb["quantity"] > 0:
                sell_ratio = min(qty, cb["quantity"]) / cb["quantity"]
                cb["total_cost"] *= (1 - sell_ratio)
            cb["quantity"] -= qty

    if not daily_pnl:
        return pd.Series(dtype=float)

    s = pd.Series(daily_pnl).sort_index()
    s.index = pd.to_datetime(s.index)
    s.index.name = "date"
    return s
