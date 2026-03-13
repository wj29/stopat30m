"""
Signal-based portfolio rebalancer.

Computes the optimal rebalancing plan given:
- Latest trading signals (target portfolio weights)
- Current positions (from trade ledger)
- Total portfolio capital and available cash
- Live market prices

Handles A-share constraints: T+1 settlement, 100-share lot sizes,
and capital feasibility (scales down buys if funds insufficient).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from loguru import logger

from stopat30m.config import get

LOT_SIZE = 100  # A-share minimum trading unit


def normalize_instrument(code: str) -> str:
    """Normalize instrument code to SH/SZ prefixed format."""
    code = code.strip().upper()
    if code.startswith(("SH", "SZ")):
        return code
    bare = code.replace(".", "").replace(" ", "")
    if len(bare) != 6 or not bare.isdigit():
        return code
    if bare.startswith(("6", "9")):
        return f"SH{bare}"
    return f"SZ{bare}"


def bare_code(instrument: str) -> str:
    """Strip SH/SZ prefix -> bare 6-digit code."""
    inst = instrument.strip().upper()
    if inst.startswith(("SH", "SZ")):
        return inst[2:]
    return inst


def fetch_spot_prices(instruments: list[str]) -> dict[str, float]:
    """Fetch latest spot prices for given instruments via AKShare.

    Returns {normalized_instrument: price}.
    Tries multiple AKShare endpoints before giving up.
    """
    if not instruments:
        return {}

    needed = {bare_code(inst): normalize_instrument(inst) for inst in instruments}

    df = _fetch_spot_dataframe()
    if df is None or df.empty:
        return {}

    prices: dict[str, float] = {}
    code_col = _find_col(df, ["代码", "code"])
    price_col = _find_col(df, ["最新价", "close", "当前价"])

    if code_col is None or price_col is None:
        logger.warning(f"Unexpected columns in spot data: {list(df.columns)}")
        return {}

    for _, row in df.iterrows():
        code = str(row[code_col]).strip()
        if code in needed:
            try:
                p = float(row[price_col])
                if p > 0:
                    prices[needed[code]] = p
            except (ValueError, TypeError):
                pass

    missing = set(needed.values()) - set(prices.keys())
    if missing:
        logger.warning(f"Missing prices for {len(missing)} instruments: {list(missing)[:5]}...")

    return prices


def fetch_stock_names(instruments: list[str]) -> dict[str, str]:
    """Fetch stock names for given instruments via AKShare.

    Returns {normalized_instrument: name}. Empty dict if AKShare unavailable.
    """
    if not instruments:
        return {}

    needed = {bare_code(inst): normalize_instrument(inst) for inst in instruments}
    df = _fetch_spot_dataframe()
    if df is None or df.empty:
        return {}

    code_col = _find_col(df, ["代码", "code"])
    name_col = _find_col(df, ["名称", "name"])
    if code_col is None or name_col is None:
        return {}

    names: dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row[code_col]).strip()
        if code in needed:
            names[needed[code]] = str(row[name_col]).strip()
    return names


_last_fetch_error: str = ""


def get_last_fetch_error() -> str:
    return _last_fetch_error


def _fetch_spot_dataframe():
    """Try multiple AKShare endpoints to get spot prices."""
    global _last_fetch_error
    import akshare as ak

    endpoints = [
        ("stock_zh_a_spot_em", lambda: ak.stock_zh_a_spot_em()),
        ("stock_zh_a_spot", lambda: ak.stock_zh_a_spot()),
    ]

    for name, fetch_fn in endpoints:
        try:
            df = fetch_fn()
            if df is not None and not df.empty:
                logger.info(f"Spot prices fetched via {name}: {len(df)} rows")
                _last_fetch_error = ""
                return df
        except Exception as e:
            _last_fetch_error = f"{name}: {e}"
            logger.warning(f"AKShare {name} failed: {e}")
            continue

    logger.error(f"All spot price endpoints failed. Last error: {_last_fetch_error}")
    return None


def fetch_prices_from_qlib(instruments: list[str]) -> dict[str, float]:
    """Fallback: fetch latest close prices from local Qlib data.

    Not real-time, but usable when AKShare is unreachable.
    Returns {normalized_instrument: last_close_price}.
    """
    try:
        from stopat30m.data.provider import init_qlib
        import qlib.data

        init_qlib()

        norm = [normalize_instrument(c) for c in instruments]
        df = qlib.data.D.features(
            instruments=norm,
            fields=["$close"],
            start_time="2020-01-01",
            end_time="2099-12-31",
        )
        if df is None or df.empty:
            return {}

        latest = df.groupby(level="instrument").tail(1)
        prices = {}
        for (_, inst), row in latest.iterrows():
            p = float(row["$close"])
            if p > 0:
                prices[inst] = p

        logger.info(f"Qlib fallback: got prices for {len(prices)} instruments")
        return prices
    except Exception as e:
        logger.warning(f"Qlib price fallback failed: {e}")
        return {}


def _find_col(df, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


@dataclass
class RebalancePlan:
    """Full rebalance result with capital flow analysis."""

    trades: pd.DataFrame
    capital_flow: dict[str, float] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    hold_unchanged: list[str] = field(default_factory=list)

    @property
    def sells(self) -> pd.DataFrame:
        return self.trades[self.trades["direction"] == "SELL"] if not self.trades.empty else self.trades

    @property
    def buys(self) -> pd.DataFrame:
        return self.trades[self.trades["direction"] == "BUY"] if not self.trades.empty else self.trades

    @property
    def errors(self) -> pd.DataFrame:
        return self.trades[self.trades["direction"] == "-"] if not self.trades.empty else self.trades


def compute_rebalance_plan(
    signals: pd.DataFrame,
    positions: pd.DataFrame,
    total_capital: float,
    prices: dict[str, float],
    cash_reserve_pct: float = 0.02,
) -> RebalancePlan:
    """Compute trades needed to align portfolio with signal targets.

    Two-pass approach:
      Pass 1 — determine all sells and ideal buys.
      Pass 2 — if total buy cost exceeds available cash + sell proceeds,
               scale down buy quantities proportionally.

    Args:
        signals: DataFrame with columns [instrument, weight, signal].
        positions: DataFrame from ledger.compute_positions().
        total_capital: Total portfolio value (cash + holdings).
        prices: {instrument: latest_price}.
        cash_reserve_pct: Fraction of capital to keep as cash buffer.

    Returns:
        RebalancePlan with trades, capital flow summary, and warnings.
    """
    cfg_bt = get("backtest") or {}
    buy_cost_rate = cfg_bt.get("buy_cost", 0.0003)
    sell_cost_rate = cfg_bt.get("sell_cost", 0.0013)

    # --- Parse inputs ---
    target_weights: dict[str, float] = {}
    for _, row in signals.iterrows():
        inst = normalize_instrument(str(row["instrument"]))
        target_weights[inst] = float(row.get("weight", 0))

    current_qty: dict[str, int] = {}
    current_cost: dict[str, float] = {}
    if not positions.empty:
        for _, row in positions.iterrows():
            inst = normalize_instrument(str(row["instrument"]))
            current_qty[inst] = int(row["quantity"])
            current_cost[inst] = float(row.get("total_cost", 0))

    # Current holding market value
    holding_value = sum(
        current_qty.get(inst, 0) * prices.get(inst, 0) for inst in current_qty
    )
    available_cash = total_capital - holding_value

    investable = total_capital * (1 - cash_reserve_pct)
    all_instruments = set(target_weights.keys()) | set(current_qty.keys())

    warnings: list[str] = []
    hold_unchanged: list[str] = []

    # --- Pass 1: compute ideal trades ---
    sell_trades: list[dict] = []
    buy_trades: list[dict] = []
    error_trades: list[dict] = []

    for inst in sorted(all_instruments):
        tw = target_weights.get(inst, 0.0)
        cq = current_qty.get(inst, 0)
        price = prices.get(inst)

        if price is None or price <= 0:
            if tw > 0 or cq > 0:
                error_trades.append(_error_row(inst, "无法获取价格"))
            continue

        target_amount = investable * tw
        target_qty = int(target_amount / price / LOT_SIZE) * LOT_SIZE
        diff = target_qty - cq

        if diff == 0:
            if cq > 0:
                hold_unchanged.append(inst)
            continue

        if diff < 0:
            sell_qty = abs(diff)
            amount = sell_qty * price
            commission = round(amount * sell_cost_rate, 2)
            sell_trades.append({
                "instrument": inst,
                "direction": "SELL",
                "quantity": sell_qty,
                "price": round(price, 4),
                "amount": round(amount, 2),
                "commission": commission,
                "reason": "清仓" if target_qty == 0 else "减仓",
                "_priority": 0,
            })
        else:
            buy_qty = diff
            amount = buy_qty * price
            commission = round(amount * buy_cost_rate, 2)
            buy_trades.append({
                "instrument": inst,
                "direction": "BUY",
                "quantity": buy_qty,
                "price": round(price, 4),
                "amount": round(amount, 2),
                "commission": commission,
                "reason": "新建仓" if cq == 0 else "加仓",
                "_target_weight": tw,
                "_priority": 1,
            })

    # --- Pass 2: capital feasibility check ---
    sell_proceeds = sum(t["amount"] - t["commission"] for t in sell_trades)
    buy_needed = sum(t["amount"] + t["commission"] for t in buy_trades)
    cash_after_sells = available_cash + sell_proceeds
    reserve_amount = total_capital * cash_reserve_pct
    budget_for_buys = max(0, cash_after_sells - reserve_amount)

    if buy_needed > 0 and buy_needed > budget_for_buys:
        scale = budget_for_buys / buy_needed
        warnings.append(
            f"买入总额 ¥{buy_needed:,.0f} 超过可用资金 ¥{budget_for_buys:,.0f}，"
            f"已按 {scale:.0%} 比例缩减"
        )
        scaled_buys = []
        for t in buy_trades:
            new_qty = int(t["quantity"] * scale / LOT_SIZE) * LOT_SIZE
            if new_qty < LOT_SIZE:
                warnings.append(f"{t['instrument']}: 缩减后不足 1 手，跳过")
                continue
            new_amount = new_qty * t["price"]
            t["quantity"] = new_qty
            t["amount"] = round(new_amount, 2)
            t["commission"] = round(new_amount * buy_cost_rate, 2)
            scaled_buys.append(t)
        buy_trades = scaled_buys
        buy_needed = sum(t["amount"] + t["commission"] for t in buy_trades)

    if available_cash < 0:
        warnings.append(f"当前现金为负 ¥{available_cash:,.0f}，请确认总资金输入是否正确")

    # --- Build final DataFrame ---
    all_trades = sell_trades + buy_trades + error_trades
    for t in all_trades:
        t.pop("_target_weight", None)
        t.pop("_priority", None)

    if not all_trades:
        trades_df = pd.DataFrame(columns=[
            "instrument", "direction", "quantity", "price",
            "amount", "commission", "reason",
        ])
    else:
        trades_df = pd.DataFrame(all_trades)

    # Recalculate final totals
    final_sell = sum(t["amount"] for t in sell_trades)
    final_buy = sum(t["amount"] for t in buy_trades)
    sell_commission = sum(t["commission"] for t in sell_trades)
    buy_commission = sum(t["commission"] for t in buy_trades)

    capital_flow = {
        "total_capital": total_capital,
        "holding_value": round(holding_value, 2),
        "available_cash": round(available_cash, 2),
        "sell_proceeds": round(sell_proceeds, 2),
        "sell_commission": round(sell_commission, 2),
        "buy_cost": round(final_buy, 2),
        "buy_commission": round(buy_commission, 2),
        "cash_after_rebalance": round(
            available_cash + sell_proceeds - final_buy - buy_commission, 2
        ),
        "reserve_target": round(reserve_amount, 2),
    }

    logger.info(
        f"Rebalance plan: {len(sell_trades)} sells (¥{final_sell:,.0f}), "
        f"{len(buy_trades)} buys (¥{final_buy:,.0f}), "
        f"cash after: ¥{capital_flow['cash_after_rebalance']:,.0f}"
    )

    return RebalancePlan(
        trades=trades_df,
        capital_flow=capital_flow,
        warnings=warnings,
        hold_unchanged=hold_unchanged,
    )


def _error_row(inst: str, reason: str) -> dict:
    return {
        "instrument": inst,
        "direction": "-",
        "quantity": 0,
        "price": 0,
        "amount": 0,
        "commission": 0,
        "reason": reason,
    }


def load_latest_signals(signal_dir: str | Path | None = None) -> pd.DataFrame | None:
    """Load the most recent signal CSV file."""
    sig_dir = Path(signal_dir or get("signal", "output_dir", "./output/signals"))
    if not sig_dir.exists():
        return None
    files = sorted(sig_dir.glob("signal_*.csv"), reverse=True)
    if not files:
        return None
    df = pd.read_csv(files[0])
    logger.info(f"Loaded signals from {files[0].name}: {len(df)} rows")
    return df
