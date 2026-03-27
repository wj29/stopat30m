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


_spot_cache: dict = {}


_BATCH_THRESHOLD = 50


def fetch_spot_prices(instruments: list[str]) -> dict[str, float]:
    """Fetch latest spot prices from multiple sources with automatic failover.

    For small lists (< 50 stocks): per-stock APIs first (fast, ~100ms).
      Sina → Tencent → AKShare batch (fallback)

    For large lists (>= 50 stocks): batch API first (efficient).
      AKShare batch → fill missing with Sina → Tencent

    Returns {normalized_instrument: price}.
    """
    if not instruments:
        return {}

    needed = {bare_code(inst): normalize_instrument(inst) for inst in instruments}
    prices: dict[str, float] = {}
    names: dict[str, str] = {}

    if len(needed) < _BATCH_THRESHOLD:
        sources = [
            ("Sina", lambda codes: _fetch_sina(codes)),
            ("Tencent", lambda codes: _fetch_tencent(codes)),
            ("AKShare", lambda codes: _fetch_via_batch(codes, needed)),
        ]
    else:
        sources = [
            ("AKShare", lambda codes: _fetch_via_batch(codes, needed)),
            ("Sina", lambda codes: _fetch_sina(codes)),
            ("Tencent", lambda codes: _fetch_tencent(codes)),
        ]

    missing_codes = dict(needed)
    for source_name, fetch_fn in sources:
        if not missing_codes:
            break
        try:
            result = fetch_fn(list(missing_codes.keys()))
            for code, (price, name) in result.items():
                inst = missing_codes.get(code)
                if inst and price > 0:
                    prices[inst] = price
                    if name:
                        names[inst] = name
            prev_missing = len(missing_codes)
            missing_codes = {c: n for c, n in missing_codes.items() if n not in prices}
            filled = prev_missing - len(missing_codes)
            if filled > 0:
                logger.info(f"{source_name}: got {filled} prices")
        except Exception as e:
            logger.warning(f"{source_name} failed: {e}")

    if not prices:
        logger.error("All real-time price sources failed")
    elif len(prices) < len(needed):
        logger.warning(f"Got prices for {len(prices)}/{len(needed)} instruments")

    _spot_cache["prices"] = prices
    _spot_cache["names"] = names
    return prices


def _fetch_via_batch(
    codes: list[str], needed: dict[str, str],
) -> dict[str, tuple[float, str]]:
    """Adapter: fetch full-market AKShare data, return per-stock results."""
    df = _fetch_spot_dataframe()
    if df is None or df.empty:
        return {}
    prices, names = _extract_from_dataframe(df, needed)
    result: dict[str, tuple[float, str]] = {}
    for code, inst in needed.items():
        if inst in prices:
            result[code] = (prices[inst], names.get(inst, ""))
    return result


def _name_cache_path() -> Path:
    from stopat30m.data.provider import get_data_dir
    return get_data_dir() / "stock_names.json"


def _load_name_cache() -> dict[str, str]:
    """Load {bare_code: name} from local JSON cache."""
    import json
    p = _name_cache_path()
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {}


def _save_name_cache(cache: dict[str, str]) -> None:
    """Persist {bare_code: name} to local JSON cache."""
    import json
    p = _name_cache_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
        tmp.replace(p)
    except Exception as e:
        logger.debug(f"Failed to save name cache: {e}")


def fetch_stock_names(instruments: list[str]) -> dict[str, str]:
    """Fetch stock names. Reads from local disk cache first, then API for misses.

    Returns {normalized_instrument: name}.
    """
    if not instruments:
        return {}

    if _spot_cache.get("names"):
        return _spot_cache["names"]

    needed = {bare_code(inst): normalize_instrument(inst) for inst in instruments}
    disk_cache = _load_name_cache()

    result: dict[str, str] = {}
    api_needed: dict[str, str] = {}
    for bare, inst in needed.items():
        if bare in disk_cache:
            result[inst] = disk_cache[bare]
        else:
            api_needed[bare] = inst

    if not api_needed:
        return result

    api_names: dict[str, str] = {}

    df = _fetch_spot_dataframe()
    if df is not None and not df.empty:
        _, api_names = _extract_from_dataframe(df, api_needed)

    if len(api_names) < len(api_needed):
        try:
            still_missing = {b: i for b, i in api_needed.items() if i not in api_names}
            if still_missing:
                sina_result = _fetch_sina(list(still_missing.keys()))
                for c, (_, name) in sina_result.items():
                    if c in still_missing and name:
                        api_names[still_missing[c]] = name
        except Exception:
            pass

    if api_names:
        result.update(api_names)
        for inst, name in api_names.items():
            disk_cache[bare_code(inst)] = name
        _save_name_cache(disk_cache)

    return result


def _extract_from_dataframe(
    df: pd.DataFrame, needed: dict[str, str],
) -> tuple[dict[str, float], dict[str, str]]:
    """Extract prices and names from an AKShare-style DataFrame."""
    code_col = _find_col(df, ["代码", "code"])
    price_col = _find_col(df, ["最新价", "close", "当前价"])
    name_col = _find_col(df, ["名称", "name"])

    if code_col is None or price_col is None:
        logger.warning(f"Unexpected columns in spot data: {list(df.columns)}")
        return {}, {}

    prices: dict[str, float] = {}
    names: dict[str, str] = {}
    for _, row in df.iterrows():
        code = str(row[code_col]).strip()
        if code in needed:
            try:
                p = float(row[price_col])
                if p > 0:
                    prices[needed[code]] = p
            except (ValueError, TypeError):
                pass
            if name_col is not None:
                names[needed[code]] = str(row[name_col]).strip()

    return prices, names


_last_fetch_error: str = ""


def get_last_fetch_error() -> str:
    return _last_fetch_error


def _fetch_spot_dataframe() -> pd.DataFrame | None:
    """Try AKShare batch endpoints for full-market spot data."""
    global _last_fetch_error

    try:
        import akshare as ak
    except ImportError:
        _last_fetch_error = "akshare not installed"
        return None

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

    logger.warning(f"All AKShare endpoints failed. Last error: {_last_fetch_error}")
    return None


def _instrument_to_sina_symbol(bare: str) -> str:
    """Convert bare code '601006' to Sina symbol 'sh601006'."""
    if bare.startswith(("6", "9")):
        return f"sh{bare}"
    return f"sz{bare}"


def _fetch_sina(codes: list[str]) -> dict[str, tuple[float, str]]:
    """Fetch real-time prices from Sina Finance HTTP API.

    Returns {bare_code: (price, name)}.
    """
    import requests

    symbols = [_instrument_to_sina_symbol(c) for c in codes]
    url = f"https://hq.sinajs.cn/list={','.join(symbols)}"
    headers = {"Referer": "https://finance.sina.com.cn"}

    resp = requests.get(url, headers=headers, timeout=10)
    resp.encoding = "gbk"

    result: dict[str, tuple[float, str]] = {}
    for line in resp.text.strip().split("\n"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        # var hq_str_sh601006="大秦铁路,9.04,9.02,9.06,..."
        var_part, _, data_part = line.partition("=")
        symbol = var_part.split("_")[-1]
        bare = symbol[2:]  # strip sh/sz prefix
        data_part = data_part.strip('" ;')
        if not data_part:
            continue

        fields = data_part.split(",")
        if len(fields) < 4:
            continue

        name = fields[0]
        try:
            price = float(fields[3])  # field[3] = current price
        except (ValueError, IndexError):
            continue

        if bare in codes and price > 0:
            result[bare] = (price, name)

    logger.info(f"Sina: got {len(result)}/{len(codes)} prices")
    return result


def _fetch_tencent(codes: list[str]) -> dict[str, tuple[float, str]]:
    """Fetch real-time prices from Tencent Finance HTTP API.

    Returns {bare_code: (price, name)}.
    """
    import requests

    symbols = [_instrument_to_sina_symbol(c) for c in codes]  # same sh/sz format
    url = f"https://qt.gtimg.cn/q={','.join(symbols)}"

    resp = requests.get(url, timeout=10)
    resp.encoding = "gbk"

    result: dict[str, tuple[float, str]] = {}
    for line in resp.text.strip().split("\n"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        # v_sh601006="1~大秦铁路~601006~9.06~..."
        var_part, _, data_part = line.partition("=")
        data_part = data_part.strip('" ;')
        if not data_part:
            continue

        fields = data_part.split("~")
        if len(fields) < 5:
            continue

        name = fields[1]
        bare = fields[2]
        try:
            price = float(fields[3])
        except (ValueError, IndexError):
            continue

        if bare in codes and price > 0:
            result[bare] = (price, name)

    logger.info(f"Tencent: got {len(result)}/{len(codes)} prices")
    return result


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
