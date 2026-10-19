"""Deterministic, locked-rule research on adjusted fractional exposures.

This module fits no ML model and performs no parameter search.  Development
periods are diagnostics, and rolling final-OOS blocks never reset the live
research portfolio.  Adjusted units below are mathematical exposure units,
never exchange shares or a representation of broker-executable fills.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sys
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd

from stopat30m.model.evaluator import compute_portfolio_metrics

POLICY_VERSION = "fixed-momentum-fractional-v1"
STRATEGIES = ("momentum", "equal_weight", "buy_hold")
LIMITATIONS = [
    "固定规则研究，没有训练机器学习模型，也没有根据验证集或最终样本外收益搜索参数。",
    "协议的真实锁定时间见 locked_at；对更早历史的检验属于回溯样本外设计，不是当年预注册的前瞻实验证据。",
    "复权开收盘用于零碎敞口收益研究，虚拟份额不是股票股数；不与未复权价格或整数股撮合混用。",
    "信号只读前一交易日及更早收盘；旧仓位承受隔夜收益，下一交易日开盘调整后承受日内收益。",
    "不模拟实际限价排队、涨跌停、盘中流动性、最小佣金、T+1或完整公司行为账本，不能据此声称券商可执行收益。",
    "费用与滑点为预声明的固定比例，不是历史各期的实际券商费率；现金收益按0计，不使用杠杆。",
    "价格指数基准不含分红且不可直接交易；ETF买入持有及等权基线承担与主策略相同费用和滑点。",
    "未知交易状态不被证明为可成交；仅在有效开收盘存在时进行敞口研究，已知停牌的调仓拒绝执行。",
    "缺失持仓估值或选中标的执行价格会使运行失败；不会前向填充缺价，亦不会把缺价改为现金收益。",
]


@dataclass(frozen=True)
class ResearchConfig:
    train_start: str
    train_end: str
    validation_start: str
    validation_end: str
    oos_start: str
    oos_end: str
    benchmark: str = "SH000300"
    buy_hold_instrument: str = "SH510300"
    lookback_sessions: int = 60
    top_k: int = 1
    rebalance_sessions: int = 20
    fold_sessions: int = 126
    buy_fee_bps: float = 3.0
    sell_fee_bps: float = 3.0
    slippage_bps: float = 2.0
    cash_reserve_pct: float = 0.0
    positive_momentum_only: bool = True
    periods_per_year: int = 252
    risk_free_rate: float = 0.03

    def __post_init__(self):
        for key in ("train_start", "train_end", "validation_start", "validation_end", "oos_start", "oos_end"):
            value = getattr(self, key)
            if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                raise ValueError(f"{key} must be an ISO session-date boundary")
        if not (self.train_start <= self.train_end < self.validation_start <= self.validation_end < self.oos_start <= self.oos_end):
            raise ValueError("train, validation and final OOS must be ordered and strictly disjoint")
        for key in ("lookback_sessions", "top_k", "rebalance_sessions", "fold_sessions", "periods_per_year"):
            value = getattr(self, key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{key} must be a positive integer")
        for key in ("buy_fee_bps", "sell_fee_bps", "slippage_bps"):
            value = getattr(self, key)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or not 0 <= value <= 1000:
                raise ValueError(f"{key} must be finite and within 0..1000 bps")
        if not math.isfinite(self.cash_reserve_pct) or not 0 <= self.cash_reserve_pct < 1:
            raise ValueError("cash_reserve_pct must be in [0, 1)")
        if not isinstance(self.positive_momentum_only, bool):
            raise ValueError("positive_momentum_only must be boolean")
        if not math.isfinite(self.risk_free_rate) or self.risk_free_rate <= -1:
            raise ValueError("risk_free_rate must be finite and greater than -1")
        for instrument in (self.benchmark, self.buy_hold_instrument):
            if not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", instrument):
                raise ValueError(f"Noncanonical instrument: {instrument}")


@dataclass
class ResearchResult:
    path: Path
    report: dict[str, Any]
    manifest: dict[str, Any]
    returns: pd.DataFrame
    folds: pd.DataFrame


def _default_json(value: Any):
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, (pd.Timestamp, date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False, default=_default_json).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _universe_payload(universe: Any) -> dict[str, Any]:
    if hasattr(universe, "as_dict"):
        value = universe.as_dict()
    elif hasattr(universe, "payload"):
        value = universe.payload
    elif hasattr(universe, "manifest"):
        value = universe.manifest
    else:
        raise ValueError("Universe must expose its complete as_dict()/payload/manifest for protocol binding")
    if not isinstance(value, dict) or value.get("kind") not in ("fixed", "index_pit"):
        raise ValueError("Only an explicit fixed research universe or audited index_pit universe is supported; snapshot membership is not historical")
    return json.loads(_canonical(value))


def lock_protocol(config: ResearchConfig, path: str | Path, *, universe: Any) -> dict[str, Any]:
    """Lock rule and pool before reading price data; never overwrite a protocol."""
    pool = _universe_payload(universe)
    binding = {"schema_version": 1, "policy_version": POLICY_VERSION,
               "config": asdict(config), "universe": pool, "universe_sha256": _digest(pool)}
    target = Path(path)
    if target.exists():
        current = load_protocol(target)
        if any(current.get(key) != value for key, value in binding.items()):
            raise ValueError("Protocol already exists with different rules or universe; it cannot be overwritten")
        return current
    payload = {**binding, "locked_at": datetime.now(timezone.utc).isoformat(),
               "historical_oos_is_retrospective": date.fromisoformat(config.oos_start) < datetime.now(timezone.utc).date()}
    payload["protocol_sha256"] = _digest(payload)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n")
    return payload


def load_protocol(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    checksum = payload.get("protocol_sha256")
    content = {key: value for key, value in payload.items() if key != "protocol_sha256"}
    if checksum != _digest(content):
        raise ValueError("Protocol checksum mismatch: rules or declaration were modified after locking")
    if payload.get("schema_version") != 1 or payload.get("policy_version") != POLICY_VERSION:
        raise ValueError("Unsupported locked research policy")
    if payload.get("universe_sha256") != _digest(payload.get("universe")):
        raise ValueError("Protocol has no valid universe binding")
    allowed = {field.name for field in fields(ResearchConfig)}
    if set(payload.get("config", {})) != allowed:
        raise ValueError("Protocol config must explicitly contain every research parameter")
    ResearchConfig(**payload["config"])
    return payload


def _dates(value: Any, label: str) -> pd.DatetimeIndex:
    try:
        result = pd.DatetimeIndex(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {label} dates") from exc
    if result.empty or result.hasnans or result.tz is not None or not result.equals(result.normalize()):
        raise ValueError(f"{label} requires nonempty, timezone-naive session dates")
    if result.has_duplicates or not result.is_monotonic_increasing:
        raise ValueError(f"{label} dates must be sorted and unique")
    return result


def _prices(value: Any, calendar: pd.DatetimeIndex, label: str) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame) or value.empty:
        raise ValueError(f"{label} prices are empty")
    result = value.copy()
    result.index = _dates(result.index, label)
    if result.columns.has_duplicates or any(not isinstance(column, str) or not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", column) for column in result.columns):
        raise ValueError(f"{label} requires unique canonical instrument columns")
    if not result.index.isin(calendar).all():
        raise ValueError(f"{label} contains dates outside the declared exchange calendar")
    try:
        result = result.apply(pd.to_numeric, errors="raise").astype(float)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{label} contains nonnumeric prices") from exc
    present = result.to_numpy()[~np.isnan(result.to_numpy())]
    if not np.isfinite(present).all() or (present <= 0).any():
        raise ValueError(f"{label} contains nonpositive or nonfinite prices")
    return result.reindex(index=calendar, columns=sorted(result.columns))  # Deliberately no fill.


def _frame_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=True, index_label="date", date_format="%Y-%m-%d", float_format="%.17g", na_rep="").encode("utf-8")


def _valid_price(prices: pd.DataFrame, day: pd.Timestamp, instrument: str) -> float:
    value = prices.at[day, instrument] if instrument in prices.columns and day in prices.index else np.nan
    if not np.isfinite(value) or value <= 0:
        raise ValueError(f"Missing or invalid required price: {day.date()} {instrument}")
    return float(value)


def _status_map(artifact: Any) -> dict[tuple[pd.Timestamp, str], Any]:
    frame = getattr(artifact, "prices", None)
    if not isinstance(frame, pd.DataFrame) or not {"date", "instrument", "tradestatus"}.issubset(frame.columns):
        return {}
    result = {}
    for row in frame[["date", "instrument", "tradestatus"]].itertuples(index=False):
        key = (pd.Timestamp(row.date), str(row.instrument))
        if key in result:
            raise ValueError("Duplicate long-form daily prices/status rows")
        result[key] = row.tradestatus
    return result


def _suspended(status: Any) -> bool:
    return str(status).strip().lower() in {"0", "0.0", "false", "suspended", "halted"}


def _members(universe: Any, day: pd.Timestamp, known_on: pd.Timestamp) -> list[str]:
    result = universe.members_on(day.strftime("%Y-%m-%d"), known_on=known_on.strftime("%Y-%m-%d"))
    return sorted(set(result))


def _target_weights(strategy: str, execution: pd.Timestamp, location: int, calendar: pd.DatetimeIndex,
                    close: pd.DataFrame, universe: Any, config: ResearchConfig,
                    statuses: dict, signals: list[dict], target_callback: Callable | None = None) -> dict[str, float]:
    signal_day = calendar[location - 1]
    known = set(_members(universe, signal_day, signal_day))
    effective = set(_members(universe, execution, signal_day))
    members = sorted(known & effective)
    if not members:
        raise ValueError(f"Universe has no eligible members known before execution on {execution.date()}")
    if any(instrument not in close.columns for instrument in members):
        raise ValueError(f"Universe contains instruments absent from the price artifact on {execution.date()}")
    usable = [instrument for instrument in members if not _suspended(statuses.get((execution, instrument)))]
    if target_callback is not None:
        weights, rows = target_callback(signal_day, execution, usable)
        if not isinstance(weights, dict) or any(
            instrument not in usable or not isinstance(weight, (float, int))
            or not math.isfinite(weight) or weight < 0
            for instrument, weight in weights.items()
        ):
            raise ValueError("External strategy returned invalid or ineligible weights")
        if len(weights) > config.top_k or sum(weights.values()) > 1 - config.cash_reserve_pct + 1e-10:
            raise ValueError("External strategy exceeds the locked portfolio constraints")
        for row in rows:
            if not isinstance(row, dict) or row.get("instrument") not in members:
                raise ValueError("External strategy returned an invalid signal record")
            signals.append({**row, "date": execution, "signal_date": signal_day})
        return weights
    if strategy == "buy_hold":
        if config.buy_hold_instrument not in usable:
            raise ValueError("Buy-and-hold reference must be an eligible member of the locked universe")
        selected = [config.buy_hold_instrument]
    elif strategy == "equal_weight":
        if not usable:
            raise ValueError(f"No eligible open-session exposures on {execution.date()}")
        selected = usable
    else:
        start = location - 1 - config.lookback_sessions
        if start < 0:
            raise ValueError("Insufficient historical warmup before the first signal")
        scores = []
        for instrument in members:
            window = close.iloc[start:location][instrument]
            valid = len(window) == config.lookback_sessions + 1 and window.notna().all()
            score = float(window.iloc[-1] / window.iloc[0] - 1) if valid else None
            reason = "eligible"
            if not valid:
                reason = "missing_history"
            elif instrument not in usable:
                reason = "known_suspended"
            elif config.positive_momentum_only and score <= 0:
                reason = "nonpositive_momentum"
            else:
                scores.append((instrument, score))
            signals.append({"date": execution, "signal_date": signal_day, "instrument": instrument,
                            "lookback_start": calendar[start], "score": score, "reason": reason})
        if not any(row["score"] is not None for row in signals if row["date"] == execution):
            raise ValueError(f"All momentum histories are missing on {signal_day.date()}")
        scores.sort(key=lambda item: (-item[1], item[0]))  # Explicit stable tie break.
        selected = [instrument for instrument, _ in scores[:config.top_k]]
        for row in signals:
            if row["date"] == execution:
                row["selected"] = row["instrument"] in selected
    if not selected:
        return {}  # A nonpositive momentum score intentionally holds cash.
    weight = (1 - config.cash_reserve_pct) / len(selected)
    return {instrument: weight for instrument in selected}


def _rebalance(cash: float, units: dict[str, float], day: pd.Timestamp, signal_day: pd.Timestamp,
               opens: pd.DataFrame, weights: dict[str, float], config: ResearchConfig,
               statuses: dict, trades: list[dict]) -> tuple[float, dict[str, float], dict[str, float]]:
    instruments = sorted(set(units) | set(weights))
    prices = {instrument: _valid_price(opens, day, instrument) for instrument in instruments}
    old = {instrument: units.get(instrument, 0.0) * prices[instrument] for instrument in instruments}
    equity = cash + sum(old.values())
    buy_rate = (config.buy_fee_bps + config.slippage_bps) / 10000
    sell_rate = (config.sell_fee_bps + config.slippage_bps) / 10000

    def deltas(post_cost_equity):
        return {instrument: weights.get(instrument, 0.0) * post_cost_equity - old[instrument] for instrument in instruments}

    def cost(post_cost_equity):
        return sum(max(delta, 0) * buy_rate + max(-delta, 0) * sell_rate for delta in deltas(post_cost_equity).values())

    low, high = 0.0, equity
    for _ in range(80):
        midpoint = (low + high) / 2
        if midpoint + cost(midpoint) > equity:
            high = midpoint
        else:
            low = midpoint
    post = (low + high) / 2
    changes = deltas(post)
    buy = sell = fees = slippage = 0.0
    for instrument, delta in changes.items():
        if abs(delta) <= equity * 1e-12:
            continue
        if _suspended(statuses.get((day, instrument))):
            raise ValueError(f"Cannot rebalance known suspended exposure {day.date()} {instrument}")
        fee = abs(delta) * (config.buy_fee_bps if delta > 0 else config.sell_fee_bps) / 10000
        slip = abs(delta) * config.slippage_bps / 10000
        fees += fee
        slippage += slip
        buy += max(delta, 0)
        sell += max(-delta, 0)
        trades.append({"date": day, "signal_date": signal_day, "instrument": instrument,
                       "direction": "BUY" if delta > 0 else "SELL", "adjusted_open": prices[instrument],
                       "notional": abs(delta), "fee_cost": fee, "slippage_cost": slip,
                       "virtual_unit_change": delta / prices[instrument]})
    target_values = {instrument: weight * post for instrument, weight in weights.items()}
    new_units = {instrument: value / prices[instrument] for instrument, value in target_values.items() if value > 0}
    new_cash = post - sum(target_values.values())
    if new_cash < -1e-10 or not math.isclose(equity - post, fees + slippage, rel_tol=1e-9, abs_tol=1e-12):
        raise ValueError("Research cost allocation did not reconcile")
    return max(0.0, new_cash), new_units, {"pre_trade_nav": equity, "buy_notional": buy, "sell_notional": sell,
        "fee_cost": fees, "slippage_cost": slippage, "turnover": (buy + sell) / equity / 2,
        "gross_traded_weight": (buy + sell) / equity}


def _simulate(days: pd.DatetimeIndex, opens: pd.DataFrame, close: pd.DataFrame,
              calendar: pd.DatetimeIndex, universe: Any, config: ResearchConfig,
              strategy: str, statuses: dict, *, target_callback: Callable | None = None,
              known_halts: Mapping | None = None
              ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cash, previous_nav = 1.0, 1.0
    units: dict[str, float] = {}
    records, positions, trades, signals = [], [], [], []
    locations = calendar.get_indexer(days)
    halts, pending_rebalance = known_halts or {}, False

    def mark(frame, day, instrument):
        if (day, instrument) not in halts:
            return _valid_price(frame, day, instrument)
        previous = close.loc[close.index < day, instrument].dropna()
        if previous.empty:
            raise ValueError("Verified suspension has no preceding valid valuation quote")
        return float(previous.iloc[-1])

    for sequence, (day, location) in enumerate(zip(days, locations)):
        if location < config.lookback_sessions + 1:
            raise ValueError("Insufficient historical warmup before the first signal")
        previous_day = calendar[location - 1]
        # A held exposure must have both close-to-open and open-to-close marks.
        for instrument in units:
            mark(close, previous_day, instrument)
        pre_trade_nav = cash + sum(quantity * mark(opens, day, instrument) for instrument, quantity in units.items())
        overnight_return = pre_trade_nav / previous_nav - 1
        ledger = {"pre_trade_nav": pre_trade_nav, "buy_notional": 0.0, "sell_notional": 0.0,
                  "fee_cost": 0.0, "slippage_cost": 0.0, "turnover": 0.0, "gross_traded_weight": 0.0}
        rebalance = pending_rebalance or sequence == 0 or (strategy != "buy_hold" and sequence % config.rebalance_sessions == 0)
        deferred = rebalance and any((day, instrument) in halts for instrument in units)
        if deferred:
            pending_rebalance, rebalance = True, False
        if rebalance:
            weights = _target_weights(strategy, day, location, calendar, close, universe, config, statuses, signals, target_callback)
            cash, units, ledger = _rebalance(cash, units, day, previous_day, opens, weights, config, statuses, trades)
            pending_rebalance = False
        nav = cash + sum(quantity * mark(close, day, instrument) for instrument, quantity in units.items())
        if not math.isfinite(nav) or nav <= 0:
            raise ValueError(f"Invalid research NAV on {day.date()}")
        post_cost_nav = pre_trade_nav - ledger["fee_cost"] - ledger["slippage_cost"]
        records.append({"date": day, "signal_date": previous_day, "strategy": strategy,
                        "nav": nav, "net_return": nav / previous_nav - 1,
                        "gross_return_before_cost": (1 + overnight_return) * (nav / post_cost_nav) - 1,
                        "overnight_return": overnight_return, "intraday_return": nav / post_cost_nav - 1,
                        "cash": cash, "rebalance": rebalance, "positions_count": len(units), **ledger})
        if halts:
            records[-1]["rebalance_deferred_for_verified_halt"] = bool(deferred)
            records[-1]["carried_valuation_instruments"] = ",".join(instrument for instrument in sorted(units) if (day, instrument) in halts)
        for instrument, quantity in sorted(units.items()):
            value = quantity * mark(close, day, instrument)
            positions.append({"date": day, "instrument": instrument, "virtual_units": quantity,
                              "market_value": value, "weight": value / nav})
        previous_nav = nav
    frame = pd.DataFrame(records).set_index("date")
    return frame, pd.DataFrame(positions), pd.DataFrame(trades), pd.DataFrame(signals)


def _benchmark(days: pd.DatetimeIndex, opens: pd.DataFrame, close: pd.DataFrame, instrument: str) -> pd.Series:
    values = []
    previous_close = None
    for day in days:
        end = _valid_price(close, day, instrument)
        start = _valid_price(opens, day, instrument) if previous_close is None else previous_close
        values.append(end / start - 1)
        previous_close = end
    return pd.Series(values, index=days, name="benchmark_return")


def _metrics(frame: pd.DataFrame, benchmark: pd.Series, config: ResearchConfig) -> dict[str, Any]:
    result = compute_portfolio_metrics(frame["net_return"], benchmark, risk_free_rate=config.risk_free_rate, periods_per_year=config.periods_per_year)
    result.update({"start": frame.index[0].date().isoformat(), "end": frame.index[-1].date().isoformat(),
                   "fees_in_initial_nav_units": float(frame.fee_cost.sum()),
                   "slippage_in_initial_nav_units": float(frame.slippage_cost.sum()),
                   "one_way_turnover_sum": float(frame.turnover.sum()),
                   "mean_daily_one_way_turnover": float(frame.turnover.mean()),
                   "entry_or_rebalance_days": int(frame.rebalance.sum()),
                   "cost_note": "Fees/slippage are summed notional losses relative to this split's initial NAV=1; fold costs retain whole-OOS initial units."})
    return result


def _audit_universe(universe: Any, config: ResearchConfig, payload: dict) -> dict:
    audit = universe.audit(config.train_start, config.oos_end, require_pit=payload["kind"] == "index_pit")
    if not isinstance(audit, dict):
        raise ValueError("Historical universe audit did not return a report")
    return audit


def _validate_source(artifact: Any, universe_payload: dict, config: ResearchConfig) -> dict:
    manifest = getattr(artifact, "manifest", {})
    if not isinstance(manifest, dict):
        raise ValueError("Price artifact manifest must be an object")
    declaration = universe_payload.get("metadata", {}).get("research_data_request")
    request = manifest.get("request", {})
    if declaration:
        if manifest.get("provider", {}).get("name") != declaration.get("source"):
            raise ValueError("Price provider differs from the locked data request")
        if declaration.get("calendar_source") and manifest.get("calendar", {}).get("source") != declaration["calendar_source"]:
            raise ValueError("Calendar provider differs from the locked data request")
        for key in ("start_date", "end_date", "adjustment"):
            if request.get(key) != declaration.get(key):
                raise ValueError(f"Price request {key} differs from the locked protocol")
        if sorted(request.get("instruments", [])) != sorted(declaration.get("instruments", [])):
            raise ValueError("Price instruments differ from the locked data request")
    # A request ending on a holiday is valid; require declared natural-day
    # coverage rather than pretending the last exchange session is that day.
    if request.get("start_date") and request["start_date"] > config.train_start:
        raise ValueError("Price request begins after the declared training period")
    if request.get("end_date") and request["end_date"] < config.oos_end:
        raise ValueError("Price request ends before the declared OOS period")
    source_path = Path(getattr(artifact, "path", "."))
    for filename, record in manifest.get("files", {}).items():
        path = source_path / filename
        if Path(filename).name != filename or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != record.get("sha256"):
            raise ValueError(f"Price snapshot checksum mismatch: {filename}")
    if getattr(artifact, "manifest_sha256", None):
        path = source_path / "manifest.json"
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != artifact.manifest_sha256:
            raise ValueError("Price snapshot manifest checksum mismatch")
        if json.loads(path.read_text(encoding="utf-8")) != manifest:
            raise ValueError("In-memory price manifest differs from the immutable snapshot")
    return json.loads(_canonical(manifest))


def run_walk_forward(artifact: Any, universe: Any, protocol_path: str | Path, output_dir: str | Path,
                     *, additional_strategies: Mapping[str, Callable] | None = None,
                     comparison_metadata: dict | None = None, known_halts: Mapping | None = None) -> ResearchResult:
    """Evaluate three locked baselines; input objects are never mutated or filled.

    ``output_dir`` is a new, exact result directory, not a shared output root.
    Calling this function again requires a different output directory; canonical
    report/CSV checksums will match for the same data, protocol and code.
    """
    protocol = load_protocol(protocol_path)
    config = ResearchConfig(**protocol["config"])
    extra = dict(additional_strategies or {})
    if any(not re.fullmatch(r"[a-z][a-z0-9_]*", name) or name in STRATEGIES or not callable(callback)
           for name, callback in extra.items()):
        raise ValueError("Additional strategies require unique safe names and callable targets")
    comparison = json.loads(_canonical(comparison_metadata or {}))
    universe_payload = _universe_payload(universe)
    if _digest(universe_payload) != protocol["universe_sha256"]:
        raise ValueError("Universe differs from the pool locked before the research run")
    audit = _audit_universe(universe, config, universe_payload)
    source_manifest = _validate_source(artifact, universe_payload, config)
    calendar = _dates(artifact.calendar, "Exchange calendar")
    opens = _prices(artifact.adjusted_open, calendar, "Adjusted open")
    close = _prices(artifact.adjusted_close, calendar, "Adjusted close")
    if set(opens.columns) != set(close.columns):
        raise ValueError("Adjusted open and close instruments must agree")
    if config.benchmark not in close.columns or config.buy_hold_instrument not in close.columns:
        raise ValueError("The locked price-index and buy-hold comparators must be present")
    if calendar[0].date() > date.fromisoformat(config.train_start):
        raise ValueError("Calendar does not cover every declared research split")
    if not source_manifest.get("request", {}).get("end_date") and calendar[-1].date() < date.fromisoformat(config.oos_end):
        raise ValueError("Calendar does not cover the declared OOS period")
    statuses = _status_map(artifact)
    halts = dict(known_halts or {})
    halt_records = []
    for (day, instrument), evidence in sorted(halts.items()):
        if day not in calendar or instrument not in close.columns or not isinstance(evidence, dict):
            raise ValueError("Invalid verified suspension identity")
        if evidence.get("not_tradeable") is not True or not str(evidence.get("source_url", "")).startswith("https://"):
            raise ValueError("Verified suspensions require explicit source-backed nontradeability")
        if pd.notna(close.at[day, instrument]) or pd.notna(opens.at[day, instrument]):
            raise ValueError("Declared full-day suspension conflicts with observed OHLC")
        statuses[(day, instrument)] = "0"
        halt_records.append({**evidence, "date": day.date().isoformat(), "instrument": instrument})
    splits = {"train": (config.train_start, config.train_end), "validation": (config.validation_start, config.validation_end), "final_oos": (config.oos_start, config.oos_end)}
    all_returns, all_positions, all_trades, all_signals, fold_rows = [], [], [], [], []
    report = {"policy_version": POLICY_VERSION, "model_fit": bool(extra), "parameter_search": False,
              "protocol_sha256": protocol["protocol_sha256"], "locked_at": protocol["locked_at"],
              "historical_oos_is_retrospective": protocol["historical_oos_is_retrospective"],
              "execution_mode": "adjusted_fractional_exposure_research", "execution_ready": False,
              "selection_rule": "previous-session close momentum; positive-only to cash; deterministic instrument tie break",
              "benchmark": {"instrument": config.benchmark, "price_index_not_total_return": True, "costs_applied": False,
                            "first_day": "first OOS/development session open to close, then close to close"},
              "universe_audit": audit, "limitations": list(dict.fromkeys(LIMITATIONS + list(audit.get("limitations", [])) + list(source_manifest.get("limitations", [])))), "splits": {}}
    if extra:
        report["comparison"] = comparison
        report["limitations"][0] = "固定动量基线与独立冻结的模型作同成本对照；模型配置和训练范围见 comparison，不按验证段或最终段收益选参数。"
        report["training_metrics_are_in_sample"] = True
    if halts:
        report["verified_halts"] = halt_records
        report["valuation_policy"] = "Only issuer-verified full-day halts carry the last valid close for valuation; no OHLC/volume is filled or traded; held halts defer rebalance. All other missing prices fail."
        report["limitations"].append(report["valuation_policy"])
    for split, (start, end) in splits.items():
        days = calendar[(calendar >= start) & (calendar <= end)]
        if days.empty:
            raise ValueError(f"Research split {split} contains no exchange sessions")
        bench = _benchmark(days, opens, close, config.benchmark)
        split_report = {}
        for strategy in (*STRATEGIES, *extra):
            result, positions, trades, signals = _simulate(days, opens, close, calendar, universe, config, strategy, statuses,
                                                         target_callback=extra.get(strategy), known_halts=halts)
            result["benchmark_return"] = bench
            result["benchmark_nav"] = (1 + bench).cumprod()
            result["split"] = split
            split_report[strategy] = _metrics(result, bench, config)
            split_report[strategy]["total_trades"] = len(trades)
            all_returns.append(result.reset_index())
            for frame, target in ((positions, all_positions), (trades, all_trades), (signals, all_signals)):
                if not frame.empty:
                    frame = frame.copy()
                    frame["strategy"], frame["split"] = strategy, split
                    target.append(frame)
            if split == "final_oos":
                for ordinal, offset in enumerate(range(0, len(result), config.fold_sessions), 1):
                    fold = result.iloc[offset:offset + config.fold_sessions]
                    metrics = _metrics(fold, bench.reindex(fold.index), config)
                    metrics["total_trades"] = int(((trades["date"] >= fold.index[0]) & (trades["date"] <= fold.index[-1])).sum()) if not trades.empty else 0
                    before = 1.0 if offset == 0 else float(result.nav.iloc[offset - 1])
                    fold_rows.append({"strategy": strategy, "fold": ordinal, "start_nav": before,
                                      "end_nav": float(fold.nav.iloc[-1]), "portfolio_reset": False,
                                      **metrics})
        report["splits"][split] = split_report
    returns = pd.concat(all_returns, ignore_index=True)
    folds = pd.DataFrame(fold_rows)
    report["rolling_final_oos"] = folds.astype(object).where(pd.notna(folds), None).to_dict(orient="records")
    input_checksums = {"adjusted_open": hashlib.sha256(_frame_bytes(opens)).hexdigest(),
                       "adjusted_close": hashlib.sha256(_frame_bytes(close)).hexdigest(),
                       "calendar": _digest([day.date().isoformat() for day in calendar]),
                       "statuses": _digest([[str(key[0].date()), key[1], None if pd.isna(value) else str(value)] for key, value in sorted(statuses.items())])}
    package = Path(__file__).resolve().parent.parent
    code_checksums = {name: hashlib.sha256((package / name).read_bytes()).hexdigest() for name in (
        "backtest/walk_forward.py", "model/evaluator.py", "data/historical_universe.py",
        "data/research_calendar.py", "data/research_prices.py",
    ) if (package / name).is_file()}
    code_checksum = code_checksums["backtest/walk_forward.py"]
    if extra:
        model_source = package / "backtest/model_comparison.py"
        if model_source.is_file():
            code_checksums["backtest/model_comparison.py"] = hashlib.sha256(model_source.read_bytes()).hexdigest()
    environment = {"python": sys.version.split()[0], "pandas": pd.__version__, "numpy": np.__version__}
    fingerprint = _digest({"inputs": input_checksums, "source_manifest": source_manifest,
                           "protocol": protocol["protocol_sha256"], "code": code_checksums, "environment": environment,
                           **({"verified_halts": halt_records} if halts else {}),
                           **({"comparison": comparison} if extra else {})})
    report["deterministic_fingerprint"] = fingerprint
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    outputs = {
        "returns.csv": returns,
        "folds.csv": folds,
        "positions.csv": pd.concat(all_positions, ignore_index=True) if all_positions else pd.DataFrame(columns=["date", "instrument", "virtual_units", "market_value", "weight", "strategy", "split"]),
        "trades.csv": pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame(columns=["date", "signal_date", "instrument", "direction", "adjusted_open", "notional", "fee_cost", "slippage_cost", "virtual_unit_change", "strategy", "split"]),
        "signals.csv": pd.concat(all_signals, ignore_index=True) if all_signals else pd.DataFrame(columns=["date", "signal_date", "instrument", "lookback_start", "score", "reason", "strategy", "split"]),
    }
    for name, frame in outputs.items():
        frame.to_csv(output / name, index=False, date_format="%Y-%m-%d", float_format="%.17g", na_rep="")
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False, default=_default_json) + "\n", encoding="utf-8")
    (output / "protocol.json").write_text(json.dumps(protocol, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    output_checksums = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(output.iterdir()) if path.is_file()}
    run_manifest = {"schema_version": 1, "run_id": fingerprint[:24], "deterministic_fingerprint": fingerprint,
                    "generated_at": datetime.now(timezone.utc).isoformat(), "policy_version": POLICY_VERSION,
                    "engine_sha256": code_checksum, "protocol_sha256": protocol["protocol_sha256"],
                    "code_checksums": code_checksums, "environment": environment,
                    "universe_sha256": protocol["universe_sha256"], "input_checksums": input_checksums,
                    "source_snapshot": str(getattr(artifact, "path", "")), "source_manifest": source_manifest,
                    "source_manifest_sha256": getattr(artifact, "manifest_sha256", None),
                    "output_checksums": output_checksums, "config": asdict(config),
                    "numeric_basis": "IEEE-754 float64, deterministic ordering; virtual adjusted units; initial NAV=1 per development split and final OOS"}
    (output / "manifest.json").write_text(json.dumps(run_manifest, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False, default=_default_json) + "\n", encoding="utf-8")
    return ResearchResult(output, report, run_manifest, returns, folds)
