"""Version-bound Qlib Alpha158 A-share experiment, separate from ETF research.

Package-declared historical constituent intervals are retained as intervals.
They are not proof of announcement-time PIT eligibility. All returns, when
complete required quotes permit them, are fractional research exposures rather
than broker share/cash ledgers. Missing execution/valuation data stops comparison.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd

from stopat30m.backtest import walk_forward as wf
from stopat30m.factors.handler import purge_label_segments
from stopat30m.model.evaluator import compute_portfolio_metrics, evaluate_predictions
from stopat30m.model.trainer import FixedQlibLGBSpec, fit_fixed_qlib_lightgbm, load_fixed_qlib_model

REQUIRED_FIELDS = ("open", "high", "low", "close", "volume", "vwap", "factor")
POLICY = "qlib-alpha158-ashare-open5-v1"
QUOTE_POLICY = "package-missing-market-fields-retained-factor-v2"
ACTION_POLICY = "verified-share-exchange-at-first-confirmed-tradable-open-v1"
LIMITATIONS = [
    "package_declared_membership_not_full_PIT: historical package intervals are used; announcement known_at/effective timing has not been independently verified.",
    "Package price/volume/factor normalization and incremental source bridges require the snapshot audit; the package is community supplied, not an official exchange historical database.",
    "Fractional adjusted-price return exposures are a research model, not integer A-share execution or a reconciled broker cash/share ledger.",
    "Board-specific price limits, ST status, suspension executability, T+1, lot sizes, minimum commission and changing historical stamp tax are not fully modelled; constant declared costs are assumptions.",
    "Package bars with all OHLC/VWAP missing and volume missing/zero are unavailable, even when the supplier retains its forward-filled adjustment factor: no trades, held exposures retain their last observed valuation for at most 20 sessions; partial damage/long unknown gaps stop comparison.",
    "Blocked orders keep their prior-close target weights and are retried after the open becomes available; intervening prices may cause additional partial rebalancing costs. No stale mark is used as an execution price.",
    "Explicit verified share exchanges, when supplied, retain suspended entitlements at the last observed mark until the first confirmed tradable session. That stale mark is not a liquid or independently fair valuation; scheduled dates alone never authorize conversion. The action ledger is not proof of complete corporate-action coverage.",
    "This is a retrospectively designed historical experiment. No return target, future profitability or historical pre-registration is claimed.",
]


@dataclass(frozen=True)
class AshareExperimentConfig:
    oos_end: str
    train_start: str = "2012-01-01"
    train_end: str = "2022-12-31"
    valid_start: str = "2023-01-01"
    valid_end: str = "2024-12-31"
    oos_start: str = "2025-01-01"
    top_k: int = 20
    rebalance_sessions: int = 5
    buy_fee_bps: float = 3.1
    sell_fee_bps: float = 8.1
    slippage_bps: float = 2.0

    def __post_init__(self):
        for name in ("train_start", "train_end", "valid_start", "valid_end", "oos_start", "oos_end"):
            value = getattr(self, name)
            if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                raise ValueError("Experiment boundaries must be explicit ISO dates")
        if not self.train_start <= self.train_end < self.valid_start <= self.valid_end < self.oos_start <= self.oos_end:
            raise ValueError("Train, validation and OOS must be strictly ordered and disjoint")
        if isinstance(self.top_k, bool) or not isinstance(self.top_k, int) or self.top_k <= 0:
            raise ValueError("top_k must be a positive integer")
        if self.rebalance_sessions != 5 or isinstance(self.rebalance_sessions, bool):
            raise ValueError("This fixed experiment uses the predeclared five-session holding horizon")
        for name in ("buy_fee_bps", "sell_fee_bps", "slippage_bps"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not np.isfinite(value) or not 0 <= value <= 1000:
                raise ValueError("Invalid declared research cost")


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _clean(value):
    if isinstance(value, dict):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return value


def _write(path, value):
    Path(path).write_text(json.dumps(_clean(value), ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")


class VerifiedCorporateActions:
    """Evidence-bound raw-share conversions, with separate public-knowledge times.

    Conversion is delayed until the confirmed new shares can trade. Until then
    the old position represents a non-tradable entitlement at an explicit stale
    research mark. This avoids selling newly issued locked shares against quotes
    for the acquirer's already outstanding shares. It does not value that claim
    at an observed market price before admission.
    """
    def __init__(self, path=None):
        self.path = Path(path).resolve() if path is not None else None
        self.sha256 = _sha(self.path) if self.path is not None else None
        self.payload = {"schema_version": 1, "actions": [], "evidence": []}
        self.actions = []
        if self.path is None:
            return
        self.payload = json.loads(self.path.read_text())
        if self.payload.get("schema_version") != 1 or not isinstance(self.payload.get("actions"), list):
            raise ValueError("Verified corporate-action document schema is invalid")
        evidence = {}
        for item in self.payload.get("evidence", []):
            source = Path(item["path"])
            if not source.is_absolute():
                source = self.path.parent / source
            if item["id"] in evidence or not re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", "")) or _sha(source) != item["sha256"]:
                raise ValueError("Corporate-action source evidence checksum mismatch")
            if not item.get("url", "").startswith("https://"):
                raise ValueError("Corporate actions require a public source URL")
            evidence[item["id"]] = item
        old_symbols, identities = set(), set()
        for item in self.payload["actions"]:
            if item.get("type") != "share_exchange" or not item.get("id") or item["id"] in identities:
                raise ValueError("Only uniquely identified verified share exchanges are supported")
            old, new = item.get("from_instrument", ""), item.get("to_instrument", "")
            if any(not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", inst) for inst in (old, new)) or old == new or old in old_symbols:
                raise ValueError("Ambiguous or invalid corporate-action instruments")
            ratio = item.get("ratio")
            if isinstance(ratio, bool) or not isinstance(ratio, (int, float)) or not np.isfinite(ratio) or ratio <= 0:
                raise ValueError("Share-exchange ratio must be new raw shares per old raw share")
            if item.get("ratio_basis") not in {"new_raw_A_shares_per_old_raw_A_share", "new_raw_shares_per_old_raw_share"}:
                raise ValueError("Share exchange must explicitly declare the raw-share ratio basis")
            ratio_known = pd.Timestamp(item.get("ratio_known_at"))
            if pd.isna(ratio_known) or ratio_known.tzinfo is None or not item.get("ratio_evidence_ids") or any(ref not in evidence for ref in item["ratio_evidence_ids"]):
                raise ValueError("Raw-share ratio requires its own evidence and timezone-aware known_at")
            for phase in ("suspension", "delisting", "confirmation"):
                fact = item.get(phase, {})
                known = pd.Timestamp(fact.get("known_at"))
                if pd.isna(known) or known.tzinfo is None:
                    raise ValueError("Corporate-action known_at must include its timezone")
                refs = fact.get("evidence_ids", [])
                if not refs or any(ref not in evidence for ref in refs):
                    raise ValueError("Every executed corporate-action fact needs verified source bytes")
            for phase, key in (("suspension", "effective_date"), ("delisting", "effective_date"), ("confirmation", "tradable_from")):
                value = item[phase].get(key)
                if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                    raise ValueError("Corporate-action effective dates must be ISO dates")
            if not item["suspension"]["effective_date"] <= item["delisting"]["effective_date"] <= item["confirmation"]["tradable_from"]:
                raise ValueError("Corporate-action suspension/delisting/tradability order is invalid")
            old_symbols.add(old); identities.add(item["id"])
            self.actions.append(item)
        if old_symbols & {item["to_instrument"] for item in self.actions}:
            raise ValueError("Chained share exchanges require an explicit separate accounting policy")

    @staticmethod
    def _known(fact, day, *, close=False):
        cutoff = pd.Timestamp(day).normalize().tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15 if close else 9, minutes=0 if close else 30)
        return pd.Timestamp(fact["known_at"]) <= cutoff

    def suspended(self, instrument, day):
        return next((item for item in self.actions if item["from_instrument"] == instrument
                     and str(pd.Timestamp(day).date()) >= item["suspension"]["effective_date"]
                     and self._known(item["suspension"], day)), None)

    def excluded(self, instrument, day, *, close=False):
        return any(item["from_instrument"] == instrument
                   and str(pd.Timestamp(day).date()) >= item["delisting"]["effective_date"]
                   and self._known(item["delisting"], day, close=close) for item in self.actions)

    def conversions(self, day):
        return [item for item in self.actions if str(pd.Timestamp(day).date()) >= item["confirmation"]["tradable_from"]
                and self._known(item["confirmation"], day) and self._known({"known_at": item["ratio_known_at"]}, day)]

    def assert_unchanged(self):
        if self.path is not None and _sha(self.path) != self.sha256:
            raise ValueError("Corporate-action document changed during comparison")
        # Re-read source bytes too: a copied document alone is not the evidence.
        if self.path is not None:
            VerifiedCorporateActions(self.path)

    def describe(self):
        evidence = [{key: item.get(key) for key in ("id", "url", "path", "sha256", "evidence_kind", "original_pdf_sha256", "pages", "pdf_pages_1_based")}
                    for item in self.payload.get("evidence", [])]
        extracts = any(item.get("evidence_kind") == "official_pdf_text_extract_via_web" for item in evidence)
        return {"policy": ACTION_POLICY, "path": str(self.path) if self.path else None,
                "sha256": self.sha256, "event_ids": [item["id"] for item in self.actions],
                "coverage_complete": False, "execution_ready": False, "evidence": evidence,
                "evidence_limitation": ("Official issuer PDF text/facts were inspected through the web reader. Hashes bind only saved local extracts/interpretations, not original PDF bytes; original PDFs were not downloaded or byte-checksummed."
                                        if extracts else None)}


def _action_price_inputs(actions, dataset, instruments):
    symbols = sorted(set(instruments) | {item["to_instrument"] for item in actions.actions})
    hashes = {}
    for symbol in sorted(set(symbols) - set(instruments)):
        for field in REQUIRED_FIELDS:
            name = f"features/{symbol.lower()}/{field}.day.bin"
            if not (dataset / name).is_file():
                raise ValueError("Share-exchange successor lacks required source price/factor fields")
            hashes[name] = _sha(dataset / name)
    return symbols, hashes


def _fitted_processors(handler):
    """Persist training-only normalizer state for reproducible future inference."""
    states = []
    for processor in handler.infer_processors:
        record = {"class": type(processor).__name__}
        for name in ("fit_start_time", "fit_end_time", "fields_group", "clip_outlier"):
            if hasattr(processor, name):
                record[name] = _clean(getattr(processor, name))
        for name in ("mean_train", "std_train"):
            if hasattr(processor, name):
                record[name] = _clean(np.asarray(getattr(processor, name)).tolist())
        states.append(record)
    return states


class PackageHistoricalPool:
    """Historical effective intervals, never today's members backdated.

    When an execution date is requested before it is known, use the preceding
    signal day's package interval. Future entries/exits are not previewed.
    """
    def __init__(self, path, *, start=None, end=None):
        self.path = Path(path)
        self.intervals = {}
        self.clipped_non_numeric_intervals = []
        first = pd.Timestamp(start) if start is not None else None
        last = pd.Timestamp(end) if end is not None else None
        for line in self.path.read_text().splitlines():
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) != 3 or not re.fullmatch(r"(?:SH|SZ|BJ)[A-Z0-9]{6}", parts[0].upper()):
                raise ValueError("Historical constituent file must contain symbol/start/end intervals")
            symbol, start, end = parts[0].upper(), pd.Timestamp(parts[1]), pd.Timestamp(parts[2])
            if pd.isna(start) or pd.isna(end) or start > end:
                raise ValueError("Invalid historical constituent interval")
            overlaps = (first is None or end >= first) and (last is None or start <= last)
            if not re.fullmatch(r"(?:SH|SZ|BJ)\d{6}", symbol):
                if overlaps:
                    raise ValueError("A nonnumeric historical alias intersects this experiment; explicit source mapping is required")
                self.clipped_non_numeric_intervals.append({"instrument": symbol, "start": str(start.date()), "end": str(end.date()), "reason": "outside_experiment_dates"})
                continue
            if not overlaps:
                continue
            self.intervals.setdefault(symbol, []).append((start, end))
        if not self.intervals:
            raise ValueError("Historical constituent file is empty")
        for intervals in self.intervals.values():
            intervals.sort()
            if any(left[1] >= right[0] for left, right in zip(intervals, intervals[1:])):
                raise ValueError("Duplicate/overlapping historical constituent intervals")

    def members_on(self, day, known_on=None):
        selected = pd.Timestamp(day)
        if known_on is not None:
            selected = min(selected, pd.Timestamp(known_on))
        return sorted(symbol for symbol, intervals in self.intervals.items()
                      if any(start <= selected <= end for start, end in intervals))

    def qlib_intervals(self, start, end):
        first, last = pd.Timestamp(start), pd.Timestamp(end)
        return {symbol: [(left, right) for left, right in intervals if right >= first and left <= last]
                for symbol, intervals in self.intervals.items() if any(right >= first and left <= last for left, right in intervals)}


def _series(pred):
    if isinstance(pred, pd.DataFrame):
        pred = pred.iloc[:, 0]
    if not isinstance(pred.index, pd.MultiIndex) or set(pred.index.names) != {"datetime", "instrument"}:
        raise ValueError("Qlib predictions require datetime/instrument index levels")
    pred = pred.reorder_levels(["datetime", "instrument"]).sort_index()
    if pred.index.has_duplicates:
        raise ValueError("Duplicate Qlib predictions")
    return pred


def _snapshot_inputs(dataset_dir, manifest_path, universe_name, config):
    from stopat30m.data.qlib_snapshot import load_qlib_snapshot
    manifest_path = Path(manifest_path)
    snapshot = load_qlib_snapshot(manifest_path.parent if manifest_path.name == "manifest.json" else manifest_path, verify=True)
    dataset = Path(snapshot.dataset_dir).resolve()
    if dataset != Path(dataset_dir).resolve():
        raise ValueError("Dataset directory differs from the frozen snapshot manifest")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", universe_name):
        raise ValueError("Invalid package universe name")
    pool_path = dataset / "instruments" / f"{universe_name}.txt"
    if not pool_path.is_file():
        raise ValueError("A real historical package constituent file is required")
    pool = PackageHistoricalPool(pool_path, start=config.train_start, end=config.oos_end)
    dates = pd.DatetimeIndex(pd.to_datetime((dataset / "calendars/day.txt").read_text().splitlines()))
    if dates.empty or dates.has_duplicates or not dates.is_monotonic_increasing:
        raise ValueError("Dataset exchange sessions are empty, duplicate or unsorted")
    if pd.Timestamp(config.train_start) < dates[0] or pd.Timestamp(config.oos_end) > dates[-1]:
        raise ValueError("Requested experiment dates exceed the verified package calendar")
    declarations = pool.qlib_intervals(config.train_start, config.oos_end)
    if not declarations:
        raise ValueError("No historical members overlap the requested experiment")
    used = {"calendars/day.txt", f"instruments/{universe_name}.txt"}
    for symbol in declarations:
        for field in REQUIRED_FIELDS:
            name = f"features/{symbol.lower()}/{field}.day.bin"
            if not (dataset / name).is_file():
                raise ValueError(f"Required Alpha158/audit field is missing: {symbol} {field}")
            used.add(name)
    hashes = {name: _sha(dataset / name) for name in sorted(used)}
    return snapshot, dataset, pool, dates, declarations, hashes


def _raw_qlib_fields(D, symbols, start, end):
    frame = D.features(instruments=symbols, fields=[f"${field}" for field in REQUIRED_FIELDS], start_time=start, end_time=end, freq="day")
    if not isinstance(frame.index, pd.MultiIndex) or not {"datetime", "instrument"}.issubset(frame.index.names):
        raise ValueError("Unexpected Qlib execution-price index")
    table = frame.reset_index()
    prices = {name: table.pivot(index="datetime", columns="instrument", values=f"${name}").sort_index() for name in REQUIRED_FIELDS}
    return prices


def _quote_state(prices, day, instrument):
    values = np.array([prices[field].at[day, instrument] for field in REQUIRED_FIELDS], dtype=float)
    quote_values = values[[0, 1, 2, 3, 5]]
    volume, factor = values[4], values[6]
    # The pinned package collector explicitly forward-fills factor. It is
    # normalization metadata, not evidence that an unavailable bar can trade.
    if np.isnan(quote_values).all() and (np.isnan(volume) or volume == 0) and (np.isnan(factor) or (np.isfinite(factor) and factor > 0)):
        return "all_fields_unavailable"
    if not np.isfinite(values).all() or (values <= 0).any():
        return "invalid_partial_bar"
    return "available"


def _quote_signature_audit(prices, calendar, pool, config):
    first = max(0, calendar.searchsorted(pd.Timestamp(config.oos_start)) - 1)
    counts = {"historical_member_sessions": 0, "unavailable_market_bars": 0,
              "unavailable_with_retained_factor": 0, "invalid_partial_bars": 0}
    for day in calendar[first:]:
        if day > pd.Timestamp(config.oos_end):
            break
        for inst in pool.members_on(day):
            counts["historical_member_sessions"] += 1
            state = _quote_state(prices, day, inst)
            if state == "all_fields_unavailable":
                counts["unavailable_market_bars"] += 1
                counts["unavailable_with_retained_factor"] += bool(np.isfinite(prices["factor"].at[day, inst]))
            elif state == "invalid_partial_bar":
                counts["invalid_partial_bars"] += 1
    return counts


def _rebalance_exposures(cash, opening, weights, frozen, config):
    equity = cash + sum(opening.values())
    instruments = sorted((set(opening) | set(weights)) - set(frozen))
    frozen_value = sum(opening.get(inst, 0) for inst in frozen)
    requested_weight = sum(weights.get(inst, 0) for inst in instruments)
    def target_values(post):
        capacity = max(0, post - frozen_value)
        requested = requested_weight * post
        scale = min(1, capacity / requested) if requested else 1
        return {inst: weights.get(inst, 0) * post * scale for inst in instruments}
    def changes(post):
        desired = target_values(post)
        return {inst: desired[inst] - opening.get(inst, 0) for inst in instruments}
    def costs(post):
        return sum(abs(delta) * ((config.buy_fee_bps if delta > 0 else config.sell_fee_bps) + config.slippage_bps) / 10000
                   for delta in changes(post).values())
    low, high = frozen_value, equity
    for _ in range(80):
        midpoint = low + (high - low) / 2
        if midpoint + costs(midpoint) > equity:
            high = midpoint
        else:
            low = midpoint
    post = low + (high - low) / 2
    targets = {**target_values(post), **{inst: opening[inst] for inst in frozen if inst in opening}}
    deals, fees, slip = [], 0.0, 0.0
    for inst, delta in changes(post).items():
        if abs(delta) < equity * 1e-12:
            continue
        fee = abs(delta) * (config.buy_fee_bps if delta > 0 else config.sell_fee_bps) / 10000
        slippage = abs(delta) * config.slippage_bps / 10000
        fees += fee
        slip += slippage
        deals.append({"instrument": inst, "direction": "BUY" if delta > 0 else "SELL",
                      "notional": abs(delta), "fee_cost": fee, "slippage_cost": slippage,
                      "execution_model": "observed_open_fractional_research", "live_fill": False})
    new_cash = post - sum(targets.values())
    if new_cash < -1e-10 or not np.isclose(equity - post, fees + slip, rtol=1e-8, atol=1e-10):
        raise ValueError("Partial research rebalance costs/cash do not reconcile")
    return max(0, new_cash), {inst: value for inst, value in targets.items() if value > 1e-14}, deals, fees, slip


def _simulate_ashare(days, calendar, prices, pool, config, strategy, predictions, corporate_actions=None):
    actions = corporate_actions or VerifiedCorporateActions()
    cash, previous_nav = 1.0, 1.0
    units, last_marks, last_factors, ages = {}, {}, {}, {}
    pending = None
    history, positions, trades, signals = [], [], [], []
    applied, action_ledger = set(), []
    for sequence, day in enumerate(days):
        location = calendar.get_loc(day)
        if location < 61:
            raise ValueError("OOS requires a preceding signal and 60-session history")
        signal_day = calendar[location - 1]
        states = {}
        def state(inst):
            if inst not in states:
                states[inst] = _quote_state(prices, day, inst)
                if states[inst] == "available" and actions.suspended(inst, day) is not None:
                    raise ValueError(f"Source bar conflicts with verified continuous suspension {day.date()} {inst}")
            return states[inst]
        converted_today = 0
        for action in actions.conversions(day):
            if action["id"] in applied:
                continue
            old, new = action["from_instrument"], action["to_instrument"]
            if old in units:
                if new not in prices["open"].columns or state(new) != "available":
                    raise ValueError(f"Confirmed share-exchange admission has no usable source bar {day.date()} {new}")
                old_factor = last_factors.get(old)
                new_factor = float(prices["factor"].at[day, new])
                if old_factor is None or not np.isfinite(old_factor) or old_factor <= 0:
                    raise ValueError("Share exchange has no last observed old normalization factor")
                old_units = units.pop(old)
                raw_shares = old_units * old_factor
                new_units = raw_shares * action["ratio"] / new_factor
                units[new] = units.get(new, 0.0) + new_units
                converted_today += 1
                action_ledger.append({"date": day, "strategy": strategy, "action_id": action["id"],
                    "from_instrument": old, "to_instrument": new, "old_virtual_units": old_units,
                    "old_factor": old_factor, "old_raw_shares": raw_shares, "raw_share_ratio": action["ratio"],
                    "new_raw_shares": raw_shares * action["ratio"], "new_factor": new_factor,
                    "new_virtual_units": new_units, "previous_carried_value": old_units * last_marks[old],
                    "admitted_open_value": new_units * float(prices["open"].at[day, new]),
                    "conversion_model": "raw_share_exchange_qfactor_conversion_at_confirmed_admission",
                    "share_quantity_basis": "raw-share equivalents per one initial NAV unit; not a reconciled broker holding",
                    "fee_cost": 0.0, "slippage_cost": 0.0, "live_fill": False,
                    "suspension_known_at": action["suspension"]["known_at"],
                    "confirmation_known_at": action["confirmation"]["known_at"],
                    "tradable_from": action["confirmation"]["tradable_from"]})
                ages.pop(old, None)
            applied.add(action["id"])
        opening = {}
        for inst, quantity in units.items():
            if state(inst) == "invalid_partial_bar":
                raise ValueError(f"Partial/corrupt held quote {day.date()} {inst}")
            if state(inst) == "all_fields_unavailable":
                ages[inst] = ages.get(inst, 0) + 1
                if (ages[inst] > 20 and actions.suspended(inst, day) is None) or inst not in last_marks:
                    raise ValueError(f"Held quote gap exceeds the declared 20-session research carry {day.date()} {inst}")
                opening[inst] = quantity * last_marks[inst]
            else:
                opening[inst] = quantity * float(prices["open"].at[day, inst])
                ages[inst] = 0
        pre_trade = cash + sum(opening.values())
        stale = [inst for inst in units if state(inst) == "all_fields_unavailable"]
        fees = slip = 0.0
        dealt = []
        scheduled = sequence % config.rebalance_sessions == 0
        if scheduled:
            declared_members = pool.members_on(signal_day)
            members = [inst for inst in declared_members if not actions.excluded(inst, signal_day, close=True)]
            known = []
            for inst in members:
                if inst not in prices["close"].columns:
                    raise ValueError("Historical constituent has no source price columns")
                prior = _quote_state(prices, signal_day, inst)
                if prior == "invalid_partial_bar":
                    raise ValueError(f"Partial signal-day source quote {signal_day.date()} {inst}")
                if prior == "available":
                    known.append(inst)
            scores = []
            if strategy == "equal_weight":
                selected = known
            elif strategy == "momentum":
                for inst in known:
                    window = prices["close"].iloc[location - 61:location][inst]
                    if window.notna().all() and (window > 0).all():
                        score = float(window.iloc[-1] / window.iloc[0] - 1)
                        if score > 0:
                            scores.append((inst, score))
                selected = [inst for inst, _ in sorted(scores, key=lambda pair: (-pair[1], pair[0]))[:config.top_k]]
            else:
                try:
                    score_rows = predictions.xs(signal_day, level="datetime")
                except KeyError:
                    raise ValueError("Required prior-session model predictions are absent") from None
                scores = [(inst, float(score_rows.loc[inst])) for inst in known
                          if inst in score_rows.index and np.isfinite(score_rows.loc[inst])]
                if not scores:
                    raise ValueError("No usable historical-pool model scores")
                selected = [inst for inst, _ in sorted(scores, key=lambda pair: (-pair[1], pair[0]))[:config.top_k]]
            weights = {inst: 1 / len(selected) for inst in selected}
            pending = (weights, signal_day)
            signals.extend({"date": day, "signal_date": signal_day, "instrument": inst,
                            "selected": inst in weights, "strategy": strategy,
                            "exclusion_reason": "verified_public_delisting" if inst not in members else ""} for inst in declared_members)
        if pending is not None:
            weights, decision_day = pending
            # A stale pending order cannot re-enter a publicly delisted symbol,
            # even if the vendor's coarse index interval has not caught up.
            weights = {inst: weight for inst, weight in weights.items() if not actions.excluded(inst, day)}
            needed = set(weights) | set(units)
            for inst in needed:
                if state(inst) == "invalid_partial_bar":
                    raise ValueError(f"Partial execution source quote {day.date()} {inst}")
            frozen = {inst for inst in needed if state(inst) == "all_fields_unavailable"}
            for inst in frozen - set(units):
                ages[inst] = ages.get(inst, 0) + 1
                if ages[inst] > 20 and actions.suspended(inst, day) is None:
                    raise ValueError(f"Pending quote gap exceeds 20 source sessions {day.date()} {inst}")
            cash, values, dealt, fees, slip = _rebalance_exposures(cash, opening, weights, frozen, config)
            next_units = {}
            for inst, value in values.items():
                next_units[inst] = units[inst] if inst in frozen else value / float(prices["open"].at[day, inst])
            units = next_units
            for trade in dealt:
                trades.append({**trade, "date": day, "signal_date": decision_day, "strategy": strategy})
            pending = (weights, decision_day) if frozen else None
        nav = cash
        day_positions = []
        for inst, quantity in units.items():
            if state(inst) == "available":
                last_marks[inst] = float(prices["close"].at[day, inst])
                last_factors[inst] = float(prices["factor"].at[day, inst])
                ages[inst] = 0
            elif inst not in last_marks:
                raise ValueError("Unavailable held quote has no preceding source valuation")
            value = quantity * last_marks[inst]
            nav += value
            day_positions.append({"date": day, "instrument": inst, "weight": value, "virtual_units": quantity,
                "valuation_source": "observed_close" if state(inst) == "available" else "carried_last_observed_close",
                "stale_sessions": ages.get(inst, 0), "strategy": strategy,
                "verified_suspended_entitlement": state(inst) != "available" and actions.suspended(inst, day) is not None,
                "corporate_action_id": (actions.suspended(inst, day) or {}).get("id", "")})
        if not np.isfinite(nav) or nav <= 0:
            raise ValueError("Invalid fractional research NAV")
        for row in day_positions:
            row["weight"] /= nav
        positions.extend(day_positions)
        history.append({"date": day, "strategy": strategy, "nav": nav, "net_return": nav / previous_nav - 1,
            "overnight_return": pre_trade / previous_nav - 1, "cash": cash, "fee_cost": fees, "slippage_cost": slip,
            "turnover": sum(row["notional"] for row in dealt) / pre_trade / 2,
            "scheduled_rebalance": scheduled, "pending_unavailable_orders": pending is not None,
            "corporate_action_count": converted_today,
            "stale_valuation_instruments": ",".join(sorted(stale)), "valuation_complete": not stale})
        previous_nav = nav
    curve = pd.DataFrame(history).set_index("date")
    curve.attrs["corporate_actions"] = action_ledger
    return curve, pd.DataFrame(positions), pd.DataFrame(trades), pd.DataFrame(signals)


def _compare(predictions, prices, calendar, pool, config, corporate_actions=None):
    days = calendar[(calendar >= config.oos_start) & (calendar <= config.oos_end)]
    if days.empty:
        raise ValueError("No OOS exchange sessions")
    if corporate_actions:
        for action in corporate_actions.actions:
            admission = pd.Timestamp(action["confirmation"]["tradable_from"])
            if days[0] <= admission <= days[-1] and admission not in calendar:
                raise ValueError("Confirmed share admission date is not a source exchange session")
    prices = {field: frame.reindex(calendar) for field, frame in prices.items()}
    outputs = {}
    for strategy in ("lightgbm", "momentum", "equal_weight"):
        outputs[strategy] = _simulate_ashare(days, calendar, prices, pool, config, strategy, predictions, corporate_actions)
    return outputs


def _write_comparison_outputs(output, report, comparison):
    curves, trades, positions, recorded_signals = [], [], [], []
    action_rows = []
    for strategy, (frame, held, dealt, decision) in comparison.items():
        report["metrics"][strategy] = compute_portfolio_metrics(frame.net_return)
        report["metrics"][strategy].update({"fees_in_initial_nav_units": float(frame.fee_cost.sum()),
            "slippage_in_initial_nav_units": float(frame.slippage_cost.sum()),
            "one_way_turnover_sum": float(frame.turnover.sum()),
            "stale_valuation_days": int((~frame.valuation_complete).sum()),
            "pending_order_days": int(frame.pending_unavailable_orders.sum()),
            "max_stale_sessions": int(held.stale_sessions.max()) if not held.empty else 0,
            "quote_carry_assumption_applied": bool((~frame.valuation_complete).any())})
        action_rows.extend(frame.attrs.get("corporate_actions", []))
        report["metrics"][strategy]["verified_share_exchanges"] = len(frame.attrs.get("corporate_actions", []))
        curves.append(frame.assign(strategy=strategy).reset_index())
        for rows, destination in ((dealt, trades), (held, positions), (decision, recorded_signals)):
            if not rows.empty:
                destination.append(rows.assign(strategy=strategy))
    pd.concat(curves, ignore_index=True).to_csv(output / "curves.csv", index=False, float_format="%.17g")
    for name, rows in (("trades", trades), ("positions", positions), ("signals", recorded_signals)):
        (pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()).to_csv(output / f"{name}.csv", index=False)
    if action_rows:
        pd.DataFrame(action_rows).to_csv(output / "corporate_actions.csv", index=False, float_format="%.17g")
    report.update({"backtest_status": "completed_fractional_research", "comparison_complete": True})


def run_ashare_experiment(dataset_dir, snapshot_manifest_path, output_dir, *, config, universe_name="csi300", declaration=None, corporate_actions_path=None):
    """Train official Qlib Alpha158 and evaluate three cost-matched strategies.

    Native model, purged split audit, package/data hashes and reports are saved
    only to a new output directory. If strict execution data is insufficient,
    the trained model/IC remain usable but no comparison return is published.
    Whole-bar unavailability has a separate, explicitly flagged carry policy.
    """
    if not isinstance(config, AshareExperimentConfig):
        raise ValueError("An explicit AshareExperimentConfig is required")
    if declaration is not None:
        if not isinstance(declaration, dict) or declaration.get("config", asdict(config)) != asdict(config) or declaration.get("universe", universe_name) != universe_name:
            raise ValueError("Experiment declaration differs from the actual configuration/universe")
    output = Path(output_dir)
    if output.exists():
        raise FileExistsError("A-share experiment output exists; never overwrite a model or result")
    snapshot, dataset_path, pool, calendar, instruments, hashes = _snapshot_inputs(dataset_dir, snapshot_manifest_path, universe_name, config)
    actions = VerifiedCorporateActions(corporate_actions_path)
    quote_symbols, action_hashes = _action_price_inputs(actions, dataset_path, instruments)
    import qlib
    from qlib.contrib.data.handler import Alpha158
    from qlib.data import D
    from qlib.data.dataset import DatasetH
    qlib.init(provider_uri=str(dataset_path), region="cn", kernels=1, expression_cache=None, dataset_cache=None)
    requested = {"train": (config.train_start, config.train_end), "valid": (config.valid_start, config.valid_end), "test": (config.oos_start, config.oos_end)}
    segments, split_audit = purge_label_segments(requested, calendar, 6, names=("train", "valid", "test"))
    beginning = int(calendar.searchsorted(pd.Timestamp(config.train_start)))
    warmup = calendar[max(0, beginning - 80)].date().isoformat()
    spec = FixedQlibLGBSpec()
    protocol = {"schema_version": 1, "policy": POLICY, "locked_at": datetime.now(timezone.utc).isoformat(),
        "quote_policy": QUOTE_POLICY,
        "corporate_actions": actions.describe(), "additional_action_price_sha256": action_hashes,
        "config": asdict(config), "model_spec": asdict(spec), "train_params": spec.params(),
        "external_declaration": _clean(declaration or {}),
        "feature_schema": "qlib.contrib.data.handler.Alpha158 official 158",
        "label": "Ref($open,-6)/Ref($open,-1)-1", "label_future_sessions": 6,
        "split_audit": split_audit, "membership_status": "package_declared_membership_not_full_PIT",
        "membership_sha256": hashes[f"instruments/{universe_name}.txt"],
        "time_clipped_non_numeric_intervals": pool.clipped_non_numeric_intervals,
        "snapshot_manifest_sha256": snapshot.manifest_sha256, "snapshot_path": str(snapshot.path),
        "used_files_sha256": hashes, "qlib_version": qlib.__version__,
        "preprocessing": "RobustZScoreNorm fitted only on purged train; feature Fillna; train-only CSRankNorm labels; fitted normalization state saved with native model",
        "implementation_sha256": {"experiment": _sha(__file__),
            "handler": _sha(Path(__file__).parent / "factors/handler.py"),
            "trainer": _sha(Path(__file__).parent / "model/trainer.py")},
        "cost_model": "same declared aggregate buy/sell fees and slippage for every strategy; fee-component sources, if supplied, are frozen in external_declaration; minimum commissions, lots and automatic changes of historical tax regimes are not modelled",
        "early_stopping": False, "parameter_search": False, "limitations": LIMITATIONS}
    protocol["protocol_sha256"] = wf._digest(protocol)
    output.mkdir(parents=True, exist_ok=False)
    _write(output / "protocol.json", protocol)
    if actions.path:
        (output / "corporate_actions_input.json").write_bytes(actions.path.read_bytes())
    handler = Alpha158(instruments=instruments, start_time=warmup, end_time=config.oos_end,
        fit_start_time=segments["train"][0], fit_end_time=segments["train"][1],
        label=[protocol["label"]], infer_processors=[{"class": "RobustZScoreNorm", "kwargs": {
            "fields_group": "feature", "fit_start_time": segments["train"][0], "fit_end_time": segments["train"][1], "clip_outlier": True}},
            {"class": "Fillna", "kwargs": {"fields_group": "feature"}}],
        learn_processors=[{"class": "DropnaLabel"}, {"class": "CSRankNorm", "kwargs": {"fields_group": "label"}}])
    dataset = DatasetH(handler=handler, segments=segments)
    model = fit_fixed_qlib_lightgbm(dataset, spec=spec)
    model_dir = model.save_native(output / "model", metadata={"protocol_sha256": protocol["protocol_sha256"],
        "snapshot_manifest_sha256": snapshot.manifest_sha256, "membership_sha256": protocol["membership_sha256"],
        "split_audit": split_audit, "qlib_version": qlib.__version__,
        "fitted_inference_processors": _fitted_processors(handler), "preprocessing": protocol["preprocessing"]})
    diagnostics = {}
    for name in ("train", "valid", "test"):
        predicted = _series(model.predict(dataset, segment=name))
        labels = dataset.prepare(name, col_set="label", data_key="infer").iloc[:, 0]
        labels = _series(labels)
        diagnostics[name] = evaluate_predictions(predicted, label=labels)
    first = calendar.searchsorted(pd.Timestamp(config.oos_start))
    if first == 0:
        raise ValueError("The first OOS execution needs a preceding source session")
    signals = _series(model.predict(dataset, segment=slice(calendar[first - 1], pd.Timestamp(config.oos_end))))
    signals.rename("score").to_csv(output / "predictions.csv", float_format="%.17g")
    prices = _raw_qlib_fields(D, quote_symbols, warmup, config.oos_end)
    report = {"policy": POLICY, "quote_policy": QUOTE_POLICY, "training_rows": model.training_rows, "feature_count": len(model.feature_names),
        "historical_member_instruments": len(instruments), "membership_status": protocol["membership_status"],
        "split_audit": split_audit, "prediction_diagnostics": diagnostics, "historical_oos_is_retrospective": True,
        "metrics": {}, "comparison_complete": False, "execution_ready": False, "limitations": LIMITATIONS}
    report["corporate_actions"] = actions.describe()
    report["quote_signature_audit"] = _quote_signature_audit(prices, calendar, pool, config)
    try:
        comparison = _compare(signals, prices, calendar, pool, config, actions)
    except ValueError as exc:
        report.update({"backtest_status": "blocked_missing_or_unusable_quotes", "backtest_error": str(exc)})
    else:
        _write_comparison_outputs(output, report, comparison)
    # Refuse publication if any consumed source file changed during fitting.
    if any(_sha(dataset_path / name) != digest for name, digest in {**hashes, **action_hashes}.items()):
        raise ValueError("Qlib dataset changed during the experiment; no result is publishable")
    actions.assert_unchanged()
    _write(output / "report.json", report)
    files = {str(path.relative_to(output)): _sha(path) for path in sorted(output.rglob("*")) if path.is_file()}
    manifest = {"schema_version": 1, "policy": POLICY, "created_at": datetime.now(timezone.utc).isoformat(),
        "protocol_sha256": protocol["protocol_sha256"], "snapshot_manifest_sha256": snapshot.manifest_sha256,
        "dataset_path": str(dataset_path), "membership_sha256": protocol["membership_sha256"],
        "training_implementation_sha256": _sha(__file__), "files": files,
        "comparison_complete": report["comparison_complete"], "execution_ready": False}
    manifest["manifest_sha256"] = wf._digest(manifest)
    _write(output / "manifest.json", manifest)
    return {"output_dir": str(output), "model_path": str(model_dir), "manifest": manifest, "report": _clean(report)}


def resume_ashare_comparison(experiment_dir, output_dir, *, corporate_actions_path=None):
    """Re-evaluate the same frozen scores/model after a documented quote fix.

    No fit, feature generation, parameter override or new prediction is allowed.
    The original failed run is verified and left byte-for-byte unchanged. A new
    sibling result binds its original model, predictions, data and correction.
    """
    origin, output = Path(experiment_dir).resolve(), Path(output_dir)
    if output.exists():
        raise FileExistsError("Recovered comparison output exists; never overwrite")
    previous_manifest = json.loads((origin / "manifest.json").read_text())
    if previous_manifest.get("manifest_sha256") != wf._digest({key: value for key, value in previous_manifest.items() if key != "manifest_sha256"}):
        raise ValueError("Original experiment manifest checksum mismatch")
    for name, digest in previous_manifest["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or _sha(origin / relative) != digest:
            raise ValueError("Original model/predictions/report changed; cannot resume comparison")
    protocol = json.loads((origin / "protocol.json").read_text())
    if protocol.get("protocol_sha256") != wf._digest({key: value for key, value in protocol.items() if key != "protocol_sha256"}):
        raise ValueError("Original protocol checksum mismatch")
    config = AshareExperimentConfig(**protocol["config"])
    membership_name = Path(next(name for name in protocol["used_files_sha256"] if name.startswith("instruments/"))).stem
    snapshot, dataset_path, pool, calendar, instruments, hashes = _snapshot_inputs(
        previous_manifest["dataset_path"], Path(protocol["snapshot_path"]) / "manifest.json", membership_name, config)
    if snapshot.manifest_sha256 != protocol["snapshot_manifest_sha256"] or hashes != protocol["used_files_sha256"]:
        raise ValueError("Recovered comparison must use the exact original dataset and membership")
    native = load_fixed_qlib_model(origin / "model")
    # A full run already bound to an action document cannot silently drop it.
    bound_actions = protocol.get("corporate_actions", {})
    if corporate_actions_path is None and bound_actions.get("path"):
        corporate_actions_path = bound_actions["path"]
    actions = VerifiedCorporateActions(corporate_actions_path)
    if bound_actions.get("sha256") and bound_actions["sha256"] != actions.sha256:
        raise ValueError("Recovered comparison cannot replace its originally bound corporate actions")
    quote_symbols, action_hashes = _action_price_inputs(actions, dataset_path, instruments)
    scores_frame = pd.read_csv(origin / "predictions.csv", parse_dates=["datetime"], float_precision="round_trip")
    predictions = _series(scores_frame.set_index(["datetime", "instrument"])["score"])
    import qlib
    from qlib.data import D
    qlib.init(provider_uri=str(dataset_path), region="cn", kernels=1, expression_cache=None, dataset_cache=None)
    first = int(calendar.searchsorted(pd.Timestamp(config.oos_start)))
    start = calendar[max(0, first - 61)]
    prices = _raw_qlib_fields(D, quote_symbols, start, config.oos_end)
    old_report = json.loads((origin / "report.json").read_text())
    report = {**old_report, "metrics": {}, "comparison_complete": False, "quote_policy": QUOTE_POLICY,
              "limitations": LIMITATIONS}
    report.pop("backtest_error", None)
    evidence_path = Path(__file__).resolve().parents[1] / "output/research/ashare/base/probes/pinned_official_yahoo_collector.py"
    correction = {"kind": "source_signature_engineering_correction", "no_refit": True,
        "original_error": old_report.get("backtest_error"), "original_report_sha256": _sha(origin / "report.json"),
        "original_model_sha256": _sha(origin / "model/model.txt"),
        "original_predictions_sha256": _sha(origin / "predictions.csv"),
        "rule": "All OHLC/VWAP missing plus volume NaN/zero is unavailable; retained finite adjustment factor is normalization metadata, not an executable quote.",
        "fixed_spec_unchanged": native.spec.params() == protocol["train_params"],
        "supplier_code_evidence": {"path": str(evidence_path), "sha256": _sha(evidence_path), "lines": "460-462; factor=(adjclose/close).ffill()"} if evidence_path.is_file() else None}
    if actions.actions:
        correction.update({"kind": "source_signature_and_verified_share_exchange_accounting_correction",
                           "corporate_actions": actions.describe(),
                           "return_based_threshold_selection": False,
                           "generic_unknown_gap_limit_sessions": 20})
    if not correction["fixed_spec_unchanged"]:
        raise ValueError("Recovered comparison model parameters differ from the original protocol")
    report["quote_policy_correction"] = correction
    report["corporate_actions"] = actions.describe()
    report["quote_signature_audit"] = _quote_signature_audit(prices, calendar, pool, config)
    output.mkdir(parents=True, exist_ok=False)
    if actions.path:
        (output / "corporate_actions_input.json").write_bytes(actions.path.read_bytes())
    _write(output / "comparison_plan.json", {"original_experiment": str(origin),
        "original_manifest_sha256": previous_manifest["manifest_sha256"], "protocol_sha256": protocol["protocol_sha256"],
        "quote_policy": QUOTE_POLICY, "correction": correction,
        "corporate_actions": actions.describe(), "additional_action_price_sha256": action_hashes,
        "locked_at": datetime.now(timezone.utc).isoformat()})
    try:
        comparison = _compare(predictions, prices, calendar, pool, config, actions)
    except ValueError as exc:
        report.update({"backtest_status": "blocked_missing_or_unusable_quotes", "backtest_error": str(exc)})
    else:
        _write_comparison_outputs(output, report, comparison)
    if any(_sha(dataset_path / name) != digest for name, digest in {**hashes, **action_hashes}.items()):
        raise ValueError("Qlib dataset changed during comparison recovery")
    actions.assert_unchanged()
    _write(output / "report.json", report)
    manifest = {"schema_version": 1, "policy": POLICY, "quote_policy": QUOTE_POLICY,
        "created_at": datetime.now(timezone.utc).isoformat(), "no_refit": True,
        "model_origin": str(origin / "model"), "model_sha256": correction["original_model_sha256"],
        "original_experiment_manifest_sha256": previous_manifest["manifest_sha256"],
        "protocol_sha256": protocol["protocol_sha256"], "snapshot_manifest_sha256": snapshot.manifest_sha256,
        "comparison_complete": report["comparison_complete"], "execution_ready": False,
        "corporate_actions": actions.describe(),
        "comparison_implementation_sha256": _sha(__file__),
        "files": {str(path.relative_to(output)): _sha(path) for path in sorted(output.rglob("*")) if path.is_file()}}
    manifest["manifest_sha256"] = wf._digest(manifest)
    _write(output / "manifest.json", manifest)
    return {"output_dir": str(output), "model_path": str(origin / "model"), "manifest": manifest, "report": _clean(report)}
