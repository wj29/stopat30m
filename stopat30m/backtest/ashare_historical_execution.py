"""Frozen A2 raw-share ledger with explicit 2023--2024 research rule extensions.

Source: stopat30m/backtest/ashare_execution.py, byte SHA256
3b19885de1569363ec69b07664e9d58d55a85eff2db438cd869bdfd777cf92a8.
This is an independent module, never a global patch or a live paper engine.
The original cash, share, T+1, corporate action and valuation algorithms are
preserved. Declared differences: rule scope, historical IPO exclusion, and
session-derived stamp/transfer rates. Official sources and source receipts are
listed in docs/research/a4_historical_execution_rules.json.

Scope: ordinary SSE/SZSE main-board, ChiNext and STAR A shares in 2023--2024.
Relistings, delisting periods, IPO initial windows and BSE are excluded. Daily
bars cannot prove queue priority or actual liquidity. Missing marks remain
incomplete even when a source verifies suspension.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
import copy
import hashlib
import json
import re
from typing import Mapping, Sequence

import pandas as pd

CENT = Decimal("0.01")
ZERO = Decimal("0")
ONE = Decimal("1")
BOARDS = {"sse_main", "szse_main", "chinext", "star", "bse"}
RULE_VERSION = "a4-raw-integer-2023-2024-historical-rules-v1"
BASE_ENGINE_SHA256 = "3b19885de1569363ec69b07664e9d58d55a85eff2db438cd869bdfd777cf92a8"
HISTORICAL_BOARDS = {"sse_main", "szse_main", "chinext", "star"}
MAIN_REGISTRATION_FIRST_LISTING = "2023-04-10"


def _historical_session(session):
    value = _day(session)
    if not "2023-01-01" <= value <= "2024-12-31":
        raise ValueError("Historical execution rules only cover 2023--2024 sessions")
    return value


def historical_policy(session, base_policy=None):
    """Return explicit full-cost rates for this historical execution day.

    Seller stamp duty was 10bp through 2023-08-25 and 5bp from 2023-08-28.
    The transfer fee is 0.01 per mille (= 0.1bp), in both directions. Commission,
    minimum commission and slippage are unchanged declared research assumptions.
    The announcement's 2023-08-27 publication date is not its effective date.
    """
    session = _historical_session(session)
    base = base_policy or ExecutionPolicy()
    return replace(base, sell_stamp_rate=Decimal("0.001") if session < "2023-08-28" else Decimal("0.0005"),
                   transfer_rate=Decimal("0.00001"))


def verify_historical_rules(path):
    """Read a reviewed rules declaration and check its original-byte receipts.

    This verifies preserved bytes and declared scope, not an external legal
    certification. It neither fetches data nor fabricates historical timestamps.
    Relative evidence paths resolve against the declaration's own directory.
    """
    from pathlib import Path
    path = Path(path).resolve()
    value = json.loads(path.read_text())
    required = {"frozen_a2_engine", "historical_engine", "historical_execution_adapter",
        "sse_2020_rules", "sse_2020_st", "sse_2019_star", "sse_2023_rules_docx",
        "szse_2023_lots_pdf", "szse_2023_rules_pdf", "stamp_2022_law",
        "stamp_2023_tianjin", "csdc_2022_original_notice_screenshot"}
    if (value.get("schema_version") != 1 or value.get("status") != "verified"
            or value.get("rule_version") != RULE_VERSION
            or value.get("supported_sessions") != ["2023-01-01", "2024-12-31"]
            or not required.issubset(value.get("bound_inputs", {}))):
        raise ValueError("Historical rules declaration is incomplete or outside the reviewed scope")
    if (value["bound_inputs"]["frozen_a2_engine"]["sha256"] != BASE_ENGINE_SHA256
            or value["bound_inputs"]["historical_engine"]["sha256"]
                != hashlib.sha256(Path(__file__).read_bytes()).hexdigest()):
        raise ValueError("Historical rules declaration does not bind this reviewed ledger implementation")
    for name, proof in value["bound_inputs"].items():
        if not isinstance(proof, dict) or not re.fullmatch(r"[0-9a-f]{64}", str(proof.get("sha256", ""))):
            raise ValueError("Historical rules require SHA-256 original-byte receipts")
        source = Path(proof["path"])
        if not source.is_absolute():
            source = path.parent / source
        if hashlib.sha256(source.read_bytes()).hexdigest() != proof["sha256"]:
            raise ValueError("Historical rule evidence checksum differs: " + name)
    return value


def decimal(value) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("Boolean is not a monetary/quantity value")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError("A finite decimal value is required") from None
    if not result.is_finite():
        raise ValueError("A finite decimal value is required")
    return result


def money(value) -> Decimal:
    return decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def _day(value) -> str:
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("An explicit ISO session date is required")
    return value


def _known(known_at: str, session: str, *, now=None) -> bool:
    stamp = pd.Timestamp(known_at)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Public known_at must be timezone aware")
    cutoff = pd.Timestamp(now) if now is not None else pd.Timestamp(session + "T09:30:00+08:00")
    if cutoff.tzinfo is None:
        raise ValueError("Execution clock must be timezone aware")
    return stamp <= cutoff


def _json(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(_json(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class ExecutionPolicy:
    initial_cash: Decimal = Decimal("100000.00")
    commission_rate: Decimal = Decimal("0.0003")
    minimum_commission: Decimal = Decimal("5.00")
    transfer_rate: Decimal = Decimal("0.00001")
    sell_stamp_rate: Decimal = Decimal("0.0005")
    slippage_bps: Decimal = Decimal("2")
    dividend_tax_rate: Decimal = Decimal("0.20")
    max_unknown_stale_sessions: int = 20
    max_live_age_seconds: int = 120
    # An explicit modelling choice, not a claim that IPO trading is prohibited.
    trade_initial_no_limit: bool = False

    def __post_init__(self):
        for name in ("initial_cash", "commission_rate", "minimum_commission", "transfer_rate", "sell_stamp_rate", "slippage_bps", "dividend_tax_rate"):
            value = decimal(getattr(self, name)); object.__setattr__(self, name, value)
            if value < 0:
                raise ValueError("Execution costs and capital cannot be negative")
        if self.initial_cash <= 0 or self.dividend_tax_rate > 1 or self.slippage_bps > 1000:
            raise ValueError("Invalid execution policy")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in (self.max_unknown_stale_sessions, self.max_live_age_seconds)):
            raise ValueError("Staleness limits must be positive integers")
        if self.trade_initial_no_limit:
            raise ValueError("IPO auction/no-limit execution is not implemented; use the conservative exclusion")

    def to_dict(self):
        return _json(asdict(self))


@dataclass(frozen=True)
class SecurityInfo:
    instrument: str
    board: str
    listing_date: str
    is_st: bool | None
    known_at: str
    source: str
    # Optional when a complete exchange calendar is supplied; needed for an IPO
    # newer than the supplied calendar's beginning. Never infer weekday sessions.
    listing_session_number: int | None = None
    delisting_period: bool | None = False
    status_session: str | None = None


@dataclass(frozen=True)
class OpeningQuote:
    session: str
    open: Decimal | None
    preclose: Decimal | None  # Exchange ex-right/ex-dividend reference, NOT blindly yesterday's raw close.
    tradestatus: str  # trading/suspended/unknown, from source status.
    source: str
    execution_price: Decimal | None = None
    execution_price_basis: str = "historical_observed_open"
    asof: str | None = None


@dataclass
class Position:
    quantity: int
    bought_today: int = 0
    bought_date: str | None = None
    last_mark: Decimal | None = None
    last_mark_date: str | None = None
    last_mark_basis: str | None = None

    def __post_init__(self):
        if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in (self.quantity, self.bought_today)) or self.bought_today > self.quantity:
            raise ValueError("Positions require nonnegative integer shares and valid T+1 locks")
        if self.last_mark is not None:
            self.last_mark = decimal(self.last_mark)
            if self.last_mark <= 0:
                raise ValueError("Held market marks must be positive")
        if self.bought_today and self.bought_date is None:
            raise ValueError("T+1 locked shares need their actual acquisition session")
        if self.bought_date is not None:
            _day(self.bought_date)
        if self.last_mark_date is not None:
            _day(self.last_mark_date)


@dataclass
class PortfolioState:
    cash: Decimal
    positions: dict[str, Position] = field(default_factory=dict)
    session: str | None = None
    claims: dict[str, dict] = field(default_factory=dict)
    applied_actions: dict[str, dict] = field(default_factory=dict)
    holdings_history: dict[str, dict[str, int]] = field(default_factory=dict)
    entitlements: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self):
        self.cash = money(self.cash)
        if self.cash < 0:
            raise ValueError("Cash cannot be negative")

    @classmethod
    def initial(cls, policy=None):
        return cls((policy or ExecutionPolicy()).initial_cash)

    def to_dict(self):
        return {"schema_version": 1, "cash": str(self.cash), "session": self.session,
            "positions": {key: _json(asdict(value)) for key, value in sorted(self.positions.items())},
            "claims": _json(self.claims), "applied_actions": _json(self.applied_actions),
            "holdings_history": copy.deepcopy(self.holdings_history), "entitlements": _json(self.entitlements)}

    @classmethod
    def from_dict(cls, value):
        if value.get("schema_version") != 1:
            raise ValueError("Unsupported integer portfolio state schema")
        return cls(cash=decimal(value["cash"]), session=value.get("session"),
            positions={key: Position(**row) for key, row in value.get("positions", {}).items()},
            claims=copy.deepcopy(value.get("claims", {})), applied_actions=copy.deepcopy(value.get("applied_actions", {})),
            holdings_history=copy.deepcopy(value.get("holdings_history", {})), entitlements=copy.deepcopy(value.get("entitlements", {})))


@dataclass
class ExecutionResult:
    state: PortfolioState
    orders: list[dict]
    trades: list[dict]
    opening_equity: Decimal


@dataclass
class ValuationResult:
    state: PortfolioState
    equity_lower_bound: Decimal
    equity_with_claim_estimate: Decimal
    equity_upper_bound: Decimal
    valuation_complete: bool
    positions: list[dict]
    claims: list[dict]


@dataclass
class ActionResult:
    state: PortfolioState
    record: dict
    replayed: bool


def _calendar(calendar: Sequence[str], session: str):
    values = [str(pd.Timestamp(day).date()) for day in calendar]
    if not values or values != sorted(set(values)) or session not in values:
        raise ValueError("A real sorted unique exchange calendar containing the execution session is required")
    return values


def _advance(state, session, calendar):
    _day(session); _calendar(calendar, session)
    result = PortfolioState.from_dict(state.to_dict())
    if result.session is not None and session < result.session:
        raise ValueError("Cannot backdate an integer portfolio")
    if result.session != session:
        for position in result.positions.values():
            position.bought_today = 0
        result.session = session
    return result


def price_limits(info: SecurityInfo, quote: OpeningQuote, *, session: str, calendar: Sequence[str], now=None):
    """Historical daily limits from day-specific status and actual preclose.

    The source fetch time must never be relabelled as historical known_at. All
    main-board IPOs before the registration transition are conservatively
    excluded for their first five sessions; their first-day 144%/64% mechanism
    needs the issue price and is intentionally not simulated as a no-limit day.
    """
    _historical_session(session)
    days = _calendar(calendar, session)
    if info.board not in HISTORICAL_BOARDS:
        raise ValueError("Unsupported board for the declared historical A-share rules")
    if type(info.is_st) is not bool or type(info.delisting_period) is not bool:
        raise ValueError("Historical ST/delisting status is unknown")
    if info.status_session is not None and info.status_session != session:
        raise ValueError("Historical security status belongs to another session")
    if not info.source or not _known(info.known_at, session, now=now):
        raise ValueError("Security metadata was not known at execution time")
    listed = _day(info.listing_date)
    if listed > session or info.delisting_period:
        raise ValueError("Pre-listing or delisting-period execution is unsupported")
    if info.listing_session_number is not None:
        age = info.listing_session_number
        if isinstance(age, bool) or not isinstance(age, int) or age < 1:
            raise ValueError("Invalid listing session ordinal")
    elif listed < days[0]:
        # Need only distinguish <=5 sessions; a calendar beginning after the
        # listing cannot prove that threshold on its own.
        after_beginning = days.index(session) + 1
        if after_beginning < 5:
            raise ValueError("Insufficient exchange sessions to establish IPO age")
        age = after_beginning + 1
    elif listed not in days:
        raise ValueError("Listing date is not a source exchange session")
    else:
        age = days.index(session) - days.index(listed) + 1
    legacy_initial = info.board in {"sse_main", "szse_main"} and listed < MAIN_REGISTRATION_FIRST_LISTING and age <= 5
    if legacy_initial:
        raise ValueError("Pre-registration main-board IPO initial five sessions conservatively excluded; first-day issue-price 144%/64% mechanism is unsupported")
    no_limit = age <= 5
    rate = Decimal("0.20") if info.board in {"chinext", "star"} else Decimal("0.05") if info.is_st else Decimal("0.10")
    if quote.preclose is None or decimal(quote.preclose) <= 0:
        raise ValueError("An observed positive exchange preclose reference is required")
    reference = decimal(quote.preclose)
    upper = max(money(reference * (1 + rate)), money(reference) + CENT)
    lower = max(CENT, min(money(reference * (1 - rate)), money(reference) - CENT))
    return {"upper": None if no_limit else upper, "lower": None if no_limit else lower,
            "rate": None if no_limit else rate, "no_limit_ipo": no_limit,
            "buy_min": 200 if info.board == "star" else 100,
            "buy_step": 1 if info.board in {"star", "bse"} else 100,
            "sell_min": 200 if info.board == "star" else 100,
            "sell_step": 1 if info.board in {"star", "bse"} else 100,
            "max_quantity": 100000 if info.board == "star" else 300000 if info.board == "chinext" else 1000000}


def _reference(quote, session, policy, *, now=None):
    if quote.session != session or not quote.source:
        raise ValueError("Execution quote belongs to another session or lacks provenance")
    if quote.tradestatus != "trading":
        raise ValueError("Suspended or unknown source trading status")
    if quote.execution_price_basis == "historical_observed_open":
        if now is not None or quote.execution_price is not None:
            raise ValueError("Historical opening fills cannot be used as a live observed-price fill")
        value = quote.open
    elif quote.execution_price_basis == "live_observed_quote":
        if now is None or quote.asof is None or quote.execution_price is None:
            raise ValueError("Live execution requires an injected clock and observed quote timestamp/price")
        stamp, clock = pd.Timestamp(quote.asof), pd.Timestamp(now)
        if stamp.tzinfo is None or clock.tzinfo is None:
            raise ValueError("Live clocks must include timezone")
        stamp, clock = stamp.tz_convert("Asia/Shanghai"), clock.tz_convert("Asia/Shanghai")
        if str(stamp.date()) != session or str(clock.date()) != session:
            raise ValueError("Live execution quote is not from today's session")
        age = (clock - stamp).total_seconds()
        if age < 0 or age > policy.max_live_age_seconds:
            raise ValueError("Live execution quote is stale or from the future")
        for moment in (stamp, clock):
            minute = moment.hour * 60 + moment.minute + moment.second / 60
            if not (570 <= minute <= 690 or 780 <= minute < 897):
                raise ValueError("Live execution is outside continuous trading hours")
        value = quote.execution_price
    else:
        raise ValueError("Unknown execution-price basis")
    if value is None or decimal(value) <= 0:
        raise ValueError("No valid observed execution reference")
    value = decimal(value)
    if value != money(value):
        raise ValueError("A-share observed execution price does not respect the CNY 0.01 tick")
    return value


def _fill_price(reference, side, limits, policy):
    if limits["no_limit_ipo"]:
        raise ValueError("Initial IPO no-limit session conservatively excluded")
    if reference > limits["upper"] or reference < limits["lower"]:
        raise ValueError("Observed execution reference lies outside the exchange daily limits")
    if side == "BUY" and reference >= limits["upper"]:
        raise ValueError("Adverse upper-limit auction queue cannot be proven fillable")
    if side == "SELL" and reference <= limits["lower"]:
        raise ValueError("Adverse lower-limit auction queue cannot be proven fillable")
    adjusted = reference * (1 + (1 if side == "BUY" else -1) * policy.slippage_bps / 10000)
    tick_price = adjusted.quantize(CENT, rounding=ROUND_CEILING if side == "BUY" else ROUND_FLOOR)
    return min(limits["upper"], max(limits["lower"], tick_price))


def fees_for_order(quantity, price, side, policy=None):
    policy = policy or ExecutionPolicy()
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 1 or side not in {"BUY", "SELL"} or decimal(price) <= 0:
        raise ValueError("An order needs positive integer shares, price and side")
    notional = money(quantity * decimal(price))
    commission = max(money(notional * policy.commission_rate), money(policy.minimum_commission))
    transfer = money(notional * policy.transfer_rate)
    stamp = money(notional * policy.sell_stamp_rate) if side == "SELL" else money(0)
    return {"notional": notional, "commission": commission, "transfer_fee": transfer,
            "stamp_tax": stamp, "total_fee": commission + transfer + stamp}


def _buy_quantity(cap, limits):
    cap = max(0, min(int(cap), limits["max_quantity"]))
    if cap < limits["buy_min"]:
        return 0
    return cap // limits["buy_step"] * limits["buy_step"]


def _sell_quantity(wanted, available, held, limits):
    cap = max(0, min(int(wanted), available, limits["max_quantity"]))
    if cap >= available:
        # Sell all available shares, including the indivisible odd remainder.
        return available
    if limits["sell_step"] == 1:
        return cap if cap >= limits["sell_min"] else 0
    odd = held % limits["sell_step"]
    regular = cap // limits["sell_step"] * limits["sell_step"]
    with_odd = (cap - odd) // limits["sell_step"] * limits["sell_step"] + odd if odd and cap >= odd else 0
    return max(regular, with_odd)


def execute_target_weights(state: PortfolioState, *, session: str, signal_date: str,
        targets: Mapping[str, object], quotes: Mapping[str, OpeningQuote], metadata: Mapping[str, SecurityInfo],
        calendar: Sequence[str], policy=None, now=None, verified_suspended: Sequence[str] = ()) -> ExecutionResult:
    """One deterministic rebalance: sell first, then buy largest deficits.

    Targets are prior-session weights, never recalculated from today's H/L/C.
    Missing holdings retain an explicit previous observed mark for budgeting;
    missing/unknown executions are rejected individually, never traded there.
    The caller owns order idempotency and session persistence. Repeated calls
    on the same state/session preserve T+1 locks, but are new orders/commissions.
    """
    if now is not None:
        raise ValueError("Historical rules are research-only and reject live execution clocks")
    policy = historical_policy(session, policy)
    days = _calendar(calendar, session)
    if _day(signal_date) >= session or signal_date not in days:
        raise ValueError("Targets must be frozen on a preceding real exchange session")
    weights = {key: decimal(value) for key, value in targets.items()}
    if any(value < 0 for value in weights.values()) or sum(weights.values(), ZERO) > 1:
        raise ValueError("Long-only target weights must sum to at most one")
    result = _advance(state, session, days)
    if session in result.holdings_history:
        raise ValueError("A finalized closing session cannot accept more model executions")
    orders, trades, validated, marks = [], [], {}, {}
    for inst in sorted(set(result.positions) | set(weights)):
        try:
            quote, info = quotes[inst], metadata[inst]
            if info.instrument != inst:
                raise ValueError("Instrument metadata identity mismatch")
            reference = _reference(quote, session, policy, now=now)
            marks[inst] = reference  # A valid raw mark does not require permission to trade this security.
            limits = price_limits(info, quote, session=session, calendar=days, now=now)
            validated[inst] = (quote, reference, limits)
            marks[inst] = reference
        except (KeyError, ValueError) as exc:
            reason = "Missing quote/security metadata" if isinstance(exc, KeyError) else str(exc)
            orders.append({"session": session, "signal_date": signal_date, "instrument": inst,
                           "status": "rejected", "reason": reason})
            position = result.positions.get(inst)
            if position and position.quantity:
                if position.last_mark is None or position.last_mark_date is None:
                    raise ValueError("Unpriced held shares have no preceding observed market mark")
                if inst not in marks:
                    marks[inst] = position.last_mark
                    stale = sum(position.last_mark_date < day <= session for day in days)
                    if stale > policy.max_unknown_stale_sessions and inst not in verified_suspended:
                        raise ValueError("Unknown held opening-price gap exceeds declared session limit")
    equity = result.cash + sum((position.quantity * marks[inst] for inst, position in result.positions.items()), ZERO)
    pending_shares = {}
    for claim in result.entitlements.values():
        if claim.get("status") != "pending":
            continue
        if claim["kind"] == "cash_dividend":
            equity += decimal(claim["net_cash"])
        else:
            whole = int(decimal(claim["quantity"]).to_integral_value(rounding=ROUND_FLOOR))
            if whole:
                inst = claim["instrument"]
                pending_shares[inst] = pending_shares.get(inst, 0) + whole
                try:
                    mark = _reference(quotes[inst], session, policy, now=now)
                except (KeyError, ValueError):
                    mark = decimal(claim["last_mark"]) if claim.get("last_mark") is not None else marks.get(inst)
                if mark is None:
                    raise ValueError("Unpriced pending stock entitlement prevents opening NAV budgeting")
                equity += whole * mark
    target_shares = {inst: max(0, int((equity * weights.get(inst, ZERO) / reference).to_integral_value(rounding=ROUND_FLOOR)) - pending_shares.get(inst, 0))
                     for inst, (_, reference, _) in validated.items()}

    def order(inst, side, wanted):
        quote, reference, limits = validated[inst]
        pos = result.positions.get(inst, Position(0))
        try:
            fill = _fill_price(reference, side, limits, policy)
            available = pos.quantity - (pos.bought_today if pos.bought_date == session else 0)
            capped_wanted = wanted
            if side == "BUY" and metadata[inst].is_st and metadata[inst].board in {"sse_main", "szse_main", "chinext"}:
                already_bought = pos.bought_today if pos.bought_date == session else 0
                capped_wanted = min(wanted, max(0, 500000 - already_bought))
                if capped_wanted <= 0:
                    raise ValueError("Risk-warning daily 500000-share buy cap is exhausted")
            quantity = _sell_quantity(wanted, available, pos.quantity, limits) if side == "SELL" else _buy_quantity(capped_wanted, limits)
            if quantity <= 0:
                raise ValueError("T+1 lock or insufficient board-valid lot size")
            if side == "BUY":
                # Integer binary search includes rounded per-order minimum fees.
                step = limits["buy_step"]
                low, high = 0, quantity // step
                while low < high:
                    mid = (low + high + 1) // 2
                    fee = fees_for_order(mid * step, fill, side, policy)
                    if fee["notional"] + fee["total_fee"] <= result.cash:
                        low = mid
                    else:
                        high = mid - 1
                quantity = _buy_quantity(low * step, limits)
                if quantity <= 0:
                    raise ValueError("Cash cannot cover a board-valid lot plus rounded minimum fees")
            fee = fees_for_order(quantity, fill, side, policy)
            before = result.cash
            delta = -(fee["notional"] + fee["total_fee"]) if side == "BUY" else fee["notional"] - fee["total_fee"]
            if before + delta < 0:
                raise ValueError("Sale proceeds cannot cover its order fees without negative cash")
            result.cash = money(before + delta)
            if side == "BUY":
                pos.quantity += quantity
                pos.bought_today = (pos.bought_today if pos.bought_date == session else 0) + quantity
                pos.bought_date = session
            else:
                pos.quantity -= quantity
            pos.last_mark, pos.last_mark_date, pos.last_mark_basis = reference, session, quote.execution_price_basis
            if pos.quantity:
                result.positions[inst] = pos
            else:
                result.positions.pop(inst, None)
            row = {"session": session, "signal_date": signal_date, "instrument": inst, "side": side,
                "quantity": quantity, "requested_quantity": wanted, "unfilled_quantity": wanted - quantity,
                "reference_price": reference, "execution_price": fill,
                "execution_price_basis": quote.execution_price_basis, "source": quote.source, "quote_asof": quote.asof,
                **fee, "slippage_cost": money(quantity * abs(fill - reference)), "cash_before": before,
                "cash_change": delta, "cash_after": result.cash, "upper_limit": limits["upper"],
                "lower_limit": limits["lower"], "status": "simulated_fill", "live_order": False}
            trades.append(_json(row)); orders.append(_json(row))
        except ValueError as exc:
            orders.append({"session": session, "signal_date": signal_date, "instrument": inst, "side": side,
                           "requested_quantity": wanted, "status": "rejected", "reason": str(exc)})

    for inst in sorted(validated):
        held = result.positions.get(inst, Position(0)).quantity
        if held > target_shares[inst]:
            order(inst, "SELL", held - target_shares[inst])
    deficits = [(inst, max(0, target_shares[inst] - result.positions.get(inst, Position(0)).quantity)) for inst in validated]
    deficits.sort(key=lambda pair: (-pair[1] * validated[pair[0]][1], pair[0]))
    for inst, wanted in deficits:
        if wanted:
            order(inst, "BUY", wanted)
    for inst, position in result.positions.items():
        if inst in validated:
            quote, reference, _ = validated[inst]
            position.last_mark, position.last_mark_date, position.last_mark_basis = reference, session, quote.execution_price_basis
    return ExecutionResult(result, orders, trades, money(equity))


def mark_to_close(state: PortfolioState, *, session: str, prices: Mapping[str, object | None],
        calendar: Sequence[str], policy=None, verified_suspended: Sequence[str] = ()) -> ValuationResult:
    """Mark actual raw close, or carry a disclosed observed mark without filling.

    Residual corporate-action claims remain unspendable. The conservative NAV
    excludes uncertain fractional allocation; the estimate and <one-share-per-
    event upper bound are separately exposed, never represented as exact wealth.
    """
    policy = policy or ExecutionPolicy()
    days = _calendar(calendar, session)
    result = _advance(state, session, days)
    lower, rows, claim_rows, complete = result.cash, [], [], True
    for inst, position in result.positions.items():
        observed = prices.get(inst)
        if observed is not None:
            price = decimal(observed)
            if price <= 0:
                raise ValueError("Invalid observed raw closing price")
            position.last_mark, position.last_mark_date, position.last_mark_basis = price, session, "observed_raw_close"
        elif position.last_mark is None or position.last_mark_date is None:
            raise ValueError("Missing held close has no previous observed mark")
        stale = sum(position.last_mark_date < day <= session for day in days)
        if stale > policy.max_unknown_stale_sessions and inst not in verified_suspended:
            raise ValueError("Unknown held raw-price gap exceeds declared session limit")
        value = money(position.quantity * position.last_mark)
        lower += value
        complete = complete and observed is not None
        rows.append(_json({"session": session, "instrument": inst, "quantity": position.quantity,
            "available_quantity": position.quantity - (position.bought_today if position.bought_date == session else 0),
            "price": position.last_mark, "price_date": position.last_mark_date, "market_value": value,
            "valuation_source": "observed_raw_close" if observed is not None else "carried_observed_mark",
            "last_mark_basis": position.last_mark_basis,
            "stale_sessions": stale, "verified_suspended_entitlement": inst in verified_suspended}))
    claim_estimate, claim_upper = ZERO, ZERO
    for identity, claim in result.entitlements.items():
        if claim.get("status") != "pending":
            continue
        if claim["kind"] == "cash_dividend":
            value = decimal(claim["net_cash"])
            lower += value
            claim_rows.append({**claim, "event_id": identity, "estimated_value": str(value), "spendable": False})
            continue
        inst = claim["instrument"]
        exact = decimal(claim["quantity"])
        if exact == 0:
            continue
        observed = prices.get(inst)
        held_mark = result.positions.get(inst)
        price = decimal(observed) if observed is not None else decimal(claim["last_mark"]) if claim.get("last_mark") is not None else held_mark.last_mark if held_mark else None
        if price is None or price <= 0:
            raise ValueError("Pending bonus-share entitlement has no source mark")
        if observed is not None:
            claim.update(last_mark=str(price), last_mark_date=session)
        whole = int(exact.to_integral_value(rounding=ROUND_FLOOR))
        lower += money(whole * price)
        estimate = money((exact - whole) * price)
        upper = money(price) if exact != whole else ZERO
        claim_estimate += estimate; claim_upper += upper
        complete = complete and observed is not None and exact == whole
        claim_rows.append({**claim, "event_id": identity, "estimated_value": str(money(exact * price)),
                           "integer_lower_value": str(money(whole * price)), "spendable": False})
    for identity, claim in result.claims.items():
        inst = claim["instrument"]
        observed = prices.get(inst)
        price = decimal(observed) if observed is not None else decimal(claim["last_mark"]) if claim.get("last_mark") is not None else None
        if price is None or price <= 0:
            raise ValueError("Residual share claim has no positive valuation evidence")
        if observed is not None:
            claim.update(last_mark=str(price), last_mark_date=session)
        estimate = money(decimal(claim["quantity"]) * price) + decimal(claim.get("cash_estimate", "0"))
        upper = money(decimal(claim.get("maximum_additional_integer_shares", 1)) * price) + decimal(claim.get("cash_upper_bound", "0"))
        claim_estimate += estimate; claim_upper += upper
        complete = False
        claim_rows.append({**claim, "action_id": identity, "estimated_value": str(estimate), "upper_value_bound": str(upper), "spendable": False})
    history = {inst: position.quantity for inst, position in result.positions.items()}
    if session in result.holdings_history and result.holdings_history[session] != history:
        raise ValueError("A finalized record-date holding snapshot cannot be overwritten")
    result.holdings_history[session] = history
    return ValuationResult(result, money(lower), money(lower + claim_estimate), money(lower + claim_upper), complete, rows, claim_rows)


def _event(state, session, calendar, identity, payload):
    if not isinstance(identity, str) or not identity:
        raise ValueError("A unique economic event id is required")
    digest = _digest(payload)
    existing = state.applied_actions.get(identity)
    if existing:
        if existing["sha256"] != digest:
            raise ValueError("Economic event id was already applied with different content")
        return PortfolioState.from_dict(state.to_dict()), existing["record"], digest
    return _advance(state, session, calendar), None, digest


def _finish_event(state, identity, digest, record):
    record = _json(record)
    state.applied_actions[identity] = {"sha256": digest, "record": record}
    return ActionResult(state, record, False)


def _claim(state, identity, inst, remainder, session, price=None):
    if remainder:
        state.claims[identity] = {"instrument": inst, "quantity": str(remainder), "created_date": session,
            "last_mark": str(price) if price is not None else None, "last_mark_date": session if price is not None else None,
            "allocation": "unresolved_CSDC_fractional_allocation", "maximum_additional_integer_shares": 1,
            "conservative_integer_allocation": 0, "spendable": False}


def _adjust_residual_claims(state, instrument, record_date, *, cash_per_share=None, tax_rate=None, bonus_ratio=None):
    """Propagate later rights on uncertain allocation, without inventing cash.

    A possible extra share can itself earn later dividends/bonus shares. Ignoring
    those would make the reported upper bound too small. These values remain
    frozen estimates/bounds, never physical portfolio shares or spendable cash.
    """
    for claim in state.claims.values():
        if claim["instrument"] != instrument or claim["created_date"] > record_date:
            continue
        quantity = decimal(claim["quantity"])
        maximum = decimal(claim.get("maximum_additional_integer_shares", 1))
        if cash_per_share is not None:
            gross_estimate = money(quantity * cash_per_share)
            gross_upper = money(maximum * cash_per_share)
            claim["cash_estimate"] = str(decimal(claim.get("cash_estimate", "0")) + gross_estimate - money(gross_estimate * tax_rate))
            claim["cash_upper_bound"] = str(decimal(claim.get("cash_upper_bound", "0")) + gross_upper - money(gross_upper * tax_rate))
        if bonus_ratio is not None:
            claim["quantity"] = str(quantity * (ONE + bonus_ratio))
            claim["maximum_additional_integer_shares"] = int((maximum * (ONE + bonus_ratio)).to_integral_value(rounding=ROUND_CEILING))


def _evidence(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("Economic terms require an explicit source evidence SHA256")


def _cash_terms(instrument, record_date, pay_date, cash_per_share, tax_rate, evidence_sha256):
    _evidence(evidence_sha256)
    if cash_per_share < 0 or not 0 <= tax_rate <= 1 or not _day(record_date) < _day(pay_date):
        raise ValueError("Invalid verified cash-dividend terms")
    return {"kind": "cash_dividend", "instrument": instrument, "record_date": record_date,
            "pay_date": pay_date, "cash_per_share": cash_per_share, "tax_rate": tax_rate,
            "evidence_sha256": evidence_sha256}


def _adjust_ex_mark(state, instrument, session):
    """An action-adjusted stale valuation is never an executable source quote."""
    position = state.positions.get(instrument)
    if position is None or position.last_mark is None or position.last_mark_date is None:
        return
    if position.last_mark_date >= session and position.last_mark_basis != "corporate_action_adjusted_carried_mark":
        return  # Already marked by a real ex-date source price, not a pre-ex close.
    matching = [row for row in state.entitlements.values() if row["instrument"] == instrument and row.get("ex_date") == session]
    saved = next((row["pre_ex_carried_mark"] for row in matching if row.get("pre_ex_carried_mark") is not None), None)
    base = decimal(saved) if saved is not None else position.last_mark
    cash = sum((decimal(row["cash_per_share"]) for row in matching if row["kind"] == "cash_dividend"), ZERO)
    additional = sum((decimal(row["additional_shares_per_record_share"]) for row in matching if row["kind"] == "bonus_shares"), ZERO)
    adjusted = (base - cash) / (ONE + additional)
    if adjusted <= 0:
        raise ValueError("Verified ex-date terms cannot produce a positive derived stale valuation")
    position.last_mark = adjusted
    position.last_mark_basis = "corporate_action_adjusted_carried_mark"
    # Keep last_mark_date at the last actual observation. The adjustment does
    # not turn a halted quote into a fresh source price or reset the stale cap.
    for row in matching:
        row["pre_ex_carried_mark"] = str(base)
        row["action_adjusted_carried_mark"] = str(adjusted)


def register_dividend_entitlement(state: PortfolioState, *, event_id: str, instrument: str,
        record_date: str, ex_date: str, pay_date: str, cash_per_share, session: str,
        known_at: str, evidence_sha256: str, calendar: Sequence[str], tax_rate=Decimal("0.20"), now=None) -> ActionResult:
    """Recognize net dividend receivable on ex-date, never spend it before pay-date."""
    terms = _cash_terms(instrument, record_date, pay_date, decimal(cash_per_share), decimal(tax_rate), evidence_sha256)
    if not record_date < _day(ex_date) <= pay_date:
        raise ValueError("Cash dividend record/ex/payment dates are inconsistent")
    payload = {**terms, "ex_date": ex_date, "session": session, "known_at": known_at}
    result, existing, digest = _event(state, session, calendar, "register:" + event_id, payload)
    if existing:
        return ActionResult(result, existing, True)
    if session != ex_date or not _known(known_at, session, now=now):
        raise ValueError("Dividend entitlement must be recognized on its known ex-date")
    quantity = _record_quantity(result, record_date, instrument)
    gross = money(quantity * terms["cash_per_share"]); tax = money(gross * terms["tax_rate"])
    record = {**_json(payload), "record_date_shares": quantity, "gross_cash": str(gross), "tax": str(tax),
              "net_cash": str(gross - tax), "status": "pending", "spendable": False, "terms_sha256": _digest(terms)}
    result.entitlements[event_id] = record
    _adjust_residual_claims(result, instrument, record_date, cash_per_share=terms["cash_per_share"], tax_rate=terms["tax_rate"])
    _adjust_ex_mark(result, instrument, session)
    return _finish_event(result, "register:" + event_id, digest, {**record, "cash_change": 0})


def _bonus_terms(instrument, record_date, listing_date, ratio, evidence_sha256):
    _evidence(evidence_sha256)
    if ratio < 0 or not _day(record_date) < _day(listing_date):
        raise ValueError("Invalid verified additional-share terms")
    return {"kind": "bonus_shares", "instrument": instrument, "record_date": record_date,
            "listing_date": listing_date, "additional_shares_per_record_share": ratio, "evidence_sha256": evidence_sha256}


def register_bonus_entitlement(state: PortfolioState, *, event_id: str, instrument: str,
        record_date: str, ex_date: str, listing_date: str, ratio, session: str,
        known_at: str, evidence_sha256: str, calendar: Sequence[str], price=None, now=None) -> ActionResult:
    """Keep ex-date to listing-date stock rights as frozen, marked entitlements."""
    terms = _bonus_terms(instrument, record_date, listing_date, decimal(ratio), evidence_sha256)
    if not record_date < _day(ex_date) <= listing_date:
        raise ValueError("Bonus record/ex/listing dates are inconsistent")
    payload = {**terms, "ex_date": ex_date, "session": session, "known_at": known_at}
    result, existing, digest = _event(state, session, calendar, "register:" + event_id, payload)
    if existing:
        return ActionResult(result, existing, True)
    if session != ex_date or not _known(known_at, session, now=now):
        raise ValueError("Bonus entitlement must be recognized on its known ex-date")
    quantity = _record_quantity(result, record_date, instrument)
    exact = quantity * terms["additional_shares_per_record_share"]
    record = {**_json(payload), "record_date_shares": quantity, "quantity": str(exact), "status": "pending",
              "spendable": False, "terms_sha256": _digest(terms), "last_mark": str(decimal(price)) if price is not None else None,
              "last_mark_date": session if price is not None else None}
    result.entitlements[event_id] = record
    _adjust_residual_claims(result, instrument, record_date, bonus_ratio=terms["additional_shares_per_record_share"])
    _adjust_ex_mark(result, instrument, session)
    return _finish_event(result, "register:" + event_id, digest, {**record, "cash_change": 0})


def apply_share_exchange(state: PortfolioState, *, action_id: str, from_instrument: str, to_instrument: str,
        ratio, session: str, tradable_from: str, confirmed_known_at: str, evidence_sha256: str,
        calendar: Sequence[str], successor_price=None, now=None) -> ActionResult:
    """Convert real integer raw shares; floor plus a frozen uncertain remainder.

    Ordinary public non-locked holdings only. Registration dates do not unlock
    shares; conversion is deferred until confirmed admission. Old Qlib virtual
    units/factors must never be passed as raw quantity or this ratio.
    """
    ratio = decimal(ratio)
    _evidence(evidence_sha256)
    if ratio <= 0 or from_instrument == to_instrument:
        raise ValueError("Invalid verified raw-share exchange")
    payload = {"kind": "share_exchange", "from": from_instrument, "to": to_instrument, "ratio": ratio,
        "session": session, "tradable_from": tradable_from, "confirmed_known_at": confirmed_known_at, "evidence_sha256": evidence_sha256}
    result, existing, digest = _event(state, session, calendar, action_id, payload)
    if existing:
        return ActionResult(result, existing, True)
    if session < _day(tradable_from) or not _known(confirmed_known_at, session, now=now):
        raise ValueError("Share exchange is not yet confirmed and tradable")
    if any(claim["instrument"] == from_instrument for claim in result.claims.values()) or any(
            row["instrument"] == from_instrument and row.get("kind") == "bonus_shares" and row.get("status") == "pending" and decimal(row["quantity"]) > 0
            for row in result.entitlements.values()):
        raise ValueError("An exchange overlapping unresolved stock entitlements needs a separately verified allocation policy")
    position = result.positions.pop(from_instrument, Position(0))
    entitlement = position.quantity * ratio
    whole = int(entitlement.to_integral_value(rounding=ROUND_FLOOR))
    remainder = entitlement - whole
    if whole:
        successor = result.positions.get(to_instrument, Position(0))
        successor.quantity += whole
        # Admitted corporate-action shares are not a same-day purchase; existing
        # same-day bought shares retain their own T+1 lock.
        if successor_price is not None:
            successor.last_mark, successor.last_mark_date, successor.last_mark_basis = decimal(successor_price), session, "observed_successor_price"
        result.positions[to_instrument] = successor
    _claim(result, action_id, to_instrument, remainder, session, successor_price)
    return _finish_event(result, action_id, digest, {**payload, "old_raw_shares": position.quantity,
        "exact_new_raw_entitlement": entitlement, "new_integer_shares": whole, "unresolved_fractional_claim": remainder,
        "cash_change": 0, "fee": 0, "allocation_policy": "floor_integer_plus_unresolved_claim_not_CSDC_random_allocation"})


def _record_quantity(state, record_date, instrument):
    if record_date not in state.holdings_history:
        raise ValueError("No finalized record-date holding snapshot; cannot infer entitlement from payment-day holdings")
    return state.holdings_history[record_date].get(instrument, 0)


def apply_cash_dividend(state: PortfolioState, *, event_id: str, instrument: str, record_date: str,
        pay_date: str, cash_per_share, session: str, known_at: str, evidence_sha256: str,
        calendar: Sequence[str], tax_rate=Decimal("0.20"), now=None) -> ActionResult:
    cash_per_share, tax_rate = decimal(cash_per_share), decimal(tax_rate)
    terms = _cash_terms(instrument, record_date, pay_date, cash_per_share, tax_rate, evidence_sha256)
    payload = {"kind": "cash_dividend", "instrument": instrument, "record_date": record_date,
        "pay_date": pay_date, "cash_per_share": cash_per_share, "tax_rate": tax_rate,
        "session": session, "known_at": known_at, "evidence_sha256": evidence_sha256}
    result, existing, digest = _event(state, session, calendar, event_id, payload)
    if existing:
        return ActionResult(result, existing, True)
    if session != pay_date or not _known(known_at, session, now=now):
        raise ValueError("Cash dividend can be posted only on its evidenced payment date")
    entitlement = result.entitlements.get(event_id)
    if not entitlement or entitlement.get("terms_sha256") != _digest(terms) or entitlement.get("status") != "pending":
        raise ValueError("Cash dividend requires its matching previously registered ex-date entitlement")
    quantity = _record_quantity(result, record_date, instrument)
    gross = money(quantity * cash_per_share); tax = money(gross * tax_rate); net = gross - tax
    result.cash += net
    entitlement["status"] = "settled"
    return _finish_event(result, event_id, digest, {**payload, "record_date_shares": quantity, "gross_cash": gross,
        "tax": tax, "net_cash": net, "cash_after": result.cash,
        "tax_assumption": "fixed_conservative_rate_not_holding_period_personal_tax"})


def apply_bonus_shares(state: PortfolioState, *, event_id: str, instrument: str, record_date: str,
        listing_date: str, ratio, session: str, known_at: str, evidence_sha256: str,
        calendar: Sequence[str], price=None, now=None) -> ActionResult:
    ratio = decimal(ratio)
    terms = _bonus_terms(instrument, record_date, listing_date, ratio, evidence_sha256)
    payload = {"kind": "bonus_shares", "instrument": instrument, "record_date": record_date,
        "listing_date": listing_date, "additional_shares_per_record_share": ratio,
        "session": session, "known_at": known_at, "evidence_sha256": evidence_sha256}
    result, existing, digest = _event(state, session, calendar, event_id, payload)
    if existing:
        return ActionResult(result, existing, True)
    if session != listing_date or not _known(known_at, session, now=now):
        raise ValueError("Additional shares cannot trade before their confirmed listing date")
    entitlement_record = result.entitlements.get(event_id)
    if not entitlement_record or entitlement_record.get("terms_sha256") != _digest(terms) or entitlement_record.get("status") != "pending":
        raise ValueError("Additional shares require their matching registered ex-date entitlement")
    record_quantity = _record_quantity(result, record_date, instrument)
    entitlement = record_quantity * ratio
    whole = int(entitlement.to_integral_value(rounding=ROUND_FLOOR)); remainder = entitlement - whole
    if whole:
        position = result.positions.get(instrument, Position(0)); position.quantity += whole
        if price is not None:
            position.last_mark, position.last_mark_date, position.last_mark_basis = decimal(price), session, "observed_bonus_listing_price"
        result.positions[instrument] = position
    _claim(result, event_id, instrument, remainder, session, price)
    entitlement_record["status"] = "settled"
    return _finish_event(result, event_id, digest, {**payload, "record_date_shares": record_quantity,
        "exact_additional_entitlement": entitlement, "new_integer_shares": whole,
        "unresolved_fractional_claim": remainder, "cash_change": 0, "fee": 0})
