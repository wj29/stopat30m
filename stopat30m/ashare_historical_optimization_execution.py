"""Historical 2023--2024 copy of A3 rank buffering, using the A4 ledger.

The source module is stopat30m/ashare_optimization_execution.py; only this
module docstring, the explicit engine import and audit policy name differ.

This is a research adapter, not an alternative fee or fill model.  Incumbent
shares are kept unchanged while a separate tradable sleeve holds exits and new
entries.  The original engine executes that sleeve once, and values the merged
portfolio at the close.  A missing or limit-blocked opening never causes a new
stock to be substituted into a signal that has already been frozen.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from typing import Mapping, Sequence

from stopat30m.backtest import ashare_historical_execution as ex


POLICY = "a4-prior-close-rank-buffer-integer-sleeve-historical-v1"


@dataclass(frozen=True)
class RankBufferPolicy:
    top_k: int = 20
    keep_rank: int = 40
    require_signal_affordability: bool = False

    def __post_init__(self):
        if (any(isinstance(value, bool) or not isinstance(value, int) or value < 1
                for value in (self.top_k, self.keep_rank)) or self.keep_rank < self.top_k
                or type(self.require_signal_affordability) is not bool):
            raise ValueError("Rank buffering requires positive top_k and keep_rank >= top_k")


@dataclass(frozen=True)
class RankBufferPlan:
    signal_date: str
    selected: tuple[str, ...]
    retained: tuple[str, ...]
    exits: tuple[str, ...]
    # Tuples make the prior-close quantity/price instructions immutable.
    entries: tuple[tuple[str, int, str, str], ...]
    prior_equity_cny: str
    budget_per_slot_cny: str
    filtered: tuple[tuple[str, str], ...]
    policy: RankBufferPolicy

    def to_dict(self):
        return {"policy_version": POLICY, "signal_date": self.signal_date,
            "selected": list(self.selected), "retained": list(self.retained), "exits": list(self.exits),
            "entries": [{"instrument": inst, "quantity": quantity, "signal_raw_close": price,
                "board": board} for inst, quantity, price, board in self.entries],
            "prior_equity_cny": self.prior_equity_cny, "budget_per_slot_cny": self.budget_per_slot_cny,
            "filtered": [{"instrument": inst, "reason": reason} for inst, reason in self.filtered],
            "top_k": self.policy.top_k, "keep_rank": self.policy.keep_rank,
            "require_signal_affordability": self.policy.require_signal_affordability}


@dataclass
class RankBufferExecution:
    execution: ex.ExecutionResult
    audit: dict

    @property
    def state(self):
        return self.execution.state

    @property
    def orders(self):
        return self.execution.orders

    @property
    def trades(self):
        return self.execution.trades

    @property
    def opening_equity(self):
        return self.execution.opening_equity


def _lot(board):
    # These are planning constraints only.  The frozen engine independently
    # validates date-specific security metadata, limits, caps, and actual fills.
    if board not in ex.BOARDS:
        raise ValueError("An explicit supported A-share board is required for prior-close budgeting")
    minimum = 200 if board == "star" else 100
    step = 1 if board in {"star", "bse"} else 100
    maximum = 100000 if board == "star" else 300000 if board == "chinext" else 1000000
    return minimum, step, maximum


def _board_quantity(quantity, board):
    minimum, step, maximum = _lot(board)
    value = max(0, min(int(quantity), maximum)) // step * step
    return value if value >= minimum else 0


def _affordable_quantity(budget, price, board, execution_policy):
    """Freeze a board-valid quantity using only the preceding raw close.

    Use the same adverse tick and fees as the original engine for the budget,
    without pretending this planning price will be tomorrow's observed fill.
    """
    _, step, maximum = _lot(board)
    planning_fill = (price * (1 + execution_policy.slippage_bps / 10000)).quantize(
        Decimal("0.01"), rounding=ROUND_CEILING)
    low, high = 0, min(int(budget / planning_fill), maximum) // step
    while low < high:
        middle = (low + high + 1) // 2
        fees = ex.fees_for_order(middle * step, planning_fill, "BUY", execution_policy)
        if fees["notional"] + fees["total_fee"] <= budget:
            low = middle
        else:
            high = middle - 1
    return _board_quantity(low * step, board)


def build_rank_buffer_plan(state: ex.PortfolioState, *, signal_date: str,
        ranked_instruments: Sequence[str], raw_signal_prices: Mapping[str, object],
        boards: Mapping[str, str], signal_equity, policy=None, execution_policy=None):
    """Select from an already eligible, deterministically sorted prior-close rank.

    Caller owns historical membership, ranking, and source-time validation.  No
    execution-day quote is accepted here.  Current actual quantities may include
    independently verified corporate actions applied before the next opening;
    retaining a name preserves those actual quantities rather than reweighting.
    """
    ex._day(signal_date)
    policy, execution_policy = policy or RankBufferPolicy(), execution_policy or ex.ExecutionPolicy()
    ranked = list(ranked_instruments)
    if len(ranked) != len(set(ranked)) or any(not isinstance(inst, str) or not inst for inst in ranked):
        raise ValueError("Prior-close rankings must contain unique instrument identities")
    equity = ex.decimal(signal_equity)
    if equity <= 0:
        raise ValueError("A positive preceding-close conservative equity is required")
    budget = equity / policy.top_k
    incumbents = {inst for inst, position in state.positions.items() if position.quantity}
    incumbents.update(row["instrument"] for row in state.entitlements.values()
        if row.get("kind") == "bonus_shares" and row.get("status") == "pending"
        and ex.decimal(row["quantity"]) > 0)
    ranks = {inst: rank for rank, inst in enumerate(ranked, 1)}
    retained = tuple(sorted((inst for inst in incumbents if ranks.get(inst, policy.keep_rank+1) <= policy.keep_rank),
        key=lambda inst: (ranks[inst], inst))[:policy.top_k])
    selected, entries, filtered = list(retained), [], []
    for inst in ranked:
        if len(selected) >= policy.top_k:
            break
        if inst in incumbents:
            continue
        if inst not in raw_signal_prices or inst not in boards:
            raise ValueError("Every candidate needs a preceding raw close and explicit board")
        price = ex.decimal(raw_signal_prices[inst])
        if price <= 0 or price != ex.money(price):
            raise ValueError("Prior-close planning prices must be positive raw CNY ticks")
        board = boards[inst]
        quantity = _affordable_quantity(budget, price, board, execution_policy)
        if quantity == 0 and policy.require_signal_affordability:
            filtered.append((inst, "signal_budget_cannot_cover_minimum_board_lot_and_fees"))
            continue
        selected.append(inst)
        entries.append((inst, quantity, str(price), board))
    return RankBufferPlan(signal_date, tuple(selected), retained, tuple(sorted(incumbents-set(retained))),
        tuple(entries), str(equity), str(budget), tuple(filtered), policy)


def execute_rank_buffer_plan(state: ex.PortfolioState, *, plan: RankBufferPlan, session: str,
        quotes: Mapping[str, ex.OpeningQuote], metadata: Mapping[str, ex.SecurityInfo],
        calendar: Sequence[str], policy=None, now=None, verified_suspended: Sequence[str] = ()):
    """Execute exits/new names once; never submit retained shares to reweighting.

    A pure engine probe provides exactly the original opening NAV and validates
    held stale-price limits. Its hypothetical orders are discarded; only the
    second engine call below produces the returned cash/trade ledger.  No probe
    changes its input state or performs I/O.
    """
    policy = policy or ex.ExecutionPolicy()
    if plan.signal_date >= session or plan.signal_date not in calendar:
        raise ValueError("A rank-buffer plan must be frozen on a preceding real exchange session")
    if set(plan.retained) & {inst for inst, *_ in plan.entries}:
        raise ValueError("Retained and new-entry sleeves cannot overlap")
    active = {inst for inst, position in state.positions.items() if position.quantity}
    active.update(row["instrument"] for row in state.entitlements.values()
        if row.get("kind") == "bonus_shares" and row.get("status") == "pending"
        and ex.decimal(row["quantity"]) > 0)
    if active-set(plan.retained)-set(plan.exits):
        raise ValueError("Portfolio exposure changed after the rank-buffer plan was frozen")
    original = ex._advance(state, session, calendar)
    targets_for_probe = {inst: 0 for inst, *_ in plan.entries}
    full_probe = ex.execute_target_weights(original, session=session, signal_date=plan.signal_date,
        targets=targets_for_probe, quotes=quotes, metadata=metadata, calendar=calendar,
        policy=policy, now=now, verified_suspended=verified_suspended)
    retained = set(plan.retained)
    sleeve = ex.PortfolioState.from_dict(original.to_dict())
    kept_positions = {inst: sleeve.positions.pop(inst) for inst in sorted(retained) if inst in sleeve.positions}
    kept_entitlements = {identity: sleeve.entitlements.pop(identity) for identity, row in list(sleeve.entitlements.items())
        if row.get("instrument") in retained}
    kept_claims = {identity: sleeve.claims.pop(identity) for identity, row in list(sleeve.claims.items())
        if row.get("instrument") in retained}
    sleeve_probe = ex.execute_target_weights(sleeve, session=session, signal_date=plan.signal_date,
        targets=targets_for_probe, quotes=quotes, metadata=metadata, calendar=calendar,
        policy=policy, now=now, verified_suspended=verified_suspended)
    equity = sleeve_probe.opening_equity
    references, missing = {}, []
    for inst, quantity, signal_price, _ in plan.entries:
        try:
            references[inst] = ex._reference(quotes[inst], session, policy, now=now)
        except (KeyError, ValueError):
            # Reserve its original allocation rather than using an unavailable
            # opening to resize or substitute another stock upward.
            references[inst] = ex.decimal(signal_price)
            missing.append(inst)
    wanted = sum((quantity * references[inst] for inst, quantity, _, _ in plan.entries), Decimal(0))
    scale = min(Decimal(1), equity/wanted) if wanted else Decimal(1)
    quantities = {inst: _board_quantity(Decimal(quantity)*scale, board)
        for inst, quantity, _, board in plan.entries}
    weights = {}
    # Extra division precision avoids rounding an exact planned 100-share lot
    # below its integer boundary in the engine's usual 28-digit multiplication.
    # ROUND_FLOOR never inflates a share instruction or creates leverage.
    with localcontext() as context:
        context.prec = 80
        context.rounding = ROUND_FLOOR
        for inst, quantity, _, _ in plan.entries:
            weights[inst] = Decimal(quantity)*references[inst]/equity if equity else Decimal(0)
    # Conservative, explicitly audited fallback for a sum rounded up at the
    # original engine's Decimal precision. No incumbent is involved.
    rounding_reduction = Decimal(0)
    if sum(weights.values(), Decimal(0)) > 1:
        key = max(weights, key=lambda inst: (weights[inst], inst))
        rounding_reduction = sum(weights.values(), Decimal(0))-1+Decimal("1e-27")
        weights[key] = max(Decimal(0), weights[key]-rounding_reduction)
    result = ex.execute_target_weights(sleeve, session=session, signal_date=plan.signal_date,
        targets=weights, quotes=quotes, metadata=metadata, calendar=calendar,
        policy=policy, now=now, verified_suspended=verified_suspended)
    if any(row["instrument"] in retained for row in result.trades):
        raise ValueError("A retained share escaped the isolated execution sleeve")
    result.state.positions.update(kept_positions)
    result.state.entitlements.update(kept_entitlements)
    result.state.claims.update(kept_claims)
    result.opening_equity = full_probe.opening_equity
    expected_cash = original.cash + sum((ex.decimal(row["cash_change"]) for row in result.trades), Decimal(0))
    if result.state.cash != expected_cash or result.state.cash < 0:
        raise ValueError("Merged sleeve cash does not reconcile to original engine fills")
    if result.state.applied_actions != original.applied_actions or result.state.holdings_history != original.holdings_history:
        raise ValueError("Execution may not rewrite corporate-action or record-date history")
    return RankBufferExecution(result, {"policy_version": POLICY, "plan": plan.to_dict(),
        "sleeve_opening_equity_cny": str(equity), "full_opening_equity_cny": str(full_probe.opening_equity),
        "opening_budget_scale": str(scale), "opening_quantities_after_budget_reduction": quantities,
        "unavailable_opening_references": missing, "weight_sum": str(sum(weights.values(), Decimal(0))),
        "weight_rounding_reduction": str(rounding_reduction), "opening_replacements": [],
        "retained_shares": {inst: position.quantity for inst, position in kept_positions.items()}})
