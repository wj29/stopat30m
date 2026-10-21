"""A11-only gross-notional caps around the unchanged rank execution ledger.

The preceding-close plan supplies the denominator and the ordered names.  Each
name receives a monetary reservation before lot/T+1 checks; unused reservations
are never recycled.  Protected exit shares stay outside the execution sleeve.
No fallback price is ever submitted as an observed execution quote.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Mapping, Sequence

from stopat30m import ashare_stability_rank_execution as rank
from stopat30m.backtest import ashare_execution as ex


POLICY = "a11-prior-nav-independent-gross-side-caps-v1"
FRACTIONS = ("0.05", "0.075", "0.10")


def _floor_cent(value):
    return ex.decimal(value).quantize(ex.CENT, rounding=ROUND_FLOOR)


def _validated_reference(inst, quotes, metadata, session, calendar, policy, now):
    """Use exactly the original engine's source/identity/clock checks."""
    quote, info = quotes[inst], metadata[inst]
    if info.instrument != inst:
        raise ValueError("Instrument metadata identity mismatch")
    reference = ex._reference(quote, session, policy, now=now)
    limits = ex.price_limits(info, quote, session=session, calendar=calendar, now=now)
    return reference, limits


def _reference_or_reserve(inst, fallback, quotes, metadata, session, calendar, policy, now):
    try:
        reference, limits = _validated_reference(inst, quotes, metadata, session, calendar, policy, now)
        return reference, limits, "observed_execution_reference", None
    except (KeyError, ValueError) as exc:
        # Metadata can be unavailable while the actual raw reference is valid.
        # In both cases this branch only reserves money: it never grants a fill.
        try:
            reference = ex._reference(quotes[inst], session, policy, now=now)
            basis = "observed_reference_invalid_security_metadata_reservation_only"
        except (KeyError, ValueError):
            reference = ex.decimal(fallback)
            basis = "prior_observed_price_reservation_only"
        if reference <= 0:
            raise ValueError("A positive observed/frozen price is required for reservation")
        reason = "Missing quote/security metadata" if isinstance(exc, KeyError) else str(exc)
        return reference, None, basis, reason


def _assert_bridge(original, result, plan, approved):
    before = {inst: pos.quantity for inst, pos in original.positions.items()}
    expected = dict(before)
    for trade in result.trades:
        inst, side, quantity = trade["instrument"], trade["side"], trade["quantity"]
        if side == "SELL":
            if inst not in plan.exits or quantity > approved["SELL"].get(inst, 0):
                raise ValueError("An exit fill exceeded its frozen approved quantity")
            expected[inst] = expected.get(inst, 0) - quantity
        elif side == "BUY":
            if inst not in approved["BUY"] or quantity > approved["BUY"][inst]:
                raise ValueError("An entry fill exceeded its frozen approved quantity")
            expected[inst] = expected.get(inst, 0) + quantity
        else:
            raise ValueError("Unexpected execution side")
    expected = {inst: qty for inst, qty in expected.items() if qty}
    actual = {inst: pos.quantity for inst, pos in result.state.positions.items() if pos.quantity}
    if actual != expected or any(qty < 0 for qty in expected.values()):
        raise ValueError("Protected-share execution does not reconcile to original shares and fills")
    expected_cash = original.cash + sum((ex.decimal(row["cash_change"]) for row in result.trades), ex.ZERO)
    if result.state.cash != expected_cash or result.state.cash < 0:
        raise ValueError("Capital-capped execution cash does not reconcile to original engine fills")
    for name in ("entitlements", "claims", "applied_actions", "holdings_history"):
        if getattr(result.state, name) != getattr(original, name):
            raise ValueError("Capital-capped execution may not rewrite corporate-action or holding history")


def _audit(result, *, plan, fraction, budget, initial_build, allocations, protected,
        approved, copied_rejections):
    totals = {side: sum((ex.decimal(t["notional"]) for t in result.trades if t["side"] == side), ex.ZERO)
        for side in ("BUY", "SELL")}
    for side in ("BUY", "SELL"):
        fills = {}
        for trade in result.trades:
            if trade["side"] == side:
                fills[trade["instrument"]] = fills.get(trade["instrument"], ex.ZERO) + ex.decimal(trade["notional"])
        for row in allocations[side]:
            value = fills.get(row["instrument"], ex.ZERO)
            reserved = ex.decimal(row["reserved_cny"])
            if value > reserved:
                raise ValueError("Actual gross notional exceeded a frozen per-name reservation")
            row["actual_notional_cny"] = str(value)
            row["unused_reserved_cny"] = str(reserved - value)
        if not initial_build and (totals[side] > budget or sum(
                (ex.decimal(row["reserved_cny"]) for row in allocations[side]), ex.ZERO) > budget):
            raise ValueError("Actual gross turnover exceeded the independent prior-NAV side cap")
    return {"policy_version": POLICY, "prior_nav_cny": plan.prior_equity_cny,
        "fraction": fraction, "budget_per_side_cny": str(budget),
        "initial_build": initial_build, "cap_enforced": not initial_build,
        "initial_exemption_source": "caller_first_scheduled_batch" if initial_build else None,
        "initial_exemption_inferred_from_empty_holdings": False,
        "actual_buy_notional_cny": str(totals["BUY"]),
        "actual_sell_notional_cny": str(totals["SELL"]),
        "unused_buy_budget_cny": str(max(ex.ZERO, budget - totals["BUY"])),
        "unused_sell_budget_cny": str(max(ex.ZERO, budget - totals["SELL"])),
        "allocations": allocations, "no_reallocation": True,
        "reservation_rule": "ordered_min_requested_bound_remaining_before_lot_T1_or_rejection_no_recycling",
        "gross_measure_includes_execution_slippage": True, "fees_excluded_from_notional_cap": True,
        "partial_kept_shares": protected,
        "approved_quantities": approved,
        "validation_rejections_preserved": copied_rejections,
        "hard_cap_satisfied_or_initial_exempt": initial_build or all(v <= budget for v in totals.values()),
        "cash_and_share_bridge_verified": True,
        "original_plan": plan.to_dict()}


def execute_capital_turnover_plan(state: ex.PortfolioState, *, plan: rank.RankBufferPlan,
        session: str, quotes: Mapping[str, ex.OpeningQuote], metadata: Mapping[str, ex.SecurityInfo],
        calendar: Sequence[str], policy: ex.ExecutionPolicy, fraction: str, initial_build: bool,
        now=None, verified_suspended: Sequence[str] = ()) -> rank.RankBufferExecution:
    """Shrink only frozen instructions; each side has its own prior-NAV cap.

    ``initial_build`` is the caller's first *scheduled batch*, even if no order
    fills.  It is deliberately not inferred from holdings, cash or session.
    Subsequent calls, including empty-account reentry, must pass ``False``.
    """
    if type(fraction) is not str or fraction not in FRACTIONS:
        raise ValueError("Capital fraction must be a declared Decimal string 0.05/0.075/0.10")
    if type(initial_build) is not bool:
        raise ValueError("The first scheduled batch requires an explicit boolean initial_build")
    prior_nav = ex.decimal(plan.prior_equity_cny)
    if prior_nav <= 0:
        raise ValueError("A positive preceding-close NAV is required")
    budget = _floor_cent(prior_nav * Decimal(fraction))
    if initial_build:
        # Preserve every original ledger field and adapter audit on this control.
        result = rank.execute_rank_buffer_plan(state, plan=plan, session=session, quotes=quotes,
            metadata=metadata, calendar=calendar, policy=policy, now=now,
            verified_suspended=verified_suspended)
        result.audit["capital_turnover"] = _audit(result, plan=plan, fraction=fraction,
            budget=budget, initial_build=True, allocations={"BUY": [], "SELL": []},
            protected={}, approved={"BUY": {}, "SELL": {}}, copied_rejections=[])
        return result

    entry_names = [inst for inst, *_ in plan.entries]
    if (len(set(entry_names)) != len(entry_names) or len(set(plan.exits)) != len(plan.exits)
            or set(plan.retained) & set(plan.exits) or set(entry_names) & (set(plan.retained) | set(plan.exits))):
        raise ValueError("Frozen retained, exit and entry identities must be distinct")
    active = {inst for inst, position in state.positions.items() if position.quantity}
    active.update(row["instrument"] for row in state.entitlements.values()
        if row.get("kind") == "bonus_shares" and row.get("status") == "pending"
        and ex.decimal(row["quantity"]) > 0)
    if active - set(plan.retained) - set(plan.exits):
        raise ValueError("Portfolio exposure changed after the rank-buffer plan was frozen")
    original = ex._advance(state, session, calendar)
    targets = {inst: 0 for inst in entry_names}
    full_probe = ex.execute_target_weights(original, session=session, signal_date=plan.signal_date,
        targets=targets, quotes=quotes, metadata=metadata, calendar=calendar, policy=policy,
        now=now, verified_suspended=verified_suspended)
    sleeve = ex.PortfolioState.from_dict(original.to_dict())
    allocations, approved = {"BUY": [], "SELL": []}, {"BUY": {}, "SELL": {}}
    protected, protected_positions = {}, {}
    remaining = budget
    for inst in plan.exits:
        position = original.positions.get(inst)
        if position is None or not position.quantity:
            allocations["SELL"].append({"instrument": inst, "requested_quantity": 0,
                "capped_quantity": 0, "reserved_cny": "0", "requested_notional_bound_cny": "0",
                "capped_notional_bound_cny": "0", "basis": "pending_entitlement_no_tradable_position",
                "available_quantity": 0, "validation_reason": None})
            approved["SELL"][inst] = 0
            continue
        reference, limits, basis, reason = _reference_or_reserve(inst, position.last_mark,
            quotes, metadata, session, calendar, policy, now)
        wanted = position.quantity
        reserved = min(remaining, ex.money(wanted * reference))
        remaining -= reserved
        available = wanted - (position.bought_today if position.bought_date == session else 0)
        maximum = int((reserved / reference).to_integral_value(rounding=ROUND_FLOOR))
        # Crucially held/available refer to the *real full position*.  A virtual
        # small position must never invent a new odd-lot full-liquidation right.
        quantity = ex._sell_quantity(maximum, available, wanted, limits) if limits is not None else 0
        approved["SELL"][inst] = quantity
        allocations["SELL"].append({"instrument": inst, "requested_quantity": wanted,
            "available_quantity": available, "capped_quantity": quantity,
            "reserved_cny": str(reserved), "requested_notional_bound_cny": str(ex.money(wanted * reference)),
            "capped_notional_bound_cny": str(ex.money(quantity * reference)),
            "reference_price": str(reference), "basis": basis, "validation_reason": reason})
        kept = wanted - quantity
        if kept:
            protected[inst] = kept
            protected_position = replace(position, quantity=kept)
            if limits is not None:
                protected_position.last_mark = reference
                protected_position.last_mark_date = session
                protected_position.last_mark_basis = quotes[inst].execution_price_basis
            protected_positions[inst] = protected_position
        if quantity:
            sleeve.positions[inst] = replace(position, quantity=quantity, bought_today=0)
        else:
            sleeve.positions.pop(inst, None)

    remaining, entries = budget, []
    for inst, wanted, signal_price, board in plan.entries:
        if isinstance(wanted, bool) or not isinstance(wanted, int) or wanted < 0:
            raise ValueError("Frozen entry quantities must be nonnegative integer shares")
        reference, limits, basis, reason = _reference_or_reserve(inst, signal_price,
            quotes, metadata, session, calendar, policy, now)
        bound = (reference * (1 + policy.slippage_bps / 10000)).quantize(ex.CENT, rounding=ROUND_CEILING)
        reserved = min(remaining, ex.money(wanted * bound))
        remaining -= reserved
        maximum = int((reserved / bound).to_integral_value(rounding=ROUND_FLOOR))
        quantity = rank._board_quantity(min(wanted, maximum), board)
        approved["BUY"][inst] = quantity
        entries.append((inst, quantity, signal_price, board))
        allocations["BUY"].append({"instrument": inst, "requested_quantity": wanted,
            "capped_quantity": quantity, "reserved_cny": str(reserved),
            "requested_notional_bound_cny": str(ex.money(wanted * bound)),
            "capped_notional_bound_cny": str(ex.money(quantity * bound)),
            "reference_price": str(reference), "adverse_tick_bound": str(bound),
            "basis": basis, "validation_reason": reason})

    capped_plan = replace(plan, entries=tuple(entries))
    result = rank.execute_rank_buffer_plan(sleeve, plan=capped_plan, session=session,
        quotes=quotes, metadata=metadata, calendar=calendar, policy=policy, now=now,
        verified_suspended=verified_suspended)
    for inst, kept in protected_positions.items():
        remainder = result.state.positions.get(inst)
        merged = replace(kept, quantity=kept.quantity + (remainder.quantity if remainder else 0))
        if remainder:
            # Approved shares are entirely T+1 available; all original locks
            # remained protected.  Engine-rejected approved shares return here.
            if remainder.bought_today:
                raise ValueError("An exit-only virtual sleeve acquired unexpected new locked shares")
            if remainder.last_mark is not None:
                merged.last_mark, merged.last_mark_date, merged.last_mark_basis = (
                    remainder.last_mark, remainder.last_mark_date, remainder.last_mark_basis)
        result.state.positions[inst] = merged
    # A protected invalid quote must retain the engine's ordinary validation
    # rejection even when its entire position was kept outside the sleeve.
    copied_rejections = []
    for row in full_probe.orders:
        if (row.get("status") == "rejected" and "side" not in row
                and row["instrument"] in protected_positions and row not in result.orders):
            result.orders.append(dict(row))
            copied_rejections.append(row["instrument"])
    result.audit["capital_virtual_opening_equity_cny"] = result.audit["full_opening_equity_cny"]
    result.audit["full_opening_equity_cny"] = str(full_probe.opening_equity)
    result.execution.opening_equity = full_probe.opening_equity
    _assert_bridge(original, result, plan, approved)
    result.audit["capital_turnover"] = _audit(result, plan=plan, fraction=fraction,
        budget=budget, initial_build=False, allocations=allocations, protected=protected,
        approved=approved, copied_rejections=copied_rejections)
    return result
