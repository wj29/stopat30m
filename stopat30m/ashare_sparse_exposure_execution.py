"""A13-only sparse cash control; frozen A12 allocation followed by dust filters.

The overlay never selects securities, reallocates rejected or filtered quantities,
or nets its actual orders against the core.  BUY proposals must fit in full,
including original rounded adverse-price fees, before the single engine batch.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_FLOOR, localcontext
from typing import Mapping, Sequence

from stopat30m.ashare_optimization_execution import RankBufferExecution
from stopat30m.ashare_exposure_matching_execution import (
    DEADBAND, ExposureMatchingUnavailableNAVError, _NAV_ERRORS,
    _active, _risk, _next_quantity, _bridge,
)
from stopat30m.backtest import ashare_execution as ex

POLICY = "a13-lagged-sparse-exposure-proportional-min-order-cash-control-v1"


def execute_sparse_exposure_overlay(state: ex.PortfolioState, *, session: str, signal_date: str,
        quotes: Mapping[str, ex.OpeningQuote], metadata: Mapping[str, ex.SecurityInfo],
        calendar: Sequence[str], policy: ex.ExecutionPolicy, target_fraction,
        minimum_order_notional_cny, now=None,
        verified_suspended: Sequence[str] = ()) -> RankBufferExecution:
    """One optional, same-direction original-engine call after the core batch.

    The caller skips its first session.  Later unknown targets must be ``None``;
    booleans/nonfinite/out-of-range values are errors and are never clipped.
    Physical market-value proportions determine ideal trade quantities.  Legal
    floors are followed by at most one additional legal lot per name, ordered
    by ideal-minus-approved share remainder and instrument code.  An addition
    must reduce total reference-notional target error.  No failed-fill rerun,
    substitute security, sleeve, cash transfer, or cost addback is possible.
    """
    minimum = ex.decimal(minimum_order_notional_cny)
    if minimum < 0 or ex.money(minimum) != minimum:
        raise ValueError("Minimum order notional must be a nonnegative exact CNY cent amount")
    days = ex._calendar(calendar, session)
    if days.index(session) == 0 or signal_date != days[days.index(session)-1]:
        raise ValueError("Exposure control requires the preceding actual exchange signal session")
    target = None if target_fraction is None else ex.decimal(target_fraction)
    if target is not None and not 0 <= target <= 1:
        raise ValueError("Known target exposure must lie in [0,1]; clipping is forbidden")
    original = ex._advance(state, session, days)
    active = _active(original)
    try:
        probe = ex.execute_target_weights(original, session=session, signal_date=signal_date,
            targets={inst: 0 for inst in active}, quotes=quotes, metadata=metadata, calendar=days,
            policy=policy, now=now, verified_suspended=verified_suspended)
    except ValueError as exc:
        if str(exc) in _NAV_ERRORS:
            raise ExposureMatchingUnavailableNAVError(str(exc)) from exc
        raise
    if probe.opening_equity <= 0:
        raise ExposureMatchingUnavailableNAVError("A positive original opening NAV is required for exposure control")
    audit = {"policy_version": POLICY, "session": session, "signal_date": signal_date,
        "target_fraction": None if target is None else str(target), "deadband_fraction": str(DEADBAND),
        "opening_nav_lower_cny": str(probe.opening_equity), "current_open_fraction_upper": None,
        "current_open_fraction_lower": None, "post_open_fraction_upper": None, "post_open_fraction_lower": None,
        "coverage_complete": False, "risk_coverage_complete": False, "execution_rules_complete": None,
        "coverage_gaps": [], "skip_reason": None, "direction": None,
        "allocations": [], "actual_buy_notional_cny": "0", "actual_sell_notional_cny": "0",
        "total_fee_cny": "0", "slippage_cost_cny": "0", "trade_friction_cny": "0",
        "core_holding_minimum_and_exit_cap_exemption": "explicit_cash_control_only",
        "one_additional_legal_lot_per_identity_maximum": True, "no_post_refusal_reallocation": True,
        "actual_engine_calls": 0, "pure_opening_nav_probes": 1,
        "active_identities_before": sorted(active), "new_identity_added": False,
        "cash_and_share_bridge_verified": True,
        "minimum_order_notional_cny": str(minimum),
        "minimum_order_filter_basis": "expected_original_adverse_execution_notional_inclusive",
        "minimum_filter_after_frozen_allocation": True,
        "filtered_dust_never_redistributed": True,
        "buy_funding_policy": "whole_order_full_adverse_notional_plus_rounded_fees_reservation",
        "reserved_buy_notional_cny": "0", "reserved_buy_fee_cny": "0",
        "reserved_buy_total_cny": "0", "orders_planned": 0,
        "actual_fills_minimum_verified": True}

    def skipped(reason):
        audit["skip_reason"] = reason
        return RankBufferExecution(ex.ExecutionResult(original, [], [], probe.opening_equity), {"exposure_matching": audit})

    risk_names = set(active) | {row["instrument"] for row in original.claims.values()}
    references = {}
    for inst in sorted(risk_names):
        try:
            references[inst] = ex._reference(quotes[inst], session, policy, now=now)
        except (KeyError, ValueError) as exc:
            audit["coverage_gaps"].append({"instrument": inst, "reason": str(exc)})
    if audit["coverage_gaps"]:
        return skipped("unknown_current_security_reference")
    try:
        current = _risk(original, references)
    except (KeyError, ValueError) as exc:
        audit["coverage_gaps"].append({"reason": str(exc)})
        return skipped("undefined_noncash_risk_bounds")
    # The original engine checks quote+metadata identity *before* accepting a
    # new mark.  With missing/wrong identity its permitted probe NAV can thus
    # retain an old observed mark despite a separately valid opening quote.
    # Establish this precise rules gap before comparing the two NAV bases;
    # never replace the original probe with an uncertified gap-price NAV.
    rules = {}
    for inst, position in sorted(original.positions.items()):
        if not position.quantity:
            continue
        try:
            info = metadata[inst]
            if info.instrument != inst:
                raise ValueError("Instrument metadata identity mismatch")
            rules[inst] = ex.price_limits(info, quotes[inst], session=session, calendar=days, now=now)
        except (KeyError, ValueError) as exc:
            audit["coverage_gaps"].append({"instrument": inst, "reason": str(exc)})
    audit["execution_rules_complete"] = not audit["coverage_gaps"]
    if current["nav_lower_cny"] != probe.opening_equity:
        if audit["coverage_gaps"]:
            return skipped("unavailable_original_security_rules")
        raise ValueError("Known opening NAV components disagree with the original engine probe")
    nav = probe.opening_equity
    current_fraction = current["risk_upper_cny"] / nav
    audit.update(coverage_complete=True, risk_coverage_complete=True,
        current_open_fraction_upper=str(current_fraction),
        current_open_fraction_lower=str(current["risk_lower_cny"] / nav),
        post_open_fraction_upper=str(current_fraction),
        post_open_fraction_lower=str(current["risk_lower_cny"] / nav),
        current_risk_upper_cny=str(current["risk_upper_cny"]),
        current_risk_lower_cny=str(current["risk_lower_cny"]),
        current_physical_cny=str(current["physical_cny"]),
        current_noncash_upper_cny=str(current["noncash_upper_cny"]),
        current_noncash_lower_cny=str(current["noncash_lower_cny"]),
        cash_dividend_receivable_excluded_from_risk_cny=str(current["cash_receivable_cny"]))
    if audit["coverage_gaps"]:
        audit["coverage_complete"] = False
        return skipped("unavailable_original_security_rules")
    if target is None:
        return skipped("unknown_reference_target")
    audit["unmatched_fraction"] = str(current_fraction - target)
    audit["unmatched_risk_cny"] = str(current["risk_upper_cny"] - target * nav)
    if abs(current_fraction - target) <= DEADBAND:
        return skipped("within_fixed_deadband")
    direction = "SELL" if current_fraction > target else "BUY"
    needed = abs(current["risk_upper_cny"] - target * nav)
    audit.update(direction=direction, required_reference_notional_cny=str(needed))
    if not current["physical_cny"]:
        return skipped("no_physical_value_for_proportional_allocation")
    approved, allocations = {}, {}
    for inst, position in sorted(original.positions.items()):
        if not position.quantity:
            continue
        available = position.quantity - (position.bought_today if position.bought_date == session else 0)
        ideal = needed * position.quantity / current["physical_cny"]
        integer = int(ideal.to_integral_value(rounding=ROUND_FLOOR))
        limits = rules[inst]
        quantity = ex._sell_quantity(integer, available, position.quantity, limits) if direction == "SELL" else ex._buy_quantity(integer, limits)
        approved[inst] = quantity
        allocations[inst] = {"instrument": inst, "full_held_quantity": position.quantity,
            "available_quantity": available, "reference_price": str(references[inst]),
            "physical_value_weight": str(position.quantity * references[inst] / current["physical_cny"]),
            "ideal_trade_quantity": str(ideal), "floor_approved_quantity": quantity,
            "share_remainder": str(ideal-quantity), "additional_lot_quantity": 0,
            "remainder_addition_reason": "not_selected", "bought_today_locked_quantity": position.quantity-available}
    allocated = sum((approved[i] * references[i] for i in approved), ex.ZERO)
    # Fixed one-pass Hamilton-style correction.  The order and candidates are
    # frozen before executing any order; rejection cannot release a slot.
    remainder_order = sorted(approved, key=lambda i: (-ex.decimal(allocations[i]["share_remainder"]), i))
    for inst in remainder_order:
        row, position = allocations[inst], original.positions[inst]
        next_quantity = _next_quantity(direction, approved[inst], available=row["available_quantity"],
            held=position.quantity, limits=rules[inst])
        if next_quantity is None:
            row["remainder_addition_reason"] = "no_further_original_board_valid_quantity"
            continue
        addition = (next_quantity-approved[inst])*references[inst]
        if abs(needed-(allocated+addition)) >= abs(needed-allocated):
            row["remainder_addition_reason"] = "does_not_strictly_reduce_total_reference_target_error"
            continue
        if direction == "BUY" and current["physical_cny"] + allocated + addition + current["noncash_lower_cny"] > nav:
            row["remainder_addition_reason"] = "would_exceed_original_long_only_target_weight_budget"
            continue
        row["additional_lot_quantity"] = next_quantity-approved[inst]
        row["remainder_addition_reason"] = "strictly_reduces_total_reference_target_error"
        approved[inst] = next_quantity
        allocated += addition
    # Freeze the complete A12 allocation before filtering.  A filtered small
    # order is not redistributed to another identity or rounded up to the floor.
    prefilter_reference_notional = allocated
    reserved_notional, reserved_fees = ex.ZERO, ex.ZERO
    remaining_cash = original.cash
    for inst in sorted(approved):
        row = allocations[inst]
        prefilter_quantity = approved[inst]
        row.update(pre_filter_allocated_trade_quantity=prefilter_quantity,
            pre_threshold_trade_quantity=prefilter_quantity,
            st_buy_cap_approved_quantity=prefilter_quantity,
            expected_adverse_execution_price=None, expected_adverse_notional_cny="0",
            expected_rounded_total_fee_cny="0", expected_total_cash_required_cny="0",
            reserved_buy_notional_cny="0", reserved_buy_fee_cny="0",
            reserved_buy_total_cny="0", filter_reason=None)
        if not prefilter_quantity:
            row["filter_reason"] = "no_frozen_allocation_quantity"
            continue
        quantity = prefilter_quantity
        if direction == "BUY":
            info, position = metadata[inst], original.positions[inst]
            if info.is_st and info.board in {"sse_main", "szse_main", "chinext"}:
                already = position.bought_today if position.bought_date == session else 0
                quantity = ex._buy_quantity(min(quantity, max(0, 500000-already)), rules[inst])
                row["st_buy_cap_approved_quantity"] = quantity
            if not quantity:
                row["filter_reason"] = "original_ST_daily_buy_cap_or_board_lot_exhausted"
                approved[inst] = 0
                continue
        row["pre_threshold_trade_quantity"] = quantity
        try:
            fill = ex._fill_price(references[inst], direction, rules[inst], policy)
        except ValueError as exc:
            # No certified execution price means no certified minimum amount.
            # This is a precheck drop, not an actual engine rejection or fill.
            approved[inst] = 0
            row["filter_reason"] = "unavailable_adverse_execution_price"
            row["original_execution_rejection_reason"] = str(exc)
            continue
        fee = ex.fees_for_order(quantity, fill, direction, policy)
        cost = fee["notional"] + fee["total_fee"]
        row.update(expected_adverse_execution_price=str(fill),
            expected_adverse_notional_cny=str(fee["notional"]),
            expected_rounded_total_fee_cny=str(fee["total_fee"]),
            expected_total_cash_required_cny=str(cost))
        if fee["notional"] < minimum:
            approved[inst] = 0
            row["filter_reason"] = "below_inclusive_minimum_order_notional"
            continue
        approved[inst] = quantity
        row["filter_reason"] = "approved_full_proposed_quantity"
    # This is precisely the original engine's order against the final proposed
    # integer deficits, including any original ST quantity cap.  Filtering does
    # not reassign a security's allocation, and a refused funded order never
    # releases its reservation to a later proposal.
    planned_order = sorted(approved, key=lambda i: (-approved[i] * references[i], i)) if direction == "BUY" else sorted(approved)
    for inst in planned_order:
        if not approved[inst] or direction != "BUY":
            continue
        row = allocations[inst]
        notional = ex.decimal(row["expected_adverse_notional_cny"])
        fee = ex.decimal(row["expected_rounded_total_fee_cny"])
        cost = notional + fee
        if cost > remaining_cash:
            approved[inst] = 0
            row["filter_reason"] = "insufficient_cash_for_whole_proposed_order_plus_rounded_fees"
            continue
        remaining_cash -= cost
        reserved_notional += notional
        reserved_fees += fee
        row.update(reserved_buy_notional_cny=str(notional),
            reserved_buy_fee_cny=str(fee), reserved_buy_total_cny=str(cost))
    allocated = sum((approved[i] * references[i] for i in approved), ex.ZERO)
    audit.update(pre_filter_reference_notional_cny=str(prefilter_reference_notional),
        minimum_filter_priority=planned_order,
        reserved_buy_notional_cny=str(reserved_notional), reserved_buy_fee_cny=str(reserved_fees),
        reserved_buy_total_cny=str(reserved_notional+reserved_fees),
        unreserved_cash_cny=str(remaining_cash),
        no_reserved_cash_release_after_refusal=True,
        orders_planned=sum(quantity > 0 for quantity in approved.values()))
    desired = {inst: pos.quantity + (approved.get(inst, 0) if direction == "BUY" else -approved.get(inst, 0))
               for inst, pos in original.positions.items() if pos.quantity}
    weights = {}
    with localcontext() as context:
        context.prec = 80
        context.rounding = ROUND_FLOOR
        for inst in sorted(active):
            weights[inst] = (desired.get(inst, 0) + current["pending_whole"].get(inst, 0))*references[inst]/nav
    reduction = ex.ZERO
    if sum(weights.values(), ex.ZERO) > 1:
        inst = max(weights, key=lambda i: (weights[i], i))
        reduction = sum(weights.values(), ex.ZERO)-1+Decimal(len(weights)+1)*Decimal("1e-27")
        weights[inst] = max(ex.ZERO, weights[inst]-reduction)
    if sum(weights.values(), ex.ZERO) > 1:
        raise ValueError("Exposure-control desired holdings exceed the original long-only weight budget")
    # Verify the exact integer cell using the original engine's arithmetic
    # before submitting its one real call.  In particular an unchanged odd-lot
    # incumbent in a BUY batch cannot round down into an unintended SELL1.
    encoded = {inst: max(0, int((nav * weight / references[inst]).to_integral_value(rounding=ROUND_FLOOR))
        - current["pending_whole"].get(inst, 0)) for inst, weight in weights.items()}
    if encoded != {inst: desired.get(inst, 0) for inst in active}:
        raise ValueError("Original engine precision cannot encode the exact cash-control integer targets")
    result = ex.execute_target_weights(original, session=session, signal_date=signal_date,
        targets=weights, quotes=quotes, metadata=metadata, calendar=days, policy=policy, now=now,
        verified_suspended=verified_suspended)
    _bridge(original, result, direction, approved)
    for trade in result.trades:
        inst = trade["instrument"]
        if ex.decimal(trade["notional"]) < minimum:
            raise ValueError("Actual sparse cash-control fill is below the frozen minimum order notional")
        if trade["quantity"] != approved[inst]:
            raise ValueError("Sparse cash-control engine unexpectedly resized a fully reserved proposed order")
        if direction == "BUY":
            reserved = allocations[inst]
            if (ex.decimal(trade["notional"]) != ex.decimal(reserved["reserved_buy_notional_cny"])
                    or ex.decimal(trade["total_fee"]) != ex.decimal(reserved["reserved_buy_fee_cny"])):
                raise ValueError("Actual sparse BUY fill disagrees with frozen rounded cash reservation")
    if _active(result.state)-active:
        raise ValueError("Cash control may not introduce a new active security identity")
    after = _risk(result.state, references)
    if after["nav_lower_cny"] <= 0:
        raise ExposureMatchingUnavailableNAVError("Cash-control post-opening NAV is nonpositive")
    fees = sum((ex.decimal(row["total_fee"]) for row in result.trades), ex.ZERO)
    slip = sum((ex.decimal(row["slippage_cost"]) for row in result.trades), ex.ZERO)
    for inst, row in allocations.items():
        row["approved_trade_quantity"] = approved[inst]
        row["desired_physical_quantity"] = desired[inst]
        row["actual_filled_quantity"] = sum(t["quantity"] for t in result.trades if t["instrument"] == inst)
    audit.update(allocations=list(allocations.values()), remainder_priority=remainder_order,
        approved_target_quantities=desired, approved_trade_quantities=approved,
        submitted_target_weights={i: str(w) for i, w in weights.items()}, weight_rounding_reduction=str(reduction),
        engine_integer_targets_exact_verified=True,
        approved_reference_notional_cny=str(allocated),
        actual_buy_notional_cny=str(sum((ex.decimal(t["notional"]) for t in result.trades if t["side"] == "BUY"), ex.ZERO)),
        actual_sell_notional_cny=str(sum((ex.decimal(t["notional"]) for t in result.trades if t["side"] == "SELL"), ex.ZERO)),
        total_fee_cny=str(fees), slippage_cost_cny=str(slip), trade_friction_cny=str(fees+slip),
        post_open_nav_lower_cny=str(after["nav_lower_cny"]),
        post_open_fraction_upper=str(after["risk_upper_cny"]/after["nav_lower_cny"]),
        post_open_fraction_lower=str(after["risk_lower_cny"]/after["nav_lower_cny"]),
        post_open_risk_upper_cny=str(after["risk_upper_cny"]), post_open_risk_lower_cny=str(after["risk_lower_cny"]),
        unmatched_fraction=str(after["risk_upper_cny"]/after["nav_lower_cny"]-target),
        unmatched_risk_cny=str(after["risk_upper_cny"]-target*after["nav_lower_cny"]),
        actual_engine_calls=1)
    return RankBufferExecution(result, {"exposure_matching": audit})
