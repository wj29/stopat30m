"""A12-only, single-pass cash control within already owned security identities.

Prices are current observed execution references, never carried fills.  The
caller supplies an explicitly lagged target; this module cannot load reference
accounts, rank predictions, future prices or a calendar-wide exposure average.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_FLOOR, localcontext
from typing import Mapping, Sequence

from stopat30m.ashare_optimization_execution import RankBufferExecution
from stopat30m.backtest import ashare_execution as ex


POLICY = "a12-lagged-exposure-proportional-one-remainder-lot-cash-control-v1"
DEADBAND = Decimal("0.01")


class ExposureMatchingUnavailableNAVError(ValueError):
    """The original engine cannot establish its permitted opening NAV."""


_NAV_ERRORS = (
    "Unpriced held shares have no preceding observed market mark",
    "Unknown held opening-price gap exceeds declared session limit",
    "Unpriced pending stock entitlement prevents opening NAV budgeting",
)


def _active(state):
    names = {inst for inst, pos in state.positions.items() if pos.quantity}
    names.update(row["instrument"] for row in state.entitlements.values()
        if row.get("status") == "pending" and row.get("kind") == "bonus_shares"
        and ex.decimal(row["quantity"]) > 0)
    return names


def _risk(state, references):
    """Known upper securities risk and original lower NAV; no cash receivable risk."""
    physical = sum((pos.quantity * references[inst] for inst, pos in state.positions.items()
                    if pos.quantity), ex.ZERO)
    lower, upper, cash_receivables = ex.ZERO, ex.ZERO, ex.ZERO
    pending_whole = {}
    for row in state.entitlements.values():
        if row.get("status") != "pending":
            continue
        if row.get("kind") == "cash_dividend":
            net = ex.decimal(row["net_cash"])
            if net < 0 or ex.money(net) != net:
                raise ValueError("Pending cash receivable has no valid cent amount")
            cash_receivables += net
        elif row.get("kind") == "bonus_shares":
            quantity = ex.decimal(row["quantity"])
            if quantity < 0:
                raise ValueError("Negative pending stock entitlement")
            if not quantity:
                continue
            inst = row["instrument"]
            whole = int(quantity.to_integral_value(rounding=ROUND_FLOOR))
            value = whole * references[inst]
            lower += value
            upper += value + (references[inst] if quantity != whole else 0)
            pending_whole[inst] = pending_whole.get(inst, 0) + whole
        else:
            raise ValueError("Unspecified noncash pending entitlement has no certified risk bounds")
    for row in state.claims.values():
        if (row.get("allocation") != "unresolved_CSDC_fractional_allocation"
                or row.get("spendable") is not False
                or "maximum_additional_integer_shares" not in row):
            raise ValueError("Noncash claim has no explicit original allocation/bound contract")
        maximum, quantity = ex.decimal(row["maximum_additional_integer_shares"]), ex.decimal(row["quantity"])
        if maximum < 0 or maximum != int(maximum) or not 0 <= quantity <= maximum:
            raise ValueError("Invalid noncash share-allocation upper bound")
        if "cash_estimate" in row and "cash_upper_bound" not in row:
            raise ValueError("Attached noncash-claim cash has no explicit upper bound")
        cash_upper = ex.decimal(row.get("cash_upper_bound", "0"))
        if cash_upper < 0:
            raise ValueError("Negative attached noncash cash upper bound")
        upper += maximum * references[row["instrument"]] + cash_upper
    nav = ex.money(state.cash + physical + cash_receivables + lower)
    return {"physical_cny": physical, "noncash_lower_cny": lower, "noncash_upper_cny": upper,
        "cash_receivable_cny": cash_receivables, "risk_lower_cny": physical + lower,
        "risk_upper_cny": physical + upper, "nav_lower_cny": nav, "pending_whole": pending_whole}


def _next_quantity(side, current, *, available, held, limits):
    """Smallest next board-valid quantity against the original full position.

    The original monotone quantity evaluator is searched, rather than assuming
    an odd remainder belongs to a small artificial execution sleeve.
    """
    evaluate = (lambda q: ex._sell_quantity(q, available, held, limits)) if side == "SELL" else (
        lambda q: ex._buy_quantity(q, limits))
    maximum = min(available, limits["max_quantity"]) if side == "SELL" else limits["max_quantity"]
    if current >= evaluate(maximum):
        return None
    lo, hi = current + 1, maximum
    while lo < hi:
        middle = (lo + hi) // 2
        if evaluate(middle) > current:
            hi = middle
        else:
            lo = middle + 1
    return evaluate(lo)


def _bridge(original, result, direction, approved):
    expected = {inst: pos.quantity for inst, pos in original.positions.items() if pos.quantity}
    for row in result.trades:
        inst, quantity = row["instrument"], row["quantity"]
        if row["side"] != direction or inst not in approved or quantity > approved[inst]:
            raise ValueError("Cash control filled an unapproved identity, side or quantity")
        expected[inst] = expected.get(inst, 0) + (quantity if direction == "BUY" else -quantity)
    expected = {inst: quantity for inst, quantity in expected.items() if quantity}
    actual = {inst: pos.quantity for inst, pos in result.state.positions.items() if pos.quantity}
    if expected != actual or any(quantity < 0 for quantity in expected.values()):
        raise ValueError("Cash-control shares do not reconcile to original holdings and actual fills")
    cash = original.cash + sum((ex.decimal(row["cash_change"]) for row in result.trades), ex.ZERO)
    if result.state.cash != cash or cash < 0:
        raise ValueError("Cash-control cash does not reconcile to original rounded engine fills")
    for field in ("claims", "entitlements", "applied_actions", "holdings_history"):
        if getattr(original, field) != getattr(result.state, field):
            raise ValueError("Cash control cannot rewrite original corporate actions or record-date rights")


def execute_exposure_overlay(state: ex.PortfolioState, *, session: str, signal_date: str,
        quotes: Mapping[str, ex.OpeningQuote], metadata: Mapping[str, ex.SecurityInfo],
        calendar: Sequence[str], policy: ex.ExecutionPolicy, target_fraction,
        now=None, verified_suspended: Sequence[str] = ()) -> RankBufferExecution:
    """One optional, same-direction original-engine call after the core batch.

    The caller skips its first session.  Later unknown targets must be ``None``;
    booleans/nonfinite/out-of-range values are errors and are never clipped.
    Physical market-value proportions determine ideal trade quantities.  Legal
    floors are followed by at most one additional legal lot per name, ordered
    by ideal-minus-approved share remainder and instrument code.  An addition
    must reduce total reference-notional target error.  No failed-fill rerun,
    substitute security, sleeve, cash transfer, or cost addback is possible.
    """
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
        "cash_and_share_bridge_verified": True}

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
