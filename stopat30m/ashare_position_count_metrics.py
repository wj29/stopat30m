"""Read-only cost and actual-exposure diagnostics for the fixed A10 ledger.

This module never values a new quote, changes a holding, or repairs an invalid
path. Money strings preserve the original modern ledger's per-order cent rule.
Risk ratios use recorded noncash upper bounds and conservative lower NAV; cash
and cash-dividend receivables are not additional securities.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_HALF_UP


ZERO = Decimal(0)
CENT = Decimal("0.01")
RATE = Decimal("0.0003")
MINIMUM = Decimal("5.00")
TRANSFER = Decimal("0.00001")
STAMP = Decimal("0.0005")
SLIPPAGE = Decimal("0.0002")


def _decimal(value):
    if isinstance(value, bool):
        raise ValueError("Boolean is not a decimal ledger value")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("A finite decimal ledger value is required") from None
    if not number.is_finite():
        raise ValueError("A finite decimal ledger value is required")
    return number


def _money(value):
    return _decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def _session(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError("An explicit ISO session is required")
    return value


def _fraction(value):
    return float(value) if value is not None else None


def _costs(trades, metrics):
    fields = ("notional", "commission", "transfer_fee", "stamp_tax", "total_fee", "slippage_cost")
    totals = {key: ZERO for key in fields}
    sides = {side: {"fill_count": 0, **{key: ZERO for key in fields}} for side in ("BUY", "SELL")}
    proportional = reference_notional = ZERO
    floor_binding = at_minimum = 0
    rows = []
    for index, row in enumerate(trades):
        try:
            session = _session(row["session"])
            side = row["side"]
            quantity = _decimal(row["quantity"])
            reference, fill = _decimal(row["reference_price"]), _decimal(row["execution_price"])
            actual = {key: _decimal(row[key]) for key in fields}
        except (KeyError, ValueError) as exc:
            raise ValueError(f"Invalid fill {index}: {exc}") from exc
        if (session < "2025-01-01" or side not in sides or quantity <= 0
                or quantity != quantity.to_integral_value() or reference <= 0 or fill <= 0
                or reference != _money(reference) or fill != _money(fill)
                or any(value < 0 for value in actual.values())):
            raise ValueError(f"Invalid modern fill {index}")
        if (side == "BUY" and fill < reference) or (side == "SELL" and fill > reference):
            raise ValueError(f"Fill {index} has favorable instead of adverse slippage")
        notional = _money(quantity * fill)
        linear = _money(notional * RATE)
        expected = {"notional": notional, "commission": max(linear, MINIMUM),
                    "transfer_fee": _money(notional * TRANSFER),
                    "stamp_tax": _money(notional * STAMP) if side == "SELL" else _money(0),
                    "slippage_cost": _money(quantity * abs(fill - reference))}
        expected["total_fee"] = expected["commission"] + expected["transfer_fee"] + expected["stamp_tax"]
        if actual != expected:
            bad = [key for key in fields if actual[key] != expected[key]]
            raise ValueError(f"Fill {index} money reconciliation failed: {','.join(bad)}")
        cash_change = -(notional + actual["total_fee"]) if side == "BUY" else notional - actual["total_fee"]
        if "cash_change" in row and _decimal(row["cash_change"]) != cash_change:
            raise ValueError(f"Fill {index} cash_change reconciliation failed")
        if "cash_before" in row and "cash_after" in row:
            if _decimal(row["cash_before"]) + cash_change != _decimal(row["cash_after"]):
                raise ValueError(f"Fill {index} cash balance reconciliation failed")
        nominal = quantity * reference * SLIPPAGE
        proportional += linear
        reference_notional += quantity * reference
        floor_binding += linear < MINIMUM
        at_minimum += actual["commission"] == MINIMUM
        sides[side]["fill_count"] += 1
        for key in fields:
            totals[key] += actual[key]
            sides[side][key] += actual[key]
        rows.append({"fill_index": index, "session": session, "instrument": row.get("instrument"),
                     "side": side, "rounded_proportional_commission_cny": str(linear),
                     "minimum_commission_binding": linear < MINIMUM,
                     "minimum_commission_excess_cny": str(actual["commission"] - linear),
                     "nominal_2bp_slippage_cny": str(nominal),
                     "adverse_tick_and_rounding_residual_cny": str(actual["slippage_cost"] - nominal)})
    for key, value in totals.items():
        if key + "_cny" in metrics and _decimal(metrics[key + "_cny"]) != value:
            raise ValueError(f"Metric/trade reconciliation failed: {key}")
    nominal = reference_notional * SLIPPAGE
    return {**{key + "_cny": str(value) for key, value in totals.items()},
            "trade_friction_cny": str(totals["total_fee"] + totals["slippage_cost"]),
            "fill_count": len(trades), "minimum_commission_binding_fill_count": floor_binding,
            "at_minimum_commission_fill_count": at_minimum,
            "minimum_commission_binding_fraction": floor_binding / len(trades) if trades else None,
            "proportional_commission_cny": str(proportional),
            "minimum_commission_excess_cny": str(totals["commission"] - proportional),
            "reference_notional_cny": str(reference_notional),
            "nominal_2bp_slippage_cny": str(nominal),
            "adverse_tick_and_rounding_residual_cny": str(totals["slippage_cost"] - nominal),
            "slippage_bps_of_reference_notional": float(totals["slippage_cost"] / reference_notional * 10000) if reference_notional else None,
            "by_side": {side: {key + "_cny" if key != "fill_count" else key:
                                      str(value) if isinstance(value, Decimal) else value
                                      for key, value in data.items()} for side, data in sides.items()},
            "per_fill": rows, "money_reconciliation_passed": True,
            "cost_addback_performed": False, "slippage_already_in_execution_prices": True}


def _claim_bounds(row):
    """Return original lower/estimate/upper, never replace an unknown by zero."""
    estimate = _decimal(row["estimated_value"])
    if estimate < 0:
        raise ValueError("Negative claim estimate")
    if row.get("kind") == "cash_dividend":
        return estimate, estimate, estimate
    if "integer_lower_value" in row:
        lower = _decimal(row["integer_lower_value"])
        quantity = _decimal(row["quantity"])
        if quantity < 0:
            raise ValueError("Negative pending bonus quantity")
        if "upper_value_bound" in row:
            upper = _decimal(row["upper_value_bound"])
        elif quantity == quantity.to_integral_value(rounding=ROUND_FLOOR):
            upper = lower
        else:
            # The unchanged engine includes the whole part in lower NAV and
            # at most one extra raw share per uncertain fractional event.
            mark = _decimal(row["last_mark"])
            if mark <= 0:
                raise ValueError("Missing positive pending bonus bound mark")
            upper = lower + _money(mark)
        if quantity > 0 and upper <= 0:
            raise ValueError("Positive pending bonus has no positive bound")
        if quantity == 0 and any(value != 0 for value in (lower, estimate, upper)):
            raise ValueError("Zero pending bonus has a positive value")
    elif "upper_value_bound" in row:
        # Original settled fractional claims contribute no physical share to
        # lower NAV; their explicit whole-claim upper includes attached cash.
        lower, upper = ZERO, _decimal(row["upper_value_bound"])
    else:
        raise ValueError("Noncash claim has no evidenced upper bound")
    if lower < 0 or not lower <= estimate <= upper:
        raise ValueError("Inconsistent claim lower/estimate/upper bounds")
    return lower, estimate, upper


def _exposures(records):
    curves = records.get("curves", [])
    if not curves:
        raise ValueError("Nonempty original daily curves are required")
    days = [_session(row["date"]) for row in curves]
    if days != sorted(set(days)):
        raise ValueError("Daily curves must have unique ordered sessions")
    positions, claims = defaultdict(list), defaultdict(list)
    for source, grouped in ((records.get("positions", []), positions), (records.get("claims", []), claims)):
        for row in source:
            day = _session(row["session"])
            if day not in days:
                raise ValueError("Exposure row is outside the original curves")
            grouped[day].append(row)
    daily = []
    for curve in curves:
        day, gaps = curve["date"], []
        nav, upper_nav, cash = (_decimal(curve[key]) for key in ("equity_cny", "equity_upper_bound_cny", "cash_cny"))
        if nav <= 0 or upper_nav < nav or cash < 0:
            raise ValueError("Invalid original daily NAV/cash bounds")
        for key in ("valuation_complete", "accounting_valid"):
            if curve.get(key) is not True:
                gaps.append(key + "_not_true")
        exposures = {}
        physical_names, noncash_names, seen = set(), set(), set()
        cash_claims, claim_lower, claim_upper = ZERO, ZERO, ZERO
        physical_value = ZERO
        def entry(inst):
            return exposures.setdefault(inst, {"physical_cny": ZERO, "noncash_lower_cny": ZERO,
                                              "noncash_estimate_cny": ZERO, "noncash_upper_cny": ZERO})
        for row in positions[day]:
            inst = row.get("instrument")
            if not isinstance(inst, str) or not inst or inst in seen:
                raise ValueError("Missing or duplicate original daily physical instrument")
            seen.add(inst)
            try:
                quantity = _decimal(row["quantity"])
                if quantity < 0 or quantity != quantity.to_integral_value():
                    raise ValueError("Invalid physical quantity")
                if quantity:
                    # A missing value cannot make a known physical holding
                    # disappear from the account's actual-name count.
                    physical_names.add(inst)
                value = _decimal(row["market_value"])
                if value < 0 or (quantity > 0 and value <= 0):
                    raise ValueError("Invalid physical quantity/value")
                if quantity == 0 and value != 0:
                    raise ValueError("Zero shares with positive physical value")
                if "price" in row and _money(quantity * _decimal(row["price"])) != value:
                    raise ValueError("Physical quantity/price/value mismatch")
                if quantity:
                    physical_value += value
                    entry(inst)["physical_cny"] += value
            except (KeyError, ValueError) as exc:
                gaps.append(f"position:{inst}:{exc}")
        seen_claims = set()
        for index, row in enumerate(claims[day]):
            identity = row.get("event_id", row.get("action_id"))
            if identity is not None and identity in seen_claims:
                raise ValueError("Duplicate original daily claim identity")
            if identity is not None:
                seen_claims.add(identity)
            is_cash = row.get("kind") == "cash_dividend"
            inst = row.get("instrument")
            if not is_cash:
                if not isinstance(inst, str) or not inst:
                    gaps.append(f"claim:{index}:missing_instrument")
                    continue
            try:
                lower, estimate, upper = _claim_bounds(row)
                if is_cash:
                    cash_claims += estimate
                else:
                    if upper > 0 or estimate > 0 or lower > 0:
                        noncash_names.add(inst)
                    claim_lower += lower
                    claim_upper += upper
                    current = entry(inst)
                    for key, value in (("noncash_lower_cny", lower), ("noncash_estimate_cny", estimate), ("noncash_upper_cny", upper)):
                        current[key] += value
            except (KeyError, ValueError) as exc:
                gaps.append(f"claim:{identity if identity is not None else index}:{exc}")
                # Preserve an available estimate only as a diagnostic. It must
                # not stand in for an unknown upper in concentration gates.
                if not is_cash:
                    noncash_names.add(inst)
                    try:
                        estimate = _decimal(row["estimated_value"])
                        if estimate >= 0:
                            entry(inst)["noncash_estimate_cny"] += estimate
                    except (KeyError, ValueError):
                        pass
        lower_reconciles = cash + physical_value + cash_claims + claim_lower == nav
        upper_reconciles = cash + physical_value + cash_claims + claim_upper == upper_nav
        if not lower_reconciles:
            gaps.append("lower_NAV_exposure_reconciliation_failed")
        if not upper_reconciles:
            gaps.append("upper_NAV_exposure_reconciliation_failed")
        weights = []
        estimate_total = lower_total = ZERO
        by_name = []
        for inst, values in sorted(exposures.items()):
            upper = values["physical_cny"] + values["noncash_upper_cny"]
            estimate_total += values["physical_cny"] + values["noncash_estimate_cny"]
            lower_total += values["physical_cny"] + values["noncash_lower_cny"]
            if upper > 0:
                weights.append(upper)
            by_name.append({"instrument": inst, **{key: str(value) for key, value in values.items()},
                            "economic_upper_cny": str(upper), "upper_nav_fraction": float(upper / nav)})
        weights.sort(reverse=True)
        risky = sum(weights, ZERO)
        hhi = sum(((value / risky) ** 2 for value in weights), ZERO) if risky else None
        effective = 1 / hhi if hhi else None
        maximum = weights[0] / nav if weights else ZERO
        top3 = sum(weights[:3], ZERO) / nav
        complete = not gaps
        daily.append({"date": day, "accounting_valid": curve.get("accounting_valid"),
            "valuation_complete": curve.get("valuation_complete"), "coverage_complete": complete,
            "coverage_gaps": gaps, "nav_lower_cny": str(nav), "nav_upper_cny": str(upper_nav),
            "cash_cny": str(cash), "cash_dividend_receivable_cny": str(cash_claims),
            "physical_market_value_cny": str(physical_value), "noncash_claim_lower_cny": str(claim_lower),
            "noncash_claim_upper_cny": str(claim_upper), "cash_nav_fraction": float(cash / nav),
            "physical_security_count": len(physical_names), "noncash_claim_security_count": len(noncash_names),
            "actual_security_count": len(physical_names | noncash_names),
            "risky_nav_fraction": float(risky / nav) if complete else None,
            "known_risky_upper_nav_fraction_diagnostic": float(risky / nav),
            "estimated_risky_nav_fraction_diagnostic": float(estimate_total / nav),
            "risky_lower_nav_fraction_diagnostic": float(lower_total / nav),
            "max_single_nav_fraction": float(maximum) if complete else None,
            "max_top3_nav_fraction": float(top3) if complete else None,
            "risky_hhi": _fraction(hhi) if complete else None,
            "effective_stock_count": _fraction(effective) if complete else None,
            "known_max_single_nav_fraction_diagnostic": float(maximum),
            "known_max_top3_nav_fraction_diagnostic": float(top3),
            "known_effective_stock_count_diagnostic": _fraction(effective),
            "lower_nav_reconciles": lower_reconciles, "upper_nav_reconciles": upper_reconciles,
            "instruments": by_name})
    complete = all(row["coverage_complete"] for row in daily)
    effective = [row["effective_stock_count"] for row in daily if row["effective_stock_count"] is not None]
    return {"coverage_complete": complete, "complete_day_count": sum(row["coverage_complete"] for row in daily),
            "incomplete_day_count": sum(not row["coverage_complete"] for row in daily),
            "session_count": len(daily),
            "max_single_nav_fraction": max(row["max_single_nav_fraction"] for row in daily) if complete else None,
            "max_top3_nav_fraction": max(row["max_top3_nav_fraction"] for row in daily) if complete else None,
            "min_effective_stock_count": min(effective) if complete and effective else None,
            "mean_risky_nav_fraction": sum(row["risky_nav_fraction"] for row in daily) / len(daily) if complete else None,
            "max_actual_security_count": max(row["actual_security_count"] for row in daily),
            "mean_actual_security_count": sum(row["actual_security_count"] for row in daily) / len(daily),
            "actual_security_count_is_hard_cap": False,
            "all_cash_day_count": sum(not row["actual_security_count"] for row in daily)}, daily


def position_count_diagnostics(result):
    """Diagnose one original result without mutating its ledger or validity.

    Incomplete valuations still retain daily known-value diagnostics, but every
    gate-facing aggregate risk ratio is ``None`` unless coverage is complete.
    A zero-risk all-cash day has no effective stock count, rather than infinite
    diversification. An originally invalid path remains invalid independently
    of whether all exposure values can be arithmetically explained.
    """
    records = result["records"]
    for key in ("curves", "positions", "claims", "trades"):
        if key not in records or not isinstance(records[key], list):
            raise ValueError(f"Complete original records list is required: {key}")
    costs = _costs(records["trades"], result.get("metrics", {}))
    risk, daily = _exposures(records)
    return {"kind": "a10_original_ledger_position_count_diagnostics", "schema_version": 1,
            "performance_valid": result.get("performance_valid"), "costs": costs, "risk": risk,
            "daily_exposures": daily,
            "policy": {"commission_rate": str(RATE), "minimum_commission_cny": str(MINIMUM),
                "transfer_rate": str(TRANSFER), "sell_stamp_rate": str(STAMP),
                "nominal_adverse_slippage_bps": "2", "tick_cny": str(CENT),
                "money_rounding": "original per-order ROUND_HALF_UP cents",
                "exposure": "physical plus recorded noncash claim upper by same instrument divided by recorded lower NAV",
                "pending_bonus_upper": "original integer_lower_value plus one last_mark-priced share for a fractional entitlement; exact integer uses its original lower value",
                "HHI": "normalized risky-name upper exposures only; cash and cash-dividend receivables excluded; bounded uncertainty is diagnostic when valuation is incomplete",
                "coverage": "all original complete/accounting-valid close flags and lower/upper NAV reconciliation required; unknown is not zero",
                "slippage_residual": "actual minus nominal2bp, including adverse tick/cent rounding and original limit clipping; may be signed",
                "actual_names": "positive physical names union noncash-claim names; target N is not a new hard cap",
                "cash_drag": "cash fraction is exposure information, not a measured causal return drag",
                "cost_addback_performed": False, "new_valuation_or_counterfactual": False,
                "original_performance_valid_unchanged": True}}
