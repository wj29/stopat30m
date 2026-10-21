"""Hand-computable diagnostics; no model, real candidate or ledger reruns."""
from copy import deepcopy
from decimal import Decimal

import pytest

from stopat30m.ashare_position_count_metrics import position_count_diagnostics


def curve(day="2025-01-02", *, nav="100000", upper=None, cash="100000", complete=True, valid=True):
    return {"date": day, "equity_cny": nav, "equity_upper_bound_cny": upper or nav,
            "cash_cny": cash, "valuation_complete": complete, "accounting_valid": valid}


def result(curves=None, positions=(), claims=(), trades=(), *, valid=True):
    return {"performance_valid": valid, "records": {"curves": curves or [curve()],
            "positions": list(positions), "claims": list(claims), "trades": list(trades)},
            "metrics": {}, "final_state": {"untouched": True}}


def position(inst, value, *, day="2025-01-02", quantity=100):
    return {"session": day, "instrument": inst, "quantity": quantity, "market_value": str(value)}


def fill(side="BUY", *, quantity=100, reference="10.00", price="10.01"):
    money = lambda n: Decimal(str(n)).quantize(Decimal(".01"), rounding="ROUND_HALF_UP")
    notional = money(Decimal(quantity) * Decimal(price))
    commission = max(Decimal(5), money(notional * Decimal(".0003")))
    transfer = money(notional * Decimal(".00001"))
    stamp = money(notional * Decimal(".0005")) if side == "SELL" else Decimal(0)
    fee = commission + transfer + stamp
    change = -(notional + fee) if side == "BUY" else notional - fee
    return {"session": "2025-01-02", "instrument": "SH600000", "side": side,
            "quantity": quantity, "reference_price": reference, "execution_price": price,
            "notional": str(notional), "commission": str(commission), "transfer_fee": str(transfer),
            "stamp_tax": str(stamp), "total_fee": str(fee),
            "slippage_cost": str(money(quantity * abs(Decimal(price) - Decimal(reference)))),
            "cash_before": "100000.00", "cash_change": str(change), "cash_after": str(Decimal(100000) + change)}


def test_one_fill_cent_cost_and_tick_residual_without_nav_addback():
    source = result(trades=[fill()])
    before = deepcopy(source)
    report = position_count_diagnostics(source)
    costs = report["costs"]
    assert Decimal(costs["notional_cny"]) == Decimal("1001")
    assert Decimal(costs["total_fee_cny"]) == Decimal("5.01")
    assert Decimal(costs["slippage_cost_cny"]) == 1
    assert Decimal(costs["trade_friction_cny"]) == Decimal("6.01")
    assert Decimal(costs["minimum_commission_excess_cny"]) == Decimal("4.70")
    assert Decimal(costs["nominal_2bp_slippage_cny"]) == Decimal(".2")
    assert Decimal(costs["adverse_tick_and_rounding_residual_cny"]) == Decimal(".8")
    assert costs["cost_addback_performed"] is False
    assert source == before


def test_equal_five_commission_is_not_always_strictly_binding():
    #16660*.0003 rounds to5.00; it is at minimum but not raised by the floor.
    report = position_count_diagnostics(result(trades=[fill(quantity=1000, reference="16.65", price="16.66")]))
    assert report["costs"]["at_minimum_commission_fill_count"] == 1
    assert report["costs"]["minimum_commission_binding_fill_count"] == 0
    assert Decimal(report["costs"]["minimum_commission_excess_cny"]) == 0


def test_sell_stamp_and_both_sides_use_actual_per_order_rounding():
    report = position_count_diagnostics(result(trades=[fill(), fill("SELL", reference="10", price="9.99")]))
    cost = report["costs"]
    assert Decimal(cost["stamp_tax_cny"]) == Decimal(".50")
    assert cost["by_side"]["BUY"]["fill_count"] == cost["by_side"]["SELL"]["fill_count"] == 1
    assert Decimal(cost["total_fee_cny"]) == Decimal("10.52")
    assert Decimal(cost["slippage_cost_cny"]) == 2
    assert Decimal(cost["trade_friction_cny"]) == Decimal("12.52")


@pytest.mark.parametrize("field,value", [("commission", "4.99"), ("notional", "1000"),
    ("slippage_cost", "0.20"), ("total_fee", "6.01"), ("cash_change", "-1007.01"),
    ("cash_after", "0"), ("transfer_fee", "0"), ("stamp_tax", ".50")])
def test_changed_recorded_fill_money_fails_reconciliation(field, value):
    trade = fill()
    trade[field] = value
    with pytest.raises(ValueError, match="reconciliation"):
        position_count_diagnostics(result(trades=[trade]))


def test_summary_cost_mismatch_is_rejected():
    source = result(trades=[fill()])
    source["metrics"]["commission_cny"] = "6"
    with pytest.raises(ValueError, match="Metric/trade"):
        position_count_diagnostics(source)


def test_all_cash_is_zero_exposure_not_an_extra_diversifying_security():
    report = position_count_diagnostics(result())
    risk = report["risk"]
    assert risk["coverage_complete"] is True
    assert risk["max_single_nav_fraction"] == risk["mean_risky_nav_fraction"] == 0
    assert risk["min_effective_stock_count"] is None
    assert risk["max_actual_security_count"] == 0
    assert report["costs"]["minimum_commission_binding_fraction"] is None
    assert risk["actual_security_count_is_hard_cap"] is False


def test_four_equal_names_and_cash_have_effective_four_not_five():
    positions = [position(f"SH60000{i}", 20000) for i in range(4)]
    report = position_count_diagnostics(result([curve(cash="20000")], positions))
    risk = report["risk"]
    assert risk["coverage_complete"] is True
    assert risk["max_single_nav_fraction"] == pytest.approx(.2)
    assert risk["max_top3_nav_fraction"] == pytest.approx(.6)
    assert risk["min_effective_stock_count"] == pytest.approx(4)
    assert risk["mean_risky_nav_fraction"] == pytest.approx(.8)
    assert risk["max_actual_security_count"] == 4


def test_cash_receivable_is_excluded_and_same_instrument_bonus_merged():
    claims = [{"session": "2025-01-02", "event_id": "cash", "kind": "cash_dividend",
               "instrument": "SH600099", "estimated_value": "1000"},
              {"session": "2025-01-02", "event_id": "bonus", "kind": "bonus_shares",
               "instrument": "SH600001", "quantity": "100", "integer_lower_value": "10000",
               "estimated_value": "10000"}]
    report = position_count_diagnostics(result([curve(cash="49000")],
        [position("SH600001", 20000), position("SH600002", 20000)], claims))
    risk = report["risk"]
    assert risk["coverage_complete"] is True
    assert risk["max_actual_security_count"] == 2
    assert risk["max_single_nav_fraction"] == pytest.approx(.3)
    assert risk["mean_risky_nav_fraction"] == pytest.approx(.5)
    assert risk["min_effective_stock_count"] == pytest.approx(1 / (.6**2 + .4**2))
    daily = report["daily_exposures"][0]
    assert [row["instrument"] for row in daily["instruments"]] == ["SH600001", "SH600002"]
    assert daily["cash_dividend_receivable_cny"] == "1000"


def test_explicit_fractional_claim_upper_merges_but_incomplete_stays_diagnostic():
    claims = [{"session": "2025-01-02", "action_id": "exchange", "instrument": "SH600001",
               "quantity": ".5", "estimated_value": "50", "upper_value_bound": "100"}]
    source = result([curve(nav="100000", upper="100100", cash="80000", complete=False)],
                    [position("SH600001", 20000)], claims, valid=False)
    report = position_count_diagnostics(source)
    assert report["performance_valid"] is False
    assert report["risk"]["coverage_complete"] is False
    assert report["risk"]["max_single_nav_fraction"] is None
    daily = report["daily_exposures"][0]
    assert daily["actual_security_count"] == 1
    assert daily["known_max_single_nav_fraction_diagnostic"] == pytest.approx(.201)
    assert daily["upper_nav_reconciles"] is True


def test_fractional_pending_bonus_uses_original_one_share_upper_bound():
    claims = [{"session": "2025-01-02", "event_id": "bonus", "kind": "bonus_shares",
               "instrument": "SH600001", "quantity": "10.5", "integer_lower_value": "1000",
               "estimated_value": "1050", "last_mark": "100"}]
    report = position_count_diagnostics(result([curve(nav="100000", upper="100100", cash="99000", complete=False)], claims=claims))
    day = report["daily_exposures"][0]
    assert day["noncash_claim_upper_cny"] == "1100.00"
    assert day["known_max_single_nav_fraction_diagnostic"] == pytest.approx(.011)
    assert day["lower_nav_reconciles"] is day["upper_nav_reconciles"] is True
    assert report["risk"]["coverage_complete"] is False


def test_unknown_claim_estimate_is_not_substituted_as_a_zero_or_known_bound():
    claims = [{"session": "2025-01-02", "instrument": "SH600001", "estimated_value": "10000"}]
    report = position_count_diagnostics(result(claims=claims))
    assert report["risk"]["coverage_complete"] is False
    assert report["risk"]["mean_risky_nav_fraction"] is None
    assert report["risk"]["min_effective_stock_count"] is None
    day = report["daily_exposures"][0]
    assert day["actual_security_count"] == 1
    assert day["estimated_risky_nav_fraction_diagnostic"] == pytest.approx(.1)
    assert any("no evidenced upper" in gap for gap in day["coverage_gaps"])


def test_unexplained_nav_value_prevents_complete_risk_coverage():
    report = position_count_diagnostics(result([curve(cash="90000")]))
    assert report["risk"]["coverage_complete"] is False
    assert report["risk"]["max_single_nav_fraction"] is None
    assert "lower_NAV_exposure_reconciliation_failed" in report["daily_exposures"][0]["coverage_gaps"]


def test_unknown_physical_value_and_false_valuation_cannot_look_safe():
    source = result([curve(cash="90000", valid=False)], [position("SH600001", "NaN")], valid=False)
    report = position_count_diagnostics(source)
    assert report["performance_valid"] is False
    assert report["risk"]["coverage_complete"] is False
    assert report["risk"]["max_top3_nav_fraction"] is None
    assert report["risk"]["max_actual_security_count"] == 1


def test_cash_days_are_in_mean_exposure_but_not_in_min_effective_count():
    days = [curve(), curve("2025-01-03", cash="0")]
    names = [position(f"SH60000{i}", 25000, day="2025-01-03") for i in range(4)]
    report = position_count_diagnostics(result(days, names))
    assert report["risk"]["coverage_complete"] is True
    assert report["risk"]["mean_risky_nav_fraction"] == pytest.approx(.5)
    assert report["risk"]["min_effective_stock_count"] == pytest.approx(4)
    assert report["risk"]["mean_actual_security_count"] == pytest.approx(2)


def test_duplicate_or_outside_daily_rows_are_not_silently_counted():
    row = position("SH600001", 10000)
    with pytest.raises(ValueError, match="duplicate"):
        position_count_diagnostics(result(positions=[row, row]))
    with pytest.raises(ValueError, match="outside"):
        position_count_diagnostics(result(positions=[position("SH600001", 10000, day="2025-01-03")]))


def test_original_invalid_path_is_never_promoted_even_with_complete_arithmetic():
    source = result(valid=False)
    report = position_count_diagnostics(source)
    assert report["risk"]["coverage_complete"] is True
    assert report["performance_valid"] is False
    assert source["performance_valid"] is False


def test_zero_noncash_claim_does_not_create_a_diversifying_name():
    claims = [{"session": "2025-01-02", "event_id": "zero", "kind": "bonus_shares",
               "instrument": "SH600001", "quantity": "0", "estimated_value": "0",
               "integer_lower_value": "0"}]
    report = position_count_diagnostics(result(claims=claims))
    assert report["risk"]["coverage_complete"] is True
    assert report["risk"]["max_actual_security_count"] == 0
    assert report["risk"]["min_effective_stock_count"] is None


def test_positive_bonus_quantity_cannot_be_treated_as_an_evidenced_zero_value():
    claims = [{"session": "2025-01-02", "event_id": "unpriced", "kind": "bonus_shares",
               "instrument": "SH600001", "quantity": "10", "estimated_value": "0",
               "integer_lower_value": "0"}]
    report = position_count_diagnostics(result(claims=claims))
    assert report["risk"]["coverage_complete"] is False
    assert report["risk"]["mean_risky_nav_fraction"] is None
    assert report["risk"]["max_actual_security_count"] == 1


@pytest.mark.parametrize("key", ["positions", "claims", "trades"])
def test_absent_record_inventory_is_not_assumed_to_be_an_empty_cash_account(key):
    source = result()
    del source["records"][key]
    with pytest.raises(ValueError, match="Complete original records"):
        position_count_diagnostics(source)
