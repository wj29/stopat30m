from dataclasses import replace
from decimal import Decimal as D

import pytest

from stopat30m import ashare_exposure_matching_execution as overlay
from stopat30m.backtest import ashare_execution as ex


SIGNAL, DAY = "2024-12-31", "2025-01-02"
CALENDAR = [SIGNAL, DAY, "2025-01-03"]
A, B, C = "SH600001", "SH600002", "SH600003"
POLICY = ex.ExecutionPolicy()


def position(quantity, *, locked=0, price="10"):
    return ex.Position(quantity, bought_today=locked, bought_date=DAY if locked else SIGNAL,
        last_mark=D(price), last_mark_date=SIGNAL, last_mark_basis="observed_raw_close")


def inputs(*symbols, prices=None, boards=None):
    prices, boards = prices or {}, boards or {}
    return dict(session=DAY, signal_date=SIGNAL, calendar=CALENDAR, policy=POLICY,
        quotes={inst: ex.OpeningQuote(DAY, D(prices.get(inst, "10")), D(prices.get(inst, "10")),
            "trading", "synthetic observed opening") for inst in symbols},
        metadata={inst: ex.SecurityInfo(inst, boards.get(inst, "sse_main"), "2000-01-03", False,
            "2024-12-01T23:59:59+08:00", "synthetic prior-known identity", listing_session_number=100)
            for inst in symbols})


def run(state, target, **kwargs):
    return overlay.execute_exposure_overlay(state, target_fraction=target, **kwargs)


def audit(result):
    return result.audit["exposure_matching"]


@pytest.mark.parametrize("target", ["0.49", "0.50", "0.51", D(".505")])
def test_fixed_inclusive_one_percentage_point_deadband_has_no_real_orders(target):
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    original = state.to_dict()
    result = run(state, target, **inputs(A))
    assert not result.orders and not result.trades
    assert result.state.cash == state.cash and result.state.positions == state.positions
    assert state.to_dict() == original
    assert audit(result)["skip_reason"] == "within_fixed_deadband"
    assert audit(result)["actual_engine_calls"] == 0
    assert D(audit(result)["current_open_fraction_upper"]) == D(".5")


@pytest.mark.parametrize("target,side,expected", [(".4", "SELL", 400), (".6", "BUY", 400)])
def test_proportional_single_direction_real_fills_cash_shares_and_original_fees(target, side, expected):
    state = ex.PortfolioState(D("20000"), {A: position(1000), B: position(1000)},
        applied_actions={"evidence": {"sha256": "a", "record": {}}}, holdings_history={SIGNAL: {A: 1000, B: 1000}})
    before = state.to_dict()
    result = run(state, target, **inputs(A, B))
    assert {t["side"] for t in result.trades} == {side}
    assert sum(t["quantity"] for t in result.trades) == expected
    assert len(result.trades) == 2
    assert all(D(t["commission"]) == 5 for t in result.trades)
    assert result.state.cash == state.cash + sum((D(t["cash_change"]) for t in result.trades), D(0))
    assert result.state.applied_actions == state.applied_actions and result.state.holdings_history == state.holdings_history
    assert state.to_dict() == before
    assert audit(result)["cash_and_share_bridge_verified"] is True
    assert audit(result)["new_identity_added"] is False and audit(result)["actual_engine_calls"] == 1
    assert D(audit(result)["total_fee_cny"]) == sum(D(t["total_fee"]) for t in result.trades)
    assert D(audit(result)["slippage_cost_cny"]) == sum(D(t["slippage_cost"]) for t in result.trades)


def test_proportional_remainder_ties_use_code_and_only_one_extra_legal_lot_each():
    state = ex.PortfolioState(D("14000"), {B: position(300), A: position(300)})
    result = run(state, ".15", **inputs(A, B))
    rows = {r["instrument"]: r for r in audit(result)["allocations"]}
    assert [D(rows[i]["ideal_trade_quantity"]) for i in (A, B)] == [D(150), D(150)]
    assert [rows[i]["floor_approved_quantity"] for i in (A, B)] == [100, 100]
    assert [rows[i]["additional_lot_quantity"] for i in (A, B)] == [100, 0]
    assert audit(result)["remainder_priority"] == [A, B]
    assert audit(result)["approved_trade_quantities"] == {A: 200, B: 100}
    assert audit(result)["no_post_refusal_reallocation"] is True


def test_remainder_allocation_can_move_when_every_proportional_floor_is_zero():
    symbols = [f"SH{600000+i:06d}" for i in range(20)]
    state = ex.PortfolioState(D("80000"), {i: position(100) for i in reversed(symbols)})
    result = run(state, ".188", **inputs(*symbols))
    assert len(result.trades) == 1 and result.trades[0]["instrument"] == symbols[0]
    assert result.trades[0]["quantity"] == 100 and result.trades[0]["side"] == "SELL"
    assert all(r["floor_approved_quantity"] == 0 for r in audit(result)["allocations"])
    assert sum(r["additional_lot_quantity"] > 0 for r in audit(result)["allocations"]) == 1


def test_oddlot_floor_uses_full_real_holding_not_a_small_virtual_liquidation():
    regular = ex.PortfolioState(D("7000"), {A: position(300)})
    result = run(regular, ".25", **inputs(A))
    assert not result.trades  # wanted50 is not a legal partial sale of300.
    odd = ex.PortfolioState(D("6500"), {A: position(350)})
    result = run(odd, ".30", **inputs(A))
    assert [t["quantity"] for t in result.trades] == [50]
    assert result.state.positions[A].quantity == 300


@pytest.mark.parametrize("target,side", [(".425", "SELL"), (".575", "BUY")])
def test_star_remainder_from_zero_requires_200_share_minimum(target, side):
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    result = run(state, target, **inputs(A, boards={A: "star"}))
    row = audit(result)["allocations"][0]
    assert D(row["ideal_trade_quantity"]) == 150 and row["floor_approved_quantity"] == 0
    assert row["additional_lot_quantity"] == 200
    assert [t["quantity"] for t in result.trades] == [200] and result.trades[0]["side"] == side


@pytest.mark.parametrize("board", ["star", "bse", "chinext", "sse_main", "szse_main"])
def test_supported_board_quantities_pass_original_engine_rules(board):
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    result = run(state, ".3", **inputs(A, boards={A: board}))
    assert result.trades and result.trades[0]["side"] == "SELL"
    assert result.state.positions[A].quantity == 600


def test_same_day_core_buy_T1_lock_keeps_actual_shares_and_unmatched_residual():
    state = ex.PortfolioState(D("10000"), {A: position(1000, locked=1000)}, session=DAY)
    result = run(state, ".2", **inputs(A))
    assert not result.trades
    assert result.state.positions[A].quantity == 1000 and result.state.positions[A].bought_today == 1000
    row = audit(result)["allocations"][0]
    assert row["available_quantity"] == 0 and row["approved_trade_quantity"] == 0
    assert row["bought_today_locked_quantity"] == 1000
    assert D(audit(result)["unmatched_fraction"]) == D(".3")


def test_partial_T1_lock_and_original_odd_available_liquidation_remain_legal():
    state = ex.PortfolioState(D("7000"), {A: position(300, locked=250)}, session=DAY)
    result = run(state, ".20", **inputs(A))
    assert [t["quantity"] for t in result.trades] == [50]
    assert result.state.positions[A].quantity == 250 and result.state.positions[A].bought_today == 250


@pytest.mark.parametrize("side,target,price,preclose", [("SELL", ".1", "9", "10"), ("BUY", ".8", "11", "10")])
def test_limit_queue_rejection_is_retained_without_reallocation_or_second_engine_pass(side, target, price, preclose):
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    kwargs = inputs(A, prices={A: price})
    kwargs["quotes"][A] = replace(kwargs["quotes"][A], preclose=D(preclose))
    result = run(state, target, **kwargs)
    assert not result.trades and result.state.positions[A].quantity == 1000
    assert any(row.get("side") == side and "limit" in row.get("reason", "") for row in result.orders)
    assert audit(result)["actual_engine_calls"] == 1 and audit(result)["no_post_refusal_reallocation"] is True


def test_missing_current_quote_skips_all_and_never_fills_from_carried_mark():
    state = ex.PortfolioState(D("10000"), {A: position(1000), B: position(1000)})
    kwargs = inputs(A, B)
    kwargs["quotes"].pop(A)
    result = run(state, ".1", **kwargs)
    assert not result.orders and not result.trades
    assert result.state.positions == state.positions and result.opening_equity == 30000
    assert audit(result)["coverage_complete"] is False
    assert audit(result)["skip_reason"] == "unknown_current_security_reference"
    assert audit(result)["post_open_fraction_upper"] is None


@pytest.mark.parametrize("failure", ["suspended", "future_live", "stale_live"])
def test_invalid_execution_source_whole_overlay_skip_preserves_original_market_rules(failure):
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    kwargs = inputs(A)
    if failure == "suspended":
        kwargs["quotes"][A] = replace(kwargs["quotes"][A], tradestatus="suspended")
    else:
        kwargs["quotes"][A] = replace(kwargs["quotes"][A], execution_price=D("10"), execution_price_basis="live_observed_quote",
            asof=DAY+("T10:01:00+08:00" if failure == "future_live" else "T09:00:00+08:00"))
        kwargs["now"] = DAY+"T10:00:00+08:00"
    result = run(state, ".1", **kwargs)
    assert not result.trades and audit(result)["coverage_complete"] is False


def test_current_live_price_is_used_and_fake_historical_open_is_not_a_fill():
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    kwargs = inputs(A)
    kwargs["quotes"][A] = replace(kwargs["quotes"][A], open=D("99"), execution_price=D("10"),
        execution_price_basis="live_observed_quote", asof=DAY+"T09:59:50+08:00")
    result = run(state, ".3", now=DAY+"T10:00:00+08:00", **kwargs)
    assert result.trades and all(D(t["reference_price"]) == 10 for t in result.trades)


def test_unknown_target_is_skipped_without_carrying_a_previous_target():
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    result = run(state, None, **inputs(A))
    assert not result.trades and not result.orders
    assert audit(result)["skip_reason"] == "unknown_reference_target"
    assert audit(result)["target_fraction"] is None and audit(result)["coverage_complete"] is True


def test_cash_dividend_receivable_is_in_lower_NAV_but_never_in_security_risk():
    state = ex.PortfolioState(D("10000"), {A: position(1000)}, entitlements={
        "cash": {"kind": "cash_dividend", "status": "pending", "instrument": A, "net_cash": "10000.00"}})
    result = run(state, str(D(1)/3), **inputs(A))
    assert not result.trades and result.opening_equity == 30000
    assert D(audit(result)["current_risk_upper_cny"]) == 10000
    assert D(audit(result)["cash_dividend_receivable_excluded_from_risk_cny"]) == 10000
    assert result.state.entitlements == state.entitlements


def test_pending_bonus_whole_and_fraction_bounds_are_fixed_not_extra_sellable_shares():
    state = ex.PortfolioState(D("10000"), {A: position(1000)}, entitlements={
        "bonus": {"kind": "bonus_shares", "status": "pending", "instrument": A, "quantity": "100.4", "last_mark": "10"}})
    result = run(state, ".4", **inputs(A))
    assert result.state.entitlements == state.entitlements
    assert result.opening_equity == 21000
    assert D(audit(result)["current_noncash_lower_cny"]) == 1000
    assert D(audit(result)["current_noncash_upper_cny"]) == 1010
    assert [t["quantity"] for t in result.trades] == [300]
    assert result.state.positions[A].quantity == 700


def test_bounded_fraction_claim_is_risk_not_an_identity_available_for_new_purchase():
    state = ex.PortfolioState(D("10000"), {A: position(1000)}, claims={
        "fraction": {"instrument": B, "quantity": ".4", "created_date": SIGNAL, "last_mark": "10",
            "allocation": "unresolved_CSDC_fractional_allocation", "maximum_additional_integer_shares": 1,
            "spendable": False}})
    result = run(state, ".6", **inputs(A, B))
    assert all(t["instrument"] == A for t in result.trades)
    assert B not in result.state.positions and result.state.claims == state.claims
    assert D(audit(result)["current_noncash_upper_cny"]) == 10
    assert audit(result)["active_identities_before"] == [A]


def test_unbounded_non_cash_claim_skips_whole_overlay_not_silently_zero():
    state = ex.PortfolioState(D("10000"), {A: position(1000)}, claims={"unknown": {"instrument": B, "quantity": ".4"}})
    result = run(state, ".1", **inputs(A, B))
    assert not result.trades and audit(result)["skip_reason"] == "undefined_noncash_risk_bounds"
    assert audit(result)["coverage_complete"] is False
    assert audit(result)["current_open_fraction_upper"] is None


def test_pending_only_or_empty_account_cannot_manufacture_new_physical_weights():
    for state, kwargs in [(ex.PortfolioState(D("10000")), inputs()),
        (ex.PortfolioState(D("10000"), entitlements={"bonus": {
            "kind": "bonus_shares", "status": "pending", "instrument": A, "quantity": "100", "last_mark": "10"}}), inputs(A))]:
        result = run(state, ".8", **kwargs)
        assert not result.trades and not result.state.positions
        assert audit(result)["skip_reason"] == "no_physical_value_for_proportional_allocation"


def test_buy_cash_search_preserves_minimum_commission_no_borrowing():
    state = ex.PortfolioState(D("1000.00"), {A: position(100)})
    result = run(state, "1", **inputs(A))
    assert not result.trades  #100new shares+adverse tick+5commission cannot fit.
    assert result.state.cash == 1000
    assert any("Cash cannot cover" in row.get("reason", "") for row in result.orders)
    assert D(audit(result)["unmatched_fraction"]) != 0


def test_original_core_and_overlay_orders_are_not_netted_or_refunded_minimum_fees():
    state = ex.PortfolioState(D("90000"), {A: position(1000)})
    kwargs = inputs(A)
    core = ex.execute_target_weights(state, targets={A: D(".05")}, **kwargs)
    result = run(core.state, ".01", **kwargs)
    assert core.trades and result.trades
    assert core.trades[0]["instrument"] == result.trades[0]["instrument"] == A
    assert D(core.trades[0]["commission"]) == D(result.trades[0]["commission"]) == 5
    assert result.state.cash == core.state.cash + sum((D(t["cash_change"]) for t in result.trades), D(0))


def test_unpriceable_original_NAV_has_specific_error_and_does_not_invent_cash_only_NAV():
    state = ex.PortfolioState(D("10000"), {A: ex.Position(100)})
    with pytest.raises(overlay.ExposureMatchingUnavailableNAVError, match="Unpriced held shares"):
        run(state, None, **inputs())
    state = ex.PortfolioState(D("10000"), entitlements={"bonus": {
        "kind": "bonus_shares", "status": "pending", "instrument": A, "quantity": "100"}})
    with pytest.raises(overlay.ExposureMatchingUnavailableNAVError, match="Unpriced pending stock"):
        run(state, ".1", **inputs())


@pytest.mark.parametrize("target", [True, "NaN", "Infinity", "-0.1", "1.01"])
def test_bad_target_is_not_clipped_or_misclassified_as_NAV_error(target):
    with pytest.raises(ValueError) as caught:
        run(ex.PortfolioState(D("10000"), {A: position(100)}), target, **inputs(A))
    assert not isinstance(caught.value, overlay.ExposureMatchingUnavailableNAVError)


def test_invalid_signal_or_finalized_session_remains_logic_error_not_swallowed_NAV_error():
    state = ex.PortfolioState(D("10000"), {A: position(100)})
    kwargs = inputs(A)
    kwargs["signal_date"] = DAY
    with pytest.raises(ValueError, match="preceding actual") as caught:
        run(state, ".1", **kwargs)
    assert not isinstance(caught.value, overlay.ExposureMatchingUnavailableNAVError)
    state.holdings_history[DAY] = {A: 100}
    with pytest.raises(ValueError, match="finalized closing") as caught:
        run(state, ".1", **inputs(A))
    assert not isinstance(caught.value, overlay.ExposureMatchingUnavailableNAVError)


@pytest.mark.parametrize("quantity", [1, 3, 101, 103, 149, 201, 350, 509])
@pytest.mark.parametrize("cash", ["1000.13", "12345.67", "99999.91"])
@pytest.mark.parametrize("price", ["2.83", "37.49", "67.31"])
def test_high_precision_targets_never_turn_zero_approved_BUY_oddlot_into_SELL(quantity, cash, price):
    state = ex.PortfolioState(D(cash), {A: position(quantity, price=price),
        B: position(299, price="6.13"), C: position(550, price="23.11")})
    physical = D(price)*quantity + D("6.13")*299 + D("23.11")*550
    target = min(D(1), physical/(physical + D(cash)) + D(".015"))
    result = run(state, target, **inputs(A, B, C, prices={A: price, B: "6.13", C: "23.11"}))
    assert all(t["side"] == "BUY" for t in result.trades)
    expected = audit(result)["approved_target_quantities"]
    weights = audit(result)["submitted_target_weights"]
    nav = D(audit(result)["opening_nav_lower_cny"])
    for inst, qty in expected.items():
        assert int(nav*D(weights[inst])/D({A: price, B: "6.13", C: "23.11"}[inst])) == qty
        assert qty >= state.positions[inst].quantity
    assert audit(result)["engine_integer_targets_exact_verified"] is True


def test_unknown_security_rules_distinguish_known_price_risk_from_executable_coverage():
    state = ex.PortfolioState(D("10000"), {A: position(1000)})
    kwargs = inputs(A)
    kwargs["metadata"][A] = replace(kwargs["metadata"][A], is_st=None)
    result = run(state, ".2", **kwargs)
    assert not result.trades and not result.orders
    assert audit(result)["risk_coverage_complete"] is True
    assert audit(result)["execution_rules_complete"] is False
    assert audit(result)["coverage_complete"] is False
    assert D(audit(result)["current_open_fraction_upper"]) == D(".5")


@pytest.mark.parametrize("invalid_identity", ["missing", "wrong_instrument"])
def test_missing_or_wrong_metadata_plus_gap_preserves_probe_NAV_and_skips_instead_of_inventing_mark(invalid_identity):
    state = ex.PortfolioState(D("10000"), {A: position(1000, price="10")})
    before = state.to_dict()
    kwargs = inputs(A, prices={A: "11"})
    if invalid_identity == "missing":
        kwargs["metadata"] = {}
    else:
        kwargs["metadata"][A] = replace(kwargs["metadata"][A], instrument=B)
    result = run(state, ".3", **kwargs)
    assert result.opening_equity == D("20000")  # Original permitted carried-mark NAV, not21000.
    assert result.state.positions == state.positions and result.state.cash == state.cash
    assert state.to_dict() == before and not result.orders and not result.trades
    assert audit(result)["skip_reason"] == "unavailable_original_security_rules"
    assert audit(result)["coverage_complete"] is False and audit(result)["risk_coverage_complete"] is False
    assert audit(result)["execution_rules_complete"] is False
    assert audit(result)["current_open_fraction_upper"] is None
    assert audit(result)["post_open_fraction_upper"] is None
    assert audit(result)["actual_engine_calls"] == 0


def test_NAV_mismatch_without_a_real_rules_gap_is_still_an_unmasked_logic_error(monkeypatch):
    original = ex.execute_target_weights
    def inconsistent_probe(*args, **kwargs):
        result = original(*args, **kwargs)
        result.opening_equity += D("1.00")
        return result
    monkeypatch.setattr(ex, "execute_target_weights", inconsistent_probe)
    with pytest.raises(ValueError, match="components disagree") as caught:
        run(ex.PortfolioState(D("10000"), {A: position(1000)}), ".3", **inputs(A))
    assert not isinstance(caught.value, overlay.ExposureMatchingUnavailableNAVError)
