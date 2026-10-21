from dataclasses import replace
from decimal import Decimal as D

import pytest

from stopat30m import ashare_capital_turnover_execution as capped
from stopat30m import ashare_stability_rank_execution as rank
from stopat30m.backtest import ashare_execution as ex


SIGNAL, DAY, NEXT = "2024-12-31", "2025-01-02", "2025-01-03"
CALENDAR = [SIGNAL, DAY, NEXT, "2025-01-06"]
A, B, C, KEEP = "SH600001", "SH600002", "SH600003", "SH600004"
POLICY = ex.ExecutionPolicy()


def position(quantity, *, locked=0, price="10"):
    return ex.Position(quantity, bought_today=locked, bought_date=DAY if locked else SIGNAL,
        last_mark=D(price), last_mark_date=SIGNAL, last_mark_basis="observed_raw_close")


def plan(*, exits=(), entries=(), retained=(), prior="100000"):
    return rank.RankBufferPlan(SIGNAL, tuple(retained) + tuple(row[0] for row in entries),
        tuple(retained), tuple(exits), tuple(entries), prior, str(D(prior) / 20), (), rank.RankBufferPolicy())


def inputs(the_plan, *, prices=None, boards=None, precloses=None, policy=POLICY):
    symbols = set(the_plan.retained) | set(the_plan.exits) | {row[0] for row in the_plan.entries}
    prices, boards, precloses = prices or {}, boards or {}, precloses or {}
    return dict(plan=the_plan, session=DAY, calendar=CALENDAR, policy=policy,
        quotes={inst: ex.OpeningQuote(DAY, D(prices.get(inst, "10")),
            D(precloses.get(inst, prices.get(inst, "10"))), "trading", "synthetic raw opening") for inst in symbols},
        metadata={inst: ex.SecurityInfo(inst, boards.get(inst, "sse_main"), "2000-01-03", False,
            "2024-12-01T23:59:59+08:00", "synthetic prior-known metadata", listing_session_number=100)
            for inst in symbols})


def execute(state, **kwargs):
    return capped.execute_capital_turnover_plan(state, fraction="0.05", initial_build=False, **kwargs)


def totals(result, side):
    return sum((D(row["notional"]) for row in result.trades if row["side"] == side), D(0))


def test_initial_exemption_has_exact_original_ledger_and_audit_even_after_session_action():
    state = ex.PortfolioState(D("100000"), session=DAY, applied_actions={"no_entitlement": {"record": {}}})
    kwargs = inputs(plan(entries=((A, 4900, "10", "sse_main"), (B, 4900, "10", "sse_main"))))
    original = rank.execute_rank_buffer_plan(state, **kwargs)
    actual = capped.execute_capital_turnover_plan(state, fraction="0.05", initial_build=True, **kwargs)
    assert actual.state.to_dict() == original.state.to_dict()
    assert actual.orders == original.orders and actual.trades == original.trades
    assert actual.opening_equity == original.opening_equity
    assert {k: v for k, v in actual.audit.items() if k != "capital_turnover"} == original.audit
    assert totals(actual, "BUY") > 5000
    assert actual.audit["capital_turnover"]["cap_enforced"] is False


@pytest.mark.parametrize("fraction", ["0.05", "0.075", "0.10"])
def test_independent_side_caps_use_prior_nav_floor_cent_and_original_fees(fraction):
    state = ex.PortfolioState(D("80000"), {A: position(2000)})
    before = state.to_dict()
    kwargs = inputs(plan(exits=(A,), entries=((B, 3000, "10", "sse_main"),), prior="100000.19"))
    actual = capped.execute_capital_turnover_plan(state, fraction=fraction, initial_build=False, **kwargs)
    audit = actual.audit["capital_turnover"]
    budget = (D("100000.19") * D(fraction)).quantize(D(".01"), rounding="ROUND_FLOOR")
    assert D(audit["budget_per_side_cny"]) == budget
    assert 0 < totals(actual, "BUY") <= budget and 0 < totals(actual, "SELL") <= budget
    assert totals(actual, "BUY") + totals(actual, "SELL") > budget
    assert all(D(t["commission"]) == max(D(5), ex.money(D(t["notional"]) * POLICY.commission_rate))
        for t in actual.trades)
    assert all(D(t["slippage_cost"]) == ex.money(t["quantity"] * abs(D(t["execution_price"]) - D(t["reference_price"])))
        for t in actual.trades)
    assert actual.state.cash == state.cash + sum((D(t["cash_change"]) for t in actual.trades), D(0))
    assert state.to_dict() == before


def test_gap_reference_buy_adverse_tick_and_sell_upper_bound_protect_actual_cap():
    state = ex.PortfolioState(D("70000"), {A: position(2000, price="15")})
    kwargs = inputs(plan(exits=(A,), entries=((B, 2000, "10", "sse_main"),)),
        prices={A: "15.99", B: "10.03"}, precloses={A: "15", B: "10"})
    actual = execute(state, **kwargs)
    audit = actual.audit["capital_turnover"]
    buy, sell = audit["allocations"]["BUY"][0], audit["allocations"]["SELL"][0]
    assert D(buy["adverse_tick_bound"]) == D("10.04")
    assert buy["capped_quantity"] == 400 and sell["capped_quantity"] == 300
    assert totals(actual, "BUY") <= 5000 and totals(actual, "SELL") <= 5000
    assert actual.opening_equity == D("101980.00")
    assert D(actual.audit["full_opening_equity_cny"]) == actual.opening_equity
    assert D(actual.audit["capital_virtual_opening_equity_cny"]) == D("74797.00")


def test_partial_exit_preserves_locked_shares_original_mark_and_action_state():
    state = ex.PortfolioState(D("1000"), {A: position(1000, locked=700), KEEP: position(100)}, session=DAY,
        entitlements={"cash": {"kind": "cash_dividend", "status": "pending", "instrument": A, "net_cash": "4.00"}},
        claims={"fraction": {"instrument": A, "quantity": ".4", "last_mark": "10"}},
        applied_actions={"source": {"sha256": "a", "record": {}}}, holdings_history={SIGNAL: {A: 1000}})
    before = state.to_dict()
    actual = execute(state, **inputs(plan(exits=(A,), retained=(KEEP,))))
    assert [t["quantity"] for t in actual.trades] == [300]
    assert actual.state.positions[A].quantity == 700
    assert actual.state.positions[A].bought_today == 700 and actual.state.positions[A].bought_date == DAY
    assert actual.state.positions[A].last_mark_date == DAY
    assert actual.state.positions[KEEP] == state.positions[KEEP]
    assert actual.state.entitlements == state.entitlements and actual.state.claims == state.claims
    assert actual.state.applied_actions == state.applied_actions and actual.state.holdings_history == state.holdings_history
    assert state.to_dict() == before
    assert actual.audit["capital_turnover"]["partial_kept_shares"] == {A: 700}


@pytest.mark.parametrize("held,budget,expected", [(300, "500", 0), (350, "500", 50),
    (350, "2500", 250), (50, "500", 50), (300, "2999", 200)])
def test_odd_lot_legality_is_derived_from_full_real_position(held, budget, expected):
    state = ex.PortfolioState(D("10000"), {A: position(held)})
    prior = str(D(budget) / D(".05"))
    actual = execute(state, **inputs(plan(exits=(A,), prior=prior), policy=replace(POLICY, slippage_bps=D(0))))
    filled = sum(t["quantity"] for t in actual.trades if t["side"] == "SELL")
    assert filled == expected
    assert actual.state.positions.get(A, ex.Position(0)).quantity == held - expected


def test_same_session_full_T1_lock_keeps_all_shares_and_does_not_reassign_exit_budget():
    state = ex.PortfolioState(D("5000"), {A: position(1000, locked=1000), B: position(1000)}, session=DAY)
    actual = execute(state, **inputs(plan(exits=(A, B))))
    assert not actual.trades
    assert actual.state.positions[A].bought_today == 1000
    rows = actual.audit["capital_turnover"]["allocations"]["SELL"]
    assert rows[0]["capped_quantity"] == 0 and D(rows[0]["reserved_cny"]) == 5000
    assert D(rows[1]["reserved_cny"]) == 0


def test_lower_limit_rejection_keeps_reserved_quota_and_does_not_sell_next_name():
    state = ex.PortfolioState(D("1000"), {A: position(1000), B: position(1000)})
    actual = execute(state, **inputs(plan(exits=(A, B)), prices={A: "9"}, precloses={A: "10"}))
    assert not actual.trades and actual.state.positions[A].quantity == 1000
    assert any("lower-limit" in row.get("reason", "") and row.get("side") == "SELL" for row in actual.orders)
    rows = actual.audit["capital_turnover"]["allocations"]["SELL"]
    assert D(rows[0]["reserved_cny"]) == 5000 and D(rows[1]["reserved_cny"]) == 0


@pytest.mark.parametrize("failure", ["missing_quote", "invalid_metadata", "suspended", "future_metadata"])
def test_invalid_source_reserves_prior_price_but_never_fills_or_reallocates(failure):
    state = ex.PortfolioState(D("70000"), {A: position(1000), B: position(1000)})
    kwargs = inputs(plan(exits=(A, B), entries=((C, 1000, "10", "sse_main"),)))
    if failure == "missing_quote":
        kwargs["quotes"].pop(A)
    elif failure == "invalid_metadata":
        kwargs["metadata"].pop(A)
    elif failure == "suspended":
        kwargs["quotes"][A] = replace(kwargs["quotes"][A], tradestatus="suspended")
    else:
        kwargs["metadata"][A] = replace(kwargs["metadata"][A], known_at="2025-01-03T08:00:00+08:00")
    actual = execute(state, **kwargs)
    assert not [t for t in actual.trades if t["side"] == "SELL"]
    assert any(row["instrument"] == A and row["status"] == "rejected" for row in actual.orders)
    assert actual.state.positions[A].quantity == 1000 and actual.state.positions[B].quantity == 1000
    rows = actual.audit["capital_turnover"]["allocations"]["SELL"]
    assert D(rows[0]["reserved_cny"]) == 5000 and D(rows[1]["reserved_cny"]) == 0
    assert rows[0]["validation_reason"]


def test_rejected_first_buy_does_not_reallocate_to_lower_ranked_frozen_entry():
    state = ex.PortfolioState(D("100000"))
    kwargs = inputs(plan(entries=((A, 1000, "10", "sse_main"), (B, 1000, "10", "sse_main"))))
    kwargs["quotes"].pop(A)
    actual = execute(state, **kwargs)
    assert not actual.trades and actual.state.cash == state.cash
    assert any(row["instrument"] == A and row["status"] == "rejected" for row in actual.orders)
    rows = actual.audit["capital_turnover"]["allocations"]["BUY"]
    assert D(rows[0]["reserved_cny"]) == 5000 and rows[1]["capped_quantity"] == 0
    assert rows[0]["basis"] == "prior_observed_price_reservation_only"


def test_no_affordable_lot_reservation_is_not_recycled_to_cheaper_later_candidate():
    kwargs = inputs(plan(entries=((A, 100, "100", "sse_main"), (B, 1000, "1", "sse_main"))),
        prices={A: "100", B: "1"})
    actual = execute(ex.PortfolioState(D("100000")), **kwargs)
    assert not actual.trades
    assert [r["capped_quantity"] for r in actual.audit["capital_turnover"]["allocations"]["BUY"]] == [0, 0]


@pytest.mark.parametrize("board,min_lot", [("star", 200), ("bse", 100), ("chinext", 100)])
def test_board_specific_buy_lots_and_partial_sell_minimum(board, min_lot):
    state = ex.PortfolioState(D("70000"), {A: position(1000)})
    kwargs = inputs(plan(exits=(A,), entries=((B, 1000, "10", board),)), boards={A: board, B: board})
    actual = execute(state, **kwargs)
    assert all(t["quantity"] >= min_lot for t in actual.trades)
    assert totals(actual, "BUY") <= 5000 and totals(actual, "SELL") <= 5000
    if board in {"star", "bse"}:
        assert [t["quantity"] for t in actual.trades if t["side"] == "BUY"] == [499]


def test_cash_affordability_keeps_original_minimum_commission_and_reservation():
    state = ex.PortfolioState(D("1004.99"))
    kwargs = inputs(plan(entries=((A, 500, "10", "sse_main"),)), policy=replace(POLICY, slippage_bps=D(0)))
    actual = execute(state, **kwargs)
    assert not actual.trades and actual.state.cash == state.cash
    assert any("Cash cannot cover" in row.get("reason", "") for row in actual.orders)


def test_pending_stock_and_fraction_claims_unchanged_and_not_sold_as_real_shares():
    state = ex.PortfolioState(D("100000"), entitlements={
        "whole": {"kind": "bonus_shares", "status": "pending", "instrument": A, "quantity": "100.4", "last_mark": "10"},
        "fraction_pending": {"kind": "bonus_shares", "status": "pending", "instrument": B, "quantity": ".4", "last_mark": "10"}},
        claims={"fraction": {"instrument": A, "quantity": ".4", "last_mark": "10"}})
    actual = execute(state, **inputs(plan(exits=(A, B), entries=((C, 1000, "10", "sse_main"),))))
    assert not [t for t in actual.trades if t["side"] == "SELL"]
    assert actual.state.entitlements == state.entitlements and actual.state.claims == state.claims
    assert actual.opening_equity == D("101000")
    assert all(row["requested_quantity"] == 0 for row in actual.audit["capital_turnover"]["allocations"]["SELL"])


def test_explicit_next_batch_is_capped_even_if_first_batch_had_zero_fills():
    state = ex.PortfolioState(D("100000"))
    kwargs = inputs(plan(entries=((A, 9000, "10", "sse_main"),)), prices={A: "11"}, precloses={A: "10"})
    first = capped.execute_capital_turnover_plan(state, fraction="0.05", initial_build=True, **kwargs)
    assert not first.trades and not first.state.positions
    later = inputs(replace(kwargs["plan"], signal_date=DAY))
    later["session"] = NEXT
    later["quotes"] = {A: replace(later["quotes"][A], session=NEXT)}
    actual = execute(first.state, **later)
    assert 0 < totals(actual, "BUY") <= 5000
    assert actual.audit["capital_turnover"]["cap_enforced"] is True


@pytest.mark.parametrize("fraction", [D(".05"), .05, True, "0.050", ".05", "0.15", "nan"])
def test_only_declared_decimal_string_parameters_are_accepted(fraction):
    with pytest.raises(ValueError, match="declared Decimal string"):
        capped.execute_capital_turnover_plan(ex.PortfolioState(D("100000")),
            fraction=fraction, initial_build=False, **inputs(plan()))


def test_initial_flag_is_strict_bool_and_changed_exposure_cannot_hide_in_protected_sleeve():
    with pytest.raises(ValueError, match="boolean initial_build"):
        capped.execute_capital_turnover_plan(ex.PortfolioState(D("100000")),
            fraction="0.05", initial_build=1, **inputs(plan()))
    with pytest.raises(ValueError, match="exposure changed"):
        execute(ex.PortfolioState(D("100000"), {A: position(100)}), **inputs(plan()))


def test_reservations_follow_frozen_priority_not_code_or_execution_sort_order():
    state = ex.PortfolioState(D("80000"), {A: position(1000), B: position(1000)})
    original_plan = plan(exits=(B, A), entries=((KEEP, 1000, "10", "sse_main"), (C, 1000, "10", "sse_main")))
    before = original_plan.to_dict()
    actual = execute(state, **inputs(original_plan))
    assert {t["instrument"] for t in actual.trades if t["side"] == "SELL"} == {B}
    assert {t["instrument"] for t in actual.trades if t["side"] == "BUY"} == {KEEP}
    assert original_plan.to_dict() == before
    assert actual.audit["capital_turnover"]["original_plan"] == before


def test_live_reference_and_clock_are_passed_through_not_historical_open_fallback():
    kwargs = inputs(plan(entries=((A, 1000, "10", "sse_main"),)))
    kwargs["quotes"][A] = replace(kwargs["quotes"][A], open=D("99"), execution_price=D("10.03"),
        execution_price_basis="live_observed_quote", asof=DAY + "T09:59:30+08:00")
    actual = execute(ex.PortfolioState(D("100000")), now=DAY + "T10:00:00+08:00", **kwargs)
    assert [D(t["reference_price"]) for t in actual.trades] == [D("10.03")]
    assert [D(t["execution_price"]) for t in actual.trades] == [D("10.04")]
    assert totals(actual, "BUY") <= 5000
    stale = execute(ex.PortfolioState(D("100000")), now=DAY + "T10:05:00+08:00", **kwargs)
    assert not stale.trades and any("stale" in row.get("reason", "") for row in stale.orders)
    assert stale.audit["capital_turnover"]["allocations"]["BUY"][0]["basis"] == "prior_observed_price_reservation_only"


def test_adverse_tick_bound_above_upper_limit_is_conservative_without_double_slippage():
    kwargs = inputs(plan(entries=((A, 1000, "10", "sse_main"),)), prices={A: "10.50"},
        precloses={A: "10"}, policy=replace(POLICY, slippage_bps=D("1000")))
    actual = execute(ex.PortfolioState(D("100000")), **kwargs)
    assert D(actual.audit["capital_turnover"]["allocations"]["BUY"][0]["adverse_tick_bound"]) == D("11.55")
    assert [D(t["execution_price"]) for t in actual.trades] == [D("11.00")]
    # Gross actual is 4400; its embedded 200 slippage is reported separately,
    # not added again to the 5000 cap measurement.
    assert totals(actual, "BUY") == 4400
    assert D(actual.audit["capital_turnover"]["actual_buy_notional_cny"]) == 4400


@pytest.mark.parametrize("board,held,locked,budget,expected", [
    ("sse_main", 300, 250, "500", 50), ("star", 1000, 0, "1500", 0),
    ("star", 150, 0, "1500", 150), ("star", 1000, 850, "1500", 150)])
def test_full_real_available_quantity_preserves_original_oddlot_and_star_exception(board, held, locked, budget, expected):
    state = ex.PortfolioState(D("5000"), {A: position(held, locked=locked)}, session=DAY)
    kwargs = inputs(plan(exits=(A,), prior=str(D(budget) / D(".05"))), boards={A: board})
    actual = execute(state, **kwargs)
    assert sum(t["quantity"] for t in actual.trades) == expected
    remaining = actual.state.positions.get(A, ex.Position(0))
    assert remaining.quantity == held - expected and remaining.bought_today == locked


def test_sell_proceeds_cannot_fund_even_minimum_fee_and_pending_bonus_is_unspendable():
    state = ex.PortfolioState(D("0"), {A: position(1, price="1")}, entitlements={
        "bonus": {"kind": "bonus_shares", "status": "pending", "instrument": A, "quantity": "100.4", "last_mark": "1"}})
    kwargs = inputs(plan(exits=(A,)), prices={A: "1"})
    actual = execute(state, **kwargs)
    assert not actual.trades and actual.state.cash == 0 and actual.state.positions[A].quantity == 1
    assert actual.state.entitlements == state.entitlements
    assert any("order fees" in row.get("reason", "") for row in actual.orders)


def test_pending_bonus_survives_partial_exit_and_full_opening_nav_includes_it_once():
    state = ex.PortfolioState(D("10000"), {A: position(1000)}, entitlements={
        "bonus": {"kind": "bonus_shares", "status": "pending", "instrument": A, "quantity": "100.4", "last_mark": "10"}})
    actual = execute(state, **inputs(plan(exits=(A,))))
    assert actual.state.positions[A].quantity == 500
    assert actual.state.entitlements == state.entitlements
    assert actual.opening_equity == D("21000") == D(actual.audit["full_opening_equity_cny"])
    assert D(actual.audit["capital_virtual_opening_equity_cny"]) == D("16000")
