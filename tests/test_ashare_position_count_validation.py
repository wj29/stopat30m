"""Synthetic A10 count-only comparisons; no real data, model fit or paper IO."""
from copy import deepcopy
from decimal import Decimal, ROUND_HALF_UP
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from stopat30m import ashare_position_count_validation as counts
from stopat30m import ashare_stability_validation as stability
from stopat30m import ashare_turnover_validation as turnover
from stopat30m.backtest import ashare_execution as ex
from test_ashare_turnover_validation import market
from test_ashare_horizon_validation import source_context


DECLARED_COUNTS = (20, 15, 10, 5)
RULE = {"key": "hold10_cap2", "minimum_hold_sessions": 10, "max_replacements": 2}


def sessions(data, period="2025"):
    start, end = data["protocol"]["periods"][period]
    return [day for day in data["calendar"] if start <= day <= end]


def money(value):
    return Decimal(value).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)


def run(data, n, period="2025"):
    return counts.simulate_position_count(data, period=period, top_k=n)


@pytest.mark.parametrize("period", ["2025", "2026", "continuous"])
@pytest.mark.parametrize("scenario", ["static", "rotating", "rejected_exit"])
def test_twenty_is_exact_entire_original_hold10_cap2_result(period, scenario):
    data = market(changing=scenario != "static", refused_exit=scenario == "rejected_exit")
    expected = turnover.simulate_turnover(deepcopy(data), period=period, variant=dict(RULE))
    actual = run(data, 20, period)
    assert actual == expected  # Includes every record, fee, origin, validity and metadata.
    assert "top_k" not in actual and "position_count" not in actual


@pytest.mark.parametrize("n", DECLARED_COUNTS)
def test_count_changes_only_prior_selection_and_equal_slot_budget(n):
    data = market(end_at=0)
    original_execution = deepcopy(data["protocol"]["execution"])
    result = run(data, n)
    plan = result["records"]["plans"][0]["plan"]
    expected_names = [f"SH{600000+i:06d}" for i in range(n)]
    assert plan["selected"] == expected_names
    assert list(result["final_state"]["positions"]) == expected_names
    assert plan["top_k"] == n and plan["keep_rank"] == 40
    assert plan["require_signal_affordability"] is True
    assert Decimal(plan["budget_per_slot_cny"]) == Decimal("100000") / n
    assert plan["prior_equity_cny"] == "100000.00"
    assert len(result["records"]["trades"]) == n
    assert all(trade["instrument"] in expected_names for trade in result["records"]["trades"])
    assert result["variant"] == RULE
    assert data["protocol"]["execution"] == original_execution == stability.EXECUTION
    assert original_execution["top_k"] == 20  # No protocol rewrite to force the planner.
    assert counts.TARGET_COUNTS == DECLARED_COUNTS


@pytest.mark.parametrize("n", DECLARED_COUNTS)
def test_original_fee_ticks_board_lots_and_cash_reconcile_without_slippage_double_charge(n):
    data = market(end_at=7)
    result = run(data, n)
    trades = result["records"]["trades"]
    running = Decimal("100000.00")
    for trade in trades:
        assert trade["status"] == "simulated_fill" and trade["live_order"] is False
        assert trade["side"] == "BUY" and trade["quantity"] > 0 and trade["quantity"] % 100 == 0
        price = Decimal(trade["execution_price"])
        assert price == Decimal("10.01")  # Prior raw10, unchanged adverse2bps rounded to CNY tick.
        notional = money(price * trade["quantity"])
        commission = max(Decimal("5.00"), money(notional * Decimal(".0003")))
        transfer = money(notional * Decimal(".00001"))
        assert Decimal(trade["notional"]) == notional
        assert Decimal(trade["commission"]) == commission
        assert Decimal(trade["transfer_fee"]) == transfer
        assert Decimal(trade["stamp_tax"]) == 0
        assert Decimal(trade["total_fee"]) == commission + transfer
        assert Decimal(trade["slippage_cost"]) == money(Decimal(".01") * trade["quantity"])
        assert Decimal(trade["cash_before"]) == running
        assert Decimal(trade["cash_change"]) == -notional - commission - transfer
        running += Decimal(trade["cash_change"])
        assert Decimal(trade["cash_after"]) == running >= 0
    assert running == Decimal(result["final_state"]["cash"])
    marked = sum(Decimal(position["quantity"]) * 10 for position in result["final_state"]["positions"].values())
    assert running + marked == Decimal(result["metrics"]["final_equity_cny"])
    fee_and_slip = sum(Decimal(t["total_fee"]) + Decimal(t["slippage_cost"]) for t in trades)
    assert Decimal("100000") - fee_and_slip == running + marked
    assert len(trades) == n  # Existing quantities are retained rather than reweighted at D5.


def test_concentrated_arm_still_charges_a_binding_five_yuan_minimum_when_lot_is_large():
    data = market(end_at=0)
    for field, value in {"open": "110", "close": "110", "preclose": "110", "high": "111", "low": "109", "amount": "1100000"}.items():
        data["raw"][field] = value
    result = run(data, 5)
    assert len(result["records"]["trades"]) == 5
    for trade in result["records"]["trades"]:
        assert trade["quantity"] == 100
        assert money(Decimal(trade["notional"]) * Decimal(".0003")) < 5
        assert Decimal(trade["commission"]) == 5
    assert Decimal(result["final_state"]["cash"]) >= 0


@pytest.mark.parametrize("n", [15, 10, 5])
def test_actual_calendar_holding_age_and_two_attempt_exit_cap_are_unchanged(n):
    data = market(changing=True)
    data["calendar"].remove("2025-01-08")  # Explicit synthetic omitted session, not a real-holiday claim.
    days = sessions(data)
    result = run(data, n)
    plans = result["records"]["turnover_plans"]
    assert [plan["session"] for plan in plans] == days[::5]
    assert len(plans[1]["minimum_hold_locked"]) == n
    assert not plans[1]["allowed_exits"]
    assert set(plans[1]["holding_age_sessions"].values()) == {5}
    assert plans[2]["exit_intent_count"] == 2
    assert set(plans[2]["holding_age_sessions"].values()) == {10}
    assert all(plan["minimum_hold_sessions"] == 10 and plan["max_replacements"] == 2 for plan in plans)
    assert all(plan["target_top_k"] == n and plan["keep_rank"] == 40 for plan in plans)
    assert all(plan["signal_date"] == data["calendar"][data["calendar"].index(plan["session"])-1] for plan in plans)


@pytest.mark.parametrize("n", [15, 10, 5])
def test_failed_exit_keeps_frozen_attempt_count_and_original_overflow_semantics(n):
    data = market(changing=True)
    day = sessions(data)[10]
    worst = f"SH{600000+n-1:06d}"
    mask = data["raw"].date.eq(day) & data["raw"].instrument.eq(worst)
    data["raw"].loc[mask, ["open", "high", "low", "close"]] = "9"
    result = run(data, n)
    plan = result["records"]["turnover_plans"][-1]
    assert plan["allowed_exits"][0] == worst
    assert plan["exit_intent_count"] == 2 and plan["actual_exit_fill_count"] == 1
    assert plan["rejected_exit_orders"] == 1 and plan["exit_intent_without_sell_fill"] == [worst]
    assert plan["rejected_exit_substitutions"] == 0
    assert plan["actual_active_count_after_execution"] == n + 1
    assert plan["actual_overflow_after_execution"] == 1
    assert worst in result["final_state"]["positions"]


def test_out_of_index_incumbent_keeps_its_locked_slot_until_actual_age_ten():
    data = market(changing=True)
    names = sorted(set(data["raw"].instrument))
    day = sessions(data)[5]
    cutoff = data["calendar"][data["calendar"].index(day)-1]
    data["pool"].members_on = lambda signal_day: names if signal_day < cutoff else names[1:]
    result = run(data, 5)
    plans = result["records"]["turnover_plans"]
    assert names[0] in plans[1]["minimum_hold_locked"] and names[0] in plans[1]["retained"]
    assert plans[2]["allowed_exits"][0] == names[0]
    assert plans[2]["holding_age_sessions"][names[0]] == 10


@pytest.mark.parametrize("n", DECLARED_COUNTS)
def test_missing_actual_origin_never_uses_position_bought_date_or_skips_minimum_hold(n, monkeypatch):
    class LostOriginTracker(turnover.HoldingAgeTracker):
        def after_trades(self, before, after, trades, day):
            super().after_trades(before, after, trades, day)
            self.holding_started.clear()  # Real fill but no trustworthy persisted acquisition record.
    monkeypatch.setattr(counts, "HoldingAgeTracker", LostOriginTracker)
    with pytest.raises(ValueError, match="Unknown actual first acquisition origin"):
        run(market(end_at=0), n)


@pytest.mark.parametrize("n", DECLARED_COUNTS)
def test_execution_day_gap_does_not_change_frozen_prior_close_selection_or_quantities(n):
    data = market(end_at=0)
    expected = run(deepcopy(data), n)
    day = sessions(data)[0]
    data["raw"].loc[data["raw"].date.eq(day), ["open", "high", "low", "close"]] = "12"
    result = run(data, n)
    assert result["records"]["plans"][0]["plan"] == expected["records"]["plans"][0]["plan"]
    assert not result["records"]["trades"]  # Source open12 exceeds prior-close10 upper limit11.
    assert len(result["records"]["orders"]) == n
    assert all(row["status"] == "rejected" for row in result["records"]["orders"])
    assert result["final_state"]["cash"] == "100000.00"


@pytest.mark.parametrize("n", DECLARED_COUNTS)
def test_delayed_bonus_keeps_real_record_quantity_and_original_holding_clock(n):
    data = market(end_at=7)
    days = sessions(data)
    inst = "SH600000"
    data["events"] = [dict(event_id="synthetic_count_bonus", instrument=inst,
        record_date=days[0], ex_date=days[1], pay_date=days[1], listing_date=days[4],
        known_at="2024-12-30T23:59:59+08:00", source_response_sha256="a"*64,
        cash_per_share="0", bonus_ratio=".1")]
    result = run(data, n)
    original_shares = result["final_state"]["holdings_history"][days[0]][inst]
    assert result["final_state"]["positions"][inst]["quantity"] == original_shares * 11 // 10
    listed = next(row for row in result["records"]["holding_age"] if row["session"] == days[4] and row["instrument"] == inst)
    assert listed["holding_started"] == days[0] and listed["holding_age_sessions"] == 4
    assert len([t for t in result["records"]["trades"] if t["instrument"] == inst]) == 1


@pytest.mark.parametrize("n", DECLARED_COUNTS)
def test_terminal_delisting_claim_remains_invalid_without_cash_or_successor_recovery(n):
    data = market(end_at=2)
    days = sessions(data)
    inst = "SH600000"
    data["identity_events"] = [dict(kind="delisting", instrument=inst, effective_date=days[-1],
        known_at=days[-2]+"T23:59:59+08:00", termination_known_at=days[-2]+"T23:59:59+08:00")]
    result = run(data, n)
    assert result["performance_valid"] is False
    assert result["records"]["valuation_audit"][-1]["unpriced_delisted_claims"] == [inst]
    assert result["final_state"]["positions"][inst]["quantity"] > 0
    assert result["metrics_basis"] == "invalid_indicative_carried_ledger_do_not_use_for_selection"


@pytest.mark.parametrize("n", [0, 1, 19, 21, -5, True, False, 20.0, "10", None])
def test_undeclared_count_is_rejected_before_context_or_source_access(n):
    with pytest.raises(ValueError, match="predeclared|position counts"):
        run({}, n)


@pytest.mark.parametrize("key,value", [("top_k", 10), ("keep_rank", 20), ("rebalance_sessions", 20),
    ("commission_rate", "0"), ("minimum_commission_cny", "0"), ("transfer_rate", "0"),
    ("adverse_slippage_bps", "0"), ("cash_dividend_tax_reserve_rate", "0"), ("initial_cash_cny", "200000")])
def test_other_frozen_execution_terms_cannot_change_in_a_count_arm(key, value):
    data = market(end_at=0)
    data["protocol"]["execution"][key] = value
    with pytest.raises(ValueError, match="all frozen execution"):
        run(data, 5)


def test_continuous_calendar_phase_does_not_reset_at_model_year_boundary():
    data = source_context()
    days = sessions(data, "continuous")
    result = run(data, 5, "continuous")
    assert [row["session"] for row in result["records"]["plans"]] == days[::5]
    assert "2026-01-02" not in days[::5]


def test_source_inputs_protocol_global_rules_and_production_files_remain_unchanged():
    data = market(changing=True)
    raw, basic = data["raw"].copy(deep=True), data["basic"].copy(deep=True)
    scores = data["scores"]["LGB"].copy(deep=True)
    protocol, calendar = deepcopy(data["protocol"]), list(data["calendar"])
    global_execution, variants = deepcopy(stability.EXECUTION), deepcopy(turnover.FIXED_VARIANTS)
    paths = [Path(m.__file__) for m in (stability, turnover, ex)]
    paths += [Path(counts.build_turnover_plan.__code__.co_filename)]
    before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    for n in DECLARED_COUNTS:
        run(data, n)
    pd.testing.assert_frame_equal(data["raw"], raw)
    pd.testing.assert_frame_equal(data["basic"], basic)
    pd.testing.assert_series_equal(data["scores"]["LGB"], scores)
    assert data["protocol"] == protocol and data["calendar"] == calendar
    assert stability.EXECUTION == global_execution and turnover.FIXED_VARIANTS == variants
    assert counts.HOLD_RULE == RULE
    assert {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths} == before
    # These are the frozen reader's established derived caches, not new inputs.
    assert set(data) - set(market()) <= {"score_coverage", "raw_day_locations"}
