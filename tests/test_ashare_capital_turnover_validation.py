"""Small synthetic real-ledger integrations, never historical model returns."""
from copy import deepcopy
from decimal import Decimal, ROUND_FLOOR
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_capital_turnover_validation as capital
from stopat30m import ashare_stability_validation as stability
from stopat30m import ashare_turnover_validation as legacy
from stopat30m.backtest import ashare_execution as execution


NAMES = [f"SH{600000 + i:06d}" for i in range(60)]


def market(*, length=21, changing=True, initial_unfilled=False,
           partial_exit=False, refused_exit=False, cross_year=False):
    # An explicit synthetic session fixture, not a real exchange-calendar
    # assertion. Omitted dates make holding age depend on this calendar rather
    # than on elapsed calendar days or a fresh weekday reconstruction.
    calendar = pd.bdate_range("2024-08-01", "2026-03-31").strftime("%Y-%m-%d").tolist()
    calendar = [day for day in calendar if day not in {"2025-01-08", "2025-12-23", "2026-01-01", "2026-01-02"}]
    start = "2025-12-15" if cross_year else "2025-01-02"
    days = [day for day in calendar if day >= start][:length]
    source_days = calendar[calendar.index(days[0])-1:calendar.index(days[-1])+1]
    raw = pd.DataFrame([dict(date=day, instrument=inst, open="10", high="10.1", low="9.9",
        close="10", preclose="10", volume="10000", amount="100000", tradestatus="1", isST="0",
        source="explicit_synthetic_only", source_response_sha256="a"*64,
        source_complete_quote=True, source_observed_market_quote=True)
        for day in source_days for inst in NAMES])
    if partial_exit:
        mask = (raw.date >= days[1]) & (raw.instrument == NAMES[19])
        raw.loc[mask, ["open", "high", "low", "close", "preclose"]] = "20"
    if refused_exit:
        assert partial_exit
        mask = (raw.date == days[10]) & (raw.instrument == NAMES[19])
        raw.loc[mask, ["open", "high", "low", "close"]] = "18"  # original lower-limit refusal
    if initial_unfilled:
        raw.loc[raw.date == days[0], ["open", "high", "low", "close"]] = "11"
    score_rows = []
    for number, day in enumerate(source_days[:-1]):
        ranked = NAMES[20:] + NAMES[:20] if changing and number >= 5 else NAMES
        score_rows.extend((pd.Timestamp(day), inst, float(100-rank)) for rank, inst in enumerate(ranked))
    scores = pd.Series([row[2] for row in score_rows], index=pd.MultiIndex.from_tuples(
        [(row[0], row[1]) for row in score_rows], names=["datetime", "instrument"]))
    actions = SimpleNamespace(actions=[], excluded=lambda *a, **k: False,
        suspended=lambda *a: False, conversions=lambda *a: [])
    context = dict(calendar=calendar, raw=raw, basic=pd.DataFrame([
        dict(code="sh."+inst[2:], ipoDate="1999-11-10", outDate="") for inst in NAMES]),
        pool=SimpleNamespace(members_on=lambda day: NAMES), actions=actions, events=[],
        rights_events=[], identity_events=[], tax_contracts=[], scores={"LGB": scores},
        protocol={"periods": {period: [days[0], days[-1]] for period in ("2025", "2026", "continuous")},
                  "execution": deepcopy(stability.EXECUTION)})
    return context, days


def observe_wrapper(monkeypatch):
    from stopat30m import ashare_capital_turnover_execution as wrapper
    original, calls = wrapper.execute_capital_turnover_plan, []

    def execute(state, **kwargs):
        before = deepcopy(state)
        result = original(state, **kwargs)
        calls.append({"before": before, "kwargs": kwargs, "result": result})
        return result

    monkeypatch.setattr(wrapper, "execute_capital_turnover_plan", execute)
    return calls


def assert_actual_caps(result, fraction):
    curves = result["records"]["curves"]
    previous = {row["date"]: Decimal("100000") if i == 0 else Decimal(curves[i-1]["equity_cny"])
                for i, row in enumerate(curves)}
    guards = result["records"]["capital_turnover"]
    assert len(guards) == len(result["records"]["turnover_plans"])
    for index, guard in enumerate(guards):
        day = guard["session"]
        assert guard["initial_build"] is (index == 0)
        assert guard["cap_enforced"] is (index != 0)
        assert Decimal(guard["prior_nav_cny"]) == previous[day]
        assert guard["no_reallocation"] is True
        for side in ("BUY", "SELL"):
            total = sum((Decimal(row["notional"]) for row in result["records"]["trades"]
                         if row["session"] == day and row["side"] == side), Decimal(0))
            assert total == Decimal(guard[f"actual_{side.lower()}_notional_cny"])
            if index:
                budget = (previous[day] * Decimal(fraction)).quantize(Decimal(".01"), rounding=ROUND_FLOOR)
                assert Decimal(guard["budget_per_side_cny"]) == budget
                assert total <= budget


@pytest.mark.parametrize("period", ["2025", "2026", "continuous"])
def test_none_is_full_original_hold10_cap2_result_for_all_three_periods(period):
    data, _ = market(length=16, partial_exit=True)
    expected = legacy.simulate_turnover(deepcopy(data), period=period, variant=dict(capital.HOLD_RULE))
    actual = capital.simulate_capital_turnover(deepcopy(data), period=period, fraction=None)
    assert actual == expected  # every metric, final state, original record and invalid flag
    assert "capital_turnover" not in actual["records"]


@pytest.mark.parametrize("fraction", ["0.05", "0.075", "0.10"])
def test_every_actual_side_is_capped_after_initial_build_with_original_fill_fees(fraction):
    data, days = market(length=21, partial_exit=True)
    original_scores = data["scores"]["LGB"].copy(deep=True)
    protocol = deepcopy(data["protocol"])
    result = capital.simulate_capital_turnover(data, period="2025", fraction=fraction)
    assert_actual_caps(result, fraction)
    assert sum(Decimal(t["notional"]) for t in result["records"]["trades"]
               if t["session"] == days[0] and t["side"] == "BUY") > Decimal(100000) * Decimal(fraction)
    assert result["performance_valid"] is True
    pd.testing.assert_series_equal(data["scores"]["LGB"], original_scores)
    assert data["protocol"] == protocol
    for trade in result["records"]["trades"]:
        expected = execution.fees_for_order(int(trade["quantity"]), trade["execution_price"], trade["side"])
        assert all(Decimal(trade[key]) == value for key, value in expected.items())
        slippage = execution.money(Decimal(trade["quantity"]) * abs(
            Decimal(trade["execution_price"]) - Decimal(trade["reference_price"])))
        assert Decimal(trade["slippage_cost"]) == slippage
        assert Decimal(trade["cash_before"]) + Decimal(trade["cash_change"]) == Decimal(trade["cash_after"])


def test_partial_exit_keeps_actual_origin_and_does_not_add_a_holdings_hard_cap(monkeypatch):
    calls = observe_wrapper(monkeypatch)
    data, days = market(length=16, partial_exit=True)
    result = capital.simulate_capital_turnover(data, period="2025", fraction="0.05")
    assert [call["kwargs"]["initial_build"] for call in calls] == [True, False, False, False]
    plans = result["records"]["turnover_plans"]
    assert not plans[1]["allowed_exits"]
    assert set(plans[1]["holding_age_sessions"].values()) == {5}
    call = next(call for call in calls if call["kwargs"]["session"] == days[10])
    partial = [t for t in call["result"].trades if t["side"] == "SELL"
               and int(t["quantity"]) < call["before"].positions[t["instrument"]].quantity]
    assert partial, "The synthetic appreciated incumbent must exercise a real partial sale"
    for trade in partial:
        inst = trade["instrument"]
        assert call["result"].state.positions[inst].quantity > 0
        observed = next(row for row in result["records"]["holding_age"]
                        if row["session"] == days[10] and row["instrument"] == inst)
        assert observed["holding_started"] == days[0]
        assert observed["holding_age_sessions"] == 10
    assert plans[2]["actual_active_count_after_execution"] > 20
    assert plans[2]["rejected_exit_substitutions"] == 0
    assert all(row["exit_intent_count"] <= 2 for row in plans)
    assert_actual_caps(result, "0.05")


def test_limit_refusal_does_not_reallocate_frozen_exit_budget_or_substitute(monkeypatch):
    calls = observe_wrapper(monkeypatch)
    data, days = market(length=11, partial_exit=True, refused_exit=True)
    result = capital.simulate_capital_turnover(data, period="2025", fraction="0.05")
    call = calls[-1]
    assert call["kwargs"]["session"] == days[10]
    blocked = NAMES[19]
    assert blocked in call["kwargs"]["plan"].exits
    assert any(row["instrument"] == blocked and row["status"] == "rejected"
               for row in call["result"].orders)
    assert not any(row["instrument"] == blocked and row["side"] == "SELL"
                   for row in call["result"].trades)
    # The first frozen exit's reservation consumes the full5% budget; a failed
    # fill must not hand that reservation to the second or a replacement name.
    assert not any(row["side"] == "SELL" for row in call["result"].trades)
    assert result["records"]["turnover_plans"][-1]["rejected_exit_substitutions"] == 0
    assert result["records"]["turnover_plans"][-1]["actual_active_count_after_execution"] > 20
    assert_actual_caps(result, "0.05")


def test_zero_fill_first_batch_does_not_repeat_the_initial_exemption(monkeypatch):
    calls = observe_wrapper(monkeypatch)
    data, days = market(length=11, changing=False, initial_unfilled=True)
    result = capital.simulate_capital_turnover(data, period="2025", fraction="0.05")
    assert not [t for t in result["records"]["trades"] if t["session"] == days[0]]
    assert [call["kwargs"]["initial_build"] for call in calls] == [True, False, False]
    later = [t for t in result["records"]["trades"] if t["session"] == days[5]]
    assert later and sum(Decimal(t["notional"]) for t in later) <= 5000
    assert len(later) < 20  # being entirely cash does not grant a new full build
    assert_actual_caps(result, "0.05")


def add_known_distribution(data, days):
    data["events"] = [dict(event_id="synthetic_cash_and_bonus", instrument=NAMES[0],
        record_date=days[0], ex_date=days[1], pay_date=days[3], listing_date=days[4],
        known_at="2024-12-30T23:59:59+08:00", source_response_sha256="a"*64,
        cash_per_share=".1", bonus_ratio=".1")]
    mask = (data["raw"].date >= days[1]) & (data["raw"].instrument == NAMES[0])
    data["raw"].loc[mask, ["open", "high", "low", "close", "preclose"]] = "9"


@pytest.mark.parametrize("fraction", ["0.05", "0.075", "0.10"])
def test_nonbinding_cap_preserves_actual_tax_bonus_cash_and_every_financial_record(fraction):
    data, days = market(length=8, changing=False)
    add_known_distribution(data, days)
    original = legacy.simulate_turnover(deepcopy(data), period="2025", variant=dict(capital.HOLD_RULE))
    result = capital.simulate_capital_turnover(deepcopy(data), period="2025", fraction=fraction)
    assert result["final_state"] == original["final_state"]
    assert result["metrics"] == original["metrics"]
    for name in original["records"]:
        if name != "plans":  # only the new wrapper's execution audit is additive
            assert result["records"][name] == original["records"][name], name
    assert Decimal(result["metrics"]["recognized_distribution_tax_reserve_cny"]) > 0
    listed = next(row for row in result["records"]["holding_age"]
                  if row["session"] == days[4] and row["instrument"] == NAMES[0])
    assert listed["holding_started"] == days[0]


def test_unpriced_delisted_holding_stays_invalid_without_synthetic_recovery():
    data, days = market(length=14, changing=False)
    data["identity_events"] = [dict(kind="delisting", instrument=NAMES[0],
        effective_date=days[-1], known_at=days[-2]+"T23:59:59+08:00")]
    data["raw"] = data["raw"][~((data["raw"].date == days[-1]) & (data["raw"].instrument == NAMES[0]))].copy()
    original = legacy.simulate_turnover(deepcopy(data), period="2025", variant=dict(capital.HOLD_RULE))
    result = capital.simulate_capital_turnover(deepcopy(data), period="2025", fraction="0.05")
    assert result["performance_valid"] is original["performance_valid"] is False
    assert result["final_state"] == original["final_state"]
    assert result["records"]["valuation_audit"] == original["records"]["valuation_audit"]
    assert result["records"]["valuation_audit"][-1]["unpriced_delisted_claims"] == [NAMES[0]]
    assert result["records"]["curves"][-1]["date"] == days[-1]
    assert NAMES[0] in result["final_state"]["positions"]


def test_continuous_calendar_phase_and_only_one_exemption_survive_year_boundary(monkeypatch):
    calls = observe_wrapper(monkeypatch)
    data, days = market(length=21, cross_year=True, partial_exit=True)
    result = capital.simulate_capital_turnover(data, period="continuous", fraction="0.075")
    assert [call["kwargs"]["session"] for call in calls] == days[::5]
    assert [call["kwargs"]["initial_build"] for call in calls] == [True, False, False, False, False]
    first_january = next(day for day in days if day.startswith("2026"))
    january_rebalances = [call["kwargs"]["session"] for call in calls if call["kwargs"]["session"].startswith("2026")]
    assert january_rebalances and january_rebalances[0] != first_january
    assert all(row["signal_date"] == data["calendar"][data["calendar"].index(row["session"])-1]
               for row in result["records"]["turnover_plans"])
    assert_actual_caps(result, "0.075")


@pytest.mark.parametrize("fraction", [False, .05, Decimal(".05"), "0.025", "0.050"])
def test_undeclared_fraction_is_rejected_before_any_ledger_access(fraction):
    with pytest.raises(ValueError, match="predeclared"):
        capital.simulate_capital_turnover({}, period="2025", fraction=fraction)


def test_changing_other_frozen_execution_parameters_is_rejected():
    data, _ = market(length=3)
    data["protocol"]["execution"]["rebalance_sessions"] = 1
    with pytest.raises(ValueError, match="all frozen execution"):
        capital.simulate_capital_turnover(data, period="2025", fraction="0.05")


def test_fixed_arm_grid_matches_predeclared_fractions_exactly():
    assert capital.CAPITAL_ARMS == (("hold10_cap2", None), ("cap5pct", "0.05"),
                                   ("cap7p5pct", "0.075"), ("cap10pct", "0.10"))
