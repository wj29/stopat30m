"""A12 synthetic integrations only: no frozen model or historical replay."""
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_exposure_matching_validation as matched
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


def add_known_distribution(data, days):
    data["events"] = [dict(event_id="synthetic_cash_and_bonus", instrument=NAMES[0],
        record_date=days[0], ex_date=days[1], pay_date=days[3], listing_date=days[4],
        known_at="2024-12-30T23:59:59+08:00", source_response_sha256="a"*64,
        cash_per_share=".1", bonus_ratio=".1")]
    mask = (data["raw"].date >= days[1]) & (data["raw"].instrument == NAMES[0])
    data["raw"].loc[mask, ["open", "high", "low", "close", "preclose"]] = "9"



def references(days, fraction="0.65", *, covered=True):
    return {day: {"date": day, "coverage_complete": covered, "risky_nav_fraction": fraction} for day in days}


def observe_overlay(monkeypatch):
    from stopat30m import ashare_exposure_matching_execution as overlay
    original, calls = overlay.execute_exposure_overlay, []

    def execute(state, **kwargs):
        before = deepcopy(state)
        result = original(state, **kwargs)
        calls.append({"before": before, "kwargs": kwargs, "result": result})
        return result

    monkeypatch.setattr(overlay, "execute_exposure_overlay", execute)
    return calls


@pytest.mark.parametrize("arm,fraction", [("hold10_cap2", None), ("cap7p5pct", "0.075")])
@pytest.mark.parametrize("period", ["2025", "2026", "continuous"])
def test_six_controls_are_exact_entire_original_results_without_new_metadata(arm, fraction, period):
    data, _ = market(length=16, partial_exit=True)
    expected = capital.simulate_capital_turnover(deepcopy(data), period=period, fraction=fraction)
    actual = matched.simulate_exposure_matching(deepcopy(data), period=period,
        arm_key=arm, reference_exposures=None)
    assert actual == expected
    assert "exposure_matching" not in actual["records"]
    assert all("order_purpose" not in t for t in actual["records"]["trades"])


def test_daily_overlay_uses_only_previous_actual_session_and_never_first_day(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=8, changing=False)
    targets = references(days, "0.65")
    targets[days[0]]["risky_nav_fraction"] = "0.45"
    targets[days[1]]["risky_nav_fraction"] = "0.55"
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    assert [c["kwargs"]["session"] for c in calls] == days[1:]
    assert [c["kwargs"]["signal_date"] for c in calls] == days[:-1]
    assert [c["kwargs"]["target_fraction"] for c in calls[:2]] == ["0.45", "0.55"]
    audits = result["records"]["exposure_matching"]
    assert len(audits) == len(days)
    assert audits[0]["initial_build_skipped"] is True and audits[0]["target"] is None
    for day, audit in zip(days[1:], audits[1:]):
        prior = data["calendar"][data["calendar"].index(day)-1]
        assert audit["source_previous_date"] == prior
        assert audit["target"] == targets[prior]["risky_nav_fraction"]
        assert audit["source_coverage"] is True


def test_reference_mapping_is_not_scanned_or_read_on_first_batch():
    data, days = market(length=4, changing=False)

    class PriorOnly(dict):
        reads = []

        def get(self, key, default=None):
            assert key in days[:-1]  # excludes initial prior-period day and final/future target
            self.reads.append(key)
            return super().get(key, default)

        def __iter__(self):
            raise AssertionError("Reference targets must not be scanned for a whole-period statistic")

    targets = PriorOnly(references(days))
    matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    assert targets.reads == days[:-1]


@pytest.mark.parametrize("value", [None, True, "NaN", "Infinity", "-0.01", "1.01"])
def test_bad_or_missing_previous_target_is_unknown_not_clipped_or_carried(value, monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=3, changing=False)
    targets = references(days, "0.45")
    targets[days[1]] = {"date": days[1], "coverage_complete": True, "risky_nav_fraction": value}
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    assert calls[0]["kwargs"]["target_fraction"] == "0.45"
    assert calls[1]["kwargs"]["target_fraction"] is None
    assert not calls[1]["result"].trades
    audit = result["records"]["exposure_matching"][-1]
    assert audit["target"] is None
    assert audit["source_reason"] == "invalid_previous_reference_exposure"


def test_incomplete_source_and_missing_session_skip_without_a_carried_target(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=4, changing=False)
    targets = references(days, "0.45")
    targets[days[1]]["coverage_complete"] = False
    del targets[days[2]]
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    assert [c["kwargs"]["target_fraction"] for c in calls] == ["0.45", None, None]
    assert not calls[1]["result"].trades and not calls[2]["result"].trades
    assert [a["source_coverage"] for a in result["records"]["exposure_matching"][1:]] == [True, False, False]


def test_missing_row_date_is_unknown_even_when_mapping_key_is_prior_session(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=2, changing=False)
    targets = references(days, "0.25")
    del targets[days[0]]["date"]
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    assert calls[0]["kwargs"]["target_fraction"] is None
    assert not calls[0]["result"].trades
    audit = result["records"]["exposure_matching"][-1]
    assert audit["source_coverage"] is False and audit["target"] is None
    assert audit["source_reason"] == "previous_reference_date_mismatch"


def test_future_row_misfiled_under_previous_key_cannot_supply_a_target(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=2, changing=False)
    targets = references(days, "0.25")
    targets[days[0]]["date"] = days[1]
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    assert calls[0]["kwargs"]["target_fraction"] is None
    assert not calls[0]["result"].trades
    audit = result["records"]["exposure_matching"][-1]
    assert audit["source_previous_date"] == days[0]
    assert audit["source_coverage"] is False and audit["target"] is None
    assert audit["source_reason"] == "previous_reference_date_mismatch"


def test_changes_to_future_reference_cannot_change_earlier_fills_or_curves():
    data, days = market(length=8, changing=False)
    a = references(days, "0.65")
    b = deepcopy(a)
    b[days[-2]]["risky_nav_fraction"] = "0.25"  # first usable only on the last session
    b["2030-01-02"] = {"coverage_complete": True, "risky_nav_fraction": "0"}
    first = matched.simulate_exposure_matching(deepcopy(data), period="2025", arm_key="matched_original", reference_exposures=a)
    second = matched.simulate_exposure_matching(deepcopy(data), period="2025", arm_key="matched_original", reference_exposures=b)
    for name in first["records"]:
        if name == "holding_origin_actions":
            continue
        def earlier(rows):
            return [r for r in rows if r.get("session", r.get("date", "")) < days[-1]]
        assert earlier(first["records"][name]) == earlier(second["records"][name]), name
    assert first["records"]["curves"][:-1] == second["records"]["curves"][:-1]
    unchanged = deepcopy(a)
    unchanged["2030-01-02"] = {"coverage_complete": True, "risky_nav_fraction": "0.01"}
    third = matched.simulate_exposure_matching(deepcopy(data), period="2025", arm_key="matched_original", reference_exposures=unchanged)
    assert third == first


def test_current_high_low_close_cannot_change_same_open_control_orders():
    first_data, days = market(length=3, changing=False)
    second_data = deepcopy(first_data)
    mask = second_data["raw"].date == days[1]
    second_data["raw"].loc[mask, ["high", "low", "close"]] = "100"
    targets = references(days, "0.65")
    first = matched.simulate_exposure_matching(first_data, period="2025", arm_key="matched_original", reference_exposures=targets)
    second = matched.simulate_exposure_matching(second_data, period="2025", arm_key="matched_original", reference_exposures=targets)
    for name in ("trades", "orders"):
        assert [r for r in first["records"][name] if r["session"] == days[1]] == [
            r for r in second["records"][name] if r["session"] == days[1]]


def test_cash_control_partial_sales_keep_actual_origins_and_charge_real_order_fees(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=7, changing=False)
    scores = data["scores"]["LGB"].copy(deep=True)
    protocol = deepcopy(data["protocol"])
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=references(days))
    assert {t["order_purpose"] for t in result["records"]["trades"]} == {"core", "cash_control"}
    assert all(o["order_purpose"] in {"core", "cash_control"} for o in result["records"]["orders"])
    partials = []
    for call in calls:
        for trade in call["result"].trades:
            if trade["side"] == "SELL" and trade["quantity"] < call["before"].positions[trade["instrument"]].quantity:
                partials.append((call["kwargs"]["session"], trade["instrument"]))
    assert partials
    for day, inst in partials:
        age = next(a for a in result["records"]["holding_age"] if a["session"] == day and a["instrument"] == inst)
        assert age["holding_started"] == days[0]
        assert age["holding_age_sessions"] == data["calendar"].index(day)-data["calendar"].index(days[0])
    # Controller may trim before h10, while the original core still locks h5.
    assert min(data["calendar"].index(day)-data["calendar"].index(days[0]) for day, _ in partials) < 10
    assert not result["records"]["turnover_plans"][1]["allowed_exits"]
    for trade in result["records"]["trades"]:
        fee = execution.fees_for_order(trade["quantity"], trade["execution_price"], trade["side"])
        assert all(Decimal(trade[k]) == v for k, v in fee.items())
        assert Decimal(trade["cash_before"]) + Decimal(trade["cash_change"]) == Decimal(trade["cash_after"])
    assert Decimal(result["metrics"]["total_fee_cny"]) == sum(Decimal(t["total_fee"]) for t in result["records"]["trades"])
    assert Decimal(result["metrics"]["slippage_cost_cny"]) == sum(Decimal(t["slippage_cost"]) for t in result["records"]["trades"])
    pd.testing.assert_series_equal(data["scores"]["LGB"], scores)
    assert data["protocol"] == protocol


def test_same_day_core_buy_cannot_be_sold_by_controller_T1(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=7, changing=False)
    targets = references(days, "0.8")
    targets[days[3]]["risky_nav_fraction"] = "0"  # day4 clears original positions
    targets[days[4]]["risky_nav_fraction"] = "0"  # day5 core rebuilds, controller must obey T1
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    assert not any(p["session"] == days[4] for p in result["records"]["positions"])
    buys = [t for t in result["records"]["trades"] if t["session"] == days[5] and t["side"] == "BUY" and t["order_purpose"] == "core"]
    assert buys
    assert not [t for t in result["records"]["trades"] if t["session"] == days[5] and t["side"] == "SELL" and t["order_purpose"] == "cash_control"]
    positions = [p for p in result["records"]["positions"] if p["session"] == days[5]]
    assert positions and all(p["available_quantity"] == 0 for p in positions)
    for pos in positions:
        a = next(a for a in result["records"]["holding_age"] if a["session"] == days[5] and a["instrument"] == pos["instrument"])
        assert a["holding_started"] == days[5] and a["holding_age_sessions"] == 0


def test_unknown_overlay_keeps_original_cash_tax_bonus_and_unmodified_control_accounting():
    data, days = market(length=8, changing=False)
    add_known_distribution(data, days)
    original = capital.simulate_capital_turnover(deepcopy(data), period="2025", fraction=None)
    actual = matched.simulate_exposure_matching(deepcopy(data), period="2025", arm_key="matched_original", reference_exposures={})
    assert actual["final_state"] == original["final_state"]
    assert actual["metrics"] == original["metrics"]
    for name in original["records"]:
        got = actual["records"][name]
        if name in {"trades", "orders"}:
            got = [{k: v for k, v in row.items() if k != "order_purpose"} for row in got]
        assert got == original["records"][name], name
    assert Decimal(actual["metrics"]["recognized_distribution_tax_reserve_cny"]) > 0


def test_only_specific_unavailable_NAV_error_stops_path_without_invented_close(monkeypatch):
    from stopat30m import ashare_exposure_matching_execution as overlay
    data, days = market(length=3, changing=False)
    def fail(*args, **kwargs):
        raise overlay.ExposureMatchingUnavailableNAVError("explicit synthetic unavailable opening NAV")
    monkeypatch.setattr(overlay, "execute_exposure_overlay", fail)
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=references(days))
    assert result["performance_valid"] is False
    assert result["stopped"]["session"] == days[1]
    assert result["stopped"]["kind"] == "exposure_matching_unavailable_opening_NAV"
    assert result["actual_last_valued_session"] == days[0]
    assert len(result["records"]["curves"]) == 1
    assert result["records"]["exposure_matching"][-1]["stopped"] is True
    assert result["metrics"]["declared_end_reached"] is False


def test_stop_after_scheduled_core_preserves_actual_core_fills_without_fake_NAV(monkeypatch):
    from stopat30m import ashare_exposure_matching_execution as overlay
    data, days = market(length=7, changing=False)
    targets = references(days, "0.8")
    targets[days[3]]["risky_nav_fraction"] = "0"  # close day4 all cash, next core rebuilds
    original = overlay.execute_exposure_overlay

    def fail_on_rebuild(state, **kwargs):
        if kwargs["session"] == days[5]:
            assert state.positions
            raise overlay.ExposureMatchingUnavailableNAVError("synthetic post-core NAV failure")
        return original(state, **kwargs)

    monkeypatch.setattr(overlay, "execute_exposure_overlay", fail_on_rebuild)
    result = matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=targets)
    final_core = [t for t in result["records"]["trades"] if t["session"] == days[5]]
    assert final_core and all(t["order_purpose"] == "core" and t["side"] == "BUY" for t in final_core)
    assert set(result["final_state"]["positions"]) == {t["instrument"] for t in final_core}
    assert result["actual_last_valued_session"] == days[4]
    assert not any(c["date"] == days[5] for c in result["records"]["curves"])
    assert result["performance_valid"] is False
    assert result["stopped"]["session"] == days[5]


def test_other_programming_or_source_errors_are_not_silently_skipped(monkeypatch):
    from stopat30m import ashare_exposure_matching_execution as overlay
    data, days = market(length=3)
    def fail(*args, **kwargs):
        raise ValueError("real implementation error")
    monkeypatch.setattr(overlay, "execute_exposure_overlay", fail)
    with pytest.raises(ValueError, match="real implementation"):
        matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures=references(days))


def test_continuous_core_phase_and_daily_control_do_not_reset_at_new_year(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=21, cross_year=True, changing=False)
    result = matched.simulate_exposure_matching(data, period="continuous", arm_key="matched_original", reference_exposures=references(days, "0.8"))
    assert [a["session"] for a in result["records"]["turnover_plans"]] == days[::5]
    assert [c["kwargs"]["session"] for c in calls] == days[1:]
    assert sum(a["initial_build_skipped"] for a in result["records"]["exposure_matching"]) == 1
    january = next(day for day in days if day.startswith("2026"))
    assert january not in [a["session"] for a in result["records"]["turnover_plans"]]


@pytest.mark.parametrize("arm", ["matched_cap", "baseline", None, True])
def test_undeclared_arm_is_rejected_without_reading_sources(arm):
    with pytest.raises(ValueError, match="predeclared"):
        matched.simulate_exposure_matching({}, period="2025", arm_key=arm, reference_exposures={})


def test_matching_requires_explicit_reference_mapping_and_original_execution():
    with pytest.raises(ValueError, match="reference exposure"):
        matched.simulate_exposure_matching({}, period="2025", arm_key="matched_original", reference_exposures=None)
    data, _ = market(length=3)
    data["protocol"]["execution"]["rebalance_sessions"] = 1
    with pytest.raises(ValueError, match="all frozen"):
        matched.simulate_exposure_matching(data, period="2025", arm_key="matched_original", reference_exposures={})
