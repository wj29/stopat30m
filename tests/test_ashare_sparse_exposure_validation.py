"""A13 integration evidence from explicit synthetic sessions, never history."""
from copy import deepcopy
from decimal import Decimal

import pandas as pd
import pytest

from stopat30m import ashare_sparse_exposure_validation as sparse
from stopat30m import ashare_exposure_matching_validation as matched
from stopat30m import ashare_capital_turnover_validation as capital
from stopat30m.backtest import ashare_execution as execution
from test_ashare_exposure_matching_validation import (
    market, references, add_known_distribution,
)


def observe_overlay(monkeypatch):
    from stopat30m import ashare_sparse_exposure_execution as overlay
    original, calls = overlay.execute_sparse_exposure_overlay, []

    def execute(state, **kwargs):
        before = deepcopy(state)
        result = original(state, **kwargs)
        calls.append({"before": before, "kwargs": kwargs, "result": result})
        return result

    monkeypatch.setattr(overlay, "execute_sparse_exposure_overlay", execute)
    return calls


@pytest.mark.parametrize("arm", sparse.CONTROL_ARMS)
@pytest.mark.parametrize("period", ["2025", "2026", "continuous"])
def test_all_nine_controls_reproduce_entire_A12_result(arm, period):
    data, days = market(length=7, partial_exit=True)
    targets = references(days)
    expected = matched.simulate_exposure_matching(deepcopy(data), period=period,
        arm_key=arm, reference_exposures=targets)
    actual = sparse.simulate_sparse_exposure(deepcopy(data), period=period,
        arm_key=arm, reference_exposures=targets)
    assert actual == expected


@pytest.mark.parametrize("arm", sparse.CONTROL_ARMS)
def test_control_delegate_precedes_all_new_validation_or_source_reads(monkeypatch, arm):
    context, targets, sentinel = object(), object(), object()
    observed = []
    def delegated(given, **kwargs):
        observed.append((given, kwargs))
        return sentinel
    monkeypatch.setattr(sparse, "simulate_exposure_matching", delegated)
    actual = sparse.simulate_sparse_exposure(context, period="unvalidated-control",
        arm_key=arm, reference_exposures=targets)
    assert actual is sentinel
    assert observed == [(context, {"period": "unvalidated-control", "arm_key": arm,
                                  "reference_exposures": targets})]


@pytest.mark.parametrize("arm", sparse.CANDIDATES)
def test_only_declared_actual_cadence_reads_prior_reference_and_executes(monkeypatch, arm):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=12, changing=False)
    cadence = sparse.CANDIDATES[arm]["cash_control_sessions"]
    minimum = sparse.CANDIDATES[arm]["minimum_order_notional_cny"]
    expected_indices = list(range(cadence, len(days), cadence))
    expected_priors = [days[i-1] for i in expected_indices]

    class ScheduledPriorOnly(dict):
        def __init__(self, value):
            super().__init__(value)
            self.reads = []
        def get(self, key, default=None):
            assert key in expected_priors, "Unscheduled or current/future target read"
            self.reads.append(key)
            return super().get(key, default)
        def __iter__(self):
            raise AssertionError("No full-period reference scan")

    targets = ScheduledPriorOnly(references(days, "0.65"))
    for n, day in enumerate(days):
        targets[day]["risky_nav_fraction"] = str(Decimal("0.40") + Decimal(n)/100)
    result = sparse.simulate_sparse_exposure(data, period="2025", arm_key=arm,
        reference_exposures=targets)
    assert targets.reads == expected_priors
    assert [c["kwargs"]["session"] for c in calls] == [days[i] for i in expected_indices]
    assert [c["kwargs"]["signal_date"] for c in calls] == expected_priors
    assert all(c["kwargs"]["minimum_order_notional_cny"] == minimum for c in calls)
    audits = result["records"]["exposure_matching"]
    assert len(audits) == len(days)
    assert sum(a["initial_build_skipped"] for a in audits) == 1
    for n, audit in enumerate(audits):
        assert audit["actual_session_index"] == n
        assert audit["cash_control_sessions"] == cadence
        assert audit["minimum_order_notional_cny"] == minimum
        assert audit["source_previous_date"] == data["calendar"][data["calendar"].index(days[n])-1]
        assert audit["cash_control_scheduled"] == (n > 0 and n % cadence == 0)
        if n == 0:
            assert audit["skip_reason"] == "first_scheduled_batch_no_overlay"
            assert audit["source_coverage"] is None and audit["target"] is None
        elif n % cadence:
            assert audit["source_reason"] == audit["skip_reason"] == "fixed_unscheduled_cash_control_session"
            assert audit["source_coverage"] is None and audit["target"] is None
        else:
            assert audit["source_coverage"] is True
            assert audit["target"] == targets[days[n-1]]["risky_nav_fraction"]


@pytest.mark.parametrize("arm", ["cadence3_min500", "cadence5_min1000"])
def test_continuous_cadence_and_core_phase_do_not_reset_for_year_or_calendar_gap(monkeypatch, arm):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=21, changing=False, cross_year=True)
    result = sparse.simulate_sparse_exposure(data, period="continuous", arm_key=arm,
        reference_exposures=references(days, "0.8"))
    cadence = sparse.CANDIDATES[arm]["cash_control_sessions"]
    assert [c["kwargs"]["session"] for c in calls] == days[cadence::cadence]
    assert [a["session"] for a in result["records"]["turnover_plans"]] == days[::5]
    assert sum(a["initial_build_skipped"] for a in result["records"]["exposure_matching"]) == 1
    assert any((pd.Timestamp(b)-pd.Timestamp(a)).days > 3 for a, b in zip(days, days[1:]))
    january = next(day for day in days if day.startswith("2026"))
    assert next(a for a in result["records"]["exposure_matching"] if a["session"] == january)["actual_session_index"] > 0


def test_initial_failed_build_does_not_delay_or_repeat_frequency_exemption(monkeypatch):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=7, changing=False, initial_unfilled=True)
    result = sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500",
        reference_exposures=references(days, "0.65"))
    assert not [t for t in result["records"]["trades"] if t["session"] == days[0]]
    assert [c["kwargs"]["session"] for c in calls] == [days[3], days[6]]
    assert sum(a["initial_build_skipped"] for a in result["records"]["exposure_matching"]) == 1


@pytest.mark.parametrize("bad", [None, True, "NaN", "-0.01", "1.01"])
def test_bad_scheduled_reference_is_unknown_and_not_carried(monkeypatch, bad):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=8, changing=False)
    targets = references(days, "0.65")
    targets[days[5]]["risky_nav_fraction"] = bad
    result = sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500",
        reference_exposures=targets)
    assert [c["kwargs"]["target_fraction"] for c in calls] == ["0.65", None]
    assert not calls[1]["result"].trades
    audit = result["records"]["exposure_matching"][6]
    assert audit["target"] is None and audit["source_reason"] == "invalid_previous_reference_exposure"


@pytest.mark.parametrize("mode,reason", [
    ("date", "previous_reference_date_mismatch"),
    ("missing", "missing_or_invalid_previous_reference_record"),
    ("coverage", "previous_reference_coverage_incomplete"),
])
def test_scheduled_reference_date_and_coverage_are_strict(monkeypatch, mode, reason):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=4, changing=False)
    targets = references(days)
    if mode == "date":
        targets[days[2]]["date"] = days[3]
    elif mode == "missing":
        del targets[days[2]]
    else:
        targets[days[2]]["coverage_complete"] = False
    result = sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500",
        reference_exposures=targets)
    assert len(calls) == 1 and calls[0]["kwargs"]["target_fraction"] is None
    assert not calls[0]["result"].trades
    assert result["records"]["exposure_matching"][3]["source_reason"] == reason


def test_future_or_unscheduled_targets_cannot_change_earlier_accounting():
    data, days = market(length=10, changing=False)
    targets = references(days, "0.65")
    changed = deepcopy(targets)
    changed[days[8]]["risky_nav_fraction"] = "0.10"  # first usable on day 9
    changed[days[0]]["risky_nav_fraction"] = "0.99"  # never scheduled target
    changed["2030-01-02"] = {"date": "2030-01-02", "coverage_complete": True, "risky_nav_fraction": "0"}
    first = sparse.simulate_sparse_exposure(deepcopy(data), period="2025", arm_key="cadence3_min1000", reference_exposures=targets)
    second = sparse.simulate_sparse_exposure(deepcopy(data), period="2025", arm_key="cadence3_min1000", reference_exposures=changed)
    for name in first["records"]:
        if name == "holding_origin_actions":
            continue
        def earlier(rows):
            return [r for r in rows if r.get("session", r.get("date", "")) < days[9]]
        assert earlier(first["records"][name]) == earlier(second["records"][name]), name


def test_today_high_low_close_cannot_change_same_open_sparse_orders():
    data, days = market(length=4, changing=False)
    changed = deepcopy(data)
    changed["raw"].loc[changed["raw"].date == days[3], ["high", "low", "close"]] = "100"
    targets = references(days, "0.65")
    first = sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500", reference_exposures=targets)
    second = sparse.simulate_sparse_exposure(changed, period="2025", arm_key="cadence3_min500", reference_exposures=targets)
    for name in ("trades", "orders"):
        assert [r for r in first["records"][name] if r["session"] == days[3]] == [r for r in second["records"][name] if r["session"] == days[3]]


@pytest.mark.parametrize("arm", sparse.CANDIDATES)
def test_purpose_costs_holding_origins_and_input_immutability(monkeypatch, arm):
    calls = observe_overlay(monkeypatch)
    data, days = market(length=7, changing=False)
    scores, protocol = data["scores"]["LGB"].copy(deep=True), deepcopy(data["protocol"])
    result = sparse.simulate_sparse_exposure(data, period="2025", arm_key=arm,
        reference_exposures=references(days, "0.45"))
    assert {t["order_purpose"] for t in result["records"]["trades"]} == {"core", "cash_control"}
    assert all(o["order_purpose"] in {"core", "cash_control"} for o in result["records"]["orders"])
    partials = []
    for call in calls:
        for trade in call["result"].trades:
            assert Decimal(trade["notional"]) >= Decimal(sparse.CANDIDATES[arm]["minimum_order_notional_cny"])
            if trade["side"] == "SELL" and trade["quantity"] < call["before"].positions[trade["instrument"]].quantity:
                partials.append((call["kwargs"]["session"], trade["instrument"]))
    assert partials
    for day, inst in partials:
        age = next(a for a in result["records"]["holding_age"] if a["session"] == day and a["instrument"] == inst)
        assert age["holding_started"] == days[0]
    for trade in result["records"]["trades"]:
        expected = execution.fees_for_order(trade["quantity"], trade["execution_price"], trade["side"])
        assert all(Decimal(trade[k]) == v for k, v in expected.items())
        assert Decimal(trade["cash_before"]) + Decimal(trade["cash_change"]) == Decimal(trade["cash_after"])
    assert Decimal(result["metrics"]["total_fee_cny"]) == sum(Decimal(t["total_fee"]) for t in result["records"]["trades"])
    assert Decimal(result["metrics"]["slippage_cost_cny"]) == sum(Decimal(t["slippage_cost"]) for t in result["records"]["trades"])
    pd.testing.assert_series_equal(data["scores"]["LGB"], scores)
    assert data["protocol"] == protocol


def test_unknown_targets_preserve_original_tax_bonus_and_accounting():
    data, days = market(length=8, changing=False)
    add_known_distribution(data, days)
    original = capital.simulate_capital_turnover(deepcopy(data), period="2025", fraction=None)
    actual = sparse.simulate_sparse_exposure(deepcopy(data), period="2025", arm_key="cadence3_min500", reference_exposures={})
    assert actual["final_state"] == original["final_state"]
    assert actual["metrics"] == original["metrics"]
    for name in original["records"]:
        got = actual["records"][name]
        if name in {"trades", "orders"}:
            got = [{k: v for k, v in row.items() if k != "order_purpose"} for row in got]
        assert got == original["records"][name], name
    assert Decimal(actual["metrics"]["recognized_distribution_tax_reserve_cny"]) > 0


def test_specific_open_NAV_error_stops_only_on_scheduled_date_without_fake_close(monkeypatch):
    from stopat30m import ashare_sparse_exposure_execution as overlay
    data, days = market(length=7, changing=False)
    def fail(*args, **kwargs):
        raise overlay.ExposureMatchingUnavailableNAVError("synthetic scheduled unavailable NAV")
    monkeypatch.setattr(overlay, "execute_sparse_exposure_overlay", fail)
    result = sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500", reference_exposures=references(days))
    assert result["performance_valid"] is False
    assert result["stopped"]["session"] == days[3]
    assert result["stopped"]["kind"] == "exposure_matching_unavailable_opening_NAV"
    assert result["actual_last_valued_session"] == days[2]
    assert len(result["records"]["curves"]) == 3
    assert result["records"]["exposure_matching"][-1]["cash_control_scheduled"] is True
    assert result["records"]["exposure_matching"][-1]["stopped"] is True
    assert result["metrics"]["declared_end_reached"] is False


def test_stop_after_core_preserves_actual_unclosed_core_fills_and_state(monkeypatch):
    from stopat30m import ashare_sparse_exposure_execution as overlay
    data, days = market(length=17, changing=False)
    targets = references(days, "0.8")
    targets[days[11]]["risky_nav_fraction"] = "0"  # day12 exits; day15 core rebuilds
    original = overlay.execute_sparse_exposure_overlay
    def fail_on_rebuild(state, **kwargs):
        if kwargs["session"] == days[15]:
            assert state.positions
            raise overlay.ExposureMatchingUnavailableNAVError("synthetic post-core NAV failure")
        return original(state, **kwargs)
    monkeypatch.setattr(overlay, "execute_sparse_exposure_overlay", fail_on_rebuild)
    result = sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500", reference_exposures=targets)
    final_core = [t for t in result["records"]["trades"] if t["session"] == days[15]]
    assert final_core and all(t["order_purpose"] == "core" and t["side"] == "BUY" for t in final_core)
    assert set(result["final_state"]["positions"]) == {t["instrument"] for t in final_core}
    assert result["actual_last_valued_session"] == days[14]
    assert not any(c["date"] == days[15] for c in result["records"]["curves"])
    assert result["performance_valid"] is False


def test_other_errors_propagate_without_masquerading_as_unknown_skip(monkeypatch):
    from stopat30m import ashare_sparse_exposure_execution as overlay
    data, days = market(length=4)
    def fail(*args, **kwargs):
        raise ValueError("synthetic real implementation error")
    monkeypatch.setattr(overlay, "execute_sparse_exposure_overlay", fail)
    with pytest.raises(ValueError, match="real implementation"):
        sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500", reference_exposures=references(days))


@pytest.mark.parametrize("arm", ["cadence1_min500", "cadence3_min0", "baseline", None, True])
def test_unfrozen_arm_rejected_before_source_read(arm):
    with pytest.raises(ValueError, match="predeclared"):
        sparse.simulate_sparse_exposure({}, period="2025", arm_key=arm, reference_exposures={})


def test_new_candidates_require_mapping_and_frozen_original_execution():
    with pytest.raises(ValueError, match="reference exposure"):
        sparse.simulate_sparse_exposure({}, period="2025", arm_key="cadence3_min500", reference_exposures=None)
    data, _ = market(length=4)
    data["protocol"]["execution"]["rebalance_sessions"] = 1
    with pytest.raises(ValueError, match="all frozen"):
        sparse.simulate_sparse_exposure(data, period="2025", arm_key="cadence3_min500", reference_exposures={})
