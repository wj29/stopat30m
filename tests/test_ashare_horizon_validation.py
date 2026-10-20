"""Synthetic routing, maturity and actual-ledger checks; no real model fits."""
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from stopat30m import ashare_horizon_validation as horizon
from stopat30m import ashare_stability_validation as stability


def source_context():
    calendar = pd.bdate_range("2025-08-01", "2026-02-10").strftime("%Y-%m-%d").tolist()
    # An explicit removed session exercises actual-session rather than weekday arithmetic.
    calendar.remove("2026-01-01")
    names = [f"SH{600000+number}" for number in range(22)]
    start = "2025-12-15"
    days = [day for day in calendar if day >= start]
    source_days = calendar[calendar.index(start)-1:]
    raw = pd.DataFrame([dict(date=day, instrument=inst, open="10", high="10.1", low="9.9",
        close="10", preclose="10", volume="100000", amount="1000000", tradestatus="1", isST="0",
        source="synthetic_source_fixture", source_response_sha256="a"*64,
        source_complete_quote=True, source_observed_market_quote=True) for day in source_days for inst in names])
    index = pd.MultiIndex.from_product([pd.to_datetime(source_days[:-1]), names], names=["datetime", "instrument"])
    scores = pd.Series([float(number) for _ in source_days[:-1] for number in range(len(names))], index=index)
    opening = pd.DataFrame({inst: [100+(.2+.03*n)*day+.001*(n%4)*day**2
        for day in range(len(calendar))] for n, inst in enumerate(names)}, index=calendar)
    actions = SimpleNamespace(actions=[], excluded=lambda *a, **k: False,
        suspended=lambda *a: False, conversions=lambda *a: [])
    return dict(calendar=calendar, raw=raw, basic=pd.DataFrame([
        dict(code="sh."+inst[2:], ipoDate="2000-01-01", outDate="") for inst in names]),
        pool=SimpleNamespace(members_on=lambda day: names), actions=actions, events=[],
        rights_events=[], identity_events=[], tax_contracts=[], normalized_open=opening,
        scores={"LGB": scores, "MOM60": scores.copy()}, score_coverage={"untouched": True},
        protocol={"periods": {"2025": [start, "2025-12-31"],
            "2026": ["2026-01-02", days[-1]], "continuous": [start, days[-1]]},
            "execution": deepcopy(stability.EXECUTION)})


def annual_predictions():
    rows = []
    for arm, offset in (("H5", 0), ("H20", 100)):
        for year in (2025, 2026):
            for day in ("2024-12-31", "2025-12-31", "2026-01-02"):
                rows.append(dict(model=arm, execution_year=year, datetime=day,
                                 instrument="SH600000", score=offset+year))
    return pd.DataFrame(rows)


def test_annual_router_preserves_arm_and_uses_next_actual_execution_year():
    calendar = ["2024-12-31", "2025-01-02", "2025-12-31", "2026-01-02", "2026-01-05"]
    frame = annual_predictions(); before = frame.copy(deep=True)
    h5 = horizon.horizon_execution_year_scores(frame, calendar, arm="H5")
    h20 = horizon.horizon_execution_year_scores(frame, calendar, arm="H20")
    assert h5.loc[(pd.Timestamp("2024-12-31"), "SH600000")] == 2025
    assert h5.loc[(pd.Timestamp("2025-12-31"), "SH600000")] == 2026
    assert h20.loc[(pd.Timestamp("2025-12-31"), "SH600000")] == 2126
    assert h20.index.equals(h5.index)
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("fault", ["old_model_name", "missing_arm", "duplicate", "nonfinite", "missing_year", "unobserved_date"])
def test_router_rejects_fake_package_identity_duplicate_or_unknown_sources(fault):
    table = annual_predictions()
    if fault == "old_model_name":
        table.loc[table.model.eq("H5"), "model"] = "LGB"
    elif fault == "missing_arm":
        table = table.loc[table.model.eq("H5")]
    elif fault == "duplicate":
        table = pd.concat([table, table.iloc[[0]]], ignore_index=True)
    elif fault == "nonfinite":
        table["score"] = table["score"].astype(float)
        table.loc[0, "score"] = np.inf
    elif fault == "missing_year":
        table = table.loc[~(table.model.eq("H20") & table.execution_year.eq(2026))]
    else:
        table.loc[0, "datetime"] = "2025-01-01"
    with pytest.raises(ValueError):
        horizon.horizon_execution_year_scores(table,
            ["2024-12-31", "2025-01-02", "2025-12-31", "2026-01-02", "2026-01-05"], arm="H5")


def test_preparation_only_changes_cadence_and_isolates_scores_and_coverage():
    data = source_context(); protocol = deepcopy(data["protocol"]); original = data["scores"]["LGB"].copy()
    prepared = horizon.prepare_horizon_context(data, original, arm="H20")
    assert prepared["protocol"]["execution"] == {**protocol["execution"], "rebalance_sessions": 20}
    assert prepared["protocol"]["periods"] == protocol["periods"]
    assert "score_coverage" not in prepared
    assert prepared["raw"] is data["raw"] and prepared["actions"] is data["actions"]
    prepared["scores"]["LGB"].iloc[0] = 999
    assert data["protocol"] == protocol and data["score_coverage"] == {"untouched": True}
    pd.testing.assert_series_equal(data["scores"]["LGB"], original)
    assert prepared["horizon_validation"]["arm"] == "H20"
    assert prepared["horizon_validation"]["a6_minimum_hold_or_exit_caps_added"] is False


@pytest.mark.parametrize("key,value", [("top_k", 10), ("keep_rank", 20), ("initial_cash_cny", "200000"),
                                     ("minimum_commission_cny", "0"), ("adverse_slippage_bps", "0"),
                                     ("rebalance_sessions", 20)])
def test_modified_old_execution_assumptions_cannot_enter_arm_context(key, value):
    data = source_context(); data["protocol"]["execution"][key] = value
    with pytest.raises(ValueError):
        horizon.prepare_horizon_context(data, data["scores"]["LGB"], arm="H5")


@pytest.mark.parametrize("arm,h", [("H5", 5), ("H20", 20)])
def test_each_arm_quality_uses_its_own_mature_open_label(arm, h):
    data = source_context()
    prepared = horizon.prepare_horizon_context(data, data["scores"]["LGB"], arm=arm)
    quality = horizon.horizon_score_quality(prepared, period="continuous", arm=arm)
    calendar = data["calendar"]; first = calendar.index(data["protocol"]["periods"]["continuous"][0])-1
    assert quality["daily"][0]["signal_date"] == calendar[first]
    assert quality["daily"][0]["label_end"] == calendar[first+h+1]
    assert quality["summary"]["label"] == f"Ref($open,-{h+1})/Ref($open,-1)-1"
    assert quality["summary"]["excluded"]["immature_signal_days"] == h
    assert quality["daily"][-1]["label_end"] == calendar[-1]
    assert quality["summary"]["prediction_arm"] == arm
    assert quality["summary"]["future_membership_filter_used"] is False


def test_quality_cannot_silently_use_the_other_arms_label():
    data = source_context()
    prepared = horizon.prepare_horizon_context(data, data["scores"]["LGB"], arm="H20")
    with pytest.raises(ValueError):
        horizon.horizon_score_quality(prepared, period="continuous", arm="H5")


def test_actual_ledger_uses_twenty_actual_sessions_across_year_without_phase_reset():
    data = source_context(); original_protocol = deepcopy(data["protocol"])
    result = horizon.simulate_horizon(data, period="continuous", arm="H20", scores=data["scores"]["LGB"])
    days = [day for day in data["calendar"] if day >= original_protocol["periods"]["continuous"][0]]
    assert [row["session"] for row in result["records"]["plans"]] == days[::20]
    assert "2026-01-02" not in days[::20]
    assert result["execution_rebalance_actual_sessions"] == 20
    assert result["arm"] == "H20" and result["label_horizon_actual_sessions"] == 20
    assert result["performance_valid"] is True
    assert data["protocol"] == original_protocol
    assert data["score_coverage"] == {"untouched": True}
    assert all(Decimal(row["commission"]) >= 5 for row in result["records"]["trades"])


def test_wrapper_exactly_reuses_original_fee_lot_and_corporate_accounting():
    data = source_context()
    prepared = horizon.prepare_horizon_context(data, data["scores"]["LGB"], arm="H5")
    direct = stability.simulate_stability(prepared, period="2026", model="LGB")
    wrapped = horizon.simulate_horizon(data, period="2026", arm="H5", scores=data["scores"]["LGB"])
    assert wrapped["records"] == direct["records"]
    assert wrapped["final_state"] == direct["final_state"] and wrapped["metrics"] == direct["metrics"]
    assert wrapped["metrics_basis"] == direct["metrics_basis"]
    assert wrapped["strategy_promoted"] is False and wrapped["active_paper_or_broker_changed"] is False
    days = [day for day in data["calendar"] if day >= "2026-01-02"]
    assert [row["session"] for row in wrapped["records"]["plans"]] == days[::20]


def test_required_prior_signal_scores_are_not_silently_forward_filled():
    data = source_context()
    signal = data["calendar"][data["calendar"].index("2025-12-15")-1]
    incomplete = data["scores"]["LGB"].drop(index=pd.Timestamp(signal), level="datetime")
    with pytest.raises(ValueError):
        horizon.simulate_horizon(data, period="continuous", arm="H5", scores=incomplete)


def test_actual_distribution_tax_and_bonus_ledger_remains_identical(tmp_path):
    from stopat30m.data.a5_distribution_tax import prepare_distribution_tax
    import hashlib

    data = source_context(); inst = data["raw"].instrument.iloc[-1]
    record = data["protocol"]["periods"]["continuous"][0]
    exday = data["calendar"][data["calendar"].index(record)+1]
    event = dict(event_id="synthetic_capital", instrument=inst, record_date=record,
        ex_date=exday, pay_date=exday, listing_date=exday, known_at="2025-12-10T23:59:59+08:00",
        source_response_sha256="a"*64, cash_per_share="0", stock_dividend_ratio="0",
        capital_reserve_ratio=".1", bonus_ratio=".1")
    source = tmp_path/"explicit-synthetic-tax-evidence.pdf"
    source.write_bytes(b"Synthetic accounting fixture, not an issuer announcement")
    terms = ("instrument", "record_date", "ex_date", "cash_per_share", "stock_dividend_ratio", "capital_reserve_ratio")
    fact = {key: event[key] for key in terms}
    fact.update(status="verified_face_value_source_classification_unresolved", review_required=False,
        original_pdf_visual_reviewed=True, evidence=[dict(path=str(source),
            sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
            url="https://static.cninfo.com.cn/synthetic-unit-test/tax.pdf", pdf_pages_1based=[1])],
        share_face_value_cny="1", capital_reserve_face_value_per_record_share=".1",
        capital_reserve_tax_classification="unresolved_conservative_upper_bound", classification_verified=False,
        conservative_tax_reserve_upper_bound=True, capital_reserve_tax_exempt=False, capital_reserve_taxable=False)
    events, report = prepare_distribution_tax([event], {"events": [fact]}, {"events": []})
    assert report["status"] == "passed"
    data["events"], data["tax_contracts"] = events, report["contracts"]
    prepared = horizon.prepare_horizon_context(data, data["scores"]["LGB"], arm="H20")
    direct = stability.simulate_stability(prepared, period="continuous", model="LGB")
    wrapped = horizon.simulate_horizon(data, period="continuous", arm="H20", scores=data["scores"]["LGB"])
    assert wrapped["records"] == direct["records"]
    assert wrapped["final_state"] == direct["final_state"] and wrapped["metrics"] == direct["metrics"]
    held = wrapped["final_state"]["holdings_history"][record][inst]
    assert held > 0
    assert Decimal(wrapped["metrics"]["additional_distribution_tax_reserved_cny"]) == held * Decimal(".02")
    extra = [row for row in wrapped["records"]["corporate_actions"] if row["kind"] == "additional_distribution_tax_reserve"]
    assert len(extra) == 1 and extra[0]["session"] == exday


def test_invalid_underlying_ledger_cannot_be_reclassified_by_new_horizon(monkeypatch):
    data = source_context()
    expected = dict(performance_valid=False, metrics_basis="invalid_indicative_carried_ledger_do_not_use_for_selection",
                    metrics={"terminal_prices_fresh": False}, records={"claims": [{"instrument": "SH601059"}]},
                    final_state={"original_held_shares": 300})
    monkeypatch.setattr(stability, "simulate_stability", lambda *a, **k: deepcopy(expected))
    result = horizon.simulate_horizon(data, period="continuous", arm="H20", scores=data["scores"]["LGB"])
    assert result["performance_valid"] is False
    assert result["metrics"] == expected["metrics"] and result["final_state"] == expected["final_state"]
    assert result["records"] == expected["records"]


@pytest.mark.parametrize("arm", ["H10", "LGB", "best_hold10_cap2"])
def test_unregistered_arms_are_rejected(arm):
    data = source_context()
    with pytest.raises(ValueError):
        horizon.simulate_horizon(data, period="continuous", arm=arm, scores=data["scores"]["LGB"])
