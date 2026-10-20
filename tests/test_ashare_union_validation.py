"""Chronology and raw-cash checks for the new fixed two-model comparison."""
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_union_validation as uv

A, B = "SH600000", "SZ000001"


def context(*, start="2023-01-03", end="2023-01-05"):
    calendar = pd.bdate_range(pd.Timestamp(start) - pd.Timedelta(days=150), end).strftime("%Y-%m-%d").tolist()
    days = [day for day in calendar if day >= start]
    source_days = calendar[calendar.index(days[0])-1:]
    rows = [{"date": day, "instrument": inst, "open": "10.00", "high": "10.10",
        "low": "9.90", "close": "10.00", "preclose": "10.00", "volume": "10000",
        "amount": "100000", "tradestatus": "1", "isST": "0", "source": "synthetic_fixture",
        "source_response_sha256": "a" * 64, "source_complete_quote": True,
        "source_observed_market_quote": True} for day in source_days for inst in (A, B)]
    scores = pd.Series([2 if inst == A else 1 for day in source_days[:-1] for inst in (A, B)],
        index=pd.MultiIndex.from_tuples([(pd.Timestamp(day), inst) for day in source_days[:-1]
                                        for inst in (A, B)], names=["datetime", "instrument"]))
    actions = SimpleNamespace(actions=[], excluded=lambda *args, **kwargs: False,
        suspended=lambda *args: False, conversions=lambda *args: [])
    return {"calendar": calendar, "raw": pd.DataFrame(rows), "basic": pd.DataFrame([
        {"code": "sh.600000", "ipoDate": "1999-11-10", "outDate": ""},
        {"code": "sz.000001", "ipoDate": "1991-04-03", "outDate": ""}]),
        "pool": SimpleNamespace(members_on=lambda day: [A, B]), "actions": actions,
        "events": [], "rights_events": [], "identity_events": [],
        "scores": {"MOM60": scores, "LGB": scores.copy()},
        # Deliberately small synthetic fixture; the production loader requires
        # the declared twenty slots, forty-rank buffer and five-session cycle.
        "protocol": {"periods": {"test": [start, end]},
            "execution": {**uv.EXECUTION, "top_k": 1, "keep_rank": 1, "rebalance_sessions": 1}}}


@pytest.mark.parametrize("day,stamp", [("2023-08-25", ".001"),
    ("2023-08-28", ".0005"), ("2025-01-02", ".0005"), ("2026-10-16", ".0005")])
def test_session_dispatch_preserves_reviewed_ledgers_and_historical_costs(day, stamp):
    policy = uv.session_policy(day)
    assert policy.sell_stamp_rate == Decimal(stamp)
    assert policy.minimum_commission == Decimal(5)
    assert policy.transfer_rate == Decimal(".00001")
    assert policy.slippage_bps == Decimal(2)


def test_prior_december_close_uses_next_actual_execution_year_model():
    predictions = pd.DataFrame([{"model": "LGB", "execution_year": year,
        "datetime": "2023-12-29", "instrument": A, "score": score}
        for year, score in ((2023, 999), (2024, 2))])
    scores = uv.execution_year_scores(predictions, ["2023-12-28", "2023-12-29", "2024-01-02"])
    assert scores.loc[(pd.Timestamp("2023-12-29"), A)] == 2


def test_duplicate_prediction_fold_is_rejected_instead_of_selecting_first():
    predictions = pd.DataFrame([{"model": "LGB", "execution_year": 2023,
        "datetime": "2022-12-30", "instrument": A, "score": 1}] * 2)
    with pytest.raises(ValueError, match="Duplicate"):
        uv.execution_year_scores(predictions, ["2022-12-30", "2023-01-03"])


def test_fixed_runner_has_real_cash_trade_fees_and_initial_transaction_drawdown():
    result = uv.simulate_union(context(), period="test", model="MOM60")
    assert result["performance_valid"]
    assert result["metrics"]["trade_count"] == 1
    trade = result["records"]["trades"][0]
    assert trade["side"] == "BUY" and trade["instrument"] == A
    assert int(trade["quantity"]) % 100 == 0
    assert Decimal(trade["commission"]) >= Decimal(5)
    assert Decimal(trade["slippage_cost"]) > 0
    assert Decimal(result["metrics"]["total_return_lower_exact"]) < 0
    assert Decimal(result["metrics"]["max_drawdown_conservative_exact"]) < 0
    assert len(result["records"]["plans"]) == 3


def test_tomorrow_suspension_does_not_replace_a_frozen_first_rank():
    data = context()
    selection_day = data["protocol"]["periods"]["test"][0]
    mask = data["raw"].date.eq(selection_day) & data["raw"].instrument.eq(A)
    data["raw"].loc[mask, ["tradestatus", "volume", "amount"]] = ["0", "0", "0"]
    data["raw"].loc[mask, ["source_complete_quote", "source_observed_market_quote"]] = False
    data["raw"].loc[mask, ["open", "high", "low", "close", "preclose"]] = ""
    result = uv.simulate_union(data, period="test", model="LGB")
    first_plan = result["records"]["plans"][0]["plan"]
    assert first_plan["selected"] == [A]
    assert all(trade["session"] != selection_day for trade in result["records"]["trades"])
    assert result["records"]["plans"][0]["opening_replacements"] == []


def test_prior_index_exit_does_not_remove_a_blocked_held_position():
    data = context()
    first = data["protocol"]["periods"]["test"][0]
    data["pool"] = SimpleNamespace(members_on=lambda day: [A, B] if day < first else [B])
    mask = data["raw"].date.gt(first) & data["raw"].instrument.eq(A)
    data["raw"].loc[mask, "tradestatus"] = "0"
    data["raw"].loc[mask, ["open", "high", "low", "close", "preclose"]] = ""
    data["raw"].loc[mask, ["volume", "amount"]] = "0"
    data["raw"].loc[mask, ["source_complete_quote", "source_observed_market_quote"]] = False
    result = uv.simulate_union(data, period="test", model="MOM60")
    assert result["final_state"]["positions"][A]["quantity"] > 0
    assert not result["performance_valid"]  # A terminal carried mark is not fresh.
    assert result["metrics"]["incomplete_valuation_days"] == 2


def test_unpriced_delisted_claim_cannot_be_reported_as_valid_performance():
    data = context()
    mask = data["raw"].date.ge("2023-01-04") & data["raw"].instrument.eq(A)
    data["raw"].loc[mask, "tradestatus"] = "0"
    data["raw"].loc[mask, ["source_complete_quote", "source_observed_market_quote"]] = False
    data["raw"].loc[mask, ["open", "high", "low", "close", "preclose"]] = ""
    data["raw"].loc[mask, ["volume", "amount"]] = "0"
    data["identity_events"] = [{"instrument": A, "kind": "delisting",
        "effective_date": "2023-01-05", "suspension_start": "2023-01-04",
        "known_at": "2023-01-03T23:59:59+08:00"}]
    result = uv.simulate_union(data, period="test", model="LGB")
    assert not result["performance_valid"]
    assert result["final_state"]["positions"][A]["quantity"] > 0
    assert result["records"]["valuation_audit"][-1]["unpriced_delisted_claims"] == [A]
    assert result["metrics"]["accounting_invalid_days"] == 1


def test_known_delisting_period_rejects_unsupported_sale_but_keeps_observed_valuation():
    data = context()
    data["identity_events"] = [{"instrument": A, "kind": "delisting",
        "effective_date": "2023-01-06", "known_at": "2023-01-03T23:59:59+08:00",
        "termination_known_at": "2023-01-03T23:59:59+08:00",
        "delisting_period_start": "2023-01-04", "last_exchange_trading_session": "2023-01-05",
        "delisting_period_known_at": "2023-01-03T23:59:59+08:00",
        "suspension_start": None}]
    result = uv.simulate_union(data, period="test", model="LGB")
    assert result["final_state"]["positions"][A]["quantity"] > 0
    assert all(row["side"] != "SELL" for row in result["records"]["trades"])
    assert result["metrics"]["rejected_order_count"] >= 1
    assert result["metrics"]["incomplete_valuation_days"] == 0


def test_known_resumption_ends_identity_halt_without_using_future_notice():
    event = {"instrument": A, "kind": "delisting", "known_at": "2023-01-03T23:59:59+08:00",
        "suspension_start": "2023-01-04", "suspension_known_at": "2023-01-03T23:59:59+08:00",
        "suspension_end": "2023-01-05", "resumption_known_at": "2023-01-04T23:59:59+08:00"}
    assert uv.identity_execution_inputs([event], "2023-01-04", {}, [B])[1] == sorted([A,B])
    assert uv.identity_execution_inputs([event], "2023-01-05", {}, [B])[1] == [B]
    event["resumption_known_at"] = "2023-01-05T23:59:59+08:00"
    assert uv.identity_execution_inputs([event], "2023-01-05", {}, [B])[1] == sorted([A,B])


def test_continuous_account_keeps_shares_across_historical_modern_engine_boundary():
    data = context(start="2024-12-30", end="2025-01-03")
    result = uv.simulate_union(data, period="test", model="LGB")
    assert result["performance_valid"]
    assert result["metrics"]["trade_count"] == 1
    assert result["records"]["curves"][-1]["equity_cny"] == result["records"]["curves"][0]["equity_cny"]
    assert list(result["metrics"]["calendar_year_returns"]) == ["2024", "2025"]


def test_stability_windows_include_initial_cash_and_actual_fee_attribution():
    curves = [{"date": day, "equity_cny": "99000", "equity_upper_bound_cny": "99000",
        "cash_cny": "1000", "valuation_complete": True, "accounting_valid": True}
        for day in pd.bdate_range("2023-01-03", periods=63).strftime("%Y-%m-%d")]
    metrics = uv.path_stability(curves, [], Decimal(100000))
    assert metrics["rolling_63_sessions"]["observations"] == 1
    assert metrics["rolling_63_sessions"]["minimum"] == pytest.approx(-.01)
    assert metrics["max_drawdown_conservative_exact"] == "-0.01"
    assert metrics["total_fee_cny"] == "0" and not metrics["cost_attribution_is_counterfactual_rerun"]


def test_future_termination_notice_does_not_change_a_past_signal_rank():
    data = context()
    signal = data["calendar"][data["calendar"].index("2023-01-03")-1]
    before = uv._rank(data, "LGB", signal)[0]
    data["identity_events"] = [{"instrument": A, "kind": "delisting",
        "known_at": "2023-01-05T23:59:59+08:00"}]
    assert uv._rank(data, "LGB", signal)[0] == before == [A, B]


def test_canonical_csv_rejects_truthy_strings_that_could_promote_missing_quotes(tmp_path):
    path = tmp_path / "daily.csv"
    pd.DataFrame([{"instrument": A, "date": "2023-01-03", "source_complete_quote": "False",
        "source_observed_market_quote": "unknown"}]).to_csv(path, index=False)
    with pytest.raises(ValueError, match="Boolean"):
        uv._read_daily(path)


def test_real_canonical_csv_without_redundant_provider_label_reaches_reviewed_ledger(tmp_path):
    data = context()
    original = data["raw"].drop(columns="source")
    path = tmp_path / "canonical_daily.csv"
    original.to_csv(path,index=False)
    data["raw"] = uv._read_daily(path)
    assert set(data["raw"].source) == {"baostock.original_daily_execution"}
    pd.testing.assert_frame_equal(data["raw"][original.columns], original)
    result = uv.simulate_union(data,period="test",model="LGB")
    assert result["performance_valid"] and result["metrics"]["trade_count"] == 1


def test_missing_canonical_source_label_cannot_hide_a_missing_original_receipt(tmp_path):
    original = context()["raw"].drop(columns="source")
    original.loc[0,"source_response_sha256"] = "unbound"
    path = tmp_path/"daily.csv"
    original.to_csv(path,index=False)
    with pytest.raises(ValueError,match="bound original response"):
        uv._read_daily(path)


def test_missing_low_rank_model_score_still_blocks_instead_of_silently_shrinking_pool():
    data = context()
    data["scores"]["LGB"] = data["scores"]["LGB"].drop(B, level="instrument")
    with pytest.raises(ValueError, match="Missing required frozen LGB"):
        uv.simulate_union(data, period="test", model="LGB")


def test_missing_momentum_requires_observed_endpoint_explanation():
    data = context()
    signal = data["calendar"][data["calendar"].index("2023-01-03")-1]
    data["scores"]["MOM60"] = data["scores"]["MOM60"].drop(B, level="instrument")
    data["normalized"] = pd.DataFrame({A: 10., B: 10.}, index=data["calendar"])
    with pytest.raises(ValueError, match="observed declared endpoints"):
        uv._rank(data, "MOM60", signal)
    endpoint = data["calendar"][data["calendar"].index(signal)-60]
    data["normalized"].at[endpoint, B] = float("nan")
    ranked, _, _ = uv._rank(data, "MOM60", signal)
    assert ranked == [A]
    assert data["score_coverage"][("MOM60", signal)]["explicit_exceptions"] == [
        {"instrument": B, "reason": "unobserved_declared_60_session_endpoint"}]


def test_distribution_tax_is_counted_once_at_recognition_not_again_at_settlement():
    data = context()
    data["events"] = [{"event_id": "synthetic_cash", "instrument": A,
        "record_date": "2023-01-03", "ex_date": "2023-01-04", "pay_date": "2023-01-05",
        "listing_date": "2023-01-04", "cash_per_share": "0.1", "bonus_ratio": "0",
        "known_at": "2023-01-02T23:59:59+08:00", "source_response_sha256": "b" * 64}]
    result = uv.simulate_union(data, period="test", model="LGB")
    records = result["records"]["corporate_actions"]
    assert len([row for row in records if "tax" in row]) == 2
    assert result["metrics"]["recognized_dividend_gross_cash_cny"] == "990.00"
    assert result["metrics"]["recognized_distribution_tax_reserve_cny"] == "198.00"


def test_rank_quality_uses_mature_labels_without_future_constituent_survival():
    days = pd.bdate_range("2023-01-02", periods=10).strftime("%Y-%m-%d").tolist()
    names = [f"SH600{number:03d}" for number in range(20)]
    opening = pd.DataFrame({inst: [1 + (number+1)*.001*day for day in range(len(days))]
        for number, inst in enumerate(names)}, index=days)
    scores = pd.Series([float(number) for day in days[:-1] for number in range(20)],
        index=pd.MultiIndex.from_tuples([(pd.Timestamp(day), inst) for day in days[:-1]
            for inst in names], names=["datetime", "instrument"]))
    data = {"calendar": days, "normalized_open": opening, "scores": {"LGB": scores},
        "pool": SimpleNamespace(members_on=lambda day: names if day <= days[1] else names[:-1]),
        "protocol": {"periods": {"test": [days[1], days[-1]]}}}
    result = uv.score_quality(data, model="LGB", period="test")
    assert result["summary"]["daily_observations"] == 2
    assert result["summary"]["rank_IC_mean"] == pytest.approx(1)
    assert result["daily"][0]["pairs"] == 20
    assert result["daily"][0]["label_end"] > days[1]  # One name has left by then.
    assert not result["summary"]["future_membership_filter_used"]
    assert result["summary"]["excluded"]["immature_signal_days"] == 5
    assert data["scores"]["LGB"].equals(scores)
