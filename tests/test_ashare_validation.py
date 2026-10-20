"""Historical selection must be source-bound, conservative and reproducible."""
from copy import deepcopy
from decimal import Decimal as D
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_validation as validation

A = "SH600000"


def all_results():
    return [{"period": period, "case_id": model + "_" + execution,
        "metrics": {"total_return_lower_exact": "0.01", "total_return_upper_exact": "0.011",
            "max_drawdown_conservative_exact": "-0.10", "notional_cny": "1000000",
            "accounting_invalid_days": 0, "terminal_prices_fresh": True}}
        for period in validation.PERIODS for model in validation.MODELS for execution in validation.EXECUTIONS]


def find_result(results, case="M0_E1", period="validation"):
    return next(row for row in results if row["period"] == period and row["case_id"] == case)


def test_selection_requires_all_twelve_unique_declared_cases_and_audited_evidence():
    results = all_results()
    find_result(results)["metrics"]["total_return_lower_exact"] = "0.25"
    find_result(results)["metrics"]["total_return_upper_exact"] = "0.25"
    assert validation.select_candidate(results, evidence_valid=False)["selected_candidate"] is None
    assert validation.select_candidate(results[:-1], evidence_valid=True)["selected_candidate"] is None
    assert validation.select_candidate(results + [deepcopy(results[0])], evidence_valid=True)["selected_candidate"] is None


def test_selection_compares_lower_return_to_baseline_upper_not_estimate():
    results = all_results()
    chosen = find_result(results)["metrics"]
    chosen.update(total_return_lower_exact="0.0109999999", total_return_upper_exact="0.012")
    assert validation.select_candidate(results, evidence_valid=True)["selected_candidate"] is None
    chosen["total_return_lower_exact"] = "0.0110000001"
    assert validation.select_candidate(results, evidence_valid=True)["selected_candidate"] == "M0_E1"


@pytest.mark.parametrize("update", [
    {"accounting_invalid_days": 1}, {"terminal_prices_fresh": False},
])
def test_baseline_unknown_mark_or_stale_terminal_quote_blocks_selection_even_with_high_profit(update):
    results = all_results()
    find_result(results)["metrics"].update(total_return_lower_exact="9.0", total_return_upper_exact="9.0")
    find_result(results, case="M0_E0", period="development")["metrics"].update(update)
    decision = validation.select_candidate(results, evidence_valid=True)
    assert decision["selection_status"].startswith("blocked_")
    assert decision["selected_candidate"] is None


@pytest.mark.parametrize("period", validation.PERIODS)
@pytest.mark.parametrize("update", [{"accounting_invalid_days": 1}, {"terminal_prices_fresh": False}])
def test_candidate_with_invalid_own_either_period_is_ineligible_without_blocking_others(period, update):
    results = all_results()
    find_result(results, "M1_E2")["metrics"].update(total_return_lower_exact="9.0", total_return_upper_exact="9.0")
    find_result(results, "M1_E2", period=period)["metrics"].update(update)
    find_result(results, "M0_E1")["metrics"].update(total_return_lower_exact="0.10", total_return_upper_exact="0.10")
    decision = validation.select_candidate(results, evidence_valid=True)
    assert decision["selected_candidate"] == "M0_E1"
    assert not decision["candidate_validity"]["M1_E2"]["eligible"]
    assert decision["candidate_validity"]["M1_E2"]["reasons"][0].startswith(period + ":")


@pytest.mark.parametrize("ret,dd", [("0", "-0.10"), ("-0.1", "-0.10"), ("0.3", "-0.20000000001")])
def test_nonpositive_or_excess_drawdown_candidate_cannot_win(ret, dd):
    results = all_results()
    find_result(results)["metrics"].update(total_return_lower_exact=ret, total_return_upper_exact=ret,
        max_drawdown_conservative_exact=dd)
    assert validation.select_candidate(results, evidence_valid=True)["selected_candidate"] is None


def test_development_profit_cannot_choose_a_different_validation_winner():
    results = all_results()
    find_result(results, case="M1_E2", period="development")["metrics"].update(
        total_return_lower_exact="900", total_return_upper_exact="900")
    find_result(results, case="M0_E1")["metrics"].update(total_return_lower_exact="0.10", total_return_upper_exact="0.10")
    assert validation.select_candidate(results, evidence_valid=True)["selected_candidate"] == "M0_E1"


def test_ties_follow_exact_return_then_drawdown_then_notional_then_identity():
    results = all_results()
    for case in ("M0_E1", "M0_E2", "M1_E0", "M1_E1", "M1_E2"):
        find_result(results, case)["metrics"].update(total_return_lower_exact="0.10", total_return_upper_exact="0.10")
    find_result(results, "M1_E0")["metrics"]["max_drawdown_conservative_exact"] = "-0.09"
    assert validation.select_candidate(results, evidence_valid=True)["selected_candidate"] == "M1_E0"
    find_result(results, "M1_E1")["metrics"].update(max_drawdown_conservative_exact="-0.09", notional_cny="900000")
    assert validation.select_candidate(results, evidence_valid=True)["selected_candidate"] == "M1_E1"
    find_result(results, "M1_E0")["metrics"]["notional_cny"] = "900000"
    assert validation.select_candidate(results, evidence_valid=True)["selected_candidate"] == "M1_E0"


def test_conservative_drawdown_uses_upper_peak_and_lower_current_wealth():
    curves = [{"equity_cny": "110000", "equity_upper_bound_cny": "110100"},
              {"equity_cny": "90000", "equity_upper_bound_cny": "90050"},
              {"equity_cny": "105000", "equity_upper_bound_cny": "105025"}]
    metrics = validation.conservative_path_metrics(curves, D(100000))
    assert D(metrics["total_return_lower_exact"]) == D(".05")
    assert D(metrics["total_return_upper_exact"]) == D(".05025")
    assert D(metrics["max_drawdown_conservative_exact"]) == D(90000) / D(110100) - 1


def test_first_day_transaction_loss_is_a_drawdown_against_initial_cash():
    metrics = validation.conservative_path_metrics(
        [{"equity_cny": "99900", "equity_upper_bound_cny": "99900"}], D(100000))
    assert D(metrics["max_drawdown_conservative_exact"]) == D("-.001")


def valuation(*, source="observed_raw_close", claims=None):
    return SimpleNamespace(positions=[{"instrument": A, "valuation_source": source}],
        claims=claims or [], valuation_complete=source == "observed_raw_close" and not claims)


def test_verified_halt_carry_is_accounting_valid_but_engine_completeness_is_preserved():
    valued = valuation(source="carried_observed_mark")
    proof = validation.valuation_validity(valued, observed_closes={}, source_suspended=[A])
    assert proof["accounting_valid"] and proof["source_confirmed_carried_instruments"] == [A]
    assert not proof["engine_valuation_complete"]
    assert valued.positions[0]["valuation_source"] == "carried_observed_mark"


def test_unknown_gap_cannot_be_relabelled_a_halt_or_complete():
    proof = validation.valuation_validity(valuation(source="carried_observed_mark"), observed_closes={}, source_suspended=[])
    assert not proof["accounting_valid"] and proof["unknown_price_instruments"] == [A]


def test_bounded_fractional_right_preserves_incompleteness_and_auditable_bound():
    valued = valuation(claims=[{"instrument": A, "action_id": "bonus-allocation", "upper_value_bound": "10.00", "quantity": ".5"}])
    proof = validation.valuation_validity(valued, observed_closes={A: D(10)}, source_suspended=[])
    assert proof["accounting_valid"] and not proof["engine_valuation_complete"]


def test_source_confirmed_halt_cannot_validate_an_unadjusted_old_cum_rights_mark():
    proof = validation.valuation_validity(valuation(source="carried_observed_mark"), observed_closes={},
        source_suspended=[A], unobserved_rights_ex_prices=[A])
    assert not proof["accounting_valid"]
    assert proof["unobserved_rights_ex_price_instruments"] == [A]


@pytest.mark.parametrize("claim", [
    {"instrument": A, "action_id": "unknown"},
    {"instrument": A, "action_id": "unbounded", "upper_value_bound": "Infinity"},
    {"instrument": A, "action_id": "negative", "upper_value_bound": "-1"},
])
def test_unknown_or_unbounded_rights_block_accounting_validity(claim):
    proof = validation.valuation_validity(valuation(claims=[claim]), observed_closes={A: D(10)}, source_suspended=[])
    assert not proof["accounting_valid"] and proof["unbounded_rights"]


def market_fixture():
    calendar = ["2022-12-30", "2023-01-03", "2023-01-04"]
    raw = pd.DataFrame([{"date": day, "instrument": A, "open": "10.00", "high": "10.10", "low": "9.90",
        "close": "10.00", "preclose": "10.00", "volume": "10000", "amount": "100000", "tradestatus": "1",
        "isST": "0", "source_response_sha256": "a" * 64} for day in calendar])
    basic = pd.DataFrame([{"code": "sh.600000", "ipoDate": "1999-11-10", "outDate": ""}])
    pool = SimpleNamespace(intervals={A: [(pd.Timestamp("2022-12-30"), pd.Timestamp("2023-01-04"))]})
    return raw, basic, calendar, pool


def check_market(raw, basic, calendar, pool):
    return validation.validate_daily_rows(raw, basic, calendar, pool, start=calendar[0], end=calendar[-1])


def test_missing_bar_is_not_inferred_to_be_a_suspension():
    raw, basic, calendar, pool = market_fixture()
    with pytest.raises(ValueError, match="missing bars are not verified halts"):
        check_market(raw.iloc[:-1], basic, calendar, pool)


@pytest.mark.parametrize("field,value", [("tradestatus", "unknown"), ("isST", ""), ("source_response_sha256", "not-evidence")])
def test_unknown_daily_status_or_missing_source_identity_blocks_before_performance(field, value):
    raw, basic, calendar, pool = market_fixture()
    raw.loc[1, field] = value
    with pytest.raises(ValueError, match="unknown original execution status/provenance"):
        check_market(raw, basic, calendar, pool)


def test_reported_trading_session_needs_actual_price_reference_and_share_units():
    raw, basic, calendar, pool = market_fixture()
    raw.loc[1, "preclose"] = ""
    with pytest.raises(ValueError, match="complete observed raw quote"):
        check_market(raw, basic, calendar, pool)
    raw.loc[1, "preclose"] = "10"
    raw.loc[1, "volume"] = "100"  # Hands incorrectly represented as shares.
    with pytest.raises(ValueError, match="amount/share-volume units"):
        check_market(raw, basic, calendar, pool)


def test_source_confirmed_halt_survives_as_explicit_status_without_fabricating_quote():
    raw, basic, calendar, pool = market_fixture()
    raw.loc[1, ["open", "high", "low", "close", "preclose", "volume", "amount"]] = ""
    raw.loc[1, "tradestatus"] = "0"
    actual = check_market(raw, basic, calendar, pool)
    assert actual.source_complete_quote.tolist() == [True, False, True]
    assert actual.loc[1, "close"] == "" and actual.loc[1, "tradestatus"] == "0"


def partial_day_fixture(tmp_path):
    raw, basic, calendar, pool = market_fixture()
    raw.loc[1, "tradestatus"] = "0"
    raw.loc[1, "close"] = "9.95"
    raw["source_complete_quote"] = raw.tradestatus.eq("1")
    issuer, quote = tmp_path / "partial-session-notice.pdf", tmp_path / "independent-quote.json"
    issuer.write_bytes(b"synthetic issuer announcement: afternoon suspension")
    quote.write_text('{"synthetic_close":"9.95"}')
    payload = {"events": [{"instrument": A, "date": calendar[1], "source_response_sha256": "a" * 64,
        "evidence": [validation._proof(issuer), validation._proof(quote)], "source_observed_open": "10.00",
        "source_observed_close": "9.95", "execution_remains_blocked": True}]}
    return raw, basic, calendar, pool, payload


def test_bound_partial_day_actual_close_is_observed_while_opening_execution_remains_blocked(tmp_path):
    from stopat30m.data.ashare_history_audit import apply_intraday_observation_evidence
    from stopat30m.backtest import ashare_historical_execution as ex
    raw, basic, calendar, pool, payload = partial_day_fixture(tmp_path)
    raw = apply_intraday_observation_evidence(raw, payload)
    actual = validation.validate_daily_rows(raw, basic, calendar, pool, start=calendar[0], end=calendar[-1],
        intraday_observations=payload, observation_evidence_base=tmp_path)
    assert actual.source_complete_quote.tolist() == [True, False, True]
    assert actual.source_observed_market_quote.tolist() == [True, True, True]
    quotes, metadata, closes, suspended = validation.daily_inputs(actual, basic, calendar[1], [A])
    assert quotes[A].tradestatus == "suspended" and closes[A] == D("9.95") and suspended == [A]
    full_calendar = pd.bdate_range("2022-09-01", "2022-12-30").strftime("%Y-%m-%d").tolist() + calendar[1:]
    result = ex.execute_target_weights(ex.PortfolioState(D(100000)), session=calendar[1], signal_date=calendar[0],
        targets={A: D(1)}, quotes=quotes, metadata=metadata, calendar=full_calendar, policy=ex.ExecutionPolicy(), verified_suspended=suspended)
    assert result.trades == [] and result.orders[0]["status"] == "rejected"
    state = ex.PortfolioState(D(99000), {A: ex.Position(100, last_mark=D(10), last_mark_date=calendar[0])})
    marked = ex.mark_to_close(state, session=calendar[1], prices=closes, calendar=full_calendar, verified_suspended=suspended)
    assert marked.valuation_complete
    assert marked.positions[0]["price_date"] == calendar[1] and marked.positions[0]["price"] == "9.95"


def test_unbound_observed_flag_cannot_promote_a_suspension_into_a_price_observation():
    raw, basic, calendar, pool = market_fixture()
    raw.loc[1, "tradestatus"] = "0"
    raw["source_observed_market_quote"] = True
    with pytest.raises(ValueError, match="not justified by the bound"):
        check_market(raw, basic, calendar, pool)


def test_bound_partial_observation_rejects_revised_original_quote_or_issuer_bytes(tmp_path):
    from stopat30m.data.ashare_history_audit import apply_intraday_observation_evidence
    raw, basic, calendar, pool, payload = partial_day_fixture(tmp_path)
    actual = apply_intraday_observation_evidence(raw, payload)
    actual.loc[1, "close"] = "10.00"
    with pytest.raises(ValueError, match="prices differ"):
        validation.validate_daily_rows(actual, basic, calendar, pool, start=calendar[0], end=calendar[-1],
            intraday_observations=payload, observation_evidence_base=tmp_path)
    issuer = Path(payload["events"][0]["evidence"][0]["path"])
    issuer.write_bytes(b"revised issuer announcement")
    with pytest.raises(ValueError, match="checksum differs"):
        validation.validate_daily_rows(raw, basic, calendar, pool, start=calendar[0], end=calendar[-1],
            intraday_observations=payload, observation_evidence_base=tmp_path)


def test_delisting_effective_session_is_outside_required_trading_coverage():
    raw, basic, calendar, pool = market_fixture()
    basic.loc[0, "outDate"] = calendar[-1]
    actual = check_market(raw.iloc[:-1], basic, calendar, pool)
    assert actual.date.tolist() == calendar[:-1]


def test_independent_market_audit_must_bind_all_consumed_execution_bytes():
    inputs = {key: {"sha256": key + "-digest"} for key in
        ("execution_collection", "dividend_events", "calendar", "historical_intervals")}
    audit = {"kind": "a4_market_evidence_audit", "status": "passed", "checks": {key: True for key in validation.AUDIT_CHECKS},
        "audited_input_sha256": {key: value["sha256"] for key, value in inputs.items()}}
    validation.validate_market_audit(audit, inputs)
    audit["audited_input_sha256"]["dividend_events"] = "different-vintage"
    with pytest.raises(ValueError, match="actual consumed source bytes"):
        validation.validate_market_audit(audit, inputs)


@pytest.mark.parametrize("missing", validation.AUDIT_CHECKS)
def test_any_incomplete_required_source_audit_blocks(missing):
    audit = {"kind": "a4_market_evidence_audit", "status": "passed", "checks": {key: key != missing for key in validation.AUDIT_CHECKS}}
    with pytest.raises(ValueError, match="audits are not complete"):
        validation.validate_market_audit(audit, {})


def rights_event(**updates):
    return {"event_id": "issuer-2023-rights", "instrument": A, "ex_date": "2023-06-14",
        "known_at": "2023-06-01T18:00:00+08:00", "transferable": False,
        "source_response_sha256": "b" * 64, **updates}


def test_fixed_nonparticipation_in_rights_issue_changes_no_cash_or_physical_shares():
    from stopat30m.backtest import ashare_historical_execution as ex
    state = ex.PortfolioState(D(50000), {A: ex.Position(100, last_mark=D(10), last_mark_date="2023-06-13")})
    before = deepcopy(state.to_dict())
    rows = validation.record_unsubscribed_rights(state, [rights_event()], "2023-06-14")
    assert state.to_dict() == before
    assert rows[0]["action"] == "rights_issue_no_subscription"
    assert rows[0]["shares_before"] == rows[0]["shares_after"] == 100
    assert rows[0]["cash_before"] == rows[0]["cash_after"] == "50000.00"
    assert rows[0]["subscription_cash_paid"] == rows[0]["transferable_right_asset_value"] == "0.00"


@pytest.mark.parametrize("updates", [
    {"known_at": "2023-06-14T10:00:00+08:00"}, {"known_at": "2023-06-01T18:00:00"},
    {"transferable": True}, {"transferable": None}, {"source_response_sha256": ""},
])
def test_unverified_or_transferable_rights_cannot_be_silently_ignored(updates):
    with pytest.raises(ValueError, match="rights issue mechanism"):
        validation.validate_rights_issue_terms([rights_event(**updates)])


def synthetic_context(*, halt=False):
    """An explicitly synthetic short source calendar, never market evidence."""
    from stopat30m.backtest import ashare_historical_execution as ex
    b = "SZ000001"
    calendar = pd.bdate_range("2022-09-01", "2022-12-30").strftime("%Y-%m-%d").tolist() + ["2023-01-03", "2023-01-04", "2023-01-05"]
    records = []
    for day in calendar[-4:]:
        for inst in (A, b):
            suspended = halt and day == "2023-01-04" and inst == A
            records.append({"date": day, "instrument": inst, "open": "" if suspended else "10.00",
                "high": "" if suspended else "10.10", "low": "" if suspended else "9.90",
                "close": "" if suspended else "10.00", "preclose": "" if suspended else "10.00",
                "volume": "0" if suspended else "10000", "amount": "0" if suspended else "100000",
                "tradestatus": "0" if suspended else "1", "isST": "0", "source": "explicit_synthetic_test",
                "source_response_sha256": "a" * 64, "source_complete_quote": not suspended})
    scores = pd.Series([2, 1, 1, 2, 1, 2], index=pd.MultiIndex.from_tuples([
        (pd.Timestamp(day), inst) for day in calendar[-4:-1] for inst in (A, b)], names=["datetime", "instrument"]))
    actions = SimpleNamespace(actions=[], excluded=lambda *args, **kwargs: False,
        suspended=lambda *args: False, conversions=lambda *args: [])
    return {"calendar": calendar, "raw": pd.DataFrame(records), "basic": pd.DataFrame([
        {"code": "sh.600000", "ipoDate": "1999-11-10", "outDate": ""},
        {"code": "sz.000001", "ipoDate": "1991-04-03", "outDate": ""}]),
        "pool": SimpleNamespace(members_on=lambda day: [A, b]),
        "normalized": pd.DataFrame({A: [10.] * len(calendar), b: [10.] * len(calendar)}, index=calendar),
        "actions": actions, "events": [], "rights_events": [], "scores": {"M0": scores},
        # Top-k=1 and daily rebalancing make a three-session synthetic test
        # observable; real bundles require the frozen A3 top-k=20 / five days.
        "spec": {"rebalance_sessions": 1, "top_k": 1, "rank_exit_threshold": 40}, "policy": ex.ExecutionPolicy()}


def test_runner_executes_prior_signal_fills_with_historical_fee_in_actual_share_ledger():
    result = validation.simulate_validation(synthetic_context(), period="development", model="M0", execution="E0")
    assert result["metrics"]["accounting_invalid_days"] == 0 and result["metrics"]["terminal_prices_fresh"]
    assert result["records"]["trades"][0]["signal_date"] == "2022-12-30"
    assert result["records"]["trades"][0]["instrument"] == A
    sale = next(row for row in result["records"]["trades"] if row["side"] == "SELL")
    from stopat30m.backtest import ashare_historical_execution as ex
    assert D(sale["stamp_tax"]) == ex.money(D(sale["notional"]) * D(".001"))
    assert D(result["metrics"]["final_equity_cny"]) < 100000


def test_runner_preserves_known_suspension_mark_date_and_original_incomplete_flag():
    result = validation.simulate_validation(synthetic_context(halt=True), period="development", model="M0", execution="E1")
    metrics = result["metrics"]
    assert metrics["incomplete_valuation_days"] == metrics["source_confirmed_carry_days"] == 1
    assert metrics["accounting_invalid_days"] == 0 and metrics["terminal_prices_fresh"]
    carried = next(row for row in result["records"]["positions"] if row["session"] == "2023-01-04")
    assert carried["price_date"] == "2023-01-03" and carried["valuation_source"] == "carried_observed_mark"
    assert all(row["session"] != "2023-01-04" for row in result["records"]["trades"])


def test_partial_day_observed_close_can_plan_next_day_without_permitting_partial_day_fill(tmp_path):
    from stopat30m.data.ashare_history_audit import apply_intraday_observation_evidence
    context = synthetic_context()
    partial = context["raw"].instrument.eq(A) & context["raw"].date.eq("2023-01-03")
    context["raw"].loc[partial, "tradestatus"] = "0"
    context["raw"].loc[partial, "source_complete_quote"] = False
    issuer, quote = tmp_path / "issuer.pdf", tmp_path / "quote.json"
    issuer.write_bytes(b"synthetic partial-day suspension announcement")
    quote.write_text('{"synthetic_observed_close":"10.00"}')
    payload = {"events": [{"instrument": A, "date": "2023-01-03", "source_response_sha256": "a" * 64,
        "evidence": [validation._proof(issuer), validation._proof(quote)], "source_observed_open": "10.00",
        "source_observed_close": "10.00", "execution_remains_blocked": True}]}
    context["raw"] = apply_intraday_observation_evidence(context["raw"], payload)
    scores = context["scores"]["M0"]
    scores.loc[(pd.Timestamp("2023-01-03"), A)] = 2
    scores.loc[(pd.Timestamp("2023-01-03"), "SZ000001")] = 1
    result = validation.simulate_validation(context, period="development", model="M0", execution="E1")
    assert result["records"]["trades"][0]["session"] == "2023-01-04"
    assert result["records"]["trades"][0]["signal_date"] == "2023-01-03"
    assert result["records"]["trades"][0]["instrument"] == A
    assert all(row["session"] != "2023-01-03" for row in result["records"]["trades"])


def test_declaration_binds_effective_rules_and_preserves_original_a3_spec(tmp_path):
    spec = Path(__file__).resolve().parents[1] / "docs/research/a3_optimization_spec.json"
    before = spec.read_bytes()
    path = tmp_path / "declaration.json"
    declaration = validation.write_validation_declaration(path, spec_path=spec, now="2026-10-20T01:00:00+00:00")
    assert declaration["a3_spec_sha256"] == validation.SPEC_SHA256
    assert declaration["selection_drawdown_basis"] == "current_lower_nav_over_running_upper_nav_peak"
    assert declaration["ineligible_nonbaseline_candidate_does_not_block_other_valid_candidates"]
    assert declaration["prior_signal_planning_prices_require_complete_original_quote"]
    assert declaration["historical_status_publication_timestamp_known"] is False
    assert declaration["historical_status_is_announcement_time_PIT"] is False
    assert "not_observed_publication_timestamp" in declaration["daily_status_known_at_semantics"]
    assert validation._validate_declaration(path) == declaration
    assert spec.read_bytes() == before
    with pytest.raises(ValueError, match="Preserve"):
        validation.write_validation_declaration(path, spec_path=spec)


def test_changed_validity_policy_is_rejected_before_any_source_load_or_performance(tmp_path):
    spec = Path(__file__).resolve().parents[1] / "docs/research/a3_optimization_spec.json"
    path = tmp_path / "declaration.json"
    declaration = validation.write_validation_declaration(path, spec_path=spec)
    declaration["selection_maximum_absolute_drawdown"] = "0.50"
    path.write_text(json.dumps(declaration))
    with pytest.raises(ValueError, match="validity/selection policy differs"):
        validation._validate_declaration(path)


def test_bound_data_source_changes_cannot_be_rebound_to_a_new_digest_silently(tmp_path):
    source = tmp_path / "source.json"
    source.write_text('{"status":"actual original"}')
    proof = validation._proof(source)
    source.write_text('{"status":"revised vintage"}')
    with pytest.raises(ValueError, match="checksum differs"):
        validation._bound_path(proof, tmp_path)


def test_nested_audit_original_sources_are_verified_and_retained_for_final_recheck(tmp_path):
    issuer = tmp_path / "issuer.pdf"
    issuer.write_bytes(b"synthetic issuer source bytes")
    quote = tmp_path / "sina.json"
    quote.write_text('{"synthetic":"original quote"}')
    inputs = {"dividend": {"issuer_documents": [validation._proof(issuer)], "quotes": {"sina": validation._proof(quote)}}}
    bound = validation.audit_source_proofs(inputs, tmp_path)
    assert set(bound) == {"market_audit_source:dividend:issuer_documents:0", "market_audit_source:dividend:quotes:sina"}
    issuer.write_bytes(b"silently revised issuer terms")
    with pytest.raises(ValueError, match="checksum differs"):
        validation.audit_source_proofs(inputs, tmp_path)
