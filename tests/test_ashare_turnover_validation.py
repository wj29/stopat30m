from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_stability_validation as stability
from stopat30m import ashare_turnover_validation as turnover
from stopat30m.backtest import ashare_execution as ex


A, B, C = "SH600000", "SH600001", "SH600002"


def actual_calendar():
    # Explicit synthetic sessions, including a gap; never used as real exchange evidence.
    return pd.bdate_range("2024-08-01", "2025-03-31").strftime("%Y-%m-%d").tolist()


def fill(day, inst, side, quantity):
    return dict(session=day, instrument=inst, side=side, quantity=quantity, status="simulated_fill")


def buy(tracker, state, day, inst=A, quantity=100):
    after = deepcopy(state)
    after.positions.setdefault(inst, ex.Position(0)).quantity += quantity
    tracker.after_trades(state, after, [fill(day, inst, "BUY", quantity)], day)
    return after


def sell(tracker, state, day, inst=A, quantity=100):
    after = deepcopy(state)
    after.positions[inst].quantity -= quantity
    if not after.positions[inst].quantity:
        del after.positions[inst]
    tracker.after_trades(state, after, [fill(day, inst, "SELL", quantity)], day)
    return after


def market(*, changing=False, refused_exit=False, end_at=14):
    calendar = actual_calendar()
    days = [day for day in calendar if day >= "2025-01-02"][:end_at+1]
    source_days = calendar[calendar.index(days[0])-1:calendar.index(days[-1])+1]
    instruments = [f"SH{600000+i:06d}" for i in range(60)]
    raw = pd.DataFrame([dict(date=day, instrument=inst, open="10", high="10.1", low="9.9",
        close="10", preclose="10", volume="10000", amount="100000", tradestatus="1", isST="0",
        source="synthetic_fixture", source_response_sha256="a"*64,
        source_complete_quote=True, source_observed_market_quote=True)
        for day in source_days for inst in instruments])
    if refused_exit:
        # The worst prior-ranked incumbent is one of two frozen exit intents.
        mask = (raw.date == days[10]) & (raw.instrument == instruments[19])
        raw.loc[mask, ["open", "high", "low", "close"]] = "9"
    score_rows = []
    for number, day in enumerate(source_days[:-1]):
        ranked = instruments[20:]+instruments[:20] if changing and number >= 5 else instruments
        score_rows.extend((pd.Timestamp(day), inst, float(100-rank)) for rank, inst in enumerate(ranked))
    scores = pd.Series([row[2] for row in score_rows], index=pd.MultiIndex.from_tuples(
        [(row[0], row[1]) for row in score_rows], names=["datetime", "instrument"]))
    actions = SimpleNamespace(actions=[], excluded=lambda *a, **k: False,
        suspended=lambda *a: False, conversions=lambda *a: [])
    return dict(calendar=calendar, raw=raw, basic=pd.DataFrame([
        dict(code="sh."+inst[2:], ipoDate="1999-11-10", outDate="") for inst in instruments]),
        pool=SimpleNamespace(members_on=lambda day: instruments), actions=actions, events=[],
        rights_events=[], identity_events=[], tax_contracts=[], scores={"LGB": scores},
        protocol={"periods": {"2025": [days[0], days[-1]], "continuous": [days[0], days[-1]],
            "2026": [days[0], days[-1]]}, "execution": deepcopy(stability.EXECUTION)})


@pytest.mark.parametrize("period", ["2025", "2026", "continuous"])
def test_baseline_is_the_entire_original_result_without_extra_fields(period):
    expected = stability.simulate_stability(market(changing=True), period=period, model="LGB")
    actual = turnover.simulate_turnover(market(changing=True), period=period,
        variant=dict(turnover.FIXED_VARIANTS[0]))
    assert actual == expected
    assert "variant" not in actual and "holding_age" not in actual["records"]


@pytest.mark.parametrize("variant", turnover.FIXED_VARIANTS[1:])
def test_unchanged_selection_preserves_real_fees_fills_and_original_ledger(variant):
    expected = stability.simulate_stability(market(), period="2025", model="LGB")
    actual = turnover.simulate_turnover(market(), period="2025", variant=variant)
    assert actual["final_state"] == expected["final_state"]
    for name, rows in expected["records"].items():
        assert actual["records"][name] == rows
    assert actual["performance_valid"] == expected["performance_valid"] is True
    assert all(actual["metrics"][name] == value for name, value in expected["metrics"].items())
    assert actual["records"]["holding_age"]


def test_hold10_allows_d10_not_d5_and_counts_only_frozen_exit_attempts():
    data = market(changing=True)
    days = [day for day in data["calendar"] if day >= data["protocol"]["periods"]["2025"][0]]
    result = turnover.simulate_turnover(data, period="2025", variant=turnover.FIXED_VARIANTS[1])
    plans = result["records"]["turnover_plans"]
    assert [row["session"] for row in plans] == days[:11:5]
    assert len(plans[1]["minimum_hold_locked"]) == 20
    assert not plans[1]["allowed_exits"]
    assert plans[2]["exit_intent_count"] == 2
    assert set(plans[2]["holding_age_sessions"].values()) == {10}
    assert result["metrics"]["planned_exit_attempt_securities"] == 2


def test_refused_exit_keeps_original_execution_no_substitution_or_slot_hard_cap():
    result = turnover.simulate_turnover(market(changing=True, refused_exit=True),
        period="2025", variant=turnover.FIXED_VARIANTS[1])
    plan = result["records"]["turnover_plans"][-1]
    assert plan["exit_intent_count"] == 2
    assert plan["actual_exit_fill_count"] == 1
    assert plan["rejected_exit_orders"] == 1
    assert len(plan["exit_intent_without_sell_fill"]) == 1
    assert plan["rejected_exit_substitutions"] == 0
    assert plan["actual_active_count_after_execution"] == 21
    assert plan["actual_overflow_after_execution"] == 1


def test_actual_buy_partial_sell_additional_buy_full_exit_and_reentry_clock():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state = buy(tracker, state, days[0], quantity=200)
    state = sell(tracker, state, days[3], quantity=100)
    state = buy(tracker, state, days[4], quantity=100)
    state.positions[A].bought_date = days[4]  # Latest BUY is deliberately different from origin.
    assert tracker.close(state, days[10])[0]["holding_age_sessions"] == 10
    assert tracker.holding_started[A] == days[0]
    state = sell(tracker, state, days[11], quantity=200)
    assert A not in tracker.holding_started
    state = buy(tracker, state, days[12])
    assert tracker.close(state, days[12])[0]["holding_age_sessions"] == 0


def test_refused_orders_cannot_create_origin_and_missing_fills_fail_closed():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    tracker.after_trades(state, state, [], days[0])
    assert not tracker.holding_started
    after = deepcopy(state)
    after.positions[A] = ex.Position(100)
    with pytest.raises(ValueError, match="reconcile"):
        tracker.after_trades(state, after, [], days[0])
    with pytest.raises(ValueError, match="genuine"):
        tracker.after_trades(state, after, [{**fill(days[0], A, "BUY", 100), "status": "rejected"}], days[0])


def test_actual_bonus_record_date_origin_survives_sale_and_delayed_listing():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state = buy(tracker, state, days[0])
    tracker.close(state, days[1])
    state.holdings_history[days[1]] = {A: 100}
    known = days[0]+"T00:00:00+08:00"
    registered = ex.register_bonus_entitlement(state, event_id="bonus", instrument=A,
        record_date=days[1], ex_date=days[2], listing_date=days[5], ratio=".1", session=days[2],
        known_at=known, evidence_sha256="a"*64, calendar=days, price="10")
    tracker.after_corporate_actions(state, registered.state, [registered.record], days[2])
    state = sell(tracker, registered.state, days[3])
    assert tracker.holding_started[A] == days[0]  # Pending stock delivery retains origin.
    listed = ex.apply_bonus_shares(state, event_id="bonus", instrument=A, record_date=days[1],
        listing_date=days[5], ratio=".1", session=days[5], known_at=known,
        evidence_sha256="a"*64, calendar=days, price="10")
    tracker.after_corporate_actions(state, listed.state, [listed.record], days[5])
    assert listed.state.positions[A].quantity == 10
    assert tracker.close(listed.state, days[5])[0]["holding_age_sessions"] == 5


def test_exact_fraction_pending_keeps_origin_then_settled_claim_does_not_bridge_reentry():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state = buy(tracker, state, days[0])
    tracker.close(state, days[1]); state.holdings_history[days[1]] = {A: 100}
    terms = dict(event_id="fraction", instrument=A, record_date=days[1], ratio=".004",
        known_at=days[0]+"T00:00:00+08:00", evidence_sha256="a"*64, calendar=days, price="10")
    registered = ex.register_bonus_entitlement(state, ex_date=days[2], listing_date=days[5], session=days[2], **terms)
    tracker.after_corporate_actions(state, registered.state, [registered.record], days[2])
    state = sell(tracker, registered.state, days[3])
    assert tracker.holding_started[A] == days[0]
    assert turnover.active_instruments(state) == {A}
    listed = ex.apply_bonus_shares(state, listing_date=days[5], session=days[5], **terms)
    tracker.after_corporate_actions(state, listed.state, [listed.record], days[5])
    assert listed.state.claims and not turnover.active_instruments(listed.state)
    assert A not in tracker.holding_started
    state = buy(tracker, listed.state, days[6])
    assert tracker.holding_started[A] == days[6]
    assert any(row["kind"] == "fractional_claim_origin_only" for row in tracker.audit)


def test_confirmed_exchange_inherits_earlier_economic_origin_into_existing_successor():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state = buy(tracker, state, days[0], A)
    state = buy(tracker, state, days[3], B)
    merged = ex.apply_share_exchange(state, action_id="actual_merge", from_instrument=A, to_instrument=B,
        ratio="1.25", session=days[5], tradable_from=days[5],
        confirmed_known_at=days[4]+"T00:00:00+08:00", evidence_sha256="a"*64,
        calendar=days, successor_price="10")
    tracker.after_corporate_actions(state, merged.state, [merged.record], days[5])
    assert merged.state.positions[B].quantity == 225
    assert tracker.holding_started == {B: days[0]}
    assert tracker.close(merged.state, days[5])[0]["holding_age_sessions"] == 5
    empty = ex.apply_share_exchange(merged.state, action_id="zero_merge", from_instrument=C, to_instrument=B,
        ratio="1", session=days[6], tradable_from=days[6],
        confirmed_known_at=days[4]+"T00:00:00+08:00", evidence_sha256="a"*64,
        calendar=days, successor_price="10")
    tracker.after_corporate_actions(merged.state, empty.state, [empty.record], days[6])
    assert tracker.holding_started == {B: days[0]}


def test_fraction_only_exchange_retains_claim_proof_but_reentry_gets_new_origin():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state = buy(tracker, state, days[0], A)
    merged = ex.apply_share_exchange(state, action_id="fraction_only_merge", from_instrument=A, to_instrument=B,
        ratio=".004", session=days[5], tradable_from=days[5],
        confirmed_known_at=days[4]+"T00:00:00+08:00", evidence_sha256="a"*64,
        calendar=days, successor_price="10")
    tracker.after_corporate_actions(state, merged.state, [merged.record], days[5])
    assert merged.state.claims and not tracker.holding_started
    after = buy(tracker, merged.state, days[6], B)
    assert tracker.holding_started == {B: days[6]}
    assert tracker.close(after, days[6])[0]["holding_age_sessions"] == 0


def test_treatment_delayed_bonus_uses_record_origin_and_preserves_original_accounting():
    data = market(end_at=7)
    days = [day for day in data["calendar"] if day >= data["protocol"]["periods"]["2025"][0]]
    data["events"] = [dict(event_id="explicit_synthetic_bonus", instrument=A,
        record_date=days[0], ex_date=days[1], pay_date=days[1], listing_date=days[4],
        known_at="2024-12-30T23:59:59+08:00", source_response_sha256="a"*64,
        cash_per_share="0", bonus_ratio=".1")]
    expected = stability.simulate_stability(deepcopy(data), period="2025", model="LGB")
    actual = turnover.simulate_turnover(data, period="2025", variant=turnover.FIXED_VARIANTS[1])
    assert actual["final_state"] == expected["final_state"]
    assert actual["records"]["corporate_actions"] == expected["records"]["corporate_actions"]
    listed = [row for row in actual["records"]["holding_age"] if row["session"] == days[4] and row["instrument"] == A]
    assert listed[0]["holding_started"] == days[0]
    assert listed[0]["holding_age_sessions"] == 4


def test_unknown_existing_or_bonus_origin_is_never_inferred_from_dates_or_prices():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state.positions[A] = ex.Position(100, bought_date=days[0], last_mark=Decimal("10"))
    with pytest.raises(ValueError, match="Unknown actual"):
        tracker.close(state, days[1])
    before, after = ex.PortfolioState.initial(), ex.PortfolioState.initial()
    after.positions[A] = ex.Position(10)
    ledger = dict(kind="bonus_shares", instrument=A, session=days[3], record_date=days[1],
        record_date_shares=100, new_integer_shares=10, evidence_sha256="a"*64)
    with pytest.raises(ValueError, match="finalized record-date"):
        tracker.after_corporate_actions(before, after, [ledger], days[3])


@pytest.mark.parametrize("variant", [
    dict(key="hold11_cap2", minimum_hold_sessions=11, max_replacements=2),
    dict(key="hold10_cap2", minimum_hold_sessions=True, max_replacements=2),
    dict(key="hold10_cap2", minimum_hold_sessions=10, max_replacements=2, extra=1),
])
def test_undeclared_variants_are_rejected_before_simulation(variant):
    with pytest.raises(ValueError, match="declared|integer"):
        turnover.simulate_turnover({}, period="2025", variant=variant)


def test_other_execution_parameters_cannot_change_in_turnover_experiment():
    data = market(); data["protocol"]["execution"]["rebalance_sessions"] = 1
    with pytest.raises(ValueError, match="all frozen execution"):
        turnover.simulate_turnover(data, period="2025", variant=turnover.FIXED_VARIANTS[1])


def test_five_variants_exactly_match_prefrozen_design():
    import json
    from pathlib import Path
    design = json.loads((Path(__file__).parents[1]/"docs/research/a6_lgb_turnover_design_v2.json").read_text())
    assert list(turnover.FIXED_VARIANTS) == design["variants"]


def test_cash_dividend_claim_does_not_extend_a_cleared_holding_epoch():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state = buy(tracker, state, days[0])
    state.entitlements["cash"] = dict(kind="cash_dividend", instrument=A, status="pending", quantity="100")
    state = sell(tracker, state, days[1])
    assert not tracker.holding_started and not turnover.active_instruments(state)
    state = buy(tracker, state, days[2])
    assert tracker.holding_started[A] == days[2]


def test_pending_bonus_and_fraction_claim_have_distinct_activity_semantics():
    state = ex.PortfolioState.initial()
    state.claims["fraction"] = dict(kind="bonus_shares", instrument=A, quantity=".4")
    assert not turnover.active_instruments(state)
    state.entitlements["pending"] = dict(kind="bonus_shares", status="pending", instrument=A, quantity=".4")
    assert turnover.active_instruments(state) == {A}
    state.entitlements["pending"]["quantity"] = "-1"
    with pytest.raises(ValueError, match="finite and nonnegative"):
        turnover.active_instruments(state)


def test_full_calendar_supports_origins_older_than_execution_engine_warmup():
    days = actual_calendar()
    tracker, state = turnover.HoldingAgeTracker(days), ex.PortfolioState.initial()
    state = buy(tracker, state, days[0])
    assert tracker.close(state, days[120])[0]["holding_age_sessions"] == 120


def test_side_absent_rejected_precheck_is_counted_without_rewriting_orders():
    state = ex.PortfolioState.initial(); state.positions[A] = ex.Position(100)
    plan = SimpleNamespace(exits=(A,), policy=SimpleNamespace(top_k=20))
    orders = [dict(instrument=A, status="rejected", reason="missing quote")]
    result = SimpleNamespace(state=state, trades=[], orders=orders)
    audit = turnover._exit_audit(plan, result, {})
    assert audit["rejected_exit_orders"] == 1
    assert audit["exit_intent_without_sell_fill"] == [A]
    assert orders == [dict(instrument=A, status="rejected", reason="missing quote")]


def test_treatment_full_extra_tax_and_rounding_reserve_failure_is_atomic(tmp_path):
    import hashlib
    from stopat30m.data.a5_distribution_tax import prepare_distribution_tax
    data = market(end_at=2)
    for field, value in {"open": "1", "close": "1", "preclose": "1", "high": "1.01", "low": ".99"}.items():
        data["raw"][field] = value
    record = data["protocol"]["periods"]["2025"][0]
    day = data["calendar"][data["calendar"].index(record)+1]
    path = tmp_path/"synthetic_issuer_proof.pdf"
    path.write_bytes(b"%PDF explicitly synthetic test fixture, not issuer evidence")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    event = dict(event_id="synthetic_large_obligation", instrument=A, record_date=record,
        ex_date=day, pay_date=day, listing_date=day, known_at="2024-12-30T23:59:59+08:00",
        source_response_sha256="a"*64, cash_per_share=".9907442", stock_dividend_ratio="0",
        capital_reserve_ratio="20", bonus_ratio="20")
    fact = {key: event[key] for key in ("instrument", "record_date", "ex_date", "cash_per_share",
        "stock_dividend_ratio", "capital_reserve_ratio")}
    fact.update(status="verified_face_value_source_classification_unresolved", review_required=False,
        original_pdf_visual_reviewed=True, evidence=[dict(path=str(path), sha256=digest,
            url="https://issuer.example/explicit-synthetic-fixture.pdf", pdf_pages_1based=[1])],
        share_face_value_cny="1", capital_reserve_face_value_per_record_share="20",
        capital_reserve_tax_classification="unresolved_conservative_upper_bound", classification_verified=False,
        conservative_tax_reserve_upper_bound=True, capital_reserve_tax_exempt=False, capital_reserve_taxable=False)
    events, report = prepare_distribution_tax([event], {"events": [fact]}, {"events": []})
    assert report["status"] == "passed"
    events[0].update(cash_payment_gross_rounding="ROUND_DOWN",
        component_resolution_document={"path": str(path), "sha256": digest})
    data["events"], data["tax_contracts"] = events, report["contracts"]
    baseline = stability.simulate_stability(deepcopy(data), period="2025", model="LGB")
    actual = turnover.simulate_turnover(data, period="2025", variant=turnover.FIXED_VARIANTS[1])
    assert not actual["performance_valid"]
    assert actual["stopped"] == baseline["stopped"]
    assert actual["stopped"]["kind"] == "unfunded_full_distribution_tax_reserve"
    assert actual["final_state"] == baseline["final_state"]
    assert actual["actual_last_valued_session"] == record
    assert actual["metrics"]["cash_distribution_rounding_reserved_cny"] == "0"
    assert not actual["records"]["corporate_actions"]
