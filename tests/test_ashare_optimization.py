"""Bind exploratory execution results and preserve record-date rights."""
from copy import deepcopy
from decimal import Decimal as D
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from stopat30m import ashare_optimization as opt
from stopat30m.backtest import ashare_execution as ex

A, B, C = "SH600000", "SZ000001", "SH600009"


def event(**updates):
    row = {"instrument": A, "event_id": "rights", "record_date": "2025-01-03",
        "ex_date": "2025-01-06", "pay_date": "2025-01-07", "listing_date": None}
    return {**row, **updates}


@pytest.mark.parametrize("ownership", ["record_shares", "cash_receivable", "bonus_receivable", "residual_claim"])
def test_economically_owned_right_survives_when_no_physical_share_is_held(ownership):
    state = ex.PortfolioState(D(10000))
    state.holdings_history["2025-01-03"] = {}
    if ownership == "record_shares":
        state.holdings_history["2025-01-03"][A] = 100
    elif ownership == "cash_receivable":
        state.entitlements["rights:cash"] = {"instrument": A, "status": "pending", "net_cash": "80"}
    elif ownership == "bonus_receivable":
        state.entitlements["rights:bonus"] = {"instrument": A, "status": "pending", "quantity": "10"}
    else:
        state.claims["allocation"] = {"instrument": A, "created_date": "2025-01-02", "quantity": ".5"}
    assert opt.relevant_events(state, [event()], "2025-01-07", "2025-01-02") == [event()]


def test_empty_zero_right_can_be_elided_but_source_event_is_untouched():
    state = ex.PortfolioState(D(10000), holdings_history={"2025-01-03": {}})
    row = event()
    before = deepcopy(row)
    assert opt.relevant_events(state, [row], "2025-01-06", "2025-01-02") == []
    assert row == before and state.cash == 10000
    # A later-created residual did not own shares on this record date.
    state.claims["later"] = {"instrument": A, "created_date": "2025-01-06", "quantity": ".5"}
    assert opt.relevant_events(state, [row], "2025-01-07", "2025-01-02") == []


def test_current_position_and_actual_event_dates_are_checked_before_elision():
    state = ex.PortfolioState(D(10000), {A: ex.Position(100, last_mark=D(10), last_mark_date="2025-01-03")},
        holdings_history={"2025-01-03": {}})
    assert opt.relevant_events(state, [event()], "2025-01-06", "2025-01-02") == [event()]
    assert opt.relevant_events(state, [event()], "2025-01-08", "2025-01-02") == []


def test_pre_experiment_record_without_owned_right_is_empty_for_cash_start():
    state = ex.PortfolioState(D(10000))
    assert opt.relevant_events(state, [event(record_date="2024-12-31")], "2025-01-06", "2025-01-02") == []


def test_missing_record_date_snapshot_cannot_be_inferred_to_mean_zero_shares():
    state = ex.PortfolioState(D(10000))
    with pytest.raises(ValueError, match="record-date holdings snapshot is missing"):
        opt.relevant_events(state, [event()], "2025-01-06", "2025-01-02")


@pytest.fixture
def baseline_fixture(tmp_path):
    baseline = tmp_path/"baseline"
    baseline.mkdir()
    curves = [{"date": "2025-01-02", "equity_cny": "9993.99", "cash_cny": "8993.99"},
              {"date": "2025-01-03", "equity_cny": "10013.99", "cash_cny": "8993.99"}]
    trade = {"session": "2025-01-02", "signal_date": "2024-12-31", "instrument": A, "side": "BUY",
        "quantity": "100", "execution_price": "10.01", "reference_price": "10", "notional": "1001",
        "commission": "5", "transfer_fee": ".01", "stamp_tax": "0", "total_fee": "5.01",
        "slippage_cost": "1", "cash_before": "10000", "cash_change": "-1006.01", "cash_after": "8993.99"}
    pd.DataFrame([{**row, "strategy": "lightgbm"} for row in curves]+[
        {"date": "2025-01-02", "equity_cny": "123456", "cash_cny": "0", "strategy": "momentum"}
    ]).to_csv(baseline/"curves.csv", index=False)
    pd.DataFrame([{**trade, "strategy": "lightgbm"}, {**trade, "strategy": "momentum"}]).to_csv(baseline/"trades.csv", index=False)
    return {"baseline": baseline}, {"records": {"curves": curves, "trades": [trade]}}


def test_reproduction_binds_daily_cash_nav_and_frozen_costed_fills(baseline_fixture):
    context, result = baseline_fixture
    opt.assert_baseline_reproduction(context, result)


@pytest.mark.parametrize("field", ["cash_cny", "equity_cny", "date"])
def test_reproduction_rejects_shifted_daily_cash_nav_or_dates(baseline_fixture, field):
    context, result = baseline_fixture
    result["records"]["curves"][1][field] = "2025-01-06" if field == "date" else "10013.98"
    with pytest.raises(ValueError, match="daily cash/NAV"):
        opt.assert_baseline_reproduction(context, result)


@pytest.mark.parametrize("field", ["quantity", "execution_price", "commission", "transfer_fee", "stamp_tax", "cash_after", "slippage_cost"])
def test_reproduction_rejects_any_cost_or_share_drift(baseline_fixture, field):
    context, result = baseline_fixture
    result["records"]["trades"][0][field] = str(D(result["records"]["trades"][0][field]) + D(".01"))
    with pytest.raises(ValueError, match="fill/cost/quantity"):
        opt.assert_baseline_reproduction(context, result)


def test_reproduction_rejects_omitted_trade_even_when_nav_looks_equal(baseline_fixture):
    context, result = baseline_fixture
    result["records"]["trades"] = []
    with pytest.raises(ValueError, match="trade count"):
        opt.assert_baseline_reproduction(context, result)


def test_ranking_uses_prior_membership_prices_and_deterministic_code_ties():
    day = "2025-01-03"
    actions = SimpleNamespace(excluded=lambda inst, date, close: inst == C)
    context = {"pool": SimpleNamespace(members_on=lambda date: [B, C, A]), "actions": actions,
        "normalized": pd.DataFrame({A: [10.], B: [10.], C: [10.]}, index=[day])}
    scores = pd.Series([.1, .1, 100], index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp(day), A), (pd.Timestamp(day), B), (pd.Timestamp(day), C)], names=["datetime", "instrument"]))
    ranked, members = opt.rank_at_close(context, scores, day)
    assert ranked == [A, B] and members == [B, C, A]
    with pytest.raises(ValueError, match="previous-close scores"):
        opt.rank_at_close(context, scores.loc[[]], day)


def test_missing_raw_signal_price_fails_instead_of_borrowing_next_open(monkeypatch):
    day = "2025-01-02"
    calendar = pd.bdate_range("2024-08-01", day).strftime("%Y-%m-%d").tolist()  # Explicit synthetic calendar.
    previous = calendar[-2]
    actions = SimpleNamespace(actions=[], suspended=lambda *args: False,
        excluded=lambda *args, **kwargs: False, conversions=lambda *args: [])
    context = {"a2_spec": {"oos_start": day, "oos_end": day, "rebalance_sessions": 5, "top_k": 1},
        "calendar": calendar, "raw": pd.DataFrame([{"date": day, "instrument": A,
            "source_complete_quote": True, "close": "10"}]), "basic": pd.DataFrame(),
        "actions": actions, "events": [], "spec": {"rank_exit_threshold": 40},
        "normalized": pd.DataFrame({A: [10.]*len(calendar)}, index=calendar),
        "pool": SimpleNamespace(members_on=lambda date: [A])}
    quote = ex.OpeningQuote(day, D(10), D(10), "trading", "synthetic actual next open")
    meta = ex.SecurityInfo(A, "sse_main", "2000-01-03", False, "2024-01-01T00:00:00+08:00", "synthetic identity")
    monkeypatch.setattr(opt.a2, "_daily_inputs", lambda *args: ({A: quote}, {A: meta}, {A: D(10)}))
    scores = pd.Series([.1], index=pd.MultiIndex.from_tuples([(pd.Timestamp(previous), A)], names=["datetime", "instrument"]))
    with pytest.raises(ValueError, match="preceding raw close"):
        opt.simulate_review(context, scores, case_id="M0_E1", execution_id="E1", policy=ex.ExecutionPolicy())


def publication_fixture(tmp_path):
    proof = tmp_path/"frozen-input.json"
    proof.write_text('{"source":"synthetic immutable evidence"}')
    actions = SimpleNamespace(assert_unchanged=lambda: None)
    context = {"bound": {"spec": {"path": str(proof), "sha256": hashlib.sha256(proof.read_bytes()).hexdigest()}},
        "actions": actions, "spec": {"previously_observed_history_review": ["2025-01-01", "2026-10-16"]}}
    result = {"case_id": "M0_E2", "execution_id": "E2", "metrics": {"total_return": 9.0, "max_drawdown": -.01},
        "records": {"curves": [], "plans": []}, "final_state": {}, "history_review_is_fresh_oos": False}
    return context, result, proof


def test_even_very_high_historical_profit_cannot_choose_or_activate_a_winner(tmp_path):
    context, result, _ = publication_fixture(tmp_path)
    report = opt.publish_review(context, tmp_path/"review", [result])
    assert report["selection_status"].startswith("blocked_")
    assert report["selected_candidate"] is None and report["active_paper_changed"] is False
    assert report["history_review_is_fresh_oos"] is False
    manifest = json.loads((tmp_path/"review/manifest.json").read_text())
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((tmp_path/"review"/name).read_bytes()).hexdigest() == digest


def test_changed_bound_input_rejects_publication_before_creating_output(tmp_path):
    context, result, proof = publication_fixture(tmp_path)
    proof.write_text('{"changed":true}')
    with pytest.raises(ValueError, match="changed during review"):
        opt.publish_review(context, tmp_path/"review", [result])
    assert not (tmp_path/"review").exists()


def mocked_baseline_run(tmp_path, monkeypatch):
    context, result, _ = publication_fixture(tmp_path)
    origin = tmp_path/"model-origin"
    origin.mkdir()
    predictions = origin/"predictions.csv"
    predictions.write_text(f"datetime,instrument,score\n2024-12-31,{A},0.125\n")
    digest = hashlib.sha256(predictions.read_bytes()).hexdigest()
    (origin/"manifest.json").write_text(json.dumps({"files": {"predictions.csv": digest}}))
    context["spec"]["models"] = [{"training_origin": str(origin)}]
    context["a2_spec"] = {}
    monkeypatch.setattr(opt, "load_review_context", lambda *args: context)
    monkeypatch.setattr(opt.a2, "execution_policy", lambda *args: ex.ExecutionPolicy())
    monkeypatch.setattr(opt, "assert_baseline_reproduction", lambda *args: None)
    return predictions, result


def test_baseline_prediction_receipt_is_verified_before_reading(tmp_path, monkeypatch):
    predictions, _ = mocked_baseline_run(tmp_path, monkeypatch)
    predictions.write_text(f"datetime,instrument,score\n2024-12-31,{A},9\n")
    def forbidden_read(*args, **kwargs):
        raise AssertionError("Changed predictions must be rejected before pandas reads them")
    monkeypatch.setattr(opt.pd, "read_csv", forbidden_read)
    with pytest.raises(ValueError, match="predictions changed before reading"):
        opt.run_existing_model_review("unused", tmp_path/"review", spec_path="unused")
    assert not (tmp_path/"review").exists()


def test_prediction_receipt_is_not_recomputed_after_simulation(tmp_path, monkeypatch):
    predictions, result = mocked_baseline_run(tmp_path, monkeypatch)
    def simulate(context, scores, *, case_id, execution_id, **kwargs):
        assert scores.iloc[0] == .125  # Every case keeps the actually read series.
        predictions.write_text(f"datetime,instrument,score\n2024-12-31,{A},9\n")
        return {**result, "case_id": case_id, "execution_id": execution_id}
    monkeypatch.setattr(opt, "simulate_review", simulate)
    with pytest.raises(ValueError, match="changed during review"):
        opt.run_existing_model_review("unused", tmp_path/"review", spec_path="unused")
    assert not (tmp_path/"review").exists()


def test_published_review_archives_and_binds_original_implementation(tmp_path, monkeypatch):
    context, result, _ = publication_fixture(tmp_path)
    sources = []
    for key in ("implementation", "execution_adapter"):
        original = tmp_path/(key+".py")
        original.write_text(f"POLICY = '{key}-frozen'\n")
        sources.append(original)
        context["bound"][key] = {"path": str(original), "sha256": opt.sha(original)}
    # A real context must revalidate its A2 accounting gate at publication time.
    context["baseline"] = tmp_path/"baseline-artifact"
    verified = []
    monkeypatch.setattr(opt, "verify_activation", lambda path: verified.append(path))
    report = opt.publish_review(context, tmp_path/"review", [result])
    assert verified == [context["baseline"]]
    for key, original in zip(("implementation", "execution_adapter"), sources):
        receipt = report["bound_inputs"][key]
        assert receipt["original_path"] == str(original)
        assert receipt["path"] == str(tmp_path/"review/source"/original.name)
        original.write_text("# Later development cannot erase the archived program.\n")
        assert opt.sha(receipt["path"]) == receipt["sha256"]


def rolling_boundary_fixture():
    calendar = ["2024-12-30", "2024-12-31", "2025-01-02", "2025-12-30", "2025-12-31", "2026-01-05"]
    frame = pd.DataFrame([
        {"model": "M1", "execution_year": 2024, "datetime": "2024-12-31", "instrument": A, "score": 1.},
        {"model": "M1", "execution_year": 2025, "datetime": "2024-12-31", "instrument": A, "score": 2.},
        {"model": "M1", "execution_year": 2025, "datetime": "2025-12-30", "instrument": A, "score": 3.},
        {"model": "M1", "execution_year": 2025, "datetime": "2025-12-31", "instrument": A, "score": 4.},
        {"model": "M1", "execution_year": 2026, "datetime": "2025-12-31", "instrument": A, "score": 5.},
        {"model": "M0", "execution_year": 2026, "datetime": "2025-12-31", "instrument": A, "score": 99.}])
    return frame, calendar


def test_rolling_year_is_selected_by_next_actual_execution_session():
    frame, calendar = rolling_boundary_fixture()
    scores = opt.rolling_scores_for_calendar(frame, calendar)
    assert scores.loc[(pd.Timestamp("2024-12-31"), A)] == 2
    assert scores.loc[(pd.Timestamp("2025-12-30"), A)] == 3
    assert scores.loc[(pd.Timestamp("2025-12-31"), A)] == 5
    assert len(scores) == 3 and not scores.index.has_duplicates


def test_duplicate_or_unroutable_rolling_scores_fail_closed():
    frame, calendar = rolling_boundary_fixture()
    with pytest.raises(ValueError, match="Duplicate frozen"):
        opt.rolling_scores_for_calendar(pd.concat([frame, frame.iloc[[0]]]), calendar)
    with pytest.raises(ValueError, match="unique actual-session"):
        opt.rolling_scores_for_calendar(frame, calendar[::-1])
    with pytest.raises(ValueError, match="did not produce unique scores"):
        opt.rolling_scores_for_calendar(frame, calendar, model="unknown")
