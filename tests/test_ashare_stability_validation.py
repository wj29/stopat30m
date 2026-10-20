from decimal import Decimal
import hashlib
from types import SimpleNamespace

import pandas as pd
import numpy as np
import pytest

from stopat30m import ashare_stability_validation as stability
from stopat30m import ashare_union_validation as primary
from stopat30m.data.a5_distribution_tax import prepare_distribution_tax


A = "SH600000"


def market():
    calendar = pd.bdate_range("2024-08-01", "2025-01-06").strftime("%Y-%m-%d").tolist()
    days = [day for day in calendar if day >= "2025-01-01"]
    source_days = calendar[calendar.index(days[0])-1:]
    raw = pd.DataFrame([dict(date=day, instrument=A, open="10", high="10.1", low="9.9",
        close="10", preclose="10", volume="10000", amount="100000", tradestatus="1", isST="0",
        source="synthetic_fixture", source_response_sha256="a" * 64,
        source_complete_quote=True, source_observed_market_quote=True) for day in source_days])
    scores = pd.Series(1., index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp(day), A) for day in source_days[:-1]], names=["datetime", "instrument"]))
    actions = SimpleNamespace(actions=[], excluded=lambda *a, **k: False,
        suspended=lambda *a: False, conversions=lambda *a: [])
    return dict(calendar=calendar, raw=raw, basic=pd.DataFrame([
        dict(code="sh.600000", ipoDate="1999-11-10", outDate="")]),
        pool=SimpleNamespace(members_on=lambda day: [A]), actions=actions, events=[],
        rights_events=[], identity_events=[], tax_contracts=[], scores={"LGB": scores, "MOM60": scores.copy()},
        protocol={"periods": {"test": [days[0], days[-1]]},
            "execution": {**primary.EXECUTION, "top_k": 1, "keep_rank": 1, "rebalance_sessions": 1}})


def with_capital_tax(tmp_path, ratio):
    data = market()
    record = data["protocol"]["periods"]["test"][0]
    ex = data["calendar"][data["calendar"].index(record)+1]
    row = dict(event_id="explicit_synthetic_transfer", instrument=A, record_date=record, ex_date=ex,
        pay_date=ex, listing_date=ex, known_at="2024-12-30T23:59:59+08:00",
        source_response_sha256="a"*64, cash_per_share="0", stock_dividend_ratio="0",
        capital_reserve_ratio=ratio, bonus_ratio=ratio)
    path = tmp_path/"issuer.pdf"
    path.write_bytes(b"%PDF synthetic source fixture, not a real issuer notice")
    fact = {key: row[key] for key in ("instrument", "record_date", "ex_date", "cash_per_share",
                                     "stock_dividend_ratio", "capital_reserve_ratio")}
    fact.update(status="verified_face_value_source_classification_unresolved", review_required=False,
        original_pdf_visual_reviewed=True, evidence=[dict(path=str(path),
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(), url="https://issuer.example/fixture.pdf",
            pdf_pages_1based=[1])], share_face_value_cny="1", capital_reserve_face_value_per_record_share=ratio,
        capital_reserve_tax_classification="unresolved_conservative_upper_bound", classification_verified=False,
        conservative_tax_reserve_upper_bound=True, capital_reserve_tax_exempt=False, capital_reserve_taxable=False)
    events, report = prepare_distribution_tax([row], {"events": [fact]}, {"events": []})
    assert report["status"] == "passed"
    data["events"], data["tax_contracts"] = events, report["contracts"]
    return data, record, ex


def test_no_extra_tax_path_reproduces_reviewed_engine_positions_and_costs():
    before = primary.simulate_union(market(), period="test", model="LGB")
    after = stability.simulate_stability(market(), period="test", model="LGB")
    assert after["final_state"] == before["final_state"]
    assert after["records"] == before["records"]
    assert after["performance_valid"]
    assert after["metrics"]["additional_distribution_tax_reserved_cny"] == "0"


def test_additional_tax_is_expensed_before_orders_once_from_record_holders(tmp_path):
    data, record, ex = with_capital_tax(tmp_path, ".1")
    result = stability.simulate_stability(data, period="test", model="LGB")
    held = result["final_state"]["holdings_history"][record][A]
    extra = [row for row in result["records"]["corporate_actions"]
             if row["kind"] == "additional_distribution_tax_reserve"]
    assert len(extra) == 1 and extra[0]["session"] == ex
    assert Decimal(extra[0]["additional_tax_reserved_cny"]) == held * Decimal(".02")
    assert result["performance_valid"] and result["stopped"] is None
    assert result["final_state"]["positions"][A]["quantity"] == held + int(held*Decimal(".1"))
    assert Decimal(result["metrics"]["total_distribution_tax_reserved_cny"]) == held * Decimal(".02")


def test_unfunded_full_tax_stops_instead_of_publishing_whole_period_performance(tmp_path):
    data, record, ex = with_capital_tax(tmp_path, ".45")
    result = stability.simulate_stability(data, period="test", model="LGB")
    assert result["stopped"]["session"] == ex
    assert result["actual_last_valued_session"] == record
    assert not result["performance_valid"]
    assert not result["metrics"]["declared_end_reached"]
    assert not result["metrics"]["terminal_prices_fresh"]
    assert result["metrics"]["additional_distribution_tax_reserved_cny"] == "0"
    assert result["final_state"]["positions"][A]["quantity"] == result["final_state"]["holdings_history"][record][A]


def test_momentum_and_ic_read_buffers_are_checked_and_bound_until_publication(tmp_path):
    calendar = tmp_path/"calendars/day.txt"
    calendar.parent.mkdir()
    calendar.write_text("2025-01-02\n2025-01-03\n")
    folder = tmp_path/"features/sh600000"
    folder.mkdir(parents=True)
    files = {}
    for field in ("open", "close"):
        path = folder/(field+".day.bin")
        np.array([0, 10, 11], dtype="<f4").tofile(path)
        files[str(path.relative_to(tmp_path))] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    source = SimpleNamespace(dataset_dir=tmp_path, manifest={"files": files})
    bound = {}
    read = stability.read_bound_execution_model_prices(source, A, bound)
    assert read.close.tolist() == [10, 11]
    assert len(bound) == 2
    stability._assert_proofs(bound)
    np.array([0, 10, 99], dtype="<f4").tofile(folder/"close.day.bin")
    with pytest.raises(ValueError, match="Consumed model price bytes"):
        stability.read_bound_execution_model_prices(source, A, {})
    with pytest.raises(ValueError, match="source bytes changed"):
        stability._assert_proofs(bound)


def rounding_fixture(tmp_path, *, quantity=200, cash="1"):
    path = tmp_path / "issuer_rounding_resolution.json"
    path.write_text('"explicit synthetic rounding source fixture"')
    record, day = "2026-06-01", "2026-06-02"
    state = primary.a2.ex.PortfolioState.initial()
    state.cash = Decimal(cash)
    state.holdings_history[record] = {A: quantity}
    event = dict(event_id="synthetic_combined_cash", instrument=A, record_date=record,
        ex_date=day, pay_date=day, known_at="2026-05-26T23:59:59+08:00",
        cash_per_share=".9907442", source_response_sha256="a" * 64,
        cash_payment_gross_rounding="ROUND_DOWN",
        component_resolution_document={"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    return state, day, event


def test_disclosed_rounding_reserves_real_one_cent_without_reducing_tax_or_replaying(tmp_path):
    state, day, event = rounding_fixture(tmp_path)
    after, rows = stability.reserve_disclosed_cash_rounding(state, day, [event])
    assert state.cash == Decimal("1") and after.cash == Decimal(".99")
    assert rows[0]["legacy_gross_cash_cny"] == "198.15"
    assert rows[0]["issuer_floored_gross_cash_cny"] == "198.14"
    assert rows[0]["record_date_shares"] == 200
    assert rows[0]["original_tax_reserve_reduced"] is False
    assert after.entitlements == state.entitlements
    replay, journal = stability.reserve_disclosed_cash_rounding(after, day, [event])
    assert replay.to_dict() == after.to_dict() and not journal
    replay, journal = stability.reserve_disclosed_cash_rounding(after, "2026-06-03", [event])
    assert replay.to_dict() == after.to_dict() and not journal
    after.holdings_history[event["record_date"]][A] = 100
    with pytest.raises(ValueError, match="record holdings changed"):
        stability.reserve_disclosed_cash_rounding(after, day, [event])


def test_disclosed_rounding_cash_shortfall_is_atomic_and_does_not_borrow(tmp_path):
    state, day, event = rounding_fixture(tmp_path, cash="0")
    before = state.to_dict()
    with pytest.raises(stability.CashDistributionRoundingFundingError, match="Insufficient existing cash"):
        stability.reserve_disclosed_cash_rounding(state, day, [event])
    assert state.to_dict() == before


def test_disclosed_rounding_requires_verified_source_and_preorder_clock(tmp_path):
    state, day, event = rounding_fixture(tmp_path)
    event["known_at"] = day + "T10:00:00+08:00"
    with pytest.raises(ValueError, match="before ex-date orders"):
        stability.reserve_disclosed_cash_rounding(state, day, [event])
    event["known_at"] = "2026-05-26T23:59:59+08:00"
    state.holdings_history[day] = {A: 200}
    with pytest.raises(ValueError, match="before ex-date orders"):
        stability.reserve_disclosed_cash_rounding(state, day, [event])
    state.holdings_history.pop(day)
    event["cash_payment_gross_rounding"] = "ROUND_HALF_UP"
    with pytest.raises(ValueError, match="exact bound original"):
        stability.reserve_disclosed_cash_rounding(state, day, [event])
    event["cash_payment_gross_rounding"] = "ROUND_DOWN"
    (tmp_path / "issuer_rounding_resolution.json").write_text("changed")
    with pytest.raises(ValueError, match="source bytes changed"):
        stability.reserve_disclosed_cash_rounding(state, day, [event])
